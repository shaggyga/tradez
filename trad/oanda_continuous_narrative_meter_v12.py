#!/usr/bin/env python3
"""Causally sealed V12 continuous narrative meter.

V11 is retained as frozen evidence.  V12 starts a new prospective,
research-only contract: immutable rows are written only after a five-minute
bucket has closed and a short ingestion grace period has elapsed.  The latest
JSON also exposes the currently available unsealed state, but labels it
provisional and never writes it to the proof ledger.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from oanda_continuous_narrative_meter import (
    BUCKET_MINUTES,
    CURRENCIES,
    DATA,
    DEFAULT_NEWS,
    MODEL_REGISTRY,
    atomic_json,
    build_contributions,
    build_meter,
    initialize_database as initialize_legacy_schema,
    latest_payload as legacy_latest_payload,
    load_articles,
    parse_time,
)


DEFAULT_DB = DATA / "state" / "continuous_narrative_meter_v12.sqlite"
DEFAULT_LATEST = DATA / "state" / "continuous_narrative_meter_v12.json"
METER_CONTRACT_ID = "continuous_currency_narrative_meter_v12_sealed_20260827"
TERM_FACTOR_CONTRACT_ID = METER_CONTRACT_ID + ".term_factor_timeseries_v2"
METER_ACTIVATED_UTC = dt.datetime(2026, 8, 27, 7, 25, tzinfo=dt.timezone.utc)
SEAL_GRACE_SECONDS = 60


def floor_clock(value: dt.datetime, minutes: int = BUCKET_MINUTES) -> dt.datetime:
    value = value.astimezone(dt.timezone.utc)
    width = minutes * 60
    seconds = int(value.timestamp())
    return dt.datetime.fromtimestamp((seconds // width) * width, dt.timezone.utc)


def sealed_bucket_end(
    now: dt.datetime, *, grace_seconds: int = SEAL_GRACE_SECONDS
) -> dt.datetime:
    """Return the latest bucket end whose close plus grace has elapsed."""

    return floor_clock(now - dt.timedelta(seconds=max(0, grace_seconds)))


def open_bucket_end(now: dt.datetime) -> dt.datetime:
    """Return the end label for the currently open five-minute bucket."""

    return floor_clock(now) + dt.timedelta(minutes=BUCKET_MINUTES)


def initialize_database(connection: sqlite3.Connection) -> None:
    # Reuse the stable row schema while writing only to the new V12 database.
    initialize_legacy_schema(connection)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS meter_contract_registry(
            meter_contract_id TEXT PRIMARY KEY,
            activated_utc TEXT NOT NULL,
            bucket_minutes INTEGER NOT NULL,
            seal_grace_seconds INTEGER NOT NULL,
            bucket_semantics TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            created_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bucket_seals(
            meter_contract_id TEXT NOT NULL,
            clock_utc TEXT NOT NULL,
            sealed_at_utc TEXT NOT NULL,
            seal_grace_seconds INTEGER NOT NULL,
            currency_row_count INTEGER NOT NULL,
            input_story_count INTEGER NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            PRIMARY KEY(meter_contract_id,clock_utc)
        );
        CREATE TABLE IF NOT EXISTS seal_integrity_events(
            event_id TEXT PRIMARY KEY,
            meter_contract_id TEXT NOT NULL,
            detected_utc TEXT NOT NULL,
            decision_clock_utc TEXT NOT NULL,
            currency TEXT NOT NULL,
            story_id TEXT NOT NULL,
            source_family TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            reason TEXT NOT NULL,
            action TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE INDEX IF NOT EXISTS ix_seal_integrity_clock
            ON seal_integrity_events(decision_clock_utc,currency);
        CREATE TABLE IF NOT EXISTS seal_gap_events(
            event_id TEXT PRIMARY KEY,
            meter_contract_id TEXT NOT NULL,
            detected_utc TEXT NOT NULL,
            prior_sealed_clock_utc TEXT NOT NULL,
            next_sealed_clock_utc TEXT NOT NULL,
            missing_bucket_count INTEGER NOT NULL,
            reason TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE TRIGGER IF NOT EXISTS no_update_v12_currency_meter
            BEFORE UPDATE ON currency_meter BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete_v12_currency_meter
            BEFORE DELETE ON currency_meter BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_update_v12_bucket_seals
            BEFORE UPDATE ON bucket_seals BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete_v12_bucket_seals
            BEFORE DELETE ON bucket_seals BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_update_v12_integrity_events
            BEFORE UPDATE ON seal_integrity_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete_v12_integrity_events
            BEFORE DELETE ON seal_integrity_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_update_v12_gap_events
            BEFORE UPDATE ON seal_gap_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS no_delete_v12_gap_events
            BEFORE DELETE ON seal_gap_events BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )


def _contract_payload(
    rows: Iterable[Mapping[str, Any]],
    selection: Mapping[str, Any],
    *,
    generated_utc: dt.datetime,
) -> dict[str, Any]:
    payload = legacy_latest_payload(rows, selection)
    payload.update(
        {
            "schema_version": "continuous_narrative_meter_latest_v12",
            "meter_contract_id": METER_CONTRACT_ID,
            "generated_utc": generated_utc.isoformat(),
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
            "orders_placed": 0,
        }
    )
    return payload


def _late_arrivals(
    connection: sqlite3.Connection,
    contributions: Iterable[Mapping[str, Any]],
    *,
    detected: dt.datetime,
) -> int:
    prior = connection.execute(
        "SELECT MAX(clock_utc) FROM bucket_seals WHERE meter_contract_id=?",
        (METER_CONTRACT_ID,),
    ).fetchone()[0]
    prior_clock = parse_time(prior)
    if prior_clock is None:
        return 0

    inserted = 0
    cached: dict[tuple[str, str], set[str]] = {}
    for contribution in contributions:
        decision = contribution.get("decision_utc")
        if not isinstance(decision, dt.datetime):
            continue
        if decision < METER_ACTIVATED_UTC or decision > prior_clock:
            continue
        currency = str(contribution.get("currency") or "")
        clock_text = decision.isoformat()
        cache_key = (clock_text, currency)
        if cache_key not in cached:
            row = connection.execute(
                "SELECT story_ids_json FROM currency_meter "
                "WHERE meter_contract_id=? AND clock_utc=? AND currency=?",
                (METER_CONTRACT_ID, clock_text, currency),
            ).fetchone()
            try:
                cached[cache_key] = set(json.loads(row[0])) if row else set()
            except (TypeError, ValueError, json.JSONDecodeError):
                cached[cache_key] = set()
        story_id = str(contribution.get("story_id") or "")
        if story_id in cached[cache_key]:
            continue
        source_family = str(contribution.get("source_family") or "unknown")
        identity = "|".join(
            (METER_CONTRACT_ID, clock_text, currency, story_id, source_family)
        )
        event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        cursor = connection.execute(
            """INSERT OR IGNORE INTO seal_integrity_events VALUES(
                   ?,?,?,?,?,?,?,?,?,?,?,?
               )""",
            (
                event_id,
                METER_CONTRACT_ID,
                detected.isoformat(),
                clock_text,
                currency,
                story_id,
                source_family,
                str(contribution.get("first_seen_utc") or ""),
                "source_row_became_available_after_bucket_was_immutably_sealed",
                "record_incident_do_not_rewrite_sealed_evidence",
                1,
                0,
            ),
        )
        inserted += max(0, int(cursor.rowcount))
    return inserted


def persist(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    contributions: Iterable[Mapping[str, Any]],
    *,
    sealed_through: dt.datetime,
    now: dt.datetime | None = None,
) -> dict[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    created_at = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    created = created_at.isoformat()
    # V12 is prospective rather than a renamed historical rebuild.  Seal only
    # the bucket due on this cycle; a worker outage remains a visible gap and
    # cannot later be filled with information that arrived after that clock.
    eligible_rows = [
        dict(row)
        for row in rows
        if row["clock_utc"] == sealed_through
        and row["clock_utc"] >= METER_ACTIVATED_UTC
    ]
    grouped: defaultdict[dt.datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in eligible_rows:
        grouped[row["clock_utc"]].append(row)

    connection = sqlite3.connect(path, timeout=60.0)
    try:
        initialize_database(connection)
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO meter_contract_registry VALUES(?,?,?,?,?,?,?,?)",
                (
                    METER_CONTRACT_ID,
                    METER_ACTIVATED_UTC.isoformat(),
                    BUCKET_MINUTES,
                    SEAL_GRACE_SECONDS,
                    "clock_utc_is_closed_bucket_end; rows_seal_only_after_close_plus_grace",
                    1,
                    0,
                    created,
                ),
            )
            for model_id, specification in MODEL_REGISTRY.items():
                connection.execute(
                    "INSERT OR IGNORE INTO model_registry VALUES(?,?,?,?,?,?)",
                    (
                        model_id,
                        METER_CONTRACT_ID,
                        json.dumps(specification, sort_keys=True),
                        1,
                        0,
                        created,
                    ),
                )

            late = _late_arrivals(connection, contributions, detected=created_at)
            prior_seal_text = connection.execute(
                "SELECT MAX(clock_utc) FROM bucket_seals WHERE meter_contract_id=?",
                (METER_CONTRACT_ID,),
            ).fetchone()[0]
            prior_seal = parse_time(prior_seal_text)
            new_gap = 0
            expected_next = (
                METER_ACTIVATED_UTC
                if prior_seal is None
                else prior_seal + dt.timedelta(minutes=BUCKET_MINUTES)
            )
            if sealed_through > expected_next:
                missing = int(
                    (sealed_through - expected_next).total_seconds()
                    // (BUCKET_MINUTES * 60)
                )
                identity = "|".join(
                    (
                        METER_CONTRACT_ID,
                        prior_seal_text or "activation",
                        sealed_through.isoformat(),
                    )
                )
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO seal_gap_events VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        hashlib.sha256(identity.encode("utf-8")).hexdigest(),
                        METER_CONTRACT_ID,
                        created,
                        prior_seal_text or METER_ACTIVATED_UTC.isoformat(),
                        sealed_through.isoformat(),
                        missing,
                        "worker_gap_preserved_no_retrospective_bucket_backfill",
                        1,
                        0,
                    ),
                )
                new_gap = max(0, int(cursor.rowcount))
            inserted_rows = 0
            inserted_clocks = 0
            for clock in sorted(grouped):
                clock_rows = grouped[clock]
                clock_inserted = 0
                story_ids: set[str] = set()
                for row in clock_rows:
                    cursor = connection.execute(
                        """INSERT OR IGNORE INTO currency_meter(
                               meter_contract_id,clock_utc,currency,model_scores_json,
                               attention_level,attention_acceleration,new_story_count,
                               active_story_count,source_family_count,agreement,novelty,
                               trusted_story_count,forward_timely_story_count,
                               source_families_json,story_ids_json,
                               classification_versions_json,evidence_class,created_utc
                           ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            METER_CONTRACT_ID,
                            clock.isoformat(),
                            row["currency"],
                            json.dumps(row["model_scores"], sort_keys=True),
                            row["attention_level"],
                            row["attention_acceleration"],
                            row["new_story_count"],
                            row["active_story_count"],
                            row["source_family_count"],
                            row["agreement"],
                            row["novelty"],
                            row["trusted_story_count"],
                            row["forward_timely_story_count"],
                            json.dumps(row["source_families"], sort_keys=True),
                            json.dumps(row["story_ids"], sort_keys=True),
                            json.dumps(row["classification_versions"], sort_keys=True),
                            row["historical_evidence_class"],
                            created,
                        ),
                    )
                    was_inserted = max(0, int(cursor.rowcount))
                    inserted_rows += was_inserted
                    clock_inserted += was_inserted
                    story_ids.update(str(value) for value in row["story_ids"])
                    if not was_inserted:
                        continue
                    term_rows = list(row.get("term_factor_contributions") or [])
                    connection.execute(
                        "INSERT OR IGNORE INTO narrative_term_factor_clock "
                        "VALUES(?,?,?,?,?,?,?)",
                        (
                            TERM_FACTOR_CONTRACT_ID,
                            clock.isoformat(),
                            row["currency"],
                            row["active_story_count"],
                            len(term_rows),
                            len({str(item["term"]) for item in term_rows}),
                            created,
                        ),
                    )
                    for item in term_rows:
                        connection.execute(
                            """INSERT OR IGNORE INTO narrative_term_factor_contribution
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (
                                TERM_FACTOR_CONTRACT_ID,
                                clock.isoformat(),
                                row["currency"],
                                item["story_id"], item["event_id"], item["source_id"],
                                item["source_family"], item["term"], item["evidence_channel"],
                                item["published_score"], item["research_score"],
                                item["secondary_score"], item["tone_score"],
                                item["quality_weight"], item["decay"],
                                item["effective_weight"], item["published_utc"],
                                item["first_seen_utc"], item["last_seen_utc"],
                                item["source_contract_id"], item["source_cohort_id"],
                                item["parser_version"], item["classification_version"],
                                item["revision_id"], item["revised_at_utc"],
                                item["supersedes_event_id"], item["superseded_at_utc"],
                                item["raw_payload_hash"], 1, 0, created,
                                item.get("observed_research_score", item["research_score"]),
                            ),
                        )
                if clock_inserted == len(CURRENCIES):
                    cursor = connection.execute(
                        "INSERT OR IGNORE INTO bucket_seals VALUES(?,?,?,?,?,?,?,?)",
                        (
                            METER_CONTRACT_ID,
                            clock.isoformat(),
                            created,
                            SEAL_GRACE_SECONDS,
                            len(clock_rows),
                            len(story_ids),
                            1,
                            0,
                        ),
                    )
                    inserted_clocks += max(0, int(cursor.rowcount))
        total_late = connection.execute(
            "SELECT COUNT(*) FROM seal_integrity_events WHERE meter_contract_id=?",
            (METER_CONTRACT_ID,),
        ).fetchone()[0]
        total_gaps = connection.execute(
            "SELECT COUNT(*) FROM seal_gap_events WHERE meter_contract_id=?",
            (METER_CONTRACT_ID,),
        ).fetchone()[0]
        return {
            "inserted_currency_rows": inserted_rows,
            "inserted_bucket_seals": inserted_clocks,
            "new_late_arrival_incidents": late,
            "total_late_arrival_incidents": int(total_late),
            "new_seal_gap_incidents": new_gap,
            "total_seal_gap_incidents": int(total_gaps),
        }
    finally:
        connection.close()


def build_cycle(
    contributions: Iterable[Mapping[str, Any]], *, now: dt.datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dt.datetime, dt.datetime]:
    # Context already known at activation may initialize the prospective
    # state, but no pre-activation clock is ever persisted under V12.
    values = [dict(row) for row in contributions]
    sealed_end = sealed_bucket_end(now)
    provisional_end = open_bucket_end(now)
    output_start = max(METER_ACTIVATED_UTC, sealed_end - dt.timedelta(hours=24))
    context_starts = [
        row["decision_utc"]
        for row in values
        if row.get("expires_utc") >= output_start
        and row["decision_utc"] < output_start
    ]
    compute_start = min(context_starts, default=output_start)
    computed_rows = build_meter(values, start=compute_start, end=provisional_end)
    all_rows = [
        row for row in computed_rows if row["clock_utc"] >= output_start
    ]
    sealed_rows = [row for row in all_rows if row["clock_utc"] <= sealed_end]
    return sealed_rows, all_rows, sealed_end, provisional_end


def run_once(
    *,
    news: Path = DEFAULT_NEWS,
    database: Path = DEFAULT_DB,
    latest: Path = DEFAULT_LATEST,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    observed_at = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    contributions, selection = build_contributions(
        load_articles(news, since=observed_at - dt.timedelta(days=2))
    )
    sealed_rows, all_rows, sealed_end, provisional_end = build_cycle(
        contributions, now=observed_at
    )
    persistence = persist(
        database,
        sealed_rows,
        contributions,
        sealed_through=sealed_end,
        now=observed_at,
    )
    sealed_payload = _contract_payload(
        sealed_rows, selection, generated_utc=observed_at
    )
    provisional_rows = [
        row for row in all_rows if row["clock_utc"] == provisional_end
    ]
    provisional_payload = _contract_payload(
        provisional_rows, selection, generated_utc=observed_at
    )
    sealed_payload.update(
        {
            "mode": "live_prospective_sealed",
            "meter_rows_built": len(all_rows),
            "activation_utc": METER_ACTIVATED_UTC.isoformat(),
            "bucket_minutes": BUCKET_MINUTES,
            "seal_grace_seconds": SEAL_GRACE_SECONDS,
            "sealed_clock_utc": (
                sealed_payload.get("clock_utc")
                if sealed_end >= METER_ACTIVATED_UTC
                else None
            ),
            "sealed_through_utc": sealed_end.isoformat(),
            "sealed_state_kind": "immutable_completed_bucket",
            "persistence": persistence,
            "integrity_status": (
                "late_arrival_and_seal_gap_incident"
                if persistence["total_late_arrival_incidents"]
                and persistence["total_seal_gap_incidents"]
                else "late_arrival_incident"
                if persistence["total_late_arrival_incidents"]
                else "seal_gap_incident"
                if persistence["total_seal_gap_incidents"]
                else "ok"
            ),
            "partial_live": {
                "state_kind": "unsealed_provisional_live_view",
                "as_of_utc": observed_at.isoformat(),
                "bucket_end_utc": provisional_end.isoformat(),
                "complete": False,
                "persisted": False,
                "proof_eligible": False,
                "research_only": True,
                "execution_eligible": False,
                "can_place_orders": False,
                "clock_utc": provisional_payload.get("clock_utc"),
                "currencies": provisional_payload["currencies"],
                "pairs": provisional_payload["pairs"],
            },
        }
    )
    atomic_json(latest, sealed_payload)
    return sealed_payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=DEFAULT_NEWS)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument("--latest", type=Path, default=DEFAULT_LATEST)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run_once(news=args.news, database=args.database, latest=args.latest)
        print(
            json.dumps(
                {
                    "sealed_clock_utc": payload["sealed_clock_utc"],
                    "partial_bucket_end_utc": payload["partial_live"]["bucket_end_utc"],
                    "currency_count": payload["currency_count"],
                    "instrument_count": payload["instrument_count"],
                    "persistence": payload["persistence"],
                    "integrity_status": payload["integrity_status"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            break
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
