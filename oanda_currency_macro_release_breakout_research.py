#!/usr/bin/env python3
"""Currency-aware, two-sided official macro-release breakout research.

This is a new immutable shadow cohort.  It does not alter or merge the frozen
USD-only discovery cohort.  Numeric releases without a causally captured
pre-release consensus open a two-sided watch; observed cross-pair price action
supplies timing and direction only after the value was actually known locally.

The worker cannot trade, promote, authorize, or change Practice 007 policy.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from oanda_instrument_pips import fallback_pip_size
from oanda_local_news_sentiment import normalized_observation_time
from oanda_macro_release_breakout_research import atomic_json, atomic_text
from oanda_policy_statement_breakout_research import (
    evaluate_breakout_setup,
    finite,
    iso,
    load_quote_rows,
    parse_utc,
)
from oanda_worker_heartbeat import WorkerHeartbeat


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "news_sources_v1.json"
NEWS_DB = DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite"
QUOTE_DB = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
DATABASE = STATE / "currency_macro_release_breakout_research_v1.sqlite"
OUTPUT = STATE / "currency_macro_release_breakout_research_v1.json"
HEARTBEAT = STATE / "currency_macro_release_breakout_research_heartbeat_v1.json"
REPORT = (
    DATA
    / "reports"
    / "currency_event_technical_coverage"
    / "CURRENCY_MACRO_RELEASE_BREAKOUT_CURRENT.md"
)

UTC = dt.timezone.utc
CONTRACT_ID = "currency_macro_release_two_sided_v2_20260816"
SUPERSEDES_CONTRACT_ID = "currency_macro_release_two_sided_v1_20260816"
COHORT_START = dt.datetime(2026, 8, 16, 7, 5, tzinfo=UTC)
COHORTS = {
    5: "currency_macro_release_two_sided_h5_v2_20260816",
    15: "currency_macro_release_two_sided_h15_v2_20260816",
    30: "currency_macro_release_two_sided_h30_v2_20260816",
    60: "currency_macro_release_two_sided_h60_v2_20260816",
}
MAX_CAUSAL_INGEST_LATENCY_MINUTES = 30
MIN_STABILIZATION_MINUTES = 3
MIN_PERSISTENT_CONFIRMATION_MINUTES = 2


def source_currency_map(config_path: Path) -> dict[str, str]:
    """Return only unambiguous one-currency official source bindings."""

    if not config_path.exists():
        return {}
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    output: dict[str, str] = {}
    for source in payload.get("sources") or []:
        currencies = [str(value).upper() for value in source.get("currencies") or []]
        source_id = str(source.get("source_id") or "")
        if (
            source_id
            and len(currencies) == 1
            and bool(source.get("verified"))
            and bool(source.get("direct"))
            and str(source.get("source_role") or "")
            == "primary_statistical_release"
        ):
            output[source_id] = currencies[0]
    return output


def _payload_source_id(payload: Mapping[str, Any]) -> str:
    source_id = str(payload.get("source_parent_id") or payload.get("source_id") or "")
    return source_id.split(":", 1)[0]


def _event_identity(payload: Mapping[str, Any], source_id: str) -> str:
    """Return one stable identity for duplicate representations of a release."""

    fields = (
        source_id,
        str(payload.get("source_url") or ""),
        str(payload.get("event_series_id") or ""),
        str(payload.get("published_utc") or ""),
        str(payload.get("actual_value") or ""),
        str(payload.get("previous_value") or ""),
        str(payload.get("reference_period") or ""),
    )
    return hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()[:32]


def load_structured_macro_events(
    news_db: Path,
    config_path: Path = CONFIG,
    heartbeat: WorkerHeartbeat | None = None,
) -> list[dict[str, Any]]:
    """Load verified official actuals with point-in-time currency binding."""

    if not news_db.exists():
        return []
    currencies = source_currency_map(config_path)
    if not currencies:
        return []
    connection = sqlite3.connect(
        f"file:{news_db.as_posix()}?mode=ro", uri=True, timeout=10
    )
    if heartbeat is not None:
        heartbeat.update(phase="querying_currency_macro_candidates")
    source_ids = tuple(currencies)
    placeholders = ",".join("?" for _ in source_ids)
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(articles)")
    }
    try:
        if "source_id" in columns:
            prefix_terms = " OR ".join("source_id LIKE ?" for _ in source_ids)
            rows = connection.execute(
                f"""
                SELECT event_id,payload_json,first_seen_utc FROM articles
                WHERE source_id IN ({placeholders}) OR {prefix_terms}
                ORDER BY first_seen_utc,event_id
                """,
                source_ids + tuple(f"{source_id}:%" for source_id in source_ids),
            ).fetchall()
        else:
            rows = connection.execute(
                f"""
                SELECT event_id,payload_json,first_seen_utc FROM articles
                WHERE CASE WHEN json_valid(payload_json) THEN
                    COALESCE(
                        NULLIF(json_extract(payload_json,'$.source_parent_id'),''),
                        CASE
                            WHEN instr(json_extract(payload_json,'$.source_id'), ':') > 0
                            THEN substr(
                                json_extract(payload_json,'$.source_id'),
                                1,
                                instr(json_extract(payload_json,'$.source_id'), ':') - 1
                            )
                            ELSE json_extract(payload_json,'$.source_id')
                        END
                    ) IN ({placeholders})
                ELSE 0 END
                ORDER BY first_seen_utc,event_id
                """,
                source_ids,
            ).fetchall()
    except sqlite3.OperationalError:
        rows = connection.execute(
            "SELECT event_id,payload_json,first_seen_utc FROM articles "
            "ORDER BY first_seen_utc,event_id"
        ).fetchall()
    connection.close()
    canonical: dict[str, dict[str, Any]] = {}
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="processing_currency_macro_candidates",
            candidate_articles=len(rows),
            candidate_articles_processed=0,
        )
    for row_number, (event_id, payload_json, first_seen) in enumerate(rows, 1):
        if heartbeat is not None and row_number % 100 == 0:
            heartbeat.mark_progress(
                phase="processing_currency_macro_candidates",
                candidate_articles=len(rows),
                candidate_articles_processed=row_number,
            )
        try:
            payload = json.loads(payload_json or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        source_id = _payload_source_id(payload)
        currency = currencies.get(source_id)
        if (
            currency is None
            or not bool(payload.get("source_verified"))
            or not bool(payload.get("source_direct"))
            or bool(payload.get("source_listing_bootstrap"))
            or payload.get("actual_value") in (None, "")
        ):
            continue
        published = parse_utc(payload.get("published_utc"))
        known = parse_utc(
            payload.get("numeric_causal_known_utc")
            or payload.get("causal_known_utc")
            or first_seen
        )
        if published is None or known is None or known < published:
            continue
        latency_minutes = (known - published).total_seconds() / 60.0
        native_publication_clock = not bool(payload.get("published_time_inferred"))
        prospective = bool(
            published >= COHORT_START
            and known >= COHORT_START
            and latency_minutes <= MAX_CAUSAL_INGEST_LATENCY_MINUTES
            and native_publication_clock
        )
        canonical_id = _event_identity(payload, source_id)
        candidate = {
                "event_id": canonical_id,
                "source_event_ids": [str(event_id)],
                "duplicate_representation_count": 1,
                "source_id": source_id,
                "event_series_id": str(payload.get("event_series_id") or ""),
                "event_name": str(
                    payload.get("event_name") or payload.get("headline") or ""
                ),
                "currency": currency,
                "known_utc": iso(known),
                "published_utc": iso(published),
                "causal_ingest_latency_minutes": round(latency_minutes, 6),
                "actual_value": payload.get("actual_value"),
                "previous_value": payload.get("previous_value"),
                "consensus_value": payload.get("consensus_value"),
                "reference_period": str(payload.get("reference_period") or ""),
                "source_url": str(payload.get("source_url") or ""),
                "prospective_eligible": prospective,
                "eligibility_reason": (
                    "prospective_collecting"
                    if prospective
                    else "inferred_publication_clock_excluded"
                    if not native_publication_clock
                    else "pre_cohort_or_ingest_latency_excluded"
                ),
                "research_only": True,
                "execution_eligible": False,
            }
        existing = canonical.get(canonical_id)
        if existing is None:
            canonical[canonical_id] = candidate
            continue
        existing["source_event_ids"] = sorted(
            set(existing["source_event_ids"] + candidate["source_event_ids"])
        )
        existing["duplicate_representation_count"] = len(
            existing["source_event_ids"]
        )
        if str(candidate["known_utc"]) < str(existing["known_utc"]):
            for key in (
                "known_utc",
                "causal_ingest_latency_minutes",
                "prospective_eligible",
                "eligibility_reason",
            ):
                existing[key] = candidate[key]
    return sorted(
        canonical.values(), key=lambda row: (row["known_utc"], row["event_id"])
    )


def confirmation_requirement(pair_count: int) -> int | None:
    """Require multiple legs while capping the currency-factor vote at four."""

    if pair_count < 2:
        return None
    return min(4, max(2, math.ceil(pair_count / 2)))


def quote_window(
    event: Mapping[str, Any], quote_db: Path
) -> dict[str, dict[int, dict[str, float]]]:
    published = parse_utc(event.get("published_utc"))
    known = parse_utc(event.get("known_utc"))
    if published is None or known is None:
        return {}
    start = int(published.timestamp() // 60 * 60) - 10 * 60
    end = int(known.timestamp() // 60 * 60) + 75 * 60
    return load_quote_rows(quote_db, str(event.get("currency") or ""), start, end)


def evaluate_two_sided(
    event: Mapping[str, Any],
    rows: Mapping[str, Mapping[int, Mapping[str, float]]],
) -> dict[str, Any]:
    """Choose the first persistent, multi-leg currency-factor confirmation."""

    known = parse_utc(event.get("known_utc"))
    requirement = confirmation_requirement(len(rows))
    if known is None:
        return {"status": "abstain", "reason": "missing_causal_known_clock"}
    if requirement is None:
        return {
            "status": "abstain",
            "reason": "fewer_than_two_currency_pair_legs",
            "available_pair_count": len(rows),
            "research_only": True,
            "execution_eligible": False,
        }
    known_minute = int(known.timestamp() // 60 * 60)
    candidates: list[dict[str, Any]] = []
    for currency_direction in ("STRENGTHEN", "WEAKEN"):
        probe = {**dict(event), "delta": {"currency_direction": currency_direction}}
        diagnostics = evaluate_breakout_setup(
            probe, rows, minimum_confirming_pairs=len(rows) + 1
        ).get("diagnostics") or []
        by_minute: dict[int, list[dict[str, Any]]] = {}
        for row in diagnostics:
            minute = parse_utc(row.get("confirmation_minute_utc"))
            if minute is None or not bool(row.get("confirmed")):
                continue
            epoch = int(minute.timestamp())
            if epoch < known_minute + MIN_STABILIZATION_MINUTES * 60:
                continue
            by_minute.setdefault(epoch, []).append(row)
        eligible = sorted(
            minute
            for minute, confirmations in by_minute.items()
            if len(confirmations) >= requirement
        )
        for minute in eligible:
            if not all(
                minute - offset * 60 in eligible
                for offset in range(MIN_PERSISTENT_CONFIRMATION_MINUTES)
            ):
                continue
            confirmations = by_minute[minute]
            selected = min(
                confirmations,
                key=lambda row: (row["entry_cost_bps"], row["instrument"]),
            )
            candidates.append(
                {
                    "status": "triggered",
                    "confirmation_minute_utc": selected["confirmation_minute_utc"],
                    "entry_minute_utc": selected["entry_minute_utc"],
                    "confirmation_count": len(confirmations),
                    "available_pair_count": len(rows),
                    "minimum_confirming_pairs": requirement,
                    "confirming_pairs": sorted(
                        row["instrument"] for row in confirmations
                    ),
                    "selected": selected,
                    "currency_direction": currency_direction,
                    "causal_consensus_available": event.get("consensus_value")
                    not in (None, ""),
                    "direction_source": "price_confirmed_after_causal_observation",
                    "one_currency_factor_counted": True,
                    "research_only": True,
                    "execution_eligible": False,
                }
            )
            break
    if not candidates:
        return {
            "status": "no_trigger",
            "reason": "neither_direction_reached_persistent_multi_leg_confirmation",
            "available_pair_count": len(rows),
            "minimum_confirming_pairs": requirement,
            "research_only": True,
            "execution_eligible": False,
        }
    candidates.sort(
        key=lambda setup: (
            str(setup.get("confirmation_minute_utc") or ""),
            -int(setup.get("confirmation_count") or 0),
            finite((setup.get("selected") or {}).get("entry_cost_bps"), 999999.0),
        )
    )
    return candidates[0]


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS currency_macro_events (
          event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
          known_utc TEXT NOT NULL, published_utc TEXT NOT NULL,
          source_id TEXT NOT NULL, currency TEXT NOT NULL,
          prospective_eligible INTEGER NOT NULL, payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prospective_entries (
          entry_id TEXT PRIMARY KEY, cohort_id TEXT NOT NULL,
          event_id TEXT NOT NULL, decided_utc TEXT NOT NULL,
          entry_minute_utc TEXT NOT NULL, instrument TEXT NOT NULL,
          direction TEXT NOT NULL, horizon_min INTEGER NOT NULL,
          entry_mid REAL NOT NULL, entry_spread_pips REAL NOT NULL,
          status TEXT NOT NULL, outcome_utc TEXT,
          gross_pips REAL, estimated_after_spread_pips REAL,
          payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_currency_macro_entry_status
          ON prospective_entries(status,entry_minute_utc,horizon_min);
        """
    )
    connection.commit()
    return connection


def persist_event(connection: sqlite3.Connection, event: Mapping[str, Any]) -> int:
    connection.execute(
        "INSERT OR IGNORE INTO currency_macro_events VALUES (?,?,?,?,?,?,?,?)",
        (
            event["event_id"],
            CONTRACT_ID,
            event["known_utc"],
            event["published_utc"],
            event["source_id"],
            event["currency"],
            int(bool(event.get("prospective_eligible"))),
            json.dumps(event, sort_keys=True),
        ),
    )
    inserted = int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return inserted


def persist_entries(
    connection: sqlite3.Connection,
    event: Mapping[str, Any],
    setup: Mapping[str, Any],
    observed: dt.datetime,
) -> int:
    if not bool(event.get("prospective_eligible")) or setup.get("status") != "triggered":
        return 0
    selected = setup.get("selected") or {}
    inserted = 0
    for horizon, cohort_id in COHORTS.items():
        identity = "|".join(
            (
                cohort_id,
                str(event["event_id"]),
                str(selected["instrument"]),
                str(selected["direction"]),
            )
        )
        entry_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        payload = {
            "event": event,
            "setup": setup,
            "cohort_id": cohort_id,
            "horizon_min": horizon,
            "research_only": True,
            "execution_eligible": False,
        }
        connection.execute(
            "INSERT OR IGNORE INTO prospective_entries VALUES "
            "(?,?,?,?,?,?,?,?,?,?,'pending',NULL,NULL,NULL,?)",
            (
                entry_id,
                cohort_id,
                event["event_id"],
                iso(observed),
                setup["entry_minute_utc"],
                selected["instrument"],
                selected["direction"],
                horizon,
                selected["entry_mid"],
                selected["entry_spread_pips"],
                json.dumps(payload, sort_keys=True),
            ),
        )
        inserted += int(connection.execute("SELECT changes()").fetchone()[0] > 0)
    connection.commit()
    return inserted


def mature_entries(connection: sqlite3.Connection, quote_db: Path) -> int:
    pending = connection.execute(
        "SELECT entry_id,entry_minute_utc,instrument,direction,horizon_min,"
        "entry_mid,entry_spread_pips,payload_json FROM prospective_entries "
        "WHERE status='pending'"
    ).fetchall()
    if not quote_db.exists():
        return 0
    quotes = sqlite3.connect(
        f"file:{quote_db.as_posix()}?mode=ro", uri=True, timeout=10
    )
    matured = 0
    for row in pending:
        entry_id, entry_text, instrument, direction, horizon, entry_mid, entry_spread, payload_json = row
        entry = parse_utc(entry_text)
        if entry is None:
            continue
        outcome_epoch = int(entry.timestamp()) + int(horizon) * 60
        outcome = quotes.execute(
            "SELECT last_mid,average_spread_pips,last_broker_time "
            "FROM quote_intensity_minutes_v1 WHERE instrument=? AND minute_epoch=?",
            (instrument, outcome_epoch),
        ).fetchone()
        if outcome is None:
            continue
        sign = 1 if str(direction) == "long" else -1
        pip = fallback_pip_size(str(instrument))
        gross = sign * (finite(outcome[0]) - finite(entry_mid)) / pip
        cost = 0.5 * (finite(entry_spread) + finite(outcome[1]))
        payload = json.loads(payload_json)
        payload["outcome"] = {
            "outcome_utc": str(
                outcome[2]
                or iso(dt.datetime.fromtimestamp(outcome_epoch, UTC))
            ),
            "gross_directional_pips": gross,
            "estimated_after_spread_pips": gross - cost,
        }
        connection.execute(
            "UPDATE prospective_entries SET status='matured',outcome_utc=?,"
            "gross_pips=?,estimated_after_spread_pips=?,payload_json=? WHERE entry_id=?",
            (
                payload["outcome"]["outcome_utc"],
                gross,
                gross - cost,
                json.dumps(payload, sort_keys=True),
                entry_id,
            ),
        )
        matured += 1
    quotes.close()
    connection.commit()
    return matured


def render_report(payload: Mapping[str, Any]) -> str:
    by_currency = payload.get("events_by_currency") or {}
    lines = [
        "# Currency macro-release breakout research",
        "",
        f"Generated: {payload.get('generated_utc')}",
        "",
        "This is a research-only, immutable prospective cohort. It does not",
        "trade, promote, authorize, or alter the frozen USD-only cohort.",
        "",
        f"- Official numeric events loaded: {payload.get('official_numeric_events_loaded')}",
        f"- Prospectively eligible events: {payload.get('prospective_events_loaded')}",
        f"- Currencies represented: {len(by_currency)}",
        f"- Entries: {payload.get('entry_status_counts')}",
        "",
        "| Currency | Official actual records | Prospective records |",
        "|---|---:|---:|",
    ]
    for currency, row in sorted(by_currency.items()):
        lines.append(
            f"| {currency} | {row.get('loaded', 0)} | {row.get('prospective', 0)} |"
        )
    lines.extend(
        [
            "",
            "## Frozen rule",
            "",
            "- Currency is bound from the verified one-currency source contract.",
            "- The release must be observed within 30 minutes and after cohort start.",
            "- Without causal consensus, both directions are watched.",
            "- Stabilization begins at the local causal-known time, not scheduled time.",
            "- Two consecutive completed minutes and multiple currency legs must agree.",
            "- The lowest-cost confirming pair is the counterfactual selection.",
            "- All related pairs count as one currency factor.",
            "",
        ]
    )
    return "\n".join(lines)


def run_once(
    *,
    news_db: Path = NEWS_DB,
    config_path: Path = CONFIG,
    quote_db: Path = QUOTE_DB,
    database_path: Path = DATABASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
    observed: dt.datetime | None = None,
    heartbeat: WorkerHeartbeat | None = None,
) -> dict[str, Any]:
    if observed is None:
        observed, clock = normalized_observation_time(dt.datetime.now(UTC))
    else:
        observed = observed.astimezone(UTC)
        clock = {"source": "provided", "trusted_for_prospective_evidence": True}
    events = load_structured_macro_events(
        news_db, config_path, heartbeat=heartbeat
    )
    supported_currencies = sorted(set(source_currency_map(config_path).values()))
    connection = open_database(database_path)
    inserted_events = 0
    inserted_entries = 0
    setups: list[dict[str, Any]] = []
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="evaluating_currency_events",
            events_loaded=len(events),
            events_processed=0,
    )
    for event_number, event in enumerate(events, 1):
        if heartbeat is not None:
            heartbeat.mark_progress(
                phase="evaluating_currency_events",
                events_loaded=len(events),
                events_processed=event_number,
            )
        inserted_events += persist_event(connection, event)
        known = parse_utc(event.get("known_utc"))
        if not bool(event.get("prospective_eligible")) or known is None:
            continue
        if observed > known + dt.timedelta(minutes=90):
            continue
        rows = quote_window(event, quote_db)
        setup = evaluate_two_sided(event, rows)
        setups.append({"event_id": event["event_id"], "setup": setup})
        inserted_entries += persist_entries(connection, event, setup, observed)
    if heartbeat is not None:
        heartbeat.update(phase="maturing_entries")
    matured = mature_entries(connection, quote_db)
    status_counts = {
        str(status): int(count)
        for status, count in connection.execute(
            "SELECT status,COUNT(*) FROM prospective_entries GROUP BY status"
        ).fetchall()
    }
    connection.close()
    by_currency: dict[str, dict[str, int]] = {}
    for event in events:
        row = by_currency.setdefault(event["currency"], {"loaded": 0, "prospective": 0})
        row["loaded"] += 1
        row["prospective"] += int(bool(event.get("prospective_eligible")))
    payload = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "supersedes_contract_id": SUPERSEDES_CONTRACT_ID,
        "cohort_start_utc": iso(COHORT_START),
        "cohorts": COHORTS,
        "generated_utc": iso(observed),
        "observation_clock": clock,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "official_numeric_events_loaded": len(events),
        "prospective_events_loaded": sum(
            int(bool(event.get("prospective_eligible"))) for event in events
        ),
        "events_by_currency": by_currency,
        "supported_currencies": supported_currencies,
        "inserted_events": inserted_events,
        "inserted_entries": inserted_entries,
        "matured_entries": matured,
        "entry_status_counts": status_counts,
        "prospective_setups": setups,
        "policy": {
            "two_sided_without_causal_consensus": True,
            "stabilization_clock": "numeric_causal_known_utc",
            "one_currency_factor_per_event": True,
            "minimum_pair_legs": 2,
            "maximum_confirmation_legs": 4,
            "maximum_causal_ingest_latency_minutes": MAX_CAUSAL_INGEST_LATENCY_MINUTES,
            "frozen_usd_cohort_unchanged": True,
        },
    }
    atomic_json(output_path, payload)
    atomic_text(report_path, render_report(payload))
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="cycle_complete",
            events_loaded=len(events),
            inserted_entries=inserted_entries,
            matured_entries=matured,
        )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT)
    args = parser.parse_args()
    started = time.monotonic()
    with WorkerHeartbeat(
        args.heartbeat,
        worker="currency_macro_release_breakout_research",
        role="research_only",
        interval_sec=5.0,
    ) as heartbeat:
        while True:
            heartbeat.update(phase="starting_cycle")
            run_once(heartbeat=heartbeat)
            if args.once or args.duration_sec <= 0:
                return 0
            if time.monotonic() - started >= args.duration_sec:
                return 0
            heartbeat.update(phase="sleeping")
            time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
