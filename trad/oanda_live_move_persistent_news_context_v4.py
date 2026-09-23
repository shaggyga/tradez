#!/usr/bin/env python3
"""V7R2-bound declared-horizon context companion.

The live V3R2 persistent-context worker remains frozen.  This module is a
parallel research cohort that consumes only the exact V7R2 mover contract and
its fixed-onset factor-episode contract.  Contract, integrity, and inertness
checks fail closed before any mover is enriched.  Each observation is also
written to a separate append-only SQLite ledger.

This module has no broker, authorization, lifecycle, promotion, or execution
surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import oanda_live_move_news_snapshot_v7 as live_v7
import oanda_live_move_persistent_news_context as base
import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
REPORTS = DATA / "reports"

DEFAULT_SNAPSHOT = live_v7.DEFAULT_OUTPUT
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_OUTPUT = STATE / "live_move_persistent_news_context_v4r2.json"
DEFAULT_HISTORY = STATE / "live_move_persistent_news_context_v4r2.sqlite"
DEFAULT_REPORT = (
    REPORTS / "live_move_news" / "LIVE_MOVE_PERSISTENT_CONTEXT_V4R2.md"
)

SCHEMA_VERSION = 4
CONTRACT_ID = (
    "live_move_persistent_news_context_v4r2_exact_v7r2_"
    "factor_episode_binding_20260827"
)
CONTRACT_ACTIVATED_UTC = dt.datetime(
    2026, 8, 27, 8, 47, tzinfo=dt.timezone.utc
)
EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID = live_v7.CONTRACT_ID
EXPECTED_FACTOR_EPISODE_CONTRACT_ID = live_v7.FACTOR_EPISODE_CONTRACT_ID
EXPECTED_NARRATIVE_METER_CONTRACT_ID = live_v7.METER_CONTRACT_ID


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_input_snapshot(source: Mapping[str, Any]) -> list[str]:
    """Return exact-contract violations; an empty result is acceptance."""

    reasons: list[str] = []

    def require(condition: bool, reason: str) -> None:
        if not condition:
            reasons.append(reason)

    movers = source.get("movers")
    mover_rows = (
        [row for row in movers if isinstance(row, Mapping)]
        if isinstance(movers, list)
        else []
    )
    factor = source.get("factor_episode_contract")
    factor = factor if isinstance(factor, Mapping) else {}
    narrative = source.get("narrative_join_contract")
    narrative = narrative if isinstance(narrative, Mapping) else {}

    require(
        source.get("contract_id") == EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
        "snapshot_contract_mismatch",
    )
    require(source.get("schema_version") == live_v7.SCHEMA_VERSION, "schema_mismatch")
    require(
        source.get("previous_contract_id") == live_v7.PREVIOUS_CONTRACT_ID,
        "previous_snapshot_contract_mismatch",
    )
    require(bool(source.get("generated_utc")), "missing_snapshot_generated_utc")
    require(
        factor.get("contract_id") == EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
        "factor_episode_contract_mismatch",
    )
    require(
        factor.get("grouping_method")
        == "same_primary_interval_overlap_or_adjacency",
        "factor_episode_grouping_mismatch",
    )
    require(
        factor.get("maximum_gap_sec") == live_v7.MAXIMUM_EPISODE_GAP_SEC,
        "factor_episode_gap_mismatch",
    )
    require(
        factor.get("primary_factor_classification_minutes")
        == live_v7.FACTOR_CLASSIFICATION_MINUTES,
        "factor_classification_cutoff_mismatch",
    )
    require(
        factor.get("centered_time_buckets_used") is False,
        "centered_bucket_contract_not_disabled",
    )
    require(
        factor.get("input_order_invariant") is True,
        "factor_input_order_invariance_missing",
    )
    require(
        source.get("factor_episode_integrity_ok") is True,
        "factor_episode_integrity_failed",
    )
    require(
        source.get("factor_episode_membership_conflict_total") == 0,
        "factor_episode_membership_conflict",
    )
    require(
        source.get("factor_episode_merge_conflict_count") == 0,
        "factor_episode_merge_conflict",
    )
    require(
        narrative.get("meter_contract_id")
        == EXPECTED_NARRATIVE_METER_CONTRACT_ID,
        "narrative_meter_contract_mismatch",
    )
    require(
        narrative.get("partial_live_excluded") is True,
        "partial_narrative_not_excluded",
    )
    require(
        narrative.get("sealed_v12_database_only") is True,
        "narrative_database_not_sealed_v12_only",
    )
    require(source.get("research_only") is True, "upstream_not_research_only")
    require(
        source.get("execution_eligible") is False,
        "upstream_execution_eligible",
    )
    require(source.get("can_place_orders") is False, "upstream_can_place_orders")
    require(source.get("can_promote") is False, "upstream_can_promote")
    require(source.get("broker_access") is False, "upstream_broker_access")
    require(
        source.get("supported_decision") == "diagnostic_only",
        "upstream_decision_not_diagnostic_only",
    )
    require(isinstance(movers, list), "movers_not_list")
    require(
        not isinstance(movers, list) or len(movers) == len(mover_rows),
        "mover_rows_not_objects",
    )
    require(source.get("mover_count") == len(mover_rows), "mover_count_mismatch")

    episode_ids: set[str] = set()
    representative_count = 0
    for index, row in enumerate(mover_rows):
        prefix = f"mover_{index}"
        require(
            row.get("contract_id") == EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
            f"{prefix}_contract_mismatch",
        )
        require(bool(row.get("case_id")), f"{prefix}_missing_case_id")
        require(
            bool(row.get("factor_primary_token")),
            f"{prefix}_missing_primary_factor",
        )
        episode_id = str(row.get("factor_episode_id") or "")
        require(bool(episode_id), f"{prefix}_missing_factor_episode_id")
        if episode_id:
            episode_ids.add(episode_id)
        representative_count += int(row.get("factor_representative") is True)
        require(
            row.get("factor_episode_membership_conflict") is False,
            f"{prefix}_membership_conflict",
        )
        require(
            row.get("factor_episode_merge_conflict") is False,
            f"{prefix}_merge_conflict",
        )

    require(
        source.get("factor_episode_count") == len(episode_ids),
        "factor_episode_count_mismatch",
    )
    require(
        source.get("factor_representative_count") == representative_count,
        "factor_representative_count_mismatch",
    )
    require(
        representative_count == len(episode_ids),
        "factor_episode_representative_invariant_failed",
    )
    return sorted(set(reasons))


def connect_history(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS context_contract_registry(
            contract_id TEXT PRIMARY KEY,
            activated_utc TEXT NOT NULL,
            required_snapshot_contract_id TEXT NOT NULL,
            required_factor_episode_contract_id TEXT NOT NULL,
            required_narrative_meter_contract_id TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
            broker_access INTEGER NOT NULL CHECK(broker_access=0),
            created_utc TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS context_contract_registry_no_update
            BEFORE UPDATE ON context_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS context_contract_registry_no_delete
            BEFORE DELETE ON context_contract_registry
            BEGIN SELECT RAISE(ABORT,'append_only'); END;

        CREATE TABLE IF NOT EXISTS context_observations(
            observation_id TEXT PRIMARY KEY,
            context_contract_id TEXT NOT NULL,
            generated_utc TEXT NOT NULL,
            input_snapshot_generated_utc TEXT,
            input_snapshot_sha256 TEXT NOT NULL,
            input_integrity_ok INTEGER NOT NULL CHECK(input_integrity_ok IN (0,1)),
            rejection_reasons_json TEXT NOT NULL,
            mover_count INTEGER NOT NULL,
            source_history_highwater_utc TEXT,
            payload_sha256 TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
            broker_access INTEGER NOT NULL CHECK(broker_access=0),
            FOREIGN KEY(context_contract_id)
                REFERENCES context_contract_registry(contract_id)
        );
        CREATE TRIGGER IF NOT EXISTS context_observations_no_update
            BEFORE UPDATE ON context_observations
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS context_observations_no_delete
            BEFORE DELETE ON context_observations
            BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


def observation_id(
    *, generated_utc: str, input_snapshot_sha256: str
) -> str:
    material = "|".join((CONTRACT_ID, generated_utc, input_snapshot_sha256))
    return "persistent_context_v4_" + _sha256_text(material)[:24]


def record_observation(
    path: Path,
    payload: Mapping[str, Any],
    *,
    input_snapshot_sha256: str,
) -> dict[str, Any]:
    payload_json = _canonical_json(payload)
    payload_sha256 = _sha256_text(payload_json)
    identity = str(payload.get("history_observation_id") or "")
    connection = connect_history(path)
    inserted = 0
    try:
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO context_contract_registry VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    CONTRACT_ID,
                    CONTRACT_ACTIVATED_UTC.isoformat(),
                    EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
                    EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
                    EXPECTED_NARRATIVE_METER_CONTRACT_ID,
                    1,
                    0,
                    0,
                    0,
                    str(payload.get("generated_utc") or utc_now()),
                ),
            )
            registry = connection.execute(
                "SELECT required_snapshot_contract_id,"
                "required_factor_episode_contract_id,"
                "required_narrative_meter_contract_id,research_only,"
                "execution_eligible,can_place_orders,broker_access "
                "FROM context_contract_registry WHERE contract_id=?",
                (CONTRACT_ID,),
            ).fetchone()
            expected_registry = (
                EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID,
                EXPECTED_FACTOR_EPISODE_CONTRACT_ID,
                EXPECTED_NARRATIVE_METER_CONTRACT_ID,
                1,
                0,
                0,
                0,
            )
            if registry != expected_registry:
                raise sqlite3.IntegrityError("context_contract_registry_mismatch")
            existing = connection.execute(
                "SELECT payload_sha256 FROM context_observations WHERE observation_id=?",
                (identity,),
            ).fetchone()
            if existing is not None and str(existing[0]) != payload_sha256:
                raise sqlite3.IntegrityError(
                    "observation_identity_payload_mismatch"
                )
            before = connection.total_changes
            connection.execute(
                "INSERT OR IGNORE INTO context_observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    CONTRACT_ID,
                    str(payload.get("generated_utc") or ""),
                    str(payload.get("input_snapshot_generated_utc") or ""),
                    input_snapshot_sha256,
                    int(bool(payload.get("input_integrity_ok"))),
                    json.dumps(
                        payload.get("input_rejection_reasons") or [],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    int(payload.get("mover_count") or 0),
                    str(payload.get("source_history_highwater_utc") or ""),
                    payload_sha256,
                    payload_json,
                    1,
                    0,
                    0,
                    0,
                ),
            )
            inserted = int(connection.total_changes > before)
        total = int(
            connection.execute("SELECT COUNT(*) FROM context_observations").fetchone()[0]
        )
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    if integrity != "ok":
        raise sqlite3.IntegrityError(f"context history integrity:{integrity}")
    return {
        "inserted": inserted,
        "total": total,
        "integrity": integrity,
        "payload_sha256": payload_sha256,
    }


def render(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Live mover persistent-news context V4R2",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        (
            "Parallel research-only consumer, bound exactly to the V7R2 "
            "fixed-onset factor-episode snapshot. It cannot place orders."
        ),
        "",
        f"Input integrity: `{payload.get('input_integrity_ok')}`",
    ]
    reasons = payload.get("input_rejection_reasons") or []
    if reasons:
        lines.extend(("", "Rejected input:", ""))
        lines.extend(f"- `{reason}`" for reason in reasons)
        return "\n".join(lines) + "\n"
    lines.extend(("", *base.render(payload).splitlines()[6:]))
    return "\n".join(lines) + "\n"


def run(
    *,
    snapshot_path: Path = DEFAULT_SNAPSHOT,
    source_database: Path = DEFAULT_SOURCES,
    output_path: Path = DEFAULT_OUTPUT,
    history_path: Path = DEFAULT_HISTORY,
    report_path: Path = DEFAULT_REPORT,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    observed = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    generated = observed.isoformat()
    source = base.read_json(snapshot_path)
    source_json = _canonical_json(source)
    source_sha256 = _sha256_text(source_json)
    reasons = validate_input_snapshot(source)
    snapshot_generated_epoch = census.parse_epoch(source.get("generated_utc"))
    if source.get("generated_utc") and snapshot_generated_epoch is None:
        reasons.append("invalid_snapshot_generated_utc")
    elif (
        snapshot_generated_epoch is not None
        and snapshot_generated_epoch > int(observed.timestamp()) + 1
    ):
        reasons.append("snapshot_generated_in_future")
    rows: list[dict[str, Any]] = []
    source_highwater: str | None = None

    if not reasons:
        movers = [
            row for row in source.get("movers") or [] if isinstance(row, Mapping)
        ]
        starts = [census.parse_epoch(row.get("start_utc")) for row in movers]
        valid = [int(value) for value in starts if value is not None]
        lower = (
            min(valid) - base.MAXIMUM_LOOKBACK_MINUTES * 60
            if valid
            else int(observed.timestamp())
        )
        upper = max(valid) if valid else int(observed.timestamp())
        try:
            source_index, source_highwater = census.load_source_index(
                source_database,
                minimum_effective_epoch=lower,
                maximum_effective_epoch=upper,
            )
            rows = [base.enrich_case(row, source_index) for row in movers]
        except (OSError, sqlite3.DatabaseError) as exc:
            reasons.append(f"source_database_load_failed:{type(exc).__name__}")
            rows = []
            source_highwater = None

    integrity_ok = not reasons
    identity = observation_id(
        generated_utc=generated,
        input_snapshot_sha256=source_sha256,
    )
    output: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "contract_activated_utc": CONTRACT_ACTIVATED_UTC.isoformat(),
        "generated_utc": generated,
        "input_snapshot_path": str(snapshot_path.resolve()),
        "input_snapshot_sha256": source_sha256,
        "input_snapshot_contract_id": source.get("contract_id"),
        "expected_input_snapshot_contract_id": (
            EXPECTED_INPUT_SNAPSHOT_CONTRACT_ID
        ),
        "input_factor_episode_contract_id": (
            (source.get("factor_episode_contract") or {}).get("contract_id")
            if isinstance(source.get("factor_episode_contract"), Mapping)
            else None
        ),
        "expected_factor_episode_contract_id": (
            EXPECTED_FACTOR_EPISODE_CONTRACT_ID
        ),
        "input_snapshot_generated_utc": source.get("generated_utc"),
        "input_narrative_join_contract": source.get("narrative_join_contract"),
        "input_integrity_ok": integrity_ok,
        "input_rejection_reasons": sorted(set(reasons)),
        "source_history_highwater_utc": source_highwater,
        "maximum_lookback_minutes": base.MAXIMUM_LOOKBACK_MINUTES,
        "mover_count": len(rows),
        "movers": rows,
        "history_database": str(history_path.resolve()),
        "history_observation_id": identity,
        "history_append_only": True,
        "history_integrity_required": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "broker_access": False,
        "supported_decision": "diagnostic_only" if integrity_ok else "reject_input",
    }
    record_observation(
        history_path,
        output,
        input_snapshot_sha256=source_sha256,
    )
    live_v7.v5.atomic_json(output_path, output)
    live_v7.v5.atomic_text(report_path, render(output))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        result = run(
            snapshot_path=args.snapshot,
            source_database=args.sources,
            output_path=args.output,
            history_path=args.history,
            report_path=args.report,
        )
        print(
            json.dumps(
                {
                    "generated_utc": result["generated_utc"],
                    "input_integrity_ok": result["input_integrity_ok"],
                    "mover_count": result["mover_count"],
                    "supported_decision": result["supported_decision"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            return 0
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
