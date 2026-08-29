#!/usr/bin/env python3
"""Causal source-factor response map V2 (research only).

V2 is a bounded successor to the sealed V1 ledger.  It keeps V1's source
identity, knowledge-time, episode-deduplication, currency-state, executable
bid/ask, prequential-training, and fail-closed safety contracts unchanged and
adds one fixed ten-minute response horizon.  V1 code and evidence remain
immutable historical artifacts.

This module has no broker, order, lifecycle, authorization, promotion, or
canonical-watchlist surface.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator, Mapping

import oanda_causal_source_factor_response_map_v1 as v1


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
REPORT_ROOT = DATA / "reports" / "causal_source_factor_response"

INPUT_MAPPING_DATABASE = v1.INPUT_MAPPING_DATABASE
INPUT_RAW_DATABASE = v1.INPUT_RAW_DATABASE
CANDLE_ROOT = v1.CANDLE_ROOT
QUOTE_PATH = v1.QUOTE_PATH
TECHNICAL_PATH = v1.TECHNICAL_PATH

OUTPUT_DATABASE = LOCAL_NEWS / "causal_source_factor_response_map_v2.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "causal_source_factor_response_map_latest_v2.json"
REPORT_PATH = REPORT_ROOT / "CAUSAL_SOURCE_FACTOR_RESPONSE_MAP_V2.md"

SCHEMA_VERSION = "causal_source_factor_response_map_v2"
CONTRACT_ID = "causal_source_factor_response_map_v2_point_in_time_episode_20260828"
COHORT_ID = "causal_source_factor_response_map_v2_prospective_20260828T030000Z"
PARENT_CONTRACT_ID = v1.CONTRACT_ID
# The boundary is later than implementation and focused validation.  Inputs
# observed before it are retained as diagnostics but can never become proof.
ACTIVATED_UTC = datetime(2026, 8, 28, 3, 0, tzinfo=timezone.utc)
HORIZONS_MIN = (1, 5, 10, 15, 30, 60, 120)

POLICY = {
    **v1.POLICY,
    "parent_contract_id": PARENT_CONTRACT_ID,
    "material_change": "fixed_10_minute_response_horizon_added",
    "v1_code_and_evidence_preserved": True,
}

CandlePoint = v1.CandlePoint
PairObservation = v1.PairObservation
iso = v1.iso
parse_time = v1.parse_time
pip_size = v1.pip_size

_V1_OPEN_OUTPUT_DATABASE = v1.open_output_database
_V1_RENDER_REPORT = v1.render_report
_V1_RUN_CYCLE = v1.run_cycle


def _normalize_schema_sql(value: str) -> str:
    return "".join(str(value or "").lower().split())


def _ensure_v2_horizon_schema(connection: sqlite3.Connection) -> None:
    """Replace only empty V1-shaped horizon tables in a new V2 database.

    ``v1.open_output_database`` constructs the shared append-only event and
    factor schema.  A newly created V2 database initially has V1's three
    horizon CHECK constraints, so those empty child tables are replaced once.
    Existing rows are never migrated, rewritten, or deleted.  A non-empty
    incompatible database fails closed.
    """

    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' "
        "AND name='source_event_response'"
    ).fetchone()
    schema = _normalize_schema_sql(row[0] if row else "")
    if "horizon_minin(1,5,10,15,30,60,120)" in schema:
        return

    tables = (
        "source_event_response",
        "source_response_terminal_gap",
        "source_factor_forecast",
    )
    counts = {
        table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in tables
    }
    if any(counts.values()):
        raise RuntimeError(
            "incompatible_nonempty_v2_horizon_schema:" + json.dumps(counts, sort_keys=True)
        )

    connection.executescript(
        """
        DROP TRIGGER IF EXISTS source_event_response_append_only_update;
        DROP TRIGGER IF EXISTS source_event_response_append_only_delete;
        DROP TRIGGER IF EXISTS source_response_terminal_gap_append_only_update;
        DROP TRIGGER IF EXISTS source_response_terminal_gap_append_only_delete;
        DROP TRIGGER IF EXISTS source_factor_forecast_append_only_update;
        DROP TRIGGER IF EXISTS source_factor_forecast_append_only_delete;
        DROP INDEX IF EXISTS idx_source_response_maturity;
        DROP TABLE source_event_response;
        DROP TABLE source_response_terminal_gap;
        DROP TABLE source_factor_forecast;

        CREATE TABLE source_event_response (
            response_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,10,15,30,60,120)),
            event_clock_utc TEXT NOT NULL,
            maturity_utc TEXT NOT NULL,
            maturity_state TEXT NOT NULL,
            currency_factor_bps REAL NOT NULL,
            absolute_currency_factor_bps REAL NOT NULL,
            currency_rank INTEGER NOT NULL,
            factor_currency_count INTEGER NOT NULL,
            usable_pair_count INTEGER NOT NULL,
            currency_pair_path_count INTEGER NOT NULL,
            entry_method TEXT NOT NULL,
            response_timing_quality TEXT NOT NULL,
            early_currency_factor_bps REAL,
            response_shape TEXT NOT NULL,
            solver_status TEXT NOT NULL,
            solver_condition_number REAL,
            solver_weighted_observation_equivalent REAL,
            solver_diagnostics_json TEXT NOT NULL,
            both_executable_paths_json TEXT NOT NULL,
            selected_side TEXT CHECK(selected_side IS NULL),
            response_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(canonical_event_id,horizon_min),
            FOREIGN KEY(canonical_event_id)
                REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE source_response_terminal_gap (
            gap_id TEXT PRIMARY KEY,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,10,15,30,60,120)),
            target_utc TEXT NOT NULL,
            recorded_utc TEXT NOT NULL,
            reason TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(canonical_event_id,horizon_min),
            FOREIGN KEY(canonical_event_id)
                REFERENCES source_event_observation(canonical_event_id)
        );
        CREATE TABLE source_factor_forecast (
            forecast_id TEXT PRIMARY KEY,
            factor_observation_id TEXT NOT NULL,
            canonical_event_id TEXT NOT NULL,
            currency TEXT NOT NULL,
            factor_key TEXT NOT NULL,
            horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,10,15,30,60,120)),
            issued_utc TEXT NOT NULL,
            training_cutoff_utc TEXT NOT NULL,
            forecast_state TEXT NOT NULL,
            abstain_reason TEXT NOT NULL,
            backoff_level TEXT NOT NULL,
            backoff_key TEXT NOT NULL,
            raw_n INTEGER NOT NULL,
            effective_event_n INTEGER NOT NULL,
            probability_strengthening REAL,
            predicted_currency_factor_bps REAL,
            predicted_absolute_factor_bps REAL,
            training_latest_maturity_utc TEXT,
            forecast_payload_json TEXT NOT NULL,
            evidence_class TEXT NOT NULL,
            prospective_proof_eligible INTEGER NOT NULL
                CHECK(prospective_proof_eligible IN (0,1)),
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            can_promote INTEGER NOT NULL CHECK(can_promote=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            UNIQUE(factor_observation_id,horizon_min),
            FOREIGN KEY(factor_observation_id)
                REFERENCES source_factor_observation(factor_observation_id)
        );
        CREATE INDEX idx_source_response_maturity
            ON source_event_response(horizon_min,maturity_utc);

        CREATE TRIGGER source_event_response_append_only_update
        BEFORE UPDATE ON source_event_response BEGIN
          SELECT RAISE(ABORT,'append_only:source_event_response');
        END;
        CREATE TRIGGER source_event_response_append_only_delete
        BEFORE DELETE ON source_event_response BEGIN
          SELECT RAISE(ABORT,'append_only:source_event_response');
        END;
        CREATE TRIGGER source_response_terminal_gap_append_only_update
        BEFORE UPDATE ON source_response_terminal_gap BEGIN
          SELECT RAISE(ABORT,'append_only:source_response_terminal_gap');
        END;
        CREATE TRIGGER source_response_terminal_gap_append_only_delete
        BEFORE DELETE ON source_response_terminal_gap BEGIN
          SELECT RAISE(ABORT,'append_only:source_response_terminal_gap');
        END;
        CREATE TRIGGER source_factor_forecast_append_only_update
        BEFORE UPDATE ON source_factor_forecast BEGIN
          SELECT RAISE(ABORT,'append_only:source_factor_forecast');
        END;
        CREATE TRIGGER source_factor_forecast_append_only_delete
        BEFORE DELETE ON source_factor_forecast BEGIN
          SELECT RAISE(ABORT,'append_only:source_factor_forecast');
        END;
        """
    )
    connection.commit()


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    connection = _V1_OPEN_OUTPUT_DATABASE(path)
    try:
        _ensure_v2_horizon_schema(connection)
    except Exception:
        connection.close()
        raise
    return connection


def _render_report_v2(snapshot: Mapping[str, Any]) -> str:
    report = _V1_RENDER_REPORT(snapshot)
    report = report.replace(
        "# Causal Source-Factor Response Map V1",
        "# Causal Source-Factor Response Map V2",
        1,
    )
    return report.replace(
        "This is an isolated research ledger.",
        "This is an isolated V2 research ledger; V1 remains preserved and inactive.",
        1,
    )


@contextmanager
def _v2_contract() -> Iterator[None]:
    replacements = {
        "SCHEMA_VERSION": SCHEMA_VERSION,
        "CONTRACT_ID": CONTRACT_ID,
        "COHORT_ID": COHORT_ID,
        "ACTIVATED_UTC": ACTIVATED_UTC,
        "HORIZONS_MIN": HORIZONS_MIN,
        "POLICY": POLICY,
        "OUTPUT_DATABASE": OUTPUT_DATABASE,
        "SNAPSHOT_PATH": SNAPSHOT_PATH,
        "REPORT_PATH": REPORT_PATH,
        "open_output_database": open_output_database,
        "render_report": _render_report_v2,
    }
    previous = {name: getattr(v1, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(v1, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(v1, name, value)


def compute_event_response(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    with _v2_contract():
        return v1.compute_event_response(*args, **kwargs)


def insert_events(connection: sqlite3.Connection, events: Any) -> dict[str, int]:
    with _v2_contract():
        return v1.insert_events(connection, events)


def insert_responses(connection: sqlite3.Connection, rows: Any) -> int:
    with _v2_contract():
        return v1.insert_responses(connection, rows)


def build_prequential_forecast(
    connection: sqlite3.Connection,
    factor: Mapping[str, Any],
    horizon_min: int,
    *,
    minimum_n: int = v1.MIN_EFFECTIVE_N,
) -> dict[str, Any]:
    with _v2_contract():
        return v1.build_prequential_forecast(
            connection, factor, horizon_min, minimum_n=minimum_n
        )


def run_cycle(
    *,
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    raw_database: Path = INPUT_RAW_DATABASE,
    candle_root: Path = CANDLE_ROOT,
    output_database: Path = OUTPUT_DATABASE,
    snapshot_path: Path = SNAPSHOT_PATH,
    report_path: Path = REPORT_PATH,
    quote_path: Path = QUOTE_PATH,
    technical_path: Path = TECHNICAL_PATH,
    observed_utc: datetime | None = None,
) -> dict[str, Any]:
    with _v2_contract():
        return _V1_RUN_CYCLE(
            mapping_database=mapping_database,
            raw_database=raw_database,
            candle_root=candle_root,
            output_database=output_database,
            snapshot_path=snapshot_path,
            report_path=report_path,
            quote_path=quote_path,
            technical_path=technical_path,
            observed_utc=observed_utc,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapping-database", type=Path, default=INPUT_MAPPING_DATABASE)
    parser.add_argument("--raw-database", type=Path, default=INPUT_RAW_DATABASE)
    parser.add_argument("--candle-root", type=Path, default=CANDLE_ROOT)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--quote-path", type=Path, default=QUOTE_PATH)
    parser.add_argument("--technical-path", type=Path, default=TECHNICAL_PATH)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        snapshot = run_cycle(
            mapping_database=args.mapping_database,
            raw_database=args.raw_database,
            candle_root=args.candle_root,
            output_database=args.output_database,
            snapshot_path=args.snapshot,
            report_path=args.report,
            quote_path=args.quote_path,
            technical_path=args.technical_path,
        )
        print(
            json.dumps(
                {
                    "generated_utc": snapshot["generated_utc"],
                    "canonical_events": snapshot["counts"]["canonical_events"],
                    "prospective_proof_events": snapshot["counts"]["prospective_proof_events"],
                    "responses": snapshot["counts"]["responses"],
                    "forecasts": snapshot["counts"]["forecasts"],
                    "inserted": snapshot["inserted"],
                    "pending_maturities_processed": snapshot["pending_maturities_processed"],
                    "sqlite_integrity": snapshot["sqlite_integrity"],
                    "policy": snapshot["policy"],
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            flush=True,
        )
        if args.duration_sec <= 0.0 or time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACTIVATED_UTC",
    "COHORT_ID",
    "CONTRACT_ID",
    "HORIZONS_MIN",
    "OUTPUT_DATABASE",
    "POLICY",
    "REPORT_PATH",
    "SCHEMA_VERSION",
    "SNAPSHOT_PATH",
    "build_prequential_forecast",
    "compute_event_response",
    "insert_events",
    "insert_responses",
    "open_output_database",
    "run_cycle",
]
