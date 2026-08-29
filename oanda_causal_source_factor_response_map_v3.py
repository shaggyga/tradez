#!/usr/bin/env python3
"""Causal source-factor response map V3 (research only).

V3 supersedes, but never mutates or imports evidence from, the sealed V2
ledger.  It preserves V2's fixed 1/5/10/15/30/60/120-minute response contract
and opens a new prospective cohort for classifier V148 plus the independently
revalidated pre-semantic executable-quote bundle.  Older V1/V2 databases,
snapshots, reports, code, and cohort identities remain immutable diagnostics.

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
import oanda_causal_source_factor_response_map_v2 as v2


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
REPORT_ROOT = DATA / "reports" / "causal_source_factor_response"

INPUT_MAPPING_DATABASE = v1.INPUT_MAPPING_DATABASE
INPUT_RAW_DATABASE = v1.INPUT_RAW_DATABASE
CANDLE_ROOT = v1.CANDLE_ROOT
QUOTE_PATH = v1.QUOTE_PATH
TECHNICAL_PATH = v1.TECHNICAL_PATH

OUTPUT_DATABASE = LOCAL_NEWS / "causal_source_factor_response_map_v3.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "causal_source_factor_response_map_latest_v3.json"
REPORT_PATH = REPORT_ROOT / "CAUSAL_SOURCE_FACTOR_RESPONSE_MAP_V3.md"

SCHEMA_VERSION = "causal_source_factor_response_map_v3"
CONTRACT_ID = (
    "causal_source_factor_response_map_v3_v148_pre_semantic_quote_20260828"
)
COHORT_ID = "causal_source_factor_response_map_v3_prospective_20260828T070000Z"
PARENT_CONTRACT_ID = v2.CONTRACT_ID
REQUIRED_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260828_v148_official_research_intervention_boundary"
)
REQUIRED_MAPPER_CONTRACT = v1.REQUIRED_MAPPER_CONTRACT
REQUIRED_PRE_MAP_QUOTE_CONTRACT = v1.REQUIRED_PRE_MAP_QUOTE_CONTRACT
# This boundary is later than the V3 implementation and focused validation.
# V148 rows from the earlier mapper probe and all V2-era observations remain
# diagnostic and can never be relabelled into this prospective cohort.
ACTIVATED_UTC = datetime(2026, 8, 28, 7, 0, tzinfo=timezone.utc)
HORIZONS_MIN = v2.HORIZONS_MIN

POLICY = {
    **v1.POLICY,
    "parent_contract_id": PARENT_CONTRACT_ID,
    "material_change": (
        "classifier_v148_and_pre_semantic_quote_capture_new_prospective_cohort"
    ),
    "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
    "required_pre_map_quote_contract": REQUIRED_PRE_MAP_QUOTE_CONTRACT,
    "v1_code_and_evidence_preserved": True,
    "v2_code_and_evidence_preserved": True,
    "v2_rows_imported": False,
}

CandlePoint = v1.CandlePoint
PairObservation = v1.PairObservation
iso = v1.iso
parse_time = v1.parse_time
pip_size = v1.pip_size

_V1_LOAD_CURRENT_OBSERVATIONS = v1.load_current_observations
_V1_CANONICALIZE_OBSERVATIONS = v1.canonicalize_observations
_V1_RENDER_REPORT = v1.render_report
_V1_RUN_CYCLE = v1.run_cycle


def load_current_observations(
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    raw_database: Path = INPUT_RAW_DATABASE,
    **_: Any,
) -> list[dict[str, Any]]:
    """Read only the exact V148 mapper contract into the V3 cohort."""

    return _V1_LOAD_CURRENT_OBSERVATIONS(
        mapping_database,
        raw_database,
        required_classifier=REQUIRED_CLASSIFICATION_VERSION,
        required_mapper_contract=REQUIRED_MAPPER_CONTRACT,
    )


def canonicalize_observations(
    observations: Any,
    **_: Any,
) -> list[dict[str, Any]]:
    """Apply the V3 activation boundary, never V1's frozen default value."""

    return _V1_CANONICALIZE_OBSERVATIONS(
        observations,
        activation_utc=ACTIVATED_UTC,
    )


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    # V3 keeps V2's fixed horizon schema but opens it only in a new database.
    return v2.open_output_database(path)


def _render_report_v3(snapshot: Mapping[str, Any]) -> str:
    report = _V1_RENDER_REPORT(snapshot)
    report = report.replace(
        "# Causal Source-Factor Response Map V1",
        "# Causal Source-Factor Response Map V3",
        1,
    )
    return report.replace(
        "This is an isolated research ledger.",
        (
            "This is an isolated V3 research ledger; V1 and V2 remain "
            "preserved and inactive."
        ),
        1,
    )


@contextmanager
def _v3_contract() -> Iterator[None]:
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
        "REQUIRED_CLASSIFICATION_VERSION": REQUIRED_CLASSIFICATION_VERSION,
        "REQUIRED_MAPPER_CONTRACT": REQUIRED_MAPPER_CONTRACT,
        "REQUIRED_PRE_MAP_QUOTE_CONTRACT": REQUIRED_PRE_MAP_QUOTE_CONTRACT,
        "load_current_observations": load_current_observations,
        "canonicalize_observations": canonicalize_observations,
        "open_output_database": open_output_database,
        "render_report": _render_report_v3,
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
    with _v3_contract():
        return v1.compute_event_response(*args, **kwargs)


def insert_events(connection: sqlite3.Connection, events: Any) -> dict[str, int]:
    with _v3_contract():
        return v1.insert_events(connection, events)


def insert_responses(connection: sqlite3.Connection, rows: Any) -> int:
    with _v3_contract():
        return v1.insert_responses(connection, rows)


def build_prequential_forecast(
    connection: sqlite3.Connection,
    factor: Mapping[str, Any],
    horizon_min: int,
    *,
    minimum_n: int = v1.MIN_EFFECTIVE_N,
) -> dict[str, Any]:
    with _v3_contract():
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
    with _v3_contract():
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
                    "prospective_proof_events": snapshot["counts"][
                        "prospective_proof_events"
                    ],
                    "responses": snapshot["counts"]["responses"],
                    "forecasts": snapshot["counts"]["forecasts"],
                    "inserted": snapshot["inserted"],
                    "pending_maturities_processed": snapshot[
                        "pending_maturities_processed"
                    ],
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
    "REQUIRED_CLASSIFICATION_VERSION",
    "SCHEMA_VERSION",
    "SNAPSHOT_PATH",
    "build_prequential_forecast",
    "canonicalize_observations",
    "compute_event_response",
    "insert_events",
    "insert_responses",
    "load_current_observations",
    "open_output_database",
    "run_cycle",
]
