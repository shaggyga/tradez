#!/usr/bin/env python3
"""Causal source-factor response map V7 (research only).

V7 is the clean prospective cohort for classifier V152.  V152 binds an
external policy recommendation to the authority currency named in the target
clause and binds secondary numeric releases to the release country before any
body text is interpreted.  V1-V6 code and evidence remain immutable
diagnostics; no prior row is copied or relabelled into V7.
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

import oanda_causal_source_factor_response_map_v6 as v6


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
REPORT_ROOT = DATA / "reports" / "causal_source_factor_response"

INPUT_MAPPING_DATABASE = v6.INPUT_MAPPING_DATABASE
INPUT_RAW_DATABASE = v6.INPUT_RAW_DATABASE
CANDLE_ROOT = v6.CANDLE_ROOT
QUOTE_PATH = v6.QUOTE_PATH
TECHNICAL_PATH = v6.TECHNICAL_PATH

OUTPUT_DATABASE = LOCAL_NEWS / "causal_source_factor_response_map_v7.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "causal_source_factor_response_map_latest_v7.json"
REPORT_PATH = REPORT_ROOT / "CAUSAL_SOURCE_FACTOR_RESPONSE_MAP_V7.md"

SCHEMA_VERSION = "causal_source_factor_response_map_v7"
CONTRACT_ID = (
    "causal_source_factor_response_map_v7_"
    "v152_subject_bound_release_policy_targets_20260901"
)
COHORT_ID = "causal_source_factor_response_map_v7_prospective_20260901T023000Z"
PARENT_CONTRACT_ID = v6.CONTRACT_ID
REQUIRED_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260901_v152_subject_bound_release_policy_targets"
)
REQUIRED_MAPPER_CONTRACT = v6.REQUIRED_MAPPER_CONTRACT
REQUIRED_PRE_MAP_QUOTE_CONTRACT = v6.REQUIRED_PRE_MAP_QUOTE_CONTRACT
# Deliberately after implementation and validation.  V6 remains sealed.
ACTIVATED_UTC = datetime(2026, 9, 1, 2, 30, tzinfo=timezone.utc)
HORIZONS_MIN = v6.HORIZONS_MIN

PROOF_ELIGIBILITY_CONTRACT = v6.PROOF_ELIGIBILITY_CONTRACT
POLICY = {
    **v6.POLICY,
    "parent_contract_id": PARENT_CONTRACT_ID,
    "material_change": "classifier_v152_subject_bound_release_policy_targets",
    "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
    "policy_target_currency_bound_before_semantic_scoring": True,
    "secondary_release_country_bound_before_body_scoring": True,
    "mentioned_entities_retained_for_audit_not_direction": True,
    "v6_code_and_evidence_preserved": True,
    "v6_rows_imported": False,
}

iso = v6.iso
parse_time = v6.parse_time
pip_size = v6.pip_size
CandlePoint = v6.CandlePoint
PairObservation = v6.PairObservation

_BASE_LOAD_CURRENT_OBSERVATIONS = v6._V1_LOAD_CURRENT_OBSERVATIONS
_BASE_CANONICALIZE_OBSERVATIONS = v6._V1_CANONICALIZE_OBSERVATIONS
_BASE_OPEN_OUTPUT_DATABASE = v6.open_output_database
_BASE_RUN_CYCLE = v6.run_cycle


def load_current_observations(
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    raw_database: Path = INPUT_RAW_DATABASE,
    **_: Any,
) -> list[dict[str, Any]]:
    """Read only exact V152 mappings; never fall back to an older cohort."""

    return _BASE_LOAD_CURRENT_OBSERVATIONS(
        mapping_database,
        raw_database,
        required_classifier=REQUIRED_CLASSIFICATION_VERSION,
        required_mapper_contract=REQUIRED_MAPPER_CONTRACT,
    )


def canonicalize_observations(
    observations: Any,
    **_: Any,
) -> list[dict[str, Any]]:
    """Retain V152 diagnostics while admitting only valid post-cutover proof."""

    events = _BASE_CANONICALIZE_OBSERVATIONS(
        observations,
        activation_utc=ACTIVATED_UTC,
    )
    for event in events:
        first_seen = parse_time(event.get("first_known_utc"))
        after_activation = bool(
            first_seen is not None and first_seen >= ACTIVATED_UTC
        )
        exclusions = v6.v5._classifier_proof_exclusions(event)
        eligible = bool(after_activation and not exclusions)
        event["prospective_proof_eligible"] = eligible
        event["proof_eligibility_contract"] = PROOF_ELIGIBILITY_CONTRACT
        event["proof_exclusion_reasons"] = exclusions
        if after_activation:
            event["evidence_class"] = (
                "prospective_v7"
                if eligible
                else "prospective_v7_excluded_diagnostic"
            )
        source_facts = event.get("source_facts")
        if isinstance(source_facts, dict):
            transports = event.get("transport_observations") or []
            primary = (
                transports[0]
                if transports and isinstance(transports[0], Mapping)
                else {}
            )
            payload = (
                primary.get("mapping_payload")
                if isinstance(primary, Mapping)
                else {}
            )
            if isinstance(payload, Mapping):
                source_facts["relevant"] = payload.get("relevant") is True
                source_facts["exclusion_reason"] = str(
                    payload.get("exclusion_reason") or ""
                ).strip()
    return events


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    return _BASE_OPEN_OUTPUT_DATABASE(path)


def _render_report_v7(snapshot: Mapping[str, Any]) -> str:
    report = v6._V1_RENDER_REPORT(snapshot)
    report = report.replace(
        "# Causal Source-Factor Response Map V1",
        "# Causal Source-Factor Response Map V7",
        1,
    )
    return report.replace(
        "This is an isolated research ledger.",
        (
            "This is an isolated V7 research ledger; V1-V6 cohorts remain "
            "preserved diagnostics and are never imported."
        ),
        1,
    )


@contextmanager
def _v7_contract() -> Iterator[None]:
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
        "_render_report_v6": _render_report_v7,
    }
    previous = {name: getattr(v6, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(v6, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(v6, name, value)


def compute_event_response(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    with _v7_contract():
        return v6.compute_event_response(*args, **kwargs)


def insert_events(connection: sqlite3.Connection, events: Any) -> dict[str, int]:
    with _v7_contract():
        return v6.insert_events(connection, events)


def insert_responses(connection: sqlite3.Connection, rows: Any) -> int:
    with _v7_contract():
        return v6.insert_responses(connection, rows)


def build_prequential_forecast(
    connection: sqlite3.Connection,
    factor: Mapping[str, Any],
    horizon_min: int,
    *,
    minimum_n: int = v6.v1.MIN_EFFECTIVE_N,
) -> dict[str, Any]:
    with _v7_contract():
        return v6.build_prequential_forecast(
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
    with _v7_contract():
        return _BASE_RUN_CYCLE(
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
        print(json.dumps(snapshot, sort_keys=True, separators=(",", ":")), flush=True)
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
    "PROOF_ELIGIBILITY_CONTRACT",
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
