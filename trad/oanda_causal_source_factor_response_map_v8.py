#!/usr/bin/env python3
"""Causal source-factor response map V8 (research only).

V8 is a clean prospective taxonomy overlay on the frozen V152 classifier. It
separates the exact BOJ Bond Market Survey from stance-bearing policy releases
while retaining it as issuer-bound JPY market-structure evidence. V1-V7 code,
classification, and evidence remain immutable; no prior row is copied or
relabelled into V8.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Iterator, Mapping

import oanda_causal_source_factor_response_map_v7 as v7


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
LOCAL_NEWS = DATA / "local_news_sentiment"
REPORT_ROOT = DATA / "reports" / "causal_source_factor_response"

INPUT_MAPPING_DATABASE = v7.INPUT_MAPPING_DATABASE
INPUT_RAW_DATABASE = v7.INPUT_RAW_DATABASE
CANDLE_ROOT = v7.CANDLE_ROOT
QUOTE_PATH = v7.QUOTE_PATH
TECHNICAL_PATH = v7.TECHNICAL_PATH

OUTPUT_DATABASE = LOCAL_NEWS / "causal_source_factor_response_map_v8.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "causal_source_factor_response_map_latest_v8.json"
REPORT_PATH = REPORT_ROOT / "CAUSAL_SOURCE_FACTOR_RESPONSE_MAP_V8.md"

SCHEMA_VERSION = "causal_source_factor_response_map_v8"
CONTRACT_ID = (
    "causal_source_factor_response_map_v8_"
    "v152_boj_market_structure_survey_overlay_20260901"
)
COHORT_ID = "causal_source_factor_response_map_v8_prospective_20260901T114500Z"
PARENT_CONTRACT_ID = v7.CONTRACT_ID
REQUIRED_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260901_v152_subject_bound_release_policy_targets"
)
REQUIRED_MAPPER_CONTRACT = v7.REQUIRED_MAPPER_CONTRACT
REQUIRED_PRE_MAP_QUOTE_CONTRACT = v7.REQUIRED_PRE_MAP_QUOTE_CONTRACT
ACTIVATED_UTC = datetime(2026, 9, 1, 11, 45, tzinfo=timezone.utc)
HORIZONS_MIN = v7.HORIZONS_MIN

PROOF_ELIGIBILITY_CONTRACT = (
    "classifier_relevant_or_exact_boj_market_structure_overlay_v1_20260901"
)
POLICY = {
    **v7.POLICY,
    "parent_contract_id": PARENT_CONTRACT_ID,
    "material_change": "v152_exact_boj_market_structure_survey_taxonomy_overlay",
    "required_classification_version": REQUIRED_CLASSIFICATION_VERSION,
    "boj_bond_market_survey_is_policy_release": False,
    "boj_bond_market_survey_source_role": "primary_market_structure_survey",
    "boj_bond_market_survey_category": "bond_market_functioning_survey",
    "market_structure_survey_direction_invented": False,
    "upstream_v152_classifier_rewritten": False,
    "exact_overlay_requires_boj_source_host_path_title_and_jpy": True,
    "proof_eligibility_contract": PROOF_ELIGIBILITY_CONTRACT,
    "proof_requires_relevant_true": False,
    "proof_requires_empty_exclusion_reason": False,
    "proof_requires_classifier_relevant_or_exact_overlay": True,
    "v7_code_and_evidence_preserved": True,
    "v7_rows_imported": False,
}

iso = v7.iso
parse_time = v7.parse_time
pip_size = v7.pip_size
CandlePoint = v7.CandlePoint
PairObservation = v7.PairObservation

_BASE_LOAD_CURRENT_OBSERVATIONS = v7._BASE_LOAD_CURRENT_OBSERVATIONS
_BASE_CANONICALIZE_OBSERVATIONS = v7._BASE_CANONICALIZE_OBSERVATIONS
_BASE_OPEN_OUTPUT_DATABASE = v7._BASE_OPEN_OUTPUT_DATABASE
_BASE_RUN_CYCLE = v7._BASE_RUN_CYCLE


def load_current_observations(
    mapping_database: Path = INPUT_MAPPING_DATABASE,
    raw_database: Path = INPUT_RAW_DATABASE,
    **_: Any,
) -> list[dict[str, Any]]:
    """Read only exact frozen V152 mappings; never fall back to older rows."""

    return _BASE_LOAD_CURRENT_OBSERVATIONS(
        mapping_database,
        raw_database,
        required_classifier=REQUIRED_CLASSIFICATION_VERSION,
        required_mapper_contract=REQUIRED_MAPPER_CONTRACT,
    )


def _apply_boj_market_structure_overlay(event: dict[str, Any]) -> bool:
    """Apply the narrow V8 taxonomy without mutating the V152 transport."""

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
    if not isinstance(payload, Mapping):
        return False
    headline = str(event.get("headline") or payload.get("headline") or "").strip()
    source_url = str(
        event.get("source_url") or payload.get("source_url") or ""
    ).lower()
    source_facts = event.get("source_facts") or {}
    source_id = str(
        source_facts.get("source_id") or payload.get("source_id") or ""
    )
    currency = str(event.get("currency") or "").upper()
    exact = bool(
        source_id == "boj_updates"
        and currency == "JPY"
        and re.search(r"^\s*Bond\s+Market\s+Survey\b", headline, flags=re.I)
        and "boj.or.jp/" in source_url
        and "/paym/bond/bond_list/" in source_url
    )
    if not exact:
        return False

    known = str(event.get("first_known_utc") or "")
    factor_values = (
        ("action", "neutral_market_structure_survey"),
        ("authority", "boj_or_jp"),
        ("category", "bond_market_functioning_survey"),
        ("mechanism", "market_liquidity_state"),
        ("mechanism", "sovereign_curve_microstructure"),
        ("source_role", "primary_market_structure_survey"),
        ("topic", "bond_market_functioning_survey"),
        ("topic", "jpy_neutral_market_structure_survey"),
    )
    event.update(
        {
            "category": "bond_market_functioning_survey",
            "source_role": "primary_market_structure_survey",
            "topic_signature": (
                "bond_market_functioning_survey_jpy_general_"
                "neutral_market_structure_survey"
            ),
            "taxonomy_overlay_contract_id": CONTRACT_ID,
            "upstream_classifier_relevant": payload.get("relevant") is True,
            "upstream_classifier_exclusion_reason": str(
                payload.get("exclusion_reason") or ""
            ).strip(),
            "factors": [
                {
                    "factor_type": factor_type,
                    "factor_value": factor_value,
                    "factor_key": f"{factor_type}:{factor_value}",
                    "factor_known_utc": known,
                }
                for factor_type, factor_value in factor_values
            ],
        }
    )
    if isinstance(source_facts, dict):
        source_facts.update(
            {
                "category": "bond_market_functioning_survey",
                "source_role": "primary_market_structure_survey",
                "topic_action": "neutral_market_structure_survey",
                "transmission_mechanisms": [
                    "sovereign_curve_microstructure",
                    "market_liquidity_state",
                ],
                "relevant": True,
                "exclusion_reason": "",
                "official_policy_release": False,
                "policy_stance_bearing_eligible": False,
                "official_market_structure_survey": True,
                "v8_taxonomy_overlay": True,
            }
        )
        event["source_facts"] = source_facts
    return True


def canonicalize_observations(
    observations: Any,
    **_: Any,
) -> list[dict[str, Any]]:
    """Retain V152 diagnostics and admit only valid post-cutover V8 proof."""

    events = _BASE_CANONICALIZE_OBSERVATIONS(
        observations,
        activation_utc=ACTIVATED_UTC,
    )
    for event in events:
        market_structure_overlay = _apply_boj_market_structure_overlay(event)
        first_seen = parse_time(event.get("first_known_utc"))
        after_activation = bool(
            first_seen is not None and first_seen >= ACTIVATED_UTC
        )
        exclusions = (
            []
            if market_structure_overlay
            else v7.v6.v5._classifier_proof_exclusions(event)
        )
        eligible = bool(after_activation and not exclusions)
        event["prospective_proof_eligible"] = eligible
        event["proof_eligibility_contract"] = PROOF_ELIGIBILITY_CONTRACT
        event["proof_exclusion_reasons"] = exclusions
        if after_activation:
            event["evidence_class"] = (
                "prospective_v8"
                if eligible
                else "prospective_v8_excluded_diagnostic"
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
                source_facts["upstream_classifier_relevant"] = (
                    payload.get("relevant") is True
                )
                source_facts["upstream_classifier_exclusion_reason"] = str(
                    payload.get("exclusion_reason") or ""
                ).strip()
                if not market_structure_overlay:
                    source_facts["relevant"] = payload.get("relevant") is True
                    source_facts["exclusion_reason"] = str(
                        payload.get("exclusion_reason") or ""
                    ).strip()
    return events


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    return _BASE_OPEN_OUTPUT_DATABASE(path)


def _render_report_v8(snapshot: Mapping[str, Any]) -> str:
    report = v7.v6._V1_RENDER_REPORT(snapshot)
    report = report.replace(
        "# Causal Source-Factor Response Map V1",
        "# Causal Source-Factor Response Map V8",
        1,
    )
    return report.replace(
        "This is an isolated research ledger.",
        (
            "This is an isolated V8 research ledger; V1-V7 cohorts remain "
            "preserved diagnostics and are never imported."
        ),
        1,
    )


@contextmanager
def _v8_contract() -> Iterator[None]:
    base = v7.v6
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
        "_render_report_v6": _render_report_v8,
    }
    previous = {name: getattr(base, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(base, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(base, name, value)


def compute_event_response(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
    with _v8_contract():
        return v7.v6.compute_event_response(*args, **kwargs)


def insert_events(connection: sqlite3.Connection, events: Any) -> dict[str, int]:
    with _v8_contract():
        return v7.v6.insert_events(connection, events)


def insert_responses(connection: sqlite3.Connection, rows: Any) -> int:
    with _v8_contract():
        return v7.v6.insert_responses(connection, rows)


def build_prequential_forecast(
    connection: sqlite3.Connection,
    factor: Mapping[str, Any],
    horizon_min: int,
    *,
    minimum_n: int = v7.v6.v1.MIN_EFFECTIVE_N,
) -> dict[str, Any]:
    with _v8_contract():
        return v7.v6.build_prequential_forecast(
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
    with _v8_contract():
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
