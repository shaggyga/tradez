#!/usr/bin/env python3
"""Consolidate locked macro candidates under initial-release replay."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping

from oanda_alfred_macro_relative_strength_research import atomic, read_json


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength"
OUTPUT = REPORTS / "MACRO_POINT_IN_TIME_VALIDATION_20260816.json"
REPORT = REPORTS / "MACRO_POINT_IN_TIME_VALIDATION_20260816.md"
UTC = dt.timezone.utc

CANDIDATES = [
    {
        "candidate_id": "cpi_acceleration_h168_liquid_threshold_025",
        "current_path": "ALFRED_MACRO_RELATIVE_STRENGTH_DISCOVERY_20260816.json",
        "point_path": "ALFRED_CPI_POINT_IN_TIME_REPLAY_20260816.json",
        "signal_rule": "cpi_acceleration_differential",
        "threshold": 0.25,
        "horizon_hours": 168,
        "liquidity_bucket": "liquid",
    },
    {
        "candidate_id": "unemployment_change_h24_liquid_threshold_000",
        "current_path": "ALFRED_MACRO_RELATIVE_STRENGTH_DISCOVERY_20260816.json",
        "point_path": "ALFRED_UNEMPLOYMENT_POINT_IN_TIME_REPLAY_20260816.json",
        "signal_rule": "unemployment_change_differential",
        "threshold": 0.0,
        "horizon_hours": 24,
        "liquidity_bucket": "liquid",
    },
    {
        "candidate_id": "short_rate_change_h24_liquid_threshold_025",
        "current_path": "ALFRED_SHORT_RATE_RELATIVE_STRENGTH_DISCOVERY_20260816.json",
        "point_path": "ALFRED_SHORT_RATE_POINT_IN_TIME_REPLAY_20260816.json",
        "signal_rule": "short_rate_change_differential",
        "threshold": 0.25,
        "horizon_hours": 24,
        "liquidity_bucket": "liquid",
    },
]


def find_summary(payload: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("signal_rule", "threshold", "horizon_hours", "liquidity_bucket")
    for row in payload.get("summaries") or []:
        if all(row.get(key) == candidate.get(key) for key in keys):
            return dict(row)
    return {}


def aligned_arm(payload: Mapping[str, Any]) -> dict[str, Any]:
    diagnostics = payload.get("technical_confirmation_diagnostics") or []
    if not diagnostics:
        return {}
    for row in diagnostics[0].get("arms") or []:
        if row.get("arm") == "macro_technical_aligned":
            return dict(row.get("rule") or {})
    return {}


def robustness_state(rule: Mapping[str, Any]) -> str:
    if not rule or not rule.get("raw_n"):
        return "unavailable"
    average = rule.get("average_net_pips")
    lower = rule.get("factor_lcb_normal_95_pips")
    q_value = rule.get("discovery_bh_q_value")
    if average is None or float(average) <= 0:
        return "failed_negative_point_estimate"
    if lower is None or float(lower) <= 0:
        return "failed_nonpositive_factor_lower_bound"
    if q_value is None or float(q_value) > 0.05:
        return "failed_multiplicity_control"
    return "historically_positive_but_not_prospective_proof"


def run(output_path: Path = OUTPUT, report_path: Path = REPORT) -> dict[str, Any]:
    rows = []
    for candidate in CANDIDATES:
        current_payload = read_json(REPORTS / candidate["current_path"])
        point_payload = read_json(REPORTS / candidate["point_path"])
        current = find_summary(current_payload, candidate)
        point = find_summary(point_payload, candidate)
        point_rule = point.get("rule") or {}
        rows.append(
            {
                "candidate": {
                    key: value
                    for key, value in candidate.items()
                    if key not in {"current_path", "point_path"}
                },
                "current_view_counterfactual": current.get("rule") or {},
                "initial_release_point_in_time": point_rule,
                "point_in_time_technical_aligned": aligned_arm(point_payload),
                "robustness_state": robustness_state(point_rule),
                "can_promote": False,
                "execution_eligible": False,
            }
        )
    all_failed = all(
        row["robustness_state"].startswith("failed_") for row in rows
    )
    result = {
        "schema_version": 1,
        "generated_utc": dt.datetime.now(UTC).isoformat(),
        "candidates": rows,
        "all_locked_candidates_failed_point_in_time_robustness": all_failed,
        "conclusion": (
            "simple_relative_macro_generation_not_supported_by_initial_release_replay"
            if all_failed
            else "mixed_or_incomplete"
        ),
        "prospective_cohorts_remain_unchanged": True,
        "historical_results_can_confirm": False,
        "can_promote": False,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    atomic(output_path, json.dumps(result, indent=2, sort_keys=True) + "\n")
    def fmt(value: Any) -> str:
        return "n/a" if value is None else f"{float(value):.3f}"
    lines = [
        "# Macro Point-in-Time Validation",
        "",
        f"Generated: `{result['generated_utc']}`",
        "",
        "Each rule was fixed before its initial-release replay. Historical results cannot confirm or authorize anything.",
        "",
        "| Candidate | Current N / avg / LCB | Initial N / avg / LCB | Technical-aligned initial N / avg / LCB | Verdict |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        current = row["current_view_counterfactual"]
        point = row["initial_release_point_in_time"]
        technical = row["point_in_time_technical_aligned"]
        lines.append(
            f"| {row['candidate']['candidate_id']} | "
            f"{current.get('raw_n', 0)} / {fmt(current.get('average_net_pips'))} / {fmt(current.get('factor_lcb_normal_95_pips'))} | "
            f"{point.get('raw_n', 0)} / {fmt(point.get('average_net_pips'))} / {fmt(point.get('factor_lcb_normal_95_pips'))} | "
            f"{technical.get('raw_n', 0)} / {fmt(technical.get('average_net_pips'))} / {fmt(technical.get('factor_lcb_normal_95_pips'))} | "
            f"{row['robustness_state']} |"
        )
    lines += [
        "",
        f"Conclusion: **{result['conclusion']}**.",
        "",
        "The immutable prospective CPI, unemployment, and rate cohorts remain unchanged so later genuinely first-seen releases can falsify this null. No current result supports practice or real-money entry.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return result


if __name__ == "__main__":
    run()
