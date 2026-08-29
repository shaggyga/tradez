#!/usr/bin/env python3
"""Reconcile one fixed macro candidate across incompatible historical inputs."""

from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any, Mapping

from oanda_alfred_macro_relative_strength_research import atomic, read_json


ROOT = Path(__file__).resolve().parent
CURRENT_VIEW = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_SHORT_RATE_RELATIVE_STRENGTH_DISCOVERY_20260816.json"
INITIAL_RELEASE = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_SHORT_RATE_POINT_IN_TIME_REPLAY_20260816.json"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_SHORT_RATE_REPLAY_RECONCILIATION_20260816.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "macro_relative_strength" / "ALFRED_SHORT_RATE_REPLAY_RECONCILIATION_20260816.md"


FIXED = {
    "signal_rule": "short_rate_change_differential",
    "threshold": 0.25,
    "horizon_hours": 24,
    "liquidity_bucket": "liquid",
}


def selected(payload: Mapping[str, Any]) -> dict[str, Any]:
    for row in payload.get("summaries") or []:
        if all(row.get(key) == value for key, value in FIXED.items()):
            return dict(row)
    return {}


def selected_details(payload: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row.get("pair")), str(row.get("reference_date"))): dict(row)
        for row in payload.get("details") or []
        if row.get("signal_rule") == FIXED["signal_rule"]
        and row.get("horizon_hours") == FIXED["horizon_hours"]
        and row.get("liquidity_bucket") == FIXED["liquidity_bucket"]
        and abs(float(row.get("signal_value") or 0.0)) >= FIXED["threshold"]
    }


def subset_stats(keys, rows: Mapping[tuple[str, str], Mapping[str, Any]]) -> dict[str, Any]:
    values = [float(rows[key]["rule_after_cost_pips"]) for key in keys]
    return {
        "n": len(values),
        "average_net_pips": statistics.fmean(values) if values else None,
        "win_rate": sum(value > 0 for value in values) / len(values) if values else None,
    }


def run(
    current_view_path: Path = CURRENT_VIEW,
    initial_release_path: Path = INITIAL_RELEASE,
    output_path: Path = OUTPUT,
    report_path: Path = REPORT,
) -> dict[str, Any]:
    current_payload = read_json(current_view_path)
    point_payload = read_json(initial_release_path)
    current = selected(current_payload)
    point = selected(point_payload)
    current_rule = current.get("rule") or {}
    point_rule = point.get("rule") or {}
    current_details = selected_details(current_payload)
    point_details = selected_details(point_payload)
    overlap_keys = sorted(set(current_details) & set(point_details))
    current_only_keys = sorted(set(current_details) - set(point_details))
    point_only_keys = sorted(set(point_details) - set(current_details))
    membership_decomposition = {
        "overlap_evaluated_on_current_view_clock": subset_stats(
            overlap_keys, current_details
        ),
        "overlap_evaluated_on_initial_release_clock": subset_stats(
            overlap_keys, point_details
        ),
        "current_view_only": subset_stats(current_only_keys, current_details),
        "initial_release_only": subset_stats(point_only_keys, point_details),
        "overlap_side_agreement_count": sum(
            current_details[key].get("predicted_side")
            == point_details[key].get("predicted_side")
            for key in overlap_keys
        ),
        "overlap_n": len(overlap_keys),
        "overlap_same_entry_clock_count": sum(
            current_details[key].get("entry_utc")
            == point_details[key].get("entry_utc")
            for key in overlap_keys
        ),
        "largest_initial_release_only_losses": [
            {
                "pair": key[0],
                "reference_date": key[1],
                "signal_value": point_details[key].get("signal_value"),
                "entry_utc": point_details[key].get("entry_utc"),
                "after_cost_pips": point_details[key].get("rule_after_cost_pips"),
            }
            for key in sorted(
                point_only_keys,
                key=lambda item: float(
                    point_details[item].get("rule_after_cost_pips") or 0.0
                ),
            )[:10]
        ],
    }
    raw_ev_delta = None
    if current_rule.get("average_net_pips") is not None and point_rule.get("average_net_pips") is not None:
        raw_ev_delta = float(point_rule["average_net_pips"]) - float(current_rule["average_net_pips"])
    sign_reversed = bool(
        current_rule.get("average_net_pips") is not None
        and point_rule.get("average_net_pips") is not None
        and float(current_rule["average_net_pips"]) * float(point_rule["average_net_pips"]) < 0
    )
    result = {
        "schema_version": 1,
        "candidate": FIXED,
        "current_view_counterfactual": current,
        "initial_release_point_in_time": point,
        "raw_average_net_pips_delta": raw_ev_delta,
        "raw_average_sign_reversed": sign_reversed,
        "membership_decomposition": membership_decomposition,
        "robustness_state": (
            "failed_point_in_time_robustness"
            if sign_reversed
            or point_rule.get("factor_lcb_normal_95_pips") is None
            or float(point_rule.get("factor_lcb_normal_95_pips")) <= 0
            else "point_in_time_positive_but_still_historical_not_proof"
        ),
        "can_promote": False,
        "can_authorize": False,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "comparison_warning": "The inputs and availability clocks differ, so results are compared side by side and never pooled.",
    }
    atomic(output_path, json.dumps(result, indent=2, sort_keys=True) + "\n")
    def fmt(value: Any) -> str:
        return "n/a" if value is None else f"{float(value):.3f}"
    lines = [
        "# ALFRED Short-Rate Replay Reconciliation",
        "",
        "One candidate fixed before the point-in-time replay. Results are not pooled.",
        "",
        "| Input | Raw N | Factor N | Win | Avg net pips | Factor mean | Factor LCB | BH q |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, row in (
        ("Current-view + 45-day lag", current_rule),
        ("Initial-release vintage + next-day clock", point_rule),
    ):
        lines.append(
            f"| {label} | {row.get('raw_n', 0)} | {row.get('factor_episode_n', 0)} | "
            f"{fmt(None if row.get('win_rate') is None else 100 * float(row['win_rate']))}% | "
            f"{fmt(row.get('average_net_pips'))} | {fmt(row.get('factor_mean_pips'))} | "
            f"{fmt(row.get('factor_lcb_normal_95_pips'))} | {fmt(row.get('discovery_bh_q_value'))} |"
        )
    lines += [
        "",
        f"Robustness state: **{result['robustness_state']}**.",
        f"Raw average change: **{fmt(raw_ev_delta)} pips**; sign reversed: **{str(sign_reversed).lower()}**.",
        "",
        "The earlier positive current-view result is not accepted as historical edge. Only later immutable prospective releases can test the frozen cohort.",
        "",
        "## Candidate-membership decomposition",
        "",
        "| Subset | N | Avg net pips | Win rate |",
        "|---|---:|---:|---:|",
    ]
    for label, key in (
        ("Overlap on current-view clock", "overlap_evaluated_on_current_view_clock"),
        ("Overlap on initial-release clock", "overlap_evaluated_on_initial_release_clock"),
        ("Current-view only", "current_view_only"),
        ("Initial-release only", "initial_release_only"),
    ):
        row = membership_decomposition[key]
        lines.append(
            f"| {label} | {row['n']} | {fmt(row['average_net_pips'])} | "
            f"{fmt(None if row['win_rate'] is None else 100 * row['win_rate'])}% |"
        )
    lines += [
        "",
        "Initial-release-only observations expose trades that later revised values moved below the fixed threshold. Their performance measures revised-data selection bias; they are not optional exclusions.",
        "",
    ]
    atomic(report_path, "\n".join(lines))
    return result


if __name__ == "__main__":
    run()
