#!/usr/bin/env python3
"""Audit fixed direction policies only where the v2 magnitude model sees opportunity.

The input archive has already been inspected.  Results are engineering discovery
only: this module cannot promote, authorize, or trade.  Its purpose is to isolate
whether the current bottleneck is direction, pair selection, or both while
counting at most one top-one decision per market timestamp.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Mapping

import joblib
import numpy as np
import pandas as pd

import oanda_68_pair_opportunity_census as source
import oanda_executable_opportunity_feature_contract as feature_contract
import oanda_executable_opportunity_ranking as ranking

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "executable_opportunity_ranking_v2.json"
ARTIFACT = ROOT / "data" / "oanda_training_manager" / "model_space" / "proof_cohorts" / "executable_opportunity_ranking_v2"
OUTPUT = ROOT / "data" / "oanda_training_manager" / "reports" / "executable_opportunity_ranking" / "OPPORTUNITY_DIRECTION_POLICY_AUDIT_20260814.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "executable_opportunity_ranking" / "OPPORTUNITY_DIRECTION_POLICY_AUDIT_20260814.md"


def atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def sign(value: float) -> int:
    return 1 if float(value) > 0 else -1 if float(value) < 0 else 0


def weighted_momentum(row: pd.Series) -> float:
    return (
        0.15 * float(row["return_1m_pips"])
        + 0.35 * float(row["return_5m_pips"])
        + 0.30 * float(row["return_15m_pips"])
        + 0.20 * float(row["return_30m_pips"])
    )


POLICIES: dict[str, Callable[[pd.Series], int]] = {
    "continuation_1m": lambda row: sign(row["return_1m_pips"]),
    "continuation_5m": lambda row: sign(row["return_5m_pips"]),
    "continuation_15m": lambda row: sign(row["return_15m_pips"]),
    "continuation_30m": lambda row: sign(row["return_30m_pips"]),
    "reversion_1m": lambda row: -sign(row["return_1m_pips"]),
    "reversion_5m": lambda row: -sign(row["return_5m_pips"]),
    "reversion_15m": lambda row: -sign(row["return_15m_pips"]),
    "reversion_30m": lambda row: -sign(row["return_30m_pips"]),
    "currency_factor_continuation": lambda row: sign(row["currency_factor_5m_pips"]),
    "currency_factor_reversion": lambda row: -sign(row["currency_factor_5m_pips"]),
    "pair_residual_continuation": lambda row: sign(row["pair_residual_5m_pips"]),
    "pair_residual_reversion": lambda row: -sign(row["pair_residual_5m_pips"]),
    "weighted_momentum_continuation": lambda row: sign(weighted_momentum(row)),
    "weighted_momentum_reversion": lambda row: -sign(weighted_momentum(row)),
    "quote_imbalance_30s": lambda row: sign(row["imbalance_30s"]),
    "quote_imbalance_120s": lambda row: sign(row["imbalance_120s"]),
}


def summarize(values: list[float], correct: list[int]) -> dict[str, Any]:
    gross = sum(max(value, 0.0) for value in values)
    loss = -sum(min(value, 0.0) for value in values)
    best = max(values) if values else None
    return {
        "n": len(values),
        "wins": sum(value > 0 for value in values),
        "win_rate": sum(value > 0 for value in values) / len(values) if values else None,
        "direction_accuracy": sum(correct) / len(correct) if correct else None,
        "average_net_pips": sum(values) / len(values) if values else None,
        "total_net_pips": sum(values),
        "profit_factor": gross / loss if loss else None,
        "minimum_net_pips": min(values) if values else None,
        "maximum_net_pips": best,
        "average_without_best_pips": (sum(values) - best) / (len(values) - 1) if len(values) > 1 else None,
        "proof_eligible": False,
    }


def selected_rows(frame: pd.DataFrame, count: int) -> pd.DataFrame:
    ordered = frame.sort_values(
        ["predicted_clear_probability", "predicted_magnitude_cost_ratio", "instrument"],
        ascending=[False, False, True],
    )
    if count == 1:
        return ordered.head(1)
    chosen: list[int] = []
    used: set[str] = set()
    for index, row in ordered.iterrows():
        legs = source.currency_legs(str(row["instrument"]))
        if used.isdisjoint(legs):
            chosen.append(index)
            used.update(legs)
        if len(chosen) == count:
            break
    return frame.loc[chosen] if len(chosen) == count else frame.iloc[0:0]


def evaluate_policy(frame: pd.DataFrame, policy: Callable[[pd.Series], int], selection_count: int) -> dict[str, Any]:
    values: list[float] = []
    correct: list[int] = []
    for _, group in frame.groupby("epoch"):
        chosen = selected_rows(group, selection_count)
        if len(chosen) != selection_count:
            continue
        leg_values: list[float] = []
        leg_correct: list[int] = []
        for _, row in chosen.iterrows():
            direction = int(policy(row))
            if not direction:
                continue
            future = float(row["future_move_pips"])
            leg_values.append(direction * future - float(row["actual_cost_pips"]))
            leg_correct.append(int(direction == sign(future)))
        if len(leg_values) == selection_count:
            values.append(sum(leg_values) / selection_count)
            correct.append(int(all(leg_correct))) if selection_count > 1 else correct.append(leg_correct[0])
    return summarize(values, correct)


def block_metrics(frame: pd.DataFrame, policy: Callable[[pd.Series], int], selection_count: int) -> list[dict[str, Any]]:
    epochs = sorted(int(value) for value in frame["epoch"].unique())
    if not epochs:
        return []
    cut = epochs[len(epochs) // 2]
    return [
        {"block": "early", **evaluate_policy(frame[frame["epoch"] < cut], policy, selection_count)},
        {"block": "late", **evaluate_policy(frame[frame["epoch"] >= cut], policy, selection_count)},
    ]


def run(config_path: Path = CONFIG, artifact_path: Path = ARTIFACT, output: Path = OUTPUT, report: Path = REPORT) -> dict[str, Any]:
    config = ranking.read_json(config_path)
    source._PIP_SIZES = source.load_pip_sizes(ranking.QUOTES)
    by_pair, instruments = source.load_minutes(ranking.DATABASE)
    models = joblib.load(artifact_path / "models.joblib")
    results: list[dict[str, Any]] = []
    for raw_horizon in config.get("horizons_sec") or []:
        horizon = int(raw_horizon)
        bundle = models[str(horizon)]
        frame = feature_contract.labeled_frame(
            by_pair,
            horizon,
            float(config["modeled_slippage_pips"]),
            float(config["maximum_entry_spread_pips"]),
        )
        frame = frame[frame["epoch"] >= int(bundle["cutoffs"]["validation_start_epoch"])].copy()
        frame["predicted_clear_probability"] = ranking.probability(
            bundle["clear_model"], bundle["clear_calibrator"], frame[ranking.FEATURES]
        )
        frame["predicted_magnitude_pips"] = np.maximum(0.0, bundle["magnitude_model"].predict(frame[ranking.FEATURES]))
        frame["predicted_magnitude_cost_ratio"] = frame["predicted_magnitude_pips"] / frame["entry_cost_pips"]
        eligible = frame[
            (frame["predicted_clear_probability"] >= float(config["minimum_clear_probability"]))
            & (frame["predicted_magnitude_cost_ratio"] >= float(config["minimum_predicted_magnitude_cost_ratio"]))
        ].copy()
        policies: list[dict[str, Any]] = []
        for name, policy in POLICIES.items():
            top = evaluate_policy(eligible, policy, 1)
            basket = evaluate_policy(eligible, policy, 3)
            policies.append(
                {
                    "policy": name,
                    "strict_top_one": top,
                    "strict_top_one_time_blocks": block_metrics(eligible, policy, 1),
                    "exactly_three_currency_disjoint": basket,
                    "proof_eligible": False,
                }
            )
        results.append(
            {
                "horizon_sec": horizon,
                "validation_rows": len(frame),
                "magnitude_eligible_pair_rows": len(eligible),
                "decision_epochs": int(eligible["epoch"].nunique()),
                "policies": sorted(
                    policies,
                    key=lambda item: (
                        -(item["strict_top_one"]["average_net_pips"] or -math.inf),
                        item["policy"],
                    ),
                ),
            }
        )
    payload = {
        "schema_version": 1,
        "cohort_id": "opportunity_direction_policy_archive_audit_v1_20260814",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "source_model_cohort_id": str(config.get("cohort_id") or ""),
        "source_instrument_count": len(instruments),
        "policy_count": len(POLICIES),
        "evidence_class": "already_inspected_archive_engineering_discovery",
        "results": results,
        "limitations": [
            "archive was already inspected",
            "policies are not multiplicity-adjusted",
            "market timestamps remain correlated across sessions and episodes",
            "a positive result can only nominate a new prospective cohort",
        ],
    }
    atomic(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# Opportunity Direction Policy Audit",
        "",
        "Engineering discovery only. No result can promote, authorize, or trade.",
        "",
        "| Horizon | Policy | Top-one N | Direction | Win | Avg net | PF | Early avg | Late avg | Basket avg |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in results:
        for item in result["policies"]:
            top = item["strict_top_one"]
            blocks = {row["block"]: row for row in item["strict_top_one_time_blocks"]}
            basket = item["exactly_three_currency_disjoint"]
            fmt = lambda value, pattern=".3f": "n/a" if value is None else format(value, pattern)
            lines.append(
                f"| {result['horizon_sec']} | {item['policy']} | {top['n']} | {fmt(top['direction_accuracy'], '.1%')} | "
                f"{fmt(top['win_rate'], '.1%')} | {fmt(top['average_net_pips'])} | {fmt(top['profit_factor'])} | "
                f"{fmt((blocks.get('early') or {}).get('average_net_pips'))} | "
                f"{fmt((blocks.get('late') or {}).get('average_net_pips'))} | "
                f"{fmt(basket.get('average_net_pips'))} |"
            )
    lines += [
        "",
        "Rows are ordered by archive top-one average within each horizon. Any selected rule must start a later immutable prospective cohort; this report is not proof.",
        "",
    ]
    atomic(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--artifact", type=Path, default=ARTIFACT)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    run(args.config, args.artifact, args.output, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
