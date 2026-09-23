#!/usr/bin/env python3
"""Audit simple live-signal gates without wiring them to an account."""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "all_signal_live_forecasts_v2.sqlite"
)
DEFAULT_REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "live_signal_gate_audit"
)
LIQUID_INSTRUMENTS = frozenset(
    {
        "AUD_JPY",
        "AUD_USD",
        "CAD_JPY",
        "CHF_JPY",
        "EUR_CHF",
        "EUR_GBP",
        "EUR_JPY",
        "EUR_USD",
        "GBP_CHF",
        "GBP_JPY",
        "GBP_USD",
        "NZD_JPY",
        "NZD_USD",
        "USD_CAD",
        "USD_CHF",
        "USD_JPY",
    }
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_matured_outcomes(database: Path, window_hours: float) -> pd.DataFrame:
    connection = sqlite3.connect(database, timeout=30.0)
    try:
        maximum = float(
            connection.execute(
                "SELECT COALESCE(MAX(observed_epoch), 0) FROM outcomes"
            ).fetchone()[0]
        )
        frame = pd.read_sql_query(
            """
            SELECT candidate_id, horizon_sec, family, model_id, instrument,
                   input_timeframe, generated_epoch, observed_epoch, direction,
                   probability_up, predicted_signed_pips,
                   predicted_magnitude_pips, entry_spread_pips,
                   direction_correct, executable_profitable,
                   executable_net_pips, snapshot_id
            FROM outcomes
            WHERE observed_epoch >= ?
            """,
            connection,
            params=(maximum - max(0.25, float(window_hours)) * 3600.0,),
        )
    finally:
        connection.close()
    return frame


def consolidate_forecasts(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["confidence"] = np.maximum(
        result["probability_up"],
        1.0 - result["probability_up"],
    )
    result["predicted_edge_pips"] = (
        result["predicted_magnitude_pips"] - result["entry_spread_pips"]
    )
    result["gross_to_spread"] = result["predicted_magnitude_pips"] / result[
        "entry_spread_pips"
    ].clip(lower=0.01)
    result["liquid_instrument"] = result["instrument"].isin(LIQUID_INSTRUMENTS)
    return (
        result.sort_values(
            [
                "snapshot_id",
                "instrument",
                "horizon_sec",
                "predicted_edge_pips",
                "confidence",
                "candidate_id",
            ],
            ascending=[True, True, True, False, False, True],
        )
        .drop_duplicates(["snapshot_id", "instrument", "horizon_sec"])
        .sort_values(["generated_epoch", "instrument", "horizon_sec"])
        .reset_index(drop=True)
    )


def gate_grid() -> Iterable[dict[str, float | int | str]]:
    for scope, confidence, edge, ratio, horizon, spread in itertools.product(
        ("all", "liquid"),
        (0.50, 0.52, 0.54, 0.55),
        (-5.0, 0.0, 0.25, 0.50),
        (0.0, 1.35, 2.0),
        (900, 3600, 14400),
        (1.5, 2.0, 3.0),
    ):
        yield {
            "scope": scope,
            "minimum_confidence": confidence,
            "minimum_predicted_edge_pips": edge,
            "minimum_gross_to_spread": ratio,
            "maximum_horizon_sec": horizon,
            "maximum_spread_pips": spread,
        }


def apply_gate(
    frame: pd.DataFrame,
    gate: dict[str, float | int | str],
) -> pd.DataFrame:
    selected = frame[
        (frame["confidence"] >= float(gate["minimum_confidence"]))
        & (
            frame["predicted_edge_pips"]
            >= float(gate["minimum_predicted_edge_pips"])
        )
        & (
            frame["gross_to_spread"]
            >= float(gate["minimum_gross_to_spread"])
        )
        & (frame["horizon_sec"] <= int(gate["maximum_horizon_sec"]))
        & (frame["entry_spread_pips"] <= float(gate["maximum_spread_pips"]))
    ]
    if gate["scope"] == "liquid":
        selected = selected[selected["liquid_instrument"]]
    return selected


def outcome_metrics(frame: pd.DataFrame, block_sec: int) -> dict[str, Any]:
    if frame.empty:
        return {
            "n": 0,
            "blocks": 0,
            "positive_blocks": 0,
            "direction_accuracy": 0.0,
            "executable_win_rate": 0.0,
            "average_net_pips": 0.0,
            "median_net_pips": 0.0,
            "sum_net_pips": 0.0,
            "block_mean_net_pips": 0.0,
            "block_mean_lower_95": -math.inf,
            "worst_block_net_pips": 0.0,
        }
    blocks = (
        frame.assign(
            block=(frame["generated_epoch"] // max(60, int(block_sec))).astype(int)
        )
        .groupby("block", observed=True)["executable_net_pips"]
        .mean()
    )
    lower = -math.inf
    if len(blocks) >= 2:
        lower = float(
            blocks.mean()
            - 1.96 * blocks.std(ddof=1) / math.sqrt(float(len(blocks)))
        )
    return {
        "n": int(len(frame)),
        "blocks": int(len(blocks)),
        "positive_blocks": int((blocks > 0.0).sum()),
        "direction_accuracy": float(frame["direction_correct"].mean()),
        "executable_win_rate": float(frame["executable_profitable"].mean()),
        "average_net_pips": float(frame["executable_net_pips"].mean()),
        "median_net_pips": float(frame["executable_net_pips"].median()),
        "sum_net_pips": float(frame["executable_net_pips"].sum()),
        "block_mean_net_pips": float(blocks.mean()),
        "block_mean_lower_95": lower,
        "worst_block_net_pips": float(blocks.min()),
    }


def audit_frame(
    frame: pd.DataFrame,
    *,
    fit_fraction: float = 0.70,
    minimum_fit_samples: int = 50,
    minimum_fit_blocks: int = 3,
    block_sec: int = 900,
) -> dict[str, Any]:
    consolidated = consolidate_forecasts(frame)
    if consolidated.empty:
        raise ValueError("no matured outcomes are available")
    unique_times = np.sort(consolidated["generated_epoch"].unique())
    split_index = min(
        len(unique_times) - 1,
        max(1, int(math.floor(len(unique_times) * fit_fraction))),
    )
    split_epoch = float(unique_times[split_index])
    fit = consolidated[consolidated["generated_epoch"] < split_epoch]
    holdout = consolidated[consolidated["generated_epoch"] >= split_epoch]
    if fit.empty or holdout.empty:
        raise ValueError("fit or holdout split is empty")

    evaluated: list[dict[str, Any]] = []
    for gate in gate_grid():
        selected = apply_gate(fit, gate)
        metrics = outcome_metrics(selected, block_sec)
        if (
            metrics["n"] < minimum_fit_samples
            or metrics["blocks"] < minimum_fit_blocks
        ):
            continue
        evaluated.append({"gate": gate, "fit": metrics})
    if not evaluated:
        raise ValueError("no gate satisfies minimum fit sample and block counts")
    evaluated.sort(
        key=lambda row: (
            row["fit"]["block_mean_lower_95"],
            row["fit"]["average_net_pips"],
            row["fit"]["n"],
        ),
        reverse=True,
    )
    best = evaluated[0]
    holdout_metrics = outcome_metrics(
        apply_gate(holdout, best["gate"]),
        block_sec,
    )
    fit_positive = best["fit"]["block_mean_lower_95"] > 0.0
    holdout_positive = (
        holdout_metrics["n"] >= max(20, minimum_fit_samples // 3)
        and holdout_metrics["blocks"] >= 2
        and holdout_metrics["block_mean_lower_95"] > 0.0
    )
    status = (
        "shadow_candidate_not_account_authorized"
        if fit_positive and holdout_positive
        else "no_stable_positive_gate"
    )
    return {
        "status": status,
        "account_wired": False,
        "contract": {
            "duplicate_outputs_consolidated_before_gate_fit": True,
            "fit_and_holdout_are_chronological": True,
            "selection_uses_fit_only": True,
            "holdout_is_untouched_until_gate_selection": True,
            "executable_net_pips_include_observed_bid_ask_costs": True,
            "automatic_account_promotion": False,
        },
        "rows": {
            "raw": int(len(frame)),
            "consolidated": int(len(consolidated)),
            "fit": int(len(fit)),
            "holdout": int(len(holdout)),
        },
        "time": {
            "start_utc": datetime.fromtimestamp(
                float(consolidated["generated_epoch"].min()),
                timezone.utc,
            ).isoformat(),
            "split_utc": datetime.fromtimestamp(split_epoch, timezone.utc).isoformat(),
            "end_utc": datetime.fromtimestamp(
                float(consolidated["generated_epoch"].max()),
                timezone.utc,
            ).isoformat(),
            "block_sec": int(block_sec),
        },
        "baseline": {
            "fit": outcome_metrics(fit, block_sec),
            "holdout": outcome_metrics(holdout, block_sec),
        },
        "selected_gate": best["gate"],
        "selected_gate_fit": best["fit"],
        "selected_gate_holdout": holdout_metrics,
        "top_fit_gates": evaluated[:20],
        "candidate_gates_evaluated": int(len(evaluated)),
    }


def run(
    database: Path,
    report_root: Path,
    *,
    window_hours: float,
    fit_fraction: float,
    minimum_fit_samples: int,
    minimum_fit_blocks: int,
    block_sec: int,
) -> dict[str, Any]:
    frame = load_matured_outcomes(database, window_hours)
    report = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "source": {
            "path": str(database.resolve()),
            "bytes": database.stat().st_size,
            "mutable_live_database": True,
            "window_hours": float(window_hours),
        },
        **audit_frame(
            frame,
            fit_fraction=fit_fraction,
            minimum_fit_samples=minimum_fit_samples,
            minimum_fit_blocks=minimum_fit_blocks,
            block_sec=block_sec,
        ),
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    atomic_json_dump(report, report_root / f"live_signal_gate_audit_{stamp}.json")
    atomic_json_dump(report, report_root / "live_signal_gate_audit_latest.json")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--window-hours", type=float, default=3.0)
    parser.add_argument("--fit-fraction", type=float, default=0.70)
    parser.add_argument("--minimum-fit-samples", type=int, default=50)
    parser.add_argument("--minimum-fit-blocks", type=int, default=3)
    parser.add_argument("--block-sec", type=int, default=900)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run(
        args.database,
        args.report_root,
        window_hours=args.window_hours,
        fit_fraction=args.fit_fraction,
        minimum_fit_samples=args.minimum_fit_samples,
        minimum_fit_blocks=args.minimum_fit_blocks,
        block_sec=args.block_sec,
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "selected_gate": report["selected_gate"],
                "fit": report["selected_gate_fit"],
                "holdout": report["selected_gate_holdout"],
                "report": str(
                    (args.report_root / "live_signal_gate_audit_latest.json").resolve()
                ),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
