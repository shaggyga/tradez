#!/usr/bin/env python3
"""Evaluate separate movement-opportunity and direction heads on a shared FX panel."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

try:
    import oanda_shared_panel_model_benchmark as benchmark
    import oanda_shared_timeframe_horizon_panel as panel
except ModuleNotFoundError:
    from trad import oanda_shared_panel_model_benchmark as benchmark
    from trad import oanda_shared_timeframe_horizon_panel as panel


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
REPORT_ROOT = DATA / "reports" / "second_microstructure_two_stage"
MODEL_ROOT = DATA / "models" / "second_microstructure_two_stage"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event_frame(frame: pd.DataFrame) -> pd.DataFrame:
    long_rows = (
        frame[frame["direction"].str.upper() == "LONG"]
        .sort_values(["prediction_time_utc", "event_id"])
        .drop_duplicates("event_id")
        .copy()
    )
    short_rows = (
        frame[frame["direction"].str.upper() == "SHORT"]
        .drop_duplicates("event_id")
        .set_index("event_id")
    )
    missing = sorted(set(long_rows["event_id"]) - set(short_rows.index))
    if missing:
        raise ValueError(f"{len(missing)} events do not have both LONG and SHORT rows")
    short = short_rows.loc[long_rows["event_id"]]
    long_rows["long_net_pips"] = long_rows["realized_net_pips"].to_numpy(float)
    long_rows["short_net_pips"] = short["realized_net_pips"].to_numpy(float)
    long_rows["target_movement"] = (
        np.maximum(long_rows["long_net_pips"], long_rows["short_net_pips"]) > 0.0
    ).astype(int)
    long_rows["target_long"] = (
        long_rows["long_net_pips"] >= long_rows["short_net_pips"]
    ).astype(int)
    # Keep the standard fold helper on the event-level movement target.
    long_rows[panel.TARGET_COLUMN] = long_rows["target_movement"]
    return long_rows.reset_index(drop=True)


def event_model_features(frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    numeric, categorical = benchmark.model_features(frame)
    numeric = [name for name in numeric if name != "side_sign"]
    categorical = [name for name in categorical if name != "direction"]
    return numeric, categorical


def score_actions(
    frame: pd.DataFrame,
    movement_probability: np.ndarray,
    long_probability: np.ndarray,
) -> pd.DataFrame:
    scored = frame.copy()
    scored["movement_probability"] = np.asarray(movement_probability, dtype=float)
    scored["long_probability"] = np.asarray(long_probability, dtype=float)
    scored["chosen_long"] = scored["long_probability"] >= 0.5
    scored["realized_net_pips"] = np.where(
        scored["chosen_long"],
        scored["long_net_pips"],
        scored["short_net_pips"],
    )
    scored["target_best_side"] = (
        scored["chosen_long"].astype(int) == scored["target_long"].astype(int)
    ).astype(int)
    scored["probability"] = scored["movement_probability"]
    return scored


def threshold_table(actions: pd.DataFrame) -> list[dict[str, Any]]:
    thresholds = benchmark.candidate_probability_thresholds(
        actions["movement_probability"].to_numpy(float)
    )
    return [
        {
            "threshold": threshold,
            "trade_fraction": float(
                np.mean(actions["movement_probability"] >= threshold)
            ),
            **benchmark.trade_metrics(
                actions[actions["movement_probability"] >= threshold]
            ),
        }
        for threshold in thresholds
    ]


def _fit_heads(
    train: pd.DataFrame,
    numeric: list[str],
    categorical: list[str],
) -> tuple[Any, Any]:
    features = [*numeric, *categorical]
    movement = benchmark.make_model("logistic_baseline", numeric, categorical)
    direction = benchmark.make_model("logistic_baseline", numeric, categorical)
    movement.fit(train[features], train["target_movement"].astype(int))
    direction.fit(train[features], train["target_long"].astype(int))
    return movement, direction


def training_tail(frame: pd.DataFrame, fraction: float) -> pd.DataFrame:
    if not 0.0 < fraction <= 1.0:
        raise ValueError("training fraction must be in (0, 1]")
    if fraction >= 1.0:
        return frame
    times = frame["prediction_time_utc"].drop_duplicates().sort_values()
    start_index = max(0, int(math.floor((1.0 - fraction) * len(times))))
    return frame[frame["prediction_time_utc"] >= times.iloc[start_index]]


def _probability(model: Any, frame: pd.DataFrame, features: list[str]) -> np.ndarray:
    return np.asarray(model.predict_proba(frame[features])[:, 1], dtype=float)


def run_two_stage(
    panel_paths: list[Path],
    *,
    max_events: int = 0,
    min_train_events: int = 500,
    min_test_events: int = 100,
    persist_artifact: bool = True,
    report_root: Path = REPORT_ROOT,
    model_root: Path = MODEL_ROOT,
) -> dict[str, Any]:
    side_frame, dataset = benchmark.load_panels(panel_paths, max_events=max_events)
    frame = build_event_frame(side_frame)
    numeric, categorical = event_model_features(frame)
    features = [*numeric, *categorical]
    folds: list[dict[str, Any]] = []
    for number, (train, test) in enumerate(
        benchmark.purged_walk_forward_folds(
            frame,
            min_train_events=min_train_events,
            min_test_events=min_test_events,
        ),
        start=1,
    ):
        if train["target_long"].nunique() < 2 or test["target_long"].nunique() < 2:
            continue
        movement, direction = _fit_heads(train, numeric, categorical)
        movement_probability = _probability(movement, test, features)
        long_probability = _probability(direction, test, features)
        actions = score_actions(test, movement_probability, long_probability)
        folds.append(
            {
                "fold": number,
                "train_events": len(train),
                "test_events": len(test),
                "movement": benchmark.classification_metrics(
                    test["target_movement"].to_numpy(int),
                    movement_probability,
                ),
                "direction": benchmark.classification_metrics(
                    test["target_long"].to_numpy(int),
                    long_probability,
                ),
                "actions": benchmark.trade_metrics(actions),
            }
        )
    if not folds:
        raise ValueError("no valid purged two-stage folds")

    calibration_start = benchmark._time_at_fraction(frame, 0.70)
    holdout_start = benchmark._time_at_fraction(frame, 0.82)
    train = frame[frame["maturity_time_utc"] < calibration_start]
    calibration = frame[
        (frame["prediction_time_utc"] >= calibration_start)
        & (frame["maturity_time_utc"] < holdout_start)
    ]
    holdout = frame[frame["prediction_time_utc"] >= holdout_start]
    for name, split in (
        ("train", train),
        ("calibration", calibration),
        ("holdout", holdout),
    ):
        minimum = min_train_events if name == "train" else min_test_events
        if len(split) < minimum:
            raise ValueError(f"{name} split has fewer than {minimum} events")
        if (
            split["target_movement"].nunique() < 2
            or split["target_long"].nunique() < 2
        ):
            raise ValueError(f"{name} split has no target variation")

    movement, _ = _fit_heads(train, numeric, categorical)
    calibration_movement_raw = _probability(movement, calibration, features)
    movement_calibrator = benchmark._fit_calibrator(
        calibration_movement_raw,
        calibration["target_movement"].to_numpy(int),
    )
    calibration_movement = benchmark._calibrated_probability(
        movement, movement_calibrator, calibration, features
    )
    direction_candidates: list[dict[str, Any]] = []
    for fraction in (1.0, 0.50, 0.25):
        direction_train = training_tail(train, fraction)
        if (
            len(direction_train) < min_train_events
            or direction_train["target_long"].nunique() < 2
        ):
            continue
        direction = benchmark.make_model(
            "logistic_baseline", numeric, categorical
        )
        direction.fit(
            direction_train[features],
            direction_train["target_long"].astype(int),
        )
        calibration_direction_raw = _probability(
            direction, calibration, features
        )
        direction_calibrator = benchmark._fit_calibrator(
            calibration_direction_raw,
            calibration["target_long"].to_numpy(int),
        )
        calibration_direction = benchmark._calibrated_probability(
            direction, direction_calibrator, calibration, features
        )
        candidate_actions = score_actions(
            calibration, calibration_movement, calibration_direction
        )
        direction_candidates.append(
            {
                "fraction": fraction,
                "train_events": len(direction_train),
                "classification": benchmark.classification_metrics(
                    calibration["target_long"].to_numpy(int),
                    calibration_direction,
                ),
                "all_actions": benchmark.trade_metrics(candidate_actions),
                "model": direction,
                "calibrator": direction_calibrator,
                "probability": calibration_direction,
            }
        )
    if not direction_candidates:
        raise ValueError("no direction training window has enough target variation")
    selected_direction = max(
        direction_candidates,
        key=lambda row: (
            row["classification"]["auc"],
            -row["classification"]["brier"],
        ),
    )
    direction = selected_direction["model"]
    direction_calibrator = selected_direction["calibrator"]
    calibration_direction = selected_direction["probability"]
    calibration_actions = score_actions(
        calibration, calibration_movement, calibration_direction
    )
    threshold_candidates = threshold_table(calibration_actions)
    viable = [
        row
        for row in threshold_candidates
        if row["trades"] >= min_test_events
        and row["mean_net_pips"] > 0.0
        and row["profit_factor"] >= 1.05
    ]
    selected_threshold = (
        max(
            viable,
            key=lambda row: row["mean_net_pips"] * math.sqrt(row["trades"]),
        )["threshold"]
        if viable
        else 1.10
    )

    holdout_movement = benchmark._calibrated_probability(
        movement, movement_calibrator, holdout, features
    )
    holdout_direction = benchmark._calibrated_probability(
        direction, direction_calibrator, holdout, features
    )
    holdout_actions = score_actions(holdout, holdout_movement, holdout_direction)
    selected = holdout_actions[
        holdout_actions["movement_probability"] >= selected_threshold
    ]
    selected_metrics = benchmark.trade_metrics(selected)
    selected_metrics["bootstrap_mean_net_pips_lower_95"] = (
        benchmark._bootstrap_mean_lower(selected)
    )
    gate_checks = {
        "minimum_trades": selected_metrics["trades"] >= max(100, min_test_events),
        "positive_mean_net_pips": selected_metrics["mean_net_pips"] > 0.0,
        "profit_factor": selected_metrics["profit_factor"] >= 1.10,
        "movement_auc": (
            benchmark.classification_metrics(
                holdout["target_movement"].to_numpy(int), holdout_movement
            )["auc"]
            >= 0.52
        ),
        "direction_auc": (
            benchmark.classification_metrics(
                holdout["target_long"].to_numpy(int), holdout_direction
            )["auc"]
            >= 0.52
        ),
        "bootstrap_lower_positive": (
            selected_metrics["bootstrap_mean_net_pips_lower_95"] > 0.0
        ),
    }
    result = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "execution_policy": "shadow_only",
        "account_wired": False,
        "dataset": {
            **dataset,
            "event_rows": len(frame),
            "movement_positive_rate": float(frame["target_movement"].mean()),
            "long_best_rate": float(frame["target_long"].mean()),
        },
        "feature_contract": {
            "numeric": numeric,
            "categorical": categorical,
            "side_specific_inputs_removed": ["side_sign", "direction"],
        },
        "validation_contract": {
            "split": "time-ordered expanding walk-forward",
            "purge": "training maturity precedes test prediction start",
            "movement_target": "at least one observed-cost side has positive net pips",
            "direction_target": "LONG has at least as much net pips as SHORT",
            "threshold_selection": "movement threshold selected on calibration only",
        },
        "walk_forward": {
            "folds": folds,
            "mean_movement_auc": float(
                np.mean([row["movement"]["auc"] for row in folds])
            ),
            "mean_direction_auc": float(
                np.mean([row["direction"]["auc"] for row in folds])
            ),
            "mean_all_action_net_pips": float(
                np.mean([row["actions"]["mean_net_pips"] for row in folds])
            ),
        },
        "calibration": {
            "events": len(calibration),
            "direction_window_selection": {
                "criterion": "highest calibration direction AUC, then lowest Brier",
                "selected_fraction": selected_direction["fraction"],
                "candidates": [
                    {
                        "fraction": row["fraction"],
                        "train_events": row["train_events"],
                        "classification": row["classification"],
                        "all_actions": row["all_actions"],
                    }
                    for row in direction_candidates
                ],
            },
            "selected_threshold": selected_threshold,
            "selection_status": (
                "selected_on_calibration"
                if viable
                else "no_viable_calibration_threshold"
            ),
            "viable_threshold_count": len(viable),
            "threshold_candidates": threshold_candidates,
        },
        "holdout": {
            "events": len(holdout),
            "movement": benchmark.classification_metrics(
                holdout["target_movement"].to_numpy(int), holdout_movement
            ),
            "direction": benchmark.classification_metrics(
                holdout["target_long"].to_numpy(int), holdout_direction
            ),
            "all_actions": benchmark.trade_metrics(holdout_actions),
            "selected": selected_metrics,
            "gate_checks": gate_checks,
            "production_gate_passed": all(gate_checks.values()),
            "account_eligible": False,
        },
    }
    report_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    timestamped = report_root / f"two_stage_microstructure_{stamp}.json"
    latest = report_root / "two_stage_microstructure_latest.json"
    benchmark.atomic_json_dump(result, timestamped)
    benchmark.atomic_json_dump(result, latest)
    result["report_paths"] = [str(timestamped), str(latest)]
    if persist_artifact:
        artifact = {
            "schema_version": 1,
            "execution_policy": "shadow_only",
            "account_eligible": False,
            "numeric_features": numeric,
            "categorical_features": categorical,
            "movement_model": movement,
            "direction_model": direction,
            "movement_calibrator": movement_calibrator,
            "direction_calibrator": direction_calibrator,
            "direction_training_fraction": selected_direction["fraction"],
            "movement_threshold": selected_threshold,
            "trained_utc": utc_iso(),
        }
        model_root.mkdir(parents=True, exist_ok=True)
        artifact_path = model_root / "two_stage_logistic_latest.joblib"
        benchmark.atomic_joblib_dump(artifact, artifact_path)
        result["artifact"] = {
            "path": str(artifact_path),
            "bytes": artifact_path.stat().st_size,
            "sha256": benchmark.sha256_file(artifact_path),
        }
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panels", type=Path, nargs="+", required=True)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--min-train-events", type=int, default=500)
    parser.add_argument("--min-test-events", type=int, default=100)
    parser.add_argument("--no-artifact", action="store_true")
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--model-root", type=Path, default=MODEL_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_two_stage(
        args.panels,
        max_events=args.max_events,
        min_train_events=args.min_train_events,
        min_test_events=args.min_test_events,
        persist_artifact=not args.no_artifact,
        report_root=args.report_root,
        model_root=args.model_root,
    )
    print(
        json.dumps(
            {
                "events": result["dataset"]["event_rows"],
                "mean_movement_auc": result["walk_forward"][
                    "mean_movement_auc"
                ],
                "mean_direction_auc": result["walk_forward"][
                    "mean_direction_auc"
                ],
                "holdout_selected": result["holdout"]["selected"]["trades"],
                "production_gate_passed": result["holdout"][
                    "production_gate_passed"
                ],
                "reports": result["report_paths"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
