#!/usr/bin/env python3
"""Recreate one fitted MA-grid cell and report its pair-level economics."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from oanda_ma_feature_grid_fit import (
    add_pair_context,
    apply_calibration,
    apply_time_split_policy,
    dataset_from_payload,
    decision_centered_probability,
    movement_cost_gate_metrics,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_ARTIFACT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "models"
    / "ma_feature_grid_intensive_xgb"
    / "ma_feature_grid_latest.joblib"
)
DEFAULT_CACHE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "cache"
    / "ma_feature_grid_intensive_all_pairs_1500.joblib"
)
DEFAULT_OUTPUT_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "ma_feature_grid_intensive_audit"
)


def profit_factor(values: np.ndarray) -> float | None:
    gains = float(values[values > 0.0].sum())
    losses = float(-values[values < 0.0].sum())
    if losses <= 1e-12:
        return None if gains <= 0.0 else 999.0
    return gains / losses


def pair_rows(
    split: str,
    instruments: np.ndarray,
    selected: np.ndarray,
    predicted_up: np.ndarray,
    actual_up: np.ndarray,
    selected_net: np.ndarray,
    decision_cost: np.ndarray,
    probability: np.ndarray,
    predicted_magnitude: np.ndarray,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pair in np.unique(instruments):
        pair_mask = instruments == pair
        pair_selected = pair_mask & selected
        values = selected_net[pair_selected]
        cost_units = values / decision_cost[pair_selected]
        count = int(pair_selected.sum())
        rows.append(
            {
                "split": split,
                "instrument": pair,
                "available_n": int(pair_mask.sum()),
                "selected_n": count,
                "selected_fraction": round(
                    count / max(1, int(pair_mask.sum())),
                    6,
                ),
                "direction_accuracy": (
                    round(
                        float(
                            np.mean(
                                predicted_up[pair_selected]
                                == actual_up[pair_selected]
                            )
                        ),
                        6,
                    )
                    if count
                    else None
                ),
                "average_net_pips": (
                    round(float(np.mean(values)), 6) if count else None
                ),
                "median_net_pips": (
                    round(float(np.median(values)), 6) if count else None
                ),
                "average_net_cost_units": (
                    round(float(np.mean(cost_units)), 6) if count else None
                ),
                "win_rate": (
                    round(float(np.mean(values > 0.0)), 6) if count else None
                ),
                "profit_factor": (
                    None
                    if not count or (value := profit_factor(values)) is None
                    else round(value, 6)
                ),
                "mean_confidence": (
                    round(
                        float(
                            np.mean(
                                np.maximum(
                                    probability[pair_selected],
                                    1.0 - probability[pair_selected],
                                )
                            )
                        ),
                        6,
                    )
                    if count
                    else None
                ),
                "mean_predicted_magnitude_pips": (
                    round(
                        float(np.mean(predicted_magnitude[pair_selected])),
                        6,
                    )
                    if count
                    else None
                ),
                "mean_decision_cost_pips": (
                    round(float(np.mean(decision_cost[pair_selected])), 6)
                    if count
                    else None
                ),
            }
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument("--dataset-cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--horizon-sec", type=int, default=86400)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    artifact = joblib.load(args.artifact)
    cache = joblib.load(args.dataset_cache)
    timeframe = args.timeframe.upper()
    spec = (artifact.get("models") or {}).get(timeframe)
    dataset = dataset_from_payload((cache.get("datasets") or {}).get(timeframe))
    if not spec or dataset is None:
        raise SystemExit(f"timeframe not available: {timeframe}")
    horizons = tuple(int(value) for value in spec["horizons_sec"])
    if args.horizon_sec not in horizons:
        raise SystemExit(f"horizon not available: {args.horizon_sec}")
    horizon_index = horizons.index(args.horizon_sec)
    split_audit = apply_time_split_policy(
        dataset,
        str((artifact.get("fit") or {}).get("split_policy") or "pair_fraction"),
        max(horizons),
    )
    add_pair_context(dataset, str(spec.get("pair_context") or "none"))
    if tuple(spec["feature_names"]) != dataset.feature_names:
        raise RuntimeError("cached feature schema does not match fitted model")

    rows: list[dict[str, Any]] = []
    aggregate_checks: dict[str, Any] = {}
    for split_index, split in ((1, "validation"), (2, "holdout")):
        mask = dataset.splits == split_index
        raw_direction = np.asarray(
            spec["direction_estimator"].predict(dataset.features[mask])
        )
        raw_magnitude = np.asarray(
            spec["magnitude_estimator"].predict(dataset.features[mask])
        )
        if raw_direction.ndim == 1:
            raw_direction = raw_direction.reshape(-1, 1)
            raw_magnitude = raw_magnitude.reshape(-1, 1)
        probability = apply_calibration(
            raw_direction,
            spec["direction_calibration"],
            probability=True,
        )[:, horizon_index]
        predicted_magnitude = apply_calibration(
            raw_magnitude,
            spec["magnitude_calibration"],
            nonnegative=True,
        )[:, horizon_index]
        if spec.get("target_space") == "local_scale":
            scale_index = dataset.feature_names.index(
                str(spec["target_scale_feature"])
            )
            target_scale = np.maximum(
                0.1,
                dataset.features[mask, scale_index].astype(np.float64),
            )
            predicted_magnitude = (
                np.clip(
                    predicted_magnitude,
                    0.0,
                    float(spec["target_clip_abs"][horizon_index]),
                )
                * target_scale
            )
        threshold = float(spec["direction_thresholds"][horizon_index])
        decision_probability = decision_centered_probability(
            probability,
            threshold,
        )
        predicted_up = decision_probability >= 0.5
        actual_up = dataset.signed_pips[mask, horizon_index] > 0.0
        selected_net = np.where(
            predicted_up,
            dataset.long_net_pips[mask, horizon_index],
            dataset.short_net_pips[mask, horizon_index],
        ).astype(np.float64)
        decision_cost = np.maximum(
            0.05,
            dataset.decision_cost_pips[mask].astype(np.float64),
        )
        gate = spec["movement_cost_gates"][horizon_index]
        minimum_ratio = float(gate["minimum_magnitude_to_cost"])
        minimum_confidence = float(gate["minimum_confidence"])
        selected = (
            (predicted_magnitude / decision_cost >= minimum_ratio)
            & (
                np.maximum(
                    decision_probability,
                    1.0 - decision_probability,
                )
                >= minimum_confidence
            )
        )
        instruments = dataset.instruments[mask].astype(str)
        rows.extend(
            pair_rows(
                split,
                instruments,
                selected,
                predicted_up,
                actual_up,
                selected_net,
                decision_cost,
                decision_probability,
                predicted_magnitude,
            )
        )
        recreated = movement_cost_gate_metrics(
            dataset.signed_pips[mask, horizon_index],
            dataset.long_net_pips[mask, horizon_index],
            dataset.short_net_pips[mask, horizon_index],
            dataset.decision_cost_pips[mask],
            dataset.exact_cost[mask, horizon_index],
            instruments,
            probability,
            predicted_magnitude,
            threshold,
            gate,
        )
        expected = (spec[f"{split}_metrics"] or {})[str(args.horizon_sec)]
        fields = (
            "movement_gate_entry_n",
            "movement_gate_average_net_pips",
            "movement_gate_pair_average_net_lower_95",
            "movement_gate_pair_average_net_cost_units_lower_95",
            "movement_gate_macro_direction_accuracy_lower_95",
        )
        aggregate_checks[split] = {
            field: {
                "recreated": recreated.get(field),
                "report": expected.get(field),
                "matches": (
                    recreated.get(field) == expected.get(field)
                    if isinstance(recreated.get(field), int)
                    else math.isclose(
                        float(recreated.get(field)),
                        float(expected.get(field)),
                        abs_tol=1e-6,
                    )
                ),
            }
            for field in fields
        }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"pair_diagnostics_{timeframe.lower()}_{args.horizon_sec}_latest"
    csv_path = args.output_dir / f"{stem}.csv"
    json_path = args.output_dir / f"{stem}.json"
    frame = pd.DataFrame(rows)
    frame.to_csv(csv_path, index=False)
    payload = {
        "artifact": str(args.artifact.resolve()),
        "dataset_cache": str(args.dataset_cache.resolve()),
        "timeframe": timeframe,
        "horizon_sec": args.horizon_sec,
        "split_audit": split_audit,
        "aggregate_recreation_checks": aggregate_checks,
        "all_aggregate_checks_pass": all(
            check["matches"]
            for split in aggregate_checks.values()
            for check in split.values()
        ),
        "pair_csv": str(csv_path.resolve()),
        "pair_rows": len(frame),
    }
    json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
