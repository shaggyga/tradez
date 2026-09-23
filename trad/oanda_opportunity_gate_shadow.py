#!/usr/bin/env python3
"""Shadow-only model of whether an FX move can beat its spread cost."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

try:
    from oanda_multihorizon_panel_model import (
        HORIZONS,
        PanelDataset,
        load_panel_dataset,
        purged_partition_indices,
        spread_pips_proxy,
        utc_now,
    )
except ModuleNotFoundError:  # pragma: no cover - package import fallback
    from trad.oanda_multihorizon_panel_model import (
        HORIZONS,
        PanelDataset,
        load_panel_dataset,
        purged_partition_indices,
        spread_pips_proxy,
        utc_now,
    )


EDGE_QUANTILES = (0.0, 0.50, 0.70, 0.80, 0.90, 0.95)
DIRECTION_POLICIES = ("m5_momentum", "m5_reversal")


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def prepare_features(
    dataset: PanelDataset,
    train: np.ndarray,
    minimum_coverage: float = 0.85,
) -> tuple[np.ndarray, list[str], dict[str, Any]]:
    raw_train = dataset.raw_features[train]
    coverage = np.mean(np.isfinite(raw_train), axis=0)
    medians = np.nanmedian(raw_train, axis=0)
    filled = np.where(np.isfinite(raw_train), raw_train, medians)
    variance = np.var(filled, axis=0)
    keep = np.flatnonzero(
        (coverage >= minimum_coverage) & np.isfinite(variance) & (variance > 1e-10)
    )
    if not len(keep):
        raise ValueError("no covered opportunity features")
    matrix = dataset.raw_features[:, keep]
    matrix = np.where(np.isfinite(matrix), matrix, medians[keep])
    low = np.quantile(matrix[train], 0.005, axis=0)
    high = np.quantile(matrix[train], 0.995, axis=0)
    matrix = np.clip(matrix, low, high)
    names = [dataset.feature_names[index] for index in keep]
    return matrix, names, {
        "retained_features": len(names),
        "minimum_train_coverage": float(minimum_coverage),
        "schema_missing_rate": dataset.schema_missing_rate,
    }


def opportunity_target(dataset: PanelDataset) -> np.ndarray:
    return (np.maximum(dataset.long_net, dataset.short_net) > 0.0).astype(np.int8)


def direction_signs(dataset: PanelDataset, policy: str) -> np.ndarray:
    if "return_m5_atr" not in dataset.feature_names:
        raise ValueError("return_m5_atr is required for fixed direction policies")
    values = dataset.raw_features[:, dataset.feature_names.index("return_m5_atr")]
    signs = np.where(np.isfinite(values) & (values >= 0.0), 1, -1).astype(np.int8)
    return signs if policy == "m5_momentum" else -signs


def gated_net_metrics(
    dataset: PanelDataset,
    probabilities: np.ndarray,
    probability_indices: np.ndarray,
    directions: np.ndarray,
    threshold: float,
    horizon_sec: int,
) -> dict[str, Any]:
    selected_local = np.flatnonzero(probabilities >= float(threshold))
    selected = probability_indices[selected_local]
    if not len(selected):
        return {
            "signals": 0,
            "independent_time_blocks": 0,
            "average_net_pips": 0.0,
            "win_rate": 0.0,
            "block_ci95_lower_pips": 0.0,
        }
    realised = np.where(
        directions[selected] > 0,
        dataset.long_net[selected],
        dataset.short_net[selected],
    )
    blocks = np.floor(dataset.epochs[selected] / max(1, horizon_sec)).astype(np.int64)
    block_means = np.asarray(
        [np.mean(realised[blocks == block]) for block in np.unique(blocks)],
        dtype=np.float64,
    )
    block_mean = float(np.mean(block_means))
    lower = block_mean
    if len(block_means) > 1:
        lower -= 1.96 * float(
            np.std(block_means, ddof=1) / math.sqrt(len(block_means))
        )
    robust = realised
    if len(realised) > 1:
        robust = np.delete(realised, int(np.argmax(np.abs(realised))))
    opportunity = opportunity_target(dataset)[selected]
    return {
        "signals": int(len(selected)),
        "coverage": round(len(selected) / max(1, len(probability_indices)), 6),
        "independent_time_blocks": int(len(block_means)),
        "probability_threshold": round(float(threshold), 8),
        "opportunity_precision": round(float(np.mean(opportunity)), 6),
        "average_net_pips": round(float(np.mean(realised)), 6),
        "median_net_pips": round(float(np.median(realised)), 6),
        "average_without_largest_abs_pips": round(float(np.mean(robust)), 6),
        "win_rate": round(float(np.mean(realised > 0.0)), 6),
        "long_fraction": round(float(np.mean(directions[selected] > 0)), 6),
        "block_mean_net_pips": round(block_mean, 6),
        "block_ci95_lower_pips": round(lower, 6),
    }


def probability_metrics(target: np.ndarray, probabilities: np.ndarray) -> dict[str, Any]:
    return {
        "rows": int(len(target)),
        "prevalence": round(float(np.mean(target)), 6),
        "roc_auc": round(float(roc_auc_score(target, probabilities)), 6)
        if len(np.unique(target)) > 1
        else None,
        "average_precision": round(
            float(average_precision_score(target, probabilities)), 6
        ),
        "brier": round(float(brier_score_loss(target, probabilities)), 6),
    }


def cost_bucket_metrics(
    dataset: PanelDataset,
    probabilities: np.ndarray,
    indices: np.ndarray,
    directions: np.ndarray,
    threshold: float,
    horizon_sec: int,
) -> dict[str, dict[str, Any]]:
    spreads = spread_pips_proxy(dataset)
    masks = {
        "liquid_le_3_pips": np.isfinite(spreads) & (spreads <= 3.0),
        "medium_gt_3_le_10_pips": (
            np.isfinite(spreads) & (spreads > 3.0) & (spreads <= 10.0)
        ),
        "wide_gt_10_pips": np.isfinite(spreads) & (spreads > 10.0),
        "spread_unknown": ~np.isfinite(spreads),
    }
    output: dict[str, dict[str, Any]] = {}
    for name, mask in masks.items():
        local = np.flatnonzero(mask[indices])
        output[name] = {
            "population_rows": int(len(local)),
            **gated_net_metrics(
                dataset,
                probabilities[local],
                indices[local],
                directions,
                threshold,
                horizon_sec,
            ),
        }
    return output


def run_horizon(database: Path, horizon_sec: int, max_rows: int) -> dict[str, Any]:
    dataset = load_panel_dataset(database, horizon_sec, max_rows=max_rows)
    train, selection, holdout, partition = purged_partition_indices(
        dataset.epochs, horizon_sec
    )
    matrix, names, feature_audit = prepare_features(dataset, train)
    target = opportunity_target(dataset)
    model = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_iter=120,
        max_leaf_nodes=15,
        min_samples_leaf=50,
        l2_regularization=1.0,
        random_state=20260804,
    )
    model.fit(matrix[train], target[train])
    selection_probability = model.predict_proba(matrix[selection])[:, 1]
    holdout_probability = model.predict_proba(matrix[holdout])[:, 1]

    baseline_names = [
        name for name in ("atr_m1_pips", "live_spread_atr", "return_m5_atr")
        if name in names
    ]
    baseline_columns = [names.index(name) for name in baseline_names]
    baseline = LogisticRegression(max_iter=500, random_state=20260804)
    baseline.fit(matrix[train][:, baseline_columns], target[train])
    baseline_holdout_probability = baseline.predict_proba(
        matrix[holdout][:, baseline_columns]
    )[:, 1]

    candidates: list[dict[str, Any]] = []
    for policy in DIRECTION_POLICIES:
        directions = direction_signs(dataset, policy)
        for quantile in EDGE_QUANTILES:
            threshold = float(np.quantile(selection_probability, quantile))
            metrics = gated_net_metrics(
                dataset,
                selection_probability,
                selection,
                directions,
                threshold,
                horizon_sec,
            )
            valid = (
                metrics["signals"] >= 50
                and metrics["independent_time_blocks"] >= 20
            )
            score = metrics["block_ci95_lower_pips"] if valid else -1e9
            candidates.append(
                {
                    "direction_policy": policy,
                    "probability_quantile": float(quantile),
                    "probability_threshold": threshold,
                    "score": float(score),
                    "selection": metrics,
                }
            )
    candidates.sort(key=lambda row: row["score"], reverse=True)
    best = candidates[0]
    directions = direction_signs(dataset, best["direction_policy"])
    holdout_metrics = gated_net_metrics(
        dataset,
        holdout_probability,
        holdout,
        directions,
        best["probability_threshold"],
        horizon_sec,
    )
    full_probability = probability_metrics(target[holdout], holdout_probability)
    baseline_probability = probability_metrics(
        target[holdout], baseline_holdout_probability
    )
    full_auc = finite(full_probability.get("roc_auc"), 0.5)
    baseline_auc = finite(baseline_probability.get("roc_auc"), 0.5)
    checks = {
        "selection_positive_ci95_lower": best["selection"][
            "block_ci95_lower_pips"
        ] > 0.0,
        "opportunity_auc_beats_cost_vol_baseline_by_0_02": (
            full_auc >= baseline_auc + 0.02
        ),
        "holdout_positive_average": holdout_metrics["average_net_pips"] > 0.0,
        "holdout_positive_ci95_lower": (
            holdout_metrics["block_ci95_lower_pips"] > 0.0
        ),
        "holdout_minimum_20_blocks": (
            holdout_metrics["independent_time_blocks"] >= 20
        ),
        "holdout_robust_without_largest": (
            holdout_metrics["average_without_largest_abs_pips"] > 0.0
        ),
    }
    return {
        "rows": dataset.rows,
        "horizon_sec": int(horizon_sec),
        "target": "max(realised_long_net_pips, realised_short_net_pips) > 0",
        "partition": partition,
        "feature_audit": feature_audit,
        "model": "hist_gradient_boosted_cost_beating_opportunity_v1",
        "baseline_model": "logistic_atr_spread_m5_return",
        "holdout_opportunity_probability": full_probability,
        "holdout_baseline_probability": baseline_probability,
        "selected_gate": {
            key: value for key, value in best.items() if key != "selection"
        },
        "selection": best["selection"],
        "untouched_holdout": holdout_metrics,
        "untouched_holdout_by_cost_bucket": cost_bucket_metrics(
            dataset,
            holdout_probability,
            holdout,
            directions,
            best["probability_threshold"],
            horizon_sec,
        ),
        "promotion_checks": checks,
        "promotion_ready": all(checks.values()),
        "shadow_decision": (
            "forward_observe_only" if all(checks.values()) else "reject_historical_candidate"
        ),
        "selection_candidates": candidates,
    }


def run_audit(
    database: Path,
    output: Path,
    horizons: Iterable[str] = ("M5", "M15"),
    max_rows: int = 80_000,
) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    failures: dict[str, str] = {}
    for label in horizons:
        name = str(label).upper()
        if name not in HORIZONS:
            failures[name] = "unsupported_horizon"
            continue
        try:
            reports[name] = run_horizon(database, HORIZONS[name], max_rows)
        except (OSError, ValueError) as exc:
            failures[name] = f"{type(exc).__name__}: {exc}"
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "model": "cost_beating_opportunity_gate_v1",
        "database": str(Path(database).resolve()),
        "research_only": True,
        "shadow_only": True,
        "account_eligible": False,
        "execution_adapter": False,
        "holdout_used_for_selection": False,
        "horizons": reports,
        "failures": failures,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(output)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--horizons", nargs="+", default=["M5", "M15"])
    parser.add_argument("--max-rows", type=int, default=80_000)
    args = parser.parse_args()
    payload = run_audit(
        args.database,
        args.output,
        horizons=args.horizons,
        max_rows=max(5_000, int(args.max_rows)),
    )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "horizons": sorted(payload["horizons"]),
                "failures": payload["failures"],
            },
            indent=2,
        )
    )
    return 0 if payload["horizons"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "cost_bucket_metrics",
    "direction_signs",
    "gated_net_metrics",
    "opportunity_target",
    "prepare_features",
    "probability_metrics",
    "run_audit",
    "run_horizon",
]
