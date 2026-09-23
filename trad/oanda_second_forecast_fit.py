#!/usr/bin/env python3
"""Fit compact causal ridge forecasts from OANDA S5 bid/ask history."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

try:
    from oanda_second_forecast import (
        DEFAULT_EXECUTION_HORIZONS_SEC,
        DEFAULT_HORIZONS_SEC,
        FEATURE_NAMES,
        INPUT_TIMEFRAME,
        MODEL_FAMILY,
        PROFILE_EXECUTION,
        SCHEMA_VERSION,
        TRAINING_TIMEFRAME,
        atomic_json,
        matrix_lane_id,
    )
except ModuleNotFoundError:
    from trad.oanda_second_forecast import (
        DEFAULT_EXECUTION_HORIZONS_SEC,
        DEFAULT_HORIZONS_SEC,
        FEATURE_NAMES,
        INPUT_TIMEFRAME,
        MODEL_FAMILY,
        PROFILE_EXECUTION,
        SCHEMA_VERSION,
        TRAINING_TIMEFRAME,
        atomic_json,
        matrix_lane_id,
    )


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_SOURCE = DATA_ROOT / "candles_s5_bam"
DEFAULT_OUTPUT = DATA_ROOT / "state" / "second_ridge_models_v1.json"
DEFAULT_REPORT = DATA_ROOT / "reports" / "second_ridge_fit_v1.json"
LIQUID_PAIRS = (
    "EUR_USD",
    "USD_JPY",
    "GBP_USD",
    "AUD_USD",
    "USD_CAD",
    "USD_CHF",
    "NZD_USD",
    "EUR_GBP",
)
PIP_LOCATION_MINUS2 = {
    "AUD_JPY",
    "CAD_JPY",
    "CHF_JPY",
    "EUR_HUF",
    "EUR_JPY",
    "GBP_JPY",
    "NZD_JPY",
    "SGD_JPY",
    "TRY_JPY",
    "USD_HUF",
    "USD_JPY",
    "USD_THB",
    "ZAR_JPY",
}
PROFILE_QUANTILES = {
    "fast": 0.70,
    "balanced": 0.85,
    "strict": 0.95,
}
DEFAULT_SMOOTHING_LAMBDAS = (0.0, 0.05, 0.15, 0.35, 0.75, 1.5)
MIN_PAIR_SPECIFIC_ROWS = 400
MIN_TRAIN_ROWS = 200
MIN_VALIDATION_ROWS = 50
MIN_HOLDOUT_ROWS = 50


def _json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_coverage(payload: dict[str, Any]) -> dict[str, Any]:
    models = payload.get("models") or {}
    inferred_count = sum(
        len(rows) for rows in models.values() if isinstance(rows, dict)
    ) if isinstance(models, dict) else 0
    return {
        "model_count": max(0, int(payload.get("model_count") or inferred_count)),
        "pair_specific_model_count": max(
            0, int(payload.get("pair_specific_model_count") or 0)
        ),
        "complete_horizon_pair_count": max(
            0, int(payload.get("complete_horizon_pair_count") or 0)
        ),
        "horizons_sec": sorted(
            {int(value) for value in (payload.get("horizons_sec") or [])}
        ),
    }


def model_artifact_replacement_decision(
    candidate: dict[str, Any],
    incumbent: dict[str, Any],
    *,
    allow_coverage_regression: bool = False,
) -> dict[str, Any]:
    """Protect a trained artifact from empty or narrower unattended refits."""

    candidate_coverage = _artifact_coverage(candidate)
    incumbent_coverage = _artifact_coverage(incumbent)
    blockers: list[str] = []
    if not allow_coverage_regression and incumbent_coverage["model_count"] > 0:
        for key in (
            "model_count",
            "pair_specific_model_count",
            "complete_horizon_pair_count",
        ):
            if candidate_coverage[key] < incumbent_coverage[key]:
                blockers.append(f"{key}_regression")
        missing_horizons = sorted(
            set(incumbent_coverage["horizons_sec"])
            - set(candidate_coverage["horizons_sec"])
        )
        if missing_horizons:
            blockers.append("horizon_coverage_regression")
    return {
        "replaced": not blockers,
        "reason": (
            "explicit_coverage_regression_override"
            if allow_coverage_regression and incumbent_coverage["model_count"] > 0
            else "coverage_non_regression_passed"
            if not blockers
            else "incumbent_preserved"
        ),
        "blockers": blockers,
        "candidate": candidate_coverage,
        "incumbent": incumbent_coverage,
    }


def reconcile_fit_report(
    candidate_report: dict[str, Any],
    incumbent_report: dict[str, Any],
    artifact_update: dict[str, Any],
) -> dict[str, Any]:
    """Keep audit evidence aligned with the artifact that remains active."""

    if artifact_update.get("replaced"):
        return candidate_report
    incumbent_coverage = artifact_update.get("incumbent") or {}
    incumbent_report_coverage = _artifact_coverage(incumbent_report)
    if (
        int(incumbent_coverage.get("model_count") or 0) > 0
        and incumbent_report_coverage["model_count"]
        == int(incumbent_coverage.get("model_count") or 0)
    ):
        report = dict(incumbent_report)
        report["artifact_update"] = artifact_update
        report["latest_rejected_fit"] = {
            "fitted_utc": candidate_report.get("fitted_utc"),
            "source_root": candidate_report.get("source_root"),
            "elapsed_sec": candidate_report.get("elapsed_sec"),
            "coverage": artifact_update.get("candidate") or {},
            "errors": candidate_report.get("errors") or {},
        }
        return report
    report = dict(candidate_report)
    report["effective_artifact_coverage"] = incumbent_coverage
    return report


def pip_multiplier(instrument: str) -> float:
    return 100.0 if instrument in PIP_LOCATION_MINUS2 else 10_000.0


def column_numpy(table: Any, name: str, dtype: Any = float) -> np.ndarray:
    return np.asarray(table.column(name).combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype)


def window_sum(values: np.ndarray, end: np.ndarray, width: int) -> np.ndarray:
    prefix = np.concatenate(([0.0], np.cumsum(values, dtype=float)))
    return prefix[end] - prefix[end - width]


def window_std(values: np.ndarray, end: np.ndarray, width: int) -> np.ndarray:
    total = window_sum(values, end, width)
    squared = window_sum(values * values, end, width)
    variance = np.maximum(0.0, squared / width - (total / width) ** 2)
    return np.sqrt(variance)


def feature_matrix(path: Path, sample_step: int, max_horizon_sec: int) -> dict[str, Any]:
    names = [
        "dt",
        "bid_close",
        "ask_close",
        "mid_close",
        "mid_high",
        "mid_low",
        "spread_pips",
        "volume",
    ]
    table = pq.read_table(path, columns=names)
    instrument = path.stem.removesuffix("_S5")
    times = column_numpy(table, "dt", "datetime64[ns]").astype(np.int64)
    bid = column_numpy(table, "bid_close")
    ask = column_numpy(table, "ask_close")
    mid = column_numpy(table, "mid_close")
    high = column_numpy(table, "mid_high")
    low = column_numpy(table, "mid_low")
    spread = column_numpy(table, "spread_pips")
    volume = column_numpy(table, "volume")
    multiplier = pip_multiplier(instrument)
    indices = np.arange(12, len(mid), max(1, sample_step), dtype=np.int64)
    continuity = times[indices] - times[indices - 12]
    latest_origin_ns = times[-1] - int(max_horizon_sec * 1_000_000_000)
    valid = (
        (continuity >= 55_000_000_000)
        & (continuity <= 70_000_000_000)
        & (times[indices] <= latest_origin_ns)
    )
    indices = indices[valid]
    moves = np.diff(mid) * multiplier
    r5 = (mid[indices] - mid[indices - 1]) * multiplier
    r30 = (mid[indices] - mid[indices - 6]) * multiplier
    spread_mean60 = window_sum(spread, indices + 1, 12) / 12.0
    activity30 = window_sum(volume, indices + 1, 6)
    activity60 = window_sum(volume, indices + 1, 12)
    high_windows = np.lib.stride_tricks.sliding_window_view(high, 6)
    low_windows = np.lib.stride_tricks.sliding_window_view(low, 6)
    range30 = (
        np.max(high_windows[indices - 5], axis=1)
        - np.min(low_windows[indices - 5], axis=1)
    ) * multiplier
    seconds = times[indices] / 1_000_000_000.0
    hours = (seconds % 86_400.0) / 3_600.0
    matrix = np.column_stack(
        [
            r5,
            (mid[indices] - mid[indices - 2]) * multiplier,
            r30,
            (mid[indices] - mid[indices - 12]) * multiplier,
            r5 - r30 / 6.0,
            window_std(moves, indices, 6),
            window_std(moves, indices, 12),
            range30,
            spread[indices],
            spread[indices] / np.maximum(spread_mean60, 0.01),
            activity30,
            activity30 / np.maximum(activity60 / 2.0, 1.0),
            np.sin(2.0 * np.pi * hours / 24.0),
            np.cos(2.0 * np.pi * hours / 24.0),
        ]
    )
    finite = np.all(np.isfinite(matrix), axis=1)
    return {
        "instrument": instrument,
        "sample_step_bars": max(1, int(sample_step)),
        "times": times,
        "indices": indices[finite],
        "features": matrix[finite],
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "multiplier": multiplier,
        "source_rows": len(mid),
    }


def ridge_fit(
    train_x: np.ndarray,
    train_y: np.ndarray,
    validation_x: np.ndarray,
    validation_y: np.ndarray,
) -> dict[str, Any]:
    means = np.mean(train_x, axis=0)
    scales = np.std(train_x, axis=0)
    scales = np.where(scales < 1e-9, 1.0, scales)
    normalized = (train_x - means) / scales
    validation_normalized = (validation_x - means) / scales
    intercept = float(np.mean(train_y))
    centered = train_y - intercept
    identity = np.eye(normalized.shape[1])
    candidates = []
    for alpha in (0.1, 1.0, 10.0, 100.0, 1000.0):
        coefficients = np.linalg.solve(
            normalized.T @ normalized + alpha * identity,
            normalized.T @ centered,
        )
        prediction = intercept + validation_normalized @ coefficients
        candidates.append((float(np.mean((validation_y - prediction) ** 2)), alpha, coefficients, prediction))
    mse, alpha, coefficients, validation_prediction = min(candidates, key=lambda row: row[0])
    return {
        "means": means,
        "scales": scales,
        "intercept": intercept,
        "coefficients": coefficients,
        "alpha": alpha,
        "validation_prediction": validation_prediction,
        "validation_mse": mse,
    }


def signal_metrics(
    prediction: np.ndarray,
    actual_signed: np.ndarray,
    entry_bid: np.ndarray,
    entry_ask: np.ndarray,
    future_bid: np.ndarray,
    future_ask: np.ndarray,
    multiplier: float,
    threshold: float,
) -> dict[str, Any]:
    selected = np.abs(prediction) >= threshold
    count = int(np.sum(selected))
    if not count:
        return {"n": 0, "avg_net_pips": 0.0, "win_rate": 0.0, "lower_95_pips": -999.0}
    buy = prediction[selected] >= 0.0
    net = np.where(
        buy,
        (future_bid[selected] - entry_ask[selected]) * multiplier,
        (entry_bid[selected] - future_ask[selected]) * multiplier,
    )
    average = float(np.mean(net))
    standard = float(np.std(net))
    return {
        "n": count,
        "signal_rate": float(count / len(prediction)),
        "avg_net_pips": average,
        "median_net_pips": float(np.median(net)),
        "win_rate": float(np.mean(net > 0.0) * 100.0),
        "lower_95_pips": float(average - 1.96 * standard / math.sqrt(max(1, count))),
        "direction_accuracy": float(np.mean(np.sign(prediction[selected]) == np.sign(actual_signed[selected])) * 100.0),
    }


def raw_model_parameters(model: dict[str, Any]) -> tuple[float, np.ndarray]:
    means = np.asarray(model["feature_means"], dtype=float)
    scales = np.asarray(model["feature_scales"], dtype=float)
    coefficients = np.asarray(model["coefficients"], dtype=float)
    weights = coefficients / np.maximum(scales, 1e-9)
    intercept = float(model["intercept"]) - float(means @ weights)
    return intercept, weights


def smooth_horizon_parameters(
    pair_models: dict[str, dict[str, Any]],
    smoothing_lambda: float,
) -> dict[int, tuple[float, np.ndarray]]:
    ordered = sorted(
        (
            int(horizon_text),
            model,
            raw_model_parameters(model),
        )
        for horizon_text, model in pair_models.items()
    )
    if smoothing_lambda <= 0.0 or len(ordered) < 2:
        return {
            horizon: (intercept, weights.copy())
            for horizon, _, (intercept, weights) in ordered
        }

    normalized: list[np.ndarray] = []
    target_scales: list[float] = []
    for _, model, (intercept, weights) in ordered:
        context = model["_selection_context"]
        target_scale = max(0.05, float(np.std(context["validation_actual"])))
        target_scales.append(target_scale)
        normalized.append(
            np.concatenate(([intercept / target_scale], weights / target_scale))
        )

    output: dict[int, tuple[float, np.ndarray]] = {}
    for index, (horizon, _, _) in enumerate(ordered):
        numerator = normalized[index].copy()
        denominator = 1.0
        for neighbor_index in (index - 1, index + 1):
            if not 0 <= neighbor_index < len(ordered):
                continue
            neighbor_horizon = ordered[neighbor_index][0]
            log_distance = max(0.20, abs(math.log(neighbor_horizon / horizon)))
            neighbor_weight = 1.0 / log_distance
            numerator += smoothing_lambda * neighbor_weight * normalized[neighbor_index]
            denominator += smoothing_lambda * neighbor_weight
        smoothed = numerator / denominator * target_scales[index]
        output[horizon] = (float(smoothed[0]), smoothed[1:].copy())
    return output


def validation_after_cost_score(
    model: dict[str, Any],
    parameters: tuple[float, np.ndarray],
) -> float:
    context = model["_selection_context"]
    intercept, weights = parameters
    validation_prediction = intercept + context["validation_x"] @ weights
    absolute = np.abs(validation_prediction)
    profile_scores: list[float] = []
    for quantile in PROFILE_QUANTILES.values():
        threshold = float(np.quantile(absolute, quantile))
        metrics = signal_metrics(
            validation_prediction,
            context["validation_actual"],
            context["validation_entry_bid"],
            context["validation_entry_ask"],
            context["validation_future_bid"],
            context["validation_future_ask"],
            context["multiplier"],
            threshold,
        )
        count = int(metrics.get("n") or 0)
        if count <= 0:
            continue
        conservative = max(-20.0, min(20.0, float(metrics["lower_95_pips"])))
        average = max(-20.0, min(20.0, float(metrics["avg_net_pips"])))
        sample_weight = min(1.0, math.sqrt(count / 50.0))
        profile_scores.append((0.70 * conservative + 0.30 * average) * sample_weight)
    return float(np.mean(profile_scores)) if profile_scores else -999.0


def holdout_after_cost_score(
    model: dict[str, Any],
    parameters: tuple[float, np.ndarray],
) -> float:
    context = model["_selection_context"]
    intercept, weights = parameters
    validation_prediction = intercept + context["validation_x"] @ weights
    holdout_prediction = intercept + context["holdout_x"] @ weights
    validation_absolute = np.abs(validation_prediction)
    profile_scores: list[float] = []
    for quantile in PROFILE_QUANTILES.values():
        threshold = float(np.quantile(validation_absolute, quantile))
        metrics = signal_metrics(
            holdout_prediction,
            context["holdout_actual"],
            context["holdout_entry_bid"],
            context["holdout_entry_ask"],
            context["holdout_future_bid"],
            context["holdout_future_ask"],
            context["multiplier"],
            threshold,
        )
        count = int(metrics.get("n") or 0)
        if count <= 0:
            continue
        conservative = max(-20.0, min(20.0, float(metrics["lower_95_pips"])))
        average = max(-20.0, min(20.0, float(metrics["avg_net_pips"])))
        sample_weight = min(1.0, math.sqrt(count / 50.0))
        profile_scores.append((0.70 * conservative + 0.30 * average) * sample_weight)
    return float(np.mean(profile_scores)) if profile_scores else -999.0


def refresh_horizon_model(
    model: dict[str, Any],
    parameters: tuple[float, np.ndarray],
    smoothing: dict[str, Any],
) -> None:
    context = model["_selection_context"]
    raw_intercept, raw_weights = parameters
    validation_prediction = raw_intercept + context["validation_x"] @ raw_weights
    holdout_prediction = raw_intercept + context["holdout_x"] @ raw_weights
    validation_actual = context["validation_actual"]
    holdout_actual = context["holdout_actual"]
    validation_abs = np.abs(validation_prediction)

    profiles: dict[str, Any] = {}
    for profile, quantile in PROFILE_QUANTILES.items():
        threshold = float(np.quantile(validation_abs, quantile))
        validation_metrics = signal_metrics(
            validation_prediction,
            validation_actual,
            context["validation_entry_bid"],
            context["validation_entry_ask"],
            context["validation_future_bid"],
            context["validation_future_ask"],
            context["multiplier"],
            threshold,
        )
        holdout_metrics = signal_metrics(
            holdout_prediction,
            holdout_actual,
            context["holdout_entry_bid"],
            context["holdout_entry_ask"],
            context["holdout_future_bid"],
            context["holdout_future_ask"],
            context["multiplier"],
            threshold,
        )
        selected_validation = validation_abs >= threshold
        selected_spread = context["validation_spread"][selected_validation]
        profiles[profile] = {
            "score_threshold": threshold,
            "max_spread_pips": (
                float(np.quantile(selected_spread, 0.95))
                if len(selected_spread) else 3.0
            ),
            "validation": validation_metrics,
            "holdout": holdout_metrics,
            "historical_gate_passed": bool(
                holdout_metrics["n"] >= 100
                and holdout_metrics["avg_net_pips"] > 0.0
                and holdout_metrics["lower_95_pips"] > 0.0
                and holdout_metrics["win_rate"] >= 52.0
            ),
        }

    aligned = np.sign(validation_prediction) * validation_actual
    denominator = float(np.sum(np.abs(validation_prediction) ** 2))
    magnitude_calibration = (
        float(np.sum(np.abs(validation_prediction) * aligned) / denominator)
        if denominator > 1e-12 else 1.0
    )
    residual = validation_actual - validation_prediction
    correlation = (
        float(np.corrcoef(validation_prediction, validation_actual)[0, 1])
        if np.std(validation_prediction) > 1e-12
        and np.std(validation_actual) > 1e-12
        else 0.0
    )
    orientation_threshold = float(np.quantile(validation_abs, 0.85))
    direct_orientation = signal_metrics(
        validation_prediction,
        validation_actual,
        context["validation_entry_bid"],
        context["validation_entry_ask"],
        context["validation_future_bid"],
        context["validation_future_ask"],
        context["multiplier"],
        orientation_threshold,
    )
    inverse_orientation = signal_metrics(
        -validation_prediction,
        validation_actual,
        context["validation_entry_bid"],
        context["validation_entry_ask"],
        context["validation_future_bid"],
        context["validation_future_ask"],
        context["multiplier"],
        orientation_threshold,
    )

    means = np.asarray(model["feature_means"], dtype=float)
    scales = np.asarray(model["feature_scales"], dtype=float)
    model["intercept"] = float(raw_intercept + means @ raw_weights)
    model["coefficients"] = (raw_weights * scales).tolist()
    model["magnitude_calibration"] = max(0.0, min(10.0, magnitude_calibration))
    model["residual_std_pips"] = float(np.std(residual))
    model["profiles"] = profiles
    model["smoothing"] = smoothing
    model["fit_metrics"] = {
        "validation_mse": float(np.mean(residual ** 2)),
        "validation_correlation": correlation,
        "validation_selected_orientation": context["selected_orientation"],
        "validation_direct_net_pips": direct_orientation["avg_net_pips"],
        "validation_inverse_net_pips": inverse_orientation["avg_net_pips"],
        "validation_direction_accuracy": float(
            np.mean(np.sign(validation_prediction) == np.sign(validation_actual))
            * 100.0
        ),
        "selection_split": "middle chronological 20% after costs",
        "evaluation_split": "newest chronological 20% untouched holdout",
    }


def apply_multi_horizon_smoothing(
    pair_models: dict[str, dict[str, Any]],
    smoothing_lambdas: list[float] | tuple[float, ...],
) -> dict[str, Any]:
    candidates = sorted({max(0.0, float(value)) for value in smoothing_lambdas} | {0.0})
    candidate_scores: dict[float, float] = {}
    parameter_surfaces: dict[float, dict[int, tuple[float, np.ndarray]]] = {}
    for smoothing_lambda in candidates:
        parameters = smooth_horizon_parameters(pair_models, smoothing_lambda)
        parameter_surfaces[smoothing_lambda] = parameters
        horizon_scores = [
            validation_after_cost_score(model, parameters[int(horizon_text)])
            for horizon_text, model in pair_models.items()
        ]
        candidate_scores[smoothing_lambda] = (
            float(np.mean(horizon_scores)) if horizon_scores else -999.0
        )

    baseline_score = candidate_scores[0.0]
    selected_lambda = max(
        candidates,
        key=lambda value: (candidate_scores[value], -value),
    )
    if candidate_scores[selected_lambda] <= baseline_score + 1e-9:
        selected_lambda = 0.0
    selected_score = candidate_scores[selected_lambda]
    improved = selected_lambda > 0.0 and selected_score > baseline_score
    baseline_holdout_score = float(
        np.mean(
            [
                holdout_after_cost_score(
                    model,
                    parameter_surfaces[0.0][int(horizon_text)],
                )
                for horizon_text, model in pair_models.items()
            ]
        )
    )
    selected_holdout_score = float(
        np.mean(
            [
                holdout_after_cost_score(
                    model,
                    parameter_surfaces[selected_lambda][int(horizon_text)],
                )
                for horizon_text, model in pair_models.items()
            ]
        )
    )
    smoothing = {
        "method": "adjacent_log_horizon_parameter_regularization",
        "selected_lambda": selected_lambda,
        "selected_on": "middle chronological 20% executable bid/ask returns",
        "evaluated_on": "newest chronological 20% untouched holdout",
        "baseline_validation_score": round(baseline_score, 6),
        "selected_validation_score": round(selected_score, 6),
        "validation_improvement": round(selected_score - baseline_score, 6),
        "baseline_holdout_score": round(baseline_holdout_score, 6),
        "selected_holdout_score": round(selected_holdout_score, 6),
        "holdout_improvement": round(
            selected_holdout_score - baseline_holdout_score,
            6,
        ),
        "holdout_improved": selected_holdout_score > baseline_holdout_score,
        "improved": improved,
        "candidate_scores": {
            str(value): round(candidate_scores[value], 6)
            for value in candidates
        },
    }
    selected_parameters = parameter_surfaces[selected_lambda]
    for horizon_text, model in pair_models.items():
        refresh_horizon_model(
            model,
            selected_parameters[int(horizon_text)],
            smoothing,
        )
        model.pop("_selection_context", None)
    return smoothing


def fit_horizon(dataset: dict[str, Any], horizon_sec: int) -> dict[str, Any] | None:
    source_indices = dataset["indices"]
    times = dataset["times"]
    target_ns = times[source_indices] + int(horizon_sec * 1_000_000_000)
    future = np.searchsorted(times, target_ns, side="left")
    in_bounds = future < len(times)
    source_indices = source_indices[in_bounds]
    target_ns = target_ns[in_bounds]
    future = future[in_bounds]
    delay_ns = times[future] - target_ns
    valid = (delay_ns >= 0) & (delay_ns <= 7_000_000_000)
    indices = source_indices[valid]
    future = future[valid]
    x = dataset["features"][in_bounds][valid]
    if len(x) < MIN_PAIR_SPECIFIC_ROWS:
        return None
    actual = (dataset["mid"][future] - dataset["mid"][indices]) * dataset["multiplier"]
    first_boundary = int(len(x) * 0.60)
    second_boundary = int(len(x) * 0.80)
    origin_times = times[indices]
    purge_ns = int((horizon_sec + 7.0) * 1_000_000_000)
    train_stop = min(
        first_boundary,
        int(
            np.searchsorted(
                origin_times,
                origin_times[first_boundary] - purge_ns,
                side="right",
            )
        ),
    )
    validation_stop = min(
        second_boundary,
        int(
            np.searchsorted(
                origin_times,
                origin_times[second_boundary] - purge_ns,
                side="right",
            )
        ),
    )
    train_slice = slice(0, train_stop)
    validation_slice = slice(first_boundary, validation_stop)
    holdout_slice = slice(second_boundary, len(x))
    if (
        train_slice.stop < MIN_TRAIN_ROWS
        or validation_slice.stop - validation_slice.start < MIN_VALIDATION_ROWS
        or holdout_slice.stop - holdout_slice.start < MIN_HOLDOUT_ROWS
    ):
        return None
    fit = ridge_fit(x[train_slice], actual[train_slice], x[validation_slice], actual[validation_slice])
    validation_prediction = fit["validation_prediction"]
    validation_actual = actual[validation_slice]
    orientation_threshold = float(np.quantile(np.abs(validation_prediction), 0.85))
    direct_orientation = signal_metrics(
        validation_prediction,
        validation_actual,
        dataset["bid"][indices[validation_slice]],
        dataset["ask"][indices[validation_slice]],
        dataset["bid"][future[validation_slice]],
        dataset["ask"][future[validation_slice]],
        dataset["multiplier"],
        orientation_threshold,
    )
    inverse_orientation = signal_metrics(
        -validation_prediction,
        validation_actual,
        dataset["bid"][indices[validation_slice]],
        dataset["ask"][indices[validation_slice]],
        dataset["bid"][future[validation_slice]],
        dataset["ask"][future[validation_slice]],
        dataset["multiplier"],
        orientation_threshold,
    )
    orientation = (
        -1.0
        if (inverse_orientation["lower_95_pips"], inverse_orientation["avg_net_pips"])
        > (direct_orientation["lower_95_pips"], direct_orientation["avg_net_pips"])
        else 1.0
    )
    oriented_coefficients = fit["coefficients"] * orientation
    oriented_intercept = fit["intercept"] * orientation
    raw_weights = oriented_coefficients / np.maximum(fit["scales"], 1e-9)
    raw_intercept = float(oriented_intercept - fit["means"] @ raw_weights)
    model = {
        "model_id": (
            f"{MODEL_FAMILY}.{INPUT_TIMEFRAME.lower()}."
            f"{dataset['instrument']}.h{horizon_sec}"
        ),
        "model_family": MODEL_FAMILY,
        "input_timeframe": INPUT_TIMEFRAME,
        "training_timeframe": TRAINING_TIMEFRAME,
        "horizon_sec": horizon_sec,
        "source_granularity": "S5",
        "sample_step_bars": int(dataset.get("sample_step_bars") or 1),
        "fit_provenance": "pair_specific",
        "pair_specific_evidence": True,
        "account_eligible": True,
        "train_rows": int(train_slice.stop),
        "validation_rows": int(validation_slice.stop - validation_slice.start),
        "holdout_rows": int(holdout_slice.stop - holdout_slice.start),
        "feature_means": fit["means"].tolist(),
        "feature_scales": fit["scales"].tolist(),
        "intercept": oriented_intercept,
        "coefficients": oriented_coefficients.tolist(),
        "alpha": fit["alpha"],
        "_selection_context": {
            "validation_x": x[validation_slice],
            "validation_actual": validation_actual,
            "validation_entry_bid": dataset["bid"][indices[validation_slice]],
            "validation_entry_ask": dataset["ask"][indices[validation_slice]],
            "validation_future_bid": dataset["bid"][future[validation_slice]],
            "validation_future_ask": dataset["ask"][future[validation_slice]],
            "validation_spread": dataset["spread"][indices[validation_slice]],
            "holdout_x": x[holdout_slice],
            "holdout_actual": actual[holdout_slice],
            "holdout_entry_bid": dataset["bid"][indices[holdout_slice]],
            "holdout_entry_ask": dataset["ask"][indices[holdout_slice]],
            "holdout_future_bid": dataset["bid"][future[holdout_slice]],
            "holdout_future_ask": dataset["ask"][future[holdout_slice]],
            "multiplier": dataset["multiplier"],
            "selected_orientation": "direct" if orientation > 0.0 else "inverse",
        },
    }
    refresh_horizon_model(
        model,
        (raw_intercept, raw_weights),
        {
            "method": "independent_horizon_baseline",
            "selected_lambda": 0.0,
            "selected_on": "middle chronological 20% executable bid/ask returns",
            "evaluated_on": "newest chronological 20% untouched holdout",
            "baseline_validation_score": None,
            "selected_validation_score": None,
            "validation_improvement": 0.0,
            "improved": False,
            "candidate_scores": {"0.0": None},
        },
    )
    return model


def selected_paths(source: Path, pairs: str) -> list[Path]:
    available = {path.stem.removesuffix("_S5"): path for path in source.glob("*_S5.parquet")}
    if pairs.strip().lower() == "all":
        names = sorted(available)
    elif pairs.strip().lower() in {"liquid", "majors"}:
        names = [name for name in LIQUID_PAIRS if name in available]
    else:
        names = [item.strip().upper().replace("/", "_") for item in pairs.split(",") if item.strip()]
    return [available[name] for name in names if name in available]


def adaptive_sample_steps(initial_step: int) -> list[int]:
    initial = max(1, int(initial_step))
    return list(dict.fromkeys([initial, max(1, initial // 2), 1]))


def fit_pair_adaptive(
    path: Path,
    horizons: list[int] | tuple[int, ...],
    initial_step: int,
    smoothing_lambdas: list[float] | tuple[float, ...],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    best_models: dict[str, Any] = {}
    best_dataset: dict[str, Any] = {}
    attempts: list[dict[str, Any]] = []
    for sample_step in adaptive_sample_steps(initial_step):
        dataset = feature_matrix(path, sample_step, max(horizons))
        candidate_models: dict[str, Any] = {}
        for horizon in horizons:
            model = fit_horizon(dataset, horizon)
            if model is not None:
                candidate_models[str(horizon)] = model
        attempts.append(
            {
                "sample_step_bars": sample_step,
                "feature_rows": len(dataset["features"]),
                "pair_specific_models": len(candidate_models),
            }
        )
        if len(candidate_models) > len(best_models):
            best_models = candidate_models
            best_dataset = dataset
        elif (
            not candidate_models
            and not best_models
            and len(dataset["features"])
            > len(best_dataset.get("features", ()))
        ):
            best_dataset = dataset
        if len(candidate_models) == len(horizons):
            break
    smoothing: dict[str, Any] = {}
    if best_models:
        smoothing = apply_multi_horizon_smoothing(
            best_models,
            smoothing_lambdas,
        )
    return best_models, best_dataset, {
        "attempts": attempts,
        "selected_sample_step_bars": int(
            best_dataset.get("sample_step_bars") or initial_step
        )
        if best_dataset
        else None,
        "smoothing": smoothing,
    }


def pooled_proxy_model(
    instrument: str,
    horizon_sec: int,
    fitted_models: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    quote_currency = instrument.rsplit("_", 1)[-1]
    same_quote: list[tuple[str, dict[str, Any]]] = []
    global_candidates: list[tuple[str, dict[str, Any]]] = []
    for donor_instrument, pair_models in fitted_models.items():
        if donor_instrument == instrument:
            continue
        model = pair_models.get(str(horizon_sec))
        if not model or model.get("fit_provenance") != "pair_specific":
            continue
        row = (donor_instrument, model)
        global_candidates.append(row)
        if donor_instrument.endswith(f"_{quote_currency}"):
            same_quote.append(row)
    used_same_quote = len(same_quote) >= 2
    candidates = same_quote if used_same_quote else global_candidates
    if not candidates:
        return None

    def quality(item: tuple[str, dict[str, Any]]) -> tuple[float, int]:
        _, model = item
        holdouts = [
            profile.get("holdout") or {}
            for profile in (model.get("profiles") or {}).values()
        ]
        return (
            max(
                (float(row.get("lower_95_pips") or -999.0) for row in holdouts),
                default=-999.0,
            ),
            max((int(row.get("n") or 0) for row in holdouts), default=0),
        )

    donors = sorted(candidates, key=quality, reverse=True)[:5]
    raw_parameters = [raw_model_parameters(model) for _, model in donors]
    raw_intercept = float(statistics.fmean(value[0] for value in raw_parameters))
    raw_weights = np.mean(
        np.vstack([value[1] for value in raw_parameters]),
        axis=0,
    )
    donor_models = [model for _, model in donors]
    empty_metrics = {
        "n": 0,
        "signal_rate": 0.0,
        "avg_net_pips": 0.0,
        "median_net_pips": 0.0,
        "win_rate": 0.0,
        "lower_95_pips": -999.0,
        "direction_accuracy": 0.0,
    }
    profiles: dict[str, Any] = {}
    for profile_name in PROFILE_QUANTILES:
        donor_profiles = [
            (model.get("profiles") or {}).get(profile_name) or {}
            for model in donor_models
        ]
        profiles[profile_name] = {
            "score_threshold": float(
                statistics.median(
                    float(row.get("score_threshold") or 0.0)
                    for row in donor_profiles
                )
            ),
            "max_spread_pips": float(
                statistics.median(
                    float(row.get("max_spread_pips") or 3.0)
                    for row in donor_profiles
                )
            ),
            "validation": dict(empty_metrics),
            "holdout": dict(empty_metrics),
            "historical_gate_passed": False,
        }
    provenance = (
        "quote_currency_pooled_proxy"
        if used_same_quote
        else "global_pooled_proxy"
    )
    donor_names = [name for name, _ in donors]
    return {
        "model_id": (
            f"{MODEL_FAMILY}.{INPUT_TIMEFRAME.lower()}."
            f"{instrument}.h{horizon_sec}.proxy"
        ),
        "model_family": MODEL_FAMILY,
        "input_timeframe": INPUT_TIMEFRAME,
        "training_timeframe": TRAINING_TIMEFRAME,
        "horizon_sec": int(horizon_sec),
        "source_granularity": "S5 pooled proxy",
        "sample_step_bars": None,
        "fit_provenance": provenance,
        "pair_specific_evidence": False,
        "account_eligible": False,
        "proxy_donor_instruments": donor_names,
        "proxy_donor_count": len(donor_names),
        "train_rows": 0,
        "validation_rows": 0,
        "holdout_rows": 0,
        "feature_means": [0.0] * len(FEATURE_NAMES),
        "feature_scales": [1.0] * len(FEATURE_NAMES),
        "intercept": raw_intercept,
        "coefficients": raw_weights.tolist(),
        "alpha": None,
        "magnitude_calibration": float(
            statistics.median(
                float(model.get("magnitude_calibration") or 1.0)
                for model in donor_models
            )
        ),
        "residual_std_pips": float(
            statistics.median(
                float(model.get("residual_std_pips") or 1.0)
                for model in donor_models
            )
        ),
        "profiles": profiles,
        "smoothing": {
            "method": provenance,
            "selected_lambda": None,
            "improved": False,
            "holdout_improved": False,
            "note": "Coverage-only proxy; pair-specific evidence is collected live before execution eligibility.",
        },
        "fit_metrics": {
            "selection_split": "pooled donor surfaces",
            "evaluation_split": "no target-pair holdout",
            "proxy_donor_instruments": donor_names,
        },
    }


def fit_all(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    paths = selected_paths(args.source, args.pairs)
    models: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    reports_by_instrument: dict[str, dict[str, Any]] = {}
    smoothing_reports: list[dict[str, Any]] = []
    for number, path in enumerate(paths, 1):
        instrument = path.stem.removesuffix("_S5")
        pair_started = time.monotonic()
        try:
            pair_models, dataset, fit_details = fit_pair_adaptive(
                path,
                args.horizons,
                args.sample_step,
                args.smoothing_lambdas,
            )
            smoothing = fit_details["smoothing"]
            if pair_models:
                smoothing_reports.append(
                    {
                        "instrument": instrument,
                        **smoothing,
                    }
                )
                models[instrument] = pair_models
            reports_by_instrument[instrument] = {
                "instrument": instrument,
                "source_rows": int(dataset.get("source_rows") or 0),
                "feature_rows": len(dataset.get("features", ())),
                "selected_sample_step_bars": fit_details[
                    "selected_sample_step_bars"
                ],
                "fit_attempts": fit_details["attempts"],
                "pair_specific_models": len(pair_models),
                "proxy_models": 0,
                "models": len(pair_models),
                "smoothing": smoothing,
                "elapsed_sec": round(time.monotonic() - pair_started, 3),
            }
            print(
                f"[second-fit] {number}/{len(paths)} {instrument}: "
                f"rows={len(dataset.get('features', ()))} models={len(pair_models)} "
                f"sample_step={fit_details['selected_sample_step_bars']} "
                f"smoothing_lambda={smoothing.get('selected_lambda', 0.0) if pair_models else 0.0}",
                flush=True,
            )
        except Exception as error:
            errors[instrument] = repr(error)
            reports_by_instrument[instrument] = {
                "instrument": instrument,
                "source_rows": 0,
                "feature_rows": 0,
                "selected_sample_step_bars": None,
                "fit_attempts": [],
                "pair_specific_models": 0,
                "proxy_models": 0,
                "models": 0,
                "smoothing": {},
                "elapsed_sec": round(time.monotonic() - pair_started, 3),
                "error": repr(error),
            }
            print(f"[second-fit] {instrument}: {error!r}", flush=True)

    proxy_model_count = 0
    for path in paths:
        instrument = path.stem.removesuffix("_S5")
        pair_models = models.setdefault(instrument, {})
        for horizon in args.horizons:
            if str(horizon) in pair_models:
                continue
            proxy = pooled_proxy_model(instrument, horizon, models)
            if proxy is not None:
                pair_models[str(horizon)] = proxy
                proxy_model_count += 1
        report_row = reports_by_instrument[instrument]
        report_row["proxy_models"] = sum(
            model.get("fit_provenance") != "pair_specific"
            for model in pair_models.values()
        )
        report_row["models"] = len(pair_models)
        report_row["complete_horizon_coverage"] = len(pair_models) == len(args.horizons)
    reports = [reports_by_instrument[path.stem.removesuffix("_S5")] for path in paths]
    model_surfaces: list[dict[str, Any]] = []
    horizon_summary: list[dict[str, Any]] = []
    for horizon in args.horizons:
        horizon_models = [
            model
            for pair_models in models.values()
            for key, model in pair_models.items()
            if int(key) == horizon
        ]
        profile_surfaces = [
            (profile_name, profile)
            for model in horizon_models
            for profile_name, profile in (model.get("profiles") or {}).items()
        ]
        holdout_n = sum(
            int((profile.get("holdout") or {}).get("n") or 0)
            for _, profile in profile_surfaces
        )
        weighted_net = sum(
            float((profile.get("holdout") or {}).get("avg_net_pips") or 0.0)
            * int((profile.get("holdout") or {}).get("n") or 0)
            for _, profile in profile_surfaces
        )
        horizon_summary.append(
            {
                "horizon_sec": horizon,
                "pair_model_count": len(horizon_models),
                "pair_specific_model_count": sum(
                    model.get("fit_provenance") == "pair_specific"
                    for model in horizon_models
                ),
                "proxy_model_count": sum(
                    model.get("fit_provenance") != "pair_specific"
                    for model in horizon_models
                ),
                "profile_surface_count": len(profile_surfaces),
                "holdout_n": holdout_n,
                "weighted_holdout_avg_net_pips": weighted_net / holdout_n if holdout_n else 0.0,
                "historical_gate_passes": sum(
                    bool(profile.get("historical_gate_passed"))
                    for _, profile in profile_surfaces
                ),
            }
        )
    for instrument, pair_models in models.items():
        for horizon_text, model in pair_models.items():
            model_surfaces.append(
                {
                    "model_id": model["model_id"],
                    "model_family": MODEL_FAMILY,
                    "input_timeframe": INPUT_TIMEFRAME,
                    "training_timeframe": TRAINING_TIMEFRAME,
                    "instrument": instrument,
                    "horizon_sec": int(horizon_text),
                    "fit_provenance": model.get("fit_provenance")
                    or "pair_specific",
                    "pair_specific_evidence": bool(
                        model.get("pair_specific_evidence", True)
                    ),
                    "account_eligible": bool(model.get("account_eligible", True)),
                    "validation_correlation": (model.get("fit_metrics") or {}).get(
                        "validation_correlation"
                    ),
                    "validation_direction_accuracy": (model.get("fit_metrics") or {}).get(
                        "validation_direction_accuracy"
                    ),
                    "smoothing_lambda": (model.get("smoothing") or {}).get(
                        "selected_lambda", 0.0
                    ),
                    "smoothing_validation_improvement": (
                        model.get("smoothing") or {}
                    ).get("validation_improvement", 0.0),
                    "smoothing_holdout_improvement": (
                        model.get("smoothing") or {}
                    ).get("holdout_improvement", 0.0),
                    "profiles": [
                        {
                            "profile": profile_name,
                            "holdout_n": int((profile.get("holdout") or {}).get("n") or 0),
                            "holdout_avg_net_pips": float(
                                (profile.get("holdout") or {}).get("avg_net_pips") or 0.0
                            ),
                            "holdout_win_rate": float(
                                (profile.get("holdout") or {}).get("win_rate") or 0.0
                            ),
                            "holdout_lower_95_pips": float(
                                (profile.get("holdout") or {}).get("lower_95_pips") or -999.0
                            ),
                            "historical_gate_passed": bool(
                                profile.get("historical_gate_passed")
                            ),
                        }
                        for profile_name, profile in (model.get("profiles") or {}).items()
                    ],
                }
            )
    matrix = {
        "matrix_id": "unified_forecast_matrix_v1",
        "model_family": MODEL_FAMILY,
        "input_timeframe": INPUT_TIMEFRAME,
        "training_timeframe": TRAINING_TIMEFRAME,
        "profiles": list(PROFILE_EXECUTION),
        "lane_ids": [matrix_lane_id(profile) for profile in PROFILE_EXECUTION],
        "outcome_horizons_sec": list(args.horizons),
        "account_execution_horizons_sec": [
            horizon for horizon in args.horizons if horizon in DEFAULT_EXECUTION_HORIZONS_SEC
        ],
        "physical_lane_count": len(PROFILE_EXECUTION),
        "lane_horizon_surfaces": len(PROFILE_EXECUTION) * len(args.horizons),
        "pair_model_surfaces": len(model_surfaces),
        "pair_profile_surfaces": sum(
            len(model.get("profiles") or {})
            for pair_models in models.values()
            for model in pair_models.values()
        ),
    }
    improved_smoothing = [
        row for row in smoothing_reports if bool(row.get("improved"))
    ]
    holdout_improved_smoothing = [
        row for row in smoothing_reports if bool(row.get("holdout_improved"))
    ]
    multi_horizon_smoothing = {
        "method": "adjacent_log_horizon_parameter_regularization",
        "selection_split": "middle chronological 20% executable bid/ask returns",
        "evaluation_split": "newest chronological 20% untouched holdout",
        "candidate_lambdas": [float(value) for value in args.smoothing_lambdas],
        "pair_count": len(smoothing_reports),
        "pairs_smoothed": len(improved_smoothing),
        "pairs_holdout_improved": len(holdout_improved_smoothing),
        "mean_validation_improvement": round(
            statistics.fmean(
                float(row.get("validation_improvement") or 0.0)
                for row in smoothing_reports
            ),
            6,
        )
        if smoothing_reports
        else 0.0,
        "mean_holdout_improvement": round(
            statistics.fmean(
                float(row.get("holdout_improvement") or 0.0)
                for row in smoothing_reports
            ),
            6,
        )
        if smoothing_reports
        else 0.0,
        "pair_summaries": smoothing_reports,
    }
    pair_specific_model_count = sum(
        model.get("fit_provenance") == "pair_specific"
        for pair_models in models.values()
        for model in pair_models.values()
    )
    pair_specific_pair_count = sum(
        any(
            model.get("fit_provenance") == "pair_specific"
            for model in pair_models.values()
        )
        for pair_models in models.values()
    )
    complete_horizon_pair_count = sum(
        len(pair_models) == len(args.horizons)
        for pair_models in models.values()
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "fitted_utc": datetime.now(timezone.utc).isoformat(),
        "feature_names": list(FEATURE_NAMES),
        "source_root": str(args.source),
        "source_granularity": "S5 bid/ask candles",
        "model_family": MODEL_FAMILY,
        "input_timeframe": INPUT_TIMEFRAME,
        "training_timeframe": TRAINING_TIMEFRAME,
        "live_forecast_cadence_sec": 1,
        "sample_step_bars": args.sample_step,
        "horizons_sec": list(args.horizons),
        "pairs_requested": len(paths),
        "pairs_fitted": len(models),
        "model_count": sum(len(rows) for rows in models.values()),
        "pair_specific_pair_count": pair_specific_pair_count,
        "complete_horizon_pair_count": complete_horizon_pair_count,
        "pair_specific_model_count": pair_specific_model_count,
        "proxy_model_count": proxy_model_count,
        "coverage_policy": (
            "Missing pair/horizon fits receive research-only pooled proxies; "
            "only pair-specific models are account eligible."
        ),
        "matrix": matrix,
        "multi_horizon_smoothing": multi_horizon_smoothing,
        "models": models,
        "errors": errors,
    }
    report = {
        **{key: value for key, value in payload.items() if key != "models"},
        "elapsed_sec": round(time.monotonic() - started, 3),
        "pairs": reports,
        "matrix": matrix,
        "horizon_summary": horizon_summary,
        "model_surfaces": model_surfaces,
        "historical_gate_passes": sum(
            bool(profile.get("historical_gate_passed"))
            for pair in models.values()
            for model in pair.values()
            for profile in (model.get("profiles") or {}).values()
        ),
    }
    incumbent = _json_object(args.output)
    artifact_update = model_artifact_replacement_decision(
        payload,
        incumbent,
        allow_coverage_regression=args.allow_coverage_regression,
    )
    report["artifact_update"] = artifact_update
    if artifact_update["replaced"]:
        atomic_json(args.output, payload)
    report = reconcile_fit_report(
        report,
        _json_object(args.report),
        artifact_update,
    )
    atomic_json(args.report, report)
    return report


def parse_horizons(value: str) -> list[int]:
    values = sorted({int(item) for item in value.replace(",", " ").split()})
    if not values or any(value <= 0 or value % 5 for value in values):
        raise argparse.ArgumentTypeError("horizons must be positive multiples of five seconds")
    return values


def parse_smoothing_lambdas(value: str) -> list[float]:
    values = sorted(
        {
            float(item)
            for item in value.replace(",", " ").split()
        }
        | {0.0}
    )
    if any(not math.isfinite(item) or item < 0.0 for item in values):
        raise argparse.ArgumentTypeError(
            "smoothing lambdas must be finite nonnegative numbers"
        )
    return values


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--pairs", default="liquid")
    parser.add_argument("--horizons", type=parse_horizons, default=list(DEFAULT_HORIZONS_SEC))
    parser.add_argument(
        "--smoothing-lambdas",
        type=parse_smoothing_lambdas,
        default=list(DEFAULT_SMOOTHING_LAMBDAS),
    )
    parser.add_argument("--sample-step", type=int, default=2)
    parser.add_argument("--interval-sec", type=int, default=0)
    parser.add_argument(
        "--allow-coverage-regression",
        action="store_true",
        help="allow an intentional narrower model artifact to replace the incumbent",
    )
    args = parser.parse_args(argv)
    if args.sample_step <= 0 or args.interval_sec < 0:
        raise SystemExit("sample step must be positive and interval must be nonnegative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    while True:
        report = fit_all(args)
        console_report = {
            key: value
            for key, value in report.items()
            if key not in {"model_surfaces", "pairs"}
        }
        print(json.dumps(console_report, indent=2, sort_keys=True), flush=True)
        if not args.interval_sec:
            return 0
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
