"""Research-only benchmark for every causal unified forecast feature.

The source matrix contains model inputs, not independently deployable trade
signals. This module fits a small historical calibration for every feature,
measures direction and movement value, removes near-duplicates using training
data only, and evaluates validation-selected combinations. It never changes
promotion or execution state.
"""

from __future__ import annotations

import gc
import hashlib
import json
import math
import os
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor
from sklearn.linear_model import Ridge

from .common import utc_now_stamp


PIPELINE_VERSION = "unified_signal_benchmark_v1"
DEFAULT_DATA_ROOT = Path(
    r"C:\Users\zmoor\AppData\Local\ForexResearchData\unified_intrahour_v1"
)
BIN_EDGES = np.asarray(
    [-2.0, -1.5, -1.0, -0.5, -0.2, 0.2, 0.5, 1.0, 1.5, 2.0],
    dtype=np.float32,
)
EPSILON = 1e-9


@dataclass
class FeatureCalibration:
    feature: str
    signed_variant: str
    linear_beta: float
    bin_values: list[float]
    magnitude_beta_linear: float
    magnitude_beta_quadratic: float


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
        return value if math.isfinite(value) else None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_json_value(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_correlation(
    left: np.ndarray, right: np.ndarray, *, rank: bool = False
) -> float:
    valid = np.isfinite(left) & np.isfinite(right)
    left = np.asarray(left[valid], dtype=float)
    right = np.asarray(right[valid], dtype=float)
    if (
        len(left) < 20
        or float(np.ptp(left)) <= EPSILON
        or float(np.ptp(right)) <= EPSILON
    ):
        return 0.0
    if rank:
        value = float(spearmanr(left, right).statistic)
    else:
        value = float(np.corrcoef(left, right)[0, 1])
    return value if math.isfinite(value) else 0.0


def chronological_indices(
    decision_time: pd.Series,
    validation_fraction: float,
    final_fraction: float,
    purge_minutes: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    timestamps = pd.to_datetime(decision_time, errors="coerce", utc=True)
    unique = np.sort(timestamps.dropna().unique())
    if len(unique) < 100:
        raise ValueError("at least 100 unique timestamps are required")
    validation_position = int(
        len(unique) * (1.0 - validation_fraction - final_fraction)
    )
    final_position = int(len(unique) * (1.0 - final_fraction))
    validation_start = pd.Timestamp(unique[validation_position])
    final_start = pd.Timestamp(unique[final_position])
    purge = pd.to_timedelta(int(purge_minutes), unit="min")
    train = np.flatnonzero((timestamps < validation_start - purge).to_numpy())
    validation = np.flatnonzero(
        (
            (timestamps >= validation_start)
            & (timestamps < final_start - purge)
        ).to_numpy()
    )
    final = np.flatnonzero((timestamps >= final_start).to_numpy())
    train_latest = timestamps.iloc[train].max() + purge
    validation_latest = timestamps.iloc[validation].max() + purge
    audit = {
        "validation_start_utc": validation_start.isoformat(),
        "final_start_utc": final_start.isoformat(),
        "purge_minutes": purge_minutes,
        "train_rows": len(train),
        "validation_rows": len(validation),
        "final_rows": len(final),
        "train_target_latest_utc": train_latest.isoformat(),
        "validation_target_latest_utc": validation_latest.isoformat(),
        "train_purge_passed": bool(train_latest < validation_start),
        "validation_purge_passed": bool(validation_latest < final_start),
    }
    if not audit["train_purge_passed"] or not audit["validation_purge_passed"]:
        raise AssertionError("purged chronological split invariant failed")
    return train, validation, final, audit


def nested_validation_indices(
    decision_time: pd.Series,
    validation_indices: np.ndarray,
    purge_minutes: int,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    values = pd.to_datetime(
        decision_time.iloc[validation_indices], errors="coerce", utc=True
    )
    unique = np.sort(values.dropna().unique())
    boundary = pd.Timestamp(unique[len(unique) // 2])
    purge = pd.to_timedelta(int(purge_minutes), unit="min")
    screen_mask = values < boundary - purge
    selection_mask = values >= boundary
    screen = validation_indices[np.flatnonzero(screen_mask.to_numpy())]
    selection = validation_indices[np.flatnonzero(selection_mask.to_numpy())]
    latest = pd.to_datetime(
        decision_time.iloc[screen], errors="coerce", utc=True
    ).max() + purge
    audit = {
        "selection_start_utc": boundary.isoformat(),
        "screen_rows": len(screen),
        "selection_rows": len(selection),
        "screen_target_latest_utc": latest.isoformat(),
        "purge_minutes": purge_minutes,
        "purge_passed": bool(latest < boundary),
    }
    if not audit["purge_passed"]:
        raise AssertionError("nested validation purge invariant failed")
    return screen, selection, audit


def _standardize_by_instrument(
    values: np.ndarray,
    pair_codes: np.ndarray,
    train_indices: np.ndarray,
    pair_count: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    feature_count = values.shape[1]
    global_train = values[train_indices]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        global_mean = np.nanmean(global_train, axis=0)
        global_std = np.nanstd(global_train, axis=0)
    global_mean = np.where(np.isfinite(global_mean), global_mean, 0.0)
    global_std = np.where(
        np.isfinite(global_std) & (global_std > 1e-7), global_std, 1.0
    )
    means = np.tile(global_mean, (pair_count, 1)).astype(np.float32)
    scales = np.tile(global_std, (pair_count, 1)).astype(np.float32)
    for pair in range(pair_count):
        positions = train_indices[pair_codes[train_indices] == pair]
        if len(positions) < 20:
            continue
        subset = values[positions]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            mean = np.nanmean(subset, axis=0)
            scale = np.nanstd(subset, axis=0)
        means[pair] = np.where(np.isfinite(mean), mean, global_mean)
        scales[pair] = np.where(
            np.isfinite(scale) & (scale > 1e-7), scale, global_std
        )
    for pair in range(pair_count):
        positions = np.flatnonzero(pair_codes == pair)
        block = (values[positions] - means[pair]) / scales[pair]
        values[positions] = np.clip(
            np.nan_to_num(block, nan=0.0, posinf=8.0, neginf=-8.0),
            -8.0,
            8.0,
        )
    constant = np.nanstd(values[train_indices], axis=0) <= 1e-7
    return values, means, scales, constant


def _pair_baseline(
    target: np.ndarray,
    pair_codes: np.ndarray,
    train_indices: np.ndarray,
    pair_count: int,
) -> np.ndarray:
    global_mean = float(np.nanmean(target[train_indices]))
    result = np.full(pair_count, global_mean, dtype=np.float64)
    for pair in range(pair_count):
        positions = train_indices[pair_codes[train_indices] == pair]
        if len(positions) >= 20:
            value = float(np.nanmean(target[positions]))
            if math.isfinite(value):
                result[pair] = value
    return result


def _fit_calibration(
    feature: str,
    x: np.ndarray,
    y_signed: np.ndarray,
    y_abs: np.ndarray,
    abs_baseline: np.ndarray,
) -> FeatureCalibration:
    denominator = float(np.dot(x, x))
    beta = float(np.dot(x, y_signed) / max(denominator, EPSILON))
    bins = np.digitize(x, BIN_EDGES)
    bin_values = np.zeros(len(BIN_EDGES) + 1, dtype=float)
    for number in range(len(bin_values)):
        selected = bins == number
        count = int(selected.sum())
        if count:
            raw = float(np.mean(y_signed[selected]))
            bin_values[number] = raw * count / (count + 500.0)
    quadratic = np.square(x, dtype=np.float64) - 1.0
    design = np.column_stack([x, quadratic])
    residual = y_abs - abs_baseline
    coefficients, *_ = np.linalg.lstsq(design, residual, rcond=None)
    return FeatureCalibration(
        feature=feature,
        signed_variant="linear",
        linear_beta=beta,
        bin_values=bin_values.tolist(),
        magnitude_beta_linear=float(coefficients[0]),
        magnitude_beta_quadratic=float(coefficients[1]),
    )


def _signed_prediction(
    calibration: FeatureCalibration, x: np.ndarray, variant: str | None = None
) -> np.ndarray:
    selected = variant or calibration.signed_variant
    if selected == "binned":
        return np.asarray(calibration.bin_values, dtype=float)[
            np.digitize(x, BIN_EDGES)
        ]
    return calibration.linear_beta * x


def _magnitude_prediction(
    calibration: FeatureCalibration,
    x: np.ndarray,
    baseline: np.ndarray,
) -> np.ndarray:
    prediction = (
        baseline
        + calibration.magnitude_beta_linear * x
        + calibration.magnitude_beta_quadratic * (np.square(x) - 1.0)
    )
    return np.maximum(0.0, prediction)


def _forecast_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, Any]:
    valid = np.isfinite(actual) & np.isfinite(predicted)
    actual = actual[valid]
    predicted = predicted[valid]
    active = np.abs(predicted) > EPSILON
    directional = active & (np.abs(actual) > EPSILON)
    baseline_mae = float(np.mean(np.abs(actual))) if len(actual) else 0.0
    mae = float(np.mean(np.abs(actual - predicted))) if len(actual) else 0.0
    return {
        "rows": len(actual),
        "active_rows": int(active.sum()),
        "coverage": float(np.mean(active)) if len(actual) else 0.0,
        "mae_pips": mae,
        "mae_relative_to_no_change": mae / max(EPSILON, baseline_mae),
        "rmse_pips": (
            float(np.sqrt(np.mean(np.square(actual - predicted))))
            if len(actual)
            else 0.0
        ),
        "direction_accuracy": (
            float(
                np.mean(
                    np.sign(actual[directional])
                    == np.sign(predicted[directional])
                )
            )
            if directional.any()
            else 0.0
        ),
        "pearson_correlation": _safe_correlation(actual, predicted),
        "rank_ic": _safe_correlation(actual, predicted, rank=True),
        "actual_mean_abs_move_pips": (
            float(np.mean(np.abs(actual))) if len(actual) else 0.0
        ),
        "predicted_mean_abs_move_pips": (
            float(np.mean(np.abs(predicted))) if len(predicted) else 0.0
        ),
    }


def _movement_metrics(
    actual_abs: np.ndarray,
    path_range: np.ndarray,
    predicted: np.ndarray,
    baseline: np.ndarray,
) -> dict[str, Any]:
    valid = (
        np.isfinite(actual_abs)
        & np.isfinite(path_range)
        & np.isfinite(predicted)
        & np.isfinite(baseline)
    )
    actual_abs = actual_abs[valid]
    path_range = path_range[valid]
    predicted = predicted[valid]
    baseline = baseline[valid]
    mae = float(np.mean(np.abs(actual_abs - predicted)))
    baseline_mae = float(np.mean(np.abs(actual_abs - baseline)))
    incremental_actual = actual_abs - baseline
    incremental_prediction = predicted - baseline
    threshold = float(np.quantile(predicted, 0.90))
    top = predicted >= threshold
    mean_move = float(np.mean(actual_abs))
    return {
        "rows": len(actual_abs),
        "mae_pips": mae,
        "mae_relative_to_pair_history": mae / max(EPSILON, baseline_mae),
        "rank_ic": _safe_correlation(actual_abs, predicted, rank=True),
        "incremental_rank_ic": _safe_correlation(
            incremental_actual, incremental_prediction, rank=True
        ),
        "path_range_rank_ic": _safe_correlation(path_range, predicted, rank=True),
        "top_decile_actual_abs_move_pips": (
            float(np.mean(actual_abs[top])) if top.any() else 0.0
        ),
        "top_decile_move_lift": (
            float(np.mean(actual_abs[top])) / max(EPSILON, mean_move)
            if top.any()
            else 0.0
        ),
    }


def _net_for_prediction(
    prediction: np.ndarray, long_net: np.ndarray, short_net: np.ndarray
) -> np.ndarray:
    return np.where(prediction > 0.0, long_net, short_net)


def rotation_metrics(
    decision_time: np.ndarray,
    prediction: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    *,
    threshold: float = 0.0,
    ranking_score: np.ndarray | None = None,
    quotas: tuple[int, ...] = (1, 3, 5),
) -> dict[str, Any]:
    predicted = np.asarray(prediction, dtype=float)
    score = (
        np.abs(predicted)
        if ranking_score is None
        else np.asarray(ranking_score, dtype=float)
    )
    net = _net_for_prediction(predicted, long_net, short_net)
    order = np.argsort(decision_time, kind="stable")
    ordered_times = decision_time[order]
    boundaries = np.r_[
        0,
        np.flatnonzero(ordered_times[1:] != ordered_times[:-1]) + 1,
        len(order),
    ]
    output: dict[str, Any] = {}
    total_timestamps = max(0, len(boundaries) - 1)
    for quota in quotas:
        selected_net: list[float] = []
        traded_timestamps = 0
        for left, right in zip(boundaries[:-1], boundaries[1:]):
            positions = order[left:right]
            eligible = positions[
                np.isfinite(score[positions])
                & np.isfinite(predicted[positions])
                & np.isfinite(net[positions])
                & (np.abs(predicted[positions]) > EPSILON)
                & (score[positions] >= threshold)
            ]
            if not len(eligible):
                continue
            take = min(quota, len(eligible))
            ranked = eligible[np.argsort(score[eligible], kind="stable")[-take:]]
            selected_net.extend(net[ranked].tolist())
            traded_timestamps += 1
        values = np.asarray(selected_net, dtype=float)
        gains = float(values[values > 0.0].sum())
        losses = float(-values[values < 0.0].sum())
        curve = np.cumsum(values)
        drawdown = (
            float(
                np.max(
                    np.maximum.accumulate(np.r_[0.0, curve])[1:] - curve
                )
            )
            if len(values)
            else 0.0
        )
        output[f"top{quota}"] = {
            "quota": quota,
            "trades": len(values),
            "traded_timestamps": traded_timestamps,
            "no_trade_timestamps": total_timestamps - traded_timestamps,
            "mean_net_pips": float(np.mean(values)) if len(values) else 0.0,
            "total_net_pips": float(np.sum(values)) if len(values) else 0.0,
            "win_rate": float(np.mean(values > 0.0)) if len(values) else 0.0,
            "profit_factor": (
                gains / losses
                if losses > 0.0
                else (float("inf") if gains > 0.0 else 0.0)
            ),
            "max_drawdown_pips": drawdown,
        }
    return output


def _choose_rotation_threshold(
    decision_time: np.ndarray,
    prediction: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    ranking_score: np.ndarray | None = None,
) -> tuple[float, dict[str, Any]]:
    base_score = (
        np.abs(prediction)
        if ranking_score is None
        else np.asarray(ranking_score, dtype=float)
    )
    finite = base_score[np.isfinite(base_score)]
    if not len(finite):
        return float("inf"), rotation_metrics(
            decision_time,
            prediction,
            long_net,
            short_net,
            threshold=float("inf"),
            ranking_score=ranking_score,
        )
    thresholds = sorted(
        {
            float(np.quantile(finite, quantile))
            for quantile in (0.0, 0.5, 0.7, 0.8, 0.9, 0.95)
        }
    )
    attempts = [
        (
            threshold,
            rotation_metrics(
                decision_time,
                prediction,
                long_net,
                short_net,
                threshold=threshold,
                ranking_score=ranking_score,
            ),
        )
        for threshold in thresholds
    ]
    eligible = [
        row for row in attempts if row[1]["top3"]["trades"] >= 100
    ] or attempts
    return max(
        eligible,
        key=lambda row: (
            row[1]["top3"]["mean_net_pips"],
            row[1]["top3"]["profit_factor"],
            row[1]["top3"]["trades"],
        ),
    )


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def correlation_groups(
    values: np.ndarray, threshold: float = 0.995
) -> tuple[list[list[int]], np.ndarray]:
    correlation = np.corrcoef(values, rowvar=False)
    correlation = np.nan_to_num(correlation, nan=0.0, posinf=0.0, neginf=0.0)
    union = _UnionFind(values.shape[1])
    left, right = np.where(
        np.triu(np.abs(correlation) >= threshold, k=1)
    )
    for first, second in zip(left.tolist(), right.tolist()):
        union.union(first, second)
    grouped: dict[int, list[int]] = {}
    for number in range(values.shape[1]):
        grouped.setdefault(union.find(number), []).append(number)
    return list(grouped.values()), correlation


def _screen_score(metrics: dict[str, Any]) -> float:
    return float(
        metrics["rank_ic"]
        + 0.15 * (metrics["direction_accuracy"] - 0.5)
        - 0.02 * max(0.0, metrics["mae_relative_to_no_change"] - 1.0)
    )


def _flatten(prefix: str, metrics: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}_{key}": value for key, value in metrics.items()}


def _predict_calibrations(
    calibrations: list[FeatureCalibration],
    feature_positions: dict[str, int],
    values: np.ndarray,
    indices: np.ndarray,
) -> np.ndarray:
    output = np.empty((len(indices), len(calibrations)), dtype=np.float32)
    for column, calibration in enumerate(calibrations):
        feature_column = feature_positions[calibration.feature]
        output[:, column] = _signed_prediction(
            calibration, values[indices, feature_column]
        )
    return output


def _fit_combination_candidates(
    values: np.ndarray,
    y_signed: np.ndarray,
    train_indices: np.ndarray,
    selection_indices: np.ndarray,
    ranked_features: list[str],
    feature_positions: dict[str, int],
    calibrations_by_feature: dict[str, FeatureCalibration],
    decision_time: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    random_state: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    fitted: dict[str, dict[str, Any]] = {}
    no_change = np.zeros(len(selection_indices), dtype=float)
    candidates.append(
        {
            "candidate": "no_change",
            "kind": "baseline",
            "feature_count": 0,
            "validation_selection": _forecast_metrics(
                y_signed[selection_indices], no_change
            ),
            "rotation_threshold": float("inf"),
            "validation_selection_rotation": rotation_metrics(
                decision_time[selection_indices],
                no_change,
                long_net[selection_indices],
                short_net[selection_indices],
            ),
        }
    )

    for count in (1, 3, 5, 10, 20, 40):
        selected = ranked_features[: min(count, len(ranked_features))]
        if not selected:
            continue
        selected_calibrations = [
            calibrations_by_feature[name] for name in selected
        ]
        prediction_matrix = _predict_calibrations(
            selected_calibrations,
            feature_positions,
            values,
            selection_indices,
        )
        predicted = np.mean(prediction_matrix, axis=1)
        name = f"equal_signal_ensemble_k{len(selected)}"
        threshold, rotation = _choose_rotation_threshold(
            decision_time[selection_indices],
            predicted,
            long_net[selection_indices],
            short_net[selection_indices],
        )
        candidates.append(
            {
                "candidate": name,
                "kind": "calibrated_signal_ensemble",
                "feature_count": len(selected),
                "features": selected,
                "validation_selection": _forecast_metrics(
                    y_signed[selection_indices], predicted
                ),
                "rotation_threshold": threshold,
                "validation_selection_rotation": rotation,
            }
        )
        fitted[name] = {
            "kind": "calibrated_signal_ensemble",
            "features": selected,
            "threshold": threshold,
        }

    for count in (10, 25, 50, 100, 200):
        selected = ranked_features[: min(count, len(ranked_features))]
        if len(selected) < 2:
            continue
        columns = [feature_positions[name] for name in selected]
        train_x = values[np.ix_(train_indices, columns)]
        selection_x = values[np.ix_(selection_indices, columns)]
        for alpha in (10.0, 100.0, 1000.0):
            model = Ridge(
                alpha=alpha,
                fit_intercept=False,
                solver="lsqr",
                max_iter=300,
                tol=1e-4,
            )
            model.fit(train_x, y_signed[train_indices])
            predicted = np.asarray(model.predict(selection_x), dtype=float)
            name = f"ridge_k{len(selected)}_a{alpha:g}"
            threshold, rotation = _choose_rotation_threshold(
                decision_time[selection_indices],
                predicted,
                long_net[selection_indices],
                short_net[selection_indices],
            )
            candidates.append(
                {
                    "candidate": name,
                    "kind": "ridge",
                    "feature_count": len(selected),
                    "alpha": alpha,
                    "features": selected,
                    "validation_selection": _forecast_metrics(
                        y_signed[selection_indices], predicted
                    ),
                    "rotation_threshold": threshold,
                    "validation_selection_rotation": rotation,
                }
            )
            fitted[name] = {
                "kind": "sklearn",
                "features": selected,
                "model": model,
                "threshold": threshold,
            }

    nonlinear_features = ranked_features[: min(50, len(ranked_features))]
    if len(nonlinear_features) >= 5:
        columns = [feature_positions[name] for name in nonlinear_features]
        capped_train = train_indices[
            np.linspace(
                0,
                len(train_indices) - 1,
                min(80_000, len(train_indices)),
                dtype=int,
            )
        ]
        train_x = values[np.ix_(capped_train, columns)]
        selection_x = values[np.ix_(selection_indices, columns)]
        models = {
            "hist_gradient_boosting_k50": HistGradientBoostingRegressor(
                learning_rate=0.05,
                max_iter=80,
                max_leaf_nodes=31,
                min_samples_leaf=80,
                l2_regularization=5.0,
                random_state=random_state,
            ),
            "extra_trees_k50": ExtraTreesRegressor(
                n_estimators=60,
                max_depth=13,
                min_samples_leaf=50,
                max_features=0.7,
                n_jobs=2,
                random_state=random_state,
            ),
        }
        for name, model in models.items():
            model.fit(train_x, y_signed[capped_train])
            predicted = np.asarray(model.predict(selection_x), dtype=float)
            threshold, rotation = _choose_rotation_threshold(
                decision_time[selection_indices],
                predicted,
                long_net[selection_indices],
                short_net[selection_indices],
            )
            candidates.append(
                {
                    "candidate": name,
                    "kind": name.rsplit("_k", 1)[0],
                    "feature_count": len(nonlinear_features),
                    "features": nonlinear_features,
                    "validation_selection": _forecast_metrics(
                        y_signed[selection_indices], predicted
                    ),
                    "rotation_threshold": threshold,
                    "validation_selection_rotation": rotation,
                }
            )
            fitted[name] = {
                "kind": "sklearn",
                "features": nonlinear_features,
                "model": model,
                "threshold": threshold,
            }
    return candidates, fitted


def _predict_fitted(
    specification: dict[str, Any],
    values: np.ndarray,
    indices: np.ndarray,
    feature_positions: dict[str, int],
    calibrations_by_feature: dict[str, FeatureCalibration],
) -> np.ndarray:
    if specification["kind"] == "calibrated_signal_ensemble":
        calibrations = [
            calibrations_by_feature[name] for name in specification["features"]
        ]
        return np.mean(
            _predict_calibrations(
                calibrations, feature_positions, values, indices
            ),
            axis=1,
        )
    columns = [
        feature_positions[name] for name in specification["features"]
    ]
    return np.asarray(
        specification["model"].predict(values[np.ix_(indices, columns)]),
        dtype=float,
    )


def _fit_movement_candidates(
    values: np.ndarray,
    y_abs: np.ndarray,
    abs_baseline_by_pair: np.ndarray,
    pair_codes: np.ndarray,
    train_indices: np.ndarray,
    selection_indices: np.ndarray,
    ranked_features: list[str],
    feature_positions: dict[str, int],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    baseline_train = abs_baseline_by_pair[pair_codes[train_indices]]
    baseline_selection = abs_baseline_by_pair[pair_codes[selection_indices]]
    residual = y_abs[train_indices] - baseline_train
    candidates: list[dict[str, Any]] = [
        {
            "candidate": "pair_historical_mean",
            "feature_count": 0,
            "validation_selection": _movement_metrics(
                y_abs[selection_indices],
                y_abs[selection_indices],
                baseline_selection,
                baseline_selection,
            ),
        }
    ]
    fitted: dict[str, Any] = {}
    for count in (10, 25, 50, 100):
        selected = ranked_features[: min(count, len(ranked_features))]
        if len(selected) < 2:
            continue
        columns = [feature_positions[name] for name in selected]
        for alpha in (10.0, 100.0, 1000.0):
            model = Ridge(
                alpha=alpha,
                fit_intercept=False,
                solver="lsqr",
                max_iter=300,
                tol=1e-4,
            )
            model.fit(values[np.ix_(train_indices, columns)], residual)
            predicted = np.maximum(
                0.0,
                baseline_selection
                + model.predict(values[np.ix_(selection_indices, columns)]),
            )
            name = f"movement_ridge_k{len(selected)}_a{alpha:g}"
            mae = float(np.mean(np.abs(y_abs[selection_indices] - predicted)))
            baseline_mae = float(
                np.mean(
                    np.abs(y_abs[selection_indices] - baseline_selection)
                )
            )
            candidates.append(
                {
                    "candidate": name,
                    "feature_count": len(selected),
                    "alpha": alpha,
                    "features": selected,
                    "validation_selection": {
                        "mae_pips": mae,
                        "mae_relative_to_pair_history": mae
                        / max(EPSILON, baseline_mae),
                        "rank_ic": _safe_correlation(
                            y_abs[selection_indices], predicted, rank=True
                        ),
                        "incremental_rank_ic": _safe_correlation(
                            y_abs[selection_indices] - baseline_selection,
                            predicted - baseline_selection,
                            rank=True,
                        ),
                    },
                }
            )
            fitted[name] = {
                "features": selected,
                "model": model,
            }
    winner = min(
        candidates,
        key=lambda row: row["validation_selection"][
            "mae_relative_to_pair_history"
        ],
    )
    return candidates, {
        "winner": winner,
        "fitted": fitted.get(str(winner["candidate"])),
    }


def _group_forecast_breakdown(
    labels: np.ndarray,
    actual: np.ndarray,
    predicted: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    label_name: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for label in sorted(set(labels.tolist())):
        selected = labels == label
        metrics = _forecast_metrics(actual[selected], predicted[selected])
        active = np.abs(predicted[selected]) > EPSILON
        chosen_net = _net_for_prediction(
            predicted[selected], long_net[selected], short_net[selected]
        )
        chosen_net = chosen_net[active & np.isfinite(chosen_net)]
        output.append(
            {
                label_name: str(label),
                **metrics,
                "forced_direction_mean_net_pips": (
                    float(np.mean(chosen_net)) if len(chosen_net) else 0.0
                ),
                "forced_direction_win_rate": (
                    float(np.mean(chosen_net > 0.0)) if len(chosen_net) else 0.0
                ),
            }
        )
    return sorted(output, key=lambda row: row["rank_ic"], reverse=True)


def _markdown_table(
    rows: list[dict[str, Any]], columns: list[tuple[str, str]], limit: int = 20
) -> list[str]:
    header = "| " + " | ".join(label for _, label in columns) + " |"
    divider = "|" + "|".join("---" for _ in columns) + "|"
    lines = [header, divider]
    for row in rows[:limit]:
        values: list[str] = []
        for key, _ in columns:
            value = row.get(key, "")
            if isinstance(value, float):
                values.append(f"{value:.4f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return lines


def run_unified_signal_benchmark(
    cfg: dict[str, Any],
    run_dir: Path,
    *,
    matrix_path: Path | None = None,
    feature_registry_path: Path | None = None,
    horizon: int = 60,
    correlation_threshold: float = 0.995,
    random_state: int = 42,
) -> Path:
    os.environ.setdefault("OMP_NUM_THREADS", "2")
    os.environ.setdefault("MKL_NUM_THREADS", "2")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "2")
    run_dir.mkdir(parents=True, exist_ok=True)
    matrix_path = matrix_path or DEFAULT_DATA_ROOT / "unified_training_matrix.parquet"
    feature_registry_path = (
        feature_registry_path
        or DEFAULT_DATA_ROOT / "unified_feature_registry.csv"
    )
    if not matrix_path.is_file() or not feature_registry_path.is_file():
        raise FileNotFoundError("unified matrix or feature registry is missing")

    registry = pd.read_csv(feature_registry_path)
    eligible = registry[
        registry["causal"].astype(str).str.lower().eq("true")
        & registry["completed_bars_only"].astype(str).str.lower().eq("true")
        & registry["historically_materialized"].astype(str).str.lower().eq("true")
        & registry["model_input"].astype(str).str.lower().eq("true")
    ].copy()
    feature_names = eligible["feature_name"].astype(str).tolist()
    targets = {
        "signed": f"target_return_pips_{horizon}",
        "abs": f"target_abs_move_pips_{horizon}",
        "path": f"target_path_range_pips_{horizon}",
        "long": f"diag_long_net_pips_{horizon}",
        "short": f"diag_short_net_pips_{horizon}",
    }
    identity = [
        "decision_time_utc",
        "instrument",
        "base_currency",
        "quote_currency",
    ]
    columns = [*identity, *feature_names, *targets.values()]
    print(
        f"[signal-benchmark] loading rows with {len(feature_names)} causal features",
        flush=True,
    )
    frame = pd.read_parquet(matrix_path, columns=columns)
    frame["decision_time_utc"] = pd.to_datetime(
        frame["decision_time_utc"], errors="coerce", utc=True
    )
    frame = (
        frame.dropna(
            subset=[
                "decision_time_utc",
                "instrument",
                *targets.values(),
            ]
        )
        .sort_values(["decision_time_utc", "instrument"])
        .reset_index(drop=True)
    )
    settings = cfg.get("unified_intrahour_forecast", {})
    train_indices, validation_indices, final_indices, split = (
        chronological_indices(
            frame["decision_time_utc"],
            float(settings.get("validation_fraction", 0.2)),
            float(settings.get("final_fraction", 0.2)),
            int(settings.get("purge_minutes", horizon)),
        )
    )
    screen_indices, selection_indices, nested_split = (
        nested_validation_indices(
            frame["decision_time_utc"],
            validation_indices,
            int(settings.get("purge_minutes", horizon)),
        )
    )
    instruments = pd.Categorical(frame["instrument"])
    pair_codes = instruments.codes.astype(np.int16)
    pair_count = len(instruments.categories)
    decision_time = frame["decision_time_utc"].to_numpy()
    y_signed = frame[targets["signed"]].to_numpy(dtype=np.float64)
    y_abs = frame[targets["abs"]].to_numpy(dtype=np.float64)
    y_path = frame[targets["path"]].to_numpy(dtype=np.float64)
    long_net = frame[targets["long"]].to_numpy(dtype=np.float64)
    short_net = frame[targets["short"]].to_numpy(dtype=np.float64)
    values = frame[feature_names].to_numpy(dtype=np.float32, copy=True)
    metadata = frame[identity].copy()
    del frame
    gc.collect()

    print("[signal-benchmark] applying training-only pair normalization", flush=True)
    values, _, _, constant = _standardize_by_instrument(
        values, pair_codes, train_indices, pair_count
    )
    abs_baseline_by_pair = _pair_baseline(
        y_abs, pair_codes, train_indices, pair_count
    )
    calibrations: list[FeatureCalibration] = []
    screen_rows: list[dict[str, Any]] = []
    for number, feature in enumerate(feature_names):
        x_train = values[train_indices, number].astype(float)
        calibration = _fit_calibration(
            feature,
            x_train,
            y_signed[train_indices],
            y_abs[train_indices],
            abs_baseline_by_pair[pair_codes[train_indices]],
        )
        x_screen = values[screen_indices, number].astype(float)
        linear_metrics = _forecast_metrics(
            y_signed[screen_indices],
            _signed_prediction(calibration, x_screen, "linear"),
        )
        binned_metrics = _forecast_metrics(
            y_signed[screen_indices],
            _signed_prediction(calibration, x_screen, "binned"),
        )
        calibration.signed_variant = (
            "binned"
            if _screen_score(binned_metrics) > _screen_score(linear_metrics)
            else "linear"
        )
        signed_metrics = (
            binned_metrics
            if calibration.signed_variant == "binned"
            else linear_metrics
        )
        magnitude = _magnitude_prediction(
            calibration,
            x_screen,
            abs_baseline_by_pair[pair_codes[screen_indices]],
        )
        movement_metrics = _movement_metrics(
            y_abs[screen_indices],
            y_path[screen_indices],
            magnitude,
            abs_baseline_by_pair[pair_codes[screen_indices]],
        )
        calibrations.append(calibration)
        screen_rows.append(
            {
                "feature": feature,
                "constant_on_train": bool(constant[number]),
                "signed_variant": calibration.signed_variant,
                "screen_score": _screen_score(signed_metrics),
                **_flatten("screen", signed_metrics),
                **_flatten("screen_movement", movement_metrics),
            }
        )
        if (number + 1) % 100 == 0 or number + 1 == len(feature_names):
            print(
                f"[signal-benchmark] calibrated {number + 1}/{len(feature_names)}",
                flush=True,
            )

    screen_by_feature = {row["feature"]: row for row in screen_rows}
    sample_positions = train_indices[
        np.linspace(
            0,
            len(train_indices) - 1,
            min(20_000, len(train_indices)),
            dtype=int,
        )
    ]
    groups, correlation = correlation_groups(
        values[sample_positions], threshold=correlation_threshold
    )
    duplicate_map: list[dict[str, Any]] = []
    representatives: list[str] = []
    for group_number, group in enumerate(groups, start=1):
        names = [feature_names[position] for position in group]
        representative = max(
            names,
            key=lambda name: (
                screen_by_feature[name]["screen_score"],
                screen_by_feature[name][
                    "screen_movement_incremental_rank_ic"
                ],
            ),
        )
        representatives.append(representative)
        representative_position = feature_names.index(representative)
        for position, name in zip(group, names):
            relationship = abs(correlation[position, representative_position])
            duplicate_map.append(
                {
                    "feature": name,
                    "group_id": group_number,
                    "group_size": len(group),
                    "representative": representative,
                    "is_representative": name == representative,
                    "absolute_training_correlation_to_representative": float(
                        relationship
                    ),
                    "duplicate_class": (
                        "exact_or_scale_sign_duplicate"
                        if name != representative and relationship >= 0.999999
                        else (
                            "near_duplicate"
                            if name != representative
                            else "representative"
                        )
                    ),
                    "deduplication_scope": "training_only",
                }
            )
    del correlation
    gc.collect()

    signed_ranked = sorted(
        [
            name
            for name in representatives
            if not screen_by_feature[name]["constant_on_train"]
        ],
        key=lambda name: screen_by_feature[name]["screen_score"],
        reverse=True,
    )
    movement_ranked = sorted(
        [
            name
            for name in representatives
            if not screen_by_feature[name]["constant_on_train"]
        ],
        key=lambda name: (
            screen_by_feature[name]["screen_movement_incremental_rank_ic"],
            -screen_by_feature[name][
                "screen_movement_mae_relative_to_pair_history"
            ],
        ),
        reverse=True,
    )
    feature_positions = {
        name: position for position, name in enumerate(feature_names)
    }
    calibrations_by_feature = {
        calibration.feature: calibration for calibration in calibrations
    }
    print(
        f"[signal-benchmark] {len(representatives)} representatives after "
        f"{correlation_threshold:.3f} correlation reduction",
        flush=True,
    )
    candidates, fitted = _fit_combination_candidates(
        values,
        y_signed,
        train_indices,
        selection_indices,
        signed_ranked,
        feature_positions,
        calibrations_by_feature,
        decision_time,
        long_net,
        short_net,
        random_state,
    )
    forecast_winner = min(
        candidates,
        key=lambda row: (
            row["validation_selection"]["mae_relative_to_no_change"],
            -row["validation_selection"]["rank_ic"],
        ),
    )
    rotation_pool = [
        row
        for row in candidates
        if row["candidate"] != "no_change"
        and row["validation_selection_rotation"]["top3"]["trades"] >= 100
    ]
    rotation_winner = max(
        rotation_pool,
        key=lambda row: (
            row["validation_selection_rotation"]["top3"]["mean_net_pips"],
            row["validation_selection_rotation"]["top3"]["profit_factor"],
        ),
    )
    movement_candidates, movement_selection = _fit_movement_candidates(
        values,
        y_abs,
        abs_baseline_by_pair,
        pair_codes,
        train_indices,
        selection_indices,
        movement_ranked,
        feature_positions,
    )
    movement_winner = movement_selection["winner"]
    movement_fitted = movement_selection["fitted"]
    selection_abs_baseline = abs_baseline_by_pair[
        pair_codes[selection_indices]
    ]
    if movement_fitted is None:
        selection_magnitude = selection_abs_baseline
    else:
        movement_columns = [
            feature_positions[name] for name in movement_fitted["features"]
        ]
        selection_magnitude = np.maximum(
            0.0,
            selection_abs_baseline
            + movement_fitted["model"].predict(
                values[np.ix_(selection_indices, movement_columns)]
            ),
        )
    rotation_name = str(rotation_winner["candidate"])
    rotation_selection_prediction = _predict_fitted(
        fitted[rotation_name],
        values,
        selection_indices,
        feature_positions,
        calibrations_by_feature,
    )
    combined_selection_score = (
        np.abs(rotation_selection_prediction) * selection_magnitude
    )
    combined_threshold, combined_validation_rotation = (
        _choose_rotation_threshold(
            decision_time[selection_indices],
            rotation_selection_prediction,
            long_net[selection_indices],
            short_net[selection_indices],
            ranking_score=combined_selection_score,
        )
    )

    frozen = {
        "scope": "nested_chronological_validation_only",
        "screen_feature_order": signed_ranked,
        "movement_feature_order": movement_ranked,
        "forecast_winner": forecast_winner["candidate"],
        "rotation_winner": rotation_winner["candidate"],
        "movement_winner": movement_winner["candidate"],
        "forecast_threshold": forecast_winner["rotation_threshold"],
        "rotation_threshold": rotation_winner["rotation_threshold"],
        "direction_plus_movement_threshold": combined_threshold,
        "final_labels_used_during_selection": False,
    }
    print(
        "[signal-benchmark] configuration frozen; opening historical final replay",
        flush=True,
    )

    feature_results: list[dict[str, Any]] = []
    validation_times = decision_time[validation_indices]
    final_times = decision_time[final_indices]
    duplicate_by_feature = {
        row["feature"]: row for row in duplicate_map
    }
    registry_by_feature = eligible.set_index("feature_name").to_dict("index")
    for number, calibration in enumerate(calibrations):
        feature = calibration.feature
        column = feature_positions[feature]
        validation_prediction = _signed_prediction(
            calibration, values[validation_indices, column]
        )
        final_prediction = _signed_prediction(
            calibration, values[final_indices, column]
        )
        validation_forecast = _forecast_metrics(
            y_signed[validation_indices], validation_prediction
        )
        final_forecast = _forecast_metrics(
            y_signed[final_indices], final_prediction
        )
        validation_magnitude_prediction = _magnitude_prediction(
            calibration,
            values[validation_indices, column],
            abs_baseline_by_pair[pair_codes[validation_indices]],
        )
        final_magnitude_prediction = _magnitude_prediction(
            calibration,
            values[final_indices, column],
            abs_baseline_by_pair[pair_codes[final_indices]],
        )
        validation_movement = _movement_metrics(
            y_abs[validation_indices],
            y_path[validation_indices],
            validation_magnitude_prediction,
            abs_baseline_by_pair[pair_codes[validation_indices]],
        )
        final_movement = _movement_metrics(
            y_abs[final_indices],
            y_path[final_indices],
            final_magnitude_prediction,
            abs_baseline_by_pair[pair_codes[final_indices]],
        )
        validation_rotation = rotation_metrics(
            validation_times,
            validation_prediction,
            long_net[validation_indices],
            short_net[validation_indices],
        )
        final_rotation = rotation_metrics(
            final_times,
            final_prediction,
            long_net[final_indices],
            short_net[final_indices],
        )
        source = registry_by_feature[feature]
        duplicate = duplicate_by_feature[feature]
        survived = bool(
            validation_forecast["rank_ic"] > 0.0
            and final_forecast["rank_ic"] > 0.0
            and (
                final_forecast["direction_accuracy"] > 0.5
                or final_movement["incremental_rank_ic"] > 0.0
            )
        )
        feature_results.append(
            {
                "feature_id": source["feature_id"],
                "feature": feature,
                "feature_family": source["feature_family"],
                "source_timeframe": source["source_timeframe"],
                "causal": True,
                "completed_bars_only": True,
                "signed_variant": calibration.signed_variant,
                "constant_on_train": bool(constant[column]),
                "dedup_group_id": duplicate["group_id"],
                "dedup_group_size": duplicate["group_size"],
                "dedup_representative": duplicate["representative"],
                "is_dedup_representative": duplicate["is_representative"],
                "screen_score": screen_by_feature[feature]["screen_score"],
                **_flatten("validation", validation_forecast),
                **_flatten("validation_movement", validation_movement),
                **_flatten("validation_rotation_top1", validation_rotation["top1"]),
                **_flatten("validation_rotation_top3", validation_rotation["top3"]),
                **_flatten("validation_rotation_top5", validation_rotation["top5"]),
                **_flatten("final", final_forecast),
                **_flatten("final_movement", final_movement),
                **_flatten("final_rotation_top1", final_rotation["top1"]),
                **_flatten("final_rotation_top3", final_rotation["top3"]),
                **_flatten("final_rotation_top5", final_rotation["top5"]),
                "validation_to_final_signal_survived": survived,
                "account_eligible": False,
                "deployment_status": "research_only_not_promoted",
            }
        )
        if (number + 1) % 100 == 0 or number + 1 == len(calibrations):
            print(
                f"[signal-benchmark] final-audited {number + 1}/"
                f"{len(calibrations)}",
                flush=True,
            )

    def selected_prediction(
        row: dict[str, Any], indices: np.ndarray
    ) -> np.ndarray:
        name = str(row["candidate"])
        if name == "no_change":
            return np.zeros(len(indices), dtype=float)
        return _predict_fitted(
            fitted[name],
            values,
            indices,
            feature_positions,
            calibrations_by_feature,
        )

    def evaluate_selected(row: dict[str, Any]) -> dict[str, Any]:
        name = str(row["candidate"])
        prediction = selected_prediction(row, final_indices)
        threshold = (
            float("inf")
            if name == "no_change"
            else float(fitted[name]["threshold"])
        )
        return {
            "candidate": name,
            "forecast": _forecast_metrics(y_signed[final_indices], prediction),
            "rotation": rotation_metrics(
                final_times,
                prediction,
                long_net[final_indices],
                short_net[final_indices],
                threshold=threshold,
            ),
            "threshold_frozen_on_validation": threshold,
        }

    final_forecast_winner = evaluate_selected(forecast_winner)
    final_rotation_winner = (
        final_forecast_winner
        if rotation_winner["candidate"] == forecast_winner["candidate"]
        else evaluate_selected(rotation_winner)
    )
    final_abs_baseline = abs_baseline_by_pair[pair_codes[final_indices]]
    if movement_fitted is None:
        final_magnitude = final_abs_baseline
    else:
        movement_columns = [
            feature_positions[name] for name in movement_fitted["features"]
        ]
        final_magnitude = np.maximum(
            0.0,
            final_abs_baseline
            + movement_fitted["model"].predict(
                values[np.ix_(final_indices, movement_columns)]
            ),
        )
    final_movement_winner = {
        "candidate": movement_winner["candidate"],
        "metrics": _movement_metrics(
            y_abs[final_indices],
            y_path[final_indices],
            final_magnitude,
            final_abs_baseline,
        ),
    }
    movement_pair_history_baseline_final = _movement_metrics(
        y_abs[final_indices],
        y_path[final_indices],
        final_abs_baseline,
        final_abs_baseline,
    )
    rotation_prediction = selected_prediction(rotation_winner, final_indices)
    combined_rotation = rotation_metrics(
        final_times,
        rotation_prediction,
        long_net[final_indices],
        short_net[final_indices],
        threshold=combined_threshold,
        ranking_score=np.abs(rotation_prediction) * final_magnitude,
    )
    forecast_prediction = selected_prediction(forecast_winner, final_indices)
    final_pair_breakdown = _group_forecast_breakdown(
        metadata.loc[final_indices, "instrument"].astype(str).to_numpy(),
        y_signed[final_indices],
        forecast_prediction,
        long_net[final_indices],
        short_net[final_indices],
        "instrument",
    )
    final_hours = pd.to_datetime(
        metadata.loc[final_indices, "decision_time_utc"], utc=True
    ).dt.hour.to_numpy()
    final_sessions = np.select(
        [
            (final_hours >= 0) & (final_hours < 7),
            (final_hours >= 7) & (final_hours < 12),
            (final_hours >= 12) & (final_hours < 16),
            (final_hours >= 16) & (final_hours < 21),
        ],
        ["asia", "london", "london_new_york_overlap", "new_york_late"],
        default="rollover",
    )
    final_session_breakdown = _group_forecast_breakdown(
        final_sessions,
        y_signed[final_indices],
        forecast_prediction,
        long_net[final_indices],
        short_net[final_indices],
        "session",
    )

    feature_results_frame = pd.DataFrame(feature_results).sort_values(
        ["screen_score", "validation_rank_ic"], ascending=False
    )
    duplicate_frame = pd.DataFrame(duplicate_map).sort_values(
        ["group_size", "group_id", "is_representative"],
        ascending=[False, True, False],
    )
    combination_rows: list[dict[str, Any]] = []
    for row in candidates:
        combination_rows.append(
            {
                "candidate": row["candidate"],
                "kind": row["kind"],
                "feature_count": row["feature_count"],
                "alpha": row.get("alpha"),
                **_flatten("validation_selection", row["validation_selection"]),
                **_flatten(
                    "validation_selection_rotation_top1",
                    row["validation_selection_rotation"]["top1"],
                ),
                **_flatten(
                    "validation_selection_rotation_top3",
                    row["validation_selection_rotation"]["top3"],
                ),
                **_flatten(
                    "validation_selection_rotation_top5",
                    row["validation_selection_rotation"]["top5"],
                ),
                "rotation_threshold": row["rotation_threshold"],
                "selected_forecast_winner": (
                    row["candidate"] == forecast_winner["candidate"]
                ),
                "selected_rotation_winner": (
                    row["candidate"] == rotation_winner["candidate"]
                ),
            }
        )
    combination_frame = pd.DataFrame(combination_rows).sort_values(
        "validation_selection_mae_relative_to_no_change"
    )
    movement_frame = pd.DataFrame(
        [
            {
                "candidate": row["candidate"],
                "feature_count": row["feature_count"],
                "alpha": row.get("alpha"),
                **_flatten("validation_selection", row["validation_selection"]),
                "selected_movement_winner": (
                    row["candidate"] == movement_winner["candidate"]
                ),
            }
            for row in movement_candidates
        ]
    ).sort_values("validation_selection_mae_relative_to_pair_history")

    feature_results_path = run_dir / "ALL_FEATURE_SIGNAL_RESULTS.csv"
    duplicate_path = run_dir / "FEATURE_DEDUPLICATION_MAP.csv"
    combination_path = run_dir / "SIGNAL_COMBINATION_RESULTS.csv"
    movement_path = run_dir / "MOVEMENT_COMBINATION_RESULTS.csv"
    registry_path = run_dir / "UNIFIED_RESEARCH_SIGNAL_REGISTRY.csv"
    pair_breakdown_path = run_dir / "SELECTED_FORECAST_PAIR_BREAKDOWN.csv"
    session_breakdown_path = run_dir / "SELECTED_FORECAST_SESSION_BREAKDOWN.csv"
    feature_results_frame.to_csv(feature_results_path, index=False)
    duplicate_frame.to_csv(duplicate_path, index=False)
    combination_frame.to_csv(combination_path, index=False)
    movement_frame.to_csv(movement_path, index=False)
    pd.DataFrame(final_pair_breakdown).to_csv(pair_breakdown_path, index=False)
    pd.DataFrame(final_session_breakdown).to_csv(
        session_breakdown_path, index=False
    )
    feature_results_frame[
        [
            "feature_id",
            "feature",
            "feature_family",
            "source_timeframe",
            "causal",
            "completed_bars_only",
            "signed_variant",
            "dedup_representative",
            "validation_rank_ic",
            "final_rank_ic",
            "validation_movement_rank_ic",
            "validation_movement_incremental_rank_ic",
            "final_movement_rank_ic",
            "final_movement_incremental_rank_ic",
            "validation_to_final_signal_survived",
            "account_eligible",
            "deployment_status",
        ]
    ].to_csv(registry_path, index=False)

    family_summary = (
        feature_results_frame.groupby(
            ["source_timeframe", "feature_family"], dropna=False
        )
        .agg(
            features=("feature", "count"),
            representatives=("is_dedup_representative", "sum"),
            median_validation_rank_ic=("validation_rank_ic", "median"),
            best_validation_rank_ic=("validation_rank_ic", "max"),
            median_final_rank_ic=("final_rank_ic", "median"),
            best_final_rank_ic=("final_rank_ic", "max"),
            median_validation_movement_rank_ic=(
                "validation_movement_rank_ic",
                "median",
            ),
            median_validation_movement_incremental_rank_ic=(
                "validation_movement_incremental_rank_ic",
                "median",
            ),
            median_final_movement_rank_ic=(
                "final_movement_rank_ic",
                "median",
            ),
            median_final_movement_incremental_rank_ic=(
                "final_movement_incremental_rank_ic",
                "median",
            ),
            survived=("validation_to_final_signal_survived", "sum"),
        )
        .reset_index()
        .to_dict("records")
    )
    prior_validation_path = (
        Path(__file__).resolve().parents[1]
        / "reports"
        / "unified_forecast_full_validation_native_pip_20260725"
        / "UNIFIED_FORECAST_VALIDATION.json"
    )
    prior_reference: dict[str, Any] = {"available": False}
    if prior_validation_path.is_file():
        prior = json.loads(prior_validation_path.read_text(encoding="utf-8"))
        prior_winner = prior["selection"]["winner"]
        prior_reference = {
            "available": True,
            "path": str(prior_validation_path.resolve()),
            "winner": prior_winner["model_candidate"],
            "validation": prior_winner["validation"],
            "final": prior["untouched_final"]["selected_model"],
            "final_tradability": prior["tradability_diagnostic"]["final"],
            "verdict": prior["verdict"],
        }

    forecast_pass = bool(
        final_forecast_winner["forecast"]["mae_relative_to_no_change"] < 1.0
        and final_forecast_winner["forecast"]["rank_ic"] > 0.0
    )
    movement_pass = bool(
        final_movement_winner["metrics"]["mae_relative_to_pair_history"] < 1.0
        and final_movement_winner["metrics"]["incremental_rank_ic"] > 0.0
    )
    rotation_pass = bool(
        final_rotation_winner["rotation"]["top3"]["mean_net_pips"] > 0.0
        and final_rotation_winner["rotation"]["top3"]["trades"] >= 100
    )
    verdict = "PASS" if forecast_pass and rotation_pass else "FAIL"
    summary = {
        "schema_version": 1,
        "pipeline_version": PIPELINE_VERSION,
        "created_utc": utc_now_stamp(),
        "status": "complete",
        "verdict": verdict,
        "forecast_pass": forecast_pass,
        "movement_pass": movement_pass,
        "rotation_after_cost_pass": rotation_pass,
        "research_only": True,
        "deployment_performed": False,
        "account_eligible": False,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "matrix": str(matrix_path.resolve()),
        "matrix_sha256": _sha256(matrix_path),
        "source_registry": str(feature_registry_path.resolve()),
        "source_registry_rows": len(registry),
        "eligible_causal_features": len(feature_names),
        "rows": len(metadata),
        "pairs": pair_count,
        "horizon_minutes": horizon,
        "split": split,
        "nested_validation": nested_split,
        "deduplication": {
            "threshold_absolute_correlation": correlation_threshold,
            "scope": "training_only",
            "groups": len(groups),
            "representatives": len(representatives),
            "features_removed_as_near_duplicates": len(feature_names)
            - len(representatives),
            "largest_group": max(len(group) for group in groups),
        },
        "selection_frozen_before_final": frozen,
        "forecast_winner_validation_selection": forecast_winner,
        "forecast_winner_final": final_forecast_winner,
        "rotation_winner_validation_selection": rotation_winner,
        "rotation_winner_final": final_rotation_winner,
        "movement_winner_validation_selection": movement_winner,
        "movement_winner_final": final_movement_winner,
        "movement_pair_history_baseline_final": (
            movement_pair_history_baseline_final
        ),
        "direction_plus_movement_validation_rotation": (
            combined_validation_rotation
        ),
        "direction_plus_movement_final_rotation": combined_rotation,
        "selected_forecast_final_pair_breakdown": final_pair_breakdown,
        "selected_forecast_final_session_breakdown": final_session_breakdown,
        "no_trade_result": {
            "total_net_pips": 0.0,
            "mean_net_pips": 0.0,
            "trades": 0,
        },
        "feature_survival": {
            "features_with_positive_validation_rank_ic": int(
                (feature_results_frame["validation_rank_ic"] > 0.0).sum()
            ),
            "features_with_positive_final_rank_ic": int(
                (feature_results_frame["final_rank_ic"] > 0.0).sum()
            ),
            "validation_positive_that_remained_final_positive": int(
                (
                    (feature_results_frame["validation_rank_ic"] > 0.0)
                    & (feature_results_frame["final_rank_ic"] > 0.0)
                ).sum()
            ),
            "features_meeting_survival_rule": int(
                feature_results_frame[
                    "validation_to_final_signal_survived"
                ].sum()
            ),
        },
        "family_summary": family_summary,
        "prior_full_matrix_validation_reference": prior_reference,
        "artifacts": {
            "all_feature_results": str(feature_results_path.resolve()),
            "deduplication_map": str(duplicate_path.resolve()),
            "combination_results": str(combination_path.resolve()),
            "movement_results": str(movement_path.resolve()),
            "research_signal_registry": str(registry_path.resolve()),
            "selected_forecast_pair_breakdown": str(
                pair_breakdown_path.resolve()
            ),
            "selected_forecast_session_breakdown": str(
                session_breakdown_path.resolve()
            ),
        },
        "limitations": [
            "The consolidated matrix is a deterministic 195-minute snapshot sample, not every M1 decision row.",
            "The historical final period was already opened by an earlier repository run; this is a frozen-config replay, not a never-before-seen test.",
            "Individual feature calibrations are diagnostic research signals and are not account-eligible.",
            "Bid/ask and configured round-trip slippage are included only in the separate rotation diagnostic.",
            "Pip totals aggregate heterogeneous instruments and are diagnostics, not account-currency portfolio P/L.",
        ],
    }
    json_path = run_dir / "UNIFIED_SIGNAL_BENCHMARK_REPORT.json"
    _write_json(json_path, summary)

    top_features = feature_results_frame.head(20).to_dict("records")
    lines = [
        "# Unified Signal Benchmark",
        "",
        f"**Verdict: {verdict}**",
        "",
        "## Scope",
        "",
        f"- Matrix rows: {len(metadata):,}",
        f"- Pairs: {pair_count}",
        f"- Causal materialized features tested: {len(feature_names)}",
        f"- Training-only correlation representatives: {len(representatives)}",
        f"- Forecast horizon: {horizon} minutes",
        "- Selection: validation screen -> purged validation selection -> historical final replay",
        "- Execution/deployment: disabled; no account promotion performed",
        "",
        "## Selected Results",
        "",
        f"- Forecast winner: `{forecast_winner['candidate']}`",
        (
            "- Forecast final MAE/no-change: "
            f"{final_forecast_winner['forecast']['mae_relative_to_no_change']:.4f}"
        ),
        (
            "- Forecast final direction accuracy: "
            f"{final_forecast_winner['forecast']['direction_accuracy']:.2%}"
        ),
        (
            "- Forecast final rank IC: "
            f"{final_forecast_winner['forecast']['rank_ic']:.4f}"
        ),
        f"- Rotation winner: `{rotation_winner['candidate']}`",
        (
            "- Rotation final top-3 mean net: "
            f"{final_rotation_winner['rotation']['top3']['mean_net_pips']:.4f} pips"
        ),
        (
            "- Rotation final top-3 total net: "
            f"{final_rotation_winner['rotation']['top3']['total_net_pips']:.2f} pips"
        ),
        f"- Movement winner: `{movement_winner['candidate']}`",
        (
            "- Movement final MAE/pair-history: "
            f"{final_movement_winner['metrics']['mae_relative_to_pair_history']:.4f}"
        ),
        (
            "- Movement final incremental rank IC: "
            f"{final_movement_winner['metrics']['incremental_rank_ic']:.4f}"
        ),
        "",
        "## Top Validation-Screened Features",
        "",
        *_markdown_table(
            top_features,
            [
                ("feature", "Feature"),
                ("source_timeframe", "TF"),
                ("validation_rank_ic", "Val IC"),
                ("final_rank_ic", "Final IC"),
                ("validation_direction_accuracy", "Val direction"),
                ("final_direction_accuracy", "Final direction"),
                (
                    "validation_movement_incremental_rank_ic",
                    "Val incremental move IC",
                ),
                (
                    "final_movement_incremental_rank_ic",
                    "Final incremental move IC",
                ),
            ],
        ),
        "",
        "## Interpretation",
        "",
        (
            f"- {summary['feature_survival']['validation_positive_that_remained_final_positive']} "
            "features had positive validation and final signed rank IC."
        ),
        (
            f"- {summary['feature_survival']['features_meeting_survival_rule']} "
            "features met the broader direction-or-movement survival rule."
        ),
        (
            f"- {len(feature_names) - len(representatives)} features were "
            "removed from combination selection as training-only near-duplicates."
        ),
        f"- No-trade net result is 0; rotation after-cost pass: {rotation_pass}.",
        "",
        "## Caveats",
        "",
        *[f"- {item}" for item in summary["limitations"]],
    ]
    md_path = run_dir / "UNIFIED_SIGNAL_BENCHMARK_REPORT.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _write_json(
        run_dir / "UNIFIED_SIGNAL_BENCHMARK_RUN_COMPLETE.json",
        {
            "status": "complete",
            "verdict": verdict,
            "report_json": str(json_path.resolve()),
            "report_markdown": str(md_path.resolve()),
            "artifacts": summary["artifacts"],
            "live_execution_enabled": False,
            "oanda_execution_enabled": False,
            "deployment_performed": False,
        },
    )
    return json_path
