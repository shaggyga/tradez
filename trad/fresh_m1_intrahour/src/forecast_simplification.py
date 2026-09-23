"""Validation-frozen simplification audit for the unified one-hour forecast.

This is a research-only diagnostic. It reuses the feature ordering discovered
by the all-signal benchmark, compares compact linear combinations, freezes the
model and signal cutoffs on nested validation, and opens the historical final
block once for the selected specifications.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge, RidgeClassifier

from .common import utc_now_stamp
from .unified_signal_benchmark import (
    DEFAULT_DATA_ROOT,
    EPSILON,
    FeatureCalibration,
    _fit_calibration,
    _forecast_metrics,
    _json_value,
    _screen_score,
    _signed_prediction,
    _standardize_by_instrument,
    chronological_indices,
    nested_validation_indices,
)


PIPELINE_VERSION = "forecast_simplification_v1"
DEFAULT_BENCHMARK_REPORT = (
    Path(__file__).resolve().parents[1]
    / "reports"
    / "unified_signal_benchmark_20260726_all795_v2"
    / "UNIFIED_SIGNAL_BENCHMARK_REPORT.json"
)


def wilson_lower_bound(correct: int, total: int, z: float = 1.96) -> float:
    """Return the Wilson lower confidence bound for a binomial proportion."""
    if total <= 0:
        return 0.0
    proportion = correct / total
    denominator = 1.0 + z * z / total
    center = proportion + z * z / (2.0 * total)
    margin = z * math.sqrt(
        proportion * (1.0 - proportion) / total
        + z * z / (4.0 * total * total)
    )
    return (center - margin) / denominator


def signal_slice_metrics(
    actual: np.ndarray,
    prediction: np.ndarray,
    long_net: np.ndarray,
    short_net: np.ndarray,
    mask: np.ndarray | None = None,
) -> dict[str, Any]:
    """Score only emitted signals, including executable post-cost outcomes."""
    actual = np.asarray(actual, dtype=float)
    prediction = np.asarray(prediction, dtype=float)
    long_net = np.asarray(long_net, dtype=float)
    short_net = np.asarray(short_net, dtype=float)
    valid = (
        np.isfinite(actual)
        & np.isfinite(prediction)
        & np.isfinite(long_net)
        & np.isfinite(short_net)
        & (np.abs(actual) > EPSILON)
        & (np.abs(prediction) > EPSILON)
    )
    if mask is not None:
        valid &= np.asarray(mask, dtype=bool)
    selected_actual = actual[valid]
    selected_prediction = prediction[valid]
    selected_net = np.where(
        selected_prediction > 0.0, long_net[valid], short_net[valid]
    )
    correct = int(
        np.sum(np.sign(selected_actual) == np.sign(selected_prediction))
    )
    trades = len(selected_actual)
    gains = float(selected_net[selected_net > 0.0].sum())
    losses = float(-selected_net[selected_net < 0.0].sum())
    gross_directional = np.sign(selected_prediction) * selected_actual
    if trades >= 20 and np.ptp(selected_prediction) > EPSILON:
        rank_ic = float(
            pd.Series(selected_prediction).corr(
                pd.Series(selected_actual), method="spearman"
            )
        )
        if not math.isfinite(rank_ic):
            rank_ic = 0.0
    else:
        rank_ic = 0.0
    return {
        "signals": trades,
        "coverage": float(trades / max(1, len(actual))),
        "direction_correct": correct,
        "direction_accuracy": float(correct / trades) if trades else 0.0,
        "direction_accuracy_wilson_lower_95": wilson_lower_bound(
            correct, trades
        ),
        "rank_ic": rank_ic,
        "mean_gross_directional_pips": (
            float(np.mean(gross_directional)) if trades else 0.0
        ),
        "mean_executable_net_pips": (
            float(np.mean(selected_net)) if trades else 0.0
        ),
        "total_executable_net_pips": (
            float(np.sum(selected_net)) if trades else 0.0
        ),
        "executable_win_rate": (
            float(np.mean(selected_net > 0.0)) if trades else 0.0
        ),
        "profit_factor": (
            gains / losses
            if losses > 0.0
            else (float("inf") if gains > 0.0 else 0.0)
        ),
    }


def movement_metrics(
    actual: np.ndarray, prediction: np.ndarray, baseline: np.ndarray
) -> dict[str, float]:
    valid = (
        np.isfinite(actual)
        & np.isfinite(prediction)
        & np.isfinite(baseline)
    )
    actual = np.asarray(actual[valid], dtype=float)
    prediction = np.asarray(prediction[valid], dtype=float)
    baseline = np.asarray(baseline[valid], dtype=float)
    mae = float(np.mean(np.abs(actual - prediction)))
    baseline_mae = float(np.mean(np.abs(actual - baseline)))
    rank_ic = float(
        pd.Series(prediction).corr(pd.Series(actual), method="spearman")
    )
    return {
        "mae_pips": mae,
        "mae_relative_to_pair_history": mae / max(EPSILON, baseline_mae),
        "rank_ic": rank_ic if math.isfinite(rank_ic) else 0.0,
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_json_value(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _pair_training_statistic(
    values: np.ndarray,
    pair_codes: np.ndarray,
    train_indices: np.ndarray,
    pair_count: int,
    *,
    reducer: Callable[[np.ndarray], float],
) -> np.ndarray:
    global_value = reducer(values[train_indices])
    result = np.full(pair_count, global_value, dtype=float)
    for pair in range(pair_count):
        positions = train_indices[pair_codes[train_indices] == pair]
        if len(positions) >= 20:
            value = reducer(values[positions])
            if math.isfinite(value):
                result[pair] = value
    return result


def _timeframe_balanced(predictions: np.ndarray, features: list[str]) -> np.ndarray:
    groups: dict[str, list[int]] = {}
    for position, feature in enumerate(features):
        groups.setdefault(feature.split("__", 1)[0], []).append(position)
    group_predictions = [
        np.mean(predictions[:, positions], axis=1)
        for positions in groups.values()
    ]
    return np.mean(np.column_stack(group_predictions), axis=1)


def run_forecast_simplification(
    cfg: dict[str, Any],
    run_dir: Path,
    *,
    benchmark_report_path: Path | None = None,
    matrix_path: Path | None = None,
    horizon: int = 60,
) -> Path:
    os.environ.setdefault("OMP_NUM_THREADS", "2")
    os.environ.setdefault("MKL_NUM_THREADS", "2")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "2")
    run_dir.mkdir(parents=True, exist_ok=True)
    benchmark_report_path = benchmark_report_path or DEFAULT_BENCHMARK_REPORT
    if not benchmark_report_path.is_file():
        raise FileNotFoundError(benchmark_report_path)
    benchmark = json.loads(benchmark_report_path.read_text(encoding="utf-8"))
    if benchmark.get("selection_frozen_before_final", {}).get(
        "final_labels_used_during_selection"
    ):
        raise ValueError("source benchmark did not preserve final isolation")
    ranked = benchmark["selection_frozen_before_final"]["screen_feature_order"]
    direction_features = [str(name) for name in ranked[:40]]
    movement_specification = benchmark["movement_winner_validation_selection"]
    movement_features = [
        str(name) for name in movement_specification["features"]
    ]
    features = list(dict.fromkeys([*direction_features, *movement_features]))
    matrix_path = matrix_path or Path(
        benchmark.get("matrix") or DEFAULT_DATA_ROOT / "unified_training_matrix.parquet"
    )
    if not matrix_path.is_file():
        raise FileNotFoundError(matrix_path)

    targets = {
        "signed": f"target_return_pips_{horizon}",
        "abs": f"target_abs_move_pips_{horizon}",
        "long": f"diag_long_net_pips_{horizon}",
        "short": f"diag_short_net_pips_{horizon}",
    }
    columns = [
        "decision_time_utc",
        "instrument",
        *features,
        *targets.values(),
    ]
    print(
        f"[forecast-simplification] loading {len(features)} selected features",
        flush=True,
    )
    frame = pd.read_parquet(matrix_path, columns=columns)
    frame["decision_time_utc"] = pd.to_datetime(
        frame["decision_time_utc"], errors="coerce", utc=True
    )
    frame = (
        frame.dropna(
            subset=["decision_time_utc", "instrument", *targets.values()]
        )
        .sort_values(["decision_time_utc", "instrument"])
        .reset_index(drop=True)
    )
    settings = cfg.get("unified_intrahour_forecast", {})
    train, validation, final, split = chronological_indices(
        frame["decision_time_utc"],
        float(settings.get("validation_fraction", 0.2)),
        float(settings.get("final_fraction", 0.2)),
        int(settings.get("purge_minutes", horizon)),
    )
    screen, selection, nested = nested_validation_indices(
        frame["decision_time_utc"],
        validation,
        int(settings.get("purge_minutes", horizon)),
    )
    instruments = pd.Categorical(frame["instrument"])
    pair_codes = instruments.codes.astype(np.int16)
    pair_count = len(instruments.categories)
    y = frame[targets["signed"]].to_numpy(dtype=float)
    y_abs = frame[targets["abs"]].to_numpy(dtype=float)
    long_net = frame[targets["long"]].to_numpy(dtype=float)
    short_net = frame[targets["short"]].to_numpy(dtype=float)
    values = frame[features].to_numpy(dtype=np.float32, copy=True)
    values, _, _, constant = _standardize_by_instrument(
        values, pair_codes, train, pair_count
    )

    pair_abs_scale = _pair_training_statistic(
        np.abs(y),
        pair_codes,
        train,
        pair_count,
        reducer=lambda block: float(np.mean(block)),
    )
    row_cost = np.maximum(0.0, -0.5 * (long_net + short_net))
    pair_cost = _pair_training_statistic(
        row_cost,
        pair_codes,
        train,
        pair_count,
        reducer=lambda block: float(np.median(block)),
    )
    feature_positions = {
        feature: position for position, feature in enumerate(features)
    }
    movement_columns = np.asarray(
        [feature_positions[feature] for feature in movement_features],
        dtype=int,
    )
    movement_model = Ridge(
        alpha=float(movement_specification["alpha"]),
        fit_intercept=False,
        solver="lsqr",
        max_iter=300,
        tol=1e-4,
    )
    movement_model.fit(
        values[np.ix_(train, movement_columns)],
        y_abs[train] - pair_abs_scale[pair_codes[train]],
    )

    def movement_prediction(indices: np.ndarray) -> np.ndarray:
        return np.maximum(
            0.0,
            pair_abs_scale[pair_codes[indices]]
            + movement_model.predict(values[np.ix_(indices, movement_columns)]),
        )

    calibrations: list[FeatureCalibration] = []
    screen_scores: list[float] = []
    for position, feature in enumerate(direction_features):
        calibration = _fit_calibration(
            feature,
            values[train, position].astype(float),
            y[train],
            np.abs(y[train]),
            pair_abs_scale[pair_codes[train]],
        )
        linear = _forecast_metrics(
            y[screen],
            _signed_prediction(
                calibration, values[screen, position].astype(float), "linear"
            ),
        )
        binned = _forecast_metrics(
            y[screen],
            _signed_prediction(
                calibration, values[screen, position].astype(float), "binned"
            ),
        )
        if _screen_score(binned) > _screen_score(linear):
            calibration.signed_variant = "binned"
            selected_metrics = binned
        else:
            calibration.signed_variant = "linear"
            selected_metrics = linear
        calibrations.append(calibration)
        screen_scores.append(_screen_score(selected_metrics))

    def calibrated_matrix(indices: np.ndarray, count: int) -> np.ndarray:
        output = np.empty((len(indices), count), dtype=np.float32)
        for column in range(count):
            output[:, column] = _signed_prediction(
                calibrations[column],
                values[indices, column].astype(float),
            )
        return output

    candidate_specs: dict[str, dict[str, Any]] = {}
    predictors: dict[str, Callable[[np.ndarray], np.ndarray]] = {}

    def add_candidate(
        name: str,
        kind: str,
        feature_count: int,
        predictor: Callable[[np.ndarray], np.ndarray],
        **metadata: Any,
    ) -> None:
        predictors[name] = predictor
        candidate_specs[name] = {
            "candidate": name,
            "kind": kind,
            "feature_count": feature_count,
            "features": direction_features[:feature_count],
            **metadata,
        }

    for count in (1, 3, 5, 10, 20, 40):
        def equal_predictor(
            indices: np.ndarray, selected_count: int = count
        ) -> np.ndarray:
            return np.mean(
                calibrated_matrix(indices, selected_count), axis=1
            )

        add_candidate(
            f"equal_calibrated_k{count}",
            "equal_calibrated",
            count,
            equal_predictor,
        )

        weights = np.maximum(
            np.asarray(screen_scores[:count], dtype=float), 0.0
        )
        if float(weights.sum()) <= EPSILON:
            weights = np.ones(count, dtype=float)

        def weighted_predictor(
            indices: np.ndarray,
            selected_count: int = count,
            selected_weights: np.ndarray = weights,
        ) -> np.ndarray:
            return np.average(
                calibrated_matrix(indices, selected_count),
                axis=1,
                weights=selected_weights,
            )

        add_candidate(
            f"screen_weighted_k{count}",
            "screen_weighted_calibrated",
            count,
            weighted_predictor,
        )

    def balanced_predictor(indices: np.ndarray) -> np.ndarray:
        return _timeframe_balanced(
            calibrated_matrix(indices, 40), direction_features
        )

    add_candidate(
        "timeframe_balanced_k40",
        "timeframe_balanced_calibrated",
        40,
        balanced_predictor,
    )

    for count in (3, 5, 10, 20, 40):
        train_x = values[np.ix_(train, np.arange(count))]
        for alpha in (10.0, 100.0, 1000.0):
            ridge = Ridge(
                alpha=alpha,
                fit_intercept=False,
                solver="lsqr",
                max_iter=300,
                tol=1e-4,
            )
            ridge.fit(train_x, y[train])

            def ridge_predictor(
                indices: np.ndarray,
                model: Ridge = ridge,
                selected_count: int = count,
            ) -> np.ndarray:
                return np.asarray(
                    model.predict(
                        values[np.ix_(indices, np.arange(selected_count))]
                    ),
                    dtype=float,
                )

            add_candidate(
                f"ridge_return_k{count}_a{alpha:g}",
                "ridge_return",
                count,
                ridge_predictor,
                alpha=alpha,
            )

            classifier = RidgeClassifier(
                alpha=alpha,
                fit_intercept=True,
                class_weight="balanced",
            )
            classifier.fit(
                train_x,
                np.where(y[train] > 0.0, 1, -1),
            )

            def classifier_predictor(
                indices: np.ndarray,
                model: RidgeClassifier = classifier,
                selected_count: int = count,
            ) -> np.ndarray:
                score = np.asarray(
                    model.decision_function(
                        values[np.ix_(indices, np.arange(selected_count))]
                    ),
                    dtype=float,
                )
                return (
                    np.tanh(score)
                    * pair_abs_scale[pair_codes[indices]]
                )

            add_candidate(
                f"ridge_direction_k{count}_a{alpha:g}",
                "ridge_direction",
                count,
                classifier_predictor,
                alpha=alpha,
            )

    validation_rows: list[dict[str, Any]] = []
    for name, specification in candidate_specs.items():
        prediction = predictors[name](selection)
        forecast = _forecast_metrics(y[selection], prediction)
        signals = signal_slice_metrics(
            y[selection],
            prediction,
            long_net[selection],
            short_net[selection],
        )
        validation_rows.append(
            {
                **specification,
                **{f"forecast_{key}": value for key, value in forecast.items()},
                **{f"signal_{key}": value for key, value in signals.items()},
            }
        )

    accuracy_winner = max(
        validation_rows,
        key=lambda row: (
            row["signal_direction_accuracy"],
            row["signal_rank_ic"],
            -row["feature_count"],
        ),
    )
    best_accuracy = float(accuracy_winner["signal_direction_accuracy"])
    best_rank = max(0.0, float(accuracy_winner["signal_rank_ic"]))
    compact_pool = [
        row
        for row in validation_rows
        if row["signal_direction_accuracy"] >= best_accuracy - 0.0025
        and row["signal_rank_ic"] >= 0.8 * best_rank
    ] or [accuracy_winner]
    compact_winner = min(
        compact_pool,
        key=lambda row: (
            row["feature_count"],
            -row["signal_direction_accuracy"],
            -row["signal_rank_ic"],
        ),
    )
    compact_name = str(compact_winner["candidate"])
    compact_validation_prediction = predictors[compact_name](selection)
    validation_movement_prediction = movement_prediction(selection)
    validation_movement_metrics = movement_metrics(
        y_abs[selection],
        validation_movement_prediction,
        pair_abs_scale[pair_codes[selection]],
    )

    # The model is frozen before policy selection. Confidence and pair-cost
    # cutoffs are then selected on validation only.
    finite_confidence = np.abs(compact_validation_prediction)
    confidence_thresholds = sorted(
        {
            float(np.quantile(finite_confidence, quantile))
            for quantile in (0.0, 0.5, 0.75, 0.9, 0.95)
        }
    )
    finite_pair_cost = pair_cost[np.isfinite(pair_cost)]
    cost_limits = sorted(
        {
            float(np.quantile(finite_pair_cost, quantile))
            for quantile in (0.25, 0.5, 0.75, 1.0)
        }
    )
    move_to_cost_thresholds = (0.0, 1.0, 1.5, 2.0, 3.0, 5.0)
    validation_move_to_cost = validation_movement_prediction / np.maximum(
        pair_cost[pair_codes[selection]], 0.1
    )
    policy_rows: list[dict[str, Any]] = []
    for confidence_threshold in confidence_thresholds:
        for cost_limit in cost_limits:
            for move_to_cost_threshold in move_to_cost_thresholds:
                mask = (
                    (
                        np.abs(compact_validation_prediction)
                        >= confidence_threshold
                    )
                    & (pair_cost[pair_codes[selection]] <= cost_limit)
                    & (
                        validation_move_to_cost
                        >= move_to_cost_threshold
                    )
                )
                metrics = signal_slice_metrics(
                    y[selection],
                    compact_validation_prediction,
                    long_net[selection],
                    short_net[selection],
                    mask,
                )
                policy_rows.append(
                    {
                        "model": compact_name,
                        "confidence_threshold_pips": confidence_threshold,
                        "maximum_training_pair_median_cost_pips": cost_limit,
                        "minimum_predicted_move_to_training_pair_cost": (
                            move_to_cost_threshold
                        ),
                        **metrics,
                    }
                )
    eligible_policies = [
        row for row in policy_rows if row["signals"] >= 500
    ] or policy_rows
    accuracy_policy = max(
        eligible_policies,
        key=lambda row: (
            row["direction_accuracy_wilson_lower_95"],
            row["direction_accuracy"],
            row["signals"],
        ),
    )
    net_policy = max(
        eligible_policies,
        key=lambda row: (
            row["mean_executable_net_pips"],
            row["profit_factor"],
            row["signals"],
        ),
    )

    frozen = {
        "selection_scope": "nested_chronological_validation_only",
        "source_feature_order_frozen_from": str(
            benchmark_report_path.resolve()
        ),
        "accuracy_winner": accuracy_winner["candidate"],
        "compact_winner": compact_name,
        "compact_features": compact_winner["features"],
        "accuracy_signal_policy": {
            key: accuracy_policy[key]
            for key in (
                "confidence_threshold_pips",
                "maximum_training_pair_median_cost_pips",
                "minimum_predicted_move_to_training_pair_cost",
            )
        },
        "net_signal_policy": {
            key: net_policy[key]
            for key in (
                "confidence_threshold_pips",
                "maximum_training_pair_median_cost_pips",
                "minimum_predicted_move_to_training_pair_cost",
            )
        },
        "base_model_trials": len(validation_rows),
        "signal_policy_trials": len(policy_rows),
        "final_labels_used_during_selection": False,
    }
    print(
        "[forecast-simplification] selections frozen; opening final block",
        flush=True,
    )

    selected_names = list(
        dict.fromkeys(
            [
                "equal_calibrated_k40",
                str(accuracy_winner["candidate"]),
                compact_name,
            ]
        )
    )
    final_models: dict[str, Any] = {}
    for name in selected_names:
        prediction = predictors[name](final)
        final_models[name] = {
            "specification": candidate_specs[name],
            "forecast": _forecast_metrics(y[final], prediction),
            "signals": signal_slice_metrics(
                y[final],
                prediction,
                long_net[final],
                short_net[final],
            ),
        }

    compact_final_prediction = predictors[compact_name](final)
    final_movement_prediction = movement_prediction(final)
    final_movement_metrics = movement_metrics(
        y_abs[final],
        final_movement_prediction,
        pair_abs_scale[pair_codes[final]],
    )

    def evaluate_final_policy(policy: dict[str, Any]) -> dict[str, Any]:
        final_move_to_cost = final_movement_prediction / np.maximum(
            pair_cost[pair_codes[final]], 0.1
        )
        mask = (
            np.abs(compact_final_prediction)
            >= float(policy["confidence_threshold_pips"])
        ) & (
            pair_cost[pair_codes[final]]
            <= float(policy["maximum_training_pair_median_cost_pips"])
        ) & (
            final_move_to_cost
            >= float(
                policy["minimum_predicted_move_to_training_pair_cost"]
            )
        )
        return {
            "frozen_policy": {
                key: policy[key]
                for key in (
                    "confidence_threshold_pips",
                    "maximum_training_pair_median_cost_pips",
                    "minimum_predicted_move_to_training_pair_cost",
                )
            },
            "metrics": signal_slice_metrics(
                y[final],
                compact_final_prediction,
                long_net[final],
                short_net[final],
                mask,
            ),
        }

    final_accuracy_policy = evaluate_final_policy(accuracy_policy)
    final_net_policy = evaluate_final_policy(net_policy)
    compact_final = final_models[compact_name]
    forecasting_pass = bool(
        compact_final["signals"]["direction_accuracy"] > 0.5
        and compact_final["signals"]["rank_ic"] > 0.0
    )
    tradability_pass = bool(
        final_net_policy["metrics"]["signals"] >= 100
        and final_net_policy["metrics"]["mean_executable_net_pips"] > 0.0
    )
    result = {
        "schema_version": 1,
        "pipeline_version": PIPELINE_VERSION,
        "created_utc": utc_now_stamp(),
        "status": "complete",
        "verdict": "PASS" if forecasting_pass and tradability_pass else "FAIL",
        "forecasting_pass": forecasting_pass,
        "tradability_pass": tradability_pass,
        "research_only": True,
        "deployment_performed": False,
        "account_eligible": False,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "matrix": str(matrix_path.resolve()),
        "source_benchmark": str(benchmark_report_path.resolve()),
        "horizon_minutes": horizon,
        "rows": len(frame),
        "pairs": pair_count,
        "features_loaded": len(features),
        "constant_features_on_train": int(np.sum(constant)),
        "split": split,
        "nested_validation": nested,
        "selection_frozen_before_final": frozen,
        "accuracy_winner_validation": accuracy_winner,
        "compact_winner_validation": compact_winner,
        "movement_gate_validation": {
            "specification": movement_specification,
            "metrics": validation_movement_metrics,
        },
        "accuracy_policy_validation": accuracy_policy,
        "net_policy_validation": net_policy,
        "selected_models_final": final_models,
        "movement_gate_final": {
            "specification": movement_specification,
            "metrics": final_movement_metrics,
        },
        "accuracy_policy_final": final_accuracy_policy,
        "net_policy_final": final_net_policy,
        "no_change_baseline": {
            "direction_accuracy": 0.0,
            "mae_relative_to_no_change": 1.0,
            "executable_net_pips": 0.0,
            "trades": 0,
        },
        "interpretation": {
            "simplification_succeeded": (
                compact_winner["feature_count"] < 40
                and compact_final["signals"]["direction_accuracy"] > 0.5
            ),
            "confidence_gate_improved_final_accuracy": (
                final_accuracy_policy["metrics"]["direction_accuracy"]
                > compact_final["signals"]["direction_accuracy"]
            ),
            "any_selected_policy_positive_after_cost": (
                final_accuracy_policy["metrics"]["mean_executable_net_pips"]
                > 0.0
                or final_net_policy["metrics"]["mean_executable_net_pips"]
                > 0.0
            ),
        },
    }

    validation_path = run_dir / "SIMPLIFICATION_VALIDATION_CANDIDATES.csv"
    policy_path = run_dir / "SIMPLIFICATION_SIGNAL_POLICIES.csv"
    json_path = run_dir / "FORECAST_SIMPLIFICATION_REPORT.json"
    md_path = run_dir / "FORECAST_SIMPLIFICATION_REPORT.md"
    pd.DataFrame(validation_rows).drop(columns=["features"]).to_csv(
        validation_path, index=False
    )
    pd.DataFrame(policy_rows).to_csv(policy_path, index=False)
    _write_json(json_path, result)

    lines = [
        "# Forecast Simplification Report",
        "",
        f"- Verdict: **{result['verdict']}**",
        f"- Horizon: {horizon} minutes",
        f"- Rows / pairs: {len(frame):,} / {pair_count}",
        f"- Source candidates: {len(validation_rows)}",
        "- Selection: nested chronological validation only; final labels were not used",
        "- Deployment: none; research only",
        "",
        "## Validation Selection",
        "",
        f"- Accuracy winner: `{accuracy_winner['candidate']}` "
        f"({accuracy_winner['feature_count']} features), "
        f"{100.0 * accuracy_winner['signal_direction_accuracy']:.3f}% direction, "
        f"rank IC {accuracy_winner['signal_rank_ic']:.4f}",
        f"- Compact winner: `{compact_name}` "
        f"({compact_winner['feature_count']} features), "
        f"{100.0 * compact_winner['signal_direction_accuracy']:.3f}% direction, "
        f"rank IC {compact_winner['signal_rank_ic']:.4f}",
        "",
        "## Historical Final",
        "",
        f"- Compact full coverage: "
        f"{100.0 * compact_final['signals']['direction_accuracy']:.3f}% direction, "
        f"rank IC {compact_final['signals']['rank_ic']:.4f}, "
        f"{compact_final['signals']['mean_executable_net_pips']:.3f} "
        "mean executable pips",
        f"- Accuracy-gated: "
        f"{100.0 * final_accuracy_policy['metrics']['direction_accuracy']:.3f}% "
        f"direction on {final_accuracy_policy['metrics']['signals']:,} signals, "
        f"{final_accuracy_policy['metrics']['mean_executable_net_pips']:.3f} "
        "mean executable pips",
        f"- Net-selected gate: "
        f"{100.0 * final_net_policy['metrics']['direction_accuracy']:.3f}% "
        f"direction on {final_net_policy['metrics']['signals']:,} signals, "
        f"{final_net_policy['metrics']['mean_executable_net_pips']:.3f} "
        "mean executable pips",
        "",
        "## Conclusion",
        "",
        (
            "- The forecast can be simplified while retaining positive final "
            "directional signal."
            if result["interpretation"]["simplification_succeeded"]
            else "- Simplification did not preserve a credible final direction edge."
        ),
        (
            "- A validation-selected policy was positive after executable costs."
            if result["interpretation"]["any_selected_policy_positive_after_cost"]
            else "- No validation-selected policy was positive after executable costs."
        ),
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[forecast-simplification] report={md_path}", flush=True)
    return md_path
