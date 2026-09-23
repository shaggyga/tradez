#!/usr/bin/env python3
"""Compare causal short-horizon FX models on executable and path-aware outcomes."""

from __future__ import annotations

import argparse
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import joblib
import numpy as np
import pyarrow.parquet as pq

try:
    from oanda_second_forecast import FEATURE_NAMES, atomic_json
    from oanda_second_forecast_fit import (
        DEFAULT_SOURCE,
        LIQUID_PAIRS,
        column_numpy,
        feature_matrix,
        ridge_fit,
        selected_paths,
        signal_metrics,
    )
except ModuleNotFoundError:
    from trad.oanda_second_forecast import FEATURE_NAMES, atomic_json
    from trad.oanda_second_forecast_fit import (
        DEFAULT_SOURCE,
        LIQUID_PAIRS,
        column_numpy,
        feature_matrix,
        ridge_fit,
        selected_paths,
        signal_metrics,
    )


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_REPORT = DATA_ROOT / "reports" / "short_horizon_model_sweep_v1.json"
DEFAULT_ARTIFACT = DATA_ROOT / "state" / "short_horizon_shadow_models_v1.joblib"
DEFAULT_HORIZONS = (30, 60, 120, 180, 300, 600, 900, 1800)
PROFILE_QUANTILES = (0.70, 0.85, 0.95)
FAMILIES = (
    "ridge_base",
    "ridge_interactions",
    "cluster_ridge",
    "hgb_squared",
    "hgb_absolute",
)
INTERACTION_NAMES = (
    "return_5_over_volatility_30",
    "return_10_over_volatility_30",
    "return_30_over_volatility_60",
    "return_60_over_volatility_60",
    "return_5_x_return_30",
    "return_10_x_return_60",
    "acceleration_x_volatility_30",
    "volatility_ratio_30_60",
    "range_over_volatility_60",
    "spread_x_volatility_30",
    "spread_x_range_30",
    "spread_ratio_x_activity_ratio",
    "return_30_x_activity_ratio",
    "return_60_x_activity_ratio",
    "return_30_x_hour_sin",
    "return_30_x_hour_cos",
)


def parse_horizons(value: str) -> list[int]:
    horizons = sorted({int(item) for item in value.replace(",", " ").split()})
    if not horizons or any(item <= 0 or item % 5 for item in horizons):
        raise argparse.ArgumentTypeError("horizons must be positive multiples of five seconds")
    return horizons


def parse_families(value: str) -> list[str]:
    families = [item.strip() for item in value.split(",") if item.strip()]
    unknown = sorted(set(families).difference(FAMILIES))
    if not families or unknown:
        raise argparse.ArgumentTypeError(
            "families must be selected from " + ",".join(FAMILIES)
        )
    return list(dict.fromkeys(families))


def interaction_matrix(base: np.ndarray) -> np.ndarray:
    """Append bounded nonlinear terms that are cheap enough for live inference."""

    x = np.asarray(base, dtype=float)
    if x.ndim != 2 or x.shape[1] != len(FEATURE_NAMES):
        raise ValueError("base feature matrix does not match the S1/S5 feature schema")
    r5, r10, r30, r60 = (x[:, index] for index in range(4))
    acceleration = x[:, 4]
    vol30 = np.maximum(np.abs(x[:, 5]), 0.05)
    vol60 = np.maximum(np.abs(x[:, 6]), 0.05)
    range30 = np.maximum(np.abs(x[:, 7]), 0.05)
    spread = np.maximum(x[:, 8], 0.0)
    spread_ratio = x[:, 9]
    activity_ratio = x[:, 11]
    hour_sin = x[:, 12]
    hour_cos = x[:, 13]
    terms = np.column_stack(
        (
            r5 / vol30,
            r10 / vol30,
            r30 / vol60,
            r60 / vol60,
            np.tanh(r5 / vol30) * np.tanh(r30 / vol60),
            np.tanh(r10 / vol30) * np.tanh(r60 / vol60),
            acceleration * np.minimum(vol30, 20.0),
            vol30 / vol60,
            range30 / vol60,
            spread * np.minimum(vol30, 20.0),
            spread * np.minimum(range30, 50.0),
            spread_ratio * activity_ratio,
            r30 * activity_ratio,
            r60 * activity_ratio,
            r30 * hour_sin,
            r30 * hour_cos,
        )
    )
    terms = np.nan_to_num(terms, nan=0.0, posinf=100.0, neginf=-100.0)
    terms = np.clip(terms, -100.0, 100.0)
    return np.column_stack((x, terms))


def purged_slices(origin_times: np.ndarray, horizon_sec: int) -> tuple[slice, slice, slice] | None:
    first = int(len(origin_times) * 0.60)
    second = int(len(origin_times) * 0.80)
    if first <= 0 or second <= first or second >= len(origin_times):
        return None
    purge_ns = int((horizon_sec + 7.0) * 1_000_000_000)
    train_stop = min(
        first,
        int(np.searchsorted(origin_times, origin_times[first] - purge_ns, side="right")),
    )
    validation_stop = min(
        second,
        int(np.searchsorted(origin_times, origin_times[second] - purge_ns, side="right")),
    )
    if train_stop < 200 or validation_stop - first < 50 or len(origin_times) - second < 50:
        return None
    return slice(0, train_stop), slice(first, validation_stop), slice(second, len(origin_times))


def ridge_predictions(
    train_x: np.ndarray,
    train_y: np.ndarray,
    validation_x: np.ndarray,
    validation_y: np.ndarray,
    holdout_x: np.ndarray,
) -> tuple[Any, np.ndarray, np.ndarray]:
    model = ridge_fit(train_x, train_y, validation_x, validation_y)
    validation = np.asarray(model["validation_prediction"], dtype=float)
    normalized_holdout = (holdout_x - model["means"]) / model["scales"]
    holdout = model["intercept"] + normalized_holdout @ model["coefficients"]
    serializable = {
        key: value
        for key, value in model.items()
        if key not in {"validation_prediction"}
    }
    return serializable, validation, np.asarray(holdout, dtype=float)


def hgb_predictions(
    train_x: np.ndarray,
    train_y: np.ndarray,
    validation_x: np.ndarray,
    validation_y: np.ndarray,
    holdout_x: np.ndarray,
    *,
    loss: str,
) -> tuple[Any, np.ndarray, np.ndarray]:
    from sklearn.ensemble import HistGradientBoostingRegressor

    model = HistGradientBoostingRegressor(
        loss=loss,
        learning_rate=0.05,
        max_iter=180,
        max_leaf_nodes=15,
        min_samples_leaf=80,
        l2_regularization=8.0,
        early_stopping=True,
        validation_fraction=None,
        n_iter_no_change=15,
        random_state=42,
    )
    model.fit(train_x, train_y, X_val=validation_x, y_val=validation_y)
    return model, model.predict(validation_x), model.predict(holdout_x)


def cluster_ridge_predictions(
    train_x: np.ndarray,
    train_y: np.ndarray,
    validation_x: np.ndarray,
    validation_y: np.ndarray,
    holdout_x: np.ndarray,
) -> tuple[Any, np.ndarray, np.ndarray]:
    from sklearn.cluster import MiniBatchKMeans

    means = np.mean(train_x, axis=0)
    scales = np.where(np.std(train_x, axis=0) < 1e-9, 1.0, np.std(train_x, axis=0))
    train_normalized = (train_x - means) / scales
    validation_normalized = (validation_x - means) / scales
    holdout_normalized = (holdout_x - means) / scales
    cluster = MiniBatchKMeans(
        n_clusters=3,
        batch_size=2048,
        n_init=5,
        random_state=42,
    ).fit(train_normalized)
    train_labels = cluster.labels_
    validation_labels = cluster.predict(validation_normalized)
    holdout_labels = cluster.predict(holdout_normalized)
    fallback, fallback_validation, fallback_holdout = ridge_predictions(
        train_x,
        train_y,
        validation_x,
        validation_y,
        holdout_x,
    )
    validation_prediction = fallback_validation.copy()
    holdout_prediction = fallback_holdout.copy()
    models: dict[int, Any] = {}
    for label in range(cluster.n_clusters):
        train_mask = train_labels == label
        validation_mask = validation_labels == label
        holdout_mask = holdout_labels == label
        if np.sum(train_mask) < 200 or np.sum(validation_mask) < 30:
            continue
        fitted, local_validation, local_holdout = ridge_predictions(
            train_x[train_mask],
            train_y[train_mask],
            validation_x[validation_mask],
            validation_y[validation_mask],
            holdout_x[holdout_mask],
        )
        models[label] = fitted
        validation_prediction[validation_mask] = local_validation
        holdout_prediction[holdout_mask] = local_holdout
    artifact = {
        "means": means,
        "scales": scales,
        "cluster": cluster,
        "models": models,
        "fallback": fallback,
    }
    return artifact, validation_prediction, holdout_prediction


def selection_score(metrics: dict[str, Any]) -> float:
    count = int(metrics.get("n") or 0)
    if count < 30:
        return -999.0 + count / 1000.0
    lower = max(-20.0, min(20.0, float(metrics["lower_95_pips"])))
    average = max(-20.0, min(20.0, float(metrics["avg_net_pips"])))
    return 0.70 * lower + 0.30 * average


def select_orientation_and_threshold(
    prediction: np.ndarray,
    actual: np.ndarray,
    entry_bid: np.ndarray,
    entry_ask: np.ndarray,
    future_bid: np.ndarray,
    future_ask: np.ndarray,
    multiplier: float,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for orientation in (1.0, -1.0):
        oriented = prediction * orientation
        absolute = np.abs(oriented)
        for quantile in PROFILE_QUANTILES:
            threshold = float(np.quantile(absolute, quantile))
            metrics = signal_metrics(
                oriented,
                actual,
                entry_bid,
                entry_ask,
                future_bid,
                future_ask,
                multiplier,
                threshold,
            )
            candidates.append(
                {
                    "orientation": orientation,
                    "quantile": quantile,
                    "threshold": threshold,
                    "score": selection_score(metrics),
                    "metrics": metrics,
                }
            )
    return max(candidates, key=lambda row: (row["score"], int(row["metrics"].get("n") or 0)))


def excursion_metrics(
    prediction: np.ndarray,
    threshold: float,
    origins: np.ndarray,
    futures: np.ndarray,
    entry_bid: np.ndarray,
    entry_ask: np.ndarray,
    future_bid: np.ndarray,
    future_ask: np.ndarray,
    bid_high: np.ndarray,
    bid_low: np.ndarray,
    ask_high: np.ndarray,
    ask_low: np.ndarray,
    multiplier: float,
) -> dict[str, Any]:
    selected_positions = np.flatnonzero(np.abs(prediction) >= threshold)
    if not len(selected_positions):
        return {
            "n": 0,
            "avg_mfe_pips": 0.0,
            "avg_mae_pips": 0.0,
            "positive_mfe_rate": 0.0,
            "mfe_ge_1_rate": 0.0,
            "mfe_ge_2_rate": 0.0,
            "ended_nonpositive_after_positive_rate": 0.0,
        }
    mfe: list[float] = []
    mae: list[float] = []
    endpoint: list[float] = []
    for position in selected_positions:
        origin = int(origins[position])
        future = int(futures[position])
        path = slice(origin + 1, future + 1)
        if prediction[position] >= 0.0:
            mfe.append((float(np.max(bid_high[path])) - entry_ask[position]) * multiplier)
            mae.append((entry_ask[position] - float(np.min(bid_low[path]))) * multiplier)
            endpoint.append((future_bid[position] - entry_ask[position]) * multiplier)
        else:
            mfe.append((entry_bid[position] - float(np.min(ask_low[path]))) * multiplier)
            mae.append((float(np.max(ask_high[path])) - entry_bid[position]) * multiplier)
            endpoint.append((entry_bid[position] - future_ask[position]) * multiplier)
    mfe_array = np.maximum(0.0, np.asarray(mfe, dtype=float))
    mae_array = np.maximum(0.0, np.asarray(mae, dtype=float))
    endpoint_array = np.asarray(endpoint, dtype=float)
    ended_nonpositive = endpoint_array <= 0.0
    had_positive = mfe_array > 0.0
    return {
        "n": len(selected_positions),
        "avg_mfe_pips": float(np.mean(mfe_array)),
        "median_mfe_pips": float(np.median(mfe_array)),
        "avg_mae_pips": float(np.mean(mae_array)),
        "positive_mfe_rate": float(np.mean(had_positive) * 100.0),
        "mfe_ge_1_rate": float(np.mean(mfe_array >= 1.0) * 100.0),
        "mfe_ge_2_rate": float(np.mean(mfe_array >= 2.0) * 100.0),
        "ended_nonpositive_after_positive_rate": float(
            np.mean(ended_nonpositive & had_positive) * 100.0
        ),
    }


def compact_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        key: round(value, 6) if isinstance(value, float) and math.isfinite(value) else value
        for key, value in metrics.items()
    }


def fit_family(
    family: str,
    train_base: np.ndarray,
    train_y: np.ndarray,
    validation_base: np.ndarray,
    validation_y: np.ndarray,
    holdout_base: np.ndarray,
) -> tuple[Any, np.ndarray, np.ndarray]:
    if family == "ridge_base":
        return ridge_predictions(
            train_base,
            train_y,
            validation_base,
            validation_y,
            holdout_base,
        )
    train = interaction_matrix(train_base)
    validation = interaction_matrix(validation_base)
    holdout = interaction_matrix(holdout_base)
    if family == "ridge_interactions":
        return ridge_predictions(train, train_y, validation, validation_y, holdout)
    if family == "cluster_ridge":
        return cluster_ridge_predictions(train, train_y, validation, validation_y, holdout)
    if family == "hgb_squared":
        return hgb_predictions(
            train,
            train_y,
            validation,
            validation_y,
            holdout,
            loss="squared_error",
        )
    if family == "hgb_absolute":
        return hgb_predictions(
            train,
            train_y,
            validation,
            validation_y,
            holdout,
            loss="absolute_error",
        )
    raise ValueError(f"unsupported family: {family}")


def fit_pair_horizon(
    path: Path,
    dataset: dict[str, Any],
    path_prices: dict[str, np.ndarray],
    horizon_sec: int,
    families: list[str],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    source_indices = dataset["indices"]
    times = dataset["times"]
    target_ns = times[source_indices] + int(horizon_sec * 1_000_000_000)
    futures = np.searchsorted(times, target_ns, side="left")
    in_bounds = futures < len(times)
    origins = source_indices[in_bounds]
    futures = futures[in_bounds]
    delay_ns = times[futures] - target_ns[in_bounds]
    valid = (delay_ns >= 0) & (delay_ns <= 7_000_000_000)
    origins = origins[valid]
    futures = futures[valid]
    base = dataset["features"][in_bounds][valid]
    if len(base) < 400:
        return None
    actual = (dataset["mid"][futures] - dataset["mid"][origins]) * dataset["multiplier"]
    slices = purged_slices(times[origins], horizon_sec)
    if slices is None:
        return None
    train_slice, validation_slice, holdout_slice = slices
    artifacts: dict[str, Any] = {}
    results: list[dict[str, Any]] = []
    for family in families:
        started = time.monotonic()
        model, validation_prediction, holdout_prediction = fit_family(
            family,
            base[train_slice],
            actual[train_slice],
            base[validation_slice],
            actual[validation_slice],
            base[holdout_slice],
        )
        selection = select_orientation_and_threshold(
            validation_prediction,
            actual[validation_slice],
            dataset["bid"][origins[validation_slice]],
            dataset["ask"][origins[validation_slice]],
            dataset["bid"][futures[validation_slice]],
            dataset["ask"][futures[validation_slice]],
            dataset["multiplier"],
        )
        oriented_holdout = holdout_prediction * float(selection["orientation"])
        holdout_metrics = signal_metrics(
            oriented_holdout,
            actual[holdout_slice],
            dataset["bid"][origins[holdout_slice]],
            dataset["ask"][origins[holdout_slice]],
            dataset["bid"][futures[holdout_slice]],
            dataset["ask"][futures[holdout_slice]],
            dataset["multiplier"],
            float(selection["threshold"]),
        )
        path_metrics = excursion_metrics(
            oriented_holdout,
            float(selection["threshold"]),
            origins[holdout_slice],
            futures[holdout_slice],
            dataset["bid"][origins[holdout_slice]],
            dataset["ask"][origins[holdout_slice]],
            dataset["bid"][futures[holdout_slice]],
            dataset["ask"][futures[holdout_slice]],
            path_prices["bid_high"],
            path_prices["bid_low"],
            path_prices["ask_high"],
            path_prices["ask_low"],
            dataset["multiplier"],
        )
        artifacts[family] = {
            "model": model,
            "orientation": float(selection["orientation"]),
            "threshold": float(selection["threshold"]),
            "quantile": float(selection["quantile"]),
            "base_feature_names": list(FEATURE_NAMES),
            "interaction_feature_names": list(INTERACTION_NAMES) if family != "ridge_base" else [],
        }
        results.append(
            {
                "family": family,
                "selected_on": "middle chronological 20% after executable bid/ask costs",
                "evaluated_on": "newest chronological 20% untouched holdout",
                "orientation": "direct" if selection["orientation"] > 0 else "inverse",
                "quantile": selection["quantile"],
                "threshold": selection["threshold"],
                "validation_score": selection["score"],
                "validation": compact_metrics(selection["metrics"]),
                "holdout": compact_metrics(holdout_metrics),
                "path": compact_metrics(path_metrics),
                "historical_gate_passed": bool(
                    int(holdout_metrics.get("n") or 0) >= 100
                    and float(holdout_metrics.get("avg_net_pips") or 0.0) > 0.0
                    and float(holdout_metrics.get("lower_95_pips") or -999.0) > 0.0
                    and float(holdout_metrics.get("win_rate") or 0.0) >= 52.0
                ),
                "fit_elapsed_sec": round(time.monotonic() - started, 3),
            }
        )
    winner = max(results, key=lambda row: (float(row["validation_score"]), row["family"] == "ridge_base"))
    baseline = next((row for row in results if row["family"] == "ridge_base"), None)
    return (
        {
            "instrument": path.stem.removesuffix("_S5"),
            "horizon_sec": horizon_sec,
            "rows": len(base),
            "train_rows": train_slice.stop - train_slice.start,
            "validation_rows": validation_slice.stop - validation_slice.start,
            "holdout_rows": holdout_slice.stop - holdout_slice.start,
            "selection_winner": winner["family"],
            "winner_holdout_avg_net_pips": winner["holdout"].get("avg_net_pips"),
            "winner_holdout_win_rate": winner["holdout"].get("win_rate"),
            "winner_holdout_lower_95_pips": winner["holdout"].get("lower_95_pips"),
            "winner_historical_gate_passed": winner["historical_gate_passed"],
            "winner_beats_ridge_on_holdout": bool(
                baseline is not None
                and float(winner["holdout"].get("avg_net_pips") or -999.0)
                > float(baseline["holdout"].get("avg_net_pips") or -999.0)
            ),
            "models": results,
        },
        artifacts,
    )


def path_price_arrays(path: Path) -> dict[str, np.ndarray]:
    table = pq.read_table(path, columns=["bid_high", "bid_low", "ask_high", "ask_low"])
    return {
        name: column_numpy(table, name)
        for name in ("bid_high", "bid_low", "ask_high", "ask_low")
    }


def aggregate_report(surfaces: list[dict[str, Any]], families: list[str]) -> dict[str, Any]:
    family_rows: list[dict[str, Any]] = []
    for family in families:
        rows = [model for surface in surfaces for model in surface["models"] if model["family"] == family]
        holdout_n = sum(int(row["holdout"].get("n") or 0) for row in rows)
        weighted_net = sum(
            float(row["holdout"].get("avg_net_pips") or 0.0) * int(row["holdout"].get("n") or 0)
            for row in rows
        )
        weighted_win = sum(
            float(row["holdout"].get("win_rate") or 0.0) * int(row["holdout"].get("n") or 0)
            for row in rows
        )
        family_rows.append(
            {
                "family": family,
                "surfaces": len(rows),
                "holdout_n": holdout_n,
                "weighted_holdout_avg_net_pips": weighted_net / holdout_n if holdout_n else 0.0,
                "weighted_holdout_win_rate": weighted_win / holdout_n if holdout_n else 0.0,
                "historical_gate_passes": sum(bool(row["historical_gate_passed"]) for row in rows),
                "validation_wins": sum(surface["selection_winner"] == family for surface in surfaces),
                "median_fit_elapsed_sec": statistics.median(
                    float(row["fit_elapsed_sec"]) for row in rows
                ) if rows else 0.0,
            }
        )
    return {
        "families": family_rows,
        "surface_count": len(surfaces),
        "winner_gate_passes": sum(bool(row["winner_historical_gate_passed"]) for row in surfaces),
        "winner_holdout_improvements_over_ridge": sum(
            bool(row["winner_beats_ridge_on_holdout"]) for row in surfaces
        ),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    paths = selected_paths(args.source, args.pairs)
    if args.max_pairs:
        paths = paths[: args.max_pairs]
    surfaces: list[dict[str, Any]] = []
    artifact_models: dict[str, dict[int, Any]] = {}
    errors: dict[str, str] = {}
    for number, path in enumerate(paths, 1):
        instrument = path.stem.removesuffix("_S5")
        try:
            dataset = feature_matrix(path, args.sample_step, max(args.horizons))
            paths_by_price = path_price_arrays(path)
            for horizon in args.horizons:
                result = fit_pair_horizon(
                    path,
                    dataset,
                    paths_by_price,
                    horizon,
                    args.families,
                )
                if result is None:
                    continue
                surface, artifacts = result
                surfaces.append(surface)
                artifact_models.setdefault(instrument, {})[horizon] = artifacts
                print(
                    f"[short-sweep] {number}/{len(paths)} {instrument} h={horizon}: "
                    f"winner={surface['selection_winner']} "
                    f"holdout_net={surface['winner_holdout_avg_net_pips']:.4f} "
                    f"win={surface['winner_holdout_win_rate']:.2f}%",
                    flush=True,
                )
        except Exception as error:
            errors[instrument] = repr(error)
            print(f"[short-sweep] {instrument}: {error!r}", flush=True)
    aggregate = aggregate_report(surfaces, args.families)
    report = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "shadow_only_no_order_execution",
        "source": str(args.source.resolve()),
        "pairs_requested": [path.stem.removesuffix("_S5") for path in paths],
        "horizons_sec": list(args.horizons),
        "families_requested": list(args.families),
        "base_feature_names": list(FEATURE_NAMES),
        "interaction_feature_names": list(INTERACTION_NAMES),
        "sample_step_bars": args.sample_step,
        "selection_split": "middle chronological 20% with horizon purge",
        "evaluation_split": "newest chronological 20% untouched holdout",
        "cost_model": "executable entry ask/bid to future bid/ask",
        "path_model": "future executable bid/ask candle extrema after entry",
        "aggregate": aggregate,
        "surfaces": surfaces,
        "errors": errors,
        "elapsed_sec": round(time.monotonic() - started, 3),
    }
    atomic_json(args.output, report)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.artifact.with_suffix(args.artifact.suffix + ".tmp")
    joblib.dump(
        {
            "schema_version": 1,
            "generated_utc": report["generated_utc"],
            "mode": report["mode"],
            "base_feature_names": list(FEATURE_NAMES),
            "interaction_feature_names": list(INTERACTION_NAMES),
            "horizons_sec": list(args.horizons),
            "models": artifact_models,
            "report_path": str(args.output.resolve()),
        },
        temporary,
        compress=3,
    )
    temporary.replace(args.artifact)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--pairs", default="liquid")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--horizons", type=parse_horizons, default=list(DEFAULT_HORIZONS))
    parser.add_argument("--families", type=parse_families, default=list(FAMILIES))
    parser.add_argument("--sample-step", type=int, default=4)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_ARTIFACT)
    args = parser.parse_args(argv)
    if args.sample_step <= 0 or args.max_pairs < 0:
        raise SystemExit("sample step must be positive and max pairs must be nonnegative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = run(args)
    print(
        {
            "report": str(args.output.resolve()),
            "artifact": str(args.artifact.resolve()),
            "elapsed_sec": report["elapsed_sec"],
            **report["aggregate"],
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
