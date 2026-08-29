#!/usr/bin/env python3
"""Fit a leakage-safe SMA meta-filter on real shadow signal candidates."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from oanda_sma_signal_filter import (
        SMA_FEATURE_NAMES,
        TIMEFRAME_MINUTES,
        TIMEFRAME_PERIODS,
        TIMEFRAME_SERIES_KEYS,
        build_sma_signal_vector,
    )
except ImportError:  # pragma: no cover - package execution.
    from trad.oanda_sma_signal_filter import (
        SMA_FEATURE_NAMES,
        TIMEFRAME_MINUTES,
        TIMEFRAME_PERIODS,
        TIMEFRAME_SERIES_KEYS,
        build_sma_signal_vector,
    )


ROOT = Path(__file__).resolve().parent
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_OUTCOME_DATABASE = STATE_ROOT / "strategy_shadow_outcomes_v1.sqlite"
DEFAULT_CANDLE_DIR = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "candles_m1_parquet_recovered_20260719"
)
DEFAULT_REPORT_ROOT = (
    ROOT / "data" / "oanda_training_manager" / "reports" / "sma_signal_filter"
)
DEFAULT_ARTIFACT = STATE_ROOT / "sma_signal_filter_v1.joblib"
DEFAULT_STATE = STATE_ROOT / "sma_signal_filter_v1.json"
DEFAULT_HORIZONS = (60, 300, 900, 3600)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric if math.isfinite(numeric) else default


def parse_ints(value: str) -> tuple[int, ...]:
    output = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not output or any(item <= 0 for item in output):
        raise argparse.ArgumentTypeError("values must be positive comma-separated integers")
    return output


def infer_pip_size(instrument: str) -> float:
    return 0.01 if str(instrument).upper().endswith("_JPY") else 0.0001


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def discover_instruments(candle_dir: Path, requested: str, max_pairs: int) -> list[str]:
    available = sorted(
        path.name[: -len("_M1.parquet")]
        for path in Path(candle_dir).glob("*_M1.parquet")
    )
    if requested.strip():
        selected = {
            item.strip().upper().replace("/", "_")
            for item in requested.split(",")
            if item.strip()
        }
        available = [instrument for instrument in available if instrument in selected]
    return available[:max_pairs] if max_pairs > 0 else available


def load_outcomes(
    database_path: Path,
    horizons: Iterable[int],
    instruments: Iterable[str],
    kinds: Iterable[str],
    max_rows: int,
    end_time: pd.Timestamp | None = None,
) -> pd.DataFrame:
    horizon_values = tuple(sorted({int(value) for value in horizons}))
    instrument_values = tuple(sorted({str(value) for value in instruments}))
    kind_values = tuple(sorted({str(value) for value in kinds}))
    if not horizon_values or not instrument_values or not kind_values:
        return pd.DataFrame()
    horizon_marks = ",".join("?" for _ in horizon_values)
    instrument_marks = ",".join("?" for _ in instrument_values)
    kind_marks = ",".join("?" for _ in kind_values)
    limit = f" LIMIT {int(max_rows)}" if max_rows > 0 else ""
    end_clause = " AND entry_time <= ?" if end_time is not None else ""
    query = f"""
        SELECT event_id, horizon_sec, lane_id, family, profile, kind,
               instrument, direction, entry_time, theoretical_pips,
               entry_spread_pips, max_favorable_pips, max_adverse_pips,
               pip
        FROM outcomes
        WHERE horizon_sec IN ({horizon_marks})
          AND instrument IN ({instrument_marks})
          AND kind IN ({kind_marks})
          {end_clause}
        ORDER BY entry_time, row_id
        {limit}
    """
    params: tuple[Any, ...] = (*horizon_values, *instrument_values, *kind_values)
    if end_time is not None:
        params = (
            *params,
            pd.Timestamp(end_time).isoformat().replace("+00:00", "Z"),
        )
    connection = sqlite3.connect(f"file:{Path(database_path)}?mode=ro", uri=True)
    try:
        frame = pd.read_sql_query(
            query,
            connection,
            params=params,
        )
    finally:
        connection.close()
    if frame.empty:
        return frame
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["entry_time"])
    frame["direction"] = frame["direction"].astype(str).str.lower()
    frame = frame[frame["direction"].isin(("buy", "sell"))]
    frame["pip"] = pd.to_numeric(frame["pip"], errors="coerce")
    frame["pip"] = frame.apply(
        lambda row: (
            finite(row["pip"])
            if finite(row["pip"]) > 0.0
            else infer_pip_size(str(row["instrument"]))
        ),
        axis=1,
    )
    frame["entry_minute"] = frame["entry_time"].dt.floor("min")
    frame["snapshot_key"] = (
        frame["instrument"].astype(str)
        + "|"
        + frame["entry_minute"].astype(str)
        + "|"
        + frame["direction"].astype(str)
    )
    return frame.reset_index(drop=True)


def candle_panel_latest_end(
    candle_dir: Path,
    instruments: Iterable[str],
) -> pd.Timestamp | None:
    maxima: list[pd.Timestamp] = []
    for instrument in instruments:
        path = Path(candle_dir) / f"{instrument}_M1.parquet"
        if not path.is_file():
            continue
        try:
            parquet = pq.ParquetFile(path)
            schema_names = parquet.schema.names
            time_index = schema_names.index("time")
            maximum: str | None = None
            for index in range(parquet.num_row_groups):
                statistics_row = parquet.metadata.row_group(index).column(
                    time_index
                ).statistics
                if statistics_row is not None and statistics_row.has_min_max:
                    value = statistics_row.max
                    maximum = str(value) if maximum is None else max(maximum, str(value))
            parsed = pd.to_datetime(maximum, utc=True, errors="coerce")
        except (OSError, ValueError):
            continue
        if not pd.isna(parsed):
            maxima.append(pd.Timestamp(parsed))
    return max(maxima) if maxima else None


def completed_close_series(
    m1_frame: pd.DataFrame,
    minutes: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return close values and their first availability timestamps."""

    if minutes == 1:
        values = m1_frame["close"].to_numpy(dtype=np.float64)
        available = (
            m1_frame["time"].astype("int64").to_numpy()
            + 60 * 1_000_000_000
        )
        return values, available
    indexed = m1_frame.set_index("time")["close"]
    close = indexed.resample(
        f"{int(minutes)}min",
        origin="epoch",
        label="left",
        closed="left",
    ).last()
    close = close.dropna()
    available = (
        close.index.astype("int64").to_numpy()
        + int(minutes) * 60 * 1_000_000_000
    )
    return close.to_numpy(dtype=np.float64), available


def load_candle_frame(
    path: Path,
    minimum_time: pd.Timestamp,
    maximum_time: pd.Timestamp,
    lookback_days: int,
) -> pd.DataFrame:
    frame = pd.read_parquet(path, columns=["time", "close"])
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["time", "close"]).sort_values("time")
    start = pd.Timestamp(minimum_time) - pd.DateOffset(
        days=max(7, int(lookback_days))
    )
    end = maximum_time.ceil("min")
    return frame[(frame["time"] >= start) & (frame["time"] <= end)].reset_index(drop=True)


def snapshot_feature_matrix(
    outcomes: pd.DataFrame,
    candle_dir: Path,
    *,
    lookback_days: int,
) -> tuple[pd.DataFrame, np.ndarray, dict[str, Any]]:
    snapshots = (
        outcomes.groupby("snapshot_key", sort=False)
        .agg(
            instrument=("instrument", "first"),
            entry_time=("entry_minute", "first"),
            direction=("direction", "first"),
            pip=("pip", "median"),
            spread_pips=("entry_spread_pips", "median"),
        )
        .reset_index()
    )
    snapshots["feature_row"] = np.arange(len(snapshots), dtype=np.int64)
    feature_index = {name: index for index, name in enumerate(SMA_FEATURE_NAMES)}
    matrix = np.full(
        (len(snapshots), len(SMA_FEATURE_NAMES)),
        np.nan,
        dtype=np.float32,
    )
    pair_summary: dict[str, Any] = {}

    for instrument, group in snapshots.groupby("instrument", sort=True):
        path = Path(candle_dir) / f"{instrument}_M1.parquet"
        if not path.is_file():
            pair_summary[str(instrument)] = {"status": "missing_candles", "snapshots": len(group)}
            continue
        minimum_time = group["entry_time"].min()
        maximum_time = group["entry_time"].max()
        candle_frame = load_candle_frame(
            path,
            minimum_time,
            maximum_time,
            lookback_days,
        )
        if candle_frame.empty:
            pair_summary[str(instrument)] = {"status": "empty_candles", "snapshots": len(group)}
            continue
        series: dict[str, tuple[np.ndarray, np.ndarray]] = {
            timeframe: completed_close_series(candle_frame, minutes)
            for timeframe, minutes in TIMEFRAME_MINUTES.items()
        }
        populated = 0
        for snapshot in group.itertuples(index=False):
            entry_ns = pd.Timestamp(snapshot.entry_time).value
            _, m1_available_ns = series["M1"]
            m1_position = int(
                np.searchsorted(m1_available_ns, entry_ns, side="right") - 1
            )
            if (
                m1_position < 0
                or entry_ns - int(m1_available_ns[m1_position])
                > 180 * 1_000_000_000
            ):
                continue
            live_features: dict[str, Any] = {
                "pip": finite(snapshot.pip, infer_pip_size(instrument)),
            }
            enough = False
            for timeframe, series_key in TIMEFRAME_SERIES_KEYS.items():
                values, available_ns = series[timeframe]
                position = int(np.searchsorted(available_ns, entry_ns, side="right") - 1)
                maximum_age_ns = (
                    TIMEFRAME_MINUTES[timeframe] * 60 + 180
                ) * 1_000_000_000
                if (
                    position < 0
                    or entry_ns - int(available_ns[position]) > maximum_age_ns
                ):
                    live_features[series_key] = []
                    continue
                history_needed = max(TIMEFRAME_PERIODS[timeframe]) + 132
                start = max(0, position - history_needed + 1)
                history = values[start : position + 1]
                live_features[series_key] = history.tolist()
                enough = enough or len(history) >= min(TIMEFRAME_PERIODS[timeframe]) + 3
            if not enough:
                continue
            vector = build_sma_signal_vector(
                live_features,
                str(snapshot.direction),
                spread_pips=finite(snapshot.spread_pips),
            )
            row_index = int(snapshot.feature_row)
            for name, value in vector.items():
                matrix[row_index, feature_index[name]] = finite(value, math.nan)
            populated += 1
        pair_summary[str(instrument)] = {
            "status": "ok",
            "snapshots": int(len(group)),
            "populated": populated,
            "candle_rows": int(len(candle_frame)),
            "candle_start": str(candle_frame["time"].min()),
            "candle_end": str(candle_frame["time"].max()),
        }
    return snapshots, matrix, pair_summary


def weighted_metrics(
    net_pips: np.ndarray,
    selected: np.ndarray,
    weights: np.ndarray,
    entry_ns: np.ndarray,
    instruments: np.ndarray,
    horizon_sec: int,
) -> dict[str, Any]:
    active = np.asarray(selected, dtype=bool)
    if not np.any(active):
        return {
            "n": 0,
            "effective_n": 0.0,
            "coverage": 0.0,
            "average_net_pips": 0.0,
            "median_net_pips": 0.0,
            "win_rate": 0.0,
            "lower_confidence_net_pips": -math.inf,
            "independent_time_blocks": 0,
            "positive_time_block_fraction": 0.0,
            "pair_count": 0,
            "positive_pair_fraction": 0.0,
        }
    values = np.asarray(net_pips, dtype=np.float64)[active]
    active_weights = np.asarray(weights, dtype=np.float64)[active]
    active_weights = np.maximum(active_weights, 1e-12)
    total_weight = float(np.sum(active_weights))
    average = float(np.dot(values, active_weights) / total_weight)
    variance = float(np.dot(np.square(values - average), active_weights) / total_weight)
    effective_n = float(total_weight * total_weight / np.sum(np.square(active_weights)))
    lower = average - 1.645 * math.sqrt(max(0.0, variance) / max(1.0, effective_n))
    block_width_ns = max(1, int(horizon_sec)) * 1_000_000_000
    blocks = np.asarray(entry_ns, dtype=np.int64)[active] // block_width_ns
    active_instruments = np.asarray(instruments, dtype=object)[active]

    def grouped_fraction(keys: np.ndarray) -> tuple[int, float]:
        values_by_key: dict[Any, list[tuple[float, float]]] = {}
        for key, value, weight in zip(keys, values, active_weights):
            values_by_key.setdefault(key, []).append((float(value), float(weight)))
        means = [
            sum(value * weight for value, weight in group)
            / max(1e-12, sum(weight for _, weight in group))
            for group in values_by_key.values()
        ]
        return len(means), sum(value > 0.0 for value in means) / max(1, len(means))

    block_count, positive_block_fraction = grouped_fraction(blocks)
    pair_count, positive_pair_fraction = grouped_fraction(active_instruments)
    return {
        "n": int(np.sum(active)),
        "effective_n": round(effective_n, 3),
        "coverage": round(float(np.mean(active)), 6),
        "average_net_pips": round(average, 6),
        "median_net_pips": round(float(np.median(values)), 6),
        "win_rate": round(float(np.dot(values > 0.0, active_weights) / total_weight), 6),
        "total_weighted_net_pips": round(float(np.dot(values, active_weights)), 4),
        "lower_confidence_net_pips": round(lower, 6),
        "independent_time_blocks": block_count,
        "positive_time_block_fraction": round(positive_block_fraction, 6),
        "pair_count": pair_count,
        "positive_pair_fraction": round(positive_pair_fraction, 6),
    }


@dataclass
class SplitRows:
    train: np.ndarray
    validation: np.ndarray
    holdout: np.ndarray
    train_end: pd.Timestamp
    validation_end: pd.Timestamp


def chronological_split(entry_time: pd.Series, horizon_sec: int) -> SplitRows:
    ordered = np.sort(entry_time.astype("int64").unique())
    if len(ordered) < 5:
        raise ValueError("not enough unique timestamps for chronological split")
    train_cut = int(ordered[max(1, int(len(ordered) * 0.60))])
    validation_cut = int(ordered[max(2, int(len(ordered) * 0.80))])
    purge_ns = int(horizon_sec) * 1_000_000_000
    values = entry_time.astype("int64").to_numpy()
    return SplitRows(
        train=values <= train_cut - purge_ns,
        validation=(values > train_cut) & (values <= validation_cut - purge_ns),
        holdout=values > validation_cut,
        train_end=pd.Timestamp(train_cut, tz="UTC"),
        validation_end=pd.Timestamp(validation_cut, tz="UTC"),
    )


def model_candidates(random_state: int) -> list[tuple[str, str, Any]]:
    return [
        (
            "ridge_logistic",
            "probability",
            Pipeline(
                (
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scale", StandardScaler()),
                    (
                        "model",
                        LogisticRegression(
                            C=0.10,
                            max_iter=600,
                            solver="liblinear",
                            random_state=random_state,
                        ),
                    ),
                )
            ),
        ),
        (
            "hist_gradient_boosting_classifier",
            "probability",
            HistGradientBoostingClassifier(
                learning_rate=0.05,
                max_iter=160,
                max_leaf_nodes=15,
                min_samples_leaf=60,
                l2_regularization=5.0,
                random_state=random_state,
            ),
        ),
        (
            "hist_gradient_boosting_regressor",
            "net_pips",
            HistGradientBoostingRegressor(
                learning_rate=0.05,
                max_iter=160,
                max_leaf_nodes=15,
                min_samples_leaf=60,
                l2_regularization=5.0,
                loss="squared_error",
                random_state=random_state,
            ),
        ),
    ]


def estimator_scores(estimator: Any, score_kind: str, matrix: np.ndarray) -> np.ndarray:
    if score_kind == "probability":
        return np.asarray(estimator.predict_proba(matrix)[:, 1], dtype=np.float64)
    return np.asarray(estimator.predict(matrix), dtype=np.float64)


def select_threshold(
    scores: np.ndarray,
    net_pips: np.ndarray,
    weights: np.ndarray,
    entry_ns: np.ndarray,
    instruments: np.ndarray,
    horizon_sec: int,
    baseline: dict[str, Any],
    minimum_effective_samples: int,
) -> tuple[float, dict[str, Any], list[dict[str, Any]]]:
    trials: list[dict[str, Any]] = []
    for coverage in (0.05, 0.10, 0.20, 0.35, 0.50, 0.75):
        threshold = float(np.quantile(scores, 1.0 - coverage))
        selected = scores >= threshold
        metrics = weighted_metrics(
            net_pips,
            selected,
            weights,
            entry_ns,
            instruments,
            horizon_sec,
        )
        lift = metrics["average_net_pips"] - baseline["average_net_pips"]
        objective = (
            metrics["average_net_pips"] * math.sqrt(max(0.01, metrics["coverage"]))
            + 0.05 * (metrics["win_rate"] - baseline["win_rate"])
        )
        metrics.update(
            {
                "threshold": round(threshold, 8),
                "requested_coverage": coverage,
                "average_net_lift_pips": round(lift, 6),
                "selection_objective": round(objective, 8),
                "support_ok": metrics["effective_n"] >= minimum_effective_samples,
            }
        )
        trials.append(metrics)
    eligible = [
        row
        for row in trials
        if row["support_ok"] and row["average_net_lift_pips"] > 0.0
    ]
    pool = eligible or trials
    best = max(
        pool,
        key=lambda row: (
            row["selection_objective"],
            row["average_net_lift_pips"],
            row["effective_n"],
        ),
    )
    return finite(best["threshold"]), best, trials


def top_feature_correlations(
    matrix: np.ndarray,
    net_pips: np.ndarray,
    train_mask: np.ndarray,
    feature_names: tuple[str, ...],
    limit: int = 30,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    target = np.asarray(net_pips, dtype=np.float64)[train_mask]
    for index, name in enumerate(feature_names):
        values = np.asarray(matrix[train_mask, index], dtype=np.float64)
        valid = np.isfinite(values) & np.isfinite(target)
        if np.sum(valid) < 50 or float(np.nanstd(values[valid])) <= 1e-12:
            continue
        correlation = float(np.corrcoef(values[valid], target[valid])[0, 1])
        if math.isfinite(correlation):
            rows.append(
                {
                    "feature": name,
                    "correlation_with_net_pips": round(correlation, 6),
                    "n": int(np.sum(valid)),
                }
            )
    rows.sort(key=lambda row: abs(row["correlation_with_net_pips"]), reverse=True)
    return rows[:limit]


def fit_horizon(
    rows: pd.DataFrame,
    feature_matrix: np.ndarray,
    feature_names: tuple[str, ...],
    horizon_sec: int,
    *,
    minimum_effective_samples: int,
    random_state: int,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    horizon_rows = rows[rows["horizon_sec"] == int(horizon_sec)].copy()
    if len(horizon_rows) < max(300, minimum_effective_samples * 3):
        return {
            "horizon_sec": int(horizon_sec),
            "status": "insufficient_rows",
            "rows": int(len(horizon_rows)),
        }, None
    indexes = horizon_rows["feature_row"].to_numpy(dtype=np.int64)
    matrix = feature_matrix[indexes].astype(np.float64, copy=False)
    finite_fraction = np.mean(np.isfinite(matrix), axis=0)
    train_variance = np.nanvar(matrix, axis=0)
    usable = (finite_fraction >= 0.30) & np.isfinite(train_variance) & (train_variance > 1e-12)
    selected_names = tuple(name for name, keep in zip(feature_names, usable) if keep)
    matrix = matrix[:, usable]
    complete_rows = np.any(np.isfinite(matrix), axis=1)
    horizon_rows = horizon_rows.loc[complete_rows].reset_index(drop=True)
    matrix = matrix[complete_rows]
    if len(horizon_rows) < max(300, minimum_effective_samples * 3) or not matrix.shape[1]:
        return {
            "horizon_sec": int(horizon_sec),
            "status": "insufficient_feature_rows",
            "rows": int(len(horizon_rows)),
            "usable_features": int(matrix.shape[1]),
        }, None

    split = chronological_split(horizon_rows["entry_time"], int(horizon_sec))
    net = horizon_rows["theoretical_pips"].to_numpy(dtype=np.float64)
    labels = (net > 0.0).astype(np.int8)
    snapshot_counts = horizon_rows.groupby("snapshot_key")["snapshot_key"].transform("size")
    weights = 1.0 / snapshot_counts.to_numpy(dtype=np.float64)
    entry_ns = horizon_rows["entry_time"].astype("int64").to_numpy()
    instruments = horizon_rows["instrument"].astype(str).to_numpy(dtype=object)
    if min(np.sum(split.train), np.sum(split.validation), np.sum(split.holdout)) < 100:
        return {
            "horizon_sec": int(horizon_sec),
            "status": "insufficient_chronological_split",
            "rows": int(len(horizon_rows)),
            "split_rows": {
                "train": int(np.sum(split.train)),
                "validation": int(np.sum(split.validation)),
                "holdout": int(np.sum(split.holdout)),
            },
        }, None
    baseline_validation = weighted_metrics(
        net[split.validation],
        np.ones(np.sum(split.validation), dtype=bool),
        weights[split.validation],
        entry_ns[split.validation],
        instruments[split.validation],
        horizon_sec,
    )
    baseline_holdout = weighted_metrics(
        net[split.holdout],
        np.ones(np.sum(split.holdout), dtype=bool),
        weights[split.holdout],
        entry_ns[split.holdout],
        instruments[split.holdout],
        horizon_sec,
    )

    fitted: list[dict[str, Any]] = []
    for model_name, score_kind, estimator in model_candidates(random_state):
        if score_kind == "probability" and len(np.unique(labels[split.train])) < 2:
            continue
        target = labels if score_kind == "probability" else net
        fit_kwargs = {"model__sample_weight": weights[split.train]} if isinstance(
            estimator, Pipeline
        ) else {"sample_weight": weights[split.train]}
        estimator.fit(
            matrix[split.train],
            target[split.train],
            **fit_kwargs,
        )
        validation_scores = estimator_scores(
            estimator,
            score_kind,
            matrix[split.validation],
        )
        threshold, validation, trials = select_threshold(
            validation_scores,
            net[split.validation],
            weights[split.validation],
            entry_ns[split.validation],
            instruments[split.validation],
            horizon_sec,
            baseline_validation,
            minimum_effective_samples,
        )
        holdout_scores = estimator_scores(
            estimator,
            score_kind,
            matrix[split.holdout],
        )
        holdout = weighted_metrics(
            net[split.holdout],
            holdout_scores >= threshold,
            weights[split.holdout],
            entry_ns[split.holdout],
            instruments[split.holdout],
            horizon_sec,
        )
        holdout["average_net_lift_pips"] = round(
            holdout["average_net_pips"] - baseline_holdout["average_net_pips"],
            6,
        )
        fitted.append(
            {
                "model_name": model_name,
                "score_kind": score_kind,
                "estimator": estimator,
                "threshold": threshold,
                "validation": validation,
                "validation_trials": trials,
                "holdout": holdout,
            }
        )
    if not fitted:
        return {
            "horizon_sec": int(horizon_sec),
            "status": "model_fit_failed",
            "rows": int(len(horizon_rows)),
        }, None
    winner = max(
        fitted,
        key=lambda row: (
            row["validation"]["selection_objective"],
            row["holdout"]["average_net_lift_pips"],
            row["holdout"]["effective_n"],
        ),
    )
    validation = winner["validation"]
    holdout = winner["holdout"]
    checks = {
        "validation_positive_net": validation["average_net_pips"] > 0.0,
        "holdout_positive_net": holdout["average_net_pips"] > 0.0,
        "holdout_positive_lower_confidence": holdout["lower_confidence_net_pips"] > 0.0,
        "holdout_majority_win_rate": holdout["win_rate"] > 0.50,
        "holdout_lift": holdout["average_net_lift_pips"] > 0.0,
        "holdout_support": holdout["effective_n"] >= minimum_effective_samples,
        "time_replication": (
            holdout["independent_time_blocks"] >= 6
            and holdout["positive_time_block_fraction"] >= 0.60
        ),
        "pair_replication": (
            holdout["pair_count"] >= 5
            and holdout["positive_pair_fraction"] >= 0.55
        ),
    }
    account_eligible = all(checks.values())
    correlations = top_feature_correlations(
        matrix,
        net,
        split.train,
        selected_names,
    )
    summary = {
        "horizon_sec": int(horizon_sec),
        "status": "validated_filter" if account_eligible else "shadow_diagnostic",
        "rows": int(len(horizon_rows)),
        "unique_snapshots": int(horizon_rows["snapshot_key"].nunique()),
        "pairs": int(horizon_rows["instrument"].nunique()),
        "families": int(horizon_rows["family"].nunique()),
        "usable_features": len(selected_names),
        "split": {
            "method": "oldest 60% / next 20% validation / newest 20% untouched holdout",
            "purge_sec": int(horizon_sec),
            "train_end": str(split.train_end),
            "validation_end": str(split.validation_end),
            "train_rows": int(np.sum(split.train)),
            "validation_rows": int(np.sum(split.validation)),
            "holdout_rows": int(np.sum(split.holdout)),
        },
        "baseline_validation": baseline_validation,
        "baseline_holdout": baseline_holdout,
        "model_name": winner["model_name"],
        "score_kind": winner["score_kind"],
        "threshold": round(finite(winner["threshold"]), 8),
        "validation": validation,
        "holdout": holdout,
        "eligibility_checks": checks,
        "account_eligible": account_eligible,
        "top_feature_correlations": correlations,
        "model_comparison": [
            {
                "model_name": row["model_name"],
                "score_kind": row["score_kind"],
                "threshold": round(finite(row["threshold"]), 8),
                "validation": row["validation"],
                "holdout": row["holdout"],
            }
            for row in fitted
        ],
    }
    artifact = {
        "estimator": winner["estimator"],
        "feature_names": selected_names,
        "model_name": winner["model_name"],
        "score_kind": winner["score_kind"],
        "threshold": finite(winner["threshold"]),
        "account_eligible": account_eligible,
        "validation": validation,
        "holdout": holdout,
        "eligibility_checks": checks,
    }
    return summary, artifact


def markdown_report(manifest: dict[str, Any]) -> str:
    lines = [
        "# Wide SMA Signal Filter",
        "",
        "This is a candidate-signal meta-filter, not a standalone moving-average strategy.",
        "Every input is direction-conditioned and formed from bars completed before entry.",
        "",
        f"- Generated: `{manifest['generated_at']}`",
        f"- Outcome source: `{manifest['outcome_database']}`",
        f"- Candle source: `{manifest['candle_dir']}`",
        f"- Instruments: {manifest['instrument_count']}",
        f"- Outcome rows: {manifest['outcome_rows']:,}",
        f"- Unique SMA snapshots: {manifest['snapshot_count']:,}",
        f"- Feature net: {manifest['feature_count']} possible SMA fields",
        "",
        "## Horizon Results",
        "",
        "| Horizon | Status | Model | Rows | Features | Baseline avg | Filter avg | Win | Coverage |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in manifest.get("horizons") or []:
        holdout = row.get("holdout") or {}
        baseline = row.get("baseline_holdout") or {}
        lines.append(
            "| {horizon}s | {status} | {model} | {rows:,} | {features:,} | "
            "{baseline_avg:+.3f} | {filter_avg:+.3f} | {win:.1%} | {coverage:.1%} |".format(
                horizon=int(row.get("horizon_sec") or 0),
                status=row.get("status") or "",
                model=row.get("model_name") or "",
                rows=int(row.get("rows") or 0),
                features=int(row.get("usable_features") or 0),
                baseline_avg=finite(baseline.get("average_net_pips")),
                filter_avg=finite(holdout.get("average_net_pips")),
                win=finite(holdout.get("win_rate")),
                coverage=finite(holdout.get("coverage")),
            )
        )
    lines.extend(
        (
            "",
            "## Promotion Rule",
            "",
            "A horizon can influence account ranking only when its newest untouched holdout",
            "has positive average and lower-confidence net pips, majority wins, positive lift,",
            "enough effective support, and replication across independent time blocks and pairs.",
            "Otherwise the score is collected and displayed in shadow mode with neutral account weight.",
            "",
        )
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outcome-database", type=Path, default=DEFAULT_OUTCOME_DATABASE)
    parser.add_argument("--candle-dir", type=Path, default=DEFAULT_CANDLE_DIR)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--artifact", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--horizons", type=parse_ints, default=DEFAULT_HORIZONS)
    parser.add_argument("--instruments", default="")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--kinds", default="signal")
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument("--minimum-effective-samples", type=int, default=100)
    parser.add_argument("--random-state", type=int, default=20260726)
    parser.add_argument("--run-name", default="")
    args = parser.parse_args(argv)
    if (
        args.max_pairs < 0
        or args.max_rows < 0
        or args.lookback_days < 7
        or args.minimum_effective_samples <= 0
    ):
        parser.error("row limits must be non-negative and fitting limits must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    instruments = discover_instruments(
        args.candle_dir,
        args.instruments,
        args.max_pairs,
    )
    kinds = tuple(item.strip() for item in args.kinds.split(",") if item.strip())
    panel_end = candle_panel_latest_end(args.candle_dir, instruments)
    outcomes = load_outcomes(
        args.outcome_database,
        args.horizons,
        instruments,
        kinds,
        args.max_rows,
        panel_end,
    )
    if outcomes.empty:
        raise SystemExit("No matching candidate outcomes were found")
    snapshots, feature_matrix, pair_summary = snapshot_feature_matrix(
        outcomes,
        args.candle_dir,
        lookback_days=args.lookback_days,
    )
    snapshot_rows = snapshots[["snapshot_key", "feature_row"]]
    rows = outcomes.merge(snapshot_rows, on="snapshot_key", how="inner")
    available = np.any(np.isfinite(feature_matrix), axis=1)
    rows = rows[available[rows["feature_row"].to_numpy(dtype=np.int64)]].reset_index(
        drop=True
    )
    generated_at = utc_now()
    run_name = args.run_name.strip() or datetime.now(timezone.utc).strftime(
        "all68_%Y%m%dT%H%M%SZ"
    )
    output_dir = args.report_root / run_name
    output_dir.mkdir(parents=True, exist_ok=True)
    horizon_summaries: list[dict[str, Any]] = []
    fitted_models: dict[str, Any] = {}
    for horizon in args.horizons:
        summary, artifact = fit_horizon(
            rows,
            feature_matrix,
            SMA_FEATURE_NAMES,
            int(horizon),
            minimum_effective_samples=args.minimum_effective_samples,
            random_state=args.random_state,
        )
        horizon_summaries.append(summary)
        if artifact is not None:
            fitted_models[str(int(horizon))] = artifact
    artifact_payload = {
        "schema_version": 1,
        "generated_at": generated_at,
        "method": "direction-conditioned wide SMA meta-filter",
        "models": fitted_models,
    }
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact_payload, args.artifact, compress=3)
    manifest = {
        "schema_version": 1,
        "generated_at": generated_at,
        "run_type": "wide_sma_signal_meta_filter",
        "run_name": run_name,
        "outcome_database": str(args.outcome_database.resolve()),
        "candle_dir": str(args.candle_dir.resolve()),
        "latest_candle_panel_end": (
            None if panel_end is None else str(panel_end)
        ),
        "artifact": str(args.artifact.resolve()),
        "state": str(args.state.resolve()),
        "split_policy": (
            "chronological 60/20/20; train and validation ends purged by outcome "
            "horizon; threshold selected on validation only; newest 20% untouched"
        ),
        "cost_policy": "theoretical_pips uses executable bid/ask entry and exit quotes",
        "candidate_policy": {
            "kinds": kinds,
            "duplicate_weighting": (
                "events sharing instrument, completed M1 decision minute, and direction "
                "sum to one sample weight per horizon"
            ),
        },
        "instrument_count": len(instruments),
        "instruments": instruments,
        "outcome_rows": int(len(rows)),
        "snapshot_count": int(len(snapshots)),
        "populated_snapshot_count": int(np.sum(np.any(np.isfinite(feature_matrix), axis=1))),
        "feature_count": len(SMA_FEATURE_NAMES),
        "timeframes": TIMEFRAME_MINUTES,
        "periods": {key: list(values) for key, values in TIMEFRAME_PERIODS.items()},
        "pair_summary": pair_summary,
        "horizons": horizon_summaries,
        "account_eligible_horizons": [
            int(row["horizon_sec"])
            for row in horizon_summaries
            if row.get("account_eligible")
        ],
    }
    state = {
        key: value
        for key, value in manifest.items()
        if key not in {"pair_summary"}
    }
    atomic_json(args.state, state)
    atomic_json(output_dir / "manifest.json", manifest)
    (output_dir / "assessment.md").write_text(
        markdown_report(manifest),
        encoding="utf-8",
    )
    comparison_rows = []
    for row in horizon_summaries:
        for model in row.get("model_comparison") or []:
            comparison_rows.append(
                {
                    "horizon_sec": row.get("horizon_sec"),
                    "status": row.get("status"),
                    "model_name": model.get("model_name"),
                    "score_kind": model.get("score_kind"),
                    "threshold": model.get("threshold"),
                    "validation_average_net_pips": (
                        model.get("validation") or {}
                    ).get("average_net_pips"),
                    "holdout_average_net_pips": (
                        model.get("holdout") or {}
                    ).get("average_net_pips"),
                    "holdout_win_rate": (model.get("holdout") or {}).get("win_rate"),
                    "holdout_coverage": (model.get("holdout") or {}).get("coverage"),
                }
            )
    pd.DataFrame(comparison_rows).to_csv(output_dir / "model_comparison.csv", index=False)
    print(
        json.dumps(
            {
                "output_dir": str(output_dir.resolve()),
                "rows": len(rows),
                "snapshots": len(snapshots),
                "features": len(SMA_FEATURE_NAMES),
                "fitted_horizons": sorted(int(value) for value in fitted_models),
                "account_eligible_horizons": manifest["account_eligible_horizons"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
