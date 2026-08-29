#!/usr/bin/env python3
"""Canonical moving-average-only forecast feature space and live runtime.

The family intentionally excludes returns, oscillators, volume, order books,
account state, and other strategy votes. Spread is used for executable
economics, but it is not an input to the structural direction or pip models.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

try:
    import joblib
except ImportError:  # pragma: no cover - collection can run without fitting deps.
    joblib = None


SCHEMA_VERSION = 1
FAMILY = "moving_average_feature_grid"

# The historical builder uses deep M1 for minute-and-slower cells and observed
# S5 for subminute cells. All labels are retained in reports even when a source
# has insufficient history.
TIMEFRAME_SECONDS: dict[str, int] = {
    "S5": 5,
    "S10": 10,
    "S15": 15,
    "S30": 30,
    "M1": 60,
    "M2": 120,
    "M3": 180,
    "M4": 240,
    "M5": 300,
    "M6": 360,
    "M7": 420,
    "M8": 480,
    "M9": 540,
    "M10": 600,
    "M12": 720,
    "M15": 900,
    "M20": 1200,
    "M30": 1800,
    "M45": 2700,
    "H1": 3600,
    "H2": 7200,
    "H3": 10800,
    "H4": 14400,
    "H6": 21600,
    "H8": 28800,
    "H12": 43200,
    "D1": 86400,
}

# Dense near-term points preserve the requested 1-10 minute surface. Longer
# points cover the same structural model through one day.
FORECAST_HORIZONS_SEC: tuple[int, ...] = (
    5,
    10,
    15,
    30,
    60,
    120,
    180,
    240,
    300,
    360,
    420,
    480,
    540,
    600,
    900,
    1200,
    1800,
    2700,
    3600,
    7200,
    10800,
    14400,
    21600,
    28800,
    43200,
    86400,
)

BASE_PERIODS: tuple[int, ...] = (
    2,
    3,
    4,
    5,
    7,
    8,
    9,
    10,
    12,
    13,
    15,
    20,
    21,
    25,
    30,
    40,
    50,
    75,
    100,
    150,
    200,
)

# These ceilings match the rolling histories maintained by the live lab. They
# are a parity constraint, not a statement that longer averages are useless.
PERIOD_CEILING: dict[str, int] = {
    "S5": 200,
    "S10": 200,
    "S15": 200,
    "S30": 200,
    "M1": 200,
    "M2": 200,
    "M3": 200,
    "M4": 200,
    "M5": 200,
    "M6": 200,
    "M7": 200,
    "M8": 200,
    "M9": 200,
    "M10": 200,
    "M12": 200,
    "M15": 200,
    "M20": 200,
    "M30": 150,
    "M45": 100,
    "H1": 200,
    "H2": 200,
    "H3": 150,
    "H4": 100,
    "H6": 75,
    "H8": 50,
    "H12": 30,
    "D1": 15,
}

CANONICAL_PERIOD_PAIRS: tuple[tuple[int, int], ...] = (
    (2, 5),
    (3, 5),
    (3, 8),
    (3, 9),
    (4, 9),
    (5, 8),
    (5, 13),
    (7, 21),
    (8, 13),
    (8, 21),
    (9, 21),
    (10, 20),
    (12, 26),
    (13, 21),
    (15, 30),
    (20, 50),
    (21, 50),
    (25, 75),
    (30, 75),
    (40, 100),
    (50, 100),
    (50, 150),
    (50, 200),
    (75, 200),
    (100, 200),
)

AGGREGATE_FEATURE_NAMES: tuple[str, ...] = (
    "ma__scale_pips",
    "ma__sma_price_above_fraction",
    "ma__ema_price_above_fraction",
    "ma__sma_positive_slope_fraction",
    "ma__ema_positive_slope_fraction",
    "ma__sma_bull_order_fraction",
    "ma__ema_bull_order_fraction",
    "ma__sma_distance_mean_scale",
    "ma__ema_distance_mean_scale",
    "ma__sma_distance_dispersion_scale",
    "ma__ema_distance_dispersion_scale",
    "ma__sma_fan_width_scale",
    "ma__ema_fan_width_scale",
    "ma__sma_ema_disagreement",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def infer_pip_size(instrument: str) -> float:
    return 0.01 if str(instrument).upper().endswith("_JPY") else 0.0001


def timeframe_label(seconds: int) -> str:
    seconds = int(seconds)
    for label, value in TIMEFRAME_SECONDS.items():
        if value == seconds:
            return label
    if seconds < 60:
        return f"S{seconds}"
    if seconds % 3600 == 0:
        return f"H{seconds // 3600}"
    if seconds % 60 == 0:
        return f"M{seconds // 60}"
    raise ValueError(f"unsupported timeframe seconds: {seconds}")


def parse_timeframe(value: str | int) -> int:
    if isinstance(value, int):
        if value <= 0:
            raise ValueError("timeframe must be positive")
        return value
    label = str(value).strip().upper()
    if label in TIMEFRAME_SECONDS:
        return TIMEFRAME_SECONDS[label]
    if len(label) > 1 and label[1:].isdigit():
        amount = int(label[1:])
        multiplier = {"S": 1, "M": 60, "H": 3600, "D": 86400}.get(label[0])
        if multiplier and amount > 0:
            return amount * multiplier
    raise ValueError(f"unsupported timeframe: {value}")


def periods_for_timeframe(timeframe: str) -> tuple[int, ...]:
    label = str(timeframe).upper()
    if label not in PERIOD_CEILING:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    ceiling = PERIOD_CEILING[label]
    return tuple(period for period in BASE_PERIODS if period <= ceiling)


def period_pairs(periods: Iterable[int]) -> tuple[tuple[int, int], ...]:
    ordered = tuple(sorted({int(value) for value in periods}))
    available = set(ordered)
    pairs = set(zip(ordered, ordered[1:]))
    pairs.update(
        (fast, slow)
        for fast, slow in CANONICAL_PERIOD_PAIRS
        if fast in available and slow in available
    )
    return tuple(sorted(pairs, key=lambda pair: (pair[1], pair[0])))


def ma_feature_names(timeframe: str) -> tuple[str, ...]:
    periods = periods_for_timeframe(timeframe)
    names: list[str] = []
    for period in periods:
        prefix = f"ma__p{period}"
        names.extend(
            (
                f"{prefix}__sma_distance_scale",
                f"{prefix}__ema_distance_scale",
                f"{prefix}__sma_slope1_scale",
                f"{prefix}__ema_slope1_scale",
                f"{prefix}__sma_slope3_scale",
                f"{prefix}__ema_slope3_scale",
                f"{prefix}__sma_curvature_scale",
                f"{prefix}__ema_curvature_scale",
                f"{prefix}__sma_ema_gap_scale",
            )
        )
    for fast, slow in period_pairs(periods):
        prefix = f"ma__p{fast}_{slow}"
        for kind in ("sma", "ema"):
            names.extend(
                (
                    f"{prefix}__{kind}_gap_scale",
                    f"{prefix}__{kind}_velocity_scale",
                    f"{prefix}__{kind}_acceleration_scale",
                    f"{prefix}__{kind}_cross_age_log",
                    f"{prefix}__{kind}_cross_support",
                )
            )
    names.extend(AGGREGATE_FEATURE_NAMES)
    return tuple(names)


def pair_context_value(name: str, instrument: str) -> float | None:
    if not name.startswith("context__"):
        return None
    parts = name.split("__", 2)
    if len(parts) != 3:
        return math.nan
    kind, expected = parts[1], parts[2]
    normalized = str(instrument).upper()
    base, separator, quote = normalized.partition("_")
    if kind == "pair":
        return float(normalized == expected)
    if kind == "base":
        return float(base == expected)
    if kind == "quote":
        return float(bool(separator) and quote == expected)
    return math.nan


def _moving_average_arrays(
    values: np.ndarray,
    periods: tuple[int, ...],
) -> tuple[dict[int, np.ndarray], dict[int, np.ndarray]]:
    series = pd.Series(values.astype(np.float64, copy=False), copy=False)
    sma = {
        period: series.rolling(period, min_periods=period).mean().to_numpy(
            dtype=np.float64,
            copy=False,
        )
        for period in periods
    }
    ema = {
        period: series.ewm(
            span=period,
            adjust=False,
            min_periods=period,
        ).mean().to_numpy(dtype=np.float64, copy=False)
        for period in periods
    }
    return sma, ema


def _moving_average_arrays_numpy(
    values: np.ndarray,
    periods: tuple[int, ...],
) -> tuple[dict[int, np.ndarray], dict[int, np.ndarray]]:
    """Compute live averages without repeated pandas object construction."""

    count = values.size
    cumulative = np.concatenate(
        (np.zeros(1, dtype=np.float64), np.cumsum(values, dtype=np.float64))
    )
    sma: dict[int, np.ndarray] = {}
    ema: dict[int, np.ndarray] = {}
    alphas = np.asarray(
        [2.0 / (float(period) + 1.0) for period in periods],
        dtype=np.float64,
    )
    exponential_matrix = np.empty((len(periods), count), dtype=np.float64)
    exponential_matrix[:, 0] = values[0]
    for index in range(1, count):
        exponential_matrix[:, index] = (
            alphas * values[index]
            + (1.0 - alphas) * exponential_matrix[:, index - 1]
        )
    for row_index, period in enumerate(periods):
        simple = np.full(count, np.nan, dtype=np.float64)
        simple[period - 1 :] = (
            cumulative[period:] - cumulative[:-period]
        ) / float(period)
        sma[period] = simple

        exponential = exponential_matrix[row_index].copy()
        exponential[: period - 1] = np.nan
        ema[period] = exponential
    return sma, ema


def _rolling_scale(values: np.ndarray, pip: float) -> np.ndarray:
    changes = np.empty(values.size, dtype=np.float64)
    changes[0] = np.nan
    changes[1:] = np.abs(np.diff(values)) / max(pip, 1e-12)
    scale = (
        pd.Series(changes, copy=False)
        .rolling(20, min_periods=5)
        .mean()
        .to_numpy(dtype=np.float64, copy=False)
    )
    finite_scale = scale[np.isfinite(scale) & (scale > 1e-6)]
    fallback = float(np.median(finite_scale)) if finite_scale.size else 0.1
    return np.where(np.isfinite(scale) & (scale > 1e-6), scale, max(0.1, fallback))


def _cross_age(gap: np.ndarray) -> np.ndarray:
    side = gap >= 0.0
    valid = np.isfinite(gap)
    changes = np.zeros(gap.size, dtype=bool)
    if gap.size:
        changes[0] = valid[0]
    if gap.size > 1:
        changes[1:] = valid[1:] & valid[:-1] & (side[1:] != side[:-1])
    indexes = np.arange(gap.size, dtype=np.int64)
    last_change = np.maximum.accumulate(np.where(changes, indexes, -1))
    age = indexes - last_change
    age[last_change < 0] = gap.size
    return age.astype(np.float64)


def _column_mean(matrix: np.ndarray) -> np.ndarray:
    valid = np.isfinite(matrix)
    count = valid.sum(axis=0)
    total = np.where(valid, matrix, 0.0).sum(axis=0)
    return np.divide(
        total,
        count,
        out=np.full(matrix.shape[1], np.nan, dtype=np.float64),
        where=count > 0,
    )


def _column_std(matrix: np.ndarray) -> np.ndarray:
    valid = np.isfinite(matrix)
    count = valid.sum(axis=0)
    mean = _column_mean(matrix)
    squared = np.where(valid, (matrix - mean) ** 2, 0.0).sum(axis=0)
    return np.sqrt(
        np.divide(
            squared,
            count,
            out=np.full(matrix.shape[1], np.nan, dtype=np.float64),
            where=count > 0,
        )
    )


def _column_positive_fraction(matrix: np.ndarray) -> np.ndarray:
    valid = np.isfinite(matrix)
    count = valid.sum(axis=0)
    positive = (valid & (matrix > 0.0)).sum(axis=0)
    return np.divide(
        positive,
        count,
        out=np.full(matrix.shape[1], np.nan, dtype=np.float64),
        where=count > 0,
    )


def _column_width(matrix: np.ndarray) -> np.ndarray:
    valid = np.isfinite(matrix)
    count = valid.sum(axis=0)
    maximum = np.where(valid, matrix, -np.inf).max(axis=0)
    minimum = np.where(valid, matrix, np.inf).min(axis=0)
    return np.where(count > 0, maximum - minimum, np.nan)


def _column_disagreement(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    valid = np.isfinite(left) & np.isfinite(right)
    count = valid.sum(axis=0)
    disagree = (valid & ((left > 0.0) != (right > 0.0))).sum(axis=0)
    return np.divide(
        disagree,
        count,
        out=np.full(left.shape[1], np.nan, dtype=np.float64),
        where=count > 0,
    )


def build_ma_feature_matrix(
    values: Iterable[float],
    positions: Iterable[int],
    pip: float,
    timeframe: str,
) -> tuple[np.ndarray, tuple[str, ...]]:
    """Build causal MA-only rows for selected completed-bar positions."""

    close = np.asarray(list(values), dtype=np.float64)
    selected = np.asarray(list(positions), dtype=np.int64)
    names = ma_feature_names(timeframe)
    if close.ndim != 1 or selected.ndim != 1:
        raise ValueError("values and positions must be one-dimensional")
    if selected.size == 0:
        return np.empty((0, len(names)), dtype=np.float32), names
    if selected.min() < 0 or selected.max() >= close.size:
        raise IndexError("feature position outside close series")
    periods = periods_for_timeframe(timeframe)
    scale_pips = _rolling_scale(close, pip)
    scale_price = np.maximum(0.1, scale_pips) * max(pip, 1e-12)
    sma, ema = _moving_average_arrays(close, periods)
    columns: dict[str, np.ndarray] = {}
    sma_distances: list[np.ndarray] = []
    ema_distances: list[np.ndarray] = []
    sma_slopes: list[np.ndarray] = []
    ema_slopes: list[np.ndarray] = []

    for period in periods:
        prefix = f"ma__p{period}"
        sma_values = sma[period]
        ema_values = ema[period]
        sma_distance = (close - sma_values) / scale_price
        ema_distance = (close - ema_values) / scale_price
        sma_slope1 = np.r_[np.nan, np.diff(sma_values)] / scale_price
        ema_slope1 = np.r_[np.nan, np.diff(ema_values)] / scale_price
        sma_slope3 = np.r_[np.full(3, np.nan), sma_values[3:] - sma_values[:-3]] / scale_price
        ema_slope3 = np.r_[np.full(3, np.nan), ema_values[3:] - ema_values[:-3]] / scale_price
        sma_curvature = np.r_[np.nan, np.diff(sma_slope1)]
        ema_curvature = np.r_[np.nan, np.diff(ema_slope1)]
        columns[f"{prefix}__sma_distance_scale"] = sma_distance[selected]
        columns[f"{prefix}__ema_distance_scale"] = ema_distance[selected]
        columns[f"{prefix}__sma_slope1_scale"] = sma_slope1[selected]
        columns[f"{prefix}__ema_slope1_scale"] = ema_slope1[selected]
        columns[f"{prefix}__sma_slope3_scale"] = sma_slope3[selected]
        columns[f"{prefix}__ema_slope3_scale"] = ema_slope3[selected]
        columns[f"{prefix}__sma_curvature_scale"] = sma_curvature[selected]
        columns[f"{prefix}__ema_curvature_scale"] = ema_curvature[selected]
        columns[f"{prefix}__sma_ema_gap_scale"] = (
            (sma_values - ema_values) / scale_price
        )[selected]
        sma_distances.append(sma_distance[selected])
        ema_distances.append(ema_distance[selected])
        sma_slopes.append(sma_slope3[selected])
        ema_slopes.append(ema_slope3[selected])

    sma_order: list[np.ndarray] = []
    ema_order: list[np.ndarray] = []
    for fast, slow in period_pairs(periods):
        prefix = f"ma__p{fast}_{slow}"
        for kind, averages, ordering in (
            ("sma", sma, sma_order),
            ("ema", ema, ema_order),
        ):
            gap = averages[fast] - averages[slow]
            normalized_gap = gap / scale_price
            velocity = np.r_[np.nan, np.diff(gap)] / scale_price
            acceleration = np.r_[np.nan, np.diff(velocity)]
            age = _cross_age(gap)
            side = np.where(gap >= 0.0, 1.0, -1.0)
            columns[f"{prefix}__{kind}_gap_scale"] = normalized_gap[selected]
            columns[f"{prefix}__{kind}_velocity_scale"] = velocity[selected]
            columns[f"{prefix}__{kind}_acceleration_scale"] = acceleration[selected]
            columns[f"{prefix}__{kind}_cross_age_log"] = np.log1p(age[selected])
            columns[f"{prefix}__{kind}_cross_support"] = (
                side[selected] / (1.0 + age[selected])
            )
            ordering.append(normalized_gap[selected])

    sma_distance_matrix = np.vstack(sma_distances)
    ema_distance_matrix = np.vstack(ema_distances)
    sma_slope_matrix = np.vstack(sma_slopes)
    ema_slope_matrix = np.vstack(ema_slopes)
    sma_order_matrix = np.vstack(sma_order)
    ema_order_matrix = np.vstack(ema_order)
    columns.update(
        {
            "ma__scale_pips": scale_pips[selected],
            "ma__sma_price_above_fraction": _column_positive_fraction(
                sma_distance_matrix
            ),
            "ma__ema_price_above_fraction": _column_positive_fraction(
                ema_distance_matrix
            ),
            "ma__sma_positive_slope_fraction": _column_positive_fraction(
                sma_slope_matrix
            ),
            "ma__ema_positive_slope_fraction": _column_positive_fraction(
                ema_slope_matrix
            ),
            "ma__sma_bull_order_fraction": _column_positive_fraction(
                sma_order_matrix
            ),
            "ma__ema_bull_order_fraction": _column_positive_fraction(
                ema_order_matrix
            ),
            "ma__sma_distance_mean_scale": _column_mean(sma_distance_matrix),
            "ma__ema_distance_mean_scale": _column_mean(ema_distance_matrix),
            "ma__sma_distance_dispersion_scale": _column_std(
                sma_distance_matrix
            ),
            "ma__ema_distance_dispersion_scale": _column_std(
                ema_distance_matrix
            ),
            "ma__sma_fan_width_scale": _column_width(sma_distance_matrix),
            "ma__ema_fan_width_scale": _column_width(ema_distance_matrix),
            "ma__sma_ema_disagreement": _column_disagreement(
                sma_distance_matrix,
                ema_distance_matrix,
            ),
        }
    )
    matrix = np.column_stack([columns[name] for name in names]).astype(
        np.float32,
        copy=False,
    )
    return matrix, names


def build_ma_feature_vector(
    values: Iterable[float],
    pip: float,
    timeframe: str,
) -> dict[str, float]:
    close = np.asarray(list(values), dtype=np.float64)
    minimum = max(periods_for_timeframe(timeframe)) + 4
    if close.size < minimum:
        return {}
    if not np.isfinite(close).all():
        matrix, names = build_ma_feature_matrix(
            close,
            (close.size - 1,),
            pip,
            timeframe,
        )
        return {
            name: round(float(value), 8)
            for name, value in zip(names, matrix[0])
            if math.isfinite(float(value))
        }

    names = ma_feature_names(timeframe)
    periods = periods_for_timeframe(timeframe)
    scale_pips = _rolling_scale(close, pip)
    scale_price = np.maximum(0.1, scale_pips) * max(pip, 1e-12)
    sma, ema = _moving_average_arrays_numpy(close, periods)
    columns: dict[str, float] = {}
    sma_distances: list[float] = []
    ema_distances: list[float] = []
    sma_slopes: list[float] = []
    ema_slopes: list[float] = []

    for period in periods:
        prefix = f"ma__p{period}"
        sma_values = sma[period]
        ema_values = ema[period]
        sma_distance = (close[-1] - sma_values[-1]) / scale_price[-1]
        ema_distance = (close[-1] - ema_values[-1]) / scale_price[-1]
        sma_slope1 = (sma_values[-1] - sma_values[-2]) / scale_price[-1]
        ema_slope1 = (ema_values[-1] - ema_values[-2]) / scale_price[-1]
        sma_previous_slope1 = (
            (sma_values[-2] - sma_values[-3]) / scale_price[-2]
        )
        ema_previous_slope1 = (
            (ema_values[-2] - ema_values[-3]) / scale_price[-2]
        )
        sma_slope3 = (sma_values[-1] - sma_values[-4]) / scale_price[-1]
        ema_slope3 = (ema_values[-1] - ema_values[-4]) / scale_price[-1]
        columns[f"{prefix}__sma_distance_scale"] = sma_distance
        columns[f"{prefix}__ema_distance_scale"] = ema_distance
        columns[f"{prefix}__sma_slope1_scale"] = sma_slope1
        columns[f"{prefix}__ema_slope1_scale"] = ema_slope1
        columns[f"{prefix}__sma_slope3_scale"] = sma_slope3
        columns[f"{prefix}__ema_slope3_scale"] = ema_slope3
        columns[f"{prefix}__sma_curvature_scale"] = (
            sma_slope1 - sma_previous_slope1
        )
        columns[f"{prefix}__ema_curvature_scale"] = (
            ema_slope1 - ema_previous_slope1
        )
        columns[f"{prefix}__sma_ema_gap_scale"] = (
            (sma_values[-1] - ema_values[-1]) / scale_price[-1]
        )
        sma_distances.append(sma_distance)
        ema_distances.append(ema_distance)
        sma_slopes.append(sma_slope3)
        ema_slopes.append(ema_slope3)

    sma_order: list[float] = []
    ema_order: list[float] = []
    for fast, slow in period_pairs(periods):
        prefix = f"ma__p{fast}_{slow}"
        for kind, averages, ordering in (
            ("sma", sma, sma_order),
            ("ema", ema, ema_order),
        ):
            gap = averages[fast] - averages[slow]
            normalized_gap = gap[-1] / scale_price[-1]
            velocity = (gap[-1] - gap[-2]) / scale_price[-1]
            previous_velocity = (gap[-2] - gap[-3]) / scale_price[-2]
            valid_gap = gap[np.isfinite(gap)]
            changes = np.flatnonzero(
                (valid_gap[1:] >= 0.0) != (valid_gap[:-1] >= 0.0)
            )
            age = (
                close.size
                if changes.size == 0
                else valid_gap.size - int(changes[-1]) - 2
            )
            side = 1.0 if gap[-1] >= 0.0 else -1.0
            columns[f"{prefix}__{kind}_gap_scale"] = normalized_gap
            columns[f"{prefix}__{kind}_velocity_scale"] = velocity
            columns[f"{prefix}__{kind}_acceleration_scale"] = (
                velocity - previous_velocity
            )
            columns[f"{prefix}__{kind}_cross_age_log"] = np.log1p(age)
            columns[f"{prefix}__{kind}_cross_support"] = side / (1.0 + age)
            ordering.append(normalized_gap)

    sma_distance_matrix = np.asarray(sma_distances, dtype=np.float64).reshape(-1, 1)
    ema_distance_matrix = np.asarray(ema_distances, dtype=np.float64).reshape(-1, 1)
    sma_slope_matrix = np.asarray(sma_slopes, dtype=np.float64).reshape(-1, 1)
    ema_slope_matrix = np.asarray(ema_slopes, dtype=np.float64).reshape(-1, 1)
    sma_order_matrix = np.asarray(sma_order, dtype=np.float64).reshape(-1, 1)
    ema_order_matrix = np.asarray(ema_order, dtype=np.float64).reshape(-1, 1)
    columns.update(
        {
            "ma__scale_pips": scale_pips[-1],
            "ma__sma_price_above_fraction": _column_positive_fraction(
                sma_distance_matrix
            )[0],
            "ma__ema_price_above_fraction": _column_positive_fraction(
                ema_distance_matrix
            )[0],
            "ma__sma_positive_slope_fraction": _column_positive_fraction(
                sma_slope_matrix
            )[0],
            "ma__ema_positive_slope_fraction": _column_positive_fraction(
                ema_slope_matrix
            )[0],
            "ma__sma_bull_order_fraction": _column_positive_fraction(
                sma_order_matrix
            )[0],
            "ma__ema_bull_order_fraction": _column_positive_fraction(
                ema_order_matrix
            )[0],
            "ma__sma_distance_mean_scale": _column_mean(
                sma_distance_matrix
            )[0],
            "ma__ema_distance_mean_scale": _column_mean(
                ema_distance_matrix
            )[0],
            "ma__sma_distance_dispersion_scale": _column_std(
                sma_distance_matrix
            )[0],
            "ma__ema_distance_dispersion_scale": _column_std(
                ema_distance_matrix
            )[0],
            "ma__sma_fan_width_scale": _column_width(
                sma_distance_matrix
            )[0],
            "ma__ema_fan_width_scale": _column_width(
                ema_distance_matrix
            )[0],
            "ma__sma_ema_disagreement": _column_disagreement(
                sma_distance_matrix,
                ema_distance_matrix,
            )[0],
        }
    )
    return {
        name: round(float(np.float32(columns[name])), 8)
        for name in names
        if math.isfinite(float(columns[name]))
    }


def artifact_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sigmoid(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _decision_centered_probability(value: float, threshold: float) -> float:
    probability = float(np.clip(value, 1e-6, 1.0 - 1e-6))
    threshold = float(np.clip(threshold, 1e-6, 1.0 - 1e-6))
    logit = math.log(probability / (1.0 - probability))
    threshold_logit = math.log(threshold / (1.0 - threshold))
    return float(_sigmoid(np.asarray([logit - threshold_logit]))[0])


def _series_map(features: Mapping[str, Any]) -> dict[str, list[float]]:
    explicit = features.get("ma_series_by_timeframe")
    if isinstance(explicit, Mapping):
        output = {
            str(label).upper(): [
                finite(value, math.nan)
                for value in values
                if math.isfinite(finite(value, math.nan))
            ]
            for label, values in explicit.items()
            if isinstance(values, (list, tuple, np.ndarray))
        }
        if output:
            return output
    legacy = {
        "M1": "closes",
        "M5": "m5_closes",
        "M10": "m10_closes",
        "M15": "m15_closes",
        "M30": "m30_closes",
        "H1": "h1_closes",
        "H2": "h2_closes",
        "H3": "h3_closes",
        "H4": "h4_closes",
    }
    return {
        timeframe: [
            finite(value, math.nan)
            for value in (features.get(key) or [])
            if math.isfinite(finite(value, math.nan))
        ]
        for timeframe, key in legacy.items()
        if features.get(key)
    }


class MaFeatureGridRuntime:
    """Hot-reload a fitted MA grid and emit shadow forecast curves."""

    def __init__(self, artifact_path: Path, minimum_timeframe_sec: int = 0) -> None:
        self.artifact_path = Path(artifact_path)
        self.minimum_timeframe_sec = max(0, int(minimum_timeframe_sec))
        self.modified_ns = -2
        self.artifact: dict[str, Any] = {}
        self.load_error = ""
        self._forecast_cache: dict[
            tuple[str, str],
            tuple[str, dict[str, Any]],
        ] = {}
        self.last_batch_stats: dict[str, int] = {
            "requested_series": 0,
            "cached_series": 0,
            "computed_series": 0,
            "candidates": 0,
        }
        self._reload()

    def _reload(self) -> None:
        try:
            modified_ns = self.artifact_path.stat().st_mtime_ns
        except OSError:
            modified_ns = -1
        if modified_ns == self.modified_ns:
            return
        self.modified_ns = modified_ns
        self._forecast_cache.clear()
        if modified_ns < 0 or joblib is None:
            self.artifact = {}
            self.load_error = (
                "artifact_missing" if modified_ns < 0 else "joblib_unavailable"
            )
            return
        try:
            payload = joblib.load(self.artifact_path)
        except Exception as exc:  # pragma: no cover - external corruption.
            self.artifact = {}
            self.load_error = f"{type(exc).__name__}: {exc}"
            return
        if not isinstance(payload, dict) or payload.get("family") != FAMILY:
            self.artifact = {}
            self.load_error = "invalid_ma_grid_artifact"
            return
        self.artifact = payload
        self.load_error = ""

    @property
    def ready(self) -> bool:
        self._reload()
        return bool(self.artifact.get("models"))

    def metadata(self) -> dict[str, Any]:
        self._reload()
        models = self.artifact.get("models") or {}
        return {
            "ready": bool(models),
            "reason": self.load_error,
            "artifact": str(self.artifact_path.resolve()),
            "generated_at": self.artifact.get("generated_at"),
            "timeframes": sorted(
                (
                    label
                    for label in models
                    if TIMEFRAME_SECONDS.get(label, 0)
                    >= self.minimum_timeframe_sec
                ),
                key=lambda label: TIMEFRAME_SECONDS.get(label, 10**12),
            ),
            "horizons_sec": list(self.artifact.get("horizons_sec") or []),
            "execution_policy": self.artifact.get("execution_policy", "shadow_only"),
        }

    @staticmethod
    def _calibrated(
        raw: np.ndarray,
        calibration: Iterable[Mapping[str, Any]],
        *,
        probability: bool = False,
        nonnegative: bool = False,
    ) -> np.ndarray:
        rows = list(calibration)
        output = raw.astype(np.float64, copy=True)
        for index in range(min(output.shape[-1], len(rows))):
            output[..., index] = (
                finite(rows[index].get("slope"), 1.0) * output[..., index]
                + finite(rows[index].get("intercept"))
            )
        if probability:
            output = _sigmoid(output)
        if nonnegative:
            output = np.maximum(0.0, output)
        return output

    @staticmethod
    def _prediction_matrix(
        values: Any,
        row_count: int,
    ) -> np.ndarray:
        matrix = np.asarray(values, dtype=np.float64)
        if matrix.ndim == 0:
            return matrix.reshape(1, 1)
        if matrix.ndim == 1:
            return (
                matrix.reshape(1, -1)
                if row_count == 1
                else matrix.reshape(-1, 1)
            )
        return matrix

    @staticmethod
    def _curve_from_calibrated(
        spec: Mapping[str, Any],
        probability_up: np.ndarray,
        signed_pips: np.ndarray,
        magnitude_pips: np.ndarray,
        spread_pips: float,
        executable_edge_pips: np.ndarray | None = None,
    ) -> dict[str, dict[str, Any]]:
        horizons = tuple(int(value) for value in spec.get("horizons_sec") or ())
        metrics = spec.get("holdout_metrics") or {}
        thresholds = tuple(
            finite(value, 0.5)
            for value in (spec.get("direction_thresholds") or ())
        )
        edge_thresholds = tuple(
            finite(value, math.inf)
            for value in (spec.get("executable_edge_thresholds") or ())
        )
        movement_gates = tuple(spec.get("movement_cost_gates") or ())
        edge_values = (
            np.asarray(executable_edge_pips, dtype=np.float64)
            if executable_edge_pips is not None
            else np.asarray([], dtype=np.float64)
        )
        edge_available = edge_values.size >= 2 * len(horizons)
        curve: dict[str, dict[str, Any]] = {}
        for index, horizon in enumerate(horizons):
            if index >= len(signed_pips):
                break
            calibrated_probability = float(probability_up[index])
            threshold = thresholds[index] if index < len(thresholds) else 0.5
            decision_probability = _decision_centered_probability(
                calibrated_probability,
                threshold,
            )
            direction = "buy" if decision_probability >= 0.5 else "sell"
            selected_signed = (
                float(signed_pips[index])
                if direction == "buy"
                else -float(signed_pips[index])
            )
            projected_net = selected_signed - max(0.0, spread_pips)
            movement_gate = (
                movement_gates[index]
                if index < len(movement_gates)
                and isinstance(movement_gates[index], Mapping)
                else {}
            )
            minimum_magnitude_to_cost = finite(
                movement_gate.get("minimum_magnitude_to_cost"),
                math.inf,
            )
            minimum_confidence = finite(
                movement_gate.get("minimum_confidence"),
                math.inf,
            )
            decision_cost = max(0.05, float(spread_pips))
            magnitude_to_cost = max(
                0.0,
                float(magnitude_pips[index]),
            ) / decision_cost
            confidence = max(decision_probability, 1.0 - decision_probability)
            movement_gate_supported = (
                math.isfinite(minimum_magnitude_to_cost)
                and math.isfinite(minimum_confidence)
            )
            edge_fields: dict[str, Any] = {}
            if edge_available:
                predicted_long_net = float(edge_values[index])
                predicted_short_net = float(
                    edge_values[index + len(horizons)]
                )
                predicted_training_cost = max(
                    0.0,
                    -0.5 * (predicted_long_net + predicted_short_net),
                )
                predicted_side_net = (
                    predicted_long_net
                    if direction == "buy"
                    else predicted_short_net
                )
                edge_threshold = (
                    edge_thresholds[index]
                    if index < len(edge_thresholds)
                    else math.inf
                )
                edge_supported = math.isfinite(edge_threshold)
                if edge_supported:
                    projected_net = (
                        predicted_side_net
                        + predicted_training_cost
                        - max(0.0, spread_pips)
                    )
                edge_fields = {
                    "predicted_long_net_pips": round(
                        predicted_long_net,
                        8,
                    ),
                    "predicted_short_net_pips": round(
                        predicted_short_net,
                        8,
                    ),
                    "predicted_training_cost_pips": round(
                        predicted_training_cost,
                        8,
                    ),
                    "executable_edge_threshold": (
                        round(float(edge_threshold), 8)
                        if edge_supported
                        else None
                    ),
                    "executable_edge_supported": edge_supported,
                    "executable_edge_pass": bool(
                        edge_supported
                        and projected_net >= edge_threshold
                    ),
                }
            curve[str(horizon)] = {
                "horizon_sec": horizon,
                "direction": direction,
                "probability_up": round(decision_probability, 8),
                "calibrated_probability_up": round(calibrated_probability, 8),
                "direction_threshold": round(float(threshold), 8),
                "predicted_signed_pips": round(float(signed_pips[index]), 8),
                "predicted_magnitude_pips": round(float(magnitude_pips[index]), 8),
                "decision_cost_pips": round(decision_cost, 8),
                "predicted_magnitude_to_cost": round(magnitude_to_cost, 8),
                "movement_gate_minimum_magnitude_to_cost": (
                    round(minimum_magnitude_to_cost, 8)
                    if movement_gate_supported
                    else None
                ),
                "movement_gate_minimum_confidence": (
                    round(minimum_confidence, 8)
                    if movement_gate_supported
                    else None
                ),
                "movement_gate_supported": movement_gate_supported,
                "movement_gate_pass": bool(
                    movement_gate_supported
                    and magnitude_to_cost >= minimum_magnitude_to_cost
                    and confidence >= minimum_confidence
                ),
                "predicted_side_gross_pips": round(selected_signed, 8),
                "projected_net_pips": round(projected_net, 8),
                **edge_fields,
                "account_eligible": False,
                "status": "shadow_only",
                "holdout": metrics.get(str(horizon)) or {},
            }
        return curve

    def predict_timeframe_batch(
        self,
        timeframe: str,
        requests: Iterable[
            tuple[Any, Iterable[float], float, float]
        ],
    ) -> dict[Any, dict[str, Any]]:
        self._reload()
        label = str(timeframe).upper()
        spec = (self.artifact.get("models") or {}).get(label) or {}
        request_rows = list(requests)
        if not spec:
            return {
                key: {
                    "ready": False,
                    "reason": "timeframe_not_fitted",
                    "timeframe": label,
                }
                for key, _, _, _ in request_rows
            }
        names = tuple(spec.get("feature_names") or ())
        results: dict[Any, dict[str, Any]] = {}
        keys: list[Any] = []
        rows: list[list[float]] = []
        spreads: list[float] = []
        feature_counts: list[int] = []
        for key, values, pip, spread_pips in request_rows:
            vector = build_ma_feature_vector(values, pip, label)
            if not vector or not names:
                results[key] = {
                    "ready": False,
                    "reason": "insufficient_live_ma_history",
                    "timeframe": label,
                    "feature_count": len(vector),
                }
                continue
            instrument = (
                str(key[0])
                if isinstance(key, tuple) and key
                else ""
            )
            feature_row: list[float] = []
            for name in names:
                context_value = pair_context_value(name, instrument)
                value = (
                    context_value
                    if context_value is not None
                    else vector.get(name)
                )
                feature_row.append(finite(value, math.nan))
            keys.append(key)
            rows.append(feature_row)
            spreads.append(max(0.0, finite(spread_pips)))
            feature_counts.append(len(names))
        if not rows:
            return results
        matrix = np.asarray(rows, dtype=np.float64)
        try:
            direction_raw = self._prediction_matrix(
                spec["direction_estimator"].predict(matrix),
                len(rows),
            )
            signed_raw = self._prediction_matrix(
                spec["pip_estimator"].predict(matrix),
                len(rows),
            )
            magnitude_raw = self._prediction_matrix(
                spec["magnitude_estimator"].predict(matrix),
                len(rows),
            )
        except Exception as exc:
            for key in keys:
                results[key] = {
                    "ready": False,
                    "reason": f"inference_error:{type(exc).__name__}",
                    "timeframe": label,
                }
            return results
        probability_up = self._calibrated(
            direction_raw,
            spec.get("direction_calibration") or (),
            probability=True,
        )
        signed_pips = self._calibrated(
            signed_raw,
            spec.get("pip_calibration") or (),
        )
        magnitude_pips = self._calibrated(
            magnitude_raw,
            spec.get("magnitude_calibration") or (),
            nonnegative=True,
        )
        executable_edge_pips: np.ndarray | None = None
        executable_edge_estimator = spec.get("executable_edge_estimator")
        if executable_edge_estimator is not None:
            try:
                executable_edge_raw = self._prediction_matrix(
                    executable_edge_estimator.predict(matrix),
                    len(rows),
                )
                executable_edge_pips = self._calibrated(
                    executable_edge_raw,
                    spec.get("executable_edge_calibration") or (),
                )
                if executable_edge_pips.shape[1] != 2 * signed_pips.shape[1]:
                    executable_edge_pips = None
            except Exception:
                executable_edge_pips = None
        if str(spec.get("target_space") or "raw_pips") == "local_scale":
            scale_feature = str(
                spec.get("target_scale_feature") or "ma__scale_pips"
            )
            try:
                scale_index = names.index(scale_feature)
            except ValueError:
                for key in keys:
                    results[key] = {
                        "ready": False,
                        "reason": "artifact_missing_target_scale_feature",
                        "timeframe": label,
                    }
                return results
            target_scales = np.maximum(
                0.1,
                np.nan_to_num(
                    matrix[:, scale_index],
                    nan=0.1,
                    posinf=0.1,
                    neginf=0.1,
                ),
            )
            target_clip_abs = np.asarray(
                spec.get("target_clip_abs") or (),
                dtype=np.float64,
            )
            if target_clip_abs.size != signed_pips.shape[1]:
                for key in keys:
                    results[key] = {
                        "ready": False,
                        "reason": "artifact_target_clip_shape_mismatch",
                        "timeframe": label,
                    }
                return results
            signed_pips = np.clip(
                signed_pips,
                -target_clip_abs,
                target_clip_abs,
            ) * target_scales[:, None]
            magnitude_pips = np.clip(
                magnitude_pips,
                0.0,
                target_clip_abs,
            ) * target_scales[:, None]
            if executable_edge_pips is not None:
                executable_edge_pips = np.clip(
                    executable_edge_pips,
                    np.tile(-target_clip_abs, 2),
                    np.tile(target_clip_abs, 2),
                ) * target_scales[:, None]
        for index, key in enumerate(keys):
            curve = self._curve_from_calibrated(
                spec,
                probability_up[index],
                signed_pips[index],
                magnitude_pips[index],
                spreads[index],
                (
                    executable_edge_pips[index]
                    if executable_edge_pips is not None
                    else None
                ),
            )
            results[key] = {
                "ready": bool(curve),
                "timeframe": label,
                "feature_count": feature_counts[index],
                "curve": curve,
            }
        return results

    def predict_timeframe(
        self,
        timeframe: str,
        values: Iterable[float],
        pip: float,
        spread_pips: float,
    ) -> dict[str, Any]:
        key = "__single__"
        return self.predict_timeframe_batch(
            timeframe,
            [(key, values, pip, spread_pips)],
        )[key]

    def _candidate_from_forecast(
        self,
        instrument: str,
        timeframe: str,
        forecast: Mapping[str, Any],
        *,
        bid: float,
        ask: float,
        pip: float,
        spread: float,
        origin: str,
        generated_epoch: float,
    ) -> dict[str, Any] | None:
        curve = {
            horizon: {
                **point,
                "projected_net_pips": round(
                    finite(point.get("predicted_side_gross_pips")) - spread,
                    8,
                ),
            }
            for horizon, point in (forecast.get("curve") or {}).items()
        }
        points = list(curve.values())
        if not points:
            return None
        reference = min(
            points,
            key=lambda row: (
                abs(
                    int(row["horizon_sec"])
                    - max(60, TIMEFRAME_SECONDS[timeframe])
                ),
                int(row["horizon_sec"]),
            ),
        )
        direction = str(reference["direction"])
        identity = json.dumps(
            {
                "family": FAMILY,
                "instrument": instrument,
                "timeframe": timeframe,
                "feature_origin_utc": origin,
                "artifact_generated_at": self.artifact.get("generated_at"),
            },
            sort_keys=True,
        )
        candidate_id = (
            "ma-grid-"
            + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        )
        return {
            "id": candidate_id,
            "feed_dedupe_key": f"latest-{FAMILY}-{instrument}-{timeframe}",
            "lane_id": f"{FAMILY}.{timeframe.lower()}",
            "family": FAMILY,
            "profile": "ma_only",
            "model_id": f"{FAMILY}.{timeframe.lower()}",
            "input_timeframe": timeframe,
            "training_timeframe": timeframe,
            "signal_role": "structural",
            "instrument": instrument,
            "direction": direction,
            "bid": float(bid),
            "ask": float(ask),
            "pip": pip,
            "spread_pips": spread,
            "generated_epoch": generated_epoch,
            "probability_up": reference["probability_up"],
            "predicted_signed_pips": reference["predicted_signed_pips"],
            "projected_net_pips": reference["projected_net_pips"],
            "signal_reference_horizon_sec": int(reference["horizon_sec"]),
            "forecast_curve": curve,
            "account_eligible": False,
            "research_only": True,
            "research_blocked_reason": "ma_grid_shadow_validation",
            "preconsensus_class": "near_threshold",
            "preconsensus_blockers": ["ma_grid_shadow_validation"],
            "matrix_input_weight": 0.25,
            "producer_metadata": {
                "artifact_generated_at": self.artifact.get("generated_at"),
                "artifact_schema_version": self.artifact.get("schema_version"),
                "feature_count": forecast.get("feature_count"),
                "strict_feature_family": "moving_averages_only",
                "feature_origin_utc": origin,
            },
        }

    def forecast_candidates_batch(
        self,
        requests: Mapping[
            str,
            tuple[Mapping[str, Any], float, float],
        ],
        *,
        generated_epoch: float | None = None,
    ) -> list[dict[str, Any]]:
        self._reload()
        if not self.artifact.get("models"):
            return []
        generated_epoch = (
            datetime.now(timezone.utc).timestamp()
            if generated_epoch is None
            else float(generated_epoch)
        )
        prepared: dict[tuple[str, str], dict[str, Any]] = {}
        cached_series = 0
        pending: dict[
            str,
            list[tuple[tuple[str, str], Iterable[float], float, float]],
        ] = {}
        for instrument, (features, bid, ask) in requests.items():
            pip = max(
                1e-12,
                finite(features.get("pip"), infer_pip_size(instrument)),
            )
            spread = max(0.0, (float(ask) - float(bid)) / pip)
            for timeframe, values in _series_map(features).items():
                if timeframe not in TIMEFRAME_SECONDS:
                    continue
                if TIMEFRAME_SECONDS[timeframe] < self.minimum_timeframe_sec:
                    continue
                origins = features.get("ma_series_origins")
                origin = (
                    str(origins.get(timeframe) or "")
                    if isinstance(origins, Mapping)
                    else ""
                )
                if not origin:
                    tail = list(values)[-2:]
                    origin = (
                        f"len={len(values)}|"
                        + "|".join(
                            f"{finite(value):.12g}" for value in tail
                        )
                    )
                cache_key = (instrument, timeframe)
                prepared[cache_key] = {
                    "bid": float(bid),
                    "ask": float(ask),
                    "pip": pip,
                    "spread": spread,
                    "origin": origin,
                }
                cached = self._forecast_cache.get(cache_key)
                if cached is not None and cached[0] == origin:
                    prepared[cache_key]["forecast"] = cached[1]
                    cached_series += 1
                    continue
                pending.setdefault(timeframe, []).append(
                    (cache_key, values, pip, 0.0)
                )
        for timeframe, rows in pending.items():
            forecasts = self.predict_timeframe_batch(timeframe, rows)
            for cache_key, forecast in forecasts.items():
                origin = str(prepared[cache_key]["origin"])
                self._forecast_cache[cache_key] = (origin, forecast)
                prepared[cache_key]["forecast"] = forecast

        candidates: list[dict[str, Any]] = []
        for (instrument, timeframe), row in prepared.items():
            forecast = row.get("forecast") or {}
            if not forecast.get("ready"):
                continue
            candidate = self._candidate_from_forecast(
                instrument,
                timeframe,
                forecast,
                bid=float(row["bid"]),
                ask=float(row["ask"]),
                pip=float(row["pip"]),
                spread=float(row["spread"]),
                origin=str(row["origin"]),
                generated_epoch=generated_epoch,
            )
            if candidate is not None:
                candidates.append(candidate)
        self.last_batch_stats = {
            "requested_series": len(prepared),
            "cached_series": cached_series,
            "computed_series": sum(len(rows) for rows in pending.values()),
            "candidates": len(candidates),
        }
        return candidates

    def forecast_candidates(
        self,
        instrument: str,
        features: Mapping[str, Any],
        bid: float,
        ask: float,
        *,
        generated_epoch: float | None = None,
    ) -> list[dict[str, Any]]:
        return self.forecast_candidates_batch(
            {instrument: (features, bid, ask)},
            generated_epoch=generated_epoch,
        )


def contract_payload() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "family": FAMILY,
        "timeframes": TIMEFRAME_SECONDS,
        "horizons_sec": list(FORECAST_HORIZONS_SEC),
        "periods": {
            timeframe: list(periods_for_timeframe(timeframe))
            for timeframe in TIMEFRAME_SECONDS
        },
        "feature_count_by_timeframe": {
            timeframe: len(ma_feature_names(timeframe))
            for timeframe in TIMEFRAME_SECONDS
        },
        "targets": {
            "direction": "future executable-window midpoint move above zero",
            "signed_pips": "future midpoint move in instrument pips",
            "magnitude_pips": "absolute future midpoint move in instrument pips",
            "executable_long_net_pips": "entry at ask and exit at bid when observed",
            "executable_short_net_pips": "entry at bid and exit at ask when observed",
        },
        "cost_policy": (
            "observed bid/ask when available; pair median observed spread proxy "
            "for older midpoint-only rows"
        ),
        "causality": "completed bars only; entry starts after decision-bar completion",
        "feature_exclusions": [
            "raw returns",
            "oscillators",
            "volume",
            "order book",
            "position book",
            "account state",
            "other model signals",
            "spread as a structural predictor",
        ],
        "execution_policy": "shadow_only_until_chronological_holdout_and_live_paper_promotion",
    }


__all__ = [
    "AGGREGATE_FEATURE_NAMES",
    "BASE_PERIODS",
    "CANONICAL_PERIOD_PAIRS",
    "FAMILY",
    "FORECAST_HORIZONS_SEC",
    "MaFeatureGridRuntime",
    "PERIOD_CEILING",
    "SCHEMA_VERSION",
    "TIMEFRAME_SECONDS",
    "artifact_checksum",
    "build_ma_feature_matrix",
    "build_ma_feature_vector",
    "contract_payload",
    "finite",
    "infer_pip_size",
    "ma_feature_names",
    "pair_context_value",
    "parse_timeframe",
    "period_pairs",
    "periods_for_timeframe",
    "timeframe_label",
    "utc_now",
]
