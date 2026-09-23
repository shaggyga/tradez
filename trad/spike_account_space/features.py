"""Causal five-minute features and exact wall-clock two-hour labels.

This module deliberately operates on the regular five-minute bar cache built by
``significant_moves_pipeline.py``.  A row shift is therefore a wall-clock shift;
missing market bars remain missing instead of silently stretching a horizon.

The feature builders never inspect a future column.  Forward columns are added
in a separate function and are intended only for research labels/evaluation.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


BAR_MINUTES = 5
DEFAULT_HORIZON_MINUTES = 120


def _utc_timestamp(value: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def load_regular_bars(
    path: str | Path,
    instrument: str,
    *,
    pip_size: float,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    spread_multiplier: float = 1.0,
) -> pd.DataFrame:
    """Load cached bars and reconstruct conservative executable bid/ask OHLC.

    The cache stores native-or-estimated bid/ask closes and mid OHLC.  Bid/ask
    highs, lows, and opens are reconstructed with the contemporaneous spread.
    The provenance fields remain available so estimated-cost rows can be gated
    or stressed rather than mistaken for observed quotes.
    """

    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(source)
    frame = pd.read_parquet(source)
    if "time_utc" in frame.columns:
        frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True, errors="coerce")
        frame = frame.set_index("time_utc")
    else:
        frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
        frame.index.name = "time_utc"
    frame = frame.loc[~frame.index.isna()].sort_index()
    frame = frame.loc[~frame.index.duplicated(keep="last")].copy()
    start_ts = _utc_timestamp(start)
    end_ts = _utc_timestamp(end)
    if start_ts is not None:
        frame = frame.loc[frame.index >= start_ts]
    if end_ts is not None:
        frame = frame.loc[frame.index <= end_ts]

    required = {"open", "high", "low", "close", "spread_pips"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{source} is missing required columns: {missing}")
    for column in [
        "open",
        "high",
        "low",
        "close",
        "bid_close",
        "ask_close",
        "spread_pips",
        "volume",
        "source_m1_observations",
        "spread_estimated_fraction",
    ]:
        if column not in frame:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    spread = frame["spread_pips"].clip(lower=0.0) * float(spread_multiplier)
    half_price = spread * float(pip_size) / 2.0
    frame["spread_pips_used"] = spread
    frame["bid_close"] = frame["bid_close"].where(
        frame["bid_close"].notna(), frame["close"] - half_price
    )
    frame["ask_close"] = frame["ask_close"].where(
        frame["ask_close"].notna(), frame["close"] + half_price
    )
    for suffix in ("open", "high", "low"):
        frame[f"bid_{suffix}"] = frame[suffix] - half_price
        frame[f"ask_{suffix}"] = frame[suffix] + half_price

    frame["instrument"] = str(instrument).upper()
    frame["pip_size"] = float(pip_size)
    frame["bar_available"] = (
        frame[["open", "high", "low", "close"]].notna().all(axis=1)
        & frame["source_m1_observations"].fillna(0).gt(0)
    )
    frame.index.name = "time_utc"
    return frame


def _valid_difference(series: pd.Series, periods: int, available: pd.Series) -> pd.Series:
    result = series.diff(periods)
    return result.where(available & available.shift(periods, fill_value=False))


def _valid_rolling(series: pd.Series, window: int, available: pd.Series, op: str) -> pd.Series:
    clean = series.where(available)
    minimum = max(2, int(math.ceil(window * 0.8)))
    rolling = clean.rolling(window, min_periods=minimum)
    if op == "mean":
        return rolling.mean()
    if op == "sum":
        return rolling.sum()
    if op == "std":
        return rolling.std(ddof=0)
    if op == "min":
        return rolling.min()
    if op == "max":
        return rolling.max()
    raise ValueError(f"unsupported rolling operation: {op}")


def add_causal_features(
    bars: pd.DataFrame,
    *,
    round_trip_slippage_pips: float = 0.4,
) -> pd.DataFrame:
    """Add a compact, auditable feature set using information at or before t."""

    frame = bars.copy()
    available = frame["bar_available"].fillna(False).astype(bool)
    pip = float(pd.to_numeric(frame["pip_size"], errors="coerce").dropna().iloc[0])
    close = frame["close"].astype(float)
    previous = close.shift(1)
    true_range_pips = pd.concat(
        [
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - previous).abs(),
            (frame["low"] - previous).abs(),
        ],
        axis=1,
    ).max(axis=1) / pip
    true_range_pips = true_range_pips.where(available & available.shift(1, fill_value=False))
    frame["true_range_pips"] = true_range_pips
    for minutes in (15, 30, 60, 120, 240):
        bars_count = minutes // BAR_MINUTES
        frame[f"momentum_{minutes}_pips"] = _valid_difference(
            close, bars_count, available
        ) / pip

    ret_5 = _valid_difference(close, 1, available) / pip
    frame["return_5_pips"] = ret_5
    frame["abs_return_5_pips"] = ret_5.abs()
    for minutes in (15, 30, 60, 120, 240):
        window = minutes // BAR_MINUTES
        frame[f"atr_{minutes}_pips"] = _valid_rolling(
            true_range_pips, window, available, "mean"
        )
        frame[f"realized_abs_{minutes}_pips"] = _valid_rolling(
            ret_5.abs(), window, available, "sum"
        )
        frame[f"realized_vol_{minutes}_pips"] = _valid_rolling(
            ret_5, window, available, "std"
        )

    scale = frame["atr_240_pips"].replace(0.0, np.nan)
    for minutes in (15, 30, 60, 120):
        frame[f"momentum_{minutes}_atr"] = frame[f"momentum_{minutes}_pips"] / scale
    frame["volatility_expansion_30_240"] = (
        frame["atr_30_pips"] / scale
    )
    frame["acceleration_15_atr"] = (
        frame["momentum_15_pips"]
        - frame["momentum_15_pips"].shift(3)
    ) / scale

    for minutes in (60, 240):
        window = minutes // BAR_MINUTES
        rolling_low = _valid_rolling(close, window, available, "min")
        rolling_high = _valid_rolling(close, window, available, "max")
        width = (rolling_high - rolling_low).replace(0.0, np.nan)
        frame[f"range_position_{minutes}"] = (close - rolling_low) / width
        path = frame[f"realized_abs_{minutes}_pips"].replace(0.0, np.nan)
        displacement = frame[f"momentum_{minutes}_pips"].abs()
        frame[f"efficiency_{minutes}"] = (displacement / path).clip(0.0, 1.0)
        range_pips = width / pip
        frame[f"choppiness_{minutes}"] = (path / range_pips.replace(0.0, np.nan)).clip(
            lower=0.0
        )

    spread = frame["spread_pips_used"].astype(float)
    frame["round_trip_cost_pips"] = spread + float(round_trip_slippage_pips)
    frame["spread_to_atr_60"] = spread / frame["atr_60_pips"].replace(0.0, np.nan)
    # This is the validated simple movement-opportunity baseline: recent
    # realized path movement divided by the cost hurdle.  It predicts magnitude,
    # not direction.
    frame["movement_score"] = frame["realized_abs_30_pips"] / frame[
        "round_trip_cost_pips"
    ].replace(0.0, np.nan)

    index = pd.DatetimeIndex(frame.index)
    hour = index.hour + index.minute / 60.0
    weekday = index.dayofweek
    frame["hour_sin"] = np.sin(2.0 * np.pi * hour / 24.0)
    frame["hour_cos"] = np.cos(2.0 * np.pi * hour / 24.0)
    frame["weekday_sin"] = np.sin(2.0 * np.pi * weekday / 7.0)
    frame["weekday_cos"] = np.cos(2.0 * np.pi * weekday / 7.0)
    frame["is_asia"] = ((hour >= 22.0) | (hour < 7.0)).astype(np.int8)
    frame["is_london"] = ((hour >= 7.0) & (hour < 16.0)).astype(np.int8)
    frame["is_new_york"] = ((hour >= 12.0) & (hour < 21.0)).astype(np.int8)
    frame["is_london_ny_overlap"] = ((hour >= 12.0) & (hour < 16.0)).astype(np.int8)
    frame["is_rollover"] = ((hour >= 20.75) & (hour <= 22.25)).astype(np.int8)
    frame["is_weekend"] = (weekday >= 5).astype(np.int8)
    frame["feature_ready"] = available & frame[causal_feature_columns()].notna().all(axis=1)
    return frame.replace([np.inf, -np.inf], np.nan)


def add_exact_forward_labels(
    featured: pd.DataFrame,
    *,
    horizon_minutes: int = DEFAULT_HORIZON_MINUTES,
    round_trip_slippage_pips: float = 0.4,
) -> pd.DataFrame:
    """Add exact timestamp-aligned endpoint labels for retrospective research."""

    if horizon_minutes % BAR_MINUTES:
        raise ValueError("horizon_minutes must be a multiple of five")
    frame = featured.copy()
    periods = int(horizon_minutes // BAR_MINUTES)
    pip = float(pd.to_numeric(frame["pip_size"], errors="coerce").dropna().iloc[0])
    current_mid = frame["close"].astype(float)
    current_bid = frame["bid_close"].astype(float)
    current_ask = frame["ask_close"].astype(float)
    future_mid = current_mid.shift(-periods)
    future_bid = current_bid.shift(-periods)
    future_ask = current_ask.shift(-periods)
    future_available = frame["bar_available"].shift(-periods, fill_value=False).astype(bool)
    path_available = (
        frame["bar_available"]
        .astype(float)
        .rolling(periods + 1, min_periods=periods + 1)
        .sum()
        .shift(-periods)
    )
    coverage = path_available / float(periods + 1)
    frame["label_end_timestamp"] = pd.Series(frame.index, index=frame.index).shift(-periods)
    frame["forward_signed_pips"] = (future_mid - current_mid) / pip
    frame["forward_abs_pips"] = frame["forward_signed_pips"].abs()
    frame["forward_signed_return"] = future_mid / current_mid - 1.0
    frame["forward_abs_return"] = frame["forward_signed_return"].abs()
    frame["forward_long_net_pips"] = (
        (future_bid - current_ask) / pip - float(round_trip_slippage_pips)
    )
    frame["forward_short_net_pips"] = (
        (current_bid - future_ask) / pip - float(round_trip_slippage_pips)
    )
    frame["forward_oracle_net_pips"] = frame[
        ["forward_long_net_pips", "forward_short_net_pips"]
    ].max(axis=1)
    frame["forward_path_coverage"] = coverage
    frame["label_ready"] = (
        frame["bar_available"].astype(bool)
        & future_available
        & coverage.ge(0.8)
        & frame["forward_signed_pips"].notna()
    )
    frame["label_is_hindsight"] = True
    return frame


def causal_feature_columns() -> list[str]:
    return [
        "momentum_15_atr",
        "momentum_30_atr",
        "momentum_60_atr",
        "momentum_120_atr",
        "acceleration_15_atr",
        "volatility_expansion_30_240",
        "realized_abs_30_pips",
        "realized_abs_60_pips",
        "realized_vol_30_pips",
        "realized_vol_120_pips",
        "range_position_60",
        "range_position_240",
        "efficiency_60",
        "efficiency_240",
        "choppiness_60",
        "choppiness_240",
        "spread_to_atr_60",
        "movement_score",
        "hour_sin",
        "hour_cos",
        "weekday_sin",
        "weekday_cos",
        "is_asia",
        "is_london",
        "is_new_york",
        "is_london_ny_overlap",
        "is_rollover",
    ]


def assert_causal_feature_names(columns: Iterable[str]) -> None:
    forbidden_tokens = ("future", "forward", "label", "target", "oracle", "outcome")
    rejected = [
        str(column)
        for column in columns
        if any(token in str(column).lower() for token in forbidden_tokens)
    ]
    if rejected:
        raise ValueError(f"non-causal feature columns rejected: {sorted(rejected)}")

