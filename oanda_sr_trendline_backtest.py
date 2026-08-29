#!/usr/bin/env python3
"""Backtest a support/resistance plus trendline break-retest strategy.

The manual strategy is discretionary, so this script makes each step explicit:

- resample local OANDA M1 candles to a configurable signal timeframe;
- require a strong prior trend on that chart;
- build a trendline from confirmed pivot highs/lows with at least 3 touches;
- wait for a close through that trendline;
- identify the nearest prior pivot resistance/support;
- wait for a close through that horizontal level;
- enter on a retest zone around the level;
- use the recent pullback high/low as the stop and a fixed 1:2 target.

Outputs are written to:

    data/oanda_training_manager/reports/sr_trendline_<timeframe>

This is a bar-based research harness using mid OHLC. Historical spread is not
available for the whole dataset, so execution cost is an optional bps input.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
REPORT_BASE_ROOT = ROOT / "data" / "oanda_training_manager" / "reports"

MAJOR_CURRENCIES = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
EXOTIC_CURRENCIES = {
    "CNH",
    "CZK",
    "DKK",
    "HKD",
    "HUF",
    "MXN",
    "NOK",
    "PLN",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "ZAR",
}
PIP_LOCATION_MINUS2 = {
    "AUD_JPY",
    "CAD_JPY",
    "CHF_JPY",
    "EUR_HUF",
    "EUR_JPY",
    "GBP_JPY",
    "HKD_JPY",
    "NZD_JPY",
    "SGD_JPY",
    "TRY_JPY",
    "USD_HUF",
    "USD_JPY",
    "USD_THB",
    "ZAR_JPY",
}


@dataclass(frozen=True)
class StrategyConfig:
    name: str = "sr_trendline_m30_default"
    timeframe: str = "30min"
    pivot_left: int = 2
    pivot_right: int = 2
    atr_window: int = 14
    ema_fast: int = 20
    ema_slow: int = 50
    trendline_lookback: int = 96
    sr_lookback: int = 144
    trend_strength_lookback: int = 48
    min_trend_atr_move: float = 1.75
    min_trendline_touches: int = 3
    max_line_pivots: int = 8
    min_line_span_bars: int = 18
    touch_atr_mult: float = 0.40
    break_atr_mult: float = 0.08
    max_level_distance_atr: float = 3.50
    level_break_max_bars: int = 24
    pullback_max_bars: int = 24
    retest_atr_mult: float = 0.15
    stop_buffer_atr: float = 0.10
    min_stop_atr: float = 0.25
    max_stop_atr: float = 2.75
    reward_r: float = 2.0
    max_hold_bars: int = 96
    round_turn_cost_bps: float = 0.0
    skip_friday_after_hour_utc: int = 20
    direction_mode: str = "both"


@dataclass(frozen=True)
class TrendlineBreak:
    direction: int
    line_slope: float
    line_intercept: float
    line_value: float
    line_touches: int
    line_touch_error_atr: float
    line_first_pos: int
    line_last_pos: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def json_safe(value: Any) -> Any:
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_safe), encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def discover_pairs() -> list[str]:
    pairs = [path.name[: -len("_M1.csv")] for path in CANDLE_ROOT.glob("*_M1.csv")]
    return sorted(pair for pair in pairs if "_" in pair)


def split_pair(pair: str) -> tuple[str, str]:
    base, quote = pair.split("_", 1)
    return base, quote


def pair_group(pair: str) -> str:
    base, quote = split_pair(pair)
    if base in EXOTIC_CURRENCIES or quote in EXOTIC_CURRENCIES:
        return "exotic_or_regional"
    if "USD" in (base, quote):
        return "usd_major"
    if base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES:
        return "major_cross"
    return "regional_cross"


def pip_multiplier(pair: str) -> float:
    return 100.0 if pair in PIP_LOCATION_MINUS2 else 10_000.0


def timeframe_slug(timeframe: str) -> str:
    return (
        str(timeframe)
        .strip()
        .lower()
        .replace("minutes", "min")
        .replace("minute", "min")
        .replace("hours", "h")
        .replace("hour", "h")
        .replace(" ", "")
    )


def report_root_for_timeframe(timeframe: str) -> Path:
    return REPORT_BASE_ROOT / f"sr_trendline_{timeframe_slug(timeframe)}"


def config_for_timeframe(timeframe: str, *, cost_bps: float) -> StrategyConfig:
    slug = timeframe_slug(timeframe)
    return StrategyConfig(
        name=f"sr_trendline_{slug}_default",
        timeframe=timeframe,
        round_turn_cost_bps=float(cost_bps),
    )


def apply_config_overrides(cfg: StrategyConfig, args: argparse.Namespace) -> StrategyConfig:
    updates: dict[str, Any] = {}
    optional_float_fields = [
        "min_trend_atr_move",
        "touch_atr_mult",
        "break_atr_mult",
        "max_level_distance_atr",
        "retest_atr_mult",
        "stop_buffer_atr",
        "min_stop_atr",
        "max_stop_atr",
        "reward_r",
    ]
    optional_int_fields = [
        "trendline_lookback",
        "sr_lookback",
        "trend_strength_lookback",
        "min_trendline_touches",
        "max_line_pivots",
        "min_line_span_bars",
        "level_break_max_bars",
        "pullback_max_bars",
        "max_hold_bars",
    ]
    for field in optional_float_fields:
        value = getattr(args, field, None)
        if value is not None:
            updates[field] = float(value)
    for field in optional_int_fields:
        value = getattr(args, field, None)
        if value is not None:
            updates[field] = int(value)
    if getattr(args, "direction_mode", None):
        updates["direction_mode"] = str(args.direction_mode)
    return replace(cfg, **updates) if updates else cfg


def load_pair_bars(pair: str, cfg: StrategyConfig, *, start: str = "", end: str = "") -> pd.DataFrame:
    path = CANDLE_ROOT / f"{pair}_M1.csv"
    usecols = ["datetime", "open", "high", "low", "close", "volume", "spread_pips"]
    frame = pd.read_csv(path, usecols=usecols)
    frame["time_utc"] = pd.to_datetime(frame["datetime"], errors="coerce", utc=True)
    for column in ["open", "high", "low", "close", "volume", "spread_pips"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["time_utc", "open", "high", "low", "close"])
    if start:
        frame = frame[frame["time_utc"] >= pd.Timestamp(start, tz="UTC")]
    if end:
        frame = frame[frame["time_utc"] <= pd.Timestamp(end, tz="UTC")]
    if frame.empty:
        return pd.DataFrame()
    frame = (
        frame.sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    bars = frame.resample(cfg.timeframe, label="right", closed="right").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "spread_pips": "mean",
        }
    )
    bars = bars.dropna(subset=["open", "high", "low", "close"])
    return bars.astype({"open": "float64", "high": "float64", "low": "float64", "close": "float64"})


def true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    ranges = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def pivot_flags(series: pd.Series, *, left: int, right: int, high: bool) -> pd.Series:
    flags = pd.Series(True, index=series.index)
    for offset in range(1, left + 1):
        if high:
            flags &= series > series.shift(offset)
        else:
            flags &= series < series.shift(offset)
    for offset in range(1, right + 1):
        if high:
            flags &= series >= series.shift(-offset)
        else:
            flags &= series <= series.shift(-offset)
    return flags.fillna(False)


def add_features(bars: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    out = bars.copy()
    out["atr"] = true_range(out).ewm(
        alpha=1.0 / float(cfg.atr_window),
        adjust=False,
        min_periods=cfg.atr_window,
    ).mean()
    out["ema_fast"] = out["close"].ewm(span=cfg.ema_fast, adjust=False, min_periods=cfg.ema_fast).mean()
    out["ema_slow"] = out["close"].ewm(span=cfg.ema_slow, adjust=False, min_periods=cfg.ema_slow).mean()
    out["pivot_high"] = pivot_flags(out["high"], left=cfg.pivot_left, right=cfg.pivot_right, high=True)
    out["pivot_low"] = pivot_flags(out["low"], left=cfg.pivot_left, right=cfg.pivot_right, high=False)
    return out


def entry_time_allowed(ts: pd.Timestamp, cfg: StrategyConfig) -> bool:
    if ts.dayofweek >= 5:
        return False
    if ts.dayofweek == 4 and ts.hour >= cfg.skip_friday_after_hour_utc:
        return False
    return True


def strong_prior_trend(direction: int, i: int, data: dict[str, np.ndarray], cfg: StrategyConfig) -> bool:
    ref = i - 1
    lookback = cfg.trend_strength_lookback
    if ref - lookback < 0:
        return False
    close = data["close"]
    ema_fast = data["ema_fast"]
    ema_slow = data["ema_slow"]
    atr = data["atr"]
    if not all(math.isfinite(float(v)) for v in [close[ref], close[ref - lookback], ema_fast[ref], ema_slow[ref], atr[ref]]):
        return False
    if atr[ref] <= 0.0:
        return False
    move_atr = (close[ref] - close[ref - lookback]) / atr[ref]
    ema_slope = ema_slow[ref] - ema_slow[max(0, ref - min(lookback, 24))]
    if direction > 0:
        return (
            ema_fast[ref] < ema_slow[ref]
            and ema_slope < 0.0
            and move_atr <= -cfg.min_trend_atr_move
        )
    return (
        ema_fast[ref] > ema_slow[ref]
        and ema_slope > 0.0
        and move_atr >= cfg.min_trend_atr_move
    )


def fit_recent_trendline(
    direction: int,
    i: int,
    pivot_positions: np.ndarray,
    pivot_prices: np.ndarray,
    data: dict[str, np.ndarray],
    cfg: StrategyConfig,
) -> TrendlineBreak | None:
    atr = finite_float(data["atr"][i], 0.0)
    if atr <= 0.0:
        return None
    max_confirmed = i - cfg.pivot_right
    min_pos = max(0, i - cfg.trendline_lookback)
    mask = (pivot_positions >= min_pos) & (pivot_positions <= max_confirmed)
    positions = pivot_positions[mask]
    prices = pivot_prices[mask]
    if len(positions) < cfg.min_trendline_touches:
        return None
    positions = positions[-cfg.max_line_pivots :]
    prices = prices[-cfg.max_line_pivots :]
    if int(positions[-1] - positions[0]) < cfg.min_line_span_bars:
        return None

    x = positions.astype(float)
    y = prices.astype(float)
    slope, intercept = np.polyfit(x, y, 1)
    if direction > 0 and slope >= 0.0:
        return None
    if direction < 0 and slope <= 0.0:
        return None

    line_at_pivots = slope * x + intercept
    errors = np.abs(y - line_at_pivots)
    tolerance = max(cfg.touch_atr_mult * atr, np.finfo(float).eps)
    touch_count = int(np.sum(errors <= tolerance))
    if touch_count < cfg.min_trendline_touches:
        return None

    close = data["close"]
    line_prev = slope * float(i - 1) + intercept
    line_now = slope * float(i) + intercept
    break_buffer = cfg.break_atr_mult * atr
    if direction > 0:
        broke = close[i - 1] <= line_prev + break_buffer and close[i] > line_now + break_buffer
    else:
        broke = close[i - 1] >= line_prev - break_buffer and close[i] < line_now - break_buffer
    if not broke:
        return None

    return TrendlineBreak(
        direction=direction,
        line_slope=float(slope),
        line_intercept=float(intercept),
        line_value=float(line_now),
        line_touches=touch_count,
        line_touch_error_atr=float(np.nanmean(errors) / atr),
        line_first_pos=int(positions[0]),
        line_last_pos=int(positions[-1]),
    )


def detect_trendline_break(
    i: int,
    high_pivots: tuple[np.ndarray, np.ndarray],
    low_pivots: tuple[np.ndarray, np.ndarray],
    data: dict[str, np.ndarray],
    cfg: StrategyConfig,
) -> TrendlineBreak | None:
    if i <= max(cfg.trendline_lookback, cfg.ema_slow + cfg.trend_strength_lookback, cfg.atr_window + 5):
        return None
    direction_mode = str(cfg.direction_mode).lower()
    allow_long = direction_mode in {"both", "long", "long_only"}
    allow_short = direction_mode in {"both", "short", "short_only"}
    if allow_long and strong_prior_trend(1, i, data, cfg):
        long_break = fit_recent_trendline(1, i, high_pivots[0], high_pivots[1], data, cfg)
        if long_break is not None:
            return long_break
    if allow_short and strong_prior_trend(-1, i, data, cfg):
        short_break = fit_recent_trendline(-1, i, low_pivots[0], low_pivots[1], data, cfg)
        if short_break is not None:
            return short_break
    return None


def nearest_horizontal_level(
    direction: int,
    i: int,
    high_pivots: tuple[np.ndarray, np.ndarray],
    low_pivots: tuple[np.ndarray, np.ndarray],
    data: dict[str, np.ndarray],
    cfg: StrategyConfig,
) -> tuple[float, int] | None:
    close = finite_float(data["close"][i], 0.0)
    atr = finite_float(data["atr"][i], 0.0)
    if close <= 0.0 or atr <= 0.0:
        return None
    max_confirmed = i - cfg.pivot_right
    min_pos = max(0, i - cfg.sr_lookback)
    break_buffer = cfg.break_atr_mult * atr
    if direction > 0:
        positions, prices = high_pivots
        mask = (positions >= min_pos) & (positions <= max_confirmed) & (prices > close + break_buffer)
        if not np.any(mask):
            return None
        candidate_positions = positions[mask]
        candidate_prices = prices[mask]
        idx = int(np.argmin(candidate_prices - close))
        level = float(candidate_prices[idx])
        if (level - close) / atr > cfg.max_level_distance_atr:
            return None
        return level, int(candidate_positions[idx])

    positions, prices = low_pivots
    mask = (positions >= min_pos) & (positions <= max_confirmed) & (prices < close - break_buffer)
    if not np.any(mask):
        return None
    candidate_positions = positions[mask]
    candidate_prices = prices[mask]
    idx = int(np.argmin(close - candidate_prices))
    level = float(candidate_prices[idx])
    if (close - level) / atr > cfg.max_level_distance_atr:
        return None
    return level, int(candidate_positions[idx])


def find_level_break(
    direction: int,
    start_pos: int,
    level: float,
    data: dict[str, np.ndarray],
    cfg: StrategyConfig,
) -> int | None:
    close = data["close"]
    atr = data["atr"]
    stop_pos = min(len(close) - 1, start_pos + cfg.level_break_max_bars)
    for pos in range(start_pos, stop_pos + 1):
        if not math.isfinite(float(atr[pos])) or atr[pos] <= 0.0:
            continue
        buffer = cfg.break_atr_mult * atr[pos]
        if direction > 0 and close[pos] > level + buffer:
            return pos
        if direction < 0 and close[pos] < level - buffer:
            return pos
    return None


def find_pullback_entry(
    direction: int,
    start_pos: int,
    level: float,
    data: dict[str, np.ndarray],
    cfg: StrategyConfig,
) -> tuple[int, float, float, float] | None:
    open_ = data["open"]
    high = data["high"]
    low = data["low"]
    close = data["close"]
    atr = data["atr"]
    stop_pos = min(len(close) - 2, start_pos + cfg.pullback_max_bars)
    for pos in range(start_pos, stop_pos + 1):
        bar_atr = finite_float(atr[pos], 0.0)
        if bar_atr <= 0.0:
            continue
        zone = cfg.retest_atr_mult * bar_atr
        stop_buffer = cfg.stop_buffer_atr * bar_atr
        entry_pos = pos + 1
        entry = finite_float(open_[entry_pos], 0.0)
        if entry <= 0.0:
            continue
        if direction > 0:
            touched = low[pos] <= level + zone and close[pos] >= level - zone
            if not touched:
                continue
            pullback_low = float(np.nanmin(low[start_pos : pos + 1]))
            stop = pullback_low - stop_buffer
            min_stop = entry - cfg.min_stop_atr * bar_atr
            stop = min(stop, min_stop)
            risk = entry - stop
            if risk <= 0.0 or risk > cfg.max_stop_atr * bar_atr:
                continue
            target = entry + cfg.reward_r * risk
            return entry_pos, float(entry), float(stop), float(target)

        touched = high[pos] >= level - zone and close[pos] <= level + zone
        if not touched:
            continue
        pullback_high = float(np.nanmax(high[start_pos : pos + 1]))
        stop = pullback_high + stop_buffer
        min_stop = entry + cfg.min_stop_atr * bar_atr
        stop = max(stop, min_stop)
        risk = stop - entry
        if risk <= 0.0 or risk > cfg.max_stop_atr * bar_atr:
            continue
        target = entry - cfg.reward_r * risk
        return entry_pos, float(entry), float(stop), float(target)
    return None


def simulate_exit(
    direction: int,
    entry_pos: int,
    entry: float,
    stop: float,
    target: float,
    data: dict[str, np.ndarray],
    cfg: StrategyConfig,
) -> tuple[int, float, str]:
    high = data["high"]
    low = data["low"]
    close = data["close"]
    stop_pos = min(len(close) - 1, entry_pos + cfg.max_hold_bars)
    exit_pos = stop_pos
    exit_price = finite_float(close[stop_pos], entry)
    exit_reason = "time_stop"
    for pos in range(entry_pos, stop_pos + 1):
        if direction > 0:
            if low[pos] <= stop:
                return pos, float(stop), "stop_loss"
            if high[pos] >= target:
                return pos, float(target), "take_profit"
        else:
            if high[pos] >= stop:
                return pos, float(stop), "stop_loss"
            if low[pos] <= target:
                return pos, float(target), "take_profit"
    return exit_pos, float(exit_price), exit_reason


def dataframe_arrays(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    return {
        "open": frame["open"].to_numpy(dtype=float),
        "high": frame["high"].to_numpy(dtype=float),
        "low": frame["low"].to_numpy(dtype=float),
        "close": frame["close"].to_numpy(dtype=float),
        "atr": frame["atr"].to_numpy(dtype=float),
        "ema_fast": frame["ema_fast"].to_numpy(dtype=float),
        "ema_slow": frame["ema_slow"].to_numpy(dtype=float),
    }


def pivot_arrays(frame: pd.DataFrame) -> tuple[tuple[np.ndarray, np.ndarray], tuple[np.ndarray, np.ndarray]]:
    positions = np.arange(len(frame), dtype=int)
    high_mask = frame["pivot_high"].to_numpy(dtype=bool)
    low_mask = frame["pivot_low"].to_numpy(dtype=bool)
    high_pivots = (positions[high_mask], frame["high"].to_numpy(dtype=float)[high_mask])
    low_pivots = (positions[low_mask], frame["low"].to_numpy(dtype=float)[low_mask])
    return high_pivots, low_pivots


def backtest_pair(pair: str, bars: pd.DataFrame, cfg: StrategyConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    features = add_features(bars, cfg)
    data = dataframe_arrays(features)
    high_pivots, low_pivots = pivot_arrays(features)
    times = features.index
    n = len(features)
    multiplier = pip_multiplier(pair)
    trades: list[dict[str, Any]] = []
    stats: dict[str, Any] = {
        "instrument": pair,
        "group": pair_group(pair),
        "bar_count": int(n),
        "bar_start_utc": times[0].isoformat() if n else "",
        "bar_end_utc": times[-1].isoformat() if n else "",
        "trendline_breaks": 0,
        "horizontal_level_found": 0,
        "horizontal_level_breaks": 0,
        "pullback_entries": 0,
    }
    i = max(cfg.trendline_lookback, cfg.ema_slow + cfg.trend_strength_lookback, cfg.atr_window + 5)
    while i < n - 2:
        ts = times[i]
        if not entry_time_allowed(ts, cfg):
            i += 1
            continue
        setup = detect_trendline_break(i, high_pivots, low_pivots, data, cfg)
        if setup is None:
            i += 1
            continue
        stats["trendline_breaks"] += 1
        level_result = nearest_horizontal_level(setup.direction, i, high_pivots, low_pivots, data, cfg)
        if level_result is None:
            i += 1
            continue
        stats["horizontal_level_found"] += 1
        level, level_source_pos = level_result
        level_break_pos = find_level_break(setup.direction, i + 1, level, data, cfg)
        if level_break_pos is None:
            i += 1
            continue
        stats["horizontal_level_breaks"] += 1
        entry_result = find_pullback_entry(setup.direction, level_break_pos + 1, level, data, cfg)
        if entry_result is None:
            i = level_break_pos + 1
            continue
        entry_pos, entry_price, stop_price, target_price = entry_result
        if not entry_time_allowed(times[entry_pos], cfg):
            i = entry_pos + 1
            continue
        stats["pullback_entries"] += 1
        exit_pos, exit_price, exit_reason = simulate_exit(
            setup.direction,
            entry_pos,
            entry_price,
            stop_price,
            target_price,
            data,
            cfg,
        )
        risk = abs(entry_price - stop_price)
        gross_return = setup.direction * ((exit_price - entry_price) / entry_price)
        net_return = gross_return - cfg.round_turn_cost_bps / 10_000.0
        risk_return = risk / entry_price if entry_price > 0.0 else 0.0
        gross_r = gross_return / risk_return if risk_return > 0.0 else 0.0
        net_r = net_return / risk_return if risk_return > 0.0 else 0.0
        gross_pips = setup.direction * (exit_price - entry_price) * multiplier
        cost_pips = (cfg.round_turn_cost_bps / 10_000.0) * entry_price * multiplier
        trades.append(
            {
                "config": cfg.name,
                "instrument": pair,
                "group": pair_group(pair),
                "entry_time": times[entry_pos].isoformat(),
                "exit_time": times[exit_pos].isoformat(),
                "direction": "LONG" if setup.direction > 0 else "SHORT",
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "support_resistance_level": level,
                "level_source_time": times[level_source_pos].isoformat(),
                "trendline_break_time": times[i].isoformat(),
                "level_break_time": times[level_break_pos].isoformat(),
                "line_touches": setup.line_touches,
                "line_slope": setup.line_slope,
                "line_touch_error_atr": setup.line_touch_error_atr,
                "line_first_time": times[setup.line_first_pos].isoformat(),
                "line_last_time": times[setup.line_last_pos].isoformat(),
                "atr": finite_float(data["atr"][entry_pos], 0.0),
                "risk_price": risk,
                "gross_return_bps": gross_return * 10_000.0,
                "net_return_bps": net_return * 10_000.0,
                "gross_pips": gross_pips,
                "cost_pips": cost_pips,
                "net_pips": gross_pips - cost_pips,
                "gross_r": gross_r,
                "net_r": net_r,
                "hold_bars": int(max(0, exit_pos - entry_pos + 1)),
                "trendline_to_level_bars": int(max(0, level_break_pos - i)),
                "level_break_to_entry_bars": int(max(0, entry_pos - level_break_pos)),
            }
        )
        i = exit_pos + 1
    stats["trade_count"] = len(trades)
    return trades, stats


def summarize_trades(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {
            "trade_count": 0,
            "total_net_bps": 0.0,
            "avg_net_bps": 0.0,
            "median_net_bps": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_net_r": 0.0,
            "avg_net_r": 0.0,
            "median_net_r": 0.0,
            "max_drawdown_r": 0.0,
            "total_net_pips": 0.0,
            "avg_hold_bars": 0.0,
        }
    bps = np.array([finite_float(t.get("net_return_bps")) for t in trades], dtype=float)
    r_vals = np.array([finite_float(t.get("net_r")) for t in trades], dtype=float)
    pips = np.array([finite_float(t.get("net_pips")) for t in trades], dtype=float)
    wins = r_vals[r_vals > 0.0]
    losses = -r_vals[r_vals < 0.0]
    equity = np.cumsum(r_vals)
    running_peak = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:]
    drawdown = running_peak - equity
    return {
        "trade_count": int(len(trades)),
        "total_net_bps": float(bps.sum()),
        "avg_net_bps": float(bps.mean()),
        "median_net_bps": float(np.median(bps)),
        "win_rate": float((r_vals > 0.0).mean()),
        "profit_factor": float(wins.sum() / losses.sum()) if losses.sum() > 0.0 else float("inf"),
        "total_net_r": float(r_vals.sum()),
        "avg_net_r": float(r_vals.mean()),
        "median_net_r": float(np.median(r_vals)),
        "max_drawdown_r": float(drawdown.max()) if len(drawdown) else 0.0,
        "total_net_pips": float(pips.sum()),
        "avg_hold_bars": float(np.mean([finite_float(t.get("hold_bars")) for t in trades])),
    }


def split_trades(trades: Sequence[dict[str, Any]], split_time: pd.Timestamp) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    train: list[dict[str, Any]] = []
    test: list[dict[str, Any]] = []
    for trade in trades:
        entry = pd.Timestamp(str(trade["entry_time"]))
        if entry.tzinfo is None:
            entry = entry.tz_localize("UTC")
        if entry <= split_time:
            train.append(trade)
        else:
            test.append(trade)
    return train, test


def pair_summary_rows(
    trades: Sequence[dict[str, Any]],
    pair_stats: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_pair: dict[str, list[dict[str, Any]]] = {str(row["instrument"]): [] for row in pair_stats}
    for trade in trades:
        by_pair.setdefault(str(trade["instrument"]), []).append(trade)
    rows: list[dict[str, Any]] = []
    for stats in pair_stats:
        pair = str(stats["instrument"])
        rows.append({**stats, **summarize_trades(by_pair.get(pair, []))})
    return rows


def monthly_summary_rows(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        entry = pd.Timestamp(str(trade["entry_time"]))
        month = entry.strftime("%Y-%m")
        buckets.setdefault(month, []).append(trade)
    rows: list[dict[str, Any]] = []
    for month in sorted(buckets):
        rows.append({"month": month, **summarize_trades(buckets[month])})
    return rows


def top_rows(rows: Sequence[dict[str, Any]], *, min_trades: int, reverse: bool, limit: int) -> list[dict[str, Any]]:
    viable = [row for row in rows if int(row.get("trade_count", 0)) >= min_trades]
    return sorted(viable, key=lambda row: finite_float(row.get("total_net_r"), 0.0), reverse=reverse)[:limit]


def format_float(value: Any, digits: int = 2) -> str:
    number = finite_float(value, 0.0)
    if math.isinf(number):
        return "inf"
    return f"{number:.{digits}f}"


def markdown_report(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    train = payload["train_summary"]
    test = payload["test_summary"]
    top_pairs = payload["top_pairs"]
    bottom_pairs = payload["bottom_pairs"]
    lines = [
        f"# Support/Resistance + Trendline {payload['config']['timeframe']} Backtest",
        "",
        f"Generated: {payload['generated_at_utc']}",
        "",
        "## Setup",
        "",
        f"- Pairs tested: `{payload['run_pair_count']}` of `{payload['pair_count']}` local OANDA pairs.",
        f"- Data range: `{payload['bar_start_utc']}` to `{payload['bar_end_utc']}`.",
        f"- Timeframe: `{payload['config']['timeframe']}`.",
        f"- Trendline: confirmed pivots, at least `{payload['config']['min_trendline_touches']}` touches.",
        f"- Entry: horizontal level break, then pullback into `{payload['config']['retest_atr_mult']}` ATR retest zone.",
        f"- Exit: stop beyond pullback high/low and `{payload['config']['reward_r']}:1` target.",
        f"- Round-turn cost: `{payload['config']['round_turn_cost_bps']}` bps.",
        "",
        "## Full Period",
        "",
        f"- Trades: `{summary['trade_count']}`",
        f"- Total net: `{format_float(summary['total_net_r'])}` R / `{format_float(summary['total_net_pips'])}` pips / `{format_float(summary['total_net_bps'])}` bps",
        f"- Avg trade: `{format_float(summary['avg_net_r'], 3)}` R / `{format_float(summary['avg_net_bps'], 3)}` bps",
        f"- Win rate: `{format_float(100.0 * summary['win_rate'])}%`",
        f"- Profit factor: `{format_float(summary['profit_factor'])}`",
        f"- Max drawdown: `{format_float(summary['max_drawdown_r'])}` R",
        "",
        "## Chronological Split",
        "",
        f"- Split time: `{payload['split_time_utc']}`",
        f"- Train: `{train['trade_count']}` trades, `{format_float(train['total_net_r'])}` R, `{format_float(100.0 * train['win_rate'])}%` win, PF `{format_float(train['profit_factor'])}`",
        f"- Test: `{test['trade_count']}` trades, `{format_float(test['total_net_r'])}` R, `{format_float(100.0 * test['win_rate'])}%` win, PF `{format_float(test['profit_factor'])}`",
        "",
    ]
    if top_pairs:
        lines.extend(["## Best Pairs", "", "| pair | group | trades | total R | win % | PF | max DD R |", "|---|---|---:|---:|---:|---:|---:|"])
        for row in top_pairs:
            lines.append(
                f"| {row['instrument']} | {row['group']} | {int(row['trade_count'])} | "
                f"{format_float(row['total_net_r'])} | {format_float(100.0 * row['win_rate'])}% | "
                f"{format_float(row['profit_factor'])} | {format_float(row['max_drawdown_r'])} |"
            )
        lines.append("")
    if bottom_pairs:
        lines.extend(["## Worst Pairs", "", "| pair | group | trades | total R | win % | PF | max DD R |", "|---|---|---:|---:|---:|---:|---:|"])
        for row in bottom_pairs:
            lines.append(
                f"| {row['instrument']} | {row['group']} | {int(row['trade_count'])} | "
                f"{format_float(row['total_net_r'])} | {format_float(100.0 * row['win_rate'])}% | "
                f"{format_float(row['profit_factor'])} | {format_float(row['max_drawdown_r'])} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Files",
            "",
            f"- JSON: `{payload['json_path']}`",
            f"- Trades CSV: `{payload['trades_csv']}`",
            f"- Pair summary CSV: `{payload['pair_summary_csv']}`",
            f"- Monthly summary CSV: `{payload['monthly_summary_csv']}`",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe", default="30min", help="Signal timeframe, e.g. 5min, 15min, 30min, 1h, 4h, 1D.")
    parser.add_argument("--target-pair", default="", help="Optional single pair, e.g. EUR_USD.")
    parser.add_argument("--max-pairs", type=int, default=0, help="Optional first-N pair cap for smoke tests.")
    parser.add_argument("--start", default="", help="Optional inclusive UTC start, e.g. 2025-01-01.")
    parser.add_argument("--end", default="", help="Optional inclusive UTC end, e.g. 2026-01-01.")
    parser.add_argument("--cost-bps", type=float, default=0.0, help="Round-turn cost deducted from each trade.")
    parser.add_argument("--direction-mode", choices=["both", "long", "short"], default="both")
    parser.add_argument("--min-trend-atr-move", type=float, default=None)
    parser.add_argument("--touch-atr-mult", type=float, default=None)
    parser.add_argument("--break-atr-mult", type=float, default=None)
    parser.add_argument("--max-level-distance-atr", type=float, default=None)
    parser.add_argument("--retest-atr-mult", type=float, default=None)
    parser.add_argument("--stop-buffer-atr", type=float, default=None)
    parser.add_argument("--min-stop-atr", type=float, default=None)
    parser.add_argument("--max-stop-atr", type=float, default=None)
    parser.add_argument("--reward-r", type=float, default=None)
    parser.add_argument("--trendline-lookback", type=int, default=None)
    parser.add_argument("--sr-lookback", type=int, default=None)
    parser.add_argument("--trend-strength-lookback", type=int, default=None)
    parser.add_argument("--min-trendline-touches", type=int, default=None)
    parser.add_argument("--max-line-pivots", type=int, default=None)
    parser.add_argument("--min-line-span-bars", type=int, default=None)
    parser.add_argument("--level-break-max-bars", type=int, default=None)
    parser.add_argument("--pullback-max-bars", type=int, default=None)
    parser.add_argument("--max-hold-bars", type=int, default=None)
    parser.add_argument("--test-fraction", type=float, default=0.30, help="Chronological test fraction.")
    parser.add_argument("--min-pair-trades", type=int, default=5, help="Minimum trades for best/worst pair tables.")
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = discover_pairs()
    if not pairs:
        raise RuntimeError(f"No *_M1.csv files found in {CANDLE_ROOT}")
    run_pairs = pairs
    if args.target_pair:
        target = args.target_pair.upper().replace("/", "_")
        if target not in pairs:
            raise ValueError(f"Target pair {target} not found under {CANDLE_ROOT}")
        run_pairs = [target]
    if args.max_pairs and args.max_pairs > 0:
        run_pairs = run_pairs[: args.max_pairs]

    cfg = apply_config_overrides(config_for_timeframe(args.timeframe, cost_bps=float(args.cost_bps)), args)
    all_trades: list[dict[str, Any]] = []
    pair_stats: list[dict[str, Any]] = []
    bar_starts: list[pd.Timestamp] = []
    bar_ends: list[pd.Timestamp] = []
    for index, pair in enumerate(run_pairs, start=1):
        print(f"[pair] {index:02d}/{len(run_pairs):02d} {pair}")
        bars = load_pair_bars(pair, cfg, start=args.start, end=args.end)
        if bars.empty:
            pair_stats.append(
                {
                    "instrument": pair,
                    "group": pair_group(pair),
                    "bar_count": 0,
                    "bar_start_utc": "",
                    "bar_end_utc": "",
                    "trendline_breaks": 0,
                    "horizontal_level_found": 0,
                    "horizontal_level_breaks": 0,
                    "pullback_entries": 0,
                    "trade_count": 0,
                }
            )
            continue
        bar_starts.append(pd.Timestamp(bars.index[0]))
        bar_ends.append(pd.Timestamp(bars.index[-1]))
        trades, stats = backtest_pair(pair, bars, cfg)
        all_trades.extend(trades)
        pair_stats.append(stats)
        print(
            f"[pair] {pair} bars={len(bars):,} trendline_breaks={stats['trendline_breaks']} "
            f"level_breaks={stats['horizontal_level_breaks']} trades={len(trades)}"
        )

    if bar_starts and bar_ends:
        global_start = min(bar_starts)
        global_end = max(bar_ends)
        split_time = global_start + (global_end - global_start) * (1.0 - max(0.0, min(float(args.test_fraction), 0.95)))
    else:
        global_start = pd.Timestamp.now(tz="UTC")
        global_end = global_start
        split_time = global_start
    train_trades, test_trades = split_trades(all_trades, split_time)
    pair_rows = pair_summary_rows(all_trades, pair_stats)
    monthly_rows = monthly_summary_rows(all_trades)
    top_pairs = top_rows(pair_rows, min_trades=args.min_pair_trades, reverse=True, limit=12)
    bottom_pairs = top_rows(pair_rows, min_trades=args.min_pair_trades, reverse=False, limit=12)

    output_dir = args.output_dir if args.output_dir is not None else report_root_for_timeframe(cfg.timeframe)
    json_path = output_dir / "latest_sr_trendline_backtest.json"
    trades_csv = output_dir / "latest_sr_trendline_trades.csv"
    pair_summary_csv = output_dir / "latest_sr_trendline_pair_summary.csv"
    monthly_summary_csv = output_dir / "latest_sr_trendline_monthly_summary.csv"
    summary_md = output_dir / "latest_sr_trendline_summary.md"
    payload: dict[str, Any] = {
        "generated_at_utc": utc_now(),
        "pair_count": len(pairs),
        "run_pair_count": len(run_pairs),
        "target_pair": args.target_pair,
        "bar_start_utc": global_start.isoformat(),
        "bar_end_utc": global_end.isoformat(),
        "split_time_utc": split_time.isoformat(),
        "config": asdict(cfg),
        "summary": summarize_trades(all_trades),
        "train_summary": summarize_trades(train_trades),
        "test_summary": summarize_trades(test_trades),
        "top_pairs": top_pairs,
        "bottom_pairs": bottom_pairs,
        "json_path": str(json_path),
        "trades_csv": str(trades_csv),
        "pair_summary_csv": str(pair_summary_csv),
        "monthly_summary_csv": str(monthly_summary_csv),
        "summary_md": str(summary_md),
    }
    write_csv(trades_csv, all_trades)
    write_csv(pair_summary_csv, pair_rows)
    write_csv(monthly_summary_csv, monthly_rows)
    atomic_write_json(json_path, payload)
    atomic_write_text(summary_md, markdown_report(payload))
    print(json.dumps(payload, indent=2, sort_keys=True, default=json_safe))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
