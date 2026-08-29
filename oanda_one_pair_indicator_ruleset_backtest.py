#!/usr/bin/env python3
"""Backtest one OANDA pair with the indicator/leg-confirmation ruleset.

Default target is EUR_SEK because the existing all-68 volatility report ranks it
as the largest 60-minute pip mover in the local candle universe. For non-USD
pairs, the confirmation layer uses USD legs when available. Example:

    EUR_SEK ~= EUR_USD * USD_SEK
    relative_strength = strength(EUR vs USD) - strength(SEK vs USD)
                      = return(EUR_USD) + return(USD_SEK)

This keeps the first experiment pair-specific and fast while matching the
"analyze the liquid legs, then trade the cross" idea from the attached prompt.
"""

from __future__ import annotations

import argparse
import csv
import itertools
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
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "one_pair_indicator_ruleset"
DEFAULT_PAIR = "EUR_SEK"


@dataclass(frozen=True)
class RuleConfig:
    name: str = "default_one_pair_h4"
    timeframe: str = "4h"
    ema_fast: int = 20
    ema_slow: int = 50
    ema_trend: int = 200
    ema_slope_lookback: int = 3
    adx_window: int = 14
    adx_min: float = 18.0
    adx_rising_lookback: int = 2
    atr_window: int = 14
    atr_stop_mult: float = 2.0
    atr_trail_mult: float = 2.5
    take_profit_r: float = 2.0
    max_hold_bars: int = 18
    donchian_period: int = 20
    strength_lookback: int = 6
    strength_min: float = 0.0
    rsi_window: int = 14
    rsi_long_min: float = 40.0
    rsi_long_max: float = 55.0
    rsi_short_min: float = 45.0
    rsi_short_max: float = 60.0
    pullback_atr_touch: float = 0.40
    entry_mode: str = "both"
    exit_on_opposite: bool = True
    round_turn_cost_bps: float = 0.0
    skip_friday_after_hour_utc: int = 18


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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_pair(value: str) -> str:
    text = str(value or "").strip().upper().replace("/", "_").replace("-", "_")
    if "_" not in text and len(text) == 6:
        return text[:3] + "_" + text[3:]
    return text


def split_pair(pair: str) -> tuple[str, str]:
    return tuple(normalize_pair(pair).split("_", 1))  # type: ignore[return-value]


def pip_multiplier(pair: str) -> float:
    return 100.0 if normalize_pair(pair) in PIP_LOCATION_MINUS2 else 10_000.0


def pair_path(pair: str) -> Path:
    return CANDLE_ROOT / f"{normalize_pair(pair)}_M1.csv"


def pair_exists(pair: str) -> bool:
    return pair_path(pair).exists()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
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


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def timeframe_to_timedelta(value: str) -> pd.Timedelta:
    return pd.to_timedelta(str(value).strip().lower())


def load_m1_ohlc(pair: str) -> pd.DataFrame:
    path = pair_path(pair)
    if not path.exists():
        raise FileNotFoundError(f"Missing candle file: {path}")
    frame = pd.read_csv(
        path,
        usecols=["datetime", "open", "high", "low", "close"],
        dtype={"open": "float64", "high": "float64", "low": "float64", "close": "float64"},
    )
    frame["time_utc"] = pd.to_datetime(frame["datetime"], errors="coerce", utc=True)
    for column in ["open", "high", "low", "close"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", "open", "high", "low", "close"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    return frame[["open", "high", "low", "close"]]


def resample_ohlc(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": frame["open"].resample(timeframe, label="right", closed="right").first(),
            "high": frame["high"].resample(timeframe, label="right", closed="right").max(),
            "low": frame["low"].resample(timeframe, label="right", closed="right").min(),
            "close": frame["close"].resample(timeframe, label="right", closed="right").last(),
        }
    ).dropna(subset=["open", "high", "low", "close"])


def usd_strength_leg(currency: str) -> tuple[str | None, float]:
    if currency == "USD":
        return None, 0.0
    direct = f"{currency}_USD"
    inverse = f"USD_{currency}"
    if pair_exists(direct):
        return direct, 1.0
    if pair_exists(inverse):
        return inverse, -1.0
    return None, 0.0


def load_leg_closes(pair: str, timeframe: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    base, quote = split_pair(pair)
    base_leg, base_sign = usd_strength_leg(base)
    quote_leg, quote_sign = usd_strength_leg(quote)
    metadata = {
        "base_currency": base,
        "quote_currency": quote,
        "base_usd_leg": base_leg,
        "base_usd_leg_sign": base_sign,
        "quote_usd_leg": quote_leg,
        "quote_usd_leg_sign": quote_sign,
        "fallback": False,
    }
    legs: dict[str, pd.Series] = {}
    for leg in sorted({x for x in [base_leg, quote_leg] if x}):
        leg_m1 = load_m1_ohlc(leg)
        legs[leg] = leg_m1["close"].resample(timeframe, label="right", closed="right").last().rename(leg)
    if not legs:
        metadata["fallback"] = True
        return pd.DataFrame(), metadata
    return pd.concat(legs.values(), axis=1).sort_index(), metadata


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


def wilder(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(alpha=1.0 / float(window), adjust=False, min_periods=window).mean()


def rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = wilder(gain, window)
    avg_loss = wilder(loss, window)
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def adx(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0), index=frame.index)
    atr = wilder(true_range(frame), window)
    plus_di = 100.0 * wilder(plus_dm, window) / atr.replace(0.0, np.nan)
    minus_di = 100.0 * wilder(minus_dm, window) / atr.replace(0.0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return pd.DataFrame({"adx": wilder(dx, window), "plus_di": plus_di, "minus_di": minus_di}, index=frame.index)


def signed_leg_return(leg_bars: pd.DataFrame, leg: str | None, sign: float, lookback: int) -> pd.Series:
    if not leg or leg not in leg_bars:
        return pd.Series(0.0, index=leg_bars.index if not leg_bars.empty else pd.DatetimeIndex([]))
    return sign * np.log(leg_bars[leg] / leg_bars[leg].shift(lookback))


def compute_features(pair: str, bars: pd.DataFrame, leg_bars: pd.DataFrame, leg_meta: dict[str, Any], cfg: RuleConfig) -> pd.DataFrame:
    frame = bars.copy()
    close = frame["close"].astype(float)
    frame["ema_fast"] = close.ewm(span=cfg.ema_fast, adjust=False, min_periods=cfg.ema_fast).mean()
    frame["ema_slow"] = close.ewm(span=cfg.ema_slow, adjust=False, min_periods=cfg.ema_slow).mean()
    frame["ema_trend"] = close.ewm(span=cfg.ema_trend, adjust=False, min_periods=cfg.ema_trend).mean()
    frame["ema_slow_slope"] = frame["ema_slow"] - frame["ema_slow"].shift(cfg.ema_slope_lookback)
    frame["atr"] = wilder(true_range(frame), cfg.atr_window)
    frame["atr_pct"] = frame["atr"] / close.replace(0.0, np.nan)
    frame = frame.join(adx(frame, cfg.adx_window))
    frame["rsi"] = rsi(close, cfg.rsi_window)
    frame["donchian_high"] = frame["high"].shift(1).rolling(cfg.donchian_period).max()
    frame["donchian_low"] = frame["low"].shift(1).rolling(cfg.donchian_period).min()

    if leg_bars.empty or leg_meta.get("fallback"):
        frame["base_strength"] = np.log(close / close.shift(cfg.strength_lookback))
        frame["quote_strength"] = 0.0
        frame["relative_strength"] = frame["base_strength"]
    else:
        base_strength = signed_leg_return(
            leg_bars,
            leg_meta.get("base_usd_leg"),
            float(leg_meta.get("base_usd_leg_sign", 0.0)),
            cfg.strength_lookback,
        )
        quote_strength = signed_leg_return(
            leg_bars,
            leg_meta.get("quote_usd_leg"),
            float(leg_meta.get("quote_usd_leg_sign", 0.0)),
            cfg.strength_lookback,
        )
        frame["base_strength"] = base_strength.reindex(frame.index)
        frame["quote_strength"] = quote_strength.reindex(frame.index)
        frame["relative_strength"] = frame["base_strength"] - frame["quote_strength"]
    return frame


def build_signals(features: pd.DataFrame, cfg: RuleConfig) -> pd.DataFrame:
    out = features.copy()
    adx_ok = (out["adx"] >= cfg.adx_min) & (out["adx"].diff(cfg.adx_rising_lookback) > 0.0)
    rs_long = out["relative_strength"] >= cfg.strength_min
    rs_short = out["relative_strength"] <= -cfg.strength_min
    long_trend = (
        (out["close"] > out["ema_slow"])
        & (out["ema_fast"] > out["ema_slow"])
        & ((out["ema_slow_slope"] >= 0.0) | (out["ema_slow"] > out["ema_trend"]))
    )
    short_trend = (
        (out["close"] < out["ema_slow"])
        & (out["ema_fast"] < out["ema_slow"])
        & ((out["ema_slow_slope"] <= 0.0) | (out["ema_slow"] < out["ema_trend"]))
    )
    long_breakout = out["close"] > out["donchian_high"]
    short_breakout = out["close"] < out["donchian_low"]
    touch_long = (out["low"] <= out["ema_fast"] + cfg.pullback_atr_touch * out["atr"]) | (
        out["low"] <= out["ema_slow"] + cfg.pullback_atr_touch * out["atr"]
    )
    touch_short = (out["high"] >= out["ema_fast"] - cfg.pullback_atr_touch * out["atr"]) | (
        out["high"] >= out["ema_slow"] - cfg.pullback_atr_touch * out["atr"]
    )
    long_pullback = (
        touch_long
        & (out["close"] >= out["ema_slow"])
        & out["rsi"].between(cfg.rsi_long_min, cfg.rsi_long_max, inclusive="both")
    )
    short_pullback = (
        touch_short
        & (out["close"] <= out["ema_slow"])
        & out["rsi"].between(cfg.rsi_short_min, cfg.rsi_short_max, inclusive="both")
    )
    if cfg.entry_mode == "breakout":
        long_entry = long_breakout
        short_entry = short_breakout
    elif cfg.entry_mode == "pullback":
        long_entry = long_pullback
        short_entry = short_pullback
    elif cfg.entry_mode == "both":
        long_entry = long_breakout | long_pullback
        short_entry = short_breakout | short_pullback
    else:
        raise ValueError(f"Unsupported entry_mode: {cfg.entry_mode!r}")
    long_signal = long_trend & adx_ok & rs_long & long_entry
    short_signal = short_trend & adx_ok & rs_short & short_entry
    out["signal"] = 0
    out.loc[long_signal & ~short_signal, "signal"] = 1
    out.loc[short_signal & ~long_signal, "signal"] = -1
    out["signal_reason"] = ""
    out.loc[out["signal"].eq(1) & long_breakout, "signal_reason"] = "long_breakout"
    out.loc[out["signal"].eq(1) & long_pullback & ~long_breakout, "signal_reason"] = "long_pullback"
    out.loc[out["signal"].eq(-1) & short_breakout, "signal_reason"] = "short_breakout"
    out.loc[out["signal"].eq(-1) & short_pullback & ~short_breakout, "signal_reason"] = "short_pullback"
    return out


def entry_time_allowed(ts: pd.Timestamp, cfg: RuleConfig) -> bool:
    if ts.dayofweek >= 5:
        return False
    if ts.dayofweek == 4 and ts.hour >= cfg.skip_friday_after_hour_utc:
        return False
    return True


def first_m1_after(index: pd.DatetimeIndex, ts: pd.Timestamp) -> int | None:
    pos = int(index.searchsorted(ts, side="right"))
    return None if pos >= len(index) else pos


def simulate_trades(pair: str, m1: pd.DataFrame, signals: pd.DataFrame, cfg: RuleConfig) -> list[dict[str, Any]]:
    signal_rows = signals[signals["signal"].ne(0)].copy()
    if signal_rows.empty:
        return []
    m1_index = m1.index
    h4_delta = timeframe_to_timedelta(cfg.timeframe)
    last_exit_time = m1_index[0]
    trades: list[dict[str, Any]] = []
    signal_by_time = signals["signal"].to_dict()
    multiplier = pip_multiplier(pair)

    for signal_time, row in signal_rows.iterrows():
        if signal_time <= last_exit_time or not entry_time_allowed(signal_time, cfg):
            continue
        direction = int(row["signal"])
        atr = finite_float(row.get("atr"), 0.0)
        entry_pos = first_m1_after(m1_index, signal_time)
        if entry_pos is None or atr <= 0.0:
            continue
        entry_time = m1_index[entry_pos]
        entry_price = finite_float(m1["open"].iloc[entry_pos], 0.0)
        if entry_price <= 0.0:
            continue
        stop_distance = atr * cfg.atr_stop_mult
        if stop_distance <= 0.0 or stop_distance / entry_price > 0.25:
            continue
        if direction > 0:
            hard_stop = entry_price - stop_distance
            target = entry_price + stop_distance * cfg.take_profit_r
            trail = entry_price - atr * cfg.atr_trail_mult
            best_price = entry_price
        else:
            hard_stop = entry_price + stop_distance
            target = entry_price - stop_distance * cfg.take_profit_r
            trail = entry_price + atr * cfg.atr_trail_mult
            best_price = entry_price

        max_exit_time = entry_time + cfg.max_hold_bars * h4_delta
        end_pos = min(int(m1_index.searchsorted(max_exit_time, side="right")), len(m1_index))
        end_pos = max(end_pos, entry_pos + 1)
        exit_time = m1_index[end_pos - 1]
        exit_price = finite_float(m1["close"].iloc[end_pos - 1], entry_price)
        exit_reason = "time_stop"

        for pos in range(entry_pos + 1, end_pos):
            ts = m1_index[pos]
            high = finite_float(m1["high"].iloc[pos], entry_price)
            low = finite_float(m1["low"].iloc[pos], entry_price)
            close = finite_float(m1["close"].iloc[pos], entry_price)
            if direction > 0:
                best_price = max(best_price, high)
                trail = max(trail, best_price - atr * cfg.atr_trail_mult)
                effective_stop = max(hard_stop, trail)
                if low <= effective_stop:
                    exit_time = ts
                    exit_price = effective_stop
                    exit_reason = "trailing_stop" if effective_stop > hard_stop else "hard_stop"
                    break
                if high >= target:
                    exit_time = ts
                    exit_price = target
                    exit_reason = "take_profit"
                    break
            else:
                best_price = min(best_price, low)
                trail = min(trail, best_price + atr * cfg.atr_trail_mult)
                effective_stop = min(hard_stop, trail)
                if high >= effective_stop:
                    exit_time = ts
                    exit_price = effective_stop
                    exit_reason = "trailing_stop" if effective_stop < hard_stop else "hard_stop"
                    break
                if low <= target:
                    exit_time = ts
                    exit_price = target
                    exit_reason = "take_profit"
                    break
            if cfg.exit_on_opposite and ts in signal_by_time and int(signal_by_time[ts]) == -direction:
                exit_time = ts
                exit_price = close
                exit_reason = "opposite_signal"
                break

        gross_return = direction * ((exit_price - entry_price) / entry_price)
        net_return = gross_return - cfg.round_turn_cost_bps / 10_000.0
        risk_return = stop_distance / entry_price
        trades.append(
            {
                "config": cfg.name,
                "instrument": normalize_pair(pair),
                "signal_time": signal_time.isoformat(),
                "entry_time": entry_time.isoformat(),
                "exit_time": exit_time.isoformat(),
                "direction": "LONG" if direction > 0 else "SHORT",
                "signal_reason": str(row.get("signal_reason", "")),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "gross_return_bps": gross_return * 10_000.0,
                "net_return_bps": net_return * 10_000.0,
                "gross_pips": direction * (exit_price - entry_price) * multiplier,
                "gross_r": gross_return / risk_return if risk_return > 0.0 else 0.0,
                "net_r": net_return / risk_return if risk_return > 0.0 else 0.0,
                "atr": atr,
                "atr_pct": finite_float(row.get("atr_pct"), 0.0),
                "adx": finite_float(row.get("adx"), 0.0),
                "rsi": finite_float(row.get("rsi"), 0.0),
                "relative_strength": finite_float(row.get("relative_strength"), 0.0),
                "hold_minutes": (exit_time - entry_time).total_seconds() / 60.0,
            }
        )
        last_exit_time = exit_time
    return trades


def summarize_trades(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {"trade_count": 0, "score": -999999.0}
    bps = np.array([finite_float(t.get("net_return_bps")) for t in trades], dtype=float)
    r_vals = np.array([finite_float(t.get("net_r")) for t in trades], dtype=float)
    wins = bps[bps > 0.0]
    losses = -bps[bps < 0.0]
    equity_bps = np.cumsum(bps)
    equity_r = np.cumsum(r_vals)
    dd_bps = np.maximum.accumulate(equity_bps) - equity_bps
    dd_r = np.maximum.accumulate(equity_r) - equity_r
    total_net_r = float(r_vals.sum())
    max_drawdown_r = float(dd_r.max()) if dd_r.size else 0.0
    return {
        "trade_count": int(len(trades)),
        "total_net_bps": float(bps.sum()),
        "avg_net_bps": float(bps.mean()),
        "median_net_bps": float(np.median(bps)),
        "win_rate": float((bps > 0.0).mean()),
        "profit_factor": float(wins.sum() / max(losses.sum(), 1e-9)),
        "total_net_r": total_net_r,
        "avg_net_r": float(r_vals.mean()),
        "median_net_r": float(np.median(r_vals)),
        "max_drawdown_r": max_drawdown_r,
        "max_drawdown_bps": float(dd_bps.max()) if dd_bps.size else 0.0,
        "best_trade_bps": float(bps.max()),
        "worst_trade_bps": float(bps.min()),
        "avg_hold_minutes": float(np.mean([finite_float(t.get("hold_minutes")) for t in trades])),
        "score": float(total_net_r - max_drawdown_r),
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


def config_summary_row(cfg: RuleConfig, trades: Sequence[dict[str, Any]], split_time: pd.Timestamp) -> dict[str, Any]:
    train, test = split_trades(trades, split_time)
    row: dict[str, Any] = {
        "config": cfg.name,
        "entry_mode": cfg.entry_mode,
        "adx_min": cfg.adx_min,
        "strength_lookback": cfg.strength_lookback,
        "strength_min": cfg.strength_min,
        "atr_stop_mult": cfg.atr_stop_mult,
        "take_profit_r": cfg.take_profit_r,
        "donchian_period": cfg.donchian_period,
        "round_turn_cost_bps": cfg.round_turn_cost_bps,
    }
    for prefix, summary in [
        ("all", summarize_trades(trades)),
        ("train", summarize_trades(train)),
        ("test", summarize_trades(test)),
    ]:
        for key, value in summary.items():
            row[f"{prefix}_{key}"] = value
    return row


def grid_configs(base: RuleConfig, preset: str) -> list[RuleConfig]:
    if preset == "none":
        return []
    if preset == "compact":
        configs: list[RuleConfig] = []
        for entry_mode, adx_min, strength_lookback in itertools.product(
            ["breakout", "pullback", "both"],
            [15.0, 18.0, 22.0],
            [3, 6, 12],
        ):
            configs.append(
                replace(
                    base,
                    name=(
                        f"grid_entry_mode{entry_mode}_adx_min{str(adx_min).replace('.', 'p')}"
                        f"_strength_lookback{strength_lookback}_atr_stop_mult2p0_take_profit_r2p0"
                    ),
                    entry_mode=entry_mode,
                    adx_min=adx_min,
                    strength_lookback=strength_lookback,
                    atr_stop_mult=2.0,
                    take_profit_r=2.0,
                )
            )
        exit_pairs = [(1.5, 1.5), (1.5, 2.0), (2.0, 1.5), (2.0, 2.5), (2.5, 2.0), (2.5, 2.5)]
        for entry_mode, (atr_stop_mult, take_profit_r) in itertools.product(["breakout", "both"], exit_pairs):
            configs.append(
                replace(
                    base,
                    name=(
                        f"grid_entry_mode{entry_mode}_adx_min18p0_strength_lookback6"
                        f"_atr_stop_mult{str(atr_stop_mult).replace('.', 'p')}"
                        f"_take_profit_r{str(take_profit_r).replace('.', 'p')}"
                    ),
                    entry_mode=entry_mode,
                    adx_min=18.0,
                    strength_lookback=6,
                    atr_stop_mult=atr_stop_mult,
                    take_profit_r=take_profit_r,
                )
            )
        return configs
    ranges = {
        "entry_mode": ["breakout", "pullback", "both"],
        "adx_min": [15.0, 18.0, 22.0],
        "strength_lookback": [3, 6, 12],
        "atr_stop_mult": [1.5, 2.0, 2.5],
        "take_profit_r": [1.5, 2.0, 2.5],
    }
    if preset == "wide":
        ranges["strength_min"] = [0.0, 0.0005, 0.0010]
        ranges["donchian_period"] = [20, 55]
    elif preset != "quick":
        raise ValueError(f"Unsupported grid preset: {preset!r}")
    keys = list(ranges)
    configs: list[RuleConfig] = []
    for values in itertools.product(*(ranges[key] for key in keys)):
        kwargs = dict(zip(keys, values))
        label = "_".join(f"{key}{str(value).replace('.', 'p')}" for key, value in kwargs.items())
        configs.append(replace(base, name=f"grid_{label}", **kwargs))
    return configs


def select_top_configs(rows: Sequence[dict[str, Any]], min_trades: int, limit: int = 10) -> list[dict[str, Any]]:
    viable = [
        row
        for row in rows
        if int(row.get("all_trade_count", 0)) >= min_trades
        and int(row.get("test_trade_count", 0)) >= max(3, min_trades // 4)
    ]
    if not viable:
        viable = [row for row in rows if int(row.get("all_trade_count", 0)) >= min_trades]
    return sorted(
        viable,
        key=lambda row: (
            finite_float(row.get("test_score"), -999999.0),
            finite_float(row.get("all_score"), -999999.0),
            finite_float(row.get("test_profit_factor"), 0.0),
        ),
        reverse=True,
    )[:limit]


def run_config(pair: str, m1: pd.DataFrame, bars: pd.DataFrame, leg_bars: pd.DataFrame, leg_meta: dict[str, Any], cfg: RuleConfig) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    features = compute_features(pair, bars, leg_bars, leg_meta, cfg)
    signals = build_signals(features, cfg)
    return simulate_trades(pair, m1, signals, cfg), signals


def format_float(value: Any, digits: int = 2) -> str:
    return f"{finite_float(value, 0.0):.{digits}f}"


def markdown_report(payload: dict[str, Any]) -> str:
    summary = payload["default_summary"]
    lines = [
        f"# {payload['pair']} Indicator Ruleset Backtest",
        "",
        f"Generated: {payload['generated_at_utc']}",
        "",
        "## Data and assumptions",
        "",
        f"- Pair: `{payload['pair']}`.",
        f"- USD-leg confirmation: `{payload['leg_formula']}`.",
        f"- M1 range: `{payload['m1_start_utc']}` to `{payload['m1_end_utc']}`.",
        f"- Signal timeframe: `{payload['timeframe']}`; entries/exits simulated on M1 direct candles.",
        f"- Round-turn execution cost: `{payload['round_turn_cost_bps']}` bps.",
        "- Historical spread/slippage/swap are not fully represented in the local candle file.",
        "",
        "## Default config",
        "",
        f"- Trades: `{summary.get('trade_count', 0)}`",
        f"- Total net: `{format_float(summary.get('total_net_bps'))}` bps / `{format_float(summary.get('total_net_r'))}` R",
        f"- Avg trade: `{format_float(summary.get('avg_net_bps'))}` bps / `{format_float(summary.get('avg_net_r'))}` R",
        f"- Win rate: `{format_float(100.0 * finite_float(summary.get('win_rate'))) }%`",
        f"- Profit factor: `{format_float(summary.get('profit_factor'))}`",
        f"- Max drawdown: `{format_float(summary.get('max_drawdown_bps'))}` bps / `{format_float(summary.get('max_drawdown_r'))}` R",
        "",
    ]
    if payload["top_grid_configs"]:
        lines.extend(
            [
                "## Top grid configs",
                "",
                "| rank | mode | ADX | strength bars | stop ATR | TP R | all trades | all R | test trades | test R | test PF |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for rank, row in enumerate(payload["top_grid_configs"], start=1):
            lines.append(
                f"| {rank} | {row['entry_mode']} | {format_float(row['adx_min'])} | "
                f"{row['strength_lookback']} | {format_float(row['atr_stop_mult'])} | "
                f"{format_float(row['take_profit_r'])} | {int(row.get('all_trade_count', 0))} | "
                f"{format_float(row.get('all_total_net_r'))} | {int(row.get('test_trade_count', 0))} | "
                f"{format_float(row.get('test_total_net_r'))} | {format_float(row.get('test_profit_factor'))} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Files",
            "",
            f"- JSON: `{payload['json_report']}`",
            f"- Grid CSV: `{payload['grid_csv']}`",
            f"- Trades CSV: `{payload['trades_csv']}`",
            f"- Signals CSV: `{payload['signals_csv']}`",
            "",
        ]
    )
    return "\n".join(lines)


def leg_formula(pair: str, meta: dict[str, Any]) -> str:
    base, quote = split_pair(pair)
    if meta.get("fallback"):
        return f"fallback direct {pair} return"
    parts = []
    if meta.get("base_usd_leg"):
        sign = "+" if float(meta.get("base_usd_leg_sign", 0.0)) > 0 else "-"
        parts.append(f"strength({base})={sign}return({meta['base_usd_leg']})")
    else:
        parts.append(f"strength({base})=0 because base is USD")
    if meta.get("quote_usd_leg"):
        sign = "+" if float(meta.get("quote_usd_leg_sign", 0.0)) > 0 else "-"
        parts.append(f"strength({quote})={sign}return({meta['quote_usd_leg']})")
    else:
        parts.append(f"strength({quote})=0 because quote is USD")
    return "; ".join(parts) + f"; relative=strength({base})-strength({quote})"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", default=DEFAULT_PAIR)
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--cost-bps", type=float, default=0.0)
    parser.add_argument("--grid-preset", choices=["none", "compact", "quick", "wide"], default="compact")
    parser.add_argument("--min-trades", type=int, default=8)
    parser.add_argument("--max-grid-configs", type=int, default=0)
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pair = normalize_pair(args.pair)
    if not pair_exists(pair):
        raise RuntimeError(f"Pair {pair!r} is not available in {CANDLE_ROOT}")
    m1 = load_m1_ohlc(pair)
    bars = resample_ohlc(m1, args.timeframe)
    leg_bars, leg_meta = load_leg_closes(pair, args.timeframe)
    split_time = bars.index[int(len(bars) * 0.70)]
    base_cfg = replace(RuleConfig(), timeframe=args.timeframe, round_turn_cost_bps=float(args.cost_bps))

    default_trades, default_signals = run_config(pair, m1, bars, leg_bars, leg_meta, base_cfg)
    default_row = config_summary_row(base_cfg, default_trades, split_time)
    grid_rows: list[dict[str, Any]] = []
    configs = grid_configs(base_cfg, args.grid_preset)
    if args.max_grid_configs > 0:
        configs = configs[: args.max_grid_configs]
    for idx, cfg in enumerate(configs, start=1):
        print(f"[grid] {idx:03d}/{len(configs):03d} {cfg.name}", flush=True)
        trades, _signals = run_config(pair, m1, bars, leg_bars, leg_meta, cfg)
        grid_rows.append(config_summary_row(cfg, trades, split_time))

    top_configs = select_top_configs([default_row, *grid_rows], args.min_trades)
    slug = pair.lower()
    output_dir = args.output_dir / slug
    json_path = output_dir / f"latest_{slug}_ruleset_backtest.json"
    grid_csv = output_dir / f"latest_{slug}_ruleset_grid.csv"
    trades_csv = output_dir / f"latest_{slug}_ruleset_default_trades.csv"
    signals_csv = output_dir / f"latest_{slug}_ruleset_default_signals.csv"
    summary_md = output_dir / f"latest_{slug}_ruleset_summary.md"

    signal_export = default_signals.reset_index().rename(columns={"time_utc": "signal_time"})
    signal_rows = signal_export[signal_export["signal"].ne(0)].to_dict(orient="records")
    payload = {
        "generated_at_utc": utc_now(),
        "pair": pair,
        "timeframe": args.timeframe,
        "round_turn_cost_bps": float(args.cost_bps),
        "m1_start_utc": m1.index.min().isoformat(),
        "m1_end_utc": m1.index.max().isoformat(),
        "bar_count": int(len(bars)),
        "split_time_utc": split_time.isoformat(),
        "leg_metadata": leg_meta,
        "leg_formula": leg_formula(pair, leg_meta),
        "default_config": asdict(base_cfg),
        "default_summary": summarize_trades(default_trades),
        "default_signal_count": int(default_signals["signal"].ne(0).sum()),
        "grid_preset": args.grid_preset,
        "grid_config_count": len(configs),
        "top_grid_configs": top_configs,
        "json_report": str(json_path),
        "grid_csv": str(grid_csv),
        "trades_csv": str(trades_csv),
        "signals_csv": str(signals_csv),
        "summary_md": str(summary_md),
    }
    write_csv(grid_csv, [default_row, *grid_rows])
    write_csv(trades_csv, default_trades)
    write_csv(signals_csv, signal_rows)
    atomic_write_json(json_path, payload)
    atomic_write_text(summary_md, markdown_report(payload))
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
