#!/usr/bin/env python3
"""Backtest a reusable indicator ruleset across all local OANDA M1 pairs.

This is the scalable version of the attached "best indicators" answer:

- 20/50/200 EMA trend stack
- ADX trend-quality filter
- ATR stop, trailing stop, and R-multiple take profit
- Donchian breakout and RSI pullback entries
- Cross-sectional currency-strength confirmation across all 68 searched pairs

The local candle universe is discovered from:

    data/oanda_training_manager/candles/*_M1.csv

Signals are generated on completed H4 bars by default. The backtest uses
resampled H4 OHLC from M1 mid closes, so it is a research/backtest harness, not
a tick-level broker fill simulator. Historical spread and swap/carry are not
available for every pair in these candle files; round-turn execution cost is a
configurable basis-point assumption.
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
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "all68_indicator_ruleset"

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
class RuleConfig:
    name: str = "default_all68_h4"
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def load_pair_close(pair: str) -> pd.Series:
    path = CANDLE_ROOT / f"{pair}_M1.csv"
    frame = pd.read_csv(path, usecols=["datetime", "close"], dtype={"close": "float64"})
    frame["time_utc"] = pd.to_datetime(frame["datetime"], errors="coerce", utc=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", "close"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    return frame["close"].astype(float).rename(pair)


def load_all_bars(pairs: Sequence[str], timeframe: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    bars_by_pair: dict[str, pd.DataFrame] = {}
    closes: dict[str, pd.Series] = {}
    for index, pair in enumerate(pairs, start=1):
        series = load_pair_close(pair)
        bars = series.resample(timeframe, label="right", closed="right").ohlc()
        bars = bars.dropna(subset=["open", "high", "low", "close"])
        bars_by_pair[pair] = bars
        closes[pair] = bars["close"].rename(pair)
        print(f"[load] {index:02d}/{len(pairs)} {pair}: {len(bars):,} {timeframe} bars")
    close_matrix = pd.concat(closes.values(), axis=1).sort_index()
    return bars_by_pair, close_matrix


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
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0),
        index=frame.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0),
        index=frame.index,
    )
    atr = wilder(true_range(frame), window)
    plus_di = 100.0 * wilder(plus_dm, window) / atr.replace(0.0, np.nan)
    minus_di = 100.0 * wilder(minus_dm, window) / atr.replace(0.0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return pd.DataFrame({"adx": wilder(dx, window), "plus_di": plus_di, "minus_di": minus_di}, index=frame.index)


def currency_strength(close_matrix: pd.DataFrame, pairs: Sequence[str], lookback: int) -> pd.DataFrame:
    currencies = sorted({currency for pair in pairs for currency in split_pair(pair)})
    returns = np.log(close_matrix / close_matrix.shift(lookback))
    strength = pd.DataFrame(0.0, index=close_matrix.index, columns=currencies)
    counts = pd.DataFrame(0.0, index=close_matrix.index, columns=currencies)
    for pair in pairs:
        base, quote = split_pair(pair)
        pair_return = returns[pair]
        valid = pair_return.notna().astype(float)
        strength[base] = strength[base] + pair_return.fillna(0.0)
        strength[quote] = strength[quote] - pair_return.fillna(0.0)
        counts[base] = counts[base] + valid
        counts[quote] = counts[quote] + valid
    return strength / counts.replace(0.0, np.nan)


def compute_features(pair: str, bars: pd.DataFrame, strength: pd.DataFrame, cfg: RuleConfig) -> pd.DataFrame:
    base, quote = split_pair(pair)
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
    aligned_strength = strength.reindex(frame.index)
    frame["base_strength"] = aligned_strength[base]
    frame["quote_strength"] = aligned_strength[quote]
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

    mode = str(cfg.entry_mode).lower()
    if mode == "breakout":
        long_entry = long_breakout
        short_entry = short_breakout
    elif mode == "pullback":
        long_entry = long_pullback
        short_entry = short_pullback
    elif mode == "both":
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


def simulate_pair(pair: str, signals: pd.DataFrame, cfg: RuleConfig) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    if signals.empty:
        return trades
    signal_values = signals["signal"].fillna(0).astype(int)
    index = signals.index
    last_exit_pos = -1
    multiplier = pip_multiplier(pair)

    for signal_pos, (signal_time, row) in enumerate(signals.iterrows()):
        direction = int(row.get("signal", 0))
        if direction == 0 or signal_pos <= last_exit_pos:
            continue
        if not entry_time_allowed(signal_time, cfg):
            continue
        entry_pos = signal_pos + 1
        if entry_pos >= len(signals):
            break
        entry_row = signals.iloc[entry_pos]
        entry_time = index[entry_pos]
        entry_price = finite_float(entry_row.get("open"), 0.0)
        atr = finite_float(row.get("atr"), 0.0)
        if entry_price <= 0.0 or atr <= 0.0:
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

        exit_pos = min(entry_pos + cfg.max_hold_bars, len(signals) - 1)
        exit_time = index[exit_pos]
        exit_price = finite_float(signals.iloc[exit_pos].get("close"), entry_price)
        exit_reason = "time_stop"

        for pos in range(entry_pos, min(entry_pos + cfg.max_hold_bars + 1, len(signals))):
            bar = signals.iloc[pos]
            ts = index[pos]
            high = finite_float(bar.get("high"), entry_price)
            low = finite_float(bar.get("low"), entry_price)
            close = finite_float(bar.get("close"), entry_price)

            if direction > 0:
                effective_stop = max(hard_stop, trail)
                if low <= effective_stop:
                    exit_pos = pos
                    exit_time = ts
                    exit_price = effective_stop
                    exit_reason = "trailing_stop" if effective_stop > hard_stop else "hard_stop"
                    break
                if high >= target:
                    exit_pos = pos
                    exit_time = ts
                    exit_price = target
                    exit_reason = "take_profit"
                    break
                best_price = max(best_price, high)
                trail = max(trail, best_price - atr * cfg.atr_trail_mult)
            else:
                effective_stop = min(hard_stop, trail)
                if high >= effective_stop:
                    exit_pos = pos
                    exit_time = ts
                    exit_price = effective_stop
                    exit_reason = "trailing_stop" if effective_stop < hard_stop else "hard_stop"
                    break
                if low <= target:
                    exit_pos = pos
                    exit_time = ts
                    exit_price = target
                    exit_reason = "take_profit"
                    break
                best_price = min(best_price, low)
                trail = min(trail, best_price + atr * cfg.atr_trail_mult)

            if cfg.exit_on_opposite and int(signal_values.iloc[pos]) == -direction:
                exit_pos = pos
                exit_time = ts
                exit_price = close
                exit_reason = "opposite_signal"
                break

        gross_return = direction * ((exit_price - entry_price) / entry_price)
        net_return = gross_return - cfg.round_turn_cost_bps / 10_000.0
        risk_return = stop_distance / entry_price
        gross_r = gross_return / risk_return if risk_return > 0.0 else 0.0
        net_r = net_return / risk_return if risk_return > 0.0 else 0.0
        trades.append(
            {
                "config": cfg.name,
                "instrument": pair,
                "group": pair_group(pair),
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
                "gross_r": gross_r,
                "net_r": net_r,
                "atr": atr,
                "atr_pct": finite_float(row.get("atr_pct"), 0.0),
                "adx": finite_float(row.get("adx"), 0.0),
                "rsi": finite_float(row.get("rsi"), 0.0),
                "relative_strength": finite_float(row.get("relative_strength"), 0.0),
                "hold_bars": int(max(0, exit_pos - entry_pos + 1)),
                "atr_stop_mult": cfg.atr_stop_mult,
                "take_profit_r": cfg.take_profit_r,
                "entry_mode": cfg.entry_mode,
            }
        )
        last_exit_pos = exit_pos

    return trades


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
            "max_drawdown_r": 0.0,
            "max_drawdown_bps": 0.0,
            "score": -999999.0,
        }
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
        "avg_hold_bars": float(np.mean([finite_float(t.get("hold_bars")) for t in trades])),
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


def run_config(
    pairs: Sequence[str],
    bars_by_pair: dict[str, pd.DataFrame],
    strengths: dict[int, pd.DataFrame],
    cfg: RuleConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    all_trades: list[dict[str, Any]] = []
    signal_rows: list[dict[str, Any]] = []
    strength = strengths[cfg.strength_lookback]
    for pair in pairs:
        features = compute_features(pair, bars_by_pair[pair], strength, cfg)
        signals = build_signals(features, cfg)
        trades = simulate_pair(pair, signals, cfg)
        all_trades.extend(trades)
        signal_rows.append(
            {
                "config": cfg.name,
                "instrument": pair,
                "group": pair_group(pair),
                "signal_count": int(signals["signal"].ne(0).sum()),
                "long_signal_count": int(signals["signal"].eq(1).sum()),
                "short_signal_count": int(signals["signal"].eq(-1).sum()),
            }
        )
    return all_trades, signal_rows


def pair_summary_rows(trades: Sequence[dict[str, Any]], pairs: Sequence[str], config_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_pair: dict[str, list[dict[str, Any]]] = {pair: [] for pair in pairs}
    for trade in trades:
        by_pair.setdefault(str(trade["instrument"]), []).append(trade)
    for pair in pairs:
        summary = summarize_trades(by_pair.get(pair, []))
        row = {
            "config": config_name,
            "instrument": pair,
            "group": pair_group(pair),
            **summary,
        }
        rows.append(row)
    return rows


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
    if preset == "quick":
        ranges = {
            "entry_mode": ["breakout", "pullback", "both"],
            "adx_min": [15.0, 18.0, 22.0],
            "strength_lookback": [3, 6, 12],
            "atr_stop_mult": [1.5, 2.0, 2.5],
            "take_profit_r": [1.5, 2.0],
        }
    elif preset == "wide":
        ranges = {
            "entry_mode": ["breakout", "pullback", "both"],
            "adx_min": [15.0, 18.0, 20.0, 22.0, 25.0],
            "strength_lookback": [3, 6, 12, 18],
            "strength_min": [0.0, 0.0005, 0.0010],
            "atr_stop_mult": [1.5, 2.0, 2.5, 3.0],
            "take_profit_r": [1.25, 1.5, 2.0, 2.5, 3.0],
            "donchian_period": [20, 55],
        }
    else:
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
        and int(row.get("test_trade_count", 0)) >= max(5, min_trades // 4)
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


def select_top_pairs(rows: Sequence[dict[str, Any]], min_trades: int, limit: int = 12, reverse: bool = True) -> list[dict[str, Any]]:
    viable = [row for row in rows if int(row.get("trade_count", 0)) >= min_trades]
    return sorted(
        viable,
        key=lambda row: finite_float(row.get("total_net_r"), 0.0),
        reverse=reverse,
    )[:limit]


def pair_ruleset_rows(pairs: Sequence[str]) -> list[dict[str, Any]]:
    rows = []
    for pair in pairs:
        base, quote = split_pair(pair)
        rows.append(
            {
                "instrument": pair,
                "group": pair_group(pair),
                "base_currency": base,
                "quote_currency": quote,
                "decomposition": f"currency_strength[{base}] - currency_strength[{quote}]",
                "trend_filter": "20/50/200 EMA stack",
                "trend_quality": "ADX(14) above threshold and rising",
                "volatility": "ATR(14) stop/trailing/position-risk proxy",
                "entries": "Donchian breakout and/or RSI pullback to EMA area",
                "exits": "ATR stop, ATR trail, take-profit R, opposite signal, max hold",
            }
        )
    return rows


def format_float(value: Any, digits: int = 2) -> str:
    return f"{finite_float(value, 0.0):.{digits}f}"


def markdown_report(payload: dict[str, Any]) -> str:
    default_summary = payload["default_summary"]
    top_pairs = payload["top_pairs"]
    bottom_pairs = payload["bottom_pairs"]
    top_configs = payload["top_grid_configs"]
    lines = [
        "# All-68 Indicator Ruleset Backtest",
        "",
        f"Generated: {payload['generated_at_utc']}",
        "",
        "## Data and assumptions",
        "",
        f"- Candle universe: `{payload['pair_count']}` OANDA M1 pairs from `{CANDLE_ROOT}`.",
        f"- Backtested pair count: `{payload.get('run_pair_count', payload['pair_count'])}`"
        + (f" (`{payload['target_pair']}`)." if payload.get("target_pair") else "."),
        f"- H4 data range: `{payload['bar_start_utc']}` to `{payload['bar_end_utc']}`.",
        f"- Signal timeframe: `{payload['timeframe']}`; execution uses next H4 open and H4 high/low path.",
        f"- Round-turn execution cost: `{payload['round_turn_cost_bps']}` bps.",
        "- Historical spread, slippage, and swap/carry are not fully represented in the local candle files.",
        "",
        "## Default config",
        "",
        f"- Trades: `{default_summary['trade_count']}`",
        f"- Total net: `{format_float(default_summary['total_net_bps'])}` bps / `{format_float(default_summary['total_net_r'])}` R",
        f"- Avg trade: `{format_float(default_summary['avg_net_bps'])}` bps / `{format_float(default_summary['avg_net_r'])}` R",
        f"- Win rate: `{format_float(100.0 * default_summary['win_rate'])}%`",
        f"- Profit factor: `{format_float(default_summary['profit_factor'])}`",
        f"- Max drawdown: `{format_float(default_summary['max_drawdown_bps'])}` bps / `{format_float(default_summary['max_drawdown_r'])}` R",
        "",
    ]

    if top_pairs:
        lines.extend(
            [
                "## Top default pairs",
                "",
                "| pair | group | trades | total R | win % | PF | max DD R |",
                "|---|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in top_pairs:
            lines.append(
                f"| {row['instrument']} | {row['group']} | {int(row['trade_count'])} | "
                f"{format_float(row['total_net_r'])} | {format_float(100.0 * row['win_rate'])} | "
                f"{format_float(row['profit_factor'])} | {format_float(row['max_drawdown_r'])} |"
            )
        lines.append("")

    if bottom_pairs:
        lines.extend(
            [
                "## Weakest default pairs",
                "",
                "| pair | group | trades | total R | win % | PF | max DD R |",
                "|---|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in bottom_pairs:
            lines.append(
                f"| {row['instrument']} | {row['group']} | {int(row['trade_count'])} | "
                f"{format_float(row['total_net_r'])} | {format_float(100.0 * row['win_rate'])} | "
                f"{format_float(row['profit_factor'])} | {format_float(row['max_drawdown_r'])} |"
            )
        lines.append("")

    if top_configs:
        lines.extend(
            [
                "## Top grid configs",
                "",
                "| rank | mode | ADX | strength bars | stop ATR | TP R | all trades | all R | test trades | test R | test PF |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for rank, row in enumerate(top_configs, start=1):
            lines.append(
                f"| {rank} | {row['entry_mode']} | {format_float(row['adx_min'])} | "
                f"{row['strength_lookback']} | {format_float(row['atr_stop_mult'])} | "
                f"{format_float(row['take_profit_r'])} | {int(row['all_trade_count'])} | "
                f"{format_float(row['all_total_net_r'])} | {int(row['test_trade_count'])} | "
                f"{format_float(row['test_total_net_r'])} | {format_float(row['test_profit_factor'])} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Files",
            "",
            f"- JSON: `{payload['json_report']}`",
            f"- Grid CSV: `{payload['grid_csv']}`",
            f"- Pair summary CSV: `{payload['pair_summary_csv']}`",
            f"- Default trades CSV: `{payload['trades_csv']}`",
            f"- Pair ruleset CSV: `{payload['pair_ruleset_csv']}`",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe", default="4h", help="Signal timeframe, e.g. 1h, 4h, 1D.")
    parser.add_argument("--target-pair", default="", help="Optional single pair to backtest while still using all pairs for strength context.")
    parser.add_argument("--cost-bps", type=float, default=0.0, help="Round-turn execution cost in basis points.")
    parser.add_argument("--grid-preset", choices=["none", "quick", "wide"], default="quick")
    parser.add_argument("--min-trades", type=int, default=20)
    parser.add_argument("--max-grid-configs", type=int, default=0, help="Optional cap for grid configs.")
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = discover_pairs()
    if not pairs:
        raise RuntimeError(f"No *_M1.csv files found in {CANDLE_ROOT}")
    target_pair = str(args.target_pair or "").strip().upper().replace("/", "_").replace("-", "_")
    if target_pair and target_pair not in pairs:
        raise RuntimeError(f"Target pair {target_pair!r} is not in the local 68-pair candle universe.")
    run_pairs = [target_pair] if target_pair else pairs
    print(f"[setup] discovered {len(pairs)} pairs; running {len(run_pairs)} pair(s)")
    bars_by_pair, close_matrix = load_all_bars(pairs, args.timeframe)
    split_time = close_matrix.index[int(len(close_matrix) * 0.70)]

    base_cfg = replace(RuleConfig(), timeframe=args.timeframe, round_turn_cost_bps=float(args.cost_bps))
    configs = grid_configs(base_cfg, args.grid_preset)
    if args.max_grid_configs > 0:
        configs = configs[: args.max_grid_configs]

    needed_lookbacks = sorted({base_cfg.strength_lookback, *(cfg.strength_lookback for cfg in configs)})
    strengths = {lookback: currency_strength(close_matrix, pairs, lookback) for lookback in needed_lookbacks}

    print("[run] default config")
    default_trades, default_signal_rows = run_config(run_pairs, bars_by_pair, strengths, base_cfg)
    default_summary = summarize_trades(default_trades)
    pair_rows = pair_summary_rows(default_trades, run_pairs, base_cfg.name)
    default_row = config_summary_row(base_cfg, default_trades, split_time)

    grid_rows: list[dict[str, Any]] = []
    for idx, cfg in enumerate(configs, start=1):
        print(f"[grid] {idx:03d}/{len(configs):03d} {cfg.name}")
        trades, _signal_rows = run_config(run_pairs, bars_by_pair, strengths, cfg)
        grid_rows.append(config_summary_row(cfg, trades, split_time))

    top_configs = select_top_configs([default_row, *grid_rows], args.min_trades, limit=10)
    top_pairs = select_top_pairs(pair_rows, min_trades=max(5, args.min_trades // 4), limit=12, reverse=True)
    bottom_pairs = select_top_pairs(pair_rows, min_trades=max(5, args.min_trades // 4), limit=12, reverse=False)

    output_dir = args.output_dir
    json_path = output_dir / "latest_all68_ruleset_backtest.json"
    grid_csv = output_dir / "latest_all68_ruleset_grid.csv"
    pair_summary_csv = output_dir / "latest_all68_ruleset_pair_summary.csv"
    trades_csv = output_dir / "latest_all68_ruleset_default_trades.csv"
    signal_csv = output_dir / "latest_all68_ruleset_default_signals.csv"
    pair_ruleset_csv = output_dir / "latest_all68_ruleset_pair_rulesets.csv"
    summary_md = output_dir / "latest_all68_ruleset_summary.md"

    payload: dict[str, Any] = {
        "generated_at_utc": utc_now(),
        "pair_count": len(pairs),
        "run_pair_count": len(run_pairs),
        "pairs": pairs,
        "run_pairs": run_pairs,
        "target_pair": target_pair,
        "timeframe": args.timeframe,
        "round_turn_cost_bps": float(args.cost_bps),
        "bar_start_utc": close_matrix.index.min().isoformat(),
        "bar_end_utc": close_matrix.index.max().isoformat(),
        "bar_count": int(len(close_matrix)),
        "split_time_utc": split_time.isoformat(),
        "default_config": asdict(base_cfg),
        "default_summary": default_summary,
        "grid_preset": args.grid_preset,
        "grid_config_count": len(configs),
        "top_grid_configs": top_configs,
        "top_pairs": top_pairs,
        "bottom_pairs": bottom_pairs,
        "json_report": str(json_path),
        "grid_csv": str(grid_csv),
        "pair_summary_csv": str(pair_summary_csv),
        "trades_csv": str(trades_csv),
        "signal_csv": str(signal_csv),
        "pair_ruleset_csv": str(pair_ruleset_csv),
        "summary_md": str(summary_md),
    }

    write_csv(grid_csv, [default_row, *grid_rows])
    write_csv(pair_summary_csv, pair_rows)
    write_csv(trades_csv, default_trades)
    write_csv(signal_csv, default_signal_rows)
    write_csv(pair_ruleset_csv, pair_ruleset_rows(run_pairs))
    atomic_write_json(json_path, payload)
    atomic_write_text(summary_md, markdown_report(payload))

    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
