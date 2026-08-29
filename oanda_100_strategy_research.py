#!/usr/bin/env python3
"""Research 100 source-inspired FX strategies on local OANDA M1 candles.

The script builds a 100-strategy catalog from common forex/technical-analysis
families, generates signals from local OANDA M1 candle files, simulates each
strategy as a separate margin account, and optionally searches pair/triple
ensembles among the best individual strategies.

It is a research simulator. It models next-bar entries, M1 stop/target paths,
round-turn spread costs, position sizing, margin-use caps, and an OANDA-like
margin closeout threshold. It is not a tick-level broker fill engine and it does
not place orders.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "strategy_100_research"

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

MAJOR_PAIRS = [
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
    "EUR_JPY",
    "GBP_JPY",
    "EUR_GBP",
]

SOURCE_REGISTRY = {
    "moving_average": {
        "title": "Moving averages",
        "url": "https://www.investopedia.com/terms/m/movingaverage.asp",
    },
    "rsi": {
        "title": "Relative Strength Index",
        "url": "https://www.investopedia.com/terms/r/rsi.asp",
    },
    "macd": {
        "title": "Moving Average Convergence Divergence",
        "url": "https://www.investopedia.com/terms/m/macd.asp",
    },
    "bollinger": {
        "title": "Bollinger Bands",
        "url": "https://www.investopedia.com/terms/b/bollingerbands.asp",
    },
    "donchian": {
        "title": "Donchian channels",
        "url": "https://www.investopedia.com/terms/d/donchianchannels.asp",
    },
    "stochastic": {
        "title": "Stochastic oscillator",
        "url": "https://www.investopedia.com/terms/s/stochasticoscillator.asp",
    },
    "cci": {
        "title": "Commodity Channel Index",
        "url": "https://www.investopedia.com/terms/c/commoditychannelindex.asp",
    },
    "adx": {
        "title": "Average Directional Index",
        "url": "https://www.investopedia.com/terms/a/adx.asp",
    },
    "atr": {
        "title": "Average True Range",
        "url": "https://www.investopedia.com/terms/a/atr.asp",
    },
    "ichimoku": {
        "title": "Ichimoku cloud",
        "url": "https://www.investopedia.com/terms/i/ichimoku-cloud.asp",
    },
    "pivot": {
        "title": "Pivot points",
        "url": "https://www.investopedia.com/terms/p/pivotpoint.asp",
    },
    "support_resistance": {
        "title": "Support and resistance",
        "url": "https://www.babypips.com/learn/forex/support-and-resistance",
    },
    "breakout": {
        "title": "Trading breakouts",
        "url": "https://www.babypips.com/learn/forex/trading-breakouts",
    },
    "candlestick": {
        "title": "Japanese candlesticks",
        "url": "https://www.babypips.com/learn/forex/japanese-candlesticks",
    },
    "reddit_h4_ema_engulfing": {
        "title": "Reddit r/Forex H4 engulfing EMA bounce",
        "url": "https://www.reddit.com/r/Forex/comments/1p4rzcb/how_the_fck_are_you_even_profitable_in_forex/",
    },
    "oanda_candles": {
        "title": "OANDA v20 instrument candles",
        "url": "https://developer.oanda.com/rest-live-v20/instrument-ep/",
    },
    "oanda_account": {
        "title": "OANDA v20 account definitions",
        "url": "https://developer.oanda.com/rest-live-v20/account-df/",
    },
}


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    name: str
    family: str
    rule: str
    timeframe: str
    source_key: str
    params: dict[str, Any]
    stop_atr: float = 1.5
    target_r: float = 1.8
    max_hold_bars: int = 24
    risk_pct: float = 1.0


@dataclass(frozen=True)
class TradeCandidate:
    strategy_id: str
    family: str
    instrument: str
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    direction: int
    entry_price: float
    exit_price: float
    stop_distance: float
    exit_reason: str
    gross_pips: float
    spread_pips: float
    signal_reason: str


@dataclass
class OpenTrade:
    candidate: TradeCandidate
    units: int
    margin_used: float
    entry_nav: float


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


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


def normalize_pair(value: str) -> str:
    text = str(value or "").strip().upper().replace("/", "_").replace("-", "_")
    if "_" not in text and len(text) == 6:
        return text[:3] + "_" + text[3:]
    return text


def split_pair(pair: str) -> tuple[str, str]:
    return tuple(normalize_pair(pair).split("_", 1))  # type: ignore[return-value]


def pip_multiplier(pair: str) -> float:
    return 100.0 if normalize_pair(pair) in PIP_LOCATION_MINUS2 else 10_000.0


def price_to_pips(pair: str, price_delta: float) -> float:
    return price_delta * pip_multiplier(pair)


def pips_to_price(pair: str, pips: float) -> float:
    return pips / pip_multiplier(pair)


def fallback_spread_pips(pair: str) -> float:
    base, quote = split_pair(pair)
    majors = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
    if base in majors and quote in majors:
        return 1.2
    if "JPY" in (base, quote):
        return 1.8
    if base in {"TRY", "ZAR", "MXN", "CNH"} or quote in {"TRY", "ZAR", "MXN", "CNH"}:
        return 8.0
    return 3.0


def discover_pairs() -> list[str]:
    return sorted(path.name[: -len("_M1.csv")] for path in CANDLE_ROOT.glob("*_M1.csv") if "_" in path.name)


def resolve_pairs(mode: str, explicit_pairs: str, max_pairs: int) -> list[str]:
    available = discover_pairs()
    available_set = set(available)
    if explicit_pairs.strip():
        pairs = [normalize_pair(item) for item in explicit_pairs.split(",") if item.strip()]
    elif mode == "majors":
        pairs = [pair for pair in MAJOR_PAIRS if pair in available_set]
    elif mode == "usd":
        pairs = [pair for pair in available if "USD" in split_pair(pair)]
    else:
        pairs = available
    pairs = [pair for pair in pairs if pair in available_set]
    if max_pairs > 0:
        pairs = pairs[:max_pairs]
    return pairs


def load_m1(pair: str, max_rows: int = 0) -> pd.DataFrame:
    path = CANDLE_ROOT / f"{pair}_M1.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing candle file: {path}")
    usecols = [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "spread_pips",
    ]
    header = pd.read_csv(path, nrows=0).columns.tolist()
    selected = [column for column in usecols if column in header]
    frame = pd.read_csv(path, usecols=selected)
    if max_rows > 0 and len(frame) > max_rows:
        frame = frame.tail(max_rows).copy()
    frame["time_utc"] = pd.to_datetime(frame["datetime"], errors="coerce", utc=True)
    for column in selected:
        if column == "datetime":
            continue
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", "open", "high", "low", "close"])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    if "spread_pips" not in frame or frame["spread_pips"].dropna().empty:
        frame["spread_pips"] = fallback_spread_pips(pair)
    else:
        frame["spread_pips"] = frame["spread_pips"].fillna(fallback_spread_pips(pair))
    return frame


def resample_bars(m1: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "open": m1["open"].resample(timeframe, label="right", closed="right").first(),
            "high": m1["high"].resample(timeframe, label="right", closed="right").max(),
            "low": m1["low"].resample(timeframe, label="right", closed="right").min(),
            "close": m1["close"].resample(timeframe, label="right", closed="right").last(),
            "spread_pips": m1["spread_pips"].resample(timeframe, label="right", closed="right").median(),
        }
    )
    return out.dropna(subset=["open", "high", "low", "close"])


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


def macd(close: pd.Series, fast: int, slow: int, signal: int) -> pd.DataFrame:
    fast_ema = close.ewm(span=fast, adjust=False, min_periods=fast).mean()
    slow_ema = close.ewm(span=slow, adjust=False, min_periods=slow).mean()
    line = fast_ema - slow_ema
    sig = line.ewm(span=signal, adjust=False, min_periods=signal).mean()
    return pd.DataFrame({"macd": line, "macd_signal": sig, "macd_hist": line - sig}, index=close.index)


def adx(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0), index=frame.index)
    atr_values = wilder(true_range(frame), window)
    plus_di = 100.0 * wilder(plus_dm, window) / atr_values.replace(0.0, np.nan)
    minus_di = 100.0 * wilder(minus_dm, window) / atr_values.replace(0.0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return pd.DataFrame({"adx": wilder(dx, window), "plus_di": plus_di, "minus_di": minus_di}, index=frame.index)


def stochastic(frame: pd.DataFrame, k_window: int, d_window: int) -> pd.DataFrame:
    low = frame["low"].rolling(k_window).min()
    high = frame["high"].rolling(k_window).max()
    k = 100.0 * (frame["close"] - low) / (high - low).replace(0.0, np.nan)
    d = k.rolling(d_window).mean()
    return pd.DataFrame({"stoch_k": k, "stoch_d": d}, index=frame.index)


def cci(frame: pd.DataFrame, window: int) -> pd.Series:
    typical = (frame["high"] + frame["low"] + frame["close"]) / 3.0
    mean = typical.rolling(window).mean()
    mad = (typical - mean).abs().rolling(window).mean()
    return (typical - mean) / (0.015 * mad.replace(0.0, np.nan))


def crossed_above(left: pd.Series, right: pd.Series | float) -> pd.Series:
    other = right if isinstance(right, pd.Series) else pd.Series(float(right), index=left.index)
    return (left > other) & (left.shift(1) <= other.shift(1))


def crossed_below(left: pd.Series, right: pd.Series | float) -> pd.Series:
    other = right if isinstance(right, pd.Series) else pd.Series(float(right), index=left.index)
    return (left < other) & (left.shift(1) >= other.shift(1))


def add_common_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    close = out["close"].astype(float)
    out["atr14"] = wilder(true_range(out), 14)
    out["rsi14"] = rsi(close, 14)
    out["ema5"] = close.ewm(span=5, adjust=False, min_periods=5).mean()
    out["ema8"] = close.ewm(span=8, adjust=False, min_periods=8).mean()
    out["ema10"] = close.ewm(span=10, adjust=False, min_periods=10).mean()
    out["ema12"] = close.ewm(span=12, adjust=False, min_periods=12).mean()
    out["ema20"] = close.ewm(span=20, adjust=False, min_periods=20).mean()
    out["ema21"] = close.ewm(span=21, adjust=False, min_periods=21).mean()
    out["ema26"] = close.ewm(span=26, adjust=False, min_periods=26).mean()
    out["ema30"] = close.ewm(span=30, adjust=False, min_periods=30).mean()
    out["ema50"] = close.ewm(span=50, adjust=False, min_periods=50).mean()
    out["ema75"] = close.ewm(span=75, adjust=False, min_periods=75).mean()
    out["ema100"] = close.ewm(span=100, adjust=False, min_periods=100).mean()
    out["ema200"] = close.ewm(span=200, adjust=False, min_periods=200).mean()
    out["sma20"] = close.rolling(20).mean()
    out["sma50"] = close.rolling(50).mean()
    out["sma200"] = close.rolling(200).mean()
    std20 = close.rolling(20).std()
    out["bb_mid20"] = out["sma20"]
    out["bb_upper20_2"] = out["sma20"] + 2.0 * std20
    out["bb_lower20_2"] = out["sma20"] - 2.0 * std20
    out["bb_width20"] = (out["bb_upper20_2"] - out["bb_lower20_2"]) / close.replace(0.0, np.nan)
    out["zscore20"] = (close - out["sma20"]) / std20.replace(0.0, np.nan)
    out["roc12"] = close.pct_change(12)
    out["roc24"] = close.pct_change(24)
    out["range_high20"] = out["high"].shift(1).rolling(20).max()
    out["range_low20"] = out["low"].shift(1).rolling(20).min()
    out["range_high55"] = out["high"].shift(1).rolling(55).max()
    out["range_low55"] = out["low"].shift(1).rolling(55).min()
    out = out.join(macd(close, 12, 26, 9))
    out = out.join(adx(out, 14))
    out = out.join(stochastic(out, 14, 3))
    out["cci20"] = cci(out, 20)
    out["tenkan"] = (out["high"].rolling(9).max() + out["low"].rolling(9).min()) / 2.0
    out["kijun"] = (out["high"].rolling(26).max() + out["low"].rolling(26).min()) / 2.0
    out["senkou_a"] = ((out["tenkan"] + out["kijun"]) / 2.0).shift(26)
    out["senkou_b"] = ((out["high"].rolling(52).max() + out["low"].rolling(52).min()) / 2.0).shift(26)
    daily = out[["high", "low", "close"]].resample("1D", label="right", closed="right").agg(
        {"high": "max", "low": "min", "close": "last"}
    )
    pivot = (daily["high"].shift(1) + daily["low"].shift(1) + daily["close"].shift(1)) / 3.0
    r1 = 2.0 * pivot - daily["low"].shift(1)
    s1 = 2.0 * pivot - daily["high"].shift(1)
    daily_pivots = pd.DataFrame({"pivot": pivot, "pivot_r1": r1, "pivot_s1": s1})
    out = out.join(daily_pivots.reindex(out.index, method="ffill"))
    candle_range = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["body"] = (out["close"] - out["open"]).abs()
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["body_pct"] = out["body"] / candle_range
    out["upper_wick_pct"] = out["upper_wick"] / candle_range
    out["lower_wick_pct"] = out["lower_wick"] / candle_range
    return out


def signal_series(spec: StrategySpec, features: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    p = spec.params
    close = features["close"]
    signal = pd.Series(0, index=features.index, dtype="int8")
    reason = pd.Series("", index=features.index, dtype="object")
    rule = spec.rule

    if rule == "ma_cross":
        fast = features[f"ema{int(p['fast'])}"]
        slow = features[f"ema{int(p['slow'])}"]
        trend = features[f"ema{int(p.get('trend', 200))}"]
        long_cond = crossed_above(fast, slow) & (close > trend)
        short_cond = crossed_below(fast, slow) & (close < trend)
    elif rule == "ma_pullback":
        fast = features[f"ema{int(p['fast'])}"]
        slow = features[f"ema{int(p['slow'])}"]
        rsi_values = features["rsi14"]
        long_cond = (close > slow) & (fast > slow) & crossed_above(close, fast) & rsi_values.between(38, 55)
        short_cond = (close < slow) & (fast < slow) & crossed_below(close, fast) & rsi_values.between(45, 62)
    elif rule == "rsi_reversion":
        low = float(p["low"])
        high = float(p["high"])
        rsi_values = features["rsi14"]
        long_cond = crossed_above(rsi_values, low) & (features["zscore20"] < -0.5)
        short_cond = crossed_below(rsi_values, high) & (features["zscore20"] > 0.5)
    elif rule == "rsi_trend":
        mid = float(p.get("mid", 50.0))
        rsi_values = features["rsi14"]
        long_cond = crossed_above(rsi_values, mid) & (close > features["ema50"])
        short_cond = crossed_below(rsi_values, 100.0 - mid) & (close < features["ema50"])
    elif rule == "macd_cross":
        long_cond = crossed_above(features["macd"], features["macd_signal"]) & (close > features["ema50"])
        short_cond = crossed_below(features["macd"], features["macd_signal"]) & (close < features["ema50"])
    elif rule == "macd_hist":
        threshold = float(p.get("threshold", 0.0))
        hist = features["macd_hist"]
        long_cond = crossed_above(hist, threshold)
        short_cond = crossed_below(hist, -threshold)
    elif rule == "bollinger_reversion":
        z = float(p.get("z", 2.0))
        long_cond = crossed_above(features["zscore20"], -z)
        short_cond = crossed_below(features["zscore20"], z)
    elif rule == "bollinger_breakout":
        min_width_q = float(p.get("min_width_quantile", 0.55))
        width_floor = features["bb_width20"].rolling(120, min_periods=40).quantile(min_width_q)
        active = features["bb_width20"] >= width_floor
        long_cond = (close > features["bb_upper20_2"]) & active
        short_cond = (close < features["bb_lower20_2"]) & active
    elif rule == "donchian_breakout":
        period = int(p["period"])
        high = features["high"].shift(1).rolling(period).max()
        low = features["low"].shift(1).rolling(period).min()
        long_cond = close > high
        short_cond = close < low
    elif rule == "stochastic_reversion":
        oversold = float(p.get("oversold", 20.0))
        overbought = float(p.get("overbought", 80.0))
        k = features["stoch_k"]
        d = features["stoch_d"]
        long_cond = crossed_above(k, d) & (k < oversold)
        short_cond = crossed_below(k, d) & (k > overbought)
    elif rule == "cci_reversion":
        level = float(p.get("level", 100.0))
        cci_values = features["cci20"]
        long_cond = crossed_above(cci_values, -level)
        short_cond = crossed_below(cci_values, level)
    elif rule == "adx_di":
        min_adx = float(p.get("min_adx", 20.0))
        adx_ok = features["adx"] >= min_adx
        long_cond = crossed_above(features["plus_di"], features["minus_di"]) & adx_ok
        short_cond = crossed_below(features["plus_di"], features["minus_di"]) & adx_ok
    elif rule == "atr_channel_breakout":
        mult = float(p.get("atr_mult", 1.5))
        basis = features["ema20"]
        long_cond = close > basis + mult * features["atr14"]
        short_cond = close < basis - mult * features["atr14"]
    elif rule == "keltner_reversion":
        mult = float(p.get("atr_mult", 1.5))
        basis = features["ema20"]
        long_cond = crossed_above(close, basis - mult * features["atr14"])
        short_cond = crossed_below(close, basis + mult * features["atr14"])
    elif rule == "ichimoku_cloud":
        cloud_top = pd.concat([features["senkou_a"], features["senkou_b"]], axis=1).max(axis=1)
        cloud_bottom = pd.concat([features["senkou_a"], features["senkou_b"]], axis=1).min(axis=1)
        long_cond = (features["tenkan"] > features["kijun"]) & (close > cloud_top)
        short_cond = (features["tenkan"] < features["kijun"]) & (close < cloud_bottom)
    elif rule == "pivot_breakout":
        long_cond = close > features["pivot_r1"]
        short_cond = close < features["pivot_s1"]
    elif rule == "pivot_reversion":
        long_cond = crossed_above(close, features["pivot_s1"])
        short_cond = crossed_below(close, features["pivot_r1"])
    elif rule == "inside_bar_breakout":
        inside = (features["high"].shift(1) < features["high"].shift(2)) & (features["low"].shift(1) > features["low"].shift(2))
        long_cond = inside & (close > features["high"].shift(1))
        short_cond = inside & (close < features["low"].shift(1))
    elif rule == "engulfing":
        prev_up = features["close"].shift(1) > features["open"].shift(1)
        prev_down = features["close"].shift(1) < features["open"].shift(1)
        cur_up = features["close"] > features["open"]
        cur_down = features["close"] < features["open"]
        long_cond = prev_down & cur_up & (features["close"] > features["open"].shift(1)) & (features["open"] < features["close"].shift(1))
        short_cond = prev_up & cur_down & (features["open"] > features["close"].shift(1)) & (features["close"] < features["open"].shift(1))
    elif rule == "reddit_h4_ema_engulfing_bounce":
        touch_atr = float(p.get("touch_atr", 0.20))
        min_adx = float(p.get("min_adx", 12.0))
        prev_up = features["close"].shift(1) > features["open"].shift(1)
        prev_down = features["close"].shift(1) < features["open"].shift(1)
        cur_up = features["close"] > features["open"]
        cur_down = features["close"] < features["open"]
        bullish_engulfing = (
            prev_down
            & cur_up
            & (features["close"] > features["open"].shift(1))
            & (features["open"] <= features["close"].shift(1))
        )
        bearish_engulfing = (
            prev_up
            & cur_down
            & (features["open"] >= features["close"].shift(1))
            & (features["close"] < features["open"].shift(1))
        )
        ema_zone_high = pd.concat([features["ema75"], features["ema100"], features["ema200"]], axis=1).max(axis=1)
        ema_zone_low = pd.concat([features["ema75"], features["ema100"], features["ema200"]], axis=1).min(axis=1)
        buffer = touch_atr * features["atr14"]
        long_trend = (features["ema75"] > features["ema100"]) & (features["ema100"] > features["ema200"]) & (close > features["ema200"])
        short_trend = (features["ema75"] < features["ema100"]) & (features["ema100"] < features["ema200"]) & (close < features["ema200"])
        long_bounce = (features["low"] <= ema_zone_high + buffer) & (features["close"] >= ema_zone_low)
        short_bounce = (features["high"] >= ema_zone_low - buffer) & (features["close"] <= ema_zone_high)
        context_ok = features["adx"] >= min_adx
        long_cond = bullish_engulfing & long_trend & long_bounce & context_ok
        short_cond = bearish_engulfing & short_trend & short_bounce & context_ok
    elif rule == "pin_bar_rejection":
        min_wick = float(p.get("min_wick_pct", 0.55))
        long_cond = (features["lower_wick_pct"] >= min_wick) & (close > features["ema20"])
        short_cond = (features["upper_wick_pct"] >= min_wick) & (close < features["ema20"])
    elif rule == "roc_momentum":
        lookback = int(p.get("lookback", 12))
        threshold = float(p.get("threshold", 0.001))
        roc = close.pct_change(lookback)
        long_cond = roc > threshold
        short_cond = roc < -threshold
    elif rule == "zscore_reversion":
        z = float(p.get("z", 1.5))
        long_cond = crossed_above(features["zscore20"], -z)
        short_cond = crossed_below(features["zscore20"], z)
    else:
        raise ValueError(f"Unsupported rule: {rule}")

    signal.loc[long_cond.fillna(False) & ~short_cond.fillna(False)] = 1
    signal.loc[short_cond.fillna(False) & ~long_cond.fillna(False)] = -1
    reason.loc[signal.ne(0)] = rule
    return signal, reason


def first_m1_after(index: pd.DatetimeIndex, ts: pd.Timestamp) -> int | None:
    pos = int(index.searchsorted(ts, side="right"))
    return None if pos >= len(index) else pos


def entry_time_allowed(ts: pd.Timestamp) -> bool:
    if ts.dayofweek >= 5:
        return False
    if ts.dayofweek == 4 and ts.hour >= 20:
        return False
    return True


def build_trade_candidates(
    pair: str,
    m1: pd.DataFrame,
    features: pd.DataFrame,
    spec: StrategySpec,
    max_signals_per_pair: int = 0,
) -> list[TradeCandidate]:
    signals, reasons = signal_series(spec, features)
    signal_rows = signals[signals.ne(0)]
    if max_signals_per_pair > 0 and len(signal_rows) > max_signals_per_pair:
        signal_rows = signal_rows.tail(max_signals_per_pair)
    if signal_rows.empty:
        return []

    m1_index = m1.index
    bar_delta = pd.to_timedelta(spec.timeframe)
    last_exit_time = m1_index[0]
    candidates: list[TradeCandidate] = []
    multiplier = pip_multiplier(pair)

    for signal_time, direction_value in signal_rows.items():
        if signal_time <= last_exit_time or not entry_time_allowed(signal_time):
            continue
        direction = int(direction_value)
        atr = finite_float(features.at[signal_time, "atr14"], 0.0)
        if atr <= 0.0:
            continue
        entry_pos = first_m1_after(m1_index, signal_time)
        if entry_pos is None:
            continue
        entry_time = m1_index[entry_pos]
        entry_price = finite_float(m1["open"].iloc[entry_pos], 0.0)
        if entry_price <= 0.0:
            continue
        spread_pips = finite_float(m1["spread_pips"].iloc[entry_pos], fallback_spread_pips(pair))
        stop_distance = max(atr * spec.stop_atr, pips_to_price(pair, spread_pips * 2.5))
        if stop_distance <= 0.0 or stop_distance / entry_price > 0.2:
            continue
        if direction > 0:
            stop_price = entry_price - stop_distance
            target_price = entry_price + stop_distance * spec.target_r
        else:
            stop_price = entry_price + stop_distance
            target_price = entry_price - stop_distance * spec.target_r

        max_exit_time = entry_time + spec.max_hold_bars * bar_delta
        end_pos = min(int(m1_index.searchsorted(max_exit_time, side="right")), len(m1_index))
        end_pos = max(end_pos, entry_pos + 1)
        exit_time = m1_index[end_pos - 1]
        exit_price = finite_float(m1["close"].iloc[end_pos - 1], entry_price)
        exit_reason = "time_stop"

        for pos in range(entry_pos + 1, end_pos):
            ts = m1_index[pos]
            high = finite_float(m1["high"].iloc[pos], entry_price)
            low = finite_float(m1["low"].iloc[pos], entry_price)
            if direction > 0:
                if low <= stop_price:
                    exit_time = ts
                    exit_price = stop_price
                    exit_reason = "stop"
                    break
                if high >= target_price:
                    exit_time = ts
                    exit_price = target_price
                    exit_reason = "target"
                    break
            else:
                if high >= stop_price:
                    exit_time = ts
                    exit_price = stop_price
                    exit_reason = "stop"
                    break
                if low <= target_price:
                    exit_time = ts
                    exit_price = target_price
                    exit_reason = "target"
                    break

        gross_pips = direction * (exit_price - entry_price) * multiplier
        candidates.append(
            TradeCandidate(
                strategy_id=spec.strategy_id,
                family=spec.family,
                instrument=pair,
                signal_time=signal_time,
                entry_time=entry_time,
                exit_time=exit_time,
                direction=direction,
                entry_price=entry_price,
                exit_price=exit_price,
                stop_distance=stop_distance,
                exit_reason=exit_reason,
                gross_pips=gross_pips,
                spread_pips=spread_pips,
                signal_reason=str(reasons.at[signal_time]),
            )
        )
        last_exit_time = exit_time
    return candidates


def quote_to_usd_rate(pair: str, price: float) -> float:
    base, quote = split_pair(pair)
    if quote == "USD":
        return 1.0
    if base == "USD":
        return 1.0 / max(price, 1e-9)
    return 1.0 / max(price, 1e-9)


def base_to_usd_rate(pair: str, price: float) -> float:
    base, quote = split_pair(pair)
    if base == "USD":
        return 1.0
    if quote == "USD":
        return price
    return 1.0


def trade_pl_usd(trade: TradeCandidate, units: int) -> float:
    direction = 1 if units > 0 else -1
    gross_quote = abs(units) * direction * (trade.exit_price - trade.entry_price)
    cost_quote = abs(units) * pips_to_price(trade.instrument, trade.spread_pips)
    return (gross_quote - cost_quote) * quote_to_usd_rate(trade.instrument, trade.exit_price)


def simulate_account(
    strategy_id: str,
    candidates: Sequence[TradeCandidate],
    specs_by_id: dict[str, StrategySpec],
    initial_nav: float,
    margin_rate: float,
    max_margin_pct: float,
    margin_closeout_percent: float,
    max_open_trades: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    balance = float(initial_nav)
    open_trades: list[OpenTrade] = []
    closed_rows: list[dict[str, Any]] = []
    equity_points: list[tuple[pd.Timestamp, float]] = []
    blocked_rows: list[dict[str, Any]] = []
    margin_closeout = False
    max_margin_used_pct = 0.0
    peak_balance = balance
    max_drawdown_pct = 0.0

    def close_due(now: pd.Timestamp) -> None:
        nonlocal balance, peak_balance, max_drawdown_pct
        due = [trade for trade in open_trades if trade.candidate.exit_time <= now]
        for open_trade in due:
            open_trades.remove(open_trade)
            pl = trade_pl_usd(open_trade.candidate, open_trade.units)
            balance += pl
            peak_balance = max(peak_balance, balance)
            drawdown = (peak_balance - balance) / max(peak_balance, 1e-9) * 100.0
            max_drawdown_pct = max(max_drawdown_pct, drawdown)
            equity_points.append((open_trade.candidate.exit_time, balance))
            closed_rows.append(
                {
                    "strategy_id": strategy_id,
                    "member_strategy_id": open_trade.candidate.strategy_id,
                    "family": open_trade.candidate.family,
                    "instrument": open_trade.candidate.instrument,
                    "entry_time": open_trade.candidate.entry_time.isoformat(),
                    "exit_time": open_trade.candidate.exit_time.isoformat(),
                    "direction": "LONG" if open_trade.units > 0 else "SHORT",
                    "units": open_trade.units,
                    "entry_price": open_trade.candidate.entry_price,
                    "exit_price": open_trade.candidate.exit_price,
                    "gross_pips": open_trade.candidate.gross_pips,
                    "spread_pips": open_trade.candidate.spread_pips,
                    "pl_usd": pl,
                    "balance_after": balance,
                    "exit_reason": open_trade.candidate.exit_reason,
                    "signal_reason": open_trade.candidate.signal_reason,
                }
            )

    for candidate in sorted(candidates, key=lambda item: (item.entry_time, item.instrument, item.strategy_id)):
        if margin_closeout:
            break
        close_due(candidate.entry_time)
        nav = balance
        if nav <= 0.0:
            margin_closeout = True
            break
        spec = specs_by_id[candidate.strategy_id]
        current_margin = sum(trade.margin_used for trade in open_trades)
        margin_used_pct = current_margin / max(nav, 1e-9) * 100.0
        max_margin_used_pct = max(max_margin_used_pct, margin_used_pct)
        if margin_used_pct >= margin_closeout_percent:
            margin_closeout = True
            break
        if len(open_trades) >= max_open_trades:
            blocked_rows.append(
                {
                    "strategy_id": strategy_id,
                    "member_strategy_id": candidate.strategy_id,
                    "instrument": candidate.instrument,
                    "entry_time": candidate.entry_time.isoformat(),
                    "reason": "max_open_trades",
                    "margin_used_pct": margin_used_pct,
                }
            )
            continue

        risk_usd = nav * max(spec.risk_pct, 0.01) / 100.0
        stop_quote_per_unit = candidate.stop_distance
        stop_usd_per_unit = stop_quote_per_unit * quote_to_usd_rate(candidate.instrument, candidate.entry_price)
        if stop_usd_per_unit <= 0.0:
            continue
        units = int(risk_usd / stop_usd_per_unit)
        if units <= 0:
            continue
        if candidate.direction < 0:
            units = -units
        margin_used = abs(units) * base_to_usd_rate(candidate.instrument, candidate.entry_price) * margin_rate
        projected_margin_pct = (current_margin + margin_used) / max(nav, 1e-9) * 100.0
        if projected_margin_pct > max_margin_pct:
            blocked_rows.append(
                {
                    "strategy_id": strategy_id,
                    "member_strategy_id": candidate.strategy_id,
                    "instrument": candidate.instrument,
                    "entry_time": candidate.entry_time.isoformat(),
                    "reason": "max_margin_pct",
                    "margin_used_pct": margin_used_pct,
                    "projected_margin_used_pct": projected_margin_pct,
                }
            )
            continue
        open_trades.append(OpenTrade(candidate=candidate, units=units, margin_used=margin_used, entry_nav=nav))
        max_margin_used_pct = max(max_margin_used_pct, projected_margin_pct)

    close_due(pd.Timestamp.max.tz_localize("UTC"))

    if closed_rows:
        pl = np.array([finite_float(row["pl_usd"]) for row in closed_rows], dtype=float)
        wins = pl[pl > 0.0]
        losses = -pl[pl < 0.0]
        win_rate = float((pl > 0.0).mean())
        profit_factor = float(wins.sum() / max(losses.sum(), 1e-9))
        avg_pl = float(pl.mean())
        best_trade = float(pl.max())
        worst_trade = float(pl.min())
    else:
        win_rate = profit_factor = avg_pl = best_trade = worst_trade = 0.0
    final_nav = balance
    return_pct = (final_nav - initial_nav) / max(initial_nav, 1e-9) * 100.0
    summary = {
        "strategy_id": strategy_id,
        "trade_count": len(closed_rows),
        "blocked_trade_count": len(blocked_rows),
        "initial_nav": initial_nav,
        "final_nav": final_nav,
        "net_pl_usd": final_nav - initial_nav,
        "return_pct": return_pct,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_pl_usd": avg_pl,
        "best_trade_usd": best_trade,
        "worst_trade_usd": worst_trade,
        "max_drawdown_pct": max_drawdown_pct,
        "max_margin_used_pct": max_margin_used_pct,
        "margin_closeout": margin_closeout,
        "score": return_pct - 0.65 * max_drawdown_pct - (25.0 if margin_closeout else 0.0),
    }
    return summary, closed_rows, blocked_rows


def append_strategy(
    catalog: list[StrategySpec],
    name: str,
    family: str,
    rule: str,
    timeframe: str,
    source_key: str,
    params: dict[str, Any],
    stop_atr: float,
    target_r: float,
    max_hold_bars: int,
    risk_pct: float,
) -> None:
    sid = f"s{len(catalog) + 1:03d}_{family}_{rule}_{timeframe}".lower().replace(" ", "_")
    catalog.append(
        StrategySpec(
            strategy_id=sid,
            name=name,
            family=family,
            rule=rule,
            timeframe=timeframe,
            source_key=source_key,
            params=params,
            stop_atr=stop_atr,
            target_r=target_r,
            max_hold_bars=max_hold_bars,
            risk_pct=risk_pct,
        )
    )


def build_strategy_catalog() -> list[StrategySpec]:
    catalog: list[StrategySpec] = []
    for fast, slow, tf in [
        (5, 20, "5min"),
        (8, 21, "15min"),
        (10, 30, "30min"),
        (20, 50, "1h"),
        (50, 200, "4h"),
        (12, 26, "1h"),
        (20, 50, "15min"),
        (10, 50, "1h"),
    ]:
        append_strategy(catalog, f"EMA {fast}/{slow} trend cross {tf}", "trend", "ma_cross", tf, "moving_average", {"fast": fast, "slow": slow, "trend": 200}, 1.5, 2.0, 24, 1.0)
    for fast, slow, tf in [
        (5, 20, "5min"),
        (8, 21, "15min"),
        (10, 30, "30min"),
        (20, 50, "1h"),
        (21, 100, "4h"),
        (12, 26, "15min"),
    ]:
        append_strategy(catalog, f"EMA pullback {fast}/{slow} {tf}", "pullback", "ma_pullback", tf, "moving_average", {"fast": fast, "slow": slow}, 1.3, 1.7, 20, 0.9)
    for low, high, tf in [
        (25, 75, "5min"),
        (30, 70, "15min"),
        (28, 72, "30min"),
        (35, 65, "1h"),
        (30, 70, "4h"),
        (20, 80, "15min"),
    ]:
        append_strategy(catalog, f"RSI reversion {low}/{high} {tf}", "mean_reversion", "rsi_reversion", tf, "rsi", {"low": low, "high": high}, 1.2, 1.3, 16, 0.8)
    for mid, tf in [(50, "15min"), (52, "30min"), (55, "1h"), (50, "4h"), (48, "15min")]:
        append_strategy(catalog, f"RSI trendline cross {mid} {tf}", "momentum", "rsi_trend", tf, "rsi", {"mid": mid}, 1.5, 1.8, 24, 0.9)
    for tf in ["5min", "15min", "30min", "1h"]:
        append_strategy(catalog, f"MACD signal cross {tf}", "momentum", "macd_cross", tf, "macd", {}, 1.5, 1.9, 24, 1.0)
    for threshold, tf in [(0.0, "15min"), (0.0, "1h")]:
        append_strategy(catalog, f"MACD histogram threshold {threshold} {tf}", "momentum", "macd_hist", tf, "macd", {"threshold": threshold}, 1.4, 1.6, 18, 0.9)
    for z, tf in [(1.5, "5min"), (1.7, "15min"), (2.0, "30min"), (2.0, "1h"), (2.2, "4h"), (1.3, "15min")]:
        append_strategy(catalog, f"Bollinger reversion z{z} {tf}", "mean_reversion", "bollinger_reversion", tf, "bollinger", {"z": z}, 1.2, 1.4, 16, 0.8)
    for q, tf in [(0.50, "5min"), (0.55, "15min"), (0.60, "30min"), (0.65, "1h"), (0.70, "4h"), (0.45, "15min")]:
        append_strategy(catalog, f"Bollinger width breakout q{q} {tf}", "breakout", "bollinger_breakout", tf, "bollinger", {"min_width_quantile": q}, 1.6, 2.0, 24, 1.0)
    for period, tf in [(20, "5min"), (20, "15min"), (20, "1h"), (55, "30min"), (55, "1h"), (55, "4h")]:
        append_strategy(catalog, f"Donchian {period} breakout {tf}", "breakout", "donchian_breakout", tf, "donchian", {"period": period}, 1.8, 2.2, 30, 1.0)
    for oversold, overbought, tf in [(20, 80, "5min"), (20, 80, "15min"), (25, 75, "30min"), (20, 80, "1h"), (15, 85, "1h")]:
        append_strategy(catalog, f"Stochastic reversion {oversold}/{overbought} {tf}", "mean_reversion", "stochastic_reversion", tf, "stochastic", {"oversold": oversold, "overbought": overbought}, 1.2, 1.3, 16, 0.7)
    for level, tf in [(100, "15min"), (100, "30min"), (100, "1h"), (150, "1h"), (200, "4h")]:
        append_strategy(catalog, f"CCI reversion {level} {tf}", "mean_reversion", "cci_reversion", tf, "cci", {"level": level}, 1.3, 1.4, 18, 0.8)
    for min_adx, tf in [(18, "15min"), (20, "30min"), (22, "1h"), (25, "4h"), (18, "1h")]:
        append_strategy(catalog, f"ADX DI trend {min_adx} {tf}", "trend", "adx_di", tf, "adx", {"min_adx": min_adx}, 1.6, 2.0, 26, 1.0)
    for mult, tf in [(1.0, "15min"), (1.2, "30min"), (1.5, "1h"), (2.0, "4h"), (1.5, "15min")]:
        append_strategy(catalog, f"ATR channel breakout {mult} {tf}", "volatility_breakout", "atr_channel_breakout", tf, "atr", {"atr_mult": mult}, 1.7, 2.0, 24, 1.0)
    for mult, tf in [(1.0, "15min"), (1.5, "30min"), (2.0, "1h")]:
        append_strategy(catalog, f"Keltner ATR reversion {mult} {tf}", "mean_reversion", "keltner_reversion", tf, "atr", {"atr_mult": mult}, 1.3, 1.4, 18, 0.8)
    for tf in ["15min", "30min", "1h", "4h"]:
        append_strategy(catalog, f"Ichimoku cloud trend {tf}", "trend", "ichimoku_cloud", tf, "ichimoku", {}, 1.7, 2.2, 30, 1.0)
    for tf in ["15min", "30min", "1h", "4h"]:
        append_strategy(catalog, f"Pivot R1/S1 breakout {tf}", "breakout", "pivot_breakout", tf, "pivot", {}, 1.4, 1.6, 18, 0.8)
    for tf in ["15min", "30min", "1h", "4h"]:
        append_strategy(catalog, f"Pivot S1/R1 reversion {tf}", "mean_reversion", "pivot_reversion", tf, "support_resistance", {}, 1.2, 1.3, 16, 0.7)
    for tf in ["5min", "15min", "30min", "1h"]:
        append_strategy(catalog, f"Inside bar breakout {tf}", "price_action", "inside_bar_breakout", tf, "candlestick", {}, 1.5, 1.8, 18, 0.8)
    for tf in ["15min", "30min"]:
        append_strategy(catalog, f"Engulfing reversal {tf}", "price_action", "engulfing", tf, "candlestick", {}, 1.2, 1.4, 14, 0.7)
    for wick, tf in [(0.55, "15min"), (0.65, "30min")]:
        append_strategy(catalog, f"Pin bar rejection {wick} {tf}", "price_action", "pin_bar_rejection", tf, "candlestick", {"min_wick_pct": wick}, 1.2, 1.5, 14, 0.7)
    for lookback, threshold, tf in [(12, 0.001, "15min"), (24, 0.0015, "30min"), (24, 0.002, "1h"), (48, 0.003, "4h")]:
        append_strategy(catalog, f"ROC momentum {lookback} {threshold} {tf}", "momentum", "roc_momentum", tf, "moving_average", {"lookback": lookback, "threshold": threshold}, 1.6, 2.0, 24, 1.0)
    for z, tf in [(1.5, "15min"), (2.0, "30min"), (2.5, "1h"), (3.0, "4h")]:
        append_strategy(catalog, f"Z-score mean reversion {z} {tf}", "mean_reversion", "zscore_reversion", tf, "support_resistance", {"z": z}, 1.2, 1.3, 16, 0.7)

    if len(catalog) != 100:
        raise AssertionError(f"Catalog should contain exactly 100 strategies, found {len(catalog)}")
    return catalog


def catalog_rows(catalog: Sequence[StrategySpec]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in catalog:
        source = SOURCE_REGISTRY[spec.source_key]
        rows.append(
            {
                **asdict(spec),
                "source_title": source["title"],
                "source_url": source["url"],
                "params": json.dumps(spec.params, sort_keys=True),
            }
        )
    return rows


def summary_rows_to_markdown(rows: Sequence[dict[str, Any]], limit: int = 20) -> str:
    lines = [
        "| rank | strategy | family | trades | return % | PF | win % | max DD % | max margin % | score |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(rows[:limit], start=1):
        lines.append(
            f"| {rank} | {row.get('strategy_id')} | {row.get('family', '')} | "
            f"{int(row.get('trade_count', 0))} | {finite_float(row.get('return_pct')):.2f} | "
            f"{finite_float(row.get('profit_factor')):.2f} | {100.0 * finite_float(row.get('win_rate')):.1f} | "
            f"{finite_float(row.get('max_drawdown_pct')):.2f} | {finite_float(row.get('max_margin_used_pct')):.2f} | "
            f"{finite_float(row.get('score')):.2f} |"
        )
    return "\n".join(lines)


def build_ensemble_candidates(
    top_rows: Sequence[dict[str, Any]],
    candidates_by_strategy: dict[str, list[TradeCandidate]],
    max_members: int,
    top_n: int,
) -> list[tuple[str, list[TradeCandidate], list[str]]]:
    selected_ids = [str(row["strategy_id"]) for row in top_rows[:top_n]]
    ensembles: list[tuple[str, list[TradeCandidate], list[str]]] = []
    for size in range(2, max_members + 1):
        for members in itertools.combinations(selected_ids, size):
            merged: list[TradeCandidate] = []
            for member in members:
                merged.extend(candidates_by_strategy.get(member, []))
            ensemble_id = "ens" + str(size) + "_" + "__".join(members)
            ensembles.append((ensemble_id, merged, list(members)))
    return ensembles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs-mode", choices=["majors", "usd", "all"], default="majors")
    parser.add_argument("--pairs", default="", help="Comma-separated explicit pair list.")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--max-rows-per-pair", type=int, default=0, help="Use latest N M1 rows per pair; 0 means all local rows.")
    parser.add_argument("--max-signals-per-pair", type=int, default=0)
    parser.add_argument("--initial-nav", type=float, default=10_000.0)
    parser.add_argument("--margin-rate", type=float, default=0.033333)
    parser.add_argument("--max-margin-pct", type=float, default=90.0)
    parser.add_argument("--margin-closeout-percent", type=float, default=100.0)
    parser.add_argument("--max-open-trades", type=int, default=8)
    parser.add_argument("--min-trades", type=int, default=20)
    parser.add_argument("--ensemble-top-n", type=int, default=12)
    parser.add_argument("--ensemble-max-members", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    catalog = build_strategy_catalog()
    specs_by_id = {spec.strategy_id: spec for spec in catalog}
    pairs = resolve_pairs(args.pairs_mode, args.pairs, args.max_pairs)
    if not pairs:
        raise RuntimeError(f"No matching local M1 pairs found in {CANDLE_ROOT}")

    output_dir = args.output_dir
    catalog_json = output_dir / "strategy_catalog_100.json"
    catalog_csv = output_dir / "strategy_catalog_100.csv"
    atomic_write_json(catalog_json, catalog_rows(catalog))
    write_csv(catalog_csv, catalog_rows(catalog))

    features_by_pair_timeframe: dict[tuple[str, str], pd.DataFrame] = {}
    m1_by_pair: dict[str, pd.DataFrame] = {}
    timeframes = sorted({spec.timeframe for spec in catalog}, key=lambda item: pd.to_timedelta(item))
    for idx, pair in enumerate(pairs, start=1):
        print(f"[load] {idx:02d}/{len(pairs):02d} {pair}", flush=True)
        m1 = load_m1(pair, max_rows=int(args.max_rows_per_pair))
        m1_by_pair[pair] = m1
        for timeframe in timeframes:
            features_by_pair_timeframe[(pair, timeframe)] = add_common_features(resample_bars(m1, timeframe))

    candidates_by_strategy: dict[str, list[TradeCandidate]] = {}
    individual_rows: list[dict[str, Any]] = []
    all_trade_rows: list[dict[str, Any]] = []
    all_blocked_rows: list[dict[str, Any]] = []

    for idx, spec in enumerate(catalog, start=1):
        print(f"[strategy] {idx:03d}/100 {spec.strategy_id}", flush=True)
        candidates: list[TradeCandidate] = []
        for pair in pairs:
            candidates.extend(
                build_trade_candidates(
                    pair,
                    m1_by_pair[pair],
                    features_by_pair_timeframe[(pair, spec.timeframe)],
                    spec,
                    max_signals_per_pair=int(args.max_signals_per_pair),
                )
            )
        candidates_by_strategy[spec.strategy_id] = candidates
        summary, trade_rows, blocked_rows = simulate_account(
            spec.strategy_id,
            candidates,
            specs_by_id,
            initial_nav=float(args.initial_nav),
            margin_rate=float(args.margin_rate),
            max_margin_pct=float(args.max_margin_pct),
            margin_closeout_percent=float(args.margin_closeout_percent),
            max_open_trades=int(args.max_open_trades),
        )
        summary.update(
            {
                "name": spec.name,
                "family": spec.family,
                "rule": spec.rule,
                "timeframe": spec.timeframe,
                "source_url": SOURCE_REGISTRY[spec.source_key]["url"],
                "candidate_count": len(candidates),
            }
        )
        individual_rows.append(summary)
        all_trade_rows.extend(trade_rows)
        all_blocked_rows.extend(blocked_rows)

    viable_rows = [
        row
        for row in individual_rows
        if int(row.get("trade_count", 0)) >= int(args.min_trades)
        and not bool(row.get("margin_closeout", False))
    ]
    ranked_individual = sorted(
        viable_rows or individual_rows,
        key=lambda row: (
            finite_float(row.get("score"), -999999.0),
            finite_float(row.get("return_pct"), -999999.0),
            finite_float(row.get("profit_factor"), 0.0),
        ),
        reverse=True,
    )

    ensemble_rows: list[dict[str, Any]] = []
    ensemble_members_rows: list[dict[str, Any]] = []
    if args.ensemble_top_n > 1 and args.ensemble_max_members >= 2:
        ensembles = build_ensemble_candidates(
            ranked_individual,
            candidates_by_strategy,
            max_members=int(args.ensemble_max_members),
            top_n=int(args.ensemble_top_n),
        )
        for idx, (ensemble_id, merged_candidates, members) in enumerate(ensembles, start=1):
            print(f"[ensemble] {idx:04d}/{len(ensembles):04d} {ensemble_id}", flush=True)
            summary, trade_rows, blocked_rows = simulate_account(
                ensemble_id,
                merged_candidates,
                specs_by_id,
                initial_nav=float(args.initial_nav),
                margin_rate=float(args.margin_rate),
                max_margin_pct=float(args.max_margin_pct),
                margin_closeout_percent=float(args.margin_closeout_percent),
                max_open_trades=int(args.max_open_trades),
            )
            summary.update(
                {
                    "name": ensemble_id,
                    "family": "ensemble",
                    "rule": "top_n_combination",
                    "timeframe": "mixed",
                    "source_url": "multiple",
                    "member_count": len(members),
                    "members": ",".join(members),
                    "candidate_count": len(merged_candidates),
                }
            )
            ensemble_rows.append(summary)
            for member in members:
                ensemble_members_rows.append({"ensemble_id": ensemble_id, "member_strategy_id": member})
            all_trade_rows.extend(trade_rows[:500])
            all_blocked_rows.extend(blocked_rows[:500])

    ranked_ensembles = sorted(
        ensemble_rows,
        key=lambda row: (
            finite_float(row.get("score"), -999999.0),
            finite_float(row.get("return_pct"), -999999.0),
            finite_float(row.get("profit_factor"), 0.0),
        ),
        reverse=True,
    )
    combined_ranked = sorted(
        [*ranked_individual, *ranked_ensembles],
        key=lambda row: (
            finite_float(row.get("score"), -999999.0),
            finite_float(row.get("return_pct"), -999999.0),
            finite_float(row.get("profit_factor"), 0.0),
        ),
        reverse=True,
    )

    individual_csv = output_dir / "individual_strategy_results.csv"
    ensemble_csv = output_dir / "ensemble_results.csv"
    ensemble_members_csv = output_dir / "ensemble_members.csv"
    trades_csv = output_dir / "sample_trades.csv"
    blocked_csv = output_dir / "sample_blocked_trades.csv"
    summary_json = output_dir / "latest_strategy_100_research.json"
    summary_md = output_dir / "latest_strategy_100_research.md"

    write_csv(individual_csv, individual_rows)
    write_csv(ensemble_csv, ensemble_rows)
    write_csv(ensemble_members_csv, ensemble_members_rows)
    write_csv(trades_csv, all_trade_rows[:50_000])
    write_csv(blocked_csv, all_blocked_rows[:50_000])

    best = combined_ranked[0] if combined_ranked else {}
    payload = {
        "generated_at_utc": utc_now(),
        "strategy_count": len(catalog),
        "pairs": pairs,
        "pair_count": len(pairs),
        "timeframes": timeframes,
        "initial_nav": float(args.initial_nav),
        "margin_rate": float(args.margin_rate),
        "max_margin_pct": float(args.max_margin_pct),
        "margin_closeout_percent": float(args.margin_closeout_percent),
        "max_open_trades": int(args.max_open_trades),
        "min_trades": int(args.min_trades),
        "individual_strategy_count": len(individual_rows),
        "ensemble_count": len(ensemble_rows),
        "best_result": best,
        "top_individual": ranked_individual[:20],
        "top_ensembles": ranked_ensembles[:20],
        "top_overall": combined_ranked[:20],
        "files": {
            "catalog_json": str(catalog_json),
            "catalog_csv": str(catalog_csv),
            "individual_csv": str(individual_csv),
            "ensemble_csv": str(ensemble_csv),
            "ensemble_members_csv": str(ensemble_members_csv),
            "trades_csv": str(trades_csv),
            "blocked_csv": str(blocked_csv),
            "summary_json": str(summary_json),
            "summary_md": str(summary_md),
        },
        "source_registry": SOURCE_REGISTRY,
        "simulation_notes": [
            "Signals are generated on resampled bars from local OANDA M1 candles.",
            "Entries use the next available M1 open after the signal bar.",
            "Stops and targets are walked through M1 high/low data.",
            "Spread cost uses historical spread_pips when present, otherwise a pair-class fallback.",
            "Margin is approximated as abs(units) * base_to_usd * margin_rate.",
            "The OANDA-like margin closeout check uses margin_used_pct >= margin_closeout_percent.",
            "The ensemble search tests all 2- and 3-member combinations among the top individual strategies by default, not all 2^100 subsets.",
        ],
    }
    atomic_write_json(summary_json, payload)
    lines = [
        "# 100 Strategy OANDA M1 Research",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Scope",
        "",
        f"- Strategies: `{len(catalog)}` source-inspired variants.",
        f"- Pairs: `{len(pairs)}` (`{', '.join(pairs)}`).",
        f"- Initial NAV per simulated account: `${float(args.initial_nav):,.2f}`.",
        f"- Margin rate: `{float(args.margin_rate):.5f}`; max margin used: `{float(args.max_margin_pct):.1f}%`; closeout threshold: `{float(args.margin_closeout_percent):.1f}%`.",
        "",
        "## Top Overall",
        "",
        summary_rows_to_markdown(combined_ranked, limit=20),
        "",
        "## Top Individual Strategies",
        "",
        summary_rows_to_markdown(ranked_individual, limit=20),
        "",
        "## Files",
        "",
    ]
    for label, path in payload["files"].items():
        lines.append(f"- {label}: `{path}`")
    lines.extend(
        [
            "",
            "## Important Limits",
            "",
            "- This is a research backtest, not investment advice and not a live execution recommendation.",
            "- The script models spread and margin, but not broker latency, partial fills, financing, swap, weekend gap fills, or exact OANDA margin-by-instrument rules.",
            "- The ensemble search is bounded to top-N pair/triple combinations because all non-empty subsets of 100 strategies would require about 1.27e30 simulations.",
            "",
        ]
    )
    atomic_write_text(summary_md, "\n".join(lines))
    print(json.dumps(payload["best_result"], indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
