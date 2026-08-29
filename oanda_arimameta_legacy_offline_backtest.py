#!/usr/bin/env python3
"""Offline backtest/replication harness for the MT5 ARIMA meta script.

The source file supplied by the user (`arimameta_multi_xxl_arima(2).py`) is a
live MT5 simulator.  This harness ports the important strategy definitions to
local OANDA parquet data and deliberately separates two modes:

- legacy: mimics the source simulator's bar-close accounting as closely as is
  practical.  The decision is computed after the scored bar is already known,
  so this is useful for reproducing old dashboard-style rows, not promotion.
- causal: computes the decision using only information available at entry time,
  then scores the following bar.  This is the production-relevant mode.

No broker, MT5, or webhook calls are made.
"""

from __future__ import annotations

import argparse
import json
import math
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.arima.model import ARIMA

warnings.filterwarnings("ignore", category=ConvergenceWarning)
warnings.filterwarnings("ignore", category=PerformanceWarning)
warnings.filterwarnings("ignore", message=".*no associated frequency information.*")
warnings.filterwarnings("ignore", message=".*No supported index is available.*")
warnings.filterwarnings("ignore", message=".*Maximum Likelihood optimization failed to converge.*")
warnings.filterwarnings("ignore", message=".*Non-stationary starting autoregressive parameters found.*")
warnings.filterwarnings("ignore", message=".*Non-invertible starting MA parameters found.*")


PROJECT_ROOT = Path(__file__).resolve().parent
FEATURE_ROOT = PROJECT_ROOT / "data" / "all68_weekly_move_study" / "features"
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "reports"
PIPELINE_VERSION = "arimameta_legacy_offline_v1"

SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD"]

HORIZON_BY_TF = {
    "H1": 12,
    "M15": 24,
    "M5": 36,
    "M1": 60,
}

DEFAULT_SPREAD_PIPS = 0.8
SLIPPAGE_STD_PIPS_BY_TF = {"H1": 0.10, "M15": 0.15, "M5": 0.20, "M1": 0.30}
FEE_PIPS_BY_TF = {"H1": 0.05, "M15": 0.05, "M5": 0.06, "M1": 0.08}
EDGE_FACTOR = 1.0
START_EQUITY = 100.0
PIP_VALUE_PER_UNIT = 0.10
TIERS = {"BASE": 1.0, "XL": 2.0, "XXL": 4.0}


@dataclass(frozen=True)
class BaseModel:
    key: str
    family: str
    timeframe: str
    source_tf: str
    order: tuple[int, int, int] | None = None
    sma_len: int = 20
    breakout_buffer_pips: float = 5.0
    atr_len: int = 14
    atr_mult: float = 0.7
    event_quantile: float = 0.6
    fast_ma: int = 10
    slow_ma: int = 30


BASE_MODELS: dict[str, BaseModel] = {
    "D1BRK_017": BaseModel("D1BRK_017", "D1_TREND_TF_BREAKOUT", timeframe="H1", source_tf="H1", breakout_buffer_pips=5.0, sma_len=20),
    "D1BRK_022": BaseModel("D1BRK_022", "D1_TREND_TF_BREAKOUT", timeframe="H1", source_tf="H1", breakout_buffer_pips=10.0, sma_len=20),
    "D1BRK_018": BaseModel("D1BRK_018", "D1_TREND_TF_BREAKOUT", timeframe="H1", source_tf="H1", breakout_buffer_pips=5.0, sma_len=20),
    "D1BRK_016": BaseModel("D1BRK_016", "D1_TREND_TF_BREAKOUT", timeframe="H1", source_tf="H1", breakout_buffer_pips=5.0, sma_len=20),
    "H1ATR_032": BaseModel("H1ATR_032", "TF_ARIMA_ATR", timeframe="H1", source_tf="H1", order=(2, 0, 1), atr_mult=0.7),
    "EV_091": BaseModel("EV_091", "TF_EVENT_ARIMA", timeframe="H1", source_tf="H1", order=(1, 2, 2), event_quantile=0.60),
    "NAIVE_ARIMA": BaseModel("NAIVE_ARIMA", "TF_ARIMA", timeframe="H1", source_tf="H1", order=(0, 1, 1)),
    "MA_CROSS": BaseModel("MA_CROSS", "MA_CROSS", timeframe="H1", source_tf="H1", fast_ma=10, slow_ma=30),
    "M1_EV_091": BaseModel("M1_EV_091", "TF_EVENT_ARIMA", timeframe="M1", source_tf="M1", order=(1, 2, 2), event_quantile=0.98),
    "M1_NAIVE": BaseModel("M1_NAIVE", "TF_ARIMA", timeframe="M1", source_tf="M1", order=(0, 1, 1)),
    "M1_MA_CROSS": BaseModel("M1_MA_CROSS", "MA_CROSS", timeframe="M1", source_tf="M1", fast_ma=50, slow_ma=200),
    "M5_EV_091": BaseModel("M5_EV_091", "TF_EVENT_ARIMA", timeframe="M5", source_tf="M1", order=(1, 2, 2), event_quantile=0.90),
    "M15_ATR_032": BaseModel("M15_ATR_032", "TF_ARIMA_ATR", timeframe="M15", source_tf="M1", order=(2, 0, 1), atr_len=14, atr_mult=0.8),
    "M15_NAIVE": BaseModel("M15_NAIVE", "TF_ARIMA", timeframe="M15", source_tf="M1", order=(0, 1, 1)),
}

REFERENCE_MODEL_KEYS = ["EV_091", "M5_EV_091", "M15_ATR_032"]


@dataclass
class ModelStats:
    equity: float = START_EQUITY
    net_pips: float = 0.0
    gross_pips: float = 0.0
    total_cost_pips: float = 0.0
    spread_pips: float = 0.0
    slippage_pips: float = 0.0
    fee_pips: float = 0.0
    trades: int = 0
    wins: int = 0


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_safe), encoding="utf-8")


def symbol_to_pair(symbol: str) -> str:
    return f"{symbol[:3]}_{symbol[3:]}"


def pip_factor(symbol: str) -> float:
    return 100.0 if symbol.endswith("JPY") else 10000.0


def tf_to_pandas_freq(tf: str) -> str:
    return {"H1": "h", "M15": "15min", "M5": "5min", "M1": "min"}[tf]


def load_oanda_base(symbol: str, since: str = "") -> pd.DataFrame:
    pair = symbol_to_pair(symbol)
    path = FEATURE_ROOT / f"{pair}.parquet"
    columns = ["open", "high", "low", "close", "spread_pips", "volume"]
    frame = pd.read_parquet(path, columns=columns).copy()
    frame.index = pd.to_datetime(frame.index, utc=True)
    frame = frame.sort_index()
    frame = frame.rename(columns={"volume": "tick_volume"})
    if since:
        frame = frame[frame.index >= pd.Timestamp(since, tz="UTC")]
    return frame.dropna(subset=["open", "high", "low", "close"])


def resample_ohlc(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    rule = tf_to_pandas_freq(tf)
    out = pd.DataFrame(
        {
            "open": df["open"].astype(float).resample(rule).first(),
            "high": df["high"].astype(float).resample(rule).max(),
            "low": df["low"].astype(float).resample(rule).min(),
            "close": df["close"].astype(float).resample(rule).last(),
            "tick_volume": df["tick_volume"].astype(float).resample(rule).sum(),
            "spread_pips": df["spread_pips"].astype(float).resample(rule).mean(),
        }
    )
    return out.dropna(subset=["open", "high", "low", "close"])


def load_tf(symbol: str, tf: str, since: str = "") -> pd.DataFrame:
    base = load_oanda_base(symbol, since=since)
    if tf == "M5":
        return base
    if tf == "M15":
        return resample_ohlc(base, "M15")
    if tf == "H1":
        return resample_ohlc(base, "H1")
    raise ValueError(f"Local OANDA feature store is M5-based; unsupported tf={tf}")


def ensure_freq(series: pd.Series, freq: str) -> pd.Series:
    series = series.sort_index()
    idx = pd.date_range(series.index.min(), series.index.max(), freq=freq, tz="UTC")
    return series.reindex(idx).ffill().bfill()


def arima_forecast_last(
    close: pd.Series,
    order: tuple[int, int, int],
    horizon_steps: int,
    freq: str,
) -> tuple[list[float], float, bool]:
    s2 = ensure_freq(close, freq=freq)
    try:
        fit = ARIMA(s2, order=order).fit()
        pred = fit.get_forecast(steps=horizon_steps)
        mean = pred.predicted_mean
        ci = pred.conf_int(alpha=0.20)
        if ci is not None and len(ci) > 0:
            half = (ci.iloc[:, 1] - ci.iloc[:, 0]).astype(float) / 2.0
            sigma = float(np.nanmean(half.values))
        else:
            sigma = float(np.nanstd(s2.diff().dropna().values))
        return [float(v) for v in mean.values], sigma, True
    except Exception:
        last = float(close.iloc[-1])
        sigma = float(np.nanstd(close.diff().dropna().values)) if len(close) > 5 else 0.0
        return [last] * horizon_steps, sigma, False


def atr_series(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev = close.shift(1)
    tr = pd.concat([(high - low).abs(), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.rolling(length).mean()


def ma_cross_dir(close: pd.Series, fast: int, slow: int) -> int:
    f = close.ewm(span=fast).mean()
    s = close.ewm(span=slow).mean()
    if len(f) < 3:
        return 0
    prev = f.iloc[-2] - s.iloc[-2]
    curr = f.iloc[-1] - s.iloc[-1]
    if prev <= 0 and curr > 0:
        return 1
    if prev >= 0 and curr < 0:
        return -1
    return 0


def event_gate(close: pd.Series, q: float) -> bool:
    rets = close.diff().abs().dropna()
    if len(rets) < 50:
        return False
    return bool(rets.iloc[-1] >= rets.quantile(q))


def d1_trend_dir(d1: pd.DataFrame, sma_len: int) -> int:
    c = d1["close"].astype(float)
    if len(c) < sma_len + 2:
        return 0
    sma = c.rolling(sma_len).mean()
    if sma.iloc[-1] > sma.iloc[-2]:
        return 1
    if sma.iloc[-1] < sma.iloc[-2]:
        return -1
    return 0


def compute_fc_metrics(symbol: str, tf: str, close: pd.Series, fc_vals: list[float], sigma_est: float) -> dict[str, float]:
    last = float(close.iloc[-1])
    pf = pip_factor(symbol)
    fc_1 = float(fc_vals[0]) if fc_vals else last
    fc_6 = float(fc_vals[min(5, len(fc_vals) - 1)]) if fc_vals else last
    fc_1step_pips = (fc_1 - last) * pf
    fc_6step_pips = (fc_6 - last) * pf
    sigma_pips = max(1e-6, float((sigma_est if sigma_est else np.nanstd(close.diff().dropna().values)) * pf))
    snr = abs(fc_1step_pips) / sigma_pips
    conf = 1.0 / (1.0 + np.exp(-2.0 * (snr - 0.8)))
    conf_pct = float(np.clip(conf * 100.0, 1.0, 99.0))

    step_minutes = {"H1": 60, "M15": 15, "M5": 5, "M1": 1}[tf]

    def fc_at_minutes(minutes: int) -> float:
        steps = max(1, int(round(minutes / step_minutes)))
        idx = min(steps - 1, len(fc_vals) - 1) if fc_vals else 0
        val = float(fc_vals[idx]) if fc_vals else last
        return (val - last) * pf

    return {
        "last_close": last,
        "tf": tf,
        "fc_1step_pips": float(fc_1step_pips),
        "fc_6step_pips": float(fc_6step_pips),
        "conf_pct": conf_pct,
        "sigma_pips": float(sigma_pips),
        "slope_ppstep": float(fc_6step_pips / 6.0),
        "fc_15m_pips": float(fc_at_minutes(15)),
        "fc_30m_pips": float(fc_at_minutes(30)),
        "fc_1h_pips": float(fc_at_minutes(60)),
        "fc_6h_pips": float(fc_at_minutes(360)),
        "slope_pph": float(fc_at_minutes(360) / 6.0),
    }


def direction_without_arima(symbol: str, bm: BaseModel, history: pd.DataFrame, d1_history: pd.DataFrame | None) -> int | None:
    close = history["close"].astype(float)
    if len(close) < 80:
        return 0
    if bm.family == "D1_TREND_TF_BREAKOUT":
        if d1_history is None:
            return 0
        trend = d1_trend_dir(d1_history, bm.sma_len)
        if trend == 0:
            return 0
        curr_close = float(close.iloc[-1])
        last_d1 = float(d1_history["close"].astype(float).iloc[-1])
        buf = bm.breakout_buffer_pips / pip_factor(symbol)
        if trend > 0 and curr_close > last_d1 + buf:
            return 1
        if trend < 0 and curr_close < last_d1 - buf:
            return -1
        return 0
    if bm.family == "MA_CROSS":
        return ma_cross_dir(close, bm.fast_ma, bm.slow_ma)
    if bm.family == "TF_ARIMA_ATR":
        atr = atr_series(history, bm.atr_len).dropna()
        if len(atr) < 50:
            return 0
        if float(atr.iloc[-1]) < bm.atr_mult * float(atr.median()):
            return 0
        return None
    if bm.family == "TF_EVENT_ARIMA":
        if not event_gate(close, bm.event_quantile):
            return 0
        return None
    if bm.family == "TF_ARIMA":
        return None
    return None


def deterministic_slippage_pips(symbol: str, tf: str, timestamp: pd.Timestamp) -> float:
    # Deterministic substitute for the MT5 script's random absolute normal draw.
    std = float(SLIPPAGE_STD_PIPS_BY_TF.get(tf, 0.20))
    seed = abs(hash((symbol, tf, timestamp.value))) % 10_000
    phase = (seed / 10_000.0) * 2.0 * math.pi
    return abs(math.sin(phase)) * std


def fee_pips(tf: str, size_mult: float) -> float:
    base = float(FEE_PIPS_BY_TF.get(tf, 0.05))
    return float(base * max(0.25, size_mult))


def update_stats(
    stats: dict[str, ModelStats],
    *,
    key: str,
    raw_pips: float,
    spread_pips: float,
    slippage_pips: float,
    fee: float,
    total_cost_pips: float,
    size_mult: float,
) -> float:
    net_pips = (raw_pips - total_cost_pips) * size_mult
    ms = stats.setdefault(key, ModelStats())
    ms.gross_pips += raw_pips * size_mult
    ms.net_pips += net_pips
    ms.spread_pips += spread_pips * size_mult
    ms.slippage_pips += slippage_pips * size_mult
    ms.fee_pips += fee * size_mult
    ms.total_cost_pips += total_cost_pips * size_mult
    ms.equity += net_pips * PIP_VALUE_PER_UNIT
    ms.trades += 1
    if net_pips > 0:
        ms.wins += 1
    return net_pips


def iter_model_indices(
    frame: pd.DataFrame,
    *,
    min_train_bars: int,
    max_train_bars: int,
    predict_every: int,
    tail_bars: int,
) -> Iterable[int]:
    start = max(min_train_bars, 1)
    stop = len(frame) - 1
    if tail_bars > 0:
        start = max(start, stop - tail_bars)
    for i in range(start, stop):
        if (i - start) % max(1, predict_every) == 0:
            yield i


def backtest_symbol_model(
    symbol: str,
    bm: BaseModel,
    frame: pd.DataFrame,
    d1: pd.DataFrame | None,
    *,
    mode: str,
    min_train_bars: int,
    max_train_bars: int,
    predict_every: int,
    tail_bars: int,
    max_fits: int,
    tiers: list[str],
) -> tuple[dict[str, ModelStats], list[dict[str, Any]], dict[str, Any]]:
    stats: dict[str, ModelStats] = {}
    trades: list[dict[str, Any]] = []
    fit_count = 0
    candidate_count = 0
    skipped_edge = 0
    arima_failures = 0
    pf = pip_factor(symbol)
    freq = tf_to_pandas_freq(bm.timeframe)
    horizon = int(HORIZON_BY_TF.get(bm.timeframe, 12))
    order = bm.order if bm.order is not None else (0, 1, 1)

    for i in iter_model_indices(
        frame,
        min_train_bars=min_train_bars,
        max_train_bars=max_train_bars,
        predict_every=predict_every,
        tail_bars=tail_bars,
    ):
        if mode == "legacy":
            decision_i = i
            entry_i = i - 1
            exit_i = i
        elif mode == "causal":
            decision_i = i - 1
            entry_i = i - 1
            exit_i = i
        else:
            raise ValueError(f"Unknown mode={mode}")
        if decision_i < min_train_bars or entry_i < 0 or exit_i >= len(frame):
            continue

        start_i = max(0, decision_i + 1 - max_train_bars)
        history = frame.iloc[start_i : decision_i + 1]
        d1_history = None
        if d1 is not None:
            d1_history = d1[d1.index <= history.index[-1]]

        direction = direction_without_arima(symbol, bm, history, d1_history)
        if direction == 0:
            continue
        candidate_count += 1

        fc = {"fc_1step_pips": 0.0, "conf_pct": 50.0}
        if direction is None:
            if max_fits > 0 and fit_count >= max_fits:
                break
            fc_vals, sigma, ok = arima_forecast_last(history["close"].astype(float), order, horizon, freq)
            fit_count += 1
            if not ok:
                arima_failures += 1
            fc = compute_fc_metrics(symbol, bm.timeframe, history["close"].astype(float), fc_vals, sigma)
            fc_1step_pips = float(fc.get("fc_1step_pips", 0.0))
            direction = 1 if fc_1step_pips > 0 else (-1 if fc_1step_pips < 0 else 0)
            if direction == 0:
                continue

        expected = abs(float(fc.get("fc_1step_pips", 0.0)))
        conf_pct = float(fc.get("conf_pct", 1.0))
        conf = conf_pct / 100.0
        conf_w = max(0.10, conf)
        entry_px = float(frame["close"].iloc[entry_i])
        exit_px = float(frame["close"].iloc[exit_i])
        raw_pips = (exit_px - entry_px) * pf * (1 if direction > 0 else -1)
        timestamp = pd.Timestamp(frame.index[exit_i])
        spread_pips = float(frame["spread_pips"].iloc[exit_i]) if "spread_pips" in frame else DEFAULT_SPREAD_PIPS
        if not np.isfinite(spread_pips) or spread_pips <= 0:
            spread_pips = DEFAULT_SPREAD_PIPS
        slippage_pips = deterministic_slippage_pips(symbol, bm.timeframe, timestamp)

        for tier in tiers:
            tier_mult = TIERS[tier]
            mk = bm.key if tier == "BASE" else f"{bm.key}_{tier}"
            size_mult = float(tier_mult) * (0.50 + 1.50 * conf_w)
            fee = fee_pips(bm.timeframe, size_mult)
            total_cost_pips = float(spread_pips + slippage_pips + fee)
            if expected < EDGE_FACTOR * total_cost_pips:
                skipped_edge += 1
                continue
            net_pips = update_stats(
                stats,
                key=f"{symbol}|{mk}",
                raw_pips=float(raw_pips),
                spread_pips=spread_pips,
                slippage_pips=slippage_pips,
                fee=fee,
                total_cost_pips=total_cost_pips,
                size_mult=size_mult,
            )
            trades.append(
                {
                    "symbol": symbol,
                    "model": mk,
                    "base_model": bm.key,
                    "tier": tier,
                    "family": bm.family,
                    "timeframe": bm.timeframe,
                    "mode": mode,
                    "direction": "LONG" if direction > 0 else "SHORT",
                    "decision_time": frame.index[decision_i],
                    "entry_time": frame.index[entry_i],
                    "exit_time": frame.index[exit_i],
                    "entry_price": entry_px,
                    "exit_price": exit_px,
                    "raw_pips": raw_pips,
                    "spread_pips": spread_pips,
                    "slippage_pips": slippage_pips,
                    "fee_pips": fee,
                    "total_cost_pips": total_cost_pips,
                    "net_pips": net_pips,
                    "conf_pct": conf_pct,
                    "size_mult": size_mult,
                    "fc_1step_pips": float(fc.get("fc_1step_pips", 0.0)),
                    "fc_6step_pips": float(fc.get("fc_6step_pips", 0.0)),
                }
            )

    diagnostics = {
        "fit_count": fit_count,
        "candidate_count": candidate_count,
        "skipped_edge": skipped_edge,
        "arima_failures": arima_failures,
    }
    return stats, trades, diagnostics


def summarize_rows(
    stats: dict[str, ModelStats],
    *,
    symbols: list[str],
    models: dict[str, BaseModel],
    tiers: list[str],
) -> tuple[pd.DataFrame, str]:
    rows: list[dict[str, Any]] = []
    lines: list[str] = []
    total_net = 0.0
    total_gross = 0.0
    total_cost = 0.0
    total_trades = 0

    for symbol in symbols:
        best: tuple[float, str] | None = None
        for model_key, bm in models.items():
            for tier in tiers:
                mk = model_key if tier == "BASE" else f"{model_key}_{tier}"
                ms = stats.get(f"{symbol}|{mk}", ModelStats())
                wr = ms.wins / ms.trades if ms.trades else 0.0
                avg = ms.net_pips / ms.trades if ms.trades else 0.0
                cost_ratio = abs(ms.total_cost_pips) / (abs(ms.gross_pips) + 1e-9) if ms.gross_pips else 0.0
                row = {
                    "symbol": symbol,
                    "model": mk,
                    "base_model": model_key,
                    "tier": tier,
                    "family": bm.family,
                    "timeframe": bm.timeframe,
                    "source_tf": bm.source_tf,
                    "equity": ms.equity,
                    "trades": ms.trades,
                    "wins": ms.wins,
                    "win_rate": wr,
                    "gross_pips": ms.gross_pips,
                    "total_cost_pips": ms.total_cost_pips,
                    "spread_pips": ms.spread_pips,
                    "slippage_pips": ms.slippage_pips,
                    "fee_pips": ms.fee_pips,
                    "net_pips": ms.net_pips,
                    "avg_net_pips_trade": avg,
                    "cost_ratio": cost_ratio,
                }
                rows.append(row)
                if tier == "BASE":
                    total_net += ms.net_pips
                    total_gross += ms.gross_pips
                    total_cost += ms.total_cost_pips
                    total_trades += ms.trades
                    line = (
                        f"{symbol} {model_key}({bm.timeframe}) Eq={ms.equity:.2f} "
                        f"Tr={ms.trades} WR={wr:.0%} Net={ms.net_pips:+.0f}p "
                        f"Cost={ms.total_cost_pips:+.0f}p"
                    )
                    if best is None or ms.equity > best[0]:
                        best = (ms.equity, line)
        if best:
            lines.append(best[1])

    avg_global = total_net / total_trades if total_trades else 0.0
    cost_ratio_global = abs(total_cost) / (abs(total_gross) + 1e-9) if total_gross else 0.0
    lines.append(
        f"Global (BASE) Trades={total_trades} Net={total_net:+.0f}p "
        f"AvgNet/Trade={avg_global:+.2f}p CostRatio={cost_ratio_global:.0%}"
    )
    return pd.DataFrame(rows), "\n".join(lines)


def parse_models(text: str) -> dict[str, BaseModel]:
    if text == "reference":
        keys = REFERENCE_MODEL_KEYS
    elif text == "all":
        keys = [key for key, model in BASE_MODELS.items() if model.timeframe != "M1"]
    else:
        keys = [part.strip() for part in text.split(",") if part.strip()]
    missing = [key for key in keys if key not in BASE_MODELS]
    if missing:
        raise ValueError(f"Unknown model keys: {missing}")
    unsupported = [key for key in keys if BASE_MODELS[key].timeframe == "M1"]
    if unsupported:
        raise ValueError(f"M1 models are unsupported by this M5 local feature store: {unsupported}")
    return {key: BASE_MODELS[key] for key in keys}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["legacy", "causal"], default="causal")
    parser.add_argument("--symbols", default=",".join(SYMBOLS), help="Comma-separated MT5-style symbols.")
    parser.add_argument("--models", default="reference", help="'reference', 'all', or comma-separated model keys.")
    parser.add_argument("--tiers", default="BASE", help="Comma-separated tiers: BASE,XL,XXL.")
    parser.add_argument("--since", default="", help="Optional UTC start timestamp, e.g. 2025-06-01.")
    parser.add_argument("--min-train-bars", type=int, default=120)
    parser.add_argument("--max-train-bars", type=int, default=500)
    parser.add_argument("--predict-every", type=int, default=1)
    parser.add_argument("--tail-bars", type=int, default=0, help="Only evaluate this many latest bars per symbol/model/tf; 0=all.")
    parser.add_argument("--max-fits-per-model", type=int, default=0, help="0=no limit.")
    parser.add_argument("--output-prefix", default="latest_arimameta_legacy_offline")
    args = parser.parse_args()

    symbols = [part.strip().upper() for part in args.symbols.split(",") if part.strip()]
    models = parse_models(args.models)
    tiers = [part.strip().upper() for part in args.tiers.split(",") if part.strip()]
    bad_tiers = [tier for tier in tiers if tier not in TIERS]
    if bad_tiers:
        raise ValueError(f"Unknown tiers: {bad_tiers}")

    all_stats: dict[str, ModelStats] = {}
    all_trades: list[dict[str, Any]] = []
    diagnostics: dict[str, Any] = {}

    for symbol in symbols:
        print(f"[arimameta-offline] {symbol}", flush=True)
        frames: dict[str, pd.DataFrame] = {}
        for tf in sorted({model.timeframe for model in models.values()}):
            frames[tf] = load_tf(symbol, tf, since=args.since)
        d1 = resample_ohlc(load_oanda_base(symbol, since=args.since), "D1") if any(
            model.family == "D1_TREND_TF_BREAKOUT" for model in models.values()
        ) else None
        for model_key, bm in models.items():
            frame = frames[bm.timeframe]
            print(f"  - {model_key} {bm.timeframe} rows={len(frame)}", flush=True)
            stats, trades, diag = backtest_symbol_model(
                symbol,
                bm,
                frame,
                d1,
                mode=args.mode,
                min_train_bars=args.min_train_bars,
                max_train_bars=args.max_train_bars,
                predict_every=args.predict_every,
                tail_bars=args.tail_bars,
                max_fits=args.max_fits_per_model,
                tiers=tiers,
            )
            all_stats.update(stats)
            all_trades.extend(trades)
            diagnostics[f"{symbol}|{model_key}"] = diag

    summary_df, snapshot = summarize_rows(all_stats, symbols=symbols, models=models, tiers=tiers)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    prefix = args.output_prefix
    summary_csv = REPORT_ROOT / f"{prefix}_summary.csv"
    trades_csv = REPORT_ROOT / f"{prefix}_trades.csv"
    json_path = REPORT_ROOT / f"{prefix}.json"
    summary_df.to_csv(summary_csv, index=False)
    pd.DataFrame(all_trades).to_csv(trades_csv, index=False)
    payload = {
        "pipeline_version": PIPELINE_VERSION,
        "mode": args.mode,
        "symbols": symbols,
        "models": {key: asdict(model) for key, model in models.items()},
        "tiers": tiers,
        "since": args.since,
        "min_train_bars": args.min_train_bars,
        "max_train_bars": args.max_train_bars,
        "predict_every": args.predict_every,
        "tail_bars": args.tail_bars,
        "max_fits_per_model": args.max_fits_per_model,
        "summary_csv": summary_csv,
        "trades_csv": trades_csv,
        "snapshot": snapshot,
        "diagnostics": diagnostics,
        "notes": [
            "legacy mode is for reproducing the MT5 simulator's old dashboard rows; it is not promotion-safe.",
            "causal mode uses only information available at entry time and should be used for model promotion.",
            "M5/M15 are sourced from local OANDA M5 feature parquet, not true MT5 M1 resampling.",
        ],
    }
    write_json(json_path, payload)
    print(snapshot, flush=True)
    print(f"json={json_path}", flush=True)
    print(f"summary_csv={summary_csv}", flush=True)
    print(f"trades_csv={trades_csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
