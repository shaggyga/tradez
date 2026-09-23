#!/usr/bin/env python3
"""Mechanical M1 gold liquidity-sweep/FVG research engine.

This module turns a discretionary gold scalping description into explicit,
testable rules:

1. Optional 15m/1h EMA bias filter.
2. M1 liquidity sweep of recent highs/lows.
3. Displacement candle in the intended direction.
4. Three-candle fair value gap.
5. Limit entry on a retrace into the gap.
6. Stop beyond the sweep and target opposing liquidity or a fixed R fallback.

It is deliberately research/signal code. It does not place live orders.
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
from typing import Any, Iterable, Sequence

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
REPORT_ROOT = SCRIPT_DIR / "data" / "gold_m1_liquidity_scalper" / "reports"


TIMEFRAME_ALIASES = {
    "M1": "1min",
    "1M": "1min",
    "1MIN": "1min",
    "M5": "5min",
    "5M": "5min",
    "5MIN": "5min",
    "M15": "15min",
    "15M": "15min",
    "15MIN": "15min",
    "H1": "1h",
    "1H": "1h",
    "H4": "4h",
    "4H": "4h",
}


@dataclass(frozen=True)
class StrategyConfig:
    """Rules and contract math for MGC-style gold backtests."""

    htf_timeframes: tuple[str, ...] = ("15min", "1h")
    require_htf_bias: bool = True
    htf_fast_ema: int = 9
    htf_slow_ema: int = 21

    sweep_lookback_bars: int = 20
    sweep_recent_bars: int = 3
    target_lookback_bars: int = 60
    cooldown_bars: int = 8

    atr_period: int = 14
    displacement_atr_mult: float = 1.20
    displacement_min_body_frac: float = 0.55
    fvg_min_points: float = 0.20

    entry_zone_fraction: float = 0.50
    entry_wait_bars: int = 8
    stop_buffer_points: float = 0.20
    min_stop_points: float = 1.00
    max_stop_points: float = 12.00
    min_rr: float = 1.50
    fallback_rr: float = 2.00
    fallback_to_rr_target: bool = True

    risk_pct: float = 0.50
    point_value: float = 10.0  # MGC: $10 per full $1.00 gold move.
    commission_round_turn: float = 3.00
    slippage_points_round_turn: float = 0.20
    min_contracts: int = 1
    max_contracts: int = 10

    max_open_positions: int = 1
    max_trades_per_day: int = 8
    daily_loss_limit_pct: float = 2.0
    session_start_hour_utc: int = -1  # -1 disables session filtering.
    session_end_hour_utc: int = -1


@dataclass(frozen=True)
class Signal:
    index: int
    time_utc: str
    direction: str
    htf_bias: str
    sweep_time_utc: str
    sweep_level: float
    displacement_close: float
    fvg_low: float
    fvg_high: float
    entry: float
    stop: float
    target: float
    risk_points: float
    reward_points: float
    rr: float
    expires_index: int
    expires_utc: str


@dataclass(frozen=True)
class Trade:
    signal_time_utc: str
    entry_time_utc: str
    exit_time_utc: str
    direction: str
    contracts: int
    entry: float
    stop: float
    target: float
    exit: float
    exit_reason: str
    pnl: float
    r_multiple: float
    equity_before: float
    equity_after: float
    risk_dollars: float
    rr_planned: float
    fvg_low: float
    fvg_high: float
    sweep_level: float


@dataclass(frozen=True)
class PendingOrder:
    signal: Signal


@dataclass(frozen=True)
class Position:
    signal: Signal
    entry_index: int
    entry_time_utc: str
    contracts: int
    equity_before: float
    risk_dollars: float


def utc_now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True, default=json_safe))


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        atomic_write_text(path, "")
        return
    fields = sorted({key for row in rows for key in row})
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def normalize_timeframe(value: str) -> str:
    text = str(value or "").strip()
    return TIMEFRAME_ALIASES.get(text.upper(), text.lower())


def parse_timeframes(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace(";", ",").split(",")]
    else:
        parts = [str(part).strip() for part in value]
    return tuple(normalize_timeframe(part) for part in parts if part)


def parse_utc(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _find_column(columns: Sequence[str], candidates: Sequence[str]) -> str | None:
    lookup = {column.lower().strip(): column for column in columns}
    for candidate in candidates:
        found = lookup.get(candidate.lower())
        if found:
            return found
    return None


def load_ohlc_csv(path: Path, since: str = "", until: str = "") -> pd.DataFrame:
    """Load a broker/export OHLC CSV into a normalized M1 DataFrame."""

    raw = pd.read_csv(path)
    if raw.empty:
        raise ValueError(f"CSV has no rows: {path}")

    time_col = _find_column(raw.columns, ("time_utc", "datetime", "timestamp", "time", "date"))
    open_col = _find_column(raw.columns, ("open", "o", "mid_o", "bid_o", "ask_o"))
    high_col = _find_column(raw.columns, ("high", "h", "mid_h", "bid_h", "ask_h"))
    low_col = _find_column(raw.columns, ("low", "l", "mid_l", "bid_l", "ask_l"))
    close_col = _find_column(raw.columns, ("close", "c", "mid_c", "bid_c", "ask_c"))
    volume_col = _find_column(raw.columns, ("volume", "tick_volume", "real_volume", "vol"))
    spread_col = _find_column(raw.columns, ("spread_points", "spread", "spread_pips"))

    required = {
        "time": time_col,
        "open": open_col,
        "high": high_col,
        "low": low_col,
        "close": close_col,
    }
    missing = [name for name, column in required.items() if column is None]
    if missing:
        raise ValueError(f"Missing required CSV columns {missing} in {path}")

    frame = pd.DataFrame(
        {
            "time_utc": pd.to_datetime(raw[time_col], errors="coerce", utc=True),
            "open": pd.to_numeric(raw[open_col], errors="coerce"),
            "high": pd.to_numeric(raw[high_col], errors="coerce"),
            "low": pd.to_numeric(raw[low_col], errors="coerce"),
            "close": pd.to_numeric(raw[close_col], errors="coerce"),
        }
    )
    frame["volume"] = pd.to_numeric(raw[volume_col], errors="coerce") if volume_col else 0.0
    frame["spread_points"] = pd.to_numeric(raw[spread_col], errors="coerce") if spread_col else 0.0

    frame = frame.dropna(subset=["time_utc", "open", "high", "low", "close"])
    since_ts = parse_utc(since)
    until_ts = parse_utc(until)
    if since_ts is not None:
        frame = frame[frame["time_utc"] >= since_ts]
    if until_ts is not None:
        frame = frame[frame["time_utc"] <= until_ts]

    frame = (
        frame.sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .set_index("time_utc")
    )
    if frame.empty:
        raise ValueError("No candles remain after filtering")

    return frame


def resample_ohlc(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    rule = normalize_timeframe(timeframe)
    agg: dict[str, str] = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "spread_points": "mean",
    }
    return (
        frame.resample(rule, label="right", closed="right")
        .agg({key: value for key, value in agg.items() if key in frame.columns})
        .dropna(subset=["open", "high", "low", "close"])
    )


def add_atr(frame: pd.DataFrame, period: int) -> pd.Series:
    prev_close = frame["close"].shift(1)
    tr = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period, min_periods=max(2, min(period, 5))).mean()


def htf_bias_for_frame(frame: pd.DataFrame, cfg: StrategyConfig) -> pd.Series:
    """Return 1, -1, or 0 for the combined higher-timeframe bias."""

    if not cfg.require_htf_bias or not cfg.htf_timeframes:
        return pd.Series(1, index=frame.index, dtype="int64")

    components: list[pd.Series] = []
    for timeframe in cfg.htf_timeframes:
        htf = resample_ohlc(frame, timeframe)
        if htf.empty:
            continue
        fast = htf["close"].ewm(span=cfg.htf_fast_ema, adjust=False).mean()
        slow = htf["close"].ewm(span=cfg.htf_slow_ema, adjust=False).mean()
        bias = pd.Series(0, index=htf.index, dtype="int64")
        bias[(fast > slow) & (htf["close"] > slow)] = 1
        bias[(fast < slow) & (htf["close"] < slow)] = -1
        aligned = bias.reindex(frame.index, method="ffill").fillna(0).shift(1).fillna(0).astype("int64")
        components.append(aligned)

    if not components:
        return pd.Series(0, index=frame.index, dtype="int64")

    combined = components[0].copy()
    for component in components[1:]:
        combined = combined.where((combined == component) & (combined != 0), 0)
    return combined.astype("int64")


def prepare_frame(frame: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    prepared = frame.copy()
    prepared["atr"] = add_atr(prepared, cfg.atr_period)
    prepared["htf_bias"] = htf_bias_for_frame(prepared, cfg)
    return prepared


def is_in_session(ts: pd.Timestamp, cfg: StrategyConfig) -> bool:
    if cfg.session_start_hour_utc < 0 or cfg.session_end_hour_utc < 0:
        return True
    hour = ts.hour
    start = cfg.session_start_hour_utc
    end = cfg.session_end_hour_utc
    if start == end:
        return True
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _direction_label(direction: int) -> str:
    return "LONG" if direction > 0 else "SHORT"


def _bias_label(direction: int) -> str:
    if direction > 0:
        return "BULLISH"
    if direction < 0:
        return "BEARISH"
    return "NEUTRAL"


def _recent_sweep(
    frame: pd.DataFrame,
    signal_index: int,
    direction: int,
    cfg: StrategyConfig,
) -> tuple[int, float] | None:
    start = max(cfg.sweep_lookback_bars, signal_index - cfg.sweep_recent_bars + 1)
    for idx in range(signal_index, start - 1, -1):
        prior_start = idx - cfg.sweep_lookback_bars
        if prior_start < 0:
            continue
        prior = frame.iloc[prior_start:idx]
        row = frame.iloc[idx]
        if prior.empty:
            continue
        if direction > 0:
            prior_low = float(prior["low"].min())
            if float(row["low"]) < prior_low and float(row["close"]) > prior_low:
                return idx, float(row["low"])
        else:
            prior_high = float(prior["high"].max())
            if float(row["high"]) > prior_high and float(row["close"]) < prior_high:
                return idx, float(row["high"])
    return None


def _has_displacement(row: pd.Series, direction: int, cfg: StrategyConfig) -> bool:
    atr = float(row.get("atr", 0.0) or 0.0)
    if atr <= 0.0 or not math.isfinite(atr):
        return False
    candle_range = float(row["high"] - row["low"])
    body = abs(float(row["close"] - row["open"]))
    if candle_range <= 0.0:
        return False
    if body / candle_range < cfg.displacement_min_body_frac:
        return False
    if body < atr * cfg.displacement_atr_mult:
        return False
    if direction > 0:
        return float(row["close"]) > float(row["open"])
    return float(row["close"]) < float(row["open"])


def _fvg_edges(frame: pd.DataFrame, index: int, direction: int, cfg: StrategyConfig) -> tuple[float, float] | None:
    if index < 2:
        return None
    first = frame.iloc[index - 2]
    third = frame.iloc[index]
    if direction > 0 and float(first["high"]) < float(third["low"]):
        low = float(first["high"])
        high = float(third["low"])
    elif direction < 0 and float(first["low"]) > float(third["high"]):
        low = float(third["high"])
        high = float(first["low"])
    else:
        return None
    if high - low < cfg.fvg_min_points:
        return None
    return low, high


def _opposing_liquidity_target(
    frame: pd.DataFrame,
    index: int,
    direction: int,
    entry: float,
    risk_points: float,
    cfg: StrategyConfig,
) -> float:
    lookback_start = max(0, index - cfg.target_lookback_bars)
    prior = frame.iloc[lookback_start:index]
    if prior.empty:
        return entry + direction * cfg.fallback_rr * risk_points
    if direction > 0:
        target = float(prior["high"].max())
        min_target = entry + cfg.min_rr * risk_points
        if target <= entry or target < min_target:
            if cfg.fallback_to_rr_target:
                target = entry + cfg.fallback_rr * risk_points
        return target
    target = float(prior["low"].min())
    max_target = entry - cfg.min_rr * risk_points
    if target >= entry or target > max_target:
        if cfg.fallback_to_rr_target:
            target = entry - cfg.fallback_rr * risk_points
    return target


def _build_signal(frame: pd.DataFrame, index: int, direction: int, cfg: StrategyConfig) -> Signal | None:
    row = frame.iloc[index]
    ts = frame.index[index]
    if not is_in_session(ts, cfg):
        return None
    if cfg.require_htf_bias:
        bias = int(row.get("htf_bias", 0))
        if bias != direction:
            return None
    if not _has_displacement(row, direction, cfg):
        return None
    fvg = _fvg_edges(frame, index, direction, cfg)
    if not fvg:
        return None
    sweep = _recent_sweep(frame, index, direction, cfg)
    if not sweep:
        return None

    fvg_low, fvg_high = fvg
    if direction > 0:
        entry = fvg_low + (fvg_high - fvg_low) * cfg.entry_zone_fraction
        stop = sweep[1] - cfg.stop_buffer_points
        risk_points = entry - stop
    else:
        entry = fvg_high - (fvg_high - fvg_low) * cfg.entry_zone_fraction
        stop = sweep[1] + cfg.stop_buffer_points
        risk_points = stop - entry

    if risk_points < cfg.min_stop_points or risk_points > cfg.max_stop_points:
        return None

    target = _opposing_liquidity_target(frame, index, direction, entry, risk_points, cfg)
    reward_points = (target - entry) * direction
    if reward_points <= 0.0:
        return None
    rr = reward_points / risk_points if risk_points > 0 else 0.0
    if rr < cfg.min_rr:
        return None

    expires_index = min(len(frame) - 1, index + cfg.entry_wait_bars)
    return Signal(
        index=index,
        time_utc=ts.isoformat(),
        direction=_direction_label(direction),
        htf_bias=_bias_label(direction if cfg.require_htf_bias else 0),
        sweep_time_utc=frame.index[sweep[0]].isoformat(),
        sweep_level=round(float(sweep[1]), 5),
        displacement_close=round(float(row["close"]), 5),
        fvg_low=round(fvg_low, 5),
        fvg_high=round(fvg_high, 5),
        entry=round(entry, 5),
        stop=round(stop, 5),
        target=round(target, 5),
        risk_points=round(risk_points, 5),
        reward_points=round(reward_points, 5),
        rr=round(rr, 4),
        expires_index=expires_index,
        expires_utc=frame.index[expires_index].isoformat(),
    )


def generate_signals(frame: pd.DataFrame, cfg: StrategyConfig) -> list[Signal]:
    prepared = frame if {"atr", "htf_bias"}.issubset(frame.columns) else prepare_frame(frame, cfg)
    signals: list[Signal] = []
    last_signal_index = -10_000
    start = max(cfg.sweep_lookback_bars + 2, cfg.atr_period + 2)
    for index in range(start, len(prepared)):
        if index - last_signal_index < cfg.cooldown_bars:
            continue
        candidates: list[Signal] = []
        for direction in (1, -1):
            signal = _build_signal(prepared, index, direction, cfg)
            if signal:
                candidates.append(signal)
        if not candidates:
            continue
        candidates.sort(key=lambda item: item.rr, reverse=True)
        signals.append(candidates[0])
        last_signal_index = index
    return signals


def round_turn_cost_per_contract(cfg: StrategyConfig) -> float:
    return cfg.commission_round_turn + cfg.slippage_points_round_turn * cfg.point_value


def calculate_contracts(equity: float, signal: Signal, cfg: StrategyConfig) -> tuple[int, float]:
    risk_budget = equity * cfg.risk_pct / 100.0
    risk_per_contract = signal.risk_points * cfg.point_value + round_turn_cost_per_contract(cfg)
    if risk_budget <= 0.0 or risk_per_contract <= 0.0:
        return 0, 0.0
    contracts = int(math.floor(risk_budget / risk_per_contract))
    contracts = min(contracts, cfg.max_contracts)
    if contracts < cfg.min_contracts:
        return 0, 0.0
    return contracts, risk_per_contract * contracts


def _date_key(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%d")


def _fillable(row: pd.Series, signal: Signal) -> bool:
    return float(row["low"]) <= signal.entry <= float(row["high"])


def _exit_for_bar(row: pd.Series, position: Position) -> tuple[float, str] | None:
    signal = position.signal
    high = float(row["high"])
    low = float(row["low"])
    if signal.direction == "LONG":
        stop_hit = low <= signal.stop
        target_hit = high >= signal.target
    else:
        stop_hit = high >= signal.stop
        target_hit = low <= signal.target
    if stop_hit:
        return signal.stop, "STOP"
    if target_hit:
        return signal.target, "TARGET"
    return None


def _trade_from_exit(
    position: Position,
    exit_time_utc: str,
    exit_price: float,
    exit_reason: str,
    equity_after: float,
    cfg: StrategyConfig,
) -> Trade:
    signal = position.signal
    direction = 1 if signal.direction == "LONG" else -1
    gross = (exit_price - signal.entry) * direction * cfg.point_value * position.contracts
    cost = round_turn_cost_per_contract(cfg) * position.contracts
    pnl = gross - cost
    risk = position.risk_dollars if position.risk_dollars > 0 else 1.0
    return Trade(
        signal_time_utc=signal.time_utc,
        entry_time_utc=position.entry_time_utc,
        exit_time_utc=exit_time_utc,
        direction=signal.direction,
        contracts=position.contracts,
        entry=signal.entry,
        stop=signal.stop,
        target=signal.target,
        exit=round(exit_price, 5),
        exit_reason=exit_reason,
        pnl=round(pnl, 2),
        r_multiple=round(pnl / risk, 4),
        equity_before=round(position.equity_before, 2),
        equity_after=round(equity_after, 2),
        risk_dollars=round(position.risk_dollars, 2),
        rr_planned=signal.rr,
        fvg_low=signal.fvg_low,
        fvg_high=signal.fvg_high,
        sweep_level=signal.sweep_level,
    )


def backtest(frame: pd.DataFrame, cfg: StrategyConfig, initial_equity: float) -> tuple[dict[str, Any], list[Trade], list[Signal]]:
    prepared = prepare_frame(frame, cfg)
    signals = generate_signals(prepared, cfg)
    signals_by_index = {signal.index: signal for signal in signals}

    equity = float(initial_equity)
    peak_equity = equity
    max_drawdown_pct = 0.0
    trades: list[Trade] = []
    pending: PendingOrder | None = None
    position: Position | None = None
    skipped_too_small = 0
    expired_orders = 0
    daily_trades: dict[str, int] = {}
    daily_pnl: dict[str, float] = {}
    daily_start_equity: dict[str, float] = {}

    for index, (ts, row) in enumerate(prepared.iterrows()):
        day = _date_key(ts)
        daily_start_equity.setdefault(day, equity)
        daily_trades.setdefault(day, 0)
        daily_pnl.setdefault(day, 0.0)

        if position is not None:
            exit_event = _exit_for_bar(row, position)
            if exit_event:
                exit_price, exit_reason = exit_event
                direction = 1 if position.signal.direction == "LONG" else -1
                gross = (exit_price - position.signal.entry) * direction * cfg.point_value * position.contracts
                cost = round_turn_cost_per_contract(cfg) * position.contracts
                pnl = gross - cost
                equity_after = equity + pnl
                trade = _trade_from_exit(position, ts.isoformat(), exit_price, exit_reason, equity_after, cfg)
                trades.append(trade)
                daily_pnl[day] = daily_pnl.get(day, 0.0) + trade.pnl
                equity = equity_after
                peak_equity = max(peak_equity, equity)
                if peak_equity > 0:
                    max_drawdown_pct = max(max_drawdown_pct, (peak_equity - equity) / peak_equity * 100.0)
                position = None

        if pending is not None and position is None:
            if index > pending.signal.expires_index:
                expired_orders += 1
                pending = None
            elif index > pending.signal.index and _fillable(row, pending.signal):
                contracts, risk_dollars = calculate_contracts(equity, pending.signal, cfg)
                if contracts <= 0:
                    skipped_too_small += 1
                    pending = None
                else:
                    position = Position(
                        signal=pending.signal,
                        entry_index=index,
                        entry_time_utc=ts.isoformat(),
                        contracts=contracts,
                        equity_before=equity,
                        risk_dollars=risk_dollars,
                    )
                    daily_trades[day] = daily_trades.get(day, 0) + 1
                    pending = None
                    same_bar_exit = _exit_for_bar(row, position)
                    if same_bar_exit:
                        exit_price, exit_reason = same_bar_exit
                        direction = 1 if position.signal.direction == "LONG" else -1
                        gross = (exit_price - position.signal.entry) * direction * cfg.point_value * position.contracts
                        cost = round_turn_cost_per_contract(cfg) * position.contracts
                        pnl = gross - cost
                        equity_after = equity + pnl
                        trade = _trade_from_exit(position, ts.isoformat(), exit_price, exit_reason, equity_after, cfg)
                        trades.append(trade)
                        daily_pnl[day] = daily_pnl.get(day, 0.0) + trade.pnl
                        equity = equity_after
                        peak_equity = max(peak_equity, equity)
                        if peak_equity > 0:
                            max_drawdown_pct = max(max_drawdown_pct, (peak_equity - equity) / peak_equity * 100.0)
                        position = None

        if position is not None or pending is not None:
            continue

        signal = signals_by_index.get(index)
        if not signal:
            continue
        if cfg.max_trades_per_day > 0 and daily_trades.get(day, 0) >= cfg.max_trades_per_day:
            continue
        if cfg.daily_loss_limit_pct > 0:
            loss_limit = daily_start_equity.get(day, equity) * cfg.daily_loss_limit_pct / 100.0
            if daily_pnl.get(day, 0.0) <= -loss_limit:
                continue
        pending = PendingOrder(signal=signal)

    summary = summarize_backtest(
        initial_equity=initial_equity,
        ending_equity=equity,
        trades=trades,
        signals=signals,
        max_drawdown_pct=max_drawdown_pct,
        skipped_too_small=skipped_too_small,
        expired_orders=expired_orders,
        daily_pnl=daily_pnl,
        cfg=cfg,
    )
    return summary, trades, signals


def summarize_backtest(
    initial_equity: float,
    ending_equity: float,
    trades: Sequence[Trade],
    signals: Sequence[Signal],
    max_drawdown_pct: float,
    skipped_too_small: int,
    expired_orders: int,
    daily_pnl: dict[str, float],
    cfg: StrategyConfig,
) -> dict[str, Any]:
    wins = [trade for trade in trades if trade.pnl > 0]
    losses = [trade for trade in trades if trade.pnl < 0]
    gross_profit = sum(trade.pnl for trade in wins)
    gross_loss = -sum(trade.pnl for trade in losses)
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    trade_count = len(trades)
    green_days = sum(1 for pnl in daily_pnl.values() if pnl > 0)
    red_days = sum(1 for pnl in daily_pnl.values() if pnl < 0)
    return {
        "initial_equity": round(initial_equity, 2),
        "ending_equity": round(ending_equity, 2),
        "net_pnl": round(ending_equity - initial_equity, 2),
        "return_pct": round((ending_equity / initial_equity - 1.0) * 100.0, 2) if initial_equity else None,
        "signals": len(signals),
        "trades": trade_count,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / trade_count * 100.0, 2) if trade_count else None,
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "avg_r": round(sum(trade.r_multiple for trade in trades) / trade_count, 4) if trade_count else None,
        "avg_win": round(gross_profit / len(wins), 2) if wins else None,
        "avg_loss": round(-gross_loss / len(losses), 2) if losses else None,
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "green_days": green_days,
        "red_days": red_days,
        "expired_orders": expired_orders,
        "skipped_too_small": skipped_too_small,
        "risk_pct": cfg.risk_pct,
        "point_value": cfg.point_value,
        "round_turn_cost_per_contract": round(round_turn_cost_per_contract(cfg), 2),
    }


def latest_signal_status(frame: pd.DataFrame, cfg: StrategyConfig) -> dict[str, Any]:
    prepared = prepare_frame(frame, cfg)
    signals = generate_signals(prepared, cfg)
    if not signals:
        return {
            "status": "NO_SIGNAL",
            "bars": len(prepared),
            "last_bar_utc": prepared.index[-1].isoformat(),
        }
    signal = signals[-1]
    last_index = len(prepared) - 1
    status = "ACTIVE_ENTRY_WINDOW" if signal.index < last_index <= signal.expires_index else "EXPIRED"
    if last_index == signal.index:
        status = "NEW_SIGNAL_WAITING_FOR_RETRACE"
    return {
        "status": status,
        "last_bar_utc": prepared.index[-1].isoformat(),
        "signal": asdict(signal),
    }


def write_report(
    output_dir: Path,
    summary: dict[str, Any],
    trades: Sequence[Trade],
    signals: Sequence[Signal],
    cfg: StrategyConfig,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output_dir / "summary.json", summary)
    atomic_write_json(output_dir / "config.json", asdict(cfg))
    write_csv(output_dir / "trades.csv", [asdict(trade) for trade in trades])
    write_csv(output_dir / "signals.csv", [asdict(signal) for signal in signals])


def build_config_from_args(args: argparse.Namespace) -> StrategyConfig:
    cfg = StrategyConfig()
    updates = {
        "htf_timeframes": parse_timeframes(args.htf_timeframes),
        "require_htf_bias": not args.no_htf_bias,
        "risk_pct": args.risk_pct,
        "point_value": args.point_value,
        "commission_round_turn": args.commission_round_turn,
        "slippage_points_round_turn": args.slippage_points_round_turn,
        "max_contracts": args.max_contracts,
        "max_trades_per_day": args.max_trades_per_day,
        "daily_loss_limit_pct": args.daily_loss_limit_pct,
        "session_start_hour_utc": args.session_start_hour_utc,
        "session_end_hour_utc": args.session_end_hour_utc,
        "min_rr": args.min_rr,
        "fallback_rr": args.fallback_rr,
        "min_stop_points": args.min_stop_points,
        "max_stop_points": args.max_stop_points,
        "fvg_min_points": args.fvg_min_points,
        "displacement_atr_mult": args.displacement_atr_mult,
        "sweep_lookback_bars": args.sweep_lookback_bars,
        "sweep_recent_bars": args.sweep_recent_bars,
        "entry_wait_bars": args.entry_wait_bars,
    }
    return replace(cfg, **updates)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--csv", required=True, type=Path, help="M1 OHLC CSV with time/open/high/low/close columns.")
    parser.add_argument("--since", default="", help="Optional inclusive UTC start timestamp.")
    parser.add_argument("--until", default="", help="Optional inclusive UTC end timestamp.")
    parser.add_argument("--htf-timeframes", default="15min,1h", help="Comma-separated HTF bias timeframes.")
    parser.add_argument("--no-htf-bias", action="store_true", help="Disable higher-timeframe bias filtering.")
    parser.add_argument("--risk-pct", type=float, default=0.50, help="Fixed fractional risk per trade.")
    parser.add_argument("--point-value", type=float, default=10.0, help="Dollars per full 1.00 gold move per contract.")
    parser.add_argument("--commission-round-turn", type=float, default=3.00, help="Round-turn commission per contract.")
    parser.add_argument("--slippage-points-round-turn", type=float, default=0.20, help="Round-turn slippage in gold points.")
    parser.add_argument("--max-contracts", type=int, default=10)
    parser.add_argument("--max-trades-per-day", type=int, default=8)
    parser.add_argument("--daily-loss-limit-pct", type=float, default=2.0)
    parser.add_argument("--session-start-hour-utc", type=int, default=-1)
    parser.add_argument("--session-end-hour-utc", type=int, default=-1)
    parser.add_argument("--min-rr", type=float, default=1.50)
    parser.add_argument("--fallback-rr", type=float, default=2.00)
    parser.add_argument("--min-stop-points", type=float, default=1.00)
    parser.add_argument("--max-stop-points", type=float, default=12.00)
    parser.add_argument("--fvg-min-points", type=float, default=0.20)
    parser.add_argument("--displacement-atr-mult", type=float, default=1.20)
    parser.add_argument("--sweep-lookback-bars", type=int, default=20)
    parser.add_argument("--sweep-recent-bars", type=int, default=3)
    parser.add_argument("--entry-wait-bars", type=int, default=8)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest or emit signals for an M1 gold liquidity/FVG scalper.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backtest_parser = subparsers.add_parser("backtest", help="Run a deterministic CSV backtest.")
    add_common_args(backtest_parser)
    backtest_parser.add_argument("--initial-equity", type=float, default=10_000.0)
    backtest_parser.add_argument("--output-dir", type=Path, default=None)

    signal_parser = subparsers.add_parser("signal", help="Emit latest signal JSON from a CSV.")
    add_common_args(signal_parser)
    signal_parser.add_argument("--output-dir", type=Path, default=None)

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = build_config_from_args(args)
    frame = load_ohlc_csv(args.csv, since=args.since, until=args.until)

    if args.command == "backtest":
        summary, trades, signals = backtest(frame, cfg, args.initial_equity)
        output_dir = args.output_dir or REPORT_ROOT / f"backtest_{utc_now_slug()}"
        write_report(output_dir, summary, trades, signals, cfg)
        print(json.dumps({"summary": summary, "output_dir": str(output_dir)}, indent=2, sort_keys=True))
        return 0

    if args.command == "signal":
        payload = latest_signal_status(frame, cfg)
        output_dir = args.output_dir
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(output_dir / "latest_signal.json", payload)
            payload = {**payload, "output_dir": str(output_dir)}
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
