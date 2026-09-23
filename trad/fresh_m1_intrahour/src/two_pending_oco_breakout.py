from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import (
    add_time_features,
    max_drawdown,
    pair_meta,
    parse_date,
    pip_value_usd_per_unit,
    quote_to_usd_from_prices,
    read_candles,
    select_pairs,
    write_json,
)
from .dataset import add_pair_features
from .economics import execution_settings_from_config, prepare_executable_prices


BRANCH_NAME = "two_pending_oco_breakout"
POSITION_UNITS = 1000
RANGE_WINDOWS = [3, 5, 8, 13, 21]
TIMEOUTS = [1, 2, 3, 5, 8]
BUFFER_SPECS = [
    ("spread_0p5x", "spread", 0.5),
    ("spread_1p0x", "spread", 1.0),
    ("spread_1p5x", "spread", 1.5),
    ("atr15_0p05x", "atr15", 0.05),
    ("atr15_0p10x", "atr15", 0.10),
    ("hybrid_max_spread_1p0_atr15_0p05", "hybrid", 0.0),
]
EXIT_SPECS = [
    ("fixed_tp_sl", "fixed"),
    ("atr_scaled_tp_sl", "atr_scaled"),
    ("spread_scaled_tp_sl", "spread_scaled"),
    ("fast_profit_lock", "fast_profit_lock"),
    ("trailing_after_mfe_threshold", "trailing"),
    ("time_stop", "time_stop"),
    ("failed_follow_through_exit", "failed_follow_through"),
]
CANCEL_LATENCY_BARS = [0, 1, 2]
SLIPPAGE_SENSITIVITY_PIPS = [0.0, 0.1, 0.3]


@dataclass(frozen=True)
class ExitSpec:
    name: str
    tp_pips: float
    sl_pips: float
    max_hold: int
    fast_lock_pips: float | None = None
    trail_start_pips: float | None = None
    trail_giveback_pips: float | None = None
    failed_follow_bars: int | None = None
    failed_follow_min_mfe_pips: float | None = None


def _session_name(ts: pd.Series) -> pd.Series:
    hour = pd.to_datetime(ts, utc=True).dt.hour
    return pd.Series(
        np.select(
            [
                hour.isin([21, 22]),
                (hour >= 12) & (hour < 16),
                (hour >= 7) & (hour < 16),
                (hour >= 12) & (hour < 21),
                (hour >= 0) & (hour < 7),
            ],
            ["rollover", "london_ny_overlap", "london", "new_york", "asia"],
            default="other",
        ),
        index=ts.index,
    )


def _vol_bucket(values: pd.Series) -> pd.Series:
    ranked = values.replace([np.inf, -np.inf], np.nan).rank(method="first")
    return pd.qcut(ranked, 3, labels=["low", "mid", "high"], duplicates="drop").astype(str)


def _spread_bucket(values: pd.Series) -> pd.Series:
    ranked = values.replace([np.inf, -np.inf], np.nan).rank(method="first")
    return pd.qcut(ranked, 3, labels=["low", "mid", "high"], duplicates="drop").astype(str)


def _buffer_pips(name: str, kind: str, mult: float, spread: float, atr15: float) -> float:
    if kind == "spread":
        return max(0.0, spread * mult)
    if kind == "atr15":
        return max(0.0, atr15 * mult)
    if kind == "hybrid":
        return max(spread, 0.05 * atr15)
    raise ValueError(f"unsupported buffer kind: {name}")


def _exit_spec(name: str, atr15: float, spread: float, timeout: int) -> ExitSpec:
    max_hold = max(3, min(30, timeout * 3))
    if name == "fixed_tp_sl":
        return ExitSpec(name, tp_pips=3.0, sl_pips=5.0, max_hold=max_hold)
    if name == "atr_scaled_tp_sl":
        return ExitSpec(name, tp_pips=max(1.2, 0.35 * atr15), sl_pips=max(2.0, 0.55 * atr15), max_hold=max_hold)
    if name == "spread_scaled_tp_sl":
        return ExitSpec(name, tp_pips=max(1.0, 3.0 * spread), sl_pips=max(1.8, 5.0 * spread), max_hold=max_hold)
    if name == "fast_profit_lock":
        return ExitSpec(name, tp_pips=max(1.2, 2.5 * spread), sl_pips=max(2.0, 4.0 * spread), max_hold=max_hold, fast_lock_pips=max(0.4, spread))
    if name == "trailing_after_mfe_threshold":
        return ExitSpec(name, tp_pips=max(3.0, 0.45 * atr15), sl_pips=max(2.5, 0.55 * atr15), max_hold=max_hold, trail_start_pips=max(1.5, 0.20 * atr15), trail_giveback_pips=max(0.7, 1.5 * spread))
    if name == "time_stop":
        return ExitSpec(name, tp_pips=1e9, sl_pips=1e9, max_hold=max_hold)
    if name == "failed_follow_through_exit":
        return ExitSpec(name, tp_pips=max(2.0, 0.30 * atr15), sl_pips=max(2.5, 0.45 * atr15), max_hold=max_hold, failed_follow_bars=2, failed_follow_min_mfe_pips=max(0.3, spread * 0.5))
    raise ValueError(f"unsupported exit spec: {name}")


def _exit_path(
    df: pd.DataFrame,
    entry_i: int,
    side: str,
    entry_price: float,
    pip: float,
    spec: ExitSpec,
    round_trip_slippage_pips: float,
    extra_slippage_pips: float = 0.0,
) -> dict[str, Any]:
    n = len(df)
    end_i = min(n - 1, entry_i + max(1, spec.max_hold) - 1)
    if entry_i < 0 or entry_i >= n:
        return {"complete": False, "pnl_pips": np.nan}
    mfe = 0.0
    mae = 0.0
    best_fav = 0.0
    lock_floor: float | None = None
    for j in range(entry_i, end_i + 1):
        if side == "long":
            fav = (float(df["exec_bid_high"].iloc[j]) - entry_price) / pip
            adv = (entry_price - float(df["exec_bid_low"].iloc[j])) / pip
            endpoint = (float(df["exec_bid_close"].iloc[j]) - entry_price) / pip
        else:
            fav = (entry_price - float(df["exec_ask_low"].iloc[j])) / pip
            adv = (float(df["exec_ask_high"].iloc[j]) - entry_price) / pip
            endpoint = (entry_price - float(df["exec_ask_close"].iloc[j])) / pip
        mfe = max(mfe, fav)
        mae = max(mae, adv)
        best_fav = max(best_fav, fav)

        if spec.failed_follow_bars is not None and j - entry_i + 1 >= spec.failed_follow_bars and best_fav < float(spec.failed_follow_min_mfe_pips or 0.0):
            return _exit_payload(j, entry_i, endpoint, mfe, mae, "failed_follow_through_exit", round_trip_slippage_pips, extra_slippage_pips)

        if spec.fast_lock_pips is not None and best_fav >= spec.fast_lock_pips and endpoint <= max(0.0, spec.fast_lock_pips * 0.25):
            return _exit_payload(j, entry_i, max(0.0, spec.fast_lock_pips * 0.25), mfe, mae, "fast_profit_lock", round_trip_slippage_pips, extra_slippage_pips)

        if spec.trail_start_pips is not None and best_fav >= spec.trail_start_pips:
            lock_floor = max(lock_floor or -1e9, best_fav - float(spec.trail_giveback_pips or 0.0))
            if endpoint <= lock_floor:
                return _exit_payload(j, entry_i, lock_floor, mfe, mae, "trailing_stop", round_trip_slippage_pips, extra_slippage_pips)

        hit_tp = fav >= spec.tp_pips
        hit_sl = adv >= spec.sl_pips
        if hit_tp and hit_sl:
            return _exit_payload(j, entry_i, -spec.sl_pips, mfe, mae, "ambiguous_adverse_first", round_trip_slippage_pips, extra_slippage_pips, ambiguous=True)
        if hit_sl:
            return _exit_payload(j, entry_i, -spec.sl_pips, mfe, mae, "sl_before_tp", round_trip_slippage_pips, extra_slippage_pips)
        if hit_tp:
            return _exit_payload(j, entry_i, spec.tp_pips, mfe, mae, "tp_before_sl", round_trip_slippage_pips, extra_slippage_pips)

    if side == "long":
        endpoint = (float(df["exec_bid_close"].iloc[end_i]) - entry_price) / pip
    else:
        endpoint = (entry_price - float(df["exec_ask_close"].iloc[end_i])) / pip
    return _exit_payload(end_i, entry_i, endpoint, mfe, mae, "time_stop", round_trip_slippage_pips, extra_slippage_pips)


def _exit_payload(
    exit_i: int,
    entry_i: int,
    gross_pips: float,
    mfe: float,
    mae: float,
    outcome: str,
    round_trip_slippage_pips: float,
    extra_slippage_pips: float,
    ambiguous: bool = False,
) -> dict[str, Any]:
    return {
        "complete": True,
        "exit_index": int(exit_i),
        "hold_minutes": int(max(1, exit_i - entry_i + 1)),
        "pnl_pips": float(gross_pips - round_trip_slippage_pips - extra_slippage_pips),
        "gross_pips": float(gross_pips),
        "mfe_pips": float(max(0.0, mfe)),
        "mae_pips": float(max(0.0, mae)),
        "outcome": outcome,
        "same_bar_ambiguous": bool(ambiguous),
    }


def _sibling_cancel_loss(
    df: pd.DataFrame,
    trigger_i: int,
    side: str,
    sibling_entry_price: float,
    cancel_latency_bars: int,
    pip: float,
    round_trip_slippage_pips: float,
) -> tuple[bool, float]:
    if cancel_latency_bars <= 0:
        return False, 0.0
    end_i = min(len(df) - 1, trigger_i + cancel_latency_bars)
    if side == "long":
        hit = float(df["exec_ask_high"].iloc[trigger_i:end_i + 1].max()) >= sibling_entry_price
        closeout = (float(df["exec_bid_close"].iloc[end_i]) - sibling_entry_price) / pip
    else:
        hit = float(df["exec_bid_low"].iloc[trigger_i:end_i + 1].min()) <= sibling_entry_price
        closeout = (sibling_entry_price - float(df["exec_ask_close"].iloc[end_i])) / pip
    if not hit:
        return False, 0.0
    return True, float(min(closeout, 0.0) - round_trip_slippage_pips)


def _simulate_one(
    df: pd.DataFrame,
    i: int,
    pair: str,
    pip: float,
    pip_value: float,
    range_window: int,
    buffer_name: str,
    buffer_kind: str,
    buffer_mult: float,
    timeout: int,
    exit_name: str,
    cancel_latency_bars: int,
    round_trip_slippage_pips: float,
    extra_slippage_pips: float,
) -> dict[str, Any] | None:
    if i < range_window or i + timeout + 30 >= len(df):
        return None
    spread = float(df["spread_pips_used"].iloc[i])
    atr15 = float(df["atr_15m_pips"].iloc[i]) if "atr_15m_pips" in df.columns else np.nan
    if not np.isfinite(atr15) or atr15 <= 0 or not np.isfinite(spread):
        return None
    recent_high = float(df["exec_ask_high"].iloc[i - range_window + 1:i + 1].max())
    recent_low = float(df["exec_bid_low"].iloc[i - range_window + 1:i + 1].min())
    buffer_pips = _buffer_pips(buffer_name, buffer_kind, buffer_mult, spread, atr15)
    buy_stop = recent_high + buffer_pips * pip
    sell_stop = recent_low - buffer_pips * pip
    if not np.isfinite(buy_stop) or not np.isfinite(sell_stop) or buy_stop <= sell_stop:
        return None

    trigger_i = -1
    side: str | None = None
    double_trigger = False
    no_trigger = True
    for j in range(i + 1, min(len(df), i + timeout + 1)):
        buy_hit = float(df["exec_ask_high"].iloc[j]) >= buy_stop
        sell_hit = float(df["exec_bid_low"].iloc[j]) <= sell_stop
        if buy_hit or sell_hit:
            trigger_i = j
            no_trigger = False
            if buy_hit and sell_hit:
                double_trigger = True
                buy_exit = _exit_path(df, j, "long", max(buy_stop, float(df["exec_ask_open"].iloc[j])), pip, _exit_spec(exit_name, atr15, spread, timeout), round_trip_slippage_pips, extra_slippage_pips)
                sell_exit = _exit_path(df, j, "short", min(sell_stop, float(df["exec_bid_open"].iloc[j])), pip, _exit_spec(exit_name, atr15, spread, timeout), round_trip_slippage_pips, extra_slippage_pips)
                side = "long" if float(buy_exit["pnl_pips"]) <= float(sell_exit["pnl_pips"]) else "short"
            else:
                side = "long" if buy_hit else "short"
            break

    base = {
        "decision_time_utc": df["decision_time_utc"].iloc[i],
        "pair": pair,
        "branch_name": BRANCH_NAME,
        "range_window_min": range_window,
        "buffer_name": buffer_name,
        "buffer_pips": buffer_pips,
        "timeout_min": timeout,
        "exit_name": exit_name,
        "cancel_latency_bars": cancel_latency_bars,
        "extra_slippage_pips": extra_slippage_pips,
        "spread_pips": spread,
        "atr_15m_pips": atr15,
        "spread_to_atr": spread / atr15 if atr15 else np.nan,
        "session": str(df["session"].iloc[i]),
        "volatility_bucket": str(df["volatility_bucket"].iloc[i]),
        "spread_bucket": str(df["spread_bucket"].iloc[i]),
        "movement_score": float(df["movement_score"].iloc[i]),
        "direction_score": float(df["direction_score"].iloc[i]),
        "breakout_quality": float(df["breakout_quality"].iloc[i]),
        "choppiness_15m": float(df["choppiness_15m"].iloc[i]),
        "eligible_router_flag": bool(df["router_oco_eligible"].iloc[i]),
        "buy_stop": buy_stop,
        "sell_stop": sell_stop,
        "triggered": not no_trigger,
        "no_trigger": no_trigger,
        "buy_trigger_first": False,
        "sell_trigger_first": False,
        "single_trigger": False,
        "double_trigger": double_trigger,
        "sibling_cancel_failure": False,
        "cancel_latency_loss_pips": 0.0,
    }
    if no_trigger or side is None:
        return {**base, "side": None, "pnl_pips": 0.0, "account_pnl": 0.0, "outcome": "no_trigger_cancel", "tp_before_sl": False, "false_breakout": False, "whipsaw": False, "mfe_pips": 0.0, "mae_pips": 0.0, "hold_minutes": 0}

    entry_price = max(buy_stop, float(df["exec_ask_open"].iloc[trigger_i])) if side == "long" else min(sell_stop, float(df["exec_bid_open"].iloc[trigger_i]))
    exit_result = _exit_path(df, trigger_i, side, entry_price, pip, _exit_spec(exit_name, atr15, spread, timeout), round_trip_slippage_pips, extra_slippage_pips)
    sibling_side = "short" if side == "long" else "long"
    sibling_entry = sell_stop if side == "long" else buy_stop
    sibling_hit, cancel_loss = _sibling_cancel_loss(df, trigger_i, sibling_side, sibling_entry, cancel_latency_bars, pip, round_trip_slippage_pips)
    pnl_pips = float(exit_result["pnl_pips"]) + cancel_loss
    if sibling_hit:
        double_trigger = True
    return {
        **base,
        "side": side,
        "trigger_bars_after_decision": int(trigger_i - i),
        "buy_trigger_first": side == "long",
        "sell_trigger_first": side == "short",
        "single_trigger": not double_trigger,
        "double_trigger": double_trigger,
        "sibling_cancel_failure": sibling_hit,
        "cancel_latency_loss_pips": cancel_loss,
        "pnl_pips": pnl_pips,
        "account_pnl": pnl_pips * pip_value * POSITION_UNITS,
        "outcome": exit_result["outcome"],
        "tp_before_sl": exit_result["outcome"] == "tp_before_sl",
        "false_breakout": pnl_pips < 0,
        "whipsaw": bool(double_trigger or exit_result["outcome"] in {"sl_before_tp", "ambiguous_adverse_first"}),
        "mfe_pips": float(exit_result["mfe_pips"]),
        "mae_pips": float(exit_result["mae_pips"]),
        "hold_minutes": int(exit_result["hold_minutes"]),
        "same_bar_ambiguous": bool(exit_result.get("same_bar_ambiguous", False)),
    }


def _prepare_pair(cfg: dict[str, Any], pair: str, start: str, end: str, max_rows_per_pair: int | None) -> tuple[pd.DataFrame, float, float, float]:
    start_ts = parse_date(start)
    end_ts = parse_date(end)
    raw = read_candles(pair, cfg, start_ts, end_ts, max_rows=max_rows_per_pair)
    raw = add_time_features(raw)
    raw = add_pair_features(raw, cfg)
    meta = pair_meta(pair, cfg)
    settings = execution_settings_from_config(cfg, meta.tier)
    df, _ = prepare_executable_prices(raw, pair, cfg, settings.spread_multiplier)
    close_by_pair = {pair: float(df["close"].iloc[-1])}
    quote_to_usd = quote_to_usd_from_prices(pair, float(df["close"].iloc[-1]), close_by_pair)
    pip_value = float(pip_value_usd_per_unit(pair, quote_to_usd))
    df["session"] = _session_name(df["decision_time_utc"])
    df["volatility_bucket"] = _vol_bucket(df["atr_15m_pips"])
    df["spread_bucket"] = _spread_bucket(df["spread_to_atr_15m"])
    df["movement_score"] = (
        df["atr_15m_pips"].fillna(0.0).rank(pct=True)
        + df["volatility_expansion_15_60"].fillna(1.0).rank(pct=True)
        + df["range_15m_pips"].fillna(0.0).rank(pct=True)
    ) / 3.0
    df["direction_score"] = df["momentum_5m_atr"].fillna(0.0).abs().clip(0, 2.0) / 2.0
    df["breakout_quality"] = (
        (1.0 - df["choppiness_15m"].fillna(1.0)).clip(0, 1)
        + df["volatility_expansion_15_60"].fillna(1.0).clip(0, 2) / 2.0
        + (1.0 - df["range_position_30m"].sub(0.5).abs().fillna(0.5) * 2.0).clip(0, 1)
    ) / 3.0
    df["router_oco_eligible"] = (
        (df["movement_score"] >= 0.60)
        & (df["direction_score"].between(0.10, 0.70))
        & (df["breakout_quality"] >= 0.45)
        & (df["spread_to_atr_15m"].fillna(9.0) <= 0.35)
        & (df["choppiness_15m"].fillna(1.0) <= 0.75)
    )
    return df.reset_index(drop=True), meta.pip_size, pip_value, settings.slippage_pips_round_trip


def _summarize_trades(df: pd.DataFrame, label: str) -> dict[str, Any]:
    if df.empty:
        return {"label": label, "rows": 0, "account_pnl": 0.0, "return_pct": 0.0, "trade_count": 0}
    triggered = df[df["triggered"]].copy()
    pnl = triggered["account_pnl"].astype(float) if not triggered.empty else pd.Series(dtype=float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    equity = (1000.0 + pnl.cumsum()).tolist() if len(pnl) else [1000.0]
    return {
        "label": label,
        "rows": int(len(df)),
        "trigger_rate": float(df["triggered"].mean()),
        "no_trigger_cancel_rate": float(df["no_trigger"].mean()),
        "single_trigger_rate": float(df["single_trigger"].mean()),
        "double_trigger_rate": float(df["double_trigger"].mean()),
        "false_breakout_rate": float(triggered["false_breakout"].mean()) if len(triggered) else None,
        "whipsaw_rate": float(triggered["whipsaw"].mean()) if len(triggered) else None,
        "tp_before_sl_rate_after_trigger": float(triggered["tp_before_sl"].mean()) if len(triggered) else None,
        "average_spread_at_trigger": float(triggered["spread_pips"].mean()) if len(triggered) else None,
        "account_pnl": float(pnl.sum()) if len(pnl) else 0.0,
        "return_pct": float(pnl.sum() / 1000.0 * 100.0) if len(pnl) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "max_drawdown_pct": max_drawdown(equity),
        "trade_count": int(len(triggered)),
        "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
        "mean_mfe_pips": float(triggered["mfe_pips"].mean()) if len(triggered) else None,
        "mean_mae_pips": float(triggered["mae_pips"].mean()) if len(triggered) else None,
        "expected_cancel_latency_loss": float(triggered["cancel_latency_loss_pips"].mean()) if len(triggered) else None,
    }


def _breakdown(df: pd.DataFrame, col: str) -> list[dict[str, Any]]:
    if df.empty or col not in df.columns:
        return []
    rows = []
    for key, group in df[df["triggered"]].groupby(col, dropna=False):
        metrics = _summarize_trades(group, str(key))
        metrics[col] = str(key)
        rows.append(metrics)
    return sorted(rows, key=lambda r: r.get("account_pnl", 0.0), reverse=True)


def _simple_directional_baseline(pair_frames: list[pd.DataFrame], pip_values: dict[str, float], pips: dict[str, float]) -> dict[str, Any]:
    rows = []
    for pair, df in pair_frames:
        pip = pips[pair]
        pip_value = pip_values[pair]
        for i in range(30, len(df) - 14):
            if float(df["movement_score"].iloc[i]) < 0.60 or float(df["spread_to_atr_15m"].iloc[i]) > 0.35:
                continue
            side = "long" if float(df["momentum_5m_atr"].iloc[i]) >= 0 else "short"
            entry_i = i + 1
            end_i = min(len(df) - 1, entry_i + 12)
            if side == "long":
                p = ((float(df["exec_bid_close"].iloc[end_i]) - float(df["exec_ask_open"].iloc[entry_i])) / pip) - 0.4
            else:
                p = ((float(df["exec_bid_open"].iloc[entry_i]) - float(df["exec_ask_close"].iloc[end_i])) / pip) - 0.4
            rows.append({"pair": pair, "decision_time_utc": df["decision_time_utc"].iloc[i], "account_pnl": p * pip_value * POSITION_UNITS, "triggered": True, "no_trigger": False, "single_trigger": True, "double_trigger": False, "false_breakout": p < 0, "whipsaw": False, "tp_before_sl": p > 0, "spread_pips": df["spread_pips_used"].iloc[i], "mfe_pips": np.nan, "mae_pips": np.nan, "cancel_latency_loss_pips": 0.0})
    return _summarize_trades(pd.DataFrame(rows), "market_directional_branch")


def run_two_pending_oco_breakout(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str = "tier1",
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_pairs = select_pairs(cfg, tier, pairs)
    records: list[dict[str, Any]] = []
    pair_frames: list[tuple[str, pd.DataFrame]] = []
    pip_values: dict[str, float] = {}
    pips: dict[str, float] = {}
    health: list[dict[str, Any]] = []
    for pair in selected_pairs:
        df, pip, pip_value, slippage = _prepare_pair(cfg, pair, start, end, max_rows_per_pair)
        pair_frames.append((pair, df))
        pip_values[pair] = pip_value
        pips[pair] = pip
        health.append({"pair": pair, "rows": int(len(df)), "start": str(df["decision_time_utc"].min()), "end": str(df["decision_time_utc"].max())})
        indices = np.flatnonzero(df["router_oco_eligible"].to_numpy())
        for i in indices:
            for rw in RANGE_WINDOWS:
                for buffer_name, kind, mult in BUFFER_SPECS:
                    for timeout in TIMEOUTS:
                        for exit_name, _ in EXIT_SPECS:
                            rec = _simulate_one(df, int(i), pair, pip, pip_value, rw, buffer_name, kind, mult, timeout, exit_name, cancel_latency_bars=1, round_trip_slippage_pips=slippage, extra_slippage_pips=0.0)
                            if rec is not None:
                                records.append(rec)
    all_results = pd.DataFrame(records)
    if all_results.empty:
        raise RuntimeError("two_pending_oco_breakout produced no candidate rows")
    all_results.to_parquet(output_dir / "two_pending_oco_breakout_candidates.parquet", index=False)
    all_results.head(5000).to_csv(output_dir / "two_pending_oco_breakout_candidates_sample.csv", index=False)

    combo_cols = ["range_window_min", "buffer_name", "timeout_min", "exit_name"]
    combo = []
    for key, group in all_results.groupby(combo_cols, dropna=False):
        metrics = _summarize_trades(group, "|".join(map(str, key)))
        metrics.update(dict(zip(combo_cols, key)))
        combo.append(metrics)
    combo_df = pd.DataFrame(combo).sort_values(["account_pnl", "profit_factor"], ascending=[False, False])
    combo_df.to_csv(output_dir / "two_pending_oco_breakout_grid_summary.csv", index=False)
    best = combo_df.iloc[0].to_dict() if not combo_df.empty else {}
    best_filter = np.ones(len(all_results), dtype=bool)
    for col in combo_cols:
        best_filter &= all_results[col].astype(str).eq(str(best.get(col))).to_numpy()
    best_results = all_results[best_filter].copy()

    slippage_sensitivity = []
    cancel_sensitivity = []
    if best:
        for extra in SLIPPAGE_SENSITIVITY_PIPS:
            adjusted = best_results.copy()
            adjusted.loc[adjusted["triggered"], "account_pnl"] -= extra * adjusted.loc[adjusted["triggered"], "pair"].map(pip_values).astype(float) * POSITION_UNITS
            slippage_sensitivity.append(_summarize_trades(adjusted, f"extra_slippage_{extra}pips"))
        best_cfg = {col: best[col] for col in combo_cols}
        for latency in CANCEL_LATENCY_BARS:
            rows = []
            for pair, df in pair_frames:
                for i in np.flatnonzero(df["router_oco_eligible"].to_numpy()):
                    rec = _simulate_one(
                        df, int(i), pair, pips[pair], pip_values[pair],
                        int(best_cfg["range_window_min"]),
                        str(best_cfg["buffer_name"]),
                        next(k for n, k, m in BUFFER_SPECS if n == str(best_cfg["buffer_name"])),
                        next(m for n, k, m in BUFFER_SPECS if n == str(best_cfg["buffer_name"])),
                        int(best_cfg["timeout_min"]),
                        str(best_cfg["exit_name"]),
                        cancel_latency_bars=latency,
                        round_trip_slippage_pips=execution_settings_from_config(cfg, pair_meta(pair, cfg).tier).slippage_pips_round_trip,
                        extra_slippage_pips=0.0,
                    )
                    if rec is not None:
                        rows.append(rec)
            cancel_sensitivity.append(_summarize_trades(pd.DataFrame(rows), f"cancel_latency_{latency}_bars"))

    directional = _simple_directional_baseline(pair_frames, pip_values, pips)
    no_trade = {"label": "no_trade", "account_pnl": 0.0, "return_pct": 0.0, "trade_count": 0, "max_drawdown_pct": 0.0}
    best_simple = max([directional, no_trade], key=lambda x: float(x.get("account_pnl", 0.0)))
    best_summary = _summarize_trades(best_results, "best_two_pending_oco_breakout")
    forecast_targets = {
        "P_either_pending_order_triggers": float(best_results["triggered"].mean()),
        "P_buy_stop_triggers_first": float(best_results["buy_trigger_first"].mean()),
        "P_sell_stop_triggers_first": float(best_results["sell_trigger_first"].mean()),
        "P_triggered_side_hits_TP_before_SL": float(best_results.loc[best_results["triggered"], "tp_before_sl"].mean()) if best_results["triggered"].any() else None,
        "P_false_breakout": float(best_results.loc[best_results["triggered"], "false_breakout"].mean()) if best_results["triggered"].any() else None,
        "P_double_trigger_whipsaw": float(best_results["double_trigger"].mean()),
        "expected_post_trigger_MFE": float(best_results.loc[best_results["triggered"], "mfe_pips"].mean()) if best_results["triggered"].any() else None,
        "expected_post_trigger_MAE": float(best_results.loc[best_results["triggered"], "mae_pips"].mean()) if best_results["triggered"].any() else None,
        "expected_account_currency_EV": float(best_results.loc[best_results["triggered"], "account_pnl"].mean()) if best_results["triggered"].any() else 0.0,
        "expected_cancel_latency_loss": float(best_results.loc[best_results["triggered"], "cancel_latency_loss_pips"].mean()) if best_results["triggered"].any() else None,
        "no_trigger_probability": float(best_results["no_trigger"].mean()),
    }
    oco_more_promising = float(best_summary.get("account_pnl", 0.0)) > float(directional.get("account_pnl", 0.0))
    report = {
        "branch_name": BRANCH_NAME,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "live_execution_enabled": False,
        "order_placement_enabled": False,
        "research_only": True,
        "assumptions": {
            "next_bar_executable": True,
            "bid_ask_prices": True,
            "same_bar_ambiguity": "adverse_first",
            "cancel_latency_simulated": True,
            "double_trigger_detection": True,
            "sibling_order_cancel_failure_delay_scenario": True,
            "no_optimistic_same_bar_sequencing": True,
        },
        "grid": {
            "range_windows": RANGE_WINDOWS,
            "buffers": [x[0] for x in BUFFER_SPECS],
            "timeouts": TIMEOUTS,
            "exits": [x[0] for x in EXIT_SPECS],
        },
        "data_health": health,
        "candidate_rows": int(len(all_results)),
        "best": best,
        "best_metrics": best_summary,
        "forecast_targets": forecast_targets,
        "slippage_sensitivity": slippage_sensitivity,
        "cancel_latency_sensitivity": cancel_sensitivity,
        "performance_by_pair": _breakdown(best_results, "pair"),
        "performance_by_session": _breakdown(best_results, "session"),
        "performance_by_volatility_bucket": _breakdown(best_results, "volatility_bucket"),
        "performance_by_spread_bucket": _breakdown(best_results, "spread_bucket"),
        "comparison": {
            "market_directional_branch": directional,
            "no_trade": no_trade,
            "best_simple_baseline": best_simple,
            "two_pending_oco_breakout": best_summary,
        },
        "router_logic": {
            "movement_score_low": "no_trade",
            "movement_score_high_direction_score_high": "market_directional_trade",
            "movement_score_high_direction_weak_moderate_breakout_quality_high": BRANCH_NAME,
            "movement_score_high_choppy_two_way": "simulation_only_two_sided_volatility_capture",
            "else": "no_trade",
        },
        "conclusion": {
            "two_pending_oco_breakout_more_promising_than_direct_directional_market_entry": bool(oco_more_promising),
            "statement": (
                f"{BRANCH_NAME} {'shows' if oco_more_promising else 'does not show'} more promise than direct directional market entry on this run."
            ),
        },
    }
    write_json(output_dir / "TWO_PENDING_OCO_BREAKOUT_REPORT.json", report)
    _write_markdown(output_dir / "TWO_PENDING_OCO_BREAKOUT_REPORT.md", report)
    write_json(output_dir / "run_manifest.json", {
        "run_dir": str(output_dir),
        "branch_name": BRANCH_NAME,
        "report_json": str(output_dir / "TWO_PENDING_OCO_BREAKOUT_REPORT.json"),
        "report_md": str(output_dir / "TWO_PENDING_OCO_BREAKOUT_REPORT.md"),
        "live_execution_enabled": False,
        "order_placement_enabled": False,
    })
    return output_dir / "TWO_PENDING_OCO_BREAKOUT_REPORT.json"


def _write_markdown(path: Path, report: dict[str, Any]) -> None:
    best = report.get("best", {})
    metrics = report.get("best_metrics", {})
    comparison = report.get("comparison", {})
    lines = [
        "# Two Pending OCO Breakout Report",
        "",
        f"- Branch: `{report['branch_name']}`",
        f"- Generated UTC: `{report['generated_utc']}`",
        "- Live/order placement enabled: `false`",
        "- Research only: `true`",
        "",
        "## Best Configuration",
        f"- Range window: `{best.get('range_window_min')}`",
        f"- Buffer: `{best.get('buffer_name')}`",
        f"- Timeout: `{best.get('timeout_min')}`",
        f"- Exit: `{best.get('exit_name')}`",
        "",
        "## Best Metrics",
        f"- Trigger rate: `{metrics.get('trigger_rate')}`",
        f"- No-trigger cancel rate: `{metrics.get('no_trigger_cancel_rate')}`",
        f"- Single-trigger rate: `{metrics.get('single_trigger_rate')}`",
        f"- Double-trigger rate: `{metrics.get('double_trigger_rate')}`",
        f"- False-breakout rate: `{metrics.get('false_breakout_rate')}`",
        f"- Whipsaw rate: `{metrics.get('whipsaw_rate')}`",
        f"- TP-before-SL rate after trigger: `{metrics.get('tp_before_sl_rate_after_trigger')}`",
        f"- Average spread at trigger: `{metrics.get('average_spread_at_trigger')}`",
        f"- Account P/L: `{metrics.get('account_pnl')}`",
        f"- Profit factor: `{metrics.get('profit_factor')}`",
        f"- Max drawdown: `{metrics.get('max_drawdown_pct')}`",
        f"- Trades: `{metrics.get('trade_count')}`",
        "",
        "## Comparisons",
        f"- Market directional branch: `{comparison.get('market_directional_branch')}`",
        f"- No-trade: `{comparison.get('no_trade')}`",
        f"- Best simple baseline: `{comparison.get('best_simple_baseline')}`",
        "",
        "## Forecast Targets",
        "```json",
        pd.Series(report.get("forecast_targets", {})).to_json(indent=2),
        "```",
        "",
        "## Slippage Sensitivity",
        "```json",
        pd.Series(report.get("slippage_sensitivity", [])).to_json(indent=2),
        "```",
        "",
        "## Cancel-Latency Sensitivity",
        "```json",
        pd.Series(report.get("cancel_latency_sensitivity", [])).to_json(indent=2),
        "```",
        "",
        "## Conclusion",
        report.get("conclusion", {}).get("statement", ""),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
