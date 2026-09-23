from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .common import fallback_spread_pips, pair_meta, pip_size_for, slippage_pips


AMBIGUITY_MODES = {"adverse_first", "favorable_first", "exclude_ambiguous", "separate_class"}


@dataclass(frozen=True)
class ExecutionSettings:
    entry_delay_bars: int
    entry_price: str
    same_bar_ambiguity: str
    spread_multiplier: float
    slippage_pips_round_trip: float
    cost_mode: str = "base"


@dataclass(frozen=True)
class ExecutionResult:
    complete: bool
    outcome: str
    time_to_exit_min: int | None
    hold_bars: int | None
    realized_pips: float
    endpoint_pips: float
    mfe_pips: float
    mae_pips: float
    same_bar_ambiguous: int
    entry_index: int | None
    exit_index: int | None
    entry_price: float
    exit_price: float


@dataclass(frozen=True)
class ExecutablePriceArrays:
    bid_open: np.ndarray
    bid_high: np.ndarray
    bid_low: np.ndarray
    bid_close: np.ndarray
    ask_open: np.ndarray
    ask_high: np.ndarray
    ask_low: np.ndarray
    ask_close: np.ndarray


def execution_settings_from_config(cfg: dict[str, Any], tier: str, cost_mode: str = "base") -> ExecutionSettings:
    execution = cfg.get("execution", {})
    costs = cfg.get("costs", {})
    spread_multiplier = float(execution.get("spread_multiplier", 1.0))
    round_trip_slippage = execution.get("slippage_pips_round_trip")
    if round_trip_slippage is None:
        round_trip_slippage = 2.0 * float(costs.get(f"slippage_pips_{tier}", costs.get("slippage_pips_tier3", 1.0)))

    if cost_mode == "spread_1p25x":
        spread_multiplier *= 1.25
    elif cost_mode == "spread_1p5x":
        spread_multiplier *= 1.5
    elif cost_mode == "spread_2x":
        spread_multiplier *= 2.0
    elif cost_mode == "slippage_plus_0p1":
        round_trip_slippage = float(round_trip_slippage) + 0.1
    elif cost_mode == "slippage_plus_0p3":
        round_trip_slippage = float(round_trip_slippage) + 0.3
    elif cost_mode == "one_bar_delay":
        execution = {**execution, "entry_delay_bars": max(int(execution.get("entry_delay_bars", 1)), 1) + 1}
    elif cost_mode == "same_bar_adverse_first":
        execution = {**execution, "entry_delay_bars": 0, "same_bar_ambiguity": "adverse_first", "entry_price": "close"}
    elif cost_mode == "exclude_same_bar_ambiguous":
        execution = {**execution, "same_bar_ambiguity": "exclude_ambiguous"}

    ambiguity = str(execution.get("same_bar_ambiguity", "adverse_first"))
    if ambiguity not in AMBIGUITY_MODES:
        raise ValueError(f"unsupported same_bar_ambiguity: {ambiguity}")
    entry_price = str(execution.get("entry_price", "open"))
    if entry_price not in {"open", "close"}:
        raise ValueError(f"unsupported entry_price: {entry_price}")

    return ExecutionSettings(
        entry_delay_bars=int(execution.get("entry_delay_bars", 1)),
        entry_price=entry_price,
        same_bar_ambiguity=ambiguity,
        spread_multiplier=float(spread_multiplier),
        slippage_pips_round_trip=float(round_trip_slippage),
        cost_mode=cost_mode,
    )


def _numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def prepare_executable_prices(
    df: pd.DataFrame,
    instrument: str,
    cfg: dict[str, Any],
    spread_multiplier: float = 1.0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Add executable bid/ask OHLC columns.

    Native bid/ask columns are used when present and the spread multiplier is
    base. Missing rows or stress modes are conservatively approximated from mid
    OHLC plus/minus half spread.
    """
    out = df.copy()
    meta = pair_meta(instrument, cfg)
    pip = pip_size_for(instrument)
    fallback = fallback_spread_pips(meta, cfg)

    native_spread = _numeric(out, "spread_pips")
    bid_close = _numeric(out, "bid_close")
    ask_close = _numeric(out, "ask_close")
    derived_spread = (ask_close - bid_close) / pip
    spread_raw = native_spread.where(native_spread.notna(), derived_spread)
    spread_raw = spread_raw.replace([np.inf, -np.inf], np.nan)
    spread_used = spread_raw.fillna(spread_raw.rolling(240, min_periods=10).median()).fillna(fallback)
    spread_used = spread_used.clip(lower=0.0) * float(spread_multiplier)
    out["spread_pips_used"] = spread_used
    out["round_trip_cost_pips_used"] = spread_used + float(2.0 * slippage_pips(meta, cfg))

    native_any = pd.Series(False, index=out.index)
    native_all_ohlc = pd.Series(True, index=out.index)
    for suffix in ["open", "high", "low", "close"]:
        bid_col = f"bid_{suffix}"
        ask_col = f"ask_{suffix}"
        native_bid = _numeric(out, bid_col)
        native_ask = _numeric(out, ask_col)
        mid = _numeric(out, suffix)
        native_pair = native_bid.notna() & native_ask.notna()
        native_any |= native_pair
        native_all_ohlc &= native_pair

        native_mid = (native_bid + native_ask) / 2.0
        mid = mid.where(mid.notna(), native_mid)
        half_spread = spread_used * pip / 2.0
        use_native = native_pair & (abs(float(spread_multiplier) - 1.0) < 1e-12)

        out[f"exec_bid_{suffix}"] = native_bid.where(use_native, mid - half_spread)
        out[f"exec_ask_{suffix}"] = native_ask.where(use_native, mid + half_spread)

    meta_payload = {
        "instrument": instrument,
        "pip_size": pip,
        "fallback_spread_pips": fallback,
        "spread_multiplier": float(spread_multiplier),
        "native_spread_present_rate": float(native_spread.notna().mean()) if len(out) else 0.0,
        "native_bidask_any_rate": float(native_any.mean()) if len(out) else 0.0,
        "native_bidask_complete_ohlc_rate": float(native_all_ohlc.mean()) if len(out) else 0.0,
        "bidask_source": "native_when_complete_else_mid_spread",
        "median_spread_pips_used": float(spread_used.median()) if len(out) else None,
        "p95_spread_pips_used": float(spread_used.quantile(0.95)) if len(out) else None,
    }
    return out, meta_payload


def executable_price_arrays(exec_df: pd.DataFrame) -> ExecutablePriceArrays:
    return ExecutablePriceArrays(
        bid_open=exec_df["exec_bid_open"].to_numpy(dtype=float),
        bid_high=exec_df["exec_bid_high"].to_numpy(dtype=float),
        bid_low=exec_df["exec_bid_low"].to_numpy(dtype=float),
        bid_close=exec_df["exec_bid_close"].to_numpy(dtype=float),
        ask_open=exec_df["exec_ask_open"].to_numpy(dtype=float),
        ask_high=exec_df["exec_ask_high"].to_numpy(dtype=float),
        ask_low=exec_df["exec_ask_low"].to_numpy(dtype=float),
        ask_close=exec_df["exec_ask_close"].to_numpy(dtype=float),
    )


def _blank_incomplete() -> ExecutionResult:
    return ExecutionResult(
        complete=False,
        outcome="incomplete",
        time_to_exit_min=None,
        hold_bars=None,
        realized_pips=np.nan,
        endpoint_pips=np.nan,
        mfe_pips=np.nan,
        mae_pips=np.nan,
        same_bar_ambiguous=0,
        entry_index=None,
        exit_index=None,
        entry_price=np.nan,
        exit_price=np.nan,
    )


def endpoint_after_cost_arrays(
    arrays: ExecutablePriceArrays,
    decision_index: int,
    side: str,
    horizon_min: int,
    pip_size: float,
    settings: ExecutionSettings,
) -> ExecutionResult:
    entry_i = int(decision_index) + int(settings.entry_delay_bars)
    end_i = entry_i + max(int(horizon_min), 1) - 1
    if entry_i < 0 or end_i >= len(arrays.bid_close):
        return _blank_incomplete()
    slip = float(settings.slippage_pips_round_trip)
    use_open = settings.entry_price == "open" and settings.entry_delay_bars > 0
    if side == "long":
        entry = float(arrays.ask_open[entry_i] if use_open else arrays.ask_close[entry_i])
        exit_price = float(arrays.bid_close[end_i])
        endpoint = ((exit_price - entry) / pip_size) - slip
        mfe = np.nanmax((arrays.bid_high[entry_i : end_i + 1] - entry) / pip_size)
        mae = np.nanmax((entry - arrays.bid_low[entry_i : end_i + 1]) / pip_size)
    else:
        entry = float(arrays.bid_open[entry_i] if use_open else arrays.bid_close[entry_i])
        exit_price = float(arrays.ask_close[end_i])
        endpoint = ((entry - exit_price) / pip_size) - slip
        mfe = np.nanmax((entry - arrays.ask_low[entry_i : end_i + 1]) / pip_size)
        mae = np.nanmax((arrays.ask_high[entry_i : end_i + 1] - entry) / pip_size)
    return ExecutionResult(
        complete=True,
        outcome="endpoint",
        time_to_exit_min=end_i - int(decision_index),
        hold_bars=end_i - entry_i + 1,
        realized_pips=float(endpoint),
        endpoint_pips=float(endpoint),
        mfe_pips=float(max(0.0, mfe)),
        mae_pips=float(max(0.0, mae)),
        same_bar_ambiguous=0,
        entry_index=entry_i,
        exit_index=end_i,
        entry_price=entry,
        exit_price=exit_price,
    )


def endpoint_after_cost(
    exec_df: pd.DataFrame,
    decision_index: int,
    side: str,
    horizon_min: int,
    pip_size: float,
    settings: ExecutionSettings,
) -> ExecutionResult:
    entry_i = int(decision_index) + int(settings.entry_delay_bars)
    end_i = entry_i + max(int(horizon_min), 1) - 1
    if entry_i < 0 or end_i >= len(exec_df):
        return _blank_incomplete()

    entry_suffix = settings.entry_price if settings.entry_delay_bars > 0 else "close"
    slip = float(settings.slippage_pips_round_trip)
    if side == "long":
        entry = float(exec_df[f"exec_ask_{entry_suffix}"].iloc[entry_i])
        exit_price = float(exec_df["exec_bid_close"].iloc[end_i])
        endpoint = ((exit_price - entry) / pip_size) - slip
        high = exec_df["exec_bid_high"].iloc[entry_i : end_i + 1].to_numpy(dtype=float)
        low = exec_df["exec_bid_low"].iloc[entry_i : end_i + 1].to_numpy(dtype=float)
        mfe = np.nanmax((high - entry) / pip_size)
        mae = np.nanmax((entry - low) / pip_size)
    else:
        entry = float(exec_df[f"exec_bid_{entry_suffix}"].iloc[entry_i])
        exit_price = float(exec_df["exec_ask_close"].iloc[end_i])
        endpoint = ((entry - exit_price) / pip_size) - slip
        high = exec_df["exec_ask_high"].iloc[entry_i : end_i + 1].to_numpy(dtype=float)
        low = exec_df["exec_ask_low"].iloc[entry_i : end_i + 1].to_numpy(dtype=float)
        mfe = np.nanmax((entry - low) / pip_size)
        mae = np.nanmax((high - entry) / pip_size)

    return ExecutionResult(
        complete=True,
        outcome="endpoint",
        time_to_exit_min=end_i - int(decision_index),
        hold_bars=end_i - entry_i + 1,
        realized_pips=float(endpoint),
        endpoint_pips=float(endpoint),
        mfe_pips=float(max(0.0, mfe)),
        mae_pips=float(max(0.0, mae)),
        same_bar_ambiguous=0,
        entry_index=entry_i,
        exit_index=end_i,
        entry_price=entry,
        exit_price=exit_price,
    )


def simulate_tp_sl_contract_arrays(
    arrays: ExecutablePriceArrays,
    decision_index: int,
    side: str,
    tp_pips: float,
    sl_pips: float,
    horizon_min: int,
    pip_size: float,
    settings: ExecutionSettings,
) -> ExecutionResult:
    entry_i = int(decision_index) + int(settings.entry_delay_bars)
    end_i = entry_i + max(int(horizon_min), 1) - 1
    if entry_i < 0 or end_i >= len(arrays.bid_close):
        return _blank_incomplete()

    use_open = settings.entry_price == "open" and settings.entry_delay_bars > 0
    slip = float(settings.slippage_pips_round_trip)
    tp_pips = float(tp_pips)
    sl_pips = float(sl_pips)
    mfe = 0.0
    mae = 0.0

    if side == "long":
        entry = float(arrays.ask_open[entry_i] if use_open else arrays.ask_close[entry_i])
        endpoint_exit = float(arrays.bid_close[end_i])
        endpoint = ((endpoint_exit - entry) / pip_size) - slip
        for j in range(entry_i, end_i + 1):
            fav = (float(arrays.bid_high[j]) - entry) / pip_size
            adv = (entry - float(arrays.bid_low[j])) / pip_size
            mfe = max(mfe, float(fav))
            mae = max(mae, float(adv))
            tp = fav >= tp_pips
            sl = adv >= sl_pips
            if tp and sl:
                return _ambiguous_result(settings, decision_index, entry_i, j, entry, endpoint, mfe, mae, tp_pips, sl_pips, slip, side, pip_size)
            if tp:
                return _terminal_result("tp_before_sl", decision_index, entry_i, j, entry, tp_pips - slip, endpoint, mfe, mae, 0, side, pip_size)
            if sl:
                return _terminal_result("sl_before_tp", decision_index, entry_i, j, entry, -sl_pips - slip, endpoint, mfe, mae, 0, side, pip_size)
    else:
        entry = float(arrays.bid_open[entry_i] if use_open else arrays.bid_close[entry_i])
        endpoint_exit = float(arrays.ask_close[end_i])
        endpoint = ((entry - endpoint_exit) / pip_size) - slip
        for j in range(entry_i, end_i + 1):
            fav = (entry - float(arrays.ask_low[j])) / pip_size
            adv = (float(arrays.ask_high[j]) - entry) / pip_size
            mfe = max(mfe, float(fav))
            mae = max(mae, float(adv))
            tp = fav >= tp_pips
            sl = adv >= sl_pips
            if tp and sl:
                return _ambiguous_result(settings, decision_index, entry_i, j, entry, endpoint, mfe, mae, tp_pips, sl_pips, slip, side, pip_size)
            if tp:
                return _terminal_result("tp_before_sl", decision_index, entry_i, j, entry, tp_pips - slip, endpoint, mfe, mae, 0, side, pip_size)
            if sl:
                return _terminal_result("sl_before_tp", decision_index, entry_i, j, entry, -sl_pips - slip, endpoint, mfe, mae, 0, side, pip_size)

    return ExecutionResult(
        complete=True,
        outcome="timeout",
        time_to_exit_min=end_i - int(decision_index),
        hold_bars=end_i - entry_i + 1,
        realized_pips=float(endpoint),
        endpoint_pips=float(endpoint),
        mfe_pips=float(max(0.0, mfe)),
        mae_pips=float(max(0.0, mae)),
        same_bar_ambiguous=0,
        entry_index=entry_i,
        exit_index=end_i,
        entry_price=entry,
        exit_price=endpoint_exit,
    )


def simulate_tp_sl_contract(
    exec_df: pd.DataFrame,
    decision_index: int,
    side: str,
    tp_pips: float,
    sl_pips: float,
    horizon_min: int,
    pip_size: float,
    settings: ExecutionSettings,
) -> ExecutionResult:
    entry_i = int(decision_index) + int(settings.entry_delay_bars)
    end_i = entry_i + max(int(horizon_min), 1) - 1
    if entry_i < 0 or end_i >= len(exec_df):
        return _blank_incomplete()

    entry_suffix = settings.entry_price if settings.entry_delay_bars > 0 else "close"
    slip = float(settings.slippage_pips_round_trip)
    tp_pips = float(tp_pips)
    sl_pips = float(sl_pips)
    mfe = 0.0
    mae = 0.0

    if side == "long":
        entry = float(exec_df[f"exec_ask_{entry_suffix}"].iloc[entry_i])
        endpoint_exit = float(exec_df["exec_bid_close"].iloc[end_i])
        endpoint = ((endpoint_exit - entry) / pip_size) - slip
        for j in range(entry_i, end_i + 1):
            fav = (float(exec_df["exec_bid_high"].iloc[j]) - entry) / pip_size
            adv = (entry - float(exec_df["exec_bid_low"].iloc[j])) / pip_size
            mfe = max(mfe, float(fav))
            mae = max(mae, float(adv))
            tp = fav >= tp_pips
            sl = adv >= sl_pips
            if tp and sl:
                return _ambiguous_result(settings, decision_index, entry_i, j, entry, endpoint, mfe, mae, tp_pips, sl_pips, slip, side, pip_size)
            if tp:
                return _terminal_result("tp_before_sl", decision_index, entry_i, j, entry, tp_pips - slip, endpoint, mfe, mae, 0, side, pip_size)
            if sl:
                return _terminal_result("sl_before_tp", decision_index, entry_i, j, entry, -sl_pips - slip, endpoint, mfe, mae, 0, side, pip_size)
    else:
        entry = float(exec_df[f"exec_bid_{entry_suffix}"].iloc[entry_i])
        endpoint_exit = float(exec_df["exec_ask_close"].iloc[end_i])
        endpoint = ((entry - endpoint_exit) / pip_size) - slip
        for j in range(entry_i, end_i + 1):
            fav = (entry - float(exec_df["exec_ask_low"].iloc[j])) / pip_size
            adv = (float(exec_df["exec_ask_high"].iloc[j]) - entry) / pip_size
            mfe = max(mfe, float(fav))
            mae = max(mae, float(adv))
            tp = fav >= tp_pips
            sl = adv >= sl_pips
            if tp and sl:
                return _ambiguous_result(settings, decision_index, entry_i, j, entry, endpoint, mfe, mae, tp_pips, sl_pips, slip, side, pip_size)
            if tp:
                return _terminal_result("tp_before_sl", decision_index, entry_i, j, entry, tp_pips - slip, endpoint, mfe, mae, 0, side, pip_size)
            if sl:
                return _terminal_result("sl_before_tp", decision_index, entry_i, j, entry, -sl_pips - slip, endpoint, mfe, mae, 0, side, pip_size)

    return ExecutionResult(
        complete=True,
        outcome="timeout",
        time_to_exit_min=end_i - int(decision_index),
        hold_bars=end_i - entry_i + 1,
        realized_pips=float(endpoint),
        endpoint_pips=float(endpoint),
        mfe_pips=float(max(0.0, mfe)),
        mae_pips=float(max(0.0, mae)),
        same_bar_ambiguous=0,
        entry_index=entry_i,
        exit_index=end_i,
        entry_price=entry,
        exit_price=endpoint_exit,
    )


def _terminal_result(
    outcome: str,
    decision_index: int,
    entry_i: int,
    exit_i: int,
    entry: float,
    realized_pips: float,
    endpoint_pips: float,
    mfe: float,
    mae: float,
    ambiguous: int,
    side: str,
    pip_size: float,
) -> ExecutionResult:
    if side == "long":
        exit_price = entry + (realized_pips * pip_size)
    else:
        exit_price = entry - (realized_pips * pip_size)
    return ExecutionResult(
        complete=True,
        outcome=outcome,
        time_to_exit_min=exit_i - int(decision_index),
        hold_bars=exit_i - entry_i + 1,
        realized_pips=float(realized_pips),
        endpoint_pips=float(endpoint_pips),
        mfe_pips=float(max(0.0, mfe)),
        mae_pips=float(max(0.0, mae)),
        same_bar_ambiguous=int(ambiguous),
        entry_index=entry_i,
        exit_index=exit_i,
        entry_price=float(entry),
        exit_price=float(exit_price),
    )


def _ambiguous_result(
    settings: ExecutionSettings,
    decision_index: int,
    entry_i: int,
    exit_i: int,
    entry: float,
    endpoint_pips: float,
    mfe: float,
    mae: float,
    tp_pips: float,
    sl_pips: float,
    slip: float,
    side: str,
    pip_size: float,
) -> ExecutionResult:
    if settings.same_bar_ambiguity == "favorable_first":
        return _terminal_result("ambiguous_favorable_first", decision_index, entry_i, exit_i, entry, tp_pips - slip, endpoint_pips, mfe, mae, 1, side, pip_size)
    if settings.same_bar_ambiguity == "exclude_ambiguous":
        return _terminal_result("ambiguous_excluded", decision_index, entry_i, exit_i, entry, np.nan, endpoint_pips, mfe, mae, 1, side, pip_size)
    if settings.same_bar_ambiguity == "separate_class":
        return _terminal_result("ambiguous_same_bar", decision_index, entry_i, exit_i, entry, np.nan, endpoint_pips, mfe, mae, 1, side, pip_size)
    return _terminal_result("ambiguous_adverse_first", decision_index, entry_i, exit_i, entry, -sl_pips - slip, endpoint_pips, mfe, mae, 1, side, pip_size)
