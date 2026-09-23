#!/usr/bin/env python3
"""Offline indicator alignment research for local OANDA M1 candles.

The goal is to score whether many indicator events consistently precede useful
future moves per instrument.  This is a research report only: it does not train
live models, enqueue trainer jobs, or modify promotion manifests.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning


PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", Path(__file__).resolve().parent))
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
CANDLE_ROOT = TRAINING_ROOT / "candles"
REPORT_ROOT = TRAINING_ROOT / "reports" / "indicator_alignment"

DEFAULT_SMOKE_INSTRUMENTS = ["EUR_USD", "USD_JPY", "USD_MXN", "EUR_HUF", "USD_HUF"]
RET_WINDOWS = [1, 3, 5, 10, 15, 20, 30, 60, 120, 240]
VOL_WINDOWS = [10, 20, 30, 60, 120, 240]
EVENT_Z_THRESHOLDS = [0.75, 1.0, 1.5, 2.0]

warnings.filterwarnings("ignore", category=PerformanceWarning)


@dataclass(frozen=True)
class EventDef:
    name: str
    direction: int
    series: pd.Series
    family: str


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def progress(message: str, **fields: Any) -> None:
    payload = {"utc": utc_iso(), "message": message}
    payload.update(fields)
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


def parse_csv_list(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def pips_size(instrument: str) -> float:
    parts = instrument.upper().replace("/", "_").split("_")
    quote = parts[-1] if parts else ""
    return 0.01 if quote == "JPY" else 0.0001


def instrument_parts(instrument: str) -> tuple[str, str]:
    parts = instrument.upper().replace("/", "_").split("_")
    if len(parts) >= 2:
        return parts[0], parts[1]
    compact = instrument.upper().replace("_", "")
    if len(compact) >= 6:
        return compact[:3], compact[3:6]
    return instrument.upper(), ""


def list_candle_instruments() -> List[str]:
    instruments: List[str] = []
    for path in sorted(CANDLE_ROOT.glob("*_M1.csv")):
        instruments.append(path.name[:-7])
    return instruments


def read_candles(instrument: str, *, max_rows: int) -> pd.DataFrame:
    path = CANDLE_ROOT / f"{instrument}_M1.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing candle file: {path}")
    usecols = [
        "datetime",
        "instrument",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "spread_pips",
    ]
    frame = pd.read_csv(path, usecols=lambda col: col in usecols)
    if max_rows > 0 and len(frame) > max_rows:
        frame = frame.tail(max_rows).copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["datetime", "open", "high", "low", "close"])
    frame = frame.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "volume", "spread_pips"]:
        if col in frame:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if "spread_pips" not in frame:
        frame["spread_pips"] = np.nan
    frame["instrument"] = instrument
    return frame


def rolling_z(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window, min_periods=max(5, window // 3)).mean()
    std = series.rolling(window, min_periods=max(5, window // 3)).std()
    return (series - mean) / std.replace(0, np.nan)


def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0).rolling(window, min_periods=window).mean()
    loss = (-delta.clip(upper=0.0)).rolling(window, min_periods=window).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def add_base_features(frame: pd.DataFrame, instrument: str) -> pd.DataFrame:
    out = frame.copy()
    pip = pips_size(instrument)
    close = out["close"].astype(float)
    out["pip_size"] = pip
    out["ret_1"] = np.log(close / close.shift(1))

    for window in RET_WINDOWS:
        out[f"logret_{window}"] = np.log(close / close.shift(window))
        out[f"move_pips_{window}"] = (close - close.shift(window)) / pip
        out[f"logret_z_{window}_120"] = rolling_z(out[f"logret_{window}"], 120)

    for window in VOL_WINDOWS:
        out[f"rv_{window}"] = out["ret_1"].rolling(window, min_periods=max(5, window // 3)).std()
        out[f"rv_z_{window}"] = rolling_z(out[f"rv_{window}"], 240)

    for short, long in [(10, 60), (30, 120), (60, 240)]:
        out[f"vol_ratio_{short}_{long}"] = out[f"rv_{short}"] / out[f"rv_{long}"].replace(0, np.nan)

    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["range_pips"] = (out["high"] - out["low"]) / pip
    out["body_pips"] = (out["close"] - out["open"]).abs() / pip
    out["signed_body_pips"] = (out["close"] - out["open"]) / pip
    out["tr_pips"] = true_range / pip
    for window in [14, 30, 60, 120, 240]:
        out[f"atr_{window}_pips"] = out["tr_pips"].rolling(window, min_periods=max(5, window // 3)).mean()
        out[f"range_z_{window}"] = rolling_z(out["range_pips"], window)
    out["atr_ratio_30_120"] = out["atr_30_pips"] / out["atr_120_pips"].replace(0, np.nan)

    candle_range = (out["high"] - out["low"]).replace(0, np.nan)
    out["close_location_value"] = (out["close"] - out["low"]) / candle_range
    out["wick_imbalance"] = (
        (out["high"] - out[["open", "close"]].max(axis=1))
        - (out[["open", "close"]].min(axis=1) - out["low"])
    ) / candle_range
    out["body_to_range"] = ((out["close"] - out["open"]).abs() / candle_range).clip(0, 10)

    out["rsi_14"] = rsi(close, 14)
    out["rsi_14_centered"] = (out["rsi_14"] - 50.0) / 50.0
    out["ema_8"] = close.ewm(span=8, adjust=False).mean()
    out["ema_21"] = close.ewm(span=21, adjust=False).mean()
    out["ema_55"] = close.ewm(span=55, adjust=False).mean()
    out["ema_8_21_atr"] = ((out["ema_8"] - out["ema_21"]) / pip) / out["atr_30_pips"].replace(0, np.nan)
    out["ema_21_55_atr"] = ((out["ema_21"] - out["ema_55"]) / pip) / out["atr_60_pips"].replace(0, np.nan)
    out["ema_21_slope_15_atr"] = ((out["ema_21"] - out["ema_21"].shift(15)) / pip) / out["atr_60_pips"].replace(0, np.nan)

    for window in [60, 240]:
        high_roll = out["high"].rolling(window, min_periods=max(10, window // 4)).max()
        low_roll = out["low"].rolling(window, min_periods=max(10, window // 4)).min()
        out[f"donchian_{window}_position"] = (close - low_roll) / (high_roll - low_roll).replace(0, np.nan)
        out[f"donchian_{window}_breakout_up"] = ((close - high_roll.shift(1)) / pip) / out["atr_60_pips"].replace(0, np.nan)
        out[f"donchian_{window}_breakout_down"] = ((low_roll.shift(1) - close) / pip) / out["atr_60_pips"].replace(0, np.nan)

    spread = pd.to_numeric(out["spread_pips"], errors="coerce")
    out["spread_available"] = spread.notna().astype(int)
    out["spread_pips_filled"] = spread.fillna(0.0)
    for window in [30, 60, 120]:
        out[f"spread_z_{window}"] = rolling_z(spread, window)
    out["spread_to_atr30"] = out["spread_pips_filled"] / out["atr_30_pips"].replace(0, np.nan)

    ts = out["datetime"]
    hour = ts.dt.hour + ts.dt.minute / 60.0
    out["hour"] = hour
    out["weekday"] = ts.dt.weekday.astype(float)
    out["is_asia"] = ((hour >= 0.0) & (hour < 7.0)).astype(int)
    out["is_london"] = ((hour >= 7.0) & (hour < 16.0)).astype(int)
    out["is_new_york"] = ((hour >= 12.0) & (hour < 21.0)).astype(int)
    out["is_london_ny_overlap"] = ((hour >= 12.0) & (hour < 16.0)).astype(int)
    out["is_rollover_30m"] = ((hour >= 21.0) & (hour < 21.5)).astype(int)
    out["is_friday_close_window"] = ((ts.dt.weekday == 4) & (hour >= 18.0)).astype(int)
    out["minutes_to_london_open"] = ((7.0 - hour) % 24.0) * 60.0
    out["minutes_to_ny_open"] = ((12.0 - hour) % 24.0) * 60.0

    for window in [30, 60, 120]:
        out[f"ret_skew_{window}"] = out["ret_1"].rolling(window, min_periods=max(10, window // 3)).skew()
        out[f"ret_kurt_{window}"] = out["ret_1"].rolling(window, min_periods=max(10, window // 3)).kurt()

    out["jump_z"] = rolling_z(out["ret_1"].abs(), 120)
    out["shock_flag"] = (out["jump_z"] > 2.0).astype(int)
    out["shock_decay_15"] = out["move_pips_15"] / out["move_pips_15"].abs().rolling(120, min_periods=30).mean().replace(0, np.nan)
    out["shock_decay_30"] = out["move_pips_30"] / out["move_pips_30"].abs().rolling(120, min_periods=30).mean().replace(0, np.nan)
    return out.copy()


def add_cross_currency_features(frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    if not frames:
        return frames
    currencies = sorted({currency for instrument in frames for currency in instrument_parts(instrument) if currency})
    for window in [5, 15, 60]:
        rows: List[pd.DataFrame] = []
        for instrument, frame in frames.items():
            base, quote = instrument_parts(instrument)
            temp = frame[["datetime", f"logret_{window}"]].copy()
            for currency in currencies:
                temp[currency] = 0.0
            if base in currencies:
                temp[base] = temp[f"logret_{window}"]
            if quote in currencies:
                temp[quote] = -temp[f"logret_{window}"]
            rows.append(temp[["datetime", *currencies]])
        wide = pd.concat(rows, ignore_index=True)
        strength = wide.groupby("datetime", sort=True)[currencies].mean().reset_index()
        strength_cols = {currency: f"{currency.lower()}_strength_{window}" for currency in currencies}
        strength = strength.rename(columns=strength_cols)
        for instrument, frame in frames.items():
            base, quote = instrument_parts(instrument)
            merged = frame.merge(strength, on="datetime", how="left")
            base_col = f"{base.lower()}_strength_{window}"
            quote_col = f"{quote.lower()}_strength_{window}"
            if base_col in merged and quote_col in merged:
                merged[f"base_minus_quote_strength_{window}"] = merged[base_col] - merged[quote_col]
            frames[instrument] = merged.copy()

    ret_wide = None
    for instrument, frame in frames.items():
        item = frame[["datetime", "ret_1"]].rename(columns={"ret_1": instrument})
        ret_wide = item if ret_wide is None else ret_wide.merge(item, on="datetime", how="outer")
    if ret_wide is not None:
        ret_wide = ret_wide.sort_values("datetime")
        pair_cols = [col for col in ret_wide.columns if col != "datetime"]
        ret_wide["xs_mean_ret1"] = ret_wide[pair_cols].mean(axis=1, skipna=True)
        ret_wide["xs_std_ret1"] = ret_wide[pair_cols].std(axis=1, skipna=True)
        for instrument, frame in frames.items():
            merged = frame.merge(ret_wide[["datetime", "xs_mean_ret1", "xs_std_ret1"]], on="datetime", how="left")
            merged["excess_ret1_vs_xs"] = merged["ret_1"] - merged["xs_mean_ret1"]
            merged["ret1_dispersion_scaled"] = merged["ret_1"] / merged["xs_std_ret1"].replace(0, np.nan)
            frames[instrument] = merged.copy()
    return frames


def event_defs(frame: pd.DataFrame) -> List[EventDef]:
    events: List[EventDef] = []

    def add_threshold_events(feature: str, family: str, thresholds: Sequence[float] = EVENT_Z_THRESHOLDS) -> None:
        if feature not in frame:
            return
        series = pd.to_numeric(frame[feature], errors="coerce")
        for threshold in thresholds:
            suffix = str(threshold).replace(".", "p")
            events.append(EventDef(f"{feature}_gt_{suffix}", 1, series > threshold, family))
            events.append(EventDef(f"{feature}_lt_minus_{suffix}", -1, series < -threshold, family))

    for feature in [
        "logret_z_1_120",
        "logret_z_5_120",
        "logret_z_15_120",
        "logret_z_60_120",
        "ema_8_21_atr",
        "ema_21_55_atr",
        "ema_21_slope_15_atr",
        "base_minus_quote_strength_5",
        "base_minus_quote_strength_15",
        "base_minus_quote_strength_60",
        "excess_ret1_vs_xs",
        "ret1_dispersion_scaled",
    ]:
        add_threshold_events(feature, "momentum_strength")

    for feature in ["rv_z_30", "rv_z_60", "range_z_30", "range_z_120", "jump_z"]:
        add_threshold_events(feature, "volatility_shock", [1.0, 1.5, 2.0])

    for feature in ["ret_skew_60", "ret_skew_120", "ret_kurt_60", "ret_kurt_120"]:
        add_threshold_events(feature, "distribution_shape", [0.75, 1.0, 1.5])

    if "rsi_14" in frame:
        rsi_series = pd.to_numeric(frame["rsi_14"], errors="coerce")
        events.extend(
            [
                EventDef("rsi14_below_30_reversion_long", 1, rsi_series < 30.0, "oscillator_reversion"),
                EventDef("rsi14_below_20_reversion_long", 1, rsi_series < 20.0, "oscillator_reversion"),
                EventDef("rsi14_above_70_reversion_short", -1, rsi_series > 70.0, "oscillator_reversion"),
                EventDef("rsi14_above_80_reversion_short", -1, rsi_series > 80.0, "oscillator_reversion"),
                EventDef("rsi14_above_60_continuation_long", 1, rsi_series > 60.0, "oscillator_continuation"),
                EventDef("rsi14_below_40_continuation_short", -1, rsi_series < 40.0, "oscillator_continuation"),
            ]
        )

    if "close_location_value" in frame:
        clv = pd.to_numeric(frame["close_location_value"], errors="coerce")
        events.extend(
            [
                EventDef("close_location_above_80_long", 1, clv > 0.80, "candle_pressure"),
                EventDef("close_location_below_20_short", -1, clv < 0.20, "candle_pressure"),
                EventDef("close_location_below_20_reversion_long", 1, clv < 0.20, "candle_reversion"),
                EventDef("close_location_above_80_reversion_short", -1, clv > 0.80, "candle_reversion"),
            ]
        )

    for window in [60, 240]:
        up = f"donchian_{window}_breakout_up"
        down = f"donchian_{window}_breakout_down"
        if up in frame:
            series = pd.to_numeric(frame[up], errors="coerce")
            events.append(EventDef(f"{up}_gt_0_long", 1, series > 0.0, "breakout"))
            events.append(EventDef(f"{up}_gt_0p5_long", 1, series > 0.5, "breakout"))
        if down in frame:
            series = pd.to_numeric(frame[down], errors="coerce")
            events.append(EventDef(f"{down}_gt_0_short", -1, series > 0.0, "breakout"))
            events.append(EventDef(f"{down}_gt_0p5_short", -1, series > 0.5, "breakout"))

    for session_col in ["is_asia", "is_london", "is_new_york", "is_london_ny_overlap"]:
        if session_col in frame and "logret_z_15_120" in frame:
            session = pd.to_numeric(frame[session_col], errors="coerce") > 0
            momentum = pd.to_numeric(frame["logret_z_15_120"], errors="coerce")
            events.append(EventDef(f"{session_col}_mom15_gt_1_long", 1, session & (momentum > 1.0), "session_momentum"))
            events.append(EventDef(f"{session_col}_mom15_lt_minus_1_short", -1, session & (momentum < -1.0), "session_momentum"))

    return events


def future_outcomes(frame: pd.DataFrame, horizon: int, direction: int) -> Dict[str, pd.Series]:
    pip = float(frame["pip_size"].iloc[0])
    close = frame["close"]
    future_close = close.shift(-horizon)
    future_pips = (future_close - close) / pip
    shifted_high = frame["high"].shift(-1)
    shifted_low = frame["low"].shift(-1)
    future_high = shifted_high.rolling(horizon, min_periods=horizon).max().shift(-(horizon - 1))
    future_low = shifted_low.rolling(horizon, min_periods=horizon).min().shift(-(horizon - 1))
    if direction > 0:
        mfe = (future_high - close) / pip
        mae = (close - future_low) / pip
    else:
        mfe = (close - future_low) / pip
        mae = (future_high - close) / pip
    directed = future_pips * direction
    cost = pd.to_numeric(frame.get("spread_pips_filled", 0.0), errors="coerce").fillna(0.0)
    net = directed - cost
    return {"directed_pips": directed, "net_pips": net, "mfe_pips": mfe, "mae_pips": mae}


def profit_factor(values: pd.Series) -> float:
    wins = values[values > 0].sum()
    losses = values[values < 0].sum()
    if losses == 0:
        return float(wins) if wins > 0 else 0.0
    return float(wins / abs(losses))


def event_score(*, mean_net: float, win_rate: float, profit_factor_value: float, positive_week_rate: float, sample_count: int) -> float:
    sample_weight = min(1.0, math.log1p(max(0, sample_count)) / math.log1p(500.0))
    pf_weight = min(3.0, max(0.0, profit_factor_value)) / 3.0
    win_edge = max(0.0, win_rate - 0.5) * 2.0
    return mean_net * sample_weight * (0.35 + 0.65 * positive_week_rate) * (0.35 + 0.65 * win_edge) * (0.50 + 0.50 * pf_weight)


def score_event(
    *,
    instrument: str,
    frame: pd.DataFrame,
    event: EventDef,
    horizon: int,
    min_events: int,
    precomputed_outcomes: Dict[tuple[int, int], Dict[str, pd.Series]],
) -> Dict[str, Any] | None:
    outcomes = precomputed_outcomes[(horizon, event.direction)]
    mask = event.series.fillna(False).astype(bool)
    valid = mask & outcomes["net_pips"].notna()
    sample_count = int(valid.sum())
    if sample_count < min_events:
        return None
    net = outcomes["net_pips"][valid]
    directed = outcomes["directed_pips"][valid]
    mfe = outcomes["mfe_pips"][valid]
    mae = outcomes["mae_pips"][valid]
    weeks = frame.loc[valid, "datetime"].dt.strftime("%G-W%V")
    by_week = net.groupby(weeks).mean()
    positive_week_rate = float((by_week > 0).mean()) if len(by_week) else 0.0
    win_rate = float((net > 0).mean())
    pf = profit_factor(net)
    mean_net = float(net.mean())
    score = event_score(
        mean_net=mean_net,
        win_rate=win_rate,
        profit_factor_value=pf,
        positive_week_rate=positive_week_rate,
        sample_count=sample_count,
    )
    return {
        "instrument": instrument,
        "event": event.name,
        "family": event.family,
        "direction": "long" if event.direction > 0 else "short",
        "horizon_minutes": horizon,
        "sample_count": sample_count,
        "week_count": int(len(by_week)),
        "positive_week_rate": round(positive_week_rate, 6),
        "win_rate": round(win_rate, 6),
        "mean_directed_pips": round(float(directed.mean()), 6),
        "mean_net_pips": round(mean_net, 6),
        "median_net_pips": round(float(net.median()), 6),
        "total_net_pips": round(float(net.sum()), 6),
        "profit_factor": round(pf, 6),
        "mean_mfe_pips": round(float(mfe.mean()), 6),
        "mean_mae_pips": round(float(mae.mean()), 6),
        "mfe_mae_ratio": round(float(mfe.mean() / max(1e-9, mae.mean())), 6),
        "alignment_score": round(score, 6),
    }


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_alignment(
    *,
    instruments: Sequence[str],
    horizons: Sequence[int],
    max_rows: int,
    min_events: int,
    top_n: int,
    output_dir: Path,
    tag: str,
) -> Dict[str, Any]:
    frames: Dict[str, pd.DataFrame] = {}
    load_errors: Dict[str, str] = {}
    progress(
        "run_start",
        instrument_count=len(instruments),
        horizons=list(horizons),
        max_rows=max_rows,
        min_events=min_events,
        top_n=top_n,
        tag=tag,
    )
    for idx, instrument in enumerate(instruments, start=1):
        try:
            frames[instrument] = add_base_features(read_candles(instrument, max_rows=max_rows), instrument)
            progress(
                "instrument_loaded",
                instrument=instrument,
                index=idx,
                total=len(instruments),
                rows=len(frames[instrument]),
                features=len(frames[instrument].columns),
            )
        except Exception as exc:
            load_errors[instrument] = f"{type(exc).__name__}: {exc}"
            progress(
                "instrument_load_error",
                instrument=instrument,
                index=idx,
                total=len(instruments),
                error=load_errors[instrument],
            )
    progress("cross_currency_features_start", instrument_count=len(frames))
    frames = add_cross_currency_features(frames)
    progress("cross_currency_features_done", instrument_count=len(frames))

    rows: List[Dict[str, Any]] = []
    feature_counts: Dict[str, int] = {}
    event_counts: Dict[str, int] = {}
    for idx, (instrument, frame) in enumerate(frames.items(), start=1):
        events = event_defs(frame)
        progress(
            "instrument_scoring_start",
            instrument=instrument,
            index=idx,
            total=len(frames),
            rows=len(frame),
            events=len(events),
            features=len(frame.columns),
        )
        precomputed_outcomes = {
            (int(horizon), int(direction)): future_outcomes(frame, int(horizon), int(direction))
            for horizon in horizons
            for direction in (-1, 1)
        }
        feature_counts[instrument] = int(len(frame.columns))
        event_counts[instrument] = int(len(events))
        for event in events:
            for horizon in horizons:
                scored = score_event(
                    instrument=instrument,
                    frame=frame,
                    event=event,
                    horizon=int(horizon),
                    min_events=min_events,
                    precomputed_outcomes=precomputed_outcomes,
                )
                if scored:
                    rows.append(scored)
        progress(
            "instrument_scoring_done",
            instrument=instrument,
            index=idx,
            total=len(frames),
            scored_rows=sum(1 for row in rows if row["instrument"] == instrument),
            total_scored_rows=len(rows),
        )

    rows.sort(
        key=lambda row: (
            str(row["instrument"]),
            -safe_float(row["alignment_score"]),
            -safe_float(row["mean_net_pips"]),
            -int(row["sample_count"]),
        )
    )
    best_rows: List[Dict[str, Any]] = []
    for instrument in sorted(frames):
        subset = [row for row in rows if row["instrument"] == instrument]
        best_rows.extend(subset[:top_n])

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"{tag}_{stamp}"
    all_csv = run_dir / "indicator_alignment.csv"
    best_csv = run_dir / "best_indicators_per_instrument.csv"
    summary_json = run_dir / "summary.json"
    latest_all = output_dir / f"latest_{tag}_indicator_alignment.csv"
    latest_best = output_dir / f"latest_{tag}_best_indicators_per_instrument.csv"
    latest_summary = output_dir / f"latest_{tag}_summary.json"

    progress("write_outputs_start", row_count=len(rows), best_row_count=len(best_rows), run_dir=str(run_dir))
    write_csv(all_csv, rows)
    write_csv(best_csv, best_rows)
    write_csv(latest_all, rows)
    write_csv(latest_best, best_rows)
    summary = {
        "generated_utc": utc_iso(),
        "tag": tag,
        "instruments_requested": list(instruments),
        "instruments_loaded": sorted(frames),
        "load_errors": load_errors,
        "horizons": list(horizons),
        "max_rows": max_rows,
        "min_events": min_events,
        "top_n": top_n,
        "row_count": len(rows),
        "best_row_count": len(best_rows),
        "feature_counts": feature_counts,
        "event_counts": event_counts,
        "outputs": {
            "run_dir": str(run_dir),
            "indicator_alignment_csv": str(all_csv),
            "best_indicators_csv": str(best_csv),
            "latest_indicator_alignment_csv": str(latest_all),
            "latest_best_indicators_csv": str(latest_best),
            "summary_json": str(summary_json),
            "latest_summary_json": str(latest_summary),
        },
        "top_rows": best_rows[: min(20, len(best_rows))],
    }
    atomic_write_json(summary_json, summary)
    atomic_write_json(latest_summary, summary)
    progress("write_outputs_done", row_count=len(rows), best_row_count=len(best_rows), latest_summary=str(latest_summary))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline indicator alignment research on local OANDA M1 candles")
    parser.add_argument(
        "--instruments",
        default=",".join(DEFAULT_SMOKE_INSTRUMENTS),
        help="Comma-separated instruments. Default is smoke set.",
    )
    parser.add_argument("--all-pairs", action="store_true", help="Use all local *_M1.csv candle files")
    parser.add_argument("--max-pairs", type=int, default=0, help="Limit number of instruments after selection")
    parser.add_argument("--horizons", default="15,30,60,120", help="Comma-separated forward horizons in minutes")
    parser.add_argument("--max-rows", type=int, default=80_000, help="Use only the most recent N rows per instrument; 0 means all")
    parser.add_argument("--min-events", type=int, default=30, help="Minimum event hits required to score an event")
    parser.add_argument("--top-n", type=int, default=25, help="Best rows per instrument to write to best report")
    parser.add_argument("--tag", default="smoke", help="Output tag")
    parser.add_argument("--output-dir", default=str(REPORT_ROOT), help="Output directory")
    args = parser.parse_args()

    instruments = list_candle_instruments() if args.all_pairs else parse_csv_list(args.instruments)
    if args.max_pairs > 0:
        instruments = instruments[: args.max_pairs]
    horizons = [int(item) for item in parse_csv_list(args.horizons)]
    summary = run_alignment(
        instruments=instruments,
        horizons=horizons,
        max_rows=max(0, int(args.max_rows)),
        min_events=max(1, int(args.min_events)),
        top_n=max(1, int(args.top_n)),
        output_dir=Path(args.output_dir),
        tag=args.tag,
    )
    print(json.dumps({
        "generated_utc": summary["generated_utc"],
        "tag": summary["tag"],
        "instruments_loaded": summary["instruments_loaded"],
        "row_count": summary["row_count"],
        "best_row_count": summary["best_row_count"],
        "outputs": summary["outputs"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
