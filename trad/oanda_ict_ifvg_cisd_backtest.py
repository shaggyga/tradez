#!/usr/bin/env python3
"""Backtest an HTF confluence -> IFVG -> CISD -> 5m/15m FVG target model.

This is a mechanical interpretation of a discretionary FXReplay workflow:

1. Wait for a higher-timeframe confluence:
   - HTF FVG first touch.
   - HTF order-block / rejection-block proxy first touch.
   - HTF liquidity sweep and rejection.
2. In the manipulation leg into that confluence, find the highest-timeframe
   FVG that later inverts.
3. Wait for a 5m CISD proxy: close through internal structure with displacement.
4. Enter on a retrace to the IFVG midpoint.
5. Target the nearest still-unfilled 5m or 15m FVG in the trade direction.

No broker calls are made. The script reads local OANDA M1 candle CSVs and writes
research artifacts under data/oanda_training_manager/reports/ict_ifvg_cisd.
"""

from __future__ import annotations

import argparse
import csv
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
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "ict_ifvg_cisd"

DEFAULT_MAJOR_PAIRS = [
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
]

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
class BacktestConfig:
    htf_timeframes: tuple[str, ...] = ("4h", "1h")
    ifvg_timeframes: tuple[str, ...] = ("1h", "15min", "5min")
    target_timeframes: tuple[str, ...] = ("15min", "5min")
    cisd_timeframe: str = "5min"
    min_gap_pips: float = 0.5
    min_manipulation_pips: float = 4.0
    manipulation_lookback_bars: int = 96
    ifvg_wait_minutes: int = 24 * 60
    cisd_wait_minutes: int = 12 * 60
    entry_wait_minutes: int = 12 * 60
    max_hold_minutes: int = 48 * 60
    sweep_lookback_bars: int = 20
    ob_break_lookback_bars: int = 20
    ob_search_bars: int = 6
    cisd_lookback_bars: int = 6
    cisd_body_atr_mult: float = 0.20
    entry_zone_fraction: float = 0.50
    stop_buffer_pips: float = 1.0
    min_stop_pips: float = 2.0
    max_stop_pips: float = 80.0
    min_rr: float = 0.25
    max_rr: float = 0.0
    target_lookback_days: int = 30
    cooldown_minutes: int = 120
    round_turn_cost_pips: float = 0.8


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, np.bool_):
        return bool(value)
    return str(value)


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


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def normalize_pair(value: str) -> str:
    text = str(value or "").strip().upper().replace("/", "_").replace("-", "_")
    if "_" not in text and len(text) == 6:
        return text[:3] + "_" + text[3:]
    return text


def pip_multiplier(pair: str) -> float:
    return 100.0 if normalize_pair(pair) in PIP_LOCATION_MINUS2 else 10_000.0


def pair_path(pair: str) -> Path:
    return CANDLE_ROOT / f"{normalize_pair(pair)}_M1.csv"


def pair_exists(pair: str) -> bool:
    return pair_path(pair).exists()


def normalize_timeframe(value: str) -> str:
    text = str(value or "").strip()
    return TIMEFRAME_ALIASES.get(text.upper(), text.lower())


def parse_timeframes(value: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = [part for part in value.replace(";", ",").split(",") if part.strip()]
    else:
        parts = list(value)
    return tuple(normalize_timeframe(part) for part in parts)


def parse_utc(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def available_pairs() -> list[str]:
    pairs = []
    for path in sorted(CANDLE_ROOT.glob("*_M1.csv")):
        pairs.append(path.name.removesuffix("_M1.csv"))
    return pairs


def parse_pairs(value: str, max_pairs: int = 0) -> list[str]:
    text = str(value or "majors").strip()
    if text.lower() == "majors":
        pairs = DEFAULT_MAJOR_PAIRS.copy()
    elif text.lower() == "all":
        pairs = available_pairs()
    else:
        pairs = [normalize_pair(part) for part in text.replace(";", ",").split(",") if part.strip()]
    pairs = [pair for pair in pairs if pair_exists(pair)]
    if max_pairs > 0:
        pairs = pairs[:max_pairs]
    return pairs


def load_m1_ohlc(pair: str, since: str = "", until: str = "") -> pd.DataFrame:
    path = pair_path(pair)
    if not path.exists():
        raise FileNotFoundError(f"Missing candle file: {path}")
    keep = {"time", "datetime", "open", "high", "low", "close", "spread_pips"}
    frame = pd.read_csv(
        path,
        usecols=lambda column: column in keep,
        dtype={
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "spread_pips": "float64",
        },
    )
    source_time = "datetime" if "datetime" in frame.columns else "time"
    frame["time_utc"] = pd.to_datetime(frame[source_time], errors="coerce", utc=True)
    for column in ["open", "high", "low", "close", "spread_pips"]:
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
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
    return frame[["open", "high", "low", "close", "spread_pips"]]


def resample_ohlc(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    tf = normalize_timeframe(timeframe)
    out = pd.DataFrame(
        {
            "open": frame["open"].resample(tf, label="right", closed="right").first(),
            "high": frame["high"].resample(tf, label="right", closed="right").max(),
            "low": frame["low"].resample(tf, label="right", closed="right").min(),
            "close": frame["close"].resample(tf, label="right", closed="right").last(),
            "spread_pips": frame["spread_pips"].resample(tf, label="right", closed="right").median(),
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


def add_cisd_features(frame: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    out = frame.copy()
    out["prev_structure_high"] = out["high"].shift(1).rolling(cfg.cisd_lookback_bars).max()
    out["prev_structure_low"] = out["low"].shift(1).rolling(cfg.cisd_lookback_bars).min()
    out["atr"] = true_range(out).rolling(14, min_periods=5).mean()
    out["body"] = (out["close"] - out["open"]).abs()
    candle_range = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["close_position"] = (out["close"] - out["low"]) / candle_range
    return out


def detect_fvgs(frame: pd.DataFrame, timeframe: str, pair: str, min_gap_pips: float) -> list[dict[str, Any]]:
    multiplier = pip_multiplier(pair)
    high_2 = frame["high"].shift(2)
    low_2 = frame["low"].shift(2)
    bull = frame["low"] > high_2
    bear = frame["high"] < low_2
    records: list[dict[str, Any]] = []
    index = frame.index
    for pos in np.flatnonzero(bull.to_numpy(dtype=bool, na_value=False)):
        lower = float(high_2.iloc[pos])
        upper = float(frame["low"].iloc[pos])
        gap_pips = (upper - lower) * multiplier
        if math.isfinite(gap_pips) and gap_pips >= min_gap_pips:
            records.append(
                {
                    "id": f"{timeframe}:{index[pos].isoformat()}:bull_fvg",
                    "source": "fvg",
                    "timeframe": timeframe,
                    "formed_time": index[pos],
                    "anchor_time": index[pos - 2],
                    "direction": 1,
                    "lower": lower,
                    "upper": upper,
                    "gap_pips": gap_pips,
                }
            )
    for pos in np.flatnonzero(bear.to_numpy(dtype=bool, na_value=False)):
        lower = float(frame["high"].iloc[pos])
        upper = float(low_2.iloc[pos])
        gap_pips = (upper - lower) * multiplier
        if math.isfinite(gap_pips) and gap_pips >= min_gap_pips:
            records.append(
                {
                    "id": f"{timeframe}:{index[pos].isoformat()}:bear_fvg",
                    "source": "fvg",
                    "timeframe": timeframe,
                    "formed_time": index[pos],
                    "anchor_time": index[pos - 2],
                    "direction": -1,
                    "lower": lower,
                    "upper": upper,
                    "gap_pips": gap_pips,
                }
            )
    return sorted(records, key=lambda row: row["formed_time"])


def annotate_gap_fill_times(gaps: Sequence[dict[str, Any]], frame: pd.DataFrame) -> list[dict[str, Any]]:
    if not gaps:
        return []
    index = frame.index
    lows = frame["low"].to_numpy(dtype=float)
    highs = frame["high"].to_numpy(dtype=float)
    out: list[dict[str, Any]] = []
    for gap in gaps:
        start = int(index.searchsorted(gap["formed_time"], side="right"))
        fill_time: pd.Timestamp | None = None
        if start < len(index):
            if int(gap["direction"]) > 0:
                hits = np.flatnonzero(lows[start:] <= float(gap["lower"]))
            else:
                hits = np.flatnonzero(highs[start:] >= float(gap["upper"]))
            if len(hits):
                fill_time = index[start + int(hits[0])]
        out.append({**gap, "fill_time": fill_time})
    return out


def detect_ob_rb_proxy_zones(
    frame: pd.DataFrame,
    timeframe: str,
    pair: str,
    cfg: BacktestConfig,
) -> list[dict[str, Any]]:
    multiplier = pip_multiplier(pair)
    prev_high = frame["high"].shift(1).rolling(cfg.ob_break_lookback_bars).max()
    prev_low = frame["low"].shift(1).rolling(cfg.ob_break_lookback_bars).min()
    atr = true_range(frame).rolling(14, min_periods=5).mean()
    body = (frame["close"] - frame["open"]).abs()
    up_break = (frame["close"] > prev_high) & (body >= 0.35 * atr)
    down_break = (frame["close"] < prev_low) & (body >= 0.35 * atr)
    records: list[dict[str, Any]] = []
    index = frame.index
    for pos in np.flatnonzero(up_break.to_numpy(dtype=bool, na_value=False)):
        start = max(0, pos - cfg.ob_search_bars)
        chosen: int | None = None
        for probe in range(pos - 1, start - 1, -1):
            if frame["close"].iloc[probe] < frame["open"].iloc[probe]:
                chosen = probe
                break
        if chosen is None:
            continue
        lower = float(frame["low"].iloc[chosen])
        upper = float(frame["high"].iloc[chosen])
        zone_pips = (upper - lower) * multiplier
        if zone_pips >= cfg.min_gap_pips:
            records.append(
                {
                    "id": f"{timeframe}:{index[pos].isoformat()}:bull_ob_rb_proxy",
                    "source": "ob_rb_proxy",
                    "timeframe": timeframe,
                    "formed_time": index[pos],
                    "anchor_time": index[chosen],
                    "direction": 1,
                    "lower": lower,
                    "upper": upper,
                    "gap_pips": zone_pips,
                }
            )
    for pos in np.flatnonzero(down_break.to_numpy(dtype=bool, na_value=False)):
        start = max(0, pos - cfg.ob_search_bars)
        chosen = None
        for probe in range(pos - 1, start - 1, -1):
            if frame["close"].iloc[probe] > frame["open"].iloc[probe]:
                chosen = probe
                break
        if chosen is None:
            continue
        lower = float(frame["low"].iloc[chosen])
        upper = float(frame["high"].iloc[chosen])
        zone_pips = (upper - lower) * multiplier
        if zone_pips >= cfg.min_gap_pips:
            records.append(
                {
                    "id": f"{timeframe}:{index[pos].isoformat()}:bear_ob_rb_proxy",
                    "source": "ob_rb_proxy",
                    "timeframe": timeframe,
                    "formed_time": index[pos],
                    "anchor_time": index[chosen],
                    "direction": -1,
                    "lower": lower,
                    "upper": upper,
                    "gap_pips": zone_pips,
                }
            )
    return sorted(records, key=lambda row: row["formed_time"])


def zone_first_touch_event(zone: dict[str, Any], bars: pd.DataFrame, source_label: str) -> dict[str, Any] | None:
    future = bars[bars.index > zone["formed_time"]]
    if future.empty:
        return None
    overlap = (future["high"] >= float(zone["lower"])) & (future["low"] <= float(zone["upper"]))
    if not bool(overlap.any()):
        return None
    event_time = overlap[overlap].index[0]
    row = future.loc[event_time]
    return {
        "event_time": event_time,
        "direction": int(zone["direction"]),
        "confluence_type": f"htf_{source_label}_touch",
        "htf_timeframe": zone["timeframe"],
        "zone_id": zone["id"],
        "zone_source": zone["source"],
        "zone_lower": float(zone["lower"]),
        "zone_upper": float(zone["upper"]),
        "event_high": float(row["high"]),
        "event_low": float(row["low"]),
        "event_close": float(row["close"]),
    }


def liquidity_sweep_events(frame: pd.DataFrame, timeframe: str, pair: str, cfg: BacktestConfig) -> list[dict[str, Any]]:
    multiplier = pip_multiplier(pair)
    prev_high = frame["high"].shift(1).rolling(cfg.sweep_lookback_bars).max()
    prev_low = frame["low"].shift(1).rolling(cfg.sweep_lookback_bars).min()
    short_sweep = (
        (frame["high"] > prev_high)
        & (frame["close"] < prev_high)
        & (frame["close"] < frame["open"])
        & ((frame["high"] - prev_high) * multiplier >= cfg.min_gap_pips)
    )
    long_sweep = (
        (frame["low"] < prev_low)
        & (frame["close"] > prev_low)
        & (frame["close"] > frame["open"])
        & ((prev_low - frame["low"]) * multiplier >= cfg.min_gap_pips)
    )
    records: list[dict[str, Any]] = []
    for event_time in short_sweep[short_sweep].index:
        row = frame.loc[event_time]
        records.append(
            {
                "event_time": event_time,
                "direction": -1,
                "confluence_type": "htf_liquidity_sweep",
                "htf_timeframe": timeframe,
                "zone_id": f"{timeframe}:{event_time.isoformat()}:buy_side_sweep",
                "zone_source": "liquidity_sweep",
                "zone_lower": float(prev_high.loc[event_time]),
                "zone_upper": float(row["high"]),
                "event_high": float(row["high"]),
                "event_low": float(row["low"]),
                "event_close": float(row["close"]),
            }
        )
    for event_time in long_sweep[long_sweep].index:
        row = frame.loc[event_time]
        records.append(
            {
                "event_time": event_time,
                "direction": 1,
                "confluence_type": "htf_liquidity_sweep",
                "htf_timeframe": timeframe,
                "zone_id": f"{timeframe}:{event_time.isoformat()}:sell_side_sweep",
                "zone_source": "liquidity_sweep",
                "zone_lower": float(row["low"]),
                "zone_upper": float(prev_low.loc[event_time]),
                "event_high": float(row["high"]),
                "event_low": float(row["low"]),
                "event_close": float(row["close"]),
            }
        )
    return records


def build_confluence_events(
    pair: str,
    bars_by_tf: dict[str, pd.DataFrame],
    fvg_by_tf: dict[str, list[dict[str, Any]]],
    ob_by_tf: dict[str, list[dict[str, Any]]],
    cfg: BacktestConfig,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for timeframe in cfg.htf_timeframes:
        bars = bars_by_tf[timeframe]
        for zone in fvg_by_tf.get(timeframe, []):
            event = zone_first_touch_event(zone, bars, "fvg")
            if event is not None:
                events.append(event)
        for zone in ob_by_tf.get(timeframe, []):
            event = zone_first_touch_event(zone, bars, "ob_rb_proxy")
            if event is not None:
                events.append(event)
        events.extend(liquidity_sweep_events(bars, timeframe, pair, cfg))
    return sorted(events, key=lambda row: (row["event_time"], row["htf_timeframe"], row["confluence_type"]))


def gap_inverted_before(gap: dict[str, Any], bars: pd.DataFrame, trade_direction: int, end_time: pd.Timestamp) -> bool:
    subset = bars[(bars.index > gap["formed_time"]) & (bars.index < end_time)]
    if subset.empty:
        return False
    if trade_direction < 0:
        return bool((subset["close"] < float(gap["lower"])).any())
    return bool((subset["close"] > float(gap["upper"])).any())


def find_gap_inversion(
    gap: dict[str, Any],
    bars: pd.DataFrame,
    trade_direction: int,
    start_time: pd.Timestamp,
    max_wait_minutes: int,
) -> pd.Timestamp | None:
    end_time = start_time + pd.Timedelta(minutes=max_wait_minutes)
    subset = bars[(bars.index >= start_time) & (bars.index <= end_time)]
    if subset.empty:
        return None
    if trade_direction < 0:
        mask = subset["close"] < float(gap["lower"])
    else:
        mask = subset["close"] > float(gap["upper"])
    if not bool(mask.any()):
        return None
    return mask[mask].index[0]


def manipulation_leg(
    bars: pd.DataFrame,
    event: dict[str, Any],
    pair: str,
    cfg: BacktestConfig,
) -> dict[str, Any] | None:
    event_time = event["event_time"]
    hist = bars[bars.index <= event_time].tail(cfg.manipulation_lookback_bars)
    if len(hist) < max(8, cfg.cisd_lookback_bars + 2):
        return None
    direction = int(event["direction"])
    multiplier = pip_multiplier(pair)
    if direction < 0:
        start_time = hist["low"].idxmin()
        leg = bars[(bars.index >= start_time) & (bars.index <= event_time)]
        if leg.empty:
            return None
        start_price = float(leg["low"].iloc[0])
        extreme_price = float(leg["high"].max())
        extreme_time = leg["high"].idxmax()
        move_pips = (extreme_price - start_price) * multiplier
    else:
        start_time = hist["high"].idxmax()
        leg = bars[(bars.index >= start_time) & (bars.index <= event_time)]
        if leg.empty:
            return None
        start_price = float(leg["high"].iloc[0])
        extreme_price = float(leg["low"].min())
        extreme_time = leg["low"].idxmin()
        move_pips = (start_price - extreme_price) * multiplier
    if move_pips < cfg.min_manipulation_pips:
        return None
    return {
        "leg_start_time": start_time,
        "leg_extreme_time": extreme_time,
        "leg_extreme_price": extreme_price,
        "manipulation_pips": move_pips,
    }


def find_ifvg(
    pair: str,
    event: dict[str, Any],
    bars_by_tf: dict[str, pd.DataFrame],
    fvg_by_tf: dict[str, list[dict[str, Any]]],
    cfg: BacktestConfig,
) -> dict[str, Any] | None:
    direction = int(event["direction"])
    wanted_fvg_direction = -direction
    event_time = event["event_time"]
    for timeframe in cfg.ifvg_timeframes:
        bars = bars_by_tf[timeframe]
        leg = manipulation_leg(bars, event, pair, cfg)
        if leg is None:
            continue
        candidates = [
            gap
            for gap in fvg_by_tf.get(timeframe, [])
            if int(gap["direction"]) == wanted_fvg_direction
            and leg["leg_start_time"] <= gap["formed_time"] <= event_time
            and not gap_inverted_before(gap, bars, direction, event_time)
        ]
        for gap in sorted(candidates, key=lambda row: row["formed_time"], reverse=True):
            inversion_time = find_gap_inversion(gap, bars, direction, event_time, cfg.ifvg_wait_minutes)
            if inversion_time is None:
                continue
            return {
                **gap,
                **leg,
                "ifvg_timeframe": timeframe,
                "ifvg_inversion_time": inversion_time,
            }
    return None


def find_cisd(
    event: dict[str, Any],
    ifvg: dict[str, Any],
    cisd_bars: pd.DataFrame,
    cfg: BacktestConfig,
) -> dict[str, Any] | None:
    direction = int(event["direction"])
    start = ifvg["ifvg_inversion_time"]
    end = start + pd.Timedelta(minutes=cfg.cisd_wait_minutes)
    subset = cisd_bars[(cisd_bars.index > start) & (cisd_bars.index <= end)]
    if subset.empty:
        return None
    displacement = subset["body"] >= (subset["atr"] * cfg.cisd_body_atr_mult)
    if direction < 0:
        mask = (
            (subset["close"] < subset["prev_structure_low"])
            & displacement
            & (subset["close_position"] <= 0.45)
        )
    else:
        mask = (
            (subset["close"] > subset["prev_structure_high"])
            & displacement
            & (subset["close_position"] >= 0.55)
        )
    if not bool(mask.fillna(False).any()):
        return None
    cisd_time = mask[mask.fillna(False)].index[0]
    row = cisd_bars.loc[cisd_time]
    return {
        "cisd_time": cisd_time,
        "cisd_timeframe": cfg.cisd_timeframe,
        "cisd_close": float(row["close"]),
        "cisd_structure_level": float(row["prev_structure_low"] if direction < 0 else row["prev_structure_high"]),
        "cisd_atr": float(row["atr"]),
    }


def entry_price_for_ifvg(ifvg: dict[str, Any], cfg: BacktestConfig) -> float:
    lower = float(ifvg["lower"])
    upper = float(ifvg["upper"])
    fraction = min(max(float(cfg.entry_zone_fraction), 0.0), 1.0)
    return lower + (upper - lower) * fraction


def find_entry(
    event: dict[str, Any],
    ifvg: dict[str, Any],
    cisd: dict[str, Any],
    m1: pd.DataFrame,
    cfg: BacktestConfig,
) -> dict[str, Any] | None:
    entry_price = entry_price_for_ifvg(ifvg, cfg)
    start = cisd["cisd_time"]
    end = start + pd.Timedelta(minutes=cfg.entry_wait_minutes)
    subset = m1[(m1.index > start) & (m1.index <= end)]
    if subset.empty:
        return None
    touched = (subset["low"] <= entry_price) & (subset["high"] >= entry_price)
    if not bool(touched.any()):
        return None
    entry_time = touched[touched].index[0]
    row = subset.loc[entry_time]
    return {
        "entry_time": entry_time,
        "entry_price": float(entry_price),
        "entry_bar_open": float(row["open"]),
        "entry_bar_high": float(row["high"]),
        "entry_bar_low": float(row["low"]),
        "entry_bar_close": float(row["close"]),
        "entry_spread_pips": finite_float(row.get("spread_pips"), float("nan")),
    }


def gap_unfilled_at(gap: dict[str, Any], bars: pd.DataFrame, asof_time: pd.Timestamp) -> bool:
    if "fill_time" in gap:
        fill_time = gap.get("fill_time")
        return fill_time is None or pd.Timestamp(fill_time) > asof_time
    subset = bars[(bars.index > gap["formed_time"]) & (bars.index <= asof_time)]
    if subset.empty:
        return True
    if int(gap["direction"]) > 0:
        return bool(subset["low"].min() > float(gap["lower"]))
    return bool(subset["high"].max() < float(gap["upper"]))


def select_target_gap(
    direction: int,
    entry_price: float,
    entry_time: pd.Timestamp,
    target_gaps: Sequence[dict[str, Any]],
    bars_by_tf: dict[str, pd.DataFrame],
    cfg: BacktestConfig,
) -> dict[str, Any] | None:
    earliest = entry_time - pd.Timedelta(days=cfg.target_lookback_days)
    candidates: list[dict[str, Any]] = []
    for gap in target_gaps:
        formed_time = gap["formed_time"]
        if formed_time >= entry_time or formed_time < earliest:
            continue
        if direction > 0:
            if float(gap["lower"]) <= entry_price:
                continue
            target_price = float(gap["lower"])
            distance = target_price - entry_price
        else:
            if float(gap["upper"]) >= entry_price:
                continue
            target_price = float(gap["upper"])
            distance = entry_price - target_price
        if distance <= 0.0:
            continue
        if not gap_unfilled_at(gap, bars_by_tf[gap["timeframe"]], entry_time):
            continue
        candidates.append({**gap, "target_price": target_price, "target_distance": distance})
    if not candidates:
        return None
    return sorted(candidates, key=lambda row: row["target_distance"])[0]


def build_stop(
    pair: str,
    direction: int,
    entry_price: float,
    ifvg: dict[str, Any],
    cfg: BacktestConfig,
) -> tuple[float, float, str]:
    multiplier = pip_multiplier(pair)
    buffer_price = cfg.stop_buffer_pips / multiplier
    if direction < 0:
        stop_price = float(ifvg["leg_extreme_price"]) + buffer_price
        stop_pips = (stop_price - entry_price) * multiplier
    else:
        stop_price = float(ifvg["leg_extreme_price"]) - buffer_price
        stop_pips = (entry_price - stop_price) * multiplier
    if stop_pips < cfg.min_stop_pips:
        return stop_price, stop_pips, "stop_too_tight"
    if stop_pips > cfg.max_stop_pips:
        return stop_price, stop_pips, "stop_too_wide"
    return stop_price, stop_pips, "ok"


def simulate_exit(
    direction: int,
    entry_time: pd.Timestamp,
    entry_price: float,
    stop_price: float,
    target_price: float,
    m1: pd.DataFrame,
    cfg: BacktestConfig,
) -> dict[str, Any]:
    end = entry_time + pd.Timedelta(minutes=cfg.max_hold_minutes)
    subset = m1[(m1.index >= entry_time) & (m1.index <= end)]
    if subset.empty:
        return {
            "exit_time": entry_time,
            "exit_price": entry_price,
            "exit_reason": "no_m1_after_entry",
        }
    for exit_time, row in subset.iterrows():
        if direction < 0:
            stop_hit = float(row["high"]) >= stop_price
            target_hit = float(row["low"]) <= target_price
        else:
            stop_hit = float(row["low"]) <= stop_price
            target_hit = float(row["high"]) >= target_price
        if stop_hit:
            return {"exit_time": exit_time, "exit_price": stop_price, "exit_reason": "stop"}
        if target_hit:
            return {"exit_time": exit_time, "exit_price": target_price, "exit_reason": "target_fvg"}
    last_time = subset.index[-1]
    return {
        "exit_time": last_time,
        "exit_price": float(subset["close"].iloc[-1]),
        "exit_reason": "max_hold",
    }


def setup_base(pair: str, event: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "pair": pair,
        "status": status,
        "event_time": event["event_time"],
        "direction": "LONG" if int(event["direction"]) > 0 else "SHORT",
        "confluence_type": event["confluence_type"],
        "htf_timeframe": event["htf_timeframe"],
        "zone_id": event["zone_id"],
        "zone_lower": event["zone_lower"],
        "zone_upper": event["zone_upper"],
        "event_high": event["event_high"],
        "event_low": event["event_low"],
        "event_close": event["event_close"],
    }


def run_pair(pair: str, cfg: BacktestConfig, since: str = "", until: str = "") -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    pair = normalize_pair(pair)
    print(f"[pair] {pair}: loading M1", flush=True)
    m1 = load_m1_ohlc(pair, since=since, until=until)
    if len(m1) < 500:
        return [], [], {"pair": pair, "error": "not_enough_m1_rows", "m1_rows": len(m1)}

    needed_timeframes = sorted(set(cfg.htf_timeframes + cfg.ifvg_timeframes + cfg.target_timeframes + (cfg.cisd_timeframe,)))
    bars_by_tf = {timeframe: resample_ohlc(m1, timeframe) for timeframe in needed_timeframes}
    cisd_bars = add_cisd_features(bars_by_tf[cfg.cisd_timeframe], cfg)
    fvg_by_tf = {}
    for timeframe in needed_timeframes:
        gaps = detect_fvgs(bars_by_tf[timeframe], timeframe, pair, cfg.min_gap_pips)
        fvg_by_tf[timeframe] = annotate_gap_fill_times(gaps, bars_by_tf[timeframe])
    ob_by_tf = {
        timeframe: detect_ob_rb_proxy_zones(bars_by_tf[timeframe], timeframe, pair, cfg)
        for timeframe in cfg.htf_timeframes
    }
    events = build_confluence_events(pair, bars_by_tf, fvg_by_tf, ob_by_tf, cfg)
    target_gaps = [
        gap
        for timeframe in cfg.target_timeframes
        for gap in fvg_by_tf.get(timeframe, [])
    ]
    print(f"[pair] {pair}: events={len(events):,} target_gaps={len(target_gaps):,}", flush=True)

    trades: list[dict[str, Any]] = []
    setups: list[dict[str, Any]] = []
    next_allowed_time = m1.index[0]
    multiplier = pip_multiplier(pair)

    for event in events:
        if event["event_time"] < next_allowed_time:
            continue
        base = setup_base(pair, event, "started")
        ifvg = find_ifvg(pair, event, bars_by_tf, fvg_by_tf, cfg)
        if ifvg is None:
            setups.append({**base, "status": "no_ifvg"})
            continue
        cisd = find_cisd(event, ifvg, cisd_bars, cfg)
        if cisd is None:
            setups.append(
                {
                    **base,
                    "status": "no_cisd",
                    "ifvg_timeframe": ifvg["ifvg_timeframe"],
                    "ifvg_formed_time": ifvg["formed_time"],
                    "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
                }
            )
            continue
        entry = find_entry(event, ifvg, cisd, m1, cfg)
        if entry is None:
            setups.append(
                {
                    **base,
                    "status": "no_ifvg_retrace_entry",
                    "ifvg_timeframe": ifvg["ifvg_timeframe"],
                    "ifvg_formed_time": ifvg["formed_time"],
                    "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
                    "cisd_time": cisd["cisd_time"],
                }
            )
            continue
        direction = int(event["direction"])
        stop_price, stop_pips, stop_status = build_stop(pair, direction, entry["entry_price"], ifvg, cfg)
        if stop_status != "ok":
            setups.append(
                {
                    **base,
                    "status": stop_status,
                    "ifvg_timeframe": ifvg["ifvg_timeframe"],
                    "ifvg_formed_time": ifvg["formed_time"],
                    "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
                    "cisd_time": cisd["cisd_time"],
                    "entry_time": entry["entry_time"],
                    "entry_price": entry["entry_price"],
                    "stop_price": stop_price,
                    "stop_pips": stop_pips,
                }
            )
            continue
        target = select_target_gap(
            direction,
            entry["entry_price"],
            entry["entry_time"],
            target_gaps,
            bars_by_tf,
            cfg,
        )
        if target is None:
            setups.append(
                {
                    **base,
                    "status": "no_unfilled_target_fvg",
                    "ifvg_timeframe": ifvg["ifvg_timeframe"],
                    "ifvg_formed_time": ifvg["formed_time"],
                    "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
                    "cisd_time": cisd["cisd_time"],
                    "entry_time": entry["entry_time"],
                    "entry_price": entry["entry_price"],
                    "stop_price": stop_price,
                    "stop_pips": stop_pips,
                }
            )
            continue
        target_pips = abs(float(target["target_price"]) - float(entry["entry_price"])) * multiplier
        rr = target_pips / max(stop_pips, 1e-9)
        if rr < cfg.min_rr:
            setups.append(
                {
                    **base,
                    "status": "target_rr_too_low",
                    "ifvg_timeframe": ifvg["ifvg_timeframe"],
                    "ifvg_formed_time": ifvg["formed_time"],
                    "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
                    "cisd_time": cisd["cisd_time"],
                    "entry_time": entry["entry_time"],
                    "entry_price": entry["entry_price"],
                    "stop_price": stop_price,
                    "stop_pips": stop_pips,
                    "target_price": target["target_price"],
                    "target_pips": target_pips,
                    "rr": rr,
                }
            )
            continue
        if cfg.max_rr > 0.0 and rr > cfg.max_rr:
            setups.append(
                {
                    **base,
                    "status": "target_rr_too_high",
                    "ifvg_timeframe": ifvg["ifvg_timeframe"],
                    "ifvg_formed_time": ifvg["formed_time"],
                    "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
                    "cisd_time": cisd["cisd_time"],
                    "entry_time": entry["entry_time"],
                    "entry_price": entry["entry_price"],
                    "stop_price": stop_price,
                    "stop_pips": stop_pips,
                    "target_price": target["target_price"],
                    "target_pips": target_pips,
                    "rr": rr,
                }
            )
            continue
        exit_info = simulate_exit(
            direction,
            entry["entry_time"],
            entry["entry_price"],
            stop_price,
            float(target["target_price"]),
            m1,
            cfg,
        )
        gross_pips = direction * (float(exit_info["exit_price"]) - float(entry["entry_price"])) * multiplier
        net_pips = gross_pips - cfg.round_turn_cost_pips
        net_r = net_pips / max(stop_pips, 1e-9)
        hold_minutes = (
            pd.Timestamp(exit_info["exit_time"]) - pd.Timestamp(entry["entry_time"])
        ).total_seconds() / 60.0
        trade = {
            **base,
            "status": "trade",
            "entry_time": entry["entry_time"],
            "exit_time": exit_info["exit_time"],
            "direction_value": direction,
            "entry_price": entry["entry_price"],
            "exit_price": exit_info["exit_price"],
            "stop_price": stop_price,
            "target_price": target["target_price"],
            "exit_reason": exit_info["exit_reason"],
            "gross_pips": gross_pips,
            "net_pips": net_pips,
            "net_r": net_r,
            "stop_pips": stop_pips,
            "target_pips": target_pips,
            "rr": rr,
            "hold_minutes": hold_minutes,
            "round_turn_cost_pips": cfg.round_turn_cost_pips,
            "ifvg_timeframe": ifvg["ifvg_timeframe"],
            "ifvg_formed_time": ifvg["formed_time"],
            "ifvg_inversion_time": ifvg["ifvg_inversion_time"],
            "ifvg_lower": ifvg["lower"],
            "ifvg_upper": ifvg["upper"],
            "ifvg_gap_pips": ifvg["gap_pips"],
            "manipulation_pips": ifvg["manipulation_pips"],
            "leg_start_time": ifvg["leg_start_time"],
            "leg_extreme_time": ifvg["leg_extreme_time"],
            "leg_extreme_price": ifvg["leg_extreme_price"],
            "cisd_time": cisd["cisd_time"],
            "cisd_close": cisd["cisd_close"],
            "cisd_structure_level": cisd["cisd_structure_level"],
            "target_timeframe": target["timeframe"],
            "target_fvg_direction": int(target["direction"]),
            "target_fvg_formed_time": target["formed_time"],
            "target_fvg_lower": target["lower"],
            "target_fvg_upper": target["upper"],
            "target_fvg_gap_pips": target["gap_pips"],
        }
        trades.append(trade)
        setups.append(trade)
        next_allowed_time = pd.Timestamp(exit_info["exit_time"]) + pd.Timedelta(minutes=cfg.cooldown_minutes)

    meta = {
        "pair": pair,
        "m1_rows": int(len(m1)),
        "m1_start_utc": m1.index.min().isoformat(),
        "m1_end_utc": m1.index.max().isoformat(),
        "event_count": int(len(events)),
        "target_gap_count": int(len(target_gaps)),
        "fvg_counts": {key: len(value) for key, value in fvg_by_tf.items()},
        "ob_rb_proxy_counts": {key: len(value) for key, value in ob_by_tf.items()},
    }
    return trades, setups, meta


def summarize_trades(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {"trade_count": 0, "score": -999999.0}
    pips = np.asarray([finite_float(row.get("net_pips")) for row in trades], dtype=float)
    r_vals = np.asarray([finite_float(row.get("net_r")) for row in trades], dtype=float)
    wins = pips[pips > 0.0]
    losses = -pips[pips < 0.0]
    equity_pips = np.cumsum(pips)
    equity_r = np.cumsum(r_vals)
    dd_pips = np.maximum.accumulate(equity_pips) - equity_pips
    dd_r = np.maximum.accumulate(equity_r) - equity_r
    return {
        "trade_count": int(len(trades)),
        "total_net_pips": float(pips.sum()),
        "avg_net_pips": float(pips.mean()),
        "median_net_pips": float(np.median(pips)),
        "win_rate": float((pips > 0.0).mean()),
        "profit_factor": float(wins.sum() / max(losses.sum(), 1e-9)),
        "total_net_r": float(r_vals.sum()),
        "avg_net_r": float(r_vals.mean()),
        "median_net_r": float(np.median(r_vals)),
        "max_drawdown_pips": float(dd_pips.max()) if dd_pips.size else 0.0,
        "max_drawdown_r": float(dd_r.max()) if dd_r.size else 0.0,
        "best_trade_pips": float(pips.max()),
        "worst_trade_pips": float(pips.min()),
        "target_rate": float(np.mean([row.get("exit_reason") == "target_fvg" for row in trades])),
        "stop_rate": float(np.mean([row.get("exit_reason") == "stop" for row in trades])),
        "avg_hold_minutes": float(np.mean([finite_float(row.get("hold_minutes")) for row in trades])),
        "score": float(r_vals.sum() - (dd_r.max() if dd_r.size else 0.0)),
    }


def group_summary(trades: Sequence[dict[str, Any]], keys: Sequence[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for trade in trades:
        key = tuple(trade.get(field, "") for field in keys)
        groups.setdefault(key, []).append(trade)
    rows: list[dict[str, Any]] = []
    for key, rows_for_key in groups.items():
        row = {field: value for field, value in zip(keys, key)}
        row.update(summarize_trades(rows_for_key))
        rows.append(row)
    return sorted(rows, key=lambda row: (str(row.get(keys[0], "")), -finite_float(row.get("total_net_r"))))


def setup_status_summary(setups: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str, str], int] = {}
    for setup in setups:
        key = (str(setup.get("pair", "")), str(setup.get("status", "")))
        counts[key] = counts.get(key, 0) + 1
    rows = [
        {"pair": pair, "status": status, "count": count}
        for (pair, status), count in sorted(counts.items())
    ]
    return rows


def format_float(value: Any, digits: int = 2) -> str:
    return f"{finite_float(value):.{digits}f}"


def markdown_report(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# ICT IFVG CISD Backtest",
        "",
        f"Generated: {payload['generated_at_utc']}",
        "",
        "## Data and assumptions",
        "",
        f"- Pairs: `{', '.join(payload['pairs'])}`.",
        f"- M1 source: `{payload['candle_root']}`.",
        f"- Date range requested: `{payload.get('since') or 'full local history'}` to `{payload.get('until') or 'latest local candle'}`.",
        f"- HTF confluence timeframes: `{', '.join(payload['config']['htf_timeframes'])}`.",
        f"- IFVG search order: `{', '.join(payload['config']['ifvg_timeframes'])}`.",
        f"- Target FVG timeframes: `{', '.join(payload['config']['target_timeframes'])}`.",
        f"- Entry: IFVG midpoint after 5m CISD proxy.",
        f"- Cost: `{format_float(payload['config']['round_turn_cost_pips'])}` pips round turn per trade.",
        "",
        "Mechanical proxies used:",
        "",
        "- HTF FVG: standard 3-candle fair-value gap, first later touch.",
        "- HTF OB/RB: last opposite candle before a displacement break of a 20-bar HTF range, first later touch.",
        "- Liquidity sweep: HTF run beyond a 20-bar high/low that closes back inside with rejection.",
        "- CISD: 5m close through a 6-bar internal high/low with body >= 0.20 ATR.",
        "",
        "## Overall result",
        "",
        f"- Trades: `{summary.get('trade_count', 0)}`",
        f"- Total net: `{format_float(summary.get('total_net_pips'))}` pips / `{format_float(summary.get('total_net_r'))}` R",
        f"- Avg trade: `{format_float(summary.get('avg_net_pips'))}` pips / `{format_float(summary.get('avg_net_r'))}` R",
        f"- Win rate: `{format_float(100.0 * finite_float(summary.get('win_rate')))}%`",
        f"- Profit factor: `{format_float(summary.get('profit_factor'))}`",
        f"- Max drawdown: `{format_float(summary.get('max_drawdown_pips'))}` pips / `{format_float(summary.get('max_drawdown_r'))}` R",
        f"- Target rate: `{format_float(100.0 * finite_float(summary.get('target_rate')))}%`",
        "",
    ]
    if payload["pair_summary"]:
        lines.extend(
            [
                "## Pair summary",
                "",
                "| pair | trades | net pips | net R | win rate | PF | max DD R |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in payload["pair_summary"]:
            lines.append(
                f"| {row['pair']} | {int(row.get('trade_count', 0))} | "
                f"{format_float(row.get('total_net_pips'))} | {format_float(row.get('total_net_r'))} | "
                f"{format_float(100.0 * finite_float(row.get('win_rate')))}% | "
                f"{format_float(row.get('profit_factor'))} | {format_float(row.get('max_drawdown_r'))} |"
            )
        lines.append("")
    if payload["confluence_summary"]:
        lines.extend(
            [
                "## Confluence summary",
                "",
                "| confluence | trades | net pips | net R | win rate | PF |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in payload["confluence_summary"]:
            lines.append(
                f"| {row['confluence_type']} | {int(row.get('trade_count', 0))} | "
                f"{format_float(row.get('total_net_pips'))} | {format_float(row.get('total_net_r'))} | "
                f"{format_float(100.0 * finite_float(row.get('win_rate')))}% | "
                f"{format_float(row.get('profit_factor'))} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Files",
            "",
            f"- JSON: `{payload['json_report']}`",
            f"- Trades CSV: `{payload['trades_csv']}`",
            f"- Setups CSV: `{payload['setups_csv']}`",
            f"- Pair summary CSV: `{payload['pair_summary_csv']}`",
            f"- Confluence summary CSV: `{payload['confluence_summary_csv']}`",
            f"- Setup status CSV: `{payload['setup_status_csv']}`",
            "",
        ]
    )
    return "\n".join(lines)


def build_config(args: argparse.Namespace) -> BacktestConfig:
    return BacktestConfig(
        htf_timeframes=parse_timeframes(args.htf_timeframes),
        ifvg_timeframes=parse_timeframes(args.ifvg_timeframes),
        target_timeframes=parse_timeframes(args.target_timeframes),
        cisd_timeframe=normalize_timeframe(args.cisd_timeframe),
        min_gap_pips=float(args.min_gap_pips),
        min_manipulation_pips=float(args.min_manipulation_pips),
        manipulation_lookback_bars=int(args.manipulation_lookback_bars),
        ifvg_wait_minutes=int(args.ifvg_wait_minutes),
        cisd_wait_minutes=int(args.cisd_wait_minutes),
        entry_wait_minutes=int(args.entry_wait_minutes),
        max_hold_minutes=int(args.max_hold_minutes),
        sweep_lookback_bars=int(args.sweep_lookback_bars),
        ob_break_lookback_bars=int(args.ob_break_lookback_bars),
        ob_search_bars=int(args.ob_search_bars),
        cisd_lookback_bars=int(args.cisd_lookback_bars),
        cisd_body_atr_mult=float(args.cisd_body_atr_mult),
        entry_zone_fraction=float(args.entry_zone_fraction),
        stop_buffer_pips=float(args.stop_buffer_pips),
        min_stop_pips=float(args.min_stop_pips),
        max_stop_pips=float(args.max_stop_pips),
        min_rr=float(args.min_rr),
        max_rr=float(args.max_rr),
        target_lookback_days=int(args.target_lookback_days),
        cooldown_minutes=int(args.cooldown_minutes),
        round_turn_cost_pips=float(args.round_turn_cost_pips),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", default="majors", help="'majors', 'all', or comma-separated pairs.")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--htf-timeframes", default="4h,1h")
    parser.add_argument("--ifvg-timeframes", default="1h,15min,5min")
    parser.add_argument("--target-timeframes", default="15min,5min")
    parser.add_argument("--cisd-timeframe", default="5min")
    parser.add_argument("--min-gap-pips", type=float, default=0.5)
    parser.add_argument("--min-manipulation-pips", type=float, default=4.0)
    parser.add_argument("--manipulation-lookback-bars", type=int, default=96)
    parser.add_argument("--ifvg-wait-minutes", type=int, default=24 * 60)
    parser.add_argument("--cisd-wait-minutes", type=int, default=12 * 60)
    parser.add_argument("--entry-wait-minutes", type=int, default=12 * 60)
    parser.add_argument("--max-hold-minutes", type=int, default=48 * 60)
    parser.add_argument("--sweep-lookback-bars", type=int, default=20)
    parser.add_argument("--ob-break-lookback-bars", type=int, default=20)
    parser.add_argument("--ob-search-bars", type=int, default=6)
    parser.add_argument("--cisd-lookback-bars", type=int, default=6)
    parser.add_argument("--cisd-body-atr-mult", type=float, default=0.20)
    parser.add_argument("--entry-zone-fraction", type=float, default=0.50)
    parser.add_argument("--stop-buffer-pips", type=float, default=1.0)
    parser.add_argument("--min-stop-pips", type=float, default=2.0)
    parser.add_argument("--max-stop-pips", type=float, default=80.0)
    parser.add_argument("--min-rr", type=float, default=0.25)
    parser.add_argument("--max-rr", type=float, default=0.0, help="Reject targets above this R multiple; 0 disables.")
    parser.add_argument("--target-lookback-days", type=int, default=30)
    parser.add_argument("--cooldown-minutes", type=int, default=120)
    parser.add_argument("--round-turn-cost-pips", type=float, default=0.8)
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = build_config(args)
    pairs = parse_pairs(args.pairs, args.max_pairs)
    if not pairs:
        raise RuntimeError(f"No requested pairs were found under {CANDLE_ROOT}")

    all_trades: list[dict[str, Any]] = []
    all_setups: list[dict[str, Any]] = []
    pair_meta: list[dict[str, Any]] = []
    for pair in pairs:
        trades, setups, meta = run_pair(pair, cfg, since=args.since, until=args.until)
        all_trades.extend(trades)
        all_setups.extend(setups)
        pair_meta.append(meta)

    pair_summary = group_summary(all_trades, ["pair"])
    confluence_summary = group_summary(all_trades, ["confluence_type"])
    ifvg_summary = group_summary(all_trades, ["ifvg_timeframe"])
    setup_status = setup_status_summary(all_setups)

    output_dir = Path(args.output_dir)
    json_path = output_dir / "latest_ict_ifvg_cisd_summary.json"
    trades_csv = output_dir / "latest_ict_ifvg_cisd_trades.csv"
    setups_csv = output_dir / "latest_ict_ifvg_cisd_setups.csv"
    pair_summary_csv = output_dir / "latest_ict_ifvg_cisd_pair_summary.csv"
    confluence_summary_csv = output_dir / "latest_ict_ifvg_cisd_confluence_summary.csv"
    ifvg_summary_csv = output_dir / "latest_ict_ifvg_cisd_ifvg_summary.csv"
    setup_status_csv = output_dir / "latest_ict_ifvg_cisd_setup_status.csv"
    summary_md = output_dir / "latest_ict_ifvg_cisd_summary.md"

    payload = {
        "generated_at_utc": utc_now(),
        "pairs": pairs,
        "since": args.since,
        "until": args.until,
        "candle_root": str(CANDLE_ROOT),
        "config": asdict(cfg),
        "pair_meta": pair_meta,
        "summary": summarize_trades(all_trades),
        "pair_summary": pair_summary,
        "confluence_summary": confluence_summary,
        "ifvg_summary": ifvg_summary,
        "setup_status_summary": setup_status,
        "json_report": str(json_path),
        "trades_csv": str(trades_csv),
        "setups_csv": str(setups_csv),
        "pair_summary_csv": str(pair_summary_csv),
        "confluence_summary_csv": str(confluence_summary_csv),
        "ifvg_summary_csv": str(ifvg_summary_csv),
        "setup_status_csv": str(setup_status_csv),
        "summary_md": str(summary_md),
    }

    write_csv(trades_csv, all_trades)
    write_csv(setups_csv, all_setups)
    write_csv(pair_summary_csv, pair_summary)
    write_csv(confluence_summary_csv, confluence_summary)
    write_csv(ifvg_summary_csv, ifvg_summary)
    write_csv(setup_status_csv, setup_status)
    atomic_write_json(json_path, payload)
    atomic_write_text(summary_md, markdown_report(payload))
    print(json.dumps(payload, indent=2, sort_keys=True, default=json_safe))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
