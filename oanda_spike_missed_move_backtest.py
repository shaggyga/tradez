#!/usr/bin/env python3
"""Backtest missed volatile spike-scout moves.

This is read-only.  It uses local event logs plus M1 candles to answer:

- how large the observed missed moves were,
- what a scout entry could have made after the move was detected,
- whether the missed opportunity came from stale detection, spread/slippage,
  model gating, canary inactivity, or repeat/campaign controls.

The key distinction is between:

- fresh_entry: entering one minute after the event window ended, i.e. what an
  earlier/fresher scanner could have attempted.
- logged_entry: entering one minute after the event log row, i.e. what the bot
  could have attempted at the actual recorded detection time.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

import oanda_gpt_training_strategy_manager as manager


PROJECT_ROOT = Path(__file__).resolve().parent
SCOUT_ROOT = PROJECT_ROOT / "data" / "technical_scout_manager" / "account_spike_scout_major_moves"
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_EVENTS = SCOUT_ROOT / "event_signals.csv"
DEFAULT_ACTIONS = SCOUT_ROOT / "actions.csv"
DEFAULT_LIFECYCLE = SCOUT_ROOT / "trade_lifecycle_ledger.csv"
DEFAULT_JSON_REPORT = REPORT_ROOT / "latest_spike_missed_move_backtest.json"
DEFAULT_CSV_REPORT = REPORT_ROOT / "latest_spike_missed_move_backtest_trades.csv"

DEFAULT_SINCE = "2026-06-21T21:00:00+00:00"
DEFAULT_HORIZONS = [5, 15, 30, 60]
RECENT_CANDLE_CACHE: Dict[Tuple[str, int, str], pd.DataFrame] = {}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except Exception:
        return default


def pips_multiplier(instrument: str) -> float:
    return 100.0 if str(instrument).upper().endswith("_JPY") else 10_000.0


def pip_size(instrument: str) -> float:
    return 1.0 / pips_multiplier(instrument)


def read_csv_rows(path: Path, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 0:
        return []
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            dt = parse_dt(row.get("time_utc"))
            if since is not None and (dt is None or dt < since):
                continue
            row = dict(row)
            row["_dt"] = dt
            out.append(row)
    return out


def load_json_text(value: Any) -> Dict[str, Any]:
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def signal_feature_flag(signal: Dict[str, Any], key: str) -> bool:
    snapshot = signal.get("_technical_feature_snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    value = signal.get(key, snapshot.get(key))
    return str(value).strip().lower() in {"1", "true", "yes"} or safe_float(value, 0.0) > 0


def is_volatile_signal(signal: Dict[str, Any]) -> bool:
    instrument = str(signal.get("instrument") or "")
    if signal_feature_flag(signal, "is_volatile_or_exotic_pair"):
        return True
    if signal_feature_flag(signal, "is_volatile_pair"):
        return True
    if signal_feature_flag(signal, "is_exotic_pair"):
        return True
    quote = instrument.split("_")[-1] if "_" in instrument else ""
    return quote in {"TRY", "ZAR", "MXN", "CZK", "HUF", "PLN", "NOK", "SEK", "HKD", "THB", "CNH"}


def accepted_trade_markers(actions_path: Path, lifecycle_path: Path, since: datetime) -> List[Dict[str, Any]]:
    markers: List[Dict[str, Any]] = []
    for row in read_csv_rows(actions_path, since):
        status = str(row.get("status") or "").strip().lower()
        action_type = str(row.get("action_type") or "").strip().lower()
        if action_type != "event_scout":
            continue
        if status not in {"accepted", "opened", "filled", "success"}:
            continue
        instrument = str(row.get("instrument") or "").strip()
        direction = str(row.get("direction") or "").strip().upper()
        if instrument and direction:
            markers.append({
                "time": row.get("_dt"),
                "instrument": instrument,
                "direction": direction,
                "source": "actions",
                "status": status,
            })
    for row in read_csv_rows(lifecycle_path, since):
        reason = str(row.get("reason") or "").strip().upper()
        if reason != "MARKET_ORDER":
            continue
        instrument = str(row.get("instrument") or "").strip()
        direction = str(row.get("direction") or "").strip().upper()
        if instrument and direction:
            markers.append({
                "time": row.get("_dt"),
                "instrument": instrument,
                "direction": direction,
                "source": "lifecycle",
                "status": "fill",
            })
    return markers


def nearest_action_reason(actions_path: Path, since: datetime) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in read_csv_rows(actions_path, since):
        instrument = str(row.get("instrument") or "").strip()
        direction = str(row.get("direction") or "").strip().upper()
        if not instrument or not direction:
            continue
        by_key[(instrument, direction)].append(row)
    return by_key


def has_near_accepted(candidate: Dict[str, Any], markers: List[Dict[str, Any]], minutes: float = 45.0) -> bool:
    entry_time = candidate.get("row_time")
    if not isinstance(entry_time, datetime):
        return False
    for marker in markers:
        if marker.get("instrument") != candidate.get("instrument"):
            continue
        if marker.get("direction") != candidate.get("direction"):
            continue
        marker_time = marker.get("time")
        if isinstance(marker_time, datetime) and abs((marker_time - entry_time).total_seconds()) <= minutes * 60:
            return True
    return False


def classify_reason(reason: str) -> str:
    text = str(reason or "").lower()
    if "no active canary" in text:
        return "canary_inactive"
    if "too old" in text or "late" in text or "continuation age" in text:
        return "stale_or_late"
    if "spread" in text or "bounds" in text or "slippage" in text:
        return "spread_bounds_slippage"
    if "model rejected" in text or "production model rejected" in text or "segment gate rejected" in text:
        return "model_or_segment_gate"
    if "approved no signals" in text:
        return "approved_no_executable_signal"
    if "repeat" in text or "campaign" in text:
        return "campaign_repeat_control"
    if "max" in text and any(token in text for token in ["risk", "margin", "trade"]):
        return "risk_or_margin_cap"
    if "new entries blocked" in text:
        return "new_entries_blocked"
    if not text:
        return "unknown"
    return "other"


def nearest_reason(candidate: Dict[str, Any], action_index: Dict[Tuple[str, str], List[Dict[str, Any]]], minutes: float = 20.0) -> str:
    rows = action_index.get((candidate["instrument"], candidate["direction"]), [])
    best: Optional[Dict[str, Any]] = None
    best_abs = float("inf")
    row_time = candidate.get("row_time")
    if not isinstance(row_time, datetime):
        return str(candidate.get("row_reason") or "")
    for row in rows:
        dt = row.get("_dt")
        if not isinstance(dt, datetime):
            continue
        diff = abs((dt - row_time).total_seconds())
        if diff <= minutes * 60 and diff < best_abs:
            best = row
            best_abs = diff
    if best:
        return str(best.get("reject_reason") or best.get("reason") or best.get("status") or "")
    return str(candidate.get("row_reason") or "")


def extract_candidates(
    *,
    events_path: Path,
    actions_path: Path,
    lifecycle_path: Path,
    since: datetime,
    min_observed_pips: float,
    only_missed: bool,
    focus_volatile: bool,
) -> List[Dict[str, Any]]:
    markers = accepted_trade_markers(actions_path, lifecycle_path, since)
    action_index = nearest_action_reason(actions_path, since)
    candidates: List[Dict[str, Any]] = []
    seen = set()
    for row in read_csv_rows(events_path, since):
        payload = load_json_text(row.get("raw_json"))
        signals = payload.get("signals") if isinstance(payload.get("signals"), list) else []
        for signal in signals:
            if not isinstance(signal, dict):
                continue
            instrument = str(signal.get("instrument") or "").strip()
            direction = str(signal.get("direction") or "").strip().upper()
            if not instrument or direction not in {"LONG", "SHORT"}:
                continue
            observed_pips = abs(safe_float(signal.get("net_pips", signal.get("abs_move_pips")), 0.0))
            if observed_pips < min_observed_pips:
                continue
            if focus_volatile and not is_volatile_signal(signal):
                continue
            event_key = str(signal.get("event_key") or "")
            start_utc = str(signal.get("start_utc") or "")
            end_utc = str(signal.get("end_utc") or "")
            key = (
                instrument,
                direction,
                str(signal.get("theme") or row.get("theme") or ""),
                event_key or start_utc,
                end_utc,
                round(observed_pips / 10.0) * 10,
            )
            if key in seen:
                continue
            candidate = {
                "row_time": row.get("_dt"),
                "row_time_utc": row.get("time_utc", ""),
                "instrument": instrument,
                "direction": direction,
                "theme": str(signal.get("theme") or row.get("theme") or ""),
                "event_key": event_key,
                "event_start_utc": start_utc,
                "event_end_utc": end_utc,
                "event_end_time": parse_dt(end_utc),
                "window_minutes": safe_int(signal.get("window_minutes"), safe_int(row.get("best_window_minutes"), 0)),
                "observed_net_pips": safe_float(signal.get("net_pips"), safe_float(row.get("best_net_pips"), 0.0)),
                "observed_abs_pips": observed_pips,
                "cost_adjusted_net_pips": safe_float(signal.get("cost_adjusted_net_pips"), 0.0),
                "move_to_spread_ratio": safe_float(signal.get("move_to_spread_ratio"), 0.0),
                "spread_avg_pips": safe_float(signal.get("spread_avg_pips"), safe_float(row.get("best_spread_pips"), 0.0)),
                "event_age_minutes": safe_float(signal.get("event_age_minutes"), 0.0),
                "event_trigger_count": safe_int(signal.get("event_trigger_count"), 0),
                "row_status": row.get("status", ""),
                "row_reason": row.get("reason", ""),
            }
            if only_missed and has_near_accepted(candidate, markers):
                continue
            reason = nearest_reason(candidate, action_index)
            candidate["miss_reason"] = reason[:500]
            candidate["miss_category"] = classify_reason(reason)
            seen.add(key)
            candidates.append(candidate)
    candidates.sort(key=lambda item: item["observed_abs_pips"], reverse=True)
    return candidates


def candle_rows_from_oanda(candles: List[Dict[str, Any]], instrument: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for candle in candles:
        if not candle.get("complete", True):
            continue
        mid = candle.get("mid") or {}
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        time = parse_dt(candle.get("time"))
        if time is None:
            continue
        row = {
            "time": time,
            "instrument": instrument,
            "open": safe_float(mid.get("o")),
            "high": safe_float(mid.get("h")),
            "low": safe_float(mid.get("l")),
            "close": safe_float(mid.get("c")),
            "bid_open": safe_float(bid.get("o"), float("nan")),
            "bid_high": safe_float(bid.get("h"), float("nan")),
            "bid_low": safe_float(bid.get("l"), float("nan")),
            "bid_close": safe_float(bid.get("c"), float("nan")),
            "ask_open": safe_float(ask.get("o"), float("nan")),
            "ask_high": safe_float(ask.get("h"), float("nan")),
            "ask_low": safe_float(ask.get("l"), float("nan")),
            "ask_close": safe_float(ask.get("c"), float("nan")),
        }
        rows.append(row)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values("time").reset_index(drop=True)
    return frame


def load_local_candles(instrument: str) -> pd.DataFrame:
    path = manager.DIRS["candles"] / f"{instrument}_M1.csv"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    time_col = "datetime" if "datetime" in frame.columns else "time"
    frame["time"] = pd.to_datetime(frame[time_col], utc=True, errors="coerce")
    frame = frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    return frame


def oanda_client() -> manager.OandaClient:
    credentials = manager.load_creds(manager.CREDS_PATH)
    token, base_url, account_id = manager.resolve_oanda_creds(credentials)
    return manager.OandaClient(token, base_url, account_id)


def fetch_recent_candles(instruments: Iterable[str], *, count: int = 5000, price: str = "BAM") -> Dict[str, pd.DataFrame]:
    client = oanda_client()
    out: Dict[str, pd.DataFrame] = {}
    for instrument in sorted(set(instruments)):
        cache_key = (instrument, int(count), str(price))
        cached = RECENT_CANDLE_CACHE.get(cache_key)
        if cached is not None:
            out[instrument] = cached.copy()
            continue
        data = client.candles(instrument, granularity="M1", count=count, price=price)
        if data.get("_error"):
            frame = pd.DataFrame()
            RECENT_CANDLE_CACHE[cache_key] = frame
            out[instrument] = frame.copy()
            continue
        frame = candle_rows_from_oanda(data.get("candles", []) or [], instrument)
        RECENT_CANDLE_CACHE[cache_key] = frame
        out[instrument] = frame.copy()
    return out


def merged_candles(instruments: Iterable[str], *, fetch_recent: bool, count: int) -> Dict[str, pd.DataFrame]:
    unique_instruments = sorted(set(instruments))
    frames: Dict[str, pd.DataFrame] = {} if fetch_recent else {
        instrument: load_local_candles(instrument) for instrument in unique_instruments
    }
    if fetch_recent:
        recent = fetch_recent_candles(unique_instruments, count=count)
        for instrument, frame in recent.items():
            if frame.empty:
                frames[instrument] = load_local_candles(instrument)
                continue
            prior = frames.get(instrument, pd.DataFrame())
            if prior.empty:
                frames[instrument] = frame
            else:
                frames[instrument] = (
                    pd.concat([prior, frame], ignore_index=True)
                    .drop_duplicates(subset=["time"], keep="last")
                    .sort_values("time")
                    .reset_index(drop=True)
                )
    return frames


def price_available(value: Any) -> bool:
    try:
        number = float(value)
        return math.isfinite(number) and number > 0
    except Exception:
        return False


def row_price(row: pd.Series, field: str, fallback: str) -> float:
    value = row.get(field)
    if price_available(value):
        return float(value)
    return safe_float(row.get(fallback), 0.0)


def trail_label(multiplier: float) -> str:
    text = f"{float(multiplier):.2f}".rstrip("0").rstrip(".")
    return "s" + text.replace(".", "p")


def trailing_exit_pips(
    *,
    instrument: str,
    direction: str,
    entry: float,
    horizon_frame: pd.DataFrame,
    trail_pips: float,
    activation_pips: float,
) -> float:
    """Conservative candle-order trailing-stop proxy.

    The stop uses the prior best excursion before each candle's new high/low is
    credited.  That avoids assuming the favorable side of an M1 candle happened
    before the adverse side.
    """
    if horizon_frame.empty or entry <= 0:
        return 0.0
    multiplier = pips_multiplier(instrument)
    trail_price = max(float(trail_pips), 0.0) / multiplier
    activation = max(float(activation_pips), 0.0)
    active = False
    if direction == "LONG":
        best = entry
        for _, row in horizon_frame.iterrows():
            low = row_price(row, "bid_low", "low")
            high = row_price(row, "bid_high", "high")
            if active:
                stop_price = best - trail_price
                if low <= stop_price:
                    return (stop_price - entry) * multiplier
            if high > best:
                best = high
            if not active and (best - entry) * multiplier >= activation:
                active = True
        final = row_price(horizon_frame.iloc[-1], "bid_close", "close")
        return (final - entry) * multiplier
    best = entry
    for _, row in horizon_frame.iterrows():
        high = row_price(row, "ask_high", "high")
        low = row_price(row, "ask_low", "low")
        if active:
            stop_price = best + trail_price
            if high >= stop_price:
                return (entry - stop_price) * multiplier
        if low < best:
            best = low
        if not active and (entry - best) * multiplier >= activation:
            active = True
    final = row_price(horizon_frame.iloc[-1], "ask_close", "close")
    return (entry - final) * multiplier


def simulate_entry(
    candidate: Dict[str, Any],
    frame: pd.DataFrame,
    *,
    mode: str,
    horizons: List[int],
    entry_delay_minutes: int,
    quote_to_usd: float,
    units: float,
    trail_spread_multipliers: List[float],
    trail_min_pips: float,
) -> Dict[str, Any]:
    if frame.empty:
        return {"available": False, "reason": "no candle data"}
    base_time = candidate.get("event_end_time") if mode == "fresh" else candidate.get("row_time")
    if not isinstance(base_time, datetime):
        return {"available": False, "reason": f"missing {mode} base time"}
    entry_after = pd.Timestamp(base_time + timedelta(minutes=entry_delay_minutes))
    candle_times = frame["time"]
    future = frame[candle_times >= entry_after]
    if future.empty:
        return {
            "available": False,
            "reason": "no candles after entry time",
            "entry_after_utc": entry_after.isoformat(),
            "data_min_utc": frame["time"].min().isoformat() if not frame.empty else "",
            "data_max_utc": frame["time"].max().isoformat() if not frame.empty else "",
        }
    entry_row = future.iloc[0]
    direction = candidate["direction"]
    multiplier = pips_multiplier(candidate["instrument"])
    psize = pip_size(candidate["instrument"])
    if direction == "LONG":
        entry = row_price(entry_row, "ask_close", "close")
    else:
        entry = row_price(entry_row, "bid_close", "close")
    result: Dict[str, Any] = {
        "available": True,
        "mode": mode,
        "entry_time_utc": entry_row["time"].isoformat(),
        "entry_price": entry,
    }
    for horizon in horizons:
        end_time = entry_row["time"] + pd.Timedelta(minutes=int(horizon))
        horizon_frame = frame[(frame["time"] >= entry_row["time"]) & (frame["time"] <= end_time)]
        if horizon_frame.empty:
            continue
        last = horizon_frame.iloc[-1]
        if direction == "LONG":
            final = row_price(last, "bid_close", "close")
            high = max(row_price(row, "bid_high", "high") for _, row in horizon_frame.iterrows())
            low = min(row_price(row, "bid_low", "low") for _, row in horizon_frame.iterrows())
            final_pips = (final - entry) * multiplier
            mfe_pips = (high - entry) * multiplier
            mae_pips = (low - entry) * multiplier
        else:
            final = row_price(last, "ask_close", "close")
            low = min(row_price(row, "ask_low", "low") for _, row in horizon_frame.iterrows())
            high = max(row_price(row, "ask_high", "high") for _, row in horizon_frame.iterrows())
            final_pips = (entry - final) * multiplier
            mfe_pips = (entry - low) * multiplier
            mae_pips = (entry - high) * multiplier
        result[f"final_{horizon}m_pips"] = final_pips
        result[f"mfe_{horizon}m_pips"] = mfe_pips
        result[f"mae_{horizon}m_pips"] = mae_pips
        result[f"final_{horizon}m_usd"] = final_pips * psize * units * quote_to_usd
        result[f"mfe_{horizon}m_usd"] = mfe_pips * psize * units * quote_to_usd
        result[f"mae_{horizon}m_usd"] = mae_pips * psize * units * quote_to_usd
        spread_ref = max(
            abs(safe_float(candidate.get("spread_avg_pips"), 0.0)),
            abs(safe_float(candidate.get("spread_pips"), 0.0)),
            1.0,
        )
        for trail_mult in trail_spread_multipliers:
            label = trail_label(trail_mult)
            trail_pips = max(float(trail_min_pips), spread_ref * float(trail_mult))
            activation_pips = trail_pips
            trail_outcome = trailing_exit_pips(
                instrument=candidate["instrument"],
                direction=direction,
                entry=entry,
                horizon_frame=horizon_frame,
                trail_pips=trail_pips,
                activation_pips=activation_pips,
            )
            result[f"trail_{label}_{horizon}m_pips"] = trail_outcome
            result[f"trail_{label}_{horizon}m_usd"] = (
                trail_outcome * psize * units * quote_to_usd
            )
    return result


def median_units_by_instrument(actions_path: Path, since: datetime) -> Dict[str, float]:
    by_inst: Dict[str, List[float]] = defaultdict(list)
    for row in read_csv_rows(actions_path, since):
        if str(row.get("action_type") or "").lower() != "event_scout":
            continue
        instrument = str(row.get("instrument") or "").strip()
        units = abs(safe_float(row.get("units"), 0.0))
        if instrument and units > 0:
            by_inst[instrument].append(units)
    return {instrument: float(median(values)) for instrument, values in by_inst.items() if values}


def pricing_mid_map(instruments: Iterable[str]) -> Dict[str, float]:
    client = oanda_client()
    prices = client.pricing(sorted(set(instruments)))
    out: Dict[str, float] = {}
    for row in prices.get("prices", []) or []:
        instrument = str(row.get("instrument") or "")
        bids = row.get("bids") or []
        asks = row.get("asks") or []
        if not instrument or not bids or not asks:
            continue
        bid = safe_float(bids[0].get("price"), 0.0)
        ask = safe_float(asks[0].get("price"), 0.0)
        if bid > 0 and ask > 0:
            out[instrument] = (bid + ask) / 2.0
    return out


def conversion_instruments(candidates: Iterable[Dict[str, Any]]) -> List[str]:
    needed = set()
    direct_usd_quotes = {"AUD", "EUR", "GBP", "NZD"}
    for candidate in candidates:
        instrument = str(candidate.get("instrument") or "")
        if "_" not in instrument:
            continue
        quote = instrument.split("_", 1)[1]
        if quote == "USD":
            continue
        if quote in direct_usd_quotes:
            needed.add(f"{quote}_USD")
        else:
            needed.add(f"USD_{quote}")
    return sorted(needed)


def quote_to_usd_map(candidates: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    candidates = list(candidates)
    prices = pricing_mid_map(conversion_instruments(candidates))
    out: Dict[str, float] = {"USD": 1.0}
    for candidate in candidates:
        instrument = str(candidate.get("instrument") or "")
        if "_" not in instrument:
            continue
        quote = instrument.split("_", 1)[1]
        if quote in out:
            continue
        direct = prices.get(f"{quote}_USD")
        inverse = prices.get(f"USD_{quote}")
        if direct and direct > 0:
            out[quote] = direct
        elif inverse and inverse > 0:
            out[quote] = 1.0 / inverse
        else:
            out[quote] = 1.0
    return out


def summarize(values: List[float]) -> Dict[str, Any]:
    values = [float(value) for value in values if math.isfinite(float(value))]
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(values),
        "sum": sum(values),
        "mean": sum(values) / len(values),
        "median": ordered[len(ordered) // 2],
        "positive_count": sum(1 for value in values if value > 0),
        "positive_rate": sum(1 for value in values if value > 0) / len(values),
        "min": min(values),
        "max": max(values),
    }


def missed_move_clusters(
    rows: List[Dict[str, Any]],
    *,
    gap_minutes: int,
) -> List[List[Dict[str, Any]]]:
    """Cluster repeated alerts so a single spike is not counted as many trades."""
    clusters: List[List[Dict[str, Any]]] = []
    keyed_rows: Dict[Tuple[str, str], List[Tuple[datetime, Dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        row_time = parse_dt(row.get("row_time_utc"))
        instrument = str(row.get("instrument") or "")
        direction = str(row.get("direction") or "")
        if row_time is None or not instrument or not direction:
            continue
        keyed_rows[(instrument, direction)].append((row_time, row))
    for _, group in sorted(keyed_rows.items()):
        current: List[Tuple[datetime, Dict[str, Any]]] = []
        for row_time, row in sorted(group, key=lambda item: item[0]):
            if (
                not current
                or (row_time - current[-1][0]).total_seconds() / 60.0
                <= float(gap_minutes)
            ):
                current.append((row_time, row))
                continue
            clusters.append([item[1] for item in current])
            current = [(row_time, row)]
        if current:
            clusters.append([item[1] for item in current])
    return clusters


def clustered_move_summary(
    rows: List[Dict[str, Any]],
    *,
    gap_minutes: int,
) -> Dict[str, Any]:
    clusters = missed_move_clusters(rows, gap_minutes=gap_minutes)
    if not clusters:
        return {"cluster_count": 0, "raw_row_count": len(rows), "gap_minutes": gap_minutes}
    representatives: List[Dict[str, Any]] = []
    for cluster in clusters:
        first = cluster[0]
        last = cluster[-1]
        max_observed = max(
            cluster,
            key=lambda row: safe_float(row.get("observed_abs_pips"), -1e18),
        )
        best_fresh_30 = max(
            cluster,
            key=lambda row: safe_float(row.get("fresh_final_30m_pips"), -1e18),
        )
        representatives.append({
            "instrument": first.get("instrument"),
            "direction": first.get("direction"),
            "row_count": len(cluster),
            "start_utc": first.get("row_time_utc"),
            "end_utc": last.get("row_time_utc"),
            "miss_categories": sorted({str(row.get("miss_category") or "") for row in cluster}),
            "max_observed_abs_pips": safe_float(max_observed.get("observed_abs_pips"), 0.0),
            "max_observed_abs_usd": safe_float(max_observed.get("observed_abs_usd"), 0.0),
            "first_alert_fresh_30m_pips": safe_float(first.get("fresh_final_30m_pips"), 0.0),
            "first_alert_fresh_30m_usd": safe_float(first.get("fresh_final_30m_usd"), 0.0),
            "first_alert_fresh_60m_pips": safe_float(first.get("fresh_final_60m_pips"), 0.0),
            "first_alert_fresh_60m_usd": safe_float(first.get("fresh_final_60m_usd"), 0.0),
            "first_alert_mfe_60m_pips": safe_float(first.get("fresh_mfe_60m_pips"), 0.0),
            "first_alert_mfe_60m_usd": safe_float(first.get("fresh_mfe_60m_usd"), 0.0),
            "best_alert_fresh_30m_pips": safe_float(best_fresh_30.get("fresh_final_30m_pips"), 0.0),
            "best_alert_fresh_30m_usd": safe_float(best_fresh_30.get("fresh_final_30m_usd"), 0.0),
        })
    trail_pip_columns = sorted({
        key
        for row in rows
        for key in row
        if key.startswith("fresh_trail_") and key.endswith("m_pips")
    })
    trail_usd_columns = sorted({
        key
        for row in rows
        for key in row
        if key.startswith("fresh_trail_") and key.endswith("m_usd")
    })
    trail_summaries: Dict[str, Any] = {}
    for col in [*trail_pip_columns, *trail_usd_columns]:
        first_values: List[float] = []
        best_values: List[float] = []
        for cluster in clusters:
            first_values.append(safe_float(cluster[0].get(col), float("nan")))
            best = max(cluster, key=lambda row: safe_float(row.get(col), -1e18))
            best_values.append(safe_float(best.get(col), float("nan")))
        trail_summaries[f"first_alert_{col}"] = summarize(first_values)
        trail_summaries[f"best_alert_{col}"] = summarize(best_values)
    return {
        "gap_minutes": gap_minutes,
        "raw_row_count": len(rows),
        "cluster_count": len(representatives),
        "observed_abs_pips_one_per_cluster": summarize([
            row["max_observed_abs_pips"] for row in representatives
        ]),
        "observed_abs_usd_one_per_cluster": summarize([
            row["max_observed_abs_usd"] for row in representatives
        ]),
        "first_alert_fresh_30m_pips": summarize([
            row["first_alert_fresh_30m_pips"] for row in representatives
        ]),
        "first_alert_fresh_30m_usd": summarize([
            row["first_alert_fresh_30m_usd"] for row in representatives
        ]),
        "first_alert_fresh_60m_pips": summarize([
            row["first_alert_fresh_60m_pips"] for row in representatives
        ]),
        "first_alert_fresh_60m_usd": summarize([
            row["first_alert_fresh_60m_usd"] for row in representatives
        ]),
        "first_alert_mfe_60m_pips": summarize([
            row["first_alert_mfe_60m_pips"] for row in representatives
        ]),
        "first_alert_mfe_60m_usd": summarize([
            row["first_alert_mfe_60m_usd"] for row in representatives
        ]),
        "best_alert_fresh_30m_pips": summarize([
            row["best_alert_fresh_30m_pips"] for row in representatives
        ]),
        "best_alert_fresh_30m_usd": summarize([
            row["best_alert_fresh_30m_usd"] for row in representatives
        ]),
        "top_clusters_by_observed_usd": sorted(
            representatives,
            key=lambda row: row["max_observed_abs_usd"],
            reverse=True,
        )[:25],
        "trailing_proxy_summary": trail_summaries,
    }


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def write_detail_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row.keys() if key not in {"row_time", "event_end_time"}})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_report(args: argparse.Namespace) -> Dict[str, Any]:
    since = parse_dt(args.since)
    if since is None:
        raise SystemExit(f"invalid --since {args.since!r}")
    horizons = [safe_int(value) for value in args.horizons]
    candidates = extract_candidates(
        events_path=args.events,
        actions_path=args.actions,
        lifecycle_path=args.lifecycle,
        since=since,
        min_observed_pips=args.min_observed_pips,
        only_missed=not args.include_traded,
        focus_volatile=not args.include_non_volatile,
    )
    if args.max_events > 0:
        candidates = candidates[: args.max_events]
    instruments = sorted({candidate["instrument"] for candidate in candidates})
    candles = merged_candles(instruments, fetch_recent=args.fetch_recent, count=args.recent_count)
    units_map = median_units_by_instrument(args.actions, since)
    quote_map = quote_to_usd_map(candidates) if args.estimate_usd else {}
    detail_rows: List[Dict[str, Any]] = []
    for candidate in candidates:
        instrument = candidate["instrument"]
        quote = instrument.split("_", 1)[1] if "_" in instrument else "USD"
        units = units_map.get(instrument, args.default_units)
        quote_to_usd = quote_map.get(quote, 1.0)
        observed_abs_pips = safe_float(candidate.get("observed_abs_pips"), 0.0)
        observed_net_pips = safe_float(candidate.get("observed_net_pips"), 0.0)
        row = {
            **{
                key: value for key, value in candidate.items()
                if key not in {"row_time", "event_end_time"}
            },
            "scout_units_assumption": units,
            "quote_to_usd": quote_to_usd,
            "observed_abs_usd": observed_abs_pips * pip_size(instrument) * units * quote_to_usd,
            "observed_net_usd": observed_net_pips * pip_size(instrument) * units * quote_to_usd,
        }
        for mode in ["fresh", "logged"]:
            sim = simulate_entry(
                candidate,
                candles.get(instrument, pd.DataFrame()),
                mode=mode,
                horizons=horizons,
                entry_delay_minutes=args.entry_delay_minutes,
                quote_to_usd=quote_to_usd,
                units=units,
                trail_spread_multipliers=[
                    safe_float(value)
                    for value in args.trail_spread_multipliers
                    if safe_float(value) > 0
                ],
                trail_min_pips=args.trail_min_pips,
            )
            row[f"{mode}_available"] = sim.get("available", False)
            if not sim.get("available", False):
                row[f"{mode}_reason"] = sim.get("reason", "")
                row[f"{mode}_data_max_utc"] = sim.get("data_max_utc", "")
            else:
                row[f"{mode}_entry_time_utc"] = sim.get("entry_time_utc", "")
                row[f"{mode}_entry_price"] = sim.get("entry_price", "")
                for key, value in sim.items():
                    if key.endswith("_pips") or key.endswith("_usd"):
                        row[f"{mode}_{key}"] = value
        detail_rows.append(row)
    grouped: Dict[str, Any] = {}
    trail_labels = [
        trail_label(safe_float(value))
        for value in args.trail_spread_multipliers
        if safe_float(value) > 0
    ]
    for mode in ["fresh", "logged"]:
        grouped[mode] = {}
        for horizon in horizons:
            key = f"{mode}_final_{horizon}m_pips"
            mfe_key = f"{mode}_mfe_{horizon}m_pips"
            usd_key = f"{mode}_final_{horizon}m_usd"
            mfe_usd_key = f"{mode}_mfe_{horizon}m_usd"
            grouped[mode][f"final_{horizon}m_pips"] = summarize([
                safe_float(row.get(key), float("nan")) for row in detail_rows if key in row
            ])
            grouped[mode][f"mfe_{horizon}m_pips"] = summarize([
                safe_float(row.get(mfe_key), float("nan")) for row in detail_rows if mfe_key in row
            ])
            grouped[mode][f"final_{horizon}m_usd"] = summarize([
                safe_float(row.get(usd_key), float("nan")) for row in detail_rows if usd_key in row
            ])
            grouped[mode][f"mfe_{horizon}m_usd"] = summarize([
                safe_float(row.get(mfe_usd_key), float("nan")) for row in detail_rows if mfe_usd_key in row
            ])
            for label in trail_labels:
                trail_key = f"{mode}_trail_{label}_{horizon}m_pips"
                trail_usd_key = f"{mode}_trail_{label}_{horizon}m_usd"
                grouped[mode][f"trail_{label}_{horizon}m_pips"] = summarize([
                    safe_float(row.get(trail_key), float("nan"))
                    for row in detail_rows
                    if trail_key in row
                ])
                grouped[mode][f"trail_{label}_{horizon}m_usd"] = summarize([
                    safe_float(row.get(trail_usd_key), float("nan"))
                    for row in detail_rows
                    if trail_usd_key in row
                ])
    by_category = Counter(row.get("miss_category", "") for row in detail_rows)
    by_instrument = Counter(row.get("instrument", "") for row in detail_rows)
    clustered = {
        f"{gap}m_gap": clustered_move_summary(detail_rows, gap_minutes=gap)
        for gap in [15, 30, 60, 120]
    }
    top_by_fresh_mfe_60 = sorted(
        detail_rows,
        key=lambda row: safe_float(row.get("fresh_mfe_60m_pips"), -1e18),
        reverse=True,
    )[:25]
    top_by_fresh_final_30 = sorted(
        detail_rows,
        key=lambda row: safe_float(row.get("fresh_final_30m_pips"), -1e18),
        reverse=True,
    )[:25]
    report = {
        "generated_utc": utc_iso(),
        "events_path": str(args.events),
        "actions_path": str(args.actions),
        "since_utc": since.isoformat(),
        "candidate_count": len(candidates),
        "instrument_count": len(instruments),
        "min_observed_pips": args.min_observed_pips,
        "entry_delay_minutes": args.entry_delay_minutes,
        "horizons": horizons,
        "fetch_recent": bool(args.fetch_recent),
        "recent_count": args.recent_count,
        "assumptions": {
            "fresh_entry": "enter after event window end plus entry_delay_minutes",
            "logged_entry": "enter after actual event log row plus entry_delay_minutes",
            "pnl": "bid/ask candles are used when OANDA returns BAM candles; otherwise mid prices are used",
            "usd_estimate": "uses median observed scout units by instrument, with default fallback units",
            "observed_abs_usd": "oracle value of the logged move size at the scout unit assumption; repeated alerts can overlap, so clustered summaries are safer than raw sums",
            "clustered_move_summary": "one representative per instrument/direction cluster; max observed move estimates opportunity, first-alert fields estimate tradable outcome after first detection",
            "trailing_proxy": "trail_sXpY fields activate after X.Y * observed spread pips, then trail by the same distance with a minimum pips floor; candle ordering is conservative",
            "not_included": "financing, margin liquidation, rejected-order retry delay, and market-impact simulation",
        },
        "missed_categories": dict(by_category.most_common()),
        "missed_by_instrument": dict(by_instrument.most_common(25)),
        "summary": grouped,
        "observed_move_value": {
            "raw_observed_abs_pips": summarize([
                safe_float(row.get("observed_abs_pips"), float("nan")) for row in detail_rows
            ]),
            "raw_observed_abs_usd": summarize([
                safe_float(row.get("observed_abs_usd"), float("nan")) for row in detail_rows
            ]),
        },
        "clustered_move_summary": clustered,
        "top_by_fresh_mfe_60m": [
            {
                key: row.get(key)
                for key in [
                    "row_time_utc", "instrument", "direction", "theme",
                    "observed_abs_pips", "cost_adjusted_net_pips",
                    "observed_abs_usd", "move_to_spread_ratio", "miss_category",
                    "fresh_final_30m_pips", "fresh_mfe_30m_pips",
                    "fresh_final_60m_pips", "fresh_mfe_60m_pips",
                    "fresh_trail_s1p5_60m_pips", "fresh_trail_s2p5_60m_pips",
                    "fresh_final_60m_usd", "fresh_mfe_60m_usd",
                    "logged_final_30m_pips", "logged_mfe_30m_pips",
                ]
            }
            for row in top_by_fresh_mfe_60
        ],
        "top_by_fresh_final_30m": [
            {
                key: row.get(key)
                for key in [
                    "row_time_utc", "instrument", "direction", "theme",
                    "observed_abs_pips", "cost_adjusted_net_pips",
                    "observed_abs_usd", "move_to_spread_ratio", "miss_category",
                    "fresh_final_30m_pips", "fresh_mfe_30m_pips",
                    "fresh_trail_s1p5_30m_pips", "fresh_trail_s2p5_30m_pips",
                    "fresh_final_30m_usd", "fresh_mfe_30m_usd",
                    "logged_final_30m_pips", "logged_mfe_30m_pips",
                ]
            }
            for row in top_by_fresh_final_30
        ],
        "detail_csv": str(args.csv_report),
    }
    write_detail_csv(args.csv_report, detail_rows)
    atomic_write_json(args.report, report)
    return report


def delay_variant_path(path: Path, delay_minutes: int) -> Path:
    """Return a sibling path for a per-entry-delay artifact."""
    return path.with_name(f"{path.stem}_delay{int(delay_minutes)}m{path.suffix}")


def _summary_stat(report: Dict[str, Any], mode: str, metric: str, field: str) -> float:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    mode_summary = summary.get(mode) if isinstance(summary.get(mode), dict) else {}
    stat = mode_summary.get(metric) if isinstance(mode_summary.get(metric), dict) else {}
    value = stat.get(field)
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return float("nan")


def _delay_comparison_row(report: Dict[str, Any]) -> Dict[str, Any]:
    delay = safe_int(report.get("entry_delay_minutes"), 0)
    row: Dict[str, Any] = {
        "entry_delay_minutes": delay,
        "report": report.get("report_path", ""),
        "detail_csv": report.get("detail_csv", ""),
        "candidate_count": report.get("candidate_count", 0),
        "instrument_count": report.get("instrument_count", 0),
    }
    for mode in ("fresh", "logged"):
        for metric in (
            "final_15m_pips",
            "final_30m_pips",
            "final_60m_pips",
            "final_120m_pips",
            "mfe_30m_pips",
            "mfe_60m_pips",
            "mfe_120m_pips",
            "final_30m_usd",
            "final_60m_usd",
            "mfe_60m_usd",
            "mfe_120m_usd",
        ):
            prefix = f"{mode}_{metric}"
            row[f"{prefix}_mean"] = _summary_stat(report, mode, metric, "mean")
            row[f"{prefix}_sum"] = _summary_stat(report, mode, metric, "sum")
            row[f"{prefix}_positive_rate"] = _summary_stat(report, mode, metric, "positive_rate")
    return row


def _best_delay(
    comparison_rows: List[Dict[str, Any]],
    metric: str,
    fallback_metric: str | None = None,
) -> Dict[str, Any]:
    candidates = []
    for row in comparison_rows:
        value = safe_float(row.get(metric), float("nan"))
        if not math.isfinite(value) and fallback_metric:
            value = safe_float(row.get(fallback_metric), float("nan"))
        if math.isfinite(value):
            candidates.append((value, row))
    if not candidates:
        return {"metric": metric, "delay_minutes": "", "value": ""}
    value, row = max(candidates, key=lambda item: item[0])
    return {
        "metric": metric,
        "fallback_metric": fallback_metric or "",
        "delay_minutes": row.get("entry_delay_minutes", ""),
        "value": value,
    }


def build_multi_delay_report(args: argparse.Namespace, delays: List[int]) -> Dict[str, Any]:
    """Run the replay for several entry delays and write a comparison report.

    Per-delay runs keep the normal full report/detail schema.  The aggregate
    report copies the best-delay report's normal top-level fields so existing
    monitor summarizers remain compatible, then adds delay_comparison metadata.
    """
    reports: List[Dict[str, Any]] = []
    original_report = args.report
    original_csv = args.csv_report
    for delay in delays:
        print(
            f"[delay_compare] start delay={delay}m "
            f"max_events={args.max_events} horizons={','.join(str(value) for value in args.horizons)}",
            flush=True,
        )
        sub_args = argparse.Namespace(**vars(args))
        sub_args.entry_delay_minutes = delay
        sub_args.report = delay_variant_path(original_report, delay)
        sub_args.csv_report = delay_variant_path(original_csv, delay)
        report = build_report(sub_args)
        report["report_path"] = str(sub_args.report)
        reports.append(report)
        print(
            f"[delay_compare] done delay={delay}m candidates={report.get('candidate_count')} "
            f"report={sub_args.report}",
            flush=True,
        )

    comparison_rows = [_delay_comparison_row(report) for report in reports]
    best_delay_by = {
        "fresh_final_30m_usd_sum": _best_delay(
            comparison_rows,
            "fresh_final_30m_usd_sum",
            "fresh_final_30m_pips_sum",
        ),
        "fresh_final_60m_usd_sum": _best_delay(
            comparison_rows,
            "fresh_final_60m_usd_sum",
            "fresh_final_60m_pips_sum",
        ),
        "fresh_mfe_60m_usd_sum": _best_delay(
            comparison_rows,
            "fresh_mfe_60m_usd_sum",
            "fresh_mfe_60m_pips_sum",
        ),
        "logged_final_30m_usd_sum": _best_delay(
            comparison_rows,
            "logged_final_30m_usd_sum",
            "logged_final_30m_pips_sum",
        ),
    }
    primary_best = best_delay_by["fresh_mfe_60m_usd_sum"]
    primary_delay = safe_int(primary_best.get("delay_minutes"), delays[0])
    best_report = next(
        (report for report in reports if safe_int(report.get("entry_delay_minutes"), -1) == primary_delay),
        reports[0],
    )
    aggregate = dict(best_report)
    aggregate.update(
        {
            "generated_utc": utc_iso(),
            "entry_delay_minutes": delays,
            "best_delay_minutes": primary_delay,
            "best_delay_selection": primary_best,
            "delay_comparison": comparison_rows,
            "best_delay_by": best_delay_by,
            "per_delay_reports": [
                {
                    "entry_delay_minutes": report.get("entry_delay_minutes"),
                    "report": report.get("report_path"),
                    "detail_csv": report.get("detail_csv"),
                }
                for report in reports
            ],
            "best_delay_detail_csv": best_report.get("detail_csv", ""),
            "detail_csv": str(original_csv),
            "comparison_csv": str(original_csv),
        }
    )
    write_detail_csv(original_csv, comparison_rows)
    atomic_write_json(original_report, aggregate)
    print(
        f"[delay_compare] wrote aggregate best_delay={primary_delay}m "
        f"report={original_report} comparison_csv={original_csv}",
        flush=True,
    )
    return aggregate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--actions", type=Path, default=DEFAULT_ACTIONS)
    parser.add_argument("--lifecycle", type=Path, default=DEFAULT_LIFECYCLE)
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--min-observed-pips", type=float, default=100.0)
    parser.add_argument("--horizons", nargs="+", default=[str(x) for x in DEFAULT_HORIZONS])
    parser.add_argument("--entry-delay-minutes", nargs="+", type=int, default=[1])
    parser.add_argument("--include-traded", action="store_true")
    parser.add_argument("--include-non-volatile", action="store_true")
    parser.add_argument("--fetch-recent", action="store_true")
    parser.add_argument("--recent-count", type=int, default=5000)
    parser.add_argument("--estimate-usd", action="store_true")
    parser.add_argument("--default-units", type=float, default=20.0)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--trail-spread-multipliers", nargs="+", default=["1.5", "2.5", "3.5"])
    parser.add_argument("--trail-min-pips", type=float, default=8.0)
    parser.add_argument("--report", type=Path, default=DEFAULT_JSON_REPORT)
    parser.add_argument("--csv-report", type=Path, default=DEFAULT_CSV_REPORT)
    args = parser.parse_args()
    delays = sorted({max(0, int(value)) for value in args.entry_delay_minutes})
    if len(delays) == 1:
        args.entry_delay_minutes = delays[0]
        report = build_report(args)
    else:
        report = build_multi_delay_report(args, delays)
    print(json.dumps({
        "time_utc": report["generated_utc"],
        "candidate_count": report["candidate_count"],
        "instrument_count": report["instrument_count"],
        "entry_delay_minutes": report["entry_delay_minutes"],
        "best_delay_minutes": report.get("best_delay_minutes", report["entry_delay_minutes"]),
        "missed_categories": report["missed_categories"],
        "fresh_30m": report["summary"].get("fresh", {}).get("final_30m_pips", {}),
        "fresh_60m_mfe": report["summary"].get("fresh", {}).get("mfe_60m_pips", {}),
        "logged_30m": report["summary"].get("logged", {}).get("final_30m_pips", {}),
        "report": str(args.report),
        "detail_csv": str(args.csv_report),
    }, indent=2, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
