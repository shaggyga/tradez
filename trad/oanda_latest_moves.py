#!/usr/bin/env python3
"""Maintain a read-only all-68 latest-moves snapshot from OANDA practice data.

The report deliberately separates normalized midpoint movement from the
executable result after crossing the opening and current bid/ask spread.  It
uses GET endpoints only and has no order, trade, authorization, or promotion
surface.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
import math
import os
from pathlib import Path
import statistics
import time
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import requests

from oanda_live_account_readonly_status import (
    DEFAULT_CREDS,
    PRACTICE_BASE_URL,
    cfg_value,
    read_creds,
)


UTC = dt.timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent
DEFAULT_QUOTES = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_market_quotes_v1.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_latest_moves_v1.json"
)
DEFAULT_HISTORY = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "market_sentiment_ticker"
    / "quote_history.json"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_moves"
    / "LATEST.md"
)
SCHEMA_VERSION = "practice_007_latest_moves_v2_elapsed_m1_20260913"
MAX_CURRENT_QUOTE_AGE_SEC = 30
DIRECTIONAL_MOVE_LOOKBACK_MINUTES = 24 * 60
MOVER_CHART_LOOKBACK_MINUTES = 180
MOVER_CHART_MAX_POINTS = 90
LIVE_VELOCITY_MAX_END_AGE_SEC = 15 * 60


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def latest_weekly_open(now: dt.datetime) -> dt.datetime:
    """Return the most recent Sunday 17:00 America/New_York market open."""

    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    local = now.astimezone(NEW_YORK)
    days_since_sunday = (local.weekday() + 1) % 7
    candidate = (local - dt.timedelta(days=days_since_sunday)).replace(
        hour=17,
        minute=0,
        second=0,
        microsecond=0,
    )
    if local < candidate:
        candidate -= dt.timedelta(days=7)
    return candidate.astimezone(UTC)


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temp.write_text(text, encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def quote_contract(
    path: Path,
) -> tuple[list[str], dict[str, float], dict[str, dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    quotes = payload.get("quotes") or {}
    instruments = sorted(str(key) for key in quotes)
    pip_by_instrument = {
        instrument: safe_float((quotes.get(instrument) or {}).get("pip"), 0.0001)
        for instrument in instruments
    }
    prices: dict[str, dict[str, Any]] = {}
    for instrument in instruments:
        quote = quotes.get(instrument) or {}
        bid = safe_float(quote.get("bid"))
        ask = safe_float(quote.get("ask"))
        if bid <= 0 or ask <= bid:
            continue
        prices[instrument] = {
            "bid": bid,
            "ask": ask,
            "time": str(quote.get("time") or ""),
            "tradeable": quote.get("tradeable"),
            "status": "live_quote_stream",
        }
    return instruments, pip_by_instrument, prices


def _get_open_candle(
    *,
    api_key: str,
    instrument: str,
    market_open: dt.datetime,
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {api_key}"}
    open_response = None
    last_error: Exception | None = None
    # The weekly-open candle is immutable.  A brief practice-endpoint/TLS
    # failure must not permanently punch a hole in the all-pair baseline.
    for attempt in range(4):
        try:
            open_response = requests.get(
                f"{PRACTICE_BASE_URL}/v3/instruments/{instrument}/candles",
                headers=headers,
                params={
                    "price": "BA",
                    "granularity": "M5",
                    "from": market_open.isoformat().replace("+00:00", "Z"),
                    "count": 3,
                },
                timeout=30,
            )
            open_response.raise_for_status()
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(0.5 * (2**attempt))
    if open_response is None or last_error is not None and not open_response.ok:
        assert last_error is not None
        raise last_error
    open_candles = [
        row for row in open_response.json().get("candles") or [] if row.get("complete")
    ]
    return {
        "instrument": instrument,
        "open": open_candles[0] if open_candles else None,
        "recent": [],
    }


def fetch_candles(
    *,
    api_key: str,
    instruments: Sequence[str],
    market_open: dt.datetime,
    workers: int = 8,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    output: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 12))) as executor:
        futures = {
            executor.submit(
                _get_open_candle,
                api_key=api_key,
                instrument=instrument,
                market_open=market_open,
            ): instrument
            for instrument in instruments
        }
        for future in as_completed(futures):
            instrument = futures[future]
            try:
                output[instrument] = future.result()
            except Exception as exc:  # network/provider failure is reported per pair
                failures.append({"instrument": instrument, "error": str(exc)[:300]})
    return output, sorted(failures, key=lambda row: row["instrument"])


def _sampled_bucket_observation(candle: Mapping[str, Any], start: dt.datetime) -> tuple[dt.datetime, dt.datetime] | None:
    """Validate retained ticker event metadata; this is not arrival authentication."""
    first, last = candle.get("source_first_event_epoch"), candle.get("source_last_event_epoch")
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in (first, last)):
        return None
    if not start.timestamp() <= first <= last < start.timestamp()+60:
        return None
    try:
        return dt.datetime.fromtimestamp(first, tz=UTC), dt.datetime.fromtimestamp(last, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def load_recent_history(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Keep sampled-minute identity and recorded quote clocks distinct from M1 candles."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in payload.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        instrument = str(row.get("instrument") or "")
        if not instrument:
            continue
        epoch = row.get("minute_epoch")
        valid_minute = type(epoch) in (int, float) and math.isfinite(epoch) and epoch > 0 and epoch % 60 == 0
        try:
            start = dt.datetime.fromtimestamp(epoch, tz=UTC) if valid_minute else None
        except (OverflowError, OSError, ValueError):
            start = None
        bid, ask = safe_float(row.get("close_bid")), safe_float(row.get("close_ask"))
        item = {
            "time": start.isoformat() if start else "", "complete": True,
            "price_source": "sampled_quote_minute_bucket",
            "completion_basis": "minute_bucket_ended_not_exhaustive_tick_coverage",
            "source_minute_epoch": epoch,
            "source_first_event_epoch": row.get("first_epoch"),
            "source_last_event_epoch": row.get("last_epoch"),
            "sample_clock_authentication": "retained_ticker_metadata_only",
            "bid": {"c": bid}, "ask": {"c": ask}, "mid": {"c": (bid+ask)/2},
        }
        observed = _sampled_bucket_observation(item, start) if start else None
        item["sample_clock_state"] = "recorded" if observed else "missing_or_invalid_sample_clock"
        item["first_sample_utc"] = observed[0].isoformat() if observed else None
        item["last_sample_utc"] = observed[1].isoformat() if observed else None
        grouped.setdefault(instrument, []).append(item)
    for rows in grouped.values():
        rows.sort(key=lambda row: str(row.get("time") or ""))
    return grouped



def cached_open_payloads(
    path: Path,
    *,
    market_open: dt.datetime,
) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    cached_open = parse_time(payload.get("market_open_utc"))
    if cached_open != market_open:
        return {}
    output: dict[str, dict[str, Any]] = {}
    for row in payload.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        instrument = str(row.get("instrument") or "")
        first_time = str(row.get("first_candle_utc") or "")
        open_bid = safe_float(row.get("open_bid"))
        open_ask = safe_float(row.get("open_ask"))
        if not instrument or not first_time or min(open_bid, open_ask) <= 0:
            continue
        output[instrument] = {
            "instrument": instrument,
            "open": {
                "time": first_time,
                "complete": True,
                "bid": {"o": open_bid},
                "ask": {"o": open_ask},
            },
            "recent": [],
        }
    return output


def _completed_m1_midpoint(candle: Mapping[str, Any]) -> tuple[float | None, str | None]:
    """One price-consistency rule for windows, charts, and directional inputs."""
    mid_value = candle.get("mid")
    mid = safe_float(mid_value.get("c"), -1) if isinstance(mid_value, Mapping) else -1
    bid_value, ask_value = candle.get("bid"), candle.get("ask")
    if bid_value is not None or ask_value is not None:
        bid = safe_float(bid_value.get("c"), -1) if isinstance(bid_value, Mapping) else -1
        ask = safe_float(ask_value.get("c"), -1) if isinstance(ask_value, Mapping) else -1
        if bid <= 0 or ask < bid:
            return None, "invalid_m1_bid_ask"
        if mid <= 0:
            mid = (bid+ask)/2
        elif not math.isclose(mid, (bid+ask)/2, rel_tol=1e-9, abs_tol=1e-12):
            return None, "inconsistent_m1_midpoint"
    if mid <= 0:
        return None, "invalid_m1_midpoint"
    return mid, None


def completed_m1_window(
    candles: Sequence[Mapping[str, Any]],
    *,
    quote_time: dt.datetime,
    minutes: int,
) -> dict[str, Any]:
    """Exact completed midpoint-close endpoints; no old-nearest substitution."""
    result: dict[str, Any] = {"schema_version": "completed_m1_move_window_v2_20260913",
        "state": "unavailable", "missing_reason": None, "minutes": minutes,
        "price_basis": None, "reference_mid": None, "end_mid": None,
        "historical_arrival_authenticated": False}
    if type(minutes) is not int or not 1 <= minutes <= 1440:
        result["missing_reason"] = "invalid_elapsed_window";return result
    if not isinstance(quote_time, dt.datetime) or quote_time.tzinfo is None or quote_time.utcoffset() is None:
        result["missing_reason"] = "timezone_aware_quote_required";return result
    quote_time = quote_time.astimezone(UTC)
    end = quote_time.replace(second=0, microsecond=0)
    start = end-dt.timedelta(minutes=minutes)
    result.update(reference_completed_utc=start.isoformat(), end_completed_utc=end.isoformat(),
        reference_bar_open_utc=(start-dt.timedelta(minutes=1)).isoformat(),
        end_bar_open_utc=(end-dt.timedelta(minutes=1)).isoformat(),
        quote_time_utc=quote_time.isoformat(), end_age_at_quote_seconds=(quote_time-end).total_seconds(),
        elapsed_seconds=minutes*60, requested_bucket_span_seconds=minutes*60,
        endpoint_time_basis="completed_minute_bucket_boundary",
        reference_bucket_end_utc=start.isoformat(), end_bucket_end_utc=end.isoformat())
    points = {};invalid = None;incomplete = 0
    for candle in candles:
        stamp = parse_time(candle.get("time"))
        if stamp is None:
            invalid = "invalid_m1_timestamp";continue
        completed = stamp+dt.timedelta(minutes=1)
        if completed < start or completed > end:
            continue
        if stamp.second or stamp.microsecond:
            invalid = "off_grid_m1_timestamp";continue
        if candle.get("complete") is not True:
            incomplete += 1;continue
        if completed in points:
            invalid = "duplicate_m1_endpoint";continue
        mid, price_error = _completed_m1_midpoint(candle)
        if price_error:
            invalid = price_error;continue
        source = candle.get("price_source", "completed_m1_candle")
        if source == "sampled_quote_minute_bucket":
            observations = _sampled_bucket_observation(candle, stamp)
            if observations is None:
                invalid = "missing_or_invalid_sample_observation_clock";continue
            first_observed, price_observed = observations
        elif source == "completed_m1_candle":
            first_observed = price_observed = completed
        else:
            invalid = "unsupported_m1_price_source";continue
        points[completed] = {"mid": mid, "source": source,
            "first_observed": first_observed, "price_observed": price_observed}
    result.update(observed_endpoint_count=len(points), expected_endpoint_count=minutes+1,
        incomplete_bar_count=incomplete, interior_m1_complete=len(points)==minutes+1)
    if invalid:
        result["missing_reason"] = invalid;return result
    if end not in points:
        result["missing_reason"] = "exact_completed_end_missing";return result
    if start not in points:
        result["missing_reason"] = "exact_completed_reference_missing";return result
    reference, endpoint = points[start], points[end]
    sources = {point["source"] for point in points.values()}
    if len(sources) != 1:
        result["missing_reason"] = "mixed_m1_price_source_basis";return result
    sampled = endpoint["source"] == "sampled_quote_minute_bucket"
    result.update(state="available", reference_mid=reference["mid"], end_mid=endpoint["mid"],
        price_basis="sampled_quote_minute_last_observation" if sampled else "completed_m1_midpoint_close",
        price_source=endpoint["source"],
        price_observation_time_basis="retained_ticker_event_metadata_not_arrival" if sampled else "candle_close_time_not_arrival",
        reference_price_observed_utc=reference["price_observed"].isoformat(),
        end_price_observed_utc=endpoint["price_observed"].isoformat(),
        reference_first_sample_utc=reference["first_observed"].isoformat() if sampled else None,
        end_first_sample_utc=endpoint["first_observed"].isoformat() if sampled else None,
        actual_price_observation_span_seconds=(endpoint["price_observed"]-reference["price_observed"]).total_seconds(),
        end_observation_age_at_quote_seconds=(quote_time-endpoint["price_observed"]).total_seconds())
    return result


def prior_mid(
    candles: Sequence[Mapping[str, Any]],
    *,
    quote_time: dt.datetime,
    minutes: int,
) -> float | None:
    """Compatibility scalar; richer completed_m1_window retains both clocks."""
    window = completed_m1_window(candles, quote_time=quote_time, minutes=minutes)
    return window["reference_mid"] if window["state"] == "available" else None


def current_quote_status(price: Mapping[str, Any], *, now: dt.datetime | None) -> dict[str, Any]:
    quote_time = parse_time(price.get("time"))
    reason = None
    age = None
    if quote_time is None:reason = "invalid_or_missing_quote_time"
    elif not isinstance(now, dt.datetime) or now.tzinfo is None or now.utcoffset() is None:reason = "current_observation_time_not_supplied"
    else:
        age = (now.astimezone(UTC)-quote_time).total_seconds()
        if age < 0:reason = "future_quote_time"
        elif age > MAX_CURRENT_QUOTE_AGE_SEC:reason = "stale_quote"
    if reason is None and price.get("tradeable") is not True:
        reason = "nontradeable_quote" if price.get("tradeable") is False else "quote_tradeability_unknown"
    return {"current_quote_eligible": reason is None, "quote_missing_reason": reason,
        "quote_age_seconds": age, "maximum_quote_age_seconds": MAX_CURRENT_QUOTE_AGE_SEC}


def normalized_chart_points(
    candles: Sequence[Mapping[str, Any]],
    *,
    lookback_minutes: int = MOVER_CHART_LOOKBACK_MINUTES,
    max_points: int = MOVER_CHART_MAX_POINTS,
) -> list[list[float | int]]:
    """Return a compact, pair-comparable midpoint chart in basis points."""

    points: list[tuple[dt.datetime, float]] = []
    for candle in candles:
        observed = parse_time(candle.get("time"))
        bid_value = candle.get("bid")
        ask_value = candle.get("ask")
        bid = safe_float(
            bid_value.get("c") if isinstance(bid_value, Mapping) else bid_value
        )
        ask = safe_float(
            ask_value.get("c") if isinstance(ask_value, Mapping) else ask_value
        )
        mid = safe_float((candle.get("mid") or {}).get("c"))
        if mid <= 0 and bid > 0 and ask > bid:
            mid = (bid + ask) / 2.0
        if observed is not None and mid > 0:
            points.append((observed, mid))
    points.sort(key=lambda item: item[0])
    if not points:
        return []
    cutoff = points[-1][0] - dt.timedelta(minutes=max(1, lookback_minutes))
    points = [point for point in points if point[0] >= cutoff]
    if len(points) > max(2, max_points):
        step = (len(points) - 1) / float(max_points - 1)
        indexes = sorted({round(index * step) for index in range(max_points)})
        points = [points[index] for index in indexes]
    base = points[0][1]
    return [
        [int(observed.timestamp()), round((mid / base - 1.0) * 10_000.0, 4)]
        for observed, mid in points
    ]


def directional_move_legs(
    rows: Sequence[Mapping[str, Any]],
    *,
    instrument: str,
    pip: float,
    lookback_minutes: int = DIRECTIONAL_MOVE_LOOKBACK_MINUTES,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Extract cost-aware pivot-to-pivot directional legs.

    An endpoint return is deliberately not called a move.  A move starts at a
    local pivot, extends to a new extreme, and ends only after price reverses
    by a pair-specific noise threshold.  The threshold is the maximum of one
    pip, 1.5 median executable spreads, and three median one-bar changes.
    The final un-reversed leg is retained as ``active`` rather than silently
    treated as confirmed.
    """

    points: list[dict[str, Any]] = []
    for row in rows:
        observed = parse_time(row.get("time"))
        bid_value = row.get("bid")
        ask_value = row.get("ask")
        bid = safe_float(bid_value.get("c") if isinstance(bid_value, Mapping) else bid_value)
        ask = safe_float(ask_value.get("c") if isinstance(ask_value, Mapping) else ask_value)
        mid = safe_float((row.get("mid") or {}).get("c"))
        if mid <= 0 and bid > 0 and ask > bid:
            mid = (bid + ask) / 2.0
        if observed is None or min(bid, ask, mid, pip) <= 0 or ask <= bid:
            continue
        points.append(
            {
                "time": observed,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_pips": (ask - bid) / pip,
            }
        )
    points.sort(key=lambda row: row["time"])
    if not points:
        return [], {
            "instrument": instrument,
            "point_count": 0,
            "history_span_minutes": 0.0,
            "lookback_complete": False,
        }
    cutoff = points[-1]["time"] - dt.timedelta(minutes=max(1, lookback_minutes))
    points = [row for row in points if row["time"] >= cutoff]
    changes = [
        abs(points[index]["mid"] - points[index - 1]["mid"]) / pip
        for index in range(1, len(points))
    ]
    median_spread = statistics.median(row["spread_pips"] for row in points)
    median_change = statistics.median(changes) if changes else 0.0
    reversal_pips = max(1.0, 1.5 * median_spread, 3.0 * median_change)

    def emit(
        start_index: int,
        extreme_index: int,
        *,
        direction: int,
        confirmed: bool,
        confirmation_index: int | None,
    ) -> dict[str, Any] | None:
        if extreme_index <= start_index:
            return None
        start = points[start_index]
        end = points[extreme_index]
        gross = abs(end["mid"] - start["mid"]) / pip
        if gross + 1e-12 < reversal_pips:
            return None
        if direction > 0:
            executable = (end["bid"] - start["ask"]) / pip
            label = "increase"
        else:
            executable = (start["bid"] - end["ask"]) / pip
            label = "decrease"
        path = points[start_index : extreme_index + 1]
        travelled = sum(
            abs(path[index]["mid"] - path[index - 1]["mid"]) / pip
            for index in range(1, len(path))
        )
        effective_cost = max(0.0, gross - executable)
        cost_multiple = gross / effective_cost if effective_cost > 0 else None
        confirmation = points[confirmation_index] if confirmation_index is not None else None
        duration_minutes = max(
            0.0, (end["time"] - start["time"]).total_seconds() / 60.0
        )
        signed_gross = gross if direction > 0 else -gross
        signed_bps = (end["mid"] / start["mid"] - 1.0) * 10_000.0
        chart_path = points[start_index:] if not confirmed else []
        chart = normalized_chart_points(
            [
                {
                    "time": row["time"].isoformat(),
                    "bid": row["bid"],
                    "ask": row["ask"],
                    "mid": {"c": row["mid"]},
                }
                for row in chart_path
            ],
            lookback_minutes=lookback_minutes,
        )
        return {
            "instrument": instrument,
            "direction": label,
            "state": "confirmed" if confirmed else "active",
            "start_utc": start["time"].isoformat(),
            "end_utc": end["time"].isoformat(),
            "confirmed_utc": confirmation["time"].isoformat() if confirmation else "",
            "duration_minutes": round(duration_minutes, 3),
            "gross_pips": round(gross, 3),
            "signed_move_pips": round(signed_gross, 3),
            "move_bps": round(abs(signed_bps), 3),
            "signed_move_bps": round(signed_bps, 3),
            "velocity_pips_per_minute": (
                round(signed_gross / duration_minutes, 4)
                if duration_minutes > 0
                else None
            ),
            "velocity_bps_per_hour": (
                round(signed_bps * 60.0 / duration_minutes, 3)
                if duration_minutes > 0
                else None
            ),
            "absolute_velocity_bps_per_hour": (
                round(abs(signed_bps) * 60.0 / duration_minutes, 3)
                if duration_minutes > 0
                else None
            ),
            "executable_net_pips": round(executable, 3),
            "effective_cost_pips": round(effective_cost, 3),
            "gross_to_cost": round(cost_multiple, 3) if cost_multiple is not None else None,
            "path_efficiency": round(gross / travelled, 4) if travelled > 0 else 1.0,
            "reversal_threshold_pips": round(reversal_pips, 3),
            "clear_move": bool(executable > 0 and effective_cost > 0 and gross >= 1.5 * effective_cost),
            "chart_points": chart,
        }

    legs: list[dict[str, Any]] = []
    if len(points) >= 2:
        direction = 0
        start_index = 0
        high_index = 0
        low_index = 0
        extreme_index = 0
        for index in range(1, len(points)):
            if points[index]["mid"] >= points[high_index]["mid"]:
                high_index = index
            if points[index]["mid"] <= points[low_index]["mid"]:
                low_index = index
            if direction == 0:
                if (points[high_index]["mid"] - points[low_index]["mid"]) / pip >= reversal_pips:
                    if high_index > low_index:
                        direction, start_index, extreme_index = 1, low_index, high_index
                    else:
                        direction, start_index, extreme_index = -1, high_index, low_index
                continue
            if direction > 0:
                if points[index]["mid"] >= points[extreme_index]["mid"]:
                    extreme_index = index
                reversal = (points[extreme_index]["mid"] - points[index]["mid"]) / pip
                if reversal >= reversal_pips:
                    leg = emit(start_index, extreme_index, direction=1, confirmed=True, confirmation_index=index)
                    if leg:
                        legs.append(leg)
                    direction, start_index, extreme_index = -1, extreme_index, index
                    low_index = index
            else:
                if points[index]["mid"] <= points[extreme_index]["mid"]:
                    extreme_index = index
                reversal = (points[index]["mid"] - points[extreme_index]["mid"]) / pip
                if reversal >= reversal_pips:
                    leg = emit(start_index, extreme_index, direction=-1, confirmed=True, confirmation_index=index)
                    if leg:
                        legs.append(leg)
                    direction, start_index, extreme_index = 1, extreme_index, index
                    high_index = index
        if direction:
            leg = emit(start_index, extreme_index, direction=direction, confirmed=False, confirmation_index=None)
            if leg:
                legs.append(leg)
    span_minutes = (points[-1]["time"] - points[0]["time"]).total_seconds() / 60.0
    diagnostics = {
        "instrument": instrument,
        "point_count": len(points),
        "history_span_minutes": round(span_minutes, 3),
        "lookback_complete": span_minutes >= max(1, lookback_minutes) - 10,
        "median_spread_pips": round(median_spread, 3),
        "median_bar_change_pips": round(median_change, 3),
        "reversal_threshold_pips": round(reversal_pips, 3),
    }
    return legs, diagnostics


def movement_row(
    *,
    instrument: str,
    pip: float,
    market_open: dt.datetime,
    price: Mapping[str, Any],
    candle_payload: Mapping[str, Any],
    now: dt.datetime | None = None,
) -> dict[str, Any] | None:
    if not isinstance(market_open, dt.datetime) or market_open.tzinfo is None or market_open.utcoffset() is None:
        return None
    if now is not None and (not isinstance(now, dt.datetime) or now.tzinfo is None or now.utcoffset() is None):
        return None
    quote_time = parse_time(price.get("time"))
    bid, ask = safe_float(price.get("bid")), safe_float(price.get("ask"))
    if quote_time is None or type(pip) not in (int, float) or not math.isfinite(pip) or pip <= 0 or bid <= 0 or ask <= bid:
        return None
    current_mid = (bid+ask)/2
    opening = candle_payload.get("open")
    opening = opening if isinstance(opening, Mapping) else {}
    open_time = parse_time(opening.get("time"))
    open_bid = safe_float((opening.get("bid") or {}).get("o"))
    open_ask = safe_float((opening.get("ask") or {}).get("o"))
    valid_open = (open_time is not None and opening.get("complete") is True and
        market_open <= open_time and open_time+dt.timedelta(minutes=1) <= quote_time and open_bid > 0 and open_ask > open_bid)
    open_mid = (open_bid+open_ask)/2 if valid_open else None
    signed_pips = (current_mid-open_mid)/pip if valid_open else None
    long_net = (bid-open_ask)/pip if valid_open else None
    short_net = (open_bid-ask)/pip if valid_open else None
    best_net = max(long_net, short_net) if valid_open else None
    recent = candle_payload.get("recent") or []
    health = current_quote_status(price, now=now)
    row: dict[str, Any] = {
        "instrument": instrument, "market_open_utc": market_open.isoformat(),
        "first_candle_utc": open_time.isoformat() if open_time else "",
        "open_delay_minutes": round((open_time-market_open).total_seconds()/60, 3) if open_time else None,
        "exact_market_open": valid_open and open_time == market_open,
        "opening_reference_valid": valid_open,
        "opening_missing_reason": None if valid_open else "missing_or_invalid_opening_candle",
        "quote_time_utc": quote_time.isoformat(), "tradeable": price.get("tradeable") is True, **health,
        "open_bid": open_bid if valid_open else None, "open_ask": open_ask if valid_open else None, "open_mid": open_mid,
        "bid": bid, "ask": ask, "mid": current_mid,
        "current_spread_pips": round((ask-bid)/pip, 3),
        "opening_spread_pips": round((open_ask-open_bid)/pip, 3) if valid_open else None,
        "since_open_direction": ("up" if signed_pips >= 0 else "down") if valid_open else None,
        "since_open_move_pips": round(signed_pips, 3) if valid_open else None,
        "since_open_abs_pips": round(abs(signed_pips), 3) if valid_open else None,
        "since_open_move_bps": round((current_mid/open_mid-1)*10_000, 3) if valid_open else None,
        "since_open_move_pct": round((current_mid/open_mid-1)*100, 6) if valid_open else None,
        "best_executable_side": ("long" if long_net >= short_net else "short") if valid_open else None,
        "best_executable_net_pips": round(best_net, 3) if valid_open else None,
        "best_executable_net_bps": round(best_net*pip/open_mid*10_000, 3) if valid_open else None,
        "long_net_pips": round(long_net, 3) if valid_open else None,
        "short_net_pips": round(short_net, 3) if valid_open else None,
        "movement_cleared_round_trip_spread": best_net > 0 if valid_open else False,
        "recent_move_basis": "source_labeled_completed_minute_endpoints_live_quote_separate",
        "chart_points": normalized_chart_points(completed_m1_observations(recent, asof=min(quote_time, now) if now is not None else quote_time)),
        "move_windows": {},
    }
    for minutes in (5, 15, 60):
        window = completed_m1_window(recent, quote_time=quote_time, minutes=minutes)
        reference, endpoint = window["reference_mid"], window["end_mid"]
        usable = window["state"] == "available"
        row["move_windows"][f"{minutes}m"] = window
        row[f"move_{minutes}m_pips"] = round((endpoint-reference)/pip, 3) if usable else None
        row[f"move_{minutes}m_bps"] = round((endpoint/reference-1)*10_000, 3) if usable else None
        row[f"move_{minutes}m_pct"] = round((endpoint/reference-1)*100, 6) if usable else None
    return row


def completed_m1_observations(candles: Sequence[Mapping[str, Any]], *, asof: dt.datetime) -> list[dict[str, Any]]:
    """Adapt completed M1 closes to the existing generic observation-point views."""
    points = {};duplicates = set();seen = set()
    for candle in candles:
        start = parse_time(candle.get("time"))
        if start is None or start.second or start.microsecond or candle.get("complete") is not True:
            continue
        end = start+dt.timedelta(minutes=1)
        if end > asof:
            continue
        if end in seen:duplicates.add(end)
        seen.add(end)
        mid, price_error = _completed_m1_midpoint(candle)
        if price_error:
            continue
        source = candle.get("price_source", "completed_m1_candle")
        if source == "sampled_quote_minute_bucket":
            observations = _sampled_bucket_observation(candle, start)
            if observations is None:
                continue
            price_time = observations[1]
        elif source == "completed_m1_candle":
            price_time = end
        else:
            continue
        points[end] = {**dict(candle), "mid": {"c": mid}, "time": price_time.isoformat(),
            "source_bar_open_utc": start.isoformat(), "source_bucket_end_utc": end.isoformat()}
    # A duplicate has no ordering authority; do not select one variant.
    return [points[end] for end in sorted(points) if end not in duplicates]



def _rank(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    absolute: bool,
    positive_only: bool = False,
    limit: int = 20,
    require_current_quote: bool = True,
    require_exact_open: bool = False,
) -> list[dict[str, Any]]:
    eligible = [
        dict(row)
        for row in rows
        if row.get(field) is not None
        and type(row.get(field)) in (int, float) and math.isfinite(row[field])
        and (not require_current_quote or row.get("current_quote_eligible") is True)
        and (not require_exact_open or (row.get("exact_market_open") is True and row.get("opening_reference_valid") is True))
        and (not positive_only or safe_float(row.get(field)) > 0)
    ]
    eligible.sort(
        key=lambda row: abs(safe_float(row.get(field))) if absolute else safe_float(row.get(field)),
        reverse=True,
    )
    result = eligible[:limit]
    for index, row in enumerate(result, start=1):
        row["rank"] = index
    return result


def build_payload(
    *,
    now: dt.datetime,
    market_open: dt.datetime,
    prices: Mapping[str, Mapping[str, Any]],
    candles: Mapping[str, Mapping[str, Any]],
    pip_by_instrument: Mapping[str, float],
    failures: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    if not isinstance(now, dt.datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("timezone_aware_current_observation_required")
    if not isinstance(market_open, dt.datetime) or market_open.tzinfo is None or market_open.utcoffset() is None:
        raise ValueError("timezone_aware_market_open_required")
    rows = []
    directional_legs: list[dict[str, Any]] = []
    directional_diagnostics: list[dict[str, Any]] = []
    integrity_failures = [dict(row) for row in failures]
    for instrument in sorted(pip_by_instrument):
        price = prices.get(instrument)
        candle_payload = candles.get(instrument)
        if price is None or candle_payload is None:
            integrity_failures.append({"instrument": instrument, "error": "missing_price_or_candles"})
            continue
        row = movement_row(
            instrument=instrument,
            pip=safe_float(pip_by_instrument.get(instrument), 0.0001),
            market_open=market_open,
            price=price,
            candle_payload=candle_payload,
            now=now,
        )
        if row is None:
            integrity_failures.append({"instrument": instrument, "error": "invalid_movement_row"})
            continue
        rows.append(row)
        legs, diagnostics = directional_move_legs(
            completed_m1_observations(candle_payload.get("recent") or [], asof=min(now, parse_time(price.get("time")))) ,
            instrument=instrument,
            pip=safe_float(pip_by_instrument.get(instrument), 0.0001),
        )
        for leg in legs:
            leg.update({key: row[key] for key in ("current_quote_eligible", "quote_age_seconds", "quote_missing_reason")})
            if leg.get("clear_move"):
                directional_legs.append(leg)
        directional_diagnostics.append(diagnostics)
    rows.sort(key=lambda row: row["instrument"])
    exact_rows = [row for row in rows if row["exact_market_open"]]
    cost_clear = [row for row in exact_rows if row["movement_cleared_round_trip_spread"]]
    for row in directional_legs:
        end = parse_time(row.get("end_utc"))
        end_age_sec = (
            (now.astimezone(UTC) - end).total_seconds()
            if end is not None
            else None
        )
        row["end_age_sec"] = (
            round(end_age_sec, 3) if end_age_sec is not None else None
        )
        row["live_velocity_fresh"] = bool(
            end_age_sec is not None
            and 0 <= end_age_sec <= LIVE_VELOCITY_MAX_END_AGE_SEC
            and row.get("current_quote_eligible") is True
        )
    active_velocity_legs = [
        row
        for row in directional_legs
        if row.get("state") == "active"
        and row.get("live_velocity_fresh") is True
        and safe_float(row.get("duration_minutes")) >= 5.0
        and row.get("absolute_velocity_bps_per_hour") is not None
    ]
    rankings = {
        "directional_legs": _rank(directional_legs, "move_bps", absolute=False, limit=40, require_current_quote=False),
        "live_velocity": _rank(
            active_velocity_legs,
            "absolute_velocity_bps_per_hour",
            absolute=False,
            limit=20,
        ),
        "since_open_normalized": _rank(rows, "since_open_move_bps", absolute=True, require_exact_open=True),
        "since_open_executable": _rank(
            rows,
            "best_executable_net_bps",
            absolute=False,
            positive_only=True,
            require_exact_open=True,
        ),
        "latest_5m": _rank(rows, "move_5m_bps", absolute=True),
        "latest_15m": _rank(rows, "move_15m_bps", absolute=True),
        "latest_60m": _rank(rows, "move_60m_bps", absolute=True),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": now.astimezone(UTC).isoformat(),
        "maximum_current_quote_age_seconds": MAX_CURRENT_QUOTE_AGE_SEC,
        "recent_window_semantics": "requested_minute_bucket_span_separate_from_recorded_price_observation_span",
        "recent_windows_independent_of_weekly_open": True,
        "configured_instrument_count": len(pip_by_instrument),
        "current_quote_eligible_count": sum(row["current_quote_eligible"] for row in rows),
        "account_scope": "practice_007_read_only",
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "market_open_utc": market_open.isoformat(),
        "instrument_count": len(rows),
        "exact_market_open_count": len(exact_rows),
        "round_trip_cost_clear_count": len(cost_clear),
        "round_trip_cost_clear_pct": round(100.0 * len(cost_clear) / len(exact_rows), 3) if exact_rows else 0.0,
        "failures": sorted(integrity_failures, key=lambda row: row.get("instrument", "")),
        "directional_move_definition": {
            "metric": "cost_aware_pivot_to_pivot_leg_v1",
            "lookback_minutes": DIRECTIONAL_MOVE_LOOKBACK_MINUTES,
            "endpoint_displacement_is_a_move": False,
            "reversal_threshold": "max(1 pip, 1.5x median spread, 3x median absolute bar change)",
            "clear_move": "executable net positive and gross movement at least 1.5x effective entry/exit cost",
            "active_leg_is_labeled_unconfirmed": True,
            "cross_pair_ranking_field": "absolute basis-point movement",
            "live_velocity_ranking": (
                "active clear legs lasting at least five minutes with an endpoint no more than "
                f"{LIVE_VELOCITY_MAX_END_AGE_SEC} seconds old, ranked by absolute basis points per hour"
            ),
            "live_velocity_max_end_age_sec": LIVE_VELOCITY_MAX_END_AGE_SEC,
        },
        "directional_history_complete_instrument_count": sum(
            1 for row in directional_diagnostics if row.get("lookback_complete")
        ),
        "directional_history_instrument_count": len(directional_diagnostics),
        "directional_leg_count": len(directional_legs),
        "directional_history_diagnostics": sorted(
            directional_diagnostics, key=lambda row: row.get("instrument", "")
        ),
        "directional_legs": directional_legs,
        "rankings": rankings,
        "rows": rows,
    }


def markdown_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Practice 007 latest moves",
        "",
        f"Generated: `{payload.get('generated_utc')}`",
        "",
        f"Weekly market open: `{payload.get('market_open_utc')}`",
        "",
        (
            f"Coverage: **{payload.get('exact_market_open_count')}/{payload.get('instrument_count')}** exact-open pairs; "
            f"**{payload.get('round_trip_cost_clear_count')}** cleared both executable spreads "
            f"({payload.get('round_trip_cost_clear_pct')}%)."
        ),
        "",
        "Research-only, read-only, and unable to place or promote orders.",
        "",
        (
            "Canonical move metric: cost-aware pivot-to-pivot directional legs. "
            "Endpoint displacement remains context only and is not classified as a move."
        ),
        (
            f"24-hour path coverage: **{payload.get('directional_history_complete_instrument_count')}/"
            f"{payload.get('directional_history_instrument_count')}** instruments complete."
        ),
    ]
    lines.extend(
        [
            "",
            "## Largest clear directional legs",
            "",
            "| # | Pair | Direction | State | Start | End | Duration | Gross | Net | Bps | Efficiency |",
            "|---:|---|---|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in (payload.get("rankings") or {}).get("directional_legs") or []:
        lines.append(
            "| {rank} | {instrument} | {direction} | {state} | {start_utc} | {end_utc} | "
            "{duration_minutes:.0f}m | {gross_pips:.1f} | {executable_net_pips:.1f} | "
            "{move_bps:.2f} | {path_efficiency:.2f} |".format(**row)
        )
    sections = (
        ("Largest normalized moves since open", "since_open_normalized"),
        ("Largest executable after-spread moves since open", "since_open_executable"),
        ("Largest latest 5-minute moves", "latest_5m"),
        ("Largest latest 15-minute moves", "latest_15m"),
        ("Largest latest 60-minute moves", "latest_60m"),
    )
    for title, key in sections:
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| # | Pair | Since-open move | Move (bps) | Best executable | Net pips | Spread | 5m | 15m | 60m |",
                "|---:|---|---:|---:|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in (payload.get("rankings") or {}).get(key) or []:
            lines.append(
                "| {rank} | {instrument} | {since_open_move_pips} | {since_open_move_bps} | "
                "{best_executable_side} | {best_executable_net_pips} | {current_spread_pips:.1f} | "
                "{move_5m_pips} | {move_15m_pips} | {move_60m_pips} |".format(
                    **{
                        **row,
                        "move_5m_pips": row.get("move_5m_pips"),
                        "move_15m_pips": row.get("move_15m_pips"),
                        "move_60m_pips": row.get("move_60m_pips"),
                    }
                )
            )
    return "\n".join(lines) + "\n"


def run_once(
    *,
    creds: Path,
    quotes: Path,
    history: Path,
    output: Path,
    report: Path,
    workers: int,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    current_time = now or dt.datetime.now(tz=UTC)
    market_open = latest_weekly_open(current_time)
    creds_text = read_creds(creds)
    api_key = cfg_value(creds_text, "OANDA_API_KEY", "OANDA_API_TOKEN")
    account_id = cfg_value(creds_text, "OANDA_ACCOUNT_ID_DUM4")
    if not api_key or not account_id:
        raise RuntimeError("missing practice-007 OANDA credentials")
    instruments, pip_by_instrument, prices = quote_contract(quotes)
    recent = load_recent_history(history)
    candles = cached_open_payloads(output, market_open=market_open)
    missing = [instrument for instrument in instruments if instrument not in candles]
    failures: list[dict[str, str]] = []
    if missing:
        fetched, failures = fetch_candles(
            api_key=api_key,
            instruments=missing,
            market_open=market_open,
            workers=workers,
        )
        candles.update(fetched)
    for instrument, payload in candles.items():
        payload["recent"] = recent.get(instrument, [])
    payload = build_payload(
        now=current_time,
        market_open=market_open,
        prices=prices,
        candles=candles,
        pip_by_instrument=pip_by_instrument,
        failures=failures,
    )
    atomic_write(output, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_write(report, markdown_report(payload))
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args(argv)
    started = time.monotonic()
    while True:
        payload = run_once(
            creds=args.creds,
            quotes=args.quotes,
            history=args.history,
            output=args.output,
            report=args.report,
            workers=args.workers,
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "instrument_count": payload["instrument_count"],
                    "round_trip_cost_clear_count": payload["round_trip_cost_clear_count"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.duration_sec <= 0:
            return 0
        if time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(15.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
