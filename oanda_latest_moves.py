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
SCHEMA_VERSION = "practice_007_latest_moves_v1"
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
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
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
            "time": str(quote.get("time") or payload.get("generated_utc") or ""),
            "tradeable": True,
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


def load_recent_history(path: Path) -> dict[str, list[dict[str, Any]]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in payload.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        instrument = str(row.get("instrument") or "")
        epoch = int(safe_float(row.get("minute_epoch")))
        bid = safe_float(row.get("close_bid"))
        ask = safe_float(row.get("close_ask"))
        if not instrument or epoch <= 0 or min(bid, ask) <= 0:
            continue
        grouped.setdefault(instrument, []).append(
            {
                "time": dt.datetime.fromtimestamp(epoch, tz=UTC).isoformat(),
                "complete": True,
                "bid": {"c": bid},
                "ask": {"c": ask},
                "mid": {"c": (bid + ask) / 2.0},
            }
        )
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


def prior_mid(
    candles: Sequence[Mapping[str, Any]],
    *,
    quote_time: dt.datetime,
    minutes: int,
) -> float | None:
    cutoff = quote_time - dt.timedelta(minutes=minutes)
    selected: float | None = None
    for candle in candles:
        observed = parse_time(candle.get("time"))
        if observed is None or observed > cutoff:
            continue
        close = safe_float((candle.get("mid") or {}).get("c"))
        if close > 0:
            selected = close
    return selected


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
) -> dict[str, Any] | None:
    opening = candle_payload.get("open")
    if not isinstance(opening, Mapping):
        return None
    open_time = parse_time(opening.get("time"))
    quote_time = parse_time(price.get("time"))
    if open_time is None or quote_time is None or pip <= 0:
        return None
    open_bid = safe_float((opening.get("bid") or {}).get("o"))
    open_ask = safe_float((opening.get("ask") or {}).get("o"))
    bid = safe_float(price.get("bid"))
    ask = safe_float(price.get("ask"))
    if min(open_bid, open_ask, bid, ask) <= 0 or open_ask <= open_bid or ask <= bid:
        return None
    open_mid = (open_bid + open_ask) / 2.0
    current_mid = (bid + ask) / 2.0
    signed_pips = (current_mid - open_mid) / pip
    long_net = (bid - open_ask) / pip
    short_net = (open_bid - ask) / pip
    best_side = "long" if long_net >= short_net else "short"
    best_net = max(long_net, short_net)
    recent = candle_payload.get("recent") or []
    row: dict[str, Any] = {
        "instrument": instrument,
        "market_open_utc": market_open.isoformat(),
        "first_candle_utc": open_time.isoformat(),
        "open_delay_minutes": round((open_time - market_open).total_seconds() / 60.0, 3),
        "exact_market_open": abs((open_time - market_open).total_seconds()) <= 15 * 60,
        "quote_time_utc": quote_time.isoformat(),
        "tradeable": bool(price.get("tradeable", True)),
        "open_bid": open_bid,
        "open_ask": open_ask,
        "open_mid": open_mid,
        "bid": bid,
        "ask": ask,
        "mid": current_mid,
        "current_spread_pips": round((ask - bid) / pip, 3),
        "opening_spread_pips": round((open_ask - open_bid) / pip, 3),
        "since_open_direction": "up" if signed_pips >= 0 else "down",
        "since_open_move_pips": round(signed_pips, 3),
        "since_open_abs_pips": round(abs(signed_pips), 3),
        "since_open_move_bps": round((current_mid / open_mid - 1.0) * 10_000.0, 3),
        "best_executable_side": best_side,
        "best_executable_net_pips": round(best_net, 3),
        "long_net_pips": round(long_net, 3),
        "short_net_pips": round(short_net, 3),
        "movement_cleared_round_trip_spread": best_net > 0,
        "chart_points": normalized_chart_points(recent),
    }
    for minutes in (5, 15, 60):
        reference = prior_mid(recent, quote_time=quote_time, minutes=minutes)
        row[f"move_{minutes}m_pips"] = (
            round((current_mid - reference) / pip, 3) if reference else None
        )
        row[f"move_{minutes}m_bps"] = (
            round((current_mid / reference - 1.0) * 10_000.0, 3)
            if reference
            else None
        )
    return row


def _rank(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    absolute: bool,
    positive_only: bool = False,
    limit: int = 20,
) -> list[dict[str, Any]]:
    eligible = [
        dict(row)
        for row in rows
        if row.get(field) is not None
        and bool(row.get("exact_market_open", True))
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
        )
        if row is None:
            integrity_failures.append({"instrument": instrument, "error": "invalid_movement_row"})
            continue
        rows.append(row)
        legs, diagnostics = directional_move_legs(
            candle_payload.get("recent") or [],
            instrument=instrument,
            pip=safe_float(pip_by_instrument.get(instrument), 0.0001),
        )
        directional_legs.extend(leg for leg in legs if leg.get("clear_move"))
        directional_diagnostics.append(diagnostics)
    rows.sort(key=lambda row: row["instrument"])
    exact_rows = [row for row in rows if row["exact_market_open"]]
    cost_clear = [row for row in exact_rows if row["movement_cleared_round_trip_spread"]]
    for row in directional_legs:
        end = parse_time(row.get("end_utc"))
        end_age_sec = (
            max(0.0, (now.astimezone(UTC) - end).total_seconds())
            if end is not None
            else None
        )
        row["end_age_sec"] = (
            round(end_age_sec, 3) if end_age_sec is not None else None
        )
        row["live_velocity_fresh"] = bool(
            end_age_sec is not None
            and end_age_sec <= LIVE_VELOCITY_MAX_END_AGE_SEC
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
        "directional_legs": _rank(directional_legs, "move_bps", absolute=False, limit=40),
        "live_velocity": _rank(
            active_velocity_legs,
            "absolute_velocity_bps_per_hour",
            absolute=False,
            limit=20,
        ),
        "since_open_normalized": _rank(rows, "since_open_move_bps", absolute=True),
        "since_open_executable": _rank(
            rows,
            "best_executable_net_pips",
            absolute=False,
            positive_only=True,
        ),
        "latest_5m": _rank(rows, "move_5m_bps", absolute=True),
        "latest_15m": _rank(rows, "move_15m_bps", absolute=True),
        "latest_60m": _rank(rows, "move_60m_bps", absolute=True),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": now.astimezone(UTC).isoformat(),
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
                "| {rank} | {instrument} | {since_open_move_pips:.1f} | {since_open_move_bps:.2f} | "
                "{best_executable_side} | {best_executable_net_pips:.1f} | {current_spread_pips:.1f} | "
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
