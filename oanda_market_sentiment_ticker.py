#!/usr/bin/env python3
"""Derive a read-only FX sentiment ticker from completed OANDA quote bars."""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np


UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parent
DEFAULT_DATABASE = ROOT / "data" / "significant_moves" / "live_alerts" / "move_alerts_v1.sqlite"
DEFAULT_QUOTES = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_market_quotes_v1.json"
)
DEFAULT_HISTORY = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "market_sentiment_ticker"
    / "quote_history.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "market_sentiment_ticker"
    / "LATEST.json"
)
SCHEMA_VERSION = "oanda_cross_currency_market_sentiment_v1"
WINDOWS_MINUTES = (1, 5, 15, 30, 60)
HAVENS = ("JPY", "CHF")
RISK_CURRENCIES = ("AUD", "NZD", "CAD", "NOK")
EM_CURRENCIES = ("ZAR", "MXN", "TRY", "PLN", "HUF", "CZK")


def iso_utc_from_epoch(value: float) -> str:
    return dt.datetime.fromtimestamp(value, tz=UTC).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def parse_epoch(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def split_instrument(instrument: str) -> tuple[str, str]:
    parts = str(instrument or "").upper().split("_", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", "")


def quote_mid(row: Mapping[str, Any]) -> float:
    return (
        safe_float(row.get("close_bid")) + safe_float(row.get("close_ask"))
    ) / 2.0


def load_recent_quote_bars(
    database: Path,
    *,
    history_minutes: int = 125,
) -> tuple[list[dict[str, Any]], int]:
    uri = f"file:{database.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        latest_row = connection.execute(
            """
            SELECT minute_epoch
            FROM quote_bars INDEXED BY idx_move_quote_time
            ORDER BY minute_epoch DESC
            LIMIT 1
            """
        ).fetchone()
        if latest_row is None:
            return [], 0
        latest = int(latest_row[0])
        rows = connection.execute(
            """
            SELECT instrument, minute_epoch, first_epoch, last_epoch,
                   open_bid, open_ask, close_bid, close_ask,
                   high_mid, low_mid, pip
            FROM quote_bars INDEXED BY idx_move_quote_time
            WHERE minute_epoch >= ?
            ORDER BY instrument, minute_epoch
            """,
            (latest - max(65, history_minutes) * 60,),
        ).fetchall()
        return [dict(row) for row in rows], latest
    finally:
        connection.close()


def quote_snapshot_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    generated = parse_epoch(payload.get("generated_utc"))
    output = []
    for instrument, quote in (payload.get("quotes") or {}).items():
        if not isinstance(quote, dict):
            continue
        bid = safe_float(quote.get("bid"))
        ask = safe_float(quote.get("ask"))
        observed = parse_epoch(quote.get("time")) or generated
        if bid <= 0 or ask <= bid or observed <= 0:
            continue
        minute = int(observed // 60) * 60
        mid = (bid + ask) / 2.0
        output.append(
            {
                "instrument": str(instrument),
                "minute_epoch": minute,
                "first_epoch": observed,
                "last_epoch": observed,
                "open_bid": bid,
                "open_ask": ask,
                "close_bid": bid,
                "close_ask": ask,
                "high_mid": mid,
                "low_mid": mid,
                "pip": safe_float(quote.get("pip"), 0.0001),
            }
        )
    return output


def merge_quote_history(
    history: Sequence[Mapping[str, Any]],
    updates: Sequence[Mapping[str, Any]],
    *,
    retention_minutes: int = 125,
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int], dict[str, Any]] = {}
    for source_row in [*history, *updates]:
        row = dict(source_row)
        instrument = str(row.get("instrument") or "")
        minute = int(safe_float(row.get("minute_epoch")))
        if not instrument or minute <= 0:
            continue
        key = (instrument, minute)
        existing = merged.get(key)
        if existing is None:
            merged[key] = row
            continue
        if safe_float(row.get("last_epoch")) >= safe_float(existing.get("last_epoch")):
            existing["last_epoch"] = row.get("last_epoch")
            existing["close_bid"] = row.get("close_bid")
            existing["close_ask"] = row.get("close_ask")
            existing["pip"] = row.get("pip")
        existing["first_epoch"] = min(
            safe_float(existing.get("first_epoch")),
            safe_float(row.get("first_epoch")),
        )
        existing["high_mid"] = max(
            safe_float(existing.get("high_mid")),
            safe_float(row.get("high_mid")),
        )
        existing["low_mid"] = min(
            safe_float(existing.get("low_mid")),
            safe_float(row.get("low_mid")),
        )
    latest = max((key[1] for key in merged), default=0)
    cutoff = latest - max(65, retention_minutes) * 60
    return [
        merged[key]
        for key in sorted(merged)
        if key[1] >= cutoff
    ]


def load_history(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    return [
        dict(row)
        for row in payload.get("rows") or []
        if isinstance(row, dict)
    ]


def write_history(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    atomic_write_json(
        path,
        {
            "schema_version": SCHEMA_VERSION,
            "generated_utc": dt.datetime.now(tz=UTC).isoformat(),
            "row_count": len(rows),
            "rows": list(rows),
        },
    )


def pair_windows(
    rows: Sequence[Mapping[str, Any]],
    latest_epoch: int,
) -> dict[str, dict[str, Any]]:
    """Return pair windows using only quotes known by each cutoff clock.

    Retained rows are minute aggregates, but ``first_epoch`` and
    ``last_epoch`` preserve the exact quote clocks inside that minute.  A
    historical query inside a minute may use the opening quote once its first
    clock has arrived; it may not use the aggregate close until ``last_epoch``.
    This keeps historical factor surfaces causal without weakening the normal
    current-time surface.
    """

    def known_quote(
        values: Sequence[Mapping[str, Any]],
        minute_times: Sequence[int],
        cutoff: int,
    ) -> tuple[Mapping[str, Any], float, float, int] | None:
        index = bisect.bisect_right(minute_times, int(cutoff)) - 1
        while index >= 0:
            row = values[index]
            minute = int(row.get("minute_epoch") or 0)
            first = int(safe_float(row.get("first_epoch"), minute))
            last = int(safe_float(row.get("last_epoch"), first))
            if first <= int(cutoff):
                if last <= int(cutoff):
                    bid = safe_float(row.get("close_bid"))
                    ask = safe_float(row.get("close_ask"))
                    observed = last
                else:
                    bid = safe_float(row.get("open_bid"))
                    ask = safe_float(row.get("open_ask"))
                    observed = first
                if bid > 0.0 and ask > bid:
                    return row, bid, ask, observed
            index -= 1
        return None

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("instrument") or ""), []).append(row)
    output: dict[str, dict[str, Any]] = {}
    for instrument, values in grouped.items():
        values.sort(key=lambda row: int(row.get("minute_epoch") or 0))
        times = [int(row.get("minute_epoch") or 0) for row in values]
        latest_known = known_quote(values, times, latest_epoch)
        if latest_known is None:
            continue
        latest, latest_bid, latest_ask, latest_clock = latest_known
        latest_mid = (latest_bid + latest_ask) / 2.0
        if latest_mid <= 0:
            continue
        spread_bps = (
            (latest_ask - latest_bid)
            / latest_mid
            * 10000.0
        )
        windows: dict[str, Any] = {}
        for minutes in WINDOWS_MINUTES:
            target = latest_epoch - minutes * 60
            old_known = known_quote(values, times, target)
            if old_known is None:
                continue
            old, old_bid, old_ask, old_clock = old_known
            old_mid = (old_bid + old_ask) / 2.0
            if old_mid <= 0:
                continue
            return_bps = math.log(latest_mid / old_mid) * 10000.0
            windows[str(minutes)] = {
                "return_bps": round(return_bps, 6),
                "return_pips": round(
                    (latest_mid - old_mid) / max(safe_float(latest.get("pip")), 1e-12),
                    6,
                ),
                "start_epoch": old_clock,
                "end_epoch": latest_clock,
                "observed_minutes": round(
                    (latest_clock - old_clock) / 60.0,
                    3,
                ),
            }
        output[instrument] = {
            "instrument": instrument,
            "latest_epoch": latest_clock,
            "mid": round(latest_mid, 9),
            "spread_bps": round(max(0.0, spread_bps), 6),
            "windows": windows,
        }
    return output


def solve_currency_strength(
    pair_moves: Mapping[str, Mapping[str, Any]],
    minutes: int,
) -> dict[str, Any]:
    observations: list[tuple[str, str, float, float, str]] = []
    for instrument, pair in pair_moves.items():
        base, quote = split_instrument(instrument)
        point = (pair.get("windows") or {}).get(str(minutes)) or {}
        if not base or not quote or "return_bps" not in point:
            continue
        move = safe_float(point.get("return_bps"))
        spread = max(0.05, safe_float(pair.get("spread_bps"), 1.0))
        observations.append((base, quote, move, min(10.0, 1.0 / spread), instrument))
    currencies = sorted({value for row in observations for value in row[:2]})
    if len(observations) < 3 or len(currencies) < 3:
        return {"currency_strength_bps": {}, "pair_residual_bps": {}, "observation_count": 0}

    raw_moves = [row[2] for row in observations]
    median_abs = statistics.median(abs(value) for value in raw_moves)
    clip = max(5.0, median_abs * 8.0)
    index = {currency: position for position, currency in enumerate(currencies)}
    matrix = np.zeros((len(observations) + 1, len(currencies)), dtype=float)
    target = np.zeros(len(observations) + 1, dtype=float)
    for row_index, (base, quote, move, weight, _) in enumerate(observations):
        root_weight = math.sqrt(weight)
        matrix[row_index, index[base]] = root_weight
        matrix[row_index, index[quote]] = -root_weight
        target[row_index] = max(-clip, min(clip, move)) * root_weight
    matrix[-1, :] = 1.0
    solution, _, _, _ = np.linalg.lstsq(matrix, target, rcond=None)
    strengths = {
        currency: round(float(solution[position]), 6)
        for currency, position in index.items()
    }
    residuals = {}
    for base, quote, move, _, instrument in observations:
        residuals[instrument] = round(move - (strengths[base] - strengths[quote]), 6)
    return {
        "currency_strength_bps": strengths,
        "pair_residual_bps": residuals,
        "observation_count": len(observations),
        "currency_count": len(currencies),
        "return_clip_bps": round(clip, 6),
        "median_abs_pair_return_bps": round(median_abs, 6),
        "residual_mae_bps": round(
            sum(abs(value) for value in residuals.values()) / len(residuals),
            6,
        ),
    }


def mean_currency(scores: Mapping[str, Any], currencies: Sequence[str]) -> float:
    values = [safe_float(scores.get(currency)) for currency in currencies if currency in scores]
    return sum(values) / len(values) if values else 0.0


def theme_state(strengths: Mapping[str, Any]) -> dict[str, Any]:
    values = [abs(safe_float(value)) for value in strengths.values()]
    scale = max(0.10, statistics.median(values) if values else 0.10)
    haven = mean_currency(strengths, HAVENS)
    risk = mean_currency(strengths, RISK_CURRENCIES)
    emerging = mean_currency(strengths, EM_CURRENCIES)
    usd = safe_float(strengths.get("USD"))
    risk_off = haven - risk
    commodity_weakness = -risk
    em_stress = -emerging
    standardized = {
        "risk_off": risk_off / scale,
        "haven_demand": haven / scale,
        "commodity_currency_weakness": commodity_weakness / scale,
        "em_stress": em_stress / scale,
        "usd_strength": usd / scale,
    }
    if standardized["risk_off"] >= 1.0:
        regime = "RISK_OFF"
    elif standardized["risk_off"] <= -1.0:
        regime = "RISK_ON"
    elif standardized["usd_strength"] >= 1.0:
        regime = "USD_STRENGTH"
    elif standardized["usd_strength"] <= -1.0:
        regime = "USD_WEAKNESS"
    else:
        regime = "MIXED"
    return {
        "regime": regime,
        "cross_sectional_scale_bps": round(scale, 6),
        "raw_bps": {
            "risk_off": round(risk_off, 6),
            "haven_demand": round(haven, 6),
            "commodity_currency_weakness": round(commodity_weakness, 6),
            "em_stress": round(em_stress, 6),
            "usd_strength": round(usd, 6),
        },
        "standardized": {
            key: round(value, 6) for key, value in standardized.items()
        },
    }


def build_market_surface(
    rows: Sequence[Mapping[str, Any]],
    latest_epoch: int,
    *,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    now_value = safe_float(now_epoch, dt.datetime.now(tz=UTC).timestamp())
    pairs = pair_windows(rows, latest_epoch)
    horizons = {
        str(minutes): solve_currency_strength(pairs, minutes)
        for minutes in WINDOWS_MINUTES
    }
    themes = {
        key: theme_state(value.get("currency_strength_bps") or {})
        for key, value in horizons.items()
    }
    current = themes.get("15") or themes.get("5") or {"regime": "UNKNOWN"}
    age_seconds = max(0.0, now_value - latest_epoch) if latest_epoch else None
    fresh = age_seconds is not None and age_seconds <= 180.0
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": dt.datetime.now(tz=UTC).isoformat(),
        "latest_quote_utc": iso_utc_from_epoch(latest_epoch) if latest_epoch else "",
        "quote_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "fresh": fresh,
        "status": "ready" if fresh and len(pairs) >= 30 else "degraded",
        "instrument_count": len(pairs),
        "windows_minutes": list(WINDOWS_MINUTES),
        "current_regime": current.get("regime", "UNKNOWN"),
        "horizons": horizons,
        "themes": themes,
        "pair_moves": pairs,
        "research_only": True,
        "execution_eligible": False,
        "source": {
            "kind": "completed_oanda_bid_ask_minute_bars",
            "database": str(DEFAULT_DATABASE),
            "broker_api_calls": 0,
        },
    }


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.4, 0.025 * (2**attempt)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--source", choices=("database", "quotes"), default="database")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history-minutes", type=int, default=125)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()
    history_rows = load_history(args.history) if args.source == "quotes" else []
    while True:
        if args.source == "quotes":
            history_rows = merge_quote_history(
                history_rows,
                quote_snapshot_rows(load_json(args.quotes)),
                retention_minutes=max(65, args.history_minutes),
            )
            rows = history_rows
            latest = max(
                (int(safe_float(row.get("minute_epoch"))) for row in rows),
                default=0,
            )
            write_history(args.history, rows)
        else:
            rows, latest = load_recent_quote_bars(
                args.database,
                history_minutes=max(65, args.history_minutes),
            )
            write_history(args.history, rows)
        payload = build_market_surface(rows, latest)
        payload["source"] = {
            "kind": (
                "atomic_oanda_quote_snapshots"
                if args.source == "quotes"
                else "completed_oanda_bid_ask_minute_bars"
            ),
            "database": str(args.database.resolve()) if args.source == "database" else "",
            "quotes": str(args.quotes.resolve()) if args.source == "quotes" else "",
            "history": str(args.history.resolve()),
            "broker_api_calls": 0,
        }
        atomic_write_json(args.output, payload)
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "status": payload["status"],
                    "fresh": payload["fresh"],
                    "quote_age_seconds": payload["quote_age_seconds"],
                    "instrument_count": payload["instrument_count"],
                    "current_regime": payload["current_regime"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            break
        if args.duration_sec > 0 and time.time() - started >= args.duration_sec:
            break
        time.sleep(max(5.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
