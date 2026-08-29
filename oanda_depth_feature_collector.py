#!/usr/bin/env python3
"""Read-only OANDA depth/spread collector for prospective production-model research."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List

import oanda_gpt_training_strategy_manager as manager


SCRIPT_VERSION = "depth_collector_v1"
DEFAULT_PAIRS = manager.PREFERRED_MAJOR_INSTRUMENTS[:12]
ROOT = manager.TRAINING_ROOT / "prospective_depth"
STATE_PATH = ROOT / "collector_state.json"
ERROR_PATH = ROOT / "collector_errors.jsonl"
FIELDS = [
    "collected_utc",
    "broker_time_utc",
    "instrument",
    "tradeable",
    "status",
    "bid",
    "ask",
    "mid",
    "spread_pips",
    "closeout_bid",
    "closeout_ask",
    "closeout_spread_pips",
    "bid_top_liquidity",
    "ask_top_liquidity",
    "bid_total_liquidity",
    "ask_total_liquidity",
    "depth_imbalance",
    "bid_levels",
    "ask_levels",
    "bid_vwap_3",
    "ask_vwap_3",
    "microprice",
    "microprice_offset_pips",
    "positive_units_conversion",
    "negative_units_conversion",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def daily_path(timestamp: datetime) -> Path:
    return ROOT / f"depth_{timestamp.strftime('%Y%m%d')}.csv"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def weighted_price(levels: List[Dict[str, Any]], count: int = 3) -> float:
    selected = levels[:count]
    total = sum(safe_float(level.get("liquidity")) for level in selected)
    if total <= 0:
        return safe_float(selected[0].get("price")) if selected else 0.0
    return sum(
        safe_float(level.get("price")) * safe_float(level.get("liquidity"))
        for level in selected
    ) / total


def price_row(price: Dict[str, Any], collected: datetime) -> Dict[str, Any] | None:
    instrument = str(price.get("instrument") or "")
    bids = list(price.get("bids") or [])
    asks = list(price.get("asks") or [])
    if not instrument or not bids or not asks:
        return None
    bid = safe_float(bids[0].get("price"))
    ask = safe_float(asks[0].get("price"))
    if bid <= 0 or ask <= 0:
        return None
    multiplier = manager.pips_multiplier(instrument)
    bid_top_liquidity = safe_float(bids[0].get("liquidity"))
    ask_top_liquidity = safe_float(asks[0].get("liquidity"))
    bid_total = sum(safe_float(level.get("liquidity")) for level in bids)
    ask_total = sum(safe_float(level.get("liquidity")) for level in asks)
    total_depth = bid_total + ask_total
    imbalance = (bid_total - ask_total) / total_depth if total_depth > 0 else 0.0
    top_total = bid_top_liquidity + ask_top_liquidity
    microprice = (
        (ask * bid_top_liquidity + bid * ask_top_liquidity) / top_total
        if top_total > 0 else (bid + ask) / 2.0
    )
    mid = (bid + ask) / 2.0
    closeout_bid = safe_float(price.get("closeoutBid"), bid)
    closeout_ask = safe_float(price.get("closeoutAsk"), ask)
    conversions = price.get("quoteHomeConversionFactors") or {}
    return {
        "collected_utc": collected.isoformat(),
        "broker_time_utc": price.get("time", ""),
        "instrument": instrument,
        "tradeable": bool(price.get("tradeable", False)),
        "status": price.get("status", ""),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_pips": (ask - bid) * multiplier,
        "closeout_bid": closeout_bid,
        "closeout_ask": closeout_ask,
        "closeout_spread_pips": (closeout_ask - closeout_bid) * multiplier,
        "bid_top_liquidity": bid_top_liquidity,
        "ask_top_liquidity": ask_top_liquidity,
        "bid_total_liquidity": bid_total,
        "ask_total_liquidity": ask_total,
        "depth_imbalance": imbalance,
        "bid_levels": len(bids),
        "ask_levels": len(asks),
        "bid_vwap_3": weighted_price(bids),
        "ask_vwap_3": weighted_price(asks),
        "microprice": microprice,
        "microprice_offset_pips": (microprice - mid) * multiplier,
        "positive_units_conversion": safe_float(conversions.get("positiveUnits"), 1.0),
        "negative_units_conversion": safe_float(conversions.get("negativeUnits"), 1.0),
    }


def append_rows(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def save_state(state: Dict[str, Any]) -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    for attempt in range(6):
        try:
            os.replace(temporary, STATE_PATH)
            return
        except PermissionError:
            if attempt >= 5:
                raise
            time.sleep(0.15 * (attempt + 1))


def log_error(error: Exception) -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    with ERROR_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "time_utc": utc_now().isoformat(),
            "error": repr(error),
        }) + "\n")


def collect_once(client: manager.OandaClient, pairs: List[str]) -> Dict[str, Any]:
    collected = utc_now()
    payload = client.pricing(pairs)
    if payload.get("_error"):
        raise RuntimeError(manager.json_dumps(payload))
    rows = [
        row for price in payload.get("prices", [])
        if (row := price_row(price, collected)) is not None
    ]
    append_rows(daily_path(collected), rows)
    return {
        "time_utc": collected.isoformat(),
        "rows_written": len(rows),
        "instruments_requested": len(pairs),
        "http_status": payload.get("_http_status"),
        "latency_ms": payload.get("_latency_ms"),
        "output_file": str(daily_path(collected)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--pairs", nargs="+", default=DEFAULT_PAIRS)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manager.ensure_dirs()
    credentials = manager.load_creds(manager.CREDS_PATH)
    token, base_url, account_id = manager.resolve_oanda_creds(credentials)
    client = manager.OandaClient(token, base_url, account_id)
    state = {
        "script_version": SCRIPT_VERSION,
        "started_utc": utc_now().isoformat(),
        "pairs": args.pairs,
        "interval_seconds": max(1.0, args.interval_seconds),
        "cycles": 0,
        "rows_written": 0,
        "errors": 0,
        "status": "running",
    }
    save_state(state)
    while True:
        started = time.monotonic()
        try:
            result = collect_once(client, args.pairs)
            state["cycles"] += 1
            state["rows_written"] += result["rows_written"]
            state["last_success"] = result
            state["status"] = "running"
        except Exception as error:
            state["errors"] += 1
            state["last_error_utc"] = utc_now().isoformat()
            state["last_error"] = repr(error)
            log_error(error)
        save_state(state)
        if args.once:
            print(json.dumps(state, indent=2))
            return 0
        elapsed = time.monotonic() - started
        time.sleep(max(0.0, max(1.0, args.interval_seconds) - elapsed))


if __name__ == "__main__":
    raise SystemExit(main())
