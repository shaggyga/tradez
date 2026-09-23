#!/usr/bin/env python3
"""Collect OANDA pricing depth snapshots into hourly Parquet partitions.

This records the bid/ask levels OANDA includes in the pricing response:
top bid/ask, level counts, liquidity totals, VWAP-3, imbalance, and microprice.
It is read-only and does not place orders.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

import oanda_depth_feature_collector as depth
import oanda_gpt_training_strategy_manager as manager


ROOT = manager.TRAINING_ROOT / "prospective_depth_parquet"
STATE_PATH = ROOT / "collector_state.json"
ERROR_PATH = ROOT / "collector_errors.jsonl"
DEFAULT_INTERVAL_SECONDS = 1.0
DEFAULT_FLUSH_ROWS = 1024
STATE_REPLACE_ATTEMPTS = 20


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def partition_directory(row: dict[str, Any]) -> Path:
    timestamp = datetime.fromisoformat(str(row["collected_utc"]))
    return (
        ROOT
        / f"date={timestamp.strftime('%Y%m%d')}"
        / f"hour={timestamp.strftime('%H')}"
    )


def write_partition(directory: Path, rows: list[dict[str, Any]]) -> Path | None:
    if not rows:
        return None
    directory.mkdir(parents=True, exist_ok=True)
    nonce = time.time_ns()
    path = directory / f"depth_{os.getpid()}_{nonce}.parquet"
    table = pa.Table.from_pylist(rows)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    pq.write_table(table, temporary, compression="zstd", use_dictionary=True)
    os.replace(temporary, path)
    return path


def flush(rows: list[dict[str, Any]]) -> int:
    by_directory: dict[Path, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_directory[partition_directory(row)].append(row)
    for directory, chunk in by_directory.items():
        write_partition(directory, chunk)
    rows.clear()
    return sum(len(chunk) for chunk in by_directory.values())


def save_state(state: dict[str, Any]) -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    for attempt in range(STATE_REPLACE_ATTEMPTS):
        try:
            os.replace(temporary, STATE_PATH)
            return
        except PermissionError:
            if attempt + 1 >= STATE_REPLACE_ATTEMPTS:
                raise
            # Windows readers can briefly pin the destination file.  Keep the
            # atomic write and retry the replace instead of killing the worker.
            time.sleep(min(0.05 * (attempt + 1), 0.25))


def log_error(error: BaseException) -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    with ERROR_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"time_utc": utc_now().isoformat(), "error": repr(error)}) + "\n")


def collect_once(client: manager.OandaClient, pairs: list[str], buffer: list[dict[str, Any]]) -> int:
    collected = utc_now()
    payload = client.pricing(pairs)
    if payload.get("_error"):
        raise RuntimeError(manager.json_dumps(payload))
    count = 0
    for price in payload.get("prices", []) or []:
        row = depth.price_row(price, collected)
        if row is not None:
            buffer.append(row)
            count += 1
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--flush-rows", type=int, default=DEFAULT_FLUSH_ROWS)
    parser.add_argument("--duration-sec", type=int, default=28800)
    parser.add_argument("--pairs", nargs="+", default=depth.DEFAULT_PAIRS)
    parser.add_argument("--all-tradeable", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval_seconds <= 0.0 or args.flush_rows <= 0 or args.duration_sec <= 0:
        raise SystemExit("interval, flush rows, and duration must be positive")
    return args


def main() -> int:
    args = parse_args()
    manager.ensure_dirs()
    token, base_url, account_id = manager.resolve_oanda_creds(manager.load_creds(manager.CREDS_PATH))
    client = manager.OandaClient(token, base_url, account_id)
    pairs = client.list_instruments() if args.all_tradeable else list(args.pairs)
    state = {
        "script_version": "depth_parquet_collector_v2",
        "started_utc": utc_now().isoformat(),
        "pairs": pairs,
        "interval_seconds": args.interval_seconds,
        "cycles": 0,
        "rows_collected": 0,
        "rows_flushed": 0,
        "errors": 0,
        "status": "running",
        "storage": "immutable parquet parts in hourly hive partitions, compressed with zstd",
        "root": str(ROOT),
    }
    save_state(state)
    stop_at = time.monotonic() + args.duration_sec
    buffer: list[dict[str, Any]] = []
    try:
        while time.monotonic() < stop_at:
            started = time.monotonic()
            try:
                rows = collect_once(client, pairs, buffer)
                state["cycles"] += 1
                state["rows_collected"] += rows
                state["last_success_utc"] = utc_now().isoformat()
                state["status"] = "running"
                if len(buffer) >= args.flush_rows:
                    state["rows_flushed"] += flush(buffer)
            except Exception as error:
                state["errors"] += 1
                state["last_error_utc"] = utc_now().isoformat()
                state["last_error"] = repr(error)
                log_error(error)
            save_state(state)
            if args.once:
                break
            elapsed = time.monotonic() - started
            time.sleep(max(0.0, args.interval_seconds - elapsed))
    finally:
        state["rows_flushed"] += flush(buffer)
        state["status"] = "complete" if time.monotonic() >= stop_at or args.once else "stopped"
        state["finished_utc"] = utc_now().isoformat()
        save_state(state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
