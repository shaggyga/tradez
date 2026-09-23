#!/usr/bin/env python3
"""Nonblocking WAL maintenance for the consolidated practice signal feed.

This worker owns PASSIVE checkpoints so forecast producers and the paper
executor never perform checkpoint I/O in their commit path. PASSIVE mode does
not wait for readers or writers and this worker never truncates a live WAL.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
DEFAULT_DATABASE = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/state/practice_007_signal_feed_v1.sqlite"
)
DEFAULT_STATE = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/state/"
    "practice_007_signal_feed_wal_maintenance_v1.json"
)


def iso_utc() -> str:
    return dt.datetime.now(UTC).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def checkpoint_once(database: Path, threshold_bytes: int) -> dict[str, Any]:
    wal_path = database.with_name(database.name + "-wal")
    try:
        before = wal_path.stat().st_size
        wal_mtime_ns = wal_path.stat().st_mtime_ns
    except OSError:
        before = 0
        wal_mtime_ns = 0
    result: dict[str, Any] = {
        "checked_utc": iso_utc(),
        "database": str(database.resolve()),
        "wal": str(wal_path.resolve()),
        "wal_bytes_before": int(before),
        "wal_mtime_ns": int(wal_mtime_ns),
        "threshold_bytes": int(threshold_bytes),
        "mode": "PASSIVE",
        "can_place_orders": False,
        "research_only": True,
    }
    if before < max(0, int(threshold_bytes)):
        result.update(status="not_needed", duration_ms=0.0)
        return result
    started = time.perf_counter()
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(database, timeout=0.1)
        connection.execute("PRAGMA busy_timeout=0")
        busy, wal_frames, checkpointed_frames = connection.execute(
            "PRAGMA wal_checkpoint(PASSIVE)"
        ).fetchone()
        result.update(
            status="busy" if int(busy) else "ok",
            busy=int(busy),
            wal_frames=int(wal_frames),
            checkpointed_frames=int(checkpointed_frames),
            remaining_frames=max(
                0,
                int(wal_frames) - int(checkpointed_frames),
            ),
        )
    except sqlite3.Error as exc:
        result.update(
            status="busy" if "locked" in str(exc).lower() else "error",
            error={"kind": type(exc).__name__, "message": str(exc)},
        )
    finally:
        if connection is not None:
            connection.close()
    try:
        result["wal_bytes_after"] = int(wal_path.stat().st_size)
    except OSError:
        result["wal_bytes_after"] = 0
    result["duration_ms"] = round(
        (time.perf_counter() - started) * 1000.0,
        3,
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--threshold-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--urgent-threshold-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--minimum-checkpoint-sec", type=float, default=300.0)
    parser.add_argument("--duration-sec", type=float, default=7 * 86400.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stop_at = time.monotonic() + max(1.0, float(args.duration_sec))
    last_wal_mtime_ns = -1
    last_checkpoint_at = -float("inf")
    last_result: dict[str, Any] = {}
    while time.monotonic() < stop_at:
        wal_path = args.database.with_name(args.database.name + "-wal")
        try:
            current_mtime_ns = wal_path.stat().st_mtime_ns
        except OSError:
            current_mtime_ns = 0
        try:
            current_wal_bytes = wal_path.stat().st_size
        except OSError:
            current_wal_bytes = 0
        checkpoint_due = (
            time.monotonic() - last_checkpoint_at
            >= max(5.0, float(args.minimum_checkpoint_sec))
        )
        urgent = current_wal_bytes >= max(
            int(args.threshold_bytes),
            int(args.urgent_threshold_bytes),
        )
        if (
            current_mtime_ns != last_wal_mtime_ns
            and (checkpoint_due or urgent)
        ):
            last_result = checkpoint_once(
                args.database,
                args.threshold_bytes,
            )
            last_wal_mtime_ns = current_mtime_ns
            if last_result.get("status") in {"ok", "busy", "error"}:
                last_checkpoint_at = time.monotonic()
                checkpoint_due = False
        state = {
            "schema_version": 1,
            "updated_utc": iso_utc(),
            "status": "running",
            "worker": "oanda_signal_feed_wal_maintenance",
            "pid": os.getpid(),
            "last_checkpoint": last_result,
            "checkpoint_due": checkpoint_due,
            "urgent": urgent,
            "minimum_checkpoint_sec": float(args.minimum_checkpoint_sec),
            "can_place_orders": False,
            "research_only": True,
        }
        atomic_write_json(args.state, state)
        time.sleep(max(5.0, float(args.interval_sec)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
