#!/usr/bin/env python3
"""Publish an explicit operational heartbeat for an isolated joint price/news role.

This worker is intentionally small. It does not compute forecasts, place orders,
or repair the joint study. It keeps the supervised operational profile honest by
publishing a fresh, non-failing heartbeat that names the isolated role and the
last observed source blocker while the rest of the research system keeps running.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "joint_price_news_isolation_status_v1_20260916"
WORKER = "joint_price_news_isolation_status_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"source_read_status": "missing"}
    except Exception as exc:  # keep status publishing even if the failing worker wrote partial JSON
        return {"source_read_status": "unreadable", "source_read_error": f"{type(exc).__name__}:{exc}"}


def _summarize_source(source: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in (
        "schema_version",
        "worker",
        "status",
        "phase",
        "last_error",
        "errors",
        "generated_utc",
        "generated_at",
        "generated_epoch",
        "shared_history_prepared",
        "pairs_with_forecast",
        "news_bootstrap_phase",
    ):
        if key in source:
            summary[f"source_{key}"] = source[key]
    if "last_failure" in source:
        summary["source_last_failure"] = source["last_failure"]
    if "source_read_status" in source:
        summary["source_read_status"] = source["source_read_status"]
    if "source_read_error" in source:
        summary["source_read_error"] = source["source_read_error"]
    return summary


def build_status(args: argparse.Namespace) -> dict[str, Any]:
    now = time.time()
    source = _read_json(Path(args.source_heartbeat)) if args.source_heartbeat else {}
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "worker": WORKER,
        "status": "isolated",
        "phase": "research_collection_isolated",
        "last_error": "",
        "errors": [],
        "reported_failure": False,
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "isolated_role": args.isolated_role,
        "isolated_reason": args.reason,
        "operator_action": "joint price/news study is removed from live supervised health until a successor cohort is source-bound and validated",
        "source_heartbeat_path": str(Path(args.source_heartbeat)) if args.source_heartbeat else "",
        "generated_utc": _utc_now(),
        "generated_epoch": now,
        "heartbeat_interval_sec": args.interval_sec,
    }
    status.update(_summarize_source(source))
    return status


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a supervised joint-news isolation heartbeat.")
    parser.add_argument("--output", required=True, help="Heartbeat JSON path to publish.")
    parser.add_argument("--source-heartbeat", default="", help="Previous/failing joint study heartbeat to summarize.")
    parser.add_argument("--isolated-role", default="joint_price_news_study_v9")
    parser.add_argument("--reason", default="bounded_validation_capture_blocker")
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = Path(args.output)
    deadline = time.time() + max(0.0, args.duration_sec)
    while True:
        write_json_atomic(output, build_status(args))
        if args.once or time.time() >= deadline:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
