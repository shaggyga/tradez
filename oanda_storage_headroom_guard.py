#!/usr/bin/env python3
"""Read-only disk, SQLite allocation, WAL, archive, and log headroom audit."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_OUTPUT = STATE / "storage_headroom_v1.json"
DEFAULT_HISTORY = DATA / "logs" / "storage_headroom_v1.jsonl"
DEFAULT_REPORT = DATA / "reports" / "storage" / "STORAGE_HEADROOM_CURRENT.md"
MINIMUM_FREE_BYTES = 50 * 1024**3
CRITICAL_FREE_BYTES = 25 * 1024**3
MINIMUM_GROWTH_WINDOW_SEC = 240.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def tree_bytes(path: Path) -> tuple[int, int]:
    count = total = 0
    if not path.exists():
        return count, total
    for item in path.rglob("*"):
        try:
            if item.is_file():
                count += 1
                total += item.stat().st_size
        except OSError:
            continue
    return count, total


def sqlite_allocation(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path.resolve()),
        "exists": path.is_file(),
        "allocated_bytes": path.stat().st_size if path.is_file() else 0,
        "wal_bytes": Path(str(path) + "-wal").stat().st_size
        if Path(str(path) + "-wal").is_file()
        else 0,
    }
    if not path.is_file():
        return result
    try:
        connection = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro", uri=True, timeout=2.0
        )
        try:
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
            free_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        finally:
            connection.close()
        result.update(
            {
                "page_size": page_size,
                "page_count": page_count,
                "freelist_count": free_count,
                "reclaimable_internal_bytes": page_size * free_count,
                "estimated_live_bytes": page_size * (page_count - free_count),
                "integrity_readable": True,
            }
        )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        result.update(
            {"integrity_readable": False, "read_error": f"{type(exc).__name__}: {exc}"}
        )
    return result


def classify(*, free_bytes: int, total_bytes: int) -> str:
    free_fraction = free_bytes / max(1, total_bytes)
    if free_bytes < CRITICAL_FREE_BYTES or free_fraction < 0.03:
        return "critical"
    if free_bytes < MINIMUM_FREE_BYTES or free_fraction < 0.05:
        return "warning"
    return "ok"


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def growth_projection(
    *, previous: dict[str, Any], generated: str, databases: list[dict[str, Any]],
    free_bytes: int,
) -> dict[str, Any]:
    """Estimate managed SQLite growth without authorizing cleanup.

    The estimate deliberately sums only positive allocation changes. WAL contraction
    therefore cannot conceal simultaneous growth in another evidence database. A
    four-minute observation window is required so startup/checkpoint noise is not
    presented as a useful projection.
    """
    now = _parse_utc(generated)
    before = _parse_utc(previous.get("generated_utc"))
    elapsed = (now - before).total_seconds() if now and before else 0.0
    prior_rows = {
        str(row.get("path")): row
        for row in (previous.get("databases") or [])
        if isinstance(row, dict) and row.get("path")
    }
    rows: list[dict[str, Any]] = []
    positive_delta = 0
    for row in databases:
        current_total = int(row.get("allocated_bytes") or 0) + int(
            row.get("wal_bytes") or 0
        )
        old = prior_rows.get(str(row.get("path")))
        old_total = (
            int(old.get("allocated_bytes") or 0) + int(old.get("wal_bytes") or 0)
            if old
            else current_total
        )
        delta = current_total - old_total
        positive_delta += max(0, delta)
        rows.append(
            {
                "path": row.get("path"),
                "current_bytes": current_total,
                "delta_bytes": delta,
                "positive_delta_bytes": max(0, delta),
            }
        )
    usable = elapsed >= MINIMUM_GROWTH_WINDOW_SEC and bool(prior_rows)
    bytes_per_day = (
        positive_delta * 86400.0 / elapsed if usable and positive_delta > 0 else 0.0
    )

    def days_until(threshold: int) -> float | None:
        if bytes_per_day <= 0:
            return None
        return max(0.0, (free_bytes - threshold) / bytes_per_day)

    days_minimum = days_until(MINIMUM_FREE_BYTES)
    days_critical = days_until(CRITICAL_FREE_BYTES)
    if not usable:
        projection_status = "insufficient_observation_window"
    elif days_critical is not None and days_critical <= 2.0:
        projection_status = "critical"
    elif days_minimum is not None and days_minimum <= 7.0:
        projection_status = "warning"
    elif days_minimum is not None and days_minimum <= 30.0:
        projection_status = "watch"
    else:
        projection_status = "ok"
    return {
        "observation_seconds": round(elapsed, 3),
        "minimum_observation_seconds": MINIMUM_GROWTH_WINDOW_SEC,
        "usable": usable,
        "status": projection_status,
        "method": "sum_positive_managed_sqlite_allocation_and_wal_delta",
        "positive_growth_bytes": positive_delta,
        "estimated_positive_growth_bytes_per_day": round(bytes_per_day, 3),
        "projected_days_to_minimum_free": (
            round(days_minimum, 3) if days_minimum is not None else None
        ),
        "projected_days_to_critical_free": (
            round(days_critical, 3) if days_critical is not None else None
        ),
        "databases": rows,
        "advisory_only": True,
    }


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def run(
    *, output: Path = DEFAULT_OUTPUT, history: Path = DEFAULT_HISTORY,
    report: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    usage = shutil.disk_usage(ROOT)
    status = classify(free_bytes=usage.free, total_bytes=usage.total)
    logs_count, logs_bytes = tree_bytes(DATA / "logs")
    archive_count, archive_bytes = tree_bytes(DATA / "shadow_outcome_archive")
    databases = [
        sqlite_allocation(STATE / name)
        for name in (
            "strategy_shadow_outcomes_v1.sqlite",
            "edge_evidence_v1.sqlite",
            "signal_combination_audit_v1.sqlite",
            "lane_promotion_v1.sqlite",
            "strategy_exit_fit_v1.sqlite",
            "canonical_outcome_worker_v1.sqlite",
        )
    ]
    previous: dict[str, Any] = {}
    try:
        previous = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    generated = utc_now()
    previous_free = int((previous.get("disk") or {}).get("free_bytes") or usage.free)
    projection = growth_projection(
        previous=previous,
        generated=generated,
        databases=databases,
        free_bytes=usage.free,
    )
    payload = {
        "schema_version": 2,
        "generated_utc": generated,
        "status": status,
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "automatic_vacuum": False,
        "automatic_evidence_deletion": False,
        "disk": {
            "root": str(ROOT),
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
            "free_gib": round(usage.free / 1024**3, 3),
            "free_percent": round(100.0 * usage.free / max(1, usage.total), 3),
            "minimum_free_gib": MINIMUM_FREE_BYTES / 1024**3,
            "critical_free_gib": CRITICAL_FREE_BYTES / 1024**3,
            "free_delta_since_previous_bytes": usage.free - previous_free,
        },
        "logs": {"file_count": logs_count, "bytes": logs_bytes},
        "verified_shadow_archive": {
            "file_count": archive_count,
            "bytes": archive_bytes,
        },
        "databases": databases,
        "growth_projection": projection,
        "totals": {
            "database_allocated_bytes": sum(row["allocated_bytes"] for row in databases),
            "database_wal_bytes": sum(row["wal_bytes"] for row in databases),
            "database_reclaimable_internal_bytes": sum(
                int(row.get("reclaimable_internal_bytes") or 0) for row in databases
            ),
        },
        "policy": {
            "live_vacuum_forbidden": True,
            "internal_free_pages_are_reusable_without_disk_growth": True,
            "physical_database_rewrite_requires_validated_offline_migration": True,
            "rotated_log_compression_requires_round_trip_hash_validation": True,
        },
    }
    lines = [
        "# Forex storage headroom", "", f"Generated: `{generated}`", "",
        f"Status: **{status}**", "",
        f"- Free: **{payload['disk']['free_gib']:.2f} GiB ({payload['disk']['free_percent']:.2f}%)**",
        f"- Managed logs: **{logs_bytes / 1024**3:.2f} GiB**",
        f"- Verified causal archive: **{archive_bytes / 1024**3:.2f} GiB**",
        f"- Reusable internal SQLite pages: **{payload['totals']['database_reclaimable_internal_bytes'] / 1024**3:.2f} GiB**",
        f"- WAL allocation: **{payload['totals']['database_wal_bytes'] / 1024**3:.2f} GiB**", "",
        f"- Managed growth projection: **{projection['status']}**",
        f"- Estimated positive managed growth: **{projection['estimated_positive_growth_bytes_per_day'] / 1024**3:.2f} GiB/day**",
        f"- Projected days to 50 GiB free: **{projection['projected_days_to_minimum_free']}**", "",
        "No live VACUUM or evidence deletion is authorized by this guard.", "",
    ]
    atomic_write(output, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    atomic_write(report, "\n".join(lines))
    history.parent.mkdir(parents=True, exist_ok=True)
    with history.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        run(output=args.output, history=args.history, report=args.report)
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
