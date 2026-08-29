#!/usr/bin/env python3
"""Emit compact read-only health snapshots for the canonical OANDA research stack."""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import queue
import shutil
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
DEFAULT_VAULT = Path(
    r"D:\vault_backups\thevault_snapshot_20260714_235524\projects\forex"
    r"\forex_model_checkpoint_current.zip"
)


def read_json(
    path: Path,
    *,
    attempts: int = 8,
    retry_delay_sec: float = 0.05,
) -> dict[str, Any]:
    for attempt in range(max(1, int(attempts))):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            if attempt + 1 < max(1, int(attempts)):
                time.sleep(max(0.0, float(retry_delay_sec)))
            continue
        return payload if isinstance(payload, dict) else {}
    return {}


def file_age_sec(path: Path, now: float) -> float | None:
    try:
        return round(max(0.0, now - path.stat().st_mtime), 3)
    except OSError:
        return None


def finite_float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def integer_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def state_health(root: Path, now: float) -> dict[str, Any]:
    state = root / "state"
    tracked = {
        "account_007": state / "account_007_dashboard_v1.json",
        "strategy_lab": state / "strategy_lab_heartbeat_v1.json",
        "second_forecast_hot": state / "second_forecast_hot_heartbeat_v1.json",
        "second_forecast_tracker": state / "second_forecast_tracker_heartbeat_v1.json",
        "micro_pattern": state / "micro_pattern_dashboard_v1.json",
        "depth_collector": root / "prospective_depth_parquet" / "collector_state.json",
        "strategy_exit_fit": state / "strategy_exit_fit_v1.json",
        "strategy_exit_fit_cursor": state / "strategy_exit_fit_worker_cursor_v1.json",
    }
    output: dict[str, Any] = {}
    for name, path in tracked.items():
        item: dict[str, Any] = {
            "age_sec": file_age_sec(path, now),
            "path": str(path),
        }
        if name in {
            "strategy_lab",
            "second_forecast_hot",
            "second_forecast_tracker",
            "depth_collector",
        }:
            payload = read_json(path)
            item.update(
                {
                    "status": payload.get("status"),
                    "phase": payload.get("phase"),
                    "pid": payload.get("pid"),
                    "updated_at": payload.get("updated_at"),
                    "details": payload.get("details") or {},
                }
            )
        output[name] = item
    logs = root / "logs"
    supervisor_logs = sorted(
        logs.glob("always_on_supervisor_*.jsonl"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    supervisor_path = supervisor_logs[0] if supervisor_logs else logs / "always_on_supervisor_missing.jsonl"
    output["supervisor"] = {
        "age_sec": file_age_sec(supervisor_path, now),
        "path": str(supervisor_path),
    }
    return output


def account_summary(root: Path) -> dict[str, Any]:
    payload = read_json(
        root / "state" / "account_007_dashboard_v1.json",
        attempts=50,
        retry_delay_sec=0.1,
    )
    accounts = payload.get("accounts") or []
    account = accounts[0] if accounts and isinstance(accounts[0], dict) else {}
    aggregate = (
        payload.get("aggregate")
        if isinstance(payload.get("aggregate"), dict)
        else {}
    )
    snapshot_state = str(aggregate.get("snapshot_state") or "").strip().lower()
    explicit_current = account.get(
        "account_values_current", aggregate.get("account_values_current")
    )
    explicit_ok = account.get("ok")
    legacy_values_present = any(
        account.get(key) is not None for key in ("NAV", "balance", "pl")
    )
    account_current = bool(account) and snapshot_state not in {
        "unavailable",
        "retained_stale_account_values",
        "stale",
        "failed",
    }
    if explicit_current is False or explicit_ok is False:
        account_current = False
    elif explicit_current is None and explicit_ok is None:
        account_current = account_current and legacy_values_present
    account_ok = bool(explicit_ok) if explicit_ok is not None else account_current
    positions_current = bool(
        account_current
        and account.get(
            "positions_current", aggregate.get("positions_current", True)
        )
    )
    orders_current = bool(
        account_current
        and account.get("orders_current", aggregate.get("orders_current", True))
    )
    return {
        "account_id": account.get("account_id"),
        "environment": account.get("env") or payload.get("environment"),
        "ok": account_ok,
        "account_current": account_current,
        "snapshot_state": snapshot_state
        or ("current" if account_current else "unavailable"),
        "positions_current": positions_current,
        "orders_current": orders_current,
        "balance": (
            finite_float_or_none(account.get("balance")) if account_current else None
        ),
        "nav": finite_float_or_none(account.get("NAV")) if account_current else None,
        "unrealized_pl": (
            finite_float_or_none(account.get("unrealizedPL"))
            if account_current
            else None
        ),
        "realized_pl_total": (
            finite_float_or_none(account.get("pl")) if account_current else None
        ),
        "margin_used": (
            finite_float_or_none(account.get("marginUsed"))
            if account_current
            else None
        ),
        "open_trade_count": (
            integer_or_none(account.get("openTradeCount"))
            if positions_current
            else None
        ),
        "pending_order_count": (
            integer_or_none(account.get("pendingOrderCount"))
            if orders_current
            else None
        ),
        "last_transaction_id": account.get("lastTransactionID"),
        "trades": (account.get("trades") or []) if positions_current else None,
        "last_verified": aggregate.get("last_verified")
        or account.get("last_verified"),
        "status_code": account.get("status_code"),
        "error": account.get("error"),
        "snapshot_time": payload.get("time"),
    }


def combination_summary(root: Path, now: float) -> dict[str, Any]:
    state = root / "state"
    output: dict[str, Any] = {}
    for name, filename in (
        ("recent", "signal_combination_audit_v1.json"),
        ("deep", "signal_combination_deep_v1.json"),
        ("historical", "signal_combination_historical_v1.json"),
    ):
        path = state / filename
        payload = read_json(path)
        output[name] = {
            "age_sec": file_age_sec(path, now),
            "status": payload.get("status"),
            "rules": int(payload.get("validated_rule_count") or 0),
            "account_eligible_rules": int(payload.get("account_eligible_rule_count") or 0),
            "rule_depth_counts": payload.get("rule_depth_counts") or {},
            "last_fitted_horizons": (payload.get("search_config") or {}).get(
                "last_fitted_horizons"
            )
            or [],
        }
    return output


def _query_signal_feed(path: Path, cutoff: float, window_sec: float) -> dict[str, Any]:
    try:
        connection = sqlite3.connect(path, timeout=2.0)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=2000")
        candidate_sources = dict(
            connection.execute(
                "SELECT source, COUNT(*) FROM candidates WHERE expires_epoch >= ? GROUP BY source",
                (cutoff,),
            ).fetchall()
        )
        execution_statuses = dict(
            connection.execute(
                "SELECT status, COUNT(*) FROM executions WHERE submitted_epoch >= ? GROUP BY status",
                (cutoff,),
            ).fetchall()
        )
        recent_rows = connection.execute(
            """
            SELECT candidate_id, submitted_epoch, status, trade_id, payload_json
            FROM executions
            WHERE submitted_epoch >= ?
            ORDER BY submitted_epoch DESC
            LIMIT 20
            """,
            (cutoff,),
        ).fetchall()
        recent: list[dict[str, Any]] = []
        for candidate_id, submitted_epoch, status, trade_id, payload_json in recent_rows:
            try:
                details = json.loads(payload_json or "{}")
            except json.JSONDecodeError:
                details = {}
            recent.append(
                {
                    "candidate_id": candidate_id,
                    "submitted_epoch": submitted_epoch,
                    "status": status,
                    "trade_id": trade_id,
                    "instrument": details.get("instrument"),
                    "direction": details.get("direction"),
                    "reason": details.get("reason"),
                }
            )
        totals = {
            "candidates": int(
                connection.execute(
                    "SELECT COUNT(*) FROM candidates WHERE expires_epoch >= ?",
                    (cutoff,),
                ).fetchone()[0]
            ),
            "executions": int(
                connection.execute(
                    "SELECT COUNT(*) FROM executions WHERE submitted_epoch >= ?",
                    (cutoff,),
                ).fetchone()[0]
            ),
        }
        connection.close()
        return {
            "window_sec": window_sec,
            **totals,
            "candidate_sources": candidate_sources,
            "execution_statuses": execution_statuses,
            "recent_executions": recent,
            "error": "",
        }
    except (OSError, sqlite3.Error) as exc:
        return {
            "window_sec": window_sec,
            "candidates": 0,
            "executions": 0,
            "candidate_sources": {},
            "execution_statuses": {},
            "recent_executions": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def _signal_feed_query_worker(
    path: Path,
    cutoff: float,
    window_sec: float,
    output: Any,
) -> None:
    output.put(_query_signal_feed(path, cutoff, window_sec))


def signal_feed_summary(root: Path, now: float, window_sec: float) -> dict[str, Any]:
    path = root / "state" / "practice_007_signal_feed_v1.sqlite"
    context = mp.get_context("spawn")
    output = context.Queue(maxsize=1)
    process = context.Process(
        target=_signal_feed_query_worker,
        args=(path, now - window_sec, window_sec, output),
        daemon=True,
    )
    process.start()
    process.join(timeout=4.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=1.0)
        if process.is_alive():
            process.kill()
            process.join(timeout=1.0)
        output.close()
        return {
            "window_sec": window_sec,
            "candidates": 0,
            "executions": 0,
            "candidate_sources": {},
            "execution_statuses": {},
            "recent_executions": [],
            "error": "TimeoutError: live signal-feed database remained locked for 4 seconds",
        }
    try:
        result = output.get(timeout=0.5)
    except queue.Empty:
        result = {
            "window_sec": window_sec,
            "candidates": 0,
            "executions": 0,
            "candidate_sources": {},
            "execution_statuses": {},
            "recent_executions": [],
            "error": f"signal-feed query process exited with code {process.exitcode}",
        }
    output.close()
    return result


def dashboard_health(url: str) -> dict[str, Any]:
    started = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=5.0) as response:
            status = int(response.status)
            response.read(1)
        return {
            "ok": status == 200,
            "status": status,
            "latency_ms": round((time.monotonic() - started) * 1000.0, 3),
            "error": "",
        }
    except (OSError, urllib.error.URLError) as exc:
        return {
            "ok": False,
            "status": None,
            "latency_ms": round((time.monotonic() - started) * 1000.0, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }


def storage_summary(root: Path, vault: Path, now: float) -> dict[str, Any]:
    usage = shutil.disk_usage(root.anchor)
    state = root / "state"
    large_files = []
    for filename in (
        "micro_pattern_quotes_v1.sqlite3",
        "micro_pattern_quotes_v1.sqlite3-wal",
        "second_forecast_live_v1.sqlite",
        "strategy_shadow_outcomes_v1.sqlite",
    ):
        path = state / filename
        try:
            large_files.append({"name": filename, "bytes": path.stat().st_size})
        except OSError:
            continue
    return {
        "drive_total_bytes": usage.total,
        "drive_free_bytes": usage.free,
        "drive_used_percent": round(100.0 * usage.used / max(1, usage.total), 3),
        "large_files": large_files,
        "vault_checkpoint": {
            "path": str(vault),
            "age_sec": file_age_sec(vault, now),
            "bytes": vault.stat().st_size if vault.is_file() else 0,
        },
    }


def runtime_snapshot(
    root: Path,
    vault: Path,
    dashboard_url: str,
    signal_window_sec: float,
) -> dict[str, Any]:
    now = time.time()
    health = state_health(root, now)
    maximum_ages = {
        "micro_pattern": 180.0,
        "strategy_exit_fit": 1800.0,
        "strategy_exit_fit_cursor": 1800.0,
        "supervisor": 90.0,
    }
    stale = [
        name
        for name, item in health.items()
        if item["age_sec"] is None or item["age_sec"] > maximum_ages.get(name, 60.0)
    ]
    dashboard = dashboard_health(dashboard_url)
    account = account_summary(root)
    signal_feed = signal_feed_summary(root, now, signal_window_sec)
    return {
        "time_local": datetime.now().astimezone().isoformat(),
        "time_epoch": now,
        "account_007": account,
        "health": health,
        "stale_components": stale,
        "dashboard": dashboard,
        "signal_feed": signal_feed,
        "combinations": combination_summary(root, now),
        "storage": storage_summary(root, vault, now),
        "overall_ok": bool(
            not stale
            and dashboard["ok"]
            and account["ok"]
            and not signal_feed["error"]
        ),
    }


def parse_until_local(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    hour, minute = (int(part) for part in text.split(":", 1))
    now = datetime.now().astimezone()
    deadline = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if deadline <= now:
        deadline += timedelta(days=1)
    return deadline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--vault", type=Path, default=DEFAULT_VAULT)
    parser.add_argument("--dashboard-url", default="http://127.0.0.1:8765/")
    parser.add_argument("--signal-window-sec", type=float, default=900.0)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--until-local", default="")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    if args.signal_window_sec <= 0.0 or args.interval_sec < 0.0:
        raise SystemExit("signal window must be positive and interval cannot be negative")
    return args


def main() -> int:
    args = parse_args()
    deadline = parse_until_local(args.until_local)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
    while True:
        payload = runtime_snapshot(
            args.root,
            args.vault,
            args.dashboard_url,
            args.signal_window_sec,
        )
        serialized = json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True)
        print(serialized, flush=True)
        if args.output is not None:
            with args.output.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True) + "\n")
        if args.interval_sec <= 0.0:
            return 0
        if deadline is not None and datetime.now().astimezone() >= deadline:
            return 0
        sleep_for = args.interval_sec
        if deadline is not None:
            remaining = (deadline - datetime.now().astimezone()).total_seconds()
            sleep_for = min(sleep_for, max(0.0, remaining))
        if sleep_for <= 0.0:
            return 0
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main())
