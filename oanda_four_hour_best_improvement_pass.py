#!/usr/bin/env python3
"""Bounded in-project observer for repeated best-improvement passes.

The observer refreshes the fail-closed integrity/controller reports, compares
material state with the preceding pass, and records the next evidence-gated
action.  It cannot edit models, restart workers, promote, authorize, or trade.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
CONFIG = ROOT / "config" / "four_hour_best_improvement_pass_v1.json"
REPORT_ROOT = DATA / "reports" / "four_hour_best_improvement_pass_20260808"
HISTORY = REPORT_ROOT / "best_improvement_watch.jsonl"
CURRENT = REPORT_ROOT / "BEST_IMPROVEMENT_CURRENT.md"
FINAL = REPORT_ROOT / "FOUR_HOUR_BEST_IMPROVEMENT_SUMMARY_20260808.md"
RUN_STATE = STATE / "four_hour_best_improvement_pass_v1.json"
LOCK = STATE / "four_hour_best_improvement_pass_v1.lock"


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def quote_highwater() -> dict[str, Any]:
    path = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
    try:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=15)
        row = db.execute(
            "SELECT MAX(minute_epoch),MAX(last_event_epoch),COUNT(*),COUNT(DISTINCT instrument) FROM quote_intensity_minutes_v1"
        ).fetchone()
        db.close()
        return {"minute_epoch": int(row[0] or 0), "last_event_epoch": float(row[1] or 0), "rows": int(row[2] or 0), "instruments": int(row[3] or 0)}
    except (OSError, sqlite3.Error) as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def process_snapshot(needles: list[str]) -> dict[str, Any]:
    try:
        import psutil  # type: ignore
    except ImportError:
        return {needle: {"running": None, "error": "psutil unavailable"} for needle in needles}
    rows: dict[str, Any] = {}
    excluded = {os.getpid()}
    try:
        cursor = psutil.Process(os.getpid()).parent()
        while cursor is not None:
            excluded.add(cursor.pid)
            cursor = cursor.parent()
    except (psutil.Error, OSError):
        pass
    processes = []
    for process in psutil.process_iter(["pid", "ppid", "name", "cmdline", "memory_info", "cpu_times"]):
        try:
            if process.pid in excluded:
                continue
            command = " ".join(process.info.get("cmdline") or [])
            processes.append((process, command))
        except (psutil.Error, OSError):
            continue
    for needle in needles:
        matches = []
        for process, command in processes:
            if needle.lower() not in command.lower():
                continue
            try:
                memory = process.info.get("memory_info")
                cpu = process.info.get("cpu_times")
                matches.append({
                    "pid": int(process.pid), "ppid": int(process.ppid()),
                    "rss_bytes": int(memory.rss if memory else 0),
                    "cpu_sec": float((cpu.user + cpu.system) if cpu else 0.0),
                })
            except (psutil.Error, OSError):
                continue
        rows[needle] = {"running": bool(matches), "process_count": len(matches), "processes": matches}
    return rows


def important_files() -> dict[str, int | None]:
    paths = [
        STATE / "strategy_shadow_outcomes_v1.sqlite",
        STATE / "strategy_shadow_outcomes_v1.sqlite-wal",
        STATE / "edge_evidence_v1.sqlite",
        STATE / "executable_opportunity_prospective_v1.sqlite",
        DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite",
        DATA / "local_news_sentiment" / "local_news_sentiment_v1.sqlite-wal",
    ]
    result = {}
    for path in paths:
        try:
            result[str(path)] = path.stat().st_size
        except OSError:
            result[str(path)] = None
    return result


def compact_account_snapshot(payload: Mapping[str, Any]) -> dict[str, Any]:
    aggregate = payload.get("aggregate") or {}
    account = ((payload.get("accounts") or [{}])[0] or {})
    snapshot_state = str(aggregate.get("snapshot_state") or "").strip().lower()
    account_current = bool(account)
    if account.get("ok") is False or account.get(
        "account_values_current", aggregate.get("account_values_current")
    ) is False:
        account_current = False
    if snapshot_state in {
        "unavailable",
        "retained_stale_account_values",
        "stale",
        "failed",
    }:
        account_current = False
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
        "environment": payload.get("environment"), "account_id": account.get("account_id"),
        "account_current": account_current,
        "account_ok": bool(account.get("ok")) if account.get("ok") is not None else account_current,
        "snapshot_state": snapshot_state or ("current" if account_current else "unavailable"),
        "positions_current": positions_current, "orders_current": orders_current,
        "balance": aggregate.get("balance") if account_current else None,
        "nav": aggregate.get("nav") if account_current else None,
        "cumulative_pl": aggregate.get("pl") if account_current else None,
        "unrealized_pl": aggregate.get("unrealizedPL") if account_current else None,
        "open_trades": aggregate.get("openTradeCount") if positions_current else None,
        "pending_orders": aggregate.get("pendingOrderCount") if orders_current else None,
        "last_transaction_id": account.get("lastTransactionID") if account_current else None,
        "last_verified": aggregate.get("last_verified") or account.get("last_verified"),
        "snapshot_utc": payload.get("time"), "practice_only": payload.get("environment") == "practice" and not (payload.get("live_accounts") or []),
    }


def account_snapshot() -> dict[str, Any]:
    return compact_account_snapshot(
        read_json(STATE / "account_007_dashboard_v1.json")
    )


def select_action(snapshot: Mapping[str, Any]) -> dict[str, str]:
    integrity = snapshot.get("integrity") or {}
    failures = integrity.get("failures") or []
    if failures:
        first = str(failures[0])
        action = "preserve fail-closed state and repair the first integrity blocker"
        if first == "clock_explicitly_classified":
            action = "administrator must synchronize Windows Time; retain no_trade and untrusted timestamp gate"
        return {"branch": "operational_integrity", "action": action, "reason": first}
    controller = snapshot.get("controller") or {}
    active = ((controller.get("portfolio") or {}).get("active_build_branch"))
    if active:
        return {"branch": str(active), "action": str((controller.get("portfolio") or {}).get("active_build_action") or "follow frozen branch contract"), "reason": "controller priority"}
    return {
        "branch": "governed_evidence_collection",
        "action": "continue immutable cohorts unchanged; wait for independent market/source episodes",
        "reason": "no safe unblocked build outranks evidence collection",
    }


def compare(previous: Mapping[str, Any], current: Mapping[str, Any]) -> list[str]:
    if not previous:
        return ["initial_snapshot"]
    changes: list[str] = []
    paths = (
        ("account.last_transaction_id", (previous.get("account") or {}).get("last_transaction_id"), (current.get("account") or {}).get("last_transaction_id")),
        ("account.open_trades", (previous.get("account") or {}).get("open_trades"), (current.get("account") or {}).get("open_trades")),
        ("account.snapshot_state", (previous.get("account") or {}).get("snapshot_state"), (current.get("account") or {}).get("snapshot_state")),
        ("integrity.status", (previous.get("integrity") or {}).get("status"), (current.get("integrity") or {}).get("status")),
        ("quotes.minute_epoch", (previous.get("quotes") or {}).get("minute_epoch"), (current.get("quotes") or {}).get("minute_epoch")),
        ("opportunity.forecasts", ((previous.get("opportunity") or {}).get("totals") or {}).get("forecasts"), ((current.get("opportunity") or {}).get("totals") or {}).get("forecasts")),
        ("opportunity.matured", ((previous.get("opportunity") or {}).get("totals") or {}).get("matured"), ((current.get("opportunity") or {}).get("totals") or {}).get("matured")),
    )
    for label, before, after in paths:
        if before != after:
            changes.append(f"{label}:{before}->{after}")
    previous_files = previous.get("files") or {}
    for path, size in (current.get("files") or {}).items():
        old = previous_files.get(path)
        if isinstance(size, int) and isinstance(old, int) and abs(size - old) >= 100 * 1024 * 1024:
            changes.append(f"file_delta:{Path(path).name}:{size-old:+d}")
    prior_free = (previous.get("disk") or {}).get("free_bytes")
    current_free = (current.get("disk") or {}).get("free_bytes")
    if isinstance(prior_free, int) and isinstance(current_free, int) and abs(current_free - prior_free) >= 1024**3:
        changes.append(f"drive_free_delta_bytes:{current_free-prior_free:+d}")
    return changes


def refresh_control() -> list[dict[str, Any]]:
    commands = [
        [sys.executable, str(ROOT / "oanda_project_integrity_audit.py")],
        [sys.executable, str(ROOT / "oanda_improvement_control_engine.py"), "--once"],
    ]
    results = []
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    for command in commands:
        completed = subprocess.run(command, cwd=ROOT.parent, capture_output=True, text=True, timeout=120, creationflags=flags, check=False)
        results.append({"script": Path(command[1]).name, "returncode": completed.returncode, "stderr_tail": (completed.stderr or "")[-500:]})
    return results


def run_tests(test_names: list[str]) -> dict[str, Any]:
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"] + [str(ROOT / name) for name in test_names]
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    completed = subprocess.run(command, cwd=ROOT.parent, capture_output=True, text=True, timeout=300, creationflags=flags, check=False)
    return {"returncode": completed.returncode, "stdout_tail": (completed.stdout or "")[-1000:], "stderr_tail": (completed.stderr or "")[-1000:]}


def snapshot(config: Mapping[str, Any], cycle: int, started: dt.datetime, tests: Mapping[str, Any] | None = None) -> dict[str, Any]:
    refresh = refresh_control()
    disk = shutil.disk_usage(ROOT)
    payload: dict[str, Any] = {
        "schema_version": 1, "run_id": config.get("run_id"), "cycle": cycle,
        "generated_utc": utc_now().isoformat(), "started_utc": started.isoformat(),
        "research_only": True, "execution_eligible": False, "can_place_orders": False, "can_promote": False,
        "refresh": refresh, "tests": tests,
        "account": account_snapshot(), "quotes": quote_highwater(),
        "integrity": read_json(STATE / "project_integrity_audit_v1.json"),
        "controller": read_json(STATE / "improvement_control_engine_v1.json"),
        "clock": read_json(STATE / "clock_integrity_v1.json"),
        "opportunity": read_json(STATE / "executable_opportunity_prospective_v1.json"),
        "direct_source": read_json(STATE / "direct_source_response_v1.json"),
        "news_watchlist": read_json(STATE / "news_technical_watchlist_v1.json"),
        "storage": read_json(STATE / "storage_headroom_v1.json"),
        "disk": {"free_bytes": disk.free, "used_bytes": disk.used, "total_bytes": disk.total},
        "files": important_files(), "workers": process_snapshot(list(config.get("monitored_workers") or [])),
        "contract": config.get("contract") or {}, "supported_execution_decision": "no_trade",
    }
    payload["selected_action"] = select_action(payload)
    return payload


def render_current(payload: Mapping[str, Any], changes: list[str], end: dt.datetime) -> str:
    account = payload.get("account") or {}; clock = payload.get("clock") or {}; opportunity = payload.get("opportunity") or {}
    action = payload.get("selected_action") or {}; integrity = payload.get("integrity") or {}
    return "\n".join([
        "# Four-hour best-improvement pass", "", f"Updated: `{payload.get('generated_utc')}`; scheduled end: `{end.isoformat()}`", "",
        f"- Active branch: **{action.get('branch')}** — {action.get('action')}",
        f"- Integrity: **{integrity.get('status')}** ({', '.join(integrity.get('failures') or []) or 'no failures'})",
        f"- Clock: **{clock.get('status')}**, external offset **{(clock.get('external_https_clock') or {}).get('offset_sec')}s**",
        f"- Practice 007: NAV **{account.get('nav')}**, trades/orders **{account.get('open_trades')}/{account.get('pending_orders')}**",
        f"- Opportunity proof: forecasts/maturities **{((opportunity.get('totals') or {}).get('forecasts') or 0)}/{((opportunity.get('totals') or {}).get('matured') or 0)}**",
        f"- Drive free: **{float((payload.get('disk') or {}).get('free_bytes') or 0) / 1024**3:.2f} GiB**",
        f"- New material changes: **{'; '.join(changes) if changes else 'none'}**",
        "- Execution decision: **no_trade**", "",
    ])


def final_report(history: list[dict[str, Any]], started: dt.datetime, ended: dt.datetime) -> str:
    first = history[0] if history else {}; last = history[-1] if history else {}
    material = [change for row in history for change in (row.get("changes") or []) if change != "initial_snapshot"]
    return "\n".join([
        "# Four-hour best-improvement summary", "", f"Window: `{started.isoformat()}` to `{ended.isoformat()}`", "",
        f"- Completed passes: **{len(history)}**", f"- Material state changes: **{len(material)}**",
        f"- Initial/final Practice-007 NAV: **{(first.get('account') or {}).get('nav')} / {(last.get('account') or {}).get('nav')}**",
        f"- Initial/final quote highwater: **{(first.get('quotes') or {}).get('minute_epoch')} / {(last.get('quotes') or {}).get('minute_epoch')}**",
        f"- Initial/final opportunity forecasts: **{((first.get('opportunity') or {}).get('totals') or {}).get('forecasts')} / {((last.get('opportunity') or {}).get('totals') or {}).get('forecasts')}**",
        f"- Final integrity: **{(last.get('integrity') or {}).get('status')}** ({', '.join((last.get('integrity') or {}).get('failures') or []) or 'no failures'})",
        f"- Final selected branch: **{(last.get('selected_action') or {}).get('branch')}**",
        "- Final execution decision: **no_trade**", "",
        "The runner made no model edits, restarts, promotions, authorizations, database vacuum, or orders. It only refreshed and compared governed evidence.", "",
    ])


def acquire_lock() -> None:
    try:
        descriptor = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise SystemExit(f"bounded improvement pass lock already exists: {LOCK}") from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"pid": os.getpid(), "created_utc": utc_now().isoformat()}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--duration-sec", type=float)
    parser.add_argument("--interval-sec", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()
    config = read_json(args.config)
    duration = 0.0 if args.once else float(args.duration_sec if args.duration_sec is not None else config.get("duration_sec") or 14400)
    interval = float(args.interval_sec if args.interval_sec is not None else config.get("interval_sec") or 900)
    acquire_lock(); REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    started = utc_now(); end = started + dt.timedelta(seconds=duration); records: list[dict[str, Any]] = []
    atomic(RUN_STATE, json.dumps({"status": "running", "pid": os.getpid(), "started_utc": started.isoformat(), "scheduled_end_utc": end.isoformat(), "research_only": True, "can_place_orders": False}, indent=2, sort_keys=True))
    try:
        cycle = 0
        previous: dict[str, Any] = {}
        tests = None if args.skip_tests else run_tests(list(config.get("focused_tests") or []))
        while True:
            cycle += 1
            current = snapshot(config, cycle, started, tests if cycle == 1 else None)
            changes = compare(previous, current); current["changes"] = changes
            with HISTORY.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(current, sort_keys=True, separators=(",", ":")) + "\n")
            records.append(current); atomic(CURRENT, render_current(current, changes, end)); previous = current
            if args.once or utc_now() >= end:
                break
            time.sleep(min(interval, max(0.0, (end - utc_now()).total_seconds())))
        ended = utc_now(); atomic(FINAL, final_report(records, started, ended))
        atomic(RUN_STATE, json.dumps({"status": "complete", "pid": os.getpid(), "started_utc": started.isoformat(), "ended_utc": ended.isoformat(), "cycles": len(records), "research_only": True, "can_place_orders": False}, indent=2, sort_keys=True))
        return 0
    finally:
        try:
            LOCK.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
