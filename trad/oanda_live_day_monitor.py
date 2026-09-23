#!/usr/bin/env python3
"""Read-only live forex stack day monitor.

This script does not restart managers and does not submit broker writes.  It
polls process health, read-only OANDA account summaries/open trades, the GPT
research monitor ledger, and scout scan summaries, then writes a compact CSV and
latest JSON snapshot for intraday review.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter, deque
from pathlib import Path
from typing import Any

import oanda_advisor_account_manager_auto as advisor
import oanda_gpt_prod_live_account_manager as gpt_live
import oanda_primary_challenger_live_account_manager as primary_live
import oanda_tech_prod_live_account_manager as tech_live
import oanda_technical_account_manager_auto as tech_base


OUT_DIR = advisor.SCRIPT_DIR / "data" / "live_day_monitor"
OUT_CSV = OUT_DIR / "live_day_monitor.csv"
LATEST_JSON = OUT_DIR / "latest_status.json"
ADVISOR_JSON = OUT_DIR / "latest_advisor_note.json"
ADVISOR_MD = OUT_DIR / "latest_advisor_note.md"
ADVISOR_METRICS_CSV = OUT_DIR / "advisor_metrics_24h.csv"

SCOUT_ROOT = advisor.SCRIPT_DIR / "data" / "technical_scout_manager"
SCOUT_SCAN_PATHS = {
    "primary": SCOUT_ROOT / "account_live_primary_challenger_scout" / "event_scan_summary.csv",
    "tech": SCOUT_ROOT / "account_live_tech_broad_regime_scout" / "event_scan_summary.csv",
}
SCOUT_AUDIT_PATHS = {
    "primary": SCOUT_ROOT / "account_live_primary_challenger_scout" / "scout_audit_ledger.csv",
    "tech": SCOUT_ROOT / "account_live_tech_broad_regime_scout" / "scout_audit_ledger.csv",
}

LOCALIZED_CLUSTER_CURRENCIES = {
    "ZAR",
    "TRY",
    "MXN",
    "NOK",
    "SEK",
    "CZK",
    "HKD",
    "THB",
    "CNH",
    "PLN",
    "HUF",
}

DAILY_RECAP_PATHS = {
    "primary": SCOUT_ROOT / "account_live_primary_challenger_scout" / "latest_daily_recap.json",
    "tech": SCOUT_ROOT / "account_live_tech_broad_regime_scout" / "latest_daily_recap.json",
}

GPT_RESEARCH_JSON = (
    advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_prod_live"
    / "latest_research_monitor.json"
)

TRAINER_REPORT_JSON = (
    advisor.SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_trainer_reporting_extensions.json"
)
TRAINER_RESEARCH_STATE_JSON = (
    advisor.SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "research_state.json"
)
TRAINER_EXPERIMENT_LEDGER_CSV = (
    advisor.SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "experiment_ledger.csv"
)
TRAINER_RESEARCH_QUEUE_JSONL = (
    advisor.SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "model_lifecycle"
    / "research_queue.jsonl"
)

TRAINER_REPORT_DIR = advisor.SCRIPT_DIR / "data" / "oanda_training_manager" / "reports"
LATEST_ACCOUNT_IMPROVEMENT_SEED_JSON = TRAINER_REPORT_DIR / "latest_weekend_account_improvement_seed.json"
TRAINER_DUPLICATE_VALIDATION_JSON = TRAINER_REPORT_DIR / "latest_trainer_duplicate_validation_report.json"
TRAINER_DUPLICATE_VALIDATION_CSV = TRAINER_REPORT_DIR / "trainer_duplicate_validation_report.csv"
MODEL_TEST_ARTIFACTS = {
    "ensemble_summary": advisor.SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "ensemble_research"
    / "latest_ensemble_summary.json",
    "pre_spike_lead": TRAINER_REPORT_DIR / "latest_pre_spike_lead_backtest.json",
    "pre_spike_trainer_focus": TRAINER_REPORT_DIR / "latest_pre_spike_lead_trainer_focus.json",
    "missed_spike_replay": TRAINER_REPORT_DIR / "latest_spike_missed_move_backtest.json",
    "missed_spike_delay_compare": TRAINER_REPORT_DIR / "latest_spike_missed_delay_compare.json",
    "missed_spike_tight_trail": TRAINER_REPORT_DIR / "latest_spike_missed_tight_trail_compare.json",
    "missed_spike_subsets": TRAINER_REPORT_DIR / "latest_missed_spike_subset_report.json",
    "missed_spike_capture_gap": TRAINER_REPORT_DIR / "latest_missed_spike_capture_gap_report.json",
    "scout_near_pass_replay": TRAINER_REPORT_DIR / "latest_scout_near_pass_replay.json",
    "technical_exhaustion": TRAINER_REPORT_DIR / "latest_technical_exhaustion_evidence.json",
    "model_metrics": TRAINER_REPORT_DIR / "latest_model_metrics.json",
    "arima_challengers": TRAINER_REPORT_DIR / "latest_arima_challengers.json",
    "trainer_duplicate_validation": TRAINER_DUPLICATE_VALIDATION_JSON,
}

ERROR_LOGS = {
    "gpt": advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_prod_live"
    / "errors.log",
    "primary": SCOUT_ROOT / "account_live_primary_challenger_scout" / "errors.log",
    "tech": SCOUT_ROOT / "account_live_tech_broad_regime_scout" / "errors.log",
    "canary": SCOUT_ROOT / "account_dum_canary_model_candidate" / "errors.log",
}

MANAGER_STATE_FILES = {
    "gpt": advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_prod_live"
    / "state.json",
    "tech": SCOUT_ROOT / "account_live_tech_broad_regime_scout" / "state.json",
    "primary": SCOUT_ROOT / "account_live_primary_challenger_scout" / "state.json",
    "canary": SCOUT_ROOT / "account_dum_canary_model_candidate" / "state.json",
}

SCOUT_SCAN_STALE_AFTER_MINUTES = 10.0
DAILY_REPORT_STALE_AFTER_MINUTES = float(
    getattr(tech_base, "AUTO_DAILY_REPORT_INTERVAL_MINUTES", 30)
) + 5.0
TRAINER_REPORT_REFRESH_STALE_MINUTES = 20.0
TRAINER_HEARTBEAT_REFRESH_STALE_MINUTES = 15.0
MANAGER_STATE_STALE_AFTER_MINUTES = 45.0
SYSTEM_MEMORY_FREE_WARN_MB = 2048.0
SYSTEM_MEMORY_USED_WARN_PCT = 92.0
CORE_TRAINER_MEMORY_WARN_MB = 7000.0
RECENT_ACCOUNT_TRANSACTION_LOOKBACK_IDS = 60
RECENT_ACCOUNT_EXIT_LIMIT = 5
RECENT_ACCOUNT_ACTION_LIMIT = 8
PENDING_ORDER_DETAIL_LIMIT = 8
RECENT_ACCOUNT_EXIT_MAX_AGE_MINUTES = 24 * 60


def account_advisor_only_mode() -> bool:
    return str(os.environ.get("OANDA_ACCOUNT_ADVISOR_ONLY") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

CSV_FIELDS = [
    "time_utc",
    "time_ny",
    "gpt_pid",
    "tech_pid",
    "primary_pid",
    "gpt_nav",
    "gpt_balance",
    "gpt_unrealized",
    "gpt_margin_used",
    "gpt_open_trades",
    "tech_nav",
    "tech_balance",
    "tech_unrealized",
    "tech_margin_used",
    "tech_open_trades",
    "primary_nav",
    "primary_balance",
    "primary_unrealized",
    "primary_margin_used",
    "primary_open_trades",
    "gpt_research_verdict",
    "gpt_research_issue_count",
    "primary_scan_time_ny",
    "primary_top_candidate",
    "primary_top_reject_reason",
    "tech_scan_time_ny",
    "tech_top_candidate",
    "tech_top_reject_reason",
    "status",
    "error",
]

ADVISOR_METRICS_FIELDS = [
    "time_utc",
    "time_ny",
    "status",
    "gpt_nav",
    "gpt_open_trades",
    "gpt_margin_pct",
    "tech_nav",
    "tech_open_trades",
    "tech_margin_pct",
    "primary_nav",
    "primary_open_trades",
    "primary_margin_pct",
    "daily_date_ny",
    "daily_age_minutes",
    "daily_market_rows",
    "daily_missed_rows",
    "daily_captured_rows",
    "daily_unique_missed_rows",
    "daily_missed_value_usd",
    "daily_missed_value_rows",
    "daily_near_value_usd",
    "daily_near_value_count",
    "daily_top_missed_pair",
    "daily_top_miss_reason",
    "daily_top_miss_gate",
    "daily_top_value_instrument",
    "daily_top_near_value_instrument",
    "tech_scout_recent_rows",
    "tech_scout_recent_accepted",
    "tech_scout_recent_passed",
    "tech_scout_recent_max_score",
    "tech_scout_recent_max_cluster",
    "tech_scout_near_value_usd",
    "tech_scout_near_value_count",
    "tech_scout_near_score_count",
    "tech_scout_near_ratio_count",
    "primary_scout_recent_rows",
    "primary_scout_recent_accepted",
    "primary_scout_recent_passed",
    "primary_scout_recent_max_score",
    "primary_scout_recent_max_cluster",
    "primary_scout_near_value_usd",
    "primary_scout_near_value_count",
    "primary_scout_near_score_count",
    "primary_scout_near_ratio_count",
    "trainer_heartbeat_age_minutes",
    "trainer_queue_rows",
    "trainer_pending_specs",
    "trainer_completed_specs",
    "trainer_current_source",
    "trainer_current_model",
    "trainer_current_target",
    "trainer_current_outcome",
    "trainer_current_status",
    "live_miss_value_pending",
    "live_miss_value_completed",
    "live_miss_ev_near_pending",
    "live_miss_ev_near_completed",
    "live_miss_unknown_pending",
    "live_miss_unknown_completed",
    "live_miss_value_gates",
    "live_miss_ev_near_gates",
    "live_miss_unknown_gates",
    "active_research_processes",
    "ensemble_age_minutes",
    "pre_spike_age_minutes",
    "missed_spike_replay_age_minutes",
]


def forex_weekend_market_closed_now() -> bool:
    """Return true during the normal Friday-close/Sunday-reopen quiet window."""
    now = advisor.ny_now()
    weekday = now.weekday()
    if weekday == 4 and now.hour >= 17:
        return True
    if weekday == 5:
        return True
    if weekday == 6 and now.hour < 17:
        return True
    return False


def iso_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _transaction_trade_id(tx: dict[str, Any]) -> Any:
    opened = tx.get("tradeOpened")
    if isinstance(opened, dict) and opened.get("tradeID"):
        return opened.get("tradeID")
    closed = tx.get("tradesClosed")
    if isinstance(closed, list) and closed and isinstance(closed[0], dict):
        return closed[0].get("tradeID")
    reduced = tx.get("tradeReduced")
    if isinstance(reduced, dict) and reduced.get("tradeID"):
        return reduced.get("tradeID")
    return tx.get("tradeID")


def recent_account_actions(client: Any, account: dict[str, Any]) -> list[dict[str, Any]]:
    """Summarize recent broker-recorded account actions using read-only transactions."""
    if not hasattr(client, "get_transactions_since"):
        return []
    try:
        last_id = int(str(account.get("lastTransactionID") or "0"))
    except Exception:
        return []
    if last_id <= 0:
        return []
    since_id = max(0, last_id - RECENT_ACCOUNT_TRANSACTION_LOOKBACK_IDS)
    try:
        payload = client.get_transactions_since(str(since_id))
    except Exception:
        return []
    transactions = payload.get("transactions") if isinstance(payload, dict) else []
    if not isinstance(transactions, list):
        return []
    actions: list[dict[str, Any]] = []
    tracked_order_types = {
        "MARKET_ORDER",
        "LIMIT_ORDER",
        "STOP_ORDER",
        "TAKE_PROFIT_ORDER",
        "STOP_LOSS_ORDER",
        "TRAILING_STOP_LOSS_ORDER",
        "MARKET_IF_TOUCHED_ORDER",
    }
    for tx in transactions:
        if not isinstance(tx, dict):
            continue
        action_age = age_minutes_from_iso(tx.get("time"))
        if not isinstance(action_age, (int, float)) or action_age > RECENT_ACCOUNT_EXIT_MAX_AGE_MINUTES:
            continue
        tx_type = str(tx.get("type") or "")
        action = ""
        closed = tx.get("tradesClosed")
        reduced = tx.get("tradeReduced")
        if tx_type == "ORDER_FILL":
            if tx.get("tradeOpened"):
                action = "open"
            elif closed:
                action = "close"
            elif reduced:
                action = "reduce"
            else:
                action = "fill"
        elif tx_type == "ORDER_CANCEL":
            action = "order_cancel"
        elif tx_type in tracked_order_types:
            action = "order_create"
        else:
            continue
        row = {
            "id": tx.get("id"),
            "time": tx.get("time"),
            "age_minutes": action_age,
            "type": tx_type,
            "action": action,
            "instrument": tx.get("instrument"),
            "reason": tx.get("reason"),
            "price": tx.get("price"),
            "pl": tx.get("pl"),
            "units": tx.get("units"),
            "orderID": tx.get("orderID") or tx.get("orderIDReplaced") or tx.get("orderIDCancelled"),
            "tradeID": _transaction_trade_id(tx),
        }
        if isinstance(closed, list) and closed and isinstance(closed[0], dict):
            row["tradeID"] = closed[0].get("tradeID")
            row["realizedPL"] = closed[0].get("realizedPL")
        elif isinstance(reduced, dict):
            row["tradeID"] = reduced.get("tradeID")
            row["realizedPL"] = reduced.get("realizedPL")
        opened = tx.get("tradeOpened")
        if isinstance(opened, dict):
            row["tradeID"] = opened.get("tradeID")
            row["openedUnits"] = opened.get("units")
            row["initialMarginRequired"] = opened.get("initialMarginRequired")
        actions.append(row)
    return actions[-RECENT_ACCOUNT_ACTION_LIMIT:]


def recent_account_exits(client: Any, account: dict[str, Any]) -> list[dict[str, Any]]:
    """Summarize recent broker-recorded exits using read-only transactions."""
    exits = [
        row
        for row in recent_account_actions(client, account)
        if row.get("action") in {"close", "reduce"}
    ]
    return exits[-RECENT_ACCOUNT_EXIT_LIMIT:]


def pending_account_orders(client: Any) -> list[dict[str, Any]]:
    """Return compact pending-order details using the existing read-only client."""
    if not hasattr(client, "request") or not getattr(client, "cfg", None):
        return []
    try:
        account_id = client.cfg.oanda_account_id
        payload = client.request("GET", f"/v3/accounts/{account_id}/pendingOrders")
    except Exception:
        return []
    orders = payload.get("orders") if isinstance(payload, dict) else []
    if not isinstance(orders, list):
        return []
    compact: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        compact.append(
            {
                "id": order.get("id"),
                "type": order.get("type"),
                "instrument": order.get("instrument"),
                "tradeID": order.get("tradeID"),
                "price": order.get("price"),
                "distance": order.get("distance"),
                "units": order.get("units"),
                "state": order.get("state"),
                "timeInForce": order.get("timeInForce"),
            }
        )
    return compact


def tail_csv_row(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        return rows[-1] if rows else {}
    except Exception as exc:
        return {"error": str(exc)[:240]}


def tail_csv_rows(path: Path, max_rows: int = 10000) -> list[dict[str, Any]]:
    """Read a bounded CSV tail while preserving the header.

    Scout audit ledgers can grow quickly during volatile sessions, so advisor
    reporting should not read the whole file on every poll.
    """
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            header = handle.readline()
            if not header:
                return []
            tail_lines = deque(handle, maxlen=max(1, int(max_rows)))
        return list(csv.DictReader([header, *tail_lines]))
    except Exception as exc:
        return [{"error": str(exc)[:240]}]


def read_jsonl_rows(path: Path, max_rows: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
        if max_rows is not None and max_rows >= 0:
            return rows[-max_rows:]
        return rows
    except Exception as exc:
        return [{"error": str(exc)[:240]}]


def _synthetic_read_error_row(row: dict[str, Any]) -> bool:
    """Return true only for rows created by local read helpers on failure."""
    if not isinstance(row, dict) or not row.get("error"):
        return False
    return not any(key in row for key in ("time_utc", "source", "spec_hash", "experiment_id", "status"))


def recent_regime_lock_seed_summary(
    *,
    max_rows: int = 800,
    max_age_minutes: float = 180.0,
) -> dict[str, Any]:
    """Summarize recent research specs seeded from scout regime-lock misses."""
    rows = read_jsonl_rows(TRAINER_RESEARCH_QUEUE_JSONL, max_rows=max_rows)
    if rows and _synthetic_read_error_row(rows[-1]):
        return {"count": 0, "error": rows[-1].get("error", "")}
    now = dt.datetime.now(dt.timezone.utc)
    profile_counts: Counter[str] = Counter()
    instrument_counts: Counter[str] = Counter()
    latest_time: dt.datetime | None = None
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        spec = row.get("spec") if isinstance(row.get("spec"), dict) else {}
        profile = str(spec.get("specialist_profile") or "")
        if "current_regime_lock" not in profile:
            continue
        raw_time = str(row.get("time_utc") or "").strip()
        try:
            parsed = dt.datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
        except Exception:
            parsed = None
        if parsed is not None:
            age = (now - parsed.astimezone(dt.timezone.utc)).total_seconds() / 60.0
            if age > max_age_minutes:
                continue
            latest_time = max(latest_time or parsed, parsed)
        count += 1
        if profile:
            profile_counts[profile] += 1
        instruments = spec.get("instrument_whitelist")
        if isinstance(instruments, list):
            for instrument in instruments:
                inst = str(instrument or "").strip().upper()
                if inst:
                    instrument_counts[inst] += 1
    return {
        "count": count,
        "latest_age_minutes": age_minutes_from_iso(latest_time.isoformat()) if latest_time else None,
        "top_profiles": [{"name": name, "count": cnt} for name, cnt in profile_counts.most_common(3)],
        "top_instruments": [
            {"instrument": name, "count": cnt}
            for name, cnt in instrument_counts.most_common(6)
        ],
    }


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc)[:240]}
    return obj if isinstance(obj, dict) else {}


def tail_text(path: Path, limit: int = 2200) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"read_error: {exc}"
    return text[-limit:]


def error_log_snapshot(path: Path, limit: int = 2200) -> dict[str, Any]:
    """Return error-log context without treating stale traces as new failures."""
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "size_bytes": 0,
            "mtime_utc": "",
            "mtime_ny": "",
            "age_minutes": "",
            "tail": "",
        }
    try:
        stat = path.stat()
        mtime = dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc)
        age = max(0.0, (dt.datetime.now(dt.timezone.utc) - mtime).total_seconds() / 60.0)
        return {
            "path": str(path),
            "exists": True,
            "size_bytes": stat.st_size,
            "mtime_utc": mtime.isoformat(),
            "mtime_ny": advisor.iso_ny(mtime),
            "age_minutes": round(age, 2),
            "tail": tail_text(path, limit=limit),
        }
    except Exception as exc:
        return {
            "path": str(path),
            "exists": True,
            "size_bytes": "",
            "mtime_utc": "",
            "mtime_ny": "",
            "age_minutes": "",
            "tail": f"read_error: {exc}",
        }


def age_minutes_from_iso(value: Any) -> float | str:
    if not isinstance(value, str) or not value:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    age = (dt.datetime.now(dt.timezone.utc) - parsed.astimezone(dt.timezone.utc)).total_seconds() / 60.0
    return round(max(0.0, age), 2)


def file_age_minutes(path: Path) -> float | str:
    try:
        if not path.exists():
            return ""
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    except Exception:
        return ""
    age = (dt.datetime.now(dt.timezone.utc) - mtime).total_seconds() / 60.0
    return round(max(0.0, age), 2)


def process_status() -> dict[str, Any]:
    scripts = {
        "gpt": "oanda_gpt_prod_live_account_manager.py",
        "tech": "oanda_tech_prod_live_account_manager.py",
        "primary": "oanda_primary_challenger_live_account_manager.py",
        "trainer": "oanda_gpt_training_strategy_manager.py",
        "depth_collector": "oanda_depth_feature_collector.py",
        "canary": "oanda_dum_canary_executor.py",
        "day_monitor": "oanda_live_day_monitor.py",
    }
    status = {name: {"pid": "", "running": False} for name in scripts}

    def previous_process_status(reason: str) -> dict[str, Any]:
        previous = read_json(LATEST_JSON).get("processes")
        if not isinstance(previous, dict):
            status["_error"] = reason[:500]
            return status
        fallback = {name: dict(previous.get(name) or {}) for name in scripts}
        for name in scripts:
            fallback.setdefault(name, {"pid": "", "running": False})
            fallback[name]["snapshot_stale"] = True
        fallback["_warning"] = reason[:500]
        return fallback

    ps_command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match 'python' -and $_.CommandLine -match 'oanda_.*live_account_manager|oanda_live_day_monitor.py|oanda_gpt_training_strategy_manager.py|oanda_depth_feature_collector.py|oanda_dum_canary_executor.py' } | "
        "Select-Object ProcessId,"
        "@{Name='WorkingSetMB';Expression={[math]::Round(($_.WorkingSetSize / 1MB),1)}},"
        "@{Name='CpuSeconds';Expression={[math]::Round(((([double]$_.UserModeTime + [double]$_.KernelModeTime) / 10000000)),1)}},"
        "CommandLine | ConvertTo-Json -Compress"
    )
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_command],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return previous_process_status((proc.stderr or proc.stdout or "").strip())
        parsed = json.loads(proc.stdout)
        if isinstance(parsed, dict):
            parsed = [parsed]
        if not isinstance(parsed, list):
            return status
        for item in parsed:
            if not isinstance(item, dict):
                continue
            cmd = str(item.get("CommandLine") or "")
            for name, script in scripts.items():
                if script in cmd and (name != "day_monitor" or "--loop" in cmd):
                    status[name] = {
                        "pid": str(item.get("ProcessId") or ""),
                        "running": True,
                        "working_set_mb": item.get("WorkingSetMB", ""),
                        "cpu_seconds": item.get("CpuSeconds", ""),
                        "command": cmd,
                    }
    except Exception as exc:
        return previous_process_status(str(exc))
    return status


def system_memory_status() -> dict[str, Any]:
    """Return compact host memory pressure for research/advisor decisions."""

    class MemoryStatusEx(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    try:
        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(MemoryStatusEx)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise ctypes.WinError()
        total_mb = round(float(status.ullTotalPhys) / (1024 * 1024), 1)
        available_mb = round(float(status.ullAvailPhys) / (1024 * 1024), 1)
        used_pct = round(float(status.dwMemoryLoad), 1)
        available_pct = round((available_mb / total_mb) * 100.0, 1) if total_mb else ""
        return {
            "total_mb": total_mb,
            "available_mb": available_mb,
            "available_pct": available_pct,
            "used_pct": used_pct,
            "warn_free_mb": SYSTEM_MEMORY_FREE_WARN_MB,
            "warn_used_pct": SYSTEM_MEMORY_USED_WARN_PCT,
        }
    except Exception as exc:
        return {"error": str(exc)[:500]}


def research_process_status() -> list[dict[str, Any]]:
    """Return active standalone research/backtest Python processes.

    This intentionally excludes the always-on trainer manager.  The goal is to
    make long manual/scheduled model tests visible in the advisor note without
    managing or interrupting them.
    """
    ps_command = (
        "$pattern = 'oanda_pre_spike_lead_backtest.py|oanda_spike_missed_move_backtest.py|"
        "oanda_ensemble_candidate_backtester.py|oanda_.*backtest.py|arima.*sweep|oanda_.*arima.*py'; "
        "@(Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match 'python' -and $_.CommandLine -match $pattern } | "
        "Select-Object ProcessId,CreationDate,"
        "@{Name='AgeMinutes';Expression={[math]::Round(((Get-Date)-$_.CreationDate).TotalMinutes,2)}},"
        "@{Name='WorkingSetMB';Expression={[math]::Round(($_.WorkingSetSize / 1MB),1)}},"
        "@{Name='CpuSeconds';Expression={[math]::Round(((([double]$_.UserModeTime + [double]$_.KernelModeTime) / 10000000)),1)}},"
        "CommandLine) | ConvertTo-Json -Compress"
    )
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_command],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return []
        parsed = json.loads(proc.stdout)
    except Exception:
        return []
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return []
    grouped: dict[str, dict[str, Any]] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        cmd = str(item.get("CommandLine") or "")
        if not cmd:
            continue
        parts = cmd.replace('"', "").split()
        script = next((part for part in parts if part.endswith(".py")), "")
        script_name = Path(script).name if script else ""
        script_pos = cmd.lower().find(script.lower()) if script else -1
        logical_command = cmd[script_pos:] if script_pos >= 0 else cmd
        key = logical_command.lower()
        pid = str(item.get("ProcessId") or "")
        if key not in grouped:
            grouped[key] = {
                "pid": pid,
                "pids": [pid] if pid else [],
                "process_count": 1,
                "script": script_name,
                "creation_date": str(item.get("CreationDate") or ""),
                "age_minutes": item.get("AgeMinutes", ""),
                "working_set_mb": item.get("WorkingSetMB", ""),
                "cpu_seconds": item.get("CpuSeconds", ""),
                "command": logical_command[:900],
            }
        else:
            grouped[key]["process_count"] = int(grouped[key].get("process_count") or 1) + 1
            try:
                grouped[key]["age_minutes"] = max(
                    float(grouped[key].get("age_minutes") or 0.0),
                    float(item.get("AgeMinutes") or 0.0),
                )
            except Exception:
                pass
            try:
                grouped[key]["working_set_mb"] = round(
                    float(grouped[key].get("working_set_mb") or 0.0)
                    + float(item.get("WorkingSetMB") or 0.0),
                    1,
                )
            except Exception:
                pass
            try:
                grouped[key]["cpu_seconds"] = round(
                    float(grouped[key].get("cpu_seconds") or 0.0)
                    + float(item.get("CpuSeconds") or 0.0),
                    1,
                )
            except Exception:
                pass
            if pid:
                grouped[key].setdefault("pids", []).append(pid)
                grouped[key]["pid"] = ",".join(grouped[key]["pids"])
    return list(grouped.values())


def account_statuses() -> dict[str, Any]:
    gpt_live.apply_live_runtime_overrides()
    tech_live.apply_live_runtime_overrides()
    primary_live.apply_live_runtime_overrides()

    gpt_cfg = gpt_live.build_live_config(
        advisor.BotConfig.load(),
        execute_requested=False,
        confirmation_ok=True,
        scan_on_launch_live=False,
    )
    tech_cfg_base = tech_base.BotConfig.load()
    configs = {
        "gpt": (advisor, gpt_cfg),
        "tech": (tech_base, tech_live.build_live_tech_config(tech_cfg_base, execute=False, dry_run=True)),
        "primary": (
            tech_base,
            primary_live.build_live_primary_config(tech_cfg_base, execute=False, dry_run=True),
        ),
    }
    out: dict[str, Any] = {}
    for name, (mod, cfg) in configs.items():
        try:
            client = mod.OandaClient(cfg)
            account = client.get_account_summary()
            trades = client.get_open_trades()
            pending_orders = pending_account_orders(client)
            recent_actions = recent_account_actions(client, account)
            recent_exits = [
                row
                for row in recent_actions
                if row.get("action") in {"close", "reduce"}
            ][-RECENT_ACCOUNT_EXIT_LIMIT:]
            out[name] = {
                "lane": cfg.account_lane,
                "account_id_tail": str(cfg.oanda_account_id)[-4:],
                "nav": mod.safe_float(account.get("NAV"), 0.0),
                "balance": mod.safe_float(account.get("balance"), 0.0),
                "unrealized": mod.safe_float(account.get("unrealizedPL"), 0.0),
                "margin_used": mod.safe_float(account.get("marginUsed"), 0.0),
                "last_transaction_id": account.get("lastTransactionID"),
                "pending_orders": len(pending_orders),
                "pending_order_details": pending_orders[:PENDING_ORDER_DETAIL_LIMIT],
                "recent_actions": recent_actions,
                "recent_exits": recent_exits,
                "open_trades": len(trades),
                "trades": [
                    {
                        "id": trade.get("id"),
                        "instrument": trade.get("instrument"),
                        "units": trade.get("currentUnits"),
                        "unrealizedPL": trade.get("unrealizedPL"),
                    }
                    for trade in trades
                ],
            }
        except Exception as exc:
            out[name] = {"error": str(exc)[:700]}
    return out


def scout_statuses() -> dict[str, Any]:
    out = {}
    weekend_new_entries_blocked = tech_base.volatile_weekend_block_new_scouts_now()
    weekend_market_closed = forex_weekend_market_closed_now()
    for name, path in SCOUT_SCAN_PATHS.items():
        row = tail_csv_row(path)
        scan_age = age_minutes_from_iso(row.get("time_ny", ""))
        scan_stale = (
            isinstance(scan_age, (int, float))
            and scan_age > SCOUT_SCAN_STALE_AFTER_MINUTES
        )
        scan_pause_reason = ""
        scan_expected_paused = False
        if weekend_market_closed:
            scan_expected_paused = True
            scan_pause_reason = "forex_weekend_market_closed"
        elif name == "tech" and weekend_new_entries_blocked:
            scan_expected_paused = True
            scan_pause_reason = "volatile_weekend_new_entries_blocked"
        out[name] = {
            "path": str(path),
            "time_ny": row.get("time_ny", ""),
            "age_minutes": scan_age,
            "stale_after_minutes": SCOUT_SCAN_STALE_AFTER_MINUTES,
            "scan_stale": scan_stale,
            "scan_expected_paused": scan_expected_paused,
            "scan_pause_reason": scan_pause_reason,
            "signals": row.get("signals", ""),
            "themes": row.get("themes", ""),
            "pressure_watch": row.get("pressure_watch", ""),
            "pressure_trade": row.get("pressure_trade", ""),
            "scout_attempts": row.get("scout_attempts", ""),
            "top_candidate": row.get("top_candidate", ""),
            "top_reject_reason": row.get("top_reject_reason", ""),
            "error": row.get("error", ""),
        }
    return out


def _top_recap_bucket(obj: dict[str, Any], name: str, limit: int = 3) -> list[dict[str, Any]]:
    bucket = obj.get(name)
    if not isinstance(bucket, dict):
        return []
    rows = []
    for key, metrics in bucket.items():
        if not isinstance(metrics, dict):
            continue
        rows.append(
            {
                "name": key,
                "closed": metrics.get("closed", 0),
                "losses": metrics.get("losses", 0),
                "wins": metrics.get("wins", 0),
                "loss_rate": metrics.get("loss_rate", 0.0),
                "realized_pl": metrics.get("realized_pl", 0.0),
            }
        )
    return sorted(rows, key=lambda row: float(row.get("realized_pl") or 0.0))[:limit]


def daily_recap_statuses() -> dict[str, Any]:
    out = {}
    current_date_ny = advisor.ny_now().date().isoformat()
    for name, path in DAILY_RECAP_PATHS.items():
        obj = read_json(path)
        totals = obj.get("totals") if isinstance(obj.get("totals"), dict) else {}
        dashboard = obj.get("dashboard") if isinstance(obj.get("dashboard"), dict) else {}
        stop_audit = obj.get("tighter_stop_audit") if isinstance(obj.get("tighter_stop_audit"), dict) else {}
        demotions = obj.get("model_score_demotions") if isinstance(obj.get("model_score_demotions"), dict) else {}
        reentry = obj.get("reentry_behavior") if isinstance(obj.get("reentry_behavior"), dict) else {}
        date_ny = obj.get("date_ny", "")
        generated_utc = obj.get("generated_utc", "")
        out[name] = {
            "path": str(path),
            "exists": path.exists(),
            "date_ny": date_ny,
            "current_date_ny": current_date_ny,
            "date_stale": bool(date_ny and date_ny != current_date_ny),
            "generated_utc": generated_utc,
            "age_minutes": age_minutes_from_iso(generated_utc),
            "mode": dashboard.get("mode", ""),
            "bad_day": bool(dashboard.get("bad_day", False)),
            "failsafe_active": bool(dashboard.get("failsafe_active", False)),
            "failsafe_triggers": dashboard.get("failsafe_triggers") or [],
            "model_score_demotions_active": bool(dashboard.get("model_score_demotions_active", False)),
            "model_score_demotions_enforced": bool(dashboard.get("model_score_demotions_enforced", False)),
            "demoted_score_buckets": dashboard.get("demoted_score_buckets") or [],
            "closed": totals.get("closed", 0),
            "wins": totals.get("wins", 0),
            "losses": totals.get("losses", 0),
            "loss_rate": totals.get("loss_rate", 0.0),
            "realized_pl": totals.get("realized_pl", 0.0),
            "tighter_stop_most_helpful_pips": stop_audit.get("most_helpful_candidate_pips", ""),
            "tighter_stop_audited_losers": stop_audit.get("audited_losing_trades", 0),
            "losses_with_reentry": reentry.get("losses_with_reentry", 0),
            "demotion_min_closed_trades": demotions.get("min_closed_trades", ""),
            "demotion_max_loss_rate": demotions.get("max_loss_rate", ""),
            "worst_pairs": _top_recap_bucket(obj, "losses_by_pair"),
            "worst_themes": _top_recap_bucket(obj, "losses_by_theme"),
            "error": obj.get("error", ""),
        }
    return out


def manager_state_statuses() -> dict[str, Any]:
    """Read local manager state heartbeats without touching broker state."""
    out: dict[str, Any] = {}
    for name, path in MANAGER_STATE_FILES.items():
        meta: dict[str, Any] = {
            "path": str(path),
            "exists": path.exists(),
            "mtime_age_minutes": file_age_minutes(path),
            "last_monitor_utc": "",
            "last_monitor_age_minutes": "",
            "last_reconciled_utc": "",
            "last_reconciled_age_minutes": "",
            "last_event_scan_utc": "",
            "last_event_scan_age_minutes": "",
            "live_crisis_mode": "",
            "error": "",
        }
        if not path.exists():
            out[name] = meta
            continue
        try:
            obj = read_json(path)
            meta["last_monitor_utc"] = str(obj.get("last_monitor_utc") or "")
            meta["last_monitor_age_minutes"] = age_minutes_from_iso(meta["last_monitor_utc"])
            meta["last_reconciled_utc"] = str(obj.get("last_reconciled_utc") or "")
            meta["last_reconciled_age_minutes"] = age_minutes_from_iso(meta["last_reconciled_utc"])
            meta["last_event_scan_utc"] = str(obj.get("last_event_scan_utc") or "")
            meta["last_event_scan_age_minutes"] = age_minutes_from_iso(meta["last_event_scan_utc"])
            crisis = obj.get("live_crisis_mode")
            if isinstance(crisis, dict):
                meta["live_crisis_mode"] = str(
                    crisis.get("mode")
                    or crisis.get("status")
                    or crisis.get("review_mode")
                    or ""
                )
                if crisis.get("trigger_count") not in ("", None):
                    meta["live_crisis_triggers"] = crisis.get("trigger_count")
            else:
                meta["live_crisis_mode"] = str(crisis or "")
        except Exception as exc:
            meta["error"] = str(exc)[:300]
        out[name] = meta
    return out


def gpt_research_status() -> dict[str, Any]:
    obj = read_json(GPT_RESEARCH_JSON)
    review = obj.get("review") if isinstance(obj.get("review"), dict) else {}
    row = obj.get("row") if isinstance(obj.get("row"), dict) else {}
    issues = review.get("issues") if isinstance(review.get("issues"), list) else []
    return {
        "path": str(GPT_RESEARCH_JSON),
        "status": obj.get("status", ""),
        "time_utc": row.get("time_utc", ""),
        "time_ny": row.get("time_ny", ""),
        "age_minutes": age_minutes_from_iso(row.get("time_utc", "")),
        "local_verdict": review.get("local_verdict") or row.get("local_verdict", ""),
        "portfolio_bias": review.get("portfolio_bias") or row.get("portfolio_bias", ""),
        "portfolio_mode": review.get("portfolio_mode") or row.get("portfolio_mode", ""),
        "portfolio_usd_thesis": review.get("portfolio_usd_thesis", ""),
        "summary_usd_bias": review.get("summary_usd_bias") or row.get("summary_usd_bias", ""),
        "currency_table_usd_bias": review.get("currency_table_usd_bias")
        or row.get("currency_table_usd_bias", ""),
        "candidate_count": row.get("candidate_count", ""),
        "orders_to_execute_count": row.get("orders_to_execute_count", ""),
        "open_position_action_count": row.get("open_position_action_count", ""),
        "blocked_permission_count": row.get("blocked_permission_count", ""),
        "underdeployment_reason": row.get("underdeployment_reason", ""),
        "issue_count": len(issues) if issues else row.get("issue_count", ""),
        "issues": issues,
        "blocked_orders": review.get("blocked_orders") or [],
        "blocked_event_permissions": review.get("blocked_event_permissions") or [],
        "error": obj.get("error", ""),
    }


def daily_report_status() -> dict[str, Any]:
    """Best-effort shared daily comparison report refresh/status.

    The day monitor is the natural read-only cadence owner for advisor
    snapshots.  Keep this guarded and non-fatal: report generation can be slow,
    but it must never block account/scout health reporting from being written.
    """
    try:
        status_path = tech_base._auto_report_status_path()
        before_mtime = status_path.stat().st_mtime if status_path.exists() else 0.0
        tech_base.maybe_auto_daily_move_report(account_lane="day_monitor", writer=True, force=False)
        after_mtime = status_path.stat().st_mtime if status_path.exists() else 0.0
        status = read_json(status_path)
        if not isinstance(status, dict):
            status = {}
        latest_dir = tech_base._auto_report_root() / tech_base._auto_report_today_ny()
        return {
            "status_path": str(status_path),
            "latest_report_dir": str(latest_dir),
            "last_run_utc": status.get("last_run_utc", ""),
            "date_ny": status.get("date_ny", ""),
            "refreshed_this_poll": bool(after_mtime and after_mtime != before_mtime),
            "age_minutes": age_minutes_from_iso(status.get("last_run_utc", "")),
            "event_counts": status.get("event_counts") if isinstance(status.get("event_counts"), dict) else {},
            "error": "",
        }
    except Exception as exc:
        return {
            "status_path": "",
            "latest_report_dir": "",
            "last_run_utc": "",
            "date_ny": "",
            "refreshed_this_poll": False,
            "age_minutes": "",
            "event_counts": {},
            "error": str(exc)[:700],
        }


def _float_or_none(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def _account_watch_line(name: str, meta: dict[str, Any]) -> str:
    if meta.get("error"):
        return f"{name}: account_error={meta.get('error')}"
    nav = _float_or_none(meta.get("nav"))
    margin = _float_or_none(meta.get("margin_used"))
    open_trades = meta.get("open_trades", "")
    pending_orders = meta.get("pending_orders", "")
    margin_pct = ""
    if nav and margin is not None:
        margin_pct = f" margin={margin / nav * 100.0:.1f}%"
    trades = meta.get("trades") if isinstance(meta.get("trades"), list) else []
    trade_bits = []
    for trade in trades[:4]:
        trade_bits.append(
            f"{trade.get('instrument')} {trade.get('units')} uPL={trade.get('unrealizedPL')}"
        )
    trade_text = f" trades={'; '.join(trade_bits)}" if trade_bits else " trades=flat"
    exits = meta.get("recent_exits") if isinstance(meta.get("recent_exits"), list) else []
    exit_text = ""
    if exits:
        last_exit = exits[-1]
        reason = _short_text(last_exit.get("reason"), 34)
        inst = last_exit.get("instrument") or ""
        pl = last_exit.get("realizedPL") or last_exit.get("pl") or ""
        when = str(last_exit.get("time") or "")[11:19]
        age = _float_or_none(last_exit.get("age_minutes"))
        age_text = f" age={age:.0f}m" if age is not None else ""
        exit_text = f" lastExit={inst} {reason} pl={pl} t={when}Z{age_text}"
    nav_text = f"{nav:.4f}" if nav is not None else ""
    pending_text = f" pending={pending_orders}" if pending_orders not in ("", None) else ""
    return f"{name}: nav={nav_text}{margin_pct} open={open_trades}{pending_text}{trade_text}{exit_text}"


def _account_action_line(action: dict[str, Any]) -> str:
    action_name = str(action.get("action") or action.get("type") or "")
    tx_type = str(action.get("type") or "")
    inst = str(action.get("instrument") or "")
    units = action.get("units")
    if units in ("", None):
        units = action.get("openedUnits")
    price = action.get("price")
    pl = action.get("realizedPL")
    if pl in ("", None):
        pl = action.get("pl")
    reason = _short_text(action.get("reason"), 36)
    when = str(action.get("time") or "")[11:19]
    age = _float_or_none(action.get("age_minutes"))
    age_text = f" age={age:.0f}m" if age is not None else ""
    id_text = f"tx={action.get('id')}" if action.get("id") else ""
    order_text = f" order={action.get('orderID')}" if action.get("orderID") else ""
    trade_text = f" trade={action.get('tradeID')}" if action.get("tradeID") else ""
    price_text = f" price={price}" if price not in ("", None) else ""
    unit_text = f" units={units}" if units not in ("", None) else ""
    pl_text = f" pl={pl}" if pl not in ("", None) else ""
    reason_text = f" reason={reason}" if reason else ""
    type_text = f"/{tx_type}" if tx_type and tx_type != action_name else ""
    return (
        f"{when}Z{age_text} {action_name}{type_text} {inst}"
        f"{unit_text}{price_text}{pl_text}{trade_text}{order_text}"
        f"{reason_text} {id_text}"
    ).strip()


def _pending_order_line(order: dict[str, Any]) -> str:
    order_type = str(order.get("type") or "")
    inst = str(order.get("instrument") or "")
    trade = order.get("tradeID")
    price = order.get("price")
    distance = order.get("distance")
    units = order.get("units")
    state = order.get("state")
    bits = [order_type]
    if inst:
        bits.append(inst)
    if units not in ("", None):
        bits.append(f"units={units}")
    if price not in ("", None):
        bits.append(f"price={price}")
    if distance not in ("", None):
        bits.append(f"dist={distance}")
    if trade not in ("", None):
        bits.append(f"trade={trade}")
    if state:
        bits.append(f"state={state}")
    if order.get("id"):
        bits.append(f"id={order.get('id')}")
    return " ".join(bits)


def _currency_exposure_conflicts(name: str, meta: dict[str, Any]) -> list[str]:
    """Return report-only warnings for opposing same-currency exposure."""
    trades = meta.get("trades") if isinstance(meta.get("trades"), list) else []
    exposure: dict[str, dict[str, list[str]]] = {}
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        instrument = (
            str(trade.get("instrument") or "")
            .upper()
            .replace("/", "_")
            .replace("-", "_")
        )
        if "_" not in instrument:
            continue
        try:
            units = float(trade.get("units") or 0.0)
        except Exception:
            units = 0.0
        if units == 0:
            continue
        base, quote = instrument.split("_", 1)
        base_side, quote_side = ("long", "short") if units > 0 else ("short", "long")
        exposure.setdefault(base, {"long": [], "short": []})[base_side].append(instrument)
        exposure.setdefault(quote, {"long": [], "short": []})[quote_side].append(instrument)
    conflicts: list[str] = []
    for currency, sides in sorted(exposure.items()):
        longs = sorted(set(sides.get("long") or []))
        shorts = sorted(set(sides.get("short") or []))
        if not longs or not shorts:
            continue
        conflicts.append(
            f"{name} mixed {currency} exposure: "
            f"long via {','.join(longs[:3])}; short via {','.join(shorts[:3])}"
        )
    return conflicts[:4]


def _short_text(value: Any, max_len: int = 80) -> str:
    text = str(value or "").strip().replace("\n", " ")
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


def _fmt_num(value: Any, digits: int = 1) -> str:
    num = _float_or_none(value)
    if num is None:
        return ""
    return f"{num:.{digits}f}"


def _recent_rows_by_age(rows: list[dict[str, Any]], minutes: float, time_field: str = "time_utc") -> list[dict[str, Any]]:
    recent: list[dict[str, Any]] = []
    for row in rows:
        age = age_minutes_from_iso(row.get(time_field, ""))
        if not isinstance(age, (int, float)) or age > minutes:
            continue
        recent.append(row)
    return recent


def reconstructed_localized_cluster_opportunities(
    audit_rows: list[dict[str, Any]],
    *,
    min_cluster: int = 4,
    min_direction_cluster: int = 4,
    min_ratio: float = 1.35,
    min_score: float = 80.0,
) -> dict[str, Any]:
    """Advisor-only reconstruction of localized EM/high-spread basket opportunities."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in audit_rows:
        stamp = str(row.get("time_utc") or "")[:19]
        if stamp:
            groups.setdefault(stamp, []).append(row)

    candidates: list[dict[str, Any]] = []
    for stamp, rows in groups.items():
        currency_instruments: dict[str, set[str]] = {}
        currency_direction_instruments: dict[tuple[str, str], set[str]] = {}
        for row in rows:
            instrument = str(row.get("instrument") or "").upper().replace("/", "_")
            if "_" not in instrument:
                continue
            direction = str(row.get("direction") or "").upper()
            base, quote = instrument.split("_", 1)
            for currency in {base, quote} & LOCALIZED_CLUSTER_CURRENCIES:
                currency_instruments.setdefault(currency, set()).add(instrument)
                if direction in {"LONG", "SHORT"}:
                    currency_direction_instruments.setdefault((currency, direction), set()).add(instrument)

        for row in rows:
            if str(row.get("decision_stage") or "").strip() != "strict_pressure_gate":
                continue
            if str(row.get("status") or "").strip().lower() not in {"skipped", "rejected", "blocked"}:
                continue
            reason = str(row.get("reject_reason") or row.get("reason") or "")
            if "cluster instruments" not in reason:
                continue
            instrument = str(row.get("instrument") or "").upper().replace("/", "_")
            direction = str(row.get("direction") or "").upper()
            if "_" not in instrument or direction not in {"LONG", "SHORT"}:
                continue
            ratio = _float_or_none(row.get("move_to_spread_ratio")) or 0.0
            score = _float_or_none(row.get("pressure_score")) or 0.0
            if ratio < min_ratio or score < min_score:
                continue
            base, quote = instrument.split("_", 1)
            best_currency = ""
            best_cluster = 0
            best_direction_cluster = 0
            for currency in {base, quote} & LOCALIZED_CLUSTER_CURRENCIES:
                cluster_count = len(currency_instruments.get(currency, set()))
                direction_count = len(currency_direction_instruments.get((currency, direction), set()))
                if (direction_count, cluster_count) > (best_direction_cluster, best_cluster):
                    best_currency = currency
                    best_cluster = cluster_count
                    best_direction_cluster = direction_count
            if (
                best_currency
                and best_cluster >= min_cluster
                and best_direction_cluster >= min_direction_cluster
            ):
                candidates.append({
                    "time_utc": row.get("time_utc") or stamp,
                    "instrument": instrument,
                    "direction": direction,
                    "currency": best_currency,
                    "cluster": best_cluster,
                    "direction_cluster": best_direction_cluster,
                    "net_pips": _float_or_none(row.get("net_pips")) or 0.0,
                    "ratio": ratio,
                    "score": score,
                })

    if not candidates:
        return {"count": 0}
    top = max(
        candidates,
        key=lambda row: (
            abs(float(row.get("net_pips") or 0.0)),
            float(row.get("ratio") or 0.0),
            float(row.get("score") or 0.0),
        ),
    )
    return {
        "count": len(candidates),
        "top": top,
        "currencies": Counter(str(row["currency"]) for row in candidates).most_common(3),
        "instruments": Counter(str(row["instrument"]) for row in candidates).most_common(3),
    }


def summarize_scout_near_passes(
    rows: list[dict[str, Any]],
    *,
    min_value_gate_ratio: float = 0.75,
    min_score_ratio: float = 0.85,
    max_score_gap: float = 10.0,
    min_spread_ratio: float = 0.65,
) -> dict[str, Any]:
    """Summarize skipped scout signals that were close to live execution gates.

    This is advisor-only.  It helps separate true no-signal rows from rows that
    were close enough to justify counterfactual backtests or threshold studies.
    """
    value_buckets: dict[str, dict[str, Any]] = {}
    score_buckets: dict[str, dict[str, Any]] = {}
    spread_buckets: dict[str, dict[str, Any]] = {}

    for row in rows:
        if str(row.get("status") or "").strip().lower() not in {"skipped", "rejected", "blocked"}:
            continue
        text = str(row.get("reject_reason") or row.get("reason") or "")
        if not text:
            continue
        instrument = str(row.get("instrument") or "").upper().replace("/", "_") or "UNKNOWN"
        direction = str(row.get("direction") or "").upper()

        value_gate = _extract_value_gate_pair(text)
        if value_gate:
            expected, threshold = value_gate
            if threshold > 0.0 and expected > 0.0:
                ratio = expected / threshold
                if ratio >= min_value_gate_ratio:
                    bucket = value_buckets.setdefault(
                        instrument,
                        {
                            "instrument": instrument,
                            "count": 0,
                            "expected_usd": 0.0,
                            "max_ratio": 0.0,
                            "min_gap_usd": None,
                            "direction": direction,
                        },
                    )
                    gap = max(0.0, threshold - expected)
                    bucket["count"] = int(bucket.get("count") or 0) + 1
                    bucket["expected_usd"] = float(bucket.get("expected_usd") or 0.0) + expected
                    bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), ratio)
                    prev_gap = bucket.get("min_gap_usd")
                    bucket["min_gap_usd"] = gap if prev_gap is None else min(float(prev_gap), gap)

        score_match = re.search(
            r"score\s+too\s+low\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )
        if score_match:
            score = _float_or_none(score_match.group(1))
            threshold = _float_or_none(score_match.group(2))
            if score is not None and threshold is not None and threshold > 0.0:
                ratio = score / threshold
                gap = threshold - score
                if ratio >= min_score_ratio or 0.0 <= gap <= max_score_gap:
                    bucket = score_buckets.setdefault(
                        f"{instrument}|{direction}",
                        {
                            "instrument": instrument,
                            "direction": direction,
                            "count": 0,
                            "max_score": 0.0,
                            "threshold": threshold,
                            "max_ratio": 0.0,
                            "min_gap": None,
                            "reason": _short_text(text, 90),
                        },
                    )
                    bucket["count"] = int(bucket.get("count") or 0) + 1
                    bucket["max_score"] = max(float(bucket.get("max_score") or 0.0), score)
                    bucket["threshold"] = max(float(bucket.get("threshold") or 0.0), threshold)
                    bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), ratio)
                    prev_gap = bucket.get("min_gap")
                    bucket["min_gap"] = gap if prev_gap is None else min(float(prev_gap), gap)

        spread_match = re.search(
            r"move/spread\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )
        if spread_match:
            observed = _float_or_none(spread_match.group(1))
            threshold = _float_or_none(spread_match.group(2))
            if observed is not None and threshold is not None and threshold > 0.0:
                ratio = observed / threshold
                if ratio >= min_spread_ratio:
                    bucket = spread_buckets.setdefault(
                        instrument,
                        {
                            "instrument": instrument,
                            "count": 0,
                            "max_observed": 0.0,
                            "threshold": threshold,
                            "max_ratio": 0.0,
                            "direction": direction,
                        },
                    )
                    bucket["count"] = int(bucket.get("count") or 0) + 1
                    bucket["max_observed"] = max(float(bucket.get("max_observed") or 0.0), observed)
                    bucket["threshold"] = max(float(bucket.get("threshold") or 0.0), threshold)
                    bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), ratio)

    top_value = sorted(
        value_buckets.values(),
        key=lambda item: (
            -(float(item.get("expected_usd") or 0.0)),
            -float(item.get("max_ratio") or 0.0),
            str(item.get("instrument") or ""),
        ),
    )[:5]
    top_score = sorted(
        score_buckets.values(),
        key=lambda item: (
            -float(item.get("max_ratio") or 0.0),
            float(item.get("min_gap") if item.get("min_gap") is not None else 999999.0),
            -int(item.get("count") or 0),
            str(item.get("instrument") or ""),
        ),
    )[:5]
    top_spread = sorted(
        spread_buckets.values(),
        key=lambda item: (
            -float(item.get("max_ratio") or 0.0),
            -int(item.get("count") or 0),
            str(item.get("instrument") or ""),
        ),
    )[:5]

    return {
        "value_gate_count": sum(int(row.get("count") or 0) for row in value_buckets.values()),
        "value_gate_expected_usd": round(
            sum(float(row.get("expected_usd") or 0.0) for row in value_buckets.values()),
            6,
        ),
        "top_value_gates": top_value,
        "score_near_count": sum(int(row.get("count") or 0) for row in score_buckets.values()),
        "top_score_near": top_score,
        "spread_ratio_near_count": sum(int(row.get("count") or 0) for row in spread_buckets.values()),
        "top_spread_ratio_near": top_spread,
    }


def scout_audit_brief(name: str, lookback_minutes: float = 120.0) -> dict[str, Any]:
    """Summarize recent scout gate/order audit rows for advisor visibility only."""
    path = SCOUT_AUDIT_PATHS.get(name)
    if not path:
        return {"name": name, "exists": False, "error": "unknown scout audit path"}
    rows = _recent_rows_by_age(tail_csv_rows(path, max_rows=10000), lookback_minutes)
    if rows and rows[-1].get("error"):
        return {
            "name": name,
            "path": str(path),
            "exists": path.exists(),
            "lookback_minutes": lookback_minutes,
            "rows": 0,
            "error": rows[-1].get("error", ""),
        }

    status_counts = Counter(str(row.get("status") or "unknown").strip() or "unknown" for row in rows)
    reject_counts = Counter(
        _short_text(row.get("reject_reason") or row.get("reason") or "no_reason", 72)
        for row in rows
        if str(row.get("status") or "").strip().lower() in {"skipped", "rejected", "blocked"}
    )
    accepted_count = sum(
        1
        for row in rows
        if str(row.get("status") or "").strip().lower() in {"accepted", "placed", "submitted", "filled"}
    )
    localized_shadow_count = sum(
        1
        for row in rows
        if str(row.get("decision_stage") or "").strip().lower()
        == "localized_cluster_shadow"
        and str(row.get("status") or "").strip().lower() == "shadow"
    )
    localized_opportunities = reconstructed_localized_cluster_opportunities(rows)
    near_pass = summarize_scout_near_passes(rows)
    passed_count = sum(1 for row in rows if str(row.get("status") or "").strip().lower() == "passed")
    max_score = max((_float_or_none(row.get("pressure_score")) or 0.0 for row in rows), default=0.0)
    max_cluster = max((_float_or_none(row.get("pressure_cluster_instruments")) or 0.0 for row in rows), default=0.0)
    max_dir_cluster = max(
        (_float_or_none(row.get("pressure_cluster_direction_instruments")) or 0.0 for row in rows),
        default=0.0,
    )
    strict_reject_rows = [
        row
        for row in rows
        if str(row.get("decision_stage") or "").strip() == "strict_pressure_gate"
        and str(row.get("status") or "").strip().lower() in {"skipped", "rejected", "blocked"}
    ]
    strict_near_count = 0
    for row in strict_reject_rows:
        reason_text = str(row.get("reject_reason") or row.get("reason") or "")
        match = re.search(
            r"(?:direction\s+)?cluster instruments\s+(\d+(?:\.\d+)?)\s*<\s*(\d+(?:\.\d+)?)",
            reason_text,
        )
        if not match:
            continue
        observed = _float_or_none(match.group(1))
        required = _float_or_none(match.group(2))
        if observed is not None and required is not None and 0.0 <= required - observed <= 2.0:
            strict_near_count += 1
    strict_max_cluster = max(
        (_float_or_none(row.get("pressure_cluster_instruments")) or 0.0 for row in strict_reject_rows),
        default=0.0,
    )
    strict_max_dir_cluster = max(
        (_float_or_none(row.get("pressure_cluster_direction_instruments")) or 0.0 for row in strict_reject_rows),
        default=0.0,
    )
    latest_time = rows[-1].get("time_utc", "") if rows else ""
    return {
        "name": name,
        "path": str(path),
        "exists": path.exists(),
        "lookback_minutes": lookback_minutes,
        "rows": len(rows),
        "latest_age_minutes": age_minutes_from_iso(latest_time),
        "status_counts": [{"name": key, "count": count} for key, count in status_counts.most_common(4)],
        "top_rejects": [{"reason": key, "count": count} for key, count in reject_counts.most_common(3)],
        "accepted_count": accepted_count,
        "localized_shadow_count": localized_shadow_count,
        "localized_opportunities": localized_opportunities,
        "near_pass": near_pass,
        "passed_count": passed_count,
        "max_pressure_score": round(max_score, 2),
        "max_cluster_instruments": int(max_cluster),
        "max_direction_cluster_instruments": int(max_dir_cluster),
        "strict_reject_count": len(strict_reject_rows),
        "strict_near_count": strict_near_count,
        "strict_max_cluster_instruments": int(strict_max_cluster),
        "strict_max_direction_cluster_instruments": int(strict_max_dir_cluster),
        "error": "",
    }


def _scout_audit_line(name: str, audit: dict[str, Any]) -> str:
    if audit.get("error"):
        return f"{name} scout audit: error={audit.get('error')}"
    if not audit.get("rows"):
        return f"{name} scout audit: no rows in {audit.get('lookback_minutes', '')}m"
    status_text = ",".join(
        f"{row.get('name')}:{row.get('count')}" for row in audit.get("status_counts", [])[:3]
    )
    reject_text = "; ".join(
        f"{row.get('reason')}:{row.get('count')}" for row in audit.get("top_rejects", [])[:2]
    )
    localized_opps = audit.get("localized_opportunities") or {}
    localized_text = ""
    if isinstance(localized_opps, dict) and localized_opps.get("count"):
        top = localized_opps.get("top") or {}
        currencies = localized_opps.get("currencies") or []
        currency_text = ""
        if currencies:
            currency_text = " locCur=" + ";".join(f"{cur}:{count}" for cur, count in currencies[:2])
        localized_text = (
            f"localizedOpp={localized_opps.get('count')}"
            f" top={top.get('instrument', '')} {top.get('direction', '')}"
            f" {_fmt_num(top.get('net_pips'), 1)}p"
            f" {top.get('currency', '')}:{top.get('cluster', '')}/{top.get('direction_cluster', '')}"
            f"{currency_text}"
        )
    near_pass = audit.get("near_pass") if isinstance(audit.get("near_pass"), dict) else {}
    near_text = ""
    if near_pass:
        near_parts = []
        if int(near_pass.get("value_gate_count") or 0):
            top_value = near_pass.get("top_value_gates") or []
            top_value_text = ""
            if top_value:
                top = top_value[0]
                top_value_text = (
                    f" topValue={top.get('instrument', '')}"
                    f" {_fmt_num(top.get('max_ratio'), 2)}x"
                )
            near_parts.append(
                f"value=${_fmt_num(near_pass.get('value_gate_expected_usd'), 3)}/"
                f"{near_pass.get('value_gate_count')}{top_value_text}"
            )
        if int(near_pass.get("score_near_count") or 0):
            top_score = near_pass.get("top_score_near") or []
            top_score_text = ""
            if top_score:
                top = top_score[0]
                top_score_text = (
                    f" topScore={top.get('instrument', '')}"
                    f" {_fmt_num(top.get('max_score'), 1)}/{_fmt_num(top.get('threshold'), 1)}"
                )
            near_parts.append(f"score={near_pass.get('score_near_count')}{top_score_text}")
        if int(near_pass.get("spread_ratio_near_count") or 0):
            top_ratio = near_pass.get("top_spread_ratio_near") or []
            top_ratio_text = ""
            if top_ratio:
                top = top_ratio[0]
                top_ratio_text = (
                    f" topRatio={top.get('instrument', '')}"
                    f" {_fmt_num(top.get('max_observed'), 2)}/{_fmt_num(top.get('threshold'), 2)}"
                )
            near_parts.append(f"ratio={near_pass.get('spread_ratio_near_count')}{top_ratio_text}")
        if near_parts:
            near_text = " nearPass[" + " ".join(near_parts) + "]"
    return (
        f"{name} scout audit {int(float(audit.get('lookback_minutes') or 0))}m: "
        f"rows={audit.get('rows')} status={status_text} "
        f"accepted={audit.get('accepted_count')} passed={audit.get('passed_count')} "
        f"localizedShadow={audit.get('localized_shadow_count', 0)} "
        f"{localized_text + ' ' if localized_text else ''}"
        f"{near_text + ' ' if near_text else ''}"
        f"max_score={_fmt_num(audit.get('max_pressure_score'), 1)} "
        f"max_cluster={audit.get('max_cluster_instruments')}/{audit.get('max_direction_cluster_instruments')} "
        f"strict_near2={audit.get('strict_near_count')} "
        f"rejects={reject_text}"
    )


def _top_counter(rows: list[dict[str, Any]], field: str, limit: int = 5) -> list[dict[str, Any]]:
    counts = Counter(str(row.get(field) or "") for row in rows)
    counts.pop("", None)
    return [{"name": name, "count": count} for name, count in counts.most_common(limit)]


def _extract_move_pips(row: dict[str, Any]) -> float | None:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("detail", "reason", "top_reject_reason")
    )
    for pattern in (
        r"net_after_spread=([-+]?\d+(?:\.\d+)?)p",
        r"\bnet=([-+]?\d+(?:\.\d+)?)p",
        r"\bpips=([-+]?\d+(?:\.\d+)?)",
    ):
        match = re.search(pattern, text)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def _classify_miss_detail(detail: Any) -> str:
    text = str(detail or "").lower()
    if "blocked counter-regime scout signal" in text or "scout_regime_lock" in text:
        return "scout_regime_lock"
    if "pre_spike_pressure strict gate rejected" in text:
        if "direction cluster" in text:
            return "pressure_direction_cluster"
        if "cluster instruments" in text:
            return "pressure_cluster"
        if "value" in text:
            return "pressure_value"
        return "pressure_strict_gate"
    if "unknown-profile" in text:
        return "unknown_profile"
    if "value gate" in text:
        return "value_gate"
    if "ev score too low" in text:
        return "ev_low"
    if "score demotion" in text or "demotion blocked" in text:
        return "score_demotion_block"
    if "spread" in text or "ratio" in text:
        return "spread_ratio"
    if "margin" in text:
        return "margin"
    if "risk" in text:
        return "risk"
    return "other"


def _extract_regime_lock_detail(detail: Any) -> dict[str, Any]:
    """Parse scout-regime-lock skip details for value-weighted monitoring.

    Example detail:
    ``blocked counter-regime scout signal; USD_RALLY_RISK_EM_WEAKNESS:
    USD_MXN SHORT edge=-2.47 USD=+2.47 MXN=+0.00``

    The parser is intentionally permissive because these strings are human
    logs, not a schema contract.
    """
    text = str(detail or "").strip()
    if "counter-regime" not in text.lower() and "scout_regime_lock" not in text.lower():
        return {}
    parsed: dict[str, Any] = {"raw": text[:240]}
    match = re.search(
        r"counter-regime scout signal;\s*([^:]+):\s*([A-Z]{3}_[A-Z]{3})\s+"
        r"(LONG|SHORT)\s+edge=([-+]?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        parsed.update(
            {
                "label": match.group(1).strip(),
                "instrument": match.group(2).upper().replace("/", "_").replace("-", "_"),
                "direction": match.group(3).upper(),
                "edge": float(match.group(4)),
            }
        )
    else:
        label_match = re.search(
            r"counter-regime scout signal;\s*([^:;]+)",
            text,
            flags=re.IGNORECASE,
        )
        if label_match:
            parsed["label"] = label_match.group(1).strip()
    return parsed


def _extract_detail_value_usd(detail: Any) -> float | None:
    text = str(detail or "")
    if not text:
        return None
    match = re.search(r"(?:value=|expected\s*)\$(-?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\$(-?\d+(?:\.\d+)?)", text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_value_gate_pair(detail: Any) -> tuple[float, float] | None:
    text = str(detail or "")
    if not text:
        return None
    match = re.search(
        r"value\s+gate:\s*expected\s*\$(-?\d+(?:\.\d+)?)\s*<\s*\$(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return float(match.group(1)), float(match.group(2))
    except ValueError:
        return None


def _regime_lock_context_key(instrument: Any, direction: Any, detail: Any) -> tuple[str, str, str]:
    return (
        str(instrument or "").strip().upper(),
        str(direction or "").strip().upper(),
        re.sub(r"\s+", " ", str(detail or "").strip()),
    )


def _scout_regime_lock_audit_context(date_ny: str, *, max_rows: int = 25000) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Joinable value/net-pip context for scout-regime-lock misses.

    The daily missed-move report is intentionally narrow and stores the skip
    text, while the scout audit ledgers can carry net pips, spread ratio, and
    value-weighted estimates.  Build a small exact-detail lookup from bounded
    audit tails so advisor summaries can value regime-lock misses without
    touching live execution.
    """
    context: dict[tuple[str, str, str], dict[str, Any]] = {}
    target_date = str(date_ny or "").strip()
    for lane, path in SCOUT_AUDIT_PATHS.items():
        for row in tail_csv_rows(path, max_rows=max_rows):
            if row.get("error"):
                continue
            time_ny = str(row.get("time_ny") or "").strip()
            if target_date and not time_ny.startswith(target_date):
                continue
            reason = str(row.get("reason") or "").strip()
            reject = str(row.get("reject_reason") or "").strip()
            detail = reason if "blocked counter-regime scout signal" in reason else reject
            if "blocked counter-regime scout signal" not in detail:
                continue
            key = _regime_lock_context_key(row.get("instrument"), row.get("direction"), detail)
            if not key[0] or not key[1] or not key[2]:
                continue
            value = _float_or_none(row.get("value_expected_usd"))
            net_pips = _float_or_none(row.get("net_pips"))
            ratio = _float_or_none(row.get("move_to_spread_ratio"))
            existing = context.setdefault(
                key,
                {
                    "instrument": key[0],
                    "direction": key[1],
                    "detail": key[2],
                    "lanes": set(),
                    "count": 0,
                    "value_usd": None,
                    "net_pips": None,
                    "move_to_spread_ratio": None,
                    "latest_time_ny": "",
                },
            )
            existing["count"] = int(existing.get("count") or 0) + 1
            lanes = existing.get("lanes")
            if isinstance(lanes, set):
                lanes.add(str(row.get("account_lane") or lane or "").strip())
            if value is not None:
                old_value = _float_or_none(existing.get("value_usd"))
                if old_value is None or abs(value) >= abs(old_value):
                    existing["value_usd"] = value
            if net_pips is not None:
                old_pips = _float_or_none(existing.get("net_pips"))
                if old_pips is None or abs(net_pips) >= abs(old_pips):
                    existing["net_pips"] = net_pips
            if ratio is not None:
                old_ratio = _float_or_none(existing.get("move_to_spread_ratio"))
                if old_ratio is None or ratio >= old_ratio:
                    existing["move_to_spread_ratio"] = ratio
            if time_ny > str(existing.get("latest_time_ny") or ""):
                existing["latest_time_ny"] = time_ny
    for item in context.values():
        lanes = item.get("lanes")
        if isinstance(lanes, set):
            item["lanes"] = sorted(x for x in lanes if x)
    return context


def _dedupe_rows(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(str(row.get(field) or "") for field in fields)
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _value_weighted_miss_summary(
    rows: list[dict[str, Any]],
    regime_lock_audit_context: dict[tuple[str, str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = _dedupe_rows(rows, ("instrument", "direction", "status", "miss_reason", "detail", "movement_key"))
    gate_counts: dict[str, int] = {}
    value_by_instrument: dict[str, dict[str, Any]] = {}
    value_events: list[dict[str, Any]] = []
    regime_lock_by_label: dict[str, dict[str, Any]] = {}
    regime_lock_by_instrument: dict[str, dict[str, Any]] = {}
    regime_lock_events: list[dict[str, Any]] = []
    near_by_instrument: dict[str, dict[str, Any]] = {}
    total_value = 0.0
    value_rows = 0
    regime_lock_count = 0
    regime_lock_value = 0.0
    regime_lock_value_rows = 0
    regime_lock_audit_value_rows = 0
    regime_lock_net_pip_rows = 0
    regime_lock_abs_net_pips = 0.0
    near_total = 0.0
    near_count = 0
    for row in rows:
        gate = _classify_miss_detail(row.get("detail"))
        gate_counts[gate] = gate_counts.get(gate, 0) + 1
        instrument = str(row.get("instrument") or "").strip() or "UNKNOWN"
        direction = str(row.get("direction") or "").strip()
        miss_reason = str(row.get("miss_reason") or "").strip()

        value = _extract_detail_value_usd(row.get("detail"))
        value_source = "detail" if value is not None else ""
        regime_lock = _extract_regime_lock_detail(row.get("detail"))
        regime_context: dict[str, Any] = {}
        if regime_lock and regime_lock_audit_context:
            regime_context = regime_lock_audit_context.get(
                _regime_lock_context_key(instrument, direction, row.get("detail")),
                {},
            )
            if value is None:
                audit_value = _float_or_none(regime_context.get("value_usd"))
                if audit_value is not None:
                    value = audit_value
                    value_source = "scout_audit"
        if value is not None:
            total_value += value
            value_rows += 1
            bucket = value_by_instrument.setdefault(
                instrument,
                {"instrument": instrument, "value_usd": 0.0, "count": 0},
            )
            bucket["value_usd"] = float(bucket.get("value_usd") or 0.0) + value
            bucket["count"] = int(bucket.get("count") or 0) + 1
            value_events.append(
                {
                    "instrument": instrument,
                    "direction": direction,
                    "miss_reason": miss_reason,
                    "gate": gate,
                    "value_usd": value,
                    "value_source": value_source,
                }
            )

        if regime_lock:
            regime_lock_count += 1
            label = str(regime_lock.get("label") or "unknown_lock").strip()
            lock_instrument = str(regime_lock.get("instrument") or instrument).strip() or "UNKNOWN"
            lock_direction = str(regime_lock.get("direction") or direction).strip()
            edge = regime_lock.get("edge")
            audit_net_pips = _float_or_none(regime_context.get("net_pips"))
            audit_ratio = _float_or_none(regime_context.get("move_to_spread_ratio"))
            label_bucket = regime_lock_by_label.setdefault(
                label,
                {
                    "label": label,
                    "count": 0,
                    "value_usd": 0.0,
                    "value_rows": 0,
                    "net_pip_rows": 0,
                    "abs_net_pips": 0.0,
                    "max_net_pips": 0.0,
                    "max_ratio": 0.0,
                },
            )
            inst_bucket = regime_lock_by_instrument.setdefault(
                lock_instrument,
                {
                    "instrument": lock_instrument,
                    "count": 0,
                    "value_usd": 0.0,
                    "value_rows": 0,
                    "net_pip_rows": 0,
                    "abs_net_pips": 0.0,
                    "max_net_pips": 0.0,
                    "max_ratio": 0.0,
                },
            )
            label_bucket["count"] = int(label_bucket.get("count") or 0) + 1
            inst_bucket["count"] = int(inst_bucket.get("count") or 0) + 1
            event = {
                "instrument": lock_instrument,
                "direction": lock_direction,
                "label": label,
                "miss_reason": miss_reason,
                "edge": edge,
                "value_usd": value if value is not None else 0.0,
                "value_source": value_source,
                "net_pips": audit_net_pips,
                "move_to_spread_ratio": audit_ratio,
            }
            if value is not None:
                regime_lock_value += value
                regime_lock_value_rows += 1
                if value_source == "scout_audit":
                    regime_lock_audit_value_rows += 1
                label_bucket["value_usd"] = float(label_bucket.get("value_usd") or 0.0) + value
                label_bucket["value_rows"] = int(label_bucket.get("value_rows") or 0) + 1
                inst_bucket["value_usd"] = float(inst_bucket.get("value_usd") or 0.0) + value
                inst_bucket["value_rows"] = int(inst_bucket.get("value_rows") or 0) + 1
            if audit_net_pips is not None:
                abs_net = abs(audit_net_pips)
                regime_lock_net_pip_rows += 1
                regime_lock_abs_net_pips += abs_net
                for bucket in (label_bucket, inst_bucket):
                    bucket["net_pip_rows"] = int(bucket.get("net_pip_rows") or 0) + 1
                    bucket["abs_net_pips"] = float(bucket.get("abs_net_pips") or 0.0) + abs_net
                    if abs_net >= abs(float(bucket.get("max_net_pips") or 0.0)):
                        bucket["max_net_pips"] = audit_net_pips
                    if audit_ratio is not None:
                        bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), audit_ratio)
            regime_lock_events.append(event)

        parsed_gate = _extract_value_gate_pair(row.get("detail"))
        if parsed_gate:
            expected, threshold = parsed_gate
            if threshold > 0.0 and expected > 0.0:
                threshold_ratio = expected / threshold
                if threshold_ratio >= 0.50:
                    near_total += expected
                    near_count += 1
                    near_bucket = near_by_instrument.setdefault(
                        instrument,
                        {
                            "instrument": instrument,
                            "expected_usd": 0.0,
                            "count": 0,
                            "max_ratio": 0.0,
                        },
                    )
                    near_bucket["expected_usd"] = float(near_bucket.get("expected_usd") or 0.0) + expected
                    near_bucket["count"] = int(near_bucket.get("count") or 0) + 1
                    near_bucket["max_ratio"] = max(float(near_bucket.get("max_ratio") or 0.0), threshold_ratio)

    top_value_instruments = sorted(
        value_by_instrument.values(),
        key=lambda item: (-(float(item.get("value_usd") or 0.0)), str(item.get("instrument") or "")),
    )[:5]
    top_value_events = sorted(
        value_events,
        key=lambda item: (-(float(item.get("value_usd") or 0.0)), str(item.get("instrument") or "")),
    )[:5]
    top_near_instruments = sorted(
        near_by_instrument.values(),
        key=lambda item: (
            -(float(item.get("expected_usd") or 0.0)),
            -int(item.get("count") or 0),
            str(item.get("instrument") or ""),
        ),
    )[:5]
    top_gates = [
        {"gate": key, "count": count}
        for key, count in sorted(gate_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]
    top_regime_lock_labels = sorted(
        regime_lock_by_label.values(),
        key=lambda item: (
            -(float(item.get("value_usd") or 0.0)),
            -(float(item.get("abs_net_pips") or 0.0)),
            -int(item.get("count") or 0),
            str(item.get("label") or ""),
        ),
    )[:5]
    top_regime_lock_instruments = sorted(
        regime_lock_by_instrument.values(),
        key=lambda item: (
            -(float(item.get("value_usd") or 0.0)),
            -(float(item.get("abs_net_pips") or 0.0)),
            -int(item.get("count") or 0),
            str(item.get("instrument") or ""),
        ),
    )[:5]
    top_regime_lock_events = sorted(
        regime_lock_events,
        key=lambda item: (
            -(float(item.get("value_usd") or 0.0)),
            -(abs(float(item.get("net_pips") or 0.0))),
            str(item.get("instrument") or ""),
            str(item.get("label") or ""),
        ),
    )[:5]
    return {
        "unique_rows": len(rows),
        "top_gate_classes": top_gates,
        "value_rows": value_rows,
        "total_value_usd": round(total_value, 6),
        "top_value_instruments": top_value_instruments,
        "top_value_events": top_value_events,
        "regime_lock_count": regime_lock_count,
        "regime_lock_value_rows": regime_lock_value_rows,
        "regime_lock_audit_value_rows": regime_lock_audit_value_rows,
        "regime_lock_net_pip_rows": regime_lock_net_pip_rows,
        "regime_lock_abs_net_pips": round(regime_lock_abs_net_pips, 3),
        "regime_lock_value_usd": round(regime_lock_value, 6),
        "top_regime_lock_labels": top_regime_lock_labels,
        "top_regime_lock_instruments": top_regime_lock_instruments,
        "top_regime_lock_events": top_regime_lock_events,
        "near_value_count": near_count,
        "near_value_usd": round(near_total, 6),
        "top_near_value_instruments": top_near_instruments,
    }


def _summarize_daily_report_dir(path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "market_rows": 0,
        "missed_rows": 0,
        "captured_rows": 0,
        "unique_missed_rows": 0,
        "top_missed_pairs": [],
        "top_miss_reasons": [],
        "top_missed_net_pips": [],
        "miss_value_summary": {},
        "error": "",
    }
    if not path.exists():
        return summary

    def read_csv_rows(name: str) -> list[dict[str, Any]]:
        csv_path = path / name
        if not csv_path.exists():
            return []
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    try:
        market_rows = read_csv_rows("daily_market_moves.csv")
        missed_rows = read_csv_rows("missed_moves.csv")
        captured_rows = read_csv_rows("captured_moves.csv")
        summary["market_rows"] = len(market_rows)
        summary["missed_rows"] = len(missed_rows)
        summary["captured_rows"] = len(captured_rows)
        summary["top_missed_pairs"] = _top_counter(missed_rows, "instrument")
        summary["top_miss_reasons"] = _top_counter(missed_rows, "miss_reason")
        value_summary = _value_weighted_miss_summary(
            missed_rows,
            regime_lock_audit_context=_scout_regime_lock_audit_context(path.name),
        )
        summary["unique_missed_rows"] = value_summary.get("unique_rows", 0)
        summary["miss_value_summary"] = value_summary
        pips_rows: list[tuple[float, dict[str, Any]]] = []
        for row in missed_rows:
            pips = _extract_move_pips(row)
            if pips is not None and abs(pips) > 0.01:
                pips_rows.append((abs(pips), row))
        top_pips = []
        seen_pip_keys: set[tuple[str, str, str, float]] = set()
        for _abs_pips, row in sorted(pips_rows, key=lambda item: item[0], reverse=True):
            pips = _extract_move_pips(row)
            key = (
                str(row.get("instrument") or ""),
                str(row.get("direction") or ""),
                str(row.get("miss_reason") or ""),
                round(float(pips or 0.0), 2),
            )
            if key in seen_pip_keys:
                continue
            seen_pip_keys.add(key)
            top_pips.append(
                {
                    "instrument": row.get("instrument", ""),
                    "direction": row.get("direction", ""),
                    "miss_reason": row.get("miss_reason", ""),
                    "parsed_net_pips": round(float(pips or 0.0), 2),
                    "detail": str(row.get("detail") or "")[:180],
                }
            )
            if len(top_pips) >= 5:
                break
        summary["top_missed_net_pips"] = top_pips
    except Exception as exc:
        summary["error"] = str(exc)[:500]
    return summary


def daily_move_brief(daily_report: dict[str, Any]) -> dict[str, Any]:
    """Compact daily missed/captured move context for the report-only note."""
    latest_dir_raw = str(daily_report.get("latest_report_dir") or "")
    latest_dir = Path(latest_dir_raw) if latest_dir_raw else Path()
    current = _summarize_daily_report_dir(latest_dir) if latest_dir_raw else {"exists": False}
    previous: dict[str, Any] = {"exists": False}
    try:
        date_ny = str(daily_report.get("date_ny") or latest_dir.name)
        parsed = dt.date.fromisoformat(date_ny)
        previous_dir = latest_dir.parent / (parsed - dt.timedelta(days=1)).isoformat()
        previous = _summarize_daily_report_dir(previous_dir)
    except Exception:
        previous = {"exists": False}
    return {"current": current, "previous": previous}


def trainer_live_miss_queue_brief() -> dict[str, Any]:
    """Summarize research-only follow-up queue progress for live missed moves."""
    sources = {
        "operator_live_value_miss_followup_v1": "value",
        "operator_live_ev_near_miss_followup_v1": "ev_near",
        "operator_live_unknown_profile_followup_v1": "unknown",
        "operator_live_missed_spike_subset_followup_v1": "subset",
    }
    queue_rows = read_jsonl_rows(TRAINER_RESEARCH_QUEUE_JSONL)
    ledger_rows = tail_csv_rows(TRAINER_EXPERIMENT_LEDGER_CSV, max_rows=50000)
    if queue_rows and _synthetic_read_error_row(queue_rows[-1]):
        return {
            "path": str(TRAINER_RESEARCH_QUEUE_JSONL),
            "exists": TRAINER_RESEARCH_QUEUE_JSONL.exists(),
            "error": queue_rows[-1].get("error", ""),
            "sources": [],
        }
    if ledger_rows and _synthetic_read_error_row(ledger_rows[-1]):
        return {
            "path": str(TRAINER_RESEARCH_QUEUE_JSONL),
            "exists": TRAINER_RESEARCH_QUEUE_JSONL.exists(),
            "error": ledger_rows[-1].get("error", ""),
            "sources": [],
        }
    completed_hashes = {
        str(row.get("spec_hash") or "")
        for row in ledger_rows
        if str(row.get("spec_hash") or "").strip()
    }
    rows: dict[str, dict[str, Any]] = {
        source: {
            "source": source,
            "label": label,
            "queued": 0,
            "pending": 0,
            "completed": 0,
            "calibration_queued": 0,
            "calibration_pending": 0,
            "calibration_completed": 0,
            "directional_precursor_queued": 0,
            "directional_precursor_pending": 0,
            "directional_precursor_completed": 0,
            "scan_cost_queued": 0,
            "scan_cost_pending": 0,
            "scan_cost_completed": 0,
            "segment_focus_queued": 0,
            "segment_focus_pending": 0,
            "segment_focus_completed": 0,
            "lead_only_robust_queued": 0,
            "lead_only_robust_pending": 0,
            "lead_only_robust_completed": 0,
            "models": set(),
            "targets": set(),
            "pairs": set(),
        }
        for source, label in sources.items()
    }
    for item in queue_rows:
        spec = item.get("spec") if isinstance(item, dict) else {}
        if not isinstance(spec, dict):
            continue
        source = str(spec.get("source") or "")
        if source not in rows:
            continue
        row = rows[source]
        row["queued"] += 1
        spec_hash = str(item.get("spec_hash") or "")
        is_calibration = "calibration" in str(spec.get("specialist_profile") or "").lower()
        is_directional_precursor = "directional_precursor" in str(
            spec.get("specialist_profile") or ""
        ).lower()
        is_scan_cost = "current_scan_cost" in str(
            spec.get("specialist_profile") or ""
        ).lower()
        is_segment_focus = "calibration_detection_segment_focus" in str(
            spec.get("specialist_profile") or ""
        ).lower()
        is_lead_only_robust = "lead_only_robust" in str(
            spec.get("specialist_profile") or ""
        ).lower()
        if is_calibration:
            row["calibration_queued"] += 1
        if is_directional_precursor:
            row["directional_precursor_queued"] += 1
        if is_scan_cost:
            row["scan_cost_queued"] += 1
        if is_segment_focus:
            row["segment_focus_queued"] += 1
        if is_lead_only_robust:
            row["lead_only_robust_queued"] += 1
        if spec_hash and spec_hash in completed_hashes:
            row["completed"] += 1
            if is_calibration:
                row["calibration_completed"] += 1
            if is_directional_precursor:
                row["directional_precursor_completed"] += 1
            if is_scan_cost:
                row["scan_cost_completed"] += 1
            if is_segment_focus:
                row["segment_focus_completed"] += 1
            if is_lead_only_robust:
                row["lead_only_robust_completed"] += 1
        else:
            row["pending"] += 1
            if is_calibration:
                row["calibration_pending"] += 1
            if is_directional_precursor:
                row["directional_precursor_pending"] += 1
            if is_scan_cost:
                row["scan_cost_pending"] += 1
            if is_segment_focus:
                row["segment_focus_pending"] += 1
            if is_lead_only_robust:
                row["lead_only_robust_pending"] += 1
        model = str(spec.get("model_type") or "").strip()
        target = str(spec.get("target") or "").strip()
        if model:
            row["models"].add(model)
        if target:
            row["targets"].add(target)
        whitelist = spec.get("instrument_whitelist") or []
        if isinstance(whitelist, list):
            for instrument in whitelist[:8]:
                if instrument:
                    row["pairs"].add(str(instrument))

    source_rows: list[dict[str, Any]] = []
    for row in rows.values():
        if not row.get("queued"):
            continue
        source_rows.append(
            {
                "label": row["label"],
                "source": row["source"],
                "queued": row["queued"],
                "pending": row["pending"],
                "completed": row["completed"],
                "calibration_queued": row["calibration_queued"],
                "calibration_pending": row["calibration_pending"],
                "calibration_completed": row["calibration_completed"],
                "directional_precursor_queued": row["directional_precursor_queued"],
                "directional_precursor_pending": row["directional_precursor_pending"],
                "directional_precursor_completed": row["directional_precursor_completed"],
                "scan_cost_queued": row["scan_cost_queued"],
                "scan_cost_pending": row["scan_cost_pending"],
                "scan_cost_completed": row["scan_cost_completed"],
                "segment_focus_queued": row["segment_focus_queued"],
                "segment_focus_pending": row["segment_focus_pending"],
                "segment_focus_completed": row["segment_focus_completed"],
                "lead_only_robust_queued": row["lead_only_robust_queued"],
                "lead_only_robust_pending": row["lead_only_robust_pending"],
                "lead_only_robust_completed": row["lead_only_robust_completed"],
                "models": sorted(row["models"])[:6],
                "targets": sorted(row["targets"])[:6],
                "pairs": sorted(row["pairs"])[:8],
            }
        )
    return {
        "path": str(TRAINER_RESEARCH_QUEUE_JSONL),
        "exists": TRAINER_RESEARCH_QUEUE_JSONL.exists(),
        "ledger_path": str(TRAINER_EXPERIMENT_LEDGER_CSV),
        "ledger_exists": TRAINER_EXPERIMENT_LEDGER_CSV.exists(),
        "sources": source_rows,
        "error": "",
    }


def _live_miss_queue_line(brief: dict[str, Any]) -> str:
    if brief.get("error"):
        return f"live miss queue: error={brief.get('error')}"
    bits: list[str] = []
    for row in brief.get("sources", []):
        if not isinstance(row, dict):
            continue
        pairs = ",".join(str(pair) for pair in (row.get("pairs") or [])[:4])
        pair_text = f" pairs={pairs}" if pairs else ""
        calibration_text = ""
        if int(row.get("calibration_queued") or 0):
            calibration_text = (
                f" calib={row.get('calibration_pending', 0)}/"
                f"{row.get('calibration_queued', 0)}"
            )
        directional_text = ""
        if int(row.get("directional_precursor_queued") or 0):
            directional_text = (
                f" dirPrec={row.get('directional_precursor_pending', 0)}/"
                f"{row.get('directional_precursor_queued', 0)}"
            )
        scan_cost_text = ""
        if int(row.get("scan_cost_queued") or 0):
            scan_cost_text = (
                f" scanCost={row.get('scan_cost_pending', 0)}/"
                f"{row.get('scan_cost_queued', 0)}"
            )
        segment_focus_text = ""
        if int(row.get("segment_focus_queued") or 0):
            segment_focus_text = (
                f" segFocus={row.get('segment_focus_pending', 0)}/"
                f"{row.get('segment_focus_queued', 0)}"
            )
        robust_text = ""
        if int(row.get("lead_only_robust_queued") or 0):
            robust_text = (
                f" robust={row.get('lead_only_robust_pending', 0)}/"
                f"{row.get('lead_only_robust_queued', 0)}"
            )
        bits.append(
            f"{row.get('label')}:q={row.get('queued', 0)} "
            f"p={row.get('pending', 0)} done={row.get('completed', 0)}"
            f"{calibration_text}{directional_text}{scan_cost_text}{segment_focus_text}{robust_text}{pair_text}"
        )
    return "live miss queue: " + ("; ".join(bits) if bits else "none")


def _gate_failure_text_from_detail(row: dict[str, Any]) -> str:
    path_raw = str(row.get("detail_path") or "").strip()
    if not path_raw:
        return ""
    detail_path = Path(path_raw)
    payload = read_json(detail_path)
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    gate = result.get("gate") if isinstance(result.get("gate"), dict) else {}
    if not gate or gate.get("passed") is True:
        return ""
    failure_names = {
        "mean_auc_at_least_0_58": "auc<.58",
        "minimum_week_auc_at_least_0_52": "minAuc<.52",
        "mean_average_precision_lift_at_least_3": "apLift<3",
        "mean_brier_skill_positive": "brier<=0",
        "mean_direction_accuracy_at_least_0_52": "dirAcc<.52",
        "selected_trades_at_least_75": "trades<75",
        "selected_mean_net_pips_positive": "net<=0",
        "selected_bootstrap_lower_mean_positive": "boot<=0",
        "selected_positive_in_70pct_weeks": "posWeeks",
        "selected_median_profit_factor_at_least_1_20": "pf<1.2",
        "top_pair_trade_share_at_configured_limit": "concentration",
    }
    failures = [
        label
        for key, label in failure_names.items()
        if gate.get(key) is False
    ]
    return ",".join(failures[:4])


def _detail_metric(row: dict[str, Any], metric: str) -> float | None:
    direct = _float_or_none(row.get(metric))
    if direct is not None:
        return direct
    path_raw = str(row.get("detail_path") or "").strip()
    if not path_raw:
        return None
    payload = read_json(Path(path_raw))
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    return _float_or_none(result.get(metric))


def _followup_candidate_text(row: dict[str, Any]) -> str:
    whitelist = str(row.get("instrument_whitelist") or row.get("instrument_subset") or "").replace(",", "+")
    failures = _gate_failure_text_from_detail(row)
    failure_text = f" fail={failures}" if failures else ""
    ap_lift = _detail_metric(row, "mean_average_precision_lift")
    brier = _detail_metric(row, "mean_brier_skill")
    ap_text = f" ap={ap_lift:.2f}" if ap_lift is not None else ""
    brier_text = f" brier={brier:.4f}" if brier is not None else ""
    return (
        f"{row.get('model_type', '')}/{row.get('target', '')}/{whitelist} "
        f"auc={_fmt_num(row.get('mean_auc'), 3)} "
        f"minAuc={_fmt_num(row.get('minimum_week_auc'), 3)}"
        f"{ap_text}{brier_text} "
        f"folds={_fmt_num(row.get('fold_count'), 0)} "
        f"trades={_fmt_num(row.get('trades'), 0)} "
        f"net={_fmt_num(row.get('mean_net_pips'), 3)}p "
        f"gate={row.get('gate_passed', '')}"
        f"{failure_text}"
    )


def _successful_missed_spike_segment_candidates(
    segment_leaders: dict[str, Any],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Return compact successful missed-spike segment leaders.

    The current trainer row can be a failed follow-up even while earlier
    missed-spike candidates passed validation.  Surface the best distinct
    successful profiles so the advisor note keeps the viable scout evidence in
    view without changing live execution.
    """
    top_by_scorecard = (
        segment_leaders.get("top_by_scorecard")
        if isinstance(segment_leaders.get("top_by_scorecard"), dict)
        else {}
    )
    best_by_profile: dict[str, dict[str, Any]] = {}
    for rows in top_by_scorecard.values():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("source") != "operator_live_missed_spike_subset_followup_v1":
                continue
            if str(row.get("status") or "").lower() != "successful" and row.get("gate_passed") is not True:
                continue
            profile = str(row.get("specialist_profile") or row.get("experiment_id") or "")
            if not profile:
                continue
            current = best_by_profile.get(profile)
            score = _float_or_none(row.get("leader_score")) or 0.0
            current_score = _float_or_none(current.get("leader_score")) if current else None
            if current is None or score > (current_score or float("-inf")):
                best_by_profile[profile] = row

    def sort_key(row: dict[str, Any]) -> tuple[float, float, float]:
        return (
            _float_or_none(row.get("leader_score")) or 0.0,
            _float_or_none(row.get("total_net")) or 0.0,
            _float_or_none(row.get("mean_auc")) or 0.0,
        )

    compact: list[dict[str, Any]] = []
    for row in sorted(best_by_profile.values(), key=sort_key, reverse=True)[:limit]:
        compact.append(
            {
                "profile": row.get("specialist_profile", ""),
                "model_type": row.get("model_type", ""),
                "target": row.get("target", ""),
                "instrument_whitelist": row.get("instrument_whitelist", ""),
                "scorecard": row.get("scorecard", ""),
                "segment": row.get("segment", ""),
                "leader_score": row.get("leader_score", ""),
                "mean_auc": row.get("mean_auc", ""),
                "mean_net": row.get("mean_net", ""),
                "total_net": row.get("total_net", ""),
                "profit_factor": row.get("profit_factor", ""),
                "trades": row.get("trades", ""),
                "experiment_id": row.get("experiment_id", ""),
            }
        )
    return compact


def trainer_live_miss_followup_result_brief() -> dict[str, Any]:
    """Summarize result quality for live miss-driven research lanes."""
    result_configs = [
        {
            "source": "operator_live_value_miss_followup_v1",
            "label": "value",
            "profile_contains": "",
        },
        {
            "source": "operator_live_value_miss_followup_v1",
            "label": "scan_cost",
            "profile_contains": "current_scan_cost",
        },
        {
            "source": "operator_live_value_miss_followup_v1",
            "label": "segment_focus",
            "profile_contains": "calibration_detection_segment_focus",
        },
        {
            "source": "operator_live_ev_near_miss_followup_v1",
            "label": "ev_near",
            "profile_contains": "",
        },
        {
            "source": "operator_live_unknown_profile_followup_v1",
            "label": "unknown",
            "profile_contains": "",
        },
        {
            "source": "operator_live_missed_spike_subset_followup_v1",
            "label": "subset",
            "profile_contains": "",
        },
        {
            "source": "operator_live_missed_spike_subset_followup_v1",
            "label": "localized_cluster",
            "profile_contains": "localized_cluster",
        },
        {
            "source": "operator_live_missed_spike_subset_followup_v1",
            "label": "localized_directional",
            "profile_contains": "directional_precursor",
        },
        {
            "source": "operator_live_value_miss_followup_v1",
            "label": "robust_value",
            "profile_contains": "lead_only_robust",
        },
        {
            "source": "operator_live_unknown_profile_followup_v1",
            "label": "robust_unknown",
            "profile_contains": "lead_only_robust",
        },
        {
            "source": "operator_live_missed_spike_subset_followup_v1",
            "label": "robust_subset",
            "profile_contains": "lead_only_robust",
        },
        {
            "source": "operator_live_ev_near_miss_followup_v1",
            "label": "robust_ev_near",
            "profile_contains": "lead_only_robust",
        },
    ]
    ledger_rows = tail_csv_rows(TRAINER_EXPERIMENT_LEDGER_CSV, max_rows=50000)
    if ledger_rows and _synthetic_read_error_row(ledger_rows[-1]):
        return {
            "path": str(TRAINER_EXPERIMENT_LEDGER_CSV),
            "exists": TRAINER_EXPERIMENT_LEDGER_CSV.exists(),
            "error": ledger_rows[-1].get("error", ""),
            "sources": [],
        }

    def parse_time(row: dict[str, Any]) -> dt.datetime:
        raw = str(row.get("time_utc") or "")
        try:
            parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except Exception:
            return dt.datetime.min.replace(tzinfo=dt.timezone.utc)

    source_rows: list[dict[str, Any]] = []
    for config in result_configs:
        source = str(config.get("source") or "")
        label = str(config.get("label") or source)
        profile_filter = str(config.get("profile_contains") or "").lower().strip()
        rows = [
            row
            for row in ledger_rows
            if str(row.get("source") or "") == source
            and (
                not profile_filter
                or profile_filter in str(row.get("specialist_profile") or "").lower()
            )
        ]
        if not rows:
            continue
        status_counts = Counter(str(row.get("status") or "unknown").strip() or "unknown" for row in rows)
        gate_count = sum(1 for row in rows if str(row.get("gate_passed") or "").strip().lower() == "true")
        scored = [
            row for row in rows
            if _float_or_none(row.get("score")) is not None or _float_or_none(row.get("mean_auc")) is not None
        ]
        scored.sort(
            key=lambda row: (
                1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
                _float_or_none(row.get("score")) or -1.0,
                _float_or_none(row.get("mean_auc")) or -1.0,
                _float_or_none(row.get("trades")) or 0.0,
            ),
            reverse=True,
        )
        latest = max(rows, key=parse_time)
        latest_scored = max(scored, key=parse_time) if scored else latest
        source_rows.append(
            {
                "label": label,
                "source": source,
                "rows": len(rows),
                "gate_count": gate_count,
                "status_counts": [
                    {"name": key, "count": count}
                    for key, count in status_counts.most_common(3)
                ],
                "latest_time_utc": latest.get("time_utc", ""),
                "latest_age_minutes": age_minutes_from_iso(latest.get("time_utc", "")),
                "latest_summary": _followup_candidate_text(latest_scored),
                "best_summary": _followup_candidate_text(scored[0]) if scored else "",
            }
        )
    return {
        "path": str(TRAINER_EXPERIMENT_LEDGER_CSV),
        "exists": TRAINER_EXPERIMENT_LEDGER_CSV.exists(),
        "sources": source_rows,
        "error": "",
    }


def _live_miss_result_lines(brief: dict[str, Any]) -> list[str]:
    if brief.get("error"):
        return [f"live miss result error: {brief.get('error')}"]
    lines: list[str] = []
    for row in brief.get("sources", []):
        if not isinstance(row, dict):
            continue
        status_text = ",".join(
            f"{item.get('name')}:{item.get('count')}"
            for item in row.get("status_counts", [])[:3]
            if isinstance(item, dict)
        )
        lines.append(
            f"live miss result {row.get('label')}: rows={row.get('rows', 0)} "
            f"gates={row.get('gate_count', 0)} status={status_text} "
            f"latest={row.get('latest_summary', '')}"
        )
    return lines


def maybe_refresh_trainer_report() -> dict[str, Any]:
    """Refresh trainer reporting only when the report is stale/missing.

    This is report-only.  It runs the reporting extension script, which reads
    trainer artifacts and writes summary files; it does not change broker state,
    promotions, or live manager behavior.
    """
    meta: dict[str, Any] = {
        "path": str(TRAINER_REPORT_JSON),
        "stale_after_minutes": TRAINER_REPORT_REFRESH_STALE_MINUTES,
        "refreshed": False,
        "skipped_reason": "",
        "age_minutes_before": "",
        "returncode": "",
        "stdout_tail": "",
        "stderr_tail": "",
        "error": "",
    }
    try:
        existing = read_json(TRAINER_REPORT_JSON)
        generated_utc = existing.get("generated_utc", "") if isinstance(existing, dict) else ""
        age = age_minutes_from_iso(generated_utc)
        status = existing.get("trainer_status") if isinstance(existing.get("trainer_status"), dict) else {}
        heartbeat_age = age_minutes_from_iso(status.get("research_heartbeat_utc", ""))
        meta["age_minutes_before"] = age
        meta["heartbeat_age_minutes_before"] = heartbeat_age
        if isinstance(age, (int, float)) and age <= TRAINER_REPORT_REFRESH_STALE_MINUTES:
            if not (
                isinstance(heartbeat_age, (int, float))
                and heartbeat_age > TRAINER_HEARTBEAT_REFRESH_STALE_MINUTES
            ):
                meta["skipped_reason"] = "fresh"
                return meta
            meta["skipped_reason"] = "heartbeat_stale_force_refresh"
        script = advisor.SCRIPT_DIR / "oanda_trainer_reporting_extensions.py"
        proc = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(advisor.SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        meta["returncode"] = proc.returncode
        meta["stdout_tail"] = (proc.stdout or "")[-1000:]
        meta["stderr_tail"] = (proc.stderr or "")[-1000:]
        meta["refreshed"] = proc.returncode == 0
        if proc.returncode != 0:
            meta["error"] = f"trainer report refresh failed rc={proc.returncode}"
    except subprocess.TimeoutExpired as exc:
        meta["error"] = f"trainer report refresh timeout: {exc}"
    except Exception as exc:
        meta["error"] = str(exc)[:700]
    return meta


def _model_metrics_error_context(obj: dict[str, Any]) -> dict[str, Any]:
    """Summarize whether model-metric error rows are recent or historical."""
    csv_path_raw = str(obj.get("csv_path") or "")
    if not csv_path_raw:
        return {}
    csv_path = Path(csv_path_raw)
    if not csv_path.exists():
        return {"csv_missing": True}
    error_rows = 0
    recent_24h = 0
    latest_error_age: float | None = None
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if str(row.get("status") or "").strip().lower() != "error":
                    continue
                error_rows += 1
                age = age_minutes_from_iso(row.get("time_utc", ""))
                if not isinstance(age, (int, float)):
                    continue
                latest_error_age = age if latest_error_age is None else min(latest_error_age, age)
                if age <= 24 * 60:
                    recent_24h += 1
    except Exception as exc:
        return {"error": str(exc)[:160]}
    return {
        "error_rows": error_rows,
        "recent_24h": recent_24h,
        "latest_error_age_minutes": latest_error_age,
    }


def _best_metric_summary(obj: dict[str, Any]) -> str:
    def stat_piece(label: str, stat: Any, unit: str = "p") -> str:
        if not isinstance(stat, dict):
            return ""
        mean = stat.get("mean")
        count = stat.get("count")
        pos_rate = stat.get("positive_rate")
        if isinstance(mean, (int, float)):
            piece = f"{label}={mean:.2f}{unit}"
            if count not in ("", None):
                piece += f"/n={count}"
            if isinstance(pos_rate, (int, float)):
                piece += f"/pos={pos_rate:.0%}"
            return piece
        return ""

    if obj.get("report_type") == "trainer_duplicate_validation":
        def source_label(value: Any) -> str:
            text = str(value or "")
            if text == "operator_scout_value_capture_v1":
                return "scout_value"
            return text.replace("operator_live_", "").replace("_followup_v1", "")

        parts = ["dupValidation"]
        validation_rows = obj.get("validation_rows")
        if validation_rows not in ("", None):
            parts.append(f"rows={validation_rows}")
        duplicate_rows = obj.get("duplicate_rows")
        duplicate_keys = obj.get("duplicate_relaxed_keys")
        if duplicate_rows not in ("", None):
            piece = f"dupRows={duplicate_rows}"
            duplicate_rate = obj.get("duplicate_row_rate")
            if isinstance(duplicate_rate, (int, float)):
                piece += f"/{duplicate_rate:.0%}"
            parts.append(piece)
        if duplicate_keys not in ("", None):
            parts.append(f"dupKeys={duplicate_keys}")
        duplicate_windows = (
            obj.get("duplicate_windows")
            if isinstance(obj.get("duplicate_windows"), dict)
            else {}
        )
        for window_label in ("30", "60"):
            window_summary = (
                duplicate_windows.get(window_label)
                if isinstance(duplicate_windows.get(window_label), dict)
                else {}
            )
            if not window_summary:
                continue
            window_validation_rows = window_summary.get("validation_rows")
            window_duplicate_rows = window_summary.get("duplicate_rows")
            if window_validation_rows in ("", None) or window_duplicate_rows in ("", None):
                continue
            window_piece = f"w{window_label}dup={window_duplicate_rows}/{window_validation_rows}"
            window_rate = window_summary.get("duplicate_row_rate")
            if isinstance(window_rate, (int, float)):
                window_piece += f"/{window_rate:.0%}"
            parts.append(window_piece)
            break
        top_duplicates = (
            obj.get("top_duplicates")
            if isinstance(obj.get("top_duplicates"), list)
            else []
        )
        if top_duplicates and isinstance(top_duplicates[0], dict):
            top = top_duplicates[0]
            instruments = str(top.get("instrument_whitelist") or "").replace(",", "+")
            parts.append(
                f"top={top.get('count')}x "
                f"{top.get('model_type', '')}/{top.get('target', '')} "
                f"{instruments}"
            )
        source_counts = (
            obj.get("source_counts")
            if isinstance(obj.get("source_counts"), list)
            else []
        )
        if source_counts:
            top_sources = []
            for row in source_counts[:2]:
                if isinstance(row, list | tuple) and len(row) >= 2:
                    top_sources.append(f"{source_label(row[0])}:{row[1]}")
            if top_sources:
                parts.append("src=" + ",".join(top_sources))
        source_windows = obj.get("source_windows") if isinstance(obj.get("source_windows"), dict) else {}
        short_window = (
            source_windows.get("60")
            if isinstance(source_windows.get("60"), list)
            else []
        )
        if short_window:
            short_sources = []
            for row in short_window[:3]:
                if isinstance(row, list | tuple) and len(row) >= 2:
                    short_sources.append(f"{source_label(row[0])}:{row[1]}")
            if short_sources:
                parts.append("w60=" + ",".join(short_sources))
        return " ".join(str(part) for part in parts[:7] if part)

    summary = obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
    if obj.get("pipeline_version") == "reference_strategy_mimic_v1" or summary.get("pipeline_version") == "reference_strategy_mimic_v1":
        reference = summary.get("reference_summary") if isinstance(summary.get("reference_summary"), dict) else {}
        test = (
            summary.get("calibration_selected_test_summary")
            if isinstance(summary.get("calibration_selected_test_summary"), dict)
            else {}
        )
        full = (
            summary.get("full_window_best_fit_summary")
            if isinstance(summary.get("full_window_best_fit_summary"), dict)
            else {}
        )
        parts = ["referenceMimic"]
        if reference.get("listed_net_pips") not in ("", None):
            parts.append(f"ref={float(reference.get('listed_net_pips') or 0.0):+.1f}p")
        if test:
            parts.append(
                f"test={float(test.get('net_pips') or 0.0):+.1f}p/"
                f"{int(float(test.get('trades') or 0.0))}tr"
            )
            if isinstance(test.get("weighted_win_rate"), (int, float)):
                parts.append(f"testWR={float(test.get('weighted_win_rate') or 0.0):.0%}")
        if full:
            parts.append(
                f"full={float(full.get('net_pips') or 0.0):+.1f}p/"
                f"{int(float(full.get('trades') or 0.0))}tr"
            )
        if obj.get("fast") not in ("", None):
            parts.append(f"fast={bool(obj.get('fast'))}")
        return " ".join(parts[:7])

    if obj.get("execution") == "read_only_no_broker_writes" and "candidate_rows" in obj and "summary" in obj:
        summary = obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
        parts = ["nearPassReplay"]
        if obj.get("candidate_rows") not in ("", None):
            parts.append(f"rows={obj.get('candidate_rows')}")
        if obj.get("raw_candidate_rows") not in ("", None):
            parts.append(f"raw={obj.get('raw_candidate_rows')}")
        gate_counts = summary.get("gate_counts") if isinstance(summary.get("gate_counts"), list) else []
        if gate_counts:
            parts.append(
                "gates="
                + ",".join(
                    f"{row[0]}:{row[1]}"
                    for row in gate_counts[:3]
                    if isinstance(row, list | tuple) and len(row) >= 2
                )
            )
        for horizon in (60, 120):
            stat = summary.get(f"final_{horizon}m") if isinstance(summary.get(f"final_{horizon}m"), dict) else {}
            if stat:
                sum_usd = stat.get("sum_usd")
                pos_rate = stat.get("positive_rate")
                if isinstance(sum_usd, (int, float)):
                    piece = f"f{horizon}=${sum_usd:.2f}"
                    if isinstance(pos_rate, (int, float)):
                        piece += f"/pos={pos_rate:.0%}"
                    parts.append(piece)
            trail_stats = (
                summary.get(f"trail_{horizon}m")
                if isinstance(summary.get(f"trail_{horizon}m"), list)
                else []
            )
            if trail_stats and isinstance(trail_stats[0], dict):
                best_trail = trail_stats[0]
                trail_sum = best_trail.get("sum_usd")
                trail_pos = best_trail.get("positive_rate")
                config = str(best_trail.get("config") or "")
                if isinstance(trail_sum, (int, float)):
                    piece = f"t{horizon}:{config}=${trail_sum:.2f}"
                    if isinstance(trail_pos, (int, float)):
                        piece += f"/pos={trail_pos:.0%}"
                    parts.append(piece)
        top = summary.get("top_final_60m") if isinstance(summary.get("top_final_60m"), list) else []
        if top and isinstance(top[0], dict):
            parts.append(
                f"top60={top[0].get('instrument', '')} {top[0].get('direction', '')} "
                f"${float(top[0].get('final_60m_usd') or 0.0):.3f}"
            )
        return " ".join(part for part in parts[:9] if part)

    capture_groups = (
        obj.get("top_positive_capture_groups")
        if isinstance(obj.get("top_positive_capture_groups"), list)
        else []
    )
    gap_groups = (
        obj.get("top_capture_gap_groups")
        if isinstance(obj.get("top_capture_gap_groups"), list)
        else []
    )
    if capture_groups or gap_groups:
        leader = capture_groups[0] if capture_groups else gap_groups[0]
        leader = leader if isinstance(leader, dict) else {}
        aggregate = obj.get("aggregate") if isinstance(obj.get("aggregate"), dict) else {}
        parts = ["captureGap"]
        if obj.get("row_count") not in ("", None):
            parts.append(f"rows={obj.get('row_count')}")
        if obj.get("group_count") not in ("", None):
            parts.append(f"groups={obj.get('group_count')}")
        if isinstance(aggregate.get("best_exit_usd_sum"), (int, float)):
            parts.append(f"aggBest=${aggregate.get('best_exit_usd_sum'):.2f}")
        if isinstance(aggregate.get("fresh_mfe_60m_usd_sum"), (int, float)):
            parts.append(f"aggMFE60=${aggregate.get('fresh_mfe_60m_usd_sum'):.2f}")
        group = leader.get("group", "")
        key = leader.get("key", "")
        if group or key:
            parts.append(f"top={group}:{key}"[:110])
        if isinstance(leader.get("best_exit_usd_sum"), (int, float)):
            parts.append(f"best=${leader.get('best_exit_usd_sum'):.2f}")
        if isinstance(leader.get("best_exit_pos_rate"), (int, float)):
            parts.append(f"pos={leader.get('best_exit_pos_rate'):.0%}")
        if isinstance(leader.get("capture_gap_60m_usd_sum"), (int, float)):
            parts.append(f"gap=${leader.get('capture_gap_60m_usd_sum'):.2f}")
        exit_key = str(leader.get("top_exit_key") or "")
        if exit_key:
            parts.append(f"exit={exit_key.replace('fresh_', '')[:40]}")
        return " ".join(parts[:10])

    top_subsets = (
        obj.get("top_delay_specific_positive_subsets")
        if isinstance(obj.get("top_delay_specific_positive_subsets"), list)
        else []
    )
    fallback_top_subsets = (
        obj.get("top_positive_subsets")
        if isinstance(obj.get("top_positive_subsets"), list)
        else []
    )
    oracle_subsets = (
        obj.get("top_oracle_mfe_subsets")
        if isinstance(obj.get("top_oracle_mfe_subsets"), list)
        else []
    )
    if top_subsets or fallback_top_subsets or oracle_subsets:
        leader = top_subsets[0] if top_subsets else (fallback_top_subsets[0] if fallback_top_subsets else oracle_subsets[0])
        leader = leader if isinstance(leader, dict) else {}
        parts = ["missedSubsets"]
        if obj.get("row_count") not in ("", None):
            parts.append(f"rows={obj.get('row_count')}")
        if obj.get("unique_event_count") not in ("", None):
            parts.append(f"events={obj.get('unique_event_count')}")
        delays = obj.get("delay_minutes") if isinstance(obj.get("delay_minutes"), list) else []
        if delays:
            parts.append(f"delays={','.join(str(value) for value in delays[:6])}m")
        if obj.get("group_count") not in ("", None):
            parts.append(f"groups={obj.get('group_count')}")
        if top_subsets:
            parts.append(f"delayPos={len(top_subsets)}")
        elif fallback_top_subsets:
            parts.append(f"positive={len(fallback_top_subsets)}")
        group = leader.get("group", "")
        key = leader.get("key", "")
        if group or key:
            parts.append(f"top={group}:{key}"[:110])
        best_exit = leader.get("best_exit_usd_sum")
        if isinstance(best_exit, (int, float)):
            parts.append(f"bestExit=${best_exit:.2f}")
        pos_rate = leader.get("best_exit_pos_rate")
        if isinstance(pos_rate, (int, float)):
            parts.append(f"pos={pos_rate:.0%}")
        mfe120 = leader.get("fresh_mfe_120m_usd_sum")
        if isinstance(mfe120, (int, float)):
            parts.append(f"mfe120=${mfe120:.2f}")
        return " ".join(parts[:9])

    members = obj.get("members") if isinstance(obj.get("members"), list) else []
    if members:
        parts = [f"ensemble members={len(members)}"]
        score = obj.get("research_score")
        if isinstance(score, (int, float)):
            parts.append(f"score={score:.1f}")
        selected_records = obj.get("selected_record_count")
        if selected_records not in ("", None):
            parts.append(f"selected={selected_records}")
        gate = obj.get("gate") if isinstance(obj.get("gate"), dict) else {}
        if gate:
            parts.append(f"gate={'pass' if gate.get('passed') else 'fail'}")
        threshold_rows = obj.get("threshold_summary") if isinstance(obj.get("threshold_summary"), list) else []
        if threshold_rows:
            mean_values = [
                row.get("mean_net_pips")
                for row in threshold_rows
                if isinstance(row, dict) and isinstance(row.get("mean_net_pips"), (int, float))
            ]
            trades = [
                row.get("trades")
                for row in threshold_rows
                if isinstance(row, dict) and isinstance(row.get("trades"), (int, float))
            ]
            if mean_values:
                parts.append(f"holdoutMean={sum(mean_values) / len(mean_values):.2f}p")
            if trades:
                parts.append(f"holdoutTrades={sum(trades):.0f}")
        first = members[0] if isinstance(members[0], dict) else {}
        spec = first.get("spec") if isinstance(first.get("spec"), dict) else {}
        if spec:
            parts.append(
                f"top={spec.get('model_type', '')}/{spec.get('target', '')}/{spec.get('instrument_subset', '')}"
            )
        return " ".join(parts[:8])

    summary = obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
    observed = obj.get("observed_move_value") if isinstance(obj.get("observed_move_value"), dict) else {}
    if summary or observed:
        fresh = summary.get("fresh") if isinstance(summary.get("fresh"), dict) else {}
        parts = ["missedReplay"]
        if isinstance(obj.get("entry_delay_minutes"), list):
            delay_values = ",".join(str(value) for value in obj.get("entry_delay_minutes", [])[:6])
            parts.append(f"delays={delay_values}m")
            if obj.get("best_delay_minutes") not in ("", None):
                parts.append(f"bestDelay={obj.get('best_delay_minutes')}m")
        elif obj.get("entry_delay_minutes") not in ("", None):
            parts.append(f"delay={obj.get('entry_delay_minutes')}m")
        recent_count = obj.get("recent_count") or obj.get("candidate_count")
        if recent_count not in ("", None):
            parts.append(f"events={recent_count}")
        instruments = obj.get("instrument_count")
        if instruments not in ("", None):
            parts.append(f"instruments={instruments}")
        observed_pips = observed.get("raw_observed_abs_pips") if isinstance(observed, dict) else {}
        piece = stat_piece("obsAbs", observed_pips, "p")
        if piece:
            parts.append(piece)
        for label, key in (
            ("final30", "final_30m_pips"),
            ("mfe60", "mfe_60m_pips"),
            ("final60", "final_60m_pips"),
        ):
            piece = stat_piece(label, fresh.get(key), "p")
            if piece:
                parts.append(piece)
        mfe60 = fresh.get("mfe_60m_pips") if isinstance(fresh.get("mfe_60m_pips"), dict) else {}
        final60 = fresh.get("final_60m_pips") if isinstance(fresh.get("final_60m_pips"), dict) else {}
        mfe60_mean = mfe60.get("mean") if isinstance(mfe60.get("mean"), (int, float)) else None
        final60_mean = final60.get("mean") if isinstance(final60.get("mean"), (int, float)) else None
        if mfe60_mean is not None and final60_mean is not None:
            gap = float(mfe60_mean) - float(final60_mean)
            parts.append(f"curveGap60={gap:+.2f}p")
        trail_rows: list[tuple[float, str, dict[str, Any]]] = []
        for key, stat in fresh.items():
            if not (isinstance(key, str) and key.startswith("trail_") and key.endswith("_usd")):
                continue
            if not isinstance(stat, dict) or not isinstance(stat.get("sum"), (int, float)):
                continue
            trail_rows.append((float(stat.get("sum") or 0.0), key, stat))
        if trail_rows:
            trail_sum, trail_key, trail_stat = max(trail_rows, key=lambda item: item[0])
            label = (
                trail_key
                .removeprefix("trail_")
                .removesuffix("_usd")
                .replace("_", "/")
            )
            trail_piece = f"bestTrail={label}=${trail_sum:.2f}"
            pos_rate = trail_stat.get("positive_rate")
            if isinstance(pos_rate, (int, float)):
                trail_piece += f"/pos={pos_rate:.0%}"
            parts.append(trail_piece)
        top = obj.get("top_by_fresh_final_30m") if isinstance(obj.get("top_by_fresh_final_30m"), list) else []
        if top and isinstance(top[0], dict):
            parts.append(
                f"top30={top[0].get('instrument', '')} {top[0].get('direction', '')} "
                f"{float(top[0].get('fresh_final_30m_pips') or 0.0):.1f}p"
            )
        return " ".join(parts[:11])

    if "decision" in obj or "stable_rule_count" in obj:
        parts = ["exhaustionEvidence"]
        if obj.get("stable_rule_count") not in ("", None):
            parts.append(f"stableRules={obj.get('stable_rule_count')}")
        if obj.get("execution_approved_rule_count") not in ("", None):
            parts.append(f"execApproved={obj.get('execution_approved_rule_count')}")
        if obj.get("live_shadow_rows") not in ("", None):
            parts.append(f"shadowRows={obj.get('live_shadow_rows')}")
        decision = str(obj.get("decision") or "")
        if decision:
            parts.append(decision[:120])
        return " ".join(parts[:6])

    if "status_counts" in obj or "model_type_counts" in obj:
        status_counts = obj.get("status_counts") if isinstance(obj.get("status_counts"), dict) else {}
        model_counts = obj.get("model_type_counts") if isinstance(obj.get("model_type_counts"), dict) else {}
        parts = ["modelMetrics"]
        if obj.get("row_count") not in ("", None):
            parts.append(f"rows={obj.get('row_count')}")
        for key in ("active", "successful", "unsuccessful", "error"):
            if key in status_counts:
                parts.append(f"{key}={status_counts.get(key)}")
        error_context = _model_metrics_error_context(obj)
        if error_context.get("recent_24h") not in ("", None):
            parts.append(f"err24h={error_context.get('recent_24h')}")
        latest_error_age = error_context.get("latest_error_age_minutes")
        if isinstance(latest_error_age, (int, float)):
            parts.append(f"lastErrAge={latest_error_age:.0f}m")
        if model_counts:
            top_models = sorted(model_counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
            parts.append("models=" + ",".join(f"{k}:{v}" for k, v in top_models))
        return " ".join(parts[:10])

    if "arima_beats_current_pair_count" in obj or "recommendation" in obj:
        parts = ["arimaChallengers"]
        if obj.get("recommendation"):
            parts.append(f"rec={obj.get('recommendation')}")
        if obj.get("matched_pairs_total") not in ("", None):
            parts.append(f"matched={obj.get('matched_pairs_total')}")
        if obj.get("arima_beats_current_pair_count") not in ("", None):
            parts.append(f"arimaWins={obj.get('arima_beats_current_pair_count')}")
        if obj.get("current_beats_arima_pair_count") not in ("", None):
            parts.append(f"currentWins={obj.get('current_beats_arima_pair_count')}")
        stable_pairs = obj.get("stable_pairs")
        if stable_pairs not in ("", None):
            stable_count = len(stable_pairs) if isinstance(stable_pairs, list) else stable_pairs
            parts.append(f"stable={stable_count}")
        if obj.get("horizon_minutes") not in ("", None):
            parts.append(f"h={obj.get('horizon_minutes')}m")
        challengers = obj.get("arima_challengers")
        if isinstance(challengers, list) and challengers:
            top = challengers[0] if isinstance(challengers[0], dict) else {}
            if top:
                parts.append(
                    "top="
                    f"{top.get('pair', '')}/"
                    f"{top.get('arima_model', top.get('model', ''))}/"
                    f"{_fmt_num(top.get('arima_mean_net_pips'), 1)}p"
                )
        return " ".join(parts[:9])

    best = obj.get("best") if isinstance(obj.get("best"), dict) else {}
    if not best:
        best = obj.get("top") if isinstance(obj.get("top"), dict) else {}
    if not best:
        leader = obj.get("leader") if isinstance(obj.get("leader"), dict) else {}
        best = leader
    if not best:
        return ""

    parts: list[str] = []
    for key in ("model", "model_type", "mode", "target", "instrument_subset"):
        value = best.get(key)
        if value not in ("", None):
            parts.append(str(value))
    lead = best.get("lead_minutes")
    move = best.get("move_horizon")
    execution = best.get("execution_horizon")
    if lead not in ("", None) or move not in ("", None) or execution not in ("", None):
        parts.append(f"lead={lead}m move={move}m exec={execution}m")
    auc = best.get("mean_event_auc", best.get("mean_auc"))
    ap = best.get("mean_event_ap_lift", best.get("ap_lift"))
    if isinstance(auc, (int, float)):
        parts.append(f"auc={auc:.3f}")
    if isinstance(ap, (int, float)):
        parts.append(f"apLift={ap:.2f}")
    weeks = best.get("positive_weeks")
    folds = best.get("fold_count")
    if weeks not in ("", None) or folds not in ("", None):
        parts.append(f"posWeeks={weeks}/{folds}")
    risk = best.get("selected_risk_summary") if isinstance(best.get("selected_risk_summary"), dict) else {}
    clustered = risk.get("clustered_first") if isinstance(risk.get("clustered_first"), dict) else {}
    raw = risk.get("raw") if isinstance(risk.get("raw"), dict) else {}
    clustered_pips = clustered.get("pips") if isinstance(clustered.get("pips"), dict) else {}
    raw_pips = raw.get("pips") if isinstance(raw.get("pips"), dict) else {}
    if clustered_pips:
        parts.append(
            f"cluster={clustered_pips.get('count', '')} mean={clustered_pips.get('mean', ''):.2f}p"
            if isinstance(clustered_pips.get("mean"), (int, float))
            else f"cluster={clustered_pips.get('count', '')}"
        )
    elif raw_pips:
        parts.append(
            f"trades={raw_pips.get('count', '')} mean={raw_pips.get('mean', ''):.2f}p"
            if isinstance(raw_pips.get("mean"), (int, float))
            else f"trades={raw_pips.get('count', '')}"
        )
    score = best.get("score")
    if isinstance(score, (int, float)):
        parts.append(f"score={score:.1f}")
    return " ".join(parts[:10])


def _partial_delay_compare_summary(path: Path) -> str:
    """Summarize completed per-delay reports while aggregate comparison runs."""
    parent = path.parent
    stem = path.stem
    if not parent.exists():
        return ""
    partials: list[dict[str, Any]] = []
    for partial_path in sorted(parent.glob(f"{stem}_delay*m.json")):
        obj = read_json(partial_path)
        if not isinstance(obj, dict):
            continue
        fresh = (obj.get("summary") or {}).get("fresh") if isinstance(obj.get("summary"), dict) else {}
        if not isinstance(fresh, dict):
            fresh = {}
        mfe60 = fresh.get("mfe_60m_usd") if isinstance(fresh.get("mfe_60m_usd"), dict) else {}
        final30 = fresh.get("final_30m_usd") if isinstance(fresh.get("final_30m_usd"), dict) else {}
        final60 = fresh.get("final_60m_usd") if isinstance(fresh.get("final_60m_usd"), dict) else {}
        partials.append(
            {
                "delay": obj.get("entry_delay_minutes"),
                "path": partial_path,
                "mfe60_sum": _float_or_none(mfe60.get("sum")),
                "mfe60_pos": _float_or_none(mfe60.get("positive_rate")),
                "final30_sum": _float_or_none(final30.get("sum")),
                "final60_sum": _float_or_none(final60.get("sum")),
                "count": mfe60.get("count") or final30.get("count") or final60.get("count"),
            }
        )
    if not partials:
        return ""
    best_mfe = max(
        partials,
        key=lambda row: (
            row.get("mfe60_sum") if isinstance(row.get("mfe60_sum"), (int, float)) else float("-inf"),
            row.get("mfe60_pos") if isinstance(row.get("mfe60_pos"), (int, float)) else float("-inf"),
        ),
    )
    completed = ",".join(str(row.get("delay")) for row in partials if row.get("delay") not in ("", None))
    label = "tightTrailPartial" if "tight_trail" in stem else "delayComparePartial"
    parts = [f"{label} completed={completed}m"]
    if best_mfe.get("delay") not in ("", None):
        parts.append(f"bestMFE={best_mfe.get('delay')}m")
    if isinstance(best_mfe.get("mfe60_sum"), (int, float)):
        parts.append(f"mfe60=${best_mfe.get('mfe60_sum'):.2f}")
    if isinstance(best_mfe.get("mfe60_pos"), (int, float)):
        parts.append(f"mfePos={best_mfe.get('mfe60_pos'):.0%}")
    final30_values = [
        row.get("final30_sum")
        for row in partials
        if isinstance(row.get("final30_sum"), (int, float))
    ]
    if final30_values:
        best_final30 = max(partials, key=lambda row: row.get("final30_sum") if isinstance(row.get("final30_sum"), (int, float)) else float("-inf"))
        parts.append(f"bestFinal30={best_final30.get('delay')}m/${best_final30.get('final30_sum'):.2f}")
    count = best_mfe.get("count")
    if count not in ("", None):
        parts.append(f"n={count}")
    return " ".join(parts[:7])


LIVE_VALIDATION_DUPLICATE_SOURCES = {
    "operator_scout_value_capture_v1",
    "operator_live_value_miss_followup_v1",
    "operator_live_ev_near_miss_followup_v1",
    "operator_live_unknown_profile_followup_v1",
    "operator_live_missed_spike_subset_followup_v1",
}


def _normalized_whitelist_key(value: Any) -> str:
    parts: set[str] = set()
    if isinstance(value, list):
        items = value
    else:
        items = str(value or "").replace(";", ",").split(",")
    for item in items:
        text = str(item or "").strip().upper().replace("/", "_").replace("-", "_")
        if text:
            parts.add(text)
    return ",".join(sorted(parts))


def _normalized_segment_key(value: Any) -> str:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    text = str(value or "").strip()
    if not text:
        return "{}"
    try:
        parsed = json.loads(text)
    except Exception:
        return text.lower()
    if isinstance(parsed, dict):
        return json.dumps(parsed, sort_keys=True)
    return str(parsed).lower()


def refresh_trainer_duplicate_validation_report(
    *,
    recent_rows: int = 240,
) -> dict[str, Any]:
    """Write a compact report of repeated live-validation baskets.

    This is report-only scheduler telemetry.  It helps verify whether the
    trainer is rotating to fresh validation work or repeatedly spending cycles
    on the same sorted whitelist/segment/model/target basket.
    """
    rows = tail_csv_rows(TRAINER_EXPERIMENT_LEDGER_CSV, max_rows=recent_rows)
    if len(rows) == 1 and rows[0].get("error") and len(rows[0]) == 1:
        payload = {
            "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "report_type": "trainer_duplicate_validation",
            "execution": "read_only_no_broker_writes",
            "error": rows[0].get("error", ""),
            "recent_rows": len(rows),
        }
        TRAINER_DUPLICATE_VALIDATION_JSON.parent.mkdir(parents=True, exist_ok=True)
        TRAINER_DUPLICATE_VALIDATION_JSON.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
        return payload

    validation_rows: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    profile_counts: Counter[str] = Counter()
    key_rows: dict[str, list[dict[str, Any]]] = {}
    source_windows: dict[str, list[tuple[str, int]]] = {}

    def duplicate_window_summary(window_rows: list[dict[str, Any]]) -> dict[str, Any]:
        window_key_counts: Counter[str] = Counter()
        window_source_counts: Counter[str] = Counter()
        validation_count = 0
        for window_row in window_rows:
            window_source = str(window_row.get("source") or "").lower()
            window_stage = str(window_row.get("evaluation_stage") or "").lower()
            if window_stage != "validation" or window_source not in LIVE_VALIDATION_DUPLICATE_SOURCES:
                continue
            validation_count += 1
            window_source_counts[window_source] += 1
            window_model = str(window_row.get("model_type") or "").lower()
            window_target = str(window_row.get("target") or "").lower()
            window_whitelist = _normalized_whitelist_key(window_row.get("instrument_whitelist"))
            window_subset = str(window_row.get("instrument_subset") or "").strip().lower()
            if not window_whitelist:
                window_whitelist = f"subset:{window_subset or 'all'}"
            window_segment_key = _normalized_segment_key(window_row.get("segment_filters"))
            window_key_counts[
                "|".join(
                    [
                        window_source,
                        window_model,
                        window_target,
                        window_segment_key.lower(),
                        window_whitelist.lower(),
                    ]
                )
            ] += 1
        duplicate_rows = sum(count for count in window_key_counts.values() if count > 1)
        duplicate_keys = sum(1 for count in window_key_counts.values() if count > 1)
        return {
            "input_rows": len(window_rows),
            "validation_rows": validation_count,
            "unique_keys": len(window_key_counts),
            "duplicate_rows": duplicate_rows,
            "duplicate_relaxed_keys": duplicate_keys,
            "duplicate_row_rate": (
                round(duplicate_rows / validation_count, 4)
                if validation_count
                else 0.0
            ),
            "source_counts": window_source_counts.most_common(12),
        }

    duplicate_windows: dict[str, dict[str, Any]] = {}
    for window in (30, 60, 120, 240):
        window_rows = rows[-window:]
        window_counts: Counter[str] = Counter()
        for window_row in window_rows:
            window_source = str(window_row.get("source") or "").lower()
            window_stage = str(window_row.get("evaluation_stage") or "").lower()
            if (
                window_stage == "validation"
                and window_source in LIVE_VALIDATION_DUPLICATE_SOURCES
            ):
                window_counts[window_source] += 1
        source_windows[str(window)] = window_counts.most_common(12)
        duplicate_windows[str(window)] = duplicate_window_summary(window_rows)
    for row in rows:
        source = str(row.get("source") or "").lower()
        stage = str(row.get("evaluation_stage") or "").lower()
        if stage != "validation" or source not in LIVE_VALIDATION_DUPLICATE_SOURCES:
            continue
        model = str(row.get("model_type") or "").lower()
        target = str(row.get("target") or "").lower()
        whitelist = _normalized_whitelist_key(row.get("instrument_whitelist"))
        instrument_subset = str(row.get("instrument_subset") or "").strip().lower()
        if not whitelist:
            whitelist = f"subset:{instrument_subset or 'all'}"
        segment_key = _normalized_segment_key(row.get("segment_filters"))
        relaxed_key = "|".join([source, model, target, segment_key.lower(), whitelist.lower()])
        compact = {
            "source": source,
            "status": str(row.get("status") or "").lower(),
            "model_type": model,
            "target": target,
            "instrument_subset": instrument_subset,
            "instrument_whitelist": whitelist,
            "segment_filters": segment_key,
            "specialist_profile": str(row.get("specialist_profile") or ""),
            "validation_profile": str(row.get("validation_profile") or ""),
            "execution_policy": str(row.get("execution_policy") or ""),
            "gate_passed": str(row.get("gate_passed") or ""),
            "mean_auc": row.get("mean_auc", ""),
            "minimum_week_auc": row.get("minimum_week_auc", ""),
            "mean_brier_skill": row.get("mean_brier_skill", ""),
            "mean_net_pips": row.get("mean_net_pips", ""),
            "trades": row.get("trades", ""),
            "relaxed_key": relaxed_key,
        }
        validation_rows.append(compact)
        key_rows.setdefault(relaxed_key, []).append(compact)
        source_counts[source] += 1
        status_counts[compact["status"]] += 1
        profile = compact["specialist_profile"].lower()
        if profile:
            profile_counts[profile] += 1

    duplicate_groups = [
        {
            "relaxed_key": key,
            "count": len(group),
            "source": group[-1].get("source", ""),
            "model_type": group[-1].get("model_type", ""),
            "target": group[-1].get("target", ""),
            "instrument_subset": group[-1].get("instrument_subset", ""),
            "instrument_whitelist": group[-1].get("instrument_whitelist", ""),
            "segment_filters": group[-1].get("segment_filters", ""),
            "latest_status": group[-1].get("status", ""),
            "latest_profile": group[-1].get("specialist_profile", ""),
            "latest_mean_auc": group[-1].get("mean_auc", ""),
            "latest_min_week_auc": group[-1].get("minimum_week_auc", ""),
            "latest_mean_net_pips": group[-1].get("mean_net_pips", ""),
            "latest_trades": group[-1].get("trades", ""),
        }
        for key, group in key_rows.items()
        if len(group) > 1
    ]
    duplicate_groups.sort(key=lambda row: (row["count"], row["source"]), reverse=True)
    duplicate_rows = sum(row["count"] for row in duplicate_groups)
    payload = {
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "report_type": "trainer_duplicate_validation",
        "execution": "read_only_no_broker_writes",
        "ledger_path": str(TRAINER_EXPERIMENT_LEDGER_CSV),
        "recent_rows": len(rows),
        "validation_rows": len(validation_rows),
        "duplicate_relaxed_keys": len(duplicate_groups),
        "duplicate_rows": duplicate_rows,
        "duplicate_row_rate": (
            round(duplicate_rows / len(validation_rows), 4)
            if validation_rows
            else 0.0
        ),
        "source_counts": source_counts.most_common(12),
        "source_windows": source_windows,
        "duplicate_windows": duplicate_windows,
        "status_counts": status_counts.most_common(12),
        "top_profiles": profile_counts.most_common(12),
        "top_duplicates": duplicate_groups[:20],
        "csv_path": str(TRAINER_DUPLICATE_VALIDATION_CSV),
    }
    TRAINER_DUPLICATE_VALIDATION_JSON.parent.mkdir(parents=True, exist_ok=True)
    TRAINER_DUPLICATE_VALIDATION_JSON.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    with TRAINER_DUPLICATE_VALIDATION_CSV.open("w", newline="", encoding="utf-8") as handle:
        fields = [
            "count",
            "source",
            "model_type",
            "target",
            "instrument_subset",
            "instrument_whitelist",
            "latest_status",
            "latest_profile",
            "latest_mean_auc",
            "latest_min_week_auc",
            "latest_mean_net_pips",
            "latest_trades",
            "segment_filters",
            "relaxed_key",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in duplicate_groups:
            writer.writerow({field: row.get(field, "") for field in fields})
    return payload


def model_testing_brief() -> dict[str, Any]:
    """Compact model/backtest testing context for advisor snapshots."""
    artifacts: list[dict[str, Any]] = []
    refresh_trainer_duplicate_validation_report()
    for name, path in MODEL_TEST_ARTIFACTS.items():
        obj = read_json(path)
        generated_utc = obj.get("generated_utc", "") if isinstance(obj, dict) else ""
        exists = path.exists()
        best_summary = _best_metric_summary(obj) if isinstance(obj, dict) else ""
        if name in {"missed_spike_delay_compare", "missed_spike_tight_trail"} and not best_summary:
            best_summary = _partial_delay_compare_summary(path)
        artifacts.append(
            {
                "name": name,
                "path": str(path),
                "exists": bool(exists or best_summary),
                "file_age_minutes": file_age_minutes(path),
                "generated_utc": generated_utc,
                "generated_age_minutes": age_minutes_from_iso(generated_utc),
                "best_summary": best_summary,
                "error": obj.get("error", "") if isinstance(obj, dict) else "",
            }
        )
    reference_candidates = sorted(
        TRAINER_REPORT_DIR.glob("*reference_strategy_mimic*.json"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
        reverse=True,
    )
    if reference_candidates:
        path = reference_candidates[0]
        obj = read_json(path)
        generated_utc = obj.get("generated_utc", "") if isinstance(obj, dict) else ""
        artifacts.append(
            {
                "name": "reference_strategy_mimic",
                "path": str(path),
                "exists": path.exists(),
                "file_age_minutes": file_age_minutes(path),
                "generated_utc": generated_utc,
                "generated_age_minutes": age_minutes_from_iso(generated_utc),
                "best_summary": _best_metric_summary(obj) if isinstance(obj, dict) else "",
                "error": obj.get("error", "") if isinstance(obj, dict) else "",
            }
        )
    return {
        "active_processes": research_process_status(),
        "artifacts": artifacts,
    }


def trainer_report_brief() -> dict[str, Any]:
    """Compact always-on trainer context for the report-only note."""
    refresh = maybe_refresh_trainer_report()
    obj = read_json(TRAINER_REPORT_JSON)
    if not obj:
        return {
            "path": str(TRAINER_REPORT_JSON),
            "exists": TRAINER_REPORT_JSON.exists(),
            "refresh": refresh,
            "error": "trainer report missing or empty",
        }
    status = obj.get("trainer_status") if isinstance(obj.get("trainer_status"), dict) else {}
    status = dict(status)
    queue = obj.get("queue_summary") if isinstance(obj.get("queue_summary"), dict) else {}
    live_shadow = obj.get("live_shadow") if isinstance(obj.get("live_shadow"), dict) else {}
    promotion = obj.get("promotion_readiness") if isinstance(obj.get("promotion_readiness"), dict) else {}
    research_state = read_json(TRAINER_RESEARCH_STATE_JSON)
    current_spec = (
        research_state.get("current_experiment_spec")
        if isinstance(research_state, dict)
        and isinstance(research_state.get("current_experiment_spec"), dict)
        else {}
    )
    if current_spec:
        status.update(
            {
                "research_heartbeat_utc": research_state.get(
                    "last_research_heartbeat_utc",
                    status.get("research_heartbeat_utc", ""),
                ),
                "current_experiment_status": research_state.get(
                    "current_experiment_status",
                    status.get("current_experiment_status", ""),
                ),
                "current_experiment_started_utc": research_state.get(
                    "current_experiment_started_utc",
                    status.get("current_experiment_started_utc", ""),
                ),
                "current_spec_source": current_spec.get("source", ""),
                "current_spec_stage": current_spec.get("evaluation_stage", ""),
                "current_spec_dataset_kind": current_spec.get("dataset_kind", ""),
                "current_spec_model_type": current_spec.get("model_type", ""),
                "current_spec_target": current_spec.get("target", ""),
                "current_spec_outcome": current_spec.get("outcome", ""),
                "current_spec_feature_set": current_spec.get("feature_set", ""),
                "current_spec_instrument_subset": current_spec.get("instrument_subset", ""),
                "current_spec_specialist_profile": current_spec.get("specialist_profile", ""),
                "current_spec_validation_profile": current_spec.get("validation_profile", ""),
                "current_spec_instrument_whitelist": current_spec.get("instrument_whitelist", []),
                "current_spec_effective_stage": current_spec.get("effective_evaluation_stage", ""),
                "current_spec_effective_holdout_weeks": current_spec.get("effective_holdout_weeks", ""),
                "current_spec_effective_max_train_rows": current_spec.get("effective_max_train_rows", ""),
                "current_spec_requested_max_train_rows": current_spec.get("requested_max_train_rows", ""),
                "current_spec_max_train_rows_capped": current_spec.get("max_train_rows_capped", ""),
            }
        )
    ensemble_refresh_raw = (
        research_state.get("latest_ensemble_shadow_refresh")
        if isinstance(research_state.get("latest_ensemble_shadow_refresh"), dict)
        else {}
    )
    ensemble_refresh = {
        "status": ensemble_refresh_raw.get("status", ""),
        "time_utc": ensemble_refresh_raw.get("time_utc", ""),
        "age_minutes": age_minutes_from_iso(ensemble_refresh_raw.get("time_utc", "")),
        "execution": ensemble_refresh_raw.get("execution", ""),
        "timeout_seconds": ensemble_refresh_raw.get("timeout_seconds", ""),
        "reason": str(ensemble_refresh_raw.get("reason", ""))[:220],
        "error": str(ensemble_refresh_raw.get("error", ""))[:260],
    }
    ensemble_summary_path = MODEL_TEST_ARTIFACTS["ensemble_summary"]
    ensemble_summary_obj = read_json(ensemble_summary_path)
    latest_ensemble_artifact = {
        "path": str(ensemble_summary_path),
        "exists": ensemble_summary_path.exists(),
        "file_age_minutes": file_age_minutes(ensemble_summary_path),
        "generated_utc": ensemble_summary_obj.get("generated_utc", "") if isinstance(ensemble_summary_obj, dict) else "",
        "generated_age_minutes": age_minutes_from_iso(
            ensemble_summary_obj.get("generated_utc", "") if isinstance(ensemble_summary_obj, dict) else ""
        ),
        "best_summary": _best_metric_summary(ensemble_summary_obj) if isinstance(ensemble_summary_obj, dict) else "",
    }
    latest_seed_obj = read_json(LATEST_ACCOUNT_IMPROVEMENT_SEED_JSON)
    seeded_by_lane = (
        latest_seed_obj.get("seeded_by_lane")
        if isinstance(latest_seed_obj, dict) and isinstance(latest_seed_obj.get("seeded_by_lane"), dict)
        else {}
    )
    latest_seed = {
        "path": str(LATEST_ACCOUNT_IMPROVEMENT_SEED_JSON),
        "exists": LATEST_ACCOUNT_IMPROVEMENT_SEED_JSON.exists(),
        "generated_utc": latest_seed_obj.get("generated_utc", "") if isinstance(latest_seed_obj, dict) else "",
        "age_minutes": age_minutes_from_iso(
            latest_seed_obj.get("generated_utc", "") if isinstance(latest_seed_obj, dict) else ""
        ),
        "execution": latest_seed_obj.get("execution", "") if isinstance(latest_seed_obj, dict) else "",
        "seeded_total": latest_seed_obj.get("seeded_total", "") if isinstance(latest_seed_obj, dict) else "",
        "seeded_by_lane": seeded_by_lane,
        "recent_regime_lock_seed": recent_regime_lock_seed_summary(),
    }
    stopcap = obj.get("stopcap_comparison") if isinstance(obj.get("stopcap_comparison"), dict) else {}
    segment_leaders = obj.get("segment_leaders") if isinstance(obj.get("segment_leaders"), dict) else {}
    account_models = obj.get("account_model_improvement") if isinstance(obj.get("account_model_improvement"), dict) else {}
    lanes = account_models.get("lanes") if isinstance(account_models.get("lanes"), list) else []
    live_miss_queue = trainer_live_miss_queue_brief()

    lane_rows = []
    for lane in lanes[:5]:
        if not isinstance(lane, dict):
            continue
        lane_rows.append(
            {
                "account_lane": lane.get("account_lane", ""),
                "completed_specs": lane.get("completed_specs", ""),
                "pending_specs": lane.get("pending_specs", ""),
                "coverage_completed_pct": lane.get("coverage_completed_pct", ""),
                "top_model": lane.get("top_candidate_model_type", ""),
                "top_target": lane.get("top_candidate_target", ""),
                "top_subset": lane.get("top_candidate_subset", ""),
                "top_score": lane.get("top_candidate_readiness_score", ""),
                "next_action": lane.get("next_research_action", ""),
            }
        )

    top_stopcap = []
    for row in (stopcap.get("top_avg_net") if isinstance(stopcap.get("top_avg_net"), list) else [])[:3]:
        if not isinstance(row, dict):
            continue
        top_stopcap.append(
            {
                "model_type": row.get("model_type", ""),
                "target": row.get("target", ""),
                "subset": row.get("instrument_subset", ""),
                "policy": row.get("policy", ""),
                "avg_mean_net_pips": row.get("avg_mean_net_pips", ""),
                "avg_mean_auc": row.get("avg_mean_auc", ""),
                "gate_passed_runs": row.get("gate_passed_runs", ""),
                "runs": row.get("runs", ""),
            }
        )

    return {
        "path": str(TRAINER_REPORT_JSON),
        "exists": TRAINER_REPORT_JSON.exists(),
        "refresh": refresh,
        "generated_utc": obj.get("generated_utc", ""),
        "age_minutes": age_minutes_from_iso(obj.get("generated_utc", "")),
        "execution": obj.get("execution", ""),
        "research_loop_mode": status.get("research_loop_mode", ""),
        "heartbeat_utc": status.get("research_heartbeat_utc", ""),
        "heartbeat_age_minutes": age_minutes_from_iso(status.get("research_heartbeat_utc", "")),
        "current": {
            "source": status.get("current_spec_source", ""),
            "model_type": status.get("current_spec_model_type", ""),
            "target": status.get("current_spec_target", ""),
            "stage": status.get("current_spec_stage", ""),
            "outcome": status.get("current_spec_outcome", ""),
            "status": status.get("current_experiment_status", ""),
            "subset": status.get("current_spec_instrument_subset", ""),
            "profile": status.get("current_spec_specialist_profile", ""),
            "validation_profile": status.get("current_spec_validation_profile", ""),
            "whitelist": status.get("current_spec_instrument_whitelist", []),
            "effective_stage": status.get("current_spec_effective_stage", ""),
            "effective_holdout_weeks": status.get("current_spec_effective_holdout_weeks", ""),
            "effective_max_train_rows": status.get("current_spec_effective_max_train_rows", ""),
            "requested_max_train_rows": status.get("current_spec_requested_max_train_rows", ""),
            "max_train_rows_capped": status.get("current_spec_max_train_rows_capped", ""),
        },
        "queue": {
            "queue_rows": queue.get("queue_rows", status.get("lifecycle_queue_rows", "")),
            "pending_unique_specs": queue.get("pending_unique_specs", status.get("pending_queued_specs", "")),
            "completed_unique_specs": queue.get("completed_unique_specs", status.get("lifecycle_completed_unique_specs", "")),
        },
        "promotion": {
            "validation_grade_count": promotion.get("validation_grade_count", ""),
            "robust_followup_count": promotion.get("robust_followup_count", ""),
            "specialist_validation_grade_count": promotion.get("specialist_validation_grade_count", ""),
        },
        "live_shadow": {
            "latest_scan_time_utc": live_shadow.get("latest_scan_time_utc", ""),
            "top_candidate": live_shadow.get("latest_scan_top_candidate", ""),
            "top_reject_reason": live_shadow.get("latest_scan_top_reject_reason", ""),
        },
        "ensemble_refresh": ensemble_refresh,
        "latest_ensemble_artifact": latest_ensemble_artifact,
        "latest_seed": latest_seed,
        "live_miss_queue": live_miss_queue,
        "live_miss_results": trainer_live_miss_followup_result_brief(),
        "successful_missed_spike_candidates": _successful_missed_spike_segment_candidates(
            segment_leaders
        ),
        "account_model_lanes": lane_rows,
        "top_stopcap": top_stopcap,
        "error": obj.get("error", ""),
    }


def _first_existing_artifact(model_tests: dict[str, Any], name: str) -> dict[str, Any]:
    artifacts = model_tests.get("artifacts") if isinstance(model_tests.get("artifacts"), list) else []
    for artifact in artifacts:
        if isinstance(artifact, dict) and artifact.get("name") == name:
            return artifact
    return {}


def _top_value_instruments_text(move_brief: dict[str, Any], *, limit: int = 4) -> str:
    current = move_brief.get("current") if isinstance(move_brief.get("current"), dict) else {}
    value_summary = (
        current.get("miss_value_summary")
        if isinstance(current.get("miss_value_summary"), dict)
        else {}
    )
    rows = (
        value_summary.get("top_value_instruments")
        if isinstance(value_summary.get("top_value_instruments"), list)
        else []
    )
    parts: list[str] = []
    for row in rows[:limit]:
        if not isinstance(row, dict) or not row.get("instrument"):
            continue
        parts.append(f"{row.get('instrument')}:${float(row.get('value_usd') or 0.0):.2f}")
    return ", ".join(parts)


def missed_spike_delay_replay_decision() -> str:
    path = MODEL_TEST_ARTIFACTS.get("missed_spike_delay_compare")
    if not isinstance(path, Path) or not path.exists():
        return ""
    obj = read_json(path)
    rows = obj.get("delay_comparison") if isinstance(obj.get("delay_comparison"), list) else []
    if not rows:
        return ""

    def row_value(row: dict[str, Any], key: str) -> float:
        try:
            return float(row.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    best_final_60 = max(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: row_value(row, "fresh_final_60m_usd_sum"),
        default={},
    )
    best_final_30 = max(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: row_value(row, "fresh_final_30m_usd_sum"),
        default={},
    )
    best_mfe_60 = max(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: row_value(row, "fresh_mfe_60m_usd_sum"),
        default={},
    )
    final_60 = row_value(best_final_60, "fresh_final_60m_usd_sum")
    final_30 = row_value(best_final_30, "fresh_final_30m_usd_sum")
    mfe_60 = row_value(best_mfe_60, "fresh_mfe_60m_usd_sum")
    final_60_delay = best_final_60.get("entry_delay_minutes", "")
    final_30_delay = best_final_30.get("entry_delay_minutes", "")
    mfe_delay = best_mfe_60.get("entry_delay_minutes", "")
    age = age_minutes_from_iso(obj.get("generated_utc", ""))
    age_text = f" age={age}m" if age not in ("", None) else ""
    if final_60 < 0 and final_30 < 0 and mfe_60 > 0:
        return (
            "Late-entry replay: chase-only exits remain negative "
            f"(best final30=${final_30:.2f}@{final_30_delay}m, "
            f"best final60=${final_60:.2f}@{final_60_delay}m) while MFE60 is positive "
            f"(${mfe_60:.2f}@{mfe_delay}m); prioritize pre-spike lead selection and exit-curve timing over simple post-spike chasing."
            f"{age_text}"
        )
    return (
        "Late-entry replay: "
        f"best final30=${final_30:.2f}@{final_30_delay}m, "
        f"best final60=${final_60:.2f}@{final_60_delay}m, "
        f"best MFE60=${mfe_60:.2f}@{mfe_delay}m."
        f"{age_text}"
    )


def advisor_decision_board(
    *,
    accounts: dict[str, Any],
    processes: dict[str, Any],
    move_brief: dict[str, Any],
    trainer_brief: dict[str, Any],
    model_tests: dict[str, Any],
    watch_items: list[str],
    account_advisor_only: bool = False,
) -> list[str]:
    """Translate the read-only snapshot into account/model guidance.

    This is intentionally report-only.  It does not change broker state, live
    thresholds, promotions, or manager behavior; it just makes the advisor note
    easier to act on during the first-24h monitor window.
    """

    decisions: list[str] = []
    gpt = accounts.get("gpt") if isinstance(accounts.get("gpt"), dict) else {}
    tech = accounts.get("tech") if isinstance(accounts.get("tech"), dict) else {}
    primary = accounts.get("primary") if isinstance(accounts.get("primary"), dict) else {}
    gpt_open = int(gpt.get("open_trades") or 0)
    tech_open = int(tech.get("open_trades") or 0)
    primary_open = int(primary.get("open_trades") or 0)

    if account_advisor_only:
        if gpt_open:
            decisions.append(
                f"GPT lane: {gpt_open} open trade(s); observation/reporting only, no manual broker action from this monitor."
            )
        else:
            decisions.append(
                "GPT lane: flat; observation/reporting only, no live manager restart from this monitor."
            )
    elif not (processes.get("gpt") or {}).get("running"):
        if gpt_open:
            decisions.append(
                f"GPT lane: manager is down with {gpt_open} open trade(s); monitor exposure, but do not alter GPT behavior from this report."
            )
        else:
            decisions.append(
                "GPT lane: flat and manager is down; keep it untouched unless live GPT execution is explicitly requested."
            )
    else:
        decisions.append(
            f"GPT lane: manager running with {gpt_open} open trade(s); advisor role is observe/escalate only."
        )

    non_gpt_open = tech_open + primary_open
    if non_gpt_open:
        decisions.append(
            f"Scout/technical lanes: {non_gpt_open} small protected live trade(s) are open across tech/primary; no manual broker action recommended from this poll."
        )
    elif (
        not account_advisor_only
        and not ((processes.get("tech") or {}).get("running") or (processes.get("primary") or {}).get("running"))
    ):
        decisions.append(
            "Scout/technical lanes: both execution managers are down and flat; use this window for research/replay unless live execution is explicitly restarted."
        )

    current_moves = move_brief.get("current") if isinstance(move_brief.get("current"), dict) else {}
    missed_value = (
        current_moves.get("miss_value_summary")
        if isinstance(current_moves.get("miss_value_summary"), dict)
        else {}
    )
    missed_value_usd = float(missed_value.get("total_value_usd") or 0.0)
    near_value_usd = float(missed_value.get("near_value_usd") or 0.0)
    top_value_text = _top_value_instruments_text(move_brief)
    if missed_value_usd > 0:
        decisions.append(
            "Missed-move focus: today's value-weighted misses are the training target "
            f"(${missed_value_usd:.2f} deduped; near-gate ${near_value_usd:.2f}"
            + (f"; top {top_value_text}" if top_value_text else "")
            + ")."
        )

    if not account_advisor_only:
        promotion = (
            trainer_brief.get("promotion")
            if isinstance(trainer_brief.get("promotion"), dict)
            else {}
        )
        robust_count = int(promotion.get("robust_followup_count") or 0)
        validation_count = int(promotion.get("validation_grade_count") or 0)
        if robust_count <= 0 and validation_count <= 0:
            decisions.append(
                "Promotion posture: no non-GPT model is promotion-ready; lead-only or short-fold wins should remain research/shadow until robust follow-up clears."
            )
        else:
            decisions.append(
                f"Promotion posture: {validation_count} validation-grade / {robust_count} robust candidate(s) exist; inspect before any live mapping."
            )

        ensemble = (
            trainer_brief.get("latest_ensemble_artifact")
            if isinstance(trainer_brief.get("latest_ensemble_artifact"), dict)
            else {}
        )
        ensemble_best = str(ensemble.get("best_summary") or "")
        if ensemble.get("exists") and "gate=pass" in ensemble_best:
            decisions.append(
                "Model testing: the multi-model ensemble has a passing shadow holdout; next useful work is live-shadow mapping/risk simulation, not direct production promotion."
            )
        elif ensemble.get("exists"):
            decisions.append(
                "Model testing: ensemble artifact exists but is not a deployable pass in the latest summary."
            )

        arima_artifact = _first_existing_artifact(model_tests, "arima_challengers")
        arima_best = str(arima_artifact.get("best_summary") or "")
        if arima_best:
            decisions.append(
                f"ARIMA baseline: keep it as pair-level challenger evidence ({arima_best[:180]}), not a broad account replacement."
            )

        duplicate_artifact = _first_existing_artifact(model_tests, "trainer_duplicate_validation")
        duplicate_best = str(duplicate_artifact.get("best_summary") or "")
        if "dupRows=" in duplicate_best:
            decisions.append(
                f"Trainer efficiency: duplicate pressure remains visible ({duplicate_best[:150]}), so queue pruning/priority rotation should stay active."
            )

        replay_decision = missed_spike_delay_replay_decision()
        if replay_decision:
            decisions.append(replay_decision)

    if any("bad_day" in item for item in watch_items):
        decisions.append(
            "Risk lesson: current-day tech recap is still a bad-day sample; use it for stop/reentry audits before increasing non-GPT live aggressiveness."
        )

    return decisions


def build_advisor_note(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a compact report-only advisor summary from one monitor snapshot."""
    accounts = payload.get("accounts") if isinstance(payload.get("accounts"), dict) else {}
    processes = payload.get("processes") if isinstance(payload.get("processes"), dict) else {}
    scouts = payload.get("scouts") if isinstance(payload.get("scouts"), dict) else {}
    manager_states = (
        payload.get("manager_states")
        if isinstance(payload.get("manager_states"), dict)
        else {}
    )
    research = payload.get("gpt_research") if isinstance(payload.get("gpt_research"), dict) else {}
    daily_report = payload.get("daily_report") if isinstance(payload.get("daily_report"), dict) else {}
    account_advisor_only = account_advisor_only_mode()
    move_brief = daily_move_brief(daily_report)
    if account_advisor_only:
        trainer_brief: dict[str, Any] = {}
        model_tests: dict[str, Any] = {}
    else:
        trainer_brief = trainer_report_brief()
        model_tests = model_testing_brief()
    daily_recaps = payload.get("daily_recaps") if isinstance(payload.get("daily_recaps"), dict) else {}
    error_logs = payload.get("error_logs") if isinstance(payload.get("error_logs"), dict) else {}
    system_resources = (
        payload.get("system_resources")
        if isinstance(payload.get("system_resources"), dict)
        else {}
    )
    memory_status = (
        system_resources.get("memory")
        if isinstance(system_resources.get("memory"), dict)
        else {}
    )
    scout_audits = {name: scout_audit_brief(name) for name in ("tech", "primary")}
    expected_offline_processes = (
        {"gpt", "tech", "primary", "trainer", "depth_collector", "canary"}
        if account_advisor_only
        else set()
    )

    watch_items: list[str] = []
    if memory_status.get("error"):
        watch_items.append(f"system memory read error: {memory_status.get('error')}")
    elif memory_status:
        try:
            available_mb = float(memory_status.get("available_mb") or 0.0)
        except (TypeError, ValueError):
            available_mb = 0.0
        try:
            used_pct = float(memory_status.get("used_pct") or 0.0)
        except (TypeError, ValueError):
            used_pct = 0.0
        if available_mb and available_mb < SYSTEM_MEMORY_FREE_WARN_MB:
            watch_items.append(
                f"system memory low: free={available_mb:.0f}MB "
                f"used={used_pct:.0f}% threshold={SYSTEM_MEMORY_FREE_WARN_MB:.0f}MB"
            )
        elif used_pct >= SYSTEM_MEMORY_USED_WARN_PCT:
            watch_items.append(
                f"system memory high: used={used_pct:.0f}% "
                f"free={available_mb:.0f}MB threshold={SYSTEM_MEMORY_USED_WARN_PCT:.0f}%"
            )
    for name in ("gpt", "tech", "primary", "trainer", "depth_collector", "canary", "day_monitor"):
        if name in expected_offline_processes:
            continue
        if not (processes.get(name) or {}).get("running"):
            watch_items.append(f"{name} process is not running")
    trainer_process = processes.get("trainer") if isinstance(processes.get("trainer"), dict) else {}
    if trainer_process.get("running"):
        try:
            trainer_working_set_mb = float(trainer_process.get("working_set_mb") or 0.0)
        except (TypeError, ValueError):
            trainer_working_set_mb = 0.0
        if trainer_working_set_mb >= CORE_TRAINER_MEMORY_WARN_MB:
            watch_items.append(
                f"trainer process high memory: pid={trainer_process.get('pid', '')} "
                f"rss={trainer_working_set_mb:.0f}MB "
                f"threshold={CORE_TRAINER_MEMORY_WARN_MB:.0f}MB"
            )
    for name in ("gpt", "tech", "primary"):
        acct = accounts.get(name) if isinstance(accounts.get(name), dict) else {}
        if acct.get("error"):
            watch_items.append(f"{name} account read error: {acct.get('error')}")
        else:
            watch_items.extend(_currency_exposure_conflicts(name, acct))
    for name, scout in scouts.items():
        process_running = bool((processes.get(name) or {}).get("running"))
        if (
            isinstance(scout, dict)
            and scout.get("scan_stale")
            and process_running
            and not scout.get("scan_expected_paused")
        ):
            watch_items.append(f"{name} scout scan is stale: age={scout.get('age_minutes')}m")
    for name, state in manager_states.items():
        if not isinstance(state, dict):
            continue
        if state.get("error"):
            watch_items.append(f"{name} manager state read error: {state.get('error')}")
            continue
        if not state.get("exists"):
            watch_items.append(f"{name} manager state file missing")
            continue
        age = state.get("last_monitor_age_minutes")
        if not isinstance(age, (int, float)):
            age = state.get("mtime_age_minutes")
        if (
            isinstance(age, (int, float))
            and age > MANAGER_STATE_STALE_AFTER_MINUTES
            and (processes.get(name) or {}).get("running")
        ):
            watch_items.append(f"{name} manager state stale: age={age}m")
    if daily_report.get("error"):
        watch_items.append(f"daily move report error: {daily_report.get('error')}")
    elif (
        isinstance(daily_report.get("age_minutes"), (int, float))
        and daily_report["age_minutes"] > DAILY_REPORT_STALE_AFTER_MINUTES
    ):
        watch_items.append(f"daily move report stale: age={daily_report.get('age_minutes')}m")
    if not account_advisor_only:
        if trainer_brief.get("error"):
            watch_items.append(f"trainer report error: {trainer_brief.get('error')}")
        trainer_refresh = trainer_brief.get("refresh") if isinstance(trainer_brief.get("refresh"), dict) else {}
        if trainer_refresh.get("error"):
            watch_items.append(f"trainer report refresh error: {trainer_refresh.get('error')}")
        if isinstance(trainer_brief.get("heartbeat_age_minutes"), (int, float)) and trainer_brief["heartbeat_age_minutes"] > 15:
            watch_items.append(f"trainer heartbeat stale: age={trainer_brief.get('heartbeat_age_minutes')}m")
        active_model_processes = (
            model_tests.get("active_processes") if isinstance(model_tests.get("active_processes"), list) else []
        )
        active_scripts = {
            str(proc.get("script") or "")
            for proc in active_model_processes
            if isinstance(proc, dict)
        }
        active_commands = [
            str(proc.get("command") or "")
            for proc in active_model_processes
            if isinstance(proc, dict)
        ]
        for proc in active_model_processes:
            if not isinstance(proc, dict):
                continue
            try:
                working_set_mb = float(proc.get("working_set_mb") or 0.0)
            except (TypeError, ValueError):
                working_set_mb = 0.0
            if working_set_mb >= 6000.0:
                watch_items.append(
                    f"standalone research process high memory: "
                    f"{proc.get('script', '')} pid={proc.get('pid', '')} "
                    f"rss={working_set_mb:.0f}MB age={proc.get('age_minutes', '')}m"
                )
        pre_spike_focus_active = any(
            "latest_pre_spike_lead_trainer_focus" in command
            for command in active_commands
        )
        pre_spike_focus_recent = False
        for artifact in model_tests.get("artifacts", []):
            if not isinstance(artifact, dict):
                continue
            if artifact.get("name") != "pre_spike_trainer_focus":
                continue
            focus_age = artifact.get("file_age_minutes")
            pre_spike_focus_recent = isinstance(focus_age, (int, float)) and focus_age <= 480
            break
        for artifact in model_tests.get("artifacts", []):
            if not isinstance(artifact, dict) or not artifact.get("exists"):
                continue
            age = artifact.get("file_age_minutes")
            if (
                isinstance(age, (int, float))
                and age > 480
                and artifact.get("name") == "pre_spike_lead"
                and not (pre_spike_focus_active or pre_spike_focus_recent)
            ):
                watch_items.append(f"{artifact.get('name')} report stale: file_age={age}m")
            if (
                isinstance(age, (int, float))
                and age > 480
                and artifact.get("name") == "missed_spike_replay"
            ):
                watch_items.append(f"{artifact.get('name')} report stale: file_age={age}m")
            if (
                artifact.get("name") == "ensemble_summary"
                and isinstance(age, (int, float))
                and age > 480
                and "oanda_ensemble_candidate_backtester.py" not in active_scripts
            ):
                watch_items.append(f"ensemble summary stale and no ensemble backtester running: file_age={age}m")
    for name, recap in daily_recaps.items():
        if isinstance(recap, dict) and recap.get("bad_day") and not recap.get("date_stale"):
            watch_items.append(
                f"{name} current-day recap bad_day: closed={recap.get('closed')} loss_rate={recap.get('loss_rate')}"
            )
            audited_losers = recap.get("tighter_stop_audited_losers")
            helpful_stop = recap.get("tighter_stop_most_helpful_pips")
            if audited_losers and helpful_stop:
                watch_items.append(
                    f"{name} tighter-stop audit: audited_losers={audited_losers} best_candidate={helpful_stop}p"
                )
            losses_with_reentry = recap.get("losses_with_reentry")
            if losses_with_reentry:
                watch_items.append(f"{name} same-day loss/reentry pattern count={losses_with_reentry}")
    for name, meta in error_logs.items():
        if account_advisor_only and name == "canary":
            continue
        if isinstance(meta, dict) and meta.get("size_bytes"):
            age = meta.get("age_minutes")
            age_text = f" age={age}m" if age not in ("", None) else ""
            stale_text = ""
            if isinstance(age, (int, float)) and age > 360:
                stale_text = " stale"
            watch_items.append(
                f"{name}{stale_text} error log non-empty: "
                f"{meta.get('size_bytes')} bytes{age_text}"
            )

    notes = []
    if account_advisor_only:
        notes.append(
            "Monitor mode: account-advisor-only; execution/training sidecars are intentionally not managed."
        )
    verdict = research.get("local_verdict", "")
    if verdict:
        notes.append(
            "GPT research: "
            f"verdict={verdict} mode={research.get('portfolio_mode', '')} "
            f"bias={research.get('portfolio_bias', '')} "
            f"underdeployment={research.get('underdeployment_reason', '')}"
        )
    for name in ("tech", "primary"):
        scout = scouts.get(name) if isinstance(scouts.get(name), dict) else {}
        notes.append(
            f"{name} scout: age={scout.get('age_minutes', '')}m "
            f"watch={scout.get('pressure_watch', '')} trade={scout.get('pressure_trade', '')} "
            f"attempts={scout.get('scout_attempts', '')} "
            f"top={scout.get('top_candidate', '') or 'none'} "
            f"reject={str(scout.get('top_reject_reason', ''))[:220]}"
        )
        audit = scout_audits.get(name) if isinstance(scout_audits.get(name), dict) else {}
        if audit:
            notes.append(_scout_audit_line(name, audit))
    if not watch_items:
        watch_items.append("No process/account/report errors in this poll")

    decision_board = advisor_decision_board(
        accounts=accounts,
        processes=processes,
        move_brief=move_brief,
        trainer_brief=trainer_brief,
        model_tests=model_tests,
        watch_items=watch_items,
        account_advisor_only=account_advisor_only,
    )

    summary = {
        "time_utc": payload.get("time_utc", ""),
        "time_ny": payload.get("time_ny", ""),
        "status": payload.get("status", ""),
        "accounts": {
            name: _account_watch_line(name, accounts.get(name) if isinstance(accounts.get(name), dict) else {})
            for name in ("gpt", "tech", "primary")
        },
        "account_actions": {
            name: [
                _account_action_line(row)
                for row in (
                    (accounts.get(name) if isinstance(accounts.get(name), dict) else {}).get("recent_actions")
                    or []
                )
            ]
            for name in ("gpt", "tech", "primary")
        },
        "pending_orders": {
            name: [
                _pending_order_line(row)
                for row in (
                    (accounts.get(name) if isinstance(accounts.get(name), dict) else {}).get("pending_order_details")
                    or []
                )
            ]
            for name in ("gpt", "tech", "primary")
        },
        "notes": notes,
        "decision_board": decision_board,
        "scout_audits": scout_audits,
        "manager_states": manager_states,
        "system_resources": system_resources,
        "watch_items": watch_items,
        "daily_report": {
            "date_ny": daily_report.get("date_ny", ""),
            "age_minutes": daily_report.get("age_minutes", ""),
            "refreshed_this_poll": daily_report.get("refreshed_this_poll", False),
            "latest_report_dir": daily_report.get("latest_report_dir", ""),
        },
        "daily_move_brief": move_brief,
        "trainer": trainer_brief,
        "model_tests": model_tests,
        "manual_trade_action": "none; monitor is report-only",
    }
    return summary


def _counter_label(rows: Any, name_key: str = "name", count_key: str = "count") -> str:
    if not isinstance(rows, list) or not rows:
        return ""
    first = rows[0] if isinstance(rows[0], dict) else {}
    name = str(first.get(name_key) or first.get("gate") or first.get("instrument") or "")
    count = first.get(count_key, "")
    if count == "":
        return name
    return f"{name}:{count}"


def _top_value_label(rows: Any, value_key: str = "value_usd") -> str:
    if not isinstance(rows, list) or not rows:
        return ""
    first = rows[0] if isinstance(rows[0], dict) else {}
    instrument = str(first.get("instrument") or "")
    value = _float_or_none(first.get(value_key))
    count = first.get("count", "")
    if value is None:
        return instrument
    count_text = f"/{count}" if count != "" else ""
    return f"{instrument}:${value:.3f}{count_text}"


def _account_metric_fields(accounts: dict[str, Any], name: str) -> dict[str, Any]:
    acct = accounts.get(name) if isinstance(accounts.get(name), dict) else {}
    nav = _float_or_none(acct.get("nav"))
    margin = _float_or_none(acct.get("margin_used"))
    margin_pct = ""
    if nav and margin is not None:
        margin_pct = round(margin / nav * 100.0, 4)
    return {
        f"{name}_nav": acct.get("nav", ""),
        f"{name}_open_trades": acct.get("open_trades", ""),
        f"{name}_margin_pct": margin_pct,
    }


def _queue_source_row(live_miss_queue: dict[str, Any], label: str) -> dict[str, Any]:
    for row in live_miss_queue.get("sources", []) if isinstance(live_miss_queue.get("sources"), list) else []:
        if isinstance(row, dict) and row.get("label") == label:
            return row
    return {}


def _result_source_row(live_miss_results: dict[str, Any], label: str) -> dict[str, Any]:
    for row in live_miss_results.get("sources", []) if isinstance(live_miss_results.get("sources"), list) else []:
        if isinstance(row, dict) and row.get("label") == label:
            return row
    return {}


def _artifact_age(model_tests: dict[str, Any], name: str) -> Any:
    artifacts = model_tests.get("artifacts") if isinstance(model_tests.get("artifacts"), list) else []
    for artifact in artifacts:
        if isinstance(artifact, dict) and artifact.get("name") == name:
            return artifact.get("file_age_minutes", "")
    return ""


def _advisor_metrics_row(payload: dict[str, Any], note: dict[str, Any]) -> dict[str, Any]:
    accounts = payload.get("accounts") if isinstance(payload.get("accounts"), dict) else {}
    daily_report = note.get("daily_report") if isinstance(note.get("daily_report"), dict) else {}
    move_brief = note.get("daily_move_brief") if isinstance(note.get("daily_move_brief"), dict) else {}
    current_moves = move_brief.get("current") if isinstance(move_brief.get("current"), dict) else {}
    value_summary = (
        current_moves.get("miss_value_summary")
        if isinstance(current_moves.get("miss_value_summary"), dict)
        else {}
    )
    scout_audits = note.get("scout_audits") if isinstance(note.get("scout_audits"), dict) else {}
    tech_audit = scout_audits.get("tech") if isinstance(scout_audits.get("tech"), dict) else {}
    primary_audit = scout_audits.get("primary") if isinstance(scout_audits.get("primary"), dict) else {}
    tech_near = tech_audit.get("near_pass") if isinstance(tech_audit.get("near_pass"), dict) else {}
    primary_near = (
        primary_audit.get("near_pass") if isinstance(primary_audit.get("near_pass"), dict) else {}
    )
    trainer = note.get("trainer") if isinstance(note.get("trainer"), dict) else {}
    queue = trainer.get("queue") if isinstance(trainer.get("queue"), dict) else {}
    current = trainer.get("current") if isinstance(trainer.get("current"), dict) else {}
    live_miss_queue = (
        trainer.get("live_miss_queue")
        if isinstance(trainer.get("live_miss_queue"), dict)
        else {}
    )
    live_miss_results = (
        trainer.get("live_miss_results")
        if isinstance(trainer.get("live_miss_results"), dict)
        else {}
    )
    model_tests = note.get("model_tests") if isinstance(note.get("model_tests"), dict) else {}
    active_processes = (
        model_tests.get("active_processes")
        if isinstance(model_tests.get("active_processes"), list)
        else []
    )
    value_queue = _queue_source_row(live_miss_queue, "value")
    ev_queue = _queue_source_row(live_miss_queue, "ev_near")
    unknown_queue = _queue_source_row(live_miss_queue, "unknown")
    value_result = _result_source_row(live_miss_results, "value")
    ev_result = _result_source_row(live_miss_results, "ev_near")
    unknown_result = _result_source_row(live_miss_results, "unknown")

    row: dict[str, Any] = {
        "time_utc": payload.get("time_utc", ""),
        "time_ny": payload.get("time_ny", ""),
        "status": payload.get("status", ""),
        "daily_date_ny": daily_report.get("date_ny", ""),
        "daily_age_minutes": daily_report.get("age_minutes", ""),
        "daily_market_rows": current_moves.get("market_rows", ""),
        "daily_missed_rows": current_moves.get("missed_rows", ""),
        "daily_captured_rows": current_moves.get("captured_rows", ""),
        "daily_unique_missed_rows": current_moves.get("unique_missed_rows", ""),
        "daily_missed_value_usd": value_summary.get("total_value_usd", ""),
        "daily_missed_value_rows": value_summary.get("value_rows", ""),
        "daily_near_value_usd": value_summary.get("near_value_usd", ""),
        "daily_near_value_count": value_summary.get("near_value_count", ""),
        "daily_top_missed_pair": _counter_label(current_moves.get("top_missed_pairs")),
        "daily_top_miss_reason": _counter_label(current_moves.get("top_miss_reasons")),
        "daily_top_miss_gate": _counter_label(value_summary.get("top_gate_classes"), "gate", "count"),
        "daily_top_value_instrument": _top_value_label(value_summary.get("top_value_instruments"), "value_usd"),
        "daily_top_near_value_instrument": _top_value_label(
            value_summary.get("top_near_value_instruments"),
            "expected_usd",
        ),
        "tech_scout_recent_rows": tech_audit.get("rows", ""),
        "tech_scout_recent_accepted": tech_audit.get("accepted_count", ""),
        "tech_scout_recent_passed": tech_audit.get("passed_count", ""),
        "tech_scout_recent_max_score": tech_audit.get("max_pressure_score", ""),
        "tech_scout_recent_max_cluster": (
            f"{tech_audit.get('max_cluster_instruments', '')}/"
            f"{tech_audit.get('max_direction_cluster_instruments', '')}"
        ),
        "tech_scout_near_value_usd": tech_near.get("value_gate_expected_usd", ""),
        "tech_scout_near_value_count": tech_near.get("value_gate_count", ""),
        "tech_scout_near_score_count": tech_near.get("score_near_count", ""),
        "tech_scout_near_ratio_count": tech_near.get("spread_ratio_near_count", ""),
        "primary_scout_recent_rows": primary_audit.get("rows", ""),
        "primary_scout_recent_accepted": primary_audit.get("accepted_count", ""),
        "primary_scout_recent_passed": primary_audit.get("passed_count", ""),
        "primary_scout_recent_max_score": primary_audit.get("max_pressure_score", ""),
        "primary_scout_recent_max_cluster": (
            f"{primary_audit.get('max_cluster_instruments', '')}/"
            f"{primary_audit.get('max_direction_cluster_instruments', '')}"
        ),
        "primary_scout_near_value_usd": primary_near.get("value_gate_expected_usd", ""),
        "primary_scout_near_value_count": primary_near.get("value_gate_count", ""),
        "primary_scout_near_score_count": primary_near.get("score_near_count", ""),
        "primary_scout_near_ratio_count": primary_near.get("spread_ratio_near_count", ""),
        "trainer_heartbeat_age_minutes": trainer.get("heartbeat_age_minutes", ""),
        "trainer_queue_rows": queue.get("queue_rows", ""),
        "trainer_pending_specs": queue.get("pending_unique_specs", ""),
        "trainer_completed_specs": queue.get("completed_unique_specs", ""),
        "trainer_current_source": current.get("source", ""),
        "trainer_current_model": current.get("model_type", ""),
        "trainer_current_target": current.get("target", ""),
        "trainer_current_outcome": current.get("outcome", ""),
        "trainer_current_status": current.get("status", ""),
        "live_miss_value_pending": value_queue.get("pending", ""),
        "live_miss_value_completed": value_queue.get("completed", ""),
        "live_miss_ev_near_pending": ev_queue.get("pending", ""),
        "live_miss_ev_near_completed": ev_queue.get("completed", ""),
        "live_miss_unknown_pending": unknown_queue.get("pending", ""),
        "live_miss_unknown_completed": unknown_queue.get("completed", ""),
        "live_miss_value_gates": value_result.get("gate_count", ""),
        "live_miss_ev_near_gates": ev_result.get("gate_count", ""),
        "live_miss_unknown_gates": unknown_result.get("gate_count", ""),
        "active_research_processes": len(active_processes),
        "ensemble_age_minutes": _artifact_age(model_tests, "ensemble_summary"),
        "pre_spike_age_minutes": _artifact_age(model_tests, "pre_spike_lead"),
        "missed_spike_replay_age_minutes": _artifact_age(model_tests, "missed_spike_replay"),
    }
    for name in ("gpt", "tech", "primary"):
        row.update(_account_metric_fields(accounts, name))
    return row


def append_advisor_metrics(payload: dict[str, Any], note: dict[str, Any]) -> None:
    """Append compact 24h advisor metrics for trend review.

    This is deliberately separate from the older health CSV so adding fields
    does not rotate or invalidate the existing monitor history.
    """
    advisor.append_csv(ADVISOR_METRICS_CSV, _advisor_metrics_row(payload, note), ADVISOR_METRICS_FIELDS)


def write_advisor_note(payload: dict[str, Any]) -> dict[str, Any]:
    note = build_advisor_note(payload)
    advisor.write_json(ADVISOR_JSON, note)
    lines = [
        "# Live forex advisor note",
        "",
        f"- Time NY: {note.get('time_ny', '')}",
        f"- Status: {note.get('status', '')}",
        f"- Manual trade action: {note.get('manual_trade_action', '')}",
        "",
        "## Accounts",
        "",
    ]
    for line in (note.get("accounts") or {}).values():
        lines.append(f"- {line}")
    account_actions = note.get("account_actions") if isinstance(note.get("account_actions"), dict) else {}
    if any(account_actions.get(name) for name in ("gpt", "tech", "primary")):
        lines.extend(["", "## Recent broker actions", ""])
        for name in ("gpt", "tech", "primary"):
            action_lines = account_actions.get(name) if isinstance(account_actions.get(name), list) else []
            if not action_lines:
                continue
            lines.append(f"- {name}:")
            for action_line in action_lines[-RECENT_ACCOUNT_ACTION_LIMIT:]:
                lines.append(f"  - {action_line}")
    pending_orders = note.get("pending_orders") if isinstance(note.get("pending_orders"), dict) else {}
    if any(pending_orders.get(name) for name in ("gpt", "tech", "primary")):
        lines.extend(["", "## Pending orders / protection", ""])
        for name in ("gpt", "tech", "primary"):
            order_lines = pending_orders.get(name) if isinstance(pending_orders.get(name), list) else []
            if not order_lines:
                continue
            lines.append(f"- {name}:")
            for order_line in order_lines[:PENDING_ORDER_DETAIL_LIMIT]:
                lines.append(f"  - {order_line}")
    system_resources = (
        note.get("system_resources")
        if isinstance(note.get("system_resources"), dict)
        else {}
    )
    memory_status = (
        system_resources.get("memory")
        if isinstance(system_resources.get("memory"), dict)
        else {}
    )
    if memory_status:
        lines.extend(["", "## System resources", ""])
        if memory_status.get("error"):
            lines.append(f"- Memory: error={memory_status.get('error')}")
        else:
            lines.append(
                f"- Memory: free={memory_status.get('available_mb', '')}MB/"
                f"{memory_status.get('total_mb', '')}MB "
                f"({memory_status.get('available_pct', '')}% free), "
                f"used={memory_status.get('used_pct', '')}%"
            )
    lines.extend(["", "## Notes", ""])
    for item in note.get("notes") or []:
        lines.append(f"- {item}")
    decision_board = note.get("decision_board") if isinstance(note.get("decision_board"), list) else []
    if decision_board:
        lines.extend(["", "## Advisor decision board", ""])
        for item in decision_board:
            lines.append(f"- {item}")
    manager_states = note.get("manager_states") if isinstance(note.get("manager_states"), dict) else {}
    if manager_states:
        lines.extend(["", "## Manager state heartbeats", ""])
        for name in ("gpt", "tech", "primary", "canary"):
            state = manager_states.get(name) if isinstance(manager_states.get(name), dict) else {}
            if not state:
                continue
            age = state.get("last_monitor_age_minutes")
            if age in ("", None):
                age = state.get("mtime_age_minutes", "")
            crisis = state.get("live_crisis_mode", "")
            crisis_text = f" crisis={crisis}" if crisis else ""
            lines.append(
                f"- {name}: exists={state.get('exists', False)} "
                f"last_monitor_age={age}m "
                f"mtime_age={state.get('mtime_age_minutes', '')}m{crisis_text}"
            )
    lines.extend(["", "## Watch items", ""])
    for item in note.get("watch_items") or []:
        lines.append(f"- {item}")
    report = note.get("daily_report") or {}
    move_brief = note.get("daily_move_brief") if isinstance(note.get("daily_move_brief"), dict) else {}
    trainer = note.get("trainer") if isinstance(note.get("trainer"), dict) else {}
    model_tests = note.get("model_tests") if isinstance(note.get("model_tests"), dict) else {}
    lines.extend(
        [
            "",
            "## Daily move report",
            "",
            f"- Date NY: {report.get('date_ny', '')}",
            f"- Age minutes: {report.get('age_minutes', '')}",
            f"- Stale threshold minutes: {DAILY_REPORT_STALE_AFTER_MINUTES}",
            f"- Refreshed this poll: {report.get('refreshed_this_poll', False)}",
            f"- Path: {report.get('latest_report_dir', '')}",
            "",
        ]
    )
    for label in ("current", "previous"):
        brief = move_brief.get(label) if isinstance(move_brief.get(label), dict) else {}
        if not brief or not brief.get("exists"):
            continue
        lines.extend(
            [
                f"### {label.title()} report",
                "",
                f"- Market rows: {brief.get('market_rows', 0)}",
                f"- Missed rows: {brief.get('missed_rows', 0)}",
                f"- Captured rows: {brief.get('captured_rows', 0)}",
                f"- Unique missed rows: {brief.get('unique_missed_rows', 0)}",
                f"- Top missed pairs: "
                + "; ".join(
                    f"{row.get('name')}:{row.get('count')}"
                    for row in brief.get("top_missed_pairs", [])[:5]
                ),
                f"- Top miss reasons: "
                + "; ".join(
                    f"{row.get('name')}:{row.get('count')}"
                    for row in brief.get("top_miss_reasons", [])[:5]
                ),
            ]
        )
        value_summary = (
            brief.get("miss_value_summary")
            if isinstance(brief.get("miss_value_summary"), dict)
            else {}
        )
        gate_rows = (
            value_summary.get("top_gate_classes")
            if isinstance(value_summary.get("top_gate_classes"), list)
            else []
        )
        if gate_rows:
            lines.append(
                "- Parsed gate classes: "
                + "; ".join(f"{row.get('gate')}:{row.get('count')}" for row in gate_rows[:5])
            )
        top_value_instruments = (
            value_summary.get("top_value_instruments")
            if isinstance(value_summary.get("top_value_instruments"), list)
            else []
        )
        if top_value_instruments:
            lines.append(
                f"- Deduped missed estimated value: ${float(value_summary.get('total_value_usd') or 0.0):.3f}/"
                f"{value_summary.get('value_rows', 0)} rows; top="
                + "; ".join(
                    f"{row.get('instrument')}:${float(row.get('value_usd') or 0.0):.3f}/{row.get('count')}"
                    for row in top_value_instruments[:5]
                )
            )
        top_value_events = (
            value_summary.get("top_value_events")
            if isinstance(value_summary.get("top_value_events"), list)
            else []
        )
        if top_value_events:
            lines.append(
                "- Top value-missed events: "
                + "; ".join(
                    f"{row.get('instrument')} {row.get('direction')} "
                    f"${float(row.get('value_usd') or 0.0):.3f} {row.get('gate')}"
                    for row in top_value_events[:5]
                )
            )
        top_regime_lock_labels = (
            value_summary.get("top_regime_lock_labels")
            if isinstance(value_summary.get("top_regime_lock_labels"), list)
            else []
        )
        top_regime_lock_instruments = (
            value_summary.get("top_regime_lock_instruments")
            if isinstance(value_summary.get("top_regime_lock_instruments"), list)
            else []
        )
        if int(value_summary.get("regime_lock_count") or 0):
            label_text = "; ".join(
                f"{row.get('label')}:{row.get('count')}/${float(row.get('value_usd') or 0.0):.3f}"
                f"/{float(row.get('abs_net_pips') or 0.0):.1f}p"
                for row in top_regime_lock_labels[:3]
            )
            instrument_text = "; ".join(
                f"{row.get('instrument')}:{row.get('count')}/${float(row.get('value_usd') or 0.0):.3f}"
                f"/{float(row.get('abs_net_pips') or 0.0):.1f}p"
                for row in top_regime_lock_instruments[:3]
            )
            detail_parts = []
            if label_text:
                detail_parts.append(f"labels={label_text}")
            if instrument_text:
                detail_parts.append(f"instruments={instrument_text}")
            if int(value_summary.get("regime_lock_net_pip_rows") or 0):
                detail_parts.append(
                    f"auditNet={float(value_summary.get('regime_lock_abs_net_pips') or 0.0):.1f}p/"
                    f"{value_summary.get('regime_lock_net_pip_rows', 0)} rows"
                )
            if int(value_summary.get("regime_lock_audit_value_rows") or 0):
                detail_parts.append(f"auditValueRows={value_summary.get('regime_lock_audit_value_rows', 0)}")
            lines.append(
                f"- Scout regime-lock blocked misses: {value_summary.get('regime_lock_count', 0)} rows; "
                f"value=${float(value_summary.get('regime_lock_value_usd') or 0.0):.3f}/"
                f"{value_summary.get('regime_lock_value_rows', 0)} rows"
                + (("; " + "; ".join(detail_parts)) if detail_parts else "")
            )
        top_near_value = (
            value_summary.get("top_near_value_instruments")
            if isinstance(value_summary.get("top_near_value_instruments"), list)
            else []
        )
        if top_near_value:
            lines.append(
                f"- Deduped near value-gate misses: ${float(value_summary.get('near_value_usd') or 0.0):.3f}/"
                f"{value_summary.get('near_value_count', 0)} rows; top="
                + "; ".join(
                    f"{row.get('instrument')}:${float(row.get('expected_usd') or 0.0):.3f}"
                    f"({float(row.get('max_ratio') or 0.0) * 100:.0f}%)"
                    for row in top_near_value[:5]
                )
            )
        top_pips = brief.get("top_missed_net_pips") if isinstance(brief.get("top_missed_net_pips"), list) else []
        if top_pips:
            lines.append("- Top parsed missed net pips: " + "; ".join(
                f"{row.get('instrument')} {row.get('direction')} {row.get('parsed_net_pips')}p {row.get('miss_reason')}"
                for row in top_pips[:5]
            ))
        if brief.get("error"):
            lines.append(f"- Brief error: {brief.get('error')}")
        lines.append("")
    if trainer:
        current = trainer.get("current") if isinstance(trainer.get("current"), dict) else {}
        queue = trainer.get("queue") if isinstance(trainer.get("queue"), dict) else {}
        promotion = trainer.get("promotion") if isinstance(trainer.get("promotion"), dict) else {}
        shadow = trainer.get("live_shadow") if isinstance(trainer.get("live_shadow"), dict) else {}
        refresh = trainer.get("refresh") if isinstance(trainer.get("refresh"), dict) else {}
        ensemble_refresh = (
            trainer.get("ensemble_refresh") if isinstance(trainer.get("ensemble_refresh"), dict) else {}
        )
        latest_ensemble = (
            trainer.get("latest_ensemble_artifact")
            if isinstance(trainer.get("latest_ensemble_artifact"), dict)
            else {}
        )
        latest_seed = trainer.get("latest_seed") if isinstance(trainer.get("latest_seed"), dict) else {}
        live_miss_queue = (
            trainer.get("live_miss_queue")
            if isinstance(trainer.get("live_miss_queue"), dict)
            else {}
        )
        live_miss_results = (
            trainer.get("live_miss_results")
            if isinstance(trainer.get("live_miss_results"), dict)
            else {}
        )
        seeded_by_lane = (
            latest_seed.get("seeded_by_lane")
            if isinstance(latest_seed.get("seeded_by_lane"), dict)
            else {}
        )
        seed_bits = [
            f"{lane}:{count}"
            for lane, count in seeded_by_lane.items()
            if int(count or 0) > 0
        ][:5]
        regime_seed = (
            latest_seed.get("recent_regime_lock_seed")
            if isinstance(latest_seed.get("recent_regime_lock_seed"), dict)
            else {}
        )
        regime_seed_bits: list[str] = []
        top_regime_seed_instruments = (
            regime_seed.get("top_instruments")
            if isinstance(regime_seed.get("top_instruments"), list)
            else []
        )
        for row in top_regime_seed_instruments[:4]:
            if isinstance(row, dict) and row.get("instrument"):
                regime_seed_bits.append(f"{row.get('instrument')}:{row.get('count')}")
        regime_seed_text = ""
        if int(regime_seed.get("count") or 0) > 0:
            regime_seed_text = (
                f" regimeLockSpecs={regime_seed.get('count')}"
                + (f"[{','.join(regime_seed_bits)}]" if regime_seed_bits else "")
            )
        lines.extend(
            [
                "## Research trainer",
                "",
                f"- Report generated UTC: {trainer.get('generated_utc', '')}",
                f"- Report age minutes: {trainer.get('age_minutes', '')}",
                f"- Report refresh: refreshed={refresh.get('refreshed', False)} "
                f"skipped={refresh.get('skipped_reason', '')} "
                f"age_before={refresh.get('age_minutes_before', '')}",
                f"- Loop mode: {trainer.get('research_loop_mode', '')}",
                f"- Heartbeat UTC: {trainer.get('heartbeat_utc', '')}",
                f"- Heartbeat age minutes: {trainer.get('heartbeat_age_minutes', '')}",
                f"- Current: {current.get('source', '')} / {current.get('model_type', '')} / "
                f"{current.get('target', '')} / {current.get('outcome', '')} / {current.get('status', '')}",
                f"- Current profile: {current.get('profile', '')}; "
                f"validation={current.get('validation_profile', '')}; "
                f"whitelist={','.join(str(item) for item in (current.get('whitelist') or [])[:8])}",
                f"- Current resource cap: stage={current.get('effective_stage', '') or current.get('stage', '')} "
                f"weeks={current.get('effective_holdout_weeks', '')} "
                f"maxTrainRows={current.get('effective_max_train_rows', '')} "
                f"requestedRows={current.get('requested_max_train_rows', '')} "
                f"capped={current.get('max_train_rows_capped', '')}",
                f"- Queue: rows={queue.get('queue_rows', '')} pending={queue.get('pending_unique_specs', '')} "
                f"completed={queue.get('completed_unique_specs', '')}",
                f"- Promotion readiness: validation={promotion.get('validation_grade_count', '')} "
                f"robust_followup={promotion.get('robust_followup_count', '')} "
                f"specialist_validation={promotion.get('specialist_validation_grade_count', '')}",
                f"- Live-shadow latest: {shadow.get('top_candidate', '') or 'none'}; "
                f"reject={str(shadow.get('top_reject_reason', ''))[:180]}",
                f"- Ensemble refresh: status={ensemble_refresh.get('status', '')} "
                f"age={ensemble_refresh.get('age_minutes', '')}m "
                f"timeout={ensemble_refresh.get('timeout_seconds', '')}s "
                f"reason={ensemble_refresh.get('reason', '')}",
                f"- Latest ensemble artifact: exists={latest_ensemble.get('exists', False)} "
                f"file_age={latest_ensemble.get('file_age_minutes', '')}m "
                f"best={latest_ensemble.get('best_summary', '') or 'n/a'}",
                f"- Latest research seed: exists={latest_seed.get('exists', False)} "
                f"age={latest_seed.get('age_minutes', '')}m "
                f"total={latest_seed.get('seeded_total', '')} "
                f"lanes={';'.join(seed_bits) if seed_bits else 'none'} "
                f"execution={latest_seed.get('execution', '')}"
                f"{regime_seed_text}",
                f"- {_live_miss_queue_line(live_miss_queue)}",
                "",
            ]
        )
        live_miss_result_lines = _live_miss_result_lines(live_miss_results)
        for result_line in live_miss_result_lines:
            lines.append(f"- {result_line}")
        if live_miss_result_lines:
            lines.append("")
        successful_spike_candidates = (
            trainer.get("successful_missed_spike_candidates")
            if isinstance(trainer.get("successful_missed_spike_candidates"), list)
            else []
        )
        if successful_spike_candidates:
            lines.append("### Successful missed-spike candidates")
            lines.append("")
            for row in successful_spike_candidates[:3]:
                if not isinstance(row, dict):
                    continue
                profile = str(row.get("profile") or "")
                profile_tail = profile[-95:] if profile else ""
                lines.append(
                    f"- {row.get('instrument_whitelist', '') or row.get('segment', '')} "
                    f"{row.get('model_type', '')}/{row.get('target', '')}: "
                    f"score={_fmt_num(row.get('leader_score'), 2)} "
                    f"auc={_fmt_num(row.get('mean_auc'), 3)} "
                    f"net={_fmt_num(row.get('mean_net'), 3)}p "
                    f"total={_fmt_num(row.get('total_net'), 2)}p "
                    f"pf={_fmt_num(row.get('profit_factor'), 2)} "
                    f"trades={_fmt_num(row.get('trades'), 0)} "
                    f"segment={row.get('scorecard', '')}:{row.get('segment', '')} "
                    f"profile={profile_tail}"
                )
            lines.append("")
        lane_rows = trainer.get("account_model_lanes") if isinstance(trainer.get("account_model_lanes"), list) else []
        if lane_rows:
            lines.append("### Account model lanes")
            lines.append("")
            for lane in lane_rows[:4]:
                if not isinstance(lane, dict):
                    continue
                lines.append(
                    f"- {lane.get('account_lane', '')}: completed={lane.get('completed_specs', '')} "
                    f"pending={lane.get('pending_specs', '')} coverage={lane.get('coverage_completed_pct', '')}% "
                    f"top={lane.get('top_model', '')}/{lane.get('top_target', '')}/{lane.get('top_subset', '')} "
                    f"score={lane.get('top_score', '')}"
                )
            lines.append("")
        top_stopcap = trainer.get("top_stopcap") if isinstance(trainer.get("top_stopcap"), list) else []
        if top_stopcap:
            lines.append("### Stop-cap leaders")
            lines.append("")
            for row in top_stopcap[:3]:
                if not isinstance(row, dict):
                    continue
                lines.append(
                    f"- {row.get('model_type', '')}/{row.get('target', '')}/{row.get('subset', '')}/"
                    f"{row.get('policy', '')}: avgNet={row.get('avg_mean_net_pips', '')}p "
                    f"auc={row.get('avg_mean_auc', '')} gates={row.get('gate_passed_runs', '')}/{row.get('runs', '')}"
                )
            lines.append("")
    if model_tests:
        active = model_tests.get("active_processes") if isinstance(model_tests.get("active_processes"), list) else []
        artifacts = model_tests.get("artifacts") if isinstance(model_tests.get("artifacts"), list) else []
        lines.extend(["## Model testing", ""])
        if active:
            lines.append(f"- Active standalone research processes: {len(active)}")
            for proc in active[:5]:
                if not isinstance(proc, dict):
                    continue
                lines.append(
                    f"  - pid={proc.get('pid', '')} age={proc.get('age_minutes', '')}m "
                    f"procs={proc.get('process_count', '')} "
                    f"rss={proc.get('working_set_mb', '')}MB "
                    f"cpu={proc.get('cpu_seconds', '')}s "
                    f"script={proc.get('script', '')} "
                    f"cmd={str(proc.get('command', ''))[:220]}"
                )
        else:
            lines.append("- Active standalone research processes: 0")
        if artifacts:
            lines.append("- Latest artifacts:")
            for artifact in artifacts:
                if not isinstance(artifact, dict):
                    continue
                lines.append(
                    f"  - {artifact.get('name', '')}: exists={artifact.get('exists', False)} "
                    f"file_age={artifact.get('file_age_minutes', '')}m "
                    f"generated_age={artifact.get('generated_age_minutes', '')}m "
                    f"best={artifact.get('best_summary', '') or 'n/a'}"
                )
        lines.append("")
    ADVISOR_MD.write_text("\n".join(lines), encoding="utf-8")
    return note


def monitor_once() -> dict[str, Any]:
    now_utc = iso_utc()
    now_ny = advisor.iso_ny()
    processes = process_status()
    accounts = account_statuses()
    scouts = scout_statuses()
    manager_states = manager_state_statuses()
    daily_recaps = daily_recap_statuses()
    research = gpt_research_status()
    daily_report = daily_report_status()
    error_logs = {name: error_log_snapshot(path) for name, path in ERROR_LOGS.items()}
    errors = {name: str(meta.get("tail", "")) for name, meta in error_logs.items()}
    account_advisor_only = account_advisor_only_mode()

    problem_bits = []
    process_problem_names = ("day_monitor",) if account_advisor_only else ("gpt", "tech", "primary", "day_monitor")
    for name in process_problem_names:
        if not processes.get(name, {}).get("running"):
            problem_bits.append(f"{name}_process_down")
    for name in ("gpt", "tech", "primary"):
        if accounts.get(name, {}).get("error"):
            problem_bits.append(f"{name}_account_error")
    for name, state in manager_states.items():
        if not isinstance(state, dict):
            continue
        age = state.get("last_monitor_age_minutes")
        if not isinstance(age, (int, float)):
            age = state.get("mtime_age_minutes")
        if (
            isinstance(age, (int, float))
            and age > MANAGER_STATE_STALE_AFTER_MINUTES
            and (processes.get(name) or {}).get("running")
        ):
            problem_bits.append(f"{name}_state_stale")
    if research.get("error"):
        problem_bits.append("gpt_research_error")
    if daily_report.get("error"):
        problem_bits.append("daily_report_error")
    status = "ok" if not problem_bits else ",".join(problem_bits)

    row = {
        "time_utc": now_utc,
        "time_ny": now_ny,
        "gpt_pid": processes.get("gpt", {}).get("pid", ""),
        "tech_pid": processes.get("tech", {}).get("pid", ""),
        "primary_pid": processes.get("primary", {}).get("pid", ""),
        "gpt_nav": accounts.get("gpt", {}).get("nav", ""),
        "gpt_balance": accounts.get("gpt", {}).get("balance", ""),
        "gpt_unrealized": accounts.get("gpt", {}).get("unrealized", ""),
        "gpt_margin_used": accounts.get("gpt", {}).get("margin_used", ""),
        "gpt_open_trades": accounts.get("gpt", {}).get("open_trades", ""),
        "tech_nav": accounts.get("tech", {}).get("nav", ""),
        "tech_balance": accounts.get("tech", {}).get("balance", ""),
        "tech_unrealized": accounts.get("tech", {}).get("unrealized", ""),
        "tech_margin_used": accounts.get("tech", {}).get("margin_used", ""),
        "tech_open_trades": accounts.get("tech", {}).get("open_trades", ""),
        "primary_nav": accounts.get("primary", {}).get("nav", ""),
        "primary_balance": accounts.get("primary", {}).get("balance", ""),
        "primary_unrealized": accounts.get("primary", {}).get("unrealized", ""),
        "primary_margin_used": accounts.get("primary", {}).get("margin_used", ""),
        "primary_open_trades": accounts.get("primary", {}).get("open_trades", ""),
        "gpt_research_verdict": research.get("local_verdict", ""),
        "gpt_research_issue_count": research.get("issue_count", ""),
        "primary_scan_time_ny": scouts.get("primary", {}).get("time_ny", ""),
        "primary_top_candidate": str(scouts.get("primary", {}).get("top_candidate", ""))[:240],
        "primary_top_reject_reason": str(scouts.get("primary", {}).get("top_reject_reason", ""))[:320],
        "tech_scan_time_ny": scouts.get("tech", {}).get("time_ny", ""),
        "tech_top_candidate": str(scouts.get("tech", {}).get("top_candidate", ""))[:240],
        "tech_top_reject_reason": str(scouts.get("tech", {}).get("top_reject_reason", ""))[:320],
        "status": status,
        "error": "; ".join(problem_bits),
    }

    payload = {
        "time_utc": now_utc,
        "time_ny": now_ny,
        "status": status,
        "processes": processes,
        "accounts": accounts,
        "scouts": scouts,
        "manager_states": manager_states,
        "daily_recaps": daily_recaps,
        "daily_report": daily_report,
        "gpt_research": research,
        "error_logs": error_logs,
        "errors_tail": errors,
        "system_resources": {"memory": system_memory_status()},
        "csv_row": row,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    advisor.append_csv(OUT_CSV, row, CSV_FIELDS)
    advisor.write_json(LATEST_JSON, payload)
    advisor_note = write_advisor_note(payload)
    append_advisor_metrics(payload, advisor_note)
    payload["advisor_note"] = advisor_note
    advisor.write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loop", action="store_true", help="Keep polling.")
    parser.add_argument("--interval-seconds", type=int, default=900)
    parser.add_argument("--count", type=int, default=1, help="Number of polls in loop mode; 0 means forever.")
    parser.add_argument(
        "--account-advisor-only",
        action="store_true",
        help="Suppress expected trainer/depth/canary offline warnings while monitoring accounts, actions, and missed moves.",
    )
    args = parser.parse_args()
    if args.account_advisor_only:
        os.environ["OANDA_ACCOUNT_ADVISOR_ONLY"] = "1"

    polls = 0
    while True:
        payload = monitor_once()
        print(
            json.dumps(
                {
                    "time_ny": payload.get("time_ny"),
                    "status": payload.get("status"),
                    "gpt_open": (payload.get("accounts", {}).get("gpt") or {}).get("open_trades"),
                    "tech_open": (payload.get("accounts", {}).get("tech") or {}).get("open_trades"),
                    "primary_open": (payload.get("accounts", {}).get("primary") or {}).get("open_trades"),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        polls += 1
        if not args.loop:
            break
        if args.count > 0 and polls >= args.count:
            break
        time.sleep(max(30, int(args.interval_seconds)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
