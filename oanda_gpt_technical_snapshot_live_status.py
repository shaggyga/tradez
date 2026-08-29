#!/usr/bin/env python3
"""Local status report for the GPT technical-snapshot live lane.

This reads only local files. It does not call OANDA, OpenAI, or place orders.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import oanda_gpt_technical_snapshot_account_manager as tech_manager

TECH_DIR = ROOT / "data" / "forex_gpt_manager" / "account_gpt_technical_snapshot_live"
SNAPSHOT_LOCK = TECH_DIR / "account_process.lock"
PROD_DIR = ROOT / "data" / "forex_gpt_manager" / "account_gpt_prod_live"
PROD_LOCK = PROD_DIR / "account_process.lock"
TECH_PROD_DIR = ROOT / "data" / "technical_scout_manager" / "account_live_tech_broad_regime_scout"
TECH_PROD_LOCK = TECH_PROD_DIR / "account_process.lock"


def iso_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def latest_csv_row(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            last: dict[str, Any] = {}
            for row in csv.DictReader(handle):
                last = dict(row)
            return last
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def compact_csv_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    raw = out.pop("raw_json", "")
    if raw:
        out["raw_json_bytes"] = len(str(raw))
    return out


def last_jsonl_row(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        last = ""
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    last = line
        return json.loads(last) if last else {}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def lock_busy(path: Path) -> bool:
    if not path.exists():
        return False
    handle = None
    try:
        handle = path.open("r+b")
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        return False
    except OSError:
        return True
    finally:
        if handle:
            try:
                handle.close()
            except Exception:
                pass


def active_trade_memory(state: dict[str, Any]) -> list[dict[str, Any]]:
    memory = state.get("trade_profit_memory")
    if not isinstance(memory, dict):
        return []
    rows: list[dict[str, Any]] = []
    for trade_id, item in memory.items():
        if not isinstance(item, dict):
            continue
        if item.get("closed_or_missing_first_seen_utc"):
            continue
        units = item.get("current_units")
        try:
            if abs(float(units)) <= 0:
                continue
        except Exception:
            continue
        rows.append(
            {
                "trade_id": str(trade_id),
                "instrument": item.get("instrument"),
                "direction": item.get("direction"),
                "current_units": item.get("current_units"),
                "entry_price": item.get("entry_price"),
                "last_unrealized_pl": item.get("last_unrealized_pl"),
                "max_unrealized_pl": item.get("max_unrealized_pl"),
                "min_unrealized_pl": item.get("min_unrealized_pl"),
                "last_seen_utc": item.get("last_seen_utc"),
            }
        )
    return rows


def account_suffix(value: Any) -> str:
    text = str(value or "").strip()
    return text[-4:] if text else ""


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in {None, ""}:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in {None, ""}:
            return default
        return int(float(value))
    except Exception:
        return default


def handoff_manage_only_status(
    latest_tech_prod_monitor: dict[str, Any],
    *,
    resolved_account: str,
    tech_prod_account: str,
    tech_prod_active_memory: list[dict[str, Any]],
) -> dict[str, Any]:
    max_open_trades = max(
        0,
        tech_manager._setting_int("FOREX_TECH_LIVE_HANDOFF_FRESH_OPEN_MAX_OPEN_TRADES", 3),
    )
    max_margin_pct = max(
        0.0,
        tech_manager._setting_float("FOREX_TECH_LIVE_HANDOFF_FRESH_OPEN_MAX_MARGIN_USED_PCT", 45.0),
    )
    enabled = tech_manager._setting_bool("FOREX_TECH_LIVE_HANDOFF_MANAGE_ONLY_ENABLED", True)
    same_tech_account = bool(resolved_account and tech_prod_account and resolved_account == tech_prod_account)
    open_trade_count = max(
        safe_int(latest_tech_prod_monitor.get("open_trade_count"), 0),
        len(tech_prod_active_memory or []),
    )
    margin_used_pct = safe_float(latest_tech_prod_monitor.get("margin_used_pct"), 0.0)
    reasons: list[str] = []
    if enabled and same_tech_account:
        if open_trade_count > max_open_trades:
            reasons.append(f"open_trades {open_trade_count} > handoff fresh-open max {max_open_trades}")
        if margin_used_pct >= max_margin_pct:
            reasons.append(
                f"margin_used_pct {margin_used_pct:.2f} >= handoff fresh-open max {max_margin_pct:.2f}"
            )
    return {
        "handoff_manage_only_if_started": bool(reasons),
        "handoff_manage_only_reasons": reasons,
        "handoff_manage_only_enabled": enabled,
        "handoff_fresh_open_max_open_trades": max_open_trades,
        "handoff_fresh_open_max_margin_used_pct": max_margin_pct,
        "handoff_source_same_tech_account": same_tech_account,
        "handoff_source_open_trade_count": open_trade_count,
        "handoff_source_margin_used_pct": margin_used_pct,
    }


def build_status() -> dict[str, Any]:
    latest_snapshot = read_json(TECH_DIR / "latest_technical_snapshot.json", {})
    latest_snapshot_ledger = last_jsonl_row(TECH_DIR / "technical_snapshot_ledger.jsonl")
    prod_state = read_json(PROD_DIR / "state.json", {})
    tech_prod_state = read_json(TECH_PROD_DIR / "state.json", {})
    tech_prod_active_memory = active_trade_memory(tech_prod_state)
    prod_lock_busy = lock_busy(PROD_LOCK)
    tech_prod_lock_busy = lock_busy(TECH_PROD_LOCK)
    latest_prod_action = compact_csv_row(latest_csv_row(PROD_DIR / "actions.csv"))
    latest_prod_monitor = compact_csv_row(latest_csv_row(PROD_DIR / "monitor.csv"))
    latest_tech_prod_monitor = compact_csv_row(latest_csv_row(TECH_PROD_DIR / "monitor.csv"))
    technical_call_times = tech_manager._technical_call_times()
    resolved_account, resolved_alias = tech_manager._cred_or_env_with_name(
        "OANDA_ACCOUNT_LIVE_GPT_TECHNICAL",
        "OANDA_ACCOUNT_LIVE_GPT_TECH",
        "OANDA_ACCOUNT_ID_GPT_TECH_LIVE",
        "OANDA_ACCOUNT_ID_GPT_TECHNICAL_LIVE",
        "OANDA_ACCOUNT_LIVE_TECH",
        "OANDA_ACCOUNT_ID_TECH_LIVE",
        "OANDA_ACCOUNT_ID_LIVE_TECH",
        "OANDA_ACCOUNT_LIVE_MAIN",
    )
    prod_account = tech_manager._cred_or_env(
        "OANDA_ACCOUNT_ID_GPT_LIVE",
        "OANDA_ACCOUNT_LIVE_MAIN",
        "OANDA_LIVE_ACCOUNT_ID_GPT",
        "OANDA_ACCOUNT_ID_LIVE",
        "OANDA_LIVE_ACCOUNT_ID",
    )
    tech_prod_account = tech_manager._cred_or_env(
        "OANDA_ACCOUNT_LIVE_TECH",
        "OANDA_ACCOUNT_ID_TECH_LIVE",
        "OANDA_ACCOUNT_ID_LIVE_TECH",
    )
    blocked_by_prod = bool(prod_lock_busy and prod_account and prod_account == resolved_account)
    blocked_by_tech_prod = bool(
        tech_prod_lock_busy and tech_prod_account and tech_prod_account == resolved_account
    )
    handoff_status = handoff_manage_only_status(
        latest_tech_prod_monitor,
        resolved_account=resolved_account,
        tech_prod_account=tech_prod_account,
        tech_prod_active_memory=tech_prod_active_memory,
    )
    own_lock_busy = lock_busy(SNAPSHOT_LOCK)
    blockers: list[str] = []
    if own_lock_busy:
        blockers.append("snapshot lane already has an active process lock")
    if blocked_by_prod:
        blockers.append("gpt_prod_live owns the resolved live account")
    if blocked_by_tech_prod:
        blockers.append("tech_prod_live owns the resolved live account")
    activation_ready = bool(resolved_account) and not blockers
    if activation_ready:
        next_safe_action = "start_snapshot_lane"
    elif blocked_by_tech_prod:
        next_safe_action = "wait_or_stop_oanda_tech_prod_live_account_manager_before_starting_snapshot_lane"
    elif blocked_by_prod:
        next_safe_action = "wait_or_stop_oanda_gpt_prod_live_account_manager_before_starting_snapshot_lane"
    elif own_lock_busy:
        next_safe_action = "snapshot_lane_already_running"
    else:
        next_safe_action = "set_required_live_account_alias_before_starting_snapshot_lane"

    return {
        "time_utc": iso_utc(),
        "technical_snapshot_live": {
            "data_dir": str(TECH_DIR),
            "own_process_lock_exists": SNAPSHOT_LOCK.exists(),
            "own_process_lock_busy": own_lock_busy,
            "resolved_account_alias": resolved_alias,
            "resolved_account_suffix": account_suffix(resolved_account),
            "activation_ready": activation_ready,
            "activation_blockers": blockers,
            "next_safe_action": next_safe_action,
            "execution_blocked_by_any_live_lock": bool(blockers),
            "execution_blocked_by_prod_live_lock": blocked_by_prod,
            "execution_blocked_by_tech_prod_live_lock": blocked_by_tech_prod,
            **handoff_status,
            "configured_call_count": len(technical_call_times),
            "configured_call_times_ny": technical_call_times,
            "configured_scan_interval_minutes": tech_manager._setting_int(
                "FOREX_TECH_LIVE_SCAN_INTERVAL_MINUTES", 30
            ),
            "configured_min_minutes_between_gpt_scans": tech_manager._setting_int(
                "FOREX_TECH_LIVE_MIN_MINUTES_BETWEEN_GPT_SCANS", 30
            ),
            "configured_local_monitor_interval_minutes": tech_manager._setting_int(
                "FOREX_TECH_LIVE_LOCAL_MONITOR_INTERVAL_MINUTES", 5
            ),
            "latest_snapshot_exists": bool(latest_snapshot),
            "latest_snapshot_time_utc": latest_snapshot.get("time_utc"),
            "latest_snapshot_candidate_count": latest_snapshot.get("candidate_count"),
            "latest_snapshot_open_eligible_count": latest_snapshot.get("open_eligible_count"),
            "latest_snapshot_watch_only_count": latest_snapshot.get("watch_only_count"),
            "latest_snapshot_top_candidates": latest_snapshot.get("top_candidates", [])[:8],
            "latest_ledger_time_utc": latest_snapshot_ledger.get("time_utc"),
        },
        "shared_live_account": {
            "prod_live_data_dir": str(PROD_DIR),
            "prod_live_lock_exists": PROD_LOCK.exists(),
            "prod_live_lock_busy": prod_lock_busy,
            "prod_live_account_suffix": account_suffix(prod_account),
            "tech_live_execution_blocked_by_prod_lock": blocked_by_prod,
            "latest_prod_action": latest_prod_action,
            "latest_prod_monitor": latest_prod_monitor,
            "active_trade_memory": active_trade_memory(prod_state),
            "tech_prod_live_data_dir": str(TECH_PROD_DIR),
            "tech_prod_live_lock_exists": TECH_PROD_LOCK.exists(),
            "tech_prod_live_lock_busy": tech_prod_lock_busy,
            "tech_prod_live_account_suffix": account_suffix(tech_prod_account),
            "tech_live_execution_blocked_by_tech_prod_lock": blocked_by_tech_prod,
            "latest_tech_prod_monitor": latest_tech_prod_monitor,
            "tech_prod_active_trade_memory": tech_prod_active_memory,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Local GPT technical-snapshot live status report.")
    parser.add_argument("--no-write", action="store_true", help="Print status only; do not update latest_local_status.json.")
    args = parser.parse_args()
    status = build_status()
    if not args.no_write:
        TECH_DIR.mkdir(parents=True, exist_ok=True)
        (TECH_DIR / "latest_local_status.json").write_text(
            json.dumps(status, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(status, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
