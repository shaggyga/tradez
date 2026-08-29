#!/usr/bin/env python3
"""Monitor primary forecast rotation bot state without touching broker orders."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data" / "technical_scout_manager" / "account_live_primary_forecast_rotation"
LOG_DIR = PROJECT_ROOT / "data" / "runtime_logs"
CONFIG_PATH = PROJECT_ROOT / "config" / "primary_forecast_rotation_bot.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {"_read_error": f"{type(exc).__name__}: {exc}", "_path": str(path)}
    return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def optional_path(value: Any) -> Optional[Path]:
    text = str(value or "").strip()
    return Path(text) if text else None


def file_size(path: Optional[Path]) -> int:
    if path and path.exists() and path.is_file():
        return path.stat().st_size
    return 0


def process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, int(pid))
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def tail_actions(path: Path, count: int = 10) -> List[Dict[str, Any]]:
    try:
        if not path.exists():
            return []
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        return rows[-count:]
    except Exception as exc:
        return [{"_read_error": f"{type(exc).__name__}: {exc}", "_path": str(path)}]


def instrument_parts(instrument: str) -> tuple[str, str]:
    parts = str(instrument or "").split("_", 1)
    if len(parts) != 2:
        return str(instrument or ""), ""
    return parts[0], parts[1]


def pips_size(instrument: str) -> float:
    _, quote = instrument_parts(instrument)
    return 0.01 if quote == "JPY" else 0.0001


def pip_value_per_unit_usd(instrument: str, mid: float, mids: Dict[str, float]) -> float:
    base, quote = instrument_parts(instrument)
    pip = pips_size(instrument)
    if quote == "USD":
        return pip
    if base == "USD":
        return pip / max(mid, 1e-9)
    cross = mids.get(f"{quote}_USD")
    if cross:
        return pip * max(cross, 1e-9)
    inverse = mids.get(f"USD_{quote}")
    if inverse:
        return pip / max(inverse, 1e-9)
    return pip


def quote_to_usd_rate(instrument: str, mid: float, mids: Dict[str, float]) -> float:
    base, quote = instrument_parts(instrument)
    if quote == "USD":
        return 1.0
    if base == "USD":
        return 1.0 / max(mid, 1e-9)
    cross = mids.get(f"{quote}_USD")
    if cross:
        return max(cross, 1e-9)
    inverse = mids.get(f"USD_{quote}")
    if inverse:
        return 1.0 / max(inverse, 1e-9)
    return 1.0


def margin_rate_for_instrument(instrument: str, cfg: Dict[str, Any], state: Dict[str, Any]) -> float:
    if cfg.get("use_broker_margin_rates", False):
        stored = state.get("broker_margin_rates") if isinstance(state, dict) else {}
        if isinstance(stored, dict):
            rate = safe_float(stored.get(instrument), 0.0)
            if rate > 0:
                return rate
    return safe_float(cfg.get("default_margin_rate"), 0.0333)


def forecast_entry_diagnostics(decision: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    forecasts_path = DATA_DIR / "latest_forecasts.csv"
    if not forecasts_path.exists():
        return {"available": False, "reason": "latest_forecasts_missing"}
    try:
        with forecasts_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except Exception as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
    safety = decision.get("safety") or {}
    state = read_json(DATA_DIR / "state.json", {})
    nav = safe_float(safety.get("paper_equity"))
    margin_used = safe_float(safety.get("margin_used"))
    margin_rate = safe_float(cfg.get("default_margin_rate"), 0.0333)
    target_margin_total = nav * safe_float(cfg.get("target_margin_used_pct"), 55.0) / 100.0
    hard_margin_total = nav * safe_float(cfg.get("max_margin_used_pct"), 72.0) / 100.0
    remaining_target_margin = max(0.0, target_margin_total - margin_used)
    remaining_hard_margin = max(0.0, hard_margin_total - margin_used)
    day_start = safe_float(state.get("day_start_equity"), nav)
    daily_loss_kill_pct = safe_float(cfg.get("daily_loss_kill_pct"))
    max_day_loss_usd = day_start * daily_loss_kill_pct / 100.0 if day_start > 0 else 0.0
    current_day_loss_usd = max(0.0, day_start - nav)
    remaining_daily_loss_budget = max(0.0, max_day_loss_usd - current_day_loss_usd)
    risk_budget_fraction = safe_float(cfg.get("max_trade_risk_remaining_day_loss_fraction"))
    daily_loss_risk_cap_usd: Optional[float] = None
    if day_start > 0 and daily_loss_kill_pct > 0 and risk_budget_fraction > 0:
        daily_loss_risk_cap_usd = remaining_daily_loss_budget * risk_budget_fraction
    mids: Dict[str, float] = {}
    for row in rows:
        instrument = str(row.get("instrument") or "")
        if instrument and instrument not in mids:
            mids[instrument] = safe_float(row.get("mid"))
    conversion_prices = state.get("broker_conversion_prices") if isinstance(state, dict) else {}
    if isinstance(conversion_prices, dict):
        for instrument, mid in conversion_prices.items():
            if str(instrument):
                mids[str(instrument)] = safe_float(mid)

    reason_counts: Dict[str, int] = {}
    candidates: List[Dict[str, Any]] = []
    viable_count = 0
    tradable_count = 0
    for row in rows:
        if str(row.get("reject_reason") or "").strip():
            continue
        tradable_count += 1
        instrument = str(row.get("instrument") or "")
        mid = safe_float(row.get("mid"))
        edge_pips = safe_float(row.get("edge_pips"))
        stop_pips = safe_float(row.get("stop_pips"))
        take_profit_pips = safe_float(row.get("take_profit_pips"))
        rank_score = safe_float(row.get("rank_score"))
        pip_value = pip_value_per_unit_usd(instrument, mid, mids)
        margin_rate = margin_rate_for_instrument(instrument, cfg, state)
        margin_mid_usd = max(0.0, mid) * quote_to_usd_rate(instrument, mid, mids)
        risk_low = safe_float(cfg.get("risk_pct_min"), 0.25)
        risk_high = safe_float(cfg.get("risk_pct_max"), 1.75)
        risk_full = max(0.01, safe_float(cfg.get("risk_pct_score_full"), 2.5))
        risk_pct = round(risk_low + (risk_high - risk_low) * min(1.0, max(0.0, rank_score / risk_full)), 4)
        risk_usd = nav * risk_pct / 100.0
        if daily_loss_risk_cap_usd is not None:
            risk_usd = min(risk_usd, daily_loss_risk_cap_usd)
        base_units = int(risk_usd / max(stop_pips * pip_value, 1e-9))
        min_profit_units = 0
        min_expected = safe_float(cfg.get("min_expected_profit_usd"))
        min_take_profit = safe_float(cfg.get("min_take_profit_usd"))
        if edge_pips > 0 and pip_value > 0:
            min_profit_units = max(min_profit_units, int((min_expected / max(edge_pips * pip_value, 1e-9)) + 0.999999))
        if take_profit_pips > 0 and pip_value > 0:
            min_profit_units = max(
                min_profit_units,
                int((min_take_profit / max(take_profit_pips * pip_value, 1e-9)) + 0.999999),
            )
        max_trade_risk_usd = nav * safe_float(cfg.get("max_trade_risk_pct"), risk_high) / 100.0
        if daily_loss_risk_cap_usd is not None:
            max_trade_risk_usd = min(max_trade_risk_usd, daily_loss_risk_cap_usd)
        max_risk_units = int(max_trade_risk_usd / max(stop_pips * pip_value, 1e-9))
        sized_units = max(base_units, min_profit_units)
        if max_risk_units <= 0:
            sized_units = 0
        else:
            sized_units = min(sized_units, max_risk_units)
        target_margin_units = int(remaining_target_margin / max(margin_mid_usd * margin_rate, 1e-9))
        hard_margin_units = int(remaining_hard_margin / max(margin_mid_usd * margin_rate, 1e-9))
        planned_units = max(0, min(sized_units, target_margin_units))
        hard_units = max(0, min(sized_units, hard_margin_units))
        expected_profit = max(0.0, edge_pips) * pip_value * planned_units
        take_profit_usd = max(0.0, take_profit_pips) * pip_value * planned_units
        estimated_margin = planned_units * max(margin_mid_usd, 1e-9) * margin_rate
        profit_per_margin = expected_profit / estimated_margin if estimated_margin > 0 else 0.0
        reason = "viable"
        if hard_units <= 0:
            reason = "insufficient_hard_margin"
        elif planned_units <= 0:
            reason = "insufficient_target_margin"
        elif expected_profit < min_expected:
            reason = "expected_below_floor"
        elif take_profit_usd < min_take_profit:
            reason = "take_profit_below_floor"
        elif profit_per_margin < safe_float(cfg.get("min_profit_per_margin")):
            reason = "profit_per_margin_below_floor"
        else:
            viable_count += 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        allocation_score = expected_profit * (1.0 + max(0.0, rank_score) * 20.0) * (
            1.0 + min(5.0, max(0.0, profit_per_margin) * 100.0)
        )
        candidates.append(
            {
                "instrument": instrument,
                "direction": row.get("direction"),
                "horizon_minutes": row.get("horizon_minutes"),
                "rank_score": round(rank_score, 6),
                "edge_pips": round(edge_pips, 6),
                "planned_units": planned_units,
                "hard_units": hard_units,
                "expected_profit_usd": round(expected_profit, 6),
                "take_profit_usd": round(take_profit_usd, 6),
                "estimated_margin_required": round(estimated_margin, 6),
                "profit_per_margin": round(profit_per_margin, 6),
                "allocation_score": round(allocation_score, 6),
                "entry_reason": reason,
            }
        )
    candidates.sort(key=lambda item: (item["entry_reason"] == "viable", item["allocation_score"]), reverse=True)
    return {
        "available": True,
        "nav": round(nav, 6),
        "margin_used": round(margin_used, 6),
        "remaining_target_margin": round(remaining_target_margin, 6),
        "remaining_hard_margin": round(remaining_hard_margin, 6),
        "remaining_daily_loss_budget": round(remaining_daily_loss_budget, 6),
        "daily_loss_risk_cap_usd": round(daily_loss_risk_cap_usd, 6) if daily_loss_risk_cap_usd is not None else None,
        "target_active_positions": int(safe_float(cfg.get("target_active_positions"), 0.0)),
        "max_new_positions_per_cycle": int(safe_float(cfg.get("max_new_positions_per_cycle"), 0.0)),
        "tradable_forecast_count": tradable_count,
        "viable_candidate_count": viable_count,
        "entry_reason_counts": reason_counts,
        "top_entry_candidates": candidates[:10],
    }


def snapshot() -> Dict[str, Any]:
    launch = read_json(DATA_DIR / "latest_launch.json", {})
    if not launch:
        launch = read_json(DATA_DIR / "live_latest_launch.json", {})
    cfg = read_json(CONFIG_PATH, {})
    default_pid_path = DATA_DIR / "latest.pid"
    if not default_pid_path.exists():
        default_pid_path = DATA_DIR / "live_latest.pid"
    pid_path = Path(str(launch.get("pid_file") or default_pid_path))
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip()) if pid_path.exists() else int(launch.get("pid") or 0)
    except Exception:
        pid = int(launch.get("pid") or 0)
    decision_path = Path(str(launch.get("latest_decision") or DATA_DIR / "latest_decision.json"))
    decision = read_json(decision_path, {})
    actions = decision.get("actions") or []
    if isinstance(actions, dict):
        actions = [actions]
    stderr = optional_path(launch.get("stderr"))
    stdout = optional_path(launch.get("stdout"))
    safety = decision.get("safety") or {}
    managed = decision.get("live_managed_positions") or []
    if isinstance(managed, dict):
        managed = [managed]
    entry_diagnostics = forecast_entry_diagnostics(decision, cfg)
    flags = []
    if not process_running(pid):
        flags.append("process_not_running")
    stderr_length = file_size(stderr)
    if stderr_length > 0:
        flags.append("stderr_nonempty")
    kill_switches_disabled = bool(cfg.get("disable_kill_switches", False))
    live_new_entries_enabled = bool(cfg.get("live_new_entries_enabled", True))
    if not live_new_entries_enabled:
        flags.append("live_new_entries_disabled")
    if safety.get("kill_switch_active"):
        flags.append("kill_switch_active")
    daily_loss_kill = safe_float(cfg.get("daily_loss_kill_pct"), 5.0)
    if not kill_switches_disabled and safe_float(safety.get("day_loss_pct")) >= daily_loss_kill * 0.90:
        flags.append("day_loss_near_kill")
    if float(safety.get("margin_used_pct") or 0.0) < 50.0 and len(managed) == 0:
        if entry_diagnostics.get("available") and safe_float(entry_diagnostics.get("viable_candidate_count")) <= 0:
            flags.append("under_allocated_no_economic_candidates")
        else:
            flags.append("under_allocated_no_positions")
    return {
        "snapshot_utc": utc_now(),
        "pid": pid,
        "process_running": process_running(pid),
        "decision_generated_utc": decision.get("generated_utc"),
        "forecast_count": (decision.get("meta") or {}).get("forecast_count"),
        "tradable_forecast_count": (decision.get("meta") or {}).get("tradable_forecast_count"),
        "live_open_trade_count": decision.get("live_open_trade_count"),
        "live_managed_position_count": len(managed),
        "managed_positions": [
            {
                "instrument": item.get("instrument"),
                "direction": item.get("direction"),
                "units": item.get("units"),
                "unrealized_usd": item.get("unrealized_usd"),
                "last_score": item.get("last_score"),
            }
            for item in managed[:50]
        ],
        "action_count": len(actions),
        "actions": actions[:20],
        "safety": safety,
        "entry_diagnostics": entry_diagnostics,
        "flags": flags,
        "live_new_entries_enabled": live_new_entries_enabled,
        "stdout": str(stdout) if stdout else None,
        "stderr": str(stderr) if stderr else None,
        "stderr_length": stderr_length,
        "recent_journal_actions": tail_actions(DATA_DIR / "actions.csv", 10),
    }


def main() -> int:
    global DATA_DIR, CONFIG_PATH
    parser = argparse.ArgumentParser(description="Monitor primary forecast rotation bot.")
    parser.add_argument("--duration-seconds", type=int, default=4 * 60 * 60)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--config", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--latest-output", default="")
    args = parser.parse_args()
    if args.data_dir:
        DATA_DIR = Path(args.data_dir)
    if args.config:
        CONFIG_PATH = Path(args.config)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    monitor_name = DATA_DIR.name or "primary_forecast_rotation"
    output = Path(args.output) if args.output else LOG_DIR / f"{monitor_name}_monitor_{stamp}.jsonl"
    latest = Path(args.latest_output) if args.latest_output else LOG_DIR / f"{monitor_name}_monitor_latest.json"
    deadline = time.time() + max(1, args.duration_seconds)
    while time.time() < deadline:
        snap = snapshot()
        with output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(snap, sort_keys=True, default=str) + "\n")
        latest.write_text(json.dumps(snap, indent=2, sort_keys=True, default=str), encoding="utf-8")
        time.sleep(max(5, args.interval_seconds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
