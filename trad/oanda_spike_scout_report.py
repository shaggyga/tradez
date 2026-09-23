#!/usr/bin/env python3
"""Offline spike-scout monitoring report.

The spike-scout account is intentionally separate from GPT, technical
production, and canary.  This script summarizes its local runtime evidence so
the trainer has an auditable feedback artifact without changing scout trading
behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parent
SCOUT_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "technical_scout_manager"
    / "account_spike_scout_major_moves"
)
REPORT_PATH = (
    PROJECT_ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_spike_scout_report.json"
)
PROMOTIONS_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "promotions"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            return default
        return number
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except Exception:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def recent_csv_rows(path: Path, limit: int) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(deque((dict(row) for row in reader), maxlen=max(1, limit)))


def count_values(rows: Iterable[Dict[str, Any]], field: str, limit: int = 10) -> List[Dict[str, Any]]:
    counts = Counter(str(row.get(field) or "") for row in rows)
    counts.pop("", None)
    return [
        {"value": value, "count": count}
        for value, count in counts.most_common(limit)
    ]


def top_numeric(
    rows: Iterable[Dict[str, Any]],
    field: str,
    *,
    limit: int = 10,
    absolute: bool = False,
) -> List[Dict[str, Any]]:
    ranked = []
    for row in rows:
        value = safe_float(row.get(field), 0.0)
        sort_value = abs(value) if absolute else value
        ranked.append((sort_value, row, value))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [
        {
            "time_utc": row.get("time_utc", ""),
            "instrument": row.get("instrument", row.get("best_instrument", "")),
            "direction": row.get("direction", row.get("best_direction", "")),
            field: value,
            "status": row.get("status", row.get("event_status", "")),
            "reason": str(row.get("reason", ""))[:500],
        }
        for _, row, value in ranked[:limit]
    ]


def summarize_actions(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    by_status = count_values(rows, "status")
    by_instrument = count_values(rows, "instrument")
    by_action_type = count_values(rows, "action_type")
    fills = [
        row for row in rows
        if str(row.get("status") or "").lower() in {"opened", "filled", "success"}
        or str(row.get("trade_id") or "").strip()
    ]
    cancelled = [
        row for row in rows
        if str(row.get("status") or "").lower() == "cancelled"
    ]
    rejected = [
        row for row in rows
        if str(row.get("status") or "").lower() in {"rejected", "error"}
        or str(row.get("reject_reason") or "").strip()
    ]
    return {
        "rows": len(rows),
        "first_time_utc": rows[0].get("time_utc", "") if rows else "",
        "last_time_utc": rows[-1].get("time_utc", "") if rows else "",
        "by_status": by_status,
        "by_action_type": by_action_type,
        "by_instrument": by_instrument,
        "fill_like_count": len(fills),
        "cancelled_count": len(cancelled),
        "rejected_or_error_count": len(rejected),
        "recent_actions": rows[-10:],
    }


def summarize_event_signals(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    attempts = sum(safe_int(row.get("scout_attempts"), 0) for row in rows)
    signal_count = sum(safe_int(row.get("signal_count"), 0) for row in rows)
    triggered_research = sum(
        1
        for row in rows
        if str(row.get("triggered_research") or "").lower() in {"true", "1", "yes"}
    )
    return {
        "rows": len(rows),
        "first_time_utc": rows[0].get("time_utc", "") if rows else "",
        "last_time_utc": rows[-1].get("time_utc", "") if rows else "",
        "signal_count_sum": signal_count,
        "scout_attempts_sum": attempts,
        "triggered_research_count": triggered_research,
        "by_status": count_values(rows, "status"),
        "by_theme": count_values(rows, "theme"),
        "by_best_instrument": count_values(rows, "best_instrument"),
        "largest_best_net_pips": top_numeric(
            rows,
            "best_net_pips",
            limit=10,
            absolute=True,
        ),
    }


def summarize_scan(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    latest = rows[-1] if rows else {}
    return {
        "rows": len(rows),
        "first_time_utc": rows[0].get("time_utc", "") if rows else "",
        "last_time_utc": latest.get("time_utc", ""),
        "latest": {
            key: latest.get(key, "")
            for key in [
                "scanned",
                "tradeable",
                "spread_rejected",
                "candidate_windows",
                "signals",
                "themes",
                "pressure_watch",
                "pressure_trade",
                "scout_attempts",
                "reason",
            ]
        },
        "scout_attempts_sum": sum(safe_int(row.get("scout_attempts"), 0) for row in rows),
        "signals_sum": sum(safe_int(row.get("signals"), 0) for row in rows),
        "spread_rejected_sum": sum(safe_int(row.get("spread_rejected"), 0) for row in rows),
    }


def summarize_monitor(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    latest = rows[-1] if rows else {}
    return {
        "rows": len(rows),
        "last_time_utc": latest.get("time_utc", ""),
        "latest_nav": safe_float(latest.get("nav"), 0.0),
        "latest_margin_used_pct": safe_float(latest.get("margin_used_pct"), 0.0),
        "latest_open_trade_count": safe_int(latest.get("open_trade_count"), 0),
        "max_margin_used_pct_recent": max(
            [safe_float(row.get("margin_used_pct"), 0.0) for row in rows] or [0.0]
        ),
        "max_open_trade_count_recent": max(
            [safe_int(row.get("open_trade_count"), 0) for row in rows] or [0]
        ),
    }


def summarize_position_health(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    latest_by_trade: Dict[str, Dict[str, str]] = {}
    for row in rows:
        trade_id = str(row.get("trade_id") or "")
        if trade_id:
            latest_by_trade[trade_id] = row
    latest_rows = list(latest_by_trade.values())
    by_action = count_values(latest_rows, "health_action")
    by_instrument = count_values(latest_rows, "instrument")
    return {
        "rows": len(rows),
        "tracked_open_trade_like_count": len(latest_rows),
        "last_time_utc": rows[-1].get("time_utc", "") if rows else "",
        "by_health_action_latest": by_action,
        "by_instrument_latest": by_instrument,
        "latest_positions": [
            {
                "trade_id": row.get("trade_id", ""),
                "instrument": row.get("instrument", ""),
                "direction": row.get("direction", ""),
                "unrealized_pl": safe_float(row.get("unrealized_pl"), 0.0),
                "health_score": safe_float(row.get("health_score"), 0.0),
                "health_action": row.get("health_action", ""),
                "reason": row.get("reason", ""),
                "time_utc": row.get("time_utc", ""),
            }
            for row in sorted(
                latest_rows,
                key=lambda item: safe_float(item.get("health_score"), 0.0),
            )
        ],
    }


def summarize_lifecycle(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    fills = [
        row for row in rows
        if str(row.get("event") or "").lower() == "transaction_order_fill"
    ]
    closes = [
        row for row in rows
        if "close" in str(row.get("event") or "").lower()
        or safe_float(row.get("pl"), 0.0) != 0.0
    ]
    return {
        "rows": len(rows),
        "last_time_utc": rows[-1].get("time_utc", "") if rows else "",
        "fills": len(fills),
        "closes_or_realized_pl_rows": len(closes),
        "by_event": count_values(rows, "event"),
        "by_instrument": count_values(rows, "instrument"),
        "realized_pl_sum_recent": sum(safe_float(row.get("pl"), 0.0) for row in rows),
        "largest_realized_pl_abs": top_numeric(rows, "pl", limit=10, absolute=True),
    }


def build_report(*, data_dir: Path, row_limit: int) -> Dict[str, Any]:
    actions = recent_csv_rows(data_dir / "actions.csv", row_limit)
    event_signals = recent_csv_rows(data_dir / "event_signals.csv", row_limit)
    scan = recent_csv_rows(data_dir / "event_scan_summary.csv", row_limit)
    monitor = recent_csv_rows(data_dir / "monitor.csv", row_limit)
    health = recent_csv_rows(data_dir / "position_health.csv", row_limit)
    lifecycle = recent_csv_rows(data_dir / "trade_lifecycle_ledger.csv", row_limit)
    production_factor = PROMOTIONS_ROOT / "major_move_factor_production.json"
    shadow_factor = PROMOTIONS_ROOT / "major_move_factor_shadow.json"
    report = {
        "generated_utc": utc_iso(),
        "data_dir": str(data_dir),
        "row_limit_per_file": row_limit,
        "available": data_dir.exists(),
        "lane": "spike_scout_major_moves",
        "separate_lane": True,
        "trade_behavior_changed_by_report": False,
        "major_move_factor_shadow_exists": shadow_factor.exists(),
        "major_move_factor_production_exists": production_factor.exists(),
        "major_move_factor_can_modify_live_risk": production_factor.exists(),
        "actions": summarize_actions(actions),
        "event_signals": summarize_event_signals(event_signals),
        "event_scan_summary": summarize_scan(scan),
        "monitor": summarize_monitor(monitor),
        "position_health": summarize_position_health(health),
        "trade_lifecycle": summarize_lifecycle(lifecycle),
        "source_files": {
            name: str(data_dir / name)
            for name in [
                "actions.csv",
                "event_signals.csv",
                "event_scan_summary.csv",
                "monitor.csv",
                "position_health.csv",
                "trade_lifecycle_ledger.csv",
            ]
        },
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=SCOUT_DATA_DIR)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--row-limit", type=int, default=5000)
    args = parser.parse_args()
    report = build_report(data_dir=args.data_dir, row_limit=args.row_limit)
    atomic_write_json(args.report, report)
    print(json.dumps({
        "time_utc": report["generated_utc"],
        "available": report["available"],
        "latest_nav": report["monitor"].get("latest_nav"),
        "latest_open_trade_count": report["monitor"].get("latest_open_trade_count"),
        "recent_scout_attempts": report["event_signals"].get("scout_attempts_sum"),
        "recent_signal_count": report["event_signals"].get("signal_count_sum"),
        "major_move_factor_shadow_exists": report["major_move_factor_shadow_exists"],
        "major_move_factor_production_exists": report["major_move_factor_production_exists"],
        "report": str(args.report),
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
