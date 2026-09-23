"""Compact read-only primary account snapshot for chat monitoring."""

from __future__ import annotations

import json
import csv
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import monitor_primary_market_open as monitor  # noqa: E402

DATA_DIR = ROOT / "data" / "technical_scout_manager" / "account_live_primary_forecast_rotation"
ACTIONS_CSV = DATA_DIR / "actions.csv"
STATE_PATH = DATA_DIR / "monitor_primary_compact_state.json"


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def fnum(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "?"


def load_state() -> dict[str, Any]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_state(snapshot: dict[str, Any], action_count: int) -> None:
    trades = snapshot.get("trades") if isinstance(snapshot.get("trades"), list) else []
    state = {
        "sample_utc": snapshot.get("sample_utc"),
        "trade_ids": [str(t.get("id")) for t in trades if t.get("id") is not None],
        "action_count": action_count,
    }
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def read_action_rows() -> list[dict[str, Any]]:
    if not ACTIONS_CSV.exists():
        return []
    csv.field_size_limit(sys.maxsize)
    with ACTIONS_CSV.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def summarize_action(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "time_utc": row.get("time_utc"),
        "action": row.get("action"),
        "instrument": row.get("instrument"),
        "direction": row.get("direction"),
        "units": row.get("units") or row.get("planned_units"),
        "reason": row.get("reason"),
        "status": row.get("status"),
        "realized_pl": row.get("realized_pl"),
        "rank_score": row.get("rank_score"),
        "probability": row.get("probability"),
        "edge_pips": row.get("edge_pips"),
        "broker_trade_id": row.get("broker_trade_id"),
        "replacement": row.get("replacement_instrument"),
    }


def attach_since_last(snapshot: dict[str, Any]) -> dict[str, Any]:
    state = load_state()
    action_rows = [row for row in read_action_rows() if row.get("mode") == "live"]
    previous_action_count = state.get("action_count")
    if isinstance(previous_action_count, int) and previous_action_count <= len(action_rows):
        new_action_rows = action_rows[previous_action_count:]
    else:
        new_action_rows = action_rows[-8:]

    current_trades = snapshot.get("trades") if isinstance(snapshot.get("trades"), list) else []
    current_by_id = {str(t.get("id")): t for t in current_trades if t.get("id") is not None}
    previous_ids = set(str(tid) for tid in as_list(state.get("trade_ids")))
    current_ids = set(current_by_id)

    opened = [current_by_id[tid] for tid in sorted(current_ids - previous_ids)]
    closed_ids = sorted(previous_ids - current_ids)
    if previous_ids:
        snapshot["position_diff"] = {"opened": opened, "closed_ids": closed_ids}
    else:
        snapshot["position_diff"] = {"opened": [], "closed_ids": []}
    snapshot["actions_since_last"] = [summarize_action(row) for row in new_action_rows]
    snapshot["live_action_count"] = len(action_rows)
    save_state(snapshot, len(action_rows))
    return snapshot


def compact_snapshot() -> dict[str, Any]:
    row = monitor.sample()
    decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
    counts = decision.get("counts") if isinstance(decision.get("counts"), dict) else {}
    safety = decision.get("safety") if isinstance(decision.get("safety"), dict) else {}
    account = row.get("account") if isinstance(row.get("account"), dict) else {}
    stderr = row.get("stderr") if isinstance(row.get("stderr"), dict) else {}
    processes = [p for p in as_list(row.get("processes")) if isinstance(p, dict) and not p.get("error")]
    trades = account.get("trades") if isinstance(account.get("trades"), list) else []
    actions = as_list(decision.get("actions"))

    return {
        "sample_utc": row.get("sample_utc"),
        "decision_utc": decision.get("generated_utc"),
        "feature_utc": meta.get("latest_model_feature_time_utc"),
        "model_rows": meta.get("model_rows"),
        "tradable": meta.get("tradable_forecast_count"),
        "actions_count": counts.get("actions"),
        "open_count": account.get("openTradeCount"),
        "nav": account.get("NAV"),
        "upl": account.get("unrealizedPL"),
        "margin_used": account.get("marginUsed"),
        "margin_closeout_pct": account.get("marginCloseoutPercent"),
        "margin_available": account.get("marginAvailable"),
        "process_count": len(processes),
        "stderr_bytes": stderr.get("size_bytes"),
        "market_closed": safety.get("market_closed"),
        "rollover": safety.get("rollover_window"),
        "kill_switch": safety.get("kill_switch_active"),
        "rejected_by_reason": meta.get("rejected_by_reason"),
        "entry_block_reasons": decision.get("entry_block_reasons"),
        "entry_block_details": decision.get("entry_block_details"),
        "actions": actions,
        "trades": [
            {
                "id": trade.get("id"),
                "instrument": trade.get("instrument"),
                "units": trade.get("units"),
                "price": trade.get("price"),
                "upl": trade.get("unrealizedPL"),
                "openTime": trade.get("openTime"),
                "missingProtection": trade.get("missingProtection"),
                "protection": trade.get("protection"),
            }
            for trade in trades
            if isinstance(trade, dict)
        ],
    }


def print_compact(snapshot: dict[str, Any]) -> None:
    sample_utc = str(snapshot.get("sample_utc") or datetime.now(tz=UTC).isoformat())
    try:
        sample_time = monitor.parse_utc(sample_utc).strftime("%H:%M:%SZ")
    except Exception:
        sample_time = sample_utc
    line = (
        f"[{sample_time}] "
        f"feature={snapshot.get('feature_utc')} "
        f"tradable={snapshot.get('tradable')} actions={snapshot.get('actions_count')} "
        f"open={snapshot.get('open_count')} NAV={fnum(snapshot.get('nav'))} "
        f"uPL={fnum(snapshot.get('upl'))} margin={fnum(snapshot.get('margin_closeout_pct'), 5)} "
        f"proc={snapshot.get('process_count')} stderr={snapshot.get('stderr_bytes')} "
        f"kill={snapshot.get('kill_switch')}"
    )
    print(line, flush=True)

    trades = snapshot.get("trades") if isinstance(snapshot.get("trades"), list) else []
    if trades:
        print("  trades=" + json.dumps(trades, sort_keys=True), flush=True)
    actions = snapshot.get("actions") if isinstance(snapshot.get("actions"), list) else []
    if actions:
        print("  actions=" + json.dumps([summarize_action(a) for a in actions[:5]], sort_keys=True), flush=True)
    position_diff = snapshot.get("position_diff") if isinstance(snapshot.get("position_diff"), dict) else {}
    if position_diff:
        print("  position_diff=" + json.dumps(position_diff, sort_keys=True), flush=True)
    actions_since_last = snapshot.get("actions_since_last")
    if isinstance(actions_since_last, list) and actions_since_last:
        print("  actions_since_last=" + json.dumps(actions_since_last[-10:], sort_keys=True), flush=True)
    print(
        "  rejected="
        + json.dumps(snapshot.get("rejected_by_reason") or {}, sort_keys=True)
        + " blocks="
        + json.dumps(snapshot.get("entry_block_reasons") or {}, sort_keys=True),
        flush=True,
    )


def main() -> int:
    snapshot = compact_snapshot()
    if "--no-track" not in sys.argv:
        snapshot = attach_since_last(snapshot)
    if "--json" in sys.argv:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    else:
        print_compact(snapshot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
