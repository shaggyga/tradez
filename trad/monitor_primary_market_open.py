"""Read-only monitor for the primary OANDA forecast rotation bot.

This samples the live decision file, primary account status, process table, and
stderr log during a market-open window. It does not create, modify, or close
orders.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
ACCOUNT_DIR = ROOT / "data" / "technical_scout_manager" / "account_live_primary_forecast_rotation"
LATEST_DECISION = ACCOUNT_DIR / "latest_decision.json"
LATEST_LAUNCH = ACCOUNT_DIR / "latest_launch.json"
STATUS_SCRIPT = ROOT / "oanda_live_account_readonly_status.py"


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def parse_utc(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def process_rows() -> list[dict[str, Any]]:
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -like '*oanda_primary_forecast_rotation_bot.py*' "
        "-and $_.CommandLine -notlike '*Get-CimInstance*' } | "
        "Select-Object ProcessId,ParentProcessId,CommandLine | ConvertTo-Json -Compress -Depth 4"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode != 0:
        return [
            {
                "error": "process_query_failed",
                "returncode": completed.returncode,
                "stderr": completed.stderr[-1000:],
            }
        ]
    output = completed.stdout.strip()
    if not output:
        return []
    try:
        return [row for row in as_list(json.loads(output)) if isinstance(row, dict)]
    except json.JSONDecodeError:
        return [{"error": "process_query_json_decode_failed", "stdout": output[-1000:]}]


def primary_account_status() -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            str(STATUS_SCRIPT),
            "--json",
            "--role",
            "primary_live",
        ],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=60,
    )
    result: dict[str, Any] = {
        "helper_returncode": completed.returncode,
        "helper_stderr": completed.stderr[-2000:],
    }
    try:
        rows = json.loads(completed.stdout)
    except json.JSONDecodeError:
        result.update({"ok": False, "error": "account_status_json_decode_failed", "stdout": completed.stdout[-2000:]})
        return result
    primary_rows = [row for row in as_list(rows) if isinstance(row, dict) and row.get("role") == "primary_live"]
    if not primary_rows:
        result.update({"ok": False, "error": "primary_live_not_returned", "rows": rows})
        return result
    result.update(primary_rows[0])
    return result


def stderr_snapshot(launch: dict[str, Any]) -> dict[str, Any]:
    stderr_path = Path(str(launch.get("stderr") or ""))
    if not stderr_path:
        stderr_path = ACCOUNT_DIR / "logs" / "primary_live_dual_history_fastpull_stderr.log"
    if not stderr_path.exists():
        return {"path": str(stderr_path), "exists": False, "size_bytes": None, "tail": ""}
    size = stderr_path.stat().st_size
    tail = ""
    if size:
        with stderr_path.open("rb") as handle:
            handle.seek(max(0, size - 4000))
            tail = handle.read().decode("utf-8", errors="replace")
    return {"path": str(stderr_path), "exists": True, "size_bytes": size, "tail": tail}


def reason_counts(rows: list[Any], key: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        reason = row.get(key)
        if reason is None and key == "reason":
            reason = row.get("detail")
        if reason is None:
            reason = "unknown"
        counts[str(reason)] += 1
    return dict(sorted(counts.items()))


def compact_decision(decision: dict[str, Any]) -> dict[str, Any]:
    meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
    safety = decision.get("safety") if isinstance(decision.get("safety"), dict) else {}
    actions = as_list(decision.get("actions"))
    entry_blocks = as_list(decision.get("entry_blocks"))
    top_forecasts = as_list(decision.get("top_forecasts"))
    open_positions = as_list(decision.get("open_positions"))
    managed_positions = as_list(decision.get("live_managed_positions"))

    return {
        "generated_utc": decision.get("generated_utc"),
        "mode": decision.get("mode"),
        "source": decision.get("source"),
        "live_open_trade_count": decision.get("live_open_trade_count"),
        "meta": {
            "forecast_source": meta.get("forecast_source"),
            "model_stream": meta.get("model_stream"),
            "forecast_count": meta.get("forecast_count"),
            "model_rows": meta.get("model_rows"),
            "model_rows_by_timeframe": meta.get("model_rows_by_timeframe"),
            "latest_model_feature_time_utc": meta.get("latest_model_feature_time_utc"),
            "tradable_forecast_count": meta.get("tradable_forecast_count"),
            "rejected_active_count": meta.get("rejected_active_count"),
            "rejected_by_reason": meta.get("rejected_by_reason"),
        },
        "safety": safety,
        "counts": {
            "actions": len(actions),
            "entry_blocks": len(entry_blocks),
            "top_forecasts": len(top_forecasts),
            "open_positions": len(open_positions),
            "managed_positions": len(managed_positions),
        },
        "entry_block_reasons": reason_counts(entry_blocks, "reason"),
        "entry_block_details": reason_counts(entry_blocks, "detail"),
        "actions": actions,
        "top_forecasts": top_forecasts[:10],
        "open_positions": open_positions,
        "managed_positions": managed_positions,
    }


def sample() -> dict[str, Any]:
    launch = read_json(LATEST_LAUNCH) if LATEST_LAUNCH.exists() else {}
    decision = read_json(LATEST_DECISION)
    return {
        "sample_utc": utc_now().isoformat(),
        "processes": process_rows(),
        "account": primary_account_status(),
        "launch": launch,
        "stderr": stderr_snapshot(launch if isinstance(launch, dict) else {}),
        "decision": compact_decision(decision if isinstance(decision, dict) else {}),
    }


def fnum(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "?"


def print_sample(row: dict[str, Any]) -> None:
    decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
    counts = decision.get("counts") if isinstance(decision.get("counts"), dict) else {}
    safety = decision.get("safety") if isinstance(decision.get("safety"), dict) else {}
    account = row.get("account") if isinstance(row.get("account"), dict) else {}
    stderr = row.get("stderr") if isinstance(row.get("stderr"), dict) else {}
    processes = [p for p in as_list(row.get("processes")) if isinstance(p, dict) and not p.get("error")]

    sample_time = parse_utc(str(row.get("sample_utc"))).strftime("%H:%M:%SZ")
    line = (
        f"[{sample_time}] "
        f"decision={decision.get('generated_utc')} "
        f"feature={meta.get('latest_model_feature_time_utc')} "
        f"rows={meta.get('model_rows')} tradable={meta.get('tradable_forecast_count')} "
        f"actions={counts.get('actions')} open={account.get('openTradeCount')} "
        f"NAV={fnum(account.get('NAV'))} uPL={fnum(account.get('unrealizedPL'))} "
        f"marginCloseout={fnum(account.get('marginCloseoutPercent'), 5)} "
        f"proc={len(processes)} stderr={stderr.get('size_bytes')} "
        f"market_closed={safety.get('market_closed')} rollover={safety.get('rollover_window')}"
    )
    print(line, flush=True)

    rejected = meta.get("rejected_by_reason")
    if rejected:
        print(f"  rejected={json.dumps(rejected, sort_keys=True)}", flush=True)
    block_reasons = decision.get("entry_block_reasons")
    block_details = decision.get("entry_block_details")
    if block_reasons or block_details:
        print(
            f"  entry_blocks={json.dumps(block_reasons, sort_keys=True)} "
            f"details={json.dumps(block_details, sort_keys=True)}",
            flush=True,
        )

    actions = as_list(decision.get("actions"))
    if actions:
        print(f"  actions={json.dumps(actions[:5], sort_keys=True)}", flush=True)

    trades = account.get("trades") if isinstance(account.get("trades"), list) else []
    if trades:
        compact_trades = [
            {
                "id": trade.get("id"),
                "instrument": trade.get("instrument"),
                "units": trade.get("units"),
                "price": trade.get("price"),
                "upl": trade.get("unrealizedPL"),
                "missingProtection": trade.get("missingProtection"),
            }
            for trade in trades
            if isinstance(trade, dict)
        ]
        print(f"  trades={json.dumps(compact_trades, sort_keys=True)}", flush=True)

    alerts: list[str] = []
    if not processes:
        alerts.append("primary bot process not found")
    if not account.get("ok"):
        alerts.append(f"account helper problem: {account.get('error') or account.get('helper_stderr')}")
    if account.get("unprotectedTradeIds"):
        alerts.append(f"unprotected trades: {account.get('unprotectedTradeIds')}")
    if stderr.get("size_bytes"):
        alerts.append(f"stderr non-empty: {stderr.get('size_bytes')} bytes")
    if safety.get("kill_switch_active"):
        alerts.append("kill switch active")
    if alerts:
        print(f"  ALERT {'; '.join(alerts)}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-seconds", type=int, default=120)
    parser.add_argument("--duration-minutes", type=float, default=None)
    parser.add_argument("--until-utc", default=None)
    parser.add_argument("--log-path", type=Path, default=None)
    args = parser.parse_args(argv)

    if args.until_utc:
        until = parse_utc(args.until_utc)
    elif args.duration_minutes is not None:
        until = utc_now() + timedelta(minutes=args.duration_minutes)
    else:
        until = utc_now() + timedelta(hours=2)

    log_dir = ACCOUNT_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.log_path or log_dir / f"market_open_monitor_{utc_now().strftime('%Y%m%d_%H%M%S')}.jsonl"

    print(f"monitor_start_utc={utc_now().isoformat()} until_utc={until.isoformat()} log={log_path}", flush=True)
    with log_path.open("a", encoding="utf-8") as handle:
        while True:
            try:
                row = sample()
            except Exception as exc:  # Keep monitoring through transient API/file errors.
                row = {
                    "sample_utc": utc_now().isoformat(),
                    "monitor_error": repr(exc),
                }
                print(f"[{utc_now().strftime('%H:%M:%SZ')}] monitor_error={exc!r}", flush=True)
            handle.write(json.dumps(row, sort_keys=True) + "\n")
            handle.flush()
            if "monitor_error" not in row:
                print_sample(row)

            now = utc_now()
            if now >= until:
                break
            sleep_for = min(max(args.interval_seconds, 10), max(1.0, (until - now).total_seconds()))
            time.sleep(sleep_for)

    print(f"monitor_done_utc={utc_now().isoformat()} log={log_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
