#!/usr/bin/env python3
"""Print a concise live status for the local OANDA forex stack.

This is intentionally read-only.  It reads:

- config/accounts_registry.json
- monitor/state CSV/JSON files written by the managers
- local process command lines
- promotion manifests

It does not import the trading managers, call OANDA, or place/cancel orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import subprocess
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_REGISTRY = ROOT / "config" / "accounts_registry.json"
MODEL_SPACE_AGENDA_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "model_space"
    / "model_space_agenda_latest.json"
)
MODEL_SPACE_PLAN_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "model_space"
    / "model_space_plan_latest.json"
)
MODEL_METRICS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_model_metrics.json"
)
SPIKE_SCOUT_REPORT_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_spike_scout_report.json"
)
MISSED_SPIKE_BACKTEST_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_spike_missed_move_backtest.json"
)
MISSED_SPIKE_CAPTURE_GAP_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_missed_spike_capture_gap_report.json"
)
PRE_SPIKE_LEAD_BACKTEST_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_pre_spike_lead_backtest.json"
)
PRE_SPIKE_LEAD_TRAINER_FOCUS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_pre_spike_lead_trainer_focus.json"
)
TRAINER_REPORTING_EXTENSIONS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_trainer_reporting_extensions.json"
)
TRAINER_REPORT_DIR = ROOT / "data" / "oanda_training_manager" / "reports"
TRAINER_RESEARCH_STATE_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "research_state.json"
)
LIVE_WATCHDOG_SUMMARY_PATH = (
    ROOT / "data" / "runtime_logs" / "live_account_watchdog_latest.json"
)
RESEARCH_TRAINER_LAUNCHER_LATEST_PATH = (
    ROOT / "data" / "runtime_logs" / "research_trainer_launcher_latest.json"
)
LIVE_WATCHDOG_PROCESS_STALE_MINUTES = 15.0
LATEST_ACCOUNT_IMPROVEMENT_SEED_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_weekend_account_improvement_seed.json"
)
MISSED_SPIKE_USDZAR_ROBUST_SEED_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_missed_spike_usdzar_robust_seed.json"
)
MISSED_SPIKE_USDZAR_RELAXED_SEED_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_missed_spike_usdzar_relaxed_seed.json"
)
TRAINER_PROMOTION_READINESS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "trainer_promotion_readiness.csv"
)
TRAINER_EXPERIMENT_LEDGER_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "continuous_research"
    / "experiment_ledger.csv"
)
TRAINER_RESEARCH_QUEUE_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "model_lifecycle"
    / "research_queue.jsonl"
)
LIVE_DAY_MONITOR_STATUS_PATH = ROOT / "data" / "live_day_monitor" / "latest_status.json"
LIVE_DAY_MONITOR_STATUS_MAX_AGE_MINUTES = 20.0
LIVE_DAY_MONITOR_ROLE_KEYS = {
    "gpt_live_prod": "gpt",
    "tech_live_prod": "tech",
    "primary_live_challenger": "primary",
}
PRE_SPIKE_LEAD_FOLDS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_pre_spike_lead_backtest_folds.csv"
)
PRE_SPIKE_LEAD_TRAINER_FOCUS_FOLDS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_pre_spike_lead_trainer_focus_folds.csv"
)
PRE_SPIKE_LEAD_ML_FAST_FOLDS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "latest_pre_spike_lead_ml_fast_folds.csv"
)
PRESSURE_VALIDATION_REPORTS = [
    (
        "primary_exact",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_m1_primary_exact_7d"
        / "summary.json",
        "score85_ratio1_value0p075_cluster20",
    ),
    (
        "primary_allpairs3d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_m1_primary_exact_allpairs_3d"
        / "summary.json",
        "score85_ratio1_value0p075_cluster20",
    ),
    (
        "primary_cluster16_3d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_gate_cluster16_3d_20260629"
        / "summary.json",
        "score85_ratio1_value0p075_cluster16",
    ),
    (
        "primary_cluster16_7d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_gate_cluster16_7d_20260629"
        / "summary.json",
        "score85_ratio1_value0p075_cluster16",
    ),
    (
        "primary_dir16_7d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_gate_cluster16_dir16_7d_20260629"
        / "summary.json",
        "score85_ratio1_value0p075_cluster16_dircluster16",
    ),
    (
        "primary_dir14_7d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_gate_cluster16_dir14_7d_20260629"
        / "summary.json",
        "score85_ratio1_value0p075_cluster16_dircluster14",
    ),
    (
        "primary_dir12_7d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_gate_cluster16_dir12_7d_20260629"
        / "summary.json",
        "score85_ratio1_value0p075_cluster16_dircluster12",
    ),
    (
        "primary_dir12_14d",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_gate_cluster16_dir12_14d_20260629"
        / "summary.json",
        "score85_ratio1_value0p075_cluster16_dircluster12",
    ),
    (
        "current_value05",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_m1_current_7d"
        / "summary.json",
        "score78_ratio0p8_value0p05_cluster20",
    ),
    (
        "tech_value012",
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "pressure_m1_tech_value012_cluster20_7d"
        / "summary.json",
        "score78_ratio0p8_value0p012_cluster20",
    ),
]
TRAINER_RECENT_ERRORS_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "live_dum1"
    / "recent_errors.csv"
)
DAILY_MOVE_COMPARISON_ROOT = (
    ROOT
    / "data"
    / "forex"
    / "reports"
    / "daily_move_comparison"
)
PRIMARY_SCOUT_LIVE_ROOT = (
    ROOT
    / "data"
    / "technical_scout_manager"
    / "account_live_primary_challenger_scout"
)
TECH_SCOUT_LIVE_ROOT = (
    ROOT
    / "data"
    / "technical_scout_manager"
    / "account_live_tech_broad_regime_scout"
)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default
    return default


def read_jsonl_rows(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if isinstance(payload, dict):
                    rows.append(payload)
        if limit is not None and limit >= 0:
            return rows[-limit:]
        return rows
    except Exception:
        return []


def read_last_csv_row(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        return dict(rows[-1]) if rows else {}
    except Exception:
        return {}


def read_csv_rows(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
        if limit is not None and limit >= 0:
            return rows[-limit:]
        return rows
    except Exception:
        return []


def read_csv_tail_rows(path: Path, max_rows: int = 1000) -> list[dict[str, Any]]:
    """Read only the final CSV rows into memory.

    Some live journals grow quickly.  The advisor view only needs recent rows,
    so avoid materializing large files just to print a short status line.
    """
    if not path.exists() or max_rows <= 0:
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = deque(csv.DictReader(handle), maxlen=max_rows)
        return [dict(row) for row in rows]
    except Exception:
        return []


def dedupe_daily_report_rows(rows: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        time_key = str(row.get("time_ny") or "")[:19]
        key = tuple([time_key] + [str(row.get(field) or "").strip() for field in key_fields])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def count_rows_by_field(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "").strip()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return [
        {field: key, "count": count}
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def classify_miss_detail(detail: Any) -> str:
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


def extract_detail_value_usd(detail: Any) -> float | None:
    """Extract the expected/audited dollar value from a missed-move detail.

    Daily move rows are intentionally schema-light because they aggregate
    multiple manager journals.  The useful value signal currently lives in
    strings such as:

      - "audit[value=$0.1019 return=1.035%]"
      - "value gate: expected $0.0050 < $0.0120"

    Prefer those explicit value/expected fields; fall back to the first dollar
    amount only when no labeled value is present.
    """
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
    except Exception:
        return None


def extract_value_gate_pair(detail: Any) -> tuple[float, float] | None:
    """Extract ``expected < threshold`` from a value-gate miss string."""
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
    except Exception:
        return None


def near_value_gate_summary(rows: list[dict[str, Any]], min_threshold_ratio: float = 0.50) -> dict[str, Any]:
    """Summarize misses that were close to the live value gate.

    These are useful review candidates because they were not obvious no-value
    skips, but still failed the expected-dollar threshold.
    """
    by_instrument: dict[str, dict[str, Any]] = {}
    total_expected = 0.0
    count = 0
    for row in rows:
        parsed = extract_value_gate_pair(row.get("detail"))
        if not parsed:
            continue
        expected, threshold = parsed
        if threshold <= 0.0 or expected <= 0.0:
            continue
        ratio = expected / threshold
        if ratio < min_threshold_ratio:
            continue
        instrument = str(row.get("instrument") or "").strip() or "UNKNOWN"
        bucket = by_instrument.setdefault(
            instrument,
            {
                "instrument": instrument,
                "count": 0,
                "expected_usd": 0.0,
                "threshold_usd": threshold,
                "max_ratio": 0.0,
            },
        )
        bucket["count"] = int(bucket.get("count") or 0) + 1
        bucket["expected_usd"] = float(bucket.get("expected_usd") or 0.0) + expected
        bucket["threshold_usd"] = max(float(bucket.get("threshold_usd") or 0.0), threshold)
        bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), ratio)
        total_expected += expected
        count += 1
    top = sorted(
        by_instrument.values(),
        key=lambda item: (-(float(item.get("expected_usd") or 0.0)), -int(item.get("count") or 0), str(item.get("instrument") or "")),
    )[:5]
    return {
        "count": count,
        "total_expected_usd": total_expected,
        "top_instruments": top,
    }


def missed_value_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    instrument_values: dict[str, dict[str, Any]] = {}
    event_values: list[dict[str, Any]] = []
    total_value = 0.0
    value_rows = 0
    for row in rows:
        value = extract_detail_value_usd(row.get("detail"))
        if value is None:
            continue
        value_rows += 1
        total_value += value
        instrument = str(row.get("instrument") or "").strip() or "UNKNOWN"
        bucket = instrument_values.setdefault(instrument, {"instrument": instrument, "value_usd": 0.0, "count": 0})
        bucket["value_usd"] = float(bucket.get("value_usd") or 0.0) + value
        bucket["count"] = int(bucket.get("count") or 0) + 1
        event_values.append(
            {
                "instrument": instrument,
                "direction": row.get("direction") or "",
                "miss_reason": row.get("miss_reason") or "",
                "gate": classify_miss_detail(row.get("detail")),
                "value_usd": value,
            }
        )
    by_instrument = sorted(
        instrument_values.values(),
        key=lambda item: (-(float(item.get("value_usd") or 0.0)), str(item.get("instrument") or "")),
    )
    top_events = sorted(
        event_values,
        key=lambda item: (-(float(item.get("value_usd") or 0.0)), str(item.get("instrument") or "")),
    )
    return {
        "value_rows": value_rows,
        "total_value_usd": total_value,
        "top_value_instruments": by_instrument[:5],
        "top_value_events": top_events[:5],
    }


def recent_regime_lock_seed_summary(
    path: Path = TRAINER_RESEARCH_QUEUE_PATH,
    *,
    max_rows: int = 800,
    max_age_minutes: float = 180.0,
) -> dict[str, Any]:
    """Summarize recent research specs seeded from scout regime-lock misses."""
    rows = read_jsonl_rows(path, limit=max_rows)
    if not rows:
        return {"count": 0, "top_profiles": [], "top_instruments": []}
    now = datetime.now(timezone.utc)
    profile_counts: Counter[str] = Counter()
    instrument_counts: Counter[str] = Counter()
    count = 0
    latest_time: datetime | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        spec = row.get("spec") if isinstance(row.get("spec"), dict) else {}
        profile = str(spec.get("specialist_profile") or "")
        if "current_regime_lock" not in profile:
            continue
        raw_time = str(row.get("time_utc") or "").strip()
        try:
            parsed = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        except Exception:
            parsed = None
        if parsed is not None:
            age = (now - parsed.astimezone(timezone.utc)).total_seconds() / 60.0
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
        "latest_age_minutes": utc_age_minutes(latest_time.isoformat()) if latest_time else None,
        "top_profiles": [{"name": name, "count": cnt} for name, cnt in profile_counts.most_common(3)],
        "top_instruments": [{"instrument": name, "count": cnt} for name, cnt in instrument_counts.most_common(6)],
    }


def recent_capture_gap_seed_summary(
    path: Path = TRAINER_RESEARCH_QUEUE_PATH,
    *,
    max_rows: int = 1000,
    max_age_minutes: float = 180.0,
) -> dict[str, Any]:
    """Summarize recent research specs seeded from missed-spike capture gaps."""
    rows = read_jsonl_rows(path, limit=max_rows)
    if not rows:
        return {"count": 0, "top_profiles": [], "top_instruments": []}
    now = datetime.now(timezone.utc)
    profile_counts: Counter[str] = Counter()
    instrument_counts: Counter[str] = Counter()
    count = 0
    latest_time: datetime | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        spec = row.get("spec") if isinstance(row.get("spec"), dict) else {}
        profile = str(spec.get("specialist_profile") or "")
        if "capture_gap" not in profile:
            continue
        raw_time = str(row.get("time_utc") or "").strip()
        try:
            parsed = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        except Exception:
            parsed = None
        if parsed is not None:
            age = (now - parsed.astimezone(timezone.utc)).total_seconds() / 60.0
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
        "latest_age_minutes": utc_age_minutes(latest_time.isoformat()) if latest_time else None,
        "top_profiles": [{"name": name, "count": cnt} for name, cnt in profile_counts.most_common(3)],
        "top_instruments": [{"instrument": name, "count": cnt} for name, cnt in instrument_counts.most_common(6)],
    }


def captured_value_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    order_keys: set[str] = set()
    movement_values: dict[str, dict[str, Any]] = {}
    order_values: dict[str, dict[str, Any]] = {}

    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        if not status.startswith("accepted"):
            continue
        event_type = str(row.get("event_type") or "").strip().lower()
        instrument = str(row.get("instrument") or "").strip() or "UNKNOWN"
        direction = str(row.get("direction") or "").strip()
        reason = row.get("reason") or row.get("detail") or ""
        value = extract_detail_value_usd(reason) or 0.0

        trade_id = str(row.get("trade_id") or "").strip()
        order_id = str(row.get("order_id") or "").strip()
        if event_type == "order_result" or trade_id:
            order_key = trade_id or order_id or f"{instrument}:{direction}:{row.get('price') or ''}"
            if order_key:
                order_keys.add(order_key)
                order_values.setdefault(
                    order_key,
                    {
                        "instrument": instrument,
                        "direction": direction,
                        "value_usd": value,
                    },
                )

        movement_key = str(row.get("movement_key") or "").strip()
        if movement_key:
            movement_values.setdefault(
                movement_key,
                {
                    "instrument": instrument,
                    "direction": direction,
                    "value_usd": value,
                },
            )

    value_source = movement_values if movement_values else order_values
    total_value = sum(float(item.get("value_usd") or 0.0) for item in value_source.values())
    by_instrument: dict[str, dict[str, Any]] = {}
    for item in value_source.values():
        instrument = str(item.get("instrument") or "UNKNOWN")
        bucket = by_instrument.setdefault(instrument, {"instrument": instrument, "value_usd": 0.0, "count": 0})
        bucket["value_usd"] = float(bucket.get("value_usd") or 0.0) + float(item.get("value_usd") or 0.0)
        bucket["count"] = int(bucket.get("count") or 0) + 1
    top = sorted(
        by_instrument.values(),
        key=lambda item: (-(float(item.get("value_usd") or 0.0)), str(item.get("instrument") or "")),
    )
    return {
        "captured_orders": len(order_keys),
        "captured_movements": len(movement_values),
        "total_value_usd": total_value,
        "top_value_instruments": top[:5],
    }


def crisis_summary(state: dict[str, Any]) -> str:
    crisis = state.get("live_crisis_mode")
    if not isinstance(crisis, dict) or not crisis.get("active"):
        return ""
    mode = str(crisis.get("mode") or "active")
    blocks = "blocks" if crisis.get("blocks_trading") else "advisory"
    triggers = crisis.get("triggers")
    trigger_count = len(triggers) if isinstance(triggers, list) else 0
    updated = crisis.get("updated_utc") or ""
    age = utc_age_minutes(updated)
    age_part = f" age={age:.0f}m" if age is not None else ""
    return f"crisis={mode}/{blocks} triggers={trigger_count}{age_part}"


def recent_error_summary(data_dir: Path | None) -> str:
    if not data_dir:
        return ""
    path = data_dir / "recent_errors.csv"
    count = count_csv_data_rows(path)
    if not count:
        return ""
    row = read_last_csv_row(path)
    time_utc = row.get("time_utc") or row.get("time") or ""
    age = utc_age_minutes(time_utc)
    age_part = f" age={age:.0f}m" if age is not None else ""
    error_type = row.get("error_type") or row.get("type") or "error"
    message = str(row.get("error_message") or row.get("message") or "").strip()
    if len(message) > 48:
        message = message[:45] + "..."
    message_part = f":{message}" if message else ""
    return f"errors={count} last={error_type}{message_part}{age_part}"


def count_csv_data_rows(path: Path | None) -> int | None:
    """Count data rows in a CSV without loading it into memory."""
    if not path or not path.exists():
        return None
    try:
        line_count = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                line_count += chunk.count(b"\n")
        return max(0, line_count - 1)
    except Exception:
        return None


def parse_raw_json(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_json")
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def maybe_float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def utc_age_minutes(value: Any) -> float | None:
    try:
        raw = str(value or "").strip()
        if not raw:
            return None
        # PowerShell JSON timestamps can include 7 fractional digits; Python
        # datetime accepts microseconds, so trim only the excess precision.
        raw = re.sub(r"(\.\d{6})\d+(?=Z|[+-]\d{2}:\d{2}|$)", r"\1", raw)
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds() / 60.0)
    except Exception:
        return None


def rel_path(path: str | Path | None) -> Path | None:
    if not path:
        return None
    p = Path(str(path))
    return p if p.is_absolute() else ROOT / p


def pid_is_running(pid: Any) -> bool:
    try:
        number = int(str(pid or "").strip())
    except Exception:
        return False
    if number <= 0:
        return False
    if platform.system().lower().startswith("win"):
        try:
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    (
                        f"$p = Get-Process -Id {number} -ErrorAction SilentlyContinue; "
                        "if ($p) { '1' }"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return completed.returncode == 0 and "1" in completed.stdout
        except Exception:
            return False
    try:
        os.kill(number, 0)
        return True
    except Exception:
        return False


def merge_process_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for group in groups:
        for row in group:
            pid = str(row.get("pid") or "")
            command = str(row.get("command") or "")
            key = (pid, command)
            if not pid or key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def watchdog_process_rows() -> list[dict[str, Any]]:
    """Use the live watchdog's read-only PID snapshot when CIM/ps is blocked."""
    snapshot = read_json(LIVE_WATCHDOG_SUMMARY_PATH, {})
    if not isinstance(snapshot, dict):
        return []
    age = utc_age_minutes(snapshot.get("time"))
    if age is None or age > LIVE_WATCHDOG_PROCESS_STALE_MINUTES:
        return []
    lanes = snapshot.get("lanes") if isinstance(snapshot.get("lanes"), list) else []
    rows: list[dict[str, Any]] = []
    for lane in lanes:
        if not isinstance(lane, dict):
            continue
        pid = str(lane.get("pid") or "").strip()
        script = str(lane.get("script") or "").strip()
        status = str(lane.get("status") or "").strip().lower()
        if not pid or not script or status != "ok":
            continue
        rows.append({
            "pid": pid,
            "command": f"live_account_watchdog:{script}",
            "source": "live_account_watchdog",
        })
    return rows


def process_rows() -> list[dict[str, Any]]:
    """Return local process rows with pid and command line.

    Uses PowerShell on Windows because it preserves full command lines better
    than plain tasklist.  Falls back to ps elsewhere, then to the live watchdog
    snapshot for guarded live account lanes.
    """
    try:
        if platform.system().lower().startswith("win"):
            command = [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "Get-CimInstance Win32_Process | "
                    "Select-Object ProcessId,CommandLine | "
                    "ConvertTo-Json -Compress"
                ),
            ]
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return watchdog_process_rows()
            payload = json.loads(result.stdout)
            if isinstance(payload, dict):
                payload = [payload]
            out = []
            for row in payload:
                command_line = str(row.get("CommandLine") or "")
                out.append({
                    "pid": str(row.get("ProcessId") or ""),
                    "command": command_line,
                })
            return merge_process_rows(out, watchdog_process_rows())
        result = subprocess.run(
            ["ps", "-eo", "pid,args"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        rows = []
        for line in result.stdout.splitlines()[1:]:
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                rows.append({"pid": parts[0], "command": parts[1]})
        return merge_process_rows(rows, watchdog_process_rows())
    except Exception:
        return watchdog_process_rows()


def process_matches(processes: list[dict[str, Any]], script: str | None) -> list[str]:
    if not script:
        return []
    needle = script.lower()
    return [
        str(row["pid"])
        for row in processes
        if needle in str(row.get("command") or "").lower()
        and "forex_stack_status.py" not in str(row.get("command") or "").lower()
    ]


def format_num(value: Any, digits: int = 2) -> str:
    number = maybe_float(value)
    if number is None:
        return ""
    return f"{number:.{digits}f}"


def trade_summary(raw_payload: dict[str, Any]) -> str:
    trades = raw_payload.get("open_trades") or []
    if not isinstance(trades, list) or not trades:
        return ""
    parts = []
    for trade in trades[:6]:
        inst = str(trade.get("instrument") or "")
        direction = str(trade.get("direction") or "")
        units = trade.get("current_units", trade.get("initial_units", ""))
        pl = format_num(trade.get("unrealized_pl"), 4)
        label = f"{inst} {direction}".strip()
        if units not in ("", None):
            label += f" {units}"
        if pl:
            label += f" P/L={pl}"
        parts.append(label)
    if len(trades) > len(parts):
        parts.append(f"+{len(trades) - len(parts)} more")
    return "; ".join(parts)


def live_day_monitor_account_snapshot(role: str) -> tuple[dict[str, Any], float | None]:
    """Return a fresh direct-broker account snapshot from the day monitor.

    Manager-local monitor CSVs can lag closed trades because they are written by
    strategy loops.  The day monitor performs a read-only broker poll, so prefer
    it for display-only account open-trade rows when it is fresh.
    """
    account_key = LIVE_DAY_MONITOR_ROLE_KEYS.get(role)
    if not account_key:
        return {}, None
    payload = read_json(LIVE_DAY_MONITOR_STATUS_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return {}, None
    age = utc_age_minutes(payload.get("time_utc"))
    if age is None or age > LIVE_DAY_MONITOR_STATUS_MAX_AGE_MINUTES:
        return {}, age
    accounts = payload.get("accounts") if isinstance(payload.get("accounts"), dict) else {}
    account = accounts.get(account_key) if isinstance(accounts, dict) else None
    return (account if isinstance(account, dict) else {}), age


def live_day_monitor_trade_summary(account: dict[str, Any]) -> str:
    trades = account.get("trades") if isinstance(account, dict) else []
    if not isinstance(trades, list) or not trades:
        return ""
    parts: list[str] = []
    for trade in trades[:6]:
        if not isinstance(trade, dict):
            continue
        inst = str(trade.get("instrument") or "")
        units = trade.get("units", trade.get("currentUnits", ""))
        pl = format_num(trade.get("unrealizedPL"), 4)
        label = inst
        if units not in ("", None):
            label += f" {units}"
        if pl:
            label += f" P/L={pl}"
        parts.append(label.strip())
    if len(trades) > len(parts):
        parts.append(f"+{len(trades) - len(parts)} more")
    return "; ".join(part for part in parts if part)


def live_day_monitor_status_summary() -> str:
    payload = read_json(LIVE_DAY_MONITOR_STATUS_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return ""
    age = utc_age_minutes(payload.get("time_utc"))
    age_part = f" age={age:.0f}m" if age is not None else ""
    status_text = str(payload.get("status") or "unknown").strip() or "unknown"
    processes = payload.get("processes") if isinstance(payload.get("processes"), dict) else {}
    day_monitor = processes.get("day_monitor") if isinstance(processes, dict) else {}
    pid = str(day_monitor.get("pid") or "") if isinstance(day_monitor, dict) else ""
    running = bool(day_monitor.get("running")) if isinstance(day_monitor, dict) else False
    pid_part = f" pid={pid}" if pid else ""
    accounts = payload.get("accounts") if isinstance(payload.get("accounts"), dict) else {}
    account_parts = []
    for key in ("gpt", "tech", "primary"):
        account = accounts.get(key) if isinstance(accounts, dict) else {}
        if not isinstance(account, dict):
            continue
        open_trades = account.get("open_trades")
        nav = format_num(account.get("nav"), 2)
        if open_trades in ("", None) and not nav:
            continue
        nav_part = f" nav={nav}" if nav else ""
        account_parts.append(f"{key}:open={open_trades}{nav_part}")
    account_text = " ".join(account_parts)
    return (
        f"day_monitor:status={status_text} "
        f"running={running}{pid_part}{age_part} {account_text}"
    ).strip()


def production_model_summary(path: Path | None) -> str:
    if not path:
        return ""
    manifest = read_json(path, {})
    if not isinstance(manifest, dict) or not manifest:
        return ""
    model_type = manifest.get("model_type", "")
    target = manifest.get("target", "")
    stage = manifest.get("stage", "")
    active = manifest.get("activation_effective")
    threshold = (
        (manifest.get("validation") or {})
        .get("selected_threshold", {})
        .get("threshold", "")
    )
    score = manifest.get("research_score", "")
    bits = []
    if model_type or target:
        bits.append(f"{model_type}:{target}")
    if threshold != "":
        bits.append(f"thr={threshold}")
    if score != "":
        bits.append(f"score={format_num(score, 2)}")
    benchmark = manifest.get("benchmark_comparison") or {}
    if isinstance(benchmark, dict):
        if benchmark.get("arima_available"):
            status = (
                "beat"
                if benchmark.get("candidate_beats_arima") is True
                else "block"
            )
            bits.append(
                f"arima={status}:{format_num(benchmark.get('arima_mean_net_pips'), 2)}"
            )
        elif "arima_available" in benchmark:
            bits.append("arima=na")
    coverage = manifest.get("arima_coverage") or {}
    if isinstance(coverage, dict) and coverage.get("scorecard_pairs") not in ("", None):
        bits.append(
            "arima_cov="
            f"{coverage.get('stable_required_pairs', 0)}/"
            f"{coverage.get('matched_pairs_total', 0)}/"
            f"{coverage.get('scorecard_pairs', 0)}"
        )
    challenger = manifest.get("arima_challenger_report") or {}
    if isinstance(challenger, dict) and challenger.get("arima_beats_current_pair_count") not in ("", None):
        bits.append(
            f"arima_chal={challenger.get('arima_beats_current_pair_count', 0)}"
        )
    if stage:
        bits.append(f"stage={stage}")
    if active is not None:
        bits.append(f"active={bool(active)}")
    return " ".join(str(bit) for bit in bits if str(bit))


def ensemble_shadow_summary(path: Path | None) -> str:
    if not path:
        return ""
    manifest = read_json(path, {})
    if not isinstance(manifest, dict) or not manifest:
        return ""
    validation = manifest.get("validation") or {}
    selected = validation.get("selected_threshold") or {}
    gate = validation.get("gate") or {}
    mode = manifest.get("ensemble_mode", manifest.get("stage", "ensemble"))
    trades = selected.get("trades", "")
    mean_net = selected.get("mean_net_pips", "")
    weeks = selected.get("weeks", "")
    passed = gate.get("passed", "")
    return (
        f"ensemble:{mode} "
        f"gate={passed} trades={format_num(trades, 0)} "
        f"mean={format_num(mean_net, 2)} weeks={format_num(weeks, 0)}"
    ).strip()


def arima_challenger_summary(path: Path | None) -> str:
    if not path:
        return ""
    manifest = read_json(path, {})
    if not isinstance(manifest, dict) or not manifest:
        return ""
    latest_gate = manifest.get("latest_shadow_gate") or {}
    latest_aggregate = manifest.get("latest_shadow_aggregate") or {}
    repeated_gate = manifest.get("shadow_repeated_gate") or {}
    shadow_bits = ""
    if isinstance(latest_gate, dict) and latest_gate:
        shadow_bits = (
            f" shadow_pass={all(bool(value) for value in latest_gate.values())} "
            f"mean={format_num(latest_aggregate.get('mean_net_pips'), 2)} "
            f"pf={format_num(latest_aggregate.get('profit_factor'), 2)}"
        )
    if isinstance(repeated_gate, dict) and repeated_gate:
        shadow_bits += (
            f" streak={repeated_gate.get('consecutive_independent_passes', 0)}/"
            f"{repeated_gate.get('required_consecutive_independent_passes', 0)}"
            f" repeated={bool(repeated_gate.get('passed', False))}"
        )
    robust_count = manifest.get("robust_arima_challenger_count")
    if robust_count not in ("", None):
        shadow_bits += f" robust={format_num(robust_count, 0)}"
    return (
        f"arima_shadow:stage={manifest.get('stage', '')} "
        f"challengers={format_num(manifest.get('challenger_count'), 0)} "
        f"exec={bool(manifest.get('execution_enabled', False))}"
        f"{shadow_bits}"
    ).strip()


def arima_shadow_report_summary(path: Path | None) -> str:
    if not path:
        return ""
    report = read_json(path, {})
    if not isinstance(report, dict) or not report:
        return ""
    aggregate = report.get("aggregate") or {}
    return (
        f"arima_shadow_report:passed={bool(report.get('passed', False))} "
        f"pairs={report.get('pair_gate_passed_count', '')}/"
        f"{report.get('evaluated_pair_count', '')}/"
        f"{report.get('challenger_count', '')} "
        f"trades={format_num(aggregate.get('trades'), 0)} "
        f"mean={format_num(aggregate.get('mean_net_pips'), 2)} "
        f"pf={format_num(aggregate.get('profit_factor'), 2)}"
    ).strip()


def arima_adapter_summary(report_path: Path | None, pending_path: Path | None) -> str:
    report = read_json(report_path, {}) if report_path else {}
    pending = read_json(pending_path, {}) if pending_path else {}
    if not isinstance(report, dict):
        report = {}
    if not isinstance(pending, dict):
        pending = {}
    if not report and not pending:
        return ""
    blockers = report.get("canary_assignment_blockers")
    if not isinstance(blockers, list):
        blockers = pending.get("canary_assignment_blockers")
    if not isinstance(blockers, list):
        blockers = []
    return (
        f"arima_adapter:passed={bool(report.get('passed', False))} "
        f"ready={bool(report.get('canary_assignment_ready', pending.get('canary_assignment_ready', False)))} "
        f"signals={report.get('actionable_signal_count', '')}/"
        f"{report.get('signal_count', '')} "
        f"stage={pending.get('stage', '')} "
        f"exec={bool(pending.get('execution_enabled', False))} "
        f"blockers={len(blockers)}"
    ).strip()


def arima_canary_executor_summary(report_path: Path | None) -> str:
    if not report_path:
        return ""
    report = read_json(report_path, {})
    if not isinstance(report, dict) or not report:
        return ""
    blockers = report.get("blockers")
    if not isinstance(blockers, list):
        blockers = []
    return (
        f"arima_exec:pending={bool(report.get('pending_manifest_supported', False))} "
        f"orders_adapter={bool(report.get('order_adapter_supported', False))} "
        f"broker={bool(report.get('broker_execution_supported', False))} "
        f"orders={report.get('order_candidate_count', '')} "
        f"exec_available={bool(report.get('broker_execution_available', False))} "
        f"blockers={len(blockers)}"
    ).strip()


def spike_scout_report_summary(path: Path | None = SPIKE_SCOUT_REPORT_PATH) -> str:
    report = read_json(path, {}) if path else {}
    if not isinstance(report, dict) or not report:
        return ""
    signals = report.get("event_signals") or {}
    monitor = report.get("monitor") or {}
    health = report.get("position_health") or {}
    if not isinstance(signals, dict):
        signals = {}
    if not isinstance(monitor, dict):
        monitor = {}
    if not isinstance(health, dict):
        health = {}
    return (
        "spike_report:"
        f"signals={signals.get('signal_count_sum', '')} "
        f"attempts={signals.get('scout_attempts_sum', '')} "
        f"open_like={health.get('tracked_open_trade_like_count', '')} "
        f"nav={format_num(monitor.get('latest_nav'), 2)} "
        f"factor_shadow={bool(report.get('major_move_factor_shadow_exists', False))} "
        f"factor_prod={bool(report.get('major_move_factor_production_exists', False))}"
    ).strip()


def missed_spike_backtest_summary(path: Path | None = MISSED_SPIKE_BACKTEST_PATH) -> str:
    report = read_json(path, {}) if path else {}
    if not isinstance(report, dict) or not report:
        return ""
    cluster = (
        (report.get("clustered_move_summary") or {}).get("60m_gap")
        if isinstance(report.get("clustered_move_summary"), dict)
        else {}
    )
    if not isinstance(cluster, dict):
        cluster = {}
    observed = (
        (cluster.get("observed_abs_usd_one_per_cluster") or {}).get("sum")
        if isinstance(cluster.get("observed_abs_usd_one_per_cluster"), dict)
        else ""
    )
    first_30 = (
        (cluster.get("first_alert_fresh_30m_usd") or {}).get("sum")
        if isinstance(cluster.get("first_alert_fresh_30m_usd"), dict)
        else ""
    )
    first_60 = (
        (cluster.get("first_alert_fresh_60m_usd") or {}).get("sum")
        if isinstance(cluster.get("first_alert_fresh_60m_usd"), dict)
        else ""
    )
    mfe_60 = (
        (cluster.get("first_alert_mfe_60m_usd") or {}).get("sum")
        if isinstance(cluster.get("first_alert_mfe_60m_usd"), dict)
        else ""
    )
    gap_60 = ""
    first_60_num = maybe_float(first_60)
    mfe_60_num = maybe_float(mfe_60)
    if first_60_num is not None and mfe_60_num is not None:
        gap_60 = mfe_60_num - first_60_num
    pos_rate = (
        (cluster.get("first_alert_fresh_30m_pips") or {}).get("positive_rate")
        if isinstance(cluster.get("first_alert_fresh_30m_pips"), dict)
        else ""
    )
    pos_pct = ""
    pos_rate_num = maybe_float(pos_rate)
    if pos_rate_num is not None:
        pos_pct = format_num(pos_rate_num * 100.0, 0)
    return (
        "missed_spikes:"
        f"rows={report.get('candidate_count', '')} "
        f"clusters60={cluster.get('cluster_count', '')} "
        f"oracle60usd={format_num(observed, 2)} "
        f"first30usd={format_num(first_30, 2)} "
        f"first30pos={pos_pct}% "
        f"first60usd={format_num(first_60, 2)} "
        f"mfe60usd={format_num(mfe_60, 2)} "
        f"mfeGap60usd={format_num(gap_60, 2)}"
    ).strip()


def missed_spike_capture_gap_summary(path: Path | None = MISSED_SPIKE_CAPTURE_GAP_PATH) -> str:
    report = read_json(path, {}) if path else {}
    if not isinstance(report, dict) or not report:
        return ""
    aggregate = report.get("aggregate") if isinstance(report.get("aggregate"), dict) else {}
    top_positive = (
        report.get("top_positive_capture_groups")
        if isinstance(report.get("top_positive_capture_groups"), list)
        else []
    )
    top_gap = (
        report.get("top_capture_gap_groups")
        if isinstance(report.get("top_capture_gap_groups"), list)
        else []
    )
    leader = top_positive[0] if top_positive else (top_gap[0] if top_gap else {})
    leader = leader if isinstance(leader, dict) else {}
    parts = [
        "capture_gap:",
        f"rows={report.get('row_count', '')}",
        f"groups={report.get('group_count', '')}",
        f"aggBest=${format_num(aggregate.get('best_exit_usd_sum'), 2)}",
        f"aggMFE60=${format_num(aggregate.get('fresh_mfe_60m_usd_sum'), 2)}",
        f"aggGap=${format_num(aggregate.get('capture_gap_60m_usd_sum'), 2)}",
    ]
    group = str(leader.get("group") or "")
    key = str(leader.get("key") or "")
    if group or key:
        parts.append(f"top={group}:{key}"[:110])
    if leader:
        parts.extend(
            [
                f"best=${format_num(leader.get('best_exit_usd_sum'), 2)}",
                f"pos={format_num((maybe_float(leader.get('best_exit_pos_rate')) or 0.0) * 100.0, 0)}%",
                f"gap=${format_num(leader.get('capture_gap_60m_usd_sum'), 2)}",
            ]
        )
    return " ".join(part for part in parts if part)


def pre_spike_lead_backtest_summary(path: Path | None = PRE_SPIKE_LEAD_BACKTEST_PATH) -> str:
    report = read_json(path, {}) if path else {}
    if not isinstance(report, dict) or not report:
        return ""
    best = report.get("best") or {}
    if not isinstance(best, dict) or not best:
        return ""
    results = report.get("results") or report.get("leaderboard") or []
    ml_result_count = 0
    if isinstance(results, list):
        ml_result_count = sum(
            1
            for row in results
            if isinstance(row, dict)
            and str(row.get("model") or "").lower() != "rule_baseline"
        )
    selected = best.get("selected_summary") or {}
    if not isinstance(selected, dict):
        selected = {}
    risk = best.get("selected_risk_summary") or {}
    if not isinstance(risk, dict):
        risk = {}
    clustered = (risk.get("clustered_first") or {}) if isinstance(risk.get("clustered_first"), dict) else {}
    clustered_pips = (clustered.get("pips") or {}) if isinstance(clustered.get("pips"), dict) else {}
    pos_rate = maybe_float(selected.get("positive_rate"))
    pos_pct = format_num(pos_rate * 100.0, 0) if pos_rate is not None else ""
    cluster_pos = maybe_float(clustered_pips.get("positive_rate"))
    cluster_pos_pct = format_num(cluster_pos * 100.0, 0) if cluster_pos is not None else ""
    best_model = str(best.get("model") or "")
    label = (
        "lead_spikes_rule"
        if best_model.lower() == "rule_baseline"
        else "lead_spikes_ml"
    )
    text = (
        f"{label}:"
        f"model={best.get('model', '')} "
        f"lead={best.get('lead_minutes', '')}m "
        f"h={best.get('move_horizon', '')}m "
        f"folds={best.get('fold_count', '')} "
        f"auc={format_num(best.get('mean_event_auc'), 2)} "
        f"apLift={format_num(best.get('mean_event_ap_lift'), 1)} "
        f"dir={format_num((maybe_float(best.get('mean_direction_accuracy_on_events')) or 0.0) * 100.0, 0)}% "
        f"trades={selected.get('count', '')} "
        f"net={format_num(selected.get('sum'), 1)}p "
        f"mean={format_num(selected.get('mean'), 1)}p "
        f"pos={pos_pct}%"
    )
    if risk.get("cluster_count"):
        text += (
            f" clusters={risk.get('cluster_count', '')} "
            f"cmean={format_num(clustered_pips.get('mean'), 1)}p "
            f"cpos={cluster_pos_pct}%"
        )
    if label == "lead_spikes_rule" and ml_result_count == 0:
        text += " ml=none_yet"
    return text.strip()


def trainer_stopcap_summary(path: Path | None = TRAINER_REPORTING_EXTENSIONS_PATH) -> str:
    report = read_json(path, {}) if path else {}
    if not isinstance(report, dict) or not report:
        return ""
    stopcap = report.get("stopcap_comparison") or {}
    if not isinstance(stopcap, dict):
        return ""
    top = stopcap.get("top_avg_net") or []
    if not isinstance(top, list) or not top:
        return ""
    best = top[0] if isinstance(top[0], dict) else {}
    policy_counts = stopcap.get("policy_counts") or []
    count_bits = []
    if isinstance(policy_counts, list):
        for row in policy_counts:
            if isinstance(row, dict):
                key = str(row.get("key") or "").replace("trailing_stop", "s")
                count = row.get("count", "")
                if key and count != "":
                    count_bits.append(f"{key}:{count}")
    generated = report.get("generated_utc")
    age = utc_age_minutes(generated)
    age_part = f" age={age:.0f}m" if age is not None else ""
    return (
        "stopcap:"
        f"top={best.get('model_type', '')}/"
        f"{best.get('target', '')}/"
        f"{best.get('instrument_subset', '')}/"
        f"{best.get('policy', '')} "
        f"avg={format_num(best.get('avg_mean_net_pips'), 3)}p "
        f"auc={format_num(best.get('avg_mean_auc'), 3)} "
        f"runs={best.get('runs', '')} "
        f"gates={best.get('gate_passed_runs', '')} "
        f"counts={','.join(count_bits[:4])}"
        f"{age_part}"
    ).strip()


def trainer_promotion_leader_summary(path: Path = TRAINER_PROMOTION_READINESS_PATH) -> str:
    rows = read_csv_rows(path)
    if not rows:
        return ""
    successful = [
        row
        for row in rows
        if str(row.get("status") or "").lower() == "successful"
        and str(row.get("gate_passed") or "").lower() == "true"
    ]
    if not successful:
        return ""
    successful.sort(key=lambda row: maybe_float(row.get("readiness_score")) or -1.0, reverse=True)
    best = successful[0]
    return (
        "promotion_leader:"
        f"lane={best.get('deployment_lane', '')} "
        f"model={best.get('model_type', '')}/"
        f"{best.get('target', '')}/"
        f"{best.get('instrument_subset', '')} "
        f"score={format_num(best.get('readiness_score'), 1)} "
        f"auc={format_num(best.get('mean_auc'), 3)} "
        f"minAuc={format_num(best.get('minimum_week_auc'), 3)} "
        f"weeks={format_num(best.get('positive_weeks'), 0)}/"
        f"{format_num(best.get('weeks'), 0)} "
        f"trades={format_num(best.get('trades'), 0)} "
        f"pairs={format_num(best.get('unique_pairs'), 0)} "
        f"net={format_num(best.get('mean_net_pips'), 2)}p "
        f"pf={format_num(best.get('median_profit_factor'), 1)}"
    ).strip()


def live_value_miss_followup_summary(
    path: Path = TRAINER_EXPERIMENT_LEDGER_PATH,
    *,
    source: str | Iterable[str] = "operator_live_value_miss_followup_v1",
    label: str = "value_miss_followup",
    profile_contains: str = "",
) -> str:
    profile_filter = profile_contains.lower().strip()
    if isinstance(source, str):
        source_values = {source}
    else:
        source_values = {str(item) for item in source}
    rows = [
        row
        for row in read_csv_rows(path)
        if str(row.get("source") or "") in source_values
        and (
            not profile_filter
            or profile_filter in str(row.get("specialist_profile") or "").lower()
        )
    ]
    if not rows:
        return ""
    status_counts: dict[str, int] = {}
    gate_count = 0
    for row in rows:
        status = str(row.get("status") or "unknown").strip() or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        if str(row.get("gate_passed") or "").strip().lower() == "true":
            gate_count += 1
    promotion_ready = [
        row
        for row in rows
        if str(row.get("gate_passed") or "").strip().lower() == "true"
        and (maybe_float(row.get("fold_count")) or 0.0) >= 4.0
        and (maybe_float(row.get("trades")) or 0.0) >= 100.0
        and (maybe_float(row.get("mean_net_pips")) or -999.0) > 0.0
    ]
    promotion_ready.sort(
        key=lambda row: (
            maybe_float(row.get("fold_count")) or 0.0,
            maybe_float(row.get("minimum_week_auc")) or -1.0,
            maybe_float(row.get("mean_auc")) or -1.0,
            maybe_float(row.get("mean_net_pips")) or -999.0,
            maybe_float(row.get("score")) or -1.0,
        ),
        reverse=True,
    )
    lead_only_gate_count = max(0, gate_count - len(promotion_ready))
    status_bits = [
        f"{status}:{count}"
        for status, count in sorted(status_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
    ]

    scored_in_ledger_order = [
        row
        for row in rows
        if maybe_float(row.get("score")) is not None or maybe_float(row.get("mean_auc")) is not None
    ]
    scored = list(scored_in_ledger_order)
    if not scored:
        return (
            f"{label}:"
            f"rows={len(rows)} "
            f"gates={gate_count} "
            f"status={','.join(status_bits)}"
        ).strip()
    scored.sort(
        key=lambda row: (
            1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
            maybe_float(row.get("score")) or -1.0,
            maybe_float(row.get("mean_auc")) or -1.0,
        ),
        reverse=True,
    )
    best = scored[0]
    multi_fold = [
        row
        for row in scored
        if (maybe_float(row.get("fold_count")) or 0.0) >= 2.0
    ]
    multi_fold.sort(
        key=lambda row: (
            1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
            maybe_float(row.get("minimum_week_auc")) or -1.0,
            maybe_float(row.get("mean_auc")) or -1.0,
            maybe_float(row.get("score")) or -1.0,
        ),
        reverse=True,
    )
    currency_scored = [
        row
        for row in scored
        if "current_currency" in str(row.get("specialist_profile") or "")
    ]
    currency_scored.sort(
        key=lambda row: (
            1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
            maybe_float(row.get("fold_count")) or 0.0,
            maybe_float(row.get("mean_auc")) or -1.0,
            maybe_float(row.get("minimum_week_auc")) or -1.0,
            maybe_float(row.get("score")) or -1.0,
        ),
        reverse=True,
    )
    calibration_scored = [
        row
        for row in scored
        if "calibration" in str(row.get("specialist_profile") or "").lower()
    ]
    calibration_scored.sort(
        key=lambda row: (
            1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
            maybe_float(row.get("fold_count")) or 0.0,
            maybe_float(row.get("mean_auc")) or -1.0,
            maybe_float(row.get("minimum_week_auc")) or -1.0,
            maybe_float(row.get("mean_net_pips")) or -999.0,
            maybe_float(row.get("score")) or -1.0,
        ),
        reverse=True,
    )

    detail_result_cache: dict[str, dict[str, Any]] = {}

    def detail_result(row: dict[str, Any]) -> dict[str, Any]:
        detail_path = rel_path(row.get("detail_path"))
        if not detail_path or not detail_path.exists():
            return {}
        cache_key = str(detail_path)
        if cache_key in detail_result_cache:
            return detail_result_cache[cache_key]
        payload = read_json(detail_path, {}) or {}
        result = payload.get("result") if isinstance(payload, dict) else {}
        if not isinstance(result, dict):
            result = {}
        detail_result_cache[cache_key] = result
        return result

    def gate_failure_text(row: dict[str, Any]) -> str:
        result = detail_result(row)
        gate = result.get("gate") if isinstance(result, dict) else {}
        if not isinstance(gate, dict) or gate.get("passed") is True:
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
        return f" fail={','.join(failures[:4])}" if failures else ""

    def detail_metric(row: dict[str, Any], name: str) -> float | None:
        direct = maybe_float(row.get(name))
        if direct is not None:
            return direct
        return maybe_float(detail_result(row).get(name))

    def candidate_text(prefix: str, row: dict[str, Any]) -> str:
        whitelist = str(row.get("instrument_whitelist") or row.get("instrument_subset") or "").replace(",", "+")
        ap_lift = detail_metric(row, "mean_average_precision_lift")
        brier = detail_metric(row, "mean_brier_skill")
        extra_metrics = ""
        if ap_lift is not None:
            extra_metrics += f" ap={format_num(ap_lift, 2)}"
        if brier is not None:
            extra_metrics += f" brier={format_num(brier, 4)}"
        return (
            f"{prefix}={row.get('model_type', '')}/"
            f"{row.get('target', '')}/"
            f"{whitelist} "
            f"auc={format_num(row.get('mean_auc'), 3)} "
            f"minAuc={format_num(row.get('minimum_week_auc'), 3)} "
            f"{extra_metrics} "
            f"folds={format_num(row.get('fold_count'), 0)} "
            f"trades={format_num(row.get('trades'), 0)} "
            f"net={format_num(row.get('mean_net_pips'), 3)}p "
            f"score={format_num(row.get('score'), 1)} "
            f"gate={row.get('gate_passed', '')}"
            f"{gate_failure_text(row)}"
        )

    def parse_row_time(row: dict[str, Any]) -> datetime | None:
        try:
            raw = str(row.get("time_utc") or "").strip()
            if not raw:
                return None
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except Exception:
            return None

    latest_seed = read_json(LATEST_ACCOUNT_IMPROVEMENT_SEED_PATH, {}) or {}
    latest_seed_time: datetime | None = None
    if isinstance(latest_seed, dict):
        try:
            raw_seed_time = str(latest_seed.get("generated_utc") or "").strip()
            if raw_seed_time:
                latest_seed_time = datetime.fromisoformat(raw_seed_time.replace("Z", "+00:00"))
                if latest_seed_time.tzinfo is None:
                    latest_seed_time = latest_seed_time.replace(tzinfo=timezone.utc)
        except Exception:
            latest_seed_time = None
    if latest_seed_time is not None:
        latest_batch = [
            row
            for row in scored_in_ledger_order
            if (parse_row_time(row) is not None and parse_row_time(row) >= latest_seed_time)
        ]
    else:
        latest_batch = scored_in_ledger_order[-20:]
    latest_batch.sort(
        key=lambda row: (
            maybe_float(row.get("fold_count")) or 0.0,
            maybe_float(row.get("mean_auc")) or -1.0,
            maybe_float(row.get("minimum_week_auc")) or -1.0,
            maybe_float(row.get("mean_net_pips")) or -999.0,
            maybe_float(row.get("score")) or -1.0,
            maybe_float(row.get("trades")) or 0.0,
        ),
        reverse=True,
    )

    text = (
        f"{label}:"
        f"rows={len(rows)} "
        f"gates={gate_count} "
        f"promoReady={len(promotion_ready)} "
        f"leadOnly={lead_only_gate_count} "
        f"status={','.join(status_bits)} "
        f"{candidate_text('top', best)}"
    )
    if promotion_ready:
        text += f" {candidate_text('ready', promotion_ready[0])}"
    if multi_fold:
        text += f" {candidate_text('best2f', multi_fold[0])}"
    if currency_scored:
        text += f" {candidate_text('currency', currency_scored[0])}"
    if calibration_scored:
        text += f" {candidate_text('calibration', calibration_scored[0])}"
    if latest_batch:
        existing_ids = {
            str(row.get("spec_hash") or row.get("experiment_id") or "")
            for row in [
                best,
                multi_fold[0] if multi_fold else {},
                currency_scored[0] if currency_scored else {},
                calibration_scored[0] if calibration_scored else {},
            ]
        }
        latest_key = str(latest_batch[0].get("spec_hash") or latest_batch[0].get("experiment_id") or "")
        if latest_key not in existing_ids:
            text += f" latestN={len(latest_batch)} {candidate_text('latest', latest_batch[0])}"
    return text.strip()


def live_ev_near_miss_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_ev_near_miss_followup_v1",
        label="ev_near_miss_followup",
    )


def live_unknown_profile_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_unknown_profile_followup_v1",
        label="unknown_profile_followup",
    )


def live_missed_spike_subset_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_missed_spike_subset_followup_v1",
        label="missed_spike_subset_followup",
    )


def live_capture_gap_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    """Summarize trainer tests seeded specifically from missed-spike capture gaps."""
    def is_curve_spec(spec: dict[str, Any] | None, profile: str = "") -> bool:
        if not isinstance(spec, dict):
            return "curve" in profile.lower()
        return (
            "curve" in profile.lower()
            or "curve" in str(spec.get("outcome") or "").lower()
            or str(spec.get("execution_policy") or "").lower() == "curve"
            or "curve" in str(spec.get("validation_profile") or "").lower()
        )

    def is_curve_row(row: dict[str, Any]) -> bool:
        profile = str(row.get("specialist_profile") or "").lower()
        return (
            "curve" in profile
            or "curve" in str(row.get("outcome") or "").lower()
            or str(row.get("execution_policy") or "").lower() == "curve"
            or "curve" in str(row.get("validation_profile") or "").lower()
        )

    def pending_queue_counts() -> tuple[int, int, int]:
        state = read_json(TRAINER_RESEARCH_STATE_PATH, {}) or {}
        completed = {
            str(item)
            for item in state.get("completed_spec_hashes", [])
            if str(item).strip()
        } if isinstance(state, dict) else set()
        directional_fixed_pending = 0
        directional_curve_pending = 0
        generic_pending = 0
        for item in read_jsonl_rows(TRAINER_RESEARCH_QUEUE_PATH):
            spec = item.get("spec") if isinstance(item, dict) else None
            if not isinstance(spec, dict):
                continue
            spec_hash = str(item.get("spec_hash") or "").strip()
            if spec_hash and spec_hash in completed:
                continue
            profile = str(spec.get("specialist_profile") or "").lower()
            if "capture_gap" not in profile:
                continue
            if "directional_precursor" in profile:
                if is_curve_spec(spec, profile):
                    directional_curve_pending += 1
                else:
                    directional_fixed_pending += 1
            else:
                generic_pending += 1
        return directional_fixed_pending, directional_curve_pending, generic_pending

    queue_directional_fixed_pending, queue_directional_curve_pending, queue_generic_pending = pending_queue_counts()
    rows = [
        row
        for row in read_csv_rows(path)
        if str(row.get("source") or "") == "operator_live_missed_spike_subset_followup_v1"
        and "capture_gap" in str(row.get("specialist_profile") or "").lower()
    ]
    if not rows:
        if (
            not queue_directional_fixed_pending
            and not queue_directional_curve_pending
            and not queue_generic_pending
        ):
            return ""
        return (
            "capture_gap_followup:"
            f" rows=0 dir=0 generic=0 gates=0"
            f" qDir={queue_directional_fixed_pending + queue_directional_curve_pending}"
            f" qFixed={queue_directional_fixed_pending}"
            f" qCurve={queue_directional_curve_pending}"
            f" qGen={queue_generic_pending}"
        ).strip()

    directional_rows = [
        row
        for row in rows
        if "directional_precursor" in str(row.get("specialist_profile") or "").lower()
    ]
    directional_curve_rows = [row for row in directional_rows if is_curve_row(row)]
    directional_fixed_rows = [row for row in directional_rows if not is_curve_row(row)]
    generic_rows = [row for row in rows if row not in directional_rows]
    gate_count = sum(
        1
        for row in rows
        if str(row.get("gate_passed") or "").strip().lower() == "true"
    )

    def status_bits(candidates: list[dict[str, Any]]) -> str:
        if not candidates:
            return "none"
        counts: Counter[str] = Counter(
            str(row.get("status") or "unknown").strip() or "unknown"
            for row in candidates
        )
        return ",".join(
            f"{status}:{count}"
            for status, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:3]
        )

    def best_text(prefix: str, candidates: list[dict[str, Any]]) -> str:
        scored = [
            row
            for row in candidates
            if maybe_float(row.get("score")) is not None
            or maybe_float(row.get("mean_auc")) is not None
        ]
        if not scored:
            return ""
        scored.sort(
            key=lambda row: (
                1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
                maybe_float(row.get("score")) or -1.0,
                maybe_float(row.get("mean_auc")) or -1.0,
                maybe_float(row.get("minimum_week_auc")) or -1.0,
            ),
            reverse=True,
        )
        best = scored[0]
        whitelist = str(best.get("instrument_whitelist") or best.get("instrument_subset") or "")
        whitelist = whitelist.replace(",", "+")
        if len(whitelist) > 70:
            whitelist = whitelist[:67] + "..."
        return (
            f"{prefix}={best.get('model_type', '')}/"
            f"{best.get('target', '')}/"
            f"{whitelist} "
            f"auc={format_num(best.get('mean_auc'), 3)} "
            f"minAuc={format_num(best.get('minimum_week_auc'), 3)} "
            f"folds={format_num(best.get('fold_count'), 0)} "
            f"trades={format_num(best.get('trades'), 0)} "
            f"net={format_num(best.get('mean_net_pips'), 3)}p "
            f"score={format_num(best.get('score'), 1)} "
            f"gate={best.get('gate_passed', '')}"
        ).strip()

    parts = [
        "capture_gap_followup:",
        f"rows={len(rows)}",
        f"dir={len(directional_rows)}",
        f"dirFixed={len(directional_fixed_rows)}",
        f"dirCurve={len(directional_curve_rows)}",
        f"generic={len(generic_rows)}",
        f"gates={gate_count}",
        f"qDir={queue_directional_fixed_pending + queue_directional_curve_pending}",
        f"qFixed={queue_directional_fixed_pending}",
        f"qCurve={queue_directional_curve_pending}",
        f"qGen={queue_generic_pending}",
        f"dirStatus={status_bits(directional_rows)}",
        f"genStatus={status_bits(generic_rows)}",
    ]
    dir_best = best_text("dirBest", directional_rows)
    dir_fixed_best = best_text("dirFixedBest", directional_fixed_rows)
    dir_curve_best = best_text("dirCurveBest", directional_curve_rows)
    generic_best = best_text("genBest", generic_rows)
    if dir_best:
        parts.append(dir_best)
    if dir_fixed_best:
        parts.append(dir_fixed_best)
    if dir_curve_best:
        parts.append(dir_curve_best)
    if generic_best:
        parts.append(generic_best)
    return " ".join(parts).strip()


def live_localized_cluster_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_missed_spike_subset_followup_v1",
        label="localized_cluster_followup",
        profile_contains="localized_cluster",
    )


def live_localized_directional_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_missed_spike_subset_followup_v1",
        label="localized_directional_followup",
        profile_contains="directional_precursor",
    )


def live_near_value_gate_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_value_miss_followup_v1",
        label="near_value_gate_followup",
        profile_contains="current_near_value_gate",
    )


def live_scan_cost_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_value_miss_followup_v1",
        label="scan_cost_followup",
        profile_contains="current_scan_cost",
    )


def live_segment_focus_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source="operator_live_value_miss_followup_v1",
        label="segment_focus_followup",
        profile_contains="calibration_detection_segment_focus",
    )


def live_lead_only_robust_followup_summary(path: Path = TRAINER_EXPERIMENT_LEDGER_PATH) -> str:
    return live_value_miss_followup_summary(
        path,
        source=[
            "operator_live_value_miss_followup_v1",
            "operator_live_unknown_profile_followup_v1",
            "operator_live_missed_spike_subset_followup_v1",
            "operator_live_ev_near_miss_followup_v1",
        ],
        label="lead_only_robust_followup",
        profile_contains="lead_only_robust",
    )


def live_miss_queue_source_summary(
    queue_path: Path = TRAINER_RESEARCH_QUEUE_PATH,
    ledger_path: Path = TRAINER_EXPERIMENT_LEDGER_PATH,
) -> str:
    sources = [
        "operator_live_value_miss_followup_v1",
        "operator_live_ev_near_miss_followup_v1",
        "operator_live_unknown_profile_followup_v1",
        "operator_live_missed_spike_subset_followup_v1",
    ]
    labels = {
        "operator_live_value_miss_followup_v1": "value",
        "operator_live_ev_near_miss_followup_v1": "ev_near",
        "operator_live_unknown_profile_followup_v1": "unknown",
        "operator_live_missed_spike_subset_followup_v1": "subset",
    }
    queue_rows = read_jsonl_rows(queue_path)
    if not queue_rows:
        return ""
    ledger_rows = read_csv_rows(ledger_path)
    completed_hashes = {str(row.get("spec_hash") or "") for row in ledger_rows if row.get("spec_hash")}
    source_stats: dict[str, dict[str, Any]] = {
        source: {
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
            "contexts": {},
        }
        for source in sources
    }
    for item in queue_rows:
        spec = item.get("spec") if isinstance(item, dict) else {}
        if not isinstance(spec, dict):
            continue
        source = str(spec.get("source") or "")
        if source not in source_stats:
            continue
        stats = source_stats[source]
        stats["queued"] += 1
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
            stats["calibration_queued"] += 1
        if is_directional_precursor:
            stats["directional_precursor_queued"] += 1
        if is_scan_cost:
            stats["scan_cost_queued"] += 1
        if is_segment_focus:
            stats["segment_focus_queued"] += 1
        if is_lead_only_robust:
            stats["lead_only_robust_queued"] += 1
        if spec_hash and spec_hash in completed_hashes:
            stats["completed"] += 1
            if is_calibration:
                stats["calibration_completed"] += 1
            if is_directional_precursor:
                stats["directional_precursor_completed"] += 1
            if is_scan_cost:
                stats["scan_cost_completed"] += 1
            if is_segment_focus:
                stats["segment_focus_completed"] += 1
            if is_lead_only_robust:
                stats["lead_only_robust_completed"] += 1
        else:
            stats["pending"] += 1
            if is_calibration:
                stats["calibration_pending"] += 1
            if is_directional_precursor:
                stats["directional_precursor_pending"] += 1
            if is_scan_cost:
                stats["scan_cost_pending"] += 1
            if is_segment_focus:
                stats["segment_focus_pending"] += 1
            if is_lead_only_robust:
                stats["lead_only_robust_pending"] += 1
        model = str(spec.get("model_type") or "")
        target = str(spec.get("target") or "")
        if model:
            stats["models"].add(model)
        if target:
            stats["targets"].add(target)
        context = spec.get("subset_validation_context") or {}
        if isinstance(context, dict):
            filter_label = str(context.get("filter_label") or "").strip()
            if filter_label:
                contexts = stats.setdefault("contexts", {})
                contexts[filter_label] = int(contexts.get(filter_label, 0)) + 1
        whitelist = spec.get("instrument_whitelist") or []
        if isinstance(whitelist, list):
            for instrument in whitelist[:8]:
                if instrument:
                    stats["pairs"].add(str(instrument))
    bits: list[str] = []
    for source in sources:
        stats = source_stats[source]
        if not stats.get("queued"):
            continue
        pairs = ",".join(sorted(stats["pairs"])[:4])
        pair_part = f" pairs={pairs}" if pairs else ""
        calibration_part = ""
        if stats.get("calibration_queued"):
            calibration_part = (
                f" calib={format_num(stats.get('calibration_pending'), 0)}/"
                f"{format_num(stats.get('calibration_queued'), 0)}"
            )
        directional_part = ""
        if stats.get("directional_precursor_queued"):
            directional_part = (
                f" dirPrec={format_num(stats.get('directional_precursor_pending'), 0)}/"
                f"{format_num(stats.get('directional_precursor_queued'), 0)}"
            )
        scan_cost_part = ""
        if stats.get("scan_cost_queued"):
            scan_cost_part = (
                f" scanCost={format_num(stats.get('scan_cost_pending'), 0)}/"
                f"{format_num(stats.get('scan_cost_queued'), 0)}"
            )
        segment_focus_part = ""
        if stats.get("segment_focus_queued"):
            segment_focus_part = (
                f" segFocus={format_num(stats.get('segment_focus_pending'), 0)}/"
                f"{format_num(stats.get('segment_focus_queued'), 0)}"
            )
        robust_part = ""
        if stats.get("lead_only_robust_queued"):
            robust_part = (
                f" robust={format_num(stats.get('lead_only_robust_pending'), 0)}/"
                f"{format_num(stats.get('lead_only_robust_queued'), 0)}"
            )
        context_part = ""
        contexts = stats.get("contexts") or {}
        if isinstance(contexts, dict) and contexts:
            context_bits = [
                f"{label}:{format_num(count, 0)}"
                for label, count in sorted(contexts.items())
            ]
            context_part = f" ctx={','.join(context_bits[:5])}"
        bits.append(
            f"{labels[source]}:queued={format_num(stats.get('queued'), 0)} "
            f"pending={format_num(stats.get('pending'), 0)} "
            f"done={format_num(stats.get('completed'), 0)}"
            f"{calibration_part}"
            f"{directional_part}"
            f"{scan_cost_part}"
            f"{segment_focus_part}"
            f"{robust_part}"
            f"{context_part}"
            f"{pair_part}"
        )
    if not bits:
        return ""
    return "live_miss_queue:" + " ".join(bits)


def pre_spike_fold_leader_summary(path: Path = PRE_SPIKE_LEAD_FOLDS_PATH) -> str:
    rows = read_csv_rows(path)
    if not rows:
        return ""
    groups: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("model") or ""),
            str(row.get("mode") or ""),
            str(row.get("lead_minutes") or ""),
            str(row.get("move_horizon") or ""),
            str(row.get("execution_horizon") or ""),
            str(row.get("lead_stop_atr") or ""),
        )
        if not any(key):
            continue
        group = groups.setdefault(
            key,
            {
                "folds": 0,
                "trades": 0.0,
                "net_pips": 0.0,
                "auc_sum": 0.0,
                "auc_count": 0,
                "min_auc": None,
                "ap_lift_sum": 0.0,
                "ap_lift_count": 0,
                "pf_sum": 0.0,
                "pf_count": 0,
                "positive_folds": 0,
                "min_selected_count": None,
            },
        )
        group["folds"] += 1
        selected_count = maybe_float(row.get("selected_count")) or 0.0
        selected_sum = maybe_float(row.get("selected_sum")) or 0.0
        group["trades"] += selected_count
        group["net_pips"] += selected_sum
        if selected_sum > 0:
            group["positive_folds"] += 1
        if group["min_selected_count"] is None:
            group["min_selected_count"] = selected_count
        else:
            group["min_selected_count"] = min(
                maybe_float(group.get("min_selected_count")) or 0.0,
                selected_count,
            )
        auc = maybe_float(row.get("event_auc"))
        if auc is not None:
            group["auc_sum"] += auc
            group["auc_count"] += 1
            if group["min_auc"] is None:
                group["min_auc"] = auc
            else:
                group["min_auc"] = min(
                    maybe_float(group.get("min_auc")) or auc,
                    auc,
                )
        ap_lift = maybe_float(row.get("event_ap_lift"))
        if ap_lift is not None:
            group["ap_lift_sum"] += ap_lift
            group["ap_lift_count"] += 1
        pf = maybe_float(row.get("selected_profit_factor"))
        if pf is not None:
            group["pf_sum"] += pf
            group["pf_count"] += 1
    if not groups:
        return ""

    def group_summary(
        label: str,
        key: tuple[str, str, str, str, str, str],
        best: dict[str, Any],
    ) -> str:
        model, mode, lead, move, execution, stop = key
        trades = maybe_float(best.get("trades")) or 0.0
        mean_pips = (maybe_float(best.get("net_pips")) or 0.0) / trades if trades else 0.0
        mean_auc = best["auc_sum"] / best["auc_count"] if best["auc_count"] else None
        mean_ap_lift = best["ap_lift_sum"] / best["ap_lift_count"] if best["ap_lift_count"] else None
        mean_pf = best["pf_sum"] / best["pf_count"] if best["pf_count"] else None
        positive_folds = maybe_float(best.get("positive_folds")) or 0.0
        folds = maybe_float(best.get("folds")) or 0.0
        return (
            f"{label}:"
            f"model={model}/{mode} "
            f"lead={lead}m "
            f"move={move}m "
            f"exec={execution}m "
            f"stop={stop}atr "
            f"folds={best.get('folds', '')} "
            f"posFolds={format_num(positive_folds, 0)}/{format_num(folds, 0)} "
            f"trades={format_num(trades, 0)} "
            f"minTrades={format_num(best.get('min_selected_count'), 0)} "
            f"net={format_num(best.get('net_pips'), 1)}p "
            f"mean={format_num(mean_pips, 2)}p "
            f"auc={format_num(mean_auc, 3)} "
            f"minAuc={format_num(best.get('min_auc'), 3)} "
            f"apLift={format_num(mean_ap_lift, 1)} "
            f"pf={format_num(mean_pf, 2)}"
        ).strip()

    rule_items = [
        (key, value)
        for key, value in groups.items()
        if str(key[0]).lower() == "rule_baseline"
    ]
    ml_items = [
        (key, value)
        for key, value in groups.items()
        if str(key[0]).lower() != "rule_baseline"
    ]
    parts: list[str] = []
    if rule_items:
        rule_key, rule_best = max(rule_items, key=lambda item: item[1].get("net_pips", 0.0))
        parts.append(group_summary("pre_spike_rule_leader", rule_key, rule_best))
    if ml_items:
        ml_key, ml_best = max(ml_items, key=lambda item: item[1].get("net_pips", 0.0))
        parts.append(group_summary("pre_spike_ml_leader", ml_key, ml_best))
        robust_ml_items = [
            (key, value)
            for key, value in ml_items
            if (maybe_float(value.get("folds")) or 0.0) >= 4.0
            and (maybe_float(value.get("positive_folds")) or 0.0)
            >= max(3.0, (maybe_float(value.get("folds")) or 0.0) - 1.0)
            and (maybe_float(value.get("min_auc")) or 0.0) >= 0.52
            and (maybe_float(value.get("min_selected_count")) or 0.0) >= 30.0
            and (maybe_float(value.get("net_pips")) or 0.0) > 0.0
        ]
        if robust_ml_items:
            robust_key, robust_best = max(
                robust_ml_items,
                key=lambda item: item[1].get("net_pips", 0.0),
            )
            parts.append(
                group_summary("pre_spike_ml_robust", robust_key, robust_best)
            )
        else:
            parts.append("pre_spike_ml_robust:none_yet")
    else:
        parts.append("pre_spike_ml_leader:none_yet")
    return " ".join(parts)


def pre_spike_ml_fast_process_summary(processes: list[dict[str, Any]]) -> str:
    matches = [
        row
        for row in processes
        if "oanda_pre_spike_lead_backtest.py" in str(row.get("command") or "")
        and "latest_pre_spike_lead_ml_fast" in str(row.get("command") or "")
    ]
    if not matches:
        return ""
    pids = ",".join(str(row.get("pid") or "") for row in matches if row.get("pid"))
    artifact_state = "artifact_pending"
    if PRE_SPIKE_LEAD_ML_FAST_FOLDS_PATH.exists():
        artifact_state = "folds_ready"
    report_path = (
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "latest_pre_spike_lead_ml_fast.json"
    )
    report_state = "report_ready" if report_path.exists() else "report_pending"
    progress = ""
    report = read_json(report_path, {}) if report_path.exists() else {}
    if isinstance(report, dict) and report:
        completed = report.get("completed_combos", "")
        total = report.get("total_combos", "")
        status = str(report.get("status") or "")
        generated_age = utc_age_minutes(report.get("generated_utc"))
        age_text = (
            f" age={format_num(generated_age, 0)}m"
            if generated_age is not None
            else ""
        )
        progress = (
            f" status={status} progress={completed}/{total}"
            f" results={len(report.get('results') or [])}{age_text}"
        )
    return (
        "pre_spike_ml_fast_process:"
        f"running=yes pids={pids} {artifact_state} {report_state}{progress}"
    )


def pre_spike_trainer_focus_process_summary(processes: list[dict[str, Any]]) -> str:
    matches = [
        row
        for row in processes
        if "oanda_pre_spike_lead_backtest.py" in str(row.get("command") or "")
        and "latest_pre_spike_lead_trainer_focus" in str(row.get("command") or "")
    ]
    if not matches:
        return ""
    pids = ",".join(str(row.get("pid") or "") for row in matches if row.get("pid"))
    artifact_state = (
        "folds_ready"
        if PRE_SPIKE_LEAD_TRAINER_FOCUS_FOLDS_PATH.exists()
        else "artifact_pending"
    )
    report_state = (
        "report_ready"
        if PRE_SPIKE_LEAD_TRAINER_FOCUS_PATH.exists()
        else "report_pending"
    )
    progress = ""
    report = (
        read_json(PRE_SPIKE_LEAD_TRAINER_FOCUS_PATH, {})
        if PRE_SPIKE_LEAD_TRAINER_FOCUS_PATH.exists()
        else {}
    )
    if isinstance(report, dict) and report:
        completed = report.get("completed_combos", "")
        total = report.get("total_combos", "")
        status = str(report.get("status") or "")
        generated_age = utc_age_minutes(report.get("generated_utc"))
        age_text = ""
        stale_prefix = ""
        if generated_age is not None:
            age_text = f" last_report_age={format_num(generated_age, 0)}m"
            # The trainer can launch a new focus refresh while the previous
            # completed report remains on disk.  Label that state explicitly so
            # advisor output does not read as if the active process already
            # completed.
            if generated_age > 30:
                stale_prefix = " active_refresh=yes"
        progress = (
            f"{stale_prefix} last_report_status={status} last_report_progress={completed}/{total}"
            f" results={len(report.get('results') or [])}{age_text}"
        )
    return (
        "pre_spike_trainer_focus_process:"
        f"running=yes pids={pids} {artifact_state} {report_state}{progress}"
    )


def pre_spike_default_output_process_summary(processes: list[dict[str, Any]]) -> str:
    """Warn when a pre-spike research run is using shared default artifacts."""
    matches = []
    for row in processes:
        command = str(row.get("command") or "")
        if "oanda_pre_spike_lead_backtest.py" not in command:
            continue
        if "--report" in command or "--folds-csv" in command:
            continue
        matches.append(row)
    if not matches:
        return ""
    pids = ",".join(str(row.get("pid") or "") for row in matches if row.get("pid"))
    return (
        "pre_spike_default_output_warning:"
        f"running=yes pids={pids} "
        "writes=latest_pre_spike_lead_backtest.* "
        "action=future_trainer_refreshes_use_trainer_focus_artifacts"
    )


def pressure_gate_validation_summary() -> str:
    parts: list[str] = []
    for label, path, row_key in PRESSURE_VALIDATION_REPORTS:
        payload = read_json(path, {})
        if not isinstance(payload, dict) or not payload:
            continue
        aggregate = payload.get("aggregate") or {}
        if not isinstance(aggregate, dict) or not aggregate:
            continue
        row = aggregate.get(row_key) or {}
        if not isinstance(row, dict) or not row:
            continue
        days = maybe_float(aggregate.get("days")) or 0.0
        episodes = maybe_float(row.get("episodes"))
        if episodes is None:
            continue
        episodes = episodes or 0.0
        first_usd = maybe_float(row.get("sum_first_fixed_usd_60m")) or 0.0
        best_usd = maybe_float(row.get("sum_best_fixed_usd_60m")) or 0.0
        positive_days = maybe_float(row.get("positive_days_first_fixed_usd")) or 0.0
        fixed_precision = maybe_float(row.get("precision_value_fixed"))
        favorable_precision = maybe_float(row.get("precision_value_favorable"))
        sign = "+" if first_usd >= 0 else ""
        parts.append(
            f"{label}={sign}${first_usd:.2f}/60m "
            f"best=${best_usd:.2f} "
            f"days={format_num(positive_days, 0)}/{format_num(days, 0)} "
            f"eps={format_num(episodes, 0)} "
            f"prec={format_num((fixed_precision or 0.0) * 100.0, 1)}% "
            f"fav={format_num((favorable_precision or 0.0) * 100.0, 1)}%"
        )
    if not parts:
        return ""
    return "pressure_gate:" + " ".join(parts)


def recent_rows_by_age(rows: list[dict[str, Any]], minutes: float, time_field: str = "time_utc") -> list[dict[str, Any]]:
    recent: list[dict[str, Any]] = []
    for row in rows:
        age = utc_age_minutes(row.get(time_field))
        if age is None or age > minutes:
            continue
        recent.append(row)
    return recent


def short_text(value: Any, max_len: int = 70) -> str:
    text = str(value or "").strip().replace("\n", " ")
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 3)] + "..."


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


def reconstructed_localized_cluster_opportunities(
    audit_rows: list[dict[str, Any]],
    *,
    min_cluster: int = 4,
    min_direction_cluster: int = 4,
    min_ratio: float = 1.35,
    min_score: float = 80.0,
) -> dict[str, Any]:
    """Reconstruct localized EM/high-spread basket opportunities from audit rows.

    This is read-only and intentionally approximate: it groups rows by the
    scan timestamp second, then asks whether a strict-gate rejection was part
    of a same-direction localized currency basket. It lets us quantify the
    opportunity before loosening any live gate.
    """
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
                    currency_direction_instruments.setdefault(
                        (currency, direction),
                        set(),
                    ).add(instrument)
        for row in rows:
            if str(row.get("decision_stage") or "").strip() != "strict_pressure_gate":
                continue
            if str(row.get("status") or "").strip().lower() not in {
                "skipped",
                "rejected",
                "blocked",
            }:
                continue
            reason = str(row.get("reject_reason") or row.get("reason") or "")
            if "cluster instruments" not in reason:
                continue
            instrument = str(row.get("instrument") or "").upper().replace("/", "_")
            direction = str(row.get("direction") or "").upper()
            if "_" not in instrument or direction not in {"LONG", "SHORT"}:
                continue
            ratio = maybe_float(row.get("move_to_spread_ratio")) or 0.0
            score = maybe_float(row.get("pressure_score")) or 0.0
            if ratio < min_ratio or score < min_score:
                continue
            base, quote = instrument.split("_", 1)
            best_currency = ""
            best_cluster = 0
            best_direction_cluster = 0
            for currency in {base, quote} & LOCALIZED_CLUSTER_CURRENCIES:
                cluster_count = len(currency_instruments.get(currency, set()))
                direction_count = len(
                    currency_direction_instruments.get(
                        (currency, direction),
                        set(),
                    )
                )
                if (direction_count, cluster_count) > (
                    best_direction_cluster,
                    best_cluster,
                ):
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
                    "net_pips": maybe_float(row.get("net_pips")) or 0.0,
                    "ratio": ratio,
                    "score": score,
                    "reason": reason,
                })
    if not candidates:
        return {"count": 0}
    currency_counts = Counter(str(row["currency"]) for row in candidates)
    instrument_counts = Counter(str(row["instrument"]) for row in candidates)
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
        "currencies": currency_counts.most_common(3),
        "instruments": instrument_counts.most_common(3),
    }


def summarize_scout_near_passes(
    rows: list[dict[str, Any]],
    *,
    min_value_gate_ratio: float = 0.75,
    min_score_ratio: float = 0.85,
    max_score_gap: float = 10.0,
    min_spread_ratio: float = 0.65,
) -> dict[str, Any]:
    """Summarize skipped scout rows that were close to live execution gates."""
    value_buckets: dict[str, dict[str, Any]] = {}
    score_buckets: dict[str, dict[str, Any]] = {}
    spread_buckets: dict[str, dict[str, Any]] = {}

    for row in rows:
        if str(row.get("status") or "").strip().lower() not in {
            "skipped",
            "rejected",
            "blocked",
        }:
            continue
        text = str(row.get("reject_reason") or row.get("reason") or "")
        if not text:
            continue
        instrument = str(row.get("instrument") or "").upper().replace("/", "_") or "UNKNOWN"
        direction = str(row.get("direction") or "").upper()

        value_gate = extract_value_gate_pair(text)
        if value_gate:
            expected, threshold = value_gate
            if threshold > 0.0 and expected > 0.0:
                ratio = expected / threshold
                if ratio >= min_value_gate_ratio:
                    bucket = value_buckets.setdefault(
                        instrument,
                        {
                            "instrument": instrument,
                            "direction": direction,
                            "count": 0,
                            "expected_usd": 0.0,
                            "max_ratio": 0.0,
                        },
                    )
                    bucket["count"] = int(bucket.get("count") or 0) + 1
                    bucket["expected_usd"] = float(bucket.get("expected_usd") or 0.0) + expected
                    bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), ratio)

        score_match = re.search(
            r"score\s+too\s+low\s+(-?\d+(?:\.\d+)?)\s*<\s*(-?\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )
        if score_match:
            score = maybe_float(score_match.group(1))
            threshold = maybe_float(score_match.group(2))
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
            observed = maybe_float(spread_match.group(1))
            threshold = maybe_float(spread_match.group(2))
            if observed is not None and threshold is not None and threshold > 0.0:
                ratio = observed / threshold
                if ratio >= min_spread_ratio:
                    bucket = spread_buckets.setdefault(
                        instrument,
                        {
                            "instrument": instrument,
                            "direction": direction,
                            "count": 0,
                            "max_observed": 0.0,
                            "threshold": threshold,
                            "max_ratio": 0.0,
                        },
                    )
                    bucket["count"] = int(bucket.get("count") or 0) + 1
                    bucket["max_observed"] = max(float(bucket.get("max_observed") or 0.0), observed)
                    bucket["threshold"] = max(float(bucket.get("threshold") or 0.0), threshold)
                    bucket["max_ratio"] = max(float(bucket.get("max_ratio") or 0.0), ratio)

    return {
        "value_gate_count": sum(int(row.get("count") or 0) for row in value_buckets.values()),
        "value_gate_expected_usd": round(
            sum(float(row.get("expected_usd") or 0.0) for row in value_buckets.values()),
            6,
        ),
        "top_value_gates": sorted(
            value_buckets.values(),
            key=lambda item: (
                -(float(item.get("expected_usd") or 0.0)),
                -float(item.get("max_ratio") or 0.0),
                str(item.get("instrument") or ""),
            ),
        )[:3],
        "score_near_count": sum(int(row.get("count") or 0) for row in score_buckets.values()),
        "top_score_near": sorted(
            score_buckets.values(),
            key=lambda item: (
                -float(item.get("max_ratio") or 0.0),
                float(item.get("min_gap") if item.get("min_gap") is not None else 999999.0),
                -int(item.get("count") or 0),
                str(item.get("instrument") or ""),
            ),
        )[:3],
        "spread_ratio_near_count": sum(int(row.get("count") or 0) for row in spread_buckets.values()),
        "top_spread_ratio_near": sorted(
            spread_buckets.values(),
            key=lambda item: (
                -float(item.get("max_ratio") or 0.0),
                -int(item.get("count") or 0),
                str(item.get("instrument") or ""),
            ),
        )[:3],
    }


def scout_live_rollout_summary(
    label: str,
    root: Path,
    lookback_minutes: float = 120.0,
) -> str:
    """Summarize a current scout rollout from recent local journals.

    This is intentionally read-only and uses bounded tail reads.  It helps
    distinguish "watching many candidates" from "actually firing orders" after
    gate/config changes.
    """
    # Scan rows are low volume, but scout audit rows can be very high volume:
    # one scan may log hundreds of skipped candidate/theme combinations.  A
    # 500-row tail can cover only a few minutes and miss accepted/passed rows in
    # the requested 120m advisor window, so keep this bounded but deeper.
    scan_rows = recent_rows_by_age(
        read_csv_tail_rows(root / "event_scan_summary.csv", max_rows=1000),
        lookback_minutes,
    )
    audit_rows = recent_rows_by_age(
        read_csv_tail_rows(root / "scout_audit_ledger.csv", max_rows=10000),
        lookback_minutes,
    )
    if not scan_rows and not audit_rows:
        return ""

    pressure_watch = sum(int(maybe_float(row.get("pressure_watch")) or 0) for row in scan_rows)
    pressure_trade = sum(int(maybe_float(row.get("pressure_trade")) or 0) for row in scan_rows)
    attempts = sum(int(maybe_float(row.get("scout_attempts")) or 0) for row in scan_rows)
    latest_scan = scan_rows[-1] if scan_rows else {}
    latest_age = utc_age_minutes(latest_scan.get("time_utc")) if latest_scan else None
    top_candidate = short_text(latest_scan.get("top_candidate"), 45) if latest_scan else ""

    status_counts = Counter(str(row.get("status") or "unknown").strip() or "unknown" for row in audit_rows)
    reject_counts = Counter(
        short_text(row.get("reject_reason") or row.get("reason") or "no_reason", 55)
        for row in audit_rows
        if str(row.get("status") or "").strip().lower() in {"skipped", "rejected", "blocked"}
    )
    accepted_count = sum(
        1
        for row in audit_rows
        if str(row.get("status") or "").strip().lower() in {"accepted", "placed", "submitted", "filled"}
    )
    localized_shadow_count = sum(
        1
        for row in audit_rows
        if str(row.get("decision_stage") or "").strip().lower()
        == "localized_cluster_shadow"
        and str(row.get("status") or "").strip().lower() == "shadow"
    )
    max_score = max((maybe_float(row.get("pressure_score")) or 0.0 for row in audit_rows), default=0.0)
    max_cluster = max((maybe_float(row.get("pressure_cluster_instruments")) or 0.0 for row in audit_rows), default=0.0)
    max_dir_cluster = max(
        (maybe_float(row.get("pressure_cluster_direction_instruments")) or 0.0 for row in audit_rows),
        default=0.0,
    )
    strict_reject_rows = [
        row
        for row in audit_rows
        if str(row.get("decision_stage") or "").strip() == "strict_pressure_gate"
        and str(row.get("status") or "").strip().lower() in {"skipped", "rejected", "blocked"}
    ]
    strict_near_count = 0
    for row in strict_reject_rows:
        reason_text = str(row.get("reject_reason") or row.get("reason") or "")
        match = re.search(r"(?:direction\s+)?cluster instruments\s+(\d+(?:\.\d+)?)\s*<\s*(\d+(?:\.\d+)?)", reason_text)
        if not match:
            continue
        observed = maybe_float(match.group(1))
        required = maybe_float(match.group(2))
        if observed is not None and required is not None and 0.0 <= required - observed <= 2.0:
            strict_near_count += 1
    strict_max_cluster = max(
        (maybe_float(row.get("pressure_cluster_instruments")) or 0.0 for row in strict_reject_rows),
        default=0.0,
    )
    strict_max_dir_cluster = max(
        (maybe_float(row.get("pressure_cluster_direction_instruments")) or 0.0 for row in strict_reject_rows),
        default=0.0,
    )
    localized_opportunities = reconstructed_localized_cluster_opportunities(
        audit_rows,
    )
    near_pass = summarize_scout_near_passes(audit_rows)
    parts = [
        f"{label}[{format_num(lookback_minutes, 0)}m]:",
        f"scans={len(scan_rows)}",
        f"watch={pressure_watch}",
        f"trade={pressure_trade}",
        f"attempts={attempts}",
    ]
    if latest_age is not None:
        parts.append(f"latest_age={format_num(latest_age, 0)}m")
    if top_candidate:
        parts.append(f"top={top_candidate}")
    if audit_rows:
        status_part = ",".join(f"{key}:{count}" for key, count in status_counts.most_common(3))
        reject_part = ";".join(f"{key}:{count}" for key, count in reject_counts.most_common(2))
        parts.extend(
            [
                f"audit={len(audit_rows)}",
                f"status={status_part}",
                f"accepted={accepted_count}",
                f"max_score={format_num(max_score, 1)}",
                f"max_cluster={format_num(max_cluster, 0)}/{format_num(max_dir_cluster, 0)}",
            ]
        )
        if localized_shadow_count:
            parts.append(f"localizedShadow={localized_shadow_count}")
        if localized_opportunities.get("count"):
            top_local = localized_opportunities.get("top") or {}
            currency_bits = [
                f"{currency}:{count}"
                for currency, count in localized_opportunities.get("currencies", [])[:2]
            ]
            parts.append(
                "localizedOpp="
                f"{localized_opportunities.get('count')} "
                f"top={top_local.get('instrument', '')} "
                f"{top_local.get('direction', '')} "
                f"{format_num(top_local.get('net_pips'), 1)}p "
                f"{top_local.get('currency', '')}:"
                f"{top_local.get('cluster', '')}/"
                f"{top_local.get('direction_cluster', '')}"
            )
            if currency_bits:
                parts.append(f"locCur={';'.join(currency_bits)}")
        near_bits: list[str] = []
        if int(near_pass.get("value_gate_count") or 0):
            top_value = near_pass.get("top_value_gates") or []
            suffix = ""
            if top_value:
                top = top_value[0]
                suffix = (
                    f" topValue={top.get('instrument', '')}"
                    f" {format_num(top.get('max_ratio'), 2)}x"
                )
            near_bits.append(
                f"value=${format_num(near_pass.get('value_gate_expected_usd'), 3)}/"
                f"{near_pass.get('value_gate_count')}{suffix}"
            )
        if int(near_pass.get("score_near_count") or 0):
            top_score = near_pass.get("top_score_near") or []
            suffix = ""
            if top_score:
                top = top_score[0]
                suffix = (
                    f" topScore={top.get('instrument', '')}"
                    f" {format_num(top.get('max_score'), 1)}/{format_num(top.get('threshold'), 1)}"
                )
            near_bits.append(f"score={near_pass.get('score_near_count')}{suffix}")
        if int(near_pass.get("spread_ratio_near_count") or 0):
            top_ratio = near_pass.get("top_spread_ratio_near") or []
            suffix = ""
            if top_ratio:
                top = top_ratio[0]
                suffix = (
                    f" topRatio={top.get('instrument', '')}"
                    f" {format_num(top.get('max_observed'), 2)}/{format_num(top.get('threshold'), 2)}"
                )
            near_bits.append(f"ratio={near_pass.get('spread_ratio_near_count')}{suffix}")
        if near_bits:
            parts.append(f"nearPass[{';'.join(near_bits)}]")
        if strict_reject_rows:
            parts.append(
                f"strict_reject_max={format_num(strict_max_cluster, 0)}/"
                f"{format_num(strict_max_dir_cluster, 0)} near2={strict_near_count}"
            )
        if reject_part:
            parts.append(f"rejects={reject_part}")
    return " ".join(parts).strip()


def primary_scout_live_rollout_summary() -> str:
    return scout_live_rollout_summary("primary_scout_live", PRIMARY_SCOUT_LIVE_ROOT)


def tech_scout_live_rollout_summary() -> str:
    return scout_live_rollout_summary("tech_scout_live", TECH_SCOUT_LIVE_ROOT)


def latest_daily_move_dir(root: Path = DAILY_MOVE_COMPARISON_ROOT) -> Path | None:
    if not root.exists():
        return None
    dated = [
        path
        for path in root.iterdir()
        if path.is_dir() and len(path.name) == 10 and path.name[4] == "-" and path.name[7] == "-"
    ]
    return max(dated, key=lambda path: path.name) if dated else None


def previous_daily_move_dir(root: Path = DAILY_MOVE_COMPARISON_ROOT, current: Path | None = None) -> Path | None:
    if not root.exists():
        return None
    current = current or latest_daily_move_dir(root)
    dated = sorted(
        [
            path
            for path in root.iterdir()
            if path.is_dir() and len(path.name) == 10 and path.name[4] == "-" and path.name[7] == "-"
        ],
        key=lambda path: path.name,
    )
    if not dated or current is None:
        return None
    prior = [path for path in dated if path.name < current.name]
    return prior[-1] if prior else None


def daily_move_summary(path: Path | None = None) -> dict[str, Any]:
    day_dir = path or latest_daily_move_dir()
    if not day_dir:
        return {}
    market_path = day_dir / "daily_market_moves.csv"
    missed_path = day_dir / "missed_moves.csv"
    captured_path = day_dir / "captured_moves.csv"
    reason_path = day_dir / "miss_reason_summary.csv"
    account_path = day_dir / "account_capture_comparison.csv"
    theme_path = day_dir / "theme_summary.csv"
    missed_rows = dedupe_daily_report_rows(
        read_csv_rows(missed_path),
        ["instrument", "direction", "status", "miss_reason", "detail", "movement_key"],
    )
    captured_rows = dedupe_daily_report_rows(
        read_csv_rows(captured_path),
        ["instrument", "direction", "status", "trade_id", "order_id", "units", "price", "movement_key", "reason"],
    )
    reasons = count_rows_by_field(missed_rows, "miss_reason")[:5] or read_csv_rows(reason_path, limit=5)
    missed_instruments = count_rows_by_field(missed_rows, "instrument")[:5]
    miss_gate_counts: dict[str, int] = {}
    for row in missed_rows:
        key = classify_miss_detail(row.get("detail"))
        miss_gate_counts[key] = miss_gate_counts.get(key, 0) + 1
    miss_gates = [
        {"gate": key, "count": count}
        for key, count in sorted(miss_gate_counts.items(), key=lambda item: (-item[1], item[0]))
    ][:5]
    missed_value = missed_value_summary(missed_rows)
    near_value_gate = near_value_gate_summary(missed_rows)
    captured_value = captured_value_summary(captured_rows)
    accounts = read_csv_rows(account_path, limit=8)
    themes = read_csv_rows(theme_path, limit=5)
    return {
        "date": day_dir.name,
        "market_moves": count_csv_data_rows(market_path) or 0,
        "missed_moves": len(missed_rows),
        "captured_moves": len(captured_rows),
        "raw_missed_rows": count_csv_data_rows(missed_path) or 0,
        "raw_captured_rows": count_csv_data_rows(captured_path) or 0,
        "top_miss_reasons": reasons,
        "top_missed_instruments": missed_instruments,
        "miss_gate_classes": miss_gates,
        "missed_value": missed_value,
        "near_value_gate": near_value_gate,
        "captured_value": captured_value,
        "account_capture": accounts,
        "top_themes": themes,
        "generated_age_minutes": utc_age_minutes(
            datetime.fromtimestamp(
                max(
                    (
                        p.stat().st_mtime
                        for p in [market_path, missed_path, captured_path, reason_path, account_path, theme_path]
                        if p.exists()
                    ),
                    default=0.0,
                ),
                tz=timezone.utc,
            ).isoformat()
        ),
    }


def daily_move_summary_text(summary: dict[str, Any] | None = None) -> str:
    summary = summary or daily_move_summary()
    if not summary:
        return ""
    reasons = summary.get("top_miss_reasons") or []
    reason_bits = []
    if isinstance(reasons, list):
        for row in reasons[:3]:
            if not isinstance(row, dict):
                continue
            reason = row.get("miss_reason") or row.get("reason") or row.get("metric") or ""
            count = row.get("count") or row.get("rows") or ""
            if reason:
                reason_bits.append(f"{reason}:{count}")
    themes = summary.get("top_themes") or []
    theme_bits = []
    if isinstance(themes, list):
        for row in themes[:3]:
            if not isinstance(row, dict):
                continue
            theme = row.get("theme") or ""
            count = row.get("count") or ""
            if theme:
                theme_bits.append(f"{theme}:{count}")
    missed_instruments = summary.get("top_missed_instruments") or []
    missed_bits = []
    if isinstance(missed_instruments, list):
        for row in missed_instruments[:3]:
            if not isinstance(row, dict):
                continue
            inst = row.get("instrument") or ""
            count = row.get("count") or ""
            if inst:
                missed_bits.append(f"{inst}:{count}")
    gate_classes = summary.get("miss_gate_classes") or []
    gate_bits = []
    if isinstance(gate_classes, list):
        selected_gates = list(gate_classes[:3])
        if not any(isinstance(row, dict) and row.get("gate") == "scout_regime_lock" for row in selected_gates):
            regime_row = next(
                (
                    row
                    for row in gate_classes
                    if isinstance(row, dict) and row.get("gate") == "scout_regime_lock"
                ),
                None,
            )
            if regime_row:
                selected_gates.append(regime_row)
        for row in selected_gates:
            if not isinstance(row, dict):
                continue
            gate = row.get("gate") or ""
            count = row.get("count") or ""
            if gate:
                gate_bits.append(f"{gate}:{count}")
    missed_value = summary.get("missed_value") or {}
    value_part = ""
    if isinstance(missed_value, dict):
        total_value = maybe_float(missed_value.get("total_value_usd"))
        value_rows = missed_value.get("value_rows") or 0
        value_instruments = missed_value.get("top_value_instruments") or []
        value_bits = []
        if isinstance(value_instruments, list):
            for row in value_instruments[:3]:
                if not isinstance(row, dict):
                    continue
                instrument = row.get("instrument") or ""
                value = maybe_float(row.get("value_usd"))
                if instrument and value is not None:
                    value_bits.append(f"{instrument}:${format_num(value, 3)}")
        if total_value is not None and value_rows:
            value_part = f" miss_value=${format_num(total_value, 3)}/{value_rows}"
            if value_bits:
                value_part += f" value_top={';'.join(value_bits)}"
    near_value_gate = summary.get("near_value_gate") or {}
    near_value_part = ""
    if isinstance(near_value_gate, dict):
        near_count = int(maybe_float(near_value_gate.get("count")) or 0)
        near_total = maybe_float(near_value_gate.get("total_expected_usd"))
        near_instruments = near_value_gate.get("top_instruments") or []
        near_bits = []
        if isinstance(near_instruments, list):
            for row in near_instruments[:3]:
                if not isinstance(row, dict):
                    continue
                instrument = row.get("instrument") or ""
                expected = maybe_float(row.get("expected_usd"))
                max_ratio = maybe_float(row.get("max_ratio"))
                if instrument and expected is not None:
                    bit = f"{instrument}:${format_num(expected, 3)}"
                    if max_ratio is not None:
                        bit += f"({format_num(max_ratio * 100.0, 0)}%)"
                    near_bits.append(bit)
        if near_count and near_total is not None:
            near_value_part = f" near_value=${format_num(near_total, 3)}/{near_count}"
            if near_bits:
                near_value_part += f" near_top={';'.join(near_bits)}"
    captured_value = summary.get("captured_value") or {}
    capture_part = ""
    if isinstance(captured_value, dict):
        captured_orders = captured_value.get("captured_orders") or 0
        captured_movements = captured_value.get("captured_movements") or 0
        captured_total = maybe_float(captured_value.get("total_value_usd"))
        if captured_orders or captured_movements or captured_total:
            capture_part = (
                f" captured_orders={captured_orders} "
                f"captured_events={captured_movements}"
            )
            if captured_total is not None:
                capture_part += f" captured_value=${format_num(captured_total, 3)}"
    age = summary.get("generated_age_minutes")
    age_part = f" age={format_num(age, 0)}m" if age is not None else ""
    reasons_part = f" reasons={';'.join(reason_bits)}" if reason_bits else ""
    themes_part = f" themes={';'.join(theme_bits)}" if theme_bits else ""
    missed_part = f" missed_top={';'.join(missed_bits)}" if missed_bits else ""
    gates_part = f" miss_gates={';'.join(gate_bits)}" if gate_bits else ""
    raw_bits = []
    if summary.get("raw_missed_rows") not in (None, summary.get("missed_moves")):
        raw_bits.append(f"raw_missed={summary.get('raw_missed_rows')}")
    if summary.get("raw_captured_rows") not in (None, summary.get("captured_moves")):
        raw_bits.append(f"raw_captured={summary.get('raw_captured_rows')}")
    raw_part = f" {' '.join(raw_bits)}" if raw_bits else ""
    return (
        f"daily_moves[{summary.get('date', '')}]:"
        f"market={summary.get('market_moves', 0)} "
        f"missed={summary.get('missed_moves', 0)} "
        f"captured_rows={summary.get('captured_moves', 0)}"
        f"{themes_part}{reasons_part}{missed_part}{gates_part}{value_part}{near_value_part}{capture_part}{raw_part}{age_part}"
    ).strip()


def trainer_recent_error_summary(
    path: Path = TRAINER_RECENT_ERRORS_PATH,
    state_path: Path | None = None,
) -> dict[str, Any]:
    row = read_last_csv_row(path)
    if not row:
        return {}
    state = read_json(state_path or (ROOT / "data" / "oanda_training_manager" / "continuous_research" / "research_state.json"), {})
    if not isinstance(state, dict):
        state = {}
    time_utc = row.get("time_utc") or ""
    age = utc_age_minutes(time_utc)
    heartbeat_utc = str(state.get("last_research_heartbeat_utc") or "")
    last_experiment_utc = str(state.get("last_experiment_utc") or "")
    resolved_after_error = bool(
        time_utc
        and (
            (heartbeat_utc and heartbeat_utc > time_utc)
            or (last_experiment_utc and last_experiment_utc > time_utc)
        )
    )
    message = str(row.get("error_message") or "").strip()
    if len(message) > 90:
        message = message[:87] + "..."
    return {
        "time_utc": time_utc,
        "age_minutes": age,
        "heartbeat_utc": heartbeat_utc,
        "last_experiment_utc": last_experiment_utc,
        "resolved_after_error": resolved_after_error,
        "where": row.get("where", ""),
        "error_type": row.get("error_type", ""),
        "error_message": message,
        "row_count": count_csv_data_rows(path) or 0,
    }


def latest_account_improvement_seed_summary(
    path: Path = LATEST_ACCOUNT_IMPROVEMENT_SEED_PATH,
) -> str:
    payload = read_json(path, {})
    if not isinstance(payload, dict) or not payload:
        return ""
    seeded_by_lane = payload.get("seeded_by_lane") or {}
    if not isinstance(seeded_by_lane, dict):
        seeded_by_lane = {}
    total = maybe_float(payload.get("seeded_total"))
    if total is None:
        total = sum(maybe_float(value) or 0.0 for value in seeded_by_lane.values())
    lane_bits = []
    for lane, count in sorted(
        seeded_by_lane.items(),
        key=lambda item: (-(maybe_float(item[1]) or 0.0), str(item[0])),
    ):
        numeric = maybe_float(count) or 0.0
        if numeric <= 0:
            continue
        lane_bits.append(f"{lane}:{format_num(numeric, 0)}")
    if not lane_bits and total <= 0:
        return ""
    age = utc_age_minutes(payload.get("generated_utc"))
    age_part = f" age={format_num(age, 0)}m" if age is not None else ""
    examples = payload.get("examples") or []
    example_part = ""
    if isinstance(examples, list) and examples:
        first = next((item for item in examples if isinstance(item, dict)), None)
        if isinstance(first, dict):
            instruments = first.get("instrument_whitelist") or []
            if isinstance(instruments, list):
                instruments_text = ",".join(str(inst) for inst in instruments[:4])
            else:
                instruments_text = str(instruments or "")
            example_part = (
                f" example={first.get('source', '')}/"
                f"{first.get('model_type', '')}/"
                f"{first.get('target', '')}"
            )
            if instruments_text:
                example_part += f"[{instruments_text}]"
    execution = str(payload.get("execution") or "")
    exec_part = f" exec={execution}" if execution else ""
    regime_seed = recent_regime_lock_seed_summary()
    regime_seed_part = ""
    if int(regime_seed.get("count") or 0) > 0:
        instruments = regime_seed.get("top_instruments") or []
        inst_bits: list[str] = []
        if isinstance(instruments, list):
            for row in instruments[:4]:
                if isinstance(row, dict) and row.get("instrument"):
                    inst_bits.append(f"{row.get('instrument')}:{row.get('count')}")
        regime_seed_part = f" regimeLockSpecs={regime_seed.get('count')}"
        if inst_bits:
            regime_seed_part += f"[{','.join(inst_bits)}]"
    capture_seed = recent_capture_gap_seed_summary()
    capture_seed_part = ""
    if int(capture_seed.get("count") or 0) > 0:
        instruments = capture_seed.get("top_instruments") or []
        inst_bits = []
        if isinstance(instruments, list):
            for row in instruments[:4]:
                if isinstance(row, dict) and row.get("instrument"):
                    inst_bits.append(f"{row.get('instrument')}:{row.get('count')}")
        capture_seed_part = f" captureGapSpecs={capture_seed.get('count')}"
        if inst_bits:
            capture_seed_part += f"[{','.join(inst_bits)}]"
    return (
        f"latest_seed:total={format_num(total, 0)} "
        f"lanes={';'.join(lane_bits[:5])}"
        f"{age_part}{exec_part}{regime_seed_part}{capture_seed_part}{example_part}"
    ).strip()


def missed_spike_usdzar_robust_seed_summary(
    path: Path = MISSED_SPIKE_USDZAR_ROBUST_SEED_PATH,
) -> str:
    payload = read_json(path, {})
    if not isinstance(payload, dict) or not payload:
        return ""
    count = maybe_float(payload.get("seeded_count"))
    if count is None or count <= 0:
        return ""
    baskets = payload.get("baskets") or []
    basket_names: list[str] = []
    if isinstance(baskets, list):
        for item in baskets:
            if isinstance(item, (list, tuple)) and item:
                basket_names.append(str(item[0]))
    age = utc_age_minutes(payload.get("generated_utc"))
    age_part = f" age={format_num(age, 0)}m" if age is not None else ""
    execution = str(payload.get("execution") or "")
    exec_part = f" exec={execution}" if execution else ""
    return (
        "usdzar_robust_seed:"
        f"count={format_num(count, 0)} "
        f"baskets={','.join(basket_names[:6])}"
        f"{age_part}{exec_part}"
    ).strip()


def missed_spike_usdzar_relaxed_seed_summary(
    path: Path = MISSED_SPIKE_USDZAR_RELAXED_SEED_PATH,
) -> str:
    payload = read_json(path, {})
    if not isinstance(payload, dict) or not payload:
        return ""
    count = maybe_float(payload.get("seeded_count"))
    if count is None or count <= 0:
        return ""
    contexts = payload.get("contexts") or []
    context_names: list[str] = []
    if isinstance(contexts, list):
        for item in contexts:
            if isinstance(item, (list, tuple)) and item:
                context_names.append(str(item[0]))
    age = utc_age_minutes(payload.get("generated_utc"))
    age_part = f" age={format_num(age, 0)}m" if age is not None else ""
    return (
        "usdzar_relaxed_seed:"
        f"count={format_num(count, 0)} "
        f"contexts={','.join(context_names[:6])}"
        f"{age_part}"
    ).strip()


def missed_spike_usdzar_followup_result_summary(
    path: Path = TRAINER_EXPERIMENT_LEDGER_PATH,
) -> str:
    if not path.exists():
        return ""
    rows: list[dict[str, Any]] = []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                profile = str(row.get("specialist_profile") or "").lower()
                if "usdzar_robust" in profile or "usdzar_relaxed" in profile:
                    rows.append(row)
    except Exception:
        return ""
    if not rows:
        return ""
    successful = [
        row for row in rows
        if str(row.get("status") or "").strip().lower() == "successful"
        or str(row.get("gate_passed") or "").strip().lower() == "true"
    ]
    insufficient = [
        row for row in rows
        if "insufficientrollingfolds" in str(row.get("error") or "").lower()
        or "no valid purged" in str(row.get("error") or "").lower()
    ]
    scored = [
        row for row in rows
        if (maybe_float(row.get("fold_count")) or 0.0) > 0.0
    ]
    scored.sort(
        key=lambda row: (
            1 if str(row.get("gate_passed") or "").strip().lower() == "true" else 0,
            maybe_float(row.get("minimum_week_auc")) or -1.0,
            maybe_float(row.get("mean_auc")) or -1.0,
            maybe_float(row.get("score")) or -1.0,
        ),
        reverse=True,
    )

    def followup_fail_text(row: dict[str, Any]) -> str:
        detail_path = rel_path(row.get("detail_path"))
        if not detail_path or not detail_path.exists():
            if "insufficientrollingfolds" in str(row.get("error") or "").lower():
                return " fail=insufficient_folds"
            return ""
        payload = read_json(detail_path, {})
        result = payload.get("result") if isinstance(payload, dict) else {}
        gate = result.get("gate") if isinstance(result, dict) else {}
        if not isinstance(gate, dict) or gate.get("passed") is True:
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
        return f" fail={','.join(failures[:5])}" if failures else ""

    best = scored[0] if scored else (successful[0] if successful else {})
    latest = rows[-1]
    latest_age = utc_age_minutes(latest.get("time_utc"))
    age_part = f" latestAge={format_num(latest_age, 0)}m" if latest_age is not None else ""
    best_part = "best=none_yet"
    if best:
        whitelist = str(best.get("instrument_whitelist") or best.get("instrument_subset") or "").replace(",", "+")
        profile = str(best.get("specialist_profile") or "")
        mode = "relaxed" if "usdzar_relaxed" in profile.lower() else "robust"
        best_part = (
            f"best={mode}/{best.get('model_type', '')}/"
            f"{best.get('target', '')}/{whitelist} "
            f"auc={format_num(best.get('mean_auc'), 3)} "
            f"minAuc={format_num(best.get('minimum_week_auc'), 3)} "
            f"folds={format_num(best.get('fold_count'), 0)} "
            f"trades={format_num(best.get('trades'), 0)} "
            f"net={format_num(best.get('mean_net_pips'), 3)}p "
            f"gate={best.get('gate_passed', '')}"
            f"{followup_fail_text(best)}"
        )
    return (
        "usdzar_followup:"
        f"rows={len(rows)} gates={len(successful)} "
        f"insufficient={len(insufficient)} {best_part}{age_part}"
    ).strip()


def advisor_sections(rows: list[dict[str, Any]]) -> dict[str, Any]:
    trainer_report = read_json(TRAINER_REPORTING_EXTENSIONS_PATH, {})
    if not isinstance(trainer_report, dict):
        trainer_report = {}
    trainer_status = trainer_report.get("trainer_status") or {}
    if not isinstance(trainer_status, dict):
        trainer_status = {}
    else:
        trainer_status = dict(trainer_status)
    research_state = read_json(TRAINER_RESEARCH_STATE_PATH, {})
    current_spec = (
        research_state.get("current_experiment_spec")
        if isinstance(research_state, dict)
        and isinstance(research_state.get("current_experiment_spec"), dict)
        else {}
    )
    if current_spec:
        trainer_status.update(
            {
                "research_heartbeat_utc": research_state.get(
                    "last_research_heartbeat_utc",
                    trainer_status.get("research_heartbeat_utc", ""),
                ),
                "current_experiment_status": research_state.get(
                    "current_experiment_status",
                    trainer_status.get("current_experiment_status", ""),
                ),
                "current_experiment_started_utc": research_state.get(
                    "current_experiment_started_utc",
                    trainer_status.get("current_experiment_started_utc", ""),
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
    latest_daily_dir = latest_daily_move_dir()
    previous_daily_dir = previous_daily_move_dir(current=latest_daily_dir)
    daily = daily_move_summary(latest_daily_dir)
    previous_daily = daily_move_summary(previous_daily_dir) if previous_daily_dir else {}
    role_by_name = {
        str(row.get("role") or ""): row
        for row in rows
        if isinstance(row, dict)
    }
    trainer_row = role_by_name.get("trainer_research", {})
    trainer_running = bool(trainer_row.get("running"))
    trainer_heartbeat_age = utc_age_minutes(trainer_status.get("research_heartbeat_utc"))
    trainer_current_age = utc_age_minutes(trainer_status.get("current_experiment_started_utc"))
    trainer_stale = bool(
        (not trainer_running)
        or (
            trainer_heartbeat_age is not None
            and trainer_heartbeat_age > 30.0
        )
    )
    errors = [
        {
            "role": row.get("role"),
            "state": row.get("state"),
        }
        for row in rows
        if "errors=" in str(row.get("state") or "")
        and " last=" in str(row.get("state") or "")
    ]
    return {
        "daily_move_summary": daily,
        "daily_move_summary_text": daily_move_summary_text(daily),
        "previous_daily_move_summary": previous_daily,
        "previous_daily_move_summary_text": daily_move_summary_text(previous_daily) if previous_daily else "",
        "live_day_monitor_summary": live_day_monitor_status_summary(),
        "trainer_report_generated_utc": trainer_report.get("generated_utc", ""),
        "latest_account_improvement_seed_summary": latest_account_improvement_seed_summary(),
        "missed_spike_usdzar_robust_seed_summary": missed_spike_usdzar_robust_seed_summary(),
        "missed_spike_usdzar_relaxed_seed_summary": missed_spike_usdzar_relaxed_seed_summary(),
        "missed_spike_usdzar_followup_result_summary": missed_spike_usdzar_followup_result_summary(),
        "trainer_current": {
            "source": trainer_status.get("current_spec_source", ""),
            "model": trainer_status.get("current_spec_model_type", ""),
            "target": trainer_status.get("current_spec_target", ""),
            "outcome": trainer_status.get("current_spec_outcome", ""),
            "subset": trainer_status.get("current_spec_instrument_subset", ""),
            "profile": trainer_status.get("current_spec_specialist_profile", ""),
            "validation_profile": trainer_status.get("current_spec_validation_profile", ""),
            "whitelist": trainer_status.get("current_spec_instrument_whitelist", []),
            "status": trainer_status.get("current_experiment_status", ""),
            "process_running": trainer_running,
            "heartbeat_age_minutes": trainer_heartbeat_age,
            "current_age_minutes": trainer_current_age,
            "stale": trainer_stale,
            "pending": trainer_status.get("pending_queued_specs", ""),
            "loop_mode": trainer_status.get("research_loop_mode", ""),
            "effective_stage": trainer_status.get("current_spec_effective_stage", ""),
            "effective_holdout_weeks": trainer_status.get("current_spec_effective_holdout_weeks", ""),
            "effective_max_train_rows": trainer_status.get("current_spec_effective_max_train_rows", ""),
            "requested_max_train_rows": trainer_status.get("current_spec_requested_max_train_rows", ""),
            "max_train_rows_capped": trainer_status.get("current_spec_max_train_rows_capped", ""),
        },
        "trainer_stopcap_summary": trainer_stopcap_summary(),
        "live_value_miss_followup_summary": live_value_miss_followup_summary(),
        "live_near_value_gate_followup_summary": live_near_value_gate_followup_summary(),
        "live_scan_cost_followup_summary": live_scan_cost_followup_summary(),
        "live_segment_focus_followup_summary": live_segment_focus_followup_summary(),
        "live_lead_only_robust_followup_summary": live_lead_only_robust_followup_summary(),
        "live_ev_near_miss_followup_summary": live_ev_near_miss_followup_summary(),
        "live_unknown_profile_followup_summary": live_unknown_profile_followup_summary(),
        "live_missed_spike_subset_followup_summary": live_missed_spike_subset_followup_summary(),
        "live_capture_gap_followup_summary": live_capture_gap_followup_summary(),
        "live_localized_cluster_followup_summary": live_localized_cluster_followup_summary(),
        "live_localized_directional_followup_summary": live_localized_directional_followup_summary(),
        "live_miss_queue_source_summary": live_miss_queue_source_summary(),
        "trainer_promotion_leader_summary": trainer_promotion_leader_summary(),
        "pre_spike_fold_leader_summary": pre_spike_fold_leader_summary(),
        "pre_spike_ml_fast_fold_leader_summary": pre_spike_fold_leader_summary(
            PRE_SPIKE_LEAD_ML_FAST_FOLDS_PATH,
        ),
        "pre_spike_trainer_focus_fold_leader_summary": pre_spike_fold_leader_summary(
            PRE_SPIKE_LEAD_TRAINER_FOCUS_FOLDS_PATH,
        ),
        "pre_spike_ml_fast_process_summary": pre_spike_ml_fast_process_summary(
            process_rows(),
        ),
        "pre_spike_trainer_focus_process_summary": pre_spike_trainer_focus_process_summary(
            process_rows(),
        ),
        "pre_spike_default_output_process_summary": pre_spike_default_output_process_summary(
            process_rows(),
        ),
        "pressure_gate_validation_summary": pressure_gate_validation_summary(),
        "missed_spike_capture_gap_summary": missed_spike_capture_gap_summary(),
        "reference_strategy_mimic_summary": reference_strategy_mimic_summary(),
        "primary_scout_live_rollout_summary": primary_scout_live_rollout_summary(),
        "tech_scout_live_rollout_summary": tech_scout_live_rollout_summary(),
        "trainer_recent_error": trainer_recent_error_summary(),
        "local_recent_errors": errors,
    }


def model_space_agenda_summary() -> str:
    agenda = read_json(MODEL_SPACE_AGENDA_PATH, {})
    if not isinstance(agenda, dict) or not agenda:
        return ""
    lane = agenda.get("trainer_lane") or {}
    model_types = lane.get("model_types") or []
    if not isinstance(model_types, list):
        model_types = []
    external = agenda.get("external_baseline_lanes") or []
    return (
        f"agenda_seeded={lane.get('seeded_this_run', '')} "
        f"models={','.join(str(model) for model in model_types[:5])} "
        f"external_lanes={len(external) if isinstance(external, list) else ''}"
    ).strip()


def model_space_plan_summary() -> str:
    plan = read_json(MODEL_SPACE_PLAN_PATH, {})
    if not isinstance(plan, dict) or not plan:
        return ""
    return (
        f"plan={plan.get('active_phase', '')} "
        f"pending={plan.get('total_pending_unique', '')} "
        f"seeded={plan.get('seeded_this_run', '')}"
    ).strip()


def model_metrics_summary() -> str:
    metrics = read_json(MODEL_METRICS_PATH, {})
    if not isinstance(metrics, dict) or not metrics:
        return ""
    active = metrics.get("active_technical_production") or {}
    if not isinstance(active, dict):
        active = {}
    return (
        f"metrics_rows={metrics.get('row_count', '')} "
        f"prod_arima={active.get('candidate_beats_arima', '')} "
        f"prod_arima_cov={active.get('arima_matched_pairs', '')}/"
        f"{active.get('arima_matched_pairs_total', '')}/"
        f"{active.get('arima_scorecard_pairs', '')} "
        f"arima_chal={(metrics.get('active_arima_challengers') or {}).get('arima_beats_current_pair_count', '')}"
    ).strip()


def reference_strategy_mimic_summary() -> str:
    """Compact newest reference-mimic result for advisor status.

    The reference mimic run is a useful rejected branch: it tests whether the
    older pasted EV/ATR-style strategy shapes reproduce edge on local data. Keep
    the latest result visible in compact status so it is not mistaken for an
    untested path later.
    """
    candidates = sorted(
        TRAINER_REPORT_DIR.glob("*reference_strategy_mimic*.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
        reverse=True,
    )
    if not candidates:
        return ""
    path = candidates[0]
    obj = read_json(path, {})
    if not isinstance(obj, dict) or not obj:
        return ""
    summary = obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
    if summary.get("pipeline_version") != "reference_strategy_mimic_v1":
        return ""
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
    generated_age = utc_age_minutes(obj.get("generated_utc", ""))
    age_part = f" age={generated_age:.0f}m" if generated_age is not None else ""
    pieces = [
        "reference_mimic:",
        f"ref={format_num(reference.get('listed_net_pips'), 1)}p",
    ]
    if test:
        pieces.append(
            f"test={format_num(test.get('net_pips'), 1)}p/"
            f"{format_num(test.get('trades'), 0)}tr"
        )
        if isinstance(test.get("weighted_win_rate"), (int, float)):
            pieces.append(f"wr={float(test.get('weighted_win_rate') or 0.0):.0%}")
    if full:
        pieces.append(
            f"full={format_num(full.get('net_pips'), 1)}p/"
            f"{format_num(full.get('trades'), 0)}tr"
        )
    pieces.append(f"fast={bool(obj.get('fast'))}")
    if age_part:
        pieces.append(age_part.strip())
    return " ".join(piece for piece in pieces if piece)


def compact_hook(text: Any) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    first_sentence = value.split(".", 1)[0].strip()
    return first_sentence or value


def trainer_control_state_summary(path: Path | None) -> str:
    state = read_json(path, {}) if path else {}
    if not isinstance(state, dict) or not state:
        return ""
    pieces: list[str] = []
    tick_age = utc_age_minutes(state.get("last_tick_utc"))
    if tick_age is not None:
        pieces.append(f"control_age={tick_age:.0f}m")
    jobs = [
        ("latest_missed_spike_backtest", "missed"),
        ("latest_pre_spike_lead_backtest", "pre_spike"),
        ("latest_ensemble_shadow_refresh", "ensemble"),
        ("latest_reporting_extensions", "reporting"),
        ("latest_technical_evidence", "technical_evidence"),
    ]
    for key, label in jobs:
        payload = state.get(key) if isinstance(state.get(key), dict) else {}
        if not payload:
            continue
        status = str(payload.get("status") or "").strip().lower()
        if status in {"", "completed", "success", "successful", "ok"}:
            continue
        age = utc_age_minutes(payload.get("time_utc") or payload.get("generated_utc"))
        age_part = f":{age:.0f}m" if age is not None else ""
        pieces.append(f"{label}={status}{age_part}")
    return " ".join(pieces)


def role_status(
    role: str,
    info: dict[str, Any],
    processes: list[dict[str, Any]],
) -> dict[str, Any]:
    monitor_path = rel_path(info.get("monitor_path"))
    state_path = rel_path(info.get("state_path"))
    monitor = read_last_csv_row(monitor_path) if monitor_path else {}
    state = read_json(state_path, {}) if state_path else {}
    raw = parse_raw_json(monitor)
    account = raw.get("account") if isinstance(raw.get("account"), dict) else {}
    pids = process_matches(processes, info.get("script"))
    effective_running = bool(pids)
    if role == "trainer_research" and not pids:
        launcher = read_json(RESEARCH_TRAINER_LAUNCHER_LATEST_PATH, {})
        launcher_pid = str(launcher.get("pid") or "") if isinstance(launcher, dict) else ""
        launcher_status = str(launcher.get("status") or "") if isinstance(launcher, dict) else ""
        if launcher_status not in {"exited", "crashed"} and pid_is_running(launcher_pid):
            pids = [launcher_pid]
            effective_running = True
    production_path = rel_path(info.get("production_manifest_path"))
    canary_path = rel_path(info.get("canary_manifest_path"))
    shadow_path = rel_path(info.get("shadow_manifest_path"))
    ensemble_shadow_path = rel_path(info.get("ensemble_shadow_manifest_path"))
    arima_challenger_path = rel_path(info.get("arima_challenger_manifest_path"))
    arima_shadow_report_path = rel_path(info.get("arima_shadow_report_path"))
    arima_adapter_report_path = rel_path(info.get("arima_adapter_report_path"))
    arima_pending_path = rel_path(info.get("arima_canary_candidate_pending_path"))
    arima_executor_report_path = rel_path(info.get("arima_canary_executor_report_path"))

    account_id = (
        info.get("account_id")
        or account.get("id")
        or monitor.get("account_id")
        or ""
    )
    open_trades = (
        monitor.get("open_trade_count")
        or account.get("open_trade_count")
        or ""
    )
    nav = monitor.get("nav") or account.get("nav") or ""
    margin_used_pct = (
        monitor.get("margin_used_pct")
        or account.get("margin_used_pct_of_nav")
        or ""
    )
    trades_text = trade_summary(raw)
    broker_snapshot, broker_snapshot_age = live_day_monitor_account_snapshot(role)
    if broker_snapshot:
        account_id = account_id or str(broker_snapshot.get("account_id") or "")
        open_trades = broker_snapshot.get("open_trades", open_trades)
        nav = broker_snapshot.get("nav") or nav
        margin_used_pct = broker_snapshot.get("margin_used_pct") or margin_used_pct
        trades_text = live_day_monitor_trade_summary(broker_snapshot)

    hook = info.get("hooked_to", "")
    model = production_model_summary(production_path)
    if role == "canary" and canary_path:
        canary = read_json(canary_path, {})
        if canary:
            model = (
                f"canary:{canary.get('model_type','')}:"
                f"{canary.get('target','')} stage={canary.get('stage','')}"
            )
    if role == "shadow_evaluator" and shadow_path:
        shadow = read_json(shadow_path, {})
        if shadow:
            model = (
                f"shadow:{shadow.get('model_type','')}:"
                f"{shadow.get('target','')} stage={shadow.get('stage','')}"
            )
    if not model:
        model = compact_hook(hook)
    ensemble = ensemble_shadow_summary(ensemble_shadow_path)
    if ensemble:
        model = f"{model} | {ensemble}" if model else ensemble
    arima_challenger = arima_challenger_summary(arima_challenger_path)
    if arima_challenger:
        model = f"{model} | {arima_challenger}" if model else arima_challenger
    arima_shadow_report = arima_shadow_report_summary(arima_shadow_report_path)
    if arima_shadow_report:
        model = f"{model} | {arima_shadow_report}" if model else arima_shadow_report
    arima_adapter = arima_adapter_summary(arima_adapter_report_path, arima_pending_path)
    if arima_adapter:
        model = f"{model} | {arima_adapter}" if model else arima_adapter
    arima_executor = arima_canary_executor_summary(arima_executor_report_path)
    if arima_executor:
        model = f"{model} | {arima_executor}" if model else arima_executor
    if role == "spike_scout":
        scout_report = spike_scout_report_summary()
        if scout_report:
            model = f"{model} | {scout_report}" if model else scout_report
        missed_backtest = missed_spike_backtest_summary()
        if missed_backtest:
            model = f"{model} | {missed_backtest}" if model else missed_backtest
        lead_backtest = pre_spike_lead_backtest_summary()
        if lead_backtest:
            model = f"{model} | {lead_backtest}" if model else lead_backtest

    extra_state = ""
    if role == "trainer_research":
        research_path = rel_path(info.get("research_state_path"))
        research = read_json(research_path, {})
        if isinstance(research, dict):
            current = research.get("current_experiment_spec")
            heartbeat_age = utc_age_minutes(research.get("last_research_heartbeat_utc"))
            heartbeat_part = (
                f" heartbeat_age={heartbeat_age:.0f}m"
                if heartbeat_age is not None
                else ""
            )
            stale_part = (
                " stale_research_process=true"
                if not pids and heartbeat_age is not None and heartbeat_age > 30.0
                else ""
            )
            inferred_part = (
                " process_unconfirmed=true"
                if not pids and heartbeat_age is not None and heartbeat_age <= 30.0
                else ""
            )
            if not pids and heartbeat_age is not None and heartbeat_age <= 30.0:
                effective_running = True
            current_part = ""
            if isinstance(current, dict):
                age = utc_age_minutes(research.get("current_experiment_started_utc"))
                age_part = f" age={age:.0f}m" if age is not None else ""
                current_part = (
                    f" current={current.get('model_type','')}/"
                    f"{current.get('target','')}/"
                    f"{current.get('evaluation_stage','')}{age_part}"
                )
            extra_state = (
                f"idx={research.get('generated_index','')} "
                f"last={research.get('last_experiment_status','')}"
                f"{current_part}"
                f"{heartbeat_part}"
                f"{stale_part}"
                f"{inferred_part}"
            ).strip()
        agenda_summary = model_space_agenda_summary()
        if agenda_summary:
            extra_state = f"{extra_state} {agenda_summary}".strip()
        plan_summary = model_space_plan_summary()
        if plan_summary:
            extra_state = f"{extra_state} {plan_summary}".strip()
        metrics_summary = model_metrics_summary()
        if metrics_summary:
            extra_state = f"{extra_state} {metrics_summary}".strip()
        missed_backtest = missed_spike_backtest_summary()
        if missed_backtest:
            extra_state = f"{extra_state} {missed_backtest}".strip()
        lead_backtest = pre_spike_lead_backtest_summary()
        if lead_backtest:
            extra_state = f"{extra_state} {lead_backtest}".strip()
        stopcap_summary = trainer_stopcap_summary()
        if stopcap_summary:
            extra_state = f"{extra_state} {stopcap_summary}".strip()
        control_summary = trainer_control_state_summary(rel_path(info.get("control_state_path")))
        if control_summary:
            extra_state = f"{extra_state} {control_summary}".strip()
    elif role == "depth_collector" and isinstance(state, dict):
        last_success = state.get("last_success") if isinstance(state.get("last_success"), dict) else {}
        output_file = rel_path(last_success.get("output_file")) if last_success else None
        rows_today = count_csv_data_rows(output_file)
        rows_today_part = f" rows_today={rows_today}" if rows_today is not None else ""
        success_age = utc_age_minutes(last_success.get("time_utc") if last_success else "")
        success_age_part = (
            f" last_success_age={success_age:.0f}m" if success_age is not None else ""
        )
        error_age = utc_age_minutes(state.get("last_error_utc"))
        error_age_part = (
            f" last_error_age={error_age:.0f}m" if error_age is not None else ""
        )
        extra_state = (
            f"rows_session={state.get('rows_written','')}{rows_today_part} "
            f"errors={state.get('errors','')}{success_age_part}{error_age_part}"
        ).strip()
    elif isinstance(state, dict):
        last_tick = state.get("last_tick_utc") or state.get("last_monitor_utc") or ""
        if last_tick:
            extra_state = f"last={last_tick}"
        crisis = crisis_summary(state)
        if crisis:
            extra_state = f"{extra_state} {crisis}".strip()

    errors = recent_error_summary(rel_path(info.get("data_dir")))
    if errors:
        extra_state = f"{extra_state} {errors}".strip()
    if broker_snapshot_age is not None and not broker_snapshot and role in LIVE_DAY_MONITOR_ROLE_KEYS:
        extra_state = f"{extra_state} broker_snapshot_stale={broker_snapshot_age:.0f}m".strip()

    return {
        "role": role,
        "display_name": info.get("display_name", role),
        "account_id": account_id or "",
        "script": info.get("script", ""),
        "running": effective_running,
        "pids": ",".join(pids),
        "execution": info.get("execution", ""),
        "nav": format_num(nav, 2),
        "margin_pct": format_num(margin_used_pct, 2),
        "open_trades": str(open_trades) if open_trades != "" else "",
        "trades": trades_text,
        "hooked_to": hook,
        "model": model,
        "state": extra_state,
    }


def print_table(rows: list[dict[str, Any]]) -> None:
    columns = [
        ("role", "role"),
        ("running", "run"),
        ("account_id", "account"),
        ("nav", "nav"),
        ("margin_pct", "margin%"),
        ("open_trades", "open"),
        ("model", "model/hook"),
    ]
    widths = {}
    for key, label in columns:
        cap = 86 if key == "model" else 54
        widths[key] = max(
            len(label),
            min(cap, max((len(str(row.get(key, ""))) for row in rows), default=0)),
        )
    header = "  ".join(label.ljust(widths[key]) for key, label in columns)
    print(header)
    print("-" * len(header))
    for row in rows:
        values = []
        for key, _ in columns:
            value = str(row.get(key, ""))
            if key == "running":
                value = "yes" if row.get(key) else "no"
            if len(value) > widths[key]:
                value = value[: max(0, widths[key] - 3)] + "..."
            values.append(value.ljust(widths[key]))
        print("  ".join(values))
        trades = str(row.get("trades") or "")
        state = str(row.get("state") or "")
        if trades:
            print(f"  trades: {trades}")
        if state:
            print(f"  state:  {state}")


def print_advisor_sections(sections: dict[str, Any]) -> None:
    print()
    print("advisor")
    print("-------")
    daily_text = sections.get("daily_move_summary_text") or ""
    if daily_text:
        print(daily_text)
    previous_daily_text = sections.get("previous_daily_move_summary_text") or ""
    if previous_daily_text:
        print("previous_" + previous_daily_text)
    day_monitor = sections.get("live_day_monitor_summary") or ""
    if day_monitor:
        print(day_monitor)
    trainer = sections.get("trainer_current") or {}
    if isinstance(trainer, dict) and trainer:
        whitelist = trainer.get("whitelist") if isinstance(trainer.get("whitelist"), list) else []
        whitelist_text = ",".join(str(item) for item in whitelist[:6])
        profile_text = str(trainer.get("profile") or "")
        validation_text = str(trainer.get("validation_profile") or "")
        heartbeat_age = trainer.get("heartbeat_age_minutes")
        heartbeat_text = (
            f" heartbeatAge={format_num(heartbeat_age, 0)}m"
            if heartbeat_age is not None
            else ""
        )
        current_age = trainer.get("current_age_minutes")
        current_age_text = (
            f" currentAge={format_num(current_age, 0)}m"
            if current_age is not None
            else ""
        )
        print(
            "trainer:"
            f"mode={trainer.get('loop_mode', '')} "
            f"status={trainer.get('status', '')} "
            f"process={'running' if trainer.get('process_running') else 'stopped'} "
            f"stale={bool(trainer.get('stale', False))}"
            f"{heartbeat_text}{current_age_text} "
            f"current={trainer.get('source', '')}/"
            f"{trainer.get('model', '')}/"
            f"{trainer.get('target', '')}/"
            f"{trainer.get('outcome', '')}/"
            f"{trainer.get('subset', '')} "
            f"profile={profile_text} "
            f"validation={validation_text} "
            f"whitelist={whitelist_text} "
            f"rows={trainer.get('effective_max_train_rows', '')}/"
            f"{trainer.get('requested_max_train_rows', '')} "
            f"capped={trainer.get('max_train_rows_capped', '')} "
            f"pending={trainer.get('pending', '')}"
        )
    latest_seed = sections.get("latest_account_improvement_seed_summary") or ""
    if latest_seed:
        print(latest_seed)
    usdzar_seed = sections.get("missed_spike_usdzar_robust_seed_summary") or ""
    if usdzar_seed:
        print(usdzar_seed)
    usdzar_relaxed_seed = sections.get("missed_spike_usdzar_relaxed_seed_summary") or ""
    if usdzar_relaxed_seed:
        print(usdzar_relaxed_seed)
    usdzar_followup = sections.get("missed_spike_usdzar_followup_result_summary") or ""
    if usdzar_followup:
        print(usdzar_followup)
    stopcap = sections.get("trainer_stopcap_summary") or ""
    if stopcap:
        print(stopcap)
    value_miss_followup = sections.get("live_value_miss_followup_summary") or ""
    if value_miss_followup:
        print(value_miss_followup)
    near_value_gate_followup = sections.get("live_near_value_gate_followup_summary") or ""
    if near_value_gate_followup:
        print(near_value_gate_followup)
    scan_cost_followup = sections.get("live_scan_cost_followup_summary") or ""
    if scan_cost_followup:
        print(scan_cost_followup)
    segment_focus_followup = sections.get("live_segment_focus_followup_summary") or ""
    if segment_focus_followup:
        print(segment_focus_followup)
    lead_only_robust_followup = sections.get("live_lead_only_robust_followup_summary") or ""
    if lead_only_robust_followup:
        print(lead_only_robust_followup)
    ev_near_miss_followup = sections.get("live_ev_near_miss_followup_summary") or ""
    if ev_near_miss_followup:
        print(ev_near_miss_followup)
    unknown_profile_followup = sections.get("live_unknown_profile_followup_summary") or ""
    if unknown_profile_followup:
        print(unknown_profile_followup)
    missed_spike_subset_followup = sections.get("live_missed_spike_subset_followup_summary") or ""
    if missed_spike_subset_followup:
        print(missed_spike_subset_followup)
    capture_gap_followup = sections.get("live_capture_gap_followup_summary") or ""
    if capture_gap_followup:
        print(capture_gap_followup)
    localized_cluster_followup = sections.get("live_localized_cluster_followup_summary") or ""
    if localized_cluster_followup:
        print(localized_cluster_followup)
    localized_directional_followup = sections.get("live_localized_directional_followup_summary") or ""
    if localized_directional_followup:
        print(localized_directional_followup)
    live_miss_queue = sections.get("live_miss_queue_source_summary") or ""
    if live_miss_queue:
        print(live_miss_queue)
    promotion = sections.get("trainer_promotion_leader_summary") or ""
    if promotion:
        print(promotion)
    pre_spike = sections.get("pre_spike_fold_leader_summary") or ""
    if pre_spike:
        print(pre_spike)
    pre_spike_ml_fast = sections.get("pre_spike_ml_fast_fold_leader_summary") or ""
    if pre_spike_ml_fast:
        fast_text = (
            pre_spike_ml_fast
            .replace("pre_spike_ml_leader:", "pre_spike_ml_fast_leader:")
            .replace("pre_spike_ml_robust:", "pre_spike_ml_fast_robust:")
            .replace("pre_spike_rule_leader:", "pre_spike_ml_fast_rule_leader:")
        )
        print(fast_text)
    pre_spike_ml_fast_process = sections.get("pre_spike_ml_fast_process_summary") or ""
    if pre_spike_ml_fast_process:
        print(pre_spike_ml_fast_process)
    pre_spike_trainer_focus_process = sections.get("pre_spike_trainer_focus_process_summary") or ""
    if pre_spike_trainer_focus_process:
        print(pre_spike_trainer_focus_process)
    pre_spike_default_output_process = sections.get("pre_spike_default_output_process_summary") or ""
    if pre_spike_default_output_process:
        print(pre_spike_default_output_process)
    pre_spike_trainer_focus = sections.get("pre_spike_trainer_focus_fold_leader_summary") or ""
    if pre_spike_trainer_focus:
        focus_text = (
            pre_spike_trainer_focus
            .replace("pre_spike_ml_leader:", "pre_spike_trainer_focus_ml_leader:")
            .replace("pre_spike_ml_robust:", "pre_spike_trainer_focus_ml_robust:")
            .replace("pre_spike_rule_leader:", "pre_spike_trainer_focus_rule_leader:")
        )
        print(focus_text)
    pressure_gate = sections.get("pressure_gate_validation_summary") or ""
    if pressure_gate:
        print(pressure_gate)
    capture_gap = sections.get("missed_spike_capture_gap_summary") or ""
    if capture_gap:
        print(capture_gap)
    reference_mimic = sections.get("reference_strategy_mimic_summary") or ""
    if reference_mimic:
        print(reference_mimic)
    primary_scout_live = sections.get("primary_scout_live_rollout_summary") or ""
    if primary_scout_live:
        print(primary_scout_live)
    tech_scout_live = sections.get("tech_scout_live_rollout_summary") or ""
    if tech_scout_live:
        print(tech_scout_live)
    trainer_error = sections.get("trainer_recent_error") or {}
    if isinstance(trainer_error, dict) and trainer_error:
        age = trainer_error.get("age_minutes")
        age_part = f" age={format_num(age, 0)}m" if age is not None else ""
        status = (
            "resolved_after_new_heartbeat"
            if trainer_error.get("resolved_after_error")
            else "latest"
        )
        print(
            "trainer_last_error:"
            f"status={status} "
            f"{trainer_error.get('error_type', '')} "
            f"where={trainer_error.get('where', '')}"
            f"{age_part} "
            f"rows={trainer_error.get('row_count', '')} "
            f"msg={trainer_error.get('error_message', '')}"
        )
    errors = sections.get("local_recent_errors") or []
    if isinstance(errors, list) and errors:
        print("recent_errors:")
        for row in errors[:8]:
            if isinstance(row, dict):
                print(f"- {row.get('role', '')}: {row.get('state', '')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY,
        help="Path to accounts registry JSON.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    parser.add_argument(
        "--role",
        action="append",
        default=[],
        help="Limit to one role. Can be repeated.",
    )
    parser.add_argument(
        "--advisor",
        action="store_true",
        help="Include read-only advisor summaries from trainer and daily move reports.",
    )
    args = parser.parse_args()

    registry = read_json(args.registry, {})
    roles = registry.get("roles", {}) if isinstance(registry, dict) else {}
    if not isinstance(roles, dict) or not roles:
        raise SystemExit(f"No roles found in registry: {args.registry}")

    wanted = set(args.role)
    processes = process_rows()
    rows = [
        role_status(role, info, processes)
        for role, info in roles.items()
        if not wanted or role in wanted
    ]

    if args.json:
        sections = advisor_sections(rows) if args.advisor else {}
        print(json.dumps({
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "registry": str(args.registry),
            "roles": rows,
            "advisor": sections,
        }, indent=2))
    else:
        print_table(rows)
        if args.advisor:
            print_advisor_sections(advisor_sections(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
