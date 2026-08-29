#!/usr/bin/env python3
"""Reporting-only extensions for the always-on OANDA training manager.

This module does not touch broker state, account state, promotions, or live
strategy routing.  It reads local trainer/tech ledgers and writes compact
reports that make the current research loop easier to audit:

* live-shadow scout decisions and model/ensemble evidence
* segment leaders by pair family, regime, session, and instrument
* entry timing/rejection buckets
* margin/drawdown proxy telemetry from the live tech account logs
* promotion/ensemble manifest evidence
"""

from __future__ import annotations

import csv
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
RESEARCH_ROOT = TRAINING_ROOT / "continuous_research"
EXPERIMENTS_ROOT = RESEARCH_ROOT / "experiments"
MODEL_LIFECYCLE_ROOT = TRAINING_ROOT / "model_lifecycle"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
REPORTS_ROOT = TRAINING_ROOT / "reports"
STATE_ROOT = TRAINING_ROOT / "state"
TECH_LIVE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "technical_scout_manager"
    / "account_live_tech_broad_regime_scout"
)

RESEARCH_STATE_PATH = RESEARCH_ROOT / "research_state.json"
RESEARCH_ONLY_STATE_PATH = STATE_ROOT / "research_only_state.json"
RESEARCH_QUEUE_PATH = MODEL_LIFECYCLE_ROOT / "research_queue.jsonl"
EXPERIMENT_LEDGER_PATH = RESEARCH_ROOT / "experiment_ledger.csv"

TECH_ACTIONS_PATH = TECH_LIVE_ROOT / "actions.csv"
TECH_EVENT_SCAN_PATH = TECH_LIVE_ROOT / "event_scan_summary.csv"
TECH_EVENT_SIGNALS_PATH = TECH_LIVE_ROOT / "event_signals.csv"
TECH_MONITOR_PATH = TECH_LIVE_ROOT / "monitor.csv"
TECH_POSITION_HEALTH_PATH = TECH_LIVE_ROOT / "position_health.csv"
TECH_COMPOUND_ATTEMPTS_PATH = TECH_LIVE_ROOT / "compound_attempts.csv"

LATEST_JSON_PATH = REPORTS_ROOT / "latest_trainer_reporting_extensions.json"
LATEST_MD_PATH = REPORTS_ROOT / "latest_trainer_reporting_extensions.md"
SEGMENT_LEADERBOARD_CSV = REPORTS_ROOT / "trainer_segment_leaderboard.csv"
LIVE_SHADOW_CSV = REPORTS_ROOT / "trainer_live_shadow_report.csv"
ENTRY_TIMING_CSV = REPORTS_ROOT / "trainer_entry_timing_report.csv"
PORTFOLIO_MARGIN_CSV = REPORTS_ROOT / "trainer_portfolio_margin_proxy.csv"
ENSEMBLE_EVIDENCE_CSV = REPORTS_ROOT / "trainer_ensemble_evidence_report.csv"
QUEUE_SUMMARY_CSV = REPORTS_ROOT / "trainer_queue_summary.csv"
PROMOTION_READINESS_CSV = REPORTS_ROOT / "trainer_promotion_readiness.csv"
ACCOUNT_MODEL_IMPROVEMENT_CSV = REPORTS_ROOT / "trainer_account_model_improvement.csv"
STOPCAP_COMPARISON_CSV = REPORTS_ROOT / "trainer_stopcap_comparison.csv"


PROMOTION_MANIFESTS = [
    "technical_production.json",
    "technical_model_manifest.json",
    "research_leader.json",
    "shadow_candidate.json",
    "ensemble_shadow_candidate.json",
    "arima_pair_challenger_shadow.json",
    "arima_canary_candidate_pending.json",
    "dum_validation_manifest.json",
]


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_mtime_iso(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    except Exception:
        return ""


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
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


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "passed"}


def rounded(value: Any, digits: int = 4) -> Any:
    try:
        number = safe_float(value)
        return round(number, digits)
    except Exception:
        return value


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(
        json.dumps(obj, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp, path)


def tail_text_lines(path: Path, max_lines: int, chunk_size: int = 1024 * 128) -> List[str]:
    if max_lines <= 0 or not path.exists():
        return []
    try:
        data = bytearray()
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            pos = handle.tell()
            while pos > 0 and data.count(b"\n") <= max_lines:
                read_size = min(chunk_size, pos)
                pos -= read_size
                handle.seek(pos)
                data[:0] = handle.read(read_size)
        lines = data.splitlines()
        if pos > 0 and lines:
            lines = lines[1:]  # discard partial first line
        return [
            line.decode("utf-8", errors="replace")
            for line in lines[-max_lines:]
            if line
        ]
    except Exception:
        return []


def tail_csv_rows(path: Path, max_rows: int) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            header = handle.readline().rstrip("\r\n")
    except Exception:
        return []
    if not header:
        return []
    lines = tail_text_lines(path, max_rows + 5)
    clean_lines = [line for line in lines if line.strip() and line.rstrip("\r\n") != header]
    if not clean_lines:
        return []
    try:
        reader = csv.DictReader([header] + clean_lines)
        return [dict(row) for row in reader]
    except Exception:
        return []


def read_csv_rows(path: Path, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    if max_rows:
        return tail_csv_rows(path, max_rows)
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def read_jsonl_rows(path: Path, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    lines: Iterable[str]
    if max_rows:
        lines = tail_text_lines(path, max_rows)
    else:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            return []
    rows: List[Dict[str, Any]] = []
    for line in lines:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def parse_json_obj(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        obj = json.loads(str(value))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def find_key(obj: Any, target_key: str, max_depth: int = 8) -> Any:
    if max_depth < 0:
        return None
    if isinstance(obj, dict):
        if target_key in obj:
            return obj[target_key]
        for value in obj.values():
            found = find_key(value, target_key, max_depth - 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_key(value, target_key, max_depth - 1)
            if found is not None:
                return found
    return None


def find_first_dict_with_key(obj: Any, target_key: str, max_depth: int = 8) -> Dict[str, Any]:
    found = find_key(obj, target_key, max_depth=max_depth)
    return found if isinstance(found, dict) else {}


def age_from_reason(text: str) -> Optional[float]:
    match = re.search(r"\bage=([0-9]+(?:\.[0-9]+)?)m\b", text or "", re.IGNORECASE)
    if match:
        return safe_float(match.group(1), 0.0)
    return None


def score_from_reason(text: str) -> Optional[float]:
    match = re.search(
        r"(?:score(?:=| too low )|ev score too low )([0-9]+(?:\.[0-9]+)?)",
        text or "",
        re.IGNORECASE,
    )
    if match:
        return safe_float(match.group(1), 0.0)
    return None


def classify_reason(status: str, reason: str, reject_reason: str) -> str:
    text = f"{status or ''} {reason or ''} {reject_reason or ''}".lower()
    status_text = (status or "").lower()
    if status_text in {"accepted", "filled", "opened", "submitted", "executed"}:
        return "accepted_or_submitted"
    if "counter-regime" in text or "counter regime" in text:
        return "counter_regime"
    if "ev score too low" in text or "score too low" in text:
        return "score_too_low"
    if "score demotion" in text or "demotion blocked" in text:
        return "score_demotion_block"
    if "too old" in text or "stale" in text:
        return "entry_timing_stale"
    if "move/spread" in text and "<" in text:
        return "move_spread_ratio_low"
    if "below_ratio" in text or "ratio" in text and "<" in text:
        return "move_spread_ratio_low"
    if "basket" in text and "<" in text:
        return "basket_cluster_low"
    if "below_pips" in text:
        return "move_size_low"
    if "spread" in text and ("wide" in text or "cost" in text or "ratio" in text):
        return "spread_cost"
    if "opposite" in text or "already open" in text or "rotation" in text:
        return "exposure_rotation"
    if "margin" in text or "risk cap" in text or "cap" in text:
        return "risk_margin_cap"
    if "duplicate" in text or "repeat" in text:
        return "duplicate_repeat"
    if "model" in text and ("reject" in text or "denied" in text or "blocked" in text):
        return "model_rejected"
    if "blocked" in text:
        return "blocked_other"
    if "cancel" in text:
        return "cancelled"
    if status_text in {"skipped", "blocked", "rejected"}:
        return "skipped_other"
    return "other"


def age_bin(age_minutes: Optional[float]) -> str:
    if age_minutes is None:
        return "unknown"
    if age_minutes < 5:
        return "00_05m"
    if age_minutes < 10:
        return "05_10m"
    if age_minutes < 20:
        return "10_20m"
    if age_minutes < 60:
        return "20_60m"
    if age_minutes < 180:
        return "60_180m"
    return "180m_plus"


def top_counter(counter: Counter, limit: int = 10) -> List[Dict[str, Any]]:
    return [{"key": key, "count": count} for key, count in counter.most_common(limit)]


def mean(values: Iterable[float]) -> float:
    vals = [v for v in values if not math.isnan(v) and not math.isinf(v)]
    return sum(vals) / len(vals) if vals else 0.0


def lifecycle_queue_counts(research_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return queue counts from the lifecycle queue, not only trainer state.

    The always-on trainer updates ``research_state.pending_queued_specs`` on its
    own cadence.  External research-queue seeders can append specs between
    trainer ticks, so reporting should surface both the trainer's last seen
    count and the authoritative queue-file count.
    """
    research_state = research_state if isinstance(research_state, dict) else read_json(RESEARCH_STATE_PATH, {})
    completed = set(research_state.get("completed_spec_hashes") or [])
    queue_rows = read_jsonl_rows(RESEARCH_QUEUE_PATH)
    all_hashes = {
        str(item.get("spec_hash") or "").strip()
        for item in queue_rows
        if str(item.get("spec_hash") or "").strip()
    }
    completed_hashes = all_hashes & completed
    pending_hashes = all_hashes - completed
    return {
        "queue_rows": len(queue_rows),
        "unique_specs": len(all_hashes),
        "pending_unique_specs": len(pending_hashes),
        "completed_unique_specs": len(completed_hashes),
        "research_state_pending_queued_specs": safe_int(
            research_state.get("pending_queued_specs"), 0
        ),
    }


def build_trainer_status() -> Dict[str, Any]:
    research_state = read_json(RESEARCH_STATE_PATH, {})
    loop_state = read_json(RESEARCH_ONLY_STATE_PATH, {})
    current_spec = research_state.get("current_experiment_spec") or {}
    queue_counts = lifecycle_queue_counts(research_state)
    return {
        "research_heartbeat_utc": research_state.get("last_research_heartbeat_utc", ""),
        "current_experiment_status": research_state.get("current_experiment_status", ""),
        "current_experiment_started_utc": research_state.get("current_experiment_started_utc", ""),
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
        "pending_queued_specs": queue_counts.get("pending_unique_specs", 0),
        "research_state_pending_queued_specs": queue_counts.get(
            "research_state_pending_queued_specs", 0
        ),
        "lifecycle_queue_rows": queue_counts.get("queue_rows", 0),
        "lifecycle_unique_specs": queue_counts.get("unique_specs", 0),
        "lifecycle_pending_unique_specs": queue_counts.get("pending_unique_specs", 0),
        "lifecycle_completed_unique_specs": queue_counts.get("completed_unique_specs", 0),
        "completed_spec_hashes": len(research_state.get("completed_spec_hashes") or []),
        "last_experiment_status": research_state.get("last_experiment_status", ""),
        "last_experiment_utc": research_state.get("last_experiment_utc", ""),
        "exhaustion_hybrid_specs_seeded": safe_int(
            research_state.get("exhaustion_hybrid_priority_specs_seeded"), 0
        ),
        "reversal_curve_focus_specs_seeded": safe_int(
            research_state.get("reversal_curve_focus_specs_seeded"), 0
        ),
        "research_loop_last_tick_utc": loop_state.get("last_tick_utc", ""),
        "research_loop_mode": loop_state.get("mode", ""),
    }


def build_queue_summary() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    research_state = read_json(RESEARCH_STATE_PATH, {})
    completed_hashes = set(research_state.get("completed_spec_hashes") or [])
    queue_rows = read_jsonl_rows(RESEARCH_QUEUE_PATH)
    queue_counts = lifecycle_queue_counts(research_state)
    counters: Dict[Tuple[str, str, str, str, str], Counter] = defaultdict(Counter)
    source_counter: Counter = Counter()
    pending_counter: Counter = Counter()
    completed_counter: Counter = Counter()
    for item in queue_rows:
        spec = item.get("spec") if isinstance(item.get("spec"), dict) else {}
        source = str(spec.get("source") or item.get("source") or "unknown")
        stage = str(spec.get("evaluation_stage") or "unknown")
        dataset = str(spec.get("dataset_kind") or "unknown")
        model = str(spec.get("model_type") or "unknown")
        target = str(spec.get("target") or "unknown")
        status = "completed" if item.get("spec_hash") in completed_hashes else "pending"
        key = (source, stage, dataset, model, target)
        counters[key][status] += 1
        source_counter[source] += 1
        if status == "pending":
            pending_counter[source] += 1
        else:
            completed_counter[source] += 1

    rows: List[Dict[str, Any]] = []
    for key, counts in counters.items():
        source, stage, dataset, model, target = key
        rows.append(
            {
                "source": source,
                "evaluation_stage": stage,
                "dataset_kind": dataset,
                "model_type": model,
                "target": target,
                "pending": counts.get("pending", 0),
                "completed": counts.get("completed", 0),
                "total": sum(counts.values()),
            }
        )
    rows.sort(key=lambda row: (safe_int(row["pending"]), safe_int(row["total"])), reverse=True)
    summary = {
        "queue_rows": len(queue_rows),
        "unique_specs": queue_counts.get("unique_specs", 0),
        "pending_unique_specs": queue_counts.get("pending_unique_specs", 0),
        "completed_unique_specs": queue_counts.get("completed_unique_specs", 0),
        "research_state_pending_queued_specs": queue_counts.get(
            "research_state_pending_queued_specs", 0
        ),
        "pending_by_source": top_counter(pending_counter, 20),
        "completed_by_source": top_counter(completed_counter, 20),
        "all_by_source": top_counter(source_counter, 20),
        "output_csv": str(QUEUE_SUMMARY_CSV),
    }
    write_csv_rows(
        QUEUE_SUMMARY_CSV,
        rows,
        [
            "source",
            "evaluation_stage",
            "dataset_kind",
            "model_type",
            "target",
            "pending",
            "completed",
            "total",
        ],
    )
    return summary, rows


def stopcap_policy_from_outcome(outcome: Any) -> str:
    text = str(outcome or "")
    match = re.search(r"two_stage_(trailing_stop\d+)_net_atr_", text)
    return match.group(1) if match else ""


def build_stopcap_comparison_report() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Summarize major-move stop-cap variants without changing research state."""
    ledger_rows = read_csv_rows(EXPERIMENT_LEDGER_PATH)
    groups: Dict[Tuple[str, str, str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in ledger_rows:
        policy = stopcap_policy_from_outcome(row.get("outcome"))
        if not policy:
            continue
        if str(row.get("target") or "").startswith("major_event") is False:
            continue
        key = (
            str(row.get("source") or ""),
            str(row.get("evaluation_stage") or ""),
            str(row.get("model_type") or ""),
            str(row.get("target") or ""),
            str(row.get("instrument_subset") or ""),
            policy,
        )
        groups[key].append(row)

    rows: List[Dict[str, Any]] = []
    best_by_family: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for key, members in groups.items():
        source, stage, model_type, target, subset, policy = key
        valid_net = [safe_float(row.get("mean_net_pips"), math.nan) for row in members]
        valid_net = [value for value in valid_net if math.isfinite(value)]
        valid_auc = [safe_float(row.get("mean_auc"), math.nan) for row in members]
        valid_auc = [value for value in valid_auc if math.isfinite(value)]
        trade_counts = [safe_float(row.get("trades"), 0.0) for row in members]
        pass_count = sum(1 for row in members if safe_bool(row.get("gate_passed")))
        error_count = sum(1 for row in members if str(row.get("error") or "").strip())
        best = max(
            members,
            key=lambda row: safe_float(row.get("mean_net_pips"), -1e9),
        )
        out = {
            "source": source,
            "evaluation_stage": stage,
            "model_type": model_type,
            "target": target,
            "instrument_subset": subset,
            "policy": policy,
            "runs": len(members),
            "gate_passed_runs": pass_count,
            "error_runs": error_count,
            "avg_mean_auc": rounded(sum(valid_auc) / len(valid_auc), 4) if valid_auc else "",
            "avg_trades": rounded(sum(trade_counts) / max(len(trade_counts), 1), 2),
            "avg_mean_net_pips": rounded(sum(valid_net) / len(valid_net), 6) if valid_net else "",
            "best_mean_net_pips": rounded(best.get("mean_net_pips"), 6),
            "best_mean_auc": rounded(best.get("mean_auc"), 4),
            "best_trades": rounded(best.get("trades"), 2),
            "best_time_utc": best.get("time_utc", ""),
            "best_gate_passed": best.get("gate_passed", ""),
            "best_error": best.get("error", ""),
        }
        rows.append(out)
        family_key = (source, stage, model_type, target, subset)
        current_best = best_by_family.get(family_key)
        if current_best is None or safe_float(out.get("avg_mean_net_pips"), -1e9) > safe_float(
            current_best.get("avg_mean_net_pips"),
            -1e9,
        ):
            best_by_family[family_key] = out

    rows.sort(
        key=lambda row: (
            safe_int(row.get("gate_passed_runs"), 0),
            safe_float(row.get("avg_mean_net_pips"), -1e9),
            safe_float(row.get("best_mean_net_pips"), -1e9),
        ),
        reverse=True,
    )
    write_csv_rows(
        STOPCAP_COMPARISON_CSV,
        rows,
        [
            "source",
            "evaluation_stage",
            "model_type",
            "target",
            "instrument_subset",
            "policy",
            "runs",
            "gate_passed_runs",
            "error_runs",
            "avg_mean_auc",
            "avg_trades",
            "avg_mean_net_pips",
            "best_mean_net_pips",
            "best_mean_auc",
            "best_trades",
            "best_time_utc",
            "best_gate_passed",
            "best_error",
        ],
    )
    policy_counts = Counter(row["policy"] for row in rows)
    leaders = sorted(
        best_by_family.values(),
        key=lambda row: safe_float(row.get("avg_mean_net_pips"), -1e9),
        reverse=True,
    )[:10]
    summary = {
        "rows": len(rows),
        "families": len(best_by_family),
        "policy_counts": top_counter(policy_counts, 10),
        "top_avg_net": [
            {
                "target": row.get("target", ""),
                "model_type": row.get("model_type", ""),
                "instrument_subset": row.get("instrument_subset", ""),
                "policy": row.get("policy", ""),
                "avg_mean_net_pips": row.get("avg_mean_net_pips", ""),
                "avg_mean_auc": row.get("avg_mean_auc", ""),
                "runs": row.get("runs", ""),
                "gate_passed_runs": row.get("gate_passed_runs", ""),
            }
            for row in leaders
        ],
        "output_csv": str(STOPCAP_COMPARISON_CSV),
    }
    return summary, rows


def latest_experiment_paths(limit: int = 350) -> List[Path]:
    if not EXPERIMENTS_ROOT.exists():
        return []
    paths = list(EXPERIMENTS_ROOT.glob("*.json"))
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[:limit]


def gate_passed_from_result(result: Dict[str, Any]) -> bool:
    gate = result.get("gate")
    if isinstance(gate, dict):
        if "passed" in gate:
            return safe_bool(gate.get("passed"))
        if "gate_passed" in gate:
            return safe_bool(gate.get("gate_passed"))
    if "gate_passed" in result:
        return safe_bool(result.get("gate_passed"))
    return False


def gate_bool_from_result(result: Dict[str, Any], key: str) -> bool:
    gate = result.get("gate")
    if isinstance(gate, dict) and key in gate:
        return safe_bool(gate.get(key))
    return False


def gate_failure_reasons_from_result(result: Dict[str, Any]) -> List[str]:
    gate = result.get("gate")
    if not isinstance(gate, dict):
        return []
    ignored = {"passed", "gate_passed"}
    reasons: List[str] = []
    for key, value in gate.items():
        if key in ignored:
            continue
        if isinstance(value, bool) and not value:
            reasons.append(key)
    return reasons


def build_segment_leaderboard(limit_experiments: int = 350) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    for path in latest_experiment_paths(limit_experiments):
        payload = read_json(path, {})
        if not isinstance(payload, dict):
            continue
        spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        scorecards = result.get("scorecards")
        if not isinstance(scorecards, dict):
            continue
        base = {
            "experiment_id": payload.get("experiment_id", ""),
            "detail_path": str(path),
            "experiment_mtime_utc": file_mtime_iso(path),
            "status": payload.get("status", ""),
            "source": spec.get("source", ""),
            "research_role": spec.get("research_role", ""),
            "evaluation_stage": spec.get("evaluation_stage", result.get("evaluation_stage", "")),
            "dataset_kind": spec.get("dataset_kind", ""),
            "model_type": spec.get("model_type", ""),
            "target": spec.get("target", ""),
            "outcome": spec.get("outcome", ""),
            "execution_policy": spec.get("execution_policy", ""),
            "feature_set": spec.get("feature_set", ""),
            "instrument_subset": spec.get("instrument_subset", result.get("instrument_subset", "")),
            "instrument_whitelist": ",".join(
                str(value) for value in (spec.get("instrument_whitelist") or result.get("instrument_whitelist") or [])
            ),
            "segment_filters": json.dumps(
                spec.get("segment_filters") or result.get("segment_filters") or {},
                sort_keys=True,
                default=str,
            ),
            "specialist_profile": spec.get("specialist_profile", ""),
            "specialist_parent_experiment_id": spec.get("specialist_parent_experiment_id", ""),
            "score": rounded(result.get("score", 0.0), 4),
            "mean_auc": rounded(result.get("mean_auc", 0.0), 4),
            "mean_average_precision": rounded(result.get("mean_average_precision", 0.0), 4),
            "selected_threshold": rounded(result.get("selected_threshold", 0.0), 4),
            "gate_passed": gate_passed_from_result(result),
        }
        for scorecard_name, entries in scorecards.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                trades = safe_float(entry.get("trades"), 0.0)
                mean_net = safe_float(entry.get("mean_net"), 0.0)
                profit_factor = safe_float(entry.get("profit_factor"), 0.0)
                leader_score = mean_net * math.log1p(max(trades, 0.0)) * max(min(profit_factor, 10.0), 0.0)
                rows.append(
                    {
                        **base,
                        "scorecard": scorecard_name,
                        "segment": entry.get("segment", ""),
                        "trades": rounded(trades, 2),
                        "win_rate": rounded(entry.get("win_rate", 0.0), 4),
                        "mean_net": rounded(mean_net, 5),
                        "total_net": rounded(entry.get("total_net", 0.0), 5),
                        "profit_factor": rounded(profit_factor, 4),
                        "leader_score": rounded(leader_score, 5),
                    }
                )
    rows.sort(
        key=lambda row: (
            safe_bool(row.get("gate_passed")),
            safe_float(row.get("leader_score")),
            safe_float(row.get("trades")),
        ),
        reverse=True,
    )
    output_rows = rows[:1000]
    write_csv_rows(
        SEGMENT_LEADERBOARD_CSV,
        output_rows,
        [
            "experiment_id",
            "detail_path",
            "experiment_mtime_utc",
            "status",
            "source",
            "research_role",
            "evaluation_stage",
            "dataset_kind",
            "model_type",
            "target",
            "outcome",
            "execution_policy",
            "feature_set",
            "instrument_subset",
            "instrument_whitelist",
            "segment_filters",
            "specialist_profile",
            "specialist_parent_experiment_id",
            "score",
            "mean_auc",
            "mean_average_precision",
            "selected_threshold",
            "gate_passed",
            "scorecard",
            "segment",
            "trades",
            "win_rate",
            "mean_net",
            "total_net",
            "profit_factor",
            "leader_score",
        ],
    )
    by_scorecard: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in output_rows:
        if len(by_scorecard[row["scorecard"]]) < 8:
            by_scorecard[row["scorecard"]].append(row)
    summary = {
        "experiments_scanned": len(latest_experiment_paths(limit_experiments)),
        "leader_rows": len(output_rows),
        "top_by_scorecard": dict(by_scorecard),
        "output_csv": str(SEGMENT_LEADERBOARD_CSV),
    }
    return summary, output_rows


def build_promotion_readiness_report(limit_experiments: int = 700) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Rank model candidates by stability before anyone considers live routing.

    This is deliberately reporting-only.  The trainer can produce impressive
    raw scores from a narrow pair/window sample; this report pushes the
    opposite direction by making weeks, selected trades, concentration,
    bootstrap lower bound, and live-lane focus visible in one place.
    """
    rows: List[Dict[str, Any]] = []
    for path in latest_experiment_paths(limit_experiments):
        payload = read_json(path, {})
        if not isinstance(payload, dict):
            continue
        spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        selected = result.get("selected_threshold") if isinstance(result.get("selected_threshold"), dict) else {}
        gate_passed = gate_passed_from_result(result)
        gate_failed_reasons = gate_failure_reasons_from_result(result)
        economic_passed = gate_bool_from_result(result, "economic_passed")
        detection_passed = gate_bool_from_result(result, "detection_passed")
        trades = safe_float(selected.get("trades"), 0.0)
        weeks = safe_float(selected.get("weeks"), spec.get("validation_weeks") or 0.0)
        positive_weeks = safe_float(selected.get("positive_weeks"), 0.0)
        positive_week_rate = (positive_weeks / weeks) if weeks > 0 else 0.0
        mean_net = safe_float(selected.get("mean_net_pips"), 0.0)
        total_net = safe_float(selected.get("total_net_pips"), 0.0)
        bootstrap_lower = safe_float(selected.get("bootstrap_lower_mean_net_pips"), 0.0)
        profit_factor = safe_float(selected.get("median_profit_factor"), 0.0)
        top_pair_share = safe_float(selected.get("top_pair_trade_share"), 1.0)
        top_pair_share_limit = safe_float(
            selected.get("top_pair_trade_share_limit"),
            0.35,
        )
        unique_pairs = safe_float(selected.get("unique_pairs"), 0.0)
        mean_auc = safe_float(result.get("mean_auc"), 0.0)
        min_auc = safe_float(result.get("minimum_week_auc"), 0.0)
        mean_brier_skill = safe_float(result.get("mean_brier_skill"), 0.0)
        stage = str(spec.get("evaluation_stage") or result.get("evaluation_stage") or "")
        instrument_whitelist_values = (
            spec.get("instrument_whitelist")
            or result.get("instrument_whitelist")
            or []
        )
        if isinstance(instrument_whitelist_values, str):
            instrument_whitelist = [
                part.strip()
                for part in instrument_whitelist_values.split(",")
                if part.strip()
            ]
        elif isinstance(instrument_whitelist_values, list):
            instrument_whitelist = [
                str(part).strip()
                for part in instrument_whitelist_values
                if str(part).strip()
            ]
        else:
            instrument_whitelist = []
        is_specialist = bool(instrument_whitelist)

        validation_grade = (
            gate_passed
            and not is_specialist
            and stage == "validation"
            and weeks >= 8
            and positive_week_rate >= 0.80
            and trades >= 300
            and mean_net > 0
            and bootstrap_lower > 0
            and top_pair_share <= 0.25
            and unique_pairs >= 8
            and mean_auc >= 0.58
            and min_auc >= 0.52
        )
        specialist_validation_grade = (
            gate_passed
            and is_specialist
            and stage == "validation"
            and weeks >= 6
            and positive_week_rate >= 0.70
            and trades >= 40
            and mean_net > 0
            and bootstrap_lower > 0
            and profit_factor >= 1.20
            and mean_auc >= 0.55
            and min_auc >= 0.50
            and top_pair_share <= max(top_pair_share_limit, 0.35)
        )
        needs_robust_followup = (
            gate_passed
            and stage == "screen"
            and trades >= 250
            and mean_net > 0
            and positive_week_rate >= 0.70
            and (is_specialist or top_pair_share <= 0.30)
        )
        calibration_detection_followup = (
            not gate_passed
            and stage == "validation"
            and economic_passed
            and not detection_passed
            and trades >= 75
            and mean_net > 0
            and bootstrap_lower > 0
            and profit_factor >= 1.20
            and positive_week_rate >= 0.70
            and mean_auc >= 0.55
            and min_auc >= 0.50
            and top_pair_share <= max(top_pair_share_limit, 0.35)
        )
        if is_specialist:
            stability_factor = 1.0 if top_pair_share <= top_pair_share_limit else 0.25
            breadth_factor = 1.0
        else:
            stability_factor = max(0.0, 1.0 - min(top_pair_share, 1.0))
            breadth_factor = min(1.0, unique_pairs / 20.0) if unique_pairs else 0.0
        week_factor = min(1.0, positive_week_rate) * math.log1p(max(weeks, 0.0))
        trade_factor = math.log1p(max(trades, 0.0))
        pf_factor = min(max(profit_factor, 0.0), 5.0)
        # Not a dollar-P/L estimate.  This is a stability/value proxy for
        # prioritizing research follow-ups before live sizing decisions.
        readiness_score = (
            max(mean_net, 0.0)
            * trade_factor
            * week_factor
            * pf_factor
            * stability_factor
            * max(0.25, breadth_factor)
        )
        if validation_grade:
            next_action = "candidate_for_shadow_or_canary_review"
        elif specialist_validation_grade:
            next_action = "specialist_candidate_for_pair_routing_review"
        elif needs_robust_followup:
            next_action = "queue_robust_validation_followup"
        elif calibration_detection_followup:
            next_action = "calibration_detection_followup"
        elif not gate_passed:
            next_action = "rejected_or_gate_failed"
        elif trades < 300 or weeks < 8:
            next_action = "needs_more_sample_or_longer_holdout"
        elif top_pair_share > 0.25:
            next_action = "concentration_too_high"
        elif bootstrap_lower <= 0:
            next_action = "bootstrap_lower_bound_not_positive"
        else:
            next_action = "monitor_only"

        rows.append(
            {
                "experiment_id": payload.get("experiment_id", ""),
                "detail_path": str(path),
                "experiment_mtime_utc": file_mtime_iso(path),
                "status": payload.get("status", ""),
                "source": spec.get("source", ""),
                "research_role": spec.get("research_role", ""),
                "deployment_lane": spec.get("deployment_lane", ""),
                "account_focus": spec.get("account_focus", ""),
                "evaluation_stage": stage,
                "dataset_kind": spec.get("dataset_kind", ""),
                "model_type": spec.get("model_type", ""),
                "target": spec.get("target", ""),
                "outcome": spec.get("outcome", ""),
                "execution_policy": spec.get("execution_policy", ""),
                "feature_set": spec.get("feature_set", ""),
                "instrument_subset": spec.get("instrument_subset", result.get("instrument_subset", "")),
                "instrument_whitelist": ",".join(instrument_whitelist),
                "segment_filters": json.dumps(
                    spec.get("segment_filters") or result.get("segment_filters") or {},
                    sort_keys=True,
                    default=str,
                ),
                "specialist_profile": spec.get("specialist_profile", ""),
                "specialist_parent_experiment_id": spec.get("specialist_parent_experiment_id", ""),
                "gate_passed": gate_passed,
                "economic_passed": economic_passed,
                "detection_passed": detection_passed,
                "gate_failed_reasons": ";".join(gate_failed_reasons),
                "validation_grade": validation_grade,
                "specialist_validation_grade": specialist_validation_grade,
                "needs_robust_followup": needs_robust_followup,
                "calibration_detection_followup": calibration_detection_followup,
                "next_action": next_action,
                "readiness_score": rounded(readiness_score, 5),
                "model_score": rounded(result.get("score", 0.0), 5),
                "mean_auc": rounded(mean_auc, 5),
                "minimum_week_auc": rounded(min_auc, 5),
                "mean_brier_skill": rounded(mean_brier_skill, 7),
                "selected_threshold": rounded(selected.get("threshold", 0.0), 5),
                "weeks": rounded(weeks, 2),
                "positive_weeks": rounded(positive_weeks, 2),
                "positive_week_rate": rounded(positive_week_rate, 4),
                "trades": rounded(trades, 2),
                "unique_pairs": rounded(unique_pairs, 2),
                "top_pair_trade_share": rounded(top_pair_share, 4),
                "top_pair_trade_share_limit": rounded(top_pair_share_limit, 4),
                "mean_net_pips": rounded(mean_net, 5),
                "bootstrap_lower_mean_net_pips": rounded(bootstrap_lower, 5),
                "total_net_pips": rounded(total_net, 5),
                "median_profit_factor": rounded(profit_factor, 5),
                "value_proxy_note": "readiness score is not USD P/L; it downweights low-sample and concentrated raw-pip wins",
            }
        )

    rows.sort(
        key=lambda row: (
            safe_bool(row.get("validation_grade")),
            safe_bool(row.get("specialist_validation_grade")),
            safe_bool(row.get("needs_robust_followup")),
            safe_bool(row.get("calibration_detection_followup")),
            safe_float(row.get("readiness_score")),
            safe_float(row.get("trades")),
        ),
        reverse=True,
    )
    output_rows = rows[:500]
    write_csv_rows(
        PROMOTION_READINESS_CSV,
        output_rows,
        [
            "experiment_id",
            "detail_path",
            "experiment_mtime_utc",
            "status",
            "source",
            "research_role",
            "deployment_lane",
            "account_focus",
            "evaluation_stage",
            "dataset_kind",
            "model_type",
            "target",
            "outcome",
            "execution_policy",
            "feature_set",
            "instrument_subset",
            "instrument_whitelist",
            "segment_filters",
            "specialist_profile",
            "specialist_parent_experiment_id",
            "gate_passed",
            "economic_passed",
            "detection_passed",
            "gate_failed_reasons",
            "validation_grade",
            "specialist_validation_grade",
            "needs_robust_followup",
            "calibration_detection_followup",
            "next_action",
            "readiness_score",
            "model_score",
            "mean_auc",
            "minimum_week_auc",
            "mean_brier_skill",
            "selected_threshold",
            "weeks",
            "positive_weeks",
            "positive_week_rate",
            "trades",
            "unique_pairs",
            "top_pair_trade_share",
            "top_pair_trade_share_limit",
            "mean_net_pips",
            "bootstrap_lower_mean_net_pips",
            "total_net_pips",
            "median_profit_factor",
            "value_proxy_note",
        ],
    )

    by_action = Counter(str(row.get("next_action") or "") for row in output_rows)
    by_lane = Counter(str(row.get("deployment_lane") or "unassigned") for row in output_rows)
    summary = {
        "experiments_scanned": len(latest_experiment_paths(limit_experiments)),
        "rows": len(output_rows),
        "validation_grade_count": sum(1 for row in output_rows if safe_bool(row.get("validation_grade"))),
        "specialist_validation_grade_count": sum(1 for row in output_rows if safe_bool(row.get("specialist_validation_grade"))),
        "robust_followup_count": sum(1 for row in output_rows if safe_bool(row.get("needs_robust_followup"))),
        "calibration_detection_followup_count": sum(1 for row in output_rows if safe_bool(row.get("calibration_detection_followup"))),
        "by_next_action": top_counter(by_action, 20),
        "by_deployment_lane": top_counter(by_lane, 20),
        "top_candidates": output_rows[:12],
        "output_csv": str(PROMOTION_READINESS_CSV),
    }
    return summary, output_rows


def build_account_model_improvement_report(
    readiness_rows: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Summarize model-improvement coverage by account/research lane.

    This is intentionally separate from live manager status.  It tells us
    whether each account idea has model evidence queued or validated before
    anything is considered for execution routing.
    """
    research_state = read_json(RESEARCH_STATE_PATH, {})
    completed_hashes = set(research_state.get("completed_spec_hashes") or [])
    queue_rows = read_jsonl_rows(RESEARCH_QUEUE_PATH)
    readiness_rows = readiness_rows if readiness_rows is not None else []

    lanes = [
        {
            "account_lane": "gpt_live_model_support",
            "account_focus": "gpt_live_001-001-21715580-001",
            "deployment_lane": "gpt_live_model_support",
            "description": "General opportunity-gate models that can support GPT/macro decisions without replacing GPT.",
            "account_specific_sources": {"operator_weekend_gpt_support_v1"},
            "queue_filter": lambda spec: (
                (
                    not spec.get("deployment_lane")
                    and str(spec.get("dataset_kind") or "") == "profitable_move_precursor"
                )
                or spec.get("deployment_lane") == "gpt_live_model_support"
                or str(spec.get("source") or "") == "operator_weekend_gpt_support_v1"
            ),
            "readiness_filter": lambda row: (
                (
                    not row.get("deployment_lane")
                    and str(row.get("dataset_kind") or "") == "profitable_move_precursor"
                )
                or row.get("deployment_lane") == "gpt_live_model_support"
                or str(row.get("source") or "") == "operator_weekend_gpt_support_v1"
            ),
        },
        {
            "account_lane": "tech_live_champion",
            "account_focus": "technical_live_001-001-21715580-003",
            "deployment_lane": "live_tech_champion_candidate",
            "description": "Strict technical champion candidates for broad/return-curve/major-move evidence.",
            "account_specific_sources": {"operator_weekend_tech_improvement_v1"},
            "queue_filter": lambda spec: spec.get("deployment_lane") == "live_tech_champion_candidate",
            "readiness_filter": lambda row: row.get("deployment_lane") == "live_tech_champion_candidate",
        },
        {
            "account_lane": "primary_live_challenger",
            "account_focus": "primary_live_001-001-21715580-002",
            "deployment_lane": "live_primary_challenger_candidate",
            "description": "Primary challenger/scout candidates focused on volatile/non-USD/pair-family movement.",
            "account_specific_sources": {"operator_weekend_primary_improvement_v1"},
            "queue_filter": lambda spec: spec.get("deployment_lane") == "live_primary_challenger_candidate",
            "readiness_filter": lambda row: row.get("deployment_lane") == "live_primary_challenger_candidate",
        },
    ]

    rows: List[Dict[str, Any]] = []
    for lane in lanes:
        lane_queue_items: List[Dict[str, Any]] = []
        for item in queue_rows:
            spec = item.get("spec") if isinstance(item.get("spec"), dict) else {}
            if lane["queue_filter"](spec):
                lane_queue_items.append(item)
        lane_specs = [
            item.get("spec")
            for item in lane_queue_items
            if isinstance(item.get("spec"), dict)
        ]
        lane_pending = [
            item
            for item in lane_queue_items
            if str(item.get("spec_hash") or "") not in completed_hashes
        ]
        lane_readiness = [
            row for row in readiness_rows if lane["readiness_filter"](row)
        ]
        lane_specialists = [
            row
            for row in lane_readiness
            if safe_bool(row.get("specialist_validation_grade"))
            or str(row.get("specialist_profile") or "")
            or str(row.get("instrument_whitelist") or "")
        ]
        action_counts = Counter(str(row.get("next_action") or "") for row in lane_readiness)
        target_counts = Counter(str(spec.get("target") or "") for spec in lane_specs)
        model_counts = Counter(str(spec.get("model_type") or "") for spec in lane_specs)
        subset_counts = Counter(str(spec.get("instrument_subset") or "") for spec in lane_specs)
        specialist_specs = sum(
            1
            for spec in lane_specs
            if spec.get("instrument_whitelist")
        )
        top_candidate = lane_readiness[0] if lane_readiness else {}
        top_specialist = lane_specialists[0] if lane_specialists else {}
        edge_pool = [
            row
            for row in lane_readiness
            if str(row.get("evaluation_stage") or "") == "validation"
            and safe_bool(row.get("gate_passed"))
            and safe_float(row.get("mean_net_pips")) > 0
        ]
        edge_pool.sort(
            key=lambda row: (
                safe_float(row.get("mean_net_pips")),
                safe_float(row.get("bootstrap_lower_mean_net_pips")),
                safe_float(row.get("trades")),
                safe_float(row.get("readiness_score")),
            ),
            reverse=True,
        )
        top_edge_candidate = edge_pool[0] if edge_pool else {}
        account_specific_sources = set(lane.get("account_specific_sources") or set())
        account_specific_pool = [
            row
            for row in lane_readiness
            if str(row.get("source") or "") in account_specific_sources
            and str(row.get("evaluation_stage") or "") == "validation"
            and safe_bool(row.get("gate_passed"))
            and safe_float(row.get("mean_net_pips")) > 0
        ]
        account_specific_pool.sort(
            key=lambda row: (
                safe_bool(row.get("validation_grade")),
                safe_bool(row.get("specialist_validation_grade")),
                safe_float(row.get("readiness_score")),
                safe_float(row.get("mean_net_pips")),
                safe_float(row.get("trades")),
            ),
            reverse=True,
        )
        top_account_specific = account_specific_pool[0] if account_specific_pool else {}
        calibration_detection_pool = [
            row
            for row in lane_readiness
            if safe_bool(row.get("calibration_detection_followup"))
        ]
        calibration_detection_pool.sort(
            key=lambda row: (
                safe_float(row.get("readiness_score")),
                safe_float(row.get("mean_net_pips")),
                safe_float(row.get("trades")),
            ),
            reverse=True,
        )
        top_calibration_detection = (
            calibration_detection_pool[0] if calibration_detection_pool else {}
        )
        validation_grade = sum(1 for row in lane_readiness if safe_bool(row.get("validation_grade")))
        specialist_validation_grade = sum(
            1
            for row in lane_readiness
            if safe_bool(row.get("specialist_validation_grade"))
        )
        robust_followup = sum(1 for row in lane_readiness if safe_bool(row.get("needs_robust_followup")))
        calibration_detection_followup = sum(
            1
            for row in lane_readiness
            if safe_bool(row.get("calibration_detection_followup"))
        )
        completed_specs = max(0, len(lane_specs) - len(lane_pending))
        coverage_completed_pct = (
            round((completed_specs / len(lane_specs)) * 100.0, 3)
            if lane_specs
            else 0.0
        )
        if not lane_specs:
            next_action = "seed_account_specific_model_specs"
        elif validation_grade:
            next_action = "review_validation_grade_for_shadow_or_canary"
        elif robust_followup:
            next_action = "queue_or_run_robust_followups"
        elif calibration_detection_followup:
            next_action = "queue_calibration_detection_followups"
        elif lane_pending:
            next_action = "wait_for_trainer_to_process_pending_specs"
        else:
            next_action = "expand_model_space_for_lane"
        rows.append(
            {
                "account_lane": lane["account_lane"],
                "account_focus": lane["account_focus"],
                "deployment_lane": lane["deployment_lane"],
                "description": lane["description"],
                "queue_specs": len(lane_specs),
                "completed_specs": completed_specs,
                "pending_specs": len(lane_pending),
                "coverage_completed_pct": coverage_completed_pct,
                "readiness_rows": len(lane_readiness),
                "validation_grade_candidates": validation_grade,
                "specialist_validation_grade_candidates": specialist_validation_grade,
                "robust_followup_candidates": robust_followup,
                "calibration_detection_followup_candidates": calibration_detection_followup,
                "specialist_specs": specialist_specs,
                "rejected_or_gate_failed": action_counts.get("rejected_or_gate_failed", 0),
                "calibration_detection_followups": action_counts.get("calibration_detection_followup", 0),
                "needs_more_sample_or_longer_holdout": action_counts.get("needs_more_sample_or_longer_holdout", 0),
                "top_targets": json.dumps(top_counter(target_counts, 8), default=str),
                "top_models": json.dumps(top_counter(model_counts, 8), default=str),
                "top_subsets": json.dumps(top_counter(subset_counts, 8), default=str),
                "top_candidate_experiment_id": top_candidate.get("experiment_id", ""),
                "top_candidate_model_type": top_candidate.get("model_type", ""),
                "top_candidate_target": top_candidate.get("target", ""),
                "top_candidate_subset": top_candidate.get("instrument_subset", ""),
                "top_candidate_instrument_whitelist": top_candidate.get("instrument_whitelist", ""),
                "top_candidate_segment_filters": top_candidate.get("segment_filters", ""),
                "top_candidate_specialist_profile": top_candidate.get("specialist_profile", ""),
                "top_candidate_stage": top_candidate.get("evaluation_stage", ""),
                "top_candidate_readiness_score": top_candidate.get("readiness_score", ""),
                "top_candidate_mean_net_pips": top_candidate.get("mean_net_pips", ""),
                "top_candidate_mean_auc": top_candidate.get("mean_auc", ""),
                "top_edge_validation_experiment_id": top_edge_candidate.get("experiment_id", ""),
                "top_edge_validation_model_type": top_edge_candidate.get("model_type", ""),
                "top_edge_validation_target": top_edge_candidate.get("target", ""),
                "top_edge_validation_subset": top_edge_candidate.get("instrument_subset", ""),
                "top_edge_validation_segment_filters": top_edge_candidate.get("segment_filters", ""),
                "top_edge_validation_readiness_score": top_edge_candidate.get("readiness_score", ""),
                "top_edge_validation_mean_net_pips": top_edge_candidate.get("mean_net_pips", ""),
                "top_edge_validation_trades": top_edge_candidate.get("trades", ""),
                "top_edge_validation_positive_weeks": top_edge_candidate.get("positive_weeks", ""),
                "top_edge_validation_mean_auc": top_edge_candidate.get("mean_auc", ""),
                "top_account_specific_validation_experiment_id": top_account_specific.get("experiment_id", ""),
                "top_account_specific_validation_source": top_account_specific.get("source", ""),
                "top_account_specific_validation_model_type": top_account_specific.get("model_type", ""),
                "top_account_specific_validation_target": top_account_specific.get("target", ""),
                "top_account_specific_validation_subset": top_account_specific.get("instrument_subset", ""),
                "top_account_specific_validation_segment_filters": top_account_specific.get("segment_filters", ""),
                "top_account_specific_validation_readiness_score": top_account_specific.get("readiness_score", ""),
                "top_account_specific_validation_mean_net_pips": top_account_specific.get("mean_net_pips", ""),
                "top_account_specific_validation_trades": top_account_specific.get("trades", ""),
                "top_account_specific_validation_positive_weeks": top_account_specific.get("positive_weeks", ""),
                "top_account_specific_validation_mean_auc": top_account_specific.get("mean_auc", ""),
                "top_calibration_detection_experiment_id": top_calibration_detection.get("experiment_id", ""),
                "top_calibration_detection_model_type": top_calibration_detection.get("model_type", ""),
                "top_calibration_detection_target": top_calibration_detection.get("target", ""),
                "top_calibration_detection_instrument_whitelist": top_calibration_detection.get("instrument_whitelist", ""),
                "top_calibration_detection_gate_failed_reasons": top_calibration_detection.get("gate_failed_reasons", ""),
                "top_calibration_detection_readiness_score": top_calibration_detection.get("readiness_score", ""),
                "top_calibration_detection_mean_net_pips": top_calibration_detection.get("mean_net_pips", ""),
                "top_calibration_detection_mean_auc": top_calibration_detection.get("mean_auc", ""),
                "top_calibration_detection_min_auc": top_calibration_detection.get("minimum_week_auc", ""),
                "top_calibration_detection_mean_brier_skill": top_calibration_detection.get("mean_brier_skill", ""),
                "top_specialist_experiment_id": top_specialist.get("experiment_id", ""),
                "top_specialist_target": top_specialist.get("target", ""),
                "top_specialist_instrument_whitelist": top_specialist.get("instrument_whitelist", ""),
                "top_specialist_segment_filters": top_specialist.get("segment_filters", ""),
                "top_specialist_profile": top_specialist.get("specialist_profile", ""),
                "top_specialist_stage": top_specialist.get("evaluation_stage", ""),
                "top_specialist_readiness_score": top_specialist.get("readiness_score", ""),
                "top_specialist_mean_net_pips": top_specialist.get("mean_net_pips", ""),
                "top_specialist_mean_auc": top_specialist.get("mean_auc", ""),
                "next_research_action": next_action,
            }
        )

    arima_shadow = read_json(PROMOTIONS_ROOT / "arima_pair_challenger_shadow.json", {})
    arima_candidates = arima_shadow.get("arima_challengers") if isinstance(arima_shadow, dict) else []
    if not isinstance(arima_candidates, list):
        arima_candidates = []
    rows.append(
        {
            "account_lane": "canary_paper_validation",
            "account_focus": "canary_or_paper_validation_accounts",
            "deployment_lane": "arima_pair_challenger_shadow",
            "description": "Canary/paper validation lane for ARIMA/pair challengers and shadow candidates.",
            "queue_specs": 0,
            "completed_specs": 0,
            "pending_specs": 0,
            "coverage_completed_pct": 0.0,
            "readiness_rows": 0,
            "validation_grade_candidates": safe_int(arima_shadow.get("robust_arima_challenger_count"), 0),
            "specialist_validation_grade_candidates": 0,
            "robust_followup_candidates": len(arima_candidates),
            "calibration_detection_followup_candidates": 0,
            "specialist_specs": 0,
            "rejected_or_gate_failed": 0,
            "calibration_detection_followups": 0,
            "needs_more_sample_or_longer_holdout": 0,
            "top_targets": "[]",
            "top_models": json.dumps(top_counter(Counter(str(row.get("arima_model") or "") for row in arima_candidates), 8), default=str),
            "top_subsets": json.dumps(top_counter(Counter(str(row.get("pair") or "") for row in arima_candidates), 8), default=str),
            "top_candidate_experiment_id": arima_shadow.get("source_experiment_id", ""),
            "top_candidate_model_type": "arima_pair_challenger",
            "top_candidate_target": arima_shadow.get("source_target", ""),
            "top_candidate_subset": "pair_level",
            "top_candidate_instrument_whitelist": "",
            "top_candidate_segment_filters": "",
            "top_candidate_specialist_profile": "",
            "top_candidate_stage": arima_shadow.get("stage", ""),
            "top_candidate_readiness_score": "",
            "top_candidate_mean_net_pips": arima_candidates[0].get("arima_mean_net_pips", "") if arima_candidates else "",
            "top_candidate_mean_auc": "",
            "top_edge_validation_experiment_id": "",
            "top_edge_validation_model_type": "",
            "top_edge_validation_target": "",
            "top_edge_validation_subset": "",
            "top_edge_validation_segment_filters": "",
            "top_edge_validation_readiness_score": "",
            "top_edge_validation_mean_net_pips": "",
            "top_edge_validation_trades": "",
            "top_edge_validation_positive_weeks": "",
            "top_edge_validation_mean_auc": "",
            "top_account_specific_validation_experiment_id": "",
            "top_account_specific_validation_source": "",
            "top_account_specific_validation_model_type": "",
            "top_account_specific_validation_target": "",
            "top_account_specific_validation_subset": "",
            "top_account_specific_validation_segment_filters": "",
            "top_account_specific_validation_readiness_score": "",
            "top_account_specific_validation_mean_net_pips": "",
            "top_account_specific_validation_trades": "",
            "top_account_specific_validation_positive_weeks": "",
            "top_account_specific_validation_mean_auc": "",
            "top_calibration_detection_experiment_id": "",
            "top_calibration_detection_model_type": "",
            "top_calibration_detection_target": "",
            "top_calibration_detection_instrument_whitelist": "",
            "top_calibration_detection_gate_failed_reasons": "",
            "top_calibration_detection_readiness_score": "",
            "top_calibration_detection_mean_net_pips": "",
            "top_calibration_detection_mean_auc": "",
            "top_calibration_detection_min_auc": "",
            "top_calibration_detection_mean_brier_skill": "",
            "top_specialist_experiment_id": "",
            "top_specialist_target": "",
            "top_specialist_instrument_whitelist": "",
            "top_specialist_segment_filters": "",
            "top_specialist_profile": "",
            "top_specialist_stage": "",
            "top_specialist_readiness_score": "",
            "top_specialist_mean_net_pips": "",
            "top_specialist_mean_auc": "",
            "next_research_action": (
                "review_arima_canary_assignment"
                if safe_bool(arima_shadow.get("canary_assignment_ready"))
                else "continue_shadow_validation"
            ),
        }
    )

    write_csv_rows(
        ACCOUNT_MODEL_IMPROVEMENT_CSV,
        rows,
        [
            "account_lane",
            "account_focus",
            "deployment_lane",
            "description",
            "queue_specs",
            "completed_specs",
            "pending_specs",
            "coverage_completed_pct",
            "readiness_rows",
            "validation_grade_candidates",
            "specialist_validation_grade_candidates",
            "robust_followup_candidates",
            "calibration_detection_followup_candidates",
            "specialist_specs",
            "rejected_or_gate_failed",
            "calibration_detection_followups",
            "needs_more_sample_or_longer_holdout",
            "top_targets",
            "top_models",
            "top_subsets",
            "top_candidate_experiment_id",
            "top_candidate_model_type",
            "top_candidate_target",
            "top_candidate_subset",
            "top_candidate_instrument_whitelist",
            "top_candidate_segment_filters",
            "top_candidate_specialist_profile",
            "top_candidate_stage",
            "top_candidate_readiness_score",
            "top_candidate_mean_net_pips",
            "top_candidate_mean_auc",
            "top_edge_validation_experiment_id",
            "top_edge_validation_model_type",
            "top_edge_validation_target",
            "top_edge_validation_subset",
            "top_edge_validation_segment_filters",
            "top_edge_validation_readiness_score",
            "top_edge_validation_mean_net_pips",
            "top_edge_validation_trades",
            "top_edge_validation_positive_weeks",
            "top_edge_validation_mean_auc",
            "top_account_specific_validation_experiment_id",
            "top_account_specific_validation_source",
            "top_account_specific_validation_model_type",
            "top_account_specific_validation_target",
            "top_account_specific_validation_subset",
            "top_account_specific_validation_segment_filters",
            "top_account_specific_validation_readiness_score",
            "top_account_specific_validation_mean_net_pips",
            "top_account_specific_validation_trades",
            "top_account_specific_validation_positive_weeks",
            "top_account_specific_validation_mean_auc",
            "top_calibration_detection_experiment_id",
            "top_calibration_detection_model_type",
            "top_calibration_detection_target",
            "top_calibration_detection_instrument_whitelist",
            "top_calibration_detection_gate_failed_reasons",
            "top_calibration_detection_readiness_score",
            "top_calibration_detection_mean_net_pips",
            "top_calibration_detection_mean_auc",
            "top_calibration_detection_min_auc",
            "top_calibration_detection_mean_brier_skill",
            "top_specialist_experiment_id",
            "top_specialist_target",
            "top_specialist_instrument_whitelist",
            "top_specialist_segment_filters",
            "top_specialist_profile",
            "top_specialist_stage",
            "top_specialist_readiness_score",
            "top_specialist_mean_net_pips",
            "top_specialist_mean_auc",
            "next_research_action",
        ],
    )
    summary = {
        "rows": len(rows),
        "output_csv": str(ACCOUNT_MODEL_IMPROVEMENT_CSV),
        "lanes": rows,
    }
    return summary, rows


def extract_shadow_action_row(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = parse_json_obj(row.get("raw_json"))
    reason = row.get("reason", "")
    reject_reason = row.get("reject_reason", "")
    status = row.get("status", "")
    age_value = find_key(raw, "event_age_minutes")
    age = safe_float(age_value, math.nan) if age_value is not None else None
    if age is not None and (math.isnan(age) or math.isinf(age)):
        age = None
    if age is None:
        age = age_from_reason(reject_reason or reason)
    promoted_decision = find_first_dict_with_key(raw, "_promoted_model_decision")
    ensemble_decision = find_first_dict_with_key(raw, "_ensemble_shadow_decision")
    if not ensemble_decision and isinstance(promoted_decision.get("ensemble_shadow"), dict):
        ensemble_decision = promoted_decision.get("ensemble_shadow", {})
    members = ensemble_decision.get("members") if isinstance(ensemble_decision, dict) else []
    event_key = find_key(raw, "event_key") or ""
    signal_theme = find_key(raw, "theme") or ""
    if not signal_theme:
        theme_match = re.search(r"\bevent scout ([A-Z0-9_]+)\b", reason or "")
        if theme_match:
            signal_theme = theme_match.group(1)
    score = score_from_reason(reject_reason or reason)
    if score is None:
        score = find_key(raw, "event_score") or find_key(raw, "score")
    return {
        "time_utc": row.get("time_utc", ""),
        "time_ny": row.get("time_ny", ""),
        "action_type": row.get("action_type", ""),
        "status": status,
        "instrument": row.get("instrument", ""),
        "direction": row.get("direction", ""),
        "risk_pct": row.get("risk_pct", ""),
        "reason": reason,
        "reject_reason": reject_reason,
        "reason_class": classify_reason(status, reason, reject_reason),
        "event_age_minutes": "" if age is None else rounded(age, 3),
        "age_bin": age_bin(age),
        "event_score": "" if score is None else rounded(score, 3),
        "event_key": event_key,
        "theme": signal_theme,
        "promoted_experiment_id": promoted_decision.get("experiment_id", ""),
        "promoted_candidate_id": promoted_decision.get("candidate_id", ""),
        "promoted_reason": promoted_decision.get("reason", ""),
        "ensemble_experiment_id": ensemble_decision.get("experiment_id", ""),
        "ensemble_probability": rounded(ensemble_decision.get("probability", ""), 5)
        if ensemble_decision
        else "",
        "ensemble_threshold": rounded(ensemble_decision.get("threshold", ""), 5)
        if ensemble_decision
        else "",
        "ensemble_approved": ensemble_decision.get("shadow_approved", "")
        if ensemble_decision
        else "",
        "ensemble_member_count": len(members) if isinstance(members, list) else "",
    }


def build_live_shadow_and_timing_reports() -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    action_rows = tail_csv_rows(TECH_ACTIONS_PATH, 1500)
    scan_rows = tail_csv_rows(TECH_EVENT_SCAN_PATH, 400)
    signal_rows = tail_csv_rows(TECH_EVENT_SIGNALS_PATH, 300)
    decision_rows = [extract_shadow_action_row(row) for row in action_rows]
    decision_rows = decision_rows[-1000:]

    status_counter = Counter(row.get("status", "") for row in decision_rows)
    reason_counter = Counter(row.get("reason_class", "") for row in decision_rows)
    instrument_counter = Counter(row.get("instrument", "") for row in decision_rows if row.get("instrument"))
    theme_counter = Counter(row.get("theme", "") for row in decision_rows if row.get("theme"))
    signal_theme_counter = Counter(row.get("theme", "") for row in signal_rows if row.get("theme"))
    top_reject_counter = Counter(
        row.get("top_reject_reason", "") for row in scan_rows if row.get("top_reject_reason")
    )

    numeric_scan_totals: Dict[str, float] = {}
    for col in [
        "candidate_windows",
        "signals",
        "triggered_themes",
        "pressure_watch",
        "pressure_trade",
        "exhaustion_shadow",
        "exhaustion_hybrid",
        "scout_attempts",
        "below_pips",
        "below_ratio",
        "spread_rejected",
    ]:
        numeric_scan_totals[col] = round(sum(safe_float(row.get(col), 0.0) for row in scan_rows), 4)

    write_csv_rows(
        LIVE_SHADOW_CSV,
        decision_rows,
        [
            "time_utc",
            "time_ny",
            "action_type",
            "status",
            "instrument",
            "direction",
            "risk_pct",
            "reason",
            "reject_reason",
            "reason_class",
            "event_age_minutes",
            "age_bin",
            "event_score",
            "event_key",
            "theme",
            "promoted_experiment_id",
            "promoted_candidate_id",
            "promoted_reason",
            "ensemble_experiment_id",
            "ensemble_probability",
            "ensemble_threshold",
            "ensemble_approved",
            "ensemble_member_count",
        ],
    )

    timing_groups: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for row in decision_rows:
        key = (
            row.get("action_type", ""),
            row.get("status", ""),
            row.get("reason_class", ""),
            row.get("age_bin", ""),
        )
        group = timing_groups.setdefault(
            key,
            {
                "action_type": key[0],
                "status": key[1],
                "reason_class": key[2],
                "age_bin": key[3],
                "count": 0,
                "age_sum": 0.0,
                "age_count": 0,
                "score_sum": 0.0,
                "score_count": 0,
            },
        )
        group["count"] += 1
        age = safe_float(row.get("event_age_minutes"), math.nan)
        if not math.isnan(age):
            group["age_sum"] += age
            group["age_count"] += 1
        score = safe_float(row.get("event_score"), math.nan)
        if not math.isnan(score):
            group["score_sum"] += score
            group["score_count"] += 1

    timing_rows: List[Dict[str, Any]] = []
    for group in timing_groups.values():
        timing_rows.append(
            {
                "action_type": group["action_type"],
                "status": group["status"],
                "reason_class": group["reason_class"],
                "age_bin": group["age_bin"],
                "count": group["count"],
                "avg_event_age_minutes": rounded(
                    group["age_sum"] / group["age_count"], 3
                )
                if group["age_count"]
                else "",
                "avg_event_score": rounded(group["score_sum"] / group["score_count"], 3)
                if group["score_count"]
                else "",
            }
        )
    timing_rows.sort(key=lambda row: safe_int(row.get("count")), reverse=True)
    write_csv_rows(
        ENTRY_TIMING_CSV,
        timing_rows,
        [
            "action_type",
            "status",
            "reason_class",
            "age_bin",
            "count",
            "avg_event_age_minutes",
            "avg_event_score",
        ],
    )

    latest_scan = scan_rows[-1] if scan_rows else {}
    live_summary = {
        "actions_scanned": len(action_rows),
        "scan_rows_scanned": len(scan_rows),
        "signal_rows_scanned": len(signal_rows),
        "latest_scan_time_utc": latest_scan.get("time_utc", ""),
        "latest_scan_top_candidate": latest_scan.get("top_candidate", ""),
        "latest_scan_top_reject_reason": latest_scan.get("top_reject_reason", ""),
        "recent_scan_totals": numeric_scan_totals,
        "action_status_counts": top_counter(status_counter, 20),
        "reason_class_counts": top_counter(reason_counter, 20),
        "top_action_instruments": top_counter(instrument_counter, 20),
        "top_action_themes": top_counter(theme_counter, 20),
        "top_signal_themes": top_counter(signal_theme_counter, 20),
        "top_scan_reject_reasons": top_counter(top_reject_counter, 10),
        "output_csv": str(LIVE_SHADOW_CSV),
    }
    timing_summary = {
        "timing_groups": len(timing_rows),
        "top_timing_groups": timing_rows[:20],
        "output_csv": str(ENTRY_TIMING_CSV),
    }
    return live_summary, decision_rows, timing_summary, timing_rows


def build_portfolio_margin_proxy() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    monitor_rows = tail_csv_rows(TECH_MONITOR_PATH, 1000)
    position_rows = tail_csv_rows(TECH_POSITION_HEALTH_PATH, 1000)
    compound_rows = tail_csv_rows(TECH_COMPOUND_ATTEMPTS_PATH, 1000)
    action_rows = tail_csv_rows(TECH_ACTIONS_PATH, 1000)

    margin_values = [safe_float(row.get("margin_used_pct"), math.nan) for row in monitor_rows]
    margin_values = [value for value in margin_values if not math.isnan(value)]
    open_trade_values = [
        safe_float(row.get("open_trade_count"), math.nan) for row in monitor_rows
    ]
    open_trade_values = [value for value in open_trade_values if not math.isnan(value)]
    nav_values = [safe_float(row.get("nav"), math.nan) for row in monitor_rows]
    nav_values = [value for value in nav_values if not math.isnan(value)]
    last_monitor = monitor_rows[-1] if monitor_rows else {}

    health_action_counter = Counter(
        row.get("health_action", "") for row in position_rows if row.get("health_action")
    )
    latest_by_trade: Dict[str, Dict[str, Any]] = {}
    for row in position_rows:
        trade_id = row.get("trade_id")
        if trade_id:
            latest_by_trade[trade_id] = row
    compound_allowed_counter = Counter(str(row.get("allowed", "")) for row in compound_rows)
    compound_reason_counter = Counter(row.get("reason", "") for row in compound_rows if row.get("reason"))
    action_status_counter = Counter(row.get("status", "") for row in action_rows)
    risk_values = [safe_float(row.get("risk_pct"), math.nan) for row in action_rows]
    risk_values = [value for value in risk_values if not math.isnan(value) and value > 0]

    metric_rows = [
        {"section": "monitor", "metric": "latest_time_utc", "value": last_monitor.get("time_utc", "")},
        {"section": "monitor", "metric": "latest_nav", "value": rounded(last_monitor.get("nav", ""), 4)},
        {"section": "monitor", "metric": "latest_balance", "value": rounded(last_monitor.get("balance", ""), 4)},
        {"section": "monitor", "metric": "latest_margin_used_pct", "value": rounded(last_monitor.get("margin_used_pct", ""), 4)},
        {"section": "monitor", "metric": "max_margin_used_pct_recent", "value": rounded(max(margin_values), 4) if margin_values else ""},
        {"section": "monitor", "metric": "avg_margin_used_pct_recent", "value": rounded(mean(margin_values), 4) if margin_values else ""},
        {"section": "monitor", "metric": "latest_open_trade_count", "value": rounded(last_monitor.get("open_trade_count", ""), 2)},
        {"section": "monitor", "metric": "max_open_trade_count_recent", "value": rounded(max(open_trade_values), 2) if open_trade_values else ""},
        {"section": "monitor", "metric": "min_nav_recent", "value": rounded(min(nav_values), 4) if nav_values else ""},
        {"section": "monitor", "metric": "max_nav_recent", "value": rounded(max(nav_values), 4) if nav_values else ""},
        {"section": "position_health", "metric": "unique_recent_trade_ids", "value": len(latest_by_trade)},
        {"section": "actions", "metric": "avg_positive_risk_pct_recent", "value": rounded(mean(risk_values), 4) if risk_values else ""},
    ]
    for key, count in health_action_counter.most_common(12):
        metric_rows.append({"section": "position_health", "metric": f"health_action:{key}", "value": count})
    for key, count in compound_allowed_counter.most_common(8):
        metric_rows.append({"section": "compound_attempts", "metric": f"allowed:{key}", "value": count})
    for key, count in compound_reason_counter.most_common(12):
        metric_rows.append({"section": "compound_attempts", "metric": f"reason:{key[:80]}", "value": count})
    for key, count in action_status_counter.most_common(12):
        metric_rows.append({"section": "actions", "metric": f"status:{key}", "value": count})

    write_csv_rows(PORTFOLIO_MARGIN_CSV, metric_rows, ["section", "metric", "value"])
    summary = {
        "monitor_rows_scanned": len(monitor_rows),
        "position_health_rows_scanned": len(position_rows),
        "compound_attempt_rows_scanned": len(compound_rows),
        "action_rows_scanned": len(action_rows),
        "latest_margin_used_pct": rounded(last_monitor.get("margin_used_pct", ""), 4),
        "max_margin_used_pct_recent": rounded(max(margin_values), 4) if margin_values else "",
        "latest_open_trade_count": rounded(last_monitor.get("open_trade_count", ""), 2),
        "max_open_trade_count_recent": rounded(max(open_trade_values), 2) if open_trade_values else "",
        "health_action_counts": top_counter(health_action_counter, 12),
        "compound_allowed_counts": top_counter(compound_allowed_counter, 8),
        "action_status_counts": top_counter(action_status_counter, 12),
        "output_csv": str(PORTFOLIO_MARGIN_CSV),
    }
    return summary, metric_rows


def manifest_artifact_exists(path_value: Any) -> bool:
    if not path_value:
        return False
    try:
        return Path(str(path_value)).exists()
    except Exception:
        return False


def compact_allowed_segments(data: Dict[str, Any]) -> str:
    segments = data.get("allowed_segments")
    if not isinstance(segments, dict):
        segments = {
            "allowed_pair_families": data.get("allowed_pair_families", []),
            "allowed_regimes": data.get("allowed_regimes", []),
            "allowed_sessions": data.get("allowed_sessions", []),
        }
    try:
        return json.dumps(segments, sort_keys=True, default=str)[:1000]
    except Exception:
        return ""


def promotion_row_from_manifest(path: Path, data: Dict[str, Any]) -> Dict[str, Any]:
    validation = data.get("validation") if isinstance(data.get("validation"), dict) else {}
    production_gate = data.get("production_gate") if isinstance(data.get("production_gate"), dict) else {}
    benchmark = data.get("benchmark_comparison") if isinstance(data.get("benchmark_comparison"), dict) else {}
    arima = data.get("arima_challenger_report") if isinstance(data.get("arima_challenger_report"), dict) else {}
    return {
        "manifest": path.name,
        "row_type": "manifest",
        "mtime_utc": file_mtime_iso(path),
        "activation_effective": data.get("activation_effective", ""),
        "auto_promotion_enabled": data.get("auto_promotion_enabled", ""),
        "execution_enabled": data.get("execution_enabled", ""),
        "stage": data.get("stage", ""),
        "experiment_id": data.get("experiment_id", ""),
        "candidate_id": data.get("candidate_id", ""),
        "model_type": data.get("model_type", ""),
        "target": data.get("target", ""),
        "outcome": data.get("outcome", ""),
        "feature_set": data.get("feature_set", ""),
        "instrument_subset": data.get("instrument_subset", ""),
        "research_score": rounded(data.get("research_score", ""), 4),
        "selected_threshold": rounded(data.get("selected_threshold", ""), 5),
        "member_count": len(data.get("members") or []),
        "challenger_count": data.get("challenger_count", ""),
        "robust_arima_challenger_count": data.get("robust_arima_challenger_count", ""),
        "gate_passed": production_gate.get("passed", validation.get("passed", "")),
        "candidate_beats_arima": benchmark.get("candidate_beats_arima", ""),
        "arima_beats_current_pair_count": arima.get("arima_beats_current_pair_count", ""),
        "current_beats_arima_pair_count": arima.get("current_beats_arima_pair_count", ""),
        "model_artifact_path": data.get("model_artifact_path", ""),
        "artifact_exists": manifest_artifact_exists(data.get("model_artifact_path")),
        "allowed_segments": compact_allowed_segments(data),
        "reason": data.get("reason", data.get("promotion_note", "")),
    }


def build_ensemble_evidence_report() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    for name in PROMOTION_MANIFESTS:
        path = PROMOTIONS_ROOT / name
        data = read_json(path, {})
        if not isinstance(data, dict) or not data:
            continue
        rows.append(promotion_row_from_manifest(path, data))
        members = data.get("members")
        if isinstance(members, list):
            for idx, member in enumerate(members, start=1):
                if not isinstance(member, dict):
                    continue
                rows.append(
                    {
                        "manifest": path.name,
                        "row_type": f"ensemble_member_{idx}",
                        "mtime_utc": file_mtime_iso(path),
                        "activation_effective": data.get("activation_effective", ""),
                        "auto_promotion_enabled": data.get("auto_promotion_enabled", ""),
                        "execution_enabled": data.get("execution_enabled", ""),
                        "stage": data.get("stage", ""),
                        "experiment_id": member.get("experiment_id", ""),
                        "candidate_id": member.get("candidate_id", ""),
                        "model_type": member.get("model_type", ""),
                        "target": member.get("target", ""),
                        "outcome": member.get("outcome", ""),
                        "feature_set": member.get("feature_set", ""),
                        "instrument_subset": member.get("instrument_subset", ""),
                        "research_score": rounded(member.get("research_score", member.get("weight", "")), 4),
                        "selected_threshold": rounded(member.get("selected_threshold", member.get("threshold", "")), 5),
                        "member_count": "",
                        "challenger_count": "",
                        "robust_arima_challenger_count": "",
                        "gate_passed": member.get("gate_passed", ""),
                        "candidate_beats_arima": "",
                        "arima_beats_current_pair_count": "",
                        "current_beats_arima_pair_count": "",
                        "model_artifact_path": member.get("model_artifact_path", ""),
                        "artifact_exists": manifest_artifact_exists(member.get("model_artifact_path")),
                        "allowed_segments": "",
                        "reason": member.get("reason", ""),
                    }
                )
        challengers = data.get("arima_challengers") or data.get("robust_arima_challengers")
        if isinstance(challengers, list):
            for idx, challenger in enumerate(challengers[:25], start=1):
                if not isinstance(challenger, dict):
                    continue
                rows.append(
                    {
                        "manifest": path.name,
                        "row_type": f"arima_challenger_{idx}",
                        "mtime_utc": file_mtime_iso(path),
                        "activation_effective": data.get("activation_effective", ""),
                        "auto_promotion_enabled": data.get("auto_promotion_enabled", ""),
                        "execution_enabled": data.get("execution_enabled", ""),
                        "stage": data.get("stage", ""),
                        "experiment_id": challenger.get("experiment_id", ""),
                        "candidate_id": challenger.get("candidate_id", ""),
                        "model_type": challenger.get("model", challenger.get("model_type", "arima")),
                        "target": challenger.get("instrument", challenger.get("pair", "")),
                        "outcome": challenger.get("horizon_minutes", data.get("horizon_minutes", "")),
                        "feature_set": "",
                        "instrument_subset": "",
                        "research_score": rounded(challenger.get("score", challenger.get("mean_net_pips", "")), 4),
                        "selected_threshold": "",
                        "member_count": "",
                        "challenger_count": "",
                        "robust_arima_challenger_count": "",
                        "gate_passed": challenger.get("gate_passed", ""),
                        "candidate_beats_arima": "",
                        "arima_beats_current_pair_count": "",
                        "current_beats_arima_pair_count": "",
                        "model_artifact_path": challenger.get("model_artifact_path", ""),
                        "artifact_exists": manifest_artifact_exists(challenger.get("model_artifact_path")),
                        "allowed_segments": "",
                        "reason": challenger.get("reason", ""),
                    }
                )

    write_csv_rows(
        ENSEMBLE_EVIDENCE_CSV,
        rows,
        [
            "manifest",
            "row_type",
            "mtime_utc",
            "activation_effective",
            "auto_promotion_enabled",
            "execution_enabled",
            "stage",
            "experiment_id",
            "candidate_id",
            "model_type",
            "target",
            "outcome",
            "feature_set",
            "instrument_subset",
            "research_score",
            "selected_threshold",
            "member_count",
            "challenger_count",
            "robust_arima_challenger_count",
            "gate_passed",
            "candidate_beats_arima",
            "arima_beats_current_pair_count",
            "current_beats_arima_pair_count",
            "model_artifact_path",
            "artifact_exists",
            "allowed_segments",
            "reason",
        ],
    )
    summary = {
        "manifest_rows": len([row for row in rows if row.get("row_type") == "manifest"]),
        "total_rows": len(rows),
        "active_or_effective": [
            {
                "manifest": row.get("manifest", ""),
                "experiment_id": row.get("experiment_id", ""),
                "model_type": row.get("model_type", ""),
                "target": row.get("target", ""),
                "research_score": row.get("research_score", ""),
                "activation_effective": row.get("activation_effective", ""),
                "artifact_exists": row.get("artifact_exists", ""),
            }
            for row in rows
            if row.get("row_type") == "manifest"
            and str(row.get("activation_effective", "")).lower() in {"true", "1", "yes"}
        ],
        "output_csv": str(ENSEMBLE_EVIDENCE_CSV),
    }
    return summary, rows


def write_markdown_summary(payload: Dict[str, Any]) -> None:
    trainer = payload.get("trainer_status", {})
    live = payload.get("live_shadow", {})
    portfolio = payload.get("portfolio_margin_proxy", {})
    queue = payload.get("queue_summary", {})
    ensemble = payload.get("ensemble_evidence", {})
    readiness = payload.get("promotion_readiness", {})
    account_models = payload.get("account_model_improvement", {})
    stopcaps = payload.get("stopcap_comparison", {})
    lines = [
        "# Trainer reporting extensions",
        "",
        f"Generated UTC: {payload.get('generated_utc', '')}",
        "",
        "## Trainer",
        "",
        f"- Heartbeat: {trainer.get('research_heartbeat_utc', '')}",
        f"- Current: {trainer.get('current_spec_source', '')} / {trainer.get('current_spec_model_type', '')} / {trainer.get('current_spec_target', '')}",
        f"- Current profile: {trainer.get('current_spec_specialist_profile', '')}; "
        f"validation={trainer.get('current_spec_validation_profile', '')}; "
        f"whitelist={','.join(str(item) for item in (trainer.get('current_spec_instrument_whitelist') or [])[:8])}",
        f"- Status: {trainer.get('current_experiment_status', '')}; pending queued specs: {trainer.get('pending_queued_specs', '')}",
        "",
        "## Live-shadow scout telemetry",
        "",
        f"- Latest scan: {live.get('latest_scan_time_utc', '')}",
        f"- Top candidate: {live.get('latest_scan_top_candidate', '')}",
        f"- Recent scan totals: {json.dumps(live.get('recent_scan_totals', {}), sort_keys=True)}",
        "",
        "## Portfolio/margin proxy",
        "",
        f"- Latest margin used pct: {portfolio.get('latest_margin_used_pct', '')}",
        f"- Max recent margin used pct: {portfolio.get('max_margin_used_pct_recent', '')}",
        f"- Latest open trades: {portfolio.get('latest_open_trade_count', '')}",
        "",
        "## Queue / ensemble evidence",
        "",
        f"- Queue rows: {queue.get('queue_rows', '')}",
        f"- Promotion manifest rows: {ensemble.get('manifest_rows', '')}",
        f"- Promotion-readiness candidates: validation-grade={readiness.get('validation_grade_count', '')}; "
        f"robust-followup={readiness.get('robust_followup_count', '')}; "
        f"calibration-detection={readiness.get('calibration_detection_followup_count', '')}",
        f"- Account model lanes: {account_models.get('rows', '')}",
        f"- Stop-cap families: {stopcaps.get('families', '')}; rows={stopcaps.get('rows', '')}",
        "",
        "## Output files",
        "",
    ]
    for label, path in payload.get("outputs", {}).items():
        lines.append(f"- {label}: `{path}`")
    LATEST_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LATEST_MD_PATH.with_suffix(LATEST_MD_PATH.suffix + f".{os.getpid()}.tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, LATEST_MD_PATH)


def write_reporting_extensions(project_root: Optional[Path] = None) -> Dict[str, Any]:
    global PROJECT_ROOT, TRAINING_ROOT, RESEARCH_ROOT, EXPERIMENTS_ROOT
    global MODEL_LIFECYCLE_ROOT, PROMOTIONS_ROOT, REPORTS_ROOT, STATE_ROOT
    global TECH_LIVE_ROOT, RESEARCH_STATE_PATH, RESEARCH_ONLY_STATE_PATH
    global RESEARCH_QUEUE_PATH, EXPERIMENT_LEDGER_PATH, TECH_ACTIONS_PATH
    global TECH_EVENT_SCAN_PATH, TECH_EVENT_SIGNALS_PATH, TECH_MONITOR_PATH
    global TECH_POSITION_HEALTH_PATH, TECH_COMPOUND_ATTEMPTS_PATH
    global LATEST_JSON_PATH, LATEST_MD_PATH, SEGMENT_LEADERBOARD_CSV
    global LIVE_SHADOW_CSV, ENTRY_TIMING_CSV, PORTFOLIO_MARGIN_CSV
    global ENSEMBLE_EVIDENCE_CSV, QUEUE_SUMMARY_CSV, PROMOTION_READINESS_CSV
    global ACCOUNT_MODEL_IMPROVEMENT_CSV, STOPCAP_COMPARISON_CSV

    if project_root is not None:
        PROJECT_ROOT = Path(project_root)
        TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
        RESEARCH_ROOT = TRAINING_ROOT / "continuous_research"
        EXPERIMENTS_ROOT = RESEARCH_ROOT / "experiments"
        MODEL_LIFECYCLE_ROOT = TRAINING_ROOT / "model_lifecycle"
        PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
        REPORTS_ROOT = TRAINING_ROOT / "reports"
        STATE_ROOT = TRAINING_ROOT / "state"
        TECH_LIVE_ROOT = (
            PROJECT_ROOT
            / "data"
            / "technical_scout_manager"
            / "account_live_tech_broad_regime_scout"
        )
        RESEARCH_STATE_PATH = RESEARCH_ROOT / "research_state.json"
        RESEARCH_ONLY_STATE_PATH = STATE_ROOT / "research_only_state.json"
        RESEARCH_QUEUE_PATH = MODEL_LIFECYCLE_ROOT / "research_queue.jsonl"
        EXPERIMENT_LEDGER_PATH = RESEARCH_ROOT / "experiment_ledger.csv"
        TECH_ACTIONS_PATH = TECH_LIVE_ROOT / "actions.csv"
        TECH_EVENT_SCAN_PATH = TECH_LIVE_ROOT / "event_scan_summary.csv"
        TECH_EVENT_SIGNALS_PATH = TECH_LIVE_ROOT / "event_signals.csv"
        TECH_MONITOR_PATH = TECH_LIVE_ROOT / "monitor.csv"
        TECH_POSITION_HEALTH_PATH = TECH_LIVE_ROOT / "position_health.csv"
        TECH_COMPOUND_ATTEMPTS_PATH = TECH_LIVE_ROOT / "compound_attempts.csv"
        LATEST_JSON_PATH = REPORTS_ROOT / "latest_trainer_reporting_extensions.json"
        LATEST_MD_PATH = REPORTS_ROOT / "latest_trainer_reporting_extensions.md"
        SEGMENT_LEADERBOARD_CSV = REPORTS_ROOT / "trainer_segment_leaderboard.csv"
        LIVE_SHADOW_CSV = REPORTS_ROOT / "trainer_live_shadow_report.csv"
        ENTRY_TIMING_CSV = REPORTS_ROOT / "trainer_entry_timing_report.csv"
        PORTFOLIO_MARGIN_CSV = REPORTS_ROOT / "trainer_portfolio_margin_proxy.csv"
        ENSEMBLE_EVIDENCE_CSV = REPORTS_ROOT / "trainer_ensemble_evidence_report.csv"
        QUEUE_SUMMARY_CSV = REPORTS_ROOT / "trainer_queue_summary.csv"
        PROMOTION_READINESS_CSV = REPORTS_ROOT / "trainer_promotion_readiness.csv"
        ACCOUNT_MODEL_IMPROVEMENT_CSV = REPORTS_ROOT / "trainer_account_model_improvement.csv"
        STOPCAP_COMPARISON_CSV = REPORTS_ROOT / "trainer_stopcap_comparison.csv"

    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    trainer_status = build_trainer_status()
    queue_summary, _queue_rows = build_queue_summary()
    segment_summary, _segment_rows = build_segment_leaderboard()
    live_shadow, _decision_rows, entry_timing, _timing_rows = build_live_shadow_and_timing_reports()
    portfolio_margin_proxy, _portfolio_rows = build_portfolio_margin_proxy()
    ensemble_evidence, _ensemble_rows = build_ensemble_evidence_report()
    promotion_readiness, _readiness_rows = build_promotion_readiness_report()
    account_model_improvement, _account_model_rows = build_account_model_improvement_report(_readiness_rows)
    stopcap_comparison, _stopcap_rows = build_stopcap_comparison_report()

    payload = {
        "generated_utc": utc_iso(),
        "execution": "reporting_only_no_broker_or_promotion_changes",
        "project_root": str(PROJECT_ROOT),
        "trainer_status": trainer_status,
        "queue_summary": queue_summary,
        "segment_leaders": segment_summary,
        "live_shadow": live_shadow,
        "entry_timing": entry_timing,
        "portfolio_margin_proxy": portfolio_margin_proxy,
        "ensemble_evidence": ensemble_evidence,
        "promotion_readiness": promotion_readiness,
        "account_model_improvement": account_model_improvement,
        "stopcap_comparison": stopcap_comparison,
        "inputs": {
            "research_state": str(RESEARCH_STATE_PATH),
            "research_queue": str(RESEARCH_QUEUE_PATH),
            "experiments_root": str(EXPERIMENTS_ROOT),
            "tech_live_root": str(TECH_LIVE_ROOT),
            "promotions_root": str(PROMOTIONS_ROOT),
        },
        "outputs": {
            "json": str(LATEST_JSON_PATH),
            "markdown": str(LATEST_MD_PATH),
            "queue_summary_csv": str(QUEUE_SUMMARY_CSV),
            "segment_leaderboard_csv": str(SEGMENT_LEADERBOARD_CSV),
            "live_shadow_csv": str(LIVE_SHADOW_CSV),
            "entry_timing_csv": str(ENTRY_TIMING_CSV),
            "portfolio_margin_proxy_csv": str(PORTFOLIO_MARGIN_CSV),
            "ensemble_evidence_csv": str(ENSEMBLE_EVIDENCE_CSV),
            "promotion_readiness_csv": str(PROMOTION_READINESS_CSV),
            "account_model_improvement_csv": str(ACCOUNT_MODEL_IMPROVEMENT_CSV),
            "stopcap_comparison_csv": str(STOPCAP_COMPARISON_CSV),
        },
    }
    atomic_write_json(LATEST_JSON_PATH, payload)
    write_markdown_summary(payload)
    return payload


def main() -> int:
    payload = write_reporting_extensions()
    print(
        json.dumps(
            {
                "status": "completed",
                "generated_utc": payload.get("generated_utc"),
                "outputs": payload.get("outputs"),
                "trainer_status": payload.get("trainer_status"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
