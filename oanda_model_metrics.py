#!/usr/bin/env python3
"""Production-grade model metrics and ARIMA benchmark comparison for OANDA FX.

This module is deliberately broker-free.  It reads local trainer outputs,
promotion manifests, and ARIMA baseline files, then writes auditable reports
used by the research trainer and status tooling.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
REPORTS_ROOT = TRAINING_ROOT / "reports"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
ARIMA_ROOT = TRAINING_ROOT / "arima_baselines"
RESEARCH_LEDGER_PATH = TRAINING_ROOT / "continuous_research" / "experiment_ledger.csv"
LATEST_MODEL_METRICS_JSON = REPORTS_ROOT / "latest_model_metrics.json"
LATEST_MODEL_METRICS_CSV = REPORTS_ROOT / "latest_model_metrics.csv"
LATEST_ARIMA_CHALLENGERS_JSON = REPORTS_ROOT / "latest_arima_challengers.json"
ARIMA_PAIR_CHALLENGER_SHADOW_PATH = PROMOTIONS_ROOT / "arima_pair_challenger_shadow.json"
ARIMA_SHADOW_REPORT_PATH = TRAINING_ROOT / "arima_shadow" / "latest_arima_shadow_report.json"
ARIMA_SHADOW_OBSERVATION_CSV_PATH = TRAINING_ROOT / "arima_shadow" / "arima_shadow_observations.csv"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


ARIMA_SHADOW_AUTO_REFRESH_SECONDS = safe_int(
    os.environ.get("OANDA_ARIMA_SHADOW_AUTO_REFRESH_SECONDS"),
    60 * 60,
)
ARIMA_SHADOW_AUTO_ENABLED = str(
    os.environ.get("OANDA_ARIMA_SHADOW_AUTO", "1")
).strip().lower() not in {"0", "false", "no", "off"}
ARIMA_SHADOW_REQUIRED_PASSES = 2
PRODUCTION_MIN_WALK_FORWARD_FOLDS = 6
PRODUCTION_MIN_SELECTED_TRADES = 75
PRODUCTION_MIN_POSITIVE_WEEK_SHARE = 0.70
ARIMA_ADAPTER_REPORT_PATH = TRAINING_ROOT / "arima_adapter" / "latest_arima_adapter_report.json"
ARIMA_CANARY_CANDIDATE_PENDING_PATH = PROMOTIONS_ROOT / "arima_canary_candidate_pending.json"
ARIMA_ADAPTER_AUTO_REFRESH_SECONDS = safe_int(
    os.environ.get("OANDA_ARIMA_ADAPTER_AUTO_REFRESH_SECONDS"),
    30 * 60,
)
ARIMA_ADAPTER_AUTO_ENABLED = str(
    os.environ.get("OANDA_ARIMA_ADAPTER_AUTO", "1")
).strip().lower() not in {"0", "false", "no", "off"}


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def replace_with_retries(tmp: Path, path: Path, *, attempts: int = 6, delay_seconds: float = 0.25) -> None:
    """Replace a report file, tolerating short-lived Windows file locks."""
    last_exc: PermissionError | None = None
    for attempt in range(max(1, attempts)):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt >= attempts - 1:
                break
            time.sleep(delay_seconds * (attempt + 1))
    if last_exc is not None:
        raise last_exc


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")
    replace_with_retries(tmp, path)


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    replace_with_retries(tmp, path)


def parse_utc_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def seconds_since(value: Any) -> Optional[float]:
    dt = parse_utc_datetime(value)
    if dt is None:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds()


def infer_horizon_minutes(target: Any, outcome: Any = "") -> Optional[int]:
    text = f"{target or ''} {outcome or ''}"
    matches = re.findall(r"(?:^|_|\b)(30|60|120)(?:$|_|\b)", text)
    if not matches:
        return None
    return safe_int(matches[-1], 0) or None


def latest_arima_summary_path() -> Optional[Path]:
    candidates = sorted(
        ARIMA_ROOT.glob("*_summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def latest_arima_results_path() -> Optional[Path]:
    summary_path = latest_arima_summary_path()
    if summary_path:
        summary = read_json(summary_path, {})
        results_csv = summary.get("results_csv") if isinstance(summary, dict) else ""
        if results_csv:
            path = Path(str(results_csv))
            if path.exists():
                return path
    candidates = sorted(
        ARIMA_ROOT.glob("*_results.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def arima_results_paths() -> List[Path]:
    paths = sorted(
        ARIMA_ROOT.glob("*_results.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return paths


def arima_candidate_quality(row: Dict[str, Any]) -> Tuple[int, float, float, float]:
    calibration_pf = safe_float(row.get("calibration_profit_factor"), 0.0)
    calibration_mean = safe_float(row.get("calibration_mean_net_pips"), -1e18)
    calibration_trades = safe_float(row.get("calibration_trades"), 0.0)
    robust = int(calibration_pf >= 1.0 and calibration_mean > 0.0 and calibration_trades >= 25.0)
    return (
        robust,
        safe_float(row.get("mean_net_pips"), -1e18),
        safe_float(row.get("profit_factor"), 0.0),
        safe_float(row.get("trades"), 0.0),
    )


def arima_group_record(
    key: Tuple[str, int, str],
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    pair, horizon, model = key
    values = [safe_float(row.get("mean_net_pips"), 0.0) for row in rows]
    trades = [safe_float(row.get("trades"), 0.0) for row in rows]
    pfs = [safe_float(row.get("profit_factor"), 0.0) for row in rows]
    medians = [safe_float(row.get("median_net_pips"), 0.0) for row in rows]
    win_rates = [safe_float(row.get("win_rate"), 0.0) for row in rows]
    direction_accuracy = [
        safe_float(row.get("direction_accuracy"), 0.0)
        for row in rows
    ]
    cal_means = [
        safe_float(row.get("calibration_mean_net_pips"), 0.0)
        for row in rows
    ]
    cal_pfs = [
        safe_float(row.get("calibration_profit_factor"), 0.0)
        for row in rows
    ]
    cal_trades = [
        safe_float(row.get("calibration_trades"), 0.0)
        for row in rows
    ]
    calibration_qualified_windows = sum(
        1
        for mean, pf, trade_count in zip(cal_means, cal_pfs, cal_trades)
        if mean > 0.0 and pf >= 1.0 and trade_count >= 10.0
    )
    windows = len(rows)
    profitable_windows = sum(1 for value in values if value > 0.0)
    avg_mean = sum(values) / max(windows, 1)
    avg_pf = sum(pfs) / max(windows, 1)
    avg_trades = sum(trades) / max(windows, 1)
    stable = (
        windows >= 2
        and profitable_windows == windows
        and min(values or [0.0]) > 0.0
        and avg_pf >= 1.20
        and avg_trades >= 10.0
    )
    return {
        "pair": pair,
        "horizon_minutes": horizon,
        "model": model,
        "family": "ARIMA",
        "windows": windows,
        "profitable_windows": profitable_windows,
        "min_mean_net_pips": min(values or [0.0]),
        "mean_net_pips": avg_mean,
        "median_net_pips": sum(medians) / max(windows, 1),
        "profit_factor": avg_pf,
        "trades": avg_trades,
        "win_rate": sum(win_rates) / max(windows, 1),
        "direction_accuracy": sum(direction_accuracy) / max(windows, 1),
        "calibration_mean_net_pips": sum(cal_means) / max(windows, 1),
        "calibration_profit_factor": sum(cal_pfs) / max(windows, 1),
        "calibration_trades": sum(cal_trades) / max(windows, 1),
        "calibration_qualified": calibration_qualified_windows > 0,
        "calibration_qualified_windows": calibration_qualified_windows,
        "stable_across_windows": stable,
        "source_csv": ";".join(sorted({
            str(row.get("_source_csv") or "")
            for row in rows
            if row.get("_source_csv")
        })),
    }


def arima_group_quality(record: Dict[str, Any]) -> Tuple[int, int, float, float, float]:
    return (
        int(bool(record.get("stable_across_windows"))),
        safe_int(record.get("calibration_qualified_windows"), 0),
        safe_float(record.get("mean_net_pips"), -1e18),
        safe_float(record.get("profit_factor"), 0.0),
        safe_float(record.get("trades"), 0.0),
    )


def arima_challenger_signature(challengers: List[Dict[str, Any]]) -> str:
    canonical = [
        {
            "pair": str(row.get("pair") or ""),
            "horizon_minutes": safe_int(row.get("horizon_minutes"), 0),
            "arima_model": str(row.get("arima_model") or ""),
            "source_candidate_mean_net_pips": round(
                safe_float(row.get("candidate_mean_net_pips"), 0.0),
                8,
            ),
            "source_arima_mean_net_pips": round(
                safe_float(row.get("arima_mean_net_pips"), 0.0),
                8,
            ),
        }
        for row in challengers
        if isinstance(row, dict)
    ]
    canonical.sort(
        key=lambda row: (
            row["pair"],
            row["horizon_minutes"],
            row["arima_model"],
        )
    )
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_arima_lookup() -> Dict[Tuple[str, int], Dict[str, Any]]:
    """Return best stable ARIMA aggregate per pair/horizon.

    Groups by model/pair/horizon first, then prefers models that are profitable
    across multiple windows.  If no stable model exists for a pair/horizon, the
    best available aggregate is still returned but labelled as unstable.
    """
    paths = arima_results_paths()
    rows: List[Dict[str, Any]] = []
    for path in paths:
        for row in read_csv_rows(path):
            row["_source_csv"] = str(path)
            rows.append(row)
    by_model: Dict[Tuple[str, int, str], List[Dict[str, Any]]] = {}
    for row in rows:
        if str(row.get("error") or "").strip():
            continue
        pair = str(row.get("pair") or "").strip()
        horizon = safe_int(row.get("horizon_minutes"), 0)
        model = str(row.get("model") or "").strip()
        if not pair or horizon <= 0 or not model:
            continue
        by_model.setdefault((pair, horizon, model), []).append(row)
    grouped: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for key, model_rows in by_model.items():
        record = arima_group_record(key, model_rows)
        grouped.setdefault((key[0], key[1]), []).append(record)
    lookup: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for key, records in grouped.items():
        lookup[key] = sorted(records, key=arima_group_quality, reverse=True)[0]
    return lookup


def instrument_scorecard_rows(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    scorecards = manifest.get("scorecards") or {}
    if not isinstance(scorecards, dict):
        return []
    rows = scorecards.get("instrument") or []
    return [row for row in rows if isinstance(row, dict)]


def compare_manifest_to_arima(manifest: Dict[str, Any]) -> Dict[str, Any]:
    horizon = infer_horizon_minutes(manifest.get("target"), manifest.get("outcome"))
    if not horizon:
        return {
            "arima_available": False,
            "arima_required_for_promotion": False,
            "candidate_beats_arima": None,
            "reason": "no 30/60/120 horizon could be inferred from candidate target/outcome",
        }

    lookup = load_arima_lookup()
    scorecard_rows = instrument_scorecard_rows(manifest)
    matched: List[Dict[str, Any]] = []
    for row in scorecard_rows:
        pair = str(row.get("segment") or "").strip()
        baseline = lookup.get((pair, horizon))
        if not baseline:
            continue
        trades = safe_float(row.get("trades"), 0.0)
        candidate_mean = safe_float(row.get("mean_net"), 0.0)
        matched.append({
            "pair": pair,
            "candidate_trades": trades,
            "candidate_mean_net_pips": candidate_mean,
            "candidate_total_net_pips": safe_float(row.get("total_net"), candidate_mean * trades),
            "candidate_profit_factor": safe_float(row.get("profit_factor"), 0.0),
            "arima_model": baseline.get("model", ""),
            "arima_mean_net_pips": safe_float(baseline.get("mean_net_pips"), 0.0),
            "arima_profit_factor": safe_float(baseline.get("profit_factor"), 0.0),
            "arima_trades": safe_float(baseline.get("trades"), 0.0),
            "arima_calibration_qualified": bool(baseline.get("calibration_qualified")),
            "arima_stable_across_windows": bool(baseline.get("stable_across_windows")),
            "arima_windows": safe_int(baseline.get("windows"), 0),
            "arima_profitable_windows": safe_int(baseline.get("profitable_windows"), 0),
        })

    if not matched:
        return {
            "arima_available": False,
            "arima_required_for_promotion": False,
            "candidate_beats_arima": None,
            "matched_horizon_minutes": horizon,
            "matched_pairs": 0,
            "reason": "no matched pair/horizon ARIMA baseline available for candidate scorecards",
        }

    required_matches = [
        row
        for row in matched
        if row.get("arima_stable_across_windows")
        and row.get("arima_calibration_qualified")
    ]
    if not required_matches:
        return {
            "arima_available": True,
            "arima_required_for_promotion": False,
            "candidate_beats_arima": None,
            "matched_horizon_minutes": horizon,
            "matched_pairs": 0,
            "matched_pairs_total": len(matched),
            "candidate_mean_net_pips": None,
            "arima_mean_net_pips": None,
            "arima_calibration_qualified_pairs": sum(
                1 for row in matched if row["arima_calibration_qualified"]
            ),
            "arima_weak_calibration_pairs": len(matched),
            "matched_pair_details": matched[:25],
            "reason": (
                "matched ARIMA rows exist, but none are stable across windows; "
                "not required for promotion"
            ),
        }

    total_candidate_trades = sum(
        max(0.0, row["candidate_trades"])
        for row in required_matches
    )
    if total_candidate_trades > 0:
        candidate_mean = sum(
            row["candidate_mean_net_pips"] * max(0.0, row["candidate_trades"])
            for row in required_matches
        ) / total_candidate_trades
    else:
        selected = (manifest.get("validation") or {}).get("selected_threshold") or {}
        candidate_mean = safe_float(selected.get("mean_net_pips"), 0.0)
    if total_candidate_trades > 0:
        arima_mean = sum(
            row["arima_mean_net_pips"] * max(0.0, row["candidate_trades"])
            for row in required_matches
        ) / total_candidate_trades
    else:
        arima_mean = sum(
            row["arima_mean_net_pips"]
            for row in required_matches
        ) / max(len(required_matches), 1)
    candidate_beats = bool(candidate_mean > arima_mean)
    calibration_qualified_pairs = sum(
        1 for row in matched if row["arima_calibration_qualified"]
    )
    return {
        "arima_available": True,
        "arima_required_for_promotion": True,
        "candidate_beats_arima": candidate_beats,
        "matched_horizon_minutes": horizon,
        "matched_pairs": len(required_matches),
        "matched_pairs_total": len(matched),
        "candidate_mean_net_pips": candidate_mean,
        "arima_mean_net_pips": arima_mean,
        "arima_calibration_qualified_pairs": calibration_qualified_pairs,
        "arima_weak_calibration_pairs": len(matched) - len(required_matches),
        "required_pair_details": required_matches[:25],
        "matched_pair_details": matched[:25],
        "reason": (
            "candidate validation net edge exceeds matched ARIMA baseline"
            if candidate_beats
            else "candidate validation net edge does not exceed matched ARIMA baseline"
        ),
    }


def validation_gate_passed(gate: Dict[str, Any]) -> bool:
    if not isinstance(gate, dict) or not gate:
        return False
    if "passed" in gate:
        return bool(gate.get("passed"))
    bool_values = [value for value in gate.values() if isinstance(value, bool)]
    return bool(bool_values) and all(bool_values)


def canary_validation_passed(manifest: Dict[str, Any]) -> bool:
    if bool(manifest.get("canary_passed", False)):
        return True
    canary = manifest.get("canary")
    if isinstance(canary, dict) and bool(canary.get("passed", False)):
        return True
    production_gate = manifest.get("production_gate")
    canary_report_path = str(manifest.get("canary_report_path") or "")
    if canary_report_path and isinstance(production_gate, dict):
        bool_values = [
            value
            for value in production_gate.values()
            if isinstance(value, bool)
        ]
        if bool_values and all(bool_values):
            return True
    return False


def shadow_validation_passed(manifest: Dict[str, Any]) -> bool:
    if bool(manifest.get("shadow_passed", False)):
        return True
    if bool(manifest.get("shadow_validation_passed", False)):
        return True
    shadow = manifest.get("shadow")
    if isinstance(shadow, dict) and bool(shadow.get("passed", False)):
        return True
    return False


def technical_production_readiness_gate(
    manifest: Dict[str, Any],
    comparison: Dict[str, Any],
) -> Dict[str, Any]:
    validation = manifest.get("validation")
    validation = validation if isinstance(validation, dict) else {}
    selected = validation.get("selected_threshold")
    selected = selected if isinstance(selected, dict) else {}
    source_gate = validation.get("gate")
    source_gate = source_gate if isinstance(source_gate, dict) else {}
    fold_count = safe_int(validation.get("fold_count"), 0)
    weeks = safe_float(selected.get("weeks"), 0.0)
    positive_weeks = safe_float(selected.get("positive_weeks"), 0.0)
    required_positive_weeks = (
        max(1, math.ceil(weeks * PRODUCTION_MIN_POSITIVE_WEEK_SHARE))
        if weeks > 0
        else 1
    )
    arima_required = bool(comparison.get("arima_required_for_promotion", False))
    arima_ok = (
        comparison.get("candidate_beats_arima") is True
        if arima_required
        else True
    )
    shadow_ok = shadow_validation_passed(manifest)
    canary_ok = canary_validation_passed(manifest)
    checks = {
        "source_validation_gate_passed": validation_gate_passed(source_gate),
        "walk_forward_folds_at_least_min": fold_count >= PRODUCTION_MIN_WALK_FORWARD_FOLDS,
        "mean_auc_at_least_0_55": safe_float(validation.get("mean_auc"), 0.0) >= 0.55,
        "minimum_week_auc_at_least_0_50": safe_float(validation.get("minimum_week_auc"), 0.0) >= 0.50,
        "selected_trades_at_least_min": safe_float(selected.get("trades"), 0.0) >= PRODUCTION_MIN_SELECTED_TRADES,
        "selected_mean_net_pips_positive": safe_float(selected.get("mean_net_pips"), 0.0) > 0.0,
        "selected_bootstrap_lower_mean_positive": safe_float(
            selected.get("bootstrap_lower_mean_net_pips"),
            -1e18,
        ) > 0.0,
        "selected_positive_in_70pct_weeks": positive_weeks >= required_positive_weeks,
        "top_pair_trade_share_at_most_35pct": safe_float(
            selected.get("top_pair_trade_share"),
            1.0,
        ) <= 0.35,
        "candidate_beats_required_arima_baseline": arima_ok,
        "shadow_validation_passed": shadow_ok,
        "canary_validation_passed": canary_ok,
    }
    failed = [name for name, passed in checks.items() if not bool(passed)]
    return {
        "passed": not failed,
        "failed_checks": failed,
        "checks": checks,
        "requirements": {
            "min_walk_forward_folds": PRODUCTION_MIN_WALK_FORWARD_FOLDS,
            "min_selected_trades": PRODUCTION_MIN_SELECTED_TRADES,
            "min_positive_week_share": PRODUCTION_MIN_POSITIVE_WEEK_SHARE,
            "requires_arima_when_available_and_stable": True,
            "requires_shadow_validation": True,
            "requires_canary_validation": True,
        },
        "evidence": {
            "fold_count": fold_count,
            "weeks": weeks,
            "positive_weeks": positive_weeks,
            "required_positive_weeks": required_positive_weeks,
            "arima_required_for_promotion": arima_required,
            "candidate_beats_arima": comparison.get("candidate_beats_arima"),
            "shadow_report_path": manifest.get("shadow_report_path", ""),
            "canary_report_path": manifest.get("canary_report_path", ""),
        },
        "reason": (
            "technical production readiness gate passed"
            if not failed
            else "technical production readiness failed: " + ", ".join(failed)
        ),
    }


def arima_coverage_for_manifest(
    manifest: Dict[str, Any],
    comparison: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    horizon = infer_horizon_minutes(manifest.get("target"), manifest.get("outcome"))
    scorecard_rows = instrument_scorecard_rows(manifest)
    total_pairs = len(scorecard_rows)
    total_trades = sum(
        max(0.0, safe_float(row.get("trades"), 0.0))
        for row in scorecard_rows
    )
    if not horizon:
        return {
            "horizon_minutes": None,
            "scorecard_pairs": total_pairs,
            "scorecard_trades": total_trades,
            "matched_pairs_total": 0,
            "stable_required_pairs": 0,
            "missing_pairs": [],
            "unstable_pairs": [],
            "matched_trade_share": 0.0,
            "stable_trade_share": 0.0,
            "reason": "no 30/60/120 horizon could be inferred",
        }
    lookup = load_arima_lookup()
    comparison = comparison if isinstance(comparison, dict) else compare_manifest_to_arima(manifest)
    required_pairs = {
        str(row.get("pair") or "")
        for row in comparison.get("required_pair_details", []) or []
        if isinstance(row, dict)
    }
    matched_pairs: List[Dict[str, Any]] = []
    stable_pairs: List[Dict[str, Any]] = []
    unstable_pairs: List[Dict[str, Any]] = []
    missing_pairs: List[Dict[str, Any]] = []
    matched_trades = 0.0
    stable_trades = 0.0
    for row in scorecard_rows:
        pair = str(row.get("segment") or "").strip()
        trades = max(0.0, safe_float(row.get("trades"), 0.0))
        mean_net = safe_float(row.get("mean_net"), 0.0)
        total_net = safe_float(row.get("total_net"), mean_net * trades)
        priority_score = total_net + 5.0 * mean_net + 0.05 * trades
        if not pair:
            continue
        baseline = lookup.get((pair, horizon))
        item = {
            "pair": pair,
            "trades": trades,
            "mean_net": mean_net,
            "total_net": total_net,
            "priority_score": priority_score,
        }
        if not baseline:
            missing_pairs.append(item)
            continue
        matched_trades += trades
        matched = {
            **item,
            "arima_model": baseline.get("model", ""),
            "arima_mean_net_pips": baseline.get("mean_net_pips"),
            "arima_stable_across_windows": bool(
                baseline.get("stable_across_windows")
            ),
            "arima_calibration_qualified": bool(
                baseline.get("calibration_qualified")
            ),
        }
        matched_pairs.append(matched)
        if pair in required_pairs:
            stable_trades += trades
            stable_pairs.append(matched)
        else:
            unstable_pairs.append(matched)
    missing_pairs.sort(key=lambda item: item["priority_score"], reverse=True)
    unstable_pairs.sort(key=lambda item: item["priority_score"], reverse=True)
    stable_pairs.sort(key=lambda item: item["priority_score"], reverse=True)
    return {
        "horizon_minutes": horizon,
        "scorecard_pairs": total_pairs,
        "scorecard_trades": total_trades,
        "matched_pairs_total": len(matched_pairs),
        "stable_required_pairs": len(stable_pairs),
        "missing_pairs_count": len(missing_pairs),
        "unstable_pairs_count": len(unstable_pairs),
        "matched_trade_share": (
            matched_trades / total_trades if total_trades > 0 else 0.0
        ),
        "stable_trade_share": (
            stable_trades / total_trades if total_trades > 0 else 0.0
        ),
        "stable_pairs": stable_pairs[:25],
        "unstable_pairs": unstable_pairs[:25],
        "missing_pairs": missing_pairs[:25],
    }


def promotion_benchmark_allows_auto_promotion(manifest: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    comparison = manifest.get("benchmark_comparison")
    if not isinstance(comparison, dict):
        comparison = compare_manifest_to_arima(manifest)
    readiness_gate = technical_production_readiness_gate(manifest, comparison)
    comparison = {
        **comparison,
        "technical_production_readiness_gate": readiness_gate,
    }
    if not readiness_gate.get("passed", False):
        comparison["reason"] = readiness_gate.get("reason", "technical production readiness failed")
        return False, comparison
    if comparison.get("arima_available") and comparison.get("candidate_beats_arima") is not True:
        return False, comparison
    return True, comparison


def arima_challengers_for_manifest(
    manifest: Dict[str, Any],
    comparison: Optional[Dict[str, Any]] = None,
    coverage: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    comparison = comparison if isinstance(comparison, dict) else compare_manifest_to_arima(manifest)
    coverage = coverage if isinstance(coverage, dict) else arima_coverage_for_manifest(manifest, comparison)
    challengers: List[Dict[str, Any]] = []
    candidate_better: List[Dict[str, Any]] = []
    for row in coverage.get("stable_pairs", []) or []:
        if not isinstance(row, dict):
            continue
        candidate_mean = safe_float(row.get("mean_net"), 0.0)
        arima_mean = safe_float(row.get("arima_mean_net_pips"), 0.0)
        trades = safe_float(row.get("trades"), 0.0)
        delta = arima_mean - candidate_mean
        item = {
            "pair": row.get("pair", ""),
            "horizon_minutes": coverage.get("horizon_minutes"),
            "candidate_mean_net_pips": candidate_mean,
            "arima_mean_net_pips": arima_mean,
            "delta_arima_minus_candidate": delta,
            "candidate_trades": trades,
            "candidate_total_net_pips": safe_float(row.get("total_net"), candidate_mean * trades),
            "arima_model": row.get("arima_model", ""),
            "arima_stable_across_windows": bool(row.get("arima_stable_across_windows")),
            "arima_calibration_qualified": bool(row.get("arima_calibration_qualified")),
            "priority_score": delta * max(trades, 1.0),
        }
        if delta > 0:
            challengers.append(item)
        else:
            candidate_better.append(item)
    challengers.sort(key=lambda item: item["priority_score"], reverse=True)
    candidate_better.sort(key=lambda item: item["priority_score"])
    return {
        "generated_utc": utc_iso(),
        "source_candidate_id": manifest.get("candidate_id", ""),
        "source_experiment_id": manifest.get("experiment_id", ""),
        "source_model_type": manifest.get("model_type", ""),
        "source_target": manifest.get("target", ""),
        "horizon_minutes": coverage.get("horizon_minutes"),
        "stable_pairs": coverage.get("stable_required_pairs", 0),
        "matched_pairs_total": coverage.get("matched_pairs_total", 0),
        "scorecard_pairs": coverage.get("scorecard_pairs", 0),
        "arima_beats_current_pair_count": len(challengers),
        "current_beats_arima_pair_count": len(candidate_better),
        "arima_challengers": challengers[:25],
        "current_model_better_pairs": candidate_better[:25],
        "recommendation": (
            "shadow_arima_pair_challengers"
            if challengers
            else "no_stable_arima_pair_challenger_yet"
        ),
        "note": (
            "Pair-level challenger evidence only. This report does not assign "
            "ARIMA to a trading account."
        ),
    }


def publish_arima_pair_challenger_shadow(challengers: Dict[str, Any]) -> Dict[str, Any]:
    challenger_rows = [
        row
        for row in challengers.get("arima_challengers", []) or []
        if isinstance(row, dict)
    ]
    signature = arima_challenger_signature(challenger_rows)
    previous = read_json(ARIMA_PAIR_CHALLENGER_SHADOW_PATH, {})
    previous_signature = (
        previous.get("challenger_set_signature")
        if isinstance(previous, dict)
        else ""
    )
    previous_report = read_json(ARIMA_SHADOW_REPORT_PATH, {})
    previous_report_signature = (
        previous_report.get("challenger_set_signature")
        if isinstance(previous_report, dict)
        else ""
    )
    manifest = {
        "updated_utc": utc_iso(),
        "stage": "shadow",
        "target_account_role": "SHADOW_OBSERVATION_ONLY",
        "activation_effective": False,
        "technical_account_activation": False,
        "execution_enabled": False,
        "standalone_promotion_allowed": False,
        "source_report": str(LATEST_ARIMA_CHALLENGERS_JSON),
        "source_candidate_id": challengers.get("source_candidate_id", ""),
        "source_experiment_id": challengers.get("source_experiment_id", ""),
        "source_model_type": challengers.get("source_model_type", ""),
        "source_target": challengers.get("source_target", ""),
        "horizon_minutes": challengers.get("horizon_minutes"),
        "stable_pairs": challengers.get("stable_pairs", 0),
        "matched_pairs_total": challengers.get("matched_pairs_total", 0),
        "scorecard_pairs": challengers.get("scorecard_pairs", 0),
        "challenger_count": len(challenger_rows),
        "challenger_set_signature": signature,
        "arima_challengers": challenger_rows,
        "promotion_requirements": {
            "requires_pair_level_shadow_observations": True,
            "requires_repeated_independent_shadow_passes": True,
            "requires_canary_assignment": True,
            "requires_execution_adapter": True,
            "requires_broker_execution_disabled_until_adapter_exists": True,
            "minimum_shadow_positive_windows": ARIMA_SHADOW_REQUIRED_PASSES,
            "minimum_shadow_pair_count": 1,
        },
        "reason": (
            "Stable ARIMA pair-level challengers beat the active technical "
            "production model on matched scorecard pairs. This manifest is "
            "research/shadow only and cannot place trades."
        ),
    }
    preserve_fields = [
        "latest_shadow_report_path",
        "latest_shadow_observation_csv",
        "latest_shadow_updated_utc",
        "latest_shadow_gate",
        "latest_shadow_aggregate",
        "rolling_pair_gate",
        "robust_arima_challengers",
        "robust_arima_challenger_count",
        "shadow_pass_signature",
        "shadow_pass_observation_key",
        "shadow_pass_streak",
        "shadow_required_passes",
        "shadow_repeated_gate",
        "shadow_history",
        "canary_assignment_prerequisites_met",
        "canary_assignment_ready",
        "canary_assignment_blockers",
        "arima_adapter_available",
        "arima_adapter_report_path",
        "arima_canary_candidate_pending_path",
        "arima_adapter_updated_utc",
    ]
    preserved_state = False
    if isinstance(previous, dict) and previous_signature == signature:
        for field in preserve_fields:
            if field in previous:
                manifest[field] = previous[field]
        preserved_state = True
    if isinstance(previous_report, dict) and previous_report_signature == signature:
        report_backfill = {
            "latest_shadow_report_path": str(ARIMA_SHADOW_REPORT_PATH),
            "latest_shadow_observation_csv": str(ARIMA_SHADOW_OBSERVATION_CSV_PATH),
            "latest_shadow_updated_utc": previous_report.get("generated_utc", ""),
            "latest_shadow_gate": previous_report.get("gate", {}),
            "latest_shadow_aggregate": previous_report.get("aggregate", {}),
            "rolling_pair_gate": previous_report.get("rolling_pair_gate", {}),
            "robust_arima_challengers": previous_report.get("robust_arima_challengers", []),
            "robust_arima_challenger_count": len(
                previous_report.get("robust_arima_challengers", []) or []
            ),
        }
        for field, value in report_backfill.items():
            if value not in ("", None, {}, []):
                manifest[field] = value
        preserved_state = True
    if not preserved_state:
        manifest["shadow_pass_streak"] = 0
        manifest["shadow_required_passes"] = ARIMA_SHADOW_REQUIRED_PASSES
        manifest["shadow_repeated_gate"] = {
            "passed": False,
            "consecutive_independent_passes": 0,
            "required_consecutive_independent_passes": ARIMA_SHADOW_REQUIRED_PASSES,
            "latest_observation_passed": False,
            "latest_observation_duplicate": False,
        }
        manifest["shadow_history"] = []
        manifest["canary_assignment_prerequisites_met"] = False
        manifest["canary_assignment_ready"] = False
        manifest["canary_assignment_blockers"] = [
            "requires_repeated_independent_shadow_passes",
            "requires_explicit_arima_execution_adapter",
        ]
    atomic_write_json(ARIMA_PAIR_CHALLENGER_SHADOW_PATH, manifest)
    return manifest


def refresh_arima_shadow_evidence_if_due(manifest: Dict[str, Any]) -> Dict[str, Any]:
    if not ARIMA_SHADOW_AUTO_ENABLED:
        return {
            "enabled": False,
            "ran": False,
            "reason": "OANDA_ARIMA_SHADOW_AUTO disabled",
        }
    if not isinstance(manifest, dict) or not manifest.get("arima_challengers"):
        return {
            "enabled": True,
            "ran": False,
            "reason": "no ARIMA challengers available",
        }
    signature = str(manifest.get("challenger_set_signature") or "")
    previous_report = read_json(ARIMA_SHADOW_REPORT_PATH, {})
    previous_report_signature = (
        str(previous_report.get("challenger_set_signature") or "")
        if isinstance(previous_report, dict)
        else ""
    )
    latest_shadow_updated = manifest.get("latest_shadow_updated_utc")
    age_seconds = seconds_since(latest_shadow_updated)
    signature_changed = bool(signature and signature != previous_report_signature)
    stale = age_seconds is None or age_seconds >= ARIMA_SHADOW_AUTO_REFRESH_SECONDS
    if not signature_changed and not stale:
        return {
            "enabled": True,
            "ran": False,
            "reason": "recent ARIMA shadow evidence already exists",
            "age_seconds": age_seconds,
            "refresh_seconds": ARIMA_SHADOW_AUTO_REFRESH_SECONDS,
            "challenger_set_signature": signature,
        }
    try:
        import oanda_arima_shadow_evaluator as shadow

        report = shadow.evaluate_once(
            manifest_path=ARIMA_PAIR_CHALLENGER_SHADOW_PATH,
            report_path=ARIMA_SHADOW_REPORT_PATH,
            observation_csv_path=ARIMA_SHADOW_OBSERVATION_CSV_PATH,
            train_rows=shadow.DEFAULT_TRAIN_ROWS,
            calibration_rows=shadow.DEFAULT_CALIBRATION_ROWS,
            test_rows=shadow.DEFAULT_TEST_ROWS,
            predict_every=shadow.DEFAULT_PREDICT_EVERY,
            min_calibration_trades=shadow.DEFAULT_MIN_CALIBRATION_TRADES,
            min_test_trades=shadow.DEFAULT_MIN_TEST_TRADES,
            min_profit_factor=shadow.DEFAULT_MIN_PROFIT_FACTOR,
            max_pairs=0,
            shadow_windows=shadow.DEFAULT_SHADOW_WINDOWS,
        )
        refreshed_manifest = read_json(ARIMA_PAIR_CHALLENGER_SHADOW_PATH, {})
        return {
            "enabled": True,
            "ran": True,
            "reason": (
                "challenger set changed"
                if signature_changed
                else "ARIMA shadow evidence stale or missing"
            ),
            "age_seconds_before_refresh": age_seconds,
            "refresh_seconds": ARIMA_SHADOW_AUTO_REFRESH_SECONDS,
            "challenger_set_signature": signature,
            "passed": bool(report.get("passed", False)),
            "pair_gate_passed_count": report.get("pair_gate_passed_count", 0),
            "evaluated_pair_count": report.get("evaluated_pair_count", 0),
            "shadow_pass_streak": (
                refreshed_manifest.get("shadow_pass_streak", "")
                if isinstance(refreshed_manifest, dict)
                else ""
            ),
            "shadow_repeated_gate": (
                refreshed_manifest.get("shadow_repeated_gate", {})
                if isinstance(refreshed_manifest, dict)
                else {}
            ),
            "report_path": str(ARIMA_SHADOW_REPORT_PATH),
        }
    except Exception as exc:
        return {
            "enabled": True,
            "ran": False,
            "reason": "ARIMA shadow refresh failed",
            "error": str(exc),
            "challenger_set_signature": signature,
        }


def refresh_arima_canary_adapter_if_due(manifest: Dict[str, Any]) -> Dict[str, Any]:
    if not ARIMA_ADAPTER_AUTO_ENABLED:
        return {
            "enabled": False,
            "ran": False,
            "reason": "OANDA_ARIMA_ADAPTER_AUTO disabled",
        }
    if not isinstance(manifest, dict) or not manifest.get("arima_challengers"):
        return {
            "enabled": True,
            "ran": False,
            "reason": "no ARIMA challenger shadow manifest",
        }
    latest_shadow_updated = str(manifest.get("latest_shadow_updated_utc") or "")
    if not latest_shadow_updated:
        return {
            "enabled": True,
            "ran": False,
            "reason": "waiting for ARIMA shadow evidence",
        }
    previous_report = read_json(ARIMA_ADAPTER_REPORT_PATH, {})
    previous_pending = read_json(ARIMA_CANARY_CANDIDATE_PENDING_PATH, {})
    previous_signature = (
        str(previous_pending.get("challenger_set_signature") or "")
        if isinstance(previous_pending, dict)
        else ""
    )
    signature = str(manifest.get("challenger_set_signature") or "")
    adapter_age = (
        seconds_since(
            previous_report.get("generated_utc")
            if isinstance(previous_report, dict)
            else ""
        )
    )
    shadow_dt = parse_utc_datetime(latest_shadow_updated)
    adapter_dt = parse_utc_datetime(
        previous_report.get("generated_utc")
        if isinstance(previous_report, dict)
        else ""
    )
    signature_changed = bool(signature and signature != previous_signature)
    shadow_newer = bool(shadow_dt and (adapter_dt is None or shadow_dt > adapter_dt))
    stale = adapter_age is None or adapter_age >= ARIMA_ADAPTER_AUTO_REFRESH_SECONDS
    pending_missing = not ARIMA_CANARY_CANDIDATE_PENDING_PATH.exists()
    if not signature_changed and not shadow_newer and not stale and not pending_missing:
        return {
            "enabled": True,
            "ran": False,
            "reason": "recent ARIMA adapter report already exists",
            "age_seconds": adapter_age,
            "refresh_seconds": ARIMA_ADAPTER_AUTO_REFRESH_SECONDS,
            "challenger_set_signature": signature,
        }
    try:
        import oanda_arima_canary_adapter as adapter

        report = adapter.build_once(
            shadow_manifest_path=ARIMA_PAIR_CHALLENGER_SHADOW_PATH,
            observations_path=ARIMA_SHADOW_OBSERVATION_CSV_PATH,
            report_path=ARIMA_ADAPTER_REPORT_PATH,
            pending_manifest_path=ARIMA_CANARY_CANDIDATE_PENDING_PATH,
            train_rows=adapter.DEFAULT_TRAIN_ROWS,
            max_signals=adapter.MAX_SIGNALS,
        )
        refreshed_manifest = read_json(ARIMA_PAIR_CHALLENGER_SHADOW_PATH, {})
        return {
            "enabled": True,
            "ran": True,
            "reason": (
                "challenger set changed"
                if signature_changed
                else "shadow evidence newer or adapter stale/missing"
            ),
            "age_seconds_before_refresh": adapter_age,
            "refresh_seconds": ARIMA_ADAPTER_AUTO_REFRESH_SECONDS,
            "challenger_set_signature": signature,
            "passed": bool(report.get("passed", False)),
            "canary_assignment_ready": bool(report.get("canary_assignment_ready", False)),
            "passing_shadow_pair_count": report.get("passing_shadow_pair_count", 0),
            "signal_count": report.get("signal_count", 0),
            "actionable_signal_count": report.get("actionable_signal_count", 0),
            "blockers": report.get("canary_assignment_blockers", []),
            "shadow_manifest_canary_ready": (
                refreshed_manifest.get("canary_assignment_ready", False)
                if isinstance(refreshed_manifest, dict)
                else False
            ),
            "report_path": str(ARIMA_ADAPTER_REPORT_PATH),
            "pending_manifest_path": str(ARIMA_CANARY_CANDIDATE_PENDING_PATH),
        }
    except Exception as exc:
        return {
            "enabled": True,
            "ran": False,
            "reason": "ARIMA canary adapter refresh failed",
            "error": str(exc),
            "challenger_set_signature": signature,
        }


def manifest_summary_row(path: Path, role: str) -> Dict[str, Any]:
    manifest = read_json(path, {})
    if not isinstance(manifest, dict) or not manifest:
        return {}
    validation = manifest.get("validation") or {}
    selected = validation.get("selected_threshold") or {}
    comparison = manifest.get("benchmark_comparison")
    if not isinstance(comparison, dict):
        comparison = compare_manifest_to_arima(manifest)
    coverage = manifest.get("arima_coverage")
    if not isinstance(coverage, dict):
        coverage = arima_coverage_for_manifest(manifest, comparison)
    challengers = arima_challengers_for_manifest(
        manifest,
        comparison,
        coverage,
    )
    return {
        "record_type": f"manifest:{role}",
        "time_utc": manifest.get("updated_utc", ""),
        "experiment_id": manifest.get("experiment_id", ""),
        "candidate_id": manifest.get("candidate_id", ""),
        "source": "promotion_manifest",
        "stage": manifest.get("stage", ""),
        "status": "active" if manifest.get("activation_effective") else "",
        "dataset_kind": manifest.get("dataset_kind", ""),
        "model_type": manifest.get("model_type", ""),
        "target": manifest.get("target", ""),
        "outcome": manifest.get("outcome", ""),
        "horizon_minutes": infer_horizon_minutes(manifest.get("target"), manifest.get("outcome")) or "",
        "feature_set": manifest.get("feature_set", ""),
        "instrument_subset": manifest.get("instrument_subset", ""),
        "mean_auc": validation.get("mean_auc", ""),
        "minimum_week_auc": validation.get("minimum_week_auc", ""),
        "selected_threshold": selected.get("threshold", ""),
        "trades": selected.get("trades", ""),
        "positive_weeks": selected.get("positive_weeks", ""),
        "mean_net_pips": selected.get("mean_net_pips", ""),
        "median_week_mean_net_pips": selected.get("median_week_mean_net_pips", ""),
        "median_profit_factor": selected.get("median_profit_factor", ""),
        "bootstrap_lower_mean_net_pips": selected.get("bootstrap_lower_mean_net_pips", ""),
        "top_pair_trade_share": selected.get("top_pair_trade_share", ""),
        "score": manifest.get("research_score", ""),
        "gate_passed": (validation.get("gate") or {}).get("passed", ""),
        "arima_available": comparison.get("arima_available", ""),
        "arima_required_for_promotion": comparison.get("arima_required_for_promotion", ""),
        "candidate_beats_arima": comparison.get("candidate_beats_arima", ""),
        "arima_mean_net_pips": comparison.get("arima_mean_net_pips", ""),
        "arima_matched_pairs": comparison.get("matched_pairs", ""),
        "arima_matched_pairs_total": comparison.get("matched_pairs_total", ""),
        "arima_scorecard_pairs": coverage.get("scorecard_pairs", ""),
        "arima_missing_pairs_count": coverage.get("missing_pairs_count", ""),
        "arima_unstable_pairs_count": coverage.get("unstable_pairs_count", ""),
        "arima_matched_trade_share": coverage.get("matched_trade_share", ""),
        "arima_stable_trade_share": coverage.get("stable_trade_share", ""),
        "arima_challenger_pairs": challengers.get("arima_beats_current_pair_count", ""),
        "arima_reason": comparison.get("reason", ""),
    }


def refresh_promotion_manifest_benchmarks() -> Dict[str, Any]:
    refreshed: List[str] = []
    skipped: List[str] = []
    for filename in [
        "technical_production.json",
        "research_leader.json",
        "shadow_candidate.json",
        "technical_model_manifest.json",
        "major_move_factor_shadow.json",
        "major_move_factor_production.json",
    ]:
        path = PROMOTIONS_ROOT / filename
        manifest = read_json(path, {})
        if not isinstance(manifest, dict) or not manifest:
            skipped.append(filename)
            continue
        if not manifest.get("target"):
            skipped.append(filename)
            continue
        comparison = compare_manifest_to_arima(manifest)
        coverage = arima_coverage_for_manifest(manifest, comparison)
        challengers = arima_challengers_for_manifest(
            manifest,
            comparison,
            coverage,
        )
        if (
            manifest.get("benchmark_comparison") == comparison
            and manifest.get("arima_coverage") == coverage
            and manifest.get("arima_challenger_report") == challengers
        ):
            skipped.append(filename)
            continue
        manifest["benchmark_comparison"] = comparison
        manifest["arima_coverage"] = coverage
        manifest["arima_challenger_report"] = challengers
        atomic_write_json(path, manifest)
        refreshed.append(filename)
    return {
        "refreshed": refreshed,
        "skipped": skipped,
    }


def ledger_summary_rows() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in read_csv_rows(RESEARCH_LEDGER_PATH):
        horizon = infer_horizon_minutes(row.get("target"), row.get("outcome"))
        out.append({
            "record_type": "research_ledger",
            "time_utc": row.get("time_utc", ""),
            "experiment_id": row.get("experiment_id", ""),
            "candidate_id": row.get("candidate_id", ""),
            "source": row.get("source", ""),
            "stage": row.get("evaluation_stage", ""),
            "status": row.get("status", ""),
            "dataset_kind": row.get("dataset_kind", ""),
            "model_type": row.get("model_type", ""),
            "target": row.get("target", ""),
            "outcome": row.get("outcome", ""),
            "horizon_minutes": horizon or "",
            "feature_set": row.get("feature_set", ""),
            "instrument_subset": row.get("instrument_subset", ""),
            "mean_auc": row.get("mean_auc", ""),
            "minimum_week_auc": row.get("minimum_week_auc", ""),
            "selected_threshold": row.get("selected_threshold", ""),
            "trades": row.get("trades", ""),
            "positive_weeks": row.get("positive_weeks", ""),
            "mean_net_pips": row.get("mean_net_pips", ""),
            "median_week_mean_net_pips": row.get("median_week_mean_net_pips", ""),
            "median_profit_factor": row.get("median_profit_factor", ""),
            "bootstrap_lower_mean_net_pips": row.get("bootstrap_lower_mean_net_pips", ""),
            "top_pair_trade_share": row.get("top_pair_trade_share", ""),
            "score": row.get("score", ""),
            "gate_passed": row.get("gate_passed", ""),
            "arima_available": "",
            "arima_required_for_promotion": "",
            "candidate_beats_arima": "",
            "arima_mean_net_pips": "",
            "arima_matched_pairs": "",
            "arima_matched_pairs_total": "",
            "arima_scorecard_pairs": "",
            "arima_missing_pairs_count": "",
            "arima_unstable_pairs_count": "",
            "arima_matched_trade_share": "",
            "arima_stable_trade_share": "",
            "arima_challenger_pairs": "",
            "arima_reason": "ledger row has no instrument scorecard; see promotion manifest comparisons",
        })
    return out


def write_latest_model_metrics_report() -> Dict[str, Any]:
    refresh_result = refresh_promotion_manifest_benchmarks()
    rows = ledger_summary_rows()
    for role, filename in [
        ("technical_production", "technical_production.json"),
        ("research_leader", "research_leader.json"),
        ("shadow_candidate", "shadow_candidate.json"),
        ("major_move_factor_shadow", "major_move_factor_shadow.json"),
    ]:
        summary = manifest_summary_row(PROMOTIONS_ROOT / filename, role)
        if summary:
            rows.append(summary)
    fieldnames = [
        "record_type",
        "time_utc",
        "experiment_id",
        "candidate_id",
        "source",
        "stage",
        "status",
        "dataset_kind",
        "model_type",
        "target",
        "outcome",
        "horizon_minutes",
        "feature_set",
        "instrument_subset",
        "mean_auc",
        "minimum_week_auc",
        "selected_threshold",
        "trades",
        "positive_weeks",
        "mean_net_pips",
        "median_week_mean_net_pips",
        "median_profit_factor",
        "bootstrap_lower_mean_net_pips",
        "top_pair_trade_share",
        "score",
        "gate_passed",
        "arima_available",
        "arima_required_for_promotion",
        "candidate_beats_arima",
        "arima_mean_net_pips",
        "arima_matched_pairs",
        "arima_matched_pairs_total",
        "arima_scorecard_pairs",
        "arima_missing_pairs_count",
        "arima_unstable_pairs_count",
        "arima_matched_trade_share",
        "arima_stable_trade_share",
        "arima_challenger_pairs",
        "arima_reason",
    ]
    write_csv_rows(LATEST_MODEL_METRICS_CSV, rows, fieldnames)
    active_production = manifest_summary_row(
        PROMOTIONS_ROOT / "technical_production.json",
        "technical_production",
    )
    active_manifest = read_json(PROMOTIONS_ROOT / "technical_production.json", {})
    active_challengers = (
        arima_challengers_for_manifest(active_manifest)
        if isinstance(active_manifest, dict) and active_manifest.get("target")
        else {}
    )
    if active_challengers:
        atomic_write_json(LATEST_ARIMA_CHALLENGERS_JSON, active_challengers)
        arima_shadow_manifest = publish_arima_pair_challenger_shadow(
            active_challengers
        )
        arima_shadow_refresh = refresh_arima_shadow_evidence_if_due(
            arima_shadow_manifest
        )
        arima_shadow_manifest = read_json(
            ARIMA_PAIR_CHALLENGER_SHADOW_PATH,
            arima_shadow_manifest,
        )
        arima_adapter_refresh = refresh_arima_canary_adapter_if_due(
            arima_shadow_manifest
        )
        arima_shadow_manifest = read_json(
            ARIMA_PAIR_CHALLENGER_SHADOW_PATH,
            arima_shadow_manifest,
        )
        arima_adapter_report = read_json(ARIMA_ADAPTER_REPORT_PATH, {})
        arima_canary_pending = read_json(ARIMA_CANARY_CANDIDATE_PENDING_PATH, {})
    else:
        arima_shadow_manifest = {}
        arima_adapter_report = {}
        arima_canary_pending = {}
        arima_shadow_refresh = {
            "enabled": ARIMA_SHADOW_AUTO_ENABLED,
            "ran": False,
            "reason": "no active ARIMA challengers",
        }
        arima_adapter_refresh = {
            "enabled": ARIMA_ADAPTER_AUTO_ENABLED,
            "ran": False,
            "reason": "no active ARIMA challengers",
        }
    payload = {
        "generated_utc": utc_iso(),
        "row_count": len(rows),
        "ledger_rows": len([row for row in rows if row.get("record_type") == "research_ledger"]),
        "manifest_rows": len([row for row in rows if str(row.get("record_type", "")).startswith("manifest:")]),
        "arima_results_csv": str(latest_arima_results_path() or ""),
        "arima_results_csvs": [str(path) for path in arima_results_paths()],
        "manifest_benchmark_refresh": refresh_result,
        "active_technical_production": active_production,
        "active_arima_challengers": active_challengers,
        "arima_challengers_json": str(LATEST_ARIMA_CHALLENGERS_JSON),
        "arima_pair_challenger_shadow_manifest": str(ARIMA_PAIR_CHALLENGER_SHADOW_PATH),
        "arima_shadow_auto_refresh": arima_shadow_refresh,
        "arima_adapter_auto_refresh": arima_adapter_refresh,
        "arima_pair_challenger_shadow": {
            "stage": arima_shadow_manifest.get("stage", ""),
            "challenger_count": arima_shadow_manifest.get("challenger_count", 0),
            "execution_enabled": arima_shadow_manifest.get("execution_enabled", False),
            "activation_effective": arima_shadow_manifest.get("activation_effective", False),
            "latest_shadow_updated_utc": arima_shadow_manifest.get("latest_shadow_updated_utc", ""),
            "shadow_pass_streak": arima_shadow_manifest.get("shadow_pass_streak", 0),
            "shadow_required_passes": arima_shadow_manifest.get("shadow_required_passes", ARIMA_SHADOW_REQUIRED_PASSES),
            "shadow_repeated_gate": arima_shadow_manifest.get("shadow_repeated_gate", {}),
            "canary_assignment_prerequisites_met": arima_shadow_manifest.get("canary_assignment_prerequisites_met", False),
            "canary_assignment_ready": arima_shadow_manifest.get("canary_assignment_ready", False),
            "canary_assignment_blockers": arima_shadow_manifest.get("canary_assignment_blockers", []),
            "arima_adapter_available": arima_shadow_manifest.get("arima_adapter_available", False),
            "arima_adapter_report_path": arima_shadow_manifest.get("arima_adapter_report_path", ""),
            "arima_canary_candidate_pending_path": arima_shadow_manifest.get("arima_canary_candidate_pending_path", ""),
        },
        "arima_canary_adapter": {
            "report_path": str(ARIMA_ADAPTER_REPORT_PATH),
            "pending_manifest_path": str(ARIMA_CANARY_CANDIDATE_PENDING_PATH),
            "available": bool(arima_adapter_report),
            "passed": arima_adapter_report.get("passed", ""),
            "canary_assignment_ready": arima_adapter_report.get("canary_assignment_ready", ""),
            "passing_shadow_pair_count": arima_adapter_report.get("passing_shadow_pair_count", ""),
            "signal_count": arima_adapter_report.get("signal_count", ""),
            "actionable_signal_count": arima_adapter_report.get("actionable_signal_count", ""),
            "blockers": arima_adapter_report.get("canary_assignment_blockers", []),
            "pending_stage": arima_canary_pending.get("stage", ""),
            "pending_execution_enabled": arima_canary_pending.get("execution_enabled", False),
        },
        "status_counts": {},
        "model_type_counts": {},
        "csv_path": str(LATEST_MODEL_METRICS_CSV),
    }
    for row in rows:
        status = str(row.get("status") or "")
        if status:
            payload["status_counts"][status] = payload["status_counts"].get(status, 0) + 1
        model_type = str(row.get("model_type") or "")
        if model_type:
            payload["model_type_counts"][model_type] = payload["model_type_counts"].get(model_type, 0) + 1
    atomic_write_json(LATEST_MODEL_METRICS_JSON, payload)
    return payload


def main() -> int:
    payload = write_latest_model_metrics_report()
    print(json.dumps({
        "generated_utc": payload["generated_utc"],
        "row_count": payload["row_count"],
        "csv_path": payload["csv_path"],
        "active_technical_production": payload.get("active_technical_production", {}),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
