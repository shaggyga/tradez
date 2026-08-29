#!/usr/bin/env python3
"""Offline ARIMA challenger adapter for future canary assignment.

This module bridges the ARIMA pair-level challenger research into an auditable
pending canary artifact.  It deliberately does not write canary_candidate.json,
does not call OANDA, and cannot place trades.  A separate explicit promotion
step is still required before any live paper canary execution can consume ARIMA.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
from statsmodels.tsa.arima.model import ARIMA

import oanda_arima_baseline_grid as arima_grid
import oanda_arima_shadow_evaluator as shadow


PROJECT_ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
ADAPTER_ROOT = TRAINING_ROOT / "arima_adapter"
SHADOW_MANIFEST_PATH = PROMOTIONS_ROOT / "arima_pair_challenger_shadow.json"
SHADOW_OBSERVATIONS_PATH = TRAINING_ROOT / "arima_shadow" / "arima_shadow_observations.csv"
ADAPTER_REPORT_PATH = ADAPTER_ROOT / "latest_arima_adapter_report.json"
PENDING_CANARY_MANIFEST_PATH = PROMOTIONS_ROOT / "arima_canary_candidate_pending.json"

DEFAULT_TRAIN_ROWS = 12_000
MIN_PASSING_SHADOW_PAIRS = 1
MAX_SIGNALS = 8


def arima_executor_support() -> tuple[bool, bool, bool]:
    try:
        import oanda_arima_canary_executor as executor

        return (
            bool(getattr(executor, "EXECUTOR_SUPPORTS_ARIMA_PENDING_MANIFEST", False)),
            bool(getattr(executor, "EXECUTOR_SUPPORTS_ARIMA_ORDER_ADAPTER", False)),
            bool(getattr(executor, "EXECUTOR_SUPPORTS_ARIMA_BROKER_EXECUTION", False)),
        )
    except Exception:
        return False, False, False


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, Path):
        return str(value)
    return value


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_safe), encoding="utf-8")
    os.replace(tmp, path)


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


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
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def pip_multiplier(pair: str) -> float:
    return 100.0 if pair.endswith("_JPY") else 10_000.0


def shadow_observation_lookup(path: Path) -> Dict[Tuple[str, int, str], Dict[str, Any]]:
    out: Dict[Tuple[str, int, str], Dict[str, Any]] = {}
    for row in read_csv_rows(path):
        pair = str(row.get("pair") or "").strip()
        horizon = safe_int(row.get("horizon_minutes"), 0)
        model = str(row.get("arima_model") or "").strip()
        if pair and horizon and model:
            key = (pair, horizon, model)
            current = out.get(key)
            row_window = safe_int(row.get("shadow_window_id"), 999999)
            current_window = safe_int(current.get("shadow_window_id"), 999999) if isinstance(current, dict) else 999999
            if current is None or row_window < current_window:
                out[key] = row
    return out


def latest_arima_forecast(
    *,
    pair: str,
    model_name: str,
    horizon: int,
    train_rows: int,
) -> Dict[str, Any]:
    order, trend = shadow.parse_arima_model(model_name)
    frame = arima_grid.load_pair(pair)
    if len(frame) < train_rows:
        raise ValueError(f"not enough rows for latest forecast: {pair} has {len(frame)}<{train_rows}")
    train = frame.tail(train_rows).copy()
    series = np.log(train["close"].to_numpy(dtype=float))
    current = float(train["close"].iloc[-1])
    current_log = float(series[-1])
    steps = max(1, horizon // 5)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fitted = ARIMA(
            series,
            order=order,
            trend=trend,
            enforce_stationarity=False,
            enforce_invertibility=False,
        ).fit(method_kwargs={"maxiter": 60})
        forecast_log = np.asarray(fitted.forecast(steps=steps), dtype=float)
    if len(forecast_log) < steps or not np.isfinite(forecast_log[steps - 1]):
        raise ValueError(f"ARIMA forecast unavailable for {pair} {model_name}")
    final_log = float(forecast_log[steps - 1])
    if abs(final_log - current_log) > 0.05:
        raise ValueError(
            f"forecast rejected as unstable: log_delta={final_log - current_log:.6f}"
        )
    predicted_price = math.exp(final_log)
    predicted_pips = (predicted_price - current) * pip_multiplier(pair)
    return {
        "latest_bar_utc": str(train["time_utc"].iloc[-1]),
        "latest_close": current,
        "predicted_price": predicted_price,
        "predicted_pips": predicted_pips,
        "steps": steps,
        "train_start_utc": str(train["time_utc"].iloc[0]),
        "train_end_utc": str(train["time_utc"].iloc[-1]),
    }


def candidate_rows(
    manifest: Dict[str, Any],
    observations: Dict[Tuple[str, int, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows = []
    source_challengers = manifest.get("robust_arima_challengers") or []
    source_label = "robust_arima_challengers"
    if not isinstance(source_challengers, list) or not source_challengers:
        source_challengers = manifest.get("arima_challengers", []) or []
        source_label = "arima_challengers"
    for challenger in source_challengers:
        if not isinstance(challenger, dict):
            continue
        pair = str(challenger.get("pair") or "").strip()
        horizon = safe_int(challenger.get("horizon_minutes"), 0)
        model = str(challenger.get("arima_model") or "").strip()
        obs = observations.get((pair, horizon, model), {})
        if not safe_bool(obs.get("pair_gate_passed")):
            continue
        rows.append({
            **challenger,
            "shadow_observation": obs,
            "adapter_candidate_source": source_label,
        })
    rows.sort(
        key=lambda row: (
            safe_float(
                (row.get("shadow_observation") or {}).get("test_mean_net_pips"),
                -1e18,
            ),
            safe_float(row.get("priority_score"), -1e18),
        ),
        reverse=True,
    )
    return rows


def signal_from_forecast(predicted_pips: float, threshold: float) -> str:
    if not np.isfinite(predicted_pips) or not np.isfinite(threshold):
        return "hold"
    if predicted_pips >= threshold:
        return "long"
    if predicted_pips <= -threshold:
        return "short"
    return "hold"


def build_once(
    *,
    shadow_manifest_path: Path,
    observations_path: Path,
    report_path: Path,
    pending_manifest_path: Path,
    train_rows: int,
    max_signals: int,
) -> Dict[str, Any]:
    manifest = read_json(shadow_manifest_path, {})
    if not isinstance(manifest, dict) or not manifest:
        report = {
            "generated_utc": utc_iso(),
            "available": False,
            "reason": f"missing ARIMA shadow manifest: {shadow_manifest_path}",
        }
        atomic_write_json(report_path, report)
        return report

    observations = shadow_observation_lookup(observations_path)
    candidates = candidate_rows(manifest, observations)
    selected = candidates[:max(1, max_signals)]
    signals: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for row in selected:
        pair = str(row.get("pair") or "")
        horizon = safe_int(row.get("horizon_minutes"), 0)
        model = str(row.get("arima_model") or "")
        obs = row.get("shadow_observation") or {}
        threshold = abs(safe_float(obs.get("threshold_pips"), float("inf")))
        try:
            forecast = latest_arima_forecast(
                pair=pair,
                model_name=model,
                horizon=horizon,
                train_rows=train_rows,
            )
            action = signal_from_forecast(
                safe_float(forecast.get("predicted_pips"), 0.0),
                threshold,
            )
            signals.append({
                "pair": pair,
                "horizon_minutes": horizon,
                "arima_model": model,
                "action": action,
                "predicted_pips": forecast["predicted_pips"],
                "threshold_pips": threshold,
                "latest_bar_utc": forecast["latest_bar_utc"],
                "latest_close": forecast["latest_close"],
                "predicted_price": forecast["predicted_price"],
                "train_start_utc": forecast["train_start_utc"],
                "train_end_utc": forecast["train_end_utc"],
                "shadow_test_trades": safe_float(obs.get("test_trades"), 0.0),
                "shadow_test_mean_net_pips": safe_float(obs.get("test_mean_net_pips"), 0.0),
                "shadow_test_profit_factor": safe_float(obs.get("test_profit_factor"), 0.0),
                "historical_arima_mean_net_pips": safe_float(row.get("arima_mean_net_pips"), 0.0),
                "candidate_mean_net_pips": safe_float(row.get("candidate_mean_net_pips"), 0.0),
            })
        except Exception as exc:
            errors.append({
                "pair": pair,
                "horizon_minutes": horizon,
                "arima_model": model,
                "error": str(exc),
            })

    actionable = [row for row in signals if row.get("action") in {"long", "short"}]
    repeated_gate = manifest.get("shadow_repeated_gate") or {}
    rolling_pair_gate = manifest.get("rolling_pair_gate") or {}
    robust_pair_count = safe_int(rolling_pair_gate.get("robust_pair_count"), 0) if isinstance(rolling_pair_gate, dict) else 0
    candidate_source = (
        candidates[0].get("adapter_candidate_source")
        if candidates
        else (
            "robust_arima_challengers"
            if manifest.get("robust_arima_challengers")
            else "arima_challengers"
        )
    )
    pair_level_repeated_passed = bool(
        candidate_source == "robust_arima_challengers"
        and robust_pair_count >= MIN_PASSING_SHADOW_PAIRS
    )
    basket_repeated_passed = bool(repeated_gate.get("passed", False))
    repeated_passed = bool(basket_repeated_passed or pair_level_repeated_passed)
    adapter_smoke_passed = (
        len(candidates) >= MIN_PASSING_SHADOW_PAIRS
        and len(signals) >= MIN_PASSING_SHADOW_PAIRS
        and not errors
    )
    pending_executor_supported, order_adapter_supported, broker_executor_supported = arima_executor_support()
    blockers = []
    if not repeated_passed:
        blockers.append("requires_repeated_independent_shadow_passes")
    if not adapter_smoke_passed:
        blockers.append("requires_successful_arima_adapter_signal_generation")
    if not pending_executor_supported:
        blockers.append("requires_arima_canary_executor_support")
    if not order_adapter_supported:
        blockers.append("requires_arima_canary_stop_risk_order_adapter")
    if not broker_executor_supported:
        blockers.append("requires_broker_execution_enablement")
    ready = (
        repeated_passed
        and adapter_smoke_passed
        and pending_executor_supported
        and order_adapter_supported
        and broker_executor_supported
    )
    pending_manifest = {
        "updated_utc": utc_iso(),
        "stage": "arima_canary_pending",
        "adapter_type": "arima_pair_challenger",
        "source_shadow_manifest_path": str(shadow_manifest_path),
        "source_shadow_report_path": manifest.get("latest_shadow_report_path", ""),
        "source_shadow_observation_csv": str(observations_path),
        "source_candidate_id": manifest.get("source_candidate_id", ""),
        "source_experiment_id": manifest.get("source_experiment_id", ""),
        "challenger_set_signature": manifest.get("challenger_set_signature", ""),
        "shadow_repeated_gate": repeated_gate,
        "rolling_pair_gate": {
            "robust_pair_count": robust_pair_count,
            "pair_level_repeated_passed": pair_level_repeated_passed,
            "basket_repeated_passed": basket_repeated_passed,
            "candidate_source": candidate_source,
        },
        "adapter_smoke_passed": adapter_smoke_passed,
        "arima_canary_executor_support_available": pending_executor_supported,
        "arima_canary_order_adapter_available": order_adapter_supported,
        "arima_canary_broker_execution_support_available": broker_executor_supported,
        "canary_assignment_ready": ready,
        "canary_assignment_blockers": blockers,
        "execution_enabled": False,
        "activation_effective": False,
        "technical_account_activation": False,
        "actual_canary_file_written": False,
        "requires_manual_assignment_to_canary_candidate_json": True,
        "passing_shadow_pair_count": len(candidates),
        "robust_shadow_pair_count": robust_pair_count,
        "adapter_candidate_source": candidate_source,
        "signal_count": len(signals),
        "actionable_signal_count": len(actionable),
        "signals": signals,
        "errors": errors,
        "risk_policy": {
            "account_role": "canary",
            "max_open_trades": 1,
            "max_new_trades_per_scan": 1,
            "max_total_new_risk_pct_per_scan": 0.10,
            "paper_only": True,
            "requires_existing_canary_executor_support": not pending_executor_supported,
            "requires_pair_specific_stop_risk_order_adapter": not order_adapter_supported,
            "requires_broker_execution_enablement": not broker_executor_supported,
        },
        "reason": (
            "ARIMA adapter generated pending canary signals from shadow-passed "
            "pair challengers. This artifact is non-executing and is not the "
            "live canary_candidate.json manifest."
        ),
    }
    report = {
        "generated_utc": utc_iso(),
        "available": bool(candidates),
        "passed": adapter_smoke_passed,
        "canary_assignment_ready": ready,
        "canary_assignment_blockers": blockers,
        "pending_manifest_path": str(pending_manifest_path),
        "source_shadow_manifest_path": str(shadow_manifest_path),
        "passing_shadow_pair_count": len(candidates),
        "robust_shadow_pair_count": robust_pair_count,
        "adapter_candidate_source": candidate_source,
        "pair_level_repeated_passed": pair_level_repeated_passed,
        "basket_repeated_passed": basket_repeated_passed,
        "signal_count": len(signals),
        "actionable_signal_count": len(actionable),
        "hold_signal_count": len(signals) - len(actionable),
        "error_count": len(errors),
        "signals": signals,
        "errors": errors,
        "execution_enabled": False,
        "activation_effective": False,
    }
    atomic_write_json(pending_manifest_path, pending_manifest)
    atomic_write_json(report_path, report)

    shadow_manifest = read_json(shadow_manifest_path, {})
    if isinstance(shadow_manifest, dict) and shadow_manifest:
        shadow_manifest["arima_adapter_available"] = adapter_smoke_passed
        shadow_manifest["arima_adapter_report_path"] = str(report_path)
        shadow_manifest["arima_canary_candidate_pending_path"] = str(pending_manifest_path)
        shadow_manifest["arima_adapter_updated_utc"] = report["generated_utc"]
        shadow_manifest["canary_assignment_prerequisites_met"] = ready
        shadow_manifest["canary_assignment_ready"] = ready
        shadow_manifest["canary_assignment_blockers"] = blockers
        shadow_manifest["execution_enabled"] = False
        shadow_manifest["activation_effective"] = False
        shadow_manifest["technical_account_activation"] = False
        atomic_write_json(shadow_manifest_path, shadow_manifest)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shadow-manifest", default=str(SHADOW_MANIFEST_PATH))
    parser.add_argument("--observations", default=str(SHADOW_OBSERVATIONS_PATH))
    parser.add_argument("--report", default=str(ADAPTER_REPORT_PATH))
    parser.add_argument("--pending-manifest", default=str(PENDING_CANARY_MANIFEST_PATH))
    parser.add_argument("--train-rows", type=int, default=DEFAULT_TRAIN_ROWS)
    parser.add_argument("--max-signals", type=int, default=MAX_SIGNALS)
    args = parser.parse_args()
    report = build_once(
        shadow_manifest_path=Path(args.shadow_manifest),
        observations_path=Path(args.observations),
        report_path=Path(args.report),
        pending_manifest_path=Path(args.pending_manifest),
        train_rows=args.train_rows,
        max_signals=args.max_signals,
    )
    print(json.dumps({
        "time_utc": utc_iso(),
        "passed": report.get("passed", False),
        "canary_assignment_ready": report.get("canary_assignment_ready", False),
        "passing_shadow_pair_count": report.get("passing_shadow_pair_count", 0),
        "signal_count": report.get("signal_count", 0),
        "actionable_signal_count": report.get("actionable_signal_count", 0),
        "blockers": report.get("canary_assignment_blockers", []),
        "report": str(Path(args.report)),
    }, default=json_safe), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
