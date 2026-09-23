#!/usr/bin/env python3
"""Offline ARIMA challenger shadow evaluator.

This process is deliberately broker-free.  It reads the pair-level ARIMA
challenger manifest, re-scores those challengers on the most recent local
feature data, and publishes evidence reports.  It cannot place trades and it
does not write any canary or production activation manifest.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np

import oanda_arima_baseline_grid as arima_grid


PROJECT_ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
REPORT_ROOT = TRAINING_ROOT / "arima_shadow"
MANIFEST_PATH = PROMOTIONS_ROOT / "arima_pair_challenger_shadow.json"
REPORT_PATH = REPORT_ROOT / "latest_arima_shadow_report.json"
OBSERVATION_CSV_PATH = REPORT_ROOT / "arima_shadow_observations.csv"

DEFAULT_TRAIN_ROWS = 12_000
DEFAULT_CALIBRATION_ROWS = 2_500
DEFAULT_TEST_ROWS = 2_500
DEFAULT_PREDICT_EVERY = 12
DEFAULT_MIN_CALIBRATION_TRADES = 25
DEFAULT_MIN_TEST_TRADES = 8
DEFAULT_MIN_PROFIT_FACTOR = 1.05
DEFAULT_REQUIRED_SHADOW_PASSES = 2
DEFAULT_SHADOW_WINDOWS = 2
SHADOW_HISTORY_LIMIT = 20


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json_safe(row.get(key, "")) for key in fieldnames})
    os.replace(tmp, path)


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


def challenger_signature(challengers: List[Dict[str, Any]]) -> str:
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


def observation_key(rows: List[Dict[str, Any]], signature: str) -> str:
    canonical = [
        {
            "pair": str(row.get("pair") or ""),
            "horizon_minutes": safe_int(row.get("horizon_minutes"), 0),
            "arima_model": str(row.get("arima_model") or ""),
            "test_start_utc": str(row.get("test_start_utc") or ""),
            "test_end_utc": str(row.get("test_end_utc") or ""),
            "error": str(row.get("error") or ""),
        }
        for row in rows
    ]
    canonical.sort(
        key=lambda row: (
            row["pair"],
            row["horizon_minutes"],
            row["arima_model"],
        )
    )
    raw = json.dumps(
        {"signature": signature, "rows": canonical},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_arima_model(model: str) -> Tuple[Tuple[int, int, int], str]:
    match = re.fullmatch(r"ARIMA\((\d+),\s*(\d+),\s*(\d+)\)_trend_([a-z]+)", model.strip())
    if not match:
        raise ValueError(f"Unsupported ARIMA model string: {model!r}")
    return (
        (int(match.group(1)), int(match.group(2)), int(match.group(3))),
        match.group(4),
    )


def pip_multiplier(pair: str) -> float:
    return 100.0 if pair.endswith("_JPY") else 10_000.0


def choose_slices(
    frame_length: int,
    train_rows: int,
    calibration_rows: int,
    test_rows: int,
    *,
    window_offset: int = 0,
) -> Tuple[slice, slice, slice]:
    test_end = frame_length - max(0, int(window_offset)) * test_rows
    test_start = test_end - test_rows
    calibration_start = test_start - calibration_rows
    train_start = calibration_start - train_rows
    if train_start < 0:
        raise ValueError(
            "not enough rows for shadow split: "
            f"have={frame_length}, need={train_rows + calibration_rows + test_rows}, "
            f"window_offset={window_offset}"
        )
    return (
        slice(train_start, calibration_start),
        slice(calibration_start, test_start),
        slice(test_start, test_end),
    )


def trade_direction_counts(predictions: np.ndarray, threshold: float) -> Dict[str, int]:
    finite = predictions[np.isfinite(predictions)]
    if len(finite) == 0 or not np.isfinite(threshold):
        return {"long_signals": 0, "short_signals": 0}
    return {
        "long_signals": int((finite >= threshold).sum()),
        "short_signals": int((finite <= -threshold).sum()),
    }


def evaluate_challenger(
    challenger: Dict[str, Any],
    *,
    train_rows: int,
    calibration_rows: int,
    test_rows: int,
    predict_every: int,
    min_calibration_trades: int,
    min_test_trades: int,
    min_profit_factor: float,
    window_id: int,
    window_offset: int,
) -> Tuple[Dict[str, Any], np.ndarray]:
    pair = str(challenger.get("pair") or "").strip()
    horizon = safe_int(challenger.get("horizon_minutes"), 0)
    model_name = str(challenger.get("arima_model") or "").strip()
    if not pair or horizon <= 0 or not model_name:
        raise ValueError(f"incomplete challenger row: {challenger}")

    order, trend = parse_arima_model(model_name)
    frame = arima_grid.load_pair(pair)
    train_slice, calibration_slice, test_slice = choose_slices(
        len(frame),
        train_rows=train_rows,
        calibration_rows=calibration_rows,
        test_rows=test_rows,
        window_offset=window_offset,
    )
    train = frame.iloc[train_slice]
    calibration = frame.iloc[calibration_slice]
    test = frame.iloc[test_slice]
    cal_predictions, test_predictions = arima_grid.arima_predictions(
        train,
        calibration,
        test,
        order=order,
        trend=trend,
        horizons=[horizon],
        predict_every=predict_every,
        pip_multiplier=pip_multiplier(pair),
    )
    cal_series = cal_predictions[:, 0]
    test_series = test_predictions[:, 0]
    threshold, calibration_metrics = arima_grid.choose_threshold(
        calibration,
        cal_series,
        horizon,
        min_calibration_trades=min_calibration_trades,
    )
    trade_values = arima_grid.trading_values(test, test_series, horizon, threshold)
    test_metrics = arima_grid.metrics_from_values(trade_values)
    forecast_metrics = arima_grid.forecast_metrics(
        test[f"future_move_pips_{horizon}"].to_numpy(dtype=float),
        test_series,
    )
    direction_counts = trade_direction_counts(test_series, threshold)
    pair_gate = {
        "test_trades_at_least_min": bool(test_metrics["trades"] >= min_test_trades),
        "mean_net_positive": bool(test_metrics["mean_net_pips"] > 0),
        "profit_factor_at_least_min": bool(test_metrics["profit_factor"] >= min_profit_factor),
        "threshold_is_finite": bool(np.isfinite(threshold)),
    }
    row = {
        "time_utc": utc_iso(),
        "shadow_window_id": window_id,
        "shadow_window_offset": window_offset,
        "pair": pair,
        "horizon_minutes": horizon,
        "arima_model": model_name,
        "train_start_utc": str(train["time_utc"].iloc[0]),
        "train_end_utc": str(train["time_utc"].iloc[-1]),
        "calibration_start_utc": str(calibration["time_utc"].iloc[0]),
        "calibration_end_utc": str(calibration["time_utc"].iloc[-1]),
        "test_start_utc": str(test["time_utc"].iloc[0]),
        "test_end_utc": str(test["time_utc"].iloc[-1]),
        "threshold_pips": threshold,
        "calibration_trades": calibration_metrics["trades"],
        "calibration_mean_net_pips": calibration_metrics["mean_net_pips"],
        "calibration_total_net_pips": calibration_metrics["total_net_pips"],
        "calibration_profit_factor": calibration_metrics["profit_factor"],
        "calibration_win_rate": calibration_metrics["win_rate"],
        "test_trades": test_metrics["trades"],
        "test_mean_net_pips": test_metrics["mean_net_pips"],
        "test_median_net_pips": test_metrics["median_net_pips"],
        "test_total_net_pips": test_metrics["total_net_pips"],
        "test_profit_factor": test_metrics["profit_factor"],
        "test_win_rate": test_metrics["win_rate"],
        "forecast_mae_pips": forecast_metrics["mae_pips"],
        "forecast_rmse_pips": forecast_metrics["rmse_pips"],
        "forecast_direction_accuracy": forecast_metrics["direction_accuracy"],
        "forecast_correlation": forecast_metrics["correlation"],
        "long_signals": direction_counts["long_signals"],
        "short_signals": direction_counts["short_signals"],
        "candidate_mean_net_pips": safe_float(challenger.get("candidate_mean_net_pips"), 0.0),
        "candidate_total_net_pips": safe_float(challenger.get("candidate_total_net_pips"), 0.0),
        "candidate_trades": safe_float(challenger.get("candidate_trades"), 0.0),
        "historical_arima_mean_net_pips": safe_float(challenger.get("arima_mean_net_pips"), 0.0),
        "historical_delta_arima_minus_candidate": safe_float(challenger.get("delta_arima_minus_candidate"), 0.0),
        "shadow_delta_arima_minus_candidate": test_metrics["mean_net_pips"]
        - safe_float(challenger.get("candidate_mean_net_pips"), 0.0),
        "pair_gate_passed": all(pair_gate.values()),
        "pair_gate": pair_gate,
        "error": "",
    }
    return row, trade_values


def error_row(
    challenger: Dict[str, Any],
    error: Exception,
    *,
    window_id: int = 1,
    window_offset: int = 0,
) -> Dict[str, Any]:
    return {
        "time_utc": utc_iso(),
        "shadow_window_id": window_id,
        "shadow_window_offset": window_offset,
        "pair": str(challenger.get("pair") or ""),
        "horizon_minutes": safe_int(challenger.get("horizon_minutes"), 0),
        "arima_model": str(challenger.get("arima_model") or ""),
        "error": str(error),
        "pair_gate_passed": False,
    }


def fieldnames(rows: Iterable[Dict[str, Any]]) -> List[str]:
    preferred = [
        "time_utc",
        "shadow_window_id",
        "shadow_window_offset",
        "pair",
        "horizon_minutes",
        "arima_model",
        "train_start_utc",
        "train_end_utc",
        "calibration_start_utc",
        "calibration_end_utc",
        "test_start_utc",
        "test_end_utc",
        "threshold_pips",
        "calibration_trades",
        "calibration_mean_net_pips",
        "calibration_total_net_pips",
        "calibration_profit_factor",
        "calibration_win_rate",
        "test_trades",
        "test_mean_net_pips",
        "test_median_net_pips",
        "test_total_net_pips",
        "test_profit_factor",
        "test_win_rate",
        "forecast_mae_pips",
        "forecast_rmse_pips",
        "forecast_direction_accuracy",
        "forecast_correlation",
        "long_signals",
        "short_signals",
        "candidate_mean_net_pips",
        "candidate_total_net_pips",
        "candidate_trades",
        "historical_arima_mean_net_pips",
        "historical_delta_arima_minus_candidate",
        "shadow_delta_arima_minus_candidate",
        "pair_gate_passed",
        "error",
    ]
    discovered = []
    for row in rows:
        for key in row:
            if key not in preferred and key not in discovered and key != "pair_gate":
                discovered.append(key)
    return preferred + discovered


def update_manifest_shadow_status(manifest_path: Path, report: Dict[str, Any]) -> None:
    manifest = read_json(manifest_path, {})
    if not isinstance(manifest, dict) or not manifest:
        return
    signature = str(report.get("challenger_set_signature") or "")
    key = str(report.get("observation_key") or "")
    rolling_gate = report.get("rolling_shadow_gate") or {}
    previous_signature = str(manifest.get("shadow_pass_signature") or "")
    previous_key = str(manifest.get("shadow_pass_observation_key") or "")
    previous_streak = safe_int(manifest.get("shadow_pass_streak"), 0)
    if isinstance(rolling_gate, dict) and rolling_gate:
        streak = safe_int(rolling_gate.get("independent_pass_count"), 0)
        repeated_passed = bool(rolling_gate.get("passed", False))
        latest_duplicate = False
    elif report.get("passed") is True:
        if signature != previous_signature:
            streak = 1
        elif key == previous_key:
            streak = max(previous_streak, 1)
        else:
            streak = previous_streak + 1
        repeated_passed = streak >= DEFAULT_REQUIRED_SHADOW_PASSES
        latest_duplicate = bool(key and key == previous_key)
    else:
        streak = 0
        repeated_passed = False
        latest_duplicate = False
    history = manifest.get("shadow_history") or []
    if not isinstance(history, list):
        history = []
    history.append({
        "generated_utc": report.get("generated_utc"),
        "challenger_set_signature": signature,
        "observation_key": key,
        "passed": bool(report.get("passed", False)),
        "pair_gate_passed_count": report.get("pair_gate_passed_count", 0),
        "evaluated_pair_count": report.get("evaluated_pair_count", 0),
        "challenger_count": report.get("challenger_count", 0),
        "aggregate": report.get("aggregate", {}),
        "rolling_shadow_gate": rolling_gate,
        "duplicate_observation": latest_duplicate,
    })
    manifest["latest_shadow_report_path"] = str(REPORT_PATH)
    manifest["latest_shadow_observation_csv"] = str(OBSERVATION_CSV_PATH)
    manifest["latest_shadow_updated_utc"] = report.get("generated_utc")
    manifest["latest_shadow_gate"] = report.get("gate", {})
    manifest["latest_shadow_aggregate"] = report.get("aggregate", {})
    manifest["rolling_pair_gate"] = report.get("rolling_pair_gate", {})
    manifest["robust_arima_challengers"] = report.get("robust_arima_challengers", [])
    manifest["robust_arima_challenger_count"] = len(report.get("robust_arima_challengers", []) or [])
    manifest["challenger_set_signature"] = signature
    manifest["shadow_pass_signature"] = signature
    manifest["shadow_pass_observation_key"] = key
    manifest["shadow_pass_streak"] = streak
    manifest["shadow_required_passes"] = DEFAULT_REQUIRED_SHADOW_PASSES
    manifest["shadow_repeated_gate"] = {
        "passed": repeated_passed,
        "consecutive_independent_passes": streak,
        "required_consecutive_independent_passes": DEFAULT_REQUIRED_SHADOW_PASSES,
        "latest_observation_passed": bool(report.get("passed", False)),
        "latest_observation_duplicate": latest_duplicate,
        "rolling_windows": rolling_gate.get("window_count", "") if isinstance(rolling_gate, dict) else "",
        "rolling_window_pass_count": rolling_gate.get("independent_pass_count", "") if isinstance(rolling_gate, dict) else "",
    }
    manifest["shadow_history"] = history[-SHADOW_HISTORY_LIMIT:]
    adapter_available = bool(manifest.get("arima_adapter_available", False))
    blockers = []
    if not repeated_passed:
        blockers.append("requires_repeated_independent_shadow_passes")
    if not adapter_available:
        blockers.append("requires_explicit_arima_execution_adapter")
    manifest["canary_assignment_prerequisites_met"] = bool(repeated_passed and adapter_available)
    manifest["canary_assignment_ready"] = False
    manifest["canary_assignment_blockers"] = blockers
    manifest["execution_enabled"] = False
    manifest["activation_effective"] = False
    manifest["technical_account_activation"] = False
    manifest["standalone_promotion_allowed"] = False
    atomic_write_json(manifest_path, manifest)


def evaluate_once(
    *,
    manifest_path: Path,
    report_path: Path,
    observation_csv_path: Path,
    train_rows: int,
    calibration_rows: int,
    test_rows: int,
    predict_every: int,
    min_calibration_trades: int,
    min_test_trades: int,
    min_profit_factor: float,
    max_pairs: int,
    shadow_windows: int,
) -> Dict[str, Any]:
    manifest = read_json(manifest_path, {})
    if not isinstance(manifest, dict) or not manifest:
        report = {
            "generated_utc": utc_iso(),
            "available": False,
            "reason": f"missing ARIMA challenger manifest: {manifest_path}",
        }
        atomic_write_json(report_path, report)
        return report

    challengers = manifest.get("arima_challengers") or []
    if not isinstance(challengers, list):
        challengers = []
    if max_pairs > 0:
        challengers = challengers[:max_pairs]
    signature = challenger_signature([
        row for row in challengers if isinstance(row, dict)
    ])

    window_count = max(1, int(shadow_windows))
    rows: List[Dict[str, Any]] = []
    all_trade_values: List[float] = []
    window_reports: List[Dict[str, Any]] = []
    for window_offset in range(window_count):
        window_id = window_offset + 1
        window_rows: List[Dict[str, Any]] = []
        window_trade_values: List[float] = []
        for challenger in challengers:
            if not isinstance(challenger, dict):
                continue
            try:
                row, trade_values = evaluate_challenger(
                    challenger,
                    train_rows=train_rows,
                    calibration_rows=calibration_rows,
                    test_rows=test_rows,
                    predict_every=predict_every,
                    min_calibration_trades=min_calibration_trades,
                    min_test_trades=min_test_trades,
                    min_profit_factor=min_profit_factor,
                    window_id=window_id,
                    window_offset=window_offset,
                )
                rows.append(row)
                window_rows.append(row)
                finite_values = [float(value) for value in trade_values if np.isfinite(value)]
                all_trade_values.extend(finite_values)
                window_trade_values.extend(finite_values)
            except Exception as exc:
                row = error_row(
                    challenger,
                    exc,
                    window_id=window_id,
                    window_offset=window_offset,
                )
                rows.append(row)
                window_rows.append(row)
        window_aggregate = arima_grid.metrics_from_values(np.asarray(window_trade_values, dtype=float))
        window_evaluated = [row for row in window_rows if not row.get("error")]
        window_passed_pairs = [row for row in window_evaluated if row.get("pair_gate_passed") is True]
        window_gate = {
            "available": bool(challengers),
            "evaluated_all_challengers": bool(len(window_evaluated) == len(challengers)),
            "positive_pair_count_at_least_half": bool(
                len(window_passed_pairs) >= max(1, math.ceil(len(challengers) * 0.5))
            )
            if challengers
            else False,
            "aggregate_trades_at_least_pair_min": bool(
                window_aggregate["trades"] >= max(min_test_trades, min_test_trades * max(1, len(challengers)) * 0.5)
            ),
            "aggregate_mean_net_positive": bool(window_aggregate["mean_net_pips"] > 0),
            "aggregate_profit_factor_at_least_min": bool(window_aggregate["profit_factor"] >= min_profit_factor),
            "execution_disabled": bool(manifest.get("execution_enabled") is False),
        }
        window_reports.append({
            "window_id": window_id,
            "window_offset": window_offset,
            "passed": all(window_gate.values()),
            "gate": window_gate,
            "aggregate": window_aggregate,
            "evaluated_pair_count": len(window_evaluated),
            "errored_pair_count": len(window_rows) - len(window_evaluated),
            "pair_gate_passed_count": len(window_passed_pairs),
            "test_start_utc": str(window_evaluated[0].get("test_start_utc", "")) if window_evaluated else "",
            "test_end_utc": str(window_evaluated[0].get("test_end_utc", "")) if window_evaluated else "",
        })

    aggregate = arima_grid.metrics_from_values(np.asarray(all_trade_values, dtype=float))
    evaluated_rows = [row for row in rows if not row.get("error")]
    passed_pairs = [row for row in evaluated_rows if row.get("pair_gate_passed") is True]
    key = observation_key(rows, signature)
    gate = {
        "available": bool(challengers),
        "evaluated_all_challengers": bool(
            len(evaluated_rows) == len(challengers) * window_count
        ),
        "positive_pair_count_at_least_half": bool(
            len(passed_pairs) >= max(1, math.ceil(len(challengers) * window_count * 0.5))
        )
        if challengers
        else False,
        "aggregate_trades_at_least_pair_min": bool(
            aggregate["trades"] >= max(
                min_test_trades,
                min_test_trades * max(1, len(challengers) * window_count) * 0.5,
            )
        ),
        "aggregate_mean_net_positive": bool(aggregate["mean_net_pips"] > 0),
        "aggregate_profit_factor_at_least_min": bool(aggregate["profit_factor"] >= min_profit_factor),
        "execution_disabled": bool(manifest.get("execution_enabled") is False),
    }
    passed = all(gate.values())
    independent_pass_count = sum(1 for window in window_reports if window.get("passed") is True)
    rolling_shadow_gate = {
        "passed": independent_pass_count >= DEFAULT_REQUIRED_SHADOW_PASSES,
        "independent_pass_count": independent_pass_count,
        "required_independent_passes": DEFAULT_REQUIRED_SHADOW_PASSES,
        "window_count": len(window_reports),
        "passed_window_ids": [
            window.get("window_id")
            for window in window_reports
            if window.get("passed") is True
        ],
    }
    pair_groups: Dict[Tuple[str, int, str], List[Dict[str, Any]]] = {}
    for row in rows:
        if row.get("error"):
            continue
        pair = str(row.get("pair") or "").strip()
        horizon = safe_int(row.get("horizon_minutes"), 0)
        model = str(row.get("arima_model") or "").strip()
        if pair and horizon and model:
            pair_groups.setdefault((pair, horizon, model), []).append(row)
    rolling_pairs: List[Dict[str, Any]] = []
    for key, pair_rows in sorted(pair_groups.items()):
        window_ids = sorted({safe_int(row.get("shadow_window_id"), 0) for row in pair_rows})
        pass_count = sum(1 for row in pair_rows if row.get("pair_gate_passed") is True)
        means = [safe_float(row.get("test_mean_net_pips"), 0.0) for row in pair_rows]
        pfs = [safe_float(row.get("test_profit_factor"), 0.0) for row in pair_rows]
        trades = [safe_float(row.get("test_trades"), 0.0) for row in pair_rows]
        all_required_windows_present = len(window_ids) >= DEFAULT_REQUIRED_SHADOW_PASSES
        robust = bool(
            all_required_windows_present
            and pass_count >= DEFAULT_REQUIRED_SHADOW_PASSES
            and pass_count == len(pair_rows)
        )
        rolling_pairs.append({
            "pair": key[0],
            "horizon_minutes": key[1],
            "arima_model": key[2],
            "window_count": len(pair_rows),
            "window_ids": window_ids,
            "passed_windows": pass_count,
            "required_windows": DEFAULT_REQUIRED_SHADOW_PASSES,
            "robust_pair_gate_passed": robust,
            "min_mean_net_pips": min(means or [0.0]),
            "avg_mean_net_pips": sum(means) / max(len(means), 1),
            "min_profit_factor": min(pfs or [0.0]),
            "avg_profit_factor": sum(pfs) / max(len(pfs), 1),
            "total_trades": sum(trades),
        })
    robust_keys = {
        (
            row.get("pair"),
            safe_int(row.get("horizon_minutes"), 0),
            row.get("arima_model"),
        )
        for row in rolling_pairs
        if row.get("robust_pair_gate_passed") is True
    }
    robust_challengers = [
        {
            **challenger,
            "rolling_pair_gate": next(
                (
                    row
                    for row in rolling_pairs
                    if (
                        row.get("pair"),
                        safe_int(row.get("horizon_minutes"), 0),
                        row.get("arima_model"),
                    )
                    == (
                        challenger.get("pair"),
                        safe_int(challenger.get("horizon_minutes"), 0),
                        challenger.get("arima_model"),
                    )
                ),
                {},
            ),
        }
        for challenger in challengers
        if isinstance(challenger, dict)
        and (
            challenger.get("pair"),
            safe_int(challenger.get("horizon_minutes"), 0),
            challenger.get("arima_model"),
        )
        in robust_keys
    ]
    rolling_pair_gate = {
        "passed": bool(robust_challengers),
        "robust_pair_count": len(robust_challengers),
        "required_windows": DEFAULT_REQUIRED_SHADOW_PASSES,
        "window_count": window_count,
        "robust_pairs": [
            {
                "pair": row.get("pair"),
                "horizon_minutes": row.get("horizon_minutes"),
                "arima_model": row.get("arima_model"),
                "avg_mean_net_pips": row.get("avg_mean_net_pips"),
                "min_mean_net_pips": row.get("min_mean_net_pips"),
                "avg_profit_factor": row.get("avg_profit_factor"),
                "min_profit_factor": row.get("min_profit_factor"),
                "total_trades": row.get("total_trades"),
            }
            for row in rolling_pairs
            if row.get("robust_pair_gate_passed") is True
        ],
        "all_pairs": rolling_pairs,
    }
    report = {
        "generated_utc": utc_iso(),
        "available": bool(challengers),
        "passed": passed,
        "source_manifest_path": str(manifest_path),
        "observation_csv_path": str(observation_csv_path),
        "challenger_set_signature": signature,
        "observation_key": key,
        "challenger_count": len(challengers),
        "evaluated_pair_count": len(evaluated_rows),
        "errored_pair_count": len(rows) - len(evaluated_rows),
        "pair_gate_passed_count": len(passed_pairs),
        "aggregate": aggregate,
        "gate": gate,
        "rolling_shadow_gate": rolling_shadow_gate,
        "rolling_pair_gate": rolling_pair_gate,
        "robust_arima_challengers": robust_challengers,
        "shadow_windows": window_reports,
        "requirements": {
            "train_rows": train_rows,
            "calibration_rows": calibration_rows,
            "test_rows": test_rows,
            "shadow_windows": window_count,
            "predict_every": predict_every,
            "min_calibration_trades": min_calibration_trades,
            "min_test_trades": min_test_trades,
            "min_profit_factor": min_profit_factor,
        },
        "rows": rows,
        "execution_enabled": False,
        "technical_account_activation": False,
        "reason": (
            "Shadow evidence only. ARIMA challengers remain disabled for broker execution "
            "until an explicit adapter/canary workflow is added and promoted."
        ),
    }
    write_csv_rows(observation_csv_path, rows, fieldnames(rows))
    atomic_write_json(report_path, report)
    if manifest_path == MANIFEST_PATH:
        update_manifest_shadow_status(manifest_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--report", default=str(REPORT_PATH))
    parser.add_argument("--observations", default=str(OBSERVATION_CSV_PATH))
    parser.add_argument("--train-rows", type=int, default=DEFAULT_TRAIN_ROWS)
    parser.add_argument("--calibration-rows", type=int, default=DEFAULT_CALIBRATION_ROWS)
    parser.add_argument("--test-rows", type=int, default=DEFAULT_TEST_ROWS)
    parser.add_argument("--predict-every", type=int, default=DEFAULT_PREDICT_EVERY)
    parser.add_argument("--min-calibration-trades", type=int, default=DEFAULT_MIN_CALIBRATION_TRADES)
    parser.add_argument("--min-test-trades", type=int, default=DEFAULT_MIN_TEST_TRADES)
    parser.add_argument("--min-profit-factor", type=float, default=DEFAULT_MIN_PROFIT_FACTOR)
    parser.add_argument("--shadow-windows", type=int, default=DEFAULT_SHADOW_WINDOWS, help="Disjoint recent test windows for repeated shadow validation")
    parser.add_argument("--max-pairs", type=int, default=0, help="Limit challengers for smoke tests; 0 means all")
    args = parser.parse_args()

    report = evaluate_once(
        manifest_path=Path(args.manifest),
        report_path=Path(args.report),
        observation_csv_path=Path(args.observations),
        train_rows=args.train_rows,
        calibration_rows=args.calibration_rows,
        test_rows=args.test_rows,
        predict_every=args.predict_every,
        min_calibration_trades=args.min_calibration_trades,
        min_test_trades=args.min_test_trades,
        min_profit_factor=args.min_profit_factor,
        max_pairs=args.max_pairs,
        shadow_windows=args.shadow_windows,
    )
    print(
        json.dumps(
            {
                "time_utc": utc_iso(),
                "passed": report.get("passed", False),
                "challengers": report.get("challenger_count", 0),
                "evaluated": report.get("evaluated_pair_count", 0),
                "pair_passed": report.get("pair_gate_passed_count", 0),
                "rolling_shadow_gate": report.get("rolling_shadow_gate", {}),
                "aggregate_mean_net_pips": (report.get("aggregate") or {}).get("mean_net_pips", 0),
                "aggregate_profit_factor": (report.get("aggregate") or {}).get("profit_factor", 0),
                "report": str(Path(args.report)),
            },
            default=json_safe,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
