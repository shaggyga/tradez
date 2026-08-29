#!/usr/bin/env python3
"""Fit and tune the HGB shadow ledger with causal, cost-aware walk-forward tests.

The fitter never edits a broker configuration. It writes a recommendation and a
promotion audit that the practice dashboard can inspect. A source that has not
passed its frozen validation contract cannot become promotion-ready.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc
ROOT = Path(__file__).resolve().parent
DEFAULT_LEDGER = ROOT / "data" / "oanda_training_manager" / "state" / "hgb_live_outcomes_v1.json"
DEFAULT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "hgb_adaptive_fit_v1.json"
DEFAULT_CONFIG = ROOT / "config" / "primary_forecast_rotation_demo_019.json"


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-min(40.0, value))
        return 1.0 / (1.0 + z)
    z = math.exp(max(-40.0, value))
    return z / (1.0 + z)


def logit(probability: float) -> float:
    p = min(1.0 - 1e-6, max(1e-6, probability))
    return math.log(p / (1.0 - p))


def fit_platt(rows: list[dict[str, Any]], *, l2: float = 2.0) -> dict[str, float]:
    """Fit intercept + slope * logit(raw probability) with damped Newton steps."""
    positives = sum(1 for row in rows if safe_float(row.get("net_pips")) > 0)
    intercept = logit((positives + 1.0) / (len(rows) + 2.0)) if rows else 0.0
    slope = 0.0
    for _ in range(60):
        g0 = 0.0
        g1 = -l2 * slope
        h00 = 1e-6
        h01 = 0.0
        h11 = l2 + 1e-6
        for row in rows:
            x = logit(safe_float(row.get("probability"), 0.5))
            y = 1.0 if safe_float(row.get("net_pips")) > 0 else 0.0
            prediction = sigmoid(intercept + slope * x)
            residual = y - prediction
            weight = max(1e-8, prediction * (1.0 - prediction))
            g0 += residual
            g1 += residual * x
            h00 += weight
            h01 += weight * x
            h11 += weight * x * x
        determinant = h00 * h11 - h01 * h01
        if determinant <= 1e-12:
            break
        delta0 = (g0 * h11 - g1 * h01) / determinant
        delta1 = (g1 * h00 - g0 * h01) / determinant
        scale = min(1.0, 2.0 / max(2.0, abs(delta0), abs(delta1)))
        intercept += delta0 * scale
        slope += delta1 * scale
        if max(abs(delta0 * scale), abs(delta1 * scale)) < 1e-7:
            break
    return {"intercept": round(intercept, 10), "slope": round(slope, 10), "n": len(rows)}


def calibrated_probability(row: dict[str, Any], model: dict[str, float]) -> float:
    return sigmoid(safe_float(model.get("intercept")) + safe_float(model.get("slope")) * logit(safe_float(row.get("probability"), 0.5)))


def chronological_folds(
    rows: list[dict[str, Any]],
    *,
    purge_minutes: int,
    minimum_train: int,
    minimum_test: int,
) -> list[tuple[list[dict[str, Any]], list[dict[str, Any]]]]:
    timestamps = sorted({parse_time(row.get("generated_utc")) for row in rows} - {None})
    if len(timestamps) < 8:
        return []
    fractions = ((0.50, 0.65), (0.65, 0.80), (0.80, 1.00))
    folds: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = []
    for train_fraction, test_fraction in fractions:
        train_index = min(len(timestamps) - 2, max(1, int(len(timestamps) * train_fraction)))
        test_index = min(len(timestamps), max(train_index + 1, int(len(timestamps) * test_fraction)))
        test_start = timestamps[train_index]
        test_end = timestamps[test_index] if test_index < len(timestamps) else None
        purge_before = test_start - timedelta(minutes=purge_minutes)
        train = [row for row in rows if (parse_time(row.get("target_utc")) or parse_time(row.get("generated_utc")) or test_start) < purge_before]
        test = [
            row for row in rows
            if (parse_time(row.get("generated_utc")) or test_start) >= test_start
            and (test_end is None or (parse_time(row.get("generated_utc")) or test_start) < test_end)
        ]
        if len(train) >= minimum_train and len(test) >= minimum_test:
            folds.append((train, test))
    return folds


def brier(rows: Iterable[dict[str, Any]], probability_key: str) -> float | None:
    values = []
    for row in rows:
        p = safe_float(row.get(probability_key), -1.0)
        if not 0.0 <= p <= 1.0:
            continue
        y = 1.0 if safe_float(row.get("net_pips")) > 0 else 0.0
        values.append((p - y) ** 2)
    return statistics.fmean(values) if values else None


def simulate_exit(row: dict[str, Any], stop_r: float | None, target_r: float | None) -> float:
    endpoint = safe_float(row.get("net_pips"))
    if stop_r is None or target_r is None:
        return endpoint
    risk = safe_float(row.get("risk_pips"))
    hits = row.get("r_hit_utc") if isinstance(row.get("r_hit_utc"), dict) else {}
    if risk <= 0 or not hits:
        return endpoint
    stop_time = parse_time(hits.get(f"sl_{stop_r:g}"))
    target_time = parse_time(hits.get(f"tp_{target_r:g}"))
    if stop_time is not None and (target_time is None or stop_time <= target_time):
        return -risk * stop_r
    if target_time is not None:
        return risk * target_r
    return endpoint


def summarize_values(rows: list[dict[str, Any]], *, stop_r: float | None = None, target_r: float | None = None) -> dict[str, Any]:
    values = [simulate_exit(row, stop_r, target_r) for row in rows]
    if not values:
        return {"n": 0, "avg_net_pips": None, "profit_factor": None, "win_rate": None, "block_lcb_pips": None}
    gross_profit = sum(value for value in values if value > 0)
    gross_loss = -sum(value for value in values if value < 0)
    blocks: dict[str, list[float]] = defaultdict(list)
    fold_values: dict[int, list[float]] = defaultdict(list)
    for row, value in zip(rows, values):
        stamp = parse_time(row.get("generated_utc")) or utc_now()
        block = stamp.replace(hour=(stamp.hour // 4) * 4, minute=0, second=0, microsecond=0).isoformat()
        blocks[block].append(value)
        fold_values[int(safe_float(row.get("_fold"), -1))].append(value)
    block_means = [statistics.fmean(items) for items in blocks.values()]
    block_mean = statistics.fmean(block_means)
    block_se = statistics.stdev(block_means) / math.sqrt(len(block_means)) if len(block_means) > 1 else float("inf")
    fold_means = [statistics.fmean(items) for key, items in sorted(fold_values.items()) if key >= 0]
    return {
        "n": len(values),
        "win_rate": round(sum(1 for value in values if value > 0) / len(values), 6),
        "avg_net_pips": round(statistics.fmean(values), 6),
        "median_net_pips": round(statistics.median(values), 6),
        "total_net_pips": round(sum(values), 6),
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
        "distinct_4h_blocks": len(block_means),
        "positive_4h_block_rate": round(sum(1 for value in block_means if value > 0) / len(block_means), 6),
        "block_lcb_pips": round(block_mean - 1.96 * block_se, 6) if math.isfinite(block_se) else None,
        "positive_fold_count": sum(1 for value in fold_means if value > 0),
        "fold_count": len(fold_means),
        "fold_avg_net_pips": [round(value, 6) for value in fold_means],
    }


def gate_rows(rows: list[dict[str, Any]], gate: dict[str, Any]) -> list[dict[str, Any]]:
    streams = set(gate.get("streams") or [])
    return [
        row for row in rows
        if safe_float(row.get("adaptive_probability")) >= safe_float(gate.get("calibrated_probability_min"), 0.5)
        and safe_float(row.get("rank_score")) >= safe_float(gate.get("rank_score_min"))
        and safe_float(row.get("entry_spread_pips")) <= safe_float(gate.get("max_spread_pips"), 30.0)
        and (not streams or str(row.get("source_stream") or "") in streams)
    ]


def fit_walk_forward(rows: list[dict[str, Any]], *, purge_minutes: int) -> dict[str, Any]:
    folds = chronological_folds(rows, purge_minutes=purge_minutes, minimum_train=120, minimum_test=40)
    oof: list[dict[str, Any]] = []
    fold_models = []
    for fold_index, (train, test) in enumerate(folds):
        global_model = fit_platt(train)
        stream_models = {
            stream: fit_platt([row for row in train if str(row.get("source_stream") or "") == stream])
            for stream in sorted({str(row.get("source_stream") or "") for row in train})
            if sum(1 for row in train if str(row.get("source_stream") or "") == stream) >= 80
        }
        fold_models.append({"fold": fold_index, "train_n": len(train), "test_n": len(test), "global": global_model, "streams": stream_models})
        for source in test:
            row = dict(source)
            model = stream_models.get(str(row.get("source_stream") or ""), global_model)
            row["adaptive_probability"] = calibrated_probability(row, model)
            row["_fold"] = fold_index
            oof.append(row)
    full_global = fit_platt(rows)
    full_streams = {
        stream: fit_platt([row for row in rows if str(row.get("source_stream") or "") == stream])
        for stream in sorted({str(row.get("source_stream") or "") for row in rows})
        if sum(1 for row in rows if str(row.get("source_stream") or "") == stream) >= 100
    }
    return {
        "folds": fold_models,
        "oof": oof,
        "full_model": {"global": full_global, "streams": full_streams},
        "raw_brier": brier(oof, "probability"),
        "calibrated_brier": brier(oof, "adaptive_probability"),
    }


def tune_gates(oof: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not oof:
        return [], None
    available_streams = sorted({str(row.get("source_stream") or "") for row in oof})
    scopes = [available_streams] + [[stream] for stream in available_streams]
    candidates: list[dict[str, Any]] = []
    minimum_n = max(30, int(len(oof) * 0.03))
    for streams in scopes:
        for probability_min in (0.50, 0.525, 0.55, 0.575, 0.60, 0.625, 0.65, 0.675, 0.70, 0.725, 0.75):
            for rank_min in (0.0, 0.005, 0.01, 0.02, 0.04, 0.08):
                for max_spread in (3.0, 5.0, 8.0, 14.0, 30.0):
                    gate = {
                        "streams": streams,
                        "calibrated_probability_min": probability_min,
                        "rank_score_min": rank_min,
                        "max_spread_pips": max_spread,
                    }
                    selected = gate_rows(oof, gate)
                    if len(selected) < minimum_n:
                        continue
                    metrics = summarize_values(selected)
                    candidate = {**gate, **metrics}
                    lcb = metrics.get("block_lcb_pips")
                    candidate["selection_score"] = round((safe_float(lcb, -999.0) * 0.8) + safe_float(metrics.get("avg_net_pips")) * 0.2, 6)
                    candidates.append(candidate)
    candidates.sort(
        key=lambda row: (
            safe_float(row.get("selection_score"), -999.0),
            safe_float(row.get("profit_factor")),
            int(safe_float(row.get("n"))),
        ),
        reverse=True,
    )
    return candidates[:30], candidates[0] if candidates else None


def tune_exits(selected: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    candidates = []
    for stop_r in (0.5, 0.75, 1.0, 1.25, 1.5):
        for target_r in (0.25, 0.5, 0.75, 1.0, 1.5):
            metrics = summarize_values(selected, stop_r=stop_r, target_r=target_r)
            row = {"stop_r": stop_r, "target_r": target_r, **metrics}
            row["selection_score"] = round(
                safe_float(metrics.get("block_lcb_pips"), -999.0) * 0.8 + safe_float(metrics.get("avg_net_pips")) * 0.2,
                6,
            )
            candidates.append(row)
    candidates.sort(key=lambda row: (safe_float(row.get("selection_score"), -999.0), safe_float(row.get("profit_factor"))), reverse=True)
    return candidates[:15], candidates[0] if candidates else None


def window_summary(rows: list[dict[str, Any]], days: int) -> dict[str, Any]:
    start = utc_now() - timedelta(days=days)
    selected = [row for row in rows if (parse_time(row.get("generated_utc")) or datetime.min.replace(tzinfo=UTC)) >= start]
    accepted = [row for row in selected if bool(row.get("accepted"))]
    misses = [row for row in selected if not bool(row.get("accepted"))]
    return {
        "days": days,
        "all": summarize_values(selected),
        "accepted": summarize_values(accepted),
        "misses": summarize_values(misses),
    }


def build_report(ledger: dict[str, Any], config: dict[str, Any], *, lookback_days: int) -> dict[str, Any]:
    outcomes = [row for row in ledger.get("outcomes") or [] if isinstance(row, dict) and row.get("net_pips") is not None]
    cutoff = utc_now() - timedelta(days=lookback_days)
    recent = [row for row in outcomes if (parse_time(row.get("generated_utc")) or datetime.min.replace(tzinfo=UTC)) >= cutoff]
    recent.sort(key=lambda row: str(row.get("generated_utc") or ""))
    pending = [row for row in ledger.get("pending") or [] if isinstance(row, dict)]
    adaptive_cfg = config.get("adaptive_fit") if isinstance(config.get("adaptive_fit"), dict) else {}
    min_fit = int(safe_float(adaptive_cfg.get("minimum_matured_for_fit"), 300))
    min_promotion = int(safe_float(adaptive_cfg.get("minimum_matured_for_promotion"), 2000))
    min_blocks = int(safe_float(adaptive_cfg.get("minimum_distinct_4h_blocks_for_promotion"), 12))
    source_eligible = bool(adaptive_cfg.get("source_validation_eligible", False))
    report: dict[str, Any] = {
        "schema_version": 1,
        "time": utc_now().isoformat(),
        "mode": "shadow_recommendation",
        "account_id": ledger.get("account_id"),
        "ledger": str(DEFAULT_LEDGER),
        "lookback_days": lookback_days,
        "status": "collecting",
        "evidence": {
            "matured": len(recent),
            "accepted_matured": sum(1 for row in recent if bool(row.get("accepted"))),
            "miss_matured": sum(1 for row in recent if not bool(row.get("accepted"))),
            "pending": len(pending),
            "pending_accepted": sum(1 for row in pending if bool(row.get("accepted"))),
            "pending_misses": sum(1 for row in pending if not bool(row.get("accepted"))),
            "path_tracked_matured": sum(1 for row in recent if int(safe_float(row.get("path_samples"))) > 0),
        },
        "pending_rejection_reasons": dict(Counter(str(row.get("reject_reason") or "accepted").split(":", 1)[0] for row in pending)),
        "recent_windows": [window_summary(recent, days) for days in (1, 7, 30)],
        "source_validation": {
            "eligible_for_automatic_promotion": source_eligible,
            "status": "rejected_by_2026-07-12_validation" if not source_eligible else "eligible",
            "reason": adaptive_cfg.get("reason") or "",
        },
    }
    if len(recent) < min_fit:
        report["requirements"] = {
            "minimum_matured_for_fit": min_fit,
            "remaining_for_fit": max(0, min_fit - len(recent)),
            "minimum_matured_for_promotion": min_promotion,
            "minimum_distinct_4h_blocks_for_promotion": min_blocks,
        }
        return report

    fit = fit_walk_forward(recent, purge_minutes=120)
    oof = fit.pop("oof")
    gate_leaderboard, best_gate = tune_gates(oof)
    best_gate_rows = gate_rows(oof, best_gate) if best_gate else []
    exit_leaderboard, best_exit = tune_exits(best_gate_rows) if best_gate_rows else ([], None)
    baseline = summarize_values([row for row in oof if bool(row.get("accepted"))])
    report.update(
        {
            "status": "fitted" if oof else "insufficient_walk_forward_folds",
            "walk_forward": {
                **fit,
                "oof_n": len(oof),
                "brier_improvement": (
                    round(safe_float(fit.get("raw_brier")) - safe_float(fit.get("calibrated_brier")), 8)
                    if fit.get("raw_brier") is not None and fit.get("calibrated_brier") is not None else None
                ),
            },
            "current_accepted_baseline": baseline,
            "gate_leaderboard": gate_leaderboard,
            "exit_leaderboard": exit_leaderboard,
            "recommended_gate": best_gate,
            "recommended_exit": best_exit,
        }
    )
    selected_metrics = best_exit or best_gate or {}
    checks = {
        "source_passed_frozen_validation": source_eligible,
        "minimum_matured": len(recent) >= min_promotion,
        "minimum_selected": int(safe_float(selected_metrics.get("n"))) >= 200,
        "minimum_distinct_4h_blocks": int(safe_float(selected_metrics.get("distinct_4h_blocks"))) >= min_blocks,
        "positive_block_lower_bound": safe_float(selected_metrics.get("block_lcb_pips"), -999.0) > 0.0,
        "profit_factor_at_least_1p10": safe_float(selected_metrics.get("profit_factor")) >= 1.10,
        "positive_4h_block_rate_at_least_60pct": safe_float(selected_metrics.get("positive_4h_block_rate")) >= 0.60,
        "majority_positive_walk_forward_folds": int(safe_float(selected_metrics.get("positive_fold_count"))) >= max(2, math.ceil(safe_float(selected_metrics.get("fold_count")) * 2.0 / 3.0)),
        "calibration_brier_improved": safe_float(report["walk_forward"].get("brier_improvement"), -1.0) > 0.0,
    }
    report["promotion"] = {
        "ready": all(checks.values()),
        "applied": False,
        "checks": checks,
        "policy": "Recommendations remain shadow-only. Automatic broker-config edits are intentionally disabled.",
    }
    if report["promotion"]["ready"]:
        report["status"] = "promotion_ready"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    while True:
        report = build_report(load_json(args.ledger), load_json(args.config), lookback_days=max(1, args.lookback_days))
        report["ledger"] = str(args.ledger)
        atomic_json(args.output, report)
        print(json.dumps({"time": report["time"], "status": report["status"], **report["evidence"]}), flush=True)
        if args.once:
            return 0
        time.sleep(max(15.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
