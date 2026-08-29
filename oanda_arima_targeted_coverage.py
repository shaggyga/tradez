#!/usr/bin/env python3
"""Target missing ARIMA benchmark coverage for active promotion manifests.

The broad ARIMA grid can be expensive across all 68 instruments.  This runner
prioritizes the instruments that matter right now: scorecard pairs from the
active technical/research manifests that do not already have an ARIMA
pair+horizon benchmark.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd

import oanda_arima_baseline_grid as arima_grid
from oanda_model_metrics import (
    PROMOTIONS_ROOT,
    ARIMA_ROOT,
    compare_manifest_to_arima,
    infer_horizon_minutes,
    load_arima_lookup,
    read_json,
)


PLAN_PATH = ARIMA_ROOT / "targeted_arima_coverage_plan.json"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_safe(value: Any) -> Any:
    try:
        import numpy as np

        if isinstance(value, (np.bool_,)):
            return bool(value)
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
    except Exception:
        pass
    if isinstance(value, Path):
        return str(value)
    return value


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=json_safe), encoding="utf-8")
    os.replace(tmp, path)


def scorecard_candidates(
    manifest: Dict[str, Any],
    *,
    min_trades: float,
) -> List[Dict[str, Any]]:
    scorecards = manifest.get("scorecards") or {}
    rows = scorecards.get("instrument") if isinstance(scorecards, dict) else []
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        pair = str(row.get("segment") or "").strip()
        trades = float(row.get("trades") or 0.0)
        if not pair or trades < min_trades:
            continue
        mean_net = float(row.get("mean_net") or 0.0)
        total_net = float(row.get("total_net") or (mean_net * trades))
        out.append({
            "pair": pair,
            "trades": trades,
            "mean_net": mean_net,
            "total_net": total_net,
            "profit_factor": float(row.get("profit_factor") or 0.0),
            "priority_score": total_net + 5.0 * mean_net + 0.05 * trades,
        })
    return sorted(out, key=lambda row: row["priority_score"], reverse=True)


def build_plan(
    *,
    manifest_names: Sequence[str],
    max_pairs: int,
    min_scorecard_trades: float,
) -> Dict[str, Any]:
    lookup = load_arima_lookup()
    seen_pairs: set[str] = set()
    candidates: List[Dict[str, Any]] = []
    manifest_summaries = []
    for name in manifest_names:
        path = PROMOTIONS_ROOT / name
        manifest = read_json(path, {})
        if not isinstance(manifest, dict) or not manifest:
            continue
        horizon = infer_horizon_minutes(manifest.get("target"), manifest.get("outcome"))
        comparison = compare_manifest_to_arima(manifest)
        manifest_summaries.append({
            "manifest": name,
            "candidate_id": manifest.get("candidate_id", ""),
            "experiment_id": manifest.get("experiment_id", ""),
            "target": manifest.get("target", ""),
            "outcome": manifest.get("outcome", ""),
            "horizon_minutes": horizon,
            "current_arima_available": comparison.get("arima_available"),
            "current_matched_pairs": comparison.get("matched_pairs", 0),
            "current_reason": comparison.get("reason", ""),
        })
        if not horizon:
            continue
        for row in scorecard_candidates(manifest, min_trades=min_scorecard_trades):
            pair = row["pair"]
            if pair in seen_pairs:
                continue
            has_baseline = (pair, horizon) in lookup
            if has_baseline:
                continue
            seen_pairs.add(pair)
            candidates.append({
                **row,
                "horizon_minutes": horizon,
                "source_manifest": name,
            })
            if len(candidates) >= max_pairs:
                break
        if len(candidates) >= max_pairs:
            break
    return {
        "generated_utc": utc_iso(),
        "manifest_summaries": manifest_summaries,
        "missing_pair_count_selected": len(candidates),
        "selected_missing_pairs": candidates,
        "existing_arima_lookup_count": len(lookup),
    }


def run_targeted_grid(
    plan: Dict[str, Any],
    *,
    windows: int,
    train_rows: int,
    calibration_rows: int,
    test_rows: int,
    predict_every: int,
    min_calibration_trades: int,
    max_specs: int,
    output_prefix: str,
) -> Dict[str, Any]:
    selected = plan.get("selected_missing_pairs") or []
    if not selected:
        return {
            "ran": False,
            "reason": "no missing pairs selected",
        }
    original_specs = list(arima_grid.ARIMA_SPECS)
    if max_specs > 0:
        arima_grid.ARIMA_SPECS = original_specs[:max_specs]
    try:
        all_results: List[Dict[str, Any]] = []
        for index, row in enumerate(selected, 1):
            pair = row["pair"]
            horizon = int(row["horizon_minutes"])
            print(f"[targeted-arima] {index}/{len(selected)} {pair} h{horizon}", flush=True)
            try:
                all_results.extend(
                    arima_grid.evaluate_pair(
                        pair,
                        horizons=[horizon],
                        windows=windows,
                        train_rows=train_rows,
                        calibration_rows=calibration_rows,
                        test_rows=test_rows,
                        predict_every=predict_every,
                        min_calibration_trades=min_calibration_trades,
                    )
                )
            except Exception as exc:
                all_results.append({
                    "pipeline_version": arima_grid.PIPELINE_VERSION,
                    "family": "ARIMA",
                    "pair": pair,
                    "horizon_minutes": horizon,
                    "error": repr(exc),
                })
    finally:
        arima_grid.ARIMA_SPECS = original_specs

    ARIMA_ROOT.mkdir(parents=True, exist_ok=True)
    results = pd.DataFrame(all_results)
    csv_path = ARIMA_ROOT / f"{output_prefix}_results.csv"
    summary_path = ARIMA_ROOT / f"{output_prefix}_summary.json"
    results.to_csv(csv_path, index=False)
    summary = arima_grid.summarize(results)
    summary.update({
        "generated_utc": utc_iso(),
        "targeted_plan": plan,
        "results_csv": str(csv_path),
        "pairs_requested": [row["pair"] for row in selected],
        "horizons_requested": sorted({int(row["horizon_minutes"]) for row in selected}),
        "windows_requested": windows,
        "train_rows": train_rows,
        "calibration_rows": calibration_rows,
        "test_rows": test_rows,
        "predict_every": predict_every,
        "max_specs": max_specs or len(original_specs),
    })
    atomic_write_json(summary_path, summary)
    return {
        "ran": True,
        "results_csv": str(csv_path),
        "summary_json": str(summary_path),
        "rows": summary.get("rows", 0),
        "successful_rows": summary.get("successful_rows", 0),
        "pairs": summary.get("pairs", []),
        "promotion_ready_count": summary.get("promotion_ready_count", 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Target ARIMA coverage for missing production scorecard pairs")
    parser.add_argument(
        "--manifest",
        action="append",
        default=[],
        help="Promotion manifest filename. Can be repeated.",
    )
    parser.add_argument("--max-pairs", type=int, default=5)
    parser.add_argument("--min-scorecard-trades", type=float, default=5.0)
    parser.add_argument("--run", action="store_true", help="Run ARIMA grid for selected pairs")
    parser.add_argument("--windows", type=int, default=2)
    parser.add_argument("--train-rows", type=int, default=12_000)
    parser.add_argument("--calibration-rows", type=int, default=2_000)
    parser.add_argument("--test-rows", type=int, default=2_000)
    parser.add_argument("--predict-every", type=int, default=12)
    parser.add_argument("--min-calibration-trades", type=int, default=10)
    parser.add_argument("--max-specs", type=int, default=15)
    parser.add_argument("--output-prefix", default="")
    args = parser.parse_args()

    manifests = args.manifest or [
        "technical_production.json",
        "research_leader.json",
        "shadow_candidate.json",
    ]
    plan = build_plan(
        manifest_names=manifests,
        max_pairs=max(0, args.max_pairs),
        min_scorecard_trades=max(0.0, args.min_scorecard_trades),
    )
    run_result = {"ran": False, "reason": "dry run"}
    if args.run:
        prefix = args.output_prefix or f"targeted_arima_{utc_stamp()}"
        run_result = run_targeted_grid(
            plan,
            windows=max(1, args.windows),
            train_rows=max(100, args.train_rows),
            calibration_rows=max(50, args.calibration_rows),
            test_rows=max(50, args.test_rows),
            predict_every=max(1, args.predict_every),
            min_calibration_trades=max(1, args.min_calibration_trades),
            max_specs=max(0, args.max_specs),
            output_prefix=prefix,
        )
    payload = {
        **plan,
        "run_result": run_result,
    }
    atomic_write_json(PLAN_PATH, payload)
    print(json.dumps({
        "generated_utc": payload["generated_utc"],
        "selected_missing_pairs": payload["selected_missing_pairs"],
        "run_result": run_result,
        "plan_path": str(PLAN_PATH),
    }, indent=2, sort_keys=True, default=json_safe))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
