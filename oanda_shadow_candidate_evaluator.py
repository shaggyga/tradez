#!/usr/bin/env python3
"""Evaluate validated candidates on post-training shadow data.

This process is intentionally offline.  It does not use OANDA account APIs and
cannot place trades.  It reads the validated shadow manifest, scores only data
after the model training cutoff, and publishes a canary-ready manifest only when
unseen cost-adjusted results pass.
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

import oanda_gpt_training_strategy_manager as manager
from oanda_model_lifecycle import ModelLifecycleRegistry


SHADOW_MIN_ROWS = 2_500
SHADOW_MIN_TRADES = 75
SHADOW_MIN_WEEKS = 2
SHADOW_MIN_PROFIT_FACTOR = 1.20
SHADOW_MIN_POSITIVE_WEEK_SHARE = 0.70
SHADOW_SLEEP_SECONDS = 15 * 60


def write_json(path: Path, obj: Any) -> None:
    manager.save_json(path, obj)


def load_shadow_manifest() -> Dict[str, Any]:
    return manager.load_json(
        manager.DIRS["promotions"] / "shadow_candidate.json",
        {},
    )


def load_candidate_frame(manifest: Dict[str, Any]) -> tuple[pd.DataFrame, List[str]]:
    spec = {
        "dataset_kind": manifest["dataset_kind"],
        "model_type": manifest["model_type"],
        "target": manifest["target"],
        "outcome": manifest["outcome"],
        "feature_set": manifest["feature_set"],
        "research_role": manifest.get("research_role", "standalone_candidate"),
        "direction_target": manifest.get("direction_target", ""),
        "execution_policy": manifest.get("execution_policy", ""),
        "parameters": manifest.get("parameters", {}),
        "evaluation_stage": "shadow",
    }
    engine = manager.ContinuousResearchEngine()
    frame, _, _, features = engine.load_dataset(spec)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
    for column in [*features, manifest["target"], manifest["outcome"]]:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["time_utc", *features, manifest["target"], manifest["outcome"]]), features


def evaluate_once() -> Dict[str, Any]:
    manifest = load_shadow_manifest()
    report_path = manager.DIRS["reports"] / "latest_shadow_candidate_report.json"
    if not manifest:
        result = {
            "generated_utc": manager.iso_utc(),
            "available": False,
            "reason": "no shadow_candidate.json exists",
        }
        write_json(report_path, result)
        return result
    artifact = Path(str(manifest.get("model_artifact_path", "")))
    if not artifact.exists():
        result = {
            "generated_utc": manager.iso_utc(),
            "available": False,
            "reason": f"missing model artifact: {artifact}",
            "manifest": manifest,
        }
        write_json(report_path, result)
        return result
    if not manager.JOBLIB_AVAILABLE:
        raise RuntimeError("joblib is required for shadow evaluation")
    bundle = manager.joblib.load(artifact)
    model = bundle.get("model") if isinstance(bundle, dict) else None
    if model is None:
        result = {
            "generated_utc": manager.iso_utc(),
            "available": False,
            "reason": "shadow evaluator supports standalone model bundles only",
            "manifest": manifest,
        }
        write_json(report_path, result)
        return result
    frame, features = load_candidate_frame(manifest)
    cutoff_raw = manifest.get("dataset_end_utc", "")
    if cutoff_raw:
        cutoff = pd.Timestamp(cutoff_raw)
        if cutoff.tzinfo is None:
            cutoff = cutoff.tz_localize("UTC")
        frame = frame[frame["time_utc"] > cutoff]
    frame = frame.sort_values(["time_utc", "instrument"]).reset_index(drop=True)
    if len(frame) < SHADOW_MIN_ROWS:
        result = {
            "generated_utc": manager.iso_utc(),
            "available": True,
            "passed": False,
            "reason": f"waiting for more post-training rows: {len(frame)}<{SHADOW_MIN_ROWS}",
            "rows": len(frame),
            "manifest": manifest,
        }
        write_json(report_path, result)
        return result
    probability = model.predict_proba(frame[features])[:, 1]
    threshold = manager.safe_float(
        (manifest.get("validation", {}).get("selected_threshold") or {}).get("threshold"),
        0.65,
    )
    selected = probability >= threshold
    selected_frame = frame.loc[selected].copy()
    values = selected_frame[manifest["outcome"]].to_numpy(dtype=float)
    outcome = manager.ContinuousResearchEngine.outcome_summary(values)
    if not selected_frame.empty:
        selected_frame["week_start"] = (
            selected_frame["time_utc"].dt.normalize()
            - pd.to_timedelta(selected_frame["time_utc"].dt.weekday, unit="D")
        )
        week_summary = (
            selected_frame.groupby("week_start")[manifest["outcome"]]
            .mean()
            .reset_index(name="mean_net")
        )
    else:
        week_summary = pd.DataFrame(columns=["week_start", "mean_net"])
    positive_weeks = int((week_summary["mean_net"] > 0).sum()) if not week_summary.empty else 0
    week_count = int(len(week_summary))
    bootstrap_lower = manager.ContinuousResearchEngine.bootstrap_lower_mean(values, seed=8843)
    concentration = manager.ContinuousResearchEngine.concentration_summary(
        selected_frame[["time_utc", "instrument"]].to_dict("records")
        if not selected_frame.empty
        else []
    )
    gate = {
        "rows_at_least_min": bool(len(frame) >= SHADOW_MIN_ROWS),
        "selected_trades_at_least_75": bool(outcome["trades"] >= SHADOW_MIN_TRADES),
        "shadow_weeks_at_least_2": bool(week_count >= SHADOW_MIN_WEEKS),
        "mean_net_positive": bool(outcome["mean_net_pips"] > 0),
        "bootstrap_lower_mean_positive": bool(bootstrap_lower > 0),
        "profit_factor_at_least_1_20": bool(outcome["profit_factor"] >= SHADOW_MIN_PROFIT_FACTOR),
        "positive_in_70pct_weeks": bool(
            positive_weeks >= max(1, math.ceil(week_count * SHADOW_MIN_POSITIVE_WEEK_SHARE))
        ),
        "top_pair_trade_share_at_most_35pct": bool(
            concentration["top_pair_trade_share"] <= manager.MAX_VALIDATION_TOP_PAIR_TRADE_SHARE
        ),
    }
    passed = all(gate.values())
    result = {
        "generated_utc": manager.iso_utc(),
        "available": True,
        "passed": passed,
        "candidate_id": manifest.get("candidate_id", ""),
        "experiment_id": manifest.get("experiment_id", ""),
        "rows": int(len(frame)),
        "threshold": threshold,
        "selected": {
            **outcome,
            "positive_weeks": positive_weeks,
            "weeks": week_count,
            "bootstrap_lower_mean_net_pips": bootstrap_lower,
            **concentration,
        },
        "gate": gate,
        "manifest": manifest,
    }
    write_json(report_path, result)
    if passed:
        canary_ready = {
            **manifest,
            "stage": "shadow",
            "shadow_passed": True,
            "technical_account_activation": False,
            "canary_assignment_required": True,
            "shadow_report_path": str(report_path),
            "shadow": result,
            "reason": "Shadow validation passed on post-training data; explicit canary assignment is now allowed.",
        }
        write_json(manager.DIRS["promotions"] / "canary_ready.json", canary_ready)
        cid = str(manifest.get("candidate_id") or "")
        if cid:
            ModelLifecycleRegistry(
                manager.DIRS["registry"],
                manager.DIRS["promotions"],
            ).set_stage(
                cid,
                "shadow",
                reason="post-training shadow validation passed",
                payload={"report_path": str(report_path)},
            )
    return result


def main() -> int:
    while True:
        try:
            result = evaluate_once()
            print(json.dumps({
                "time_utc": manager.iso_utc(),
                "passed": result.get("passed", False),
                "reason": result.get("reason", ""),
                "rows": result.get("rows", 0),
                "experiment_id": result.get("experiment_id", ""),
            }, default=str), flush=True)
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            manager.log_error("shadow_candidate_evaluator", exc)
        time.sleep(SHADOW_SLEEP_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
