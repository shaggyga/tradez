#!/usr/bin/env python3
"""Compare modern tabular challengers on the cost-aware event-meta contract."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

import oanda_event_meta_model_pipeline as event_meta
import oanda_gpt_training_strategy_manager as manager


PROJECT_ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "training_sets"
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "reports" / "modern_model_gap"
MODEL_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "models" / "modern_model_gap"
DEFAULT_MODELS = ("hist_gradient_boosting", "extra_trees", "catboost", "ngboost")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def latest_default_dataset() -> Path:
    candidates = sorted(
        TRAINING_ROOT.glob("s5_event_meta_training_set_*.parquet"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No S5 event-meta parquet dataset is available")
    return candidates[0]


def required_columns() -> list[str]:
    return list(
        dict.fromkeys(
            [
                "time_utc",
                "realized_pips",
                "realized_r",
                event_meta.TARGET,
                *event_meta.NUMERIC_FEATURES,
                *event_meta.CATEGORICAL_FEATURES,
            ]
        )
    )


def load_dataset(path: Path, max_rows: int = 0) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = required_columns()
    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path, columns=columns)
    else:
        frame = pd.read_csv(path, usecols=columns)
    source_rows = len(frame)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=columns).sort_values(
        ["time_utc", "instrument", "direction", "target_r_multiple"]
    )
    complete_rows = len(frame)
    if max_rows > 0 and len(frame) > max_rows:
        frame = frame.tail(max_rows)
    frame = frame.reset_index(drop=True)
    if frame.empty or frame[event_meta.TARGET].nunique() < 2:
        raise ValueError("The benchmark dataset has no usable binary target variation")
    return frame, {
        "source_rows": source_rows,
        "complete_rows": complete_rows,
        "benchmark_rows": len(frame),
        "dropped_incomplete_rows": source_rows - complete_rows,
        "start_utc": frame["time_utc"].min().isoformat(),
        "end_utc": frame["time_utc"].max().isoformat(),
        "positive_rate": float(frame[event_meta.TARGET].mean()),
    }


def distribution_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return ""


def atomic_joblib_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        joblib.dump(value, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_json_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def evaluate_model(name: str, frame: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    validation = event_meta.evaluate_candidate(name, frame)
    artifact, holdout = event_meta.final_fit(name, frame)
    return artifact, {
        "status": "evaluated",
        "model": name,
        "walk_forward": validation,
        "holdout": holdout,
    }


def run_benchmark(
    dataset_path: Path,
    models: list[str],
    max_rows: int = 0,
) -> dict[str, Any]:
    frame, dataset_summary = load_dataset(dataset_path, max_rows=max_rows)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for name in models:
        print(f"[modern-gap] evaluating={name} rows={len(frame)}", flush=True)
        try:
            artifact, result = evaluate_model(name, frame)
            artifact_path = MODEL_ROOT / f"{name}_event_meta_latest.joblib"
            atomic_joblib_dump(artifact, artifact_path)
            artifact_row = {
                "model": name,
                "path": str(artifact_path),
                "bytes": artifact_path.stat().st_size,
                "sha256": sha256_file(artifact_path),
            }
            result["artifact"] = artifact_row
            artifacts.append(artifact_row)
        except Exception as exc:
            result = {
                "status": "error",
                "model": name,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        results.append(result)

    evaluated = [row for row in results if row["status"] == "evaluated"]
    ranked = sorted(
        evaluated,
        key=lambda row: (
            bool(row["holdout"]["production_gate"]["passed"]),
            row["walk_forward"]["mean_auc"],
            -row["walk_forward"]["mean_brier"],
        ),
        reverse=True,
    )
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "execution_policy": "shadow_only",
        "account_wired": False,
        "dataset": {
            "path": str(dataset_path),
            "bytes": dataset_path.stat().st_size,
            "sha256": sha256_file(dataset_path),
            **dataset_summary,
        },
        "feature_contract": {
            "numeric_features": event_meta.NUMERIC_FEATURES,
            "categorical_features": event_meta.CATEGORICAL_FEATURES,
            "target": event_meta.TARGET,
            "purge_minutes": event_meta.PURGE_MINUTES,
            "cost_semantics": (
                "Bid/ask path labels include the event pipeline's 0.20 pip "
                "slippage reserve; realized_pips and realized_r are evaluated "
                "after one-action-per-time-and-instrument consolidation."
            ),
        },
        "dependencies": {
            "catboost": distribution_version("catboost"),
            "ngboost": distribution_version("ngboost"),
            "pandas": distribution_version("pandas"),
            "scikit_learn": distribution_version("scikit-learn"),
        },
        "requested_models": models,
        "results": results,
        "ranking": [row["model"] for row in ranked],
        "research_winner": ranked[0]["model"] if ranked else None,
        "all_requested_evaluated": len(evaluated) == len(models),
        "any_production_gate_passed": any(
            row["holdout"]["production_gate"]["passed"] for row in evaluated
        ),
        "artifacts": artifacts,
        "run_id": stamp,
    }
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    timestamped = REPORT_ROOT / f"modern_tabular_gap_benchmark_{stamp}.json"
    latest = REPORT_ROOT / "modern_tabular_gap_benchmark_latest.json"
    atomic_json_dump(report, timestamped)
    atomic_json_dump(report, latest)
    report["report_paths"] = [str(timestamped), str(latest)]
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--models", nargs="+", default=list(DEFAULT_MODELS))
    parser.add_argument("--max-rows", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset = (args.dataset or latest_default_dataset()).resolve()
    report = run_benchmark(dataset, list(dict.fromkeys(args.models)), args.max_rows)
    print(
        json.dumps(
            {
                "rows": report["dataset"]["benchmark_rows"],
                "ranking": report["ranking"],
                "all_requested_evaluated": report["all_requested_evaluated"],
                "any_production_gate_passed": report["any_production_gate_passed"],
                "reports": report["report_paths"],
            },
            indent=2,
        )
    )
    return 0 if report["all_requested_evaluated"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
