#!/usr/bin/env python3
"""Retrain bounded shadow challengers when enough new prospective data matures."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TRAD_ROOT = Path(__file__).resolve().parent
if str(TRAD_ROOT) not in sys.path:
    sys.path.insert(0, str(TRAD_ROOT))

try:
    import oanda_shared_panel_model_benchmark as benchmark
except ModuleNotFoundError:
    from trad import oanda_shared_panel_model_benchmark as benchmark


ROOT = TRAD_ROOT
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_PANEL = (
    DATA
    / "training_sets"
    / "prospective_live"
    / "prospective_live_shared_panel_latest.parquet"
)
DEFAULT_PANEL_REPORT = (
    DATA
    / "reports"
    / "modern_model_gap"
    / "prospective_live_panel_latest.json"
)
DEFAULT_HISTORICAL_PANELS = DATA / "training_sets" / "shared_timeframe_horizon_panel"
DEFAULT_CANDIDATES = DATA / "reports" / "modern_model_gap" / "prospective_candidates"
DEFAULT_STATE = DATA / "state" / "prospective_model_refresh_v1.json"
DEFAULT_ACTIVE_REPORT = (
    DATA
    / "reports"
    / "modern_model_gap"
    / "shared_panel_model_benchmark_latest.json"
)
DEFAULT_ACTIVE_MODELS = DATA / "models" / "modern_model_gap"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def robust_results(report: dict[str, Any], models: list[str]) -> dict[str, bool]:
    rows = {
        str(row.get("model") or ""): row
        for row in report.get("results") or []
        if isinstance(row, dict)
    }
    return {
        model: bool(
            rows.get(model, {}).get("status") == "evaluated"
            and (
                (rows.get(model, {}).get("holdout") or {}).get("production_gate")
                or {}
            ).get("passed")
        )
        for model in models
    }


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + f".{os.getpid()}.tmp")
    try:
        shutil.copy2(source, temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def activate_shadow_candidate(
    report: dict[str, Any],
    candidate_model_root: Path,
    active_model_root: Path,
    active_report: Path,
    models: list[str],
    run_id: str,
    *,
    activation_policy: str = "all requested challengers passed robust holdout gate",
    performance_gate_required: bool = True,
) -> dict[str, Any]:
    gate_results = robust_results(report, models)
    if performance_gate_required and not all(gate_results.values()):
        raise RuntimeError("shadow activation requires every requested performance gate")
    archive = active_model_root / "archive" / run_id
    archive.mkdir(parents=True, exist_ok=True)
    if active_report.is_file():
        shutil.copy2(active_report, archive / active_report.name)
    for model in models:
        current = active_model_root / f"{model}_shared_panel_latest.joblib"
        if current.is_file():
            shutil.copy2(current, archive / current.name)
    activated_artifacts: list[dict[str, Any]] = []
    records = {
        str(row.get("model") or ""): row
        for row in report.get("artifacts") or []
        if isinstance(row, dict)
    }
    for model in models:
        source = candidate_model_root / f"{model}_shared_panel_latest.joblib"
        destination = active_model_root / source.name
        if not source.is_file():
            raise FileNotFoundError(source)
        expected_sha = str((records.get(model) or {}).get("sha256") or "")
        source_sha = benchmark.sha256_file(source)
        if not expected_sha or source_sha.lower() != expected_sha.lower():
            raise RuntimeError(f"candidate artifact hash mismatch: {model}")
        atomic_copy(source, destination)
        record = dict(records.get(model) or {})
        record["model"] = model
        record["path"] = str(destination.resolve())
        record["bytes"] = destination.stat().st_size
        record["sha256"] = benchmark.sha256_file(destination)
        activated_artifacts.append(record)
    activated = json.loads(json.dumps(report, default=str))
    activated["artifacts"] = activated_artifacts
    activated["shadow_activation"] = {
        "activated_utc": utc_iso(),
        "policy": activation_policy,
        "performance_gate_required": bool(performance_gate_required),
        "performance_gate_passed": all(gate_results.values()),
        "account_eligible": False,
        "previous_artifacts_archive": str(archive.resolve()),
    }
    benchmark.atomic_json_dump(activated, active_report)
    return activated["shadow_activation"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--panel-report", type=Path, default=DEFAULT_PANEL_REPORT)
    parser.add_argument("--historical-panel-dir", type=Path, default=DEFAULT_HISTORICAL_PANELS)
    parser.add_argument("--candidate-root", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--active-report", type=Path, default=DEFAULT_ACTIVE_REPORT)
    parser.add_argument("--active-model-root", type=Path, default=DEFAULT_ACTIVE_MODELS)
    parser.add_argument("--models", default="catboost,ngboost")
    parser.add_argument("--min-events", type=int, default=5000)
    parser.add_argument("--min-new-events", type=int, default=1000)
    parser.add_argument("--max-events", type=int, default=250000)
    parser.add_argument("--min-train-events", type=int, default=2500)
    parser.add_argument("--min-test-events", type=int, default=500)
    parser.add_argument("--no-activate", action="store_true")
    args = parser.parse_args(argv)
    if min(
        args.min_events,
        args.min_new_events,
        args.max_events,
        args.min_train_events,
        args.min_test_events,
    ) <= 0:
        raise SystemExit("model refresh sample thresholds must be positive")
    return args


def run(args: argparse.Namespace) -> dict[str, Any]:
    panel_report = read_json(args.panel_report)
    artifact = panel_report.get("artifact") or {}
    events = int(artifact.get("events") or 0)
    panel_sha = str(artifact.get("sha256") or "")
    previous = read_json(args.state)
    base = {
        "schema_version": 1,
        "updated_utc": utc_iso(),
        "panel": str(args.panel.resolve()),
        "panel_sha256": panel_sha,
        "prospective_events": events,
        "minimum_events": args.min_events,
        "minimum_new_events": args.min_new_events,
        "account_execution_authorized": False,
    }
    if (
        panel_report.get("status") != "ready"
        or not args.panel.is_file()
        or not panel_sha
        or events < args.min_events
    ):
        state = {**base, "status": "waiting_for_matured_prospective_data"}
        atomic_json(args.state, state)
        return state
    last_events = int(previous.get("last_evaluated_events") or 0)
    if (
        previous.get("last_evaluated_panel_sha256") == panel_sha
        or events - last_events < args.min_new_events
    ):
        state = {
            **base,
            **{
                key: value
                for key, value in previous.items()
                if key.startswith("last_")
            },
            "status": "waiting_for_additional_matured_events",
        }
        atomic_json(args.state, state)
        return state
    models = [value.strip() for value in args.models.split(",") if value.strip()]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_root = args.candidate_root / run_id
    report_root = run_root / "reports"
    model_root = run_root / "models"
    historical = (
        sorted(args.historical_panel_dir.rglob("*.parquet"))
        if args.historical_panel_dir.is_dir()
        else []
    )
    panel_paths = [*historical, args.panel]
    report = benchmark.run_benchmark(
        panel_paths,
        models,
        max_events=args.max_events,
        min_train_events=args.min_train_events,
        min_test_events=args.min_test_events,
        persist_artifacts=True,
        report_root=report_root,
        model_root=model_root,
    )
    robust = robust_results(report, models)
    activate = bool(models and all(robust.values()) and not args.no_activate)
    activation: dict[str, Any] = {}
    if activate:
        activation = activate_shadow_candidate(
            report,
            model_root,
            args.active_model_root,
            args.active_report,
            models,
            run_id,
        )
    state = {
        **base,
        "status": "shadow_candidate_activated" if activate else "candidate_retained_not_activated",
        "last_evaluated_panel_sha256": panel_sha,
        "last_evaluated_events": events,
        "last_evaluated_utc": utc_iso(),
        "models": models,
        "historical_panels_included": [str(path.resolve()) for path in historical],
        "candidate_run_root": str(run_root.resolve()),
        "candidate_report": str((report_root / "shared_panel_model_benchmark_latest.json").resolve()),
        "robust_holdout_gate": robust,
        "all_requested_robust": bool(models and all(robust.values())),
        "activated": activate,
        "activation": activation,
        "contract": {
            "prospective_panel_required": True,
            "historical_panels_included_when_available": True,
            "all_requested_models_must_pass_before_replacement": True,
            "replacement_remains_shadow_only": True,
            "account_promotion_is_separate": True,
        },
    }
    atomic_json(args.state, state)
    return state


def main(argv: list[str] | None = None) -> int:
    state = run(parse_args(argv))
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
