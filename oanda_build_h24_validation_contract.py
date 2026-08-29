#!/usr/bin/env python3
"""Build the current credential-free full-matrix vault validation contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE = ROOT / "trad" / "config" / "vault_validation_recreation.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def evidence(root: Path, relative: str, assertions: dict[str, Any] | None = None) -> dict[str, Any]:
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(f"validation evidence is missing: {path}")
    row: dict[str, Any] = {"path": relative, "sha256": sha256_file(path)}
    if assertions:
        row["assertions"] = assertions
    return row


def unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def build_contract(root: Path, base_path: Path) -> dict[str, Any]:
    base = json.loads(base_path.read_text(encoding="utf-8"))
    old_profile = next(iter(base.get("profiles", {}).values()), {})
    manager = "trad/data/oanda_training_manager"
    reports = f"{manager}/reports/modern_model_gap"
    model_space = f"{manager}/model_space"
    fixtures = f"{model_space}/validation_fixtures"
    arima = f"{manager}/arima_baselines"

    rows = [
        evidence(
            root,
            f"{model_space}/model_gap_qualification_latest.json",
            {
                "summary.results": 19,
                "summary.synthetic_qualified": 18,
                "summary.blocked": 1,
                "summary.failed": 0,
                "summary.production_eligible": 0,
                "summary.account_wired": 0,
                "registry.summary.models": 30,
                "registry.summary.families": 7,
            },
        ),
        evidence(
            root,
            f"{model_space}/model_gap_runtime_profiles_latest.json",
            {
                "summary.models_runtime_available": 27,
                "summary.source_pins_valid": 2,
                "summary.failed": 0,
                "summary.account_wired": 0,
                "explicit_blockers.$len": 2,
                "model_runtime.$len": 30,
            },
        ),
        evidence(
            root,
            f"{model_space}/model_gap_completion_latest.json",
            {
                "summary.models": 30,
                "summary.families": 7,
                "summary.runtime_available": 28,
                "summary.bounded_market_qualified": 28,
                "summary.market_backtest_complete": 28,
                "summary.market_performance_passed": 0,
                "summary.missing_market_backtests.$len": 0,
                "summary.all_runnable_models_market_backtested": True,
                "summary.production_eligible": 0,
                "summary.account_wired": 0,
                "summary.matrix_complete": True,
                "summary.evidence_complete": True,
                "matrix_coverage.observed_pair_cells": 14102,
                "matrix_coverage.expected_pair_cells": 14144,
                "matrix_coverage.documented_unavailable_count": 42,
                "matrix_coverage.observed_or_documented_pair_cells": 14144,
                "matrix_coverage.unaccounted_pair_cell_count": 0,
                "matrix_coverage.contract_complete": True,
            },
        ),
        evidence(
            root,
            f"{model_space}/foundation_weight_sync_latest.json",
            {
                "summary.requested": 7,
                "summary.available": 7,
                "summary.failed": 0,
                "summary.account_wired": 0,
            },
        ),
        evidence(
            root,
            f"{manager}/training_sets/model_gap_full_matrix/full_matrix_latest.json",
            {
                "status": "complete_with_documented_unavailable",
                "coverage.expected_timeframe_horizon_cells": 208,
                "coverage.observed_timeframe_horizon_cells": 208,
                "coverage.expected_pair_cells": 14144,
                "coverage.observed_pair_cells": 14102,
                "coverage.documented_unavailable_count": 42,
                "coverage.observed_or_documented_pair_cells": 14144,
                "coverage.unaccounted_pair_cell_count": 0,
                "coverage.total_events": 4416423,
                "coverage.panels.$len": 22,
                "coverage.fully_covered": False,
                "coverage.contract_complete": True,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{reports}/shared_panel_model_benchmark_latest.json",
            {
                "dataset.events": 150000,
                "dataset.instruments": 68,
                "dataset.timeframes.$len": 13,
                "dataset.horizons_sec.$len": 16,
                "requested_models.$len": 4,
                "results.$len": 4,
                "artifacts.$len": 4,
                "all_requested_evaluated": True,
                "any_production_gate_passed": False,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{reports}/shared_panel_neural_benchmark_latest.json",
            {
                "dataset.events": 150000,
                "dataset.instruments": 68,
                "dataset.timeframes.$len": 13,
                "dataset.horizons_sec.$len": 16,
                "dataset.sources.$len": 22,
                "requested_models.$len": 2,
                "results.$len": 2,
                "artifacts.$len": 2,
                "all_requested_qualified": True,
                "any_production_gate_passed": False,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{reports}/shared_panel_neural_long_horizon_benchmark_latest.json",
            {
                "dataset.events": 556475,
                "dataset.instruments": 68,
                "dataset.timeframes.$len": 2,
                "dataset.horizons_sec.$len": 16,
                "dataset.sources.$len": 4,
                "requested_models.$len": 2,
                "results.$len": 2,
                "artifacts.$len": 2,
                "all_requested_qualified": True,
                "any_production_gate_passed": False,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{reports}/foundation_market_benchmark_latest.json",
            {
                "summary.requested": 7,
                "summary.scored": 7,
                "summary.failed_or_blocked": 0,
                "summary.production_eligible": 0,
                "summary.account_wired": 0,
                "results.$len": 7,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{reports}/remaining_market_validation_latest.json",
            {
                "dataset.instruments": 68,
                "dataset.events": 876369,
                "dataset.timeframe_horizon_cells": 208,
                "dataset.pair_cells": 14102,
                "summary.requested": 17,
                "summary.backtest_valid": 17,
                "summary.backtest_failed_or_error": 0,
                "summary.performance_passed": 0,
                "summary.all_requested_backtested": True,
                "results.$len": 17,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{reports}/unified_model_gap_market_latest.json",
            {
                "summary.models": 30,
                "summary.runtime_available": 28,
                "summary.runtime_blocked": 2,
                "summary.market_backtested": 28,
                "summary.long_horizon_coverage_complete": 28,
                "summary.all_requested_backtested": True,
                "summary.all_runnable_long_horizons_covered": True,
                "summary.canonical_matrix_artifact_complete": True,
                "summary.inventory_complete": True,
                "summary.production_eligible": 0,
                "summary.account_wired": 0,
                "summary.integration_complete": True,
                "dataset.expected_pair_cells": 14144,
                "dataset.observed_pair_cells": 14102,
                "dataset.documented_unavailable_count": 42,
                "dataset.observed_or_documented_pair_cells": 14144,
                "dataset.unaccounted_pair_cell_count": 0,
            },
        ),
        evidence(
            root,
            f"{reports}/signal_audit_20260721_latest.json",
            {
                "heartbeat.accepted_lane_events": 111,
                "heartbeat.api_errors": 0,
                "execution.selected": 0,
                "execution.fills": 0,
                "execution.closes": 0,
                "process_state_at_final_audit.bots_running": False,
            },
        ),
        evidence(
            root,
            f"{reports}/signal_engine_refresh_latest.json",
            {
                "status": "passed",
                "validation_passed": True,
                "counts.strategy_families": 52,
                "counts.strategy_parameter_lanes": 208,
                "counts.continuous_equation_timeframes": 13,
                "counts.canonical_horizons": 16,
                "counts.canonical_timeframe_horizon_cells": 208,
                "counts.model_gap_models": 30,
                "counts.model_gap_live_artifact_producers": 2,
                "counts.model_gap_adapter_only_pending_live_producer": 28,
                "counts.live_artifact_models_supported": 4,
                "counts.live_artifact_matched_baselines": 2,
                "checks.all_adapter_sources_present": True,
                "checks.all_model_outputs_route_through_feed": True,
                "checks.synthetic_probe_cannot_reach_account": True,
                "checks.verified_artifact_live_worker_present": True,
                "contract_probe.accepted": 30,
                "contract_probe.account_eligible": 0,
                "contract_probe.market_evidence": False,
            },
        ),
        evidence(
            root,
            f"{manager}/training_sets/model_gap_full_matrix/"
            "full_68x13x16_oanda_api_20260722/coverage_repair_report.json",
            {
                "status": "complete_with_documented_unavailable",
                "after.expected_pair_cells": 14144,
                "after.observed_pair_cells": 14102,
                "after.documented_unavailable_count": 42,
                "after.unaccounted_pair_cell_count": 0,
                "after.contract_complete": True,
                "after.fully_covered": False,
                "account_wired": False,
            },
        ),
        evidence(
            root,
            f"{arima}/arima_h1_h24_recovery_20260721_summary.json",
            {
                "rows": 1260,
                "successful_rows": 1260,
                "error_count": 0,
                "promotion_ready_count": 24,
                "horizons_requested.$len": 6,
                "windows_requested": 2,
            },
        ),
        evidence(
            root,
            f"{arima}/arima_h4_h24_recovery_20260721_summary.json",
            {
                "rows": 840,
                "successful_rows": 840,
                "error_count": 0,
                "promotion_ready_count": 42,
                "horizons_requested.$len": 4,
                "windows_requested": 2,
            },
        ),
    ]
    for relative in (
        f"{arima}/arima_h1_h24_recovery_20260721_results.csv",
        f"{arima}/arima_h4_h24_recovery_20260721_results.csv",
        f"{fixtures}/foundation_h1_h24_recovery_20260721.npz",
        f"{fixtures}/foundation_h1_h24_recovery_20260721.npz.manifest.json",
        f"{fixtures}/foundation_h4_h24_recovery_20260721.npz",
        f"{fixtures}/foundation_h4_h24_recovery_20260721.npz.manifest.json",
    ):
        rows.append(evidence(root, relative))

    compile_files = unique(
        list(old_profile.get("compile_files") or [])
        + [
            "trad/oanda_foundation_market_report_merge.py",
            "trad/oanda_model_gap_unified_report.py",
            "trad/oanda_build_h24_validation_contract.py",
            "trad/oanda_arima_multiframe_sweep.py",
            "trad/oanda_signal_contribution_feed.py",
            "trad/oanda_signal_engine_refresh_audit.py",
            "trad/oanda_lane_promotion.py",
            "trad/oanda_practice_shadow_strategy_lab.py",
            "trad/oanda_practice_live_dashboard.py",
        ]
    )
    pytest_files = unique(
        list(old_profile.get("pytest_files") or [])
        + [
            "trad/test_oanda_foundation_market_report_merge.py",
            "trad/test_oanda_model_gap_unified_report.py",
            "trad/test_oanda_arima_multiframe_sweep.py",
            "trad/test_oanda_signal_contribution_feed.py",
            "trad/test_oanda_signal_engine_refresh_audit.py",
            "trad/test_oanda_lane_promotion.py",
            "trad/test_oanda_practice_shadow_strategy_lab.py",
            "trad/test_oanda_practice_live_dashboard.py",
        ]
    )
    profile = {
        "description": "Verify the July 22 full 68x13x16 checkpoint without D-drive access or account processes.",
        "evidence": rows,
        "bounded_fixture": old_profile.get("bounded_fixture") or {},
        "compile_files": compile_files,
        "pytest_files": pytest_files,
        "pytest_minimum_passed": int(old_profile.get("pytest_minimum_passed", 227)),
        "pytest_isolate_files": True,
        "compile_isolate_files": True,
        "compile_timeout_seconds": 120,
        "pytest_timeout_seconds": 900,
        "checks": [],
        "full_reruns": [],
    }
    return {
        "schema_version": 2,
        "updated_utc": utc_iso(),
        "execution_policy": "offline_validation_only_no_D_access_no_account_processes",
        "profiles": {"model_gap_h24_recovery": profile},
    }


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--base", type=Path, default=DEFAULT_BASE)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    base = args.base.resolve()
    output = args.output.resolve() if args.output else base
    contract = build_contract(root, base)
    atomic_json(output, contract)
    summary = contract["profiles"]["model_gap_h24_recovery"]
    print(
        json.dumps(
            {
                "output": str(output),
                "evidence_files": len(summary["evidence"]),
                "compile_files": len(summary["compile_files"]),
                "pytest_files": len(summary["pytest_files"]),
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
