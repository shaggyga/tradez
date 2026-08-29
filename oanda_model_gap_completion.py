#!/usr/bin/env python3
"""Consolidate model-gap runtime, qualification, market, and matrix evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import oanda_shared_timeframe_horizon_panel as panel
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad import oanda_shared_timeframe_horizon_panel as panel

import oanda_model_gap_registry as registry


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_OUTPUT = DATA_ROOT / "model_space" / "model_gap_completion_latest.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_optional(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def preserve_generated_utc_if_unchanged(
    report: dict[str, Any],
    previous: dict[str, Any],
) -> dict[str, Any]:
    """Keep content-addressed evidence stable across no-op audit refreshes."""

    candidate = dict(report)
    candidate_without_time = dict(candidate)
    previous_without_time = dict(previous)
    candidate_without_time.pop("generated_utc", None)
    previous_without_time.pop("generated_utc", None)
    previous_time = str(previous.get("generated_utc") or "")
    if previous_time and candidate_without_time == previous_without_time:
        candidate["generated_utc"] = previous_time
    return candidate


def evidence_source(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "present": path.is_file(),
        "bytes": path.stat().st_size if path.is_file() else 0,
        "sha256": sha256_file(path) if path.is_file() else "",
    }


def market_index(*reports: dict[str, Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for report in reports:
        for row in report.get("results") or []:
            model = str(row.get("model") or "")
            if model:
                output[model] = row
    return output


def qualification_index(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("model")): row
        for row in report.get("results") or []
        if row.get("model")
    }


def runtime_index(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("model")): row
        for row in report.get("model_runtime") or []
        if row.get("model")
    }


def highest_level(
    runtime: dict[str, Any],
    qualification: dict[str, Any],
    market: dict[str, Any],
) -> str:
    if market.get("status") in {
        "evaluated",
        "adapter_qualified",
        "bounded_market_scored",
    }:
        return "bounded_market_qualified"
    if qualification.get("status") in {
        "synthetic_qualified",
        "bounded_vault_smoke_qualified",
    }:
        return "synthetic_qualified"
    if runtime.get("status") in {"runtime_available", "source_pinned"}:
        return "runtime_available"
    return "adapter_implemented"


def compact_market(row: dict[str, Any]) -> dict[str, Any]:
    holdout = row.get("holdout") or {}
    summary = row.get("summary") or {}
    cells = (
        holdout.get("all_prediction_cell_metrics")
        or holdout.get("cell_metrics")
        or row.get("cells")
        or row.get("all_prediction_cell_metrics")
        or row.get("cell_metrics")
        or []
    )
    return {
        "status": row.get("status"),
        "cell_count": len(cells),
        "summary": summary,
        "classification": holdout.get("classification") or row.get("classification"),
        "selected": holdout.get("selected") or row.get("selected"),
        "validation": row.get("validation"),
        "backtest_scope": row.get("backtest_scope"),
        "production_gate": holdout.get("production_gate") or row.get("production_gate"),
    }


def market_backtest_complete(row: dict[str, Any]) -> bool:
    if row.get("status") not in {
        "evaluated",
        "adapter_qualified",
        "bounded_market_scored",
    }:
        return False
    validation = row.get("validation") or {}
    return validation.get("backtest_valid", True) is True


def parse_args() -> argparse.Namespace:
    reports = DATA_ROOT / "reports" / "modern_model_gap"
    model_space = DATA_ROOT / "model_space"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runtime-report",
        type=Path,
        default=model_space / "model_gap_runtime_profiles_latest.json",
    )
    parser.add_argument(
        "--qualification-report",
        type=Path,
        default=model_space / "model_gap_qualification_latest.json",
    )
    parser.add_argument(
        "--matrix-report",
        type=Path,
        default=DATA_ROOT
        / "training_sets"
        / "model_gap_full_matrix"
        / "full_matrix_latest.json",
    )
    parser.add_argument(
        "--tabular-report",
        type=Path,
        default=reports / "shared_panel_model_benchmark_latest.json",
    )
    parser.add_argument(
        "--neural-report",
        type=Path,
        default=reports / "shared_panel_neural_benchmark_latest.json",
    )
    parser.add_argument(
        "--foundation-report",
        type=Path,
        default=reports / "foundation_market_benchmark_latest.json",
    )
    parser.add_argument(
        "--remaining-market-report",
        type=Path,
        default=reports / "remaining_market_validation_latest.json",
    )
    parser.add_argument(
        "--weight-report",
        type=Path,
        default=model_space / "foundation_weight_sync_latest.json",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--require-complete", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = {
        "runtime": args.runtime_report,
        "qualification": args.qualification_report,
        "matrix": args.matrix_report,
        "tabular": args.tabular_report,
        "neural": args.neural_report,
        "foundation": args.foundation_report,
        "remaining_market": args.remaining_market_report,
        "weights": args.weight_report,
    }
    payloads = {name: load_optional(path) for name, path in paths.items()}
    runtimes = runtime_index(payloads["runtime"])
    qualifications = qualification_index(payloads["qualification"])
    markets = market_index(
        payloads["tabular"],
        payloads["neural"],
        payloads["foundation"],
        payloads["remaining_market"],
    )
    weight_status = {
        str(row.get("model")): row
        for row in payloads["weights"].get("records") or []
        if row.get("model")
    }
    models: list[dict[str, Any]] = []
    for spec in registry.MODEL_SPECS:
        runtime = runtimes.get(spec.model_id, {})
        qualification = qualifications.get(spec.model_id, {})
        market = markets.get(spec.model_id, {})
        weight = weight_status.get(spec.model_id, {})
        runtime_status = runtime.get("status") or "not_reported"
        runtime_blocked = runtime_status == "blocked"
        backtest_complete = market_backtest_complete(market)
        market_validation = market.get("validation") or {}
        models.append(
            {
                "model": spec.model_id,
                "family": spec.family,
                "provider": spec.provider,
                "evidence_level": highest_level(runtime, qualification, market),
                "runtime_status": runtime_status,
                "runtime_reason": runtime.get("reason") or runtime.get("runtime_reason"),
                "qualification_status": qualification.get("status") or "not_run",
                "weight_status": weight.get("status") or (
                    "not_required" if spec.weight_policy == "none" else "not_downloaded"
                ),
                "resolved_weight_revision": weight.get("resolved_revision"),
                "market": compact_market(market) if market else {"status": "not_run"},
                "market_backtest_required": not runtime_blocked,
                "market_backtest_complete": backtest_complete,
                "market_performance_passed": bool(
                    market_validation.get("performance_passed")
                    or ((market.get("production_gate") or {}).get("passed"))
                    or (((market.get("holdout") or {}).get("production_gate") or {}).get("passed"))
                ),
                "runtime_blocked": runtime_blocked,
                "production_eligible": False,
                "account_wired": False,
            }
        )
    matrix = payloads["matrix"].get("coverage") or {}
    required_names = (
        "runtime",
        "qualification",
        "matrix",
        "tabular",
        "neural",
        "foundation",
        "remaining_market",
        "weights",
    )
    missing_reports = [name for name in required_names if not payloads[name]]
    required_present = not missing_reports
    expected_timeframe_horizon_cells = len(panel.CANONICAL_TIMEFRAMES) * len(
        panel.CANONICAL_HORIZONS_SEC
    )
    expected_pair_cells = 68 * expected_timeframe_horizon_cells
    observed_timeframe_horizon_cells = int(
        matrix.get("observed_timeframe_horizon_cells") or 0
    )
    observed_pair_cells = int(matrix.get("observed_pair_cells") or 0)
    documented_unavailable_count = int(
        matrix.get("documented_unavailable_count") or 0
    )
    accounted_pair_cells = int(
        matrix.get("observed_or_documented_pair_cells")
        or (observed_pair_cells + documented_unavailable_count)
    )
    unaccounted_pair_cell_count = int(
        matrix.get(
            "unaccounted_pair_cell_count",
            max(expected_pair_cells - accounted_pair_cells, 0),
        )
        or 0
    )
    fully_observed = observed_pair_cells == expected_pair_cells
    matrix_contract = matrix.get("contract_complete")
    matrix_complete = (
        bool(matrix_contract)
        if matrix_contract is not None
        else accounted_pair_cells == expected_pair_cells
        and unaccounted_pair_cell_count == 0
    )
    required_market_models = [row for row in models if row["market_backtest_required"]]
    missing_market_backtests = [
        row["model"] for row in required_market_models if not row["market_backtest_complete"]
    ]
    blocked_models = [row["model"] for row in models if row["runtime_blocked"]]
    inventory_complete = required_present and matrix_complete
    market_backtest_complete_for_all_runnable = not missing_market_backtests
    evidence_complete = inventory_complete and market_backtest_complete_for_all_runnable
    incomplete_reasons: list[str] = []
    if missing_reports:
        incomplete_reasons.append(
            "missing required reports: " + ", ".join(missing_reports)
        )
    if not matrix_complete:
        incomplete_reasons.append(
            "canonical matrix contract incomplete: "
            f"{accounted_pair_cells}/{expected_pair_cells} pair-cells accounted, "
            f"{unaccounted_pair_cell_count} unaccounted"
        )
    if missing_market_backtests:
        incomplete_reasons.append(
            "runnable models missing valid market backtests: "
            + ", ".join(missing_market_backtests)
        )
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "scope": "complete modern model-gap evidence ledger",
        "execution_policy": "shadow_only_no_account_wiring",
        "sources": {name: evidence_source(path) for name, path in paths.items()},
        "matrix_coverage": {
            "expected_timeframe_horizon_cells": expected_timeframe_horizon_cells,
            "observed_timeframe_horizon_cells": observed_timeframe_horizon_cells,
            "expected_pair_cells": expected_pair_cells,
            "observed_pair_cells": observed_pair_cells,
            "documented_unavailable_count": documented_unavailable_count,
            "documented_unavailable_pair_cells": matrix.get(
                "documented_unavailable_pair_cells"
            )
            or [],
            "observed_or_documented_pair_cells": accounted_pair_cells,
            "unaccounted_pair_cell_count": unaccounted_pair_cell_count,
            "unaccounted_pair_cells": matrix.get("unaccounted_pair_cells") or [],
            "coverage_ratio": (
                observed_pair_cells / expected_pair_cells if expected_pair_cells else 0.0
            ),
            "accounted_coverage_ratio": (
                accounted_pair_cells / expected_pair_cells if expected_pair_cells else 0.0
            ),
            "total_events": matrix.get("total_events"),
            "fully_covered": fully_observed,
            "contract_complete": matrix_complete,
        },
        "models": models,
        "summary": {
            "models": len(models),
            "families": len({row["family"] for row in models}),
            "runtime_available": sum(
                row["runtime_status"] in {"runtime_available", "source_pinned"}
                for row in models
            ),
            "synthetic_or_better": sum(
                registry.evidence_at_least(row["evidence_level"], "synthetic_qualified")
                for row in models
            ),
            "bounded_market_qualified": sum(
                row["evidence_level"] == "bounded_market_qualified" for row in models
            ),
            "market_backtest_required": len(required_market_models),
            "market_backtest_complete": sum(
                row["market_backtest_complete"] for row in required_market_models
            ),
            "market_performance_passed": sum(
                row["market_performance_passed"] for row in required_market_models
            ),
            "missing_market_backtests": missing_market_backtests,
            "runtime_blocked_models": blocked_models,
            "production_eligible": 0,
            "account_wired": 0,
            "required_reports_present": required_present,
            "missing_required_reports": missing_reports,
            "matrix_complete": matrix_complete,
            "inventory_complete": inventory_complete,
            "all_runnable_models_market_backtested": market_backtest_complete_for_all_runnable,
            "evidence_complete": evidence_complete,
            "incomplete_reasons": incomplete_reasons,
        },
    }
    report = preserve_generated_utc_if_unchanged(report, load_optional(args.output))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, args.output)
    finally:
        temporary.unlink(missing_ok=True)
    print(json.dumps(report["summary"], indent=2))
    if args.require_complete and not evidence_complete:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
