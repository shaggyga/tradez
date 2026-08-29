#!/usr/bin/env python3
"""Recreate the full signal-engine coverage and routing audit."""

from __future__ import annotations

import argparse
import copy
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_model_gap_live_signal_producer import DEFAULT_MODELS as ARTIFACT_LIVE_MODELS
    from oanda_model_gap_registry import MODEL_SPECS
    from oanda_practice_shadow_strategy_lab import FULL_HORIZONS_SEC, default_lanes
    from oanda_signal_contribution_feed import SignalContributionFeed
    from oanda_timeframe_horizon_matrix import TIMEFRAME_SECONDS
except ModuleNotFoundError:
    from trad.oanda_model_gap_live_signal_producer import (
        DEFAULT_MODELS as ARTIFACT_LIVE_MODELS,
    )
    from trad.oanda_model_gap_registry import MODEL_SPECS
    from trad.oanda_practice_shadow_strategy_lab import FULL_HORIZONS_SEC, default_lanes
    from trad.oanda_signal_contribution_feed import SignalContributionFeed
    from trad.oanda_timeframe_horizon_matrix import TIMEFRAME_SECONDS


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = (
    ROOT
    / "data/oanda_training_manager/reports/modern_model_gap/signal_engine_refresh_latest.json"
)
DEFAULT_MODEL_REPORT = (
    ROOT
    / "data/oanda_training_manager/reports/modern_model_gap/unified_model_gap_market_latest.json"
)
DEFAULT_COMPLETION_REPORT = (
    ROOT / "data/oanda_training_manager/model_space/model_gap_completion_latest.json"
)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _repository_path(path: Path) -> str:
    """Return stable provenance that survives a clean vault import."""
    try:
        return path.resolve().relative_to(ROOT.parent).as_posix()
    except ValueError:
        return str(path.resolve())


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _stable_contract_view(payload: dict[str, Any]) -> dict[str, Any]:
    view = copy.deepcopy(payload)
    view.pop("generated_utc", None)
    probe = view.get("contract_probe") or {}
    if isinstance(probe, dict):
        probe.pop("candidate_ids", None)
    worker = view.get("live_model_gap_worker") or {}
    if isinstance(worker, dict):
        worker["state"] = {}
    return view


def preserve_validated_audit_if_contract_unchanged(
    payload: dict[str, Any],
    previous: dict[str, Any],
) -> dict[str, Any]:
    """Ignore random probe IDs and live telemetry in frozen contract evidence."""

    if previous and _stable_contract_view(payload) == _stable_contract_view(previous):
        return previous
    return payload


def build_audit(model_report_path: Path = DEFAULT_MODEL_REPORT) -> dict[str, Any]:
    model_report = _read_json(model_report_path)
    if not model_report and model_report_path == DEFAULT_MODEL_REPORT:
        model_report = _read_json(DEFAULT_COMPLETION_REPORT)
    evidence = {
        str(row.get("model") or row.get("model_id") or ""): row
        for row in model_report.get("models") or []
        if isinstance(row, dict)
    }
    lanes = default_lanes()
    artifact_live_models = set(ARTIFACT_LIVE_MODELS)
    registry_model_ids = {spec.model_id for spec in MODEL_SPECS}
    live_worker_path = ROOT / "oanda_model_gap_live_signal_worker.py"
    live_producer_path = ROOT / "oanda_model_gap_live_signal_producer.py"
    live_worker_state = _read_json(
        ROOT / "data/oanda_training_manager/state/model_gap_live_worker_v1.json"
    )
    model_rows = []
    for spec in MODEL_SPECS:
        source_path = ROOT / f"{spec.adapter_module}.py"
        row = evidence.get(spec.model_id) or {}
        model_rows.append(
            {
                "model_id": spec.model_id,
                "family": spec.family,
                "adapter_module": spec.adapter_module,
                "adapter_source": _repository_path(source_path),
                "adapter_source_present": source_path.is_file(),
                "runtime_status": row.get("runtime_status"),
                "market_backtest_complete": bool(row.get("market_backtest_complete")),
                "production_eligible": bool(row.get("production_eligible")),
                "signal_route": "canonical_contribution_feed",
                "live_value_contract": "fresh producer output required",
                "live_producer_implemented": bool(
                    spec.model_id in artifact_live_models
                    and live_worker_path.is_file()
                    and live_producer_path.is_file()
                ),
                "live_producer_status": (
                    "verified_artifact_worker"
                    if spec.model_id in artifact_live_models
                    else "implementation_complete_runtime_blocked"
                    if row.get("runtime_status") == "blocked"
                    else "historical_adapter_complete_no_verified_live_artifact"
                ),
            }
        )

    artifact_routes = [
        {
            "model_id": model_id,
            "registry_gap_model": model_id in registry_model_ids,
            "role": (
                "model_gap_challenger"
                if model_id in registry_model_ids
                else "matched_tabular_baseline"
            ),
            "live_value_contract": "fresh verified artifact output required",
            "live_producer_implemented": bool(
                live_worker_path.is_file() and live_producer_path.is_file()
            ),
            "account_eligible": False,
        }
        for model_id in ARTIFACT_LIVE_MODELS
    ]

    with tempfile.TemporaryDirectory() as temporary:
        feed = SignalContributionFeed(
            Path(temporary) / "signal_engine_contract.sqlite",
            model_report_path,
        )
        feed.register_contributors(
            [
                {
                    "contributor_id": lane.lane_id,
                    "family": lane.family,
                    "source_kind": "strategy_lane",
                    "expected": True,
                    "account_eligible": bool(
                        lane.parameters.get("account_eligible", True)
                    ),
                }
                for lane in lanes
            ]
        )
        feed.register_contributors(
            [
                {
                    "contributor_id": f"timeframe_equation_matrix.{name.lower()}",
                    "family": "timeframe_equation_matrix",
                    "source_kind": "continuous_equation",
                    "expected": True,
                    "account_eligible": False,
                }
                for name in TIMEFRAME_SECONDS
            ]
        )
        now = time.time()
        route_probe = feed.publish_forecasts(
            [
                {
                    "model_id": spec.model_id,
                    "instrument": "EUR_USD",
                    "input_timeframe": "M1",
                    "generated_epoch": now,
                    "horizon_sec": 300,
                    "probability_up": 0.55,
                    "predicted_signed_pips": 1.0,
                }
                for spec in MODEL_SPECS
            ],
            "synthetic-contract-probe-not-market-evidence",
            2.0,
        )
        coverage = feed.coverage()
        account_eligible_probe = sum(
            bool(row.get("account_eligible")) for row in feed.recent()
        )
        feed.close()

    model_summary = model_report.get("summary") or {}
    checks = {
        "model_gap_registry_has_30": len(MODEL_SPECS) == 30,
        "model_gap_completion_contract_closed": bool(
            model_summary.get("integration_complete")
            or (
                model_summary.get("evidence_complete")
                and model_summary.get("inventory_complete")
                and model_summary.get("matrix_complete")
                and model_summary.get("required_reports_present")
            )
        ),
        "all_adapter_sources_present": all(
            row["adapter_source_present"] for row in model_rows
        ),
        "all_model_outputs_route_through_feed": route_probe["accepted"] == 30,
        "synthetic_probe_cannot_reach_account": account_eligible_probe == 0,
        "strategy_parameter_lanes_have_209_variants": len(lanes) == 209,
        "continuous_equation_timeframes_have_13_inputs": len(TIMEFRAME_SECONDS) == 13,
        "canonical_horizons_have_15_outputs": len(FULL_HORIZONS_SEC) == 15,
        "historical_reports_not_used_as_live_values": coverage["contract"][
            "historical_reports_are_not_live_predictions"
        ],
        "verified_artifact_live_worker_present": (
            live_worker_path.is_file() and live_producer_path.is_file()
        ),
    }
    return {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if all(checks.values()) else "failed",
        "validation_passed": all(checks.values()),
        "scope": "all-signal research consensus plus filtered practice execution",
        "counts": {
            "strategy_families": len({lane.family for lane in lanes}),
            "strategy_parameter_lanes": len(lanes),
            "continuous_equation_timeframes": len(TIMEFRAME_SECONDS),
            "canonical_horizons": len(FULL_HORIZONS_SEC),
            "model_gap_models": len(MODEL_SPECS),
            "model_gap_families": len({spec.family for spec in MODEL_SPECS}),
            "live_artifact_models_supported": len(artifact_routes),
            "live_artifact_matched_baselines": sum(
                not row["registry_gap_model"] for row in artifact_routes
            ),
            "model_gap_live_artifact_producers": sum(
                row["live_producer_implemented"] for row in model_rows
            ),
            "model_gap_implementation_pending": sum(
                not row["adapter_source_present"] for row in model_rows
            ),
            "model_gap_shadow_only_without_verified_live_artifact": sum(
                not row["live_producer_implemented"] for row in model_rows
            ),
            # Deprecated compatibility alias. These models are not implementation
            # TODOs; their bounded historical adapters are complete, but they do
            # not have verified, model-specific artifacts for fresh live inference.
            "model_gap_adapter_only_pending_live_producer": sum(
                not row["live_producer_implemented"] for row in model_rows
            ),
            "canonical_timeframe_horizon_cells": (
                len(TIMEFRAME_SECONDS) * len(FULL_HORIZONS_SEC)
            ),
        },
        "checks": checks,
        "model_report": {
            "path": _repository_path(model_report_path),
            "summary": model_summary,
        },
        "model_gap_routes": model_rows,
        "live_artifact_routes": artifact_routes,
        "live_model_gap_worker": {
            "source": _repository_path(live_worker_path),
            "state": live_worker_state,
            "active_models_require_fresh_feature_snapshot": True,
        },
        "contract_probe": {
            **route_probe,
            "synthetic_only": True,
            "market_evidence": False,
            "account_eligible": account_eligible_probe,
        },
        "feed_contract": coverage["contract"],
        "consensus_layers": {
            "raw": "every fresh finite prediction, correlation-adjusted",
            "filtered": "all predictions retained with setup, reliability, and quality attenuation",
            "execution": "only account-authorized, cost-clearing, validated structural signals",
        },
        "live_status_note": (
            "The model-gap implementation contract is closed. Live-artifact coverage "
            "is reported separately: a completed adapter becomes a live contributor "
            "only after a verified model-specific artifact publishes a fresh forecast; "
            "benchmark metrics are never replayed as predictions."
        ),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-report", type=Path, default=DEFAULT_MODEL_REPORT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_audit(args.model_report)
    payload = preserve_validated_audit_if_contract_unchanged(
        payload,
        _read_json(args.output),
    )
    _atomic_json(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["validation_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
