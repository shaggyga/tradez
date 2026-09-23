#!/usr/bin/env python3
"""Explicit lifecycle promotion controls.

Training never auto-assigns a live account.  Use this tool to move a
shadow-passed candidate to canary assignment, or to promote a canary-passed
candidate to technical production after a candidate-specific report passes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import oanda_gpt_training_strategy_manager as manager
from oanda_model_lifecycle import ModelLifecycleRegistry


CANARY_MIN_TRADES = 30
CANARY_MIN_PROFIT_FACTOR = 1.20


def load_json(path: Path) -> Dict[str, Any]:
    return manager.load_json(path, {})


def save_json(path: Path, obj: Dict[str, Any]) -> None:
    manager.save_json(path, obj)


def registry() -> ModelLifecycleRegistry:
    return ModelLifecycleRegistry(manager.DIRS["registry"], manager.DIRS["promotions"])


def assign_canary() -> Dict[str, Any]:
    ready_path = manager.DIRS["promotions"] / "canary_ready.json"
    ready = load_json(ready_path)
    if not ready or not ready.get("shadow_passed"):
        raise SystemExit(f"No shadow-passed canary_ready.json at {ready_path}")
    manifest = {
        **ready,
        "updated_utc": manager.iso_utc(),
        "stage": "canary",
        "target_account_role": "DUM_CANARY",
        "dum_account": manager.DEFAULT_DUM1_ACCOUNT,
        "technical_account_activation": False,
        "production_promotion_requires_canary_report": True,
        "reason": (
            "Explicit canary assignment. DUM canary executor may use this "
            "candidate at reduced risk; technical production remains disabled."
        ),
    }
    path = manager.DIRS["promotions"] / "canary_candidate.json"
    save_json(path, manifest)
    cid = str(manifest.get("candidate_id") or "")
    if cid:
        registry().set_stage(
            cid,
            "canary",
            reason="explicit canary assignment",
            payload={"manifest_path": str(path)},
        )
    return {"path": str(path), "manifest": manifest}


def canary_report_passed(report: Dict[str, Any]) -> tuple[bool, Dict[str, bool]]:
    selected = report.get("selected") or report.get("live") or report
    trades = manager.safe_float(selected.get("trades"), 0.0)
    mean_net = manager.safe_float(
        selected.get("mean_net_pips", selected.get("mean_net")),
        0.0,
    )
    pf = manager.safe_float(selected.get("profit_factor"), 0.0)
    bootstrap = manager.safe_float(
        selected.get("bootstrap_lower_mean_net_pips", selected.get("bootstrap_lower_mean")),
        -1e9,
    )
    gate = {
        "canary_trades_at_least_30": trades >= CANARY_MIN_TRADES,
        "canary_mean_net_positive": mean_net > 0,
        "canary_bootstrap_lower_mean_positive": bootstrap > 0,
        "canary_profit_factor_at_least_1_20": pf >= CANARY_MIN_PROFIT_FACTOR,
    }
    return all(gate.values()), gate


def promote_production(canary_report_path: Path) -> Dict[str, Any]:
    canary = load_json(manager.DIRS["promotions"] / "canary_candidate.json")
    if not canary:
        raise SystemExit("No canary_candidate.json exists")
    report = load_json(canary_report_path)
    passed, gate = canary_report_passed(report)
    if not passed:
        raise SystemExit(f"Canary report failed production gate: {json.dumps(gate, sort_keys=True)}")
    manifest = {
        **canary,
        "updated_utc": manager.iso_utc(),
        "stage": "production",
        "technical_account_activation": True,
        "activation_effective": True,
        "production_gate": gate,
        "canary_report_path": str(canary_report_path),
        "reason": "Explicit production promotion after candidate-specific canary report passed.",
    }
    path = manager.DIRS["promotions"] / "technical_production.json"
    save_json(path, manifest)
    cid = str(manifest.get("candidate_id") or "")
    if cid:
        registry().set_stage(
            cid,
            "production",
            reason="explicit production promotion",
            payload={"manifest_path": str(path), "canary_report_path": str(canary_report_path)},
        )
    return {"path": str(path), "manifest": manifest}


def retire(candidate_id: str, reason: str) -> Dict[str, Any]:
    rec = registry().set_stage(candidate_id, "retired", reason=reason)
    return {"candidate_id": candidate_id, "record": rec}


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage model lifecycle promotions explicitly.")
    parser.add_argument("--assign-canary", action="store_true", help="Publish canary_candidate.json from canary_ready.json.")
    parser.add_argument("--promote-production", type=Path, help="Promote canary to technical_production.json using this canary report JSON.")
    parser.add_argument("--retire", type=str, help="Candidate id to retire.")
    parser.add_argument("--reason", type=str, default="", help="Reason for retirement.")
    args = parser.parse_args()
    if args.assign_canary:
        print(json.dumps(assign_canary(), indent=2, default=str))
        return 0
    if args.promote_production:
        print(json.dumps(promote_production(args.promote_production), indent=2, default=str))
        return 0
    if args.retire:
        print(json.dumps(retire(args.retire, args.reason or "manual retirement"), indent=2, default=str))
        return 0
    parser.error("choose --assign-canary, --promote-production, or --retire")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
