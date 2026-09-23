#!/usr/bin/env python3
"""Explicit ARIMA canary promotion controls.

The ARIMA adapter writes only a pending artifact.  This tool is the explicit
bridge from that pending artifact to an active ARIMA canary manifest.  It is
safe by default: staged manifests remain paper/inactive unless execution is
explicitly enabled with a separate command-line flag and confirmation token.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
PROMOTIONS_ROOT = TRAINING_ROOT / "promotions"
PENDING_MANIFEST_PATH = PROMOTIONS_ROOT / "arima_canary_candidate_pending.json"
ACTIVE_MANIFEST_PATH = PROMOTIONS_ROOT / "arima_canary_candidate.json"
CONFIRM_ENABLE_EXECUTION = "ENABLE_ARIMA_CANARY_EXECUTION"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=json_safe),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def load_pending(path: Path = PENDING_MANIFEST_PATH) -> Dict[str, Any]:
    manifest = read_json(path, {})
    if not isinstance(manifest, dict):
        return {}
    return manifest


def validate_pending(manifest: Dict[str, Any]) -> List[str]:
    blockers: List[str] = []
    if not manifest:
        return ["missing_pending_arima_canary_manifest"]
    if str(manifest.get("stage", "")).lower() != "arima_canary_pending":
        blockers.append("pending_manifest_stage_not_arima_canary_pending")
    if not bool(manifest.get("canary_assignment_ready", False)):
        blockers.append("pending_manifest_not_canary_assignment_ready")
    if manifest.get("canary_assignment_blockers"):
        blockers.append("pending_manifest_has_assignment_blockers")
    if not bool(manifest.get("arima_canary_executor_support_available", False)):
        blockers.append("executor_does_not_support_pending_manifest")
    if not bool(manifest.get("arima_canary_order_adapter_available", False)):
        blockers.append("executor_does_not_support_order_adapter")
    if not bool(manifest.get("arima_canary_broker_execution_support_available", False)):
        blockers.append("executor_does_not_support_broker_execution")
    if int(manifest.get("signal_count") or 0) <= 0:
        blockers.append("pending_manifest_has_no_signals")
    if str(manifest.get("adapter_candidate_source", "")).lower() != "robust_arima_challengers":
        rolling_pair_gate = manifest.get("rolling_pair_gate")
        if not isinstance(rolling_pair_gate, dict) or not rolling_pair_gate.get("basket_repeated_passed"):
            blockers.append("pending_manifest_not_from_robust_or_basket_passed_source")
    return blockers


def build_active_manifest(
    pending: Dict[str, Any],
    *,
    enable_execution: bool,
    reason: str,
    pending_path: Path,
) -> Dict[str, Any]:
    now = utc_iso()
    manifest = {
        **pending,
        "updated_utc": now,
        "promoted_utc": now,
        "stage": "arima_canary",
        "source_pending_manifest_path": str(pending_path),
        "target_account_role": "arima_canary",
        "execution_enabled": bool(enable_execution),
        "activation_effective": bool(enable_execution),
        "technical_account_activation": False,
        "actual_canary_file_written": True,
        "requires_manual_assignment_to_canary_candidate_json": False,
        "requires_explicit_executor_flag": True,
        "production_promotion_requires_canary_report": True,
        "reason": reason
        or (
            "Explicit ARIMA canary assignment from validated pending manifest. "
            "Technical production remains disabled."
        ),
    }
    risk_policy = manifest.get("risk_policy")
    if not isinstance(risk_policy, dict):
        risk_policy = {}
    manifest["risk_policy"] = {
        **risk_policy,
        "account_role": "arima_canary",
        "paper_only": not bool(enable_execution),
        "requires_explicit_executor_flag": True,
    }
    return manifest


def stage_active(
    *,
    pending_path: Path = PENDING_MANIFEST_PATH,
    active_path: Path = ACTIVE_MANIFEST_PATH,
    enable_execution: bool = False,
    confirm_enable_execution: str = "",
    reason: str = "",
    dry_run: bool = True,
) -> Dict[str, Any]:
    pending = load_pending(pending_path)
    blockers = validate_pending(pending)
    if enable_execution and confirm_enable_execution != CONFIRM_ENABLE_EXECUTION:
        blockers.append("missing_enable_execution_confirmation_token")
    if blockers:
        return {
            "time_utc": utc_iso(),
            "passed": False,
            "dry_run": dry_run,
            "pending_manifest_path": str(pending_path),
            "active_manifest_path": str(active_path),
            "execution_enabled": False,
            "blockers": blockers,
        }
    manifest = build_active_manifest(
        pending,
        enable_execution=enable_execution,
        reason=reason,
        pending_path=pending_path,
    )
    if not dry_run:
        atomic_write_json(active_path, manifest)
    return {
        "time_utc": utc_iso(),
        "passed": True,
        "dry_run": dry_run,
        "pending_manifest_path": str(pending_path),
        "active_manifest_path": str(active_path),
        "wrote_active_manifest": not dry_run,
        "stage": manifest.get("stage"),
        "execution_enabled": manifest.get("execution_enabled"),
        "activation_effective": manifest.get("activation_effective"),
        "signal_count": manifest.get("signal_count", 0),
        "actionable_signal_count": manifest.get("actionable_signal_count", 0),
        "adapter_candidate_source": manifest.get("adapter_candidate_source", ""),
        "robust_shadow_pair_count": manifest.get("robust_shadow_pair_count", 0),
        "signals": [
            {
                "pair": row.get("pair"),
                "action": row.get("action"),
                "predicted_pips": row.get("predicted_pips"),
                "threshold_pips": row.get("threshold_pips"),
            }
            for row in manifest.get("signals", []) or []
            if isinstance(row, dict)
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pending", type=Path, default=PENDING_MANIFEST_PATH)
    parser.add_argument("--active", type=Path, default=ACTIVE_MANIFEST_PATH)
    parser.add_argument(
        "--stage-inactive",
        action="store_true",
        help="Write arima_canary_candidate.json with execution_enabled=false.",
    )
    parser.add_argument(
        "--enable-execution",
        action="store_true",
        help="Stage with execution_enabled=true; requires --confirm-enable-execution token.",
    )
    parser.add_argument(
        "--confirm-enable-execution",
        default="",
        help=f"Required token for --enable-execution: {CONFIRM_ENABLE_EXECUTION}",
    )
    parser.add_argument("--reason", default="")
    args = parser.parse_args()
    if args.enable_execution and not args.stage_inactive:
        parser.error("--enable-execution requires --stage-inactive")
    result = stage_active(
        pending_path=args.pending,
        active_path=args.active,
        enable_execution=args.enable_execution,
        confirm_enable_execution=args.confirm_enable_execution,
        reason=args.reason,
        dry_run=not args.stage_inactive,
    )
    print(json.dumps(result, indent=2, default=json_safe), flush=True)
    return 0 if result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
