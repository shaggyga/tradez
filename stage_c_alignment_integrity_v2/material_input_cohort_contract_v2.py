"""Validation helpers for material input/cohort forecasting contracts.

This module is intentionally small and dependency-light. It validates the contract
that freezes a future research candidate before any fit or replay is launched.
The goal is to keep lower-model continuation runs from relabeling old evidence,
rerunning completed baselines, or starting compute without exact provenance.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

REQUIRED_TOP_LEVEL = {
    "schema",
    "step_id",
    "status",
    "selected_candidate",
    "design_sections",
    "material_difference",
    "source_artifacts",
    "baseline_controls",
    "target_support_gate",
    "chronology_gate",
    "leakage_checks",
    "resource_bounds",
    "completion_gate",
    "deferred_boundaries",
}

REQUIRED_ARTIFACT_KEYS = {"id", "path", "sha256", "role", "status"}
REQUIRED_RESOURCE_KEYS = {"max_runtime_seconds", "max_peak_disk_bytes", "max_peak_memory_bytes", "fit_allowed"}
FORBIDDEN_BOUNDARIES = {
    "GPT/advisor comparisons",
    "paid APIs",
    "broker/service/account actions",
    "D-drive investigation",
    "EUR/USD data capture",
    "live-bot changes",
}


class ContractValidationError(ValueError):
    """Raised when a material input/cohort contract is not executable enough."""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_material_input_contract(contract: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    """Validate a material input/cohort contract and return a compact receipt.

    The validator checks structure, declared artifact hashes when local paths exist,
    and safety gates. It does not authorize fitting; fit launch remains a later gate.
    """

    missing = sorted(REQUIRED_TOP_LEVEL - set(contract))
    if missing:
        raise ContractValidationError(f"missing required fields: {missing}")
    if contract["schema"] != "forex_material_input_cohort_contract.v1":
        raise ContractValidationError("unsupported schema")
    if contract["status"] not in {"frozen_no_compute", "blocked_missing_artifact", "ready_for_preflight"}:
        raise ContractValidationError("status must be frozen_no_compute, blocked_missing_artifact, or ready_for_preflight")
    if not contract["selected_candidate"].get("id"):
        raise ContractValidationError("selected_candidate.id is required")
    if not contract["selected_candidate"].get("hypothesis"):
        raise ContractValidationError("selected_candidate.hypothesis is required")
    if len(contract["design_sections"]) < 2:
        raise ContractValidationError("at least two design sections must be bound")
    if not contract["material_difference"].get("differs_from_completed"):
        raise ContractValidationError("material_difference.differs_from_completed is required")
    if not contract["baseline_controls"]:
        raise ContractValidationError("at least one baseline control is required")
    if "ridge" not in " ".join(contract["baseline_controls"]).lower():
        raise ContractValidationError("ridge baseline/control must be named")
    if "hgb" not in " ".join(contract["baseline_controls"]).lower():
        raise ContractValidationError("HGB baseline/control must be named")
    bounds = contract["resource_bounds"]
    missing_bounds = sorted(REQUIRED_RESOURCE_KEYS - set(bounds))
    if missing_bounds:
        raise ContractValidationError(f"missing resource bounds: {missing_bounds}")
    if bounds["fit_allowed"] is not False and contract["status"] == "frozen_no_compute":
        raise ContractValidationError("frozen_no_compute contracts must not allow fit launch")
    if bounds["max_runtime_seconds"] <= 0 or bounds["max_peak_disk_bytes"] <= 0 or bounds["max_peak_memory_bytes"] <= 0:
        raise ContractValidationError("resource bounds must be positive")
    missing_boundaries = sorted(FORBIDDEN_BOUNDARIES - set(contract["deferred_boundaries"]))
    if missing_boundaries:
        raise ContractValidationError(f"deferred boundaries missing: {missing_boundaries}")

    root = root or Path.cwd()
    artifact_receipts: list[dict[str, Any]] = []
    for artifact in contract["source_artifacts"]:
        missing_keys = sorted(REQUIRED_ARTIFACT_KEYS - set(artifact))
        if missing_keys:
            raise ContractValidationError(f"artifact {artifact.get('id', '<unknown>')} missing {missing_keys}")
        path = Path(artifact["path"])
        if not path.is_absolute():
            path = root / path
        exists = path.exists()
        actual_sha = _sha256(path) if exists and path.is_file() else None
        if exists and actual_sha != artifact["sha256"]:
            raise ContractValidationError(f"artifact hash mismatch for {artifact['id']}: {actual_sha} != {artifact['sha256']}")
        if artifact["status"] == "required_local" and not exists:
            raise ContractValidationError(f"required local artifact missing: {artifact['id']}")
        artifact_receipts.append({
            "id": artifact["id"],
            "path": str(path),
            "exists": exists,
            "declared_sha256": artifact["sha256"],
            "actual_sha256": actual_sha,
            "status": artifact["status"],
        })

    for gate_name in ("target_support_gate", "chronology_gate", "leakage_checks", "completion_gate"):
        gate = contract[gate_name]
        if not isinstance(gate, list) or not gate:
            raise ContractValidationError(f"{gate_name} must be a non-empty list")

    return {
        "schema": "forex_material_input_cohort_contract_validation.v1",
        "valid": True,
        "step_id": contract["step_id"],
        "selected_candidate_id": contract["selected_candidate"]["id"],
        "status": contract["status"],
        "fit_allowed": bounds["fit_allowed"],
        "artifact_receipts": artifact_receipts,
        "completion_gate_count": len(contract["completion_gate"]),
    }


def load_and_validate(path: Path, *, root: Path | None = None) -> dict[str, Any]:
    return validate_material_input_contract(json.loads(path.read_text(encoding="utf-8")), root=root)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Validate a Forex material input/cohort contract")
    parser.add_argument("contract", type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args(argv)
    receipt = load_and_validate(args.contract, root=args.root)
    text = json.dumps(receipt, indent=2) + "\n"
    if args.receipt:
        args.receipt.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
