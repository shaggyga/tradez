import hashlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stage_c_alignment_integrity_v2.material_input_cohort_contract_v2 import (
    ContractValidationError,
    validate_material_input_contract,
)


def _contract(tmp_path: Path) -> dict:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"ok": true}\n', encoding="utf-8")
    return {
        "schema": "forex_material_input_cohort_contract.v1",
        "step_id": "material_input_cohort_contract_v2",
        "status": "frozen_no_compute",
        "selected_candidate": {"id": "macro_incremental_current_matched_contract_v1", "hypothesis": "macro information adds value"},
        "design_sections": ["12.2", "27.4", "27.5"],
        "material_difference": {"differs_from_completed": ["uses macro/text/event source family rather than ridge/HGB-only matched controls"]},
        "source_artifacts": [
            {"id": "artifact", "path": str(artifact), "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(), "role": "fixture", "status": "required_local"}
        ],
        "baseline_controls": ["matched ridge baseline", "recovered HGB baseline"],
        "target_support_gate": ["same target rows must be proven before compute"],
        "chronology_gate": ["train labels mature before assessment"],
        "leakage_checks": ["future perturbation check required"],
        "resource_bounds": {"max_runtime_seconds": 60, "max_peak_disk_bytes": 1000, "max_peak_memory_bytes": 1000, "fit_allowed": False},
        "completion_gate": ["contract validates", "no fit launched"],
        "deferred_boundaries": ["GPT/advisor comparisons", "paid APIs", "broker/service/account actions", "D-drive investigation", "EUR/USD data capture", "live-bot changes"],
    }


def test_valid_contract_receipt(tmp_path: Path) -> None:
    receipt = validate_material_input_contract(_contract(tmp_path))
    assert receipt["valid"] is True
    assert receipt["fit_allowed"] is False
    assert receipt["artifact_receipts"][0]["exists"] is True


def test_rejects_missing_hgb_control(tmp_path: Path) -> None:
    contract = _contract(tmp_path)
    contract["baseline_controls"] = ["matched ridge baseline"]
    with pytest.raises(ContractValidationError, match="HGB"):
        validate_material_input_contract(contract)


def test_rejects_hash_mismatch(tmp_path: Path) -> None:
    contract = _contract(tmp_path)
    contract["source_artifacts"][0]["sha256"] = "0" * 64
    with pytest.raises(ContractValidationError, match="hash mismatch"):
        validate_material_input_contract(contract)
