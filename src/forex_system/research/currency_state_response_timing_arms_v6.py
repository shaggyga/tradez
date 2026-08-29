"""Inert Timing V6 with externally anchored self-closure.

V6 preserves V5 exactly, pins every non-self dependency to module constants,
and refuses to certify its own module/tests/manifest from caller-controlled
manifest hashes.  Only a later frozen independent-review certificate may close
that boundary.  There is no worker, write, broker, promotion, authorization,
or execution surface.
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping

from . import currency_state_response_timing_arms_v5 as v5


CONTRACT_ID = "currency_state_response_timing_arms_v6_20260817_r1"
SNAPSHOT_SCHEMA = "currency_state_response_timing_diagnostic_snapshot_v6"
RESEARCH_GENERATION = "r3_bound_response_timing_v6_r1"
CONTRACT_SHA256 = "525039ed9014943f7950cb465cebeb103332a7f317a6c47472362368def773b4"
CONTRACT_BYTES = 2_987
MANIFEST_ID = "currency_state_response_timing_arms_v6_manifest_20260817_r1"
FROZEN_UTC = "2026-08-17T14:20:00Z"
PARENT_CONTRACT_ID = v5.CONTRACT_ID
PARENT_DISPOSITION = "independent_no_approve_preserved_exact_bytes_zero_runtime_zero_canonical_evidence"

TRUSTED_ARTIFACTS = {
    "contract": (
        "config/currency_state_response_timing_arms_v6.json",
        CONTRACT_SHA256,
        CONTRACT_BYTES,
    ),
    "parent_v5_module": (
        "src/forex_system/research/currency_state_response_timing_arms_v5.py",
        "37dbaa53bd093e129a84db31aece7bfdddec37127753cc404ddef1c1a957eb04",
        18_064,
    ),
    "parent_v5_contract": (
        "config/currency_state_response_timing_arms_v5.json",
        "08674d293897332d438872f8276a46e8de3525f07e35ddb8796996b408ee8cd0",
        4_288,
    ),
    "parent_v5_manifest": (
        "config/currency_state_response_timing_arms_v5_manifest.json",
        "4103313565bb50205ac182f546769614fc07b7b85ab70293c8c89b599f7f98e1",
        3_461,
    ),
    "parent_v5_tests": (
        "test_currency_state_response_timing_arms_v5.py",
        "181657fff94bc357944e51a0e3eb189932638f93fffeca0e5b60a6f1e2ffad93",
        7_445,
    ),
}
_STATE = {"enabled": False, "registered": False, "research_worker_enabled": False, "collector_enabled": False, "canonical_output_initialized": False}
_DISPOSITION = {"barred_event_response_generations": ["v6_r1", "v6_r2", "v6_r3"], "approved_event_response_successor_bound": False, "production_dependency_gate": "closed", "blocking_reason": "event_response_v6_r4_final_approval_not_bound", "resolution_requires_new_timing_contract_id": True}
_OUTPUT = {"canonical_ledger_path": None, "canonical_state_path": None, "canonical_report_path": None, "worker_entrypoint": None, "collector_entrypoint": None, "test_fixture_builder": "forex_system.research.currency_state_response_timing_arms_v6.build_test_diagnostic_snapshot"}
_GUARD = {"research_only": True, "execution_eligible": False, "can_place_orders": False, "can_promote": False, "can_authorize": False, "supported_execution_decision": "no_trade"}
_POLICY = {"self_manifest_cannot_self_certify": True, "external_review_certificate_required": True, "event_response_dependency_is_unresolved": True, "unresolved_dependency_must_produce_zero_rows": True, **_GUARD}
_SELF_CLOSURE = {"authority": "external_frozen_independent_review_certificate", "certificate_artifact": None, "self_module_verified": False, "self_tests_verified": False, "self_manifest_verified": False, "activation_allowed": False}
_TOP_FIELDS = {"manifest_schema_version", "manifest_id", "contract_id", "research_generation", "parent_contract_id", "parent_disposition", "review_state", "candidate_frozen_utc", "independent_review_started_utc", "independent_review_completed_utc", "runtime_started_utc", "state", "dependency_disposition", "trusted_artifacts", "self_artifacts", "external_review_certificate", "output_surface", "policy"}


class ResponseTimingV6Error(ValueError):
    pass


class ResponseTimingV6DependencyError(ResponseTimingV6Error):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _exact(actual: Any, expected: Any, *, path: str) -> None:
    if type(actual) is not type(expected):
        raise ResponseTimingV6Error(f"{path}_type_mismatch")
    if type(expected) is dict:
        if set(actual) != set(expected):
            raise ResponseTimingV6Error(f"{path}_closed_schema_mismatch")
        for key, value in expected.items():
            _exact(actual[key], value, path=f"{path}.{key}")
    elif type(expected) is list:
        if len(actual) != len(expected):
            raise ResponseTimingV6Error(f"{path}_length_mismatch")
        for index, value in enumerate(expected):
            _exact(actual[index], value, path=f"{path}[{index}]")
    elif actual != expected:
        raise ResponseTimingV6Error(f"{path}_value_mismatch")


def _load_contract() -> dict[str, Any]:
    try:
        value = v5.v4._strict_json(v5.v4._read_exact(*TRUSTED_ARTIFACTS["contract"]), label="timing_v6_contract")
    except v5.v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV6Error(str(exc)) from exc
    for key, expected in {"schema_version": 6, "contract_id": CONTRACT_ID, "snapshot_schema": SNAPSHOT_SCHEMA, "research_generation": RESEARCH_GENERATION, "parent_contract_id": PARENT_CONTRACT_ID, "parent_disposition": PARENT_DISPOSITION, "candidate_frozen_utc": FROZEN_UTC}.items():
        _exact(value.get(key), expected, path=f"contract.{key}")
    _exact(value.get("state"), {**_STATE, "independent_review_state": "pending"}, path="contract.state")
    _exact(value.get("self_closure"), _SELF_CLOSURE, path="contract.self_closure")
    _exact(value.get("dependency_disposition"), _DISPOSITION, path="contract.dependency_disposition")
    _exact(value.get("output_surface"), _OUTPUT, path="contract.output_surface")
    _exact(value.get("policy"), _POLICY, path="contract.policy")
    return value


def _verify_nonself() -> dict[str, Any]:
    _load_contract()
    for name, spec in TRUSTED_ARTIFACTS.items():
        try:
            v5.v4._read_exact(*spec)
        except v5.v4.ResponseTimingV4Error as exc:
            raise ResponseTimingV6Error(f"trusted_artifact_invalid:{name}:{exc}") from exc
    status = v5.production_dependency_status()
    if status.get("approved") is not False or status.get("row_count") != 0 or status.get("proof_row_count") != 0:
        raise ResponseTimingV6Error("parent_gate_not_closed")
    return status


def production_dependency_status() -> dict[str, Any]:
    try:
        parent = _verify_nonself()
    except ResponseTimingV6Error as exc:
        reason = f"immutable_dependency_verification_failed:{exc}"
        parent = {}
    else:
        reason = _DISPOSITION["blocking_reason"]
    return {"contract_id": CONTRACT_ID, "contract_sha256": CONTRACT_SHA256, "approved": False, "blocked": True, "blocking_reasons": [reason], "canonical_universe": copy.deepcopy(parent.get("canonical_universe", {"currency_count": 21, "instrument_count": 68})), "row_count": 0, "proof_row_count": 0, "self_closure_verified": False, **_GUARD}


def require_production_dependencies() -> None:
    raise ResponseTimingV6DependencyError(_DISPOSITION["blocking_reason"])


def build_replay_diagnostic(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def build_prospective_proof(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def _snapshot_id(value: Mapping[str, Any]) -> str:
    return "currency_state_response_timing_v6_test_" + _sha256_json({key: item for key, item in value.items() if key != "snapshot_id"})


def build_test_diagnostic_snapshot(*, event_response_snapshot: Mapping[str, Any], dependency_attestation: Mapping[str, Any]) -> dict[str, Any]:
    _verify_nonself()
    try:
        result = v5.build_test_diagnostic_snapshot(event_response_snapshot=event_response_snapshot, dependency_attestation=dependency_attestation)
    except v5.ResponseTimingV5Error as exc:
        raise ResponseTimingV6Error(str(exc)) from exc
    result = copy.deepcopy(result)
    result.update({"schema_version": 6, "snapshot_schema": SNAPSHOT_SCHEMA, "snapshot_id": None, "contract_id": CONTRACT_ID, "contract_sha256": CONTRACT_SHA256, "research_generation": RESEARCH_GENERATION})
    result["snapshot_id"] = _snapshot_id(result)
    validate_test_diagnostic_snapshot(result)
    return result


def validate_test_diagnostic_snapshot(snapshot: Mapping[str, Any]) -> None:
    if type(snapshot) is not dict or set(snapshot) != v5.v4._RESULT_FIELDS:
        raise ResponseTimingV6Error("snapshot_closed_schema_mismatch")
    for key, expected in {"schema_version": 6, "snapshot_schema": SNAPSHOT_SCHEMA, "contract_id": CONTRACT_ID, "contract_sha256": CONTRACT_SHA256, "research_generation": RESEARCH_GENERATION, "test_only": True, "canonical_output": False, "proof_row_count": 0, **_GUARD}.items():
        _exact(snapshot.get(key), expected, path=f"snapshot.{key}")
    if type(snapshot.get("snapshot_id")) is not str or snapshot["snapshot_id"] != _snapshot_id(snapshot):
        raise ResponseTimingV6Error("snapshot_id_mismatch")
    parent = copy.deepcopy(snapshot)
    parent.update({"schema_version": 5, "snapshot_schema": v5.SNAPSHOT_SCHEMA, "snapshot_id": None, "contract_id": v5.CONTRACT_ID, "contract_sha256": v5.CONTRACT_SHA256, "research_generation": v5.RESEARCH_GENERATION})
    parent["snapshot_id"] = v5._v5_snapshot_id(parent)
    try:
        v5.validate_test_diagnostic_snapshot(parent)
    except v5.ResponseTimingV5Error as exc:
        raise ResponseTimingV6Error(str(exc)) from exc


def verify_candidate_manifest() -> dict[str, Any]:
    _verify_nonself()
    try:
        manifest = v5.v4._strict_json(v5.v4._stable_read("config/currency_state_response_timing_arms_v6_manifest.json"), label="timing_v6_manifest")
    except v5.v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV6Error(str(exc)) from exc
    if type(manifest) is not dict or set(manifest) != _TOP_FIELDS:
        raise ResponseTimingV6Error("manifest_closed_schema_mismatch")
    for key, expected in {"manifest_schema_version": 1, "manifest_id": MANIFEST_ID, "contract_id": CONTRACT_ID, "research_generation": RESEARCH_GENERATION, "parent_contract_id": PARENT_CONTRACT_ID, "parent_disposition": PARENT_DISPOSITION, "review_state": "pending_independent_review", "candidate_frozen_utc": FROZEN_UTC, "independent_review_started_utc": None, "independent_review_completed_utc": None, "runtime_started_utc": None, "external_review_certificate": None}.items():
        _exact(manifest.get(key), expected, path=f"manifest.{key}")
    _exact(manifest["state"], _STATE, path="manifest.state")
    _exact(manifest["dependency_disposition"], _DISPOSITION, path="manifest.dependency_disposition")
    _exact(manifest["output_surface"], _OUTPUT, path="manifest.output_surface")
    _exact(manifest["policy"], _POLICY, path="manifest.policy")
    artifacts = manifest["trusted_artifacts"]
    if type(artifacts) is not dict or set(artifacts) != set(TRUSTED_ARTIFACTS):
        raise ResponseTimingV6Error("trusted_artifact_set_mismatch")
    for name, expected in TRUSTED_ARTIFACTS.items():
        try:
            actual = v5.v4._artifact_spec(artifacts[name], path=f"manifest.trusted.{name}")
        except v5.v4.ResponseTimingV4Error as exc:
            raise ResponseTimingV6Error(str(exc)) from exc
        if actual != expected:
            raise ResponseTimingV6Error(f"trusted_artifact_spec_mismatch:{name}")
    self_artifacts = manifest["self_artifacts"]
    expected_self_paths = {"runtime_core": "src/forex_system/research/currency_state_response_timing_arms_v6.py", "focused_tests": "test_currency_state_response_timing_arms_v6.py", "manifest": "config/currency_state_response_timing_arms_v6_manifest.json"}
    if type(self_artifacts) is not dict or set(self_artifacts) != set(expected_self_paths):
        raise ResponseTimingV6Error("self_artifact_set_mismatch")
    for name, path in expected_self_paths.items():
        spec = self_artifacts[name]
        if type(spec) is not dict or set(spec) != {"relative_path", "sha256", "byte_length", "trust_state"} or spec["relative_path"] != path or spec["trust_state"] != "untrusted_until_external_frozen_review" or type(spec["sha256"]) is not str or len(spec["sha256"]) != 64 or type(spec["byte_length"]) is not int or spec["byte_length"] <= 0:
            raise ResponseTimingV6Error(f"self_artifact_schema_mismatch:{name}")
    return {"manifest_id": MANIFEST_ID, "nonself_closure_verified": True, "self_closure_verified": False, "candidate_closure_verified": False, "activation_allowed": False, "external_review_certificate_required": True}


__all__ = ["CONTRACT_ID", "CONTRACT_SHA256", "MANIFEST_ID", "ResponseTimingV6DependencyError", "ResponseTimingV6Error", "build_prospective_proof", "build_replay_diagnostic", "build_test_diagnostic_snapshot", "production_dependency_status", "validate_test_diagnostic_snapshot", "verify_candidate_manifest"]
