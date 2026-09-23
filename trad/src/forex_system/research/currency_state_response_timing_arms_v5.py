"""Frozen inert Timing V5 manifest-governance successor.

V5 preserves Timing V4 byte-for-byte and delegates its already-closed
diagnostic economics, canonical-pip, numeric-UTC, and nested-schema checks to
that exact parent.  V5 adds exact value-and-type validation for every manifest
field.  It has no worker, collector, write, network, database, broker,
promotion, authorization, or execution surface.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
from typing import Any, Mapping

from . import currency_state_response_timing_arms_v4 as v4


CONTRACT_ID = "currency_state_response_timing_arms_v5_20260817_r1"
SNAPSHOT_SCHEMA = "currency_state_response_timing_diagnostic_snapshot_v5"
RESEARCH_GENERATION = "r3_bound_response_timing_v5_r1"
CONTRACT_SHA256 = "08674d293897332d438872f8276a46e8de3525f07e35ddb8796996b408ee8cd0"
CONTRACT_BYTES = 4_288
MANIFEST_ID = "currency_state_response_timing_arms_v5_manifest_20260817_r1"
FROZEN_UTC = "2026-08-17T14:05:00Z"
SUPPORTED_EXECUTION_DECISION = "no_trade"

PARENT_CONTRACT_ID = v4.CONTRACT_ID
PARENT_DISPOSITION = (
    "independent_no_approve_preserved_exact_bytes_zero_runtime_zero_canonical_evidence"
)
PARENT_SPECS = {
    "parent_v4_module": (
        "src/forex_system/research/currency_state_response_timing_arms_v4.py",
        "89fba58197e833616f132c9074bb385637dbac9dc9075ba8786c2e7d31845c02",
        44_841,
    ),
    "parent_v4_contract": (
        "config/currency_state_response_timing_arms_v4.json",
        "33fbbb95a5ee29949ef4fd07b0b39586c1cec19e4467aaccde64c8c2803e472b",
        7_238,
    ),
    "parent_v4_manifest": (
        "config/currency_state_response_timing_arms_v4_manifest.json",
        "4e2959c114d9cd3c1b2d7e19c6e8fda178fe63a5c13e49f6fa707465669620cf",
        5_451,
    ),
    "parent_v4_tests": (
        "test_currency_state_response_timing_arms_v4.py",
        "85420028e4a7bd1ba38a2d78d53534c81b5fbe23a4d109e2ba85ede9465c5f44",
        13_946,
    ),
}

_STATE = {
    "enabled": False,
    "registered": False,
    "research_worker_enabled": False,
    "collector_enabled": False,
    "canonical_output_initialized": False,
}
_CONTRACT_STATE = {**_STATE, "independent_review_state": "pending"}
_OUTPUT_SURFACE = {
    "canonical_ledger_path": None,
    "canonical_state_path": None,
    "canonical_report_path": None,
    "worker_entrypoint": None,
    "collector_entrypoint": None,
    "test_fixture_builder": (
        "forex_system.research.currency_state_response_timing_arms_v5."
        "build_test_diagnostic_snapshot"
    ),
}
_GUARD = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_promote": False,
    "can_authorize": False,
    "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
}
_POLICY = {
    "event_response_dependency_is_unresolved": True,
    "dependency_resolution_requires_new_timing_contract": True,
    "diagnostic_capture_may_not_be_reclassified_as_proof": True,
    "unresolved_dependency_must_produce_zero_rows": True,
    "observed_response_is_post_outcome_not_a_forecast": True,
    **_GUARD,
}
_DISPOSITION = {
    "approved_official_fact_currency_state_v5_r3": True,
    "barred_event_response_generations": ["v6_r1", "v6_r2", "v6_r3"],
    "approved_event_response_successor_bound": False,
    "production_dependency_gate": "closed",
    "blocking_reason": "event_response_v6_r4_final_approval_not_bound",
    "resolution_requires_new_timing_contract_id": True,
}
_MANIFEST_TOP_FIELDS = {
    "manifest_schema_version",
    "manifest_id",
    "contract_id",
    "research_generation",
    "parent_contract_id",
    "parent_disposition",
    "review_state",
    "candidate_frozen_utc",
    "independent_review_started_utc",
    "independent_review_completed_utc",
    "runtime_started_utc",
    "state",
    "dependency_disposition",
    "artifacts",
    "output_surface",
    "policy",
}


class ResponseTimingV5Error(ValueError):
    """Frozen contract, manifest, diagnostic, or dependency violation."""


class ResponseTimingV5DependencyError(ResponseTimingV5Error):
    """Raised by every unavailable production/proof entrypoint."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _exact_value(actual: Any, expected: Any, *, path: str) -> None:
    """Require recursively identical values *and* exact builtin scalar types."""

    if type(actual) is not type(expected):
        raise ResponseTimingV5Error(f"{path}_type_mismatch")
    if type(expected) is dict:
        if set(actual) != set(expected):
            raise ResponseTimingV5Error(f"{path}_closed_schema_mismatch")
        for key, value in expected.items():
            _exact_value(actual[key], value, path=f"{path}.{key}")
        return
    if type(expected) is list:
        if len(actual) != len(expected):
            raise ResponseTimingV5Error(f"{path}_length_mismatch")
        for index, value in enumerate(expected):
            _exact_value(actual[index], value, path=f"{path}[{index}]")
        return
    if actual != expected:
        raise ResponseTimingV5Error(f"{path}_value_mismatch")


def _canonical_timestamp_or_null(value: Any, *, expected: str | None, path: str) -> None:
    if expected is None:
        if value is not None:
            raise ResponseTimingV5Error(f"{path}_must_be_null")
        return
    if type(value) is not str or value != expected:
        raise ResponseTimingV5Error(f"{path}_mismatch")
    try:
        parsed = v4._utc(value, path=path)
    except v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV5Error(str(exc)) from exc
    if parsed.tzinfo != dt.timezone.utc:
        raise ResponseTimingV5Error(f"{path}_not_utc")


def _load_contract() -> dict[str, Any]:
    try:
        payload = v4._read_exact(
            "config/currency_state_response_timing_arms_v5.json",
            CONTRACT_SHA256,
            CONTRACT_BYTES,
        )
        contract = v4._strict_json(payload, label="timing_v5_contract")
    except v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV5Error(str(exc)) from exc
    if type(contract) is not dict:
        raise ResponseTimingV5Error("contract_not_exact_dict")
    expected_identity = {
        "schema_version": 5,
        "contract_id": CONTRACT_ID,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "research_generation": RESEARCH_GENERATION,
        "parent_contract_id": PARENT_CONTRACT_ID,
        "parent_disposition": PARENT_DISPOSITION,
    }
    for key, expected in expected_identity.items():
        _exact_value(contract.get(key), expected, path=f"contract.{key}")
    _exact_value(contract.get("state"), _CONTRACT_STATE, path="contract.state")
    _exact_value(
        contract.get("dependency_disposition"),
        _DISPOSITION,
        path="contract.dependency_disposition",
    )
    _exact_value(contract.get("output_surface"), _OUTPUT_SURFACE, path="contract.output_surface")
    _exact_value(contract.get("policy"), _POLICY, path="contract.policy")
    manifest_contract = contract.get("manifest_contract")
    expected_manifest_contract = {
        "manifest_schema_version": 1,
        "manifest_id": MANIFEST_ID,
        "candidate_frozen_utc": FROZEN_UTC,
        "independent_review_started_utc": None,
        "independent_review_completed_utc": None,
        "runtime_started_utc": None,
        "all_scalar_types_are_exact": True,
        "all_top_level_fields_are_value_bound": True,
        "lifecycle_timestamps_are_exact_canonical_utc_or_null": True,
    }
    _exact_value(manifest_contract, expected_manifest_contract, path="contract.manifest_contract")
    return contract


def _verify_parent_v4(contract: Mapping[str, Any]) -> dict[str, Any]:
    parent = contract.get("frozen_parent_v4")
    if type(parent) is not dict or set(parent) != {
        "module", "contract", "manifest", "tests"
    }:
        raise ResponseTimingV5Error("parent_v4_closed_schema_mismatch")
    for short_name, manifest_name in (
        ("module", "parent_v4_module"),
        ("contract", "parent_v4_contract"),
        ("manifest", "parent_v4_manifest"),
        ("tests", "parent_v4_tests"),
    ):
        try:
            spec = v4._artifact_spec(parent[short_name], path=f"parent_v4.{short_name}")
            if spec != PARENT_SPECS[manifest_name]:
                raise ResponseTimingV5Error(f"parent_v4_{short_name}_spec_mismatch")
            v4._read_exact(*spec)
        except v4.ResponseTimingV4Error as exc:
            raise ResponseTimingV5Error(str(exc)) from exc
    status = v4.production_dependency_status()
    if (
        type(status) is not dict
        or status.get("approved") is not False
        or status.get("blocked") is not True
        or status.get("row_count") != 0
        or status.get("proof_row_count") != 0
        or status.get("canonical_universe", {}).get("currency_count") != 21
        or status.get("canonical_universe", {}).get("instrument_count") != 68
        or len(status.get("barred_predecessors") or []) != 3
    ):
        raise ResponseTimingV5Error("parent_v4_closure_mismatch")
    return status


def production_dependency_status() -> dict[str, Any]:
    try:
        contract = _load_contract()
        parent = _verify_parent_v4(contract)
    except ResponseTimingV5Error as exc:
        return {
            "contract_id": CONTRACT_ID,
            "approved": False,
            "blocked": True,
            "blocking_reasons": [f"immutable_dependency_verification_failed:{exc}"],
            "row_count": 0,
            "proof_row_count": 0,
            **_GUARD,
        }
    return {
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "approved": False,
        "blocked": True,
        "blocking_reasons": [_DISPOSITION["blocking_reason"]],
        "canonical_universe": dict(parent["canonical_universe"]),
        "barred_predecessors": copy.deepcopy(parent["barred_predecessors"]),
        "required_event_response": copy.deepcopy(parent["required_event_response"]),
        "row_count": 0,
        "proof_row_count": 0,
        **_GUARD,
    }


def require_production_dependencies() -> None:
    status = production_dependency_status()
    raise ResponseTimingV5DependencyError(
        ",".join(status.get("blocking_reasons") or ["dependency_not_approved"])
    )


def build_replay_diagnostic(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def build_prospective_proof(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def _v5_snapshot_id(snapshot: Mapping[str, Any]) -> str:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    return "currency_state_response_timing_v5_test_" + _sha256_json(material)


def build_test_diagnostic_snapshot(
    *, event_response_snapshot: Mapping[str, Any], dependency_attestation: Mapping[str, Any]
) -> dict[str, Any]:
    contract = _load_contract()
    _verify_parent_v4(contract)
    try:
        result = v4.build_test_diagnostic_snapshot(
            event_response_snapshot=event_response_snapshot,
            dependency_attestation=dependency_attestation,
        )
    except v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV5Error(str(exc)) from exc
    result = copy.deepcopy(result)
    result.update(
        {
            "schema_version": 5,
            "snapshot_schema": SNAPSHOT_SCHEMA,
            "snapshot_id": None,
            "contract_id": CONTRACT_ID,
            "contract_sha256": CONTRACT_SHA256,
            "research_generation": RESEARCH_GENERATION,
        }
    )
    result["snapshot_id"] = _v5_snapshot_id(result)
    validate_test_diagnostic_snapshot(result)
    return result


def validate_test_diagnostic_snapshot(snapshot: Mapping[str, Any]) -> None:
    if type(snapshot) is not dict or set(snapshot) != v4._RESULT_FIELDS:
        raise ResponseTimingV5Error("snapshot_closed_schema_mismatch")
    expected_types = {
        "schema_version": int,
        "snapshot_schema": str,
        "snapshot_id": str,
        "contract_id": str,
        "contract_sha256": str,
        "research_generation": str,
        "mode": str,
        "partition": str,
        "test_only": bool,
        "canonical_output": bool,
        "source_snapshot_id": str,
        "source_contract_id": str,
        "source_cohort_id": str,
        "source_binding_sha256": str,
        "dependency_attestation": dict,
        "capture_cutoff_utc": str,
        "response_count": int,
        "timing_arm_count": int,
        "proof_row_count": int,
        "responses": list,
        "timing_arms": list,
        "limitations": list,
        "research_only": bool,
        "execution_eligible": bool,
        "can_place_orders": bool,
        "can_promote": bool,
        "can_authorize": bool,
        "supported_execution_decision": str,
    }
    for key, expected_type in expected_types.items():
        if type(snapshot[key]) is not expected_type:
            raise ResponseTimingV5Error(f"snapshot_{key}_type_mismatch")
    expected_identity = {
        "schema_version": 5,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "research_generation": RESEARCH_GENERATION,
        "test_only": True,
        "canonical_output": False,
        "proof_row_count": 0,
        **_GUARD,
    }
    for key, expected in expected_identity.items():
        _exact_value(snapshot[key], expected, path=f"snapshot.{key}")
    if snapshot["snapshot_id"] != _v5_snapshot_id(snapshot):
        raise ResponseTimingV5Error("snapshot_id_mismatch")
    _load_contract()
    parent = copy.deepcopy(snapshot)
    parent.update(
        {
            "schema_version": 4,
            "snapshot_schema": v4.SNAPSHOT_SCHEMA,
            "snapshot_id": None,
            "contract_id": v4.CONTRACT_ID,
            "contract_sha256": v4.CONTRACT_SHA256,
            "research_generation": v4.RESEARCH_GENERATION,
        }
    )
    parent["snapshot_id"] = (
        "currency_state_response_timing_v4_test_"
        + v4._sha256_json({key: value for key, value in parent.items() if key != "snapshot_id"})
    )
    try:
        v4.validate_test_diagnostic_snapshot(parent)
    except v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV5Error(str(exc)) from exc


def verify_candidate_manifest() -> dict[str, Any]:
    contract = _load_contract()
    _verify_parent_v4(contract)
    try:
        manifest = v4._strict_json(
            v4._stable_read("config/currency_state_response_timing_arms_v5_manifest.json"),
            label="timing_v5_manifest",
        )
    except v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV5Error(str(exc)) from exc
    if type(manifest) is not dict or set(manifest) != _MANIFEST_TOP_FIELDS:
        raise ResponseTimingV5Error("manifest_closed_schema_mismatch")
    expected_top = {
        "manifest_schema_version": 1,
        "manifest_id": MANIFEST_ID,
        "contract_id": CONTRACT_ID,
        "research_generation": RESEARCH_GENERATION,
        "parent_contract_id": PARENT_CONTRACT_ID,
        "parent_disposition": PARENT_DISPOSITION,
        "review_state": "pending_independent_review",
    }
    for key, expected in expected_top.items():
        _exact_value(manifest[key], expected, path=f"manifest.{key}")
    _canonical_timestamp_or_null(
        manifest["candidate_frozen_utc"], expected=FROZEN_UTC, path="manifest.candidate_frozen_utc"
    )
    for field in (
        "independent_review_started_utc",
        "independent_review_completed_utc",
        "runtime_started_utc",
    ):
        _canonical_timestamp_or_null(manifest[field], expected=None, path=f"manifest.{field}")
    _exact_value(manifest["state"], _STATE, path="manifest.state")
    _exact_value(
        manifest["dependency_disposition"],
        _DISPOSITION,
        path="manifest.dependency_disposition",
    )
    _exact_value(manifest["output_surface"], _OUTPUT_SURFACE, path="manifest.output_surface")
    _exact_value(manifest["policy"], _POLICY, path="manifest.policy")
    expected_paths = {
        "runtime_core": "src/forex_system/research/currency_state_response_timing_arms_v5.py",
        "contract": "config/currency_state_response_timing_arms_v5.json",
        "focused_tests": "test_currency_state_response_timing_arms_v5.py",
        **{name: spec[0] for name, spec in PARENT_SPECS.items()},
    }
    artifacts = manifest["artifacts"]
    if type(artifacts) is not dict or set(artifacts) != set(expected_paths):
        raise ResponseTimingV5Error("manifest_artifact_set_mismatch")
    for name, path in expected_paths.items():
        try:
            spec = v4._artifact_spec(artifacts[name], path=f"manifest.{name}")
            if spec[0] != path:
                raise ResponseTimingV5Error(f"manifest_artifact_path_mismatch:{name}")
            v4._read_exact(*spec)
        except v4.ResponseTimingV4Error as exc:
            raise ResponseTimingV5Error(str(exc)) from exc
    return {
        "manifest_id": MANIFEST_ID,
        "review_state": manifest["review_state"],
        "artifact_count": len(expected_paths),
        "dependency_gate": "closed",
        "candidate_closure_verified": True,
    }


__all__ = [
    "CONTRACT_ID",
    "CONTRACT_SHA256",
    "ResponseTimingV5DependencyError",
    "ResponseTimingV5Error",
    "SNAPSHOT_SCHEMA",
    "build_prospective_proof",
    "build_replay_diagnostic",
    "build_test_diagnostic_snapshot",
    "production_dependency_status",
    "require_production_dependencies",
    "validate_test_diagnostic_snapshot",
    "verify_candidate_manifest",
]
