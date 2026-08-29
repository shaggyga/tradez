"""Inert Timing V7 with strict transitive dependency success.

V7 preserves V6 exactly and closes its deep-lineage fail-open: the complete
successful V5 dependency status must match an exact trusted schema.  Any deep
artifact failure raises; no fallback or synthetic universe is returned.
Self closure remains external, unbound, and incapable of activation here.
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Mapping

from . import currency_state_response_timing_arms_v6 as v6


CONTRACT_ID = "currency_state_response_timing_arms_v7_20260817_r1"
SNAPSHOT_SCHEMA = "currency_state_response_timing_diagnostic_snapshot_v7"
RESEARCH_GENERATION = "r3_bound_response_timing_v7_r1"
CONTRACT_SHA256 = "d074c2bc6505d2fc98ad75ead17c35606102f64c14077011a1e587866278bddb"
CONTRACT_BYTES = 1_979
MANIFEST_ID = "currency_state_response_timing_arms_v7_manifest_20260817_r1"
FROZEN_UTC = "2026-08-17T14:29:00Z"
PARENT_CONTRACT_ID = v6.CONTRACT_ID
PARENT_DISPOSITION = "preserved_inert_no_approve_after_deep_lineage_fail_open_found"

UNIVERSE = {
    "currency_count": 21,
    "instrument_count": 68,
    "instrument_universe_sha256": "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142",
    "canonical_pip_mapping_sha256": "46532e9598c7fab24c57935bb4bb4d747901d68acc9f3d50a28bd1957ce19f2c",
}
GUARD = {
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_promote": False,
    "can_authorize": False,
    "supported_execution_decision": "no_trade",
}
STATE = {
    "enabled": False,
    "registered": False,
    "research_worker_enabled": False,
    "collector_enabled": False,
    "canonical_output_initialized": False,
}
DISPOSITION = {
    "barred_event_response_generations": ["v6_r1", "v6_r2", "v6_r3"],
    "approved_event_response_successor_bound": False,
    "production_dependency_gate": "closed",
    "blocking_reason": "event_response_v6_r4_final_approval_not_bound",
    "resolution_requires_new_timing_contract_id": True,
}
OUTPUT = {
    "canonical_ledger_path": None,
    "canonical_state_path": None,
    "canonical_report_path": None,
    "worker_entrypoint": None,
    "collector_entrypoint": None,
    "test_fixture_builder": "forex_system.research.currency_state_response_timing_arms_v7.build_test_diagnostic_snapshot",
}
DEPENDENCY_POLICY = {
    "complete_exact_v5_success_status_required": True,
    "immutable_dependency_failure_must_raise": True,
    "synthetic_universe_on_failure_forbidden": True,
    "self_manifest_cannot_self_certify": True,
    "external_review_certificate_required": True,
}
EXPECTED_V5_STATUS = {
    "contract_id": "currency_state_response_timing_arms_v5_20260817_r1",
    "contract_sha256": "08674d293897332d438872f8276a46e8de3525f07e35ddb8796996b408ee8cd0",
    "approved": False,
    "blocked": True,
    "blocking_reasons": ["event_response_v6_r4_final_approval_not_bound"],
    "canonical_universe": UNIVERSE,
    "barred_predecessors": [
        {
            "contract_id": "prospective_event_response_capture_v6_20260817_r1",
            "cohort_id": "prospective_official_event_response_v6_20260817_r1",
            "may_supply_rows": False,
        },
        {
            "contract_id": "prospective_event_response_capture_v6_20260817_r2",
            "cohort_id": "prospective_official_event_response_v6_20260817_r2",
            "may_supply_rows": False,
        },
        {
            "contract_id": "prospective_event_response_capture_v6_20260817_r3",
            "cohort_id": "prospective_official_event_response_v6_20260817_r3",
            "may_supply_rows": False,
        },
    ],
    "required_event_response": {
        "contract_id": "prospective_event_response_capture_v6_20260817_r4",
        "cohort_id": "prospective_official_event_response_v6_20260817_r4",
        "approval_state": "unresolved_fail_closed",
    },
    "row_count": 0,
    "proof_row_count": 0,
    **GUARD,
}
TRUSTED_ARTIFACTS = {
    "contract": (
        "config/currency_state_response_timing_arms_v7.json",
        CONTRACT_SHA256,
        CONTRACT_BYTES,
    ),
    "parent_v6_module": (
        "src/forex_system/research/currency_state_response_timing_arms_v6.py",
        "4de3e9f66fa9345723c2af4dc726cfb30cbc2d4afc1afceee910e9bf20b1fe96",
        13_240,
    ),
    "parent_v6_contract": (
        "config/currency_state_response_timing_arms_v6.json",
        "525039ed9014943f7950cb465cebeb103332a7f317a6c47472362368def773b4",
        2_987,
    ),
    "parent_v6_manifest": (
        "config/currency_state_response_timing_arms_v6_manifest.json",
        "82d12f151546fed7a857168b7ef5265acec1e19e62e936a2a803699703080846",
        3_960,
    ),
    "parent_v6_tests": (
        "test_currency_state_response_timing_arms_v6.py",
        "6aee761616d61a6c956a899746d556d49bdad6f1c55efae452afbca4d385aedd",
        8_216,
    ),
}
TOP_FIELDS = {
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
    "dependency_policy",
    "dependency_disposition",
    "trusted_artifacts",
    "self_artifacts",
    "external_review_certificate",
    "output_surface",
    "policy",
}


class ResponseTimingV7Error(ValueError):
    pass


class ResponseTimingV7DependencyError(ResponseTimingV7Error):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _exact(actual: Any, expected: Any, *, path: str) -> None:
    if type(actual) is not type(expected):
        raise ResponseTimingV7Error(f"{path}_type_mismatch")
    if type(expected) is dict:
        if set(actual) != set(expected):
            raise ResponseTimingV7Error(f"{path}_closed_schema_mismatch")
        for key, value in expected.items():
            _exact(actual[key], value, path=f"{path}.{key}")
    elif type(expected) is list:
        if len(actual) != len(expected):
            raise ResponseTimingV7Error(f"{path}_length_mismatch")
        for index, value in enumerate(expected):
            _exact(actual[index], value, path=f"{path}[{index}]")
    elif actual != expected:
        raise ResponseTimingV7Error(f"{path}_value_mismatch")


def _load_contract() -> dict[str, Any]:
    try:
        value = v6.v5.v4._strict_json(
            v6.v5.v4._read_exact(*TRUSTED_ARTIFACTS["contract"]),
            label="timing_v7_contract",
        )
    except v6.v5.v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV7Error(str(exc)) from exc
    expected = {
        "schema_version": 7,
        "contract_id": CONTRACT_ID,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "research_generation": RESEARCH_GENERATION,
        "parent_contract_id": PARENT_CONTRACT_ID,
        "parent_disposition": PARENT_DISPOSITION,
        "candidate_frozen_utc": FROZEN_UTC,
        "state": {**STATE, "independent_review_state": "pending"},
        "dependency_policy": DEPENDENCY_POLICY,
        "canonical_universe": UNIVERSE,
        "dependency_disposition": DISPOSITION,
        "output_surface": OUTPUT,
        "policy": GUARD,
    }
    _exact(value, expected, path="contract")
    return value


def _verify_nonself() -> dict[str, Any]:
    _load_contract()
    for name, spec in TRUSTED_ARTIFACTS.items():
        try:
            v6.v5.v4._read_exact(*spec)
        except v6.v5.v4.ResponseTimingV4Error as exc:
            raise ResponseTimingV7Error(
                f"trusted_artifact_invalid:{name}:{exc}"
            ) from exc
    # V6 verifies the exact V5 module/contract/manifest/tests before returning
    # the V5 status.  V7 then closes V6's former fail-open by requiring that
    # returned status to be the complete successful dependency result.
    status = v6._verify_nonself()
    _exact(status, EXPECTED_V5_STATUS, path="parent_v5_success_status")
    return status


def production_dependency_status() -> dict[str, Any]:
    parent = _verify_nonself()
    return {
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "approved": False,
        "blocked": True,
        "blocking_reasons": [DISPOSITION["blocking_reason"]],
        "canonical_universe": copy.deepcopy(parent["canonical_universe"]),
        "barred_predecessors": copy.deepcopy(parent["barred_predecessors"]),
        "required_event_response": copy.deepcopy(parent["required_event_response"]),
        "row_count": 0,
        "proof_row_count": 0,
        "self_closure_verified": False,
        **GUARD,
    }


def require_production_dependencies() -> None:
    _verify_nonself()
    raise ResponseTimingV7DependencyError(DISPOSITION["blocking_reason"])


def build_replay_diagnostic(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def build_prospective_proof(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def _snapshot_id(value: Mapping[str, Any]) -> str:
    return "currency_state_response_timing_v7_test_" + _sha256_json(
        {key: item for key, item in value.items() if key != "snapshot_id"}
    )


def build_test_diagnostic_snapshot(
    *,
    event_response_snapshot: Mapping[str, Any],
    dependency_attestation: Mapping[str, Any],
) -> dict[str, Any]:
    _verify_nonself()
    try:
        result = v6.build_test_diagnostic_snapshot(
            event_response_snapshot=event_response_snapshot,
            dependency_attestation=dependency_attestation,
        )
    except v6.ResponseTimingV6Error as exc:
        raise ResponseTimingV7Error(str(exc)) from exc
    result = copy.deepcopy(result)
    result.update(
        {
            "schema_version": 7,
            "snapshot_schema": SNAPSHOT_SCHEMA,
            "snapshot_id": None,
            "contract_id": CONTRACT_ID,
            "contract_sha256": CONTRACT_SHA256,
            "research_generation": RESEARCH_GENERATION,
        }
    )
    result["snapshot_id"] = _snapshot_id(result)
    validate_test_diagnostic_snapshot(result)
    return result


def validate_test_diagnostic_snapshot(snapshot: Mapping[str, Any]) -> None:
    _verify_nonself()
    if type(snapshot) is not dict or set(snapshot) != v6.v5.v4._RESULT_FIELDS:
        raise ResponseTimingV7Error("snapshot_closed_schema_mismatch")
    for key, expected in {
        "schema_version": 7,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "research_generation": RESEARCH_GENERATION,
        "test_only": True,
        "canonical_output": False,
        "proof_row_count": 0,
        **GUARD,
    }.items():
        _exact(snapshot.get(key), expected, path=f"snapshot.{key}")
    if type(snapshot.get("snapshot_id")) is not str or snapshot["snapshot_id"] != _snapshot_id(snapshot):
        raise ResponseTimingV7Error("snapshot_id_mismatch")
    parent = copy.deepcopy(snapshot)
    parent.update(
        {
            "schema_version": 6,
            "snapshot_schema": v6.SNAPSHOT_SCHEMA,
            "snapshot_id": None,
            "contract_id": v6.CONTRACT_ID,
            "contract_sha256": v6.CONTRACT_SHA256,
            "research_generation": v6.RESEARCH_GENERATION,
        }
    )
    parent["snapshot_id"] = v6._snapshot_id(parent)
    try:
        v6.validate_test_diagnostic_snapshot(parent)
    except v6.ResponseTimingV6Error as exc:
        raise ResponseTimingV7Error(str(exc)) from exc


def verify_candidate_manifest() -> dict[str, Any]:
    _verify_nonself()
    try:
        manifest = v6.v5.v4._strict_json(
            v6.v5.v4._stable_read(
                "config/currency_state_response_timing_arms_v7_manifest.json"
            ),
            label="timing_v7_manifest",
        )
    except v6.v5.v4.ResponseTimingV4Error as exc:
        raise ResponseTimingV7Error(str(exc)) from exc
    if type(manifest) is not dict or set(manifest) != TOP_FIELDS:
        raise ResponseTimingV7Error("manifest_closed_schema_mismatch")
    fixed = {
        "manifest_schema_version": 1,
        "manifest_id": MANIFEST_ID,
        "contract_id": CONTRACT_ID,
        "research_generation": RESEARCH_GENERATION,
        "parent_contract_id": PARENT_CONTRACT_ID,
        "parent_disposition": PARENT_DISPOSITION,
        "review_state": "pending_independent_review",
        "candidate_frozen_utc": FROZEN_UTC,
        "independent_review_started_utc": None,
        "independent_review_completed_utc": None,
        "runtime_started_utc": None,
        "state": STATE,
        "dependency_policy": DEPENDENCY_POLICY,
        "dependency_disposition": DISPOSITION,
        "external_review_certificate": None,
        "output_surface": OUTPUT,
        "policy": GUARD,
    }
    for key, expected in fixed.items():
        _exact(manifest.get(key), expected, path=f"manifest.{key}")
    artifacts = manifest["trusted_artifacts"]
    if type(artifacts) is not dict or set(artifacts) != set(TRUSTED_ARTIFACTS):
        raise ResponseTimingV7Error("trusted_artifact_set_mismatch")
    for name, expected in TRUSTED_ARTIFACTS.items():
        try:
            actual = v6.v5.v4._artifact_spec(
                artifacts[name], path=f"manifest.trusted.{name}"
            )
        except v6.v5.v4.ResponseTimingV4Error as exc:
            raise ResponseTimingV7Error(str(exc)) from exc
        if actual != expected:
            raise ResponseTimingV7Error(f"trusted_artifact_spec_mismatch:{name}")
    self_paths = {
        "runtime_core": "src/forex_system/research/currency_state_response_timing_arms_v7.py",
        "focused_tests": "test_currency_state_response_timing_arms_v7.py",
        "manifest": "config/currency_state_response_timing_arms_v7_manifest.json",
    }
    self_artifacts = manifest["self_artifacts"]
    if type(self_artifacts) is not dict or set(self_artifacts) != set(self_paths):
        raise ResponseTimingV7Error("self_artifact_set_mismatch")
    for name, relative_path in self_paths.items():
        spec = self_artifacts[name]
        if (
            type(spec) is not dict
            or set(spec) != {"relative_path", "sha256", "byte_length", "trust_state"}
            or spec["relative_path"] != relative_path
            or spec["trust_state"] != "untrusted_until_external_frozen_review"
            or not v6.v5.v4._is_sha256(spec["sha256"])
            or type(spec["byte_length"]) is not int
            or spec["byte_length"] <= 0
        ):
            raise ResponseTimingV7Error(f"self_artifact_schema_mismatch:{name}")
    return {
        "manifest_id": MANIFEST_ID,
        "nonself_closure_verified": True,
        "self_closure_verified": False,
        "candidate_closure_verified": False,
        "activation_allowed": False,
        "external_review_certificate_required": True,
    }


__all__ = [
    "CONTRACT_ID",
    "CONTRACT_SHA256",
    "MANIFEST_ID",
    "ResponseTimingV7DependencyError",
    "ResponseTimingV7Error",
    "build_prospective_proof",
    "build_replay_diagnostic",
    "build_test_diagnostic_snapshot",
    "production_dependency_status",
    "validate_test_diagnostic_snapshot",
    "verify_candidate_manifest",
]
