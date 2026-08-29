"""Disabled, closed-schema response/timing research successor V3.

V3 repairs the rejected V2 diagnostic validator and dependency closure while
preserving every V2 byte.  It owns its hashing implementation, recursively
verifies the approved R3 artifact certificate, binds the exact canonical
21-currency/68-instrument universe, and reconstructs every diagnostic row,
arm and snapshot identity during validation.

The production dependency is deliberately unresolved.  The barred V6 R1
event-response candidate can supply no rows, and an approved V6 R2 successor
would require a *new* timing contract.  This module therefore exposes only a
noncanonical synthetic test-fixture builder.  It has no write, worker, broker,
lifecycle, promotion, authorization, proof, or execution surface.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping


UTC = dt.timezone.utc
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_ID = "currency_state_response_timing_arms_v3_20260817_r1"
SNAPSHOT_SCHEMA = "currency_state_response_timing_diagnostic_snapshot_v3"
RESEARCH_GENERATION = "r3_bound_response_timing_v3_r1"
SUPPORTED_EXECUTION_DECISION = "no_trade"
CONTRACT_SHA256 = "f649d6b610573714d0e5abdd7869d32b341e81ad7adf96c4714384de699f27c7"
CONTRACT_BYTES = 6_377

R3_CERTIFICATE_ID = "official_fact_currency_state_v5_r3_independent_review_20260817"
R3_CERTIFICATE_SHA256 = "0a61948a9fc2f2a9a06d30557b838d7fb389958c9ef2b1845bc8b06e70c7b299"
R3_CERTIFICATE_BYTES = 6_003
R3_ADAPTER_CONTRACT_ID = "official_fact_adapter_v5_20260817_r3"
R3_ADAPTER_MANIFEST_ID = "official_fact_adapter_v5_manifest_20260817_r3"
R3_CONTEXT_CONTRACT_ID = "currency_state_official_context_v5_20260817_r3"
R3_CONTEXT_MANIFEST_ID = "currency_state_official_context_v5_manifest_20260817_r3"
R3_BASIS_CONTRACT_ID = "official_fact_basis_eligibility_v5_20260817_r3"
R3_CLOCK_CONTRACT_ID = "immutable_point_in_time_event_clock_v2_20260817"
R3_REVIEWED_ARTIFACT_COUNT = 23

UNIVERSE_CONTRACT_ID = "currency_state_engine_v2_20260816"
UNIVERSE_CONTRACT_SHA256 = "20138fe4328ee88b47b8edce2b3bd6081191cda35dd4124a9a457ea9b2efa400"
UNIVERSE_CONTRACT_BYTES = 3_272
INSTRUMENT_UNIVERSE_SHA256 = "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"

BARRED_V6_R1_CONTRACT_ID = "prospective_event_response_capture_v6_20260817_r1"
BARRED_V6_R1_COHORT_ID = "prospective_official_event_response_v6_20260817_r1"
REQUIRED_V6_R2_CONTRACT_ID = "prospective_event_response_capture_v6_20260817_r2"
REQUIRED_V6_R2_COHORT_ID = "prospective_official_event_response_v6_20260817_r2"

DIAGNOSTIC_MODES = {
    "replay_diagnostic_test_fixture": "diagnostic_replay",
    "prospective_capture_test_fixture": "diagnostic_prospective_capture",
}
HORIZONS_SEC = (60, 300, 900, 1800, 3600)
ARM_IDS = ("observed_response", "cost_clearance", "response_latency", "no_trade")
LIMITATIONS = [
    "test_fixture_only_noncanonical",
    "replay_and_prospective_capture_are_separate_diagnostic_partitions",
    "all_response_direction_is_post_outcome_and_not_a_forecast",
    "diagnostics_are_not_prospective_proof",
    "production_gate_remains_closed_and_any_approved_v6_r2_requires_a_new_timing_contract",
]


class ResponseTimingV3Error(ValueError):
    """Closed-schema, chronology, identity, or dependency failure."""


class ResponseTimingV3DependencyError(ResponseTimingV3Error):
    """Raised by every unavailable canonical/proof entrypoint."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _strict_json(payload: bytes, *, label: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ResponseTimingV3Error(f"{label}_duplicate_json_key:{key}")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise ResponseTimingV3Error(f"{label}_nonfinite_json:{value}")

    try:
        return json.loads(
            payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResponseTimingV3Error(f"{label}_invalid_json") from exc


def _safe_path(relative_path: str) -> Path:
    if (
        type(relative_path) is not str
        or not relative_path
        or "\\" in relative_path
        or Path(relative_path).is_absolute()
        or any(part in {"", ".", ".."} for part in relative_path.split("/"))
    ):
        raise ResponseTimingV3Error("artifact_path_invalid")
    try:
        root = PROJECT_ROOT.resolve(strict=True)
        candidate = PROJECT_ROOT.joinpath(*relative_path.split("/"))
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
        cursor = root
        for part in relative_path.split("/"):
            cursor = cursor / part
            if cursor.is_symlink():
                raise ResponseTimingV3Error(f"artifact_symlink_forbidden:{relative_path}")
    except ResponseTimingV3Error:
        raise
    except (OSError, ValueError) as exc:
        raise ResponseTimingV3Error(
            f"artifact_path_escape_or_missing:{relative_path}"
        ) from exc
    return candidate


def _stable_read(relative_path: str) -> bytes:
    path = _safe_path(relative_path)
    try:
        before = path.stat()
        payload = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise ResponseTimingV3Error(f"artifact_read_failed:{relative_path}") from exc
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise ResponseTimingV3Error(f"artifact_identity_changed:{relative_path}")
    if after.st_nlink != 1:
        raise ResponseTimingV3Error(f"artifact_hardlink_forbidden:{relative_path}")
    return payload


def _read_exact(relative_path: str, sha256: str, byte_length: int) -> bytes:
    payload = _stable_read(relative_path)
    if len(payload) != byte_length or _sha256_bytes(payload) != sha256:
        raise ResponseTimingV3Error(f"artifact_content_mismatch:{relative_path}")
    return payload


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _utc(value: Any, *, path: str) -> dt.datetime:
    if type(value) is not str or not value:
        raise ResponseTimingV3Error(f"{path}_must_be_strict_utc_string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ResponseTimingV3Error(f"{path}_invalid_utc") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ResponseTimingV3Error(f"{path}_must_have_zero_utc_offset")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _number(value: Any, *, path: str, positive: bool = False) -> float:
    if type(value) not in {int, float}:
        raise ResponseTimingV3Error(f"{path}_must_be_number")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0):
        raise ResponseTimingV3Error(f"{path}_invalid_number")
    return result


def _guard() -> dict[str, Any]:
    return {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }


def _artifact_spec(value: Any, *, path: str) -> tuple[str, str, int]:
    if type(value) is not dict or set(value) != {"relative_path", "sha256", "byte_length"}:
        raise ResponseTimingV3Error(f"{path}_closed_schema_mismatch")
    relative = value["relative_path"]
    sha256 = value["sha256"]
    byte_length = value["byte_length"]
    if type(relative) is not str or not relative or not _is_sha256(sha256):
        raise ResponseTimingV3Error(f"{path}_identity_invalid")
    if type(byte_length) is not int or byte_length <= 0:
        raise ResponseTimingV3Error(f"{path}_byte_length_invalid")
    return relative, sha256, byte_length


def _load_contract() -> Mapping[str, Any]:
    contract = _strict_json(
        _read_exact(
            "config/currency_state_response_timing_arms_v3.json",
            CONTRACT_SHA256,
            CONTRACT_BYTES,
        ),
        label="timing_v3_contract",
    )
    if contract.get("contract_id") != CONTRACT_ID or contract.get("schema_version") != 3:
        raise ResponseTimingV3Error("timing_contract_identity_mismatch")
    if contract.get("snapshot_schema") != SNAPSHOT_SCHEMA:
        raise ResponseTimingV3Error("timing_snapshot_schema_mismatch")
    if contract.get("state") != {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "canonical_output_initialized": False,
        "independent_review_state": "pending",
    }:
        raise ResponseTimingV3Error("timing_contract_not_inert")
    policy = contract.get("policy")
    if type(policy) is not dict:
        raise ResponseTimingV3Error("timing_policy_missing")
    for field, expected in _guard().items():
        if policy.get(field) != expected or type(policy.get(field)) is not type(expected):
            raise ResponseTimingV3Error(f"unsafe_timing_policy:{field}")
    output = contract.get("output_surface")
    if type(output) is not dict or any(
        output.get(field) is not None
        for field in (
            "canonical_ledger_path",
            "canonical_state_path",
            "canonical_report_path",
            "worker_entrypoint",
            "collector_entrypoint",
        )
    ):
        raise ResponseTimingV3Error("timing_contract_runtime_or_canonical_surface")
    return contract


def _reviewed_artifact_map(certificate: Mapping[str, Any]) -> dict[str, tuple[str, str, int]]:
    rows = certificate.get("reviewed_artifacts")
    if type(rows) is not list or len(rows) != R3_REVIEWED_ARTIFACT_COUNT:
        raise ResponseTimingV3Error("r3_reviewed_artifact_count_mismatch")
    result: dict[str, tuple[str, str, int]] = {}
    for index, value in enumerate(rows):
        spec = _artifact_spec(value, path=f"r3.reviewed_artifacts[{index}]")
        if spec[0] in result:
            raise ResponseTimingV3Error(f"r3_duplicate_reviewed_path:{spec[0]}")
        _read_exact(*spec)
        result[spec[0]] = spec
    return result


def _verify_nested_manifest(
    manifest: Mapping[str, Any], *, reviewed: Mapping[str, tuple[str, str, int]], label: str
) -> None:
    artifacts = manifest.get("artifacts")
    if type(artifacts) is not dict or not artifacts:
        raise ResponseTimingV3Error(f"{label}_artifacts_missing")
    for name, value in artifacts.items():
        spec = _artifact_spec(value, path=f"{label}.{name}")
        if reviewed.get(spec[0]) != spec:
            raise ResponseTimingV3Error(f"{label}_artifact_not_in_review_certificate:{name}")


def _verify_r3(contract: Mapping[str, Any]) -> dict[str, Any]:
    dependency = contract.get("approved_r3_dependency")
    if type(dependency) is not dict:
        raise ResponseTimingV3Error("r3_dependency_missing")
    cert_spec = _artifact_spec(dependency.get("review_certificate"), path="r3.certificate")
    adapter_spec = _artifact_spec(dependency.get("official_fact_manifest"), path="r3.adapter")
    context_spec = _artifact_spec(dependency.get("currency_state_manifest"), path="r3.context")
    if cert_spec != (
        "config/official_fact_currency_state_v5_r3_review_certificate.json",
        R3_CERTIFICATE_SHA256,
        R3_CERTIFICATE_BYTES,
    ):
        raise ResponseTimingV3Error("r3_certificate_spec_mismatch")
    certificate = _strict_json(_read_exact(*cert_spec), label="r3_certificate")
    if certificate.get("review_certificate_id") != R3_CERTIFICATE_ID:
        raise ResponseTimingV3Error("r3_certificate_id_mismatch")
    if certificate.get("review_disposition") != "independent_approve_disabled_unregistered_candidate":
        raise ResponseTimingV3Error("r3_certificate_not_approved")
    results = certificate.get("review_results")
    if type(results) is not dict or any(
        type(results.get(key)) is not int or results.get(key) != 0
        for key in ("p0_findings", "p1_findings", "p2_findings")
    ):
        raise ResponseTimingV3Error("r3_certificate_has_material_findings")
    reviewed_contracts = certificate.get("reviewed_contracts")
    if type(reviewed_contracts) is not dict or reviewed_contracts != {
        "official_fact_adapter_contract_id": R3_ADAPTER_CONTRACT_ID,
        "currency_state_official_context_contract_id": R3_CONTEXT_CONTRACT_ID,
        "fact_basis_eligibility_contract_id": R3_BASIS_CONTRACT_ID,
        "immutable_event_clock_contract_id": R3_CLOCK_CONTRACT_ID,
    }:
        raise ResponseTimingV3Error("r3_certificate_contract_set_mismatch")
    reviewed = _reviewed_artifact_map(certificate)
    for spec, label in ((adapter_spec, "adapter"), (context_spec, "context")):
        if reviewed.get(spec[0]) != spec:
            raise ResponseTimingV3Error(f"r3_{label}_manifest_not_certificate_bound")
    adapter_manifest = _strict_json(_read_exact(*adapter_spec), label="r3_adapter_manifest")
    context_manifest = _strict_json(_read_exact(*context_spec), label="r3_context_manifest")
    if (
        adapter_manifest.get("manifest_id") != R3_ADAPTER_MANIFEST_ID
        or adapter_manifest.get("adapter_contract_id") != R3_ADAPTER_CONTRACT_ID
    ):
        raise ResponseTimingV3Error("r3_adapter_manifest_identity_mismatch")
    if (
        context_manifest.get("manifest_id") != R3_CONTEXT_MANIFEST_ID
        or context_manifest.get("context_contract_id") != R3_CONTEXT_CONTRACT_ID
        or context_manifest.get("official_fact_adapter_contract_id") != R3_ADAPTER_CONTRACT_ID
    ):
        raise ResponseTimingV3Error("r3_context_manifest_identity_mismatch")
    _verify_nested_manifest(adapter_manifest, reviewed=reviewed, label="r3_adapter_manifest")
    _verify_nested_manifest(context_manifest, reviewed=reviewed, label="r3_context_manifest")
    return {
        "review_certificate_id": R3_CERTIFICATE_ID,
        "review_certificate_sha256": R3_CERTIFICATE_SHA256,
        "reviewed_artifact_count": len(reviewed),
        "transitive_closure_verified": True,
    }


def _load_universe(contract: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    spec_value = contract.get("canonical_universe")
    if type(spec_value) is not dict:
        raise ResponseTimingV3Error("canonical_universe_missing")
    spec = _artifact_spec(spec_value.get("contract"), path="canonical_universe.contract")
    expected = (
        "config/currency_state_engine_v2.json",
        UNIVERSE_CONTRACT_SHA256,
        UNIVERSE_CONTRACT_BYTES,
    )
    if spec != expected:
        raise ResponseTimingV3Error("canonical_universe_spec_mismatch")
    payload = _strict_json(_read_exact(*spec), label="canonical_universe")
    if payload.get("contract_id") != UNIVERSE_CONTRACT_ID:
        raise ResponseTimingV3Error("canonical_universe_contract_id_mismatch")
    currencies = payload.get("currencies")
    instruments = payload.get("instruments")
    if (
        type(currencies) is not list
        or type(instruments) is not list
        or len(currencies) != 21
        or len(set(currencies)) != 21
        or len(instruments) != 68
        or len(set(instruments)) != 68
        or any(type(value) is not str for value in currencies + instruments)
    ):
        raise ResponseTimingV3Error("canonical_universe_shape_mismatch")
    if _sha256_json(instruments) != INSTRUMENT_UNIVERSE_SHA256:
        raise ResponseTimingV3Error("canonical_instrument_universe_hash_mismatch")
    if spec_value.get("currency_count") != 21 or spec_value.get("instrument_count") != 68:
        raise ResponseTimingV3Error("canonical_universe_contract_count_mismatch")
    if spec_value.get("instrument_universe_sha256") != INSTRUMENT_UNIVERSE_SHA256:
        raise ResponseTimingV3Error("canonical_universe_contract_hash_mismatch")
    return tuple(currencies), tuple(instruments)


def _verify_barred_r1(contract: Mapping[str, Any]) -> dict[str, Any]:
    barred = contract.get("barred_event_response_predecessor")
    if type(barred) is not dict:
        raise ResponseTimingV3Error("barred_v6_r1_dependency_missing")
    contract_spec = _artifact_spec(barred.get("contract"), path="barred_v6_r1.contract")
    manifest_spec = _artifact_spec(barred.get("manifest"), path="barred_v6_r1.manifest")
    predecessor_contract = _strict_json(_read_exact(*contract_spec), label="barred_v6_r1_contract")
    predecessor_manifest = _strict_json(_read_exact(*manifest_spec), label="barred_v6_r1_manifest")
    if (
        predecessor_contract.get("contract_id") != BARRED_V6_R1_CONTRACT_ID
        or predecessor_contract.get("cohort_id") != BARRED_V6_R1_COHORT_ID
        or predecessor_manifest.get("contract_id") != BARRED_V6_R1_CONTRACT_ID
        or predecessor_manifest.get("cohort_id") != BARRED_V6_R1_COHORT_ID
    ):
        raise ResponseTimingV3Error("barred_v6_r1_identity_mismatch")
    if barred.get("may_supply_timing_rows") is not False or barred.get("may_supply_proof_rows") is not False:
        raise ResponseTimingV3Error("barred_v6_r1_can_supply_rows")
    if predecessor_manifest.get("review_state") == "independent_approved":
        raise ResponseTimingV3Error("barred_v6_r1_unexpectedly_claims_approval")
    return {
        "contract_id": BARRED_V6_R1_CONTRACT_ID,
        "cohort_id": BARRED_V6_R1_COHORT_ID,
        "contract_sha256": contract_spec[1],
        "manifest_sha256": manifest_spec[1],
        "may_supply_rows": False,
    }


def production_dependency_status() -> dict[str, Any]:
    """Verify immutable prerequisites and return the permanently closed gate."""

    try:
        contract = _load_contract()
        r3 = _verify_r3(contract)
        currencies, instruments = _load_universe(contract)
        barred = _verify_barred_r1(contract)
    except ResponseTimingV3Error as exc:
        return {
            "contract_id": CONTRACT_ID,
            "approved": False,
            "blocked": True,
            "blocking_reasons": [f"immutable_dependency_verification_failed:{exc}"],
            "r3": None,
            "canonical_universe": None,
            "barred_predecessor": None,
            "required_event_response": None,
            "row_count": 0,
            "proof_row_count": 0,
            **_guard(),
        }
    successor = contract.get("required_event_response_successor")
    if type(successor) is not dict:
        reason = "event_response_v6_r2_requirement_missing"
    elif (
        successor.get("required_contract_id") != REQUIRED_V6_R2_CONTRACT_ID
        or successor.get("required_cohort_id") != REQUIRED_V6_R2_COHORT_ID
    ):
        reason = "event_response_v6_r2_identity_not_bound"
    elif any(
        successor.get(field) is None
        for field in (
            "contract_artifact",
            "manifest_artifact",
            "independent_review_certificate_artifact",
        )
    ):
        reason = "event_response_v6_r2_final_approval_not_bound"
    elif successor.get("approval_state") != "independent_approved_frozen":
        reason = "event_response_v6_r2_not_independently_approved"
    else:
        reason = "resolved_v6_r2_requires_new_timing_contract_id"
    return {
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "approved": False,
        "blocked": True,
        "blocking_reasons": [reason],
        "r3": r3,
        "canonical_universe": {
            "contract_id": UNIVERSE_CONTRACT_ID,
            "currency_count": len(currencies),
            "instrument_count": len(instruments),
            "instrument_universe_sha256": INSTRUMENT_UNIVERSE_SHA256,
            "verified": True,
        },
        "barred_predecessor": barred,
        "required_event_response": {
            "contract_id": REQUIRED_V6_R2_CONTRACT_ID,
            "cohort_id": REQUIRED_V6_R2_COHORT_ID,
            "approval_state": None if type(successor) is not dict else successor.get("approval_state"),
        },
        "row_count": 0,
        "proof_row_count": 0,
        **_guard(),
    }


def require_production_dependencies() -> None:
    status = production_dependency_status()
    if status.get("approved") is not True:
        reasons = ",".join(status.get("blocking_reasons") or ["dependency_not_approved"])
        raise ResponseTimingV3DependencyError(reasons)


def build_replay_diagnostic(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable: timing V3 R1 has no canonical builder")


def build_prospective_proof(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable: timing V3 R1 has no proof builder")


_ATTESTATION_FIELDS = {
    "contract_id",
    "cohort_id",
    "source_snapshot_id",
    "source_binding_sha256",
    "contract_sha256",
    "manifest_sha256",
    "review_certificate_id",
    "review_certificate_sha256",
    "review_disposition",
    "artifact_closure_verified",
    "test_fixture_only",
}

_SOURCE_FIELDS = {
    "snapshot_id",
    "contract_id",
    "cohort_id",
    "mode",
    "partition",
    "capture_cutoff_utc",
    "source_binding_sha256",
    "response_diagnostics",
    "proof_row_count",
    "test_only",
    "canonical_output",
    "research_only",
    "execution_eligible",
    "can_place_orders",
    "can_promote",
    "can_authorize",
    "supported_execution_decision",
}

_RESPONSE_FIELDS = {
    "response_id",
    "partition",
    "event_id",
    "event_version_id",
    "currency",
    "instrument",
    "horizon_sec",
    "event_scheduled_utc",
    "start_target_utc",
    "end_target_utc",
    "start_quote_time_utc",
    "end_quote_time_utc",
    "start_known_utc",
    "end_known_utc",
    "start_bid",
    "start_ask",
    "end_bid",
    "end_ask",
    "pip",
    "start_source_payload_sha256",
    "end_source_payload_sha256",
    "start_source_record_id",
    "end_source_record_id",
    "direction_selected",
    "forecast_mean_pips",
    "forecast_probability",
    "expected_value_pips",
    "allocator_rank",
    "basis_eligible",
    "proof_eligible",
    "research_only",
    "execution_eligible",
    "can_place_orders",
    "can_promote",
    "can_authorize",
    "supported_execution_decision",
}

_DERIVED_RESPONSE_FIELDS = {
    "start_alignment_sec",
    "end_alignment_sec",
    "actual_quote_duration_sec",
    "start_spread_pips",
    "end_spread_pips",
    "gross_long_mid_pips",
    "gross_short_mid_pips",
    "long_after_executable_spread_pips",
    "short_after_executable_spread_pips",
    "long_execution_cost_pips",
    "short_execution_cost_pips",
}

_RESULT_FIELDS = {
    "schema_version",
    "snapshot_schema",
    "snapshot_id",
    "contract_id",
    "contract_sha256",
    "research_generation",
    "mode",
    "partition",
    "test_only",
    "canonical_output",
    "source_snapshot_id",
    "source_contract_id",
    "source_cohort_id",
    "source_binding_sha256",
    "dependency_attestation",
    "capture_cutoff_utc",
    "response_count",
    "timing_arm_count",
    "proof_row_count",
    "responses",
    "timing_arms",
    "limitations",
    "research_only",
    "execution_eligible",
    "can_place_orders",
    "can_promote",
    "can_authorize",
    "supported_execution_decision",
}


def _validate_test_attestation(
    value: Any, *, source_snapshot_id: str, source_binding_sha256: str
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ATTESTATION_FIELDS:
        raise ResponseTimingV3Error("test_attestation_closed_schema_mismatch")
    if value["contract_id"] != REQUIRED_V6_R2_CONTRACT_ID:
        raise ResponseTimingV3Error("test_attestation_contract_id_mismatch")
    if value["cohort_id"] != REQUIRED_V6_R2_COHORT_ID:
        raise ResponseTimingV3Error("test_attestation_cohort_id_mismatch")
    if value["source_snapshot_id"] != source_snapshot_id:
        raise ResponseTimingV3Error("test_attestation_source_snapshot_mismatch")
    if value["source_binding_sha256"] != source_binding_sha256:
        raise ResponseTimingV3Error("test_attestation_source_binding_mismatch")
    for field in (
        "source_binding_sha256",
        "contract_sha256",
        "manifest_sha256",
        "review_certificate_sha256",
    ):
        if not _is_sha256(value[field]):
            raise ResponseTimingV3Error(f"test_attestation_{field}_invalid")
    if type(value["review_certificate_id"]) is not str or not value["review_certificate_id"]:
        raise ResponseTimingV3Error("test_attestation_review_certificate_id_invalid")
    if value["review_disposition"] != "independent_approve_disabled_unregistered_candidate":
        raise ResponseTimingV3Error("test_attestation_not_approved")
    if value["artifact_closure_verified"] is not True or value["test_fixture_only"] is not True:
        raise ResponseTimingV3Error("test_attestation_guards_invalid")
    return copy.deepcopy(value)


def _validate_source_envelope(value: Any) -> tuple[dict[str, Any], dt.datetime]:
    if type(value) is not dict or set(value) != _SOURCE_FIELDS:
        raise ResponseTimingV3Error("event_response_snapshot_closed_schema_mismatch")
    source = copy.deepcopy(value)
    if source.get("contract_id") != REQUIRED_V6_R2_CONTRACT_ID:
        raise ResponseTimingV3Error("event_response_snapshot_contract_mismatch")
    if source.get("cohort_id") != REQUIRED_V6_R2_COHORT_ID:
        raise ResponseTimingV3Error("event_response_snapshot_cohort_mismatch")
    mode = source.get("mode")
    if mode not in DIAGNOSTIC_MODES:
        raise ResponseTimingV3Error("event_response_snapshot_mode_not_test_diagnostic")
    if source.get("partition") != DIAGNOSTIC_MODES[mode]:
        raise ResponseTimingV3Error("event_response_snapshot_partition_mismatch")
    for field, expected in _guard().items():
        if source.get(field) != expected or type(source.get(field)) is not type(expected):
            raise ResponseTimingV3Error(f"unsafe_source_guard:{field}")
    if source.get("test_only") is not True or source.get("canonical_output") is not False:
        raise ResponseTimingV3Error("event_response_snapshot_not_test_only")
    if source.get("proof_row_count") != 0 or type(source.get("proof_row_count")) is not int:
        raise ResponseTimingV3Error("event_response_snapshot_proof_rows_forbidden")
    if type(source.get("snapshot_id")) is not str or not source["snapshot_id"]:
        raise ResponseTimingV3Error("event_response_snapshot_id_invalid")
    if not _is_sha256(source.get("source_binding_sha256")):
        raise ResponseTimingV3Error("event_response_source_binding_invalid")
    if type(source.get("response_diagnostics")) not in {list, tuple}:
        raise ResponseTimingV3Error("response_diagnostics_must_be_sequence")
    cutoff = _utc(source.get("capture_cutoff_utc"), path="capture_cutoff_utc")
    source["capture_cutoff_utc"] = _iso(cutoff)
    return source, cutoff


def _response_identity(row: Mapping[str, Any], *, source_binding_sha256: str, mode: str) -> str:
    material = {
        "contract_id": REQUIRED_V6_R2_CONTRACT_ID,
        "cohort_id": REQUIRED_V6_R2_COHORT_ID,
        "mode": mode,
        "source_binding_sha256": source_binding_sha256,
        **{key: row[key] for key in (
            "event_id",
            "event_version_id",
            "currency",
            "instrument",
            "horizon_sec",
            "event_scheduled_utc",
            "start_target_utc",
            "end_target_utc",
            "start_quote_time_utc",
            "end_quote_time_utc",
            "start_known_utc",
            "end_known_utc",
            "start_bid",
            "start_ask",
            "end_bid",
            "end_ask",
            "pip",
            "start_source_payload_sha256",
            "end_source_payload_sha256",
            "start_source_record_id",
            "end_source_record_id",
        )},
    }
    return "event_response_v6_r2_" + _sha256_json(material)[:40]


def _validated_response(
    value: Any,
    *,
    mode: str,
    partition: str,
    capture_cutoff: dt.datetime,
    source_binding_sha256: str,
    response_contract: Mapping[str, Any],
    currencies: tuple[str, ...],
    instruments: tuple[str, ...],
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _RESPONSE_FIELDS:
        raise ResponseTimingV3Error("response_closed_schema_mismatch")
    row = copy.deepcopy(value)
    for field, expected in _guard().items():
        if row.get(field) != expected or type(row.get(field)) is not type(expected):
            raise ResponseTimingV3Error(f"unsafe_response_guard:{field}")
    if row.get("partition") != partition:
        raise ResponseTimingV3Error("response_partition_mismatch")
    for field in (
        "direction_selected",
        "forecast_mean_pips",
        "forecast_probability",
        "expected_value_pips",
        "allocator_rank",
    ):
        if row.get(field) is not None:
            raise ResponseTimingV3Error(f"response_invented_forecast_field:{field}")
    if row.get("basis_eligible") is not False or row.get("proof_eligible") is not False:
        raise ResponseTimingV3Error("response_invented_basis_or_proof")
    if type(row.get("instrument")) is not str or row["instrument"] not in instruments:
        raise ResponseTimingV3Error("response_instrument_outside_canonical_universe")
    base, quote = row["instrument"].split("_")
    if row.get("currency") not in currencies or row["currency"] not in {base, quote}:
        raise ResponseTimingV3Error("response_currency_not_canonical_pair_leg")
    horizon = row.get("horizon_sec")
    if type(horizon) is not int or horizon not in HORIZONS_SEC:
        raise ResponseTimingV3Error("response_horizon_invalid")
    event_time = _utc(row["event_scheduled_utc"], path="response.event_scheduled_utc")
    start_target = _utc(row["start_target_utc"], path="response.start_target_utc")
    end_target = _utc(row["end_target_utc"], path="response.end_target_utc")
    start_quote = _utc(row["start_quote_time_utc"], path="response.start_quote_time_utc")
    end_quote = _utc(row["end_quote_time_utc"], path="response.end_quote_time_utc")
    start_known = _utc(row["start_known_utc"], path="response.start_known_utc")
    end_known = _utc(row["end_known_utc"], path="response.end_known_utc")
    if start_target != event_time or end_target != event_time + dt.timedelta(seconds=horizon):
        raise ResponseTimingV3Error("response_target_chronology_invalid")
    start_alignment = (start_quote - start_target).total_seconds()
    end_alignment = (end_quote - end_target).total_seconds()
    if not 0 <= start_alignment <= float(response_contract["maximum_start_alignment_sec"]):
        raise ResponseTimingV3Error("response_start_alignment_invalid")
    if not 0 <= end_alignment <= float(response_contract["maximum_end_alignment_sec"]):
        raise ResponseTimingV3Error("response_end_alignment_invalid")
    maximum_known_lag = float(response_contract["maximum_quote_known_lag_sec"])
    for quote_time, known_time, label in (
        (start_quote, start_known, "start"),
        (end_quote, end_known, "end"),
    ):
        lag = (known_time - quote_time).total_seconds()
        if lag < 0 or lag > maximum_known_lag or known_time > capture_cutoff:
            raise ResponseTimingV3Error(f"response_{label}_knowledge_clock_invalid")
    if start_quote > end_quote or start_known > end_known:
        raise ResponseTimingV3Error("response_clock_order_invalid")
    start_bid = _number(row["start_bid"], path="response.start_bid", positive=True)
    start_ask = _number(row["start_ask"], path="response.start_ask", positive=True)
    end_bid = _number(row["end_bid"], path="response.end_bid", positive=True)
    end_ask = _number(row["end_ask"], path="response.end_ask", positive=True)
    pip = _number(row["pip"], path="response.pip", positive=True)
    if start_ask <= start_bid or end_ask <= end_bid:
        raise ResponseTimingV3Error("response_bid_ask_invalid")
    maximum_spread = float(response_contract["maximum_spread_pips"])
    if (start_ask - start_bid) / pip > maximum_spread or (end_ask - end_bid) / pip > maximum_spread:
        raise ResponseTimingV3Error("response_spread_exceeds_contract")
    for field in ("start_source_payload_sha256", "end_source_payload_sha256"):
        if not _is_sha256(row.get(field)):
            raise ResponseTimingV3Error(f"response_{field}_invalid")
    for field in (
        "response_id",
        "event_id",
        "event_version_id",
        "start_source_record_id",
        "end_source_record_id",
    ):
        if type(row.get(field)) is not str or not row[field]:
            raise ResponseTimingV3Error(f"response_{field}_invalid")
    expected_id = _response_identity(row, source_binding_sha256=source_binding_sha256, mode=mode)
    if row["response_id"] != expected_id:
        raise ResponseTimingV3Error("response_identity_mismatch")
    start_mid = (start_bid + start_ask) / 2.0
    end_mid = (end_bid + end_ask) / 2.0
    gross_long = (end_mid - start_mid) / pip
    long_after = (end_bid - start_ask) / pip
    short_after = (start_bid - end_ask) / pip
    return {
        **row,
        "event_scheduled_utc": _iso(event_time),
        "start_target_utc": _iso(start_target),
        "end_target_utc": _iso(end_target),
        "start_quote_time_utc": _iso(start_quote),
        "end_quote_time_utc": _iso(end_quote),
        "start_known_utc": _iso(start_known),
        "end_known_utc": _iso(end_known),
        "start_alignment_sec": start_alignment,
        "end_alignment_sec": end_alignment,
        "actual_quote_duration_sec": (end_quote - start_quote).total_seconds(),
        "start_spread_pips": (start_ask - start_bid) / pip,
        "end_spread_pips": (end_ask - end_bid) / pip,
        "gross_long_mid_pips": gross_long,
        "gross_short_mid_pips": -gross_long,
        "long_after_executable_spread_pips": long_after,
        "short_after_executable_spread_pips": short_after,
        "long_execution_cost_pips": gross_long - long_after,
        "short_execution_cost_pips": -gross_long - short_after,
    }


def _arms(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    long_after = float(row["long_after_executable_spread_pips"])
    short_after = float(row["short_after_executable_spread_pips"])
    if long_after > 0 and long_after > short_after:
        observed_direction = "long"
        clearance = "long_only"
    elif short_after > 0 and short_after > long_after:
        observed_direction = "short"
        clearance = "short_only"
    else:
        observed_direction = None
        clearance = "neither"
    common = {
        "response_id": row["response_id"],
        "event_id": row["event_id"],
        "event_version_id": row["event_version_id"],
        "instrument": row["instrument"],
        "horizon_sec": row["horizon_sec"],
        "source_partition": row["partition"],
        "post_outcome_only": True,
        "forecast_direction": None,
        "forecast_probability": None,
        "expected_value_pips": None,
        "allocator_rank": None,
        "basis_eligible": False,
        "proof_eligible": False,
        **_guard(),
    }
    return [
        {
            **common,
            "arm_id": "observed_response",
            "observed_direction_after_cost": observed_direction,
            "observed_long_after_cost_pips": long_after,
            "observed_short_after_cost_pips": short_after,
        },
        {
            **common,
            "arm_id": "cost_clearance",
            "cost_clearance_state": clearance,
            "best_observed_after_cost_pips": max(long_after, short_after),
        },
        {
            **common,
            "arm_id": "response_latency",
            "start_alignment_sec": row["start_alignment_sec"],
            "end_alignment_sec": row["end_alignment_sec"],
            "start_known_lag_sec": (
                _utc(row["start_known_utc"], path="row.start_known_utc")
                - _utc(row["start_quote_time_utc"], path="row.start_quote_time_utc")
            ).total_seconds(),
            "end_known_lag_sec": (
                _utc(row["end_known_utc"], path="row.end_known_utc")
                - _utc(row["end_quote_time_utc"], path="row.end_quote_time_utc")
            ).total_seconds(),
        },
        {**common, "arm_id": "no_trade", "control_state": "no_trade"},
    ]


def _canonical_responses(
    values: list[Any] | tuple[Any, ...],
    *,
    mode: str,
    partition: str,
    capture_cutoff: dt.datetime,
    source_binding_sha256: str,
    response_contract: Mapping[str, Any],
    currencies: tuple[str, ...],
    instruments: tuple[str, ...],
    enriched: bool,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen_natural: set[tuple[str, str, int]] = set()
    seen_ids: set[str] = set()
    for value in values:
        if enriched:
            if type(value) is not dict or set(value) != _RESPONSE_FIELDS | _DERIVED_RESPONSE_FIELDS:
                raise ResponseTimingV3Error("enriched_response_closed_schema_mismatch")
            raw = {key: copy.deepcopy(value[key]) for key in _RESPONSE_FIELDS}
        else:
            raw = value
        row = _validated_response(
            raw,
            mode=mode,
            partition=partition,
            capture_cutoff=capture_cutoff,
            source_binding_sha256=source_binding_sha256,
            response_contract=response_contract,
            currencies=currencies,
            instruments=instruments,
        )
        if enriched and _canonical_json(row) != _canonical_json(value):
            raise ResponseTimingV3Error("enriched_response_reconstruction_mismatch")
        natural = (row["event_version_id"], row["instrument"], row["horizon_sec"])
        if natural in seen_natural:
            raise ResponseTimingV3Error("duplicate_response_natural_cell")
        if row["response_id"] in seen_ids:
            raise ResponseTimingV3Error("duplicate_response_id")
        seen_natural.add(natural)
        seen_ids.add(row["response_id"])
        result.append(row)
    result.sort(
        key=lambda row: (
            row["event_scheduled_utc"],
            row["event_version_id"],
            row["instrument"],
            row["horizon_sec"],
        )
    )
    return result


def _canonical_arms(responses: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for response in responses:
        rows.extend(_arms(response))
    rows.sort(
        key=lambda row: (
            row["event_version_id"],
            row["instrument"],
            row["horizon_sec"],
            ARM_IDS.index(row["arm_id"]),
        )
    )
    return rows


def build_test_diagnostic_snapshot(
    *, event_response_snapshot: Mapping[str, Any], dependency_attestation: Mapping[str, Any]
) -> dict[str, Any]:
    """Build a deterministic, noncanonical test-only diagnostic snapshot."""

    contract = _load_contract()
    _verify_r3(contract)
    currencies, instruments = _load_universe(contract)
    _verify_barred_r1(contract)
    source, capture_cutoff = _validate_source_envelope(event_response_snapshot)
    attestation = _validate_test_attestation(
        dependency_attestation,
        source_snapshot_id=source["snapshot_id"],
        source_binding_sha256=source["source_binding_sha256"],
    )
    responses = _canonical_responses(
        source["response_diagnostics"],
        mode=source["mode"],
        partition=source["partition"],
        capture_cutoff=capture_cutoff,
        source_binding_sha256=source["source_binding_sha256"],
        response_contract=contract["response_contract"],
        currencies=currencies,
        instruments=instruments,
        enriched=False,
    )
    arm_rows = _canonical_arms(responses)
    result: dict[str, Any] = {
        "schema_version": 3,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "snapshot_id": None,
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "research_generation": RESEARCH_GENERATION,
        "mode": source["mode"],
        "partition": source["partition"],
        "test_only": True,
        "canonical_output": False,
        "source_snapshot_id": source["snapshot_id"],
        "source_contract_id": source["contract_id"],
        "source_cohort_id": source["cohort_id"],
        "source_binding_sha256": source["source_binding_sha256"],
        "dependency_attestation": attestation,
        "capture_cutoff_utc": _iso(capture_cutoff),
        "response_count": len(responses),
        "timing_arm_count": len(arm_rows),
        "proof_row_count": 0,
        "responses": responses,
        "timing_arms": arm_rows,
        "limitations": list(LIMITATIONS),
        **_guard(),
    }
    material = {key: value for key, value in result.items() if key != "snapshot_id"}
    result["snapshot_id"] = "currency_state_response_timing_v3_test_" + _sha256_json(material)[:32]
    validate_test_diagnostic_snapshot(result)
    return result


def validate_test_diagnostic_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Fully reconstruct and validate every byte-significant diagnostic field."""

    if type(snapshot) is not dict or set(snapshot) != _RESULT_FIELDS:
        raise ResponseTimingV3Error("timing_snapshot_closed_schema_mismatch")
    contract = _load_contract()
    _verify_r3(contract)
    currencies, instruments = _load_universe(contract)
    _verify_barred_r1(contract)
    if (
        snapshot.get("schema_version") != 3
        or snapshot.get("snapshot_schema") != SNAPSHOT_SCHEMA
        or snapshot.get("contract_id") != CONTRACT_ID
        or snapshot.get("contract_sha256") != CONTRACT_SHA256
        or snapshot.get("research_generation") != RESEARCH_GENERATION
    ):
        raise ResponseTimingV3Error("timing_snapshot_identity_mismatch")
    mode = snapshot.get("mode")
    if mode not in DIAGNOSTIC_MODES or snapshot.get("partition") != DIAGNOSTIC_MODES[mode]:
        raise ResponseTimingV3Error("timing_snapshot_partition_invalid")
    if snapshot.get("test_only") is not True or snapshot.get("canonical_output") is not False:
        raise ResponseTimingV3Error("timing_snapshot_not_test_only")
    if snapshot.get("proof_row_count") != 0 or type(snapshot.get("proof_row_count")) is not int:
        raise ResponseTimingV3Error("timing_snapshot_proof_rows_forbidden")
    for field, expected in _guard().items():
        if snapshot.get(field) != expected or type(snapshot.get(field)) is not type(expected):
            raise ResponseTimingV3Error(f"unsafe_timing_snapshot_guard:{field}")
    if snapshot.get("source_contract_id") != REQUIRED_V6_R2_CONTRACT_ID:
        raise ResponseTimingV3Error("timing_snapshot_source_contract_mismatch")
    if snapshot.get("source_cohort_id") != REQUIRED_V6_R2_COHORT_ID:
        raise ResponseTimingV3Error("timing_snapshot_source_cohort_mismatch")
    if type(snapshot.get("source_snapshot_id")) is not str or not snapshot["source_snapshot_id"]:
        raise ResponseTimingV3Error("timing_snapshot_source_id_invalid")
    if not _is_sha256(snapshot.get("source_binding_sha256")):
        raise ResponseTimingV3Error("timing_snapshot_source_binding_invalid")
    _validate_test_attestation(
        snapshot.get("dependency_attestation"),
        source_snapshot_id=snapshot["source_snapshot_id"],
        source_binding_sha256=snapshot["source_binding_sha256"],
    )
    cutoff = _utc(snapshot.get("capture_cutoff_utc"), path="timing.capture_cutoff_utc")
    if snapshot["capture_cutoff_utc"] != _iso(cutoff):
        raise ResponseTimingV3Error("timing_capture_cutoff_not_canonical")
    responses_value = snapshot.get("responses")
    arms_value = snapshot.get("timing_arms")
    if type(responses_value) is not list or type(arms_value) is not list:
        raise ResponseTimingV3Error("timing_snapshot_rows_invalid")
    responses = _canonical_responses(
        responses_value,
        mode=mode,
        partition=snapshot["partition"],
        capture_cutoff=cutoff,
        source_binding_sha256=snapshot["source_binding_sha256"],
        response_contract=contract["response_contract"],
        currencies=currencies,
        instruments=instruments,
        enriched=True,
    )
    if _canonical_json(responses) != _canonical_json(responses_value):
        raise ResponseTimingV3Error("timing_response_order_or_content_mismatch")
    expected_arms = _canonical_arms(responses)
    if _canonical_json(expected_arms) != _canonical_json(arms_value):
        raise ResponseTimingV3Error("timing_arm_reconstruction_or_linkage_mismatch")
    if snapshot.get("response_count") != len(responses):
        raise ResponseTimingV3Error("timing_snapshot_response_count_mismatch")
    if snapshot.get("timing_arm_count") != len(expected_arms):
        raise ResponseTimingV3Error("timing_snapshot_arm_count_mismatch")
    if snapshot.get("limitations") != LIMITATIONS:
        raise ResponseTimingV3Error("timing_snapshot_limitations_mismatch")
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    expected_snapshot_id = "currency_state_response_timing_v3_test_" + _sha256_json(material)[:32]
    if snapshot.get("snapshot_id") != expected_snapshot_id:
        raise ResponseTimingV3Error("timing_snapshot_id_mismatch")


def verify_candidate_manifest() -> dict[str, Any]:
    """Verify the inert V3 candidate closure without treating it as approval."""

    manifest = _strict_json(
        _stable_read("config/currency_state_response_timing_arms_v3_manifest.json"),
        label="timing_v3_manifest",
    )
    if manifest.get("manifest_id") != "currency_state_response_timing_arms_v3_manifest_20260817_r1":
        raise ResponseTimingV3Error("candidate_manifest_identity_mismatch")
    if manifest.get("contract_id") != CONTRACT_ID:
        raise ResponseTimingV3Error("candidate_manifest_contract_mismatch")
    if manifest.get("review_state") != "pending_independent_review":
        raise ResponseTimingV3Error("candidate_manifest_cannot_self_approve")
    if manifest.get("state") != {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "canonical_output_initialized": False,
    }:
        raise ResponseTimingV3Error("candidate_manifest_not_inert")
    disposition = manifest.get("dependency_disposition")
    if type(disposition) is not dict or disposition.get("production_dependency_gate") != "closed":
        raise ResponseTimingV3Error("candidate_manifest_dependency_gate_not_closed")
    if disposition.get("approved_event_response_v6_r2_available") is not False:
        raise ResponseTimingV3Error("candidate_manifest_invented_v6_r2_approval")
    output = manifest.get("output_surface")
    if type(output) is not dict or any(
        output.get(field) is not None
        for field in (
            "canonical_ledger_path",
            "canonical_state_path",
            "canonical_report_path",
            "worker_entrypoint",
            "collector_entrypoint",
        )
    ):
        raise ResponseTimingV3Error("candidate_manifest_runtime_or_canonical_surface")
    expected_paths = {
        "runtime_core": "src/forex_system/research/currency_state_response_timing_arms_v3.py",
        "contract": "config/currency_state_response_timing_arms_v3.json",
        "adversarial_tests": "test_currency_state_response_timing_arms_v3.py",
        "parent_v2_module": "src/forex_system/research/currency_state_response_timing_arms_v2.py",
        "parent_v2_contract": "config/currency_state_response_timing_arms_v2.json",
        "parent_v2_manifest": "config/currency_state_response_timing_arms_v2_manifest.json",
        "parent_v2_tests": "test_currency_state_response_timing_arms_v2.py",
        "approved_r3_review_certificate": "config/official_fact_currency_state_v5_r3_review_certificate.json",
        "official_fact_v5_r3_manifest": "config/official_fact_adapter_v5_manifest.json",
        "currency_state_context_v5_r3_manifest": "config/currency_state_official_context_v5_manifest.json",
        "canonical_universe_contract": "config/currency_state_engine_v2.json",
        "barred_event_response_v6_r1_contract": "config/prospective_event_response_v6_contract.json",
        "barred_event_response_v6_r1_manifest": "config/prospective_event_response_v6_manifest.json",
    }
    artifacts = manifest.get("artifacts")
    if type(artifacts) is not dict or set(artifacts) != set(expected_paths):
        raise ResponseTimingV3Error("candidate_manifest_artifact_set_mismatch")
    for name, expected_path in expected_paths.items():
        spec = _artifact_spec(artifacts[name], path=f"candidate_manifest.{name}")
        if spec[0] != expected_path:
            raise ResponseTimingV3Error(f"candidate_manifest_artifact_path_mismatch:{name}")
        _read_exact(*spec)
    return {
        "manifest_id": manifest["manifest_id"],
        "review_state": manifest["review_state"],
        "artifact_count": len(artifacts),
        "dependency_gate": "closed",
        "candidate_closure_verified": True,
    }


__all__ = [
    "ARM_IDS",
    "CONTRACT_ID",
    "CONTRACT_SHA256",
    "DIAGNOSTIC_MODES",
    "REQUIRED_V6_R2_COHORT_ID",
    "REQUIRED_V6_R2_CONTRACT_ID",
    "ResponseTimingV3DependencyError",
    "ResponseTimingV3Error",
    "build_prospective_proof",
    "build_replay_diagnostic",
    "build_test_diagnostic_snapshot",
    "production_dependency_status",
    "require_production_dependencies",
    "validate_test_diagnostic_snapshot",
    "verify_candidate_manifest",
]
