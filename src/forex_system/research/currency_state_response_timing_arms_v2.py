"""Disabled R3-bound response/timing research successor V2.

The production dependency gate is intentionally closed.  Approved
OfficialFact/CurrencyState V5 R3 bytes are pinned, but the rejected V6 R1
event-response candidate is only a barred predecessor and no independently
approved V6 R2 dependency exists yet.  Consequently this module cannot emit
canonical rows or prospective proof.

The sole builder is an explicit noncanonical test-fixture surface.  It keeps
replay and prospective-capture diagnostics in different partitions, treats
all price response as post-outcome observation rather than forecast evidence,
and has no filesystem write, worker, broker, lifecycle, promotion,
authorization, or execution surface.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..contracts.currency_state import stable_hash


UTC = dt.timezone.utc
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_ID = "currency_state_response_timing_arms_v2_20260817_r1"
SNAPSHOT_SCHEMA = "currency_state_response_timing_diagnostic_snapshot_v2"
RESEARCH_GENERATION = "r3_bound_response_timing_v2_r1"
SUPPORTED_EXECUTION_DECISION = "no_trade"
CONTRACT_PATH = PROJECT_ROOT / "config" / "currency_state_response_timing_arms_v2.json"
MANIFEST_PATH = PROJECT_ROOT / "config" / "currency_state_response_timing_arms_v2_manifest.json"
CONTRACT_SHA256 = "2885b19773565ce63dd706ced1c4e5eb9c5405c028664f60befc155b8f378618"
CONTRACT_BYTES = 5_732

R3_CERTIFICATE_ID = "official_fact_currency_state_v5_r3_independent_review_20260817"
R3_CERTIFICATE_SHA256 = "0a61948a9fc2f2a9a06d30557b838d7fb389958c9ef2b1845bc8b06e70c7b299"
R3_CERTIFICATE_BYTES = 6_003
R3_ADAPTER_CONTRACT_ID = "official_fact_adapter_v5_20260817_r3"
R3_CONTEXT_CONTRACT_ID = "currency_state_official_context_v5_20260817_r3"
R3_BASIS_CONTRACT_ID = "official_fact_basis_eligibility_v5_20260817_r3"
R3_CLOCK_CONTRACT_ID = "immutable_point_in_time_event_clock_v2_20260817"

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


class ResponseTimingV2Error(ValueError):
    """Base error for closed schemas, chronology, or guard violations."""


class ResponseTimingV2DependencyError(ResponseTimingV2Error):
    """Raised when the immutable upstream approval gate is not open."""


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
                raise ResponseTimingV2Error(f"{label}_duplicate_json_key:{key}")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise ResponseTimingV2Error(f"{label}_nonfinite_json:{value}")

    try:
        return json.loads(
            payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResponseTimingV2Error(f"{label}_invalid_json") from exc


def _safe_path(relative_path: str) -> Path:
    if type(relative_path) is not str or not relative_path or "\\" in relative_path:
        raise ResponseTimingV2Error("artifact_path_invalid")
    candidate = PROJECT_ROOT.joinpath(*relative_path.split("/"))
    try:
        root = PROJECT_ROOT.resolve(strict=True)
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ResponseTimingV2Error(f"artifact_path_escape_or_missing:{relative_path}") from exc
    if candidate.is_symlink() or resolved != candidate.resolve():
        raise ResponseTimingV2Error(f"artifact_symlink_forbidden:{relative_path}")
    return candidate


def _read_exact(relative_path: str, sha256: str, byte_length: int) -> bytes:
    path = _safe_path(relative_path)
    before = path.stat()
    payload = path.read_bytes()
    after = path.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise ResponseTimingV2Error(f"artifact_identity_changed:{relative_path}")
    if after.st_nlink != 1:
        raise ResponseTimingV2Error(f"artifact_hardlink_forbidden:{relative_path}")
    if len(payload) != byte_length or _sha256_bytes(payload) != sha256:
        raise ResponseTimingV2Error(f"artifact_content_mismatch:{relative_path}")
    return payload


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _utc(value: Any, *, path: str) -> dt.datetime:
    if type(value) is not str or not value:
        raise ResponseTimingV2Error(f"{path}_must_be_strict_utc_string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ResponseTimingV2Error(f"{path}_invalid_utc") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ResponseTimingV2Error(f"{path}_must_have_zero_utc_offset")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _number(value: Any, *, path: str, positive: bool = False) -> float:
    if type(value) not in {int, float}:
        raise ResponseTimingV2Error(f"{path}_must_be_number")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0.0):
        raise ResponseTimingV2Error(f"{path}_invalid_number")
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
        raise ResponseTimingV2Error(f"{path}_closed_schema_mismatch")
    relative = value["relative_path"]
    sha256 = value["sha256"]
    byte_length = value["byte_length"]
    if type(relative) is not str or not relative or not _is_sha256(sha256):
        raise ResponseTimingV2Error(f"{path}_identity_invalid")
    if type(byte_length) is not int or byte_length <= 0:
        raise ResponseTimingV2Error(f"{path}_byte_length_invalid")
    return relative, sha256, byte_length


def _load_contract() -> Mapping[str, Any]:
    contract = _strict_json(
        _read_exact(
            "config/currency_state_response_timing_arms_v2.json",
            CONTRACT_SHA256,
            CONTRACT_BYTES,
        ),
        label="timing_v2_contract",
    )
    if contract.get("contract_id") != CONTRACT_ID or contract.get("schema_version") != 2:
        raise ResponseTimingV2Error("timing_contract_identity_mismatch")
    state = contract.get("state")
    expected_state = {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "canonical_output_initialized": False,
        "independent_review_state": "pending",
    }
    if state != expected_state:
        raise ResponseTimingV2Error("timing_contract_not_inert")
    policy = contract.get("policy")
    if type(policy) is not dict:
        raise ResponseTimingV2Error("timing_policy_missing")
    for field, expected in _guard().items():
        if policy.get(field) != expected or type(policy.get(field)) is not type(expected):
            raise ResponseTimingV2Error(f"unsafe_timing_policy:{field}")
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
        raise ResponseTimingV2Error("timing_contract_runtime_or_canonical_surface")
    return contract


def _verify_r3(contract: Mapping[str, Any]) -> dict[str, Any]:
    dependency = contract.get("approved_r3_dependency")
    if type(dependency) is not dict:
        raise ResponseTimingV2Error("r3_dependency_missing")
    cert_spec = _artifact_spec(dependency.get("review_certificate"), path="r3.review_certificate")
    adapter_spec = _artifact_spec(dependency.get("official_fact_manifest"), path="r3.adapter_manifest")
    context_spec = _artifact_spec(dependency.get("currency_state_manifest"), path="r3.context_manifest")
    if cert_spec != (
        "config/official_fact_currency_state_v5_r3_review_certificate.json",
        R3_CERTIFICATE_SHA256,
        R3_CERTIFICATE_BYTES,
    ):
        raise ResponseTimingV2Error("r3_certificate_spec_mismatch")
    certificate = _strict_json(_read_exact(*cert_spec), label="r3_certificate")
    adapter_manifest = _strict_json(_read_exact(*adapter_spec), label="r3_adapter_manifest")
    context_manifest = _strict_json(_read_exact(*context_spec), label="r3_context_manifest")
    if certificate.get("review_certificate_id") != R3_CERTIFICATE_ID:
        raise ResponseTimingV2Error("r3_certificate_id_mismatch")
    if certificate.get("review_disposition") != "independent_approve_disabled_unregistered_candidate":
        raise ResponseTimingV2Error("r3_certificate_not_approved")
    results = certificate.get("review_results")
    if type(results) is not dict or any(
        type(results.get(key)) is not int or results.get(key) != 0
        for key in ("p0_findings", "p1_findings", "p2_findings")
    ):
        raise ResponseTimingV2Error("r3_certificate_has_material_findings")
    if dependency.get("official_fact_adapter_contract_id") != R3_ADAPTER_CONTRACT_ID:
        raise ResponseTimingV2Error("r3_adapter_contract_mismatch")
    if dependency.get("currency_state_official_context_contract_id") != R3_CONTEXT_CONTRACT_ID:
        raise ResponseTimingV2Error("r3_context_contract_mismatch")
    if dependency.get("fact_basis_eligibility_contract_id") != R3_BASIS_CONTRACT_ID:
        raise ResponseTimingV2Error("r3_basis_contract_mismatch")
    if dependency.get("immutable_event_clock_contract_id") != R3_CLOCK_CONTRACT_ID:
        raise ResponseTimingV2Error("r3_clock_contract_mismatch")
    if adapter_manifest.get("adapter_contract_id") != R3_ADAPTER_CONTRACT_ID:
        raise ResponseTimingV2Error("r3_adapter_manifest_identity_mismatch")
    if context_manifest.get("context_contract_id") != R3_CONTEXT_CONTRACT_ID:
        raise ResponseTimingV2Error("r3_context_manifest_identity_mismatch")
    return {
        "review_certificate_id": R3_CERTIFICATE_ID,
        "review_certificate_sha256": R3_CERTIFICATE_SHA256,
        "official_fact_manifest_sha256": adapter_spec[1],
        "currency_state_manifest_sha256": context_spec[1],
        "verified": True,
    }


def _verify_barred_r1(contract: Mapping[str, Any]) -> dict[str, Any]:
    barred = contract.get("barred_event_response_predecessor")
    if type(barred) is not dict:
        raise ResponseTimingV2Error("barred_v6_r1_dependency_missing")
    contract_spec = _artifact_spec(barred.get("contract"), path="barred_v6_r1.contract")
    manifest_spec = _artifact_spec(barred.get("manifest"), path="barred_v6_r1.manifest")
    predecessor_contract = _strict_json(_read_exact(*contract_spec), label="barred_v6_r1_contract")
    predecessor_manifest = _strict_json(_read_exact(*manifest_spec), label="barred_v6_r1_manifest")
    if predecessor_contract.get("contract_id") != BARRED_V6_R1_CONTRACT_ID:
        raise ResponseTimingV2Error("barred_v6_r1_contract_identity_mismatch")
    if predecessor_contract.get("cohort_id") != BARRED_V6_R1_COHORT_ID:
        raise ResponseTimingV2Error("barred_v6_r1_cohort_identity_mismatch")
    if predecessor_manifest.get("contract_id") != BARRED_V6_R1_CONTRACT_ID:
        raise ResponseTimingV2Error("barred_v6_r1_manifest_identity_mismatch")
    if barred.get("may_supply_timing_rows") is not False or barred.get("may_supply_proof_rows") is not False:
        raise ResponseTimingV2Error("barred_v6_r1_can_supply_rows")
    if predecessor_manifest.get("review_state") == "independent_approved":
        raise ResponseTimingV2Error("barred_v6_r1_unexpectedly_claims_approval")
    return {
        "contract_id": BARRED_V6_R1_CONTRACT_ID,
        "cohort_id": BARRED_V6_R1_COHORT_ID,
        "contract_sha256": contract_spec[1],
        "manifest_sha256": manifest_spec[1],
        "may_supply_rows": False,
    }


def production_dependency_status() -> dict[str, Any]:
    """Verify known bytes and report the deliberately closed V6 R2 gate."""

    try:
        contract = _load_contract()
        r3 = _verify_r3(contract)
        barred = _verify_barred_r1(contract)
    except ResponseTimingV2Error as exc:
        return {
            "contract_id": CONTRACT_ID,
            "approved": False,
            "blocked": True,
            "blocking_reasons": [f"immutable_dependency_verification_failed:{exc}"],
            "r3": None,
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
        # Resolving these immutable artifacts changes the dependency closure and
        # is intentionally forbidden in this R1 timing contract.
        reason = "resolved_v6_r2_requires_new_timing_contract_id"
    return {
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "approved": False,
        "blocked": True,
        "blocking_reasons": [reason],
        "r3": r3,
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


def verify_candidate_manifest() -> dict[str, Any]:
    """Verify the inert candidate closure without treating it as approval."""

    manifest_path = _safe_path("config/currency_state_response_timing_arms_v2_manifest.json")
    manifest = _strict_json(manifest_path.read_bytes(), label="timing_v2_manifest")
    if manifest.get("manifest_id") != "currency_state_response_timing_arms_v2_manifest_20260817_r1":
        raise ResponseTimingV2Error("candidate_manifest_identity_mismatch")
    if manifest.get("contract_id") != CONTRACT_ID:
        raise ResponseTimingV2Error("candidate_manifest_contract_mismatch")
    if manifest.get("review_state") != "pending_independent_review":
        raise ResponseTimingV2Error("candidate_manifest_cannot_self_approve")
    if manifest.get("state") != {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "canonical_output_initialized": False,
    }:
        raise ResponseTimingV2Error("candidate_manifest_not_inert")
    disposition = manifest.get("dependency_disposition")
    if type(disposition) is not dict or disposition.get("production_dependency_gate") != "closed":
        raise ResponseTimingV2Error("candidate_manifest_dependency_gate_not_closed")
    if disposition.get("approved_event_response_v6_r2_available") is not False:
        raise ResponseTimingV2Error("candidate_manifest_invented_v6_r2_approval")
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
        raise ResponseTimingV2Error("candidate_manifest_runtime_or_canonical_surface")
    expected_paths = {
        "runtime_core": "src/forex_system/research/currency_state_response_timing_arms_v2.py",
        "contract": "config/currency_state_response_timing_arms_v2.json",
        "adversarial_tests": "test_currency_state_response_timing_arms_v2.py",
        "parent_v1_module": "src/forex_system/research/currency_state_response_timing_arms.py",
        "parent_v1_contract": "config/currency_state_response_timing_arms_v1.json",
        "approved_r3_review_certificate": "config/official_fact_currency_state_v5_r3_review_certificate.json",
        "official_fact_v5_r3_manifest": "config/official_fact_adapter_v5_manifest.json",
        "currency_state_context_v5_r3_manifest": "config/currency_state_official_context_v5_manifest.json",
        "barred_event_response_v6_r1_contract": "config/prospective_event_response_v6_contract.json",
        "barred_event_response_v6_r1_manifest": "config/prospective_event_response_v6_manifest.json",
    }
    artifacts = manifest.get("artifacts")
    if type(artifacts) is not dict or set(artifacts) != set(expected_paths):
        raise ResponseTimingV2Error("candidate_manifest_artifact_set_mismatch")
    for name, expected_path in expected_paths.items():
        relative_path, sha256, byte_length = _artifact_spec(
            artifacts[name], path=f"candidate_manifest.{name}"
        )
        if relative_path != expected_path:
            raise ResponseTimingV2Error(f"candidate_manifest_artifact_path_mismatch:{name}")
        _read_exact(relative_path, sha256, byte_length)
    return {
        "manifest_id": manifest["manifest_id"],
        "review_state": manifest["review_state"],
        "artifact_count": len(artifacts),
        "dependency_gate": "closed",
        "candidate_closure_verified": True,
    }


def require_production_dependencies() -> None:
    status = production_dependency_status()
    if status.get("approved") is not True:
        reasons = ",".join(status.get("blocking_reasons") or ["dependency_not_approved"])
        raise ResponseTimingV2DependencyError(reasons)


def build_replay_diagnostic(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Canonical replay is unavailable until a new approved dependency is frozen."""

    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable: timing V2 R1 has no canonical builder")


def build_prospective_proof(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Prospective proof is unavailable and cannot be caller-enabled."""

    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable: timing V2 R1 has no proof builder")


_ATTESTATION_FIELDS = {
    "contract_id",
    "cohort_id",
    "contract_sha256",
    "manifest_sha256",
    "review_certificate_id",
    "review_certificate_sha256",
    "review_disposition",
    "artifact_closure_verified",
    "test_fixture_only",
}


def _validate_test_attestation(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ATTESTATION_FIELDS:
        raise ResponseTimingV2Error("test_attestation_closed_schema_mismatch")
    if value["contract_id"] != REQUIRED_V6_R2_CONTRACT_ID:
        raise ResponseTimingV2Error("test_attestation_contract_id_mismatch")
    if value["cohort_id"] != REQUIRED_V6_R2_COHORT_ID:
        raise ResponseTimingV2Error("test_attestation_cohort_id_mismatch")
    for field in ("contract_sha256", "manifest_sha256", "review_certificate_sha256"):
        if not _is_sha256(value[field]):
            raise ResponseTimingV2Error(f"test_attestation_{field}_invalid")
    if type(value["review_certificate_id"]) is not str or not value["review_certificate_id"]:
        raise ResponseTimingV2Error("test_attestation_review_certificate_id_invalid")
    if value["review_disposition"] != "independent_approve_disabled_unregistered_candidate":
        raise ResponseTimingV2Error("test_attestation_not_approved")
    if value["artifact_closure_verified"] is not True or value["test_fixture_only"] is not True:
        raise ResponseTimingV2Error("test_attestation_guards_invalid")
    return copy.deepcopy(value)


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


def _response_identity(
    row: Mapping[str, Any], *, source_binding_sha256: str, mode: str
) -> str:
    material = {
        "contract_id": REQUIRED_V6_R2_CONTRACT_ID,
        "cohort_id": REQUIRED_V6_R2_COHORT_ID,
        "mode": mode,
        "source_binding_sha256": source_binding_sha256,
        "event_id": row["event_id"],
        "event_version_id": row["event_version_id"],
        "currency": row["currency"],
        "instrument": row["instrument"],
        "horizon_sec": row["horizon_sec"],
        "event_scheduled_utc": row["event_scheduled_utc"],
        "start_target_utc": row["start_target_utc"],
        "end_target_utc": row["end_target_utc"],
        "start_quote_time_utc": row["start_quote_time_utc"],
        "end_quote_time_utc": row["end_quote_time_utc"],
        "start_known_utc": row["start_known_utc"],
        "end_known_utc": row["end_known_utc"],
        "start_bid": row["start_bid"],
        "start_ask": row["start_ask"],
        "end_bid": row["end_bid"],
        "end_ask": row["end_ask"],
        "pip": row["pip"],
        "start_source_payload_sha256": row["start_source_payload_sha256"],
        "end_source_payload_sha256": row["end_source_payload_sha256"],
        "start_source_record_id": row["start_source_record_id"],
        "end_source_record_id": row["end_source_record_id"],
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
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _RESPONSE_FIELDS:
        raise ResponseTimingV2Error("response_closed_schema_mismatch")
    row = copy.deepcopy(value)
    for field, expected in _guard().items():
        if row.get(field) != expected or type(row.get(field)) is not type(expected):
            raise ResponseTimingV2Error(f"unsafe_response_guard:{field}")
    if row.get("partition") != partition:
        raise ResponseTimingV2Error("response_partition_mismatch")
    for field in (
        "direction_selected",
        "forecast_mean_pips",
        "forecast_probability",
        "expected_value_pips",
        "allocator_rank",
    ):
        if row.get(field) is not None:
            raise ResponseTimingV2Error(f"response_invented_forecast_field:{field}")
    if row.get("basis_eligible") is not False or row.get("proof_eligible") is not False:
        raise ResponseTimingV2Error("response_invented_basis_or_proof")
    if type(row.get("instrument")) is not str or row["instrument"].count("_") != 1:
        raise ResponseTimingV2Error("response_instrument_invalid")
    base, quote = row["instrument"].split("_")
    if row.get("currency") not in {base, quote}:
        raise ResponseTimingV2Error("response_currency_not_pair_leg")
    horizon = row.get("horizon_sec")
    if type(horizon) is not int or horizon not in HORIZONS_SEC:
        raise ResponseTimingV2Error("response_horizon_invalid")
    event_time = _utc(row["event_scheduled_utc"], path="response.event_scheduled_utc")
    start_target = _utc(row["start_target_utc"], path="response.start_target_utc")
    end_target = _utc(row["end_target_utc"], path="response.end_target_utc")
    start_quote = _utc(row["start_quote_time_utc"], path="response.start_quote_time_utc")
    end_quote = _utc(row["end_quote_time_utc"], path="response.end_quote_time_utc")
    start_known = _utc(row["start_known_utc"], path="response.start_known_utc")
    end_known = _utc(row["end_known_utc"], path="response.end_known_utc")
    if start_target != event_time or end_target != event_time + dt.timedelta(seconds=horizon):
        raise ResponseTimingV2Error("response_target_chronology_invalid")
    start_alignment = (start_quote - start_target).total_seconds()
    end_alignment = (end_quote - end_target).total_seconds()
    if not 0 <= start_alignment <= float(response_contract["maximum_start_alignment_sec"]):
        raise ResponseTimingV2Error("response_start_alignment_invalid")
    if not 0 <= end_alignment <= float(response_contract["maximum_end_alignment_sec"]):
        raise ResponseTimingV2Error("response_end_alignment_invalid")
    maximum_known_lag = float(response_contract["maximum_quote_known_lag_sec"])
    for quote_time, known_time, label in (
        (start_quote, start_known, "start"),
        (end_quote, end_known, "end"),
    ):
        lag = (known_time - quote_time).total_seconds()
        if lag < 0 or lag > maximum_known_lag or known_time > capture_cutoff:
            raise ResponseTimingV2Error(f"response_{label}_knowledge_clock_invalid")
    if start_quote > end_quote or start_known > end_known:
        raise ResponseTimingV2Error("response_clock_order_invalid")
    start_bid = _number(row["start_bid"], path="response.start_bid", positive=True)
    start_ask = _number(row["start_ask"], path="response.start_ask", positive=True)
    end_bid = _number(row["end_bid"], path="response.end_bid", positive=True)
    end_ask = _number(row["end_ask"], path="response.end_ask", positive=True)
    pip = _number(row["pip"], path="response.pip", positive=True)
    if start_ask <= start_bid or end_ask <= end_bid:
        raise ResponseTimingV2Error("response_bid_ask_invalid")
    for field in ("start_source_payload_sha256", "end_source_payload_sha256"):
        if not _is_sha256(row.get(field)):
            raise ResponseTimingV2Error(f"response_{field}_invalid")
    for field in ("response_id", "event_id", "event_version_id", "start_source_record_id", "end_source_record_id"):
        if type(row.get(field)) is not str or not row[field]:
            raise ResponseTimingV2Error(f"response_{field}_invalid")
    expected_id = _response_identity(
        row, source_binding_sha256=source_binding_sha256, mode=mode
    )
    if row["response_id"] != expected_id:
        raise ResponseTimingV2Error("response_identity_mismatch")
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
        {
            **common,
            "arm_id": "no_trade",
            "control_state": "no_trade",
        },
    ]


def build_test_diagnostic_snapshot(
    *, event_response_snapshot: Mapping[str, Any], dependency_attestation: Mapping[str, Any]
) -> dict[str, Any]:
    """Build a deterministic test-only timing diagnostic from synthetic V6 R2 rows.

    This function cannot produce proof or canonical output.  The caller cannot
    change that policy through either payload or attestation.
    """

    contract = _load_contract()
    attestation = _validate_test_attestation(dependency_attestation)
    if type(event_response_snapshot) is not dict or set(event_response_snapshot) != _SOURCE_FIELDS:
        raise ResponseTimingV2Error("event_response_snapshot_closed_schema_mismatch")
    source = copy.deepcopy(event_response_snapshot)
    if source.get("contract_id") != REQUIRED_V6_R2_CONTRACT_ID:
        raise ResponseTimingV2Error("event_response_snapshot_contract_mismatch")
    if source.get("cohort_id") != REQUIRED_V6_R2_COHORT_ID:
        raise ResponseTimingV2Error("event_response_snapshot_cohort_mismatch")
    mode = source.get("mode")
    if mode not in DIAGNOSTIC_MODES:
        raise ResponseTimingV2Error("event_response_snapshot_mode_not_test_diagnostic")
    partition = DIAGNOSTIC_MODES[mode]
    if source.get("partition") != partition:
        raise ResponseTimingV2Error("event_response_snapshot_partition_mismatch")
    for field, expected in _guard().items():
        if source.get(field) != expected or type(source.get(field)) is not type(expected):
            raise ResponseTimingV2Error(f"unsafe_source_guard:{field}")
    if source.get("test_only") is not True or source.get("canonical_output") is not False:
        raise ResponseTimingV2Error("event_response_snapshot_not_test_only")
    if source.get("proof_row_count") != 0 or type(source.get("proof_row_count")) is not int:
        raise ResponseTimingV2Error("event_response_snapshot_proof_rows_forbidden")
    if not _is_sha256(source.get("source_binding_sha256")):
        raise ResponseTimingV2Error("event_response_source_binding_invalid")
    if type(source.get("snapshot_id")) is not str or not source["snapshot_id"]:
        raise ResponseTimingV2Error("event_response_snapshot_id_invalid")
    capture_cutoff = _utc(source.get("capture_cutoff_utc"), path="capture_cutoff_utc")
    raw_responses = source.get("response_diagnostics")
    if type(raw_responses) not in {list, tuple}:
        raise ResponseTimingV2Error("response_diagnostics_must_be_sequence")
    validated: list[dict[str, Any]] = []
    seen_natural: set[tuple[str, str, int]] = set()
    seen_ids: set[str] = set()
    response_contract = contract["response_contract"]
    for value in raw_responses:
        row = _validated_response(
            value,
            mode=mode,
            partition=partition,
            capture_cutoff=capture_cutoff,
            source_binding_sha256=source["source_binding_sha256"],
            response_contract=response_contract,
        )
        natural = (row["event_version_id"], row["instrument"], row["horizon_sec"])
        if natural in seen_natural:
            raise ResponseTimingV2Error("duplicate_response_natural_cell")
        if row["response_id"] in seen_ids:
            raise ResponseTimingV2Error("duplicate_response_id")
        seen_natural.add(natural)
        seen_ids.add(row["response_id"])
        validated.append(row)
    validated.sort(
        key=lambda row: (
            row["event_scheduled_utc"],
            row["event_version_id"],
            row["instrument"],
            row["horizon_sec"],
        )
    )
    arm_rows: list[dict[str, Any]] = []
    for row in validated:
        arm_rows.extend(_arms(row))
    arm_rows.sort(
        key=lambda row: (
            row["event_version_id"], row["instrument"], row["horizon_sec"], ARM_IDS.index(row["arm_id"])
        )
    )
    result: dict[str, Any] = {
        "schema_version": 2,
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "snapshot_id": None,
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "research_generation": RESEARCH_GENERATION,
        "mode": mode,
        "partition": partition,
        "test_only": True,
        "canonical_output": False,
        "source_snapshot_id": source["snapshot_id"],
        "source_contract_id": source["contract_id"],
        "source_cohort_id": source["cohort_id"],
        "source_binding_sha256": source["source_binding_sha256"],
        "dependency_attestation": attestation,
        "capture_cutoff_utc": _iso(capture_cutoff),
        "response_count": len(validated),
        "timing_arm_count": len(arm_rows),
        "proof_row_count": 0,
        "responses": validated,
        "timing_arms": arm_rows,
        "limitations": [
            "test_fixture_only_noncanonical",
            "replay_and_prospective_capture_are_separate_diagnostic_partitions",
            "all_response_direction_is_post_outcome_and_not_a_forecast",
            "diagnostics_are_not_prospective_proof",
            "production_gate_remains_closed_until_a_new_timing_contract_binds_approved_v6_r2_bytes",
        ],
        **_guard(),
    }
    material = {key: value for key, value in result.items() if key != "snapshot_id"}
    result["snapshot_id"] = "currency_state_response_timing_v2_test_" + stable_hash(material)[:32]
    validate_test_diagnostic_snapshot(result)
    return result


def validate_test_diagnostic_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("snapshot_schema") != SNAPSHOT_SCHEMA or snapshot.get("contract_id") != CONTRACT_ID:
        raise ResponseTimingV2Error("timing_snapshot_identity_mismatch")
    if snapshot.get("mode") not in DIAGNOSTIC_MODES:
        raise ResponseTimingV2Error("timing_snapshot_mode_invalid")
    if snapshot.get("partition") != DIAGNOSTIC_MODES[snapshot["mode"]]:
        raise ResponseTimingV2Error("timing_snapshot_partition_invalid")
    if snapshot.get("test_only") is not True or snapshot.get("canonical_output") is not False:
        raise ResponseTimingV2Error("timing_snapshot_not_test_only")
    if snapshot.get("proof_row_count") != 0 or type(snapshot.get("proof_row_count")) is not int:
        raise ResponseTimingV2Error("timing_snapshot_proof_rows_forbidden")
    for field, expected in _guard().items():
        if snapshot.get(field) != expected or type(snapshot.get(field)) is not type(expected):
            raise ResponseTimingV2Error(f"unsafe_timing_snapshot_guard:{field}")
    responses = snapshot.get("responses")
    arms = snapshot.get("timing_arms")
    if type(responses) is not list or type(arms) is not list:
        raise ResponseTimingV2Error("timing_snapshot_rows_invalid")
    if snapshot.get("response_count") != len(responses):
        raise ResponseTimingV2Error("timing_snapshot_response_count_mismatch")
    if snapshot.get("timing_arm_count") != len(arms) or len(arms) != len(responses) * len(ARM_IDS):
        raise ResponseTimingV2Error("timing_snapshot_arm_count_mismatch")
    for row in arms:
        if row.get("arm_id") not in ARM_IDS or row.get("post_outcome_only") is not True:
            raise ResponseTimingV2Error("timing_arm_identity_invalid")
        if row.get("forecast_direction") is not None or row.get("forecast_probability") is not None:
            raise ResponseTimingV2Error("timing_arm_invented_forecast")
        if row.get("expected_value_pips") is not None or row.get("allocator_rank") is not None:
            raise ResponseTimingV2Error("timing_arm_invented_ev_or_rank")
        if row.get("basis_eligible") is not False or row.get("proof_eligible") is not False:
            raise ResponseTimingV2Error("timing_arm_invented_basis_or_proof")
        for field, expected in _guard().items():
            if row.get(field) != expected or type(row.get(field)) is not type(expected):
                raise ResponseTimingV2Error(f"unsafe_timing_arm_guard:{field}")


__all__ = [
    "ARM_IDS",
    "CONTRACT_ID",
    "CONTRACT_SHA256",
    "DIAGNOSTIC_MODES",
    "REQUIRED_V6_R2_COHORT_ID",
    "REQUIRED_V6_R2_CONTRACT_ID",
    "ResponseTimingV2DependencyError",
    "ResponseTimingV2Error",
    "build_prospective_proof",
    "build_replay_diagnostic",
    "build_test_diagnostic_snapshot",
    "production_dependency_status",
    "require_production_dependencies",
    "validate_test_diagnostic_snapshot",
    "verify_candidate_manifest",
]
