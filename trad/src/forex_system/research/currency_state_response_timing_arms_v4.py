"""Disabled, closed-schema response/timing research successor V4.

V4 is intentionally test-fixture-only.  It repairs V3's economic-unit,
timestamp-ordering, scalar-schema, and manifest-closure defects while keeping
every production/proof entrypoint closed.  A separately approved event-response
dependency must be bound by a later timing contract; this module cannot adopt
one by configuration or relabelling.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping


UTC = dt.timezone.utc
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_ID = "currency_state_response_timing_arms_v4_20260817_r1"
SNAPSHOT_SCHEMA = "currency_state_response_timing_diagnostic_snapshot_v4"
RESEARCH_GENERATION = "r3_bound_response_timing_v4_r1"
CONTRACT_SHA256 = "33fbbb95a5ee29949ef4fd07b0b39586c1cec19e4467aaccde64c8c2803e472b"
CONTRACT_BYTES = 7_238
SUPPORTED_EXECUTION_DECISION = "no_trade"

R3_CERTIFICATE_ID = "official_fact_currency_state_v5_r3_independent_review_20260817"
R3_CERTIFICATE_SHA256 = "0a61948a9fc2f2a9a06d30557b838d7fb389958c9ef2b1845bc8b06e70c7b299"
R3_CERTIFICATE_BYTES = 6_003
R3_REVIEWED_ARTIFACT_COUNT = 23
R3_ADAPTER_CONTRACT_ID = "official_fact_adapter_v5_20260817_r3"
R3_ADAPTER_MANIFEST_ID = "official_fact_adapter_v5_manifest_20260817_r3"
R3_CONTEXT_CONTRACT_ID = "currency_state_official_context_v5_20260817_r3"
R3_CONTEXT_MANIFEST_ID = "currency_state_official_context_v5_manifest_20260817_r3"
R3_BASIS_CONTRACT_ID = "official_fact_basis_eligibility_v5_20260817_r3"
R3_CLOCK_CONTRACT_ID = "immutable_point_in_time_event_clock_v2_20260817"

UNIVERSE_CONTRACT_ID = "currency_state_engine_v2_20260816"
UNIVERSE_CONTRACT_SHA256 = "20138fe4328ee88b47b8edce2b3bd6081191cda35dd4124a9a457ea9b2efa400"
UNIVERSE_CONTRACT_BYTES = 3_272
INSTRUMENT_UNIVERSE_SHA256 = "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
CANONICAL_PIP_MAPPING_SHA256 = "46532e9598c7fab24c57935bb4bb4d747901d68acc9f3d50a28bd1957ce19f2c"

REQUIRED_SUCCESSOR_CONTRACT_ID = "prospective_event_response_capture_v6_20260817_r4"
REQUIRED_SUCCESSOR_COHORT_ID = "prospective_official_event_response_v6_20260817_r4"
DIAGNOSTIC_MODES = {
    "replay_diagnostic_test_fixture": "diagnostic_replay",
    "prospective_capture_test_fixture": "diagnostic_prospective_capture",
}
HORIZONS_SEC = (60, 300, 900, 1800, 3600)
ARM_IDS = ("observed_response", "cost_clearance", "response_latency", "no_trade")
LIMITATIONS = [
    "test_fixture_only_noncanonical",
    "all_response_direction_is_post_outcome_and_not_a_forecast",
    "diagnostics_are_not_prospective_proof",
    "approved_event_response_dependency_requires_a_new_timing_contract",
]


class ResponseTimingV4Error(ValueError):
    """Closed-schema, chronology, identity, or dependency failure."""


class ResponseTimingV4DependencyError(ResponseTimingV4Error):
    """Raised by every unavailable canonical/proof entrypoint."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _strict_json(payload: bytes, *, label: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ResponseTimingV4Error(f"{label}_duplicate_json_key:{key}")
            result[key] = value
        return result

    def invalid(value: str) -> None:
        raise ResponseTimingV4Error(f"{label}_nonfinite_json:{value}")

    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResponseTimingV4Error(f"{label}_invalid_json") from exc


def _safe_path(relative_path: str) -> Path:
    if (
        type(relative_path) is not str
        or not relative_path
        or "\\" in relative_path
        or Path(relative_path).is_absolute()
        or any(part in {"", ".", ".."} for part in relative_path.split("/"))
    ):
        raise ResponseTimingV4Error("artifact_path_invalid")
    try:
        root = PROJECT_ROOT.resolve(strict=True)
        candidate = PROJECT_ROOT.joinpath(*relative_path.split("/"))
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
        cursor = root
        for part in relative_path.split("/"):
            cursor /= part
            if cursor.is_symlink():
                raise ResponseTimingV4Error(f"artifact_symlink_forbidden:{relative_path}")
    except ResponseTimingV4Error:
        raise
    except (OSError, ValueError) as exc:
        raise ResponseTimingV4Error(f"artifact_path_escape_or_missing:{relative_path}") from exc
    return candidate


def _stable_read(relative_path: str) -> bytes:
    path = _safe_path(relative_path)
    before = path.stat()
    payload = path.read_bytes()
    after = path.stat()
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
    if identity(before) != identity(after):
        raise ResponseTimingV4Error(f"artifact_identity_changed:{relative_path}")
    if after.st_nlink != 1:
        raise ResponseTimingV4Error(f"artifact_hardlink_forbidden:{relative_path}")
    return payload


def _read_exact(relative_path: str, sha256: str, byte_length: int) -> bytes:
    payload = _stable_read(relative_path)
    if len(payload) != byte_length or _sha256_bytes(payload) != sha256:
        raise ResponseTimingV4Error(f"artifact_content_mismatch:{relative_path}")
    return payload


def _is_sha256(value: Any) -> bool:
    return type(value) is str and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _artifact_spec(value: Any, *, path: str) -> tuple[str, str, int]:
    if type(value) is not dict or set(value) != {"relative_path", "sha256", "byte_length"}:
        raise ResponseTimingV4Error(f"{path}_closed_schema_mismatch")
    relative, sha256, byte_length = value["relative_path"], value["sha256"], value["byte_length"]
    if type(relative) is not str or not relative or not _is_sha256(sha256):
        raise ResponseTimingV4Error(f"{path}_identity_invalid")
    if type(byte_length) is not int or byte_length <= 0:
        raise ResponseTimingV4Error(f"{path}_byte_length_invalid")
    return relative, sha256, byte_length


def _utc(value: Any, *, path: str) -> dt.datetime:
    if type(value) is not str or not value:
        raise ResponseTimingV4Error(f"{path}_must_be_exact_str")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ResponseTimingV4Error(f"{path}_invalid_utc") from exc
    if type(parsed) is not dt.datetime or parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ResponseTimingV4Error(f"{path}_must_have_zero_utc_offset")
    canonical = parsed.astimezone(UTC)
    if value != _iso(canonical):
        raise ResponseTimingV4Error(f"{path}_must_be_canonical_utc")
    return canonical


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _exact_float(value: Any, *, path: str, positive: bool = False) -> float:
    if type(value) is not float or not math.isfinite(value) or (positive and value <= 0.0):
        raise ResponseTimingV4Error(f"{path}_must_be_exact_finite_float")
    return value


def _guard() -> dict[str, Any]:
    return {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }


_STATE = {
    "enabled": False,
    "registered": False,
    "research_worker_enabled": False,
    "collector_enabled": False,
    "canonical_output_initialized": False,
    "independent_review_state": "pending",
}
_OUTPUT_SURFACE = {
    "canonical_ledger_path": None,
    "canonical_state_path": None,
    "canonical_report_path": None,
    "worker_entrypoint": None,
    "collector_entrypoint": None,
    "test_fixture_builder": "forex_system.research.currency_state_response_timing_arms_v4.build_test_diagnostic_snapshot",
}
_POLICY = {
    "event_response_dependency_is_unresolved": True,
    "dependency_resolution_requires_new_timing_contract": True,
    "diagnostic_capture_may_not_be_reclassified_as_proof": True,
    "unresolved_dependency_must_produce_zero_rows": True,
    "observed_response_is_post_outcome_not_a_forecast": True,
    **_guard(),
}


def _load_contract() -> dict[str, Any]:
    contract = _strict_json(
        _read_exact("config/currency_state_response_timing_arms_v4.json", CONTRACT_SHA256, CONTRACT_BYTES),
        label="timing_v4_contract",
    )
    if type(contract) is not dict or contract.get("schema_version") != 4 or type(contract.get("schema_version")) is not int:
        raise ResponseTimingV4Error("timing_contract_schema_mismatch")
    if contract.get("contract_id") != CONTRACT_ID or contract.get("snapshot_schema") != SNAPSHOT_SCHEMA:
        raise ResponseTimingV4Error("timing_contract_identity_mismatch")
    if contract.get("state") != _STATE or contract.get("output_surface") != _OUTPUT_SURFACE or contract.get("policy") != _POLICY:
        raise ResponseTimingV4Error("timing_contract_not_exactly_inert")
    return contract


def _verify_r3(contract: Mapping[str, Any]) -> dict[str, Any]:
    dependency = contract.get("approved_r3_dependency")
    if type(dependency) is not dict:
        raise ResponseTimingV4Error("r3_dependency_missing")
    cert_spec = _artifact_spec(dependency.get("review_certificate"), path="r3.certificate")
    if cert_spec != (
        "config/official_fact_currency_state_v5_r3_review_certificate.json",
        R3_CERTIFICATE_SHA256,
        R3_CERTIFICATE_BYTES,
    ):
        raise ResponseTimingV4Error("r3_certificate_spec_mismatch")
    certificate = _strict_json(_read_exact(*cert_spec), label="r3_certificate")
    if certificate.get("review_certificate_id") != R3_CERTIFICATE_ID:
        raise ResponseTimingV4Error("r3_certificate_id_mismatch")
    if certificate.get("review_disposition") != "independent_approve_disabled_unregistered_candidate":
        raise ResponseTimingV4Error("r3_certificate_not_approved")
    results = certificate.get("review_results")
    if type(results) is not dict or any(type(results.get(key)) is not int or results[key] != 0 for key in ("p0_findings", "p1_findings", "p2_findings")):
        raise ResponseTimingV4Error("r3_certificate_has_material_findings")
    if certificate.get("reviewed_contracts") != {
        "official_fact_adapter_contract_id": R3_ADAPTER_CONTRACT_ID,
        "currency_state_official_context_contract_id": R3_CONTEXT_CONTRACT_ID,
        "fact_basis_eligibility_contract_id": R3_BASIS_CONTRACT_ID,
        "immutable_event_clock_contract_id": R3_CLOCK_CONTRACT_ID,
    }:
        raise ResponseTimingV4Error("r3_contract_set_mismatch")
    reviewed = certificate.get("reviewed_artifacts")
    if type(reviewed) is not list or len(reviewed) != R3_REVIEWED_ARTIFACT_COUNT:
        raise ResponseTimingV4Error("r3_reviewed_artifact_count_mismatch")
    reviewed_map: dict[str, tuple[str, str, int]] = {}
    for index, value in enumerate(reviewed):
        spec = _artifact_spec(value, path=f"r3.reviewed[{index}]")
        if spec[0] in reviewed_map:
            raise ResponseTimingV4Error("r3_duplicate_reviewed_path")
        _read_exact(*spec)
        reviewed_map[spec[0]] = spec
    for name, manifest_id, contract_key, contract_id in (
        ("official_fact_manifest", R3_ADAPTER_MANIFEST_ID, "adapter_contract_id", R3_ADAPTER_CONTRACT_ID),
        ("currency_state_manifest", R3_CONTEXT_MANIFEST_ID, "context_contract_id", R3_CONTEXT_CONTRACT_ID),
    ):
        spec = _artifact_spec(dependency.get(name), path=f"r3.{name}")
        if reviewed_map.get(spec[0]) != spec:
            raise ResponseTimingV4Error(f"r3_{name}_not_reviewed")
        manifest = _strict_json(_read_exact(*spec), label=f"r3_{name}")
        if manifest.get("manifest_id") != manifest_id or manifest.get(contract_key) != contract_id:
            raise ResponseTimingV4Error(f"r3_{name}_identity_mismatch")
        artifacts = manifest.get("artifacts")
        if type(artifacts) is not dict or not artifacts:
            raise ResponseTimingV4Error(f"r3_{name}_artifacts_missing")
        for artifact in artifacts.values():
            nested = _artifact_spec(artifact, path=f"r3.{name}.artifact")
            if reviewed_map.get(nested[0]) != nested:
                raise ResponseTimingV4Error(f"r3_{name}_artifact_not_reviewed")
    return {"review_certificate_id": R3_CERTIFICATE_ID, "reviewed_artifact_count": len(reviewed_map), "transitive_closure_verified": True}


def _canonical_pip(instrument: str, instruments: tuple[str, ...]) -> float:
    if type(instrument) is not str or instrument not in instruments:
        raise ResponseTimingV4Error("unknown_instrument_for_canonical_pip")
    return 0.01 if instrument.split("_")[1] == "JPY" else 0.0001


def _load_universe(contract: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    value = contract.get("canonical_universe")
    if type(value) is not dict:
        raise ResponseTimingV4Error("canonical_universe_missing")
    spec = _artifact_spec(value.get("contract"), path="canonical_universe.contract")
    if spec != ("config/currency_state_engine_v2.json", UNIVERSE_CONTRACT_SHA256, UNIVERSE_CONTRACT_BYTES):
        raise ResponseTimingV4Error("canonical_universe_spec_mismatch")
    payload = _strict_json(_read_exact(*spec), label="canonical_universe")
    currencies, instruments = payload.get("currencies"), payload.get("instruments")
    if (
        payload.get("contract_id") != UNIVERSE_CONTRACT_ID
        or type(currencies) is not list
        or type(instruments) is not list
        or len(currencies) != 21
        or len(instruments) != 68
        or len(set(currencies)) != 21
        or len(set(instruments)) != 68
        or any(type(item) is not str for item in currencies + instruments)
    ):
        raise ResponseTimingV4Error("canonical_universe_shape_mismatch")
    currency_tuple, instrument_tuple = tuple(currencies), tuple(instruments)
    if _sha256_json(instruments) != INSTRUMENT_UNIVERSE_SHA256:
        raise ResponseTimingV4Error("canonical_instrument_universe_hash_mismatch")
    mapping = {instrument: _canonical_pip(instrument, instrument_tuple) for instrument in instrument_tuple}
    if _sha256_json(mapping) != CANONICAL_PIP_MAPPING_SHA256:
        raise ResponseTimingV4Error("canonical_pip_mapping_hash_mismatch")
    if value.get("canonical_pip_mapping_sha256") != CANONICAL_PIP_MAPPING_SHA256:
        raise ResponseTimingV4Error("canonical_pip_contract_hash_mismatch")
    return currency_tuple, instrument_tuple


def _verify_barred_lineage(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = contract.get("barred_event_response_lineage")
    expected = (
        ("prospective_event_response_capture_v6_20260817_r1", "prospective_official_event_response_v6_20260817_r1"),
        ("prospective_event_response_capture_v6_20260817_r2", "prospective_official_event_response_v6_20260817_r2"),
        ("prospective_event_response_capture_v6_20260817_r3", "prospective_official_event_response_v6_20260817_r3"),
    )
    if type(rows) is not list or len(rows) != len(expected):
        raise ResponseTimingV4Error("barred_lineage_shape_mismatch")
    result = []
    for index, ((contract_id, cohort_id), row) in enumerate(zip(expected, rows)):
        if type(row) is not dict or set(row) != {"contract_id", "cohort_id", "contract", "manifest", "may_supply_rows"}:
            raise ResponseTimingV4Error("barred_lineage_closed_schema_mismatch")
        contract_spec = _artifact_spec(row["contract"], path=f"barred[{index}].contract")
        manifest_spec = _artifact_spec(row["manifest"], path=f"barred[{index}].manifest")
        contract_payload = _strict_json(_read_exact(*contract_spec), label=f"barred_contract_{index}")
        manifest_payload = _strict_json(_read_exact(*manifest_spec), label=f"barred_manifest_{index}")
        if row["contract_id"] != contract_id or row["cohort_id"] != cohort_id or row["may_supply_rows"] is not False:
            raise ResponseTimingV4Error("barred_lineage_identity_mismatch")
        if contract_payload.get("contract_id") != contract_id or contract_payload.get("cohort_id") != cohort_id:
            raise ResponseTimingV4Error("barred_contract_identity_mismatch")
        if manifest_payload.get("contract_id") != contract_id or manifest_payload.get("cohort_id") != cohort_id:
            raise ResponseTimingV4Error("barred_manifest_identity_mismatch")
        result.append({"contract_id": contract_id, "cohort_id": cohort_id, "may_supply_rows": False})
    return result


def production_dependency_status() -> dict[str, Any]:
    try:
        contract = _load_contract()
        r3 = _verify_r3(contract)
        currencies, instruments = _load_universe(contract)
        barred = _verify_barred_lineage(contract)
    except ResponseTimingV4Error as exc:
        return {"contract_id": CONTRACT_ID, "approved": False, "blocked": True, "blocking_reasons": [f"immutable_dependency_verification_failed:{exc}"], "row_count": 0, "proof_row_count": 0, **_guard()}
    successor = contract.get("required_event_response_successor")
    if type(successor) is not dict or successor.get("required_contract_id") != REQUIRED_SUCCESSOR_CONTRACT_ID or successor.get("required_cohort_id") != REQUIRED_SUCCESSOR_COHORT_ID:
        reason = "event_response_successor_identity_not_bound"
    elif any(successor.get(field) is not None for field in ("contract_artifact", "manifest_artifact", "independent_review_certificate_artifact")):
        reason = "resolved_successor_requires_new_timing_contract_id"
    else:
        reason = "event_response_v6_r4_final_approval_not_bound"
    return {
        "contract_id": CONTRACT_ID,
        "contract_sha256": CONTRACT_SHA256,
        "approved": False,
        "blocked": True,
        "blocking_reasons": [reason],
        "r3": r3,
        "canonical_universe": {"currency_count": len(currencies), "instrument_count": len(instruments), "instrument_universe_sha256": INSTRUMENT_UNIVERSE_SHA256, "canonical_pip_mapping_sha256": CANONICAL_PIP_MAPPING_SHA256},
        "barred_predecessors": barred,
        "required_event_response": {"contract_id": REQUIRED_SUCCESSOR_CONTRACT_ID, "cohort_id": REQUIRED_SUCCESSOR_COHORT_ID, "approval_state": successor.get("approval_state") if type(successor) is dict else None},
        "row_count": 0,
        "proof_row_count": 0,
        **_guard(),
    }


def require_production_dependencies() -> None:
    status = production_dependency_status()
    if status.get("approved") is not True:
        raise ResponseTimingV4DependencyError(",".join(status.get("blocking_reasons") or ["dependency_not_approved"]))


def build_replay_diagnostic(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


def build_prospective_proof(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    require_production_dependencies()
    raise AssertionError("unreachable")


_ATTESTATION_FIELDS = {"contract_id", "cohort_id", "source_snapshot_id", "source_binding_sha256", "test_fixture_only", "approval_state"}
_SOURCE_FIELDS = {"snapshot_id", "contract_id", "cohort_id", "mode", "partition", "capture_cutoff_utc", "source_binding_sha256", "response_diagnostics", "proof_row_count", "test_only", "canonical_output", *_guard().keys()}
_RESPONSE_FIELDS = {
    "response_id", "partition", "event_id", "event_version_id", "currency", "instrument", "horizon_sec",
    "event_scheduled_utc", "start_target_utc", "end_target_utc", "start_quote_time_utc", "end_quote_time_utc",
    "start_known_utc", "end_known_utc", "start_bid", "start_ask", "end_bid", "end_ask", "pip",
    "start_source_payload_sha256", "end_source_payload_sha256", "start_source_record_id", "end_source_record_id",
    "direction_selected", "forecast_mean_pips", "forecast_probability", "expected_value_pips", "allocator_rank",
    "basis_eligible", "proof_eligible", *_guard().keys(),
}
_DERIVED_FIELDS = {"start_alignment_sec", "end_alignment_sec", "actual_quote_duration_sec", "start_spread_pips", "end_spread_pips", "gross_long_mid_pips", "gross_short_mid_pips", "long_after_executable_spread_pips", "short_after_executable_spread_pips", "long_execution_cost_pips", "short_execution_cost_pips"}
_RESULT_FIELDS = {"schema_version", "snapshot_schema", "snapshot_id", "contract_id", "contract_sha256", "research_generation", "mode", "partition", "test_only", "canonical_output", "source_snapshot_id", "source_contract_id", "source_cohort_id", "source_binding_sha256", "dependency_attestation", "capture_cutoff_utc", "response_count", "timing_arm_count", "proof_row_count", "responses", "timing_arms", "limitations", *_guard().keys()}


def _validate_attestation(value: Any, *, source_snapshot_id: str, source_binding: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ATTESTATION_FIELDS:
        raise ResponseTimingV4Error("attestation_closed_schema_mismatch")
    expected = {
        "contract_id": REQUIRED_SUCCESSOR_CONTRACT_ID,
        "cohort_id": REQUIRED_SUCCESSOR_COHORT_ID,
        "source_snapshot_id": source_snapshot_id,
        "source_binding_sha256": source_binding,
        "test_fixture_only": True,
        "approval_state": "synthetic_fixture_not_approval",
    }
    for key, item in expected.items():
        if type(value.get(key)) is not type(item) or value.get(key) != item:
            raise ResponseTimingV4Error(f"attestation_{key}_mismatch")
    if not _is_sha256(value["source_binding_sha256"]):
        raise ResponseTimingV4Error("attestation_binding_invalid")
    return dict(value)


def _validate_source(value: Any) -> tuple[dict[str, Any], dt.datetime]:
    if type(value) is not dict or set(value) != _SOURCE_FIELDS:
        raise ResponseTimingV4Error("source_closed_schema_mismatch")
    for key, expected in {
        "snapshot_id": "x", "contract_id": REQUIRED_SUCCESSOR_CONTRACT_ID, "cohort_id": REQUIRED_SUCCESSOR_COHORT_ID,
        "mode": "x", "partition": "x", "capture_cutoff_utc": "x", "source_binding_sha256": "x",
        "response_diagnostics": [], "proof_row_count": 0, "test_only": True, "canonical_output": False, **_guard(),
    }.items():
        if type(value.get(key)) is not type(expected):
            raise ResponseTimingV4Error(f"source_{key}_type_mismatch")
    if not value["snapshot_id"] or value["contract_id"] != REQUIRED_SUCCESSOR_CONTRACT_ID or value["cohort_id"] != REQUIRED_SUCCESSOR_COHORT_ID:
        raise ResponseTimingV4Error("source_identity_mismatch")
    if value["mode"] not in DIAGNOSTIC_MODES or value["partition"] != DIAGNOSTIC_MODES[value["mode"]]:
        raise ResponseTimingV4Error("source_partition_mismatch")
    if not _is_sha256(value["source_binding_sha256"]) or value["proof_row_count"] != 0 or value["test_only"] is not True or value["canonical_output"] is not False:
        raise ResponseTimingV4Error("source_guards_invalid")
    for key, expected in _guard().items():
        if value[key] is not expected and value[key] != expected:
            raise ResponseTimingV4Error(f"source_guard_invalid:{key}")
    cutoff = _utc(value["capture_cutoff_utc"], path="source.capture_cutoff_utc")
    return dict(value), cutoff


def _response_identity(row: Mapping[str, Any], *, source_binding_sha256: str, mode: str) -> str:
    material = {"contract_id": REQUIRED_SUCCESSOR_CONTRACT_ID, "cohort_id": REQUIRED_SUCCESSOR_COHORT_ID, "mode": mode, "source_binding_sha256": source_binding_sha256, **{key: row[key] for key in sorted(_RESPONSE_FIELDS - {"response_id", "partition", "direction_selected", "forecast_mean_pips", "forecast_probability", "expected_value_pips", "allocator_rank", "basis_eligible", "proof_eligible", *_guard().keys()})}}
    return "event_response_v6_r4_fixture_" + _sha256_json(material)


def _validated_response(value: Any, *, mode: str, partition: str, cutoff: dt.datetime, source_binding: str, response_contract: Mapping[str, Any], currencies: tuple[str, ...], instruments: tuple[str, ...]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _RESPONSE_FIELDS:
        raise ResponseTimingV4Error("response_closed_schema_mismatch")
    string_fields = _RESPONSE_FIELDS - {"horizon_sec", "start_bid", "start_ask", "end_bid", "end_ask", "pip", "direction_selected", "forecast_mean_pips", "forecast_probability", "expected_value_pips", "allocator_rank", "basis_eligible", "proof_eligible", "research_only", "execution_eligible", "can_place_orders", "can_promote", "can_authorize"}
    for field in string_fields:
        if type(value.get(field)) is not str or not value[field]:
            raise ResponseTimingV4Error(f"response_{field}_must_be_exact_str")
    for field in ("start_bid", "start_ask", "end_bid", "end_ask", "pip"):
        _exact_float(value.get(field), path=f"response.{field}", positive=True)
    if type(value.get("horizon_sec")) is not int or value["horizon_sec"] not in HORIZONS_SEC:
        raise ResponseTimingV4Error("response_horizon_invalid")
    for field in ("direction_selected", "forecast_mean_pips", "forecast_probability", "expected_value_pips", "allocator_rank"):
        if value.get(field) is not None:
            raise ResponseTimingV4Error(f"response_{field}_must_be_none")
    for field in ("basis_eligible", "proof_eligible"):
        if type(value.get(field)) is not bool or value[field] is not False:
            raise ResponseTimingV4Error(f"response_{field}_must_be_false")
    for field, expected in _guard().items():
        if type(value.get(field)) is not type(expected) or value[field] != expected:
            raise ResponseTimingV4Error(f"response_guard_invalid:{field}")
    row = dict(value)
    if row["partition"] != partition or row["instrument"] not in instruments:
        raise ResponseTimingV4Error("response_partition_or_instrument_invalid")
    base, quote = row["instrument"].split("_")
    if row["currency"] not in currencies or row["currency"] not in {base, quote}:
        raise ResponseTimingV4Error("response_currency_not_canonical_pair_leg")
    canonical_pip = _canonical_pip(row["instrument"], instruments)
    if row["pip"] != canonical_pip:
        raise ResponseTimingV4Error("response_pip_not_exact_canonical")
    event = _utc(row["event_scheduled_utc"], path="response.event_scheduled_utc")
    start_target = _utc(row["start_target_utc"], path="response.start_target_utc")
    end_target = _utc(row["end_target_utc"], path="response.end_target_utc")
    start_quote = _utc(row["start_quote_time_utc"], path="response.start_quote_time_utc")
    end_quote = _utc(row["end_quote_time_utc"], path="response.end_quote_time_utc")
    start_known = _utc(row["start_known_utc"], path="response.start_known_utc")
    end_known = _utc(row["end_known_utc"], path="response.end_known_utc")
    if start_target != event or end_target != event + dt.timedelta(seconds=row["horizon_sec"]):
        raise ResponseTimingV4Error("response_target_chronology_invalid")
    start_alignment = (start_quote - start_target).total_seconds()
    end_alignment = (end_quote - end_target).total_seconds()
    if not 0 <= start_alignment <= float(response_contract["maximum_start_alignment_sec"]) or not 0 <= end_alignment <= float(response_contract["maximum_end_alignment_sec"]):
        raise ResponseTimingV4Error("response_alignment_invalid")
    for quote_time, known_time in ((start_quote, start_known), (end_quote, end_known)):
        lag = (known_time - quote_time).total_seconds()
        if lag < 0 or lag > float(response_contract["maximum_quote_known_lag_sec"]) or known_time > cutoff:
            raise ResponseTimingV4Error("response_knowledge_clock_invalid")
    if start_quote > end_quote or start_known > end_known:
        raise ResponseTimingV4Error("response_clock_order_invalid")
    if row["start_ask"] <= row["start_bid"] or row["end_ask"] <= row["end_bid"]:
        raise ResponseTimingV4Error("response_bid_ask_invalid")
    if max((row["start_ask"] - row["start_bid"]) / canonical_pip, (row["end_ask"] - row["end_bid"]) / canonical_pip) > float(response_contract["maximum_spread_pips"]):
        raise ResponseTimingV4Error("response_spread_exceeds_contract")
    for field in ("start_source_payload_sha256", "end_source_payload_sha256"):
        if not _is_sha256(row[field]):
            raise ResponseTimingV4Error(f"response_{field}_invalid")
    expected_id = _response_identity(row, source_binding_sha256=source_binding, mode=mode)
    if row["response_id"] != expected_id:
        raise ResponseTimingV4Error("response_identity_mismatch")
    start_mid = (row["start_bid"] + row["start_ask"]) / 2.0
    end_mid = (row["end_bid"] + row["end_ask"]) / 2.0
    gross_long = (end_mid - start_mid) / canonical_pip
    long_after = (row["end_bid"] - row["start_ask"]) / canonical_pip
    short_after = (row["start_bid"] - row["end_ask"]) / canonical_pip
    return {**row, "start_alignment_sec": start_alignment, "end_alignment_sec": end_alignment, "actual_quote_duration_sec": (end_quote - start_quote).total_seconds(), "start_spread_pips": (row["start_ask"] - row["start_bid"]) / canonical_pip, "end_spread_pips": (row["end_ask"] - row["end_bid"]) / canonical_pip, "gross_long_mid_pips": gross_long, "gross_short_mid_pips": -gross_long, "long_after_executable_spread_pips": long_after, "short_after_executable_spread_pips": short_after, "long_execution_cost_pips": gross_long - long_after, "short_execution_cost_pips": -gross_long - short_after}


def _exact_mapping(actual: Any, expected: Mapping[str, Any], *, path: str) -> None:
    if type(actual) is not dict or set(actual) != set(expected):
        raise ResponseTimingV4Error(f"{path}_closed_schema_mismatch")
    for key, value in expected.items():
        if type(actual[key]) is not type(value) or actual[key] != value:
            raise ResponseTimingV4Error(f"{path}_{key}_mismatch")


def _canonical_responses(values: list[Any], *, mode: str, partition: str, cutoff: dt.datetime, source_binding: str, response_contract: Mapping[str, Any], currencies: tuple[str, ...], instruments: tuple[str, ...], enriched: bool) -> list[dict[str, Any]]:
    if type(values) is not list:
        raise ResponseTimingV4Error("responses_must_be_exact_list")
    result, natural_seen, id_seen = [], set(), set()
    for value in values:
        if enriched:
            if type(value) is not dict or set(value) != _RESPONSE_FIELDS | _DERIVED_FIELDS:
                raise ResponseTimingV4Error("enriched_response_closed_schema_mismatch")
            raw = {key: value[key] for key in _RESPONSE_FIELDS}
        else:
            raw = value
        row = _validated_response(raw, mode=mode, partition=partition, cutoff=cutoff, source_binding=source_binding, response_contract=response_contract, currencies=currencies, instruments=instruments)
        if enriched:
            _exact_mapping(value, row, path="enriched_response")
        natural = (row["event_version_id"], row["instrument"], row["horizon_sec"])
        if natural in natural_seen or row["response_id"] in id_seen:
            raise ResponseTimingV4Error("duplicate_response_cell_or_id")
        natural_seen.add(natural)
        id_seen.add(row["response_id"])
        result.append(row)
    result.sort(key=lambda row: (_utc(row["event_scheduled_utc"], path="response.sort_time"), row["event_version_id"], row["instrument"], row["horizon_sec"]))
    return result


def _arms(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    long_after, short_after = row["long_after_executable_spread_pips"], row["short_after_executable_spread_pips"]
    if long_after > 0 and long_after > short_after:
        direction, clearance = "long", "long_only"
    elif short_after > 0 and short_after > long_after:
        direction, clearance = "short", "short_only"
    else:
        direction, clearance = None, "neither"
    common = {"response_id": row["response_id"], "event_id": row["event_id"], "event_version_id": row["event_version_id"], "instrument": row["instrument"], "horizon_sec": row["horizon_sec"], "source_partition": row["partition"], "post_outcome_only": True, "forecast_direction": None, "forecast_probability": None, "expected_value_pips": None, "allocator_rank": None, "basis_eligible": False, "proof_eligible": False, **_guard()}
    return [
        {**common, "arm_id": "observed_response", "observed_direction_after_cost": direction, "observed_long_after_cost_pips": long_after, "observed_short_after_cost_pips": short_after},
        {**common, "arm_id": "cost_clearance", "cost_clearance_state": clearance, "best_observed_after_cost_pips": max(long_after, short_after)},
        {**common, "arm_id": "response_latency", "start_alignment_sec": row["start_alignment_sec"], "end_alignment_sec": row["end_alignment_sec"], "start_known_lag_sec": (_utc(row["start_known_utc"], path="arm.start_known") - _utc(row["start_quote_time_utc"], path="arm.start_quote")).total_seconds(), "end_known_lag_sec": (_utc(row["end_known_utc"], path="arm.end_known") - _utc(row["end_quote_time_utc"], path="arm.end_quote")).total_seconds()},
        {**common, "arm_id": "no_trade", "control_state": "no_trade"},
    ]


def _canonical_arms(responses: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = [arm for response in responses for arm in _arms(response)]
    result.sort(key=lambda row: (row["event_version_id"], row["instrument"], row["horizon_sec"], ARM_IDS.index(row["arm_id"])))
    return result


def build_test_diagnostic_snapshot(*, event_response_snapshot: Mapping[str, Any], dependency_attestation: Mapping[str, Any]) -> dict[str, Any]:
    contract = _load_contract()
    _verify_r3(contract)
    currencies, instruments = _load_universe(contract)
    _verify_barred_lineage(contract)
    source, cutoff = _validate_source(event_response_snapshot)
    attestation = _validate_attestation(dependency_attestation, source_snapshot_id=source["snapshot_id"], source_binding=source["source_binding_sha256"])
    responses = _canonical_responses(source["response_diagnostics"], mode=source["mode"], partition=source["partition"], cutoff=cutoff, source_binding=source["source_binding_sha256"], response_contract=contract["response_contract"], currencies=currencies, instruments=instruments, enriched=False)
    arms = _canonical_arms(responses)
    result = {"schema_version": 4, "snapshot_schema": SNAPSHOT_SCHEMA, "snapshot_id": None, "contract_id": CONTRACT_ID, "contract_sha256": CONTRACT_SHA256, "research_generation": RESEARCH_GENERATION, "mode": source["mode"], "partition": source["partition"], "test_only": True, "canonical_output": False, "source_snapshot_id": source["snapshot_id"], "source_contract_id": source["contract_id"], "source_cohort_id": source["cohort_id"], "source_binding_sha256": source["source_binding_sha256"], "dependency_attestation": attestation, "capture_cutoff_utc": source["capture_cutoff_utc"], "response_count": len(responses), "timing_arm_count": len(arms), "proof_row_count": 0, "responses": responses, "timing_arms": arms, "limitations": list(LIMITATIONS), **_guard()}
    result["snapshot_id"] = "currency_state_response_timing_v4_test_" + _sha256_json({key: value for key, value in result.items() if key != "snapshot_id"})
    validate_test_diagnostic_snapshot(result)
    return result


def validate_test_diagnostic_snapshot(snapshot: Mapping[str, Any]) -> None:
    if type(snapshot) is not dict or set(snapshot) != _RESULT_FIELDS:
        raise ResponseTimingV4Error("snapshot_closed_schema_mismatch")
    expected_types = {"schema_version": int, "snapshot_schema": str, "snapshot_id": str, "contract_id": str, "contract_sha256": str, "research_generation": str, "mode": str, "partition": str, "test_only": bool, "canonical_output": bool, "source_snapshot_id": str, "source_contract_id": str, "source_cohort_id": str, "source_binding_sha256": str, "dependency_attestation": dict, "capture_cutoff_utc": str, "response_count": int, "timing_arm_count": int, "proof_row_count": int, "responses": list, "timing_arms": list, "limitations": list, "research_only": bool, "execution_eligible": bool, "can_place_orders": bool, "can_promote": bool, "can_authorize": bool, "supported_execution_decision": str}
    for field, expected in expected_types.items():
        if type(snapshot[field]) is not expected:
            raise ResponseTimingV4Error(f"snapshot_{field}_type_mismatch")
    contract = _load_contract()
    _verify_r3(contract)
    currencies, instruments = _load_universe(contract)
    _verify_barred_lineage(contract)
    if snapshot["schema_version"] != 4 or snapshot["snapshot_schema"] != SNAPSHOT_SCHEMA or snapshot["contract_id"] != CONTRACT_ID or snapshot["contract_sha256"] != CONTRACT_SHA256 or snapshot["research_generation"] != RESEARCH_GENERATION:
        raise ResponseTimingV4Error("snapshot_identity_mismatch")
    if snapshot["mode"] not in DIAGNOSTIC_MODES or snapshot["partition"] != DIAGNOSTIC_MODES[snapshot["mode"]]:
        raise ResponseTimingV4Error("snapshot_partition_mismatch")
    if snapshot["test_only"] is not True or snapshot["canonical_output"] is not False or snapshot["proof_row_count"] != 0:
        raise ResponseTimingV4Error("snapshot_guards_invalid")
    for field, expected in _guard().items():
        if snapshot[field] != expected:
            raise ResponseTimingV4Error(f"snapshot_guard_invalid:{field}")
    if snapshot["source_contract_id"] != REQUIRED_SUCCESSOR_CONTRACT_ID or snapshot["source_cohort_id"] != REQUIRED_SUCCESSOR_COHORT_ID or not snapshot["source_snapshot_id"] or not _is_sha256(snapshot["source_binding_sha256"]):
        raise ResponseTimingV4Error("snapshot_source_identity_invalid")
    _validate_attestation(snapshot["dependency_attestation"], source_snapshot_id=snapshot["source_snapshot_id"], source_binding=snapshot["source_binding_sha256"])
    cutoff = _utc(snapshot["capture_cutoff_utc"], path="snapshot.capture_cutoff_utc")
    responses = _canonical_responses(snapshot["responses"], mode=snapshot["mode"], partition=snapshot["partition"], cutoff=cutoff, source_binding=snapshot["source_binding_sha256"], response_contract=contract["response_contract"], currencies=currencies, instruments=instruments, enriched=True)
    if len(responses) != len(snapshot["responses"]):
        raise ResponseTimingV4Error("snapshot_response_order_mismatch")
    for actual, expected in zip(snapshot["responses"], responses):
        _exact_mapping(actual, expected, path="snapshot_response")
    expected_arms = _canonical_arms(responses)
    if len(snapshot["timing_arms"]) != len(expected_arms):
        raise ResponseTimingV4Error("snapshot_arm_count_mismatch")
    for actual, expected in zip(snapshot["timing_arms"], expected_arms):
        _exact_mapping(actual, expected, path="snapshot_arm")
    if snapshot["response_count"] != len(responses) or snapshot["timing_arm_count"] != len(expected_arms):
        raise ResponseTimingV4Error("snapshot_declared_count_mismatch")
    if snapshot["limitations"] != LIMITATIONS or any(type(item) is not str for item in snapshot["limitations"]):
        raise ResponseTimingV4Error("snapshot_limitations_mismatch")
    expected_id = "currency_state_response_timing_v4_test_" + _sha256_json({key: value for key, value in snapshot.items() if key != "snapshot_id"})
    if snapshot["snapshot_id"] != expected_id:
        raise ResponseTimingV4Error("snapshot_id_mismatch")


_MANIFEST_STATE = {"enabled": False, "registered": False, "research_worker_enabled": False, "collector_enabled": False, "canonical_output_initialized": False}
_MANIFEST_POLICY = {"exact_closed_manifest_schema": True, "canonical_pip_is_instrument_derived": True, "timestamps_sort_by_numeric_utc": True, "exact_builtin_scalar_types_required": True, "dependency_resolution_requires_new_timing_contract": True, **_guard()}
_MANIFEST_OUTPUT = dict(_OUTPUT_SURFACE)
_MANIFEST_TOP_FIELDS = {"manifest_schema_version", "manifest_id", "contract_id", "research_generation", "parent_contract_id", "parent_disposition", "review_state", "candidate_frozen_utc", "independent_review_started_utc", "independent_review_completed_utc", "runtime_started_utc", "state", "dependency_disposition", "artifacts", "output_surface", "policy"}


def verify_candidate_manifest() -> dict[str, Any]:
    manifest = _strict_json(_stable_read("config/currency_state_response_timing_arms_v4_manifest.json"), label="timing_v4_manifest")
    if type(manifest) is not dict or set(manifest) != _MANIFEST_TOP_FIELDS:
        raise ResponseTimingV4Error("manifest_closed_schema_mismatch")
    if type(manifest["manifest_schema_version"]) is not int or manifest["manifest_schema_version"] != 1 or manifest["manifest_id"] != "currency_state_response_timing_arms_v4_manifest_20260817_r1" or manifest["contract_id"] != CONTRACT_ID or manifest["review_state"] != "pending_independent_review":
        raise ResponseTimingV4Error("manifest_identity_or_review_state_mismatch")
    if manifest["state"] != _MANIFEST_STATE or manifest["output_surface"] != _MANIFEST_OUTPUT or manifest["policy"] != _MANIFEST_POLICY:
        raise ResponseTimingV4Error("manifest_inert_schema_mismatch")
    disposition = manifest["dependency_disposition"]
    expected_disposition = {"approved_official_fact_currency_state_v5_r3": True, "barred_event_response_generations": ["v6_r1", "v6_r2", "v6_r3"], "approved_event_response_successor_bound": False, "production_dependency_gate": "closed", "blocking_reason": "event_response_v6_r4_final_approval_not_bound", "resolution_requires_new_timing_contract_id": True}
    if disposition != expected_disposition:
        raise ResponseTimingV4Error("manifest_dependency_disposition_mismatch")
    expected_paths = {
        "runtime_core": "src/forex_system/research/currency_state_response_timing_arms_v4.py", "contract": "config/currency_state_response_timing_arms_v4.json", "focused_tests": "test_currency_state_response_timing_arms_v4.py",
        "parent_v3_module": "src/forex_system/research/currency_state_response_timing_arms_v3.py", "parent_v3_contract": "config/currency_state_response_timing_arms_v3.json", "parent_v3_manifest": "config/currency_state_response_timing_arms_v3_manifest.json", "parent_v3_tests": "test_currency_state_response_timing_arms_v3.py",
        "approved_r3_review_certificate": "config/official_fact_currency_state_v5_r3_review_certificate.json", "official_fact_v5_r3_manifest": "config/official_fact_adapter_v5_manifest.json", "currency_state_context_v5_r3_manifest": "config/currency_state_official_context_v5_manifest.json", "canonical_universe_contract": "config/currency_state_engine_v2.json",
        "barred_v6_r1_contract": "config/prospective_event_response_v6_contract.json", "barred_v6_r1_manifest": "config/prospective_event_response_v6_manifest.json", "barred_v6_r2_contract": "config/prospective_event_response_v6_r2_contract.json", "barred_v6_r2_manifest": "config/prospective_event_response_v6_r2_manifest.json", "barred_v6_r3_contract": "config/prospective_event_response_v6_r3_contract.json", "barred_v6_r3_manifest": "config/prospective_event_response_v6_r3_manifest.json",
    }
    if type(manifest["artifacts"]) is not dict or set(manifest["artifacts"]) != set(expected_paths):
        raise ResponseTimingV4Error("manifest_artifact_set_mismatch")
    for name, path in expected_paths.items():
        spec = _artifact_spec(manifest["artifacts"][name], path=f"manifest.{name}")
        if spec[0] != path:
            raise ResponseTimingV4Error(f"manifest_artifact_path_mismatch:{name}")
        _read_exact(*spec)
    return {"manifest_id": manifest["manifest_id"], "review_state": manifest["review_state"], "artifact_count": len(expected_paths), "dependency_gate": "closed", "candidate_closure_verified": True}


__all__ = ["ARM_IDS", "CANONICAL_PIP_MAPPING_SHA256", "CONTRACT_ID", "CONTRACT_SHA256", "DIAGNOSTIC_MODES", "REQUIRED_SUCCESSOR_COHORT_ID", "REQUIRED_SUCCESSOR_CONTRACT_ID", "ResponseTimingV4DependencyError", "ResponseTimingV4Error", "build_prospective_proof", "build_replay_diagnostic", "build_test_diagnostic_snapshot", "production_dependency_status", "require_production_dependencies", "validate_test_diagnostic_snapshot", "verify_candidate_manifest"]
