"""Disabled prospective official-event response candidate V5.

V5 is deliberately a research-only, temp-ledger candidate.  It cannot place
orders, promote evidence, initialize a canonical ledger, or declare itself
reviewed.  Its bundle remains unresolved until independently approved source
dependencies are supplied and an external reviewer freezes the final bytes.

The module removes V4's legacy selector dependency.  It consumes a complete,
strictly validated Clock V2 reconstruction, binds the entire reconstruction
and its provenance, plans exactly 68 instruments, and records executable
bid/ask responses without selecting a trade direction.
"""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .immutable_event_clock import (
    CONTRACT_ID as CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as CLOCK_SCHEMA_VERSION,
    reconstruct_as_of as reconstruct_clock_as_of,
)


UTC = dt.timezone.utc
SCHEMA_VERSION = 5
CONTRACT_ID = "prospective_event_response_capture_v5_20260817"
COHORT_ID = "prospective_official_event_response_v5_20260817"
RESEARCH_GENERATION = "prospective_official_event_response_capture_v5"
SUPPORTED_EXECUTION_DECISION = "no_trade"
EXPECTED_EVENT_PIPELINE_VERSION = "all_pair_news_event_tags_v3"
EXPECTED_DEPENDENCY_CONTRACT_ID = "currency_policy_dependency_registry_v1_20260816"

CANONICAL_INSTRUMENTS = (
    "AUD_CAD", "AUD_CHF", "AUD_HKD", "AUD_JPY", "AUD_NZD", "AUD_SGD", "AUD_USD",
    "CAD_CHF", "CAD_HKD", "CAD_JPY", "CAD_SGD", "CHF_HKD", "CHF_JPY", "CHF_ZAR",
    "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_CZK", "EUR_DKK", "EUR_GBP", "EUR_HKD",
    "EUR_HUF", "EUR_JPY", "EUR_NOK", "EUR_NZD", "EUR_PLN", "EUR_SEK", "EUR_SGD",
    "EUR_TRY", "EUR_USD", "EUR_ZAR", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_HKD",
    "GBP_JPY", "GBP_NZD", "GBP_PLN", "GBP_SGD", "GBP_USD", "GBP_ZAR", "HKD_JPY",
    "NZD_CAD", "NZD_CHF", "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD", "SGD_CHF",
    "SGD_JPY", "TRY_JPY", "USD_CAD", "USD_CHF", "USD_CNH", "USD_CZK", "USD_DKK",
    "USD_HKD", "USD_HUF", "USD_JPY", "USD_MXN", "USD_NOK", "USD_PLN", "USD_SEK",
    "USD_SGD", "USD_THB", "USD_TRY", "USD_ZAR", "ZAR_JPY",
)


class ProspectiveEventResponseV5Error(RuntimeError):
    """Raised when a V5 proof, identity, or safety invariant fails."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _id(prefix: str, value: Mapping[str, Any]) -> str:
    return f"{prefix}_{_sha256_json(value)[:40]}"


CANONICAL_INSTRUMENT_UNIVERSE_SHA256 = _sha256_json(list(CANONICAL_INSTRUMENTS))
PROJECT_ROOT = Path(__file__).resolve().parents[3]
EMBEDDED_CANONICAL_LEDGER = (
    PROJECT_ROOT
    / "data"
    / "oanda_training_manager"
    / "research_ledgers"
    / "prospective_event_response_v5.sqlite"
)


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _string(value: Any, *, path: str, nonempty: bool = True) -> str:
    if type(value) is not str or (nonempty and not value):
        raise ProspectiveEventResponseV5Error(f"{path}_must_be_string")
    return value


def _boolean(value: Any, *, path: str) -> bool:
    if type(value) is not bool:
        raise ProspectiveEventResponseV5Error(f"{path}_must_be_boolean")
    return value


def _integer(value: Any, *, path: str) -> int:
    if type(value) is not int:
        raise ProspectiveEventResponseV5Error(f"{path}_must_be_integer")
    return value


def _number(value: Any, *, path: str) -> float:
    if type(value) not in {int, float}:
        raise ProspectiveEventResponseV5Error(f"{path}_must_be_number")
    number = float(value)
    if not math.isfinite(number):
        raise ProspectiveEventResponseV5Error(f"{path}_must_be_finite")
    return number


def _utc(value: Any, *, path: str) -> dt.datetime:
    text = _string(value, path=path)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ProspectiveEventResponseV5Error(f"{path}_invalid_utc") from exc
    if parsed.tzinfo is None:
        raise ProspectiveEventResponseV5Error(f"{path}_naive_utc")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _closed_mapping(
    value: Any,
    *,
    path: str,
    fields: set[str] | frozenset[str],
    required: set[str] | frozenset[str] | None = None,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProspectiveEventResponseV5Error(f"{path}_must_be_mapping")
    if any(type(key) is not str for key in value):
        raise ProspectiveEventResponseV5Error(f"{path}_keys_must_be_strings")
    extra = set(value) - set(fields)
    missing = set(required if required is not None else fields) - set(value)
    if extra:
        raise ProspectiveEventResponseV5Error(
            f"{path}_unknown_fields:" + ",".join(sorted(extra))
        )
    if missing:
        raise ProspectiveEventResponseV5Error(
            f"{path}_missing_fields:" + ",".join(sorted(missing))
        )
    return value


_FORBIDDEN_KEY_PARTS = frozenset(
    {
        "direction", "side", "trade", "order", "execute", "execution",
        "authorize", "authorization", "promote", "promotion", "position",
        "units", "notional", "allocation", "rank", "signal", "stop", "target",
    }
)
_GENERATED_SAFE_ALIAS_KEYS = frozenset(
    {
        "execution_eligible",
        "can_place_orders",
        "promotion_eligible",
        "authorization_eligible",
        "supported_execution_decision",
        "target_offset_sec",
        "target_utc",
        "target_distance_sec",
    }
)


def _reject_forbidden_nested_keys(
    value: Any,
    *,
    path: str,
    allowed_exact: frozenset[str] = frozenset(),
) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if type(key) is not str:
                raise ProspectiveEventResponseV5Error(f"{path}_key_not_string")
            normalized = "".join(character if character.isalnum() else "_" for character in key.lower())
            pieces = {piece for piece in normalized.split("_") if piece}
            if key not in allowed_exact and pieces.intersection(_FORBIDDEN_KEY_PARTS):
                raise ProspectiveEventResponseV5Error(f"forbidden_safety_alias:{path}.{key}")
            _reject_forbidden_nested_keys(child, path=f"{path}.{key}", allowed_exact=allowed_exact)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_forbidden_nested_keys(child, path=f"{path}[{index}]", allowed_exact=allowed_exact)


def _research_guard() -> dict[str, Any]:
    return {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }


def _validate_guard(value: Mapping[str, Any]) -> None:
    for key, expected in _research_guard().items():
        actual = value.get(key)
        if type(expected) is bool:
            if type(actual) is not bool or actual is not expected:
                raise ProspectiveEventResponseV5Error(f"unsafe_guard:{key}")
        elif actual != expected:
            raise ProspectiveEventResponseV5Error(f"unsafe_guard:{key}")


_CONTRACT_FIELDS = frozenset(
    {
        "schema_version", "contract_id", "cohort_id", "research_generation",
        "parent_cohort_id", "parent_disposition", "review_state",
        "candidate_bytes_frozen_utc", "independent_reviewed_utc", "cohort_start_utc",
        "enabled", "registered", "canonical_initialization_allowed",
        "registration_blockers", "research_only", "execution_eligible",
        "can_place_orders", "promotion_eligible", "authorization_eligible",
        "supported_execution_decision", "clock_contract_id", "clock_schema_version",
        "required_source_pipeline_version", "policy_dependency_contract_id",
        "instrument_universe", "instrument_universe_sha256", "target_offsets_sec",
        "minimum_schedule_lead_sec", "maximum_planning_horizon_days",
        "host_clock_max_age_sec", "semantic_catalog_max_age_sec", "sampling",
        "postbaseline_semantic_refresh_required", "dependency_slots",
        "forbidden_capabilities",
    }
)


def load_contract(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    _closed_mapping(payload, path="contract", fields=_CONTRACT_FIELDS)
    expected = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "research_generation": RESEARCH_GENERATION,
        "parent_cohort_id": "prospective_official_event_response_v4_20260817",
        "parent_disposition": "independent_no_register_zero_evidence",
        "review_state": "blocked_external_dependencies",
        "candidate_bytes_frozen_utc": None,
        "independent_reviewed_utc": None,
        "cohort_start_utc": None,
        "enabled": False,
        "registered": False,
        "canonical_initialization_allowed": False,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
        "clock_contract_id": CLOCK_CONTRACT_ID,
        "clock_schema_version": CLOCK_SCHEMA_VERSION,
        "required_source_pipeline_version": EXPECTED_EVENT_PIPELINE_VERSION,
        "policy_dependency_contract_id": EXPECTED_DEPENDENCY_CONTRACT_ID,
        "instrument_universe": list(CANONICAL_INSTRUMENTS),
        "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
        "postbaseline_semantic_refresh_required": False,
    }
    for key, wanted in expected.items():
        actual = payload.get(key)
        if type(wanted) is bool:
            if type(actual) is not bool or actual is not wanted:
                raise ProspectiveEventResponseV5Error(f"contract_mismatch:{key}")
        elif actual != wanted:
            raise ProspectiveEventResponseV5Error(f"contract_mismatch:{key}")
    blockers = payload["registration_blockers"]
    if blockers != [
        "independent_v5_review_pending",
        "official_fact_adapter_v3_independent_approval_pending",
        "currency_state_official_context_v3_independent_approval_pending",
        "final_byte_closure_and_external_freeze_pending",
    ]:
        raise ProspectiveEventResponseV5Error("registration_blockers_mismatch")
    slots = payload["dependency_slots"]
    if not isinstance(slots, list) or len(slots) != 2:
        raise ProspectiveEventResponseV5Error("dependency_slots_invalid")
    for index, slot in enumerate(slots):
        _closed_mapping(
            slot,
            path=f"dependency_slots[{index}]",
            fields={"name", "state", "relative_path", "sha256"},
        )
        _string(slot["name"], path=f"dependency_slots[{index}].name")
        if slot["state"] != "pending_independent_approval" or slot["relative_path"] is not None or slot["sha256"] is not None:
            raise ProspectiveEventResponseV5Error("dependency_slot_cannot_self_approve")
    offsets = payload["target_offsets_sec"]
    if not isinstance(offsets, list) or any(type(value) is not int for value in offsets):
        raise ProspectiveEventResponseV5Error("target_offsets_must_be_integer_list")
    if offsets != sorted(set(offsets)) or not offsets or offsets[0] != 0:
        raise ProspectiveEventResponseV5Error("target_offsets_invalid")
    exact_numbers = {
        "minimum_schedule_lead_sec": 60,
        "maximum_planning_horizon_days": 370,
        "host_clock_max_age_sec": 90,
        "semantic_catalog_max_age_sec": 300,
    }
    for key, wanted in exact_numbers.items():
        if _integer(payload[key], path=f"contract.{key}") != wanted:
            raise ProspectiveEventResponseV5Error(f"contract_mismatch:{key}")
    if payload["forbidden_capabilities"] != [
        "broker_api",
        "order_submission",
        "position_management",
        "promotion",
        "authorization",
        "canonical_ledger_initialization",
    ]:
        raise ProspectiveEventResponseV5Error("forbidden_capabilities_mismatch")
    sampling = _closed_mapping(
        payload["sampling"],
        path="sampling",
        fields={
            "maximum_target_lateness_sec", "maximum_quote_lead_sec",
            "maximum_quote_age_at_collection_sec", "maximum_snapshot_age_at_collection_sec",
            "maximum_market_attestation_age_sec", "maximum_future_clock_skew_sec",
            "maximum_spread_pips_sanity", "first_valid_observation_wins",
        },
    )
    exact_sampling = {
        "maximum_target_lateness_sec": 45,
        "maximum_quote_lead_sec": 2,
        "maximum_quote_age_at_collection_sec": 15,
        "maximum_snapshot_age_at_collection_sec": 15,
        "maximum_market_attestation_age_sec": 90,
        "maximum_future_clock_skew_sec": 2,
        "maximum_spread_pips_sanity": 5000,
        "first_valid_observation_wins": True,
    }
    for key, wanted in exact_sampling.items():
        if sampling[key] != wanted or type(sampling[key]) is not type(wanted):
            raise ProspectiveEventResponseV5Error(f"sampling_mismatch:{key}")
    return payload


def make_temp_test_contract(candidate: Mapping[str, Any], *, cohort_start_utc: dt.datetime) -> dict[str, Any]:
    result = json.loads(_canonical_json(candidate))
    result["review_state"] = "temp_fixture_only"
    result["cohort_start_utc"] = _iso(cohort_start_utc)
    result["test_only"] = True
    return result


def validate_temp_contract(contract: Mapping[str, Any]) -> dt.datetime:
    _closed_mapping(
        contract,
        path="temp_contract",
        fields=set(_CONTRACT_FIELDS) | {"test_only"},
    )
    if contract.get("review_state") != "temp_fixture_only" or contract.get("test_only") is not True:
        raise ProspectiveEventResponseV5Error("temp_fixture_contract_required")
    fixed = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "research_generation": RESEARCH_GENERATION,
        "parent_cohort_id": "prospective_official_event_response_v4_20260817",
        "parent_disposition": "independent_no_register_zero_evidence",
        "candidate_bytes_frozen_utc": None,
        "independent_reviewed_utc": None,
        "clock_contract_id": CLOCK_CONTRACT_ID,
        "clock_schema_version": CLOCK_SCHEMA_VERSION,
        "required_source_pipeline_version": EXPECTED_EVENT_PIPELINE_VERSION,
        "policy_dependency_contract_id": EXPECTED_DEPENDENCY_CONTRACT_ID,
        "target_offsets_sec": [0, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400],
        "minimum_schedule_lead_sec": 60,
        "maximum_planning_horizon_days": 370,
        "host_clock_max_age_sec": 90,
        "semantic_catalog_max_age_sec": 300,
        "postbaseline_semantic_refresh_required": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    for key, wanted in fixed.items():
        if contract.get(key) != wanted or (
            type(wanted) is int and type(contract.get(key)) is not int
        ):
            raise ProspectiveEventResponseV5Error(f"temp_contract_drift:{key}")
    for key in ("enabled", "registered", "canonical_initialization_allowed"):
        if type(contract.get(key)) is not bool or contract.get(key) is not False:
            raise ProspectiveEventResponseV5Error(f"unsafe_temp_contract:{key}")
    for key, wanted in _research_guard().items():
        if contract.get(key) != wanted or (
            type(wanted) is bool and type(contract.get(key)) is not bool
        ):
            raise ProspectiveEventResponseV5Error(f"temp_contract_guard_drift:{key}")
    if contract.get("instrument_universe") != list(CANONICAL_INSTRUMENTS):
        raise ProspectiveEventResponseV5Error("exact_68_instrument_universe_required")
    if contract.get("instrument_universe_sha256") != CANONICAL_INSTRUMENT_UNIVERSE_SHA256:
        raise ProspectiveEventResponseV5Error("instrument_universe_hash_mismatch")
    expected_sampling = {
        "maximum_target_lateness_sec": 45,
        "maximum_quote_lead_sec": 2,
        "maximum_quote_age_at_collection_sec": 15,
        "maximum_snapshot_age_at_collection_sec": 15,
        "maximum_market_attestation_age_sec": 90,
        "maximum_future_clock_skew_sec": 2,
        "maximum_spread_pips_sanity": 5000,
        "first_valid_observation_wins": True,
    }
    if contract.get("sampling") != expected_sampling:
        raise ProspectiveEventResponseV5Error("temp_contract_sampling_drift")
    if contract.get("registration_blockers") != [
        "independent_v5_review_pending",
        "official_fact_adapter_v3_independent_approval_pending",
        "currency_state_official_context_v3_independent_approval_pending",
        "final_byte_closure_and_external_freeze_pending",
    ]:
        raise ProspectiveEventResponseV5Error("temp_contract_blocker_drift")
    if contract.get("dependency_slots") != [
        {
            "name": "official_fact_adapter_v3",
            "state": "pending_independent_approval",
            "relative_path": None,
            "sha256": None,
        },
        {
            "name": "currency_state_official_context_v3",
            "state": "pending_independent_approval",
            "relative_path": None,
            "sha256": None,
        },
    ]:
        raise ProspectiveEventResponseV5Error("temp_contract_dependency_slot_drift")
    if contract.get("forbidden_capabilities") != [
        "broker_api", "order_submission", "position_management", "promotion",
        "authorization", "canonical_ledger_initialization",
    ]:
        raise ProspectiveEventResponseV5Error("temp_contract_forbidden_capability_drift")
    return _utc(contract.get("cohort_start_utc"), path="cohort_start_utc")


_CLOCK_TOP_FIELDS = frozenset(
    {
        "schema_version", "contract_id", "decision_cutoff_utc", "snapshot_id",
        "snapshot_captured_utc", "clock_observation_id", "clock_observed_utc",
        "clock_observation_source_generated_utc", "clock_observation_events_sha256",
        "clock_observation_manifest_sha256", "clock_observation_semantic_clock_sha256",
        "source_generated_utc", "source_pipeline_version", "events_sha256",
        "manifest_sha256", "semantic_snapshot_events_sha256",
        "semantic_snapshot_manifest_sha256", "clock_attestation",
        "snapshot_clock_attestation", "event_count", "events", "policy",
        "research_only", "execution_eligible", "supported_execution_decision",
    }
)
_CLOCK_ATTESTATION_FIELDS = frozenset(
    {
        "age_at_capture_sec", "artifact_name", "attested", "generated_utc",
        "host_clock_synchronized", "maximum_age_sec", "payload_sha256",
        "source_fresh", "state", "status", "timestamp_normalization_trusted",
    }
)
_CLOCK_POLICY_FIELDS = frozenset(
    {
        "calendar_is_not_direction", "as_of_uses_last_observed_complete_snapshot",
        "source_contract_versions_cannot_be_backdated",
        "event_availability_is_maximum_causal_clock",
        "proof_availability_requires_fresh_trusted_capture", "research_only",
        "can_place_orders", "can_promote",
    }
)
_EVENT_FIELDS = frozenset(
    {
        "category", "clock_semantic_id", "clock_semantics", "currencies",
        "direct_currencies", "event_availability_utc", "event_snapshot_first_observed_utc",
        "event_snapshot_first_trusted_observed_utc", "event_time_basis", "event_utc",
        "event_version_id", "headline", "independent_domestic_event",
        "ledger_effective_known_utc", "linked_policy_factor", "material_content_sha256",
        "original_fact_known_utc", "raw_event_availability_utc",
        "release_rule_bytes_verified", "release_time_rule_archive_name",
        "release_time_rule_observed_sha256", "schedule_window_end_utc", "scheduled_utc",
        "source_cohort_id", "source_contract_first_observed_utc",
        "source_contract_first_trusted_observed_utc", "source_contract_id", "source_id",
        "source_name", "source_type", "source_url", "source_verified",
        "timing_precision", "trusted_for_prospective_evidence", "upstream_event_id",
        "upstream_first_known_utc", "upstream_updated_utc",
    }
)


def _validate_clock_attestation(value: Any, *, path: str, at: dt.datetime, maximum_age: float) -> dict[str, Any]:
    row = _closed_mapping(value, path=path, fields=_CLOCK_ATTESTATION_FIELDS)
    for key in ("attested", "host_clock_synchronized", "source_fresh", "timestamp_normalization_trusted"):
        if _boolean(row[key], path=f"{path}.{key}") is not True:
            raise ProspectiveEventResponseV5Error(f"{path}.{key}_not_true")
    if row["state"] != "fresh_trusted" or row["status"] != "ok":
        raise ProspectiveEventResponseV5Error(f"{path}_not_fresh_trusted")
    _string(row["artifact_name"], path=f"{path}.artifact_name")
    if not _is_sha256(row["payload_sha256"]):
        raise ProspectiveEventResponseV5Error(f"{path}.payload_sha256_invalid")
    if _number(row["maximum_age_sec"], path=f"{path}.maximum_age_sec") != 90.0:
        raise ProspectiveEventResponseV5Error(f"{path}.maximum_age_sec_invalid")
    _number(row["age_at_capture_sec"], path=f"{path}.age_at_capture_sec")
    generated = _utc(row["generated_utc"], path=f"{path}.generated_utc")
    age = (at - generated).total_seconds()
    if age < 0 or age > maximum_age:
        raise ProspectiveEventResponseV5Error(f"{path}_stale_or_future")
    return {"generated_utc": _iso(generated), "age_sec": round(age, 6)}


def _validate_event(row: Any, *, index: int, planned: dt.datetime) -> dict[str, Any]:
    path = f"clock.events[{index}]"
    event = _closed_mapping(row, path=path, fields=_EVENT_FIELDS)
    _reject_forbidden_nested_keys(event, path=path)
    for key in _EVENT_FIELDS - {"currencies", "direct_currencies", "independent_domestic_event", "linked_policy_factor", "release_rule_bytes_verified", "source_verified", "trusted_for_prospective_evidence"}:
        _string(event[key], path=f"{path}.{key}", nonempty=key == "headline" or key not in {"schedule_window_end_utc"})
    for key in ("independent_domestic_event", "linked_policy_factor", "release_rule_bytes_verified", "source_verified", "trusted_for_prospective_evidence"):
        _boolean(event[key], path=f"{path}.{key}")
    for key in ("currencies", "direct_currencies"):
        values = event[key]
        if not isinstance(values, list) or not values or any(type(item) is not str or len(item) != 3 or item != item.upper() for item in values):
            raise ProspectiveEventResponseV5Error(f"{path}.{key}_invalid")
        if values != sorted(set(values)):
            raise ProspectiveEventResponseV5Error(f"{path}.{key}_not_canonical")
    for key in ("material_content_sha256", "release_time_rule_observed_sha256"):
        if not _is_sha256(event[key]):
            raise ProspectiveEventResponseV5Error(f"{path}.{key}_invalid")
    if event["timing_precision"] != "minute" or event["event_time_basis"] != "scheduled_release":
        raise ProspectiveEventResponseV5Error(f"{path}_not_exact_scheduled_release")
    if event["clock_semantics"] not in {"domestic_official_policy_release", "domestic_official_statistical_release"}:
        raise ProspectiveEventResponseV5Error(f"{path}_clock_semantics_invalid")
    for key in ("independent_domestic_event", "release_rule_bytes_verified", "source_verified", "trusted_for_prospective_evidence"):
        if event[key] is not True:
            raise ProspectiveEventResponseV5Error(f"{path}.{key}_not_true")
    times = {
        key: _utc(event[key], path=f"{path}.{key}")
        for key in (
            "scheduled_utc", "event_utc", "event_availability_utc",
            "event_snapshot_first_observed_utc", "event_snapshot_first_trusted_observed_utc",
            "ledger_effective_known_utc", "original_fact_known_utc",
            "raw_event_availability_utc", "source_contract_first_observed_utc",
            "source_contract_first_trusted_observed_utc", "upstream_first_known_utc",
            "upstream_updated_utc",
        )
    }
    if times["event_utc"] != times["scheduled_utc"]:
        raise ProspectiveEventResponseV5Error(f"{path}_event_clock_disagrees")
    expected_availability = max(
        times["original_fact_known_utc"],
        times["event_snapshot_first_trusted_observed_utc"],
        times["source_contract_first_trusted_observed_utc"],
    )
    if times["event_availability_utc"] != expected_availability or times["ledger_effective_known_utc"] != expected_availability:
        raise ProspectiveEventResponseV5Error(f"{path}_availability_backdating")
    if expected_availability > planned or expected_availability >= times["scheduled_utc"]:
        raise ProspectiveEventResponseV5Error(f"{path}_not_known_pre_release")
    result = dict(event)
    result["event_payload_sha256"] = _sha256_json(event)
    result["natural_event_key"] = _id(
        "natural_official_event_v5",
        {"upstream_event_id": event["upstream_event_id"], "scheduled_utc": _iso(times["scheduled_utc"])},
    )
    result["scheduled_utc"] = _iso(times["scheduled_utc"])
    return result


def validate_clock_v2_envelope(
    snapshot: Mapping[str, Any],
    *,
    at_utc: dt.datetime,
    semantic_freshness_required: bool,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    at = at_utc.astimezone(UTC)
    row = _closed_mapping(snapshot, path="clock", fields=_CLOCK_TOP_FIELDS)
    _reject_forbidden_nested_keys(
        row,
        path="clock",
        allowed_exact=frozenset(
            {
                "execution_eligible",
                "can_place_orders",
                "can_promote",
                "supported_execution_decision",
                "calendar_is_not_direction",
            }
        ),
    )
    if row["schema_version"] != CLOCK_SCHEMA_VERSION or row["contract_id"] != CLOCK_CONTRACT_ID:
        raise ProspectiveEventResponseV5Error("clock_v2_contract_identity_mismatch")
    if row["source_pipeline_version"] != EXPECTED_EVENT_PIPELINE_VERSION:
        raise ProspectiveEventResponseV5Error("clock_pipeline_mismatch")
    for key in ("snapshot_id", "clock_observation_id"):
        _string(row[key], path=f"clock.{key}")
    for key in ("decision_cutoff_utc", "snapshot_captured_utc"):
        _utc(row[key], path=f"clock.{key}")
    if _boolean(row["research_only"], path="clock.research_only") is not True or _boolean(row["execution_eligible"], path="clock.execution_eligible") is not False:
        raise ProspectiveEventResponseV5Error("clock_safety_guard_mismatch")
    if row["supported_execution_decision"] != SUPPORTED_EXECUTION_DECISION:
        raise ProspectiveEventResponseV5Error("clock_decision_mismatch")
    policy = _closed_mapping(row["policy"], path="clock.policy", fields=_CLOCK_POLICY_FIELDS)
    expected_policy = {
        "calendar_is_not_direction": True,
        "as_of_uses_last_observed_complete_snapshot": True,
        "source_contract_versions_cannot_be_backdated": True,
        "event_availability_is_maximum_causal_clock": True,
        "proof_availability_requires_fresh_trusted_capture": True,
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
    }
    for key, wanted in expected_policy.items():
        if _boolean(policy[key], path=f"clock.policy.{key}") is not wanted:
            raise ProspectiveEventResponseV5Error(f"clock.policy.{key}_mismatch")
    host_max = _number(contract["host_clock_max_age_sec"], path="contract.host_clock_max_age_sec")
    internal = _validate_clock_attestation(row["clock_attestation"], path="clock.clock_attestation", at=at, maximum_age=host_max)
    snapshot_attestation = _validate_clock_attestation(row["snapshot_clock_attestation"], path="clock.snapshot_clock_attestation", at=at, maximum_age=host_max)
    if row["clock_attestation"] != row["snapshot_clock_attestation"]:
        raise ProspectiveEventResponseV5Error("clock_attestation_copies_disagree")
    observed = _utc(row["clock_observed_utc"], path="clock.clock_observed_utc")
    observed_age = (at - observed).total_seconds()
    if observed_age < 0 or observed_age > host_max:
        raise ProspectiveEventResponseV5Error("clock_observation_stale_or_future")
    source_generated = _utc(row["clock_observation_source_generated_utc"], path="clock.clock_observation_source_generated_utc")
    if row["source_generated_utc"] != row["clock_observation_source_generated_utc"]:
        raise ProspectiveEventResponseV5Error("clock_source_generated_alias_disagrees")
    semantic_age = (at - source_generated).total_seconds()
    if semantic_age < 0 or (semantic_freshness_required and semantic_age > float(contract["semantic_catalog_max_age_sec"])):
        raise ProspectiveEventResponseV5Error("clock_semantic_catalog_stale_or_future")
    hash_groups = (
        ("events_sha256", "semantic_snapshot_events_sha256", "clock_observation_events_sha256"),
        ("manifest_sha256", "semantic_snapshot_manifest_sha256", "clock_observation_manifest_sha256"),
    )
    for group in hash_groups:
        values = [row[key] for key in group]
        if any(not _is_sha256(value) for value in values) or len(set(values)) != 1:
            raise ProspectiveEventResponseV5Error("clock_internal_hash_aliases_disagree")
    if not _is_sha256(row["clock_observation_semantic_clock_sha256"]):
        raise ProspectiveEventResponseV5Error("clock_semantic_clock_hash_invalid")
    events = row["events"]
    if not isinstance(events, list) or _integer(row["event_count"], path="clock.event_count") != len(events):
        raise ProspectiveEventResponseV5Error("clock_event_count_mismatch")
    validated: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        try:
            validated.append(_validate_event(event, index=index, planned=at))
        except ProspectiveEventResponseV5Error:
            # The complete Clock payload stays bound, but ineligible rows are
            # kept out of the prospective admission set by the caller.
            continue
    return {
        "clock_snapshot_payload_sha256": _sha256_json(row),
        "clock_events_payload_sha256": _sha256_json(events),
        "clock_top_level_payload_sha256": _sha256_json({key: value for key, value in row.items() if key != "events"}),
        "clock_snapshot_id": row["snapshot_id"],
        "clock_observation_id": row["clock_observation_id"],
        "clock_observed_utc": _iso(observed),
        "clock_observation_age_sec": round(observed_age, 6),
        "clock_semantic_source_generated_utc": _iso(source_generated),
        "clock_semantic_age_sec": round(semantic_age, 6),
        "clock_attestation_generated_utc": internal["generated_utc"],
        "clock_attestation_age_sec": internal["age_sec"],
        "snapshot_attestation_generated_utc": snapshot_attestation["generated_utc"],
        "clock_internal_events_sha256": row["events_sha256"],
        "clock_internal_manifest_sha256": row["manifest_sha256"],
        "clock_semantic_clock_sha256": row["clock_observation_semantic_clock_sha256"],
        "validated_events": validated,
    }


_CLOCK_PROVENANCE_FIELDS = frozenset(
    {
        "source_kind", "clock_database_resolved_path", "clock_database_device",
        "clock_database_file_id", "clock_database_size", "reconstructed_at_utc",
        "clock_snapshot_payload_sha256", "clock_snapshot_id", "clock_observation_id",
    }
)


def validate_clock_reconstruction_provenance(
    snapshot: Mapping[str, Any],
    provenance: Mapping[str, Any],
    *,
    at_utc: dt.datetime,
) -> dict[str, Any]:
    row = _closed_mapping(
        provenance, path="clock_provenance", fields=_CLOCK_PROVENANCE_FIELDS
    )
    _reject_forbidden_nested_keys(row, path="clock_provenance")
    if row["source_kind"] != "immutable_clock_v2_reconstruct":
        raise ProspectiveEventResponseV5Error("clock_provenance_source_kind_mismatch")
    _string(row["clock_database_resolved_path"], path="clock_provenance.clock_database_resolved_path")
    for key in ("clock_database_device", "clock_database_file_id", "clock_database_size"):
        if _integer(row[key], path=f"clock_provenance.{key}") < 0:
            raise ProspectiveEventResponseV5Error(f"clock_provenance.{key}_negative")
    reconstructed = _utc(row["reconstructed_at_utc"], path="clock_provenance.reconstructed_at_utc")
    age = (at_utc.astimezone(UTC) - reconstructed).total_seconds()
    if age < 0 or age > 90:
        raise ProspectiveEventResponseV5Error("clock_reconstruction_provenance_stale_or_future")
    expected_hash = _sha256_json(snapshot)
    if row["clock_snapshot_payload_sha256"] != expected_hash:
        raise ProspectiveEventResponseV5Error("clock_reconstruction_payload_binding_mismatch")
    if row["clock_snapshot_id"] != snapshot.get("snapshot_id") or row["clock_observation_id"] != snapshot.get("clock_observation_id"):
        raise ProspectiveEventResponseV5Error("clock_reconstruction_identity_binding_mismatch")
    return {
        **dict(row),
        "clock_reconstruction_age_sec": round(age, 6),
        "clock_provenance_sha256": _sha256_json(row),
    }


def reconstruct_clock_v2_evidence(database_path: Path, *, cutoff_utc: dt.datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    """Execute Clock V2 and bind the complete verified reconstruction."""

    path = database_path.resolve(strict=True)
    before = path.stat()
    snapshot = reconstruct_clock_as_of(path, cutoff_utc.astimezone(UTC))
    after = path.stat()
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise ProspectiveEventResponseV5Error("clock_database_identity_changed_during_reconstruction")
    evidence = {
        "source_kind": "immutable_clock_v2_reconstruct",
        "clock_database_resolved_path": str(path),
        "clock_database_device": int(after.st_dev),
        "clock_database_file_id": int(after.st_ino),
        "clock_database_size": int(after.st_size),
        "reconstructed_at_utc": _iso(cutoff_utc),
        "clock_snapshot_payload_sha256": _sha256_json(snapshot),
        "clock_snapshot_id": snapshot.get("snapshot_id"),
        "clock_observation_id": snapshot.get("clock_observation_id"),
    }
    return snapshot, evidence


_POLICY_TOP_FIELDS = frozenset({"schema_version", "contract_id", "research_only", "execution_eligible", "dependencies", "explicit_non_dependencies"})
_POLICY_DEP_FIELDS = frozenset({"dependent_currency", "driver_currency", "driver_event_category", "authority", "mechanism", "interpretation", "official_reference", "timing_policy", "assign_direction"})
_POLICY_NONDEP_FIELDS = frozenset({"currency", "authority", "mechanism", "reason"})


def validate_policy_dependency_registry(value: Mapping[str, Any]) -> str:
    row = _closed_mapping(value, path="policy_registry", fields=_POLICY_TOP_FIELDS)
    _reject_forbidden_nested_keys(row, path="policy_registry", allowed_exact=frozenset({"execution_eligible", "assign_direction"}))
    if _integer(row["schema_version"], path="policy_registry.schema_version") != 1 or row["contract_id"] != EXPECTED_DEPENDENCY_CONTRACT_ID:
        raise ProspectiveEventResponseV5Error("policy_registry_contract_mismatch")
    if _boolean(row["research_only"], path="policy_registry.research_only") is not True or _boolean(row["execution_eligible"], path="policy_registry.execution_eligible") is not False:
        raise ProspectiveEventResponseV5Error("policy_registry_safety_mismatch")
    dependencies = row["dependencies"]
    nondeps = row["explicit_non_dependencies"]
    if not isinstance(dependencies, list) or not isinstance(nondeps, list):
        raise ProspectiveEventResponseV5Error("policy_registry_rows_not_lists")
    for index, item in enumerate(dependencies):
        dep = _closed_mapping(item, path=f"policy_registry.dependencies[{index}]", fields=_POLICY_DEP_FIELDS)
        for key in _POLICY_DEP_FIELDS - {"assign_direction"}:
            _string(dep[key], path=f"policy_registry.dependencies[{index}].{key}")
        if _boolean(dep["assign_direction"], path=f"policy_registry.dependencies[{index}].assign_direction") is not False:
            raise ProspectiveEventResponseV5Error("policy_registry_assigns_direction")
    for index, item in enumerate(nondeps):
        dep = _closed_mapping(item, path=f"policy_registry.explicit_non_dependencies[{index}]", fields=_POLICY_NONDEP_FIELDS)
        for key in _POLICY_NONDEP_FIELDS:
            _string(dep[key], path=f"policy_registry.explicit_non_dependencies[{index}].{key}")
    return _sha256_json(row)


_COHORT_IDENTITY_FIELDS = frozenset(
    {
        "contract_id", "cohort_id", "contract_payload_sha256", "closure_manifest_sha256",
        "source_dependency_sha256", "policy_dependency_registry_sha256",
        "instrument_universe_sha256", "clock_contract_id", "clock_schema_version",
        "research_only", "execution_eligible", "can_place_orders", "promotion_eligible",
        "authorization_eligible", "supported_execution_decision",
    }
)


def validate_cohort_identity(value: Mapping[str, Any], *, policy_registry_sha256: str) -> dict[str, Any]:
    row = _closed_mapping(value, path="cohort_identity", fields=_COHORT_IDENTITY_FIELDS)
    _reject_forbidden_nested_keys(
        row,
        path="cohort_identity",
        allowed_exact=_GENERATED_SAFE_ALIAS_KEYS,
    )
    if row["contract_id"] != CONTRACT_ID or row["cohort_id"] != COHORT_ID:
        raise ProspectiveEventResponseV5Error("cohort_identity_contract_mismatch")
    for key in ("contract_payload_sha256", "closure_manifest_sha256"):
        if not _is_sha256(row[key]):
            raise ProspectiveEventResponseV5Error(f"cohort_identity_{key}_invalid")
    dependencies = row["source_dependency_sha256"]
    if not isinstance(dependencies, Mapping) or not dependencies:
        raise ProspectiveEventResponseV5Error("cohort_source_dependencies_missing")
    if any(type(key) is not str or not key or not _is_sha256(item) for key, item in dependencies.items()):
        raise ProspectiveEventResponseV5Error("cohort_source_dependency_hash_invalid")
    if row["policy_dependency_registry_sha256"] != policy_registry_sha256:
        raise ProspectiveEventResponseV5Error("cohort_policy_dependency_hash_mismatch")
    if row["instrument_universe_sha256"] != CANONICAL_INSTRUMENT_UNIVERSE_SHA256:
        raise ProspectiveEventResponseV5Error("cohort_instrument_universe_mismatch")
    if row["clock_contract_id"] != CLOCK_CONTRACT_ID or row["clock_schema_version"] != CLOCK_SCHEMA_VERSION:
        raise ProspectiveEventResponseV5Error("cohort_clock_contract_mismatch")
    _validate_guard(row)
    result = dict(row)
    result["cohort_identity_sha256"] = _sha256_json(row)
    return result


def _event_groups(snapshot: Mapping[str, Any], *, planned: dt.datetime) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    raw_events = snapshot.get("events")
    if not isinstance(raw_events, list):
        raise ProspectiveEventResponseV5Error("clock_events_not_list")
    groups: dict[tuple[str, str], list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    attempts: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_events):
        if not isinstance(raw, Mapping):
            attempts.append({"status": "event_rejected_failed_closed", "reason": "event_not_mapping", "source_ref": str(index), **_research_guard()})
            continue
        upstream = raw.get("upstream_event_id")
        scheduled = raw.get("scheduled_utc")
        if type(upstream) is not str or type(scheduled) is not str:
            attempts.append({"status": "event_rejected_failed_closed", "reason": "natural_event_identity_missing", "source_ref": str(index), **_research_guard()})
            continue
        try:
            scheduled_iso = _iso(_utc(scheduled, path=f"clock.events[{index}].scheduled_utc"))
        except ProspectiveEventResponseV5Error as exc:
            attempts.append({"status": "event_rejected_failed_closed", "reason": str(exc), "source_ref": str(index), **_research_guard()})
            continue
        groups[(upstream, scheduled_iso)].append((index, raw))
    accepted: dict[str, dict[str, Any]] = {}
    for (upstream, scheduled), rows in sorted(groups.items()):
        natural_key = _id("natural_official_event_v5", {"upstream_event_id": upstream, "scheduled_utc": scheduled})
        if len(rows) != 1:
            attempts.append({
                "status": "event_rejected_failed_closed",
                "reason": "duplicate_natural_event_conflict",
                "source_ref": natural_key,
                "duplicate_count": len(rows),
                **_research_guard(),
            })
            continue
        index, raw = rows[0]
        try:
            event = _validate_event(raw, index=index, planned=planned)
        except ProspectiveEventResponseV5Error as exc:
            attempts.append({"status": "event_rejected_failed_closed", "reason": str(exc), "source_ref": natural_key, **_research_guard()})
            continue
        accepted[natural_key] = event
    return accepted, attempts


def build_event_plans(
    clock_snapshot: Mapping[str, Any],
    *,
    clock_provenance: Mapping[str, Any],
    contract: Mapping[str, Any],
    policy_dependency_registry: Mapping[str, Any],
    cohort_identity: Mapping[str, Any],
    planned_utc: dt.datetime,
) -> dict[str, Any]:
    cohort_start = validate_temp_contract(contract)
    planned = planned_utc.astimezone(UTC)
    if planned < cohort_start:
        raise ProspectiveEventResponseV5Error("planning_before_cohort_start")
    clock = validate_clock_v2_envelope(clock_snapshot, at_utc=planned, semantic_freshness_required=True, contract=contract)
    provenance = validate_clock_reconstruction_provenance(
        clock_snapshot, clock_provenance, at_utc=planned
    )
    policy_hash = validate_policy_dependency_registry(policy_dependency_registry)
    identity = validate_cohort_identity(cohort_identity, policy_registry_sha256=policy_hash)
    accepted, attempts = _event_groups(clock_snapshot, planned=planned)
    dependencies = policy_dependency_registry["dependencies"]
    offsets = contract["target_offsets_sec"]
    plans: list[dict[str, Any]] = []
    for natural_key, event in sorted(accepted.items()):
        scheduled = _utc(event["scheduled_utc"], path="event.scheduled_utc")
        if scheduled < cohort_start:
            attempts.append({"status": "event_rejected_failed_closed", "reason": "event_before_cohort_start", "source_ref": natural_key, **_research_guard()})
            continue
        lead = (scheduled - planned).total_seconds()
        if lead < float(contract["minimum_schedule_lead_sec"]) or lead > float(contract["maximum_planning_horizon_days"]) * 86400:
            attempts.append({"status": "event_rejected_failed_closed", "reason": "event_outside_planning_horizon", "source_ref": natural_key, **_research_guard()})
            continue
        direct = set(event["direct_currencies"])
        linked = {
            dep["dependent_currency"]
            for dep in dependencies
            if dep["driver_currency"] in direct and dep["driver_event_category"] == event["category"]
        }
        event_instance_id = _id("official_event_instance_v5", {"cohort_id": COHORT_ID, "natural_event_key": natural_key, "event_payload_sha256": event["event_payload_sha256"]})
        factor_id = _id("official_event_factor_v5", {"event_instance_id": event_instance_id, "driver_currencies": sorted(direct)})
        for instrument in CANONICAL_INSTRUMENTS:
            legs = set(instrument.split("_"))
            relationship = "direct_leg" if legs.intersection(direct) else "linked_policy_dependency" if legs.intersection(linked) else "unaffected_control"
            for offset in offsets:
                target = scheduled + dt.timedelta(seconds=offset)
                natural_sample_key = _id("natural_event_response_sample_v5", {"upstream_event_id": event["upstream_event_id"], "scheduled_utc": event["scheduled_utc"], "instrument": instrument, "horizon_sec": offset})
                plan_identity = {"cohort_identity_sha256": identity["cohort_identity_sha256"], "natural_sample_key": natural_sample_key}
                plans.append({
                    "plan_id": _id("event_response_plan_v5", plan_identity),
                    "natural_sample_key": natural_sample_key,
                    "natural_event_key": natural_key,
                    "contract_id": CONTRACT_ID,
                    "cohort_id": COHORT_ID,
                    "cohort_identity_sha256": identity["cohort_identity_sha256"],
                    "event_instance_id": event_instance_id,
                    "event_version_id": event["event_version_id"],
                    "upstream_event_id": event["upstream_event_id"],
                    "scheduled_utc": event["scheduled_utc"],
                    "instrument": instrument,
                    "target_offset_sec": offset,
                    "target_utc": _iso(target),
                    "sample_role": "baseline" if offset == 0 else "outcome",
                    "relationship": relationship,
                    "underlying_factor_id": factor_id,
                    "event_payload_sha256": event["event_payload_sha256"],
                    "frozen_event_payload_json": _canonical_json({key: value for key, value in event.items() if key not in {"event_payload_sha256", "natural_event_key"}}),
                    "planning_clock_snapshot_payload_sha256": clock["clock_snapshot_payload_sha256"],
                    "planning_clock_events_payload_sha256": clock["clock_events_payload_sha256"],
                    "planning_clock_top_level_payload_sha256": clock["clock_top_level_payload_sha256"],
                    "planning_clock_snapshot_id": clock["clock_snapshot_id"],
                    "planning_clock_observation_id": clock["clock_observation_id"],
                    "planning_clock_provenance_sha256": provenance["clock_provenance_sha256"],
                    "planning_clock_database_device": provenance["clock_database_device"],
                    "planning_clock_database_file_id": provenance["clock_database_file_id"],
                    "planning_semantic_source_generated_utc": clock["clock_semantic_source_generated_utc"],
                    "planning_semantic_age_sec": clock["clock_semantic_age_sec"],
                    "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
                    "policy_dependency_registry_sha256": policy_hash,
                    **_research_guard(),
                })
    plans.sort(key=lambda item: (item["target_utc"], item["natural_sample_key"]))
    if len({row["natural_sample_key"] for row in plans}) != len(plans):
        raise ProspectiveEventResponseV5Error("natural_plan_identity_collision")
    duplicate_conflict = any(
        item.get("reason") == "duplicate_natural_event_conflict" for item in attempts
    )
    return {
        "status": (
            "planning_failed_closed"
            if duplicate_conflict
            else "plans_ready" if plans else "no_eligible_exact_events"
        ),
        "plans": plans,
        "attempts": attempts,
        "plan_count": len(plans),
        "instrument_count": len(CANONICAL_INSTRUMENTS),
        "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
        "policy_dependency_registry_sha256": policy_hash,
        "clock_binding": {key: value for key, value in clock.items() if key != "validated_events"},
        "clock_provenance": provenance,
        "cohort_identity_sha256": identity["cohort_identity_sha256"],
        **_research_guard(),
    }


_QUOTE_TOP_FIELDS = frozenset({"schema_version", "generated_utc", "quotes"})
_QUOTE_FIELDS = frozenset({"bid", "ask", "pip", "source", "time"})
_MARKET_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version", "state", "attested", "artifact_name", "payload_sha256",
        "byte_length", "generated_utc", "quote_snapshot_generated_utc",
        "instrument_universe_sha256", "instrument_count",
    }
)


def quote_snapshot_candidates(
    artifact_payload_bytes: bytes,
    *,
    artifact_name: str,
    market_attestation: Mapping[str, Any],
    contract: Mapping[str, Any],
    collector_observed_utc: dt.datetime,
    cohort_identity_sha256: str,
) -> list[dict[str, Any]]:
    validate_temp_contract(contract)
    if type(artifact_payload_bytes) is not bytes:
        raise ProspectiveEventResponseV5Error("quote_artifact_must_be_exact_bytes")
    _string(artifact_name, path="quote_artifact_name")
    try:
        payload = json.loads(artifact_payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveEventResponseV5Error("quote_artifact_invalid_json") from exc
    row = _closed_mapping(payload, path="quote_artifact", fields=_QUOTE_TOP_FIELDS)
    attestation = _closed_mapping(market_attestation, path="market_attestation", fields=_MARKET_ATTESTATION_FIELDS)
    _reject_forbidden_nested_keys(attestation, path="market_attestation")
    payload_hash = _sha256_bytes(artifact_payload_bytes)
    if _integer(attestation["schema_version"], path="market_attestation.schema_version") != 1:
        raise ProspectiveEventResponseV5Error("market_attestation_schema_mismatch")
    if attestation["state"] != "fresh_trusted" or _boolean(attestation["attested"], path="market_attestation.attested") is not True:
        raise ProspectiveEventResponseV5Error("market_attestation_not_trusted")
    if attestation["artifact_name"] != artifact_name or attestation["payload_sha256"] != payload_hash or _integer(attestation["byte_length"], path="market_attestation.byte_length") != len(artifact_payload_bytes):
        raise ProspectiveEventResponseV5Error("market_attestation_artifact_binding_mismatch")
    if attestation["instrument_universe_sha256"] != CANONICAL_INSTRUMENT_UNIVERSE_SHA256 or _integer(attestation["instrument_count"], path="market_attestation.instrument_count") != 68:
        raise ProspectiveEventResponseV5Error("market_attestation_universe_mismatch")
    collector = collector_observed_utc.astimezone(UTC)
    generated = _utc(row["generated_utc"], path="quote_artifact.generated_utc")
    if attestation["quote_snapshot_generated_utc"] != row["generated_utc"]:
        raise ProspectiveEventResponseV5Error("market_attestation_snapshot_clock_mismatch")
    attested = _utc(attestation["generated_utc"], path="market_attestation.generated_utc")
    for name, value, maximum in (
        ("quote_snapshot", generated, float(contract["sampling"]["maximum_snapshot_age_at_collection_sec"])),
        ("market_attestation", attested, float(contract["sampling"]["maximum_market_attestation_age_sec"])),
    ):
        age = (collector - value).total_seconds()
        if age < 0 or age > maximum:
            raise ProspectiveEventResponseV5Error(f"{name}_stale_or_future")
    quotes = row["quotes"]
    if not isinstance(quotes, Mapping) or set(quotes) != set(CANONICAL_INSTRUMENTS) or len(quotes) != 68:
        raise ProspectiveEventResponseV5Error("quote_artifact_requires_exact_68_coverage")
    observations: list[dict[str, Any]] = []
    for instrument in CANONICAL_INSTRUMENTS:
        quote = _closed_mapping(quotes[instrument], path=f"quotes.{instrument}", fields=_QUOTE_FIELDS)
        bid = _number(quote["bid"], path=f"quotes.{instrument}.bid")
        ask = _number(quote["ask"], path=f"quotes.{instrument}.ask")
        pip = _number(quote["pip"], path=f"quotes.{instrument}.pip")
        _string(quote["source"], path=f"quotes.{instrument}.source")
        quote_time = _utc(quote["time"], path=f"quotes.{instrument}.time")
        quote_age = (collector - quote_time).total_seconds()
        if bid <= 0 or ask <= bid or pip <= 0 or quote_age < 0 or quote_age > float(contract["sampling"]["maximum_quote_age_at_collection_sec"]):
            raise ProspectiveEventResponseV5Error(f"quote_invalid_or_stale:{instrument}")
        identity = {"artifact_sha256": payload_hash, "instrument": instrument, "quote_time_utc": _iso(quote_time), "bid": bid, "ask": ask}
        observations.append({
            "source_type": "live_executable_quote",
            "source_record_id": _id("live_quote_v5", identity),
            "cohort_identity_sha256": cohort_identity_sha256,
            "source_payload_sha256": payload_hash,
            "source_artifact_name": artifact_name,
            "source_artifact_byte_length": len(artifact_payload_bytes),
            "instrument": instrument,
            "collector_observed_utc": _iso(collector),
            "source_generated_utc": _iso(generated),
            "quote_time_utc": _iso(quote_time),
            "observation_known_utc": _iso(max(collector, generated, quote_time, attested)),
            "bid": bid, "ask": ask, "pip": pip,
            "market_attestation_generated_utc": _iso(attested),
            "market_attestation_artifact_name": artifact_name,
            "market_attestation_payload_sha256": payload_hash,
            "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
            **_research_guard(),
        })
    return observations


_PLAN_FIELDS = frozenset(
    {
        "plan_id", "natural_sample_key", "natural_event_key", "contract_id", "cohort_id",
        "cohort_identity_sha256", "event_instance_id", "event_version_id",
        "upstream_event_id", "scheduled_utc", "instrument", "target_offset_sec",
        "target_utc", "sample_role", "relationship", "underlying_factor_id",
        "event_payload_sha256", "frozen_event_payload_json",
        "planning_clock_snapshot_payload_sha256", "planning_clock_events_payload_sha256",
        "planning_clock_top_level_payload_sha256", "planning_clock_snapshot_id",
        "planning_clock_observation_id", "planning_clock_provenance_sha256",
        "planning_clock_database_device", "planning_clock_database_file_id",
        "planning_semantic_source_generated_utc", "planning_semantic_age_sec",
        "instrument_universe_sha256", "policy_dependency_registry_sha256",
        "research_only", "execution_eligible", "can_place_orders", "promotion_eligible",
        "authorization_eligible", "supported_execution_decision",
    }
)
_OBSERVATION_FIELDS = frozenset(
    {
        "source_type", "source_record_id", "cohort_identity_sha256",
        "source_payload_sha256", "source_artifact_name", "source_artifact_byte_length",
        "instrument", "collector_observed_utc", "source_generated_utc", "quote_time_utc",
        "observation_known_utc", "bid", "ask", "pip", "market_attestation_generated_utc",
        "market_attestation_artifact_name", "market_attestation_payload_sha256",
        "instrument_universe_sha256", "research_only", "execution_eligible",
        "can_place_orders", "promotion_eligible", "authorization_eligible",
        "supported_execution_decision",
    }
)
_SAMPLE_FIELDS = frozenset(
    {
        "sample_id", "plan_id", "natural_sample_key", "contract_id", "cohort_id",
        "cohort_identity_sha256", "event_instance_id", "natural_event_key",
        "event_version_id", "upstream_event_id", "instrument", "target_offset_sec",
        "target_utc", "sample_role", "event_lineage_state", "event_payload_sha256",
        "planning_clock_snapshot_payload_sha256", "planning_clock_events_payload_sha256",
        "planning_clock_top_level_payload_sha256", "planning_clock_snapshot_id",
        "planning_clock_observation_id", "planning_clock_provenance_sha256",
        "planning_clock_database_device", "planning_clock_database_file_id",
        "planning_semantic_source_generated_utc", "planning_semantic_age_sec",
        "baseline_semantic_source_generated_utc", "collection_clock_snapshot_payload_sha256",
        "collection_clock_provenance_sha256", "quote_time_utc", "observation_known_utc",
        "target_distance_sec", "bid", "ask", "mid", "pip", "spread_pips",
        "source_record_id", "source_payload_sha256", "source_artifact_name",
        "policy_dependency_registry_sha256", "instrument_universe_sha256",
        "research_only", "execution_eligible", "can_place_orders", "promotion_eligible",
        "authorization_eligible", "supported_execution_decision",
    }
)


def _validate_plan(plan: Mapping[str, Any], *, identity_sha: str, policy_hash: str) -> None:
    _closed_mapping(plan, path="plan", fields=_PLAN_FIELDS)
    _reject_forbidden_nested_keys(
        plan,
        path="plan",
        allowed_exact=_GENERATED_SAFE_ALIAS_KEYS,
    )
    _validate_guard(plan)
    if plan.get("contract_id") != CONTRACT_ID or plan.get("cohort_id") != COHORT_ID or plan.get("cohort_identity_sha256") != identity_sha:
        raise ProspectiveEventResponseV5Error("plan_cohort_dependency_drift")
    if plan.get("policy_dependency_registry_sha256") != policy_hash or plan.get("instrument_universe_sha256") != CANONICAL_INSTRUMENT_UNIVERSE_SHA256:
        raise ProspectiveEventResponseV5Error("plan_static_dependency_drift")
    frozen = _string(plan.get("frozen_event_payload_json"), path="plan.frozen_event_payload_json")
    decoded = json.loads(frozen)
    if _sha256_json(decoded) != plan.get("event_payload_sha256"):
        raise ProspectiveEventResponseV5Error("plan_frozen_event_payload_hash_mismatch")
    expected_natural = _id("natural_event_response_sample_v5", {"upstream_event_id": plan.get("upstream_event_id"), "scheduled_utc": plan.get("scheduled_utc"), "instrument": plan.get("instrument"), "horizon_sec": plan.get("target_offset_sec")})
    if plan.get("natural_sample_key") != expected_natural:
        raise ProspectiveEventResponseV5Error("plan_natural_identity_mismatch")


def build_samples(
    plans: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]],
    *,
    existing_samples: Sequence[Mapping[str, Any]],
    current_clock_snapshot: Mapping[str, Any],
    current_clock_provenance: Mapping[str, Any],
    contract: Mapping[str, Any],
    policy_dependency_registry: Mapping[str, Any],
    cohort_identity: Mapping[str, Any],
    collector_observed_utc: dt.datetime,
) -> dict[str, Any]:
    validate_temp_contract(contract)
    collector = collector_observed_utc.astimezone(UTC)
    clock = validate_clock_v2_envelope(current_clock_snapshot, at_utc=collector, semantic_freshness_required=False, contract=contract)
    provenance = validate_clock_reconstruction_provenance(
        current_clock_snapshot, current_clock_provenance, at_utc=collector
    )
    policy_hash = validate_policy_dependency_registry(policy_dependency_registry)
    identity = validate_cohort_identity(cohort_identity, policy_registry_sha256=policy_hash)
    identity_sha = identity["cohort_identity_sha256"]
    existing_by_plan = {row.get("plan_id"): row for row in existing_samples}
    for index, row in enumerate(existing_samples):
        _closed_mapping(row, path=f"existing_samples[{index}]", fields=_SAMPLE_FIELDS)
        _reject_forbidden_nested_keys(
            row,
            path=f"existing_samples[{index}]",
            allowed_exact=_GENERATED_SAFE_ALIAS_KEYS,
        )
    baselines = {(row.get("event_instance_id"), row.get("instrument")): row for row in existing_samples if row.get("target_offset_sec") == 0}
    current_events, duplicate_attempts = _event_groups(current_clock_snapshot, planned=collector)
    by_instrument: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in observations:
        _closed_mapping(row, path="observation", fields=_OBSERVATION_FIELDS)
        _validate_guard(row)
        _reject_forbidden_nested_keys(
            row,
            path="observation",
            allowed_exact=_GENERATED_SAFE_ALIAS_KEYS,
        )
        by_instrument[str(row.get("instrument") or "")].append(row)
    output: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = list(duplicate_attempts)
    late = float(contract["sampling"]["maximum_target_lateness_sec"])
    lead = float(contract["sampling"]["maximum_quote_lead_sec"])
    max_quote_age = float(contract["sampling"]["maximum_quote_age_at_collection_sec"])
    max_snapshot_age = float(contract["sampling"]["maximum_snapshot_age_at_collection_sec"])
    max_attestation_age = float(contract["sampling"]["maximum_market_attestation_age_sec"])
    max_spread = float(contract["sampling"]["maximum_spread_pips_sanity"])
    for plan in plans:
        _validate_plan(plan, identity_sha=identity_sha, policy_hash=policy_hash)
        if plan["plan_id"] in existing_by_plan:
            continue
        target = _utc(plan["target_utc"], path="plan.target_utc")
        offset = _integer(plan["target_offset_sec"], path="plan.target_offset_sec")
        if collector < target:
            continue
        if (collector - target).total_seconds() > late:
            attempts.append({
                "status": "baseline_missed_failed_closed" if offset == 0 else "endpoint_missed_failed_closed",
                "reason": "baseline_window_missed" if offset == 0 else "endpoint_window_missed",
                "plan_id": plan["plan_id"], "natural_sample_key": plan["natural_sample_key"],
                "cohort_identity_sha256": identity_sha, **_research_guard(),
            })
            continue
        key = (plan["event_instance_id"], plan["instrument"])
        if offset == 0:
            # Baseline alone requires a current semantic catalog and exact
            # event payload.  Planning-time semantic timestamps remain on the
            # sample and are never replaced by this current snapshot.
            validate_clock_v2_envelope(current_clock_snapshot, at_utc=collector, semantic_freshness_required=True, contract=contract)
            current = current_events.get(plan["natural_event_key"])
            if current is None or current["event_payload_sha256"] != plan["event_payload_sha256"]:
                attempts.append({"status": "baseline_rejected_failed_closed", "reason": "event_payload_changed_or_duplicate_before_baseline", "plan_id": plan["plan_id"], "natural_sample_key": plan["natural_sample_key"], "cohort_identity_sha256": identity_sha, **_research_guard()})
                continue
            lineage_state = "baseline_exact_full_event_payload"
            baseline_semantic_source = clock["clock_semantic_source_generated_utc"]
        else:
            baseline = baselines.get(key)
            if baseline is None:
                attempts.append({"status": "endpoint_waiting_failed_closed", "reason": "endpoint_waiting_for_valid_baseline", "plan_id": plan["plan_id"], "natural_sample_key": plan["natural_sample_key"], "cohort_identity_sha256": identity_sha, **_research_guard()})
                continue
            if baseline.get("event_payload_sha256") != plan["event_payload_sha256"] or baseline.get("cohort_identity_sha256") != identity_sha:
                raise ProspectiveEventResponseV5Error("baseline_to_endpoint_dependency_drift")
            lineage_state = "postbaseline_frozen_exact_full_event_payload"
            baseline_semantic_source = baseline.get("baseline_semantic_source_generated_utc")
        valid: list[tuple[dt.datetime, str, Mapping[str, Any]]] = []
        reasons: Counter[str] = Counter()
        for observation in by_instrument.get(plan["instrument"], []):
            if observation.get("cohort_identity_sha256") != identity_sha:
                reasons["quote_cohort_dependency_drift"] += 1; continue
            if observation.get("source_type") != "live_executable_quote":
                reasons["quote_source_type_invalid"] += 1; continue
            if observation.get("instrument_universe_sha256") != CANONICAL_INSTRUMENT_UNIVERSE_SHA256:
                reasons["quote_universe_drift"] += 1; continue
            if observation.get("market_attestation_artifact_name") != observation.get("source_artifact_name") or observation.get("market_attestation_payload_sha256") != observation.get("source_payload_sha256"):
                reasons["quote_artifact_attestation_binding_drift"] += 1; continue
            if not _is_sha256(observation.get("source_payload_sha256")):
                reasons["quote_source_payload_hash_invalid"] += 1; continue
            try:
                if _integer(observation.get("source_artifact_byte_length"), path="quote.source_artifact_byte_length") <= 0:
                    reasons["quote_artifact_length_invalid"] += 1; continue
            except ProspectiveEventResponseV5Error:
                reasons["quote_artifact_length_invalid"] += 1; continue
            try:
                quote_time = _utc(observation.get("quote_time_utc"), path="quote.quote_time_utc")
                generated = _utc(observation.get("source_generated_utc"), path="quote.source_generated_utc")
                observed = _utc(observation.get("collector_observed_utc"), path="quote.collector_observed_utc")
                attested = _utc(observation.get("market_attestation_generated_utc"), path="quote.market_attestation_generated_utc")
                known = _utc(observation.get("observation_known_utc"), path="quote.observation_known_utc")
                bid = _number(observation.get("bid"), path="quote.bid")
                ask = _number(observation.get("ask"), path="quote.ask")
                pip = _number(observation.get("pip"), path="quote.pip")
            except ProspectiveEventResponseV5Error:
                reasons["quote_schema_or_clock_invalid"] += 1; continue
            # Freshness is re-evaluated at selection, not inherited from the
            # earlier candidate-construction call.
            quote_age_at_selection = (collector - quote_time).total_seconds()
            if quote_age_at_selection < 0 or quote_age_at_selection > max_quote_age:
                reasons["quote_stale_at_selection"] += 1; continue
            snapshot_age = (collector - generated).total_seconds()
            attestation_age = (collector - attested).total_seconds()
            if snapshot_age < 0 or snapshot_age > max_snapshot_age:
                reasons["quote_snapshot_stale_at_selection"] += 1; continue
            if attestation_age < 0 or attestation_age > max_attestation_age:
                reasons["market_attestation_stale_at_selection"] += 1; continue
            if known != max(quote_time, generated, observed, attested):
                reasons["quote_known_clock_not_max"] += 1; continue
            alignment = (quote_time - target).total_seconds()
            if alignment < -lead or alignment > late:
                reasons["quote_outside_target_window"] += 1; continue
            if bid <= 0 or ask <= bid or pip <= 0 or (ask - bid) / pip > max_spread:
                reasons["quote_price_or_spread_invalid"] += 1; continue
            expected_record_id = _id(
                "live_quote_v5",
                {
                    "artifact_sha256": observation["source_payload_sha256"],
                    "instrument": observation["instrument"],
                    "quote_time_utc": _iso(quote_time),
                    "bid": bid,
                    "ask": ask,
                },
            )
            if observation.get("source_record_id") != expected_record_id:
                reasons["quote_record_identity_mismatch"] += 1; continue
            valid.append((known, observation["source_record_id"], observation))
        if not valid:
            attempts.append({"status": "sample_rejected_failed_closed", "reason": reasons.most_common(1)[0][0] if reasons else "quote_missing", "plan_id": plan["plan_id"], "natural_sample_key": plan["natural_sample_key"], "cohort_identity_sha256": identity_sha, **_research_guard()})
            continue
        valid.sort(key=lambda item: (item[0], item[1]))
        observation = valid[0][2]
        quote_time = _utc(observation["quote_time_utc"], path="selected_quote.quote_time_utc")
        sample_identity = {"cohort_identity_sha256": identity_sha, "natural_sample_key": plan["natural_sample_key"], "source_record_id": observation["source_record_id"]}
        sample = {
            "sample_id": _id("event_response_sample_v5", sample_identity),
            "plan_id": plan["plan_id"], "natural_sample_key": plan["natural_sample_key"],
            "contract_id": CONTRACT_ID, "cohort_id": COHORT_ID,
            "cohort_identity_sha256": identity_sha, "event_instance_id": plan["event_instance_id"],
            "natural_event_key": plan["natural_event_key"], "event_version_id": plan["event_version_id"],
            "upstream_event_id": plan["upstream_event_id"], "instrument": plan["instrument"],
            "target_offset_sec": offset, "target_utc": plan["target_utc"], "sample_role": plan["sample_role"],
            "event_lineage_state": lineage_state, "event_payload_sha256": plan["event_payload_sha256"],
            "planning_clock_snapshot_payload_sha256": plan["planning_clock_snapshot_payload_sha256"],
            "planning_clock_events_payload_sha256": plan["planning_clock_events_payload_sha256"],
            "planning_clock_top_level_payload_sha256": plan["planning_clock_top_level_payload_sha256"],
            "planning_clock_snapshot_id": plan["planning_clock_snapshot_id"],
            "planning_clock_observation_id": plan["planning_clock_observation_id"],
            "planning_clock_provenance_sha256": plan["planning_clock_provenance_sha256"],
            "planning_clock_database_device": plan["planning_clock_database_device"],
            "planning_clock_database_file_id": plan["planning_clock_database_file_id"],
            "planning_semantic_source_generated_utc": plan["planning_semantic_source_generated_utc"],
            "planning_semantic_age_sec": plan["planning_semantic_age_sec"],
            "baseline_semantic_source_generated_utc": baseline_semantic_source,
            "collection_clock_snapshot_payload_sha256": clock["clock_snapshot_payload_sha256"],
            "collection_clock_provenance_sha256": provenance["clock_provenance_sha256"],
            "quote_time_utc": _iso(quote_time), "observation_known_utc": observation["observation_known_utc"],
            "target_distance_sec": round((quote_time - target).total_seconds(), 6),
            "bid": observation["bid"], "ask": observation["ask"], "mid": (observation["bid"] + observation["ask"]) / 2.0,
            "pip": observation["pip"], "spread_pips": (observation["ask"] - observation["bid"]) / observation["pip"],
            "source_record_id": observation["source_record_id"], "source_payload_sha256": observation["source_payload_sha256"],
            "source_artifact_name": observation["source_artifact_name"],
            "policy_dependency_registry_sha256": policy_hash,
            "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
            **_research_guard(),
        }
        output.append(sample)
        if offset == 0:
            baselines[key] = sample
    failed = any(
        item.get("status")
        in {
            "baseline_missed_failed_closed",
            "baseline_rejected_failed_closed",
            "sample_rejected_failed_closed",
        }
        or item.get("reason") == "duplicate_natural_event_conflict"
        for item in attempts
    )
    return {
        "status": "sampling_failed_closed" if failed else "samples_captured" if output else "no_due_samples",
        "samples": output, "attempts": attempts, "sample_count": len(output),
        "cohort_identity_sha256": identity_sha, **_research_guard(),
    }


def mature_outcomes(samples: Sequence[Mapping[str, Any]], *, cohort_identity_sha256: str) -> list[dict[str, Any]]:
    baselines: dict[tuple[Any, Any], Mapping[str, Any]] = {}
    for index, row in enumerate(samples):
        _closed_mapping(row, path=f"samples[{index}]", fields=_SAMPLE_FIELDS)
        _validate_guard(row)
        _reject_forbidden_nested_keys(
            row, path=f"samples[{index}]", allowed_exact=_GENERATED_SAFE_ALIAS_KEYS
        )
        if row.get("cohort_identity_sha256") != cohort_identity_sha256:
            raise ProspectiveEventResponseV5Error("outcome_input_cohort_dependency_drift")
        if row.get("target_offset_sec") == 0:
            key = (row.get("event_instance_id"), row.get("instrument"))
            if key in baselines:
                raise ProspectiveEventResponseV5Error("duplicate_natural_baseline_conflict")
            baselines[key] = row
    outcomes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for endpoint in samples:
        horizon = endpoint.get("target_offset_sec")
        if type(horizon) is not int or horizon <= 0:
            continue
        baseline = baselines.get((endpoint.get("event_instance_id"), endpoint.get("instrument")))
        if baseline is None:
            continue
        if any(row.get("cohort_identity_sha256") != cohort_identity_sha256 for row in (baseline, endpoint)):
            raise ProspectiveEventResponseV5Error("outcome_cohort_dependency_drift")
        if baseline.get("event_payload_sha256") != endpoint.get("event_payload_sha256"):
            raise ProspectiveEventResponseV5Error("outcome_event_payload_drift")
        natural_key = _id("natural_event_response_outcome_v5", {"upstream_event_id": endpoint["upstream_event_id"], "scheduled_utc": endpoint["target_utc"] if horizon == 0 else baseline["target_utc"], "instrument": endpoint["instrument"], "horizon_sec": horizon})
        if natural_key in seen:
            raise ProspectiveEventResponseV5Error("duplicate_natural_outcome_conflict")
        seen.add(natural_key)
        pip = float(baseline["pip"])
        identity = {"cohort_identity_sha256": cohort_identity_sha256, "natural_outcome_key": natural_key}
        outcomes.append({
            "outcome_id": _id("event_response_outcome_v5", identity), "natural_outcome_key": natural_key,
            "contract_id": CONTRACT_ID, "cohort_id": COHORT_ID, "cohort_identity_sha256": cohort_identity_sha256,
            "event_instance_id": endpoint["event_instance_id"], "instrument": endpoint["instrument"], "horizon_sec": horizon,
            "baseline_sample_id": baseline["sample_id"], "outcome_sample_id": endpoint["sample_id"],
            "long_after_spread_pips": (float(endpoint["bid"]) - float(baseline["ask"])) / pip,
            "short_after_spread_pips": (float(baseline["bid"]) - float(endpoint["ask"])) / pip,
            "direction_selected": False, "best_side_metric": None, **_research_guard(),
        })
    return outcomes


def derive_top_status(*, planning_status: str, sampling_status: str) -> str:
    """Map any subordinate failed-closed state to a failed top-level state."""

    for value in (planning_status, sampling_status):
        if type(value) is not str or not value:
            return "temp_v5_failed_closed"
        if "failed_closed" in value:
            return "temp_v5_failed_closed"
    return "temp_v5_cycle_ok"


def _same_file_or_identity(left: Path, right: Path) -> bool:
    try:
        if left.resolve() == right.resolve():
            return True
    except OSError:
        pass
    if left.exists() and right.exists():
        try:
            if os.path.samefile(left, right):
                return True
        except OSError:
            pass
        left_stat, right_stat = left.stat(), right.stat()
        return (left_stat.st_dev, left_stat.st_ino) == (right_stat.st_dev, right_stat.st_ino)
    return False


def refuse_canonical_alias(path: Path, canonicals: Sequence[Path], *, field: str) -> None:
    for canonical in canonicals:
        if _same_file_or_identity(path, canonical):
            raise ProspectiveEventResponseV5Error(f"canonical_v5_{field}_samefile_forbidden")


_TEMP_TABLES = {"plans": "plan_id", "samples": "sample_id", "outcomes": "outcome_id", "attempts": "attempt_id"}


def open_temp_ledger(path: Path, *, canonical_paths: Sequence[Path], cohort_identity: Mapping[str, Any], policy_registry_sha256: str) -> sqlite3.Connection:
    refuse_canonical_alias(
        path,
        [EMBEDDED_CANONICAL_LEDGER, *canonical_paths],
        field="ledger",
    )
    identity = validate_cohort_identity(cohort_identity, policy_registry_sha256=policy_registry_sha256)
    if path.exists() and path.is_dir():
        raise ProspectiveEventResponseV5Error("temp_ledger_path_is_directory")
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS v5_temp_cohort_identity(identity_sha256 TEXT PRIMARY KEY,payload_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS v5_temp_plans(plan_id TEXT PRIMARY KEY,natural_sample_key TEXT UNIQUE NOT NULL,cohort_identity_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS v5_temp_samples(sample_id TEXT PRIMARY KEY,plan_id TEXT UNIQUE NOT NULL,natural_sample_key TEXT UNIQUE NOT NULL,cohort_identity_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS v5_temp_outcomes(outcome_id TEXT PRIMARY KEY,natural_outcome_key TEXT UNIQUE NOT NULL,cohort_identity_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS v5_temp_attempts(attempt_id TEXT PRIMARY KEY,cohort_identity_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL);
        """
    )
    for table in _TEMP_TABLES:
        connection.executescript(f"""
        CREATE TRIGGER IF NOT EXISTS v5_temp_{table}_no_update BEFORE UPDATE ON v5_temp_{table} BEGIN SELECT RAISE(ABORT,'append_only_v5_temp_ledger'); END;
        CREATE TRIGGER IF NOT EXISTS v5_temp_{table}_no_delete BEFORE DELETE ON v5_temp_{table} BEGIN SELECT RAISE(ABORT,'append_only_v5_temp_ledger'); END;
        """)
    payload = _canonical_json({key: value for key, value in identity.items() if key != "cohort_identity_sha256"})
    existing = connection.execute("SELECT identity_sha256,payload_json FROM v5_temp_cohort_identity").fetchall()
    if existing and (len(existing) != 1 or existing[0]["identity_sha256"] != identity["cohort_identity_sha256"] or existing[0]["payload_json"] != payload):
        connection.close()
        raise ProspectiveEventResponseV5Error("temp_ledger_cohort_identity_drift")
    if not existing:
        connection.execute("INSERT INTO v5_temp_cohort_identity(identity_sha256,payload_json) VALUES(?,?)", (identity["cohort_identity_sha256"], payload))
    connection.commit()
    return connection


def insert_temp_rows(connection: sqlite3.Connection, table: str, rows: Iterable[Mapping[str, Any]], *, cohort_identity_sha256: str) -> int:
    if table not in _TEMP_TABLES:
        raise ProspectiveEventResponseV5Error("unsupported_temp_table")
    key = _TEMP_TABLES[table]
    inserted = 0
    for source in rows:
        row = dict(source)
        if row.get("cohort_identity_sha256") != cohort_identity_sha256:
            raise ProspectiveEventResponseV5Error("temp_row_cohort_identity_drift")
        if key not in row:
            if table != "attempts":
                raise ProspectiveEventResponseV5Error(f"missing_{key}")
            row[key] = _id("event_response_attempt_v5", row)
        _validate_guard(row)
        payload = _canonical_json(row)
        payload_sha = _sha256_bytes(payload.encode("utf-8"))
        columns = [key]
        values: list[Any] = [row[key]]
        if table in {"plans", "samples"}:
            columns.append("natural_sample_key"); values.append(row["natural_sample_key"])
        if table == "samples":
            columns.append("plan_id"); values.append(row["plan_id"])
        if table == "outcomes":
            columns.append("natural_outcome_key"); values.append(row["natural_outcome_key"])
        columns += ["cohort_identity_sha256", "payload_sha256", "payload_json"]
        values += [cohort_identity_sha256, payload_sha, payload]
        existing = connection.execute(f"SELECT payload_sha256,payload_json FROM v5_temp_{table} WHERE {key}=?", (row[key],)).fetchone()
        if existing:
            if existing["payload_sha256"] != payload_sha or existing["payload_json"] != payload:
                raise ProspectiveEventResponseV5Error("temp_primary_key_content_collision")
            continue
        try:
            connection.execute(f"INSERT INTO v5_temp_{table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})", values)
        except sqlite3.IntegrityError as exc:
            raise ProspectiveEventResponseV5Error("temp_natural_identity_collision") from exc
        inserted += 1
    return inserted


def read_temp_rows(connection: sqlite3.Connection, table: str, *, cohort_identity_sha256: str) -> list[dict[str, Any]]:
    if table not in _TEMP_TABLES:
        raise ProspectiveEventResponseV5Error("unsupported_temp_table")
    key = _TEMP_TABLES[table]
    output: list[dict[str, Any]] = []
    for row in connection.execute(f"SELECT {key},cohort_identity_sha256,payload_sha256,payload_json FROM v5_temp_{table} ORDER BY {key}"):
        if row["cohort_identity_sha256"] != cohort_identity_sha256:
            raise ProspectiveEventResponseV5Error("temp_persisted_cohort_identity_drift")
        payload = row["payload_json"]
        if _sha256_bytes(payload.encode("utf-8")) != row["payload_sha256"]:
            raise ProspectiveEventResponseV5Error("temp_payload_hash_mismatch")
        decoded = json.loads(payload)
        if decoded.get(key) != row[key] or decoded.get("cohort_identity_sha256") != cohort_identity_sha256:
            raise ProspectiveEventResponseV5Error("temp_payload_identity_mismatch")
        _validate_guard(decoded)
        output.append(decoded)
    return output


def _resolve_local_import(root: Path, source: Path, node: ast.AST) -> set[Path]:
    candidates: set[Path] = set()
    if isinstance(node, ast.Import):
        names = [alias.name for alias in node.names]
    elif isinstance(node, ast.ImportFrom):
        if node.level:
            relative = source.relative_to(root / "src") if str(source).startswith(str(root / "src")) else source.relative_to(root)
            package = list(relative.with_suffix("").parts[:-1])
            if node.level > 1:
                package = package[: -(node.level - 1)]
            base = package + (node.module.split(".") if node.module else [])
            names = [".".join(base)] if node.module else [".".join(base + [alias.name]) for alias in node.names]
        else:
            names = [node.module] if node.module else []
    else:
        return candidates
    for name in names:
        if not name:
            continue
        parts = name.split(".")
        for base in (root, root / "src"):
            file_candidate = base.joinpath(*parts).with_suffix(".py")
            init_candidate = base.joinpath(*parts, "__init__.py")
            if file_candidate.is_file(): candidates.add(file_candidate.resolve())
            elif init_candidate.is_file(): candidates.add(init_candidate.resolve())
    return candidates


def discover_local_dependency_closure(root: Path, entry_paths: Sequence[str]) -> set[str]:
    root = root.resolve()
    pending = [(root / relative).resolve(strict=True) for relative in entry_paths]
    discovered: set[Path] = set()
    while pending:
        source = pending.pop()
        if source in discovered:
            continue
        discovered.add(source)
        if source.suffix != ".py":
            continue
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                pending.extend(_resolve_local_import(root, source, node) - discovered)
    return {path.relative_to(root).as_posix() for path in discovered}


_PENDING_CLOSURE_FIELDS = frozenset(
    {
        "schema_version", "contract_id", "cohort_id", "state", "review_state",
        "candidate_bytes_frozen_utc", "independent_reviewed_utc", "cohort_start_utc",
        "entry_paths", "files", "source_artifacts", "unresolved_external_dependencies",
    }
)


def verify_pending_closure(root: Path, manifest_path: Path, *, require_reviewed: bool = False) -> dict[str, Any]:
    if require_reviewed:
        raise ProspectiveEventResponseV5Error("candidate_cannot_self_assert_external_approval")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _closed_mapping(manifest, path="closure_manifest", fields=_PENDING_CLOSURE_FIELDS)
    if manifest["schema_version"] != 1 or manifest["contract_id"] != CONTRACT_ID or manifest["cohort_id"] != COHORT_ID:
        raise ProspectiveEventResponseV5Error("closure_manifest_identity_mismatch")
    if manifest["state"] != "pending_final_dependency_closure" or manifest["review_state"] != "blocked_external_dependencies":
        raise ProspectiveEventResponseV5Error("closure_manifest_pending_state_mismatch")
    if any(manifest[key] is not None for key in ("candidate_bytes_frozen_utc", "independent_reviewed_utc", "cohort_start_utc")):
        raise ProspectiveEventResponseV5Error("pending_closure_cannot_claim_chronology")
    unresolved = manifest["unresolved_external_dependencies"]
    if unresolved != ["official_fact_adapter_v3", "currency_state_official_context_v3"]:
        raise ProspectiveEventResponseV5Error("closure_unresolved_dependency_set_mismatch")
    entries = manifest["entry_paths"]
    if not isinstance(entries, list) or any(type(value) is not str for value in entries):
        raise ProspectiveEventResponseV5Error("closure_entry_paths_invalid")
    discovered = discover_local_dependency_closure(root, entries)
    declared_rows = manifest["files"]
    if not isinstance(declared_rows, list):
        raise ProspectiveEventResponseV5Error("closure_files_not_list")
    declared = {row.get("path"): row for row in declared_rows if isinstance(row, Mapping)}
    if set(declared) != discovered:
        raise ProspectiveEventResponseV5Error("closure_direct_transitive_path_set_mismatch")
    for relative in sorted(discovered):
        row = _closed_mapping(declared[relative], path=f"closure.files.{relative}", fields={"path", "role", "sha256", "byte_length", "last_write_time_ns"})
        target = root / relative
        if not target.is_file() or row["sha256"] != _sha256_bytes(target.read_bytes()) or _integer(row["byte_length"], path=f"closure.files.{relative}.byte_length") != target.stat().st_size or _integer(row["last_write_time_ns"], path=f"closure.files.{relative}.last_write_time_ns") != target.stat().st_mtime_ns:
            raise ProspectiveEventResponseV5Error(f"closure_file_identity_mismatch:{relative}")
        _string(row["role"], path=f"closure.files.{relative}.role")
    artifacts = manifest["source_artifacts"]
    if not isinstance(artifacts, list) or not artifacts:
        raise ProspectiveEventResponseV5Error("closure_source_artifacts_missing")
    artifact_paths: set[str] = set()
    for index, item in enumerate(artifacts):
        row = _closed_mapping(
            item,
            path=f"closure.source_artifacts[{index}]",
            fields={"path", "role", "sha256", "byte_length", "last_write_time_ns"},
        )
        relative = _string(row["path"], path=f"closure.source_artifacts[{index}].path")
        if relative in artifact_paths or relative in discovered:
            raise ProspectiveEventResponseV5Error("closure_source_artifact_duplicate")
        artifact_paths.add(relative)
        target = root / relative
        if not target.is_file() or row["sha256"] != _sha256_bytes(target.read_bytes()) or _integer(row["byte_length"], path=f"closure.source_artifacts[{index}].byte_length") != target.stat().st_size or _integer(row["last_write_time_ns"], path=f"closure.source_artifacts[{index}].last_write_time_ns") != target.stat().st_mtime_ns:
            raise ProspectiveEventResponseV5Error(f"closure_source_artifact_identity_mismatch:{relative}")
        _string(row["role"], path=f"closure.source_artifacts[{index}].role")
    return manifest


__all__ = [
    "CANONICAL_INSTRUMENTS", "CANONICAL_INSTRUMENT_UNIVERSE_SHA256", "COHORT_ID",
    "CONTRACT_ID", "ProspectiveEventResponseV5Error", "build_event_plans", "build_samples",
    "derive_top_status", "discover_local_dependency_closure", "insert_temp_rows", "load_contract",
    "make_temp_test_contract", "mature_outcomes", "open_temp_ledger",
    "quote_snapshot_candidates", "read_temp_rows", "reconstruct_clock_v2_evidence",
    "refuse_canonical_alias", "validate_clock_v2_envelope", "validate_cohort_identity",
    "validate_clock_reconstruction_provenance", "validate_policy_dependency_registry",
    "validate_temp_contract", "verify_pending_closure",
]
