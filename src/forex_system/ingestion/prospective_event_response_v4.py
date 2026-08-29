"""Candidate prospective official-event response capture contract (V4).

V4 is a clean, research-only successor to rejected V3.  It deliberately does
not import rejected V3.  Its only legacy selector dependency is a hash-pinned,
SQLite-free extraction of the archived V2 implementation.  V4 cannot place
orders and cannot initialize a canonical ledger while independent review is
pending.  The only persistence surface in this candidate is an explicitly
test-only SQLite ledger.

The central correction is clock separation:

* the host observation/attestation must be at most 90 seconds old on every
  capture;
* the semantic catalog has its own 300-second SLA at planning and offset zero;
* after a valid offset-zero sample, exact event lineage is frozen and later
  endpoints do not require a refreshed semantic catalog;
* the original semantic source timestamp is always retained and is never
  replaced with the host re-read time.
"""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import prospective_event_response_v2_frozen_helpers as v2_frozen


UTC = dt.timezone.utc
SCHEMA_VERSION = 4
CONTRACT_ID = "prospective_event_response_capture_v4_20260817"
COHORT_ID = "prospective_official_event_response_v4_20260817"
RESEARCH_GENERATION = "prospective_official_event_response_capture_v4"
SUPPORTED_EXECUTION_DECISION = "no_trade"
EXPECTED_CLOCK_CONTRACT_ID = "immutable_point_in_time_event_clock_v2_20260817"
EXPECTED_EVENT_PIPELINE_VERSION = "all_pair_news_event_tags_v3"
EXPECTED_DEPENDENCY_CONTRACT_ID = "currency_policy_dependency_registry_v1_20260816"


class ProspectiveEventResponseV4Error(RuntimeError):
    """Raised when a V4 causal, frozen-lineage, or safety invariant fails."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
        default=str,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _id(prefix: str, value: Mapping[str, Any]) -> str:
    return f"{prefix}_{_sha256_json(value)[:40]}"


def _parse_utc(value: Any, *, field: str, required: bool = True) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            if required:
                raise ProspectiveEventResponseV4Error(f"missing_{field}")
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise ProspectiveEventResponseV4Error(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveEventResponseV4Error(f"naive_{field}")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _exact_bool(value: Any, *, field: str) -> bool:
    if type(value) is not bool:
        raise ProspectiveEventResponseV4Error(f"{field}_must_be_boolean")
    return value


def _research_guard() -> dict[str, Any]:
    return {
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }


def _assert_research_guard(value: Mapping[str, Any]) -> None:
    for key, expected in _research_guard().items():
        actual = value.get(key)
        if isinstance(expected, bool):
            if type(actual) is not bool or actual is not expected:
                raise ProspectiveEventResponseV4Error(f"unsafe_guard:{key}")
        elif actual != expected:
            raise ProspectiveEventResponseV4Error(f"unsafe_guard:{key}")


def load_contract(path: Path, *, allow_pending_review: bool = True) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ProspectiveEventResponseV4Error("contract_not_mapping")
    expected = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "research_generation": RESEARCH_GENERATION,
        "parent_cohort_id": "prospective_official_event_response_v3_20260817",
        "parent_disposition": "zero_evidence_rejected_no_wire",
        "enabled": False,
        "registered": False,
        "canonical_initialization_allowed": False,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
        "immutable_event_clock_contract_id": EXPECTED_CLOCK_CONTRACT_ID,
        "required_source_pipeline_version": EXPECTED_EVENT_PIPELINE_VERSION,
        "linked_policy_dependency_contract_id": EXPECTED_DEPENDENCY_CONTRACT_ID,
        "required_clock_attestation_state": "fresh_trusted",
        "semantic_catalog_max_age_at_planning_or_baseline_sec": 300,
        "host_clock_max_age_sec": 90,
        "maximum_current_event_clock_snapshot_age_sec": 90,
        "postbaseline_semantic_refresh_required": False,
        "never_replace_source_generated_with_host_observed": True,
    }
    for key, wanted in expected.items():
        actual = payload.get(key)
        if isinstance(wanted, bool):
            if type(actual) is not bool or actual is not wanted:
                raise ProspectiveEventResponseV4Error(f"contract_mismatch:{key}")
        elif actual != wanted:
            raise ProspectiveEventResponseV4Error(f"contract_mismatch:{key}")
    if payload.get("review_state") != "pending_independent_review":
        raise ProspectiveEventResponseV4Error("candidate_review_state_mismatch")
    frozen = _parse_utc(
        payload.get("candidate_bytes_frozen_utc"),
        field="candidate_bytes_frozen_utc",
    )
    assert frozen is not None
    if payload.get("independent_reviewed_utc") is not None:
        raise ProspectiveEventResponseV4Error("candidate_cannot_claim_review_timestamp")
    if payload.get("cohort_start_utc") is not None:
        raise ProspectiveEventResponseV4Error("candidate_cannot_predeclare_cohort_start")
    if payload.get("registration_blockers") != [
        "independent_v4_review_pending",
        "official_fact_adapter_v2_alias_repair_pending",
        "currency_state_official_context_v2_alias_repair_pending",
    ]:
        raise ProspectiveEventResponseV4Error("candidate_registration_blockers_mismatch")
    expected_dependency_policy = {
        "pin_actual_official_fact_adapter_v2_bytes": True,
        "pin_actual_currency_state_official_context_v2_bytes": True,
        "pin_sqlite_free_v2_helper_bytes": True,
        "pin_archived_v2_implementation_bytes_executed_by_helper": True,
        "response_v2_tombstone_is_not_the_implementation_identity": True,
        "rejected_v3_is_not_a_v4_runtime_dependency": True,
    }
    if payload.get("frozen_dependency_policy") != expected_dependency_policy:
        raise ProspectiveEventResponseV4Error("frozen_dependency_policy_mismatch")
    if not allow_pending_review:
        raise ProspectiveEventResponseV4Error("v4_pending_independent_review_no_registration")
    offsets = payload.get("target_offsets_sec")
    if not isinstance(offsets, list) or not offsets:
        raise ProspectiveEventResponseV4Error("target_offsets_missing")
    normalized = [int(value) for value in offsets]
    if normalized != sorted(set(normalized)) or normalized[0] != 0:
        raise ProspectiveEventResponseV4Error("target_offsets_invalid")
    sampling = payload.get("sampling")
    if not isinstance(sampling, Mapping):
        raise ProspectiveEventResponseV4Error("sampling_contract_missing")
    required_sampling = {
        "maximum_target_lateness_sec": 45,
        "maximum_quote_lead_sec": 2,
        "maximum_quote_age_at_collection_sec": 15,
        "maximum_snapshot_age_at_collection_sec": 15,
        "maximum_market_attestation_age_sec": 90,
        "maximum_future_clock_skew_sec": 2,
        "maximum_spread_pips_sanity": 5000,
    }
    for key, wanted in required_sampling.items():
        if float(sampling.get(key, -1)) != float(wanted):
            raise ProspectiveEventResponseV4Error(f"sampling_contract_mismatch:{key}")
    return payload


def make_temp_test_contract(
    candidate: Mapping[str, Any], *, cohort_start_utc: dt.datetime
) -> dict[str, Any]:
    """Create an in-memory test-only contract; never suitable for registration."""

    result = json.loads(_canonical_json(candidate))
    result["review_state"] = "temp_fixture_only"
    result["cohort_start_utc"] = _iso(cohort_start_utc)
    result["independent_reviewed_utc"] = _iso(cohort_start_utc)
    result["test_only"] = True
    result["registered"] = False
    result["enabled"] = False
    result["canonical_initialization_allowed"] = False
    return result


def validate_temp_contract(contract: Mapping[str, Any]) -> dt.datetime:
    if contract.get("review_state") != "temp_fixture_only":
        raise ProspectiveEventResponseV4Error("only_temp_fixture_contract_allowed")
    if _exact_bool(contract.get("test_only"), field="test_only") is not True:
        raise ProspectiveEventResponseV4Error("test_only_contract_required")
    for key in ("registered", "enabled", "canonical_initialization_allowed"):
        if _exact_bool(contract.get(key), field=key) is not False:
            raise ProspectiveEventResponseV4Error(f"temp_contract_unsafe:{key}")
    return _parse_utc(contract.get("cohort_start_utc"), field="cohort_start_utc")  # type: ignore[return-value]


def validate_host_clock_freshness(
    snapshot: Mapping[str, Any],
    *,
    at_utc: dt.datetime,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate only the local trusted re-read/attestation clock, not catalog age."""

    at = at_utc.astimezone(UTC)
    maximum = float(contract.get("host_clock_max_age_sec") or 0)
    if maximum != 90.0:
        raise ProspectiveEventResponseV4Error("host_clock_sla_must_be_90_seconds")
    if snapshot.get("contract_id") != EXPECTED_CLOCK_CONTRACT_ID:
        raise ProspectiveEventResponseV4Error("clock_contract_mismatch")
    observation_id = str(snapshot.get("clock_observation_id") or "")
    observed = _parse_utc(snapshot.get("clock_observed_utc"), field="clock_observed_utc")
    attestation = snapshot.get("clock_attestation")
    if not observation_id or not isinstance(attestation, Mapping):
        raise ProspectiveEventResponseV4Error("host_clock_lineage_missing")
    if attestation.get("state") != "fresh_trusted":
        raise ProspectiveEventResponseV4Error("host_clock_not_fresh_trusted")
    if _exact_bool(attestation.get("attested"), field="clock_attested") is not True:
        raise ProspectiveEventResponseV4Error("host_clock_not_attested")
    attested = _parse_utc(
        attestation.get("generated_utc"), field="clock_attestation_generated_utc"
    )
    assert observed is not None and attested is not None
    for name, value in (("clock_observed_utc", observed), ("clock_attestation_generated_utc", attested)):
        age = (at - value).total_seconds()
        if age < 0:
            raise ProspectiveEventResponseV4Error(f"{name}_in_future")
        if age > maximum:
            raise ProspectiveEventResponseV4Error(f"{name}_stale")
    payload_hash = str(attestation.get("payload_sha256") or "")
    artifact = str(attestation.get("artifact_name") or "")
    if not _is_sha256(payload_hash) or not artifact:
        raise ProspectiveEventResponseV4Error("host_clock_attestation_identity_missing")
    return {
        "host_clock_observation_id": observation_id,
        "host_clock_observed_utc": _iso(observed),
        "host_clock_observation_age_sec": round((at - observed).total_seconds(), 6),
        "host_clock_attestation_generated_utc": _iso(attested),
        "host_clock_attestation_age_sec": round((at - attested).total_seconds(), 6),
        "host_clock_attestation_payload_sha256": payload_hash,
        "host_clock_attestation_artifact_name": artifact,
    }


def validate_semantic_catalog_freshness(
    snapshot: Mapping[str, Any],
    *,
    at_utc: dt.datetime,
    contract: Mapping[str, Any],
    phase: str,
) -> dict[str, Any]:
    if phase not in {"planning", "baseline"}:
        raise ProspectiveEventResponseV4Error("semantic_catalog_sla_wrong_phase")
    maximum = float(contract.get("semantic_catalog_max_age_at_planning_or_baseline_sec") or 0)
    if maximum != 300.0:
        raise ProspectiveEventResponseV4Error("semantic_catalog_sla_must_be_300_seconds")
    if snapshot.get("source_pipeline_version") != EXPECTED_EVENT_PIPELINE_VERSION:
        raise ProspectiveEventResponseV4Error("semantic_catalog_pipeline_mismatch")
    source_generated = _parse_utc(
        snapshot.get("clock_observation_source_generated_utc"),
        field="semantic_catalog_source_generated_utc",
    )
    assert source_generated is not None
    at = at_utc.astimezone(UTC)
    age = (at - source_generated).total_seconds()
    if age < 0:
        raise ProspectiveEventResponseV4Error("semantic_catalog_source_generated_in_future")
    if age > maximum:
        raise ProspectiveEventResponseV4Error("semantic_catalog_stale_at_" + phase)
    events_hash = str(snapshot.get("semantic_snapshot_events_sha256") or "")
    manifest_hash = str(snapshot.get("semantic_snapshot_manifest_sha256") or "")
    if not _is_sha256(events_hash) or not _is_sha256(manifest_hash):
        raise ProspectiveEventResponseV4Error("semantic_catalog_hash_lineage_missing")
    return {
        "semantic_catalog_phase": phase,
        "semantic_catalog_source_generated_utc": _iso(source_generated),
        "semantic_catalog_age_sec": round(age, 6),
        "semantic_snapshot_events_sha256": events_hash,
        "semantic_snapshot_manifest_sha256": manifest_hash,
        "semantic_catalog_fresh_at_required_phase": True,
    }


_EVENT_LINEAGE_FIELDS = (
    "event_version_id",
    "upstream_event_id",
    "scheduled_utc",
    "source_id",
    "source_contract_id",
    "source_cohort_id",
    "original_fact_known_utc",
    "event_snapshot_first_observed_utc",
    "event_snapshot_first_trusted_observed_utc",
    "source_contract_first_observed_utc",
    "source_contract_first_trusted_observed_utc",
    "event_availability_utc",
    "material_content_sha256",
    "timing_precision",
    "clock_semantics",
    "independent_domestic_event",
    "category",
    "direct_currencies",
)


def _frozen_event_lineage(event: Mapping[str, Any], *, planned_utc: dt.datetime) -> dict[str, Any]:
    missing = [field for field in _EVENT_LINEAGE_FIELDS if field not in event]
    if missing:
        raise ProspectiveEventResponseV4Error("event_lineage_missing:" + ",".join(missing))
    lineage = {field: event[field] for field in _EVENT_LINEAGE_FIELDS}
    if str(lineage["timing_precision"]) != "minute":
        raise ProspectiveEventResponseV4Error("event_timing_not_exact_minute")
    if lineage["clock_semantics"] not in {
        "domestic_official_policy_release",
        "domestic_official_statistical_release",
    }:
        raise ProspectiveEventResponseV4Error("event_clock_semantics_not_official_exact")
    if type(lineage["independent_domestic_event"]) is not bool or lineage["independent_domestic_event"] is not True:
        raise ProspectiveEventResponseV4Error("event_not_independent_domestic")
    if not _is_sha256(lineage["material_content_sha256"]):
        raise ProspectiveEventResponseV4Error("event_material_hash_missing")
    clocks = {
        key: _parse_utc(lineage[key], field=key)
        for key in (
            "original_fact_known_utc",
            "event_snapshot_first_observed_utc",
            "event_snapshot_first_trusted_observed_utc",
            "source_contract_first_observed_utc",
            "source_contract_first_trusted_observed_utc",
            "event_availability_utc",
        )
    }
    expected_availability = max(
        clocks["original_fact_known_utc"],
        clocks["event_snapshot_first_trusted_observed_utc"],
        clocks["source_contract_first_trusted_observed_utc"],
    )
    if clocks["event_availability_utc"] != expected_availability:
        raise ProspectiveEventResponseV4Error("event_availability_backdating_detected")
    if clocks["event_availability_utc"] > planned_utc.astimezone(UTC):
        raise ProspectiveEventResponseV4Error("event_not_known_at_planning_cutoff")
    scheduled = _parse_utc(lineage["scheduled_utc"], field="scheduled_utc")
    assert scheduled is not None
    if clocks["event_availability_utc"] >= scheduled:
        raise ProspectiveEventResponseV4Error("event_not_observed_pre_release")
    direct = sorted({str(value).upper() for value in lineage["direct_currencies"] if str(value)})
    if not direct:
        raise ProspectiveEventResponseV4Error("event_direct_currency_missing")
    lineage["direct_currencies"] = direct
    lineage["scheduled_utc"] = _iso(scheduled)
    for key, value in clocks.items():
        lineage[key] = _iso(value)
    return lineage


def _normalize_instruments(instruments: Sequence[str]) -> list[str]:
    normalized = sorted({str(value).strip().upper() for value in instruments if str(value).strip()})
    if not normalized:
        raise ProspectiveEventResponseV4Error("instrument_universe_empty")
    for instrument in normalized:
        legs = instrument.split("_")
        if len(legs) != 2 or any(len(leg) != 3 for leg in legs):
            raise ProspectiveEventResponseV4Error(f"invalid_instrument:{instrument}")
    return normalized


def _dependency_fingerprint(registry: Mapping[str, Any]) -> str:
    if registry.get("contract_id") != EXPECTED_DEPENDENCY_CONTRACT_ID:
        raise ProspectiveEventResponseV4Error("policy_dependency_contract_mismatch")
    if registry.get("research_only") is not True or registry.get("execution_eligible") is not False:
        raise ProspectiveEventResponseV4Error("policy_dependency_registry_not_research_only")
    dependencies = registry.get("dependencies")
    if not isinstance(dependencies, list):
        raise ProspectiveEventResponseV4Error("policy_dependency_rows_missing")
    for row in dependencies:
        if not isinstance(row, Mapping) or row.get("assign_direction") is not False:
            raise ProspectiveEventResponseV4Error("policy_dependency_direction_or_schema_invalid")
    return _sha256_json(registry)


def build_event_plans(
    clock_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    instruments: Sequence[str],
    policy_dependency_registry: Mapping[str, Any],
    planned_utc: dt.datetime,
) -> dict[str, Any]:
    cohort_start = validate_temp_contract(contract)
    planned = planned_utc.astimezone(UTC)
    if planned < cohort_start:
        raise ProspectiveEventResponseV4Error("planning_before_temp_cohort_start")
    host = validate_host_clock_freshness(clock_snapshot, at_utc=planned, contract=contract)
    semantic = validate_semantic_catalog_freshness(
        clock_snapshot, at_utc=planned, contract=contract, phase="planning"
    )
    instruments_normalized = _normalize_instruments(instruments)
    universe_hash = _sha256_json(instruments_normalized)
    dependency_hash = _dependency_fingerprint(policy_dependency_registry)
    dependencies = [row for row in policy_dependency_registry["dependencies"]]
    offsets = [int(value) for value in contract["target_offsets_sec"]]
    v2_contract = dict(contract)
    v2_contract["contract_id"] = v2_frozen.CONTRACT_ID
    try:
        selector = v2_frozen.build_event_plans(
            clock_snapshot,
            contract=v2_contract,
            instruments=instruments_normalized,
            planned_utc=planned,
        )
    except v2_frozen.ProspectiveEventResponseError as exc:
        raise ProspectiveEventResponseV4Error(f"frozen_v2_selector_failed:{exc}") from exc
    selector_versions = {
        str(row.get("event_version_id") or "")
        for row in selector.get("plans") or []
        if isinstance(row, Mapping)
    }
    plans: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    for event in clock_snapshot.get("events") or []:
        if not isinstance(event, Mapping):
            continue
        if str(event.get("event_version_id") or "") not in selector_versions:
            continue
        try:
            lineage = _frozen_event_lineage(event, planned_utc=planned)
        except ProspectiveEventResponseV4Error as exc:
            attempts.append({
                "status": "event_rejected_failed_closed",
                "reason": str(exc),
                "source_ref": str(event.get("event_version_id") or ""),
                **_research_guard(),
            })
            continue
        scheduled = _parse_utc(lineage["scheduled_utc"], field="scheduled_utc")
        assert scheduled is not None
        if scheduled < cohort_start:
            attempts.append({
                "status": "event_rejected_failed_closed",
                "reason": "event_scheduled_before_v4_cohort_start",
                "source_ref": str(lineage["event_version_id"]),
                **_research_guard(),
            })
            continue
        if (scheduled - planned).total_seconds() < float(contract.get("minimum_schedule_lead_sec") or 0):
            attempts.append({
                "status": "event_rejected_failed_closed",
                "reason": "insufficient_pre_release_planning_lead",
                "source_ref": str(lineage["event_version_id"]),
                **_research_guard(),
            })
            continue
        lineage_hash = _sha256_json(lineage)
        event_instance_id = _id(
            "official_event_instance_v4",
            {
                "contract_id": CONTRACT_ID,
                "cohort_id": COHORT_ID,
                "event_version_id": lineage["event_version_id"],
                "scheduled_utc": lineage["scheduled_utc"],
                "frozen_event_lineage_sha256": lineage_hash,
            },
        )
        direct = set(lineage["direct_currencies"])
        linked: dict[str, Mapping[str, Any]] = {}
        if "policy" in str(lineage["category"]).lower():
            for row in dependencies:
                if str(row.get("driver_currency") or "").upper() in direct:
                    linked[str(row.get("dependent_currency") or "").upper()] = row
        factor_id = _id(
            "official_event_factor_v4",
            {"event_instance_id": event_instance_id, "driver_currencies": sorted(direct)},
        )
        for instrument in instruments_normalized:
            legs = set(instrument.split("_"))
            if legs.intersection(direct):
                relationship = "direct_leg"
            elif legs.intersection(linked):
                relationship = "linked_policy_dependency"
            else:
                relationship = "unaffected_control"
            for offset in offsets:
                target = scheduled + dt.timedelta(seconds=offset)
                identity = {
                    "contract_id": CONTRACT_ID,
                    "cohort_id": COHORT_ID,
                    "event_instance_id": event_instance_id,
                    "instrument": instrument,
                    "target_offset_sec": offset,
                    "target_utc": _iso(target),
                }
                plan = {
                    "plan_id": _id("event_response_plan_v4", identity),
                    **identity,
                    "event_version_id": lineage["event_version_id"],
                    "upstream_event_id": lineage["upstream_event_id"],
                    "scheduled_utc": lineage["scheduled_utc"],
                    "target_utc": _iso(target),
                    "sample_role": "baseline" if offset == 0 else "outcome",
                    "relationship": relationship,
                    "underlying_factor_id": factor_id,
                    "frozen_event_lineage_json": _canonical_json(lineage),
                    "frozen_event_lineage_sha256": lineage_hash,
                    "instrument_universe_sha256": universe_hash,
                    "policy_dependency_registry_sha256": dependency_hash,
                    "semantic_catalog_source_generated_utc": semantic[
                        "semantic_catalog_source_generated_utc"
                    ],
                    "semantic_catalog_age_sec_at_planning": semantic[
                        "semantic_catalog_age_sec"
                    ],
                    "semantic_snapshot_events_sha256": semantic[
                        "semantic_snapshot_events_sha256"
                    ],
                    "semantic_snapshot_manifest_sha256": semantic[
                        "semantic_snapshot_manifest_sha256"
                    ],
                    **host,
                    **_research_guard(),
                }
                plans.append(plan)
    plans.sort(key=lambda row: (row["target_utc"], row["plan_id"]))
    status = "plans_ready" if plans else "no_eligible_exact_events"
    return {
        "status": status,
        "plans": plans,
        "attempts": attempts,
        "plan_count": len(plans),
        "instrument_universe_sha256": universe_hash,
        "policy_dependency_registry_sha256": dependency_hash,
        "host_clock": host,
        "semantic_catalog": semantic,
        **_research_guard(),
    }


def quote_snapshot_candidates(
    payload: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    artifact_sha256: str,
    collector_observed_utc: dt.datetime,
    market_clock_attestation: Mapping[str, Any],
) -> list[dict[str, Any]]:
    collector = collector_observed_utc.astimezone(UTC)
    sampling = contract.get("sampling") or {}
    if not _is_sha256(artifact_sha256):
        raise ProspectiveEventResponseV4Error("quote_artifact_hash_invalid")
    if market_clock_attestation.get("state") != "fresh_trusted":
        raise ProspectiveEventResponseV4Error("market_clock_not_fresh_trusted")
    if _exact_bool(market_clock_attestation.get("attested"), field="market_clock_attested") is not True:
        raise ProspectiveEventResponseV4Error("market_clock_not_attested")
    attested = _parse_utc(
        market_clock_attestation.get("generated_utc"), field="market_clock_generated_utc"
    )
    assert attested is not None
    attestation_age = (collector - attested).total_seconds()
    maximum_attestation = float(sampling.get("maximum_market_attestation_age_sec") or 0)
    if attestation_age < 0 or attestation_age > maximum_attestation:
        raise ProspectiveEventResponseV4Error("market_clock_attestation_stale_or_future")
    source_generated = _parse_utc(payload.get("generated_utc"), field="quote_snapshot_generated_utc")
    assert source_generated is not None
    snapshot_age = (collector - source_generated).total_seconds()
    if snapshot_age < 0 or snapshot_age > float(sampling.get("maximum_snapshot_age_at_collection_sec") or 0):
        raise ProspectiveEventResponseV4Error("quote_snapshot_stale_or_future")
    quotes = payload.get("quotes")
    if not isinstance(quotes, Mapping):
        raise ProspectiveEventResponseV4Error("quote_snapshot_quotes_missing")
    output: list[dict[str, Any]] = []
    for instrument, raw in quotes.items():
        if not isinstance(raw, Mapping):
            continue
        bid = _finite(raw.get("bid"))
        ask = _finite(raw.get("ask"))
        pip = _finite(raw.get("pip"))
        quote_time = _parse_utc(raw.get("time"), field="quote_time_utc", required=False)
        if bid is None or ask is None or pip is None or quote_time is None:
            continue
        if bid <= 0 or ask <= bid or pip <= 0:
            continue
        quote_age = (collector - quote_time).total_seconds()
        if quote_age < 0 or quote_age > float(sampling.get("maximum_quote_age_at_collection_sec") or 0):
            continue
        identity = {
            "artifact_sha256": artifact_sha256,
            "instrument": str(instrument).upper(),
            "quote_time_utc": _iso(quote_time),
            "bid": bid,
            "ask": ask,
        }
        output.append({
            "source_type": "live_executable_quote",
            "source_record_id": _id("live_quote_v4", identity),
            "source_payload_sha256": artifact_sha256,
            "instrument": str(instrument).upper(),
            "collector_observed_utc": _iso(collector),
            "source_generated_utc": _iso(source_generated),
            "quote_time_utc": _iso(quote_time),
            "observation_known_utc": _iso(max(collector, source_generated, quote_time)),
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "market_clock_attestation_generated_utc": _iso(attested),
            "market_clock_attestation_age_sec": round(attestation_age, 6),
            "market_clock_attestation_payload_sha256": str(
                market_clock_attestation.get("payload_sha256") or ""
            ),
            **_research_guard(),
        })
    return output


def _validate_plan_static_inputs(
    plan: Mapping[str, Any],
    *,
    instruments: Sequence[str],
    policy_dependency_registry: Mapping[str, Any],
) -> None:
    if plan.get("contract_id") != CONTRACT_ID or plan.get("cohort_id") != COHORT_ID:
        raise ProspectiveEventResponseV4Error("plan_contract_or_cohort_mismatch")
    if plan.get("instrument_universe_sha256") != _sha256_json(_normalize_instruments(instruments)):
        raise ProspectiveEventResponseV4Error("instrument_universe_tampering_detected")
    if plan.get("policy_dependency_registry_sha256") != _dependency_fingerprint(
        policy_dependency_registry
    ):
        raise ProspectiveEventResponseV4Error("policy_dependency_tampering_detected")
    lineage_json = str(plan.get("frozen_event_lineage_json") or "")
    if not lineage_json or _sha256_bytes(lineage_json.encode("utf-8")) != plan.get(
        "frozen_event_lineage_sha256"
    ):
        raise ProspectiveEventResponseV4Error("frozen_event_lineage_hash_mismatch")


def build_samples(
    plans: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]],
    *,
    existing_samples: Sequence[Mapping[str, Any]],
    current_clock_snapshot: Mapping[str, Any],
    contract: Mapping[str, Any],
    instruments: Sequence[str],
    policy_dependency_registry: Mapping[str, Any],
    collector_observed_utc: dt.datetime,
) -> dict[str, Any]:
    validate_temp_contract(contract)
    collector = collector_observed_utc.astimezone(UTC)
    host = validate_host_clock_freshness(
        current_clock_snapshot, at_utc=collector, contract=contract
    )
    sampling = contract.get("sampling") or {}
    late = float(sampling.get("maximum_target_lateness_sec") or 0)
    lead = float(sampling.get("maximum_quote_lead_sec") or 0)
    future_skew = float(sampling.get("maximum_future_clock_skew_sec") or 0)
    max_spread = float(sampling.get("maximum_spread_pips_sanity") or 0)
    samples_by_plan = {str(row.get("plan_id") or ""): row for row in existing_samples}
    baselines = {
        (str(row.get("event_instance_id") or ""), str(row.get("instrument") or "")): row
        for row in existing_samples
        if int(row.get("target_offset_sec") or 0) == 0
    }
    current_events = {
        str(row.get("event_version_id") or ""): row
        for row in current_clock_snapshot.get("events") or []
        if isinstance(row, Mapping)
    }
    by_instrument: dict[str, list[Mapping[str, Any]]] = {}
    for row in observations:
        by_instrument.setdefault(str(row.get("instrument") or "").upper(), []).append(row)
    output: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    for plan in plans:
        _validate_plan_static_inputs(
            plan,
            instruments=instruments,
            policy_dependency_registry=policy_dependency_registry,
        )
        plan_id = str(plan.get("plan_id") or "")
        if plan_id in samples_by_plan:
            continue
        target = _parse_utc(plan.get("target_utc"), field="target_utc")
        assert target is not None
        if collector < target or (collector - target).total_seconds() > late:
            continue
        offset = int(plan.get("target_offset_sec") or 0)
        key = (str(plan.get("event_instance_id") or ""), str(plan.get("instrument") or ""))
        semantic_at_sample: dict[str, Any]
        if offset == 0:
            semantic_at_sample = validate_semantic_catalog_freshness(
                current_clock_snapshot,
                at_utc=collector,
                contract=contract,
                phase="baseline",
            )
            current = current_events.get(str(plan.get("event_version_id") or ""))
            if current is None:
                attempts.append({
                    "status": "baseline_rejected_failed_closed",
                    "reason": "event_revised_or_removed_before_baseline",
                    "plan_id": plan_id,
                    **_research_guard(),
                })
                continue
            try:
                current_lineage = _frozen_event_lineage(current, planned_utc=collector)
            except ProspectiveEventResponseV4Error:
                attempts.append({
                    "status": "baseline_rejected_failed_closed",
                    "reason": "event_lineage_invalid_before_baseline",
                    "plan_id": plan_id,
                    **_research_guard(),
                })
                continue
            if _sha256_json(current_lineage) != plan.get("frozen_event_lineage_sha256"):
                attempts.append({
                    "status": "baseline_rejected_failed_closed",
                    "reason": "event_lineage_changed_before_baseline",
                    "plan_id": plan_id,
                    **_research_guard(),
                })
                continue
            lineage_state = "baseline_exact_event_lineage"
            semantic_refresh_evaluated = True
        else:
            baseline = baselines.get(key)
            if baseline is None:
                attempts.append({
                    "status": "endpoint_waiting_failed_closed",
                    "reason": "endpoint_waiting_for_valid_baseline",
                    "plan_id": plan_id,
                    **_research_guard(),
                })
                continue
            if baseline.get("frozen_event_lineage_sha256") != plan.get(
                "frozen_event_lineage_sha256"
            ):
                raise ProspectiveEventResponseV4Error("baseline_to_plan_lineage_mismatch")
            source_generated = _parse_utc(
                current_clock_snapshot.get("clock_observation_source_generated_utc"),
                field="semantic_catalog_source_generated_utc",
                required=False,
            )
            semantic_at_sample = {
                "semantic_catalog_phase": "postbaseline_not_refreshed",
                "semantic_catalog_source_generated_utc": (
                    _iso(source_generated) if source_generated is not None else None
                ),
                "semantic_catalog_age_sec": (
                    round((collector - source_generated).total_seconds(), 6)
                    if source_generated is not None
                    else None
                ),
                "semantic_catalog_fresh_at_required_phase": None,
            }
            lineage_state = "postbaseline_frozen_exact_event_lineage"
            semantic_refresh_evaluated = False
        valid: list[tuple[dt.datetime, str, Mapping[str, Any]]] = []
        reason_counts: Counter[str] = Counter()
        for observation in by_instrument.get(str(plan.get("instrument") or "").upper(), []):
            if observation.get("source_type") != "live_executable_quote":
                reason_counts["source_type_not_allowed"] += 1
                continue
            known = _parse_utc(observation.get("observation_known_utc"), field="observation_known_utc", required=False)
            quote_time = _parse_utc(observation.get("quote_time_utc"), field="quote_time_utc", required=False)
            generated = _parse_utc(observation.get("source_generated_utc"), field="source_generated_utc", required=False)
            observed = _parse_utc(observation.get("collector_observed_utc"), field="collector_observed_utc", required=False)
            bid = _finite(observation.get("bid"))
            ask = _finite(observation.get("ask"))
            pip = _finite(observation.get("pip"))
            if None in (known, quote_time, generated, observed, bid, ask, pip):
                reason_counts["quote_value_or_clock_missing"] += 1
                continue
            assert known is not None and quote_time is not None and generated is not None and observed is not None
            assert bid is not None and ask is not None and pip is not None
            if bid <= 0 or ask <= bid or pip <= 0:
                reason_counts["invalid_executable_quote"] += 1
                continue
            if any(value > collector + dt.timedelta(seconds=future_skew) for value in (known, quote_time, generated, observed)):
                reason_counts["quote_clock_in_future"] += 1
                continue
            if known != max(quote_time, generated, observed):
                reason_counts["observation_known_not_max_lineage_clock"] += 1
                continue
            alignment = (quote_time - target).total_seconds()
            if alignment < -lead or alignment > late:
                reason_counts["quote_outside_target_window"] += 1
                continue
            spread = (ask - bid) / pip
            if spread > max_spread:
                reason_counts["spread_exceeds_sanity"] += 1
                continue
            valid.append((known, str(observation.get("source_record_id") or ""), observation))
        if not valid:
            attempts.append({
                "status": "sample_rejected_failed_closed",
                "reason": reason_counts.most_common(1)[0][0] if reason_counts else "quote_missing",
                "plan_id": plan_id,
                **_research_guard(),
            })
            continue
        valid.sort(key=lambda item: (item[0], item[1]))
        observation = valid[0][2]
        bid = float(observation["bid"])
        ask = float(observation["ask"])
        pip = float(observation["pip"])
        quote_time = _parse_utc(observation["quote_time_utc"], field="quote_time_utc")
        known = _parse_utc(observation["observation_known_utc"], field="observation_known_utc")
        assert quote_time is not None and known is not None
        identity = {
            "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID,
            "plan_id": plan_id,
            "source_record_id": observation["source_record_id"],
            "quote_time_utc": _iso(quote_time),
        }
        sample = {
            "sample_id": _id("event_response_sample_v4", identity),
            **identity,
            "event_instance_id": plan["event_instance_id"],
            "event_version_id": plan["event_version_id"],
            "instrument": plan["instrument"],
            "target_offset_sec": offset,
            "target_utc": plan["target_utc"],
            "sample_role": plan["sample_role"],
            "event_lineage_state": lineage_state,
            "frozen_event_lineage_sha256": plan["frozen_event_lineage_sha256"],
            "semantic_refresh_evaluated": semantic_refresh_evaluated,
            "semantic_catalog_source_generated_utc": semantic_at_sample.get(
                "semantic_catalog_source_generated_utc"
            ),
            "semantic_catalog_age_sec_at_sample": semantic_at_sample.get(
                "semantic_catalog_age_sec"
            ),
            "semantic_catalog_fresh_at_required_phase": semantic_at_sample.get(
                "semantic_catalog_fresh_at_required_phase"
            ),
            **host,
            "quote_time_utc": _iso(quote_time),
            "observation_known_utc": _iso(known),
            "target_distance_sec": round((quote_time - target).total_seconds(), 6),
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "pip": pip,
            "spread_pips": (ask - bid) / pip,
            "source_payload_sha256": observation["source_payload_sha256"],
            "policy_dependency_registry_sha256": plan[
                "policy_dependency_registry_sha256"
            ],
            "instrument_universe_sha256": plan["instrument_universe_sha256"],
            **_research_guard(),
        }
        output.append(sample)
        if offset == 0:
            baselines[key] = sample
    return {
        "status": "samples_captured" if output else "no_valid_samples",
        "samples": output,
        "attempts": attempts,
        "sample_count": len(output),
        "host_clock": host,
        **_research_guard(),
    }


def mature_outcomes(samples: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    baselines = {
        (row.get("event_instance_id"), row.get("instrument")): row
        for row in samples
        if int(row.get("target_offset_sec") or 0) == 0
    }
    outcomes: list[dict[str, Any]] = []
    for endpoint in samples:
        horizon = int(endpoint.get("target_offset_sec") or 0)
        if horizon <= 0:
            continue
        baseline = baselines.get((endpoint.get("event_instance_id"), endpoint.get("instrument")))
        if baseline is None:
            continue
        if endpoint.get("frozen_event_lineage_sha256") != baseline.get(
            "frozen_event_lineage_sha256"
        ):
            raise ProspectiveEventResponseV4Error("outcome_lineage_mismatch")
        pip = float(baseline["pip"])
        long_pips = (float(endpoint["bid"]) - float(baseline["ask"])) / pip
        short_pips = (float(baseline["bid"]) - float(endpoint["ask"])) / pip
        identity = {
            "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID,
            "event_instance_id": endpoint["event_instance_id"],
            "instrument": endpoint["instrument"],
            "horizon_sec": horizon,
        }
        outcomes.append({
            "outcome_id": _id("event_response_outcome_v4", identity),
            **identity,
            "baseline_sample_id": baseline["sample_id"],
            "outcome_sample_id": endpoint["sample_id"],
            "long_after_spread_pips": long_pips,
            "short_after_spread_pips": short_pips,
            "direction_selected": False,
            "best_side_metric": None,
            **_research_guard(),
        })
    return outcomes


_TEMP_TABLES = {
    "plans": "plan_id",
    "samples": "sample_id",
    "outcomes": "outcome_id",
    "attempts": "attempt_id",
}


def open_temp_ledger(path: Path, *, canonical_path: Path | None = None) -> sqlite3.Connection:
    if canonical_path is not None and path.resolve() == canonical_path.resolve():
        raise ProspectiveEventResponseV4Error("canonical_v4_ledger_initialization_forbidden")
    if path.exists() and path.is_dir():
        raise ProspectiveEventResponseV4Error("temp_ledger_path_is_directory")
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS v4_temp_meta(
          key TEXT PRIMARY KEY,value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v4_temp_plans(
          plan_id TEXT PRIMARY KEY,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v4_temp_samples(
          sample_id TEXT PRIMARY KEY,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v4_temp_outcomes(
          outcome_id TEXT PRIMARY KEY,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v4_temp_attempts(
          attempt_id TEXT PRIMARY KEY,payload_sha256 TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        """
    )
    for table in _TEMP_TABLES:
        connection.executescript(
            f"""
            CREATE TRIGGER IF NOT EXISTS v4_temp_{table}_no_update
            BEFORE UPDATE ON v4_temp_{table}
            BEGIN SELECT RAISE(ABORT,'append_only_v4_temp_ledger'); END;
            CREATE TRIGGER IF NOT EXISTS v4_temp_{table}_no_delete
            BEFORE DELETE ON v4_temp_{table}
            BEGIN SELECT RAISE(ABORT,'append_only_v4_temp_ledger'); END;
            """
        )
    connection.execute(
        "INSERT OR IGNORE INTO v4_temp_meta(key,value) VALUES('mode','temp_fixture_only')"
    )
    connection.commit()
    return connection


def insert_temp_rows(
    connection: sqlite3.Connection, table: str, rows: Iterable[Mapping[str, Any]]
) -> int:
    if table not in _TEMP_TABLES:
        raise ProspectiveEventResponseV4Error("unsupported_temp_table")
    key = _TEMP_TABLES[table]
    inserted = 0
    for source in rows:
        row = dict(source)
        if key not in row:
            if table == "attempts":
                row[key] = _id("event_response_attempt_v4", row)
            else:
                raise ProspectiveEventResponseV4Error(f"missing_{key}")
        _assert_research_guard(row)
        payload = _canonical_json(row)
        payload_hash = _sha256_bytes(payload.encode("utf-8"))
        existing = connection.execute(
            f"SELECT payload_sha256,payload_json FROM v4_temp_{table} WHERE {key}=?",
            (row[key],),
        ).fetchone()
        if existing is not None:
            if existing[0] != payload_hash or existing[1] != payload:
                raise ProspectiveEventResponseV4Error("temp_primary_key_content_collision")
            continue
        connection.execute(
            f"INSERT INTO v4_temp_{table}({key},payload_sha256,payload_json) VALUES(?,?,?)",
            (row[key], payload_hash, payload),
        )
        inserted += 1
    return inserted


def read_temp_rows(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    if table not in _TEMP_TABLES:
        raise ProspectiveEventResponseV4Error("unsupported_temp_table")
    key = _TEMP_TABLES[table]
    output: list[dict[str, Any]] = []
    for row in connection.execute(
        f"SELECT {key},payload_sha256,payload_json FROM v4_temp_{table} ORDER BY {key}"
    ):
        payload = str(row["payload_json"])
        if _sha256_bytes(payload.encode("utf-8")) != row["payload_sha256"]:
            raise ProspectiveEventResponseV4Error("temp_payload_hash_mismatch")
        decoded = json.loads(payload)
        if decoded.get(key) != row[key]:
            raise ProspectiveEventResponseV4Error("temp_payload_identity_mismatch")
        _assert_research_guard(decoded)
        output.append(decoded)
    return output


def verify_helper_closure(
    root: Path,
    manifest_path: Path,
    *,
    require_reviewed: bool,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("contract_id") != CONTRACT_ID:
        raise ProspectiveEventResponseV4Error("helper_manifest_contract_mismatch")
    helpers = manifest.get("helpers")
    if not isinstance(helpers, list) or not helpers:
        raise ProspectiveEventResponseV4Error("helper_closure_empty")
    required_paths = {
        "src/forex_system/ingestion/prospective_event_response_v4.py": "candidate_runtime_core",
        "oanda_prospective_event_response_v4.py": "temp_only_collector",
        "oanda_prospective_event_response_worker_v4.py": "temp_only_worker",
        "src/forex_system/ingestion/immutable_event_clock.py": "upstream_clock_implementation",
        "src/forex_system/ingestion/official_fact_adapter.py": "upstream_official_fact_v1_implementation",
        "src/forex_system/ingestion/official_fact_adapter_v2.py": "actual_official_fact_v2_implementation",
        "src/forex_system/features/currency_state_official_context.py": "upstream_currency_context_v1_implementation",
        "src/forex_system/features/currency_state_official_context_v2.py": "actual_currency_context_v2_implementation",
        "src/forex_system/contracts/currency_state.py": "upstream_currency_state_contract",
        "src/forex_system/ingestion/prospective_event_response_v2_frozen_helpers.py": "sqlite_free_selector_loader",
        "src/forex_system/ingestion/prospective_event_response_v2_tombstone.py": "transitive_exception_only_not_implementation_identity",
        "artifacts/retired_contracts/prospective_event_response_v2_08bfa4b0bb213cae.py.frozen": "executed_v2_implementation_identity",
        "config/prospective_event_response_capture_v4.json": "candidate_contract",
    }
    declared_roles = {
        str(row.get("path") or "").replace("\\", "/"): str(row.get("role") or "")
        for row in helpers
    }
    if declared_roles != required_paths:
        raise ProspectiveEventResponseV4Error("helper_closure_path_set_mismatch")
    for row in helpers:
        relative = str(row["path"]).replace("\\", "/")
        target = root / Path(relative)
        expected_hash = str(row.get("sha256") or "")
        if not target.is_file() or not _is_sha256(expected_hash):
            raise ProspectiveEventResponseV4Error("helper_closure_file_or_hash_missing")
        if _sha256_bytes(target.read_bytes()) != expected_hash:
            raise ProspectiveEventResponseV4Error(f"helper_closure_hash_mismatch:{relative}")
    frozen = _parse_utc(manifest.get("candidate_bytes_frozen_utc"), field="candidate_bytes_frozen_utc")
    assert frozen is not None
    contract_payload = json.loads(
        (root / "config" / "prospective_event_response_capture_v4.json").read_text(
            encoding="utf-8"
        )
    )
    contract_frozen = _parse_utc(
        contract_payload.get("candidate_bytes_frozen_utc"),
        field="contract_candidate_bytes_frozen_utc",
    )
    if contract_frozen != frozen:
        raise ProspectiveEventResponseV4Error("helper_manifest_freeze_mismatch")
    reviewed = _parse_utc(
        manifest.get("independent_reviewed_utc"),
        field="independent_reviewed_utc",
        required=False,
    )
    cohort_start = _parse_utc(
        manifest.get("cohort_start_utc"), field="cohort_start_utc", required=False
    )
    if require_reviewed:
        if reviewed is None or reviewed < frozen:
            raise ProspectiveEventResponseV4Error("review_timestamp_backdates_final_bytes")
        if cohort_start is None or cohort_start < reviewed:
            raise ProspectiveEventResponseV4Error("cohort_start_backdates_review")
        if manifest.get("review_state") != "independently_approved":
            raise ProspectiveEventResponseV4Error("helper_manifest_not_approved")
    else:
        if reviewed is not None or cohort_start is not None:
            raise ProspectiveEventResponseV4Error("pending_manifest_claims_review_or_start")
        if manifest.get("review_state") != "pending_independent_review":
            raise ProspectiveEventResponseV4Error("pending_manifest_state_mismatch")
    return manifest


def imported_local_helper_paths(path: Path) -> set[str]:
    """Expose import closure for adversarial review; stdlib imports are ignored."""

    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith("forex_system") or node.module.startswith("src.forex_system"):
                names.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("oanda_prospective_event_response_v4"):
                    names.add(alias.name)
    return names


__all__ = [
    "COHORT_ID",
    "CONTRACT_ID",
    "EXPECTED_CLOCK_CONTRACT_ID",
    "EXPECTED_DEPENDENCY_CONTRACT_ID",
    "EXPECTED_EVENT_PIPELINE_VERSION",
    "ProspectiveEventResponseV4Error",
    "build_event_plans",
    "build_samples",
    "imported_local_helper_paths",
    "insert_temp_rows",
    "load_contract",
    "make_temp_test_contract",
    "mature_outcomes",
    "open_temp_ledger",
    "quote_snapshot_candidates",
    "read_temp_rows",
    "validate_host_clock_freshness",
    "validate_semantic_catalog_freshness",
    "validate_temp_contract",
    "verify_helper_closure",
]
