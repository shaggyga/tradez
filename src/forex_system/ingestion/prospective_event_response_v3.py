"""Governed prospective official-event response capture (v3).

This module is deliberately research-only.  It freezes official event clocks,
captures executable bid/ask observations at predeclared offsets, and records
two-sided endpoint economics.  It has no broker, order, lifecycle, promotion,
or authorization imports and cannot create an execution decision.

Version three hardens the engineering-only v2 cohort with dual freshness,
exact-horizon truth, baseline-frozen post-release lineage, single-transaction
append semantics, terminal/attempt separation, and full-record integrity.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import prospective_event_response_v2_frozen_helpers as v2


UTC = dt.timezone.utc
SCHEMA_VERSION = "prospective_event_response_ledger_v3"
CONTRACT_ID = "prospective_event_response_capture_v3_20260817"
RESEARCH_GENERATION = "prospective_official_event_response_capture_v3"
SUPPORTED_EXECUTION_DECISION = "no_trade"
EXPECTED_EVENT_PIPELINE_VERSION = "all_pair_news_event_tags_v3"


class ProspectiveEventResponseV3Error(RuntimeError):
    """Raised when an input violates the frozen v3 evidence contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _id(prefix: str, identity_material: Mapping[str, Any]) -> str:
    return f"{prefix}_{_sha256_json(identity_material)[:40]}"


def _parse_utc(value: Any, *, field: str, required: bool = False) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        if required:
            raise ProspectiveEventResponseV3Error(f"missing_{field}")
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProspectiveEventResponseV3Error(f"invalid_{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveEventResponseV3Error(f"naive_{field}:{value}")
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _finalize_record(
    *,
    id_field: str,
    id_prefix: str,
    identity_material: Mapping[str, Any],
    evidence_fields: Mapping[str, Any],
) -> dict[str, Any]:
    """Separate stable identity from a full canonical evidence payload.

    The ID intentionally hashes only immutable identity material.  The payload
    hashes every persisted evidence column (including the ID) other than the
    payload hash/JSON themselves.  Insert/read verification can therefore
    detect a same-ID/different-content collision instead of silently ignoring
    it.
    """

    record = {id_field: _id(id_prefix, identity_material), **dict(evidence_fields)}
    record["payload_json"] = _canonical_json(record)
    record["payload_sha256"] = hashlib.sha256(
        record["payload_json"].encode("utf-8")
    ).hexdigest()
    return record


def _verify_record_payload(row: Mapping[str, Any]) -> None:
    payload_json = str(row.get("payload_json") or "")
    payload_sha = str(row.get("payload_sha256") or "")
    expected = {
        str(key): value
        for key, value in row.items()
        if str(key) not in {"payload_json", "payload_sha256"}
    }
    canonical = _canonical_json(expected)
    if payload_json != canonical:
        raise ProspectiveEventResponseV3Error("full_record_payload_json_mismatch")
    if payload_sha != hashlib.sha256(canonical.encode("utf-8")).hexdigest():
        raise ProspectiveEventResponseV3Error("full_record_payload_sha256_mismatch")


def load_contract(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ProspectiveEventResponseV3Error("contract_is_not_mapping")
    if str(payload.get("contract_id") or "") != CONTRACT_ID:
        raise ProspectiveEventResponseV3Error("unexpected_contract_id")
    if not bool(payload.get("research_only")) or bool(payload.get("execution_eligible")):
        raise ProspectiveEventResponseV3Error("contract_not_research_only")
    if bool(payload.get("can_place_orders")):
        raise ProspectiveEventResponseV3Error("contract_can_place_orders")
    if str(payload.get("supported_execution_decision") or "") != SUPPORTED_EXECUTION_DECISION:
        raise ProspectiveEventResponseV3Error("contract_decision_not_no_trade")
    offsets = payload.get("target_offsets_sec")
    if not isinstance(offsets, list) or not offsets:
        raise ProspectiveEventResponseV3Error("missing_target_offsets")
    normalized = sorted({int(value) for value in offsets})
    if normalized != [int(value) for value in offsets] or normalized[0] != 0:
        raise ProspectiveEventResponseV3Error(
            "target_offsets_must_be_sorted_unique_and_start_zero"
        )
    if int(payload.get("baseline_offset_sec", -1)) != 0:
        raise ProspectiveEventResponseV3Error("baseline_offset_must_be_zero")
    if payload.get("outcome", {}).get("best_side_metric_forbidden") is not True:
        raise ProspectiveEventResponseV3Error("best_side_must_be_forbidden")
    _parse_utc(payload.get("cohort_start_utc"), field="cohort_start_utc", required=True)
    if payload.get("exclude_pre_cohort_observations") is not True:
        raise ProspectiveEventResponseV3Error("pre_cohort_observations_must_be_excluded")
    if payload.get("allow_late_backfill") is not False:
        raise ProspectiveEventResponseV3Error("late_backfill_must_be_disabled")
    if payload.get("material_change_requires_new_cohort") is not True:
        raise ProspectiveEventResponseV3Error("material_change_must_require_new_cohort")
    expected_admission = "append_only_from_each_new_immutable_semantic_snapshot"
    if str(payload.get("event_instance_admission") or "") != expected_admission:
        raise ProspectiveEventResponseV3Error("unexpected_event_instance_admission")
    if payload.get("event_catalog_frozen_at_cohort_start") is not False:
        raise ProspectiveEventResponseV3Error("event_catalog_must_remain_append_admissible")
    if str(payload.get("immutable_event_clock_contract_id") or "") != (
        "immutable_point_in_time_event_clock_v2_20260817"
    ):
        raise ProspectiveEventResponseV3Error("unexpected_immutable_event_clock_contract")
    if str(payload.get("required_source_pipeline_version") or "") != (
        EXPECTED_EVENT_PIPELINE_VERSION
    ):
        raise ProspectiveEventResponseV3Error("unexpected_source_pipeline_contract")
    for required_true in (
        "admit_previously_unseen_future_exact_events",
        "new_event_requires_pre_release_observation",
        "interim_source_rows_are_not_permanent_universe",
        "never_reclassify_or_backfill_prior_event_instances",
    ):
        if payload.get(required_true) is not True:
            raise ProspectiveEventResponseV3Error(f"missing_{required_true}")
    if float(payload.get("maximum_clock_attestation_age_sec") or 0) != 90.0:
        raise ProspectiveEventResponseV3Error("clock_attestation_age_must_be_90_seconds")
    if str(payload.get("source_producer_ownership") or "") != "independent_supervisor_owned":
        raise ProspectiveEventResponseV3Error("source_producer_must_be_independent")
    if payload.get("worker_runs_catalog_sync") is not False:
        raise ProspectiveEventResponseV3Error("target_worker_must_not_run_catalog_sync")
    if payload.get("worker_runs_immutable_clock_writer") is not False:
        raise ProspectiveEventResponseV3Error("target_worker_must_not_write_event_clock")
    event_selection = payload.get("event_selection") or {}
    if event_selection.get("require_source_contract_lineage") is not True:
        raise ProspectiveEventResponseV3Error(
            "source_contract_lineage_must_be_required"
        )
    if str(event_selection.get("require_exact_timing_precision") or "") != "minute":
        raise ProspectiveEventResponseV3Error("exact_minute_timing_must_be_required")
    if event_selection.get("require_independent_domestic_event") is not True:
        raise ProspectiveEventResponseV3Error(
            "independent_domestic_event_must_be_required"
        )
    if event_selection.get("allowed_clock_semantics") != [
        "domestic_official_policy_release",
        "domestic_official_statistical_release",
    ]:
        raise ProspectiveEventResponseV3Error("unexpected_clock_semantics_contract")
    if event_selection.get("require_material_content_sha256") is not True:
        raise ProspectiveEventResponseV3Error("material_content_hash_must_be_required")
    worker = payload.get("worker_cadence") or {}
    expected_worker = {
        "idle_cycle_sec": 15,
        "active_poll_sec": 1,
        "active_lead_sec": 2,
        "active_lag_sec": 45,
    }
    if any(float(worker.get(key, -1)) != float(value) for key, value in expected_worker.items()):
        raise ProspectiveEventResponseV3Error("unexpected_worker_cadence_contract")
    return payload


def validate_clock_freshness(
    snapshot: Mapping[str, Any],
    *,
    at_utc: dt.datetime,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Require both a fresh re-read and a fresh upstream source publication."""

    at = at_utc.astimezone(UTC)
    maximum_age = float(contract.get("maximum_clock_attestation_age_sec") or 0)
    required_state = str(contract.get("required_clock_attestation_state") or "")
    observation_id = str(snapshot.get("clock_observation_id") or "")
    observed = _parse_utc(snapshot.get("clock_observed_utc"), field="clock_observed_utc")
    source_generated = _parse_utc(
        snapshot.get("clock_observation_source_generated_utc"),
        field="clock_observation_source_generated_utc",
    )
    attestation = snapshot.get("clock_attestation")
    if not observation_id or observed is None or source_generated is None:
        raise ProspectiveEventResponseV3Error("immutable_clock_observation_lineage_missing")
    if not isinstance(attestation, Mapping):
        raise ProspectiveEventResponseV3Error("immutable_clock_attestation_missing")
    generated = _parse_utc(
        attestation.get("generated_utc"), field="clock_attestation_generated_utc"
    )
    if generated is None:
        raise ProspectiveEventResponseV3Error("immutable_clock_attestation_generated_missing")
    if str(attestation.get("state") or "") != required_state or not bool(
        attestation.get("attested")
    ):
        raise ProspectiveEventResponseV3Error("immutable_clock_attestation_not_fresh_trusted")
    clocks = {
        "clock_observed_utc": observed,
        "clock_observation_source_generated_utc": source_generated,
        "clock_attestation_generated_utc": generated,
    }
    for name, value in clocks.items():
        age = (at - value).total_seconds()
        if age < 0:
            raise ProspectiveEventResponseV3Error(f"{name}_in_future")
        if maximum_age <= 0 or age > maximum_age:
            raise ProspectiveEventResponseV3Error(f"{name}_stale")
    declared_age = _finite(attestation.get("age_at_capture_sec"))
    calculated_age = (observed - generated).total_seconds()
    if declared_age is None or abs(declared_age - calculated_age) > 1.0:
        raise ProspectiveEventResponseV3Error("clock_attestation_age_lineage_mismatch")
    payload_sha = str(attestation.get("payload_sha256") or "")
    artifact = str(attestation.get("artifact_name") or "")
    if not payload_sha or not artifact:
        raise ProspectiveEventResponseV3Error("clock_attestation_identity_missing")
    return {
        "clock_observation_id": observation_id,
        "clock_observed_utc": _iso(observed),
        "clock_observation_source_generated_utc": _iso(source_generated),
        "clock_attestation_state": required_state,
        "clock_attested": 1,
        "clock_attestation_artifact_name": artifact,
        "clock_attestation_generated_utc": _iso(generated),
        "clock_attestation_age_sec": round(declared_age, 6),
        "clock_attestation_payload_sha256": payload_sha,
    }


def _attempt(
    reason: str,
    *,
    source_ref: str,
    observed_utc: dt.datetime,
    phase: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    observed = observed_utc.astimezone(UTC)
    identity = {
        "reason": reason,
        "source_ref": source_ref,
        "observed_utc": _iso(observed),
        "phase": phase,
    }
    fields = {
        "reason": reason,
        "source_ref": source_ref,
        "observed_utc": _iso(observed),
        "phase": phase,
        "terminal": 0,
        "details_json": _canonical_json(dict(details or {})),
        "research_only": 1,
        "execution_eligible": 0,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    return _finalize_record(
        id_field="attempt_id",
        id_prefix="event_response_attempt_v3",
        identity_material=identity,
        evidence_fields=fields,
    )


def transient_attempt_record(
    reason: str,
    *,
    source_ref: str,
    observed_utc: dt.datetime,
    phase: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a governed nonterminal diagnostic for a failed capture attempt."""

    return _attempt(
        reason,
        source_ref=source_ref,
        observed_utc=observed_utc,
        phase=phase,
        details=details,
    )


def _terminal_exclusion(
    reason: str,
    *,
    plan_id: str,
    observed_utc: dt.datetime,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if reason not in {
        "target_window_expired_without_valid_observation",
        "event_clock_revised_or_removed_before_baseline",
    }:
        raise ProspectiveEventResponseV3Error("nonterminal_reason_sent_to_terminal_ledger")
    observed = observed_utc.astimezone(UTC)
    identity = {"reason": reason, "plan_id": plan_id}
    fields = {
        "plan_id": plan_id,
        "reason": reason,
        "observed_utc": _iso(observed),
        "details_json": _canonical_json(dict(details or {})),
        "terminal": 1,
        "research_only": 1,
        "execution_eligible": 0,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    return _finalize_record(
        id_field="exclusion_id",
        id_prefix="event_response_terminal_exclusion_v3",
        identity_material=identity,
        evidence_fields=fields,
    )


def build_event_plans(
    clock_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    instruments: Sequence[str],
    policy_dependency_registry: Mapping[str, Any],
    planned_utc: dt.datetime,
) -> dict[str, Any]:
    """Build fully attested 68-pair event snapshots from eligible clocks.

    The v2 selector is used only to identify causally eligible exact official
    events.  V3 then prospectively freezes the *complete* executable universe
    for each event.  This prevents direct-leg-only selection from hiding
    cross-currency propagation or unaffected controls.
    """

    planned = planned_utc.astimezone(UTC)
    freshness = validate_clock_freshness(clock_snapshot, at_utc=planned, contract=contract)
    source_pipeline_version = str(clock_snapshot.get("source_pipeline_version") or "")
    if source_pipeline_version != str(
        contract.get("required_source_pipeline_version") or ""
    ):
        raise ProspectiveEventResponseV3Error("event_source_pipeline_version_mismatch")
    if str(clock_snapshot.get("contract_id") or "") != str(
        contract.get("immutable_event_clock_contract_id") or ""
    ):
        raise ProspectiveEventResponseV3Error("event_clock_contract_id_mismatch")
    v2_contract = dict(contract)
    v2_contract["contract_id"] = v2.CONTRACT_ID
    try:
        built = v2.build_event_plans(
            clock_snapshot,
            contract=v2_contract,
            instruments=instruments,
            planned_utc=planned,
        )
    except v2.ProspectiveEventResponseError as exc:
        raise ProspectiveEventResponseV3Error(str(exc)) from exc
    normalized_instruments = tuple(
        sorted({str(value).strip().upper() for value in instruments})
    )
    offsets = tuple(int(value) for value in contract["target_offsets_sec"])
    events_by_version = {
        str(row.get("event_version_id") or ""): row
        for row in clock_snapshot.get("events") or []
        if isinstance(row, Mapping)
    }
    eligible_templates: dict[str, Mapping[str, Any]] = {}
    for source in built["plans"]:
        eligible_templates.setdefault(str(source["event_instance_id"]), source)

    lineage_attempts: list[dict[str, Any]] = []

    def is_policy_event(event: Mapping[str, Any]) -> bool:
        text = " ".join(
            (
                str(event.get("category") or ""),
                str(event.get("headline") or ""),
                str(event.get("upstream_event_id") or ""),
            )
        ).lower()
        return any(
            token in text
            for token in (
                "monetary_policy",
                "monetary policy",
                "policy decision",
                "policy rate",
                "interest rate decision",
                "loan prime rate",
            )
        )

    dependencies = [
        row
        for row in policy_dependency_registry.get("dependencies") or []
        if isinstance(row, Mapping) and not bool(row.get("assign_direction"))
    ]
    plans: list[dict[str, Any]] = []
    relationship_counts: Counter[str] = Counter()
    represented_currencies: set[str] = set()
    direct_event_currencies: set[str] = set()
    for event_instance_id, template in sorted(eligible_templates.items()):
        event = events_by_version.get(str(template["event_version_id"])) or {}
        event_selection = contract.get("event_selection") or {}
        semantic_requirements = {
            "timing_precision": str(event.get("timing_precision") or "")
            == str(event_selection.get("require_exact_timing_precision") or ""),
            "independent_domestic_event": event.get("independent_domestic_event")
            is True,
            "clock_semantics": str(event.get("clock_semantics") or "")
            in {
                str(value)
                for value in event_selection.get("allowed_clock_semantics") or []
            },
            "material_content_sha256": _is_sha256(
                event.get("material_content_sha256")
            ),
        }
        failed_semantics = sorted(
            key for key, accepted in semantic_requirements.items() if not accepted
        )
        if failed_semantics:
            lineage_attempts.append(
                _attempt(
                    "exact_official_event_semantics_missing",
                    source_ref=str(template.get("event_version_id") or ""),
                    observed_utc=planned,
                    phase="planning",
                    details={"failed_fields": failed_semantics},
                )
            )
            continue
        if bool((contract.get("event_selection") or {}).get("require_source_contract_lineage")):
            required_lineage = {
                "source_id": str(event.get("source_id") or ""),
                "source_contract_id": str(event.get("source_contract_id") or ""),
                "source_cohort_id": str(event.get("source_cohort_id") or ""),
                "original_fact_known_utc": str(
                    event.get("original_fact_known_utc") or ""
                ),
                "event_snapshot_first_observed_utc": str(
                    event.get("event_snapshot_first_observed_utc") or ""
                ),
                "event_snapshot_first_trusted_observed_utc": str(
                    event.get("event_snapshot_first_trusted_observed_utc") or ""
                ),
                "source_contract_first_observed_utc": str(
                    event.get("source_contract_first_observed_utc") or ""
                ),
                "source_contract_first_trusted_observed_utc": str(
                    event.get("source_contract_first_trusted_observed_utc") or ""
                ),
                "event_availability_utc": str(
                    event.get("event_availability_utc") or ""
                ),
            }
            missing = sorted(key for key, value in required_lineage.items() if not value)
            if missing:
                lineage_attempts.append(
                    _attempt(
                        "source_contract_lineage_missing",
                        source_ref=str(template.get("event_version_id") or ""),
                        observed_utc=planned,
                        phase="planning",
                        details={"missing_fields": missing},
                    )
                )
                continue
            original_known = _parse_utc(
                required_lineage["original_fact_known_utc"],
                field="original_fact_known_utc",
                required=True,
            )
            snapshot_first = _parse_utc(
                required_lineage["event_snapshot_first_observed_utc"],
                field="event_snapshot_first_observed_utc",
                required=True,
            )
            snapshot_first_trusted = _parse_utc(
                required_lineage["event_snapshot_first_trusted_observed_utc"],
                field="event_snapshot_first_trusted_observed_utc",
                required=True,
            )
            contract_first = _parse_utc(
                required_lineage["source_contract_first_observed_utc"],
                field="source_contract_first_observed_utc",
                required=True,
            )
            contract_first_trusted = _parse_utc(
                required_lineage["source_contract_first_trusted_observed_utc"],
                field="source_contract_first_trusted_observed_utc",
                required=True,
            )
            availability = _parse_utc(
                required_lineage["event_availability_utc"],
                field="event_availability_utc",
                required=True,
            )
            assert all(
                value is not None
                for value in (
                    original_known,
                    snapshot_first,
                    snapshot_first_trusted,
                    contract_first,
                    contract_first_trusted,
                    availability,
                )
            )
            expected_availability = max(
                original_known,
                snapshot_first_trusted,
                contract_first_trusted,
            )
            if availability != expected_availability:
                raise ProspectiveEventResponseV3Error(
                    "event_availability_not_maximum_causal_clock"
                )
            event_known = _parse_utc(
                template.get("event_known_utc"),
                field="event_known_utc",
                required=True,
            )
            assert event_known is not None
            if event_known != availability:
                raise ProspectiveEventResponseV3Error(
                    "event_known_clock_does_not_match_event_availability"
                )
            if availability > planned:
                raise ProspectiveEventResponseV3Error(
                    "event_availability_after_planning_cutoff"
                )
        direct_currencies = sorted(
            {
                str(value).upper()
                for value in event.get("direct_currencies") or []
                if str(value).strip()
            }
        )
        direct_event_currencies.update(direct_currencies)
        linked_rows: list[Mapping[str, Any]] = []
        if is_policy_event(event):
            linked_rows = [
                row
                for row in dependencies
                if str(row.get("driver_currency") or "").upper() in direct_currencies
                and str(row.get("driver_event_category") or "")
                in {"", str(event.get("category") or "")}
            ]
        linked_currencies = sorted(
            {str(row.get("dependent_currency") or "").upper() for row in linked_rows}
        )
        dependency_by_currency = {
            str(row.get("dependent_currency") or "").upper(): row
            for row in linked_rows
        }
        scheduled = _parse_utc(
            template.get("scheduled_utc"), field="scheduled_utc", required=True
        )
        assert scheduled is not None
        for instrument in normalized_instruments:
            legs = instrument.split("_")
            if len(legs) != 2:
                raise ProspectiveEventResponseV3Error(f"invalid_instrument:{instrument}")
            base, quote = legs
            represented_currencies.update(legs)
            direct_legs = sorted(set(legs).intersection(direct_currencies))
            linked_legs = sorted(set(legs).intersection(linked_currencies))
            if direct_legs:
                relationship = "direct_leg"
            elif linked_legs:
                relationship = "linked_policy_dependency"
            else:
                relationship = "unaffected_control"
            relationship_counts[relationship] += len(offsets)
            driver_currencies = sorted(
                {
                    str(dependency_by_currency[currency].get("driver_currency") or "").upper()
                    for currency in linked_legs
                    if currency in dependency_by_currency
                }
                | set(direct_legs)
            )
            factor_driver = (
                driver_currencies[0]
                if len(driver_currencies) == 1
                else "+".join(direct_currencies)
            )
            underlying_factor_id = _id(
                "official_event_factor_v3",
                {
                    "event_instance_id": event_instance_id,
                    "driver_currency": factor_driver,
                },
            )
            linked_contract = [
                {
                    "dependent_currency": currency,
                    "driver_currency": str(
                        dependency_by_currency[currency].get("driver_currency") or ""
                    ).upper(),
                    "mechanism": str(
                        dependency_by_currency[currency].get("mechanism") or ""
                    ),
                    "assign_direction": False,
                }
                for currency in linked_legs
                if currency in dependency_by_currency
            ]
            for offset in offsets:
                target = scheduled + dt.timedelta(seconds=offset)
                source = template
                identity = {
                    "contract_id": contract["contract_id"],
                    "cohort_id": contract["cohort_id"],
                    "source_pipeline_version": source_pipeline_version,
                    "event_instance_id": event_instance_id,
                    "event_version_id": source["event_version_id"],
                    "upstream_event_id": source["upstream_event_id"],
                    "scheduled_utc": _iso(scheduled),
                    "instrument": instrument,
                    "target_offset_sec": offset,
                    "target_utc": _iso(target),
                }
                fields = {
                    "contract_id": str(contract["contract_id"]),
                    "cohort_id": str(contract["cohort_id"]),
                    "event_instance_id": event_instance_id,
                    "event_version_id": str(source["event_version_id"]),
                    "upstream_event_id": str(source["upstream_event_id"]),
                    "clock_snapshot_id": str(source["clock_snapshot_id"]),
                    "clock_snapshot_captured_utc": str(
                        source["clock_snapshot_captured_utc"]
                    ),
                    "source_pipeline_version": source_pipeline_version,
                    **freshness,
                    "semantic_snapshot_events_sha256": str(
                        clock_snapshot.get("semantic_snapshot_events_sha256") or ""
                    ),
                    "semantic_snapshot_manifest_sha256": str(
                        clock_snapshot.get("semantic_snapshot_manifest_sha256") or ""
                    ),
                    "clock_observation_events_sha256": str(
                        clock_snapshot.get("clock_observation_events_sha256") or ""
                    ),
                    "clock_observation_manifest_sha256": str(
                        clock_snapshot.get("clock_observation_manifest_sha256") or ""
                    ),
                    "event_known_utc": str(source["event_known_utc"]),
                    "original_fact_known_utc": str(
                        event.get("original_fact_known_utc") or ""
                    ),
                    "event_snapshot_first_observed_utc": str(
                        event.get("event_snapshot_first_observed_utc") or ""
                    ),
                    "event_snapshot_first_trusted_observed_utc": str(
                        event.get("event_snapshot_first_trusted_observed_utc") or ""
                    ),
                    "source_id": str(event.get("source_id") or ""),
                    "source_contract_id": str(
                        event.get("source_contract_id") or ""
                    ),
                    "source_cohort_id": str(event.get("source_cohort_id") or ""),
                    "source_contract_first_observed_utc": str(
                        event.get("source_contract_first_observed_utc") or ""
                    ),
                    "source_contract_first_trusted_observed_utc": str(
                        event.get("source_contract_first_trusted_observed_utc") or ""
                    ),
                    "event_availability_utc": str(
                        event.get("event_availability_utc") or ""
                    ),
                    "source_material_content_sha256": str(
                        event.get("material_content_sha256") or ""
                    ),
                    "source_timing_precision": str(
                        event.get("timing_precision") or ""
                    ),
                    "source_clock_semantics": str(
                        event.get("clock_semantics") or ""
                    ),
                    "source_independent_domestic_event": int(
                        event.get("independent_domestic_event") is True
                    ),
                    "schedule_lock_utc": str(source["schedule_lock_utc"]),
                    "scheduled_utc": _iso(scheduled),
                    "headline": str(source["headline"]),
                    "category": str(source["category"]),
                    "source_name": str(source["source_name"]),
                    "source_url": str(source["source_url"]),
                    "direct_currencies_json": _canonical_json(direct_currencies),
                    "relationship": relationship,
                    "relationship_currencies_json": _canonical_json(
                        direct_legs if direct_legs else linked_legs
                    ),
                    "linked_policy_dependencies_json": _canonical_json(linked_contract),
                    "underlying_factor_id": underlying_factor_id,
                    "independent_confirmation_eligible": 0,
                    "direction_assigned": 0,
                    "instrument": instrument,
                    "target_offset_sec": offset,
                    "target_utc": _iso(target),
                    "sample_role": "baseline" if offset == 0 else "outcome",
                    "first_planned_utc": _iso(planned),
                    "instrument_universe_sha256": str(
                        built["instrument_universe_sha256"]
                    ),
                    "parent_v2_plan_id": str(source["plan_id"]),
                    "research_only": 1,
                    "execution_eligible": 0,
                    "can_place_orders": 0,
                    "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
                }
                plans.append(
                    _finalize_record(
                        id_field="plan_id",
                        id_prefix="event_response_plan_v3",
                        identity_material=identity,
                        evidence_fields=fields,
                    )
                )
    attempts = [
        _attempt(
            str(row.get("reason") or "planning_selection_rejected"),
            source_ref=str(row.get("source_ref") or ""),
            observed_utc=planned,
            phase="planning",
            details=row.get("details") if isinstance(row.get("details"), Mapping) else {},
        )
        for row in built.get("exclusions") or []
    ] + lineage_attempts
    plans.sort(key=lambda row: (row["target_utc"], row["plan_id"]))
    attempts.sort(key=lambda row: row["attempt_id"])
    return {
        "contract_id": contract["contract_id"],
        "cohort_id": contract["cohort_id"],
        "clock_snapshot_id": str(clock_snapshot.get("snapshot_id") or ""),
        "planned_utc": _iso(planned),
        "instrument_universe_sha256": built["instrument_universe_sha256"],
        "plans": plans,
        "attempts": attempts,
        "plan_count": len(plans),
        "event_instance_count": len({row["event_instance_id"] for row in plans}),
        "relationship_counts": dict(sorted(relationship_counts.items())),
        "represented_currencies": sorted(represented_currencies),
        "direct_event_currencies": sorted(direct_event_currencies),
    }


def quote_snapshot_candidates(
    payload: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    artifact_sha256: str,
    collector_observed_utc: dt.datetime,
    market_clock_attestation: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Normalize live quotes while binding complete market-clock attestation."""

    collector = collector_observed_utc.astimezone(UTC)
    required = str(contract.get("required_clock_attestation_state") or "")
    maximum_age = float(contract.get("maximum_clock_attestation_age_sec") or 0)
    state = str(market_clock_attestation.get("state") or "")
    attested = bool(market_clock_attestation.get("attested"))
    generated = _parse_utc(
        market_clock_attestation.get("generated_utc"),
        field="market_clock_attestation_generated_utc",
    )
    if state != required or not attested or generated is None:
        raise ProspectiveEventResponseV3Error("market_clock_attestation_not_fresh_trusted")
    age = (collector - generated).total_seconds()
    declared_age = _finite(market_clock_attestation.get("age_at_capture_sec"))
    if age < 0 or maximum_age <= 0 or age > maximum_age:
        raise ProspectiveEventResponseV3Error("market_clock_attestation_stale")
    if declared_age is None or abs(declared_age - age) > 1.0:
        raise ProspectiveEventResponseV3Error("market_clock_attestation_age_mismatch")
    artifact_name = str(market_clock_attestation.get("artifact_name") or "")
    attestation_sha = str(market_clock_attestation.get("payload_sha256") or "")
    if not artifact_name or not attestation_sha:
        raise ProspectiveEventResponseV3Error("market_clock_attestation_identity_missing")
    v2_contract = dict(contract)
    v2_contract["contract_id"] = v2.CONTRACT_ID
    try:
        rows = v2.quote_snapshot_candidates(
            payload,
            contract=v2_contract,
            artifact_sha256=artifact_sha256,
            collector_observed_utc=collector,
            clock_attestation=market_clock_attestation,
        )
    except v2.ProspectiveEventResponseError as exc:
        raise ProspectiveEventResponseV3Error(str(exc)) from exc
    output: list[dict[str, Any]] = []
    for row in rows:
        identity = {
            "instrument": row["instrument"],
            "quote_time_utc": row["quote_time_utc"],
            "bid": row["bid"],
            "ask": row["ask"],
            "pip": row["pip"],
            "source_payload_sha256": row["source_payload_sha256"],
            "market_clock_attestation_payload_sha256": attestation_sha,
            "market_clock_attestation_generated_utc": _iso(generated),
        }
        normalized = {
            **{key: value for key, value in row.items() if key != "source_record_id"},
            "market_clock_attestation_state": state,
            "market_clock_attested": 1,
            "market_clock_attestation_artifact_name": artifact_name,
            "market_clock_attestation_generated_utc": _iso(generated),
            "market_clock_attestation_age_sec": round(declared_age, 6),
            "market_clock_attestation_payload_sha256": attestation_sha,
        }
        normalized["source_record_id"] = _id("live_quote_v3", identity)
        output.append(normalized)
    return output


def build_samples(
    plans: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]],
    *,
    existing_samples: Sequence[Mapping[str, Any]],
    current_clock_snapshot: Mapping[str, Any],
    contract: Mapping[str, Any],
    collector_observed_utc: dt.datetime,
) -> dict[str, Any]:
    """Capture the first valid causal observation for each active plan.

    Exact event/schedule version is mandatory through baseline offset zero.
    Once a baseline exists, later endpoints remain bound to that frozen event
    lineage even when the live news artifact is revised, relabeled, or pruned.
    """

    collector = collector_observed_utc.astimezone(UTC)
    freshness = validate_clock_freshness(
        current_clock_snapshot, at_utc=collector, contract=contract
    )
    sampling = contract.get("sampling") or {}
    accepted_sources = {str(value) for value in sampling.get("accepted_source_types") or []}
    late = float(sampling.get("maximum_target_lateness_sec") or 0)
    lead = float(sampling.get("maximum_quote_lead_sec") or 0)
    max_quote_age = float(sampling.get("maximum_quote_age_at_collection_sec") or 0)
    max_snapshot_age = float(sampling.get("maximum_snapshot_age_at_collection_sec") or 0)
    future_skew = float(sampling.get("maximum_future_clock_skew_sec") or 0)
    max_spread = float(sampling.get("maximum_spread_pips_sanity") or 0)
    required_attestation = str(contract.get("required_clock_attestation_state") or "")
    cohort_start = _parse_utc(
        contract.get("cohort_start_utc"), field="cohort_start_utc", required=True
    )
    assert cohort_start is not None
    # Recurring official series legitimately reuse one upstream identifier for
    # several scheduled releases.  Index by the immutable version instead of
    # upstream ID so a later release cannot shadow an earlier still-valid one.
    current_events_by_version = {
        str(row.get("event_version_id") or ""): row
        for row in current_clock_snapshot.get("events") or []
        if isinstance(row, Mapping) and str(row.get("event_version_id") or "")
    }
    samples_by_plan = {
        str(row.get("plan_id") or ""): row for row in existing_samples
    }
    baseline_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for plan in plans:
        if int(plan.get("target_offset_sec") or 0) != 0:
            continue
        sample = samples_by_plan.get(str(plan.get("plan_id") or ""))
        if sample is not None:
            baseline_by_key[
                (
                    str(plan.get("event_instance_id") or ""),
                    str(plan.get("instrument") or ""),
                )
            ] = sample
    # Existing plans passed to this call may be only the active workset.  Index
    # baselines directly from sample lineage as a fallback.
    for sample in existing_samples:
        if int(sample.get("target_offset_sec") or 0) == 0:
            baseline_by_key[
                (
                    str(sample.get("event_instance_id") or ""),
                    str(sample.get("instrument") or ""),
                )
            ] = sample

    by_instrument: dict[str, list[Mapping[str, Any]]] = {}
    for observation in observations:
        instrument = str(observation.get("instrument") or "").upper()
        if instrument:
            by_instrument.setdefault(instrument, []).append(observation)

    samples: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    terminal_exclusions: list[dict[str, Any]] = []
    for plan in plans:
        plan_id = str(plan.get("plan_id") or "")
        target = _parse_utc(plan.get("target_utc"), field="target_utc", required=True)
        assert target is not None
        if collector < target or (collector - target).total_seconds() > late:
            continue
        offset = int(plan.get("target_offset_sec") or 0)
        key = (
            str(plan.get("event_instance_id") or ""),
            str(plan.get("instrument") or ""),
        )
        lineage_state = "baseline_exact_event_version"
        if offset == 0:
            current_event = current_events_by_version.get(
                str(plan.get("event_version_id") or "")
            )
            if (
                current_event is None
                or str(current_event.get("upstream_event_id") or "")
                != str(plan.get("upstream_event_id") or "")
                or str(current_event.get("scheduled_utc") or "")
                != str(plan.get("scheduled_utc") or "")
            ):
                terminal_exclusions.append(
                    _terminal_exclusion(
                        "event_clock_revised_or_removed_before_baseline",
                        plan_id=plan_id,
                        observed_utc=collector,
                        details={
                            "frozen_event_version_id": str(
                                plan.get("event_version_id") or ""
                            ),
                            "current_event_version_id": str(
                                (current_event or {}).get("event_version_id") or ""
                            ),
                        },
                    )
                )
                continue
        else:
            if key not in baseline_by_key:
                attempts.append(
                    _attempt(
                        "endpoint_waiting_for_valid_baseline",
                        source_ref=plan_id,
                        observed_utc=collector,
                        phase="sampling",
                        details={"target_utc": _iso(target)},
                    )
                )
                continue
            lineage_state = "postbaseline_frozen_event_lineage"

        invalid_reasons: Counter[str] = Counter()
        valid: list[tuple[dt.datetime, str, dict[str, Any]]] = []
        for observation in by_instrument.get(str(plan.get("instrument") or "").upper(), []):
            source_type = str(observation.get("source_type") or "")
            if source_type not in accepted_sources:
                invalid_reasons["market_source_type_not_allowed"] += 1
                continue
            if str(observation.get("market_clock_attestation_state") or "") != required_attestation:
                invalid_reasons["market_observation_clock_not_attested"] += 1
                continue
            if int(observation.get("market_clock_attested") or 0) != 1:
                invalid_reasons["market_observation_clock_not_attested"] += 1
                continue
            market_attestation_generated = _parse_utc(
                observation.get("market_clock_attestation_generated_utc"),
                field="market_clock_attestation_generated_utc",
            )
            market_attestation_age = _finite(
                observation.get("market_clock_attestation_age_sec")
            )
            if (
                market_attestation_generated is None
                or market_attestation_age is None
                or not str(
                    observation.get("market_clock_attestation_artifact_name") or ""
                )
                or not str(
                    observation.get("market_clock_attestation_payload_sha256") or ""
                )
            ):
                invalid_reasons["market_attestation_lineage_missing"] += 1
                continue
            if (
                market_attestation_age < 0
                or market_attestation_age
                > float(contract.get("maximum_clock_attestation_age_sec") or 0)
                or market_attestation_generated > collector
            ):
                invalid_reasons["market_attestation_stale_or_future"] += 1
                continue
            if source_type == "immutable_completed_bid_ask_candle" and bool(
                sampling.get("require_immutable_prospective_candle_lineage")
            ) and not bool(observation.get("immutable_prospective_lineage")):
                invalid_reasons["candle_missing_immutable_prospective_lineage"] += 1
                continue
            source_record_id = str(observation.get("source_record_id") or "")
            source_payload_sha = str(observation.get("source_payload_sha256") or "")
            if not source_record_id or not source_payload_sha:
                invalid_reasons["market_observation_missing_immutable_source_identity"] += 1
                continue
            bid = _finite(observation.get("bid"))
            ask = _finite(observation.get("ask"))
            pip = _finite(observation.get("pip"))
            quote_time = _parse_utc(observation.get("quote_time_utc"), field="quote_time_utc")
            generated = _parse_utc(
                observation.get("source_generated_utc"), field="source_generated_utc"
            )
            observed = _parse_utc(
                observation.get("collector_observed_utc"), field="collector_observed_utc"
            )
            known = _parse_utc(
                observation.get("observation_known_utc"), field="observation_known_utc"
            )
            if (
                bid is None
                or ask is None
                or pip is None
                or quote_time is None
                or generated is None
                or observed is None
                or known is None
            ):
                invalid_reasons["market_observation_missing_value_or_clock"] += 1
                continue
            if bid <= 0 or ask <= bid or pip <= 0:
                invalid_reasons["invalid_executable_bid_ask"] += 1
                continue
            spread_pips = (ask - bid) / pip
            if max_spread > 0 and spread_pips > max_spread:
                invalid_reasons["spread_exceeds_sanity_limit"] += 1
                continue
            lineage_clocks = (quote_time, generated, observed, known)
            if any(value > collector + dt.timedelta(seconds=future_skew) for value in lineage_clocks):
                invalid_reasons["market_clock_ahead_of_collector"] += 1
                continue
            if bool(contract.get("exclude_pre_cohort_observations")) and any(
                value < cohort_start for value in lineage_clocks
            ):
                invalid_reasons["pre_cohort_market_observation"] += 1
                continue
            if abs((known - max(quote_time, generated, observed)).total_seconds()) > 1e-6:
                invalid_reasons["observation_known_clock_not_max_lineage_clock"] += 1
                continue
            if (collector - quote_time).total_seconds() > max_quote_age:
                invalid_reasons["stale_quote_at_collection"] += 1
                continue
            if (collector - generated).total_seconds() > max_snapshot_age:
                invalid_reasons["stale_market_snapshot_at_collection"] += 1
                continue
            alignment = (quote_time - target).total_seconds()
            if alignment < -lead or alignment > late:
                invalid_reasons["market_time_outside_predeclared_target_window"] += 1
                continue
            identity = {
                "contract_id": contract["contract_id"],
                "cohort_id": contract["cohort_id"],
                "plan_id": plan_id,
                "source_record_id": source_record_id,
                "quote_time_utc": _iso(quote_time),
            }
            fields = {
                "plan_id": plan_id,
                "contract_id": str(contract["contract_id"]),
                "cohort_id": str(contract["cohort_id"]),
                "event_instance_id": str(plan.get("event_instance_id") or ""),
                "event_version_id": str(plan.get("event_version_id") or ""),
                "validation_clock_snapshot_id": str(
                    current_clock_snapshot.get("snapshot_id") or ""
                ),
                **{f"validation_{key}": value for key, value in freshness.items()},
                "event_lineage_state": lineage_state,
                "instrument": str(plan.get("instrument") or ""),
                "target_offset_sec": offset,
                "target_utc": str(plan.get("target_utc") or ""),
                "sample_role": str(plan.get("sample_role") or ""),
                "source_type": source_type,
                "source_record_id": source_record_id,
                "source_payload_sha256": source_payload_sha,
                "collector_observed_utc": _iso(observed),
                "source_generated_utc": _iso(generated),
                "quote_time_utc": _iso(quote_time),
                "observation_known_utc": _iso(known),
                "target_distance_sec": round(alignment, 6),
                "bid": bid,
                "ask": ask,
                "mid": (bid + ask) / 2.0,
                "pip": pip,
                "spread_pips": spread_pips,
                "market_clock_attestation_state": str(
                    observation.get("market_clock_attestation_state") or ""
                ),
                "market_clock_attested": 1,
                "market_clock_attestation_artifact_name": str(
                    observation.get("market_clock_attestation_artifact_name") or ""
                ),
                "market_clock_attestation_generated_utc": _iso(
                    market_attestation_generated
                ),
                "market_clock_attestation_age_sec": market_attestation_age,
                "market_clock_attestation_payload_sha256": str(
                    observation.get("market_clock_attestation_payload_sha256") or ""
                ),
                "immutable_prospective_lineage": int(
                    bool(observation.get("immutable_prospective_lineage"))
                ),
                "research_only": 1,
                "execution_eligible": 0,
                "can_place_orders": 0,
                "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
            }
            sample = _finalize_record(
                id_field="sample_id",
                id_prefix="event_response_sample_v3",
                identity_material=identity,
                evidence_fields=fields,
            )
            valid.append((known, source_record_id, sample))
        if valid:
            valid.sort(key=lambda item: (item[0], item[1]))
            chosen = valid[0][2]
            samples.append(chosen)
            if offset == 0:
                baseline_by_key[key] = chosen
        else:
            reason = (
                invalid_reasons.most_common(1)[0][0]
                if invalid_reasons
                else "missing_instrument_observation"
            )
            attempts.append(
                _attempt(
                    reason,
                    source_ref=plan_id,
                    observed_utc=collector,
                    phase="sampling",
                    details={"reason_counts": dict(sorted(invalid_reasons.items()))},
                )
            )
    samples.sort(key=lambda row: (row["observation_known_utc"], row["plan_id"]))
    attempts.sort(key=lambda row: row["attempt_id"])
    terminal_exclusions.sort(key=lambda row: row["exclusion_id"])
    return {
        "samples": samples,
        "attempts": attempts,
        "terminal_exclusions": terminal_exclusions,
        "sample_count": len(samples),
    }


def collection_workset(
    plans: Sequence[Mapping[str, Any]],
    samples: Sequence[Mapping[str, Any]],
    terminal_exclusions: Sequence[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    collector_observed_utc: dt.datetime,
) -> dict[str, Any]:
    collector = collector_observed_utc.astimezone(UTC)
    late = float((contract.get("sampling") or {}).get("maximum_target_lateness_sec") or 0)
    sampled_plan_ids = {str(row.get("plan_id") or "") for row in samples}
    terminal_plan_ids = {str(row.get("plan_id") or "") for row in terminal_exclusions}
    active: list[Mapping[str, Any]] = []
    expired: list[dict[str, Any]] = []
    future = 0
    for plan in plans:
        plan_id = str(plan.get("plan_id") or "")
        if plan_id in sampled_plan_ids or plan_id in terminal_plan_ids:
            continue
        target = _parse_utc(plan.get("target_utc"), field="target_utc", required=True)
        assert target is not None
        if target > collector:
            future += 1
        elif collector <= target + dt.timedelta(seconds=late):
            active.append(plan)
        else:
            expired.append(
                _terminal_exclusion(
                    "target_window_expired_without_valid_observation",
                    plan_id=plan_id,
                    observed_utc=collector,
                    details={"target_utc": _iso(target)},
                )
            )
    return {
        "active_plans": active,
        "expired_exclusions": expired,
        "future_plan_count": future,
        "active_plan_count": len(active),
        "expired_plan_count": len(expired),
    }


def mature_outcome_rows(
    plans: Sequence[Mapping[str, Any]],
    samples: Sequence[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    recorded_utc: dt.datetime,
) -> list[dict[str, Any]]:
    """Mature endpoint economics and truthfully classify exact observations."""

    recorded = recorded_utc.astimezone(UTC)
    plans_by_id = {str(row.get("plan_id") or ""): row for row in plans}
    sample_by_plan = {str(row.get("plan_id") or ""): row for row in samples}
    tolerance = float((contract.get("outcome") or {}).get("exact_horizon_tolerance_sec") or 0)
    cohort_start = _parse_utc(
        contract.get("cohort_start_utc"), field="cohort_start_utc", required=True
    )
    assert cohort_start is not None
    baselines: dict[tuple[str, str], Mapping[str, Any]] = {}
    for plan_id, plan in plans_by_id.items():
        if int(plan.get("target_offset_sec") or 0) == 0 and plan_id in sample_by_plan:
            baselines[
                (
                    str(plan.get("event_instance_id") or ""),
                    str(plan.get("instrument") or ""),
                )
            ] = sample_by_plan[plan_id]
    outcomes: list[dict[str, Any]] = []
    for plan_id, endpoint_plan in sorted(plans_by_id.items()):
        horizon = int(endpoint_plan.get("target_offset_sec") or 0)
        if horizon <= 0 or plan_id not in sample_by_plan:
            continue
        key = (
            str(endpoint_plan.get("event_instance_id") or ""),
            str(endpoint_plan.get("instrument") or ""),
        )
        baseline = baselines.get(key)
        if baseline is None:
            continue
        endpoint = sample_by_plan[plan_id]
        pip = _finite(baseline.get("pip"))
        endpoint_pip = _finite(endpoint.get("pip"))
        entry_bid = _finite(baseline.get("bid"))
        entry_ask = _finite(baseline.get("ask"))
        exit_bid = _finite(endpoint.get("bid"))
        exit_ask = _finite(endpoint.get("ask"))
        entry_time = _parse_utc(
            baseline.get("quote_time_utc"), field="entry_quote_time_utc"
        )
        exit_time = _parse_utc(
            endpoint.get("quote_time_utc"), field="outcome_quote_time_utc"
        )
        baseline_known = _parse_utc(
            baseline.get("observation_known_utc"), field="baseline_observation_known_utc"
        )
        endpoint_known = _parse_utc(
            endpoint.get("observation_known_utc"), field="endpoint_observation_known_utc"
        )
        if (
            pip is None
            or endpoint_pip is None
            or abs(pip - endpoint_pip) > 1e-12
            or entry_bid is None
            or entry_ask is None
            or exit_bid is None
            or exit_ask is None
            or entry_time is None
            or exit_time is None
            or baseline_known is None
            or endpoint_known is None
        ):
            continue
        actual_duration = (exit_time - entry_time).total_seconds()
        baseline_distance = float(baseline.get("target_distance_sec") or 0)
        endpoint_distance = float(endpoint.get("target_distance_sec") or 0)
        outcome_known = max(baseline_known, endpoint_known)
        clock_order_consistent = bool(
            entry_time >= cohort_start
            and exit_time >= cohort_start
            and baseline_known >= entry_time
            and endpoint_known >= exit_time
            and exit_time > entry_time
        )
        outcome_known_after_both = bool(
            outcome_known >= baseline_known and outcome_known >= endpoint_known
        )
        maturation_recorded_after_outcome = recorded >= outcome_known
        reasons: list[str] = []
        if abs(baseline_distance) > tolerance:
            reasons.append("baseline_target_distance_exceeds_tolerance")
        if abs(endpoint_distance) > tolerance:
            reasons.append("endpoint_target_distance_exceeds_tolerance")
        if not clock_order_consistent:
            reasons.append("sample_clock_order_inconsistent")
        if actual_duration <= 0:
            reasons.append("nonpositive_observation_duration")
        if abs(actual_duration - horizon) > tolerance:
            reasons.append("observation_duration_exceeds_horizon_tolerance")
        if not outcome_known_after_both:
            reasons.append("outcome_known_before_sample_lineage")
        if not maturation_recorded_after_outcome:
            reasons.append("maturation_recorded_before_outcome_known")
        exact = not reasons
        long_net = (exit_bid - entry_ask) / pip
        short_net = (entry_bid - exit_ask) / pip
        mid_move = (
            ((exit_bid + exit_ask) / 2.0) - ((entry_bid + entry_ask) / 2.0)
        ) / pip
        identity = {
            "contract_id": contract["contract_id"],
            "cohort_id": contract["cohort_id"],
            "event_instance_id": key[0],
            "instrument": key[1],
            "horizon_sec": horizon,
            "baseline_sample_id": baseline["sample_id"],
            "outcome_sample_id": endpoint["sample_id"],
        }
        fields = {
            "contract_id": str(contract["contract_id"]),
            "cohort_id": str(contract["cohort_id"]),
            "event_instance_id": key[0],
            "instrument": key[1],
            "horizon_sec": horizon,
            "baseline_sample_id": str(baseline["sample_id"]),
            "outcome_sample_id": str(endpoint["sample_id"]),
            "entry_quote_time_utc": _iso(entry_time),
            "outcome_quote_time_utc": _iso(exit_time),
            "baseline_observation_known_utc": _iso(baseline_known),
            "endpoint_observation_known_utc": _iso(endpoint_known),
            "outcome_known_utc": _iso(outcome_known),
            "maturation_recorded_utc": _iso(recorded),
            "baseline_target_distance_sec": baseline_distance,
            "outcome_target_distance_sec": endpoint_distance,
            "actual_observation_duration_sec": actual_duration,
            "clocks_order_consistent": int(clock_order_consistent),
            "outcome_known_after_both_samples": int(outcome_known_after_both),
            "maturation_recorded_after_outcome_known": int(
                maturation_recorded_after_outcome
            ),
            "exact_horizon_observation": int(exact),
            "proof_evaluation_eligible": int(exact),
            "exactness_reasons_json": _canonical_json(reasons),
            "entry_bid": entry_bid,
            "entry_ask": entry_ask,
            "outcome_bid": exit_bid,
            "outcome_ask": exit_ask,
            "pip": pip,
            "entry_spread_pips": (entry_ask - entry_bid) / pip,
            "outcome_spread_pips": (exit_ask - exit_bid) / pip,
            "midpoint_move_pips": mid_move,
            "long_after_spread_pips": long_net,
            "short_after_spread_pips": short_net,
            "direction_selected": 0,
            "best_side_metric": None,
            "research_only": 1,
            "execution_eligible": 0,
            "can_place_orders": 0,
            "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
        }
        outcomes.append(
            _finalize_record(
                id_field="outcome_id",
                id_prefix="event_response_outcome_v3",
                identity_material=identity,
                evidence_fields=fields,
            )
        )
    outcomes.sort(
        key=lambda row: (
            row["outcome_known_utc"],
            row["event_instance_id"],
            row["instrument"],
            row["horizon_sec"],
        )
    )
    return outcomes


_TABLE_PRIMARY_KEYS = {
    "event_response_cohort_manifests_v3": "cohort_id",
    "event_response_plans_v3": "plan_id",
    "event_response_samples_v3": "sample_id",
    "event_response_outcomes_v3": "outcome_id",
    "event_response_terminal_exclusions_v3": "exclusion_id",
    "event_response_attempt_diagnostics_v3": "attempt_id",
    "event_response_worker_cycles_v3": "cycle_id",
}


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS event_response_cohort_manifests_v3(
          cohort_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          cohort_start_utc TEXT NOT NULL,
          research_generation TEXT NOT NULL CHECK(research_generation='prospective_official_event_response_capture_v3'),
          parent_cohort_id TEXT NOT NULL,
          parent_cohort_disposition TEXT NOT NULL,
          static_fingerprint_sha256 TEXT NOT NULL,
          fingerprint_json TEXT NOT NULL,
          cohort_start_source_lineage_json TEXT NOT NULL,
          source_pipeline_version TEXT NOT NULL CHECK(source_pipeline_version='all_pair_news_event_tags_v3'),
          research_genealogy_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS event_response_plans_v3(
          plan_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          event_instance_id TEXT NOT NULL,
          event_version_id TEXT NOT NULL,
          upstream_event_id TEXT NOT NULL,
          clock_snapshot_id TEXT NOT NULL,
          clock_snapshot_captured_utc TEXT NOT NULL,
          source_pipeline_version TEXT NOT NULL CHECK(source_pipeline_version='all_pair_news_event_tags_v3'),
          clock_observation_id TEXT NOT NULL,
          clock_observed_utc TEXT NOT NULL,
          clock_observation_source_generated_utc TEXT NOT NULL,
          clock_attestation_state TEXT NOT NULL,
          clock_attested INTEGER NOT NULL CHECK(clock_attested=1),
          clock_attestation_artifact_name TEXT NOT NULL,
          clock_attestation_generated_utc TEXT NOT NULL,
          clock_attestation_age_sec REAL NOT NULL CHECK(clock_attestation_age_sec>=0 AND clock_attestation_age_sec<=90),
          clock_attestation_payload_sha256 TEXT NOT NULL,
          semantic_snapshot_events_sha256 TEXT NOT NULL,
          semantic_snapshot_manifest_sha256 TEXT NOT NULL,
          clock_observation_events_sha256 TEXT NOT NULL,
          clock_observation_manifest_sha256 TEXT NOT NULL,
          event_known_utc TEXT NOT NULL,
          original_fact_known_utc TEXT NOT NULL,
          event_snapshot_first_observed_utc TEXT NOT NULL,
          event_snapshot_first_trusted_observed_utc TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_contract_id TEXT NOT NULL,
          source_cohort_id TEXT NOT NULL,
          source_contract_first_observed_utc TEXT NOT NULL,
          source_contract_first_trusted_observed_utc TEXT NOT NULL,
          event_availability_utc TEXT NOT NULL,
          source_material_content_sha256 TEXT NOT NULL CHECK(length(source_material_content_sha256)=64),
          source_timing_precision TEXT NOT NULL CHECK(source_timing_precision='minute'),
          source_clock_semantics TEXT NOT NULL CHECK(source_clock_semantics IN ('domestic_official_policy_release','domestic_official_statistical_release')),
          source_independent_domestic_event INTEGER NOT NULL CHECK(source_independent_domestic_event=1),
          schedule_lock_utc TEXT NOT NULL,
          scheduled_utc TEXT NOT NULL,
          headline TEXT NOT NULL,
          category TEXT NOT NULL,
          source_name TEXT NOT NULL,
          source_url TEXT NOT NULL,
          direct_currencies_json TEXT NOT NULL,
          relationship TEXT NOT NULL CHECK(relationship IN ('direct_leg','linked_policy_dependency','unaffected_control')),
          relationship_currencies_json TEXT NOT NULL,
          linked_policy_dependencies_json TEXT NOT NULL,
          underlying_factor_id TEXT NOT NULL,
          independent_confirmation_eligible INTEGER NOT NULL CHECK(independent_confirmation_eligible=0),
          direction_assigned INTEGER NOT NULL CHECK(direction_assigned=0),
          instrument TEXT NOT NULL,
          target_offset_sec INTEGER NOT NULL CHECK(target_offset_sec>=0),
          target_utc TEXT NOT NULL,
          sample_role TEXT NOT NULL CHECK(sample_role IN ('baseline','outcome')),
          first_planned_utc TEXT NOT NULL,
          instrument_universe_sha256 TEXT NOT NULL,
          parent_v2_plan_id TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          UNIQUE(cohort_id,event_instance_id,instrument,target_offset_sec),
          FOREIGN KEY(cohort_id) REFERENCES event_response_cohort_manifests_v3(cohort_id)
        );
        CREATE INDEX IF NOT EXISTS ix_event_response_plans_v3_due
          ON event_response_plans_v3(target_utc,instrument,target_offset_sec);

        CREATE TABLE IF NOT EXISTS event_response_samples_v3(
          sample_id TEXT PRIMARY KEY,
          plan_id TEXT NOT NULL UNIQUE,
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          event_instance_id TEXT NOT NULL,
          event_version_id TEXT NOT NULL,
          validation_clock_snapshot_id TEXT NOT NULL,
          validation_clock_observation_id TEXT NOT NULL,
          validation_clock_observed_utc TEXT NOT NULL,
          validation_clock_observation_source_generated_utc TEXT NOT NULL,
          validation_clock_attestation_state TEXT NOT NULL,
          validation_clock_attested INTEGER NOT NULL CHECK(validation_clock_attested=1),
          validation_clock_attestation_artifact_name TEXT NOT NULL,
          validation_clock_attestation_generated_utc TEXT NOT NULL,
          validation_clock_attestation_age_sec REAL NOT NULL CHECK(validation_clock_attestation_age_sec>=0 AND validation_clock_attestation_age_sec<=90),
          validation_clock_attestation_payload_sha256 TEXT NOT NULL,
          event_lineage_state TEXT NOT NULL CHECK(event_lineage_state IN ('baseline_exact_event_version','postbaseline_frozen_event_lineage')),
          instrument TEXT NOT NULL,
          target_offset_sec INTEGER NOT NULL CHECK(target_offset_sec>=0),
          target_utc TEXT NOT NULL,
          sample_role TEXT NOT NULL CHECK(sample_role IN ('baseline','outcome')),
          source_type TEXT NOT NULL,
          source_record_id TEXT NOT NULL,
          source_payload_sha256 TEXT NOT NULL,
          collector_observed_utc TEXT NOT NULL,
          source_generated_utc TEXT NOT NULL,
          quote_time_utc TEXT NOT NULL,
          observation_known_utc TEXT NOT NULL,
          target_distance_sec REAL NOT NULL,
          bid REAL NOT NULL CHECK(bid>0),
          ask REAL NOT NULL CHECK(ask>bid),
          mid REAL NOT NULL,
          pip REAL NOT NULL CHECK(pip>0),
          spread_pips REAL NOT NULL CHECK(spread_pips>=0),
          market_clock_attestation_state TEXT NOT NULL,
          market_clock_attested INTEGER NOT NULL CHECK(market_clock_attested=1),
          market_clock_attestation_artifact_name TEXT NOT NULL,
          market_clock_attestation_generated_utc TEXT NOT NULL,
          market_clock_attestation_age_sec REAL NOT NULL CHECK(market_clock_attestation_age_sec>=0 AND market_clock_attestation_age_sec<=90),
          market_clock_attestation_payload_sha256 TEXT NOT NULL,
          immutable_prospective_lineage INTEGER NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          FOREIGN KEY(plan_id) REFERENCES event_response_plans_v3(plan_id),
          FOREIGN KEY(cohort_id) REFERENCES event_response_cohort_manifests_v3(cohort_id)
        );
        CREATE INDEX IF NOT EXISTS ix_event_response_samples_v3_scope
          ON event_response_samples_v3(event_instance_id,instrument,target_offset_sec);

        CREATE TABLE IF NOT EXISTS event_response_outcomes_v3(
          outcome_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          event_instance_id TEXT NOT NULL,
          instrument TEXT NOT NULL,
          horizon_sec INTEGER NOT NULL CHECK(horizon_sec>0),
          baseline_sample_id TEXT NOT NULL,
          outcome_sample_id TEXT NOT NULL,
          entry_quote_time_utc TEXT NOT NULL,
          outcome_quote_time_utc TEXT NOT NULL,
          baseline_observation_known_utc TEXT NOT NULL,
          endpoint_observation_known_utc TEXT NOT NULL,
          outcome_known_utc TEXT NOT NULL,
          maturation_recorded_utc TEXT NOT NULL,
          baseline_target_distance_sec REAL NOT NULL,
          outcome_target_distance_sec REAL NOT NULL,
          actual_observation_duration_sec REAL NOT NULL,
          clocks_order_consistent INTEGER NOT NULL,
          outcome_known_after_both_samples INTEGER NOT NULL,
          maturation_recorded_after_outcome_known INTEGER NOT NULL,
          exact_horizon_observation INTEGER NOT NULL,
          proof_evaluation_eligible INTEGER NOT NULL CHECK(proof_evaluation_eligible IN (0,1)),
          exactness_reasons_json TEXT NOT NULL,
          entry_bid REAL NOT NULL,
          entry_ask REAL NOT NULL,
          outcome_bid REAL NOT NULL,
          outcome_ask REAL NOT NULL,
          pip REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,
          outcome_spread_pips REAL NOT NULL,
          midpoint_move_pips REAL NOT NULL,
          long_after_spread_pips REAL NOT NULL,
          short_after_spread_pips REAL NOT NULL,
          direction_selected INTEGER NOT NULL CHECK(direction_selected=0),
          best_side_metric REAL CHECK(best_side_metric IS NULL),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          UNIQUE(cohort_id,event_instance_id,instrument,horizon_sec),
          CHECK(exact_horizon_observation=proof_evaluation_eligible),
          FOREIGN KEY(baseline_sample_id) REFERENCES event_response_samples_v3(sample_id),
          FOREIGN KEY(outcome_sample_id) REFERENCES event_response_samples_v3(sample_id),
          FOREIGN KEY(cohort_id) REFERENCES event_response_cohort_manifests_v3(cohort_id)
        );

        CREATE TABLE IF NOT EXISTS event_response_terminal_exclusions_v3(
          exclusion_id TEXT PRIMARY KEY,
          plan_id TEXT NOT NULL UNIQUE,
          reason TEXT NOT NULL CHECK(reason IN ('target_window_expired_without_valid_observation','event_clock_revised_or_removed_before_baseline')),
          observed_utc TEXT NOT NULL,
          details_json TEXT NOT NULL,
          terminal INTEGER NOT NULL CHECK(terminal=1),
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          FOREIGN KEY(plan_id) REFERENCES event_response_plans_v3(plan_id)
        );

        CREATE TABLE IF NOT EXISTS event_response_attempt_diagnostics_v3(
          attempt_id TEXT PRIMARY KEY,
          reason TEXT NOT NULL,
          source_ref TEXT NOT NULL,
          observed_utc TEXT NOT NULL,
          phase TEXT NOT NULL,
          terminal INTEGER NOT NULL CHECK(terminal=0),
          details_json TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS event_response_worker_cycles_v3(
          cycle_id TEXT PRIMARY KEY,
          cohort_id TEXT NOT NULL,
          started_utc TEXT NOT NULL,
          completed_utc TEXT NOT NULL,
          mode TEXT NOT NULL,
          cadence_sec REAL NOT NULL,
          plans_inserted INTEGER NOT NULL,
          samples_inserted INTEGER NOT NULL,
          outcomes_inserted INTEGER NOT NULL,
          terminal_exclusions_inserted INTEGER NOT NULL,
          attempts_inserted INTEGER NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          supported_execution_decision TEXT NOT NULL CHECK(supported_execution_decision='no_trade'),
          payload_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          FOREIGN KEY(cohort_id) REFERENCES event_response_cohort_manifests_v3(cohort_id)
        );
        """
    )
    for table in _TABLE_PRIMARY_KEYS:
        connection.executescript(
            f"""
            CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table}
            BEGIN SELECT RAISE(ABORT, 'append_only_event_response_v3_ledger'); END;
            CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table}
            BEGIN SELECT RAISE(ABORT, 'append_only_event_response_v3_ledger'); END;
            """
        )
    connection.commit()
    return connection


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")]


def _row_as_payload_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): row[key] for key in row.keys()}


def _verify_parent_contract(
    connection: sqlite3.Connection, table: str, row: Mapping[str, Any]
) -> None:
    if table == "event_response_plans_v3":
        parent = connection.execute(
            "SELECT contract_id FROM event_response_cohort_manifests_v3 WHERE cohort_id=?",
            (row["cohort_id"],),
        ).fetchone()
        if parent is None or str(parent[0]) != str(row["contract_id"]):
            raise ProspectiveEventResponseV3Error("plan_to_cohort_contract_mismatch")
    elif table == "event_response_samples_v3":
        parent = connection.execute(
            "SELECT cohort_id,contract_id,event_instance_id,event_version_id,instrument,target_offset_sec,target_utc,sample_role FROM event_response_plans_v3 WHERE plan_id=?",
            (row["plan_id"],),
        ).fetchone()
        if parent is None:
            raise ProspectiveEventResponseV3Error("sample_plan_missing")
        expected = (
            row["cohort_id"], row["contract_id"], row["event_instance_id"],
            row["event_version_id"], row["instrument"], row["target_offset_sec"],
            row["target_utc"], row["sample_role"],
        )
        if tuple(parent) != expected:
            raise ProspectiveEventResponseV3Error("sample_to_plan_lineage_mismatch")
        excluded = connection.execute(
            "SELECT 1 FROM event_response_terminal_exclusions_v3 WHERE plan_id=?",
            (row["plan_id"],),
        ).fetchone()
        if excluded is not None:
            raise ProspectiveEventResponseV3Error("sample_conflicts_with_terminal_exclusion")
    elif table == "event_response_outcomes_v3":
        base = connection.execute(
            "SELECT cohort_id,event_instance_id,instrument,target_offset_sec FROM event_response_samples_v3 WHERE sample_id=?",
            (row["baseline_sample_id"],),
        ).fetchone()
        endpoint = connection.execute(
            "SELECT cohort_id,event_instance_id,instrument,target_offset_sec FROM event_response_samples_v3 WHERE sample_id=?",
            (row["outcome_sample_id"],),
        ).fetchone()
        if base is None or endpoint is None:
            raise ProspectiveEventResponseV3Error("outcome_sample_parent_missing")
        expected_scope = (row["cohort_id"], row["event_instance_id"], row["instrument"])
        if tuple(base[:3]) != expected_scope or tuple(endpoint[:3]) != expected_scope:
            raise ProspectiveEventResponseV3Error("outcome_to_sample_scope_mismatch")
        if int(base[3]) != 0 or int(endpoint[3]) != int(row["horizon_sec"]):
            raise ProspectiveEventResponseV3Error("outcome_to_sample_horizon_mismatch")
    elif table == "event_response_terminal_exclusions_v3":
        sampled = connection.execute(
            "SELECT 1 FROM event_response_samples_v3 WHERE plan_id=?", (row["plan_id"],)
        ).fetchone()
        if sampled is not None:
            raise ProspectiveEventResponseV3Error("terminal_exclusion_conflicts_with_sample")


def insert_records(
    connection: sqlite3.Connection,
    table: str,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    """Insert rows plainly; verify idempotent duplicates and reject collisions."""

    if table not in _TABLE_PRIMARY_KEYS:
        raise ProspectiveEventResponseV3Error(f"unsupported_table:{table}")
    columns = _table_columns(connection, table)
    primary_key = _TABLE_PRIMARY_KEYS[table]
    inserted = 0
    for source in rows:
        row = dict(source)
        if set(row) != set(columns):
            missing = sorted(set(columns) - set(row))
            extra = sorted(set(row) - set(columns))
            raise ProspectiveEventResponseV3Error(
                f"record_schema_mismatch:{table}:missing={missing}:extra={extra}"
            )
        _verify_record_payload(row)
        existing = connection.execute(
            f"SELECT * FROM {table} WHERE {primary_key}=?", (row[primary_key],)
        ).fetchone()
        if existing is not None:
            existing_mapping = _row_as_payload_mapping(existing)
            _verify_record_payload(existing_mapping)
            if existing_mapping != row:
                raise ProspectiveEventResponseV3Error(
                    f"{table}_primary_key_content_collision"
                )
            continue
        _verify_parent_contract(connection, table, row)
        placeholders = ",".join("?" for _ in columns)
        try:
            connection.execute(
                f"INSERT INTO {table}({','.join(columns)}) VALUES({placeholders})",
                [row[column] for column in columns],
            )
        except sqlite3.IntegrityError as exc:
            raise ProspectiveEventResponseV3Error(
                f"{table}_unique_or_constraint_collision:{exc}"
            ) from exc
        inserted += 1
    return inserted


def register_frozen_cohort(
    connection: sqlite3.Connection,
    *,
    contract: Mapping[str, Any],
    static_fingerprint: Mapping[str, Any],
    cohort_start_source_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    cohort_id = str(contract.get("cohort_id") or "")
    if str(cohort_start_source_lineage.get("source_pipeline_version") or "") != str(
        contract.get("required_source_pipeline_version") or ""
    ):
        raise ProspectiveEventResponseV3Error(
            "cohort_source_pipeline_version_mismatch"
        )
    fingerprint_json = _canonical_json(dict(static_fingerprint))
    static_sha = hashlib.sha256(fingerprint_json.encode("utf-8")).hexdigest()
    existing = connection.execute(
        "SELECT * FROM event_response_cohort_manifests_v3 WHERE cohort_id=?",
        (cohort_id,),
    ).fetchone()
    if existing is not None:
        row = _row_as_payload_mapping(existing)
        _verify_record_payload(row)
        if str(row["static_fingerprint_sha256"]) != static_sha:
            raise ProspectiveEventResponseV3Error(
                "cohort_fingerprint_mismatch_new_cohort_id_required"
            )
        return {"status": "verified_existing", **row}
    genealogy = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": "prospective_official_event_response_v2_20260817",
        "research_generation": RESEARCH_GENERATION,
        "idea_origin": "whole_universe_official_event_to_executable_response_measurement",
        "pre_registered": True,
        "cohort_start_utc": str(contract.get("cohort_start_utc") or ""),
        "holdouts_touched": False,
        "execution_eligible": False,
        "v2_disposition": "zero_sample_engineering_superseded",
        "material_change_requires_new_cohort": True,
    }
    identity = {"cohort_id": cohort_id, "static_fingerprint_sha256": static_sha}
    fields = {
        "contract_id": str(contract.get("contract_id") or ""),
        "cohort_start_utc": str(contract.get("cohort_start_utc") or ""),
        "research_generation": RESEARCH_GENERATION,
        "parent_cohort_id": "prospective_official_event_response_v2_20260817",
        "parent_cohort_disposition": "zero_sample_engineering_superseded",
        "static_fingerprint_sha256": static_sha,
        "fingerprint_json": fingerprint_json,
        "cohort_start_source_lineage_json": _canonical_json(
            dict(cohort_start_source_lineage)
        ),
        "source_pipeline_version": str(
            cohort_start_source_lineage.get("source_pipeline_version") or ""
        ),
        "research_genealogy_json": _canonical_json(genealogy),
        "research_only": 1,
        "execution_eligible": 0,
        "can_place_orders": 0,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    row = _finalize_record(
        id_field="cohort_id",
        id_prefix="unused",
        identity_material=identity,
        evidence_fields=fields,
    )
    # Cohort IDs are human-readable, predeclared contract identities rather
    # than hash-derived row IDs.
    row["cohort_id"] = cohort_id
    row["payload_json"] = _canonical_json(
        {key: value for key, value in row.items() if key not in {"payload_json", "payload_sha256"}}
    )
    row["payload_sha256"] = hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest()
    inserted = insert_records(connection, "event_response_cohort_manifests_v3", [row])
    if inserted != 1:
        raise ProspectiveEventResponseV3Error("frozen_cohort_registration_failed")
    return {"status": "registered", **row}


def read_evidence(
    connection: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    output: list[list[dict[str, Any]]] = []
    for table, order in (
        ("event_response_plans_v3", "target_utc,plan_id"),
        ("event_response_samples_v3", "observation_known_utc,sample_id"),
        ("event_response_terminal_exclusions_v3", "observed_utc,exclusion_id"),
    ):
        rows = [
            _row_as_payload_mapping(row)
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}")
        ]
        for row in rows:
            _verify_record_payload(row)
        output.append(rows)
    return output[0], output[1], output[2]


def read_active_capture_scope(
    connection: sqlite3.Connection,
    *,
    collector_observed_utc: dt.datetime,
    maximum_target_lateness_sec: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read only active plans plus their offset-zero baseline lineage.

    This bounded query is the 1 Hz capture path.  Full-ledger verification and
    expiry sweeps remain on the 15-second idle path, while every row used by
    the active path is still full-payload verified.
    """

    collector = _iso(collector_observed_utc.astimezone(UTC))
    late_days = max(0.0, float(maximum_target_lateness_sec)) / 86400.0
    active = [
        _row_as_payload_mapping(row)
        for row in connection.execute(
            """
            SELECT p.*
            FROM event_response_plans_v3 p
            LEFT JOIN event_response_samples_v3 s ON s.plan_id=p.plan_id
            LEFT JOIN event_response_terminal_exclusions_v3 x ON x.plan_id=p.plan_id
            WHERE s.plan_id IS NULL AND x.plan_id IS NULL
              AND julianday(p.target_utc)<=julianday(?)
              AND julianday(p.target_utc)+?>=julianday(?)
            ORDER BY p.target_utc,p.plan_id
            """,
            (collector, late_days, collector),
        )
    ]
    for row in active:
        _verify_record_payload(row)
    keys = sorted(
        {
            (str(row["event_instance_id"]), str(row["instrument"]))
            for row in active
        }
    )
    if not keys:
        return [], []
    clauses = " OR ".join("(event_instance_id=? AND instrument=?)" for _ in keys)
    parameters = [value for key in keys for value in key]
    baselines = [
        _row_as_payload_mapping(row)
        for row in connection.execute(
            f"""
            SELECT * FROM event_response_plans_v3
            WHERE target_offset_sec=0 AND ({clauses})
            ORDER BY plan_id
            """,
            parameters,
        )
    ]
    baseline_ids = [str(row["plan_id"]) for row in baselines]
    samples: list[dict[str, Any]] = []
    if baseline_ids:
        placeholders = ",".join("?" for _ in baseline_ids)
        samples = [
            _row_as_payload_mapping(row)
            for row in connection.execute(
                f"SELECT * FROM event_response_samples_v3 WHERE plan_id IN ({placeholders}) ORDER BY sample_id",
                baseline_ids,
            )
        ]
    for row in [*baselines, *samples]:
        _verify_record_payload(row)
    by_id = {str(row["plan_id"]): row for row in [*active, *baselines]}
    return list(by_id.values()), samples


def append_cycle(
    connection: sqlite3.Connection,
    *,
    plans: Sequence[Mapping[str, Any]] = (),
    samples: Sequence[Mapping[str, Any]] = (),
    outcomes: Sequence[Mapping[str, Any]] = (),
    terminal_exclusions: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
) -> dict[str, int]:
    return {
        "plans_inserted": insert_records(connection, "event_response_plans_v3", plans),
        "samples_inserted": insert_records(connection, "event_response_samples_v3", samples),
        "outcomes_inserted": insert_records(connection, "event_response_outcomes_v3", outcomes),
        "terminal_exclusions_inserted": insert_records(
            connection, "event_response_terminal_exclusions_v3", terminal_exclusions
        ),
        "attempts_inserted": insert_records(
            connection, "event_response_attempt_diagnostics_v3", attempts
        ),
    }


def worker_cycle_record(
    *,
    cohort_id: str,
    started_utc: dt.datetime,
    completed_utc: dt.datetime,
    mode: str,
    cadence_sec: float,
    counts: Mapping[str, Any],
) -> dict[str, Any]:
    identity = {
        "cohort_id": cohort_id,
        "started_utc": _iso(started_utc),
        "completed_utc": _iso(completed_utc),
    }
    fields = {
        "cohort_id": cohort_id,
        "started_utc": _iso(started_utc),
        "completed_utc": _iso(completed_utc),
        "mode": mode,
        "cadence_sec": float(cadence_sec),
        "plans_inserted": int(counts.get("plans_inserted", 0)),
        "samples_inserted": int(counts.get("samples_inserted", 0)),
        "outcomes_inserted": int(counts.get("outcomes_inserted", 0)),
        "terminal_exclusions_inserted": int(
            counts.get("terminal_exclusions_inserted", 0)
        ),
        "attempts_inserted": int(counts.get("attempts_inserted", 0)),
        "research_only": 1,
        "execution_eligible": 0,
        "can_place_orders": 0,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
    }
    return _finalize_record(
        id_field="cycle_id",
        id_prefix="event_response_worker_cycle_v3",
        identity_material=identity,
        evidence_fields=fields,
    )


def verify_ledger(connection: sqlite3.Connection) -> dict[str, Any]:
    violations: list[str] = []
    for table in _TABLE_PRIMARY_KEYS:
        for db_row in connection.execute(f"SELECT * FROM {table}"):
            row = _row_as_payload_mapping(db_row)
            try:
                _verify_record_payload(row)
                _verify_parent_contract(connection, table, row)
            except ProspectiveEventResponseV3Error as exc:
                violations.append(f"{table}:{row.get(_TABLE_PRIMARY_KEYS[table])}:{exc}")
    fk_violations = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
    if fk_violations:
        violations.append(f"foreign_key_check:{fk_violations}")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        violations.append(f"integrity_check:{integrity}")
    return {
        "status": "ok" if not violations else "failed",
        "violations": violations,
        "sqlite_integrity": integrity,
        "foreign_key_violation_count": len(fk_violations),
    }


def ledger_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    counts = {
        table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in _TABLE_PRIMARY_KEYS
    }
    exact = int(
        connection.execute(
            "SELECT COUNT(*) FROM event_response_outcomes_v3 WHERE proof_evaluation_eligible=1"
        ).fetchone()[0]
    )
    inexact = int(
        connection.execute(
            "SELECT COUNT(*) FROM event_response_outcomes_v3 WHERE proof_evaluation_eligible=0"
        ).fetchone()[0]
    )
    relationship_counts = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT relationship,COUNT(*) FROM event_response_plans_v3 GROUP BY relationship"
        )
    }
    verification = verify_ledger(connection)
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
        "table_counts": counts,
        "relationship_counts": relationship_counts,
        "proof_evaluation_eligible_outcomes": exact,
        "inexact_diagnostic_outcomes": inexact,
        "verification": verification,
    }
