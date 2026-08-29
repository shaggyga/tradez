"""CurrencyState official-context V2 bound to OfficialFactAdapter/Clock V2.

The V1 context implementation remains frozen because it is pinned by existing
after-cost cohorts.  V2 composes that unchanged eligibility logic, then freezes
new context identities and proves that official information introduced no
direction, expected value, allocator rank, or execution surface.
"""

from __future__ import annotations

from typing import Any, Mapping

from ..contracts.currency_state import stable_hash
from ..ingestion.immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
)
from ..ingestion.official_fact_adapter_v2 import (
    ADAPTER_CONTRACT_ID as OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
    ADAPTER_SCHEMA_VERSION as OFFICIAL_FACT_ADAPTER_SCHEMA_VERSION,
    CANONICAL_CURRENCIES,
    OFFICIAL_FACT_EVIDENCE_CLASSES,
    OFFICIAL_FACT_TYPES,
    OfficialFactAdapterV2Error,
    validate_official_event_clock_provenance,
    validate_official_fact_v2_snapshot,
    validate_official_global_gaps,
    validate_official_source_health,
)
from .currency_state_official_context import (
    CONTEXT_CONTRACT_ID as PARENT_CONTEXT_CONTRACT_ID,
    FACT_BASIS_ELIGIBILITY_CONTRACT_ID as PARENT_FACT_BASIS_CONTRACT_ID,
    attach_official_fact_context as attach_official_fact_context_v1,
)


CONTEXT_CONTRACT_ID = "currency_state_official_context_v2_20260817"
FACT_BASIS_ELIGIBILITY_CONTRACT_ID = "official_fact_basis_eligibility_v2_20260817"
SNAPSHOT_SCHEMA = "currency_state_official_context_snapshot_v2"

CONTEXT_V2_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_schema",
        "snapshot_id",
        "contract_id",
        "contract_sha256",
        "decision_cutoff_utc",
        "completed_bar_cutoff_utc",
        "event_time_watermark_utc",
        "input_refs",
        "input_identity",
        "currency_count",
        "instrument_count",
        "horizons",
        "research_only",
        "execution_eligible",
        "can_place_orders",
        "supported_execution_decision",
        "status",
        "limitations",
        "base_currency_state_snapshot_id",
        "official_fact_snapshot_id",
        "official_fact_adapter_contract_id",
        "component_context_contract_id",
        "parent_component_context_contract_id",
        "immutable_event_clock_schema_version",
        "immutable_event_clock_contract_id",
        "component_context",
        "can_promote",
        "can_authorize",
    }
)
OFFICIAL_COMPONENT_CONTEXT_FIELDS = frozenset(
    {
        "currency_summary",
        "fact_count",
        "upcoming_event_count",
        "causal_consensus_count",
        "intraday_rates_connected",
        "fact_basis_eligibility_contract_id",
        "fact_metadata_by_id",
        "basis_eligible_fact_counts",
        "global_gaps",
        "direction_policy",
        "parent_fact_basis_eligibility_contract_id",
        "immutable_event_clock_contract_id",
        "event_clock_provenance",
    }
)
OFFICIAL_CURRENCY_SUMMARY_FIELDS = frozenset(
    {
        "currency",
        "fact_count",
        "fact_type_counts",
        "evidence_class_counts",
        "fact_ids",
        "upcoming_event_count",
        "upcoming_event_ids",
        "causal_consensus_count",
        "source_health",
        "missing_or_degraded",
        "direction_policy",
        "forecast_mean_bps",
        "forecast_absolute_move_bps",
        "cost_clear_probability",
    }
)
OFFICIAL_FACT_METADATA_FIELDS = frozenset(
    {
        "fact_id",
        "currency",
        "fact_type",
        "evidence_class",
        "effective_from_utc",
        "consensus_causal",
        "source_id",
        "source_contract_id",
        "raw_payload_sha256",
        "degradation_reasons",
        "basis_eligibility",
        "proof_eligible_for_any_direction_basis",
    }
)
OFFICIAL_BASIS_FIELDS = frozenset(
    {
        "causal_numeric_surprise",
        "frozen_policy_statement_delta",
        "causal_rate_repricing",
        "versioned_semantic_thesis",
    }
)
_OFFICIAL_COMPONENT_BASE_FIELDS = frozenset(
    {
        "component_id",
        "role",
        "state",
        "observed_signed_move_bps",
        "forecast_mean_bps",
        "forecast_absolute_move_bps",
        "cost_clear_probability",
        "uncertainty_bps",
        "source_age_sec",
        "pair_observation_count",
        "reason",
    }
)
OFFICIAL_COMPONENT_FIELDS_BY_ID = {
    "structured_official_fact": frozenset(
        {
            *_OFFICIAL_COMPONENT_BASE_FIELDS,
            "fact_count",
            "fact_ids",
            "causal_consensus_count",
        }
    ),
    "policy_statement_delta": frozenset(
        {*_OFFICIAL_COMPONENT_BASE_FIELDS, "fact_count", "fact_ids"}
    ),
    "intraday_rate_repricing": _OFFICIAL_COMPONENT_BASE_FIELDS,
}


def _validate_v2_official_snapshot(official_snapshot: Mapping[str, Any]) -> None:
    if official_snapshot.get("schema_version") != OFFICIAL_FACT_ADAPTER_SCHEMA_VERSION:
        raise ValueError("official-fact adapter V2 schema is required")
    if official_snapshot.get("adapter_contract_id") != OFFICIAL_FACT_ADAPTER_CONTRACT_ID:
        raise ValueError("official-fact adapter V2 contract is required")
    try:
        validate_official_fact_v2_snapshot(official_snapshot)
    except OfficialFactAdapterV2Error as exc:
        raise ValueError(f"official-fact V2 snapshot integrity failed: {exc}") from exc
    if (
        official_snapshot.get("immutable_event_clock_schema_version")
        != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
    ):
        raise ValueError("immutable event-clock V2 schema is required")
    if (
        official_snapshot.get("immutable_event_clock_contract_id")
        != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    ):
        raise ValueError("immutable event-clock V2 contract is required")
    provenance = dict(official_snapshot.get("event_clock_provenance") or {})
    if provenance.get("schema_version") != IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION:
        raise ValueError("official-fact provenance is not Clock V2")
    if provenance.get("contract_id") != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
        raise ValueError("official-fact provenance contract is not Clock V2")
    if provenance.get("fallback_used") is not False:
        raise ValueError("mutable event-clock fallback is forbidden")
    if official_snapshot.get("research_only") is not True:
        raise ValueError("official-fact V2 snapshot must be research-only")
    if official_snapshot.get("execution_eligible") is not False:
        raise ValueError("official-fact V2 snapshot cannot be execution eligible")
    if official_snapshot.get("can_place_orders") is not False:
        raise ValueError("official-fact V2 snapshot cannot place orders")
    if official_snapshot.get("supported_execution_decision") != "no_trade":
        raise ValueError("official-fact V2 snapshot must remain no_trade")


def _closed_mapping(
    value: Any,
    allowed: frozenset[str],
    *,
    path: str,
    required: frozenset[str] = frozenset(),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"closed schema requires a mapping at {path}")
    if any(not isinstance(key, str) for key in value):
        raise ValueError(f"closed schema has a non-string key at {path}")
    keys = set(value)
    unknown = sorted(keys - allowed)
    if unknown:
        raise ValueError(f"closed schema has unknown fields at {path}: {unknown}")
    missing = sorted(required - keys)
    if missing:
        raise ValueError(f"closed schema is missing fields at {path}: {missing}")
    return value


def _string_list(value: Any, *, path: str) -> None:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"closed schema requires a string list at {path}")


def _validate_count_mapping(
    value: Any, allowed: frozenset[str], *, path: str
) -> None:
    row = _closed_mapping(value, allowed, path=path)
    if any(type(count) is not int or count < 0 for count in row.values()):
        raise ValueError(f"closed schema requires nonnegative integer counts at {path}")


def _validate_official_context_closed_schema(snapshot: Mapping[str, Any]) -> None:
    _closed_mapping(
        snapshot,
        CONTEXT_V2_TOP_LEVEL_FIELDS,
        path="root",
        required=frozenset(
            {
                "snapshot_schema",
                "snapshot_id",
                "decision_cutoff_utc",
                "horizons",
                "research_only",
                "execution_eligible",
                "can_place_orders",
                "supported_execution_decision",
                "base_currency_state_snapshot_id",
                "official_fact_snapshot_id",
                "official_fact_adapter_contract_id",
                "component_context_contract_id",
                "parent_component_context_contract_id",
                "immutable_event_clock_schema_version",
                "immutable_event_clock_contract_id",
                "component_context",
                "can_promote",
                "can_authorize",
            }
        ),
    )
    context = _closed_mapping(
        snapshot.get("component_context"),
        OFFICIAL_COMPONENT_CONTEXT_FIELDS,
        path="root.component_context",
        required=OFFICIAL_COMPONENT_CONTEXT_FIELDS,
    )
    if context.get("direction_policy") != "abstain":
        raise ValueError("official component context assigned a direction")

    summaries = context.get("currency_summary")
    if not isinstance(summaries, Mapping):
        raise ValueError("official currency summaries must be a mapping")
    if any(not isinstance(key, str) or key not in CANONICAL_CURRENCIES for key in summaries):
        raise ValueError("official currency summaries contain an unknown currency")
    for currency, raw_summary in summaries.items():
        path = f"root.component_context.currency_summary.{currency}"
        summary = _closed_mapping(
            raw_summary,
            OFFICIAL_CURRENCY_SUMMARY_FIELDS,
            path=path,
            required=OFFICIAL_CURRENCY_SUMMARY_FIELDS,
        )
        if summary.get("currency") != currency:
            raise ValueError(f"official currency summary identity mismatch at {path}")
        if summary.get("direction_policy") != "abstain":
            raise ValueError(f"official currency summary assigned a direction at {path}")
        for field in (
            "forecast_mean_bps",
            "forecast_absolute_move_bps",
            "cost_clear_probability",
        ):
            if summary.get(field) is not None:
                raise ValueError(f"official currency summary populated {field} at {path}")
        _validate_count_mapping(
            summary.get("fact_type_counts"),
            OFFICIAL_FACT_TYPES,
            path=f"{path}.fact_type_counts",
        )
        _validate_count_mapping(
            summary.get("evidence_class_counts"),
            OFFICIAL_FACT_EVIDENCE_CLASSES,
            path=f"{path}.evidence_class_counts",
        )
        for field in ("fact_ids", "upcoming_event_ids", "missing_or_degraded"):
            _string_list(summary.get(field), path=f"{path}.{field}")
        validate_official_source_health(
            summary.get("source_health"), path=f"{path}.source_health"
        )

    metadata_by_id = context.get("fact_metadata_by_id")
    if not isinstance(metadata_by_id, Mapping):
        raise ValueError("official fact metadata must be a mapping")
    for fact_id, raw_metadata in metadata_by_id.items():
        if not isinstance(fact_id, str) or not fact_id:
            raise ValueError("official fact metadata has an invalid identity")
        path = f"root.component_context.fact_metadata_by_id.{fact_id}"
        metadata = _closed_mapping(
            raw_metadata,
            OFFICIAL_FACT_METADATA_FIELDS,
            path=path,
            required=OFFICIAL_FACT_METADATA_FIELDS,
        )
        if metadata.get("fact_id") != fact_id:
            raise ValueError(f"official fact metadata identity mismatch at {path}")
        if metadata.get("fact_type") not in OFFICIAL_FACT_TYPES:
            raise ValueError(f"official fact metadata type invalid at {path}")
        if metadata.get("evidence_class") not in OFFICIAL_FACT_EVIDENCE_CLASSES:
            raise ValueError(f"official fact evidence class invalid at {path}")
        _string_list(metadata.get("degradation_reasons"), path=f"{path}.degradation_reasons")
        basis = _closed_mapping(
            metadata.get("basis_eligibility"),
            OFFICIAL_BASIS_FIELDS,
            path=f"{path}.basis_eligibility",
            required=OFFICIAL_BASIS_FIELDS,
        )
        if any(type(flag) is not bool for flag in basis.values()):
            raise ValueError(f"official fact basis flags must be booleans at {path}")

    _validate_count_mapping(
        context.get("basis_eligible_fact_counts"),
        OFFICIAL_BASIS_FIELDS,
        path="root.component_context.basis_eligible_fact_counts",
    )
    try:
        validate_official_global_gaps(
            context.get("global_gaps"), path="root.component_context.global_gaps"
        )
        validate_official_event_clock_provenance(
            context.get("event_clock_provenance"),
            path="root.component_context.event_clock_provenance",
        )
    except OfficialFactAdapterV2Error as exc:
        raise ValueError(str(exc)) from exc

    horizons = snapshot.get("horizons")
    if not isinstance(horizons, Mapping):
        raise ValueError("currency-state horizons must be a mapping")
    for horizon_id, horizon in horizons.items():
        if not isinstance(horizon, Mapping):
            raise ValueError(f"currency-state horizon {horizon_id} must be a mapping")
        currencies = horizon.get("currencies")
        if not isinstance(currencies, Mapping):
            raise ValueError(f"currency-state horizon {horizon_id} currencies missing")
        for currency_id, currency in currencies.items():
            if not isinstance(currency, Mapping):
                raise ValueError(f"currency-state currency {currency_id} must be a mapping")
            if currency_id not in summaries:
                raise ValueError(f"official summary missing for currency {currency_id}")
            summary = _closed_mapping(
                currency.get("official_fact_context"),
                OFFICIAL_CURRENCY_SUMMARY_FIELDS,
                path=f"root.horizons.{horizon_id}.currencies.{currency_id}.official_fact_context",
                required=OFFICIAL_CURRENCY_SUMMARY_FIELDS,
            )
            if dict(summary) != dict(summaries[currency_id]):
                raise ValueError(f"official currency summary copy mismatch for {currency_id}")
            components = currency.get("components")
            if not isinstance(components, list):
                raise ValueError(f"currency components missing for {currency_id}")
            found: set[str] = set()
            for component in components:
                if not isinstance(component, Mapping):
                    raise ValueError(f"currency component invalid for {currency_id}")
                component_id = str(component.get("component_id") or "")
                if component_id not in OFFICIAL_COMPONENT_FIELDS_BY_ID:
                    continue
                found.add(component_id)
                path = (
                    f"root.horizons.{horizon_id}.currencies.{currency_id}."
                    f"components.{component_id}"
                )
                _closed_mapping(
                    component,
                    OFFICIAL_COMPONENT_FIELDS_BY_ID[component_id],
                    path=path,
                    required=OFFICIAL_COMPONENT_FIELDS_BY_ID[component_id],
                )
                for field in (
                    "forecast_mean_bps",
                    "forecast_absolute_move_bps",
                    "cost_clear_probability",
                ):
                    if component.get(field) is not None:
                        raise ValueError(f"official component populated {field} at {path}")
                if "fact_ids" in component:
                    _string_list(component["fact_ids"], path=f"{path}.fact_ids")
            if found != set(OFFICIAL_COMPONENT_FIELDS_BY_ID):
                raise ValueError(f"official components incomplete for {currency_id}")
        edges = horizon.get("pair_edges")
        if not isinstance(edges, Mapping):
            raise ValueError(f"currency-state horizon {horizon_id} edges missing")
        for edge in edges.values():
            if edge.get("forecast_mean_bps") is not None:
                raise ValueError("official context cannot create a pair forecast")
            if edge.get("expected_net_pips") is not None:
                raise ValueError("official context cannot create pair expected value")
            if edge.get("allocator_rank") is not None:
                raise ValueError("official context cannot create allocator rank")
            if edge.get("execution_eligible") is not False:
                raise ValueError("official context cannot create execution eligibility")

    if snapshot.get("research_only") is not True:
        raise ValueError("official context must be research-only")
    if snapshot.get("execution_eligible") is not False:
        raise ValueError("official context cannot be execution eligible")
    if snapshot.get("can_place_orders") is not False:
        raise ValueError("official context cannot place orders")
    if snapshot.get("can_promote") is not False:
        raise ValueError("official context cannot promote")
    if snapshot.get("can_authorize") is not False:
        raise ValueError("official context cannot authorize")
    if snapshot.get("supported_execution_decision") != "no_trade":
        raise ValueError("official context must remain no_trade")


def attach_official_fact_context_v2(
    currency_snapshot: Mapping[str, Any],
    official_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach strict V2 official context without changing any forecast field."""

    _validate_v2_official_snapshot(official_snapshot)
    output = attach_official_fact_context_v1(
        currency_snapshot,
        official_snapshot,
        contract=contract,
    )
    output["snapshot_schema"] = SNAPSHOT_SCHEMA
    output["component_context_contract_id"] = CONTEXT_CONTRACT_ID
    output["parent_component_context_contract_id"] = PARENT_CONTEXT_CONTRACT_ID
    output["official_fact_adapter_contract_id"] = OFFICIAL_FACT_ADAPTER_CONTRACT_ID
    output["immutable_event_clock_schema_version"] = (
        IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
    )
    output["immutable_event_clock_contract_id"] = IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    output["component_context"]["fact_basis_eligibility_contract_id"] = (
        FACT_BASIS_ELIGIBILITY_CONTRACT_ID
    )
    output["component_context"]["parent_fact_basis_eligibility_contract_id"] = (
        PARENT_FACT_BASIS_CONTRACT_ID
    )
    output["component_context"]["immutable_event_clock_contract_id"] = (
        IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    )
    output["component_context"]["event_clock_provenance"] = dict(
        official_snapshot.get("event_clock_provenance") or {}
    )
    output["can_promote"] = False
    output["can_authorize"] = False
    output["snapshot_id"] = "currency_state_official_v2_" + stable_hash(
        {
            "context_contract_id": CONTEXT_CONTRACT_ID,
            "fact_basis_eligibility_contract_id": FACT_BASIS_ELIGIBILITY_CONTRACT_ID,
            "base_snapshot_id": output["base_currency_state_snapshot_id"],
            "official_fact_snapshot_id": output["official_fact_snapshot_id"],
            "official_fact_adapter_contract_id": OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
            "immutable_event_clock_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
            "component_context": output["component_context"],
        }
    )[:24]
    output["status"] = "clock_v2_context_attached_unscored"
    validate_currency_state_official_context_v2_snapshot(output)
    return output


def validate_currency_state_official_context_v2_snapshot(
    snapshot: Mapping[str, Any],
) -> None:
    """Validate exact V2 context branding and every nested safety surface."""

    if snapshot.get("snapshot_schema") != SNAPSHOT_SCHEMA:
        raise ValueError("currency-state official-context V2 schema mismatch")
    if snapshot.get("component_context_contract_id") != CONTEXT_CONTRACT_ID:
        raise ValueError("currency-state official-context V2 contract mismatch")
    if snapshot.get("official_fact_adapter_contract_id") != OFFICIAL_FACT_ADAPTER_CONTRACT_ID:
        raise ValueError("currency-state official-context adapter contract mismatch")
    if snapshot.get("immutable_event_clock_contract_id") != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
        raise ValueError("currency-state official-context clock contract mismatch")
    context = snapshot.get("component_context")
    if not isinstance(context, Mapping):
        raise ValueError("currency-state official-context component context missing")
    if context.get("fact_basis_eligibility_contract_id") != FACT_BASIS_ELIGIBILITY_CONTRACT_ID:
        raise ValueError("currency-state official-context basis contract mismatch")
    expected_id = "currency_state_official_v2_" + stable_hash(
        {
            "context_contract_id": CONTEXT_CONTRACT_ID,
            "fact_basis_eligibility_contract_id": FACT_BASIS_ELIGIBILITY_CONTRACT_ID,
            "base_snapshot_id": snapshot.get("base_currency_state_snapshot_id"),
            "official_fact_snapshot_id": snapshot.get("official_fact_snapshot_id"),
            "official_fact_adapter_contract_id": OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
            "immutable_event_clock_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
            "component_context": context,
        }
    )[:24]
    if snapshot.get("snapshot_id") != expected_id:
        raise ValueError("currency-state official-context snapshot identity mismatch")
    try:
        _validate_official_context_closed_schema(snapshot)
    except (OfficialFactAdapterV2Error, ValueError) as exc:
        raise ValueError(
            f"currency-state official-context V2 integrity failed: {exc}"
        ) from exc


__all__ = [
    "CONTEXT_CONTRACT_ID",
    "CONTEXT_V2_TOP_LEVEL_FIELDS",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "OFFICIAL_COMPONENT_CONTEXT_FIELDS",
    "OFFICIAL_COMPONENT_FIELDS_BY_ID",
    "OFFICIAL_CURRENCY_SUMMARY_FIELDS",
    "OFFICIAL_FACT_METADATA_FIELDS",
    "SNAPSHOT_SCHEMA",
    "attach_official_fact_context_v2",
    "validate_currency_state_official_context_v2_snapshot",
]
