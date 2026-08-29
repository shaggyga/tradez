"""Strict, unscored CurrencyState + OfficialFact V3 composition.

This disabled successor verifies the inherited CurrencyState snapshot identity
before copying any horizon, accepts only an exact recursively typed output
schema, and hashes the entire accepted composite except ``snapshot_id``.
Official facts remain context: they cannot create direction, magnitude,
allocator rank, promotion, authorization, or execution eligibility.
"""

from __future__ import annotations

import copy
import math
from typing import Any, Mapping

from ..contracts.currency_state import load_contract, stable_hash, validate_contract
from ..ingestion.immutable_event_clock import (
    CONTRACT_ID as IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
)
from ..ingestion.official_fact_adapter_v3 import (
    ADAPTER_CONTRACT_ID as OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
    ADAPTER_SCHEMA_VERSION as OFFICIAL_FACT_ADAPTER_SCHEMA_VERSION,
    FACT_BASIS_ELIGIBILITY_CONTRACT_ID,
    OfficialFactAdapterV3Error,
    _exact_bool,
    _exact_int,
    _exact_str,
    _finite_number,
    _hash64,
    _mapping,
    _string_list,
    _timestamp,
    _validate_global_gaps,
    _validate_provenance,
    _validate_source_health,
    validate_official_fact_v3_snapshot,
)
from ..ingestion.official_fact_adapter_v2 import (
    OFFICIAL_FACT_EVIDENCE_CLASSES,
    OFFICIAL_FACT_TYPES,
)
from .currency_state_engine import validate_snapshot as validate_base_snapshot
from .currency_state_official_context import (
    attach_official_fact_context as attach_official_fact_context_v1,
)
from .currency_state_official_context_v2 import (
    CONTEXT_CONTRACT_ID as PARENT_CONTEXT_CONTRACT_ID,
    OFFICIAL_BASIS_FIELDS,
    OFFICIAL_COMPONENT_CONTEXT_FIELDS,
    OFFICIAL_COMPONENT_FIELDS_BY_ID,
    OFFICIAL_CURRENCY_SUMMARY_FIELDS,
    OFFICIAL_FACT_METADATA_FIELDS,
)


CONTEXT_CONTRACT_ID = "currency_state_official_context_v3_20260817"
PARENT_FACT_BASIS_ELIGIBILITY_CONTRACT_ID = (
    "official_fact_basis_eligibility_v2_20260817"
)
SNAPSHOT_SCHEMA = "currency_state_official_context_snapshot_v3"
STATUS = "clock_v2_official_fact_v3_context_attached_unscored"

CONTEXT_V3_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_schema",
        "snapshot_id",
        "contract_id",
        "contract_sha256",
        "decision_cutoff_utc",
        "completed_bar_cutoff_utc",
        "event_time_watermark_utc",
        "generated_utc",
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
        "base_currency_state_snapshot_sha256",
        "base_currency_state_horizons_sha256",
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

_BASE_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_schema",
        "snapshot_id",
        "contract_id",
        "contract_sha256",
        "decision_cutoff_utc",
        "completed_bar_cutoff_utc",
        "event_time_watermark_utc",
        "generated_utc",
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
    }
)
_INPUT_REF_FIELDS = frozenset(
    {
        "contract_path",
        "contract_sha256",
        "quote_history_generated_utc",
        "quote_history_path",
        "quote_history_sha256",
    }
)
_INPUT_IDENTITY_FIELDS = frozenset({"contract_sha256", "quote_history_sha256"})
_HORIZON_FIELDS = frozenset(
    {
        "horizon_sec",
        "decision_cutoff_utc",
        "completed_bar_cutoff_utc",
        "solver",
        "rejected_observation_counts",
        "currencies",
        "pair_edges",
    }
)
_CURRENCY_FIELDS = frozenset(
    {
        "component_id",
        "currency",
        "horizon_sec",
        "state",
        "observed_currency_return_bps",
        "observed_response_uncertainty_bps",
        "forecast_mean_bps",
        "forecast_absolute_move_bps",
        "forecast_uncertainty_bps",
        "cost_clear_probability",
        "pair_observation_count",
        "expected_pair_count",
        "coverage_ratio",
        "components",
        "abstention_reasons",
        "official_fact_context",
    }
)
_BASE_CURRENCY_FIELDS = _CURRENCY_FIELDS - {"official_fact_context"}
_COMPONENT_BASE_FIELDS = frozenset(
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
_ALL_COMPONENT_IDS = frozenset(
    {
        "structured_official_fact",
        "policy_statement_delta",
        "intraday_rate_repricing",
        "semantic_analog",
        "positioning",
        "media_context",
        "latent_price_response",
    }
)
_COMPONENT_FIELDS_BY_ID = {
    **{key: value for key, value in OFFICIAL_COMPONENT_FIELDS_BY_ID.items()},
    "semantic_analog": _COMPONENT_BASE_FIELDS,
    "positioning": _COMPONENT_BASE_FIELDS,
    "media_context": _COMPONENT_BASE_FIELDS,
    "latent_price_response": _COMPONENT_BASE_FIELDS,
}
_EDGE_FIELDS = frozenset(
    {
        "instrument",
        "base_currency",
        "quote_currency",
        "horizon_sec",
        "state",
        "observed_pair_return_bps",
        "observed_solver_clipped_return_bps",
        "observed_return_was_clipped",
        "observed_factor_move_bps",
        "observed_residual_bps",
        "observed_residual_basis",
        "observed_solver_residual_bps",
        "observed_solver_residual_basis",
        "leave_one_edge_out_factor_bps",
        "self_influence_bps",
        "observed_edge_uncertainty_bps",
        "forecast_mean_bps",
        "forecast_absolute_move_bps",
        "cost_clear_probability",
        "expected_net_pips",
        "allocator_rank",
        "bid",
        "ask",
        "pip",
        "spread_bps",
        "start_epoch",
        "end_epoch",
        "actual_observation_duration_sec",
        "declared_horizon_sec",
        "exact_horizon_observation",
        "endpoint_age_sec",
        "start_alignment_sec",
        "observation_weight",
        "research_observable",
        "execution_eligible",
        "abstention_reasons",
    }
)
_SOLVER_MINIMAL_FIELDS = frozenset(
    {
        "status",
        "active_currencies",
        "components",
        "observation_count",
        "duplicate_observation_count",
    }
)
_SOLVER_FULL_FIELDS = frozenset(
    {
        *_SOLVER_MINIMAL_FIELDS,
        "unavailable_currencies",
        "discarded_component_currencies",
        "weighted_observation_equivalent",
        "matrix_rank",
        "condition_number",
        "return_clip_bps",
        "median_abs_pair_return_bps",
        "residual_rmse_bps",
        "weighted_residual_sigma_bps",
        "strengths_bps",
        "uncertainty_bps",
        "pair_counts",
        "clipped_pair_returns_bps",
        "pair_residuals_clipped_bps",
        "pair_residuals_bps",
        "pair_residual_basis",
        "covariance_currencies",
        "covariance_bps2",
    }
)


def _canonical_sha(value: Any) -> str:
    return stable_hash(value)


def _optional_number(value: Any, *, path: str) -> None:
    _finite_number(value, path=path, nullable=True)


def _optional_bool(value: Any, *, path: str) -> None:
    if value is not None:
        _exact_bool(value, path=path)


def _typed_string_map(value: Any, allowed: frozenset[str], *, path: str) -> None:
    row = _mapping(value, allowed, path=path)
    for key, item in row.items():
        _exact_str(item, path=f"{path}.{key}", nonempty=True)
        if key.endswith("sha256"):
            _hash64(item, path=f"{path}.{key}")
        if key.endswith("generated_utc"):
            _timestamp(item, path=f"{path}.{key}")


def _validate_solver(
    value: Any,
    *,
    path: str,
    currencies: frozenset[str],
    instruments: frozenset[str],
) -> None:
    if type(value) is not dict:
        raise ValueError(f"typed schema requires exact mapping at {path}")
    status = value.get("status")
    _exact_str(status, path=f"{path}.status", nonempty=True)
    fields = _SOLVER_MINIMAL_FIELDS if status == "insufficient_observations" else _SOLVER_FULL_FIELDS
    row = _mapping(value, fields, path=path, required=fields)
    if status not in {"insufficient_observations", "ok", "degraded_disconnected"}:
        raise ValueError(f"unknown solver state at {path}")
    active = _string_list(row["active_currencies"], path=f"{path}.active_currencies")
    if len(set(active)) != len(active) or not set(active).issubset(currencies):
        raise ValueError(f"invalid active currency list at {path}")
    components = row["components"]
    if type(components) is not list:
        raise ValueError(f"typed schema requires component list at {path}.components")
    flattened: list[str] = []
    for index, component in enumerate(components):
        values = _string_list(component, path=f"{path}.components[{index}]")
        if len(set(values)) != len(values) or not set(values).issubset(currencies):
            raise ValueError(f"invalid solver component at {path}.components[{index}]")
        flattened.extend(values)
    if len(set(flattened)) != len(flattened):
        raise ValueError(f"duplicate solver component currency at {path}")
    for field in ("observation_count", "duplicate_observation_count"):
        _exact_int(row[field], path=f"{path}.{field}")
    if fields == _SOLVER_MINIMAL_FIELDS:
        return
    for field in ("unavailable_currencies", "discarded_component_currencies", "covariance_currencies"):
        values = _string_list(row[field], path=f"{path}.{field}")
        if len(set(values)) != len(values) or not set(values).issubset(currencies):
            raise ValueError(f"invalid currency list at {path}.{field}")
    _exact_int(row["matrix_rank"], path=f"{path}.matrix_rank")
    for field in (
        "weighted_observation_equivalent",
        "condition_number",
        "return_clip_bps",
        "median_abs_pair_return_bps",
        "residual_rmse_bps",
        "weighted_residual_sigma_bps",
    ):
        _finite_number(row[field], path=f"{path}.{field}", nullable=(field == "condition_number"))
    map_specs = {
        "strengths_bps": currencies,
        "uncertainty_bps": currencies,
        "pair_counts": currencies,
        "clipped_pair_returns_bps": instruments,
        "pair_residuals_clipped_bps": instruments,
        "pair_residuals_bps": instruments,
    }
    for field, allowed_keys in map_specs.items():
        mapping = row[field]
        if type(mapping) is not dict or any(type(key) is not str or key not in allowed_keys for key in mapping):
            raise ValueError(f"invalid typed solver map at {path}.{field}")
        for key, item in mapping.items():
            if field == "pair_counts":
                _exact_int(item, path=f"{path}.{field}.{key}")
            else:
                _finite_number(item, path=f"{path}.{field}.{key}")
    _exact_str(row["pair_residual_basis"], path=f"{path}.pair_residual_basis", nonempty=True)
    matrix = row["covariance_bps2"]
    order = row["covariance_currencies"]
    if type(matrix) is not list or len(matrix) != len(order):
        raise ValueError(f"invalid covariance shape at {path}.covariance_bps2")
    for i, matrix_row in enumerate(matrix):
        if type(matrix_row) is not list or len(matrix_row) != len(order):
            raise ValueError(f"invalid covariance row at {path}.covariance_bps2[{i}]")
        for j, item in enumerate(matrix_row):
            _finite_number(item, path=f"{path}.covariance_bps2[{i}][{j}]")


def _validate_component(value: Any, *, path: str) -> Mapping[str, Any]:
    if type(value) is not dict:
        raise ValueError(f"typed schema requires exact component mapping at {path}")
    component_id = value.get("component_id")
    if type(component_id) is not str or component_id not in _COMPONENT_FIELDS_BY_ID:
        raise ValueError(f"unknown component at {path}")
    fields = _COMPONENT_FIELDS_BY_ID[component_id]
    row = _mapping(value, fields, path=path, required=fields)
    for field in ("component_id", "role", "state", "reason"):
        _exact_str(row[field], path=f"{path}.{field}", nonempty=True)
    for field in (
        "observed_signed_move_bps",
        "forecast_mean_bps",
        "forecast_absolute_move_bps",
        "cost_clear_probability",
        "uncertainty_bps",
        "source_age_sec",
    ):
        _optional_number(row[field], path=f"{path}.{field}")
    _exact_int(row["pair_observation_count"], path=f"{path}.pair_observation_count")
    if component_id in OFFICIAL_COMPONENT_FIELDS_BY_ID:
        for field in ("forecast_mean_bps", "forecast_absolute_move_bps", "cost_clear_probability"):
            if row[field] is not None:
                raise ValueError(f"official component populated {field} at {path}")
        if "fact_count" in row:
            _exact_int(row["fact_count"], path=f"{path}.fact_count")
        if "causal_consensus_count" in row:
            _exact_int(row["causal_consensus_count"], path=f"{path}.causal_consensus_count")
        if "fact_ids" in row:
            ids = _string_list(row["fact_ids"], path=f"{path}.fact_ids")
            if len(ids) != len(set(ids)):
                raise ValueError(f"duplicate component fact ID at {path}")
    return row


def _validate_summary(value: Any, *, path: str, currency: str) -> Mapping[str, Any]:
    row = _mapping(
        value,
        OFFICIAL_CURRENCY_SUMMARY_FIELDS,
        path=path,
        required=OFFICIAL_CURRENCY_SUMMARY_FIELDS,
    )
    _exact_str(row["currency"], path=f"{path}.currency", nonempty=True)
    if row["currency"] != currency:
        raise ValueError(f"currency summary identity mismatch at {path}")
    for field in ("fact_count", "upcoming_event_count", "causal_consensus_count"):
        _exact_int(row[field], path=f"{path}.{field}")
    for field in ("fact_ids", "upcoming_event_ids", "missing_or_degraded"):
        values = _string_list(row[field], path=f"{path}.{field}")
        if field != "missing_or_degraded" and len(values) != len(set(values)):
            raise ValueError(f"duplicate identity at {path}.{field}")
    for field in ("fact_type_counts", "evidence_class_counts"):
        counts = row[field]
        if type(counts) is not dict or any(type(key) is not str for key in counts):
            raise ValueError(f"typed count mapping required at {path}.{field}")
        for key, count in counts.items():
            _exact_int(count, path=f"{path}.{field}.{key}")
        if sum(counts.values()) != row["fact_count"]:
            raise ValueError(f"count reconciliation failed at {path}.{field}")
    _validate_source_health(row["source_health"], path=f"{path}.source_health")
    _exact_str(row["direction_policy"], path=f"{path}.direction_policy")
    if row["direction_policy"] != "abstain":
        raise ValueError(f"summary direction is not abstain at {path}")
    for field in ("forecast_mean_bps", "forecast_absolute_move_bps", "cost_clear_probability"):
        if row[field] is not None:
            raise ValueError(f"summary populated {field} at {path}")
    return row


def validate_base_currency_state_identity(
    snapshot: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> dict[str, str]:
    """Independently prove the base ID before any horizon is copied."""

    validate_contract(contract)
    row = _mapping(
        snapshot,
        _BASE_TOP_LEVEL_FIELDS,
        path="base",
        required=_BASE_TOP_LEVEL_FIELDS - {"generated_utc"},
    )
    validate_base_snapshot(row, contract=contract)
    if type(row["schema_version"]) is not int or row["schema_version"] != 1:
        raise ValueError("base CurrencyState schema_version mismatch")
    for field in ("snapshot_schema", "contract_id", "contract_sha256", "decision_cutoff_utc", "completed_bar_cutoff_utc", "event_time_watermark_utc", "status"):
        _exact_str(row[field], path=f"base.{field}", nonempty=True)
    if row["snapshot_schema"] != contract["snapshot_schema"] or row["contract_id"] != contract["contract_id"] or row["contract_sha256"] != contract["contract_sha256"]:
        raise ValueError("base CurrencyState contract identity mismatch")
    for field in ("decision_cutoff_utc", "completed_bar_cutoff_utc", "event_time_watermark_utc"):
        _timestamp(row[field], path=f"base.{field}")
    if "generated_utc" in row:
        _timestamp(row["generated_utc"], path="base.generated_utc")
    _typed_string_map(row["input_refs"], _INPUT_REF_FIELDS, path="base.input_refs")
    _typed_string_map(row["input_identity"], _INPUT_IDENTITY_FIELDS, path="base.input_identity")
    for field, expected in (("currency_count", 21), ("instrument_count", 68)):
        _exact_int(row[field], path=f"base.{field}")
        if row[field] != expected:
            raise ValueError(f"base CurrencyState {field} mismatch")
    _string_list(row["limitations"], path="base.limitations")
    for field, expected in (("research_only", True), ("execution_eligible", False), ("can_place_orders", False)):
        _exact_bool(row[field], path=f"base.{field}")
        if row[field] is not expected:
            raise ValueError(f"base CurrencyState {field} mismatch")
    if row["supported_execution_decision"] != "no_trade":
        raise ValueError("base CurrencyState decision mismatch")
    identity_material = {
        "contract_id": row["contract_id"],
        "contract_sha256": row["contract_sha256"],
        "decision_cutoff_utc": row["decision_cutoff_utc"],
        "completed_bar_cutoff_utc": row["completed_bar_cutoff_utc"],
        "input_identity": row["input_identity"],
        "horizons": row["horizons"],
    }
    expected_id = "currency_state_snapshot_" + stable_hash(identity_material)[:24]
    if row["snapshot_id"] != expected_id:
        raise ValueError("base CurrencyState snapshot identity mismatch")
    return {
        "snapshot_id": expected_id,
        "snapshot_sha256": stable_hash(dict(row)),
        "horizons_sha256": stable_hash(row["horizons"]),
    }


def _validate_preserved_horizons(
    base: Mapping[str, Any], output: Mapping[str, Any]
) -> None:
    if set(base["horizons"]) != set(output["horizons"]):
        raise ValueError("copied horizon identities changed")
    for horizon_id, base_horizon in base["horizons"].items():
        result_horizon = output["horizons"][horizon_id]
        for field in _HORIZON_FIELDS - {"currencies"}:
            if result_horizon[field] != base_horizon[field]:
                raise ValueError(f"base horizon field changed: {horizon_id}.{field}")
        for currency, base_currency in base_horizon["currencies"].items():
            result_currency = result_horizon["currencies"][currency]
            for field in _BASE_CURRENCY_FIELDS - {"components"}:
                if result_currency[field] != base_currency[field]:
                    raise ValueError(f"base currency field changed: {horizon_id}.{currency}.{field}")
            base_components = {item["component_id"]: item for item in base_currency["components"]}
            result_components = {item["component_id"]: item for item in result_currency["components"]}
            for component_id in _ALL_COMPONENT_IDS - set(OFFICIAL_COMPONENT_FIELDS_BY_ID):
                if result_components[component_id] != base_components[component_id]:
                    raise ValueError(f"nonofficial component changed: {horizon_id}.{currency}.{component_id}")


def _validate_context_schema(
    snapshot: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> None:
    row = _mapping(
        snapshot,
        CONTEXT_V3_TOP_LEVEL_FIELDS,
        path="root",
        required=CONTEXT_V3_TOP_LEVEL_FIELDS - {"generated_utc"},
    )
    if type(row["schema_version"]) is not int or row["schema_version"] != 1:
        raise ValueError("context inherited schema_version mismatch")
    literals = {
        "snapshot_schema": SNAPSHOT_SCHEMA,
        "contract_id": contract["contract_id"],
        "contract_sha256": contract["contract_sha256"],
        "supported_execution_decision": "no_trade",
        "status": STATUS,
        "official_fact_adapter_contract_id": OFFICIAL_FACT_ADAPTER_CONTRACT_ID,
        "component_context_contract_id": CONTEXT_CONTRACT_ID,
        "parent_component_context_contract_id": PARENT_CONTEXT_CONTRACT_ID,
        "immutable_event_clock_schema_version": IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION,
        "immutable_event_clock_contract_id": IMMUTABLE_EVENT_CLOCK_CONTRACT_ID,
    }
    for field, expected in literals.items():
        _exact_str(row[field], path=f"root.{field}", nonempty=True)
        if row[field] != expected:
            raise ValueError(f"context literal mismatch at {field}")
    for field in ("snapshot_id", "base_currency_state_snapshot_id", "official_fact_snapshot_id"):
        _exact_str(row[field], path=f"root.{field}", nonempty=True)
    for field in ("base_currency_state_snapshot_sha256", "base_currency_state_horizons_sha256"):
        _hash64(row[field], path=f"root.{field}")
    for field in ("decision_cutoff_utc", "completed_bar_cutoff_utc", "event_time_watermark_utc"):
        _timestamp(row[field], path=f"root.{field}")
    if "generated_utc" in row:
        _timestamp(row["generated_utc"], path="root.generated_utc")
    _typed_string_map(row["input_refs"], _INPUT_REF_FIELDS, path="root.input_refs")
    _typed_string_map(row["input_identity"], _INPUT_IDENTITY_FIELDS, path="root.input_identity")
    for field, expected in (("currency_count", 21), ("instrument_count", 68)):
        _exact_int(row[field], path=f"root.{field}")
        if row[field] != expected:
            raise ValueError(f"context {field} mismatch")
    _string_list(row["limitations"], path="root.limitations")
    for field, expected in (
        ("research_only", True),
        ("execution_eligible", False),
        ("can_place_orders", False),
        ("can_promote", False),
        ("can_authorize", False),
    ):
        _exact_bool(row[field], path=f"root.{field}")
        if row[field] is not expected:
            raise ValueError(f"context {field} mismatch")

    context = _mapping(
        row["component_context"],
        OFFICIAL_COMPONENT_CONTEXT_FIELDS,
        path="root.component_context",
        required=OFFICIAL_COMPONENT_CONTEXT_FIELDS,
    )
    if context["fact_basis_eligibility_contract_id"] != FACT_BASIS_ELIGIBILITY_CONTRACT_ID or context["parent_fact_basis_eligibility_contract_id"] != PARENT_FACT_BASIS_ELIGIBILITY_CONTRACT_ID or context["immutable_event_clock_contract_id"] != IMMUTABLE_EVENT_CLOCK_CONTRACT_ID:
        raise ValueError("context nested contract identity mismatch")
    if context["direction_policy"] != "abstain":
        raise ValueError("context assigned direction")
    for field in ("fact_count", "upcoming_event_count", "causal_consensus_count"):
        _exact_int(context[field], path=f"root.component_context.{field}")
    _exact_bool(context["intraday_rates_connected"], path="root.component_context.intraday_rates_connected")
    _validate_global_gaps(context["global_gaps"], path="root.component_context.global_gaps")
    _validate_provenance(context["event_clock_provenance"], path="root.component_context.event_clock_provenance")

    metadata_by_id = context["fact_metadata_by_id"]
    if type(metadata_by_id) is not dict:
        raise ValueError("fact metadata must be an exact mapping")
    basis_totals = {basis: 0 for basis in OFFICIAL_BASIS_FIELDS}
    metadata_by_currency: dict[str, list[Mapping[str, Any]]] = {currency: [] for currency in contract["currencies"]}
    for fact_id, raw_metadata in metadata_by_id.items():
        if type(fact_id) is not str or not fact_id:
            raise ValueError("invalid fact metadata identity")
        path = f"root.component_context.fact_metadata_by_id.{fact_id}"
        metadata = _mapping(raw_metadata, OFFICIAL_FACT_METADATA_FIELDS, path=path, required=OFFICIAL_FACT_METADATA_FIELDS)
        for field in ("fact_id", "currency", "fact_type", "evidence_class", "source_id", "source_contract_id"):
            _exact_str(metadata[field], path=f"{path}.{field}")
        if metadata["fact_id"] != fact_id or metadata["currency"] not in metadata_by_currency:
            raise ValueError(f"fact metadata identity mismatch at {path}")
        if metadata["fact_type"] not in OFFICIAL_FACT_TYPES:
            raise ValueError(f"unknown fact type at {path}")
        if metadata["evidence_class"] not in OFFICIAL_FACT_EVIDENCE_CLASSES:
            raise ValueError(f"unknown evidence class at {path}")
        _timestamp(metadata["effective_from_utc"], path=f"{path}.effective_from_utc")
        _exact_bool(metadata["consensus_causal"], path=f"{path}.consensus_causal")
        _hash64(metadata["raw_payload_sha256"], path=f"{path}.raw_payload_sha256")
        _string_list(metadata["degradation_reasons"], path=f"{path}.degradation_reasons")
        _exact_bool(metadata["proof_eligible_for_any_direction_basis"], path=f"{path}.proof_eligible_for_any_direction_basis")
        basis = _mapping(metadata["basis_eligibility"], OFFICIAL_BASIS_FIELDS, path=f"{path}.basis_eligibility", required=OFFICIAL_BASIS_FIELDS)
        for name, flag in basis.items():
            _exact_bool(flag, path=f"{path}.basis_eligibility.{name}")
            basis_totals[name] += int(flag)
        if metadata["proof_eligible_for_any_direction_basis"] is not any(basis.values()):
            raise ValueError(f"proof eligibility mismatch at {path}")
        metadata_by_currency[metadata["currency"]].append(metadata)
    if context["fact_count"] != len(metadata_by_id):
        raise ValueError("context fact count does not match metadata")
    basis_counts = _mapping(context["basis_eligible_fact_counts"], OFFICIAL_BASIS_FIELDS, path="root.component_context.basis_eligible_fact_counts", required=OFFICIAL_BASIS_FIELDS)
    for basis, count in basis_counts.items():
        _exact_int(count, path=f"root.component_context.basis_eligible_fact_counts.{basis}")
        if count != basis_totals[basis]:
            raise ValueError(f"basis count mismatch for {basis}")

    summaries = context["currency_summary"]
    if type(summaries) is not dict or set(summaries) != set(contract["currencies"]):
        raise ValueError("context summaries do not cover exactly 21 currencies")
    summary_rows: dict[str, Mapping[str, Any]] = {}
    for currency in contract["currencies"]:
        path = f"root.component_context.currency_summary.{currency}"
        summary = _validate_summary(summaries[currency], path=path, currency=currency)
        metadata = metadata_by_currency[currency]
        if set(summary["fact_ids"]) != {item["fact_id"] for item in metadata}:
            raise ValueError(f"summary fact identities mismatch at {path}")
        expected_types: dict[str, int] = {}
        expected_classes: dict[str, int] = {}
        for item in metadata:
            expected_types[item["fact_type"]] = expected_types.get(item["fact_type"], 0) + 1
            expected_classes[item["evidence_class"]] = expected_classes.get(item["evidence_class"], 0) + 1
        if summary["fact_type_counts"] != dict(sorted(expected_types.items())) or summary["evidence_class_counts"] != dict(sorted(expected_classes.items())):
            raise ValueError(f"summary count mapping mismatch at {path}")
        if summary["causal_consensus_count"] != sum(item["consensus_causal"] is True for item in metadata):
            raise ValueError(f"summary consensus count mismatch at {path}")
        summary_rows[currency] = summary
    if sum(item["fact_count"] for item in summary_rows.values()) != context["fact_count"] or sum(item["upcoming_event_count"] for item in summary_rows.values()) != context["upcoming_event_count"] or sum(item["causal_consensus_count"] for item in summary_rows.values()) != context["causal_consensus_count"]:
        raise ValueError("context aggregate counts do not reconcile with summaries")

    horizons = row["horizons"]
    expected_horizons = {str(int(value)) for value in contract["horizons_sec"]}
    expected_currencies = frozenset(str(value) for value in contract["currencies"])
    expected_instruments = frozenset(str(value) for value in contract["instruments"])
    if type(horizons) is not dict or set(horizons) != expected_horizons:
        raise ValueError("context horizons do not match contract")
    for horizon_id, raw_horizon in horizons.items():
        path = f"root.horizons.{horizon_id}"
        horizon = _mapping(raw_horizon, _HORIZON_FIELDS, path=path, required=_HORIZON_FIELDS)
        _exact_int(horizon["horizon_sec"], path=f"{path}.horizon_sec", minimum=1)
        if str(horizon["horizon_sec"]) != horizon_id:
            raise ValueError(f"horizon identity mismatch at {path}")
        for field in ("decision_cutoff_utc", "completed_bar_cutoff_utc"):
            _timestamp(horizon[field], path=f"{path}.{field}")
        if horizon["decision_cutoff_utc"] != row["decision_cutoff_utc"] or horizon["completed_bar_cutoff_utc"] != row["completed_bar_cutoff_utc"]:
            raise ValueError(f"horizon cutoff mismatch at {path}")
        rejected = horizon["rejected_observation_counts"]
        if type(rejected) is not dict or any(type(key) is not str for key in rejected):
            raise ValueError(f"invalid rejected count mapping at {path}")
        for key, count in rejected.items():
            _exact_int(count, path=f"{path}.rejected_observation_counts.{key}")
        _validate_solver(horizon["solver"], path=f"{path}.solver", currencies=expected_currencies, instruments=expected_instruments)
        currencies = horizon["currencies"]
        edges = horizon["pair_edges"]
        if type(currencies) is not dict or set(currencies) != expected_currencies:
            raise ValueError(f"currency map mismatch at {path}")
        if type(edges) is not dict or set(edges) != expected_instruments:
            raise ValueError(f"edge map mismatch at {path}")
        for currency, raw_currency in currencies.items():
            currency_path = f"{path}.currencies.{currency}"
            currency_row = _mapping(raw_currency, _CURRENCY_FIELDS, path=currency_path, required=_CURRENCY_FIELDS)
            if currency_row["component_id"] is not None:
                _exact_str(
                    currency_row["component_id"],
                    path=f"{currency_path}.component_id",
                    nonempty=True,
                )
            for field in ("currency", "state"):
                _exact_str(currency_row[field], path=f"{currency_path}.{field}", nonempty=True)
            if currency_row["currency"] != currency:
                raise ValueError(f"currency row identity mismatch at {currency_path}")
            for field in ("horizon_sec", "pair_observation_count", "expected_pair_count"):
                _exact_int(currency_row[field], path=f"{currency_path}.{field}")
            if currency_row["horizon_sec"] != horizon["horizon_sec"]:
                raise ValueError(f"currency horizon mismatch at {currency_path}")
            for field in ("observed_currency_return_bps", "observed_response_uncertainty_bps", "forecast_mean_bps", "forecast_absolute_move_bps", "forecast_uncertainty_bps", "cost_clear_probability"):
                _optional_number(currency_row[field], path=f"{currency_path}.{field}")
            _finite_number(currency_row["coverage_ratio"], path=f"{currency_path}.coverage_ratio", minimum=0.0)
            if float(currency_row["coverage_ratio"]) > 1.0:
                raise ValueError(f"coverage ratio above one at {currency_path}")
            for field in ("forecast_mean_bps", "forecast_absolute_move_bps", "forecast_uncertainty_bps", "cost_clear_probability"):
                if currency_row[field] is not None:
                    raise ValueError(f"context populated {field} at {currency_path}")
            _string_list(currency_row["abstention_reasons"], path=f"{currency_path}.abstention_reasons")
            components = currency_row["components"]
            if type(components) is not list:
                raise ValueError(f"component list missing at {currency_path}")
            component_rows = [_validate_component(item, path=f"{currency_path}.components[{index}]") for index, item in enumerate(components)]
            if {item["component_id"] for item in component_rows} != _ALL_COMPONENT_IDS or len(component_rows) != len(_ALL_COMPONENT_IDS):
                raise ValueError(f"component coverage mismatch at {currency_path}")
            summary = _validate_summary(currency_row["official_fact_context"], path=f"{currency_path}.official_fact_context", currency=currency)
            if dict(summary) != dict(summary_rows[currency]):
                raise ValueError(f"summary copy mismatch at {currency_path}")
        for instrument, raw_edge in edges.items():
            edge_path = f"{path}.pair_edges.{instrument}"
            edge = _mapping(raw_edge, _EDGE_FIELDS, path=edge_path, required=_EDGE_FIELDS)
            for field in ("instrument", "base_currency", "quote_currency", "state", "observed_residual_basis", "observed_solver_residual_basis"):
                _exact_str(edge[field], path=f"{edge_path}.{field}", nonempty=True)
            base, quote = instrument.split("_")
            if edge["instrument"] != instrument or edge["base_currency"] != base or edge["quote_currency"] != quote:
                raise ValueError(f"edge identity mismatch at {edge_path}")
            for field in ("horizon_sec", "declared_horizon_sec"):
                _exact_int(edge[field], path=f"{edge_path}.{field}", minimum=1)
                if edge[field] != horizon["horizon_sec"]:
                    raise ValueError(f"edge horizon mismatch at {edge_path}.{field}")
            for field in _EDGE_FIELDS - {"instrument", "base_currency", "quote_currency", "state", "observed_residual_basis", "observed_solver_residual_basis", "horizon_sec", "declared_horizon_sec", "observed_return_was_clipped", "exact_horizon_observation", "research_observable", "execution_eligible", "abstention_reasons"}:
                _optional_number(edge[field], path=f"{edge_path}.{field}")
            for field in ("observed_return_was_clipped", "exact_horizon_observation"):
                _optional_bool(edge[field], path=f"{edge_path}.{field}")
            _exact_bool(edge["research_observable"], path=f"{edge_path}.research_observable")
            _exact_bool(edge["execution_eligible"], path=f"{edge_path}.execution_eligible")
            _string_list(edge["abstention_reasons"], path=f"{edge_path}.abstention_reasons")
            if edge["forecast_mean_bps"] is not None or edge["forecast_absolute_move_bps"] is not None or edge["cost_clear_probability"] is not None or edge["expected_net_pips"] is not None or edge["allocator_rank"] is not None or edge["execution_eligible"] is not False:
                raise ValueError(f"context created forecast or execution surface at {edge_path}")
            base_value = currencies[base]["observed_currency_return_bps"]
            quote_value = currencies[quote]["observed_currency_return_bps"]
            factor = edge["observed_factor_move_bps"]
            if base_value is None or quote_value is None:
                if factor is not None:
                    raise ValueError(f"context invented factor at {edge_path}")
            elif factor is None or not math.isclose(float(factor), float(base_value) - float(quote_value), abs_tol=1e-9):
                raise ValueError(f"context violates base-minus-quote algebra at {edge_path}")


def attach_official_fact_context_v3(
    currency_snapshot: Mapping[str, Any],
    official_snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach exact V3 official context without altering base market state."""

    base_identity = validate_base_currency_state_identity(currency_snapshot, contract=contract)
    try:
        validate_official_fact_v3_snapshot(official_snapshot)
    except OfficialFactAdapterV3Error as exc:
        raise ValueError(f"official-fact V3 snapshot integrity failed: {exc}") from exc
    if official_snapshot["schema_version"] != OFFICIAL_FACT_ADAPTER_SCHEMA_VERSION or official_snapshot["adapter_contract_id"] != OFFICIAL_FACT_ADAPTER_CONTRACT_ID:
        raise ValueError("OfficialFact V3 identity is required")
    output = attach_official_fact_context_v1(currency_snapshot, official_snapshot, contract=contract)
    _validate_preserved_horizons(currency_snapshot, output)
    output["snapshot_schema"] = SNAPSHOT_SCHEMA
    output["component_context_contract_id"] = CONTEXT_CONTRACT_ID
    output["parent_component_context_contract_id"] = PARENT_CONTEXT_CONTRACT_ID
    output["official_fact_adapter_contract_id"] = OFFICIAL_FACT_ADAPTER_CONTRACT_ID
    output["immutable_event_clock_schema_version"] = IMMUTABLE_EVENT_CLOCK_SCHEMA_VERSION
    output["immutable_event_clock_contract_id"] = IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    output["base_currency_state_snapshot_sha256"] = base_identity["snapshot_sha256"]
    output["base_currency_state_horizons_sha256"] = base_identity["horizons_sha256"]
    context = output["component_context"]
    context["fact_basis_eligibility_contract_id"] = FACT_BASIS_ELIGIBILITY_CONTRACT_ID
    context["parent_fact_basis_eligibility_contract_id"] = PARENT_FACT_BASIS_ELIGIBILITY_CONTRACT_ID
    context["immutable_event_clock_contract_id"] = IMMUTABLE_EVENT_CLOCK_CONTRACT_ID
    context["event_clock_provenance"] = copy.deepcopy(official_snapshot["event_clock_provenance"])
    output["can_promote"] = False
    output["can_authorize"] = False
    output["status"] = STATUS
    material = {key: value for key, value in output.items() if key != "snapshot_id"}
    output["snapshot_id"] = "currency_state_official_v3_" + stable_hash(material)[:24]
    validate_currency_state_official_context_v3_snapshot(output, contract=contract)
    return output


def validate_currency_state_official_context_v3_snapshot(
    snapshot: Mapping[str, Any], *, contract: Mapping[str, Any] | None = None
) -> None:
    """Validate every composite leaf and its full-snapshot identity."""

    active_contract = dict(contract or load_contract())
    validate_contract(active_contract)
    try:
        _validate_context_schema(snapshot, contract=active_contract)
    except OfficialFactAdapterV3Error as exc:
        raise ValueError(
            f"currency-state official-context V3 typed integrity failed: {exc}"
        ) from exc
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    expected_id = "currency_state_official_v3_" + stable_hash(material)[:24]
    if snapshot.get("snapshot_id") != expected_id:
        raise ValueError("currency-state official-context V3 snapshot identity mismatch")


__all__ = [
    "CONTEXT_CONTRACT_ID",
    "CONTEXT_V3_TOP_LEVEL_FIELDS",
    "FACT_BASIS_ELIGIBILITY_CONTRACT_ID",
    "SNAPSHOT_SCHEMA",
    "attach_official_fact_context_v3",
    "validate_base_currency_state_identity",
    "validate_currency_state_official_context_v3_snapshot",
]
