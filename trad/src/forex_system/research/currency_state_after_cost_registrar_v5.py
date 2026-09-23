"""Deterministic registrar definition for the engineering-blocked V5 cohort.

This module does not write to the research genealogy.  It defines the exact
row and immutable definition contract that a future registrar must insert.
The V5 publisher independently re-hashes and compares every stored column.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from ..contracts.currency_state import stable_hash


EXPERIMENT_COLUMNS = (
    "hypothesis_id", "parent_hypothesis_id", "experiment_kind",
    "research_generation", "idea_origin", "pre_registered",
    "data_sources_json", "feature_contract_json", "label_contract_json",
    "model_contract_json", "cost_contract_json", "allocator_contract_json",
    "training_period_json", "selection_period_json",
    "confirmation_period_json", "all_parameters_tried_json",
    "selection_rule", "holdouts_touched_json", "source_code_hash",
    "data_snapshot_hash", "definition_sha256", "created_at",
    "definition_json",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def registrar_contract_v5(
    *,
    contract: Mapping[str, Any],
    external_anchor: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the complete policy definition hashed by the registrar."""
    return {
        "cohort_id": contract["counterfactual_cohort_id"],
        "contract_id": contract["contract_id"],
        "snapshot_schema": contract["snapshot_schema"],
        "supersedes_contract_id": contract["supersedes_contract_id"],
        "supersedes_cohort_id": contract["supersedes_cohort_id"],
        "external_trust_anchor": dict(external_anchor),
        "canonical_genealogy_relative_path": contract["genealogy_contract"][
            "canonical_relative_path"
        ],
        "identity_registry_contract": dict(contract["identity_registry_contract"]),
        "operational_input_policy": dict(contract["operational_input_policy"]),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def registrar_definition_v5(
    *,
    contract: Mapping[str, Any],
    external_anchor: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the exact 23-column experiments row for V5 registration."""
    definition_contract = registrar_contract_v5(
        contract=contract, external_anchor=external_anchor,
    )
    artifacts = external_anchor["artifacts"]
    cohort_id = contract["counterfactual_cohort_id"]
    return {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": contract["supersedes_cohort_id"],
        "experiment_kind": "engineering_policy_level_counterfactual",
        "research_generation": "currency_state_branch_20260817",
        "idea_origin": "currency_state_after_cost_v5_identity_and_genealogy_remediation",
        "pre_registered": 1,
        "data_sources_json": canonical_json([
            "CurrencyState_v2",
            "OfficialFactAdapter",
            "versioned_response_timing_arms",
            "OANDA_executable_bid_ask",
            "locked_calibration_and_cost_envelopes_when_available",
        ]),
        "feature_contract_json": canonical_json({
            "response_contract_id": contract["required_response_contract_id"],
            "horizons_sec": contract["expected_horizons_sec"],
            "arm_ids": contract["expected_arm_ids"],
        }),
        "label_contract_json": canonical_json({
            "targets": [
                "after_cost_ev", "cost_clearance", "top_one",
                "disjoint_basket", "hold_switch",
            ],
            "observed_price_is_never_forward_evidence": True,
        }),
        "model_contract_json": canonical_json({
            "contract_id": contract["contract_id"],
            "config_sha256": artifacts["counterfactual_v5_config"]["sha256"],
            "module_sha256": artifacts["counterfactual_v5_module"]["sha256"],
            "manifest_id": external_anchor["manifest_id"],
            "manifest_sha256": external_anchor["manifest_sha256"],
        }),
        "cost_contract_json": canonical_json({
            "missing_cost_policy": "unavailable_not_zero",
            "executable_bid_ask_required": True,
            "config_sha256": artifacts["counterfactual_v5_config"]["sha256"],
        }),
        "allocator_contract_json": canonical_json({
            "top_one_and_disjoint_basket": True,
            "hold_switch": True,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        }),
        "training_period_json": "{}",
        "selection_period_json": canonical_json({
            "cohort_start_utc": contract["cohort_start_utc"],
        }),
        "confirmation_period_json": canonical_json({
            "untouched_prospective_required": True,
        }),
        "all_parameters_tried_json": canonical_json({
            "frozen_by_contract_and_byte_hash_manifest": True,
            "persistent_identity_registry_required_before_nonempty_input": True,
            "producer_integration_missing": True,
        }),
        "selection_rule": (
            "rank only causally grounded rows with locked probability, magnitude, "
            "executable spread, slippage, latency, rotation economics, and "
            "persistent non-equivocating replay identity"
        ),
        "holdouts_touched_json": "[]",
        "source_code_hash": artifacts["counterfactual_v5_module"]["sha256"],
        "data_snapshot_hash": "zero_input_engineering_initialization",
        "definition_sha256": stable_hash(definition_contract),
        "created_at": contract["cohort_start_utc"],
        "definition_json": canonical_json(definition_contract),
    }


__all__ = [
    "EXPERIMENT_COLUMNS", "canonical_json", "registrar_contract_v5",
    "registrar_definition_v5",
]
