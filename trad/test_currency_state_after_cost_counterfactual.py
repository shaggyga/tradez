from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

try:
    from forex_system.contracts.signed_currency_exposure import canonical_factor_ids
    from forex_system.research.currency_state_after_cost_counterfactual import (
        AfterCostCounterfactualError,
        build_after_cost_counterfactual,
    )
    from forex_system.research.currency_state_response_timing_arms import (
        build_response_timing_arms,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.signed_currency_exposure import canonical_factor_ids
    from src.forex_system.research.currency_state_after_cost_counterfactual import (
        AfterCostCounterfactualError,
        build_after_cost_counterfactual,
    )
    from src.forex_system.research.currency_state_response_timing_arms import (
        build_response_timing_arms,
    )

from test_currency_state_response_timing_arms import (
    SEMANTIC_LINEAGE,
    arm_contract,
    envelope,
    official_context_snapshot,
)


ROOT = Path(__file__).resolve().parent
TEST_SHA = "a" * 64


def artifact_fingerprints():
    return {
        "counterfactual_module_sha256": TEST_SHA,
        "counterfactual_config_file_sha256": "b" * 64,
        "response_module_sha256": "c" * 64,
        "response_config_file_sha256": "d" * 64,
        "currency_state_module_sha256": "e" * 64,
        "currency_state_contract_file_sha256": "f" * 64,
        "official_context_module_sha256": "1" * 64,
    }


def counterfactual_contract():
    return json.loads(
        (ROOT / "config" / "currency_state_after_cost_counterfactual_v1.json").read_text(
            encoding="utf-8"
        )
    )


def official_record(
    instrument: str,
    *,
    horizon: int = 3600,
    direction: str = "buy",
    number: int = 1,
):
    base, quote = instrument.split("_")
    return {
        "thesis_id": f"thesis-{instrument}-{horizon}-{number}",
        "instrument": instrument,
        "horizon_sec": horizon,
        "known_at_utc": "2026-01-01T00:45:00+00:00",
        "direction": direction,
        "direction_basis": "versioned_semantic_thesis",
        **SEMANTIC_LINEAGE,
        "expected_absolute_move_bps": 8.0,
        "uncertainty_bps": 2.0,
        "source_ids": [f"official:{base}:fixture", f"official:{quote}:fixture"],
        "source_fact_ids": [f"fixture:{base}:semantic", f"fixture:{quote}:semantic"],
        "independent_factor_ids": [canonical_factor_ids(instrument, direction)[0]],
    }


def response_with_official(records):
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    fact_metadata = source["component_context"]["fact_metadata_by_id"]
    for raw in records:
        for currency in str(raw["instrument"]).split("_"):
            fact_metadata.setdefault(
                f"fixture:{currency}:semantic",
                {
                    "fact_id": f"fixture:{currency}:semantic",
                    "currency": currency,
                    "basis_eligibility": {"versioned_semantic_thesis": True},
                },
            )
    theses = envelope(
        cutoff,
        contract_id="frozen-official-thesis-contract-v1",
        cohort_id="official-cohort-001",
        records=records,
    )
    return build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        official_theses=theses,
    )


def economics_record(
    instrument: str,
    *,
    arm_id: str = "official_context_only",
    horizon: int = 3600,
    direction: str = "buy",
    favorable: float = 8.0,
    adverse: float = 3.0,
    p_direction: float = 0.7,
    p_clear: float = 0.65,
    number: int = 1,
):
    base, quote = instrument.split("_")
    return {
        "economics_record_id": f"economics-{arm_id}-{instrument}-{horizon}-{number}",
        "arm_id": arm_id,
        "instrument": instrument,
        "horizon_sec": horizon,
        "known_at_utc": "2026-01-01T01:00:00+00:00",
        "forecast_direction": direction,
        "forecast_id": f"forecast-{instrument}-{horizon}-{number}",
        "model_contract_id": "frozen-model-v1",
        "model_contract_sha256": "3" * 64,
        "model_cohort_id": "frozen-model-cohort-001",
        "model_cohort_sha256": "4" * 64,
        "feature_contract_id": "frozen-features-v1",
        "feature_contract_sha256": "5" * 64,
        "calibration_contract_id": "locked-calibration-v1",
        "calibration_contract_sha256": "6" * 64,
        "cost_model_contract_id": "locked-cost-model-v1",
        "cost_model_contract_sha256": "7" * 64,
        "probability_known_at_utc": "2026-01-01T00:59:00+00:00",
        "magnitude_known_at_utc": "2026-01-01T00:59:00+00:00",
        "slippage_known_at_utc": "2026-01-01T00:58:00+00:00",
        "latency_known_at_utc": "2026-01-01T00:58:00+00:00",
        "rotation_cost_known_at_utc": "2026-01-01T00:58:00+00:00",
        "calibration_data_cutoff_utc": "2025-12-31T23:00:00+00:00",
        "point_in_time_validated": True,
        "calibration_state": "out_of_fold_locked",
        "calibration_effective_n": 75.0,
        "calibrated_direction_probability": p_direction,
        "calibrated_cost_clear_probability": p_clear,
        "expected_favorable_move_bps": favorable,
        "expected_adverse_move_bps": adverse,
        "modeled_slippage_bps": 0.15,
        "latency_cost_bps": 0.10,
        "rotation_cost_bps": 0.20,
        "source_ids": [f"official:{base}:fixture", f"official:{quote}:fixture", "frozen-model-v1"],
        "source_fact_ids": [f"fixture:{base}:semantic", f"fixture:{quote}:semantic"],
        "independent_factor_ids": [canonical_factor_ids(instrument, direction)[0]],
    }


def research_envelope(cutoff, records, *, kind="economics"):
    return {
        "payload_id": f"{kind}-payload-001",
        "contract_id": f"{kind}-input-contract-v1",
        "contract_sha256": "8" * 64,
        "cohort_id": f"{kind}-input-cohort-001",
        "cohort_sha256": "9" * 64,
        "frozen_at_utc": cutoff,
        "decision_cutoff_utc": cutoff,
        "records": records,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def find(result, arm_id, instrument="EUR_USD", horizon=3600):
    return next(
        row
        for row in result["records"]
        if row["arm_id"] == arm_id
        and row["instrument"] == instrument
        and row["horizon_sec"] == horizon
    )


def test_empty_economics_retains_full_grid_and_is_explicitly_fail_closed():
    response = response_with_official([])
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
    )
    assert result["row_count"] == 8 * 68 * 5
    assert result["edge_horizon_count"] == 68 * 5
    assert result["summary"]["economics_admissible_count"] == 0
    assert result["summary"]["ranked_count"] == 0
    assert result["summary"]["selectable_counterfactual_count"] == 0
    assert result["summary"]["top_one_available_count"] == 0
    assert result["supported_execution_decision"] == "no_trade"
    assert result["execution_eligible"] is False
    assert result["can_place_orders"] is False
    assert find(result, "observed_price_only")["blockers"] == [
        "observed_price_hindsight_forbidden_as_forward_forecast"
    ]
    for row in result["records"]:
        assert row["expected_after_cost_ev_bps"] is None
        assert row["rank_within_arm_horizon"] is None
        assert row["paper_decision"] == "no_trade"
        assert row["execution_eligible"] is False
        assert row["can_place_orders"] is False


def test_complete_point_in_time_economics_calculates_ev_and_preserves_lineage():
    response = response_with_official([official_record("EUR_USD")])
    record = economics_record("EUR_USD")
    inputs = research_envelope(response["decision_cutoff_utc"], [record])
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=inputs,
    )
    row = find(result, "official_context_only")
    expected_gross = 0.7 * 8.0 - 0.3 * 3.0
    expected_cost = row["expected_costs_bps"]["executable_spread"] + 0.15 + 0.10 + 0.20
    assert row["economics_admissible"] is True
    assert row["blockers"] == []
    assert row["expected_gross_ev_bps"] == pytest.approx(expected_gross)
    assert row["expected_after_cost_ev_bps"] == pytest.approx(expected_gross - expected_cost)
    assert row["rank_within_arm_horizon"] == 1
    assert row["selectable_counterfactual"] is True
    assert row["independent_factor_ids"] == list(canonical_factor_ids("EUR_USD", "buy")[:1])
    assert set(row["source_fact_ids"]) == {"fixture:EUR:semantic", "fixture:USD:semantic"}
    assert row["lineage_id"] in result["lineage_registry"]
    assert result["counterfactual_cohort_id"] == (
        "currency_state_after_cost_counterfactual_cohort_20260817a"
    )
    assert result["artifact_fingerprints"]["fingerprint_manifest_id"].startswith(
        "counterfactual_manifest_"
    )
    assert len(result["response_snapshot_sha256"]) == 64
    allocation = result["allocations"]["official_context_only"]["3600"]
    assert allocation["top_one"]["instrument"] == "EUR_USD"
    assert allocation["supported_execution_decision"] == "no_trade"


def test_observed_price_control_cannot_receive_forward_economics_or_rank():
    response = response_with_official([official_record("EUR_USD")])
    record = economics_record("EUR_USD", arm_id="observed_price_only")
    inputs = research_envelope(response["decision_cutoff_utc"], [record])
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=inputs,
    )
    row = find(result, "observed_price_only")
    assert row["economics_admissible"] is False
    assert row["expected_after_cost_ev_bps"] is None
    assert row["rank_within_arm_horizon"] is None
    assert "observed_price_hindsight_forbidden_as_forward_forecast" in row["blockers"]
    assert any(
        rejection["reason"] == "arm_not_forward_admissible"
        for rejection in result["input_rejections"]
    )


def test_fact_id_presence_without_explicit_basis_eligibility_cannot_produce_ev():
    response = copy.deepcopy(response_with_official([official_record("EUR_USD")]))
    source_row = next(
        row
        for row in response["arms"]["official_context_only"]["records"]
        if row["instrument"] == "EUR_USD" and row["horizon_sec"] == 3600
    )
    source_row["lineage"]["official_thesis"]["basis_eligible_for_after_cost"] = False
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=research_envelope(
            response["decision_cutoff_utc"], [economics_record("EUR_USD")]
        ),
    )
    row = find(result, "official_context_only")
    assert row["source_fact_ids"]
    assert row["economics_admissible"] is False
    assert row["expected_after_cost_ev_bps"] is None
    assert row["blockers"] == ["response_basis_not_explicitly_eligible_for_after_cost"]


def test_artifact_fingerprints_are_mandatory_and_sha256_validated():
    response = response_with_official([])
    with pytest.raises(AfterCostCounterfactualError, match="artifact_fingerprints are required"):
        build_after_cost_counterfactual(
            response,
            contract=counterfactual_contract(),
            artifact_fingerprints=None,
        )
    invalid = artifact_fingerprints()
    invalid["response_module_sha256"] = "not-a-hash"
    with pytest.raises(AfterCostCounterfactualError, match="response_module_sha256"):
        build_after_cost_counterfactual(
            response,
            contract=counterfactual_contract(),
            artifact_fingerprints=invalid,
        )


@pytest.mark.parametrize(
    "mutation,reason",
    [
        (
            lambda row: row.update(
                {"probability_known_at_utc": "2026-01-01T01:01:00+00:00"}
            ),
            "probability_known_at_utc_after_record_knowledge",
        ),
        (
            lambda row: row.update({"calibration_effective_n": 49.0}),
            "calibration_effective_n_below_floor",
        ),
        (
            lambda row: row.update({"modeled_slippage_bps": None}),
            "missing_or_negative_magnitude_or_cost",
        ),
        (
            lambda row: row.update({"independent_factor_ids": ["currency:USD:long"]}),
            "factor_lineage_does_not_exactly_match_response_thesis",
        ),
    ],
)
def test_incomplete_or_noncausal_economics_remains_null(mutation, reason):
    response = response_with_official([official_record("EUR_USD")])
    record = economics_record("EUR_USD")
    mutation(record)
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=research_envelope(response["decision_cutoff_utc"], [record]),
    )
    row = find(result, "official_context_only")
    assert row["economics_admissible"] is False
    assert row["expected_after_cost_ev_bps"] is None
    assert row["rank_within_arm_horizon"] is None
    assert reason in row["blockers"]


def test_executable_spread_is_recomputed_and_mismatch_blocks_ev():
    response = response_with_official([official_record("EUR_USD")])
    response = copy.deepcopy(response)
    source_row = next(
        row
        for row in response["arms"]["official_context_only"]["records"]
        if row["instrument"] == "EUR_USD" and row["horizon_sec"] == 3600
    )
    source_row["cost_context"]["observed_entry_spread_bps"] += 0.25
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=research_envelope(
            response["decision_cutoff_utc"], [economics_record("EUR_USD")]
        ),
    )
    row = find(result, "official_context_only")
    assert row["expected_after_cost_ev_bps"] is None
    assert "spread_not_consistent_with_executable_bid_ask" in row["blockers"]


def test_top_one_and_disjoint_basket_do_not_reuse_currency_or_factor():
    instruments = ["EUR_USD", "GBP_USD", "AUD_JPY", "CAD_CHF"]
    theses = [official_record(instrument, number=index) for index, instrument in enumerate(instruments)]
    response = response_with_official(theses)
    economics = [
        economics_record("EUR_USD", favorable=500.0, number=1),
        economics_record("GBP_USD", favorable=400.0, number=2),
        economics_record("AUD_JPY", favorable=300.0, number=3),
        economics_record("CAD_CHF", favorable=18.0, number=4),
    ]
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=research_envelope(response["decision_cutoff_utc"], economics),
    )
    allocation = result["allocations"]["official_context_only"]["3600"]
    assert allocation["top_one"]["instrument"] == "EUR_USD"
    assert [row["instrument"] for row in allocation["disjoint_basket"]] == [
        "EUR_USD",
        "AUD_JPY",
        "CAD_CHF",
    ]
    assert find(result, "official_context_only", "EUR_USD")["rank_within_arm_horizon"] == 1
    assert find(result, "official_context_only", "GBP_USD")["rank_within_arm_horizon"] == 2


def valid_hold_record(instrument="GBP_USD", *, expected_favorable=2.0):
    return {
        "hold_record_id": "hold-001",
        "arm_id": "official_context_only",
        "instrument": instrument,
        "horizon_sec": 3600,
        "hold_direction": "buy",
        "known_at_utc": "2026-01-01T01:00:00+00:00",
        "probability_known_at_utc": "2026-01-01T00:59:00+00:00",
        "magnitude_known_at_utc": "2026-01-01T00:59:00+00:00",
        "exit_cost_known_at_utc": "2026-01-01T00:58:00+00:00",
        "point_in_time_validated": True,
        "calibration_state": "out_of_fold_locked",
        "calibration_effective_n": 75.0,
        "calibrated_direction_probability": 0.60,
        "expected_favorable_move_bps": expected_favorable,
        "expected_adverse_move_bps": 2.0,
        "modeled_exit_spread_bps": 1.0,
        "modeled_exit_slippage_bps": 0.1,
        "modeled_exit_latency_bps": 0.1,
        "hold_model_contract_id": "hold-model-v1",
        "hold_model_contract_sha256": "a" * 64,
        "calibration_contract_id": "hold-calibration-v1",
        "calibration_contract_sha256": "b" * 64,
        "cost_model_contract_id": "hold-cost-v1",
        "cost_model_contract_sha256": "c" * 64,
        "source_ids": ["hold-ledger", "official:USD:macro"],
        "independent_factor_ids": [canonical_factor_ids(instrument, "buy")[0]],
    }


def test_hold_vs_switch_is_computed_only_from_admissible_frozen_inputs():
    instruments = ["EUR_USD", "GBP_USD"]
    response = response_with_official(
        [official_record(instrument, number=index) for index, instrument in enumerate(instruments)]
    )
    economics = [
        economics_record("EUR_USD", favorable=10.0, number=1),
        economics_record("GBP_USD", favorable=6.0, number=2),
    ]
    holds = research_envelope(
        response["decision_cutoff_utc"], [valid_hold_record()], kind="hold"
    )
    result = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=research_envelope(response["decision_cutoff_utc"], economics),
        hold_states=holds,
    )
    comparison = result["hold_switch_counterfactuals"][0]
    assert comparison["state"] == "paper_switch"
    assert comparison["best_switch_candidate"]["instrument"] == "EUR_USD"
    assert comparison["incremental_switch_ev_bps"] > 0.5
    assert comparison["supported_execution_decision"] == "no_trade"


def test_snapshot_id_is_deterministic_and_never_contains_an_actionable_decision():
    response = response_with_official([official_record("EUR_USD")])
    inputs = research_envelope(
        response["decision_cutoff_utc"], [economics_record("EUR_USD")]
    )
    first = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=inputs,
    )
    second = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract(),
        artifact_fingerprints=artifact_fingerprints(),
        economics_inputs=copy.deepcopy(inputs),
    )
    assert first["snapshot_id"] == second["snapshot_id"]
    assert all(row["paper_decision"] == "no_trade" for row in first["records"])
    assert all(row["supported_execution_decision"] == "no_trade" for row in first["records"])


def test_duplicate_economics_for_one_arm_pair_horizon_is_rejected_structurally():
    response = response_with_official([official_record("EUR_USD")])
    record = economics_record("EUR_USD")
    duplicate = copy.deepcopy(record)
    duplicate["economics_record_id"] = "economics-duplicate"
    with pytest.raises(AfterCostCounterfactualError, match="duplicate economics"):
        build_after_cost_counterfactual(
            response,
            contract=counterfactual_contract(),
            artifact_fingerprints=artifact_fingerprints(),
            economics_inputs=research_envelope(
                response["decision_cutoff_utc"], [record, duplicate]
            ),
        )
