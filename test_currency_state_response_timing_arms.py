from __future__ import annotations

import copy
import json
import math
from pathlib import Path

import pytest

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.features.currency_state_engine import build_currency_state_snapshot
    from forex_system.features.currency_state_official_context import attach_official_fact_context
    from forex_system.research.currency_state_response_timing_arms import (
        ResponseTimingArmError,
        build_response_timing_arms,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.features.currency_state_engine import build_currency_state_snapshot
    from src.forex_system.features.currency_state_official_context import attach_official_fact_context
    from src.forex_system.research.currency_state_response_timing_arms import (
        ResponseTimingArmError,
        build_response_timing_arms,
    )


ROOT = Path(__file__).resolve().parent
SEMANTIC_LINEAGE = {
    "semantic_contract_id": "semantic-thesis-fixture-v1",
    "semantic_contract_sha256": "a" * 64,
    "semantic_model_version": "fixture-model-v1",
    "semantic_prompt_sha256": "b" * 64,
    "semantic_known_at_utc": "2026-01-01T00:20:00+00:00",
}


def arm_contract():
    return json.loads(
        (ROOT / "config" / "currency_state_response_timing_arms_v1.json").read_text(
            encoding="utf-8"
        )
    )


def official_context_snapshot():
    contract = load_contract()
    currency_moves = {
        "AUD": -3.0,
        "CAD": -1.0,
        "CHF": 0.5,
        "CNH": -0.3,
        "CZK": 0.2,
        "DKK": 0.3,
        "EUR": 3.0,
        "GBP": 1.5,
        "HKD": -0.2,
        "HUF": -0.5,
        "JPY": 2.0,
        "MXN": -0.8,
        "NOK": 0.7,
        "NZD": -1.5,
        "PLN": 0.1,
        "SEK": 0.4,
        "SGD": 0.6,
        "THB": -0.1,
        "TRY": -2.5,
        "USD": -0.4,
        "ZAR": -0.7,
    }
    start = 1767225600
    rows = []
    for minute in range(62):
        for instrument in contract["instruments"]:
            base, quote = instrument.split("_")
            move_bps = (currency_moves[base] - currency_moves[quote]) * minute / 60.0
            mid = math.exp(move_bps / 10_000.0)
            pip = 0.01 if quote == "JPY" else 0.0001
            spread = pip * 1.2
            rows.append(
                {
                    "instrument": instrument,
                    "minute_epoch": start + minute * 60,
                    "last_epoch": start + minute * 60 + 2,
                    "close_bid": mid - spread / 2.0,
                    "close_ask": mid + spread / 2.0,
                    "pip": pip,
                }
            )
    base = build_currency_state_snapshot(
        {"generated_utc": "2026-01-01T01:02:00+00:00", "rows": rows},
        contract=contract,
        input_refs={"quote_history_sha256": "fixture-quotes"},
    )
    facts = [
        {
            "fact_id": "official:EUR:policy",
            "currency": "EUR",
            "fact_type": "official_policy_document_context",
            "evidence_class": "policy_context_only",
            "effective_from_utc": "2026-01-01T00:30:00+00:00",
            "published_at_utc": "2026-01-01T00:29:00+00:00",
            "first_seen_at_utc": "2026-01-01T00:30:00+00:00",
            "source_id": "official-eur-policy",
            "source_contract_id": "official-policy-fixture-v1",
            "raw_payload_sha256": "c" * 64,
            "degradation_reasons": [],
            "consensus_causal": False,
            "direction_policy": "abstain",
        },
        {
            "fact_id": "official:USD:macro",
            "currency": "USD",
            "fact_type": "official_macro_actual",
            "evidence_class": "prospective_causal",
            "effective_from_utc": "2026-01-01T00:40:00+00:00",
            "published_at_utc": "2026-01-01T00:39:00+00:00",
            "first_seen_at_utc": "2026-01-01T00:40:00+00:00",
            "source_id": "official-usd-macro",
            "source_contract_id": "official-macro-fixture-v1",
            "raw_payload_sha256": "d" * 64,
            "degradation_reasons": [],
            "consensus_causal": False,
            "direction_policy": "abstain",
        },
    ]
    official = {
        "snapshot_id": "official-fixture-id",
        "adapter_contract_id": "official-fixture-contract",
        "decision_cutoff_utc": base["decision_cutoff_utc"],
        "fact_count": len(facts),
        "upcoming_event_count": 0,
        "causal_consensus_count": 0,
        "facts": facts,
        "upcoming_events": [],
        "currency_evidence": {
            currency: {
                "source_health": {"state": "fixture"},
                "missing_or_degraded": ["no_causal_pre_release_consensus"],
            }
            for currency in contract["currencies"]
        },
        "global_gaps": [{"code": "no_causal_pre_release_consensus"}],
        "intraday_rates": {"connected": False},
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    return contract, attach_official_fact_context(base, official, contract=contract)


def envelope(cutoff, *, contract_id, cohort_id, records):
    return {
        "payload_id": f"payload:{cohort_id}",
        "contract_id": contract_id,
        "cohort_id": cohort_id,
        "frozen_at_utc": cutoff,
        "decision_cutoff_utc": cutoff,
        "records": records,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def find(snapshot, arm_id, instrument="EUR_USD", horizon=3600):
    return next(
        row
        for row in snapshot["arms"][arm_id]["records"]
        if row["instrument"] == instrument and row["horizon_sec"] == horizon
    )


def test_contract_cannot_disable_official_thesis_grounding():
    state_contract, source = official_context_snapshot()
    weakened = arm_contract()
    weakened["official_thesis_grounding"]["require_source_fact_ids_present_in_context_snapshot"] = False
    with pytest.raises(ResponseTimingArmError, match="grounding requirements cannot be disabled"):
        build_response_timing_arms(source, state_contract=state_contract, arm_contract=weakened)


def test_empty_inputs_preserve_all_edges_and_fail_closed():
    state_contract, source = official_context_snapshot()
    result = build_response_timing_arms(
        source, state_contract=state_contract, arm_contract=arm_contract()
    )
    assert result["edge_horizon_count"] == 68 * 5
    assert result["arm_count"] == 8
    assert result["supported_execution_decision"] == "no_trade"
    assert result["execution_eligible"] is False
    assert result["can_place_orders"] is False

    for arm in result["arms"].values():
        assert arm["record_count"] == 68 * 5
        assert arm["execution_eligible"] is False
        for row in arm["records"]:
            assert row["execution_eligible"] is False
            assert row["can_place_orders"] is False
            assert row["supported_execution_decision"] == "no_trade"
            assert row["source_edge"]["instrument"] == row["instrument"]
            assert row["cost_context"]["observed_entry_spread_bps"] == row["source_edge"]["spread_bps"]

    official = find(result, "official_context_only")
    assert official["paper_action"] == "abstain"
    assert official["reasons"] == ["no_explicit_frozen_official_thesis"]
    technical = find(result, "technical_timing_verification")
    assert technical["paper_action"] == "abstain"
    assert technical["research_direction"] is None
    magnitude = find(result, "magnitude_only")
    assert magnitude["paper_action"] == "abstain"
    assert magnitude["expected_absolute_move_bps"] is None


def test_technical_conflict_is_separate_and_cannot_flip_macro_direction():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    official = envelope(
        cutoff,
        contract_id="frozen-official-thesis-contract-v1",
        cohort_id="official-cohort-001",
        records=[
            {
                "thesis_id": "thesis-eurusd",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:45:00+00:00",
                "direction": "sell",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "expected_absolute_move_bps": 8.0,
                "uncertainty_bps": 2.0,
                "source_ids": ["official:EUR:policy", "official:USD:macro"],
                "source_fact_ids": ["official:EUR:policy", "official:USD:macro"],
                "independent_factor_ids": ["currency:EUR:short"],
            }
        ],
    )
    technical = envelope(
        cutoff,
        contract_id="technical-state-contract-v1",
        cohort_id="technical-cohort-001",
        records=[
            {
                "technical_state_id": "technical-eurusd",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:55:00+00:00",
                "technical_direction": "buy",
                "timing_state": "ready",
                "uncertainty_bps": 1.0,
                "source_ids": ["currency-state-fixture"],
            }
        ],
    )
    result = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        official_theses=official,
        technical_states=technical,
    )
    assert find(result, "official_context_only")["paper_action"] == "sell"
    assert find(result, "aligned")["paper_action"] == "abstain"
    conflict = find(result, "direction_conflicted_aggressive_paper")
    assert conflict["paper_action"] == "sell"
    assert conflict["official_direction"] == "sell"
    assert conflict["technical_direction"] == "buy"
    assert conflict["direction_origin"] == "frozen_official_thesis"
    assert conflict["lineage"]["official_thesis"]["basis_eligible_for_after_cost"] is True
    assert conflict["lineage"]["official_thesis"]["proof_eligible_lineage"] is True
    verifier = find(result, "technical_timing_verification")
    assert verifier["technical_relation"] == "conflicted"
    assert verifier["paper_action"] == "abstain"

    macro_arms = (
        "official_context_only",
        "aligned",
        "direction_conflicted_aggressive_paper",
        "delayed_reconfirmed",
    )
    for arm_id in macro_arms:
        row = find(result, arm_id)
        if row["paper_candidate"]:
            assert row["research_direction"] == "sell"


def test_aligned_and_delayed_arms_retain_macro_side_after_reconfirmation():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    official = envelope(
        cutoff,
        contract_id="official-v1",
        cohort_id="official-cohort",
        records=[
            {
                "thesis_id": "t1",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:40:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "source_ids": ["official-a"],
                "source_fact_ids": ["official:EUR:policy"],
                "independent_factor_ids": ["currency:EUR:long"],
            }
        ],
    )
    technical = envelope(
        cutoff,
        contract_id="technical-v1",
        cohort_id="technical-cohort",
        records=[
            {
                "technical_state_id": "v1",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:55:00+00:00",
                "reconfirmed_at_utc": "2026-01-01T00:50:00+00:00",
                "technical_direction": "buy",
                "timing_state": "reconfirmed",
                "source_ids": ["technical-a"],
            }
        ],
    )
    result = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        official_theses=official,
        technical_states=technical,
    )
    aligned = find(result, "aligned")
    delayed = find(result, "delayed_reconfirmed")
    assert aligned["paper_action"] == "buy"
    assert delayed["paper_action"] == "buy"
    assert delayed["reconfirmation_delay_sec"] == 600.0
    assert find(result, "direction_conflicted_aggressive_paper")["paper_action"] == "abstain"


def test_magnitude_arm_is_directionless_and_cost_diagnostic_only():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    magnitude = envelope(
        cutoff,
        contract_id="magnitude-v1",
        cohort_id="magnitude-cohort-9",
        records=[
            {
                "magnitude_estimate_id": "m1",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:50:00+00:00",
                "expected_absolute_move_bps": 20.0,
                "uncertainty_bps": 5.0,
                "source_ids": ["magnitude-source"],
            }
        ],
    )
    result = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        magnitude_estimates=magnitude,
    )
    row = find(result, "magnitude_only")
    assert row["expected_absolute_move_bps"] == 20.0
    assert row["expected_signed_move_bps"] is None
    assert row["research_direction"] is None
    assert row["paper_action"] == "abstain"
    assert row["forward_hypothesis"] is True
    assert row["cost_clear_probability"] is None
    assert row["magnitude_vs_observed_spread"] == "clears_observed_spread"
    assert row["lineage"]["magnitude_estimate"]["cohort_id"] == "magnitude-cohort-9"
    assert row["lineage"]["magnitude_estimate"]["source_ids"] == ["magnitude-source"]


def test_future_records_are_rejected_without_losing_grid_cells():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    future = envelope(
        cutoff,
        contract_id="official-v1",
        cohort_id="future-cohort",
        records=[
            {
                "thesis_id": "future-thesis",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T02:00:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "source_ids": ["future-source"],
                "source_fact_ids": ["official:EUR:policy"],
                "independent_factor_ids": ["currency:EUR:long"],
            }
        ],
    )
    result = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        official_theses=future,
    )
    assert result["edge_horizon_count"] == 340
    assert result["arms"]["official_context_only"]["record_count"] == 340
    assert find(result, "official_context_only")["paper_action"] == "abstain"
    assert result["input_rejections"] == [
        {
            "kind": "official_thesis",
            "record_id": "future-thesis",
            "reason": "known_after_decision_cutoff",
        }
    ]


def test_official_theses_require_freeze_time_fact_grounding_and_pair_factor():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    payload = envelope(
        cutoff,
        contract_id="official-grounding-v1",
        cohort_id="official-grounding-cohort",
        records=[
            {
                "thesis_id": "after-freeze",
                "instrument": "AUD_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:50:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "source_ids": ["official-aud"],
                "source_fact_ids": ["official:EUR:policy"],
                "independent_factor_ids": ["currency:AUD:long"],
            },
            {
                "thesis_id": "wrong-factor-side",
                "instrument": "GBP_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:30:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "source_ids": ["official-gbp"],
                "source_fact_ids": ["official:EUR:policy"],
                "independent_factor_ids": ["currency:GBP:short"],
            },
            {
                "thesis_id": "unknown-fact",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:30:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "source_ids": ["official-unknown"],
                "source_fact_ids": ["official:not-in-snapshot"],
                "independent_factor_ids": ["currency:EUR:long"],
            },
        ],
    )
    payload["frozen_at_utc"] = "2026-01-01T00:40:00+00:00"

    result = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        official_theses=payload,
    )

    assert {
        (row["record_id"], row["reason"]) for row in result["input_rejections"]
    } == {
        ("after-freeze", "record_known_after_envelope_freeze"),
        ("wrong-factor-side", "missing_or_pair_inconsistent_signed_currency_factor"),
        ("unknown-fact", "source_fact_not_present_in_context_snapshot"),
    }
    assert find(result, "official_context_only")["paper_action"] == "abstain"


def test_direction_basis_requires_eligible_facts_pair_legs_and_semantic_lineage():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    payload = envelope(
        cutoff,
        contract_id="basis-grounding-v1",
        cohort_id="basis-grounding-cohort",
        records=[
            {
                "thesis_id": "numeric-without-consensus",
                "instrument": "EUR_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:50:00+00:00",
                "direction": "buy",
                "direction_basis": "causal_numeric_surprise",
                "source_ids": ["official-usd-macro"],
                "source_fact_ids": ["official:USD:macro"],
                "independent_factor_ids": ["currency:EUR:long"],
            },
            {
                "thesis_id": "wrong-pair-currency",
                "instrument": "AUD_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:50:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                **SEMANTIC_LINEAGE,
                "source_ids": ["official-eur-policy"],
                "source_fact_ids": ["official:EUR:policy"],
                "independent_factor_ids": ["currency:AUD:long"],
            },
            {
                "thesis_id": "unversioned-semantic",
                "instrument": "GBP_USD",
                "horizon_sec": 3600,
                "known_at_utc": "2026-01-01T00:50:00+00:00",
                "direction": "buy",
                "direction_basis": "versioned_semantic_thesis",
                "source_ids": ["official-usd-macro"],
                "source_fact_ids": ["official:USD:macro"],
                "independent_factor_ids": ["currency:GBP:long"],
            },
        ],
    )

    result = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        official_theses=payload,
    )
    assert {
        (row["record_id"], row["reason"]) for row in result["input_rejections"]
    } == {
        ("numeric-without-consensus", "source_fact_not_eligible_for_direction_basis"),
        ("wrong-pair-currency", "source_fact_currency_not_a_pair_leg"),
        ("unversioned-semantic", "missing_versioned_semantic_lineage"),
    }


def test_input_order_does_not_change_snapshot_identity():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    records = [
        {
            "technical_state_id": "b",
            "instrument": "GBP_USD",
            "horizon_sec": 900,
            "known_at_utc": "2026-01-01T00:50:00+00:00",
            "technical_direction": "neutral",
            "timing_state": "wait",
            "source_ids": ["z", "a"],
        },
        {
            "technical_state_id": "a",
            "instrument": "EUR_USD",
            "horizon_sec": 3600,
            "known_at_utc": "2026-01-01T00:50:00+00:00",
            "technical_direction": "sell",
            "timing_state": "ready",
            "source_ids": ["b"],
        },
    ]
    first_payload = envelope(
        cutoff, contract_id="technical-v1", cohort_id="technical-order", records=records
    )
    second_payload = copy.deepcopy(first_payload)
    second_payload["records"].reverse()
    first = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        technical_states=first_payload,
    )
    second = build_response_timing_arms(
        source,
        state_contract=state_contract,
        arm_contract=arm_contract(),
        technical_states=second_payload,
    )
    assert first["snapshot_id"] == second["snapshot_id"]
    assert first["arms"] == second["arms"]


def test_bad_cutoff_or_execution_capability_fails_closed():
    state_contract, source = official_context_snapshot()
    payload = envelope(
        "2026-01-01T00:10:00+00:00",
        contract_id="official-v1",
        cohort_id="bad-cutoff",
        records=[],
    )
    with pytest.raises(ResponseTimingArmError, match="cutoff must equal"):
        build_response_timing_arms(
            source,
            state_contract=state_contract,
            arm_contract=arm_contract(),
            official_theses=payload,
        )

    payload["decision_cutoff_utc"] = source["decision_cutoff_utc"]
    payload["execution_eligible"] = True
    with pytest.raises(ResponseTimingArmError, match="cannot be execution eligible"):
        build_response_timing_arms(
            source,
            state_contract=state_contract,
            arm_contract=arm_contract(),
            official_theses=payload,
        )


def test_inexact_observed_horizon_abstains_in_price_only_arm():
    state_contract, source = official_context_snapshot()
    mutated = copy.deepcopy(source)
    mutated["horizons"]["3600"]["pair_edges"]["EUR_USD"]["exact_horizon_observation"] = False
    result = build_response_timing_arms(
        mutated, state_contract=state_contract, arm_contract=arm_contract()
    )
    row = find(result, "observed_price_only")
    assert row["paper_action"] == "abstain"
    assert row["reasons"] == ["inexact_horizon_observation"]


def test_duplicate_record_for_same_cell_is_rejected_as_ambiguous():
    state_contract, source = official_context_snapshot()
    cutoff = source["decision_cutoff_utc"]
    record = {
        "technical_state_id": "one",
        "instrument": "EUR_USD",
        "horizon_sec": 3600,
        "known_at_utc": "2026-01-01T00:50:00+00:00",
        "technical_direction": "buy",
        "timing_state": "ready",
    }
    duplicate = dict(record, technical_state_id="two")
    payload = envelope(
        cutoff,
        contract_id="technical-v1",
        cohort_id="ambiguous",
        records=[record, duplicate],
    )
    with pytest.raises(ResponseTimingArmError, match="duplicate technical_state"):
        build_response_timing_arms(
            source,
            state_contract=state_contract,
            arm_contract=arm_contract(),
            technical_states=payload,
        )
