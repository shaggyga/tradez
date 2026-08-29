from __future__ import annotations

import copy
import math

import pytest

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.features.currency_state_engine import build_currency_state_snapshot
    from forex_system.features.currency_state_official_context import (
        attach_official_fact_context,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.features.currency_state_engine import build_currency_state_snapshot
    from src.forex_system.features.currency_state_official_context import (
        attach_official_fact_context,
    )


def base_snapshot():
    contract = load_contract()
    states = {"EUR": 4.0, "GBP": 2.0, "USD": -2.0, "AUD": -3.0, "NZD": -1.0}
    instruments = (
        "EUR_USD", "GBP_USD", "EUR_GBP", "AUD_USD", "EUR_AUD",
        "GBP_AUD", "NZD_USD", "EUR_NZD", "GBP_NZD", "AUD_NZD",
    )
    start = 1767225600
    rows = []
    for minute in range(61):
        for instrument in instruments:
            base, quote = instrument.split("_")
            move_bps = (states[base] - states[quote]) * minute / 60.0
            mid = math.exp(move_bps / 10_000.0)
            rows.append(
                {
                    "instrument": instrument,
                    "minute_epoch": start + minute * 60,
                    "last_epoch": start + minute * 60 + 2,
                    "close_bid": mid - 0.00005,
                    "close_ask": mid + 0.00005,
                    "pip": 0.0001,
                }
            )
    snapshot = build_currency_state_snapshot(
        {"generated_utc": "2026-01-01T01:01:00+00:00", "rows": rows},
        contract=contract,
    )
    return contract, snapshot


def official_snapshot(cutoff: str):
    currencies = load_contract()["currencies"]
    facts = [
        {
            "fact_id": "macro:AUD",
            "currency": "AUD",
            "fact_type": "official_macro_actual",
            "evidence_class": "prospective_causal",
            "effective_from_utc": "2026-01-01T00:30:00+00:00",
            "consensus_causal": False,
            "direction_policy": "abstain",
            "direction": "strengthen",
        },
        {
            "fact_id": "policy:AUD",
            "currency": "AUD",
            "fact_type": "official_policy_document_context",
            "evidence_class": "policy_context_only",
            "effective_from_utc": "2026-01-01T00:15:00+00:00",
            "consensus_causal": False,
            "direction_policy": "abstain",
        },
    ]
    evidence = {
        currency: {
            "source_health": {"state": "fixture"},
            "missing_or_degraded": ["no_causal_pre_release_consensus"],
        }
        for currency in currencies
    }
    return {
        "snapshot_id": "official-fixture",
        "adapter_contract_id": "official-fixture-v1",
        "decision_cutoff_utc": cutoff,
        "fact_count": len(facts),
        "upcoming_event_count": 1,
        "causal_consensus_count": 0,
        "facts": facts,
        "upcoming_events": [
            {"event_id": "future:AUD", "currency": "AUD", "scheduled_utc": "2026-01-02T00:00:00+00:00"}
        ],
        "currency_evidence": evidence,
        "global_gaps": [{"code": "no_causal_pre_release_consensus"}],
        "intraday_rates": {"connected": False},
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }


def test_official_facts_attach_as_unscored_context_only():
    contract, base = base_snapshot()
    official = official_snapshot(base["decision_cutoff_utc"])
    combined = attach_official_fact_context(base, official, contract=contract)

    aud = combined["horizons"]["3600"]["currencies"]["AUD"]
    components = {row["component_id"]: row for row in aud["components"]}
    assert components["structured_official_fact"]["state"] == "available_unscored"
    assert components["policy_statement_delta"]["state"] == "context_available_unscored"
    assert components["structured_official_fact"]["forecast_mean_bps"] is None
    assert components["intraday_rate_repricing"]["state"] == "unavailable"
    assert aud["official_fact_context"]["direction_policy"] == "abstain"
    assert combined["component_context"]["basis_eligible_fact_counts"] == {
        "causal_numeric_surprise": 0,
        "causal_rate_repricing": 0,
        "frozen_policy_statement_delta": 0,
        "versioned_semantic_thesis": 0,
    }

    for horizon in combined["horizons"].values():
        for edge in horizon["pair_edges"].values():
            assert edge["forecast_mean_bps"] is None
            assert edge["expected_net_pips"] is None
            assert edge["execution_eligible"] is False
    assert combined["supported_execution_decision"] == "no_trade"


def test_numeric_surprise_eligibility_requires_consensus_and_frozen_scale_clocks():
    contract, base = base_snapshot()
    official = official_snapshot(base["decision_cutoff_utc"])
    macro = official["facts"][0]
    macro.update(
        {
            "source_id": "official-aud-stats",
            "raw_payload_sha256": "e" * 64,
            "scheduled_utc": "2026-01-01T00:29:00+00:00",
            "actual_known_utc": "2026-01-01T00:30:00+00:00",
            "actual_value": 3.2,
            "consensus_causal": True,
            "consensus_value": 3.0,
            "standardized_surprise": 0.5,
            "standardized_surprise_known_utc": "2026-01-01T00:30:00+00:00",
            "surprise_scale_known_utc": "2026-01-01T00:20:00+00:00",
            "degradation_reasons": [],
        }
    )
    official["causal_consensus_count"] = 1
    combined = attach_official_fact_context(base, official, contract=contract)
    metadata = combined["component_context"]["fact_metadata_by_id"]["macro:AUD"]
    assert metadata["basis_eligibility"]["causal_numeric_surprise"] is True
    assert metadata["basis_eligibility"]["versioned_semantic_thesis"] is True
    assert combined["component_context"]["basis_eligible_fact_counts"][
        "causal_numeric_surprise"
    ] == 1


def test_missing_currency_fact_stays_unavailable_not_neutral():
    contract, base = base_snapshot()
    official = official_snapshot(base["decision_cutoff_utc"])
    combined = attach_official_fact_context(base, official, contract=contract)
    jpy = combined["horizons"]["300"]["currencies"]["JPY"]
    components = {row["component_id"]: row for row in jpy["components"]}
    assert components["structured_official_fact"]["state"] == "unavailable"
    assert components["structured_official_fact"]["fact_count"] == 0
    assert components["structured_official_fact"]["forecast_mean_bps"] is None


def test_fact_order_cannot_change_context_snapshot_identity():
    contract, base = base_snapshot()
    official = official_snapshot(base["decision_cutoff_utc"])
    first = attach_official_fact_context(base, official, contract=contract)
    shuffled = copy.deepcopy(official)
    shuffled["facts"].reverse()
    shuffled["upcoming_events"].reverse()
    second = attach_official_fact_context(base, shuffled, contract=contract)
    assert first["snapshot_id"] == second["snapshot_id"]
    assert first["component_context"] == second["component_context"]


def test_cutoff_mismatch_fails_closed():
    contract, base = base_snapshot()
    official = official_snapshot("2026-01-01T01:02:00+00:00")
    with pytest.raises(ValueError, match="cutoffs must match"):
        attach_official_fact_context(base, official, contract=contract)
