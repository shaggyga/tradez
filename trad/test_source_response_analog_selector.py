import ast
import json
from copy import deepcopy
from pathlib import Path
import random

import pytest

from src.forex_system.research.source_response_analog_selector import (
    AnalogSelectorError,
    attach_known_outcomes,
    compute_distance,
    normalize_point_in_time_event,
    select_analogs,
    verify_frozen_selection,
)


ROOT = Path(__file__).resolve().parent
CONTRACT = json.loads(
    (ROOT / "config" / "source_response_analog_selector_v1.json").read_text(encoding="utf-8")
)
CUTOFF = "2026-08-16T12:01:00Z"


def event(event_id, event_time, *, surprise=1.0, **overrides):
    row = {
        "event_id": event_id,
        "event_time_utc": event_time,
        "feature_known_utc": event_time,
        "scheduled_release_time_utc": event_time,
        "currency": "JPY",
        "event_series_id": "boj_policy_rate",
        "event_class": "central_bank_decision",
        "source_family": "official_central_bank",
        "policy_regime": "tightening",
        "session": "asia",
        "liquidity_bucket": "liquid",
        "consensus_value": 0.5,
        "consensus_observed_at_utc": "2026-01-01T00:00:00Z",
        "consensus_source_type": "market_consensus",
        "market_consensus": True,
        "initial_actual_value": 0.75,
        "actual_known_utc": event_time,
        "standardized_surprise": surprise,
        "standardized_surprise_known_utc": event_time,
        "surprise_scale_known_utc": "2025-12-31T00:00:00Z",
        "rates_state": "intraday_causal",
        "rates_known_utc": event_time,
        "rates_repricing_bps": 4.0,
        "statement_delta_score": 0.6,
        "pre_event_currency_return_bps": 1.0,
        "pre_event_volatility_percentile": 0.5,
        "spread_percentile": 0.4,
        "liquidity_percentile": 0.7,
    }
    row.update(overrides)
    return row


def query():
    return event("query", "2026-08-16T12:00:00Z", surprise=1.2)


def candidates():
    return [
        event("near", "2026-07-31T03:00:00Z", surprise=1.1),
        event(
            "far",
            "2026-06-15T03:00:00Z",
            surprise=-1.0,
            policy_regime="easing",
            session="new_york",
            liquidity_bucket="impaired",
            rates_repricing_bps=-8.0,
        ),
    ]


def test_outcomes_cannot_change_ranking_query_hash_or_selection_hash():
    rows = candidates()
    rows[0]["after_cost_pips"] = -99999
    rows[0]["outcomes"] = [{"matured_utc": "2026-08-01T00:00:00Z", "after_cost_pips": -99}]
    rows[1]["after_cost_pips"] = 99999
    first = select_analogs(query(), rows, decision_cutoff_utc=CUTOFF, contract=CONTRACT)

    mutated = deepcopy(rows)
    mutated[0]["after_cost_pips"] = 999999
    mutated[0]["outcomes"][0]["after_cost_pips"] = 999999
    mutated[1]["after_cost_pips"] = -999999
    mutated[1]["outcomes"] = [{"matured_utc": "2026-08-01T00:00:00Z", "after_cost_pips": -999}]
    second = select_analogs(query(), mutated, decision_cutoff_utc=CUTOFF, contract=CONTRACT)

    assert [row["event_id"] for row in first["selected"]] == ["near", "far"]
    assert first["query_feature_hash"] == second["query_feature_hash"]
    assert first["candidate_universe_hash"] == second["candidate_universe_hash"]
    assert first["selection_hash"] == second["selection_hash"]
    assert all("after_cost_pips" not in row["point_in_time_features"] for row in first["selected"])


def test_ingestion_order_and_ties_are_deterministic():
    rows = [
        event("tie_b", "2026-07-01T03:00:00Z"),
        event("tie_a", "2026-07-01T03:00:00Z"),
        event("different", "2026-07-02T03:00:00Z", surprise=-0.5),
    ]
    first = select_analogs(query(), rows, decision_cutoff_utc=CUTOFF, contract=CONTRACT)
    shuffled = deepcopy(rows)
    random.Random(7).shuffle(shuffled)
    second = select_analogs(query(), shuffled, decision_cutoff_utc=CUTOFF, contract=CONTRACT)

    assert [row["event_id"] for row in first["selected"]][:2] == ["tie_a", "tie_b"]
    assert first["selected"] == second["selected"]
    assert first["candidate_universe_hash"] == second["candidate_universe_hash"]
    assert first["selection_hash"] == second["selection_hash"]


def test_outcomes_attach_only_after_maturity_and_knowledge_by_cutoff():
    rows = candidates()
    rows[0]["outcomes"] = [
        {
            "horizon": "M15",
            "matured_utc": "2026-08-16T11:30:00Z",
            "outcome_known_utc": "2026-08-16T11:31:00Z",
            "after_cost_pips": 4.0,
        },
        {
            "horizon": "H1",
            "matured_utc": "2026-08-16T12:30:00Z",
            "outcome_known_utc": "2026-08-16T12:31:00Z",
            "after_cost_pips": 20.0,
        },
        {"horizon": "missing_time", "after_cost_pips": 1000.0},
    ]
    frozen = select_analogs(query(), rows, decision_cutoff_utc=CUTOFF, contract=CONTRACT)
    original_hash = frozen["selection_hash"]
    attached = attach_known_outcomes(
        frozen, rows, decision_cutoff_utc=CUTOFF, contract=CONTRACT
    )

    near = next(row for row in attached["selected"] if row["event_id"] == "near")
    assert [outcome["horizon"] for outcome in near["known_outcomes"]] == ["M15"]
    assert attached["known_outcome_count"] == 1
    assert attached["withheld_outcome_count"] == 2
    assert attached["selection_hash"] == original_hash
    assert verify_frozen_selection(frozen)
    assert verify_frozen_selection(attached)


def test_market_consensus_internal_expectation_and_missing_are_distinct():
    causal = normalize_point_in_time_event(
        event("causal", "2026-07-01T03:00:00Z"),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )
    internal = normalize_point_in_time_event(
        event(
            "internal",
            "2026-07-02T03:00:00Z",
            consensus_value=None,
            consensus_observed_at_utc=None,
            consensus_source_type=None,
            market_consensus=False,
            standardized_surprise=50.0,
            internal_expectation_value=0.6,
            internal_expectation_issued_at_utc="2026-07-01T02:00:00Z",
            internal_expectation_error_standardized=0.4,
            internal_expectation_error_known_utc="2026-07-02T03:00:00Z",
            internal_expectation_scale_known_utc="2026-07-01T01:00:00Z",
        ),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )
    post_release = normalize_point_in_time_event(
        event(
            "post",
            "2026-07-03T03:00:00Z",
            consensus_observed_at_utc="2026-07-03T03:00:01Z",
            standardized_surprise=99.0,
        ),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )

    assert causal["consensus_state"] == "causal_market_consensus"
    assert causal["standardized_surprise"] == 1.0
    assert internal["consensus_state"] == "internal_expectation_only"
    assert internal["standardized_surprise"] is None
    assert internal["internal_expectation_error_standardized"] == 0.4
    assert post_release["consensus_state"] == "missing"
    assert post_release["standardized_surprise"] is None


def test_surprise_requires_post_release_known_time_and_pre_release_scale():
    early_actual = normalize_point_in_time_event(
        event(
            "early_actual",
            "2026-07-04T03:00:00Z",
            actual_known_utc="2026-07-04T02:59:59Z",
        ),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )
    late_scale = normalize_point_in_time_event(
        event(
            "late_scale",
            "2026-07-05T03:00:00Z",
            surprise_scale_known_utc="2026-07-05T03:00:01Z",
        ),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )
    missing_surprise_clock = normalize_point_in_time_event(
        event(
            "missing_surprise_clock",
            "2026-07-06T03:00:00Z",
            standardized_surprise_known_utc=None,
        ),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )

    assert early_actual["actual_known_causally"] is False
    assert early_actual["standardized_surprise"] is None
    assert late_scale["standardized_surprise_causal"] is False
    assert late_scale["standardized_surprise"] is None
    assert missing_surprise_clock["standardized_surprise_causal"] is False
    assert missing_surprise_clock["standardized_surprise"] is None


def test_distance_exposes_currency_series_rates_session_and_liquidity_contributions():
    q = normalize_point_in_time_event(query(), decision_cutoff_utc=CUTOFF, contract=CONTRACT)
    candidate = normalize_point_in_time_event(
        event(
            "context",
            "2026-07-01T03:00:00Z",
            event_series_id="boj_statement",
            rates_repricing_bps=-1.0,
            session="london",
            liquidity_bucket="thin",
        ),
        decision_cutoff_utc=CUTOFF,
        contract=CONTRACT,
    )
    result = compute_distance(q, candidate, contract=CONTRACT)
    by_field = {row["field"]: row for row in result["contributions"]}

    assert result["eligible"] is True
    assert by_field["currency"]["state"] == "match"
    assert by_field["event_series_id"]["state"] == "mismatch"
    assert by_field["rates_repricing_bps"]["weighted_distance"] > 0
    assert by_field["session"]["state"] == "mismatch"
    assert by_field["liquidity_bucket"]["state"] == "mismatch"
    assert result["distance"] == pytest.approx(
        sum(row["weighted_distance"] for row in result["contributions"])
    )


def test_future_features_and_nonhistorical_events_are_not_selected():
    rows = [
        event(
            "future_known",
            "2026-07-01T03:00:00Z",
            feature_known_utc="2026-08-16T12:02:00Z",
        ),
        event("future_event", "2026-08-17T03:00:00Z"),
        event("valid", "2026-07-02T03:00:00Z"),
    ]
    selection = select_analogs(query(), rows, decision_cutoff_utc=CUTOFF, contract=CONTRACT)
    assert [row["event_id"] for row in selection["selected"]] == ["valid"]
    assert {row["reason"] for row in selection["rejections"]} == {
        "features_not_known_at_cutoff",
        "candidate_event_not_historical",
    }


def test_tampering_with_frozen_selection_is_detected():
    selection = select_analogs(query(), candidates(), decision_cutoff_utc=CUTOFF, contract=CONTRACT)
    tampered = deepcopy(selection)
    tampered["selected"][0]["distance"] = 0.0
    with pytest.raises(AnalogSelectorError, match="selection_hash"):
        verify_frozen_selection(tampered)

    query_tampered = deepcopy(selection)
    query_tampered["query_point_in_time_features"]["rates_repricing_bps"] = 999.0
    with pytest.raises(AnalogSelectorError, match="query_feature_hash"):
        verify_frozen_selection(query_tampered)


def test_safety_contract_is_fail_closed():
    selection = select_analogs(query(), candidates(), decision_cutoff_utc=CUTOFF, contract=CONTRACT)
    assert selection["research_only"] is True
    assert selection["execution_eligible"] is False
    assert selection["can_place_orders"] is False
    assert selection["supported_execution_decision"] == "no_trade"


def test_pure_module_has_no_io_model_or_execution_imports():
    module_path = (
        ROOT / "src" / "forex_system" / "research" / "source_response_analog_selector.py"
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots <= {
        "__future__",
        "copy",
        "datetime",
        "hashlib",
        "json",
        "math",
        "typing",
    }
