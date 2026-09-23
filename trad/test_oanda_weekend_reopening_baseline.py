"""Synthetic clocks and prices only; no market accuracy is established here."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json

import pytest

from oanda_exact_price_scoring import score_prediction
from oanda_weekend_reopening_baseline import (
    MODEL_VERSION, PARAMETERS, forecast_weekend_reopening, to_jsonable,
)


REOPEN = int(datetime(2026, 9, 6, 21, tzinfo=timezone.utc).timestamp())
CLOSE = REOPEN - 172800


@pytest.fixture
def inputs():
    return {
        "session": {"session_id": "fixture_weekend_20260906", "instrument": "EUR_USD",
            "friday_close_epoch": CLOSE, "reopen_epoch": REOPEN,
            "calendar_observed_epoch": CLOSE - 86400,
            "calendar_source_id": "synthetic_calendar", "calendar_source_sha256": "a" * 64},
        "friday_anchor": {"instrument": "EUR_USD", "market_epoch": CLOSE - 1,
            "observed_epoch": CLOSE + 1, "bid": "1.09995", "ask": "1.10005",
            "source_id": "synthetic_friday_quote", "source_sha256": "b" * 64,
            "anchor_kind": "quote", "is_final_preclose_anchor": True,
            "tradeable": True, "retained_last_known": False},
        "opening_quote": {"instrument": "EUR_USD", "market_epoch": REOPEN + 1,
            "observed_epoch": REOPEN + 2, "bid": "1.10095", "ask": "1.10105",
            "source_id": "synthetic_opening_quote", "source_sha256": "c" * 64,
            "is_first_observed_tradeable_quote": True, "tradeable": True,
            "retained_last_known": False},
        "decision_epoch": REOPEN + 3,
    }


def run(inputs):
    return forecast_weekend_reopening(**inputs)


def abstains(result, reason):
    assert result["status"] == "abstain"
    assert any(reason in entry for entry in result["reasons"]), result["reasons"]
    assert all(arm["direction"] == 0 and arm["status"] == "abstain" for arm in result["arms"].values())
    assert all(result[name] is False for name in (
        "collection_enabled", "can_place_orders", "can_authorize", "can_promote",
        "account_eligible", "proof_eligible", "runtime_started", "historical_accuracy_established"))


def test_immediate_three_hypotheses_need_only_two_prices(inputs):
    result = run(inputs)
    assert result["status"] == "hypotheses"
    assert result["model_version"] == MODEL_VERSION
    assert result["gap_price"] == Decimal(".001")
    assert result["opening_spread_price"] == Decimal(".0001")
    assert {name: arm["direction"] for name, arm in result["arms"].items()} == {
        "gap_fade": -1, "gap_continuation": 1, "no_trade": 0}
    assert result["reference_epoch"] == REOPEN + 1
    assert result["target_epoch"] == REOPEN + 3601
    assert all(arm["probability_up"] is None and arm["expected_return_bps"] is None
               for arm in result["arms"].values())
    assert result["independent_sample_size"] is None
    assert result["research_only"] is True
    for flag in ("can_place_orders", "can_authorize", "can_promote", "account_eligible",
                 "proof_eligible", "runtime_started", "collection_enabled"):
        assert result[flag] is False


def test_down_gap_reverses_both_sides(inputs):
    inputs["opening_quote"].update(bid="1.09895", ask="1.09905")
    result = run(inputs)
    assert result["arms"]["gap_fade"]["direction"] == 1
    assert result["arms"]["gap_continuation"]["direction"] == -1


@pytest.mark.parametrize("gap", ["0", ".0001", ".0002", "-.0002"])
def test_flat_and_gap_at_or_inside_two_spreads_abstain(inputs, gap):
    midpoint = Decimal("1.1") + Decimal(gap)
    inputs["opening_quote"].update(bid=str(midpoint - Decimal(".00005")), ask=str(midpoint + Decimal(".00005")))
    abstains(run(inputs), "gap_not_above_two_opening_spreads")


def test_tiny_real_price_difference_above_gap_threshold_is_not_rounded_away(inputs):
    with localcontext() as ctx:
        ctx.prec = 160
        midpoint = Decimal("1.1002") + Decimal("1e-100")
        inputs["opening_quote"].update(bid=str(midpoint - Decimal(".00005")), ask=str(midpoint + Decimal(".00005")))
    result = run(inputs)
    assert result["status"] == "hypotheses"
    assert result["gap_price"] > Decimal(".0002")


@pytest.mark.parametrize("extra,expected", [("0", "hypotheses"), ("1e-100", "abstain")])
def test_spread_cap_uses_exact_boundary(inputs, extra, expected):
    with localcontext() as ctx:
        ctx.prec = 160
        inputs["opening_quote"].update(bid="1.1009", ask=str(Decimal("1.1011") + Decimal(extra)))
    result = run(inputs)
    assert result["status"] == expected
    if expected == "abstain":
        abstains(result, "opening_spread_exceeds_fixed_limit")


@pytest.mark.parametrize("record", ["friday_anchor", "opening_quote"])
@pytest.mark.parametrize("field,value", [
    ("bid", 1.1), ("ask", True), ("bid", "NaN"), ("ask", "Infinity"),
    ("bid", "-1"), ("bid", "0"), ("bid", "1.2"), ("ask", None),
    ("instrument", "GBP_USD"), ("source_id", ""), ("source_sha256", "fake"),
])
def test_malformed_price_or_identity_abstains(inputs, record, field, value):
    inputs[record][field] = value
    abstains(run(inputs), record)


@pytest.mark.parametrize("field,value", [
    ("is_first_observed_tradeable_quote", None), ("is_first_observed_tradeable_quote", False),
    ("is_first_observed_tradeable_quote", 1), ("tradeable", False),
    ("retained_last_known", True), ("retained_last_known", None),
])
def test_unknown_or_nontradeable_opening_abstains(inputs, field, value):
    inputs["opening_quote"][field] = value
    abstains(run(inputs), "opening")


def test_missing_opening_never_substitutes_current_monday_quote(inputs):
    inputs["opening_quote"] = None
    abstains(run(inputs), "unknown_opening_reference")


@pytest.mark.parametrize("field,value", [
    ("is_final_preclose_anchor", False), ("anchor_kind", "unknown"),
    ("tradeable", False), ("retained_last_known", True),
])
def test_unknown_friday_anchor_abstains(inputs, field, value):
    inputs["friday_anchor"][field] = value
    abstains(run(inputs), "friday_anchor")


def test_completed_m1_anchor_uses_bar_close_boundary(inputs):
    inputs["friday_anchor"].update(anchor_kind="completed_m1", bar_start_epoch=CLOSE - 60,
                                  market_epoch=CLOSE, complete=True)
    assert run(inputs)["status"] == "hypotheses"


@pytest.mark.parametrize("start,complete", [(CLOSE, True), (CLOSE - 61, True), (CLOSE - 60, False), (None, True)])
def test_partial_malformed_or_start_stamped_m1_anchor_rejected(inputs, start, complete):
    inputs["friday_anchor"].update(anchor_kind="completed_m1", bar_start_epoch=start,
                                  market_epoch=CLOSE, complete=complete)
    abstains(run(inputs), "friday_anchor")


@pytest.mark.parametrize("record", ["friday_anchor", "opening_quote"])
@pytest.mark.parametrize("offset", [0, 1])
def test_observation_equal_to_or_after_decision_is_not_available(inputs, record, offset):
    inputs[record]["observed_epoch"] = inputs["decision_epoch"] + offset
    abstains(run(inputs), "price_observation_not_strictly_before_decision")


@pytest.mark.parametrize("record", ["friday_anchor", "opening_quote"])
def test_observation_before_price_market_timestamp_abstains(inputs, record):
    inputs[record]["observed_epoch"] = inputs[record]["market_epoch"] - 1
    abstains(run(inputs), "price_observation_not_strictly_before_decision")


@pytest.mark.parametrize("value", [None, True, "NaN", float("inf"), -1, "32503680000"])
def test_invalid_decision_clock_abstains(inputs, value):
    inputs["decision_epoch"] = value
    abstains(run(inputs), "decision_epoch")


@pytest.mark.parametrize("offset,expected", [(60, "hypotheses"), (61, "abstain")])
def test_quote_freshness_counts_market_to_decision_elapsed_time(inputs, offset, expected):
    inputs["decision_epoch"] = REOPEN + 1 + offset
    result = run(inputs)
    assert result["status"] == expected
    if expected == "abstain":
        abstains(result, "opening_quote_stale_at_decision")


def test_fresh_observation_of_old_quote_does_not_restore_freshness(inputs):
    inputs["opening_quote"]["observed_epoch"] = REOPEN + 89
    inputs["decision_epoch"] = REOPEN + 90
    abstains(run(inputs), "opening_quote_stale_at_decision")


@pytest.mark.parametrize("offset,expected", [(300, "hypotheses"), (301, "abstain")])
def test_initial_window_anchored_to_calendar_reopen_not_capture(inputs, offset, expected):
    inputs["opening_quote"].update(market_epoch=REOPEN + 250, observed_epoch=REOPEN + 251)
    inputs["decision_epoch"] = REOPEN + offset
    result = run(inputs)
    assert result["status"] == expected
    if expected == "abstain":
        abstains(result, "late_initial_decision")


@pytest.mark.parametrize("offset", [-1, 301])
def test_opening_reference_must_be_within_initial_session_window(inputs, offset):
    inputs["opening_quote"].update(market_epoch=REOPEN + offset, observed_epoch=REOPEN + offset + 1)
    inputs["decision_epoch"] = REOPEN + offset + 2
    abstains(run(inputs), "opening_reference_outside_initial_window")


def test_elapsed_original_target_is_retained_and_never_shifted(inputs):
    inputs["decision_epoch"] = REOPEN + 3601
    result = run(inputs)
    abstains(result, "original_target_elapsed")
    assert result["target_epoch"] == REOPEN + 3601


def test_monday_replay_cannot_emit_sunday_opening_forecast(inputs):
    inputs["decision_epoch"] = REOPEN + 86400
    abstains(run(inputs), "original_target_elapsed")


@pytest.mark.parametrize("offset,expected", [(0, "hypotheses"), (-300, "hypotheses"), (-301, "abstain"), (1, "abstain")])
def test_friday_anchor_window(inputs, offset, expected):
    inputs["friday_anchor"].update(market_epoch=CLOSE + offset, observed_epoch=CLOSE + offset + 1)
    result = run(inputs)
    assert result["status"] == expected
    if expected == "abstain":
        abstains(result, "friday_anchor_outside_close_window")


@pytest.mark.parametrize("field,value", [
    ("calendar_observed_epoch", REOPEN), ("calendar_observed_epoch", REOPEN + 3),
    ("calendar_source_id", ""), ("calendar_source_sha256", ""),
    ("session_id", ""), ("instrument", "USD_JPY"),
    ("friday_close_epoch", CLOSE - 86400), ("reopen_epoch", REOPEN - 86400),
    ("reopen_epoch", REOPEN + 7 * 86400),
])
def test_unknown_late_or_nonweekend_calendar_abstains(inputs, field, value):
    inputs["session"][field] = value
    abstains(run(inputs), "calendar" if "observed" in field or "epoch" in field else "session")


def test_decision_timestamp_does_not_modify_original_target(inputs):
    first = run(inputs)
    inputs["decision_epoch"] += 10
    second = run(inputs)
    assert first["target_epoch"] == second["target_epoch"]
    assert first["event_id"] == second["event_id"]
    assert first["forecast_batch_id"] != second["forecast_batch_id"]


def test_event_identity_deduplicates_session_aliases_and_clock_spellings(inputs):
    first = run(inputs)
    inputs["session"].update(session_id="another_label", friday_close_epoch=str(CLOSE) + ".000", reopen_epoch=Decimal(REOPEN))
    assert run(inputs)["event_id"] == first["event_id"]


def test_capture_hash_binds_source_and_parameter_identity(inputs):
    first = run(inputs)
    inputs["friday_anchor"]["source_sha256"] = "d" * 64
    second = run(inputs)
    assert first["input_capture_sha256"] != second["input_capture_sha256"]
    assert first["forecast_batch_id"] != second["forecast_batch_id"]
    assert first["parameters_sha256"] == second["parameters_sha256"]


def test_inputs_and_policy_are_not_mutated_or_shared(inputs):
    original = deepcopy(inputs)
    first = run(inputs)
    assert inputs == original
    first["parameters"]["horizon_sec"] = 1
    first["parameters"]["arms"].append("other")
    first["input_snapshot"]["opening_quote"]["bid"] = Decimal("100")
    assert run(inputs)["parameters"]["horizon_sec"] == 3600
    assert len(run(inputs)["parameters"]["arms"]) == 3
    assert inputs == original
    with pytest.raises(TypeError):
        PARAMETERS["horizon_sec"] = 1


def test_decimal_context_does_not_change_results_and_json_has_no_floats(inputs):
    first = run(inputs)
    with localcontext() as ctx:
        ctx.prec = 2
        assert run(inputs) == first
    encoded = json.dumps(to_jsonable(first), allow_nan=False)
    decoded = json.loads(encoded)
    assert decoded["gap_price"] == "0.00100"
    assert decoded["decision_epoch"] == str(REOPEN + 3)


def test_exact_scorer_can_compare_executable_arms_without_probability_claim(inputs):
    hypotheses = run(inputs)
    # Synthetic independent entry after decision; target at original +3600.
    entry = {"bid": "1.10094", "ask": "1.10106"}
    target = {"bid": "1.09995", "ask": "1.10005"}
    scores = {name: score_prediction(inputs["opening_quote"], target,
        entry_quote=entry, direction=arm["direction"], probability_up=".5",
        pip_size=".0001", extra_cost_stress_bps=["0", ".5", "1"])
        for name, arm in hypotheses["arms"].items()}
    # .5 is a scorer placeholder only; no Brier or confidence is claimed.
    assert scores["gap_fade"]["net_price_move"] == Decimal(".00089")
    assert scores["gap_continuation"]["net_price_move"] == Decimal("-.00111")
    assert scores["no_trade"]["net_price_move"] == 0
    assert all(value == 0 for value in scores["no_trade"]["stress_net_bps"].values())
    assert all(arm["probability_up"] is None for arm in hypotheses["arms"].values())
