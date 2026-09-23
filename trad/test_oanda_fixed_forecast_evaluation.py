"""Adversarial offline fixtures for the frozen, four-family scorer."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

import oanda_fixed_forecast_evaluation as scorer


@pytest.fixture
def protocol():
    return json.loads((Path(__file__).parent / "config" / "fixed_forecast_evaluation_v1_20260906.json").read_text())


def quote(identity, market, available, mid=1.1, spread=.0002, **overrides):
    return {"quote_id": identity, "instrument": "EUR_USD", "tradeable": True,
            "market_epoch": market, "available_epoch": available,
            "bid": mid-spread/2, "ask": mid+spread/2, **overrides}


def add_decision(dataset, protocol, *, offset=0, target_mid=1.101, probability=.75, entry_mid=1.1):
    reference = scorer.epoch(protocol["historical_start_utc"]) + 1800 + offset
    identity = f"d{offset}"
    forecasts = []
    for index, (family, cohort) in enumerate(protocol["cohorts"].items()):
        forecasts.append({"forecast_id": f"{identity}-{family}", "family": family,
            "cohort_id": cohort, "instrument": protocol["instrument"],
            "horizon_sec": protocol["horizon_sec"], "input_timeframe": protocol["input_timeframe"],
            "model_version": protocol["model_version"], "feature_version": protocol["feature_version"],
            "issued_epoch": reference+2+index, "committed_available_epoch": reference+5+index,
            "feature_cutoff_epoch": reference-60, "features_available_epoch": reference,
            "training_label_maturity_max_epoch": reference-3600,
            "training_labels_available_max_epoch": reference-3500,
            "reference_epoch": reference, "target_epoch": reference+3600, "reference_mid": 1.1,
            "probability_up": probability, "side": scorer.side(probability), "predicted_return_bps": 5.0})
    decision = {"decision_id": identity, "reference_epoch": reference,
                "target_epoch": reference+3600, "reference_quote_id": f"{identity}-reference",
                "forecasts": forecasts}
    dataset["decisions"].append(decision)
    dataset["quotes"].extend([
        quote(f"{identity}-reference", reference, reference+1),
        quote(f"{identity}-entry", reference+12, reference+13, entry_mid),
        quote(f"{identity}-target", reference+3601, reference+3602, target_mid),
    ])
    dataset["observed_cutoff_epoch"] = max(dataset["observed_cutoff_epoch"], reference+7200)
    return decision


@pytest.fixture
def dataset(protocol):
    value = {"schema_version": scorer.SCHEMA, "observed_cutoff_epoch": 0, "decisions": [], "quotes": []}
    add_decision(value, protocol)
    return value


def assert_excluded(dataset, protocol, reason, forecast_reason=None):
    report = scorer.evaluate(dataset, protocol)
    assert report["status"] == "no_clock_valid_paired_decisions"
    assert report["coverage"]["paired_scored_decisions"] == 0
    assert report["decision_exclusion_counts"][reason] == 1
    if forecast_reason:
        errors = report["exclusions"][0]["forecast_errors"]
        assert any(forecast_reason in values for values in errors.values())
    return report


def test_valid_four_family_paired_metrics_costs_and_input_immutability(dataset, protocol):
    original = deepcopy(dataset)
    report = scorer.evaluate(dataset, protocol)
    assert dataset == original
    assert report["proof_eligible"] is report["account_eligible"] is report["runtime_started"] is False
    assert report["independent_sample_size"] is None
    assert report["coverage"] == {"total_decisions": 1, "total_forecasts": 4,
        "complete_four_family_decisions": 1, "complete_clock_valid_decisions": 1,
        "paired_scored_decisions": 1}
    row = report["decisions"][0]
    assert row["common_decision_epoch"] == dataset["decisions"][0]["reference_epoch"]+8
    assert row["entry"]["quote_id"] == "d0-entry"
    assert row["target"]["quote_id"] == "d0-target"
    assert row["actual_holding_sec"] == 3589
    actual = 10000*.001/1.1
    net = 10000*.0008/1.1
    assert row["actual_return_bps"] == pytest.approx(actual)
    for family in protocol["cohorts"]:
        score = row["scores"][family]
        assert score["brier"] == .0625
        assert score["net_bps"] == pytest.approx(net)
        assert score["stress_net_bps"]["1.0"] == pytest.approx(net-1)
        assert report["per_family_coverage"][family] == {"issued":1,"clock_valid":1,"paired_scored":1}
        delta = report["paired_deltas"][family]
        assert delta["mean_brier_minus_fair_coin"] == -.1875
        assert delta["mae_bps_minus_zero_move"] == pytest.approx(abs(5-actual)-abs(actual))
        assert delta["mean_net_bps_minus_no_trade"] == pytest.approx(net)
    assert report["paired_summaries"]["fair_coin"]["mean_brier"] == .25
    assert report["paired_summaries"]["zero_move"]["mae_bps"] == pytest.approx(actual)
    assert report["paired_summaries"]["no_trade"]["mean_net_bps_per_decision"] == 0
    assert report["paired_summaries"]["no_trade"]["mean_brier"] is None


def test_prediction_target_is_original_reference_while_execution_uses_later_entry(dataset, protocol):
    # A move before execution is part of the issued forecast target, but cannot
    # be claimed as executable profit. Delayed entry never moves the expiry.
    dataset["quotes"][1].update(bid=1.102-.0001, ask=1.102+.0001)
    result = scorer.evaluate(dataset, protocol)["decisions"][0]
    assert result["actual_return_bps"] > 0
    assert result["target_epoch"] == dataset["decisions"][0]["reference_epoch"]+3600
    for family in protocol["cohorts"]:
        assert result["scores"][family]["direction_correct"] is True
        assert result["scores"][family]["net_bps"] < 0


@pytest.mark.parametrize("field", ["issued_epoch", "committed_available_epoch", "feature_cutoff_epoch",
    "features_available_epoch", "training_label_maturity_max_epoch", "training_labels_available_max_epoch",
    "reference_epoch", "target_epoch"])
def test_missing_original_forecast_clocks_are_excluded(dataset, protocol, field):
    del dataset["decisions"][0]["forecasts"][0][field]
    assert_excluded(dataset, protocol, "forecast_clock_identity_or_value_invalid", "missing_or_invalid_clock:"+field)


@pytest.mark.parametrize("field,value", [("reference_epoch", None), ("target_epoch", None),
    ("reference_epoch", "not-a-clock"), ("target_epoch", "not-a-clock")])
def test_malformed_decision_clocks_are_reported_not_crashes(dataset, protocol, field, value):
    dataset["decisions"][0][field] = value
    assert_excluded(dataset, protocol, "missing_or_invalid_reference_target", "missing_or_invalid_decision_clock")


@pytest.mark.parametrize("field", ["reference_epoch", "target_epoch"])
def test_absent_decision_clocks_are_reported(dataset, protocol, field):
    del dataset["decisions"][0][field]
    assert_excluded(dataset, protocol, "missing_or_invalid_reference_target", "missing_or_invalid_decision_clock")


@pytest.mark.parametrize("field,relative,error", [
    ("committed_available_epoch", -1, "issue_publication_target_order"),
    ("committed_available_epoch", 3600, "issue_publication_target_order"),
    ("features_available_epoch", 3, "features_unavailable_at_issue"),
    ("feature_cutoff_epoch", 3, "features_unavailable_at_issue"),
    ("training_label_maturity_max_epoch", 2, "training_labels_unavailable_at_issue"),
    ("training_labels_available_max_epoch", 2, "training_labels_unavailable_at_issue"),
    ("target_epoch", 3601, "original_forecast_target_mismatch"),
])
def test_future_features_labels_and_changed_target_fail(dataset, protocol, field, relative, error):
    decision = dataset["decisions"][0]
    decision["forecasts"][0][field] = decision["reference_epoch"]+relative
    assert_excluded(dataset, protocol, "forecast_clock_identity_or_value_invalid", error)


def test_decision_target_cannot_shift_even_if_all_forecasts_are_changed(dataset, protocol):
    decision = dataset["decisions"][0]
    decision["target_epoch"] += 13
    for forecast in decision["forecasts"]:
        forecast["target_epoch"] += 13
    assert_excluded(dataset, protocol, "target_shifted_from_original_reference")


def test_missing_target_does_not_use_nearest_earlier_or_late_quote(dataset, protocol):
    reference = dataset["decisions"][0]["reference_epoch"]
    dataset["quotes"].pop()
    dataset["quotes"].extend([quote("early", reference+3599, reference+3600),
                              quote("late", reference+3661, reference+3662)])
    assert_excluded(dataset, protocol, "missing_original_target_quote")


@pytest.mark.parametrize("mutation,excluded", [
    ({"bid":-1}, "invalid_bid_ask"), ({"ask":0}, "invalid_bid_ask"),
    ({"bid":1.2,"ask":1.1}, "invalid_bid_ask"), ({"bid":float("inf")}, "nonfinite_value"),
    ({"bid":True}, "missing_or_non_numeric_value"), ({"instrument":"GBP_USD"}, "wrong_instrument_or_not_tradeable"),
    ({"tradeable":False}, "wrong_instrument_or_not_tradeable"),
])
def test_bad_quotes_do_not_reach_scores(dataset, protocol, mutation, excluded):
    dataset["quotes"][1].update(mutation)
    # Direct validation avoids serializing deliberately non-finite test input.
    quotes, counts = scorer.validate_quotes(dataset["quotes"], dataset["observed_cutoff_epoch"], protocol)
    assert "d0-entry" not in {q["quote_id"] for q in quotes}
    assert counts[excluded] == 1


@pytest.mark.parametrize("kind", ["stale", "future_market", "future_availability", "late_entry", "prepublication_market"])
def test_unusable_entry_clocks_cannot_fill(dataset, protocol, kind):
    reference = dataset["decisions"][0]["reference_epoch"]
    entry = dataset["quotes"][1]
    if kind == "stale": entry["market_epoch"] = entry["available_epoch"]-61
    if kind == "future_market": entry["market_epoch"] = entry["available_epoch"]+1
    if kind == "future_availability": entry["available_epoch"] = dataset["observed_cutoff_epoch"]+1
    if kind == "late_entry": entry.update(market_epoch=reference+69, available_epoch=reference+70)
    if kind == "prepublication_market": entry.update(market_epoch=reference+7, available_epoch=reference+9)
    assert_excluded(dataset, protocol, "missing_postpublication_entry_quote")


def test_first_available_entry_not_lowest_price_or_market_clock(dataset, protocol):
    reference = dataset["decisions"][0]["reference_epoch"]
    dataset["quotes"].extend([quote("earlier-market-later-arrival", reference+9, reference+15, 1.0),
                              quote("first-arrival", reference+11, reference+12, 1.2)])
    dataset["quotes"].reverse()
    row = scorer.evaluate(dataset, protocol)["decisions"][0]
    assert row["entry"]["quote_id"] == "first-arrival"


@pytest.mark.parametrize("invalid", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_quote_ids_fail_even_when_one_record_invalid(dataset, protocol, invalid, reverse):
    other = deepcopy(dataset["quotes"][1])
    other["bid"] = -1 if invalid else other["bid"]-.00001
    dataset["quotes"].append(other)
    if reverse: dataset["quotes"].reverse()
    with pytest.raises(ValueError, match="conflicting_quote_identity"):
        scorer.evaluate(dataset, protocol)


def test_identical_quote_duplicates_do_not_multiply_outcomes(dataset, protocol):
    dataset["quotes"].extend(deepcopy(dataset["quotes"]))
    report = scorer.evaluate(dataset, protocol)
    assert report["coverage"]["paired_scored_decisions"] == 1
    assert report["outcome_counts"] == {"up":1}


@pytest.mark.parametrize("kind", ["decision_id", "forecast_id", "reference_epoch"])
def test_repeated_identity_cannot_be_selected_by_outcome(dataset, protocol, kind):
    second = add_decision(dataset, protocol, offset=7200)
    first = dataset["decisions"][0]
    if kind == "decision_id": second["decision_id"] = first["decision_id"]
    if kind == "forecast_id": second["forecasts"][0]["forecast_id"] = first["forecasts"][0]["forecast_id"]
    if kind == "reference_epoch": second["reference_epoch"] = first["reference_epoch"]
    with pytest.raises(ValueError, match="duplicate"):
        scorer.evaluate(dataset, protocol)


def test_missing_family_preserves_other_family_coverage_without_unpaired_scores(dataset, protocol):
    lost = dataset["decisions"][0]["forecasts"].pop()["family"]
    report = assert_excluded(dataset, protocol, "missing_duplicate_or_unexpected_family")
    assert report["per_family_coverage"][lost] == {"issued":0,"clock_valid":0,"paired_scored":0}
    for family in set(protocol["cohorts"])-{lost}:
        assert report["per_family_coverage"][family] == {"issued":1,"clock_valid":1,"paired_scored":0}
    assert all(row["decisions"] == 0 for row in report["paired_summaries"].values())


def test_duplicate_family_is_not_a_four_model_comparison(dataset, protocol):
    forecasts = dataset["decisions"][0]["forecasts"]
    forecasts[3] = deepcopy(forecasts[0])
    forecasts[3]["forecast_id"] = "duplicate-family-unique-id"
    assert_excluded(dataset, protocol, "missing_duplicate_or_unexpected_family")


@pytest.mark.parametrize("field,value", [("cohort_id","old"), ("model_version","old"),
    ("feature_version","old"), ("input_timeframe","M5"), ("instrument","GBP_USD"), ("horizon_sec",60)])
def test_wrong_exact_identity_is_rejected(dataset, protocol, field, value):
    dataset["decisions"][0]["forecasts"][0][field] = value
    assert_excluded(dataset, protocol, "forecast_clock_identity_or_value_invalid", "identity_mismatch:"+field)


def test_flat_and_probability_tie_are_not_directional_wins(dataset, protocol):
    dataset["quotes"][2].update(bid=1.1-.0001, ask=1.1+.0001)
    families = list(protocol["cohorts"])
    dataset["decisions"][0]["forecasts"][0]["probability_up"] = .5
    dataset["decisions"][0]["forecasts"][0]["side"] = 0
    dataset["decisions"][0]["forecasts"][1]["probability_up"] = .25
    dataset["decisions"][0]["forecasts"][1]["side"] = -1
    report = scorer.evaluate(dataset, protocol)
    assert report["outcome_counts"] == {"flat":1}
    tied = report["paired_summaries"][families[0]]
    assert tied["abstentions"] == 1 and tied["directional_decisions"] == 0
    assert tied["direction_hit_rate_when_directional"] is None
    assert tied["mean_net_bps_per_decision"] == 0
    assert all(value == 0 for value in tied["stress_mean_net_bps_per_decision"].values())
    assert report["decisions"][0]["scores"][families[1]]["direction_correct"] is False
    assert report["decisions"][0]["scores"][families[1]]["brier"] == .0625


def test_late_publication_does_not_reuse_prepublication_entry(dataset, protocol):
    dataset["decisions"][0]["forecasts"][0]["committed_available_epoch"] += 20
    assert_excluded(dataset, protocol, "missing_postpublication_entry_quote")


def test_rolling_baseline_filters_at_original_earliest_issue_and_is_order_invariant(dataset, protocol):
    protocol["rolling_min_labels"] = 1
    protocol["rolling_lookback"] = 2
    first = dataset["decisions"][0]
    # First label becomes available exactly at second decision's earliest issue:
    # it must be excluded even though all models publish later than that label.
    second = add_decision(dataset, protocol, offset=3601, target_mid=1.099)
    third = add_decision(dataset, protocol, offset=7202, target_mid=1.102)
    for decision in (second, third):
        decision["forecasts"][0]["issued_epoch"] = decision["reference_epoch"]+1
    report = scorer.evaluate(dataset, protocol)
    by_id = {r["decision_id"]:r for r in report["decisions"]}
    assert by_id[second["decision_id"]]["rolling_baseline_training"]["n_training_labels"] == 0
    training = by_id[third["decision_id"]]["rolling_baseline_training"]
    assert training["n_training_labels"] == 1
    assert training["probability_up"] == pytest.approx(2/3)
    assert len(training["training_event_ids"]) == 1  # not four family copies
    assert training["last_training_target_epoch"] == first["target_epoch"]
    dataset["decisions"].reverse()
    dataset["quotes"].reverse()
    reordered = scorer.evaluate(dataset, protocol)
    assert reordered["decisions"] == report["decisions"]
    assert reordered["paired_summaries"] == report["paired_summaries"]


def test_prior_target_whose_label_arrives_after_issue_cannot_train(dataset, protocol):
    protocol["rolling_min_labels"] = 1
    # A 30-second quote delay is valid, but it does not make its label available
    # to a forecast originally issued only 12 seconds after that target.
    dataset["quotes"][2]["available_epoch"] += 30
    second = add_decision(dataset, protocol, offset=3610)
    # Its reference is still within the frozen age tolerance, but is not itself
    # an available post-target quote that could reveal the prior label.
    reference_quote = next(q for q in dataset["quotes"] if q["quote_id"] == "d3610-reference")
    reference_quote["market_epoch"] -= 40
    reference_quote["available_epoch"] -= 40
    report = scorer.evaluate(dataset, protocol)
    row = next(r for r in report["decisions"] if r["decision_id"] == second["decision_id"])
    assert row["rolling_baseline_training"]["n_training_labels"] == 0


@pytest.mark.parametrize("flag", ["proof_eligible", "account_eligible", "collection_enabled"])
def test_protocol_cannot_enable_execution_or_confirmation(dataset, protocol, flag):
    protocol[flag] = True
    with pytest.raises(ValueError, match="offline_only_protocol_required"):
        scorer.evaluate(dataset, protocol)


@pytest.mark.parametrize("value", [None, True, 1.0, "buy", 2])
def test_emitted_side_must_be_explicit_exact_integer(dataset, protocol, value):
    dataset["decisions"][0]["forecasts"][0]["side"] = value
    assert_excluded(dataset, protocol, "forecast_clock_identity_or_value_invalid", "invalid_emitted_side")


def test_emitted_buy_side_is_preserved_when_probability_direction_is_down(dataset, protocol):
    forecast = dataset["decisions"][0]["forecasts"][0]
    forecast.update(probability_up=.25, side=1)
    report = scorer.evaluate(dataset, protocol)
    score = report["decisions"][0]["scores"][forecast["family"]]
    assert score["side"] == 1
    assert score["probability_direction"] == -1
    assert score["emitted_side_differs_from_probability_direction"] is True
    assert score["brier"] == .5625
    assert score["direction_correct"] is True
    assert score["net_bps"] > 0


def test_emitted_abstention_is_preserved_even_when_probability_is_directional(dataset, protocol):
    forecast = dataset["decisions"][0]["forecasts"][0]
    forecast.update(probability_up=.75, side=0, abstention_reason="original_risk_gate")
    score = scorer.evaluate(dataset, protocol)["decisions"][0]["scores"][forecast["family"]]
    assert score["side"] == 0
    assert score["probability_direction"] == 1
    assert score["brier"] == .0625
    assert score["net_bps"] == 0
    assert score["abstention_reason"] == "original_risk_gate"
    assert all(value == 0 for value in score["stress_net_bps"].values())


@pytest.mark.parametrize("value", [None, -1, 0, True, "1.1"])
def test_original_reference_price_is_required_and_numeric(dataset, protocol, value):
    dataset["decisions"][0]["forecasts"][0]["reference_mid"] = value
    assert_excluded(dataset, protocol, "forecast_clock_identity_or_value_invalid", "invalid_original_reference_mid")


def test_different_forecast_price_anchor_cannot_use_common_target_return(dataset, protocol):
    dataset["decisions"][0]["forecasts"][0]["reference_mid"] = 1.10001
    assert_excluded(dataset, protocol, "forecast_reference_price_mismatch")


def test_reference_price_rounding_is_allowed_only_with_tight_absolute_tolerance(dataset, protocol):
    dataset["decisions"][0]["forecasts"][0]["reference_mid"] += 5e-13
    assert scorer.evaluate(dataset, protocol)["coverage"]["paired_scored_decisions"] == 1


def test_numeric_overflow_is_an_explicit_invalid_clock(dataset, protocol):
    dataset["decisions"][0]["forecasts"][0]["issued_epoch"] = 10**400
    assert_excluded(dataset, protocol, "forecast_clock_identity_or_value_invalid", "missing_or_invalid_clock:issued_epoch")
