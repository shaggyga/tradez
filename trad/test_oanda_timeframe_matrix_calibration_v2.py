import copy
import random

import pytest

from trad.oanda_timeframe_matrix_calibration_v2 import (
    CONTRACT_ID, CalibrationScope, evaluate_calibration,
)


SCOPE = CalibrationScope("EUR_USD", "timeframe_equation_matrix.m1", "fresh-v2-test", 60)


def forecast(index=0, **changes):
    reference = float(index * 100)
    result = dict(contract_id=CONTRACT_ID, forecast_id=f"forecast-{index:03d}",
                  event_id=f"event-{index:03d}", instrument=SCOPE.instrument,
                  lane_id=SCOPE.lane_id, cohort_id=SCOPE.cohort_id,
                  horizon_sec=SCOPE.horizon_sec, reference_epoch=reference,
                  target_epoch=reference + 60, reference_mid=1.1,
                  issued_epoch=reference + 1, committed_available_epoch=reference + 2,
                  raw_probability_up=0.8)
    result.update(changes)
    return result


def label(index=0, **changes):
    reference = float(index * 100)
    result = dict(contract_id=CONTRACT_ID, event_id=f"event-{index:03d}",
                  instrument=SCOPE.instrument, horizon_sec=SCOPE.horizon_sec,
                  reference_epoch=reference, target_epoch=reference + 60,
                  reference_mid=1.1, target_mid=1.1001,
                  target_quote_epoch=reference + 60, committed_available_epoch=reference + 61)
    result.update(changes)
    return result


def evaluate(forecasts, labels, **kwargs):
    return evaluate_calibration(forecasts, labels, scope=kwargs.pop("scope", SCOPE), **kwargs)


def test_filters_at_original_issue_not_later_publication():
    prior = forecast(0)
    current = forecast(1, issued_epoch=101, committed_available_epoch=120)
    delayed = label(0, committed_available_epoch=110)
    row = evaluate([prior, current], [delayed])["rows"][1]
    assert row["n_training_labels"] == 0
    assert row["outcome_available"] is False


@pytest.mark.parametrize("available", [101, 102, 1000])
def test_label_availability_equal_or_after_issue_is_excluded(available):
    row = evaluate([forecast(0), forecast(1)], [label(0, committed_available_epoch=available)])["rows"][1]
    assert row["n_training_labels"] == 0


def test_target_and_actual_maturity_must_both_precede_issue():
    current = forecast(1, reference_epoch=50, target_epoch=110, issued_epoch=60,
                       committed_available_epoch=61)
    row = evaluate([forecast(0), current], [label(0, target_quote_epoch=60,
                                                       committed_available_epoch=60)])["rows"][1]
    assert row["n_training_labels"] == 0
    current["issued_epoch"] = 70
    current["committed_available_epoch"] = 71
    row = evaluate([forecast(0), current], [label(0, target_quote_epoch=70,
                                                       committed_available_epoch=70)])["rows"][1]
    assert row["n_training_labels"] == 0


def test_disordered_arrival_and_input_order_are_deterministic_without_own_label():
    forecasts = [forecast(i) for i in range(45)]
    labels = [label(i) for i in range(45)]
    labels[0]["committed_available_epoch"] = 4250
    before = copy.deepcopy((forecasts, labels))
    expected = evaluate(forecasts, labels)
    random.Random(9).shuffle(forecasts)
    random.Random(10).shuffle(labels)
    assert evaluate(forecasts, labels) == expected
    # A very old target arrives late; it is not training data for early issues.
    rows = {row["forecast_id"]: row for row in expected["rows"]}
    assert "forecast-000" not in rows["forecast-042"]["training_forecast_ids"]
    assert "forecast-000" in rows["forecast-043"]["training_forecast_ids"]
    for row in expected["rows"]:
        assert row["forecast_id"] not in row["training_forecast_ids"]
        if row["n_training_labels"]:
            assert row["last_training_target_epoch"] < row["issued_epoch"]
            assert row["last_training_maturity_epoch"] < row["issued_epoch"]
            assert row["last_training_available_epoch"] < row["issued_epoch"]
    assert sorted(forecasts, key=lambda x:x["forecast_id"]) == before[0]
    assert sorted(labels, key=lambda x:x["event_id"]) == before[1]


def test_simultaneous_issue_forecasts_cannot_train_one_another():
    other = forecast(1, reference_epoch=1, target_epoch=61,
                     issued_epoch=2, committed_available_epoch=3)
    first = forecast(0, issued_epoch=2, committed_available_epoch=3)
    report = evaluate([other, first], [label(0), label(1, reference_epoch=1,
                        target_epoch=61, target_quote_epoch=61, committed_available_epoch=62)])
    assert [row["n_training_labels"] for row in report["rows"]] == [0, 0]


def test_identical_duplicates_and_aliases_count_one_market_label():
    forecasts = [forecast(i) for i in range(42)]
    labels = [label(i) for i in range(42)]
    baseline = evaluate(forecasts, labels)
    forecasts.extend([copy.deepcopy(forecasts[0]), forecast(0, forecast_id="z-alias", event_id="alias")])
    labels.extend([copy.deepcopy(labels[0]), label(0, event_id="alias")])
    report = evaluate(forecasts, labels)
    assert report["unique_scope_forecast_count"] == 42
    assert report["rows"] == baseline["rows"]
    assert report["rows"][-1]["n_training_labels"] == 41


@pytest.mark.parametrize("changes", [
    {"target_mid": 1.0}, {"reference_mid": 1.2}, {"committed_available_epoch": 70},
])
def test_conflicting_label_aliases_fail_closed(changes):
    with pytest.raises(ValueError, match="conflicting label"):
        evaluate([forecast()], [label(), label(event_id="alias", **changes)])


def test_same_scope_conflicting_forecast_alias_is_not_extra_training_weight():
    with pytest.raises(ValueError, match="conflicting forecasts"):
        evaluate([forecast(), forecast(forecast_id="alias", raw_probability_up=0.2)], [label()])


def test_ids_cannot_be_reused_for_different_events_or_forecasts():
    with pytest.raises(ValueError, match="event_id reused"):
        evaluate([forecast(), forecast(1, event_id="event-000")], [])
    with pytest.raises(ValueError, match="forecast_id reused"):
        evaluate([forecast(), forecast(1, forecast_id="forecast-000")], [])


@pytest.mark.parametrize("field,value", [
    ("instrument", "USD_JPY"), ("lane_id", "other"), ("cohort_id", "old-v1"),
    ("horizon_sec", 120),
])
def test_training_is_scoped_by_pair_lane_cohort_and_horizon(field, value):
    prior = forecast(0, **{field:value})
    realized = label(0)
    if field in {"instrument", "horizon_sec"}:
        realized[field] = value
    if field == "horizon_sec":
        prior["target_epoch"] = realized["target_epoch"] = 120
        realized["target_quote_epoch"] = 120
        realized["committed_available_epoch"] = 121
    report = evaluate([prior, forecast(2)], [realized])
    assert report["outside_scope_forecast_count"] == 1
    assert report["rows"][0]["n_training_labels"] == 0


def test_missing_outcome_has_frozen_prediction_and_does_not_train():
    row = evaluate([forecast(0), forecast(1)], [label(1)])["rows"][1]
    assert row["n_training_labels"] == 0
    assert row["outcome_available"] is True
    assert row["raw_brier"] == pytest.approx(0.04)


def test_warmup_boundary_uses_unique_available_labels_and_known_bin_posterior():
    report = evaluate([forecast(i) for i in range(41)], [label(i) for i in range(41)])
    before, after = report["rows"][-2:]
    assert before["warmup_fallback"] is True
    assert before["calibrated_probability_up"] == 0.8
    assert after["warmup_fallback"] is False
    assert after["bin_training_labels"] == 40
    assert after["calibrated_probability_up"] == pytest.approx((40 + 20 * 0.85) / 60)
    assert report["scored_after_warmup_count"] == 1
    assert report["fair_coin_brier"] == 0.25
    assert report["proof_eligible"] is False
    assert report["account_eligible"] is False
    assert report["collection_enabled"] is False
    assert report["validation_ready"] is False
    assert report["base_forecast_provenance_verified"] is False
    assert report["independent_sample_count"] is None


def test_probability_ties_abstain_and_realized_flats_are_not_down_accuracy_credit():
    forecasts = [forecast(i, raw_probability_up=0.5) for i in range(43)]
    # 19 ups, 21 downs plus a bin prior of 11 / 20 produces a 30 / 60 tie.
    labels = [label(i, target_mid=1.1001 if i < 19 else 1.0999) for i in range(40)]
    labels.append(label(40, target_mid=1.1))
    report = evaluate(forecasts, labels)
    row = report["rows"][40]
    assert row["calibrated_probability_up"] == 0.5
    assert row["raw_side"] == row["calibrated_side"] == 0
    assert row["actual_up"] == 0
    assert row["actual_side"] == 0
    assert report["flat_outcome_count"] == 1
    assert report["directional_target_count"] == 0
    assert report["raw_directional_accuracy"] is None
    assert report["calibrated_directional_accuracy"] is None


@pytest.mark.parametrize("field", ["issued_epoch", "committed_available_epoch",
                                       "reference_epoch", "target_epoch"])
def test_missing_forecast_clock_is_not_inferred(field):
    row = forecast()
    del row[field]
    with pytest.raises(ValueError, match="finite number"):
        evaluate([row], [label()])


@pytest.mark.parametrize("field", ["target_epoch", "target_quote_epoch", "committed_available_epoch"])
def test_missing_label_clock_is_not_inferred(field):
    row = label()
    del row[field]
    with pytest.raises(ValueError, match="finite number"):
        evaluate([forecast()], [row])


@pytest.mark.parametrize("field,value", [
    ("raw_probability_up", True), ("raw_probability_up", -0.1),
    ("raw_probability_up", 1.1), ("raw_probability_up", float("nan")),
    ("issued_epoch", float("inf")), ("issued_epoch", "1"),
    ("issued_epoch", -1), ("committed_available_epoch", 0),
    ("committed_available_epoch", 60), ("target_epoch", 61),
    ("reference_mid", 0), ("horizon_sec", 60.0), ("horizon_sec", True),
])
def test_invalid_forecasts_fail_closed(field, value):
    with pytest.raises(ValueError):
        evaluate([forecast(**{field:value})], [label()])


@pytest.mark.parametrize("changes", [
    {"committed_available_epoch": 59}, {"target_quote_epoch": 59},
    {"target_quote_epoch": 62}, {"target_mid": 0}, {"target_mid": float("nan")},
    {"reference_mid": 1.2},
    {"target_quote_epoch": 121, "committed_available_epoch": 122},
])
def test_invalid_labels_fail_closed(changes):
    with pytest.raises(ValueError):
        evaluate([forecast()], [label(**changes)])


def test_legacy_rows_cannot_be_relabelled_by_omitted_contract():
    with pytest.raises(ValueError, match="separate V2 contract"):
        evaluate([{"row_id":1, "observed_utc":"2026-09-01T12:00:00Z"}], [])
    with pytest.raises(ValueError, match="separate V2 contract"):
        evaluate([forecast(contract_id="legacy")], [])


def test_malformed_unrelated_scope_is_rejected_before_filtering():
    with pytest.raises(ValueError):
        evaluate([forecast(instrument="USD_JPY", issued_epoch=None)], [])


def test_empty_inputs_are_not_positive_evidence():
    report = evaluate([], [])
    assert report["raw_brier"] is None
    assert report["calibrated_brier"] is None
    assert report["fair_coin_brier"] is None
    assert report["rows"] == []
    assert report["proof_eligible"] is False
