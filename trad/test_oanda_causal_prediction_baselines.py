"""Forecast-time contamination and reference baseline contracts."""

from copy import deepcopy
from itertools import permutations

import pytest

from oanda_causal_prediction_baselines import decide_baselines


def label(event_id, target, available=None, up=True, **updates):
    row = {
        "event_id": event_id,
        "instrument": "EUR_USD",
        "horizon_sec": 3600,
        "target_epoch": target,
        "available_epoch": target if available is None else available,
        "up": up,
    }
    row.update(updates)
    return row


def rolling(rows, issue=100, **kwargs):
    return decide_baselines(issue, rows, **kwargs)["rolling_class_rate"]


def test_fixed_references_and_empty_warmup_are_deterministic():
    actual = decide_baselines(100, [])
    assert actual["fair_coin"] == {"probability_up": 0.5, "side": 0}
    assert actual["zero_move"] == {"predicted_return_bps": 0.0}
    assert actual["no_trade"] == {"side": 0}
    assert actual["rolling_class_rate"] == {
        "probability_up": 0.5, "side": 0, "n_training_labels": 0,
        "eligible_training_count": 0, "warmup_fallback": True,
        "last_training_target_epoch": None, "last_training_available_epoch": None,
        "training_event_ids": [],
    }


def test_label_availability_not_row_order_controls_causality():
    rows = [
        label("known-down", 10, 15, False),
        label("old-target-unavailable", 1, 101, True),
        label("future-target", 101, 102, True),
        label("simultaneous-availability", 20, 100, True),
        label("simultaneous-target", 100, 100, True),
    ]
    for permuted in permutations(rows):
        result = rolling(list(permuted), min_labels=1)
        assert result["training_event_ids"] == ["known-down"]
        assert result["n_training_labels"] == result["eligible_training_count"] == 1
        assert result["probability_up"] == pytest.approx(1 / 3)
        assert result["side"] == -1


def test_default_warmup_threshold_does_not_fit_fewer_than_twenty_labels():
    rows = [label(f"event-{i}", i) for i in range(20)]
    result = rolling(rows[:-1])
    assert result["warmup_fallback"] is True
    assert result["n_training_labels"] == 19
    assert result["probability_up"] == 0.5
    assert result["side"] == 0
    result = rolling(rows)
    assert result["warmup_fallback"] is False
    assert result["probability_up"] == pytest.approx(21 / 22)
    assert result["side"] == 1


def test_four_model_families_do_not_multiply_labels_or_end_warmup():
    rows = [label(f"event-{i}", i, up=i % 2 == 0) for i in range(6)]
    copies = [dict(row, family=family) for family in range(4) for row in rows]
    assert rolling(copies) == rolling(rows)
    assert rolling(copies)["n_training_labels"] == 6
    assert rolling(copies)["warmup_fallback"] is True


def test_identical_duplicate_does_not_change_fitted_probability():
    rows = [label("up", 1), label("down", 2, up=False), label("up2", 3)]
    assert rolling(rows * 4, min_labels=1) == rolling(rows, min_labels=1)
    assert rolling(rows, min_labels=1)["probability_up"] == 3 / 5


def test_bounded_lookback_orders_by_target_then_availability_then_event_id():
    rows = [
        label("older-arrived-last", 1, 99, False),
        label("a", 90, 95, False),
        label("c", 90, 96, True),
        label("b", 90, 96, False),
        label("newest", 91, 91, True),
    ]
    expected = rolling(rows, lookback=3, min_labels=1)
    for permuted in permutations(rows):
        assert rolling(list(permuted), lookback=3, min_labels=1) == expected
    assert expected["training_event_ids"] == ["b", "c", "newest"]
    assert expected["eligible_training_count"] == 5
    assert expected["n_training_labels"] == 3
    assert expected["probability_up"] == 3 / 5
    assert expected["last_training_target_epoch"] == 91
    assert expected["last_training_available_epoch"] == 96


def test_default_lookback_uses_latest_one_hundred_unique_events():
    rows = [label(f"event-{i:03}", i, up=i >= 5) for i in range(105)]
    result = rolling(rows, issue=200)
    assert result["eligible_training_count"] == 105
    assert result["n_training_labels"] == 100
    assert result["training_event_ids"][0] == "event-005"
    assert result["probability_up"] == pytest.approx(101 / 102)


def test_training_clock_maxima_include_late_arriving_older_target():
    result = rolling([label("old", 1, 99), label("new", 90, 91)], min_labels=1)
    assert result["last_training_target_epoch"] == 90
    assert result["last_training_available_epoch"] == 99


def test_balanced_labels_abstain_instead_of_defaulting_to_up():
    result = rolling([label("up", 1), label("down", 2, up=False)], min_labels=1)
    assert result["probability_up"] == 0.5
    assert result["side"] == 0
    assert result["warmup_fallback"] is False


def test_other_instruments_and_horizons_do_not_train_selected_baseline():
    rows = [
        label("selected", 1, up=False),
        label("wrong-pair", 2, instrument="GBP_USD"),
        label("wrong-horizon", 3, horizon_sec=60),
    ]
    assert rolling(rows, min_labels=1)["training_event_ids"] == ["selected"]
    result = rolling(rows, min_labels=1, instrument="GBP_USD", horizon_sec=3600)
    assert result["training_event_ids"] == ["wrong-pair"]


@pytest.mark.parametrize("field,value", [
    ("up", False), ("target_epoch", 2), ("available_epoch", 3),
    ("instrument", "GBP_USD"), ("horizon_sec", 60),
])
def test_conflicting_event_ids_fail_closed_even_if_outside_selected_cohort(field, value):
    first = label("same-event", 1)
    duplicate = dict(first, **{field: value})
    if field == "target_epoch":
        duplicate["available_epoch"] = 2
    with pytest.raises(ValueError, match="conflicting labels"):
        rolling([first, duplicate])


@pytest.mark.parametrize("field,value", [
    ("target_epoch", float("nan")), ("available_epoch", float("inf")),
    ("available_epoch", float("-inf")), ("target_epoch", True),
    ("available_epoch", "1"), ("horizon_sec", 0), ("horizon_sec", -1),
    ("horizon_sec", float("nan")), ("horizon_sec", False),
    ("up", 1), ("up", 0), ("up", "false"), ("up", None),
    ("event_id", ""), ("event_id", 1), ("instrument", "  "),
    ("instrument", None),
])
def test_malformed_label_values_fail_closed(field, value):
    malformed = label("test", 1)
    malformed[field] = value
    with pytest.raises(ValueError):
        rolling([malformed])


def test_invalid_label_is_rejected_even_when_in_future_or_other_instrument():
    with pytest.raises(ValueError, match="up must be a boolean"):
        rolling([label("future-other", 200, up=1, instrument="GBP_USD")])


def test_nan_duplicate_cannot_bypass_validation():
    with pytest.raises(ValueError, match="finite number"):
        rolling([label("same", 1), label("same", float("nan"))])


def test_availability_before_target_is_rejected():
    with pytest.raises(ValueError, match="availability precedes its target"):
        rolling([label("inverted", 10, 9)])


@pytest.mark.parametrize("rows", [None, (), {}, [None], [{}]])
def test_missing_or_malformed_label_containers(rows):
    with pytest.raises(ValueError):
        rolling(rows)


@pytest.mark.parametrize("issue", [float("nan"), float("inf"), True, "100", None])
def test_invalid_issue_clock(issue):
    with pytest.raises(ValueError, match="issue_epoch"):
        decide_baselines(issue, [])


@pytest.mark.parametrize("kwargs", [
    {"lookback": 0}, {"lookback": -1}, {"lookback": True}, {"lookback": 2.5},
    {"min_labels": 0}, {"min_labels": False}, {"instrument": ""},
    {"horizon_sec": -1}, {"horizon_sec": float("inf")},
])
def test_invalid_selection_parameters(kwargs):
    with pytest.raises(ValueError):
        decide_baselines(100, [], **kwargs)


def test_inputs_are_not_mutated():
    rows = [label("b", 2, family="ridge"), label("a", 1, up=False)]
    original = deepcopy(rows)
    decide_baselines(100, rows, min_labels=1)
    assert rows == original
