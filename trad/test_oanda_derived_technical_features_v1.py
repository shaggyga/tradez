"""Pure derived-grid parity, causality, clocks and assumption provenance."""
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pytest

import oanda_derived_technical_features_v1 as derived
import oanda_rolling_technical_features_v1 as original


def candles(count=720, step=60):
    rng = np.random.default_rng(426)
    close = 1.12 + np.cumsum(rng.normal(0, .00008, count))
    opening = close+rng.normal(0, .00002, count)
    spread = .00004+(np.arange(count) % 7)*.000001
    base = (1789516800//step)*step
    return {"time": base+np.arange(count, dtype=np.int64)*step,
            "open": opening, "high": np.maximum(opening, close)+.00006,
            "low": np.minimum(opening, close)-.00006, "close": close,
            "bid_close": close-spread, "ask_close": close+spread,
            "volume": rng.integers(1, 100, count).astype(float)}


def select(data, selection):
    return {name: values[selection] for name, values in data.items()}


def test_required_original_source_hash_is_exact():
    actual = hashlib.sha256(Path(original.__file__).read_bytes()).hexdigest()
    assert actual == derived.REQUIRED_SOURCE_BINDINGS[Path(original.__file__).name]
    assert derived.schema_metadata()["source_authentication_is_callers_responsibility"] is True


def test_no_fill_m1_exact_numeric_parity_and_no_input_mutation():
    data = candles()
    before = {name: values.copy() for name, values in data.items()}
    result = derived.compute_features(data, "EUR_USD", .0001, max_consecutive_fill=0)
    expected = original.compute_features(data, "EUR_USD", .0001)
    assert len(result["features"]) == 216
    assert result["observed_mask"].all()
    assert not result["imputed_mask"].any()
    for name, values in expected.items():
        np.testing.assert_array_equal(result["features"][name.replace("m1__", "m1_derived__")], values)
    for name, values in before.items():
        np.testing.assert_array_equal(data[name], values)
        np.testing.assert_array_equal(result["data"][name], values)


def test_short_flat_fill_uses_only_prior_actual_values_and_assumed_zero_volume():
    data = candles(8)
    gap = select(data, [0, 1, 5, 6, 7])
    result = derived.compute_features(gap, "EUR_USD", .0001)
    np.testing.assert_array_equal(result["data"]["time"], data["time"])
    np.testing.assert_array_equal(result["imputed_mask"], [False, False, True, True, True, False, False, False])
    for name in ("open", "high", "low", "close"):
        np.testing.assert_array_equal(result["data"][name][2:5], [data["close"][1]]*3)
    for name in ("bid_close", "ask_close"):
        np.testing.assert_array_equal(result["data"][name][2:5], [data[name][1]]*3)
    np.testing.assert_array_equal(result["data"]["volume"][2:5], [0, 0, 0])
    np.testing.assert_array_equal(result["last_actual_bar_start_epoch"][2:5], [data["time"][1]]*3)
    for name in data:
        np.testing.assert_array_equal(result["data"][name][[0, 1, 5, 6, 7]], data[name][[0, 1, 5, 6, 7]])


@pytest.mark.parametrize("step", [60, 300])
def test_every_prefix_matches_full_computation_including_long_gap_first_n(step):
    data = candles(680, step)
    keep = np.ones(680, dtype=bool)
    keep[[5, 6, 40, 43]] = False
    keep[620:638] = False
    data = select(data, keep)
    full = derived.compute_features(data, "EUR_USD", .0001, bar_seconds=step)
    for ordinal in (1, 7, 44, 621, 625, 636, 642, 679):
        cutoff = int(candles(680, step)["time"][ordinal]+step)
        prefix_data = select(data, data["time"]+step <= cutoff)
        prefix = derived.compute_features(prefix_data, "EUR_USD", .0001, bar_seconds=step, as_of_epoch=cutoff)
        positions = full["data"]["time"]+step <= cutoff
        for name in full["data"]:
            np.testing.assert_array_equal(prefix["data"][name], full["data"][name][positions])
        for name in full["features"]:
            np.testing.assert_array_equal(prefix["features"][name], full["features"][name][positions], err_msg=name)
        for name in ("observed_mask", "imputed_mask", "last_actual_bar_start_epoch", "unfilled_gap_before_bars"):
            np.testing.assert_array_equal(prefix[name], full[name][positions])
        for name, quality in full["feature_quality"].items():
            for field in ("observed_count", "imputed_count", "missing_grid_count", "imputed_fraction", "gap_mask"):
                np.testing.assert_array_equal(prefix["feature_quality"][name][field], quality[field][positions])


def test_future_endpoint_change_never_changes_earlier_assumed_bars():
    data = select(candles(20), [0, 1, 18, 19])
    before = derived.compute_features(data, "EUR_USD", .0001)
    altered = {name: values.copy() for name, values in data.items()}
    for name in ("open", "high", "low", "close", "bid_close", "ask_close"):
        altered[name][2:] += .5
    after = derived.compute_features(altered, "EUR_USD", .0001)
    mask = before["data"]["time"] < data["time"][2]
    for name in before["features"]:
        np.testing.assert_array_equal(before["features"][name][mask], after["features"][name][mask])
    assert before["unfilled_gap_before_bars"].max() == 11


@pytest.mark.parametrize("step", [60, 300])
def test_weekend_remains_a_real_gap_and_does_not_support_cross_gap_return(step):
    data = candles(8, step)
    data["time"][4:] += 2*86400
    result = derived.compute_features(data, "EUR_USD", .0001, bar_seconds=step)
    actual_index = int(np.flatnonzero(result["data"]["time"] == data["time"][4])[0])
    assert result["gap_before_mask"][actual_index]
    assert result["unfilled_gap_before_bars"][actual_index] > 100
    prefix = "m1_derived__" if step == 60 else "m5_derived__"
    assert math.isnan(result["features"][prefix+"return_1_bps"][actual_index])
    assert result["feature_quality"][prefix+"return_1_bps"]["gap_mask"][actual_index]
    assert np.isfinite(result["features"][prefix+"return_1_bps"][actual_index+1])


def test_m5_numeric_windows_and_four_calendar_fields_use_original_elapsed_clock():
    data = candles(720, 300)
    result = derived.compute_features(data, "EUR_USD", .0001, bar_seconds=300)
    nominal = dict(data, time=(data["time"]//300)*60)
    expected = original.compute_features(nominal, "EUR_USD", .0001)
    for name, values in expected.items():
        if "__utc_" not in name:
            np.testing.assert_array_equal(result["features"][name.replace("m1__", "m5_derived__")], values)
    end = data["time"]+300
    hour = end % 86400/3600.
    weekday = ((end//86400+3) % 7).astype(float)
    for label, values, period in (("hour", hour, 24.), ("weekday", weekday, 7.)):
        for operation in ("sin", "cos"):
            np.testing.assert_array_equal(result["features"][f"m5_derived__utc_{label}_{operation}"],
                                          getattr(np, operation)(2*math.pi*values/period))
    assert not np.array_equal(result["features"]["m5_derived__utc_hour_sin"], expected["m1__utc_hour_sin"])
    registry = {row["name"]: row for row in derived.feature_registry(300)}
    assert registry["m5_derived__return_15_bps"]["lookback_seconds"] == 16*300
    assert registry["m5_derived__finite_ema_slope3_200_pips"]["lookback_seconds"] == 603*300
    assert all(row["actual_bar_seconds"] == 300 and row["original_weight_compatibility"] is False for row in registry.values())


def test_window_quality_counts_observed_assumed_and_leading_absence_without_caps():
    data = select(candles(10), [0, 1, 4, 5, 6, 7, 8, 9])
    result = derived.compute_features(data, "EUR_USD", .0001)
    quality = result["feature_quality"]["m1_derived__return_5_bps"]
    assert quality["observed_count"][5] == 4
    assert quality["imputed_count"][5] == 2
    assert quality["missing_grid_count"][5] == 0
    assert quality["imputed_fraction"][5] == 2/6
    assert quality["missing_grid_count"][0] == 5
    assert np.isfinite(result["features"]["m1_derived__return_5_bps"][5])


def test_flat_assumptions_do_not_make_undefined_denominators_zero():
    data = candles(1)
    at = int(data["time"][-1])
    result = derived.latest_features(data, "EUR_USD", .0001, as_of_epoch=at+6*60)
    assert result["imputed"] and result["consecutive_imputed_bars"] == 5
    assert result["values"]["m1_derived__bar_body_pips"] == 0.
    assert result["values"]["m1_derived__body_to_range"] is None
    assert result["values"]["m1_derived__upper_wick_fraction"] is None
    assert result["values"]["m1_derived__lower_wick_fraction"] is None
    assert result["publication_epoch"] is None
    assert result["latest_actual_bar_age_seconds"] == 300
    json.dumps(result, allow_nan=False)


def test_trailing_fill_stops_at_bound_and_actual_age_never_retimestamped():
    data = candles(10, 300)
    at = int(data["time"][-1])
    result = derived.latest_features(data, "EUR_USD", .0001, bar_seconds=300, as_of_epoch=at+11*300)
    assert result["bar_start_epoch"] == at+3*300
    assert result["latest_actual_bar_start_epoch"] == at
    assert result["latest_actual_bar_age_seconds"] == 10*300
    assert result["trailing_unfilled_bars"] == 7
    assert result["max_consecutive_fill"] == 3


def test_invalid_prior_close_prevents_fill_and_observed_row_is_never_replaced():
    data = select(candles(8), [0, 1, 7])
    data["close"][1] = np.nan
    result = derived.regularize(data, "EUR_USD", .0001)
    np.testing.assert_array_equal(result["data"]["time"], data["time"])
    assert np.isnan(result["data"]["close"][1])
    assert not result["imputed_mask"].any()


@pytest.mark.parametrize("change", ["duplicate", "step", "incomplete", "capacity", "fill_bound"])
def test_explicit_clock_and_memory_bounds(change):
    data = candles(10)
    kwargs = {}
    if change == "duplicate":
        data["time"][2] = data["time"][1]
    elif change == "step":
        kwargs["bar_seconds"] = 120
    elif change == "incomplete":
        kwargs["as_of_epoch"] = int(data["time"][-1])+59
    elif change == "capacity":
        kwargs["max_output_rows"] = 5
    else:
        kwargs["max_consecutive_fill"] = True
    with pytest.raises(ValueError):
        derived.compute_features(data, "EUR_USD", .0001, **kwargs)


def test_empty_input_returns_null_latest_without_fabrication():
    data = candles(0)
    result = derived.latest_features(data, "EUR_USD", .0001, as_of_epoch=1789538000)
    assert result["bar_start_epoch"] is None
    assert result["latest_actual_bar_start_epoch"] is None
    assert all(value is None for value in result["values"].values())
    assert result["observed"] is None and result["imputed"] is None
