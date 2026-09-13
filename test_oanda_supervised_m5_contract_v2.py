from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import oanda_practice_shadow_strategy_lab as lab
import oanda_strategy_lab_historical_backtest as history
import oanda_supervised_m5_contract_v2 as contract


def candles(size=80):
    start = datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc)
    result = []
    for index in range(size):
        mid = 1.1 + index * .0001 + math.sin(index / 5) * .00002
        result.append({"time": (start + timedelta(minutes=5 * index)).isoformat().replace("+00:00", "Z"),
            "complete": True, "mid": {"c": mid, "h": mid + .0001, "l": mid - .0001},
            "bid": {"c": mid - .00005}, "ask": {"c": mid + .00005}})
    return result


def test_contiguous_inputs_preserve_existing_feature_and_payoff_maths():
    values = candles()
    before = deepcopy(values)
    original = lab._supervised_contiguous_candle_data(values, .0001)
    corrected = lab._supervised_candle_data(values, .0001)
    assert corrected["x"] == original["x"]
    assert corrected["y"] == original["y"]
    assert corrected["current_x"] == original["current_x"]
    assert values == before
    assert corrected["times"][0] == "2026-01-05T03:05:00Z"
    assert corrected["target_times"][0] == "2026-01-05T03:10:00Z"
    assert corrected["data_contract"] == contract.CONTRACT


def test_weekend_gap_resets_recursive_features_and_keeps_older_segments():
    values = candles(160)
    for row in values[80:]:
        row["time"] = (datetime.fromisoformat(row["time"].replace("Z", "+00:00")) + timedelta(days=2)).isoformat()
    corrected = lab._supervised_candle_data(values, .0001)
    first = lab._supervised_contiguous_candle_data(values[:80], .0001)
    last = lab._supervised_contiguous_candle_data(values[80:], .0001)
    assert corrected["x"] == first["x"] + last["x"]
    assert corrected["y"] == first["y"] + last["y"]
    assert corrected["current_x"] == last["current_x"]
    assert corrected["data_quality"]["clock_gap"] == 1
    assert corrected["data_quality"]["eligible_segments"] == 2
    for origin, target in zip(corrected["times"], corrected["target_times"], strict=True):
        assert (datetime.fromisoformat(target) - datetime.fromisoformat(origin)).total_seconds() == 300


@pytest.mark.parametrize("mutation", ["duplicate", "reverse", "naive", "off_grid", "missing", "incomplete", "crossed", "nan"])
def test_ambiguous_or_unusable_current_history_cannot_publish(mutation):
    values = candles()
    if mutation == "duplicate":
        values[50]["time"] = values[49]["time"]
    elif mutation == "reverse":
        values[40], values[41] = values[41], values[40]
    elif mutation == "naive":
        for row in values:
            row["time"] = row["time"].rstrip("Z")
    elif mutation == "off_grid":
        values[-1]["time"] = "2026-01-05T06:35:01Z"
    elif mutation == "missing":
        values[-1].pop("time")
    elif mutation == "incomplete":
        values[-1]["complete"] = False
    elif mutation == "crossed":
        values[-1]["bid"]["c"] = values[-1]["ask"]["c"] + .001
    else:
        values[-1]["mid"]["c"] = math.nan
    assert lab._supervised_candle_data(values, .0001) is None


def test_missing_completion_flag_is_not_assumed_true():
    values = candles()
    for row in values:
        row.pop("complete")
    assert lab._supervised_candle_data(values, .0001) is None


@pytest.mark.parametrize("pip", [math.nan, math.inf, -1., 0., True])
def test_invalid_pip_never_reaches_feature_builder(pip):
    assert lab._supervised_candle_data(candles(), pip) is None


def test_old_price_revision_and_pip_change_invalidate_cache_identity():
    values = candles()
    original = contract.input_fingerprint(values, .0001)
    revised = deepcopy(values)
    revised[40]["bid"]["c"] -= .00001
    assert contract.input_fingerprint(revised, .0001) != original
    assert contract.input_fingerprint(values, .01) != original
    revised = deepcopy(values)
    revised[40]["volume"] = 500
    assert contract.input_fingerprint(revised, .0001) == original  # Not consumed by this model.


def test_equivalent_aware_clocks_normalize_before_temporal_split():
    values = candles()
    expected = lab._supervised_candle_data(values, .0001)
    eastern = timezone(timedelta(hours=-5))
    for row in values:
        row["time"] = datetime.fromisoformat(row["time"]).astimezone(eastern).isoformat()
    result = lab._supervised_candle_data(values, .0001)
    assert result == expected


def test_training_maturity_is_strictly_before_validation_and_cache_revisions_refit(monkeypatch):
    features = {f"PAIR_{index}": {"pip": .0001} for index in range(20)}
    data = {key: {"M5": candles()} for key in features}
    lab.SUPERVISED_MODEL_CACHE.clear()
    fit = lab._fit_ridge
    seen_rows = []
    def capture(x, y):
        seen_rows.append(len(x))
        return fit(x, y)
    monkeypatch.setattr(lab, "_fit_ridge", capture)
    try:
        lab.augment_supervised_return_features(features, data)
        assert all(item["supervised_ready"] for item in features.values())
        assert seen_rows == [660, 860]
        assert features["PAIR_0"]["supervised_purged_training_rows"] == 20
        lab.augment_supervised_return_features(features, data)
        assert len(seen_rows) == 2
        data["PAIR_0"]["M5"][40]["bid"]["c"] -= .00001
        lab.augment_supervised_return_features(features, data)
        assert len(seen_rows) == 4
    finally:
        lab.SUPERVISED_MODEL_CACHE.clear()


def m1_frame(rows=263):
    index = pd.date_range("2026-01-05T00:00:00Z", periods=rows, freq="min")
    mid = 1.1 + np.arange(rows) * .00001
    return pd.DataFrame({"open": mid, "high": mid + .0001, "low": mid - .0001, "close": mid,
        "volume": 10., "bid_open": mid - .00005, "bid_close": mid - .00005,
        "ask_open": mid + .00005, "ask_close": mid + .00005}, index=index)


def test_historical_builder_omits_partial_m5_buckets_and_reports_them():
    frame = m1_frame().drop(pd.Timestamp("2026-01-05T00:12:00Z"))
    result = history.build_pair_history("EUR_USD", frame)
    assert pd.Timestamp("2026-01-05T00:10:00Z") not in result.m5_times
    assert pd.Timestamp("2026-01-05T04:20:00Z") not in result.m5_times
    assert len(result.m5_times) == 51
    assert result.frame.attrs["input_quality"]["partial_m5_buckets_excluded"] == 2
    assert result.frame.attrs["input_quality"]["derived_bid_ask_extrema_from_endpoints"] is True


@pytest.mark.parametrize("pair,pip", [("EUR_HUF", .01), ("USD_HUF", .01), ("USD_THB", .01), ("HKD_JPY", .0001), ("USD_JPY", .01), ("EUR_USD", .0001)])
def test_historical_replay_uses_existing_audited_pip_map(pair, pip):
    assert history.build_pair_history(pair, m1_frame()).pip == pip


@pytest.mark.parametrize("mutation,error", [("duplicate", "duplicate_historical_m1_clock"),
    ("off_grid", "off_grid_historical_m1_clock"), ("crossed", "crossed_historical_bid_mid_ask"),
    ("infinite", "invalid_historical_price")])
def test_historical_clock_and_execution_data_fail_explicitly(mutation, error):
    frame = m1_frame()
    if mutation == "duplicate":
        frame = pd.concat([frame, frame.iloc[[5]]])
    elif mutation == "off_grid":
        index = list(frame.index); index[5] += pd.Timedelta(seconds=1); frame.index = pd.DatetimeIndex(index)
    elif mutation == "crossed":
        frame.loc[frame.index[5], "bid_close"] = 2.
    else:
        frame.loc[frame.index[5], "close"] = math.inf
    with pytest.raises(ValueError, match=error):
        history.build_pair_history("EUR_USD", frame)


@pytest.mark.parametrize("prefix", ["", "trad."])
@pytest.mark.parametrize("name", ["oanda_supervised_m5_contract_v2", "oanda_practice_shadow_strategy_lab", "oanda_strategy_lab_historical_backtest"])
def test_staged_import_bindings(prefix, name):
    loaded = importlib.import_module(prefix + name)
    assert Path(loaded.__file__).resolve().parent == Path(__file__).resolve().parent
