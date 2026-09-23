"""Causality, elapsed support and historical/live parity of the shared kernel."""
import math
from datetime import datetime, timezone

import numpy as np
import pytest

import oanda_rolling_technical_features_v1 as kernel


def candles(count=850, seed=419):
    rng = np.random.default_rng(seed)
    close = 1.12 + np.cumsum(rng.normal(0,.00008,count))
    open_ = close + rng.normal(0,.00002,count)
    return {"time":1700000040+np.arange(count,dtype=np.int64)*60,
        "open":open_,"high":np.maximum(open_,close)+.00006,
        "low":np.minimum(open_,close)-.00006,"close":close,
        "bid_close":close-.00005,"ask_close":close+.00005,
        "volume":rng.integers(1,100,count).astype(float)}


def section(data,start=None,end=None):
    return {k:v[start:end] for k,v in data.items()}


def assert_columns_equal(left,right):
    assert list(left) == list(right)
    for name in left:
        np.testing.assert_array_equal(left[name],right[name],err_msg=name)


def test_prefix_causal_and_input_unmodified():
    data = candles()
    untouched = {k:v.copy() for k,v in data.items()}
    full = kernel.compute_features(data,"EUR_USD",.0001)
    for end in (1,15,61,250,603,701):
        prefix = kernel.compute_features(section(data,end=end),"EUR_USD",.0001)
        assert_columns_equal(prefix,{name:values[:end] for name,values in full.items()})
    for name in data:
        np.testing.assert_array_equal(data[name],untouched[name])


def test_chunk_overlap_and_latest_are_exactly_batch_equivalent():
    data = candles(1001)
    full = kernel.compute_features(data,"EUR_USD",.0001)
    overlap = kernel.max_lookback_bars()-1
    for start in (0,103,401,803):
        end = min(start+198,len(data["time"]))
        left = max(0,start-overlap)
        result = kernel.compute_features(section(data,left,end),"EUR_USD",.0001)
        assert_columns_equal({k:v[start-left:] for k,v in result.items()},
                             {k:v[start:end] for k,v in full.items()})
    latest = kernel.latest_features(section(data,-kernel.max_lookback_bars()),"EUR_USD",.0001)
    for name,values in full.items():
        assert latest[name] is None if not np.isfinite(values[-1]) else latest[name] == values[-1]


def test_gaps_invalidate_only_crossing_support_and_single_bar_is_useful():
    data = candles(750)
    data["time"][650:] += 2*60
    result = kernel.compute_features(data,"EUR_USD",.0001)
    assert math.isnan(result["m1__return_1_pips"][650])
    assert math.isfinite(result["m1__return_1_pips"][651])
    assert np.isnan(result["m1__return_5_pips"][650:655]).all()
    assert math.isfinite(result["m1__return_5_pips"][655])
    assert math.isfinite(result["m1__historical_spread_pips"][650])
    assert math.isfinite(result["m1__bar_range_pips"][650])
    assert np.isnan(result["m1__finite_ema_gap_200_pips"][650:]).all()
    first = kernel.latest_features(section(data,end=1),"EUR_USD",.0001)
    assert first["m1__bar_range_pips"] > 0
    assert first["m1__tick_activity"] > 0
    assert first["m1__return_1_pips"] is None


@pytest.mark.parametrize("direction,expected",[(0,50.),(1,100.),(-1,0.)])
def test_rsi_flat_gain_only_and_loss_only_are_defined(direction,expected):
    data = candles(40)
    close = 1.12+direction*np.arange(40)*.0001
    data.update(close=close,open=close.copy(),high=close+.0001,low=close-.0001,
                bid_close=close-.00005,ask_close=close+.00005)
    out = kernel.compute_features(data,"EUR_USD",.0001)
    assert np.isnan(out["m1__rsi_14"][:14]).all()
    np.testing.assert_array_equal(out["m1__rsi_14"][14:],expected)


def test_real_elapsed_return_15_is_distinct_from_1_and_atr_matches_formula():
    data = candles(40)
    close = 1.+np.arange(40)*.0001
    data.update(close=close,open=close.copy(),high=close+.0002,low=close-.0002)
    result = kernel.compute_features(data,"EUR_USD",.0001)
    np.testing.assert_allclose(result["m1__return_15_pips"][15:],15.)
    np.testing.assert_allclose(result["m1__return_1_pips"][1:],1.)
    np.testing.assert_allclose(result["m1__atr_14_pips"][14:],4.)
    assert np.isnan(result["m1__atr_14_pips"][:14]).all()


def test_existing_structural_calculator_oracle_on_supported_contiguous_bars():
    # Existing reviewed AST slice does not import accounts or fitted models.
    from oanda_research_feature_calculator_v1 import calculators
    from oanda_feature_candle_inputs_v2 import aggregate
    data = candles(360)
    rows = []
    for index,stamp in enumerate(data["time"]):
        mid = {short:float(data[long][index]) for short,long in (("o","open"),("h","high"),("l","low"),("c","close"))}
        rows.append({"time":datetime.fromtimestamp(int(stamp),timezone.utc).isoformat(),"complete":True,
            "mid":mid,"bid":dict(mid),"ask":dict(mid),"volume":float(data["volume"][index])})
    expected,reason = calculators()["build_features"]("EUR_USD",{"M1":rows,"M5":aggregate(rows,300,60)},.0001,include_ma_grid=False)
    assert not reason
    actual = kernel.latest_features(data,"EUR_USD",.0001)
    for canonical,legacy in (("return_1_pips","r1_pips"),("return_3_pips","r3_pips"),
            ("return_5_pips","r5_pips"),("close_range_position_20","pos20"),("atr_14_pips","m1_atr14_pips")):
        assert actual["m1__"+canonical] == pytest.approx(expected[legacy],rel=1e-12,abs=1e-12)


def test_optional_missing_activity_spread_do_not_block_close_features():
    data = candles(100)
    for name in ("bid_close","ask_close","volume"):
        del data[name]
    result = kernel.compute_features(data,"EUR_USD",.0001)
    assert np.isnan(result["m1__historical_spread_pips"]).all()
    assert np.isnan(result["m1__tick_activity"]).all()
    assert math.isfinite(result["m1__return_60_pips"][-1])
    assert math.isfinite(result["m1__atr_14_pips"][-1])


def test_invalid_price_and_crossed_spread_are_not_zero_imputed():
    data = candles(100)
    data["close"][70] = np.nan
    data["ask_close"][80] = data["bid_close"][80]-.0001
    result = kernel.compute_features(data,"EUR_USD",.0001)
    assert np.isnan(result["m1__return_1_pips"][70:72]).all()
    # Endpoints alone do not establish an observed elapsed path across a bad bar.
    assert math.isnan(result["m1__return_5_pips"][72])
    assert math.isnan(result["m1__historical_spread_pips"][80])
    assert math.isfinite(result["m1__return_1_pips"][80])


def test_registry_matches_values_support_aliases_and_families():
    data = candles(650)
    features = kernel.compute_features(data,"EUR_USD",.0001)
    registry = kernel.feature_registry()
    assert [row["name"] for row in registry] == list(features)
    assert len(features) == len(set(features))
    assert {r["family"] for r in registry} == {"returns","path","volatility","ohlc","oscillator","trend","activity","spread","calendar"}
    aliases = [alias for r in registry for alias in r["aliases"]]
    assert len(aliases) == len(set(aliases))
    assert "return_lag_00_pips" in aliases
    assert "m1__return_lag_00_pips" not in features
    assert kernel.max_lookback_bars() == 603
    for row in registry:
        values = features[row["name"]]
        assert np.isnan(values[:row["lookback_bars"]-1]).all(),row["name"]
        assert row["formula"] and row["source_origin"]
    registry[0]["name"] = "caller_mutation"
    assert kernel.feature_registry()[0]["name"] != "caller_mutation"
    empty = kernel.compute_features(candles(0),"EUR_USD",.0001)
    assert all(len(values) == 0 for values in empty.values())


@pytest.mark.parametrize("fault",["duplicate","off_minute","negative_pip"])
def test_rejects_ambiguous_original_clock_or_pip(fault):
    data = candles(10)
    pip = .0001
    if fault == "duplicate":
        data["time"][4] = data["time"][3]
    elif fault == "off_minute":
        data["time"][4] += 1
    else:
        pip = -.0001
    with pytest.raises(ValueError):
        kernel.compute_features(data,"EUR_USD",pip)
