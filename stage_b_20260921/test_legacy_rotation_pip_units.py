"""Focused all-68 regression tests for the isolated legacy rotation repair."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import oanda_indicator_alignment_research as alignment
import oanda_primary_forecast_rotation_bot as rotation


def _metadata():
    return json.loads((Path(alignment.PROJECT_ROOT) / "config/pair_local_operational_v2_20260913.json").read_text())["pairs"]


def test_all_68_registered_pip_sizes_match_the_reviewed_metadata():
    expected = {name: float(value["pip_size"]) for name, value in _metadata().items()}
    assert len(expected) == 68
    assert alignment.registered_pip_sizes() == expected
    assert {pair: alignment.pips_size(pair) for pair in expected} == expected


@pytest.mark.parametrize(
    ("pair", "expected", "legacy_shortcut"),
    [
        ("EUR_HUF", 0.01, 0.0001),
        ("HKD_JPY", 0.0001, 0.01),
        ("USD_HUF", 0.01, 0.0001),
        ("USD_THB", 0.01, 0.0001),
    ],
)
def test_exceptional_pair_units_reject_the_old_quote_currency_shortcut(pair, expected, legacy_shortcut):
    assert alignment.pips_size(pair) == expected
    assert alignment.pips_size(pair) != legacy_shortcut


@pytest.mark.parametrize("pair", ["EUR_HUF", "HKD_JPY", "USD_HUF", "USD_THB"])
def test_rotation_synthetic_quote_and_observed_quote_spreads_use_registered_units(pair):
    pip = alignment.pips_size(pair)
    frame = pd.DataFrame({"close": [100.0], "spread_pips": [2.0]})
    synthetic = rotation.price_from_frame(frame, pair)
    observed = rotation.price_from_oanda({"bids": [{"price": "99.99"}], "asks": [{"price": "100.01"}]}, 100.0, pair)
    assert synthetic["ask"] - synthetic["bid"] == pytest.approx(2.0 * pip)
    assert observed["spread_pips"] == pytest.approx(0.02 / pip)


def test_atr_and_price_delta_are_measured_in_the_registered_pips_for_huf_pair():
    count = 250
    frame = pd.DataFrame({
        "datetime": pd.date_range("2026-01-01", periods=count, freq="min", tz="UTC"),
        "open": np.full(count, 100.0),
        "high": np.full(count, 100.02),
        "low": np.full(count, 99.98),
        "close": np.full(count, 100.0),
        "volume": np.ones(count),
        "spread_pips": np.full(count, 2.0),
    })
    features = alignment.add_base_features(frame, "EUR_HUF")
    assert features["range_pips"].iloc[-1] == pytest.approx(4.0)
    assert features["atr_30_pips"].iloc[-1] == pytest.approx(4.0)
    assert features["spread_to_atr30"].iloc[-1] == pytest.approx(0.5)


def test_unregistered_pair_fails_closed():
    with pytest.raises(ValueError, match="unregistered_instrument_pip_size"):
        alignment.pips_size("AAA_BBB")
