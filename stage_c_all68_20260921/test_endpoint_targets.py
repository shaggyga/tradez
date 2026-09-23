import pytest

from endpoint_targets import endpoint_outcomes


def test_endpoint_target_is_available_despite_a_missing_intermediate_minute():
    data = {"time": [0, 60, 180], "close": [100.0, 101.0, 102.0], "bid_close": [99.9, 100.9, 101.9], "ask_close": [100.1, 101.1, 102.1]}
    result = endpoint_outcomes(data, 3)
    assert result["state"][0] == "endpoint_available"
    assert result["midpoint_return_bps"][0] == pytest.approx(200.0)
    assert result["assumed_available_epoch"][0] == 240


def test_missing_endpoint_stays_missing_and_never_becomes_zero_return():
    data = {"time": [0, 60], "close": [100.0, 101.0], "bid_close": [99.9, 100.9], "ask_close": [100.1, 101.1]}
    result = endpoint_outcomes(data, 3)
    assert result["state"][0] == "missing_target"
    assert str(result["midpoint_return_bps"][0]) == "nan"
