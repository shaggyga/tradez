import numpy as np

from all68_endpoint_baseline import _features, _fit_predict, _score


def test_features_use_only_present_and_past_prices():
    time = np.arange(0, 1501, dtype=np.int64) * 60
    close = 100.0 + np.arange(len(time), dtype=float)
    data = {"time": time, "close": close, "bid_close": close - 0.01, "ask_close": close + 0.01}
    features, decision = _features(data)
    before = features[1440].copy()
    data["close"][1441:] *= 1000.0
    after, _ = _features(data)
    assert np.allclose(before, after[1440], equal_nan=True)
    assert decision[1440] == time[1440] + 60


def test_ridge_prediction_has_expected_shape():
    prediction = _fit_predict(np.array([[0.0], [1.0], [2.0]]), np.array([0.0, 1.0, 2.0]), np.array([[1.5], [3.0]]))
    assert prediction.shape == (2,)


def test_no_change_is_an_abstaining_directional_baseline():
    score = _score(np.array([-1.0, 2.0]), np.zeros(2))
    assert score["directional_accuracy_on_nonzero_forecasts"] is None
    assert score["directional_forecast_coverage"] == 0.0
