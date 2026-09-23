import numpy as np
import pandas as pd

from seven_major_multihorizon_models import (
    HORIZONS,
    Window,
    exact_future,
    non_overlapping_indices,
    slice_window,
)


def test_exact_future_requires_exact_clock_timestamp():
    times = pd.Series(
        pd.to_datetime(
            [
                "2026-01-02T20:59:00Z",
                "2026-01-04T22:00:00Z",
                "2026-01-04T22:05:00Z",
            ],
            utc=True,
        )
    )
    values = pd.Series([1.0, 2.0, 3.0])
    future = exact_future(values, times, 5)
    assert np.isnan(future[0])
    assert future[1] == 3.0


def test_window_slices_purge_full_maximum_horizon():
    times = pd.date_range("2026-01-01", periods=300, freq="5min", tz="UTC")
    frame = pd.DataFrame({"time_utc": times})
    window = Window(
        window_id=1,
        train_start=times[0],
        train_end=times[120],
        calibration_end=times[200],
        test_end=times[-1] + pd.Timedelta(minutes=5),
    )
    train, calibration, test = slice_window(frame, window, max(HORIZONS))
    assert train["time_utc"].max() < window.train_end - pd.Timedelta(minutes=60)
    assert calibration["time_utc"].max() < window.calibration_end - pd.Timedelta(minutes=60)
    assert test["time_utc"].max() < window.test_end - pd.Timedelta(minutes=60)


def test_trade_selection_is_non_overlapping():
    times = pd.Series(pd.date_range("2026-01-01", periods=10, freq="5min", tz="UTC"))
    selected = non_overlapping_indices(times, np.ones(10, dtype=bool), horizon=15)
    assert selected.tolist() == [0, 3, 6, 9]
