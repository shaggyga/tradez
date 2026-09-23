from __future__ import annotations

from datetime import timedelta

import pandas as pd

import oanda_shared_panel_neural_benchmark as neural
from test_oanda_shared_panel_model_benchmark import synthetic_panel


def test_trainer_accelerator_allows_explicit_cpu(monkeypatch) -> None:
    monkeypatch.setenv("OANDA_NEURAL_ACCELERATOR", "cpu")
    assert neural.trainer_accelerator() == "cpu"


def test_only_gpu_tft_uses_warn_determinism() -> None:
    assert neural.trainer_determinism("tft", "gpu") == "warn"
    assert neural.trainer_determinism("tft", "cpu") is True
    assert neural.trainer_determinism("deepar", "gpu") is True


def test_maturity_tracks_never_expose_overlapping_targets() -> None:
    frame = synthetic_panel(80)
    frame["maturity_time_utc"] = frame["prediction_time_utc"] + timedelta(minutes=7)

    tracked = neural.assign_maturity_tracks(frame)

    assert tracked["maturity_track"].max() > 0
    neural.validate_maturity_tracks(tracked)
    for _, group in tracked.groupby("sequence_id", observed=True):
        ordered = group.sort_values("prediction_time_utc")
        assert (
            ordered["maturity_time_utc"].shift(1).dropna().to_numpy()
            <= ordered["prediction_time_utc"].iloc[1:].to_numpy()
        ).all()


def test_window_samples_are_fixed_length_and_target_aligned() -> None:
    tracked = neural.assign_maturity_tracks(synthetic_panel(120))

    windows, targets = neural.build_window_samples(tracked, encoder_length=4)

    assert not windows.empty
    assert windows.groupby("sample_id", observed=True).size().eq(5).all()
    last_rows = windows.sort_values("sample_time_idx").groupby("sample_id", observed=True).tail(1)
    aligned = last_rows.set_index("sample_id")["side_id"].sort_index()
    expected = targets.set_index("sample_id")["side_id"].sort_index()
    pd.testing.assert_series_equal(aligned, expected, check_names=False)


def test_prepared_neural_splits_are_chronological_and_nonempty() -> None:
    prepared = neural.prepare_neural_frames(
        synthetic_panel(240), encoder_length=4, max_samples_per_split=50
    )

    train = prepared["targets"]["train"]
    calibration = prepared["targets"]["calibration"]
    holdout = prepared["targets"]["holdout"]
    assert prepared["sample_counts"]["train"] <= 50
    assert prepared["sample_counts"]["calibration"] <= 50
    assert prepared["sample_counts"]["holdout"] <= 50
    assert train["maturity_time_utc"].max() < calibration["prediction_time_utc"].max()
    assert calibration["maturity_time_utc"].max() < holdout["prediction_time_utc"].min()


def test_limited_builder_materializes_only_selected_windows() -> None:
    tracked = neural.assign_maturity_tracks(synthetic_panel(240))

    windows, targets, split_ids = neural.build_limited_window_samples(
        tracked,
        encoder_length=4,
        max_samples_per_split=20,
    )

    assert all(len(ids) <= 20 for ids in split_ids.values())
    assert len(targets) == sum(len(ids) for ids in split_ids.values())
    assert len(windows) == len(targets) * 5


def test_stratified_limit_preserves_pair_timeframe_horizon_groups() -> None:
    rows = []
    for instrument, timeframe, horizon in (
        ("EUR_USD", "M1", 300),
        ("USD_JPY", "M5", 3600),
        ("GBP_USD", "H1", 86400),
        ("AUD_CAD", "H4", 86400),
    ):
        for index, timestamp in enumerate(
            pd.date_range("2026-01-01", periods=10, freq="h", tz="UTC")
        ):
            rows.append(
                {
                    "prediction_time_utc": timestamp,
                    "maturity_time_utc": timestamp + timedelta(hours=1),
                    "instrument": instrument,
                    "input_timeframe": timeframe,
                    "horizon_sec": horizon,
                    "tracked_row_id": len(rows),
                }
            )
    frame = pd.DataFrame(rows)

    limited = neural._stratified_time_limit(frame, 8)

    assert len(limited) == 8
    assert limited.groupby(
        ["instrument", "input_timeframe", "horizon_sec"], observed=True
    ).ngroups == 4


def test_matched_logistic_uses_same_encoder_eligible_samples() -> None:
    prepared = neural.prepare_neural_frames(synthetic_panel(240), encoder_length=4)

    result = neural.evaluate_matched_logistic(prepared)

    assert result["model"] == "logistic_baseline"
    assert result["classification"]["auc"] > 0.9
    assert result["all_actions"]["trades"] > 0
