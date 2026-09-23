from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

try:
    from oanda_ma_feature_grid import (
        FAMILY,
        FORECAST_HORIZONS_SEC,
        MaFeatureGridRuntime,
        TIMEFRAME_SECONDS,
        build_ma_feature_matrix,
        build_ma_feature_vector,
        contract_payload,
        ma_feature_names,
        pair_context_value,
        periods_for_timeframe,
    )
    from oanda_ma_feature_grid_fit import (
        TimeframeDataset,
        apply_time_split_policy,
        decision_centered_probability,
        direction_threshold,
        executable_edge_threshold,
        load_source_frame,
        main as fit_main,
        movement_cost_gate_metrics,
        movement_cost_gate_pass,
        movement_cost_gate_threshold,
    )
except ModuleNotFoundError:
    from trad.oanda_ma_feature_grid import (
        FAMILY,
        FORECAST_HORIZONS_SEC,
        MaFeatureGridRuntime,
        TIMEFRAME_SECONDS,
        build_ma_feature_matrix,
        build_ma_feature_vector,
        contract_payload,
        ma_feature_names,
        pair_context_value,
        periods_for_timeframe,
    )
    from trad.oanda_ma_feature_grid_fit import (
        TimeframeDataset,
        apply_time_split_policy,
        decision_centered_probability,
        direction_threshold,
        executable_edge_threshold,
        load_source_frame,
        main as fit_main,
        movement_cost_gate_metrics,
        movement_cost_gate_pass,
        movement_cost_gate_threshold,
    )


def synthetic_prices(rows: int = 1200) -> np.ndarray:
    index = np.arange(rows, dtype=np.float64)
    changes = (
        0.000015 * np.sin(index / 9.0)
        + 0.000009 * np.sin(index / 31.0)
        + 0.000001 * np.cos(index / 3.0)
    )
    return 1.10 + np.cumsum(changes)


def test_contract_covers_full_timeframe_and_horizon_surface() -> None:
    payload = contract_payload()
    assert payload["family"] == FAMILY
    assert {"S5", "S30", "M1", "M7", "M45", "H1", "H4", "H12", "D1"} <= set(
        payload["timeframes"]
    )
    assert {5, 30, 60, 600, 3600, 14400, 86400} <= set(
        payload["horizons_sec"]
    )
    assert len(TIMEFRAME_SECONDS) * len(FORECAST_HORIZONS_SEC) >= 600
    assert payload["feature_count_by_timeframe"]["M1"] == len(
        ma_feature_names("M1")
    )


def test_ma_features_are_causal_and_continuous() -> None:
    values = synthetic_prices(700)
    position = 499
    before, names = build_ma_feature_matrix(values, [position], 0.0001, "M1")
    changed = values.copy()
    changed[position + 1 :] += np.linspace(0.0, 0.05, len(changed) - position - 1)
    after, changed_names = build_ma_feature_matrix(
        changed,
        [position],
        0.0001,
        "M1",
    )
    assert names == changed_names
    np.testing.assert_allclose(before, after, rtol=0.0, atol=0.0, equal_nan=True)
    assert np.isfinite(before).all()
    assert np.unique(np.round(before, 6)).size > 50


def test_rising_market_has_positive_ma_geometry() -> None:
    values = 1.0 + np.arange(600, dtype=np.float64) * 0.00001
    vector = build_ma_feature_vector(values, 0.0001, "M1")
    assert vector["ma__p8__sma_distance_scale"] > 0.0
    assert vector["ma__p8__ema_distance_scale"] > 0.0
    assert vector["ma__p8__sma_slope3_scale"] > 0.0
    assert vector["ma__p8_21__sma_gap_scale"] > 0.0
    assert vector["ma__sma_price_above_fraction"] == 1.0
    assert vector["ma__ema_positive_slope_fraction"] == 1.0


def test_live_vector_matches_reference_matrix() -> None:
    values = synthetic_prices(700)
    for timeframe in ("S5", "M1", "M15", "H1", "H4", "H12", "D1"):
        matrix, names = build_ma_feature_matrix(
            values,
            [len(values) - 1],
            0.0001,
            timeframe,
        )
        expected = {
            name: round(float(value), 8)
            for name, value in zip(names, matrix[0])
            if np.isfinite(value)
        }
        actual = build_ma_feature_vector(values, 0.0001, timeframe)
        assert actual.keys() == expected.keys()
        np.testing.assert_allclose(
            list(actual.values()),
            list(expected.values()),
            rtol=0.0,
            atol=1e-7,
        )


def test_direction_threshold_centers_validation_decision_score() -> None:
    labels = np.asarray([0] * 40 + [1] * 60, dtype=np.int8)
    probabilities = np.linspace(0.45, 0.65, len(labels))
    threshold = direction_threshold(labels, probabilities)
    centered = decision_centered_probability(
        probabilities,
        threshold,
    )
    assert 0.45 <= threshold <= 0.65
    assert np.isclose(
        decision_centered_probability(np.asarray([threshold]), threshold)[0],
        0.5,
    )
    assert 0.05 <= np.mean(centered >= 0.5) <= 0.95


def test_executable_edge_threshold_uses_only_nonnegative_scores() -> None:
    probability_up = np.full(100, 0.75)
    predicted_long = np.linspace(-1.0, 2.0, 100)
    predicted_short = -predicted_long
    actual_long = predicted_long + 0.25
    actual_short = -actual_long

    threshold = executable_edge_threshold(
        actual_long,
        actual_short,
        probability_up,
        predicted_long,
        predicted_short,
        0.5,
        minimum_rows=10,
    )

    assert threshold >= 0.0
    assert np.sum(predicted_long >= threshold) >= 10


def test_executable_edge_threshold_disables_unsupported_gate() -> None:
    threshold = executable_edge_threshold(
        np.full(100, -1.0),
        np.full(100, -1.0),
        np.full(100, 0.75),
        np.full(100, -0.5),
        np.full(100, -0.5),
        0.5,
        minimum_rows=10,
    )

    assert np.isinf(threshold)


def test_global_time_split_purges_boundary_crossing_labels() -> None:
    row_count = 100
    dataset = TimeframeDataset(
        timeframe="M1",
        features=np.zeros((row_count, 2), dtype=np.float32),
        feature_names=("a", "b"),
        signed_pips=np.zeros((row_count, 1), dtype=np.float32),
        long_net_pips=np.zeros((row_count, 1), dtype=np.float32),
        short_net_pips=np.zeros((row_count, 1), dtype=np.float32),
        decision_cost_pips=np.ones(row_count, dtype=np.float32),
        exact_cost=np.ones((row_count, 1), dtype=bool),
        instruments=np.full(row_count, "EUR_USD", dtype=object),
        timestamps_ns=np.arange(row_count, dtype=np.int64) * 1_000_000_000,
        splits=np.zeros(row_count, dtype=np.int8),
        source_rows=row_count,
        sampled_rows=row_count,
        pair_inventory=[],
    )

    audit = apply_time_split_policy(dataset, "global_time_purged", 10)

    assert audit["purged_rows"] == 20
    assert audit["retained_rows"] == 80
    assert np.bincount(dataset.splits, minlength=3).tolist() == [50, 10, 20]
    development_times = dataset.timestamps_ns[dataset.splits == 0]
    validation_times = dataset.timestamps_ns[dataset.splits == 1]
    assert development_times.max() + 10_000_000_000 < 60_000_000_000
    assert validation_times.max() + 10_000_000_000 < 80_000_000_000


def test_movement_cost_gate_is_fitted_without_future_cost_leakage() -> None:
    row_count = 400
    instruments = np.asarray(
        [f"P{index % 10}" for index in range(row_count)],
        dtype=object,
    )
    phase = np.arange(row_count) % 40
    predicted_magnitude = 0.5 + phase / 10.0
    profitable = predicted_magnitude >= 2.0
    actual_signed = np.where(profitable, 1.0, -1.0)
    actual_long = np.where(profitable, 1.5, -1.0)
    actual_short = np.where(profitable, -3.0, 0.2)
    decision_cost = np.ones(row_count)
    probability_up = np.full(row_count, 0.75)

    gate = movement_cost_gate_threshold(
        actual_long,
        actual_short,
        decision_cost,
        instruments,
        probability_up,
        predicted_magnitude,
        0.5,
    )
    changed_unselected_side_gate = movement_cost_gate_threshold(
        actual_long,
        np.linspace(-500.0, 500.0, row_count),
        decision_cost,
        instruments,
        probability_up,
        predicted_magnitude,
        0.5,
    )
    metrics = movement_cost_gate_metrics(
        actual_signed,
        actual_long,
        actual_short,
        decision_cost,
        np.ones(row_count, dtype=bool),
        instruments,
        probability_up,
        predicted_magnitude,
        0.5,
        gate,
    )

    assert gate["minimum_magnitude_to_cost"] >= 1.0
    assert (
        changed_unselected_side_gate["minimum_magnitude_to_cost"]
        == gate["minimum_magnitude_to_cost"]
    )
    assert (
        changed_unselected_side_gate["minimum_confidence"]
        == gate["minimum_confidence"]
    )
    assert metrics["movement_gate_entry_n"] >= 50
    assert metrics["movement_gate_pair_count"] == 10
    assert metrics["movement_gate_average_net_pips"] > 0.0
    assert metrics["movement_gate_pair_average_net_lower_95"] > 0.0
    assert (
        metrics["movement_gate_pair_average_net_cost_units_lower_95"] > 0.0
    )
    assert movement_cost_gate_pass(metrics)


def test_pair_context_values_are_instrument_specific() -> None:
    assert pair_context_value("context__pair__EUR_USD", "EUR_USD") == 1.0
    assert pair_context_value("context__pair__GBP_USD", "EUR_USD") == 0.0
    assert pair_context_value("context__base__EUR", "EUR_USD") == 1.0
    assert pair_context_value("context__quote__USD", "EUR_USD") == 1.0
    assert pair_context_value("ma__scale_pips", "EUR_USD") is None


def write_synthetic_m1(path: Path, rows: int = 1400) -> None:
    close = synthetic_prices(rows)
    times = pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC")
    spread = 0.00012
    frame = pd.DataFrame(
        {
            "time": times.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetime": times.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "open": np.r_[close[0], close[:-1]],
            "high": close + 0.00004,
            "low": close - 0.00004,
            "close": close,
            "volume": 100 + (np.arange(rows) % 17),
            "bid_open": np.r_[close[0], close[:-1]] - spread / 2.0,
            "bid_high": close + 0.00004 - spread / 2.0,
            "bid_low": close - 0.00004 - spread / 2.0,
            "bid_close": close - spread / 2.0,
            "ask_open": np.r_[close[0], close[:-1]] + spread / 2.0,
            "ask_high": close + 0.00004 + spread / 2.0,
            "ask_low": close - 0.00004 + spread / 2.0,
            "ask_close": close + spread / 2.0,
            "spread_pips": np.full(rows, spread / 0.0001),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_end_to_end_fit_report_and_live_runtime(tmp_path: Path) -> None:
    m1_dir = tmp_path / "m1"
    model_dir = tmp_path / "models"
    report_dir = tmp_path / "reports"
    dataset_cache = tmp_path / "ma_dataset_cache.joblib"
    write_synthetic_m1(m1_dir / "EUR_USD_M1.parquet")

    def fit_arguments(output_models: Path, output_reports: Path) -> list[str]:
        return [
            "--m1-dir",
            str(m1_dir),
            "--s5-dir",
            str(tmp_path / "s5"),
            "--model-dir",
            str(output_models),
            "--report-dir",
            str(output_reports),
            "--timeframes",
            "M1",
            "--horizons-sec",
            "5,60,300",
            "--instruments",
            "EUR_USD",
            "--samples-per-pair-timeframe",
            "500",
            "--minimum-split-rows",
            "20",
            "--target-space",
            "local_scale",
            "--pair-context",
            "pair_and_currencies",
            "--split-policy",
            "global_time_purged",
            "--dataset-cache",
            str(dataset_cache),
            "--no-stamped-copy",
        ]

    status = fit_main(fit_arguments(model_dir, report_dir))
    assert status == 0
    artifact_path = model_dir / "ma_feature_grid_latest.joblib"
    report_path = report_dir / "ma_feature_grid_latest.json"
    artifact = joblib.load(artifact_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert artifact["family"] == FAMILY
    assert artifact["execution_policy"] == "shadow_only"
    assert artifact["models"]["M1"]["horizons_sec"] == [60, 300]
    assert len(artifact["models"]["M1"]["direction_thresholds"]) == 2
    assert artifact["models"]["M1"]["target_space"] == "local_scale"
    assert artifact["models"]["M1"]["pair_context"] == "pair_and_currencies"
    assert artifact["models"]["M1"]["executable_edge_estimator"] is not None
    assert len(artifact["models"]["M1"]["executable_edge_thresholds"]) == 2
    assert len(artifact["models"]["M1"]["movement_cost_gates"]) == 2
    assert any(
        name.startswith("context__pair__")
        for name in artifact["models"]["M1"]["feature_names"]
    )
    assert artifact["models"]["M1"]["target_scale_feature"] == "ma__scale_pips"
    assert len(artifact["models"]["M1"]["target_clip_abs"]) == 2
    assert report["fit"]["target_space"] == "local_scale"
    assert report["fit"]["pair_context"] == "pair_and_currencies"
    assert report["fit"]["executable_edge_head"]
    assert report["fit"]["split_policy"] == "global_time_purged"
    assert report["fit"]["dataset_cache"]["status"] == "written"
    assert dataset_cache.is_file()
    assert report["timeframe_reports"][0]["split_audit"]["purged_rows"] > 0
    assert report["planned_cell_count"] == 3
    assert report["fitted_cell_count"] == 2
    unsupported = [
        row
        for row in report["grid"]
        if row.get("status") == "unsupported_source_resolution"
    ]
    assert unsupported == [
        {
            "horizon_sec": 5,
            "n": 0,
            "split": None,
            "status": "unsupported_source_resolution",
            "timeframe": "M1",
        }
    ]
    holdout = [
        row
        for row in report["grid"]
        if row.get("split") == "holdout"
    ]
    assert {row["horizon_sec"] for row in holdout} == {60, 300}
    assert all(row["n"] >= 20 for row in holdout)
    assert all(row["exact_cost_fraction"] == 1.0 for row in holdout)
    assert all(row["signed_pip_mae"] >= 0.0 for row in holdout)
    assert all(row["magnitude_pip_mae"] >= 0.0 for row in holdout)
    assert all(
        "executable_average_net_cost_units" in row
        and "pair_average_net_cost_units_lower_95" in row
        and "movement_gate_average_net_cost_units" in row
        for row in holdout
    )

    runtime = MaFeatureGridRuntime(artifact_path)
    assert runtime.ready
    values = synthetic_prices(600)
    candidates = runtime.forecast_candidates(
        "EUR_USD",
        {
            "pip": 0.0001,
            "ma_series_by_timeframe": {"M1": values.tolist()},
        },
        bid=float(values[-1] - 0.00006),
        ask=float(values[-1] + 0.00006),
        generated_epoch=1_800_000_000.0,
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["input_timeframe"] == "M1"
    assert candidate["research_only"]
    assert not candidate["account_eligible"]
    assert set(candidate["forecast_curve"]) == {"60", "300"}
    assert all(
        0.0 <= point["probability_up"] <= 1.0
        for point in candidate["forecast_curve"].values()
    )
    assert all(
        "calibrated_probability_up" in point
        and "direction_threshold" in point
        and np.isfinite(point["predicted_signed_pips"])
        and np.isfinite(point["predicted_magnitude_pips"])
        and np.isfinite(point["predicted_long_net_pips"])
        and np.isfinite(point["predicted_short_net_pips"])
        and "executable_edge_pass" in point
        and np.isfinite(point["decision_cost_pips"])
        and np.isfinite(point["predicted_magnitude_to_cost"])
        and "movement_gate_pass" in point
        for point in candidate["forecast_curve"].values()
    )
    batch_runtime = MaFeatureGridRuntime(artifact_path)
    shared_features = {
        "pip": 0.0001,
        "ma_series_by_timeframe": {"M1": values.tolist()},
    }
    batch = batch_runtime.forecast_candidates_batch(
        {
            "EUR_USD": (
                shared_features,
                float(values[-1] - 0.00006),
                float(values[-1] + 0.00006),
            ),
            "GBP_USD": (
                shared_features,
                float(values[-1] - 0.00008),
                float(values[-1] + 0.00008),
            ),
        },
        generated_epoch=1_800_000_000.0,
    )
    assert {row["instrument"] for row in batch} == {"EUR_USD", "GBP_USD"}
    assert all(row["input_timeframe"] == "M1" for row in batch)
    assert all(set(row["forecast_curve"]) == {"60", "300"} for row in batch)
    repeated = batch_runtime.forecast_candidates_batch(
        {
            "EUR_USD": (
                shared_features,
                float(values[-1] - 0.00006),
                float(values[-1] + 0.00006),
            ),
            "GBP_USD": (
                shared_features,
                float(values[-1] - 0.00008),
                float(values[-1] + 0.00008),
            ),
        },
        generated_epoch=1_800_000_120.0,
    )
    assert {row["id"] for row in repeated} == {row["id"] for row in batch}

    cached_model_dir = tmp_path / "cached_models"
    cached_report_dir = tmp_path / "cached_reports"
    assert fit_main(fit_arguments(cached_model_dir, cached_report_dir)) == 0
    cached_report = json.loads(
        (
            cached_report_dir / "ma_feature_grid_latest.json"
        ).read_text(encoding="utf-8")
    )
    assert cached_report["fit"]["dataset_cache"]["status"] == "hit"


def test_period_ceiling_preserves_live_parity() -> None:
    assert max(periods_for_timeframe("M1")) == 200
    assert max(periods_for_timeframe("H4")) == 100
    assert max(periods_for_timeframe("D1")) == 15


def test_observed_s5_midpoint_schema_is_normalized(tmp_path: Path) -> None:
    path = tmp_path / "EUR_USD_S5.parquet"
    times = pd.date_range("2026-01-01", periods=3, freq="5s", tz="UTC")
    pd.DataFrame(
        {
            "time": times.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dt": times.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "mid_open": [1.0, 1.1, 1.2],
            "mid_high": [1.1, 1.2, 1.3],
            "mid_low": [0.9, 1.0, 1.1],
            "mid_close": [1.05, 1.15, 1.25],
            "bid_open": [0.99, 1.09, 1.19],
            "bid_close": [1.04, 1.14, 1.24],
            "ask_open": [1.01, 1.11, 1.21],
            "ask_close": [1.06, 1.16, 1.26],
            "spread_pips": [2.0, 2.0, 2.0],
            "volume": [1, 2, 3],
        }
    ).to_parquet(path, index=False)

    frame = load_source_frame(path)

    assert frame["open"].tolist() == [1.0, 1.1, 1.2]
    assert frame["close"].tolist() == [1.05, 1.15, 1.25]
    assert frame.index.is_monotonic_increasing
