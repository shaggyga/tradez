from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import oanda_shared_panel_model_benchmark as benchmark
import oanda_shared_timeframe_horizon_panel as panel


def synthetic_panel(events: int = 240) -> pd.DataFrame:
    rows = []
    start = pd.Timestamp("2025-01-01T00:00:00Z")
    for index in range(events):
        prediction = start + timedelta(minutes=index)
        long_wins = index % 4 in (0, 1)
        event_id = f"event-{index}"
        for direction, side_sign in (("LONG", 1.0), ("SHORT", -1.0)):
            profitable = long_wins == (direction == "LONG")
            row = {
                "event_id": event_id,
                "side_id": f"{event_id}|{direction}",
                "decision_candle_utc": prediction - timedelta(minutes=1),
                "prediction_time_utc": prediction,
                "maturity_time_utc": prediction + timedelta(minutes=1),
                "max_feature_origin_utc": prediction - timedelta(minutes=1),
                "instrument": "EUR_USD" if index % 2 else "GBP_USD",
                "input_timeframe": "M1",
                "input_timeframe_seconds": 60,
                "horizon_sec": 60,
                "direction": direction,
                "side_sign": side_sign,
                "source": "m1",
                "spread_mode": "observed_bid_ask",
                "feature_version": panel.FEATURE_VERSION,
                "entry_spread_pips": 1.0,
                "realized_net_pips": 1.5 if profitable else -1.0,
                "opposite_net_pips": -1.0 if profitable else 1.5,
                "realized_edge_pips": 2.5 if profitable else -2.5,
                "market_mid_move_pips": 2.0 if long_wins else -2.0,
                "realized_directional_move_pips": 2.0 if profitable else -2.0,
                "target_profitable": int(profitable),
                "target_best_side": int(profitable),
                "hour_sin": np.sin(2 * np.pi * prediction.hour / 24),
                "hour_cos": np.cos(2 * np.pi * prediction.hour / 24),
                "weekday_sin": np.sin(2 * np.pi * prediction.dayofweek / 7),
                "weekday_cos": np.cos(2 * np.pi * prediction.dayofweek / 7),
            }
            for feature in (*panel.BASE_SCALAR_FEATURES, *panel.CROSS_PAIR_FEATURES):
                row[feature] = float((1 if long_wins else -1) * side_sign)
            rows.append(row)
    return pd.DataFrame(rows)


def test_purged_folds_never_train_on_unmatured_outcomes() -> None:
    frame = synthetic_panel()

    folds = benchmark.purged_walk_forward_folds(
        frame, min_train_events=20, min_test_events=10
    )

    assert folds
    for train, test in folds:
        assert train["maturity_time_utc"].max() < test["prediction_time_utc"].min()
        assert set(train["event_id"]).isdisjoint(test["event_id"])


def test_choose_one_action_returns_one_side_per_event() -> None:
    frame = synthetic_panel(20)
    probability = np.where(frame["target_profitable"].to_numpy() == 1, 0.8, 0.2)

    actions = benchmark.choose_one_action(frame, probability)

    assert len(actions) == 20
    assert actions["event_id"].nunique() == 20
    assert actions["target_best_side"].eq(1).all()


def test_threshold_candidates_cover_rare_calibrated_probabilities() -> None:
    probability = np.linspace(0.002, 0.04, 100)

    thresholds = benchmark.candidate_probability_thresholds(probability)

    assert min(thresholds) == probability.min()
    assert any(0.01 < threshold < 0.04 for threshold in thresholds)
    assert 0.5 in thresholds


def test_trade_metrics_reports_infinite_profit_factor_without_losses() -> None:
    frame = synthetic_panel(10)
    winners = frame[frame["target_profitable"] == 1].copy()

    metrics = benchmark.trade_metrics(winners)

    assert np.isinf(metrics["profit_factor"])


def test_model_aliases_are_canonical_and_deduplicated() -> None:
    assert benchmark.canonical_model_names(
        ["logistic", "logistic_baseline", "HGB"]
    ) == ["logistic_baseline", "hist_gradient_boosting"]


def test_bounded_event_sample_preserves_every_cell_and_full_span() -> None:
    frame = synthetic_panel(40)
    second = frame["event_id"].str.removeprefix("event-").astype(int) >= 20
    frame.loc[second, "input_timeframe"] = "M5"
    frame.loc[second, "input_timeframe_seconds"] = 300
    frame.loc[second, "horizon_sec"] = 300

    sampled = benchmark._event_tail(frame, max_events=10)
    events = sampled.drop_duplicates("event_id")
    counts = events.groupby(["input_timeframe", "horizon_sec"], observed=True).size()

    assert sampled["event_id"].nunique() == 10
    assert counts.to_dict() == {("M1", 60): 5, ("M5", 300): 5}
    for keys, source in frame.groupby(["input_timeframe", "horizon_sec"], observed=True):
        selected = events[
            (events["input_timeframe"] == keys[0])
            & (events["horizon_sec"] == keys[1])
        ]
        assert selected["prediction_time_utc"].min() == source["prediction_time_utc"].min()
        assert selected["prediction_time_utc"].max() == source["prediction_time_utc"].max()


def test_bounded_panel_loader_samples_before_concatenation(tmp_path: Path) -> None:
    first = synthetic_panel(20)
    second = synthetic_panel(20)
    second["event_id"] += "-m5"
    second["side_id"] += "-m5"
    second["input_timeframe"] = "M5"
    second["input_timeframe_seconds"] = 300
    second["horizon_sec"] = 300
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"
    panel.write_panel_atomic(first, first_path)
    panel.write_panel_atomic(second, second_path)

    sampled, summary = benchmark.load_panels(
        [first_path, second_path], max_events=10
    )
    counts = (
        sampled.drop_duplicates("event_id")
        .groupby(["input_timeframe", "horizon_sec"], observed=True)
        .size()
    )

    assert summary["events"] == 10
    assert counts.to_dict() == {("M1", 60): 5, ("M5", 300): 5}
    assert "optional_feature_coverage" in summary


def test_prediction_cell_metrics_include_every_forecast_cell() -> None:
    frame = synthetic_panel(40)
    second = frame["event_id"].str.removeprefix("event-").astype(int) >= 20
    frame.loc[second, "input_timeframe"] = "M5"
    frame.loc[second, "horizon_sec"] = 300
    probability = np.where(frame["target_profitable"].to_numpy() == 1, 0.8, 0.2)

    rows = benchmark.prediction_cell_metrics(frame, probability)

    assert [(row["input_timeframe"], row["horizon_sec"]) for row in rows] == [
        ("M1", 60),
        ("M5", 300),
    ]
    assert all(row["auc"] == 1.0 for row in rows)
    assert all(row["best_side_rate"] == 1.0 for row in rows)


def test_prediction_cell_metrics_accept_prospective_s1_panel() -> None:
    frame = synthetic_panel(40)
    frame["input_timeframe"] = "S1"
    frame["input_timeframe_seconds"] = 1
    probability = np.where(frame["target_best_side"].to_numpy() == 1, 0.8, 0.2)

    rows = benchmark.prediction_cell_metrics(frame, probability)

    assert [(row["input_timeframe"], row["horizon_sec"]) for row in rows] == [
        ("S1", 60),
    ]
    assert benchmark.timeframe_seconds("S1") == 1


def test_preprocessor_keeps_features_missing_from_early_fold() -> None:
    source = pd.DataFrame(
        {
            "observed": [1.0, 2.0, 3.0],
            "prospective": [np.nan, np.nan, np.nan],
        }
    )

    transformed = benchmark._preprocessor(
        ["observed", "prospective"],
        [],
        scale=False,
    ).fit_transform(source)

    assert transformed.shape == (3, 2)
    assert np.isfinite(transformed).all()


def test_logistic_baseline_runs_end_to_end(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "shared.parquet"
    panel.write_panel_atomic(synthetic_panel(), source)
    monkeypatch.setattr(benchmark, "REPORT_ROOT", tmp_path / "reports")
    monkeypatch.setattr(benchmark, "MODEL_ROOT", tmp_path / "models")

    report = benchmark.run_benchmark(
        [source],
        ["logistic_baseline"],
        min_train_events=20,
        min_test_events=10,
        persist_artifacts=True,
    )

    assert report["all_requested_evaluated"] is True
    assert report["account_wired"] is False
    assert report["ranking"] == ["logistic_baseline"]
    result = report["results"][0]
    assert result["holdout"]["classification"]["auc"] > 0.9
    assert result["holdout"]["selected"]["mean_net_pips"] > 0.0
    assert result["holdout"]["threshold_selection"]["candidate_count"] > 17
    assert (
        result["holdout"]["threshold_selection"]["status"]
        == "selected_on_calibration"
    )
    assert Path(result["artifact"]["path"]).is_file()
