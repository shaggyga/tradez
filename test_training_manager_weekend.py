from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

import oanda_gpt_training_strategy_manager as manager
from oanda_model_lifecycle import candidate_id


def test_market_hours_distinguish_week_and_weekend() -> None:
    assert not manager.market_is_likely_closed(
        dt.datetime(2026, 6, 17, 15, 0, tzinfo=dt.timezone.utc)
    )
    assert manager.market_is_likely_closed(
        dt.datetime(2026, 6, 20, 15, 0, tzinfo=dt.timezone.utc)
    )
    assert not manager.market_is_likely_closed(
        dt.datetime(2026, 6, 21, 21, 1, tzinfo=dt.timezone.utc)
    )


def test_dataframe_fingerprint_is_content_based() -> None:
    left = pd.DataFrame({"b": [2, 3], "a": [1, 4]})
    right = left[["a", "b"]].copy()

    assert manager.dataframe_fingerprint(left) == manager.dataframe_fingerprint(
        right
    )
    right.loc[0, "a"] = 99
    assert manager.dataframe_fingerprint(left) != manager.dataframe_fingerprint(
        right
    )


def test_all68_pip_location_exceptions_match_training_features() -> None:
    assert manager.pips_multiplier("USD_JPY") == 100.0
    assert manager.pips_multiplier("EUR_HUF") == 100.0
    assert manager.pips_multiplier("USD_THB") == 100.0
    assert manager.pips_multiplier("HKD_JPY") == 10_000.0
    assert manager.pips_multiplier("EUR_USD") == 10_000.0
    assert manager.historical_spread_pips("GBP_HKD") > 5.0


def test_generated_research_specs_use_scale_normalized_outcomes() -> None:
    technical = next(
        manager.generated_experiment_spec(index)
        for index in range(20)
        if manager.generated_experiment_spec(index)["dataset_kind"]
        == "technical_spike"
    )
    base = next(
        manager.generated_experiment_spec(index)
        for index in range(20)
        if manager.generated_experiment_spec(index)["dataset_kind"]
        == "base_trade_quality"
    )

    assert "_net_atr_" in technical["outcome"]
    assert base["outcome"] == "net_vol_units_30"
    assert manager.normalize_gpt_experiment_spec(technical) is not None


def test_major_moves_are_cataloged_as_factor_targets() -> None:
    index = pd.date_range(
        "2026-01-01",
        periods=1_300,
        freq="5min",
        tz="UTC",
    )
    close = np.full(len(index), 1.1000)
    high = close + 0.0001
    low = close - 0.0001
    high[1_101] = 1.1100
    frame = pd.DataFrame(
        {
            "close": close,
            "high": high,
            "low": low,
            "spread_pips": 0.5,
            "atr240_pips": 1.0,
            "future_move_pips_30": 0.0,
        },
        index=index,
    )

    labeled = manager.add_major_move_fields(
        frame,
        "EUR_USD",
        horizons=[30],
    )

    assert np.isnan(labeled.iloc[0]["major_up_30"])
    assert labeled["major_up_30"].fillna(0).max() == 1
    assert labeled["major_event_30"].fillna(0).sum() == 1
    assert "long_trailing_net_atr_30" in labeled
    assert "short_fixed_net_atr_30" in labeled


def test_generated_major_move_models_are_factor_only() -> None:
    factor = next(
        spec
        for index in range(500)
        if (
            (spec := manager.generated_experiment_spec(index)).get(
                "research_role"
            )
            == "major_move_factor"
        )
    )

    assert factor["target"].startswith("major_event_")
    assert factor["research_role"] == "major_move_factor"
    assert factor["direction_target"].startswith("major_direction_up_")
    assert factor["execution_policy"] in {"fixed", "trailing"}
    assert manager.normalize_gpt_experiment_spec(factor) is not None


def test_lifecycle_candidate_identity_ignores_evaluation_stage() -> None:
    screen = manager.generated_experiment_spec(3)
    validation = {
        **screen,
        "evaluation_stage": "validation",
        "validation_weeks": 8,
        "max_train_rows": 250_000,
    }

    assert candidate_id(screen) == candidate_id(validation)
    assert manager.research_spec_hash(screen) != manager.research_spec_hash(
        validation
    )


def test_path_execution_uses_stop_first_when_same_bar_hits_both() -> None:
    index = pd.date_range(
        "2026-01-01",
        periods=60,
        freq="5min",
        tz="UTC",
    )
    frame = pd.DataFrame(
        {
            "close": 1.1000,
            "high": 1.1001,
            "low": 1.0999,
            "spread_pips": 0.2,
            "atr240_pips": 1.0,
            "future_long_net_pips_30": 0.0,
            "future_short_net_pips_30": 0.0,
        },
        index=index,
    )
    frame.loc[index[1], "high"] = 1.1100
    frame.loc[index[1], "low"] = 1.0900

    outcome = manager.path_execution_outcomes(frame, "EUR_USD", 30)

    expected = np.sqrt(6.0)
    assert round(outcome["long_fixed_pips"][0], 6) == round(
        -expected * manager.MAJOR_MOVE_STOP_EXPECTED_UNITS,
        6,
    )
    assert round(outcome["short_fixed_pips"][0], 6) == round(
        -expected * manager.MAJOR_MOVE_STOP_EXPECTED_UNITS,
        6,
    )


def test_rolling_week_validation_uses_prior_weeks_only(tmp_path) -> None:
    rng = np.random.default_rng(42)
    rows = []
    start = pd.Timestamp("2026-01-05", tz="UTC")
    for week in range(7):
        for index in range(1_300):
            momentum = rng.normal()
            spread = rng.uniform(0.5, 2.0)
            probability = 1.0 / (1.0 + np.exp(-momentum))
            target = int(rng.random() < probability)
            rows.append(
                {
                    "time_utc": start
                    + pd.Timedelta(weeks=week, minutes=index),
                    "instrument": f"PAIR_{index % 5}",
                    "spread_pips": spread,
                    "momentum_5_pips": momentum,
                    "momentum_15_pips": momentum + rng.normal(scale=0.2),
                    "momentum_30_pips": momentum + rng.normal(scale=0.3),
                    "accel_pips": rng.normal(),
                    "volatility_30_pips": abs(rng.normal()) + 0.1,
                    "compression_score": rng.uniform(),
                    "move_spread_ratio": abs(momentum) / spread,
                    "signal_score": 50 + momentum * 10,
                    "would_profit_30m": target,
                    "future_30m_pips": (4.0 if target else -3.0) + spread,
                }
            )
    path = tmp_path / "historical_training_set.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    original_reports = manager.DIRS["reports"]
    manager.DIRS["reports"] = tmp_path
    try:
        result = manager.rolling_week_model_validation(
            path,
            min_train_weeks=4,
            max_holdout_weeks=3,
            max_train_rows=20_000,
        )
    finally:
        manager.DIRS["reports"] = original_reports

    assert result["validated"]
    assert result["fold_count"] == 3
    assert all(
        fold["train_rows"] >= 5_200 and fold["test_rows"] == 1_300
        for fold in result["folds"]
    )
