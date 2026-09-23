from __future__ import annotations

import numpy as np
import pandas as pd

import oanda_model_gap_market_validation as market


def _events(rows: int = 240) -> pd.DataFrame:
    prediction = pd.date_range("2026-01-01", periods=rows, freq="h", tz="UTC")
    move = np.where(np.arange(rows) % 3, 1.5, -2.0)
    return pd.DataFrame(
        {
            "event_id": [f"event-{index}" for index in range(rows)],
            "prediction_time_utc": prediction,
            "maturity_time_utc": prediction + np.timedelta64(5, "m"),
            "max_feature_origin_utc": prediction,
            "instrument": np.where(np.arange(rows) % 2, "EUR_USD", "USD_JPY"),
            "input_timeframe": "M1",
            "input_timeframe_seconds": 60,
            "horizon_sec": 300,
            "market_mid_move_pips": move,
            "long_net_pips": move - 0.5,
            "short_net_pips": -move - 0.5,
            "target_up": (move > 0).astype(int),
        }
    )


def test_runnable_gap_inventory_is_exact() -> None:
    assert len(market.RUNNABLE_MODELS) == 17
    assert len(set(market.RUNNABLE_MODELS)) == 17
    assert "timesfm_icf" not in market.RUNNABLE_MODELS
    assert "mamba" not in market.RUNNABLE_MODELS


def test_default_forecast_scope_is_the_full_canonical_matrix() -> None:
    expected = {
        (timeframe, horizon)
        for timeframe in market.panel.CANONICAL_TIMEFRAMES
        for horizon in market.panel.CANONICAL_HORIZONS_SEC
    }

    assert len(market.DEFAULT_FORECAST_CELLS) == 208
    assert set(market.DEFAULT_FORECAST_CELLS) == expected
    assert set(market.SMOKE_FORECAST_CELLS) < expected


def test_graph_route_uses_requested_forecast_cells(monkeypatch, tmp_path) -> None:
    requested = (("S5", 30), ("M1", 300), ("H4", 86400))
    evaluated: list[tuple[str, int]] = []
    monkeypatch.setattr(
        market,
        "load_bounded_market_panel",
        lambda *_args, **_kwargs: (pd.DataFrame(), {"events": 0}),
    )
    graph_events = _events(len(requested))
    graph_events["input_timeframe"] = [cell[0] for cell in requested]
    graph_events["horizon_sec"] = [cell[1] for cell in requested]
    monkeypatch.setattr(market, "event_table", lambda _frame: graph_events)

    def evaluate(_events_frame, _steps, *, cell):
        evaluated.append(cell)
        return {"cell": cell, "status": "bounded_market_scored"}

    monkeypatch.setattr(market, "evaluate_cross_pair_graph", evaluate)
    monkeypatch.setattr(
        market,
        "combine_cell_results",
        lambda name, _results: {
            "model": name,
            "status": "bounded_market_scored",
            "cells": [],
            "validation": {
                "backtest_valid": True,
                "performance_passed": False,
            },
        },
    )

    report = market.run_validation(
        [tmp_path / "unused.parquet"],
        models=("temporal_cross_pair_gnn",),
        forecast_cells=requested,
        output=tmp_path / "report.json",
    )

    assert evaluated == list(requested)
    assert report["configuration"]["forecast_cells"] == [
        {"input_timeframe": timeframe, "horizon_sec": horizon}
        for timeframe, horizon in requested
    ]


def test_graph_cell_aggregation_returns_auditable_result() -> None:
    results = []
    for timeframe, horizon in (("M1", 300), ("H1", 3600)):
        results.append(
            {
                "status": "bounded_market_scored",
                "backtest_scope": {
                    "cells": [
                        {"input_timeframe": timeframe, "horizon_sec": horizon}
                    ]
                },
                "selected": {"trades": 5},
                "cells": [
                    {
                        "input_timeframe": timeframe,
                        "horizon_sec": horizon,
                        "events": 10,
                        "trades": 5,
                        "direction_accuracy": 0.6,
                        "win_rate": 0.6,
                        "mean_net_pips": 0.2,
                        "median_net_pips": 0.1,
                        "sum_net_pips": 1.0,
                        "profit_factor": 1.2,
                        "max_drawdown_pips": 0.5,
                    }
                ],
                "validation": {"backtest_valid": True},
            }
        )

    combined = market.combine_cell_results("temporal_cross_pair_gnn", results)

    assert combined["status"] == "bounded_market_scored"
    assert combined["selected"]["events"] == 20
    assert combined["selected"]["trades"] == 10
    assert len(combined["cells"]) == 2
    assert len(combined["cell_runs"]) == 2


def test_temporal_split_purges_unmatured_training_labels() -> None:
    split = market.temporal_event_split(_events())
    assert split["train"]["maturity_time_utc"].max() < split["calibration"][
        "prediction_time_utc"
    ].min()
    assert split["calibration"]["maturity_time_utc"].max() < split["holdout"][
        "prediction_time_utc"
    ].min()


def test_spread_aware_exposure_uses_observed_side_outcomes() -> None:
    events = _events(3)
    scored = market.score_exposure(events, np.asarray([1.0, -1.0, 0.5]))
    assert scored.loc[0, "realized_net_pips"] == events.loc[0, "long_net_pips"]
    assert scored.loc[1, "realized_net_pips"] == events.loc[1, "short_net_pips"]
    assert scored.loc[2, "realized_net_pips"] == 0.5 * events.loc[2, "long_net_pips"]


def test_performance_gate_rejects_concentrated_cell_profit() -> None:
    events = _events(240)
    cells = (("S5", 30), ("M1", 300), ("M5", 3600), ("H1", 14400))
    for index, (timeframe, horizon) in enumerate(cells):
        mask = np.arange(len(events)) // 60 == index
        events.loc[mask, "input_timeframe"] = timeframe
        events.loc[mask, "horizon_sec"] = horizon
    exposure = np.ones(len(events), dtype=float)
    scored = market.score_exposure(events, exposure)
    scored.loc[scored["input_timeframe"] == "S5", "realized_net_pips"] = 20.0
    scored.loc[scored["input_timeframe"] != "S5", "realized_net_pips"] = -1.0

    validation = market._validation_block(
        scored,
        {"auc": 0.70},
        minimum_events=200,
        minimum_instruments=2,
        minimum_cells=4,
    )

    assert validation["performance_checks"]["positive_mean_net_pips"] is True
    assert validation["performance_checks"]["positive_cell_majority"] is False
    assert validation["performance_passed"] is False
    assert validation["stability"]["positive_active_cells"] == 1


def test_performance_gate_accepts_broad_cell_profit() -> None:
    events = _events(240)
    cells = (("S5", 30), ("M1", 300), ("M5", 3600), ("H1", 14400))
    for index, (timeframe, horizon) in enumerate(cells):
        mask = np.arange(len(events)) // 60 == index
        events.loc[mask, "input_timeframe"] = timeframe
        events.loc[mask, "horizon_sec"] = horizon
    scored = market.score_exposure(events, np.ones(len(events), dtype=float))
    scored["realized_net_pips"] = 1.0
    for index in range(0, len(scored), 60):
        scored.loc[index, "realized_net_pips"] = -0.1

    validation = market._validation_block(
        scored,
        {"auc": 0.70},
        minimum_events=200,
        minimum_instruments=2,
        minimum_cells=4,
    )

    assert validation["performance_checks"]["broad_cell_activity"] is True
    assert validation["performance_checks"]["positive_cell_majority"] is True
    assert validation["performance_checks"]["positive_timeframe_majority"] is True
    assert validation["performance_passed"] is True


def test_forecast_fixture_keeps_maturity_gap_out_of_training() -> None:
    rows = []
    for instrument in ("EUR_USD", "USD_JPY"):
        prediction = pd.date_range("2026-01-01", periods=24, freq="min", tz="UTC")
        for index, timestamp in enumerate(prediction):
            move = float(np.sin(index / 3.0))
            rows.append(
                {
                    "event_id": f"{instrument}-{index}",
                    "prediction_time_utc": timestamp,
                    "maturity_time_utc": timestamp + np.timedelta64(30, "s"),
                    "max_feature_origin_utc": timestamp,
                    "instrument": instrument,
                    "input_timeframe": "S5",
                    "input_timeframe_seconds": 5,
                    "horizon_sec": 30,
                    "market_mid_move_pips": move,
                    "long_net_pips": move - 0.2,
                    "short_net_pips": -move - 0.2,
                    "target_up": int(move > 0.0),
                }
            )
    events = pd.DataFrame(rows)
    fixture = market.prepare_forecast_fixture(
        events,
        cells=(("S5", 30),),
        series_points=20,
        score_steps=4,
        input_size=4,
    )
    train = fixture.metadata[fixture.metadata["ds"] < fixture.train_rows]
    scored = fixture.metadata[fixture.metadata["ds"] >= fixture.calibration_ds_min]
    assert train["maturity_time_utc"].max() < scored["prediction_time_utc"].min()
    assert fixture.forecast_steps >= 4
