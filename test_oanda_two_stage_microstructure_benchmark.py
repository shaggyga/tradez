from pathlib import Path

import numpy as np

import oanda_shared_timeframe_horizon_panel as panel
import oanda_two_stage_microstructure_benchmark as two_stage
from test_oanda_shared_panel_model_benchmark import synthetic_panel


def panel_with_flat_events(events: int = 280):
    frame = synthetic_panel(events)
    event_number = frame["event_id"].str.removeprefix("event-").astype(int)
    flat = event_number % 5 == 4
    frame.loc[flat, "realized_net_pips"] = -0.5
    frame.loc[flat, "target_profitable"] = 0
    return frame


def test_event_frame_separates_movement_and_direction_targets() -> None:
    events = two_stage.build_event_frame(panel_with_flat_events(20))

    assert len(events) == 20
    assert events["target_movement"].sum() == 16
    assert events["target_long"].nunique() == 2
    numeric, categorical = two_stage.event_model_features(events)
    assert "side_sign" not in numeric
    assert "direction" not in categorical


def test_score_actions_uses_direction_head_for_side() -> None:
    events = two_stage.build_event_frame(panel_with_flat_events(10))
    direction = events["target_long"].to_numpy(float)
    actions = two_stage.score_actions(
        events,
        np.full(len(events), 0.7),
        direction,
    )

    assert actions["target_best_side"].eq(1).all()
    assert np.allclose(
        actions["realized_net_pips"],
        np.maximum(actions["long_net_pips"], actions["short_net_pips"]),
    )


def test_training_tail_keeps_the_newest_requested_fraction() -> None:
    events = two_stage.build_event_frame(panel_with_flat_events(20))

    tail = two_stage.training_tail(events, 0.25)

    assert len(tail) == 5
    assert tail["prediction_time_utc"].min() > events["prediction_time_utc"].min()
    assert tail["prediction_time_utc"].max() == events["prediction_time_utc"].max()


def test_two_stage_runs_end_to_end_without_account_authorization(
    tmp_path: Path,
) -> None:
    source = tmp_path / "panel.parquet"
    panel.write_panel_atomic(panel_with_flat_events(), source)

    result = two_stage.run_two_stage(
        [source],
        min_train_events=20,
        min_test_events=10,
        persist_artifact=False,
        report_root=tmp_path / "reports",
        model_root=tmp_path / "models",
    )

    assert result["account_wired"] is False
    assert result["holdout"]["account_eligible"] is False
    assert result["walk_forward"]["folds"]
    assert result["calibration"]["direction_window_selection"][
        "selected_fraction"
    ] in (1.0, 0.5, 0.25)
    assert Path(result["report_paths"][1]).is_file()
