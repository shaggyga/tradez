from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

import oanda_shared_timeframe_horizon_panel as panel
from oanda_s5_timeframe_strategy_replay import TimeframeHistory


def synthetic_history(instrument: str = "EUR_USD") -> TimeframeHistory:
    decision = pd.Timestamp("2026-01-01T00:00:00Z")
    execution_times = list(pd.date_range(decision + timedelta(seconds=5), periods=13, freq="5s"))
    bid_close = [1.1000] * len(execution_times)
    ask_close = [1.1002] * len(execution_times)
    bid_close[5] = 1.1005
    ask_close[5] = 1.1007
    bid_close[11] = 1.0996
    ask_close[11] = 1.0998
    execution = pd.DataFrame(
        {
            "bid_open": [1.1000] * len(execution_times),
            "ask_open": [1.1002] * len(execution_times),
            "bid_close": bid_close,
            "ask_close": ask_close,
        },
        index=execution_times,
    )
    empty = pd.DataFrame(index=[decision])
    return TimeframeHistory(
        instrument=instrument,
        pip=0.0001,
        primary_seconds=5,
        primary=empty,
        higher=empty,
        primary_times=[decision],
        higher_times=[decision],
        primary_candles=[],
        higher_candles=[],
        positions={decision: 0},
        execution=execution,
        execution_seconds=5,
        execution_times=execution_times,
        execution_positions={time: index for index, time in enumerate(execution_times)},
        spread_mode="observed_bid_ask",
    )


def fake_features(instrument, windows, pip):
    features = {
        name: float(index + 1)
        for index, name in enumerate(panel.BASE_SCALAR_FEATURES)
    }
    features.update(
        {
            "instrument": instrument,
            "series_origins": {"M1": "2026-01-01T00:00:00Z"},
        }
    )
    return features, ""


def test_build_panel_has_two_costed_sides_per_event(monkeypatch) -> None:
    history = synthetic_history()
    decision = history.primary_times[0]
    monkeypatch.setattr(history, "windows", lambda value: {"M1": [], "M5": []})
    monkeypatch.setattr(panel, "build_features", fake_features)
    monkeypatch.setattr(panel, "augment_cross_sectional_features", lambda cache: None)

    frame = panel.build_panel_rows(
        {history.instrument: history},
        [decision],
        timeframe="S5",
        source="s5",
        horizons=[30, 60],
    )
    summary = panel.validate_panel(frame)

    assert summary["events"] == 2
    assert summary["rows"] == 4
    assert set(frame["direction"]) == {"LONG", "SHORT"}
    h30 = frame[frame["horizon_sec"] == 30].set_index("direction")
    assert h30.loc["LONG", "realized_net_pips"] == pytest.approx(3.0)
    assert h30.loc["SHORT", "realized_net_pips"] == pytest.approx(-7.0)
    assert h30.loc["LONG", "target_profitable"] == 1
    assert h30.loc["LONG", "target_best_side"] == 1


def test_validate_panel_rejects_future_feature_origin(monkeypatch) -> None:
    history = synthetic_history()
    decision = history.primary_times[0]
    monkeypatch.setattr(history, "windows", lambda value: {"M1": [], "M5": []})
    monkeypatch.setattr(panel, "build_features", fake_features)
    monkeypatch.setattr(panel, "augment_cross_sectional_features", lambda cache: None)
    frame = panel.build_panel_rows(
        {history.instrument: history}, [decision], "S5", "s5", [30]
    )
    frame["max_feature_origin_utc"] = frame["prediction_time_utc"] + timedelta(seconds=1)

    with pytest.raises(ValueError, match="feature provenance"):
        panel.validate_panel(frame)


def test_write_panel_persists_hash_and_shadow_contract(monkeypatch, tmp_path: Path) -> None:
    history = synthetic_history()
    decision = history.primary_times[0]
    monkeypatch.setattr(history, "windows", lambda value: {"M1": [], "M5": []})
    monkeypatch.setattr(panel, "build_features", fake_features)
    monkeypatch.setattr(panel, "augment_cross_sectional_features", lambda cache: None)
    frame = panel.build_panel_rows(
        {history.instrument: history}, [decision], "S5", "s5", [30]
    )
    output = tmp_path / "panel.parquet"

    manifest = panel.write_panel_atomic(frame, output, {"sampling_mode": "test"})

    assert output.is_file()
    assert Path(manifest["manifest_path"]).is_file()
    assert manifest["account_wired"] is False
    assert manifest["panel"]["sha256"] == panel.sha256_file(output)
    assert manifest["model_contract"]["purge_rule"].startswith("training maturity")


def test_canonical_contract_covers_full_matrix() -> None:
    assert len(panel.CANONICAL_TIMEFRAMES) == 13
    assert len(panel.CANONICAL_HORIZONS_SEC) == 16
    assert panel.CANONICAL_HORIZONS_SEC[-4:] == (21600, 28800, 43200, 86400)
    assert len(panel.CANONICAL_TIMEFRAMES) * len(panel.CANONICAL_HORIZONS_SEC) == 208
    assert ("H4", 3600) not in panel.REQUIRED_LONG_CELLS
    assert len(panel.REQUIRED_LONG_CELLS) == 7


def test_hybrid_loader_bounds_observed_s5_history(monkeypatch, tmp_path: Path) -> None:
    history = synthetic_history()
    history.primary = pd.DataFrame(
        index=pd.date_range("2026-01-01T00:00:00Z", periods=2_881, freq="1min")
    )
    observed = synthetic_history()
    requested_tail_rows: list[int] = []

    monkeypatch.setattr(
        panel,
        "load_history_with_fallback",
        lambda **kwargs: (history, "m1_deep", ""),
    )

    def fake_load_history(s5_dir, instrument, primary_seconds, tail_rows, compact):
        requested_tail_rows.append(tail_rows)
        return observed

    monkeypatch.setattr(panel, "load_history", fake_load_history)

    loaded, source, warning = panel.load_panel_history(
        source="hybrid",
        m1_dir=tmp_path,
        m1_cache_dir=tmp_path,
        s5_dir=tmp_path,
        instrument="EUR_USD",
        timeframe_seconds=60,
        tail_rows=2_881,
    )

    assert loaded is history
    assert source == "m1_deep_features_s5_execution"
    assert warning == ""
    assert requested_tail_rows == [51_840]
    assert loaded.execution_seconds == 5
