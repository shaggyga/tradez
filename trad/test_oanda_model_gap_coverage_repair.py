from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import oanda_model_gap_coverage_repair as repair


def test_parse_supplement_restricts_target_to_filename(tmp_path: Path) -> None:
    target, source = repair.parse_supplement(f"panel.parquet={tmp_path / 'source.parquet'}")
    assert target == "panel.parquet"
    assert source == (tmp_path / "source.parquet").resolve()
    with pytest.raises(Exception):
        repair.parse_supplement(f"nested/panel.parquet={tmp_path / 'source.parquet'}")


def test_select_missing_rows_keeps_only_requested_pair_cells() -> None:
    frame = pd.DataFrame(
        {
            "instrument": ["TRY_JPY", "TRY_JPY", "EUR_USD"],
            "input_timeframe": ["S5", "S5", "S5"],
            "horizon_sec": [60, 300, 60],
            "side_id": ["a", "b", "c"],
        }
    )

    selected = repair.select_missing_rows(frame, {("TRY_JPY", "S5", 60)})

    assert selected["side_id"].tolist() == ["a"]


def test_specs_from_state_deduplicates_retried_output() -> None:
    run = {
        "timeframe": "H4",
        "source": "m1",
        "horizons_sec": [21600, 28800, 43200, 86400],
        "sampling": "uniform",
        "cycles": 64,
        "tail_rows": 0,
        "output": "panel_h4_extension.parquet",
        "role": "canonical_horizon_extension",
    }

    specs = repair.specs_from_state(
        {"runs": [{**run, "status": "failed"}, {**run, "status": "completed"}]}
    )

    assert len(specs) == 1
    assert specs[0].output == "panel_h4_extension.parquet"


def test_latest_failed_runs_ignores_superseded_failure() -> None:
    state = {
        "runs": [
            {"output": "retried.parquet", "status": "failed"},
            {"output": "still-failed.parquet", "status": "failed"},
            {"output": "retried.parquet", "status": "completed"},
        ]
    }

    failures = repair.latest_failed_runs(state)

    assert [row["output"] for row in failures] == ["still-failed.parquet"]


def test_pair_cells_and_records_are_stable() -> None:
    frame = pd.DataFrame(
        {
            "instrument": ["USD_TRY", "EUR_TRY", "USD_TRY"],
            "input_timeframe": ["H1", "H1", "H1"],
            "horizon_sec": [43200, 43200, 43200],
        }
    )

    cells = repair.pair_cells(frame)

    assert cells == {("EUR_TRY", "H1", 43200), ("USD_TRY", "H1", 43200)}
    assert repair.cell_records(cells) == [
        {"instrument": "EUR_TRY", "input_timeframe": "H1", "horizon_sec": 43200},
        {"instrument": "USD_TRY", "input_timeframe": "H1", "horizon_sec": 43200},
    ]
