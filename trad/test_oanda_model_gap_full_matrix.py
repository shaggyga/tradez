from __future__ import annotations

import oanda_model_gap_full_matrix as matrix
import oanda_shared_timeframe_horizon_panel as panel


def test_full_matrix_specs_cover_every_canonical_cell() -> None:
    specs = matrix.build_specs(
        list(panel.CANONICAL_TIMEFRAMES),
        list(panel.CANONICAL_HORIZONS_SEC),
        cycles=500,
        supplement_cycles=500,
        include_subminute_supplement=True,
    )
    observed = {
        (spec.timeframe, horizon)
        for spec in specs
        for horizon in spec.horizons_sec
    }
    expected = {
        (timeframe, horizon)
        for timeframe in panel.CANONICAL_TIMEFRAMES
        for horizon in panel.CANONICAL_HORIZONS_SEC
    }

    assert len(specs) == 22
    assert observed == expected
    assert all(spec.tail_rows == 0 for spec in specs if spec.role == "full_span_primary")
    assert all(spec.tail_rows > 0 for spec in specs if spec.source == "hybrid")


def test_completion_status_requires_pair_cell_coverage() -> None:
    assert matrix.completion_status([], {"fully_covered": True}) == "complete"
    assert (
        matrix.completion_status([], {"fully_covered": False})
        == "incomplete_coverage"
    )
    assert (
        matrix.completion_status([{"status": "failed"}], {"fully_covered": True})
        == "complete_with_errors"
    )
    assert (
        matrix.completion_status(
            [], {"fully_covered": False, "contract_complete": True}
        )
        == "complete_with_documented_unavailable"
    )
