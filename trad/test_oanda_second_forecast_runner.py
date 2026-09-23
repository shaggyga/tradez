from __future__ import annotations

from dataclasses import dataclass

from oanda_second_forecast_runner import (
    compact_runtime_summary,
    shared_forecast_snapshot,
)


@dataclass
class Quote:
    bid: float
    ask: float
    time: str = "2026-07-27T06:00:00Z"


def test_compact_runtime_summary_removes_persisted_matrix_duplicates() -> None:
    summary = {
        "cycles": 10,
        "top_forecasts": [{"instrument": "EUR_USD"}],
        "forecast_curves": [{"instrument": "EUR_USD"}, {"instrument": "USD_JPY"}],
        "multi_horizon_smoothing": {"pair_diagnostics": [{}, {}, {}]},
    }

    compact = compact_runtime_summary(summary)

    assert compact["cycles"] == 10
    assert compact["top_forecasts"] == [{"instrument": "EUR_USD"}]
    assert compact["forecast_curve_pairs"] == 2
    assert compact["smoothing_pair_records"] == 3
    assert "forecast_curves" not in compact
    assert "multi_horizon_smoothing" not in compact


def test_shared_forecast_snapshot_uses_executable_stream_quotes() -> None:
    snapshot = shared_forecast_snapshot(
        {
            "EUR_USD": Quote(1.1, 1.1001),
            "INVALID": Quote(0.0, 0.0),
        },
        1234.5,
    )

    assert snapshot["generated_epoch"] == 1234.5
    assert snapshot["instruments"] == {
        "EUR_USD": {
            "quote": {
                "bid": 1.1,
                "ask": 1.1001,
                "time": "2026-07-27T06:00:00Z",
            }
        }
    }
