from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

import oanda_m1_api_recovery as recovery


class FakeClient:
    def __init__(self) -> None:
        self.calls = 0

    def candles(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            times = ["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"]
        else:
            times = ["2025-12-31T23:58:00Z", "2025-12-31T23:59:00Z"]
        return {
            "candles": [
                {
                    "complete": True,
                    "time": stamp,
                    "volume": 5,
                    "mid": {"o": "1.0", "h": "1.1", "l": "0.9", "c": "1.0"},
                    "bid": {"o": "0.9999", "h": "1.0999", "l": "0.8999", "c": "0.9999"},
                    "ask": {"o": "1.0001", "h": "1.1001", "l": "0.9001", "c": "1.0001"},
                }
                for stamp in times
            ]
        }


def test_api_recovery_writes_new_parquet_without_local_input(tmp_path) -> None:
    record = recovery.recover_pair(
        FakeClient(),
        "EUR_USD",
        cutoff=datetime(2025, 12, 31, 23, 58, tzinfo=timezone.utc),
        batch_size=5000,
        pause_seconds=0.0,
        output_dir=tmp_path,
    )

    frame = pd.read_parquet(tmp_path / "EUR_USD_M1.parquet")
    assert record["requests"] == 2
    assert record["rows"] == 4
    assert frame.columns.tolist() == list(recovery.M1_COLUMNS)
    assert frame["datetime"].iloc[0].startswith("2025-12-31T23:58")


def test_s5_api_recovery_writes_loader_compatible_schema(tmp_path) -> None:
    record = recovery.recover_pair(
        FakeClient(),
        "EUR_USD",
        cutoff=datetime(2025, 12, 31, 23, 58, tzinfo=timezone.utc),
        batch_size=5000,
        pause_seconds=0.0,
        output_dir=tmp_path,
        granularity="S5",
    )

    frame = pd.read_parquet(tmp_path / "EUR_USD_S5.parquet")
    assert record["source_granularity"] == "S5"
    assert frame.columns.tolist() == list(recovery.S5_COLUMNS)
    assert frame["dt"].iloc[0].startswith("2025-12-31T23:58")
    assert frame["mid_close"].iloc[0] == frame["bid_close"].iloc[0] + 0.0001
