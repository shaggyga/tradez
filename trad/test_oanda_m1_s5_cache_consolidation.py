from __future__ import annotations

import pandas as pd

import oanda_m1_s5_cache_consolidation as consolidation


def test_consolidation_appends_recent_s5_and_prefers_observed_overlap(tmp_path) -> None:
    m1_dir = tmp_path / "m1"
    cache_dir = tmp_path / "cache"
    s5_dir = tmp_path / "s5"
    output_dir = tmp_path / "output"
    for path in (m1_dir, cache_dir, s5_dir):
        path.mkdir()
    old = pd.DataFrame(
        {
            "time": ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"],
            "datetime": ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"],
            "open": [1.0, 1.1],
            "high": [1.01, 1.11],
            "low": [0.99, 1.09],
            "close": [1.0, 1.1],
            "volume": [10, 10],
        }
    )
    old.to_parquet(cache_dir / "EUR_USD_M1.parquet", index=False)
    s5 = pd.DataFrame(
        {
            "dt": pd.to_datetime(
                ["2026-01-01T00:01:00Z", "2026-01-01T00:01:05Z", "2026-01-01T00:02:00Z"]
            ),
            "mid_open": [2.0, 2.1, 3.0],
            "mid_high": [2.05, 2.15, 3.05],
            "mid_low": [1.95, 2.05, 2.95],
            "mid_close": [2.02, 2.12, 3.02],
            "bid_open": [1.9999, 2.0999, 2.9999],
            "bid_high": [2.0499, 2.1499, 3.0499],
            "bid_low": [1.9499, 2.0499, 2.9499],
            "bid_close": [2.0199, 2.1199, 3.0199],
            "ask_open": [2.0001, 2.1001, 3.0001],
            "ask_high": [2.0501, 2.1501, 3.0501],
            "ask_low": [1.9501, 2.0501, 2.9501],
            "ask_close": [2.0201, 2.1201, 3.0201],
            "spread_pips": [2.0, 2.0, 2.0],
            "volume": [2, 3, 4],
        }
    )
    s5.to_parquet(s5_dir / "EUR_USD_S5.parquet", index=False)

    record = consolidation.consolidate_pair(
        "EUR_USD",
        m1_dir=m1_dir,
        cache_dir=cache_dir,
        s5_dir=s5_dir,
        output_dir=output_dir,
    )

    result = pd.read_parquet(output_dir / "EUR_USD_M1.parquet")
    assert record["combined_rows"] == 3
    assert result["datetime"].tolist() == [
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:01:00Z",
        "2026-01-01T00:02:00Z",
    ]
    assert result.loc[1, "open"] == 2.0
    assert result.loc[1, "high"] == 2.15
    assert result.loc[1, "volume"] == 5
