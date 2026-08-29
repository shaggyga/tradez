from __future__ import annotations

import json
import sys

import pandas as pd

import oanda_m1_recovered_cache_audit as audit


def test_cache_audit_records_complete_pair_and_source(monkeypatch, tmp_path) -> None:
    cache = tmp_path / "cache"
    s5 = tmp_path / "s5"
    cache.mkdir()
    s5.mkdir()
    frame = pd.DataFrame(
        {
            "time": ["2026-01-01T00:00:00Z"],
            "datetime": ["2026-01-01T00:00:00Z"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
            "bid_open": [0.9],
            "bid_high": [0.9],
            "bid_low": [0.9],
            "bid_close": [0.9],
            "ask_open": [1.1],
            "ask_high": [1.1],
            "ask_low": [1.1],
            "ask_close": [1.1],
            "spread_pips": [2.0],
        }
    )
    frame.to_parquet(cache / "USD_JPY_M1.parquet", index=False)
    (s5 / "USD_JPY_S5.parquet").write_bytes(b"discovery only")
    api_summary = tmp_path / "api.json"
    api_summary.write_text(
        json.dumps({"records": [{"instrument": "USD_JPY", "status": "completed"}]}),
        encoding="utf-8",
    )
    output = tmp_path / "audit.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_m1_recovered_cache_audit.py",
            "--cache-dir",
            str(cache),
            "--s5-dir",
            str(s5),
            "--api-summary",
            str(api_summary),
            "--output",
            str(output),
        ],
    )

    assert audit.main() == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["summary"]["fully_covered"] is True
    assert report["instruments"][0]["recovery_source"] == "OANDA_REST_V20_M1_BAM"
