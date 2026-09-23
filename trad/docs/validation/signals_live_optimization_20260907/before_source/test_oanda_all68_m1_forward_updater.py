import argparse
import datetime as dt
import json
from pathlib import Path

import oanda_all68_m1_forward_updater as updater


def test_recent_archive_uses_gap_sized_request_not_full_5000():
    reference = dt.datetime(2026, 8, 28, 12, 0, tzinfo=dt.timezone.utc)
    assert updater.forward_request_count(
        updater.pd.Timestamp("2026-08-28T11:54:00Z"),
        batch_size=5000,
        reference_time=reference,
    ) == 10
    assert updater.forward_request_count(
        updater.pd.Timestamp("2026-08-28T10:00:00Z"),
        batch_size=5000,
        reference_time=reference,
    ) == 123


def test_missing_or_very_stale_archive_keeps_recovery_batch():
    reference = dt.datetime(2026, 8, 28, 12, 0, tzinfo=dt.timezone.utc)
    assert updater.forward_request_count(
        None, batch_size=5000, reference_time=reference
    ) == 5000
    assert updater.forward_request_count(
        updater.pd.Timestamp("2026-08-01T00:00:00Z"),
        batch_size=5000,
        reference_time=reference,
    ) == 5000


def test_atomic_report_replaces_complete_json(tmp_path: Path):
    path = tmp_path / "state" / "report.json"
    updater.write_json_atomic(path, {"schema": 1, "rows": [1, 2, 3]})
    assert json.loads(path.read_text(encoding="utf-8"))["rows"] == [1, 2, 3]
    assert not list(path.parent.glob("*.tmp"))


def test_priced_instruments_uses_only_pair_shaped_quotes(tmp_path: Path):
    path = tmp_path / "quotes.json"
    path.write_text(
        json.dumps(
            {
                "quotes": {
                    "EUR_USD": {"bid": 1.1},
                    "USD_JPY": {"bid": 150.0},
                    "INVALID": {"bid": 1.0},
                    "GBP_USD": None,
                }
            }
        ),
        encoding="utf-8",
    )
    assert updater.priced_instruments(path) == ["EUR_USD", "USD_JPY"]


def test_run_once_writes_health_report_without_account_or_order_surface(
    tmp_path: Path, monkeypatch
):
    candle = tmp_path / "EUR_USD_M1.csv"
    candle.write_text("time,datetime\n", encoding="utf-8")
    report = tmp_path / "report.json"
    monkeypatch.setattr(updater, "candle_files", lambda: [candle])
    monkeypatch.setattr(
        updater,
        "resolve_readonly_oanda_client",
        lambda: (object(), {"environment": "practice", "base_url": "https://api-fxpractice.oanda.com"}),
    )
    monkeypatch.setattr(
        updater,
        "update_pair",
        lambda *args, **kwargs: {
            "instrument": "EUR_USD",
            "rows_appended": 2,
            "rows_backfilled": 0,
            "error": "",
        },
    )
    args = argparse.Namespace(
        pairs=[],
        bootstrap_all_priced=False,
        max_requests_per_pair=1,
        backfill_requests_per_pair=0,
        batch_size=5000,
        pause_seconds=0.0,
        dry_run=False,
        report=report,
        interval_sec=0.0,
        duration_sec=0.0,
    )
    assert updater.run_once(args) == 0
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["source"]["environment"] == "practice"
    assert payload["total_rows_appended"] == 2
    assert payload["error_count"] == 0
