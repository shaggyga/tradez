from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from trad.oanda_quote_intensity_shadow import SignedQuoteIntensityTracker


def test_tracker_preserves_signed_changes_and_minute_rows(tmp_path) -> None:
    database = tmp_path / "intensity.sqlite"
    state = tmp_path / "intensity.json"
    tracker = SignedQuoteIntensityTracker(
        database,
        state,
        retention_clock=lambda: datetime(
            2026, 8, 5, 5, 2, tzinfo=timezone.utc
        ).timestamp(),
    )
    tracker.observe("EUR_USD", mid=1.1000, spread_pips=1.0, broker_time="2026-08-05T05:00:01Z", received_monotonic=1.0)
    tracker.observe("EUR_USD", mid=1.1001, spread_pips=1.2, broker_time="2026-08-05T05:00:02Z", received_monotonic=2.0)
    tracker.observe("EUR_USD", mid=1.0999, spread_pips=1.4, broker_time="2026-08-05T05:01:01Z", received_monotonic=61.0)
    assert tracker.flush() == 2
    connection = sqlite3.connect(database)
    rows = connection.execute(
        "SELECT minute_epoch,up_moves,down_moves,updates FROM quote_intensity_minutes_v1 ORDER BY minute_epoch"
    ).fetchall()
    connection.close()
    assert rows[0][1:] == (1, 0, 2)
    assert rows[1][1:] == (0, 1, 1)
    payload = tracker.snapshot()
    assert payload["can_place_orders"] is False
    assert "not trade/order flow" in payload["measurement"]


def test_intensity_decays_between_events(tmp_path) -> None:
    tracker = SignedQuoteIntensityTracker(tmp_path / "i.sqlite", tmp_path / "i.json")
    tracker.observe("USD_JPY", mid=150.00, spread_pips=1.0, broker_time="2026-08-05T05:00:01Z", received_monotonic=0.0)
    tracker.observe("USD_JPY", mid=150.01, spread_pips=1.0, broker_time="2026-08-05T05:00:02Z", received_monotonic=1.0)
    first = tracker.snapshot()["top_absolute_30s_imbalances"][0]["up_intensity_30s"]
    tracker.observe("USD_JPY", mid=150.01, spread_pips=1.0, broker_time="2026-08-05T05:00:32Z", received_monotonic=31.0)
    second = tracker.snapshot()["top_absolute_30s_imbalances"][0]["up_intensity_30s"]
    assert 0.45 < second / first < 0.55
