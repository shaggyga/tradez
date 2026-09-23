from __future__ import annotations

import datetime as dt

from trad.oanda_gdelt_attention_mapping_audit import (
    build_currency_hours,
    currency_move_bps,
    executable_net_bps,
    floor_hour,
)


def test_hour_bucket_decision_uses_only_completed_hour() -> None:
    stamp = dt.datetime(2026, 8, 4, 15, 24, tzinfo=dt.timezone.utc)
    assert floor_hour(stamp) == dt.datetime(2026, 8, 4, 15, 0, tzinfo=dt.timezone.utc)
    rows = build_currency_hours([
        {"currency": "EUR", "first_seen_utc": stamp.isoformat(), "generic_tone": -0.1, "source_id": "g1", "relevant": True, "forward_signal_timely": True, "duplicate_observations": 0, "lineage_id": "a"},
        {"currency": "EUR", "first_seen_utc": stamp.replace(minute=50).isoformat(), "generic_tone": 0.0, "source_id": "g2", "relevant": False, "forward_signal_timely": True, "duplicate_observations": 0, "lineage_id": "b"},
    ])
    assert len(rows) == 1
    assert rows[0]["decision_utc"] == "2026-08-04T16:00:00+00:00"
    assert rows[0]["story_count"] == 2
    assert abs(rows[0]["mean_tone"] + 0.05) < 1e-12


def test_currency_orientation_and_executable_cost() -> None:
    entry = {"mid_o": 1.1000, "bid_o": 1.0999, "ask_o": 1.1001}
    exit_row = {"mid_o": 1.1010, "bid_o": 1.1009, "ask_o": 1.1011}
    assert currency_move_bps("EUR", "EUR_USD", entry, exit_row) > 0
    assert currency_move_bps("USD", "EUR_USD", entry, exit_row) < 0
    gross = currency_move_bps("EUR", "EUR_USD", entry, exit_row)
    net = executable_net_bps("EUR", "EUR_USD", 1, entry, exit_row)
    assert 0 < net < gross
