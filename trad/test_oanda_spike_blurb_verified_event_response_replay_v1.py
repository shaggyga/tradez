import sqlite3

import pytest

import oanda_spike_blurb_verified_event_response_replay_v1 as replay


def candle(epoch, mid_o, mid_c, spread=0.0002):
    half = spread / 2
    return {
        "epoch": epoch,
        "mid_o": mid_o,
        "mid_c": mid_c,
        "bid_c": mid_c - half,
        "ask_c": mid_c + half,
        "bid_h": mid_c + 0.0004 - half,
        "bid_l": mid_c - 0.0004 - half,
        "ask_h": mid_c + 0.0004 + half,
        "ask_l": mid_c - 0.0004 + half,
        "bid_o": mid_o - half,
        "ask_o": mid_o + half,
    }


def test_currency_snapshot_orients_base_and_quote_legs():
    event = 1_700_000_000
    paths = {
        "EUR_USD": {event: candle(event, 1.1000, 1.0989)},
        "GBP_USD": {event: candle(event, 1.2500, 1.24875)},
        "USD_CHF": {event: candle(event, 0.9000, 0.9009)},
    }
    result = replay.currency_snapshot(paths, "USD", event, 1)
    assert result is not None
    assert result["currency_direction"] == "stronger"
    assert result["breadth"] == 1.0
    assert result["leg_count"] == 3
    assert result["selected_pair_direction"] in {"long", "short"}


def test_strength_technical_confirmation_requires_fast_above_slow():
    history = [
        {"currency_strength_bps": float(value), "direction_sign": 1}
        for value in [1, 1, 1, 1, 1, 1, 1, 2, 3, 4]
    ]
    aligned, fast, slow = replay.technical_state(history)
    assert aligned is True
    assert fast > slow


def test_outcome_uses_executable_bid_ask_and_slippage():
    detection_epoch = 1_700_000_060
    detection = {
        "detection_epoch": detection_epoch,
        "selected_instrument": "EUR_USD",
        "selected_pair_direction": "long",
        "entry_bid": 0.9999,
        "entry_ask": 1.0001,
    }
    candles = {
        detection_epoch + minute * 60: candle(
            detection_epoch + minute * 60,
            1.0000 + minute * 0.0001,
            1.0001 + minute * 0.0001,
        )
        for minute in range(5)
    }
    result = replay.outcome(detection, candles, 5)
    assert result is not None
    assert result["executable_net_before_slippage_pips"] == pytest.approx(3.0)
    assert result["net_after_cost_pips"] == pytest.approx(2.75)
    assert result["modeled_slippage_pips"] == 0.25


def test_response_tables_are_immutable(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.sqlite")
    replay.ensure_schema(connection)
    connection.execute(
        "INSERT INTO verified_event_response_contracts VALUES (?,?,?,?,?,?,?)",
        ("c", "s", "p", "{}", "a" * 64, "b" * 64, "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM verified_event_response_contracts WHERE contract_id='c'")
