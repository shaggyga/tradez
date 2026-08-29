import sqlite3

import pytest

import oanda_spike_blurb_unmatched_episode_queue_v2 as episode


def test_movement_bps_uses_pair_price_scale():
    assert episode.movement_bps(10.0, "EUR_USD", 1.0) == pytest.approx(10.0)
    assert episode.movement_bps(10.0, "USD_JPY", 100.0) == pytest.approx(10.0)


def test_choose_factor_uses_breadth_then_stable_tie_break():
    candidates = [{"currency": "USD", "sign": 1}, {"currency": "JPY", "sign": -1}]
    counts = episode.Counter({"USD:positive": 3, "JPY:negative": 4})
    assert episode.signed_factor_key(episode.choose_factor(candidates, counts)) == "JPY:negative"
    counts = episode.Counter({"USD:positive": 4, "JPY:negative": 4})
    assert episode.signed_factor_key(episode.choose_factor(candidates, counts)) == "JPY:negative"


def test_episode_tables_are_immutable(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.sqlite")
    episode.ensure_schema(connection)
    connection.execute(
        "INSERT INTO unmatched_episode_queue_contracts VALUES (?,?,?,?)",
        ("c", "{}", "a" * 64, "2026-01-01T00:00:00+00:00"),
    )
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute("DELETE FROM unmatched_episode_queue_contracts WHERE contract_id='c'")
