import datetime as dt

from oanda_news_currency_response_diagnostic import (
    build_currency_clocks,
    ceil_clock,
    executable_net_bps,
    placebo_directions,
    price_candidates,
    summarize,
)


def article(**changes):
    row = {
        "event_id": "event-1", "source_id": "source-a", "source_verified": 1,
        "published_utc": "2026-08-20T12:00:00+00:00",
        "first_seen_utc": "2026-08-20T12:01:01+00:00",
        "headline": "Bank raises rate", "relevant": 1,
        "currency_scores_json": '{"JPY": 0.8}',
        "payload_json": '{"forward_signal_timely":true,"reports_prior_market_move":false,"source_direct":true,"event_lineage_id":"story-a"}',
    }
    row.update(changes)
    return row


def test_ceil_clock_waits_for_complete_bucket():
    value = dt.datetime(2026, 8, 20, 12, 1, 1, tzinfo=dt.timezone.utc)
    assert ceil_clock(value) == dt.datetime(2026, 8, 20, 12, 5, tzinfo=dt.timezone.utc)


def test_story_currency_is_deduplicated_and_clock_is_causal():
    duplicate = article(event_id="event-2", source_id="source-b")
    clocks, selection = build_currency_clocks([article(), duplicate])
    assert selection["currency_clocks"] == 1
    assert clocks[0]["story_count"] == 1
    assert clocks[0]["decision_utc"].minute == 5


def test_recap_and_late_direction_are_rejected():
    recap = article(payload_json='{"forward_signal_timely":true,"reports_prior_market_move":true}')
    late = article(payload_json='{"forward_signal_timely":false}')
    clocks, selection = build_currency_clocks([recap, late])
    assert clocks == []
    assert selection["reports_prior_market_move"] == 1
    assert selection["not_forward_timely"] == 1


def test_executable_return_pays_both_sides_of_spread():
    entry = (99.0, 101.0, 100.0)
    unchanged = (99.0, 101.0, 100.0)
    assert executable_net_bps(1, entry, unchanged) < 0
    assert executable_net_bps(-1, entry, unchanged) < 0


def test_placebo_rotates_within_currency():
    clocks = [
        {"clock_id": "a", "currency": "JPY", "direction": 1, "decision_utc": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)},
        {"clock_id": "b", "currency": "JPY", "direction": -1, "decision_utc": dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)},
    ]
    result = placebo_directions(clocks)
    assert result == {"a": -1, "b": 1}


def test_summary_never_marks_retrospective_result_eligible():
    rows = [{
        "decision_utc": "2026-08-20T12:00:00+00:00", "currency": "JPY",
        "horizon_minutes": 5, "arm_net_bps": {"news_only": 1.0},
        "arm_directions": {"news_only": 1},
        "realized_currency_mid_bps": 2.0, "spread_bps": 1.0,
        "magnitude_cleared_spread": True,
    }]
    result = summarize(rows)[0]
    assert result["confirmation_eligible"] is False
    assert result["evidence_class"] == "retrospective_classifier_adaptive_diagnostic"


def test_custom_horizon_is_not_shadowed_by_result_dictionary(tmp_path):
    candle = tmp_path / "USD_JPY_M1.csv"
    candle.write_text(
        "datetime,bid_open,ask_open,open\n"
        "2026-08-20T11:45:00+00:00,99.9,100.1,100.0\n"
        "2026-08-20T12:00:00+00:00,100.0,100.2,100.1\n"
        "2026-08-20T12:05:00+00:00,100.2,100.4,100.3\n",
        encoding="utf-8",
    )
    clock = {
        "clock_id": "clock", "currency": "JPY", "direction": 1,
        "decision_utc": dt.datetime(2026, 8, 20, 12, 0, tzinfo=dt.timezone.utc),
    }
    result = price_candidates([clock], tmp_path, horizons=(5,))
    assert set(result["clock"][0]["horizon_pair_net_bps"]) == {5}
