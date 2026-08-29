import json
import sqlite3
from pathlib import Path

import oanda_live_move_news_snapshot as live


def _event(payload, *, epoch=100, event_id="e1"):
    return {
        "source_event_id": event_id,
        "source_id": "official",
        "source_population": "official_policy_publisher",
        "event_type": "monetary_policy",
        "story_cluster_id": event_id,
        "published_at_utc": "1970-01-01T00:01:35+00:00",
        "first_seen_at_utc": "1970-01-01T00:01:40+00:00",
        "retrieved_at_utc": "1970-01-01T00:01:41+00:00",
        "effective_from_utc": "1970-01-01T00:01:40+00:00",
        "revised_at_utc": "1970-01-01T00:01:42+00:00",
        "event_version": 2,
        "supersedes_source_event_id": "e0",
        "effective_epoch": epoch,
        "valid_until_epoch": None,
        "superseded_epoch": None,
        "payload_json": json.dumps({"raw_payload": payload}),
    }


def test_selected_movers_keeps_clear_unique_directional_rows():
    state = {
        "rankings": {
            "live_velocity": [
                {"instrument": "EUR_USD", "direction": "increase", "clear_move": True},
                {"instrument": "EUR_USD", "direction": "increase", "clear_move": True},
                {"instrument": "USD_JPY", "direction": "decrease", "clear_move": False},
            ]
        }
    }
    assert [row["instrument"] for row in live.selected_movers(state, 5)] == [
        "EUR_USD"
    ]


def test_mover_case_keeps_strict_news_separate_from_context():
    event = _event(
        {
            "headline": "Official policy tightens",
            "directional_bias": {"EUR": "BULLISH"},
            "directional_confidence": 0.8,
            "forward_signal_timely": True,
            "directional_publish_eligible": True,
            "official_policy_release": True,
        }
    )
    source_index = {
        "EUR": {"events": [event], "epochs": [100]},
        "USD": {"events": [], "epochs": []},
    }
    row = {
        "instrument": "EUR_USD",
        "direction": "increase",
        "clear_move": True,
        "start_utc": "1970-01-01T00:03:20+00:00",
        "end_utc": "1970-01-01T00:04:20+00:00",
        "duration_minutes": 1.0,
        "gross_pips": 4.0,
        "executable_net_pips": 2.0,
        "move_bps": 3.0,
        "velocity_bps_per_hour": 180.0,
    }
    result = live.mover_case(
        row,
        source_index,
        {"EUR_USD": {"direction": "LONG", "score": 0.4}},
        generated_epoch=300,
        lookback_minutes=5,
    )
    assert result["strict_forward_alignment"] == "aligned"
    assert result["explanation_state"] == "strict_forward_source_aligned"
    assert result["strict_forward_independent_story_count"] == 1
    assert result["continuous_narrative_state"]["score"] == 0.4
    assert result["execution_eligible"] is False


def test_unpublished_context_never_becomes_strict_direction():
    event = _event(
        {
            "headline": "Research interpretation",
            "directional_bias": {"EUR": "BULLISH"},
            "forward_signal_timely": True,
            "directional_publish_eligible": False,
        }
    )
    source_index = {
        "EUR": {"events": [event], "epochs": [100]},
        "USD": {"events": [], "epochs": []},
    }
    row = {
        "instrument": "EUR_USD",
        "direction": "increase",
        "clear_move": True,
        "start_utc": "1970-01-01T00:03:20+00:00",
        "end_utc": "1970-01-01T00:04:20+00:00",
    }
    result = live.mover_case(
        row,
        source_index,
        {},
        generated_epoch=300,
        lookback_minutes=5,
    )
    assert result["strict_forward_alignment"] == "no_strict_direction"
    assert result["broad_research_side"] == "long"
    assert result["explanation_state"] == "research_context_aligned_not_publishable"
    assert result["recent_context_stories"][0]["headline"]


def test_context_story_preserves_first_seen_and_revision_lineage():
    story = live._context_story(_event({"headline": "Policy update"}), "EUR", "USD")

    assert story["source_event_id"] == "e1"
    assert story["story_cluster_id"] == "e1"
    assert story["published_at_utc"] == "1970-01-01T00:01:35+00:00"
    assert story["first_seen_at_utc"] == "1970-01-01T00:01:40+00:00"
    assert story["retrieved_at_utc"] == "1970-01-01T00:01:41+00:00"
    assert story["effective_from_utc"] == "1970-01-01T00:01:40+00:00"
    assert story["revised_at_utc"] == "1970-01-01T00:01:42+00:00"
    assert story["event_version"] == 2
    assert story["supersedes_source_event_id"] == "e0"


def test_research_worker_is_supervised_without_execution_arguments():
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert '-Name "live_move_news_snapshot"' in supervisor
    assert 'oanda_live_move_news_snapshot.py' in supervisor
    block = supervisor.split('-Name "live_move_news_snapshot"', 1)[1].split(
        '$managed += Start-ManagedProcess', 1
    )[0]
    assert "order" not in block.lower()
    assert "authorization" not in block.lower()


def test_case_history_is_append_only_and_deduplicated(tmp_path):
    path = tmp_path / "cases.sqlite"
    row = {
        "instrument": "EUR_USD",
        "start_utc": "2026-08-24T12:00:00Z",
        "end_utc": "2026-08-24T12:05:00Z",
        "move_direction": "up",
        "execution_eligible": False,
    }
    first = live.record_cases(path, [row], recorded_utc="2026-08-24T12:06:00Z")
    second = live.record_cases(path, [row], recorded_utc="2026-08-24T12:07:00Z")

    assert first == {"inserted": 1, "total": 1}
    assert second == {"inserted": 0, "total": 1}
    connection = sqlite3.connect(path)
    try:
        try:
            connection.execute("DELETE FROM mover_cases")
        except sqlite3.DatabaseError as exc:
            assert "append_only" in str(exc)
        else:
            raise AssertionError("mover history allowed deletion")
    finally:
        connection.close()


def test_case_identity_does_not_change_as_same_move_extends():
    row = {
        "instrument": "USD_JPY",
        "start_utc": "2026-08-24T12:00:00Z",
        "end_utc": "2026-08-24T12:05:00Z",
        "move_direction": "up",
    }
    extended = dict(row, end_utc="2026-08-24T12:11:00Z")
    assert live.stable_case_id(row) == live.stable_case_id(extended)
    assert live.stable_case_id(row) != live.stable_case_id(
        dict(row, move_direction="down")
    )


def test_explanation_state_keeps_strict_and_research_evidence_separate():
    assert live.CONTRACT_ID == "live_move_news_snapshot_v5_causal_factor_strength_20260827"
    assert live.explanation_state(
        actual_side=1,
        strict_side=-1,
        broad_side=1,
        narrative_side=1,
        relevant_event_count=4,
    ) == "strict_forward_source_opposed"
    assert live.explanation_state(
        actual_side=-1,
        strict_side=0,
        broad_side=0,
        narrative_side=-1,
        relevant_event_count=2,
    ) == "continuous_narrative_aligned_unvalidated"
    assert live.explanation_state(
        actual_side=1,
        strict_side=0,
        broad_side=0,
        narrative_side=0,
        relevant_event_count=0,
    ) == "no_relevant_source_event"


def test_factor_episode_dedup_collapses_correlated_currency_propagation():
    rows = [
        {
            "instrument": "USD_ZAR",
            "move_direction": "up",
            "start_utc": "2026-08-24T12:00:00Z",
            "move_bps": 10.0,
            "executable_net_pips": 100.0,
        },
        {
            "instrument": "GBP_ZAR",
            "move_direction": "up",
            "start_utc": "2026-08-24T12:02:00Z",
            "move_bps": 12.0,
            "executable_net_pips": 90.0,
        },
        {
            "instrument": "EUR_JPY",
            "move_direction": "down",
            "start_utc": "2026-08-24T12:03:00Z",
            "move_bps": 8.0,
            "executable_net_pips": 7.0,
        },
    ]

    count = live.assign_factor_episodes(rows)

    assert count == 2
    assert rows[0]["factor_primary_token"] == "ZAR-"
    assert rows[1]["factor_primary_token"] == "ZAR-"
    assert rows[0]["factor_episode_id"] == rows[1]["factor_episode_id"]
    assert rows[0]["factor_representative"] is False
    assert rows[1]["factor_representative"] is True
    assert rows[2]["factor_representative"] is True


def test_causal_strength_dedupes_huf_zar_grid_without_factor_count_inflation():
    rows = [
        {
            "instrument": instrument,
            "move_direction": "up",
            "start_utc": "2026-08-27T04:15:00Z",
            "end_utc": end,
            "duration_minutes": duration,
            "move_bps": move,
            "executable_net_pips": move,
        }
        for instrument, end, duration, move in (
            ("EUR_HUF", "2026-08-27T05:10:00Z", 49.0, 15.44),
            ("USD_HUF", "2026-08-27T05:10:00Z", 53.0, 14.27),
            ("EUR_ZAR", "2026-08-27T05:14:00Z", 59.0, 9.81),
            ("USD_ZAR", "2026-08-27T05:14:00Z", 59.0, 8.98),
        )
    ]
    huf_end = int(live.census.parse_epoch("2026-08-27T05:10:00Z"))
    zar_end = int(live.census.parse_epoch("2026-08-27T05:14:00Z"))
    surfaces = {
        (huf_end, 60): {
            "valid": True,
            "status": "ready",
            "as_of_utc": "2026-08-27T05:10:00+00:00",
            "as_of_age_sec": 0,
            "oldest_pair_age_sec": 0,
            "observation_count": 68,
            "currency_count": 21,
            "source_contract_id": "test_all68",
            "currency_strength_bps": {
                "EUR": 1.99,
                "USD": 1.35,
                "HUF": -7.40,
            },
        },
        (zar_end, 60): {
            "valid": True,
            "status": "ready",
            "as_of_utc": "2026-08-27T05:14:00+00:00",
            "as_of_age_sec": 0,
            "oldest_pair_age_sec": 0,
            "observation_count": 68,
            "currency_count": 21,
            "source_contract_id": "test_all68",
            "currency_strength_bps": {
                "EUR": 2.58,
                "USD": 2.07,
                "ZAR": -6.64,
            },
        },
    }

    count = live.assign_factor_episodes(rows, strength_surfaces=surfaces)

    assert count == 2
    assert [row["factor_primary_token"] for row in rows] == [
        "HUF-",
        "HUF-",
        "ZAR-",
        "ZAR-",
    ]
    assert rows[0]["factor_episode_id"] == rows[1]["factor_episode_id"]
    assert rows[2]["factor_episode_id"] == rows[3]["factor_episode_id"]
    assert rows[0]["factor_representative"] is True
    assert rows[1]["factor_representative"] is False
    assert rows[2]["factor_representative"] is True
    assert rows[3]["factor_representative"] is False
    assert rows[0]["factor_primary_method"] == "causal_all68_currency_strength"
    assert rows[0]["factor_primary_scores_bps"] == {"EUR+": 1.99, "HUF-": 7.4}
    assert rows[0]["factor_strength_as_of_age_sec"] == 0
    assert rows[0]["factor_primary_ambiguous"] is False


def test_factor_fallback_is_bucket_local_and_records_unavailable_surface():
    rows = [
        {
            "instrument": "EUR_HUF",
            "move_direction": "up",
            "start_utc": "2026-08-27T04:15:00Z",
            "end_utc": "2026-08-27T05:10:00Z",
            "duration_minutes": 55,
            "move_bps": 5,
            "executable_net_pips": 5,
        },
        {
            "instrument": "USD_HUF",
            "move_direction": "up",
            "start_utc": "2026-08-27T04:16:00Z",
            "end_utc": "2026-08-27T05:10:00Z",
            "duration_minutes": 54,
            "move_bps": 4,
            "executable_net_pips": 4,
        },
        {
            "instrument": "EUR_USD",
            "move_direction": "up",
            "start_utc": "2026-08-27T04:45:00Z",
            "end_utc": "2026-08-27T05:00:00Z",
            "duration_minutes": 15,
            "move_bps": 3,
            "executable_net_pips": 3,
        },
    ]

    count = live.assign_factor_episodes(rows, strength_surfaces={})

    assert count == 2
    assert rows[0]["factor_primary_token"] == "HUF-"
    assert rows[1]["factor_primary_token"] == "HUF-"
    assert rows[2]["factor_primary_token"] == "EUR+"
    assert (
        rows[0]["factor_primary_method"]
        == "time_bucket_token_recurrence_fallback"
    )
    assert rows[0]["factor_strength_surface_status"] == "unavailable"


def test_causal_strength_surface_ignores_future_quote_rows():
    start = 1_000
    end = start + 15 * 60

    def quote(instrument, epoch, mid):
        return {
            "instrument": instrument,
            "minute_epoch": epoch,
            "first_epoch": epoch,
            "last_epoch": epoch,
            "close_bid": mid - 0.00005,
            "close_ask": mid + 0.00005,
            "pip": 0.0001,
        }

    history = []
    for instrument, old, current, future in (
        ("EUR_HUF", 100.0, 101.0, 95.0),
        ("USD_HUF", 100.0, 101.0, 95.0),
        ("EUR_USD", 1.0, 1.0, 1.0),
    ):
        history.extend(
            [
                quote(instrument, start, old),
                quote(instrument, end, current),
                quote(instrument, end + 60, future),
            ]
        )
    history.append(quote("EUR_TRY", start, 50.0))
    move = {
        "instrument": "EUR_HUF",
        "start_utc": live.census.iso_epoch(start),
        "end_utc": live.census.iso_epoch(end),
        "duration_minutes": 15,
        "move_direction": "up",
        "move_bps": 10,
        "executable_net_pips": 10,
    }
    try_move = {
        **move,
        "instrument": "EUR_TRY",
        "move_bps": 2,
        "executable_net_pips": 2,
    }

    surfaces = live.build_causal_factor_strength_surfaces(
        [move, try_move],
        history,
        minimum_observation_count=3,
        maximum_pair_age_sec=0,
    )
    surface = surfaces[(end, 15)]

    assert surface["valid"] is True
    assert surface["future_clock_count"] == 0
    assert live.census.parse_epoch(surface["as_of_utc"]) == end
    assert surface["as_of_age_sec"] == 0
    assert surface["observation_count"] == 3
    assert surface["currency_strength_bps"]["HUF"] < 0
    assert surface["status"] == "ready_with_logged_pair_exclusions"
    assert surface["stale_instruments"] == ["EUR_TRY"]
    assert surface["missing_currencies"] == ["TRY"]

    live.assign_factor_episodes([move, try_move], strength_surfaces=surfaces)

    assert move["factor_primary_method"] == "causal_all68_currency_strength"
    assert move["factor_primary_token"] == "HUF-"
    assert (
        try_move["factor_primary_method"]
        == "time_bucket_token_recurrence_fallback"
    )
    assert try_move["factor_strength_missing_currencies"] == ["TRY"]


def test_causal_strength_surface_cannot_see_intraminute_close_after_move_end():
    start = 1_000
    end = 1_320

    def aggregate(instrument, minute, opening, closing):
        return {
            "instrument": instrument,
            "minute_epoch": minute,
            "first_epoch": minute + 5,
            "last_epoch": minute + 50,
            "open_bid": opening - 0.00005,
            "open_ask": opening + 0.00005,
            "close_bid": closing - 0.00005,
            "close_ask": closing + 0.00005,
            "high_mid": max(opening, closing),
            "low_mid": min(opening, closing),
            "pip": 0.0001,
        }

    rows = []
    for instrument in ("EUR_HUF", "USD_HUF", "EUR_USD"):
        rows.append(aggregate(instrument, start - 5, 1.0, 1.0))
        # The close is deliberately extreme but occurs after the 1,320 move
        # watermark. Only the 1,305 opening quote is causally available.
        rows.append(aggregate(instrument, 1_300, 1.01, 2.0))
    cases = [
        {
            "instrument": "EUR_HUF",
            "move_direction": "up",
            "start_utc": live.census.iso_epoch(start),
            "end_utc": live.census.iso_epoch(end),
            "duration_minutes": 5,
        }
    ]
    surfaces = live.build_causal_factor_strength_surfaces(
        cases, rows, minimum_observation_count=3
    )
    surface = surfaces[(end, 5)]

    assert surface["valid"]
    assert surface["as_of_utc"] == live.census.iso_epoch(1_305)
    # If the future close leaked, the absolute strengths would be hundreds or
    # thousands of bps rather than the bounded opening move below.
    assert max(abs(value) for value in surface["currency_strength_bps"].values()) < 200


def test_entry_quote_freezes_executable_forward_shadow_arms():
    row = {
        "move_direction": "up",
        "broad_research_side": "long",
        "strict_forward_side": "neutral",
        "continuous_narrative_state": {"direction": "NEUTRAL"},
    }
    live.attach_entry_quote(
        row,
        {
            "time": "2026-08-24T12:00:00Z",
            "bid": 1.1,
            "ask": 1.1002,
            "pip": 0.0001,
        },
        observation_epoch=1787572810,
    )
    assert row["entry_quote_fresh"] is True
    assert row["entry_spread_pips"] == 2.0
    assert row["forward_shadow_arms"] == {
        "technical_continuation": 1,
        "broad_context_direction": 1,
        "strict_forward_direction": 0,
        "continuous_narrative_direction": 0,
        "broad_context_plus_continuation": 1,
    }
