import datetime as dt
import json

import oanda_policy_statement_breakout_research as policy


UTC = dt.timezone.utc


def _event():
    prior = policy.policy_stance_snapshot(
        "The Committee's current assessment implies that it will likely be "
        "necessary to raise the policy rate. Inflation pressures are slightly "
        "stronger than anticipated. Capacity utilisation is drifting down."
    )
    current = policy.policy_stance_snapshot(
        "Inflation has slowed and been lower than projected this summer. "
        "It may thus still become necessary to raise the policy rate. "
        "Capacity utilisation is close to normal but is drifting down. "
        "At the monetary policy meeting in June it was likely necessary to raise."
    )
    return {
        "event_id": "fixture",
        "source_id": "norges_press",
        "currency": "NOK",
        "known_utc": "2026-08-13T08:02:04+00:00",
        "published_utc": "2026-08-13T08:00:00+00:00",
        "prior_stance": prior,
        "current_stance": current,
        "delta": policy.policy_stance_delta(current, prior),
    }


def _row(mid, spread, imbalance):
    return {
        "mid": mid,
        "spread_pips": spread,
        "imbalance_30s": imbalance,
        "imbalance_120s": imbalance / 2,
    }


def _quotes():
    event_minute = int(dt.datetime(2026, 8, 13, 8, 0, tzinfo=UTC).timestamp())
    result = {}
    for instrument, pre_mid, known_mid, confirm_mid, entry_mid, pip in (
        ("EUR_NOK", 10.9340, 10.926045, 10.936915, 10.940315, 0.0001),
        ("USD_NOK", 9.4860, 9.480835, 9.491155, 9.49355, 0.0001),
    ):
        rows = {}
        for offset in range(-10, 0):
            rows[event_minute + offset * 60] = _row(
                pre_mid + offset * pip / 20, 32.0, 0.0
            )
        rows[event_minute + 2 * 60] = _row(known_mid, 60.0, 0.0)
        rows[event_minute + 3 * 60] = _row(confirm_mid, 49.0, 0.14)
        rows[event_minute + 4 * 60] = _row(entry_mid, 42.0, 0.06)
        result[instrument] = rows
    return result


def test_norges_statement_delta_is_dovish_despite_conditional_hike_words():
    event = _event()
    assert event["prior_stance"]["guidance_score"] == 0.85
    assert event["current_stance"]["guidance_score"] == 0.35
    assert event["current_stance"]["inflation_score"] == -0.65
    assert event["delta"]["direction"] == "DOVISH"
    assert event["delta"]["currency_direction"] == "WEAKEN"


def test_current_excerpt_excludes_retrospective_prior_meeting_guidance():
    excerpt = policy.current_policy_excerpt(
        "Inflation was lower than projected. It may still become necessary to "
        "raise. At the monetary policy meeting in June it was likely necessary "
        "to raise."
    )
    assert "meeting in June" not in excerpt


def test_official_policy_currency_is_constrained_to_source_contract():
    contracts = {"bot_mpc_decisions_direct_v2": {"THB"}}

    assert policy.source_native_policy_currencies(
        "bot_mpc_decisions_direct_v2", ["THB", "USD"], contracts
    ) == ["THB"]


def test_multicurrency_contract_requires_payload_intersection():
    contracts = {"fixture": {"EUR", "USD"}}

    assert policy.source_native_policy_currencies(
        "fixture", ["USD", "JPY"], contracts
    ) == ["USD"]


def test_policy_document_class_prevents_cross_format_delta_comparisons():
    assert policy.policy_document_class("FOMC statement") == "decision_statement"
    assert policy.policy_document_class("Minutes of the FOMC meeting") == "minutes_or_account"
    documents = [
        {
            "event_id": "statement",
            "source_id": "fed",
            "currency": "USD",
            "document_class": "decision_statement",
            "known_utc": "2026-09-16T18:00:01+00:00",
            "published_utc": "2026-09-16T18:00:00+00:00",
            "headline": "FOMC statement",
            "source_url": "https://example.test/statement",
            "stance": policy.policy_stance_snapshot("Inflation remains elevated."),
            "baseline": False,
        },
        {
            "event_id": "minutes",
            "source_id": "fed",
            "currency": "USD",
            "document_class": "minutes_or_account",
            "known_utc": "2026-10-01T18:00:01+00:00",
            "published_utc": "2026-10-01T18:00:00+00:00",
            "headline": "Minutes of the FOMC meeting",
            "source_url": "https://example.test/minutes",
            "stance": policy.policy_stance_snapshot("Inflation remains elevated."),
            "baseline": False,
        },
    ]
    assert policy.build_policy_delta_events(documents) == []


def test_two_pair_breakout_triggers_next_minute_entry():
    setup = policy.evaluate_breakout_setup(_event(), _quotes())
    assert setup["status"] == "triggered"
    assert setup["confirmation_minute_utc"] == "2026-08-13T08:03:00+00:00"
    assert setup["entry_minute_utc"] == "2026-08-13T08:04:00+00:00"
    assert setup["confirmation_count"] == 2
    assert setup["selected"]["direction"] == "long"


def test_one_pair_cannot_inflate_policy_confirmation():
    quotes = _quotes()
    quotes.pop("USD_NOK")
    setup = policy.evaluate_breakout_setup(_event(), quotes)
    assert setup["status"] == "no_trigger"


def test_abnormal_spread_blocks_breakout_even_when_price_moves():
    quotes = _quotes()
    event_minute = int(dt.datetime(2026, 8, 13, 8, 0, tzinfo=UTC).timestamp())
    for rows in quotes.values():
        rows[event_minute + 3 * 60]["spread_pips"] = 90.0
    setup = policy.evaluate_breakout_setup(_event(), quotes)
    assert setup["status"] == "no_trigger"


def test_prospective_entry_persistence_is_research_only(tmp_path):
    database = tmp_path / "policy.sqlite"
    connection = policy.open_database(database)
    event = _event()
    event["prospective_eligible"] = True
    setup = policy.evaluate_breakout_setup(event, _quotes())
    inserted = policy.persist_prospective_entries(
        connection,
        event,
        setup,
        dt.datetime(2026, 8, 15, 15, 1, tzinfo=UTC),
    )
    rows = connection.execute(
        "SELECT cohort_id,status,payload_json FROM prospective_entries "
        "ORDER BY horizon_min"
    ).fetchall()
    connection.close()
    assert inserted == 3
    assert len(rows) == 3
    assert all(row[1] == "pending" for row in rows)
    assert all('"execution_eligible": false' in row[2] for row in rows)


def test_worker_heartbeat_is_separate_and_fail_closed(tmp_path):
    heartbeat = tmp_path / "heartbeat.json"
    policy.write_worker_heartbeat(
        heartbeat,
        phase="building_research_projection",
        cycle_started_utc="2026-09-02T04:00:00+00:00",
    )
    payload = json.loads(heartbeat.read_text(encoding="utf-8"))
    assert payload["schema_version"] == (
        "policy_statement_breakout_research_heartbeat_v1"
    )
    assert payload["contract_id"] == policy.CONTRACT_ID
    assert payload["phase"] == "building_research_projection"
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False
    assert payload["can_place_orders"] is False
    assert payload["can_promote"] is False
    assert payload["can_authorize"] is False
