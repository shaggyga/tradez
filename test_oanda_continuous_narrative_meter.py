import datetime as dt
import json
import sqlite3

import pytest

from oanda_continuous_narrative_meter import (
    CURRENCIES,
    INSTRUMENTS,
    METER_CONTRACT_ID,
    RECENT_ARTICLE_FILTER_SQL,
    TERM_FACTOR_CONTRACT_ID,
    build_contributions,
    build_meter,
    ceil_clock,
    initialize_database,
    latest_payload,
    persist,
)


UTC = dt.timezone.utc


def article(**changes):
    payload = {
        "event_lineage_id": "story-one",
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "observation_clock_trusted": True,
        "source_direct": True,
        "estimated_reaction_horizon_minutes": 60,
        "classification_version": "frozen-test-classifier",
        "source_contract_id": "official-policy-v1",
        "source_cohort_id": "boj-rss-202608",
        "parser_version": "rss-parser-v2",
        "factor_terms": ["policy rate", "policy rate"],
        "topic_signature": "monetary_policy|rate_change",
        "directional_evidence": True,
        "directional_publish_eligible": True,
        "research_currency_scores": {"JPY": 0.6},
    }
    row = {
        "event_id": "event-one",
        "source_id": "boj",
        "source_kind": "rss",
        "source_verified": 1,
        "published_utc": "2026-08-24T12:00:00+00:00",
        "first_seen_utc": "2026-08-24T12:01:01+00:00",
        "headline": "Bank changes policy",
        "domain": "boj.or.jp",
        "relevant": 1,
        "category": "central bank decision",
        "currencies_json": '["JPY"]',
        "currency_scores_json": '{"JPY":0.5}',
        "generic_sentiment_score": -0.2,
        "directional_confidence": 0.8,
        "payload_json": json.dumps(payload),
    }
    row.update(changes)
    return row


def test_clock_waits_until_completed_five_minute_bucket():
    value = dt.datetime(2026, 8, 24, 12, 1, 1, tzinfo=UTC)
    assert ceil_clock(value) == dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)


def test_recent_article_filter_uses_existing_range_index(tmp_path):
    database = tmp_path / "articles.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "CREATE TABLE articles(event_id TEXT,relevant INTEGER NOT NULL,"
            "first_seen_utc TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE INDEX idx_articles_relevant "
            "ON articles(relevant,first_seen_utc)"
        )
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT event_id FROM articles WHERE "
            + RECENT_ARTICLE_FILTER_SQL,
            ("2026-08-22T00:00:00+00:00",),
        ).fetchall()
    finally:
        connection.close()
    assert any("idx_articles_relevant" in str(row) for row in plan)


def test_syndicated_story_is_one_vote_and_recap_is_excluded():
    duplicate = article(event_id="event-two", source_id="wire-copy", domain="copy.test")
    recap_payload = json.loads(article()["payload_json"])
    recap_payload["event_lineage_id"] = "recap"
    recap_payload["reports_prior_market_move"] = True
    recap = article(event_id="recap", payload_json=json.dumps(recap_payload))
    rows, counts = build_contributions([article(), duplicate, recap])
    assert len(rows) == 1
    assert rows[0]["story_id"] == "story-one"
    assert counts["prior_move_recaps_excluded"] == 1
    assert counts["syndicated_rows_collapsed"] == 1
    assert rows[0]["terms"] == [
        "category:central_bank_decision",
        "factor:policy_rate",
        "topic:monetary_policy",
        "topic:rate_change",
    ]


def test_revision_and_source_lineage_are_preserved_without_using_raw_prose():
    payload = json.loads(article()["payload_json"])
    payload.update({
        "revision_id": "revision-2",
        "revised_at_utc": "2026-08-24T12:03:00+00:00",
        "supersedes_event_id": "event-zero",
        "raw_payload_hash": "abc123",
    })
    contributions, _ = build_contributions([
        article(last_seen_utc="2026-08-24T12:04:00+00:00", payload_json=json.dumps(payload))
    ])
    row = contributions[0]
    assert row["source_contract_id"] == "official-policy-v1"
    assert row["source_cohort_id"] == "boj-rss-202608"
    assert row["parser_version"] == "rss-parser-v2"
    assert row["revision_id"] == "revision-2"
    assert row["supersedes_event_id"] == "event-zero"
    assert all("bank_changes_policy" not in term for term in row["terms"])


def test_secondary_directional_discovery_is_separate_and_inert():
    payload = json.loads(article()["payload_json"])
    payload.update(
        {
            "event_lineage_id": "secondary-warning",
            "directional_evidence": True,
            "directional_corroboration_required": True,
            "directional_source_grade": "secondary_requires_corroboration",
        }
    )
    secondary = article(
        event_id="secondary-warning",
        relevant=0,
        currency_scores_json='{"JPY":0.4}',
        payload_json=json.dumps(payload),
    )
    contributions, counts = build_contributions([secondary])
    assert len(contributions) == 1
    assert counts["secondary_directional_discovery_rows"] == 1
    assert contributions[0]["evidence_channel"] == (
        "secondary_directional_discovery"
    )
    assert contributions[0]["published_score"] == 0.0
    assert contributions[0]["secondary_score"] > 0.0
    start = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=start, end=start)
    jpy = next(row for row in rows if row["currency"] == "JPY")
    assert jpy["model_scores"]["published_semantic_v1"] == 0.0
    assert jpy["model_scores"]["secondary_directional_discovery_v1"] > 0.0
    assert jpy["secondary_story_count"] == 1
    assert jpy["published_story_count"] == 0
    latest = latest_payload(rows, counts)
    assert latest["execution_eligible"] is False
    assert latest["can_place_orders"] is False


def test_late_secondary_story_is_retained_but_has_zero_live_weight():
    payload = json.loads(article()["payload_json"])
    payload.update(
        {
            "event_lineage_id": "late-secondary",
            "directional_publish_eligible": False,
            "forward_signal_timely": False,
        }
    )
    late = article(
        event_id="late-secondary",
        relevant=0,
        currency_scores_json='{"JPY":0.45}',
        payload_json=json.dumps(payload),
    )

    contributions, counts = build_contributions([late])

    assert len(contributions) == 1
    assert counts["late_secondary_context_rows"] == 1
    assert counts.get("secondary_directional_discovery_rows", 0) == 0
    contribution = contributions[0]
    assert contribution["evidence_channel"] == "late_secondary_context"
    assert contribution["observed_research_score"] > 0.0
    assert contribution["research_score"] == 0.0
    assert contribution["secondary_score"] == 0.0
    assert contribution["terms"]

    clock = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=clock, end=clock)
    jpy = next(row for row in rows if row["currency"] == "JPY")
    assert jpy["active_story_count"] == 1
    assert jpy["secondary_story_count"] == 0
    assert jpy["attention_level"] == 0.0
    assert jpy["model_scores"]["research_semantic_v1"] == 0.0
    assert jpy["model_scores"]["secondary_directional_discovery_v1"] == 0.0
    term = jpy["term_factor_contributions"][0]
    assert term["evidence_channel"] == "late_secondary_context"
    assert term["effective_weight"] == 0.0
    assert term["observed_research_score"] > 0.0


def test_timely_secondary_story_keeps_existing_live_research_weight():
    payload = json.loads(article()["payload_json"])
    payload.update(
        {
            "event_lineage_id": "timely-secondary",
            "directional_publish_eligible": False,
            "forward_signal_timely": True,
        }
    )
    timely = article(
        event_id="timely-secondary",
        relevant=0,
        currency_scores_json='{"JPY":0.45}',
        payload_json=json.dumps(payload),
    )

    contributions, counts = build_contributions([timely])

    assert counts["secondary_directional_discovery_rows"] == 1
    contribution = contributions[0]
    assert contribution["evidence_channel"] == "secondary_directional_discovery"
    assert contribution["research_score"] > 0.0
    assert contribution["secondary_score"] > 0.0
    clock = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=clock, end=clock)
    jpy = next(row for row in rows if row["currency"] == "JPY")
    assert jpy["secondary_story_count"] == 1
    assert jpy["attention_level"] > 0.0
    assert jpy["model_scores"]["secondary_directional_discovery_v1"] > 0.0


def test_relevant_but_publish_ineligible_row_stays_secondary():
    payload = json.loads(article()["payload_json"])
    payload.update(
        {
            "event_lineage_id": "relevant-secondary",
            "directional_publish_eligible": False,
        }
    )
    secondary = article(
        event_id="relevant-secondary",
        relevant=1,
        currency_scores_json='{"JPY":0.45}',
        payload_json=json.dumps(payload),
    )
    contributions, counts = build_contributions([secondary])
    assert len(contributions) == 1
    assert counts["secondary_directional_discovery_rows"] == 1
    assert contributions[0]["evidence_channel"] == (
        "secondary_directional_discovery"
    )
    assert contributions[0]["directional_publish_eligible"] is False
    assert contributions[0]["published_score"] == 0.0
    assert contributions[0]["secondary_score"] > 0.0


def test_secondary_topic_signature_collapses_source_local_lineages():
    first_payload = json.loads(article()["payload_json"])
    first_payload.update(
        {
            "event_lineage_id": "source-local-one",
            "topic_signature": "risk_off|global|middle_east|escalation",
            "directional_publish_eligible": False,
        }
    )
    second_payload = dict(first_payload)
    second_payload["event_lineage_id"] = "source-local-two"
    first = article(
        event_id="one", relevant=0, payload_json=json.dumps(first_payload)
    )
    second = article(
        event_id="two",
        source_id="copy",
        relevant=0,
        first_seen_utc="2026-08-24T12:02:01+00:00",
        payload_json=json.dumps(second_payload),
    )
    contributions, counts = build_contributions([first, second])
    assert len(contributions) == 1
    assert counts["syndicated_rows_collapsed"] == 1
    assert contributions[0]["story_id"].startswith("secondary_topic:")


def test_narrative_score_magnitude_decays_before_hard_expiry():
    contributions, _ = build_contributions([article()])
    start = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    end = start + dt.timedelta(minutes=30)
    rows = build_meter(contributions, start=start, end=end)
    start_jpy = next(
        row for row in rows
        if row["currency"] == "JPY" and row["clock_utc"] == start
    )
    end_jpy = next(
        row for row in rows
        if row["currency"] == "JPY" and row["clock_utc"] == end
    )
    assert 0 < end_jpy["model_scores"]["published_semantic_v1"] < (
        start_jpy["model_scores"]["published_semantic_v1"]
    )


def test_dense_meter_emits_all_currencies_and_separate_models():
    contributions, _ = build_contributions([article()])
    start = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=start, end=start)
    assert len(rows) == len(CURRENCIES)
    jpy = next(row for row in rows if row["currency"] == "JPY")
    usd = next(row for row in rows if row["currency"] == "USD")
    assert jpy["model_scores"]["published_semantic_v1"] > 0
    assert jpy["model_scores"]["research_semantic_v1"] > jpy["model_scores"]["published_semantic_v1"]
    assert jpy["model_scores"]["recovered_blurb_analog_v1"] == 0
    assert jpy["model_scores"]["linguistic_tone_v1"] < 0
    assert usd["model_scores"]["narrative_acceleration_v1"] == 0
    assert usd["historical_evidence_class"] == "no_current_evidence"


def test_latest_pair_score_is_base_minus_quote_and_inert():
    contributions, selection = build_contributions([article()])
    start = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=start, end=start)
    payload = latest_payload(rows, selection)
    assert payload["currency_count"] == 21
    assert payload["instrument_count"] == 68
    assert len(INSTRUMENTS) == 68
    assert payload["pairs"]["USD_JPY"]["score"] < 0
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False
    assert payload["can_place_orders"] is False


def test_database_keeps_formula_registry_and_compact_rows(tmp_path):
    contributions, _ = build_contributions([article()])
    start = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=start, end=start)
    database = tmp_path / "meter.sqlite"
    persist(database, rows)
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("SELECT COUNT(*) FROM model_registry").fetchone()[0] == 7
        assert connection.execute("SELECT COUNT(*) FROM currency_meter").fetchone()[0] == 21
        assert connection.execute(
            "SELECT COUNT(*) FROM currency_meter WHERE meter_contract_id=?",
            (METER_CONTRACT_ID,),
        ).fetchone()[0] == 21
        assert connection.execute(
            "SELECT COUNT(*) FROM narrative_term_factor_clock WHERE term_factor_contract_id=?",
            (TERM_FACTOR_CONTRACT_ID,),
        ).fetchone()[0] == 21
        assert connection.execute(
            "SELECT contribution_count FROM narrative_term_factor_clock WHERE currency='USD'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM narrative_term_factor_contribution"
        ).fetchone()[0] == 4
        lineage = connection.execute(
            "SELECT source_contract_id,source_cohort_id,parser_version "
            "FROM narrative_term_factor_contribution LIMIT 1"
        ).fetchone()
        assert lineage == ("official-policy-v1", "boj-rss-202608", "rss-parser-v2")
        persist(database, rows)
        assert connection.execute(
            "SELECT COUNT(*) FROM narrative_term_factor_contribution"
        ).fetchone()[0] == 4
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE narrative_term_factor_clock SET contribution_count=9 WHERE currency='USD'"
            )
    finally:
        connection.close()


def test_observed_research_score_round_trips_for_late_and_timely_rows(tmp_path):
    late_payload = json.loads(article()["payload_json"])
    late_payload.update(
        {
            "event_lineage_id": "late-persisted",
            "directional_publish_eligible": False,
            "forward_signal_timely": False,
        }
    )
    timely_payload = dict(late_payload)
    timely_payload.update(
        {
            "event_lineage_id": "timely-persisted",
            "topic_signature": "monetary_policy|timely_rate_change",
            "forward_signal_timely": True,
        }
    )
    contributions, _ = build_contributions(
        [
            article(
                event_id="late-persisted",
                relevant=0,
                currency_scores_json='{"JPY":0.45}',
                payload_json=json.dumps(late_payload),
            ),
            article(
                event_id="timely-persisted",
                relevant=0,
                first_seen_utc="2026-08-24T12:02:01+00:00",
                currency_scores_json='{"JPY":0.35}',
                payload_json=json.dumps(timely_payload),
            ),
        ]
    )
    clock = dt.datetime(2026, 8, 24, 12, 5, tzinfo=UTC)
    rows = build_meter(contributions, start=clock, end=clock)
    database = tmp_path / "meter.sqlite"

    persist(database, rows)

    connection = sqlite3.connect(database)
    try:
        persisted = connection.execute(
            "SELECT evidence_channel,research_score,secondary_score,"
            "effective_weight,observed_research_score "
            "FROM narrative_term_factor_contribution "
            "WHERE currency='JPY' GROUP BY story_id ORDER BY evidence_channel"
        ).fetchall()
    finally:
        connection.close()
    by_channel = {row[0]: row[1:] for row in persisted}
    assert by_channel["late_secondary_context"] == (0.0, 0.0, 0.0, 0.6)
    timely = by_channel["secondary_directional_discovery"]
    assert timely[0] == 0.6
    assert timely[1] == 0.6
    assert timely[2] > 0.0
    assert timely[3] == 0.6


def test_existing_term_ledger_adds_observed_score_without_rewriting_rows(tmp_path):
    database = tmp_path / "legacy.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """CREATE TABLE narrative_term_factor_contribution(
                term_factor_contract_id TEXT NOT NULL,
                clock_utc TEXT NOT NULL,
                currency TEXT NOT NULL,
                story_id TEXT NOT NULL,
                source_family TEXT NOT NULL,
                term TEXT NOT NULL,
                research_score REAL NOT NULL
            )"""
        )
        connection.execute(
            "INSERT INTO narrative_term_factor_contribution VALUES(?,?,?,?,?,?,?)",
            ("old", "2026-08-24T12:00:00Z", "JPY", "story", "rss", "rate", 0.7),
        )
        connection.commit()
        initialize_database(connection)
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(narrative_term_factor_contribution)"
            )
        }
        assert "observed_research_score" in columns
        assert connection.execute(
            "SELECT research_score,observed_research_score "
            "FROM narrative_term_factor_contribution"
        ).fetchone() == (0.7, 0.0)
        assert connection.execute(
            "SELECT COUNT(*) FROM narrative_term_factor_contribution"
        ).fetchone()[0] == 1
    finally:
        connection.close()
