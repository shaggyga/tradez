import datetime as dt

from oanda_news_historical_quality_audit import (
    CURRENCIES,
    audit_articles,
    historical_evidence_summary,
    price_covered,
    quantile,
)


def row(**updates):
    base = {
        "event_id": "event-1",
        "source_id": "official-test",
        "source_name": "Official test",
        "source_kind": "rss",
        "source_verified": 1,
        "published_utc": "2026-08-20T12:00:00+00:00",
        "first_seen_utc": "2026-08-20T12:01:00+00:00",
        "last_seen_utc": "2026-08-20T12:02:00+00:00",
        "headline": "Policy decision",
        "relevant": 1,
        "category": "monetary_policy",
        "currencies_json": '["JPY"]',
        "currency_scores_json": '{"JPY": 0.7}',
        "generic_sentiment_score": 0.1,
        "directional_confidence": 0.8,
        "severity": 80.0,
        "movement_potential": "HIGH",
        "duplicate_count": 0,
        "payload_json": """{
          "observation_clock_trusted": true,
          "forward_signal_timely": true,
          "source_direct": true,
          "event_lineage_id": "story-1",
          "classification_version": "frozen-v1",
          "structured_event": true,
          "actual_value": 1.0,
          "consensus_value": 0.5,
          "consensus_capture_state": "causal_pre_release_snapshot"
        }""",
    }
    base.update(updates)
    return base


def ranges():
    return {
        "JPY": [(
            dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 8, 31, tzinfo=dt.timezone.utc),
        )]
    }


def test_audit_accepts_complete_point_in_time_official_row():
    result = audit_articles([row()], ranges())
    assert result["counts"]["diagnostic_direction_candidates"] == 1
    assert result["counts"]["diagnostic_timely_direction_candidates"] == 1
    assert result["counts"]["diagnostic_trusted_direction_candidates"] == 1
    assert result["counts"]["replay_candidates"] == 1
    assert result["counts"]["official_replay_candidates"] == 1
    assert result["counts"]["causal_surprise_ready"] == 1
    assert result["per_currency"]["JPY"]["replay_candidates"] == 1


def test_missing_consensus_is_not_surprise_ready():
    value = row(payload_json=row().get("payload_json").replace(
        '"consensus_value": 0.5', '"consensus_value": null'
    ))
    result = audit_articles([value], ranges())
    assert result["counts"].get("causal_surprise_ready", 0) == 0
    assert result["counts"]["structured_actual_without_causal_consensus"] == 1


def test_untrusted_or_late_row_cannot_enter_replay_population():
    value = row(payload_json=row().get("payload_json").replace(
        '"observation_clock_trusted": true', '"observation_clock_trusted": false'
    ))
    result = audit_articles([value], ranges())
    assert result["counts"]["diagnostic_direction_candidates"] == 1
    assert result["counts"].get("diagnostic_trusted_direction_candidates", 0) == 0
    assert result["counts"].get("replay_candidates", 0) == 0
    assert result["counts"].get("official_replay_candidates", 0) == 0


def test_price_coverage_is_currency_and_time_bounded():
    inside = dt.datetime(2026, 8, 20, tzinfo=dt.timezone.utc)
    outside = dt.datetime(2026, 9, 20, tzinfo=dt.timezone.utc)
    assert price_covered(inside, ["JPY"], ranges())
    assert not price_covered(outside, ["JPY"], ranges())
    assert not price_covered(inside, ["EUR"], ranges())


def test_all_21_currencies_have_report_rows_even_without_observations():
    result = audit_articles([], {})
    assert tuple(result["per_currency"]) == CURRENCIES


def test_quantile_interpolates_and_empty_is_none():
    assert quantile([0, 10], 0.5) == 5
    assert quantile([], 0.5) is None


def test_historical_summary_preserves_selection_bias_labels():
    summary = historical_evidence_summary(
        {"market_days": 7, "mapping_quality": {"independent_currency_story_rows": 12}, "arm_summaries": [{"arm": "news_only", "raw_n": 3}]},
        {"arms": {"published_directional": {"metrics": {"raw_episode_rows": 4}}}},
    )
    assert summary["gdelt_generic_tone"]["market_days"] == 7
    assert summary["gdelt_generic_tone"]["news_only"][0]["raw_n"] == 3
    assert "selection_biased" in summary["movement_conditioned_direction"]["conclusion"]
