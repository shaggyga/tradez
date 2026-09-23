import datetime as dt
import json

import oanda_news_feed_backtest as backtest


UTC = dt.timezone.utc


def test_directional_calls_use_first_known_time_and_dedupe_syndication():
    rows = [
        {
            "event_id": "one",
            "headline": "Fed raises rates | Publisher One",
            "first_seen_utc": "2026-07-29T12:05:00Z",
            "currency_scores": {"USD": 1.0},
        },
        {
            "event_id": "two",
            "headline": "Fed raises rates | Publisher Two",
            "first_seen_utc": "2026-07-29T12:08:00Z",
            "currency_scores": {"USD": 1.0},
        },
    ]
    calls = backtest.directional_calls(rows, ["EUR_USD"])
    assert len(calls) == 1
    assert calls[0]["direction"] == "SHORT"
    assert calls[0]["signal_utc"] == dt.datetime(
        2026, 7, 29, 12, 5, tzinfo=UTC
    )


def test_score_call_is_spread_aware_and_never_enters_before_signal():
    candles = [
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 5, tzinfo=UTC),
            "bid_open": 1.1000,
            "ask_open": 1.1002,
        },
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 6, tzinfo=UTC),
            "bid_open": 1.1001,
            "ask_open": 1.1003,
        },
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 21, tzinfo=UTC),
            "bid_open": 1.1010,
            "ask_open": 1.1012,
        },
    ]
    call = {
        "pair": "EUR_USD",
        "signal_utc": dt.datetime(2026, 7, 29, 12, 5, 30, tzinfo=UTC),
        "direction": "LONG",
        "headline": "Test",
    }
    result = backtest.score_call(call, candles, 15)
    assert result is not None
    assert result["entry_utc"] == "2026-07-29T12:06:00+00:00"
    assert result["entry_price"] == 1.1003
    assert result["exit_price"] == 1.1010
    assert round(result["pnl_pips"], 6) == 7.0


def test_score_call_records_executable_mfe_and_mae():
    candles = [
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 6, tzinfo=UTC),
            "bid_open": 1.1000,
            "ask_open": 1.1002,
            "bid_high": 1.1007,
            "bid_low": 1.0997,
            "ask_high": 1.1009,
            "ask_low": 1.0999,
        },
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 21, tzinfo=UTC),
            "bid_open": 1.1005,
            "ask_open": 1.1007,
            "bid_high": 1.1012,
            "bid_low": 1.1001,
            "ask_high": 1.1014,
            "ask_low": 1.1003,
        },
    ]
    result = backtest.score_call(
        {
            "pair": "EUR_USD",
            "signal_utc": dt.datetime(2026, 7, 29, 12, 5, 30, tzinfo=UTC),
            "direction": "LONG",
        },
        candles,
        15,
    )
    assert result is not None
    assert round(result["mfe_pips"], 6) == 10.0
    assert round(result["mae_pips"], 6) == -5.0
    assert round(result["entry_spread_pips"], 6) == 2.0


def test_score_call_uses_instrument_metadata_pip_size():
    candles = [
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 6, tzinfo=UTC),
            "bid_open": 350.10,
            "ask_open": 350.12,
        },
        {
            "timestamp": dt.datetime(2026, 7, 29, 12, 21, tzinfo=UTC),
            "bid_open": 350.22,
            "ask_open": 350.24,
        },
    ]
    result = backtest.score_call(
        {
            "pair": "USD_HUF",
            "signal_utc": dt.datetime(2026, 7, 29, 12, 5, 30, tzinfo=UTC),
            "direction": "LONG",
        },
        candles,
        15,
        pip_size=0.01,
    )
    assert result is not None
    assert round(result["pnl_pips"], 6) == 10.0
    assert round(result["entry_spread_pips"], 6) == 2.0


def test_source_quality_weighting_can_filter_weak_news_call():
    rows = [
        {
            "event_id": "weak",
            "topic_id": "weak",
            "first_seen_utc": "2026-07-29T12:05:00Z",
            "published_utc": "2026-07-29T12:05:00Z",
            "currency_scores": {"USD": 0.4},
            "source_quality": 0.5,
            "directional_confidence": 0.5,
        }
    ]
    assert len(backtest.directional_calls(rows, ["EUR_USD"])) == 1
    assert (
        backtest.directional_calls(
            rows,
            ["EUR_USD"],
            weight_by_source_quality=True,
        )
        == []
    )


def test_secondary_research_scores_are_restored_only_in_copy():
    original = [
        {
            "currency_scores": {},
            "research_currency_scores": {"AUD": -0.4},
        }
    ]
    restored = backtest.restore_secondary_research_scores(original)
    assert original[0]["currency_scores"] == {}
    assert restored[0]["currency_scores"] == {"AUD": -0.4}
    assert restored[0]["directional_bias"] == {"AUD": "BEARISH"}


def test_inverse_direction_research_calls_preserve_identity_and_fail_closed():
    original = [
        {
            "topic_id": "topic-1",
            "event_id": "event-1",
            "pair": "USD_JPY",
            "signal_utc": dt.datetime(2026, 8, 10, 0, 50, tzinfo=UTC),
            "direction": "SHORT",
            "pair_score": -0.7,
            "raw_pair_score": -0.8,
        }
    ]

    inverted = backtest.inverse_direction_research_calls(original)

    assert original[0]["direction"] == "SHORT"
    assert inverted[0]["topic_id"] == "topic-1"
    assert inverted[0]["signal_utc"] == original[0]["signal_utc"]
    assert inverted[0]["direction"] == "LONG"
    assert inverted[0]["pair_score"] == 0.7
    assert inverted[0]["raw_pair_score"] == 0.8
    assert inverted[0]["predictive_eligible"] is False
    assert inverted[0]["execution_eligible"] is False
    assert inverted[0]["research_ablation"] == "INVERSE_DIRECTION"


def test_official_policy_release_filter_requires_direct_verified_source():
    base = {
        "topic_id": "topic",
        "official_policy_release": True,
        "source_direct": True,
        "source_verified": True,
        "direction": "LONG",
    }
    selected = backtest.official_policy_release_calls(
        [base, {**base, "topic_id": "media", "source_verified": False}]
    )

    assert len(selected) == 1
    assert selected[0]["topic_id"] == "topic"
    assert selected[0]["direction"] == "LONG"
    assert selected[0]["predictive_eligible"] is False
    assert selected[0]["execution_eligible"] is False


def test_fresh_catalyst_filter_rejects_recaps_and_unverified_singletons():
    base = {
        "topic_id": "fresh",
        "reports_prior_market_move": False,
        "forward_signal_timely": True,
        "source_verified": False,
        "distinct_source_count": 2,
    }
    selected = backtest.fresh_catalyst_research_calls(
        [
            base,
            {**base, "topic_id": "recap", "reports_prior_market_move": True},
            {**base, "topic_id": "singleton", "distinct_source_count": 1},
        ]
    )

    assert [row["topic_id"] for row in selected] == ["fresh"]
    assert selected[0]["execution_eligible"] is False


def test_catalyst_class_separates_policy_numeric_and_geopolitical_news():
    assert (
        backtest.catalyst_class({"official_policy_release": True})
        == "official_policy_text"
    )
    assert (
        backtest.catalyst_class({"structured_event": True})
        == "scheduled_numeric_release"
    )
    assert (
        backtest.catalyst_class(
            {"category": "risk_off_geopolitical_or_financial"}
        )
        == "geopolitical_or_systemic_risk"
    )


def test_episode_summary_by_catalyst_class_preserves_event_deduplication():
    rows = [
        {
            "horizon_minutes": 15,
            "first_catalyst_class": "official_policy_text",
            "first_topic_equal_weight_pair_basket_return_pct": value,
        }
        for value in (0.02, -0.01)
    ]

    [summary] = backtest.episode_summary_by_catalyst_class(rows)

    assert summary["observations"] == 2
    assert summary["profitable_observations"] == 1
    assert summary["average_return_pct"] == 0.005
    assert summary["execution_eligible"] is False


def test_multi_horizon_topic_path_detects_whipsaw_recovery():
    rows = [
        {
            "topic_id": "rba",
            "headline": "RBA decision",
            "horizon_minutes": horizon,
            "return_pct": value,
            "pair": "AUD_CHF",
        }
        for horizon, value in ((5, 0.02), (15, -0.01), (60, -0.02), (180, 0.03))
    ]

    result = backtest.multi_horizon_topic_paths(rows)

    assert len(result) == 1
    assert result[0]["path_class"] == (
        "initial_move_then_reversal_then_recovery"
    )
    assert result[0]["execution_eligible"] is False


def test_classification_cache_key_changes_with_content_and_config(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"sources": []}), encoding="utf-8")
    base = [{"event_id": "one", "headline": "A", "first_seen_utc": "x"}]
    first = backtest.classification_cache_key(base, config_path=config)
    same = backtest.classification_cache_key(list(base), config_path=config)
    changed_article = backtest.classification_cache_key(
        [{**base[0], "headline": "B"}], config_path=config
    )
    config.write_text(json.dumps({"sources": [{"source_id": "x"}]}), encoding="utf-8")
    changed_config = backtest.classification_cache_key(base, config_path=config)

    assert first == same
    assert first != changed_article
    assert first != changed_config


def test_article_classification_key_ignores_poll_count_but_tracks_text():
    base = {
        "event_id": "one",
        "source_id": "source",
        "headline": "Policy statement",
        "summary": "Rates unchanged",
        "first_seen_utc": "2026-08-11T12:00:00Z",
        "last_seen_utc": "2026-08-11T12:01:00Z",
        "duplicate_observation_count": 1,
    }
    later_poll = {
        **base,
        "last_seen_utc": "2026-08-11T12:10:00Z",
        "duplicate_observation_count": 10,
    }
    changed_text = {**later_poll, "summary": "Rates raised"}

    assert backtest.article_classification_key(base) == (
        backtest.article_classification_key(later_poll)
    )
    assert backtest.article_classification_key(base) != (
        backtest.article_classification_key(changed_text)
    )


def test_enriched_article_uses_detail_availability_as_causal_time():
    causal = backtest.article_causal_known_utc(
        {
            "detail_enriched": True,
            "detail_available_utc": "2026-08-10T14:17:04Z",
        },
        "2026-08-10T05:14:58Z",
    )

    assert causal == "2026-08-10T14:17:04+00:00"


def test_reclassification_preserves_enriched_official_source_provenance(
    tmp_path,
):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_id": "boj_updates",
                        "direct": True,
                        "currencies": ["JPY"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    source = {
        "source_id": "boj_updates",
        "source_name": "Bank of Japan",
        "source_kind": "rss",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "headline": "Summary of Opinions",
        "summary": "Further rate hikes may be appropriate.",
        "source_url": "https://example.test/boj.pdf",
        "published_utc": "2026-08-09T23:50:00Z",
        "first_seen_utc": "2026-08-10T05:14:58Z",
        "causal_known_utc": "2026-08-10T14:17:04Z",
        "detail_enriched": True,
        "detail_available_utc": "2026-08-10T14:17:04Z",
        "detail_content_sha256": "abc123",
        "detail_enrichment_research_only": True,
        "structured_event": True,
        "external_id": "boj-test-release",
        "actual_value": 1.0,
        "consensus_value": 0.8,
        "official_policy_release": True,
        "policy_document_type": "summary_of_opinions",
    }

    [row] = backtest.reclassify_articles([source], config_path=config)

    assert row["detail_enriched"] is True
    assert row["detail_available_utc"] == "2026-08-10T14:17:04Z"
    assert row["detail_content_sha256"] == "abc123"
    assert row["detail_enrichment_research_only"] is True
    assert row["structured_event"] is True
    assert row["actual_value"] == 1.0
    assert row["consensus_value"] == 0.8
    assert row["official_policy_release"] is True
    assert row["policy_document_type"] == "summary_of_opinions"
    assert row["causal_known_utc"] == "2026-08-10T14:17:04Z"


def test_topic_clustering_keeps_later_enriched_official_interpretation():
    common = {
        "source_id": "boj_updates",
        "source_name": "Bank of Japan",
        "domain": "boj.or.jp",
        "source_verified": True,
        "source_direct": True,
        "source_quality": 1.0,
        "official_policy_release": True,
        "headline": "Summary of Opinions",
        "published_utc": "2026-08-09T23:50:00Z",
        "first_seen_utc": "2026-08-09T23:50:17Z",
        "causal_known_utc": "2026-08-09T23:50:17Z",
        "currency_scores": {},
        "direct_currencies": ["JPY"],
    }
    enriched = {
        **common,
        "event_id": "detail",
        "first_seen_utc": "2026-08-10T05:14:58Z",
        "causal_known_utc": "2026-08-10T14:17:04Z",
        "summary": "The Bank may need to raise the policy rate further.",
        "detail_enriched": True,
        "detail_enrichment_research_only": True,
        "detail_available_utc": "2026-08-10T14:17:04Z",
        "currency_scores": {"JPY": 1.0},
    }

    [topic] = backtest.news.cluster_articles(
        [{**common, "event_id": "listing", "summary": ""}, enriched]
    )

    assert topic["detail_enriched"] is True
    assert topic["causal_known_utc"] == "2026-08-10T14:17:04+00:00"
    assert topic["currency_scores"] == {}
    assert topic["research_currency_scores"] == {"JPY": 1.0}
    assert topic["directional_publish_eligible"] is False


def test_continuation_call_requires_completed_aligned_price_shock():
    signal_time = dt.datetime(2026, 7, 30, 12, 15, 30, tzinfo=UTC)
    candles = [
        {
            "timestamp": dt.datetime(2026, 7, 30, 12, 0, tzinfo=UTC),
            "bid_open": 1.0999,
            "ask_open": 1.1001,
        },
        {
            "timestamp": dt.datetime(2026, 7, 30, 12, 14, tzinfo=UTC),
            "bid_open": 1.1009,
            "ask_open": 1.1011,
        },
        {
            # This candle is after local first-seen and must never be read.
            "timestamp": dt.datetime(2026, 7, 30, 12, 16, tzinfo=UTC),
            "bid_open": 1.0900,
            "ask_open": 1.0902,
        },
    ]
    calls = [
        {
            "pair": "EUR_USD",
            "signal_utc": signal_time,
            "direction": "LONG",
            "reports_prior_market_move": True,
        },
        {
            "pair": "EUR_USD",
            "signal_utc": signal_time,
            "direction": "SHORT",
            "reports_prior_market_move": True,
        },
        {
            "pair": "EUR_USD",
            "signal_utc": signal_time,
            "direction": "LONG",
            "reports_prior_market_move": False,
        },
    ]
    selected = backtest.continuation_calls_after_price_shock(
        calls,
        {"EUR_USD": candles},
        pip_sizes={"EUR_USD": 0.0001},
    )

    assert len(selected) == 1
    assert selected[0]["direction"] == "LONG"
    assert selected[0]["causal_relation"] == "CONTINUATION_SIGNAL"
    assert selected[0]["prior_shock_end_utc"] == "2026-07-30T12:14:00+00:00"


def test_currency_basket_summary_uses_shared_equal_weight_budget():
    rows = [
        {
            "topic_id": "topic-jpy",
            "currency_exposure_groups": ["JPY:LONG"],
            "horizon_minutes": 15,
            "pair": pair,
            "pair_score": score,
            "entry_spread_pips": spread,
            "entry_spread_pct": spread_pct,
            "return_pct": outcome,
        }
        for pair, score, spread, spread_pct, outcome in (
            ("USD_JPY", -0.8, 1.2, 0.01, 0.9),
            ("EUR_JPY", -0.7, 1.0, 0.012, 0.6),
            ("GBP_JPY", -0.6, 1.5, 0.014, 0.3),
            ("NZD_JPY", -0.5, 2.0, 0.02, -0.2),
        )
    ]
    summary = backtest.currency_basket_outcome_summary(rows)[0]

    assert summary["selected_single_pair"] == "USD_JPY"
    assert summary["selected_single_return_pct"] == 0.9
    assert summary["equal_weight_top3_return_pct"] == 0.6
    assert summary["equal_weight_all_legs_return_pct"] == 0.4
    assert summary["execution_eligible"] is False


def test_event_cluster_summary_collapses_pair_fanout_and_related_headlines():
    base = dt.datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    rows = []
    for topic_id, offset, direction, pair_returns in (
        ("topic-a", 0, "SHORT", (("USD_JPY", 0.4), ("EUR_JPY", 0.2))),
        ("topic-b", 10, "SHORT", (("USD_JPY", 0.5), ("EUR_JPY", 0.3))),
        ("topic-c", 12, "LONG", (("USD_JPY", -0.1), ("EUR_JPY", 0.1))),
    ):
        for pair, outcome in pair_returns:
            rows.append(
                {
                    "topic_id": topic_id,
                    "event_id": topic_id,
                    "signal_utc": base + dt.timedelta(minutes=offset),
                    "horizon_minutes": 120,
                    "pair": pair,
                    "direction": direction,
                    "pair_score": -0.8 if direction == "SHORT" else 0.8,
                    "entry_spread_pct": 0.02 if pair == "USD_JPY" else 0.01,
                    "return_pct": outcome,
                    "headline": topic_id,
                }
            )

    summary, episodes = backtest.event_cluster_outcome_summary(rows)
    h2 = summary["120"]

    assert h2["pair_leg_count"] == 6
    assert h2["topic_count"] == 3
    assert h2["event_cluster_count"] == 2
    assert len(episodes) == 2
    assert episodes[0]["topic_count"] == 2
    assert episodes[0]["first_topic_id"] == "topic-a"
    assert episodes[0]["first_topic_selected_pair"] == "EUR_JPY"
    assert h2["event_first_topic_equal_weight_pair_baskets"][
        "observations"
    ] == 2


def test_normalized_return_summary_exposes_outlier_dependency():
    summary = backtest.normalized_return_summary([-1.0, -0.5, -0.2, 5.0])

    assert summary["average_return_pct"] > 0
    assert summary["median_return_pct"] < 0
    assert summary["leave_best_out_average_return_pct"] < 0
