from __future__ import annotations

import json

import pandas as pd

import oanda_news_event_tagger as tagger


def _event(**overrides):
    raw = {
        "event_id": "event_test",
        "event_utc": "2026-07-27T12:00:00Z",
        "first_known_utc": "2026-07-27T12:00:00Z",
        "headline": "Test event",
        "summary": "Timestamped event used by the all-pair test.",
        "category": "macro_data",
        "currencies": ["USD"],
        "scope": "all_pairs",
        "directional_bias": {"USD": "BULLISH"},
        "severity": 80,
        "post_window_minutes": 120,
        "source_url": "https://example.test/event",
    }
    raw.update(overrides)
    return tagger.normalize_event(raw, source_type="curated_seed")


def _huf_v3_event(**overrides):
    raw = {
        "event_id": "huf-cpi-clock",
        "event_utc": "2026-09-08T06:30:00Z",
        "scheduled_utc": "2026-09-08T06:30:00Z",
        "published_utc": "2026-08-17T06:44:31Z",
        "first_known_utc": "2026-08-17T06:44:31Z",
        "headline": "Hungary headline consumer price inflation",
        "summary": "Official pre-announced exact release clock.",
        "category": "inflation_context",
        "source_id": tagger.HUF_KSH_VERIFIED_SOURCE_ID,
        "source_contract_id": tagger.HUF_KSH_VERIFIED_CONTRACT_ID,
        "source_cohort_id": tagger.HUF_KSH_VERIFIED_CONTRACT_ID,
        "source_name": tagger.HUF_KSH_SOURCE_NAME,
        "source_kind": "ksh_release_calendar_verified_rule_exact_v3",
        "source_type": "local_no_gpt_news",
        "source_quality": 1.0,
        "source_verified": True,
        "source_direct": True,
        "retrieval_via": tagger.HUF_KSH_RETRIEVAL_VIA,
        "source_role": "primary_statistical_calendar",
        "source_url": tagger.HUF_KSH_CALENDAR_URL,
        "publisher_url": tagger.HUF_KSH_CALENDAR_URL,
        "source_currencies": ["HUF"],
        "direct_currencies": ["HUF"],
        "currencies": ["HUF"],
        "scope": "currency",
        "event_series_id": "hungary_headline_cpi_yoy",
        "event_name": "Hungary headline consumer price inflation",
        "event_country": "Hungary",
        "event_time_basis": "scheduled_release",
        "timing_precision": "minute",
        "clock_semantics": "domestic_official_statistical_release",
        "independent_domestic_event": True,
        "linked_policy_factor": False,
        "material_content_sha256": "a" * 64,
        "release_time_rule_observed_sha256": tagger.HUF_KSH_POLICY_SHA256,
        "release_time_rule_archive_name": tagger.HUF_KSH_POLICY_ARCHIVE,
        "release_rule_bytes_verified": True,
        "directional_research_only": True,
        "research_only": True,
        "context_only": True,
        "relevant": False,
        "directional_publish_eligible": False,
        "execution_eligible": False,
        "can_place_orders": False,
        "currency_scores": {},
        "directional_bias": {},
        "pair_bias": {},
        "direction": None,
        "actual": None,
        "actual_value": None,
        "consensus": None,
        "consensus_value": None,
        "severity": 65,
        "post_window_minutes": 180,
    }
    raw.update(overrides)
    return tagger.normalize_event(raw, source_type="live_news_watch")


def test_engineering_only_huf_v1_v2_are_not_admitted_to_semantic_catalog() -> None:
    assert _event(source_id="hungary_ksh_release_calendar_exact_v1") is None
    assert _event(
        source_id="hungary_ksh_headline_cpi_release_clock_exact_v2"
    ) is None
    assert _huf_v3_event() is not None


def test_huf_v3_tagger_rejects_directional_or_semantic_impersonation() -> None:
    mutations = [
        {"release_time_rule_archive_name": "anything.pdf"},
        {"source_verified": "0"},
        {"source_direct": False},
        {"source_url": "https://www.ksh.hu/industry"},
        {"direct_currencies": ["USD"], "currencies": ["USD"]},
        {"timing_precision": "date_window"},
        {"clock_semantics": "domestic_official_policy_release"},
        {"independent_domestic_event": False},
        {"linked_policy_factor": True},
        {"directional_bias": {"HUF": "BULLISH"}},
        {"currency_scores": {"HUF": 1.0}},
        {"direction": "BULLISH"},
        {"directional_research_only": False},
        {"context_only": False, "relevant": True},
        {"execution_eligible": True},
        {"can_place_orders": True},
        {"material_content_sha256": "not-a-hash"},
        {
            "event_utc": "2026-09-08T07:30:00Z",
            "scheduled_utc": "2026-09-08T07:30:00Z",
        },
    ]
    for mutation in mutations:
        assert _huf_v3_event(**mutation) is None, mutation


def test_all_pair_event_expands_to_every_supplied_instrument() -> None:
    instruments = ["EUR_USD", "GBP_JPY", "USD_CAD"]
    rows = tagger.expand_events([_event()], instruments)

    assert {row["instrument"] for row in rows} == set(instruments)
    assert len(rows) == len(instruments)
    direct = next(row for row in rows if row["instrument"] == "EUR_USD")
    spillover = next(row for row in rows if row["instrument"] == "GBP_JPY")
    assert direct["pair_relevance"] > spillover["pair_relevance"]
    assert direct["expected_pair_direction"] == "SHORT"
    assert direct["currency_exposure_groups"] == ["USD:LONG"]
    assert direct["currency_basket_max_legs"] == 3
    assert direct["currency_basket_leg_risk_fraction"] == 0.333333
    assert direct["execution_eligible"] is False


def test_currency_scope_only_maps_pairs_containing_the_currency() -> None:
    event = _event(scope="currency")
    rows = tagger.expand_events(
        [event],
        ["EUR_USD", "GBP_JPY", "USD_CAD"],
    )

    assert {row["instrument"] for row in rows} == {"EUR_USD", "USD_CAD"}


def test_inferred_global_currency_fanout_does_not_outrank_direct_currency() -> None:
    global_event = _event(
        event_id="global-risk",
        scope="all_pairs",
        direct_currencies=[],
        currencies=["AUD", "CAD", "CHF", "JPY", "USD"],
        severity=80,
    )
    direct_event = _event(
        event_id="direct-jpy",
        scope="currency",
        direct_currencies=["JPY"],
        currencies=["JPY"],
        severity=55,
    )

    rows = tagger.expand_events(
        [global_event, direct_event],
        ["USD_JPY"],
    )
    global_row = next(row for row in rows if row["event_id"] == "global-risk")
    direct_row = next(row for row in rows if row["event_id"] == "direct-jpy")

    assert global_row["pair_relevance"] == 0.42
    assert global_row["relevance_reason"] == "global_event"
    assert direct_row["pair_relevance"] == 0.78
    assert direct_row["relevance_reason"] == "one_currency"
    context = tagger.build_current_pair_context(
        rows,
        ["USD_JPY"],
        as_of="2026-07-27T12:30:00Z",
        max_events_per_pair=1,
    )
    assert context["USD_JPY"]["events"][0]["event_id"] == "direct-jpy"


def test_context_only_event_is_retained_but_not_directional_or_predictive() -> None:
    event = _event(
        context_only=True,
        directional_bias={"USD": "BULLISH"},
    )
    rows = tagger.expand_events([event], ["EUR_USD"])
    assert rows[0]["context_only"] is True

    context = tagger.build_current_pair_context(
        rows,
        ["EUR_USD"],
        as_of="2026-07-27T12:30:00Z",
    )
    assert context["EUR_USD"]["active_event_count"] == 1
    assert context["EUR_USD"]["directional_state"] == "UNKNOWN"
    assert context["EUR_USD"]["events"][0]["context_only"] is True

    matches = tagger.match_events_for_movement(
        rows,
        instrument="EUR_USD",
        start_utc="2026-07-27T12:31:00Z",
        end_utc="2026-07-27T12:40:00Z",
    )
    assert matches[0]["predictive_eligible"] is False


def test_exact_domestic_clock_semantics_survive_pair_expansion() -> None:
    event = _event(
        event_utc=None,
        scheduled_utc="2026-08-20T08:30:00Z",
        first_known_utc="2026-08-17T12:00:00Z",
        currencies=["HKD"],
        direct_currencies=["HKD"],
        scope="currency",
        directional_bias={},
        timing_precision="minute",
        clock_semantics="domestic_official_statistical_release",
        independent_domestic_event=True,
        linked_policy_factor=False,
        context_only=True,
    )

    assert event["event_time_basis"] == "scheduled_release"
    assert event["timing_precision"] == "minute"
    assert event["independent_domestic_event"] is True
    rows = tagger.expand_events([event], ["USD_HKD", "EUR_USD"])
    assert len(rows) == 1
    assert rows[0]["instrument"] == "USD_HKD"
    assert rows[0]["clock_semantics"] == "domestic_official_statistical_release"
    assert rows[0]["independent_domestic_event"] is True
    assert rows[0]["linked_policy_factor"] is False
    assert rows[0]["execution_eligible"] is False
    assert rows[0]["can_place_orders"] is False


def test_first_known_timestamp_prevents_lookahead_links() -> None:
    event = _event(first_known_utc="2026-07-27T12:30:00Z")
    rows = tagger.expand_events([event], ["EUR_USD"])

    assert rows[0]["active_from_utc"] == "2026-07-27T12:30:00+00:00"

    before_known = tagger.match_events_for_movement(
        rows,
        instrument="EUR_USD",
        start_utc="2026-07-27T12:05:00Z",
        end_utc="2026-07-27T12:20:00Z",
    )
    after_known = tagger.match_events_for_movement(
        rows,
        instrument="EUR_USD",
        start_utc="2026-07-27T12:31:00Z",
        end_utc="2026-07-27T12:40:00Z",
    )

    assert before_known == []
    assert len(after_known) == 1
    assert after_known[0]["temporal_relation"] == "post_event"
    assert after_known[0]["causal_relation"] == "PRE_MOVE"
    assert after_known[0]["predictive_eligible"] is True


def test_syndicated_event_tag_never_activates_at_pre_observation_publish_time() -> None:
    event = _event(
        event_utc="2026-08-14T06:00:29Z",
        published_utc="2026-08-14T06:00:29Z",
        first_known_utc="2026-08-14T06:02:29Z",
        pre_window_minutes=0,
    )

    rows = tagger.expand_events([event], ["USD_JPY"])

    assert rows[0]["active_from_utc"] == "2026-08-14T06:02:29+00:00"


def test_intervention_report_after_first_wave_is_continuation_signal() -> None:
    event = _event(
        event_utc="2026-07-30T13:45:00Z",
        first_known_utc="2026-07-30T13:49:00Z",
        category="fx_intervention",
        currencies=["JPY"],
        directional_bias={"JPY": "BULLISH"},
        reports_prior_market_move=True,
        intervention_status="suspected",
        topic_tags='["#fx_intervention", "#jpy_suspected_strengthening"]',
    )
    rows = tagger.expand_events([event], ["USD_JPY"])
    matches = tagger.match_events_for_movement(
        rows,
        instrument="USD_JPY",
        start_utc="2026-07-30T14:16:00Z",
        end_utc="2026-07-30T14:21:00Z",
    )

    assert len(matches) == 1
    assert matches[0]["causal_relation"] == "CONTINUATION_SIGNAL"
    assert matches[0]["availability_to_move_lead_minutes"] == 27
    assert matches[0]["predictive_eligible"] is True
    assert matches[0]["intervention_status"] == "suspected"
    assert matches[0]["topic_tags"] == [
        "#fx_intervention",
        "#jpy_suspected_strengthening",
    ]


def test_news_arriving_during_move_is_confirmation_not_prediction() -> None:
    event = _event(
        event_utc="2026-07-30T13:45:00Z",
        first_known_utc="2026-07-30T13:49:00Z",
        category="fx_intervention",
        reports_prior_market_move=True,
    )
    rows = tagger.expand_events([event], ["EUR_USD"])
    matches = tagger.match_events_for_movement(
        rows,
        instrument="EUR_USD",
        start_utc="2026-07-30T13:47:00Z",
        end_utc="2026-07-30T13:52:00Z",
    )

    assert matches[0]["causal_relation"] == "FIRST_WAVE_CONFIRMATION"
    assert matches[0]["predictive_eligible"] is False
    assert matches[0]["confirmation_eligible"] is True


def test_post_hoc_news_is_diagnostic_only_when_requested() -> None:
    event = _event(
        event_utc="2026-07-30T14:26:00Z",
        first_known_utc="2026-07-30T15:05:00Z",
    )
    rows = tagger.expand_events([event], ["EUR_USD"])
    default_matches = tagger.match_events_for_movement(
        rows,
        instrument="EUR_USD",
        start_utc="2026-07-30T13:37:00Z",
        end_utc="2026-07-30T13:42:00Z",
    )
    diagnostic_matches = tagger.match_events_for_movement(
        rows,
        instrument="EUR_USD",
        start_utc="2026-07-30T13:37:00Z",
        end_utc="2026-07-30T13:42:00Z",
        include_post_hoc=True,
    )

    assert default_matches == []
    assert diagnostic_matches[0]["causal_relation"] == "POST_HOC_EXPLANATION"
    assert diagnostic_matches[0]["predictive_eligible"] is False


def test_scheduled_event_uses_release_anchor_and_appears_in_pre_window() -> None:
    event = _event(
        event_utc=None,
        scheduled_utc="2026-07-30T14:00:00Z",
        published_utc="2026-07-30T12:00:00Z",
        first_known_utc="2026-07-30T12:01:00Z",
        pre_window_minutes=30,
    )
    rows = tagger.expand_events([event], ["EUR_USD"])
    context = tagger.build_current_pair_context(
        rows,
        ["EUR_USD"],
        as_of="2026-07-30T13:45:00Z",
    )

    assert event["event_time_basis"] == "scheduled_release"
    assert event["event_utc"] == "2026-07-30T14:00:00+00:00"
    assert context["EUR_USD"]["active_event_count"] == 1


def test_current_context_contains_explicit_empty_rows_for_all_pairs() -> None:
    event = _event(post_window_minutes=30)
    instruments = ["EUR_USD", "GBP_JPY"]
    rows = tagger.expand_events([event], instruments)

    context = tagger.build_current_pair_context(
        rows,
        instruments,
        as_of="2026-07-27T14:00:00Z",
    )

    assert set(context) == set(instruments)
    assert all(row["active_event_count"] == 0 for row in context.values())
    assert all(row["events"] == [] for row in context.values())


def test_synchronize_catalog_writes_all_pair_json_csv_and_sqlite(
    tmp_path,
    monkeypatch,
) -> None:
    event = _event()
    monkeypatch.setattr(
        tagger,
        "collect_events",
        lambda **_: [event],
    )
    output_root = tmp_path / "news"

    result = tagger.synchronize_catalog(
        instruments=["EUR_USD", "GBP_JPY"],
        output_root=output_root,
        as_of="2026-07-27T12:30:00Z",
    )
    context = json.loads(
        (output_root / "latest_pair_news_context.json").read_text(
            encoding="utf-8"
        )
    )

    assert result["status"] == "ok"
    assert result["instrument_count"] == 2
    assert result["pair_event_tag_count"] == 2
    assert set(context["pairs"]) == {"EUR_USD", "GBP_JPY"}
    assert (output_root / "events_latest.csv").exists()
    assert (output_root / "pair_event_tags_latest.csv").exists()
    assert (output_root / "news_event_tags.sqlite").exists()


def test_unmatched_significant_move_backfill_still_writes_valid_parquet(
    tmp_path,
) -> None:
    source = tmp_path / "moves.parquet"
    pd.DataFrame(
        [
            {
                "move_id": "move_1",
                "instrument": "EUR_USD",
                "start_timestamp": "2026-07-27T10:00:00Z",
                "end_timestamp": "2026-07-27T10:05:00Z",
            }
        ]
    ).to_parquet(source, index=False)

    result = tagger.backfill_significant_moves(
        source_path=source,
        output_root=tmp_path / "output",
        legacy_links_path=tmp_path / "legacy.csv",
        pair_events=[],
    )
    summaries = pd.read_parquet(result["summary_parquet"])
    links = pd.read_parquet(tmp_path / "output" / "significant_move_news_links.parquet")

    assert result["movement_count"] == 1
    assert result["matched_movement_count"] == 0
    assert summaries.loc[0, "news_match_status"] == "no_verified_time_pair_match"
    assert links.empty


def test_mixed_matched_and_unmatched_backfill_has_stable_parquet_types(
    tmp_path,
) -> None:
    source = tmp_path / "mixed_moves.parquet"
    pd.DataFrame(
        [
            {
                "move_id": "matched",
                "instrument": "EUR_USD",
                "start_timestamp": "2026-07-27T12:05:00Z",
                "end_timestamp": "2026-07-27T12:10:00Z",
            },
            {
                "move_id": "unmatched",
                "instrument": "GBP_JPY",
                "start_timestamp": "2026-07-27T09:00:00Z",
                "end_timestamp": "2026-07-27T09:05:00Z",
            },
        ]
    ).to_parquet(source, index=False)
    pair_events = tagger.expand_events([_event()], ["EUR_USD"])

    result = tagger.backfill_significant_moves(
        source_path=source,
        output_root=tmp_path / "output",
        legacy_links_path=tmp_path / "legacy.csv",
        pair_events=pair_events,
    )
    summaries = pd.read_parquet(result["summary_parquet"])

    assert result["matched_movement_count"] == 1
    assert summaries["primary_pair_relevance"].dtype.kind == "f"
    assert summaries["primary_source_verified"].dtype.name == "boolean"


def test_large_market_ledger_reader_uses_only_requested_tail(tmp_path) -> None:
    ledger = tmp_path / "market_movements.csv"
    ledger.write_text(
        "movement_key,instrument,start_utc,end_utc,direction\n"
        + "".join(
            f"move_{index},EUR_USD,2026-07-27T12:00:00Z,"
            f"2026-07-27T12:01:00Z,LONG\n"
            for index in range(100)
        ),
        encoding="utf-8",
    )

    rows = tagger.read_csv_tail(ledger, 5)

    assert [row["movement_key"] for row in rows] == [
        "move_95",
        "move_96",
        "move_97",
        "move_98",
        "move_99",
    ]
