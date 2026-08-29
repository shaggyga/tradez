from __future__ import annotations

import datetime as dt
import json
import sqlite3

import oanda_signal_news_monitor as monitor


UTC = dt.timezone.utc


def test_open_database_migrates_legacy_account_columns_to_nullable(tmp_path) -> None:
    database = tmp_path / "monitor.sqlite"
    legacy = sqlite3.connect(database)
    legacy.execute(
        """
        CREATE TABLE cycles (
            observed_utc TEXT PRIMARY KEY,
            signal_updated_utc TEXT NOT NULL,
            signal_count INTEGER NOT NULL,
            qualified_count INTEGER NOT NULL,
            selected_json TEXT NOT NULL,
            news_generated_utc TEXT NOT NULL,
            active_news_articles INTEGER NOT NULL,
            account_nav REAL NOT NULL,
            account_balance REAL NOT NULL,
            open_trades INTEGER NOT NULL,
            pending_orders INTEGER NOT NULL,
            bot_status TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    legacy.execute(
        "INSERT INTO cycles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "2026-07-28T04:00:00+00:00",
            "2026-07-28T04:00:00+00:00",
            1,
            0,
            "null",
            "2026-07-28T04:00:00+00:00",
            0,
            42.5,
            42.5,
            0,
            0,
            "running",
            "{}",
        ),
    )
    legacy.commit()
    legacy.close()

    migrated = monitor.open_database(database)
    columns = {
        str(row[1]): int(row[3] or 0)
        for row in migrated.execute("PRAGMA table_info(cycles)").fetchall()
    }

    assert columns["account_nav"] == 0
    assert columns["account_balance"] == 0
    assert columns["open_trades"] == 0
    assert columns["pending_orders"] == 0
    assert migrated.execute("SELECT COUNT(*) FROM cycles").fetchone()[0] == 1
    migrated.close()


def test_independent_news_rejections_dedupe_topics_and_explain_no_trade() -> None:
    retrospective = {
        "topic_id": "topic-oil-recap",
        "headline": "Oil drops as stocks rise",
        "first_seen_utc": "2026-08-03T22:12:45+00:00",
        "source_name": "Example Wire",
        "forward_pair_eligible": False,
        "forward_signal_timely": False,
        "reports_prior_market_move": True,
        "source_verified": True,
        "distinct_source_count": 2,
        "estimated_reaction_horizon_label": "H3",
    }
    weak = {
        "topic_id": "topic-rumor",
        "headline": "Unconfirmed policy rumor",
        "first_seen_utc": "2026-08-03T22:13:45+00:00",
        "source_name": "Unknown Blog",
        "forward_pair_eligible": False,
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "source_verified": False,
        "distinct_source_count": 1,
        "estimated_reaction_horizon_label": "H1",
    }
    summary = monitor.summarize_independent_news_rejections(
        {
            "pairs": {
                "USD_CAD": {"events": [dict(retrospective), dict(weak)]},
                "USD_NOK": {"events": [dict(retrospective)]},
            }
        }
    )
    assert summary["unique_rejected_topics"] == 2
    assert summary["reason_counts"] == {
        "retrospective_market_move": 1,
        "unverified_or_uncorroborated": 1,
    }
    recap = next(
        row for row in summary["topics"] if row["topic_id"] == "topic-oil-recap"
    )
    assert recap["mapped_pair_count"] == 2


def test_retrospective_rewrites_collapse_to_one_independent_factor() -> None:
    first = {
        "topic_id": "yen-wire-one",
        "headline": "US intervenes to support Japanese yen",
        "category": "fx_intervention",
        "topic_tags": ["#fx_intervention", "#jpy_reported_strengthening"],
        "first_seen_utc": "2026-08-03T20:24:21+00:00",
        "forward_pair_eligible": False,
        "forward_signal_timely": False,
        "reports_prior_market_move": True,
        "source_verified": True,
    }
    rewrite = {
        **first,
        "topic_id": "yen-wire-two",
        "headline": "Yen surges after intervention",
        "first_seen_utc": "2026-08-03T21:58:47+00:00",
        "topic_tags": ["#fx_intervention", "#jpy_context_strengthening"],
    }
    summary = monitor.summarize_independent_news_rejections(
        {
            "pairs": {
                "USD_JPY": {"events": [first, rewrite]},
                "EUR_JPY": {"events": [rewrite]},
            }
        }
    )
    assert summary["unique_rejected_topics"] == 1
    factor = summary["topics"][0]
    assert factor["topic_id"].startswith("news_factor_")
    assert factor["source_topic_ids"] == ["yen-wire-one", "yen-wire-two"]
    assert factor["source_topic_count"] == 2
    assert factor["mapped_pair_count"] == 2


def test_source_topic_identity_accepts_event_catalog_schema() -> None:
    assert monitor.source_news_topic_id({"event_id": "event-topic"}) == "event-topic"


def snapshots(bid: float, ask: float) -> tuple[dict, dict, dict, dict, dict]:
    signal = {
        "updated_at": "2026-07-28T04:00:00+00:00",
        "qualified_signal_count": 0,
        "top_signals": [
            {
                "instrument": "EUR_USD",
                "direction": "sell",
                "direction_state": "long",
                "signal_confidence": 0.6,
                "projected_net_pips": 1.0,
                "preferred_horizon_sec": 60,
                "signal_eligible": False,
                "signal_blocked_by": ["unvalidated_signal"],
            }
        ],
        "market_quotes": {"EUR_USD": {"bid": bid, "ask": ask}},
    }
    account = {
        "environment": "practice",
        "accounts": [
            {
                "env": "practice",
                "NAV": 42.5,
                "balance": 42.5,
                "openTradeCount": 0,
                "pendingOrderCount": 0,
            }
        ],
    }
    heartbeat = {
        "status": "running",
        "phase": "pricing",
        "pid": 1,
        "worker": "strategy_lab",
        "updated_at": "2026-07-28T04:00:00+00:00",
        "details": {"api_errors": 0, "run_label": "test"},
    }
    news = {
        "generated_utc": "2026-07-28T04:00:00+00:00",
        "active_article_count": 1,
        "pairs": {
            "EUR_USD": {
                "direction": "long",
                "score": 0.8,
                "confidence": 0.7,
                "active_event_count": 1,
                "estimated_reaction_horizon_sec": 60,
                "events": [],
                "execution_eligible": False,
            }
        },
    }
    collector = {
        "status": "ok",
        "policy": {"openai_calls": 0},
    }
    return signal, account, heartbeat, news, collector


def test_monitor_matures_executable_outcomes_and_prefers_direction_state(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    first = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    assert first["top_rows"][0]["signal_direction"] == "long"
    assert "news_events" not in first["top_rows"][0]
    assert first["top_rows"][0]["news_topic_ids"] == []
    assert first["bot"]["healthy"] is True
    stored_cycle = json.loads(
        connection.execute("SELECT payload_json FROM cycles").fetchone()[0]
    )
    assert "top_rows" not in stored_cycle
    stored_observation = json.loads(
        connection.execute("SELECT payload_json FROM observations").fetchone()[0]
    )
    assert "news_events" not in stored_observation
    assert stored_observation["news_topic_ids"] == []

    signal["market_quotes"]["EUR_USD"] = {"bid": 1.1003, "ask": 1.1005}
    heartbeat["updated_at"] = "2026-07-28T04:01:00+00:00"
    second = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 1, tzinfo=UTC),
    )
    assert second["matured_outcome_count"] == 1

    summary = monitor.build_summary(
        connection,
        dt.datetime(2026, 7, 28, 13, 0, tzinfo=UTC),
    )
    price = summary["outcome_performance"]["1"]["price_signal"]
    news_only = summary["outcome_performance"]["1"]["news_only"]
    agreement = summary["outcome_performance"]["1"]["agreement_only"]
    assert price["n"] == 1
    assert price["direction_accuracy_pct"] == 100.0
    assert price["mean_executable_bps"] > 0.0
    assert price["cost_buckets"]["low_0_2bps"]["n"] == 1
    assert price["cost_buckets"]["medium_2_5bps"]["n"] == 0
    assert price["nonoverlapping_per_instrument"]["n"] == 1
    assert (
        price["nonoverlapping_per_instrument"]["cost_buckets"]["low_0_2bps"]["n"]
        == 1
    )
    assert price["currency_factor_episode_weighted"]["n"] == 1
    assert price["preferred_horizon_match"]["n"] == 1
    assert news_only["mean_executable_bps"] == price["mean_executable_bps"]
    assert agreement["n"] == 1
    connection.close()


def test_neutral_observations_do_not_create_outcome_rows(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    signal["top_signals"] = []
    news["pairs"]["EUR_USD"]["direction"] = "neutral"
    monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    signal["market_quotes"]["EUR_USD"] = {"bid": 1.1003, "ask": 1.1005}
    monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 1, tzinfo=UTC),
    )
    assert connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
    summary = monitor.build_summary(
        connection,
        dt.datetime(2026, 7, 28, 13, 0, tzinfo=UTC),
    )
    assert summary["signal_direction_counts"] == {"neutral": 2}
    assert summary["news_direction_counts"] == {"neutral": 2}
    connection.close()


def test_missing_heartbeat_is_explicitly_unhealthy(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, _, news, collector = snapshots(1.1000, 1.1002)
    cycle = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat={},
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    assert cycle["bot"]["status"] == "heartbeat_unavailable"
    assert cycle["bot"]["healthy"] is False
    connection.close()


def test_unavailable_account_snapshot_remains_unknown_not_zero_or_flat(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, _, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    account = {
        "environment": "practice",
        "accounts": [
            {
                "account_id": "101-001-37981792-007",
                "env": "practice",
                "ok": False,
                "account_values_current": False,
                "positions_current": False,
                "orders_current": False,
                "status_code": 503,
                "error": "maintenance",
            }
        ],
        "aggregate": {
            "snapshot_state": "unavailable",
            "account_values_current": False,
            "positions_current": False,
            "orders_current": False,
            "nav": None,
            "balance": None,
            "openTradeCount": None,
            "pendingOrderCount": None,
        },
    }

    cycle = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )

    assert cycle["account"] == {
        "suffix": "-007",
        "environment": "practice",
        "account_current": False,
        "account_ok": False,
        "snapshot_state": "unavailable",
        "positions_current": False,
        "orders_current": False,
        "nav": None,
        "balance": None,
        "open_trades": None,
        "pending_orders": None,
        "last_verified": None,
        "status_code": 503,
        "error": "maintenance",
    }
    connection.close()


def test_summary_collapses_correlated_jpy_crosses_to_one_factor_episode(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    forecast = "2026-07-28T04:00:00+00:00"
    rows = []
    for instrument in ("AUD_JPY", "USD_JPY"):
        executable = 1.5 if instrument == "AUD_JPY" else 0.0
        rows.append(
            (
                forecast,
                instrument,
                30,
                "2026-07-28T04:30:00+00:00",
                30.0,
                "short",
                -3.0,
                "short",
                "short",
                "aligned",
                3.0,
                executable,
                3.0,
                executable,
                3.0,
                executable,
                3.0,
                executable,
            )
        )
    connection.executemany(
        """
        INSERT INTO outcomes (
            forecast_utc, instrument, horizon_min, outcome_utc,
            realized_min, actual_direction, gross_return_bps,
            signal_direction, news_direction, relationship,
            signal_gross_bps, signal_executable_bps,
            news_gross_bps, news_executable_bps,
            agreement_gross_bps, agreement_executable_bps,
            news_veto_gross_bps, news_veto_executable_bps
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    summary = monitor.build_summary(
        connection,
        dt.datetime(2026, 7, 28, 13, 0, tzinfo=UTC),
    )
    result = summary["outcome_performance"]["30"]["agreement_only"]
    assert result["nonoverlapping_per_instrument"]["n"] == 2
    assert result["currency_factor_episode_weighted"]["n"] == 1
    assert result["currency_factor_episode_weighted"]["mean_executable_bps"] == 0.75
    assert (
        result["currency_factor_episode_weighted"]["cost_buckets"]["low_0_2bps"]["n"]
        == 0
    )
    assert (
        result["currency_factor_episode_weighted"]["cost_buckets"]["medium_2_5bps"]["n"]
        == 1
    )
    assert sum(
        bucket["n"]
        for bucket in result["currency_factor_episode_weighted"]["cost_buckets"].values()
    ) == result["currency_factor_episode_weighted"]["n"]
    connection.close()


def test_historical_error_counter_does_not_mask_current_health(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    heartbeat["details"].update(
        {
            "api_errors": 5,
            "last_api_error": {
                "kind": "OperationalError",
                "message": "database is locked",
                "time": "2026-07-28T03:00:00+00:00",
            },
        }
    )
    cycle = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    assert cycle["bot"]["healthy"] is True
    assert cycle["bot"]["recent_api_error"] is False
    assert cycle["bot"]["last_api_error_age_sec"] == 3600.0
    connection.close()


def test_recent_error_is_unhealthy(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    heartbeat["details"].update(
        {
            "api_errors": 1,
            "last_api_error": {
                "kind": "OperationalError",
                "message": "database is locked",
                "time": "2026-07-28T03:59:00+00:00",
            },
        }
    )
    cycle = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    assert cycle["bot"]["healthy"] is False
    assert cycle["bot"]["recent_api_error"] is True
    connection.close()


def test_stale_news_is_recorded_as_neutral_and_hard_blocked(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    cycle = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        observed=dt.datetime(2026, 7, 28, 4, 5, 1, tzinfo=UTC),
    )

    assert cycle["news_hard_blocked"] is True
    assert cycle["news_fresh"] is False
    assert cycle["top_rows"][0]["news_direction"] == "neutral"
    assert cycle["top_rows"][0]["news_event_count"] == 0
    connection.close()


def test_independent_news_decision_covers_pair_without_price_signal_and_scores_cost(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    news["pairs"]["USD_JPY"] = {
        "direction": "short",
        "score": -0.7,
        "confidence": 0.6,
        "active_event_count": 1,
        "estimated_reaction_horizon_sec": 60,
        "events": [
            {
                "topic_id": "yen-intervention-topic",
                "headline": "Officials announce yen-buying intervention",
                "category": "fx_intervention",
                "source_name": "official",
                "first_seen_utc": "2026-07-28T04:00:00+00:00",
                "pair_score": -0.7,
                "estimated_reaction_horizon_minutes": 1,
                "availability_lag_minutes": 0.2,
                "forward_pair_eligible": True,
                "forward_signal_timely": True,
                "reports_prior_market_move": False,
            }
        ],
        "execution_eligible": False,
    }
    quotes = {
        "quotes": {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001},
            "USD_JPY": {"bid": 157.00, "ask": 157.02, "pip": 0.01},
        }
    }
    first = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        quote_snapshot=quotes,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    assert any(row["instrument"] == "USD_JPY" for row in first["top_rows"])
    assert first["new_independent_news_decisions"] == 1
    stored = json.loads(
        connection.execute(
            "SELECT payload_json FROM observations WHERE instrument = 'USD_JPY'"
        ).fetchone()[0]
    )
    assert stored["news_topic_ids"] == ["yen-intervention-topic"]
    assert "news_events" not in stored

    news["generated_utc"] = "2026-07-28T04:01:00+00:00"
    heartbeat["updated_at"] = "2026-07-28T04:01:00+00:00"
    quotes["quotes"]["USD_JPY"] = {
        "bid": 156.94,
        "ask": 156.96,
        "pip": 0.01,
    }
    second = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        quote_snapshot=quotes,
        observed=dt.datetime(2026, 7, 28, 4, 1, tzinfo=UTC),
    )
    assert second["matured_independent_news_decisions"] == 1
    summary = monitor.build_summary(
        connection,
        dt.datetime(2026, 7, 28, 13, 0, tzinfo=UTC),
    )
    result = summary["independent_news"]["episode_weighted_results"]["1"]
    assert result["n"] == 1
    assert result["direction_accuracy_pct"] == 100.0
    assert result["positive_after_spread_pct"] == 100.0
    connection.close()


def test_research_only_pair_score_cannot_enter_independent_decision_ledger(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    rows = [
        {
            "instrument": "GBP_USD",
            "news_direction": "long",
            "news_confidence": 0.6,
            "news_horizon_min": 60,
            "quote_fresh": True,
            "bid": 1.3500,
            "ask": 1.3502,
            "mid": 1.3501,
            "pip": 0.0001,
            "news_events": [
                {
                    "topic_id": "secondary-inflation-expectations",
                    "headline": "Inflation expectations rise",
                    "category": "inflation_context",
                    "source_name": "Secondary Wire",
                    "first_seen_utc": "2026-08-26T12:00:00+00:00",
                    "pair_score": 0.5,
                    "forward_pair_eligible": False,
                    "forward_signal_timely": True,
                    "reports_prior_market_move": False,
                }
            ],
        }
    ]
    inserted = monitor.record_independent_news_decisions(
        connection,
        rows,
        dt.datetime(2026, 8, 26, 12, 1, tzinfo=UTC),
    )
    assert inserted == 0
    assert connection.execute(
        "SELECT COUNT(*) FROM independent_news_decisions"
    ).fetchone()[0] == 0
    connection.close()


def test_current_macro_category_aliases_share_factor_identity() -> None:
    common = {
        "first_seen_utc": "2026-08-26T12:00:00+00:00",
        "currency_exposure_groups": ["GBP"],
        "reference_period": "2026-08",
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
    }
    release = monitor.independent_news_factor_id(
        {**common, "topic_id": "release", "category": "inflation_release"}
    )
    context = monitor.independent_news_factor_id(
        {**common, "topic_id": "rewrite", "category": "inflation_context"}
    )
    assert release == context


def test_independent_news_does_not_activate_retained_topic_after_horizon(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    news["pairs"]["USD_JPY"] = {
        "direction": "short",
        "score": -0.7,
        "confidence": 0.6,
        "active_event_count": 1,
        "estimated_reaction_horizon_sec": 10_800,
        "events": [
            {
                "topic_id": "retained-yen-topic",
                "headline": "Officials discuss yen-buying intervention",
                "category": "fx_intervention",
                "source_name": "official",
                "first_seen_utc": "2026-07-28T04:00:00+00:00",
                "pair_score": -0.7,
                "estimated_reaction_horizon_minutes": 180,
                "availability_lag_minutes": 0.2,
                "forward_signal_timely": True,
                "reports_prior_market_move": False,
            }
        ],
        "execution_eligible": False,
    }
    quotes = {
        "quotes": {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001},
            "USD_JPY": {"bid": 157.00, "ask": 157.02, "pip": 0.01},
        }
    }
    cycle = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        quote_snapshot=quotes,
        observed=dt.datetime(2026, 7, 28, 9, 0, tzinfo=UTC),
    )
    assert cycle["new_independent_news_decisions"] == 0
    assert connection.execute(
        "SELECT COUNT(*) FROM independent_news_decisions"
    ).fetchone()[0] == 0
    connection.close()


def test_rejected_retrospective_news_tracks_continuation_without_calling_it_a_miss(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    signal, account, heartbeat, news, collector = snapshots(1.1000, 1.1002)
    news["pairs"]["USD_JPY"] = {
        "direction": "neutral",
        "score": 0.0,
        "confidence": 0.0,
        "active_event_count": 1,
        "estimated_reaction_horizon_sec": 60,
        "events": [
            {
                "topic_id": "retrospective-yen-recap",
                "headline": "Yen strengthens after intervention",
                "category": "fx_intervention",
                "source_name": "Example Wire",
                "first_seen_utc": "2026-07-28T04:00:00+00:00",
                "pair_score": 0.0,
                "research_pair_score": -0.7,
                "estimated_reaction_horizon_minutes": 1,
                "forward_pair_eligible": False,
                "forward_signal_timely": False,
                "reports_prior_market_move": True,
                "source_verified": True,
                "distinct_source_count": 2,
            }
        ],
        "execution_eligible": False,
    }
    quotes = {
        "quotes": {
            "EUR_USD": {"bid": 1.1000, "ask": 1.1002, "pip": 0.0001},
            "USD_JPY": {"bid": 157.00, "ask": 157.02, "pip": 0.01},
        }
    }
    first = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        quote_snapshot=quotes,
        observed=dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC),
    )
    assert first["new_rejected_news_shadows"] == 1
    assert first["new_independent_news_decisions"] == 0

    news["generated_utc"] = "2026-07-28T04:01:00+00:00"
    heartbeat["updated_at"] = "2026-07-28T04:01:00+00:00"
    quotes["quotes"]["USD_JPY"] = {
        "bid": 156.94,
        "ask": 156.96,
        "pip": 0.01,
    }
    second = monitor.record_cycle(
        connection,
        signal_snapshot=signal,
        account_snapshot=account,
        heartbeat=heartbeat,
        news_snapshot=news,
        collector_snapshot=collector,
        quote_snapshot=quotes,
        observed=dt.datetime(2026, 7, 28, 4, 1, tzinfo=UTC),
    )
    assert second["matured_rejected_news_shadows"] == 1
    summary = monitor.build_summary(
        connection,
        dt.datetime(2026, 7, 28, 13, 0, tzinfo=UTC),
    )
    result = summary["independent_news"]["rejected_shadow_audit"]["results"][
        "retrospective_continuation_counterfactual"
    ]["retrospective_market_move"]["1"]
    assert result["n_topic_episodes"] == 1
    assert result["post_recap_continuation_after_cost_pct"] == 100.0
    assert "missed_after_cost_pct" not in result
    connection.close()


def test_rejected_unverified_news_reports_true_after_cost_miss(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    rows = [
        {
            "instrument": "EUR_USD",
            "bid": 1.1000,
            "ask": 1.1002,
            "mid": 1.1001,
            "pip": 0.0001,
            "news_horizon_min": 1,
            "news_events": [
                {
                    "topic_id": "unverified-euro-rumor",
                    "headline": "Unverified euro policy rumor",
                    "category": "central_bank",
                    "source_name": "Unknown Blog",
                    "first_seen_utc": "2026-07-28T04:00:00+00:00",
                    "pair_score": 0.0,
                    "research_pair_score": 0.6,
                    "estimated_reaction_horizon_minutes": 1,
                    "forward_pair_eligible": False,
                    "forward_signal_timely": True,
                    "reports_prior_market_move": False,
                    "source_verified": False,
                    "distinct_source_count": 1,
                }
            ],
        }
    ]
    observed = dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC)
    assert monitor.record_rejected_news_shadows(connection, rows, observed) == 1
    rows[0].update({"bid": 1.1005, "ask": 1.1007, "mid": 1.1006})
    assert monitor.mature_rejected_news_shadows(
        connection,
        rows,
        observed + dt.timedelta(minutes=1),
    ) == 1
    result_class = connection.execute(
        "SELECT result_class FROM rejected_news_shadows"
    ).fetchone()[0]
    assert result_class == "missed_after_cost"
    summary = monitor.build_summary(
        connection,
        dt.datetime(2026, 7, 28, 13, 0, tzinfo=UTC),
    )
    result = summary["independent_news"]["rejected_shadow_audit"]["results"][
        "prospective_gate_counterfactual"
    ]["unverified_or_uncorroborated"]["1"]
    assert result["missed_after_cost_pct"] == 100.0
    assert result["correct_no_trade_pct"] == 0.0
    connection.close()


def test_rejected_news_pair_cap_persists_across_cycles_and_excludes_exotics(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    event = {
        "topic_id": "one-yen-topic",
        "headline": "Yen recap",
        "category": "fx_intervention",
        "source_name": "Example Wire",
        "first_seen_utc": "2026-07-28T04:00:00+00:00",
        "pair_score": 0.0,
        "research_pair_score": -0.7,
        "estimated_reaction_horizon_minutes": 60,
        "forward_pair_eligible": False,
        "reports_prior_market_move": True,
    }

    def row(instrument: str, bid: float, ask: float, pip: float) -> dict:
        return {
            "instrument": instrument,
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "pip": pip,
            "news_horizon_min": 60,
            "news_events": [dict(event)],
        }

    rows = [
        row("USD_JPY", 157.00, 157.02, 0.01),
        row("EUR_JPY", 168.00, 168.03, 0.01),
        row("GBP_JPY", 199.00, 199.04, 0.01),
        row("AUD_JPY", 103.00, 103.05, 0.01),
        row("TRY_JPY", 4.00, 4.01, 0.01),
    ]
    observed = dt.datetime(2026, 7, 28, 4, 0, tzinfo=UTC)
    assert monitor.record_rejected_news_shadows(connection, rows, observed) == 3
    assert monitor.record_rejected_news_shadows(
        connection,
        list(reversed(rows)),
        observed + dt.timedelta(minutes=1),
    ) == 0
    instruments = {
        row[0]
        for row in connection.execute(
            "SELECT instrument FROM rejected_news_shadows"
        ).fetchall()
    }
    assert len(instruments) == 3
    assert "TRY_JPY" not in instruments
    connection.close()


def test_rejected_news_source_topic_identity_drift_does_not_restart_decision(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    event = {
        "topic_id": "stable-source-topic",
        "headline": "Oil slides on diplomacy hopes",
        "category": "commodity_shock",
        "source_name": "Example Wire",
        "first_seen_utc": "2026-08-04T00:00:00+00:00",
        "pair_score": 0.0,
        "research_pair_score": 0.6,
        "estimated_reaction_horizon_minutes": 180,
        "forward_pair_eligible": False,
        "forward_signal_timely": False,
        "reports_prior_market_move": False,
        "source_verified": False,
        "distinct_source_count": 1,
        "topic_tags": ["#commodity_shock", "#oil_oil_down"],
    }
    row = {
        "instrument": "EUR_USD",
        "bid": 1.1000,
        "ask": 1.1002,
        "mid": 1.1001,
        "pip": 0.0001,
        "news_horizon_min": 180,
        "news_events": [event],
    }
    observed = dt.datetime(2026, 8, 4, 0, 1, tzinfo=UTC)
    assert monitor.record_rejected_news_shadows(connection, [row], observed) == 1

    # Corrected timing changes the derived identity back to the source topic,
    # but it remains one causal observation with the original entry price.
    event["forward_signal_timely"] = True
    event["topic_id"] = "reclassified-source-topic"
    assert monitor.record_rejected_news_shadows(
        connection,
        [row],
        observed + dt.timedelta(minutes=10),
    ) == 0
    count, first_decided = connection.execute(
        "SELECT COUNT(*), MIN(decided_utc) FROM rejected_news_shadows"
    ).fetchone()
    assert count == 1
    assert first_decided == monitor.iso_utc(observed)
    connection.close()


def test_rejected_rewrites_share_one_three_leg_factor_cap(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    base_event = {
        "headline": "US intervenes to support Japanese yen",
        "category": "fx_intervention",
        "first_seen_utc": "2026-08-03T20:24:21+00:00",
        "pair_score": 0.0,
        "research_pair_score": -0.7,
        "estimated_reaction_horizon_minutes": 180,
        "forward_pair_eligible": False,
        "forward_signal_timely": False,
        "reports_prior_market_move": True,
        "topic_tags": ["#fx_intervention", "#jpy_reported_strengthening"],
    }
    rewrite_event = {
        **base_event,
        "topic_id": "yen-rewrite-two",
        "headline": "Yen surges after intervention",
        "first_seen_utc": "2026-08-03T21:58:47+00:00",
        "topic_tags": ["#fx_intervention", "#jpy_context_strengthening"],
    }
    base_event["topic_id"] = "yen-rewrite-one"

    def row(instrument: str, bid: float, ask: float, event: dict) -> dict:
        return {
            "instrument": instrument,
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2.0,
            "pip": 0.01,
            "news_horizon_min": 180,
            "news_events": [event],
        }

    rows = [
        row("USD_JPY", 157.00, 157.02, dict(base_event)),
        row("EUR_JPY", 168.00, 168.03, dict(base_event)),
        row("GBP_JPY", 199.00, 199.04, dict(rewrite_event)),
        row("AUD_JPY", 103.00, 103.05, dict(rewrite_event)),
    ]
    inserted = monitor.record_rejected_news_shadows(
        connection,
        rows,
        dt.datetime(2026, 8, 3, 22, 0, tzinfo=UTC),
    )
    assert inserted == 3
    count, factor_count = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT topic_id) FROM rejected_news_shadows"
    ).fetchone()
    assert count == 3
    assert factor_count == 1
    payloads = [
        json.loads(row[0])
        for row in connection.execute(
            "SELECT payload_json FROM rejected_news_shadows"
        ).fetchall()
    ]
    assert {payload["source_topic_id"] for payload in payloads} <= {
        "yen-rewrite-one",
        "yen-rewrite-two",
    }
    connection.close()


def test_reclassified_rejected_shadow_is_quarantined(tmp_path) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    news_database = tmp_path / "news.sqlite"
    news = sqlite3.connect(news_database)
    news.execute(
        """
        CREATE TABLE articles (
            event_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            headline TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    news.execute(
        "INSERT INTO articles VALUES (?, ?, ?, ?, ?)",
        (
            "replacement-event-id",
            "Example Publisher",
            "Euro declines in a local parallel market",
            "2026-08-04T00:00:00+00:00",
            json.dumps(
                {
                    "classification_version": "rules-v2",
                    "localized_parallel_currency_market": True,
                    "exclusion_reason": "localized_parallel_currency_market",
                }
            ),
        ),
    )
    news.commit()
    news.close()
    payload = {
        "source_topic_id": "legacy-event-id",
        "status": "pending",
    }
    connection.execute(
        """
        INSERT INTO rejected_news_shadows (
            decision_id, decided_utc, topic_id, instrument, direction,
            confidence, horizon_min, headline, category, source_name,
            source_first_seen_utc, rejection_reason, decision_kind,
            entry_bid, entry_ask, entry_mid, pip, entry_spread_pips,
            status, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "decision-one",
            "2026-08-04T00:01:00+00:00",
            "legacy-event-id",
            "EUR_USD",
            "short",
            0.7,
            60,
            "Euro declines in a local parallel market",
            "market_news",
            "Example Publisher",
            "2026-08-04T00:00:00+00:00",
            "retrospective_market_move",
            "retrospective_continuation_counterfactual",
            1.1,
            1.1002,
            1.1001,
            0.0001,
            2.0,
            "pending",
            json.dumps(payload),
        ),
    )
    changed = monitor.quarantine_reclassified_rejected_news_shadows(
        connection,
        news_database,
        dt.datetime(2026, 8, 4, 0, 2, tzinfo=UTC),
    )
    assert changed == 1
    status, result_class, stored_payload = connection.execute(
        """
        SELECT status, result_class, payload_json
        FROM rejected_news_shadows
        WHERE decision_id = 'decision-one'
        """
    ).fetchone()
    assert status == "invalidated_classification"
    assert result_class == "excluded_reclassified_news_mapping"
    assert json.loads(stored_payload)["replacement_event_id"] == (
        "replacement-event-id"
    )
    connection.close()


def test_reclassified_independent_decision_is_quarantined_before_maturity(
    tmp_path,
) -> None:
    connection = monitor.open_database(tmp_path / "monitor.sqlite")
    news_database = tmp_path / "news.sqlite"
    news = sqlite3.connect(news_database)
    news.execute(
        """
        CREATE TABLE topic_events (
            topic_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL
        )
        """
    )
    news.execute(
        "INSERT INTO topic_events VALUES (?, ?)",
        (
            "topic-one",
            json.dumps(
                {
                    "classification_version": "rules-v35",
                    "context_only": True,
                    "context_reason": "reported_market_move_context",
                    "reports_prior_market_move": True,
                    "directional_evidence": False,
                    "currency_scores": {},
                }
            ),
        ),
    )
    news.commit()
    news.close()
    payload = {
        "source_topic_id": "legacy-article-id",
        "status": "pending",
        "reports_prior_market_move": False,
    }
    connection.execute(
        """
        INSERT INTO independent_news_decisions (
            decision_id, decided_utc, topic_id, instrument, direction,
            confidence, horizon_min, headline, category, source_name,
            source_first_seen_utc, availability_lag_minutes,
            forward_signal_timely, reports_prior_market_move,
            entry_bid, entry_ask, entry_mid, pip, entry_spread_pips,
            status, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "decision-one",
            "2026-08-04T00:01:00+00:00",
            "topic-one",
            "USD_CAD",
            "short",
            0.6,
            180,
            "Stocks mixed while oil prices rise",
            "commodity_shock",
            "Example Publisher",
            "2026-08-04T00:00:00+00:00",
            1.0,
            1,
            0,
            1.3,
            1.3002,
            1.3001,
            0.0001,
            2.0,
            "pending",
            json.dumps(payload),
        ),
    )
    observed = dt.datetime(2026, 8, 4, 0, 2, tzinfo=UTC)
    changed = monitor.quarantine_reclassified_independent_news_decisions(
        connection,
        news_database,
        observed,
    )
    assert changed == 1
    status, outcome_utc, reports_prior, result_class, stored_payload = (
        connection.execute(
            """
            SELECT status, outcome_utc, reports_prior_market_move,
                   result_class, payload_json
            FROM independent_news_decisions
            WHERE decision_id = 'decision-one'
            """
        ).fetchone()
    )
    assert status == "invalidated_classification"
    assert outcome_utc == monitor.iso_utc(observed)
    assert reports_prior == 1
    assert result_class == "excluded_reclassified_news_mapping"
    stored = json.loads(stored_payload)
    assert stored["classification_correction"] == "rules-v35"
    assert stored["classification_exclusion_reason"] == (
        "reported_market_move_context"
    )
    assert stored["replacement_event_id"] == "topic-one"
    connection.close()
