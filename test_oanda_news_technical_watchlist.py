import datetime as dt
import json
import sqlite3

import oanda_news_technical_watchlist as watch


UTC = dt.timezone.utc


def _history_rows(instrument, mids, start_epoch, pip=0.0001, spread_pips=1.0):
    half = spread_pips * pip / 2.0
    return [
        {
            "instrument": instrument,
            "minute_epoch": start_epoch + index * 60,
            "close_bid": mid - half,
            "close_ask": mid + half,
            "pip": pip,
        }
        for index, mid in enumerate(mids)
    ]


def test_persistent_sma_requires_alignment_without_recross():
    rows = _history_rows("EUR_USD", [1.0 + index * 0.0001 for index in range(65)], 0)
    state = watch.persistent_sma_confirmation(
        "EUR_USD", "long", {"EUR_USD": rows}
    )
    assert state["state"] == "aligned_persistent"
    rows[-1]["close_bid"] -= 0.02
    rows[-1]["close_ask"] -= 0.02
    unstable = watch.persistent_sma_confirmation(
        "EUR_USD", "long", {"EUR_USD": rows}
    )
    assert unstable["state"] != "aligned_persistent"


def test_initial_reaction_uses_post_event_window_and_executable_threshold():
    event = dt.datetime(2026, 8, 18, 6, 0, tzinfo=UTC)
    start = int(event.timestamp()) - 60
    rows = _history_rows(
        "GBP_USD", [1.3000, 1.2998, 1.2995, 1.2992], start
    )
    quote = {"pip": 0.0001, "spread_pips": 1.0}
    state = watch.initial_reaction_state(
        "GBP_USD", "short", event, quote, {"GBP_USD": rows}
    )
    assert state["state"] == "aligned"
    assert state["signed_move_pips"] >= 8.0


def test_remaining_move_assessment_is_cost_age_path_and_direction_gated():
    model = {
        "predicted_magnitude_pips": 10.0,
        "predicted_clear_probability": 0.7,
        "predicted_direction": "long",
    }
    priced = {
        "state": "not_detected",
        "causal_valid": True,
        "signed_pre_event_move_pips": 1.0,
    }
    eligible = watch.remaining_move_assessment(
        model,
        priced,
        expected_direction="long",
        factor_age_sec=60.0,
        factor_horizon_min=15,
        modeled_cost_pips=2.0,
    )
    assert eligible["eligible"] is True
    assert eligible["conservative_remaining_magnitude_pips"] == 9.0
    assert eligible["required_remaining_magnitude_pips"] == 3.0
    assert eligible["research_only"] is True
    assert eligible["execution_eligible"] is False

    priced_in = watch.remaining_move_assessment(
        model,
        {
            "state": "possibly_priced_in",
            "causal_valid": True,
            "signed_pre_event_move_pips": 8.0,
        },
        expected_direction="long",
        factor_age_sec=60.0,
        factor_horizon_min=15,
        modeled_cost_pips=2.0,
    )
    assert priced_in["eligible"] is False
    assert "possibly_priced_in" in priced_in["negative_control_reasons"]
    assert (
        "insufficient_remaining_move_after_cost_margin"
        in priced_in["negative_control_reasons"]
    )

    conflicted = watch.remaining_move_assessment(
        {**model, "predicted_direction": "short"},
        priced,
        expected_direction="long",
        factor_age_sec=901.0,
        factor_horizon_min=15,
        modeled_cost_pips=2.0,
    )
    assert conflicted["eligible"] is False
    assert "model_direction_conflicts_with_news" in conflicted[
        "negative_control_reasons"
    ]
    assert "declared_reaction_horizon_elapsed" in conflicted[
        "negative_control_reasons"
    ]


def test_watchlist_database_has_cohort_summary_index(tmp_path):
    connection = watch.open_database(tmp_path / "watchlist.sqlite")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index'"
        )
    }
    connection.close()
    assert "watchlist_cohort_status_arm" in indexes
    assert "cohort_id,status,arm" in indexes["watchlist_cohort_status_arm"].replace(
        " ", ""
    )


def test_official_release_fast_arm_requires_two_aligned_pair_legs(tmp_path):
    observed = dt.datetime(2026, 8, 18, 6, 4, tzinfo=UTC)
    event = observed - dt.timedelta(minutes=4)
    start = int(event.timestamp()) - 60
    factors = [{
        "episode_id": "ons-labour",
        "currency": "GBP",
        "score": -0.35,
        "confidence": 0.5,
        "horizon_min": 60,
        "first_known_utc": watch.iso(event),
        "category": "labor_release",
        "structured_event": True,
        "official_release_research": True,
        "uncorroborated_research": False,
        "event_series_id": "ons_uk_labour_release_package",
        "reference_period": "2026-06",
        "topic_ids": ["ons-topic"],
        "headlines": ["UK Labour Market"],
        "terms": ["labour"],
        "article_count": 1,
        "publisher_count": 1,
        "episode_topic_count": 1,
        "source_ids": ["ons_published_releases"],
    }]
    quote_time = watch.iso(observed)
    quotes = {
        "GBP_USD": {"bid": 1.2991, "ask": 1.2992, "mid": 1.29915, "pip": .0001, "spread_pips": 1.0, "fresh": True, "time": quote_time},
        "GBP_CAD": {"bid": 1.7991, "ask": 1.7992, "mid": 1.79915, "pip": .0001, "spread_pips": 1.0, "fresh": True, "time": quote_time},
        "EUR_GBP": {"bid": 0.8508, "ask": 0.8509, "mid": 0.85085, "pip": .0001, "spread_pips": 1.0, "fresh": True, "time": quote_time},
    }
    history = {
        "GBP_USD": _history_rows("GBP_USD", [1.3000, 1.2998, 1.2995, 1.2992], start),
        "GBP_CAD": _history_rows("GBP_CAD", [1.8000, 1.7998, 1.7995, 1.7992], start),
        "EUR_GBP": _history_rows("EUR_GBP", [0.8500, 0.8502, 0.8505, 0.8508], start),
    }
    entries = watch.build_watchlist(
        factors, {}, quotes, history, observed,
        tmp_path / "macro.sqlite", tmp_path / "rates.json",
    )
    fast = [row for row in entries if row["arm"] == "official_release_fast_multileg_h15"]
    assert len(fast) == 3
    assert all(row["execution_eligible"] is False for row in fast)
    surprise_rate = [
        row
        for row in entries
        if row["arm"] == "official_surprise_rate_negative_control_h15"
    ]
    assert len(surprise_rate) == 3
    assert all(row["execution_eligible"] is False for row in surprise_rate)
    assert all(
        "causal_pre_release_consensus_unavailable"
        in row["official_surprise_rate_assessment"]["negative_control_reasons"]
        for row in surprise_rate
    )


def test_uncorroborated_risk_factor_rejects_wrong_way_pair(tmp_path):
    observed = dt.datetime(2026, 8, 18, 10, 4, tzinfo=UTC)
    event = observed - dt.timedelta(minutes=4)
    start = int(event.timestamp()) - 60
    factor = [{
        "episode_id": "risk-event",
        "currency": "JPY",
        "score": 0.4,
        "confidence": 0.5,
        "horizon_min": 60,
        "first_known_utc": watch.iso(event),
        "category": "risk_off_geopolitical_or_financial",
        "uncorroborated_research": True,
        "official_release_research": False,
        "event_series_id": "",
        "reference_period": "",
        "topic_ids": ["risk-topic"],
        "headlines": ["Risk event"],
        "terms": ["risk"],
        "article_count": 1,
        "publisher_count": 1,
        "episode_topic_count": 1,
        "source_ids": ["secondary"],
    }]
    quotes = {
        "USD_JPY": {"bid": 150.09, "ask": 150.10, "mid": 150.095, "pip": .01, "spread_pips": 1.0, "fresh": True, "time": watch.iso(observed)},
    }
    # JPY-strength implies USD/JPY short, but price rose after the event.
    history = {
        "USD_JPY": _history_rows("USD_JPY", [150.00, 150.02, 150.05, 150.09], start, pip=.01),
    }
    entries = watch.build_watchlist(
        factor, {}, quotes, history, observed,
        tmp_path / "macro.sqlite", tmp_path / "rates.json",
    )
    assert not any(str(row.get("arm", "")).startswith("uncorroborated_news") for row in entries)


def topic_provenance(**overrides):
    value = {
        "collector_contract_id": watch.COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": watch.COLLECTOR_COHORT_ID,
        "observation_time_contract_id": watch.OBSERVATION_TIME_CONTRACT_ID,
        "observation_clock_trusted": True,
    }
    value.update(overrides)
    return value


def test_macro_headlines_share_one_episode():
    base = {
        "category": "inflation",
        "currencies": ["EUR"],
        "first_known_utc": "2026-07-31T09:00:00Z",
        "payload": {},
    }
    a = watch.canonical_episode_id({**base, "topic_id": "a", "topic_signature": "one"})
    b = watch.canonical_episode_id({**base, "topic_id": "b", "topic_signature": "two"})
    assert a == b


def test_missing_consensus_and_rates_are_not_neutral(tmp_path):
    missing_macro = watch.lookup_macro_context(
        {"currency": "EUR", "event_series_id": "cpi", "reference_period": "2026-07"},
        tmp_path / "missing.sqlite",
        dt.datetime(2026, 8, 8, tzinfo=UTC),
    )
    rates = watch.lookup_rate_context("EUR", tmp_path / "missing.json", dt.datetime(2026, 8, 8, tzinfo=UTC))
    state = watch.verification_state(missing_macro, rates, {"state": "insufficient_history"})
    assert not missing_macro["causal_valid"]
    assert not rates["causal_valid"]
    assert state == "missing_consensus_and_rates"


def test_official_surprise_rate_binding_requires_two_causal_aligned_sources():
    confirmed = watch.official_surprise_rate_assessment(
        {
            "causal_valid": True,
            "standardized_surprise": 1.2,
            "directional_interpretation": "JPY_strengthening",
        },
        {"causal_valid": True, "change_bps_15m": 2.4},
        expected_currency_direction="long",
    )
    assert confirmed["eligible"] is True
    assert confirmed["macro_direction"] == "long"
    assert confirmed["rate_direction"] == "long"
    assert confirmed["research_only"] is True
    assert confirmed["execution_eligible"] is False

    conflicted = watch.official_surprise_rate_assessment(
        {
            "causal_valid": True,
            "standardized_surprise": 1.2,
            "directional_interpretation": "JPY_strengthening",
        },
        {"causal_valid": True, "change_bps_15m": -2.4},
        expected_currency_direction="long",
    )
    assert conflicted["eligible"] is False
    assert "rate_repricing_conflicts_with_source_thesis" in conflicted[
        "negative_control_reasons"
    ]

    missing = watch.official_surprise_rate_assessment(
        {
            "causal_valid": False,
            "standardized_surprise": None,
            "directional_interpretation": "pending_series_semantics",
        },
        {"causal_valid": False},
        expected_currency_direction="short",
    )
    assert missing["eligible"] is False
    assert "causal_pre_release_consensus_unavailable" in missing[
        "negative_control_reasons"
    ]
    assert "event_time_rate_repricing_unavailable" in missing[
        "negative_control_reasons"
    ]


def test_macro_context_requires_exact_collector_clock_provenance(tmp_path):
    path = tmp_path / "macro.sqlite"
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE macro_release_revisions(
             row_id INTEGER,release_key TEXT,causal_known_utc TEXT,
             scheduled_utc TEXT,event_series_id TEXT,
             known_before_recorded_timestamp INTEGER,
             actual_value REAL,consensus_value REAL,
             standardized_surprise REAL,directional_interpretation TEXT,
             payload_json TEXT)"""
    )
    known = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    connection.execute(
        "INSERT INTO macro_release_revisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            1, "release", watch.iso(known), watch.iso(known), "JP_POLICY", 1,
            0.75, 0.5, 1.2, "JPY_strengthening",
            json.dumps(topic_provenance(observation_clock_trusted=False)),
        ),
    )
    connection.commit()
    connection.close()
    factor = {"event_series_id": "JP_POLICY"}

    blocked = watch.lookup_macro_context(factor, path, known)
    assert blocked["state"] == "unbound_macro_source_provenance"
    assert blocked["causal_valid"] is False

    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE macro_release_revisions SET payload_json=?",
        (json.dumps(topic_provenance()),),
    )
    connection.commit()
    connection.close()
    accepted = watch.lookup_macro_context(factor, path, known)
    assert accepted["state"] == "causal_actual_consensus_available"
    assert accepted["causal_valid"] is True


def test_intraday_rate_context_requires_clock_bound_cohort(tmp_path):
    path = tmp_path / "rates.json"
    observed = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    row = {
        "observed_utc": watch.iso(observed),
        "change_bps_15m": 2.0,
        "change_bps_60m": 3.0,
        "instrument": "JPY_2Y",
        "source_id": "rates",
    }
    path.write_text(
        json.dumps({"currencies": {"JPY": row}}), encoding="utf-8"
    )
    blocked = watch.lookup_rate_context("JPY", path, observed)
    assert blocked == {
        "state": "unbound_rate_repricing_provenance",
        "causal_valid": False,
    }

    row.update(
        {
            "observation_clock_trusted": True,
            "observation_time_contract_id": watch.OBSERVATION_TIME_CONTRACT_ID,
            "collector_cohort_id": watch.RATE_REPRICING_CLOCK_BOUND_COHORT_ID,
        }
    )
    path.write_text(
        json.dumps({"currencies": {"JPY": row}}), encoding="utf-8"
    )
    accepted = watch.lookup_rate_context("JPY", path, observed)
    assert accepted["state"] == "available"
    assert accepted["causal_valid"] is True


def test_daily_rate_context_never_impersonates_intraday_repricing(tmp_path):
    path = tmp_path / "daily_rates.json"
    path.write_text(
        json.dumps(
            {
                "currencies": {
                    "GBP": {
                        "observed_utc": "2026-08-16T07:30:00Z",
                        "rate_date": "2026-08-13",
                        "rate_pct": 4.15,
                        "change_bps_1d": -3.6,
                        "source_id": "bank_of_england_ois_spot_2y",
                        "source_contract_id": "official_boe_ois_spot_2y_v1",
                        "cohort_id": "daily-rate-cohort",
                        "observation_kind": "bootstrap_current_view",
                        "prospective_eligible": False,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    context = watch.lookup_daily_rate_context(
        "GBP", path, dt.datetime(2026, 8, 16, 8, 0, tzinfo=UTC)
    )
    assert context["state"] == "bootstrap_current_view"
    assert context["underlying_observation_fresh"] is True
    assert context["rate_age_calendar_days"] == 3
    assert not context["causal_valid"]
    assert not context["intraday_rate_confirmation"]
    assert context["direction_policy"] == "abstain"


def test_stale_underlying_rate_date_is_not_refreshed_by_collector_poll(tmp_path):
    path = tmp_path / "daily_rates.json"
    path.write_text(
        json.dumps(
            {
                "currencies": {
                    "JPY": {
                        "observed_utc": "2026-08-16T08:00:00Z",
                        "rate_date": "2026-07-31",
                        "rate_pct": 1.507,
                        "source_id": "japan_mof_constant_maturity_2y",
                        "observation_kind": "new_rate_date_first_observed",
                        "prospective_eligible": True,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    context = watch.lookup_daily_rate_context(
        "JPY", path, dt.datetime(2026, 8, 16, 8, 1, tzinfo=UTC)
    )
    assert context["state"] == "stale_daily_rate_context"
    assert context["rate_age_calendar_days"] == 16
    assert context["underlying_observation_fresh"] is False
    assert context["causal_valid"] is False
    assert context["proof_eligible"] is False


def test_fresh_prospective_daily_rate_requires_clock_bound_cohort(tmp_path):
    path = tmp_path / "daily_rates.json"
    observed = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    row = {
        "observed_utc": watch.iso(observed),
        "rate_date": "2026-08-17",
        "rate_pct": 1.0,
        "change_bps_1d": 4.0,
        "source_id": "japan_mof_constant_maturity_2y",
        "prospective_eligible": True,
    }
    path.write_text(
        json.dumps({"currencies": {"JPY": row}}), encoding="utf-8"
    )
    blocked = watch.lookup_daily_rate_context("JPY", path, observed)
    assert blocked["state"] == "unbound_daily_rate_provenance"
    assert blocked["causal_valid"] is False
    assert blocked["proof_eligible"] is False

    row.update(
        {
            "observation_clock_trusted": True,
            "observation_time_contract_id": watch.OBSERVATION_TIME_CONTRACT_ID,
            "collector_cohort_id": watch.DAILY_RATE_CLOCK_BOUND_COHORT_ID,
        }
    )
    path.write_text(
        json.dumps({"currencies": {"JPY": row}}), encoding="utf-8"
    )
    accepted = watch.lookup_daily_rate_context("JPY", path, observed)
    assert accepted["state"] == "prospective_daily_context"
    assert accepted["causal_valid"] is True


def test_thesis_lifecycle_separates_alignment_conflict_and_expiry():
    observed = dt.datetime(2026, 8, 15, 12, 0, tzinfo=UTC)
    active = watch.thesis_lifecycle_state(
        observed=observed,
        expires=observed + dt.timedelta(minutes=30),
        news_direction="short",
        technical_direction="neutral",
        priced={"state": "not_detected"},
    )
    aligned = watch.thesis_lifecycle_state(
        observed=observed,
        expires=observed + dt.timedelta(minutes=30),
        news_direction="short",
        technical_direction="short",
        priced={"state": "not_detected"},
    )
    contradicted = watch.thesis_lifecycle_state(
        observed=observed,
        expires=observed + dt.timedelta(minutes=30),
        news_direction="short",
        technical_direction="long",
        priced={"state": "not_detected"},
    )
    expired = watch.thesis_lifecycle_state(
        observed=observed,
        expires=observed - dt.timedelta(seconds=1),
        news_direction="short",
        technical_direction="short",
        priced={"state": "possibly_priced_in"},
    )
    assert active == {"state": "active_unresolved", "basis": "no_price_confirmation"}
    assert aligned == {"state": "price_aligned", "basis": "technical_direction"}
    assert contradicted == {"state": "contradicted", "basis": "technical_direction"}
    assert expired == {
        "state": "expired",
        "basis": "declared_reaction_horizon_elapsed",
    }


def test_signed_currency_factors_and_correlated_jpy_clustering():
    assert watch.signed_currency_factors("USD_JPY", "long") == ["USD:long", "JPY:short"]
    assert watch.signed_currency_factors("EUR_JPY", "short") == ["EUR:short", "JPY:long"]
    rows = [
        {"arm": "technical_only", "instrument": "USD_JPY", "direction": "long"},
        {"arm": "technical_only", "instrument": "EUR_JPY", "direction": "long"},
        {"arm": "technical_only", "instrument": "GBP_CHF", "direction": "short"},
        {"arm": "news_only", "instrument": "USD_JPY", "direction": "long"},
    ]
    result = watch.current_technical_factor_clusters(rows)
    assert result["raw_entry_count"] == 3
    assert result["cluster_count"] == 2
    assert any(cluster["instruments"] == ["EUR_JPY", "USD_JPY"] for cluster in result["clusters"])


def test_factor_diagnostics_do_not_change_frozen_runtime_policy(tmp_path):
    observed = dt.datetime(2026, 8, 10, 14, 0, tzinfo=UTC)
    payload = watch.run(
        news_db=tmp_path / "news.sqlite",
        macro_db=tmp_path / "macro.sqlite",
        signals_path=tmp_path / "signals.json",
        quotes_path=tmp_path / "quotes.json",
        history_path=tmp_path / "history.json",
        rates_path=tmp_path / "rates.json",
        opportunity_path=tmp_path / "opportunity.sqlite",
        policy_state_path=tmp_path / "policy.json",
        database_path=tmp_path / "watch.sqlite",
        state_path=tmp_path / "watch.json",
        report_path=tmp_path / "watch.md",
        observed=observed,
    )
    assert payload["status"] == "blocked_clock_integrity"
    assert (
        payload["required_news_classification_version"]
        == watch.NEWS_CLASSIFICATION_VERSION
    )
    assert payload["observation_clock"]["trusted_for_prospective_evidence"] is False
    assert payload["diagnostics"]["technical_pair_rows_clustered_by_signed_currency_factor"]
    assert "technical_pair_rows_clustered_by_signed_currency_factor" not in payload["policy"]


def test_watchlist_has_separate_arms_and_bounded_pair_fanout(tmp_path):
    observed = dt.datetime(2026, 8, 8, 14, 0, tzinfo=UTC)
    factors = [{
        "episode_id": "episode-1", "currency": "EUR", "score": 0.8,
        "confidence": 0.7, "horizon_min": 60,
        "first_known_utc": watch.iso(observed - dt.timedelta(minutes=10)),
        "event_series_id": "", "reference_period": "", "topic_ids": ["t1"],
        "headlines": ["ECB signal"], "terms": ["ecb"],
        "article_count": 12, "publisher_count": 5,
        "episode_topic_count": 1, "source_ids": ["ecb", "wire"],
    }]
    quotes = {}
    for pair in ["EUR_USD", "EUR_GBP", "EUR_JPY", "EUR_CHF"]:
        quotes[pair] = {"bid": 1.0, "ask": 1.0001, "mid": 1.00005, "pip": .0001,
                        "spread_pips": 1.0, "fresh": True, "time": watch.iso(observed)}
    quotes["EUR_USD"]["spread_pips"] = 0.8
    technical = {"EUR_USD": {"direction": "long", "confidence": .6,
                               "projected_net_pips": 2.0, "horizon_min": 60, "source_id": "s"}}
    history = {
        "EUR_USD": _history_rows(
            "EUR_USD",
            [0.99 + index * 0.0002 for index in range(65)],
            int(observed.timestamp()) - 64 * 60,
        )
    }
    entries = watch.build_watchlist(factors, technical, quotes, history, observed,
                                    tmp_path / "macro.sqlite", tmp_path / "rates.json")
    assert sum(row["arm"] == "news_only" for row in entries) == 3
    assert any(row["arm"] == "news_technical_confirmed" for row in entries)
    assert any(row["arm"] == "technical_only" for row in entries)
    assert all(row["execution_eligible"] is False for row in entries)
    news_row = next(row for row in entries if row["arm"] == "news_only")
    assert news_row["news_factor_age_sec"] == 600.0
    assert news_row["news_factor_article_count"] == 12
    assert news_row["news_factor_publisher_count"] == 5
    assert news_row["news_repetition_refreshes_causal_age"] is False
    technical_row = next(row for row in entries if row["arm"] == "technical_only")
    assert technical_row["signed_currency_factors"] == ["EUR:long", "USD:short"]


def test_news_direction_uses_magnitude_model_only_for_pair_and_horizon_rank(tmp_path):
    observed = dt.datetime(2026, 8, 14, 6, 2, tzinfo=UTC)
    factor = {
        "episode_id": "boj-rate-episode",
        "currency": "JPY",
        "score": 0.95,
        "confidence": 0.8,
        "horizon_min": 180,
        "first_known_utc": watch.iso(observed - dt.timedelta(minutes=4)),
        "event_series_id": "",
        "reference_period": "",
        "topic_ids": ["boj-topic"],
        "headlines": ["BOJ eyes faster tightening"],
        "terms": ["boj", "rate_hike"],
    }
    quotes = {
        pair: {
            "bid": 150.0,
            "ask": 150.0 + spread * 0.01,
            "mid": 150.0 + spread * 0.005,
            "pip": 0.01,
            "spread_pips": spread,
            "fresh": True,
            "time": watch.iso(observed),
        }
        for pair, spread in {
            "USD_JPY": 1.4,
            "EUR_JPY": 2.4,
            "AUD_JPY": 2.2,
            "ZAR_JPY": 2.0,
        }.items()
    }
    clear = {
        "USD_JPY": 0.74,
        "EUR_JPY": 0.58,
        "AUD_JPY": 0.37,
        "ZAR_JPY": 0.11,
    }
    opportunity = {}
    for pair, probability in clear.items():
        for horizon in (300, 900, 1800):
            opportunity[(pair, horizon)] = {
                "forecast_id": f"{pair}-{horizon}",
                "issued_at_utc": "2026-08-14T06:01:00+00:00",
                "age_sec": 60.0,
                "predicted_clear_probability": probability,
                "predicted_magnitude_pips": 3.0,
                "modeled_entry_cost_pips": quotes[pair]["spread_pips"],
                "predicted_direction": "short" if pair != "AUD_JPY" else "long",
            }
    # The new comparison must retain a liquid expression even when the
    # magnitude adapter has no row; it becomes a negative control.
    opportunity.pop(("ZAR_JPY", 900))
    start_epoch = int(observed.timestamp()) - 64 * 60
    history = {
        pair: _history_rows(
            pair,
            [
                150.5 - index * 0.007
                if pair in {"USD_JPY", "EUR_JPY"}
                else 149.5 + index * 0.007
                if pair == "AUD_JPY"
                else 150.0
                for index in range(65)
            ],
            start_epoch,
            pip=.01,
            spread_pips=quotes[pair]["spread_pips"],
        )
        for pair in quotes
    }
    rows = watch.build_watchlist(
        [factor],
        {},
        quotes,
        history,
        observed,
        tmp_path / "macro.sqlite",
        tmp_path / "rates.json",
        {},
        opportunity,
    )
    ranked_h15 = [row for row in rows if row["arm"] == "news_magnitude_ranked_h15"]
    assert [row["instrument"] for row in ranked_h15] == [
        "USD_JPY",
        "EUR_JPY",
        "AUD_JPY",
    ]
    assert all(row["direction"] == "short" for row in ranked_h15)
    assert all(
        row["cohort_id"] == watch.NEWS_MAGNITUDE_COHORTS[row["arm"]]
        for row in ranked_h15
    )
    confirmed_h15 = [
        row
        for row in rows
        if row["arm"] == "news_magnitude_direction_confirmed_h15"
    ]
    assert [row["instrument"] for row in confirmed_h15] == ["USD_JPY", "EUR_JPY"]
    ranked_h30 = [row for row in rows if row["arm"] == "news_magnitude_ranked_h30"]
    assert [row["instrument"] for row in ranked_h30] == [
        "USD_JPY",
        "EUR_JPY",
        "AUD_JPY",
    ]
    assert all(row["horizon_min"] == 30 for row in ranked_h30)
    assert all(row["execution_eligible"] is False for row in ranked_h15)
    remaining_h15 = [
        row
        for row in rows
        if row["arm"]
        in {
            "news_remaining_move_cost_clear_h15",
            "news_remaining_move_negative_control_h15",
        }
    ]
    assert {row["instrument"] for row in remaining_h15} == set(quotes)
    assert len(remaining_h15) == 4
    assert all(row["pair_expression_count"] == 4 for row in remaining_h15)
    assert all(row["execution_eligible"] is False for row in remaining_h15)
    assert all(
        row["cohort_id"] == watch.NEWS_REMAINING_MOVE_COHORTS[row["arm"]]
        for row in remaining_h15
    )
    missing_model = next(
        row for row in remaining_h15 if row["instrument"] == "ZAR_JPY"
    )
    assert missing_model["arm"] == "news_remaining_move_negative_control_h15"
    assert "model_direction_unavailable" in missing_model[
        "remaining_move_assessment"
    ]["negative_control_reasons"]


def test_opportunity_loader_is_point_in_time_and_uses_latest_forecast(tmp_path):
    database = tmp_path / "opportunity.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE forecasts (
            forecast_id TEXT, cohort_id TEXT, issued_at_utc TEXT, instrument TEXT,
            horizon_sec INTEGER, predicted_clear_probability REAL,
            predicted_up_probability REAL, predicted_direction INTEGER,
            predicted_direction_confidence REAL, predicted_magnitude_pips REAL,
            predicted_ev_pips REAL, entry_spread_pips REAL,
            modeled_entry_cost_pips REAL
        )
        """
    )
    connection.executemany(
        "INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("v1-newer", "executable_opportunity_ranking_v1_20260808.collector.old", "2026-08-14T06:01:30Z", "USD_JPY", 300, .99, .9, 1, .8, 9., 1., 1.5, 1.7),
            ("older", "executable_opportunity_ranking_v2_20260814.collector.test", "2026-08-14T06:00:00Z", "USD_JPY", 300, .4, .4, -1, .2, 2., -1., 1.5, 1.7),
            ("latest", "executable_opportunity_ranking_v2_20260814.collector.test", "2026-08-14T06:01:00Z", "USD_JPY", 300, .7, .6, 1, .2, 4., -1., 1.5, 1.7),
            ("h30", "executable_opportunity_ranking_v2_20260814.collector.test", "2026-08-14T06:01:00Z", "USD_JPY", 1800, .8, .4, -1, .2, 6., -1., 1.5, 1.7),
            ("future", "executable_opportunity_ranking_v2_20260814.collector.test", "2026-08-14T06:03:00Z", "EUR_JPY", 300, .9, .4, -1, .2, 5., -1., 2.5, 2.8),
        ],
    )
    connection.commit()
    connection.close()
    rows, state = watch.load_opportunity_forecasts(
        database,
        dt.datetime(2026, 8, 14, 6, 2, tzinfo=UTC),
        "executable_opportunity_ranking_v2_20260814.collector.test",
    )
    assert state == "fresh"
    assert set(rows) == {("USD_JPY", 300), ("USD_JPY", 1800)}
    assert rows[("USD_JPY", 300)]["forecast_id"] == "latest"
    assert rows[("USD_JPY", 300)]["predicted_direction"] == "long"
    assert rows[("USD_JPY", 1800)]["forecast_id"] == "h30"


def test_retired_opportunity_source_is_not_a_live_watchlist_input():
    assert watch.opportunity_source_is_retired({"runtime_mode": "mature_only"})
    assert watch.opportunity_source_is_retired({"runtime_mode": "drain_only"})
    assert watch.opportunity_source_is_retired(
        {"runtime_mode": "active", "future_forecast_production": False}
    )
    assert not watch.opportunity_source_is_retired(
        {"runtime_mode": "active", "future_forecast_production": True}
    )


def test_supervisor_binds_watchlist_to_current_classification_contract() -> None:
    supervisor = (watch.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    block = supervisor.split('-Name "news_technical_watchlist"', 1)[1].split(
        '-Name "event_technical_preflight"', 1
    )[0]
    assert (
        'ExpectedJsonField = "required_news_classification_version"'
        in block
    )
    assert watch.NEWS_CLASSIFICATION_VERSION in block


def test_technical_only_episode_is_stable_across_poll_cycles(tmp_path):
    observed = dt.datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    technical = {
        "EUR_USD": {
            "direction": "long", "confidence": 0.6,
            "projected_net_pips": 2.0, "horizon_min": 60,
            "source_id": "frozen-upstream-signal",
        }
    }
    quotes = {
        "EUR_USD": {
            "bid": 1.0, "ask": 1.0001, "mid": 1.00005, "pip": 0.0001,
            "spread_pips": 1.0, "fresh": True, "time": watch.iso(observed),
        }
    }
    first = watch.build_watchlist([], technical, quotes, {}, observed)
    second = watch.build_watchlist(
        [], technical, quotes, {}, observed + dt.timedelta(minutes=1)
    )
    assert first[0]["episode_id"] == second[0]["episode_id"]
    assert watch.entry_identifier(
        first[0]["episode_id"], "EUR_USD", "technical_only", "long", 60
    ) == watch.entry_identifier(
        second[0]["episode_id"], "EUR_USD", "technical_only", "long", 60
    )


def test_sampling_revision_uses_new_parented_cohort():
    assert watch.COHORT_ID == (
        "news_technical_watchlist_v40_comparison_arms_20260824"
    )
    assert watch.PARENT_COHORT_ID == (
        "news_technical_watchlist_v39_energy_exporter_ambiguity_20260818"
    )
    assert watch.RECONFIRMATION_PARENT_COHORT_ID == (
        "news_technical_reconfirmation_h15_v31_persistent_sma_20260818"
    )
    assert watch.OFFICIAL_RELEASE_FAST_CONFIRMATION_PARENT_COHORT_ID == (
        "news_technical_watchlist_v38_event_novelty_release_bundle_20260818"
    )
    assert watch.COHORT_ID != watch.PARENT_COHORT_ID


def test_h30_extension_starts_new_magnitude_cohorts():
    assert watch.NEWS_MAGNITUDE_COHORTS["news_magnitude_ranked_h30"] == (
        "news_magnitude_ranked_h30_v27_energy_exporter_ambiguity_20260818"
    )
    assert watch.NEWS_MAGNITUDE_COHORTS[
        "news_magnitude_direction_confirmed_h30"
    ] == "news_magnitude_direction_confirmed_h30_v27_energy_exporter_ambiguity_20260818"
    assert all(
        "_v33_energy_exporter_ambiguity_20260818" in value
        for arm, value in watch.NEWS_MAGNITUDE_COHORTS.items()
        if not arm.endswith("h30")
    )


def test_factor_loader_fails_closed_on_stale_classifier_payload(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE topic_events (
            topic_id TEXT PRIMARY KEY, topic_signature TEXT,
            first_known_utc TEXT, published_utc TEXT, category TEXT,
            direct_currencies_json TEXT, article_count INTEGER,
            distinct_source_count INTEGER, payload_json TEXT
        )
        """
    )
    observed = dt.datetime(2026, 8, 14, 13, 4, tzinfo=UTC)
    payload = {
        **topic_provenance(),
        "prospective_provenance_known_utc": watch.iso(observed),
        "classification_version": "obsolete_rules",
        "directional_publish_eligible": True,
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "context_only": False,
        "currency_scores": {"USD": 0.9},
        "estimated_reaction_horizon_minutes": 60,
    }
    connection.execute(
        "INSERT INTO topic_events VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "topic", "signature", watch.iso(observed), watch.iso(observed),
            "market_news", '["USD"]', 1, 1, json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()
    assert watch.load_currency_factors(database, observed) == []
    connection = sqlite3.connect(database)
    payload["classification_version"] = watch.NEWS_CLASSIFICATION_VERSION
    connection.execute(
        "UPDATE topic_events SET payload_json=? WHERE topic_id='topic'",
        (json.dumps(payload),),
    )
    connection.commit()
    connection.close()
    factors = watch.load_currency_factors(database, observed)
    assert len(factors) == 1
    assert factors[0]["currency"] == "USD"


def test_official_duration_liquidity_policy_enters_only_fast_confirmation_arm(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE topic_events (
            topic_id TEXT PRIMARY KEY, topic_signature TEXT,
            first_known_utc TEXT, published_utc TEXT, category TEXT,
            direct_currencies_json TEXT, article_count INTEGER,
            distinct_source_count INTEGER, payload_json TEXT
        )
        """
    )
    observed = dt.datetime(2026, 8, 19, 12, 35, tzinfo=UTC)
    known = observed - dt.timedelta(minutes=4)
    payload = {
        **topic_provenance(),
        "prospective_provenance_known_utc": watch.iso(known),
        "classification_version": watch.NEWS_CLASSIFICATION_VERSION,
        "directional_publish_eligible": False,
        "directional_source_grade": "verified_primary_or_publisher",
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "context_only": True,
        "research_currency_scores": {"USD": -0.55},
        "research_directional_basis": (
            "official_duration_liquidity_policy_requires_rate_and_price_confirmation"
        ),
        "estimated_reaction_horizon_minutes": 60,
        "headline": "Treasury Announces Increased Sizes of Nominal Long-End",
        "source_ids": ["us_treasury_press"],
    }
    connection.execute(
        "INSERT INTO topic_events VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "topic", "duration-liquidity", watch.iso(known), watch.iso(known),
            "sovereign_duration_liquidity_policy", '["USD"]', 1, 1,
            json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()
    factors = watch.load_currency_factors(database, observed)
    assert len(factors) == 1
    assert factors[0]["currency"] == "USD"
    assert factors[0]["official_release_research"] is True
    assert factors[0]["uncorroborated_research"] is False


def test_uncorroborated_secondary_topic_is_isolated_shadow_response(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE topic_events (
            topic_id TEXT PRIMARY KEY, topic_signature TEXT,
            first_known_utc TEXT, published_utc TEXT, category TEXT,
            direct_currencies_json TEXT, article_count INTEGER,
            distinct_source_count INTEGER, payload_json TEXT
        )
        """
    )
    observed = dt.datetime(2026, 8, 14, 15, 21, tzinfo=UTC)
    payload = {
        **topic_provenance(),
        "prospective_provenance_known_utc": watch.iso(observed - dt.timedelta(minutes=4)),
        "classification_version": watch.NEWS_CLASSIFICATION_VERSION,
        "directional_publish_eligible": False,
        "directional_source_grade": "secondary_requires_corroboration",
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "context_only": True,
        "context_reason": "secondary_uncorroborated_directional_context",
        "research_currency_scores": {"CHF": 0.4, "JPY": 0.4},
        "estimated_reaction_horizon_minutes": 360,
        "headline": "Drone attack targets tanker in Hormuz",
        "source_ids": ["secondary_discovery"],
    }
    connection.execute(
        "INSERT INTO topic_events VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "topic", "risk-off", watch.iso(observed - dt.timedelta(minutes=4)), watch.iso(observed),
            "risk_off_geopolitical_or_financial", "[]", 1, 1,
            json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()
    factors = watch.load_currency_factors(database, observed)
    assert {row["currency"] for row in factors} == {"CHF", "JPY"}
    assert all(row["uncorroborated_research"] for row in factors)
    quotes = {
        "CHF_JPY": {
            "bid": 200.0, "ask": 200.01, "mid": 200.005, "pip": 0.01,
            "spread_pips": 1.0, "fresh": True, "time": watch.iso(observed),
        }
    }
    history = {
        "CHF_JPY": _history_rows(
            "CHF_JPY", [200.0, 200.02, 200.05, 200.09],
            int(observed.timestamp()) - 5 * 60,
            pip=.01,
        )
    }
    rows = watch.build_watchlist(factors, {}, quotes, history, observed)
    assert {row["horizon_min"] for row in rows} == {5, 15, 30}
    assert all(row["arm"].startswith("uncorroborated_news_response_h") for row in rows)
    assert all(row["execution_eligible"] is False for row in rows)


def test_legacy_untrusted_topic_cannot_seed_v36_factor(tmp_path):
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE topic_events (
            topic_id TEXT PRIMARY KEY, topic_signature TEXT,
            first_known_utc TEXT, published_utc TEXT, category TEXT,
            direct_currencies_json TEXT, article_count INTEGER,
            distinct_source_count INTEGER, payload_json TEXT
        )
        """
    )
    observed = dt.datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
    payload = {
        **topic_provenance(
            collector_contract_id="local_news_incremental_source_commit_v38",
            collector_cohort_id="local_news_incremental_source_commit_v38",
            observation_clock_trusted=False,
        ),
        "prospective_provenance_known_utc": watch.iso(observed),
        "classification_version": watch.NEWS_CLASSIFICATION_VERSION,
        "directional_publish_eligible": True,
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "context_only": False,
        "currency_scores": {"JPY": 0.8},
        "estimated_reaction_horizon_minutes": 60,
    }
    connection.execute(
        "INSERT INTO topic_events VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "legacy", "policy|JPY", watch.iso(observed), watch.iso(observed),
            "monetary_policy", '["JPY"]', 1, 1, json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()

    assert watch.load_currency_factors(database, observed) == []


def test_uncorroborated_response_samples_one_pair_per_event_currency_horizon(tmp_path):
    connection = watch.open_database(tmp_path / "watch.sqlite")
    observed = dt.datetime(2026, 8, 14, 16, 0, tzinfo=UTC)
    base = {
        "episode_id": "secondary-event",
        "currency": "JPY",
        "horizon_min": 5,
        "arm": "uncorroborated_news_response_h5",
        "direction": "short",
        "news_score": 0.4,
        "technical_confidence": None,
        "verification_state": "missing_consensus_and_rates",
        "terms": ["hormuz"],
        "cohort_id": watch.UNCORROBORATED_NEWS_COHORTS[5],
        "entry_quote": {
            "bid": 150.0, "ask": 150.01, "mid": 150.005, "pip": 0.01,
            "spread_pips": 1.0,
        },
    }
    first = {**base, "instrument": "USD_JPY"}
    second = {
        **base,
        "instrument": "EUR_JPY",
        "entry_quote": {**base["entry_quote"], "bid": 160.0, "ask": 160.01, "mid": 160.005},
    }
    assert watch.persist_entries(connection, [first], observed) == 1
    assert watch.persist_entries(connection, [second], observed + dt.timedelta(minutes=1)) == 0
    assert connection.execute(
        "SELECT instrument,COUNT(*) FROM watchlist_entries"
    ).fetchone() == ("USD_JPY", 1)


def test_uncorroborated_v1_rows_are_retained_but_invalidated(tmp_path):
    connection = watch.open_database(tmp_path / "watch.sqlite")
    observed = dt.datetime(2026, 8, 14, 15, 46, tzinfo=UTC)
    entry = {
        "episode_id": "transition", "currency": "JPY", "instrument": "USD_JPY",
        "horizon_min": 5, "arm": "uncorroborated_news_response_h5",
        "direction": "short", "news_score": 0.4, "technical_confidence": None,
        "verification_state": "missing_consensus_and_rates", "terms": [],
        "cohort_id": watch.UNCORROBORATED_NEWS_PARENT_COHORTS[5],
        "entry_quote": {"bid": 150.0, "ask": 150.01, "mid": 150.005,
                        "pip": 0.01, "spread_pips": 1.0},
    }
    assert watch.persist_entries(connection, [entry], observed) == 1
    assert watch.invalidate_uncorroborated_sampling_transition(connection) == 1
    assert connection.execute(
        "SELECT status FROM watchlist_entries"
    ).fetchone()[0] == "invalid_sampling_transition"


def test_maturity_rejects_quote_far_from_declared_horizon(tmp_path):
    db = watch.open_database(tmp_path / "watch.sqlite")
    observed = dt.datetime(2026, 8, 8, 14, 0, tzinfo=UTC)
    entry = {
        "episode_id": "e", "currency": "EUR", "instrument": "EUR_USD",
        "horizon_min": 60, "arm": "news_only", "direction": "long",
        "news_score": .5, "technical_confidence": None,
        "verification_state": "missing_consensus_and_rates", "terms": ["inflation"],
        "entry_quote": {"bid": 1.0, "ask": 1.0001, "mid": 1.00005, "pip": .0001,
                        "spread_pips": 1.0},
    }
    assert watch.persist_entries(db, [entry], observed) == 1
    late = observed + dt.timedelta(minutes=70)
    quote = {"EUR_USD": {"bid": 1.001, "ask": 1.0011, "mid": 1.00105, "pip": .0001,
                         "fresh": True, "time": watch.iso(late)}}
    assert watch.mature_entries(db, quote, late) == 0
    exact = observed + dt.timedelta(minutes=60)
    quote["EUR_USD"]["time"] = watch.iso(exact)
    assert watch.mature_entries(db, quote, exact) == 1


def test_term_features_keep_category_and_action():
    terms = watch.term_features("Inflation slows sharply", category="inflation", action="slows")
    assert "inflation_slows" in terms
    assert "category:inflation" in terms
    assert "action:slows" in terms


def test_term_spike_statistics_suppresses_low_support_without_dropping_raw_evidence():
    summary = watch.term_spike_statistics(
        {
            "inflation": [(1.0, 0.5)] * 9,
            "category:inflation": [(1.0, 0.5)] * 10,
        }
    )

    assert [row["term"] for row in summary["rows"]] == ["category:inflation"]
    assert summary["rows"][0]["n"] == 10
    assert summary["policy"] == {
        "minimum_matured_observations": 10,
        "maximum_reported_terms": 100,
        "candidate_term_count": 2,
        "supported_term_count": 1,
        "suppressed_low_support_count": 1,
        "raw_evidence_retained": True,
        "evidence_unit": "term_x_news_episode_x_currency_factor",
    }


def test_term_spike_values_collapse_correlated_pairs_by_event_currency_factor():
    values = watch.collapse_term_factor_episode_values(
        [
            ("event-a", "JPY", '["policy"]', 4.0, 2.0),
            ("event-a", "JPY", '["policy"]', 2.0, -2.0),
            ("event-b", "JPY", '["policy"]', -1.0, -3.0),
        ]
    )

    assert values == {"policy": [(3.0, 0.0), (-1.0, -3.0)]}


def test_watchlist_carries_persistent_policy_as_nonexecuting_context(tmp_path):
    observed = dt.datetime(2026, 8, 10, 14, 0, tzinfo=UTC)
    factor = {
        "episode_id": "episode-policy",
        "currency": "JPY",
        "score": 0.6,
        "confidence": 0.7,
        "horizon_min": 60,
        "first_known_utc": watch.iso(observed),
        "event_series_id": "",
        "reference_period": "",
        "topic_ids": ["t1"],
        "headlines": ["BOJ statement"],
        "terms": ["boj"],
    }
    quote = {
        "USD_JPY": {
            "bid": 150.0,
            "ask": 150.01,
            "mid": 150.005,
            "pip": 0.01,
            "spread_pips": 1.0,
            "fresh": True,
            "time": watch.iso(observed),
        }
    }
    policy = {
        "research_only": True,
        "execution_eligible": False,
        "currencies": {"JPY": {"currency": "JPY", "state": "HAWKISH"}},
    }
    rows = watch.build_watchlist(
        [factor], {}, quote, {}, observed,
        tmp_path / "macro.sqlite", tmp_path / "rates.json", policy,
    )
    assert rows[0]["persistent_policy_context"]["state"] == "HAWKISH"
    assert rows[0]["execution_eligible"] is False


def test_reconfirmation_is_a_separate_prospective_h15_transition_cohort(tmp_path):
    connection = watch.open_database(tmp_path / "watch.sqlite")
    observed = dt.datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    base = {
        "episode_id": "boj-episode",
        "currency": "JPY",
        "instrument": "USD_JPY",
        "horizon_min": 180,
        "arm": "news_only",
        "direction": "short",
        "news_direction": "short",
        "news_score": 0.9,
        "technical_direction": "neutral",
        "technical_confidence": None,
        "technical_source_id": "",
        "verification_state": "missing_consensus_and_rates",
        "terms": ["boj"],
        "entry_quote": {
            "bid": 159.0,
            "ask": 159.01,
            "mid": 159.005,
            "pip": 0.01,
            "spread_pips": 1.0,
        },
        "research_only": True,
        "execution_eligible": False,
    }
    # Initialization is not allowed to masquerade as a reconfirmation.
    assert watch.build_reconfirmation_entries(connection, [base], observed) == []

    confirmed = {
        **base,
        "technical_direction": "short",
        "technical_confidence": 0.61,
        "technical_source_id": "frozen-signal-a",
        "persistent_sma_confirmation": {"state": "aligned_persistent"},
    }
    rows = watch.build_reconfirmation_entries(
        connection, [confirmed], observed + dt.timedelta(minutes=12)
    )
    assert len(rows) == 1
    assert rows[0]["arm"] == "news_technical_reconfirmed_h15"
    assert rows[0]["horizon_min"] == 15
    assert rows[0]["cohort_id"] == watch.RECONFIRMATION_COHORT_ID
    assert rows[0]["execution_eligible"] is False
    assert rows[0]["reconfirmation_transition"] == "neutral_to_confirmed"
    assert watch.persist_entries(connection, rows, observed + dt.timedelta(minutes=12)) == 1
    stored = connection.execute(
        "SELECT cohort_id,horizon_min,arm FROM watchlist_entries"
    ).fetchone()
    assert tuple(stored) == (
        watch.RECONFIRMATION_COHORT_ID,
        15,
        "news_technical_reconfirmed_h15",
    )

    # A continuously agreeing state is not repeatedly sampled.
    assert watch.build_reconfirmation_entries(
        connection, [confirmed], observed + dt.timedelta(minutes=13)
    ) == []
