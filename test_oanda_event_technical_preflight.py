import datetime as dt
import json
import sqlite3

import oanda_event_technical_preflight as preflight


UTC = dt.timezone.utc


def fixtures():
    events = [
        {
            "event_id": "fed_clock",
            "raw": {"event_series_id": "fed_policy_decision"},
            "headline": "Federal Reserve Monetary Policy Decision",
            "category": "monetary_policy",
            "severity": 90.0,
            "movement_potential": "HIGH",
            "scheduled_utc": "2026-09-16T18:00:00+00:00",
            "reference_period": "2026-09",
            "currencies": ["USD"],
        },
        {
            "event_id": "boj_window",
            "headline": "Bank of Japan Monetary Policy Decision Release Window",
            "category": "monetary_policy",
            "severity": 90.0,
            "movement_potential": "HIGH",
            "scheduled_utc": "2026-09-18T00:00:00+00:00",
            "currencies": ["JPY"],
            "timing_precision": "date_window",
            "schedule_window_end_utc": "2026-09-18T06:00:00+00:00",
        },
    ]
    quotes = {
        "generated_utc": "2026-08-16T06:00:00+00:00",
        "quotes": {
            "EUR_USD": {"bid": 1.0, "ask": 1.0001},
            "USD_JPY": {"bid": 150.0, "ask": 150.01},
        },
    }
    signals = {
        "updated_at": "2026-08-16T06:00:00+00:00",
        "top_signals": [
            {"instrument": "EUR_USD", "direction": "long"},
            {"instrument": "USD_JPY", "direction": "short"},
        ],
    }
    coverage = {
        "currencies": [
            {"currency": "USD", "causal_pre_release_consensus": 0, "daily_rate_context": True},
            {"currency": "JPY", "causal_pre_release_consensus": 0, "daily_rate_context": False},
        ]
    }
    return events, quotes, signals, coverage


def test_preflight_joins_scheduled_events_to_pair_legs_without_direction():
    events, quotes, signals, coverage = fixtures()
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
    )
    assert payload["priced_pair_count"] == 2
    assert payload["scheduled_policy_currency_rows"] == 2
    assert {row["currency"] for row in payload["events"]} == {"USD", "JPY"}
    assert all(row["direction"] == "unknown_until_causal_release_evidence" for row in payload["events"])
    assert all(row["execution_eligible"] is False for row in payload["events"])
    assert all(row["technical_pair_count"] >= 1 for row in payload["events"])
    fed = next(row for row in payload["events"] if row["event_id"] == "fed_clock")
    assert fed["event_series_id"] == "fed_policy_decision"
    assert fed["reference_period"] == "2026-09"


def test_policy_release_transport_readiness_keeps_fallback_separate_from_direct():
    events, quotes, signals, coverage = fixtures()
    official_sources = {
        "currencies": [
            {
                "currency": "USD",
                "authority_id": "fed",
                "release_source_ids": ["fed_policy_release"],
                "calendar_source_ids": ["fed_calendar"],
            },
            {
                "currency": "JPY",
                "authority_id": "boj",
                "release_source_ids": ["boj_policy_release"],
                "calendar_source_ids": ["boj_calendar"],
            },
        ]
    }
    source_coverage = {
        "currencies": {
            "USD": {
                "sources": [
                    {
                        "source_id": "fed_policy_release",
                        "operational": True,
                        "healthy": True,
                        "runtime_status": "enabled",
                    },
                    {
                        "source_id": "fed_calendar",
                        "operational": True,
                        "healthy": True,
                    },
                ]
            },
            "JPY": {
                "sources": [
                    {
                        "source_id": "boj_policy_release",
                        "operational": False,
                        "healthy": False,
                        "runtime_status": "unsupported",
                    },
                    {
                        "source_id": "boj_calendar",
                        "operational": True,
                        "healthy": True,
                    },
                    {
                        "source_id": "boj_official_search",
                        "source_role": "news_aggregator",
                        "operational": True,
                        "healthy": True,
                    },
                ]
            },
        }
    }
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
        official_sources=official_sources,
        source_coverage=source_coverage,
    )
    fed = next(row for row in payload["events"] if row["currency"] == "USD")
    boj = next(row for row in payload["events"] if row["currency"] == "JPY")
    assert fed["policy_release_transport_state"] == "direct_release_ready"
    assert fed["policy_direct_release_transport_available"] is True
    assert boj["policy_release_transport_state"] == (
        "fallback_only_direct_release_unavailable"
    )
    assert boj["policy_direct_release_transport_available"] is False
    assert boj["policy_fallback_transport_available"] is True
    assert boj["policy_calendar_transport_available"] is True
    assert boj["policy_source_blockers"] == ["boj_policy_release:unsupported"]
    assert payload["policy_direct_release_ready_rows"] == 1
    assert payload["policy_fallback_only_rows"] == 1
    assert payload["policy_release_transport_blocked_rows"] == 0
    assert payload["policy"][
        "publisher_search_fallback_is_not_direct_release_transport"
    ] is True


def test_preflight_restores_reference_period_from_exact_calendar_identity():
    events = [{
        "event_id": "claims",
        "scheduled_utc": "2026-08-27T12:30:00Z",
        "raw": {"event_series_id": "dol_ui_weekly_claims_bundle"},
    }]
    context = [{
        "event_series_id": "dol_ui_weekly_claims_bundle",
        "scheduled_utc": "2026-08-27T08:30:00-04:00",
        "reference_period": "2026-08-22",
    }]
    restored = preflight.preserve_reference_periods(events, context)
    assert restored[0]["reference_period"] == "2026-08-22"
    conflict = preflight.preserve_reference_periods(
        events,
        context + [{**context[0], "reference_period": "2026-08-21"}],
    )
    assert conflict[0]["reference_period"] == ""


def test_preflight_reads_calendar_periods_that_aged_out_of_latest_snapshot(tmp_path):
    database = tmp_path / "calendar.sqlite"
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE articles (source_kind TEXT, payload_json TEXT)")
        connection.execute(
            "INSERT INTO articles VALUES (?, ?)",
            (
                "census_release_calendar",
                json.dumps({
                    "event_series_id": (
                        "advance economic indicators report international trade "
                        "retail wholesale"
                    ),
                    "scheduled_utc": "2026-08-27T12:30:00Z",
                    "reference_period": "July 2026",
                }),
            ),
        )
        connection.commit()
    finally:
        connection.close()
    rows = preflight.canonical_calendar_articles(database)
    assert rows[0]["reference_period"] == "July 2026"


def test_preflight_adds_small_sample_explicit_direction_abstaining_magnitude_prior():
    events, quotes, signals, coverage = fixtures()
    historical = {
        "evidence_class": "historical_availability_counterfactual",
        "generated_utc": "2026-08-24T23:00:00Z",
        "source_event_count": 4,
        "independent_source_time_count": 2,
        "details": [
            {"release_key": "release-1", "currency": "USD"},
            {"release_key": "release-2", "currency": "USD"},
        ],
        "episode_horizon_rows": [
            {"release_key": "release-1", "event_series_id": "fed_policy_decision",
             "event_name": "Federal Reserve Monetary Policy Decision",
             "horizon_sec": 3600, "median_absolute_currency_bps": 8.0,
             "cost_clear_pair_fraction": 0.75},
            {"release_key": "release-2", "event_series_id": "fed_policy_decision",
             "event_name": "Federal Reserve Monetary Policy Decision",
             "horizon_sec": 3600, "median_absolute_currency_bps": 4.0,
             "cost_clear_pair_fraction": 0.25},
        ],
        "independent_control_episode_horizon_rows": [
            {"currency": "USD", "horizon_sec": 3600,
             "release_keys": ["control|release-1"],
             "median_absolute_currency_bps": 2.0, "cost_clear_pair_fraction": 0.1},
            {"currency": "USD", "horizon_sec": 3600,
             "release_keys": ["control|release-2"],
             "median_absolute_currency_bps": 2.0, "cost_clear_pair_fraction": 0.1},
        ],
    }
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
        historical_magnitude=historical,
    )
    usd = next(row for row in payload["events"] if row["currency"] == "USD")
    prior = usd["movement_risk_priors"]["3600"]
    assert prior["scope"] == "release_series"
    assert prior["event"]["independent_episode_n"] == 2
    assert prior["event"]["mean_absolute_currency_bps"] == 6.0
    assert prior["event_minus_control_mean_absolute_bps"] == 4.0
    assert prior["direction_policy"] == "abstain"
    assert payload["policy"]["historical_magnitude_prior_assigns_no_direction"] is True
    assert payload["execution_eligible"] is False


def test_minor_unmatched_release_cannot_inherit_global_policy_or_event_prior():
    events, quotes, signals, coverage = fixtures()
    events = [{
        "event_id": "minor_nz_poultry",
        "raw": {"event_series_id": "primary production poultry"},
        "headline": "New Zealand Primary production - poultry",
        "category": "market_news",
        "severity": 54.75,
        "movement_potential": "MEDIUM",
        "scheduled_utc": "2026-08-17T00:00:00+00:00",
        "currencies": ["JPY"],
    }]
    historical = {
        "details": [{"release_key": "policy-1", "currency": "JPY"}],
        "episode_horizon_rows": [{
            "release_key": "policy-1", "event_series_id": "boj_policy_decision",
            "event_name": "Bank of Japan Monetary Policy Decision",
            "horizon_sec": 3600, "median_absolute_currency_bps": 50.0,
            "cost_clear_pair_fraction": 1.0,
        }],
        "independent_control_episode_horizon_rows": [{
            "currency": "JPY", "horizon_sec": 3600,
            "release_keys": ["control|policy-1"],
            "median_absolute_currency_bps": 2.0, "cost_clear_pair_fraction": 0.1,
        }],
    }
    payload = preflight.build(
        events, quotes, signals, coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45, historical_magnitude=historical,
    )
    row = payload["events"][0]
    assert row["movement_risk_priors"] == {}
    assert row["movement_risk_state"] == (
        "no_exact_series_prior_and_event_below_class_fallback_threshold"
    )
    assert payload["historical_magnitude_prior"]["generic_event_fallback_allowed"] is False


def test_frozen_rbnz_schedule_history_adds_raw_magnitude_not_direction_or_control():
    events, quotes, signals, coverage = fixtures()
    events = [{
        "event_id": "rbnz_clock",
        "raw": {"event_series_id": "rbnz_policy_decision"},
        "headline": "Reserve Bank of New Zealand Monetary Policy Decision",
        "category": "monetary_policy",
        "severity": 90.0,
        "movement_potential": "HIGH",
        "scheduled_utc": "2026-09-02T02:00:00+00:00",
        "currencies": ["NZD"],
    }]
    quotes["quotes"]["NZD_USD"] = {"bid": 0.6, "ask": 0.6001}
    signals["top_signals"].append({"instrument": "NZD_USD", "direction": "long"})
    coverage["currencies"].append(
        {"currency": "NZD", "causal_pre_release_consensus": 0,
         "daily_rate_context": True}
    )
    schedule_history = {
        "contract_id": "spike_blurb_rbnz_schedule_cohort_v1_20260820",
        "selection_rule": (
            "every_scheduled_rbnz_policy_decision_20240228_through_20250820"
        ),
        "supported_execution_decision": "no_trade",
        "execution_eligible_count": 0,
        "event_count": 2,
        "trajectory_rows": [
            {"event_id": "one", "horizon_minutes": 60,
             "currency_strength_bps": -10.0},
            {"event_id": "two", "horizon_minutes": 60,
             "currency_strength_bps": 30.0},
            # A duplicate event/horizon must not increase independent N.
            {"event_id": "two", "horizon_minutes": 60,
             "currency_strength_bps": 999.0},
        ],
    }
    payload = preflight.build(
        events, quotes, signals, coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45, historical_magnitude={}, specialized_histories=[schedule_history],
    )
    row = payload["events"][0]
    prior = row["movement_risk_priors"]["3600"]
    assert prior["scope"] == "release_series_specialized_schedule_archive"
    assert prior["event"]["independent_episode_n"] == 2
    assert prior["event"]["median_absolute_currency_bps"] == 20.0
    assert prior["matched_control"]["independent_episode_n"] == 0
    assert prior["event_minus_control_mean_absolute_bps"] is None
    assert prior["direction_policy"] == "abstain"
    assert row["direction"] == "unknown_until_causal_release_evidence"
    assert row["execution_eligible"] is False


def test_invalid_or_operational_rbnz_history_is_rejected():
    priors, metadata = preflight._rbnz_schedule_magnitude_priors({
        "contract_id": "spike_blurb_rbnz_schedule_cohort_v1_20260820",
        "selection_rule": (
            "every_scheduled_rbnz_policy_decision_20240228_through_20250820"
        ),
        "supported_execution_decision": "trade",
        "execution_eligible_count": 1,
        "event_count": 2,
        "trajectory_rows": [
            {"event_id": "one", "horizon_minutes": 60,
             "currency_strength_bps": 10.0},
            {"event_id": "two", "horizon_minutes": 60,
             "currency_strength_bps": 20.0},
        ],
    })
    assert priors == {}
    assert metadata["integrity_state"] == "unavailable_or_invalid"
    assert metadata["execution_eligible"] is False


def test_preflight_preserves_uncertain_release_window():
    events, quotes, signals, coverage = fixtures()
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
    )
    boj = next(row for row in payload["events"] if row["event_id"] == "boj_window")
    assert boj["timing_precision"] == "date_window"
    assert boj["schedule_window_end_utc"] == "2026-09-18T06:00:00+00:00"


def test_preflight_missing_precision_is_unknown_never_exact():
    events, quotes, signals, coverage = fixtures()
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
    )
    fed = next(row for row in payload["events"] if row["event_id"] == "fed_clock")
    assert fed["timing_precision"] == "unknown"


def test_preflight_excludes_past_and_out_of_horizon_events():
    events, quotes, signals, coverage = fixtures()
    events += [
        {"event_id": "past", "scheduled_utc": "2026-08-15T00:00:00Z", "currencies": ["USD"]},
        {"event_id": "far", "scheduled_utc": "2027-01-01T00:00:00Z", "currencies": ["USD"]},
    ]
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
    )
    assert {row["event_id"] for row in payload["events"]} == {"fed_clock", "boj_window"}


def test_policy_dependency_adds_readiness_leg_without_direction_or_fake_decision():
    events, quotes, signals, coverage = fixtures()
    quotes["quotes"]["USD_HKD"] = {"bid": 7.75, "ask": 7.7502}
    signals["top_signals"].append({"instrument": "USD_HKD", "direction": "long"})
    coverage["currencies"].append(
        {"currency": "HKD", "causal_pre_release_consensus": 0, "daily_rate_context": False}
    )
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
        {
            "dependencies": [
                {
                    "dependent_currency": "HKD",
                    "driver_currency": "USD",
                    "mechanism": "linked_exchange_rate_base_rate_formula",
                    "assign_direction": False,
                }
            ]
        },
    )
    hkd = next(row for row in payload["events"] if row["currency"] == "HKD")
    assert hkd["policy_dependency"] is True
    assert hkd["direct_event_currency"] is False
    assert hkd["driver_currency"] == "USD"
    assert hkd["direction"] == "unknown_until_causal_release_evidence"
    assert payload["derived_policy_dependency_rows"] == 1


def test_authoritative_direct_currencies_prevent_expanded_policy_legs_becoming_direct():
    events, quotes, signals, coverage = fixtures()
    events[0].update(
        {
            "headline": "Federal Reserve Chair scheduled remarks",
            "currencies": [
                "AUD", "CAD", "CHF", "JPY", "MXN",
                "NOK", "NZD", "SEK", "USD", "ZAR",
            ],
            "direct_currencies": ["USD"],
            "direct_currencies_known": True,
        }
    )
    quotes["quotes"]["USD_HKD"] = {"bid": 7.75, "ask": 7.7502}
    coverage["currencies"].append(
        {"currency": "HKD", "causal_pre_release_consensus": 0,
         "daily_rate_context": False}
    )
    payload = preflight.build(
        events,
        quotes,
        signals,
        coverage,
        dt.datetime(2026, 8, 16, 6, 0, tzinfo=UTC),
        45,
        {
            "dependencies": [
                {
                    "dependent_currency": "HKD",
                    "driver_currency": "USD",
                    "mechanism": "linked_exchange_rate_base_rate_formula",
                    "assign_direction": False,
                }
            ]
        },
    )
    fed_rows = [row for row in payload["events"] if row["event_id"] == "fed_clock"]
    assert {row["currency"] for row in fed_rows} == {"USD", "HKD"}
    assert [
        row["currency"] for row in fed_rows if row["direct_event_currency"]
    ] == ["USD"]
    hkd = next(row for row in fed_rows if row["currency"] == "HKD")
    assert hkd["policy_dependency"] is True
    assert hkd["driver_currency"] == "USD"
    assert not any(
        row["currency"] in {"AUD", "CAD", "CHF", "JPY", "MXN", "NOK", "NZD", "SEK", "ZAR"}
        for row in fed_rows
    )
