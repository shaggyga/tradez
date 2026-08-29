import oanda_currency_event_technical_coverage_audit as audit


def test_universe_from_quotes_uses_exact_pair_keys():
    instruments = audit.universe_from_quotes(
        {"quotes": {"EUR_USD": {}, "USD_JPY": {}, "bad": {}}}
    )

    assert instruments == ["EUR_USD", "USD_JPY"]
    assert audit.pair_currencies(instruments) == ["EUR", "JPY", "USD"]


def test_currency_rows_do_not_confuse_technical_breadth_with_causal_depth():
    rows = audit.build_currency_rows(
        ["EUR_USD", "USD_JPY"],
        [
            {
                "source_id": "fed",
                "source_population": "official_policy_publisher",
                "configured_currencies": ["USD"],
                "timely_directional": 1,
                "actual": 0,
                "structured": 1,
                "consensus": 0,
                "causal_consensus": 0,
            },
            {
                "source_id": "us_macro",
                "source_population": "official_macro_publisher",
                "configured_currencies": ["USD"],
                "actual": 2,
                "prospective_actual": 1,
                "structured": 2,
                "consensus": 1,
                "causal_consensus": 1,
            },
        ],
        {"USD"},
        {"USD"},
        {"USD"},
        True,
        {"USD": 2},
        {"USD": 1},
    )
    by_currency = {row["currency"]: row for row in rows}

    assert by_currency["USD"]["pair_leg_count"] == 2
    assert by_currency["USD"]["technical_join"] is True
    assert by_currency["USD"]["structured_numeric_parser"] is True
    assert by_currency["USD"]["full_causal_stack"] is True
    assert by_currency["USD"]["prospective_actual_value_observations"] == 1
    assert by_currency["EUR"]["technical_join"] is True
    assert by_currency["EUR"]["full_causal_stack"] is False
    assert "no_causal_pre_release_consensus" in by_currency["EUR"]["blockers"]
    assert "no_structured_numeric_parser" in by_currency["EUR"]["blockers"]


def test_explicit_numeric_adapter_reports_capability_before_first_actual():
    rows = audit.build_currency_rows(
        ["AUD_USD"],
        [],
        set(),
        set(),
        set(),
        True,
        {},
        {},
        {"AUD"},
    )
    by_currency = {row["currency"]: row for row in rows}
    assert by_currency["AUD"]["structured_numeric_parser"] is True
    assert by_currency["AUD"]["actual_value_observations"] == 0
    assert "no_structured_actual_value" in by_currency["AUD"]["blockers"]
    assert "no_structured_numeric_parser" not in by_currency["AUD"]["blockers"]


def test_pair_rows_distinguish_one_leg_from_both_leg_economic_depth():
    currency_rows = [
        {
            "currency": "EUR",
            "official_policy_sources": ["ecb"],
            "official_macro_sources": ["eurostat"],
            "structured_numeric_parser": True,
            "actual_value_observations": 2,
            "prospective_actual_value_observations": 1,
            "daily_rate_context": True,
            "fresh_daily_rate_context": True,
            "daily_rate_comparable_2y": True,
            "causal_pre_release_consensus": 0,
            "future_scheduled_events": 1,
            "technical_join": True,
            "full_causal_stack": False,
        },
        {
            "currency": "USD",
            "official_policy_sources": ["fed"],
            "official_macro_sources": ["bls"],
            "structured_numeric_parser": True,
            "actual_value_observations": 0,
            "prospective_actual_value_observations": 0,
            "daily_rate_context": False,
            "fresh_daily_rate_context": False,
            "daily_rate_comparable_2y": False,
            "causal_pre_release_consensus": 1,
            "future_scheduled_events": 0,
            "technical_join": True,
            "full_causal_stack": False,
        },
    ]

    row = audit.build_pair_rows(["EUR_USD"], currency_rows)[0]

    assert row["numeric_parser_both_legs"] is True
    assert row["actual_value_any_leg"] is True
    assert row["actual_value_both_legs"] is False
    assert row["prospective_actual_any_leg"] is True
    assert row["prospective_actual_both_legs"] is False
    assert row["future_event_clock_any_leg"] is True
    assert row["future_event_clock_both_legs"] is False
    assert row["daily_rate_any_leg"] is True
    assert row["daily_rate_both_legs"] is False
    assert row["fresh_daily_rate_any_leg"] is True
    assert row["fresh_daily_rate_both_legs"] is False
    assert row["comparable_2y_rate_any_leg"] is True
    assert row["causal_consensus_any_leg"] is True
    assert row["causal_consensus_both_legs"] is False
    assert audit.pair_leg_grade(True, False) == "one"


def test_rate_source_and_freshness_are_explicit():
    rows = audit.build_currency_rows(
        ["JPY_USD"],
        [],
        {"JPY"},
        set(),
        set(),
        True,
        rate_context_by_currency={
            "JPY": {
                "source_id": "mof_2y",
                "provider": "Japan Ministry of Finance",
                "rate_date": "2026-07-31",
                "tenor_label": "2Y",
                "rate_measure": "two_year_rate",
                "comparison_group": "two_year_market_rate_context",
            }
        },
        as_of_date=audit.dt.date(2026, 8, 16),
    )
    jpy = next(row for row in rows if row["currency"] == "JPY")
    assert jpy["official_rate_sources"] == ["mof_2y"]
    assert jpy["daily_rate_context"] is True
    assert jpy["fresh_daily_rate_context"] is False
    assert jpy["daily_rate_age_calendar_days"] == 16
    assert jpy["daily_rate_comparable_2y"] is True
    assert "stale_daily_rate_context" in jpy["blockers"]


def test_future_schedule_counts_separate_macro_and_policy_clocks():
    as_of = audit.news.parse_datetime("2026-08-16T05:00:00Z")
    all_events, policy = audit.future_schedule_counts(
        [
            {
                "scheduled_utc": "2026-08-18T12:30:00Z",
                "currencies": ["USD"],
                "category": "growth_release",
                "headline": "Housing starts",
            },
            {
                "scheduled_utc": "2026-08-20T11:00:00Z",
                "currencies": ["GBP"],
                "category": "monetary_policy",
                "headline": "Bank Rate decision",
            },
            {
                "scheduled_utc": "2026-08-15T12:30:00Z",
                "currencies": ["USD"],
                "category": "inflation_release",
            },
        ],
        as_of,
    )
    assert all_events == {"GBP": 1, "USD": 1}
    assert policy == {"GBP": 1}


def test_linked_currency_uses_driver_clock_without_claiming_independence():
    rows = audit.build_currency_rows(
        ["EUR_DKK"],
        [],
        set(),
        set(),
        set(),
        True,
        {"EUR": 2},
        {"EUR": 2},
        policy_driver_by_currency={
            "DKK": {
                "driver_currency": "EUR",
                "relationship": "fixed_exchange_rate_policy_against_euro",
                "factor_independence": "same_eur_policy_factor",
                "can_confirm": False,
            }
        },
    )
    dkk = next(row for row in rows if row["currency"] == "DKK")
    assert dkk["future_scheduled_policy_decisions"] == 0
    assert dkk["future_inherited_policy_decisions"] == 2
    assert dkk["future_relevant_policy_decisions"] == 2
    assert dkk["inherited_policy_factor_independence"] == "same_eur_policy_factor"
    assert dkk["inherited_policy_clock_can_confirm"] is False
    assert "no_future_relevant_policy_decision_clock" not in dkk["blockers"]


def test_rbnz_policy_source_is_first_party_and_research_mapped():
    config = audit.read_json(audit.ROOT / "config" / "news_sources_v1.json")
    source = next(
        row
        for row in config["sources"]
        if row.get("source_id") == "rbnz_policy_decisions_direct_v1"
    )

    assert source["verified"] is True
    assert source["direct"] is True
    assert source["runtime_supported"] is False
    assert source["currencies"] == ["NZD"]
    assert source["source_role"] == "primary_policy_release"
    assert source["trusted_domains"] == ["rbnz.govt.nz"]
