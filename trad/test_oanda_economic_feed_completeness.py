from __future__ import annotations

import oanda_economic_feed_completeness as audit


def source(source_id: str, role: str, currency: str) -> dict:
    return {
        "source_id": source_id,
        "source_role": role,
        "currencies": [currency],
        "verified": True,
        "direct": True,
    }


def runtime(currency: str, *source_ids: str) -> dict:
    return {
        "currencies": {
            currency: {
                "sources": [
                    {
                        "source_id": source_id,
                        "operational": True,
                        "healthy": True,
                    }
                    for source_id in source_ids
                ]
            }
        }
    }


def depth(
    currency: str,
    *,
    parser: bool = True,
    actuals: int = 1,
    prospective_actuals: int = 1,
    consensus: int = 0,
) -> dict:
    return {
        "currencies": [
            {
                "currency": currency,
                "pair_leg_count": 3,
                "structured_numeric_parser": parser,
                "actual_value_observations": actuals,
                "prospective_actual_value_observations": prospective_actuals,
                "causal_pre_release_consensus": consensus,
                "future_relevant_events": 1,
            }
        ]
    }


def test_minimum_feed_requires_live_policy_macro_parser_and_clock() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    rows = audit.build_currency_rows(
        sources, runtime("AAA", "policy", "macro"), depth("AAA")
    )
    assert rows[0]["minimum_feed_ready"] is True
    assert rows[0]["prospective_surprise_ready"] is False
    assert rows[0]["prospective_actual_value_observations"] == 1
    assert "no_causal_pre_release_consensus" in rows[0]["research_gaps"]


def test_configured_but_unhealthy_source_does_not_count() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    rows = audit.build_currency_rows(sources, runtime("AAA", "policy"), depth("AAA"))
    assert rows[0]["minimum_feed_ready"] is False
    assert "no_live_direct_statistical_release" in rows[0]["gaps"]


def test_bounded_recent_success_counts_but_is_reported_degraded() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    runtime_payload = runtime("AAA", "policy", "macro")
    runtime_payload["currencies"]["AAA"]["sources"][0].update(
        {"healthy": False, "usable_recent_success": True}
    )
    report = audit.build_report({"sources": sources}, runtime_payload, depth("AAA"))
    row = report["currencies"][0]
    assert row["minimum_feed_ready"] is True
    assert row["degraded_recent_policy_sources"] == ["policy"]
    assert report["minimum_feed_degraded_recent_count"] == 1


def test_consensus_is_separate_from_minimum_feed_readiness() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    row = audit.build_currency_rows(
        sources,
        runtime("AAA", "policy", "macro"),
        depth("AAA", consensus=2),
    )[0]
    assert row["minimum_feed_ready"] is True
    assert row["prospective_surprise_ready"] is True


def test_statistical_calendar_does_not_substitute_for_release_feed() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("calendar", "primary_statistical_calendar", "AAA"),
    ]
    row = audit.build_currency_rows(
        sources,
        runtime("AAA", "policy", "calendar"),
        depth("AAA"),
    )[0]
    assert row["minimum_feed_ready"] is False
    assert row["live_macro_calendars"] == ["calendar"]
    assert "no_live_direct_statistical_release" in row["gaps"]


def test_observed_central_bank_item_cannot_self_certify_statistical_feed() -> None:
    sources = [source("central_bank", "primary_policy_release", "ZAR")]
    observed = depth("ZAR")
    observed["currencies"][0].update(
        {
            "official_policy_sources": ["central_bank"],
            "official_macro_sources": ["central_bank"],
        }
    )
    row = audit.build_currency_rows(
        sources,
        runtime("ZAR", "central_bank"),
        observed,
    )[0]
    assert row["live_policy_sources"] == ["central_bank"]
    assert row["live_macro_sources"] == []
    assert row["minimum_feed_ready"] is False


def test_report_prioritizes_blockers_by_pair_leg_impact() -> None:
    sources = [
        source("aaa_policy", "primary_policy_release", "AAA"),
        source("bbb_policy", "primary_policy_release", "BBB"),
    ]
    runtime_payload = {
        "currencies": {
            "AAA": {"sources": [{"source_id": "aaa_policy", "operational": True, "healthy": True}]},
            "BBB": {"sources": [{"source_id": "bbb_policy", "operational": True, "healthy": True}]},
        }
    }
    depth_payload = {
        "currencies": [
            {"currency": "AAA", "pair_leg_count": 2, "future_relevant_events": 1},
            {"currency": "BBB", "pair_leg_count": 8, "future_relevant_events": 1},
        ]
    }
    report = audit.build_report({"sources": sources}, runtime_payload, depth_payload)
    assert [row["currency"] for row in report["blocker_priority"]] == ["BBB", "AAA"]
    assert report["total_pair_legs"] == 10
    assert report["minimum_feed_ready_pair_legs"] == 0
    assert report["structured_numeric_parser_count"] == 0
    assert report["future_event_clock_count"] == 2
    assert report["actual_observation_currency_count"] == 0
    assert report["prospective_actual_observation_currency_count"] == 0
    assert report["causal_consensus_currency_count"] == 0
    assert report["minimum_feed_ready_pct"] == 0.0
    assert report["actual_observation_currency_pct"] == 0.0
    assert report["prospective_actual_observation_currency_pct"] == 0.0
    assert [row["currency"] for row in report["surprise_blocker_priority"]] == ["BBB", "AAA"]


def test_historical_vintage_and_internal_expectation_are_reported_without_self_certifying_consensus() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    alfred = {
        "coverage": {
            "latest_observation_date_by_currency": {"AAA": "2026-07-01"},
            "latest_observation_age_days_by_currency": {"AAA": 46},
        }
    }
    expectation = {
        "summary": {
            "currencies": [{"currency": "AAA", "forecast_count": 3}]
        }
    }
    report = audit.build_report(
        {"sources": sources},
        runtime("AAA", "policy", "macro"),
        depth("AAA", consensus=0),
        alfred,
        expectation,
    )
    row = report["currencies"][0]
    assert row["historical_vintage_context"] is True
    assert row["historical_vintage_current_120d"] is True
    assert row["internal_expectation_forecasts"] == 3
    assert row["causal_pre_release_consensus"] == 0
    assert report["prospective_surprise_ready_count"] == 0


def test_consensus_provider_access_is_reported_without_self_certifying_currency_readiness() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    access = {
        "status": "blocked_no_accessible_consensus_provider",
        "causal_consensus_provider_accessible": False,
        "unlock_requirement": "licensed provider required",
        "providers": [{"provider": "Example", "access_state": "missing_credential"}],
    }
    report = audit.build_report(
        {"sources": sources},
        runtime("AAA", "policy", "macro"),
        depth("AAA", consensus=0),
        consensus_access_payload=access,
    )
    assert report["consensus_provider_access"]["status"] == access["status"]
    assert report["prospective_surprise_ready_count"] == 0


def test_timestamp_valid_historical_replay_is_separate_from_feed_readiness() -> None:
    sources = [
        source("policy", "primary_policy_release", "AAA"),
        source("macro", "primary_statistical_release", "AAA"),
    ]
    replay = {
        "mapped_source_currencies": ["AAA"],
        "source_event_count": 4,
        "independent_source_time_count": 3,
        "excluded_source_events": [{"release_key": "blocked"}],
        "evidence_class": "availability_counterfactual_discovery",
    }
    report = audit.build_report(
        {"sources": sources},
        runtime("AAA", "policy", "macro"),
        depth("AAA", consensus=0),
        direct_replay_payload=replay,
    )
    historical = report["direct_historical_replay"]
    assert historical["timestamp_valid_currency_count"] == 1
    assert historical["timestamp_valid_pair_leg_count"] == 3
    assert historical["timestamp_valid_pair_leg_pct"] == 100.0
    assert historical["source_event_count"] == 4
    assert historical["excluded_source_event_count"] == 1
    assert report["prospective_surprise_ready_count"] == 0
