import datetime as dt
import json
import sqlite3

import oanda_current_week_official_event_response_audit_v1 as audit


UTC = dt.timezone.utc


def test_current_week_window_is_bound_to_generation_clock():
    start, end = audit.current_utc_week_window(
        dt.datetime(2026, 8, 27, 7, 49, 17, tzinfo=UTC)
    )
    assert start == dt.datetime(2026, 8, 24, tzinfo=UTC)
    assert end == dt.datetime(2026, 8, 27, 7, 49, 17, tzinfo=UTC)


def test_event_clock_separates_calendar_only_from_actual_release():
    start = dt.datetime(2026, 8, 17, tzinfo=UTC)
    end = dt.datetime(2026, 8, 22, tzinfo=UTC)
    clock, basis = audit._event_clock(
        {
            "scheduled_utc": "2026-08-20T06:00:00Z",
            "published_utc": "2026-08-17T04:00:00Z",
            "actual_value": None,
            "category": "growth_release",
        },
        start,
        end,
    )
    assert clock == dt.datetime(2026, 8, 20, 6, tzinfo=UTC)
    assert basis == "calendar_only_clock"


def test_event_clock_prefers_scheduled_actual_release_clock():
    start = dt.datetime(2026, 8, 17, tzinfo=UTC)
    end = dt.datetime(2026, 8, 22, tzinfo=UTC)
    clock, basis = audit._event_clock(
        {
            "scheduled_utc": "2026-08-21T12:30:00Z",
            "published_utc": "2026-08-21T12:33:00Z",
            "actual_value": 0.6,
            "category": "growth_release",
        },
        start,
        end,
    )
    assert clock == dt.datetime(2026, 8, 21, 12, 30, tzinfo=UTC)
    assert basis == "scheduled_release_clock"


def test_scheduled_policy_calendar_is_not_misrepresented_as_release():
    start = dt.datetime(2026, 8, 17, tzinfo=UTC)
    end = dt.datetime(2026, 8, 22, tzinfo=UTC)
    clock, basis = audit._event_clock(
        {
            "scheduled_utc": "2026-08-20T07:30:00Z",
            "published_utc": "2026-08-16T06:00:00Z",
            "actual_value": None,
            "category": "monetary_policy",
            "source_role": "primary_policy_calendar",
        },
        start,
        end,
    )
    assert clock == dt.datetime(2026, 8, 20, 7, 30, tzinfo=UTC)
    assert basis == "calendar_only_clock"


def test_event_clock_does_not_turn_old_calendar_row_into_current_event():
    start = dt.datetime(2026, 8, 17, tzinfo=UTC)
    end = dt.datetime(2026, 8, 22, tzinfo=UTC)
    clock, basis = audit._event_clock(
        {
            "scheduled_utc": "2025-09-22T08:30:00Z",
            "published_utc": "2026-08-17T04:32:32Z",
            "actual_value": None,
            "category": "inflation_release",
        },
        start,
        end,
    )
    assert clock is None
    assert basis == "outside_week"


def test_substantive_filter_rejects_archive_and_bank_navigation_rows():
    assert not audit._substantive_official_event(
        {
            "headline": "Inflation indicators",
            "category": "inflation_context",
            "source_role": "primary_reserve_and_market_rate_statistics",
            "published_time_inferred": True,
        },
        "publisher_or_observation_clock",
    )
    assert not audit._substantive_official_event(
        {
            "headline": "HKICL alerts public of fraudulent website",
            "category": "monetary_policy",
            "source_role": "primary_policy_release",
            "published_time_inferred": False,
        },
        "publisher_or_observation_clock",
    )


def test_substantive_filter_accepts_dated_statistical_and_policy_releases():
    assert audit._substantive_official_event(
        {
            "headline": "Consumer Price Index, July 2026",
            "category": "inflation_release",
            "source_role": "primary_statistical_release",
            "published_time_inferred": False,
        },
        "publisher_or_observation_clock",
    )
    assert audit._substantive_official_event(
        {
            "headline": "Minutes of the Federal Open Market Committee",
            "category": "monetary_policy",
            "source_role": "primary_policy_release",
            "published_time_inferred": False,
        },
        "publisher_or_observation_clock",
    )


def test_single_currency_prefers_source_native_leg():
    assert audit._single_currency(
        {"source_currencies": ["cad"], "research_currency_scores": {"USD": -0.5}}
    ) == "CAD"
    assert audit._single_currency(
        {"source_currencies": [], "research_currency_scores": {"USD": -0.5}}
    ) == "USD"
    assert audit._single_currency(
        {"source_currencies": ["USD", "CAD"], "research_currency_scores": {}}
    ) == ""


def test_pair_factor_direction_respects_base_and_quote():
    assert audit._pair_expresses_factor("USD_CAD", "long", "USD", 1)
    assert audit._pair_expresses_factor("USD_CAD", "long", "CAD", -1)
    assert not audit._pair_expresses_factor("USD_CAD", "short", "USD", 1)


def test_same_currency_and_five_minute_bucket_is_one_event_episode():
    left = {"currency": "NZD", "event_utc": "2026-08-18T22:45:00Z"}
    right = {"currency": "NZD", "event_utc": "2026-08-18T22:47:00Z"}
    other = {"currency": "AUD", "event_utc": "2026-08-18T22:47:00Z"}
    assert audit._event_episode_id(left) == audit._event_episode_id(right)
    assert audit._event_episode_id(left) != audit._event_episode_id(other)


def test_episode_representative_prefers_actual_over_calendar_clock():
    actual = {
        "event_id": "actual",
        "clock_basis": "publisher_or_observation_clock",
        "actual_value": 1.6,
        "source_score": 0.0,
    }
    calendar = {
        "event_id": "calendar",
        "clock_basis": "calendar_only_clock",
        "actual_value": None,
        "source_score": 0.0,
    }
    assert audit._episode_representative_rank(actual) > audit._episode_representative_rank(calendar)


def test_strict_attribution_requires_timing_extreme_factor_and_dominant_leg():
    response = {
        "currency_factor_bps": -8.0,
        "currency_factor_extreme_rank": 2,
        "representative_path": {
            "factor_consistent": True,
            "source_currency_dominant": True,
            "observed_after_cost_pips": 12.0,
            "observed_side": "short",
        },
        "post_observation_path": {
            "observed_after_cost_pips": 9.0,
            "observed_side": "short",
        },
    }
    assert audit._response_is_attribution_candidate(
        response, calendar_only=False, source_latency_seconds=120.0
    )
    assert not audit._response_is_attribution_candidate(
        response, calendar_only=False, source_latency_seconds=301.0
    )
    assert not audit._response_is_attribution_candidate(
        response, calendar_only=True, source_latency_seconds=-3600.0
    )
    assert not audit._response_is_attribution_candidate(
        response, calendar_only=False, source_latency_seconds=-3600.0
    )
    assert not audit._response_is_attribution_candidate(
        response,
        calendar_only=False,
        source_latency_seconds=120.0,
        source_change_information=False,
    )
    assert not audit._response_is_attribution_candidate(
        response,
        calendar_only=False,
        source_latency_seconds=120.0,
        supporting_artifact=True,
    )
    exhausted = dict(response)
    exhausted["post_observation_path"] = {
        "observed_after_cost_pips": 1.0,
        "observed_side": "short",
    }
    assert not audit._response_is_attribution_candidate(
        exhausted, calendar_only=False, source_latency_seconds=120.0
    )


def test_supporting_dataset_is_not_an_event_change_but_release_bulletin_is():
    supporting = {
        "headline": "Labour market statistics time series: August 2026",
        "summary": "Large dataset which contains labour market statistics data time series.",
        "category": "labor_release",
    }
    bulletin = {
        "headline": "Retail sales, Great Britain: July 2026",
        "summary": "Retail sales rose in the three months to July 2026.",
        "category": "growth_release",
    }
    assert audit._supporting_statistical_artifact(supporting)
    assert not audit._has_source_change_information(supporting)
    assert not audit._supporting_statistical_artifact(bulletin)
    assert audit._has_source_change_information(bulletin)


def test_contract_is_inert():
    assert audit.CONTRACT_ID.startswith("current_week_official_event_response_audit_v3")
    source = (audit.ROOT / "oanda_current_week_official_event_response_audit_v1.py").read_text(encoding="utf-8")
    assert "can_place_orders\": False" in source
    assert "execution_decision\": \"no_trade\"" in source


def test_structured_material_dedup_ignores_repeated_inferred_clock():
    base = {
        "structured_event": True,
        "source_id": "swiss_fso_releases",
        "source_url": "https://www.admin.ch/de/newnsb/NSUt8um5SBxJ",
        "headline": "Consumer prices rose 0.4 percent in August",
        "source_currencies": ["CHF"],
        "event_series_id": "swiss_cpi_headline_yoy",
        "reference_period": "2026M08",
        "actual_value": 0.4,
        "published_time_inferred": True,
    }
    earlier = {**base, "scheduled_utc": "2026-09-03T06:33:52Z"}
    repeated = {**base, "scheduled_utc": "2026-09-04T14:13:29Z"}
    revised = {**base, "actual_value": 0.5}

    assert audit._dedup_key(earlier, "CHF") == audit._dedup_key(repeated, "CHF")
    assert audit._dedup_key(earlier, "CHF") != audit._dedup_key(revised, "CHF")


def test_frozen_cutoff_rejects_calendar_discovered_after_cutoff(tmp_path, monkeypatch):
    database = tmp_path / "news.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE articles(
               event_id TEXT, source_id TEXT, source_name TEXT, source_kind TEXT,
               source_quality REAL, source_verified INTEGER, headline TEXT,
               summary TEXT, source_url TEXT, published_utc TEXT,
               first_seen_utc TEXT, payload_json TEXT)"""
    )
    payload = {
        "scheduled_utc": "2026-08-27T08:30:00Z",
        "source_currencies": ["USD"],
        "category": "growth_release",
    }
    connection.execute(
        "INSERT INTO articles VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "late-calendar",
            "official",
            "Official",
            "html",
            1.0,
            1,
            "Release calendar",
            "",
            "https://example.invalid",
            "2026-08-27T07:00:00Z",
            "2026-08-27T10:00:00Z",
            json.dumps(payload),
        ),
    )
    connection.commit()
    connection.close()
    monkeypatch.setattr(audit.news, "classify_article", lambda row, first_seen: row)
    monkeypatch.setattr(audit, "_substantive_official_event", lambda row, basis: True)

    rows = audit.load_official_events(
        database,
        dt.datetime(2026, 8, 24, 4, tzinfo=UTC),
        dt.datetime(2026, 8, 27, 9, tzinfo=UTC),
    )

    assert rows == []


def test_compile_marks_only_horizons_matured_at_frozen_cutoff(monkeypatch):
    event_time = dt.datetime(2026, 8, 27, 8, 50, tzinfo=UTC)
    cutoff = dt.datetime(2026, 8, 27, 9, 0, tzinfo=UTC)
    event = {
        "event_id": "event-1",
        "source_id": "official",
        "headline": "Official release",
        "source_url": "https://example.invalid",
        "audit_currency": "USD",
        "audit_event_utc": audit.weekly.iso_utc(event_time),
        "audit_clock_basis": "scheduled_release_clock",
        "first_seen_utc": audit.weekly.iso_utc(event_time),
        "category": "growth_release",
        "actual_value": 1.0,
        "consensus_value": None,
        "previous_value": None,
        "research_currency_scores": {},
    }
    monkeypatch.setattr(audit, "load_official_events", lambda *args: [event])
    monkeypatch.setattr(audit.weekly, "_load_candles", lambda *args: {f"P{i}": [] for i in range(68)})
    called = []

    def response(panel, event_clock, currency, horizon):
        called.append(horizon)
        return {
            "horizon_minutes": horizon,
            "currency_factor_bps": 0.0,
            "currency_factor_rank_from_strongest": 1,
            "currency_factor_extreme_rank": 1,
            "usable_pair_count": 68,
            "cost_clearing_factor_consistent_pair_count": 0,
            "representative_path": None,
        }

    monkeypatch.setattr(audit, "response_at_horizon", response)
    monkeypatch.setattr(
        audit,
        "_with_knowledge_time_paths",
        lambda panel, row, event_clock, first_seen: {
            **row,
            "pre_observation_path": None,
            "post_observation_path": None,
        },
    )

    result = audit.compile_audit(
        week_start=dt.datetime(2026, 8, 24, 4, tzinfo=UTC),
        week_end=cutoff,
    )

    assert called == [5]
    states = {
        row["horizon_minutes"]: row["maturity_state"]
        for row in result["events"][0]["responses"]
    }
    assert states[5] == "matured_at_frozen_cutoff"
    assert states[15] == "not_matured_at_frozen_cutoff"
    assert states[60] == "not_matured_at_frozen_cutoff"
    assert states[120] == "not_matured_at_frozen_cutoff"
    assert result["frozen_cutoff_utc"] == audit.weekly.iso_utc(cutoff)
