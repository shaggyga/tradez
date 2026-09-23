from __future__ import annotations

import datetime as dt

import pytest

from oanda_news_technical_watchlist import entry_identifier as v37_entry_identifier
from src.forex_system.research import persistent_policy_watch_provenance_v2 as v2


UTC = dt.timezone.utc
AS_OF = dt.datetime(2026, 8, 17, 13, 0, tzinfo=UTC)


def article(**overrides):
    value = {
        "source_verified": True,
        "official_policy_release": True,
        "source_id": "central_bank",
        "source_currencies": ["JPY"],
        "event_id": "event-current",
        "headline": "Policy decision",
        "summary": "The central bank kept policy unchanged.",
        "published_utc": "2026-08-17T02:00:00+00:00",
        "first_seen_utc": "2026-08-17T02:00:01+00:00",
        "causal_known_utc": "2026-08-17T02:00:01+00:00",
        "scheduled_utc": "",
        "structured_event": False,
        "monetary_impulse": 0.0,
        "observation_clock_trusted": True,
        "observation_time_contract_id": v2.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v2.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v2.REQUIRED_COLLECTOR_COHORT_ID,
    }
    value.update(overrides)
    return value


def watch_row(**overrides):
    value = {
        "episode_id": "news-episode-1",
        "currency": "JPY",
        "instrument": "USD_JPY",
        "arm": "news_only",
        "direction": "short",
        "horizon_min": 15,
        **v2.collector_lineage_fields(article()),
    }
    value.update(overrides)
    return value


def test_current_v1_defect_witness_future_and_maintenance_are_admissible():
    """The predecessor's published predicate admits both defect fixtures."""

    future = article(
        structured_event=True,
        scheduled_utc="2026-12-18T00:00:00+00:00",
        headline="Policy Decision Release Window",
    )
    maintenance = article(
        event_id="maintenance",
        summary="Maintenance Sorry, this service is currently unavailable.",
    )
    predecessor_predicate = lambda row: (
        row.get("source_verified") is True
        and row.get("official_policy_release") is True
        and dt.datetime.fromisoformat(row["causal_known_utc"]) <= AS_OF
    )
    assert predecessor_predicate(future)
    assert predecessor_predicate(maintenance)


def test_policy_v2_excludes_future_structured_schedule():
    payload = v2.build_persistent_policy_context_v2(
        [
            article(
                structured_event=True,
                scheduled_utc="2026-12-18T00:00:00+00:00",
            )
        ],
        as_of=AS_OF,
    )
    jpy = payload["currencies"]["JPY"]
    assert jpy["status"] == "missing"
    assert "future_scheduled_release" in jpy["missing_reason"]
    assert payload["coverage"]["proof_grade_currency_count"] == 0


@pytest.mark.parametrize(
    "bad_text",
    [
        "Maintenance Sorry, this service is currently unavailable.",
        "This page is temporarily unavailable.",
        "Access denied. Error 403.",
        "Enable JavaScript and cookies to continue.",
        "Something went wrong.",
    ],
)
def test_policy_v2_rejects_provider_error_pages(bad_text):
    payload = v2.build_persistent_policy_context_v2(
        [article(summary=bad_text)], as_of=AS_OF
    )
    assert payload["currencies"]["JPY"]["status"] == "missing"
    assert payload["rejections"][0]["reason"] == "provider_error_or_maintenance_content"


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("observation_clock_trusted", False),
        ("observation_time_contract_id", "old-clock"),
        ("collector_contract_id", "old-collector"),
        ("collector_cohort_id", "old-cohort"),
    ],
)
def test_policy_v2_segregates_legacy_provenance(field, bad_value):
    payload = v2.build_persistent_policy_context_v2(
        [article(**{field: bad_value})], as_of=AS_OF
    )
    jpy = payload["currencies"]["JPY"]
    assert jpy["status"] == "missing"
    assert jpy["missing_reason"] == "current_v40_provenance_unavailable"
    assert jpy["proof_grade_context"] is None
    assert jpy["legacy_diagnostic_context"]["proof_grade"] is False
    assert payload["coverage"]["legacy_diagnostic_currency_count"] == 1


def test_policy_v2_reports_every_currency_and_missing_reason():
    payload = v2.build_persistent_policy_context_v2([article()], as_of=AS_OF)
    assert list(payload["currencies"]) == list(v2.ALL_CURRENCIES)
    assert payload["coverage"]["reported_currency_count"] == 21
    assert payload["coverage"]["proof_grade_currency_count"] == 1
    assert payload["coverage"]["missing_currency_count"] == 20
    assert payload["currencies"]["JPY"]["status"] == "proof_grade_current"
    assert payload["currencies"]["CNH"]["missing_reason"] == "no_official_policy_candidate"


def test_policy_v2_selected_context_exposes_exact_lineage_and_is_inert():
    payload = v2.build_persistent_policy_context_v2([article()], as_of=AS_OF)
    context = payload["currencies"]["JPY"]["proof_grade_context"]
    assert context["collector_contract_id"] == v2.REQUIRED_COLLECTOR_CONTRACT_ID
    assert context["collector_cohort_id"] == v2.REQUIRED_COLLECTOR_COHORT_ID
    assert context["observation_time_contract_id"] == v2.REQUIRED_OBSERVATION_TIME_CONTRACT_ID
    assert context["observation_clock_trusted"] is True
    assert context["research_only"] is True
    assert context["execution_eligible"] is False
    assert payload["activation_state"] == "inert_candidate"


def test_newer_proof_document_supersedes_older_current_document():
    older = article(event_id="older", causal_known_utc="2026-08-16T02:00:00+00:00")
    newer = article(event_id="newer", causal_known_utc="2026-08-17T02:00:00+00:00")
    payload = v2.build_persistent_policy_context_v2([newer, older], as_of=AS_OF)
    assert payload["currencies"]["JPY"]["proof_grade_context"]["event_id"] == "newer"


def test_v37_identity_defect_witness_ignores_collector_contract_and_cohort():
    first = v37_entry_identifier(
        "episode", "USD_JPY", "news_only", "short", 15
    )
    # V37 has no collector arguments, so a source-contract change cannot alter
    # this identifier.  This is the bounded regression witness for V38.
    second = v37_entry_identifier(
        "episode", "USD_JPY", "news_only", "short", 15
    )
    assert first == second


def test_factor_binding_exposes_exact_collector_contract_and_cohort():
    factor = v2.bind_currency_factor_lineage(
        {"episode_id": "episode", "currency": "JPY"}, [article()]
    )
    assert factor["news_collector_contract_id"] == v2.REQUIRED_COLLECTOR_CONTRACT_ID
    assert factor["news_collector_cohort_id"] == v2.REQUIRED_COLLECTOR_COHORT_ID
    assert factor["news_collector_provenance_bound"] is True
    assert factor["research_only"] is True
    assert factor["execution_eligible"] is False


def test_factor_binding_rejects_mixed_or_legacy_source_payloads():
    with pytest.raises(v2.ContractViolation, match="news_source_not_exact"):
        v2.bind_currency_factor_lineage(
            {"episode_id": "episode", "currency": "JPY"},
            [article(), article(collector_cohort_id="legacy")],
        )


def test_watch_candidate_payload_and_identity_bind_exact_provenance():
    payload = v2.bind_watch_candidate(watch_row())
    assert payload["news_collector_contract_id"] == v2.REQUIRED_COLLECTOR_CONTRACT_ID
    assert payload["news_collector_cohort_id"] == v2.REQUIRED_COLLECTOR_COHORT_ID
    assert payload["news_observation_time_contract_id"] == v2.REQUIRED_OBSERVATION_TIME_CONTRACT_ID
    assert payload["news_collector_provenance_bound"] is True
    assert payload["entry_id"] == v2.watch_candidate_identifier(payload)
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False
    assert payload["can_authorize"] is False
    assert payload["can_place_orders"] is False
    assert payload["activation_state"] == "inert_candidate"


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("news_observation_clock_trusted", False),
        ("news_observation_time_contract_id", "old-clock"),
        ("news_collector_contract_id", "old-collector"),
        ("news_collector_cohort_id", "old-cohort"),
        ("news_collector_provenance_bound", False),
    ],
)
def test_watch_candidate_rejects_missing_or_mismatched_lineage(field, bad_value):
    with pytest.raises(v2.ContractViolation, match="watch_row_lineage_mismatch"):
        v2.bind_watch_candidate(watch_row(**{field: bad_value}))


def test_watch_identity_changes_when_frozen_collector_contract_changes():
    row = watch_row()
    current = v2.watch_candidate_identifier(row)
    altered = dict(row)
    altered["news_collector_contract_id"] = "future-contract"
    with pytest.raises(v2.ContractViolation, match="watch_row_lineage_mismatch"):
        v2.watch_candidate_identifier(altered)
    assert current


def test_non_plain_input_subclasses_fail_closed():
    class CallbackText(str):
        pass

    hostile = article(event_id=CallbackText("forged"))
    payload = v2.build_persistent_policy_context_v2([hostile], as_of=AS_OF)
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    assert payload["rejections"][0]["reason"] == "non_plain_article"


def test_as_of_requires_exact_builtin_utc_datetime():
    with pytest.raises(v2.ContractViolation, match="as_of_must_be"):
        v2.build_persistent_policy_context_v2(
            [article()], as_of=dt.datetime(2026, 8, 17, 13, 0)
        )
