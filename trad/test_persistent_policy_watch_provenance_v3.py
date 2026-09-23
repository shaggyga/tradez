from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path

import pytest

from src.forex_system.research import persistent_policy_watch_provenance_v3 as v3


UTC = dt.timezone.utc
AS_OF = dt.datetime(2026, 8, 17, 13, 0, tzinfo=UTC)
V2_MODULE_SHA256 = "ab0b6a3d3c6907c2bf3354e8de6817f35967d57ff48b93814a6e378be64764eb"
V2_TEST_SHA256 = "b29599f19ff511be2acf678947f42f2fb0d7b8693cc0c3a7d7005fe38802cf35"


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
        "material_content_sha256": "1" * 64,
        "observation_clock_trusted": True,
        "observation_time_contract_id": v3.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v3.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v3.REQUIRED_COLLECTOR_COHORT_ID,
    }
    value.update(overrides)
    return value


def evidence(event_id="event-1", payload_sha256="a" * 64, **overrides):
    value = {
        "event_id": event_id,
        "payload_sha256": payload_sha256,
        "observation_clock_trusted": True,
        "observation_time_contract_id": v3.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v3.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v3.REQUIRED_COLLECTOR_COHORT_ID,
    }
    value.update(overrides)
    return value


def factor(**overrides):
    args = {
        "episode_id": "news_episode_1",
        "currency": "JPY",
        "score": 0.5,
        "confidence": 0.7,
        "horizon_min": 15,
        "source_evidence": [evidence()],
    }
    args.update(overrides)
    return v3.build_currency_factor_v3(**args)


def test_v2_exact_bytes_are_preserved():
    root = Path(__file__).resolve().parent
    module = root / "src/forex_system/research/persistent_policy_watch_provenance_v2.py"
    tests = root / "test_persistent_policy_watch_provenance_v2.py"
    assert hashlib.sha256(module.read_bytes()).hexdigest() == V2_MODULE_SHA256
    assert hashlib.sha256(tests.read_bytes()).hexdigest() == V2_TEST_SHA256


def test_canonical_universe_is_exact_builtin_21_and_68():
    assert type(v3.CANONICAL_CURRENCIES) is tuple
    assert type(v3.CANONICAL_INSTRUMENTS) is tuple
    assert len(v3.CANONICAL_CURRENCIES) == 21
    assert len(v3.CANONICAL_INSTRUMENTS) == 68
    assert len(set(v3.CANONICAL_CURRENCIES)) == 21
    assert len(set(v3.CANONICAL_INSTRUMENTS)) == 68
    assert all(type(value) is str for value in v3.CANONICAL_CURRENCIES)
    assert all(type(value) is str for value in v3.CANONICAL_INSTRUMENTS)


def test_aware_offset_schedule_is_normalized_to_utc():
    payload = v3.build_persistent_policy_context_v3(
        [
            article(
                structured_event=True,
                scheduled_utc="2026-08-17T16:00:00+03:00",
                actual_value=0.5,
            )
        ],
        as_of=AS_OF,
    )
    context = payload["currencies"]["JPY"]["proof_grade_context"]
    assert context["scheduled_utc"] == "2026-08-17T13:00:00Z"


def test_future_structured_offset_schedule_never_becomes_proof_grade():
    payload = v3.build_persistent_policy_context_v3(
        [
            article(
                structured_event=True,
                scheduled_utc="2026-08-17T09:30:00-04:00",
                actual_value=0.5,
            )
        ],
        as_of=AS_OF,
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    assert "future_scheduled_release" in payload["currencies"]["JPY"]["missing_reason"]


@pytest.mark.parametrize(
    "schedule",
    ["2026-08-17T13:00:00", "not-a-time", " 2026-08-17T13:00:00Z", [], {}],
)
def test_malformed_nonempty_schedule_fails_closed(schedule):
    payload = v3.build_persistent_policy_context_v3(
        [article(scheduled_utc=schedule)], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    assert "malformed_nonempty_schedule" in payload["currencies"]["JPY"]["missing_reason"]


def test_past_structured_calendar_without_actual_is_not_policy_proof():
    payload = v3.build_persistent_policy_context_v3(
        [
            article(
                structured_event=True,
                scheduled_utc="2026-08-16T13:00:00Z",
                actual_value=None,
            )
        ],
        as_of=AS_OF,
    )
    assert "structured_release_not_actual" in payload["currencies"]["JPY"]["missing_reason"]


def test_structured_actual_rejects_bool_and_nonfinite():
    for bad in (True, float("nan"), float("inf"), float("-inf")):
        payload = v3.build_persistent_policy_context_v3(
            [
                article(
                    structured_event=True,
                    scheduled_utc="2026-08-16T13:00:00Z",
                    actual_value=bad,
                )
            ],
            as_of=AS_OF,
        )
        assert payload["coverage"]["proof_grade_currency_count"] == 0


@pytest.mark.parametrize(
    "bad_text",
    [
        "Maintenance: this service is currently unavailable.",
        "The page is temporarily unavailable.",
        "Access denied. Error 403.",
        "Enable JavaScript and cookies to continue.",
        "Something went wrong.",
        "Checking your browser before accessing the page.",
    ],
)
def test_provider_error_and_maintenance_bodies_are_rejected(bad_text):
    payload = v3.build_persistent_policy_context_v3(
        [article(summary=bad_text)], as_of=AS_OF
    )
    assert payload["rejections"][0]["reason"] == "provider_error_or_maintenance_content"
    assert payload["coverage"]["proof_grade_currency_count"] == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("fetch_status", "failed"),
        ("detail_enrichment_status", "maintenance"),
        ("source_status", "access denied"),
        ("http_status", 503),
        ("last_status", "404"),
        ("http_status", "HTTP 503"),
    ],
)
def test_bad_fetch_or_enrichment_status_is_rejected(field, value):
    payload = v3.build_persistent_policy_context_v3(
        [article(**{field: value})], as_of=AS_OF
    )
    assert payload["rejections"][0]["reason"] == "bad_fetch_or_enrichment_status"


@pytest.mark.parametrize(
    "field,value",
    [("http_status", 200), ("http_status", 304), ("fetch_status", "ok")],
)
def test_good_fetch_status_does_not_block_actual_document(field, value):
    payload = v3.build_persistent_policy_context_v3(
        [article(**{field: value})], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 1


@pytest.mark.parametrize("bad", [True, False, float("nan"), float("inf"), float("-inf")])
def test_bool_and_nonfinite_monetary_impulse_is_rejected(bad):
    payload = v3.build_persistent_policy_context_v3(
        [article(monetary_impulse=bad)], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0


@pytest.mark.parametrize(
    "field,bad",
    [
        ("observation_clock_trusted", False),
        ("observation_time_contract_id", "old-clock"),
        ("collector_contract_id", "old-collector"),
        ("collector_cohort_id", "old-cohort"),
    ],
)
def test_policy_mismatched_provenance_is_legacy_only(field, bad):
    payload = v3.build_persistent_policy_context_v3(
        [article(**{field: bad})], as_of=AS_OF
    )
    row = payload["currencies"]["JPY"]
    assert row["status"] == "missing"
    assert row["missing_reason"] == "current_v40_provenance_unavailable"
    assert row["legacy_diagnostic_context"]["grade"] == "legacy_diagnostic_only"


def test_policy_reports_all_21_with_missing_reasons():
    payload = v3.build_persistent_policy_context_v3([article()], as_of=AS_OF)
    assert tuple(payload["currencies"]) == v3.CANONICAL_CURRENCIES
    assert payload["coverage"]["expected_currency_count"] == 21
    assert payload["coverage"]["reported_currency_count"] == 21
    assert payload["coverage"]["proof_grade_currency_count"] == 1
    assert payload["coverage"]["missing_currency_count"] == 20
    assert payload["currencies"]["CNH"]["missing_reason"] == "no_official_policy_candidate"


def test_policy_output_is_closed_and_cannot_pass_operational_keys():
    payload = v3.build_persistent_policy_context_v3(
        [
            article(
                broker_order={"units": 999},
                authorization_id="forged",
                promotion_state="confirmed",
                operational_route="live",
            )
        ],
        as_of=AS_OF,
    )
    v3.assert_closed_output_schemas(payload, kind="policy")
    text = repr(payload)
    for forbidden in ("broker_order", "authorization_id", "promotion_state", "operational_route"):
        assert forbidden not in text
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", " bad"),
        ("event_id", "bad\nvalue"),
        ("event_id", "bäd"),
        ("source_id", "bad source"),
        ("structured_event", 1),
    ],
)
def test_policy_identifier_and_structured_flag_inputs_fail_closed(field, value):
    payload = v3.build_persistent_policy_context_v3(
        [article(**{field: value})], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0


def test_legacy_provenance_identifiers_cannot_carry_controls_or_spaces():
    payload = v3.build_persistent_policy_context_v3(
        [article(collector_contract_id="legacy contract\nforged")], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    assert "invalid_provenance_identifier" in payload["currencies"]["JPY"]["missing_reason"]


def test_policy_type_is_printable_normalized_identifier():
    payload = v3.build_persistent_policy_context_v3(
        [article(policy_document_type="Monetary Policy Statement")], as_of=AS_OF
    )
    context = payload["currencies"]["JPY"]["proof_grade_context"]
    assert context["policy_document_type"] == "monetary_policy_statement"


def test_source_evidence_digest_is_order_independent_and_deduplicated():
    first = evidence("event-a", "a" * 64)
    second = evidence("event-b", "b" * 64)
    left = factor(source_evidence=[first, second, first])
    right = factor(source_evidence=[second, first])
    assert left["source_evidence_count"] == 2
    assert left["event_ids"] == ["event-a", "event-b"]
    assert left["source_evidence_digest_sha256"] == right["source_evidence_digest_sha256"]


def test_source_evidence_digest_changes_with_payload_hash():
    first = factor(source_evidence=[evidence(payload_sha256="a" * 64)])
    second = factor(source_evidence=[evidence(payload_sha256="b" * 64)])
    assert first["source_evidence_digest_sha256"] != second["source_evidence_digest_sha256"]


def test_conflicting_payload_hash_for_one_event_id_fails_closed():
    with pytest.raises(v3.ContractViolation, match="conflicting_payload_hash"):
        factor(
            source_evidence=[
                evidence("event-a", "a" * 64),
                evidence("event-a", "b" * 64),
            ]
        )


def test_source_evidence_schema_cannot_pass_operational_keys():
    with pytest.raises(v3.ContractViolation, match="closed_schema_mismatch"):
        factor(source_evidence=[evidence(authorization_id="forged")])


@pytest.mark.parametrize(
    "field,bad",
    [
        ("observation_clock_trusted", False),
        ("observation_time_contract_id", "old-clock"),
        ("collector_contract_id", "old-collector"),
        ("collector_cohort_id", "old-cohort"),
    ],
)
def test_source_evidence_requires_exact_collector_clock_ids(field, bad):
    with pytest.raises(v3.ContractViolation, match="source_evidence_not_exact"):
        factor(source_evidence=[evidence(**{field: bad})])


@pytest.mark.parametrize(
    "event_id",
    [" bad", "bad ", "bad\nvalue", "bäd", "", "bad value"],
)
def test_source_evidence_identifiers_must_be_printable_normalized(event_id):
    with pytest.raises(v3.ContractViolation, match="invalid_identifier"):
        factor(source_evidence=[evidence(event_id=event_id)])


def test_payload_hash_must_be_exact_lowercase_sha256():
    with pytest.raises(v3.ContractViolation, match="invalid_sha256"):
        factor(source_evidence=[evidence(payload_sha256="A" * 64)])


@pytest.mark.parametrize("bad", [True, False, float("nan"), float("inf"), 0.0])
def test_factor_score_rejects_bool_nonfinite_and_zero(bad):
    with pytest.raises(v3.ContractViolation):
        factor(score=bad)


def test_factor_output_is_closed_inert_and_documents_episode_semantics():
    payload = factor()
    v3.assert_closed_output_schemas(payload, kind="factor")
    assert set(payload) == set(v3.FACTOR_OUTPUT_FIELDS)
    assert payload["episode_semantics_id"] == v3.EPISODE_SEMANTICS_ID
    assert payload["episode_semantics"] == v3.EPISODE_SEMANTICS
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False


def test_watch_currency_must_be_a_pair_leg():
    with pytest.raises(v3.ContractViolation, match="currency_not_in_instrument"):
        v3.bind_watch_candidate_v3(
            factor(),
            instrument="EUR_USD",
            arm="news_only",
            direction="short",
            horizon_min=15,
        )


def test_watch_direction_must_match_currency_factor_pair_semantics():
    # Positive JPY means USD_JPY short, not long.
    with pytest.raises(v3.ContractViolation, match="currency_factor_direction_mismatch"):
        v3.bind_watch_candidate_v3(
            factor(),
            instrument="USD_JPY",
            arm="news_only",
            direction="long",
            horizon_min=15,
        )


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"instrument": "BAD_PAIR", "arm": "news_only", "direction": "short", "horizon_min": 15}, "instrument_not_canonical"),
        ({"instrument": "USD_JPY", "arm": "bad_arm", "direction": "short", "horizon_min": 15}, "arm_not_allowlisted"),
        ({"instrument": "USD_JPY", "arm": "news_only", "direction": "neutral", "horizon_min": 15}, "direction_not_allowlisted"),
        ({"instrument": "USD_JPY", "arm": "news_magnitude_ranked_h5", "direction": "short", "horizon_min": 15}, "arm_horizon_mismatch"),
    ],
)
def test_watch_exact_universe_direction_arm_and_horizon_allowlists(kwargs, match):
    with pytest.raises(v3.ContractViolation, match=match):
        v3.bind_watch_candidate_v3(factor(), **kwargs)


def test_watch_output_and_identity_bind_evidence_and_exact_cohort():
    source_factor = factor()
    watch = v3.bind_watch_candidate_v3(
        source_factor,
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    v3.assert_closed_output_schemas(watch, kind="watch")
    assert set(watch) == set(v3.WATCH_OUTPUT_FIELDS)
    assert watch["cohort_id"] == v3.WATCH_COHORT_BY_ARM["news_only"]
    assert watch["source_evidence_digest_sha256"] == source_factor["source_evidence_digest_sha256"]
    assert watch["collector_contract_id"] == v3.REQUIRED_COLLECTOR_CONTRACT_ID
    assert watch["collector_cohort_id"] == v3.REQUIRED_COLLECTOR_COHORT_ID
    assert watch["research_only"] is True
    assert watch["execution_eligible"] is False


def test_watch_identity_changes_when_source_evidence_changes():
    first = v3.bind_watch_candidate_v3(
        factor(source_evidence=[evidence(payload_sha256="a" * 64)]),
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    second = v3.bind_watch_candidate_v3(
        factor(source_evidence=[evidence(payload_sha256="b" * 64)]),
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    assert first["entry_id"] != second["entry_id"]


def test_tampered_factor_evidence_list_or_digest_fails_closed():
    for field, value in (
        ("event_ids", ["different-event"]),
        ("payload_sha256s", ["b" * 64]),
        ("source_evidence_digest_sha256", "b" * 64),
    ):
        tampered = factor()
        tampered[field] = value
        with pytest.raises(v3.ContractViolation, match="factor_source_evidence"):
            v3.bind_watch_candidate_v3(
                tampered,
                instrument="USD_JPY",
                arm="news_only",
                direction="short",
                horizon_min=15,
            )


def test_factor_schema_tampering_cannot_add_broker_authorization_or_promotion():
    for key in ("broker_order", "authorization_id", "promotion_state", "operational_route"):
        tampered = factor()
        tampered[key] = "forged"
        with pytest.raises(v3.ContractViolation, match="closed_schema_mismatch"):
            v3.bind_watch_candidate_v3(
                tampered,
                instrument="USD_JPY",
                arm="news_only",
                direction="short",
                horizon_min=15,
            )


def test_watch_output_contains_no_operational_surface_keys():
    watch = v3.bind_watch_candidate_v3(
        factor(),
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    forbidden = {"broker_order", "authorization_id", "promotion_state", "operational_route", "can_place_orders"}
    assert not forbidden.intersection(watch)


def test_non_plain_subclasses_fail_closed():
    class CallbackText(str):
        pass

    payload = v3.build_persistent_policy_context_v3(
        [article(event_id=CallbackText("forged"))], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    with pytest.raises(v3.ContractViolation, match="non_plain_json"):
        factor(source_evidence=[evidence(event_id=CallbackText("forged"))])


def test_as_of_rejects_naive_custom_or_subclass_datetime():
    class CustomDatetime(dt.datetime):
        pass

    for bad in (
        dt.datetime(2026, 8, 17, 13, 0),
        CustomDatetime(2026, 8, 17, 13, 0, tzinfo=UTC),
    ):
        with pytest.raises(v3.ContractViolation):
            v3.build_persistent_policy_context_v3([article()], as_of=bad)
