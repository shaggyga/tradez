from __future__ import annotations

import copy
import datetime as dt
import hashlib
from pathlib import Path

import pytest

from src.forex_system.research import persistent_policy_watch_provenance_v4 as v4


UTC = dt.timezone.utc
AS_OF = dt.datetime(2026, 8, 17, 13, 0, tzinfo=UTC)
V3_MODULE_SHA256 = "f44efe05e968a770eaf07a6f18aa5bdb3f7e9de569ca3f277ec0b54eb180d2d1"
V3_TEST_SHA256 = "333911390528ddc70ae979e9a5b1c8a1ed63f21f4d2a08c36c83c157768625c6"


def article(**overrides):
    value = {
        "source_verified": True,
        "official_policy_release": True,
        "source_id": "central_bank",
        "source_currencies": ["JPY"],
        "event_id": "event-current",
        "headline": "Policy decision",
        "summary": "The central bank kept policy unchanged.",
        "published_utc": "2026-08-17T02:00:00Z",
        "first_seen_utc": "2026-08-17T02:00:01Z",
        "causal_known_utc": "2026-08-17T02:00:01Z",
        "scheduled_utc": "",
        "structured_event": False,
        "monetary_impulse": 0.0,
        "material_content_sha256": "1" * 64,
        "observation_clock_trusted": True,
        "observation_time_contract_id": v4.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v4.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v4.REQUIRED_COLLECTOR_COHORT_ID,
    }
    value.update(overrides)
    return value


def evidence(
    event_id="event-1",
    story_cluster_id="story-1",
    payload_sha256="a" * 64,
    **overrides,
):
    value = {
        "event_id": event_id,
        "story_cluster_id": story_cluster_id,
        "payload_sha256": payload_sha256,
        "observation_clock_trusted": True,
        "observation_time_contract_id": v4.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v4.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v4.REQUIRED_COLLECTOR_COHORT_ID,
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
    return v4.build_currency_factor_v4(**args)


def test_v3_exact_bytes_are_preserved():
    root = Path(__file__).resolve().parent
    assert hashlib.sha256(
        (root / "src/forex_system/research/persistent_policy_watch_provenance_v3.py").read_bytes()
    ).hexdigest() == V3_MODULE_SHA256
    assert hashlib.sha256(
        (root / "test_persistent_policy_watch_provenance_v3.py").read_bytes()
    ).hexdigest() == V3_TEST_SHA256


def test_proof_grade_requires_nonempty_content_hash():
    payload = v4.build_persistent_policy_context_v4(
        [article(material_content_sha256="")], as_of=AS_OF
    )
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    assert "proof_grade_content_hash_required" in payload["currencies"]["JPY"]["missing_reason"]


def test_legacy_document_may_remain_diagnostic_without_content_hash():
    payload = v4.build_persistent_policy_context_v4(
        [article(material_content_sha256="", collector_cohort_id="legacy_cohort")],
        as_of=AS_OF,
    )
    row = payload["currencies"]["JPY"]
    assert row["status"] == "missing"
    assert row["legacy_diagnostic_context"]["grade"] == "legacy_diagnostic_only"


def test_current_content_hashed_document_is_proof_grade_and_inert():
    payload = v4.build_persistent_policy_context_v4([article()], as_of=AS_OF)
    row = payload["currencies"]["JPY"]
    assert row["status"] == "proof_grade_current"
    assert row["proof_grade_context"]["content_sha256"] == "1" * 64
    assert row["proof_grade_context"]["policy_context_contract_id"] == v4.POLICY_CONTEXT_CONTRACT_ID
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False


def test_exact_duplicate_policy_rows_collapse_without_conflict():
    value = article()
    payload = v4.build_persistent_policy_context_v4([value, dict(value)], as_of=AS_OF)
    assert payload["coverage"]["proof_grade_document_count"] == 1
    assert not any(row["reason"] == "same_key_policy_conflict" for row in payload["rejections"])


def test_same_key_policy_variants_fail_closed():
    first = article(summary="Version A", material_content_sha256="a" * 64)
    second = article(summary="Version B", material_content_sha256="b" * 64)
    payload = v4.build_persistent_policy_context_v4([first, second], as_of=AS_OF)
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    conflict = [row for row in payload["rejections"] if row["reason"] == "same_key_policy_conflict"]
    assert len(conflict) == 1
    assert len(conflict[0]["conflict_version_sha256"]) == 64


def test_policy_conflict_version_and_output_are_input_order_independent():
    first = article(summary="Version A", material_content_sha256="a" * 64)
    second = article(summary="Version B", material_content_sha256="b" * 64)
    left = v4.build_persistent_policy_context_v4([first, second], as_of=AS_OF)
    right = v4.build_persistent_policy_context_v4([second, first], as_of=AS_OF)
    assert left == right


def test_same_key_proof_and_legacy_variants_fail_closed_together():
    proof = article(material_content_sha256="a" * 64)
    legacy = article(
        material_content_sha256="a" * 64,
        collector_cohort_id="legacy_cohort",
    )
    payload = v4.build_persistent_policy_context_v4([proof, legacy], as_of=AS_OF)
    assert payload["coverage"]["proof_grade_currency_count"] == 0
    assert payload["coverage"]["legacy_diagnostic_currency_count"] == 0
    assert any(row["reason"] == "same_key_policy_conflict" for row in payload["rejections"])


def test_different_event_ids_at_same_time_select_deterministically():
    first = article(event_id="event-a", material_content_sha256="a" * 64)
    second = article(event_id="event-b", material_content_sha256="b" * 64)
    left = v4.build_persistent_policy_context_v4([first, second], as_of=AS_OF)
    right = v4.build_persistent_policy_context_v4([second, first], as_of=AS_OF)
    assert left == right
    assert left["currencies"]["JPY"]["proof_grade_context"]["event_id"] == "event-b"


def test_policy_reports_all_21_with_missing_reasons():
    payload = v4.build_persistent_policy_context_v4([article()], as_of=AS_OF)
    assert tuple(payload["currencies"]) == v4.CANONICAL_CURRENCIES
    assert payload["coverage"]["reported_currency_count"] == 21
    assert payload["coverage"]["missing_currency_count"] == 20
    assert payload["currencies"]["CNH"]["missing_reason"] == "no_official_policy_candidate"


def test_syndicated_events_same_story_payload_count_once():
    source = [
        evidence(event_id="wire-copy-a"),
        evidence(event_id="wire-copy-b"),
        evidence(event_id="wire-copy-a"),
    ]
    payload = factor(source_evidence=source)
    assert payload["source_event_count"] == 2
    assert payload["source_story_count"] == 1
    assert payload["event_ids"] == ["wire-copy-a", "wire-copy-b"]
    assert payload["story_cluster_ids"] == ["story-1"]


def test_same_payload_without_cluster_is_canonical_story_and_deduplicated():
    source = [
        evidence(event_id="copy-a", story_cluster_id=""),
        evidence(event_id="copy-b", story_cluster_id=""),
    ]
    payload = factor(source_evidence=source)
    assert payload["source_story_count"] == 1
    assert payload["story_cluster_ids"] == ["payload_" + "a" * 64]


def test_story_digest_is_order_independent():
    first = evidence("copy-a", "story-1", "a" * 64)
    second = evidence("copy-b", "story-1", "a" * 64)
    left = factor(source_evidence=[first, second])
    right = factor(source_evidence=[second, first])
    assert left == right


def test_same_event_to_different_payload_or_story_is_rejected():
    with pytest.raises(v4.ContractViolation, match="ambiguous_event_to_story_payload"):
        factor(
            source_evidence=[
                evidence("event-a", "story-a", "a" * 64),
                evidence("event-a", "story-b", "b" * 64),
            ]
        )


def test_same_payload_to_different_story_is_rejected():
    with pytest.raises(v4.ContractViolation, match="ambiguous_payload_to_story"):
        factor(
            source_evidence=[
                evidence("event-a", "story-a", "a" * 64),
                evidence("event-b", "story-b", "a" * 64),
            ]
        )


def test_same_story_to_different_payload_is_rejected():
    with pytest.raises(v4.ContractViolation, match="ambiguous_story_to_payload"):
        factor(
            source_evidence=[
                evidence("event-a", "story-a", "a" * 64),
                evidence("event-b", "story-a", "b" * 64),
            ]
        )


def test_explicit_and_payload_fallback_story_ambiguity_is_rejected():
    with pytest.raises(v4.ContractViolation, match="ambiguous_payload_to_story"):
        factor(
            source_evidence=[
                evidence("event-a", "story-a", "a" * 64),
                evidence("event-b", "", "a" * 64),
            ]
        )


def test_tampered_story_membership_or_digest_is_rejected_by_watch():
    for mutate in ("event", "story", "payload", "digest"):
        value = factor()
        if mutate == "event":
            value["source_story_evidence"][0]["event_ids"] = ["forged-event"]
        elif mutate == "story":
            value["source_story_evidence"][0]["story_cluster_id"] = "forged-story"
        elif mutate == "payload":
            value["source_story_evidence"][0]["payload_sha256"] = "b" * 64
        else:
            value["source_evidence_digest_sha256"] = "b" * 64
        with pytest.raises(v4.ContractViolation):
            v4.bind_watch_candidate_v4(
                value,
                instrument="USD_JPY",
                arm="news_only",
                direction="short",
                horizon_min=15,
            )


def test_watch_rejects_one_event_reused_across_two_stories():
    factor = v4.build_currency_factor_v4(
        episode_id="episode-jpy",
        currency="JPY",
        score=0.7,
        confidence=0.8,
        horizon_min=60,
        source_evidence=[evidence()],
    )
    forged = copy.deepcopy(factor)
    first = forged["source_story_evidence"][0]
    second = {
        **first,
        "story_cluster_id": "story-duplicate-owner",
        "payload_sha256": "b" * 64,
    }
    forged["source_story_evidence"].append(second)
    forged["story_cluster_ids"].append(second["story_cluster_id"])
    forged["payload_sha256s"].append(second["payload_sha256"])
    forged["source_story_count"] = 2
    forged["source_evidence_digest_sha256"] = v4._sha256_json(
        {
            "episode_semantics_id": v4.EPISODE_SEMANTICS_ID,
            "canonical_stories": forged["source_story_evidence"],
        }
    )
    with pytest.raises(v4.ContractViolation, match="ambiguous_event"):
        v4.bind_watch_candidate_v4(
            forged,
            instrument="USD_JPY",
            arm="technical_confirmed",
            direction="sell",
            horizon_min=60,
        )


def test_watch_identity_binds_story_digest_and_is_inert():
    value = factor(
        source_evidence=[
            evidence("copy-a", "story-1", "a" * 64),
            evidence("copy-b", "story-1", "a" * 64),
        ]
    )
    watch = v4.bind_watch_candidate_v4(
        value,
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    assert set(watch) == set(v4.WATCH_OUTPUT_FIELDS)
    assert watch["source_story_count"] == 1
    assert watch["source_event_count"] == 2
    assert watch["source_evidence_digest_sha256"] == value["source_evidence_digest_sha256"]
    assert watch["research_only"] is True
    assert watch["execution_eligible"] is False


def test_watch_identity_changes_when_story_evidence_changes():
    first = v4.bind_watch_candidate_v4(
        factor(source_evidence=[evidence(payload_sha256="a" * 64)]),
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    second = v4.bind_watch_candidate_v4(
        factor(source_evidence=[evidence(payload_sha256="b" * 64)]),
        instrument="USD_JPY",
        arm="news_only",
        direction="short",
        horizon_min=15,
    )
    assert first["entry_id"] != second["entry_id"]


def test_closed_schemas_cannot_pass_operational_fields():
    with pytest.raises(v4.ContractViolation, match="closed_schema_mismatch"):
        factor(source_evidence=[evidence(authorization_id="forged")])
    value = factor()
    value["broker_order"] = {"units": 1}
    with pytest.raises(v4.ContractViolation, match="closed_schema_mismatch"):
        v4.bind_watch_candidate_v4(
            value,
            instrument="USD_JPY",
            arm="news_only",
            direction="short",
            horizon_min=15,
        )


def test_canonical_currency_pair_direction_contract_remains_closed():
    with pytest.raises(v4.ContractViolation, match="currency_not_in_instrument"):
        v4.bind_watch_candidate_v4(
            factor(),
            instrument="EUR_USD",
            arm="news_only",
            direction="short",
            horizon_min=15,
        )
    with pytest.raises(v4.ContractViolation, match="currency_factor_direction_mismatch"):
        v4.bind_watch_candidate_v4(
            factor(),
            instrument="USD_JPY",
            arm="news_only",
            direction="long",
            horizon_min=15,
        )
