from __future__ import annotations

import copy
import datetime as dt
import hashlib
from pathlib import Path

import pytest

from src.forex_system.research import persistent_policy_watch_provenance_v5 as v5


UTC = dt.timezone.utc
AS_OF = dt.datetime(2026, 8, 17, 13, 50, tzinfo=UTC)
V4_MODULE_SHA256 = "7879b3f574d635a7f9f845bbdb8400b062da3c15d6fb9472735e7fd8ea3132e9"
V4_TEST_SHA256 = "d73ff5e3ef5bb6af07909b19a30b877a90c556088756358f5ca60875b39861bb"


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
        "observation_time_contract_id": v5.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v5.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v5.REQUIRED_COLLECTOR_COHORT_ID,
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
        "observation_time_contract_id": v5.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v5.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v5.REQUIRED_COLLECTOR_COHORT_ID,
    }
    value.update(overrides)
    return value


def factor(**overrides):
    args = {
        "episode_id": "news-episode-1",
        "currency": "JPY",
        "score": 0.5,
        "confidence": 0.7,
        "horizon_min": 15,
        "source_evidence": [evidence()],
    }
    args.update(overrides)
    return v5.build_currency_factor_v5(**args)


def bind(value=None, **overrides):
    args = {
        "instrument": "USD_JPY",
        "arm": "news_only",
        "direction": "short",
        "horizon_min": 15,
    }
    args.update(overrides)
    return v5.bind_watch_candidate_v5(value or factor(), **args)


def test_v4_exact_bytes_are_preserved():
    root = Path(__file__).resolve().parent
    assert hashlib.sha256(
        (root / "src/forex_system/research/persistent_policy_watch_provenance_v4.py").read_bytes()
    ).hexdigest() == V4_MODULE_SHA256
    assert hashlib.sha256(
        (root / "test_persistent_policy_watch_provenance_v4.py").read_bytes()
    ).hexdigest() == V4_TEST_SHA256


def test_policy_v5_retags_frozen_v4_result_and_remains_inert():
    result = v5.build_persistent_policy_context_v5([article()], as_of=AS_OF)
    assert result["schema_version"] == "persistent_policy_context_v5"
    assert result["contract_id"] == v5.POLICY_CONTEXT_CONTRACT_ID
    assert result["proof_grade_documents"][0]["policy_context_contract_id"] == v5.POLICY_CONTEXT_CONTRACT_ID
    assert result["research_only"] is True
    assert result["execution_eligible"] is False


def test_factor_and_watch_round_trip_use_exact_inert_contract():
    value = factor()
    watch = bind(value)
    assert value["research_only"] is True and value["execution_eligible"] is False
    assert watch["research_only"] is True and watch["execution_eligible"] is False
    assert watch["source_evidence_digest_sha256"] == value["source_evidence_digest_sha256"]
    assert watch["factor_score"] == 0.5


def test_zero_evidence_with_recomputed_empty_digest_fails_closed():
    forged = factor()
    forged["event_ids"] = []
    forged["story_cluster_ids"] = []
    forged["payload_sha256s"] = []
    forged["source_story_evidence"] = []
    forged["source_event_count"] = 0
    forged["source_story_count"] = 0
    forged["source_evidence_digest_sha256"] = v5._sha256_json(
        {"episode_semantics_id": v5.EPISODE_SEMANTICS_ID, "canonical_stories": []}
    )
    with pytest.raises(v5.ContractViolation, match="shape_invalid"):
        bind(forged)


@pytest.mark.parametrize(
    ("field", "value"),
    [("research_only", False), ("research_only", 1), ("execution_eligible", True), ("execution_eligible", 0)],
)
def test_forged_inert_flags_fail_closed(field, value):
    forged = factor()
    forged[field] = value
    with pytest.raises(v5.ContractViolation, match="factor_not_inert"):
        bind(forged)


def test_forged_zero_score_fails_even_with_valid_story_digest():
    forged = factor()
    forged["score"] = 0.0
    with pytest.raises(v5.ContractViolation, match="score_must_be_directional"):
        bind(forged)


@pytest.mark.parametrize("bad", [True, 0, -1, 999999])
def test_builder_rejects_invalid_factor_horizons(bad):
    with pytest.raises(v5.ContractViolation, match="factor_horizon_invalid"):
        factor(horizon_min=bad)


@pytest.mark.parametrize("bad", [True, 0, -1, 999999])
def test_binder_rejects_forged_factor_horizons(bad):
    forged = factor()
    forged["horizon_min"] = bad
    with pytest.raises(v5.ContractViolation, match="factor_horizon_invalid"):
        bind(forged)


@pytest.mark.parametrize("bad", [True, 0, -1, 999999])
def test_binder_rejects_invalid_declared_horizons(bad):
    with pytest.raises(v5.ContractViolation, match="declared_horizon_invalid"):
        bind(horizon_min=bad)


def test_fixed_horizon_arm_requires_factor_and_declared_horizon_match_suffix():
    with pytest.raises(v5.ContractViolation, match="arm_horizon_mismatch"):
        bind(
            factor(horizon_min=15),
            arm="news_magnitude_ranked_h5",
            horizon_min=5,
        )
    watch = bind(
        factor(horizon_min=5),
        arm="news_magnitude_ranked_h5",
        horizon_min=5,
    )
    assert watch["horizon_min"] == 5


def test_unsuffixed_arm_requires_declared_horizon_match_factor():
    with pytest.raises(v5.ContractViolation, match="arm_horizon_mismatch"):
        bind(factor(horizon_min=30), horizon_min=15)


def test_multistory_alignment_round_trip_allows_payload_b_then_a():
    value = factor(
        source_evidence=[
            evidence("event-a", "story-a", "b" * 64),
            evidence("event-b", "story-b", "a" * 64),
        ]
    )
    assert value["story_cluster_ids"] == ["story-a", "story-b"]
    assert value["payload_sha256s"] == ["b" * 64, "a" * 64]
    watch = bind(value)
    assert list(zip(watch["story_cluster_ids"], watch["payload_sha256s"])) == [
        ("story-a", "b" * 64),
        ("story-b", "a" * 64),
    ]


def test_independently_sorted_payload_index_is_rejected():
    forged = factor(
        source_evidence=[
            evidence("event-a", "story-a", "b" * 64),
            evidence("event-b", "story-b", "a" * 64),
        ]
    )
    forged["payload_sha256s"] = sorted(forged["payload_sha256s"])
    with pytest.raises(v5.ContractViolation, match="story_payload_index_mismatch"):
        bind(forged)


def test_recomputed_digest_cannot_make_duplicate_story_rows_canonical():
    forged = copy.deepcopy(factor())
    duplicate = copy.deepcopy(forged["source_story_evidence"][0])
    forged["source_story_evidence"].append(duplicate)
    forged["story_cluster_ids"].append(duplicate["story_cluster_id"])
    forged["payload_sha256s"].append(duplicate["payload_sha256"])
    forged["source_story_count"] = 2
    forged["source_evidence_digest_sha256"] = v5._sha256_json(
        {
            "episode_semantics_id": v5.EPISODE_SEMANTICS_ID,
            "canonical_stories": forged["source_story_evidence"],
        }
    )
    with pytest.raises(v5.ContractViolation, match="story_pairs_not_canonical"):
        bind(forged)


def test_score_direction_contract_is_rechecked_by_binder():
    with pytest.raises(v5.ContractViolation, match="currency_factor_direction_mismatch"):
        bind(direction="long")


def test_tampered_nested_story_digest_is_rejected():
    forged = copy.deepcopy(factor())
    forged["source_story_evidence"][0]["event_ids"] = ["forged-event"]
    with pytest.raises(v5.ContractViolation):
        bind(forged)


def test_closed_schema_rejects_operational_fields():
    forged = factor()
    forged["authorization_id"] = "forged"
    with pytest.raises(v5.ContractViolation, match="closed_schema_mismatch"):
        bind(forged)
