from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest

from src.forex_system.research import persistent_policy_watch_provenance_v6 as v6
from src.forex_system.research import persistent_policy_watch_provenance_v5 as v5


V5_MODULE_SHA256 = "2b2fa9b601923493205676e5572cb06c5ee92a948d64b33e305aaad1aef2cb2a"
V5_TEST_SHA256 = "beedf5d0a643009f76eda324009991e44834ef487529cabf5028610f0cff4954"


def evidence(event_id="event-1", story_cluster_id="story-1", payload="a" * 64):
    return {
        "event_id": event_id,
        "story_cluster_id": story_cluster_id,
        "payload_sha256": payload,
        "observation_clock_trusted": True,
        "observation_time_contract_id": v6.REQUIRED_OBSERVATION_TIME_CONTRACT_ID,
        "collector_contract_id": v6.REQUIRED_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": v6.REQUIRED_COLLECTOR_COHORT_ID,
    }


def factor(**overrides):
    args = {
        "episode_id": "episode-jpy",
        "currency": "JPY",
        "score": 0.5,
        "confidence": 0.7,
        "horizon_min": 15,
        "source_evidence": [evidence()],
    }
    args.update(overrides)
    return v6.build_currency_factor_v6(**args)


def bind(value=None, **overrides):
    args = {
        "instrument": "USD_JPY",
        "arm": "news_only",
        "direction": "short",
        "horizon_min": 15,
    }
    args.update(overrides)
    return v6.bind_watch_candidate_v6(factor() if value is None else value, **args)


def test_v5_exact_bytes_are_preserved():
    root = Path(__file__).resolve().parent
    assert hashlib.sha256(
        (root / "src/forex_system/research/persistent_policy_watch_provenance_v5.py").read_bytes()
    ).hexdigest() == V5_MODULE_SHA256
    assert hashlib.sha256(
        (root / "test_persistent_policy_watch_provenance_v5.py").read_bytes()
    ).hexdigest() == V5_TEST_SHA256


def test_factor_and_watch_round_trip_are_inert():
    value = factor()
    watch = bind(value)
    assert value["research_only"] is True and value["execution_eligible"] is False
    assert watch["research_only"] is True and watch["execution_eligible"] is False
    assert watch["factor_score"] == 0.5


def test_mutable_v4_number_monkeypatch_cannot_weaken_v6(monkeypatch):
    monkeypatch.setattr(v5.v4, "_number", lambda *args, **kwargs: 999.0)
    with pytest.raises(v6.ContractViolation, match="above_maximum"):
        factor(score=999)
    assert bind()["factor_score"] == 0.5


def test_mutable_v5_number_monkeypatch_cannot_weaken_v6(monkeypatch):
    monkeypatch.setattr(v5, "_number", lambda *args, **kwargs: 999.0)
    with pytest.raises(v6.ContractViolation, match="above_maximum"):
        factor(score=999)


def test_mutable_v3_direction_monkeypatch_cannot_weaken_v6(monkeypatch):
    monkeypatch.setattr(
        v5.v4.v3,
        "_expected_pair_direction",
        lambda *args, **kwargs: "long",
    )
    assert bind()["direction"] == "short"
    with pytest.raises(v6.ContractViolation, match="direction_mismatch"):
        bind(direction="long")


def test_mutable_v4_story_normalizer_monkeypatch_cannot_weaken_v6(monkeypatch):
    monkeypatch.setattr(
        v5.v4,
        "_normalize_source_stories",
        lambda *args, **kwargs: ([], [], "0" * 64),
    )
    with pytest.raises(v6.ContractViolation, match="source_evidence_required"):
        factor(source_evidence=[])
    assert factor()["source_story_count"] == 1


def test_dependency_constant_drift_after_import_cannot_change_v6(monkeypatch):
    original = v6.REQUIRED_COLLECTOR_CONTRACT_ID
    monkeypatch.setattr(v5, "REQUIRED_COLLECTOR_CONTRACT_ID", "forged")
    monkeypatch.setattr(v5.v4, "REQUIRED_COLLECTOR_CONTRACT_ID", "forged")
    assert v6.REQUIRED_COLLECTOR_CONTRACT_ID == original
    assert bind()["collector_contract_id"] == original


def test_zero_evidence_with_recomputed_digest_fails():
    forged = factor()
    for key in ("event_ids", "story_cluster_ids", "payload_sha256s", "source_story_evidence"):
        forged[key] = []
    forged["source_event_count"] = 0
    forged["source_story_count"] = 0
    forged["source_evidence_digest_sha256"] = v6._sha256_json(
        {"episode_semantics_id": v6.EPISODE_SEMANTICS_ID, "canonical_stories": []}
    )
    with pytest.raises(v6.ContractViolation, match="shape_invalid"):
        bind(forged)


@pytest.mark.parametrize(("field", "value"), [("research_only", False), ("execution_eligible", True)])
def test_forged_inert_flags_fail(field, value):
    forged = factor()
    forged[field] = value
    with pytest.raises(v6.ContractViolation, match="factor_not_inert"):
        bind(forged)


def test_zero_score_fails():
    forged = factor()
    forged["score"] = 0.0
    with pytest.raises(v6.ContractViolation, match="score_must_be_directional"):
        bind(forged)


@pytest.mark.parametrize("bad", [True, 0, -1, 999999])
def test_invalid_factor_horizons_fail(bad):
    with pytest.raises(v6.ContractViolation, match="factor_horizon_invalid"):
        factor(horizon_min=bad)


@pytest.mark.parametrize("bad", [True, 0, -1, 999999])
def test_invalid_declared_horizons_fail(bad):
    with pytest.raises(v6.ContractViolation, match="declared_horizon_invalid"):
        bind(horizon_min=bad)


def test_fixed_arm_suffix_binds_factor_and_declared_horizon():
    with pytest.raises(v6.ContractViolation, match="arm_horizon_mismatch"):
        bind(factor(horizon_min=15), arm="news_magnitude_ranked_h5", horizon_min=5)
    assert bind(
        factor(horizon_min=5), arm="news_magnitude_ranked_h5", horizon_min=5
    )["horizon_min"] == 5


def test_story_payload_alignment_b_then_a_round_trips():
    value = factor(
        source_evidence=[
            evidence("event-a", "story-a", "b" * 64),
            evidence("event-b", "story-b", "a" * 64),
        ]
    )
    assert value["payload_sha256s"] == ["b" * 64, "a" * 64]
    watch = bind(value)
    assert list(zip(watch["story_cluster_ids"], watch["payload_sha256s"])) == [
        ("story-a", "b" * 64),
        ("story-b", "a" * 64),
    ]


def test_nested_digest_tampering_fails():
    forged = copy.deepcopy(factor())
    forged["source_story_evidence"][0]["event_ids"] = ["forged-event"]
    with pytest.raises(v6.ContractViolation):
        bind(forged)


def test_closed_schema_blocks_operational_fields():
    forged = factor()
    forged["authorization_id"] = "forged"
    with pytest.raises(v6.ContractViolation, match="closed_schema_mismatch"):
        bind(forged)
