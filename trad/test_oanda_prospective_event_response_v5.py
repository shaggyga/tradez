from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from forex_system.ingestion.prospective_event_response_v5 import (  # noqa: E402
    CANONICAL_INSTRUMENTS,
    CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
    CLOCK_CONTRACT_ID,
    CLOCK_SCHEMA_VERSION,
    COHORT_ID,
    CONTRACT_ID,
    ProspectiveEventResponseV5Error,
    build_event_plans,
    build_samples,
    derive_top_status,
    discover_local_dependency_closure,
    insert_temp_rows,
    load_contract,
    make_temp_test_contract,
    open_temp_ledger,
    quote_snapshot_candidates,
    read_temp_rows,
    validate_cohort_identity,
    validate_policy_dependency_registry,
    validate_temp_contract,
    verify_pending_closure,
)
import oanda_prospective_event_response_v5 as collector  # noqa: E402


UTC = dt.timezone.utc
CONTRACT_PATH = ROOT / "config" / "prospective_event_response_capture_v5.json"
CLOSURE_PATH = ROOT / "config" / "prospective_event_response_v5_pending_closure.json"
POLICY_PATH = ROOT / "config" / "currency_policy_dependency_registry_v1.json"


def stamp(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@pytest.fixture
def candidate() -> dict:
    return load_contract(CONTRACT_PATH)


@pytest.fixture
def contract(candidate: dict) -> dict:
    return make_temp_test_contract(candidate, cohort_start_utc=stamp("2026-08-17T03:50:00Z"))


@pytest.fixture
def policy() -> dict:
    return json.loads(POLICY_PATH.read_text(encoding="utf-8"))


def official_event(**overrides: object) -> dict:
    row = {
        "category": "monetary_policy",
        "clock_semantic_id": "clock_semantic_fomc",
        "clock_semantics": "domestic_official_policy_release",
        "currencies": ["USD"],
        "direct_currencies": ["USD"],
        "event_availability_utc": "2026-08-17T03:59:00Z",
        "event_snapshot_first_observed_utc": "2026-08-17T03:58:00Z",
        "event_snapshot_first_trusted_observed_utc": "2026-08-17T03:59:00Z",
        "event_time_basis": "scheduled_release",
        "event_utc": "2026-08-17T05:00:00Z",
        "event_version_id": "event_version_one",
        "headline": "FOMC monetary policy decision",
        "independent_domestic_event": True,
        "ledger_effective_known_utc": "2026-08-17T03:59:00Z",
        "linked_policy_factor": False,
        "material_content_sha256": "f" * 64,
        "original_fact_known_utc": "2026-08-17T03:45:00Z",
        "raw_event_availability_utc": "2026-08-17T03:59:00Z",
        "release_rule_bytes_verified": True,
        "release_time_rule_archive_name": "fomc_schedule.html",
        "release_time_rule_observed_sha256": "e" * 64,
        "schedule_window_end_utc": "",
        "scheduled_utc": "2026-08-17T05:00:00Z",
        "source_cohort_id": "fomc_schedule_v1",
        "source_contract_first_observed_utc": "2026-08-17T03:58:00Z",
        "source_contract_first_trusted_observed_utc": "2026-08-17T03:59:00Z",
        "source_contract_id": "fomc_schedule_v1",
        "source_id": "fomc_schedule",
        "source_name": "Federal Reserve",
        "source_type": "official_html",
        "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        "source_verified": True,
        "timing_precision": "minute",
        "trusted_for_prospective_evidence": True,
        "upstream_event_id": "official-fomc-release",
        "upstream_first_known_utc": "2026-08-17T03:45:00Z",
        "upstream_updated_utc": "2026-08-17T03:45:00Z",
    }
    row.update(overrides)
    return row


def clock(*, at: str, semantic_at: str | None = None, events: list[dict] | None = None) -> dict:
    now = stamp(at)
    source = stamp(semantic_at) if semantic_at else now - dt.timedelta(seconds=5)
    attested = now - dt.timedelta(seconds=1)
    attestation = {
        "age_at_capture_sec": 1.0,
        "artifact_name": "clock_integrity_v1.json",
        "attested": True,
        "generated_utc": iso(attested),
        "host_clock_synchronized": True,
        "maximum_age_sec": 90.0,
        "payload_sha256": "d" * 64,
        "source_fresh": True,
        "state": "fresh_trusted",
        "status": "ok",
        "timestamp_normalization_trusted": True,
    }
    event_rows = [official_event()] if events is None else events
    return {
        "schema_version": CLOCK_SCHEMA_VERSION,
        "contract_id": CLOCK_CONTRACT_ID,
        "decision_cutoff_utc": iso(now),
        "snapshot_id": "clock_snapshot_v5_fixture",
        "snapshot_captured_utc": iso(now - dt.timedelta(seconds=2)),
        "clock_observation_id": "clock_observation_v5_fixture_" + at.replace(":", ""),
        "clock_observed_utc": iso(now - dt.timedelta(seconds=1)),
        "clock_observation_source_generated_utc": iso(source),
        "clock_observation_events_sha256": "a" * 64,
        "clock_observation_manifest_sha256": "b" * 64,
        "clock_observation_semantic_clock_sha256": "c" * 64,
        "source_generated_utc": iso(source),
        "source_pipeline_version": "all_pair_news_event_tags_v3",
        "events_sha256": "a" * 64,
        "manifest_sha256": "b" * 64,
        "semantic_snapshot_events_sha256": "a" * 64,
        "semantic_snapshot_manifest_sha256": "b" * 64,
        "clock_attestation": copy.deepcopy(attestation),
        "snapshot_clock_attestation": copy.deepcopy(attestation),
        "event_count": len(event_rows),
        "events": event_rows,
        "policy": {
            "calendar_is_not_direction": True,
            "as_of_uses_last_observed_complete_snapshot": True,
            "source_contract_versions_cannot_be_backdated": True,
            "event_availability_is_maximum_causal_clock": True,
            "proof_availability_requires_fresh_trusted_capture": True,
            "research_only": True,
            "can_place_orders": False,
            "can_promote": False,
        },
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }


def cohort_identity(policy: dict, **overrides: object) -> dict:
    policy_hash = validate_policy_dependency_registry(policy)
    row = {
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "contract_payload_sha256": sha(CONTRACT_PATH.read_bytes()),
        "closure_manifest_sha256": sha(CLOSURE_PATH.read_bytes()),
        "source_dependency_sha256": {"clock_v2": "1" * 64, "pending_source_bridge": "2" * 64},
        "policy_dependency_registry_sha256": policy_hash,
        "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
        "clock_contract_id": CLOCK_CONTRACT_ID,
        "clock_schema_version": CLOCK_SCHEMA_VERSION,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "promotion_eligible": False,
        "authorization_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    row.update(overrides)
    return row


def bundle_identity(policy: dict) -> dict:
    manifest = json.loads(CLOSURE_PATH.read_text(encoding="utf-8"))
    identity = cohort_identity(policy)
    identity["source_dependency_sha256"] = {
        row["path"]: row["sha256"]
        for row in [*manifest["files"], *manifest["source_artifacts"]]
    }
    return identity


def clock_provenance(snapshot: dict, *, at: str) -> dict:
    return {
        "source_kind": "immutable_clock_v2_reconstruct",
        "clock_database_resolved_path": str(ROOT / "fixture_clock_v2.sqlite"),
        "clock_database_device": 1,
        "clock_database_file_id": 2,
        "clock_database_size": 4096,
        "reconstructed_at_utc": at,
        "clock_snapshot_payload_sha256": hashlib.sha256(
            json.dumps(
                snapshot,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest(),
        "clock_snapshot_id": snapshot["snapshot_id"],
        "clock_observation_id": snapshot["clock_observation_id"],
    }


def plans(contract: dict, policy: dict, identity: dict | None = None, source_clock: dict | None = None) -> dict:
    selected_clock = source_clock or clock(at="2026-08-17T04:00:00Z", semantic_at="2026-08-17T03:59:55Z")
    return build_event_plans(
        selected_clock,
        clock_provenance=clock_provenance(selected_clock, at="2026-08-17T04:00:00Z"),
        contract=contract,
        policy_dependency_registry=policy,
        cohort_identity=identity or cohort_identity(policy),
        planned_utc=stamp("2026-08-17T04:00:00Z"),
    )


def quote_artifact(*, at: str, instruments: tuple[str, ...] = CANONICAL_INSTRUMENTS) -> tuple[bytes, dict]:
    now = stamp(at)
    payload = {
        "schema_version": 2,
        "generated_utc": iso(now),
        "quotes": {
            instrument: {
                "bid": 1.1000,
                "ask": 1.1002,
                "pip": 0.0001,
                "source": "practice_stream",
                "time": iso(now - dt.timedelta(milliseconds=500)),
            }
            for instrument in instruments
        },
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    attestation = {
        "schema_version": 1,
        "state": "fresh_trusted",
        "attested": True,
        "artifact_name": "practice_quotes_v2.json",
        "payload_sha256": sha(raw),
        "byte_length": len(raw),
        "generated_utc": iso(now - dt.timedelta(seconds=1)),
        "quote_snapshot_generated_utc": iso(now),
        "instrument_universe_sha256": CANONICAL_INSTRUMENT_UNIVERSE_SHA256,
        "instrument_count": 68,
    }
    return raw, attestation


def observations(contract: dict, identity: dict, *, at: str) -> list[dict]:
    raw, attestation = quote_artifact(at=at)
    validated = validate_cohort_identity(identity, policy_registry_sha256=identity["policy_dependency_registry_sha256"])
    return quote_snapshot_candidates(
        raw,
        artifact_name="practice_quotes_v2.json",
        market_attestation=attestation,
        contract=contract,
        collector_observed_utc=stamp(at),
        cohort_identity_sha256=validated["cohort_identity_sha256"],
    )


def baseline_plan(built: dict, instrument: str = "EUR_USD") -> dict:
    return next(row for row in built["plans"] if row["instrument"] == instrument and row["target_offset_sec"] == 0)


def test_p1_freeze_review_chronology_cannot_be_self_asserted(candidate: dict, tmp_path: Path) -> None:
    assert candidate["candidate_bytes_frozen_utc"] is None
    assert candidate["independent_reviewed_utc"] is None
    assert candidate["cohort_start_utc"] is None
    assert candidate["review_state"] == "blocked_external_dependencies"
    with pytest.raises(ProspectiveEventResponseV5Error, match="cannot_self_assert"):
        verify_pending_closure(ROOT, CLOSURE_PATH, require_reviewed=True)
    mutated = copy.deepcopy(candidate)
    mutated["candidate_bytes_frozen_utc"] = "2026-08-17T00:00:00Z"
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(mutated), encoding="utf-8")
    with pytest.raises(ProspectiveEventResponseV5Error, match="contract_mismatch:candidate_bytes_frozen_utc"):
        load_contract(path)


def test_p1_complete_direct_transitive_closure_and_unapproved_dependencies(tmp_path: Path) -> None:
    manifest = verify_pending_closure(ROOT, CLOSURE_PATH)
    discovered = discover_local_dependency_closure(ROOT, manifest["entry_paths"])
    assert discovered == {row["path"] for row in manifest["files"]}
    assert "src/forex_system/ingestion/immutable_event_clock.py" in discovered
    broken = copy.deepcopy(manifest)
    broken["files"] = broken["files"][:-1]
    path = tmp_path / "broken.json"
    path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(ProspectiveEventResponseV5Error, match="path_set_mismatch"):
        verify_pending_closure(ROOT, path)


def test_p1_full_event_payload_mutation_is_rejected_before_baseline(contract: dict, policy: dict) -> None:
    identity = cohort_identity(policy)
    built = plans(contract, policy, identity)
    plan = baseline_plan(built)
    changed_event = official_event(headline="Mutated after planning")
    current = clock(at="2026-08-17T05:00:01Z", semantic_at="2026-08-17T04:59:58Z", events=[changed_event])
    sampled = build_samples(
        [plan], observations(contract, identity, at="2026-08-17T05:00:01Z"),
        existing_samples=[], current_clock_snapshot=current,
        current_clock_provenance=clock_provenance(current, at="2026-08-17T05:00:01Z"),
        contract=contract,
        policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )
    assert sampled["samples"] == []
    assert any(row["reason"] == "event_payload_changed_or_duplicate_before_baseline" for row in sampled["attempts"])
    original_clock = clock(at="2026-08-17T04:00:00Z")
    frozen_provenance = clock_provenance(original_clock, at="2026-08-17T04:00:00Z")
    original_clock["events"][0]["headline"] = "Mutation after provenance was bound"
    with pytest.raises(ProspectiveEventResponseV5Error, match="payload_binding_mismatch"):
        build_event_plans(
            original_clock,
            clock_provenance=frozen_provenance,
            contract=contract,
            policy_dependency_registry=policy,
            cohort_identity=identity,
            planned_utc=stamp("2026-08-17T04:00:00Z"),
        )


def test_p1_duplicate_natural_event_conflict_cannot_inflate_plans(contract: dict, policy: dict) -> None:
    duplicate = official_event(event_version_id="event_version_two")
    source = clock(at="2026-08-17T04:00:00Z", events=[official_event(), duplicate])
    built = plans(contract, policy, source_clock=source)
    assert built["plan_count"] == 0
    assert built["status"] == "planning_failed_closed"
    assert len(built["attempts"]) == 1
    assert built["attempts"][0]["reason"] == "duplicate_natural_event_conflict"
    conflicting = official_event(event_version_id="event_version_three", source_verified=False)
    built2 = plans(contract, policy, source_clock=clock(at="2026-08-17T04:00:00Z", events=[official_event(), conflicting]))
    assert built2["plan_count"] == 0
    assert built2["attempts"][0]["duplicate_count"] == 2


def test_p1_recursive_closed_types_reject_nested_safety_aliases_and_bool_int(contract: dict, policy: dict) -> None:
    bad_event = official_event(metadata={"trade_direction": "buy"})
    with pytest.raises(ProspectiveEventResponseV5Error, match="forbidden_safety_alias"):
        plans(contract, policy, source_clock=clock(at="2026-08-17T04:00:00Z", events=[bad_event]))
    bad_clock = clock(at="2026-08-17T04:00:00Z")
    bad_clock["policy"]["calendar_is_not_direction"] = 1
    with pytest.raises(ProspectiveEventResponseV5Error, match="must_be_boolean"):
        plans(contract, policy, source_clock=bad_clock)
    bad_policy = copy.deepcopy(policy)
    bad_policy["dependencies"][0]["metadata"] = {"maximum_notional": 1000}
    with pytest.raises(ProspectiveEventResponseV5Error, match="forbidden_safety_alias"):
        validate_policy_dependency_registry(bad_policy)


def test_p1_exact_68_universe_and_quote_coverage_are_mandatory(contract: dict, policy: dict) -> None:
    sparse_raw, sparse_attestation = quote_artifact(at="2026-08-17T05:00:01Z", instruments=("EUR_USD",))
    sparse_attestation["instrument_count"] = 1
    sparse_attestation["instrument_universe_sha256"] = sha(b"EUR_USD")
    with pytest.raises(ProspectiveEventResponseV5Error, match="universe_mismatch"):
        quote_snapshot_candidates(
            sparse_raw, artifact_name="practice_quotes_v2.json", market_attestation=sparse_attestation,
            contract=contract, collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
            cohort_identity_sha256="1" * 64,
        )
    mutated = copy.deepcopy(contract)
    mutated["instrument_universe"] = ["EUR_USD"]
    with pytest.raises(ProspectiveEventResponseV5Error, match="exact_68"):
        validate_temp_contract(mutated)


def test_p1_quote_freshness_is_rechecked_at_selection(contract: dict, policy: dict) -> None:
    identity = cohort_identity(policy)
    plan = baseline_plan(plans(contract, policy, identity))
    obs = observations(contract, identity, at="2026-08-17T05:00:01Z")
    sampled = build_samples(
        [plan], obs, existing_samples=[],
        current_clock_snapshot=(current := clock(at="2026-08-17T05:00:40Z", semantic_at="2026-08-17T04:59:58Z")),
        current_clock_provenance=clock_provenance(current, at="2026-08-17T05:00:40Z"),
        contract=contract, policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:40Z"),
    )
    assert sampled["samples"] == []
    assert any(row["reason"] == "quote_stale_at_selection" for row in sampled["attempts"])
    forged = observations(contract, identity, at="2026-08-17T05:00:01Z")
    relevant = next(row for row in forged if row["instrument"] == "EUR_USD")
    relevant["market_attestation_payload_sha256"] = "0" * 64
    current = clock(at="2026-08-17T05:00:01Z", semantic_at="2026-08-17T04:59:58Z")
    rejected = build_samples(
        [plan], forged, existing_samples=[], current_clock_snapshot=current,
        current_clock_provenance=clock_provenance(current, at="2026-08-17T05:00:01Z"),
        contract=contract, policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )
    assert rejected["samples"] == []
    assert any(row["reason"] == "quote_artifact_attestation_binding_drift" for row in rejected["attempts"])


def test_p1_market_attestation_must_bind_name_hash_and_bytes(contract: dict) -> None:
    raw, attestation = quote_artifact(at="2026-08-17T05:00:01Z")
    missing = copy.deepcopy(attestation)
    missing.pop("artifact_name")
    with pytest.raises(ProspectiveEventResponseV5Error, match="missing_fields:artifact_name"):
        quote_snapshot_candidates(raw, artifact_name="practice_quotes_v2.json", market_attestation=missing, contract=contract, collector_observed_utc=stamp("2026-08-17T05:00:01Z"), cohort_identity_sha256="1" * 64)
    mismatch = copy.deepcopy(attestation)
    mismatch["payload_sha256"] = "0" * 64
    with pytest.raises(ProspectiveEventResponseV5Error, match="artifact_binding_mismatch"):
        quote_snapshot_candidates(raw, artifact_name="practice_quotes_v2.json", market_attestation=mismatch, contract=contract, collector_observed_utc=stamp("2026-08-17T05:00:01Z"), cohort_identity_sha256="1" * 64)


def test_p1_cohort_dependency_drift_is_rejected(contract: dict, policy: dict, tmp_path: Path) -> None:
    identity = cohort_identity(policy)
    plan = baseline_plan(plans(contract, policy, identity))
    drifted = cohort_identity(policy, closure_manifest_sha256="9" * 64)
    with pytest.raises(ProspectiveEventResponseV5Error, match="plan_cohort_dependency_drift"):
        build_samples(
            [plan], observations(contract, drifted, at="2026-08-17T05:00:01Z"), existing_samples=[],
            current_clock_snapshot=(current := clock(at="2026-08-17T05:00:01Z", semantic_at="2026-08-17T04:59:58Z")),
            current_clock_provenance=clock_provenance(current, at="2026-08-17T05:00:01Z"),
            contract=contract, policy_dependency_registry=policy, cohort_identity=drifted,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )
    ledger = tmp_path / "identity.sqlite"
    connection = open_temp_ledger(
        ledger, canonical_paths=[], cohort_identity=identity,
        policy_registry_sha256=validate_policy_dependency_registry(policy),
    )
    connection.close()
    with pytest.raises(ProspectiveEventResponseV5Error, match="cohort_identity_drift"):
        open_temp_ledger(
            ledger, canonical_paths=[], cohort_identity=drifted,
            policy_registry_sha256=validate_policy_dependency_registry(policy),
        )


def test_p1_hardlink_alias_cannot_open_canonical_ledger(contract: dict, policy: dict, tmp_path: Path) -> None:
    canonical = tmp_path / "canonical.sqlite"
    canonical.write_bytes(b"canonical")
    alias = tmp_path / "alias.sqlite"
    os.link(canonical, alias)
    with pytest.raises(ProspectiveEventResponseV5Error, match="samefile_forbidden"):
        open_temp_ledger(
            alias, canonical_paths=[canonical], cohort_identity=cohort_identity(policy),
            policy_registry_sha256=validate_policy_dependency_registry(policy),
        )


def test_p2_top_status_is_fail_closed() -> None:
    assert derive_top_status(planning_status="plans_ready", sampling_status="sampling_failed_closed") == "temp_v5_failed_closed"
    assert derive_top_status(planning_status="", sampling_status="samples_captured") == "temp_v5_failed_closed"
    assert derive_top_status(planning_status="plans_ready", sampling_status="samples_captured") == "temp_v5_cycle_ok"


def test_p2_missed_baseline_is_recorded_durably_ready(contract: dict, policy: dict, tmp_path: Path) -> None:
    identity = cohort_identity(policy)
    plan = baseline_plan(plans(contract, policy, identity))
    sampled = build_samples(
        [plan], [], existing_samples=[],
        current_clock_snapshot=(current := clock(at="2026-08-17T05:00:50Z", semantic_at="2026-08-17T04:59:58Z")),
        current_clock_provenance=clock_provenance(current, at="2026-08-17T05:00:50Z"),
        contract=contract, policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:50Z"),
    )
    assert sampled["status"] == "sampling_failed_closed"
    assert any(row["status"] == "baseline_missed_failed_closed" and row["reason"] == "baseline_window_missed" for row in sampled["attempts"])
    identity_sha = validate_cohort_identity(
        identity, policy_registry_sha256=validate_policy_dependency_registry(policy)
    )["cohort_identity_sha256"]
    connection = open_temp_ledger(
        tmp_path / "missed.sqlite", canonical_paths=[], cohort_identity=identity,
        policy_registry_sha256=validate_policy_dependency_registry(policy),
    )
    insert_temp_rows(
        connection, "attempts", sampled["attempts"],
        cohort_identity_sha256=identity_sha,
    )
    connection.commit()
    persisted = read_temp_rows(
        connection, "attempts", cohort_identity_sha256=identity_sha
    )
    connection.close()
    assert any(row["reason"] == "baseline_window_missed" for row in persisted)


def test_p2_planning_semantic_time_is_retained_on_baseline(contract: dict, policy: dict) -> None:
    identity = cohort_identity(policy)
    plan = baseline_plan(plans(contract, policy, identity))
    sampled = build_samples(
        [plan], observations(contract, identity, at="2026-08-17T05:00:01Z"), existing_samples=[],
        current_clock_snapshot=(current := clock(at="2026-08-17T05:00:01Z", semantic_at="2026-08-17T04:59:58Z")),
        current_clock_provenance=clock_provenance(current, at="2026-08-17T05:00:01Z"),
        contract=contract, policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )
    assert len(sampled["samples"]) == 1
    sample = sampled["samples"][0]
    assert sample["planning_semantic_source_generated_utc"] == "2026-08-17T03:59:55Z"
    assert sample["baseline_semantic_source_generated_utc"] == "2026-08-17T04:59:58Z"
    assert sample["planning_semantic_source_generated_utc"] != sample["baseline_semantic_source_generated_utc"]


def test_postbaseline_endpoint_keeps_planning_semantics_without_refresh(contract: dict, policy: dict) -> None:
    identity = cohort_identity(policy)
    built = plans(contract, policy, identity)
    baseline_plan_row = baseline_plan(built)
    endpoint_plan = next(
        row for row in built["plans"]
        if row["instrument"] == "EUR_USD" and row["target_offset_sec"] == 30
    )
    baseline_clock = clock(at="2026-08-17T05:00:01Z", semantic_at="2026-08-17T04:59:58Z")
    baseline_result = build_samples(
        [baseline_plan_row], observations(contract, identity, at="2026-08-17T05:00:01Z"),
        existing_samples=[], current_clock_snapshot=baseline_clock,
        current_clock_provenance=clock_provenance(baseline_clock, at="2026-08-17T05:00:01Z"),
        contract=contract, policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )
    assert len(baseline_result["samples"]) == 1
    stale_semantic_clock = clock(at="2026-08-17T05:00:30Z", semantic_at="2026-08-17T04:00:00Z")
    endpoint_result = build_samples(
        [endpoint_plan], observations(contract, identity, at="2026-08-17T05:00:30Z"),
        existing_samples=baseline_result["samples"], current_clock_snapshot=stale_semantic_clock,
        current_clock_provenance=clock_provenance(stale_semantic_clock, at="2026-08-17T05:00:30Z"),
        contract=contract, policy_dependency_registry=policy, cohort_identity=identity,
        collector_observed_utc=stamp("2026-08-17T05:00:30Z"),
    )
    assert len(endpoint_result["samples"]) == 1
    assert endpoint_result["samples"][0]["planning_semantic_source_generated_utc"] == "2026-08-17T03:59:55Z"
    assert endpoint_result["samples"][0]["event_lineage_state"] == "postbaseline_frozen_exact_full_event_payload"


def test_temp_runner_executes_complete_pending_bundle_without_canonical_writes(contract: dict, policy: dict, tmp_path: Path) -> None:
    identity = bundle_identity(policy)
    source_clock = clock(at="2026-08-17T04:00:00Z", semantic_at="2026-08-17T03:59:55Z")
    raw, attestation = quote_artifact(at="2026-08-17T04:00:00Z")
    result = collector.run_once(
        contract_override=contract,
        clock_snapshot_override=source_clock,
        clock_provenance_override=clock_provenance(source_clock, at="2026-08-17T04:00:00Z"),
        quote_artifact_bytes_override=raw,
        quote_artifact_name_override="practice_quotes_v2.json",
        market_attestation_override=attestation,
        policy_dependency_override=policy,
        cohort_identity_override=identity,
        ledger_path=tmp_path / "fixture.sqlite",
        state_path=tmp_path / "state.json",
        report_path=tmp_path / "report.md",
        observed_utc_override=stamp("2026-08-17T04:00:00Z"),
        test_only=True,
    )
    assert result["status"] == "temp_v5_cycle_ok"
    assert result["plans_inserted"] == 68 * 11
    assert result["samples_inserted"] == 0
    assert result["dependency_bundle_state"] == "pending_external_approval"
