from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from forex_system.contracts.currency_state import load_contract, stable_hash  # noqa: E402
from forex_system.features.currency_state_official_context_v3 import (  # noqa: E402
    CONTEXT_CONTRACT_ID,
    FACT_BASIS_ELIGIBILITY_CONTRACT_ID,
    attach_official_fact_context_v3,
    validate_base_currency_state_identity,
    validate_currency_state_official_context_v3_snapshot,
)
from forex_system.ingestion.immutable_event_clock import (  # noqa: E402
    CONTRACT_ID as CLOCK_CONTRACT_ID,
    SCHEMA_VERSION as CLOCK_SCHEMA_VERSION,
)
from forex_system.ingestion.official_fact_adapter_v3 import (  # noqa: E402
    ADAPTER_CONTRACT_ID,
    CLOCK_DEPENDENCY_POLICY,
    OfficialFactAdapterV3Error,
    validate_official_fact_v3_snapshot,
    verify_canonical_clock_v1_retirement,
)
from test_oanda_currency_state_official_context import base_snapshot  # noqa: E402


def _brand_official(snapshot: dict) -> None:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = "official_fact_v3_snapshot_" + stable_hash(material)[:24]


def _brand_context(snapshot: dict) -> None:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = "currency_state_official_v3_" + stable_hash(material)[:24]


def _source_health() -> dict:
    return {
        "state": "fixture",
        "as_of_utc": None,
        "configured_source_count": 0,
        "operational_source_count": 0,
        "healthy_direct_source_count": 0,
        "degraded_recent_direct_source_count": 0,
        "official_source_issues": [],
    }


def _valid_official(cutoff: str) -> dict:
    currencies = load_contract()["currencies"]
    fact = {
        "fact_id": "macro:AUD:fixture",
        "currency": "AUD",
        "fact_type": "official_macro_actual",
        "series_id": "fixture_cpi",
        "consensus_value": None,
        "consensus_causal": False,
        "published_at_utc": "2026-01-01T00:30:00+00:00",
        "first_seen_at_utc": "2026-01-01T00:30:01+00:00",
        "retrieved_at_utc": "2026-01-01T00:30:01+00:00",
        "effective_from_utc": "2026-01-01T00:30:01+00:00",
        "source_id": "official-fixture",
        "source_name": "Official Fixture",
        "source_contract_id": "official_fixture_v1",
        "source_cohort_id": "official_fixture_v1.discovery",
        "raw_payload_sha256": "a" * 64,
        "evidence_class": "prospective_causal",
        "degradation_reasons": ["no_causal_pre_release_consensus"],
        "direction_policy": "abstain",
        "event_name": "Fixture CPI",
        "release_key": "fixture-cpi-2026-01",
        "reference_period": "January 2026",
        "reference_date": "2026-01-01",
        "unit": "percent",
        "importance": "high",
        "actual_value": 2.0,
        "previous_value": 1.9,
        "revised_previous_value": None,
        "noncausal_consensus_value": None,
        "surprise_raw": None,
        "standardized_surprise": None,
        "noncanonical_ledger_standardized_surprise": None,
        "scheduled_utc": "2026-01-01T00:30:00+00:00",
        "ledger_recorded_at_utc": "2026-01-01T00:30:01+00:00",
        "source_event_id": "fixture-event",
        "source_url": "https://example.invalid/official",
        "collector_contract_id": "fixture_collector_v1",
        "collector_cohort_id": "fixture_collector_v1.discovery",
    }
    evidence = {}
    for currency in currencies:
        has_fact = currency == "AUD"
        evidence[currency] = {
            "currency": currency,
            "fact_count": int(has_fact),
            "macro_fact_record_count": int(has_fact),
            "distinct_macro_observation_count": int(has_fact),
            "fact_types": ["official_macro_actual"] if has_fact else [],
            "causal_consensus_count": 0,
            "upcoming_event_count": 0,
            "source_health": _source_health(),
            "missing_or_degraded": ["no_causal_pre_release_consensus"],
        }
    snapshot = {
        "schema_version": 3,
        "adapter_contract_id": ADAPTER_CONTRACT_ID,
        "parent_adapter_contract_id": "official_fact_adapter_v2_20260817",
        "snapshot_id": "",
        "decision_cutoff_utc": cutoff,
        "currency_count": 21,
        "fact_count": 1,
        "fact_count_semantics": "normalized_provenance_records_not_independent_events",
        "macro_fact_record_count": 1,
        "distinct_macro_observation_count": 1,
        "upcoming_event_count": 0,
        "causal_consensus_count": 0,
        "facts": [fact],
        "upcoming_events": [],
        "event_clock_provenance": {
            "state": "immutable_v2_stale_at_cutoff",
            "schema_version": CLOCK_SCHEMA_VERSION,
            "contract_id": CLOCK_CONTRACT_ID,
            "snapshot_id": "event_clock_snapshot_" + "b" * 32,
            "snapshot_captured_utc": "2026-01-01T00:40:00+00:00",
            "clock_observation_id": "event_clock_observation_" + "c" * 32,
            "clock_observed_utc": "2026-01-01T00:40:00+00:00",
            "source_generated_utc": "2026-01-01T00:39:00+00:00",
            "events_sha256": "d" * 64,
            "manifest_sha256": "e" * 64,
            "clock_attestation": {
                "state": "fresh_trusted",
                "attested": True,
                "artifact_name": "clock_integrity_v1.json",
                "generated_utc": "2026-01-01T00:39:59+00:00",
                "age_at_capture_sec": 1.0,
                "payload_sha256": "f" * 64,
                "maximum_age_sec": 90.0,
                "status": "ok",
                "timestamp_normalization_trusted": True,
                "host_clock_synchronized": True,
                "source_fresh": True,
            },
            "clock_ready_for_cutoff": False,
            "observation_age_sec": 1200.0,
            "maximum_observation_age_sec": 300.0,
            "fallback_used": False,
            "mutable_fallback_forbidden": True,
            "complete_snapshot_at_cutoff": False,
            "degradation_reason": "latest_attested_clock_observation_stale",
        },
        "currency_evidence": evidence,
        "global_gaps": [{"code": "no_causal_pre_release_consensus"}],
        "clock": {
            "state": "not_known_at_cutoff",
            "trusted_for_prospective_evidence": False,
        },
        "intraday_rates": {
            "state": "source_not_connected",
            "connected": False,
            "as_of_utc": None,
            "contract": {
                "required_fields": ["currency", "observed_utc"],
                "causal_rule": "known before cutoff",
                "no_data_policy": "missing is unavailable",
                "material_change_policy": "new cohort",
            },
            "blocker": "not connected",
        },
        "quarantined_source_event_count": 0,
        "status": "degraded",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
        "immutable_event_clock_schema_version": CLOCK_SCHEMA_VERSION,
        "immutable_event_clock_contract_id": CLOCK_CONTRACT_ID,
        "clock_dependency_policy": CLOCK_DEPENDENCY_POLICY,
        "maximum_clock_observation_age_sec": 300.0,
        "can_promote": False,
        "can_authorize": False,
    }
    _brand_official(snapshot)
    return snapshot


def test_v3_valid_fixture_and_context_are_strictly_unscored() -> None:
    contract, base = base_snapshot()
    official = _valid_official(base["decision_cutoff_utc"])
    validate_official_fact_v3_snapshot(official)
    combined = attach_official_fact_context_v3(base, official, contract=contract)
    validate_currency_state_official_context_v3_snapshot(combined, contract=contract)
    assert combined["component_context_contract_id"] == CONTEXT_CONTRACT_ID
    assert combined["component_context"]["fact_basis_eligibility_contract_id"] == FACT_BASIS_ELIGIBILITY_CONTRACT_ID
    assert combined["execution_eligible"] is False
    assert combined["can_place_orders"] is False
    assert combined["can_promote"] is False
    assert combined["can_authorize"] is False
    assert combined["supported_execution_decision"] == "no_trade"
    for horizon in combined["horizons"].values():
        for edge in horizon["pair_edges"].values():
            assert edge["forecast_mean_bps"] is None
            assert edge["expected_net_pips"] is None
            assert edge["allocator_rank"] is None
            assert edge["execution_eligible"] is False


@pytest.mark.parametrize("container", [{"trade_direction": "buy"}, ["buy"], ("buy",)])
@pytest.mark.parametrize("rebrand", [False, True], ids=("no_rebrand", "rebrand"))
@pytest.mark.parametrize(
    "surface",
    [
        ("currency_evidence", "source_health", "state"),
        ("global_gaps", 0, "code"),
        ("event_clock_provenance", "state"),
        ("event_clock_provenance", "clock_attestation", "status"),
    ],
)
def test_official_v3_rejects_container_in_every_scalar_surface(
    surface: tuple, rebrand: bool, container: object
) -> None:
    _, base = base_snapshot()
    snapshot = _valid_official(base["decision_cutoff_utc"])
    if surface[0] == "currency_evidence":
        target = snapshot["currency_evidence"]["AUD"]
        path = surface[1:]
    else:
        target = snapshot
        path = surface
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = container
    if rebrand:
        _brand_official(snapshot)
    with pytest.raises(OfficialFactAdapterV3Error, match="typed_schema"):
        validate_official_fact_v3_snapshot(snapshot)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda x: x.__setitem__("fact_count", True),
        lambda x: x["facts"][0].__setitem__("consensus_causal", 0),
        lambda x: x["facts"][0].__setitem__("actual_value", True),
        lambda x: x["currency_evidence"]["AUD"]["source_health"].__setitem__("configured_source_count", False),
        lambda x: x["event_clock_provenance"].__setitem__("clock_ready_for_cutoff", 0),
        lambda x: x.__setitem__("execution_eligible", 0),
    ],
)
@pytest.mark.parametrize("rebrand", [False, True], ids=("no_rebrand", "rebrand"))
def test_official_v3_rejects_bool_int_equivocation(mutator, rebrand: bool) -> None:
    _, base = base_snapshot()
    snapshot = _valid_official(base["decision_cutoff_utc"])
    mutator(snapshot)
    if rebrand:
        _brand_official(snapshot)
    with pytest.raises(OfficialFactAdapterV3Error, match="typed_schema"):
        validate_official_fact_v3_snapshot(snapshot)


@pytest.mark.parametrize("invalid", [math.nan, math.inf, -math.inf])
def test_official_v3_rejects_nonfinite_numbers_even_when_rebranded(invalid: float) -> None:
    _, base = base_snapshot()
    snapshot = _valid_official(base["decision_cutoff_utc"])
    snapshot["facts"][0]["actual_value"] = invalid
    snapshot["snapshot_id"] = "official_fact_v3_snapshot_rebranded"
    with pytest.raises(OfficialFactAdapterV3Error, match="finite_number"):
        validate_official_fact_v3_snapshot(snapshot)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("clock_observation_id", "observation"),
        ("snapshot_id", "snapshot"),
        ("events_sha256", "not-a-hash"),
        ("clock_observed_utc", "2026-01-01T00:40:00Z"),
    ],
)
def test_ready_provenance_requires_exact_identity_timestamp_and_hash(
    field: str, bad: object
) -> None:
    _, base = base_snapshot()
    snapshot = _valid_official(base["decision_cutoff_utc"])
    provenance = snapshot["event_clock_provenance"]
    provenance["state"] = "immutable_v2_fresh_attested_at_cutoff"
    provenance["clock_ready_for_cutoff"] = True
    provenance["complete_snapshot_at_cutoff"] = True
    provenance[field] = bad
    provenance.pop("degradation_reason", None)
    provenance["observation_age_sec"] = 1.0
    _brand_official(snapshot)
    with pytest.raises(OfficialFactAdapterV3Error, match="typed_schema"):
        validate_official_fact_v3_snapshot(snapshot)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda x: x.__setitem__("fact_count", 2),
        lambda x: x["currency_evidence"]["AUD"].__setitem__("fact_count", 0),
        lambda x: x["currency_evidence"]["AUD"].__setitem__("fact_types", []),
        lambda x: x.__setitem__("causal_consensus_count", 1),
        lambda x: x["currency_evidence"]["AUD"].__setitem__("distinct_macro_observation_count", 0),
    ],
)
def test_official_v3_reconciles_all_aggregate_and_nested_counts(mutator) -> None:
    _, base = base_snapshot()
    snapshot = _valid_official(base["decision_cutoff_utc"])
    mutator(snapshot)
    _brand_official(snapshot)
    with pytest.raises(OfficialFactAdapterV3Error):
        validate_official_fact_v3_snapshot(snapshot)


def test_base_identity_is_independently_checked_before_copy() -> None:
    contract, base = base_snapshot()
    validate_base_currency_state_identity(base, contract=contract)
    tampered = copy.deepcopy(base)
    tampered["horizons"]["300"]["currencies"]["AUD"]["observed_currency_return_bps"] += 0.01
    with pytest.raises(ValueError, match="(snapshot identity mismatch|base-minus-quote algebra)"):
        validate_base_currency_state_identity(tampered, contract=contract)
    with pytest.raises(ValueError, match="(snapshot identity mismatch|base-minus-quote algebra)"):
        attach_official_fact_context_v3(
            tampered, _valid_official(base["decision_cutoff_utc"]), contract=contract
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda x: x.__setitem__("status", {"trade_side": "buy"}),
        lambda x: x["component_context"]["currency_summary"]["AUD"].__setitem__("fact_count", True),
        lambda x: x["horizons"]["300"]["currencies"]["AUD"]["components"][0].__setitem__("pair_observation_count", False),
        lambda x: x["horizons"]["300"]["pair_edges"]["AUD_CAD"].__setitem__("spread_bps", math.inf),
        lambda x: x["component_context"]["event_clock_provenance"].__setitem__("state", ["buy"]),
    ],
)
@pytest.mark.parametrize("rebrand", [False, True], ids=("no_rebrand", "rebrand"))
def test_context_v3_rejects_rebranded_container_bool_and_nonfinite_aliases(
    mutator, rebrand: bool
) -> None:
    contract, base = base_snapshot()
    combined = attach_official_fact_context_v3(
        base, _valid_official(base["decision_cutoff_utc"]), contract=contract
    )
    mutator(combined)
    if rebrand:
        try:
            _brand_context(combined)
        except ValueError:
            pass
    with pytest.raises(ValueError):
        validate_currency_state_official_context_v3_snapshot(combined, contract=contract)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda x: x.__setitem__("decision_cutoff_utc", "2026-01-01T01:01:01+00:00"),
        lambda x: x.__setitem__("status", "clock_v2_official_fact_v3_context_attached_unscored_changed"),
        lambda x: x["component_context"]["currency_summary"]["AUD"].__setitem__("missing_or_degraded", []),
        lambda x: x["horizons"]["300"]["currencies"]["AUD"]["components"][0].__setitem__("reason", "changed"),
    ],
)
def test_context_snapshot_id_covers_cutoff_status_horizons_summaries_components(
    mutator,
) -> None:
    contract, base = base_snapshot()
    combined = attach_official_fact_context_v3(
        base, _valid_official(base["decision_cutoff_utc"]), contract=contract
    )
    mutator(combined)
    with pytest.raises(ValueError):
        validate_currency_state_official_context_v3_snapshot(combined, contract=contract)


def _sqlite_identity(path: Path) -> tuple[int, str, str]:
    size = path.stat().st_size
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        connection.close()
    return size, digest, quick_check


def test_canonical_clock_and_response_evidence_is_preserved_byte_exact() -> None:
    ledgers = ROOT / "data" / "oanda_training_manager" / "research_ledgers"
    paths = [
        ledgers / "immutable_event_clock_v1.sqlite",
        ledgers / "immutable_event_clock_v2.sqlite",
        ledgers / "prospective_event_response_v2.sqlite",
    ]
    before = {path: _sqlite_identity(path) for path in paths}
    response_artifacts = [
        ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v2_frozen_helpers.py",
        ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v2_tombstone.py",
        ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v3.py",
        ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v4.py",
        ROOT / "config" / "prospective_event_response_capture_v2_retirement.json",
        ROOT / "config" / "prospective_event_response_capture_v3.json",
        ROOT / "config" / "prospective_event_response_capture_v4.json",
    ]
    response_before = {
        path: (path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in response_artifacts
    }
    assert all(identity[2] == "ok" for identity in before.values())
    verification = verify_canonical_clock_v1_retirement()
    assert verification["quick_check"] == "ok"
    contract, base = base_snapshot()
    combined = attach_official_fact_context_v3(
        base, _valid_official(base["decision_cutoff_utc"]), contract=contract
    )
    validate_currency_state_official_context_v3_snapshot(combined, contract=contract)
    after = {path: _sqlite_identity(path) for path in paths}
    assert after == before
    assert response_before == {
        path: (path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in response_artifacts
    }
    assert not (ledgers / "prospective_event_response_v3.sqlite").exists()
    assert not (ledgers / "prospective_event_response_v4.sqlite").exists()


def test_v3_modules_expose_no_registration_authorization_or_execution_api() -> None:
    import forex_system.features.currency_state_official_context_v3 as context_v3
    import forex_system.ingestion.official_fact_adapter_v3 as adapter_v3

    forbidden = ("register", "authorize", "promote", "order", "execute", "initialize")
    for module in (adapter_v3, context_v3):
        assert not any(any(token in name.lower() for token in forbidden) for name in module.__all__)


def test_v3_contracts_and_manifests_are_disabled_and_byte_exact() -> None:
    contract_paths = (
        ROOT / "config" / "official_fact_adapter_contract_v3.json",
        ROOT / "config" / "currency_state_official_context_contract_v3.json",
    )
    for path in contract_paths:
        contract = json.loads(path.read_text(encoding="utf-8"))
        assert contract["state"] == {
            "enabled": False,
            "registered": False,
            "independent_review_state": "pending",
            "canonical_output_initialized": False,
        }
        assert contract["research_only"] is True
        assert contract["execution_eligible"] is False
        assert contract["can_place_orders"] is False
        assert contract["can_promote"] is False
        assert contract["can_authorize"] is False
        assert contract["supported_execution_decision"] == "no_trade"
    manifest_paths = (
        ROOT / "config" / "official_fact_adapter_v3_manifest.json",
        ROOT / "config" / "currency_state_official_context_v3_manifest.json",
    )
    for path in manifest_paths:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        assert manifest["state"] == {
            "enabled": False,
            "registered": False,
            "independent_review_state": "pending",
            "canonical_output_initialized": False,
        }
        assert manifest["policy"]["research_only"] is True
        assert manifest["policy"]["execution_eligible"] is False
        assert manifest["policy"]["can_place_orders"] is False
        for artifact in manifest["artifacts"].values():
            artifact_path = ROOT / artifact["relative_path"]
            assert artifact_path.is_file()
            assert artifact_path.stat().st_size == artifact["byte_length"]
            assert hashlib.sha256(artifact_path.read_bytes()).hexdigest() == artifact["sha256"]
