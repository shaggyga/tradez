from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from forex_system.ingestion.prospective_event_response_v4 import (  # noqa: E402
    CONTRACT_ID,
    ProspectiveEventResponseV4Error,
    build_event_plans,
    build_samples,
    load_contract,
    make_temp_test_contract,
    open_temp_ledger,
    quote_snapshot_candidates,
    validate_host_clock_freshness,
    validate_semantic_catalog_freshness,
    verify_helper_closure,
)
import oanda_prospective_event_response_v4 as collector  # noqa: E402
import oanda_prospective_event_response_worker_v4 as worker  # noqa: E402


UTC = dt.timezone.utc
CONTRACT_PATH = ROOT / "config" / "prospective_event_response_capture_v4.json"
DEPENDENCY_PATH = ROOT / "config" / "currency_policy_dependency_registry_v1.json"


def stamp(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@pytest.fixture
def candidate() -> dict:
    return load_contract(CONTRACT_PATH)


@pytest.fixture
def contract(candidate: dict) -> dict:
    return make_temp_test_contract(candidate, cohort_start_utc=stamp("2026-08-17T03:50:00Z"))


@pytest.fixture
def dependencies() -> dict:
    return json.loads(DEPENDENCY_PATH.read_text(encoding="utf-8"))


def event(**overrides: object) -> dict:
    row = {
        "event_version_id": "clock_version_one",
        "upstream_event_id": "official-fomc-release",
        "scheduled_utc": "2026-08-17T05:00:00Z",
        "ledger_effective_known_utc": "2026-08-17T03:59:00Z",
        "original_fact_known_utc": "2026-08-17T03:45:00Z",
        "event_snapshot_first_observed_utc": "2026-08-17T03:58:00Z",
        "event_snapshot_first_trusted_observed_utc": "2026-08-17T03:59:00Z",
        "source_id": "fomc_policy_calendar",
        "source_contract_id": "fomc_policy_calendar_v1",
        "source_cohort_id": "fomc_policy_calendar_v1",
        "source_contract_first_observed_utc": "2026-08-17T03:58:00Z",
        "source_contract_first_trusted_observed_utc": "2026-08-17T03:59:00Z",
        "event_availability_utc": "2026-08-17T03:59:00Z",
        "material_content_sha256": "f" * 64,
        "timing_precision": "minute",
        "clock_semantics": "domestic_official_policy_release",
        "independent_domestic_event": True,
        "linked_policy_factor": False,
        "headline": "FOMC monetary policy decision",
        "category": "monetary_policy",
        "source_name": "Federal Reserve",
        "source_url": "https://federalreserve.gov/example",
        "source_verified": True,
        "direct_currencies": ["USD"],
        "currencies": ["USD"],
        "event_time_basis": "scheduled_release",
        "schedule_window_end_utc": "",
    }
    row.update(overrides)
    return row


def clock(
    *,
    at: str,
    host_age_sec: float = 0,
    semantic_age_sec: float = 5,
    events: list[dict] | None = None,
) -> dict:
    now = stamp(at)
    observed = now - dt.timedelta(seconds=host_age_sec)
    source_generated = now - dt.timedelta(seconds=semantic_age_sec)
    return {
        "contract_id": "immutable_point_in_time_event_clock_v2_20260817",
        "source_pipeline_version": "all_pair_news_event_tags_v3",
        "snapshot_id": "clock_snapshot_v4_fixture",
        "snapshot_captured_utc": observed.isoformat(),
        "clock_observation_id": "clock_observation_" + at.replace(":", ""),
        "clock_observed_utc": observed.isoformat(),
        "clock_observation_source_generated_utc": source_generated.isoformat(),
        "semantic_snapshot_events_sha256": "a" * 64,
        "semantic_snapshot_manifest_sha256": "b" * 64,
        "clock_attestation": {
            "state": "fresh_trusted",
            "attested": True,
            "artifact_name": "clock_integrity_v1.json",
            "generated_utc": observed.isoformat(),
            "payload_sha256": "e" * 64,
        },
        "events": [event()] if events is None else events,
    }


def quote_payload(*, at: str, instrument: str = "EUR_USD", bid: float = 1.1, ask: float = 1.1002, attestation_age_sec: float = 1) -> dict:
    now = stamp(at)
    return {
        "schema_version": 2,
        "generated_utc": now.isoformat(),
        "quotes": {
            instrument: {
                "bid": bid,
                "ask": ask,
                "pip": 0.0001,
                "source": "stream",
                "time": (now - dt.timedelta(milliseconds=500)).isoformat(),
            }
        },
        "market_clock_attestation": {
            "state": "fresh_trusted",
            "attested": True,
            "artifact_name": "clock_integrity_v1.json",
            "generated_utc": (now - dt.timedelta(seconds=attestation_age_sec)).isoformat(),
            "payload_sha256": "d" * 64,
        },
    }


def observations(payload: dict, contract: dict, at: str) -> list[dict]:
    body = {key: value for key, value in payload.items() if key != "market_clock_attestation"}
    return quote_snapshot_candidates(
        body,
        contract=contract,
        artifact_sha256=hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        collector_observed_utc=stamp(at),
        market_clock_attestation=payload["market_clock_attestation"],
    )


def build(contract: dict, dependencies: dict) -> dict:
    return build_event_plans(
        clock(at="2026-08-17T04:00:00Z", semantic_age_sec=240),
        contract=contract,
        instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
        policy_dependency_registry=dependencies,
        planned_utc=stamp("2026-08-17T04:00:00Z"),
    )


def plan(built: dict, instrument: str, offset: int) -> dict:
    return next(
        row
        for row in built["plans"]
        if row["instrument"] == instrument and row["target_offset_sec"] == offset
    )


def helper_manifest(path: Path, *, reviewed: str | None = None, cohort_start: str | None = None) -> Path:
    required = {
        "src/forex_system/ingestion/prospective_event_response_v4.py": "candidate_runtime_core",
        "oanda_prospective_event_response_v4.py": "temp_only_collector",
        "oanda_prospective_event_response_worker_v4.py": "temp_only_worker",
        "src/forex_system/ingestion/immutable_event_clock.py": "upstream_clock_implementation",
        "src/forex_system/ingestion/official_fact_adapter.py": "upstream_official_fact_v1_implementation",
        "src/forex_system/ingestion/official_fact_adapter_v2.py": "actual_official_fact_v2_implementation",
        "src/forex_system/features/currency_state_official_context.py": "upstream_currency_context_v1_implementation",
        "src/forex_system/features/currency_state_official_context_v2.py": "actual_currency_context_v2_implementation",
        "src/forex_system/contracts/currency_state.py": "upstream_currency_state_contract",
        "src/forex_system/ingestion/prospective_event_response_v2_frozen_helpers.py": "sqlite_free_selector_loader",
        "src/forex_system/ingestion/prospective_event_response_v2_tombstone.py": "transitive_exception_only_not_implementation_identity",
        "artifacts/retired_contracts/prospective_event_response_v2_08bfa4b0bb213cae.py.frozen": "executed_v2_implementation_identity",
        "config/prospective_event_response_capture_v4.json": "candidate_contract",
    }
    candidate_frozen = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))[
        "candidate_bytes_frozen_utc"
    ]
    manifest = {
        "contract_id": CONTRACT_ID,
        "candidate_bytes_frozen_utc": candidate_frozen,
        "independent_reviewed_utc": reviewed,
        "cohort_start_utc": cohort_start,
        "review_state": "independently_approved" if reviewed else "pending_independent_review",
        "helpers": [
            {
                "path": relative,
                "role": role,
                "sha256": hashlib.sha256((ROOT / relative).read_bytes()).hexdigest(),
            }
            for relative, role in required.items()
        ],
    }
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def test_candidate_is_new_disabled_unregistered_and_has_no_start(candidate: dict) -> None:
    assert candidate["contract_id"] == CONTRACT_ID
    assert candidate["parent_disposition"] == "zero_evidence_rejected_no_wire"
    assert candidate["enabled"] is False
    assert candidate["registered"] is False
    assert candidate["canonical_initialization_allowed"] is False
    assert stamp(candidate["candidate_bytes_frozen_utc"])
    assert candidate["cohort_start_utc"] is None
    assert candidate["independent_reviewed_utc"] is None
    with pytest.raises(ProspectiveEventResponseV4Error, match="pending_independent_review"):
        load_contract(CONTRACT_PATH, allow_pending_review=False)


def test_host_clock_89_seconds_passes_and_91_fails(contract: dict) -> None:
    accepted = validate_host_clock_freshness(
        clock(at="2026-08-17T04:00:00Z", host_age_sec=89),
        at_utc=stamp("2026-08-17T04:00:00Z"),
        contract=contract,
    )
    assert accepted["host_clock_observation_age_sec"] == 89
    with pytest.raises(ProspectiveEventResponseV4Error, match="stale"):
        validate_host_clock_freshness(
            clock(at="2026-08-17T04:00:00Z", host_age_sec=91),
            at_utc=stamp("2026-08-17T04:00:00Z"),
            contract=contract,
        )


def test_semantic_catalog_240_seconds_passes_but_301_fails(contract: dict) -> None:
    accepted = validate_semantic_catalog_freshness(
        clock(at="2026-08-17T04:00:00Z", semantic_age_sec=240),
        at_utc=stamp("2026-08-17T04:00:00Z"),
        contract=contract,
        phase="planning",
    )
    assert accepted["semantic_catalog_age_sec"] == 240
    with pytest.raises(ProspectiveEventResponseV4Error, match="stale_at_planning"):
        validate_semantic_catalog_freshness(
            clock(at="2026-08-17T04:00:00Z", semantic_age_sec=301),
            at_utc=stamp("2026-08-17T04:00:00Z"),
            contract=contract,
            phase="planning",
        )


def test_planning_uses_separate_catalog_sla_and_preserves_source_time(
    contract: dict, dependencies: dict
) -> None:
    built = build(contract, dependencies)
    assert built["status"] == "plans_ready"
    assert built["plan_count"] == 33
    assert built["semantic_catalog"]["semantic_catalog_age_sec"] == 240
    assert {
        row["relationship"] for row in built["plans"] if row["target_offset_sec"] == 0
    } == {"direct_leg", "linked_policy_dependency", "unaffected_control"}
    direct = plan(built, "EUR_USD", 0)
    linked = plan(built, "HKD_JPY", 0)
    assert direct["underlying_factor_id"] == linked["underlying_factor_id"]
    assert direct["semantic_catalog_source_generated_utc"] == "2026-08-17T03:56:00Z"
    assert direct["host_clock_observed_utc"] == "2026-08-17T04:00:00Z"


def test_catalog_gap_310_fails_planning_and_prebaseline(
    contract: dict, dependencies: dict
) -> None:
    with pytest.raises(ProspectiveEventResponseV4Error, match="stale_at_planning"):
        build_event_plans(
            clock(at="2026-08-17T04:00:00Z", semantic_age_sec=310),
            contract=contract,
            instruments=["EUR_USD"],
            policy_dependency_registry=dependencies,
            planned_utc=stamp("2026-08-17T04:00:00Z"),
        )
    baseline_plan = plan(build(contract, dependencies), "EUR_USD", 0)
    with pytest.raises(ProspectiveEventResponseV4Error, match="stale_at_baseline"):
        build_samples(
            [baseline_plan],
            observations(quote_payload(at="2026-08-17T05:00:01Z"), contract, "2026-08-17T05:00:01Z"),
            existing_samples=[],
            current_clock_snapshot=clock(
                at="2026-08-17T05:00:01Z", semantic_age_sec=310
            ),
            contract=contract,
            instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
            policy_dependency_registry=dependencies,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )


def test_postbaseline_495_second_catalog_gap_succeeds_without_refresh(
    contract: dict, dependencies: dict
) -> None:
    built = build(contract, dependencies)
    baseline_plan = plan(built, "EUR_USD", 0)
    baseline = build_samples(
        [baseline_plan],
        observations(quote_payload(at="2026-08-17T05:00:01Z"), contract, "2026-08-17T05:00:01Z"),
        existing_samples=[],
        current_clock_snapshot=clock(
            at="2026-08-17T05:00:01Z", semantic_age_sec=240
        ),
        contract=contract,
        instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
        policy_dependency_registry=dependencies,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )["samples"][0]
    endpoint_plan = plan(built, "EUR_USD", 300)
    captured = build_samples(
        [endpoint_plan],
        observations(
            quote_payload(at="2026-08-17T05:05:01Z", bid=1.1005, ask=1.1007),
            contract,
            "2026-08-17T05:05:01Z",
        ),
        existing_samples=[baseline],
        current_clock_snapshot=clock(
            at="2026-08-17T05:05:01Z", semantic_age_sec=495, events=[]
        ),
        contract=contract,
        instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
        policy_dependency_registry=dependencies,
        collector_observed_utc=stamp("2026-08-17T05:05:01Z"),
    )
    assert captured["sample_count"] == 1
    sample = captured["samples"][0]
    assert sample["event_lineage_state"] == "postbaseline_frozen_exact_event_lineage"
    assert sample["semantic_refresh_evaluated"] is False
    assert sample["semantic_catalog_age_sec_at_sample"] == 495
    assert sample["semantic_catalog_source_generated_utc"] == "2026-08-17T04:56:46Z"
    assert sample["host_clock_observed_utc"] == "2026-08-17T05:05:01Z"


def test_event_revision_prebaseline_fails_but_postbaseline_does_not(
    contract: dict, dependencies: dict
) -> None:
    built = build(contract, dependencies)
    baseline_plan = plan(built, "EUR_USD", 0)
    rejected = build_samples(
        [baseline_plan],
        observations(quote_payload(at="2026-08-17T05:00:01Z"), contract, "2026-08-17T05:00:01Z"),
        existing_samples=[],
        current_clock_snapshot=clock(
            at="2026-08-17T05:00:01Z", semantic_age_sec=240, events=[]
        ),
        contract=contract,
        instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
        policy_dependency_registry=dependencies,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )
    assert rejected["sample_count"] == 0
    assert rejected["attempts"][0]["reason"] == "event_revised_or_removed_before_baseline"

    accepted_baseline = build_samples(
        [baseline_plan],
        observations(quote_payload(at="2026-08-17T05:00:01Z"), contract, "2026-08-17T05:00:01Z"),
        existing_samples=[],
        current_clock_snapshot=clock(
            at="2026-08-17T05:00:01Z", semantic_age_sec=240
        ),
        contract=contract,
        instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
        policy_dependency_registry=dependencies,
        collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
    )["samples"][0]
    endpoint = build_samples(
        [plan(built, "EUR_USD", 300)],
        observations(quote_payload(at="2026-08-17T05:05:01Z"), contract, "2026-08-17T05:05:01Z"),
        existing_samples=[accepted_baseline],
        current_clock_snapshot=clock(
            at="2026-08-17T05:05:01Z", semantic_age_sec=495, events=[]
        ),
        contract=contract,
            instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
        policy_dependency_registry=dependencies,
        collector_observed_utc=stamp("2026-08-17T05:05:01Z"),
    )
    assert endpoint["sample_count"] == 1


@pytest.mark.parametrize(
    "override",
    [
        {"event_availability_utc": "2026-08-17T03:58:00Z"},
        {
            "event_availability_utc": "2026-08-17T04:00:01Z",
            "event_snapshot_first_trusted_observed_utc": "2026-08-17T04:00:01Z",
            "source_contract_first_trusted_observed_utc": "2026-08-17T04:00:01Z",
        },
    ],
)
def test_event_clock_backdating_or_future_knowledge_fails_closed(
    contract: dict, dependencies: dict, override: dict
) -> None:
    built = build_event_plans(
        clock(
            at="2026-08-17T04:00:00Z",
            semantic_age_sec=240,
            events=[event(**override)],
        ),
        contract=contract,
        instruments=["EUR_USD"],
        policy_dependency_registry=dependencies,
        planned_utc=stamp("2026-08-17T04:00:00Z"),
    )
    assert built["plan_count"] == 0
    assert "backdating" in built["attempts"][0]["reason"] or "not_known" in built["attempts"][0]["reason"]


def test_dependency_and_universe_tampering_are_detected(
    contract: dict, dependencies: dict
) -> None:
    built = build(contract, dependencies)
    baseline_plan = plan(built, "EUR_USD", 0)
    tampered = copy.deepcopy(dependencies)
    tampered["dependencies"][0]["mechanism"] = "changed_after_plan"
    with pytest.raises(ProspectiveEventResponseV4Error, match="dependency_tampering"):
        build_samples(
            [baseline_plan],
            observations(quote_payload(at="2026-08-17T05:00:01Z"), contract, "2026-08-17T05:00:01Z"),
            existing_samples=[],
            current_clock_snapshot=clock(
                at="2026-08-17T05:00:01Z", semantic_age_sec=240
            ),
            contract=contract,
            instruments=["AUD_NZD", "EUR_USD", "HKD_JPY"],
            policy_dependency_registry=tampered,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )
    with pytest.raises(ProspectiveEventResponseV4Error, match="universe_tampering"):
        build_samples(
            [baseline_plan],
            [],
            existing_samples=[],
            current_clock_snapshot=clock(
                at="2026-08-17T05:00:01Z", semantic_age_sec=240
            ),
            contract=contract,
            instruments=["EUR_USD"],
            policy_dependency_registry=dependencies,
            collector_observed_utc=stamp("2026-08-17T05:00:01Z"),
        )


def test_quote_attestation_89_seconds_passes_and_91_fails(contract: dict) -> None:
    accepted = observations(
        quote_payload(at="2026-08-17T05:00:01Z", attestation_age_sec=89),
        contract,
        "2026-08-17T05:00:01Z",
    )
    assert len(accepted) == 1
    with pytest.raises(ProspectiveEventResponseV4Error, match="stale_or_future"):
        observations(
            quote_payload(at="2026-08-17T05:00:01Z", attestation_age_sec=91),
            contract,
            "2026-08-17T05:00:01Z",
        )


def test_helper_closure_pins_actual_adapter_context_helper_and_archive(tmp_path: Path) -> None:
    path = helper_manifest(tmp_path / "closure.json")
    manifest = verify_helper_closure(ROOT, path, require_reviewed=False)
    paths = {row["path"] for row in manifest["helpers"]}
    assert "src/forex_system/ingestion/official_fact_adapter_v2.py" in paths
    assert "src/forex_system/features/currency_state_official_context_v2.py" in paths
    assert "src/forex_system/ingestion/prospective_event_response_v2_frozen_helpers.py" in paths
    assert "artifacts/retired_contracts/prospective_event_response_v2_08bfa4b0bb213cae.py.frozen" in paths
    assert "src/forex_system/ingestion/prospective_event_response_v3.py" not in paths

    broken = json.loads(path.read_text(encoding="utf-8"))
    broken["helpers"][0]["sha256"] = "0" * 64
    path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(ProspectiveEventResponseV4Error, match="hash_mismatch"):
        verify_helper_closure(ROOT, path, require_reviewed=False)


def test_helper_closure_rejects_role_and_freeze_identity_tampering(tmp_path: Path) -> None:
    path = helper_manifest(tmp_path / "closure.json")
    broken = json.loads(path.read_text(encoding="utf-8"))
    broken["helpers"][0]["role"] = "executed_v2_implementation_identity"
    path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(ProspectiveEventResponseV4Error, match="path_set_mismatch"):
        verify_helper_closure(ROOT, path, require_reviewed=False)

    path = helper_manifest(tmp_path / "freeze.json")
    broken = json.loads(path.read_text(encoding="utf-8"))
    frozen = stamp(broken["candidate_bytes_frozen_utc"])
    broken["candidate_bytes_frozen_utc"] = iso(frozen + dt.timedelta(seconds=1))
    path.write_text(json.dumps(broken), encoding="utf-8")
    with pytest.raises(ProspectiveEventResponseV4Error, match="freeze_mismatch"):
        verify_helper_closure(ROOT, path, require_reviewed=False)


def test_review_and_cohort_timestamps_cannot_backdate_final_bytes(tmp_path: Path) -> None:
    frozen = stamp(
        json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))[
            "candidate_bytes_frozen_utc"
        ]
    )
    before_freeze = helper_manifest(
        tmp_path / "before.json",
        reviewed=iso(frozen - dt.timedelta(seconds=1)),
        cohort_start=iso(frozen + dt.timedelta(seconds=1)),
    )
    with pytest.raises(ProspectiveEventResponseV4Error, match="backdates_final_bytes"):
        verify_helper_closure(ROOT, before_freeze, require_reviewed=True)
    before_review = helper_manifest(
        tmp_path / "start.json",
        reviewed=iso(frozen + dt.timedelta(seconds=1)),
        cohort_start=iso(frozen),
    )
    with pytest.raises(ProspectiveEventResponseV4Error, match="backdates_review"):
        verify_helper_closure(ROOT, before_review, require_reviewed=True)


def test_canonical_ledger_and_unreviewed_run_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ProspectiveEventResponseV4Error, match="canonical_v4_ledger"):
        open_temp_ledger(
            collector.DEFAULT_CANONICAL_LEDGER,
            canonical_path=collector.DEFAULT_CANONICAL_LEDGER,
        )
    with pytest.raises(ProspectiveEventResponseV4Error, match="unregistered"):
        collector.run_once(test_only=False)


def test_temp_only_end_to_end_survives_postbaseline_catalog_gap(
    tmp_path: Path, contract: dict, dependencies: dict
) -> None:
    manifest = helper_manifest(tmp_path / "closure.json")
    ledger = tmp_path / "v4-temp.sqlite"
    state = tmp_path / "state.json"
    report = tmp_path / "report.md"
    universe = ["AUD_NZD", "EUR_USD", "HKD_JPY"]

    planning = collector.run_once(
        helper_manifest_path=manifest,
        contract_override=contract,
        clock_snapshot_override=clock(
            at="2026-08-17T04:00:00Z", semantic_age_sec=240
        ),
        quote_snapshot_override=quote_payload(at="2026-08-17T04:00:00Z"),
        instrument_universe_override=universe,
        policy_dependency_override=dependencies,
        ledger_path=ledger,
        state_path=state,
        report_path=report,
        observed_utc_override=stamp("2026-08-17T04:00:00Z"),
        test_only=True,
    )
    assert planning["status"] == "temp_v4_cycle_ok"
    assert planning["plans_inserted"] == 33

    baseline = collector.run_once(
        helper_manifest_path=manifest,
        contract_override=contract,
        clock_snapshot_override=clock(
            at="2026-08-17T05:00:01Z", semantic_age_sec=240
        ),
        quote_snapshot_override=quote_payload(at="2026-08-17T05:00:01Z"),
        instrument_universe_override=universe,
        policy_dependency_override=dependencies,
        ledger_path=ledger,
        state_path=state,
        report_path=report,
        observed_utc_override=stamp("2026-08-17T05:00:01Z"),
        test_only=True,
    )
    assert baseline["samples_inserted"] == 1

    endpoint = collector.run_once(
        helper_manifest_path=manifest,
        contract_override=contract,
        clock_snapshot_override=clock(
            at="2026-08-17T05:05:01Z", semantic_age_sec=495, events=[]
        ),
        quote_snapshot_override=quote_payload(
            at="2026-08-17T05:05:01Z", bid=1.1005, ask=1.1007
        ),
        instrument_universe_override=universe,
        policy_dependency_override=dependencies,
        ledger_path=ledger,
        state_path=state,
        report_path=report,
        observed_utc_override=stamp("2026-08-17T05:05:01Z"),
        test_only=True,
    )
    assert endpoint["status"] == "temp_v4_cycle_ok"
    assert endpoint["planning_status"] == "new_event_planning_failed_closed"
    assert endpoint["samples_inserted"] == 1
    assert endpoint["outcomes_inserted"] == 1
    assert endpoint["total_outcomes"] == 1
    assert state.is_file() and report.is_file()
    connection = sqlite3.connect(ledger)
    try:
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_worker_passes_all_dependency_overrides_and_keeps_top_level_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinels = {
        "contract_override": {"contract": "sentinel"},
        "clock_snapshot_override": {"clock": "sentinel"},
        "quote_snapshot_override": {"quote": "sentinel"},
        "instrument_universe_override": ["EUR_USD"],
        "policy_dependency_override": {"dependency": "sentinel"},
    }
    observed: dict = {}

    def fake_run_once(**kwargs):
        observed.update(kwargs)
        return {"status": "fake_ok"}

    monkeypatch.setattr(worker.collector, "run_once", fake_run_once)
    result = worker.run_worker(
        **sentinels,
        ledger_path=Path("temp-ledger"),
        state_path=Path("temp-state"),
        report_path=Path("temp-report"),
        test_only=True,
    )[0]
    for key, value in sentinels.items():
        assert observed[key] is value
    assert result["status"] == "fake_ok"
    assert result["worker_status"] == "temp_v4_worker_cycle_ok"


def test_v4_has_no_rejected_v3_or_execution_stack_imports() -> None:
    module_text = (
        ROOT / "src" / "forex_system" / "ingestion" / "prospective_event_response_v4.py"
    ).read_text(encoding="utf-8")
    collector_text = (ROOT / "oanda_prospective_event_response_v4.py").read_text(
        encoding="utf-8"
    )
    assert "prospective_event_response_v3" not in module_text
    assert "oandapyV20" not in module_text + collector_text
    assert "def place_order" not in module_text + collector_text
    assert ".place_order(" not in module_text + collector_text
    assert "close_trade" not in module_text + collector_text
