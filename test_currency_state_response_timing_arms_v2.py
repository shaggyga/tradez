from __future__ import annotations

import ast
import copy
import datetime as dt
import json
from pathlib import Path

import pytest

try:
    import forex_system.research.currency_state_response_timing_arms_v2 as timing_v2
except ModuleNotFoundError:
    import src.forex_system.research.currency_state_response_timing_arms_v2 as timing_v2


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _attestation() -> dict:
    return {
        "contract_id": timing_v2.REQUIRED_V6_R2_CONTRACT_ID,
        "cohort_id": timing_v2.REQUIRED_V6_R2_COHORT_ID,
        "contract_sha256": "1" * 64,
        "manifest_sha256": "2" * 64,
        "review_certificate_id": "synthetic-independent-review-test-fixture",
        "review_certificate_sha256": "3" * 64,
        "review_disposition": "independent_approve_disabled_unregistered_candidate",
        "artifact_closure_verified": True,
        "test_fixture_only": True,
    }


def _response(
    *,
    mode: str = "prospective_capture_test_fixture",
    instrument: str = "EUR_USD",
    currency: str = "EUR",
    horizon: int = 300,
    event_version_id: str = "event-version-1",
    end_bid: float = 1.1010,
    end_ask: float = 1.1012,
    source_binding: str = "f" * 64,
) -> tuple[dict, str]:
    scheduled = dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    start_quote = scheduled + dt.timedelta(seconds=1)
    end_target = scheduled + dt.timedelta(seconds=horizon)
    end_quote = end_target + dt.timedelta(seconds=2)
    row = {
        "response_id": "pending",
        "partition": timing_v2.DIAGNOSTIC_MODES[mode],
        "event_id": "event-1",
        "event_version_id": event_version_id,
        "currency": currency,
        "instrument": instrument,
        "horizon_sec": horizon,
        "event_scheduled_utc": _iso(scheduled),
        "start_target_utc": _iso(scheduled),
        "end_target_utc": _iso(end_target),
        "start_quote_time_utc": _iso(start_quote),
        "end_quote_time_utc": _iso(end_quote),
        "start_known_utc": _iso(start_quote + dt.timedelta(seconds=1)),
        "end_known_utc": _iso(end_quote + dt.timedelta(seconds=1)),
        "start_bid": 1.1000,
        "start_ask": 1.1002,
        "end_bid": end_bid,
        "end_ask": end_ask,
        "pip": 0.0001,
        "start_source_payload_sha256": "a" * 64,
        "end_source_payload_sha256": "b" * 64,
        "start_source_record_id": "start-record",
        "end_source_record_id": "end-record",
        "direction_selected": None,
        "forecast_mean_pips": None,
        "forecast_probability": None,
        "expected_value_pips": None,
        "allocator_rank": None,
        "basis_eligible": False,
        "proof_eligible": False,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "supported_execution_decision": "no_trade",
    }
    row["response_id"] = timing_v2._response_identity(
        row, source_binding_sha256=source_binding, mode=mode
    )
    capture = _iso(end_quote + dt.timedelta(seconds=5))
    return row, capture


def _snapshot(
    *, mode: str = "prospective_capture_test_fixture", responses: list[dict] | None = None
) -> dict:
    if responses is None:
        response, capture = _response(mode=mode)
        responses = [response]
    else:
        _, capture = _response(mode=mode)
    return {
        "snapshot_id": f"synthetic-v6-r2-{mode}",
        "contract_id": timing_v2.REQUIRED_V6_R2_CONTRACT_ID,
        "cohort_id": timing_v2.REQUIRED_V6_R2_COHORT_ID,
        "mode": mode,
        "partition": timing_v2.DIAGNOSTIC_MODES[mode],
        "capture_cutoff_utc": capture,
        "source_binding_sha256": "f" * 64,
        "response_diagnostics": responses,
        "proof_row_count": 0,
        "test_only": True,
        "canonical_output": False,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "can_authorize": False,
        "supported_execution_decision": "no_trade",
    }


def _build(*, mode: str = "prospective_capture_test_fixture", responses=None) -> dict:
    return timing_v2.build_test_diagnostic_snapshot(
        event_response_snapshot=_snapshot(mode=mode, responses=responses),
        dependency_attestation=_attestation(),
    )


def test_production_gate_verifies_r3_bars_r1_and_emits_zero_rows() -> None:
    status = timing_v2.production_dependency_status()
    assert status["approved"] is False
    assert status["blocked"] is True
    assert status["blocking_reasons"] == ["event_response_v6_r2_final_approval_not_bound"]
    assert status["r3"]["verified"] is True
    assert status["barred_predecessor"]["contract_id"] == timing_v2.BARRED_V6_R1_CONTRACT_ID
    assert status["barred_predecessor"]["may_supply_rows"] is False
    assert status["row_count"] == 0
    assert status["proof_row_count"] == 0
    assert status["can_place_orders"] is False
    assert status["can_promote"] is False
    assert status["can_authorize"] is False
    assert status["supported_execution_decision"] == "no_trade"


def test_replay_and_prospective_proof_entrypoints_fail_closed() -> None:
    with pytest.raises(
        timing_v2.ResponseTimingV2DependencyError,
        match="event_response_v6_r2_final_approval_not_bound",
    ):
        timing_v2.build_replay_diagnostic({"caller": "cannot bypass"})
    with pytest.raises(
        timing_v2.ResponseTimingV2DependencyError,
        match="event_response_v6_r2_final_approval_not_bound",
    ):
        timing_v2.build_prospective_proof(allow=True, approved=True)


def test_replay_and_prospective_capture_test_partitions_are_distinct() -> None:
    prospective = _build(mode="prospective_capture_test_fixture")
    replay = _build(mode="replay_diagnostic_test_fixture")
    assert prospective["partition"] == "diagnostic_prospective_capture"
    assert replay["partition"] == "diagnostic_replay"
    assert prospective["snapshot_id"] != replay["snapshot_id"]
    assert prospective["responses"][0]["response_id"] != replay["responses"][0]["response_id"]
    assert prospective["proof_row_count"] == replay["proof_row_count"] == 0


def test_test_fixture_is_deterministic_post_outcome_only_and_no_trade() -> None:
    first = _build()
    second = _build()
    assert first == second
    assert first["response_count"] == 1
    assert first["timing_arm_count"] == 4
    assert first["test_only"] is True
    assert first["canonical_output"] is False
    assert first["proof_row_count"] == 0
    for arm in first["timing_arms"]:
        assert arm["post_outcome_only"] is True
        assert arm["forecast_direction"] is None
        assert arm["forecast_probability"] is None
        assert arm["expected_value_pips"] is None
        assert arm["allocator_rank"] is None
        assert arm["basis_eligible"] is False
        assert arm["proof_eligible"] is False
        assert arm["execution_eligible"] is False
        assert arm["can_place_orders"] is False
        assert arm["can_promote"] is False
        assert arm["can_authorize"] is False
        assert arm["supported_execution_decision"] == "no_trade"


def test_executable_response_and_latency_fields_are_recomputed() -> None:
    result = _build()
    response = result["responses"][0]
    assert response["start_alignment_sec"] == 1.0
    assert response["end_alignment_sec"] == 2.0
    assert response["actual_quote_duration_sec"] == 301.0
    assert response["start_spread_pips"] == pytest.approx(2.0)
    assert response["end_spread_pips"] == pytest.approx(2.0)
    assert response["gross_long_mid_pips"] == pytest.approx(10.0)
    assert response["long_after_executable_spread_pips"] == pytest.approx(8.0)
    assert response["short_after_executable_spread_pips"] == pytest.approx(-12.0)
    observed = next(row for row in result["timing_arms"] if row["arm_id"] == "observed_response")
    assert observed["observed_direction_after_cost"] == "long"
    latency = next(row for row in result["timing_arms"] if row["arm_id"] == "response_latency")
    assert latency["start_known_lag_sec"] == 1.0
    assert latency["end_known_lag_sec"] == 1.0


def test_response_identity_commits_prices_payload_hashes_and_chronology() -> None:
    row, _ = _response()
    changed = copy.deepcopy(row)
    changed["end_bid"] = 1.1020
    changed["end_ask"] = 1.1022
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="response_identity_mismatch"):
        _build(responses=[changed])
    changed["response_id"] = timing_v2._response_identity(
        changed, source_binding_sha256="f" * 64, mode="prospective_capture_test_fixture"
    )
    assert changed["response_id"] != row["response_id"]
    assert _build(responses=[changed])["responses"][0]["end_bid"] == 1.1020


def test_naive_or_nonzero_offset_timestamps_are_rejected() -> None:
    row, _ = _response()
    naive = copy.deepcopy(row)
    naive["start_quote_time_utc"] = "2026-08-17T12:00:01"
    naive["response_id"] = timing_v2._response_identity(
        naive, source_binding_sha256="f" * 64, mode="prospective_capture_test_fixture"
    )
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="zero_utc_offset"):
        _build(responses=[naive])
    offset = copy.deepcopy(row)
    offset["start_quote_time_utc"] = "2026-08-17T08:00:01-04:00"
    offset["response_id"] = timing_v2._response_identity(
        offset, source_binding_sha256="f" * 64, mode="prospective_capture_test_fixture"
    )
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="zero_utc_offset"):
        _build(responses=[offset])


def test_duplicate_natural_cells_are_rejected_even_with_distinct_response_ids() -> None:
    first, _ = _response()
    second, _ = _response(end_bid=1.1020, end_ask=1.1022)
    assert first["response_id"] != second["response_id"]
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="duplicate_response_natural_cell"):
        _build(responses=[first, second])


def test_input_order_does_not_change_snapshot_or_arm_order() -> None:
    first, _ = _response()
    second, _ = _response(
        instrument="GBP_USD",
        currency="GBP",
        horizon=60,
        event_version_id="event-version-2",
    )
    forward = _build(responses=[first, second])
    reverse = _build(responses=[second, first])
    assert forward == reverse


def test_cross_partition_rows_cannot_be_reclassified() -> None:
    row, _ = _response(mode="replay_diagnostic_test_fixture")
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="response_partition_mismatch"):
        _build(mode="prospective_capture_test_fixture", responses=[row])


def test_source_or_attestation_cannot_self_enable_proof() -> None:
    source = _snapshot()
    source["proof_row_count"] = 1
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="proof_rows_forbidden"):
        timing_v2.build_test_diagnostic_snapshot(
            event_response_snapshot=source, dependency_attestation=_attestation()
        )
    attestation = _attestation()
    attestation["review_disposition"] = "self_approved"
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="not_approved"):
        timing_v2.build_test_diagnostic_snapshot(
            event_response_snapshot=_snapshot(), dependency_attestation=attestation
        )


def test_contract_is_inert_and_has_no_canonical_or_worker_surface() -> None:
    contract = json.loads((ROOT / "config" / "currency_state_response_timing_arms_v2.json").read_text())
    assert contract["state"] == {
        "enabled": False,
        "registered": False,
        "research_worker_enabled": False,
        "collector_enabled": False,
        "canonical_output_initialized": False,
        "independent_review_state": "pending",
    }
    assert contract["required_event_response_successor"]["approval_state"] == "unresolved_fail_closed"
    assert contract["required_event_response_successor"]["contract_artifact"] is None
    assert contract["modes"]["prospective_proof"]["enabled"] is False
    for field in (
        "canonical_ledger_path",
        "canonical_state_path",
        "canonical_report_path",
        "worker_entrypoint",
        "collector_entrypoint",
    ):
        assert contract["output_surface"][field] is None


def test_candidate_manifest_pins_exact_inert_closure_without_self_approval() -> None:
    result = timing_v2.verify_candidate_manifest()
    assert result == {
        "manifest_id": "currency_state_response_timing_arms_v2_manifest_20260817_r1",
        "review_state": "pending_independent_review",
        "artifact_count": 10,
        "dependency_gate": "closed",
        "candidate_closure_verified": True,
    }


def test_module_has_no_network_broker_database_write_or_worker_imports() -> None:
    path = ROOT / "src" / "forex_system" / "research" / "currency_state_response_timing_arms_v2.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    forbidden = (
        "requests",
        "httpx",
        "sqlite3",
        "subprocess",
        "oandapy",
        "oandapyV20",
        "authorization",
        "execution",
        "lifecycle",
        "supervisor",
    )
    assert all(not any(name == item or name.startswith(item + ".") for item in forbidden) for name in imported)


def test_validator_rejects_invented_forecast_or_eligibility() -> None:
    result = _build()
    tampered = copy.deepcopy(result)
    tampered["timing_arms"][0]["forecast_direction"] = "buy"
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="invented_forecast"):
        timing_v2.validate_test_diagnostic_snapshot(tampered)
    tampered = copy.deepcopy(result)
    tampered["timing_arms"][0]["proof_eligible"] = True
    with pytest.raises(timing_v2.ResponseTimingV2Error, match="invented_basis_or_proof"):
        timing_v2.validate_test_diagnostic_snapshot(tampered)
