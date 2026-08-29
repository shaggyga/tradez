from __future__ import annotations

import ast
import copy
import datetime as dt
import json
from pathlib import Path

import pytest

try:
    import forex_system.research.currency_state_response_timing_arms_v3 as timing_v3
except ModuleNotFoundError:
    import src.forex_system.research.currency_state_response_timing_arms_v3 as timing_v3


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc
SOURCE_SNAPSHOT_ID = "synthetic-approved-v6-r2-test-snapshot"
SOURCE_BINDING = "f" * 64


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _attestation(
    *, source_snapshot_id: str = SOURCE_SNAPSHOT_ID, source_binding: str = SOURCE_BINDING
) -> dict:
    return {
        "contract_id": timing_v3.REQUIRED_V6_R2_CONTRACT_ID,
        "cohort_id": timing_v3.REQUIRED_V6_R2_COHORT_ID,
        "source_snapshot_id": source_snapshot_id,
        "source_binding_sha256": source_binding,
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
    source_binding: str = SOURCE_BINDING,
) -> tuple[dict, str]:
    scheduled = dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    start_quote = scheduled + dt.timedelta(seconds=1)
    end_target = scheduled + dt.timedelta(seconds=horizon)
    end_quote = end_target + dt.timedelta(seconds=2)
    row = {
        "response_id": "pending",
        "partition": timing_v3.DIAGNOSTIC_MODES[mode],
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
    row["response_id"] = timing_v3._response_identity(
        row, source_binding_sha256=source_binding, mode=mode
    )
    return row, _iso(end_quote + dt.timedelta(seconds=5))


def _snapshot(
    *,
    mode: str = "prospective_capture_test_fixture",
    responses: list[dict] | None = None,
    snapshot_id: str = SOURCE_SNAPSHOT_ID,
    source_binding: str = SOURCE_BINDING,
) -> dict:
    if responses is None:
        response, capture = _response(mode=mode, source_binding=source_binding)
        responses = [response]
    else:
        _, capture = _response(mode=mode, source_binding=source_binding)
    return {
        "snapshot_id": snapshot_id,
        "contract_id": timing_v3.REQUIRED_V6_R2_CONTRACT_ID,
        "cohort_id": timing_v3.REQUIRED_V6_R2_COHORT_ID,
        "mode": mode,
        "partition": timing_v3.DIAGNOSTIC_MODES[mode],
        "capture_cutoff_utc": capture,
        "source_binding_sha256": source_binding,
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
    return timing_v3.build_test_diagnostic_snapshot(
        event_response_snapshot=_snapshot(mode=mode, responses=responses),
        dependency_attestation=_attestation(),
    )


def test_production_gate_recursively_verifies_r3_universe_bars_r1_and_emits_zero() -> None:
    status = timing_v3.production_dependency_status()
    assert status["approved"] is False
    assert status["blocked"] is True
    assert status["blocking_reasons"] == ["event_response_v6_r2_final_approval_not_bound"]
    assert status["r3"]["transitive_closure_verified"] is True
    assert status["r3"]["reviewed_artifact_count"] == 23
    assert status["canonical_universe"] == {
        "contract_id": "currency_state_engine_v2_20260816",
        "currency_count": 21,
        "instrument_count": 68,
        "instrument_universe_sha256": timing_v3.INSTRUMENT_UNIVERSE_SHA256,
        "verified": True,
    }
    assert status["barred_predecessor"]["contract_id"] == timing_v3.BARRED_V6_R1_CONTRACT_ID
    assert status["barred_predecessor"]["may_supply_rows"] is False
    assert status["row_count"] == status["proof_row_count"] == 0
    assert status["can_place_orders"] is False
    assert status["can_promote"] is False
    assert status["can_authorize"] is False
    assert status["supported_execution_decision"] == "no_trade"


def test_recursive_verifier_reopens_direct_helper_and_all_reviewed_artifacts(monkeypatch) -> None:
    actual = timing_v3._read_exact
    observed: list[str] = []

    def record(path: str, sha256: str, byte_length: int) -> bytes:
        observed.append(path)
        return actual(path, sha256, byte_length)

    monkeypatch.setattr(timing_v3, "_read_exact", record)
    assert timing_v3.production_dependency_status()["r3"]["transitive_closure_verified"] is True
    assert len(set(observed)) >= 27
    assert "src/forex_system/contracts/currency_state.py" in observed
    assert "config/currency_state_engine_v2.json" in observed


def test_replay_and_prospective_proof_entrypoints_fail_closed() -> None:
    with pytest.raises(timing_v3.ResponseTimingV3DependencyError, match="v6_r2_final_approval"):
        timing_v3.build_replay_diagnostic({"caller": "cannot bypass"})
    with pytest.raises(timing_v3.ResponseTimingV3DependencyError, match="v6_r2_final_approval"):
        timing_v3.build_prospective_proof(allow=True, approved=True)


def test_replay_and_prospective_capture_partitions_are_distinct() -> None:
    prospective = _build(mode="prospective_capture_test_fixture")
    replay = _build(mode="replay_diagnostic_test_fixture")
    assert prospective["partition"] == "diagnostic_prospective_capture"
    assert replay["partition"] == "diagnostic_replay"
    assert prospective["snapshot_id"] != replay["snapshot_id"]
    assert prospective["responses"][0]["response_id"] != replay["responses"][0]["response_id"]
    assert prospective["proof_row_count"] == replay["proof_row_count"] == 0


def test_fixture_is_deterministic_post_outcome_only_and_no_trade() -> None:
    first = _build()
    second = _build()
    assert first == second
    assert first["response_count"] == 1
    assert first["timing_arm_count"] == 4
    assert first["test_only"] is True
    assert first["canonical_output"] is False
    assert first["proof_row_count"] == 0
    timing_v3.validate_test_diagnostic_snapshot(first)
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
    assert latency["start_known_lag_sec"] == latency["end_known_lag_sec"] == 1.0


def test_response_identity_commits_prices_payload_hashes_chronology_and_binding() -> None:
    row, _ = _response()
    changed = copy.deepcopy(row)
    changed["end_bid"] = 1.1020
    changed["end_ask"] = 1.1022
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="response_identity_mismatch"):
        _build(responses=[changed])
    changed["response_id"] = timing_v3._response_identity(
        changed, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture"
    )
    assert changed["response_id"] != row["response_id"]
    assert _build(responses=[changed])["responses"][0]["end_bid"] == 1.1020


def test_naive_or_nonzero_offset_timestamps_are_rejected() -> None:
    row, _ = _response()
    for value in ("2026-08-17T12:00:01", "2026-08-17T08:00:01-04:00"):
        changed = copy.deepcopy(row)
        changed["start_quote_time_utc"] = value
        changed["response_id"] = timing_v3._response_identity(
            changed, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture"
        )
        with pytest.raises(timing_v3.ResponseTimingV3Error, match="zero_utc_offset"):
            _build(responses=[changed])


def test_duplicate_natural_cells_are_rejected_even_with_distinct_ids() -> None:
    first, _ = _response()
    second, _ = _response(end_bid=1.1020, end_ask=1.1022)
    assert first["response_id"] != second["response_id"]
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="duplicate_response_natural_cell"):
        _build(responses=[first, second])


def test_input_order_does_not_change_snapshot_or_arm_order() -> None:
    first, _ = _response()
    second, _ = _response(
        instrument="GBP_USD", currency="GBP", horizon=60, event_version_id="event-version-2"
    )
    assert _build(responses=[first, second]) == _build(responses=[second, first])


def test_cross_partition_rows_cannot_be_reclassified() -> None:
    row, _ = _response(mode="replay_diagnostic_test_fixture")
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="response_partition_mismatch"):
        _build(mode="prospective_capture_test_fixture", responses=[row])


def test_source_or_attestation_cannot_self_enable_proof() -> None:
    source = _snapshot()
    source["proof_row_count"] = 1
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="proof_rows_forbidden"):
        timing_v3.build_test_diagnostic_snapshot(
            event_response_snapshot=source, dependency_attestation=_attestation()
        )
    attestation = _attestation()
    attestation["review_disposition"] = "self_approved"
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="not_approved"):
        timing_v3.build_test_diagnostic_snapshot(
            event_response_snapshot=_snapshot(), dependency_attestation=attestation
        )


def test_source_and_attestation_are_exactly_cross_bound() -> None:
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="source_snapshot_mismatch"):
        timing_v3.build_test_diagnostic_snapshot(
            event_response_snapshot=_snapshot(),
            dependency_attestation=_attestation(source_snapshot_id="different"),
        )
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="source_binding_mismatch"):
        timing_v3.build_test_diagnostic_snapshot(
            event_response_snapshot=_snapshot(),
            dependency_attestation=_attestation(source_binding="e" * 64),
        )


def test_contract_is_inert_and_has_no_canonical_or_worker_surface() -> None:
    contract = json.loads(
        (ROOT / "config" / "currency_state_response_timing_arms_v3.json").read_text()
    )
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
    assert timing_v3.verify_candidate_manifest() == {
        "manifest_id": "currency_state_response_timing_arms_v3_manifest_20260817_r1",
        "review_state": "pending_independent_review",
        "artifact_count": 13,
        "dependency_gate": "closed",
        "candidate_closure_verified": True,
    }


def test_module_has_no_project_runtime_network_broker_database_or_write_imports() -> None:
    path = ROOT / "src" / "forex_system" / "research" / "currency_state_response_timing_arms_v3.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend((0, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append((node.level, node.module))
    assert all(level == 0 for level, _ in imported)
    forbidden = (
        "requests", "httpx", "sqlite3", "subprocess", "oandapy", "oandapyV20",
        "authorization", "execution", "lifecycle", "supervisor",
    )
    assert all(not any(name == item or name.startswith(item + ".") for item in forbidden) for _, name in imported)


def test_validator_rejects_forged_snapshot_id_price_chronology_and_derived_economics() -> None:
    for mutate, expected in (
        (lambda value: value.__setitem__("snapshot_id", "forged"), "snapshot_id_mismatch"),
        (lambda value: value["responses"][0].__setitem__("end_bid", 1.1009), "response_identity_mismatch"),
        (
            lambda value: value["responses"][0].__setitem__("end_quote_time_utc", "2020-01-01T00:00:00Z"),
            "response_end_alignment_invalid",
        ),
        (
            lambda value: value["responses"][0].__setitem__("gross_long_mid_pips", 999.0),
            "reconstruction_mismatch",
        ),
    ):
        changed = copy.deepcopy(_build())
        mutate(changed)
        with pytest.raises(timing_v3.ResponseTimingV3Error, match=expected):
            timing_v3.validate_test_diagnostic_snapshot(changed)


def test_validator_rejects_duplicate_responses_ids_and_natural_cells() -> None:
    changed = copy.deepcopy(_build())
    changed["responses"].append(copy.deepcopy(changed["responses"][0]))
    changed["response_count"] = 2
    changed["timing_arms"] += copy.deepcopy(changed["timing_arms"])
    changed["timing_arm_count"] = 8
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="duplicate_response_natural_cell"):
        timing_v3.validate_test_diagnostic_snapshot(changed)


def test_validator_rejects_source_attestation_changes_and_unknown_fields() -> None:
    changed = copy.deepcopy(_build())
    changed["source_contract_id"] = "evil"
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="source_contract_mismatch"):
        timing_v3.validate_test_diagnostic_snapshot(changed)
    changed = copy.deepcopy(_build())
    changed["dependency_attestation"] = {"self": "approved"}
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="attestation_closed_schema"):
        timing_v3.validate_test_diagnostic_snapshot(changed)
    changed = copy.deepcopy(_build())
    changed["unknown"] = True
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="snapshot_closed_schema"):
        timing_v3.validate_test_diagnostic_snapshot(changed)


def test_validator_rejects_missing_duplicate_or_relinked_arms() -> None:
    changed = copy.deepcopy(_build())
    changed["timing_arms"][0] = copy.deepcopy(changed["timing_arms"][3])
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="arm_reconstruction_or_linkage"):
        timing_v3.validate_test_diagnostic_snapshot(changed)
    changed = copy.deepcopy(_build())
    changed["timing_arms"][0]["response_id"] = "relinked"
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="arm_reconstruction_or_linkage"):
        timing_v3.validate_test_diagnostic_snapshot(changed)


def test_noncanonical_pair_and_currency_are_rejected() -> None:
    row, _ = _response(instrument="AAA_BBB", currency="AAA")
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="outside_canonical_universe"):
        _build(responses=[row])
    row, _ = _response(currency="AAA")
    with pytest.raises(timing_v3.ResponseTimingV3Error, match="not_canonical_pair_leg"):
        _build(responses=[row])


def test_v2_parent_bytes_are_unchanged() -> None:
    expected = {
        "src/forex_system/research/currency_state_response_timing_arms_v2.py": "b7de98af65302218791085f84c2726c8c14de6ca4c83acdd52b157b4ab75ef84",
        "config/currency_state_response_timing_arms_v2.json": "2885b19773565ce63dd706ced1c4e5eb9c5405c028664f60befc155b8f378618",
        "config/currency_state_response_timing_arms_v2_manifest.json": "ab03f54f358d3c69eed6e77767b61a9ed57fc4f4012bb9af6a96805adc1a9d54",
        "test_currency_state_response_timing_arms_v2.py": "8f01e698850396072f1047b8fe82840fee20f380632e433823b6cd4e9e0de1fa",
    }
    import hashlib

    for relative, sha256 in expected.items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == sha256
