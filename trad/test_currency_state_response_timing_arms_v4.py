from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

try:
    import forex_system.research.currency_state_response_timing_arms_v4 as timing_v4
except ModuleNotFoundError:
    import src.forex_system.research.currency_state_response_timing_arms_v4 as timing_v4


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc
SOURCE_ID = "synthetic-v6-r4-unapproved-source"
SOURCE_BINDING = "f" * 64


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _attestation(source_id: str = SOURCE_ID, binding: str = SOURCE_BINDING) -> dict:
    return {
        "contract_id": timing_v4.REQUIRED_SUCCESSOR_CONTRACT_ID,
        "cohort_id": timing_v4.REQUIRED_SUCCESSOR_COHORT_ID,
        "source_snapshot_id": source_id,
        "source_binding_sha256": binding,
        "test_fixture_only": True,
        "approval_state": "synthetic_fixture_not_approval",
    }


def _response(
    *,
    mode: str = "prospective_capture_test_fixture",
    event_version_id: str = "event-version-1",
    currency: str = "EUR",
    instrument: str = "EUR_USD",
    horizon: int = 300,
    scheduled: dt.datetime | None = None,
) -> tuple[dict, str]:
    scheduled = scheduled or dt.datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    start_quote = scheduled + dt.timedelta(seconds=1)
    end_target = scheduled + dt.timedelta(seconds=horizon)
    end_quote = end_target + dt.timedelta(seconds=2)
    pip = 0.01 if instrument.endswith("_JPY") else 0.0001
    if pip == 0.01:
        start_bid, start_ask, end_bid, end_ask = 150.00, 150.02, 150.10, 150.12
    else:
        start_bid, start_ask, end_bid, end_ask = 1.1000, 1.1002, 1.1010, 1.1012
    row = {
        "response_id": "pending",
        "partition": timing_v4.DIAGNOSTIC_MODES[mode],
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
        "start_bid": start_bid,
        "start_ask": start_ask,
        "end_bid": end_bid,
        "end_ask": end_ask,
        "pip": pip,
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
    row["response_id"] = timing_v4._response_identity(row, source_binding_sha256=SOURCE_BINDING, mode=mode)
    return row, _iso(end_quote + dt.timedelta(seconds=5))


def _source(responses: list[dict] | None = None, *, mode: str = "prospective_capture_test_fixture") -> dict:
    if responses is None:
        row, cutoff = _response(mode=mode)
        responses = [row]
    else:
        latest = max(dt.datetime.fromisoformat(row["end_known_utc"].replace("Z", "+00:00")) for row in responses)
        cutoff = _iso(latest + dt.timedelta(seconds=5))
    return {
        "snapshot_id": SOURCE_ID,
        "contract_id": timing_v4.REQUIRED_SUCCESSOR_CONTRACT_ID,
        "cohort_id": timing_v4.REQUIRED_SUCCESSOR_COHORT_ID,
        "mode": mode,
        "partition": timing_v4.DIAGNOSTIC_MODES[mode],
        "capture_cutoff_utc": cutoff,
        "source_binding_sha256": SOURCE_BINDING,
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


def _build(responses: list[dict] | None = None, *, mode: str = "prospective_capture_test_fixture") -> dict:
    return timing_v4.build_test_diagnostic_snapshot(event_response_snapshot=_source(responses, mode=mode), dependency_attestation=_attestation())


def _rehash(snapshot: dict) -> None:
    material = {key: value for key, value in snapshot.items() if key != "snapshot_id"}
    snapshot["snapshot_id"] = "currency_state_response_timing_v4_test_" + timing_v4._sha256_json(material)


def test_production_gate_is_permanently_closed_for_this_contract() -> None:
    status = timing_v4.production_dependency_status()
    assert status["approved"] is False and status["blocked"] is True
    assert status["blocking_reasons"] == ["event_response_v6_r4_final_approval_not_bound"]
    assert status["row_count"] == status["proof_row_count"] == 0
    assert status["canonical_universe"]["currency_count"] == 21
    assert status["canonical_universe"]["instrument_count"] == 68
    assert status["r3"]["reviewed_artifact_count"] == 23
    assert len(status["barred_predecessors"]) == 3
    assert status["can_place_orders"] is False and status["can_promote"] is False


def test_canonical_and_proof_entrypoints_fail_closed() -> None:
    with pytest.raises(timing_v4.ResponseTimingV4DependencyError):
        timing_v4.build_replay_diagnostic()
    with pytest.raises(timing_v4.ResponseTimingV4DependencyError):
        timing_v4.build_prospective_proof()


def test_fixture_builds_and_reconstructs_exact_economics() -> None:
    snapshot = _build()
    timing_v4.validate_test_diagnostic_snapshot(snapshot)
    row = snapshot["responses"][0]
    assert row["start_spread_pips"] == pytest.approx(2.0)
    assert row["long_after_executable_spread_pips"] == pytest.approx(8.0)
    assert snapshot["proof_row_count"] == 0
    assert all(arm["supported_execution_decision"] == "no_trade" for arm in snapshot["timing_arms"])


@pytest.mark.parametrize(
    "instrument,currency,wrong_pip",
    [("EUR_USD", "EUR", 1.0), ("USD_JPY", "JPY", 0.0001)],
)
def test_canonical_pip_is_derived_from_instrument(instrument: str, currency: str, wrong_pip: float) -> None:
    row, _ = _response(instrument=instrument, currency=currency)
    row["pip"] = wrong_pip
    row["response_id"] = timing_v4._response_identity(row, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture")
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="pip_not_exact_canonical"):
        _build([row])


def test_fractional_timestamps_sort_by_numeric_utc() -> None:
    early, _ = _response(event_version_id="zero")
    later, _ = _response(event_version_id="later", instrument="GBP_USD", currency="GBP", scheduled=dt.datetime(2026, 8, 17, 12, 0, 0, 100000, tzinfo=UTC))
    snapshot = _build([later, early])
    assert [row["event_version_id"] for row in snapshot["responses"]] == ["zero", "later"]


@pytest.mark.parametrize("field,value", [("schema_version", 4.0), ("response_count", True), ("timing_arm_count", 4.0)])
def test_snapshot_scalar_types_are_exact(field: str, value: object) -> None:
    snapshot = _build()
    snapshot[field] = value
    _rehash(snapshot)
    with pytest.raises(timing_v4.ResponseTimingV4Error, match=f"snapshot_{field}_type_mismatch"):
        timing_v4.validate_test_diagnostic_snapshot(snapshot)


def test_response_exact_string_and_float_types_are_required() -> None:
    class StringSubclass(str):
        pass

    row, _ = _response()
    row["instrument"] = StringSubclass("EUR_USD")
    row["response_id"] = timing_v4._response_identity(row, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture")
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="must_be_exact_str"):
        _build([row])
    row, _ = _response()
    row["start_bid"] = 1
    row["response_id"] = timing_v4._response_identity(row, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture")
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="exact_finite_float"):
        _build([row])


def test_noncanonical_or_nonzero_offset_time_is_rejected() -> None:
    for timestamp in ("2026-08-17T12:00:00+00:00", "2026-08-17T08:00:00-04:00", "2026-08-17T12:00:00"):
        row, _ = _response()
        row["event_scheduled_utc"] = timestamp
        row["start_target_utc"] = timestamp
        row["response_id"] = timing_v4._response_identity(row, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture")
        with pytest.raises(timing_v4.ResponseTimingV4Error):
            _build([row])


def test_duplicate_natural_cell_and_reordered_output_are_rejected() -> None:
    first, _ = _response()
    second = copy.deepcopy(first)
    second["end_bid"], second["end_ask"] = 1.1020, 1.1022
    second["response_id"] = timing_v4._response_identity(second, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture")
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="duplicate_response"):
        _build([first, second])
    other, _ = _response(event_version_id="event-2", instrument="GBP_USD", currency="GBP", scheduled=dt.datetime(2026, 8, 17, 12, 1, tzinfo=UTC))
    snapshot = _build([first, other])
    snapshot["responses"].reverse()
    _rehash(snapshot)
    with pytest.raises(timing_v4.ResponseTimingV4Error):
        timing_v4.validate_test_diagnostic_snapshot(snapshot)


def test_prices_derived_economics_and_arms_are_reconstructed() -> None:
    for mutate in (
        lambda snapshot: snapshot["responses"][0].__setitem__("end_bid", 1.1009),
        lambda snapshot: snapshot["responses"][0].__setitem__("gross_long_mid_pips", 999.0),
        lambda snapshot: snapshot["timing_arms"][0].__setitem__("observed_long_after_cost_pips", 999.0),
    ):
        snapshot = copy.deepcopy(_build())
        mutate(snapshot)
        _rehash(snapshot)
        with pytest.raises(timing_v4.ResponseTimingV4Error):
            timing_v4.validate_test_diagnostic_snapshot(snapshot)


def test_source_and_attestation_are_cross_bound_and_closed() -> None:
    attestation = _attestation(binding="e" * 64)
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="source_binding"):
        timing_v4.build_test_diagnostic_snapshot(event_response_snapshot=_source(), dependency_attestation=attestation)
    attestation = _attestation()
    attestation["review_disposition"] = "approved"
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="closed_schema"):
        timing_v4.build_test_diagnostic_snapshot(event_response_snapshot=_source(), dependency_attestation=attestation)


def test_noncanonical_pair_or_currency_is_rejected() -> None:
    row, _ = _response()
    row["instrument"] = "AAA_BBB"
    row["currency"] = "AAA"
    row["response_id"] = timing_v4._response_identity(row, source_binding_sha256=SOURCE_BINDING, mode="prospective_capture_test_fixture")
    with pytest.raises(timing_v4.ResponseTimingV4Error):
        _build([row])


def test_manifest_schema_rejects_extra_output_or_enabled_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    original = timing_v4._stable_read
    path = "config/currency_state_response_timing_arms_v4_manifest.json"
    manifest = json.loads(original(path))
    manifest["output_surface"]["alternate_worker_entrypoint"] = "example.module.run"
    manifest["policy"]["can_place_orders"] = True
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    monkeypatch.setattr(timing_v4, "_stable_read", lambda requested: payload if requested == path else original(requested))
    with pytest.raises(timing_v4.ResponseTimingV4Error, match="manifest_inert_schema_mismatch"):
        timing_v4.verify_candidate_manifest()


def test_manifest_closure_is_exact_and_inert() -> None:
    result = timing_v4.verify_candidate_manifest()
    assert result["artifact_count"] == 17
    assert result["dependency_gate"] == "closed"
    assert result["review_state"] == "pending_independent_review"


def test_parent_v3_bytes_are_preserved() -> None:
    expected = {
        "src/forex_system/research/currency_state_response_timing_arms_v3.py": "8a3d60b5455ca65439cfccc39c110ea2517e77a36066fd1b607456ffede362b6",
        "config/currency_state_response_timing_arms_v3.json": "f649d6b610573714d0e5abdd7869d32b341e81ad7adf96c4714384de699f27c7",
        "config/currency_state_response_timing_arms_v3_manifest.json": "9ddac209a0e9c95dc2fddae3709476a47e143bae9f487fbdaf30c97dfa6ff258",
        "test_currency_state_response_timing_arms_v3.py": "bef00c51fc2c8ebb7824ae23d01b7226c8cfbdf78341c72d1ecc1d4c5623cc39",
    }
    for relative, sha256 in expected.items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == sha256


def test_module_has_no_project_runtime_network_database_or_broker_imports() -> None:
    path = ROOT / "src" / "forex_system" / "research" / "currency_state_response_timing_arms_v4.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    forbidden = ("requests", "httpx", "sqlite3", "subprocess", "authorization", "execution", "lifecycle", "supervisor", "oandapy")
    assert all(not any(name == item or name.startswith(item + ".") for item in forbidden) for name in imported)
