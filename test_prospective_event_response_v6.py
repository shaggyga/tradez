from __future__ import annotations

import ast
import copy
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import forex_system.ingestion.prospective_event_response_v6 as response_v6
    from forex_system.contracts.currency_state import stable_hash
    from forex_system.features.currency_state_official_context_v5 import attach_official_fact_context_v5
    from forex_system.ingestion.official_fact_adapter_v5 import (
        OfficialFactAdapterV5,
        OfficialFactAdapterV5Error,
    )
except ModuleNotFoundError:
    import src.forex_system.ingestion.prospective_event_response_v6 as response_v6
    from src.forex_system.contracts.currency_state import stable_hash
    from src.forex_system.features.currency_state_official_context_v5 import attach_official_fact_context_v5
    from src.forex_system.ingestion.official_fact_adapter_v5 import (
        OfficialFactAdapterV5,
        OfficialFactAdapterV5Error,
    )

from test_oanda_currency_state_official_context import base_snapshot


ROOT = Path(__file__).resolve().parent
FIXED_UPSTREAM_CUTOFF = "2026-08-17T08:00:00+00:00"


def _aligned_base() -> dict:
    _, base = base_snapshot()
    base["decision_cutoff_utc"] = FIXED_UPSTREAM_CUTOFF
    for horizon in base["horizons"].values():
        horizon["decision_cutoff_utc"] = FIXED_UPSTREAM_CUTOFF
    identity = {
        key: base[key]
        for key in (
            "contract_id", "contract_sha256", "decision_cutoff_utc",
            "completed_bar_cutoff_utc", "input_identity", "horizons",
        )
    }
    base["snapshot_id"] = "currency_state_snapshot_" + stable_hash(identity)[:24]
    return base


@pytest.fixture(scope="module")
def governed() -> dict:
    official = OfficialFactAdapterV5().as_of(FIXED_UPSTREAM_CUTOFF)
    base = _aligned_base()
    context = attach_official_fact_context_v5(base, official)
    return {"official": official, "base": base, "context": context}


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _observation(governed: dict, *, horizon: int = 300) -> tuple[dict, str]:
    event = governed["official"]["upcoming_events"][0]
    scheduled = dt.datetime.fromisoformat(event["scheduled_utc"].replace("Z", "+00:00"))
    instrument = next(
        item for item in response_v6.CANONICAL_INSTRUMENTS
        if event["currency"] in item.split("_")
    )
    start_quote = scheduled + dt.timedelta(seconds=1)
    end_target = scheduled + dt.timedelta(seconds=horizon)
    end_quote = end_target + dt.timedelta(seconds=2)
    response = {
        "event_id": event["event_id"],
        "event_version_id": event["event_version_id"],
        "currency": event["currency"],
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
        "end_bid": 1.1010,
        "end_ask": 1.1012,
        "pip": 0.0001,
        "start_source_payload_sha256": "a" * 64,
        "end_source_payload_sha256": "b" * 64,
        "start_source_record_id": "fixture-start-record",
        "end_source_record_id": "fixture-end-record",
    }
    capture = _iso(end_quote + dt.timedelta(seconds=2))
    return response, capture


def _build(governed: dict, observations: list[dict] | None = None, *, mode: str = "prospective_capture_test_fixture") -> dict:
    if observations is None:
        observation, capture = _observation(governed)
        observations = [observation]
    else:
        _, capture = _observation(governed)
    return response_v6.build_test_snapshot(
        context_snapshot=governed["context"],
        base_snapshot=governed["base"],
        official_snapshot=governed["official"],
        executable_responses=observations,
        capture_cutoff_utc=capture,
        mode=mode,
    )


def test_full_r3_dependency_closure_and_review_certificate_are_exact() -> None:
    result = response_v6.verify_dependency_closure()
    assert result == {
        "contract_sha256": response_v6.CONTRACT_SHA256,
        "review_certificate_sha256": response_v6.REVIEW_CERTIFICATE_SHA256,
        "review_certificate_id": response_v6.REVIEW_CERTIFICATE_ID,
        "pinned_r3_artifact_count": 23,
        "closure_verified": True,
    }


def test_successor_manifest_pins_own_and_transitive_closure_without_activation() -> None:
    result = response_v6.verify_candidate_manifest()
    assert result == {
        "manifest_id": "prospective_event_response_v6_manifest_20260817_r1",
        "artifact_count": 6,
        "transitive_upstream_artifact_count": 23,
        "candidate_closure_verified": True,
        "review_state": "pending_independent_review",
    }


def test_runtime_imports_only_approved_v5_not_rejected_predecessors_or_broker() -> None:
    tree = ast.parse(Path(response_v6.__file__).read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    forbidden = (
        "official_fact_adapter_v3", "official_fact_adapter_v4",
        "currency_state_official_context_v3", "currency_state_official_context_v4",
        "oandapy", "oandapyV20", "requests", "httpx",
    )
    assert all(not any(name.endswith(item) or name.startswith(item) for item in forbidden) for name in imported)

    script = """
import sys
for name in (
    'forex_system.ingestion.official_fact_adapter_v3',
    'forex_system.ingestion.official_fact_adapter_v4',
    'forex_system.features.currency_state_official_context_v3',
    'forex_system.features.currency_state_official_context_v4',
):
    sys.modules[name] = None
from forex_system.ingestion.prospective_event_response_v6 import verify_dependency_closure
assert verify_dependency_closure()['closure_verified'] is True
"""
    completed = subprocess.run(
        [sys.executable, "-c", script], cwd=ROOT,
        env={**dict(__import__("os").environ), "PYTHONPATH": str(ROOT / "src")},
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_fixed_cutoff_snapshot_is_deterministic_complete_and_noncanonical(governed: dict) -> None:
    first = _build(governed)
    second = _build(governed)
    assert first == second
    assert first["snapshot_id"].startswith("prospective_event_response_v6_test_")
    assert first["currency_count"] == 21
    assert first["instrument_count"] == 68
    assert first["horizon_count"] == 5
    assert len(first["context_matrix"]) == 340
    assert first["diagnostic_response_count"] == 1
    assert first["proof_row_count"] == 0
    assert first["test_only"] is True
    assert first["canonical_output"] is False
    assert first["execution_eligible"] is False
    assert first["can_place_orders"] is False
    assert first["can_promote"] is False
    assert first["can_authorize"] is False
    assert first["supported_execution_decision"] == "no_trade"


def test_exact_v5_r3_snapshot_and_input_identities_are_bound(governed: dict) -> None:
    snapshot = _build(governed)
    binding = snapshot["source_binding"]
    assert binding["official_fact_snapshot_id"] == governed["official"]["snapshot_id"]
    assert binding["currency_state_official_context_snapshot_id"] == governed["context"]["snapshot_id"]
    assert binding["base_currency_state_snapshot_id"] == governed["base"]["snapshot_id"]
    assert binding["context_input_identity_sha256"] == stable_hash(governed["context"]["input_identity"])
    assert binding["base_currency_state_input_identity_sha256"] == stable_hash(governed["base"]["input_identity"])
    assert snapshot["source_binding_sha256"] == stable_hash(binding)


def test_mutated_context_base_or_official_fails_closed(governed: dict) -> None:
    observation, capture = _observation(governed)
    cases = []
    context = copy.deepcopy(governed["context"])
    context["input_identity"]["quote_history_sha256"] = "0" * 64
    cases.append((context, governed["base"], governed["official"]))
    base = copy.deepcopy(governed["base"])
    base["input_identity"]["quote_history_sha256"] = "0" * 64
    cases.append((governed["context"], base, governed["official"]))
    official = copy.deepcopy(governed["official"])
    official["snapshot_id"] = official["snapshot_id"][:-1] + "0"
    cases.append((governed["context"], governed["base"], official))
    for context_row, base_row, official_row in cases:
        with pytest.raises((ValueError, OfficialFactAdapterV5Error, response_v6.ProspectiveEventResponseV6Error)):
            response_v6.build_test_snapshot(
                context_snapshot=context_row, base_snapshot=base_row,
                official_snapshot=official_row, executable_responses=[observation],
                capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
            )


def test_no_upstream_prediction_can_become_forecast_ev_rank_or_basis(governed: dict) -> None:
    snapshot = _build(governed)
    rows = snapshot["plans"] + snapshot["response_diagnostics"] + snapshot["context_matrix"]
    for row in rows:
        for field in (
            "forecast_mean_pips", "forecast_mean_bps", "forecast_absolute_move_bps",
            "forecast_probability", "cost_clear_probability", "expected_value_pips",
            "allocator_rank", "direction_selected",
        ):
            if field in row:
                assert row[field] is None
        assert row["basis_eligible"] is False
        assert row["proof_eligible"] is False


def test_zero_quote_inputs_create_zero_response_or_proof_rows(governed: dict) -> None:
    snapshot = _build(governed, [])
    assert snapshot["diagnostic_response_count"] == 0
    assert snapshot["response_diagnostics"] == []
    assert snapshot["proof_row_count"] == 0
    assert len(snapshot["context_matrix"]) == 340


def test_executable_chronology_and_after_cost_fields_are_exact(governed: dict) -> None:
    snapshot = _build(governed)
    row = snapshot["response_diagnostics"][0]
    assert row["start_alignment_sec"] == 1.0
    assert row["end_alignment_sec"] == 2.0
    assert row["actual_quote_duration_sec"] == 301.0
    assert row["start_spread_pips"] == pytest.approx(2.0)
    assert row["end_spread_pips"] == pytest.approx(2.0)
    assert row["gross_long_mid_pips"] == pytest.approx(10.0)
    assert row["gross_short_mid_pips"] == pytest.approx(-10.0)
    assert row["long_after_executable_spread_pips"] == pytest.approx(8.0)
    assert row["short_after_executable_spread_pips"] == pytest.approx(-12.0)
    assert row["long_execution_cost_pips"] == pytest.approx(2.0)
    assert row["short_execution_cost_pips"] == pytest.approx(2.0)


@pytest.mark.parametrize(
    ("field", "transform", "match"),
    [
        ("start_quote_time_utc", lambda event, end, value: _iso(event - dt.timedelta(seconds=1)), "start_alignment"),
        ("end_quote_time_utc", lambda event, end, value: _iso(end + dt.timedelta(seconds=46)), "end_alignment"),
        ("start_known_utc", lambda event, end, value: _iso(event + dt.timedelta(seconds=30)), "start_knowledge_clock"),
        ("end_known_utc", lambda event, end, value: _iso(end + dt.timedelta(seconds=30)), "end_knowledge_clock"),
    ],
)
def test_future_stale_alignment_and_clock_poison_fail_closed(governed: dict, field, transform, match) -> None:
    observation, capture = _observation(governed)
    event = dt.datetime.fromisoformat(observation["event_scheduled_utc"].replace("Z", "+00:00"))
    end = dt.datetime.fromisoformat(observation["end_target_utc"].replace("Z", "+00:00"))
    observation[field] = transform(event, end, observation[field])
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match=match):
        response_v6.build_test_snapshot(
            context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[observation],
            capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
        )


def test_quote_known_after_capture_and_capture_before_upstream_fail_closed(governed: dict) -> None:
    observation, capture = _observation(governed)
    observation["end_known_utc"] = _iso(dt.datetime.fromisoformat(capture.replace("Z", "+00:00")) + dt.timedelta(seconds=1))
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match="end_knowledge_clock"):
        response_v6.build_test_snapshot(
            context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[observation],
            capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
        )
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match="capture_precedes"):
        response_v6.build_test_snapshot(
            context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[],
            capture_cutoff_utc="2026-08-17T07:59:59Z", mode="prospective_capture_test_fixture",
        )


def test_replay_and_prospective_capture_are_separate_diagnostic_partitions(governed: dict) -> None:
    prospective = _build(governed, mode="prospective_capture_test_fixture")
    replay = _build(governed, mode="replay_diagnostic_test_fixture")
    assert prospective["snapshot_id"] != replay["snapshot_id"]
    assert prospective["response_diagnostics"][0]["partition"] == "diagnostic_prospective_capture"
    assert replay["response_diagnostics"][0]["partition"] == "diagnostic_replay"
    assert prospective["proof_row_count"] == replay["proof_row_count"] == 0
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match="only_explicit"):
        response_v6.build_test_snapshot(
            context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[],
            capture_cutoff_utc=FIXED_UPSTREAM_CUTOFF, mode="prospective_proof",
        )


def test_reconstruction_rejects_any_snapshot_mutation(governed: dict) -> None:
    observation, capture = _observation(governed)
    snapshot = _build(governed)
    response_v6.validate_test_snapshot(
        snapshot, context_snapshot=governed["context"], base_snapshot=governed["base"],
        official_snapshot=governed["official"], executable_responses=[observation],
        capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
    )
    tampered = copy.deepcopy(snapshot)
    tampered["context_matrix"][0]["expected_value_pips"] = 1.0
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match="snapshot_reconstruction"):
        response_v6.validate_test_snapshot(
            tampered, context_snapshot=governed["context"], base_snapshot=governed["base"],
            official_snapshot=governed["official"], executable_responses=[observation],
            capture_cutoff_utc=capture, mode="prospective_capture_test_fixture",
        )


def test_manifest_certificate_or_source_mutation_is_not_accepted(monkeypatch) -> None:
    original = response_v6._read_exact

    def poisoned(relative_path: str, sha256: str, byte_length: int) -> bytes:
        payload = original(relative_path, sha256, byte_length)
        if relative_path.endswith("review_certificate.json"):
            row = json.loads(payload)
            row["review_results"]["p2_findings"] = 1
            return json.dumps(row, separators=(",", ":"), sort_keys=True).encode()
        return payload

    monkeypatch.setattr(response_v6, "_read_exact", poisoned)
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match="material_findings"):
        response_v6.verify_dependency_closure()

    monkeypatch.setattr(response_v6, "_read_exact", original)
    path, (sha256, byte_length) = next(iter(response_v6._PINNED_R3_ARTIFACTS.items()))
    monkeypatch.setitem(response_v6._PINNED_R3_ARTIFACTS, path, ("0" * 64, byte_length))
    with pytest.raises(response_v6.ProspectiveEventResponseV6Error, match="reviewed_artifact_set_mismatch"):
        response_v6.verify_dependency_closure()


def test_contract_and_module_expose_no_live_worker_or_canonical_output_path() -> None:
    contract = json.loads((ROOT / "config/prospective_event_response_v6_contract.json").read_text(encoding="utf-8"))
    assert contract["state"]["enabled"] is False
    assert contract["state"]["registered"] is False
    assert contract["state"]["research_worker_enabled"] is False
    assert contract["state"]["collector_enabled"] is False
    assert contract["state"]["canonical_output_initialized"] is False
    assert contract["output_policy"]["canonical_output_forbidden"] is True
    source = Path(response_v6.__file__).read_text(encoding="utf-8")
    assert "sqlite3" not in source
    assert "research_ledgers" not in source
    assert "order_submission" not in source
