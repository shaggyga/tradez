from __future__ import annotations

import ast
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from src.forex_system.research import currency_state_response_timing_arms_v7 as timing_v7
from test_currency_state_response_timing_arms_v4 import (
    SOURCE_BINDING,
    _attestation,
    _response,
    _source,
)


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc


def _build(responses=None):
    return timing_v7.build_test_diagnostic_snapshot(
        event_response_snapshot=_source(responses),
        dependency_attestation=_attestation(),
    )


def _rehash(snapshot: dict) -> None:
    snapshot["snapshot_id"] = timing_v7._snapshot_id(snapshot)


def test_v6_exact_bytes_are_preserved() -> None:
    expected = {
        "src/forex_system/research/currency_state_response_timing_arms_v6.py": "4de3e9f66fa9345723c2af4dc726cfb30cbc2d4afc1afceee910e9bf20b1fe96",
        "config/currency_state_response_timing_arms_v6.json": "525039ed9014943f7950cb465cebeb103332a7f317a6c47472362368def773b4",
        "config/currency_state_response_timing_arms_v6_manifest.json": "82d12f151546fed7a857168b7ef5265acec1e19e62e936a2a803699703080846",
        "test_currency_state_response_timing_arms_v6.py": "6aee761616d61a6c956a899746d556d49bdad6f1c55efae452afbca4d385aedd",
    }
    for relative, sha256 in expected.items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == sha256


def test_complete_exact_parent_status_is_required() -> None:
    status = timing_v7.production_dependency_status()
    assert status["approved"] is False and status["blocked"] is True
    assert status["row_count"] == status["proof_row_count"] == 0
    assert status["canonical_universe"] == timing_v7.UNIVERSE
    assert len(status["barred_predecessors"]) == 3
    assert status["required_event_response"]["approval_state"] == "unresolved_fail_closed"
    assert status["self_closure_verified"] is False
    assert status["can_place_orders"] is False


def test_production_and_proof_entrypoints_remain_closed() -> None:
    with pytest.raises(timing_v7.ResponseTimingV7DependencyError):
        timing_v7.build_replay_diagnostic()
    with pytest.raises(timing_v7.ResponseTimingV7DependencyError):
        timing_v7.build_prospective_proof()


def test_v7_fixture_round_trip_preserves_v6_economics_and_guards() -> None:
    snapshot = _build()
    timing_v7.validate_test_diagnostic_snapshot(snapshot)
    assert snapshot["schema_version"] == 7
    assert snapshot["contract_id"] == timing_v7.CONTRACT_ID
    assert snapshot["proof_row_count"] == 0
    assert snapshot["responses"][0]["start_spread_pips"] == pytest.approx(2.0)
    assert snapshot["responses"][0]["long_after_executable_spread_pips"] == pytest.approx(8.0)
    assert snapshot["execution_eligible"] is False


def test_canonical_pip_and_numeric_utc_guards_are_preserved() -> None:
    row, _ = _response(instrument="EUR_USD", currency="EUR")
    row["pip"] = 1.0
    from src.forex_system.research import currency_state_response_timing_arms_v4 as v4

    row["response_id"] = v4._response_identity(
        row,
        source_binding_sha256=SOURCE_BINDING,
        mode="prospective_capture_test_fixture",
    )
    with pytest.raises(timing_v7.ResponseTimingV7Error, match="pip_not_exact_canonical"):
        _build([row])

    early, _ = _response(event_version_id="early")
    later, _ = _response(
        event_version_id="later",
        instrument="GBP_USD",
        currency="GBP",
        scheduled=dt.datetime(2026, 8, 17, 12, 0, 0, 100000, tzinfo=UTC),
    )
    assert [item["event_version_id"] for item in _build([later, early])["responses"]] == [
        "early",
        "later",
    ]


@pytest.mark.parametrize(
    ("field", "bad"),
    [("schema_version", 7.0), ("response_count", True), ("timing_arm_count", 4.0)],
)
def test_snapshot_scalar_types_are_exact(field: str, bad: object) -> None:
    snapshot = _build()
    snapshot[field] = bad
    _rehash(snapshot)
    with pytest.raises(timing_v7.ResponseTimingV7Error, match="type_mismatch"):
        timing_v7.validate_test_diagnostic_snapshot(snapshot)


def test_deep_r3_unreadable_raises_and_never_synthesizes_universe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.forex_system.research import currency_state_response_timing_arms_v4 as v4

    original = v4._stable_read
    deep_r3 = "config/prospective_event_response_v6_r3_contract.json"

    def unreadable(relative_path: str) -> bytes:
        if relative_path == deep_r3:
            raise v4.ResponseTimingV4Error("adversarial_deep_r3_unreadable")
        return original(relative_path)

    monkeypatch.setattr(v4, "_stable_read", unreadable)
    with pytest.raises(timing_v7.ResponseTimingV7Error, match="parent_v5_success_status"):
        timing_v7.production_dependency_status()
    with pytest.raises(timing_v7.ResponseTimingV7Error, match="parent_v5_success_status"):
        timing_v7.build_test_diagnostic_snapshot(
            event_response_snapshot=_source(), dependency_attestation=_attestation()
        )
    with pytest.raises(timing_v7.ResponseTimingV7Error, match="parent_v5_success_status"):
        timing_v7.verify_candidate_manifest()


def test_parent_status_missing_or_bool_alias_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad = dict(timing_v7.EXPECTED_V5_STATUS)
    bad["row_count"] = False
    monkeypatch.setattr(timing_v7.v6.v5, "production_dependency_status", lambda: bad)
    with pytest.raises(timing_v7.ResponseTimingV7Error, match="type_mismatch"):
        timing_v7.production_dependency_status()


def test_manifest_can_only_verify_nonself_closure() -> None:
    assert timing_v7.verify_candidate_manifest() == {
        "manifest_id": timing_v7.MANIFEST_ID,
        "nonself_closure_verified": True,
        "self_closure_verified": False,
        "candidate_closure_verified": False,
        "activation_allowed": False,
        "external_review_certificate_required": True,
    }


def test_module_has_no_operational_import_or_write_surface() -> None:
    path = ROOT / "src/forex_system/research/currency_state_response_timing_arms_v7.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = []
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
        "authorization",
        "execution",
        "lifecycle",
        "supervisor",
        "oandapy",
    )
    assert all(
        not any(name == item or name.startswith(item + ".") for item in forbidden)
        for name in imported
    )
    forbidden_calls = {"write_text", "write_bytes", "open", "unlink", "replace", "rename"}
    assert not any(
        isinstance(node, ast.Attribute) and node.attr in forbidden_calls
        for node in ast.walk(tree)
    )
