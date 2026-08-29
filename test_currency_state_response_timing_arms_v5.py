from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from src.forex_system.research import currency_state_response_timing_arms_v5 as timing_v5
from test_currency_state_response_timing_arms_v4 import (
    SOURCE_BINDING,
    _attestation,
    _response,
    _source,
)


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc


def _build(responses=None):
    return timing_v5.build_test_diagnostic_snapshot(
        event_response_snapshot=_source(responses),
        dependency_attestation=_attestation(),
    )


def _rehash(snapshot: dict) -> None:
    snapshot["snapshot_id"] = timing_v5._v5_snapshot_id(snapshot)


def test_v4_exact_bytes_are_preserved() -> None:
    expected = {
        "src/forex_system/research/currency_state_response_timing_arms_v4.py": "89fba58197e833616f132c9074bb385637dbac9dc9075ba8786c2e7d31845c02",
        "config/currency_state_response_timing_arms_v4.json": "33fbbb95a5ee29949ef4fd07b0b39586c1cec19e4467aaccde64c8c2803e472b",
        "config/currency_state_response_timing_arms_v4_manifest.json": "4e2959c114d9cd3c1b2d7e19c6e8fda178fe63a5c13e49f6fa707465669620cf",
        "test_currency_state_response_timing_arms_v4.py": "85420028e4a7bd1ba38a2d78d53534c81b5fbe23a4d109e2ba85ede9465c5f44",
    }
    for relative, sha256 in expected.items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == sha256


def test_production_and_proof_entrypoints_remain_closed() -> None:
    status = timing_v5.production_dependency_status()
    assert status["approved"] is False and status["blocked"] is True
    assert status["row_count"] == status["proof_row_count"] == 0
    assert status["canonical_universe"]["currency_count"] == 21
    assert status["canonical_universe"]["instrument_count"] == 68
    assert len(status["barred_predecessors"]) == 3
    assert status["can_place_orders"] is False
    with pytest.raises(timing_v5.ResponseTimingV5DependencyError):
        timing_v5.build_replay_diagnostic()
    with pytest.raises(timing_v5.ResponseTimingV5DependencyError):
        timing_v5.build_prospective_proof()


def test_v5_fixture_round_trip_preserves_v4_economics_and_guards() -> None:
    snapshot = _build()
    timing_v5.validate_test_diagnostic_snapshot(snapshot)
    assert snapshot["schema_version"] == 5
    assert snapshot["contract_id"] == timing_v5.CONTRACT_ID
    assert snapshot["proof_row_count"] == 0
    assert snapshot["responses"][0]["start_spread_pips"] == pytest.approx(2.0)
    assert snapshot["responses"][0]["long_after_executable_spread_pips"] == pytest.approx(8.0)
    assert snapshot["execution_eligible"] is False


def test_canonical_pip_is_still_instrument_derived() -> None:
    row, _ = _response(instrument="EUR_USD", currency="EUR")
    row["pip"] = 1.0
    from src.forex_system.research import currency_state_response_timing_arms_v4 as v4

    row["response_id"] = v4._response_identity(
        row,
        source_binding_sha256=SOURCE_BINDING,
        mode="prospective_capture_test_fixture",
    )
    with pytest.raises(timing_v5.ResponseTimingV5Error, match="pip_not_exact_canonical"):
        _build([row])


def test_fractional_utc_values_sort_by_numeric_instant() -> None:
    early, _ = _response(event_version_id="early")
    later, _ = _response(
        event_version_id="later",
        instrument="GBP_USD",
        currency="GBP",
        scheduled=dt.datetime(2026, 8, 17, 12, 0, 0, 100000, tzinfo=UTC),
    )
    snapshot = _build([later, early])
    assert [row["event_version_id"] for row in snapshot["responses"]] == [
        "early",
        "later",
    ]


@pytest.mark.parametrize(
    ("field", "bad"),
    [("schema_version", 5.0), ("response_count", True), ("timing_arm_count", 4.0)],
)
def test_snapshot_scalar_types_are_exact(field: str, bad: object) -> None:
    snapshot = _build()
    snapshot[field] = bad
    _rehash(snapshot)
    with pytest.raises(timing_v5.ResponseTimingV5Error, match=f"snapshot_{field}_type_mismatch"):
        timing_v5.validate_test_diagnostic_snapshot(snapshot)


def _mutated_manifest(monkeypatch: pytest.MonkeyPatch, mutate) -> None:
    from src.forex_system.research import currency_state_response_timing_arms_v4 as v4

    original = v4._stable_read
    path = "config/currency_state_response_timing_arms_v5_manifest.json"
    manifest = json.loads(original(path))
    mutate(manifest)
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    monkeypatch.setattr(v4, "_stable_read", lambda requested: payload if requested == path else original(requested))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["state"].__setitem__("enabled", 0),
        lambda value: value["policy"].__setitem__("research_only", 1),
        lambda value: value["dependency_disposition"].__setitem__(
            "approved_event_response_successor_bound", 0
        ),
    ],
)
def test_manifest_rejects_boolean_integer_aliases(
    monkeypatch: pytest.MonkeyPatch, mutate
) -> None:
    _mutated_manifest(monkeypatch, mutate)
    with pytest.raises(timing_v5.ResponseTimingV5Error, match="type_mismatch"):
        timing_v5.verify_candidate_manifest()


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("research_generation", "forged"),
        ("parent_contract_id", "forged_parent"),
        ("parent_disposition", "approved_and_running"),
        ("candidate_frozen_utc", {"bad": "object"}),
        ("independent_review_started_utc", "2026-08-17T10:00:00-04:00"),
        ("independent_review_completed_utc", "2026-08-17T14:06:00Z"),
        ("runtime_started_utc", "2026-08-17T14:06:00Z"),
    ],
)
def test_manifest_rejects_forged_lineage_or_lifecycle_metadata(
    monkeypatch: pytest.MonkeyPatch, field: str, bad: object
) -> None:
    _mutated_manifest(monkeypatch, lambda value: value.__setitem__(field, bad))
    with pytest.raises(timing_v5.ResponseTimingV5Error):
        timing_v5.verify_candidate_manifest()


def test_manifest_closure_is_exact_inert_and_pending_review() -> None:
    result = timing_v5.verify_candidate_manifest()
    assert result == {
        "manifest_id": timing_v5.MANIFEST_ID,
        "review_state": "pending_independent_review",
        "artifact_count": 7,
        "dependency_gate": "closed",
        "candidate_closure_verified": True,
    }


def test_module_has_no_operational_import_or_write_surface() -> None:
    path = ROOT / "src/forex_system/research/currency_state_response_timing_arms_v5.py"
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
