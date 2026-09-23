from __future__ import annotations

import ast
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from src.forex_system.research import currency_state_response_timing_arms_v6 as timing_v6
from test_currency_state_response_timing_arms_v4 import (
    SOURCE_BINDING,
    _attestation,
    _response,
    _source,
)


ROOT = Path(__file__).resolve().parent
UTC = dt.timezone.utc


def _build(responses=None):
    return timing_v6.build_test_diagnostic_snapshot(
        event_response_snapshot=_source(responses),
        dependency_attestation=_attestation(),
    )


def _rehash(snapshot: dict) -> None:
    snapshot["snapshot_id"] = timing_v6._snapshot_id(snapshot)


def _mutated_manifest(monkeypatch: pytest.MonkeyPatch, mutate) -> None:
    from src.forex_system.research import currency_state_response_timing_arms_v4 as v4

    original = v4._stable_read
    path = "config/currency_state_response_timing_arms_v6_manifest.json"
    manifest = json.loads(original(path))
    mutate(manifest)
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    monkeypatch.setattr(
        v4,
        "_stable_read",
        lambda requested: payload if requested == path else original(requested),
    )


def test_v5_exact_bytes_are_preserved() -> None:
    expected = {
        "src/forex_system/research/currency_state_response_timing_arms_v5.py": "37dbaa53bd093e129a84db31aece7bfdddec37127753cc404ddef1c1a957eb04",
        "config/currency_state_response_timing_arms_v5.json": "08674d293897332d438872f8276a46e8de3525f07e35ddb8796996b408ee8cd0",
        "config/currency_state_response_timing_arms_v5_manifest.json": "4103313565bb50205ac182f546769614fc07b7b85ab70293c8c89b599f7f98e1",
        "test_currency_state_response_timing_arms_v5.py": "181657fff94bc357944e51a0e3eb189932638f93fffeca0e5b60a6f1e2ffad93",
    }
    for relative, sha256 in expected.items():
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == sha256


def test_production_and_proof_entrypoints_remain_closed() -> None:
    status = timing_v6.production_dependency_status()
    assert status["approved"] is False and status["blocked"] is True
    assert status["row_count"] == status["proof_row_count"] == 0
    assert status["canonical_universe"]["currency_count"] == 21
    assert status["canonical_universe"]["instrument_count"] == 68
    assert status["self_closure_verified"] is False
    assert status["can_place_orders"] is False
    with pytest.raises(timing_v6.ResponseTimingV6DependencyError):
        timing_v6.build_replay_diagnostic()
    with pytest.raises(timing_v6.ResponseTimingV6DependencyError):
        timing_v6.build_prospective_proof()


def test_v6_fixture_round_trip_preserves_v5_economics_and_guards() -> None:
    snapshot = _build()
    timing_v6.validate_test_diagnostic_snapshot(snapshot)
    assert snapshot["schema_version"] == 6
    assert snapshot["contract_id"] == timing_v6.CONTRACT_ID
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
    with pytest.raises(timing_v6.ResponseTimingV6Error, match="pip_not_exact_canonical"):
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
    [("schema_version", 6.0), ("response_count", True), ("timing_arm_count", 4.0)],
)
def test_snapshot_scalar_types_are_exact(field: str, bad: object) -> None:
    snapshot = _build()
    snapshot[field] = bad
    _rehash(snapshot)
    with pytest.raises(timing_v6.ResponseTimingV6Error, match="type_mismatch"):
        timing_v6.validate_test_diagnostic_snapshot(snapshot)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["state"].__setitem__("enabled", 0),
        lambda value: value["policy"].__setitem__("research_only", 1),
        lambda value: value["dependency_disposition"].__setitem__(
            "approved_event_response_successor_bound", 0
        ),
        lambda value: value["trusted_artifacts"]["parent_v5_module"].__setitem__(
            "sha256", "0" * 64
        ),
        lambda value: value.__setitem__("external_review_certificate", {}),
    ],
)
def test_manifest_rejects_aliases_trusted_mutation_or_self_certificate(
    monkeypatch: pytest.MonkeyPatch, mutate
) -> None:
    _mutated_manifest(monkeypatch, mutate)
    with pytest.raises(timing_v6.ResponseTimingV6Error):
        timing_v6.verify_candidate_manifest()


def test_manifest_can_only_verify_nonself_closure() -> None:
    assert timing_v6.verify_candidate_manifest() == {
        "manifest_id": timing_v6.MANIFEST_ID,
        "nonself_closure_verified": True,
        "self_closure_verified": False,
        "candidate_closure_verified": False,
        "activation_allowed": False,
        "external_review_certificate_required": True,
    }


def test_altered_runtime_with_matching_manifest_cannot_self_certify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.forex_system.research import currency_state_response_timing_arms_v4 as v4

    original = v4._stable_read
    manifest_path = "config/currency_state_response_timing_arms_v6_manifest.json"
    runtime_path = "src/forex_system/research/currency_state_response_timing_arms_v6.py"
    changed_runtime = b"# attacker-controlled replacement with matching manifest metadata\n"
    manifest = json.loads(original(manifest_path))
    manifest["self_artifacts"]["runtime_core"].update(
        {
            "sha256": hashlib.sha256(changed_runtime).hexdigest(),
            "byte_length": len(changed_runtime),
        }
    )
    changed_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()

    def replaced(relative_path: str) -> bytes:
        if relative_path == manifest_path:
            return changed_manifest
        if relative_path == runtime_path:
            return changed_runtime
        return original(relative_path)

    monkeypatch.setattr(v4, "_stable_read", replaced)
    result = timing_v6.verify_candidate_manifest()
    assert result["nonself_closure_verified"] is True
    assert result["self_closure_verified"] is False
    assert result["candidate_closure_verified"] is False
    assert result["activation_allowed"] is False


def test_module_has_no_operational_import_or_write_surface() -> None:
    path = ROOT / "src/forex_system/research/currency_state_response_timing_arms_v6.py"
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
