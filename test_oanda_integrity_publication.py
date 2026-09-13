from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path

import pytest

from trad import oanda_integrity_publication as publication


SECTION = "move_first_operational_mapping_alignment_v4"


def _payload() -> dict:
    return {
        "schema_version": 1,
        "generated_utc": "2026-09-06T01:00:00+00:00",
        "classification_version": "fixture_classification",
        "status": "degraded",
        "checks": {"fixture_failed_check": False, "fixture_passing_check": True},
        "failures": ["fixture_failed_check"],
        "measurements": {"confirmed": 0, "governed_hypotheses": 53},
        "research_only": True,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "no_trade",
        SECTION: {
            "generated_utc": "2026-09-06T00:59:00+00:00",
            "contract_id": "frozen_fixture_contract",
            "resolved_factor_episode_count": 2,
            "arm_metrics": {"frozen": {"count": 2, "eligible": False}},
            "can_authorize": False,
            "execution_eligible": False,
            "episode_rows": [
                {"episode_id": "one", "label": "currency café", "count": 3, "eligible": False},
                {"episode_id": "two", "evidence": {"verdict": "fail", "pips": -1.25}},
            ],
        },
    }


def test_exact_reconstruction_preserves_all_fields_and_inputs(tmp_path):
    original = _payload()
    before = copy.deepcopy(original)
    compact = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    assert original == before
    assert "episode_rows" not in compact[SECTION]
    for key, value in before.items():
        if key != SECTION:
            assert compact[key] == value
    for key, value in before[SECTION].items():
        if key != "episode_rows":
            assert compact[SECTION][key] == value
    assert publication.restore_integrity_details(compact, snapshot_dir=tmp_path) == before
    assert publication.restore_integrity_details(before, snapshot_dir=tmp_path) == before
    assert publication.verified_detail_artifacts(before, snapshot_dir=tmp_path) == []


def test_only_explicit_sections_are_compacted_and_shared_rows_deduplicate(tmp_path):
    original = _payload()
    for key in publication.DETAIL_SECTIONS:
        original[key] = copy.deepcopy(original[SECTION])
    original["unrelated"] = {"episode_rows": copy.deepcopy(original[SECTION]["episode_rows"])}
    compact = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    assert compact["unrelated"] == original["unrelated"]
    assert len(publication.verified_detail_artifacts(compact, snapshot_dir=tmp_path)) == 1
    assert len(list(tmp_path.rglob("*.json"))) == 1
    assert publication.restore_integrity_details(compact, snapshot_dir=tmp_path) == original


def test_changed_clocks_and_metrics_reuse_verified_immutable_rows(tmp_path):
    original = _payload()
    first = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    artifact = publication.verified_detail_artifacts(first, snapshot_dir=tmp_path)[0]
    original_bytes, original_mtime = artifact.read_bytes(), artifact.stat().st_mtime_ns
    original["generated_utc"] = "2026-09-06T01:10:00+00:00"
    original[SECTION]["generated_utc"] = "2026-09-06T01:09:00+00:00"
    original[SECTION]["arm_metrics"]["frozen"]["count"] = 3
    second = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    assert second[SECTION]["_detail_reference"] == first[SECTION]["_detail_reference"]
    assert artifact.read_bytes() == original_bytes
    assert artifact.stat().st_mtime_ns == original_mtime
    assert len(list(tmp_path.rglob("*.json"))) == 1
    assert publication.restore_integrity_details(second, snapshot_dir=tmp_path) == original


def test_changed_rows_keep_historical_artifact(tmp_path):
    original = _payload()
    first = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    original[SECTION]["episode_rows"].append({"episode_id": "three"})
    second = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    assert first[SECTION]["_detail_reference"] != second[SECTION]["_detail_reference"]
    assert len(list(tmp_path.rglob("*.json"))) == 2
    assert len(publication.restore_integrity_details(first, snapshot_dir=tmp_path)[SECTION]["episode_rows"]) == 2
    assert publication.restore_integrity_details(second, snapshot_dir=tmp_path) == original


@pytest.mark.parametrize("bad_path", ["../detail.json", "/detail.json", "C:/detail.json", "\\\\host\\share\\detail.json", "alias.json"])
def test_reference_path_aliases_and_escapes_are_rejected(tmp_path, bad_path):
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    compact[SECTION]["_detail_reference"]["path"] = bad_path
    with pytest.raises(ValueError, match="reference_path_invalid"):
        publication.verified_detail_artifacts(compact, snapshot_dir=tmp_path)


@pytest.mark.parametrize("key,value", [
    ("contract_id", "wrong"), ("field", "other"), ("sha256", "a"),
    ("bytes", True), ("bytes", -1), ("item_count", True), ("item_count", 0),
])
def test_malformed_reference_metadata_fails_closed(tmp_path, key, value):
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    compact[SECTION]["_detail_reference"][key] = value
    with pytest.raises(ValueError, match="reference_invalid"):
        publication.restore_integrity_details(compact, snapshot_dir=tmp_path)


def test_missing_or_changed_artifact_is_never_repaired_by_reader(tmp_path):
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    artifact = publication.verified_detail_artifacts(compact, snapshot_dir=tmp_path)[0]
    artifact.write_bytes(b"corrupted immutable evidence")
    with pytest.raises(ValueError, match="hash_or_size_mismatch"):
        publication.restore_integrity_details(compact, snapshot_dir=tmp_path)
    assert artifact.read_bytes() == b"corrupted immutable evidence"
    artifact.unlink()
    with pytest.raises(FileNotFoundError):
        publication.restore_integrity_details(compact, snapshot_dir=tmp_path)
    assert not artifact.exists()


def test_publisher_does_not_overwrite_existing_corrupt_immutable_name(tmp_path):
    original = _payload()
    compact = publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    artifact = publication.verified_detail_artifacts(compact, snapshot_dir=tmp_path)[0]
    artifact.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="hash_or_size_mismatch"):
        publication.compact_integrity_payload(original, snapshot_dir=tmp_path)
    assert artifact.read_bytes() == b"corrupt"
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("wrong_envelope", [
    {"schema_version": "wrong", "episode_rows": [{}, {}]},
    {"schema_version": publication.DETAIL_CONTRACT, "episode_rows": [{}]},
])
def test_valid_hash_cannot_hide_wrong_schema_or_row_count(tmp_path, wrong_envelope):
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    encoded = json.dumps(wrong_envelope).encode()
    digest = hashlib.sha256(encoded).hexdigest()
    reference = compact[SECTION]["_detail_reference"]
    reference.update(sha256=digest, bytes=len(encoded), path=publication._relative_artifact_path(digest))
    destination = tmp_path / reference["path"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(encoded)
    with pytest.raises(ValueError, match="schema_or_count_invalid"):
        publication.restore_integrity_details(compact, snapshot_dir=tmp_path)


def test_publication_metadata_must_list_all_and_only_referenced_sections(tmp_path):
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    compact["_detail_publication"]["section_keys"] = []
    with pytest.raises(ValueError, match="publication_contract_invalid"):
        publication.verified_detail_artifacts(compact, snapshot_dir=tmp_path)


def test_null_publication_metadata_is_invalid_not_a_legacy_snapshot(tmp_path):
    payload = _payload()
    payload["_detail_publication"] = None
    with pytest.raises(ValueError, match="publication_contract_invalid"):
        publication.restore_integrity_details(payload, snapshot_dir=tmp_path)


def test_artifact_capture_returns_same_verified_bytes_without_reopening(tmp_path, monkeypatch):
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    artifact = publication.verified_detail_artifacts(compact, snapshot_dir=tmp_path)[0]
    real_read = Path.read_bytes
    reads = []
    def changing_read(path):
        value = real_read(path)
        if path == artifact:
            reads.append(path)
            path.write_bytes(b"changed after captured read")
        return value
    monkeypatch.setattr(Path, "read_bytes", changing_read)
    captured = publication.verified_detail_artifact_bytes(compact, snapshot_dir=tmp_path)
    assert reads == [artifact]
    assert hashlib.sha256(captured[artifact]).hexdigest() == compact[SECTION]["_detail_reference"]["sha256"]


def test_symlink_escape_rejected_when_platform_supports_symlinks(tmp_path):
    base, outside = tmp_path / "snapshot", tmp_path / "outside"
    base.mkdir()
    outside.mkdir()
    try:
        os.symlink(outside, base / publication.DETAIL_DIRECTORY, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"platform symlink creation unavailable: {type(error).__name__}")
    with pytest.raises(ValueError, match="escapes_snapshot_directory"):
        publication.compact_integrity_payload(_payload(), snapshot_dir=base)
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("rows", [None, [], "invalid rows"])
def test_empty_or_malformed_rows_remain_inline_for_existing_diagnostics(tmp_path, rows):
    original = _payload()
    original[SECTION]["episode_rows"] = rows
    assert publication.compact_integrity_payload(original, snapshot_dir=tmp_path) == original
    assert list(tmp_path.iterdir()) == []


def test_current_and_history_are_compact_and_exactly_restorable(tmp_path):
    from trad import oanda_project_integrity_audit as audit
    guard, output, report, history = (tmp_path / name for name in ("guard.sqlite", "current.json", "report.md", "logs/history.jsonl"))
    payload = _payload()
    owner = audit._claim_audit_publication(guard)
    assert audit._publish_owned_audit(guard=guard, ownership=owner, output=output, report=report, history=history, payload=payload, report_text="fixture report")
    current = json.loads(output.read_bytes())
    historical = json.loads(history.read_bytes())
    assert current == historical
    assert publication.restore_integrity_details(current, snapshot_dir=output.parent) == payload
    # The history's directory is intentionally not the reference base.
    with pytest.raises(FileNotFoundError):
        publication.restore_integrity_details(historical, snapshot_dir=history.parent)
    assert payload == _payload()


def test_stale_owner_cannot_create_artifacts_or_publish(tmp_path, monkeypatch):
    from trad import oanda_project_integrity_audit as audit
    guard = tmp_path / "guard.sqlite"
    older = audit._claim_audit_publication(guard)
    audit._claim_audit_publication(guard)
    def forbidden(*args, **kwargs):
        raise AssertionError("stale publisher must not prepare artifacts")
    monkeypatch.setattr(audit, "compact_integrity_payload", forbidden)
    assert not audit._publish_owned_audit(guard=guard, ownership=older, output=tmp_path/"current.json", report=tmp_path/"report.md", history=tmp_path/"history.jsonl", payload=_payload(), report_text="fixture")


def test_detail_failure_precedes_every_current_report_history_mutation(tmp_path, monkeypatch):
    from trad import oanda_project_integrity_audit as audit
    output, report, history = (tmp_path/name for name in ("current.json", "report.md", "history.jsonl"))
    for path in (output, report, history):
        path.write_bytes(b"prior immutable test bytes")
    guard = tmp_path / "guard.sqlite"
    owner = audit._claim_audit_publication(guard)
    def unavailable(*args, **kwargs):
        raise OSError("synthetic artifact publication failure")
    monkeypatch.setattr(publication.os, "link", unavailable)
    with pytest.raises(OSError, match="synthetic artifact"):
        audit._publish_owned_audit(guard=guard, ownership=owner, output=output, report=report, history=history, payload=_payload(), report_text="fixture")
    assert all(path.read_bytes() == b"prior immutable test bytes" for path in (output, report, history))
    assert not list(tmp_path.rglob("*.tmp"))


def test_current_replace_failure_leaves_complete_unreferenced_details(tmp_path, monkeypatch):
    from trad import oanda_project_integrity_audit as audit
    output = tmp_path / "current.json"
    output.write_bytes(b"prior snapshot")
    guard = tmp_path / "guard.sqlite"
    owner = audit._claim_audit_publication(guard)
    def fail_current(*args, **kwargs):
        raise OSError("synthetic current replace failure")
    monkeypatch.setattr(audit, "atomic_json", fail_current)
    with pytest.raises(OSError, match="current replace"):
        audit._publish_owned_audit(guard=guard, ownership=owner, output=output, report=tmp_path/"report.md", history=tmp_path/"history.jsonl", payload=_payload(), report_text="fixture")
    assert output.read_bytes() == b"prior snapshot"
    compact = publication.compact_integrity_payload(_payload(), snapshot_dir=tmp_path)
    assert publication.restore_integrity_details(compact, snapshot_dir=tmp_path) == _payload()
