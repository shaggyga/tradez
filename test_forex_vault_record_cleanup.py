from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import forex_vault_record_cleanup as cleanup


@pytest.fixture
def scoped_paths(tmp_path, monkeypatch):
    project = tmp_path / "vault" / "projects" / "forex"
    archive = tmp_path / "trad" / "artifacts" / "vault_cleanup"
    project.mkdir(parents=True)
    (project / cleanup.CURRENT_MANIFEST).write_text(json.dumps({
        "records": [{"name": "KEPT_CURRENT.json", "source": "local.json", "size": 2}],
        "record_count": 1,
    }), encoding="utf-8")
    (project / "KEPT_CURRENT.json").write_bytes(b"{}")
    (project / "README.md").write_bytes(b"old orientation\n")
    (project / "source").mkdir()
    (project / "source" / "CURRENT.zip").write_bytes(b"source")
    other = project.parent / "other_project"
    other.mkdir()
    (other / "keep.txt").write_bytes(b"other project")
    monkeypatch.setattr(cleanup, "FOREX_VAULT_PROJECT", project)
    monkeypatch.setattr(cleanup, "ARCHIVE_ROOT", archive)
    return project, archive


def inventory(path):
    return {
        item.relative_to(path).as_posix(): None if item.is_dir() else item.read_bytes()
        for item in path.rglob("*")
    }


@pytest.mark.parametrize("name", [
    "../other_project", "projects/forex", r"projects\forex", ".", "..",
    "C:\\temp", "legacy.json:stream", "legacy.json.", "legacy.json ", "NUL",
])
def test_rejects_traversal_or_unsafe_names(scoped_paths, name):
    project, archive = scoped_paths
    with pytest.raises(ValueError, match="safe direct-child"):
        cleanup.cleanup(project, archive, "test", [name])
    assert not archive.exists()


@pytest.mark.parametrize("name", [
    "source", "README.md", "AUDIT_START_HERE.md", "RECREATION.md", "maintenance",
    "KEPT_CURRENT.json", "kept_current.JSON", cleanup.CURRENT_MANIFEST,
    "RUNTIME_STATUS_CURRENT.json", "SOURCE_FEATURE_INDEX_CURRENT.json",
])
def test_protects_source_and_current_records(scoped_paths, name):
    project, archive = scoped_paths
    with pytest.raises(ValueError, match="protected"):
        cleanup.cleanup(project, archive, "test", [name], apply=True)
    assert not archive.exists()
    assert (project / "source" / "CURRENT.zip").read_bytes() == b"source"


def test_rejects_archive_anywhere_inside_vault(scoped_paths):
    project, _ = scoped_paths
    with pytest.raises(RuntimeError, match="outside"):
        cleanup.cleanup(project, project.parent.parent / "archive", "test", ["old.txt"])


def test_requires_exact_canonical_project_and_archive(scoped_paths, tmp_path):
    project, archive = scoped_paths
    other = project.parent / "other_project"
    with pytest.raises(RuntimeError, match="exact canonical Forex"):
        cleanup.cleanup(other, archive, "test", ["keep.txt"])
    with pytest.raises(RuntimeError, match="exact canonical local"):
        cleanup.cleanup(project, tmp_path / "elsewhere", "test", ["old.txt"])


@pytest.mark.parametrize("run_id", ["..", "a/b", "a\\b", "a:b", "NUL", "x."])
def test_rejects_unsafe_run_ids(scoped_paths, run_id):
    project, archive = scoped_paths
    with pytest.raises(ValueError):
        cleanup.cleanup(project, archive, run_id, ["old.txt"])


def test_requires_explicit_nonduplicated_reviewed_targets(scoped_paths):
    project, archive = scoped_paths
    (project / "unknown").mkdir()
    with pytest.raises(ValueError, match="explicit"):
        cleanup.cleanup(project, archive, "test", [])
    with pytest.raises(ValueError, match="duplicate"):
        cleanup.cleanup(project, archive, "test", ["old.txt", "OLD.TXT"])
    with pytest.raises(ValueError, match="unreviewed directory"):
        cleanup.cleanup(project, archive, "test", ["unknown"])


def test_plan_does_not_write_and_apply_preserves_integrity(scoped_paths, monkeypatch):
    project, archive = scoped_paths
    for name in cleanup.ALLOWED_DIRECTORIES:
        (project / name / "empty" / "nested").mkdir(parents=True)
        (project / name / "record.bin").write_bytes(name.encode() + b"\x00\xff")
    (project / "old.json").write_bytes(b"legacy json")
    names = [*sorted(cleanup.ALLOWED_DIRECTORIES), "old.json"]
    before = inventory(project.parent.parent)
    plan = cleanup.cleanup(project, archive, "test", names)
    assert plan["status"] == "planned"
    assert not archive.exists()
    assert before == inventory(project.parent.parent)
    original_rename = Path.rename
    moved = []

    def checked_rename(source, target):
        run = archive / "test"
        assert (run / "plan.json").is_file()
        events = [json.loads(item.read_text()) for item in (run / "journal").glob("*.json")]
        assert events[0]["status"] == "prepared"
        assert any(event.get("source") == str(source) for event in events)
        moved.append(source.name)
        return original_rename(source, target)

    monkeypatch.setattr(Path, "rename", checked_rename)
    receipt = cleanup.cleanup(project, archive, "test", names, apply=True)
    assert receipt["status"] == "completed_verified"
    assert moved == names
    assert receipt["moved_files"] == 7
    run = archive / "test"
    assert (run / "preserved" / "README.md").read_bytes() == b"old orientation\n"
    assert (project / "README.md").read_bytes() == b"old orientation\n"
    for target in receipt["targets"]:
        restored = run / "payload" / target["name"]
        assert cleanup.scan_target(restored) == target
        assert not (project / target["name"]).exists()
    assert (run / "payload" / "artifacts" / "empty" / "nested").is_dir()
    assert json.loads((run / "plan.json").read_text())["status"] == "planned"
    assert json.loads((run / "receipt.json").read_text()) == receipt
    for path, content in before.items():
        if path.startswith("projects/other_project/") or path.startswith("projects/forex/source/"):
            assert (project.parent.parent / path).read_bytes() == content
    assert (project / "KEPT_CURRENT.json").read_bytes() == b"{}"
    with pytest.raises(FileExistsError, match="already exists"):
        cleanup.cleanup(project, archive, "test", ["not-there.txt"], apply=True)


def test_missing_or_invalid_manifest_fails_closed(scoped_paths):
    project, archive = scoped_paths
    (project / cleanup.CURRENT_MANIFEST).write_text('{"records": []}', encoding="utf-8")
    with pytest.raises(ValueError, match="manifest"):
        cleanup.cleanup(project, archive, "test", ["legacy.json"], apply=True)
    assert not archive.exists()


def test_symlink_tree_is_rejected_without_following(scoped_paths, tmp_path):
    project, archive = scoped_paths
    (project / "artifacts").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (project / "artifacts" / "redirect").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable under current Windows privileges")
    with pytest.raises(RuntimeError, match="symlink/junction"):
        cleanup.cleanup(project, archive, "test", ["artifacts"], apply=True)
    assert not archive.exists()


def test_interrupted_move_is_recoverable_and_journaled(scoped_paths, monkeypatch):
    project, archive = scoped_paths
    (project / "old.txt").write_bytes(b"original")

    def refuse_move(source, target):
        raise OSError("simulated rename failure")

    monkeypatch.setattr(Path, "rename", refuse_move)
    with pytest.raises(OSError, match="simulated"):
        cleanup.cleanup(project, archive, "test", ["old.txt"], apply=True)
    assert (project / "old.txt").read_bytes() == b"original"
    journal = sorted((archive / "test" / "journal").glob("*.json"))
    assert json.loads(journal[-1].read_text())["status"] == "interrupted-recoverable"
    assert not (archive / "test" / "receipt.json").exists()


def test_current_manifest_drift_stops_before_any_move(scoped_paths, monkeypatch):
    project, archive = scoped_paths
    (project / "old.txt").write_bytes(b"original")
    original_write = cleanup._write_once

    def change_manifest_after_plan(path, payload):
        result = original_write(path, payload)
        if path.name == "plan.json":
            with (project / cleanup.CURRENT_MANIFEST).open("a") as handle:
                handle.write("\n")
        return result

    monkeypatch.setattr(cleanup, "_write_once", change_manifest_after_plan)
    with pytest.raises(RuntimeError, match="manifest changed"):
        cleanup.cleanup(project, archive, "test", ["old.txt"], apply=True)
    assert (project / "old.txt").read_bytes() == b"original"
    assert not (archive / "test" / "payload" / "old.txt").exists()


def test_post_move_hash_mismatch_never_publishes_completed_receipt(scoped_paths, monkeypatch):
    project, archive = scoped_paths
    (project / "old.txt").write_bytes(b"original")
    original_scan = cleanup.scan_target

    def report_bad_destination_hash(path):
        result = original_scan(path)
        if path.parent.name == "payload":
            result["tree_sha256"] = "0" * 64
        return result

    monkeypatch.setattr(cleanup, "scan_target", report_bad_destination_hash)
    with pytest.raises(RuntimeError, match="integrity verification"):
        cleanup.cleanup(project, archive, "test", ["old.txt"], apply=True)
    assert (archive / "test" / "payload" / "old.txt").read_bytes() == b"original"
    assert not (archive / "test" / "receipt.json").exists()
    journal = sorted((archive / "test" / "journal").glob("*.json"))
    assert json.loads(journal[-1].read_text())["status"] == "interrupted-recoverable"


def test_onedrive_cloud_reparse_allowed_but_junction_rejected(monkeypatch, tmp_path):
    from types import SimpleNamespace
    import stat

    path = tmp_path / "node"
    for tag, is_cloud in [(0x9000001A, True), (0x9000501A, True), (0xA0000003, False)]:
        info = SimpleNamespace(st_mode=stat.S_IFREG, st_file_attributes=0x400, st_reparse_tag=tag)
        monkeypatch.setattr(Path, "lstat", lambda self: info)
        if is_cloud:
            assert cleanup._safe_node(path) is info
        else:
            with pytest.raises(RuntimeError, match="symlink/junction"):
                cleanup._safe_node(path)


def test_deep_tree_plan_and_move_preserve_lexical_inventory(scoped_paths):
    project, archive = scoped_paths
    relative = Path("artifacts") / ("model_gap_" + "a" * 85) / ("backtest_" + "b" * 85) / ("cohort_" + "c" * 60) / "model.pkl"
    member = project / relative
    assert len(str(member)) > 260
    cleanup.io_path(member.parent).mkdir(parents=True)
    payload = b"deep immutable model payload\x00\xff"
    cleanup.io_path(member).write_bytes(payload)
    plan = cleanup.cleanup(project, archive, "deep_test", ["artifacts"])
    assert plan["moved_files"] == 1
    assert not archive.exists()
    assert plan["targets"][0]["files"][0]["path"] == relative.as_posix()
    assert "\\\\?\\" not in json.dumps(plan)
    receipt = cleanup.cleanup(project, archive, "deep_test", ["artifacts"], apply=True)
    recovered = archive / "deep_test" / "payload" / relative
    assert cleanup.io_path(recovered).read_bytes() == payload
    assert cleanup.scan_target(archive / "deep_test/payload/artifacts") == plan["targets"][0]
    assert receipt["status"] == "completed_verified"
    assert not (project / "artifacts").exists()
    assert "\\\\?\\" not in json.dumps(receipt)


def test_deep_io_uses_extended_prefix_without_resolving(monkeypatch, tmp_path):
    if cleanup.os.name != "nt":
        pytest.skip("Windows extended paths")
    path = tmp_path / ("x" * 100) / ("y" * 100) / "member.bin"
    monkeypatch.setattr(Path, "resolve", lambda *args, **kwargs: pytest.fail("I/O spelling must not resolve redirects"))
    result = cleanup.io_path(path)
    assert str(result).startswith("\\\\?\\")
    assert str(result)[4:] == str(path.absolute())
