from __future__ import annotations

import copy
import json
import zipfile
from pathlib import Path

import pytest

from tools import audit_forex_vault_readability as subject


def fixture_vault(tmp_path: Path) -> Path:
    root = tmp_path / "vault"
    (root / "source").mkdir(parents=True)
    records = {
        "README.md": ("trad/FOREX_AUDIT_START_HERE.md", b"# Forex\n\n[Guide](SYSTEM_GUIDE.md) [Index](KNOWLEDGE_INDEX.md)\n"),
        "SYSTEM_GUIDE.md": ("trad/docs/VAULT_SYSTEM_GUIDE.md", b"# Guide\n\nNever run code to read it. `model.py`\n"),
        "OLD_REPORT.md": ("trad/docs/old.md", b"# Dated report\n\n[Model](../model.py) [Guide](VAULT_SYSTEM_GUIDE.md)\n"),
        "STATE.json": ("trad/data/state.json", b'{"as_of_utc":"2026-01-01T00:00:00Z","scope":"Dated fixture"}\n'),
        "FEATURE_DICTIONARY_CURRENT.md": ("trad/docs/features.md", b"# Feature definitions\n"),
        "RECREATION.md": ("trad/docs/recreation.md", b"# Offline recreation\n"),
    }
    source = {"model.py": b"raise RuntimeError('must never execute')\n"}
    rows = []
    for name, (path, payload) in records.items():
        (root / name).write_bytes(payload)
        rows.append({"name": name, "source": path, "size": len(payload), "sha256": subject.digest(payload)})
        if "/data/" not in path:
            source[path.removeprefix("trad/")] = payload
    source_rows = [{"path": n, "size": len(b), "sha256": subject.digest(b)} for n, b in source.items()]
    archive = root / "source/snapshot.zip"
    with zipfile.ZipFile(archive, "w") as z:
        for n, b in source.items():
            z.writestr(n, b)
    manifest = {"archive": "snapshot.zip", "archive_sha256": subject.digest(archive.read_bytes()), "files": source_rows}
    manifest_bytes = subject.encoded(manifest)
    (root / "source/snapshot.manifest.json").write_bytes(manifest_bytes)
    pointer = {"manifest": "snapshot.manifest.json", "manifest_sha256": subject.digest(manifest_bytes),
               "archive": manifest["archive"], "archive_sha256": manifest["archive_sha256"], "snapshot_id": "fixture"}
    (root / "source/WORKTREE_SOURCE_LATEST.json").write_bytes(subject.encoded(pointer))
    (root / "SHARED_PROJECT_STATE_CURRENT.json").write_bytes(subject.encoded({"records": rows, "record_count": len(rows), "generated_utc": "2026-01-01T00:00:01Z"}))
    return root


def test_complete_vault_audits_without_executing_source(tmp_path):
    report = subject.audit(fixture_vault(tmp_path))
    assert report["status"] == "pass_with_declared_external_dependencies"
    assert report["record_count"] == 6
    assert report["source_members_verified"] == 6
    assert report["reference_classification_counts"]["source_archive_member"] == 2
    assert report["reference_classification_counts"]["vault_alias"] == 1
    assert report["record_format_counts"][".json"] == 1
    assert report["record_source_archive_relationship_counts"] == {"same_hash": 5, "not_in_source_archive": 1}
    assert "not a promise of a current observation" in " ".join(report["limits"])


def test_archived_reference_is_not_direct_navigation(tmp_path):
    root = fixture_vault(tmp_path)
    p = root / "README.md"
    p.write_text("# Start\n[Source](model.py)\n", encoding="utf-8")
    m = json.loads((root / "SHARED_PROJECT_STATE_CURRENT.json").read_bytes())
    m["records"][0].update(size=p.stat().st_size, sha256=subject.digest(p.read_bytes()))
    (root / "SHARED_PROJECT_STATE_CURRENT.json").write_bytes(subject.encoded(m))
    report = subject.audit(root)
    assert report["status"] == "navigation_repairs_required"
    assert report["navigation_errors"][0]["classification"] == "source_archive_member"


def test_source_record_version_mismatch_is_explicit(tmp_path):
    root = fixture_vault(tmp_path)
    p = root / "SYSTEM_GUIDE.md"
    p.write_text("# Guide\nNew version.\n", encoding="utf-8")
    m = json.loads((root / "SHARED_PROJECT_STATE_CURRENT.json").read_bytes())
    m["records"][1].update(size=p.stat().st_size, sha256=subject.digest(p.read_bytes()))
    (root / "SHARED_PROJECT_STATE_CURRENT.json").write_bytes(subject.encoded(m))
    report = subject.audit(root)
    assert report["status"] == "source_export_outdated"
    assert report["source_export_mismatches"] == ["SYSTEM_GUIDE.md"]


@pytest.mark.parametrize("kind", ["record", "pointer", "archive"])
def test_tampering_rejected(tmp_path, kind):
    root = fixture_vault(tmp_path)
    if kind == "record":
        (root / "STATE.json").write_bytes(b"{}")
    elif kind == "archive":
        with (root / "source/snapshot.zip").open("ab") as f:
            f.write(b"extra")
    else:
        (root / "source/snapshot.manifest.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="hash mismatch"):
        subject.audit(root)


@pytest.mark.parametrize("name", ["../secret", "C:/secret", "/absolute", "x/../../secret", "x\\..\\secret"])
def test_unsafe_names_rejected(name):
    with pytest.raises(ValueError):
        subject.safe_relative(name)


def test_dangling_generated_output_symlink_rejected(tmp_path):
    root = fixture_vault(tmp_path)
    outside = tmp_path / "outside.json"
    try:
        (root / subject.REPORT).symlink_to(outside)
    except OSError:
        pytest.skip("This host does not permit test symlink creation")
    with pytest.raises(ValueError, match="redirected"):
        subject.checked_output(root, subject.REPORT)
    assert not outside.exists()


def test_generated_path_must_be_direct_regular_child(tmp_path):
    root = fixture_vault(tmp_path)
    with pytest.raises(ValueError, match="direct vault child"):
        subject.checked_output(root, "source/new.json")
    (root / subject.REPORT).mkdir()
    with pytest.raises(ValueError, match="nonregular"):
        subject.checked_output(root, subject.REPORT)


def test_redirected_output_rejected_without_host_symlink_privilege():
    class Link:
        def lstat(self):
            return type("Metadata", (), {"st_reparse_tag": 0})()

        def is_symlink(self):
            return True

    class Root:
        def __truediv__(self, name):
            return Link()

    with pytest.raises(ValueError, match="redirected"):
        subject.checked_output(Root(), subject.REPORT)


def test_generated_header_destinations_are_required(tmp_path):
    root = fixture_vault(tmp_path)
    path = root / "SHARED_PROJECT_STATE_CURRENT.json"
    m = json.loads(path.read_bytes())
    m["records"] = [r for r in m["records"] if r["name"] != "FEATURE_DICTIONARY_CURRENT.md"]
    m["record_count"] -= 1
    path.write_bytes(subject.encoded(m))
    report = subject.audit(root)
    assert report["status"] == "navigation_repairs_required"
    assert any(r["target"] == "FEATURE_DICTIONARY_CURRENT.md" for r in report["navigation_errors"])


def test_generated_record_cannot_make_a_self_referential_manifest(tmp_path):
    root = fixture_vault(tmp_path)
    path = root / "SHARED_PROJECT_STATE_CURRENT.json"
    m = json.loads(path.read_bytes())
    m["records"][0]["name"] = subject.REPORT
    path.write_bytes(subject.encoded(m))
    with pytest.raises(ValueError, match="outside the canonical manifest"):
        subject.audit(root)


def test_supplemental_inventory_rejects_a_redirected_parent_without_read(tmp_path, monkeypatch):
    root = fixture_vault(tmp_path)
    folder = root / "maintenance"
    folder.mkdir()
    target = folder / "README.md"
    target.write_text("history", encoding="utf-8")
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda p: p == folder or original(p))
    assert not subject.safe_inventory_file(root, target)


def test_external_machine_paths_never_read():
    for name in ["D:/forex/trad/model.py", "C:/Users/x/data.sqlite", "file:///secret.json"]:
        assert subject.resolve_reference(name, "README.md", {}, set(), set())["classification"] == "machine_local_external_not_read"


def test_source_paths_alias_and_zip_root():
    sources = {"NEWS_CURRENT.md": "trad/docs/news.md", "REPORT.md": "trad/docs/report.md"}
    for reference in ["news.md", "docs/news.md", "C:/Users/zmoor/Documents/forex/trad/docs/news.md"]:
        assert subject.resolve_reference(reference, "REPORT.md", sources, set(), set())["resolved"] == "NEWS_CURRENT.md"
    assert subject.resolve_reference("../model.py:12", "REPORT.md", sources, {"model.py"}, set())["resolved"] == "model.py"


def test_fences_not_confused_with_links_and_anchor_duplicates():
    text = "# A/B\n## A/B\n```md\n[not a citation](missing.md)\n```\n[yes](ok.md)"
    assert subject.anchors(text) == {"ab", "ab-1"}
    assert len(subject.LINK.findall(subject.without_fences(text))) == 1


def test_invalid_json_not_called_readable(tmp_path):
    root = fixture_vault(tmp_path)
    p = root / "STATE.json"
    p.write_text("{bad}", encoding="utf-8")
    m = json.loads((root / "SHARED_PROJECT_STATE_CURRENT.json").read_bytes())
    m["records"][3].update(size=p.stat().st_size, sha256=subject.digest(p.read_bytes()))
    (root / "SHARED_PROJECT_STATE_CURRENT.json").write_bytes(subject.encoded(m))
    with pytest.raises(json.JSONDecodeError):
        subject.audit(root)


def test_output_determinism_and_readable_alias(tmp_path):
    root = fixture_vault(tmp_path)
    first = subject.audit(root)
    assert subject.encoded(first) == subject.encoded(subject.audit(root))
    rendered = subject.render(first)
    assert "[SYSTEM_GUIDE.md](SYSTEM_GUIDE.md)" in rendered
    assert "Source ZIP member `model.py`" in rendered
    assert "not a complete historical-data backup" in rendered
