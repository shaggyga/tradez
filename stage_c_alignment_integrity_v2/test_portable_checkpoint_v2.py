"""Adversarial checkpoint validation plus a real relocated subprocess replay."""
from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from portable_checkpoint_v2 import (
    MANIFEST, SOURCE_ALLOWLIST, check_dependencies, digest, encoded, environment_lock,
    export_checkpoint, inspect_package, restore_checkpoint, safe_member,
)
from publication import sha256_file


@pytest.mark.parametrize("name", [
    "../escape.py", "/root.py", "C:/escape.py", "folder\\escape.py", "file.py:stream",
    "./file.py", "folder//file.py", "NUL.txt", "con", "path/COM1.py", "LPT9.txt",
    "folder./file.py", "trailing.", "bad\x00.py", "space name.py", "",
])
def test_tst52_rejects_nonportable_or_escaping_zip_names(name):
    with pytest.raises(ValueError, match="unsafe"):
        safe_member(name)


def test_tst52_accepts_contained_portable_name():
    assert safe_member("source/portable_checkpoint_v2.py").as_posix() == "source/portable_checkpoint_v2.py"


def test_tst52_dependency_drift_is_explicit():
    lock = environment_lock()
    lock["packages"]["numpy"] = "0.0.0"
    with pytest.raises(ValueError, match="dependency lock mismatch"):
        check_dependencies(lock)


@pytest.fixture(scope="module")
def completed_package(tmp_path_factory):
    root = tmp_path_factory.mktemp("portable-build")
    package = root / "portable-v2.zip"
    receipt = export_checkpoint(ROOT, package)
    return package, receipt


def test_tst52_real_package_replays_in_unrelated_root(completed_package, tmp_path):
    package, receipt = completed_package
    destination = tmp_path / "unrelated-restore-root"
    restored = restore_checkpoint(package, destination, receipt["sha256"])
    assert restored["subprocess_replay_verified"] is True
    assert restored["dependency_lock_verified"] is True
    assert restored["replay"]["coverage_count"] == 68
    assert restored["replay"]["eligible_count"] == 68
    assert restored["replay"] == receipt["expected_replay"]
    assert (destination / "RESTORE_RECEIPT.json").is_file()
    assert {path.name for path in (destination / "source").iterdir()} == set(SOURCE_ALLOWLIST)
    assert (destination / "requirements.lock").is_file()
    refs = json.loads((destination / "DATASET_REFERENCES.json").read_text(encoding="utf-8"))
    assert refs["bulk_data_included"] is False


def test_tst52_export_does_not_replace_published_checkpoint(completed_package):
    package, receipt = completed_package
    with pytest.raises(FileExistsError, match="immutable"):
        export_checkpoint(ROOT, package)
    assert sha256_file(package) == receipt["sha256"]


def test_tst52_restore_refuses_wrong_external_hash_before_writing(completed_package, tmp_path):
    package, _ = completed_package
    destination = tmp_path / "must-not-exist"
    with pytest.raises(ValueError, match="external checkpoint SHA-256 mismatch"):
        restore_checkpoint(package, destination, "0" * 64)
    assert not destination.exists()


def test_tst52_restore_preserves_nonempty_destination(completed_package, tmp_path):
    package, receipt = completed_package
    destination = tmp_path / "occupied"
    destination.mkdir()
    marker = destination / "existing.txt"
    marker.write_text("preserve", encoding="utf-8")
    with pytest.raises(FileExistsError, match="empty"):
        restore_checkpoint(package, destination, receipt["sha256"])
    assert marker.read_text(encoding="utf-8") == "preserve"


def rewrite_zip(source, target, mutate):
    with zipfile.ZipFile(source) as archive:
        contents = [(info.filename, archive.read(info)) for info in archive.infolist()]
    changed = mutate(contents)
    with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in changed:
            info = zipfile.ZipInfo(name)
            # Preserve the adversarial on-disk name; Windows ZipInfo otherwise
            # normalizes backslashes before the restore parser can see them.
            info.filename = info.orig_filename = name
            archive.writestr(info, data)


def test_tst52_internal_hash_detects_changed_payload_even_with_new_external_hash(completed_package, tmp_path):
    package, _ = completed_package
    altered = tmp_path / "altered.zip"
    rewrite_zip(package, altered, lambda rows: [(name, data + b"tamper" if name == "source/contracts.py" else data) for name, data in rows])
    with pytest.raises(ValueError, match="member hash or size mismatch"):
        inspect_package(altered, sha256_file(altered))


@pytest.mark.parametrize("extra", ["../outside.py", "source\\contracts.py", "source/contracts.py:ads", "source/CON.txt"])
def test_tst52_hostile_members_rejected_before_restore(completed_package, tmp_path, extra):
    package, _ = completed_package
    altered = tmp_path / "hostile.zip"
    rewrite_zip(package, altered, lambda rows: [*rows[:-1], (extra, b"bad"), rows[-1]])
    destination = tmp_path / "never-created"
    with pytest.raises(ValueError, match="unsafe"):
        restore_checkpoint(altered, destination, sha256_file(altered))
    assert not destination.exists()


def test_tst52_case_alias_collision_is_rejected(completed_package, tmp_path):
    package, _ = completed_package
    altered = tmp_path / "case-alias.zip"
    rewrite_zip(package, altered, lambda rows: [*rows[:-1], ("SOURCE/contracts.py", b"alias"), rows[-1]])
    with pytest.raises(ValueError, match="duplicate"):
        inspect_package(altered, sha256_file(altered))


def test_tst52_completion_manifest_must_be_last(completed_package, tmp_path):
    package, _ = completed_package
    altered = tmp_path / "incomplete.zip"
    rewrite_zip(package, altered, lambda rows: [rows[-1], *rows[:-1]])
    with pytest.raises(ValueError, match="manifest is missing or not last"):
        inspect_package(altered, sha256_file(altered))
