from __future__ import annotations

import hashlib
import json
import os
import zipfile
from pathlib import Path

import pytest

import forex_vault_import as vault_import


def _archive(path: Path, files: dict[str, bytes], manifest_name: str) -> Path:
    rows = [
        {
            "path": name.replace("\\", "/"),
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        for name, payload in sorted(files.items())
    ]
    manifest = {
        "schema_version": 1,
        "credential_free": True,
        "content_sha256": "fixture",
        "files": rows,
    }
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(manifest_name, json.dumps(manifest))
        for name, payload in files.items():
            archive.writestr(name, payload)
    return path


def test_imports_base_and_overlay_with_final_hash_verification(tmp_path: Path) -> None:
    base = _archive(
        tmp_path / "base.zip",
        {"trad/runtime.py": b"old", "docs/readme.md": b"base"},
        "MODEL_CHECKPOINT_MANIFEST.json",
    )
    overlay = _archive(
        tmp_path / "overlay.zip",
        {"trad/runtime.py": b"new", "audit/result.json": b"{}"},
        "MANIFEST.json",
    )
    destination = tmp_path / "restored"

    report = vault_import.import_archives([base, overlay], destination)

    assert report["status"] == "imported_and_verified"
    assert (destination / "trad/runtime.py").read_bytes() == b"new"
    assert (destination / "docs/readme.md").read_bytes() == b"base"
    assert (destination / "VAULT_IMPORT_RECEIPT.json").is_file()


def test_verify_only_does_not_create_destination(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path / "base.zip",
        {"trad/runtime.py": b"ok"},
        "MODEL_CHECKPOINT_MANIFEST.json",
    )
    destination = tmp_path / "unused"

    report = vault_import.import_archives([archive], destination, verify_only=True)

    assert report["status"] == "verified"
    assert not destination.exists()


def test_rejects_path_traversal_before_extraction(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path / "escape.zip",
        {"../outside.txt": b"no"},
        "MODEL_CHECKPOINT_MANIFEST.json",
    )
    with pytest.raises(vault_import.VaultImportError, match="unsafe archive path"):
        vault_import.audit_archive(archive)
    assert not (tmp_path / "outside.txt").exists()


def test_rejects_tampered_manifested_content(tmp_path: Path) -> None:
    path = tmp_path / "tampered.zip"
    manifest = {
        "schema_version": 1,
        "files": [
            {
                "path": "trad/runtime.py",
                "size": 3,
                "sha256": hashlib.sha256(b"old").hexdigest(),
            }
        ],
    }
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("MODEL_CHECKPOINT_MANIFEST.json", json.dumps(manifest))
        archive.writestr("trad/runtime.py", b"new")

    with pytest.raises(vault_import.VaultImportError, match="content hash mismatch"):
        vault_import.audit_archive(path)


def test_rejects_credential_like_member(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path / "secret.zip",
        {"trad/creds": b"token"},
        "MODEL_CHECKPOINT_MANIFEST.json",
    )
    with pytest.raises(vault_import.VaultImportError, match="credential-like"):
        vault_import.audit_archive(archive)


@pytest.mark.skipif(os.name != "nt", reason="Windows extended-path regression")
def test_import_supports_windows_extended_paths(tmp_path: Path) -> None:
    relative = "/".join(["deep_segment_1234567890"] * 9) + "/result.json"
    archive = _archive(
        tmp_path / "long.zip",
        {relative: b"{}"},
        "MODEL_CHECKPOINT_MANIFEST.json",
    )
    destination = tmp_path / "nested_destination_1234567890" / "restored"

    report = vault_import.import_archives([archive], destination)

    assert report["status"] == "imported_and_verified"
    assert vault_import.io_path(destination / Path(relative)).is_file()
