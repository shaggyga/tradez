from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import vault_record_consolidation as consolidation


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_consolidation_moves_only_reviewed_roots_and_is_hash_verified(tmp_path) -> None:
    vault = tmp_path / "vault"
    cold = tmp_path / "cold"
    vault.mkdir()
    _write(vault / "README.md", b"legacy orientation\n")
    _write(vault / "UNIFIED_QUERY_VAULT" / "database" / "vault.sqlite", b"db")
    _write(vault / "GPT_VAULT_COMPLETE.zip", b"zip")
    _write(vault / "projects" / "forex" / "CURRENT.json", b"{}")

    receipt = consolidation.consolidate(
        vault,
        cold,
        "test_run",
        ["UNIFIED_QUERY_VAULT", "GPT_VAULT_COMPLETE.zip"],
        apply=True,
    )

    assert receipt["status"] == "completed_verified"
    assert receipt["recoverable"] is True
    assert receipt["moved_files"] == 2
    assert not (vault / "UNIFIED_QUERY_VAULT").exists()
    assert not (vault / "GPT_VAULT_COMPLETE.zip").exists()
    assert (cold / "test_run" / "UNIFIED_QUERY_VAULT").is_dir()
    assert (cold / "test_run" / "GPT_VAULT_COMPLETE.zip").is_file()
    assert (vault / "projects" / "forex" / "CURRENT.json").is_file()
    assert (cold / "test_run" / "LEGACY_VAULT_README.md").is_file()
    current = json.loads(
        (
            vault
            / "maintenance"
            / "consolidation"
            / "VAULT_RECORD_CONSOLIDATION_CURRENT.json"
        ).read_text(encoding="utf-8")
    )
    assert current["receipt_sha256"] == receipt["receipt_sha256"]
    assert (vault / "COLD_ARCHIVE_POINTERS_CURRENT.md").is_file()


def test_consolidation_rejects_arbitrary_or_nested_targets(tmp_path) -> None:
    vault = tmp_path / "vault"
    cold = tmp_path / "cold"
    vault.mkdir()
    _write(vault / "projects" / "forex" / "CURRENT.json", b"{}")

    with pytest.raises(ValueError, match="unreviewed"):
        consolidation.consolidate(
            vault,
            cold,
            "test_run",
            ["projects"],
            apply=False,
        )


def test_consolidation_requires_cold_archive_outside_vault(tmp_path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    _write(vault / "GPT_VAULT_COMPLETE.zip", b"zip")

    with pytest.raises(RuntimeError, match="outside"):
        consolidation.consolidate(
            vault,
            vault / "cold",
            "test_run",
            ["GPT_VAULT_COMPLETE.zip"],
            apply=False,
        )
