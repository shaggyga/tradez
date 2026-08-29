from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from trad.forex_git_source_vault_sync import sync_source_baseline


def run(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


def output(root: Path, *args: str, input_text: str | None = None) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        input=input_text,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_source_sync_binds_clean_exact_git_tree_without_retention(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    root.mkdir()
    run(root, "init")
    run(root, "config", "user.name", "Test")
    run(root, "config", "user.email", "test@example.invalid")
    (root / "pkg").mkdir()
    (root / "pkg" / "model.py").write_text("print('safe')\n", encoding="utf-8")
    run(root, "add", "pkg/model.py")
    run(root, "commit", "-m", "baseline")
    result = sync_source_baseline(root, vault)
    assert result["tracked_file_count"] == 1
    assert result["zip_crc_verified"] is True
    assert Path(result["archive"]).is_file()
    assert (vault / "projects/forex/source/SOURCE_BASELINE_LATEST.json").is_file()

    (root / "untracked.txt").write_text("dirty", encoding="utf-8")
    with pytest.raises(RuntimeError, match="clean Git worktree"):
        sync_source_baseline(root, vault)


def test_source_sync_rejects_tracked_symlink(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    root.mkdir()
    run(root, "init")
    run(root, "config", "user.name", "Test")
    run(root, "config", "user.email", "test@example.invalid")
    (root / "target.md").write_text("safe\n", encoding="utf-8")
    run(root, "add", "target.md")
    run(root, "commit", "-m", "target")
    blob = output(root, "hash-object", "-w", "--stdin", input_text="target.md")
    run(root, "update-index", "--add", "--cacheinfo", f"120000,{blob},current.md")
    run(root, "commit", "-m", "symlink")
    # With core.symlinks=false, Git represents the link as a regular file whose
    # bytes are the link target; this keeps the checkout clean on Windows.
    (root / "current.md").write_text("target.md", encoding="utf-8")
    with pytest.raises(RuntimeError, match="tracked symlinks"):
        sync_source_baseline(root, vault)


def test_current_records_are_read_from_commit_not_worktree(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    root.mkdir()
    run(root, "init")
    run(root, "config", "user.name", "Test")
    run(root, "config", "user.email", "test@example.invalid")
    record = root / "FOREX_PENDING_IMPROVEMENTS.md"
    record.write_text("committed bytes\n", encoding="utf-8")
    run(root, "add", record.name)
    run(root, "commit", "-m", "baseline")

    from trad import forex_git_source_vault_sync as module

    original_assertion = module._assert_same_clean_checkout

    def mutate_then_assert(repo: Path, commit: str) -> None:
        record.write_text("mutated bytes\n", encoding="utf-8")
        original_assertion(repo, commit)

    with patch.object(module, "_assert_same_clean_checkout", side_effect=mutate_then_assert):
        with pytest.raises(RuntimeError, match="worktree changed"):
            sync_source_baseline(root, vault)
    assert not (vault / "projects/forex/source/forex_source_current.zip").exists()


def test_current_record_bytes_remain_commit_bound_after_check(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    root.mkdir()
    run(root, "init")
    run(root, "config", "user.name", "Test")
    run(root, "config", "user.email", "test@example.invalid")
    record = root / "FOREX_PENDING_IMPROVEMENTS.md"
    record.write_text("committed bytes\n", encoding="utf-8")
    run(root, "add", record.name)
    run(root, "commit", "-m", "baseline")

    from trad import forex_git_source_vault_sync as module

    def mutate_after_check(_repo: Path, _commit: str) -> None:
        record.write_text("unaudited worktree bytes\n", encoding="utf-8")

    with patch.object(module, "_assert_same_clean_checkout", side_effect=mutate_after_check):
        sync_source_baseline(root, vault)
    published = vault / "projects/forex/current_records/FOREX_PENDING_IMPROVEMENTS.md"
    assert published.read_text(encoding="utf-8") == "committed bytes\n"


def test_source_sync_rejects_force_tracked_runtime_member(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    root.mkdir()
    run(root, "init")
    run(root, "config", "user.name", "Test")
    run(root, "config", "user.email", "test@example.invalid")
    runtime = root / "data" / "live.sqlite3"
    runtime.parent.mkdir()
    runtime.write_bytes(b"runtime")
    run(root, "add", "-f", "data/live.sqlite3")
    run(root, "commit", "-m", "bad runtime")
    with pytest.raises(RuntimeError, match="runtime/private members"):
        sync_source_baseline(root, vault)
