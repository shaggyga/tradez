from __future__ import annotations

import json
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


def initialise_repo(root: Path) -> None:
    root.mkdir()
    run(root, "init")
    run(root, "config", "user.name", "Test")
    run(root, "config", "user.email", "test@example.invalid")


def commit_version(root: Path, version: int) -> str:
    source = root / "pkg" / "model.py"
    source.parent.mkdir(exist_ok=True)
    source.write_text(f"VERSION = {version}\n", encoding="utf-8")
    run(root, "add", "pkg/model.py")
    run(root, "commit", "-m", f"version {version}")
    return output(root, "rev-parse", "HEAD")


def immutable_descriptors(source: Path, suffix: str) -> set[str]:
    ending = f".{suffix}"
    return {
        path.name.removeprefix("forex_source_").removesuffix(ending)
        for path in source.iterdir()
        if path.name.startswith("forex_source_")
        and path.name.endswith(ending)
        and path.name != "forex_source_current.zip"
    }


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


def test_default_source_sync_preserves_all_immutable_baselines(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commits = []
    for version in range(3):
        commits.append(commit_version(root, version))
        result = sync_source_baseline(root, vault)

    source = vault / "projects/forex/source"
    expected = {commit[:16] for commit in commits}
    assert immutable_descriptors(source, "zip") == expected
    assert immutable_descriptors(source, "manifest.json") == expected
    assert result["retention_action"] == "none"
    assert result["retention"] == {
        "action": "none_default_no_prune",
        "enabled": False,
        "requested_keep_latest": None,
        "deleted_descriptors": [],
    }
    legacy = vault / "projects/forex/forex_source_runtime_20260715_000430.zip"
    legacy.write_bytes(b"legacy")
    result = sync_source_baseline(root, vault)
    assert legacy.read_bytes() == b"legacy"
    assert result["legacy_source_prune"]["action"] == "none_not_requested"


def test_explicit_retention_prunes_only_old_exact_pairs_and_keeps_current(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commits = []
    for version in range(4):
        commits.append(commit_version(root, version))
        sync_source_baseline(root, vault)

    project = vault / "projects/forex"
    source = project / "source"
    causal = project / "data" / "causal_event_ledger.sqlite3"
    causal.parent.mkdir()
    causal.write_bytes(b"must remain untouched")
    legacy = {
        "forex_source_checkpoint_20260731_180842.zip": b"legacy zip",
        "forex_source_checkpoint_20260731_180842.manifest.json": b"{}",
        "forex_source_checkpoint_current.zip": b"legacy current",
        "forex_source_checkpoint_current.manifest.json": b"{}",
        "forex_source_runtime_20260715_000430.zip": b"legacy runtime",
        "SOURCE_CHECKPOINT_LATEST.json": b"{}",
    }
    for name, payload in legacy.items():
        (project / name).write_bytes(payload)

    result = sync_source_baseline(root, vault, retention=2)
    expected = {commit[:16] for commit in commits[-2:]}
    assert immutable_descriptors(source, "zip") == expected
    assert immutable_descriptors(source, "manifest.json") == expected
    assert result["retention_action"] == "pruned"
    assert set(result["retention"]["retained_descriptors"]) == expected
    assert set(result["retention"]["deleted_descriptors"]) == {
        commit[:16] for commit in commits[:-2]
    }
    pointer = json.loads(
        (source / "SOURCE_BASELINE_LATEST.json").read_text(encoding="utf-8")
    )
    assert pointer["git_commit"] == commits[-1]
    assert (source / "forex_source_current.zip").read_bytes() == (
        source / f"forex_source_{commits[-1][:16]}.zip"
    ).read_bytes()
    assert causal.read_bytes() == b"must remain untouched"
    assert all((project / name).read_bytes() == payload for name, payload in legacy.items())
    legacy_report = result["legacy_source_families"]
    assert legacy_report["action"] == "identified_only_no_delete"
    assert legacy_report["artifact_count"] == len(legacy)
    assert set(legacy_report["families"]) == {
        "mixed_source_checkpoint_timestamped",
        "mixed_source_checkpoint_current",
        "pre_git_source_runtime",
        "mixed_source_checkpoint_pointer",
    }


def test_retention_fails_before_deletion_on_unpaired_or_unknown_family_name(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commits = []
    for version in range(3):
        commits.append(commit_version(root, version))
        sync_source_baseline(root, vault)
    source = vault / "projects/forex/source"
    descriptors_before = immutable_descriptors(source, "zip")

    orphan_manifest = source / f"forex_source_{commits[0][:16]}.manifest.json"
    orphan_manifest.unlink()
    with pytest.raises(RuntimeError, match="pair mismatch"):
        sync_source_baseline(root, vault, retention=1)
    assert immutable_descriptors(source, "zip") == descriptors_before
    assert (source / "forex_source_current.zip").is_file()

    orphan_manifest.write_text(
        json.dumps(
            {
                "schema_version": "forex_git_source_checkpoint_v1",
                "git_commit": commits[0],
            }
        ),
        encoding="utf-8",
    )
    suspicious = source / "forex_source_NOT_A_GIT_BASELINE.zip"
    suspicious.write_bytes(b"not managed")
    with pytest.raises(RuntimeError, match="unrecognized source archive-family name"):
        sync_source_baseline(root, vault, retention=1)
    assert all(
        (source / f"forex_source_{descriptor}.zip").is_file()
        for descriptor in descriptors_before
    )
    assert suspicious.read_bytes() == b"not managed"


@pytest.mark.parametrize("retention", [0, -1, True, 1.5])
def test_invalid_retention_is_rejected_without_publishing(
    tmp_path: Path,
    retention: object,
) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commit_version(root, 1)
    with pytest.raises(ValueError, match="at least 1"):
        sync_source_baseline(root, vault, retention=retention)  # type: ignore[arg-type]
    source = vault / "projects/forex/source"
    assert not list(source.glob("forex_source_*.zip"))


def test_legacy_prune_requires_retention_and_deletes_only_exact_direct_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commit_version(root, 1)
    sync_source_baseline(root, vault)
    project = vault / "projects/forex"
    exact_legacy = {
        "forex_source_checkpoint_20260731_180842.zip": b"legacy zip",
        "forex_source_checkpoint_20260731_180842.manifest.json": b"{}",
        "forex_source_checkpoint_current.zip": b"legacy current",
        "forex_source_checkpoint_current.manifest.json": b"{}",
        "forex_source_runtime_20260715_000430.zip": b"runtime",
        "SOURCE_CHECKPOINT_LATEST.json": b"{}",
    }
    for name, payload in exact_legacy.items():
        (project / name).write_bytes(payload)
    unrelated = project / "data" / "causal_event_ledger.sqlite3"
    unrelated.parent.mkdir()
    unrelated.write_bytes(b"causal evidence")
    unrelated_source_name = project / "forex_source_notes.md"
    unrelated_source_name.write_text("keep", encoding="utf-8")

    with pytest.raises(ValueError, match="requires explicit source retention"):
        sync_source_baseline(
            root,
            vault,
            prune_identified_legacy_source_families=True,
        )
    assert all((project / name).is_file() for name in exact_legacy)

    result = sync_source_baseline(
        root,
        vault,
        retention=1,
        prune_identified_legacy_source_families=True,
    )
    assert all(not (project / name).exists() for name in exact_legacy)
    assert unrelated.read_bytes() == b"causal evidence"
    assert unrelated_source_name.read_text(encoding="utf-8") == "keep"
    report = result["legacy_source_prune"]
    assert report["action"] == "pruned"
    assert report["deleted_bytes"] == sum(len(payload) for payload in exact_legacy.values())
    assert {item["name"] for item in report["deleted_targets"]} == set(exact_legacy)
    pointer = json.loads(
        (project / "source" / "SOURCE_BASELINE_LATEST.json").read_text(
            encoding="utf-8"
        )
    )
    assert pointer["legacy_source_prune"] == report
    assert (project / "source" / "forex_source_current.zip").is_file()


def test_legacy_prune_preflights_unknown_names_before_any_unlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commit_version(root, 1)
    sync_source_baseline(root, vault)
    project = vault / "projects/forex"
    exact = project / "forex_source_runtime_20260715_000430.zip"
    unknown = project / "forex_source_runtime_not-a-timestamp.zip"
    exact.write_bytes(b"exact")
    unknown.write_bytes(b"unknown")
    with pytest.raises(RuntimeError, match="unrecognized legacy source-family names"):
        sync_source_baseline(
            root,
            vault,
            retention=1,
            prune_identified_legacy_source_families=True,
        )
    assert exact.read_bytes() == b"exact"
    assert unknown.read_bytes() == b"unknown"


def test_legacy_prune_revalidates_current_mirror_before_unlink(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commit_version(root, 1)
    sync_source_baseline(root, vault)
    project = vault / "projects/forex"
    legacy = project / "forex_source_runtime_20260715_000430.zip"
    legacy.write_bytes(b"legacy")
    current = project / "source" / "forex_source_current.zip"
    original_execute = module._execute_source_retention

    def execute_then_corrupt(*args: object, **kwargs: object) -> dict[str, object]:
        report = original_execute(*args, **kwargs)  # type: ignore[arg-type]
        current.write_bytes(b"corrupt current mirror")
        return report

    with patch.object(
        module,
        "_execute_source_retention",
        side_effect=execute_then_corrupt,
    ):
        with pytest.raises(RuntimeError, match="current source mirror archive hash mismatch"):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert legacy.read_bytes() == b"legacy"


def test_retention_helpers_reject_wrong_root_and_non_child_path(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    wrong_root = tmp_path / "not-the-vault-source"
    wrong_root.mkdir()
    with pytest.raises(RuntimeError, match="exact <vault>/projects/forex/source"):
        module._inventory_source_pairs(wrong_root)

    source = tmp_path / "vault" / "projects" / "forex" / "source"
    source.mkdir(parents=True)
    outside = tmp_path / "vault" / "projects" / "forex" / "causal.sqlite3"
    outside.write_bytes(b"evidence")
    with pytest.raises(RuntimeError, match="not a direct source child"):
        module._require_direct_regular_file(outside, source)
    assert outside.read_bytes() == b"evidence"
