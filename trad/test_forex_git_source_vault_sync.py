from __future__ import annotations

import hashlib
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


def combined_prune_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, dict[Path, bytes]]:
    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    for version in range(3):
        commit_version(root, version)
        sync_source_baseline(root, vault)
    project = vault / "projects/forex"
    source = project / "source"
    legacy_payloads = {
        "forex_source_checkpoint_20260731_180842.zip": b"legacy zip",
        "forex_source_checkpoint_20260731_180842.manifest.json": b"{}",
        "forex_source_runtime_20260715_000430.zip": b"legacy runtime",
        "SOURCE_CHECKPOINT_LATEST.json": b"{}",
    }
    for name, payload in legacy_payloads.items():
        (project / name).write_bytes(payload)
    targets = {
        path: path.read_bytes()
        for path in source.iterdir()
        if (
            path.name.startswith("forex_source_")
            and path.name != "forex_source_current.zip"
            and path.name.endswith((".zip", ".manifest.json"))
        )
    }
    targets.update(
        {project / name: payload for name, payload in legacy_payloads.items()}
    )
    return root, vault, project, source, targets


def assert_prune_targets_unchanged(targets: dict[Path, bytes]) -> None:
    assert all(path.is_file() for path in targets)
    assert all(path.read_bytes() == payload for path, payload in targets.items())


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

    expected_tombstones: dict[str, dict[str, object]] = {}
    for commit in commits[:-2]:
        descriptor = commit[:16]
        for artifact_type, filename in (
            ("archive", f"forex_source_{descriptor}.zip"),
            ("manifest", f"forex_source_{descriptor}.manifest.json"),
        ):
            payload = (source / filename).read_bytes()
            expected_tombstones[filename] = {
                "descriptor": descriptor,
                "artifact_type": artifact_type,
                "filename": filename,
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }

    result = sync_source_baseline(root, vault, retention=2)
    expected = {commit[:16] for commit in commits[-2:]}
    assert immutable_descriptors(source, "zip") == expected
    assert immutable_descriptors(source, "manifest.json") == expected
    assert result["retention_action"] == "pruned"
    assert set(result["retention"]["retained_descriptors"]) == expected
    assert set(result["retention"]["deleted_descriptors"]) == {
        commit[:16] for commit in commits[:-2]
    }
    planned_tombstones = {
        item["filename"]: item
        for item in result["retention"]["planned_file_tombstones"]
    }
    deleted_tombstones = {
        item["filename"]: item
        for item in result["retention"]["deleted_file_tombstones"]
    }
    assert planned_tombstones == expected_tombstones
    assert deleted_tombstones == expected_tombstones
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


def test_retention_publishes_full_tombstones_before_first_unlink(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    commits = []
    for version in range(3):
        commits.append(commit_version(root, version))
        sync_source_baseline(root, vault)

    source = vault / "projects/forex/source"
    pointer_path = source / "SOURCE_BASELINE_LATEST.json"
    pointer_before = pointer_path.read_bytes()
    descriptors_before = immutable_descriptors(source, "zip")
    observed_plan: dict[str, object] = {}
    original_atomic_json = module.atomic_json

    def fail_planned_pointer(path: Path, payload: dict[str, object]) -> None:
        retention = payload.get("retention")
        if (
            path == pointer_path
            and isinstance(retention, dict)
            and retention.get("action") == "planned"
        ):
            observed_plan.update(retention)
            raise OSError("simulated planned tombstone publication failure")
        original_atomic_json(path, payload)  # type: ignore[arg-type]

    with patch.object(module, "atomic_json", side_effect=fail_planned_pointer):
        with pytest.raises(
            OSError, match="simulated planned tombstone publication failure"
        ):
            sync_source_baseline(root, vault, retention=1)

    assert immutable_descriptors(source, "zip") == descriptors_before
    assert immutable_descriptors(source, "manifest.json") == descriptors_before
    assert pointer_path.read_bytes() == pointer_before
    tombstones = observed_plan["planned_file_tombstones"]
    assert isinstance(tombstones, list)
    assert len(tombstones) == 2 * (len(commits) - 1)
    assert all(
        set(item) == {
            "descriptor",
            "artifact_type",
            "filename",
            "size_bytes",
            "sha256",
        }
        for item in tombstones
    )


def test_retention_rejects_manifest_byte_drift_before_any_unlink(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    for version in range(3):
        commit_version(root, version)
        sync_source_baseline(root, vault)
    source = vault / "projects/forex/source"
    descriptors_before = immutable_descriptors(source, "zip")
    original_execute = module._execute_source_retention

    def mutate_then_execute(*args: object, **kwargs: object) -> dict[str, object]:
        plan = args[1]
        assert isinstance(plan, dict)
        manifest = Path(plan["_remove"][-1]["manifest"])
        manifest.write_bytes(manifest.read_bytes() + b"\n")
        return original_execute(*args, **kwargs)  # type: ignore[arg-type]

    with patch.object(
        module, "_execute_source_retention", side_effect=mutate_then_execute
    ):
        with pytest.raises(
            RuntimeError, match="source retention files changed before deletion"
        ):
            sync_source_baseline(root, vault, retention=1)

    assert immutable_descriptors(source, "zip") == descriptors_before
    assert immutable_descriptors(source, "manifest.json") == descriptors_before


def test_retention_report_serialization_failure_precedes_every_unlink(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root = tmp_path / "repo"
    vault = tmp_path / "vault"
    initialise_repo(root)
    for version in range(3):
        commit_version(root, version)
        sync_source_baseline(root, vault)
    source = vault / "projects/forex/source"
    descriptors_before = immutable_descriptors(source, "zip")
    original_execute = module._execute_source_retention

    def fail_report_serialization(
        *args: object, **kwargs: object
    ) -> dict[str, object]:
        with patch.object(
            module.json,
            "dumps",
            side_effect=TypeError("simulated retention report failure"),
        ):
            return original_execute(*args, **kwargs)  # type: ignore[arg-type]

    with patch.object(
        module,
        "_execute_source_retention",
        side_effect=fail_report_serialization,
    ):
        with pytest.raises(TypeError, match="simulated retention report failure"):
            sync_source_baseline(root, vault, retention=1)

    assert immutable_descriptors(source, "zip") == descriptors_before
    assert immutable_descriptors(source, "manifest.json") == descriptors_before
    planned_pointer = json.loads(
        (source / "SOURCE_BASELINE_LATEST.json").read_text(encoding="utf-8")
    )
    assert planned_pointer["retention"]["action"] == "planned"
    assert len(planned_pointer["retention"]["planned_file_tombstones"]) == 4


def test_combined_prune_receipt_covers_managed_and_legacy_targets(
    tmp_path: Path,
) -> None:
    root, vault, project, source, targets = combined_prune_fixture(tmp_path)
    latest_descriptor = json.loads(
        (source / "SOURCE_BASELINE_LATEST.json").read_text(encoding="utf-8")
    )["git_commit"][:16]
    pruned_targets = {
        path: payload
        for path, payload in targets.items()
        if path.parent == project or latest_descriptor not in path.name
    }
    expected = {
        path.name: {
            "filename": path.name,
            "size_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        for path, payload in pruned_targets.items()
    }

    result = sync_source_baseline(
        root,
        vault,
        retention=1,
        prune_identified_legacy_source_families=True,
    )

    assert all(not path.exists() for path in pruned_targets)
    assert all(
        path.exists() for path in targets if path not in pruned_targets
    )
    transaction = result["prune_transaction"]
    assert transaction["status"] == "complete"
    tombstone_path = source / transaction["tombstone"]["filename"]
    receipt_path = source / transaction["receipt"]["filename"]
    progress_path = source / transaction["progress"]["filename"]
    tombstone = json.loads(tombstone_path.read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    tombstones = {item["filename"]: item for item in tombstone["targets"]}
    assert set(tombstones) == set(expected)
    assert all(
        tombstones[name]["size_bytes"] == details["size_bytes"]
        and tombstones[name]["sha256"] == details["sha256"]
        for name, details in expected.items()
    )
    assert receipt["deleted_targets"] == tombstone["targets"]
    assert receipt["status"] == "complete"
    assert progress["status"] == "complete"
    assert progress["completed_count"] == len(pruned_targets)
    assert {
        item["filename"] for item in progress["completed_targets"]
    } == set(expected)


def test_combined_prune_legacy_hash_failure_deletes_nothing(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, _project, _source, targets = combined_prune_fixture(tmp_path)
    original_execute = module._execute_legacy_source_prune
    original_sha256 = module.sha256_file

    def fail_legacy_hash(*args: object, **kwargs: object) -> dict[str, object]:
        plan = args[-1]
        assert isinstance(plan, dict)
        fail_name = plan["planned_targets"][-1]["name"]

        def hash_or_fail(path: Path) -> str:
            if Path(path).name == fail_name:
                raise OSError("simulated legacy hash failure")
            return original_sha256(Path(path))

        with patch.object(module, "sha256_file", side_effect=hash_or_fail):
            return original_execute(*args, **kwargs)  # type: ignore[arg-type]

    with patch.object(
        module, "_execute_legacy_source_prune", side_effect=fail_legacy_hash
    ):
        with pytest.raises(OSError, match="simulated legacy hash failure"):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert_prune_targets_unchanged(targets)


def test_combined_prune_final_report_preflight_failure_deletes_nothing(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, _project, _source, targets = combined_prune_fixture(tmp_path)
    with patch.object(
        module,
        "_pre_serialize_combined_prune_report",
        side_effect=TypeError("simulated combined final report failure"),
    ):
        with pytest.raises(TypeError, match="simulated combined final report failure"):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert_prune_targets_unchanged(targets)


def test_combined_prune_tombstone_publication_failure_deletes_nothing(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, _project, _source, targets = combined_prune_fixture(tmp_path)
    original_atomic_json = module.atomic_json

    def fail_tombstone_publish(path: Path, payload: dict[str, object]) -> None:
        if path.name.startswith("SOURCE_PRUNE_TOMBSTONE_"):
            raise OSError("simulated tombstone publication failure")
        original_atomic_json(path, payload)  # type: ignore[arg-type]

    with patch.object(module, "atomic_json", side_effect=fail_tombstone_publish):
        with pytest.raises(OSError, match="simulated tombstone publication failure"):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert_prune_targets_unchanged(targets)


def test_combined_prune_tombstone_verification_failure_deletes_nothing(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, _project, _source, targets = combined_prune_fixture(tmp_path)
    original_atomic_json = module.atomic_json

    def corrupt_tombstone_after_publish(
        path: Path, payload: dict[str, object]
    ) -> None:
        original_atomic_json(path, payload)  # type: ignore[arg-type]
        if path.name.startswith("SOURCE_PRUNE_TOMBSTONE_"):
            path.write_bytes(path.read_bytes() + b"\n")

    with patch.object(
        module, "atomic_json", side_effect=corrupt_tombstone_after_publish
    ):
        with pytest.raises(
            RuntimeError, match="prune audit artifact byte verification failed"
        ):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert_prune_targets_unchanged(targets)


def test_combined_prune_reverifies_tombstone_after_both_family_preflights(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, _project, source, targets = combined_prune_fixture(tmp_path)
    original_execute = module._execute_source_retention

    def tamper_after_source_preflight(
        *args: object, **kwargs: object
    ) -> dict[str, object]:
        report = original_execute(*args, **kwargs)  # type: ignore[arg-type]
        tombstone = next(source.glob("SOURCE_PRUNE_TOMBSTONE_*.json"))
        tombstone.write_bytes(tombstone.read_bytes() + b"\n")
        return report

    with patch.object(
        module,
        "_execute_source_retention",
        side_effect=tamper_after_source_preflight,
    ):
        with pytest.raises(RuntimeError, match="prune audit artifact (size|hash) changed"):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert_prune_targets_unchanged(targets)


def test_combined_prune_progress_publication_failure_precedes_first_unlink(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, _project, _source, targets = combined_prune_fixture(tmp_path)
    original_write = module._write_verified_json

    def fail_initial_progress(
        path: Path,
        payload: dict[str, object],
        *,
        write_once: bool = False,
    ) -> dict[str, object]:
        if (
            path.name.startswith("SOURCE_PRUNE_PROGRESS_")
            and payload.get("status") == "validated_ready_to_prune"
        ):
            raise OSError("simulated progress publication failure")
        return original_write(  # type: ignore[return-value]
            path, payload, write_once=write_once  # type: ignore[arg-type]
        )

    with patch.object(module, "_write_verified_json", side_effect=fail_initial_progress):
        with pytest.raises(OSError, match="simulated progress publication failure"):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert_prune_targets_unchanged(targets)


def test_combined_prune_target_mutation_at_final_preflight_deletes_nothing(
    tmp_path: Path,
) -> None:
    from trad import forex_git_source_vault_sync as module

    root, vault, project, _source, targets = combined_prune_fixture(tmp_path)
    original_preflight = module._preflight_combined_prune_targets
    mutated: Path | None = None

    def mutate_then_preflight(
        vault_project: Path,
        source_destination: Path,
        latest_descriptor: str,
        tombstones: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        nonlocal mutated
        legacy = next(item for item in tombstones if item["scope"] == "legacy_source")
        mutated = project / str(legacy["filename"])
        mutated.write_bytes(mutated.read_bytes() + b"tamper")
        return original_preflight(  # type: ignore[return-value]
            vault_project, source_destination, latest_descriptor, tombstones
        )

    with patch.object(
        module,
        "_preflight_combined_prune_targets",
        side_effect=mutate_then_preflight,
    ):
        with pytest.raises(
            RuntimeError, match="combined prune target (size|hash) changed"
        ):
            sync_source_baseline(
                root,
                vault,
                retention=1,
                prune_identified_legacy_source_families=True,
            )
    assert mutated is not None
    assert all(path.is_file() for path in targets)
    assert all(
        path.read_bytes() == payload
        for path, payload in targets.items()
        if path != mutated
    )


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
