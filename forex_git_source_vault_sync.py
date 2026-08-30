#!/usr/bin/env python3
"""Publish a source-only Forex vault checkpoint from an exact Git commit.

This deliberately does not use the legacy mixed source/runtime checkpoint.
It requires a clean worktree, archives only tracked files, and publishes the
content archive before its pointer.  Retention is disabled unless an explicit
positive count is supplied; when enabled, it can remove only validated,
direct-child immutable Git-source archive/manifest pairs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any

try:
    from tools.credential_audit import audit as credential_audit
except ModuleNotFoundError:
    from trad.tools.credential_audit import audit as credential_audit


CURRENT_RECORDS = (
    "FOREX_PENDING_IMPROVEMENTS.md",
    "FOREX_PROJECT_LOG.md",
    "FOREX_INDEPENDENT_CORRECTNESS_AND_EVENT_PROOF_AUDIT_20260829.md",
    "FOREX_VAULT_REORIENTATION_20260829.md",
    "MODEL_FEATURE_SPACE.md",
    "config/forex_source_gap_register_v1.json",
)
BANNED_MEMBER_PARTS = {
    "creds",
    ".env",
    "credentials",
    "secrets",
    "accounts_registry.json",
}
FORBIDDEN_RUNTIME_ROOTS = {
    "data", "artifacts", "logs", "log", "models", "checkpoints", "tmp",
    "creds", "credentials", "secrets", "__pycache__", ".pytest_cache",
}
FORBIDDEN_RUNTIME_SUFFIXES = {
    ".db", ".sqlite", ".sqlite3", ".wal", ".shm", ".log", ".parquet",
    ".feather", ".arrow", ".pkl", ".pickle", ".joblib", ".onnx", ".pt",
    ".pth", ".safetensors", ".zip", ".7z", ".tar", ".gz", ".pem", ".key",
    ".pfx", ".p12", ".ppk", ".npy", ".npz", ".bin", ".model",
}
FORBIDDEN_RUNTIME_FILENAMES = {
    "accounts_registry.json", "credentials.json", "secrets.json", "auth.json",
    "token.json", ".netrc", ".npmrc", ".pypirc",
}
IMMUTABLE_ARCHIVE_RE = re.compile(r"^forex_source_([0-9a-f]{16})\.zip$")
IMMUTABLE_MANIFEST_RE = re.compile(
    r"^forex_source_([0-9a-f]{16})\.manifest\.json$"
)
FULL_GIT_OBJECT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
LEGACY_SOURCE_PATTERNS = {
    "mixed_source_checkpoint_timestamped": re.compile(
        r"^forex_source_checkpoint_[0-9]{8}_[0-9]{6}"
        r"(?:\.zip|\.manifest\.json)$"
    ),
    "mixed_source_checkpoint_current": re.compile(
        r"^forex_source_checkpoint_current(?:\.zip|\.manifest\.json)$"
    ),
    "pre_git_source_runtime": re.compile(
        r"^forex_source_runtime_[0-9]{8}_[0-9]{6}\.zip$"
    ),
    "mixed_source_checkpoint_pointer": re.compile(
        r"^SOURCE_CHECKPOINT_LATEST\.json$"
    ),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def git_bytes(root: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
    ).stdout


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def exclusive_source_writer(function):
    @wraps(function)
    def wrapped(
        root: Path,
        vault_root: Path,
        retention: int | None = None,
        prune_identified_legacy_source_families: bool = False,
    ):
        destination = Path(vault_root) / "projects" / "forex" / "source"
        destination.mkdir(parents=True, exist_ok=True)
        lock_path = destination / ".source_baseline_sync.lock"
        try:
            descriptor = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise RuntimeError(
                f"another source baseline publication owns {lock_path}"
            ) from exc
        try:
            os.write(
                descriptor,
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "started_utc": datetime.now(timezone.utc).isoformat(),
                    },
                    sort_keys=True,
                ).encode("utf-8"),
            )
            os.close(descriptor)
            descriptor = -1
            return function(
                root,
                vault_root,
                retention,
                prune_identified_legacy_source_families,
            )
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            lock_path.unlink(missing_ok=True)

    return wrapped


def _positive_retention(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("retention must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("retention must be at least 1")
    return parsed


def write_once_manifest(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        identity = (
            "schema_version", "git_commit", "git_tree", "archive_sha256",
            "tracked_file_count", "privacy_tier", "scope",
        )
        if any(existing.get(key) != payload.get(key) for key in identity):
            raise RuntimeError(f"immutable source manifest collision at {path}")
        return existing
    atomic_json(path, payload)
    return payload


def _member_is_banned(name: str) -> bool:
    parts = {part.lower() for part in Path(name).parts}
    return bool(parts & BANNED_MEMBER_PARTS) or any(
        part.endswith((".key", ".pfx", ".p12", ".pem")) for part in parts
    )


def _member_is_runtime(name: str) -> bool:
    path = Path(name)
    parts = [part.lower() for part in path.parts]
    filename = parts[-1]
    return (
        bool(set(parts[:-1]) & FORBIDDEN_RUNTIME_ROOTS)
        or filename in FORBIDDEN_RUNTIME_FILENAMES
        or filename == ".env"
        or filename.startswith((".env.", "creds.", "credentials.", "secrets."))
        or path.suffix.lower() in FORBIDDEN_RUNTIME_SUFFIXES
    )


def _tracked_tree(root: Path, commit: str) -> tuple[list[str], dict[str, str]]:
    raw = git_bytes(root, "ls-tree", "-r", "-z", commit)
    paths: list[str] = []
    modes: dict[str, str] = {}
    for record in raw.split(b"\0"):
        if not record:
            continue
        metadata, encoded_path = record.split(b"\t", 1)
        mode, object_type, _object_id = metadata.decode("ascii").split(" ", 2)
        path = encoded_path.decode("utf-8")
        if object_type != "blob":
            raise RuntimeError(f"unsupported tracked object type for {path}: {object_type}")
        paths.append(path)
        modes[path] = mode
    return sorted(paths), modes


def _assert_same_clean_checkout(root: Path, commit: str) -> None:
    if git(root, "rev-parse", "--verify", "HEAD^{commit}") != commit:
        raise RuntimeError("Git HEAD changed while building source baseline")
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise RuntimeError("Git worktree changed while building source baseline")


def _require_direct_regular_file(path: Path, directory: Path) -> Path:
    """Return a resolved direct child, rejecting links and containment drift."""

    directory_resolved = directory.resolve(strict=True)
    if path.parent.resolve(strict=True) != directory_resolved:
        raise RuntimeError(f"retention path is not a direct source child: {path}")
    if path.is_symlink():
        raise RuntimeError(f"retention refuses symlink: {path}")
    resolved = path.resolve(strict=True)
    if resolved.parent != directory_resolved or not resolved.is_file():
        raise RuntimeError(f"retention path escaped or is not a regular file: {path}")
    return resolved


def _validate_source_retention_root(destination: Path) -> Path:
    """Require the exact ``<vault>/projects/forex/source`` resolved chain."""

    project = destination.parent
    projects = project.parent
    vault_root = projects.parent
    if (
        destination.name != "source"
        or project.name != "forex"
        or projects.name != "projects"
    ):
        raise RuntimeError(
            "source retention requires the exact <vault>/projects/forex/source root"
        )
    destination_resolved = destination.resolve(strict=True)
    project_resolved = project.resolve(strict=True)
    projects_resolved = projects.resolve(strict=True)
    vault_resolved = vault_root.resolve(strict=True)
    if (
        destination_resolved.parent != project_resolved
        or project_resolved.parent != projects_resolved
        or projects_resolved.parent != vault_resolved
    ):
        raise RuntimeError("source retention root escaped the resolved vault hierarchy")
    return destination_resolved


def _parse_generated_utc(value: Any, manifest_path: Path) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError(f"invalid generated_utc in {manifest_path}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(f"invalid generated_utc in {manifest_path}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise RuntimeError(f"generated_utc is not UTC in {manifest_path}")
    return parsed


def _validate_source_pair(
    destination: Path,
    descriptor: str,
    archive: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    archive_resolved = _require_direct_regular_file(archive, destination)
    manifest_resolved = _require_direct_regular_file(manifest_path, destination)
    try:
        manifest = json.loads(manifest_resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid immutable source manifest: {manifest_path}") from exc
    commit = manifest.get("git_commit")
    tree = manifest.get("git_tree")
    expected_archive_name = f"forex_source_{descriptor}.zip"
    if manifest.get("schema_version") != "forex_git_source_checkpoint_v1":
        raise RuntimeError(f"unexpected immutable manifest schema: {manifest_path}")
    if not isinstance(commit, str) or not FULL_GIT_OBJECT_RE.fullmatch(commit):
        raise RuntimeError(f"invalid Git commit in {manifest_path}")
    if commit[:16] != descriptor:
        raise RuntimeError(f"manifest/descriptor mismatch: {manifest_path}")
    if not isinstance(tree, str) or not FULL_GIT_OBJECT_RE.fullmatch(tree):
        raise RuntimeError(f"invalid Git tree in {manifest_path}")
    recorded_archive = manifest.get("archive")
    if not isinstance(recorded_archive, str):
        raise RuntimeError(f"missing archive path in {manifest_path}")
    recorded_path = Path(recorded_archive)
    if recorded_path.name != expected_archive_name:
        raise RuntimeError(f"manifest archive name mismatch: {manifest_path}")
    try:
        if recorded_path.resolve(strict=True) != archive_resolved:
            raise RuntimeError(f"manifest archive escaped source root: {manifest_path}")
    except OSError as exc:
        raise RuntimeError(f"manifest archive path is unavailable: {manifest_path}") from exc
    archive_sha = manifest.get("archive_sha256")
    if not isinstance(archive_sha, str) or not SHA256_RE.fullmatch(archive_sha):
        raise RuntimeError(f"invalid archive hash in {manifest_path}")
    if sha256_file(archive_resolved) != archive_sha:
        raise RuntimeError(f"archive hash mismatch for {archive}")
    if manifest.get("archive_size_bytes") != archive_resolved.stat().st_size:
        raise RuntimeError(f"archive size mismatch for {archive}")
    if manifest.get("zip_crc_verified") is not True:
        raise RuntimeError(f"archive lacks CRC attestation: {manifest_path}")
    try:
        with zipfile.ZipFile(archive_resolved) as source_zip:
            if source_zip.testzip() is not None:
                raise RuntimeError(f"source archive failed CRC validation: {archive}")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"invalid source archive: {archive}") from exc
    return {
        "descriptor": descriptor,
        "archive": archive_resolved,
        "manifest": manifest_resolved,
        "archive_sha256": archive_sha,
        "generated_utc": _parse_generated_utc(
            manifest.get("generated_utc"), manifest_path
        ),
    }


def _inventory_source_pairs(destination: Path) -> dict[str, dict[str, Any]]:
    """Inventory only exact, non-recursive immutable source pair names."""

    _validate_source_retention_root(destination)

    archives: dict[str, Path] = {}
    manifests: dict[str, Path] = {}
    allowed_managed_names = {
        "forex_source_current.zip",
    }
    for entry in destination.iterdir():
        archive_match = IMMUTABLE_ARCHIVE_RE.fullmatch(entry.name)
        manifest_match = IMMUTABLE_MANIFEST_RE.fullmatch(entry.name)
        if archive_match:
            archives[archive_match.group(1)] = entry
        elif manifest_match:
            manifests[manifest_match.group(1)] = entry
        elif entry.name.startswith("forex_source_") and entry.name not in allowed_managed_names:
            raise RuntimeError(f"unrecognized source archive-family name: {entry.name}")

    archive_only = sorted(set(archives) - set(manifests))
    manifest_only = sorted(set(manifests) - set(archives))
    if archive_only or manifest_only:
        raise RuntimeError(
            "immutable source archive/manifest pair mismatch: "
            f"archive_only={archive_only}, manifest_only={manifest_only}"
        )
    return {
        descriptor: _validate_source_pair(
            destination,
            descriptor,
            archives[descriptor],
            manifests[descriptor],
        )
        for descriptor in sorted(archives)
    }


def _validate_current_source_pointer(
    destination: Path,
    pairs: dict[str, dict[str, Any]],
    expected_descriptor: str,
    expected_archive_sha256: str,
) -> None:
    pointer_path = destination / "SOURCE_BASELINE_LATEST.json"
    current_path = destination / "forex_source_current.zip"
    _require_direct_regular_file(pointer_path, destination)
    current_resolved = _require_direct_regular_file(current_path, destination)
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("invalid current source baseline pointer") from exc
    commit = pointer.get("git_commit")
    if not isinstance(commit, str) or commit[:16] != expected_descriptor:
        raise RuntimeError("current source pointer does not identify the published baseline")
    pair = pairs.get(expected_descriptor)
    if pair is None:
        raise RuntimeError("current source pointer has no immutable archive/manifest pair")
    if pointer.get("archive_sha256") != expected_archive_sha256:
        raise RuntimeError("current source pointer archive hash mismatch")
    if sha256_file(current_resolved) != expected_archive_sha256:
        raise RuntimeError("current source mirror archive hash mismatch")


def _plan_source_retention(
    destination: Path,
    retention: int,
    latest_descriptor: str,
) -> dict[str, Any]:
    if isinstance(retention, bool) or not isinstance(retention, int) or retention < 1:
        raise ValueError("retention must be an integer of at least 1")
    pairs = _inventory_source_pairs(destination)
    if latest_descriptor not in pairs:
        raise RuntimeError("latest immutable source baseline is absent from retention inventory")
    ordered = sorted(
        pairs.values(),
        key=lambda item: (item["generated_utc"], item["descriptor"]),
        reverse=True,
    )
    retained = {item["descriptor"] for item in ordered[:retention]}
    retained.add(latest_descriptor)
    remove = [item for item in ordered if item["descriptor"] not in retained]
    return {
        "action": "planned" if remove else "none_required",
        "enabled": True,
        "requested_keep_latest": retention,
        "managed_pair_count_before": len(ordered),
        "retained_descriptors": [
            item["descriptor"] for item in ordered if item["descriptor"] in retained
        ],
        "planned_delete_descriptors": [item["descriptor"] for item in remove],
        "deleted_descriptors": [],
        "_pairs": pairs,
        "_remove": remove,
    }


def _public_retention_report(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def _execute_source_retention(
    destination: Path,
    plan: dict[str, Any],
    latest_descriptor: str,
    latest_archive_sha256: str,
) -> dict[str, Any]:
    # Re-inventory every exact pair immediately before deletion. Any external
    # change aborts before the first unlink.
    current_pairs = _inventory_source_pairs(destination)
    planned_pairs = plan["_pairs"]
    if set(current_pairs) != set(planned_pairs):
        raise RuntimeError("source retention inventory changed before deletion")
    for descriptor, original in planned_pairs.items():
        current = current_pairs[descriptor]
        if current["archive_sha256"] != original["archive_sha256"]:
            raise RuntimeError("source retention archive changed before deletion")
    _validate_current_source_pointer(
        destination,
        current_pairs,
        latest_descriptor,
        latest_archive_sha256,
    )

    deleted: list[str] = []
    for item in plan["_remove"]:
        descriptor = item["descriptor"]
        if descriptor == latest_descriptor:
            raise RuntimeError("retention attempted to delete the latest source baseline")
        archive = _require_direct_regular_file(item["archive"], destination)
        manifest = _require_direct_regular_file(item["manifest"], destination)
        # Exact direct-child pair only. No directory or recursive operation is
        # used anywhere in source retention.
        archive.unlink()
        manifest.unlink()
        deleted.append(descriptor)

    report = _public_retention_report(plan)
    report["action"] = "pruned" if deleted else "none_required"
    report["deleted_descriptors"] = deleted
    report["managed_pair_count_after"] = len(current_pairs) - len(deleted)
    return report


def _validate_vault_project_root(vault_project: Path) -> Path:
    projects = vault_project.parent
    vault_root = projects.parent
    if vault_project.name != "forex" or projects.name != "projects":
        raise RuntimeError("legacy pruning requires the exact <vault>/projects/forex root")
    project_resolved = vault_project.resolve(strict=True)
    projects_resolved = projects.resolve(strict=True)
    vault_resolved = vault_root.resolve(strict=True)
    if (
        project_resolved.parent != projects_resolved
        or projects_resolved.parent != vault_resolved
    ):
        raise RuntimeError("Forex vault project escaped the resolved vault hierarchy")
    return project_resolved


def _legacy_family_for_name(name: str) -> str | None:
    return next(
        (
            family
            for family, pattern in LEGACY_SOURCE_PATTERNS.items()
            if pattern.fullmatch(name)
        ),
        None,
    )


def _identify_legacy_source_families(vault_project: Path) -> dict[str, Any]:
    """Identify superseded source artifacts without opening or deleting them."""

    project_resolved = _validate_vault_project_root(vault_project)
    families: dict[str, list[dict[str, Any]]] = {
        name: [] for name in LEGACY_SOURCE_PATTERNS
    }
    for entry in vault_project.iterdir():
        family = _legacy_family_for_name(entry.name)
        if family is None:
            continue
        # Identification is non-recursive and deliberately does not follow a
        # matching symlink or junction.
        if entry.parent.resolve(strict=True) != project_resolved:
            raise RuntimeError(f"legacy source artifact escaped project root: {entry}")
        metadata = entry.lstat()
        families[family].append(
            {
                "name": entry.name,
                "path": str(entry.absolute()),
                "size_bytes": metadata.st_size,
                "is_symlink": entry.is_symlink(),
            }
        )
    populated = {
        name: sorted(items, key=lambda item: item["name"])
        for name, items in families.items()
        if items
    }
    return {
        "action": "identified_only_no_delete",
        "scope": "direct children of the Forex vault project only",
        "family_count": len(populated),
        "artifact_count": sum(len(items) for items in populated.values()),
        "total_bytes": sum(
            item["size_bytes"]
            for items in populated.values()
            for item in items
        ),
        "families": populated,
    }


def _plan_legacy_source_prune(vault_project: Path) -> dict[str, Any]:
    """Preflight every exact legacy target without following links."""

    project_resolved = _validate_vault_project_root(vault_project)
    targets: list[dict[str, Any]] = []
    unknown_legacy_names: list[str] = []
    for entry in vault_project.iterdir():
        family = _legacy_family_for_name(entry.name)
        looks_legacy = entry.name.startswith(
            ("forex_source_checkpoint_", "forex_source_runtime_", "SOURCE_CHECKPOINT_")
        )
        if family is None:
            if looks_legacy:
                unknown_legacy_names.append(entry.name)
            continue
        if entry.parent.resolve(strict=True) != project_resolved:
            raise RuntimeError(f"legacy prune target is not a direct project child: {entry}")
        if entry.is_symlink() or getattr(entry, "is_junction", lambda: False)():
            raise RuntimeError(f"legacy prune refuses link or junction: {entry}")
        metadata = entry.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError(f"legacy prune target is not a direct regular file: {entry}")
        resolved = entry.resolve(strict=True)
        if resolved.parent != project_resolved or not resolved.is_file():
            raise RuntimeError(f"legacy prune target escaped the Forex vault project: {entry}")
        targets.append(
            {
                "family": family,
                "name": entry.name,
                "path": str(resolved),
                "size_bytes": metadata.st_size,
                "sha256": sha256_file(resolved),
            }
        )
    if unknown_legacy_names:
        raise RuntimeError(
            "unrecognized legacy source-family names: "
            + ", ".join(sorted(unknown_legacy_names))
        )
    targets.sort(key=lambda item: item["name"])
    return {
        "action": "planned" if targets else "none_required",
        "enabled": True,
        "target_count": len(targets),
        "target_bytes": sum(item["size_bytes"] for item in targets),
        "planned_targets": [
            {
                "family": item["family"],
                "name": item["name"],
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }
            for item in targets
        ],
        "deleted_targets": [],
        "deleted_bytes": 0,
        "_targets": targets,
    }


def _public_legacy_prune_report(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def _execute_legacy_source_prune(
    vault_project: Path,
    source_destination: Path,
    latest_descriptor: str,
    latest_archive_sha256: str,
    plan: dict[str, Any],
) -> dict[str, Any]:
    # Legacy deletion is subordinate to a currently valid Git-source pointer
    # and mirror.  Revalidate them after any immutable-pair retention and before
    # touching the first legacy file.
    current_pairs = _inventory_source_pairs(source_destination)
    _validate_current_source_pointer(
        source_destination,
        current_pairs,
        latest_descriptor,
        latest_archive_sha256,
    )
    fresh = _plan_legacy_source_prune(vault_project)
    if fresh["planned_targets"] != plan["planned_targets"]:
        raise RuntimeError("legacy source prune targets changed before deletion")

    deleted: list[dict[str, Any]] = []
    for item in fresh["_targets"]:
        target = _require_direct_regular_file(Path(item["path"]), vault_project)
        target.unlink()
        deleted.append(
            {
                "family": item["family"],
                "name": item["name"],
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }
        )
    return {
        "action": "pruned" if deleted else "none_required",
        "enabled": True,
        "target_count": len(fresh["_targets"]),
        "target_bytes": sum(item["size_bytes"] for item in fresh["_targets"]),
        "planned_targets": plan["planned_targets"],
        "deleted_targets": deleted,
        "deleted_bytes": sum(item["size_bytes"] for item in deleted),
    }


@exclusive_source_writer
def sync_source_baseline(
    root: Path,
    vault_root: Path,
    retention: int | None = None,
    prune_identified_legacy_source_families: bool = False,
) -> dict[str, Any]:
    if retention is not None and (
        isinstance(retention, bool) or not isinstance(retention, int) or retention < 1
    ):
        raise ValueError("retention must be an integer of at least 1")
    if not isinstance(prune_identified_legacy_source_families, bool):
        raise ValueError("legacy source pruning flag must be boolean")
    if prune_identified_legacy_source_families and retention is None:
        raise ValueError("legacy source pruning requires explicit source retention")
    root = Path(git(root, "rev-parse", "--show-toplevel"))
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise RuntimeError("source baseline requires a clean Git worktree")
    commit = git(root, "rev-parse", "--verify", "HEAD^{commit}")
    tree = git(root, "rev-parse", "--verify", f"{commit}^{{tree}}")
    credential_receipt = credential_audit(root, "commit", revision=commit)
    if not credential_receipt.get("passed"):
        raise RuntimeError("committed source failed credential audit")
    tracked, tracked_modes = _tracked_tree(root, commit)
    banned = sorted(name for name in tracked if _member_is_banned(name))
    if banned:
        raise RuntimeError("banned private members are tracked: " + ", ".join(banned[:8]))
    runtime_members = sorted(name for name in tracked if _member_is_runtime(name))
    if runtime_members:
        raise RuntimeError(
            "runtime/private members are not allowed in source baseline: "
            + ", ".join(runtime_members[:8])
        )
    symlinks = sorted(name for name, mode in tracked_modes.items() if mode == "120000")
    if symlinks:
        raise RuntimeError("tracked symlinks are not allowed in source baseline: " + ", ".join(symlinks[:8]))

    destination = vault_root / "projects" / "forex" / "source"
    destination.mkdir(parents=True, exist_ok=True)
    descriptor = commit[:16]
    immutable_archive = destination / f"forex_source_{descriptor}.zip"
    with tempfile.NamedTemporaryFile(
        prefix=".forex_source_", suffix=".zip", dir=destination, delete=False
    ) as handle:
        temporary_archive = Path(handle.name)
    try:
        subprocess.run(
            ["git", "-C", str(root), "archive", "--format=zip", f"--output={temporary_archive}", commit],
            check=True,
        )
        with zipfile.ZipFile(temporary_archive) as archive:
            if archive.testzip() is not None:
                raise RuntimeError("source archive failed ZIP CRC validation")
            names = sorted(info.filename for info in archive.infolist() if not info.is_dir())
            if names != tracked:
                raise RuntimeError("Git tree and archive member inventories differ")
        archive_sha = sha256_file(temporary_archive)
        if immutable_archive.exists():
            if sha256_file(immutable_archive) != archive_sha:
                raise RuntimeError("content-addressed source archive collision")
            temporary_archive.unlink()
        else:
            os.replace(temporary_archive, immutable_archive)
    finally:
        temporary_archive.unlink(missing_ok=True)

    # The immutable object may be retained after a concurrent checkout change,
    # but no mutable "current" pointer may advance unless the checkout is still
    # the exact clean commit audited above.
    _assert_same_clean_checkout(root, commit)

    current_archive = destination / "forex_source_current.zip"
    temporary_current = current_archive.with_name(f".{current_archive.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(immutable_archive, temporary_current)
        if sha256_file(temporary_current) != archive_sha:
            raise RuntimeError("current archive copy hash mismatch")
        os.replace(temporary_current, current_archive)
    finally:
        temporary_current.unlink(missing_ok=True)

    records_dir = vault_root / "projects" / "forex" / "current_records"
    records: list[dict[str, Any]] = []
    retired_records: list[dict[str, str]] = []
    for relative in CURRENT_RECORDS:
        if relative not in tracked_modes:
            stale = records_dir / Path(relative).name
            if stale.exists():
                retired = records_dir / "retired" / descriptor / stale.name
                retired.parent.mkdir(parents=True, exist_ok=True)
                os.replace(stale, retired)
                retired_records.append({"source": relative, "retired_to": str(retired)})
            continue
        if tracked_modes[relative] == "120000":
            raise RuntimeError(f"current record cannot be a symlink: {relative}")
        payload = git_bytes(root, "show", f"{commit}:{relative}")
        target = records_dir / Path(relative).name
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(payload)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        records.append(
            {
                "source": relative,
                "source_commit": commit,
                "target": str(target),
                "sha256": sha256_file(target),
            }
        )

    manifest = {
        "schema_version": "forex_git_source_checkpoint_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source_root": str(root),
        "git_commit": commit,
        "git_tree": tree,
        "worktree_clean": True,
        "privacy_tier": "private_local_source_no_bearer_credentials",
        "scope": "tracked source config tests and documentation; no runtime data, databases, logs, models, or account registry",
        "archive": str(immutable_archive),
        "archive_sha256": archive_sha,
        "archive_size_bytes": immutable_archive.stat().st_size,
        "tracked_file_count": len(tracked),
        "zip_crc_verified": True,
        "retention_action": "none",
        "credential_audit": credential_receipt,
        "credential_audit_sha256": hashlib.sha256(
            json.dumps(credential_receipt, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "current_records": records,
        "retired_stale_current_records": retired_records,
    }
    immutable_manifest = destination / f"forex_source_{descriptor}.manifest.json"
    manifest = write_once_manifest(immutable_manifest, manifest)
    legacy_sources = _identify_legacy_source_families(destination.parent)
    legacy_prune_plan = (
        _plan_legacy_source_prune(destination.parent)
        if prune_identified_legacy_source_families
        else None
    )
    pointer = dict(manifest)
    pointer["legacy_source_families"] = legacy_sources

    # This JSON is the canonical mutable pointer and is deliberately published
    # only after the immutable archive, current archive, and exact-commit record
    # aliases have all completed.  With retention enabled, a planned pointer is
    # published before pruning so the immutable pair being made current is
    # always protected as latest during every unlink.
    pointer_path = destination / "SOURCE_BASELINE_LATEST.json"
    if retention is None:
        pointer["retention_action"] = "none"
        pointer["retention"] = {
            "action": "none_default_no_prune",
            "enabled": False,
            "requested_keep_latest": None,
            "deleted_descriptors": [],
        }
        pointer["legacy_source_prune"] = {
            "action": "none_not_requested",
            "enabled": False,
            "deleted_targets": [],
            "deleted_bytes": 0,
        }
        atomic_json(pointer_path, pointer)
        return pointer

    plan = _plan_source_retention(destination, retention, descriptor)
    pointer["retention_action"] = plan["action"]
    pointer["retention"] = _public_retention_report(plan)
    pointer["legacy_source_prune"] = (
        _public_legacy_prune_report(legacy_prune_plan)
        if legacy_prune_plan is not None
        else {
            "action": "none_not_requested",
            "enabled": False,
            "deleted_targets": [],
            "deleted_bytes": 0,
        }
    )
    atomic_json(pointer_path, pointer)
    retention_report = _execute_source_retention(
        destination,
        plan,
        descriptor,
        archive_sha,
    )
    pointer["retention_action"] = retention_report["action"]
    pointer["retention"] = retention_report
    atomic_json(pointer_path, pointer)
    if legacy_prune_plan is not None:
        legacy_prune_report = _execute_legacy_source_prune(
            destination.parent,
            destination,
            descriptor,
            archive_sha,
            legacy_prune_plan,
        )
        pointer["legacy_source_prune"] = legacy_prune_report
        atomic_json(pointer_path, pointer)
    return pointer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument(
        "--vault-root", type=Path, default=Path.home() / "OneDrive" / "thevault"
    )
    parser.add_argument(
        "--retention",
        type=_positive_retention,
        default=None,
        metavar="N",
        help=(
            "retain the latest N validated immutable Git-source baselines; "
            "omitting this option preserves all baselines"
        ),
    )
    parser.add_argument(
        "--prune-identified-legacy-source-families",
        action="store_true",
        help=(
            "delete only preflighted exact legacy source files under the Forex "
            "vault project; requires --retention"
        ),
    )
    args = parser.parse_args()
    print(
        json.dumps(
            sync_source_baseline(
                args.root,
                args.vault_root,
                args.retention,
                args.prune_identified_legacy_source_families,
            ),
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
