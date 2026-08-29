#!/usr/bin/env python3
"""Publish a source-only Forex vault checkpoint from an exact Git commit.

This deliberately does not use the legacy mixed source/runtime checkpoint.
It requires a clean worktree, archives only tracked files, publishes the
content archive before its pointer, and never performs retention deletion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
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
    def wrapped(root: Path, vault_root: Path):
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
            return function(root, vault_root)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            lock_path.unlink(missing_ok=True)

    return wrapped


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


@exclusive_source_writer
def sync_source_baseline(root: Path, vault_root: Path) -> dict[str, Any]:
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
    # This JSON is the canonical mutable pointer and is deliberately published
    # only after the immutable archive, current archive, and exact-commit record
    # aliases have all completed.
    atomic_json(destination / "SOURCE_BASELINE_LATEST.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument(
        "--vault-root", type=Path, default=Path.home() / "OneDrive" / "thevault"
    )
    args = parser.parse_args()
    print(json.dumps(sync_source_baseline(args.root, args.vault_root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
