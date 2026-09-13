#!/usr/bin/env python3
"""Publish/verify an offline source snapshot without changing Git or account state.

This is explicitly a working-tree snapshot, never a clean committed baseline.
Ignored data is not inventoried recursively. No history is pruned.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    from tools import credential_audit as credentials
    from forex_git_source_vault_sync import _member_is_banned, _member_is_runtime
except ModuleNotFoundError:
    from trad.tools import credential_audit as credentials
    from trad.forex_git_source_vault_sync import _member_is_banned, _member_is_runtime


SCHEMA = "forex_working_tree_source_snapshot_v1"
POINTER = "WORKTREE_SOURCE_LATEST.json"
SOURCE_SUFFIXES = {".py", ".ps1", ".cmd", ".bat", ".sh", ".md", ".rst", ".txt",
                   ".html", ".css", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx",
                   ".json", ".toml", ".yaml", ".yml", ".ini", ".cfg", ".xml", ".svg"}
SOURCE_FILENAMES = {".gitignore", ".gitattributes", ".editorconfig", "Dockerfile", "Makefile"}


def encoded(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def git(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args], check=True,
        capture_output=True, env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    ).stdout


def safe_name(name: str) -> str:
    path = PurePosixPath(name)
    if (not name or "\\" in name or path.is_absolute() or
            any(part in {"", ".", ".."} or ":" in part for part in name.split("/"))):
        raise RuntimeError("unsafe snapshot member path")
    reserved = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"}
    reserved.update(f"{prefix}{suffix}" for prefix in ("COM", "LPT")
                    for suffix in "123456789\u00b9\u00b2\u00b3")
    if any(part.endswith((".", " ")) or part.split(".", 1)[0].upper() in reserved
           or any(ord(char) < 32 or char in '<>"|?*' for char in part)
           for part in name.split("/")):
        raise RuntimeError("unsafe Windows snapshot member alias")
    return name


def regular_file(root: Path, name: str) -> Path:
    safe_name(name)
    path = root / name
    for candidate in (path, *path.parents):
        if candidate == root.parent:
            break
        info = candidate.lstat()
        # Cloud hydration reparse tags are not links; name-surrogate tags are.
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0) & 0x20000000:
            raise RuntimeError(f"symlink/reparse point is prohibited: {name}")
    if not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
        raise RuntimeError(f"not a contained regular source file: {name}")
    return path


def source_state(root: Path) -> tuple[dict, list[str], dict[str, str]]:
    head = git(root, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    status_bytes = git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    index = git(root, "ls-files", "--stage", "-z")
    modes = {}
    for row in index.split(b"\0"):
        if row:
            metadata, name = row.split(b"\t", 1)
            mode, _blob, stage = metadata.decode("ascii").split()
            if stage != "0":
                raise RuntimeError("unmerged index cannot be snapshotted")
            modes[name.decode("utf-8")] = mode
    raw_paths = git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
    paths = sorted(set(row.decode("utf-8") for row in raw_paths.split(b"\0") if row))
    for name in paths:
        safe_name(name)
    return {
        "kind": "working_tree_snapshot", "base_git_commit": head,
        "worktree_dirty": bool(status_bytes), "is_clean_commit_archive": False,
        "index_inventory_sha256": digest(index),
        "porcelain_status_sha256": digest(status_bytes),
        "porcelain_status_entries": [row.decode("utf-8") for row in status_bytes.split(b"\0") if row],
    }, paths, modes


def excluded_reason(name: str) -> str | None:
    if credentials.path_is_banned(name) or _member_is_banned(name):
        return "private_path"
    if _member_is_runtime(name):
        return "runtime_or_binary_payload"
    if any(part.lower() in {".git", ".venv", "venv", "env", "node_modules"}
           for part in PurePosixPath(name).parts):
        return "environment_or_dependency_tree"
    if name.lower().endswith((".private.json", ".local.json", ".secrets.json", ".crt", ".cer")):
        return "private_path"
    if PurePosixPath(name).suffix.lower() not in SOURCE_SUFFIXES and PurePosixPath(name).name not in SOURCE_FILENAMES:
        return "non_source_extension"
    return None


def known_private_values(paths: list[Path]) -> set[bytes]:
    values = set()
    for path in paths:
        if not path.exists():
            continue
        payload = regular_file(path.parent.resolve(), path.name).read_bytes()
        for rule, pattern in credentials.PATTERNS.items():
            for match in pattern.finditer(payload):
                value = match.group(1) if match.lastindex else match.group(0)
                # An explicitly supplied private file contains private literals;
                # identifier-like spelling does not make those values source code.
                values.add(value)
        # OANDA tokens and plain token-only credentials need no assignment label.
        values.update(re.findall(rb"\b[0-9a-fA-F]{32}-[0-9a-fA-F]{32}\b", payload))
        for line in payload.splitlines():
            value = line.strip().strip(b"\"'")
            if (re.fullmatch(rb"[A-Za-z0-9._~+/=-]{20,}", value)
                    and not credentials.ACCOUNT_ID.fullmatch(value)):
                values.add(value)
    return values


def audit_payload(name: str, payload: bytes, private_values: set[bytes]) -> None:
    if any(value in payload for value in private_values):
        raise RuntimeError(f"credential audit rejected source member: {name} (known private value)")
    for rule, pattern in credentials.PATTERNS.items():
        for match in pattern.finditer(payload):
            material = match.group(1) if match.lastindex else match.group(0)
            if rule == "credential_assignment" and (
                credentials.synthetic_fixture_value(name, material)
                or credentials.credential_assignment_is_reference(name, match)
            ):
                continue
            raise RuntimeError(f"credential audit rejected source member: {name} ({rule})")


def assert_unchanged(root: Path, state: dict, paths: list[str], rows: list[dict]) -> None:
    observed, observed_paths, _modes = source_state(root)
    if observed != state or observed_paths != paths:
        raise RuntimeError("source or Git inventory changed during snapshot")
    for row in rows:
        try:
            payload = regular_file(root, row["path"]).read_bytes()
        except OSError as exc:
            raise RuntimeError("source member disappeared during snapshot") from exc
        if len(payload) != row["size"] or digest(payload) != row["sha256"]:
            raise RuntimeError(f"source changed during snapshot: {row['path']}")


def verify_snapshot(manifest_path: Path, *, compile_python: bool = True) -> dict:
    """Verify safe paths, archive/hash inventory and an offline extract/compile roundtrip."""
    manifest_path = Path(manifest_path).absolute()
    manifest_path = regular_file(manifest_path.parent.resolve(), manifest_path.name)
    manifest = json.loads(manifest_path.read_bytes())
    if (manifest.get("schema_version") != SCHEMA or manifest.get("kind") != "working_tree_snapshot"
            or manifest.get("is_clean_commit_archive") is not False):
        raise RuntimeError("unexpected worktree snapshot schema")
    archive_name = safe_name(manifest["archive"])
    if "/" in archive_name:
        raise RuntimeError("archive must be a direct sibling of manifest")
    archive_path = regular_file(manifest_path.parent.resolve(), archive_name)
    if digest(archive_path.read_bytes()) != manifest["archive_sha256"]:
        raise RuntimeError("snapshot archive hash mismatch")
    rows = manifest["files"]
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("snapshot inventory is empty")
    expected, case_names = {}, set()
    for row in rows:
        name = safe_name(row["path"])
        if name.casefold() in case_names or excluded_reason(name):
            raise RuntimeError("duplicate or prohibited snapshot member")
        expected[name] = row
        case_names.add(name.casefold())
    if digest(encoded(rows)) != manifest["content_sha256"]:
        raise RuntimeError("snapshot content inventory hash mismatch")
    state = {key: manifest[key] for key in ("kind", "base_git_commit", "worktree_dirty",
             "is_clean_commit_archive", "index_inventory_sha256", "porcelain_status_sha256",
             "porcelain_status_entries")}
    if (manifest.get("file_count") != len(rows)
            or manifest.get("snapshot_id") != digest(encoded({"state": state, "files": rows,
                                                               "excluded": manifest["excluded"]}))):
        raise RuntimeError("snapshot identity metadata mismatch")
    compiled = 0
    with zipfile.ZipFile(archive_path) as archive, tempfile.TemporaryDirectory(prefix="forex_worktree_verify_") as temp:
        infos = archive.infolist()
        names = [safe_name(info.filename) for info in infos]
        if len(set(names)) != len(names) or set(names) != set(expected) or archive.testzip() is not None:
            raise RuntimeError("snapshot archive member inventory/CRC mismatch")
        extract_root = Path(temp).resolve()
        for info in infos:
            member_type = stat.S_IFMT(info.external_attr >> 16)
            if info.is_dir() or member_type not in {0, stat.S_IFREG}:
                raise RuntimeError("nonregular archive member prohibited")
            payload = archive.read(info)
            row = expected[info.filename]
            if len(payload) != row["size"] or digest(payload) != row["sha256"]:
                raise RuntimeError("snapshot member hash mismatch")
            audit_payload(info.filename, payload, set())
            target = extract_root / info.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
            recovered = regular_file(extract_root, info.filename).read_bytes()
            if digest(recovered) != row["sha256"]:
                raise RuntimeError("extracted member hash mismatch")
            if compile_python and target.suffix == ".py":
                try:
                    compile(recovered, info.filename, "exec", dont_inherit=True)
                except SyntaxError as exc:
                    raise RuntimeError(f"source compilation failed: {info.filename}, line {exc.lineno}") from None
                compiled += 1
    return {"status": "passed", "archive_sha256": manifest["archive_sha256"],
            "files_verified": len(rows), "python_files_compiled": compiled,
            "roundtrip_verified": True, "code_executed": False,
            "account_processes_started": 0, "network_activity": False}


def sync_worktree_snapshot(root: Path, vault_project: Path, *, private_files: list[Path] | None = None) -> dict:
    root = Path(root).resolve()
    if Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() != root:
        raise RuntimeError("root must be the exact Git worktree root")
    destination = Path(vault_project).resolve() / "source"
    if destination.is_relative_to(root):
        raise RuntimeError("snapshot destination must be outside source worktree")
    destination.mkdir(parents=True, exist_ok=True)
    state, paths, modes = source_state(root)
    private_values = known_private_values(private_files if private_files is not None else [root / "creds"])
    rows, excluded, payloads = [], [], {}
    for name in paths:
        reason = excluded_reason(name)
        if reason:
            excluded.append({"path": name, "reason": reason})
            continue
        if modes.get(name) in {"120000", "160000"}:
            raise RuntimeError(f"tracked symlink/submodule is prohibited: {name}")
        try:
            payload = regular_file(root, name).read_bytes()
        except OSError as exc:
            raise RuntimeError(f"missing or unreadable source member: {name}") from exc
        audit_payload(name, payload, private_values)
        rows.append({"path": name, "size": len(payload), "sha256": digest(payload),
                     "git_membership": "tracked" if name in modes else "untracked"})
        payloads[name] = payload
    if not rows:
        raise RuntimeError("no eligible source members")
    content_sha = digest(encoded(rows))
    snapshot_id = digest(encoded({"state": state, "files": rows, "excluded": excluded}))
    stem = f"forex_worktree_source_{snapshot_id[:24]}"
    archive_path = destination / f"{stem}.zip"
    manifest_path = destination / f"{stem}.manifest.json"
    with tempfile.TemporaryDirectory(prefix=".worktree_snapshot_", dir=destination) as temp:
        staging = Path(temp)
        staged_archive = staging / archive_path.name
        with zipfile.ZipFile(staged_archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for row in rows:
                info = zipfile.ZipInfo(row["path"], date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, payloads[row["path"]])
        manifest = {
            "schema_version": SCHEMA, **state,
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "snapshot_id": snapshot_id, "source_root": str(root),
            "scope": "Git-listed tracked and untracked source/config/tests/docs; not historical runtime evidence",
            "archive": archive_path.name, "archive_sha256": digest(staged_archive.read_bytes()),
            "content_sha256": content_sha, "file_count": len(rows), "files": rows,
            "excluded": excluded,
            "ignored_scope": "Git-ignored runtime/private/environment paths are omitted; not recursively inventoried",
            "credential_audit": {"passed": True, "files_scanned": len(rows),
                "shared_pattern_rules": sorted(credentials.PATTERNS), "known_private_values_checked": bool(private_values)},
        }
        staged_manifest = staging / manifest_path.name
        staged_manifest.write_bytes(encoded(manifest))
        verification = verify_snapshot(staged_manifest)
        assert_unchanged(root, state, paths, rows)
        if archive_path.exists() != manifest_path.exists():
            raise RuntimeError("incomplete immutable snapshot pair; refusing overwrite")
        reused = archive_path.exists()
        if reused:
            old = json.loads(regular_file(destination, manifest_path.name).read_bytes())
            if (old.get("snapshot_id") != snapshot_id or old.get("archive_sha256") != manifest["archive_sha256"]
                    or old.get("files") != rows):
                raise RuntimeError("immutable snapshot collision")
            verification = verify_snapshot(manifest_path)
            manifest = old
        else:
            # Exclusive create prevents replacing any published immutable artifact.
            for target, source in ((archive_path, staged_archive), (manifest_path, staged_manifest)):
                with target.open("xb") as handle:
                    handle.write(source.read_bytes())
            # Bind the receipt to persisted bytes, not merely the staged copy.
            if manifest_path.read_bytes() != encoded(manifest):
                raise RuntimeError("persisted snapshot manifest bytes differ from staged manifest")
            verification = verify_snapshot(manifest_path)
        assert_unchanged(root, state, paths, rows)
        pointer = {"schema_version": SCHEMA, **state, "snapshot_id": snapshot_id,
                   "published_utc": datetime.now(timezone.utc).isoformat(),
                   "archive": archive_path.name, "archive_sha256": manifest["archive_sha256"],
                   "manifest": manifest_path.name, "manifest_sha256": digest(manifest_path.read_bytes()),
                   "content_sha256": content_sha, "verification": verification, "reused": reused,
                   "retention_action": "none"}
        staged_pointer = staging / POINTER
        staged_pointer.write_bytes(encoded(pointer))
        if (destination / POINTER).is_symlink():
            raise RuntimeError("snapshot pointer cannot be a symlink")
        os.replace(staged_pointer, destination / POINTER)
    return pointer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--vault-project", type=Path, default=Path.home() / "OneDrive/thevault/projects/forex")
    parser.add_argument("--private-file", type=Path, action="append")
    parser.add_argument("--verify", type=Path, metavar="MANIFEST", help="Only offline-verify this manifest and archive")
    args = parser.parse_args()
    try:
        result = verify_snapshot(args.verify) if args.verify else sync_worktree_snapshot(
            args.root, args.vault_project, private_files=args.private_file)
    except (OSError, RuntimeError, ValueError, SyntaxError, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        # Never include command stdout, source lines, or credential values in error output.
        error = "source compilation failed" if isinstance(exc, SyntaxError) else str(exc)
        print(json.dumps({"status": "failed", "error": error}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
