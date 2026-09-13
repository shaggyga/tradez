#!/usr/bin/env python3
"""Archive explicitly reviewed legacy Forex vault records, without deletion.

The default is a read-only plan printed to stdout. ``--apply`` moves only
explicit ``--target`` direct children to a new local run directory. Stop vault
writers while applying. An interrupted run is recoverable using plan.json and
the append-only numbered journal; existing runs are never reused or overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
from pathlib import Path
from typing import Any

try:
    from .vault_record_consolidation import utc_now
except ImportError:  # Direct script invocation.
    from vault_record_consolidation import utc_now


FOREX_VAULT_PROJECT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex")
ARCHIVE_ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad\artifacts\vault_cleanup")
CURRENT_MANIFEST = "SHARED_PROJECT_STATE_CURRENT.json"
ALLOWED_DIRECTORIES = frozenset(
    {"artifacts", "central", "current_records", "projects", "research", "updates"}
)
PROTECTED_NAMES = frozenset(
    name.casefold()
    for name in (
        "source", "README.md", "AUDIT_START_HERE.md", "RECREATION.md",
        "maintenance", CURRENT_MANIFEST, "RUNTIME_STATUS_CURRENT.json",
        "SOURCE_FEATURE_INDEX_CURRENT.json",
    )
)
WINDOWS_RESERVED = {"con", "prn", "aux", "nul"} | {
    f"{prefix}{number}" for prefix in ("com", "lpt") for number in range(1, 10)
}


def io_path(path: Path) -> Path:
    """Long-path I/O only; never resolve redirects or change audit path spelling."""
    if os.name != "nt":
        return Path(path)
    value = os.path.abspath(path)
    if value.startswith("\\\\?\\") or len(value) < 248:
        return Path(value)
    if value.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + value[2:])
    return Path("\\\\?\\" + value)


def _resolved(path: Path, *, strict: bool) -> Path:
    value = str(io_path(path).resolve(strict=strict))
    if os.name == "nt":
        if value.startswith("\\\\?\\UNC\\"):
            value = "\\\\" + value[8:]
        elif value.startswith("\\\\?\\"):
            value = value[4:]
    return Path(value)


def _lexists(path: Path) -> bool:
    return os.path.lexists(io_path(path))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with io_path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _single_name(name: str) -> str:
    if (
        not isinstance(name, str) or not name or name in {".", ".."}
        or name != name.rstrip(" .")
        or any(ord(character) < 32 or character in '\\/:*?"<>|' for character in name)
        or name.split(".", 1)[0].casefold() in WINDOWS_RESERVED
    ):
        raise ValueError(f"target must be a safe direct-child name: {name!r}")
    return name


def _safe_node(path: Path) -> os.stat_result:
    """Reject redirects before resolving; permit OneDrive cloud placeholders."""
    info = io_path(path).lstat()
    tag = getattr(info, "st_reparse_tag", 0)
    reparse = getattr(info, "st_file_attributes", 0) & 0x400
    cloud = (tag & 0xFFFF0FFF) == 0x9000001A
    if stat.S_ISLNK(info.st_mode) or (reparse and not cloud):
        raise RuntimeError(f"symlink/junction or unsupported reparse point: {path}")
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise RuntimeError(f"not a regular file or directory: {path}")
    return info


def _safe_chain(path: Path) -> None:
    for item in reversed((path, *path.parents)):
        if _lexists(item):
            _safe_node(item)


def _within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def scan_target(path: Path) -> dict[str, Any]:
    """Inventory files and all directories, including empty ones, without links."""
    root = _resolved(path.parent, strict=True)
    files: list[dict[str, Any]] = []
    directories: list[str] = []

    def visit(item: Path) -> None:
        before = _safe_node(item)
        if not _within(_resolved(item, strict=True), root):
            raise RuntimeError(f"target member escaped its root: {item}")
        relative = item.relative_to(root).as_posix()
        if stat.S_ISDIR(before.st_mode):
            directories.append(relative)
            with os.scandir(io_path(item)) as entries:
                children = sorted((item / entry.name for entry in entries), key=str)
            for child in children:
                visit(child)
        else:
            digest = sha256_file(item)
            after = _safe_node(item)
            identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
            if identity(before) != identity(after):
                raise RuntimeError(f"file changed while hashing: {item}")
            files.append({"path": relative, "bytes": after.st_size, "sha256": digest})

    visit(path)
    files.sort(key=lambda entry: entry["path"])
    directories.sort()
    inventory = {"files": files, "directories": directories}
    return {
        "name": path.name,
        "kind": "directory" if directories else "file",
        "file_count": len(files),
        "directory_count": len(directories),
        "bytes": sum(entry["bytes"] for entry in files),
        "tree_sha256": hashlib.sha256(_json_bytes(inventory)).hexdigest(),
        **inventory,
    }


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_once(path: Path, payload: dict[str, Any]) -> bytes:
    encoded = _json_bytes(payload)
    with io_path(path).open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    if io_path(path).read_bytes() != encoded:
        raise RuntimeError(f"audit artifact verification failed: {path}")
    return encoded


def _manifest(project: Path) -> tuple[bytes, set[str]]:
    path = project / CURRENT_MANIFEST
    if not stat.S_ISREG(_safe_node(path).st_mode):
        raise RuntimeError("current manifest must be a regular file")
    encoded = io_path(path).read_bytes()
    data = json.loads(encoded)
    records = data.get("records")
    if not isinstance(records, list) or data.get("record_count") != len(records):
        raise ValueError("current manifest must contain records and matching record_count")
    names = {_single_name(record["name"]).casefold() for record in records}
    if len(names) != len(records):
        raise ValueError("duplicate current manifest record names")
    return encoded, names | set(PROTECTED_NAMES)


def validate_paths(
    project: Path, archive_root: Path, run_id: str, target_names: list[str]
) -> tuple[Path, Path, list[Path], bytes, set[str]]:
    project, archive_root = Path(project).absolute(), Path(archive_root).absolute()
    _safe_chain(project)
    _safe_chain(archive_root)
    resolved_project = _resolved(project, strict=True)
    resolved_archive = _resolved(archive_root, strict=False)
    # Check the whole vault, not just this project's subtree.
    if _within(resolved_archive, resolved_project.parent.parent):
        raise RuntimeError("archive must be outside the OneDrive vault")
    if project != FOREX_VAULT_PROJECT.absolute() or resolved_project != project:
        raise RuntimeError("cleanup requires the exact canonical Forex vault project")
    if archive_root != ARCHIVE_ROOT.absolute() or resolved_archive != archive_root:
        raise RuntimeError("cleanup requires the exact canonical local archive root")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", run_id):
        raise ValueError("run-id must be a safe single directory name")
    _single_name(run_id)
    if not target_names:
        raise ValueError("at least one explicit --target is required")
    names = [_single_name(name) for name in target_names]
    if len({name.casefold() for name in names}) != len(names):
        raise ValueError("duplicate targets are forbidden")
    destination = archive_root / run_id
    if _lexists(destination):
        raise FileExistsError(f"archive run already exists; do not reuse: {destination}")
    manifest_bytes, protected = _manifest(project)
    sources = []
    for name in names:
        if name.casefold() in protected:
            raise ValueError(f"protected source/current record: {name}")
        source = project / name
        info = _safe_node(source)
        resolved = _resolved(source, strict=True)
        if resolved.parent != project:
            raise RuntimeError(f"target is not a direct Forex project child: {source}")
        if stat.S_ISDIR(info.st_mode) and name not in ALLOWED_DIRECTORIES:
            raise ValueError(f"unreviewed directory target: {name}")
        sources.append(source)
    return project, destination, sources, manifest_bytes, protected


def cleanup(
    project: Path,
    archive_root: Path,
    run_id: str,
    target_names: list[str],
    apply: bool = False,
) -> dict[str, Any]:
    project, destination, sources, manifest_bytes, protected = validate_paths(
        project, archive_root, run_id, target_names
    )
    targets = [scan_target(source) for source in sources]
    readme = project / "README.md"
    readme_inventory = scan_target(readme) if _lexists(readme) else None
    if readme_inventory and readme_inventory["kind"] != "file":
        raise RuntimeError("README.md must be a regular file")
    plan = {
        "schema_version": "forex_vault_record_cleanup_v1",
        "status": "planned",
        "generated_utc": utc_now(),
        "recoverable": True,
        "vault_project": str(project),
        "archive_run": str(destination),
        "current_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "protected_names": sorted(protected),
        "targets": targets,
        "moved_files": sum(item["file_count"] for item in targets),
        "moved_bytes": sum(item["bytes"] for item in targets),
        "readme_backup": readme_inventory,
        "recovery": [
            {"from": str(destination / "payload" / item["name"]),
             "to": str(project / item["name"])} for item in targets
        ],
        "readme_recovery_path": str(destination / "preserved" / "README.md")
        if readme_inventory else None,
    }
    if not apply:
        return plan

    io_path(destination).mkdir(parents=True, exist_ok=False)
    io_path(destination / "payload").mkdir()
    journal_dir = destination / "journal"
    io_path(journal_dir).mkdir()
    plan_bytes = _write_once(destination / "plan.json", plan)
    plan_sha = hashlib.sha256(plan_bytes).hexdigest()
    sequence = 0

    def journal(status: str, **details: Any) -> None:
        nonlocal sequence
        _write_once(journal_dir / f"{sequence:04d}-{status}.json", {
            "status": status, "utc": utc_now(), "plan_sha256": plan_sha, **details,
        })
        sequence += 1

    def verify_context() -> None:
        _safe_chain(project)
        _safe_chain(destination)
        if io_path(destination / "plan.json").read_bytes() != plan_bytes:
            raise RuntimeError("immutable cleanup plan changed")
        if _manifest(project)[0] != manifest_bytes:
            raise RuntimeError("current manifest changed during cleanup")

    def verify(path: Path, expected: dict[str, Any]) -> None:
        if scan_target(path) != expected:
            raise RuntimeError(f"integrity verification mismatch: {path}")

    journal("prepared", target_count=len(targets))
    completed = []
    try:
        verify_context()
        # Validate every selected tree before making the first move.
        for source, expected in zip(sources, targets):
            verify(source, expected)
        if readme_inventory:
            verify(readme, readme_inventory)
            io_path(destination / "preserved").mkdir()
            backup = destination / "preserved" / "README.md"
            shutil.copy2(io_path(readme), io_path(backup))
            verify(backup, readme_inventory)
            journal("readme-preserved", path=str(backup))
        for source, expected in zip(sources, targets):
            target = destination / "payload" / source.name
            verify_context()
            verify(source, expected)
            if _lexists(target):
                raise FileExistsError(target)
            journal("move-started", source=str(source), destination=str(target))
            # No copy-and-delete fallback: same-volume rename preserves the tree.
            io_path(source).rename(io_path(target))
            verify(target, expected)
            if _lexists(source):
                raise RuntimeError(f"source still exists after move: {source}")
            completed.append(source.name)
            journal("move-verified", name=source.name, tree_sha256=expected["tree_sha256"])
        verify_context()
        for expected in targets:
            verify(destination / "payload" / expected["name"], expected)
            if _lexists(project / expected["name"]):
                raise RuntimeError(f"source reappeared after move: {expected['name']}")
        if readme_inventory:
            verify(destination / "preserved" / "README.md", readme_inventory)
        receipt = {
            **plan, "status": "completed_verified", "completed_utc": utc_now(),
            "plan_sha256": plan_sha, "completed_targets": completed,
        }
        _write_once(destination / "receipt.json", receipt)
        journal("completed-verified", completed_targets=completed)
        return receipt
    except Exception as exc:
        journal("interrupted-recoverable", completed_targets=completed, error=str(exc))
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault-project", type=Path, default=FOREX_VAULT_PROJECT)
    parser.add_argument("--archive-root", type=Path, default=ARCHIVE_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--target", action="append", required=True, dest="targets")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true", help="read-only plan (default)")
    mode.add_argument("--apply", action="store_true", help="apply the reviewed explicit targets")
    args = parser.parse_args()
    result = cleanup(args.vault_project, args.archive_root, args.run_id, args.targets, args.apply)
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
