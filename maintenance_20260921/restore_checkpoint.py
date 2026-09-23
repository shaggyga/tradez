"""Verify or restore a portable Forex checkpoint using only the standard library.

Default action is full offline verification. Restoration requires a new, absent
destination and never executes restored code, installs dependencies, or contacts
any service. A failed restore is left in place for inspection; nothing is deleted.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import zipfile

SCHEMA = "forex_portable_checkpoint_v1"
CHUNK = 1024 * 1024
RESERVED = {"con", "prn", "aux", "nul", "clock$", "conin$", "conout$"}
RESERVED.update(f"{stem}{number}" for stem in ("com", "lpt") for number in range(1, 10))
RESERVED.update(f"{stem}{number}" for stem in ("com", "lpt") for number in "¹²³")


class CheckpointError(ValueError):
    """The checkpoint or requested destination is unsafe or inconsistent."""


def safe_relative(value: object, *, allow_empty: bool = False) -> str:
    if allow_empty and value == "":
        return ""
    if not isinstance(value, str) or not value or value.startswith("/"):
        raise CheckpointError("A manifest path is not a relative path")
    if "\\" in value or any(ord(char) < 32 or char in '<>:"|?*' for char in value):
        raise CheckpointError("A manifest path contains unsafe characters")
    for part in value.split("/"):
        if part in ("", ".", "..") or part.endswith((".", " ")):
            raise CheckpointError("A manifest path contains an unsafe component")
        if part.split(".")[0].casefold() in RESERVED:
            raise CheckpointError("A manifest path contains a reserved Windows name")
    return value


def absolute(path: Path) -> Path:
    # Do not resolve symlinks before inspecting the original chain.
    result = Path(os.path.abspath(os.fspath(path)))
    if result.anchor.startswith(("\\\\", "//")):
        raise CheckpointError("Network share paths are outside offline restore scope")
    return result


def reject_links(path: Path) -> None:
    for candidate in (path, *path.parents):
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_reparse_tag", 0) & 0x20000000:
            raise CheckpointError("A path contains a link or name-surrogate reparse point")


def contained(root: Path, name: str) -> Path:
    path = root.joinpath(*safe_relative(name).split("/"))
    if not path.is_relative_to(root):
        raise CheckpointError("A path escapes its root")
    reject_links(path)
    return path


def record(value: object) -> dict:
    if not isinstance(value, dict):
        raise CheckpointError("A file record is not an object")
    name = safe_relative(value.get("path"))
    size, digest = value.get("bytes"), value.get("sha256")
    if type(size) is not int or size < 0:
        raise CheckpointError("A file record has an invalid byte count")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
        raise CheckpointError("A file record has an invalid SHA256")
    return {"path": name, "bytes": size, "sha256": digest.lower()}


def unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise CheckpointError("JSON contains a duplicate object key")
        result[key] = value
    return result


class PathRegistry:
    """Reject case aliases, duplicate files, and file/directory collisions."""

    def __init__(self) -> None:
        self.nodes: dict[str, tuple[str, bool]] = {}

    def add(self, name: str, *, directory: bool = False) -> None:
        safe_relative(name)
        parts = name.split("/")
        for index in range(1, len(parts) + 1):
            current = "/".join(parts[:index])
            is_dir = index < len(parts) or directory
            key = current.casefold()
            previous = self.nodes.get(key)
            if previous is not None:
                if previous != (current, is_dir) or not is_dir:
                    raise CheckpointError("Duplicate, case-alias, or file/directory path collision")
            else:
                self.nodes[key] = (current, is_dir)


def stream_checked(source, expected: dict, output=None) -> None:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(CHUNK):
        size += len(chunk)
        if size > expected["bytes"]:
            raise CheckpointError("File content exceeds its declared size")
        digest.update(chunk)
        if output is not None:
            output.write(chunk)
    if size != expected["bytes"] or digest.hexdigest() != expected["sha256"]:
        raise CheckpointError("File size or SHA256 mismatch")


def check_file(root: Path, expected: dict, output=None) -> None:
    path = contained(root, expected["path"])
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink > 1:
        raise CheckpointError("A checkpoint input is not a regular file")
    if info.st_size != expected["bytes"]:
        raise CheckpointError("Checkpoint file size mismatch")
    with path.open("rb") as source:
        stream_checked(source, expected, output)


def joined(prefix: str, name: str) -> str:
    return f"{prefix}/{name}" if prefix else name


def inspect_archive(root: Path, archive: dict, destination: Path | None = None) -> None:
    archive_path = contained(root, archive["path"])
    members = {item["path"]: item for item in archive["members"]}
    with zipfile.ZipFile(archive_path, "r") as source:
        actual = {}
        registry = PathRegistry()
        seen_names = set()
        for info in source.infolist():
            is_dir = info.is_dir()
            name = info.filename[:-1] if is_dir else info.filename
            safe_relative(name)
            if name.casefold() in seen_names:
                raise CheckpointError("ZIP contains a duplicate or case-alias member")
            seen_names.add(name.casefold())
            registry.add(name, directory=is_dir)
            mode = (info.external_attr >> 16) & 0xFFFF
            kind = stat.S_IFMT(mode)
            if kind not in (0, stat.S_IFDIR if is_dir else stat.S_IFREG):
                raise CheckpointError("ZIP contains a link or nonregular member")
            if info.flag_bits & 1:
                raise CheckpointError("Encrypted ZIP members are not supported")
            if is_dir:
                if info.file_size != 0:
                    raise CheckpointError("ZIP directory contains a payload")
                continue
            if name not in members or info.file_size != members[name]["bytes"]:
                raise CheckpointError("ZIP inventory or member size mismatch")
            actual[name] = info
        if set(actual) != set(members):
            raise CheckpointError("ZIP member inventory does not match the manifest")
        for name, info in actual.items():
            with source.open(info, "r") as payload:
                if destination is None:
                    stream_checked(payload, members[name])
                else:
                    target = contained(destination, joined(archive["target_prefix"], name))
                    target.parent.mkdir(parents=True, exist_ok=True)
                    reject_links(target)
                    with target.open("xb") as output:
                        stream_checked(payload, members[name], output)


def load_plan(checkpoint: Path) -> dict:
    checkpoint = absolute(checkpoint)
    reject_links(checkpoint)
    manifest_path = contained(checkpoint, "MANIFEST.json")
    if not stat.S_ISREG(manifest_path.stat().st_mode):
        raise CheckpointError("MANIFEST.json is not a regular file")
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw, object_pairs_hook=unique_object)
    if not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA:
        raise CheckpointError("Unsupported checkpoint manifest schema")
    if not isinstance(manifest.get("files"), list) or not isinstance(manifest.get("archives"), list):
        raise CheckpointError("Manifest requires files and archives arrays")
    input_paths, output_paths = PathRegistry(), PathRegistry()
    input_paths.add("MANIFEST.json")
    output_paths.add("checkpoint_records/MANIFEST.json")
    output_paths.add("checkpoint_records/RESTORE_VERIFICATION.json")
    files = [record(item) for item in manifest["files"]]
    file_map = {}
    for item in files:
        input_paths.add(item["path"])
        file_map[item["path"]] = item
    archives, declared_archives = [], set()
    for value in manifest["archives"]:
        if not isinstance(value, dict) or not isinstance(value.get("members"), list):
            raise CheckpointError("An archive record requires a members array")
        name = safe_relative(value.get("path"))
        prefix = safe_relative(value.get("target_prefix"), allow_empty=True)
        if name not in file_map or name.casefold() in declared_archives:
            raise CheckpointError("Archive must have a unique whole-file hash record")
        declared_archives.add(name.casefold())
        members = [record(item) for item in value["members"]]
        member_paths = PathRegistry()
        for item in members:
            member_paths.add(item["path"])
            output_paths.add(joined(prefix, item["path"]))
        archives.append({"path": name, "target_prefix": prefix, "members": members})
    documents = []
    for item in files:
        if item["path"].casefold() in declared_archives:
            continue
        if item["path"].lower().endswith(".zip"):
            raise CheckpointError("Every ZIP requires a declared member inventory")
        output_paths.add(joined("checkpoint_records", item["path"]))
        documents.append(item)
    return {"checkpoint": checkpoint, "raw_manifest": raw,
            "files": files, "archives": archives, "documents": documents}


def process_checkpoint(checkpoint: Path, destination: Path | None = None) -> dict:
    plan = load_plan(checkpoint)
    root = plan["checkpoint"]
    if destination is not None:
        destination = absolute(destination)
        reject_links(destination)
        if os.path.lexists(destination):
            raise CheckpointError("Restore destination must not already exist")
        if destination.is_relative_to(root) or root.is_relative_to(destination):
            raise CheckpointError("Restore destination and checkpoint must be separate trees")
        if not destination.parent.is_dir():
            raise CheckpointError("Restore destination parent must already exist")
    for item in plan["files"]:
        check_file(root, item)
    for archive in plan["archives"]:
        inspect_archive(root, archive)
    report = {
        "schema": "forex_checkpoint_verification_v1", "status": "verified",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "restore" if destination is not None else "verify_only",
        "manifest_sha256": hashlib.sha256(plan["raw_manifest"]).hexdigest(),
        "verified_files": plan["files"],
        "verified_archives": plan["archives"],
        "file_count": len(plan["files"]), "archive_count": len(plan["archives"]),
        "archive_member_count": sum(len(item["members"]) for item in plan["archives"]),
        "code_executed": False, "network_activity": False,
        "dependencies_installed": False, "git_history_reconstructed": False,
        "source_account_or_runtime_access": False,
    }
    if destination is not None:
        destination.mkdir(exist_ok=False)
        records_dir = destination / "checkpoint_records"
        records_dir.mkdir()
        for archive in plan["archives"]:
            inspect_archive(root, archive, destination)
        for item in plan["documents"]:
            target = contained(records_dir, item["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            reject_links(target)
            with target.open("xb") as output:
                check_file(root, item, output)
        # Detect source replacement during extraction, including changed ZIP headers.
        for item in plan["files"]:
            check_file(root, item)
        if contained(root, "MANIFEST.json").read_bytes() != plan["raw_manifest"]:
            raise CheckpointError("Manifest changed during restoration")
        restored_count = 0
        for archive in plan["archives"]:
            for member in archive["members"]:
                check_file(destination, {**member, "path": joined(archive["target_prefix"], member["path"])})
                restored_count += 1
        for item in plan["documents"]:
            check_file(records_dir, item)
            restored_count += 1
        with (records_dir / "MANIFEST.json").open("xb") as output:
            output.write(plan["raw_manifest"])
        report["destination"] = str(destination)
        report["restored_file_count"] = restored_count
        report["restored_files_sha256_verified"] = True
        with (records_dir / "RESTORE_VERIFICATION.json").open("x", encoding="utf-8") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", type=Path, default=Path(__file__).resolve().parent)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--verify-only", action="store_true", help="Verify every file and ZIP payload (default)")
    action.add_argument("--destination", type=Path, help="Restore into this new, absent directory")
    args = parser.parse_args()
    try:
        report = process_checkpoint(args.checkpoint, args.destination)
    except (OSError, ValueError, RuntimeError, zipfile.BadZipFile, NotImplementedError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc),
                          "partial_destination_may_remain": args.destination is not None}), file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
