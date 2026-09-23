#!/usr/bin/env python3
"""Read the shared Vault and retrieve pinned artifacts as bytes, using only stdlib."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys
import zipfile

MAX_BYTES = 256 * 1024 * 1024
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_MEMBERS = 4096
# MS-FSCC 2.1.2.1: Cloud Files tags are CLOUD plus variants 1 through F.
# https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/c8e77b37-3909-4fe6-a4ea-2b9d423b1ee4
REPARSE_ATTRIBUTE = 0x400
NAME_SURROGATE_TAG = 0x20000000
CLOUD_TAG = 0x9000001A
CLOUD_VARIANT_MASK = 0x0000F000
SOURCE_RECALL_ATTRIBUTES = 0x1000 | 0x40000 | 0x400000
MANIFEST_NAME = "SAVED_ARTIFACTS_MANIFEST.json"
REGISTRY = Path(__file__).resolve().parents[1] / "artifacts" / "registry.json"
RESERVED = {"CON", "PRN", "AUX", "NUL", "CLOCK$"} | {
    f"{prefix}{n}" for prefix in ("COM", "LPT") for n in range(1, 10)
}


class ReviewRequired(Exception):
    """Preserve evidence and reconcile the problem before continuing."""


def require(condition, message):
    if not condition:
        raise ReviewRequired(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def safe_name(name):
    require(isinstance(name, str) and 0 < len(name) <= 240, "Invalid relative path")
    parts = name.split("/")
    for part in parts:
        require(bool(re.fullmatch(r"[A-Za-z0-9_.-]+", part)) and part not in (".", "..")
                and not part.endswith((".", " "))
                and part.split(".")[0].upper() not in RESERVED,
                f"Unsafe or non-portable relative path: {name!r}")
    return parts


def no_links(path, *, allow_cloud=False):
    """Reject redirects; source reads may allow only vetted Cloud Files tags."""
    path = Path(os.path.abspath(path))
    for item in reversed((path, *path.parents)):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        attributes = getattr(info, "st_file_attributes", 0)
        tag = getattr(info, "st_reparse_tag", 0)
        permitted_cloud = (allow_cloud and not (tag & NAME_SURROGATE_TAG)
                           and (tag & ~CLOUD_VARIANT_MASK) == CLOUD_TAG)
        require(not stat.S_ISLNK(info.st_mode)
                and not (tag & NAME_SURROGATE_TAG)
                and (not (attributes & REPARSE_ATTRIBUTE) or permitted_cloud),
                f"Symlink or reparse point requires review: {item}")
    return path


def root_path(value, *, allow_cloud=False):
    path = no_links(value, allow_cloud=allow_cloud)
    require(path.is_dir(), f"Directory unavailable: {path}")
    return path


def under(root, relative, *, allow_cloud=False):
    return no_links(root.joinpath(*safe_name(relative)), allow_cloud=allow_cloud)


def read_bytes(path, limit=MAX_JSON_BYTES, *, allow_cloud=False):
    path = no_links(path, allow_cloud=allow_cloud)
    require(path.is_file(), f"Required file unavailable: {path}")
    attributes = getattr(path.lstat(), "st_file_attributes", 0)
    require(not (allow_cloud and attributes & REPARSE_ATTRIBUTE
                 and attributes & SOURCE_RECALL_ATTRIBUTES),
            f"Source file is not locally available; make it available offline before retrying: {path}")
    require(path.stat().st_size <= limit, f"File exceeds size limit: {path}")
    with path.open("rb") as handle:
        value = handle.read(limit + 1)
    require(len(value) <= limit, f"File exceeds size limit: {path}")
    return value


def json_bytes(data):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON key: {key}")
            result[key] = value
        return result
    value = json.loads(data.decode("utf-8-sig"), object_pairs_hook=unique_pairs)
    require(isinstance(value, dict), "Expected a JSON object")
    return value


def checked_record(data, record, description):
    require(type(record.get("bytes")) is int and record["bytes"] >= 0,
            f"Invalid byte count: {description}")
    require(len(data) == record["bytes"] and sha256(data) == record.get("sha256"),
            f"Byte count or SHA256 mismatch: {description}")


def inventory(records):
    require(isinstance(records, list), "Expected manifest member list")
    result, folded = {}, set()
    for record in records:
        name = record["path"]
        safe_name(name)
        require(name.casefold() not in folded, f"Duplicate/case-colliding member: {name}")
        require(type(record.get("bytes")) is int and 0 <= record["bytes"] <= MAX_BYTES
                and bool(re.fullmatch(r"[0-9a-f]{64}", record.get("sha256", ""))),
                f"Invalid manifest record: {name}")
        result[name] = record
        folded.add(name.casefold())
    for name in result:
        parts = name.split("/")
        require(all("/".join(parts[:i]).casefold() not in folded for i in range(1, len(parts))),
                f"File/directory path collision: {name}")
    return result


def load_registry():
    return json_bytes(read_bytes(REGISTRY, allow_cloud=True))


def inspect_archive(archive, artifact):
    """Validate every entry and all manifest/identity bindings before any writes."""
    require(sha256(archive) == artifact["archive_sha256"], "Archive SHA256 mismatch")
    require(len(archive) == artifact["archive_bytes"], "Archive byte count mismatch")
    handle = zipfile.ZipFile(io.BytesIO(archive))
    infos = handle.infolist()
    require(0 < len(infos) <= MAX_MEMBERS, "ZIP member count exceeds limit")
    require(sum(info.file_size for info in infos) <= MAX_BYTES, "ZIP expansion exceeds 256 MiB")
    names, folded, directories = {}, set(), {}
    for info in infos:
        name = info.filename
        safe_name(info.orig_filename)
        parts = safe_name(name)
        require(name == info.orig_filename, "Unsafe normalized or truncated ZIP name")
        require(name.casefold() not in folded, f"Duplicate/case-colliding ZIP path: {name}")
        kind = stat.S_IFMT(info.external_attr >> 16)
        require(kind in (0, stat.S_IFREG) and not info.is_dir()
                and not (info.external_attr & 0x410), f"Non-regular ZIP member: {name}")
        require(not info.flag_bits & 1 and info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED),
                f"Unsupported encrypted/compression ZIP member: {name}")
        require(0 <= info.file_size <= MAX_BYTES, f"Invalid ZIP size: {name}")
        for i in range(1, len(parts)):
            directory = "/".join(parts[:i])
            key = directory.casefold()
            require(key not in directories or directories[key] == directory,
                    f"Case-colliding ZIP directory: {directory}")
            directories[key] = directory
        names[name] = info
        folded.add(name.casefold())
    require(not folded.intersection(directories), "ZIP file/directory collision")
    require(MANIFEST_NAME in names and names[MANIFEST_NAME].file_size <= MAX_JSON_BYTES,
            "Saved-artifact manifest missing or too large")
    manifest_data = handle.read(MANIFEST_NAME)
    require(sha256(manifest_data) == artifact["saved_artifacts_manifest_sha256"],
            "Saved-artifact manifest SHA256 mismatch")
    manifest = json_bytes(manifest_data)
    require(manifest.get("schema_version") == "forex_saved_artifacts_manifest.v1", "Unsupported manifest schema")
    expected = inventory(manifest["original_members"])
    require(MANIFEST_NAME not in expected, "Manifest cannot list itself as an original payload")
    expected[MANIFEST_NAME] = {"path": MANIFEST_NAME, "bytes": len(manifest_data),
                               "sha256": sha256(manifest_data)}
    require(set(names) == set(expected), "ZIP inventory differs from saved-artifact manifest")
    require(len(names) == artifact["archive_member_count"], "ZIP member count differs from registry")
    require(sum(item.file_size for item in infos) == artifact["archive_expanded_bytes"],
            "ZIP expanded byte count differs from registry")
    # Read to EOF so zipfile checks CRC; bound actual as well as declared sizes.
    for name, info in names.items():
        record = expected[name]
        require(info.file_size == record["bytes"], f"ZIP declared byte count mismatch: {name}")
        digest, count = hashlib.sha256(), 0
        with handle.open(info) as source:
            while block := source.read(1024 * 1024):
                count += len(block)
                require(count <= record["bytes"], f"ZIP expanded beyond declared size: {name}")
                digest.update(block)
        require(count == record["bytes"] and digest.hexdigest() == record["sha256"],
                f"ZIP member hash mismatch: {name}")
    identity = json_bytes(handle.read("RUN_IDENTITY.json"))
    completion = json_bytes(handle.read("COMPLETION_MANIFEST.json"))
    fingerprint = artifact["original_run_identity_fingerprint"]
    require(identity.get("fingerprint") == fingerprint
            and manifest.get("original_run_identity_fingerprint") == fingerprint
            and completion.get("run_identity") == identity, "Original run identity mismatch")
    unsigned = {key: value for key, value in identity.items() if key != "fingerprint"}
    canonical = json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    require(sha256(canonical) == fingerprint, "Original run fingerprint does not match its content")
    for name, key in (("RUN_IDENTITY.json", "original_run_identity_sha256"),
                      ("COMPLETION_MANIFEST.json", "original_completion_manifest_sha256")):
        require(expected[name]["sha256"] == artifact[key] == manifest[key], f"Original identity hash mismatch: {name}")
    payloads = inventory(completion["payloads"])
    required = completion["required_payloads"]
    require(isinstance(required, list) and len(set(required)) == len(required)
            and set(required) == set(payloads), "Original completion inventory mismatch")
    originals = {name: record for name, record in expected.items()
                 if name not in (MANIFEST_NAME, "RUN_IDENTITY.json", "COMPLETION_MANIFEST.json")}
    require(payloads == originals and len(payloads) == artifact["payload_count"] == manifest["payload_count"],
            "Payload inventory/count mismatch")
    models = sorted(name for name in payloads if name.endswith(".joblib"))
    require(models == sorted(manifest["model_payloads"]) and len(models) == artifact["model_count"] == manifest["model_count"],
            "Serialized model inventory mismatch")
    require(manifest["recipe_reference"] == artifact["recipe_reference"]
            and manifest["required_source_references"] == artifact["required_source_references"]
            and manifest["original_environment"] == artifact["original_environment"],
            "Original recipe/source/environment reference mismatch")
    return handle, expected


def verify_destination(destination, expected):
    destination = root_path(destination)
    actual, dirs = {}, set()
    for directory, children, files in os.walk(destination, followlinks=False):
        for name in children:
            item = no_links(Path(directory) / name)
            dirs.add(item.relative_to(destination).as_posix())
        for name in files:
            item = no_links(Path(directory) / name)
            relative = item.relative_to(destination).as_posix()
            require(item.is_file(), f"Non-regular destination file: {relative}")
            actual[relative] = item
    expected_dirs = {"/".join(name.split("/")[:i]) for name in expected
                     for i in range(1, len(name.split("/")))}
    require(set(actual) == set(expected) and dirs == expected_dirs,
            "Partial or unexpected destination inventory; preserve and review, no overwrite")
    for name, record in expected.items():
        checked_record(read_bytes(actual[name], MAX_BYTES), record, name)


def retrieve(vault, artifact_id, destination, registry=None):
    vault = root_path(vault, allow_cloud=True)
    registry = load_registry() if registry is None else registry
    require(artifact_id in registry["artifacts"], f"Artifact is not registered: {artifact_id}")
    artifact = registry["artifacts"][artifact_id]
    destination = no_links(destination)
    require(destination != vault and vault not in destination.parents,
            "Retrieval destination must be outside the read-only Vault")
    require(destination.parent.is_dir(), "Destination parent must already exist")
    archive_path = under(vault, artifact["archive_vault_relative_path"], allow_cloud=True)
    archive = read_bytes(archive_path, MAX_BYTES, allow_cloud=True)
    handle, expected = inspect_archive(archive, artifact)
    with handle:
        if destination.exists():
            verify_destination(destination, expected)
            outcome = "reused_verified"
        else:
            # mkdir is exclusive. A competing or interrupted retrieval is left for review.
            destination.mkdir(exist_ok=False)
            for name in sorted(expected):
                target = under(destination, name)
                target.parent.mkdir(parents=True, exist_ok=True)
                no_links(target.parent)
                with handle.open(name) as source, target.open("xb") as output:
                    while block := source.read(1024 * 1024):
                        output.write(block)
            verify_destination(destination, expected)
            outcome = "retrieved_verified"
    return {"status": outcome, "artifact": artifact_id, "destination": str(destination),
            "archive_sha256": artifact["archive_sha256"],
            "original_run_identity_fingerprint": artifact["original_run_identity_fingerprint"],
            "verified_file_count": len(expected), "model_count": artifact["model_count"],
            "payload_count": artifact["payload_count"], "models_loaded": 0, "models_fitted": 0,
            "runtime_requalified": False, "independent_review": False,
            "cloud_sync_verified": False, "new_experiment": False}


def status(vault):
    vault = root_path(vault, allow_cloud=True)
    records, snapshots = {}, {}
    names = ("DESIGN_ALIGNMENT_LATEST.json", "CHECKPOINT_REVIEW_LATEST.json", "REVIEW_QUEUE.json",
             "CHAT_COORDINATION_BOARD.md", "VAULT_FIRST_REUSE.md")
    for name in names:
        data = read_bytes(under(vault, name, allow_cloud=True), allow_cloud=True)
        snapshots[name] = data
        records[name] = {"sha256": sha256(data), "bytes": len(data)}
    design = json_bytes(snapshots[names[0]])
    review = json_bytes(snapshots[names[1]])
    queue = json_bytes(snapshots[names[2]])
    for pointer in (design, review):
        data = read_bytes(under(vault, pointer["manifest"], allow_cloud=True), allow_cloud=True)
        require(sha256(data) == pointer["manifest_sha256"], "Current pointer manifest SHA256 mismatch")
        records[pointer["manifest"]] = {"sha256": sha256(data), "bytes": len(data)}
    # Queue/review can legitimately advance beyond the immutable design package.
    # Report the live queue, not historical next-action text from old package logs.
    active = [step for step in queue["steps"] if step.get("step_id") == queue.get("active_step_id")]
    board = snapshots["CHAT_COORDINATION_BOARD.md"].decode("utf-8-sig")
    for name, old in snapshots.items():
        require(read_bytes(under(vault, name, allow_cloud=True), allow_cloud=True) == old,
                "Vault changed during status read; retry and reconcile ownership before work")
    return {"status": "read_only_snapshot", "vault": str(vault), "design_pointer": design,
            "review_pointer": review, "exact_next_item": queue.get("exact_next_item"),
            "active_step_id": queue.get("active_step_id"), "active_steps": active,
            "queue_step_count": len(queue["steps"]), "queue_updated_utc": queue.get("updated_utc"),
            "coordination_board": board, "read_hashes": records,
            "limitations": ["Advisory local snapshot, not an atomic cross-machine work reservation.",
                            "Pointer manifest hashes checked; full sealed-package integrity is not checked.",
                            "Read the board, current handoff and reuse policy before claiming work.",
                            "No claim, Vault edit, model loading, fitting or cloud sync verification performed."]}


def doctor():
    registry = load_registry()
    versions = {}
    environment = registry["artifacts"]["matched_remaining_saved"]["original_environment"]
    for package in environment:
        if package == "python":
            versions[package] = platform.python_version()
        else:
            try:
                versions[package] = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                versions[package] = None
    return {"status": "read_only_environment_report", "python_executable": sys.executable,
            "python_version": platform.python_version(), "byte_retrieval_uses_stdlib_only": True,
            "installed_metadata": versions, "original_run_environment": environment,
            "version_strings_match": versions == environment, "runtime_requalified": False,
            "models_loaded": 0, "models_fitted": 0, "packages_installed": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor", help="Report Python/package metadata without importing model libraries")
    read = commands.add_parser("status", help="Read current Vault pointers, queue and coordination board")
    read.add_argument("--vault", required=True, type=Path)
    copy = commands.add_parser("retrieve", help="Verify and copy a registered saved artifact; never fit")
    copy.add_argument("--vault", required=True, type=Path)
    copy.add_argument("--artifact", required=True)
    copy.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            result = status(args.vault)
        elif args.command == "retrieve":
            result = retrieve(args.vault, args.artifact, args.destination)
        else:
            result = doctor()
    except (ReviewRequired, OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile,
            NotImplementedError, RuntimeError) as error:
        print(json.dumps({"status": "review_required", "reason": str(error),
                          "next_action": "Preserve existing files and resolve the mismatch; do not refit or overwrite."}, indent=2))
        return 2
    # ASCII JSON also works through Windows pipes using legacy code pages.
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
