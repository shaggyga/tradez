#!/usr/bin/env python3
"""Verify and import credential-free Forex vault checkpoints safely."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


MANIFEST_NAMES = ("MODEL_CHECKPOINT_MANIFEST.json", "MANIFEST.json")
SECRET_NAMES = {
    ".env",
    "creds",
    "creds.py",
    "creds.txt",
    "credentials.json",
    "secrets.json",
}
SECRET_SUFFIXES = (".key", ".pem", ".p12", ".pfx")


class VaultImportError(RuntimeError):
    pass


@dataclass(frozen=True)
class ArchiveAudit:
    path: Path
    archive_sha256: str
    manifest_name: str
    manifest: dict[str, Any]
    members: dict[str, zipfile.ZipInfo]

    @property
    def rows(self) -> list[dict[str, Any]]:
        return list(self.manifest["files"])


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def io_path(path: Path) -> Path:
    """Use Windows extended paths so deep manifested trees remain importable."""
    if os.name != "nt":
        return path
    value = str(path.resolve())
    if len(value) < 248:
        return Path(value)
    if value.startswith("\\\\?\\"):
        return Path(value)
    if value.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + value[2:])
    return Path("\\\\?\\" + value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with io_path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_member(name: str, *, allow_directory: bool = False) -> str:
    value = name.replace("\\", "/")
    if value.endswith("/"):
        if allow_directory:
            value = value.rstrip("/")
        else:
            raise VaultImportError(f"manifest path is a directory: {name}")
    path = PurePosixPath(value)
    parts = path.parts
    if not value or value.startswith("/") or not parts:
        raise VaultImportError(f"invalid archive path: {name}")
    if any(part in {"", ".", ".."} for part in parts):
        raise VaultImportError(f"unsafe archive path: {name}")
    if any(":" in part for part in parts):
        raise VaultImportError(f"archive path contains a drive or stream: {name}")
    return path.as_posix()


def _is_secret_path(relative: str) -> bool:
    name = PurePosixPath(relative).name.lower()
    return name in SECRET_NAMES or name.endswith(SECRET_SUFFIXES)


def _member_map(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    members: dict[str, zipfile.ZipInfo] = {}
    for info in archive.infolist():
        if info.is_dir() or info.filename.endswith(("/", "\\")):
            _normalized_member(info.filename, allow_directory=True)
            continue
        relative = _normalized_member(info.filename)
        if relative in members:
            raise VaultImportError(f"duplicate archive member: {relative}")
        if _is_secret_path(relative):
            raise VaultImportError(f"credential-like archive member is prohibited: {relative}")
        members[relative] = info
    return members


def _hash_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> str:
    digest = hashlib.sha256()
    with archive.open(info, "r") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def audit_archive(path: Path, expected_sha256: str = "") -> ArchiveAudit:
    archive_path = path.resolve()
    if not archive_path.is_file():
        raise VaultImportError(f"archive does not exist: {archive_path}")
    archive_sha256 = sha256_file(archive_path)
    if expected_sha256 and archive_sha256 != expected_sha256.lower():
        raise VaultImportError(
            f"archive hash mismatch for {archive_path.name}: "
            f"expected {expected_sha256.lower()}, observed {archive_sha256}"
        )
    try:
        archive = zipfile.ZipFile(archive_path)
    except zipfile.BadZipFile as exc:
        raise VaultImportError(f"invalid ZIP archive: {archive_path}") from exc
    with archive:
        members = _member_map(archive)
        manifest_names = [name for name in MANIFEST_NAMES if name in members]
        if len(manifest_names) != 1:
            raise VaultImportError(
                f"archive must contain exactly one recognized manifest; found {manifest_names}"
            )
        manifest_name = manifest_names[0]
        try:
            manifest = json.loads(
                archive.read(members[manifest_name]).decode("utf-8-sig")
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise VaultImportError(f"invalid manifest in {archive_path.name}") from exc
        rows = manifest.get("files")
        if not isinstance(rows, list) or not rows:
            raise VaultImportError(f"manifest has no file inventory: {archive_path.name}")
        seen: set[str] = set()
        for raw in rows:
            if not isinstance(raw, dict):
                raise VaultImportError("manifest file row is not an object")
            relative = _normalized_member(str(raw.get("path", "")))
            if relative in seen:
                raise VaultImportError(f"duplicate manifest path: {relative}")
            seen.add(relative)
            if _is_secret_path(relative):
                raise VaultImportError(f"credential-like manifest path is prohibited: {relative}")
            info = members.get(relative)
            if info is None:
                raise VaultImportError(f"manifest member is absent: {relative}")
            expected_size = int(raw.get("size", -1))
            if info.file_size != expected_size:
                raise VaultImportError(
                    f"size mismatch for {relative}: expected {expected_size}, observed {info.file_size}"
                )
            observed = _hash_member(archive, info)
            expected = str(raw.get("sha256", "")).lower()
            if observed != expected:
                raise VaultImportError(
                    f"content hash mismatch for {relative}: expected {expected}, observed {observed}"
                )
        extras = sorted(set(members) - seen - {manifest_name})
        if extras:
            raise VaultImportError(f"archive contains unmanifested files: {extras[:5]}")
    return ArchiveAudit(
        path=archive_path,
        archive_sha256=archive_sha256,
        manifest_name=manifest_name,
        manifest=manifest,
        members=members,
    )


def _safe_target(root: Path, relative: str) -> Path:
    target = (root / PurePosixPath(relative)).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError as exc:
        raise VaultImportError(f"archive member escapes import root: {relative}") from exc
    return target


def _extract_archive(audit: ArchiveAudit, root: Path, index: int) -> None:
    with zipfile.ZipFile(audit.path) as archive:
        members = _member_map(archive)
        for row in audit.rows:
            relative = _normalized_member(str(row["path"]))
            target = _safe_target(root, relative)
            io_path(target.parent).mkdir(parents=True, exist_ok=True)
            with archive.open(members[relative], "r") as source, io_path(target).open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
        manifest_dir = root / ".vault_import_manifests"
        io_path(manifest_dir).mkdir(parents=True, exist_ok=True)
        manifest_copy = manifest_dir / f"{index:02d}_{audit.path.stem}_{audit.manifest_name}"
        io_path(manifest_copy).write_text(
            json.dumps(audit.manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _merged_inventory(audits: Iterable[ArchiveAudit]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for audit in audits:
        for raw in audit.rows:
            row = dict(raw)
            row["path"] = _normalized_member(str(row["path"]))
            merged[row["path"]] = row
    return merged


def _verify_import(root: Path, inventory: dict[str, dict[str, Any]]) -> None:
    for relative, row in inventory.items():
        path = _safe_target(root, relative)
        physical = io_path(path)
        if not physical.is_file():
            raise VaultImportError(f"imported file is absent: {relative}")
        if physical.stat().st_size != int(row["size"]):
            raise VaultImportError(f"imported file size mismatch: {relative}")
        if sha256_file(path) != str(row["sha256"]).lower():
            raise VaultImportError(f"imported file hash mismatch: {relative}")


def import_archives(
    archives: list[Path],
    destination: Path,
    *,
    expected_hashes: list[str] | None = None,
    verify_only: bool = False,
) -> dict[str, Any]:
    if not archives:
        raise VaultImportError("at least one archive is required")
    hashes = expected_hashes or []
    if hashes and len(hashes) != len(archives):
        raise VaultImportError("expected hash count must match archive count")
    audits = [
        audit_archive(path, hashes[index] if hashes else "")
        for index, path in enumerate(archives)
    ]
    inventory = _merged_inventory(audits)
    report: dict[str, Any] = {
        "schema_version": 1,
        "created_utc": utc_iso(),
        "execution_policy": "offline_import_only_no_account_or_network_activity",
        "destination": str(destination.resolve()),
        "verify_only": verify_only,
        "archives": [
            {
                "path": str(audit.path),
                "archive_sha256": audit.archive_sha256,
                "manifest_name": audit.manifest_name,
                "manifest_content_sha256": audit.manifest.get("content_sha256", ""),
                "manifest_files": len(audit.rows),
            }
            for audit in audits
        ],
        "merged_files": len(inventory),
        "credential_files": 0,
        "account_processes_started": 0,
        "status": "verified" if verify_only else "pending",
    }
    if verify_only:
        return report

    destination = destination.resolve()
    if destination.exists():
        if not destination.is_dir() or any(destination.iterdir()):
            raise VaultImportError(f"destination must be absent or empty: {destination}")
        destination.rmdir()
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.importing.{os.getpid()}"
    if staging.exists():
        raise VaultImportError(f"staging path already exists: {staging}")
    staging.mkdir()
    try:
        for index, audit in enumerate(audits):
            _extract_archive(audit, staging, index)
        _verify_import(staging, inventory)
        receipt = {
            **report,
            "status": "imported_and_verified",
            "imported_utc": utc_iso(),
            "merged_files": len(inventory),
        }
        io_path(staging / "VAULT_IMPORT_RECEIPT.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(io_path(staging), io_path(destination))
        return receipt
    except Exception:
        shutil.rmtree(io_path(staging), ignore_errors=True)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--overlay", type=Path, action="append", default=[])
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument(
        "--expected-sha256",
        action="append",
        default=[],
        help="Expected archive hashes in base-then-overlay order.",
    )
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = import_archives(
            [args.base, *args.overlay],
            args.destination,
            expected_hashes=[value.lower() for value in args.expected_sha256],
            verify_only=args.verify_only,
        )
    except (OSError, VaultImportError, zipfile.BadZipFile) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2), file=sys.stderr)
        return 2
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
