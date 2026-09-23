#!/usr/bin/env python3
"""Hash-checked, single-writer deployment for the canonical Forex runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import msvcrt


class DeploymentConflict(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def normalized_relative(value: str | Path) -> Path:
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"deployment path must be relative: {value}")
    return relative


@contextmanager
def deployment_lock(path: Path, timeout_sec: float = 30.0) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        while True:
            try:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise DeploymentConflict(
                        f"deployment lock is held: {path}"
                    )
                time.sleep(0.1)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def file_record(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "sha256": sha256_file(path),
        "size": stat.st_size,
        "mtime_utc": datetime.fromtimestamp(
            stat.st_mtime, timezone.utc
        ).isoformat(),
    }


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DeploymentConflict(f"invalid deployment manifest: {path}")
    return payload


def initialize_manifest(
    target_root: Path,
    manifest_path: Path,
    files: list[str | Path],
    *,
    owner: str,
    force: bool = False,
) -> dict[str, Any]:
    target_root = Path(target_root).resolve()
    lock_path = manifest_path.with_suffix(manifest_path.suffix + ".lock")
    with deployment_lock(lock_path):
        if manifest_path.exists() and not force:
            raise DeploymentConflict(
                "manifest already exists; use force only for deliberate re-baselining"
            )
        records = {}
        for value in files:
            relative = normalized_relative(value)
            target = target_root / relative
            if not target.is_file():
                raise FileNotFoundError(target)
            records[relative.as_posix()] = file_record(target)
        payload = {
            "schema_version": 1,
            "target_root": str(target_root),
            "generated_utc": utc_now(),
            "last_owner": owner,
            "generation": 1,
            "files": records,
            "history": [
                {
                    "time": utc_now(),
                    "owner": owner,
                    "action": "initialize",
                    "file_count": len(records),
                }
            ],
        }
        atomic_json(manifest_path, payload)
        return payload


def deploy_files(
    source_root: Path,
    target_root: Path,
    manifest_path: Path,
    files: list[str | Path],
    *,
    owner: str,
    allow_untracked: bool = False,
    lock_timeout_sec: float = 30.0,
) -> dict[str, Any]:
    source_root = Path(source_root).resolve()
    target_root = Path(target_root).resolve()
    lock_path = manifest_path.with_suffix(manifest_path.suffix + ".lock")
    with deployment_lock(lock_path, lock_timeout_sec):
        manifest = load_manifest(manifest_path)
        if not manifest:
            raise DeploymentConflict("deployment manifest is missing")
        if Path(str(manifest.get("target_root") or "")).resolve() != target_root:
            raise DeploymentConflict("manifest target root does not match deployment target")
        tracked = manifest.setdefault("files", {})
        prepared = []
        for value in files:
            relative = normalized_relative(value)
            key = relative.as_posix()
            source = source_root / relative
            target = target_root / relative
            if not source.is_file():
                raise FileNotFoundError(source)
            expected = tracked.get(key)
            current_hash = sha256_file(target) if target.is_file() else None
            if expected is None and target.exists() and not allow_untracked:
                raise DeploymentConflict(
                    f"untracked target exists; initialize or allow explicitly: {key}"
                )
            if expected is not None and current_hash != expected.get("sha256"):
                raise DeploymentConflict(
                    f"canonical file changed since manifest generation: {key}"
                )
            prepared.append((key, source, target))
        changed = []
        for key, source, target in prepared:
            source_hash = sha256_file(source)
            target_hash = sha256_file(target) if target.is_file() else None
            if source_hash != target_hash:
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(f".{target.name}.{os.getpid()}.deploy")
                shutil.copy2(source, temporary)
                os.replace(temporary, target)
                changed.append(key)
            tracked[key] = file_record(target)
        manifest["generated_utc"] = utc_now()
        manifest["last_owner"] = owner
        manifest["generation"] = int(manifest.get("generation") or 0) + 1
        history = list(manifest.get("history") or [])
        history.append(
            {
                "time": utc_now(),
                "owner": owner,
                "action": "deploy",
                "requested_files": [key for key, _, _ in prepared],
                "changed_files": changed,
            }
        )
        manifest["history"] = history[-100:]
        atomic_json(manifest_path, manifest)
        return {
            "generation": manifest["generation"],
            "owner": owner,
            "changed_files": changed,
            "requested_files": [key for key, _, _ in prepared],
        }


def verify_manifest(
    target_root: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    target_root = Path(target_root).resolve()
    manifest = load_manifest(manifest_path)
    drift = []
    missing = []
    for key, expected in (manifest.get("files") or {}).items():
        target = target_root / normalized_relative(key)
        if not target.is_file():
            missing.append(key)
        elif sha256_file(target) != expected.get("sha256"):
            drift.append(key)
    return {
        "ok": not drift and not missing,
        "generation": int(manifest.get("generation") or 0),
        "last_owner": str(manifest.get("last_owner") or ""),
        "drift": drift,
        "missing": missing,
        "tracked_files": len(manifest.get("files") or {}),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "deploy"):
        child = subparsers.add_parser(name)
        child.add_argument("--source-root", type=Path)
        child.add_argument("--target-root", type=Path, required=True)
        child.add_argument("--manifest", type=Path, required=True)
        child.add_argument("--file", action="append", required=True)
        child.add_argument("--owner", required=True)
    subparsers.choices["init"].add_argument("--force", action="store_true")
    subparsers.choices["deploy"].add_argument(
        "--allow-untracked", action="store_true"
    )
    verify = subparsers.add_parser("verify")
    verify.add_argument("--target-root", type=Path, required=True)
    verify.add_argument("--manifest", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "init":
        payload = initialize_manifest(
            args.target_root,
            args.manifest,
            args.file,
            owner=args.owner,
            force=args.force,
        )
    elif args.command == "deploy":
        if args.source_root is None:
            raise SystemExit("--source-root is required for deploy")
        payload = deploy_files(
            args.source_root,
            args.target_root,
            args.manifest,
            args.file,
            owner=args.owner,
            allow_untracked=args.allow_untracked,
        )
    else:
        payload = verify_manifest(args.target_root, args.manifest)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("ok", True) else 2


if __name__ == "__main__":
    raise SystemExit(main())
