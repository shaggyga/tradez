#!/usr/bin/env python3
"""Move superseded Forex checkpoints out of the synchronized minimal vault.

The operation is recoverable: every file is hashed before and after a same-host
move, and an atomic manifest records original and quarantine paths.  The current
checkpoint and its manifest are always retained.  Nothing is deleted.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any


DEFAULT_SOURCE = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex")
DEFAULT_QUARANTINE = Path(r"C:\Users\zmoor\Documents\forex\vault_quarantine_20260809\forex_model_checkpoints")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def candidates(source: Path) -> list[Path]:
    result = [
        path for path in source.glob("forex_model_checkpoint_*.zip")
        if path.name != "forex_model_checkpoint_current.zip"
    ]
    temporary = source / "forex_model_checkpoint_yv2_jgly.tmp"
    if temporary.is_file():
        result.append(temporary)
    return sorted(result, key=lambda path: (path.stat().st_mtime_ns, path.name))


def run(source: Path, quarantine: Path, apply: bool = False) -> dict[str, Any]:
    source = source.resolve()
    quarantine = quarantine.resolve()
    if not source.is_dir():
        raise FileNotFoundError(source)
    current = source / "forex_model_checkpoint_current.zip"
    current_manifest = source / "forex_model_checkpoint_current.manifest.json"
    if not current.is_file() or not current_manifest.is_file():
        raise RuntimeError("Current checkpoint and manifest must remain present")
    selected = candidates(source)
    rows = []
    for path in selected:
        if not within(path, source):
            raise RuntimeError(f"Source escaped vault project: {path}")
        target = quarantine / path.name
        if not within(target, quarantine):
            raise RuntimeError(f"Target escaped quarantine: {target}")
        rows.append({
            "name": path.name,
            "original_path": str(path),
            "quarantine_path": str(target),
            "size_bytes": path.stat().st_size,
            "sha256": digest(path),
            "status": "planned",
        })
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest = {
        "schema_version": 1,
        "generated_utc": generated,
        "operation": "recoverable_move_no_delete",
        "source_root": str(source),
        "quarantine_root": str(quarantine),
        "retained_current": {
            "path": str(current), "size_bytes": current.stat().st_size, "sha256": digest(current),
            "manifest_path": str(current_manifest),
        },
        "files": rows,
        "planned_bytes": sum(row["size_bytes"] for row in rows),
        "applied": False,
    }
    if not apply:
        return manifest
    quarantine.mkdir(parents=True, exist_ok=True)
    for row in rows:
        original = Path(row["original_path"])
        target = Path(row["quarantine_path"])
        if target.exists():
            if target.stat().st_size != row["size_bytes"] or digest(target) != row["sha256"]:
                raise RuntimeError(f"Conflicting quarantine target: {target}")
            if original.exists():
                raise RuntimeError(f"Both source and target exist: {original}")
            row["status"] = "already_moved_verified"
            continue
        shutil.move(str(original), str(target))
        if target.stat().st_size != row["size_bytes"] or digest(target) != row["sha256"]:
            raise RuntimeError(f"Post-move verification failed: {target}")
        row["status"] = "moved_verified"
    manifest["applied"] = True
    manifest["verified_bytes"] = sum(row["size_bytes"] for row in rows)
    manifest["completed_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    atomic_json(quarantine / "VAULT_CHECKPOINT_RETENTION_MANIFEST_20260809.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--quarantine", type=Path, default=DEFAULT_QUARANTINE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args.source, args.quarantine, args.apply)
    if args.output:
        atomic_json(args.output, result)
    print(json.dumps({
        "applied": result["applied"], "files": len(result["files"]),
        "bytes": result.get("verified_bytes", result["planned_bytes"]),
        "quarantine": result["quarantine_root"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
