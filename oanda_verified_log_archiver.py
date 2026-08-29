#!/usr/bin/env python3
"""Compress inactive rotated JSONL logs after byte-for-byte verification."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
LOG_ROOT = ROOT / "data" / "oanda_training_manager" / "logs"
DEFAULT_STATE = ROOT / "data" / "oanda_training_manager" / "state" / "verified_log_archiver_v1.json"
CHUNK = 1024 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def contained(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (OSError, ValueError):
        return False


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def archive_one(path: Path, root: Path) -> dict[str, Any]:
    path = path.resolve()
    root = root.resolve()
    if not contained(path, root) or path.suffix.lower() != ".jsonl" or ".part_" not in path.name:
        raise ValueError(f"refusing non-rotated or out-of-root log: {path}")
    original_bytes = path.stat().st_size
    original_hash = hashlib.sha256()
    line_count = 0
    final = path.with_suffix(path.suffix + ".gz")
    temporary = final.with_suffix(final.suffix + f".{os.getpid()}.tmp")
    manifest_path = final.with_suffix(final.suffix + ".manifest.json")
    try:
        with path.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as target:
            for chunk in iter(lambda: source.read(CHUNK), b""):
                original_hash.update(chunk)
                line_count += chunk.count(b"\n")
                target.write(chunk)
        roundtrip = hashlib.sha256()
        roundtrip_bytes = 0
        with gzip.open(temporary, "rb") as restored:
            for chunk in iter(lambda: restored.read(CHUNK), b""):
                roundtrip.update(chunk)
                roundtrip_bytes += len(chunk)
        original_digest = "sha256:" + original_hash.hexdigest()
        if roundtrip_bytes != original_bytes or "sha256:" + roundtrip.hexdigest() != original_digest:
            raise ValueError("gzip round-trip verification failed")
        compressed_hash = sha256_file(temporary)
        temporary.replace(final)
        manifest = {
            "schema_version": 1,
            "created_utc": utc_now(),
            "original_path": str(path),
            "archive_path": str(final),
            "original_bytes": original_bytes,
            "compressed_bytes": final.stat().st_size,
            "original_sha256": original_digest,
            "compressed_sha256": compressed_hash,
            "line_count": line_count,
            "round_trip_verified": True,
            "recoverable_with": "gzip",
        }
        manifest_tmp = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        manifest_tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        manifest_tmp.replace(manifest_path)
        path.unlink()
        manifest["reclaimed_bytes"] = original_bytes - final.stat().st_size - manifest_path.stat().st_size
        return manifest
    finally:
        temporary.unlink(missing_ok=True)


def candidates(root: Path, *, minimum_age_hours: float) -> list[Path]:
    cutoff = time.time() - max(1.0, minimum_age_hours) * 3600.0
    rows: list[Path] = []
    for path in root.rglob("*.jsonl"):
        try:
            if ".part_" in path.name and path.stat().st_mtime <= cutoff:
                rows.append(path)
        except OSError:
            continue
    return sorted(rows, key=lambda item: (item.stat().st_mtime, str(item)))


def manifest_lifetime(root: Path) -> dict[str, int]:
    rows: dict[str, tuple[dict[str, Any], int]] = {}
    for path in root.rglob("*.jsonl.gz.manifest.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            continue
        if payload.get("round_trip_verified") is not True:
            continue
        key = str(payload.get("archive_path") or path)
        rows[key] = (payload, path.stat().st_size)
    return {
        "archived_file_count": len(rows),
        "source_bytes": sum(int(row.get("original_bytes") or 0) for row, _ in rows.values()),
        "compressed_bytes": sum(int(row.get("compressed_bytes") or 0) for row, _ in rows.values()),
        "manifest_bytes": sum(manifest_bytes for _, manifest_bytes in rows.values()),
        "reclaimed_bytes": sum(
            max(
                0,
                int(row.get("original_bytes") or 0)
                - int(row.get("compressed_bytes") or 0)
                - manifest_bytes,
            )
            for row, manifest_bytes in rows.values()
        ),
    }


def run(
    *, root: Path = LOG_ROOT, state: Path = DEFAULT_STATE,
    minimum_age_hours: float = 48.0, maximum_source_bytes: int = 2 * 1024**3,
) -> dict[str, Any]:
    root = root.resolve()
    archived: list[dict[str, Any]] = []
    source_bytes = 0
    for path in candidates(root, minimum_age_hours=minimum_age_hours):
        size = path.stat().st_size
        if archived and source_bytes + size > maximum_source_bytes:
            break
        archived.append(archive_one(path, root))
        source_bytes += size
    payload = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "status": "ok",
        "can_place_orders": False,
        "can_promote": False,
        "root": str(root),
        "minimum_age_hours": minimum_age_hours,
        "maximum_source_bytes": maximum_source_bytes,
        "archived_file_count": len(archived),
        "source_bytes": source_bytes,
        "compressed_bytes": sum(int(row["compressed_bytes"]) for row in archived),
        "reclaimed_bytes": sum(int(row["reclaimed_bytes"]) for row in archived),
        "archives": archived,
        "lifetime": manifest_lifetime(root),
    }
    state.parent.mkdir(parents=True, exist_ok=True)
    temporary = state.with_suffix(state.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(state)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=LOG_ROOT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--minimum-age-hours", type=float, default=48.0)
    parser.add_argument("--maximum-source-gib", type=float, default=2.0)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        run(
            root=args.root,
            state=args.state,
            minimum_age_hours=args.minimum_age_hours,
            maximum_source_bytes=max(1, int(args.maximum_source_gib * 1024**3)),
        )
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(300.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
