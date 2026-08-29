#!/usr/bin/env python3
"""Safe maintenance helper for the OANDA training artifact folder.

The training folder intentionally contains large datasets, trained models, and
promotion manifests.  "Cleaning" it should therefore mean moving stale,
duplicative, or smoke-test artifacts out of the active view while preserving a
manifest and a reversible archive.  This script never touches live account
state, promotion manifests, candles, model artifacts, or training datasets.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
ARCHIVE_ROOT = TRAINING_ROOT / "_archive"
ZERO_BYTE_LOG_MIN_AGE_SECONDS = 2 * 60 * 60
TIMESTAMPED_TRAINER_LOG_RE = re.compile(
    r"^(?:research_)?trainer[_a-z0-9]*_\d{8}_\d{6}\.(?:out|err)\.log$",
    re.IGNORECASE,
)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


def dir_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += file_size(child)
    return total


def safe_move(path: Path, destination_root: Path, *, dry_run: bool) -> Dict[str, Any]:
    source = path.resolve()
    training_root = TRAINING_ROOT.resolve()
    destination_root = destination_root.resolve()
    if not is_relative_to(source, training_root):
        raise ValueError(f"refusing to move path outside training root: {source}")
    if not is_relative_to(destination_root, ARCHIVE_ROOT):
        raise ValueError(f"refusing to archive outside archive root: {destination_root}")
    relative = source.relative_to(training_root)
    destination = destination_root / relative
    record = {
        "source": str(source),
        "destination": str(destination),
        "kind": "directory" if path.is_dir() else "file",
        "bytes": dir_size(path) if path.is_dir() else file_size(path),
    }
    if not dry_run:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            destination = destination.with_name(
                f"{destination.name}.duplicate_{utc_stamp()}"
            )
            record["destination"] = str(destination)
        shutil.move(str(source), str(destination))
    return record


def collect_stale_report_artifacts() -> List[Path]:
    reports = TRAINING_ROOT / "reports"
    if not reports.exists():
        return []
    paths: List[Path] = []

    # Temporary and smoke-test files are useful while developing, noisy later.
    for pattern in ["tmp_*", "*smoke*"]:
        paths.extend(path for path in reports.glob(pattern) if path.is_file())

    # Timestamped model-specialization bundle directories are archived once
    # latest_* convenience files exist. Keep the newest directory visible.
    review_dirs = sorted(
        [
            path
            for path in reports.glob("model_specialization_review_*")
            if path.is_dir()
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    paths.extend(review_dirs[1:])

    # Zips are delivery bundles, not active inputs. They stay recoverable in
    # archive; latest_* report files remain in the main reports directory.
    paths.extend(path for path in reports.glob("model_specialization_review_*.zip") if path.is_file())

    # Timestamped seed snapshots are useful for audit trails, but the current
    # active view only needs latest_* plus a short recent history. Archive older
    # snapshots reversibly to keep the reports directory readable.
    seed_snapshots = sorted(
        [
            path
            for path in reports.glob("weekend_account_improvement_seed_*.json")
            if path.is_file()
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    paths.extend(seed_snapshots[8:])
    return sorted(set(paths), key=lambda path: str(path).lower())


def collect_stale_research_backups() -> List[Path]:
    research = TRAINING_ROOT / "continuous_research"
    if not research.exists():
        return []
    paths: List[Path] = []
    for pattern in [
        "*.bak",
        "*.bak_*",
        "*.bad_*",
        "*.backup_*",
        "*.schema_tmp_*",
    ]:
        paths.extend(path for path in research.glob(pattern) if path.is_file())
    return sorted(set(paths), key=lambda path: str(path).lower())


def collect_stale_zero_byte_launch_logs() -> List[Path]:
    """Archive inert trainer launcher logs without moving active log handles.

    The active research manager uses stable filenames such as
    research_trainer_stdout.log.  Those are intentionally ignored even when
    zero bytes because a running process may still have the handle open.  This
    collector only targets timestamped launch logs that are empty and old enough
    to be clearly inert.
    """
    logs = TRAINING_ROOT / "logs"
    if not logs.exists():
        return []
    now = datetime.now(timezone.utc).timestamp()
    paths: List[Path] = []
    for path in logs.glob("*.log"):
        if not path.is_file():
            continue
        if file_size(path) != 0:
            continue
        if not TIMESTAMPED_TRAINER_LOG_RE.match(path.name):
            continue
        age_seconds = now - path.stat().st_mtime
        if age_seconds >= ZERO_BYTE_LOG_MIN_AGE_SECONDS:
            paths.append(path)
    return sorted(set(paths), key=lambda path: str(path).lower())


def collect_stale_dry_run_manifests() -> List[Path]:
    """Keep only the newest training-folder cleanup dry-run manifests visible."""
    if not ARCHIVE_ROOT.exists():
        return []
    manifests = sorted(
        ARCHIVE_ROOT.glob("dry_run_cleanup_manifest_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return manifests[1:]


def collect_stale_artifacts() -> List[Path]:
    paths: List[Path] = []
    paths.extend(collect_stale_research_backups())
    paths.extend(collect_stale_report_artifacts())
    paths.extend(collect_stale_zero_byte_launch_logs())
    paths.extend(collect_stale_dry_run_manifests())
    return sorted(set(paths), key=lambda path: str(path).lower())


def folder_summary() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not TRAINING_ROOT.exists():
        return rows
    for child in sorted(TRAINING_ROOT.iterdir(), key=lambda path: path.name.lower()):
        if child.is_dir():
            files = sum(1 for item in child.rglob("*") if item.is_file())
            size = dir_size(child)
        else:
            files = 1
            size = file_size(child)
        rows.append({
            "name": child.name,
            "kind": "directory" if child.is_dir() else "file",
            "files": files,
            "bytes": size,
            "size_mb": round(size / (1024 * 1024), 3),
        })
    return rows


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def run_cleanup(*, dry_run: bool = False) -> Dict[str, Any]:
    TRAINING_ROOT.mkdir(parents=True, exist_ok=True)
    archive_dir = ARCHIVE_ROOT / f"cleanup_{utc_stamp()}"
    before = folder_summary()
    stale_paths = collect_stale_artifacts()
    moved = [
        safe_move(path, archive_dir, dry_run=dry_run)
        for path in stale_paths
        if path.exists()
    ]
    after = folder_summary() if not dry_run else before
    payload = {
        "generated_utc": utc_iso(),
        "dry_run": dry_run,
        "training_root": str(TRAINING_ROOT),
        "archive_dir": str(archive_dir),
        "policy": {
            "never_touch": [
                "candles",
                "training_sets",
                "models",
                "promotions",
                "live account state/logs outside the training folder",
            ],
            "archived": [
                "continuous_research backup/bad-schema files",
                "continuous_research schema_tmp files",
                "reports/tmp_* files",
                "reports/*smoke* files",
                "older timestamped weekend_account_improvement_seed snapshots",
                "older timestamped model-specialization review dirs",
                "timestamped model-specialization zip delivery bundles",
                "old zero-byte timestamped trainer launch logs",
                "old dry-run cleanup manifests",
            ],
        },
        "moved_count": len(moved),
        "moved_bytes": sum(int(item.get("bytes") or 0) for item in moved),
        "moved_size_mb": round(sum(int(item.get("bytes") or 0) for item in moved) / (1024 * 1024), 3),
        "moved": moved,
        "folder_summary_before": before,
        "folder_summary_after": after,
    }
    manifest_path = (
        archive_dir / "cleanup_manifest.json"
        if not dry_run
        else ARCHIVE_ROOT / f"dry_run_cleanup_manifest_{utc_stamp()}.json"
    )
    write_json(manifest_path, payload)
    payload["manifest_path"] = str(manifest_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean stale OANDA training artifacts safely")
    parser.add_argument("--dry-run", action="store_true", help="Report what would move without moving files")
    args = parser.parse_args()
    result = run_cleanup(dry_run=bool(args.dry_run))
    print(json.dumps({
        "dry_run": result["dry_run"],
        "moved_count": result["moved_count"],
        "moved_size_mb": result["moved_size_mb"],
        "archive_dir": result["archive_dir"],
        "manifest_path": result["manifest_path"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
