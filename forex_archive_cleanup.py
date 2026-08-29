#!/usr/bin/env python3
"""Archive stale runtime clutter without touching active trading state.

Default mode is a dry run.  Use `--execute` to move files into:

    data/archive/runtime_cleanup/YYYYMMDD_HHMMSS/

The archive preserves each file's original relative path, and writes a manifest
so anything can be restored manually if needed.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data"
ARCHIVE_ROOT = DATA_ROOT / "archive" / "runtime_cleanup"


ARCHIVE_SUFFIXES = {
    ".log",
    ".bak",
    ".tmp",
}

ARCHIVE_NAME_FRAGMENTS = {
    "legacy_schema",
    "schema_mismatch",
    "before_auto_promote",
}

PROTECTED_DIR_FRAGMENTS = {
    "archive",
    "candles",
    "candles_bam",
    "m1_raw",
    "models",
    "training_sets",
    "model_lifecycle",
    "promotions",
    "prospective_depth",
    "continuous_research/experiments",
}

PROTECTED_FILENAMES = {
    "state.json",
    "monitor.csv",
    "technical_production.json",
    "research_leader.json",
    "shadow_candidate.json",
    "canary_candidate.json",
    "canary_ready.json",
    "collector_state.json",
    "research_state.json",
}


@dataclass(frozen=True)
class Candidate:
    source: Path
    relative: Path
    size: int
    modified_utc: str
    reason: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def relative_posix(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def protected(path: Path) -> bool:
    if path.name in PROTECTED_FILENAMES:
        return True
    rel = path.relative_to(DATA_ROOT).as_posix().lower()
    return any(fragment.lower() in rel for fragment in PROTECTED_DIR_FRAGMENTS)


def archive_reason(path: Path) -> str:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if suffix in ARCHIVE_SUFFIXES:
        return f"suffix:{suffix}"
    for fragment in ARCHIVE_NAME_FRAGMENTS:
        if fragment.lower() in name:
            return f"name_fragment:{fragment}"
    return ""


def iter_candidates(
    roots: Iterable[Path],
    *,
    older_than_hours: float,
) -> list[Candidate]:
    cutoff = utc_now() - timedelta(hours=older_than_hours)
    candidates: list[Candidate] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if not is_under(path, DATA_ROOT):
                continue
            if protected(path):
                continue
            reason = archive_reason(path)
            if not reason:
                continue
            modified = as_utc(path.stat().st_mtime)
            if modified >= cutoff:
                continue
            candidates.append(
                Candidate(
                    source=path,
                    relative=path.relative_to(ROOT),
                    size=path.stat().st_size,
                    modified_utc=modified.isoformat(),
                    reason=reason,
                )
            )
    candidates.sort(key=lambda item: (item.modified_utc, str(item.relative)))
    return candidates


def write_manifest(
    archive_dir: Path,
    candidates: list[Candidate],
    skipped: list[dict[str, Any]] | None = None,
) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "source": relative_posix(candidate.source),
            "archived_to": (archive_dir / candidate.relative).relative_to(ROOT).as_posix(),
            "size": candidate.size,
            "modified_utc": candidate.modified_utc,
            "reason": candidate.reason,
        }
        for candidate in candidates
    ]
    (archive_dir / "manifest.json").write_text(
        json.dumps(
            {
                "generated_utc": utc_now().isoformat(),
                "count": len(rows),
                "total_bytes": sum(candidate.size for candidate in candidates),
                "skipped_count": len(skipped or []),
                "skipped": skipped or [],
                "files": rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    with (archive_dir / "manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["source", "archived_to", "size", "modified_utc", "reason"],
        )
        writer.writeheader()
        writer.writerows(rows)


def execute_archive(candidates: list[Candidate]) -> tuple[Path, int, int]:
    archive_dir = ARCHIVE_ROOT / utc_now().strftime("%Y%m%d_%H%M%S")
    moved: list[Candidate] = []
    skipped: list[dict[str, Any]] = []
    for candidate in candidates:
        destination = archive_dir / candidate.relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(candidate.source), str(destination))
            moved.append(candidate)
        except OSError as exc:
            skipped.append({
                "source": relative_posix(candidate.source),
                "size": candidate.size,
                "modified_utc": candidate.modified_utc,
                "reason": candidate.reason,
                "error": f"{type(exc).__name__}: {exc}",
            })
    write_manifest(archive_dir, moved, skipped)
    return archive_dir, len(moved), len(skipped)


def print_summary(candidates: list[Candidate], *, limit: int = 40) -> None:
    total = sum(candidate.size for candidate in candidates)
    print(f"candidate_count={len(candidates)} total_bytes={total}")
    for candidate in candidates[:limit]:
        print(
            f"{relative_posix(candidate.source)} "
            f"bytes={candidate.size} modified={candidate.modified_utc} "
            f"reason={candidate.reason}"
        )
    if len(candidates) > limit:
        print(f"... {len(candidates) - limit} more")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--older-than-hours",
        type=float,
        default=48.0,
        help="Only archive files older than this many hours.",
    )
    parser.add_argument(
        "--root",
        action="append",
        default=[],
        help="Relative root under the workspace to scan. Can be repeated. Default: data",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Move matching files into the archive. Without this, dry-run only.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary.")
    args = parser.parse_args()

    roots = [ROOT / item for item in args.root] if args.root else [DATA_ROOT]
    candidates = iter_candidates(roots, older_than_hours=args.older_than_hours)
    if args.json:
        print(
            json.dumps(
                {
                    "execute": bool(args.execute),
                    "older_than_hours": args.older_than_hours,
                    "count": len(candidates),
                    "total_bytes": sum(candidate.size for candidate in candidates),
                    "files": [
                        {
                            "source": relative_posix(candidate.source),
                            "size": candidate.size,
                            "modified_utc": candidate.modified_utc,
                            "reason": candidate.reason,
                        }
                        for candidate in candidates
                    ],
                },
                indent=2,
            )
        )
    else:
        print_summary(candidates)

    if args.execute and candidates:
        archive_dir, moved_count, skipped_count = execute_archive(candidates)
        print(f"archived_to={archive_dir.relative_to(ROOT).as_posix()}")
        print(f"moved={moved_count} skipped={skipped_count}")
    elif not args.execute:
        print("dry_run=true; pass --execute to archive these files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
