#!/usr/bin/env python3
"""Build a content-hashed index of retained FX move/news case evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parent
REPORT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_JSON = REPORT_ROOT / "HISTORICAL_CASE_INDEX_CURRENT.json"
DEFAULT_MARKDOWN = REPORT_ROOT / "HISTORICAL_CASE_INDEX_CURRENT.md"

CASE_ROOTS = (
    "major_move_case_audits",
    "live_case_audits",
    "move_first_news_case_audit",
    "week_to_date_event_move_audit",
    "direct_source_response",
    "spike_blurb_factor_reconstruction",
)
ALLOWED_SUFFIXES = {".md", ".json", ".jsonl", ".csv", ".sha256"}


def utc_iso(epoch: float | None = None) -> str:
    return datetime.fromtimestamp(
        time.time() if epoch is None else epoch,
        tz=timezone.utc,
    ).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def candidate_files(report_root: Path) -> Iterable[tuple[str, Path]]:
    for category in CASE_ROOTS:
        directory = report_root / category
        if not directory.is_dir():
            continue
        for path in directory.rglob("*"):
            if path.is_file() and path.suffix.lower() in ALLOWED_SUFFIXES:
                yield category, path
    for pattern in ("HISTORICAL_MISS*", "WEEK_TO_DATE_EVENT_MOVE_AUDIT*"):
        for path in report_root.glob(pattern):
            if path.is_file() and path.suffix.lower() in ALLOWED_SUFFIXES:
                yield "root_case_audits", path


def build_index(report_root: Path = REPORT_ROOT) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for category, path in candidate_files(report_root):
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        if relative in seen or path.name.startswith("HISTORICAL_CASE_INDEX_CURRENT"):
            continue
        seen.add(relative)
        stat = path.stat()
        rows.append(
            {
                "category": category,
                "path": relative,
                "bytes": int(stat.st_size),
                "modified_utc": utc_iso(stat.st_mtime),
                "sha256": sha256_file(path),
            }
        )
    rows.sort(key=lambda row: str(row["path"]))
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    categories: dict[str, int] = {}
    for row in rows:
        category = str(row["category"])
        categories[category] = categories.get(category, 0) + 1
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "project_root": str(PROJECT_ROOT),
        "contract": {
            "append_only_evidence_preserved": True,
            "index_does_not_reclassify_or_merge_cases": True,
            "sha256_covers_exact_retained_file_bytes": True,
        },
        "file_count": len(rows),
        "total_bytes": sum(int(row["bytes"]) for row in rows),
        "category_counts": categories,
        "entries_sha256": hashlib.sha256(canonical).hexdigest(),
        "entries": rows,
    }


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def render_markdown(payload: dict[str, object]) -> str:
    categories = payload.get("category_counts") or {}
    lines = [
        "# Historical case evidence index",
        "",
        f"Generated UTC: `{payload['generated_utc']}`",
        "",
        "This is a content-hashed inventory, not a combined performance result. "
        "Original case files remain authoritative and immutable.",
        "",
        f"- Files: **{payload['file_count']}**",
        f"- Bytes: **{payload['total_bytes']}**",
        f"- Entry-list SHA-256: `{payload['entries_sha256']}`",
        "",
        "## Coverage",
        "",
    ]
    for category, count in sorted(dict(categories).items()):
        lines.append(f"- `{category}`: {count}")
    lines.extend(
        [
            "",
            "The machine-readable file lists every retained path, byte size, "
            "mtime, category, and exact SHA-256.",
            "",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--json-output", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args(argv)
    payload = build_index(args.report_root)
    atomic_write(
        args.json_output,
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
    )
    atomic_write(args.markdown_output, render_markdown(payload))
    print(
        json.dumps(
            {
                "json": str(args.json_output),
                "markdown": str(args.markdown_output),
                "file_count": payload["file_count"],
                "entries_sha256": payload["entries_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
