#!/usr/bin/env python3
"""Download one explicitly selected foundation-model snapshot and record its identity."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, snapshot_download


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def inventory(root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
        }
        for path in sorted(root.rglob("*"))
        if path.is_file() and ".cache" not in path.parts
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--repo-id", required=True)
    parser.add_argument("--revision", default="main")
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = utc_iso()
    info = HfApi().model_info(args.repo_id, revision=args.revision)
    resolved_revision = str(info.sha)
    args.destination.mkdir(parents=True, exist_ok=True)
    snapshot_download(
        repo_id=args.repo_id,
        revision=resolved_revision,
        local_dir=args.destination,
    )
    files = inventory(args.destination)
    report = {
        "schema_version": 1,
        "model": args.model,
        "repo_id": args.repo_id,
        "requested_revision": args.revision,
        "resolved_revision": resolved_revision,
        "destination": str(args.destination.resolve()),
        "started_utc": started,
        "finished_utc": utc_iso(),
        "status": "snapshot_available",
        "files": files,
        "file_count": len(files),
        "total_bytes": sum(row["bytes"] for row in files),
        "execution_policy": "frozen_shadow_only",
        "account_wired": False,
    }
    write_json_atomic(args.output, report)
    print(json.dumps({key: report[key] for key in ("model", "resolved_revision", "file_count", "total_bytes")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
