#!/usr/bin/env python3
"""Run the sealed independent all-68 replay verifier for an explicit config.

The independently bound verifier predates multi-pack replay and intentionally
keeps its default constants immutable.  This launcher changes only those path
constants; it does not import the replay producer or either replay core.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import oanda_sequential_all68_portfolio_batch_replay_verifier as verifier


ROOT = Path(__file__).resolve().parent


def _is_link_or_reparse(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0))
    except OSError:
        return True
    return path.is_symlink() or os.path.islink(path) or bool(attributes & 0x400)


def _contained_existing_file(path: Path) -> Path:
    candidate = path if path.is_absolute() else ROOT / path
    unresolved = candidate.absolute()
    try:
        unresolved.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError("config path escapes project root") from exc
    cursor = unresolved
    while cursor != ROOT:
        if _is_link_or_reparse(cursor):
            raise ValueError("linked or reparse-point config path rejected")
        cursor = cursor.parent
    resolved = unresolved.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError("config is not a regular file")
    return resolved


def configure(config_path: Path) -> dict[str, Any]:
    config_path = _contained_existing_file(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")
    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise ValueError("config storage contract missing")
    relative_root = Path(str(storage.get("artifact_relative_root") or ""))
    if relative_root.is_absolute() or ".." in relative_root.parts:
        raise ValueError("artifact root escapes project root")
    artifact_root = (ROOT / relative_root).resolve()
    if ROOT not in artifact_root.parents:
        raise ValueError("artifact root escapes project root")
    for name in ("state_name", "verifier_name"):
        value = Path(str(storage.get(name) or ""))
        if value.is_absolute() or len(value.parts) != 1 or value.name in ("", ".", ".."):
            raise ValueError(f"unsafe {name}")
    verifier.CONFIG = config_path
    verifier.ARTIFACT_ROOT = artifact_root
    verifier.STATE = artifact_root / str(storage["state_name"])
    verifier.OUTPUT = artifact_root / str(storage["verifier_name"])
    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    configure(args.config)
    result = verifier.verify()
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("verified") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
