"""Compact integrity publications with immutable, verifiable episode detail.

Only publication copies omit rows. Audit inputs and verdict computation remain
unchanged. Reference paths are relative to the integrity snapshot directory;
JSONL and embedded-copy readers must supply that same trusted directory.
Artifacts are never automatically deleted: historical publications refer to
them even after the current snapshot changes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any


PUBLICATION_CONTRACT = "project_integrity_compact_publication_v1_20260906"
DETAIL_CONTRACT = "project_integrity_episode_rows_v1_20260906"
DETAIL_DIRECTORY = "project_integrity_details_v1"
DETAIL_SECTIONS = frozenset({
    "move_first_live_arm_alignment",
    "move_first_operational_mapping_alignment",
    "move_first_operational_mapping_alignment_v2",
    "move_first_operational_mapping_alignment_v3",
    "move_first_operational_mapping_alignment_v4",
})
_REFERENCE = "_detail_reference"
_PUBLICATION = "_detail_publication"


def _relative_artifact_path(digest: str) -> str:
    return f"{DETAIL_DIRECTORY}/sha256/{digest[:2]}/{digest}.json"


def _artifact_path(snapshot_dir: Path, digest: str) -> Path:
    base = Path(snapshot_dir).resolve()
    path = base / _relative_artifact_path(digest)
    if not path.resolve().is_relative_to(base):
        raise ValueError("integrity_detail_path_escapes_snapshot_directory")
    return path


def _verify_bytes(path: Path, digest: str, size: int) -> None:
    actual = hashlib.sha256()
    count = 0
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            count += len(block)
            actual.update(block)
    if count != size or actual.hexdigest() != digest:
        raise ValueError("integrity_detail_artifact_hash_or_size_mismatch")


def _store_artifact(snapshot_dir: Path, payload: bytes) -> tuple[Path, str]:
    """Publish a complete file without replacing an existing immutable name."""
    digest = hashlib.sha256(payload).hexdigest()
    destination = _artifact_path(snapshot_dir, digest)
    if destination.exists():
        _verify_bytes(destination, digest, len(payload))
        return destination, digest
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Recheck containment after directory creation, including symlink parents.
    destination = _artifact_path(snapshot_dir, digest)
    temporary = destination.with_name(f".{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # A hard link publishes the already-complete inode atomically and
            # fails if the name exists. replace()/POSIX rename() would overwrite
            # evidence if another publisher won this race.
            os.link(temporary, destination)
        except FileExistsError:
            pass
        _verify_bytes(destination, digest, len(payload))
    finally:
        temporary.unlink(missing_ok=True)
    return destination, digest


def compact_integrity_payload(
    payload: dict[str, Any], *, snapshot_dir: Path
) -> dict[str, Any]:
    """Copy an audit, preserving every non-row value, after storing its rows.

    Only the five explicit alignment sections are eligible. Missing or malformed
    row values remain inline so an already-degraded audit is still publishable.
    Detail failure raises before the caller writes any snapshot/history pointer.
    The caller retains publication-generation ownership throughout this call.
    """
    if _PUBLICATION in payload:
        raise ValueError("integrity_publication_already_compacted")
    result = dict(payload)
    sections: list[str] = []
    for key in sorted(DETAIL_SECTIONS):
        section = payload.get(key)
        if not isinstance(section, dict):
            continue
        if _REFERENCE in section:
            raise ValueError("integrity_detail_reserved_reference_present")
        rows = section.get("episode_rows")
        if not isinstance(rows, list) or not rows:
            continue
        encoded = json.dumps(
            {"schema_version": DETAIL_CONTRACT, "episode_rows": rows},
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        _, digest = _store_artifact(snapshot_dir, encoded)
        compact_section = dict(section)
        del compact_section["episode_rows"]
        compact_section[_REFERENCE] = {
            "contract_id": DETAIL_CONTRACT,
            "field": "episode_rows",
            "path": _relative_artifact_path(digest),
            "sha256": digest,
            "bytes": len(encoded),
            "item_count": len(rows),
        }
        result[key] = compact_section
        sections.append(key)
    if sections:
        result[_PUBLICATION] = {
            "contract_id": PUBLICATION_CONTRACT,
            "reference_base": "integrity_snapshot_directory",
            "section_keys": sections,
        }
    return result


def _reference_sections(payload: dict[str, Any]) -> list[str]:
    actual = sorted(
        key for key in DETAIL_SECTIONS
        if isinstance(payload.get(key), dict) and _REFERENCE in payload[key]
    )
    publication = payload.get(_PUBLICATION)
    if _PUBLICATION not in payload and not actual:
        return []  # Legacy, full publications remain readable.
    if (
        not isinstance(publication, dict)
        or set(publication) != {"contract_id", "reference_base", "section_keys"}
        or publication.get("contract_id") != PUBLICATION_CONTRACT
        or publication.get("reference_base") != "integrity_snapshot_directory"
        or publication.get("section_keys") != actual
        or not actual
    ):
        raise ValueError("integrity_detail_publication_contract_invalid")
    return actual


def _load_detail(
    section: dict[str, Any], *, snapshot_dir: Path
) -> tuple[Path, bytes, list[Any]]:
    reference = section.get(_REFERENCE)
    if (
        "episode_rows" in section
        or not isinstance(reference, dict)
        or set(reference) != {
            "contract_id", "field", "path", "sha256", "bytes", "item_count"
        }
        or reference.get("contract_id") != DETAIL_CONTRACT
        or reference.get("field") != "episode_rows"
        or not isinstance(reference.get("sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", reference["sha256"]) is None
        or type(reference.get("bytes")) is not int
        or reference["bytes"] <= 0
        or type(reference.get("item_count")) is not int
        or reference["item_count"] <= 0
    ):
        raise ValueError("integrity_detail_reference_invalid")
    digest = reference["sha256"]
    # Exact derivation rejects absolute, drive, UNC, traversal and alias paths.
    if reference.get("path") != _relative_artifact_path(digest):
        raise ValueError("integrity_detail_reference_path_invalid")
    path = _artifact_path(snapshot_dir, digest)
    # Verify the same bytes that are decoded; never re-open after hashing.
    encoded = path.read_bytes()
    if (
        len(encoded) != reference["bytes"]
        or hashlib.sha256(encoded).hexdigest() != digest
    ):
        raise ValueError("integrity_detail_artifact_hash_or_size_mismatch")
    value = json.loads(encoded)
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "episode_rows"}
        or value.get("schema_version") != DETAIL_CONTRACT
        or not isinstance(value.get("episode_rows"), list)
        or len(value["episode_rows"]) != reference["item_count"]
    ):
        raise ValueError("integrity_detail_artifact_schema_or_count_invalid")
    return path, encoded, value["episode_rows"]


def verified_detail_artifact_bytes(
    payload: dict[str, Any], *, snapshot_dir: Path
) -> dict[Path, bytes]:
    """Capture verified artifact bytes for a coherent export without re-reads."""
    artifacts: dict[Path, bytes] = {}
    for key in _reference_sections(payload):
        path, encoded, _ = _load_detail(payload[key], snapshot_dir=snapshot_dir)
        artifacts[path] = encoded
    return artifacts


def verified_detail_artifacts(
    payload: dict[str, Any], *, snapshot_dir: Path
) -> list[Path]:
    """Return verified dependencies for copying/restoring a publication.

    No files are created or repaired. An unavailable or invalid dependency raises;
    exporters must not claim a complete snapshot after catching that failure.
    """
    paths: set[Path] = set()
    for key in _reference_sections(payload):
        path, _, _ = _load_detail(payload[key], snapshot_dir=snapshot_dir)
        paths.add(path)
    return sorted(paths)


def restore_integrity_details(
    payload: dict[str, Any], *, snapshot_dir: Path
) -> dict[str, Any]:
    """Reconstruct the original JSON value without changing either input."""
    result = dict(payload)
    for key in _reference_sections(payload):
        _, _, rows = _load_detail(payload[key], snapshot_dir=snapshot_dir)
        section = dict(payload[key])
        del section[_REFERENCE]
        section["episode_rows"] = rows
        result[key] = section
    result.pop(_PUBLICATION, None)
    return result
