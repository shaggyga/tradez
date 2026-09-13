#!/usr/bin/env python3
"""Move legacy vault bulk intact to a hash-verified local cold archive.

The OneDrive vault is a shared record surface, not a data warehouse.  This
utility is intentionally narrow: it accepts only reviewed legacy top-level
targets, writes a complete pre-move file/hash manifest, moves each target
without deleting it, verifies the destination tree, and leaves recovery
pointers in both locations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOWED_TARGETS = (
    "UNIFIED_QUERY_VAULT",
    "FOREX_MODEL_LIBRARY",
    "BIGTRIAD_MODEL_LIBRARY",
    "fxgap26",
    "GPT_VAULT_COMPLETE.zip",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def scan_target(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise RuntimeError(f"symlink target is forbidden: {path}")
    if path.is_file():
        files = [path]
        root = path.parent
    elif path.is_dir():
        files = sorted(item for item in path.rglob("*") if item.is_file())
        root = path.parent
    else:
        raise FileNotFoundError(path)

    entries: list[dict[str, Any]] = []
    tree_digest = hashlib.sha256()
    total_bytes = 0
    for index, item in enumerate(files, start=1):
        if item.is_symlink():
            raise RuntimeError(f"symlink member is forbidden: {item}")
        relative = item.relative_to(root).as_posix()
        size = item.stat().st_size
        digest = sha256_file(item)
        row = {"path": relative, "bytes": size, "sha256": digest}
        entries.append(row)
        tree_digest.update(
            (json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode(
                "utf-8"
            )
        )
        total_bytes += size
        if index % 500 == 0:
            print(
                f"hashed {index}/{len(files)} files for {path.name}",
                flush=True,
            )
    return {
        "name": path.name,
        "kind": "file" if path.is_file() else "directory",
        "file_count": len(entries),
        "bytes": total_bytes,
        "tree_sha256": tree_digest.hexdigest(),
        "files": entries,
    }


def validate_paths(
    vault: Path,
    cold_root: Path,
    run_id: str,
    target_names: list[str],
) -> tuple[Path, list[Path]]:
    vault = vault.resolve(strict=True)
    cold_root = cold_root.resolve(strict=False)
    if is_within(cold_root, vault) or cold_root == vault:
        raise RuntimeError("cold archive must be outside the OneDrive vault")
    if not run_id or any(character in run_id for character in "\\/:"):
        raise ValueError("run-id must be a single safe directory name")
    if len(set(target_names)) != len(target_names):
        raise ValueError("duplicate targets are forbidden")
    rejected = sorted(set(target_names) - set(ALLOWED_TARGETS))
    if rejected:
        raise ValueError(f"unreviewed target(s): {', '.join(rejected)}")
    sources = []
    for name in target_names:
        source = (vault / name).resolve(strict=True)
        if source.parent != vault:
            raise RuntimeError(f"target is not a direct vault child: {source}")
        sources.append(source)
    destination = (cold_root / run_id).resolve(strict=False)
    if destination.exists():
        raise FileExistsError(destination)
    return destination, sources


def build_pointer(receipt: dict[str, Any]) -> str:
    lines = [
        "# Vault cold-archive pointers",
        "",
        "The OneDrive vault is the small shared record layer. The following",
        "legacy bulk was moved intact—not deleted—to the local cold archive.",
        "",
        f"- Completed: `{receipt['completed_utc']}`",
        f"- Cold archive: `{receipt['cold_archive_run']}`",
        f"- Receipt SHA-256: `{receipt['receipt_sha256']}`",
        f"- Moved bytes: `{receipt['moved_bytes']}`",
        f"- Moved files: `{receipt['moved_files']}`",
        "",
        "## Moved roots",
        "",
    ]
    for target in receipt["targets"]:
        lines.append(
            f"- `{target['name']}` — {target['file_count']} files, "
            f"{target['bytes']} bytes, tree `{target['tree_sha256']}`"
        )
    lines.extend(
        [
            "",
            "Restore by moving an exact named root from the cold archive back",
            "to the vault only after verifying its tree hash against the receipt.",
            "The cold archive is not live Forex truth.",
            "",
        ]
    )
    return "\n".join(lines)


def consolidate(
    vault: Path,
    cold_root: Path,
    run_id: str,
    target_names: list[str],
    apply: bool,
) -> dict[str, Any]:
    destination, sources = validate_paths(vault, cold_root, run_id, target_names)
    vault = vault.resolve(strict=True)
    print(f"validated vault={vault}", flush=True)
    print(f"validated cold_archive={destination}", flush=True)

    targets = []
    for source in sources:
        print(f"scanning {source.name}", flush=True)
        targets.append(scan_target(source))
    plan = {
        "schema_version": "vault_record_consolidation_v1",
        "status": "planned",
        "generated_utc": utc_now(),
        "recoverable": True,
        "vault": str(vault),
        "cold_archive_run": str(destination),
        "targets": targets,
        "moved_files": sum(row["file_count"] for row in targets),
        "moved_bytes": sum(row["bytes"] for row in targets),
        "record_layer_retained": ["PROJECT_COMMONS", "projects", "maintenance"],
    }
    if not apply:
        return plan

    receipt_dir = vault / "maintenance" / "consolidation"
    plan_path = receipt_dir / f"{run_id}.plan.json"
    write_json_atomic(plan_path, plan)
    destination.mkdir(parents=True, exist_ok=False)
    legacy_readme = vault / "README.md"
    if legacy_readme.is_file():
        shutil.copy2(legacy_readme, destination / "LEGACY_VAULT_README.md")

    for source in sources:
        target = destination / source.name
        if target.exists():
            raise FileExistsError(target)
        print(f"moving {source} -> {target}", flush=True)
        shutil.move(str(source), str(target))

    verified_targets = []
    for expected in targets:
        moved = destination / expected["name"]
        print(f"verifying {moved.name}", flush=True)
        actual = scan_target(moved)
        for key in ("kind", "file_count", "bytes", "tree_sha256"):
            if actual[key] != expected[key]:
                raise RuntimeError(
                    f"post-move verification mismatch for {moved.name}: {key}"
                )
        if (vault / expected["name"]).exists():
            raise RuntimeError(f"source remained after move: {expected['name']}")
        verified_targets.append(expected)

    receipt = dict(plan)
    receipt.update(
        {
            "status": "completed_verified",
            "completed_utc": utc_now(),
            "targets": verified_targets,
        }
    )
    receipt_bytes = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    receipt["receipt_sha256"] = hashlib.sha256(receipt_bytes).hexdigest()
    current_receipt = receipt_dir / "VAULT_RECORD_CONSOLIDATION_CURRENT.json"
    immutable_receipt = receipt_dir / f"{run_id}.completed.json"
    cold_receipt = destination / "VAULT_RECORD_CONSOLIDATION_RECEIPT.json"
    for path in (current_receipt, immutable_receipt, cold_receipt):
        write_json_atomic(path, receipt)
    pointer = build_pointer(receipt)
    write_text_atomic(vault / "COLD_ARCHIVE_POINTERS_CURRENT.md", pointer)
    write_text_atomic(destination / "COLD_ARCHIVE_POINTERS.md", pointer)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", type=Path, required=True)
    parser.add_argument("--cold-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--target", action="append", dest="targets")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = consolidate(
        args.vault,
        args.cold_root,
        args.run_id,
        args.targets or list(ALLOWED_TARGETS),
        args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
