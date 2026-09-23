"""Bind the first all-68 offline experiment to audited, immutable inputs.

This is deliberately a preflight, not a data reader, forecaster, fitter, or
backtest. It reads only JSON manifests and ZIP central directories, and writes
one receipt below this isolated directory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
DEFAULT_PIP_METADATA = Path(r"C:\Users\zmoor\Documents\forex\stage_b_20260921\source\config\pair_local_operational_v2_20260913.json")
EXPECTED_PIP_SHA256 = "c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise ValueError(f"required_file_missing:{path}")
    if path.is_symlink() or path.is_junction():
        raise ValueError(f"reparse_path_refused:{path}")
    return path


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(require_file(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"json_object_required:{path.name}")
    return value


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    resolved_root = STAGE_ROOT.resolve()
    if resolved_root not in path.resolve().parents:
        raise ValueError("output_must_stay_in_stage")
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def locate_long_archive(input_manifest: dict[str, Any], checkpoint: Path) -> tuple[Path, list[dict[str, Any]], str]:
    for archive in input_manifest.get("archives", []):
        if archive.get("path") == "inputs/long_m1_68.zip":
            members = archive.get("members")
            if not isinstance(members, list) or len(members) != 68:
                raise ValueError("long_archive_must_declare_68_members")
            return require_file(checkpoint / archive["path"]), members, str(archive.get("sha256") or "")
    raise ValueError("long_archive_not_declared")


def verify_zip_directory(archive_path: Path, members: list[dict[str, Any]]) -> dict[str, Any]:
    expected = {str(item["path"]): item for item in members}
    if len(expected) != 68:
        raise ValueError("long_archive_member_names_not_unique")
    with zipfile.ZipFile(archive_path) as archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
    actual = {info.filename: info for info in infos}
    if set(actual) != set(expected):
        raise ValueError("zip_member_set_does_not_match_audited_manifest")
    wrong_size = [
        name for name, item in expected.items()
        if actual[name].file_size != int(item["bytes"])
    ]
    if wrong_size:
        raise ValueError(f"zip_member_size_mismatch:{','.join(sorted(wrong_size))}")
    instruments = [str(item.get("instrument") or "") for item in members]
    if any(not item for item in instruments) or len(set(instruments)) != 68:
        raise ValueError("instrument_identity_not_complete")
    return {
        "member_count": len(actual),
        "instruments": sorted(instruments),
        "total_uncompressed_bytes": sum(info.file_size for info in infos),
        "total_compressed_bytes": sum(info.compress_size for info in infos),
        "central_directory_crc32": {name: f"{actual[name].CRC:08x}" for name in sorted(actual)},
    }


def verify_pip_metadata(path: Path) -> dict[str, Any]:
    path = require_file(path)
    actual_sha256 = sha256(path)
    if actual_sha256 != EXPECTED_PIP_SHA256:
        raise ValueError("pip_metadata_hash_mismatch")
    pairs = read_json(path).get("pairs")
    if not isinstance(pairs, dict) or len(pairs) != 68:
        raise ValueError("pip_metadata_requires_68_pairs")
    values: dict[str, float] = {}
    for pair, spec in pairs.items():
        try:
            value = float(spec["pip_size"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid_pip_metadata") from exc
        if not value > 0:
            raise ValueError("nonpositive_pip_size")
        values[str(pair)] = value
    return {"path": str(path), "sha256": actual_sha256, "pairs": values}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--pip-metadata", type=Path, default=DEFAULT_PIP_METADATA)
    parser.add_argument("--output", type=Path, default=STAGE_ROOT / "ALL68_INPUT_PREFLIGHT.json")
    args = parser.parse_args()
    checkpoint = args.checkpoint.resolve()
    source = read_json(checkpoint / "source" / "WORKTREE_SOURCE_LATEST.json")
    inputs = read_json(checkpoint / "inputs" / "INPUT_ARCHIVES.json")
    archive_path, declared_members, declared_sha256 = locate_long_archive(inputs, checkpoint)
    directory = verify_zip_directory(archive_path, declared_members)
    pip_map = verify_pip_metadata(args.pip_metadata)
    declared_instruments = set(directory["instruments"])
    pip_instruments = set(pip_map["pairs"])
    if declared_instruments != pip_instruments:
        raise ValueError("history_and_pip_universe_mismatch")
    payload = {
        "schema": "all68_offline_input_preflight_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed_preflight_not_a_forecast_or_replay",
        "source_snapshot": {
            "snapshot_id": source.get("snapshot_id"),
            "content_sha256": source.get("content_sha256"),
            "worktree_dirty": source.get("worktree_dirty"),
            "base_git_commit": source.get("base_git_commit"),
        },
        "long_history": {
            "archive_path": str(archive_path),
            "declared_archive_sha256": declared_sha256,
            "archive_hash_recomputed_this_run": False,
            "zip_directory": directory,
        },
        "pip_metadata": pip_map,
        "binding": {
            "instrument_count": 68,
            "same_instrument_universe": True,
            "selected_history_vintage": "long_m1_68_only",
            "native_vintage": "excluded_pending_source_selection_and_derived_spread_correction_contract",
        },
        "next_gate": [
            "copy the reviewed dirty worktree snapshot to a new isolated source tree and verify its manifest",
            "add global-clock, forecast-tape, and policy-ledger contracts before extracting bars",
            "benchmark a bounded partition before any 24-hour all-68 replay",
        ],
        "not_performed": [
            "parquet_member_extraction_or_row_reads",
            "model_deserialization_or_fit",
            "forecast_generation_or_policy_comparison",
            "network_or_broker_access",
        ],
    }
    atomic_json(args.output, payload)
    print(f"preflight passed: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
