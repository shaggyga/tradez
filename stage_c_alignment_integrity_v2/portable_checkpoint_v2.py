"""Export and replay a compact, offline Stage C checkpoint with explicit inputs.

Only SOURCE_ALLOWLIST is exported.  This is an issuance/recovery fixture, not a
backup of the historical archive, feature campaigns, trading state or secrets.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
import platform
import re
import stat
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from publication import sha256_file, verify_completed_run

SOURCE_ALLOWLIST = (
    "all68_neutral_runner_v2.py", "contracts.py", "publication.py",
    "calendar_contract.py", "portable_checkpoint_v2.py",
)
DEPENDENCIES = ("numpy", "pyarrow", "tzdata")
SCHEMA = "forex_portable_checkpoint.v2"
MANIFEST = "CHECKPOINT_MANIFEST.json"
ORIGIN = "2024-06-24T00:00:00+00:00"
MAX_MEMBER_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024


def encoded(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_member(name: str) -> PurePosixPath:
    """Accept portable file names only, rejecting Windows aliases and ADS."""
    if not isinstance(name, str) or not name or len(name) > 200 or "\\" in name or ":" in name:
        raise ValueError("unsafe checkpoint member")
    parts = name.split("/")
    for part in parts:
        stem = part.split(".", 1)[0].upper()
        if (not re.fullmatch(r"[A-Za-z0-9_.-]+", part) or part in {".", ".."}
                or part.endswith((".", " ")) or stem in {"CON", "PRN", "AUX", "NUL"}
                or re.fullmatch(r"(?:COM|LPT)[1-9]", stem)):
            raise ValueError("unsafe checkpoint member")
    return PurePosixPath(name)


def environment_lock() -> dict[str, Any]:
    return {"schema_version": "forex_fixture_dependencies.v2", "python_version": platform.python_version(),
            "packages": {name: importlib.metadata.version(name) for name in DEPENDENCIES}}


def check_dependencies(expected: dict[str, Any]) -> None:
    if expected != environment_lock():
        raise ValueError("dependency lock mismatch; provision the recorded Python and package versions before replay")


def make_fixture(root: Path) -> dict[str, Any]:
    """Generate 68 clearly synthetic Parquet histories; no historical data read."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    archive_path = inputs / "long_m1_68.zip"
    members = []
    with zipfile.ZipFile(archive_path, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for index in range(68):
            instrument = f"PAIR{index:02d}"
            name = f"M1/{instrument}.parquet"
            buffer = io.BytesIO()
            table = pa.table({"datetime": ["2024-06-23T23:59:00+00:00", ORIGIN, "2024-06-25T00:00:00+00:00"],
                              "close": [1 + index / 1000, 1.01 + index / 1000, 999.0 + index]})
            pq.write_table(table, buffer, row_group_size=1, compression="NONE")
            payload = buffer.getvalue()
            info = zipfile.ZipInfo(name, date_time=(2024, 6, 24, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, payload)
            members.append({"instrument": instrument, "path": name, "bytes": len(payload), "sha256": digest(payload)})
    manifest = {"schema": "forex_portable_audited_inputs_v1", "input_tier": "synthetic_contract_fixture.v2",
                "archives": [{"path": "inputs/long_m1_68.zip", "bytes": archive_path.stat().st_size,
                              "sha256": sha256_file(archive_path), "members": members}]}
    (inputs / "INPUT_ARCHIVES.json").write_bytes(encoded(manifest))
    return manifest


def replay(root: Path, runs: Path, python_executable: str) -> dict[str, Any]:
    config = json.loads((root / "RUN_CONFIG.json").read_text(encoding="utf-8"))
    if config != {"origin": ORIGIN, "horizon_seconds": 86400, "run_id": "fixture", "mode": "offline_no_service_no_order"}:
        raise ValueError("unsupported bounded replay configuration")
    runtime = subprocess.run(
        [python_executable, "-I", "-B", "-c", "import json,platform,importlib.metadata as m; "
         "print(json.dumps({'schema_version':'forex_fixture_dependencies.v2','python_version':platform.python_version(),"
         "'packages':{name:m.version(name) for name in ('numpy','pyarrow','tzdata')}}))"],
        cwd=root, capture_output=True, text=True, timeout=30, check=False,
    )
    if runtime.returncode or json.loads(runtime.stdout) != json.loads((root / "DEPENDENCIES.json").read_text(encoding="utf-8")):
        raise ValueError("subprocess interpreter dependency lock mismatch")
    result = subprocess.run(
        [python_executable, "-I", "-B", str(root / "source" / "all68_neutral_runner_v2.py"),
         "--run-id", config["run_id"], "--archive", str(root / "inputs" / "long_m1_68.zip"),
         "--input-manifest", str(root / "inputs" / "INPUT_ARCHIVES.json"), "--runs-dir", str(runs),
         "--origin", config["origin"], "--horizon-seconds", str(config["horizon_seconds"])],
        cwd=root, capture_output=True, text=True, timeout=120, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"bounded fixture runner failed ({result.returncode}): {result.stderr[-3000:]}")
    run_root = runs / "fixture"
    identity = json.loads((run_root / "RUN_IDENTITY.json").read_text(encoding="utf-8"))
    manifest = verify_completed_run(run_root, identity)
    report = json.loads((run_root / "run_report.json").read_text(encoding="utf-8"))
    if report["issuance"]["coverage_count"] != 68 or report["issuance"]["eligible_count"] != 68:
        raise ValueError("synthetic all-68 fixture did not issue exactly 68 forecasts")
    return {"identity": identity["fingerprint"],
            "payloads": {item["path"]: item["sha256"] for item in manifest["payloads"]},
            "coverage_count": 68, "eligible_count": 68}


def _reference_data(input_manifest: Path | None) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": "forex_external_dataset_references.v2", "bulk_data_included": False}
    if input_manifest is not None:
        supplied = json.loads(input_manifest.read_text(encoding="utf-8"))
        matches = [item for item in supplied["archives"] if item["path"] == "inputs/long_m1_68.zip"]
        if len(matches) != 1:
            raise ValueError("external archive reference must resolve exactly once")
        reference = matches[0]
        result["historical_archive"] = {"path_reference": str(input_manifest.parent / "long_m1_68.zip"),
                                        "declared_sha256": reference["sha256"],
                                        "manifest_sha256": sha256_file(input_manifest),
                                        "verification": "reference_only_bulk_archive_not_rehashed_by_export"}
    return result


def export_checkpoint(source: Path, package: Path, *, input_manifest: Path | None = None,
                      python_executable: str = sys.executable) -> dict[str, Any]:
    """Publish immutable ZIP then a completion receipt; no recursive source copy."""
    receipt_path = package.with_suffix(package.suffix + ".receipt.json")
    if package.exists() or receipt_path.exists():
        raise FileExistsError("checkpoint and receipt are immutable; choose a new package name")
    package.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="forex-fixture-build-", dir=package.parent) as temporary:
        stage = Path(temporary)
        (stage / "source").mkdir()
        for name in SOURCE_ALLOWLIST:
            source_file = source / name
            if source_file.is_symlink() or not source_file.is_file():
                raise ValueError(f"missing or linked allowlisted source: {name}")
            (stage / "source" / name).write_bytes(source_file.read_bytes())
        make_fixture(stage)
        lock = environment_lock()
        (stage / "DEPENDENCIES.json").write_bytes(encoded(lock))
        (stage / "requirements.lock").write_text("".join(f"{name}=={lock['packages'][name]}\n" for name in DEPENDENCIES), encoding="utf-8")
        (stage / "RUN_CONFIG.json").write_bytes(encoded({"origin": ORIGIN, "horizon_seconds": 86400, "run_id": "fixture", "mode": "offline_no_service_no_order"}))
        (stage / "DATASET_REFERENCES.json").write_bytes(encoded(_reference_data(input_manifest)))
        expected = replay(stage, stage / "expected", python_executable)
        (stage / "EXPECTED_REPLAY.json").write_bytes(encoded(expected))
        (stage / "README_RESTORE.md").write_text(
            "# Portable Stage C issuance checkpoint\n\n"
            "Scope: the all-68 synthetic no-change issuance and recovery boundary. "
            "This fixture is not market evidence, a full project backup, or a feature/accounting campaign. "
            "No credentials, trading state, services, bulk historical candles, trad modules, or feature audit programs are included.\n\n"
            "Use the external ZIP SHA-256 from its completion receipt, not a hash supplied by the ZIP. "
            "Provision the exact Python version in DEPENDENCIES.json and packages in requirements.lock. "
            "Run the existing trusted portable_checkpoint_v2.py with restore --package <zip> "
            "--sha256 <external hash> --destination <empty unrelated local directory>. "
            "Restore verifies every member, checks dependencies, reruns the frozen runner in a subprocess, "
            "and compares every deterministic payload and run identity against expected/. "
            "Completion timestamps are provenance and are not deterministic payloads.\n\n"
            "CHECKPOINT_MANIFEST.json hashes every other ZIP member; the external ZIP hash covers that manifest. "
            "The ZIP manifest is written last, and the external receipt is published only after ZIP close. "
            "A ZIP without its completed external receipt is not a published checkpoint.\n",
            encoding="utf-8",
        )
        files = sorted(path for path in stage.rglob("*") if path.is_file())
        inventory = []
        for path in files:
            name = path.relative_to(stage).as_posix()
            safe_member(name)
            inventory.append({"path": name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
        manifest = {"schema_version": SCHEMA, "source_allowlist": list(SOURCE_ALLOWLIST),
                    "scope": "synthetic_all68_issuance_recovery_only", "members": inventory}
        with zipfile.ZipFile(package, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            for item in inventory:
                archive.write(stage / item["path"], arcname=item["path"])
            archive.writestr(MANIFEST, encoded(manifest))
    receipt = {"schema_version": "forex_checkpoint_publication_receipt.v2", "status": "COMPLETE",
               "package": package.name, "sha256": sha256_file(package), "bytes": package.stat().st_size,
               "member_count": len(inventory) + 1, "expected_replay": expected}
    with receipt_path.open("xb") as handle:
        handle.write(encoded(receipt))
    return receipt


def inspect_package(package: Path, expected_sha256: str, *, expected_source_allowlist: tuple[str, ...] = SOURCE_ALLOWLIST,
                    expected_schema: str = SCHEMA) -> dict[str, bytes]:
    """Validate all bytes and names before creating the restore destination."""
    if package.stat().st_size > MAX_TOTAL_BYTES:
        raise ValueError("checkpoint exceeds compact size limit")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) or sha256_file(package) != expected_sha256.lower():
        raise ValueError("external checkpoint SHA-256 mismatch")
    payloads: dict[str, bytes] = {}
    with zipfile.ZipFile(package) as archive:
        infos = archive.infolist()
        if len(infos) > 256:
            raise ValueError("checkpoint exceeds compact member count limit")
        if not infos or infos[-1].filename != MANIFEST:
            raise ValueError("checkpoint completion manifest is missing or not last")
        if sum(info.file_size for info in infos) > MAX_TOTAL_BYTES:
            raise ValueError("checkpoint exceeds compact size limit")
        aliases: set[str] = set()
        directory_aliases: dict[str, str] = {}
        for info in infos:
            safe_member(info.orig_filename)
            if info.orig_filename != info.filename:
                raise ValueError("unsafe normalized checkpoint member")
            relative = safe_member(info.filename)
            alias = info.filename.casefold()
            mode = (info.external_attr >> 16) & 0o170000
            if alias in aliases or info.is_dir() or mode not in {0, stat.S_IFREG}:
                raise ValueError("duplicate, linked or unsupported checkpoint member")
            if info.file_size > MAX_MEMBER_BYTES:
                raise ValueError("checkpoint member exceeds compact size limit")
            aliases.add(alias)
            for parent in relative.parents:
                if parent == PurePosixPath("."):
                    continue
                text = parent.as_posix()
                if text.casefold() in directory_aliases and directory_aliases[text.casefold()] != text:
                    raise ValueError("directory case aliases are not portable")
                directory_aliases[text.casefold()] = text
            payloads[info.filename] = archive.read(info)
        if aliases.intersection(directory_aliases):
            raise ValueError("checkpoint member conflicts with a parent directory")
    manifest = json.loads(payloads[MANIFEST])
    if manifest.get("schema_version") != expected_schema or manifest.get("source_allowlist") != list(expected_source_allowlist):
        raise ValueError("unsupported checkpoint contract")
    records = manifest.get("members", [])
    if len(records) != len({record["path"] for record in records}):
        raise ValueError("duplicate checkpoint manifest entry")
    if {record["path"] for record in records} != set(payloads) - {MANIFEST}:
        raise ValueError("checkpoint manifest inventory mismatch")
    for record in records:
        data = payloads[record["path"]]
        if len(data) != record["bytes"] or digest(data) != record["sha256"]:
            raise ValueError("checkpoint member hash or size mismatch")
    return payloads


def _plain_destination(destination: Path) -> None:
    for path in (destination, *destination.parents):
        if path.exists():
            metadata = path.lstat()
            if path.is_symlink() or getattr(metadata, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024):
                raise ValueError("restore destination and parents must not be linked or reparse points")
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise FileExistsError("restore destination must be an empty directory")


def restore_checkpoint(package: Path, destination: Path, expected_sha256: str, *,
                       python_executable: str = sys.executable) -> dict[str, Any]:
    payloads = inspect_package(package, expected_sha256)
    check_dependencies(json.loads(payloads["DEPENDENCIES.json"]))
    _plain_destination(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for name, data in payloads.items():
        target = destination.joinpath(*safe_member(name).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as handle:
            handle.write(data)
        if sha256_file(target) != digest(data):
            raise ValueError("restored file readback mismatch")
    expected = json.loads(payloads["EXPECTED_REPLAY.json"])
    actual = replay(destination, destination / "replay", python_executable)
    if actual != expected:
        raise ValueError("relocated subprocess replay differs from the frozen expected payloads")
    receipt = {"schema_version": "forex_checkpoint_restore_receipt.v2", "status": "VERIFIED",
               "package_sha256": expected_sha256.lower(), "restored_member_count": len(payloads),
               "dependency_lock_verified": True, "subprocess_replay_verified": True, "replay": actual}
    with (destination / "RESTORE_RECEIPT.json").open("xb") as handle:
        handle.write(encoded(receipt))
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--source", type=Path, default=ROOT)
    export.add_argument("--package", type=Path, required=True)
    export.add_argument("--input-manifest", type=Path)
    restore = commands.add_parser("restore")
    restore.add_argument("--package", type=Path, required=True)
    restore.add_argument("--destination", type=Path, required=True)
    restore.add_argument("--sha256", required=True)
    args = parser.parse_args()
    if args.command == "export":
        result = export_checkpoint(args.source, args.package, input_manifest=args.input_manifest)
    else:
        result = restore_checkpoint(args.package, args.destination, args.sha256)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
