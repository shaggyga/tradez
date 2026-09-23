"""Issue or resume a bounded, offline all-68 no-change forecast tape.

Only the exact origin close is consumed by issuance. Physical archive integrity
checks and Parquet column reads also read later bytes; those values are never
used to select issuance, fit a model, or evaluate an outcome. Outcome records
remain PENDING. A hash-verified origin snapshot supports archive-free recovery.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import zipfile
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path, PurePosixPath
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from contracts import TrainingView, forecast_record, outcome_record
from publication import RunPublisher, effective_run_identity, sha256_file, verify_completed_run

CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
INPUT_MANIFEST = CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json"
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
RUNS = ROOT / "runs"
REQUIRED_PAYLOADS = {"origin_inputs.json", "forecast_coverage.jsonl", "forecasts.jsonl", "outcomes.jsonl", "run_report.json"}
NO_FIT_VIEW = TrainingView(0, 1, 1, 1, 2)
FIXTURE_TIER = "synthetic_contract_fixture.v2"


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


def parse_utc_epoch(value: str) -> int:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("origin must include a UTC offset")
    if stamp.microsecond or int(stamp.timestamp()) % 60:
        raise ValueError("origin must identify a whole minute bar start")
    return int(stamp.timestamp())


def _valid_digest(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def read_long_members(input_manifest: Path | None = None) -> tuple[list[dict[str, Any]], str]:
    manifest = json.loads((INPUT_MANIFEST if input_manifest is None else input_manifest).read_text(encoding="utf-8"))
    if manifest.get("schema") != "forex_portable_audited_inputs_v1":
        raise ValueError("audited_input_manifest_schema_required")
    matches = [item for item in manifest.get("archives", []) if item.get("path") == "inputs/long_m1_68.zip"]
    if len(matches) != 1:
        raise ValueError("one_audited_long_archive_required")
    long = matches[0]
    if not isinstance(long.get("members"), list) or len(long["members"]) != 68:
        raise ValueError("audited_long_68_manifest_required")
    declared = long.get("sha256")
    if not _valid_digest(declared):
        raise ValueError("audited_long_archive_hash_required")
    members = sorted(long["members"], key=lambda item: str(item["instrument"]))
    if len({item["instrument"] for item in members}) != 68 or len({item["path"] for item in members}) != 68:
        raise ValueError("audited_instrument_or_member_universe_not_unique")
    for item in members:
        path = PurePosixPath(item["path"])
        if (not isinstance(item["instrument"], str) or not item["instrument"] or path.is_absolute()
                or ".." in path.parts or "\\" in item["path"] or path.suffix != ".parquet"):
            raise ValueError("invalid_archive_member_identity")
        if not _valid_digest(item.get("sha256")) or type(item.get("bytes")) is not int or item["bytes"] <= 0:
            raise ValueError("audited_member_hash_and_size_required")
    return members, declared


def _utc_timestamps(values: pa.ChunkedArray) -> pa.ChunkedArray:
    # Arrow normalizes equivalent ISO offsets and Z; exact string equality does
    # not. A timezone-free typed column would silently invent provenance.
    if pa.types.is_timestamp(values.type) and values.type.tz is None:
        raise ValueError("origin_datetime_column_requires_timezone")
    try:
        return pc.cast(values, pa.timestamp("us", tz="UTC"))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ValueError("origin_datetime_column_not_valid_timezone_aware_timestamps") from exc


def origin_coverage(members: list[dict[str, Any]], origin_epoch: int, archive_path: Path | None = None,
                    *, verify_members: bool = False) -> list[dict[str, Any]]:
    """Select exact origin closes across every row group; later closes are unused.

    Physical integrity reads cover each member's entire bytes. Parquet reads the
    datetime/close columns by row group before selecting the origin. This is a
    causal consumer boundary, not a claim that future bytes are inaccessible.
    """
    origin = pa.scalar(datetime.fromtimestamp(origin_epoch, tz=timezone.utc), type=pa.timestamp("us", tz="UTC"))
    rows: list[dict[str, Any]] = []
    with zipfile.ZipFile(ARCHIVE if archive_path is None else archive_path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("duplicate_archive_member")
        if verify_members and set(names) != {row["path"] for row in members}:
            raise ValueError("archive_member_inventory_differs_from_audited_manifest")
        for member in members:
            instrument, path = str(member["instrument"]), str(member["path"])
            if path not in names:
                rows.append({"instrument": instrument, "status": "blocked", "reason": "archive_member_missing"})
                continue
            raw = archive.read(path)
            if verify_members and (len(raw) != member["bytes"] or hashlib.sha256(raw).hexdigest() != member["sha256"]):
                raise ValueError(f"archive_member_integrity_mismatch:{instrument}")
            parquet = pq.ParquetFile(pa.BufferReader(raw))
            closes: list[Any] = []
            for group in range(parquet.num_row_groups):
                table = parquet.read_row_group(group, columns=["datetime", "close"])
                selected = table.filter(pc.equal(_utc_timestamps(table["datetime"]), origin))
                closes.extend(selected["close"].to_pylist())
            if len(closes) != 1:
                rows.append({"instrument": instrument, "status": "blocked", "reason": "origin_bar_missing_or_ambiguous"})
                continue
            try:
                close = float(closes[0])
            except (ValueError, TypeError):
                close = float("nan")
            if not isfinite(close) or close <= 0:
                rows.append({"instrument": instrument, "status": "blocked", "reason": "origin_close_invalid"})
                continue
            rows.append({"instrument": instrument, "status": "eligible", "reason": "origin_bar_available", "origin_close": close})
    return rows


def build_identity(archive_hash: str, origin_epoch: int, horizon_seconds: int,
                   input_manifest: Path | None = None) -> dict[str, Any]:
    manifest_path = INPUT_MANIFEST if input_manifest is None else input_manifest
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dependency_hashes = {
        "archive_sha256": archive_hash,
        "input_manifest_sha256": sha256_file(manifest_path),
        "runner_source_sha256": sha256_file(Path(__file__)),
        "contracts_source_sha256": sha256_file(ROOT / "contracts.py"),
        "publication_source_sha256": sha256_file(ROOT / "publication.py"),
    }
    contract = {
        "schema": "all68_neutral_issuance.v2.1",
        "universe_size": 68,
        "required_payloads": sorted(REQUIRED_PAYLOADS),
        "origin_epoch": origin_epoch,
        "decision_epoch": origin_epoch + 60,
        "horizon_seconds": horizon_seconds,
        "target_id": "endpoint_only_midpoint_return_elapsed.v2",
        "predictor": "no_change_control.v2",
        "issuance_consumes": ["exact_origin_close_only"],
        "physical_reads": "full_archive_integrity_and_parquet_datetime_close_row_groups",
        "input_tier": manifest.get("input_tier", "historical_snapshot_original_arrival_unverified"),
        "outcome_state_at_issuance": "PENDING",
        "mode": "offline_no_service_no_order",
        "runtime_versions": {"python": ".".join(map(str, sys.version_info[:3])), "pyarrow": pa.__version__, "numpy": np.__version__},
    }
    return effective_run_identity(contract=contract, dependency_hashes=dependency_hashes)


def _validate_cached_inputs(raw: bytes, identity: dict[str, Any], members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    snapshot = json.loads(raw)
    if (set(snapshot) != {"schema_version", "run_identity_fingerprint", "origin_epoch", "coverage"}
            or snapshot["schema_version"] != "all68_origin_inputs.v2"
            or snapshot["run_identity_fingerprint"] != identity["fingerprint"]
            or snapshot["origin_epoch"] != identity["contract"]["origin_epoch"]):
        raise ValueError("cached_origin_inputs_identity_mismatch")
    coverage = snapshot["coverage"]
    if not isinstance(coverage, list) or [row.get("instrument") for row in coverage] != [row["instrument"] for row in members]:
        raise ValueError("cached_origin_inputs_universe_mismatch")
    for row in coverage:
        if row.get("status") == "eligible":
            if (set(row) != {"instrument", "status", "reason", "origin_close"}
                    or row["reason"] != "origin_bar_available" or not isfinite(float(row["origin_close"]))
                    or float(row["origin_close"]) <= 0):
                raise ValueError("cached_origin_inputs_invalid_eligible_record")
        elif (row.get("status") != "blocked" or set(row) != {"instrument", "status", "reason"}
                or row.get("reason") not in {"archive_member_missing", "origin_bar_missing_or_ambiguous", "origin_close_invalid"}):
            raise ValueError("cached_origin_inputs_invalid_blocked_record")
    return coverage


def issuance_payloads(coverage: list[dict[str, Any]], identity: dict[str, Any]) -> dict[str, bytes]:
    """Deterministic production consumer; no archive or wall-clock access."""
    contract = identity["contract"]
    origin_epoch, horizon_seconds = contract["origin_epoch"], contract["horizon_seconds"]
    decision_epoch, outcome_ready = origin_epoch + 60, origin_epoch + horizon_seconds + 60
    forecasts, outcomes, coverage_records = [], [], []
    for row in coverage:
        coverage_records.append({**row, "schema_version": "forecast_coverage.v2", "origin_epoch": origin_epoch, "decision_epoch": decision_epoch})
        if row["status"] != "eligible":
            continue
        forecast_id = f"{row['instrument']}:{origin_epoch}:{horizon_seconds}:no_change_v2"
        forecasts.append(forecast_record(forecast_id=forecast_id, instrument=row["instrument"], decision_epoch=decision_epoch,
            available_epoch=decision_epoch, model_id="no_change_control.v2", model_ready_epoch=0,
            training_view=NO_FIT_VIEW, target_id="endpoint_only_midpoint_return_elapsed.v2", prediction=0.0))
        outcomes.append(outcome_record(forecast_id=forecast_id, outcome_ready_epoch=outcome_ready, state="PENDING", value=None))
    report = {
        "schema_version": "all68_neutral_run_report.v2.1", "status": "completed_offline_issuance_not_a_model_or_pnl_result",
        "run_identity_fingerprint": identity["fingerprint"],
        "input": {"archive_sha256": identity["dependency_hashes"]["archive_sha256"], "member_count": len(coverage),
                  "input_tier": contract["input_tier"], "snapshot": "origin_inputs.json"},
        "issuance": {"origin_epoch": origin_epoch, "decision_epoch": decision_epoch, "horizon_seconds": horizon_seconds,
                     "outcome_ready_epoch": outcome_ready, "coverage_count": len(coverage), "eligible_count": len(forecasts),
                     "blocked_count": len(coverage) - len(forecasts), "outcome_records": len(outcomes)},
        "causal_boundary": "only_exact_origin_close_consumed; physical_integrity_and_row_group_reads_include_later_bytes",
        "not_performed": ["target_bar_selection", "outcome_evaluation", "model_fit", "policy_comparison", "broker_or_network_access", "order_or_trade"],
    }
    return {"forecast_coverage.jsonl": b"".join(json_bytes(row) for row in coverage_records),
            "forecasts.jsonl": b"".join(json_bytes(row) for row in forecasts),
            "outcomes.jsonl": b"".join(json_bytes(row) for row in outcomes), "run_report.json": json_bytes(report)}


def _test_crash(selected: str | None, stage: str) -> None:
    if selected == stage:
        # Available only with an explicitly synthetic manifest; never enabled
        # for audited historical inputs or through ambient environment state.
        os._exit(91)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="lowercase immutable run directory")
    parser.add_argument("--origin", default="2024-06-24T00:00:00+00:00")
    parser.add_argument("--horizon-seconds", type=int, default=86400)
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--input-manifest", type=Path, default=INPUT_MANIFEST)
    parser.add_argument("--runs-dir", type=Path, default=RUNS)
    parser.add_argument("--resume", action="store_true", help="recover an interrupted local writer and reuse verified origin inputs")
    parser.add_argument("--report-only", action="store_true", help="rebuild deterministic outputs using verified origin inputs; never open the archive")
    parser.add_argument("--test-crash-after", choices=["inputs", "first-payload", "payloads", "completion"], help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.horizon_seconds <= 0:
        raise ValueError("horizon_seconds_must_be_positive")
    origin_epoch = parse_utc_epoch(args.origin)
    members, declared_hash = read_long_members(args.input_manifest)
    identity = build_identity(declared_hash, origin_epoch, args.horizon_seconds, args.input_manifest)
    if args.test_crash_after and identity["contract"]["input_tier"] != FIXTURE_TIER:
        raise ValueError("test_crash_hooks_require_synthetic_fixture_tier")
    publisher = RunPublisher(args.runs_dir, args.run_id, identity)
    if (publisher.root / "COMPLETION_MANIFEST.json").exists():
        verify_completed_run(publisher.root, identity)
        print(f"verified completed {args.run_id}; no archive extraction")
        return 0
    publisher.acquire(recover=args.resume)
    try:
        raw_inputs = publisher.read_verified_payload("origin_inputs.json")
        if raw_inputs is None:
            if args.report_only:
                raise ValueError("report_only_requires_verified_origin_inputs")
            actual_hash = sha256_file(args.archive)
            if actual_hash != declared_hash:
                raise ValueError("long_archive_hash_does_not_match_audited_manifest")
            coverage = origin_coverage(members, origin_epoch, args.archive, verify_members=True)
            raw_inputs = json_bytes({"schema_version": "all68_origin_inputs.v2", "run_identity_fingerprint": identity["fingerprint"],
                                    "origin_epoch": origin_epoch, "coverage": coverage})
        coverage = _validate_cached_inputs(raw_inputs, identity, members)
        payloads = [publisher.write_or_validate_payload("origin_inputs.json", raw_inputs)]
        _test_crash(args.test_crash_after, "inputs")
        for index, (name, payload) in enumerate(issuance_payloads(coverage, identity).items()):
            payloads.append(publisher.write_or_validate_payload(name, payload))
            if index == 0:
                _test_crash(args.test_crash_after, "first-payload")
        _test_crash(args.test_crash_after, "payloads")
        publisher.complete(payloads, REQUIRED_PAYLOADS)
        _test_crash(args.test_crash_after, "completion")
    except Exception:
        publisher.release()
        raise
    eligible = sum(row["status"] == "eligible" for row in coverage)
    print(f"completed {args.run_id}: {eligible}/68 eligible, pending outcomes only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
