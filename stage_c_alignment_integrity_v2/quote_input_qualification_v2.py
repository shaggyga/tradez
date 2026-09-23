"""Qualify retained bid/ask candle-close availability without execution claims.

This offline inspection checks source bytes and exact origin/target rows. The
co-stamped bid/ask candle closes are not evidence of simultaneous executable
quotes or of their historical arrival times. No trades or PnL are computed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import all68_neutral_runner_v2 as runner
from publication import sha256_file


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if isfinite(result) else None


def qualify_point(rows: list[dict[str, Any]], raw_epoch: int, *, columns_present: bool) -> dict[str, Any]:
    """Qualify an exact raw-minute candle stamp; every duplicate is ambiguous."""
    point: dict[str, Any] = {
        "raw_bar_start_epoch": raw_epoch,
        "assumed_bar_ready_epoch": raw_epoch + 60,
        "row_count": len(rows),
        "missing_count": int(not rows),
        "duplicate_extra_count": max(0, len(rows) - 1),
        "invalid_quote_row_count": 0,
    }
    for row in rows:
        bid, ask = _number(row.get("bid_close")), _number(row.get("ask_close"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
            point["invalid_quote_row_count"] += 1
    if not columns_present:
        point["status"] = "missing_bid_ask_columns"
    elif not rows:
        point["status"] = "missing_exact_bar"
    elif len(rows) != 1:
        point["status"] = "duplicate_exact_bar"
    elif point["invalid_quote_row_count"]:
        point["status"] = "invalid_bid_ask_close"
    else:
        bid, ask = float(rows[0]["bid_close"]), float(rows[0]["ask_close"])
        point.update({"status": "valid_candle_close_pair", "bid_close": bid, "ask_close": ask,
                      "spread_price_units": ask - bid, "source_midpoint_close": _number(rows[0].get("close"))})
    return point


def qualify_member(raw: bytes, instrument: str, origin_epoch: int, target_epoch: int) -> dict[str, Any]:
    parquet = pq.ParquetFile(pa.BufferReader(raw))
    names = set(parquet.schema_arrow.names)
    if "datetime" not in names:
        raise ValueError(f"source_member_missing_datetime:{instrument}")
    quote_columns_present = {"bid_close", "ask_close"}.issubset(names)
    columns = [name for name in ("datetime", "bid_close", "ask_close", "close") if name in names]
    selected: dict[str, list[dict[str, Any]]] = {"origin": [], "target": []}
    clocks = {name: pa.scalar(datetime.fromtimestamp(epoch, tz=timezone.utc), type=pa.timestamp("us", tz="UTC"))
              for name, epoch in (("origin", origin_epoch), ("target", target_epoch))}
    for index in range(parquet.num_row_groups):
        table = parquet.read_row_group(index, columns=columns)
        stamps = runner._utc_timestamps(table["datetime"])
        for name, clock in clocks.items():
            selected[name].extend(table.filter(pc.equal(stamps, clock)).to_pylist())
    origin = qualify_point(selected["origin"], origin_epoch, columns_present=quote_columns_present)
    target = qualify_point(selected["target"], target_epoch, columns_present=quote_columns_present)
    return {"instrument": instrument, "parquet_row_groups": parquet.num_row_groups,
            "parquet_row_count": parquet.metadata.num_rows,
            "bid_ask_close_columns_present": quote_columns_present,
            "status": "both_candle_close_pairs_valid" if all(point["status"] == "valid_candle_close_pair" for point in (origin, target)) else "candle_close_pair_unavailable_or_invalid",
            "origin": origin, "target": target}


def qualify_archive(archive_path: Path, manifest_path: Path, origin_epoch: int, horizon_seconds: int) -> dict[str, Any]:
    if horizon_seconds <= 0:
        raise ValueError("horizon_seconds_must_be_positive")
    members, declared_archive_hash = runner.read_long_members(manifest_path)
    target_epoch = origin_epoch + horizon_seconds
    results = []
    with zipfile.ZipFile(archive_path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != {member["path"] for member in members}:
            raise ValueError("archive_member_inventory_differs_from_manifest")
        for member in members:
            raw = archive.read(member["path"])
            if len(raw) != member["bytes"] or hashlib.sha256(raw).hexdigest() != member["sha256"]:
                raise ValueError(f"source_member_hash_or_size_mismatch:{member['instrument']}")
            result = qualify_member(raw, member["instrument"], origin_epoch, target_epoch)
            result["source_member"] = {"path": member["path"], "bytes": len(raw), "sha256": member["sha256"],
                                       "validation": "actual_member_size_and_sha256_match_manifest"}
            results.append(result)
    counts: dict[str, Any] = {"universe": len(results), "members_with_bid_ask_columns": sum(row["bid_ask_close_columns_present"] for row in results),
                              "both_candle_close_pairs_valid": sum(row["status"] == "both_candle_close_pairs_valid" for row in results)}
    for point in ("origin", "target"):
        counts[point] = {
            "valid_pairs": sum(row[point]["status"] == "valid_candle_close_pair" for row in results),
            "missing_bars": sum(row[point]["missing_count"] for row in results),
            "duplicate_extra_rows": sum(row[point]["duplicate_extra_count"] for row in results),
            "invalid_quote_rows": sum(row[point]["invalid_quote_row_count"] for row in results),
            "status_counts": {status: sum(row[point]["status"] == status for row in results)
                              for status in sorted({row[point]["status"] for row in results})},
        }
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "schema_version": "forex_retained_quote_input_qualification.v2",
        "status": "offline_retained_candle_quote_availability_only",
        "input_tier": manifest.get("input_tier", "historical_snapshot_original_arrival_unverified"),
        "source_integrity": {
            "archive_sha256_declared": declared_archive_hash,
            "archive_container_sha256_recomputed": False,
            "archive_container_hash_scope": "declared_manifest_value_only; full_member_payloads_verified_individually",
            "manifest_sha256": sha256_file(manifest_path),
            "exact_archive_member_inventory_verified": True,
            "member_sha256_and_size_verified_count": len(results),
        },
        "implementation_sha256": {"qualification": sha256_file(Path(__file__)),
                                  "reader": sha256_file(Path(runner.__file__)),
                                  "publication_hash_utility": sha256_file(ROOT / "publication.py")},
        "clock_contract": {"raw_datetime_semantics": "bar_start", "origin_epoch": origin_epoch,
                           "target_epoch": target_epoch, "horizon_seconds": horizon_seconds,
                           "assumed_candle_close_ready_offset_seconds": 60,
                           "actual_quote_arrival_provenance": "unverified"},
        "counts": counts,
        "instruments": results,
        "limitations": [
            "Co-stamped bid/ask candle closes are not proof of simultaneously executable quotes.",
            "Source history does not establish original first-known, arrival, or revision timestamps.",
            "Readiness at bar start plus 60 seconds is a declared assumption, not measured arrival provenance.",
            "Only two exact historical candle stamps are qualified; availability elsewhere is not inferred.",
            "Account-currency conversion paths, units, financing, slippage, and portfolio accounting are not qualified.",
            "No broker or network access, model fitting, trades, or performance evaluation occurred.",
        ],
        "execution_ready": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=runner.ARCHIVE)
    parser.add_argument("--input-manifest", type=Path, default=runner.INPUT_MANIFEST)
    parser.add_argument("--origin", default="2024-06-24T00:00:00+00:00")
    parser.add_argument("--horizon-seconds", type=int, default=86400)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("qualification output must use a new evidence path")
    report = qualify_archive(args.archive, args.input_manifest, runner.parse_utc_epoch(args.origin), args.horizon_seconds)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as handle:
        handle.write(runner.json_bytes(report))
    print(json.dumps({"output": str(args.output), "counts": report["counts"], "execution_ready": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
