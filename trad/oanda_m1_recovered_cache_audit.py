#!/usr/bin/env python3
"""Audit recovered M1 parquet coverage without reading complete candle tables."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from oanda_m1_s5_cache_consolidation import M1_COLUMNS


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


def scalar_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def timestamp_bounds(source: pq.ParquetFile, column: str) -> tuple[str, str]:
    index = source.schema_arrow.names.index(column)
    minima: list[str] = []
    maxima: list[str] = []
    for number in range(source.num_row_groups):
        statistics = source.metadata.row_group(number).column(index).statistics
        if statistics is None or not statistics.has_min_max:
            continue
        minima.append(scalar_text(statistics.min))
        maxima.append(scalar_text(statistics.max))
    if minima and maxima:
        return min(minima), max(maxima)
    first = source.read_row_group(0, columns=[column]).column(0).to_pylist()
    last = source.read_row_group(source.num_row_groups - 1, columns=[column]).column(0).to_pylist()
    values = [scalar_text(value) for value in [*first, *last] if value is not None]
    return (min(values), max(values)) if values else ("", "")


def audit_file(path: Path, api_pairs: set[str]) -> dict[str, Any]:
    source = pq.ParquetFile(path)
    names = source.schema_arrow.names
    missing = [column for column in M1_COLUMNS if column not in names]
    time_column = "datetime" if "datetime" in names else "time"
    start, end = timestamp_bounds(source, time_column)
    instrument = path.name.removesuffix("_M1.parquet")
    return {
        "instrument": instrument,
        "path": str(path.resolve()),
        "rows": int(source.metadata.num_rows),
        "row_groups": int(source.num_row_groups),
        "bytes": path.stat().st_size,
        "start_utc": start,
        "end_utc": end,
        "schema_complete": not missing,
        "missing_columns": missing,
        "recovery_source": (
            "OANDA_REST_V20_M1_BAM" if instrument in api_pairs else "deep_M1_plus_recent_OANDA_S5"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--s5-dir", type=Path, required=True)
    parser.add_argument("--api-summary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    expected = sorted(
        path.name.removesuffix("_S5.parquet") for path in args.s5_dir.glob("*_S5.parquet")
    )
    api_payload = {}
    if args.api_summary and args.api_summary.is_file():
        api_payload = json.loads(args.api_summary.read_text(encoding="utf-8-sig"))
    api_pairs = {
        str(row.get("instrument"))
        for row in api_payload.get("records") or []
        if row.get("status") == "completed"
    }
    rows = [
        audit_file(args.cache_dir / f"{instrument}_M1.parquet", api_pairs)
        for instrument in expected
        if (args.cache_dir / f"{instrument}_M1.parquet").is_file()
    ]
    observed = {row["instrument"] for row in rows}
    missing_pairs = sorted(set(expected) - observed)
    schema_failures = [row["instrument"] for row in rows if not row["schema_complete"]]
    fully_covered = not missing_pairs and not schema_failures and len(rows) == len(expected)
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "status": "complete" if fully_covered else "incomplete",
        "execution_policy": "parquet_metadata_audit_no_account_wiring",
        "account_wired": False,
        "cache_dir": str(args.cache_dir.resolve()),
        "s5_dir": str(args.s5_dir.resolve()),
        "api_summary": str(args.api_summary.resolve()) if args.api_summary else "",
        "summary": {
            "expected_pairs": len(expected),
            "observed_pairs": len(rows),
            "missing_pairs": missing_pairs,
            "schema_failures": schema_failures,
            "rows": sum(row["rows"] for row in rows),
            "bytes": sum(row["bytes"] for row in rows),
            "fully_covered": fully_covered,
        },
        "instruments": rows,
    }
    write_json_atomic(args.output, report)
    print(json.dumps(report["summary"], indent=2), flush=True)
    return 0 if fully_covered else 1


if __name__ == "__main__":
    raise SystemExit(main())
