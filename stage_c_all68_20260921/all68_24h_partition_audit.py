"""Bounded, read-only 24-hour audit of the long 68-pair history."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from all68_global_clock import BarEvent, global_minute_support, ordered_events
CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
OUTPUT = ROOT / "ALL68_24H_PARTITION_AUDIT.json"
READ_COLUMNS = ["datetime", "close", "bid_close", "ask_close", "spread_pips"]


def utc_parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("explicit_utc_start_required")
    return parsed.astimezone(timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2024-06-24T00:00:00+00:00")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()
    start = utc_parse(args.start)
    if args.hours != 24 or start.minute or start.second or start.microsecond:
        raise ValueError("only_aligned_24_hour_benchmark_is_allowed")
    end = start + timedelta(hours=args.hours)
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    members = sorted(long["members"], key=lambda item: item["instrument"])
    events: list[BarEvent] = []
    rows: list[dict[str, object]] = []
    max_loaded_rows = 0
    began = time.perf_counter()
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in members:
            with archive.open(member["path"]) as handle:
                parquet = pq.ParquetFile(pa.PythonFile(handle, mode="r"))
                table = parquet.read_row_group(0, columns=READ_COLUMNS)
            max_loaded_rows = max(max_loaded_rows, table.num_rows)
            stamp = table["datetime"]
            mask = pc.and_(
                pc.greater_equal(stamp, pa.scalar(start.isoformat())),
                pc.less(stamp, pa.scalar(end.isoformat())),
            )
            selected = table.filter(mask)
            datetimes = selected["datetime"].to_pylist()
            closes = selected["close"].to_pylist()
            instrument = str(member["instrument"])
            events.extend(
                BarEvent(int(datetime.fromisoformat(value).timestamp()), instrument, float(close))
                for value, close in zip(datetimes, closes)
            )
            rows.append({
                "instrument": instrument,
                "rows": selected.num_rows,
                "first_utc": datetimes[0] if datetimes else None,
                "last_utc": datetimes[-1] if datetimes else None,
            })
            del table, selected, datetimes, closes
    ordered = ordered_events(events)
    support = global_minute_support(ordered, [item["instrument"] for item in members])
    missing = {pair: 0 for pair in (item["instrument"] for item in members)}
    for _, minute in support:
        for pair, state in minute.items():
            missing[pair] += state == "missing_support"
    payload = {
        "schema": "all68_long_history_24h_benchmark_audit_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed_read_only_benchmark_not_forecast_or_replay",
        "source": {"archive": str(ARCHIVE), "declared_sha256": long["sha256"], "vintage": "long_m1_68_only"},
        "window": {"start_utc": start.isoformat(), "end_utc": end.isoformat(), "hours": args.hours},
        "resource_contract": {"workers": 1, "columns_per_pair": READ_COLUMNS, "pairs_processed_serially": True, "max_rows_loaded_for_one_pair": max_loaded_rows},
        "wall_seconds": round(time.perf_counter() - began, 3),
        "instrument_count": len(rows),
        "selected_rows": len(ordered),
        "per_instrument": rows,
        "global_minutes_observed": len(support),
        "missing_minutes_by_instrument": missing,
        "not_performed": ["write_or_change_raw_history", "feature_generation", "model_fit", "forecast", "policy_ledger", "network"],
    }
    temp = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, OUTPUT)
    print(f"24h audit passed: {len(ordered)} rows, {len(support)} observed minutes, {payload['wall_seconds']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
