"""Read-only all-68 support audit for UTC daily-close trading-day endpoint targets."""
from __future__ import annotations

import json
import os
import sys
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from calendar_targets import endpoint_for_calendar_targets

CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
START = datetime(2024, 7, 1, tzinfo=timezone.utc)
ORIGIN_END = START + timedelta(days=7)
QUERY_END = START + timedelta(days=16)  # includes five weekday closes plus readiness
TRADING_DAYS = (1, 2, 5)
OUTPUT = ROOT / "ALL68_CALENDAR_TARGET_COVERAGE.json"
PROGRESS = ROOT / "ALL68_CALENDAR_TARGET_COVERAGE.progress.json"


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    prior = json.loads(PROGRESS.read_text(encoding="utf-8")) if PROGRESS.exists() else {"per_instrument": []}
    per_instrument = prior["per_instrument"]
    completed = {row["instrument"] for row in per_instrument}
    totals = Counter()
    for row in per_instrument:
        for days, values in row["states_by_trading_days"].items():
            totals.update({f"{days}:{key}": count for key, count in values.items()})
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in sorted(long["members"], key=lambda item: item["instrument"]):
            if member["instrument"] in completed:
                continue
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close"])
            selected = table.filter(pc.and_(pc.greater_equal(table["datetime"], pa.scalar(START.isoformat())), pc.less(table["datetime"], pa.scalar(QUERY_END.isoformat()))))
            data = {"time": np.array([int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()], dtype=np.int64), "close": np.asarray(selected["close"].to_pylist(), dtype=float)}
            decision = data["time"] + 60
            origin = (data["time"] >= int(START.timestamp())) & (data["time"] < int(ORIGIN_END.timestamp())) & ((data["time"] % 3600) == 0)
            counts = {}
            for days in TRADING_DAYS:
                outcome = endpoint_for_calendar_targets(data, decision, days)
                value = Counter(str(item) for item in outcome["state"][origin])
                counts[str(days)] = dict(sorted(value.items()))
                totals.update({f"{days}:{key}": count for key, count in value.items()})
            per_instrument.append({"instrument": member["instrument"], "hourly_origin_rows": int(origin.sum()), "states_by_trading_days": counts})
            _write_json(PROGRESS, {"schema": "all68_utc_calendar_endpoint_target_coverage_progress_v1", "status": "running", "per_instrument": per_instrument, "completed_instruments": len(per_instrument), "source_archive_sha256": long["sha256"]})
            print(f"checkpointed {len(per_instrument)}/68: {member['instrument']}", flush=True)
            del table, selected
    payload = {"schema": "all68_utc_calendar_endpoint_target_coverage_v1", "status": "completed_endpoint_proxy_coverage_not_execution_claim", "target_contract": "UTC daily close; weekday trading-day counting; endpoint-only midpoint outcome", "instrument_count": len(per_instrument), "trading_days": TRADING_DAYS, "window": {"origins_start_utc": START.isoformat(), "origins_end_utc": ORIGIN_END.isoformat(), "query_end_utc": QUERY_END.isoformat(), "origin_cadence_seconds": 3600}, "aggregate_state_counts": dict(sorted(totals.items())), "per_instrument": per_instrument, "source_archive_sha256": long["sha256"]}
    _write_json(OUTPUT, payload)
    PROGRESS.unlink(missing_ok=True)
    print(f"calendar target coverage complete: {len(per_instrument)} pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
