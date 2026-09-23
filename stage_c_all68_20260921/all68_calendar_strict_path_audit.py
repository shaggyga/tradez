"""All-68 strict M1-path audit for endpoint-available UTC calendar targets."""
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
QUERY_END = START + timedelta(days=16)
TRADING_DAYS = (1, 2, 5)
OUTPUT = ROOT / "ALL68_CALENDAR_STRICT_PATH_AUDIT.json"


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    totals, per_instrument = Counter(), []
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in sorted(long["members"], key=lambda item: item["instrument"]):
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close"])
            selected = table.filter(pc.and_(pc.greater_equal(table["datetime"], pa.scalar(START.isoformat())), pc.less(table["datetime"], pa.scalar(QUERY_END.isoformat()))))
            time = np.array([int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()], dtype=np.int64)
            data = {"time": time, "close": np.asarray(selected["close"].to_pylist(), dtype=float)}
            decision = time + 60
            origin = (time >= int(START.timestamp())) & (time < int(ORIGIN_END.timestamp())) & ((time % 3600) == 0)
            known = set(int(item) for item in time)
            counts = {}
            for days in TRADING_DAYS:
                outcome = endpoint_for_calendar_targets(data, decision, days)
                state = np.full(len(time), "not_origin", dtype=object)
                for index in np.flatnonzero(origin):
                    if outcome["state"][index] != "endpoint_available":
                        state[index] = str(outcome["state"][index])
                        continue
                    target_raw = int(outcome["target_decision_time"][index]) - 60
                    state[index] = "strict_calendar_minute_path_available" if all(stamp in known for stamp in range(int(time[index]), target_raw + 1, 60)) else "missing_calendar_minutes"
                values = Counter(str(item) for item in state[origin])
                counts[str(days)] = dict(sorted(values.items()))
                totals.update({f"{days}:{key}": count for key, count in values.items()})
            per_instrument.append({"instrument": member["instrument"], "hourly_origin_rows": int(origin.sum()), "states_by_trading_days": counts})
            del table, selected
    payload = {"schema": "all68_calendar_strict_path_audit_v2", "status": "completed_read_only_exact_calendar_minute_audit", "target_contract": "UTC daily close; weekday trading-day count; exact calendar-minute presence from origin through target endpoint; weekend/session closure classification is unresolved", "instrument_count": len(per_instrument), "window": {"origins_start_utc": START.isoformat(), "origins_end_utc": ORIGIN_END.isoformat(), "query_end_utc": QUERY_END.isoformat(), "origin_cadence_seconds": 3600}, "aggregate_state_counts": dict(sorted(totals.items())), "per_instrument": per_instrument, "source_archive_sha256": long["sha256"]}
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print(f"calendar strict-path audit complete: {len(per_instrument)} pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
