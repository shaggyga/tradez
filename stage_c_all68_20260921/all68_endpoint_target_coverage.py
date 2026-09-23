"""Bounded all-68 coverage audit for endpoint-only elapsed targets."""
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
from endpoint_targets import endpoint_outcomes

CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
START = datetime(2024, 6, 24, tzinfo=timezone.utc)
FORECAST_END = START + timedelta(hours=24)
QUERY_END = START + timedelta(hours=96)
HORIZONS = (1440, 2880, 4320)
OUTPUT = ROOT / "ALL68_ENDPOINT_TARGET_COVERAGE.json"


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    totals, rows = Counter(), []
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in sorted(long["members"], key=lambda item: item["instrument"]):
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close", "bid_close", "ask_close"])
            selected = table.filter(pc.and_(pc.greater_equal(table["datetime"], pa.scalar(START.isoformat())), pc.less(table["datetime"], pa.scalar(QUERY_END.isoformat()))))
            data = {name: np.asarray(selected[name].to_pylist()) for name in ("close", "bid_close", "ask_close")}
            data["time"] = np.array([int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()], dtype=np.int64)
            origin = (data["time"] >= int(START.timestamp())) & (data["time"] < int(FORECAST_END.timestamp()))
            horizon_counts = {}
            for horizon in HORIZONS:
                values = endpoint_outcomes(data, horizon)
                counts = Counter(str(value) for value in values["state"][origin])
                horizon_counts[str(horizon)] = dict(sorted(counts.items()))
                totals.update({f"{horizon}:{key}": value for key, value in counts.items()})
            rows.append({"instrument": member["instrument"], "origin_rows": int(origin.sum()), "states_by_horizon_minutes": horizon_counts})
            del table, selected
    payload = {"schema": "all68_endpoint_elapsed_target_coverage_v1", "status": "completed_endpoint_proxy_coverage_not_execution_claim", "instrument_count": len(rows), "window": {"origins_start_utc": START.isoformat(), "origins_end_utc": FORECAST_END.isoformat(), "query_end_utc": QUERY_END.isoformat()}, "horizons_minutes": HORIZONS, "target_semantics": "endpoint_only_elapsed_minutes; incomplete paths remain separately blocked by strict target audit", "aggregate_state_counts": dict(sorted(totals.items())), "per_instrument": rows, "source_archive_sha256": long["sha256"]}
    temp = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, OUTPUT)
    print(f"endpoint coverage complete: {len(rows)} pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
