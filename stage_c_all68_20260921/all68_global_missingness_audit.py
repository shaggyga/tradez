"""Classify common versus pair-specific missing M1 timestamps in a bounded window."""
from __future__ import annotations

import json
import os
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
START = datetime(2024, 7, 1, tzinfo=timezone.utc)
END = START + timedelta(days=16)
OUTPUT = ROOT / "ALL68_GLOBAL_MISSINGNESS_AUDIT.json"


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    present, per_instrument = Counter(), []
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in long["members"]:
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime"])
            selected = table.filter(pc.and_(pc.greater_equal(table["datetime"], pa.scalar(START.isoformat())), pc.less(table["datetime"], pa.scalar(END.isoformat()))))
            stamps = {int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()}
            for stamp in stamps:
                present[stamp] += 1
            per_instrument.append({"instrument": member["instrument"], "present_minutes": len(stamps)})
    expected = range(int(START.timestamp()), int(END.timestamp()), 60)
    counts = Counter()
    for stamp in expected:
        count = present[stamp]
        counts["all_pairs_present" if count == 68 else "all_pairs_absent_closure_candidate" if count == 0 else "partial_pair_presence"] += 1
    expected_minutes = len(list(expected))
    for row in per_instrument:
        row["missing_minutes"] = expected_minutes - row["present_minutes"]
        row["presence_rate"] = row["present_minutes"] / expected_minutes
    payload = {"schema": "all68_global_missingness_audit_v2", "status": "completed_read_only_classification", "window": {"start_utc": START.isoformat(), "end_utc": END.isoformat(), "expected_minutes": expected_minutes}, "instrument_count": 68, "minute_counts": dict(sorted(counts.items())), "per_instrument": sorted(per_instrument, key=lambda row: (row["presence_rate"], row["instrument"])), "interpretation": "all-pairs-absent minutes are closure candidates only; no broker/session calendar has been authenticated", "source_archive_sha256": long["sha256"]}
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, OUTPUT)
    print("global missingness audit complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
