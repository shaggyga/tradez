"""Read-only coverage audit for 24/48/72-hour elapsed targets across all 68 pairs."""
from __future__ import annotations

import importlib.util
import json
import os
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
LABEL_SOURCE = Path(r"C:\ForexRestore_20260921\trad\oanda_rolling_technical_labels_v1.py")
START = datetime(2024, 6, 24, tzinfo=timezone.utc)
FORECAST_END = START + timedelta(hours=24)
QUERY_END = START + timedelta(hours=96)
HORIZONS = (1440, 2880, 4320)
OUTPUT = ROOT / "ALL68_EXTENDED_HORIZON_COVERAGE.json"


def labels_module():
    spec = importlib.util.spec_from_file_location("extended_labels", LABEL_SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    labels = labels_module()
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    archive_manifest = next(value for value in manifest["archives"] if value["path"] == "inputs/long_m1_68.zip")
    rows, total = [], Counter()
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in sorted(archive_manifest["members"], key=lambda item: item["instrument"]):
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close", "bid_close", "ask_close"])
            stamp = table["datetime"]
            selected = table.filter(pc.and_(pc.greater_equal(stamp, pa.scalar(START.isoformat())), pc.less(stamp, pa.scalar(QUERY_END.isoformat()))))
            times = np.array([int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()], dtype=np.int64)
            data = {name: np.asarray(selected[name].to_pylist()) for name in ("close", "bid_close", "ask_close")}
            data["time"] = times
            outcome = labels.compute_outcomes(data, HORIZONS, coverage_end_epoch=int(QUERY_END.timestamp()))
            origin = (times >= int(START.timestamp())) & (times < int(FORECAST_END.timestamp()))
            horizon_counts = {}
            for horizon in HORIZONS:
                counts = Counter(str(item) for item in outcome[f"label__{horizon}m__state"][origin])
                horizon_counts[str(horizon)] = dict(sorted(counts.items()))
                total.update({f"{horizon}:{key}": value for key, value in counts.items()})
            rows.append({"instrument": member["instrument"], "origin_rows": int(origin.sum()), "states_by_horizon_minutes": horizon_counts})
            del table, selected
    payload = {
        "schema": "all68_extended_elapsed_horizon_coverage_v1",
        "status": "completed_read_only_coverage_not_forecast_evidence",
        "window": {"origins_start_utc": START.isoformat(), "origins_end_utc": FORECAST_END.isoformat(), "query_end_utc": QUERY_END.isoformat()},
        "horizons_minutes": HORIZONS,
        "target_semantics": "elapsed_minutes_only; daily_close_and_trading_day_targets_not_substituted",
        "instrument_count": len(rows), "per_instrument": rows, "aggregate_state_counts": dict(sorted(total.items())),
        "source": {"long_archive_declared_sha256": archive_manifest["sha256"], "label_source": str(LABEL_SOURCE)},
        "blockers": ["daily_close calendar contract not yet bound", "trading-day/session calendar contract not yet bound", "receipt-time availability remains unknown"],
    }
    temp = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, OUTPUT)
    print(f"extended coverage complete: {len(rows)} pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
