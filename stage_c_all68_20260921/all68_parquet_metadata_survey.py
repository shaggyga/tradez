"""Read only Parquet footer metadata for the bound long-history ZIP."""
from __future__ import annotations

import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
OUTPUT = ROOT / "ALL68_PARQUET_METADATA_SURVEY.json"
REQUIRED_COLUMNS = {"time", "datetime", "open", "high", "low", "close", "volume", "bid_close", "ask_close", "spread_pips"}


def stat_value(statistics, attribute: str):
    value = getattr(statistics, attribute, None) if statistics else None
    return value.isoformat() if hasattr(value, "isoformat") else value


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    declared = {item["path"]: item for item in long["members"]}
    rows = []
    with zipfile.ZipFile(ARCHIVE) as archive:
        for name in sorted(declared):
            with archive.open(name) as handle:
                parquet = pq.ParquetFile(pa.PythonFile(handle, mode="r"))
                names = parquet.schema.names
                missing = sorted(REQUIRED_COLUMNS - set(names))
                if missing:
                    raise ValueError(f"required_columns_missing:{name}:{','.join(missing)}")
                datetime_column = names.index("datetime")
                stats = parquet.metadata.row_group(0).column(datetime_column).statistics
                rows.append({
                    "instrument": declared[name]["instrument"],
                    "member": name,
                    "rows": parquet.metadata.num_rows,
                    "row_groups": parquet.metadata.num_row_groups,
                    "datetime_min": stat_value(stats, "min"),
                    "datetime_max": stat_value(stats, "max"),
                    "columns": names,
                })
    payload = {
        "schema": "all68_long_parquet_footer_survey_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed_metadata_only",
        "archive": str(ARCHIVE),
        "archive_declared_sha256": long["sha256"],
        "instrument_count": len(rows),
        "total_rows": sum(item["rows"] for item in rows),
        "all_have_required_columns": True,
        "members": rows,
        "not_performed": ["parquet_row_read", "data_extraction", "model_fit", "forecast", "network"],
    }
    temp = OUTPUT.with_suffix(OUTPUT.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, OUTPUT)
    print(f"metadata survey passed: {len(rows)} instruments, {payload['total_rows']} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
