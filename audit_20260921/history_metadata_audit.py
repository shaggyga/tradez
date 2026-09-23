"""Inspect only C-drive Parquet footers; no price-column loading or model use."""
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import pyarrow.parquet as pq

OUT = Path(__file__).resolve().parent
ROOT = Path(r"C:\Users\zmoor\AppData\Local\ForexResearchData")
SOURCE = ROOT / "m1_reacquired_20260725"
summary_path = ROOT / "m1_reacquired_20260725_summary.json"
summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))
declared = {r["instrument"]:r for r in summary["records"]}
records = []
for path in sorted(SOURCE.glob("*_M1.parquet")):
    before = path.stat()
    instrument = path.stem.removesuffix("_M1")
    row = {"instrument":instrument,"base":instrument.split("_")[0],"quote":instrument.split("_")[1],
        "path":str(path),"bytes":before.st_size,"modified_utc":datetime.fromtimestamp(before.st_mtime, timezone.utc).isoformat(),
        "full_file_sha256_status":"not computed in bounded metadata audit", "granularity_declared":"M1",
        "row_scan_status":"not performed; gaps, duplicates, quote consistency and vintage not verified"}
    try:
        with pq.ParquetFile(path) as parquet:
            md = parquet.metadata
            names = parquet.schema.names
            row.update(rows=md.num_rows,row_groups=md.num_row_groups,columns=names,
                bid_ask_columns_present=all(x in names for x in ["bid_open","bid_close","ask_open","ask_close"]),
                spread_column_present="spread_pips" in names,volume_column_present="volume" in names)
            ix = names.index("time")
            stats = [md.row_group(i).column(ix).statistics for i in range(md.num_row_groups)]
            supported = all(x is not None and x.has_min_max for x in stats)
            row["timestamp_bounds_source"] = "stored Parquet footer statistics, not a fresh row scan"
            row["first_time_footer"] = min(s.min for s in stats) if supported else None
            row["last_time_footer"] = max(s.max for s in stats) if supported else None
            row["time_null_count_footer"] = sum(s.null_count for s in stats) if all(s and s.has_null_count for s in stats) else None
            ref = declared.get(instrument,{})
            row["declared_source"] = ref.get("source")
            row["summary_rows_match"] = md.num_rows == ref.get("rows")
            row["summary_bytes_match"] = before.st_size == ref.get("bytes")
            row["summary_first_time"] = ref.get("start_utc")
            row["summary_last_time"] = ref.get("end_utc")
            def iso_time(value):
                return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
            row["summary_time_bounds_match"] = bool(supported and iso_time(row["first_time_footer"]) == iso_time(ref.get("start_utc")) and iso_time(row["last_time_footer"]) == iso_time(ref.get("end_utc")))
            row["timestamp_semantics"] = "bar-start availability must be verified from original collector contract; footer cannot prove original arrival time"
            row["status"] = "metadata_readable"
    except Exception as exc:
        row.update(status="metadata_error",error=f"{type(exc).__name__}: {exc}")
    after = path.stat()
    row["changed_during_read"] = (before.st_size,before.st_mtime_ns) != (after.st_size,after.st_mtime_ns)
    records.append(row)

result={"observed_utc":datetime.now(timezone.utc).isoformat(),"scope":"68 expected reacquired C-drive M1 histories; file metadata and stored footers only",
    "summary_path":str(summary_path),"summary_sha256":hashlib.sha256(summary_path.read_bytes()).hexdigest(),
    "declared_summary":summary["summary"],"actual_files":len(records),
    "readable_footers":sum(r["status"]=="metadata_readable" for r in records),
    "total_footer_rows":sum(r.get("rows",0) for r in records),"total_bytes":sum(r["bytes"] for r in records),
    "currencies":sorted({r[c] for r in records for c in ("base","quote")}),
    "missing_declared_instruments":sorted(set(declared)-{r["instrument"] for r in records}),
    "record_mismatches":[r["instrument"] for r in records if not all(r.get(k) for k in ("summary_rows_match","summary_bytes_match","summary_time_bounds_match"))],
    "records":records}
source_inventory = OUT / "SOURCE_REUSE_INVENTORY.json"
if source_inventory.exists():
    configured = set(json.loads(source_inventory.read_text(encoding="utf-8"))["registered_pairs"])
    located = {r["instrument"] for r in records}
    result["current_config_pair_reconciliation"] = {"configured_pairs":len(configured),
        "history_pairs":len(located), "configured_missing_histories":sorted(configured-located),
        "history_not_configured":sorted(located-configured), "matched":configured==located}
(OUT/"SOURCE_HISTORY_METADATA.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
fields=["instrument","base","quote","path","bytes","rows","first_time_footer","last_time_footer","bid_ask_columns_present","spread_column_present","volume_column_present","summary_rows_match","summary_bytes_match","summary_time_bounds_match","timestamp_bounds_source","full_file_sha256_status","row_scan_status","status"]
with (OUT/"SOURCE_HISTORY_COVERAGE.csv").open("w",newline="",encoding="utf-8-sig") as f:
    writer=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
print(json.dumps({k:v for k,v in result.items() if k!="records"},indent=2))
