"""Settle the neutral-control tape with read-only retrospective label proxies."""
from __future__ import annotations

import gzip
import hashlib
import importlib.util
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
from all68_global_clock import BarEvent, global_minute_support, ordered_events

CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
LABEL_SOURCE = Path(r"C:\ForexRestore_20260921\trad\oanda_rolling_technical_labels_v1.py")
START = datetime(2024, 6, 24, tzinfo=timezone.utc)
FORECAST_END = START + timedelta(hours=24)
QUERY_END = FORECAST_END + timedelta(hours=24)
HORIZONS = (5, 15, 30, 60, 120, 240, 480, 1440)
SETTLEMENT = ROOT / "ALL68_24H_NEUTRAL_OUTCOME_SETTLEMENT.jsonl.gz"
RECEIPT = ROOT / "ALL68_24H_NEUTRAL_SETTLEMENT_RECEIPT.json"


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def load_label_module():
    spec = importlib.util.spec_from_file_location("isolated_rolling_labels", LABEL_SOURCE)
    if spec is None or spec.loader is None:
        raise ValueError("label_module_load_failed")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def clean(value):
    if isinstance(value, np.generic):
        value = value.item()
    return None if isinstance(value, float) and not np.isfinite(value) else value


def main() -> int:
    labels = load_label_module()
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    members = sorted(long["members"], key=lambda item: item["instrument"])
    support_events: list[BarEvent] = []
    labels_by_pair: dict[str, tuple[np.ndarray, dict]] = {}
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in members:
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close", "bid_close", "ask_close"])
            stamp = table["datetime"]
            query_mask = pc.and_(pc.greater_equal(stamp, pa.scalar(START.isoformat())), pc.less(stamp, pa.scalar(QUERY_END.isoformat())))
            selected = table.filter(query_mask)
            times = np.array([int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()], dtype=np.int64)
            data = {"time": times, "close": np.asarray(selected["close"].to_pylist()), "bid_close": np.asarray(selected["bid_close"].to_pylist()), "ask_close": np.asarray(selected["ask_close"].to_pylist())}
            outcome = labels.compute_outcomes(data, HORIZONS, coverage_end_epoch=int(QUERY_END.timestamp()))
            labels_by_pair[member["instrument"]] = (times, outcome)
            origin_mask = (times >= int(START.timestamp())) & (times < int(FORECAST_END.timestamp()))
            support_events.extend(BarEvent(int(epoch) + 60, member["instrument"], float(close)) for epoch, close in zip(times[origin_mask], data["close"][origin_mask]))
            del table, selected
    support = global_minute_support(ordered_events(support_events), [item["instrument"] for item in members])
    positions = {pair: {int(epoch): idx for idx, epoch in enumerate(times)} for pair, (times, _) in labels_by_pair.items()}
    counts = Counter()
    total = 0
    temporary = SETTLEMENT.with_suffix(SETTLEMENT.suffix + f".{os.getpid()}.tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", newline="\n") as handle:
        for bar_end_epoch, minute in support:
            origin_epoch = bar_end_epoch - 60
            for pair, support_state in sorted(minute.items()):
                times, outcome = labels_by_pair[pair]
                index = positions[pair].get(origin_epoch)
                for horizon in HORIZONS:
                    forecast_id = f"{pair}:{origin_epoch}:{horizon * 60}:no_change_v1"
                    if support_state != "supported" or index is None:
                        row = {"forecast_id": forecast_id, "outcome_state": "missing_support", "reason": "missing_origin_support"}
                    else:
                        prefix = f"label__{horizon}m__"
                        row = {
                            "forecast_id": forecast_id,
                            "outcome_state": str(outcome[prefix + "state"][index]),
                            "target_bar_start_epoch": clean(outcome[prefix + "target_bar_start_epoch"][index]),
                            "assumed_available_epoch": clean(outcome[prefix + "assumed_available_epoch"][index]),
                            "midpoint_return_bps": clean(outcome[prefix + "return_bps"][index]),
                            "long_endpoint_net_bps": clean(outcome[prefix + "long_net_bps"][index]),
                            "short_endpoint_net_bps": clean(outcome[prefix + "short_net_bps"][index]),
                            "availability_semantics": "retrospective_target_bar_end_assumption_not_receipt_proof",
                        }
                    counts[row["outcome_state"]] += 1
                    handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
                    total += 1
    os.replace(temporary, SETTLEMENT)
    receipt = {
        "schema": "all68_neutral_control_retrospective_settlement_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "completed_label_proxy_not_execution_or_prediction_result",
        "window": {"forecast_start_utc": START.isoformat(), "forecast_end_utc": FORECAST_END.isoformat(), "query_end_utc": QUERY_END.isoformat()},
        "source": {"long_archive_declared_sha256": long["sha256"], "label_source": str(LABEL_SOURCE), "label_source_sha256": sha256(LABEL_SOURCE)},
        "horizons_minutes": HORIZONS,
        "settlement_records": total,
        "outcome_state_counts": dict(sorted(counts.items())),
        "settlement": {"path": str(SETTLEMENT), "sha256": sha256(SETTLEMENT)},
        "not_performed": ["feature_or_model_fit", "forecast_signal", "order_fill_or_execution", "currency_conversion_or_financing", "profitability_claim", "network"],
    }
    temp = RECEIPT.with_suffix(RECEIPT.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, RECEIPT)
    print(f"settlement complete: {total} records; {dict(sorted(counts.items()))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
