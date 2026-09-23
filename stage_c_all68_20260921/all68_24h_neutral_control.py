"""Create a bounded policy-independent neutral-control tape from long history.

This is a causality/accounting engineering artifact. It creates no model,
signal, order, or P&L claim. Raw history is read serially and remains unchanged.
"""
from __future__ import annotations

import gzip
import hashlib
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
START = datetime(2024, 6, 24, tzinfo=timezone.utc)
END = START + timedelta(hours=24)
HORIZONS = (300, 900, 1800, 3600, 7200, 14400, 28800, 86400)
READ_COLUMNS = ["datetime", "close"]
TAPE = ROOT / "ALL68_24H_NEUTRAL_FORECAST_TAPE.jsonl.gz"
LEDGER = ROOT / "ALL68_24H_NEUTRAL_POLICY_LEDGER.jsonl.gz"
RECEIPT = ROOT / "ALL68_24H_NEUTRAL_CONTROL_RECEIPT.json"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write_line(handle, payload: dict) -> None:
    handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")


def load_support(members: list[dict]) -> tuple:
    events: list[BarEvent] = []
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in members:
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=READ_COLUMNS)
            stamp = table["datetime"]
            mask = pc.and_(pc.greater_equal(stamp, pa.scalar(START.isoformat())), pc.less(stamp, pa.scalar(END.isoformat())))
            selected = table.filter(mask)
            events.extend(
                # Raw history receipt timing is unknown.  For this offline
                # reconstruction, its minute stamp is treated as bar start and
                # the decision clock is the completed-bar end one minute later.
                BarEvent(int(datetime.fromisoformat(value).timestamp()) + 60, member["instrument"], float(close))
                for value, close in zip(selected["datetime"].to_pylist(), selected["close"].to_pylist())
            )
            del table, selected
    return global_minute_support(ordered_events(events), [item["instrument"] for item in members])


def main() -> int:
    began = time.perf_counter()
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    members = sorted(long["members"], key=lambda item: item["instrument"])
    support = load_support(members)
    forecast_count = ledger_count = eligible = blocked = 0
    tape_tmp, ledger_tmp = TAPE.with_suffix(TAPE.suffix + f".{os.getpid()}.tmp"), LEDGER.with_suffix(LEDGER.suffix + f".{os.getpid()}.tmp")
    with gzip.open(tape_tmp, "wt", encoding="utf-8", newline="\n") as tape, gzip.open(ledger_tmp, "wt", encoding="utf-8", newline="\n") as ledger:
        for bar_end_epoch, minute in support:
            origin_epoch = bar_end_epoch - 60
            for instrument, state in sorted(minute.items()):
                for horizon in HORIZONS:
                    forecast_id = f"{instrument}:{origin_epoch}:{horizon}:no_change_v1"
                    forecast = {
                        "forecast_id": forecast_id, "instrument": instrument,
                        "origin_epoch": origin_epoch, "horizon_sec": horizon,
                        "target_epoch": origin_epoch + horizon,
                        "feature_cutoff_epoch": origin_epoch,
                        "training_labels_available_max_epoch": 0,
                        "fit_completed_epoch": 0,
                        "available_epoch": bar_end_epoch,
                        "model_id": "no_change_control_v1", "direction": 0,
                        "status": "eligible" if state == "supported" else "blocked",
                        "reason": "" if state == "supported" else "missing_support",
                    }
                    write_line(tape, forecast)
                    forecast_count += 1
                    eligible += forecast["status"] == "eligible"
                    blocked += forecast["status"] == "blocked"
                    for arm, capital in (("fixed_reference", 1000.0), ("recovered_rotation", 1000.0)):
                        write_line(ledger, {"arm": arm, "forecast_id": forecast_id, "action": "no_trade", "capital_before": capital, "capital_after": capital, "reason": forecast["reason"] or "neutral_control"})
                        ledger_count += 1
    os.replace(tape_tmp, TAPE)
    os.replace(ledger_tmp, LEDGER)
    receipt = {
        "schema": "all68_neutral_control_24h_v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "completed_engineering_control_not_predictive_result",
        "window": {"start_utc": START.isoformat(), "end_utc": END.isoformat(), "history_vintage": "long_m1_68_only"},
        "source": {"archive": str(ARCHIVE), "declared_sha256": long["sha256"], "instrument_count": len(members)},
        "horizons_sec": HORIZONS,
        "global_minutes_observed": len(support),
        "forecast_count": forecast_count, "eligible_forecast_count": eligible, "blocked_forecast_count": blocked,
        "ledger_count": ledger_count, "policy_arms": ["fixed_reference", "recovered_rotation"],
        "tape": {"path": str(TAPE), "sha256": digest(TAPE)},
        "ledger": {"path": str(LEDGER), "sha256": digest(LEDGER)},
        "wall_seconds": round(time.perf_counter() - began, 3),
        "not_performed": ["feature_or_model_fit", "directional_signal", "trade_or_order", "pnl_or_profitability_claim", "network"],
    }
    temp = RECEIPT.with_suffix(RECEIPT.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, RECEIPT)
    print(f"neutral control complete: {forecast_count} forecasts, {ledger_count} ledger rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
