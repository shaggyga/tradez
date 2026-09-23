"""Resumable causal pooled baseline for daily-close and trading-day endpoint labels."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sklearn.ensemble import HistGradientBoostingRegressor

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from all68_endpoint_baseline import _features, _fit_predict, _score
from calendar_targets import endpoint_for_calendar_targets

CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
RUN_ID = os.environ.get("FOREX_CALENDAR_RUN_ID", "calendar_baseline_v1")
MODEL_KIND = os.environ.get("FOREX_CALENDAR_MODEL", "ridge")
def _utc_setting(name: str, default: str) -> datetime:
    return datetime.fromisoformat(os.environ.get(name, default)).astimezone(timezone.utc)

TRAIN_START = _utc_setting("FOREX_CALENDAR_TRAIN_START", "2024-07-01T00:00:00+00:00")
TRAIN_END = _utc_setting("FOREX_CALENDAR_TRAIN_END", "2024-07-22T00:00:00+00:00")
TEST_END = _utc_setting("FOREX_CALENDAR_TEST_END", "2024-07-29T00:00:00+00:00")
QUERY_START = TRAIN_START - timedelta(days=1)
QUERY_END = TEST_END + timedelta(days=10)
TRADING_DAYS = (1, 2, 5)
PARTS = ROOT / f"{RUN_ID}_parts"
REPORT = ROOT / ("ALL68_CALENDAR_BASELINE_REPORT.json" if RUN_ID == "calendar_baseline_v1" else f"ALL68_CALENDAR_BASELINE_{RUN_ID}_REPORT.json")
TAPE = ROOT / ("ALL68_CALENDAR_BASELINE_FORECAST_TAPE.jsonl.gz" if RUN_ID == "calendar_baseline_v1" else f"ALL68_CALENDAR_BASELINE_{RUN_ID}_FORECAST_TAPE.jsonl.gz")


def _save_part(path: Path, values: dict[str, np.ndarray]) -> None:
    temporary = path.with_name(path.stem + f".{os.getpid()}.tmp.npz")
    np.savez_compressed(temporary, **values)
    os.replace(temporary, path)


def _predict(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray) -> tuple[str, np.ndarray]:
    if MODEL_KIND == "ridge":
        return "regularized_pooled", _fit_predict(train_x, train_y, test_x)
    if MODEL_KIND == "hist_gradient_boosting":
        model = HistGradientBoostingRegressor(
            max_iter=80, max_leaf_nodes=15, min_samples_leaf=40,
            learning_rate=0.06, l2_regularization=1.0, random_state=20260921,
        )
        model.fit(train_x, train_y)
        return "hist_gradient_boosting", model.predict(test_x)
    raise ValueError(f"unsupported model kind: {MODEL_KIND}")


def _extract_member(archive: zipfile.ZipFile, member: dict) -> None:
    target = PARTS / f"{member['instrument']}.npz"
    if target.exists():
        return
    with archive.open(member["path"]) as handle:
        table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close", "bid_close", "ask_close"])
    selected = table.filter(pc.and_(pc.greater_equal(table["datetime"], pa.scalar(QUERY_START.isoformat())), pc.less(table["datetime"], pa.scalar(QUERY_END.isoformat()))))
    data = {name: np.asarray(selected[name].to_pylist(), dtype=float) for name in ("close", "bid_close", "ask_close")}
    data["time"] = np.array([int(datetime.fromisoformat(item).timestamp()) for item in selected["datetime"].to_pylist()], dtype=np.int64)
    features, decision = _features(data)
    cadence = (data["time"] % 3600) == 0
    values: dict[str, np.ndarray] = {}
    for days in TRADING_DAYS:
        outcome = endpoint_for_calendar_targets(data, decision, days)
        valid = cadence & np.all(np.isfinite(features), axis=1) & (outcome["state"] == "endpoint_available")
        # A train label is accepted only if its own target was ready before the
        # fixed fit cutoff, even though the source extraction sees later bars.
        train = valid & (data["time"] >= int(TRAIN_START.timestamp())) & (data["time"] < int(TRAIN_END.timestamp())) & (outcome["target_decision_time"] <= int(TRAIN_END.timestamp()))
        test = valid & (data["time"] >= int(TRAIN_END.timestamp())) & (data["time"] < int(TEST_END.timestamp()))
        prefix = f"d{days}_"
        values[prefix + "train_x"] = features[train]
        values[prefix + "train_y"] = outcome["midpoint_return_bps"][train]
        values[prefix + "test_x"] = features[test]
        values[prefix + "test_y"] = outcome["midpoint_return_bps"][test]
        values[prefix + "test_time"] = decision[test]
    _save_part(target, values)
    print(f"checkpointed {member['instrument']}", flush=True)


def main() -> int:
    PARTS.mkdir(exist_ok=True)
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    members = sorted(long["members"], key=lambda item: item["instrument"])
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in members:
            _extract_member(archive, member)
    completed_parts = [path for path in PARTS.glob("*.npz") if ".tmp." not in path.name]
    if len(completed_parts) != len(members):
        raise RuntimeError("incomplete part set after extraction")
    results, tape_rows = {}, []
    for days in TRADING_DAYS:
        prefix = f"d{days}_"
        loaded = [(member["instrument"], np.load(PARTS / f"{member['instrument']}.npz")) for member in members]
        train_x = np.vstack([item[prefix + "train_x"] for _, item in loaded])
        train_y = np.concatenate([item[prefix + "train_y"] for _, item in loaded])
        test_x = np.vstack([item[prefix + "test_x"] for _, item in loaded])
        test_y = np.concatenate([item[prefix + "test_y"] for _, item in loaded])
        model_name, predicted = _predict(train_x, train_y, test_x)
        results[str(days)] = {model_name: _score(test_y, predicted), "no_change": _score(test_y, np.zeros_like(test_y)), "training_rows": int(len(train_y)), "test_rows": int(len(test_y))}
        cursor = 0
        for instrument, item in loaded:
            count = len(item[prefix + "test_y"])
            for stamp, actual, forecast in zip(item[prefix + "test_time"], item[prefix + "test_y"], predicted[cursor:cursor + count], strict=True):
                tape_rows.append({"instrument": instrument, "decision_time_utc": datetime.fromtimestamp(int(stamp), tz=timezone.utc).isoformat(), "trading_days": days, "target_family": "utc_daily_close_endpoint_midpoint_return", "forecast_bps": float(forecast), "actual_bps": float(actual), "forecast_status": "offline_research_only", "execution_status": "not_evaluated"})
            cursor += count
    tape_rows.sort(key=lambda row: (row["decision_time_utc"], row["instrument"], row["trading_days"]))
    with gzip.open(TAPE, "wt", encoding="utf-8") as handle:
        for row in tape_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    payload = {"schema": "all68_calendar_baseline_v1", "run_id": RUN_ID, "model_kind": MODEL_KIND, "status": "completed_offline_forecast_skill_only", "target_contract": "UTC daily close; weekday trading-day count; endpoint-only midpoint return; no path or execution claim", "clock_contract": "raw bars treated as interval starts; decision_time = raw timestamp + 60 seconds; train labels mature by fit cutoff", "features": ["return_60m_bps", "return_240m_bps", "return_1440m_bps", "quoted_spread_bps", "utc_time_sine"], "models": ["no_change", MODEL_KIND], "train_window_utc": [TRAIN_START.isoformat(), TRAIN_END.isoformat()], "test_window_utc": [TRAIN_END.isoformat(), TEST_END.isoformat()], "trading_days": TRADING_DAYS, "instrument_count": len(members), "results": results, "forecast_tape": {"path": str(TAPE), "records": len(tape_rows), "sha256": hashlib.sha256(TAPE.read_bytes()).hexdigest()}, "source_archive_sha256": long["sha256"]}
    temporary = REPORT.with_suffix(REPORT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, REPORT)
    print(f"calendar baseline complete: {len(tape_rows)} forecasts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
