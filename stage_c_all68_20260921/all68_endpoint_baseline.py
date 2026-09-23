"""Causal, offline pooled baseline for endpoint-only midpoint-return targets.

This deliberately evaluates forecast skill only.  It does not create orders, fills,
portfolio P&L, or any claim that a missing intraday path was executable.
"""
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

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from endpoint_targets import endpoint_outcomes

CHECKPOINT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex\RECOVERY_CHECKPOINT_20260921")
ARCHIVE = CHECKPOINT / "inputs" / "long_m1_68.zip"
TRAIN_START = datetime(2024, 7, 1, tzinfo=timezone.utc)
TRAIN_END = TRAIN_START + timedelta(days=21)
TEST_END = TRAIN_END + timedelta(days=7)
HORIZONS = (1440, 2880, 4320)
CADENCE_SECONDS = 3600
LOOKBACK_MINUTES = (60, 240, 1440)
RIDGE_LAMBDA = 20.0
TAPE = ROOT / "ALL68_ENDPOINT_BASELINE_FORECAST_TAPE.jsonl.gz"
REPORT = ROOT / "ALL68_ENDPOINT_BASELINE_REPORT.json"


def _features(data: dict[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    time = data["time"]
    close = data["close"].astype(float)
    bid = data["bid_close"].astype(float)
    ask = data["ask_close"].astype(float)
    lookup = {int(stamp): index for index, stamp in enumerate(time)}
    matrix = np.full((len(time), 5), np.nan, dtype=float)
    for index, stamp in enumerate(time):
        if not np.isfinite(close[index]) or close[index] <= 0:
            continue
        past = [lookup.get(int(stamp) - minute * 60) for minute in LOOKBACK_MINUTES]
        if any(item is None or not np.isfinite(close[item]) or close[item] <= 0 for item in past):
            continue
        matrix[index, :3] = [(close[index] / close[item] - 1.0) * 1e4 for item in past]
        matrix[index, 3] = (ask[index] - bid[index]) / close[index] * 1e4 if np.isfinite(ask[index]) and np.isfinite(bid[index]) else np.nan
        minute_of_day = (int(stamp) // 60) % 1440
        matrix[index, 4] = np.sin(2.0 * np.pi * minute_of_day / 1440.0)
    decision_time = time + 60  # source candle timestamps are reconstructed as interval starts
    return matrix, decision_time


def _fit_predict(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray) -> np.ndarray:
    mean = train_x.mean(axis=0)
    scale = train_x.std(axis=0)
    scale[scale == 0] = 1.0
    x = (train_x - mean) / scale
    design = np.column_stack((np.ones(len(x)), x))
    penalty = np.eye(design.shape[1]) * RIDGE_LAMBDA
    penalty[0, 0] = 0.0
    coef = np.linalg.solve(design.T @ design + penalty, design.T @ train_y)
    return np.column_stack((np.ones(len(test_x)), (test_x - mean) / scale)) @ coef


def _score(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float | int]:
    active = np.sign(predicted) != 0
    directional = np.sign(actual[active]) == np.sign(predicted[active])
    return {"rows": int(len(actual)), "mae_bps": float(np.mean(np.abs(actual - predicted))), "rmse_bps": float(np.sqrt(np.mean((actual - predicted) ** 2))), "directional_accuracy_on_nonzero_forecasts": float(np.mean(directional)) if active.any() else None, "directional_forecast_coverage": float(np.mean(active)), "mean_actual_bps": float(np.mean(actual)), "mean_prediction_bps": float(np.mean(predicted))}


def main() -> int:
    manifest = json.loads((CHECKPOINT / "inputs" / "INPUT_ARCHIVES.json").read_text(encoding="utf-8"))
    long = next(item for item in manifest["archives"] if item["path"] == "inputs/long_m1_68.zip")
    # Labels need the target candle; the extra minute makes readiness explicit.
    query_start = TRAIN_START - timedelta(minutes=max(LOOKBACK_MINUTES))
    query_end = TEST_END + timedelta(minutes=max(HORIZONS) + 1)
    collected: dict[int, dict[str, list[np.ndarray] | list[str]]] = {h: {"train_x": [], "train_y": [], "test_x": [], "test_y": [], "test_pair": [], "test_time": []} for h in HORIZONS}
    with zipfile.ZipFile(ARCHIVE) as archive:
        for member in sorted(long["members"], key=lambda item: item["instrument"]):
            with archive.open(member["path"]) as handle:
                table = pq.ParquetFile(pa.PythonFile(handle, mode="r")).read_row_group(0, columns=["datetime", "close", "bid_close", "ask_close"])
            selected = table.filter(pc.and_(pc.greater_equal(table["datetime"], pa.scalar(query_start.isoformat())), pc.less(table["datetime"], pa.scalar(query_end.isoformat()))))
            data = {name: np.asarray(selected[name].to_pylist(), dtype=float) for name in ("close", "bid_close", "ask_close")}
            data["time"] = np.array([int(datetime.fromisoformat(value).timestamp()) for value in selected["datetime"].to_pylist()], dtype=np.int64)
            features, decision_time = _features(data)
            cadence = (data["time"] % CADENCE_SECONDS) == 0
            for horizon in HORIZONS:
                outcome = endpoint_outcomes(data, horizon)
                valid = cadence & np.all(np.isfinite(features), axis=1) & (outcome["state"] == "endpoint_available")
                raw_time = data["time"]
                train = valid & (raw_time >= int(TRAIN_START.timestamp())) & (raw_time < int(TRAIN_END.timestamp()))
                test = valid & (raw_time >= int(TRAIN_END.timestamp())) & (raw_time < int(TEST_END.timestamp()))
                bucket = collected[horizon]
                bucket["train_x"].append(features[train]); bucket["train_y"].append(outcome["midpoint_return_bps"][train])
                bucket["test_x"].append(features[test]); bucket["test_y"].append(outcome["midpoint_return_bps"][test])
                bucket["test_pair"].extend([member["instrument"]] * int(test.sum()))
                bucket["test_time"].extend([datetime.fromtimestamp(int(item), tz=timezone.utc).isoformat() for item in decision_time[test]])
            del table, selected
    reports, tape_rows = {}, []
    for horizon, bucket in collected.items():
        train_x, train_y = np.vstack(bucket["train_x"]), np.concatenate(bucket["train_y"])
        test_x, test_y = np.vstack(bucket["test_x"]), np.concatenate(bucket["test_y"])
        predicted = _fit_predict(train_x, train_y, test_x)
        no_change = np.zeros_like(test_y)
        reports[str(horizon)] = {"regularized_pooled": _score(test_y, predicted), "no_change": _score(test_y, no_change), "training_rows": int(len(train_y)), "test_rows": int(len(test_y))}
        for pair, time, actual, forecast in zip(bucket["test_pair"], bucket["test_time"], test_y, predicted, strict=True):
            tape_rows.append({"instrument": pair, "decision_time_utc": time, "horizon_minutes": horizon, "target_family": "endpoint_only_midpoint_return", "forecast_bps": float(forecast), "actual_bps": float(actual), "forecast_status": "offline_research_only", "execution_status": "not_evaluated"})
    tape_rows.sort(key=lambda row: (row["decision_time_utc"], row["instrument"], row["horizon_minutes"]))
    with gzip.open(TAPE, "wt", encoding="utf-8") as handle:
        for row in tape_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    tape_hash = hashlib.sha256(TAPE.read_bytes()).hexdigest()
    payload = {"schema": "all68_endpoint_baseline_v1", "status": "completed_offline_forecast_skill_only", "target_contract": "endpoint-only midpoint return; no intraday path, fill, cost, or P&L claim", "clock_contract": "raw bars treated as interval starts; decision_time = raw timestamp + 60 seconds", "features": ["return_60m_bps", "return_240m_bps", "return_1440m_bps", "quoted_spread_bps", "utc_time_sine"], "models": ["no_change", "pooled_ridge_lambda_20"], "train_window_utc": [TRAIN_START.isoformat(), TRAIN_END.isoformat()], "test_window_utc": [TRAIN_END.isoformat(), TEST_END.isoformat()], "cadence_seconds": CADENCE_SECONDS, "horizons_minutes": HORIZONS, "instrument_count": 68, "results": reports, "forecast_tape": {"path": str(TAPE), "sha256": tape_hash, "records": len(tape_rows)}, "source_archive_sha256": long["sha256"]}
    temporary = REPORT.with_suffix(REPORT.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, REPORT)
    print(f"endpoint baseline complete: {len(tape_rows)} forecasts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
