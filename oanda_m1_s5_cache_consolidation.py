#!/usr/bin/env python3
"""Merge deep M1 history with recent observed S5 candles into clean M1 caches."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from oanda_s5_timeframe_strategy_replay import infer_pip_size, parse_instruments


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
M1_COLUMNS = (
    "time",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "spread_pips",
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_deep_m1(instrument: str, m1_dir: Path, cache_dir: Path) -> tuple[pd.DataFrame, Path]:
    cache = cache_dir / f"{instrument}_M1.parquet"
    raw = m1_dir / f"{instrument}_M1.csv"
    if cache.is_file():
        return pd.read_parquet(cache), cache
    if not raw.is_file():
        raise FileNotFoundError(f"neither M1 cache nor CSV exists for {instrument}")
    available = set(pd.read_csv(raw, nrows=0).columns)
    usecols = [column for column in M1_COLUMNS if column in available]
    return pd.read_csv(raw, usecols=usecols), raw


def normalize_deep_m1(frame: pd.DataFrame) -> pd.DataFrame:
    time_column = "datetime" if "datetime" in frame else "time"
    output = frame.copy()
    output["_time_utc"] = pd.to_datetime(output[time_column], errors="coerce", utc=True)
    for column in M1_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    numeric = [column for column in M1_COLUMNS if column not in {"time", "datetime"}]
    for column in numeric:
        output[column] = pd.to_numeric(output[column], errors="coerce")
    output = output.dropna(subset=["_time_utc", "open", "high", "low", "close"])
    output["_priority"] = 0
    return output[[*M1_COLUMNS, "_time_utc", "_priority"]]


def resample_s5_to_m1(path: Path, instrument: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame["_time_utc"] = pd.to_datetime(
        frame["dt"] if "dt" in frame else frame["time"], errors="coerce", utc=True
    )
    numeric = [
        f"{side}_{field}"
        for side in ("mid", "bid", "ask")
        for field in ("open", "high", "low", "close")
    ] + ["volume", "spread_pips"]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["_time_utc", *numeric[:-1]])
        .sort_values("_time_utc")
        .drop_duplicates("_time_utc", keep="last")
        .set_index("_time_utc")
    )
    aggregations: dict[str, str] = {"volume": "sum", "spread_pips": "first"}
    for side in ("mid", "bid", "ask"):
        aggregations.update(
            {
                f"{side}_open": "first",
                f"{side}_high": "max",
                f"{side}_low": "min",
                f"{side}_close": "last",
            }
        )
    minute = frame.resample("1min", label="left", closed="left").agg(aggregations)
    minute = minute.dropna(subset=["mid_open", "mid_high", "mid_low", "mid_close"])
    pip = infer_pip_size(instrument)
    minute["spread_pips"] = minute["spread_pips"].fillna(
        (minute["ask_open"] - minute["bid_open"]) / pip
    )
    output = pd.DataFrame(index=minute.index)
    output["open"] = minute["mid_open"]
    output["high"] = minute["mid_high"]
    output["low"] = minute["mid_low"]
    output["close"] = minute["mid_close"]
    output["volume"] = minute["volume"]
    for side in ("bid", "ask"):
        for field in ("open", "high", "low", "close"):
            output[f"{side}_{field}"] = minute[f"{side}_{field}"]
    output["spread_pips"] = minute["spread_pips"]
    output["_time_utc"] = output.index
    output["_priority"] = 1
    output["time"] = output["_time_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    output["datetime"] = output["time"]
    return output[[*M1_COLUMNS, "_time_utc", "_priority"]].reset_index(drop=True)


def consolidate_pair(
    instrument: str,
    *,
    m1_dir: Path,
    cache_dir: Path,
    s5_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    deep, deep_source = read_deep_m1(instrument, m1_dir, cache_dir)
    deep = normalize_deep_m1(deep)
    s5_source = s5_dir / f"{instrument}_S5.parquet"
    if not s5_source.is_file():
        raise FileNotFoundError(s5_source)
    recent = resample_s5_to_m1(s5_source, instrument)
    combined = pd.concat([deep, recent], ignore_index=True)
    combined = (
        combined.sort_values(["_time_utc", "_priority"])
        .drop_duplicates("_time_utc", keep="last")
        .sort_values("_time_utc")
    )
    stamps = combined["_time_utc"]
    stamp_text = stamps.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    combined["time"] = stamp_text
    combined["datetime"] = stamp_text
    combined["volume"] = pd.to_numeric(combined["volume"], errors="coerce").fillna(0).round().astype("int64")
    output = output_dir / f"{instrument}_M1.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        combined.loc[:, M1_COLUMNS].to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "instrument": instrument,
        "status": "completed",
        "deep_source": str(deep_source.resolve()),
        "s5_source": str(s5_source.resolve()),
        "output": str(output.resolve()),
        "deep_rows": int(len(deep)),
        "recent_m1_rows": int(len(recent)),
        "combined_rows": int(len(combined)),
        "start_utc": stamps.iloc[0].isoformat(),
        "end_utc": stamps.iloc[-1].isoformat(),
        "bytes": output.stat().st_size,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--m1-dir", type=Path, default=DATA_ROOT / "candles")
    parser.add_argument("--cache-dir", type=Path, default=DATA_ROOT / "candles_m1_parquet")
    parser.add_argument("--s5-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--instruments", default="")
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output_dir.resolve() == args.cache_dir.resolve():
        raise SystemExit("--output-dir must differ from --cache-dir")
    instruments = (
        parse_instruments(args.instruments)
        if args.instruments
        else sorted(path.name.removesuffix("_S5.parquet") for path in args.s5_dir.glob("*_S5.parquet"))
    )
    records: list[dict[str, Any]] = []
    started = utc_iso()
    for index, instrument in enumerate(instruments, start=1):
        print(f"[m1-consolidation] {index}/{len(instruments)} {instrument}", flush=True)
        try:
            record = consolidate_pair(
                instrument,
                m1_dir=args.m1_dir,
                cache_dir=args.cache_dir,
                s5_dir=args.s5_dir,
                output_dir=args.output_dir,
            )
        except Exception as exc:
            record = {
                "instrument": instrument,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
        records.append(record)
        write_json_atomic(
            args.summary,
            {
                "schema_version": 1,
                "status": "running",
                "started_utc": started,
                "updated_utc": utc_iso(),
                "output_dir": str(args.output_dir.resolve()),
                "records": records,
            },
        )
        if record["status"] == "failed" and args.stop_on_error:
            break
    failures = [record for record in records if record["status"] == "failed"]
    report = {
        "schema_version": 1,
        "status": "complete" if not failures and len(records) == len(instruments) else "complete_with_errors",
        "started_utc": started,
        "finished_utc": utc_iso(),
        "execution_policy": "offline_data_consolidation_no_account_wiring",
        "account_wired": False,
        "m1_dir": str(args.m1_dir.resolve()),
        "cache_dir": str(args.cache_dir.resolve()),
        "s5_dir": str(args.s5_dir.resolve()),
        "output_dir": str(args.output_dir.resolve()),
        "summary": {
            "requested": len(instruments),
            "completed": sum(record["status"] == "completed" for record in records),
            "failed": len(failures),
            "combined_rows": sum(int(record.get("combined_rows", 0)) for record in records),
        },
        "records": records,
    }
    write_json_atomic(args.summary, report)
    print(json.dumps(report["summary"], indent=2), flush=True)
    return 0 if report["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
