#!/usr/bin/env python3
"""Create a chronological holdout fixture for frozen foundation models."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from oanda_practice_shadow_strategy_lab import infer_pip_size
from oanda_s5_timeframe_strategy_replay import parse_horizons, parse_timeframe, seconds_label
from oanda_shared_timeframe_horizon_panel import CANONICAL_HORIZONS_SEC


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_SOURCE = DATA_ROOT / "candles_m1_parquet" / "EUR_USD_M1.parquet"
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "model_space"
    / "validation_fixtures"
    / "foundation_m1_holdout_v1.npz"
)
HORIZONS_SEC = np.asarray(CANONICAL_HORIZONS_SEC, dtype=np.int32)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--instrument", default="EUR_USD")
    parser.add_argument("--input-timeframe", default="M1")
    parser.add_argument("--horizons-sec", type=parse_horizons, default=[])
    parser.add_argument("--context-length", type=int, default=512)
    parser.add_argument("--windows", type=int, default=12)
    parser.add_argument("--holdout-fraction", type=float, default=0.20)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    timeframe_seconds = parse_timeframe(args.input_timeframe)
    timeframe = seconds_label(timeframe_seconds)
    horizons = np.asarray(
        sorted(
            {
                int(value)
                for value in (args.horizons_sec or HORIZONS_SEC.tolist())
                if int(value) >= timeframe_seconds
                and int(value) <= 86400
                and int(value) % timeframe_seconds == 0
            }
        ),
        dtype=np.int32,
    )
    if not len(horizons):
        raise SystemExit("no horizons are positive input-timeframe multiples through 24 hours")
    if args.context_length < 16 or args.windows < 2:
        raise SystemExit("context length must be at least 16 and windows at least 2")
    if not 0.05 <= args.holdout_fraction <= 0.50:
        raise SystemExit("holdout fraction must be between 0.05 and 0.50")
    columns = [
        "datetime",
        "close",
        "bid_open",
        "bid_close",
        "ask_open",
        "ask_close",
    ]
    frame = pd.read_parquet(args.source, columns=columns)
    frame["time_utc"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
    for column in columns[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["time_utc", *columns[1:]])
        .sort_values("time_utc")
        .drop_duplicates("time_utc", keep="last")
        .reset_index(drop=True)
    )
    if timeframe_seconds > 60:
        frame = (
            frame.set_index("time_utc")
            .resample(
                f"{timeframe_seconds}s",
                label="right",
                closed="right",
            )
            .agg(
                {
                    "close": "last",
                    "bid_open": "first",
                    "bid_close": "last",
                    "ask_open": "first",
                    "ask_close": "last",
                }
            )
            .dropna()
            .reset_index()
        )
    max_steps = int(horizons.max() // timeframe_seconds)
    holdout_start = max(
        args.context_length - 1,
        int(len(frame) * (1.0 - args.holdout_fraction)),
    )
    timestamps = frame["time_utc"].astype("int64").to_numpy()
    interval_ns = timeframe_seconds * 1_000_000_000
    candidates = [
        index
        for index in range(holdout_start, len(frame) - max_steps)
        if timestamps[index] - timestamps[index - args.context_length + 1]
        == (args.context_length - 1) * interval_ns
        and timestamps[index + max_steps] - timestamps[index]
        == max_steps * interval_ns
    ]
    if len(candidates) < args.windows:
        raise SystemExit(
            f"only {len(candidates)} contiguous holdout windows are available"
        )
    positions = np.linspace(0, len(candidates) - 1, num=args.windows, dtype=int)
    selected = [candidates[index] for index in np.unique(positions)]
    pip = infer_pip_size(args.instrument)
    steps = (horizons // timeframe_seconds).astype(int)
    contexts: list[np.ndarray] = []
    actual_mid: list[np.ndarray] = []
    long_net: list[np.ndarray] = []
    short_net: list[np.ndarray] = []
    decision_ns: list[int] = []
    for index in selected:
        contexts.append(
            frame["close"]
            .iloc[index - args.context_length + 1 : index + 1]
            .to_numpy(np.float32)
        )
        future = frame.iloc[index + steps]
        entry = frame.iloc[index + 1]
        actual_mid.append(future["close"].to_numpy(np.float64))
        long_net.append(
            ((future["bid_close"] - float(entry["ask_open"])) / pip).to_numpy(
                np.float64
            )
        )
        short_net.append(
            ((float(entry["bid_open"]) - future["ask_close"]) / pip).to_numpy(
                np.float64
            )
        )
        decision_ns.append(int(timestamps[index]))
    output = args.output
    if output == DEFAULT_OUTPUT and timeframe != "M1":
        output = DEFAULT_OUTPUT.with_name(f"foundation_{timeframe.lower()}_holdout_v1.npz")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as handle:
            np.savez_compressed(
                handle,
                contexts=np.stack(contexts),
                actual_mid=np.stack(actual_mid),
                long_net_pips=np.stack(long_net),
                short_net_pips=np.stack(short_net),
                decision_time_ns=np.asarray(decision_ns, dtype=np.int64),
                horizons_sec=horizons,
                pip_size=np.asarray([pip], dtype=np.float64),
                input_timeframe_seconds=np.asarray([timeframe_seconds], dtype=np.int32),
                input_timeframe=np.asarray([timeframe]),
            )
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    manifest = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "fixture": str(output.resolve()),
        "fixture_sha256": sha256_file(output),
        "source": str(args.source.resolve()),
        "source_sha256": sha256_file(args.source),
        "instrument": args.instrument,
        "input_timeframe": timeframe,
        "input_timeframe_seconds": timeframe_seconds,
        "context_length": args.context_length,
        "windows": len(selected),
        "horizons_sec": horizons.tolist(),
        "holdout_start_utc": pd.Timestamp(timestamps[holdout_start], tz="UTC").isoformat(),
        "first_decision_utc": pd.Timestamp(min(decision_ns), tz="UTC").isoformat(),
        "last_decision_utc": pd.Timestamp(max(decision_ns), tz="UTC").isoformat(),
        "cost_semantics": "observed entry ask/bid and exit bid/ask; one side chosen per forecast",
        "split": f"untouched final chronological fraction with contiguous {timeframe} windows",
        "account_wired": False,
    }
    manifest_path = output.with_suffix(output.suffix + ".manifest.json")
    write_json_atomic(manifest_path, manifest)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
