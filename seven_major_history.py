"""Acquire and validate multi-year OANDA M1 bid/ask history for seven major FX pairs."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

import oanda_gpt_training_strategy_manager as manager


PAIRS = [
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
]
ROOT = Path("data") / "seven_major_research"
HISTORY_ROOT = ROOT / "history" / "oanda"
REPORT_ROOT = ROOT / "reports"
SCHEMA_VERSION = "seven_major_oanda_m1_v1"
KEEP_COLUMNS = [
    "time_utc",
    "instrument",
    "open",
    "high",
    "low",
    "close",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "spread_pips",
    "volume",
    "source",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def atomic_parquet_write(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(path)


def normalize(frame: pd.DataFrame, instrument: str, source: str = "OANDA_REST_V20") -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=KEEP_COLUMNS)
    out = frame.copy()
    out["time_utc"] = pd.to_datetime(
        out.get("datetime", out.get("time", out.get("time_utc"))),
        errors="coerce",
        utc=True,
    )
    out["instrument"] = instrument
    for column in KEEP_COLUMNS:
        if column in {"time_utc", "instrument", "source"}:
            continue
        out[column] = pd.to_numeric(out.get(column), errors="coerce")
    out["source"] = source
    required = ["time_utc", "bid_close", "ask_close", "close"]
    out = out.dropna(subset=required)
    out = out[out["ask_close"] >= out["bid_close"]]
    out = out.sort_values("time_utc").drop_duplicates("time_utc", keep="last")
    return out[KEEP_COLUMNS].reset_index(drop=True)


def merge_partitions(frame: pd.DataFrame, instrument: str) -> dict[int, int]:
    written: dict[int, int] = {}
    if frame.empty:
        return written
    for year, part in frame.groupby(frame["time_utc"].dt.year, sort=True):
        path = HISTORY_ROOT / instrument / f"{int(year)}.parquet"
        if path.exists():
            old = pd.read_parquet(path)
            old["time_utc"] = pd.to_datetime(old["time_utc"], errors="coerce", utc=True)
            part = pd.concat([old, part], ignore_index=True)
        part = (
            part.sort_values("time_utc")
            .drop_duplicates("time_utc", keep="last")
            .reset_index(drop=True)
        )
        atomic_parquet_write(part[KEEP_COLUMNS], path)
        written[int(year)] = len(part)
    return written


def import_existing_csv(instrument: str) -> int:
    path = manager.DIRS["candles_bam"] / f"{instrument}_M1.csv"
    if not path.exists():
        return 0
    frame = normalize(pd.read_csv(path), instrument)
    merge_partitions(frame, instrument)
    return len(frame)


def partition_files(instrument: str) -> list[Path]:
    return sorted((HISTORY_ROOT / instrument).glob("*.parquet"))


def existing_bounds(instrument: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int]:
    earliest: pd.Timestamp | None = None
    latest: pd.Timestamp | None = None
    rows = 0
    for path in partition_files(instrument):
        frame = pd.read_parquet(path, columns=["time_utc"])
        if frame.empty:
            continue
        times = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True).dropna()
        if times.empty:
            continue
        rows += len(times)
        part_min, part_max = times.min(), times.max()
        earliest = part_min if earliest is None else min(earliest, part_min)
        latest = part_max if latest is None else max(latest, part_max)
    return earliest, latest, rows


def request_candles(
    client: manager.OandaClient,
    instrument: str,
    end_time: datetime,
    batch_size: int,
    attempts: int = 6,
) -> dict[str, Any]:
    last: dict[str, Any] = {}
    for attempt in range(attempts):
        last = client.candles(
            instrument,
            granularity="M1",
            count=batch_size,
            end_time=end_time,
            price="BAM",
        )
        if not last.get("_error"):
            return last
        time.sleep(min(30.0, 1.5 * (2**attempt)))
    return last


def backfill_pair(
    client: manager.OandaClient,
    instrument: str,
    target_start: datetime,
    batch_size: int,
    pause_seconds: float,
) -> dict[str, Any]:
    imported = import_existing_csv(instrument)
    earliest, latest, before_rows = existing_bounds(instrument)
    end_time = (
        earliest.to_pydatetime() - timedelta(seconds=1)
        if earliest is not None
        else utc_now()
    )
    requests = 0
    fetched_rows = 0
    error = ""
    buffer: list[pd.DataFrame] = []
    buffer_rows = 0
    previous_earliest: pd.Timestamp | None = None

    while end_time > target_start:
        payload = request_candles(client, instrument, end_time, batch_size)
        requests += 1
        if payload.get("_error"):
            error = json.dumps(payload, default=str)[:2000]
            break
        raw = manager.candle_df_from_oanda(payload, instrument, "M1")
        batch = normalize(raw, instrument)
        if batch.empty:
            break
        batch = batch[batch["time_utc"] >= pd.Timestamp(target_start)]
        if earliest is not None:
            batch = batch[batch["time_utc"] < earliest]
        raw_earliest = pd.to_datetime(raw["datetime"], errors="coerce", utc=True).min()
        if pd.isna(raw_earliest):
            break
        if previous_earliest is not None and raw_earliest >= previous_earliest:
            error = "pagination made no backward progress"
            break
        previous_earliest = raw_earliest
        end_time = raw_earliest.to_pydatetime() - timedelta(seconds=1)
        if not batch.empty:
            buffer.append(batch)
            fetched_rows += len(batch)
            buffer_rows += len(batch)
        if buffer_rows >= 250_000 or end_time <= target_start:
            merge_partitions(pd.concat(buffer, ignore_index=True), instrument)
            buffer.clear()
            buffer_rows = 0
        if requests % 25 == 0:
            print(
                f"[history] {instrument}: requests={requests} fetched={fetched_rows:,} "
                f"cursor={end_time.isoformat()}",
                flush=True,
            )
        time.sleep(max(0.0, pause_seconds))

    if buffer:
        merge_partitions(pd.concat(buffer, ignore_index=True), instrument)
    after_earliest, after_latest, after_rows = existing_bounds(instrument)
    return {
        "instrument": instrument,
        "imported_existing_csv_rows": imported,
        "rows_before": before_rows,
        "rows_after": after_rows,
        "rows_added": max(0, after_rows - before_rows),
        "requests": requests,
        "fetched_rows": fetched_rows,
        "start": after_earliest.isoformat() if after_earliest is not None else None,
        "end": after_latest.isoformat() if after_latest is not None else None,
        "error": error,
    }


def audit_pair(instrument: str) -> dict[str, Any]:
    frames = [pd.read_parquet(path) for path in partition_files(instrument)]
    if not frames:
        return {"instrument": instrument, "rows": 0}
    frame = pd.concat(frames, ignore_index=True)
    frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
    frame = frame.sort_values("time_utc")
    delta = frame["time_utc"].diff().dt.total_seconds().div(60)
    spreads = pd.to_numeric(frame["spread_pips"], errors="coerce")
    return {
        "instrument": instrument,
        "rows": len(frame),
        "start": frame["time_utc"].min().isoformat(),
        "end": frame["time_utc"].max().isoformat(),
        "duplicate_timestamps": int(frame["time_utc"].duplicated().sum()),
        "missing_required_cells": int(
            frame[["time_utc", "bid_close", "ask_close", "close", "spread_pips"]]
            .isna()
            .sum()
            .sum()
        ),
        "crossed_quotes": int((frame["ask_close"] < frame["bid_close"]).sum()),
        "median_spread_pips": float(spreads.median()),
        "p99_spread_pips": float(spreads.quantile(0.99)),
        "one_minute_intervals_pct": float((delta == 1).mean() * 100),
        "gaps_over_10_minutes": int((delta > 10).sum()),
        "years": {
            path.stem: int(len(pd.read_parquet(path, columns=["time_utc"])))
            for path in partition_files(instrument)
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=float, default=5.0)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--pause-seconds", type=float, default=0.06)
    parser.add_argument("--pairs", nargs="*", default=PAIRS)
    parser.add_argument("--audit-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = [pair.upper().replace("/", "_") for pair in args.pairs]
    unknown = sorted(set(pairs) - set(PAIRS))
    if unknown:
        raise SystemExit(f"Unsupported pairs: {unknown}")
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    target_start = utc_now() - timedelta(days=max(1.0, args.years) * 365.25)
    acquisition: list[dict[str, Any]] = []
    if not args.audit_only:
        creds = manager.load_creds(manager.CREDS_PATH)
        token, base_url, account_id = manager.resolve_oanda_creds(creds)
        client = manager.OandaClient(token, base_url, account_id, timeout=30.0)
        for number, pair in enumerate(pairs, 1):
            print(f"[history] pair {number}/{len(pairs)}: {pair}", flush=True)
            acquisition.append(
                backfill_pair(
                    client,
                    pair,
                    target_start=target_start,
                    batch_size=min(5000, max(10, args.batch_size)),
                    pause_seconds=args.pause_seconds,
                )
            )
    quality = [audit_pair(pair) for pair in pairs]
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_now().isoformat(),
        "source": {
            "name": "OANDA REST-v20 Instrument Candles",
            "price_components": "BAM",
            "granularity": "M1",
            "environment": "practice",
        },
        "target_years": args.years,
        "target_start_utc": target_start.isoformat(),
        "pairs": pairs,
        "acquisition": acquisition,
        "quality": quality,
        "total_rows": sum(int(row.get("rows", 0)) for row in quality),
    }
    report_path = REPORT_ROOT / "latest_history_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str), flush=True)
    return 0 if all(not row.get("error") for row in acquisition) else 1


if __name__ == "__main__":
    raise SystemExit(main())
