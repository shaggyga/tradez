#!/usr/bin/env python3
"""Recover selected OANDA BAM histories without reading local raw CSVs."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import oanda_gpt_training_strategy_manager as manager
from oanda_m1_s5_cache_consolidation import M1_COLUMNS


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
S5_COLUMNS = (
    "time",
    "dt",
    "mid_open",
    "mid_high",
    "mid_low",
    "mid_close",
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
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def request_batch(
    client: manager.OandaClient,
    instrument: str,
    end_time: datetime,
    batch_size: int,
    granularity: str = "M1",
    attempts: int = 6,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for attempt in range(attempts):
        result = client.candles(
            instrument,
            granularity=granularity,
            count=batch_size,
            end_time=end_time,
            price="BAM",
        )
        if not result.get("_error"):
            return result
        time.sleep(min(30.0, 1.5 * (2**attempt)))
    return result


def storage_frame(frame: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Return the canonical on-disk schema expected by each history loader."""
    if granularity != "S5":
        for column in M1_COLUMNS:
            if column not in frame:
                frame[column] = pd.NA
        return frame.loc[:, M1_COLUMNS]

    output = frame.copy()
    output["dt"] = output["datetime"]
    for field in ("open", "high", "low", "close"):
        output[f"mid_{field}"] = output[field]
    for column in S5_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    return output.loc[:, S5_COLUMNS]


def recover_pair(
    client: manager.OandaClient,
    instrument: str,
    *,
    cutoff: datetime,
    batch_size: int,
    pause_seconds: float,
    output_dir: Path,
    granularity: str = "M1",
    output_label: str = "",
) -> dict[str, Any]:
    cursor = utc_now()
    previous_earliest: pd.Timestamp | None = None
    batches: list[pd.DataFrame] = []
    requests = 0
    while cursor > cutoff:
        payload = request_batch(
            client,
            instrument,
            cursor,
            batch_size,
            granularity=granularity,
        )
        requests += 1
        if payload.get("_error"):
            raise RuntimeError(json.dumps(payload, default=str)[:2000])
        batch = manager.candle_df_from_oanda(payload, instrument, granularity)
        if batch.empty:
            break
        stamps = pd.to_datetime(
            batch["datetime"], format="ISO8601", errors="coerce", utc=True
        )
        earliest = stamps.min()
        if pd.isna(earliest):
            raise RuntimeError("OANDA batch contained no valid M1 timestamps")
        if previous_earliest is not None and earliest >= previous_earliest:
            raise RuntimeError("M1 pagination made no backward progress")
        batches.append(batch)
        previous_earliest = earliest
        cursor = earliest.to_pydatetime() - timedelta(seconds=1)
        if requests % 25 == 0:
            print(
                f"[m1-api-recovery] {instrument}: requests={requests} cursor={cursor.isoformat()}",
                flush=True,
            )
        if earliest <= pd.Timestamp(cutoff):
            break
        time.sleep(max(0.0, pause_seconds))
    if not batches:
        raise RuntimeError(f"OANDA returned no M1 candles for {instrument}")
    combined = pd.concat(batches, ignore_index=True)
    combined["_time_utc"] = pd.to_datetime(
        combined["datetime"], format="ISO8601", errors="coerce", utc=True
    )
    combined = (
        combined[combined["_time_utc"] >= pd.Timestamp(cutoff)]
        .dropna(subset=["_time_utc", "open", "high", "low", "close"])
        .sort_values("_time_utc")
        .drop_duplicates("_time_utc", keep="last")
    )
    if combined.empty:
        raise RuntimeError(f"no {instrument} candles remained after cutoff filtering")
    stored = storage_frame(combined, granularity)
    output = output_dir / f"{instrument}_{output_label or granularity}.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        stored.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "instrument": instrument,
        "status": "completed",
        "requests": requests,
        "rows": int(len(combined)),
        "start_utc": combined["_time_utc"].iloc[0].isoformat(),
        "end_utc": combined["_time_utc"].iloc[-1].isoformat(),
        "output": str(output.resolve()),
        "bytes": output.stat().st_size,
        "source": f"OANDA_REST_V20_{granularity}_BAM",
        "source_granularity": granularity,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", required=True)
    parser.add_argument("--days", type=int, default=765)
    parser.add_argument(
        "--granularity",
        choices=("S5", "M1", "M5", "M15", "M30", "H1", "H4"),
        default="M1",
    )
    parser.add_argument(
        "--output-label",
        default="",
        help="Optional filename granularity label; useful for a coarser proxy cache",
    )
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--pause-seconds", type=float, default=0.04)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Independent read-only OANDA clients used for pair recovery.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.workers < 1:
        raise SystemExit("--workers must be positive")
    pairs = sorted(
        {
            value.strip().upper().replace("/", "_")
            for value in args.pairs.split(",")
            if value.strip()
        }
    )
    if not pairs:
        raise SystemExit("--pairs must contain at least one instrument")
    cutoff = utc_now() - timedelta(days=max(1, int(args.days)))
    creds = manager.load_creds(manager.CREDS_PATH)
    token, base_url, account_id = manager.resolve_oanda_creds(creds)
    started = utc_now().isoformat()
    records: list[dict[str, Any]] = []

    def recover(instrument: str) -> dict[str, Any]:
        client = manager.OandaClient(token, base_url, account_id, timeout=30.0)
        try:
            return recover_pair(
                client,
                instrument,
                cutoff=cutoff,
                batch_size=min(5000, max(10, int(args.batch_size))),
                pause_seconds=args.pause_seconds,
                output_dir=args.output_dir,
                granularity=args.granularity,
                output_label=args.output_label,
            )
        except Exception as exc:
            return {
                "instrument": instrument,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }

    def persist_running() -> None:
        write_json_atomic(
            args.summary,
            {
                "schema_version": 1,
                "status": "running",
                "started_utc": started,
                "updated_utc": utc_now().isoformat(),
                "cutoff_utc": cutoff.isoformat(),
                "records": records,
            },
        )

    if args.workers == 1:
        for index, instrument in enumerate(pairs, start=1):
            print(f"[m1-api-recovery] {index}/{len(pairs)} {instrument}", flush=True)
            record = recover(instrument)
            records.append(record)
            persist_running()
            if record["status"] == "failed" and args.stop_on_error:
                break
    else:
        with ThreadPoolExecutor(max_workers=min(args.workers, len(pairs))) as executor:
            futures = {executor.submit(recover, instrument): instrument for instrument in pairs}
            for completed, future in enumerate(as_completed(futures), start=1):
                record = future.result()
                records.append(record)
                print(
                    f"[m1-api-recovery] {completed}/{len(pairs)} "
                    f"{record['instrument']} {record['status']}",
                    flush=True,
                )
                persist_running()
                if record["status"] == "failed" and args.stop_on_error:
                    for pending in futures:
                        pending.cancel()
                    break
    failures = [record for record in records if record["status"] == "failed"]
    report = {
        "schema_version": 1,
        "status": "complete" if not failures and len(records) == len(pairs) else "complete_with_errors",
        "started_utc": started,
        "finished_utc": utc_now().isoformat(),
        "execution_policy": "historical_candle_download_only_no_orders_no_account_wiring",
        "account_wired": False,
        "cutoff_utc": cutoff.isoformat(),
        "output_dir": str(args.output_dir.resolve()),
        "granularity": args.granularity,
        "summary": {
            "requested": len(pairs),
            "completed": sum(record["status"] == "completed" for record in records),
            "failed": len(failures),
            "rows": sum(int(record.get("rows", 0)) for record in records),
        },
        "records": records,
    }
    write_json_atomic(args.summary, report)
    print(json.dumps(report["summary"], indent=2), flush=True)
    return 0 if report["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
