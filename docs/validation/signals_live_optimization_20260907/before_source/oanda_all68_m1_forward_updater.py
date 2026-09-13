"""Forward-update local all-68 OANDA M1 candle CSVs.

This is research/data maintenance only.  It calls OANDA instrument candle GET
endpoints and appends newer complete M1 bid/ask/mid candles to:

    data/oanda_training_manager/candles/*_M1.csv

It does not inspect accounts, place orders, close trades, or change live
manager configuration.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from oanda_worker_heartbeat import WorkerHeartbeat

# Import the large manager lazily.  Research inventory/tests that only inspect
# archive contracts must not require its HTTP dependency stack.
manager: Any = None


ROOT = Path(__file__).resolve().parent
CANDLE_ROOT = ROOT / "data" / "oanda_training_manager" / "candles"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_REPORT = REPORT_ROOT / "all68_m1_forward_update_latest.json"
QUOTE_SNAPSHOT = ROOT / "data" / "oanda_training_manager" / "state" / "practice_007_market_quotes_v1.json"
DEFAULT_HEARTBEAT = ROOT / "data" / "oanda_training_manager" / "state" / "all68_m1_forward_update_heartbeat_v1.json"
SCHEMA_VERSION = "all68_m1_forward_updater_v1"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def manager_module() -> Any:
    global manager
    if manager is None:
        import oanda_gpt_training_strategy_manager as loaded_manager
        manager = loaded_manager
    return manager


def read_creds_text() -> str:
    loaded = manager_module()
    try:
        return loaded.CREDS_PATH.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def cfg_value(creds: Dict[str, Any], *names: str) -> str:
    for name in names:
        value = creds.get(name) or os.environ.get(name)
        if value not in (None, ""):
            return str(value)
    return ""


def resolve_readonly_oanda_client() -> Tuple[Any, Dict[str, str]]:
    loaded = manager_module()
    creds = loaded.load_creds(loaded.CREDS_PATH)
    practice_token = cfg_value(
        creds,
        "OANDA_API_KEY",
        "OANDA_ACCESS_TOKEN",
        "OANDA_TOKEN",
        "OANDA_API_TOKEN",
    )
    if practice_token:
        token = practice_token
        env = "practice"
        base_url = "https://api-fxpractice.oanda.com"
        account_id = cfg_value(creds, "OANDA_ACCOUNT_ID_DUM1", "OANDA_ACCOUNT_ID") or "readonly"
    else:
        raise RuntimeError(
            "Missing practice OANDA API token; this research updater refuses "
            "live credentials and api-fxtrade routing"
        )
    return loaded.OandaClient(token, base_url, account_id, timeout=30.0), {
        "environment": env,
        "base_url": base_url,
        "account_id_present": str(bool(account_id and account_id != "readonly")).lower(),
    }


def candle_files() -> List[Path]:
    return sorted(CANDLE_ROOT.glob("*_M1.csv"))


def priced_instruments(path: Path = QUOTE_SNAPSHOT) -> List[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return []
    quotes = payload.get("quotes") or {}
    return sorted(
        instrument
        for instrument, quote in quotes.items()
        if isinstance(quote, dict) and "_" in instrument
    )


def instrument_from_path(path: Path) -> str:
    return path.name[: -len("_M1.csv")]


def csv_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def last_data_line(path: Path) -> str:
    # PowerShell has cheap tail, but keep the updater self-contained and
    # portable.  Reading backwards avoids loading multi-year CSVs into memory.
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = bytearray()
        while position > 0:
            position -= 1
            handle.seek(position)
            char = handle.read(1)
            if char in {b"\n", b"\r"}:
                if buffer:
                    break
                continue
            buffer.extend(char)
        return bytes(reversed(buffer)).decode("utf-8", errors="ignore")


def last_timestamp(path: Path) -> Optional[pd.Timestamp]:
    try:
        header = csv_header(path)
        line = last_data_line(path)
        if not line:
            return None
        values = next(csv.reader([line]))
        time_idx = header.index("datetime") if "datetime" in header else header.index("time")
        return pd.to_datetime(values[time_idx], errors="coerce", utc=True)
    except Exception:
        return None


def first_timestamp(path: Path) -> Optional[pd.Timestamp]:
    try:
        frame = pd.read_csv(path, usecols=lambda column: column in {"time", "datetime"}, nrows=1)
        column = "datetime" if "datetime" in frame else "time"
        value = pd.to_datetime(frame[column].iloc[0], errors="coerce", utc=True)
        return value if pd.notna(value) else None
    except Exception:
        return None


def normalize_for_local_csv(df: pd.DataFrame, instrument: str, columns: Iterable[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(columns))
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce", utc=True)
    out = out.dropna(subset=["datetime"]).sort_values("datetime").drop_duplicates("datetime", keep="last")
    out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    out["instrument"] = instrument
    for column in columns:
        if column not in out:
            out[column] = ""
    return out.loc[:, list(columns)]


def append_rows(path: Path, rows: pd.DataFrame, columns: List[str], dry_run: bool) -> int:
    if rows.empty:
        return 0
    if dry_run:
        return int(len(rows))
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    rows.to_csv(path, mode="a" if exists else "w", header=not exists, index=False, columns=columns)
    return int(len(rows))


def prepend_rows_atomic(
    path: Path,
    rows: pd.DataFrame,
    columns: List[str],
    dry_run: bool,
) -> int:
    """Merge older rows without exposing a partial or duplicate CSV."""
    if rows.empty:
        return 0
    if not path.is_file() or path.stat().st_size == 0:
        return append_rows(path, rows, columns, dry_run)
    existing = pd.read_csv(path, usecols=lambda column: column in set(columns))
    for column in columns:
        if column not in existing:
            existing[column] = ""
    combined = pd.concat([rows.loc[:, columns], existing.loc[:, columns]], ignore_index=True)
    timestamp_column = "datetime" if "datetime" in combined else "time"
    combined["_sort_time"] = pd.to_datetime(
        combined[timestamp_column], errors="coerce", utc=True
    )
    combined = (
        combined.dropna(subset=["_sort_time"])
        .sort_values("_sort_time")
        .drop_duplicates("_sort_time", keep="last")
        .drop(columns=["_sort_time"])
    )
    added = max(0, int(len(combined) - len(existing)))
    if dry_run or added == 0:
        return added
    temporary = path.with_name(f".{path.name}.{os.getpid()}.backfill.tmp")
    try:
        combined.to_csv(temporary, index=False, columns=columns)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return added


def fetch_newer_rows(
    client: Any,
    instrument: str,
    local_last: Optional[pd.Timestamp],
    *,
    max_requests: int,
    batch_size: int,
    pause_seconds: float,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if max_requests <= 0:
        return pd.DataFrame(), {"requests": 0, "earliest_seen": None, "newest_seen": None, "error": ""}
    end_time: Optional[datetime] = None
    batches: List[pd.DataFrame] = []
    requests = 0
    error = ""
    earliest_seen: Optional[pd.Timestamp] = None
    newest_seen: Optional[pd.Timestamp] = None
    cutoff = local_last if local_last is not None and pd.notna(local_last) else None

    for _ in range(max_requests):
        reference_time = end_time or utc_now()
        payload = client.candles(
            instrument,
            granularity="M1",
            count=forward_request_count(
                cutoff,
                batch_size=batch_size,
                reference_time=reference_time,
            ),
            end_time=end_time,
            price="BAM",
        )
        requests += 1
        if payload.get("_error"):
            error = json.dumps({
                "http_status": payload.get("_http_status"),
                "error_text": payload.get("_error_text"),
                "exception": payload.get("_exception"),
            }, default=str)[:1500]
            break
        raw = manager_module().candle_df_from_oanda(payload, instrument, "M1")
        if raw.empty:
            break
        raw["datetime_ts"] = pd.to_datetime(raw["datetime"], errors="coerce", utc=True)
        raw = raw.dropna(subset=["datetime_ts"]).sort_values("datetime_ts")
        if raw.empty:
            break
        batch_earliest = raw["datetime_ts"].min()
        batch_newest = raw["datetime_ts"].max()
        earliest_seen = batch_earliest if earliest_seen is None else min(earliest_seen, batch_earliest)
        newest_seen = batch_newest if newest_seen is None else max(newest_seen, batch_newest)
        if cutoff is not None:
            keep = raw[raw["datetime_ts"] > cutoff].drop(columns=["datetime_ts"])
        else:
            keep = raw.drop(columns=["datetime_ts"])
        if not keep.empty:
            batches.append(keep)
        if cutoff is not None and batch_earliest <= cutoff:
            break
        next_end = batch_earliest.to_pydatetime() - timedelta(seconds=1)
        if end_time is not None and next_end >= end_time:
            error = "pagination made no backward progress"
            break
        end_time = next_end
        time.sleep(max(0.0, pause_seconds))

    combined = pd.concat(batches, ignore_index=True) if batches else pd.DataFrame()
    if not combined.empty:
        combined["datetime_ts"] = pd.to_datetime(combined["datetime"], errors="coerce", utc=True)
        combined = (
            combined.dropna(subset=["datetime_ts"])
            .sort_values("datetime_ts")
            .drop_duplicates("datetime_ts", keep="last")
            .drop(columns=["datetime_ts"])
            .reset_index(drop=True)
        )
    return combined, {
        "requests": requests,
        "earliest_seen": earliest_seen.isoformat() if earliest_seen is not None else None,
        "newest_seen": newest_seen.isoformat() if newest_seen is not None else None,
        "error": error,
    }


def forward_request_count(
    local_last: Optional[pd.Timestamp],
    *,
    batch_size: int,
    reference_time: datetime | pd.Timestamp | None = None,
) -> int:
    """Bound a forward request to the actual M1 gap plus a small overlap."""

    maximum = min(5000, max(10, int(batch_size)))
    if local_last is None or pd.isna(local_last):
        return maximum
    cutoff = pd.Timestamp(local_last)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    upper = pd.Timestamp(reference_time or utc_now())
    if upper.tzinfo is None:
        upper = upper.tz_localize("UTC")
    else:
        upper = upper.tz_convert("UTC")
    missing_minutes = max(0, math.ceil((upper - cutoff).total_seconds() / 60.0))
    return min(maximum, max(10, missing_minutes + 3))


def fetch_older_rows(
    client: Any,
    instrument: str,
    local_first: Optional[pd.Timestamp],
    *,
    max_requests: int,
    batch_size: int,
    pause_seconds: float,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if max_requests <= 0 or local_first is None or pd.isna(local_first):
        return pd.DataFrame(), {"requests": 0, "earliest_seen": None, "newest_seen": None, "error": ""}
    cutoff = local_first
    end_time: Optional[datetime] = cutoff.to_pydatetime() - timedelta(seconds=1)
    batches: List[pd.DataFrame] = []
    requests = 0
    error = ""
    earliest_seen: Optional[pd.Timestamp] = None
    newest_seen: Optional[pd.Timestamp] = None
    for _ in range(max_requests):
        payload = client.candles(
            instrument,
            granularity="M1",
            count=min(5000, max(10, batch_size)),
            end_time=end_time,
            price="BAM",
        )
        requests += 1
        if payload.get("_error"):
            error = json.dumps({
                "http_status": payload.get("_http_status"),
                "error_text": payload.get("_error_text"),
                "exception": payload.get("_exception"),
            }, default=str)[:1500]
            break
        raw = manager_module().candle_df_from_oanda(payload, instrument, "M1")
        if raw.empty:
            break
        raw["datetime_ts"] = pd.to_datetime(raw["datetime"], errors="coerce", utc=True)
        raw = raw.dropna(subset=["datetime_ts"]).sort_values("datetime_ts")
        raw = raw.loc[raw["datetime_ts"] < cutoff]
        if raw.empty:
            break
        batch_earliest = raw["datetime_ts"].min()
        batch_newest = raw["datetime_ts"].max()
        earliest_seen = batch_earliest if earliest_seen is None else min(earliest_seen, batch_earliest)
        newest_seen = batch_newest if newest_seen is None else max(newest_seen, batch_newest)
        batches.append(raw.drop(columns=["datetime_ts"]))
        next_end = batch_earliest.to_pydatetime() - timedelta(seconds=1)
        if end_time is not None and next_end >= end_time:
            error = "historical pagination made no backward progress"
            break
        end_time = next_end
        time.sleep(max(0.0, pause_seconds))
    combined = pd.concat(batches, ignore_index=True) if batches else pd.DataFrame()
    if not combined.empty:
        combined["datetime_ts"] = pd.to_datetime(combined["datetime"], errors="coerce", utc=True)
        combined = (
            combined.dropna(subset=["datetime_ts"])
            .sort_values("datetime_ts")
            .drop_duplicates("datetime_ts", keep="last")
            .drop(columns=["datetime_ts"])
            .reset_index(drop=True)
        )
    return combined, {
        "requests": requests,
        "earliest_seen": earliest_seen.isoformat() if earliest_seen is not None else None,
        "newest_seen": newest_seen.isoformat() if newest_seen is not None else None,
        "error": error,
    }


def update_pair(
    client: Any,
    path: Path,
    *,
    max_requests: int,
    backfill_requests: int,
    batch_size: int,
    pause_seconds: float,
    dry_run: bool,
) -> Dict[str, Any]:
    instrument = instrument_from_path(path)
    before_last = last_timestamp(path)
    before_first = first_timestamp(path)
    columns = csv_header(path) if path.exists() else [
        "time",
        "datetime",
        "instrument",
        "granularity",
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
    ]
    rows, meta = fetch_newer_rows(
        client,
        instrument,
        before_last,
        max_requests=max_requests,
        batch_size=batch_size,
        pause_seconds=pause_seconds,
    )
    normalized = normalize_for_local_csv(rows, instrument, columns)
    appended = append_rows(path, normalized, columns, dry_run=dry_run)
    backfill_cutoff = before_first
    if backfill_cutoff is None and not normalized.empty:
        backfill_cutoff = pd.to_datetime(normalized["datetime"], errors="coerce", utc=True).min()
    older, older_meta = fetch_older_rows(
        client,
        instrument,
        backfill_cutoff,
        max_requests=backfill_requests,
        batch_size=batch_size,
        pause_seconds=pause_seconds,
    )
    normalized_older = normalize_for_local_csv(older, instrument, columns)
    backfilled = prepend_rows_atomic(path, normalized_older, columns, dry_run=dry_run)
    after_last = before_last
    if not normalized.empty:
        after_last = pd.to_datetime(normalized["datetime"], errors="coerce", utc=True).max()
    return {
        "instrument": instrument,
        "path": str(path),
        "before_last": before_last.isoformat() if before_last is not None and pd.notna(before_last) else None,
        "before_first": before_first.isoformat() if before_first is not None and pd.notna(before_first) else None,
        "after_last": after_last.isoformat() if after_last is not None and pd.notna(after_last) else None,
        "rows_appended": appended,
        "rows_backfilled": backfilled,
        "backfill": older_meta,
        "dry_run": dry_run,
        **meta,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", nargs="*", default=[])
    parser.add_argument(
        "--bootstrap-all-priced",
        action="store_true",
        help="Create missing per-pair CSVs for every instrument in the practice quote snapshot.",
    )
    parser.add_argument("--max-requests-per-pair", type=int, default=8)
    parser.add_argument(
        "--backfill-requests-per-pair",
        type=int,
        default=0,
        help="Fetch this many older 5,000-candle pages before each local file's first timestamp and merge atomically.",
    )
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--pause-seconds", type=float, default=0.10)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument(
        "--interval-sec",
        type=float,
        default=0.0,
        help="Repeat the read-only archive update at this interval; zero runs once.",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=0.0,
        help="Stop a repeating worker after this duration; zero repeats indefinitely.",
    )
    return parser.parse_args()


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def run_once(
    args: argparse.Namespace, heartbeat: WorkerHeartbeat | None = None
) -> int:
    files = candle_files()
    if args.pairs:
        wanted = {pair.upper().replace("/", "_") for pair in args.pairs}
        files = [CANDLE_ROOT / f"{instrument}_M1.csv" for instrument in sorted(wanted)]
    elif args.bootstrap_all_priced:
        wanted = priced_instruments()
        if not wanted:
            raise SystemExit(f"No practice-priced instruments found in {QUOTE_SNAPSHOT}")
        files = [CANDLE_ROOT / f"{instrument}_M1.csv" for instrument in wanted]
    if not files:
        raise SystemExit(
            f"No *_M1.csv files found in {CANDLE_ROOT}; use --pairs or "
            "--bootstrap-all-priced to start a practice-only research archive"
        )
    client, client_meta = resolve_readonly_oanda_client()
    started = utc_now()
    rows: List[Dict[str, Any]] = []
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="updating_pairs", pair_count=len(files), pairs_completed=0
        )
    for number, path in enumerate(files, 1):
        instrument = instrument_from_path(path)
        print(f"[m1-update] {number}/{len(files)} {instrument}", flush=True)
        rows.append(
            update_pair(
                client,
                path,
                max_requests=args.max_requests_per_pair,
                backfill_requests=args.backfill_requests_per_pair,
                batch_size=args.batch_size,
                pause_seconds=args.pause_seconds,
                dry_run=args.dry_run,
            )
        )
        if heartbeat is not None:
            heartbeat.mark_progress(
                phase="updating_pairs",
                pair_count=len(files),
                pairs_completed=number,
                instrument=instrument,
                last_error=str(rows[-1].get("error") or "")[:300],
            )
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_now().isoformat(),
        "started_utc": started.isoformat(),
        "source": {
            "name": "OANDA REST-v20 Instrument Candles",
            "granularity": "M1",
            "price": "BAM",
            **client_meta,
        },
        "dry_run": bool(args.dry_run),
        "pair_count": len(files),
        "total_rows_appended": int(sum(int(row.get("rows_appended") or 0) for row in rows)),
        "total_rows_backfilled": int(sum(int(row.get("rows_backfilled") or 0) for row in rows)),
        "error_count": int(sum(1 for row in rows if row.get("error"))),
        "pairs": rows,
    }
    write_json_atomic(args.report, report)
    if heartbeat is not None:
        heartbeat.mark_progress(
            phase="cycle_complete",
            pair_count=len(files),
            pairs_completed=len(files),
            error_count=report["error_count"],
            total_rows_appended=report["total_rows_appended"],
        )
    print(json.dumps({
        "report": str(args.report),
        "pair_count": report["pair_count"],
        "total_rows_appended": report["total_rows_appended"],
        "total_rows_backfilled": report["total_rows_backfilled"],
        "error_count": report["error_count"],
        "dry_run": report["dry_run"],
    }, indent=2), flush=True)
    return 0 if report["error_count"] == 0 else 1


def main() -> int:
    args = parse_args()
    with WorkerHeartbeat(
        args.heartbeat,
        worker="all68_m1_forward_archive",
        role="read_only_market_data_archive",
        interval_sec=5.0,
    ) as heartbeat:
        if args.interval_sec <= 0:
            return run_once(args, heartbeat=heartbeat)
        started = time.monotonic()
        deadline = started + args.duration_sec if args.duration_sec > 0 else None
        last_status = 0
        while True:
            cycle_started = time.monotonic()
            try:
                last_status = run_once(args, heartbeat=heartbeat)
            except Exception as exc:
                # A transient read failure must not permanently stop the durable
                # archive.  The next scheduled cycle retries; the existing report
                # remains intact because writes are atomic.
                last_status = 1
                heartbeat.mark_progress(
                    phase="cycle_error",
                    error=f"{type(exc).__name__}: {exc}"[:500],
                )
                print(f"[m1-update] cycle error: {type(exc).__name__}: {exc}", flush=True)
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            sleep_seconds = max(0.1, args.interval_sec - (now - cycle_started))
            if deadline is not None:
                sleep_seconds = min(sleep_seconds, max(0.0, deadline - now))
                if sleep_seconds <= 0:
                    break
            heartbeat.update(phase="sleeping", sleep_seconds=round(sleep_seconds, 3))
            time.sleep(sleep_seconds)
        return last_status


if __name__ == "__main__":
    raise SystemExit(main())
