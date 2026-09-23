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
import hashlib
import io
import json
import math
import os
import time
import tempfile
from decimal import Decimal, InvalidOperation
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
GAP_SCHEMA_VERSION = "all68_m1_gap_recovery_v1"
GAP_TAIL_BYTES = 1024 * 1024
GAP_LOOKBACK_MINUTES = 720
GAP_MAX_WIDTH_MINUTES = 5
GAP_MAX_ATTEMPTS = 2
GAP_RETRY_SECONDS = 900


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


def candle_files(root: Path | None = None) -> List[Path]:
    selected_root = CANDLE_ROOT if root is None else Path(root)
    return sorted(selected_root.glob("*_M1.csv"))


def priced_instruments(path: Path | None = None) -> List[str]:
    path = QUOTE_SNAPSHOT if path is None else Path(path)
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


def _file_identity(stat: os.stat_result) -> tuple:
    # Windows Python exposes different ctime semantics through stat/fstat;
    # inode, size, mtime and the retained tail digest provide comparable checks.
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _minute_epoch(value: Any) -> int:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None or stamp.value % 60_000_000_000:
        raise ValueError("candle timestamp must be an explicit UTC-aligned minute")
    return int(stamp.timestamp())


def _epoch_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def scan_recent_gaps(path: Path) -> Dict[str, Any]:
    """Read at most a 1 MiB tail plus header; never scan an archive in full.

    Only interior gaps of at most five minutes in the latest 720 minutes are
    candidates. Long market closures and history outside this scope are not
    interpreted as missing broker candles. Original lines are retained verbatim
    for a possible later insertion, not reserialized through pandas.
    """
    if not path.exists():
        return {"status": "missing_archive", "missing": [], "lines": []}
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        header = handle.readline(8193)
        if len(header) > 8192 or not header.endswith(b"\n"):
            raise ValueError("invalid or oversized CSV header")
        columns = next(csv.reader([header.decode("utf-8-sig")], strict=True))
        if (len(columns) != len(set(columns)) or any(not column for column in columns)
                or not {"instrument", "granularity", "close"}.issubset(columns)):
            raise ValueError("invalid or ambiguous CSV recovery columns")
        clock_columns = [columns.index(name) for name in ("time", "datetime") if name in columns]
        if not clock_columns:
            raise ValueError("explicit CSV recovery timestamp required")
        expected_instrument = instrument_from_path(path)
        instrument_column, granularity_column = columns.index("instrument"), columns.index("granularity")
        start = max(len(header), before.st_size - GAP_TAIL_BYTES)
        if start > len(header):
            handle.seek(start - 1)
            if handle.read(1) != b"\n":
                handle.readline(GAP_TAIL_BYTES + 1)
            start = handle.tell()
        else:
            handle.seek(start)
        raw = handle.read(GAP_TAIL_BYTES + 1)
        if _file_identity(before) != _file_identity(os.fstat(handle.fileno())):
            raise RuntimeError("archive changed during gap scan")
    if len(raw) > GAP_TAIL_BYTES or (raw and not raw.endswith(b"\n")):
        raise ValueError("incomplete or oversized CSV tail")
    lines = []
    for raw_line in raw.splitlines(keepends=True):
        fields = next(csv.reader([raw_line.decode("utf-8")], strict=True))
        if len(fields) != len(columns):
            raise ValueError("malformed CSV tail row")
        stamps = [_minute_epoch(fields[column]) for column in clock_columns]
        if len(set(stamps)) != 1:
            raise ValueError("conflicting CSV recovery timestamps")
        epoch = stamps[0]
        if fields[instrument_column] != expected_instrument or fields[granularity_column] != "M1":
            raise ValueError("CSV recovery instrument/granularity mismatch")
        if lines and epoch <= lines[-1][0]:
            raise ValueError("CSV tail is not strictly chronological")
        lines.append((epoch, raw_line))
    missing = []
    large_gaps = 0
    cutoff = lines[-1][0] - GAP_LOOKBACK_MINUTES * 60 if lines else 0
    for (previous, _), (current, _) in zip(lines, lines[1:]):
        if current < cutoff:
            continue
        count = (current - previous) // 60 - 1
        if count > GAP_MAX_WIDTH_MINUTES:
            large_gaps += 1
        elif count > 0:
            missing.extend(epoch for epoch in range(previous + 60, current, 60) if epoch >= cutoff)
    return {
        "status": "scanned" if len(lines) >= 2 else "insufficient_tail_rows",
        "identity": _file_identity(before), "header": header,
        "columns": columns, "start": start, "lines": lines, "missing": missing,
        "tail_sha256": hashlib.sha256(raw).hexdigest(), "bytes_scanned": len(header) + len(raw),
        "first_scanned_utc": _epoch_iso(lines[0][0]) if lines else None,
        "last_scanned_utc": _epoch_iso(lines[-1][0]) if lines else None,
        "large_gaps_skipped": large_gaps,
    }


def _write_immutable_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # An interrupted reservation still consumes an attempt. Never overwrite an
    # observation receipt or retry forever after a crash or broker omission.
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _gap_attempt(path: Path, instrument: str, epoch: int, now: datetime) -> tuple:
    """Return next reservation path, or a bounded terminal/cooldown reason."""
    directory = path.parent / ".gap_recovery_v1" / instrument
    previous_time = None
    for number in range(1, GAP_MAX_ATTEMPTS + 1):
        receipt = directory / f"{epoch}.attempt{number}.requested.json"
        if receipt.exists():
            try:
                with receipt.open("rb") as handle:
                    raw = handle.read(8193)
                if len(raw) > 8192:
                    return None, "invalid_attempt_receipt"
                payload = json.loads(raw)
                if payload.get("instrument") != instrument or payload.get("candle_epoch") != epoch:
                    return None, "invalid_attempt_receipt"
                previous_time = datetime.fromisoformat(payload["requested_utc"])
                if previous_time.tzinfo is None or previous_time > now:
                    return None, "invalid_attempt_clock"
            except (OSError, ValueError, KeyError, TypeError):
                return None, "invalid_attempt_receipt"
            continue
        if previous_time is not None and (now - previous_time).total_seconds() < GAP_RETRY_SECONDS:
            return None, "retry_cooldown"
        return receipt, "eligible"
    return None, "attempts_exhausted"


def _actual_gap_row(payload: Dict[str, Any], instrument: str, epoch: int,
                    observed: datetime, columns: List[str]) -> Optional[Dict[str, Any]]:
    """Accept only the requested complete BAM minute; never infer OHLC values."""
    if payload.get("instrument") != instrument or payload.get("granularity") != "M1":
        raise ValueError("broker response instrument/granularity mismatch")
    candles = payload.get("candles")
    if not isinstance(candles, list) or len(candles) > 10:
        raise ValueError("invalid bounded candle response")
    matches = [item for item in candles if isinstance(item, dict)
               and _minute_epoch(item.get("time")) == epoch]
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError("duplicate requested broker minute")
    candle = matches[0]
    if candle.get("complete") is not True or epoch + 60 > observed.timestamp():
        raise ValueError("requested broker minute is incomplete or immature")
    row: Dict[str, Any] = {
        "time": candle["time"], "datetime": _epoch_iso(epoch),
        "instrument": instrument, "granularity": "M1",
    }
    parsed = {}
    for component, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
        values = candle.get(component)
        if not isinstance(values, dict):
            raise ValueError("missing actual BAM candle component")
        prices = {}
        for key, name in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close")):
            try:
                value = Decimal(str(values[key]))
            except (KeyError, InvalidOperation):
                raise ValueError("invalid actual BAM price") from None
            if not value.is_finite() or value <= 0 or not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError("nonpositive or nonfinite actual BAM price")
            prices[key] = value
            row[prefix + name] = str(value)
        if not prices["l"] <= min(prices["o"], prices["c"]) <= max(prices["o"], prices["c"]) <= prices["h"]:
            raise ValueError("invalid actual BAM OHLC bounds")
        parsed[component] = prices
    if any(parsed["bid"][key] > parsed["ask"][key] for key in "ohlc"):
        raise ValueError("crossed actual BAM prices")
    if any(not parsed["bid"][key] <= parsed["mid"][key] <= parsed["ask"][key] for key in "ohlc"):
        raise ValueError("actual BAM midpoint outside bid/ask bounds")
    volume = candle.get("volume")
    if isinstance(volume, bool) or not isinstance(volume, int) or volume < 0:
        raise ValueError("invalid actual candle volume")
    row["volume"] = volume
    pip = Decimal("0.01") if instrument.endswith("_JPY") else Decimal("0.0001")
    row["spread_pips"] = str((parsed["ask"]["c"] - parsed["bid"]["c"]) / pip)
    if any(column not in row for column in columns):
        raise ValueError("unsupported archive columns for recovery")
    return row


def _insert_gap_row(path: Path, snapshot: Dict[str, Any], epoch: int,
                    row: Dict[str, Any]) -> None:
    """Atomic ordered insertion; all preexisting bytes remain unchanged.

    The archive updater must remain the sole writer. Detect changed files before
    and during the streaming copy; refuse to merge against a stale snapshot.
    """
    if epoch not in snapshot["missing"]:
        raise ValueError("recovery minute was not missing in the captured tail")
    stream = io.StringIO(newline="")
    newline = "\r\n" if snapshot["header"].endswith(b"\r\n") else "\n"
    writer = csv.DictWriter(stream, fieldnames=snapshot["columns"], lineterminator=newline)
    writer.writerow({column: row[column] for column in snapshot["columns"]})
    inserted = stream.getvalue().encode("utf-8")
    temporary = None
    try:
        with path.open("rb") as source:
            if _file_identity(os.fstat(source.fileno())) != snapshot["identity"]:
                raise RuntimeError("archive changed before recovery publication")
            with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent,
                                             prefix=f".{path.name}.gap.", suffix=".tmp",
                                             delete=False) as destination:
                temporary = Path(destination.name)
                remaining = snapshot["start"]
                while remaining:
                    chunk = source.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise RuntimeError("archive truncated during recovery")
                    destination.write(chunk)
                    remaining -= len(chunk)
                actual_tail = source.read(GAP_TAIL_BYTES + 1)
                if hashlib.sha256(actual_tail).hexdigest() != snapshot["tail_sha256"]:
                    raise RuntimeError("archive tail changed during recovery")
                pending = True
                for existing_epoch, original in snapshot["lines"]:
                    if pending and existing_epoch > epoch:
                        destination.write(inserted)
                        pending = False
                    destination.write(original)
                if pending:
                    raise ValueError("recovery minute is not an interior gap")
                destination.flush()
                os.fsync(destination.fileno())
            if _file_identity(os.fstat(source.fileno())) != snapshot["identity"]:
                raise RuntimeError("archive changed during recovery publication")
        if _file_identity(path.stat()) != snapshot["identity"]:
            raise RuntimeError("archive replaced during recovery publication")
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def recover_recent_gaps(client: Any, path: Path, *, dry_run: bool = False,
                        enabled: bool = True) -> Dict[str, Any]:
    """At most one ten-candle GET per pair/cycle and two attempts per minute.

    Receipts store real response-observed and post-publication clocks. They do
    not claim that newly recovered old bars were available at their candle time.
    The CSV retains its original schema and every previously present row.
    """
    result: Dict[str, Any] = {
        "schema_version": GAP_SCHEMA_VERSION, "status": "disabled" if not enabled else "scanning",
        "scope": "latest_720_minutes_in_at_most_1MiB_tail; interior_gaps_at_most_5_minutes",
        "requests": 0, "rows_recovered": 0, "error": "", "dry_run": dry_run,
        "synthetic_rows": 0, "historical_availability_asserted": False,
    }
    if not enabled:
        return result
    try:
        snapshot = scan_recent_gaps(path)
        missing = snapshot["missing"]
        result.update({key: snapshot.get(key) for key in (
            "bytes_scanned", "first_scanned_utc", "last_scanned_utc", "large_gaps_skipped")})
        result.update(missing_minutes_detected=len(missing),
                      unresolved_minutes=len(missing),
                      missing_minute_utc=[_epoch_iso(epoch) for epoch in missing])
        if not missing:
            result["status"] = snapshot["status"] if snapshot["status"] != "scanned" else "no_small_gaps_in_scope"
            return result
        if dry_run:
            result["status"] = "dry_run_detected_only"
            return result
        instrument = instrument_from_path(path)
        now = utc_now()
        reason_counts: Dict[str, int] = {}
        chosen = None
        # Newest missing minutes matter most to a future contiguous suffix.
        for epoch in reversed(missing):
            reservation, reason = _gap_attempt(path, instrument, epoch, now)
            if reservation is not None and chosen is None:
                chosen = (epoch, reservation)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        result["attempt_state_counts"] = reason_counts
        if chosen is None:
            result["status"] = "unresolved_no_retry_due"
            return result
        epoch, reservation = chosen
        request = {
            "schema_version": GAP_SCHEMA_VERSION, "instrument": instrument,
            "candle_epoch": epoch, "candle_utc": _epoch_iso(epoch),
            "requested_utc": now.isoformat(), "end_time_utc": _epoch_iso(epoch + 60),
            "count": 10, "granularity": "M1", "price": "BAM",
            "prior_tail_sha256": snapshot["tail_sha256"],
            "historical_availability_asserted": False,
        }
        _write_immutable_json(reservation, request)
        result.update(requests=1, requested_minute_utc=_epoch_iso(epoch),
                      request_receipt=str(reservation))
        payload = client.candles(instrument, granularity="M1", count=10,
                                 end_time=datetime.fromtimestamp(epoch + 60, timezone.utc), price="BAM")
        observed = utc_now()
        # Retain the actual structured GET response before exposing any row.
        response_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(response_bytes) > 256 * 1024:
            raise ValueError("oversized gap response")
        observation = reservation.with_name(reservation.name.replace(".requested.json", ".observed.json"))
        _write_immutable_json(observation, {
            **request, "response_observed_utc": observed.isoformat(),
            "response_sha256": hashlib.sha256(response_bytes).hexdigest(),
            "response": payload,
        })
        result.update(response_observed_utc=observed.isoformat(), observation_receipt=str(observation))
        if observed < now:
            raise ValueError("clock rolled back during gap response")
        if not isinstance(payload, dict) or payload.get("_error"):
            raise ValueError("broker gap request failed")
        row = _actual_gap_row(payload, instrument, epoch, observed, snapshot["columns"])
        if row is None:
            result["status"] = "broker_omitted_requested_minute"
            return result
        _insert_gap_row(path, snapshot, epoch, row)
        # Publication happened before this timestamp; it is never backdated to
        # the response or to the old event time. The observed receipt is durable
        # even if a process dies between replacement and this final receipt.
        published = utc_now()
        publication = reservation.with_name(reservation.name.replace(".requested.json", ".published.json"))
        result.update(rows_recovered=1, unresolved_minutes=len(missing) - 1,
                      status="recovered_actual_broker_minute", publication_receipt=str(publication),
                      publication_clock_valid=published >= observed)
        _write_immutable_json(publication, {
            **request, "response_observed_utc": observed.isoformat(),
            "publication_recorded_utc": published.isoformat(),
            "publication_clock_valid": published >= observed,
            "observation_receipt": str(observation), "rows_recovered": 1,
            "existing_rows_rewritten": 0,
        })
        if published < observed:
            result.update(status="recovered_with_invalid_publication_clock",
                          error="clock rolled back after CSV publication; receipt records actual clock")
    except Exception as exc:
        result["status"] = "recovery_error"
        result["error"] = f"{type(exc).__name__}: {exc}"[:300]
    return result


def update_pair(
    client: Any,
    path: Path,
    *,
    max_requests: int,
    backfill_requests: int,
    batch_size: int,
    pause_seconds: float,
    dry_run: bool,
    recover_gaps: bool = True,
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
    gap_recovery = recover_recent_gaps(client, path, dry_run=dry_run, enabled=recover_gaps)
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
        "gap_recovery": gap_recovery,
        "rows_recovered": gap_recovery["rows_recovered"],
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
    parser.add_argument(
        "--no-gap-recovery", action="store_true",
        help="Disable bounded recent interior-gap detection/recovery and its observation receipts.",
    )
    parser.add_argument("--pause-seconds", type=float, default=0.10)
    parser.add_argument(
        "--gap-recovery-every-cycles",
        type=int,
        default=1,
        help=("Run bounded interior-gap recovery every N archive cycles; "
              "one preserves the original every-cycle behavior."),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--candle-root", type=Path, default=None,
                        help="Archive directory; omitted preserves the legacy candle root.")
    parser.add_argument("--quote-snapshot", type=Path, default=None,
                        help="Practice instrument snapshot; omitted preserves the legacy source.")
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
    args: argparse.Namespace, heartbeat: WorkerHeartbeat | None = None,
    *, cycle_index: int = 1,
) -> int:
    selected_candle_root = getattr(args, "candle_root", None)
    selected_quote_snapshot = getattr(args, "quote_snapshot", None)
    candle_root = CANDLE_ROOT if selected_candle_root is None else Path(selected_candle_root)
    quote_snapshot = QUOTE_SNAPSHOT if selected_quote_snapshot is None else Path(selected_quote_snapshot)
    # Preserve the no-argument interface for existing programmatic callers.
    files = candle_files() if selected_candle_root is None else candle_files(candle_root)
    if args.pairs:
        wanted = {pair.upper().replace("/", "_") for pair in args.pairs}
        files = [candle_root / f"{instrument}_M1.csv" for instrument in sorted(wanted)]
    elif args.bootstrap_all_priced:
        wanted = priced_instruments() if selected_quote_snapshot is None else priced_instruments(quote_snapshot)
        if not wanted:
            raise SystemExit(f"No practice-priced instruments found in {quote_snapshot}")
        files = [candle_root / f"{instrument}_M1.csv" for instrument in wanted]
    if not files:
        raise SystemExit(
            f"No *_M1.csv files found in {candle_root}; use --pairs or "
            "--bootstrap-all-priced to start a practice-only research archive"
        )
    recovery_every = int(getattr(args, "gap_recovery_every_cycles", 1))
    if recovery_every < 1:
        raise ValueError("gap_recovery_every_cycles_must_be_positive")
    recover_gaps = cycle_index % recovery_every == 0
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
                recover_gaps=(not getattr(args, "no_gap_recovery", False) and recover_gaps),
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
        "archive_paths": {"candle_root": str(candle_root), "quote_snapshot": str(quote_snapshot)},
        "pair_count": len(files),
        "total_rows_appended": int(sum(int(row.get("rows_appended") or 0) for row in rows)),
        "total_rows_backfilled": int(sum(int(row.get("rows_backfilled") or 0) for row in rows)),
        "total_rows_recovered": int(sum(int(row.get("rows_recovered") or 0) for row in rows)),
        "gap_recovery_requests": int(sum(int(row.get("gap_recovery", {}).get("requests") or 0) for row in rows)),
        "gap_recovery_unresolved_minutes": int(sum(int(row.get("gap_recovery", {}).get("unresolved_minutes") or 0) for row in rows)),
        "gap_recovery_unknown_pair_count": int(sum(1 for row in rows if row.get("gap_recovery", {}).get("status")
            not in (None, "disabled") and row.get("gap_recovery", {}).get("missing_minutes_detected") is None)),
        "gap_recovery_cycle": {
            "cycle_index": cycle_index,
            "every_cycles": recovery_every,
            "enabled_this_cycle": recover_gaps and not getattr(args, "no_gap_recovery", False),
        },
        "gap_recovery_scope": "bounded_recent_small_interior_gaps_only; not_a_full_archive_completeness_check",
        "error_count": int(sum(1 for row in rows if row.get("error") or row.get("gap_recovery", {}).get("error"))),
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
            total_rows_recovered=report["total_rows_recovered"],
            gap_recovery_unresolved_minutes=report["gap_recovery_unresolved_minutes"],
        )
    print(json.dumps({
        "report": str(args.report),
        "pair_count": report["pair_count"],
        "total_rows_appended": report["total_rows_appended"],
        "total_rows_backfilled": report["total_rows_backfilled"],
        "total_rows_recovered": report["total_rows_recovered"],
        "gap_recovery_unresolved_minutes": report["gap_recovery_unresolved_minutes"],
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
        cycle_index = 0
        while True:
            cycle_started = time.monotonic()
            cycle_index += 1
            try:
                last_status = run_once(args, heartbeat=heartbeat, cycle_index=cycle_index)
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
