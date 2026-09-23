"""Pure contracts for deterministic exact-window M1 replay source packs.

This module never imports broker, account, authorization, lifecycle, or live
runtime code.  It turns already-retained OANDA practice bid/ask candle files
into content-addressed historical training slices while preserving missing
minutes as explicit coverage failures.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from src.forex_system.research.sequential_deterministic_io_v1 import canonical_gzip


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_hash(*parts: Any) -> str:
    payload = parts[0] if len(parts) == 1 else list(parts)
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def parse_epoch(value: Any) -> int:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, tail = text.split(".", 1)
        offset = ""
        if "+" in tail:
            fraction, offset = tail.split("+", 1)
            offset = "+" + offset
        elif tail.count("-"):
            fraction, offset = tail.rsplit("-", 1)
            offset = "-" + offset
        else:
            fraction = tail
        text = head + "." + fraction[:6] + offset
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def iso_utc(epoch: int) -> str:
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def deterministic_gzip(raw: bytes, compresslevel: int = 9) -> bytes:
    return canonical_gzip(raw, compresslevel=int(compresslevel))


def is_link(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0)) if path.exists() or path.is_symlink() else 0
        return path.is_symlink() or bool(os.path.islink(path)) or bool(attributes & 0x400)
    except OSError:
        return True


@dataclass(frozen=True)
class ParsedSource:
    instrument: str
    path: Path
    header: bytes
    rows_by_epoch: Mapping[int, bytes]
    source_sha256: str
    source_bytes: int
    first_epoch: int
    last_epoch: int


@dataclass(frozen=True)
class PackedSlice:
    instrument: str
    session_key: str
    raw_bytes: bytes
    raw_sha256: str
    gzip_bytes: bytes
    gzip_sha256: str
    manifest: Mapping[str, Any]


def schedule_clocks(start_epoch: int, end_epoch: int, cadence_min: int) -> tuple[int, ...]:
    cadence = max(1, int(cadence_min)) * 60
    first = ((int(start_epoch) + cadence - 1) // cadence) * cadence
    return tuple(range(first, int(end_epoch), cadence))


def required_epoch_bounds(
    *,
    start_epoch: int,
    end_epoch: int,
    feature_lookback_min: int,
    feedback_horizon_min: int,
) -> tuple[int, int]:
    # The decision at t uses the completed candle t-1 and a lookback endpoint
    # t-1-lookback, so the slice begins lookback+1 minutes before t.
    first = int(start_epoch) - (int(feature_lookback_min) + 1) * 60
    last = int(end_epoch) + max(0, int(feedback_horizon_min) - 5) * 60
    return first, last


def _parse_csv_line(raw_line: bytes) -> list[str]:
    text = raw_line.decode("utf-8")
    return next(csv.reader([text]))


def read_source(
    path: Path,
    *,
    instrument: str,
    required_columns: Sequence[str],
    maximum_bytes: int,
    reject_links: bool = True,
) -> ParsedSource:
    resolved = path.resolve()
    if not path.is_file():
        raise ValueError(f"missing source file: {path}")
    if reject_links and is_link(path):
        raise ValueError(f"linked source file rejected: {path}")
    size = int(path.stat().st_size)
    if size <= 0 or size > int(maximum_bytes):
        raise ValueError(f"source size outside contract: {path}:{size}")
    payload = path.read_bytes()
    if len(payload) != size:
        raise ValueError(f"short source read: {path}")
    lines = payload.splitlines()
    if len(lines) < 2:
        raise ValueError(f"source has no data rows: {path}")
    header_fields = _parse_csv_line(lines[0])
    if header_fields != list(required_columns):
        raise ValueError(f"source header mismatch: {path}")
    time_index = header_fields.index("time")
    instrument_index = header_fields.index("instrument")
    granularity_index = header_fields.index("granularity")
    rows: dict[int, bytes] = {}
    previous: int | None = None
    for raw in lines[1:]:
        if not raw.strip():
            continue
        values = _parse_csv_line(raw)
        if len(values) != len(header_fields):
            raise ValueError(f"source row width mismatch: {path}")
        if values[instrument_index] != instrument or values[granularity_index] != "M1":
            raise ValueError(f"source identity mismatch: {path}")
        epoch = parse_epoch(values[time_index])
        if previous is not None and epoch <= previous:
            raise ValueError(f"source epochs not strictly increasing: {path}")
        previous = epoch
        rows[epoch] = raw + b"\n"
    if not rows:
        raise ValueError(f"source has no parsed rows: {path}")
    ordered = tuple(rows)
    return ParsedSource(
        instrument=instrument,
        path=resolved,
        header=lines[0] + b"\n",
        rows_by_epoch=rows,
        source_sha256=sha256_bytes(payload),
        source_bytes=size,
        first_epoch=int(ordered[0]),
        last_epoch=int(ordered[-1]),
    )


def _context_epochs(decision_epoch: int, lookback_min: int) -> range:
    return range(
        int(decision_epoch) - (int(lookback_min) + 1) * 60,
        int(decision_epoch),
        60,
    )


def build_packed_slice(
    source: ParsedSource,
    *,
    session: Mapping[str, Any],
    schedule: Mapping[str, Any],
    compresslevel: int,
    maximum_archive_raw_bytes: int,
) -> PackedSlice:
    session_key = str(session["session_key"])
    start_epoch = parse_epoch(session["start_utc"])
    end_epoch = parse_epoch(session["end_utc_exclusive"])
    cadence_min = int(schedule["decision_cadence_min"])
    lookback_min = int(schedule["feature_lookback_min"])
    execution_delay_min = int(schedule["execution_delay_min"])
    feedback_horizon_min = int(schedule["feedback_horizon_min"])
    clocks = schedule_clocks(start_epoch, end_epoch, cadence_min)
    slice_first, slice_last = required_epoch_bounds(
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        feature_lookback_min=lookback_min,
        feedback_horizon_min=feedback_horizon_min,
    )
    rows = source.rows_by_epoch
    present_epochs = tuple(epoch for epoch in rows if slice_first <= epoch <= slice_last)
    raw = source.header + b"".join(rows[epoch] for epoch in present_epochs)
    if len(raw) > int(maximum_archive_raw_bytes):
        raise ValueError(f"exact-window archive exceeds limit: {source.instrument}:{session_key}")
    context_ready = 0
    execution_ready = 0
    feedback_ready = 0
    fully_ready = 0
    failure_rows: list[dict[str, Any]] = []
    for clock in clocks:
        missing_context = tuple(epoch for epoch in _context_epochs(clock, lookback_min) if epoch not in rows)
        execution_epoch = int(clock) + execution_delay_min * 60
        feedback_epoch = int(clock) + feedback_horizon_min * 60
        has_context = not missing_context
        has_execution = execution_epoch in rows
        has_feedback = feedback_epoch in rows
        context_ready += int(has_context)
        execution_ready += int(has_execution)
        feedback_ready += int(has_feedback)
        fully_ready += int(has_context and has_execution and has_feedback)
        if not (has_context and has_execution and has_feedback):
            failure_rows.append(
                {
                    "decision_epoch": int(clock),
                    "missing_context_count": len(missing_context),
                    "missing_context_sha256": stable_hash(missing_context),
                    "execution_quote_ready": has_execution,
                    "feedback_quote_ready": has_feedback,
                }
            )
    expected_minutes = tuple(range(slice_first, slice_last + 60, 60))
    missing_minutes = tuple(epoch for epoch in expected_minutes if epoch not in rows)
    raw_sha = sha256_bytes(raw)
    compressed = deterministic_gzip(raw, compresslevel)
    manifest = {
        "instrument": source.instrument,
        "session_key": session_key,
        "session_start_epoch": start_epoch,
        "session_end_epoch": end_epoch,
        "slice_first_epoch": slice_first,
        "slice_last_epoch": slice_last,
        "scheduled_clock_count": len(clocks),
        "scheduled_clock_sha256": stable_hash(clocks),
        "context_ready_clock_count": context_ready,
        "execution_ready_clock_count": execution_ready,
        "feedback_ready_clock_count": feedback_ready,
        "fully_ready_clock_count": fully_ready,
        "coverage_failure_count": len(failure_rows),
        "coverage_failures_sha256": stable_hash(failure_rows),
        "expected_minute_count": len(expected_minutes),
        "present_minute_count": len(present_epochs),
        "missing_minute_count": len(missing_minutes),
        "missing_minutes_sha256": stable_hash(missing_minutes),
        "source_file_sha256_at_capture": source.source_sha256,
        "source_file_bytes_at_capture": source.source_bytes,
        "source_first_epoch": source.first_epoch,
        "source_last_epoch": source.last_epoch,
        "raw_bytes": len(raw),
        "raw_sha256": raw_sha,
        "gzip_bytes": len(compressed),
        "gzip_sha256": sha256_bytes(compressed),
        "coverage_failures": failure_rows,
    }
    return PackedSlice(
        instrument=source.instrument,
        session_key=session_key,
        raw_bytes=raw,
        raw_sha256=raw_sha,
        gzip_bytes=compressed,
        gzip_sha256=sha256_bytes(compressed),
        manifest=manifest,
    )


def validate_config(config: Mapping[str, Any]) -> None:
    required_false = (
        "execution_eligible",
        "proof_eligible",
        "can_promote",
        "can_place_orders",
        "can_authorize",
        "broker_access",
        "account_access",
    )
    if config.get("research_only") is not True:
        raise ValueError("source pack must be research-only")
    if any(config.get(key) is not False for key in required_false):
        raise ValueError("source pack isolation mismatch")
    if config.get("supported_decision") != "no_trade":
        raise ValueError("source pack supports no_trade only")
    schedule = config["schedule"]
    if schedule.get("schedule_clocks_before_quote_coverage") is not True:
        raise ValueError("clocks must be scheduled before coverage inspection")
    if int(schedule.get("decision_cadence_min") or 0) != 5:
        raise ValueError("V1 source pack cadence is five minutes")
    instruments = [str(value) for value in config["source"]["expected_instruments"]]
    if len(instruments) != 68 or len(set(instruments)) != 68 or instruments != sorted(instruments):
        raise ValueError("exact sorted 68-instrument universe required")
    sessions = list(schedule.get("sessions") or [])
    keys = [str(row.get("session_key") or "") for row in sessions]
    if len(sessions) < 2 or len(set(keys)) != len(keys):
        raise ValueError("multiple uniquely named deterministic sessions required")
    for row in sessions:
        if parse_epoch(row["start_utc"]) >= parse_epoch(row["end_utc_exclusive"]):
            raise ValueError("invalid session interval")
    storage = config["storage"]
    maximum_raw = int(storage.get("maximum_archive_raw_bytes") or 0)
    maximum_gzip = int(storage.get("maximum_archive_gzip_bytes") or 0)
    if maximum_gzip <= 0 or maximum_raw <= 0 or maximum_gzip > maximum_raw:
        raise ValueError("bounded compressed and raw archive limits required")


__all__ = [
    "PackedSlice",
    "ParsedSource",
    "build_packed_slice",
    "canonical_json",
    "deterministic_gzip",
    "iso_utc",
    "parse_epoch",
    "read_source",
    "required_epoch_bounds",
    "schedule_clocks",
    "sha256_bytes",
    "stable_hash",
    "validate_config",
]
