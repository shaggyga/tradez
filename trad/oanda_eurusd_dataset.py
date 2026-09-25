#!/usr/bin/env python3
"""Export a validated, finite snapshot of recorded OANDA EUR_USD quote segments."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile


FIELDS = [
    "recording_id", "segment_id", "connection_id", "sequence", "instrument",
    "broker_time", "received_time", "bid", "ask", "mid", "spread", "spread_pips",
    "tradeable", "initial_snapshot", "timestamp_to_receipt_ms",
]
PRICE_FIELDS = ["bid", "ask", "mid", "spread", "spread_pips"]
TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z")
ACTIVE_STATES = {"starting", "connecting", "recording", "reconnecting"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def aware_time(value):
    """Validate RFC3339 without rewriting or truncating its original CSV text."""
    if not TIME_RE.fullmatch(value):
        raise ValueError("timestamp must be timezone-aware RFC3339 with at most nanosecond precision")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("timestamp has no UTC offset")
    return parsed


def read_json(path, required=True):
    if not path.exists() and not required:
        return {}, None
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{path.name}: expected a JSON object")
    return value, sha256(raw)


def capture_prefix(path):
    """Read only the byte length observed on open, never chase an appending writer."""
    started = utc_now()
    with path.open("rb") as source:
        before = os.fstat(source.fileno())
        raw = source.read(before.st_size)
        after = os.fstat(source.fileno())
    if len(raw) != before.st_size or after.st_size < before.st_size:
        raise ValueError(f"{path.name}: source shrank during snapshot; retry after inspection")
    boundary = raw.rfind(b"\n") + 1
    complete, tail = raw[:boundary], raw[boundary:]
    metadata = {
        "file": path.name,
        "capture_started_utc": started,
        "capture_finished_utc": utc_now(),
        "captured_bytes": len(raw),
        "captured_bytes_sha256": sha256(raw),
        "complete_bytes": len(complete),
        "complete_bytes_sha256": sha256(complete),
        "ignored_unterminated_tail_bytes": len(tail),
        "ignored_unterminated_tail_fragments": int(bool(tail)),
        "size_observed_after_read": after.st_size,
    }
    return complete, metadata


def segment_order(directory, paths):
    """Use the recorder's append-only start journal for actual restart order."""
    journal = directory / "events.jsonl"
    if not journal.exists():
        if len(paths) != 1:
            raise ValueError("Multiple segments require events.jsonl to establish recording order")
        return paths, {"method": "single_segment", "journal": None}
    complete, evidence = capture_prefix(journal)
    wanted = {path.name: path for path in paths}
    ordered = []
    seen = set()
    for number, line in enumerate(complete.splitlines(), 1):
        try:
            entry = json.loads(line)
            if not isinstance(entry, dict):
                raise ValueError("event must be an object")
            if entry.get("event") != "segment_started":
                continue
            name = entry.get("csv_file")
            if name not in wanted:
                continue
            if name in seen:
                raise ValueError("duplicate segment_started")
            seen.add(name)
            ordered.append(wanted[name])
        except (ValueError, TypeError) as error:
            raise ValueError(f"events.jsonl line {number}: {error}") from error
    if len(ordered) != len(paths):
        raise ValueError("A segment has no complete segment_started event; retry the snapshot")
    return ordered, {"method": "events.jsonl segment_started order", "journal": evidence}


def validate_row(row, number, expected_segment=None, expected_recording=None):
    if len(row) != len(FIELDS):
        raise ValueError(f"row {number}: expected {len(FIELDS)} columns, found {len(row)}")
    item = dict(zip(FIELDS, row))
    if item["instrument"] != "EUR_USD":
        raise ValueError(f"row {number}: unexpected instrument")
    if not item["recording_id"] or not item["segment_id"]:
        raise ValueError(f"row {number}: missing recording or segment identity")
    if expected_segment is not None and item["segment_id"] != expected_segment:
        raise ValueError(f"row {number}: segment identity differs from filename")
    if expected_recording is not None and item["recording_id"] != expected_recording:
        raise ValueError(f"row {number}: recording identity differs from contract")
    for field in ("sequence", "connection_id"):
        if not item[field].isdigit() or int(item[field]) < 1:
            raise ValueError(f"row {number}: {field} must be a positive integer")
    for field in ("broker_time", "received_time"):
        aware_time(item[field])
    for field in ("tradeable", "initial_snapshot"):
        if item[field] not in ("True", "False"):
            raise ValueError(f"row {number}: invalid {field}")
    try:
        values = {field: Decimal(item[field]) for field in PRICE_FIELDS + ["timestamp_to_receipt_ms"]}
    except InvalidOperation as error:
        raise ValueError(f"row {number}: invalid decimal") from error
    if any(not value.is_finite() for value in values.values()):
        raise ValueError(f"row {number}: nonfinite decimal")
    bid, ask = values["bid"], values["ask"]
    if bid <= 0 or ask <= 0 or bid > ask:
        raise ValueError(f"row {number}: require 0 < bid <= ask")
    with localcontext() as context:
        context.prec = max(50, sum(len(value.as_tuple().digits) + abs(value.as_tuple().exponent)
                                   for value in values.values()) + 10)
        spread = ask - bid
        if values["mid"] != (bid + ask) / 2:
            raise ValueError(f"row {number}: midpoint differs from exact bid/ask arithmetic")
        if values["spread"] != spread or values["spread_pips"] != spread / Decimal("0.0001"):
            raise ValueError(f"row {number}: spread differs from exact bid/ask arithmetic")
    return item


def parse_csv(complete, filename, expected_segment=None, expected_recording=None):
    try:
        reader = csv.reader(io.StringIO(complete.decode("utf-8"), newline=""), strict=True)
        header = next(reader, None)
        if header != FIELDS:
            raise ValueError("missing or unexpected header")
        rows = []
        previous = {}
        for number, row in enumerate(reader, 2):
            item = validate_row(row, number, expected_segment, expected_recording)
            identity = (item["recording_id"], item["segment_id"])
            sequence = int(item["sequence"])
            if sequence <= previous.get(identity, 0):
                raise ValueError(f"row {number}: sequence is not strictly increasing within segment")
            previous[identity] = sequence
            rows.append(item)
        return rows
    except (ValueError, UnicodeError, csv.Error) as error:
        raise ValueError(f"{filename}: {error}") from error


def snapshot(directory):
    directory = Path(directory).resolve()
    started = utc_now()
    contract, contract_hash = read_json(directory / "recording.json")
    if contract.get("instrument") != "EUR_USD" or contract.get("columns") != FIELDS:
        raise ValueError("recording.json: unexpected instrument or schema")
    if not contract.get("source") or not contract.get("recording_id"):
        raise ValueError("recording.json: missing source or recording identity")
    cutoff = aware_time(contract["end_utc"])
    before_status, before_status_hash = read_json(directory / "status.json", required=False)
    paths = sorted(directory.glob("quotes-*.csv"))
    if not paths:
        raise ValueError("No quotes-*.csv segments exist yet")
    paths, ordering = segment_order(directory, paths)
    rows, segments = [], []
    for index, path in enumerate(paths):
        complete, evidence = capture_prefix(path)
        segment_rows = parse_csv(complete, path.name, path.stem[len("quotes-"):], contract["recording_id"])
        evidence.update(segment_order=index, row_count=len(segment_rows),
                        first_sequence=segment_rows[0]["sequence"] if segment_rows else None,
                        last_sequence=segment_rows[-1]["sequence"] if segment_rows else None)
        segments.append(evidence)
        rows.extend(segment_rows)
    status, status_hash = read_json(directory / "status.json", required=False)
    finished = utc_now()
    terminal = status.get("state") in {"cutoff_reached", "user_stopped"} and status.get("pid") is None
    final = bool(terminal and before_status_hash == status_hash and
                 not any(item["ignored_unterminated_tail_bytes"] for item in segments))
    past_cutoff = aware_time(finished) >= cutoff
    state = ("final_recorder_closed" if final else
             "snapshot_past_cutoff_completion_unconfirmed" if past_cutoff else
             "active_snapshot" if status.get("state") in ACTIVE_STATES else "snapshot_completion_unconfirmed")
    endpoints = {f"{edge}_{field}": rows[position][field] if rows else None
                 for edge, position in (("first", 0), ("last", -1))
                 for field in ("broker_time", "received_time")}
    summary = {
        "schema": "oanda_eurusd_dataset_snapshot.v1",
        "source": contract["source"], "instrument": contract["instrument"],
        "recording_id": contract["recording_id"], "columns": FIELDS,
        "row_count": len(rows), **endpoints,
        "snapshot_started_utc": started, "snapshot_finished_utc": finished,
        "asof_semantics": "Each segment is a bounded prefix captured at its own recorded time; not a simultaneous market snapshot.",
        "input_directory": str(directory), "recording_contract_sha256": contract_hash,
        "scheduled_end_utc": contract["end_utc"], "snapshot_state": state,
        "dataset_is_final": final, "cutoff_time_has_passed": past_cutoff,
        "recorder_reported_state": status.get("state", "unavailable"),
        "recorder_status_updated_utc": status.get("updated_utc"),
        "recorder_status_sha256_before": before_status_hash,
        "recorder_status_sha256_after": status_hash,
        "recorder_status_changed_during_snapshot": before_status != status,
        "live_process_or_network_verified": False,
        "ordering": ordering, "segments": segments,
        "ignored_unterminated_tail_bytes": sum(item["ignored_unterminated_tail_bytes"] for item in segments),
        "ignored_unterminated_tail_fragments": sum(item["ignored_unterminated_tail_fragments"] for item in segments),
        "partial_tail_policy": "Only newline-terminated CSV records are included. Tail fragments are counted as bytes/fragments, not claimed to be complete rows. Malformed complete records refuse export.",
        "transformations": {"deduplication": False, "imputation": False, "resampling": False,
                            "prices": "Original decimal strings", "timestamps": "Original timezone-aware strings; broker nanoseconds preserved"},
        "sampling": {key: contract.get(key) for key in
                     ("maximum_quotes_per_second", "sampling_window_ms", "regular_grid", "historical_backfill")},
    }
    return rows, summary


def atomic_write(path, data):
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def export(directory):
    directory = Path(directory).resolve()
    rows, summary = snapshot(directory)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=FIELDS, lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    data = buffer.getvalue().encode("utf-8")
    output = directory / "eurusd_bid_ask.csv"
    summary["export"] = {"file": output.name, "bytes": len(data), "sha256": sha256(data)}
    # Each replace is atomic. The summary's CSV hash binds the two files; callers
    # reading while another export runs should retry if that hash does not match.
    atomic_write(output, data)
    atomic_write(directory / "DATASET_SUMMARY.json", (json.dumps(summary, indent=2) + "\n").encode("utf-8"))
    return summary


def load_dataframe(path, prices_as_float=True):
    """Load a fresh directory snapshot or exported CSV; pandas is optional until called."""
    import pandas as pd

    path = Path(path)
    if path.is_dir():
        rows, summary = snapshot(path)
    else:
        raw = path.read_bytes()
        if raw and not raw.endswith(b"\n"):
            raise ValueError("Exported CSV has an unterminated record")
        rows = parse_csv(raw, path.name)
        summary = {"export_file": str(path.resolve()), "sha256": sha256(raw), "row_count": len(rows)}
    frame = pd.DataFrame(rows, columns=FIELDS)
    for field in ("broker_time", "received_time"):
        frame[field + "_raw"] = frame[field]
        frame[field] = pd.to_datetime(frame[field], utc=True, format="ISO8601").astype("datetime64[ns, UTC]")
    if prices_as_float:
        for field in PRICE_FIELDS:
            frame[field] = frame[field].astype(float)
    for field in ("sequence", "connection_id"):
        frame[field] = frame[field].astype("int64")
    for field in ("tradeable", "initial_snapshot"):
        frame[field] = frame[field].map({"True": True, "False": False}).astype(bool)
    frame.attrs["dataset_summary"] = summary
    return frame


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("export", help="atomically refresh CSV and hash-bound summary")
    command.add_argument("--input", required=True, type=Path)
    args = parser.parse_args()
    try:
        summary = export(args.input)
    except (ValueError, OSError, KeyError) as error:
        parser.exit(2, f"Dataset export refused: {error}\n")
    print(json.dumps({key: summary[key] for key in
                      ("row_count", "snapshot_state", "dataset_is_final", "export")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
