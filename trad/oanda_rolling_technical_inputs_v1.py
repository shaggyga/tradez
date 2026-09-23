"""Read-only, bounded candle adapters for the shared rolling technical kernel.

CSV and Parquet produce identical numeric fields. Candle timestamps are START
seconds; bar-end inference is kept separate from provider completion and actual
reader observation. These adapters never inspect future targets or remove a row
because its later outcome is missing. They do not fetch or modify source data.
"""
from __future__ import annotations

from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time

import numpy as np

SCHEMA = "rolling_technical_inputs_v1_20260915"
FIELDS = ("time", "open", "high", "low", "close", "bid_close", "ask_close", "volume")
MAX_TAIL_BYTES = 2 * 1024 * 1024
MAX_LINE_BYTES = 16384
MAX_BATCH_ROWS = 65536
MANIFEST_SCHEMA = "forex_historical_price_only_population_manifest_v1_20260912"


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json_bytes(value):
    def encode(item):
        if isinstance(item, datetime):
            return item.isoformat()
        if isinstance(item, np.generic):
            return item.item()
        raise TypeError("unsupported_source_value")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=encode,
                      allow_nan=False).encode("utf-8")


def _path(path):
    path = Path(path).absolute()
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction())
           for p in (path, *path.parents)):
        raise ValueError("linked_candle_source_refused")
    if not path.is_file():
        raise ValueError("candle_source_file_required")
    return path


def _stat(stat):
    return {"device": stat.st_dev, "inode": stat.st_ino,
            "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _identity(stat):
    return stat.st_dev, stat.st_ino


def _source_unchanged(path, handle, before, *, allow_append):
    after = path.stat()
    opened = os.fstat(handle.fileno())
    if _identity(after) != _identity(before) or _identity(opened) != _identity(before):
        raise ValueError("candle_source_replaced_during_read")
    if allow_append:
        if after.st_size < before.st_size or opened.st_size < before.st_size:
            raise ValueError("candle_source_truncated_during_read")
        if after.st_size == before.st_size and after.st_mtime_ns != before.st_mtime_ns:
            raise ValueError("candle_source_rewritten_during_read")
    elif _stat(after) != _stat(before) or _stat(opened) != _stat(before):
        raise ValueError("immutable_candle_source_changed")
    return after


def _scope(pair, observed_epoch, rows):
    if not isinstance(pair, str) or re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", pair) is None:
        raise ValueError("explicit_pair_required")
    if isinstance(observed_epoch, bool) or not isinstance(observed_epoch, (int, float)):
        raise ValueError("finite_observation_epoch_required")
    if not math.isfinite(observed_epoch) or observed_epoch <= 0:
        raise ValueError("finite_observation_epoch_required")
    if type(rows) is not int or not 1 <= rows <= MAX_BATCH_ROWS:
        raise ValueError("bounded_positive_row_count_required")


def _epoch(value):
    if isinstance(value, str):
        text = value.strip()
        fraction = re.search(r"\.(\d+)(?:Z|[+-]\d{2}:\d{2})$", text)
        if fraction and any(c != "0" for c in fraction.group(1)):
            raise ValueError("candle_clock_not_minute_aligned")
        try:
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("invalid_candle_clock") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("aware_original_candle_clock_required")
    result = value.timestamp()
    if not math.isfinite(result) or result <= 0 or result % 60 or getattr(value, "nanosecond", 0):
        raise ValueError("candle_clock_not_minute_aligned")
    return int(result)


def _number(value, name, *, positive=True):
    if isinstance(value, bool) or value is None or value == "":
        raise ValueError("invalid_candle_number:" + name)
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("invalid_candle_number:" + name) from exc
    if not math.isfinite(result) or (result <= 0 if positive else result < 0):
        raise ValueError("invalid_candle_number:" + name)
    return result


def _flag(value):
    if value is None:
        return None
    if value is True or value in ("true", "True", "1", 1):
        return True
    if value is False or value in ("false", "False", "0", 0):
        return False
    raise ValueError("invalid_explicit_completion_flag")


def _normalize(source, pair, observed_epoch, previous):
    if "instrument" in source and source["instrument"] != pair:
        raise ValueError("candle_pair_mismatch")
    if "granularity" in source and source["granularity"] != "M1":
        raise ValueError("candle_timeframe_mismatch")
    clocks = [_epoch(source[k]) for k in ("time", "datetime") if k in source]
    if not clocks or len(set(clocks)) != 1:
        raise ValueError("candle_clock_missing_or_conflicting")
    at = clocks[0]
    if previous is not None and at <= previous:
        raise ValueError("duplicate_or_unordered_candle_clock")
    complete = _flag(source.get("complete"))
    if complete is False:
        return at, None, "explicit_incomplete"
    if at + 60 > observed_epoch:
        return at, None, "bar_end_after_observation"
    values = {name: _number(source.get(name), name, positive=name != "volume")
              for name in FIELDS if name != "time"}
    for prefix in ("", "bid_", "ask_"):
        names = [prefix + x for x in ("open", "high", "low", "close")]
        if prefix and not any(k in source for k in names[:3]):
            continue
        part = {k: _number(source.get(k), k) for k in names}
        o, h, low, c = (part[k] for k in names)
        if not low <= min(o, c) <= max(o, c) <= h:
            raise ValueError("invalid_candle_ohlc_geometry:" + prefix)
    if not values["bid_close"] <= values["close"] <= values["ask_close"]:
        raise ValueError("invalid_bid_mid_ask_close_order")
    for field in ("open", "high", "low"):
        if "bid_" + field in source and "ask_" + field in source:
            if not _number(source["bid_" + field], field) <= values[field] <= _number(source["ask_" + field], field):
                raise ValueError("invalid_bid_mid_ask_ohlc_order:" + field)
    values["time"] = at
    return at, values, "explicit_complete" if complete else "inferred_bar_end"


def _arrays(rows):
    return {name: np.asarray([row[name] for row in rows],
                            dtype=np.int64 if name == "time" else np.float64)
            for name in FIELDS}


def _header(raw):
    if not raw.endswith(b"\n") or len(raw) > 4096:
        raise ValueError("bounded_complete_csv_header_required")
    columns = next(csv.reader([raw.decode("utf-8-sig").strip()], strict=True))
    if len(columns) != len(set(columns)):
        raise ValueError("duplicate_csv_columns")
    required = set(FIELDS) - {"time"}
    if not required.issubset(columns) or not {"time", "datetime"}.intersection(columns):
        raise ValueError("required_candle_columns_missing")
    return columns


def _csv_record(line, columns):
    if not line.endswith(b"\n") or len(line) > MAX_LINE_BYTES:
        raise ValueError("bounded_complete_csv_record_required")
    parsed = next(csv.reader([line.decode("utf-8-sig").rstrip("\r\n")], strict=True))
    if len(parsed) != len(columns):
        raise ValueError("csv_row_width_mismatch")
    return dict(zip(columns, parsed))


def _receipt(path, pair, before, observed_epoch, rows, hashes, bases, skipped, **extra):
    return {"schema_version": SCHEMA, "source_path": str(path), "instrument": pair,
            "granularity": "M1", "source_stat": _stat(before),
            "observed_epoch": float(observed_epoch), "read_completed_epoch": time.time(),
            "retained_rows": len(rows),
            "row_hashes": hashes, "row_value_hashes": [_sha(_json_bytes(r)) for r in rows],
            "completion_basis": bases, "completion_counts": dict(Counter(bases)),
            "skipped_rows": dict(skipped), "time_semantics": "original_candle_start_epoch_seconds",
            "bar_end_offset_seconds": 60,
            "availability_scope": "observed_epoch is the caller-supplied eligibility cutoff; read_completed_epoch is actual adapter read/validation completion. Historical bar_end availability is an assumption, not an original provider receipt.",
            "numeric_validation_scope": "Selected OHLC/volume and actual bid/ask values validated now; old five-column projection seals do not cover added fields.",
            **extra}


def read_csv_tail(path, pair, observed_epoch, max_rows=2048, *, max_bytes=MAX_TAIL_BYTES):
    """Read bounded complete records, retaining gaps and deferring a torn tail.

    The snapshot's exact bytes are reread before admission. Appends after the
    initial size are allowed; replacements, truncation and changed read ranges
    are refused. This is a range receipt, not a full-file revision guarantee.
    """
    _scope(pair, observed_epoch, max_rows)
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_TAIL_BYTES:
        raise ValueError("bounded_tail_bytes_required")
    path = _path(path)
    before = path.stat()
    with path.open("rb") as handle:
        if _identity(os.fstat(handle.fileno())) != _identity(before):
            raise ValueError("candle_source_replaced_before_open")
        header = handle.readline(4097)
        columns = _header(header)
        offset = max(len(header), before.st_size - max_bytes)
        handle.seek(offset)
        raw = handle.read(before.st_size - offset)
        if len(raw) != before.st_size - offset:
            raise ValueError("candle_source_short_read")
        handle.seek(0)
        repeated_header = handle.read(len(header))
        handle.seek(offset)
        if repeated_header != header or handle.read(len(raw)) != raw:
            raise ValueError("candle_read_range_changed")
        after = _source_unchanged(path, handle, before, allow_append=True)
    leading = 0
    selected = raw
    if offset > len(header):
        end = selected.find(b"\n")
        leading = len(selected) if end < 0 else end + 1
        selected = selected[leading:]
    last = selected.rfind(b"\n")
    trailing = len(selected) if last < 0 else len(selected) - last - 1
    selected = b"" if last < 0 else selected[:last + 1]
    lines = selected.splitlines(keepends=True)
    bounded = lines[-max_rows:]
    rows, hashes, bases, skipped = [], [], [], Counter()
    prior = None
    for line in bounded:
        prior, row, basis = _normalize(_csv_record(line, columns), pair, observed_epoch, prior)
        if row is None:
            skipped[basis] += 1
            continue
        rows.append(row); hashes.append(_sha(line)); bases.append(basis)
    receipt = _receipt(path, pair, before, observed_epoch, rows, hashes, bases, skipped,
        source_format="csv", source_tail_sha256=_sha(header + raw),
        header_sha256=_sha(header), source_range_start=offset,
        source_range_end=before.st_size, source_bytes_read=len(header) + len(raw),
        source_after_stat=_stat(after), original_row_hash_scope="exact_csv_record_bytes",
        deferred_partial_tail_bytes=trailing, discarded_leading_bytes=leading,
        rows_before_max_rows=len(lines), discarded_rows_before_max_rows=max(0, len(lines)-max_rows),
        range_truncated=offset > len(header) or len(lines) > max_rows)
    return _arrays(rows), receipt


def iter_csv(path, pair, observed_epoch, batch_rows=MAX_BATCH_ROWS):
    """Stream a fixed file extent; original CSV rows are never rewritten.

    A growing append-only file is supported, while a same-size rewrite or
    replacement aborts the traversal. Use per-row hashes to reconcile a later
    traversal after an atomic gap insertion. No cursor is only a timestamp.
    """
    _scope(pair, observed_epoch, batch_rows)
    path = _path(path); before = path.stat()
    with path.open("rb") as handle:
        header = handle.readline(4097); columns = _header(header)
        _source_unchanged(path, handle, before, allow_append=True)
        prior = None; ordinal = 0
        while handle.tell() < before.st_size:
            start = handle.tell(); rows, hashes, bases, skipped = [], [], [], Counter()
            digest = hashlib.sha256(); consumed = 0; trailing = 0
            first_ordinal = ordinal
            while consumed < batch_rows and handle.tell() < before.st_size:
                available = before.st_size - handle.tell()
                line = handle.readline(min(MAX_LINE_BYTES + 1, available))
                if not line:
                    raise ValueError("candle_source_short_read")
                digest.update(line)
                if not line.endswith(b"\n"):
                    if handle.tell() == before.st_size and len(line) <= MAX_LINE_BYTES:
                        trailing = len(line); break
                    raise ValueError("csv_record_exceeds_bound")
                prior, row, basis = _normalize(_csv_record(line, columns), pair, observed_epoch, prior)
                ordinal += 1; consumed += 1
                if row is None:
                    skipped[basis] += 1; continue
                rows.append(row); hashes.append(_sha(line)); bases.append(basis)
            after = _source_unchanged(path, handle, before, allow_append=True)
            if after.st_size == before.st_size and after.st_mtime_ns != before.st_mtime_ns:
                raise ValueError("csv_rewritten_during_traversal")
            yield _arrays(rows), _receipt(path, pair, before, observed_epoch, rows, hashes, bases, skipped,
                source_format="csv", original_row_hash_scope="exact_csv_record_bytes",
                source_range_sha256=digest.hexdigest(), header_sha256=_sha(header),
                source_range_start=start, source_range_end=handle.tell(),
                source_row_start=first_ordinal, source_row_end=ordinal,
                source_after_stat=_stat(after), deferred_partial_tail_bytes=trailing)


def iter_parquet(path, pair, observed_epoch, batch_rows=MAX_BATCH_ROWS):
    """Stream immutable original OHLC Parquet, without future-label eligibility.

    Parquet has no independent raw row bytes. Its row hash binds decoded source
    values; the source stat/footer bind the container. It is not a full-file
    cryptographic identity. Completed output publication must bind its own
    consumed projection and record an interrupted traversal as incomplete.
    """
    _scope(pair, observed_epoch, batch_rows)
    import pyarrow.parquet as pq
    path = _path(path); before = path.stat()
    with path.open("rb") as handle:
        reader = pq.ParquetFile(handle)
        names = reader.schema_arrow.names
        if len(names) != len(set(names)) or not (set(FIELDS)-{"time"}).issubset(names):
            raise ValueError("invalid_parquet_candle_columns")
        if not {"time", "datetime"}.intersection(names):
            raise ValueError("parquet_candle_clock_missing")
        footer = {"rows": reader.metadata.num_rows, "row_groups": reader.num_row_groups,
                  "schema_sha256": _sha(str(reader.schema_arrow).encode())}
        prior = None; ordinal = 0
        _source_unchanged(path, handle, before, allow_append=False)
        for batch in reader.iter_batches(batch_size=batch_rows, use_threads=False):
            start = ordinal; rows, hashes, bases, skipped = [], [], [], Counter()
            digest = hashlib.sha256()
            for source in batch.to_pylist():
                prior, row, basis = _normalize(source, pair, observed_epoch, prior)
                ordinal += 1
                if row is None:
                    skipped[basis] += 1; continue
                encoded = _json_bytes(source); digest.update(len(encoded).to_bytes(8, "big")); digest.update(encoded)
                rows.append(row); hashes.append(_sha(encoded)); bases.append(basis)
            _source_unchanged(path, handle, before, allow_append=False)
            yield _arrays(rows), _receipt(path, pair, before, observed_epoch, rows, hashes, bases, skipped,
                source_format="parquet", parquet_footer=footer,
                original_row_hash_scope="canonical_decoded_parquet_source_values_not_physical_row_bytes",
                consumed_projection_sha256=digest.hexdigest(), source_row_start=start, source_row_end=ordinal)
        _source_unchanged(path, handle, before, allow_append=False)


def discover_archive_sources(manifest_path):
    """Resolve sealed original OHLC locators; do not relabel five-column seals.

    The old source receipts are verified metadata. Original sources can since
    have grown or changed; returned paths are not newly byte-verified sources.
    Live rows after the sealed last clock form a distinct prospective cohort.
    """
    path = _path(manifest_path); raw = path.read_bytes()
    manifest = json.loads(raw)
    seal = json.loads(path.with_suffix(".seal.json").read_bytes())
    if _sha(raw) != seal.get("manifest_sha256") or manifest.get("schema") != MANIFEST_SCHEMA:
        raise ValueError("archive_manifest_seal_or_schema_mismatch")
    for name, expected in manifest.get("code_sha256", {}).items():
        if Path(name).name != name or _sha((path.parent/name).read_bytes()) != expected:
            raise ValueError("archive_manifest_code_binding_changed")
    result = {}
    for pair, recipe in manifest["pairs"].items():
        _scope(pair, 1, 1)
        sources = {}
        for lane in ("reacquired_m1_20260725", "canonical_c_m1"):
            receipt_path = Path(recipe["source_receipts"][lane])
            if not receipt_path.is_absolute():
                receipt_path = path.parent / receipt_path
            receipt_path = _path(receipt_path)
            receipt_raw = receipt_path.read_bytes()
            if _sha(receipt_raw) != recipe["source_receipt_sha256"][lane]:
                raise ValueError("archive_source_receipt_hash_mismatch")
            receipt = json.loads(receipt_raw)
            sources[lane] = {"path": receipt["source"]["path"],
                "receipt_path": str(receipt_path), "receipt_sha256": _sha(receipt_raw),
                "original_source_recorded_identity": receipt["source"],
                "original_source_recorded_sha256": receipt.get("source_full_sha256"),
                "old_projection_columns": receipt.get("retained_projection", {}).get("columns", []),
                "current_original_bytes_verified": False,
                "full_ohlc_validation": "required_now;not_covered_by_old_close_projection"}
        cutoff_ns = recipe["cutoff_ns"]
        if type(cutoff_ns) is not int or cutoff_ns % 60_000_000_000:
            raise ValueError("archive_cutoff_not_exact_minute")
        result[pair] = {"pair": pair, "reacquired_path": sources["reacquired_m1_20260725"]["path"],
            "canonical_path": sources["canonical_c_m1"]["path"], "cutoff_epoch": cutoff_ns//1_000_000_000,
            "sealed_first_utc": recipe.get("first_utc"), "sealed_last_utc": recipe.get("last_utc"),
            "sealed_raw_rows": recipe.get("raw_rows"), "manifest_path": str(path),
            "manifest_sha256": _sha(raw), "source_receipts": sources,
            "precedence": "reacquired through cutoff inclusive; canonical strictly afterward",
            "availability": "retrospective bar_end convention; original live receipts unknown"}
    return result
