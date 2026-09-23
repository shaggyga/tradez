"""Bounded, read-only primary OHLC ranges for rolling dataset construction.

Bounds are explicit half-open candle START clocks. Callers include their own
lookback and outcome extension; this reader never requires future outcomes to
admit an input row. It shares the pinned input adapter's selected-row validator.
No source candle, production configuration, database or fitted model is written.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import os
from pathlib import Path
import time

import numpy as np

import oanda_rolling_technical_inputs_v1 as inputs

SCHEMA = "rolling_technical_primary_ranges_v1_20260915"
MAX_ROWS = 1_000_000
MAX_RANGE_SECONDS = 180 * 86400
MAX_CSV_SOURCE_BYTES = 1024 * 1024 * 1024
_MINUTE_NS = 60_000_000_000
_INPUTS_SOURCE_SHA256 = inputs._sha(Path(inputs.__file__).read_bytes())


def _check_inputs_binding():
    if inputs._sha(Path(inputs.__file__).read_bytes()) != _INPUTS_SOURCE_SHA256:
        raise ValueError("range_inputs_source_changed_since_import")


def _clock(value, name):
    if type(value) is not int or value <= 0 or value % 60:
        raise ValueError("positive_minute_epoch_required:" + name)
    return value


def _arrays_hash(arrays):
    digest = hashlib.sha256()
    for name in inputs.FIELDS:
        data = np.asarray(arrays[name], dtype="<i8" if name == "time" else "<f8")
        digest.update(name.encode() + b"\0")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data.tobytes())
    return digest.hexdigest()


class _Collected:
    """Accumulate only selected numeric rows, with bounded temporary batches."""

    def __init__(self, max_rows, batch_rows):
        self.max_rows = max_rows
        self.batch_rows = batch_rows
        self.pending = []
        self.chunks = []
        self.rows = 0
        self.completion = Counter()
        self.raw_digest = hashlib.sha256()
        self.normalized_digest = hashlib.sha256()
        self.first = None
        self.last = None

    def add(self, row, basis, original):
        if self.rows >= self.max_rows:
            raise ValueError("range_materialization_row_cap_exceeded")
        if self.last is not None and row["time"] <= self.last:
            raise ValueError("primary_range_duplicate_or_unordered_clock")
        self.pending.append(row)
        self.rows += 1
        self.first = row["time"] if self.first is None else self.first
        self.last = row["time"]
        self.completion[basis] += 1
        normalized = inputs._json_bytes(row)
        for digest, encoded in ((self.raw_digest, original), (self.normalized_digest, normalized)):
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
        if len(self.pending) >= self.batch_rows:
            self.flush()

    def flush(self):
        if self.pending:
            self.chunks.append(inputs._arrays(self.pending))
            self.pending = []

    def finish(self):
        self.flush()
        if not self.chunks:
            arrays = inputs._arrays([])
        elif len(self.chunks) == 1:
            arrays = self.chunks[0]
        else:
            arrays = {name: np.concatenate([part[name] for part in self.chunks])
                      for name in inputs.FIELDS}
        self.chunks = []
        return arrays


def _check_source_name(path, pair, suffix):
    path = inputs._path(path)
    if path.name != pair + "_M1." + suffix:
        raise ValueError("explicit_primary_source_filename_mismatch")
    return path


def _clock_only(source):
    clocks = [inputs._epoch(source[k]) for k in ("time", "datetime") if k in source]
    if not clocks or len(set(clocks)) != 1:
        raise ValueError("candle_clock_missing_or_conflicting")
    return clocks[0]


def _read_csv(path, pair, start, end, observed, max_rows, batch_rows):
    path = _check_source_name(path, pair, "csv")
    before = path.stat()
    if before.st_size > MAX_CSV_SOURCE_BYTES:
        raise ValueError("csv_source_scan_byte_cap_exceeded")
    started = time.time()
    selected = _Collected(max_rows, batch_rows)
    skipped = Counter()
    scanned = 0
    previous = None
    trailing = 0
    prefix_digest = hashlib.sha256()
    with path.open("rb") as handle:
        if inputs._identity(os.fstat(handle.fileno())) != inputs._identity(before):
            raise ValueError("candle_source_replaced_before_open")
        header = handle.readline(4097)
        columns = inputs._header(header)
        prefix_digest.update(header)
        inputs._source_unchanged(path, handle, before, allow_append=True)
        while handle.tell() < before.st_size:
            available = before.st_size - handle.tell()
            line = handle.readline(min(inputs.MAX_LINE_BYTES + 1, available))
            if not line:
                raise ValueError("candle_source_short_read")
            prefix_digest.update(line)
            if not line.endswith(b"\n"):
                if handle.tell() == before.st_size and len(line) <= inputs.MAX_LINE_BYTES:
                    trailing = len(line)
                    break
                raise ValueError("csv_record_exceeds_bound")
            source = inputs._csv_record(line, columns)
            at = _clock_only(source)
            if previous is not None and at <= previous:
                raise ValueError("duplicate_or_unordered_candle_clock")
            previous = at
            scanned += 1
            if not start <= at < end:
                skipped["outside_primary_requested_range"] += 1
                continue
            _, row, basis = inputs._normalize(source, pair, observed, None)
            if row is None:
                skipped[basis] += 1
                continue
            selected.add(row, basis, line)
        # Re-hash exactly the captured prefix, so a simultaneous rewrite plus
        # append cannot evade the identity/mtime guard. New suffixes are deferred.
        inputs._source_unchanged(path, handle, before, allow_append=True)
        handle.seek(0)
        repeated = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = handle.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError("candle_source_short_read")
            repeated.update(chunk)
            remaining -= len(chunk)
        if repeated.digest() != prefix_digest.digest():
            raise ValueError("csv_captured_prefix_changed_during_read")
        after = inputs._source_unchanged(path, handle, before, allow_append=True)
    arrays = selected.finish()
    receipt = _source_receipt(path, before, after, pair, start, end, observed,
                              started, selected, skipped, arrays)
    receipt.update(source_format="csv", source_prefix_bytes=before.st_size,
                   source_prefix_sha256=prefix_digest.hexdigest(),
                   header_sha256=inputs._sha(header), scanned_clock_rows=scanned,
                   source_bytes_read=2 * before.st_size,
                   deferred_partial_tail_bytes=trailing,
                   deferred_appended_bytes=max(0, after.st_size-before.st_size),
                   original_row_hash_scope="exact_selected_csv_record_bytes",
                   nonselected_row_validation="CSV shape and ordered original clock aliases only; OHLC not decoded")
    return arrays, receipt


def _arrow_epochs(column):
    """Vectorized clock screening; selected rows still pass _normalize."""
    import pyarrow as pa
    import pyarrow.compute as pc
    if column.null_count:
        raise ValueError("candle_clock_missing_or_conflicting")
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        aware = pc.match_substring_regex(column, r"(?:Z|[+-][0-9]{2}:[0-9]{2})$")
        if not pc.all(aware).as_py():
            raise ValueError("aware_original_candle_clock_required")
    elif not pa.types.is_timestamp(column.type) or column.type.tz is None:
        raise ValueError("aware_original_candle_clock_required")
    try:
        clocks = pc.cast(column, pa.timestamp("ns", tz="UTC"), safe=True)
        nanoseconds = pc.cast(clocks, pa.int64(), safe=True).to_numpy(zero_copy_only=False)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ValueError("invalid_candle_clock") from exc
    if np.any(nanoseconds <= 0) or np.any(nanoseconds % _MINUTE_NS):
        raise ValueError("candle_clock_not_minute_aligned")
    return np.asarray(nanoseconds // 1_000_000_000, dtype=np.int64)


def _read_parquet(path, pair, start, end, observed, max_rows, batch_rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = _check_source_name(path, pair, "parquet")
    before = path.stat()
    started = time.time()
    selected = _Collected(max_rows, batch_rows)
    skipped = Counter()
    scanned = 0
    previous = None
    with path.open("rb") as handle:
        inputs._source_unchanged(path, handle, before, allow_append=False)
        reader = pq.ParquetFile(handle)
        names = reader.schema_arrow.names
        if len(names) != len(set(names)) or not (set(inputs.FIELDS)-{"time"}).issubset(names):
            raise ValueError("invalid_parquet_candle_columns")
        clock_names = [key for key in ("time", "datetime") if key in names]
        if not clock_names:
            raise ValueError("parquet_candle_clock_missing")
        footer = {"rows": reader.metadata.num_rows, "row_groups": reader.num_row_groups,
                  "schema_sha256": inputs._sha(str(reader.schema_arrow).encode())}
        for batch in reader.iter_batches(batch_size=batch_rows, use_threads=False):
            clocks = [_arrow_epochs(batch.column(name)) for name in clock_names]
            at = clocks[0]
            if any(not np.array_equal(at, value) for value in clocks[1:]):
                raise ValueError("candle_clock_missing_or_conflicting")
            if len(at):
                if ((previous is not None and at[0] <= previous) or np.any(np.diff(at) <= 0)):
                    raise ValueError("duplicate_or_unordered_candle_clock")
                previous = int(at[-1])
            mask = (at >= start) & (at < end)
            scanned += len(at)
            skipped["outside_primary_requested_range"] += int(len(at)-np.count_nonzero(mask))
            # Arrow filters before Python row decoding; rows outside the range
            # are not normalized or hashed as if their OHLC had been validated.
            chosen = batch.filter(pa.array(mask))
            for source in chosen.to_pylist():
                _, row, basis = inputs._normalize(source, pair, observed, None)
                if row is None:
                    skipped[basis] += 1
                    continue
                selected.add(row, basis, inputs._json_bytes(source))
            inputs._source_unchanged(path, handle, before, allow_append=False)
        after = inputs._source_unchanged(path, handle, before, allow_append=False)
    arrays = selected.finish()
    receipt = _source_receipt(path, before, after, pair, start, end, observed,
                              started, selected, skipped, arrays)
    receipt.update(source_format="parquet", parquet_footer=footer,
                   scanned_clock_rows=scanned,
                   original_row_hash_scope="canonical_decoded_selected_parquet_source_values_not_physical_row_bytes",
                   nonselected_row_validation="Ordered original clock aliases screened in Arrow; OHLC Python decoding limited to range",
                   full_source_bytes_cryptographically_verified=False)
    return arrays, receipt


def _source_receipt(path, before, after, pair, start, end, observed, started,
                    selected, skipped, arrays):
    return {"source_path": str(path), "instrument": pair,
            "source_stat": inputs._stat(before), "source_after_stat": inputs._stat(after),
            "requested_start_epoch": start, "requested_end_epoch_exclusive": end,
            "observed_epoch": float(observed), "read_started_epoch": started,
            "read_completed_epoch": time.time(), "original_first_observed_epoch": None,
            "retained_rows": selected.rows, "first_start_epoch": selected.first,
            "last_start_epoch": selected.last, "completion_counts": dict(selected.completion),
            "skipped_rows": dict(skipped),
            "selected_original_rows_sha256": selected.raw_digest.hexdigest(),
            "selected_normalized_rows_sha256": selected.normalized_digest.hexdigest(),
            "normalized_arrays_sha256": _arrays_hash(arrays),
            "selected_row_digest_framing": "SHA256 of each 8-byte-big-endian length followed by row bytes, in order"}


def read_range(pair, source_recipe, start_epoch, end_epoch, observed_epoch, *,
               max_rows=150_000, batch_rows=8192):
    """Return primary arrays and compact provenance for exact [start,end).

    Include 602 preceding elapsed minutes in start and the desired outcome
    extension in end at the caller. Empty ranges and gaps remain explicit.
    Memory is capped by max_rows (up to 1M), plus one input batch; final array
    concatenation may temporarily hold two copies. No full archive expansion.
    """
    inputs._scope(pair, observed_epoch, batch_rows)
    _check_inputs_binding()
    start = _clock(start_epoch, "start_epoch")
    end = _clock(end_epoch, "end_epoch")
    if end <= start or end-start > MAX_RANGE_SECONDS:
        raise ValueError("ordered_range_of_at_most_180_days_required")
    if type(max_rows) is not int or not 1 <= max_rows <= MAX_ROWS:
        raise ValueError("bounded_positive_materialization_row_cap_required")
    if not isinstance(source_recipe, dict) or source_recipe.get("pair") != pair:
        raise ValueError("matching_primary_source_recipe_required")
    cutoff = _clock(source_recipe.get("cutoff_epoch"), "cutoff_epoch")
    started = time.time()
    sources, chunks = [], []
    count = 0
    definitions = (("reacquired", start, min(end, cutoff+60), "reacquired_path", _read_parquet),
                   ("canonical", max(start, cutoff+60), end, "canonical_path", _read_csv))
    for lane, left, right, locator, read in definitions:
        if left >= right:
            sources.append({"lane": lane, "selected": False,
                            "reason": "outside_primary_requested_range"})
            continue
        arrays, receipt = read(source_recipe[locator], pair, left, right, observed_epoch,
                               max_rows-count, batch_rows)
        receipt.update(lane=lane, selected=True)
        sources.append(receipt)
        chunks.append(arrays)
        count += len(arrays["time"])
    arrays = {name: np.concatenate([chunk[name] for chunk in chunks])
              for name in inputs.FIELDS} if chunks else inputs._arrays([])
    clocks = arrays["time"]
    deltas = np.diff(clocks)
    if np.any(deltas <= 0):
        raise ValueError("primary_range_duplicate_or_unordered_clock")
    receipt = {"schema_version": SCHEMA, "pair": pair, "source_recipe_pair": source_recipe["pair"],
        "requested_start_epoch": start, "requested_end_epoch_exclusive": end,
        "requested_elapsed_minutes": (end-start)//60, "retained_rows": len(clocks),
        "observed_epoch": float(observed_epoch), "read_started_epoch": started,
        "read_completed_epoch": time.time(), "original_first_observed_epoch": None,
        "first_start_epoch": int(clocks[0]) if len(clocks) else None,
        "last_start_epoch": int(clocks[-1]) if len(clocks) else None,
        "internal_gaps": int(np.count_nonzero(deltas > 60)),
        "internal_missing_minutes": int(np.sum(deltas[deltas > 60]//60-1)),
        "unobserved_requested_minutes": (end-start)//60-len(clocks),
        "max_rows": max_rows, "batch_rows": batch_rows,
        "normalized_arrays_sha256": _arrays_hash(arrays), "sources": sources,
        "source_recipe_manifest_path": source_recipe.get("manifest_path"),
        "source_recipe_manifest_sha256": source_recipe.get("manifest_sha256"),
        "source_receipt_references": source_recipe.get("source_receipts", {}),
        "cutoff_epoch": cutoff,
        "precedence": "reacquired candle starts through cutoff inclusive; canonical strictly afterward",
        "bounds": "explicit half-open candle START clocks; caller supplies context and target extension",
        "future_outcome_eligibility_filter": False,
        "availability": "Historical bar-end availability is assumed; actual read clocks are recorded, not original provider receipt times",
        "validation_scope": "Selected original OHLC/volume/bid/ask rows validated through the pinned input adapter; source metadata seals do not establish full current OHLC validity",
        "inputs_source_sha256": _INPUTS_SOURCE_SHA256}
    _check_inputs_binding()
    return arrays, receipt
