"""Bounded, read-only diagnostics for the unchanged rolling M1 contract.

No fetching, filling, source writes, or reinterpretation of old observations.
Wall-clock completion is not an exchange-open assertion. Gap classifications
other than missing_unknown require explicit, time-bound evidence supplied by
the caller. An omitted broker minute is not proof it can never be recovered.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re

import numpy as np

import oanda_rolling_technical_features_v1 as kernel
import oanda_rolling_technical_inputs_v1 as inputs

SCHEMA = "rolling_technical_continuity_v2_20260916"
MAX_ROWS = 8192
MAX_SPAN_MINUTES = 10080
MAX_EVIDENCE = 512
MAX_FEATURES = 512
GAP_KINDS = frozenset(("archive_observation_available",
    "broker_omitted_requested_minute", "market_closed"))


def _epoch(value, *, minute=False):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.integer, np.floating)):
        raise ValueError("finite_positive_epoch_required")
    if not math.isfinite(value) or value < 60 or value > 253402300740:
        raise ValueError("finite_positive_epoch_required")
    if minute and value % 60:
        raise ValueError("aligned_minute_required")
    return int(value) if minute else float(value)


def _bound(value, ceiling, name):
    if type(value) is not int or not 1 <= value <= ceiling:
        raise ValueError("bounded_" + name + "_required")
    return value


def _pair(value):
    if not isinstance(value, str) or re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", value) is None:
        raise ValueError("explicit_pair_required")
    return value


def _times(values, max_rows):
    _bound(max_rows, MAX_ROWS, "rows")
    # Refuse oversized input before allocating/converting a second array.
    if not hasattr(values, "__len__") or len(values) > max_rows:
        raise ValueError("bounded_source_rows_required")
    raw = np.asarray(values)
    if raw.ndim != 1 or (len(raw) and raw.dtype.kind not in "iuf"):
        raise ValueError("one_dimensional_numeric_minutes_required")
    if len(raw) and (np.any(~np.isfinite(raw)) or np.any(raw < 60) or
            np.any(raw > 253402300740) or np.any(raw % 60)):
        raise ValueError("aligned_source_minutes_required")
    result = raw.astype(np.int64)
    if np.any(np.diff(result) <= 0):
        raise ValueError("unique_sorted_source_minutes_required")
    return result


def latest_complete_boundary(observed_epoch):
    """Latest elapsed minute END; its preceding minute is complete by time.

    This is only a wall-clock eligibility boundary, not proof a provider row
    exists or a market was open. A candle starting at the boundary is immature.
    """
    return int(_epoch(observed_epoch) // 60) * 60


def _checked_evidence(evidence, pair, observed_epoch):
    if not isinstance(evidence, (list, tuple)) or len(evidence) > MAX_EVIDENCE:
        raise ValueError("bounded_gap_evidence_required")
    checked = []
    for row in evidence:
        if not isinstance(row, dict) or row.get("pair") != pair or row.get("kind") not in GAP_KINDS:
            raise ValueError("pair_bound_gap_evidence_required")
        start, end = (_epoch(row[k], minute=True) for k in ("start_epoch", "end_epoch"))
        if end < start or (end - start) // 60 >= MAX_SPAN_MINUTES:
            raise ValueError("bounded_gap_evidence_interval_required")
        available = _epoch(row["observed_epoch"])
        if available > observed_epoch or available < end + 60:
            raise ValueError("gap_evidence_clock_invalid")
        if not isinstance(row.get("evidence_ref"), str) or not 1 <= len(row["evidence_ref"]) <= 1024:
            raise ValueError("gap_evidence_reference_required")
        if re.fullmatch(r"[0-9a-f]{64}", str(row.get("evidence_sha256", ""))) is None:
            raise ValueError("gap_evidence_hash_required")
        if row["kind"] == "archive_observation_available":
            if start != end or re.fullmatch(r"[0-9a-f]{64}", str(row.get("row_value_sha256", ""))) is None:
                raise ValueError("individual_validated_archive_row_required")
        checked.append(dict(row, start_epoch=start, end_epoch=end))
    return checked


def summarize_continuity(times, pair, observed_epoch, *, range_truncated=False,
                         gap_evidence=(), max_rows=4096, max_span_minutes=2880):
    """Describe only the supplied completed tail and a bounded trailing clock.

    Interior gaps and the unobserved tail to the wall-clock boundary remain
    separate. No leading coverage before the first supplied row is inferred.
    Long missing intervals are clipped explicitly, never expanded unboundedly.
    """
    pair = _pair(pair)
    observed = _epoch(observed_epoch)
    span = _bound(max_span_minutes, MAX_SPAN_MINUTES, "span_minutes")
    if type(range_truncated) is not bool:
        raise ValueError("boolean_range_truncated_required")
    times = _times(times, max_rows)
    boundary = latest_complete_boundary(observed)
    if len(times) and times[-1] + 60 > observed:
        raise ValueError("completed_source_rows_required")
    evidence = _checked_evidence(gap_evidence, pair, observed)
    latest = int(times[-1]) if len(times) else None
    first = int(times[0]) if len(times) else None
    suffix = 0
    if len(times):
        breaks = np.flatnonzero(np.diff(times) != 60)
        suffix = len(times) - (int(breaks[-1]) + 1 if len(breaks) else 0)
    lower = max(first, boundary - span * 60) if first is not None else None
    expected_last = boundary - 60
    missing = []
    if lower is not None:
        present = set(int(t) for t in times if t >= lower)
        for minute in range(lower, boundary, 60):
            if minute in present:
                continue
            matches = [e for e in evidence if e["start_epoch"] <= minute <= e["end_epoch"]]
            kinds = set(e["kind"] for e in matches)
            kind = next(iter(kinds)) if len(kinds) == 1 else "evidence_conflict" if kinds else "missing_unknown"
            refs = sorted(set(e["evidence_ref"] for e in matches))
            hashes = sorted(set(e["evidence_sha256"] for e in matches))
            location = "trailing_unobserved" if minute > latest else "interior_gap"
            signature = (kind, location, refs, hashes)
            if missing and missing[-1]["end_epoch"] + 60 == minute and missing[-1]["signature"] == signature:
                missing[-1]["end_epoch"] = minute
                missing[-1]["minutes"] += 1
            else:
                missing.append(dict(start_epoch=minute, end_epoch=minute, minutes=1,
                    kind=kind, location=location, evidence_refs=refs, evidence_sha256=hashes,
                    signature=signature))
    counts = Counter()
    for interval in missing:
        counts[interval["kind"]] += interval["minutes"]
        interval.pop("signature")
    return dict(schema=SCHEMA, pair=pair, observed_epoch=observed,
        latest_complete_boundary_epoch=boundary, expected_latest_bar_start_epoch=expected_last,
        first_observed_bar_start_epoch=first, latest_observed_bar_start_epoch=latest,
        latest_observed_bar_end_epoch=latest + 60 if latest is not None else None,
        latest_completed_boundary_present=latest == expected_last,
        trailing_unobserved_minutes=(expected_last-latest)//60 if latest is not None else None,
        consecutive_suffix_rows=suffix,
        consecutive_suffix_start_epoch=int(times[-suffix]) if suffix else None,
        suffix_is_lower_bound=bool(suffix == len(times) and suffix and range_truncated),
        source_rows=len(times), source_range_truncated=range_truncated,
        diagnostic_start_epoch=lower, diagnostic_end_epoch=expected_last if first is not None else None,
        diagnostic_span_clipped=bool(first is not None and lower > first),
        missing_minutes_in_scope=sum(counts.values()), missing_minutes_by_kind=dict(counts),
        missing_intervals=missing,
        coverage_scope="supplied completed tail only; no leading/archive-wide coverage claim",
        evidence_scope="explicit caller evidence; broker omission is not permanent unavailability",
        market_calendar_inferred=False, synthetic_rows=0)


def _dependencies(name, family):
    """Source families only; values still come from the frozen V1 calculator."""
    if family == "calendar":
        return ()
    if family == "activity":
        return ("volume", "close") if name == "m1__return_activity_corr_60" else ("volume",)
    if family == "spread":
        return ("bid_close", "ask_close", "high", "low", "close") if name == "m1__movement_to_spread_14" else ("bid_close", "ask_close")
    if family == "ohlc":
        return ("open", "high", "low", "close")
    if name.startswith(("m1__atr_", "m1__range_to_atr_", "m1__momentum_", "m1__range_position_", "m1__cci_")):
        return ("high", "low", "close")
    return ("close",)


def classify_feature_support(data, feature_values, *, registry=None,
                             range_truncated=False, max_rows=4096):
    """Explain missing latest scalars/arrays without changing their values.

    For valid admitted prices, complete support plus nonfinite calculation is
    reported as undefined_or_numerical (e.g. zero variance), not a feed gap.
    Source-input absence is kept separate. It does not claim an exact algebraic
    denominator diagnosis or treat existing model weights as compatible.
    """
    times = _times(data["time"], max_rows)
    registry = kernel.feature_registry() if registry is None else registry
    if not isinstance(registry, (list, tuple)) or not 1 <= len(registry) <= MAX_FEATURES:
        raise ValueError("bounded_feature_registry_required")
    names = [r["name"] for r in registry]
    if len(set(names)) != len(names) or set(feature_values) != set(names):
        raise ValueError("exact_registered_features_required")
    source = {}
    for key in kernel.INPUT_COLUMNS[1:]:
        raw = data.get(key)
        if raw is None:
            source[key] = np.full(len(times), np.nan)
        else:
            if not hasattr(raw, "__len__") or len(raw) != len(times):
                raise ValueError("source_input_shape_mismatch:" + key)
            arr = np.asarray(raw, dtype=float)
            if arr.ndim != 1:
                raise ValueError("source_input_shape_mismatch:" + key)
            source[key] = arr
    rows, counts = {}, Counter()
    for definition in registry:
        name = definition["name"]
        lookback = _bound(definition["lookback_bars"], MAX_ROWS, "feature_lookback")
        raw = feature_values[name]
        if isinstance(raw, (list, tuple, np.ndarray)):
            if len(raw) != len(times) or np.asarray(raw).ndim != 1:
                raise ValueError("feature_array_shape_mismatch:" + name)
            value = raw[-1] if len(raw) else None
        else:
            value = raw
        finite = value is not None and not isinstance(value, (bool, np.bool_)) and math.isfinite(float(value))
        start = int(times[-1])-(lookback-1)*60 if len(times) else None
        reasons = []
        missing_minutes = 0
        invalid_inputs = []
        if not len(times):
            reasons.append("no_completed_observation")
        else:
            if start < times[0]:
                reasons.append("insufficient_bounded_history" if range_truncated else "insufficient_source_history")
            selected = times[times >= start]
            observed_span_start = max(start, int(times[0]))
            missing_minutes = (int(times[-1])-observed_span_start)//60+1-len(selected)
            if missing_minutes:
                reasons.append("missing_elapsed_support")
            left = int(np.searchsorted(times, start))
            for key in _dependencies(name, definition["family"]):
                values = source[key][left:]
                if np.any(~np.isfinite(values)) or np.any(values < 0 if key == "volume" else values <= 0):
                    invalid_inputs.append(key)
            if invalid_inputs:
                reasons.append("source_input_unavailable")
        if finite:
            kind = "finite_with_support_conflict" if reasons else "available"
        else:
            kind = reasons[0] if reasons else "undefined_or_numerical"
        counts[kind] += 1
        rows[name] = dict(category=kind, lookback_bars=lookback,
            required_start_epoch=start, missing_elapsed_minutes_in_observed_scope=missing_minutes,
            support_reasons=reasons, unavailable_source_inputs=invalid_inputs)
    return dict(feature_count=len(rows), counts=dict(counts), features=rows,
        scope="latest supplied row only; no feature filling or forecast eligibility")


def diagnose_pair(data, feature_values, pair, observed_epoch, *, source_receipt=None,
                  gap_evidence=(), max_rows=4096, max_span_minutes=2880):
    receipt = source_receipt or {}
    if receipt and (receipt.get("instrument") != pair or receipt.get("retained_rows") != len(data["time"])):
        raise ValueError("source_receipt_pair_and_rows_required")
    truncated = bool(receipt.get("range_truncated", False))
    return dict(schema=SCHEMA, pair=pair,
        continuity=summarize_continuity(data["time"], pair, observed_epoch,
            range_truncated=truncated, gap_evidence=gap_evidence,
            max_rows=max_rows, max_span_minutes=max_span_minutes),
        feature_support=classify_feature_support(data, feature_values,
            range_truncated=truncated, max_rows=max_rows),
        source_tail_sha256=receipt.get("source_tail_sha256"),
        source_read_completed_epoch=receipt.get("read_completed_epoch"))


def gap_evidence_from_receipt(receipt, pair, candle_epoch, observed_epoch, *, evidence_ref):
    """Validate one existing immutable updater response, without reading/fetching.

    A valid exact archived BAM observation is distinguished from a successful
    query that omitted it. Failed, future, corrupt or mismatched receipts raise.
    No returned evidence authorizes rewriting a source or historical feature.
    """
    pair = _pair(pair)
    at = _epoch(candle_epoch, minute=True)
    observed = _epoch(observed_epoch)
    if (not isinstance(receipt, dict) or
        receipt.get("schema_version") != "all68_m1_gap_recovery_v1" or
        receipt.get("instrument") != pair or receipt.get("candle_epoch") != at or
        receipt.get("granularity") != "M1" or receipt.get("price") != "BAM" or
        type(receipt.get("count")) is not int or not 1 <= receipt["count"] <= 10):
        raise ValueError("bound_gap_receipt_required")
    def clock(value):
        if not isinstance(value, str):
            raise ValueError("aware_receipt_clock_required")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("aware_receipt_clock_required")
        return parsed.timestamp()
    requested = clock(receipt["requested_utc"])
    available = clock(receipt["response_observed_utc"])
    if not at+60 <= requested <= available <= observed or clock(receipt["end_time_utc"]) != at+60:
        raise ValueError("gap_receipt_clock_invalid")
    response = receipt["response"]
    if not isinstance(response, dict):
        raise ValueError("structured_gap_response_required")
    encoded = json.dumps(response, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) > 256*1024 or hashlib.sha256(encoded).hexdigest() != receipt.get("response_sha256"):
        raise ValueError("gap_response_hash_invalid")
    if (response.get("_http_status") != 200 or response.get("_error") or
        response.get("instrument") != pair or response.get("granularity") != "M1"):
        raise ValueError("successful_pair_bound_response_required")
    candles = response.get("candles")
    if (not isinstance(candles, list) or len(candles) > 10 or
        any(not isinstance(row, dict) for row in candles)):
        raise ValueError("bounded_gap_response_required")
    clocks = [inputs._epoch(row["time"]) for row in candles]
    if any(b <= a for a, b in zip(clocks, clocks[1:])):
        raise ValueError("duplicate_or_unordered_archived_minutes")
    if any(t >= at+60 or t+60 > available for t in clocks):
        raise ValueError("archived_response_outside_request")
    if any(row.get("complete") is not True for row in candles):
        raise ValueError("complete_archived_response_required")
    matches = [row for row, stamp in zip(candles, clocks) if stamp == at]
    evidence = dict(pair=pair, start_epoch=at, end_epoch=at, observed_epoch=available,
        evidence_ref=evidence_ref, evidence_sha256=hashlib.sha256(json.dumps(receipt,
            sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
        kind="broker_omitted_requested_minute")
    if matches:
        candle = matches[0]
        if candle.get("complete") is not True:
            raise ValueError("complete_archived_minute_required")
        row = dict(time=candle["time"], instrument=pair, granularity="M1", complete=True,
                   volume=candle["volume"])
        for component, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
            if not isinstance(candle.get(component), dict):
                raise ValueError("actual_archived_bam_components_required")
            for key, column in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close")):
                row[prefix+column] = candle[component][key]
        _, normalized, _ = inputs._normalize(row, pair, available, None)
        evidence.update(kind="archive_observation_available", row_value_sha256=hashlib.sha256(
            json.dumps(normalized, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest())
    _checked_evidence([evidence], pair, observed)
    return evidence


def load_gap_evidence(candle_root, pair, missing_intervals, observed_epoch, *,
                     max_minutes=8, max_total_bytes=1024*1024):
    """Read only known names for at most two retained attempts per gap minute.

    No directory traversal, broker calls or fallback archive scan. Most recent
    missing minutes are examined first. Bounds, failed/missing evidence and the
    unexamined population are explicit; callers should cache immutable receipts
    or schedule this separately if needed to preserve their cycle budget.
    """
    pair = _pair(pair)
    observed = _epoch(observed_epoch)
    _bound(max_minutes, 64, "receipt_minutes")
    _bound(max_total_bytes, 4*1024*1024, "receipt_bytes")
    if not isinstance(missing_intervals, (list, tuple)) or len(missing_intervals) > MAX_SPAN_MINUTES:
        raise ValueError("bounded_missing_intervals_required")
    intervals = []
    prior = None
    total_minutes = 0
    for row in missing_intervals:
        left, right = (_epoch(row[key], minute=True) for key in ("start_epoch", "end_epoch"))
        if right < left or (prior is not None and left <= prior):
            raise ValueError("ordered_disjoint_missing_intervals_required")
        total_minutes += (right-left)//60+1
        if total_minutes > MAX_SPAN_MINUTES:
            raise ValueError("bounded_missing_population_required")
        prior = right
        intervals.append((left, right))
    selected = []
    for left, right in reversed(intervals):
        for at in range(right, left-60, -60):
            if len(selected) == max_minutes:
                break
            selected.append(at)
        if len(selected) == max_minutes:
            break
    root = Path(candle_root).absolute()/".gap_recovery_v1"/pair
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (root, *root.parents)):
        raise ValueError("linked_gap_receipts_refused")
    evidence, attempts = [], []
    consumed = 0
    for at in selected:
        for attempt in (1, 2):
            path = root/f"{at}.attempt{attempt}.observed.json"
            detail = dict(candle_epoch=at, attempt=attempt, path=str(path))
            try:
                if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
                    raise ValueError("linked_gap_receipt_refused")
                if not path.exists():
                    detail["status"] = "not_present"
                elif consumed >= max_total_bytes:
                    detail["status"] = "byte_budget_exhausted"
                else:
                    before = path.stat()
                    remaining = min(300*1024, max_total_bytes-consumed)
                    if before.st_size > remaining:
                        detail["status"] = "receipt_exceeds_remaining_byte_bound"
                    else:
                        with path.open("rb") as handle:
                            raw = handle.read(remaining+1)
                        consumed += len(raw)
                        after = path.stat()
                        if len(raw) > remaining or (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                            raise ValueError("gap_receipt_changed_or_oversized")
                        item = gap_evidence_from_receipt(json.loads(raw), pair, at, observed, evidence_ref=str(path))
                        item["evidence_sha256"] = hashlib.sha256(raw).hexdigest()
                        evidence.append(item)
                        detail.update(status="validated", kind=item["kind"], sha256=item["evidence_sha256"])
            except (OSError, ValueError, KeyError, TypeError, OverflowError) as exc:
                detail.update(status="invalid_or_unreadable", reason=type(exc).__name__+":"+str(exc)[:160])
            attempts.append(detail)
    return dict(evidence=evidence, attempts=attempts, bytes_read=consumed,
        selected_minutes=len(selected), missing_minutes_not_examined=total_minutes-len(selected),
        complete_within_requested_scope=total_minutes == len(selected) and not any(
            a["status"] not in ("not_present", "validated") for a in attempts),
        scope="named retained gap receipts only; absence is not a completed broker query")
