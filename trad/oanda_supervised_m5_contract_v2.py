"""Clock and cache contract for the existing eight-feature M5 ridge arm.

Pure data operations only. This module never fits a model or starts a worker.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math

CONTRACT = "supervised_m5_contiguous_completed_bars_v2_20260912"
STEP_SECONDS = 300
MIN_SEGMENT_BARS = 60  # Preserve the existing per-series readiness requirement.


def _clock(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    parsed = parsed.astimezone(timezone.utc)
    if parsed.microsecond or parsed.timestamp() % STEP_SECONDS:
        return None
    return parsed


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _prices_valid(row):
    values = []
    for side, field in (("mid", "c"), ("mid", "h"), ("mid", "l"), ("bid", "c"), ("ask", "c")):
        part = row.get(side)
        if not isinstance(part, dict) or isinstance(part.get(field), bool):
            return False
        try:
            value = float(part.get(field))
        except (TypeError, ValueError):
            return False
        if not math.isfinite(value) or value <= 0:
            return False
        values.append(value)
    mid, high, low, bid, ask = values
    return low <= mid <= high and bid <= mid <= ask


def segment_candles(candles):
    """Retain ordered complete segments; never sort, fill, or bridge a gap."""
    segments, current = [], []
    quality = Counter()
    previous_clock = None
    last_valid_position = None
    fatal_order = False
    for position, row in enumerate(candles):
        stamp = _clock(row.get("time")) if isinstance(row, dict) else None
        reason = None
        if stamp is None:
            reason = "invalid_clock"
        elif previous_clock is not None and stamp <= previous_clock:
            reason = "duplicate_or_out_of_order_clock"
            fatal_order = True
        elif row.get("complete") is not True:
            reason = "not_explicitly_complete"
        elif not _prices_valid(row):
            reason = "invalid_price_or_spread"
        if stamp is not None:
            previous_clock = stamp
        if reason:
            quality[reason] += 1
            if current:
                segments.append(current)
                current = []
            continue
        if current and (stamp - current[-1][0]).total_seconds() != STEP_SECONDS:
            quality["clock_gap"] += 1
            segments.append(current)
            current = []
        # A normalized copy leaves all caller-owned candles untouched.
        normalized = dict(row)
        normalized["time"] = _iso(stamp)
        current.append((stamp, normalized))
        last_valid_position = position
    if current:
        segments.append(current)
    return {"segments": segments, "quality": dict(quality), "fatal_order": fatal_order,
            "latest_input_is_valid": bool(candles) and last_valid_position == len(candles) - 1}


def supervised_candle_data(candles, pip, contiguous_builder):
    """Reuse existing feature maths independently inside each valid segment."""
    if isinstance(pip, bool):
        return None
    try:
        pip = float(pip)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(pip) or pip <= 0 or len(candles) < MIN_SEGMENT_BARS:
        return None
    parsed = segment_candles(candles)
    segments = parsed["segments"]
    if parsed["fatal_order"] or not parsed["latest_input_is_valid"] or not segments:
        return None
    if len(segments[-1]) < MIN_SEGMENT_BARS:
        return None
    usable = [segment for segment in segments if len(segment) >= MIN_SEGMENT_BARS]
    built = [(segment, contiguous_builder([row for _, row in segment], pip)) for segment in usable]
    if not built or built[-1][1] is None:
        return None
    current = dict(built[-1][1])
    x, y, origins, targets = [], [], [], []
    for segment, data in built:
        if data is None:
            continue
        for features, label, stored_time in zip(data["x"], data["y"], data["times"], strict=True):
            start = _clock(stored_time)
            assert start is not None
            x.append(features)
            y.append(label)
            origins.append(_iso(start + timedelta(seconds=STEP_SECONDS)))
            targets.append(_iso(start + timedelta(seconds=2 * STEP_SECONDS)))
    current.update({"x": x, "y": y, "times": origins, "target_times": targets,
        "model_candle_time": _iso(segments[-1][-1][0]),
        "model_decision_time": _iso(segments[-1][-1][0] + timedelta(seconds=STEP_SECONDS)),
        "data_contract": CONTRACT,
        "data_quality": {**parsed["quality"], "input_rows": len(candles), "valid_segments": len(segments),
            "eligible_segments": len(usable), "current_segment_bars": len(segments[-1]), "training_rows": len(x)}})
    assert all(target <= current["model_decision_time"] for target in targets)
    return current


def input_fingerprint(candles, pip):
    """Bind every consumed field so a revision cannot reuse a stale fit."""
    def stable(value):
        if isinstance(value, float) and not math.isfinite(value):
            return {"invalid_number": str(value)}
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return {"invalid_type": type(value).__name__}
    rows = []
    for row in candles:
        if not isinstance(row, dict):
            rows.append({"invalid_row": type(row).__name__})
            continue
        selected = {"time": stable(row.get("time")), "complete": stable(row.get("complete"))}
        for side, fields in (("mid", ("h", "l", "c")), ("bid", ("c",)), ("ask", ("c",))):
            part = row.get(side)
            selected[side] = {field: stable(part.get(field)) for field in fields} if isinstance(part, dict) else None
        rows.append(selected)
    payload = {"contract": CONTRACT, "pip": stable(pip), "candles": rows}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
