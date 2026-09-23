"""Completed research candles with explicit weekend and intraday-gap semantics.

No network or model imports. Native bars remain at their original UTC clocks.
The scheduled closure is Friday 17:00 through Sunday 17:00 America/New_York;
other absences are data gaps. M1 MA initialization always uses a contiguous
elapsed-minute suffix. Higher-timeframe histories may cross scheduled weekends.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import hashlib
import io
import math
from pathlib import Path
from zoneinfo import ZoneInfo

SCHEMA = "feature_candle_inputs_v2_20260913_weekend_state"
CALENDAR = "fx_weekend_ny_friday_1700_sunday_1700_v1"
SECONDS = {"M1": 60, "M5": 300, "H1": 3600}
MAX_ROWS = 1024
MAX_BYTES = 512 * 1024
NY = ZoneInfo("America/New_York")


def stamp(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("candle_timezone_required")
    return result.astimezone(timezone.utc)


def scheduled_closed(at):
    local = at.astimezone(NY)
    weekday, hour = local.weekday(), local.hour
    return weekday == 5 or weekday == 4 and hour >= 17 or weekday == 6 and hour < 17


def closure_only(start, end, seconds):
    """True only when every absent native slot is scheduled closed."""
    if end < start or (end-start).total_seconds() > 4*86400:
        return False
    at = start
    while at < end:
        if not scheduled_closed(at) or not scheduled_closed(at + timedelta(seconds=seconds-1)):
            return False
        at += timedelta(seconds=seconds)
    return at == end


def open_elapsed_seconds(start, end):
    """Elapsed seconds excluding only the explicit scheduled NY weekend.

    Inputs are aware datetimes, ISO UTC strings or epoch numbers. Unknown
    holidays and intraday absences are not excluded. Long stale intervals are
    bounded and rejected, not rounded down into apparent freshness.
    """
    def convert(value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("calendar_timezone_required")
            return value.astimezone(timezone.utc)
        if type(value) in (int, float):
            return datetime.fromtimestamp(value, timezone.utc)
        return stamp(value)
    start, end = convert(start), convert(end)
    total = (end-start).total_seconds()
    if not 0 <= total <= 14*86400:
        raise ValueError("calendar_interval_invalid_or_stale")
    first = start.astimezone(NY).date()-timedelta(days=7)
    last = end.astimezone(NY).date()
    closed = 0.
    while first <= last:
        if first.weekday() == 4:
            friday = datetime(first.year,first.month,first.day,17,tzinfo=NY).astimezone(timezone.utc)
            sunday_date = first+timedelta(days=2)
            sunday = datetime(sunday_date.year,sunday_date.month,sunday_date.day,17,tzinfo=NY).astimezone(timezone.utc)
            closed += max(0.,(min(end,sunday)-max(start,friday)).total_seconds())
        first += timedelta(days=1)
    return total-closed


def supported_suffix(rows, seconds, *, allow_weekend):
    start, weekends = len(rows)-1, 0
    while start > 0:
        prior = stamp(rows[start-1]["time"]) + timedelta(seconds=seconds)
        current = stamp(rows[start]["time"])
        if prior != current:
            if not allow_weekend or not closure_only(prior, current, seconds):
                break
            weekends += 1
        start -= 1
    return rows[max(0, start):], {
        "discarded_rows_before_intraday_or_unknown_gap": max(0, start),
        "scheduled_weekend_gaps_retained": weekends,
        "calendar_contract": CALENDAR,
        "row_index_semantics": "observed_completed_native_bars; original_elapsed_clocks_retained",
    }


def validate_candle(row, seconds, observed):
    at = stamp(row["time"])
    if at.timestamp() % seconds or at + timedelta(seconds=seconds) > observed:
        raise ValueError("candle_unaligned_or_incomplete_at_read")
    if row.get("complete") is not True:
        raise ValueError("explicit_complete_candle_required")
    if scheduled_closed(at):
        raise ValueError("candle_in_scheduled_weekend_closure")
    for side in ("mid", "bid", "ask"):
        values = row.get(side) or {}
        if any(type(values.get(k)) not in (int, float) or not math.isfinite(values[k]) for k in "ohlc"):
            raise ValueError("finite_candle_prices_required")
        if not 0 < values["l"] <= min(values["o"], values["c"]) <= max(values["o"], values["c"]) <= values["h"]:
            raise ValueError("candle_ohlc_invalid")
    if row["ask"]["c"] < row["bid"]["c"]:
        raise ValueError("candle_spread_invalid")
    if type(row.get("volume")) not in (int, float) or not math.isfinite(row["volume"]) or row["volume"] < 0:
        raise ValueError("candle_volume_invalid")


def read_tail(path, pair, timeframe, *, observed_utc=None, clock=None):
    path = Path(path).absolute()
    if any(p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction()) for p in (path, *path.parents)):
        raise ValueError("candle_source_link_refused")
    before = path.stat()
    with path.open("rb") as handle:
        header = handle.readline(4097)
        if len(header) > 4096 or not header.endswith(b"\n"):
            raise ValueError("candle_header_bound")
        offset = max(len(header), before.st_size-MAX_BYTES)
        handle.seek(offset)
        raw = handle.read(MAX_BYTES+1)
    observed_utc = clock() if clock is not None else observed_utc
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or len(raw) > MAX_BYTES:
        raise ValueError("candle_source_changed_or_exceeded_bound")
    if raw and not raw.endswith(b"\n"):
        raise ValueError("candle_partial_last_record")
    if offset > len(header):
        raw = raw.split(b"\n", 1)[-1]
    fields = next(csv.reader([header.decode("utf-8-sig").strip()]))
    if len(fields) != len(set(fields)):
        raise ValueError("duplicate_candle_columns")
    parsed = []
    observed = stamp(observed_utc)
    for source in list(csv.DictReader(io.StringIO(raw.decode("utf-8")), fieldnames=fields))[-MAX_ROWS:]:
        if None in source or any(v is None for v in source.values()) or source.get("instrument") != pair or source.get("granularity") != timeframe:
            raise ValueError("candle_row_identity_or_width_invalid")
        times = [stamp(source[k]) for k in ("time", "datetime") if k in source]
        if not times or len(set(times)) != 1:
            raise ValueError("candle_original_clock_invalid")
        if "complete" in source and source["complete"].lower() not in ("true", "1"):
            raise ValueError("explicit_complete_candle_required")
        row = {"time": times[0].isoformat(), "complete": True, "volume": float(source["volume"])}
        for side, prefix in (("mid", ""), ("bid", "bid_"), ("ask", "ask_")):
            row[side] = {short: float(source[prefix+long]) for short, long in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))}
        validate_candle(row, SECONDS[timeframe], observed)
        if parsed and stamp(parsed[-1]["time"]) >= times[0]:
            raise ValueError("candle_duplicate_or_unsorted")
        parsed.append(row)
    kept, gaps = supported_suffix(parsed, SECONDS[timeframe], allow_weekend=timeframe != "M1")
    return kept, {"status": "available", "source_name": path.name, "source_path": str(path),
        "source_bytes": len(header)+len(raw), "source_tail_sha256": hashlib.sha256(header+raw).hexdigest(),
        "source_read_completed_utc": observed.isoformat(), "parsed_rows": len(parsed), "retained_rows": len(kept),
        "complete_basis": "explicit_if_present_and_original_bar_end_before_read", **gaps}


def aggregate(rows, target_seconds, source_seconds):
    """Exact complete source buckets only; never fill a partial/weekend bucket."""
    if target_seconds < source_seconds or target_seconds % source_seconds:
        raise ValueError("invalid_aggregation_period")
    buckets = {}
    for row in rows:
        at = int(stamp(row["time"]).timestamp())
        if row.get("complete") is not True or at % source_seconds:
            raise ValueError("complete_aligned_source_required")
        key = at // target_seconds * target_seconds
        if at in buckets.setdefault(key, {}):
            raise ValueError("duplicate_aggregation_source")
        buckets[key][at] = row
    output = []
    for key, values in sorted(buckets.items()):
        expected = list(range(key, key+target_seconds, source_seconds))
        if sorted(values) != expected:
            continue
        ordered = [values[t] for t in expected]
        result = {"time": datetime.fromtimestamp(key, timezone.utc).isoformat(), "complete": True,
                  "volume": sum(r["volume"] for r in ordered)}
        for side in ("mid", "bid", "ask"):
            result[side] = {"o": ordered[0][side]["o"], "h": max(r[side]["h"] for r in ordered),
                            "l": min(r[side]["l"] for r in ordered), "c": ordered[-1][side]["c"]}
        output.append(result)
    return output
