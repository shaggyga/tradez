"""Read-only clock prerequisite for the feature research workers.

The numerical acceptance rules reproduce the aligned-host subset in
oanda_local_news_sentiment_repair_v2.validate_clock_state. The monitor's
broader permission to normalize an offset is insufficient here. This module
never imports/starts the monitor, adjusts timestamps, or authorizes activation.

A supplied now_epoch is a trusted caller boundary (useful for pure tests).
The file reader samples actual time after reading when it is omitted. Workers
must retain and revalidate the first proof at completion, and read current
health again before publication; a later healthy file cannot renew an earlier
proof. Failure handling/heartbeats and pending-deadline accounting remain the
worker's responsibility. Previously retained observations are not relabelled.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import hashlib
import json
import math
import os
from pathlib import Path, PureWindowsPath
import stat
import time


SCHEMA = "feature_research_clock_gate_v1_20260913"
UPSTREAM_CONTRACT = "repaired_news_synchronized_host_attestation_v2_20260912"
MAX_STATE_BYTES = 128 * 1024
MAX_AGE_SEC = 90
DEFAULT_CLOCK_PATH = (Path(__file__).parent / "data" / "oanda_training_manager"
                      / "state" / "clock_integrity_v1.json")


class ClockBoundaryError(ValueError):
    """Expected malformed, changed, or unbounded clock-input refusal."""


def _finite(value):
    if type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _utc_epoch(value):
    if type(value) is not str or not 1 <= len(value) <= 128:
        raise ClockBoundaryError("clock_generated_utc_invalid")
    try:
        stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset() != dt.timedelta(0):
            raise ValueError("not UTC")
        return stamp.timestamp()
    except (ValueError, OverflowError, OSError) as exc:
        raise ClockBoundaryError("clock_generated_utc_invalid") from exc


def _encoded(value):
    result = bytearray()
    try:
        encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"),
                                   ensure_ascii=False, allow_nan=False)
        for part in encoder.iterencode(value):
            raw = part.encode("utf-8")
            if len(result) + len(raw) > MAX_STATE_BYTES:
                raise ClockBoundaryError("clock_state_byte_bound")
            result.extend(raw)
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as exc:
        if isinstance(exc, ClockBoundaryError):
            raise
        raise ClockBoundaryError("clock_state_json_invalid") from exc
    return bytes(result)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ClockBoundaryError("clock_json_duplicate_key")
        value[key] = item
    return value


def _constant(_):
    raise ClockBoundaryError("clock_json_nonfinite")


def _decoded(raw):
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_constant=_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, ClockBoundaryError):
            raise
        raise ClockBoundaryError("clock_state_json_invalid") from exc
    if type(value) is not dict:
        raise ClockBoundaryError("clock_state_object_required")
    return value


def _result(valid, reason, evidence):
    return {"schema_version": SCHEMA, "valid": valid, "reason": reason,
            "evidence": evidence, "research_only": True,
            "can_place_orders": False, "authorizes_activation": False}


def validate_clock_state(payload, *, now_epoch):
    """Pure fail-closed decision, without modifying the supplied clock state.

    The retained clock_state is an owned JSON copy for completion revalidation;
    its digest uses the original research contract's canonical encoding. This
    proves a clock prerequisite only, not health of a feed or authorization to
    launch a process. No field is converted into a fabricated observation time.
    """
    now = _finite(now_epoch)
    evidence = {"contract_id": UPSTREAM_CONTRACT, "observed_epoch": now,
                "applied_offset_sec": 0}
    if now is None:
        return _result(False, "clock_now_invalid", evidence)
    try:
        if type(payload) is not dict:
            raise ClockBoundaryError("clock_state_object_required")
        raw = _encoded(payload)
        state = _decoded(raw)
        evidence.update(clock_state_sha256=hashlib.sha256(raw).hexdigest(),
                        clock_state=state)
        if type(state.get("schema_version")) is not int or state["schema_version"] != 1:
            raise ClockBoundaryError("clock_state_schema_invalid")
        generated = _utc_epoch(state.get("generated_utc"))
        age = now - generated
        evidence.update(integrity_generated_epoch=generated, integrity_age_sec=age)
        if not 0 <= age <= MAX_AGE_SEC or state.get("status") not in ("ok", "mitigated"):
            raise ClockBoundaryError("clock_state_stale_or_future")
        if (state.get("timestamp_normalization_trusted") is not True
                or state.get("host_clock_synchronized") is not True):
            raise ClockBoundaryError("clock_not_synchronized")
        active = state.get("clock_discontinuity_active", False)
        if type(active) is not bool:
            raise ClockBoundaryError("clock_discontinuity_flag_invalid")
        if active:
            raise ClockBoundaryError("clock_discontinuity_active")
        if "clock_sources_consistent" in state and state["clock_sources_consistent"] is not True:
            raise ClockBoundaryError("clock_sources_disagree")
        lead = _finite(state.get("broker_clock_lead_sec"))
        samples = state.get("broker_clock_sample_count")
        source_age = _finite(state.get("source_age_sec"))
        broker = (state.get("source_fresh") is True and type(samples) is int and samples >= 32
                  and lead is not None and abs(lead) <= 300
                  and source_age is not None and source_age >= 0
                  and 0 <= source_age + age <= MAX_AGE_SEC)
        ext = state.get("external_https_clock") or {}
        if type(ext) is not dict:
            raise ClockBoundaryError("clock_external_object_invalid")
        offset, rtt, precision = (_finite(ext.get(key)) for key in
                                  ("offset_sec", "round_trip_ms", "precision_sec"))
        external = (ext.get("status") == "ok" and offset is not None
                    and rtt is not None and precision is not None
                    and abs(offset) <= 300 and 0 <= rtt <= 2000 and 0 <= precision <= 1)
        probe_epoch = None
        if external:
            try:
                server_date = email.utils.parsedate_to_datetime(ext.get("server_date"))
                probe_epoch = server_date.timestamp() - offset if server_date.tzinfo else None
                external = (probe_epoch is not None and math.isfinite(probe_epoch)
                            and 0 <= now - probe_epoch <= MAX_AGE_SEC)
            except (ValueError, TypeError, AttributeError, OverflowError, OSError):
                external = False
        evidence.update(broker_reference_usable=broker, external_reference_usable=external,
                        external_probe_epoch=probe_epoch)
        if broker and external and abs(lead - offset) > 2:
            raise ClockBoundaryError("clock_sources_disagree")
        if not ((broker and abs(lead) <= 2) or (external and abs(offset) <= 2)):
            raise ClockBoundaryError("clock_alignment_unavailable")
        evidence["aligned_source"] = ("both" if broker and abs(lead) <= 2 and external and abs(offset) <= 2
                                      else "broker" if broker and abs(lead) <= 2 else "external_https")
        return _result(True, "clock_aligned", evidence)
    except ClockBoundaryError as exc:
        return _result(False, str(exc), evidence)


def _plain_c_file(value):
    text = os.fspath(value)
    if type(text) is not str:
        raise ClockBoundaryError("clock_plain_c_path_required")
    pure = PureWindowsPath(text)
    if (not pure.is_absolute() or pure.drive.lower() != "c:" or ".." in pure.parts
            or any(":" in part for part in pure.parts[1:])):
        raise ClockBoundaryError("clock_plain_c_path_required")
    path = Path(text)
    for item in (*reversed(path.parents), path):
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 1024:
            raise ClockBoundaryError("clock_reparse_path_refused")
        if not (stat.S_ISREG(info.st_mode) if item == path else stat.S_ISDIR(info.st_mode)):
            raise ClockBoundaryError("clock_path_kind_invalid")
    return path


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def read_verified_clock(path=DEFAULT_CLOCK_PATH, *, now_epoch=None):
    """Bounded stable clock-file read. No write, fallback, or monitor startup.

    The default actual clock is sampled after the read and identity recheck.
    Callers must not present an explicit older epoch as actual read completion.
    File identity protects this read; it does not prove the monitor is running.
    """
    source = {}
    try:
        target = _plain_c_file(path)
        with target.open("rb") as stream:
            before = os.fstat(stream.fileno())
            if before.st_size > MAX_STATE_BYTES:
                raise ClockBoundaryError("clock_state_byte_bound")
            raw = stream.read(MAX_STATE_BYTES + 1)
            after = os.fstat(stream.fileno())
        current = _plain_c_file(target).stat()
        if len(raw) > MAX_STATE_BYTES:
            raise ClockBoundaryError("clock_state_byte_bound")
        if _identity(before) != _identity(after) or _identity(after) != _identity(current) or len(raw) != after.st_size:
            raise ClockBoundaryError("clock_source_changed_during_read")
        source = {"path": str(target), "file_sha256": hashlib.sha256(raw).hexdigest(),
                  "size_bytes": len(raw), "file_identity": list(_identity(after))}
        payload = _decoded(raw)
        result = validate_clock_state(payload, now_epoch=time.time() if now_epoch is None else now_epoch)
        result["evidence"]["source"] = source
        return result
    except (OSError, TypeError, ValueError, OverflowError) as exc:
        reason = str(exc) if isinstance(exc, ClockBoundaryError) else "clock_read_failed"
        return _result(False, reason, {"contract_id": UPSTREAM_CONTRACT,
                                      "observed_epoch": _finite(now_epoch), "source": source,
                                      "error_type": type(exc).__name__})
