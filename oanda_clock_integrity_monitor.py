#!/usr/bin/env python3
"""Publish read-only host/broker clock integrity for causal FX timestamps."""

from __future__ import annotations

import argparse
import email.utils
import json
import math
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_SOURCE = STATE / "practice_007_quote_stream_heartbeat_v1.json"
DEFAULT_OUTPUT = STATE / "clock_integrity_v1.json"
DEFAULT_HISTORY = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "logs"
    / "clock_integrity_samples_v1.jsonl"
)
DEFAULT_HTTPS_CLOCK_URL = "https://api-fxpractice.oanda.com/v3/accounts"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(
    path: Path,
    payload: dict[str, Any],
    *,
    replace_attempts: int = 100,
    retry_delay_sec: float = 0.1,
) -> None:
    """Publish a clock attestation without dying on a transient Windows lock.

    Readers occasionally hold the destination without delete sharing.  A
    single ``os.replace`` then raises ``PermissionError`` even though the
    monitor and its input are healthy.  Unique temporary names prevent
    concurrent publishers from deleting one another's pending write; bounded
    retries bridge short reader/antivirus locks.  If the lock persists, the
    old attestation is deliberately left in place so consumers fail closed on
    its age rather than accepting fabricated freshness.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    attempts = max(1, int(replace_attempts))
    delay = max(0.0, float(retry_delay_sec))
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        for attempt in range(attempts):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt + 1 >= attempts:
                    raise
                if delay:
                    time.sleep(delay)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def finite_clock_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def independent_https_host_clock_trusted(state: Mapping[str, Any]) -> bool:
    """Whether the independent HTTPS sample attests the host clock itself.

    A streaming price timestamp can lag during an otherwise healthy host
    session.  That says something useful about that stream, but it must not
    turn a contemporaneous, credential-free HTTPS Date sample into a false
    host-clock discontinuity.
    """
    external = state.get("external_https_clock")
    if not isinstance(external, Mapping) or external.get("status") != "ok":
        return False
    offset = finite_clock_number(external.get("offset_sec"))
    round_trip = finite_clock_number(external.get("round_trip_ms"))
    return (
        offset is not None
        and round_trip is not None
        and abs(offset) <= 2.0
        and 0.0 <= round_trip <= 2000.0
    )


def apply_continuity_guard(
    current: dict[str, Any],
    previous: Mapping[str, Any] | None,
    *,
    observed_utc: datetime | None = None,
    offset_jump_threshold_sec: float = 2.0,
    quarantine_sec: float = 600.0,
) -> dict[str, Any]:
    """Fail closed after a material broker/host offset discontinuity.

    Instantaneous agreement is insufficient immediately after a host-clock
    correction: records created around the jump may have ambiguous knowledge
    time.  Carry a bounded quarantine in the published state so downstream
    consumers cannot accept the first apparently healthy post-jump sample.
    """

    state = dict(current)
    prior = previous if isinstance(previous, Mapping) else {}
    observed = (observed_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    # Prefer the independent host attestation for continuity when it is
    # present.  A jump only in the stream timestamp is a feed-quality fact,
    # not evidence that the host time moved.
    current_lead = (
        None if independent_https_host_clock_trusted(state)
        else finite_clock_number(state.get("broker_clock_lead_sec"))
    )
    previous_lead = finite_clock_number(prior.get("broker_clock_lead_sec"))
    offset_jump = (
        None
        if current_lead is None or previous_lead is None
        else abs(current_lead - previous_lead)
    )
    detected = bool(
        offset_jump is not None and offset_jump > offset_jump_threshold_sec
    )
    prior_until = parse_utc(prior.get("clock_discontinuity_quarantine_until_utc"))
    prior_detected_at = parse_utc(prior.get("clock_discontinuity_detected_at_utc"))
    if detected:
        detected_at = observed
        quarantine_until = observed + timedelta(seconds=max(0.0, quarantine_sec))
    else:
        detected_at = prior_detected_at
        quarantine_until = prior_until
    active = bool(quarantine_until is not None and observed < quarantine_until)
    state.update(
        {
            "clock_discontinuity_detected": detected,
            "clock_discontinuity_active": active,
            "clock_discontinuity_offset_jump_sec": (
                round(offset_jump, 3) if offset_jump is not None else None
            ),
            "previous_broker_clock_lead_sec": previous_lead,
            "clock_discontinuity_detected_at_utc": (
                detected_at.isoformat() if detected_at is not None else None
            ),
            "clock_discontinuity_quarantine_until_utc": (
                quarantine_until.isoformat() if quarantine_until is not None else None
            ),
        }
    )
    if active:
        state["status"] = "degraded"
        state["timestamp_normalization_trusted"] = False
        state["host_clock_synchronized"] = False
        reasons = list(state.get("reasons") or [])
        if "clock_discontinuity_quarantine_active" not in reasons:
            reasons.append("clock_discontinuity_quarantine_active")
        state["reasons"] = reasons
        state["required_external_action"] = (
            "wait for the bounded clock-discontinuity quarantine to expire"
        )
    return state


def append_history(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    external = state.get("external_https_clock")
    if not isinstance(external, Mapping):
        external = {}
    row = {
        "schema_version": 1,
        "generated_utc": state.get("generated_utc"),
        "status": state.get("status"),
        "broker_clock_lead_sec": state.get("broker_clock_lead_sec"),
        "broker_clock_sample_count": state.get("broker_clock_sample_count"),
        "external_https_offset_sec": external.get("offset_sec"),
        "clock_sources_consistent": state.get("clock_sources_consistent"),
        "clock_discontinuity_detected": state.get(
            "clock_discontinuity_detected"
        ),
        "clock_discontinuity_active": state.get("clock_discontinuity_active"),
        "clock_discontinuity_offset_jump_sec": state.get(
            "clock_discontinuity_offset_jump_sec"
        ),
        "clock_discontinuity_detected_at_utc": state.get(
            "clock_discontinuity_detected_at_utc"
        ),
        "clock_discontinuity_quarantine_until_utc": state.get(
            "clock_discontinuity_quarantine_until_utc"
        ),
        "reasons": state.get("reasons") or [],
        "research_only": True,
        "can_place_orders": False,
        "changes_system_time": False,
    }
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def windows_time_service_status() -> dict[str, Any]:
    # psutil uses the Windows service-control API directly and avoids spawning
    # a console helper.  The latter can stall on busy hosts, which would make
    # the clock monitor itself look stale even though it is read-only.
    try:
        import psutil  # type: ignore

        service = psutil.win_service_get("W32Time")
        details = service.as_dict()
        state = str(details.get("status") or "unknown").lower()
        return {
            "state": state,
            "query_ok": True,
            "start_type": str(details.get("start_type") or ""),
            "display_name": str(details.get("display_name") or ""),
            "error": "",
            "query_method": "windows_service_api",
        }
    except (ImportError, AttributeError, OSError, KeyError) as exc:
        api_error = f"{type(exc).__name__}: {exc}"

    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(
            ["sc.exe", "query", "W32Time"],
            capture_output=True,
            text=True,
            timeout=10.0,
            creationflags=flags,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "state": "unknown",
            "query_ok": False,
            "error": f"service_api={api_error}; sc={type(exc).__name__}: {exc}",
        }
    text = (result.stdout or "") + "\n" + (result.stderr or "")
    upper = text.upper()
    state = (
        "running"
        if "RUNNING" in upper
        else "stopped"
        if "STOPPED" in upper
        else "unknown"
    )
    return {
        "state": state,
        "query_ok": result.returncode == 0,
        "returncode": int(result.returncode),
        "error": "" if result.returncode == 0 else text.strip()[-500:],
        "query_method": "sc_fallback",
        "service_api_error": api_error,
    }


def https_clock_sample(url: str = DEFAULT_HTTPS_CLOCK_URL) -> dict[str, Any]:
    """Cross-check the host clock using OANDA's credential-free HTTP Date."""
    started = time.time()
    try:
        request = urllib.request.Request(
            url, method="GET", headers={"User-Agent": "forex-clock-integrity/1"}
        )
        try:
            response = urllib.request.urlopen(request, timeout=10.0)
        except urllib.error.HTTPError as error:
            # The expected unauthenticated response still has a Date header.
            response = error
        finished = time.time()
        header = response.headers.get("Date")
        if not header:
            raise ValueError("missing Date header")
        server_epoch = email.utils.parsedate_to_datetime(header).timestamp()
        midpoint = (started + finished) / 2.0
        return {
            "status": "ok", "url": url,
            "http_status": int(getattr(response, "status", getattr(response, "code", 0)) or 0),
            "server_date": header, "offset_sec": round(server_epoch - midpoint, 3),
            "round_trip_ms": round((finished - started) * 1000.0, 3),
            "precision_sec": 1.0, "credentials_sent": False,
        }
    except (OSError, ValueError, urllib.error.URLError) as exc:
        return {
            "status": "unavailable", "url": url,
            "error": f"{type(exc).__name__}: {exc}", "credentials_sent": False,
        }


def build_state(
    source: Path,
    *,
    service: dict[str, Any] | None = None,
    external_clock: dict[str, Any] | None = None,
    maximum_source_age_sec: float = 120.0,
    maximum_trusted_offset_sec: float = 300.0,
) -> dict[str, Any]:
    payload = read_json(source)
    details = payload.get("details")
    if not isinstance(details, Mapping):
        details = {}
    stream = details.get("stream")
    if not isinstance(stream, Mapping):
        stream = {}
    raw_lead = stream.get("broker_clock_lead_sec")
    lead = (
        float(raw_lead)
        if isinstance(raw_lead, (int, float))
        and not isinstance(raw_lead, bool)
        and math.isfinite(float(raw_lead))
        else math.nan
    )
    raw_samples = stream.get("broker_clock_sample_count")
    samples = (
        int(raw_samples)
        if isinstance(raw_samples, int) and not isinstance(raw_samples, bool)
        else 0
    )
    source_age = math.inf
    try:
        source_age = time.time() - source.stat().st_mtime
    except OSError:
        pass
    source_fresh = -2.0 <= source_age <= maximum_source_age_sec
    offset_trusted = (
        math.isfinite(lead)
        and abs(lead) <= maximum_trusted_offset_sec
        and samples >= 32
        and source_fresh
    )
    system_service = service if service is not None else windows_time_service_status()
    if not isinstance(system_service, Mapping):
        system_service = {}
    external = external_clock if isinstance(external_clock, Mapping) else {}
    raw_external_offset = external.get("offset_sec")
    external_offset = (
        float(raw_external_offset)
        if isinstance(raw_external_offset, (int, float))
        and not isinstance(raw_external_offset, bool)
        and math.isfinite(float(raw_external_offset))
        else math.nan
    )
    raw_round_trip_ms = external.get("round_trip_ms")
    round_trip_ms = (
        float(raw_round_trip_ms)
        if isinstance(raw_round_trip_ms, (int, float))
        and not isinstance(raw_round_trip_ms, bool)
        and math.isfinite(float(raw_round_trip_ms))
        else math.inf
    )
    external_available = (
        external.get("status") == "ok"
        and math.isfinite(external_offset)
        and abs(external_offset) <= maximum_trusted_offset_sec
        and round_trip_ms <= 2_000.0
    )
    source_disagreement_sec = (
        abs(lead - external_offset)
        if offset_trusted and external_available
        else None
    )
    clock_sources_consistent = (
        source_disagreement_sec is None or source_disagreement_sec <= 2.0
    )
    independent_host_clock = (
        external_available
        and abs(external_offset) <= 2.0
    )
    external_trusted = independent_host_clock and clock_sources_consistent
    service_synchronized = (
        offset_trusted
        and math.isfinite(lead)
        and abs(lead) <= 2.0
        and system_service.get("state") == "running"
        and clock_sources_consistent
    )
    synchronized = service_synchronized or independent_host_clock
    timestamp_trusted = (offset_trusted and clock_sources_consistent) or independent_host_clock
    status = "ok" if service_synchronized else "mitigated" if timestamp_trusted else "degraded"
    reasons = []
    if not source_fresh:
        reasons.append("broker_clock_reference_stale")
    if not math.isfinite(lead):
        reasons.append("broker_clock_offset_unavailable")
    elif abs(lead) > 2.0:
        reasons.append("host_clock_offset_exceeds_2s")
    if system_service.get("state") != "running":
        reasons.append("windows_time_service_not_running")
    if external.get("status") != "ok":
        reasons.append("external_https_clock_unavailable")
    elif not independent_host_clock:
        reasons.append("external_clock_offset_exceeds_2s")
    if not clock_sources_consistent:
        reasons.append("clock_sources_disagree")
    return {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "status": status,
        "research_only": True,
        "can_place_orders": False,
        "changes_system_time": False,
        "source": str(source.resolve()),
        "source_age_sec": None if not math.isfinite(source_age) else round(source_age, 3),
        "source_fresh": source_fresh,
        "broker_clock_lead_sec": None if not math.isfinite(lead) else round(lead, 3),
        "broker_clock_sample_count": samples,
        "reported_clock_sync_status": str(stream.get("clock_sync_status") or ""),
        "timestamp_normalization_trusted": timestamp_trusted,
        "host_clock_synchronized": synchronized,
        "windows_time_service": system_service,
        "external_https_clock": external,
        "clock_sources_consistent": clock_sources_consistent,
        "independent_https_host_clock_trusted": independent_host_clock,
        "clock_source_disagreement_sec": (
            round(source_disagreement_sec, 3)
            if source_disagreement_sec is not None else None
        ),
        "reasons": reasons,
        "required_external_action": (
            "start/synchronize Windows Time with administrator authority"
            if not synchronized
            else ""
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--https-clock-url", default=DEFAULT_HTTPS_CLOCK_URL)
    args = parser.parse_args()
    if args.interval_sec < 0.0 or args.duration_sec < 0.0:
        raise SystemExit("interval and duration must be non-negative")
    stop_at = time.monotonic() + args.duration_sec if args.duration_sec else None
    while True:
        previous = read_json(args.output)
        state = build_state(args.source, external_clock=https_clock_sample(args.https_clock_url))
        state = apply_continuity_guard(state, previous)
        try:
            atomic_json(args.output, state)
        except OSError as exc:
            # A failed publication must not terminate the long-running monitor
            # and add a supervisor startup gap.  The prior state naturally
            # ages out at every consumer's existing fail-closed threshold.
            print(
                json.dumps(
                    {
                        "event": "clock_integrity_publication_failed",
                        "observed_utc": utc_now(),
                        "error": f"{type(exc).__name__}: {exc}",
                        "output": str(args.output),
                        "old_state_retained_until_age_gate": True,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        try:
            append_history(args.history, state)
        except OSError as exc:
            print(
                json.dumps(
                    {
                        "event": "clock_integrity_history_append_failed",
                        "observed_utc": utc_now(),
                        "error": f"{type(exc).__name__}: {exc}",
                        "history": str(args.history),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        if args.interval_sec <= 0.0:
            return 0
        if stop_at is not None and time.monotonic() >= stop_at:
            return 0
        delay = max(5.0, args.interval_sec)
        if stop_at is not None:
            delay = min(delay, max(0.0, stop_at - time.monotonic()))
            if delay <= 0.0:
                return 0
        time.sleep(delay)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "atomic_json",
    "append_history",
    "apply_continuity_guard",
    "build_state",
    "windows_time_service_status",
    "https_clock_sample",
]
