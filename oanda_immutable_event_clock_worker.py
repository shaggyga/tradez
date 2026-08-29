#!/usr/bin/env python3
"""Independent fail-closed producer for the immutable official-event clock.

The target-response collector must never run the heavy news/tagging pipeline.
This worker instead watches the supervisor-owned tagger publication and, only
when a complete new generation and a trusted host-clock attestation are both
fresh, atomically appends the semantic clock snapshot and its observation.

This process is research-only.  It has no broker, order, lifecycle, promotion,
authorization, or execution imports and can only support ``no_trade``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    from forex_system.ingestion.immutable_event_clock import (
        EventClockLedgerError,
        append_snapshot_and_observation_atomic,
        build_clock_attestation,
        build_snapshot,
        read_stable_source,
    )
except ModuleNotFoundError:
    from src.forex_system.ingestion.immutable_event_clock import (
        EventClockLedgerError,
        append_snapshot_and_observation_atomic,
        build_clock_attestation,
        build_snapshot,
        read_stable_source,
    )


UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_EVENTS = DATA / "news_event_tags" / "events_latest.json"
DEFAULT_MANIFEST = DATA / "news_event_tags" / "manifest.json"
DEFAULT_CLOCK_INTEGRITY = DATA / "state" / "clock_integrity_v1.json"
DEFAULT_DATABASE = DATA / "research_ledgers" / "immutable_event_clock_v2.sqlite"
DEFAULT_STATE = DATA / "state" / "immutable_event_clock_worker_v2.json"
DEFAULT_FAILURES = DATA / "reports" / "immutable_event_clock_worker_failures_v2.jsonl"
DEFAULT_LOCK = DATA / "research_ledgers" / "immutable_event_clock_worker_v2.lock"
SCHEMA_VERSION = "immutable_event_clock_worker_v2"
SUPPORTED_EXECUTION_DECISION = "no_trade"
MAXIMUM_FRESHNESS_SEC = 90.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_utc(value: Any) -> dt.datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("timestamp_must_include_offset")
    return parsed.astimezone(UTC)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _append_failure_once(
    path: Path,
    payload: Mapping[str, Any],
    *,
    prior_state: Mapping[str, Any],
) -> None:
    signature = _sha256_json(
        {
            "status": payload.get("status"),
            "source_generation_id": payload.get("source_generation_id"),
            "details": payload.get("details"),
        }
    )
    if signature == str(prior_state.get("last_failure_signature") or ""):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {**dict(payload), "failure_signature": signature}
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(_canonical_json(record) + "\n")


def _base_result(observed: dt.datetime) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _iso(observed),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": SUPPORTED_EXECUTION_DECISION,
        "source_producer_ownership": "independent_supervisor_owned",
        "target_collector_isolation": True,
        "maximum_freshness_sec": MAXIMUM_FRESHNESS_SEC,
    }


def _classify_source_error(error: EventClockLedgerError) -> str:
    text = str(error)
    if text.startswith("source_read_failed"):
        return "source_artifact_missing_or_unreadable"
    if text in {
        "events_newer_than_manifest",
        "manifest_event_count_mismatch",
        "manifest_changed_during_read",
        "source_changed_during_read",
    }:
        return "source_publication_incoherent_or_racing"
    return "source_publication_invalid"


def run_cycle(
    *,
    events_path: Path = DEFAULT_EVENTS,
    manifest_path: Path = DEFAULT_MANIFEST,
    clock_integrity_path: Path = DEFAULT_CLOCK_INTEGRITY,
    database_path: Path = DEFAULT_DATABASE,
    state_path: Path = DEFAULT_STATE,
    failure_path: Path = DEFAULT_FAILURES,
    observed_utc: dt.datetime | None = None,
    now_fn: Callable[[], dt.datetime] = utc_now,
) -> dict[str, Any]:
    """Process at most one newly published coherent source generation."""

    cycle_started = (observed_utc or now_fn()).astimezone(UTC)
    prior_state = _load_state(state_path)
    result = {
        **_base_result(cycle_started),
        "cycle_started_utc": _iso(cycle_started),
    }
    source_generation_id = ""
    try:
        source = read_stable_source(events_path, manifest_path)
        # Knowledge begins no earlier than successful completion of the stable
        # source read.  The explicit clock remains available for deterministic
        # tests/replay; production obtains a fresh clock only after the read.
        observed = (observed_utc or now_fn()).astimezone(UTC)
        result.update(
            {
                "generated_utc": _iso(observed),
                "source_read_completed_utc": _iso(observed),
            }
        )
        source_generated = _parse_utc(source.source_generated_utc)
        source_age = (observed - source_generated).total_seconds()
        source_generation_id = "tagger_generation_" + _sha256_json(
            {
                "source_generated_utc": source.source_generated_utc,
                "events_sha256": source.events_sha256,
                "manifest_sha256": source.manifest_sha256,
            }
        )[:32]
        result.update(
            {
                "source_generation_id": source_generation_id,
                "source_generated_utc": source.source_generated_utc,
                "source_age_sec": round(source_age, 6),
                "source_event_count": source.source_event_count,
                "events_sha256": source.events_sha256,
                "manifest_sha256": source.manifest_sha256,
            }
        )
        if source_age < 0:
            raise RuntimeError("source_generation_in_future")
        if source_age > MAXIMUM_FRESHNESS_SEC:
            raise RuntimeError("source_generation_stale")
        if source_generation_id == str(
            prior_state.get("last_successful_source_generation_id") or ""
        ):
            result.update(
                {
                    "status": "idle_already_captured_generation",
                    "database_mutated": False,
                }
            )
            _atomic_write_json(state_path, {**prior_state, **result})
            return result
        attestation = build_clock_attestation(
            clock_integrity_path,
            observed,
            maximum_age_sec=MAXIMUM_FRESHNESS_SEC,
        )
        result.update(
            {
                "clock_attestation_state": str(attestation.get("state") or ""),
                "clock_attested": bool(attestation.get("attested")),
                "clock_attestation_generated_utc": attestation.get("generated_utc"),
                "clock_attestation_age_sec": attestation.get("age_at_capture_sec"),
                "clock_attestation_payload_sha256": attestation.get("payload_sha256"),
            }
        )
        if str(attestation.get("state") or "") != "fresh_trusted" or not bool(
            attestation.get("attested")
        ):
            raise RuntimeError("clock_attestation_not_fresh_trusted")
        snapshot = build_snapshot(source, observed, clock_attestation=attestation)
        ledger = append_snapshot_and_observation_atomic(database_path, snapshot)
        result.update(
            {
                "status": "captured",
                "database_mutated": True,
                "snapshot_id": ledger["snapshot_id"],
                "clock_observation_id": ledger["clock_observation_id"],
                "semantic_status": ledger["status"],
                "scheduled_clock_count": int(snapshot["source_scheduled_clock_count"]),
                "rejected_malformed_clock_count": int(
                    snapshot["rejected_malformed_clock_count"]
                ),
            }
        )
        next_state = {
            **prior_state,
            **result,
            "last_successful_source_generation_id": source_generation_id,
            "last_successful_capture_utc": _iso(observed),
            "last_failure_signature": "",
        }
        _atomic_write_json(state_path, next_state)
        return result
    except EventClockLedgerError as exc:
        result.update(
            {
                "status": _classify_source_error(exc),
                "database_mutated": False,
                "source_generation_id": source_generation_id,
                "details": {"error": str(exc)},
            }
        )
    except (OSError, ValueError, RuntimeError) as exc:
        result.update(
            {
                "status": str(exc),
                "database_mutated": False,
                "source_generation_id": source_generation_id,
                "details": {"error": str(exc)},
            }
        )
    _append_failure_once(failure_path, result, prior_state=prior_state)
    signature = _sha256_json(
        {
            "status": result.get("status"),
            "source_generation_id": result.get("source_generation_id"),
            "details": result.get("details"),
        }
    )
    _atomic_write_json(
        state_path,
        {**prior_state, **result, "last_failure_signature": signature},
    )
    return result


class ProducerLock:
    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> "ProducerLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(f"immutable_event_clock_worker_lock_exists:{self.path}") from exc
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(_canonical_json({"pid": os.getpid(), "created_utc": _iso(utc_now())}))
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--clock-integrity", type=Path, default=DEFAULT_CLOCK_INTEGRITY)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--failures", type=Path, default=DEFAULT_FAILURES)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    arguments = parser.parse_args()
    interval = max(0.25, float(arguments.interval_sec))
    duration = max(0.0, float(arguments.duration_sec))
    started = time.monotonic()
    with ProducerLock(arguments.lock):
        while True:
            payload = run_cycle(
                events_path=arguments.events,
                manifest_path=arguments.manifest,
                clock_integrity_path=arguments.clock_integrity,
                database_path=arguments.database,
                state_path=arguments.state,
                failure_path=arguments.failures,
            )
            print(json.dumps(payload, sort_keys=True), flush=True)
            if duration <= 0 or time.monotonic() - started >= duration:
                break
            time.sleep(interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
