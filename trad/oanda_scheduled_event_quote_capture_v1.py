#!/usr/bin/env python3
"""Prospectively freeze all-68 executable quotes at scheduled event clocks.

This ledger answers a deliberately narrower question than the official-source
fast lane: what was executable at the already-known release minute, even when
the decision body itself reaches the collector later or through a fallback.
It does not infer the result, surprise, direction, or tradability of an event.

Event clocks must be registered from a fresh preflight at least one minute
before the event.  Each registered clock receives one terminal attempt at T0
and the frozen horizons.  Late or invalid attempts are persisted and never
retried.  Raw source-first-seen evidence remains a separate cohort.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Mapping, Sequence

from oanda_quote_transport import load_quote_snapshot
import oanda_official_release_fast_lane as fast_lane


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "scheduled_event_quote_capture_v1.json"
PREFLIGHT_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state" /
    "event_technical_preflight_v1.json"
)
QUOTE_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state" /
    "practice_007_market_quotes_v1.json"
)
OUTPUT_ROOT = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
DATABASE_PATH = OUTPUT_ROOT / "scheduled_event_quote_capture_v1.sqlite"
STATE_PATH = OUTPUT_ROOT / "scheduled_event_quote_capture_latest_v1.json"
HEARTBEAT_PATH = OUTPUT_ROOT / "scheduled_event_quote_capture_heartbeat_v1.json"

SCHEMA_VERSION = "scheduled_event_quote_capture_v1"
CONTRACT_ID = "scheduled_event_quote_capture_v1_all68_prospective_20260902"
COHORT_ID = "scheduled_event_quote_capture_v1_20260902a"
ACTIVATED_UTC = dt.datetime(2026, 9, 2, 0, 55, tzinfo=dt.timezone.utc)
REQUIRED_PREFLIGHT_CONTRACT_ID = (
    "neutral_event_clock_to_technical_preflight_v6_"
    "source_transport_readiness_20260901"
)
REQUIRED_QUOTE_SCHEMA_VERSION = 2
REQUIRED_QUOTE_PRODUCER = "practice_007_fast_executor_price_stream"
EXPECTED_INSTRUMENTS = fast_lane.EXPECTED_QUOTE_INSTRUMENTS
EXPECTED_INSTRUMENT_COUNT = fast_lane.EXPECTED_QUOTE_COUNT
EXPECTED_UNIVERSE_SHA256 = fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
HORIZONS_MIN = (0, 1, 5, 15, 30, 60)
LOOKAHEAD_DAYS = 45
MINIMUM_REGISTRATION_LEAD_SEC = 60.0
MAXIMUM_PREFLIGHT_AGE_SEC = 180.0
MAXIMUM_ATTEMPT_DELAY_SEC = 15.0
MAXIMUM_FUTURE_SKEW_SEC = 2.0


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc).isoformat()


def parse_time(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return default


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(dict(payload), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                # Windows readers can briefly hold an atomically published
                # JSON file without delete sharing. Preserve the same complete
                # temporary payload and retry; never expose a partial file.
                if attempt == 7:
                    raise
                time.sleep(min(0.4, 0.025 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def validate_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = read_json(path, {})
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "required_preflight_contract_id": REQUIRED_PREFLIGHT_CONTRACT_ID,
        "required_quote_snapshot_schema_version": REQUIRED_QUOTE_SCHEMA_VERSION,
        "required_quote_snapshot_producer": REQUIRED_QUOTE_PRODUCER,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "horizons_min": list(HORIZONS_MIN),
        "lookahead_days": LOOKAHEAD_DAYS,
        "minimum_registration_lead_sec": int(MINIMUM_REGISTRATION_LEAD_SEC),
        "maximum_preflight_age_sec": int(MAXIMUM_PREFLIGHT_AGE_SEC),
        "maximum_attempt_delay_sec": int(MAXIMUM_ATTEMPT_DELAY_SEC),
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"frozen scheduled-event capture mismatch:{key}")
    policy = payload.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    required_policy = {
        "prospective_only": True,
        "historical_backfill_allowed": False,
        "minute_precision_only": True,
        "one_terminal_attempt_per_event_horizon": True,
        "entry_quote_reacquisition_allowed": False,
        "missing_or_late_capture_policy": "persist_terminal_invalid_never_retry",
        "scheduled_clock_assigns_no_direction": True,
        "scheduled_clock_is_separate_from_source_first_seen_clock": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    for key, value in required_policy.items():
        if policy.get(key) != value:
            raise ValueError(f"unsafe scheduled-event capture policy:{key}")
    return payload


def open_database(path: Path = DATABASE_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduled_event_clock (
          clock_id TEXT PRIMARY KEY,
          event_id TEXT NOT NULL,
          event_series_id TEXT NOT NULL,
          headline TEXT NOT NULL,
          scheduled_utc TEXT NOT NULL,
          driver_currency TEXT NOT NULL,
          direct_currencies_json TEXT NOT NULL,
          affected_currencies_json TEXT NOT NULL,
          registered_utc TEXT NOT NULL,
          registration_lead_sec REAL NOT NULL,
          preflight_generated_utc TEXT NOT NULL,
          preflight_payload_sha256 TEXT NOT NULL,
          event_payload_json TEXT NOT NULL,
          event_payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduled_event_quote_capture (
          capture_id TEXT PRIMARY KEY,
          clock_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          target_utc TEXT NOT NULL,
          capture_read_started_utc TEXT NOT NULL,
          attempted_utc TEXT NOT NULL,
          attempt_delay_sec REAL NOT NULL,
          timing_quality TEXT NOT NULL,
          invalid_reason TEXT NOT NULL,
          observed_valid_quote_count INTEGER NOT NULL,
          proof_quote_count INTEGER NOT NULL,
          quote_snapshot_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          UNIQUE(clock_id,horizon_min),
          FOREIGN KEY(clock_id) REFERENCES scheduled_event_clock(clock_id)
        )
        """
    )
    for table in ("scheduled_event_clock", "scheduled_event_quote_capture"):
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_update
            BEFORE UPDATE ON {table}
            BEGIN SELECT RAISE(ABORT, 'append_only'); END
            """
        )
        connection.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS trg_{table}_no_delete
            BEFORE DELETE ON {table}
            BEGIN SELECT RAISE(ABORT, 'append_only'); END
            """
        )
    connection.commit()
    return connection


def _event_clock_groups(
    preflight: Mapping[str, Any], *, now: dt.datetime
) -> tuple[list[dict[str, Any]], str]:
    if preflight.get("contract_id") != REQUIRED_PREFLIGHT_CONTRACT_ID:
        return [], "preflight_contract_mismatch"
    generated = parse_time(preflight.get("generated_utc"))
    if generated is None:
        return [], "preflight_generated_clock_missing"
    age = (now - generated).total_seconds()
    if age < -MAXIMUM_FUTURE_SKEW_SEC or age > MAXIMUM_PREFLIGHT_AGE_SEC:
        return [], f"preflight_age_seconds:{age:.3f}"
    grouped: dict[str, dict[str, Any]] = {}
    for raw in preflight.get("events") or []:
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("timing_precision") or "") != "minute":
            continue
        scheduled = parse_time(raw.get("scheduled_utc"))
        if (
            scheduled is None
            or scheduled < ACTIVATED_UTC
            or scheduled > now + dt.timedelta(days=LOOKAHEAD_DAYS)
        ):
            continue
        event_id = str(raw.get("event_id") or "").strip()
        if not event_id:
            event_id = "scheduled_event_" + sha256_text(
                "|".join(
                    (
                        iso_utc(scheduled),
                        str(raw.get("event_series_id") or ""),
                        str(raw.get("headline") or ""),
                    )
                )
            )[:24]
        grouping_key = f"{event_id}|{iso_utc(scheduled)}"
        row = grouped.setdefault(
            grouping_key,
            {
                "event_id": event_id,
                "event_series_id": str(raw.get("event_series_id") or ""),
                "headline": str(raw.get("headline") or ""),
                "scheduled_utc": iso_utc(scheduled),
                "driver_currency": str(raw.get("driver_currency") or "").upper(),
                "direct_currencies": set(),
                "affected_currencies": set(),
                "rows": [],
            },
        )
        currency = str(raw.get("currency") or "").upper()
        if currency:
            row["affected_currencies"].add(currency)
            if raw.get("direct_event_currency") is True:
                row["direct_currencies"].add(currency)
        row["rows"].append(dict(raw))
    output = []
    for row in grouped.values():
        if not row["direct_currencies"]:
            continue
        row["direct_currencies"] = sorted(row["direct_currencies"])
        row["affected_currencies"] = sorted(row["affected_currencies"])
        output.append(row)
    output.sort(key=lambda row: (row["scheduled_utc"], row["event_id"]))
    return output, ""


def register_upcoming_clocks(
    connection: sqlite3.Connection,
    preflight: Mapping[str, Any],
    *,
    now: dt.datetime,
) -> tuple[int, str]:
    groups, error = _event_clock_groups(preflight, now=now)
    if error:
        return 0, error
    preflight_json = canonical_json(preflight)
    preflight_hash = sha256_text(preflight_json)
    inserted = 0
    for event in groups:
        scheduled = parse_time(event["scheduled_utc"])
        assert scheduled is not None
        lead = (scheduled - now).total_seconds()
        if lead < MINIMUM_REGISTRATION_LEAD_SEC:
            continue
        clock_id = "scheduled_event_clock_" + sha256_text(
            f"{event['event_id']}|{event['scheduled_utc']}|{CONTRACT_ID}|{COHORT_ID}"
        )[:32]
        event_json = canonical_json(event)
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO scheduled_event_clock (
              clock_id,event_id,event_series_id,headline,scheduled_utc,
              driver_currency,direct_currencies_json,affected_currencies_json,
              registered_utc,registration_lead_sec,preflight_generated_utc,
              preflight_payload_sha256,event_payload_json,event_payload_sha256,
              research_only,execution_eligible,can_place_orders,can_authorize,
              can_promote,contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clock_id,
                event["event_id"],
                event["event_series_id"],
                event["headline"],
                event["scheduled_utc"],
                event["driver_currency"],
                canonical_json(event["direct_currencies"]),
                canonical_json(event["affected_currencies"]),
                iso_utc(now),
                round(lead, 6),
                str(preflight.get("generated_utc") or ""),
                preflight_hash,
                event_json,
                sha256_text(event_json),
                1,
                0,
                0,
                0,
                0,
                CONTRACT_ID,
                COHORT_ID,
            ),
        )
        inserted += int(cursor.rowcount > 0)
    connection.commit()
    return inserted, ""


def due_attempts(
    connection: sqlite3.Connection, *, now: dt.datetime
) -> list[dict[str, Any]]:
    completed = {
        (str(row[0]), int(row[1]))
        for row in connection.execute(
            "SELECT clock_id,horizon_min FROM scheduled_event_quote_capture"
        )
    }
    output: list[dict[str, Any]] = []
    for row in connection.execute(
        "SELECT clock_id,event_id,scheduled_utc,event_payload_json "
        "FROM scheduled_event_clock ORDER BY scheduled_utc,clock_id"
    ):
        scheduled = parse_time(row[2])
        if scheduled is None:
            continue
        for horizon in HORIZONS_MIN:
            if (str(row[0]), horizon) in completed:
                continue
            target = scheduled + dt.timedelta(minutes=horizon)
            if target <= now:
                output.append(
                    {
                        "clock_id": str(row[0]),
                        "event_id": str(row[1]),
                        "scheduled_utc": iso_utc(scheduled),
                        "horizon_min": horizon,
                        "target_utc": iso_utc(target),
                        "event": json.loads(str(row[3])),
                    }
                )
    return output


def build_capture(
    attempt: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    *,
    read_started_utc: dt.datetime,
    attempted_utc: dt.datetime,
    snapshot_error: str = "",
) -> dict[str, Any]:
    target = parse_time(attempt.get("target_utc"))
    if target is None:
        raise ValueError("target clock missing")
    capture_id = "scheduled_event_quote_" + sha256_text(
        f"{attempt['clock_id']}|{attempt['horizon_min']}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    sidecar = fast_lane.build_raw_quote_capture(
        observation_id=capture_id,
        first_seen=target,
        input_prospective_observation=True,
        quote_payload=quote_payload,
        captured_utc=attempted_utc,
        snapshot_error=snapshot_error,
    )
    invalid_metadata: list[str] = []
    if quote_payload:
        if quote_payload.get("schema_version") != REQUIRED_QUOTE_SCHEMA_VERSION:
            invalid_metadata.append("quote_snapshot_schema_mismatch")
        if str(quote_payload.get("producer") or "") != REQUIRED_QUOTE_PRODUCER:
            invalid_metadata.append("quote_snapshot_producer_mismatch")
    if invalid_metadata and sidecar["timing_quality"] == "prospective_exact_live_quote":
        sidecar["timing_quality"] = "prospective_quote_coverage_invalid"
        sidecar["invalid_reason"] = ";".join(invalid_metadata)
        sidecar["proof_quote_count"] = 0
        sidecar["quote_count"] = 0
        sidecar["quotes"] = {}
    quote_json = canonical_json(quote_payload)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture_id,
        "clock_id": str(attempt["clock_id"]),
        "event_id": str(attempt["event_id"]),
        "event_scheduled_utc": str(attempt["scheduled_utc"]),
        "horizon_min": int(attempt["horizon_min"]),
        "target_utc": iso_utc(target),
        "capture_read_started_utc": iso_utc(read_started_utc),
        "attempted_utc": iso_utc(attempted_utc),
        "attempt_delay_sec": round((attempted_utc - target).total_seconds(), 6),
        "timing_quality": sidecar["timing_quality"],
        "invalid_reason": sidecar["invalid_reason"],
        "observed_valid_quote_count": sidecar["observed_valid_quote_count"],
        "proof_quote_count": sidecar["proof_quote_count"],
        "quotes": sidecar["quotes"],
        "observed_quotes": sidecar["observed_quotes"],
        "invalid_instruments": sidecar["invalid_instruments"],
        "quote_snapshot_generated_utc": sidecar["quote_snapshot_generated_utc"],
        "quote_snapshot_sha256": sha256_text(quote_json),
        "event": attempt.get("event") or {},
        "clock_semantics": "scheduled_release_time_not_source_first_seen_time",
        "direction_policy": "abstain",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }
    return payload


def insert_capture(connection: sqlite3.Connection, payload: Mapping[str, Any]) -> bool:
    payload_json = canonical_json(payload)
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO scheduled_event_quote_capture (
          capture_id,clock_id,horizon_min,target_utc,capture_read_started_utc,
          attempted_utc,attempt_delay_sec,timing_quality,invalid_reason,
          observed_valid_quote_count,proof_quote_count,quote_snapshot_sha256,
          payload_json,payload_sha256,research_only,execution_eligible,
          can_place_orders,can_authorize,can_promote,contract_id,cohort_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            payload["capture_id"],
            payload["clock_id"],
            payload["horizon_min"],
            payload["target_utc"],
            payload["capture_read_started_utc"],
            payload["attempted_utc"],
            payload["attempt_delay_sec"],
            payload["timing_quality"],
            payload["invalid_reason"],
            payload["observed_valid_quote_count"],
            payload["proof_quote_count"],
            payload["quote_snapshot_sha256"],
            payload_json,
            sha256_text(payload_json),
            1,
            0,
            0,
            0,
            0,
            CONTRACT_ID,
            COHORT_ID,
        ),
    )
    connection.commit()
    return bool(cursor.rowcount)


def database_counts(connection: sqlite3.Connection) -> dict[str, int]:
    clocks = int(connection.execute("SELECT COUNT(*) FROM scheduled_event_clock").fetchone()[0])
    row = connection.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN timing_quality='prospective_exact_live_quote' THEN 1 ELSE 0 END),
               SUM(CASE WHEN timing_quality!='prospective_exact_live_quote' THEN 1 ELSE 0 END),
               SUM(proof_quote_count)
        FROM scheduled_event_quote_capture
        """
    ).fetchone()
    return {
        "registered_event_clocks": clocks,
        "terminal_capture_attempts": int(row[0] or 0),
        "exact_all68_captures": int(row[1] or 0),
        "invalid_terminal_captures": int(row[2] or 0),
        "proof_quote_rows": int(row[3] or 0),
    }


def run_cycle(
    *,
    config_path: Path = CONFIG_PATH,
    preflight_path: Path = PREFLIGHT_PATH,
    quote_path: Path = QUOTE_PATH,
    database_path: Path = DATABASE_PATH,
    state_path: Path = STATE_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    now: dt.datetime | None = None,
    quote_loader: Callable[[Path], Mapping[str, Any]] = load_quote_snapshot,
) -> dict[str, Any]:
    validate_config(config_path)
    cycle_now = now or utc_now()
    if cycle_now.tzinfo is None:
        cycle_now = cycle_now.replace(tzinfo=dt.timezone.utc)
    cycle_now = cycle_now.astimezone(dt.timezone.utc)
    connection = open_database(database_path)
    try:
        preflight = read_json(preflight_path, {})
        registered, registration_error = register_upcoming_clocks(
            connection, preflight, now=cycle_now
        )
        attempted = 0
        inserted = 0
        for due in due_attempts(connection, now=cycle_now):
            target = parse_time(due["target_utc"])
            assert target is not None
            read_started = cycle_now if now is not None else utc_now()
            delay_before_read = (read_started - target).total_seconds()
            quote_payload: Mapping[str, Any] = {}
            snapshot_error = ""
            if delay_before_read <= MAXIMUM_ATTEMPT_DELAY_SEC:
                try:
                    quote_payload = quote_loader(quote_path)
                except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
                    snapshot_error = f"{type(exc).__name__}:{exc}"
            else:
                snapshot_error = "target_late_before_snapshot_read"
            attempted_at = cycle_now if now is not None else utc_now()
            capture = build_capture(
                due,
                quote_payload,
                read_started_utc=read_started,
                attempted_utc=attempted_at,
                snapshot_error=snapshot_error,
            )
            attempted += 1
            inserted += int(insert_capture(connection, capture))
        counts = database_counts(connection)
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        next_row = connection.execute(
            """
            SELECT c.scheduled_utc,c.event_id,c.headline
            FROM scheduled_event_clock c
            WHERE NOT EXISTS (
              SELECT 1 FROM scheduled_event_quote_capture q
              WHERE q.clock_id=c.clock_id AND q.horizon_min=0
            )
            ORDER BY c.scheduled_utc LIMIT 1
            """
        ).fetchone()
    finally:
        connection.close()
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "generated_utc": iso_utc(cycle_now),
        "status": "ok" if integrity == "ok" else "degraded",
        "database_integrity": integrity,
        "registered_this_cycle": registered,
        "attempted_this_cycle": attempted,
        "inserted_this_cycle": inserted,
        "registration_error": registration_error,
        "counts": counts,
        "next_uncaptured_event": (
            None
            if next_row is None
            else {
                "scheduled_utc": str(next_row[0]),
                "event_id": str(next_row[1]),
                "headline": str(next_row[2]),
            }
        ),
        "horizons_min": list(HORIZONS_MIN),
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "clock_semantics": "scheduled_release_time_not_source_first_seen_time",
        "direction_policy": "abstain",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    write_json_atomic(state_path, result)
    write_json_atomic(heartbeat_path, result)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args(argv)
    started = time.monotonic()
    while True:
        cycle_started = time.monotonic()
        try:
            run_cycle()
        except Exception as exc:  # keep the supervised research worker observable
            write_json_atomic(
                HEARTBEAT_PATH,
                {
                    "schema_version": SCHEMA_VERSION,
                    "contract_id": CONTRACT_ID,
                    "cohort_id": COHORT_ID,
                    "generated_utc": iso_utc(),
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}",
                    "research_only": True,
                    "execution_eligible": False,
                    "can_place_orders": False,
                    "can_authorize": False,
                    "can_promote": False,
                },
            )
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        elapsed = time.monotonic() - cycle_started
        time.sleep(max(0.1, args.interval_sec - elapsed))


if __name__ == "__main__":
    raise SystemExit(main())
