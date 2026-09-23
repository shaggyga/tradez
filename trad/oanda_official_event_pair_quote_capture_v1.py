#!/usr/bin/env python3
"""Prospectively freeze per-pair executable quotes at raw official-event time.

The original all-68 sidecar remains immutable and keeps its exact-universe
proof gate.  This separate research cohort addresses a different question:
which individual pairs had a fresh, explicitly tradeable OANDA bid/ask when a
raw official item first became known?  A stale or nontradeable exotic is
recorded but cannot erase valid quotes for unrelated pairs.

This worker reads only the append-only raw official-event ledger before taking
its one terminal quote snapshot.  It does not read semantic mappings, cannot
retry a missed event, and has no execution, lifecycle, authorization, or
promotion surface.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping, Sequence

from oanda_official_release_fast_lane_contract import (
    OFFICIAL_RELEASE_FAST_LANE_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID,
)
from oanda_quote_transport import load_quote_snapshot
import oanda_official_release_fast_lane as fast_lane


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG_PATH = ROOT / "config" / "official_event_pair_quote_capture_v1.json"
INPUT_DATABASE = LOCAL_NEWS / "official_release_fast_lane_v4.sqlite"
QUOTE_PATH = STATE / "practice_007_market_quotes_v1.json"
OUTPUT_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v1.sqlite"
SNAPSHOT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_latest_v1.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_quote_capture_heartbeat_v1.json"

SCHEMA_VERSION = "official_event_pair_quote_capture_v1"
CONTRACT_ID = "official_event_pair_quote_capture_v1_per_pair_tradeability_20260902"
COHORT_ID = "official_event_pair_quote_capture_v1_20260902a"
ACTIVATED_UTC = dt.datetime(2026, 9, 2, 20, 15, tzinfo=dt.timezone.utc)
REQUIRED_RAW_COLLECTOR_CONTRACT_ID = OFFICIAL_RELEASE_FAST_LANE_CONTRACT_ID
REQUIRED_RAW_COLLECTOR_COHORT_ID = OFFICIAL_RELEASE_FAST_LANE_COHORT_ID
REQUIRED_QUOTE_SNAPSHOT_SCHEMA_VERSION = 3
REQUIRED_QUOTE_SNAPSHOT_PRODUCER = "practice_007_fast_executor_price_stream"
EXPECTED_INSTRUMENTS = tuple(fast_lane.EXPECTED_QUOTE_INSTRUMENTS)
EXPECTED_INSTRUMENT_COUNT = fast_lane.EXPECTED_QUOTE_COUNT
EXPECTED_UNIVERSE_SHA256 = fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
MAXIMUM_DETECTION_LATENCY_SECONDS = 15.0
MAXIMUM_SNAPSHOT_AGE_SECONDS = 5.0
MAXIMUM_QUOTE_AGE_SECONDS = 30.0
MAXIMUM_FUTURE_SKEW_SECONDS = 2.0
POLICY = {
    "prospective_only": True,
    "historical_backfill_allowed": False,
    "one_terminal_capture_attempt_per_observation": True,
    "explicit_tradeable_true_required_per_pair": True,
    "stale_or_nontradeable_pair_does_not_invalidate_other_pairs": True,
    "all_68_surface_retained_for_audit": True,
    "exact_all_68_metric_retained": True,
    "semantic_mapping_read_before_capture": False,
    "research_only": True,
    "execution_eligible": False,
    "can_place_orders": False,
    "can_authorize": False,
    "can_promote": False,
    "supported_execution_decision": "no_trade",
}


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


def finite_number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = read_json(path)
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "required_raw_collector_contract_id": REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
        "required_raw_collector_cohort_id": REQUIRED_RAW_COLLECTOR_COHORT_ID,
        "required_quote_snapshot_schema_version": REQUIRED_QUOTE_SNAPSHOT_SCHEMA_VERSION,
        "required_quote_snapshot_producer": REQUIRED_QUOTE_SNAPSHOT_PRODUCER,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"pair quote capture config mismatch:{key}")
    if parse_time(payload.get("activated_utc")) != ACTIVATED_UTC:
        raise ValueError("pair quote capture config mismatch:activated_utc")
    timing = payload.get("timing")
    timing = timing if isinstance(timing, Mapping) else {}
    for key, value in {
        "maximum_detection_latency_seconds": MAXIMUM_DETECTION_LATENCY_SECONDS,
        "maximum_snapshot_age_seconds": MAXIMUM_SNAPSHOT_AGE_SECONDS,
        "maximum_quote_age_seconds": MAXIMUM_QUOTE_AGE_SECONDS,
        "maximum_future_skew_seconds": MAXIMUM_FUTURE_SKEW_SECONDS,
    }.items():
        if finite_number(timing.get(key)) != value:
            raise ValueError(f"pair quote capture config mismatch:{key}")
    configured_policy = payload.get("policy")
    configured_policy = (
        configured_policy if isinstance(configured_policy, Mapping) else {}
    )
    for key, value in POLICY.items():
        if configured_policy.get(key) != value:
            raise ValueError(f"pair quote capture config mismatch:policy.{key}")
    return payload


def open_output_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS official_event_pair_quote_capture (
            capture_id TEXT PRIMARY KEY,
            observation_id TEXT NOT NULL UNIQUE,
            source_id TEXT NOT NULL,
            source_contract_id TEXT NOT NULL,
            event_first_known_utc TEXT NOT NULL,
            captured_utc TEXT NOT NULL,
            detection_latency_seconds REAL NOT NULL,
            timing_quality TEXT NOT NULL,
            eligible_quote_count INTEGER NOT NULL,
            explicitly_tradeable_quote_count INTEGER NOT NULL,
            exact_all_68_available INTEGER NOT NULL
                CHECK(exact_all_68_available IN (0,1)),
            raw_observation_payload_sha256 TEXT NOT NULL,
            quote_snapshot_payload_sha256 TEXT NOT NULL,
            capture_payload_json TEXT NOT NULL,
            research_only INTEGER NOT NULL CHECK(research_only=1),
            execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
            can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
            can_promote INTEGER NOT NULL CHECK(can_promote=0),
            contract_id TEXT NOT NULL,
            cohort_id TEXT NOT NULL,
            activated_utc TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pair_quote_capture_quality
          ON official_event_pair_quote_capture(timing_quality,captured_utc);
        CREATE TRIGGER IF NOT EXISTS pair_quote_capture_no_update
        BEFORE UPDATE ON official_event_pair_quote_capture BEGIN
          SELECT RAISE(ABORT,'append_only:official_event_pair_quote_capture');
        END;
        CREATE TRIGGER IF NOT EXISTS pair_quote_capture_no_delete
        BEFORE DELETE ON official_event_pair_quote_capture BEGIN
          SELECT RAISE(ABORT,'append_only:official_event_pair_quote_capture');
        END;
        """
    )
    connection.commit()
    return connection


def read_pending_observations(
    input_database: Path,
    existing_observation_ids: set[str],
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    if not input_database.exists():
        return []
    uri = f"file:{input_database.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        rows = connection.execute(
            """
            SELECT observation_id,source_id,source_contract_id,first_seen_utc,
                   prospective_observation,listing_bootstrap,
                   publisher_time_eligible,raw_payload_json,
                   collector_contract_id,collector_cohort_id
            FROM official_release_observation
            WHERE first_seen_utc >= ?
              AND prospective_observation=1
              AND collector_contract_id=?
              AND collector_cohort_id=?
            ORDER BY first_seen_utc,observation_id
            LIMIT ?
            """,
            (
                iso_utc(ACTIVATED_UTC),
                REQUIRED_RAW_COLLECTOR_CONTRACT_ID,
                REQUIRED_RAW_COLLECTOR_COHORT_ID,
                max(1, int(limit) + len(existing_observation_ids)),
            ),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            observation_id = str(row[0])
            if observation_id in existing_observation_ids:
                continue
            result.append(
                {
                    "observation_id": observation_id,
                    "source_id": str(row[1]),
                    "source_contract_id": str(row[2]),
                    "first_seen_utc": str(row[3]),
                    "prospective_observation": bool(row[4]),
                    "listing_bootstrap": bool(row[5]),
                    "publisher_time_eligible": bool(row[6]),
                    "raw_payload_json": str(row[7]),
                    "collector_contract_id": str(row[8]),
                    "collector_cohort_id": str(row[9]),
                }
            )
            if len(result) >= limit:
                break
        return result
    finally:
        connection.close()


def _graph_components(instruments: Sequence[str]) -> list[list[str]]:
    graph: dict[str, set[str]] = {}
    for instrument in instruments:
        parts = str(instrument).upper().split("_")
        if len(parts) != 2:
            continue
        base, quote = parts
        graph.setdefault(base, set()).add(quote)
        graph.setdefault(quote, set()).add(base)
    components: list[list[str]] = []
    unseen = set(graph)
    while unseen:
        root = min(unseen)
        stack = [root]
        component: set[str] = set()
        while stack:
            node = stack.pop()
            if node in component:
                continue
            component.add(node)
            stack.extend(sorted(graph.get(node, set()) - component))
        unseen -= component
        components.append(sorted(component))
    return sorted(components, key=lambda row: (-len(row), row))


def build_capture(
    observation: Mapping[str, Any],
    quote_payload: Mapping[str, Any],
    captured_utc: dt.datetime,
) -> dict[str, Any]:
    captured = captured_utc.astimezone(dt.timezone.utc)
    event_time = parse_time(observation.get("first_seen_utc"))
    if event_time is None:
        event_time = captured
    latency = (captured - event_time).total_seconds()
    snapshot_generated = parse_time(quote_payload.get("generated_utc"))
    snapshot_age = (
        None
        if snapshot_generated is None
        else (captured - snapshot_generated).total_seconds()
    )
    quotes_raw = quote_payload.get("quotes")
    quotes = quotes_raw if isinstance(quotes_raw, Mapping) else {}
    normalized_quotes = {str(key).upper(): value for key, value in quotes.items()}
    coverage = quote_payload.get("coverage")
    coverage = coverage if isinstance(coverage, Mapping) else {}
    transport = quote_payload.get("transport")
    transport = transport if isinstance(transport, Mapping) else {}

    metadata_reasons: list[str] = []
    if latency < 0.0 or latency > MAXIMUM_DETECTION_LATENCY_SECONDS:
        metadata_reasons.append(f"detection_latency_seconds:{latency:.6f}")
    if int(quote_payload.get("schema_version") or 0) != REQUIRED_QUOTE_SNAPSHOT_SCHEMA_VERSION:
        metadata_reasons.append("quote_snapshot_schema_mismatch")
    if str(quote_payload.get("producer") or "") != REQUIRED_QUOTE_SNAPSHOT_PRODUCER:
        metadata_reasons.append("quote_snapshot_producer_mismatch")
    if snapshot_generated is None:
        metadata_reasons.append("quote_snapshot_generated_clock_missing")
    elif snapshot_age is not None and (
        snapshot_age < -MAXIMUM_FUTURE_SKEW_SECONDS
        or snapshot_age > MAXIMUM_SNAPSHOT_AGE_SECONDS
    ):
        metadata_reasons.append(f"quote_snapshot_age_seconds:{snapshot_age:.6f}")
    if int(quote_payload.get("quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT:
        metadata_reasons.append("quote_snapshot_count_not_68")
    if int(coverage.get("current_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT:
        metadata_reasons.append("coverage_current_quote_count_not_68")
    if int(coverage.get("last_known_quote_count") or 0) != EXPECTED_INSTRUMENT_COUNT:
        metadata_reasons.append("coverage_last_known_quote_count_not_68")
    if int(coverage.get("retained_last_known_count") or 0) != 0:
        metadata_reasons.append("retained_last_known_quotes_present")
    expected = set(EXPECTED_INSTRUMENTS)
    provided = set(normalized_quotes)
    missing = sorted(expected - provided)
    unexpected = sorted(provided - expected)
    if missing:
        metadata_reasons.append(f"missing_instruments:{len(missing)}")
    if unexpected:
        metadata_reasons.append(f"unexpected_instruments:{len(unexpected)}")

    pair_rows: dict[str, dict[str, Any]] = {}
    eligible_quotes: dict[str, dict[str, Any]] = {}
    explicitly_tradeable = 0
    invalid_reason_counts: dict[str, int] = {}
    for instrument in EXPECTED_INSTRUMENTS:
        raw = normalized_quotes.get(instrument)
        raw = raw if isinstance(raw, Mapping) else {}
        bid = finite_number(raw.get("bid"))
        ask = finite_number(raw.get("ask"))
        pip = finite_number(raw.get("pip"))
        quote_time = parse_time(raw.get("time"))
        tradeable = raw.get("tradeable") is True
        if tradeable:
            explicitly_tradeable += 1
        reason = ""
        if not raw:
            reason = "missing_quote"
        elif not tradeable:
            reason = "not_explicitly_tradeable"
        elif bid is None or ask is None or ask <= bid:
            reason = "invalid_executable_bid_ask"
        elif pip is None or pip <= 0.0:
            reason = "invalid_pip"
        elif quote_time is None:
            reason = "invalid_quote_time"
        else:
            quote_age = (captured - quote_time).total_seconds()
            if quote_age < -MAXIMUM_FUTURE_SKEW_SECONDS:
                reason = "quote_clock_ahead_of_capture"
            elif quote_age > MAXIMUM_QUOTE_AGE_SECONDS:
                reason = "stale_quote"
        eligible = bool(not metadata_reasons and not reason)
        if reason:
            invalid_reason_counts[reason] = invalid_reason_counts.get(reason, 0) + 1
        row = {
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "quote_time_utc": "" if quote_time is None else iso_utc(quote_time),
            "quote_age_seconds": (
                None
                if quote_time is None
                else round((captured - quote_time).total_seconds(), 6)
            ),
            "event_offset_seconds": (
                None
                if quote_time is None
                else round((quote_time - event_time).total_seconds(), 6)
            ),
            "tradeable": tradeable,
            "source": str(raw.get("source") or ""),
            "eligible": eligible,
            "invalid_reason": reason,
        }
        pair_rows[instrument] = row
        if eligible:
            eligible_quotes[instrument] = {
                key: row[key]
                for key in (
                    "bid",
                    "ask",
                    "pip",
                    "quote_time_utc",
                    "quote_age_seconds",
                    "event_offset_seconds",
                    "tradeable",
                    "source",
                )
            }

    exact_all_68 = bool(len(eligible_quotes) == EXPECTED_INSTRUMENT_COUNT)
    if metadata_reasons:
        timing_quality = "prospective_pair_quote_snapshot_invalid"
    elif eligible_quotes:
        timing_quality = "prospective_per_pair_executable_quotes"
    else:
        timing_quality = "prospective_no_pair_quote_eligible"
    raw_payload = str(observation.get("raw_payload_json") or "")
    capture_id = sha256_text(
        f"{observation.get('observation_id') or ''}|{CONTRACT_ID}|{COHORT_ID}"
    )
    components = _graph_components(sorted(eligible_quotes))
    currencies = sorted({part for item in eligible_quotes for part in item.split("_")})
    return {
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture_id,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "observation_id": str(observation.get("observation_id") or ""),
        "source_id": str(observation.get("source_id") or ""),
        "source_contract_id": str(observation.get("source_contract_id") or ""),
        "raw_collector_contract_id": str(observation.get("collector_contract_id") or ""),
        "raw_collector_cohort_id": str(observation.get("collector_cohort_id") or ""),
        "event_first_known_utc": iso_utc(event_time),
        "captured_utc": iso_utc(captured),
        "detection_latency_seconds": round(latency, 6),
        "timing_quality": timing_quality,
        "metadata_invalid_reasons": metadata_reasons,
        "eligible_quote_count": len(eligible_quotes),
        "explicitly_tradeable_quote_count": explicitly_tradeable,
        "exact_all_68_available": exact_all_68,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "missing_instruments": missing,
        "unexpected_instruments": unexpected,
        "invalid_reason_counts": invalid_reason_counts,
        "eligible_instruments": sorted(eligible_quotes),
        "eligible_currency_count": len(currencies),
        "eligible_currencies": currencies,
        "currency_graph_components": components,
        "eligible_currency_graph_connected": len(components) == 1,
        "pair_rows": pair_rows,
        "eligible_quotes": eligible_quotes,
        "quote_snapshot_generated_utc": (
            "" if snapshot_generated is None else iso_utc(snapshot_generated)
        ),
        "quote_snapshot_age_seconds": (
            None if snapshot_age is None else round(snapshot_age, 6)
        ),
        "quote_snapshot_schema_version": quote_payload.get("schema_version"),
        "quote_snapshot_producer": str(quote_payload.get("producer") or ""),
        "quote_transport_source": str(transport.get("source") or ""),
        "quote_transport_sequence": transport.get("sequence"),
        "connection_generation": quote_payload.get("connection_generation"),
        "raw_observation_payload_sha256": sha256_text(raw_payload),
        "quote_snapshot_payload_sha256": sha256_text(canonical_json(quote_payload)),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
    }


def insert_capture(connection: sqlite3.Connection, capture: Mapping[str, Any]) -> bool:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO official_event_pair_quote_capture (
            capture_id,observation_id,source_id,source_contract_id,
            event_first_known_utc,captured_utc,detection_latency_seconds,
            timing_quality,eligible_quote_count,
            explicitly_tradeable_quote_count,exact_all_68_available,
            raw_observation_payload_sha256,quote_snapshot_payload_sha256,
            capture_payload_json,research_only,execution_eligible,
            can_authorize,can_promote,contract_id,cohort_id,activated_utc
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,0,?,?,?)
        """,
        (
            capture["capture_id"],
            capture["observation_id"],
            capture["source_id"],
            capture["source_contract_id"],
            capture["event_first_known_utc"],
            capture["captured_utc"],
            capture["detection_latency_seconds"],
            capture["timing_quality"],
            capture["eligible_quote_count"],
            capture["explicitly_tradeable_quote_count"],
            int(bool(capture["exact_all_68_available"])),
            capture["raw_observation_payload_sha256"],
            capture["quote_snapshot_payload_sha256"],
            canonical_json(capture),
            CONTRACT_ID,
            COHORT_ID,
            iso_utc(ACTIVATED_UTC),
        ),
    )
    return bool(cursor.rowcount)


def diagnostics(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM(timing_quality='prospective_per_pair_executable_quotes'),0),
               COALESCE(SUM(exact_all_68_available),0),
               COALESCE(SUM(eligible_quote_count),0),
               COALESCE(MIN(eligible_quote_count),0),
               COALESCE(MAX(eligible_quote_count),0),
               COALESCE(MAX(captured_utc),'')
        FROM official_event_pair_quote_capture
        """
    ).fetchone()
    latest = connection.execute(
        """
        SELECT observation_id,source_id,event_first_known_utc,captured_utc,
               detection_latency_seconds,timing_quality,eligible_quote_count,
               explicitly_tradeable_quote_count,exact_all_68_available
        FROM official_event_pair_quote_capture
        ORDER BY captured_utc DESC,observation_id DESC
        LIMIT 5
        """
    ).fetchall()
    return {
        "capture_count": int(row[0]),
        "per_pair_ready_count": int(row[1]),
        "exact_all_68_count": int(row[2]),
        "eligible_pair_quote_total": int(row[3]),
        "minimum_eligible_quote_count": int(row[4]),
        "maximum_eligible_quote_count": int(row[5]),
        "latest_captured_utc": str(row[6]),
        "latest": [
            {
                "observation_id": str(item[0]),
                "source_id": str(item[1]),
                "event_first_known_utc": str(item[2]),
                "captured_utc": str(item[3]),
                "detection_latency_seconds": float(item[4]),
                "timing_quality": str(item[5]),
                "eligible_quote_count": int(item[6]),
                "explicitly_tradeable_quote_count": int(item[7]),
                "exact_all_68_available": bool(item[8]),
            }
            for item in latest
        ],
    }


def run_cycle(
    *,
    input_database: Path = INPUT_DATABASE,
    quote_path: Path = QUOTE_PATH,
    output_database: Path = OUTPUT_DATABASE,
    snapshot_path: Path = SNAPSHOT_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    validate_config()
    cycle_started = (observed_utc or utc_now()).astimezone(dt.timezone.utc)
    connection = open_output_database(output_database)
    inserted = 0
    pending_count = 0
    error = ""
    try:
        existing = {
            str(row[0])
            for row in connection.execute(
                "SELECT observation_id FROM official_event_pair_quote_capture"
            ).fetchall()
        }
        pending = read_pending_observations(input_database, existing)
        pending_count = len(pending)
        if pending:
            quote_payload = load_quote_snapshot(quote_path)
            captured = utc_now() if observed_utc is None else cycle_started
            for observation in pending:
                capture = build_capture(observation, quote_payload, captured)
                inserted += int(insert_capture(connection, capture))
            connection.commit()
        counts = diagnostics(connection)
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        connection.rollback()
        error = f"{type(exc).__name__}: {exc}"
        counts = diagnostics(connection)
        integrity = "error"
    finally:
        connection.close()
    completed = utc_now() if observed_utc is None else cycle_started
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "generated_utc": iso_utc(completed),
        "status": "ok" if not error and integrity == "ok" else "error",
        "error": error,
        "input_database": str(input_database.resolve()),
        "quote_path": str(quote_path.resolve()),
        "output_database": str(output_database.resolve()),
        "pending_observations_seen": pending_count,
        "inserted_captures": inserted,
        "counts": counts,
        "sqlite_integrity": integrity,
        "policy": dict(POLICY),
    }
    write_json_atomic(snapshot_path, snapshot)
    write_json_atomic(
        heartbeat_path,
        {
            "schema_version": 1,
            "worker": "oanda_official_event_pair_quote_capture_v1",
            "status": "running" if snapshot["status"] == "ok" else "error",
            "updated_at": snapshot["generated_utc"],
            "phase": "polling_raw_official_events",
            "details": {
                "contract_id": CONTRACT_ID,
                "cohort_id": COHORT_ID,
                "capture_count": counts["capture_count"],
                "per_pair_ready_count": counts["per_pair_ready_count"],
                "exact_all_68_count": counts["exact_all_68_count"],
                "last_error": error,
                "research_only": True,
                "execution_eligible": False,
            },
        },
    )
    if error:
        raise RuntimeError(error)
    return snapshot


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-database", type=Path, default=INPUT_DATABASE)
    parser.add_argument("--quote-path", type=Path, default=QUOTE_PATH)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    while True:
        snapshot = run_cycle(
            input_database=args.input_database,
            quote_path=args.quote_path,
            output_database=args.output_database,
            snapshot_path=args.snapshot,
            heartbeat_path=args.heartbeat,
        )
        if not args.quiet:
            print(canonical_json(snapshot), flush=True)
        if args.once or (
            args.duration_sec > 0.0
            and time.monotonic() - started >= args.duration_sec
        ):
            break
        time.sleep(max(0.25, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACTIVATED_UTC",
    "COHORT_ID",
    "CONTRACT_ID",
    "EXPECTED_INSTRUMENTS",
    "POLICY",
    "build_capture",
    "diagnostics",
    "open_output_database",
    "read_pending_observations",
    "run_cycle",
    "validate_config",
]
