#!/usr/bin/env python3
"""Prospective per-pair executable outcomes for raw official-event clocks.

The upstream pair quote cohort freezes T0 before semantic parsing.  This
worker makes one terminal attempt at each declared horizon.  Every pair is
judged independently: a stale or non-tradeable exotic cannot erase a valid
major-pair outcome.  Entry quotes are never reacquired, and both directions
are recorded without making a semantic or trading claim.
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
from typing import Any, Mapping

from oanda_quote_transport import load_quote_snapshot


ROOT = Path(__file__).resolve().parent
LOCAL_NEWS = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
CONFIG_PATH = ROOT / "config" / "official_event_pair_horizon_capture_v1.json"
INPUT_DATABASE = LOCAL_NEWS / "official_event_pair_quote_capture_v1.sqlite"
OUTPUT_DATABASE = LOCAL_NEWS / "official_event_pair_horizon_capture_v1.sqlite"
QUOTE_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state"
    / "practice_007_market_quotes_v1.json"
)
STATE_PATH = LOCAL_NEWS / "official_event_pair_horizon_capture_latest_v1.json"
HEARTBEAT_PATH = LOCAL_NEWS / "official_event_pair_horizon_capture_heartbeat_v1.json"

SCHEMA_VERSION = "official_event_pair_horizon_capture_v1"
CONTRACT_ID = "official_event_pair_horizon_capture_v1_per_pair_terminal_20260902"
COHORT_ID = "official_event_pair_horizon_capture_v1_20260902a"
ACTIVATED_UTC = dt.datetime(2026, 9, 3, 0, 20, tzinfo=dt.timezone.utc)
ENTRY_CONTRACT_ID = "official_event_pair_quote_capture_v1_per_pair_tradeability_20260902"
ENTRY_COHORT_ID = "official_event_pair_quote_capture_v1_20260902a"
SNAPSHOT_SCHEMA_VERSION = 3
SNAPSHOT_PRODUCER = "practice_007_fast_executor_price_stream"
EXPECTED_INSTRUMENT_COUNT = 68
EXPECTED_UNIVERSE_SHA256 = "b6abe559dcaa9faa0a1e48217f97a12dc9043eb9595d2b946bf80f93035be142"
HORIZONS_MIN = (1, 5, 15, 30, 60)
SLIPPAGE_STRESS_PIPS = (0.0, 0.25, 0.5)
MAX_ATTEMPT_DELAY_SEC = 15.0
MAX_SNAPSHOT_AGE_SEC = 5.0
MAX_QUOTE_AGE_SEC = 30.0
MAX_FUTURE_SKEW_SEC = 2.0
MAX_TARGET_OFFSET_SEC = 20.0

POLICY = {
    "prospective_only": True,
    "historical_backfill_allowed": False,
    "one_terminal_attempt_per_event_horizon": True,
    "input_entry_quote_reacquisition_allowed": False,
    "explicit_tradeable_true_required_per_pair": True,
    "invalid_pair_does_not_invalidate_other_pairs": True,
    "missing_or_late_attempt_policy": "persist_terminal_invalid_never_retry",
    "both_directions_recorded_without_semantic_claim": True,
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


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


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
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
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


def validate_frozen_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = read_json(path, {})
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "required_entry_capture_contract_id": ENTRY_CONTRACT_ID,
        "required_entry_capture_cohort_id": ENTRY_COHORT_ID,
        "required_quote_snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
        "required_quote_snapshot_producer": SNAPSHOT_PRODUCER,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "horizons_min": list(HORIZONS_MIN),
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"frozen pair-horizon config mismatch:{key}")
    if (payload.get("cost_stress") or {}).get("round_trip_slippage_pips") != list(
        SLIPPAGE_STRESS_PIPS
    ):
        raise ValueError("frozen pair-horizon config mismatch:cost_stress")
    timing = payload.get("timing") or {}
    for key, value in {
        "maximum_attempt_delay_seconds": MAX_ATTEMPT_DELAY_SEC,
        "maximum_snapshot_age_seconds": MAX_SNAPSHOT_AGE_SEC,
        "maximum_quote_age_seconds": MAX_QUOTE_AGE_SEC,
        "maximum_future_skew_seconds": MAX_FUTURE_SKEW_SEC,
        "maximum_target_offset_seconds": MAX_TARGET_OFFSET_SEC,
    }.items():
        if finite(timing.get(key)) != value:
            raise ValueError(f"frozen pair-horizon timing mismatch:{key}")
    policy = payload.get("policy") or {}
    for key, value in POLICY.items():
        if policy.get(key) != value:
            raise ValueError(f"unsafe pair-horizon policy:{key}")
    return payload


def open_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS official_event_pair_horizon_attempt (
          attempt_id TEXT PRIMARY KEY,
          input_capture_id TEXT NOT NULL,
          observation_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          event_first_known_utc TEXT NOT NULL,
          horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60)),
          target_utc TEXT NOT NULL,
          read_started_utc TEXT NOT NULL,
          attempted_utc TEXT NOT NULL,
          attempt_delay_seconds REAL NOT NULL,
          input_pair_count INTEGER NOT NULL,
          valid_pair_count INTEGER NOT NULL,
          invalid_pair_count INTEGER NOT NULL,
          snapshot_payload_sha256 TEXT NOT NULL,
          input_capture_payload_sha256 TEXT NOT NULL,
          component_root_sha256 TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          UNIQUE(input_capture_id,horizon_min)
        );
        CREATE TABLE IF NOT EXISTS official_event_pair_horizon_outcome (
          outcome_id TEXT PRIMARY KEY,
          attempt_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60)),
          instrument TEXT NOT NULL,
          valid INTEGER NOT NULL CHECK(valid IN (0,1)),
          invalid_reason TEXT NOT NULL,
          entry_bid REAL,
          entry_ask REAL,
          exit_bid REAL,
          exit_ask REAL,
          pip REAL,
          entry_quote_utc TEXT NOT NULL,
          exit_quote_utc TEXT NOT NULL,
          exit_quote_age_seconds REAL,
          target_offset_seconds REAL,
          signed_mid_move_pips REAL,
          buy_executable_pips REAL,
          sell_executable_pips REAL,
          cost_stress_json TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          UNIQUE(attempt_id,instrument),
          FOREIGN KEY(attempt_id) REFERENCES official_event_pair_horizon_attempt(attempt_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pair_horizon_due
          ON official_event_pair_horizon_attempt(event_first_known_utc,horizon_min);
        CREATE INDEX IF NOT EXISTS idx_pair_horizon_outcome
          ON official_event_pair_horizon_outcome(attempt_id,instrument,valid);
        CREATE TRIGGER IF NOT EXISTS pair_horizon_attempt_no_update
          BEFORE UPDATE ON official_event_pair_horizon_attempt
          BEGIN SELECT RAISE(ABORT,'append_only:pair_horizon_attempt'); END;
        CREATE TRIGGER IF NOT EXISTS pair_horizon_attempt_no_delete
          BEFORE DELETE ON official_event_pair_horizon_attempt
          BEGIN SELECT RAISE(ABORT,'append_only:pair_horizon_attempt'); END;
        CREATE TRIGGER IF NOT EXISTS pair_horizon_outcome_no_update
          BEFORE UPDATE ON official_event_pair_horizon_outcome
          BEGIN SELECT RAISE(ABORT,'append_only:pair_horizon_outcome'); END;
        CREATE TRIGGER IF NOT EXISTS pair_horizon_outcome_no_delete
          BEFORE DELETE ON official_event_pair_horizon_outcome
          BEGIN SELECT RAISE(ABORT,'append_only:pair_horizon_outcome'); END;
        """
    )
    connection.commit()
    return connection


def _snapshot_metadata_reason(
    snapshot: Mapping[str, Any], attempted_utc: dt.datetime
) -> str:
    generated = parse_time(snapshot.get("generated_utc"))
    quotes = snapshot.get("quotes")
    coverage = snapshot.get("coverage")
    coverage = coverage if isinstance(coverage, Mapping) else {}
    generation = snapshot.get("connection_generation")
    if int(snapshot.get("schema_version") or -1) != SNAPSHOT_SCHEMA_VERSION:
        return "snapshot_schema_mismatch"
    if str(snapshot.get("producer") or "") != SNAPSHOT_PRODUCER:
        return "snapshot_producer_mismatch"
    if not isinstance(quotes, Mapping) or len(quotes) != EXPECTED_INSTRUMENT_COUNT:
        return "snapshot_universe_incomplete"
    if int(snapshot.get("quote_count") or -1) != EXPECTED_INSTRUMENT_COUNT:
        return "snapshot_quote_count_mismatch"
    if int(coverage.get("current_quote_count") or -1) != EXPECTED_INSTRUMENT_COUNT:
        return "snapshot_coverage_count_mismatch"
    if generation is None or generation != coverage.get("connection_generation"):
        return "snapshot_connection_generation_mismatch"
    if generated is None:
        return "snapshot_clock_missing"
    age = (attempted_utc - generated).total_seconds()
    if age < -MAX_FUTURE_SKEW_SEC:
        return "snapshot_clock_ahead"
    if age > MAX_SNAPSHOT_AGE_SEC:
        return "snapshot_stale"
    return ""


def _pair_outcome(
    instrument: str,
    entry: Mapping[str, Any],
    exit_row: Any,
    *,
    target_utc: dt.datetime,
    attempted_utc: dt.datetime,
    metadata_reason: str,
) -> dict[str, Any]:
    reason = metadata_reason
    exit_quote = exit_row if isinstance(exit_row, Mapping) else {}
    entry_bid, entry_ask, pip = (
        finite(entry.get("bid")), finite(entry.get("ask")), finite(entry.get("pip"))
    )
    exit_bid, exit_ask = finite(exit_quote.get("bid")), finite(exit_quote.get("ask"))
    entry_time = parse_time(entry.get("quote_time_utc"))
    exit_time = parse_time(exit_quote.get("time"))
    if not reason and entry.get("tradeable") is not True:
        reason = "entry_not_explicitly_tradeable"
    if not reason and (
        entry_bid is None or entry_ask is None or pip is None
        or entry_bid <= 0 or entry_ask <= entry_bid or pip <= 0 or entry_time is None
    ):
        reason = "entry_quote_invalid"
    if not reason and exit_quote.get("tradeable") is not True:
        reason = "exit_not_explicitly_tradeable"
    if not reason and (
        exit_bid is None or exit_ask is None or exit_bid <= 0 or exit_ask <= exit_bid
        or exit_time is None
    ):
        reason = "exit_quote_invalid"
    exit_pip = finite(exit_quote.get("pip"))
    if not reason and exit_pip != pip:
        reason = "pip_mismatch"
    quote_age = None if exit_time is None else (attempted_utc - exit_time).total_seconds()
    target_offset = None if exit_time is None else (exit_time - target_utc).total_seconds()
    if not reason and quote_age is not None and quote_age < -MAX_FUTURE_SKEW_SEC:
        reason = "exit_quote_clock_ahead"
    if not reason and quote_age is not None and quote_age > MAX_QUOTE_AGE_SEC:
        reason = "exit_quote_stale"
    if not reason and target_offset is not None and abs(target_offset) > MAX_TARGET_OFFSET_SEC:
        reason = "exit_quote_outside_target_window"

    valid = not reason
    mid_move = buy = sell = None
    stresses: list[dict[str, float]] = []
    if valid:
        assert None not in (entry_bid, entry_ask, exit_bid, exit_ask, pip)
        entry_mid = (entry_bid + entry_ask) / 2.0
        exit_mid = (exit_bid + exit_ask) / 2.0
        mid_move = (exit_mid - entry_mid) / pip
        buy = (exit_bid - entry_ask) / pip
        sell = (entry_bid - exit_ask) / pip
        stresses = [
            {
                "round_trip_slippage_pips": slip,
                "buy_after_slippage_pips": round(buy - slip, 9),
                "sell_after_slippage_pips": round(sell - slip, 9),
            }
            for slip in SLIPPAGE_STRESS_PIPS
        ]
    return {
        "instrument": instrument,
        "valid": valid,
        "invalid_reason": reason,
        "entry_bid": entry_bid,
        "entry_ask": entry_ask,
        "exit_bid": exit_bid,
        "exit_ask": exit_ask,
        "pip": pip,
        "entry_quote_utc": iso_utc(entry_time) if entry_time else "",
        "exit_quote_utc": iso_utc(exit_time) if exit_time else "",
        "exit_quote_age_seconds": None if quote_age is None else round(quote_age, 6),
        "target_offset_seconds": None if target_offset is None else round(target_offset, 6),
        "signed_mid_move_pips": None if mid_move is None else round(mid_move, 9),
        "buy_executable_pips": None if buy is None else round(buy, 9),
        "sell_executable_pips": None if sell is None else round(sell, 9),
        "cost_stress": stresses,
        "spread_embedded_once_in_executable_endpoints": True,
        "semantic_direction_claim": "none_both_directions_recorded",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
    }


def build_attempt(
    entry_capture: Mapping[str, Any],
    horizon_min: int,
    snapshot: Mapping[str, Any],
    attempted_utc: dt.datetime,
    *,
    read_started_utc: dt.datetime | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    event = parse_time(entry_capture.get("event_first_known_utc"))
    if event is None:
        raise ValueError("entry event clock missing")
    target = event + dt.timedelta(minutes=int(horizon_min))
    attempt = attempted_utc.astimezone(dt.timezone.utc)
    started = (read_started_utc or attempt).astimezone(dt.timezone.utc)
    delay = (attempt - target).total_seconds()
    if delay < 0:
        raise ValueError("horizon not due")
    metadata_reason = _snapshot_metadata_reason(snapshot, attempt)
    if delay > MAX_ATTEMPT_DELAY_SEC:
        metadata_reason = f"attempt_clock_missed:{delay:.6f}"
    entries = entry_capture.get("eligible_quotes")
    entries = entries if isinstance(entries, Mapping) else {}
    exits = snapshot.get("quotes")
    exits = exits if isinstance(exits, Mapping) else {}
    outcomes = [
        _pair_outcome(
            instrument, entry, exits.get(instrument), target_utc=target,
            attempted_utc=attempt, metadata_reason=metadata_reason,
        )
        for instrument, entry in sorted(entries.items())
        if isinstance(entry, Mapping)
    ]
    valid_count = sum(bool(row["valid"]) for row in outcomes)
    input_capture_id = str(entry_capture.get("capture_id") or "")
    attempt_id = "official_event_pair_horizon_" + sha256_text(
        f"{input_capture_id}|{horizon_min}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    header = {
        "schema_version": SCHEMA_VERSION,
        "attempt_id": attempt_id,
        "input_capture_id": input_capture_id,
        "observation_id": str(entry_capture.get("observation_id") or ""),
        "source_id": str(entry_capture.get("source_id") or ""),
        "event_first_known_utc": iso_utc(event),
        "horizon_min": int(horizon_min),
        "target_utc": iso_utc(target),
        "read_started_utc": iso_utc(started),
        "attempted_utc": iso_utc(attempt),
        "attempt_delay_seconds": round(delay, 6),
        "input_pair_count": len(outcomes),
        "valid_pair_count": valid_count,
        "invalid_pair_count": len(outcomes) - valid_count,
        "snapshot_payload_sha256": sha256_text(canonical_json(snapshot)),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }
    return header, outcomes


def _insert_attempt(
    connection: sqlite3.Connection,
    header: Mapping[str, Any],
    outcomes: list[dict[str, Any]],
    *,
    input_capture_payload_sha256: str,
) -> None:
    rows: list[tuple[dict[str, Any], str, str]] = []
    for outcome in outcomes:
        payload = dict(outcome)
        payload["attempt_id"] = header["attempt_id"]
        payload["horizon_min"] = header["horizon_min"]
        payload["contract_id"] = CONTRACT_ID
        payload["cohort_id"] = COHORT_ID
        payload_json = canonical_json(payload)
        rows.append((payload, payload_json, sha256_text(payload_json)))
    component_root = sha256_text("".join(row[2] for row in rows))
    header_payload = dict(header)
    header_payload["input_capture_payload_sha256"] = input_capture_payload_sha256
    header_payload["component_root_sha256"] = component_root
    header_json = canonical_json(header_payload)
    with connection:
        connection.execute(
            """INSERT INTO official_event_pair_horizon_attempt VALUES
            (:attempt_id,:input_capture_id,:observation_id,:source_id,
             :event_first_known_utc,:horizon_min,:target_utc,:read_started_utc,
             :attempted_utc,:attempt_delay_seconds,:input_pair_count,
             :valid_pair_count,:invalid_pair_count,:snapshot_payload_sha256,
             :input_capture_payload_sha256,:component_root_sha256,:payload_json,
             :payload_sha256,1,0,0,0,0,:contract_id,:cohort_id)""",
            {
                **header_payload,
                "payload_json": header_json,
                "payload_sha256": sha256_text(header_json),
            },
        )
        for payload, payload_json, payload_sha in rows:
            outcome_id = "official_event_pair_outcome_" + sha256_text(
                f"{header['attempt_id']}|{payload['instrument']}|{payload_sha}"
            )[:32]
            connection.execute(
                """INSERT INTO official_event_pair_horizon_outcome VALUES
                (:outcome_id,:attempt_id,:horizon_min,:instrument,:valid,
                 :invalid_reason,:entry_bid,:entry_ask,:exit_bid,:exit_ask,:pip,
                 :entry_quote_utc,:exit_quote_utc,:exit_quote_age_seconds,
                 :target_offset_seconds,:signed_mid_move_pips,
                 :buy_executable_pips,:sell_executable_pips,:cost_stress_json,
                 :payload_json,:payload_sha256,1,0,0,0,0,:contract_id,:cohort_id)""",
                {
                    "outcome_id": outcome_id,
                    "attempt_id": header["attempt_id"],
                    "horizon_min": header["horizon_min"],
                    "instrument": payload["instrument"],
                    "valid": int(bool(payload["valid"])),
                    "invalid_reason": payload["invalid_reason"],
                    "entry_bid": payload["entry_bid"],
                    "entry_ask": payload["entry_ask"],
                    "exit_bid": payload["exit_bid"],
                    "exit_ask": payload["exit_ask"],
                    "pip": payload["pip"],
                    "entry_quote_utc": payload["entry_quote_utc"],
                    "exit_quote_utc": payload["exit_quote_utc"],
                    "exit_quote_age_seconds": payload["exit_quote_age_seconds"],
                    "target_offset_seconds": payload["target_offset_seconds"],
                    "signed_mid_move_pips": payload["signed_mid_move_pips"],
                    "buy_executable_pips": payload["buy_executable_pips"],
                    "sell_executable_pips": payload["sell_executable_pips"],
                    "cost_stress_json": canonical_json(payload["cost_stress"]),
                    "payload_json": payload_json,
                    "payload_sha256": payload_sha,
                    "contract_id": CONTRACT_ID,
                    "cohort_id": COHORT_ID,
                },
            )


def capture_due(
    output: sqlite3.Connection,
    source: sqlite3.Connection,
    snapshot: Mapping[str, Any],
    now: dt.datetime,
    *,
    read_started_utc: dt.datetime | None = None,
) -> dict[str, int]:
    existing = {
        (str(row[0]), int(row[1]))
        for row in output.execute(
            "SELECT input_capture_id,horizon_min FROM official_event_pair_horizon_attempt"
        )
    }
    rows = source.execute(
        """SELECT capture_id,observation_id,source_id,event_first_known_utc,
                  capture_payload_json
           FROM official_event_pair_quote_capture
           WHERE contract_id=? AND cohort_id=? AND research_only=1
           ORDER BY event_first_known_utc,capture_id""",
        (ENTRY_CONTRACT_ID, ENTRY_COHORT_ID),
    ).fetchall()
    counts = {
        "input_captures": len(rows), "preactivation_excluded": 0,
        "inserted_attempts": 0, "inserted_outcomes": 0, "not_due": 0,
        "duplicates": 0,
    }
    for capture_id, observation_id, source_id, event_text, raw_payload in rows:
        event = parse_time(event_text)
        if event is None or event < ACTIVATED_UTC:
            counts["preactivation_excluded"] += 1
            continue
        entry = json.loads(str(raw_payload))
        entry.update(
            capture_id=str(capture_id), observation_id=str(observation_id),
            source_id=str(source_id), event_first_known_utc=str(event_text),
        )
        for horizon in HORIZONS_MIN:
            key = (str(capture_id), horizon)
            if key in existing:
                counts["duplicates"] += 1
                continue
            if event + dt.timedelta(minutes=horizon) > now:
                counts["not_due"] += 1
                continue
            header, outcomes = build_attempt(
                entry, horizon, snapshot, now, read_started_utc=read_started_utc
            )
            _insert_attempt(
                output, header, outcomes,
                input_capture_payload_sha256=sha256_text(str(raw_payload)),
            )
            existing.add(key)
            counts["inserted_attempts"] += 1
            counts["inserted_outcomes"] += len(outcomes)
    return counts


def census(connection: sqlite3.Connection) -> dict[str, int]:
    attempts = connection.execute(
        "SELECT COUNT(1),COALESCE(SUM(valid_pair_count),0),"
        "COALESCE(SUM(invalid_pair_count),0),COUNT(DISTINCT input_capture_id) "
        "FROM official_event_pair_horizon_attempt"
    ).fetchone()
    outcomes = connection.execute(
        "SELECT COUNT(1),COALESCE(SUM(valid),0) FROM official_event_pair_horizon_outcome"
    ).fetchone()
    return {
        "attempts": int(attempts[0]), "valid_pair_outcomes": int(attempts[1]),
        "invalid_pair_outcomes": int(attempts[2]), "input_events": int(attempts[3]),
        "outcomes": int(outcomes[0]), "valid_outcomes": int(outcomes[1]),
    }


def run_cycle(
    *,
    input_database: Path = INPUT_DATABASE,
    output_database: Path = OUTPUT_DATABASE,
    quote_path: Path = QUOTE_PATH,
    state_path: Path = STATE_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    observed_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    validate_frozen_config()
    read_started = observed_utc or utc_now()
    error = ""
    try:
        snapshot = load_quote_snapshot(quote_path)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        snapshot = {}
        error = type(exc).__name__
    now = observed_utc or utc_now()
    output = open_database(output_database)
    source: sqlite3.Connection | None = None
    try:
        source = sqlite3.connect(
            f"file:{input_database.resolve().as_posix()}?mode=ro", uri=True, timeout=10
        )
        cycle = capture_due(
            output, source, snapshot, now, read_started_utc=read_started
        )
        db_counts = census(output)
        integrity = str(output.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        if source is not None:
            source.close()
        output.close()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": iso_utc(now),
        "status": "ok" if integrity == "ok" else "degraded",
        "error": error,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "cycle": cycle,
        "counts": db_counts,
        "sqlite_integrity": integrity,
        "input_database": str(input_database),
        "output_database": str(output_database),
        "quote_path": str(quote_path),
        "policy": POLICY,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }
    write_json_atomic(state_path, payload)
    heartbeat = {
        "worker": "oanda_official_event_pair_horizon_capture_v1",
        "updated_utc": iso_utc(now),
        "status": payload["status"],
        "attempts": db_counts["attempts"],
        "outcomes": db_counts["outcomes"],
        "error": error,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }
    write_json_atomic(heartbeat_path, heartbeat)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run_cycle()
        if not args.quiet:
            print(json.dumps(payload, sort_keys=True), flush=True)
        if args.once or args.duration_sec <= 0:
            return 0
        if time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(0.1, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
