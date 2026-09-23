#!/usr/bin/env python3
"""Prospective all-68 executable quote horizons for official FX events.

The raw official-release lane freezes one executable bid/ask sidecar at the
first-seen boundary.  This worker adds the other half of that experiment: one
terminal quote attempt at each predeclared horizon.  It never reloads an entry
quote, never backfills an event and never retries a missed horizon.

The ledger is research-only.  It cannot place an order, authorize a candidate
or promote a hypothesis.  Its purpose is to make later event, price-only and
no-trade comparisons use the same immutable market clocks.
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
from typing import Any, Iterable, Mapping

from oanda_quote_transport import load_quote_snapshot
import oanda_official_release_fast_lane as fast_lane


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "official_event_quote_horizon_capture_v1.json"
INPUT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_release_fast_lane_v4.sqlite"
)
OUTPUT_DATABASE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_event_quote_horizon_capture_v1.sqlite"
)
QUOTE_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_market_quotes_v1.json"
)
STATE_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_event_quote_horizon_capture_latest_v1.json"
)
HEARTBEAT_PATH = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "local_news_sentiment"
    / "official_event_quote_horizon_capture_heartbeat_v1.json"
)

SCHEMA_VERSION = "official_event_quote_horizon_capture_v1"
CONTRACT_ID = "official_event_quote_horizon_capture_v1_all68_append_only_20260830"
COHORT_ID = "official_event_quote_horizon_capture_v1_20260830a"
ACTIVATED_UTC = dt.datetime(2026, 8, 30, 12, 0, tzinfo=dt.timezone.utc)
REQUIRED_ENTRY_CAPTURE_CONTRACT_ID = fast_lane.QUOTE_CAPTURE_CONTRACT_ID
REQUIRED_ENTRY_CAPTURE_COHORT_ID = fast_lane.QUOTE_CAPTURE_COHORT_ID
EXPECTED_INSTRUMENTS = fast_lane.EXPECTED_QUOTE_INSTRUMENTS
EXPECTED_INSTRUMENT_COUNT = fast_lane.EXPECTED_QUOTE_COUNT
EXPECTED_UNIVERSE_SHA256 = fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
REQUIRED_SNAPSHOT_SCHEMA_VERSION = 2
REQUIRED_SNAPSHOT_PRODUCER = "practice_007_fast_executor_price_stream"
HORIZONS_MIN = (1, 5, 15, 30, 60)
MAXIMUM_ATTEMPT_DELAY_SEC = 20.0
MAXIMUM_SNAPSHOT_AGE_SEC = 15.0
MAXIMUM_QUOTE_AGE_SEC = 30.0
MAXIMUM_FUTURE_SKEW_SEC = 2.0
MAXIMUM_TARGET_OFFSET_SEC = 20.0
POLICY = {
    "prospective_only": True,
    "historical_backfill_allowed": False,
    "one_terminal_attempt_per_event_horizon": True,
    "input_entry_quote_reacquisition_allowed": False,
    "missing_or_late_capture_policy": "persist_terminal_invalid_never_retry",
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
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def finite_integer(value: Any) -> int | None:
    """Return a real integral value without letting malformed metadata raise."""

    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    )


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
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
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


def validate_frozen_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = read_json(path, {})
    expected = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "required_entry_capture_contract_id": REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
        "required_entry_capture_cohort_id": REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "required_quote_snapshot_schema_version": REQUIRED_SNAPSHOT_SCHEMA_VERSION,
        "required_quote_snapshot_producer": REQUIRED_SNAPSHOT_PRODUCER,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"frozen horizon-capture config mismatch:{key}")
    if tuple(int(item) for item in payload.get("horizons_min") or ()) != HORIZONS_MIN:
        raise ValueError("frozen horizon-capture config mismatch:horizons_min")
    if parse_time(payload.get("activated_utc")) != ACTIVATED_UTC:
        raise ValueError("frozen horizon-capture config mismatch:activated_utc")
    timing = payload.get("timing")
    timing = timing if isinstance(timing, Mapping) else {}
    for key, value in {
        "maximum_attempt_delay_sec": MAXIMUM_ATTEMPT_DELAY_SEC,
        "maximum_snapshot_age_sec": MAXIMUM_SNAPSHOT_AGE_SEC,
        "maximum_quote_age_sec": MAXIMUM_QUOTE_AGE_SEC,
        "maximum_future_skew_sec": MAXIMUM_FUTURE_SKEW_SEC,
        "maximum_target_offset_sec": MAXIMUM_TARGET_OFFSET_SEC,
    }.items():
        if finite_number(timing.get(key)) != value:
            raise ValueError(f"frozen horizon-capture timing mismatch:{key}")
    cost_stress = payload.get("cost_stress")
    cost_stress = cost_stress if isinstance(cost_stress, Mapping) else {}
    if cost_stress.get("round_trip_slippage_pips") != [0.0, 0.25, 0.5]:
        raise ValueError("frozen horizon-capture cost-stress mismatch")
    policy = payload.get("policy") or {}
    required_policy = {
        "prospective_only": True,
        "historical_backfill_allowed": False,
        "one_terminal_attempt_per_event_horizon": True,
        "input_entry_quote_reacquisition_allowed": False,
        "missing_or_late_capture_policy": "persist_terminal_invalid_never_retry",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    for key, value in required_policy.items():
        if policy.get(key) != value:
            raise ValueError(f"unsafe horizon-capture policy:{key}")
    return payload


def open_database(path: Path = OUTPUT_DATABASE) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    try:
        # Inspect the one known pre-activation schema before executing any DDL.
        # An empty dry-run ledger may adopt the finalized read-start clock;
        # nonempty evidence must never be rewritten into a new contract.
        capture_table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='official_event_horizon_capture'"
        ).fetchone()
        if capture_table_exists:
            capture_columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(official_event_horizon_capture)"
                ).fetchall()
            }
            if "capture_read_started_utc" not in capture_columns:
                row_count = int(
                    connection.execute(
                        "SELECT COUNT(1) FROM official_event_horizon_capture"
                    ).fetchone()[0]
                )
                if row_count:
                    raise ValueError(
                        "nonempty horizon ledger predates the finalized clock schema"
                    )
                connection.execute(
                    "ALTER TABLE official_event_horizon_capture "
                    "ADD COLUMN capture_read_started_utc TEXT NOT NULL DEFAULT ''"
                )
                connection.commit()

        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS official_event_horizon_capture (
          capture_id TEXT PRIMARY KEY,
          input_capture_id TEXT NOT NULL,
          observation_id TEXT NOT NULL,
          event_first_known_utc TEXT NOT NULL,
          horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60)),
          target_utc TEXT NOT NULL,
          capture_read_started_utc TEXT NOT NULL,
          attempted_utc TEXT NOT NULL,
          attempt_delay_sec REAL NOT NULL,
          timing_quality TEXT NOT NULL,
          invalid_reason TEXT NOT NULL,
          observed_valid_quote_count INTEGER NOT NULL,
          proof_quote_count INTEGER NOT NULL,
          connection_generation INTEGER,
          quote_snapshot_generated_utc TEXT NOT NULL,
          quote_snapshot_sha256 TEXT NOT NULL,
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
        CREATE TABLE IF NOT EXISTS official_event_horizon_quote (
          quote_id TEXT PRIMARY KEY,
          capture_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL CHECK(horizon_min IN (1,5,15,30,60)),
          instrument TEXT NOT NULL,
          bid REAL NOT NULL,
          ask REAL NOT NULL,
          pip REAL NOT NULL,
          quote_utc TEXT NOT NULL,
          quote_age_sec REAL NOT NULL,
          target_offset_sec REAL NOT NULL,
          spread_pips REAL NOT NULL,
          source TEXT NOT NULL,
          connection_generation INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          can_place_orders INTEGER NOT NULL CHECK(can_place_orders=0),
          can_authorize INTEGER NOT NULL CHECK(can_authorize=0),
          can_promote INTEGER NOT NULL CHECK(can_promote=0),
          contract_id TEXT NOT NULL,
          cohort_id TEXT NOT NULL,
          UNIQUE(capture_id,instrument),
          FOREIGN KEY(capture_id) REFERENCES official_event_horizon_capture(capture_id)
        );
        CREATE INDEX IF NOT EXISTS idx_event_horizon_capture_due
          ON official_event_horizon_capture(event_first_known_utc,horizon_min,timing_quality);
        CREATE INDEX IF NOT EXISTS idx_event_horizon_quote_capture
          ON official_event_horizon_quote(capture_id,instrument);
        CREATE TRIGGER IF NOT EXISTS horizon_capture_no_update
          BEFORE UPDATE ON official_event_horizon_capture
          BEGIN SELECT RAISE(ABORT,'horizon_capture is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS horizon_capture_no_delete
          BEFORE DELETE ON official_event_horizon_capture
          BEGIN SELECT RAISE(ABORT,'horizon_capture is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS horizon_quote_no_update
          BEFORE UPDATE ON official_event_horizon_quote
          BEGIN SELECT RAISE(ABORT,'horizon_quote is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS horizon_quote_no_delete
          BEFORE DELETE ON official_event_horizon_quote
          BEGIN SELECT RAISE(ABORT,'horizon_quote is append-only'); END;
        """
        )
        connection.commit()
        return connection
    except BaseException:
        connection.close()
        raise


def _validated_snapshot(
    quote_payload: Mapping[str, Any],
    *,
    attempted_utc: dt.datetime,
    target_utc: dt.datetime,
) -> tuple[dict[str, dict[str, Any]], list[str], dict[str, str], int | None]:
    payload = dict(quote_payload or {})
    raw_quotes = payload.get("quotes")
    raw_quotes = raw_quotes if isinstance(raw_quotes, Mapping) else {}
    expected = set(EXPECTED_INSTRUMENTS)
    raw_keys = {str(key) for key in raw_quotes}
    missing = sorted(expected - raw_keys)
    unexpected = sorted(raw_keys - expected)
    invalid: dict[str, str] = {}
    observed: dict[str, dict[str, Any]] = {}
    generated = parse_time(payload.get("generated_utc"))
    snapshot_age = (
        None if generated is None else (attempted_utc - generated).total_seconds()
    )
    coverage = payload.get("coverage")
    coverage = coverage if isinstance(coverage, Mapping) else {}
    snapshot_schema_version = finite_integer(payload.get("schema_version"))
    snapshot_producer = str(payload.get("producer") or "").strip()
    reported_quote_count = finite_integer(payload.get("quote_count"))
    current_quote_count = finite_integer(coverage.get("current_quote_count"))
    last_known_quote_count = finite_integer(coverage.get("last_known_quote_count"))
    retained_last_known_count = finite_integer(
        coverage.get("retained_last_known_count")
    )
    connection_generation = finite_integer(payload.get("connection_generation"))
    coverage_generation = finite_integer(coverage.get("connection_generation"))

    metadata_invalid = bool(
        len(raw_quotes) != EXPECTED_INSTRUMENT_COUNT
        or snapshot_schema_version != REQUIRED_SNAPSHOT_SCHEMA_VERSION
        or snapshot_producer != REQUIRED_SNAPSHOT_PRODUCER
        or reported_quote_count != EXPECTED_INSTRUMENT_COUNT
        or current_quote_count != EXPECTED_INSTRUMENT_COUNT
        or last_known_quote_count != EXPECTED_INSTRUMENT_COUNT
        or retained_last_known_count != 0
        or connection_generation is None
        or coverage_generation is None
        or connection_generation != coverage_generation
        or snapshot_age is None
        or snapshot_age < -MAXIMUM_FUTURE_SKEW_SEC
        or snapshot_age > MAXIMUM_SNAPSHOT_AGE_SEC
        or missing
        or unexpected
    )
    for instrument in EXPECTED_INSTRUMENTS:
        row = raw_quotes.get(instrument)
        if not isinstance(row, Mapping):
            invalid[instrument] = "quote_missing"
            continue
        bid = finite_number(row.get("bid"))
        ask = finite_number(row.get("ask"))
        pip = finite_number(row.get("pip"))
        quote_utc = parse_time(row.get("time"))
        source = str(row.get("source") or "").strip()
        reason = ""
        if bid is None or ask is None or pip is None or bid <= 0 or ask <= bid or pip <= 0:
            reason = "invalid_bid_ask_or_pip"
        elif quote_utc is None:
            reason = "quote_clock_missing"
        elif not source:
            reason = "quote_source_missing"
        else:
            age = (attempted_utc - quote_utc).total_seconds()
            offset = (quote_utc - target_utc).total_seconds()
            if age < -MAXIMUM_FUTURE_SKEW_SEC:
                reason = "quote_clock_ahead_of_attempt"
            elif age > MAXIMUM_QUOTE_AGE_SEC:
                reason = "quote_stale"
            elif abs(offset) > MAXIMUM_TARGET_OFFSET_SEC:
                reason = "quote_outside_target_window"
        if reason:
            invalid[instrument] = reason
            continue
        assert bid is not None and ask is not None and pip is not None
        assert quote_utc is not None
        observed[instrument] = {
            "instrument": instrument,
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "quote_utc": iso_utc(quote_utc),
            "quote_age_sec": round((attempted_utc - quote_utc).total_seconds(), 6),
            "target_offset_sec": round((quote_utc - target_utc).total_seconds(), 6),
            "spread_pips": round((ask - bid) / pip, 9),
            "source": source,
            "connection_generation": int(connection_generation or 0),
        }
    if metadata_invalid and not invalid:
        invalid["__snapshot__"] = "snapshot_metadata_invalid"
    return observed, missing + unexpected, invalid, (
        connection_generation
    )


def build_horizon_capture(
    entry_capture: Mapping[str, Any],
    horizon_min: int,
    quote_payload: Mapping[str, Any],
    attempted_utc: dt.datetime,
    *,
    capture_started_utc: dt.datetime | None = None,
) -> dict[str, Any]:
    """Build one terminal prospective horizon attempt without persistence."""

    if int(horizon_min) not in HORIZONS_MIN:
        raise ValueError(f"unsupported horizon:{horizon_min}")
    attempt = attempted_utc
    if attempt.tzinfo is None:
        attempt = attempt.replace(tzinfo=dt.timezone.utc)
    attempt = attempt.astimezone(dt.timezone.utc)
    capture_started = capture_started_utc or attempt
    if capture_started.tzinfo is None:
        capture_started = capture_started.replace(tzinfo=dt.timezone.utc)
    capture_started = capture_started.astimezone(dt.timezone.utc)
    if capture_started > attempt:
        raise ValueError("capture read start is after completion")
    event = parse_time(entry_capture.get("event_first_known_utc"))
    if event is None:
        raise ValueError("entry capture event clock is missing")
    target = event + dt.timedelta(minutes=int(horizon_min))
    delay = (attempt - target).total_seconds()
    if delay < 0:
        raise ValueError("horizon is not due")

    observed, coverage_gaps, invalid, generation = _validated_snapshot(
        quote_payload, attempted_utc=attempt, target_utc=target
    )
    timing_quality = "prospective_exact_live_quote"
    invalid_reason = ""
    if event < ACTIVATED_UTC:
        timing_quality = "diagnostic_pre_horizon_capture_activation"
        invalid_reason = "horizon_capture_cohort_not_active"
    elif delay > MAXIMUM_ATTEMPT_DELAY_SEC:
        timing_quality = "prospective_horizon_clock_missed"
        invalid_reason = f"attempt_delay_sec:{delay:.6f}"
    elif not quote_payload:
        timing_quality = "prospective_quote_snapshot_unavailable"
        invalid_reason = "snapshot_payload_missing"
    elif coverage_gaps or invalid or len(observed) != EXPECTED_INSTRUMENT_COUNT:
        timing_quality = "prospective_quote_coverage_invalid"
        reasons = []
        if coverage_gaps:
            reasons.append(f"coverage_gaps:{len(coverage_gaps)}")
        if invalid:
            reasons.append(f"invalid_instruments:{len(invalid)}")
        if len(observed) != EXPECTED_INSTRUMENT_COUNT:
            reasons.append(
                f"valid_quote_count:{len(observed)}!={EXPECTED_INSTRUMENT_COUNT}"
            )
        invalid_reason = ";".join(reasons)

    proof_quotes = (
        observed
        if timing_quality == "prospective_exact_live_quote"
        and len(observed) == EXPECTED_INSTRUMENT_COUNT
        else {}
    )
    input_capture_id = str(
        entry_capture.get("capture_id")
        or entry_capture.get("input_capture_id")
        or ""
    )
    observation_id = str(entry_capture.get("observation_id") or "")
    capture_id = "official_event_horizon_" + sha256_text(
        f"{input_capture_id}|{horizon_min}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    quote_snapshot_material = dict(quote_payload or {})
    quote_snapshot_json = canonical_json(quote_snapshot_material)
    component_hashes = [
        sha256_text(canonical_json(proof_quotes[instrument]))
        for instrument in sorted(proof_quotes)
    ]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture_id,
        "input_capture_id": input_capture_id,
        "observation_id": observation_id,
        "event_first_known_utc": iso_utc(event),
        "horizon_min": int(horizon_min),
        "target_utc": iso_utc(target),
        "capture_read_started_utc": iso_utc(capture_started),
        "attempted_utc": iso_utc(attempt),
        "attempt_delay_sec": round(delay, 6),
        "timing_quality": timing_quality,
        "invalid_reason": invalid_reason,
        "observed_valid_quote_count": len(observed),
        "proof_quote_count": len(proof_quotes),
        "quotes": proof_quotes,
        "observed_quotes": observed,
        "coverage_gaps": coverage_gaps,
        "invalid_instruments": invalid,
        "connection_generation": generation,
        "quote_snapshot_generated_utc": str(
            quote_payload.get("generated_utc") or ""
        ),
        "quote_snapshot_material": quote_snapshot_material,
        "quote_snapshot_sha256": sha256_text(quote_snapshot_json),
        "component_root_sha256": sha256_text("".join(component_hashes)),
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "entry_capture_contract_id": str(
            entry_capture.get("capture_contract_id") or ""
        ),
        "entry_capture_cohort_id": str(
            entry_capture.get("capture_cohort_id") or ""
        ),
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
    }
    return payload


def _insert_capture(
    connection: sqlite3.Connection,
    capture: Mapping[str, Any],
    *,
    input_capture_payload_sha256: str,
) -> None:
    payload_json = canonical_json(capture)
    payload_sha256 = sha256_text(payload_json)
    connection.execute(
        """
        INSERT INTO official_event_horizon_capture (
          capture_id,input_capture_id,observation_id,event_first_known_utc,
          horizon_min,target_utc,capture_read_started_utc,attempted_utc,
          attempt_delay_sec,timing_quality,
          invalid_reason,observed_valid_quote_count,proof_quote_count,
          connection_generation,quote_snapshot_generated_utc,
          quote_snapshot_sha256,input_capture_payload_sha256,
          component_root_sha256,payload_json,payload_sha256,research_only,
          execution_eligible,can_place_orders,can_authorize,can_promote,
          contract_id,cohort_id
        ) VALUES (
          :capture_id,:input_capture_id,:observation_id,:event_first_known_utc,
          :horizon_min,:target_utc,:capture_read_started_utc,:attempted_utc,
          :attempt_delay_sec,
          :timing_quality,:invalid_reason,:observed_valid_quote_count,
          :proof_quote_count,:connection_generation,
          :quote_snapshot_generated_utc,:quote_snapshot_sha256,
          :input_capture_payload_sha256,:component_root_sha256,:payload_json,
          :payload_sha256,1,0,0,0,0,:contract_id,:cohort_id
        )
        """,
        {
            "capture_id": capture["capture_id"],
            "input_capture_id": capture["input_capture_id"],
            "observation_id": capture["observation_id"],
            "event_first_known_utc": capture["event_first_known_utc"],
            "horizon_min": capture["horizon_min"],
            "target_utc": capture["target_utc"],
            "capture_read_started_utc": capture["capture_read_started_utc"],
            "attempted_utc": capture["attempted_utc"],
            "attempt_delay_sec": capture["attempt_delay_sec"],
            "timing_quality": capture["timing_quality"],
            "invalid_reason": capture["invalid_reason"],
            "observed_valid_quote_count": capture["observed_valid_quote_count"],
            "proof_quote_count": capture["proof_quote_count"],
            "connection_generation": capture["connection_generation"],
            "quote_snapshot_generated_utc": capture[
                "quote_snapshot_generated_utc"
            ],
            "quote_snapshot_sha256": capture["quote_snapshot_sha256"],
            "input_capture_payload_sha256": input_capture_payload_sha256,
            "component_root_sha256": capture["component_root_sha256"],
            "payload_json": payload_json,
            "payload_sha256": payload_sha256,
            "contract_id": CONTRACT_ID,
            "cohort_id": COHORT_ID,
        },
    )
    for instrument, row in sorted((capture.get("quotes") or {}).items()):
        component = dict(row)
        component_json = canonical_json(component)
        component_sha256 = sha256_text(component_json)
        quote_id = "official_event_horizon_quote_" + sha256_text(
            f"{capture['capture_id']}|{instrument}|{component_sha256}"
        )[:32]
        connection.execute(
            """
            INSERT INTO official_event_horizon_quote (
              quote_id,capture_id,horizon_min,instrument,bid,ask,pip,quote_utc,
              quote_age_sec,target_offset_sec,spread_pips,source,
              connection_generation,payload_json,payload_sha256,research_only,
              execution_eligible,can_place_orders,can_authorize,can_promote,
              contract_id,cohort_id
            ) VALUES (
              :quote_id,:capture_id,:horizon_min,:instrument,:bid,:ask,:pip,
              :quote_utc,:quote_age_sec,:target_offset_sec,:spread_pips,:source,
              :connection_generation,:payload_json,:payload_sha256,1,0,0,0,0,
              :contract_id,:cohort_id
            )
            """,
            {
                "quote_id": quote_id,
                "capture_id": capture["capture_id"],
                "horizon_min": capture["horizon_min"],
                "instrument": instrument,
                "bid": component["bid"],
                "ask": component["ask"],
                "pip": component["pip"],
                "quote_utc": component["quote_utc"],
                "quote_age_sec": component["quote_age_sec"],
                "target_offset_sec": component["target_offset_sec"],
                "spread_pips": component["spread_pips"],
                "source": component["source"],
                "connection_generation": component["connection_generation"],
                "payload_json": component_json,
                "payload_sha256": component_sha256,
                "contract_id": CONTRACT_ID,
                "cohort_id": COHORT_ID,
            },
        )


def capture_due_horizons(
    output_connection: sqlite3.Connection,
    input_connection: sqlite3.Connection,
    quote_payload: Mapping[str, Any],
    observed_utc: dt.datetime,
    *,
    capture_started_utc: dt.datetime | None = None,
) -> dict[str, int]:
    """Persist exactly one due attempt per eligible raw event and horizon."""

    now = observed_utc
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    now = now.astimezone(dt.timezone.utc)
    existing = {
        (str(row[0]), int(row[1]))
        for row in output_connection.execute(
            "SELECT input_capture_id,horizon_min FROM official_event_horizon_capture"
        ).fetchall()
    }
    rows = input_connection.execute(
        """
        SELECT q.capture_id,q.capture_payload_json,o.observation_id,
               o.prospective_observation
        FROM official_release_quote_capture q
        JOIN official_release_observation o
          ON o.observation_id=q.observation_id
        WHERE q.timing_quality='prospective_exact_live_quote'
          AND q.proof_quote_count=?
          AND q.capture_contract_id=?
          AND q.capture_cohort_id=?
          AND o.prospective_observation=1
        ORDER BY q.event_first_known_utc,q.capture_id
        """,
        (
            EXPECTED_INSTRUMENT_COUNT,
            REQUIRED_ENTRY_CAPTURE_CONTRACT_ID,
            REQUIRED_ENTRY_CAPTURE_COHORT_ID,
        ),
    ).fetchall()
    counts = {
        "eligible_input_captures": len(rows),
        "inserted": 0,
        "duplicates": 0,
        "not_due": 0,
        "exact": 0,
        "terminal_invalid": 0,
    }
    with output_connection:
        for input_capture_id, raw_payload_json, observation_id, _ in rows:
            entry = json.loads(str(raw_payload_json))
            entry["capture_id"] = str(input_capture_id)
            entry["observation_id"] = str(observation_id)
            event = parse_time(entry.get("event_first_known_utc"))
            if event is None or event < ACTIVATED_UTC:
                continue
            input_sha256 = sha256_text(str(raw_payload_json))
            for horizon in HORIZONS_MIN:
                key = (str(input_capture_id), int(horizon))
                if key in existing:
                    counts["duplicates"] += 1
                    continue
                target = event + dt.timedelta(minutes=horizon)
                if target > now:
                    counts["not_due"] += 1
                    continue
                capture = build_horizon_capture(
                    entry,
                    horizon,
                    quote_payload,
                    now,
                    capture_started_utc=capture_started_utc,
                )
                _insert_capture(
                    output_connection,
                    capture,
                    input_capture_payload_sha256=input_sha256,
                )
                existing.add(key)
                counts["inserted"] += 1
                if capture["timing_quality"] == "prospective_exact_live_quote":
                    counts["exact"] += 1
                else:
                    counts["terminal_invalid"] += 1
    return counts


def executable_path(
    entry_quote: Mapping[str, Any],
    exit_quote: Mapping[str, Any],
    side: str,
    slippage_pips: float = 0.0,
) -> dict[str, Any]:
    """Compute executable endpoint economics; spread is embedded exactly once."""

    entry_bid = finite_number(entry_quote.get("bid"))
    entry_ask = finite_number(entry_quote.get("ask"))
    exit_bid = finite_number(exit_quote.get("bid"))
    exit_ask = finite_number(exit_quote.get("ask"))
    entry_pip = finite_number(entry_quote.get("pip"))
    exit_pip = finite_number(exit_quote.get("pip"))
    slip = finite_number(slippage_pips)
    if (
        None in {entry_bid, entry_ask, exit_bid, exit_ask, entry_pip, exit_pip, slip}
        or entry_bid <= 0
        or entry_ask <= entry_bid
        or exit_bid <= 0
        or exit_ask <= exit_bid
        or entry_pip <= 0
        or exit_pip != entry_pip
        or slip < 0
    ):
        raise ValueError("invalid executable path")
    assert entry_bid is not None and entry_ask is not None
    assert exit_bid is not None and exit_ask is not None
    assert entry_pip is not None and slip is not None
    normalized_side = str(side).lower().strip()
    if normalized_side == "buy":
        executable = (exit_bid - entry_ask) / entry_pip
        midpoint = (
            ((exit_bid + exit_ask) / 2.0)
            - ((entry_bid + entry_ask) / 2.0)
        ) / entry_pip
    elif normalized_side == "sell":
        executable = (entry_bid - exit_ask) / entry_pip
        midpoint = (
            ((entry_bid + entry_ask) / 2.0)
            - ((exit_bid + exit_ask) / 2.0)
        ) / entry_pip
    else:
        raise ValueError(f"invalid side:{side}")
    return {
        "side": normalized_side,
        "midpoint_gross_pips": round(midpoint, 9),
        "signed_mid_move_pips": round(midpoint, 9),
        "executable_before_slippage_pips": round(executable, 9),
        "executable_net_pips_before_slippage": round(executable, 9),
        "round_trip_slippage_pips": round(slip, 9),
        "executable_after_slippage_pips": round(executable - slip, 9),
        "executable_net_pips_after_slippage": round(executable - slip, 9),
        "cost_drag_pips": round(midpoint - (executable - slip), 9),
        "entry_spread_pips": round((entry_ask - entry_bid) / entry_pip, 9),
        "exit_spread_pips": round((exit_ask - exit_bid) / entry_pip, 9),
        "spread_embedded_in_executable_prices": True,
        "research_only": True,
        "execution_eligible": False,
    }


def database_census(connection: sqlite3.Connection) -> dict[str, Any]:
    header = connection.execute(
        """
        SELECT COUNT(1),
               COALESCE(SUM(timing_quality='prospective_exact_live_quote'),0),
               COALESCE(SUM(timing_quality<>'prospective_exact_live_quote'),0),
               COUNT(DISTINCT input_capture_id)
        FROM official_event_horizon_capture
        """
    ).fetchone()
    components = connection.execute(
        "SELECT COUNT(1) FROM official_event_horizon_quote"
    ).fetchone()[0]
    return {
        "horizon_attempts": int(header[0]),
        "exact_horizon_attempts": int(header[1]),
        "terminal_invalid_attempts": int(header[2]),
        "input_events": int(header[3]),
        "quote_components": int(components),
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
    quote_payload: Mapping[str, Any]
    quote_error = ""
    try:
        quote_payload = load_quote_snapshot(quote_path)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        quote_payload = {}
        quote_error = type(exc).__name__
    # The evidence clock is quote-read completion, not the instant before the
    # file/SQLite transport was opened. Tests may inject one exact clock.
    now = observed_utc or utc_now()
    output = open_database(output_database)
    input_connection: sqlite3.Connection | None = None
    try:
        input_connection = sqlite3.connect(
            f"file:{input_database.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=10.0,
        )
        cycle = capture_due_horizons(
            output,
            input_connection,
            quote_payload,
            now,
            capture_started_utc=read_started,
        )
        integrity = str(output.execute("PRAGMA quick_check").fetchone()[0])
        census = database_census(output)
    finally:
        if input_connection is not None:
            input_connection.close()
        output.close()
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "generated_utc": iso_utc(now),
        "status": "ok" if integrity == "ok" else "integrity_failure",
        "database_integrity": integrity,
        "cycle": cycle,
        "census": census,
        "quote_transport_error": quote_error,
        "horizons_min": list(HORIZONS_MIN),
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    write_json_atomic(state_path, snapshot)
    write_json_atomic(heartbeat_path, snapshot)
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-database", type=Path, default=INPUT_DATABASE)
    parser.add_argument("--output-database", type=Path, default=OUTPUT_DATABASE)
    parser.add_argument("--quote-path", type=Path, default=QUOTE_PATH)
    parser.add_argument("--state", type=Path, default=STATE_PATH)
    parser.add_argument("--heartbeat", type=Path, default=HEARTBEAT_PATH)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        snapshot = run_cycle(
            input_database=args.input_database,
            output_database=args.output_database,
            quote_path=args.quote_path,
            state_path=args.state,
            heartbeat_path=args.heartbeat,
        )
        print(canonical_json(snapshot), flush=True)
        if args.duration_sec <= 0 or time.monotonic() - started >= args.duration_sec:
            break
        time.sleep(max(1.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACTIVATED_UTC",
    "COHORT_ID",
    "CONTRACT_ID",
    "EXPECTED_INSTRUMENTS",
    "HORIZONS_MIN",
    "POLICY",
    "build_horizon_capture",
    "capture_due_horizons",
    "executable_path",
    "open_database",
    "run_cycle",
]
