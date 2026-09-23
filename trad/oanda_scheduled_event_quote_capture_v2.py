#!/usr/bin/env python3
"""Capture a current OANDA Practice pricing response at scheduled event clocks.

V1 correctly preserved its attempts but made an unchanged broker tick a reason
to reject the entire 68-instrument snapshot.  V2 separates the clock at which
OANDA returned the current pricing surface from each instrument's last broker
price-change clock.  It requires all 68 rows and explicit tradeability, counts
only currently tradeable rows as executable proof, preserves old tick times as
diagnostics, assigns no direction, and cannot trade, authorize, or promote.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Mapping, Sequence

import oanda_official_release_fast_lane as fast_lane
import oanda_practice_pair_rotation_scalper as rotation
import oanda_practice_shadow_strategy_lab as lab
from oanda_scheduled_event_quote_capture_v1 import (
    canonical_json,
    iso_utc,
    parse_time,
    read_json,
    sha256_text,
    utc_now,
    write_json_atomic,
)


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "scheduled_event_quote_capture_v2.json"
PREFLIGHT_PATH = (
    ROOT / "data" / "oanda_training_manager" / "state" /
    "event_technical_preflight_v1.json"
)
OUTPUT_ROOT = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
DATABASE_PATH = OUTPUT_ROOT / "scheduled_event_quote_capture_v2.sqlite"
STATE_PATH = OUTPUT_ROOT / "scheduled_event_quote_capture_latest_v2.json"
HEARTBEAT_PATH = OUTPUT_ROOT / "scheduled_event_quote_capture_heartbeat_v2.json"

SCHEMA_VERSION = "scheduled_event_quote_capture_v2"
CONTRACT_ID = "scheduled_event_quote_capture_v2_oanda_rest_current_snapshot_20260902"
COHORT_ID = "scheduled_event_quote_capture_v2_20260902a"
ACTIVATED_UTC = dt.datetime(2026, 9, 2, 2, 10, tzinfo=dt.timezone.utc)
REQUIRED_PREFLIGHT_CONTRACT_ID = (
    "neutral_event_clock_to_technical_preflight_v6_"
    "source_transport_readiness_20260901"
)
ACCOUNT_KEY = "OANDA_ACCOUNT_ID_DUM4"
EXPECTED_INSTRUMENTS = fast_lane.EXPECTED_QUOTE_INSTRUMENTS
EXPECTED_INSTRUMENT_COUNT = fast_lane.EXPECTED_QUOTE_COUNT
EXPECTED_UNIVERSE_SHA256 = fast_lane.EXPECTED_QUOTE_UNIVERSE_SHA256
HORIZONS_MIN = (0, 1, 5, 15, 30, 60)
LOOKAHEAD_DAYS = 45
MINIMUM_REGISTRATION_LEAD_SEC = 60.0
MAXIMUM_PREFLIGHT_AGE_SEC = 180.0
MAXIMUM_ATTEMPT_DELAY_SEC = 15.0
MAXIMUM_REST_ROUND_TRIP_SEC = 10.0
MAXIMUM_FUTURE_SKEW_SEC = 2.0


def validate_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = read_json(path, {})
    expected = {
        "schema_version": 2,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "activated_utc": iso_utc(ACTIVATED_UTC),
        "required_preflight_contract_id": REQUIRED_PREFLIGHT_CONTRACT_ID,
        "account_key": ACCOUNT_KEY,
        "environment": "practice",
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "horizons_min": list(HORIZONS_MIN),
        "lookahead_days": LOOKAHEAD_DAYS,
        "minimum_registration_lead_sec": int(MINIMUM_REGISTRATION_LEAD_SEC),
        "maximum_preflight_age_sec": int(MAXIMUM_PREFLIGHT_AGE_SEC),
        "maximum_attempt_delay_sec": int(MAXIMUM_ATTEMPT_DELAY_SEC),
        "maximum_rest_round_trip_sec": int(MAXIMUM_REST_ROUND_TRIP_SEC),
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"frozen V2 scheduled capture mismatch:{key}")
    policy = payload.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    required_policy = {
        "prospective_only": True,
        "historical_backfill_allowed": False,
        "one_terminal_attempt_per_event_horizon": True,
        "quote_acquisition": "one_read_only_oanda_practice_pricing_request_all68",
        "currentness_clock": "pricing_response_observed_utc",
        "broker_price_time_preserved_as_diagnostic": True,
        "unchanged_tradeable_quote_is_not_rejected_for_old_tick_time": True,
        "tradeability_must_be_explicit": True,
        "nontradeable_rows_are_observed_but_not_executable": True,
        "scheduled_clock_assigns_no_direction": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
        "supported_execution_decision": "no_trade",
    }
    for key, value in required_policy.items():
        if policy.get(key) != value:
            raise ValueError(f"frozen V2 scheduled capture policy mismatch:{key}")
    return payload


def open_database(path: Path = DATABASE_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS scheduled_event_clock_v2 (
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
        );
        CREATE TABLE IF NOT EXISTS scheduled_event_quote_capture_v2 (
          capture_id TEXT PRIMARY KEY,
          clock_id TEXT NOT NULL,
          horizon_min INTEGER NOT NULL,
          target_utc TEXT NOT NULL,
          retrieval_started_utc TEXT NOT NULL,
          retrieval_completed_utc TEXT NOT NULL,
          attempt_delay_sec REAL NOT NULL,
          rest_round_trip_sec REAL NOT NULL,
          timing_quality TEXT NOT NULL,
          invalid_reason TEXT NOT NULL,
          universe_quote_count INTEGER NOT NULL,
          tradeable_proof_count INTEGER NOT NULL,
          nontradeable_observed_count INTEGER NOT NULL,
          direct_event_tradeable_count INTEGER NOT NULL,
          response_sha256 TEXT NOT NULL,
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
          FOREIGN KEY(clock_id) REFERENCES scheduled_event_clock_v2(clock_id)
        );
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_event_clock_v2_no_update
          BEFORE UPDATE ON scheduled_event_clock_v2
          BEGIN SELECT RAISE(ABORT, 'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_event_clock_v2_no_delete
          BEFORE DELETE ON scheduled_event_clock_v2
          BEGIN SELECT RAISE(ABORT, 'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_event_quote_capture_v2_no_update
          BEFORE UPDATE ON scheduled_event_quote_capture_v2
          BEGIN SELECT RAISE(ABORT, 'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_event_quote_capture_v2_no_delete
          BEFORE DELETE ON scheduled_event_quote_capture_v2
          BEGIN SELECT RAISE(ABORT, 'append_only'); END;
        """
    )
    connection.commit()
    return connection


def event_clock_groups(
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
        if not isinstance(raw, Mapping) or raw.get("timing_precision") != "minute":
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
                    (iso_utc(scheduled), str(raw.get("event_series_id") or ""),
                     str(raw.get("headline") or ""))
                )
            )[:24]
        key = f"{event_id}|{iso_utc(scheduled)}"
        row = grouped.setdefault(
            key,
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
    output: list[dict[str, Any]] = []
    for row in grouped.values():
        if not row["direct_currencies"]:
            continue
        row["direct_currencies"] = sorted(row["direct_currencies"])
        row["affected_currencies"] = sorted(row["affected_currencies"])
        output.append(row)
    output.sort(key=lambda item: (item["scheduled_utc"], item["event_id"]))
    return output, ""


def register_upcoming_clocks(
    connection: sqlite3.Connection,
    preflight: Mapping[str, Any],
    *,
    now: dt.datetime,
) -> tuple[int, str]:
    groups, error = event_clock_groups(preflight, now=now)
    if error:
        return 0, error
    preflight_hash = sha256_text(canonical_json(preflight))
    inserted = 0
    for event in groups:
        scheduled = parse_time(event["scheduled_utc"])
        assert scheduled is not None
        lead = (scheduled - now).total_seconds()
        if lead < MINIMUM_REGISTRATION_LEAD_SEC:
            continue
        clock_id = "scheduled_event_clock_v2_" + sha256_text(
            f"{event['event_id']}|{event['scheduled_utc']}|{CONTRACT_ID}|{COHORT_ID}"
        )[:32]
        event_json = canonical_json(event)
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO scheduled_event_clock_v2 (
              clock_id,event_id,event_series_id,headline,scheduled_utc,
              driver_currency,direct_currencies_json,affected_currencies_json,
              registered_utc,registration_lead_sec,preflight_generated_utc,
              preflight_payload_sha256,event_payload_json,event_payload_sha256,
              research_only,execution_eligible,can_place_orders,can_authorize,
              can_promote,contract_id,cohort_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clock_id, event["event_id"], event["event_series_id"],
                event["headline"], event["scheduled_utc"], event["driver_currency"],
                canonical_json(event["direct_currencies"]),
                canonical_json(event["affected_currencies"]), iso_utc(now),
                round(lead, 6), str(preflight.get("generated_utc") or ""),
                preflight_hash, event_json, sha256_text(event_json), 1, 0, 0, 0, 0,
                CONTRACT_ID, COHORT_ID,
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
            "SELECT clock_id,horizon_min FROM scheduled_event_quote_capture_v2"
        )
    }
    output: list[dict[str, Any]] = []
    for row in connection.execute(
        "SELECT clock_id,event_id,scheduled_utc,event_payload_json "
        "FROM scheduled_event_clock_v2 ORDER BY scheduled_utc,clock_id"
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


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def build_rest_snapshot(
    client: lab.MarketDataClient,
    account_id: str,
    pip_sizes: Mapping[str, float],
) -> dict[str, Any]:
    started = utc_now()
    response = client.get(
        f"/v3/accounts/{account_id}/pricing",
        params={
            "instruments": ",".join(EXPECTED_INSTRUMENTS),
            "includeUnitsAvailable": "false",
        },
        pricing=True,
    )
    completed = utc_now()
    prices: dict[str, dict[str, Any]] = {}
    for raw in response.get("prices") or []:
        if not isinstance(raw, Mapping):
            continue
        instrument = str(raw.get("instrument") or "").upper()
        bids = raw.get("bids") if isinstance(raw.get("bids"), list) else []
        asks = raw.get("asks") if isinstance(raw.get("asks"), list) else []
        if not instrument or not bids or not asks:
            continue
        prices[instrument] = {
            "bid": bids[0].get("price"),
            "ask": asks[0].get("price"),
            "pip": pip_sizes.get(
                instrument, 0.01 if instrument.endswith("_JPY") else 0.0001
            ),
            "broker_price_time_utc": str(raw.get("time") or ""),
            "tradeable": raw.get("tradeable"),
            "status": str(raw.get("status") or ""),
            "source": "oanda_practice_rest_pricing",
        }
    return {
        "retrieval_started_utc": iso_utc(started),
        "retrieval_completed_utc": iso_utc(completed),
        "response_time_utc": str(response.get("time") or ""),
        "response_price_count": len(response.get("prices") or []),
        "prices": prices,
        "account_id_sha256": hashlib.sha256(account_id.encode("utf-8")).hexdigest(),
        "account_suffix": account_id[-4:],
        "environment": "practice",
        "request_method": "GET",
        "request_scope": "pricing_read_only",
    }


def build_capture(
    attempt: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    fallback_started_utc: dt.datetime,
    fallback_completed_utc: dt.datetime,
    snapshot_error: str = "",
) -> dict[str, Any]:
    target = parse_time(attempt.get("target_utc"))
    if target is None:
        raise ValueError("target clock missing")
    started = parse_time(snapshot.get("retrieval_started_utc")) or fallback_started_utc
    completed = parse_time(snapshot.get("retrieval_completed_utc")) or fallback_completed_utc
    response_time = parse_time(snapshot.get("response_time_utc"))
    round_trip = (completed - started).total_seconds()
    delay = (completed - target).total_seconds()
    raw_prices = snapshot.get("prices")
    raw_prices = raw_prices if isinstance(raw_prices, Mapping) else {}
    expected = set(EXPECTED_INSTRUMENTS)
    provided = {str(name).upper() for name in raw_prices}
    invalid: dict[str, str] = {}
    rows: dict[str, dict[str, Any]] = {}
    stale_tick_count = 0
    for instrument in EXPECTED_INSTRUMENTS:
        raw = raw_prices.get(instrument)
        if not isinstance(raw, Mapping):
            invalid[instrument] = "missing_price_row"
            continue
        bid = _finite(raw.get("bid"))
        ask = _finite(raw.get("ask"))
        pip = _finite(raw.get("pip"))
        broker_time = parse_time(raw.get("broker_price_time_utc"))
        tradeable = raw.get("tradeable")
        reason = ""
        if bid is None or ask is None or ask <= bid:
            reason = "invalid_executable_bid_ask"
        elif pip is None or pip <= 0.0:
            reason = "invalid_pip"
        elif broker_time is None:
            reason = "broker_price_time_missing"
        elif not isinstance(tradeable, bool):
            reason = "tradeability_not_explicit"
        if reason:
            invalid[instrument] = reason
            continue
        assert broker_time is not None
        broker_tick_age = (completed - broker_time).total_seconds()
        stale_tick_count += int(broker_tick_age > 15.0)
        rows[instrument] = {
            "bid": bid,
            "ask": ask,
            "pip": pip,
            "broker_price_time_utc": iso_utc(broker_time),
            "broker_tick_age_sec": round(broker_tick_age, 6),
            "tradeable": tradeable,
            "status": str(raw.get("status") or ""),
            "source": str(raw.get("source") or ""),
        }
    metadata_reasons: list[str] = []
    if snapshot_error:
        metadata_reasons.append(f"snapshot_read_error:{snapshot_error}")
    if provided != expected:
        metadata_reasons.append(
            f"instrument_set_mismatch:missing={len(expected-provided)}:unexpected={len(provided-expected)}"
        )
    if int(snapshot.get("response_price_count") or 0) != EXPECTED_INSTRUMENT_COUNT:
        metadata_reasons.append(
            f"response_price_count:{snapshot.get('response_price_count')}!={EXPECTED_INSTRUMENT_COUNT}"
        )
    if invalid:
        metadata_reasons.append(f"invalid_instruments:{len(invalid)}")
    if response_time is None:
        metadata_reasons.append("response_time_missing")
    elif response_time > completed + dt.timedelta(seconds=MAXIMUM_FUTURE_SKEW_SEC):
        metadata_reasons.append("response_time_ahead_of_completion")
    if round_trip < 0.0 or round_trip > MAXIMUM_REST_ROUND_TRIP_SEC:
        metadata_reasons.append(f"rest_round_trip_sec:{round_trip:.3f}")
    if delay < 0.0 or delay > MAXIMUM_ATTEMPT_DELAY_SEC:
        metadata_reasons.append(f"attempt_delay_sec:{delay:.3f}")
    if snapshot.get("environment") != "practice":
        metadata_reasons.append("environment_not_practice")
    if snapshot.get("request_method") != "GET":
        metadata_reasons.append("request_method_not_get")
    if snapshot.get("request_scope") != "pricing_read_only":
        metadata_reasons.append("request_scope_not_read_only")

    valid = not metadata_reasons and len(rows) == EXPECTED_INSTRUMENT_COUNT
    timing_quality = (
        "prospective_current_oanda_pricing_snapshot"
        if valid else "prospective_oanda_pricing_snapshot_invalid"
    )
    tradeable_quotes = (
        {name: row for name, row in rows.items() if row["tradeable"]} if valid else {}
    )
    nontradeable_quotes = {
        name: row for name, row in rows.items() if not row["tradeable"]
    }
    direct_currencies = {
        str(value).upper() for value in (attempt.get("event") or {}).get("direct_currencies", [])
    }
    direct_event_instruments = [
        instrument for instrument in EXPECTED_INSTRUMENTS
        if direct_currencies.intersection(instrument.split("_"))
    ]
    direct_event_tradeable = {
        name: tradeable_quotes[name]
        for name in direct_event_instruments if name in tradeable_quotes
    }
    capture_id = "scheduled_event_quote_v2_" + sha256_text(
        f"{attempt['clock_id']}|{attempt['horizon_min']}|{CONTRACT_ID}|{COHORT_ID}"
    )[:32]
    response_json = canonical_json(snapshot)
    return {
        "schema_version": SCHEMA_VERSION,
        "capture_id": capture_id,
        "clock_id": str(attempt["clock_id"]),
        "event_id": str(attempt["event_id"]),
        "event_scheduled_utc": str(attempt["scheduled_utc"]),
        "horizon_min": int(attempt["horizon_min"]),
        "target_utc": iso_utc(target),
        "retrieval_started_utc": iso_utc(started),
        "retrieval_completed_utc": iso_utc(completed),
        "response_time_utc": "" if response_time is None else iso_utc(response_time),
        "attempt_delay_sec": round(delay, 6),
        "rest_round_trip_sec": round(round_trip, 6),
        "timing_quality": timing_quality,
        "invalid_reason": ";".join(metadata_reasons),
        "universe_quote_count": len(rows),
        "tradeable_proof_count": len(tradeable_quotes),
        "nontradeable_observed_count": len(nontradeable_quotes),
        "direct_event_instruments": direct_event_instruments,
        "direct_event_tradeable_count": len(direct_event_tradeable),
        "broker_tick_older_than_15_sec_count": stale_tick_count,
        "broker_tick_age_is_diagnostic_only": True,
        "quotes": rows,
        "tradeable_quotes": tradeable_quotes,
        "nontradeable_quotes": nontradeable_quotes,
        "direct_event_tradeable_quotes": direct_event_tradeable,
        "invalid_instruments": invalid,
        "response_sha256": sha256_text(response_json),
        "response_metadata": {
            key: snapshot.get(key) for key in (
                "account_id_sha256", "account_suffix", "environment",
                "request_method", "request_scope", "response_price_count",
            )
        },
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


def insert_capture(connection: sqlite3.Connection, payload: Mapping[str, Any]) -> bool:
    payload_json = canonical_json(payload)
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO scheduled_event_quote_capture_v2 (
          capture_id,clock_id,horizon_min,target_utc,retrieval_started_utc,
          retrieval_completed_utc,attempt_delay_sec,rest_round_trip_sec,
          timing_quality,invalid_reason,universe_quote_count,
          tradeable_proof_count,nontradeable_observed_count,
          direct_event_tradeable_count,response_sha256,payload_json,
          payload_sha256,research_only,execution_eligible,can_place_orders,
          can_authorize,can_promote,contract_id,cohort_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            payload["capture_id"], payload["clock_id"], payload["horizon_min"],
            payload["target_utc"], payload["retrieval_started_utc"],
            payload["retrieval_completed_utc"], payload["attempt_delay_sec"],
            payload["rest_round_trip_sec"], payload["timing_quality"],
            payload["invalid_reason"], payload["universe_quote_count"],
            payload["tradeable_proof_count"],
            payload["nontradeable_observed_count"],
            payload["direct_event_tradeable_count"], payload["response_sha256"],
            payload_json, sha256_text(payload_json), 1, 0, 0, 0, 0,
            CONTRACT_ID, COHORT_ID,
        ),
    )
    connection.commit()
    return bool(cursor.rowcount > 0)


def database_counts(connection: sqlite3.Connection) -> dict[str, int]:
    clocks = int(connection.execute(
        "SELECT COUNT(*) FROM scheduled_event_clock_v2"
    ).fetchone()[0])
    row = connection.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM(timing_quality='prospective_current_oanda_pricing_snapshot'),0),
               COALESCE(SUM(timing_quality!='prospective_current_oanda_pricing_snapshot'),0),
               COALESCE(SUM(universe_quote_count),0),
               COALESCE(SUM(tradeable_proof_count),0),
               COALESCE(SUM(nontradeable_observed_count),0),
               COALESCE(SUM(direct_event_tradeable_count),0)
        FROM scheduled_event_quote_capture_v2
        """
    ).fetchone()
    return {
        "registered_event_clocks": clocks,
        "terminal_capture_attempts": int(row[0] or 0),
        "valid_current_snapshots": int(row[1] or 0),
        "invalid_terminal_captures": int(row[2] or 0),
        "observed_universe_rows": int(row[3] or 0),
        "tradeable_proof_rows": int(row[4] or 0),
        "nontradeable_observed_rows": int(row[5] or 0),
        "direct_event_tradeable_rows": int(row[6] or 0),
    }


def run_cycle(
    *,
    config_path: Path = CONFIG_PATH,
    preflight_path: Path = PREFLIGHT_PATH,
    database_path: Path = DATABASE_PATH,
    state_path: Path = STATE_PATH,
    heartbeat_path: Path = HEARTBEAT_PATH,
    now: dt.datetime | None = None,
    snapshot_loader: Callable[[], Mapping[str, Any]],
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
            started = cycle_now if now is not None else utc_now()
            snapshot: Mapping[str, Any] = {}
            error = ""
            if (started - target).total_seconds() <= MAXIMUM_ATTEMPT_DELAY_SEC:
                try:
                    snapshot = snapshot_loader()
                except Exception as exc:  # persist one fail-closed terminal attempt
                    error = f"{type(exc).__name__}:{exc}"[:500]
            else:
                error = "target_late_before_snapshot_read"
            completed = cycle_now if now is not None else utc_now()
            payload = build_capture(
                due, snapshot,
                fallback_started_utc=started,
                fallback_completed_utc=completed,
                snapshot_error=error,
            )
            attempted += 1
            inserted += int(insert_capture(connection, payload))
        counts = database_counts(connection)
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        next_row = connection.execute(
            """
            SELECT c.scheduled_utc,c.event_id,c.headline
            FROM scheduled_event_clock_v2 c
            WHERE NOT EXISTS (
              SELECT 1 FROM scheduled_event_quote_capture_v2 q
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
            None if next_row is None else {
                "scheduled_utc": str(next_row[0]),
                "event_id": str(next_row[1]),
                "headline": str(next_row[2]),
            }
        ),
        "horizons_min": list(HORIZONS_MIN),
        "expected_instrument_count": EXPECTED_INSTRUMENT_COUNT,
        "expected_instrument_universe_sha256": EXPECTED_UNIVERSE_SHA256,
        "quote_acquisition": "one_read_only_oanda_practice_pricing_request_all68",
        "currentness_clock": "pricing_response_observed_utc",
        "broker_price_time_role": "preserved_diagnostic_not_currentness_gate",
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
    parser.add_argument("--creds", type=Path, default=ROOT / "creds")
    parser.add_argument("--account-key", default=ACCOUNT_KEY)
    parser.add_argument("--account-id", default="")
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args(argv)
    validate_config()
    if "practice" not in str(lab.BASE_URL).lower():
        raise SystemExit("V2 scheduled capture is restricted to OANDA practice")
    startup = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "cohort_id": COHORT_ID,
        "generated_utc": iso_utc(),
        "status": "starting",
        "phase": "reading_practice_credentials",
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_authorize": False,
        "can_promote": False,
    }
    write_json_atomic(HEARTBEAT_PATH, startup)
    token, account_id = rotation.read_credentials(
        args.creds, args.account_key, args.account_id
    )
    startup["generated_utc"] = iso_utc()
    startup["phase"] = "loading_practice_instrument_metadata"
    write_json_atomic(HEARTBEAT_PATH, startup)
    client = lab.MarketDataClient(token)
    pip_sizes = lab.account_pip_sizes(client, account_id)
    startup["generated_utc"] = iso_utc()
    startup["phase"] = "starting_prospective_cycles"
    startup["instrument_metadata_count"] = len(pip_sizes)
    write_json_atomic(HEARTBEAT_PATH, startup)
    loader = lambda: build_rest_snapshot(client, account_id, pip_sizes)
    started = time.monotonic()
    while True:
        cycle_started = time.monotonic()
        try:
            run_cycle(snapshot_loader=loader)
        except Exception as exc:
            write_json_atomic(
                HEARTBEAT_PATH,
                {
                    "schema_version": SCHEMA_VERSION,
                    "contract_id": CONTRACT_ID,
                    "cohort_id": COHORT_ID,
                    "generated_utc": iso_utc(),
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}"[:500],
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
