#!/usr/bin/env python3
"""Locked later-period replay of one RBNZ response-entry candidate.

The candidate was selected from the separate 2024-02-28 through 2025-08-20
schedule cohort.  This module freezes one rule, six later scheduled RBNZ
decisions, and two same-local-clock controls per decision before requesting
any OANDA candle path:

* observe the first complete M1 bar after the official 14:00 release;
* require median absolute NZD strength >= 1 bp and breadth >= 60%;
* select the cheapest agreeing direct NZD leg with spread <= 5 pips and an
  oriented move at least as large as the median response;
* follow the observed response direction for a fixed 15-minute hold;
* calculate outcomes from executable bid/ask candles and 0.25 pip modeled
  slippage.

The official OCR fact never supplies trade direction.  This is a temporally
later retrospective replay, not prospective proof.  It is immutable,
research-only, and cannot authorize, promote, or execute.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import statistics
from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from oanda_spike_blurb_event_watch_price_reacquisition_v3 import (
    MAX_WORKERS,
    fetch_job,
    resolve_readonly_oanda_client,
)
from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    CONTRACT_ID as DISCOVERY_CONTRACT_ID,
    DATABASE,
    canonical_json,
    immutable_insert,
    payload_row,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)
from oanda_spike_blurb_verified_event_response_replay_v2 import detect_arm, outcome


CONTRACT_ID = "spike_blurb_rbnz_response_confirmation_v1_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "rbnz_response_confirmation_v1"
)
FREEZE_UTC = "2026-08-20T20:15:00+00:00"
SCHEMA_VERSION = 1
EVENT_CURRENCY = "NZD"
AUCKLAND = ZoneInfo("Pacific/Auckland")
PRE_CLOCK_MINUTES = 5
POST_CLOCK_MINUTES = 20
HOLD_MINUTES = 15
CONTROL_DAY_OFFSETS = (-7, 7)
DIRECT_INSTRUMENTS = (
    "AUD_NZD", "EUR_NZD", "GBP_NZD", "NZD_CAD", "NZD_CHF",
    "NZD_HKD", "NZD_JPY", "NZD_SGD", "NZD_USD",
)
LOCKED_ARM_ID = "response_1m_breadth_locked_h15"
LOCKED_ARM = {
    "start_min": 1,
    "end_min": 1,
    "minimum_strength_bps": 1.0,
    "persistence_min": 1,
    "technical_confirmation": False,
}
SCHEDULE_SOURCE_URL = (
    "https://www.rbnz.govt.nz/news-and-events/news/2024/06/"
    "monetary-policy-announcement-and-financial-stability-report-dates-for-late-2025-and-2026"
)
PAST_DECISIONS_URL = "https://www.rbnz.govt.nz/monetary-policy/monetary-policy-decisions"

# Date, expected exact UTC clock for 14:00 Pacific/Auckland, OCR, delta, URL.
# These are all scheduled decisions after the discovery cohort cutoff.
SCHEDULE = (
    (
        "2025-10-08", "2025-10-08T01:00:00+00:00", 2.50, -0.50,
        "https://www.rbnz.govt.nz/news-and-events/news/2025/10/ocr-reduced-to-2-5-percent",
    ),
    (
        "2025-11-26", "2025-11-26T01:00:00+00:00", 2.25, -0.25,
        "https://www.rbnz.govt.nz/news-and-events/news/2025/11/ocr-lowered-to-2-25-percent",
    ),
    (
        "2026-02-18", "2026-02-18T01:00:00+00:00", 2.25, 0.00,
        "https://www.rbnz.govt.nz/news-and-events/news/2026/02/"
        "ocr-on-hold-at-2-25-with-inflation-expected-to-fall",
    ),
    (
        "2026-04-08", "2026-04-08T02:00:00+00:00", 2.25, 0.00,
        "https://www.rbnz.govt.nz/news-and-events/news/2026/04/ocr-on-hold-at-2-25",
    ),
    (
        "2026-05-27", "2026-05-27T02:00:00+00:00", 2.25, 0.00,
        "https://www.rbnz.govt.nz/news-and-events/news/2026/05/ocr-held-at-2-25-percent",
    ),
    (
        "2026-07-08", "2026-07-08T02:00:00+00:00", 2.50, 0.25,
        "https://www.rbnz.govt.nz/news-and-events/news/2026/07/"
        "ocr-increased-to-2-50-to-return-inflation-to-2-percent",
    ),
)


def exact_randomization_pvalue(triples: list[tuple[float, float, float]]) -> float:
    """One-sided exact p-value with treatment exchangeable inside each trio."""
    if not triples:
        raise ValueError("triples_required")
    observed_sum = sum(a - (b + c) / 2.0 for a, b, c in triples)
    permutation_sums = [0.0]
    for a, b, c in triples:
        effects = (
            a - (b + c) / 2.0,
            b - (a + c) / 2.0,
            c - (a + b) / 2.0,
        )
        permutation_sums = [running + effect for running in permutation_sums for effect in effects]
    return sum(value >= observed_sum - 1e-12 for value in permutation_sums) / len(permutation_sums)


def event_id(event_date: str) -> str:
    return f"rbnz_policy_{event_date.replace('-', '')}_later_v1"


def validate_schedule() -> list[dict[str, Any]]:
    if len(SCHEDULE) != 6:
        raise RuntimeError("exact_six_later_scheduled_decisions_required")
    rows: list[dict[str, Any]] = []
    previous_clock: datetime | None = None
    for event_date, expected_utc, rate, change, release_url in SCHEDULE:
        local = datetime.combine(date.fromisoformat(event_date), dt_time(14, 0), tzinfo=AUCKLAND)
        utc = local.astimezone(timezone.utc)
        if utc.isoformat() != expected_utc:
            raise RuntimeError(f"auckland_clock_conversion_mismatch:{event_date}")
        if previous_clock is not None and utc <= previous_clock:
            raise RuntimeError("schedule_not_strictly_increasing")
        if not release_url.startswith("https://www.rbnz.govt.nz/"):
            raise RuntimeError(f"nonofficial_release_url:{event_date}")
        rows.append({
            "event_id": event_id(event_date),
            "event_date": event_date,
            "event_clock_local": local.isoformat(),
            "event_clock_utc": utc.isoformat(),
            "event_epoch": int(utc.timestamp()),
            "event_currency": EVENT_CURRENCY,
            "official_cash_rate_percent": float(rate),
            "official_cash_rate_change_percentage_points": float(change),
            "decision_action": "cut" if change < 0 else "raise" if change > 0 else "hold",
            "release_url": release_url,
            "schedule_source_url": SCHEDULE_SOURCE_URL,
            "past_decisions_url": PAST_DECISIONS_URL,
            "source_direction_assigned": 0,
            "causal_consensus_state": "missing",
            "event_time_rate_repricing_state": "missing",
            "source_retrieval_state": "retrospectively_verified_official_release",
            "research_only": 1,
            "execution_eligible": 0,
            "forecast_proof_eligible": 0,
        })
        previous_clock = utc
    return rows


def build_clocks(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    event_dates = {date.fromisoformat(str(row["event_date"])) for row in events}
    rows: list[dict[str, Any]] = []
    for event in events:
        source_date = date.fromisoformat(str(event["event_date"]))
        for day_offset in (0, *CONTROL_DAY_OFFSETS):
            clock_date = source_date + timedelta(days=day_offset)
            if day_offset and clock_date in event_dates:
                raise RuntimeError(f"control_overlaps_policy_date:{clock_date}")
            local = datetime.combine(clock_date, dt_time(14, 0), tzinfo=AUCKLAND)
            utc = local.astimezone(timezone.utc)
            kind = "scheduled_event" if day_offset == 0 else "matched_non_event_control"
            clock_id = stable_id("rbnz_confirmation_clock", CONTRACT_ID, event["event_id"], day_offset)
            rows.append({
                "clock_id": clock_id,
                "contract_id": CONTRACT_ID,
                "source_event_id": event["event_id"],
                "clock_kind": kind,
                "day_offset": int(day_offset),
                "clock_local": local.isoformat(),
                "clock_utc": utc.isoformat(),
                "clock_epoch": int(utc.timestamp()),
                "selection_rule": (
                    "scheduled_rbnz_policy_clock" if day_offset == 0
                    else "same_weekday_same_1400_pacific_auckland_plus_or_minus_7_days"
                ),
                "research_only": 1,
                "execution_eligible": 0,
                "forecast_proof_eligible": 0,
            })
    if len(rows) != 18 or len({str(row["clock_id"]) for row in rows}) != 18:
        raise RuntimeError("exact_eighteen_unique_clocks_required")
    return sorted(rows, key=lambda row: (str(row["source_event_id"]), int(row["day_offset"])))


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_contracts (
          contract_id TEXT PRIMARY KEY, discovery_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          dependency_sha256_json TEXT NOT NULL, frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_events (
          event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_date TEXT NOT NULL,
          event_clock_local TEXT NOT NULL, event_clock_utc TEXT NOT NULL,
          event_currency TEXT NOT NULL, official_cash_rate_percent REAL NOT NULL,
          official_cash_rate_change_percentage_points REAL NOT NULL,
          decision_action TEXT NOT NULL, release_url TEXT NOT NULL,
          schedule_source_url TEXT NOT NULL, past_decisions_url TEXT NOT NULL,
          source_direction_assigned INTEGER NOT NULL, causal_consensus_state TEXT NOT NULL,
          event_time_rate_repricing_state TEXT NOT NULL, source_retrieval_state TEXT NOT NULL,
          event_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_clocks (
          clock_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          clock_kind TEXT NOT NULL, day_offset INTEGER NOT NULL,
          clock_local TEXT NOT NULL, clock_utc TEXT NOT NULL,
          selection_rule TEXT NOT NULL, clock_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,source_event_id,day_offset)
        );
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_price_windows (
          window_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          instrument TEXT NOT NULL, requested_start_utc TEXT NOT NULL,
          requested_end_utc TEXT NOT NULL, coverage_state TEXT NOT NULL,
          candle_count INTEGER NOT NULL, first_utc TEXT, last_utc TEXT,
          average_spread_pips REAL, maximum_spread_pips REAL,
          http_status INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
          error_text TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
          payload_gzip BLOB NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,clock_id,instrument)
        );
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_decisions (
          decision_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, clock_kind TEXT NOT NULL, day_offset INTEGER NOT NULL,
          arm_id TEXT NOT NULL, hold_minutes INTEGER NOT NULL, detected INTEGER NOT NULL,
          detection_utc TEXT, currency_direction TEXT, currency_strength_bps REAL,
          breadth REAL, selected_instrument TEXT, selected_pair_direction TEXT,
          entry_spread_pips REAL, net_after_cost_pips REAL NOT NULL,
          mfe_pips REAL, mae_pips REAL, cost_cleared INTEGER NOT NULL,
          decision_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,clock_id,arm_id,hold_minutes)
        );
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_contract_no_update BEFORE UPDATE ON rbnz_confirmation_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_contract_no_delete BEFORE DELETE ON rbnz_confirmation_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_event_no_update BEFORE UPDATE ON rbnz_confirmation_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_event_no_delete BEFORE DELETE ON rbnz_confirmation_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_clock_no_update BEFORE UPDATE ON rbnz_confirmation_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_clock_no_delete BEFORE DELETE ON rbnz_confirmation_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_price_no_update BEFORE UPDATE ON rbnz_confirmation_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_price_no_delete BEFORE DELETE ON rbnz_confirmation_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_decision_no_update BEFORE UPDATE ON rbnz_confirmation_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_decision_no_delete BEFORE DELETE ON rbnz_confirmation_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(
    connection: sqlite3.Connection,
    events: list[dict[str, Any]],
    clocks: list[dict[str, Any]],
) -> dict[str, Any]:
    dependency_paths = (
        Path(__file__).resolve().parent / "oanda_spike_blurb_event_watch_price_reacquisition_v3.py",
        Path(__file__).resolve().parent / "oanda_spike_blurb_verified_event_response_replay_v2.py",
        Path(__file__).resolve().parent / "oanda_spike_blurb_rbnz_schedule_cohort_v1.py",
    )
    dependencies = {path.name: sha256_file(path) for path in dependency_paths}
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "discovery_contract_id": DISCOVERY_CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "dependency_sha256": dependencies,
        "selection_state": "single_candidate_frozen_before_price_query",
        "temporal_role": "later_period_retrospective_replication_replay",
        "candidate": {
            "event_population": "every_scheduled_rbnz_decision_20251008_through_20260708",
            "event_clock": "official_1400_pacific_auckland",
            "direction_source": "first_complete_m1_cross_pair_response_only",
            "arm_id": LOCKED_ARM_ID,
            "arm": LOCKED_ARM,
            "hold_minutes": HOLD_MINUTES,
            "instrument_universe": list(DIRECT_INSTRUMENTS),
            "instrument_count": len(DIRECT_INSTRUMENTS),
            "maximum_entry_spread_pips": 5.0,
            "modeled_slippage_pips": 0.25,
            "missing_detection_policy": "explicit_no_trade_zero_return",
        },
        "matched_controls": {
            "day_offsets": list(CONTROL_DAY_OFFSETS),
            "clock_rule": "same_weekday_same_1400_pacific_auckland",
            "predeclared_with_candidate": True,
        },
        "diagnostic_acceptance": {
            "treatment_average_net_pips_min_exclusive": 0.0,
            "incremental_average_net_pips_min_exclusive": 0.0,
            "treatment_cost_clearance_rate_min": 2.0 / 3.0,
            "minimum_leave_one_out_treatment_average_min_exclusive": 0.0,
            "exact_randomization_pvalue_max": 0.05,
        },
        "event_count": len(events),
        "clock_count": len(clocks),
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM rbnz_confirmation_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_confirmation_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO rbnz_confirmation_contracts VALUES (?,?,?,?,?,?)",
            (CONTRACT_ID, DISCOVERY_CONTRACT_ID, encoded, contract["builder_sha256"], canonical_json(dependencies), FREEZE_UTC),
        )
    for event in events:
        stored = {key: value for key, value in event.items() if key != "event_epoch"}
        stored["contract_id"] = CONTRACT_ID
        immutable_insert(connection, "rbnz_confirmation_events", payload_row(stored, "event_json"), "event_id")
    for clock in clocks:
        stored = {key: value for key, value in clock.items() if key != "clock_epoch"}
        immutable_insert(connection, "rbnz_confirmation_clocks", payload_row(stored, "clock_json"), "clock_id")
    connection.commit()
    return contract


def request_jobs(clocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for clock in clocks:
        event_time = datetime.fromisoformat(str(clock["clock_utc"]))
        start = event_time - timedelta(minutes=PRE_CLOCK_MINUTES)
        end = event_time + timedelta(minutes=POST_CLOCK_MINUTES)
        for instrument in DIRECT_INSTRUMENTS:
            jobs.append({
                "case_id": clock["clock_id"],
                "clock_id": clock["clock_id"],
                "instrument": instrument,
                "start_epoch": int(start.timestamp()),
                "end_epoch": int(end.timestamp()),
                "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
            })
    return jobs


def store_price(connection: sqlite3.Connection, result: Mapping[str, Any]) -> None:
    payload = {
        "window_id": stable_id("rbnz_confirmation_price", CONTRACT_ID, result["clock_id"], result["instrument"]),
        "contract_id": CONTRACT_ID,
        "clock_id": result["clock_id"],
        "instrument": result["instrument"],
        "requested_start_utc": result["start_utc"],
        "requested_end_utc": result["end_utc"],
        "coverage_state": result["coverage_state"],
        "candle_count": int(result["candle_count"]),
        "first_utc": result["first_utc"],
        "last_utc": result["last_utc"],
        "average_spread_pips": result["average_spread_pips"],
        "maximum_spread_pips": result["maximum_spread_pips"],
        "http_status": int(result["http_status"]),
        "latency_ms": int(result["latency_ms"]),
        "error_text": result["error_text"],
        "payload_sha256": result["payload_sha256"],
        "payload_gzip": result["payload_gzip"],
        "research_only": 1,
        "execution_eligible": 0,
        "forecast_proof_eligible": 0,
    }
    columns = list(payload)
    connection.execute(
        f"INSERT OR IGNORE INTO rbnz_confirmation_price_windows ({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})",
        [payload[column] for column in columns],
    )
    existing = connection.execute(
        "SELECT payload_sha256 FROM rbnz_confirmation_price_windows WHERE window_id=?",
        (payload["window_id"],),
    ).fetchone()
    if existing is None or str(existing[0]) != str(payload["payload_sha256"]):
        raise RuntimeError(f"immutable_confirmation_price_conflict:{payload['window_id']}")


def load_paths(
    connection: sqlite3.Connection,
    clocks: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], str]:
    output = {
        str(clock["clock_id"]): {
            **clock,
            "event_epoch": int(clock["clock_epoch"]),
            "event_currency": EVENT_CURRENCY,
            "paths": {},
        }
        for clock in clocks
    }
    seen: dict[str, set[str]] = defaultdict(set)
    digest = hashlib.sha256()
    for row in connection.execute(
        "SELECT clock_id,instrument,coverage_state,payload_sha256,payload_gzip "
        "FROM rbnz_confirmation_price_windows WHERE contract_id=? ORDER BY clock_id,instrument",
        (CONTRACT_ID,),
    ):
        clock_id, instrument, state, payload_hash = map(str, row[:4])
        seen[clock_id].add(instrument)
        digest.update(canonical_json([clock_id, instrument, state, payload_hash]).encode())
        if state != "exact_window":
            raise RuntimeError(f"confirmation_requires_exact_direct_path:{clock_id}:{instrument}:{state}")
        decoded = gzip.decompress(row[4])
        if sha256_bytes(decoded) != payload_hash:
            raise RuntimeError(f"confirmation_price_hash_mismatch:{clock_id}:{instrument}")
        body = json.loads(decoded)
        candle_map = {int(candle["epoch"]): candle for candle in body["candles"]}
        if len(candle_map) != len(body["candles"]):
            raise RuntimeError(f"duplicate_confirmation_price_clock:{clock_id}:{instrument}")
        output[clock_id]["paths"][instrument] = candle_map
    expected = set(DIRECT_INSTRUMENTS)
    for clock_id, item in output.items():
        if seen[clock_id] != expected or set(item["paths"]) != expected:
            raise RuntimeError(f"confirmation_direct_universe_incomplete:{clock_id}")
    return output, digest.hexdigest()


def evaluate_clock(clock: Mapping[str, Any]) -> dict[str, Any]:
    detected = detect_arm(clock, LOCKED_ARM_ID, LOCKED_ARM)
    base = {
        "decision_id": stable_id("rbnz_confirmation_decision", CONTRACT_ID, clock["clock_id"], LOCKED_ARM_ID, HOLD_MINUTES),
        "contract_id": CONTRACT_ID,
        "clock_id": clock["clock_id"],
        "source_event_id": clock["source_event_id"],
        "clock_kind": clock["clock_kind"],
        "day_offset": int(clock["day_offset"]),
        "arm_id": LOCKED_ARM_ID,
        "hold_minutes": HOLD_MINUTES,
        "research_only": 1,
        "execution_eligible": 0,
        "forecast_proof_eligible": 0,
    }
    if detected is None:
        return {
            **base,
            "detected": 0,
            "detection_utc": None,
            "currency_direction": None,
            "currency_strength_bps": None,
            "breadth": None,
            "selected_instrument": None,
            "selected_pair_direction": None,
            "entry_spread_pips": None,
            "net_after_cost_pips": 0.0,
            "mfe_pips": None,
            "mae_pips": None,
            "cost_cleared": 0,
        }
    selected = str(detected["selected_instrument"])
    result = outcome(detected, clock["paths"][selected], HOLD_MINUTES)
    if result is None:
        raise RuntimeError(f"locked_outcome_unavailable:{clock['clock_id']}")
    return {
        **base,
        "detected": 1,
        "detection_utc": datetime.fromtimestamp(int(detected["detection_epoch"]), timezone.utc).isoformat(),
        "currency_direction": detected["currency_direction"],
        "currency_strength_bps": float(detected["currency_strength_bps"]),
        "breadth": float(detected["breadth"]),
        "selected_instrument": selected,
        "selected_pair_direction": detected["selected_pair_direction"],
        "entry_spread_pips": float(detected["entry_spread_pips"]),
        "net_after_cost_pips": float(result["net_after_cost_pips"]),
        "mfe_pips": float(result["mfe_pips"]),
        "mae_pips": float(result["mae_pips"]),
        "cost_cleared": int(result["cost_cleared"]),
    }


def store_decisions(
    connection: sqlite3.Connection,
    loaded: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in sorted(loaded.values(), key=lambda row: str(row["clock_utc"])):
        row = payload_row(evaluate_clock(item), "decision_json")
        immutable_insert(connection, "rbnz_confirmation_decisions", row, "decision_id")
        rows.append(row)
    connection.commit()
    return rows


def analyze(
    events: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> dict[str, Any]:
    by_event: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in decisions:
        by_event[str(row["source_event_id"])][int(row["day_offset"])] = row
    triples: list[tuple[float, float, float]] = []
    event_rows: list[dict[str, Any]] = []
    for event in events:
        event_id_value = str(event["event_id"])
        if set(by_event[event_id_value]) != {-7, 0, 7}:
            raise RuntimeError(f"exact_trio_required:{event_id_value}")
        treatment = by_event[event_id_value][0]
        minus = by_event[event_id_value][-7]
        plus = by_event[event_id_value][7]
        triple = tuple(float(row["net_after_cost_pips"]) for row in (treatment, minus, plus))
        triples.append(triple)  # type: ignore[arg-type]
        event_rows.append({
            "event_id": event_id_value,
            "event_date": event["event_date"],
            "decision_action": event["decision_action"],
            "rate_change_percentage_points": event["official_cash_rate_change_percentage_points"],
            "treatment_detected": int(treatment["detected"]),
            "treatment_direction": treatment["currency_direction"],
            "treatment_instrument": treatment["selected_instrument"],
            "treatment_net_pips": triple[0],
            "control_minus_7d_net_pips": triple[1],
            "control_plus_7d_net_pips": triple[2],
            "incremental_net_pips": triple[0] - (triple[1] + triple[2]) / 2.0,
        })
    treatment_values = [row[0] for row in triples]
    control_values = [value for row in triples for value in row[1:]]
    increments = [row[0] - (row[1] + row[2]) / 2.0 for row in triples]
    leave_one_out = [
        statistics.mean(value for index, value in enumerate(treatment_values) if index != omitted)
        for omitted in range(len(treatment_values))
    ]
    treatment_average = statistics.mean(treatment_values)
    control_average = statistics.mean(control_values)
    incremental_average = statistics.mean(increments)
    treatment_clearance = statistics.mean(
        int(by_event[str(event["event_id"])][0]["cost_cleared"]) for event in events
    )
    pvalue = exact_randomization_pvalue(triples)
    criteria = {
        "positive_treatment_average": treatment_average > 0.0,
        "positive_incremental_average": incremental_average > 0.0,
        "cost_clearance_at_least_two_thirds": treatment_clearance >= 2.0 / 3.0,
        "positive_minimum_leave_one_out_treatment_average": min(leave_one_out) > 0.0,
        "exact_randomization_pvalue_at_most_0_05": pvalue <= 0.05,
    }
    return {
        "independent_scheduled_event_count": len(events),
        "matched_control_count": len(events) * 2,
        "treatment_average_net_pips": treatment_average,
        "control_average_net_pips": control_average,
        "incremental_average_net_pips": incremental_average,
        "incremental_median_net_pips": statistics.median(increments),
        "incremental_positive_event_rate": statistics.mean(value > 0 for value in increments),
        "treatment_cost_clearance_rate": treatment_clearance,
        "treatment_no_trade_count": sum(int(by_event[str(event["event_id"])][0]["detected"]) == 0 for event in events),
        "control_no_trade_count": sum(int(row["detected"]) == 0 for row in decisions if int(row["day_offset"]) != 0),
        "minimum_leave_one_event_out_treatment_average_pips": min(leave_one_out),
        "exact_within_trio_randomization_pvalue": pvalue,
        "diagnostic_acceptance_criteria": criteria,
        "diagnostic_replication_passed": all(criteria.values()),
        "event_rows": event_rows,
    }


def write_report(result: Mapping[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "RBNZ_RESPONSE_CONFIRMATION_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    analysis = result["analysis"]
    lines = [
        "# Locked later RBNZ response replay V1",
        "",
        "- One candidate was frozen before these price paths were requested.",
        "- Candidate: first complete M1 NZD cross-pair response, 60% breadth, 1 bp median strength, cheapest agreeing <=5-pip-spread leg, fixed 15-minute hold.",
        "- Direction comes only from the observed price response; the official OCR fact remains unsigned.",
        "- Missing detections are explicit no-trade/zero-return observations.",
        "- This is a later-period retrospective replay, not prospective live proof or promotion evidence.",
        "- Execution decision: **no_trade**.",
        "",
        "## Locked comparison",
        "",
        f"- Scheduled decisions: **{analysis['independent_scheduled_event_count']}**.",
        f"- Matched controls: **{analysis['matched_control_count']}**.",
        f"- Treatment average: **{analysis['treatment_average_net_pips']:.3f} pips**.",
        f"- Control average: **{analysis['control_average_net_pips']:.3f} pips**.",
        f"- Incremental average: **{analysis['incremental_average_net_pips']:.3f} pips**.",
        f"- Treatment cost-clearance: **{analysis['treatment_cost_clearance_rate']:.1%}**.",
        f"- Minimum leave-one-event-out treatment average: **{analysis['minimum_leave_one_event_out_treatment_average_pips']:.3f} pips**.",
        f"- Exact within-trio randomization p: **{analysis['exact_within_trio_randomization_pvalue']:.6f}**.",
        f"- Predeclared diagnostic replication passed: **{analysis['diagnostic_replication_passed']}**.",
        "",
        "## Event rows",
        "",
        "| Date | OCR action | Response | Pair | Treatment | -7d | +7d | Incremental |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for row in analysis["event_rows"]:
        lines.append(
            f"| {row['event_date']} | {row['decision_action']} | {row['treatment_direction'] or 'no_trade'} | "
            f"{row['treatment_instrument'] or '-'} | {row['treatment_net_pips']:.3f} | "
            f"{row['control_minus_7d_net_pips']:.3f} | {row['control_plus_7d_net_pips']:.3f} | "
            f"{row['incremental_net_pips']:.3f} |"
        )
    lines.append("")
    (report_root / "RBNZ_RESPONSE_CONFIRMATION_V1.md").write_text("\n".join(lines), encoding="utf-8")


def run(
    database: Path = DATABASE,
    report_root: Path = REPORT_ROOT,
    workers: int = MAX_WORKERS,
    *,
    freeze_only: bool = False,
) -> dict[str, Any]:
    events = validate_schedule()
    clocks = build_clocks(events)
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    contract = freeze_contract(connection, events, clocks)
    if freeze_only:
        result = {
            "contract_id": CONTRACT_ID,
            "builder_sha256": contract["builder_sha256"],
            "event_count": len(events),
            "clock_count": len(clocks),
            "instrument_count": len(DIRECT_INSTRUMENTS),
            "selection_state": contract["selection_state"],
            "research_only": True,
            "execution_eligible": False,
            "forecast_proof_eligible": False,
            "supported_execution_decision": "no_trade",
        }
        connection.close()
        return result

    jobs = request_jobs(clocks)
    existing = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT clock_id,instrument FROM rbnz_confirmation_price_windows WHERE contract_id=?",
            (CONTRACT_ID,),
        )
    }
    pending = [job for job in jobs if (str(job["clock_id"]), str(job["instrument"])) not in existing]
    if pending:
        client, metadata = resolve_readonly_oanda_client()
        if metadata.get("environment") != "practice" or "api-fxpractice.oanda.com" not in str(metadata.get("base_url", "")):
            raise RuntimeError("practice_readonly_endpoint_required")
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=max(1, min(int(workers), MAX_WORKERS))) as executor:
            futures = {executor.submit(fetch_job, client, job): job for job in pending}
            for future in as_completed(futures):
                store_price(connection, future.result())
                connection.commit()

    loaded, price_snapshot = load_paths(connection, clocks)
    decisions = store_decisions(connection, loaded)
    analysis = analyze(events, decisions)
    states = dict(connection.execute(
        "SELECT coverage_state,count(*) FROM rbnz_confirmation_price_windows WHERE contract_id=? GROUP BY coverage_state",
        (CONTRACT_ID,),
    ).fetchall())
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = sum(
        int(connection.execute(
            f"SELECT coalesce(sum(execution_eligible),0) FROM {table} WHERE contract_id=?",
            (CONTRACT_ID,),
        ).fetchone()[0])
        for table in (
            "rbnz_confirmation_events",
            "rbnz_confirmation_clocks",
            "rbnz_confirmation_price_windows",
            "rbnz_confirmation_decisions",
        )
    )
    stable = {
        "contract_id": CONTRACT_ID,
        "discovery_contract_id": DISCOVERY_CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"],
        "price_snapshot_sha256": price_snapshot,
        "event_count": len(events),
        "clock_count": len(clocks),
        "instrument_count": len(DIRECT_INSTRUMENTS),
        "price_window_count": sum(int(value) for value in states.values()),
        "coverage_states": states,
        "decision_count": len(decisions),
        "analysis": analysis,
        "sqlite_integrity": integrity,
        "execution_eligible_count": execution_sum,
        "proof_state": "temporally_later_retrospective_replication_not_prospective_live_proof",
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    result = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": utc_now(),
        "fetched_this_run": len(pending),
        **stable,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
    }
    connection.close()
    if result["price_window_count"] != len(clocks) * len(DIRECT_INSTRUMENTS):
        raise RuntimeError("confirmation_price_coverage_incomplete")
    if len(decisions) != len(clocks):
        raise RuntimeError("confirmation_decision_coverage_incomplete")
    if integrity != "ok" or execution_sum != 0:
        raise RuntimeError("confirmation_integrity_or_safety_failure")
    write_report(result, report_root)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root, args.workers, freeze_only=args.freeze_only), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
