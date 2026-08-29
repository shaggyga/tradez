#!/usr/bin/env python3
"""Schedule-selected RBNZ policy-factor and response cohort.

This research cohort contains every scheduled RBNZ monetary-policy decision
from 28 February 2024 through 20 August 2025.  Selection is by the published
decision calendar, not by subsequent FX movement.  Official OCR facts are
kept directionless; executable bid/ask paths are used to reconstruct the
post-release response.  All outputs are immutable, retrospective, and barred
from authorization, promotion, or execution.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from oanda_spike_blurb_factor_reconstruction import CONFIG as RECONSTRUCTION_CONFIG, load_contract
from oanda_spike_blurb_event_watch_price_reacquisition_v3 import (
    MAX_WORKERS,
    fetch_job,
    resolve_readonly_oanda_client,
)
from oanda_spike_blurb_verified_event_response_replay_v2 import (
    ARMS,
    OUTCOME_HORIZONS_MIN,
    currency_snapshot,
    detect_arm,
    outcome,
)
from oanda_spike_blurb_verified_source_cases_v3 import (
    DATABASE,
    canonical_json,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)


CONTRACT_ID = "spike_blurb_rbnz_schedule_cohort_v1_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "rbnz_schedule_cohort_v1"
)
FREEZE_UTC = "2026-08-20T18:30:00+00:00"
SCHEMA_VERSION = 1
EVENT_CURRENCY = "NZD"
PRE_EVENT_MINUTES = 5
POST_EVENT_MINUTES = 125
TRAJECTORY_HORIZONS_MIN = (1, 3, 5, 10, 15, 30, 60)
MINIMUM_PRIOR_ANALOGS = 3

SCHEDULE_2024_URL = (
    "https://www.rbnz.govt.nz/news-and-events/news/2022/12/"
    "monetary-policy-announcement-and-financial-stability-report-dates-for-202324"
)
SCHEDULE_2025_URL = (
    "https://www.rbnz.govt.nz/news-and-events/news/2024/06/"
    "monetary-policy-announcement-and-financial-stability-report-dates-for-late-2025-and-2026"
)
PAST_DECISIONS_URL = "https://www.rbnz.govt.nz/Monetary%20policy/Monetary%20policy%20decisions"

# event date, exact UTC clock (14:00 Pacific/Auckland), OCR, delta, release URL
SCHEDULE = (
    ("2024-02-28", "2024-02-28T01:00:00+00:00", 5.50, 0.00,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/02/monetary-policy-remains-restrictive"),
    ("2024-04-10", "2024-04-10T02:00:00+00:00", 5.50, 0.00,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/04/official-cash-rate-remains-unchanged"),
    ("2024-05-22", "2024-05-22T02:00:00+00:00", 5.50, 0.00,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/05/official-cash-rate-to-remain-restrictive"),
    ("2024-07-10", "2024-07-10T02:00:00+00:00", 5.50, 0.00,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/07/ocr-5-50-inflation-approaching-target-range"),
    ("2024-08-14", "2024-08-14T02:00:00+00:00", 5.25, -0.25,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/08/ocr-5-25--monetary-restraint-tempered-as-inflation-converges-on-target"),
    ("2024-10-09", "2024-10-09T01:00:00+00:00", 4.75, -0.50,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/10/ocr-4-75-monetary-restraint-reduced-as-inflation-converges-to-target"),
    ("2024-11-27", "2024-11-27T01:00:00+00:00", 4.25, -0.50,
     "https://www.rbnz.govt.nz/news-and-events/news/2024/11/ocr-425-ocr-lowered-further-as-inflation-returns-to-target"),
    ("2025-02-19", "2025-02-19T01:00:00+00:00", 3.75, -0.50,
     "https://www.rbnz.govt.nz/news-and-events/news/2025/02/ocr-375-ocr-reduced-further-as-inflation-abates"),
    ("2025-04-09", "2025-04-09T02:00:00+00:00", 3.50, -0.25,
     "https://www.rbnz.govt.nz/news-and-events/news/2025/04/ocr-3-50-further-reduction-in-ocr-appropriate"),
    ("2025-05-28", "2025-05-28T02:00:00+00:00", 3.25, -0.25,
     "https://www.rbnz.govt.nz/news-and-events/news/2025/05/ocr-lowered-to-3-25"),
    ("2025-07-09", "2025-07-09T02:00:00+00:00", 3.25, 0.00,
     "https://www.rbnz.govt.nz/news-and-events/news/2025/07/ocr-3-25-percent-ocr-unchanged"),
    ("2025-08-20", "2025-08-20T02:00:00+00:00", 3.00, -0.25,
     "https://www.rbnz.govt.nz/news-and-events/news/2025/08/ocr-lowered-to-3-percent"),
)


def event_id(event_date: str) -> str:
    return f"rbnz_policy_{event_date.replace('-', '')}_schedule_v1"


def number_identity(value: float) -> str:
    return f"number:{float(value):+.8f}"


def analog_key(change: float, horizon: int) -> str:
    return "|".join((
        "monetary_policy_release", EVENT_CURRENCY, "policy_rate_action",
        "official_cash_rate_change", "percentage_points", number_identity(change), f"h{horizon}",
    ))


def validate_schedule() -> list[dict[str, Any]]:
    if len(SCHEDULE) != 12:
        raise ValueError("complete_20240228_through_20250820_schedule_required")
    rows: list[dict[str, Any]] = []
    prior_rate: float | None = None
    last_clock: datetime | None = None
    for event_date, clock_text, rate, change, url in SCHEDULE:
        clock = datetime.fromisoformat(clock_text)
        if clock.tzinfo is None or clock.utcoffset() != timedelta(0):
            raise ValueError(f"event_clock_must_be_utc:{event_date}")
        if clock.date().isoformat() != event_date:
            raise ValueError(f"event_date_clock_mismatch:{event_date}")
        if last_clock is not None and clock <= last_clock:
            raise ValueError("schedule_not_strictly_increasing")
        if not url.startswith("https://www.rbnz.govt.nz/"):
            raise ValueError(f"nonofficial_release_url:{event_date}")
        if prior_rate is not None and round(rate - prior_rate, 8) != round(change, 8):
            raise ValueError(f"rate_change_mismatch:{event_date}")
        rows.append({
            "event_id": event_id(event_date),
            "event_date": event_date,
            "event_clock_utc": clock.isoformat(),
            "event_epoch": int(clock.timestamp()),
            "event_currency": EVENT_CURRENCY,
            "event_family": "monetary_policy_release",
            "official_cash_rate_percent": float(rate),
            "official_cash_rate_change_percentage_points": float(change),
            "decision_action": "cut" if change < 0 else "hold" if change == 0 else "raise",
            "release_url": url,
            "clock_source_url": SCHEDULE_2024_URL if event_date < "2025-01-01" else SCHEDULE_2025_URL,
            "past_decisions_url": PAST_DECISIONS_URL,
            "selection_rule": "every_scheduled_rbnz_policy_decision_in_closed_date_range",
            "direction_interpretation": "not_assigned",
            "consensus_state": "causal_pre_release_decision_consensus_missing",
            "rate_repricing_state": "event_time_rate_repricing_missing",
            "research_only": True,
            "execution_eligible": False,
            "forecast_proof_eligible": False,
        })
        prior_rate = float(rate)
        last_clock = clock
    return rows


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rbnz_schedule_cohort_contracts (
          contract_id TEXT PRIMARY KEY, contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, dependency_sha256_json TEXT NOT NULL,
          universe_sha256 TEXT NOT NULL, frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_events (
          event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_date TEXT NOT NULL,
          event_clock_utc TEXT NOT NULL, event_currency TEXT NOT NULL,
          official_cash_rate_percent REAL NOT NULL,
          official_cash_rate_change_percentage_points REAL NOT NULL,
          decision_action TEXT NOT NULL, release_url TEXT NOT NULL,
          clock_source_url TEXT NOT NULL, past_decisions_url TEXT NOT NULL,
          selection_rule TEXT NOT NULL, direction_interpretation TEXT NOT NULL,
          consensus_state TEXT NOT NULL, rate_repricing_state TEXT NOT NULL,
          verified_retrieved_utc TEXT NOT NULL,
          event_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_date)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_factors (
          factor_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          factor_family TEXT NOT NULL, metric TEXT NOT NULL, value_number REAL NOT NULL,
          unit TEXT NOT NULL, value_identity TEXT NOT NULL,
          factor_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_id,metric)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_price_windows (
          window_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          instrument TEXT NOT NULL, requested_start_utc TEXT NOT NULL,
          requested_end_utc TEXT NOT NULL, coverage_state TEXT NOT NULL,
          candle_count INTEGER NOT NULL, first_utc TEXT, last_utc TEXT,
          average_spread_pips REAL, maximum_spread_pips REAL,
          http_status INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
          error_text TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
          payload_gzip BLOB NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_id,instrument)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_response_trajectories (
          trajectory_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL, response_utc TEXT NOT NULL,
          currency_direction TEXT NOT NULL, currency_strength_bps REAL NOT NULL,
          breadth REAL NOT NULL, leg_count INTEGER NOT NULL,
          agreeing_leg_count INTEGER NOT NULL, selected_instrument TEXT NOT NULL,
          selected_pair_direction TEXT NOT NULL, selected_spread_pips REAL NOT NULL,
          trajectory_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_id,horizon_minutes)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_prequential_predictions (
          prediction_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL, analog_key TEXT NOT NULL,
          factor_value_identity TEXT NOT NULL, prior_event_ids_json TEXT NOT NULL,
          prior_effective_n INTEGER NOT NULL, predicted_direction TEXT NOT NULL,
          predicted_strength_bps REAL NOT NULL, actual_direction TEXT NOT NULL,
          actual_strength_bps REAL NOT NULL, direction_correct INTEGER NOT NULL,
          prediction_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_id,horizon_minutes,analog_key)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_response_detections (
          detection_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, event_id TEXT NOT NULL,
          arm_id TEXT NOT NULL, detection_utc TEXT NOT NULL, elapsed_minutes INTEGER NOT NULL,
          currency_direction TEXT NOT NULL, currency_strength_bps REAL NOT NULL,
          breadth REAL NOT NULL, selected_instrument TEXT NOT NULL,
          selected_pair_direction TEXT NOT NULL, entry_spread_pips REAL NOT NULL,
          detection_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,event_id,arm_id)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_response_outcomes (
          outcome_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, detection_id TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL, net_after_cost_pips REAL NOT NULL,
          mfe_pips REAL NOT NULL, mae_pips REAL NOT NULL, cost_cleared INTEGER NOT NULL,
          outcome_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,detection_id,horizon_minutes)
        );
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_events_no_update BEFORE UPDATE ON rbnz_schedule_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_events_no_delete BEFORE DELETE ON rbnz_schedule_events BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_factors_no_update BEFORE UPDATE ON rbnz_schedule_factors BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_price_no_update BEFORE UPDATE ON rbnz_schedule_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_trajectory_no_update BEFORE UPDATE ON rbnz_schedule_response_trajectories BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_prediction_no_update BEFORE UPDATE ON rbnz_schedule_prequential_predictions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_detection_no_update BEFORE UPDATE ON rbnz_schedule_response_detections BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_schedule_outcome_no_update BEFORE UPDATE ON rbnz_schedule_response_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def immutable_insert(connection: sqlite3.Connection, table: str, row: Mapping[str, Any], key: str) -> None:
    columns = list(row)
    connection.execute(
        f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        [row[column] for column in columns],
    )
    existing = connection.execute(
        f"SELECT row_sha256 FROM {table} WHERE {key}=?", (row[key],)
    ).fetchone()
    if existing is None or str(existing[0]) != str(row["row_sha256"]):
        raise RuntimeError(f"immutable_row_conflict:{table}:{row[key]}")


def payload_row(payload: dict[str, Any], json_field: str) -> dict[str, Any]:
    encoded = canonical_json(payload)
    return {**payload, json_field: encoded, "row_sha256": sha256_bytes(encoded.encode())}


def source_rows(connection: sqlite3.Connection, events: list[dict[str, Any]]) -> None:
    for event in events:
        stored = {
            "event_id": event["event_id"], "contract_id": CONTRACT_ID,
            "event_date": event["event_date"], "event_clock_utc": event["event_clock_utc"],
            "event_currency": EVENT_CURRENCY,
            "official_cash_rate_percent": event["official_cash_rate_percent"],
            "official_cash_rate_change_percentage_points": event["official_cash_rate_change_percentage_points"],
            "decision_action": event["decision_action"], "release_url": event["release_url"],
            "clock_source_url": event["clock_source_url"],
            "past_decisions_url": event["past_decisions_url"],
            "selection_rule": event["selection_rule"],
            "direction_interpretation": event["direction_interpretation"],
            "consensus_state": event["consensus_state"],
            "rate_repricing_state": event["rate_repricing_state"],
            "verified_retrieved_utc": FREEZE_UTC,
            "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
        }
        immutable_insert(connection, "rbnz_schedule_events", payload_row(stored, "event_json"), "event_id")
        for metric, value, unit in (
            ("official_cash_rate", event["official_cash_rate_percent"], "percent"),
            ("official_cash_rate_change", event["official_cash_rate_change_percentage_points"], "percentage_points"),
        ):
            factor = {
                "factor_id": stable_id("rbnz_schedule_factor", CONTRACT_ID, event["event_id"], metric),
                "contract_id": CONTRACT_ID, "event_id": event["event_id"],
                "factor_family": "policy_rate_action", "metric": metric,
                "value_number": float(value), "unit": unit,
                "value_identity": number_identity(float(value)),
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            immutable_insert(connection, "rbnz_schedule_factors", payload_row(factor, "factor_json"), "factor_id")


def event_jobs(events: list[dict[str, Any]], instruments: list[str]) -> list[dict[str, Any]]:
    jobs = []
    for event in events:
        clock = datetime.fromisoformat(event["event_clock_utc"])
        start = clock - timedelta(minutes=PRE_EVENT_MINUTES)
        end = clock + timedelta(minutes=POST_EVENT_MINUTES)
        for instrument in instruments:
            jobs.append({
                "case_id": event["event_id"], "event_id": event["event_id"],
                "instrument": instrument, "start_epoch": int(start.timestamp()),
                "end_epoch": int(end.timestamp()), "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
            })
    return jobs


def store_price_result(connection: sqlite3.Connection, result: Mapping[str, Any]) -> None:
    payload = {
        "window_id": stable_id("rbnz_schedule_price", CONTRACT_ID, result["event_id"], result["instrument"]),
        "contract_id": CONTRACT_ID, "event_id": result["event_id"],
        "instrument": result["instrument"], "requested_start_utc": result["start_utc"],
        "requested_end_utc": result["end_utc"], "coverage_state": result["coverage_state"],
        "candle_count": int(result["candle_count"]), "first_utc": result["first_utc"],
        "last_utc": result["last_utc"], "average_spread_pips": result["average_spread_pips"],
        "maximum_spread_pips": result["maximum_spread_pips"],
        "http_status": int(result["http_status"]), "latency_ms": int(result["latency_ms"]),
        "error_text": result["error_text"], "payload_sha256": result["payload_sha256"],
        "payload_gzip": result["payload_gzip"], "research_only": 1,
        "execution_eligible": 0, "forecast_proof_eligible": 0,
    }
    columns = list(payload)
    connection.execute(
        f"INSERT OR IGNORE INTO rbnz_schedule_price_windows ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        [payload[column] for column in columns],
    )
    existing = connection.execute(
        "SELECT payload_sha256 FROM rbnz_schedule_price_windows WHERE window_id=?",
        (payload["window_id"],),
    ).fetchone()
    if existing is None or str(existing[0]) != str(payload["payload_sha256"]):
        raise RuntimeError(f"immutable_price_conflict:{payload['window_id']}")


def load_paths(connection: sqlite3.Connection, events: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], str]:
    output = {event["event_id"]: {**event, "paths": {}, "excluded": []} for event in events}
    digest = hashlib.sha256()
    seen: dict[str, set[str]] = defaultdict(set)
    for row in connection.execute(
        "SELECT event_id,instrument,coverage_state,payload_sha256,payload_gzip FROM rbnz_schedule_price_windows WHERE contract_id=? ORDER BY event_id,instrument",
        (CONTRACT_ID,),
    ):
        event = output[str(row[0])]
        instrument, state, payload_hash = str(row[1]), str(row[2]), str(row[3])
        seen[event["event_id"]].add(instrument)
        digest.update(canonical_json([event["event_id"], instrument, state, payload_hash]).encode())
        if state != "exact_window":
            event["excluded"].append({"instrument": instrument, "coverage_state": state})
            continue
        decoded = gzip.decompress(row[4])
        if sha256_bytes(decoded) != payload_hash:
            raise RuntimeError(f"price_payload_hash_mismatch:{event['event_id']}:{instrument}")
        body = json.loads(decoded)
        candle_map = {int(candle["epoch"]): candle for candle in body["candles"]}
        if len(candle_map) != len(body["candles"]):
            raise RuntimeError(f"duplicate_price_clock:{event['event_id']}:{instrument}")
        event["paths"][instrument] = candle_map
    for item in output.values():
        if len(seen[item["event_id"]]) != 68:
            raise RuntimeError(f"all_68_price_records_required:{item['event_id']}")
        direct = {pair for pair in seen[item["event_id"]] if EVENT_CURRENCY in pair.split("_")}
        exact_direct = {pair for pair in item["paths"] if EVENT_CURRENCY in pair.split("_")}
        if not direct or direct != exact_direct or len(item["paths"]) < 60:
            raise RuntimeError(f"event_currency_price_coverage_missing:{item['event_id']}")
    return output, digest.hexdigest()


def store_trajectories(connection: sqlite3.Connection, events: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in sorted(events.values(), key=lambda value: value["event_clock_utc"]):
        for horizon in TRAJECTORY_HORIZONS_MIN:
            response = currency_snapshot(item["paths"], EVENT_CURRENCY, item["event_epoch"], horizon)
            if response is None:
                raise RuntimeError(f"trajectory_unavailable:{item['event_id']}:h{horizon}")
            payload = {
                "trajectory_id": stable_id("rbnz_schedule_trajectory", CONTRACT_ID, item["event_id"], horizon),
                "contract_id": CONTRACT_ID, "event_id": item["event_id"],
                "horizon_minutes": horizon,
                "response_utc": datetime.fromtimestamp(item["event_epoch"] + horizon * 60, timezone.utc).isoformat(),
                "currency_direction": response["currency_direction"],
                "currency_strength_bps": float(response["currency_strength_bps"]),
                "breadth": float(response["breadth"]), "leg_count": int(response["leg_count"]),
                "agreeing_leg_count": int(response["agreeing_leg_count"]),
                "selected_instrument": response["selected_instrument"],
                "selected_pair_direction": response["selected_pair_direction"],
                "selected_spread_pips": float(response["entry_spread_pips"]),
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            row = payload_row(payload, "trajectory_json")
            immutable_insert(connection, "rbnz_schedule_response_trajectories", row, "trajectory_id")
            rows.append(row)
    return rows


def build_prequential_predictions(trajectories: list[dict[str, Any]], event_map: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in trajectories:
        by_horizon[int(row["horizon_minutes"])].append(row)
    predictions: list[dict[str, Any]] = []
    for horizon, rows in sorted(by_horizon.items()):
        rows.sort(key=lambda row: event_map[str(row["event_id"])]["event_clock_utc"])
        history: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            event = event_map[str(row["event_id"])]
            identity = number_identity(float(event["official_cash_rate_change_percentage_points"]))
            key = analog_key(float(event["official_cash_rate_change_percentage_points"]), horizon)
            prior = history[key]
            if len(prior) >= MINIMUM_PRIOR_ANALOGS:
                predicted = float(statistics.median(float(item["currency_strength_bps"]) for item in prior))
                actual = float(row["currency_strength_bps"])
                payload = {
                    "prediction_id": stable_id("rbnz_schedule_prequential", CONTRACT_ID, row["event_id"], horizon, key),
                    "contract_id": CONTRACT_ID, "event_id": row["event_id"],
                    "horizon_minutes": horizon, "analog_key": key,
                    "factor_value_identity": identity,
                    "prior_event_ids_json": canonical_json([item["event_id"] for item in prior]),
                    "prior_effective_n": len(prior),
                    "predicted_direction": "stronger" if predicted > 0 else "weaker",
                    "predicted_strength_bps": predicted,
                    "actual_direction": "stronger" if actual > 0 else "weaker",
                    "actual_strength_bps": actual,
                    "direction_correct": int((predicted > 0) == (actual > 0)),
                    "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
                }
                predictions.append(payload_row(payload, "prediction_json"))
            history[key].append(row)
    return predictions


def store_replay(connection: sqlite3.Connection, events: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter[str]]:
    detections: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    exclusions: Counter[str] = Counter()
    for item in sorted(events.values(), key=lambda value: value["event_clock_utc"]):
        for arm_id, arm in ARMS.items():
            detected = detect_arm(item, arm_id, arm)
            if detected is None:
                exclusions[f"no_detection:{arm_id}"] += 1
                continue
            detection = {
                "detection_id": stable_id("rbnz_schedule_detection", CONTRACT_ID, item["event_id"], arm_id),
                "contract_id": CONTRACT_ID, "event_id": item["event_id"], "arm_id": arm_id,
                "detection_utc": datetime.fromtimestamp(int(detected["detection_epoch"]), timezone.utc).isoformat(),
                "elapsed_minutes": int(detected["elapsed_minutes"]),
                "currency_direction": detected["currency_direction"],
                "currency_strength_bps": float(detected["currency_strength_bps"]),
                "breadth": float(detected["breadth"]),
                "selected_instrument": detected["selected_instrument"],
                "selected_pair_direction": detected["selected_pair_direction"],
                "entry_spread_pips": float(detected["entry_spread_pips"]),
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            detection_row = payload_row(detection, "detection_json")
            immutable_insert(connection, "rbnz_schedule_response_detections", detection_row, "detection_id")
            detections.append(detection_row)
            selected_path = item["paths"][detected["selected_instrument"]]
            for horizon in OUTCOME_HORIZONS_MIN:
                result = outcome(detected, selected_path, horizon)
                if result is None:
                    exclusions[f"outcome_unavailable:h{horizon}"] += 1
                    continue
                outcome_payload = {
                    "outcome_id": stable_id("rbnz_schedule_outcome", CONTRACT_ID, detection["detection_id"], horizon),
                    "contract_id": CONTRACT_ID, "detection_id": detection["detection_id"],
                    "horizon_minutes": horizon,
                    "net_after_cost_pips": float(result["net_after_cost_pips"]),
                    "mfe_pips": float(result["mfe_pips"]), "mae_pips": float(result["mae_pips"]),
                    "cost_cleared": int(result["cost_cleared"]),
                    "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
                }
                outcome_row = payload_row(outcome_payload, "outcome_json")
                immutable_insert(connection, "rbnz_schedule_response_outcomes", outcome_row, "outcome_id")
                outcomes.append(outcome_row)
    return detections, outcomes, exclusions


def report_result(connection: sqlite3.Connection, events: list[dict[str, Any]], fetched: int) -> dict[str, Any]:
    state_counts = dict(connection.execute(
        "SELECT coverage_state,count(*) FROM rbnz_schedule_price_windows WHERE contract_id=? GROUP BY coverage_state",
        (CONTRACT_ID,),
    ).fetchall())
    trajectories = [dict(row) for row in connection.execute(
        "SELECT event_id,horizon_minutes,currency_direction,currency_strength_bps,breadth,selected_instrument,selected_pair_direction,selected_spread_pips,row_sha256 FROM rbnz_schedule_response_trajectories WHERE contract_id=? ORDER BY event_id,horizon_minutes",
        (CONTRACT_ID,),
    )]
    predictions = [dict(row) for row in connection.execute(
        "SELECT event_id,horizon_minutes,factor_value_identity,prior_effective_n,predicted_direction,predicted_strength_bps,actual_direction,actual_strength_bps,direction_correct,row_sha256 FROM rbnz_schedule_prequential_predictions WHERE contract_id=? ORDER BY event_id,horizon_minutes",
        (CONTRACT_ID,),
    )]
    outcome_summary = [dict(row) for row in connection.execute(
        "SELECT d.arm_id,o.horizon_minutes,count(*) n,avg(o.net_after_cost_pips) avg_net_pips,avg(o.cost_cleared) win_rate FROM rbnz_schedule_response_outcomes o JOIN rbnz_schedule_response_detections d ON d.detection_id=o.detection_id WHERE o.contract_id=? GROUP BY d.arm_id,o.horizon_minutes ORDER BY d.arm_id,o.horizon_minutes",
        (CONTRACT_ID,),
    )]
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = 0
    for table in (
        "rbnz_schedule_events", "rbnz_schedule_factors", "rbnz_schedule_price_windows",
        "rbnz_schedule_response_trajectories", "rbnz_schedule_prequential_predictions",
        "rbnz_schedule_response_detections", "rbnz_schedule_response_outcomes",
    ):
        execution_sum += int(connection.execute(
            f"SELECT coalesce(sum(execution_eligible),0) FROM {table} WHERE contract_id=?", (CONTRACT_ID,)
        ).fetchone()[0])
    stable = {
        "contract_id": CONTRACT_ID,
        "event_count": len(events), "instrument_count": 68,
        "price_window_count": sum(int(value) for value in state_counts.values()),
        "coverage_states": state_counts,
        "trajectory_count": len(trajectories), "prediction_count": len(predictions),
        "prediction_direction_accuracy": (
            sum(int(row["direction_correct"]) for row in predictions) / len(predictions)
            if predictions else None
        ),
        "trajectory_rows": trajectories, "prequential_rows": predictions,
        "response_entry_cells": outcome_summary,
        "execution_eligible_count": execution_sum, "sqlite_integrity": integrity,
        "selection_rule": "every_scheduled_rbnz_policy_decision_20240228_through_20250820",
        "proof_state": "retrospective_prequential_diagnostic_not_untouched_confirmation",
        "supported_execution_decision": "no_trade",
    }
    return {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(),
        "fetched_this_run": fetched, **stable,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
    }


def write_report(result: dict[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "RBNZ_SCHEDULE_COHORT_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    accuracy = result["prediction_direction_accuracy"]
    lines = [
        "# Schedule-selected RBNZ policy cohort V1", "",
        f"- Consecutive scheduled decisions: **{result['event_count']}**.",
        f"- All-68 executable price windows: **{result['price_window_count']}**.",
        f"- Coverage: `{canonical_json(result['coverage_states'])}`.",
        f"- Raw factor-response trajectories: **{result['trajectory_count']}**.",
        f"- Retrospective prequential predictions: **{result['prediction_count']}**.",
        f"- Prequential directional accuracy: **{accuracy:.1%}**." if accuracy is not None else "- Prequential directional accuracy: **not available**.",
        "- Event selection is schedule-based, not movement-based.",
        "- Source sign remains unassigned because causal consensus and event-time rate repricing are absent.",
        "- This is retrospective diagnostic evidence, not untouched confirmation.",
        "- Execution decision: **no_trade**.", "", "## Response-entry cells", "",
        "| Arm | Horizon | N | Avg net pips | Cost-clearing rate |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in result["response_entry_cells"]:
        lines.append(
            f"| {row['arm_id']} | {row['horizon_minutes']}m | {row['n']} | "
            f"{row['avg_net_pips']:.3f} | {row['win_rate']:.1%} |"
        )
    lines.append("")
    (report_root / "RBNZ_SCHEDULE_COHORT_V1.md").write_text("\n".join(lines), encoding="utf-8")


def run(database: Path = DATABASE, report_root: Path = REPORT_ROOT, workers: int = MAX_WORKERS, *, source_only: bool = False) -> dict[str, Any]:
    events = validate_schedule()
    reconstruction = load_contract(RECONSTRUCTION_CONFIG)
    instruments = sorted(str(value) for value in reconstruction["expected_instruments"])
    if len(instruments) != 68 or len(set(instruments)) != 68:
        raise RuntimeError("exact_68_instrument_universe_required")
    dependencies = {
        path.name: sha256_file(path)
        for path in (
            Path(__file__).resolve().parent / "oanda_spike_blurb_event_watch_price_reacquisition_v3.py",
            Path(__file__).resolve().parent / "oanda_spike_blurb_verified_event_response_replay_v2.py",
            Path(__file__).resolve().parent / "oanda_spike_blurb_factor_reconstruction.py",
        )
    }
    builder_hash = sha256_file(Path(__file__).resolve())
    universe_hash = sha256_bytes(canonical_json(instruments).encode())
    contract = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "builder_sha256": builder_hash, "dependency_sha256": dependencies,
        "universe_sha256": universe_hash, "instrument_count": 68,
        "event_count": len(events), "event_date_start": events[0]["event_date"],
        "event_date_end": events[-1]["event_date"],
        "selection_rule": "every_scheduled_rbnz_policy_decision_in_closed_date_range",
        "event_clock_rule": "official_14_00_pacific_auckland_converted_to_utc",
        "source_direction_assignment": "forbidden",
        "causal_consensus_available": False, "event_time_rate_repricing_available": False,
        "minimum_prior_analogs": MINIMUM_PRIOR_ANALOGS,
        "research_only": True, "execution_eligible": False, "forecast_proof_eligible": False,
    }
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    contract_json = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM rbnz_schedule_cohort_contracts WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()
    if existing is not None and str(existing[0]) != contract_json:
        raise RuntimeError("immutable_rbnz_schedule_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO rbnz_schedule_cohort_contracts VALUES (?,?,?,?,?,?)",
            (CONTRACT_ID, contract_json, builder_hash, canonical_json(dependencies), universe_hash, FREEZE_UTC),
        )
    source_rows(connection, events)
    connection.commit()
    if source_only:
        result = {
            "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
            "generated_utc": utc_now(), "event_count": len(events), "factor_count": len(events) * 2,
            "selection_rule": contract["selection_rule"], "research_only": True,
            "execution_eligible": False, "forecast_proof_eligible": False,
            "supported_execution_decision": "no_trade",
        }
        connection.close()
        return result
    jobs = event_jobs(events, instruments)
    existing_keys = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT event_id,instrument FROM rbnz_schedule_price_windows WHERE contract_id=?",
            (CONTRACT_ID,),
        )
    }
    pending = [job for job in jobs if (job["event_id"], job["instrument"]) not in existing_keys]
    if pending:
        client, meta = resolve_readonly_oanda_client()
        if meta.get("environment") != "practice" or "api-fxpractice.oanda.com" not in meta.get("base_url", ""):
            raise RuntimeError("practice_readonly_endpoint_required")
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max(1, min(int(workers), MAX_WORKERS))) as executor:
            futures = {executor.submit(fetch_job, client, job): job for job in pending}
            for future in as_completed(futures):
                store_price_result(connection, future.result())
                connection.commit()
    loaded, _ = load_paths(connection, events)
    trajectories = store_trajectories(connection, loaded)
    event_map = {event["event_id"]: event for event in events}
    predictions = build_prequential_predictions(trajectories, event_map)
    for row in predictions:
        immutable_insert(connection, "rbnz_schedule_prequential_predictions", row, "prediction_id")
    store_replay(connection, loaded)
    connection.commit()
    result = report_result(connection, events, len(pending))
    connection.close()
    if result["price_window_count"] != 816 or result["event_count"] != 12:
        raise RuntimeError("schedule_cohort_price_coverage_incomplete")
    if result["execution_eligible_count"] != 0 or result["sqlite_integrity"] != "ok":
        raise RuntimeError("schedule_cohort_safety_or_integrity_failure")
    write_report(result, report_root)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--source-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root, args.workers, source_only=args.source_only), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
