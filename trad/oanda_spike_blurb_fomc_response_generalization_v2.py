#!/usr/bin/env python3
"""Availability-repaired evaluation of frozen FOMC generalization V1.

V1 froze the RBNZ-selected candidate and acquired all 1,260 requested FOMC
event/control windows before failing closed.  Its all-20-exact-path condition
was too strict: one control was Christmas and several thin crosses had sparse
M1 updates.  V2 does not change the candidate, event set, price observations,
direction rule, spread gate, or horizon.

Before any fallback price request or outcome calculation, V2 freezes this
availability-only policy:

* every original clock with at least three direct USD legs containing the
  event/detection candle remains unchanged;
* a non-event control below that minimum uses the first usable same-weekday
  candidate farther in the same direction (14, 21, then 28 days);
* all available direct legs participate in first-minute currency strength;
* a selected leg must have its declared 15-minute exit candle or the complete
  matched event trio is unavailable, never imputed as profit or loss.

This remains immutable retrospective research with no broker, authorization,
promotion, or execution surface.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import oanda_spike_blurb_fomc_response_generalization_v1 as v1
from oanda_spike_blurb_event_watch_price_reacquisition_v3 import (
    MAX_WORKERS,
    fetch_job,
    resolve_readonly_oanda_client,
)
from oanda_spike_blurb_rbnz_response_confirmation_v2 import fixed_horizon_outcome
from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    DATABASE,
    canonical_json,
    immutable_insert,
    payload_row,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)
from oanda_spike_blurb_verified_event_response_replay_v2 import detect_arm


CONTRACT_ID = "spike_blurb_fomc_response_generalization_v2_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_response_generalization_v2"
)
FREEZE_UTC = "2026-08-20T18:35:00+00:00"
SCHEMA_VERSION = 1
MINIMUM_DETECTION_LEGS = 3
FALLBACK_ABSOLUTE_OFFSETS = (14, 21, 28)
MINIMUM_ANALYSIS_EVENTS = 18


def decode_candles(blob: bytes, expected_hash: str) -> dict[int, dict[str, Any]]:
    decoded = gzip.decompress(blob)
    if sha256_bytes(decoded) != expected_hash:
        raise RuntimeError("price_payload_hash_mismatch")
    body = json.loads(decoded)
    rows = {int(candle["epoch"]): candle for candle in body["candles"]}
    if len(rows) != len(body["candles"]):
        raise RuntimeError("duplicate_price_clock")
    return rows


def detection_leg_count(paths: Mapping[str, Mapping[int, Mapping[str, Any]]], event_epoch: int) -> int:
    return sum(event_epoch in candles for candles in paths.values())


def parent_input_snapshot(
    connection: sqlite3.Connection,
    clocks: list[dict[str, Any]],
) -> tuple[str, dict[str, dict[str, dict[str, Any]]], list[dict[str, Any]]]:
    parent = connection.execute(
        "SELECT contract_json,builder_sha256 FROM fomc_generalization_contracts WHERE contract_id=?",
        (v1.CONTRACT_ID,),
    ).fetchone()
    if parent is None:
        raise RuntimeError("frozen_fomc_v1_parent_contract_missing")
    clock_map = {str(row["clock_id"]): row for row in clocks}
    loaded: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    identities: list[list[str]] = []
    rows = connection.execute(
        "SELECT clock_id,instrument,coverage_state,payload_sha256,payload_gzip "
        "FROM fomc_generalization_price_windows WHERE contract_id=? ORDER BY clock_id,instrument",
        (v1.CONTRACT_ID,),
    ).fetchall()
    if len(rows) != 63 * len(v1.DIRECT_INSTRUMENTS):
        raise RuntimeError(f"fomc_v1_price_identity_count_mismatch:{len(rows)}")
    for row in rows:
        clock_id, instrument, state, payload_hash = map(str, row[:4])
        if clock_id not in clock_map:
            raise RuntimeError(f"unknown_parent_clock:{clock_id}")
        loaded[clock_id][instrument] = decode_candles(row[4], payload_hash)
        identities.append([clock_id, instrument, state, payload_hash])
    invalid: list[dict[str, Any]] = []
    for clock in clocks:
        clock_id = str(clock["clock_id"])
        count = detection_leg_count(loaded[clock_id], int(clock["clock_epoch"]))
        if int(clock["day_offset"]) == 0 and count < MINIMUM_DETECTION_LEGS:
            raise RuntimeError(f"scheduled_event_detection_coverage_unavailable:{clock_id}:{count}")
        if int(clock["day_offset"]) != 0 and count < MINIMUM_DETECTION_LEGS:
            invalid.append({
                "source_event_id": str(clock["source_event_id"]),
                "control_slot": -1 if int(clock["day_offset"]) < 0 else 1,
                "original_clock_id": clock_id,
                "original_day_offset": int(clock["day_offset"]),
                "detection_leg_count": count,
                "availability_reason": "fewer_than_three_direct_usd_event_candles",
            })
    snapshot = sha256_bytes(canonical_json({
        "parent_contract_json": str(parent[0]),
        "parent_builder_sha256": str(parent[1]),
        "price_identities": identities,
        "invalid_control_slots": invalid,
    }).encode())
    return snapshot, loaded, invalid


def build_fallback_clocks(
    events: list[dict[str, Any]],
    invalid_slots: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_id = {str(event["event_id"]): event for event in events}
    event_dates = {date.fromisoformat(str(event["event_date"])) for event in events}
    rows: list[dict[str, Any]] = []
    for invalid in invalid_slots:
        event = by_id[str(invalid["source_event_id"])]
        slot = int(invalid["control_slot"])
        source_date = date.fromisoformat(str(event["event_date"]))
        for absolute in FALLBACK_ABSOLUTE_OFFSETS:
            day_offset = slot * absolute
            clock_date = source_date + timedelta(days=day_offset)
            if clock_date in event_dates:
                continue
            local = datetime.combine(clock_date, dt_time(14, 0), tzinfo=v1.EASTERN)
            utc = local.astimezone(timezone.utc)
            rows.append({
                "clock_id": stable_id("fomc_generalization_v2_fallback", CONTRACT_ID, event["event_id"], day_offset),
                "contract_id": CONTRACT_ID,
                "source_event_id": event["event_id"],
                "control_slot": slot,
                "actual_day_offset": day_offset,
                "clock_local": local.isoformat(),
                "clock_utc": utc.isoformat(),
                "clock_epoch": int(utc.timestamp()),
                "selection_rule": "predeclared_same_weekday_availability_fallback_14_21_28_days",
                "research_only": 1,
                "execution_eligible": 0,
                "forecast_proof_eligible": 0,
            })
    expected = len(invalid_slots) * len(FALLBACK_ABSOLUTE_OFFSETS)
    if len(rows) != expected:
        raise RuntimeError(f"fallback_clock_count_mismatch:{len(rows)}:{expected}")
    return sorted(rows, key=lambda row: (str(row["source_event_id"]), abs(int(row["actual_day_offset"]))))


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_generalization_v2_contracts (
          contract_id TEXT PRIMARY KEY, parent_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          dependency_sha256_json TEXT NOT NULL, input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_v2_fallback_clocks (
          clock_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          control_slot INTEGER NOT NULL, actual_day_offset INTEGER NOT NULL,
          clock_local TEXT NOT NULL, clock_utc TEXT NOT NULL, selection_rule TEXT NOT NULL,
          clock_json TEXT NOT NULL, row_sha256 TEXT NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,source_event_id,actual_day_offset)
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_v2_fallback_prices (
          window_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          instrument TEXT NOT NULL, requested_start_utc TEXT NOT NULL,
          requested_end_utc TEXT NOT NULL, coverage_state TEXT NOT NULL,
          candle_count INTEGER NOT NULL, first_utc TEXT, last_utc TEXT,
          average_spread_pips REAL, maximum_spread_pips REAL, http_status INTEGER NOT NULL,
          latency_ms INTEGER NOT NULL, error_text TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
          payload_gzip BLOB NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,clock_id,instrument)
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_v2_resolutions (
          resolution_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          control_slot INTEGER NOT NULL, selected_clock_id TEXT NOT NULL,
          actual_day_offset INTEGER NOT NULL, detection_leg_count INTEGER NOT NULL,
          used_fallback INTEGER NOT NULL, resolution_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL, UNIQUE(contract_id,source_event_id,control_slot)
        );
        CREATE TABLE IF NOT EXISTS fomc_generalization_v2_decisions (
          decision_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          control_slot INTEGER NOT NULL, selected_clock_id TEXT NOT NULL,
          actual_day_offset INTEGER NOT NULL, detected INTEGER NOT NULL, detection_utc TEXT,
          currency_direction TEXT, currency_strength_bps REAL, breadth REAL,
          selected_instrument TEXT, selected_pair_direction TEXT, entry_spread_pips REAL,
          net_after_cost_pips REAL, mfe_pips REAL, mae_pips REAL, cost_cleared INTEGER,
          path_expected_bars INTEGER NOT NULL, path_observed_bars INTEGER NOT NULL,
          path_complete INTEGER NOT NULL, path_quality TEXT NOT NULL,
          analysis_eligible INTEGER NOT NULL, decision_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,source_event_id,control_slot)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_contract_no_update BEFORE UPDATE ON fomc_generalization_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_contract_no_delete BEFORE DELETE ON fomc_generalization_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_clock_no_update BEFORE UPDATE ON fomc_generalization_v2_fallback_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_clock_no_delete BEFORE DELETE ON fomc_generalization_v2_fallback_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_price_no_update BEFORE UPDATE ON fomc_generalization_v2_fallback_prices BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_price_no_delete BEFORE DELETE ON fomc_generalization_v2_fallback_prices BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_resolution_no_update BEFORE UPDATE ON fomc_generalization_v2_resolutions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_resolution_no_delete BEFORE DELETE ON fomc_generalization_v2_resolutions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_decision_no_update BEFORE UPDATE ON fomc_generalization_v2_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_generalization_v2_decision_no_delete BEFORE DELETE ON fomc_generalization_v2_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(
    connection: sqlite3.Connection,
    snapshot: str,
    invalid_slots: list[dict[str, Any]],
    fallback_clocks: list[dict[str, Any]],
) -> dict[str, Any]:
    dependencies = {
        "parent_v1": sha256_file(Path(v1.__file__).resolve()),
        "gap_tolerant_outcome": sha256_file(
            Path(__file__).resolve().parent / "oanda_spike_blurb_rbnz_response_confirmation_v2.py"
        ),
    }
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "parent_contract_id": v1.CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "dependency_sha256": dependencies,
        "input_snapshot_sha256": snapshot,
        "candidate_identity_preserved": {
            "arm_id": v1.LOCKED_ARM_ID, "arm": v1.LOCKED_ARM,
            "hold_minutes": v1.HOLD_MINUTES,
            "direct_instruments": list(v1.DIRECT_INSTRUMENTS),
        },
        "availability_policy": {
            "minimum_detection_legs": MINIMUM_DETECTION_LEGS,
            "initial_controls": [-7, 7],
            "fallback_absolute_offsets": list(FALLBACK_ABSOLUTE_OFFSETS),
            "fallback_selection": "first_same_direction_same_weekday_with_minimum_detection_legs",
            "availability_uses_price_presence_not_return_or_direction": True,
            "all_available_direct_usd_legs_participate": True,
            "selected_leg_declared_exit_required": True,
            "unavailable_selected_exit_excludes_complete_matched_trio": True,
            "minimum_complete_event_trios": MINIMUM_ANALYSIS_EVENTS,
        },
        "invalid_parent_control_slots": invalid_slots,
        "fallback_clock_count": len(fallback_clocks),
        "freeze_state": "frozen_after_v1_coverage_failure_before_fallback_prices_or_v2_outcomes",
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM fomc_generalization_v2_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_fomc_v2_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO fomc_generalization_v2_contracts VALUES (?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, v1.CONTRACT_ID, encoded, contract["builder_sha256"],
                canonical_json(dependencies), snapshot, FREEZE_UTC,
            ),
        )
    for clock in fallback_clocks:
        stored = {key: value for key, value in clock.items() if key != "clock_epoch"}
        immutable_insert(
            connection, "fomc_generalization_v2_fallback_clocks",
            payload_row(stored, "clock_json"), "clock_id",
        )
    connection.commit()
    return contract


def fallback_jobs(clocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for clock in clocks:
        event_time = datetime.fromisoformat(str(clock["clock_utc"]))
        start = event_time - timedelta(minutes=v1.PRE_CLOCK_MINUTES)
        end = event_time + timedelta(minutes=v1.POST_CLOCK_MINUTES)
        for instrument in v1.DIRECT_INSTRUMENTS:
            jobs.append({
                "case_id": clock["clock_id"], "clock_id": clock["clock_id"],
                "instrument": instrument, "start_epoch": int(start.timestamp()),
                "end_epoch": int(end.timestamp()), "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
            })
    return jobs


def store_fallback_price(connection: sqlite3.Connection, result: Mapping[str, Any]) -> None:
    payload = {
        "window_id": stable_id("fomc_generalization_v2_price", CONTRACT_ID, result["clock_id"], result["instrument"]),
        "contract_id": CONTRACT_ID, "clock_id": result["clock_id"],
        "instrument": result["instrument"], "requested_start_utc": result["start_utc"],
        "requested_end_utc": result["end_utc"], "coverage_state": result["coverage_state"],
        "candle_count": int(result["candle_count"]), "first_utc": result["first_utc"],
        "last_utc": result["last_utc"], "average_spread_pips": result["average_spread_pips"],
        "maximum_spread_pips": result["maximum_spread_pips"], "http_status": int(result["http_status"]),
        "latency_ms": int(result["latency_ms"]), "error_text": result["error_text"],
        "payload_sha256": result["payload_sha256"], "payload_gzip": result["payload_gzip"],
        "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
    }
    columns = list(payload)
    connection.execute(
        f"INSERT OR IGNORE INTO fomc_generalization_v2_fallback_prices ({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})", [payload[column] for column in columns],
    )
    existing = connection.execute(
        "SELECT payload_sha256 FROM fomc_generalization_v2_fallback_prices WHERE window_id=?",
        (payload["window_id"],),
    ).fetchone()
    if existing is None or str(existing[0]) != str(payload["payload_sha256"]):
        raise RuntimeError(f"immutable_fomc_v2_price_conflict:{payload['window_id']}")


def load_fallback_paths(
    connection: sqlite3.Connection,
    fallback_clocks: list[dict[str, Any]],
) -> dict[str, dict[str, dict[int, dict[str, Any]]]]:
    output: dict[str, dict[str, dict[int, dict[str, Any]]]] = defaultdict(dict)
    rows = connection.execute(
        "SELECT clock_id,instrument,payload_sha256,payload_gzip FROM fomc_generalization_v2_fallback_prices "
        "WHERE contract_id=? ORDER BY clock_id,instrument", (CONTRACT_ID,),
    ).fetchall()
    for row in rows:
        output[str(row[0])][str(row[1])] = decode_candles(row[3], str(row[2]))
    expected = set(v1.DIRECT_INSTRUMENTS)
    for clock in fallback_clocks:
        if set(output[str(clock["clock_id"])]) != expected:
            raise RuntimeError(f"fallback_direct_universe_incomplete:{clock['clock_id']}")
    return output


def resolve_clocks(
    events: list[dict[str, Any]],
    parent_clocks: list[dict[str, Any]],
    parent_paths: Mapping[str, Mapping[str, Mapping[int, Mapping[str, Any]]]],
    fallback_clocks: list[dict[str, Any]],
    fallback_paths: Mapping[str, Mapping[str, Mapping[int, Mapping[str, Any]]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    original: dict[tuple[str, int], dict[str, Any]] = {}
    for clock in parent_clocks:
        slot = 0 if int(clock["day_offset"]) == 0 else (-1 if int(clock["day_offset"]) < 0 else 1)
        original[(str(clock["source_event_id"]), slot)] = clock
    fallbacks: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for clock in fallback_clocks:
        fallbacks[(str(clock["source_event_id"]), int(clock["control_slot"]))].append(clock)
    resolutions: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    for event in events:
        event_key = str(event["event_id"])
        for slot in (-1, 0, 1):
            first = original[(event_key, slot)]
            candidates = [first, *sorted(fallbacks[(event_key, slot)], key=lambda row: abs(int(row["actual_day_offset"])))]
            chosen = None
            count = 0
            chosen_paths: Mapping[str, Mapping[int, Mapping[str, Any]]] | None = None
            for candidate in candidates:
                clock_id = str(candidate["clock_id"])
                paths = parent_paths[clock_id] if clock_id in parent_paths else fallback_paths[clock_id]
                candidate_epoch = int(candidate["clock_epoch"])
                candidate_count = detection_leg_count(paths, candidate_epoch)
                if candidate_count >= MINIMUM_DETECTION_LEGS:
                    chosen, count, chosen_paths = candidate, candidate_count, paths
                    break
            if chosen is None or chosen_paths is None:
                raise RuntimeError(f"no_usable_fomc_clock:{event_key}:{slot}")
            actual_offset = int(chosen.get("actual_day_offset", chosen.get("day_offset", 0)))
            resolution = {
                "resolution_id": stable_id("fomc_v2_resolution", CONTRACT_ID, event_key, slot),
                "contract_id": CONTRACT_ID, "source_event_id": event_key,
                "control_slot": slot, "selected_clock_id": chosen["clock_id"],
                "actual_day_offset": actual_offset, "detection_leg_count": count,
                "used_fallback": int(str(chosen["clock_id"]) != str(first["clock_id"])),
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            }
            resolutions.append(resolution)
            selected.append({
                **chosen, "source_event_id": event_key, "control_slot": slot,
                "actual_day_offset": actual_offset, "event_epoch": int(chosen["clock_epoch"]),
                "event_currency": v1.EVENT_CURRENCY, "paths": chosen_paths,
            })
    return selected, resolutions


def evaluate_clock(clock: Mapping[str, Any]) -> dict[str, Any]:
    detected = detect_arm(clock, v1.LOCKED_ARM_ID, v1.LOCKED_ARM)
    base = {
        "decision_id": stable_id("fomc_v2_decision", CONTRACT_ID, clock["source_event_id"], clock["control_slot"]),
        "contract_id": CONTRACT_ID, "source_event_id": clock["source_event_id"],
        "control_slot": int(clock["control_slot"]), "selected_clock_id": clock["clock_id"],
        "actual_day_offset": int(clock["actual_day_offset"]),
        "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
    }
    if detected is None:
        return {
            **base, "detected": 0, "detection_utc": None, "currency_direction": None,
            "currency_strength_bps": None, "breadth": None, "selected_instrument": None,
            "selected_pair_direction": None, "entry_spread_pips": None,
            "net_after_cost_pips": 0.0, "mfe_pips": None, "mae_pips": None,
            "cost_cleared": 0, "path_expected_bars": v1.HOLD_MINUTES,
            "path_observed_bars": 0, "path_complete": 0,
            "path_quality": "no_trade_no_detection", "analysis_eligible": 1,
        }
    instrument = str(detected["selected_instrument"])
    try:
        result = fixed_horizon_outcome(detected, clock["paths"][instrument])
        eligible = 1
    except RuntimeError as exc:
        if str(exc) != "declared_exit_candle_missing":
            raise
        result = {
            "net_after_cost_pips": None, "mfe_pips": None, "mae_pips": None,
            "cost_cleared": None, "path_expected_bars": v1.HOLD_MINUTES,
            "path_observed_bars": 0, "path_complete": 0,
            "path_quality": "selected_leg_declared_exit_missing",
        }
        eligible = 0
    return {
        **base, "detected": 1,
        "detection_utc": datetime.fromtimestamp(int(detected["detection_epoch"]), timezone.utc).isoformat(),
        "currency_direction": detected["currency_direction"],
        "currency_strength_bps": float(detected["currency_strength_bps"]),
        "breadth": float(detected["breadth"]), "selected_instrument": instrument,
        "selected_pair_direction": detected["selected_pair_direction"],
        "entry_spread_pips": float(detected["entry_spread_pips"]),
        **result, "analysis_eligible": eligible,
    }


def store_outputs(
    connection: sqlite3.Connection,
    selected: list[dict[str, Any]],
    resolutions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    for resolution in resolutions:
        immutable_insert(
            connection, "fomc_generalization_v2_resolutions",
            payload_row(resolution, "resolution_json"), "resolution_id",
        )
    decisions: list[dict[str, Any]] = []
    for clock in selected:
        row = payload_row(evaluate_clock(clock), "decision_json")
        immutable_insert(connection, "fomc_generalization_v2_decisions", row, "decision_id")
        decisions.append(row)
    connection.commit()
    return decisions


def analyze(events: list[dict[str, Any]], decisions: list[dict[str, Any]]) -> dict[str, Any]:
    by_event: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in decisions:
        by_event[str(row["source_event_id"])][int(row["control_slot"])] = row
    usable_events: list[dict[str, Any]] = []
    excluded_events: list[dict[str, Any]] = []
    triples: list[tuple[float, float, float]] = []
    event_rows: list[dict[str, Any]] = []
    for event in events:
        event_key = str(event["event_id"])
        trio = by_event[event_key]
        if set(trio) != {-1, 0, 1}:
            raise RuntimeError(f"fomc_v2_exact_trio_required:{event_key}")
        if any(int(trio[slot]["analysis_eligible"]) == 0 for slot in (-1, 0, 1)):
            excluded_events.append({"event_id": event_key, "reason": "selected_leg_declared_exit_missing"})
            continue
        values = tuple(float(trio[slot]["net_after_cost_pips"]) for slot in (0, -1, 1))
        triples.append(values)  # type: ignore[arg-type]
        usable_events.append(event)
        event_rows.append({
            "event_id": event_key, "event_date": event["event_date"],
            "decision_action": event["decision_action"],
            "rate_change_percentage_points": event["official_target_change_percentage_points"],
            "treatment_detected": int(trio[0]["detected"]),
            "treatment_direction": trio[0]["currency_direction"],
            "treatment_instrument": trio[0]["selected_instrument"],
            "treatment_net_pips": values[0], "control_minus_net_pips": values[1],
            "control_plus_net_pips": values[2],
            "incremental_net_pips": values[0] - (values[1] + values[2]) / 2.0,
            "minus_actual_day_offset": int(trio[-1]["actual_day_offset"]),
            "plus_actual_day_offset": int(trio[1]["actual_day_offset"]),
        })
    if len(usable_events) < MINIMUM_ANALYSIS_EVENTS:
        raise RuntimeError(f"fomc_v2_insufficient_complete_event_trios:{len(usable_events)}")
    treatment = [row[0] for row in triples]
    controls = [value for row in triples for value in row[1:]]
    increments = [row[0] - (row[1] + row[2]) / 2.0 for row in triples]
    leave_one_out = [
        statistics.mean(value for index, value in enumerate(treatment) if index != omitted)
        for omitted in range(len(treatment))
    ]
    clearance = statistics.mean(int(by_event[str(event["event_id"])][0]["cost_cleared"]) for event in usable_events)
    randomization = v1.monte_carlo_within_trio_pvalue(triples)
    treatment_average = statistics.mean(treatment)
    control_average = statistics.mean(controls)
    incremental_average = statistics.mean(increments)
    criteria = {
        "positive_treatment_average": treatment_average > 0.0,
        "positive_incremental_average": incremental_average > 0.0,
        "cost_clearance_at_least_two_thirds": clearance >= 2.0 / 3.0,
        "positive_minimum_leave_one_out_treatment_average": min(leave_one_out) > 0.0,
        "randomization_pvalue_at_most_0_05": float(randomization["pvalue"]) <= 0.05,
    }
    return {
        "scheduled_event_count": len(events), "analysis_event_count": len(usable_events),
        "excluded_event_count": len(excluded_events), "matched_control_count": len(usable_events) * 2,
        "treatment_average_net_pips": treatment_average,
        "control_average_net_pips": control_average,
        "incremental_average_net_pips": incremental_average,
        "incremental_median_net_pips": statistics.median(increments),
        "incremental_positive_event_rate": statistics.mean(value > 0 for value in increments),
        "treatment_cost_clearance_rate": clearance,
        "minimum_leave_one_event_out_treatment_average_pips": min(leave_one_out),
        "within_trio_randomization": randomization,
        "diagnostic_acceptance_criteria": criteria,
        "diagnostic_generalization_passed": all(criteria.values()),
        "excluded_events": excluded_events, "event_rows": event_rows,
    }


def write_report(result: Mapping[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "FOMC_RESPONSE_GENERALIZATION_V2.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    analysis = result["analysis"]
    lines = [
        "# Availability-repaired FOMC response generalization V2", "",
        "- V1 candidate, official events, price observations, response direction, cost gate, and 15-minute hold are unchanged.",
        "- V2 changes only the rule for sparse direct legs and a closed-market matched control.",
        "- Availability selection uses candle presence only; it never uses return or direction.",
        "- This is retrospective cross-authority evidence, not prospective proof.",
        "- Execution decision: **no_trade**.", "", "## Result", "",
        f"- Scheduled events: **{analysis['scheduled_event_count']}**; usable complete trios: **{analysis['analysis_event_count']}**.",
        f"- Treatment average: **{analysis['treatment_average_net_pips']:.3f} pips**.",
        f"- Control average: **{analysis['control_average_net_pips']:.3f} pips**.",
        f"- Incremental average: **{analysis['incremental_average_net_pips']:.3f} pips**.",
        f"- Treatment cost-clearance: **{analysis['treatment_cost_clearance_rate']:.1%}**.",
        f"- Minimum leave-one-event-out treatment average: **{analysis['minimum_leave_one_event_out_treatment_average_pips']:.3f} pips**.",
        f"- Randomization p: **{analysis['within_trio_randomization']['pvalue']:.6f}**.",
        f"- Predeclared diagnostic generalization passed: **{analysis['diagnostic_generalization_passed']}**.",
        f"- Fallback controls used: **{result['fallback_resolution_count']}**.",
        "", "## Event rows", "",
        "| Date | Action | USD response | Pair | Treatment | Before control | After control | Incremental |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for row in analysis["event_rows"]:
        lines.append(
            f"| {row['event_date']} | {row['decision_action']} | {row['treatment_direction'] or 'no_trade'} | "
            f"{row['treatment_instrument'] or '-'} | {row['treatment_net_pips']:.3f} | "
            f"{row['control_minus_net_pips']:.3f} | {row['control_plus_net_pips']:.3f} | "
            f"{row['incremental_net_pips']:.3f} |"
        )
    lines.append("")
    (report_root / "FOMC_RESPONSE_GENERALIZATION_V2.md").write_text("\n".join(lines), encoding="utf-8")


def run(
    database: Path = DATABASE,
    report_root: Path = REPORT_ROOT,
    workers: int = MAX_WORKERS,
    *,
    freeze_only: bool = False,
) -> dict[str, Any]:
    events = v1.validate_schedule()
    parent_clocks = v1.build_clocks(events)
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    snapshot, parent_paths, invalid_slots = parent_input_snapshot(connection, parent_clocks)
    fallback_clocks = build_fallback_clocks(events, invalid_slots)
    contract = freeze_contract(connection, snapshot, invalid_slots, fallback_clocks)
    if freeze_only:
        result = {
            "contract_id": CONTRACT_ID, "parent_contract_id": v1.CONTRACT_ID,
            "builder_sha256": contract["builder_sha256"], "input_snapshot_sha256": snapshot,
            "invalid_parent_control_slot_count": len(invalid_slots),
            "fallback_clock_count": len(fallback_clocks), "research_only": True,
            "execution_eligible": False, "forecast_proof_eligible": False,
            "supported_execution_decision": "no_trade",
        }
        connection.close()
        return result
    jobs = fallback_jobs(fallback_clocks)
    existing = {
        (str(row[0]), str(row[1])) for row in connection.execute(
            "SELECT clock_id,instrument FROM fomc_generalization_v2_fallback_prices WHERE contract_id=?",
            (CONTRACT_ID,),
        )
    }
    pending = [job for job in jobs if (str(job["clock_id"]), str(job["instrument"])) not in existing]
    if pending:
        client, metadata = resolve_readonly_oanda_client()
        if metadata.get("environment") != "practice" or "api-fxpractice.oanda.com" not in str(metadata.get("base_url", "")):
            raise RuntimeError("practice_readonly_endpoint_required")
        with ThreadPoolExecutor(max_workers=max(1, min(int(workers), MAX_WORKERS))) as executor:
            futures = {executor.submit(fetch_job, client, job): job for job in pending}
            for future in as_completed(futures):
                store_fallback_price(connection, future.result())
                connection.commit()
    fallback_paths = load_fallback_paths(connection, fallback_clocks)
    selected, resolutions = resolve_clocks(
        events, parent_clocks, parent_paths, fallback_clocks, fallback_paths,
    )
    decisions = store_outputs(connection, selected, resolutions)
    analysis = analyze(events, decisions)
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = sum(
        int(connection.execute(
            f"SELECT coalesce(sum(execution_eligible),0) FROM {table} WHERE contract_id=?",
            (CONTRACT_ID,),
        ).fetchone()[0])
        for table in (
            "fomc_generalization_v2_fallback_clocks", "fomc_generalization_v2_fallback_prices",
            "fomc_generalization_v2_resolutions", "fomc_generalization_v2_decisions",
        )
    )
    fallback_used = sum(int(row["used_fallback"]) for row in resolutions)
    stable = {
        "contract_id": CONTRACT_ID, "parent_contract_id": v1.CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"], "input_snapshot_sha256": snapshot,
        "invalid_parent_control_slot_count": len(invalid_slots),
        "fallback_clock_count": len(fallback_clocks),
        "fallback_price_window_count": len(jobs), "fallback_resolution_count": fallback_used,
        "resolution_count": len(resolutions), "decision_count": len(decisions),
        "analysis": analysis, "sqlite_integrity": integrity,
        "execution_eligible_count": execution_sum,
        "proof_state": "availability_repaired_cross_authority_retrospective_generalization_not_prospective_proof",
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    result = {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(),
        "fetched_this_run": len(pending), **stable,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
    }
    connection.close()
    if len(resolutions) != 63 or len(decisions) != 63 or integrity != "ok" or execution_sum != 0:
        raise RuntimeError("fomc_v2_integrity_or_safety_failure")
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
