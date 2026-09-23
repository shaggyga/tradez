#!/usr/bin/env python3
"""Replay response-detected entries for the verified event-watch cases.

Source facts open a currency watch but never provide a retrospective direction.
Direction is derived only from the post-event cross-pair response.  Entries and
outcomes use executable bid/ask paths plus modeled slippage.  The entire replay
is retrospective, research-only, and execution-ineligible.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import sqlite3
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from oanda_spike_blurb_event_watch_price_reacquisition_v3 import (
    CONTRACT_ID as PRICE_CONTRACT_ID,
    DATABASE,
    canonical_json,
    pip_size,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)
from oanda_spike_blurb_verified_source_cases_v3 import CONTRACT_ID as SOURCE_CASE_CONTRACT_ID, parse_utc


CONTRACT_ID = "spike_blurb_verified_event_response_replay_v2_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_event_response_replay_v2"
)
FREEZE_UTC = "2026-08-20T17:10:00+00:00"
SCHEMA_VERSION = 1
MINIMUM_LEGS = 3
MINIMUM_BREADTH = 0.60
MAXIMUM_ENTRY_SPREAD_PIPS = 5.0
MODELED_SLIPPAGE_PIPS = 0.25
OUTCOME_HORIZONS_MIN = (5, 15, 30, 60)
ARMS = {
    "response_1m_breadth": {
        "start_min": 1, "end_min": 15, "minimum_strength_bps": 1.0,
        "persistence_min": 1, "technical_confirmation": False,
    },
    "response_3m_persistent": {
        "start_min": 3, "end_min": 30, "minimum_strength_bps": 2.0,
        "persistence_min": 3, "technical_confirmation": False,
    },
    "source_plus_strength_technical_confirmation": {
        "start_min": 10, "end_min": 45, "minimum_strength_bps": 2.0,
        "persistence_min": 3, "technical_confirmation": True,
    },
    "delayed_reconfirmation": {
        "start_min": 5, "end_min": 60, "minimum_strength_bps": 2.5,
        "persistence_min": 5, "technical_confirmation": False,
    },
}


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS verified_event_response_contracts (
          contract_id TEXT PRIMARY KEY,
          source_case_contract_id TEXT NOT NULL,
          price_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verified_event_response_detections (
          detection_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          event_currency TEXT NOT NULL,
          arm_id TEXT NOT NULL,
          event_clock_utc TEXT NOT NULL,
          detection_utc TEXT NOT NULL,
          elapsed_minutes INTEGER NOT NULL,
          currency_direction TEXT NOT NULL,
          currency_strength_bps REAL NOT NULL,
          breadth REAL NOT NULL,
          leg_count INTEGER NOT NULL,
          agreeing_leg_count INTEGER NOT NULL,
          selected_instrument TEXT NOT NULL,
          selected_pair_direction TEXT NOT NULL,
          selected_pair_return_bps REAL NOT NULL,
          entry_bid REAL NOT NULL,
          entry_ask REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,
          entry_spread_bps REAL NOT NULL,
          strength_fast_mean_bps REAL,
          strength_slow_mean_bps REAL,
          technical_aligned INTEGER NOT NULL,
          source_direction_assigned INTEGER NOT NULL,
          response_direction_used INTEGER NOT NULL,
          evidence_unit_id TEXT NOT NULL,
          detection_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,case_id,arm_id)
        );
        CREATE TABLE IF NOT EXISTS verified_event_response_outcomes (
          outcome_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          detection_id TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL,
          exit_utc TEXT NOT NULL,
          midpoint_move_pips REAL NOT NULL,
          executable_net_before_slippage_pips REAL NOT NULL,
          modeled_slippage_pips REAL NOT NULL,
          net_after_cost_pips REAL NOT NULL,
          mfe_pips REAL NOT NULL,
          mae_pips REAL NOT NULL,
          missed_entry_slippage_1m_pips REAL,
          delayed_1m_net_after_cost_pips REAL,
          cost_cleared INTEGER NOT NULL,
          outcome_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          outcome_selected INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,detection_id,horizon_minutes)
        );
        CREATE INDEX IF NOT EXISTS verified_event_response_outcomes_cell
          ON verified_event_response_outcomes(contract_id,horizon_minutes,detection_id);
        CREATE TRIGGER IF NOT EXISTS verified_event_response_contracts_no_update
          BEFORE UPDATE ON verified_event_response_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_contracts_no_delete
          BEFORE DELETE ON verified_event_response_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_detections_no_update
          BEFORE UPDATE ON verified_event_response_detections BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_detections_no_delete
          BEFORE DELETE ON verified_event_response_detections BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_outcomes_no_update
          BEFORE UPDATE ON verified_event_response_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_outcomes_no_delete
          BEFORE DELETE ON verified_event_response_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def epoch(value: str) -> int:
    parsed = parse_utc(value, "timestamp")
    assert parsed is not None
    return int(parsed.timestamp())


def iso_utc(value: int) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def load_inputs(connection: sqlite3.Connection) -> tuple[dict[str, dict[str, Any]], str]:
    cases: dict[str, dict[str, Any]] = {}
    digest = hashlib.sha256()
    for raw in connection.execute(
        """
        SELECT case_id,event_currency,response_watch_eligible_from_utc,direction_policy,
               consensus_state,rate_repricing_state,row_sha256
        FROM verified_external_source_cases
        WHERE contract_id=? AND response_watch_eligible=1
        ORDER BY case_id
        """, (SOURCE_CASE_CONTRACT_ID,)
    ):
        row = dict(raw)
        row["event_epoch"] = epoch(str(row["response_watch_eligible_from_utc"]))
        row["paths"] = {}
        cases[str(row["case_id"])] = row
        digest.update(canonical_json({k: v for k, v in row.items() if k not in {"paths"}}).encode())
    if len(cases) != 2:
        raise RuntimeError(f"unexpected_watch_case_count:{len(cases)}")
    seen_instruments: dict[str, set[str]] = defaultdict(set)
    for raw in connection.execute(
        """
        SELECT case_id,instrument,coverage_state,payload_sha256,payload_gzip
        FROM verified_event_price_windows
        WHERE contract_id=? ORDER BY case_id,instrument
        """, (PRICE_CONTRACT_ID,)
    ):
        case_id = str(raw[0])
        instrument = str(raw[1])
        coverage_state = str(raw[2])
        if case_id not in cases:
            raise RuntimeError(f"price_window_unknown_case:{case_id}")
        seen_instruments[case_id].add(instrument)
        digest.update(canonical_json({
            "case_id": case_id,
            "instrument": instrument,
            "coverage_state": coverage_state,
            "sha256": raw[3],
        }).encode())
        if coverage_state != "exact_window":
            cases[case_id].setdefault("excluded_price_windows", []).append({
                "instrument": instrument,
                "coverage_state": coverage_state,
                "payload_sha256": str(raw[3]),
            })
            continue
        decoded = gzip.decompress(raw[4])
        if sha256_bytes(decoded) != raw[3]:
            raise RuntimeError(f"price_payload_hash_mismatch:{case_id}:{instrument}")
        payload = json.loads(decoded)
        if payload["case_id"] != case_id or payload["instrument"] != instrument:
            raise RuntimeError(f"price_payload_identity_mismatch:{case_id}:{instrument}")
        candle_map = {int(candle["epoch"]): candle for candle in payload["candles"]}
        if len(candle_map) != len(payload["candles"]):
            raise RuntimeError(f"duplicate_price_clock:{case_id}:{instrument}")
        cases[case_id]["paths"][instrument] = candle_map
    for case_id, case in cases.items():
        if len(seen_instruments[case_id]) != 68:
            raise RuntimeError(f"all_68_price_window_records_required:{case_id}")
        event_currency = str(case["event_currency"])
        expected_direct = {
            instrument for instrument in seen_instruments[case_id]
            if event_currency in instrument.split("_")
        }
        exact_direct = {
            instrument for instrument in case["paths"]
            if event_currency in instrument.split("_")
        }
        if not expected_direct or exact_direct != expected_direct:
            raise RuntimeError(f"event_currency_direct_price_path_missing:{case_id}")
        if len(case["paths"]) < 60:
            raise RuntimeError(f"insufficient_exact_cross_pair_paths:{case_id}:{len(case['paths'])}")
    return cases, digest.hexdigest()


def currency_snapshot(
    paths: Mapping[str, Mapping[int, Mapping[str, Any]]],
    currency: str,
    event_epoch: int,
    elapsed_minutes: int,
) -> dict[str, Any] | None:
    target_candle_epoch = event_epoch + (elapsed_minutes - 1) * 60
    legs: list[dict[str, Any]] = []
    for instrument, candles in paths.items():
        base, quote = instrument.split("_")
        if currency not in {base, quote}:
            continue
        baseline = candles.get(event_epoch)
        target = candles.get(target_candle_epoch)
        if baseline is None or target is None:
            continue
        baseline_mid = float(baseline["mid_o"])
        target_mid = float(target["mid_c"])
        if baseline_mid <= 0 or target_mid <= 0:
            continue
        pair_return_bps = (target_mid / baseline_mid - 1.0) * 10_000.0
        oriented_bps = pair_return_bps if base == currency else -pair_return_bps
        pip = pip_size(instrument)
        spread_pips = (float(target["ask_c"]) - float(target["bid_c"])) / pip
        spread_bps = (float(target["ask_c"]) - float(target["bid_c"])) / target_mid * 10_000.0
        legs.append({
            "instrument": instrument,
            "base": base,
            "quote": quote,
            "oriented_return_bps": oriented_bps,
            "spread_pips": spread_pips,
            "spread_bps": spread_bps,
            "target": target,
            "target_candle_epoch": target_candle_epoch,
        })
    if len(legs) < MINIMUM_LEGS:
        return None
    strength = float(statistics.median(leg["oriented_return_bps"] for leg in legs))
    if strength == 0:
        return None
    sign = 1 if strength > 0 else -1
    agreeing = [leg for leg in legs if leg["oriented_return_bps"] * sign > 0]
    breadth = len(agreeing) / len(legs)
    liquid = [
        leg for leg in agreeing
        if leg["spread_pips"] <= MAXIMUM_ENTRY_SPREAD_PIPS
        and abs(leg["oriented_return_bps"]) >= abs(strength)
    ]
    if not liquid:
        return None
    selected = min(
        liquid,
        key=lambda leg: (leg["spread_bps"], -abs(leg["oriented_return_bps"]), leg["instrument"]),
    )
    if sign > 0:
        pair_direction = "long" if selected["base"] == currency else "short"
    else:
        pair_direction = "short" if selected["base"] == currency else "long"
    return {
        "elapsed_minutes": elapsed_minutes,
        "detection_epoch": event_epoch + elapsed_minutes * 60,
        "currency_direction": "stronger" if sign > 0 else "weaker",
        "direction_sign": sign,
        "currency_strength_bps": strength,
        "breadth": breadth,
        "leg_count": len(legs),
        "agreeing_leg_count": len(agreeing),
        "selected_instrument": selected["instrument"],
        "selected_pair_direction": pair_direction,
        "selected_pair_return_bps": float(selected["oriented_return_bps"]),
        "entry_bid": float(selected["target"]["bid_c"]),
        "entry_ask": float(selected["target"]["ask_c"]),
        "entry_mid": float(selected["target"]["mid_c"]),
        "entry_spread_pips": float(selected["spread_pips"]),
        "entry_spread_bps": float(selected["spread_bps"]),
    }


def technical_state(history: list[dict[str, Any]]) -> tuple[bool, float | None, float | None]:
    if len(history) < 10:
        return False, None, None
    values = [float(row["currency_strength_bps"]) for row in history]
    fast = statistics.mean(values[-3:])
    slow = statistics.mean(values[-10:])
    sign = int(history[-1]["direction_sign"])
    aligned = sign * fast > sign * slow and sign * values[-1] > 0
    return aligned, float(fast), float(slow)


def detect_arm(case: Mapping[str, Any], arm_id: str, arm: Mapping[str, Any]) -> dict[str, Any] | None:
    history: list[dict[str, Any]] = []
    snapshots: dict[int, dict[str, Any]] = {}
    for elapsed in range(1, int(arm["end_min"]) + 1):
        snapshot = currency_snapshot(case["paths"], case["event_currency"], case["event_epoch"], elapsed)
        if snapshot is None:
            continue
        snapshots[elapsed] = snapshot
        history.append(snapshot)
        if elapsed < int(arm["start_min"]):
            continue
        sign = int(snapshot["direction_sign"])
        persistence = int(arm["persistence_min"])
        recent = [snapshots.get(minute) for minute in range(elapsed - persistence + 1, elapsed + 1)]
        if any(value is None for value in recent):
            continue
        if any(int(value["direction_sign"]) != sign for value in recent if value is not None):
            continue
        if min(float(value["breadth"]) for value in recent if value is not None) < MINIMUM_BREADTH:
            continue
        if float(snapshot["breadth"]) < MINIMUM_BREADTH:
            continue
        if abs(float(snapshot["currency_strength_bps"])) < float(arm["minimum_strength_bps"]):
            continue
        aligned, fast, slow = technical_state(history)
        if bool(arm["technical_confirmation"]) and not aligned:
            continue
        return {
            **snapshot,
            "arm_id": arm_id,
            "strength_fast_mean_bps": fast,
            "strength_slow_mean_bps": slow,
            "technical_aligned": aligned,
        }
    return None


def outcome(
    detection: Mapping[str, Any],
    candles: Mapping[int, Mapping[str, Any]],
    horizon_minutes: int,
) -> dict[str, Any] | None:
    detection_epoch = int(detection["detection_epoch"])
    exit_candle_epoch = detection_epoch + (horizon_minutes - 1) * 60
    exit_candle = candles.get(exit_candle_epoch)
    if exit_candle is None:
        return None
    future = [
        candles.get(detection_epoch + minute * 60)
        for minute in range(horizon_minutes)
    ]
    if any(row is None for row in future):
        return None
    rows = [row for row in future if row is not None]
    pip = pip_size(str(detection["selected_instrument"]))
    direction = str(detection["selected_pair_direction"])
    entry_bid = float(detection["entry_bid"])
    entry_ask = float(detection["entry_ask"])
    entry_mid = (entry_bid + entry_ask) / 2.0
    exit_mid = float(exit_candle["mid_c"])
    if direction == "long":
        midpoint = (exit_mid - entry_mid) / pip
        before_slippage = (float(exit_candle["bid_c"]) - entry_ask) / pip
        mfe = (max(float(row["bid_h"]) for row in rows) - entry_ask) / pip
        mae = (min(float(row["bid_l"]) for row in rows) - entry_ask) / pip
    else:
        midpoint = (entry_mid - exit_mid) / pip
        before_slippage = (entry_bid - float(exit_candle["ask_c"])) / pip
        mfe = (entry_bid - min(float(row["ask_l"]) for row in rows)) / pip
        mae = (entry_bid - max(float(row["ask_h"]) for row in rows)) / pip
    delayed = candles.get(detection_epoch)
    missed_slippage = None
    delayed_net = None
    if delayed is not None:
        if direction == "long":
            missed_slippage = (float(delayed["ask_o"]) - entry_ask) / pip
            delayed_before = (float(exit_candle["bid_c"]) - float(delayed["ask_o"])) / pip
        else:
            missed_slippage = (entry_bid - float(delayed["bid_o"])) / pip
            delayed_before = (float(delayed["bid_o"]) - float(exit_candle["ask_c"])) / pip
        delayed_net = delayed_before - MODELED_SLIPPAGE_PIPS
    net = before_slippage - MODELED_SLIPPAGE_PIPS
    return {
        "horizon_minutes": horizon_minutes,
        "exit_utc": iso_utc(detection_epoch + horizon_minutes * 60),
        "midpoint_move_pips": midpoint,
        "executable_net_before_slippage_pips": before_slippage,
        "modeled_slippage_pips": MODELED_SLIPPAGE_PIPS,
        "net_after_cost_pips": net,
        "mfe_pips": mfe,
        "mae_pips": mae,
        "missed_entry_slippage_1m_pips": missed_slippage,
        "delayed_1m_net_after_cost_pips": delayed_net,
        "cost_cleared": int(net > 0),
    }


DETECTION_COLUMNS = [
    "detection_id", "contract_id", "case_id", "event_currency", "arm_id", "event_clock_utc",
    "detection_utc", "elapsed_minutes", "currency_direction", "currency_strength_bps", "breadth",
    "leg_count", "agreeing_leg_count", "selected_instrument", "selected_pair_direction",
    "selected_pair_return_bps", "entry_bid", "entry_ask", "entry_spread_pips", "entry_spread_bps",
    "strength_fast_mean_bps", "strength_slow_mean_bps", "technical_aligned", "source_direction_assigned",
    "response_direction_used", "evidence_unit_id", "detection_json", "row_sha256", "outcome_selected",
    "forecast_proof_eligible", "research_only", "execution_eligible",
]
OUTCOME_COLUMNS = [
    "outcome_id", "contract_id", "detection_id", "horizon_minutes", "exit_utc", "midpoint_move_pips",
    "executable_net_before_slippage_pips", "modeled_slippage_pips", "net_after_cost_pips", "mfe_pips",
    "mae_pips", "missed_entry_slippage_1m_pips", "delayed_1m_net_after_cost_pips", "cost_cleared",
    "outcome_json", "row_sha256", "outcome_selected", "forecast_proof_eligible", "research_only",
    "execution_eligible",
]


def insert_immutable(
    connection: sqlite3.Connection,
    table: str,
    columns: list[str],
    row: Mapping[str, Any],
    key_column: str,
) -> None:
    marks = ",".join("?" for _ in columns)
    connection.execute(
        f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({marks})",
        [row[column] for column in columns],
    )
    existing = connection.execute(
        f"SELECT row_sha256 FROM {table} WHERE {key_column}=?", (row[key_column],)
    ).fetchone()
    if existing is None or existing[0] != row["row_sha256"]:
        raise RuntimeError(f"immutable_replay_row_conflict:{table}:{row[key_column]}")


def run(database: Path = DATABASE, report_root: Path = REPORT_ROOT) -> dict[str, Any]:
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    cases, input_hash = load_inputs(connection)
    builder_hash = sha256_file(Path(__file__).resolve())
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "source_case_contract_id": SOURCE_CASE_CONTRACT_ID,
        "price_contract_id": PRICE_CONTRACT_ID,
        "builder_sha256": builder_hash,
        "input_snapshot_sha256": input_hash,
        "arms": ARMS,
        "outcome_horizons_minutes": OUTCOME_HORIZONS_MIN,
        "minimum_legs": MINIMUM_LEGS,
        "minimum_breadth": MINIMUM_BREADTH,
        "maximum_entry_spread_pips": MAXIMUM_ENTRY_SPREAD_PIPS,
        "modeled_slippage_pips": MODELED_SLIPPAGE_PIPS,
        "source_direction_assignment": "forbidden",
        "entry_direction": "post_event_cross_pair_response_only",
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
    }
    contract_json = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM verified_event_response_contracts WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()
    if existing is not None and existing[0] != contract_json:
        raise RuntimeError("immutable_event_response_contract_conflict")
    connection.execute("BEGIN IMMEDIATE")
    if existing is None:
        connection.execute(
            "INSERT INTO verified_event_response_contracts VALUES (?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, SOURCE_CASE_CONTRACT_ID, PRICE_CONTRACT_ID, contract_json,
                builder_hash, input_hash, FREEZE_UTC,
            ),
        )

    detections: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    exclusions: Counter[str] = Counter()
    for case in cases.values():
        exclusions["non_exact_unrelated_price_window"] += len(case.get("excluded_price_windows", []))
    for case_id, case in sorted(cases.items()):
        if not str(case["direction_policy"]).startswith("abstain"):
            raise RuntimeError(f"source_direction_policy_not_abstaining:{case_id}")
        for arm_id, arm in ARMS.items():
            detected = detect_arm(case, arm_id, arm)
            if detected is None:
                exclusions[f"no_detection:{arm_id}"] += 1
                continue
            detection_id = stable_id("verified_response_detection", CONTRACT_ID, case_id, arm_id)
            evidence_unit_id = stable_id(
                "verified_response_evidence", CONTRACT_ID, case_id,
                int(detected["detection_epoch"]) // (15 * 60),
            )
            payload = {
                "detection_id": detection_id,
                "contract_id": CONTRACT_ID,
                "case_id": case_id,
                "event_currency": case["event_currency"],
                "arm_id": arm_id,
                "event_clock_utc": case["response_watch_eligible_from_utc"],
                "detection_utc": iso_utc(int(detected["detection_epoch"])),
                "elapsed_minutes": int(detected["elapsed_minutes"]),
                "currency_direction": detected["currency_direction"],
                "currency_strength_bps": float(detected["currency_strength_bps"]),
                "breadth": float(detected["breadth"]),
                "leg_count": int(detected["leg_count"]),
                "agreeing_leg_count": int(detected["agreeing_leg_count"]),
                "selected_instrument": detected["selected_instrument"],
                "selected_pair_direction": detected["selected_pair_direction"],
                "selected_pair_return_bps": float(detected["selected_pair_return_bps"]),
                "entry_bid": float(detected["entry_bid"]),
                "entry_ask": float(detected["entry_ask"]),
                "entry_spread_pips": float(detected["entry_spread_pips"]),
                "entry_spread_bps": float(detected["entry_spread_bps"]),
                "strength_fast_mean_bps": detected["strength_fast_mean_bps"],
                "strength_slow_mean_bps": detected["strength_slow_mean_bps"],
                "technical_aligned": int(bool(detected["technical_aligned"])),
                "source_direction_assigned": 0,
                "response_direction_used": 1,
                "evidence_unit_id": evidence_unit_id,
                "outcome_selected": 0,
                "forecast_proof_eligible": 0,
                "research_only": 1,
                "execution_eligible": 0,
            }
            detection_json = canonical_json(payload)
            row = {
                **payload,
                "detection_json": detection_json,
                "row_sha256": sha256_bytes(detection_json.encode()),
            }
            insert_immutable(
                connection, "verified_event_response_detections", DETECTION_COLUMNS, row, "detection_id"
            )
            detections.append(row)
            candles = case["paths"][str(detected["selected_instrument"])]
            for horizon in OUTCOME_HORIZONS_MIN:
                measured = outcome(detected, candles, horizon)
                if measured is None:
                    exclusions[f"outcome_unavailable_h{horizon}"] += 1
                    continue
                outcome_id = stable_id("verified_response_outcome", CONTRACT_ID, detection_id, horizon)
                outcome_payload = {
                    "outcome_id": outcome_id,
                    "contract_id": CONTRACT_ID,
                    "detection_id": detection_id,
                    **measured,
                    "outcome_selected": 0,
                    "forecast_proof_eligible": 0,
                    "research_only": 1,
                    "execution_eligible": 0,
                }
                outcome_json = canonical_json(outcome_payload)
                outcome_row = {
                    **outcome_payload,
                    "outcome_json": outcome_json,
                    "row_sha256": sha256_bytes(outcome_json.encode()),
                }
                insert_immutable(
                    connection, "verified_event_response_outcomes", OUTCOME_COLUMNS, outcome_row, "outcome_id"
                )
                outcomes.append(outcome_row)
    connection.commit()
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = int(connection.execute(
        """
        SELECT
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_event_response_detections WHERE contract_id=?) +
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_event_response_outcomes WHERE contract_id=?)
        """, (CONTRACT_ID, CONTRACT_ID)
    ).fetchone()[0])
    connection.close()
    if execution_sum:
        raise RuntimeError("event_response_execution_surface_must_be_zero")

    cells: dict[tuple[str, int], list[float]] = defaultdict(list)
    cost_clears: dict[tuple[str, int], int] = Counter()
    detection_by_id = {row["detection_id"]: row for row in detections}
    for row in outcomes:
        arm_id = str(detection_by_id[row["detection_id"]]["arm_id"])
        key = (arm_id, int(row["horizon_minutes"]))
        cells[key].append(float(row["net_after_cost_pips"]))
        cost_clears[key] += int(row["cost_cleared"])
    cell_rows = []
    for (arm_id, horizon), values in sorted(cells.items()):
        cell_rows.append({
            "arm_id": arm_id,
            "horizon_minutes": horizon,
            "raw_n": len(values),
            "independent_event_n": len(values),
            "average_net_after_cost_pips": statistics.mean(values),
            "median_net_after_cost_pips": statistics.median(values),
            "cost_clear_rate": cost_clears[(arm_id, horizon)] / len(values),
            "promotion_eligible": False,
            "reason": "retrospective_two_event_diagnostic_only",
        })
    stable = {
        "contract": contract,
        "detections": sorted(row["row_sha256"] for row in detections),
        "outcomes": sorted(row["row_sha256"] for row in outcomes),
        "exclusions": dict(sorted(exclusions.items())),
    }
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "source_case_contract_id": SOURCE_CASE_CONTRACT_ID,
        "price_contract_id": PRICE_CONTRACT_ID,
        "generated_utc": utc_now(),
        "case_count": len(cases),
        "arm_count": len(ARMS),
        "detection_count": len(detections),
        "outcome_count": len(outcomes),
        "independent_event_count": len({row["case_id"] for row in detections}),
        "source_direction_assignment_count": sum(row["source_direction_assigned"] for row in detections),
        "response_direction_count": sum(row["response_direction_used"] for row in detections),
        "execution_eligible_count": execution_sum,
        "forecast_proof_eligible_count": 0,
        "exclusions": dict(sorted(exclusions.items())),
        "detections": [{
            key: row[key] for key in (
                "case_id", "event_currency", "arm_id", "detection_utc", "elapsed_minutes",
                "currency_direction", "currency_strength_bps", "breadth", "selected_instrument",
                "selected_pair_direction", "entry_spread_pips", "technical_aligned"
            )
        } for row in detections],
        "cells": cell_rows,
        "positive_point_estimate_cell_count": sum(
            row["average_net_after_cost_pips"] > 0 for row in cell_rows
        ),
        "promotion_eligible_cell_count": 0,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
        "sqlite_integrity": integrity,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "VERIFIED_EVENT_RESPONSE_REPLAY_V2.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# Verified event-response replay V1",
        "",
        f"- Exact source-watch cases: **{result['case_count']}**.",
        f"- Response detections: **{result['detection_count']}**; matured outcomes: **{result['outcome_count']}**.",
        f"- Independent historical events: **{result['independent_event_count']}**.",
        f"- Source-assigned directions: **{result['source_direction_assignment_count']}**.",
        f"- Positive diagnostic cells: **{result['positive_point_estimate_cell_count']}**; promotion eligible: **0**.",
        "- Entries use response-derived direction and executable bid/ask plus 0.25-pip modeled slippage.",
        "- Execution decision: **no_trade**.",
        "",
        "## Detections",
        "",
        "| Case | Arm | Elapsed | Response | Strength | Breadth | Pair | Side | Spread |",
        "|---|---|---:|---|---:|---:|---|---|---:|",
    ]
    for row in result["detections"]:
        lines.append(
            f"| {row['case_id']} | {row['arm_id']} | {row['elapsed_minutes']}m | "
            f"{row['event_currency']} {row['currency_direction']} | {row['currency_strength_bps']:.2f} | "
            f"{row['breadth']:.2f} | {row['selected_instrument']} | {row['selected_pair_direction']} | "
            f"{row['entry_spread_pips']:.2f} |"
        )
    lines.extend([
        "",
        "## After-cost cells",
        "",
        "| Arm | Horizon | N | Avg net pips | Cost-clear rate |",
        "|---|---:|---:|---:|---:|",
    ])
    for row in cell_rows:
        lines.append(
            f"| {row['arm_id']} | {row['horizon_minutes']}m | {row['raw_n']} | "
            f"{row['average_net_after_cost_pips']:.2f} | {row['cost_clear_rate']:.1%} |"
        )
    lines.append("")
    (report_root / "VERIFIED_EVENT_RESPONSE_REPLAY_V2.md").write_text("\n".join(lines), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
