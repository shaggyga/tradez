#!/usr/bin/env python3
"""Materialize exact-source factor-to-response analog trajectories.

Each event response is stored once per horizon, then linked to the event's
versioned source factors.  Factor links retain the same effective event ID so
multiple facts from one release cannot inflate independent evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from oanda_spike_blurb_event_watch_price_reacquisition_v3 import DATABASE, canonical_json, sha256_bytes, sha256_file, stable_id, utc_now
from oanda_spike_blurb_verified_event_response_replay_v2 import (
    PRICE_CONTRACT_ID,
    SOURCE_CASE_CONTRACT_ID,
    currency_snapshot,
    load_inputs,
)


CONTRACT_ID = "spike_blurb_verified_factor_response_analogs_v2_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_factor_response_analogs_v2"
)
FREEZE_UTC = "2026-08-20T17:15:00+00:00"
SCHEMA_VERSION = 1
HORIZONS_MIN = (1, 3, 5, 10, 15, 30, 60)


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS verified_factor_response_analog_contracts (
          contract_id TEXT PRIMARY KEY,
          source_case_contract_id TEXT NOT NULL,
          price_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL,
          input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verified_event_response_trajectories (
          trajectory_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          effective_event_id TEXT NOT NULL,
          event_currency TEXT NOT NULL,
          event_family TEXT NOT NULL,
          event_clock_utc TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL,
          response_utc TEXT NOT NULL,
          currency_direction TEXT NOT NULL,
          currency_strength_bps REAL NOT NULL,
          breadth REAL NOT NULL,
          leg_count INTEGER NOT NULL,
          agreeing_leg_count INTEGER NOT NULL,
          selected_instrument TEXT NOT NULL,
          selected_pair_direction TEXT NOT NULL,
          selected_pair_return_bps REAL NOT NULL,
          selected_spread_pips REAL NOT NULL,
          consensus_state TEXT NOT NULL,
          rate_repricing_state TEXT NOT NULL,
          response_direction_observed INTEGER NOT NULL,
          source_direction_assigned INTEGER NOT NULL,
          trajectory_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,case_id,horizon_minutes)
        );
        CREATE TABLE IF NOT EXISTS verified_factor_response_analog_links (
          analog_link_id TEXT PRIMARY KEY,
          contract_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          effective_event_id TEXT NOT NULL,
          factor_id TEXT NOT NULL,
          factor_version INTEGER NOT NULL,
          factor_family TEXT NOT NULL,
          factor_metric TEXT NOT NULL,
          factor_unit TEXT NOT NULL,
          factor_value_number REAL,
          factor_value_text TEXT,
          trajectory_id TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL,
          analog_key TEXT NOT NULL,
          analog_role TEXT NOT NULL,
          independent_event_weight REAL NOT NULL,
          analog_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL,
          prequential_prediction INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,case_id,factor_id,horizon_minutes)
        );
        CREATE INDEX IF NOT EXISTS verified_factor_response_analog_key
          ON verified_factor_response_analog_links(contract_id,analog_key,effective_event_id);
        CREATE TRIGGER IF NOT EXISTS verified_factor_response_analog_contracts_no_update
          BEFORE UPDATE ON verified_factor_response_analog_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_factor_response_analog_contracts_no_delete
          BEFORE DELETE ON verified_factor_response_analog_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_trajectories_no_update
          BEFORE UPDATE ON verified_event_response_trajectories BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_event_response_trajectories_no_delete
          BEFORE DELETE ON verified_event_response_trajectories BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_factor_response_analog_links_no_update
          BEFORE UPDATE ON verified_factor_response_analog_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS verified_factor_response_analog_links_no_delete
          BEFORE DELETE ON verified_factor_response_analog_links BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def insert_immutable(
    connection: sqlite3.Connection,
    table: str,
    row: Mapping[str, Any],
    key: str,
) -> None:
    columns = list(row)
    connection.execute(
        f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        [row[column] for column in columns],
    )
    existing = connection.execute(
        f"SELECT row_sha256 FROM {table} WHERE {key}=?", (row[key],)
    ).fetchone()
    if existing is None or existing[0] != row["row_sha256"]:
        raise RuntimeError(f"immutable_analog_row_conflict:{table}:{row[key]}")


def source_factors(connection: sqlite3.Connection, case_ids: set[str]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in connection.execute(
        """
        SELECT case_id,factor_id,factor_version,factor_family,metric,value_number,value_text,unit,row_sha256
        FROM verified_external_source_factors
        WHERE contract_id=? ORDER BY case_id,factor_id
        """, (SOURCE_CASE_CONTRACT_ID,)
    ):
        row = dict(raw)
        if row["case_id"] in case_ids:
            output[str(row["case_id"])].append(row)
    if set(output) != case_ids or any(not rows for rows in output.values()):
        raise RuntimeError("source_factor_coverage_missing")
    return output


def analog_key(case: Mapping[str, Any], factor: Mapping[str, Any], horizon: int) -> str:
    return "|".join([
        str(case["event_family"]), str(case["event_currency"]), str(factor["factor_family"]),
        str(factor["metric"]), str(factor["unit"]), f"h{horizon}",
    ])


def run(database: Path = DATABASE, report_root: Path = REPORT_ROOT) -> dict[str, Any]:
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    cases, price_input_hash = load_inputs(connection)
    for case in cases.values():
        source_case = json.loads(connection.execute(
            "SELECT case_json FROM verified_external_source_cases WHERE case_id=?", (case["case_id"],)
        ).fetchone()[0])
        case["event_family"] = source_case["event_family"]
    factors = source_factors(connection, set(cases))
    input_digest = hashlib.sha256(price_input_hash.encode())
    for case_id in sorted(factors):
        for factor in factors[case_id]:
            input_digest.update(str(factor["row_sha256"]).encode())
    input_hash = input_digest.hexdigest()
    builder_hash = sha256_file(Path(__file__).resolve())
    contract = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "source_case_contract_id": SOURCE_CASE_CONTRACT_ID,
        "price_contract_id": PRICE_CONTRACT_ID,
        "builder_sha256": builder_hash,
        "input_snapshot_sha256": input_hash,
        "horizons_minutes": HORIZONS_MIN,
        "event_response_stored_once": True,
        "factor_links_share_effective_event_id": True,
        "source_direction_assignment": "none",
        "prequential_minimum_prior_analogs": 3,
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
    }
    contract_json = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM verified_factor_response_analog_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and existing[0] != contract_json:
        raise RuntimeError("immutable_factor_response_analog_contract_conflict")
    connection.execute("BEGIN IMMEDIATE")
    if existing is None:
        connection.execute(
            "INSERT INTO verified_factor_response_analog_contracts VALUES (?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, SOURCE_CASE_CONTRACT_ID, PRICE_CONTRACT_ID, contract_json,
                builder_hash, input_hash, FREEZE_UTC,
            ),
        )

    trajectories: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    case_trajectory: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case_id, case in sorted(cases.items()):
        effective_event_id = stable_id("verified_effective_event", CONTRACT_ID, case_id)
        for horizon in HORIZONS_MIN:
            response = currency_snapshot(
                case["paths"], case["event_currency"], int(case["event_epoch"]), horizon
            )
            if response is None:
                raise RuntimeError(f"response_trajectory_unavailable:{case_id}:h{horizon}")
            trajectory_id = stable_id("verified_response_trajectory", CONTRACT_ID, case_id, horizon)
            payload = {
                "trajectory_id": trajectory_id,
                "contract_id": CONTRACT_ID,
                "case_id": case_id,
                "effective_event_id": effective_event_id,
                "event_currency": case["event_currency"],
                "event_family": case["event_family"],
                "event_clock_utc": case["response_watch_eligible_from_utc"],
                "horizon_minutes": horizon,
                "response_utc": datetime.fromtimestamp(
                    int(case["event_epoch"]) + horizon * 60, timezone.utc
                ).isoformat(),
                "currency_direction": response["currency_direction"],
                "currency_strength_bps": float(response["currency_strength_bps"]),
                "breadth": float(response["breadth"]),
                "leg_count": int(response["leg_count"]),
                "agreeing_leg_count": int(response["agreeing_leg_count"]),
                "selected_instrument": response["selected_instrument"],
                "selected_pair_direction": response["selected_pair_direction"],
                "selected_pair_return_bps": float(response["selected_pair_return_bps"]),
                "selected_spread_pips": float(response["entry_spread_pips"]),
                "consensus_state": case["consensus_state"],
                "rate_repricing_state": case["rate_repricing_state"],
                "response_direction_observed": 1,
                "source_direction_assigned": 0,
                "forecast_proof_eligible": 0,
                "research_only": 1,
                "execution_eligible": 0,
            }
            payload_json = canonical_json(payload)
            row = {
                **payload,
                "trajectory_json": payload_json,
                "row_sha256": sha256_bytes(payload_json.encode()),
            }
            insert_immutable(connection, "verified_event_response_trajectories", row, "trajectory_id")
            trajectories.append(row)
            case_trajectory[case_id].append(row)
            for factor in factors[case_id]:
                link_id = stable_id(
                    "verified_factor_response_link", CONTRACT_ID, case_id, factor["factor_id"], horizon
                )
                key = analog_key(case, factor, horizon)
                link_payload = {
                    "analog_link_id": link_id,
                    "contract_id": CONTRACT_ID,
                    "case_id": case_id,
                    "effective_event_id": effective_event_id,
                    "factor_id": factor["factor_id"],
                    "factor_version": int(factor["factor_version"]),
                    "factor_family": factor["factor_family"],
                    "factor_metric": factor["metric"],
                    "factor_unit": factor["unit"],
                    "factor_value_number": factor["value_number"],
                    "factor_value_text": factor["value_text"],
                    "trajectory_id": trajectory_id,
                    "horizon_minutes": horizon,
                    "analog_key": key,
                    "analog_role": "retrospective_exact_source_response_not_prediction",
                    "independent_event_weight": 1.0 / len(factors[case_id]),
                    "prequential_prediction": 0,
                    "forecast_proof_eligible": 0,
                    "research_only": 1,
                    "execution_eligible": 0,
                }
                link_json = canonical_json(link_payload)
                link_row = {
                    **link_payload,
                    "analog_json": link_json,
                    "row_sha256": sha256_bytes(link_json.encode()),
                }
                insert_immutable(
                    connection, "verified_factor_response_analog_links", link_row, "analog_link_id"
                )
                links.append(link_row)
    connection.commit()
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = int(connection.execute(
        """
        SELECT
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_event_response_trajectories WHERE contract_id=?) +
          (SELECT coalesce(sum(execution_eligible),0) FROM verified_factor_response_analog_links WHERE contract_id=?)
        """, (CONTRACT_ID, CONTRACT_ID)
    ).fetchone()[0])
    connection.close()
    if execution_sum:
        raise RuntimeError("factor_response_analog_execution_surface_must_be_zero")

    case_summaries = []
    reversal_count = 0
    for case_id, rows in sorted(case_trajectory.items()):
        rows.sort(key=lambda row: row["horizon_minutes"])
        first = rows[0]["currency_direction"]
        flip = next((row for row in rows[1:] if row["currency_direction"] != first), None)
        reversal_count += int(flip is not None)
        case_summaries.append({
            "case_id": case_id,
            "event_currency": rows[0]["event_currency"],
            "first_direction": first,
            "last_direction": rows[-1]["currency_direction"],
            "first_strength_bps": rows[0]["currency_strength_bps"],
            "last_strength_bps": rows[-1]["currency_strength_bps"],
            "first_observed_flip_horizon_minutes": flip["horizon_minutes"] if flip else None,
            "factor_count": len(factors[case_id]),
            "effective_event_count": 1,
        })
    key_events: dict[str, set[str]] = defaultdict(set)
    for row in links:
        key_events[row["analog_key"]].add(row["effective_event_id"])
    repeated_keys = {key: len(events) for key, events in key_events.items() if len(events) >= 2}
    stable = {
        "contract": contract,
        "trajectories": sorted(row["row_sha256"] for row in trajectories),
        "links": sorted(row["row_sha256"] for row in links),
    }
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "generated_utc": utc_now(),
        "case_count": len(cases),
        "effective_event_count": len(cases),
        "source_factor_count": sum(len(rows) for rows in factors.values()),
        "trajectory_count": len(trajectories),
        "factor_response_link_count": len(links),
        "analog_key_count": len(key_events),
        "repeated_analog_key_count": len(repeated_keys),
        "repeated_analog_keys": repeated_keys,
        "response_reversal_case_count": reversal_count,
        "prequential_prediction_count": sum(row["prequential_prediction"] for row in links),
        "forecast_proof_eligible_count": 0,
        "execution_eligible_count": execution_sum,
        "case_summaries": case_summaries,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
        "sqlite_integrity": integrity,
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "VERIFIED_FACTOR_RESPONSE_ANALOGS_V2.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# Verified factor-response analogs V2",
        "",
        f"- Exact event cases / effective events: **{len(cases)} / {len(cases)}**.",
        f"- Versioned source factors: **{result['source_factor_count']}**.",
        f"- Event trajectories: **{len(trajectories)}**; factor-response links: **{len(links)}**.",
        f"- Repeated exact analog keys: **{len(repeated_keys)}**.",
        f"- Cases with an observed response-direction reversal: **{reversal_count}**.",
        "- Multiple factors from one release share one effective event and cannot inflate N.",
        "- Prequential predictions: **0**; execution decision: **no_trade**.",
        "",
        "## Case trajectories",
        "",
        "| Case | Currency | 1m response | 60m response | First observed flip | Factors | Effective N |",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for row in case_summaries:
        lines.append(
            f"| {row['case_id']} | {row['event_currency']} | "
            f"{row['first_direction']} {row['first_strength_bps']:.2f} bps | "
            f"{row['last_direction']} {row['last_strength_bps']:.2f} bps | "
            f"{row['first_observed_flip_horizon_minutes'] or '-'} | {row['factor_count']} | 1 |"
        )
    lines.append("")
    (report_root / "VERIFIED_FACTOR_RESPONSE_ANALOGS_V2.md").write_text("\n".join(lines), encoding="utf-8")
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
