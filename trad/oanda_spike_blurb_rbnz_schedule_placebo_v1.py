#!/usr/bin/env python3
"""Matched non-event controls for the RBNZ schedule-selected response cohort.

For every scheduled RBNZ decision, this module fixes two same-weekday controls
at 14:00 Pacific/Auckland, seven days before and seven days after.  It applies
the identical post-clock response detector and executable bid/ask outcome
calculation to the canonical NZD legs.  This tests whether the apparent event
edge is merely generic intraday momentum.  It is immutable research evidence
and cannot authorize, promote, or execute.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import statistics
from collections import Counter, defaultdict
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
    CONTRACT_ID as TREATMENT_CONTRACT_ID,
    DATABASE,
    EVENT_CURRENCY,
    PRE_EVENT_MINUTES,
    POST_EVENT_MINUTES,
    canonical_json,
    immutable_insert,
    payload_row,
    sha256_bytes,
    sha256_file,
    stable_id,
    utc_now,
)
from oanda_spike_blurb_verified_event_response_replay_v2 import (
    ARMS,
    OUTCOME_HORIZONS_MIN,
    detect_arm,
    outcome,
)


CONTRACT_ID = "spike_blurb_rbnz_schedule_placebo_v1_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "rbnz_schedule_placebo_v1"
)
FREEZE_UTC = "2026-08-20T19:00:00+00:00"
SCHEMA_VERSION = 1
CONTROL_DAY_OFFSETS = (-7, 7)
AUCKLAND = ZoneInfo("Pacific/Auckland")


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rbnz_schedule_placebo_contracts (
          contract_id TEXT PRIMARY KEY, treatment_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          dependency_sha256_json TEXT NOT NULL, input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_placebo_clocks (
          control_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          day_offset INTEGER NOT NULL, control_clock_utc TEXT NOT NULL,
          control_clock_local TEXT NOT NULL, selection_rule TEXT NOT NULL,
          control_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,source_event_id,day_offset)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_placebo_price_windows (
          window_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, control_id TEXT NOT NULL,
          instrument TEXT NOT NULL, requested_start_utc TEXT NOT NULL,
          requested_end_utc TEXT NOT NULL, coverage_state TEXT NOT NULL,
          candle_count INTEGER NOT NULL, first_utc TEXT, last_utc TEXT,
          average_spread_pips REAL, maximum_spread_pips REAL,
          http_status INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
          error_text TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
          payload_gzip BLOB NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,control_id,instrument)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_placebo_detections (
          detection_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, control_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, arm_id TEXT NOT NULL, detection_utc TEXT NOT NULL,
          elapsed_minutes INTEGER NOT NULL, currency_direction TEXT NOT NULL,
          currency_strength_bps REAL NOT NULL, breadth REAL NOT NULL,
          selected_instrument TEXT NOT NULL, selected_pair_direction TEXT NOT NULL,
          entry_spread_pips REAL NOT NULL, detection_json TEXT NOT NULL,
          row_sha256 TEXT NOT NULL, research_only INTEGER NOT NULL,
          execution_eligible INTEGER NOT NULL, forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,control_id,arm_id)
        );
        CREATE TABLE IF NOT EXISTS rbnz_schedule_placebo_outcomes (
          outcome_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, detection_id TEXT NOT NULL,
          horizon_minutes INTEGER NOT NULL, net_after_cost_pips REAL NOT NULL,
          mfe_pips REAL NOT NULL, mae_pips REAL NOT NULL, cost_cleared INTEGER NOT NULL,
          outcome_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,detection_id,horizon_minutes)
        );
        CREATE TRIGGER IF NOT EXISTS rbnz_placebo_clock_no_update BEFORE UPDATE ON rbnz_schedule_placebo_clocks BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_placebo_price_no_update BEFORE UPDATE ON rbnz_schedule_placebo_price_windows BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_placebo_detection_no_update BEFORE UPDATE ON rbnz_schedule_placebo_detections BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_placebo_outcome_no_update BEFORE UPDATE ON rbnz_schedule_placebo_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def load_treatment_events(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], str]:
    contract = connection.execute(
        "SELECT contract_json FROM rbnz_schedule_cohort_contracts WHERE contract_id=?",
        (TREATMENT_CONTRACT_ID,),
    ).fetchone()
    if contract is None:
        raise RuntimeError("treatment_contract_missing")
    rows = [dict(row) for row in connection.execute(
        "SELECT event_id,event_date,event_clock_utc,row_sha256 FROM rbnz_schedule_events WHERE contract_id=? ORDER BY event_clock_utc",
        (TREATMENT_CONTRACT_ID,),
    )]
    if len(rows) != 12:
        raise RuntimeError("exact_12_treatment_events_required")
    snapshot = sha256_bytes(canonical_json({
        "contract_json": str(contract[0]),
        "events": [{"event_id": row["event_id"], "row_sha256": row["row_sha256"]} for row in rows],
    }).encode())
    return rows, snapshot


def build_control_clocks(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    event_dates = {date.fromisoformat(str(row["event_date"])) for row in events}
    controls: list[dict[str, Any]] = []
    for event in events:
        original_date = date.fromisoformat(str(event["event_date"]))
        for offset in CONTROL_DAY_OFFSETS:
            control_date = original_date + timedelta(days=offset)
            if control_date in event_dates:
                raise RuntimeError(f"control_overlaps_policy_date:{control_date}")
            local = datetime.combine(control_date, dt_time(14, 0), tzinfo=AUCKLAND)
            utc = local.astimezone(timezone.utc)
            control_id = stable_id("rbnz_schedule_placebo_clock", CONTRACT_ID, event["event_id"], offset)
            controls.append({
                "control_id": control_id, "contract_id": CONTRACT_ID,
                "source_event_id": event["event_id"], "day_offset": offset,
                "control_clock_utc": utc.isoformat(), "control_clock_local": local.isoformat(),
                "control_epoch": int(utc.timestamp()),
                "selection_rule": "same_weekday_same_1400_pacific_auckland_plus_or_minus_7_days",
                "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
            })
    if len(controls) != 24 or len({row["control_id"] for row in controls}) != 24:
        raise RuntimeError("exact_24_matched_controls_required")
    return controls


def control_jobs(controls: list[dict[str, Any]], instruments: list[str]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for control in controls:
        clock = datetime.fromisoformat(control["control_clock_utc"])
        start = clock - timedelta(minutes=PRE_EVENT_MINUTES)
        end = clock + timedelta(minutes=POST_EVENT_MINUTES)
        for instrument in instruments:
            jobs.append({
                "case_id": control["control_id"], "control_id": control["control_id"],
                "instrument": instrument, "start_epoch": int(start.timestamp()),
                "end_epoch": int(end.timestamp()), "start_utc": start.isoformat(),
                "end_utc": end.isoformat(),
            })
    return jobs


def store_price(connection: sqlite3.Connection, result: Mapping[str, Any]) -> None:
    payload = {
        "window_id": stable_id("rbnz_placebo_price", CONTRACT_ID, result["control_id"], result["instrument"]),
        "contract_id": CONTRACT_ID, "control_id": result["control_id"],
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
        f"INSERT OR IGNORE INTO rbnz_schedule_placebo_price_windows ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
        [payload[column] for column in columns],
    )
    existing = connection.execute(
        "SELECT payload_sha256 FROM rbnz_schedule_placebo_price_windows WHERE window_id=?",
        (payload["window_id"],),
    ).fetchone()
    if existing is None or str(existing[0]) != str(payload["payload_sha256"]):
        raise RuntimeError(f"immutable_placebo_price_conflict:{payload['window_id']}")


def load_control_paths(connection: sqlite3.Connection, controls: list[dict[str, Any]], instruments: list[str]) -> dict[str, dict[str, Any]]:
    output = {row["control_id"]: {**row, "event_epoch": row["control_epoch"], "event_currency": EVENT_CURRENCY, "paths": {}} for row in controls}
    seen: dict[str, set[str]] = defaultdict(set)
    for raw in connection.execute(
        "SELECT control_id,instrument,coverage_state,payload_sha256,payload_gzip FROM rbnz_schedule_placebo_price_windows WHERE contract_id=? ORDER BY control_id,instrument",
        (CONTRACT_ID,),
    ):
        control_id, instrument, state, payload_hash = map(str, raw[:4])
        seen[control_id].add(instrument)
        if state != "exact_window":
            raise RuntimeError(f"matched_control_requires_exact_direct_path:{control_id}:{instrument}:{state}")
        decoded = gzip.decompress(raw[4])
        if sha256_bytes(decoded) != payload_hash:
            raise RuntimeError(f"placebo_payload_hash_mismatch:{control_id}:{instrument}")
        body = json.loads(decoded)
        output[control_id]["paths"][instrument] = {int(row["epoch"]): row for row in body["candles"]}
    expected = set(instruments)
    for control_id, row in output.items():
        if seen[control_id] != expected or set(row["paths"]) != expected:
            raise RuntimeError(f"placebo_direct_universe_incomplete:{control_id}")
    return output


def store_replay(connection: sqlite3.Connection, controls: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter[str]]:
    detections: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    exclusions: Counter[str] = Counter()
    for control in sorted(controls.values(), key=lambda row: row["control_clock_utc"]):
        for arm_id, arm in ARMS.items():
            detected = detect_arm(control, arm_id, arm)
            if detected is None:
                exclusions[f"no_detection:{arm_id}"] += 1
                continue
            detection = {
                "detection_id": stable_id("rbnz_placebo_detection", CONTRACT_ID, control["control_id"], arm_id),
                "contract_id": CONTRACT_ID, "control_id": control["control_id"],
                "source_event_id": control["source_event_id"], "arm_id": arm_id,
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
            immutable_insert(connection, "rbnz_schedule_placebo_detections", detection_row, "detection_id")
            detections.append(detection_row)
            path = control["paths"][detected["selected_instrument"]]
            for horizon in OUTCOME_HORIZONS_MIN:
                result = outcome(detected, path, horizon)
                if result is None:
                    exclusions[f"outcome_unavailable:h{horizon}"] += 1
                    continue
                payload = {
                    "outcome_id": stable_id("rbnz_placebo_outcome", CONTRACT_ID, detection["detection_id"], horizon),
                    "contract_id": CONTRACT_ID, "detection_id": detection["detection_id"],
                    "horizon_minutes": horizon,
                    "net_after_cost_pips": float(result["net_after_cost_pips"]),
                    "mfe_pips": float(result["mfe_pips"]), "mae_pips": float(result["mae_pips"]),
                    "cost_cleared": int(result["cost_cleared"]),
                    "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
                }
                outcome_row = payload_row(payload, "outcome_json")
                immutable_insert(connection, "rbnz_schedule_placebo_outcomes", outcome_row, "outcome_id")
                outcomes.append(outcome_row)
    return detections, outcomes, exclusions


def compare_cells(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    treatment = defaultdict(dict)
    for row in connection.execute(
        """
        SELECT d.event_id,d.arm_id,o.horizon_minutes,o.net_after_cost_pips,o.cost_cleared
        FROM rbnz_schedule_response_outcomes o
        JOIN rbnz_schedule_response_detections d ON d.detection_id=o.detection_id
        WHERE o.contract_id=?
        """, (TREATMENT_CONTRACT_ID,),
    ):
        treatment[(str(row[1]), int(row[2]))][str(row[0])] = (float(row[3]), int(row[4]))
    placebo = defaultdict(lambda: defaultdict(list))
    for row in connection.execute(
        """
        SELECT d.source_event_id,d.arm_id,o.horizon_minutes,o.net_after_cost_pips,o.cost_cleared
        FROM rbnz_schedule_placebo_outcomes o
        JOIN rbnz_schedule_placebo_detections d ON d.detection_id=o.detection_id
        WHERE o.contract_id=?
        """, (CONTRACT_ID,),
    ):
        placebo[(str(row[1]), int(row[2]))][str(row[0])].append((float(row[3]), int(row[4])))
    cells = []
    for key in sorted(treatment):
        arm_id, horizon = key
        paired = []
        for source_event, treatment_value in sorted(treatment[key].items()):
            controls = placebo[key].get(source_event, [])
            if len(controls) != 2:
                continue
            placebo_mean = statistics.mean(value[0] for value in controls)
            paired.append((treatment_value[0], treatment_value[1], placebo_mean, statistics.mean(value[1] for value in controls)))
        if not paired:
            continue
        treatment_values = [row[0] for row in paired]
        placebo_values = [row[2] for row in paired]
        best = max(treatment_values)
        cells.append({
            "arm_id": arm_id, "horizon_minutes": horizon, "paired_event_count": len(paired),
            "treatment_average_net_pips": statistics.mean(treatment_values),
            "placebo_average_net_pips": statistics.mean(placebo_values),
            "incremental_average_net_pips": statistics.mean(a - b for a, _, b, _ in paired),
            "treatment_cost_clearance_rate": statistics.mean(row[1] for row in paired),
            "placebo_cost_clearance_rate": statistics.mean(row[3] for row in paired),
            "treatment_average_without_best_event_pips": (
                statistics.mean(value for value in treatment_values if value != best)
                if len(treatment_values) > 1 else None
            ),
            "best_event_profit_share": best / sum(treatment_values) if sum(treatment_values) > 0 else None,
        })
    return cells


def write_report(result: dict[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "RBNZ_SCHEDULE_PLACEBO_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# RBNZ schedule-matched placebo V1", "",
        f"- Scheduled treatment events: **{result['treatment_event_count']}**.",
        f"- Same-weekday matched controls: **{result['control_count']}**.",
        f"- Canonical NZD instruments per clock: **{result['instrument_count']}**.",
        f"- Immutable price windows: **{result['price_window_count']}**.",
        f"- Coverage: `{canonical_json(result['coverage_states'])}`.",
        "- Each control was fixed at 14:00 Pacific/Auckland, exactly seven days before or after its source event.",
        "- Evidence remains retrospective and cannot authorize execution.", "", "## Treatment versus placebo", "",
        "| Arm | Horizon | Paired N | Treatment net | Placebo net | Incremental | Treatment wins | Placebo wins | Treatment ex-best |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["comparison_cells"]:
        lines.append(
            f"| {row['arm_id']} | {row['horizon_minutes']}m | {row['paired_event_count']} | "
            f"{row['treatment_average_net_pips']:.3f} | {row['placebo_average_net_pips']:.3f} | "
            f"{row['incremental_average_net_pips']:.3f} | {row['treatment_cost_clearance_rate']:.1%} | "
            f"{row['placebo_cost_clearance_rate']:.1%} | {row['treatment_average_without_best_event_pips']:.3f} |"
        )
    lines.append("")
    (report_root / "RBNZ_SCHEDULE_PLACEBO_V1.md").write_text("\n".join(lines), encoding="utf-8")


def run(database: Path = DATABASE, report_root: Path = REPORT_ROOT, workers: int = MAX_WORKERS, *, clocks_only: bool = False) -> dict[str, Any]:
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    events, input_hash = load_treatment_events(connection)
    controls = build_control_clocks(events)
    for control in controls:
        stored = {key: value for key, value in control.items() if key != "control_epoch"}
        immutable_insert(connection, "rbnz_schedule_placebo_clocks", payload_row(stored, "control_json"), "control_id")
    instruments = sorted({
        str(row[0]) for row in connection.execute(
            "SELECT DISTINCT instrument FROM rbnz_schedule_price_windows WHERE contract_id=?",
            (TREATMENT_CONTRACT_ID,),
        )
        if EVENT_CURRENCY in str(row[0]).split("_")
    })
    if not instruments or any(EVENT_CURRENCY not in instrument.split("_") for instrument in instruments):
        raise RuntimeError("canonical_nzd_direct_universe_missing")
    dependencies = {
        "treatment_builder": sha256_file(Path(__file__).resolve().parent / "oanda_spike_blurb_rbnz_schedule_cohort_v1.py"),
        "price_builder": sha256_file(Path(__file__).resolve().parent / "oanda_spike_blurb_event_watch_price_reacquisition_v3.py"),
        "replay_builder": sha256_file(Path(__file__).resolve().parent / "oanda_spike_blurb_verified_event_response_replay_v2.py"),
    }
    contract = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "treatment_contract_id": TREATMENT_CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "dependency_sha256": dependencies, "input_snapshot_sha256": input_hash,
        "control_offsets_days": CONTROL_DAY_OFFSETS,
        "control_clock_rule": "same_weekday_same_1400_pacific_auckland",
        "instrument_universe": instruments, "instrument_count": len(instruments),
        "research_only": True, "execution_eligible": False, "forecast_proof_eligible": False,
    }
    contract_json = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM rbnz_schedule_placebo_contracts WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()
    if existing is not None and str(existing[0]) != contract_json:
        raise RuntimeError("immutable_placebo_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO rbnz_schedule_placebo_contracts VALUES (?,?,?,?,?,?,?)",
            (CONTRACT_ID, TREATMENT_CONTRACT_ID, contract_json, contract["builder_sha256"], canonical_json(dependencies), input_hash, FREEZE_UTC),
        )
    connection.commit()
    if clocks_only:
        result = {"contract_id": CONTRACT_ID, "control_count": len(controls), "instrument_count": len(instruments), "research_only": True, "execution_eligible": False}
        connection.close()
        return result
    jobs = control_jobs(controls, instruments)
    existing_keys = {
        (str(row[0]), str(row[1])) for row in connection.execute(
            "SELECT control_id,instrument FROM rbnz_schedule_placebo_price_windows WHERE contract_id=?", (CONTRACT_ID,)
        )
    }
    pending = [job for job in jobs if (job["control_id"], job["instrument"]) not in existing_keys]
    if pending:
        client, meta = resolve_readonly_oanda_client()
        if meta.get("environment") != "practice" or "api-fxpractice.oanda.com" not in meta.get("base_url", ""):
            raise RuntimeError("practice_readonly_endpoint_required")
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max(1, min(int(workers), MAX_WORKERS))) as executor:
            futures = {executor.submit(fetch_job, client, job): job for job in pending}
            for future in as_completed(futures):
                store_price(connection, future.result())
                connection.commit()
    loaded = load_control_paths(connection, controls, instruments)
    _, _, exclusions = store_replay(connection, loaded)
    connection.commit()
    cells = compare_cells(connection)
    states = dict(connection.execute(
        "SELECT coverage_state,count(*) FROM rbnz_schedule_placebo_price_windows WHERE contract_id=? GROUP BY coverage_state", (CONTRACT_ID,)
    ).fetchall())
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = sum(int(connection.execute(
        f"SELECT coalesce(sum(execution_eligible),0) FROM {table} WHERE contract_id=?", (CONTRACT_ID,)
    ).fetchone()[0]) for table in (
        "rbnz_schedule_placebo_clocks", "rbnz_schedule_placebo_price_windows",
        "rbnz_schedule_placebo_detections", "rbnz_schedule_placebo_outcomes",
    ))
    stable = {
        "contract_id": CONTRACT_ID, "treatment_contract_id": TREATMENT_CONTRACT_ID,
        "treatment_event_count": len(events), "control_count": len(controls),
        "instrument_count": len(instruments), "price_window_count": sum(int(v) for v in states.values()),
        "coverage_states": states, "comparison_cells": cells, "exclusions": dict(exclusions),
        "execution_eligible_count": execution_sum, "sqlite_integrity": integrity,
        "proof_state": "matched_retrospective_placebo_not_untouched_confirmation",
        "supported_execution_decision": "no_trade",
    }
    result = {"schema_version": SCHEMA_VERSION, "generated_utc": utc_now(), "fetched_this_run": len(pending), **stable, "snapshot_sha256": sha256_bytes(canonical_json(stable).encode())}
    connection.close()
    if result["price_window_count"] != len(controls) * len(instruments):
        raise RuntimeError("placebo_price_coverage_incomplete")
    if execution_sum or integrity != "ok":
        raise RuntimeError("placebo_safety_or_integrity_failure")
    write_report(result, report_root)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--clocks-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root, args.workers, clocks_only=args.clocks_only), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
