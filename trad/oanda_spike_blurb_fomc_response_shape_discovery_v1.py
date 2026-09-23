#!/usr/bin/env python3
"""Discovery-only FOMC continuation/reversal response-shape map.

The frozen cross-authority test rejected the RBNZ-selected first-M1 response
followed for 15 minutes.  This module uses only the already immutable FOMC V1
price paths and V2 availability resolutions to diagnose *when* the observed
USD response continues or reverses.  It freezes a 24-cell grid before writing
any shape outcomes: three detection clocks, follow/fade, and four fixed holds.

All cells use executable bid/ask, observed spread, 0.25 pip slippage, matched
same-clock controls, event-level trios, and Holm adjustment.  Results are
adaptive discovery because this grid was motivated by the failed FOMC hold.
Even a passing cell can only become a seed for a different or future untouched
cohort.  This module cannot authorize, promote, or execute.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import oanda_spike_blurb_fomc_response_generalization_v1 as f1
import oanda_spike_blurb_fomc_response_generalization_v2 as f2
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
from oanda_spike_blurb_verified_event_response_replay_v2 import (
    MODELED_SLIPPAGE_PIPS,
    detect_arm,
    pip_size,
)


CONTRACT_ID = "spike_blurb_fomc_response_shape_discovery_v1_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "fomc_response_shape_discovery_v1"
)
FREEZE_UTC = "2026-08-20T18:15:00+00:00"
SCHEMA_VERSION = 1
HOLDS = (3, 5, 10, 15)
TRADE_MODES = ("follow", "fade")
ARMS = (
    ("response_at_1m", {"start_min": 1, "end_min": 1, "minimum_strength_bps": 1.0, "persistence_min": 1, "technical_confirmation": False}),
    ("response_at_3m_persist_2", {"start_min": 3, "end_min": 3, "minimum_strength_bps": 1.0, "persistence_min": 2, "technical_confirmation": False}),
    ("response_at_5m_persist_3", {"start_min": 5, "end_min": 5, "minimum_strength_bps": 1.0, "persistence_min": 3, "technical_confirmation": False}),
)
MINIMUM_COMPLETE_EVENT_TRIOS = 18


def input_snapshot(connection: sqlite3.Connection) -> str:
    v1_contract = connection.execute(
        "SELECT builder_sha256,contract_json FROM fomc_generalization_contracts WHERE contract_id=?",
        (f1.CONTRACT_ID,),
    ).fetchone()
    v2_contract = connection.execute(
        "SELECT builder_sha256,input_snapshot_sha256,contract_json FROM fomc_generalization_v2_contracts WHERE contract_id=?",
        (f2.CONTRACT_ID,),
    ).fetchone()
    if v1_contract is None or v2_contract is None:
        raise RuntimeError("fomc_shape_parent_contract_missing")
    parent_prices = [list(map(str, row)) for row in connection.execute(
        "SELECT clock_id,instrument,payload_sha256 FROM fomc_generalization_price_windows "
        "WHERE contract_id=? ORDER BY clock_id,instrument", (f1.CONTRACT_ID,),
    )]
    fallback_prices = [list(map(str, row)) for row in connection.execute(
        "SELECT clock_id,instrument,payload_sha256 FROM fomc_generalization_v2_fallback_prices "
        "WHERE contract_id=? ORDER BY clock_id,instrument", (f2.CONTRACT_ID,),
    )]
    resolutions = [list(map(str, row)) for row in connection.execute(
        "SELECT resolution_id,row_sha256 FROM fomc_generalization_v2_resolutions "
        "WHERE contract_id=? ORDER BY resolution_id", (f2.CONTRACT_ID,),
    )]
    if len(parent_prices) != 1260 or len(fallback_prices) != 60 or len(resolutions) != 63:
        raise RuntimeError(
            f"fomc_shape_input_coverage_mismatch:{len(parent_prices)}:{len(fallback_prices)}:{len(resolutions)}"
        )
    return sha256_bytes(canonical_json({
        "v1_builder_sha256": str(v1_contract[0]),
        "v1_contract_json_sha256": sha256_bytes(str(v1_contract[1]).encode()),
        "v2_builder_sha256": str(v2_contract[0]),
        "v2_input_snapshot_sha256": str(v2_contract[1]),
        "v2_contract_json_sha256": sha256_bytes(str(v2_contract[2]).encode()),
        "parent_price_identities": parent_prices,
        "fallback_price_identities": fallback_prices,
        "resolution_identities": resolutions,
    }).encode())


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS fomc_shape_contracts (
          contract_id TEXT PRIMARY KEY, parent_v1_contract_id TEXT NOT NULL,
          parent_v2_contract_id TEXT NOT NULL, contract_json TEXT NOT NULL,
          builder_sha256 TEXT NOT NULL, dependency_sha256_json TEXT NOT NULL,
          input_snapshot_sha256 TEXT NOT NULL, frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fomc_shape_outcomes (
          outcome_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
          control_slot INTEGER NOT NULL, selected_clock_id TEXT NOT NULL,
          arm_id TEXT NOT NULL, trade_mode TEXT NOT NULL, hold_minutes INTEGER NOT NULL,
          detected INTEGER NOT NULL, detection_utc TEXT, currency_direction TEXT,
          selected_instrument TEXT, selected_pair_direction TEXT, entry_spread_pips REAL,
          net_after_cost_pips REAL, mfe_pips REAL, mae_pips REAL, cost_cleared INTEGER,
          path_expected_bars INTEGER NOT NULL, path_observed_bars INTEGER NOT NULL,
          path_complete INTEGER NOT NULL, path_quality TEXT NOT NULL,
          analysis_eligible INTEGER NOT NULL, outcome_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,source_event_id,control_slot,arm_id,trade_mode,hold_minutes)
        );
        CREATE TRIGGER IF NOT EXISTS fomc_shape_contract_no_update BEFORE UPDATE ON fomc_shape_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_shape_contract_no_delete BEFORE DELETE ON fomc_shape_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_shape_outcome_no_update BEFORE UPDATE ON fomc_shape_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fomc_shape_outcome_no_delete BEFORE DELETE ON fomc_shape_outcomes BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def freeze_contract(connection: sqlite3.Connection, snapshot: str) -> dict[str, Any]:
    dependencies = {
        "fomc_v1": sha256_file(Path(f1.__file__).resolve()),
        "fomc_v2": sha256_file(Path(f2.__file__).resolve()),
        "response_math": sha256_file(
            Path(__file__).resolve().parent / "oanda_spike_blurb_verified_event_response_replay_v2.py"
        ),
    }
    contract = {
        "schema_version": SCHEMA_VERSION, "contract_id": CONTRACT_ID,
        "parent_v1_contract_id": f1.CONTRACT_ID,
        "parent_v2_contract_id": f2.CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "dependency_sha256": dependencies, "input_snapshot_sha256": snapshot,
        "selection_state": "adaptive_grid_frozen_after_primary_fomc_generalization_failure_before_shape_outcomes",
        "grid": {
            "arms": [{"arm_id": arm_id, "arm": arm} for arm_id, arm in ARMS],
            "trade_modes": list(TRADE_MODES), "hold_minutes": list(HOLDS),
            "cell_count": len(ARMS) * len(TRADE_MODES) * len(HOLDS),
        },
        "economics": {
            "price_basis": "executable_bid_ask", "modeled_slippage_pips": MODELED_SLIPPAGE_PIPS,
            "missing_detection": "no_trade_zero", "missing_declared_exit": "cell_trio_unavailable",
        },
        "inference": {
            "unit": "scheduled_event_matched_trio", "minimum_complete_event_trios": MINIMUM_COMPLETE_EVENT_TRIOS,
            "randomization": "frozen_200000_draw_matched_trio", "multiplicity": "holm_all_24_cells",
        },
        "evidence_role": "adaptive_discovery_only_requires_different_or_future_untouched_confirmation",
        "research_only": True, "execution_eligible": False,
        "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM fomc_shape_contracts WHERE contract_id=?", (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_fomc_shape_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO fomc_shape_contracts VALUES (?,?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, f1.CONTRACT_ID, f2.CONTRACT_ID, encoded,
                contract["builder_sha256"], canonical_json(dependencies), snapshot, FREEZE_UTC,
            ),
        )
    connection.commit()
    return contract


def selected_clocks(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events = f1.validate_schedule()
    parent_clocks = f1.build_clocks(events)
    _, parent_paths, invalid_slots = f2.parent_input_snapshot(connection, parent_clocks)
    fallbacks = f2.build_fallback_clocks(events, invalid_slots)
    fallback_paths = f2.load_fallback_paths(connection, fallbacks)
    selected, resolutions = f2.resolve_clocks(
        events, parent_clocks, parent_paths, fallbacks, fallback_paths,
    )
    stored = {
        (str(row[0]), int(row[1])): (str(row[2]), int(row[3]))
        for row in connection.execute(
            "SELECT source_event_id,control_slot,selected_clock_id,actual_day_offset "
            "FROM fomc_generalization_v2_resolutions WHERE contract_id=?",
            (f2.CONTRACT_ID,),
        )
    }
    rebuilt = {
        (str(row["source_event_id"]), int(row["control_slot"])):
        (str(row["selected_clock_id"]), int(row["actual_day_offset"]))
        for row in resolutions
    }
    if stored != rebuilt or len(stored) != 63:
        raise RuntimeError("fomc_shape_resolution_mismatch")
    return events, selected


def horizon_outcome(
    detection: Mapping[str, Any],
    candles: Mapping[int, Mapping[str, Any]],
    hold_minutes: int,
) -> dict[str, Any]:
    detection_epoch = int(detection["detection_epoch"])
    expected_epochs = [detection_epoch + minute * 60 for minute in range(hold_minutes)]
    exit_candle = candles.get(expected_epochs[-1])
    if exit_candle is None:
        raise RuntimeError("declared_exit_candle_missing")
    rows = [candles[epoch] for epoch in expected_epochs if epoch in candles]
    if not rows:
        raise RuntimeError("outcome_path_has_no_observed_bars")
    instrument = str(detection["selected_instrument"])
    direction = str(detection["selected_pair_direction"])
    pip = pip_size(instrument)
    entry_bid = float(detection["entry_bid"])
    entry_ask = float(detection["entry_ask"])
    if direction == "long":
        before = (float(exit_candle["bid_c"]) - entry_ask) / pip
        mfe = (max(float(row["bid_h"]) for row in rows) - entry_ask) / pip
        mae = (min(float(row["bid_l"]) for row in rows) - entry_ask) / pip
    elif direction == "short":
        before = (entry_bid - float(exit_candle["ask_c"])) / pip
        mfe = (entry_bid - min(float(row["ask_l"]) for row in rows)) / pip
        mae = (entry_bid - max(float(row["ask_h"]) for row in rows)) / pip
    else:
        raise RuntimeError(f"invalid_pair_direction:{direction}")
    net = before - MODELED_SLIPPAGE_PIPS
    complete = len(rows) == len(expected_epochs)
    return {
        "net_after_cost_pips": float(net), "mfe_pips": float(mfe), "mae_pips": float(mae),
        "cost_cleared": int(net > 0), "path_expected_bars": len(expected_epochs),
        "path_observed_bars": len(rows), "path_complete": int(complete),
        "path_quality": "complete" if complete else "partial_interior_bars_mfe_mae_partial",
    }


def flip_detection(detection: Mapping[str, Any]) -> dict[str, Any]:
    direction = str(detection["selected_pair_direction"])
    if direction not in {"long", "short"}:
        raise RuntimeError(f"invalid_pair_direction:{direction}")
    return {**detection, "selected_pair_direction": "short" if direction == "long" else "long"}


def evaluate(
    clock: Mapping[str, Any], arm_id: str, arm: Mapping[str, Any],
    trade_mode: str, hold_minutes: int,
) -> dict[str, Any]:
    base = {
        "outcome_id": stable_id(
            "fomc_shape_outcome", CONTRACT_ID, clock["source_event_id"],
            clock["control_slot"], arm_id, trade_mode, hold_minutes,
        ),
        "contract_id": CONTRACT_ID, "source_event_id": clock["source_event_id"],
        "control_slot": int(clock["control_slot"]), "selected_clock_id": clock["clock_id"],
        "arm_id": arm_id, "trade_mode": trade_mode, "hold_minutes": hold_minutes,
        "research_only": 1, "execution_eligible": 0, "forecast_proof_eligible": 0,
    }
    detection = detect_arm(clock, arm_id, arm)
    if detection is None:
        return {
            **base, "detected": 0, "detection_utc": None, "currency_direction": None,
            "selected_instrument": None, "selected_pair_direction": None,
            "entry_spread_pips": None, "net_after_cost_pips": 0.0,
            "mfe_pips": None, "mae_pips": None, "cost_cleared": 0,
            "path_expected_bars": hold_minutes, "path_observed_bars": 0,
            "path_complete": 0, "path_quality": "no_trade_no_detection",
            "analysis_eligible": 1,
        }
    traded = detection if trade_mode == "follow" else flip_detection(detection)
    instrument = str(traded["selected_instrument"])
    try:
        result = horizon_outcome(traded, clock["paths"][instrument], hold_minutes)
        eligible = 1
    except RuntimeError as exc:
        if str(exc) != "declared_exit_candle_missing":
            raise
        result = {
            "net_after_cost_pips": None, "mfe_pips": None, "mae_pips": None,
            "cost_cleared": None, "path_expected_bars": hold_minutes,
            "path_observed_bars": 0, "path_complete": 0,
            "path_quality": "selected_leg_declared_exit_missing",
        }
        eligible = 0
    return {
        **base, "detected": 1,
        "detection_utc": datetime.fromtimestamp(int(traded["detection_epoch"]), timezone.utc).isoformat(),
        "currency_direction": traded["currency_direction"], "selected_instrument": instrument,
        "selected_pair_direction": traded["selected_pair_direction"],
        "entry_spread_pips": float(traded["entry_spread_pips"]),
        **result, "analysis_eligible": eligible,
    }


def store_outcomes(connection: sqlite3.Connection, clocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for clock in clocks:
        for arm_id, arm in ARMS:
            for trade_mode in TRADE_MODES:
                for hold in HOLDS:
                    row = payload_row(evaluate(clock, arm_id, arm, trade_mode, hold), "outcome_json")
                    immutable_insert(connection, "fomc_shape_outcomes", row, "outcome_id")
                    rows.append(row)
    connection.commit()
    return rows


def holm_adjust(pvalues: list[float]) -> list[float]:
    count = len(pvalues)
    ordered = sorted(range(count), key=lambda index: pvalues[index])
    adjusted = [1.0] * count
    running = 0.0
    for rank, index in enumerate(ordered):
        running = max(running, min(1.0, pvalues[index] * (count - rank)))
        adjusted[index] = running
    return adjusted


def cell_analysis(
    events: list[dict[str, Any]], rows: list[dict[str, Any]],
    arm_id: str, trade_mode: str, hold: int,
) -> dict[str, Any]:
    selected = [
        row for row in rows
        if row["arm_id"] == arm_id and row["trade_mode"] == trade_mode
        and int(row["hold_minutes"]) == hold
    ]
    by_event: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in selected:
        by_event[str(row["source_event_id"])][int(row["control_slot"])] = row
    triples: list[tuple[float, float, float]] = []
    complete_events: list[dict[str, Any]] = []
    for event in events:
        trio = by_event[str(event["event_id"])]
        if set(trio) != {-1, 0, 1}:
            raise RuntimeError("fomc_shape_exact_trio_missing")
        if any(int(trio[slot]["analysis_eligible"]) == 0 for slot in (-1, 0, 1)):
            continue
        triples.append(tuple(float(trio[slot]["net_after_cost_pips"]) for slot in (0, -1, 1)))
        complete_events.append(event)
    if len(triples) < MINIMUM_COMPLETE_EVENT_TRIOS:
        raise RuntimeError(f"fomc_shape_cell_undercovered:{arm_id}:{trade_mode}:{hold}:{len(triples)}")
    treatment = [values[0] for values in triples]
    controls = [value for values in triples for value in values[1:]]
    increments = [values[0] - (values[1] + values[2]) / 2.0 for values in triples]
    leave_one_out = [
        statistics.mean(value for index, value in enumerate(treatment) if index != omitted)
        for omitted in range(len(treatment))
    ]
    randomization = f1.monte_carlo_within_trio_pvalue(triples)
    clearance = statistics.mean(
        int(by_event[str(event["event_id"])][0]["cost_cleared"]) for event in complete_events
    )
    return {
        "cell_id": f"{arm_id}:{trade_mode}:h{hold}", "arm_id": arm_id,
        "trade_mode": trade_mode, "hold_minutes": hold,
        "event_count": len(triples), "excluded_event_count": len(events) - len(triples),
        "treatment_average_net_pips": statistics.mean(treatment),
        "control_average_net_pips": statistics.mean(controls),
        "incremental_average_net_pips": statistics.mean(increments),
        "incremental_median_net_pips": statistics.median(increments),
        "incremental_positive_event_rate": statistics.mean(value > 0 for value in increments),
        "treatment_cost_clearance_rate": clearance,
        "minimum_leave_one_out_treatment_average_pips": min(leave_one_out),
        "randomization_pvalue": float(randomization["pvalue"]),
        "randomization_standard_error": float(randomization["monte_carlo_standard_error"]),
    }


def analyze(events: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    cells = [
        cell_analysis(events, rows, arm_id, mode, hold)
        for arm_id, _ in ARMS for mode in TRADE_MODES for hold in HOLDS
    ]
    adjusted = holm_adjust([float(cell["randomization_pvalue"]) for cell in cells])
    for cell, value in zip(cells, adjusted):
        cell["holm_adjusted_pvalue"] = value
        gates = {
            "positive_treatment_average": float(cell["treatment_average_net_pips"]) > 0,
            "positive_incremental_average": float(cell["incremental_average_net_pips"]) > 0,
            "cost_clearance_at_least_two_thirds": float(cell["treatment_cost_clearance_rate"]) >= 2 / 3,
            "positive_minimum_leave_one_out": float(cell["minimum_leave_one_out_treatment_average_pips"]) > 0,
            "holm_adjusted_pvalue_at_most_0_05": value <= 0.05,
        }
        cell["discovery_gates"] = gates
        cell["discovery_seed_eligible"] = all(gates.values())
    seeds = sorted(
        (cell for cell in cells if cell["discovery_seed_eligible"]),
        key=lambda cell: (-float(cell["incremental_average_net_pips"]), str(cell["cell_id"])),
    )
    return {
        "cell_count": len(cells), "multiplicity_family": "all_24_fomc_shape_cells",
        "discovery_seed_count": len(seeds), "selected_discovery_seed": seeds[0] if seeds else None,
        "cells": sorted(cells, key=lambda cell: (-float(cell["incremental_average_net_pips"]), str(cell["cell_id"]))),
        "proof_state": "adaptive_discovery_only_not_confirmation",
        "supported_execution_decision": "no_trade",
    }


def assert_primary_reproduction(
    connection: sqlite3.Connection, rows: list[dict[str, Any]],
) -> None:
    shape = {
        (str(row["source_event_id"]), int(row["control_slot"])): float(row["net_after_cost_pips"])
        for row in rows
        if row["arm_id"] == "response_at_1m" and row["trade_mode"] == "follow"
        and int(row["hold_minutes"]) == 15 and int(row["analysis_eligible"]) == 1
    }
    parent = {
        (str(row[0]), int(row[1])): float(row[2])
        for row in connection.execute(
            "SELECT source_event_id,control_slot,net_after_cost_pips FROM fomc_generalization_v2_decisions "
            "WHERE contract_id=? AND analysis_eligible=1", (f2.CONTRACT_ID,),
        )
    }
    if shape != parent:
        raise RuntimeError("fomc_shape_primary_cell_does_not_reproduce_frozen_v2")


def write_report(result: Mapping[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "FOMC_RESPONSE_SHAPE_DISCOVERY_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    analysis = result["analysis"]
    lines = [
        "# FOMC continuation/reversal response-shape discovery V1", "",
        "- Uses only frozen FOMC event/control paths and availability resolutions.",
        "- Grid: 1/3/5-minute response clocks, follow/fade, 3/5/10/15-minute holds.",
        "- All 24 cells are one Holm family; no cell is confirmation evidence.",
        "- Any seed requires a different or future untouched cohort.",
        "- Execution decision: **no_trade**.", "",
        f"- Discovery seeds passing every diagnostic gate: **{analysis['discovery_seed_count']}**.",
        "", "## Cells", "",
        "| Cell | N | Treatment | Control | Incremental | Clear cost | Raw p | Holm p | Seed |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for cell in analysis["cells"]:
        lines.append(
            f"| {cell['cell_id']} | {cell['event_count']} | {cell['treatment_average_net_pips']:.3f} | "
            f"{cell['control_average_net_pips']:.3f} | {cell['incremental_average_net_pips']:.3f} | "
            f"{cell['treatment_cost_clearance_rate']:.1%} | {cell['randomization_pvalue']:.6f} | "
            f"{cell['holm_adjusted_pvalue']:.6f} | {cell['discovery_seed_eligible']} |"
        )
    lines.append("")
    (report_root / "FOMC_RESPONSE_SHAPE_DISCOVERY_V1.md").write_text("\n".join(lines), encoding="utf-8")


def run(
    database: Path = DATABASE, report_root: Path = REPORT_ROOT, *, freeze_only: bool = False,
) -> dict[str, Any]:
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    snapshot = input_snapshot(connection)
    contract = freeze_contract(connection, snapshot)
    if freeze_only:
        result = {
            "contract_id": CONTRACT_ID, "builder_sha256": contract["builder_sha256"],
            "input_snapshot_sha256": snapshot, "cell_count": 24,
            "research_only": True, "execution_eligible": False,
            "forecast_proof_eligible": False, "supported_execution_decision": "no_trade",
        }
        connection.close()
        return result
    events, clocks = selected_clocks(connection)
    rows = store_outcomes(connection, clocks)
    assert_primary_reproduction(connection, rows)
    analysis = analyze(events, rows)
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = int(connection.execute(
        "SELECT coalesce(sum(execution_eligible),0) FROM fomc_shape_outcomes WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()[0])
    stable = {
        "contract_id": CONTRACT_ID, "builder_sha256": contract["builder_sha256"],
        "input_snapshot_sha256": snapshot, "event_count": len(events),
        "selected_clock_count": len(clocks), "outcome_count": len(rows),
        "analysis": analysis, "sqlite_integrity": integrity,
        "execution_eligible_count": execution_sum, "research_only": True,
        "execution_eligible": False, "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    result = {
        "schema_version": SCHEMA_VERSION, "generated_utc": utc_now(), **stable,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
    }
    connection.close()
    if len(rows) != 63 * 24 or integrity != "ok" or execution_sum != 0:
        raise RuntimeError("fomc_shape_integrity_or_safety_failure")
    write_report(result, report_root)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.database, args.report_root, freeze_only=args.freeze_only), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
