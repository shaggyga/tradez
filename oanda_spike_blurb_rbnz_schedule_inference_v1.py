#!/usr/bin/env python3
"""Read-only inference audit for the frozen RBNZ treatment/placebo cohorts.

Missing detections are scored as explicit no-trade/zero return.  Each source
event contributes one scheduled-decision observation and two same-weekday
matched controls.  An exact within-trio randomization test evaluates the mean
incremental return, followed by Holm adjustment across all arm/horizon cells.
The audit is retrospective and cannot authorize or promote anything.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

from oanda_spike_blurb_rbnz_schedule_cohort_v1 import (
    CONTRACT_ID as TREATMENT_CONTRACT_ID,
    DATABASE,
    canonical_json,
    sha256_bytes,
    sha256_file,
    utc_now,
)
from oanda_spike_blurb_rbnz_schedule_placebo_v1 import CONTRACT_ID as PLACEBO_CONTRACT_ID


CONTRACT_ID = "spike_blurb_rbnz_schedule_inference_v1_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "rbnz_schedule_inference_v1"
)
SCHEMA_VERSION = 1


def exact_randomization_pvalue(triples: list[tuple[float, float, float]]) -> float:
    """One-sided exact p-value for treatment minus mean of two controls."""
    if not triples:
        raise ValueError("triples_required")
    observed = statistics.mean(a - (b + c) / 2.0 for a, b, c in triples)
    sums = [0.0]
    for a, b, c in triples:
        effects = (a - (b + c) / 2.0, b - (a + c) / 2.0, c - (a + b) / 2.0)
        sums = [running + effect for running in sums for effect in effects]
    threshold = observed * len(triples)
    return sum(value >= threshold - 1e-12 for value in sums) / len(sums)


def holm_adjust(rows: list[dict[str, Any]], field: str = "randomization_pvalue") -> None:
    ordered = sorted(enumerate(rows), key=lambda item: float(item[1][field]))
    running = 0.0
    count = len(rows)
    for rank, (index, row) in enumerate(ordered):
        adjusted = min(1.0, float(row[field]) * (count - rank))
        running = max(running, adjusted)
        rows[index]["holm_adjusted_pvalue"] = running


def load_inputs(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, dict[int, str]], list[tuple[str, int]]]:
    events = [dict(row) for row in connection.execute(
        "SELECT event_id,event_date,decision_action,official_cash_rate_change_percentage_points FROM rbnz_schedule_events WHERE contract_id=? ORDER BY event_date",
        (TREATMENT_CONTRACT_ID,),
    )]
    if len(events) != 12:
        raise RuntimeError("exact_12_treatment_events_required")
    controls: dict[str, dict[int, str]] = defaultdict(dict)
    for row in connection.execute(
        "SELECT control_id,source_event_id,day_offset FROM rbnz_schedule_placebo_clocks WHERE contract_id=? ORDER BY source_event_id,day_offset",
        (PLACEBO_CONTRACT_ID,),
    ):
        controls[str(row[1])][int(row[2])] = str(row[0])
    if set(controls) != {str(row["event_id"]) for row in events} or any(set(value) != {-7, 7} for value in controls.values()):
        raise RuntimeError("exact_two_controls_per_event_required")
    cells = sorted({
        (str(row[0]), int(row[1])) for row in connection.execute(
            "SELECT DISTINCT arm_id,horizon_minutes FROM rbnz_schedule_response_detections d JOIN rbnz_schedule_response_outcomes o ON o.detection_id=d.detection_id WHERE o.contract_id=?",
            (TREATMENT_CONTRACT_ID,),
        )
    })
    if len(cells) != 16:
        raise RuntimeError(f"exact_16_treatment_cells_required:{len(cells)}")
    return events, controls, cells


def outcome_maps(connection: sqlite3.Connection) -> tuple[dict[tuple[str, str, int], float], dict[tuple[str, str, int], int], dict[tuple[str, str, int], float], dict[tuple[str, str, int], int]]:
    treatment_value: dict[tuple[str, str, int], float] = {}
    treatment_win: dict[tuple[str, str, int], int] = {}
    for row in connection.execute(
        """
        SELECT d.event_id,d.arm_id,o.horizon_minutes,o.net_after_cost_pips,o.cost_cleared
        FROM rbnz_schedule_response_outcomes o JOIN rbnz_schedule_response_detections d
          ON d.detection_id=o.detection_id WHERE o.contract_id=?
        """, (TREATMENT_CONTRACT_ID,),
    ):
        key = (str(row[0]), str(row[1]), int(row[2]))
        treatment_value[key] = float(row[3])
        treatment_win[key] = int(row[4])
    placebo_value: dict[tuple[str, str, int], float] = {}
    placebo_win: dict[tuple[str, str, int], int] = {}
    for row in connection.execute(
        """
        SELECT d.control_id,d.arm_id,o.horizon_minutes,o.net_after_cost_pips,o.cost_cleared
        FROM rbnz_schedule_placebo_outcomes o JOIN rbnz_schedule_placebo_detections d
          ON d.detection_id=o.detection_id WHERE o.contract_id=?
        """, (PLACEBO_CONTRACT_ID,),
    ):
        key = (str(row[0]), str(row[1]), int(row[2]))
        placebo_value[key] = float(row[3])
        placebo_win[key] = int(row[4])
    return treatment_value, treatment_win, placebo_value, placebo_win


def analyze(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events, controls, cells = load_inputs(connection)
    treatment_value, treatment_win, placebo_value, placebo_win = outcome_maps(connection)
    result_cells: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    for arm_id, horizon in cells:
        triples: list[tuple[float, float, float]] = []
        actions: dict[str, list[float]] = defaultdict(list)
        details = []
        for event in events:
            event_id = str(event["event_id"])
            treatment = treatment_value.get((event_id, arm_id, horizon), 0.0)
            minus = placebo_value.get((controls[event_id][-7], arm_id, horizon), 0.0)
            plus = placebo_value.get((controls[event_id][7], arm_id, horizon), 0.0)
            triple = (treatment, minus, plus)
            triples.append(triple)
            increment = treatment - (minus + plus) / 2.0
            actions[str(event["decision_action"])].append(treatment)
            details.append({
                "event_id": event_id, "event_date": event["event_date"],
                "decision_action": event["decision_action"],
                "rate_change_percentage_points": event["official_cash_rate_change_percentage_points"],
                "treatment_net_pips": treatment, "control_minus_7d_net_pips": minus,
                "control_plus_7d_net_pips": plus, "incremental_net_pips": increment,
                "treatment_executed": int((event_id, arm_id, horizon) in treatment_value),
                "control_minus_executed": int((controls[event_id][-7], arm_id, horizon) in placebo_value),
                "control_plus_executed": int((controls[event_id][7], arm_id, horizon) in placebo_value),
            })
        treatment_values = [row[0] for row in triples]
        control_values = [value for row in triples for value in row[1:]]
        increments = [row[0] - (row[1] + row[2]) / 2.0 for row in triples]
        best_index = max(range(len(treatment_values)), key=lambda index: treatment_values[index])
        leave_one_out = [
            statistics.mean(value for index, value in enumerate(treatment_values) if index != omitted)
            for omitted in range(len(treatment_values))
        ]
        cell = {
            "arm_id": arm_id, "horizon_minutes": horizon,
            "independent_scheduled_event_count": len(events), "matched_control_count": len(events) * 2,
            "treatment_average_net_pips": statistics.mean(treatment_values),
            "control_average_net_pips": statistics.mean(control_values),
            "incremental_average_net_pips": statistics.mean(increments),
            "incremental_median_net_pips": statistics.median(increments),
            "incremental_positive_event_rate": statistics.mean(value > 0 for value in increments),
            "treatment_cost_clearance_rate": statistics.mean(
                treatment_win.get((str(event["event_id"]), arm_id, horizon), 0) for event in events
            ),
            "control_cost_clearance_rate": statistics.mean(
                placebo_win.get((control_id, arm_id, horizon), 0)
                for mapping in controls.values() for control_id in mapping.values()
            ),
            "treatment_no_trade_count": sum((str(event["event_id"]), arm_id, horizon) not in treatment_value for event in events),
            "control_no_trade_count": sum(
                (control_id, arm_id, horizon) not in placebo_value
                for mapping in controls.values() for control_id in mapping.values()
            ),
            "treatment_average_without_best_event_pips": statistics.mean(
                value for index, value in enumerate(treatment_values) if index != best_index
            ),
            "minimum_leave_one_event_out_average_pips": min(leave_one_out),
            "best_event_id": str(events[best_index]["event_id"]),
            "best_event_net_pips": treatment_values[best_index],
            "hold_average_net_pips": statistics.mean(actions["hold"]),
            "cut_average_net_pips": statistics.mean(actions["cut"]),
            "randomization_pvalue": exact_randomization_pvalue(triples),
        }
        result_cells.append(cell)
        event_rows.extend({"arm_id": arm_id, "horizon_minutes": horizon, **row} for row in details)
    holm_adjust(result_cells)
    return result_cells, event_rows


def write_report(result: dict[str, Any], root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "RBNZ_SCHEDULE_INFERENCE_V1.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# RBNZ schedule response inference V1", "",
        "- Missing detections are included as no-trade with zero return.",
        "- P-values are exact within-event trio randomization tests.",
        "- Holm adjustment covers all 16 inspected arm/horizon cells.",
        "- Controls were constructed after the treatment study; this is falsification evidence, not untouched confirmation.",
        "- Execution decision: **no_trade**.", "", "## Cells", "",
        "| Arm | Horizon | Treatment | Control | Incremental | Inc. positive | Ex-best | Exact p | Holm p |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["cells"]:
        lines.append(
            f"| {row['arm_id']} | {row['horizon_minutes']}m | {row['treatment_average_net_pips']:.3f} | "
            f"{row['control_average_net_pips']:.3f} | {row['incremental_average_net_pips']:.3f} | "
            f"{row['incremental_positive_event_rate']:.1%} | {row['treatment_average_without_best_event_pips']:.3f} | "
            f"{row['randomization_pvalue']:.6f} | {row['holm_adjusted_pvalue']:.6f} |"
        )
    lines.append("")
    (root / "RBNZ_SCHEDULE_INFERENCE_V1.md").write_text("\n".join(lines), encoding="utf-8")


def run(database: Path = DATABASE, report_root: Path = REPORT_ROOT) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    cells, event_rows = analyze(connection)
    upstream = {
        "treatment_contract": str(connection.execute(
            "SELECT contract_json FROM rbnz_schedule_cohort_contracts WHERE contract_id=?", (TREATMENT_CONTRACT_ID,)
        ).fetchone()[0]),
        "placebo_contract": str(connection.execute(
            "SELECT contract_json FROM rbnz_schedule_placebo_contracts WHERE contract_id=?", (PLACEBO_CONTRACT_ID,)
        ).fetchone()[0]),
    }
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    connection.close()
    stable = {
        "contract_id": CONTRACT_ID, "treatment_contract_id": TREATMENT_CONTRACT_ID,
        "placebo_contract_id": PLACEBO_CONTRACT_ID,
        "builder_sha256": sha256_file(Path(__file__).resolve()),
        "upstream_snapshot_sha256": sha256_bytes(canonical_json(upstream).encode()),
        "cell_count": len(cells), "cells": cells, "event_rows": event_rows,
        "multiplicity_method": "holm_familywise_16_cells",
        "missing_detection_policy": "explicit_no_trade_zero_return",
        "sqlite_integrity": integrity, "research_only": True,
        "execution_eligible": False, "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    result = {"schema_version": SCHEMA_VERSION, "generated_utc": utc_now(), **stable,
              "snapshot_sha256": sha256_bytes(canonical_json(stable).encode())}
    if integrity != "ok" or len(cells) != 16:
        raise RuntimeError("inference_input_integrity_failure")
    if any(not math.isfinite(float(row["randomization_pvalue"])) for row in cells):
        raise RuntimeError("inference_nonfinite")
    write_report(result, report_root)
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
