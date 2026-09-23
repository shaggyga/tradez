#!/usr/bin/env python3
"""Gap-tolerant outcome layer for the frozen RBNZ response candidate V1.

V1 froze the candidate and acquired all 162 direct-NZD price windows before
failing closed because one selected path lacked a single interior M1 candle.
The entry candle and declared 15-minute exit candle were present.  V2 leaves
the candidate, clocks, prices, direction, and horizon unchanged.  It freezes
one outcome rule before calculating returns: executable entry and exit prices
are mandatory; missing interior bars do not erase the realized fixed-horizon
return, while MFE/MAE are marked partial and use only observed bars.

This is immutable retrospective research.  It cannot authorize, promote, or
execute and it remains ineligible for forecast proof.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import oanda_spike_blurb_rbnz_response_confirmation_v1 as v1
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


CONTRACT_ID = "spike_blurb_rbnz_response_confirmation_v2_20260820"
REPORT_ROOT = Path(__file__).resolve().parent / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "rbnz_response_confirmation_v2"
)
FREEZE_UTC = "2026-08-20T20:45:00+00:00"
SCHEMA_VERSION = 1


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_v2_contracts (
          contract_id TEXT PRIMARY KEY, parent_contract_id TEXT NOT NULL,
          contract_json TEXT NOT NULL, builder_sha256 TEXT NOT NULL,
          dependency_sha256_json TEXT NOT NULL, input_snapshot_sha256 TEXT NOT NULL,
          frozen_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rbnz_confirmation_v2_decisions (
          decision_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, clock_id TEXT NOT NULL,
          source_event_id TEXT NOT NULL, clock_kind TEXT NOT NULL, day_offset INTEGER NOT NULL,
          arm_id TEXT NOT NULL, hold_minutes INTEGER NOT NULL, detected INTEGER NOT NULL,
          detection_utc TEXT, currency_direction TEXT, currency_strength_bps REAL,
          breadth REAL, selected_instrument TEXT, selected_pair_direction TEXT,
          entry_spread_pips REAL, net_after_cost_pips REAL NOT NULL,
          mfe_pips REAL, mae_pips REAL, cost_cleared INTEGER NOT NULL,
          path_expected_bars INTEGER NOT NULL, path_observed_bars INTEGER NOT NULL,
          path_complete INTEGER NOT NULL, path_quality TEXT NOT NULL,
          decision_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL, execution_eligible INTEGER NOT NULL,
          forecast_proof_eligible INTEGER NOT NULL,
          UNIQUE(contract_id,clock_id,arm_id,hold_minutes)
        );
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_v2_contract_no_update BEFORE UPDATE ON rbnz_confirmation_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_v2_contract_no_delete BEFORE DELETE ON rbnz_confirmation_v2_contracts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_v2_decision_no_update BEFORE UPDATE ON rbnz_confirmation_v2_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS rbnz_confirmation_v2_decision_no_delete BEFORE DELETE ON rbnz_confirmation_v2_decisions BEGIN SELECT RAISE(ABORT,'immutable'); END;
        """
    )


def input_snapshot(connection: sqlite3.Connection) -> str:
    parent = connection.execute(
        "SELECT contract_json,builder_sha256 FROM rbnz_confirmation_contracts WHERE contract_id=?",
        (v1.CONTRACT_ID,),
    ).fetchone()
    if parent is None:
        raise RuntimeError("frozen_v1_parent_contract_missing")
    events = [
        [str(row[0]), str(row[1])]
        for row in connection.execute(
            "SELECT event_id,row_sha256 FROM rbnz_confirmation_events WHERE contract_id=? ORDER BY event_id",
            (v1.CONTRACT_ID,),
        )
    ]
    clocks = [
        [str(row[0]), str(row[1])]
        for row in connection.execute(
            "SELECT clock_id,row_sha256 FROM rbnz_confirmation_clocks WHERE contract_id=? ORDER BY clock_id",
            (v1.CONTRACT_ID,),
        )
    ]
    prices = [
        [str(row[0]), str(row[1]), str(row[2])]
        for row in connection.execute(
            "SELECT clock_id,instrument,payload_sha256 FROM rbnz_confirmation_price_windows "
            "WHERE contract_id=? ORDER BY clock_id,instrument",
            (v1.CONTRACT_ID,),
        )
    ]
    if len(events) != 6 or len(clocks) != 18 or len(prices) != 162:
        raise RuntimeError(f"v1_input_coverage_incomplete:{len(events)}:{len(clocks)}:{len(prices)}")
    return sha256_bytes(canonical_json({
        "parent_contract_json": str(parent[0]),
        "parent_builder_sha256": str(parent[1]),
        "events": events,
        "clocks": clocks,
        "prices": prices,
    }).encode())


def freeze_contract(connection: sqlite3.Connection) -> dict[str, Any]:
    snapshot = input_snapshot(connection)
    dependencies = {
        "parent_v1": sha256_file(Path(v1.__file__).resolve()),
        "response_math": sha256_file(
            Path(__file__).resolve().parent / "oanda_spike_blurb_verified_event_response_replay_v2.py"
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
            "arm_id": v1.LOCKED_ARM_ID,
            "arm": v1.LOCKED_ARM,
            "hold_minutes": v1.HOLD_MINUTES,
            "direct_instruments": list(v1.DIRECT_INSTRUMENTS),
        },
        "outcome_path_policy": {
            "entry_price_required": True,
            "declared_exit_candle_required": True,
            "fixed_horizon_net_uses_executable_entry_and_exit": True,
            "interior_bar_gaps_do_not_change_trade_selection_or_net_return": True,
            "mfe_mae_use_observed_bars_and_are_labeled_partial": True,
            "missing_detection_policy": "explicit_no_trade_zero_return",
        },
        "freeze_state": "frozen_after_v1_gap_detection_before_v2_outcome_calculation",
        "research_only": True,
        "execution_eligible": False,
        "forecast_proof_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    encoded = canonical_json(contract)
    existing = connection.execute(
        "SELECT contract_json FROM rbnz_confirmation_v2_contracts WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()
    if existing is not None and str(existing[0]) != encoded:
        raise RuntimeError("immutable_confirmation_v2_contract_conflict")
    if existing is None:
        connection.execute(
            "INSERT INTO rbnz_confirmation_v2_contracts VALUES (?,?,?,?,?,?,?)",
            (
                CONTRACT_ID, v1.CONTRACT_ID, encoded, contract["builder_sha256"],
                canonical_json(dependencies), snapshot, FREEZE_UTC,
            ),
        )
    connection.commit()
    return contract


def fixed_horizon_outcome(
    detection: Mapping[str, Any],
    candles: Mapping[int, Mapping[str, Any]],
) -> dict[str, Any]:
    detection_epoch = int(detection["detection_epoch"])
    expected_epochs = [detection_epoch + minute * 60 for minute in range(v1.HOLD_MINUTES)]
    exit_epoch = expected_epochs[-1]
    exit_candle = candles.get(exit_epoch)
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
        before_slippage = (float(exit_candle["bid_c"]) - entry_ask) / pip
        mfe = (max(float(row["bid_h"]) for row in rows) - entry_ask) / pip
        mae = (min(float(row["bid_l"]) for row in rows) - entry_ask) / pip
    elif direction == "short":
        before_slippage = (entry_bid - float(exit_candle["ask_c"])) / pip
        mfe = (entry_bid - min(float(row["ask_l"]) for row in rows)) / pip
        mae = (entry_bid - max(float(row["ask_h"]) for row in rows)) / pip
    else:
        raise RuntimeError(f"invalid_pair_direction:{direction}")
    net = before_slippage - MODELED_SLIPPAGE_PIPS
    complete = len(rows) == len(expected_epochs)
    return {
        "net_after_cost_pips": float(net),
        "mfe_pips": float(mfe),
        "mae_pips": float(mae),
        "cost_cleared": int(net > 0),
        "path_expected_bars": len(expected_epochs),
        "path_observed_bars": len(rows),
        "path_complete": int(complete),
        "path_quality": "complete" if complete else "partial_interior_bars_mfe_mae_partial",
    }


def evaluate_clock(clock: Mapping[str, Any]) -> dict[str, Any]:
    detected = detect_arm(clock, v1.LOCKED_ARM_ID, v1.LOCKED_ARM)
    base = {
        "decision_id": stable_id("rbnz_confirmation_v2_decision", CONTRACT_ID, clock["clock_id"], v1.LOCKED_ARM_ID, v1.HOLD_MINUTES),
        "contract_id": CONTRACT_ID,
        "clock_id": clock["clock_id"],
        "source_event_id": clock["source_event_id"],
        "clock_kind": clock["clock_kind"],
        "day_offset": int(clock["day_offset"]),
        "arm_id": v1.LOCKED_ARM_ID,
        "hold_minutes": v1.HOLD_MINUTES,
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
            "path_expected_bars": v1.HOLD_MINUTES,
            "path_observed_bars": 0,
            "path_complete": 0,
            "path_quality": "no_trade_no_detection",
        }
    instrument = str(detected["selected_instrument"])
    result = fixed_horizon_outcome(detected, clock["paths"][instrument])
    return {
        **base,
        "detected": 1,
        "detection_utc": datetime.fromtimestamp(int(detected["detection_epoch"]), timezone.utc).isoformat(),
        "currency_direction": detected["currency_direction"],
        "currency_strength_bps": float(detected["currency_strength_bps"]),
        "breadth": float(detected["breadth"]),
        "selected_instrument": instrument,
        "selected_pair_direction": detected["selected_pair_direction"],
        "entry_spread_pips": float(detected["entry_spread_pips"]),
        **result,
    }


def store_decisions(
    connection: sqlite3.Connection,
    loaded: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in sorted(loaded.values(), key=lambda row: str(row["clock_utc"])):
        row = payload_row(evaluate_clock(item), "decision_json")
        immutable_insert(connection, "rbnz_confirmation_v2_decisions", row, "decision_id")
        rows.append(row)
    connection.commit()
    return rows


def write_report(result: Mapping[str, Any], report_root: Path) -> None:
    report_root.mkdir(parents=True, exist_ok=True)
    (report_root / "RBNZ_RESPONSE_CONFIRMATION_V2.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    analysis = result["analysis"]
    lines = [
        "# Locked later RBNZ response replay V2",
        "",
        "- V1 candidate, clocks, prices, directions, and 15-minute horizon are unchanged.",
        "- V2 permits missing interior bars only when executable entry and declared exit prices exist.",
        "- Fixed-horizon net return is exact; MFE/MAE are labeled partial for an incomplete path.",
        "- This is later-period retrospective replication, not prospective live proof.",
        "- Execution decision: **no_trade**.",
        "",
        "## Result",
        "",
        f"- Scheduled events: **{analysis['independent_scheduled_event_count']}**; controls: **{analysis['matched_control_count']}**.",
        f"- Treatment average: **{analysis['treatment_average_net_pips']:.3f} pips**.",
        f"- Control average: **{analysis['control_average_net_pips']:.3f} pips**.",
        f"- Incremental average: **{analysis['incremental_average_net_pips']:.3f} pips**.",
        f"- Treatment cost-clearance: **{analysis['treatment_cost_clearance_rate']:.1%}**.",
        f"- Minimum leave-one-event-out treatment average: **{analysis['minimum_leave_one_event_out_treatment_average_pips']:.3f} pips**.",
        f"- Exact within-trio randomization p: **{analysis['exact_within_trio_randomization_pvalue']:.6f}**.",
        f"- Predeclared diagnostic replication passed: **{analysis['diagnostic_replication_passed']}**.",
        f"- Partial detected paths: **{result['partial_detected_path_count']}**.",
        "",
        "## Event rows",
        "",
        "| Date | Action | Response | Pair | Treatment | -7d | +7d | Incremental |",
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
    (report_root / "RBNZ_RESPONSE_CONFIRMATION_V2.md").write_text("\n".join(lines), encoding="utf-8")


def run(
    database: Path = DATABASE,
    report_root: Path = REPORT_ROOT,
    *,
    freeze_only: bool = False,
) -> dict[str, Any]:
    events = v1.validate_schedule()
    clocks = v1.build_clocks(events)
    connection = sqlite3.connect(database, timeout=120)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    ensure_schema(connection)
    contract = freeze_contract(connection)
    if freeze_only:
        result = {
            "contract_id": CONTRACT_ID,
            "parent_contract_id": v1.CONTRACT_ID,
            "builder_sha256": contract["builder_sha256"],
            "input_snapshot_sha256": contract["input_snapshot_sha256"],
            "freeze_state": contract["freeze_state"],
            "research_only": True,
            "execution_eligible": False,
            "forecast_proof_eligible": False,
            "supported_execution_decision": "no_trade",
        }
        connection.close()
        return result
    loaded, price_snapshot = v1.load_paths(connection, clocks)
    decisions = store_decisions(connection, loaded)
    analysis = v1.analyze(events, decisions)
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    execution_sum = int(connection.execute(
        "SELECT coalesce(sum(execution_eligible),0) FROM rbnz_confirmation_v2_decisions WHERE contract_id=?",
        (CONTRACT_ID,),
    ).fetchone()[0])
    partial_count = sum(
        int(row["detected"]) == 1 and int(row["path_complete"]) == 0 for row in decisions
    )
    stable = {
        "contract_id": CONTRACT_ID,
        "parent_contract_id": v1.CONTRACT_ID,
        "builder_sha256": contract["builder_sha256"],
        "input_snapshot_sha256": contract["input_snapshot_sha256"],
        "price_snapshot_sha256": price_snapshot,
        "decision_count": len(decisions),
        "partial_detected_path_count": partial_count,
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
        **stable,
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
    }
    connection.close()
    if len(decisions) != 18 or integrity != "ok" or execution_sum != 0:
        raise RuntimeError("confirmation_v2_integrity_or_safety_failure")
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
