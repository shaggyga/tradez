#!/usr/bin/env python3
"""Compare frozen retrospective event-response cohorts without pooling them.

The first cohort may suggest hypotheses; the second remains a retrospective
replication diagnostic because its events were selected from a movement-first
inventory.  This report cannot confirm, promote, authorize, or execute.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_cohort_comparison_v1"
)
REPLAY_V1 = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_event_response_replay_v1/VERIFIED_EVENT_RESPONSE_REPLAY_V1.json"
)
REPLAY_V2 = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_event_response_replay_v2/VERIFIED_EVENT_RESPONSE_REPLAY_V2.json"
)
ANALOG_V1 = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_factor_response_analogs_v1/VERIFIED_FACTOR_RESPONSE_ANALOGS_V1.json"
)
ANALOG_V2 = ROOT / (
    "data/oanda_training_manager/reports/spike_blurb_factor_reconstruction/"
    "verified_factor_response_analogs_v2/VERIFIED_FACTOR_RESPONSE_ANALOGS_V2.json"
)
CONTRACT_ID = "spike_blurb_verified_cohort_comparison_v1_20260820"
FREEZE_UTC = "2026-08-20T17:20:00+00:00"
EXPECTED_CONTRACTS = {
    "replay_v1": "spike_blurb_verified_event_response_replay_v1_20260820",
    "replay_v2": "spike_blurb_verified_event_response_replay_v2_20260820",
    "analog_v1": "spike_blurb_verified_factor_response_analogs_v1_20260820",
    "analog_v2": "spike_blurb_verified_factor_response_analogs_v2_20260820",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_report(path: Path, contract_id: str) -> dict[str, Any]:
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("contract_id") != contract_id:
        raise RuntimeError(f"upstream_contract_mismatch:{path.name}")
    if report.get("research_only") is not True or report.get("execution_eligible") is not False:
        raise RuntimeError(f"upstream_not_inert:{path.name}")
    if report.get("supported_execution_decision") != "no_trade":
        raise RuntimeError(f"upstream_decision_not_no_trade:{path.name}")
    if int(report.get("execution_eligible_count", -1)) != 0:
        raise RuntimeError(f"upstream_execution_surface_nonzero:{path.name}")
    if int(report.get("forecast_proof_eligible_count", -1)) != 0:
        raise RuntimeError(f"upstream_proof_surface_nonzero:{path.name}")
    return report


def cell_map(report: dict[str, Any]) -> dict[tuple[str, int], dict[str, Any]]:
    cells: dict[tuple[str, int], dict[str, Any]] = {}
    for cell in report["cells"]:
        key = (str(cell["arm_id"]), int(cell["horizon_minutes"]))
        if key in cells:
            raise RuntimeError(f"duplicate_cell:{key}")
        if int(cell["independent_event_n"]) != 2 or int(cell["raw_n"]) != 2:
            raise RuntimeError(f"unexpected_retrospective_cell_n:{key}")
        if cell["promotion_eligible"] is not False:
            raise RuntimeError(f"retrospective_cell_cannot_promote:{key}")
        cells[key] = cell
    return cells


def build(report_root: Path = REPORT_ROOT) -> dict[str, Any]:
    paths = {
        "replay_v1": REPLAY_V1,
        "replay_v2": REPLAY_V2,
        "analog_v1": ANALOG_V1,
        "analog_v2": ANALOG_V2,
    }
    upstream = {
        name: load_report(path, EXPECTED_CONTRACTS[name])
        for name, path in paths.items()
    }
    first = cell_map(upstream["replay_v1"])
    second = cell_map(upstream["replay_v2"])
    if set(first) != set(second):
        raise RuntimeError("cohort_cell_surface_mismatch")

    cells: list[dict[str, Any]] = []
    discovery_positive_keys: list[str] = []
    replicated_positive_keys: list[str] = []
    for arm_id, horizon in sorted(first):
        left = first[(arm_id, horizon)]
        right = second[(arm_id, horizon)]
        left_ev = float(left["average_net_after_cost_pips"])
        right_ev = float(right["average_net_after_cost_pips"])
        discovery_positive = left_ev > 0
        same_cell_positive = discovery_positive and right_ev > 0
        key_text = f"{arm_id}|h{horizon}"
        if discovery_positive:
            discovery_positive_keys.append(key_text)
        if same_cell_positive:
            replicated_positive_keys.append(key_text)
        cells.append({
            "cell_key": key_text,
            "arm_id": arm_id,
            "horizon_minutes": horizon,
            "cohort_v1_event_n": int(left["independent_event_n"]),
            "cohort_v1_average_net_after_cost_pips": left_ev,
            "cohort_v1_cost_clear_rate": float(left["cost_clear_rate"]),
            "cohort_v2_event_n": int(right["independent_event_n"]),
            "cohort_v2_average_net_after_cost_pips": right_ev,
            "cohort_v2_cost_clear_rate": float(right["cost_clear_rate"]),
            "v1_discovery_point_positive": discovery_positive,
            "v2_same_cell_point_positive": right_ev > 0,
            "retrospective_sign_replication": same_cell_positive,
            "confirmation_eligible": False,
            "reason": "movement_selected_retrospective_cases_not_untouched_confirmation",
        })

    trajectory_cases = (
        list(upstream["analog_v1"]["case_summaries"])
        + list(upstream["analog_v2"]["case_summaries"])
    )
    initial_directions = [str(case["first_direction"]) for case in trajectory_cases]
    reversal_count = sum(
        case["first_observed_flip_horizon_minutes"] is not None for case in trajectory_cases
    )
    exact_analog_keys = {
        key
        for report in (upstream["analog_v1"], upstream["analog_v2"])
        for key in report["repeated_analog_keys"]
    }
    input_hashes = {name: sha256_file(path) for name, path in paths.items()}
    stable = {
        "contract_id": CONTRACT_ID,
        "freeze_utc": FREEZE_UTC,
        "upstream_file_sha256": input_hashes,
        "cells": cells,
        "trajectory_case_ids": sorted(case["case_id"] for case in trajectory_cases),
    }
    result = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": FREEZE_UTC,
        "upstream_contracts": EXPECTED_CONTRACTS,
        "upstream_file_sha256": input_hashes,
        "cohort_count": 2,
        "independent_event_count": len(trajectory_cases),
        "event_currency_count": len({case["event_currency"] for case in trajectory_cases}),
        "cell_count": len(cells),
        "cells": cells,
        "v1_discovery_point_positive_cell_count": len(discovery_positive_keys),
        "v1_discovery_point_positive_cells": discovery_positive_keys,
        "v1_positive_cells_remaining_positive_in_v2_count": len(replicated_positive_keys),
        "v1_positive_cells_remaining_positive_in_v2": replicated_positive_keys,
        "initial_weaker_response_count": initial_directions.count("weaker"),
        "initial_stronger_response_count": initial_directions.count("stronger"),
        "response_reversal_case_count": reversal_count,
        "cross_cohort_repeated_exact_analog_key_count": len(exact_analog_keys),
        "prequential_prediction_count": 0,
        "confirmation_eligible_count": 0,
        "forecast_proof_eligible_count": 0,
        "execution_eligible_count": 0,
        "interpretation": (
            "Immediate weakening appeared in all four selected cases, but timing-arm performance "
            "was not stable: both V1-positive response_1m cells turned negative in V2. Exact factor "
            "keys have not repeated, so no analog prediction or confirmation exists."
        ),
        "snapshot_sha256": sha256_bytes(canonical_json(stable).encode()),
        "research_only": True,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
    }
    report_root.mkdir(parents=True, exist_ok=True)
    json_path = report_root / "VERIFIED_COHORT_COMPARISON_V1.json"
    md_path = report_root / "VERIFIED_COHORT_COMPARISON_V1.md"
    json_text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    lines = [
        "# Verified event-response cohort comparison V1",
        "",
        f"- Frozen cohorts: **2**; selected independent events: **{result['independent_event_count']}**.",
        f"- V1 point-positive cells: **{len(discovery_positive_keys)}**; still positive in V2: **{len(replicated_positive_keys)}**.",
        f"- Exact factor analog keys repeated across cohorts: **{len(exact_analog_keys)}**.",
        f"- Response reversals by 60 minutes: **{reversal_count}/{len(trajectory_cases)}**.",
        "- Confirmation/proof/execution eligibility: **0 / 0 / 0**; decision: **no_trade**.",
        "",
        "The second cohort is not an untouched confirmation set because cases were selected from the movement-first inventory.",
        "It is a replication diagnostic only and is never numerically pooled with V1.",
        "",
        "| Arm | Horizon | V1 net pips | V2 net pips | Positive in both? |",
        "|---|---:|---:|---:|---:|",
    ]
    for cell in cells:
        lines.append(
            f"| {cell['arm_id']} | {cell['horizon_minutes']}m | "
            f"{cell['cohort_v1_average_net_after_cost_pips']:.3f} | "
            f"{cell['cohort_v2_average_net_after_cost_pips']:.3f} | "
            f"{str(cell['retrospective_sign_replication']).lower()} |"
        )
    md_text = "\n".join(lines) + "\n"
    for path, text in ((json_path, json_text), (md_path, md_text)):
        if path.exists() and path.read_text(encoding="utf-8") != text:
            raise RuntimeError(f"immutable_comparison_report_conflict:{path.name}")
        path.write_text(text, encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-root", type=Path, default=REPORT_ROOT)
    args = parser.parse_args()
    print(json.dumps(build(args.report_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
