#!/usr/bin/env python3
"""Publish the current research-only CurrencyState response/timing arms.

The default run deliberately supplies no external thesis, technical, or
magnitude envelopes.  It therefore documents the complete 68-pair x
five-horizon arm grid while every causal macro arm abstains.  This entrypoint
performs no network, broker, lifecycle, authorization, supervisor, or
execution work.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.research.currency_state_response_timing_arms import (
        build_response_timing_arms,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.research.currency_state_response_timing_arms import (
        build_response_timing_arms,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "currency_state_official_context_v1.json"
)
DEFAULT_CONTRACT = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "currency_state_response_timing_arms_v1.json"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "currency_state_response_timing_arms"
    / "CURRENCY_STATE_RESPONSE_TIMING_ARMS_CURRENT.md"
)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _arm_count(arm: Mapping[str, Any], key: str) -> int:
    return sum(bool(row.get(key)) for row in arm.get("records") or [])


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    lines = [
        "# Currency State response/timing arms - current research snapshot",
        "",
        f"Decision cutoff: `{snapshot['decision_cutoff_utc']}`",
        "",
        "Eight mutually labeled research arms over the complete pair/horizon grid. This artifact cannot authorize or place an order.",
        "",
        f"- Snapshot: `{snapshot['snapshot_id']}`",
        f"- Contract: `{snapshot['arm_contract_id']}`",
        f"- Fact-basis eligibility contract: `{snapshot['fact_basis_eligibility_contract_id']}`",
        f"- Pair/horizon cells per arm: **{snapshot['edge_horizon_count']}**",
        f"- Input rejections: **{len(snapshot.get('input_rejections') or [])}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**",
        "",
        "## Basis-eligible official facts",
        "",
        "| Claimed basis | Eligible facts |",
        "|---|---:|",
    ]
    for basis, count in sorted((snapshot.get("basis_eligible_fact_counts") or {}).items()):
        lines.append(f"| {basis} | {count} |")
    lines.extend(
        [
        "",
        "| Arm | Role | Cells | Paper candidates | Abstain | Execution eligible |",
        "|---|---|---:|---:|---:|---:|",
        ]
    )
    for arm_id, arm in snapshot["arms"].items():
        lines.append(
            f"| {arm_id} | {arm.get('role', '')} | {arm['record_count']} | "
            f"{_arm_count(arm, 'paper_candidate')} | "
            f"{sum(str(row.get('research_direction') or 'abstain') == 'abstain' for row in arm['records'])} | "
            f"{_arm_count(arm, 'execution_eligible')} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The observed-price arm is a hindsight-freeze engineering control, not a forward forecast.",
            "- A technical record may confirm, veto, delay, or identify conflict; it cannot invent or reverse an official thesis.",
            "- Official thesis rows require an allowlisted basis, official fact IDs, and pair-consistent signed currency-factor IDs.",
            "- Missing calibrated direction, magnitude, slippage, latency, or rotation economics remains unavailable rather than imputed.",
            "- Every record remains research-only, execution-ineligible, and `no_trade`.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    input_path: Path = DEFAULT_INPUT,
    contract_path: Path = DEFAULT_CONTRACT,
    output_path: Path = DEFAULT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    source = json.loads(input_path.read_text(encoding="utf-8"))
    arm_contract = json.loads(contract_path.read_text(encoding="utf-8"))
    snapshot = build_response_timing_arms(
        source,
        state_contract=load_contract(),
        arm_contract=arm_contract,
    )
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render_markdown(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    snapshot = run(
        input_path=args.input,
        contract_path=args.contract,
        output_path=args.output,
        report_path=args.report,
    )
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "arm_count": snapshot["arm_count"],
                "cells_per_arm": snapshot["edge_horizon_count"],
                "execution_eligible": snapshot["execution_eligible"],
                "supported_execution_decision": snapshot[
                    "supported_execution_decision"
                ],
                "output": str(args.output),
                "report": str(args.report),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
