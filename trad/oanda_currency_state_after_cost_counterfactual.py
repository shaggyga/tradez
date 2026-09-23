#!/usr/bin/env python3
"""Publish the fail-closed CurrencyState after-cost research counterfactual.

The default run reads only local research artifacts.  With no frozen
economics envelope it emits the complete 8-arm x 68-pair x 5-horizon grid with
null EV/ranks and explicit blockers.  It has no broker, lifecycle,
authorization, supervisor, or execution adapter.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.research.currency_state_after_cost_counterfactual import (
        build_after_cost_counterfactual,
    )
    from forex_system.research.currency_state_response_timing_arms import (
        build_response_timing_arms,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.research.currency_state_after_cost_counterfactual import (
        build_after_cost_counterfactual,
    )
    from src.forex_system.research.currency_state_response_timing_arms import (
        build_response_timing_arms,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_COUNTERFACTUAL_CONFIG = ROOT / "config" / "currency_state_after_cost_counterfactual_v1.json"
DEFAULT_RESPONSE_CONFIG = ROOT / "config" / "currency_state_response_timing_arms_v1.json"
DEFAULT_STATE_CONFIG = ROOT / "config" / "currency_state_engine_v2.json"
DEFAULT_CONTEXT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
DEFAULT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_after_cost_counterfactual_v1.json"
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "currency_state_after_cost_counterfactual"
    / "CURRENCY_STATE_AFTER_COST_COUNTERFACTUAL_CURRENT.md"
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def artifact_fingerprints(
    *,
    counterfactual_config_path: Path,
    response_config_path: Path,
    state_config_path: Path,
) -> dict[str, str]:
    return {
        "counterfactual_module_sha256": file_sha256(
            ROOT / "src" / "forex_system" / "research" / "currency_state_after_cost_counterfactual.py"
        ),
        "counterfactual_config_file_sha256": file_sha256(counterfactual_config_path),
        "response_module_sha256": file_sha256(
            ROOT / "src" / "forex_system" / "research" / "currency_state_response_timing_arms.py"
        ),
        "response_config_file_sha256": file_sha256(response_config_path),
        "currency_state_module_sha256": file_sha256(
            ROOT / "src" / "forex_system" / "features" / "currency_state_engine.py"
        ),
        "currency_state_contract_file_sha256": file_sha256(state_config_path),
        "official_context_module_sha256": file_sha256(
            ROOT / "src" / "forex_system" / "features" / "currency_state_official_context.py"
        ),
    }


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    summary = snapshot["summary"]
    blocker_counts = Counter(summary.get("blocker_counts") or {})
    lines = [
        "# CurrencyState after-cost counterfactual - current research diagnostic",
        "",
        f"Generated: `{snapshot.get('generated_utc', 'not recorded')}`",
        "",
        "This is a shadow-only, fail-closed evidence artifact. It cannot authorize or place an order.",
        "",
        f"- Snapshot: `{snapshot['snapshot_id']}`",
        f"- Contract / cohort: `{snapshot['counterfactual_contract_id']}` / `{snapshot['counterfactual_cohort_id']}`",
        f"- Fingerprint manifest: `{snapshot['artifact_fingerprints']['fingerprint_manifest_id']}`",
        f"- Decision cutoff: `{snapshot['decision_cutoff_utc']}`",
        f"- Grid: **{snapshot['instrument_count']} pairs x {len(snapshot['horizons_sec'])} horizons x {len(snapshot['arm_ids'])} arms = {snapshot['row_count']} rows**",
        f"- Admissible economics / ranked / selectable: **{summary['economics_admissible_count']} / {summary['ranked_count']} / {summary['selectable_counterfactual_count']}**",
        f"- Top-one outputs / disjoint members: **{summary['top_one_available_count']} / {summary['disjoint_basket_member_count']}**",
        f"- Hold/switch comparisons: **{summary['hold_switch_comparison_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**",
        "",
        "## Current blockers",
        "",
        "| Blocker | Rows |",
        "|---|---:|",
    ]
    if blocker_counts:
        lines.extend(f"| `{name}` | {count} |" for name, count in sorted(blocker_counts.items()))
    else:
        lines.append("| none | 0 |")
    lines.extend(
        [
            "",
            "## Contract behavior",
            "",
            "- Observed price response is never admitted as a forward forecast.",
            "- A source-fact ID alone does not prove causal basis eligibility; an upstream proof-eligibility marker and contract fingerprint are required.",
            "- EV and rank require locked calibrated direction/cost-clear probabilities, favorable/adverse magnitude, executable bid/ask spread, slippage, latency, and rotation cost, all known by the cutoff.",
            "- Top-one and disjoint-basket results require positive after-cost value and the frozen cost-clearance threshold. Disjoint baskets cannot reuse a currency or signed factor.",
            "- Hold/switch is emitted only from a separately frozen, point-in-time hold-economics envelope.",
            "- Every record remains `no_trade`, `execution_eligible=false`, and `can_place_orders=false`.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    response_path: Path | None = None,
    context_path: Path = DEFAULT_CONTEXT,
    counterfactual_config_path: Path = DEFAULT_COUNTERFACTUAL_CONFIG,
    response_config_path: Path = DEFAULT_RESPONSE_CONFIG,
    state_config_path: Path = DEFAULT_STATE_CONFIG,
    economics_path: Path | None = None,
    hold_path: Path | None = None,
    output_path: Path = DEFAULT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    counterfactual_contract = json.loads(counterfactual_config_path.read_text(encoding="utf-8"))
    response_contract = json.loads(response_config_path.read_text(encoding="utf-8"))
    if response_path is None:
        official_context = json.loads(context_path.read_text(encoding="utf-8"))
        response = build_response_timing_arms(
            official_context,
            state_contract=load_contract(state_config_path),
            arm_contract=response_contract,
        )
    else:
        response = json.loads(response_path.read_text(encoding="utf-8"))
    economics = None if economics_path is None else json.loads(economics_path.read_text(encoding="utf-8"))
    holds = None if hold_path is None else json.loads(hold_path.read_text(encoding="utf-8"))
    snapshot = build_after_cost_counterfactual(
        response,
        contract=counterfactual_contract,
        artifact_fingerprints=artifact_fingerprints(
            counterfactual_config_path=counterfactual_config_path,
            response_config_path=response_config_path,
            state_config_path=state_config_path,
        ),
        economics_inputs=economics,
        hold_states=holds,
    )
    snapshot["generated_utc"] = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render_markdown(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--response", type=Path)
    parser.add_argument("--context", type=Path, default=DEFAULT_CONTEXT)
    parser.add_argument("--counterfactual-config", type=Path, default=DEFAULT_COUNTERFACTUAL_CONFIG)
    parser.add_argument("--response-config", type=Path, default=DEFAULT_RESPONSE_CONFIG)
    parser.add_argument("--state-config", type=Path, default=DEFAULT_STATE_CONFIG)
    parser.add_argument("--economics", type=Path)
    parser.add_argument("--hold", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    snapshot = run(
        response_path=args.response,
        context_path=args.context,
        counterfactual_config_path=args.counterfactual_config,
        response_config_path=args.response_config,
        state_config_path=args.state_config,
        economics_path=args.economics,
        hold_path=args.hold,
        output_path=args.output,
        report_path=args.report,
    )
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "counterfactual_cohort_id": snapshot["counterfactual_cohort_id"],
                "row_count": snapshot["row_count"],
                "economics_admissible_count": snapshot["summary"]["economics_admissible_count"],
                "ranked_count": snapshot["summary"]["ranked_count"],
                "selectable_counterfactual_count": snapshot["summary"]["selectable_counterfactual_count"],
                "fingerprint_manifest_id": snapshot["artifact_fingerprints"]["fingerprint_manifest_id"],
                "supported_execution_decision": snapshot["supported_execution_decision"],
                "output": str(args.output),
                "report": str(args.report),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
