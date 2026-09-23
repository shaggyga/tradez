#!/usr/bin/env python3
"""Attach point-in-time official facts to the research CurrencyState snapshot.

The composite remains diagnostic-only.  It reads the already-generated local
CurrencyState artifact, reconstructs official knowledge at that exact cutoff,
and writes an unscored context snapshot.  It performs no broker/network calls.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.features.currency_state_official_context import (
        attach_official_fact_context,
    )
    from forex_system.ingestion.official_fact_adapter import OfficialFactAdapter
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.features.currency_state_official_context import (
        attach_official_fact_context,
    )
    from src.forex_system.ingestion.official_fact_adapter import OfficialFactAdapter


ROOT = Path(__file__).resolve().parent
DEFAULT_BASE = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_engine_v2.json"
DEFAULT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_official_context_v1.json"
DEFAULT_FACT_OUTPUT = ROOT / "data" / "oanda_training_manager" / "state" / "official_fact_adapter_currency_state_aligned_v1.json"
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "currency_state_engine"
    / "CURRENCY_STATE_OFFICIAL_CONTEXT_CURRENT.md"
)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    context = snapshot["component_context"]
    lines = [
        "# Currency State + official context - current diagnostic",
        "",
        f"Knowledge cutoff: `{snapshot['decision_cutoff_utc']}`",
        "",
        "The price-response state and official-fact state share one knowledge cutoff. Official facts remain unscored and supply no trade direction.",
        "",
        f"- Composite snapshot: `{snapshot['snapshot_id']}`",
        f"- Base state snapshot: `{snapshot['base_currency_state_snapshot_id']}`",
        f"- Official-fact snapshot: `{snapshot['official_fact_snapshot_id']}`",
        f"- Facts / upcoming clocks: **{context['fact_count']} / {context['upcoming_event_count']}**",
        f"- Causal consensus observations: **{context['causal_consensus_count']}**",
        f"- Intraday rate repricing connected: **{str(context['intraday_rates_connected']).lower()}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**",
        "",
        "| Currency | Facts | Fact types | Evidence classes | Causal consensus | Upcoming | Missing/degraded |",
        "|---|---:|---|---|---:|---:|---|",
    ]
    for currency, row in sorted(context["currency_summary"].items()):
        fact_types = ", ".join(
            f"{name}:{count}" for name, count in row["fact_type_counts"].items()
        ) or "none"
        classes = ", ".join(
            f"{name}:{count}" for name, count in row["evidence_class_counts"].items()
        ) or "none"
        lines.append(
            f"| {currency} | {row['fact_count']} | {fact_types} | {classes} | "
            f"{row['causal_consensus_count']} | {row['upcoming_event_count']} | "
            f"{', '.join(row['missing_or_degraded']) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- All official components are `available_unscored`, context-only, or unavailable.",
            "- A semantic direction present in an upstream payload is not copied into a forecast field.",
            "- Every pair forecast, cost-clear probability, expected net value, allocator rank, and execution flag remains null/false.",
            "- `no_trade` remains the only supported execution decision.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    base_path: Path = DEFAULT_BASE,
    output_path: Path = DEFAULT_OUTPUT,
    fact_output_path: Path = DEFAULT_FACT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    base = json.loads(base_path.read_text(encoding="utf-8"))
    contract = load_contract()
    official = OfficialFactAdapter().as_of(base["decision_cutoff_utc"])
    composite = attach_official_fact_context(base, official, contract=contract)
    atomic_text(fact_output_path, json.dumps(official, indent=2, sort_keys=True) + "\n")
    atomic_text(output_path, json.dumps(composite, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render_markdown(composite))
    return composite


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, default=DEFAULT_BASE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--fact-output", type=Path, default=DEFAULT_FACT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    snapshot = run(
        base_path=args.base,
        output_path=args.output,
        fact_output_path=args.fact_output,
        report_path=args.report,
    )
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "base_snapshot_id": snapshot["base_currency_state_snapshot_id"],
                "official_fact_snapshot_id": snapshot["official_fact_snapshot_id"],
                "fact_count": snapshot["component_context"]["fact_count"],
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
