#!/usr/bin/env python3
"""Publish CurrencyState official context through strict Clock/Adapter V2.

The command is research-only, reads an existing CurrencyState snapshot, and
writes only new V2-named diagnostic artifacts.  It has no broker or execution
imports and cannot fall back to Clock V1.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.contracts.currency_state import load_contract
    from forex_system.features.currency_state_official_context_v2 import (
        attach_official_fact_context_v2,
    )
    from forex_system.ingestion.official_fact_adapter_v2 import OfficialFactAdapterV2
except ModuleNotFoundError:
    from src.forex_system.contracts.currency_state import load_contract
    from src.forex_system.features.currency_state_official_context_v2 import (
        attach_official_fact_context_v2,
    )
    from src.forex_system.ingestion.official_fact_adapter_v2 import OfficialFactAdapterV2


ROOT = Path(__file__).resolve().parent
DEFAULT_BASE = (
    ROOT / "data" / "oanda_training_manager" / "state" / "currency_state_engine_v2.json"
)
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "currency_state_official_context_v2.json"
)
DEFAULT_FACT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "state"
    / "official_fact_adapter_currency_state_aligned_v2.json"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "currency_state_engine_v2"
    / "CURRENCY_STATE_OFFICIAL_CONTEXT_V2_CURRENT.md"
)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    context = snapshot["component_context"]
    lines = [
        "# CurrencyState + official context V2 - current diagnostic",
        "",
        f"Knowledge cutoff: `{snapshot['decision_cutoff_utc']}`",
        "",
        "CurrencyState and strict Clock-V2 official facts share one knowledge cutoff. Official facts remain unscored and supply no trade direction.",
        "",
        f"- Composite snapshot: `{snapshot['snapshot_id']}`",
        f"- Base state snapshot: `{snapshot['base_currency_state_snapshot_id']}`",
        f"- Official-fact snapshot: `{snapshot['official_fact_snapshot_id']}`",
        f"- Context contract: `{snapshot['component_context_contract_id']}`",
        f"- Clock contract: `{snapshot['immutable_event_clock_contract_id']}`",
        f"- Facts / upcoming clocks: **{context['fact_count']} / {context['upcoming_event_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**",
        "",
        "## Safety",
        "",
        "- Clock V1 and mutable event-clock fallback are rejected.",
        "- Official components remain unscored or unavailable.",
        "- Pair forecasts, expected net values, allocator ranks, and execution flags remain null/false.",
        "- `no_trade` is the only supported execution decision.",
        "",
    ]
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
    official = OfficialFactAdapterV2().as_of(base["decision_cutoff_utc"])
    composite = attach_official_fact_context_v2(base, official, contract=contract)
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
                "component_context_contract_id": snapshot[
                    "component_context_contract_id"
                ],
                "immutable_event_clock_contract_id": snapshot[
                    "immutable_event_clock_contract_id"
                ],
                "fact_count": snapshot["component_context"]["fact_count"],
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
