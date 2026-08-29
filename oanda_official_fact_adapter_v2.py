#!/usr/bin/env python3
"""Publish the strict Clock-V2 OfficialFactAdapter research snapshot.

This command performs no network, broker, lifecycle, authorization, or order
operation.  It refuses Clock V1 and has no mutable event-clock fallback.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

try:
    from forex_system.ingestion.official_fact_adapter_v2 import OfficialFactAdapterV2
except ModuleNotFoundError:
    from src.forex_system.ingestion.official_fact_adapter_v2 import OfficialFactAdapterV2


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = (
    ROOT / "data" / "oanda_training_manager" / "state" / "official_fact_adapter_v2.json"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "official_fact_adapter_v2"
    / "OFFICIAL_FACT_ADAPTER_V2_CURRENT.md"
)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    class_counts = Counter(
        str(row.get("evidence_class") or "unknown") for row in snapshot["facts"]
    )
    type_counts = Counter(
        str(row.get("fact_type") or "unknown") for row in snapshot["facts"]
    )
    clock = dict(snapshot.get("event_clock_provenance") or {})
    lines = [
        "# Official Fact Adapter V2 - current diagnostic",
        "",
        f"Decision cutoff: `{snapshot['decision_cutoff_utc']}`",
        "",
        "Clock V2 point-in-time facts and clocks only. This artifact assigns no direction and cannot authorize a trade.",
        "",
        f"- Snapshot: `{snapshot['snapshot_id']}`",
        f"- Adapter contract: `{snapshot['adapter_contract_id']}`",
        f"- Clock contract: `{snapshot['immutable_event_clock_contract_id']}`",
        f"- Clock snapshot: `{clock.get('snapshot_id') or 'none'}`",
        f"- Facts / upcoming clocks: **{snapshot['fact_count']} / {snapshot['upcoming_event_count']}**",
        f"- Causal pre-release consensus observations: **{snapshot['causal_consensus_count']}**",
        f"- Supported execution decision: **{snapshot['supported_execution_decision']}**",
        "",
        "## Evidence classes",
        "",
        "| Class | Facts |",
        "|---|---:|",
    ]
    lines.extend(f"| {name} | {count} |" for name, count in sorted(class_counts.items()))
    lines.extend(["", "## Fact types", "", "| Fact type | Facts |", "|---|---:|"])
    lines.extend(f"| {name} | {count} |" for name, count in sorted(type_counts.items()))
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- Clock V1, malformed clocks, and mutable fallbacks are rejected.",
            "- Scheduled clocks are not directions.",
            "- Every normalized fact remains research-only and abstaining.",
            "- `no_trade` is the only supported execution decision.",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    decision_cutoff_utc: str | None = None,
    output_path: Path = DEFAULT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    cutoff = decision_cutoff_utc or dt.datetime.now(tz=dt.timezone.utc).isoformat()
    snapshot = OfficialFactAdapterV2().as_of(cutoff)
    atomic_text(output_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    atomic_text(report_path, render_markdown(snapshot))
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--decision-cutoff-utc")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    snapshot = run(
        decision_cutoff_utc=args.decision_cutoff_utc,
        output_path=args.output,
        report_path=args.report,
    )
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "fact_count": snapshot["fact_count"],
                "upcoming_event_count": snapshot["upcoming_event_count"],
                "adapter_contract_id": snapshot["adapter_contract_id"],
                "immutable_event_clock_contract_id": snapshot[
                    "immutable_event_clock_contract_id"
                ],
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
