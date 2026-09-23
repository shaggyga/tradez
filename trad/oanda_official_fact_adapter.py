#!/usr/bin/env python3
"""Write the read-only point-in-time official-fact research snapshot.

This entrypoint performs no network or broker calls.  It reads bounded local
ledgers through ``OfficialFactAdapter`` and publishes a diagnostic state/report
pair.  Facts remain unscored and cannot authorize a trade.
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
    from forex_system.ingestion.official_fact_adapter import OfficialFactAdapter
except ModuleNotFoundError:
    from src.forex_system.ingestion.official_fact_adapter import OfficialFactAdapter


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = (
    ROOT / "data" / "oanda_training_manager" / "state" / "official_fact_adapter_v1.json"
)
DEFAULT_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "official_fact_adapter"
    / "OFFICIAL_FACT_ADAPTER_CURRENT.md"
)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    class_counts = Counter(str(row.get("evidence_class") or "unknown") for row in snapshot["facts"])
    type_counts = Counter(str(row.get("fact_type") or "unknown") for row in snapshot["facts"])
    event_clock = snapshot.get("event_clock_provenance") or {}
    lines = [
        "# Official Fact Adapter - current diagnostic",
        "",
        f"Decision cutoff: `{snapshot['decision_cutoff_utc']}`",
        "",
        "Point-in-time official facts and clocks. This artifact assigns no currency direction and cannot authorize a trade.",
        "",
        f"- Snapshot: `{snapshot['snapshot_id']}`",
        f"- Adapter contract: `{snapshot['adapter_contract_id']}`",
        f"- Currencies: **{snapshot['currency_count']}**",
        f"- Facts / upcoming clocks: **{snapshot['fact_count']} / {snapshot['upcoming_event_count']}**",
        f"- Event-clock provenance: **{event_clock.get('state', 'unavailable')}**",
        f"- Immutable event-clock snapshot: `{event_clock.get('snapshot_id') or 'none'}`",
        f"- Causal pre-release consensus observations: **{snapshot['causal_consensus_count']}**",
        f"- Quarantined source events excluded: **{snapshot['quarantined_source_event_count']}**",
        f"- Intraday rate repricing connected: **{str(bool(snapshot['intraday_rates'].get('connected'))).lower()}**",
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
            "## Currency completeness",
            "",
            "| Currency | Facts | Causal consensus | Upcoming | Missing or degraded |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for currency, row in sorted(snapshot["currency_evidence"].items()):
        lines.append(
            f"| {currency} | {row['fact_count']} | {row['causal_consensus_count']} | "
            f"{row['upcoming_event_count']} | {', '.join(row['missing_or_degraded']) or 'none'} |"
        )
    lines.extend(["", "## Global blockers", ""])
    for row in snapshot["global_gaps"]:
        lines.append(f"- `{row['code']}`")
    if not snapshot["global_gaps"]:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `prospective_causal` means the official fact itself was known point-in-time; it does not mean its currency direction or trading value is proven.",
            "- Internal expectations are retained as non-consensus research baselines.",
            "- Noncausal consensus and surprise values are segregated and cannot enter a causal feature.",
            "- Daily rates remain context. They are not intraday policy-path repricing.",
            "- Mutable event-preflight fallback is labeled explicitly and is not historical proof.",
            "- Every fact has `direction_policy=abstain`; execution and authorization remain disabled.",
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
    snapshot = OfficialFactAdapter().as_of(cutoff)
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
                "causal_consensus_count": snapshot["causal_consensus_count"],
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
