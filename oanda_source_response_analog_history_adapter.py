#!/usr/bin/env python3
"""Generate the read-only source-response analog history coverage snapshot."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Mapping

from src.forex_system.ingestion.source_response_analog_history_adapter import (
    HistoryAdapterPaths,
    SourceResponseAnalogHistoryAdapter,
    load_json_object,
)


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORT_ROOT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "source_response_analog_history_adapter"
)
DEFAULT_OUTPUT = STATE / "source_response_analog_history_adapter_v1.json"
DEFAULT_REPORT = REPORT_ROOT / "SOURCE_RESPONSE_ANALOG_HISTORY_ADAPTER_CURRENT.md"


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _report(snapshot: Mapping[str, Any]) -> str:
    coverage = snapshot.get("coverage") or {}
    inputs = snapshot.get("input_states") or {}
    direct = inputs.get("direct_source_response") or {}
    exclusions = coverage.get("excluded_count_by_reason") or {}
    degradations = coverage.get("mapped_event_degradation_count_by_reason") or {}
    lines = [
        "# Source-Response Analog Historical Adapter",
        "",
        f"- Snapshot: `{snapshot.get('snapshot_id')}`",
        f"- Decision cutoff: `{snapshot.get('decision_cutoff_utc')}`",
        f"- Adapter contract: `{snapshot.get('adapter_contract_id')}`",
        f"- Selector contract: `{snapshot.get('selector_contract_id')}`",
        "- Safety: **research-only / no-trade / execution-ineligible**",
        "",
        "## Mapped coverage",
        "",
        f"- Selector-ready official events: **{int(coverage.get('mapped_event_count') or 0):,}**",
        f"- Events with exact-clock outcomes: **{int(coverage.get('mapped_event_with_outcome_count') or 0):,}**",
        f"- Exact-clock immutable outcomes: **{int(coverage.get('mapped_outcome_count') or 0):,}**",
        f"- Events with causal pre-release market consensus: **{int(coverage.get('mapped_event_with_causal_consensus_count') or 0):,}**",
        f"- Events with numeric intraday rate repricing: **{int(coverage.get('mapped_event_with_numeric_intraday_rate_repricing_count') or 0):,}**",
        "",
        "The mapped outcomes come only from immutable `macro_reaction_samples`: "
        "`quote_time_utc` is maturity and `sampled_utc` is the exact outcome-known clock. "
        "No stored historical result is used to rank analogs.",
        "",
        "## Deliberately withheld evidence",
        "",
        f"- Direct-response matured rows withheld for missing exact outcome-known timestamp: **{int(direct.get('withheld_matured_outcome_count') or 0):,}**",
        "- Stored macro z-scores remain null because their scale was not proven frozen before release.",
        "- Revision-table consensus remains null unless a matching verified pre-release consensus observation exists.",
        "- Official daily rates remain context only; they are not relabeled as intraday rate repricing.",
        "- Outcome-selected movement episodes remain miss-analysis evidence only; they cannot define an unbiased analog candidate universe.",
        "",
        "## Exclusion counts",
        "",
        "| Reason | Count |",
        "|---|---:|",
    ]
    if exclusions:
        for reason, count in sorted(exclusions.items()):
            lines.append(f"| `{reason}` | {int(count):,} |")
    else:
        lines.append("| none | 0 |")
    lines.extend(["", "## Mapped-event degradation counts", "", "| Reason | Count |", "|---|---:|"])
    if degradations:
        for reason, count in sorted(degradations.items()):
            lines.append(f"| `{reason}` | {int(count):,} |")
    else:
        lines.append("| none | 0 |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This is an engineering coverage result, not evidence of edge. The adapter may support "
            "historical analog selection only after the selector freezes feature-only ranking at a later "
            "decision cutoff. It cannot promote, authorize, route, or place an order. The only supported "
            "execution decision is `no_trade`.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--decision-cutoff-utc",
        default=dt.datetime.now(dt.timezone.utc).isoformat(),
        help="Exact timezone-aware knowledge cutoff (default: current UTC).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "config" / "source_response_analog_history_adapter_v1.json",
    )
    parser.add_argument(
        "--selector-contract",
        type=Path,
        default=ROOT / "config" / "source_response_analog_selector_v1.json",
    )
    parser.add_argument("--macro-db", type=Path, default=STATE / "macro_surprise_v1.sqlite")
    parser.add_argument(
        "--source-governance-db", type=Path, default=STATE / "source_governance_v1.sqlite"
    )
    parser.add_argument(
        "--direct-source-response-db",
        type=Path,
        default=STATE / "direct_source_response_v1.sqlite",
    )
    parser.add_argument(
        "--daily-rates-db", type=Path, default=STATE / "official_daily_rate_context_v1.sqlite"
    )
    parser.add_argument(
        "--movement-episode-db",
        type=Path,
        default=STATE / "movement_news_episode_research_v1.sqlite",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = HistoryAdapterPaths(
        macro_surprise_db=args.macro_db,
        source_governance_db=args.source_governance_db,
        direct_source_response_db=args.direct_source_response_db,
        daily_rates_db=args.daily_rates_db,
        movement_episode_db=args.movement_episode_db,
    )
    adapter = SourceResponseAnalogHistoryAdapter(
        paths=paths,
        config=load_json_object(args.config),
        selector_contract=load_json_object(args.selector_contract),
    )
    snapshot = adapter.build_snapshot(decision_cutoff_utc=args.decision_cutoff_utc)
    _atomic_write(args.output, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    _atomic_write(args.report, _report(snapshot))
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "decision_cutoff_utc": snapshot["decision_cutoff_utc"],
                "coverage": snapshot["coverage"],
                "output": str(args.output),
                "report": str(args.report),
                "supported_execution_decision": "no_trade",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
