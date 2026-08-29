#!/usr/bin/env python3
"""Declared-horizon context companion for causal V6 mover cases."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Mapping

import oanda_live_move_news_snapshot_v6 as live_v6
import oanda_live_move_persistent_news_context as base
import oanda_major_move_gap_census as census


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_SNAPSHOT = STATE / "live_move_news_snapshot_v6r2.json"
DEFAULT_SOURCES = STATE / "source_governance_v1.sqlite"
DEFAULT_OUTPUT = STATE / "live_move_persistent_news_context_v3r2.json"
DEFAULT_REPORT = REPORTS / "live_move_news" / "LIVE_MOVE_PERSISTENT_CONTEXT_V3R2.md"
CONTRACT_ID = "live_move_persistent_news_context_v3r2_precise_start_clock_20260827"


def run(
    *,
    snapshot_path: Path = DEFAULT_SNAPSHOT,
    source_database: Path = DEFAULT_SOURCES,
    output_path: Path = DEFAULT_OUTPUT,
    report_path: Path = DEFAULT_REPORT,
) -> dict[str, Any]:
    source = base.read_json(snapshot_path)
    movers = [row for row in source.get("movers") or [] if isinstance(row, Mapping)]
    starts = [census.parse_epoch(row.get("start_utc")) for row in movers]
    valid = [int(value) for value in starts if value is not None]
    lower = (
        min(valid) - base.MAXIMUM_LOOKBACK_MINUTES * 60
        if valid
        else int(time.time())
    )
    upper = max(valid) if valid else int(time.time())
    source_index, highwater = census.load_source_index(
        source_database,
        minimum_effective_epoch=lower,
        maximum_effective_epoch=upper,
    )
    rows = [base.enrich_case(row, source_index) for row in movers]
    output = {
        "schema_version": 1,
        "contract_id": CONTRACT_ID,
        "generated_utc": base.utc_now(),
        "input_snapshot_contract_id": source.get("contract_id"),
        "expected_input_snapshot_contract_id": live_v6.CONTRACT_ID,
        "input_snapshot_generated_utc": source.get("generated_utc"),
        "input_narrative_join_contract": source.get("narrative_join_contract"),
        "source_history_highwater_utc": highwater,
        "maximum_lookback_minutes": base.MAXIMUM_LOOKBACK_MINUTES,
        "mover_count": len(rows),
        "movers": rows,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "can_promote": False,
        "supported_decision": "diagnostic_only",
    }
    live_v6.v5.atomic_json(output_path, output)
    live_v6.v5.atomic_text(report_path, base.render(output))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        result = run(
            snapshot_path=args.snapshot,
            source_database=args.sources,
            output_path=args.output,
            report_path=args.report,
        )
        print(
            json.dumps(
                {
                    "generated_utc": result["generated_utc"],
                    "mover_count": result["mover_count"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if args.interval_sec <= 0:
            return 0
        if args.duration_sec > 0 and time.monotonic() - started >= args.duration_sec:
            return 0
        time.sleep(max(1.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
