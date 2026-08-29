#!/usr/bin/env python3
"""Forward outcomes for the causal V6 live mover/news cases.

This is a separate prospective evidence contract and database.  The V1/V5
case and outcome history remains untouched.  All arms are research-only and
cannot authorize or route an order.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import oanda_live_move_news_outcomes as base
import oanda_live_move_news_snapshot_v6 as live_v6


DATA = Path(__file__).resolve().parent / "data" / "oanda_training_manager"
STATE = DATA / "state"
DEFAULT_DATABASE = STATE / "live_move_news_cases_v6r2.sqlite"
DEFAULT_CANDLES = DATA / "candles"
DEFAULT_OUTPUT = STATE / "live_move_news_outcomes_v2r2.json"
DEFAULT_REPORT = (
    DATA / "reports" / "live_move_news" / "LIVE_MOVE_NEWS_OUTCOMES_CURRENT_V2R2.md"
)
CONTRACT_ID = "live_move_news_forward_outcomes_v2r2_precise_start_clock_20260827"
CASE_CONTRACT_ID = live_v6.CONTRACT_ID


def run(
    *,
    database: Path = DEFAULT_DATABASE,
    candle_root: Path = DEFAULT_CANDLES,
    output: Path = DEFAULT_OUTPUT,
    report: Path = DEFAULT_REPORT,
):
    return base.run(
        database=database,
        candle_root=candle_root,
        output=output,
        report=report,
        case_contract_id=CASE_CONTRACT_ID,
        outcome_contract_id=CONTRACT_ID,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--candles", type=Path, default=DEFAULT_CANDLES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.monotonic()
    while True:
        payload = run(
            database=args.database,
            candle_root=args.candles,
            output=args.output,
            report=args.report,
        )
        print(
            json.dumps(
                {
                    "generated_utc": payload["generated_utc"],
                    "case_count": payload["case_count"],
                    "retained_outcome_count": payload["retained_outcome_count"],
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
