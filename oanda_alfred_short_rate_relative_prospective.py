#!/usr/bin/env python3
"""Collect the locked ALFRED short-rate-change H24 hypothesis in shadow."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import oanda_alfred_unemployment_relative_prospective as relative


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG = ROOT / "config" / "alfred_short_rate_relative_prospective_v1.json"
DATABASE = STATE / "alfred_short_rate_relative_prospective_v1.sqlite"
OUTPUT = STATE / "alfred_short_rate_relative_prospective_v1.json"
REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "macro_relative_strength"
    / "ALFRED_SHORT_RATE_RELATIVE_PROSPECTIVE_CURRENT.md"
)
SOURCE_DATABASE = STATE / "alfred_short_rate_prospective_v1.sqlite"
SOURCE_STATE = STATE / "alfred_short_rate_prospective_v1.json"
SOURCE_CONFIG = ROOT / "config" / "alfred_short_rate_prospective_v1.json"


def run_once():
    return relative.run_once(
        config_path=CONFIG,
        database_path=DATABASE,
        output_path=OUTPUT,
        report_path=REPORT,
        source_database_path=SOURCE_DATABASE,
        source_state_path=SOURCE_STATE,
        source_config_path=SOURCE_CONFIG,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--duration-sec", type=float, default=604800)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec
    while True:
        run_once()
        if args.once or time.monotonic() >= stop:
            return 0
        time.sleep(min(args.interval_sec, max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
