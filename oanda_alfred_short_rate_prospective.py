#!/usr/bin/env python3
"""Run the frozen ALFRED/OECD short-rate source as an independent cohort."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import oanda_alfred_vintage_prospective as collector


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
CONFIG = ROOT / "config" / "alfred_short_rate_prospective_v1.json"
DATABASE = STATE / "alfred_short_rate_prospective_v1.sqlite"
OUTPUT = STATE / "alfred_short_rate_prospective_v1.json"
REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "alfred_vintages"
    / "ALFRED_SHORT_RATE_PROSPECTIVE_CURRENT.md"
)


def run_once():
    return collector.collect_once(CONFIG, DATABASE, OUTPUT, REPORT)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=float, default=21600)
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
