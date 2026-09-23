#!/usr/bin/env python3
"""Backfill OANDA S5 bid/ask candles without running downstream model training."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import oanda_gpt_training_strategy_manager as manager
import oanda_s5_microstructure_pipeline as s5_pipeline


def parse_pairs(value: str) -> list[str]:
    pairs = sorted({item.strip().upper().replace("/", "_") for item in value.split(",") if item.strip()})
    if not pairs:
        raise argparse.ArgumentTypeError("provide one or more comma-separated instruments")
    return pairs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--pairs", type=parse_pairs, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=s5_pipeline.S5_ROOT,
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=manager.DIRS["reports"] / "latest_s5_backfill_only_summary.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manager.ensure_dirs()
    s5_pipeline.S5_ROOT = args.output_dir.resolve()
    s5_pipeline.S5_ROOT.mkdir(parents=True, exist_ok=True)
    training_manager = manager.TrainingStrategyManager()
    summaries = []
    for index, instrument in enumerate(args.pairs, start=1):
        print(f"[s5-backfill-only] {index}/{len(args.pairs)} {instrument}", flush=True)
        summaries.append(
            s5_pipeline.backfill_s5(training_manager.client, instrument, args.days)
        )
    payload = {
        "generated_utc": manager.iso_utc(),
        "days": args.days,
        "instrument_count": len(args.pairs),
        "output_dir": str(s5_pipeline.S5_ROOT),
        "instruments": summaries,
    }
    manager.save_json(args.summary, payload)
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
