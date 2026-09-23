#!/usr/bin/env python3
"""Incrementally backfill and fit persistent multi-horizon lane promotion state."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

try:
    from oanda_lane_promotion import LanePromotionModel, LanePromotionStore, PromotionThresholds
except ImportError:  # pragma: no cover - package execution
    from trad.oanda_lane_promotion import LanePromotionModel, LanePromotionStore, PromotionThresholds


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_DIR = PROJECT_ROOT / "data" / "oanda_training_manager" / "state"
FULL_HORIZONS_SEC = (
    15,
    30,
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    10800,
    14400,
)


def parse_horizons(value: str) -> tuple[int, ...]:
    horizons = tuple(sorted({int(item.strip()) for item in value.split(",") if item.strip()}))
    if not horizons or any(value <= 0 for value in horizons):
        raise argparse.ArgumentTypeError("horizons must contain positive seconds")
    return horizons


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path, default=DEFAULT_STATE_DIR / "strategy_exit_fit_v1.sqlite")
    parser.add_argument("--database", type=Path, default=DEFAULT_STATE_DIR / "lane_promotion_v1.sqlite")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_DIR / "lane_promotion_v1.json")
    parser.add_argument("--additional-database", type=Path)
    parser.add_argument("--horizons", type=parse_horizons, default=FULL_HORIZONS_SEC)
    parser.add_argument("--interval-sec", type=float, default=900.0)
    parser.add_argument("--chunk-rows", type=int, default=100000)
    parser.add_argument("--max-chunks", type=int, default=20)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--min-samples", type=int, default=30)
    parser.add_argument("--min-average-pips", type=float, default=0.10)
    parser.add_argument("--min-median-pips", type=float, default=0.0)
    parser.add_argument("--min-win-rate", type=float, default=52.0)
    parser.add_argument("--min-lower-confidence-pips", type=float, default=0.0)
    parser.add_argument("--min-independent-blocks", type=int, default=12)
    parser.add_argument("--min-holdout-blocks", type=int, default=4)
    parser.add_argument("--min-pairs", type=int, default=3)
    parser.add_argument("--min-sessions", type=int, default=2)
    parser.add_argument("--min-segment-samples", type=int, default=10)
    parser.add_argument(
        "--allow-state-regression",
        action="store_true",
        help="allow an intentional narrower promotion state to replace the incumbent",
    )
    args = parser.parse_args(argv)
    if args.interval_sec <= 0 or args.chunk_rows <= 0 or args.max_chunks <= 0:
        raise SystemExit("interval, chunk rows, and max chunks must be positive")
    return args


def fit_cycle(args: argparse.Namespace) -> dict[str, object]:
    store = LanePromotionStore(args.database)
    try:
        progress = store.backfill_from(
            args.source_database,
            args.horizons,
            chunk_rows=args.chunk_rows,
            max_chunks=args.max_chunks,
        )
        additional = (
            store.sync_from_compact(
                args.additional_database,
                args.horizons,
                cursor_key="second_forecast_source_cursor",
                chunk_rows=args.chunk_rows,
                max_chunks=args.max_chunks,
            )
            if args.additional_database is not None
            else {"source_complete": True, "inserted": 0}
        )
    finally:
        store.close()
    model = LanePromotionModel(
        args.database,
        args.state,
        args.horizons,
        thresholds=PromotionThresholds(
            min_samples=args.min_samples,
            min_average_pips=args.min_average_pips,
            min_median_pips=args.min_median_pips,
            min_win_rate=args.min_win_rate,
            min_lower_confidence_pips=args.min_lower_confidence_pips,
            min_independent_blocks=args.min_independent_blocks,
            min_holdout_blocks=args.min_holdout_blocks,
            min_pairs=args.min_pairs,
            min_sessions=args.min_sessions,
            min_segment_samples=args.min_segment_samples,
        ),
        source_complete=bool(progress.get("source_complete") and additional.get("source_complete")),
        source_metadata={**progress, "additional": additional},
        allow_state_regression=args.allow_state_regression,
    )
    return model.fit()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    while True:
        started = time.monotonic()
        failed = False
        try:
            state = fit_cycle(args)
            print(
                json.dumps(
                    {
                        "generated_at": state.get("generated_at"),
                        "status": state.get("status"),
                        "raw_rows": state.get("raw_rows"),
                        "evidence_count": state.get("evidence_count"),
                        "eligible_count": state.get("eligible_count"),
                        "backfill": state.get("backfill"),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        except Exception as exc:
            failed = True
            print(json.dumps({"error": type(exc).__name__, "message": str(exc)[:500]}), flush=True)
            if args.once:
                raise
        if args.once:
            return 0
        delay = min(60.0, args.interval_sec) if failed else args.interval_sec
        time.sleep(max(1.0, delay - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
