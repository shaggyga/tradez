#!/usr/bin/env python3
"""Fit exit recommendations outside the latency-sensitive strategy process."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

try:
    from oanda_strategy_exit_fit import StrategyExitFit
except ModuleNotFoundError:
    from trad.oanda_strategy_exit_fit import StrategyExitFit


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager" / "state"


def read_cursor(path: Path) -> int | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    value = payload.get("source_row_id") if isinstance(payload, dict) else None
    return int(value) if value is not None else None


def write_cursor(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def sync_shadow_outcomes(
    fitter: StrategyExitFit,
    source_path: Path,
    cursor_path: Path,
    *,
    batch_rows: int,
    max_batches: int,
) -> dict[str, Any]:
    if not source_path.is_file():
        return {"status": "missing_source", "inserted": 0, "cursor": 0, "source_max_row": 0}
    source = sqlite3.connect(source_path, timeout=120.0)
    source.execute("PRAGMA query_only=ON")
    source.execute("PRAGMA busy_timeout=120000")
    try:
        source_max_row = int(
            source.execute("SELECT COALESCE(MAX(row_id), 0) FROM outcomes").fetchone()[0]
        )
        cursor = read_cursor(cursor_path)
        if cursor is None:
            write_cursor(
                cursor_path,
                {
                    "source_row_id": source_max_row,
                    "source_max_row": source_max_row,
                    "status": "bootstrapped_to_existing_tail",
                },
            )
            return {
                "status": "bootstrapped_to_existing_tail",
                "inserted": 0,
                "cursor": source_max_row,
                "source_max_row": source_max_row,
            }
        source_columns = {
            str(row[1]) for row in source.execute("PRAGMA table_info(outcomes)")
        }

        def optional_column(name: str, fallback: str) -> str:
            return name if name in source_columns else f"{fallback} AS {name}"

        model_column = optional_column("model_id", "''")
        timeframe_column = optional_column("input_timeframe", "''")
        spread_column = optional_column("entry_spread_pips", "0")
        first_positive_column = optional_column("first_positive_sec", "NULL")
        positive_samples_column = optional_column("positive_path_samples", "0")
        before = fitter.connection.total_changes if fitter.connection is not None else 0
        batches = 0
        while cursor < source_max_row and batches < max_batches:
            rows = source.execute(
                f"""
                SELECT row_id, event_id, horizon_sec, lane_id, family, profile,
                       {model_column}, {timeframe_column}, kind, instrument,
                       direction, entry_time, exit_time, theoretical_pips,
                       {spread_column}, max_favorable_pips, max_adverse_pips,
                       {first_positive_column}, path_samples,
                       {positive_samples_column}, diagnostics_json
                FROM outcomes
                WHERE row_id > ? AND kind = 'signal'
                ORDER BY row_id
                LIMIT ?
                """,
                (cursor, batch_rows),
            ).fetchall()
            if not rows:
                cursor = source_max_row
                break
            for row in rows:
                try:
                    diagnostics = json.loads(row[20] or "{}")
                except json.JSONDecodeError:
                    diagnostics = {}
                fitter.observe(
                    event_id=row[1],
                    horizon_sec=row[2],
                    lane_id=row[3],
                    family=row[4],
                    profile=row[5],
                    model_id=row[6],
                    input_timeframe=row[7],
                    kind=row[8],
                    instrument=row[9],
                    direction=row[10],
                    entry_time=row[11],
                    exit_time=row[12],
                    endpoint_pips=row[13],
                    entry_spread_pips=row[14],
                    max_favorable_pips=row[15],
                    max_adverse_pips=row[16],
                    first_positive_sec=row[17],
                    path_samples=row[18],
                    positive_path_samples=row[19],
                    favorable_hits=diagnostics.get("favorable_hits") or {},
                    adverse_hits=diagnostics.get("adverse_hits") or {},
                    volatility_regime=diagnostics.get("volatility_regime") or "",
                )
            fitter.flush()
            cursor = int(rows[-1][0])
            batches += 1
            write_cursor(
                cursor_path,
                {
                    "source_row_id": cursor,
                    "source_max_row": source_max_row,
                    "status": "syncing" if cursor < source_max_row else "current",
                },
            )
        inserted = (
            fitter.connection.total_changes - before if fitter.connection is not None else 0
        )
        return {
            "status": "current" if cursor >= source_max_row else "syncing",
            "inserted": max(0, inserted),
            "cursor": cursor,
            "source_max_row": source_max_row,
            "batches": batches,
        }
    finally:
        source.close()


def parse_horizons(value: str) -> list[int]:
    horizons = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if not horizons or min(horizons) <= 0:
        raise argparse.ArgumentTypeError("horizons must be positive")
    return horizons


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_ROOT / "strategy_exit_fit_v1.sqlite")
    parser.add_argument("--state", type=Path, default=DEFAULT_ROOT / "strategy_exit_fit_v1.json")
    parser.add_argument(
        "--source-shadow-database",
        type=Path,
        default=DEFAULT_ROOT / "strategy_shadow_outcomes_v1.sqlite",
    )
    parser.add_argument(
        "--cursor-state",
        type=Path,
        default=DEFAULT_ROOT / "strategy_exit_fit_worker_cursor_v1.json",
    )
    parser.add_argument(
        "--horizons",
        type=parse_horizons,
        default=parse_horizons("60,120,180,300,600,900,1800,3600,7200,10800,14400"),
    )
    parser.add_argument("--ranking-horizon-sec", type=int, default=300)
    parser.add_argument("--max-fit-rows", type=int, default=50000)
    parser.add_argument("--sync-batch-rows", type=int, default=20000)
    parser.add_argument("--sync-max-batches", type=int, default=4)
    parser.add_argument("--interval-sec", type=float, default=900.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if min(
        args.ranking_horizon_sec,
        args.max_fit_rows,
        args.sync_batch_rows,
        args.sync_max_batches,
    ) <= 0 or args.interval_sec <= 0.0:
        raise SystemExit("ranking horizon, fit rows, and interval must be positive")
    return args


def main() -> int:
    args = parse_args()
    fitter = StrategyExitFit(
        args.database,
        args.state,
        horizon_sec=args.ranking_horizon_sec,
        horizons_sec=args.horizons,
        max_fit_rows=args.max_fit_rows,
        fit_enabled=False,
    )
    try:
        while True:
            started = time.monotonic()
            sync = sync_shadow_outcomes(
                fitter,
                args.source_shadow_database,
                args.cursor_state,
                batch_rows=args.sync_batch_rows,
                max_batches=args.sync_max_batches,
            )
            state = fitter.fit()
            counts = state.get("counts") or {}
            print(
                f"exit fit rows={counts.get('fit_signal_rows', 0)} "
                f"eligible={counts.get('eligible_recommendations', 0)} "
                f"sync={sync.get('status')} inserted={sync.get('inserted', 0)} "
                f"elapsed_sec={time.monotonic() - started:.3f}",
                flush=True,
            )
            if args.once:
                return 0
            time.sleep(args.interval_sec)
    finally:
        fitter.close()


if __name__ == "__main__":
    raise SystemExit(main())
