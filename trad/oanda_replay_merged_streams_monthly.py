#!/usr/bin/env python3
"""Replay monthly OANDA-style results from pre-generated stream candidate CSVs."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

from oanda_broker_env_sweep import transform_candidates
from oanda_broker_style_portfolio_replay import PRIMARY_CONFIG, objective
from oanda_monthly_dataset_comparison_backtest import (
    month_bounds,
    parse_months,
    sort_candidates,
    write_json,
)
from oanda_primary_live_exact_backtest import run as run_exact_backtest


def parse_stream_arg(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("--stream must be name=path")
    name, path = value.split("=", 1)
    name = name.strip().lower()
    if not name:
        raise argparse.ArgumentTypeError("stream name is empty")
    candidate_path = Path(path.strip())
    if not candidate_path.exists():
        raise argparse.ArgumentTypeError(f"missing stream candidate CSV: {candidate_path}")
    return name, candidate_path


def load_streams(streams: List[tuple[str, Path]]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for name, path in streams:
        frame = pd.read_csv(path)
        if frame.empty:
            frames[name] = frame
            continue
        frame["time_utc"] = pd.to_datetime(frame["time_utc"], utc=True)
        frame["planned_exit_time"] = pd.to_datetime(frame["planned_exit_time"], utc=True)
        frame["source_stream"] = name
        frames[name] = sort_candidates(frame)
    return frames


def merge_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame()
    return sort_candidates(pd.concat(non_empty, ignore_index=True))


def run(args: argparse.Namespace) -> pd.DataFrame:
    months = parse_months(args.months)
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    stream_frames = load_streams(args.stream)
    rows = []
    for month in months:
        start, end = month_bounds(month)
        monthly_frames = []
        stream_counts = {}
        for name, frame in stream_frames.items():
            if frame.empty:
                monthly = frame
            else:
                times = pd.to_datetime(frame["time_utc"], utc=True)
                monthly = frame[(times >= start) & (times < end)].copy()
            stream_counts[f"{name}_candidate_rows"] = int(len(monthly))
            monthly_frames.append(monthly)
        candidates = transform_candidates(
            merge_frames(monthly_frames),
            "inverted" if args.direction_mode == "inverted" else "normal",
        )
        candidates = sort_candidates(candidates)
        month_dir = output_root / "months" / month
        month_dir.mkdir(parents=True, exist_ok=True)
        candidate_path = month_dir / "candidate_rows.csv"
        candidates.to_csv(candidate_path, index=False)
        if candidates.empty:
            row = {
                "month": month,
                "candidate_rows": 0,
                "opened_trades": 0,
                "return_pct": 0.0,
                "profit_factor": 0.0,
                "max_drawdown_pct": 0.0,
                "margin_call_rows": 0,
                "output_dir": str(month_dir),
            }
            write_json(month_dir / "summary.json", row)
        else:
            summary = run_exact_backtest(
                month_dir,
                config_path=args.config,
                candidate_rows_csv=candidate_path,
                profile_name=args.profile_name,
                start_equity=args.start_equity,
            )
            row = {
                "month": month,
                "objective": objective(summary),
                "output_dir": str(month_dir),
                **summary,
            }
        row.update(
            {
                "period_start_utc": start.isoformat(),
                "period_end_utc": end.isoformat(),
                "direction_mode": args.direction_mode,
                "timeframes": ",".join(stream_frames.keys()),
                **stream_counts,
            }
        )
        rows.append(row)
        print(row, flush=True)
    summary_frame = pd.DataFrame(rows)
    summary_frame.to_csv(output_root / "monthly_dataset_comparison_summary.csv", index=False)
    write_json(output_root / "monthly_dataset_comparison_summary.json", rows)
    return summary_frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", required=True)
    parser.add_argument("--stream", action="append", type=parse_stream_arg, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=PRIMARY_CONFIG)
    parser.add_argument("--direction-mode", choices=["normal", "inverted"], default="inverted")
    parser.add_argument("--profile-name", default="h_tf_growth")
    parser.add_argument("--start-equity", type=float, default=1000.0)
    args = parser.parse_args()
    summary = run(args)
    columns = [
        column
        for column in [
            "month",
            "candidate_rows",
            "opened_trades",
            "return_pct",
            "profit_factor",
            "max_drawdown_pct",
            "max_margin_used_pct",
            "margin_call_rows",
            "output_dir",
        ]
        if column in summary.columns
    ]
    print(summary[columns].to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
