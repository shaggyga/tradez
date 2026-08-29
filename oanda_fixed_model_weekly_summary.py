#!/usr/bin/env python3
"""Week-by-week backtest for a fixed OANDA strategy ensemble."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from oanda_100_strategy_research import (
    REPORT_ROOT,
    StrategySpec,
    add_common_features,
    atomic_write_json,
    atomic_write_text,
    build_strategy_catalog,
    build_trade_candidates,
    finite_float,
    load_m1,
    resolve_pairs,
    resample_bars,
    simulate_account,
    write_csv,
)


DEFAULT_MEMBERS = [
    "s061_trend_adx_di_30min",
    "s015_mean_reversion_rsi_reversion_5min",
    "s046_breakout_donchian_breakout_1h",
]
DEFAULT_OUTPUT_DIR = REPORT_ROOT.parent / "fixed_model_weekly_summary"


def filter_frame(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    return frame[(frame.index >= start) & (frame.index < end)].copy()


def summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Fixed Model Weekly Summary",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Model",
        "",
        f"- Members: `{', '.join(payload['members'])}`",
        f"- Pairs: `{', '.join(payload['pairs'])}`",
        f"- Initial NAV per week: `${payload['initial_nav']:.2f}`",
        "",
        "## Weekly Results",
        "",
        "| week | start UTC | end UTC | trades | final NAV | P/L | return % | PF | win % | DD % | max margin % | blocked | closeout |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload["weekly_rows"]:
        lines.append(
            f"| {int(row['week_index'])} | {row['start_utc']} | {row['end_utc']} | "
            f"{int(row['trade_count'])} | {finite_float(row['final_nav']):.2f} | "
            f"{finite_float(row['net_pl_usd']):.2f} | {finite_float(row['return_pct']):.2f} | "
            f"{finite_float(row['profit_factor']):.2f} | {100.0 * finite_float(row['win_rate']):.1f} | "
            f"{finite_float(row['max_drawdown_pct']):.2f} | {finite_float(row['max_margin_used_pct']):.2f} | "
            f"{int(row['blocked_trade_count'])} | {row['margin_closeout']} |"
        )
    totals = payload["aggregate"]
    lines.extend(
        [
            "",
            "## Aggregate",
            "",
            f"- Weeks tested: `{totals['weeks']}`",
            f"- Positive weeks: `{totals['positive_weeks']}`",
            f"- Sum of weekly P/L: `${totals['sum_net_pl_usd']:.2f}`",
            f"- Average weekly return: `{totals['avg_return_pct']:.2f}%`",
            f"- Median weekly return: `{totals['median_return_pct']:.2f}%`",
            f"- Worst week: `{totals['worst_return_pct']:.2f}%`",
            f"- Best week: `{totals['best_return_pct']:.2f}%`",
            "",
            "## Files",
            "",
        ]
    )
    for label, path in payload["files"].items():
        lines.append(f"- {label}: `{path}`")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--members", default=",".join(DEFAULT_MEMBERS))
    parser.add_argument("--weeks", type=int, default=10)
    parser.add_argument("--pairs-mode", choices=["majors", "usd", "all"], default="majors")
    parser.add_argument("--pairs", default="")
    parser.add_argument("--max-pairs", type=int, default=5)
    parser.add_argument("--initial-nav", type=float, default=50.0)
    parser.add_argument("--margin-rate", type=float, default=0.033333)
    parser.add_argument("--max-margin-pct", type=float, default=90.0)
    parser.add_argument("--margin-closeout-percent", type=float, default=100.0)
    parser.add_argument("--max-open-trades", type=int, default=8)
    parser.add_argument("--warmup-days", type=int, default=14)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    catalog = build_strategy_catalog()
    specs_by_id = {spec.strategy_id: spec for spec in catalog}
    member_ids = [item.strip() for item in str(args.members).split(",") if item.strip()]
    missing = [member for member in member_ids if member not in specs_by_id]
    if missing:
        raise RuntimeError(f"Unknown strategy member(s): {missing}")
    member_specs: list[StrategySpec] = [specs_by_id[member] for member in member_ids]
    pair_list = resolve_pairs(args.pairs_mode, args.pairs, args.max_pairs)
    if not pair_list:
        raise RuntimeError("No local OANDA M1 pairs matched the requested universe.")

    full_m1_by_pair = {pair: load_m1(pair) for pair in pair_list}
    latest = min(frame.index.max() for frame in full_m1_by_pair.values())
    week_delta = pd.Timedelta(days=7)
    warmup_delta = pd.Timedelta(days=int(args.warmup_days))
    first_start = latest - week_delta * int(args.weeks)

    weekly_rows: list[dict[str, Any]] = []
    all_trade_rows: list[dict[str, Any]] = []
    all_blocked_rows: list[dict[str, Any]] = []

    for idx in range(int(args.weeks)):
        start = first_start + week_delta * idx
        end = start + week_delta
        print(f"[week] {idx + 1:02d}/{args.weeks:02d} {start.isoformat()} -> {end.isoformat()}", flush=True)
        candidates = []
        for spec in member_specs:
            for pair in pair_list:
                source = full_m1_by_pair[pair]
                m1 = filter_frame(source, start - warmup_delta, end)
                if m1.empty:
                    continue
                features = add_common_features(resample_bars(m1, spec.timeframe))
                pair_candidates = build_trade_candidates(pair, m1, features, spec)
                candidates.extend(
                    candidate
                    for candidate in pair_candidates
                    if candidate.entry_time >= start and candidate.entry_time < end
                )
        summary, trade_rows, blocked_rows = simulate_account(
            f"fixed_{'_'.join(member_ids)}_week_{idx + 1:02d}",
            candidates,
            specs_by_id,
            initial_nav=float(args.initial_nav),
            margin_rate=float(args.margin_rate),
            max_margin_pct=float(args.max_margin_pct),
            margin_closeout_percent=float(args.margin_closeout_percent),
            max_open_trades=int(args.max_open_trades),
        )
        row = {
            **summary,
            "week_index": idx + 1,
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
            "candidate_count": len(candidates),
        }
        weekly_rows.append(row)
        for trade in trade_rows:
            trade["week_index"] = idx + 1
        for blocked in blocked_rows:
            blocked["week_index"] = idx + 1
        all_trade_rows.extend(trade_rows)
        all_blocked_rows.extend(blocked_rows)

    returns = [finite_float(row["return_pct"]) for row in weekly_rows]
    pls = [finite_float(row["net_pl_usd"]) for row in weekly_rows]
    aggregate = {
        "weeks": len(weekly_rows),
        "positive_weeks": sum(1 for value in returns if value > 0.0),
        "sum_net_pl_usd": float(sum(pls)),
        "avg_return_pct": float(pd.Series(returns).mean()) if returns else 0.0,
        "median_return_pct": float(pd.Series(returns).median()) if returns else 0.0,
        "worst_return_pct": float(min(returns)) if returns else 0.0,
        "best_return_pct": float(max(returns)) if returns else 0.0,
    }

    output_dir = args.output_dir
    weekly_csv = output_dir / "fixed_model_weekly_rows.csv"
    trades_csv = output_dir / "fixed_model_weekly_trades.csv"
    blocked_csv = output_dir / "fixed_model_weekly_blocked_trades.csv"
    summary_json = output_dir / "latest_fixed_model_weekly_summary.json"
    summary_md = output_dir / "latest_fixed_model_weekly_summary.md"
    write_csv(weekly_csv, weekly_rows)
    write_csv(trades_csv, all_trade_rows)
    write_csv(blocked_csv, all_blocked_rows)
    payload = {
        "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
        "members": member_ids,
        "pairs": pair_list,
        "latest_common_data_utc": latest.isoformat(),
        "initial_nav": float(args.initial_nav),
        "margin_rate": float(args.margin_rate),
        "max_margin_pct": float(args.max_margin_pct),
        "margin_closeout_percent": float(args.margin_closeout_percent),
        "max_open_trades": int(args.max_open_trades),
        "weekly_rows": weekly_rows,
        "aggregate": aggregate,
        "files": {
            "weekly_csv": str(weekly_csv),
            "trades_csv": str(trades_csv),
            "blocked_csv": str(blocked_csv),
            "summary_json": str(summary_json),
            "summary_md": str(summary_md),
        },
    }
    atomic_write_json(summary_json, payload)
    atomic_write_text(summary_md, summary_markdown(payload))
    print(json.dumps({"aggregate": aggregate, "latest_week": weekly_rows[-1] if weekly_rows else {}}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
