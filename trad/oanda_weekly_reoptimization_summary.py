#!/usr/bin/env python3
"""Week-by-week in-sample reoptimization for the 100-strategy OANDA set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from oanda_100_strategy_research import (
    REPORT_ROOT,
    add_common_features,
    atomic_write_json,
    atomic_write_text,
    build_ensemble_candidates,
    build_strategy_catalog,
    build_trade_candidates,
    finite_float,
    load_m1,
    resolve_pairs,
    resample_bars,
    simulate_account,
    write_csv,
)


DEFAULT_OUTPUT_DIR = REPORT_ROOT.parent / "weekly_reoptimization_summary"


def filter_frame(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    return frame[(frame.index >= start) & (frame.index < end)].copy()


def rank_rows(rows: list[dict[str, Any]], min_trades: int) -> list[dict[str, Any]]:
    viable = [
        row
        for row in rows
        if int(row.get("trade_count", 0)) >= min_trades
        and not bool(row.get("margin_closeout", False))
    ]
    return sorted(
        viable or rows,
        key=lambda row: (
            finite_float(row.get("score"), -999999.0),
            finite_float(row.get("return_pct"), -999999.0),
            finite_float(row.get("profit_factor"), 0.0),
        ),
        reverse=True,
    )


def summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Weekly Reoptimization Summary",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        "",
        "## Method",
        "",
        "- Each week starts a fresh simulated account.",
        "- Each week tests all 100 strategy variants.",
        "- Each week then tests pair/triple ensembles among that week's top individual strategies.",
        "- The reported model is the best same-week result, so this is an in-sample hindsight upper bound.",
        "",
        "## Scope",
        "",
        f"- Pairs: `{', '.join(payload['pairs'])}`",
        f"- Initial NAV per week: `${payload['initial_nav']:.2f}`",
        f"- Ensemble top N: `{payload['ensemble_top_n']}`",
        f"- Max ensemble members: `{payload['ensemble_max_members']}`",
        "",
        "## Weekly Best Results",
        "",
        "| week | start UTC | end UTC | best model | trades | final NAV | P/L | return % | PF | win % | DD % | max margin % | blocked |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["weekly_rows"]:
        lines.append(
            f"| {int(row['week_index'])} | {row['start_utc']} | {row['end_utc']} | {row['strategy_id']} | "
            f"{int(row['trade_count'])} | {finite_float(row['final_nav']):.2f} | "
            f"{finite_float(row['net_pl_usd']):.2f} | {finite_float(row['return_pct']):.2f} | "
            f"{finite_float(row['profit_factor']):.2f} | {100.0 * finite_float(row['win_rate']):.1f} | "
            f"{finite_float(row['max_drawdown_pct']):.2f} | {finite_float(row['max_margin_used_pct']):.2f} | "
            f"{int(row['blocked_trade_count'])} |"
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
    parser.add_argument("--weeks", type=int, default=10)
    parser.add_argument("--pairs-mode", choices=["majors", "usd", "all"], default="majors")
    parser.add_argument("--pairs", default="")
    parser.add_argument("--max-pairs", type=int, default=5)
    parser.add_argument("--initial-nav", type=float, default=50.0)
    parser.add_argument("--margin-rate", type=float, default=0.033333)
    parser.add_argument("--max-margin-pct", type=float, default=90.0)
    parser.add_argument("--margin-closeout-percent", type=float, default=100.0)
    parser.add_argument("--max-open-trades", type=int, default=8)
    parser.add_argument("--warmup-days", type=int, default=45)
    parser.add_argument("--min-trades", type=int, default=2)
    parser.add_argument("--ensemble-top-n", type=int, default=8)
    parser.add_argument("--ensemble-max-members", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    catalog = build_strategy_catalog()
    specs_by_id = {spec.strategy_id: spec for spec in catalog}
    pairs = resolve_pairs(args.pairs_mode, args.pairs, args.max_pairs)
    if not pairs:
        raise RuntimeError("No local OANDA M1 pairs matched the requested universe.")

    full_m1_by_pair = {pair: load_m1(pair) for pair in pairs}
    latest = min(frame.index.max() for frame in full_m1_by_pair.values())
    week_delta = pd.Timedelta(days=7)
    warmup_delta = pd.Timedelta(days=int(args.warmup_days))
    first_start = latest - week_delta * int(args.weeks)
    timeframes = sorted({spec.timeframe for spec in catalog}, key=lambda value: pd.to_timedelta(value))

    weekly_rows: list[dict[str, Any]] = []
    top_individual_rows: list[dict[str, Any]] = []
    top_ensemble_rows: list[dict[str, Any]] = []

    for week_idx in range(int(args.weeks)):
        start = first_start + week_delta * week_idx
        end = start + week_delta
        print(f"[week] {week_idx + 1:02d}/{args.weeks:02d} {start.isoformat()} -> {end.isoformat()}", flush=True)

        m1_by_pair = {
            pair: filter_frame(frame, start - warmup_delta, end)
            for pair, frame in full_m1_by_pair.items()
        }
        features_by_pair_timeframe: dict[tuple[str, str], pd.DataFrame] = {}
        for pair, m1 in m1_by_pair.items():
            for timeframe in timeframes:
                features_by_pair_timeframe[(pair, timeframe)] = add_common_features(resample_bars(m1, timeframe))

        candidates_by_strategy = {}
        individual_rows: list[dict[str, Any]] = []
        for strategy_idx, spec in enumerate(catalog, start=1):
            candidates = []
            for pair in pairs:
                pair_candidates = build_trade_candidates(
                    pair,
                    m1_by_pair[pair],
                    features_by_pair_timeframe[(pair, spec.timeframe)],
                    spec,
                )
                candidates.extend(
                    candidate
                    for candidate in pair_candidates
                    if candidate.entry_time >= start and candidate.entry_time < end
                )
            candidates_by_strategy[spec.strategy_id] = candidates
            summary, _trades, _blocked = simulate_account(
                spec.strategy_id,
                candidates,
                specs_by_id,
                initial_nav=float(args.initial_nav),
                margin_rate=float(args.margin_rate),
                max_margin_pct=float(args.max_margin_pct),
                margin_closeout_percent=float(args.margin_closeout_percent),
                max_open_trades=int(args.max_open_trades),
            )
            summary.update(
                {
                    "week_index": week_idx + 1,
                    "start_utc": start.isoformat(),
                    "end_utc": end.isoformat(),
                    "name": spec.name,
                    "family": spec.family,
                    "rule": spec.rule,
                    "timeframe": spec.timeframe,
                    "candidate_count": len(candidates),
                }
            )
            individual_rows.append(summary)
        ranked_individual = rank_rows(individual_rows, int(args.min_trades))
        top_individual_rows.extend(ranked_individual[:8])

        ensemble_rows: list[dict[str, Any]] = []
        ensembles = build_ensemble_candidates(
            ranked_individual,
            candidates_by_strategy,
            max_members=int(args.ensemble_max_members),
            top_n=int(args.ensemble_top_n),
        )
        for ensemble_id, merged_candidates, members in ensembles:
            summary, _trades, _blocked = simulate_account(
                ensemble_id,
                merged_candidates,
                specs_by_id,
                initial_nav=float(args.initial_nav),
                margin_rate=float(args.margin_rate),
                max_margin_pct=float(args.max_margin_pct),
                margin_closeout_percent=float(args.margin_closeout_percent),
                max_open_trades=int(args.max_open_trades),
            )
            summary.update(
                {
                    "week_index": week_idx + 1,
                    "start_utc": start.isoformat(),
                    "end_utc": end.isoformat(),
                    "name": ensemble_id,
                    "family": "ensemble",
                    "rule": "weekly_top_n_combination",
                    "timeframe": "mixed",
                    "members": ",".join(members),
                    "member_count": len(members),
                    "candidate_count": len(merged_candidates),
                }
            )
            ensemble_rows.append(summary)
        ranked_ensembles = rank_rows(ensemble_rows, int(args.min_trades))
        top_ensemble_rows.extend(ranked_ensembles[:8])

        best_overall = rank_rows([*ranked_individual[:20], *ranked_ensembles[:20]], int(args.min_trades))[0]
        best_overall["week_index"] = week_idx + 1
        best_overall["start_utc"] = start.isoformat()
        best_overall["end_utc"] = end.isoformat()
        best_overall["best_individual_strategy_id"] = ranked_individual[0].get("strategy_id", "")
        best_overall["best_individual_return_pct"] = ranked_individual[0].get("return_pct", 0.0)
        best_overall["best_ensemble_strategy_id"] = ranked_ensembles[0].get("strategy_id", "") if ranked_ensembles else ""
        best_overall["best_ensemble_return_pct"] = ranked_ensembles[0].get("return_pct", 0.0) if ranked_ensembles else 0.0
        weekly_rows.append(best_overall)

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
    weekly_csv = output_dir / "weekly_reoptimized_best_rows.csv"
    individual_csv = output_dir / "weekly_top_individual_rows.csv"
    ensemble_csv = output_dir / "weekly_top_ensemble_rows.csv"
    summary_json = output_dir / "latest_weekly_reoptimization_summary.json"
    summary_md = output_dir / "latest_weekly_reoptimization_summary.md"
    write_csv(weekly_csv, weekly_rows)
    write_csv(individual_csv, top_individual_rows)
    write_csv(ensemble_csv, top_ensemble_rows)
    payload = {
        "generated_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "pairs": pairs,
        "latest_common_data_utc": latest.isoformat(),
        "initial_nav": float(args.initial_nav),
        "margin_rate": float(args.margin_rate),
        "max_margin_pct": float(args.max_margin_pct),
        "margin_closeout_percent": float(args.margin_closeout_percent),
        "max_open_trades": int(args.max_open_trades),
        "warmup_days": int(args.warmup_days),
        "min_trades": int(args.min_trades),
        "ensemble_top_n": int(args.ensemble_top_n),
        "ensemble_max_members": int(args.ensemble_max_members),
        "weekly_rows": weekly_rows,
        "aggregate": aggregate,
        "files": {
            "weekly_csv": str(weekly_csv),
            "individual_csv": str(individual_csv),
            "ensemble_csv": str(ensemble_csv),
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
