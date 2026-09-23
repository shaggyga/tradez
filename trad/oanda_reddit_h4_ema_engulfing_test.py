#!/usr/bin/env python3
"""Test the Reddit r/Forex H4 engulfing EMA-bounce strategy.

The referenced thread contains a concrete discretionary idea: use engulfing
candles around 75/100/200 EMAs on H4 as trend/bounce context. This runner turns
that into explicit variants and sends them through the same OANDA M1 account
simulator used by ``oanda_100_strategy_research.py``.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from oanda_100_strategy_research import (
    REPORT_ROOT,
    SOURCE_REGISTRY,
    StrategySpec,
    add_common_features,
    atomic_write_json,
    atomic_write_text,
    build_trade_candidates,
    finite_float,
    load_m1,
    resolve_pairs,
    resample_bars,
    simulate_account,
    utc_now,
    write_csv,
)


DEFAULT_OUTPUT_DIR = REPORT_ROOT.parent / "reddit_h4_ema_engulfing_test"


def build_specs() -> list[StrategySpec]:
    specs: list[StrategySpec] = []
    variants = [
        ("baseline", 0.20, 12.0, 1.5, 2.0, 24, 1.0),
        ("tight_touch", 0.10, 12.0, 1.5, 2.0, 24, 1.0),
        ("wide_touch", 0.35, 12.0, 1.5, 2.0, 24, 1.0),
        ("trendier", 0.20, 18.0, 1.5, 2.0, 24, 1.0),
        ("loose_trend", 0.20, 8.0, 1.5, 2.0, 24, 1.0),
        ("faster_exit", 0.20, 12.0, 1.2, 1.5, 12, 0.8),
        ("swing_hold", 0.20, 12.0, 1.8, 2.5, 36, 1.0),
        ("high_conviction", 0.15, 18.0, 1.8, 2.5, 36, 1.0),
        ("aggressive", 0.35, 8.0, 1.2, 1.8, 18, 1.2),
    ]
    for idx, (label, touch_atr, min_adx, stop_atr, target_r, max_hold_bars, risk_pct) in enumerate(variants, start=1):
        specs.append(
            StrategySpec(
                strategy_id=f"reddit_h4_ema_engulfing_{idx:02d}_{label}",
                name=f"Reddit H4 EMA engulfing bounce {label}",
                family="reddit_price_action",
                rule="reddit_h4_ema_engulfing_bounce",
                timeframe="4h",
                source_key="reddit_h4_ema_engulfing",
                params={"touch_atr": touch_atr, "min_adx": min_adx},
                stop_atr=stop_atr,
                target_r=target_r,
                max_hold_bars=max_hold_bars,
                risk_pct=risk_pct,
            )
        )
    return specs


def summary_rows_to_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| rank | variant | trades | return % | PF | win % | max DD % | max margin % | score |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(rows, start=1):
        lines.append(
            f"| {rank} | {row['strategy_id']} | {int(row.get('trade_count', 0))} | "
            f"{finite_float(row.get('return_pct')):.2f} | {finite_float(row.get('profit_factor')):.2f} | "
            f"{100.0 * finite_float(row.get('win_rate')):.1f} | {finite_float(row.get('max_drawdown_pct')):.2f} | "
            f"{finite_float(row.get('max_margin_used_pct')):.2f} | {finite_float(row.get('score')):.2f} |"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs-mode", choices=["majors", "usd", "all"], default="majors")
    parser.add_argument("--pairs", default="", help="Comma-separated explicit pair list.")
    parser.add_argument("--max-pairs", type=int, default=5)
    parser.add_argument("--max-rows-per-pair", type=int, default=0, help="Use latest N M1 rows per pair; 0 means all local rows.")
    parser.add_argument("--max-signals-per-pair", type=int, default=0)
    parser.add_argument("--initial-nav", type=float, default=10_000.0)
    parser.add_argument("--margin-rate", type=float, default=0.033333)
    parser.add_argument("--max-margin-pct", type=float, default=90.0)
    parser.add_argument("--margin-closeout-percent", type=float, default=100.0)
    parser.add_argument("--max-open-trades", type=int, default=8)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    specs = build_specs()
    specs_by_id = {spec.strategy_id: spec for spec in specs}
    pairs = resolve_pairs(args.pairs_mode, args.pairs, args.max_pairs)
    if not pairs:
        raise RuntimeError("No local OANDA M1 pairs matched the requested universe.")

    output_dir = args.output_dir
    m1_by_pair = {}
    features_by_pair = {}
    for idx, pair in enumerate(pairs, start=1):
        print(f"[load] {idx:02d}/{len(pairs):02d} {pair}", flush=True)
        m1 = load_m1(pair, max_rows=int(args.max_rows_per_pair))
        m1_by_pair[pair] = m1
        features_by_pair[pair] = add_common_features(resample_bars(m1, "4h"))

    result_rows: list[dict[str, Any]] = []
    all_trade_rows: list[dict[str, Any]] = []
    all_blocked_rows: list[dict[str, Any]] = []
    for idx, spec in enumerate(specs, start=1):
        print(f"[variant] {idx:02d}/{len(specs):02d} {spec.strategy_id}", flush=True)
        candidates = []
        for pair in pairs:
            candidates.extend(
                build_trade_candidates(
                    pair,
                    m1_by_pair[pair],
                    features_by_pair[pair],
                    spec,
                    max_signals_per_pair=int(args.max_signals_per_pair),
                )
            )
        summary, trade_rows, blocked_rows = simulate_account(
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
                "name": spec.name,
                "family": spec.family,
                "rule": spec.rule,
                "timeframe": spec.timeframe,
                "params": json.dumps(spec.params, sort_keys=True),
                "stop_atr": spec.stop_atr,
                "target_r": spec.target_r,
                "max_hold_bars": spec.max_hold_bars,
                "risk_pct": spec.risk_pct,
                "candidate_count": len(candidates),
                "source_url": SOURCE_REGISTRY[spec.source_key]["url"],
            }
        )
        result_rows.append(summary)
        all_trade_rows.extend(trade_rows)
        all_blocked_rows.extend(blocked_rows)

    ranked = sorted(
        result_rows,
        key=lambda row: (
            finite_float(row.get("score"), -999999.0),
            finite_float(row.get("return_pct"), -999999.0),
            finite_float(row.get("profit_factor"), 0.0),
        ),
        reverse=True,
    )
    best = ranked[0] if ranked else {}
    result_csv = output_dir / "reddit_h4_ema_engulfing_results.csv"
    trades_csv = output_dir / "reddit_h4_ema_engulfing_trades.csv"
    blocked_csv = output_dir / "reddit_h4_ema_engulfing_blocked_trades.csv"
    summary_json = output_dir / "latest_reddit_h4_ema_engulfing.json"
    summary_md = output_dir / "latest_reddit_h4_ema_engulfing.md"

    write_csv(result_csv, result_rows)
    write_csv(trades_csv, all_trade_rows)
    write_csv(blocked_csv, all_blocked_rows)
    payload = {
        "generated_at_utc": utc_now(),
        "source_url": SOURCE_REGISTRY["reddit_h4_ema_engulfing"]["url"],
        "source_interpretation": "H4 engulfing candles that bounce from a 75/100/200 EMA trend zone.",
        "pairs": pairs,
        "pair_count": len(pairs),
        "variant_count": len(specs),
        "initial_nav": float(args.initial_nav),
        "margin_rate": float(args.margin_rate),
        "max_margin_pct": float(args.max_margin_pct),
        "margin_closeout_percent": float(args.margin_closeout_percent),
        "max_open_trades": int(args.max_open_trades),
        "best_result": best,
        "ranked_results": ranked,
        "strategy_specs": [asdict(spec) for spec in specs],
        "files": {
            "result_csv": str(result_csv),
            "trades_csv": str(trades_csv),
            "blocked_csv": str(blocked_csv),
            "summary_json": str(summary_json),
            "summary_md": str(summary_md),
        },
    }
    atomic_write_json(summary_json, payload)
    lines = [
        "# Reddit H4 EMA Engulfing Bounce Test",
        "",
        f"Generated: `{payload['generated_at_utc']}`",
        f"Source: `{payload['source_url']}`",
        "",
        "## Interpretation",
        "",
        "- H4 bullish/bearish engulfing candle.",
        "- 75 EMA, 100 EMA, and 200 EMA define trend and bounce zone.",
        "- Entry is next M1 open after the completed H4 signal bar.",
        "- Stop/target/margin account mechanics reuse the OANDA M1 simulator.",
        "",
        "## Scope",
        "",
        f"- Pairs: `{', '.join(pairs)}`.",
        f"- Variants: `{len(specs)}`.",
        f"- Initial NAV per variant: `${float(args.initial_nav):,.2f}`.",
        "",
        "## Ranked Results",
        "",
        summary_rows_to_markdown(ranked),
        "",
        "## Files",
        "",
    ]
    for label, path in payload["files"].items():
        lines.append(f"- {label}: `{path}`")
    lines.append("")
    atomic_write_text(summary_md, "\n".join(lines))
    print(json.dumps(best, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
