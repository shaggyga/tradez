#!/usr/bin/env python3
"""Targeted parameter sweep for the SR trendline backtest.

This is designed for follow-up research after the broad timeframe sweep. It
loads all pair bars once for a timeframe, then tests a compact parameter grid
around the only cost-aware survivor found so far: 4h short setups with stricter
trendline quality and quick level confirmation.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

import oanda_sr_trendline_backtest as bt


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "sr_trendline_param_sweep"


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def summarize(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return bt.summarize_trades(trades)


def split_trades(trades: Sequence[dict[str, Any]], split_time: pd.Timestamp) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return bt.split_trades(trades, split_time)


def row_for_trades(name: str, cfg: bt.StrategyConfig, trades: Sequence[dict[str, Any]], split_time: pd.Timestamp) -> dict[str, Any]:
    train, test = split_trades(trades, split_time)
    row: dict[str, Any] = {
        "config": name,
        "trade_count": len(trades),
        "reward_r": cfg.reward_r,
        "min_trend_atr_move": cfg.min_trend_atr_move,
        "touch_atr_mult": cfg.touch_atr_mult,
        "break_atr_mult": cfg.break_atr_mult,
        "retest_atr_mult": cfg.retest_atr_mult,
        "max_hold_bars": cfg.max_hold_bars,
        "cost_bps": cfg.round_turn_cost_bps,
    }
    for prefix, summary in [("full", summarize(trades)), ("train", summarize(train)), ("test", summarize(test))]:
        for key, value in summary.items():
            row[f"{prefix}_{key}"] = value
    return row


def quality_filter(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for trade in trades:
        atr = finite_float(trade.get("atr"))
        if atr <= 0.0:
            continue
        risk_atr = finite_float(trade.get("risk_price")) / atr
        dist_atr = abs(finite_float(trade.get("entry_price")) - finite_float(trade.get("support_resistance_level"))) / atr
        if (
            str(trade.get("direction")) == "SHORT"
            and int(finite_float(trade.get("line_touches"))) >= 5
            and finite_float(trade.get("line_touch_error_atr")) <= 0.60
            and int(finite_float(trade.get("trendline_to_level_bars"))) <= 4
            and int(finite_float(trade.get("level_break_to_entry_bars"))) <= 8
            and risk_atr <= 2.75
            and dist_atr <= 0.75
        ):
            out.append(trade)
    return out


def numeric_best_filter(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter found by the fine-candidate numeric screen.

    It keeps the original three-touch trendline rule but tightens fit,
    confirmation speed, risk size, and retest distance.
    """
    out: list[dict[str, Any]] = []
    for trade in trades:
        atr = finite_float(trade.get("atr"))
        if atr <= 0.0:
            continue
        risk_atr = finite_float(trade.get("risk_price")) / atr
        dist_atr = abs(finite_float(trade.get("entry_price")) - finite_float(trade.get("support_resistance_level"))) / atr
        if (
            str(trade.get("direction")) == "SHORT"
            and int(finite_float(trade.get("line_touches"))) >= 3
            and finite_float(trade.get("line_touch_error_atr")) <= 0.50
            and int(finite_float(trade.get("trendline_to_level_bars"))) <= 4
            and int(finite_float(trade.get("level_break_to_entry_bars"))) <= 8
            and risk_atr <= 2.00
            and dist_atr <= 0.50
        ):
            out.append(trade)
    return out


def generate_configs(timeframe: str, cost_bps: float, grid_preset: str) -> list[bt.StrategyConfig]:
    base = bt.config_for_timeframe(timeframe, cost_bps=cost_bps)
    configs: list[bt.StrategyConfig] = []
    grids = {
        "compact": {
            "reward_r": [1.5, 2.0, 2.5],
            "min_trend_atr_move": [1.25, 1.75],
            "touch_atr_mult": [0.40, 0.60],
            "retest_atr_mult": [0.10, 0.15],
            "max_hold_bars": [48, 96],
        },
        "fine": {
            "reward_r": [1.75, 2.0, 2.25],
            "min_trend_atr_move": [1.50, 1.75, 2.00],
            "touch_atr_mult": [0.50, 0.60, 0.70],
            "retest_atr_mult": [0.10, 0.125],
            "max_hold_bars": [72, 96],
        },
        "refine": {
            "reward_r": [1.90, 2.00, 2.10],
            "min_trend_atr_move": [1.65, 1.75, 1.85],
            "touch_atr_mult": [0.55, 0.60, 0.65],
            "retest_atr_mult": [0.115, 0.125, 0.135],
            "max_hold_bars": [48, 72],
        },
    }
    if grid_preset not in grids:
        raise ValueError(f"Unknown grid preset: {grid_preset}")
    grid = grids[grid_preset]
    keys = list(grid)
    for values in itertools.product(*(grid[key] for key in keys)):
        updates = dict(zip(keys, values, strict=True))
        slug = "_".join(f"{key}{str(value).replace('.', 'p')}" for key, value in updates.items())
        configs.append(
            replace(
                base,
                name=f"sr_tl_{timeframe}_short_{slug}",
                direction_mode="short",
                **updates,
            )
        )
    return configs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--cost-bps", type=float, default=1.0)
    parser.add_argument("--grid-preset", choices=["compact", "fine", "refine"], default="compact")
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    parser.add_argument("--max-configs", type=int, default=0)
    parser.add_argument("--max-pairs", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = bt.discover_pairs()
    if args.max_pairs > 0:
        pairs = pairs[: args.max_pairs]
    base_cfg = bt.config_for_timeframe(args.timeframe, cost_bps=float(args.cost_bps))
    bars_by_pair: dict[str, pd.DataFrame] = {}
    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    for idx, pair in enumerate(pairs, start=1):
        print(f"[load] {idx:02d}/{len(pairs):02d} {pair}")
        bars = bt.load_pair_bars(pair, base_cfg)
        bars_by_pair[pair] = bars
        if not bars.empty:
            starts.append(pd.Timestamp(bars.index[0]))
            ends.append(pd.Timestamp(bars.index[-1]))
    if not starts or not ends:
        raise RuntimeError("No bars loaded")
    split_time = min(starts) + (max(ends) - min(starts)) * 0.70

    configs = generate_configs(args.timeframe, float(args.cost_bps), args.grid_preset)
    if args.max_configs > 0:
        configs = configs[: args.max_configs]

    rows: list[dict[str, Any]] = []
    top_trades: list[dict[str, Any]] = []
    for config_index, cfg in enumerate(configs, start=1):
        print(f"[config] {config_index:03d}/{len(configs):03d} {cfg.name}")
        trades: list[dict[str, Any]] = []
        for pair in pairs:
            pair_bars = bars_by_pair[pair]
            if pair_bars.empty:
                continue
            pair_trades, _stats = bt.backtest_pair(pair, pair_bars, cfg)
            trades.extend(pair_trades)
        all_row = row_for_trades(f"{cfg.name}_all", cfg, trades, split_time)
        all_row["post_filter"] = "none"
        filtered_rows = [all_row]
        for filter_name, filter_func in [
            ("4h_short_quality_v1", quality_filter),
            ("4h_short_numeric_best_v1", numeric_best_filter),
        ]:
            filtered = filter_func(trades)
            filtered_row = row_for_trades(f"{cfg.name}_{filter_name}", cfg, filtered, split_time)
            filtered_row["post_filter"] = filter_name
            filtered_rows.append(filtered_row)
            top_trades.extend(filtered)
        rows.extend(filtered_rows)
        rows = sorted(
            rows,
            key=lambda row: (
                finite_float(row.get("test_total_net_r")),
                finite_float(row.get("test_profit_factor")),
                finite_float(row.get("full_total_net_r")),
            ),
            reverse=True,
        )[:5000]

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = output_dir / "param_sweep_summary.csv"
    write_csv(summary_csv, rows)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "timeframe": args.timeframe,
        "cost_bps": args.cost_bps,
        "grid_preset": args.grid_preset,
        "pair_count": len(pairs),
        "config_count": len(configs),
        "split_time_utc": split_time.isoformat(),
        "summary_csv": str(summary_csv),
        "top_rows": rows[:50],
    }
    json_path = output_dir / "param_sweep_summary.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    md_path = output_dir / "param_sweep_summary.md"
    lines = [
        "# SR Trendline Parameter Sweep",
        "",
        f"Timeframe: `{args.timeframe}`; cost: `{args.cost_bps}` bps; grid: `{args.grid_preset}`; configs: `{len(configs)}`.",
        "",
        "| rank | filter | reward | trend ATR | touch ATR | retest ATR | hold | train R | test R | test PF | full R | full PF | trades |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, row in enumerate(rows[:40], start=1):
        lines.append(
            f"| {rank} | {row.get('post_filter')} | {finite_float(row.get('reward_r')):.2f} | "
            f"{finite_float(row.get('min_trend_atr_move')):.2f} | {finite_float(row.get('touch_atr_mult')):.2f} | "
            f"{finite_float(row.get('retest_atr_mult')):.2f} | {int(finite_float(row.get('max_hold_bars')))} | "
            f"{finite_float(row.get('train_total_net_r')):.2f} | {finite_float(row.get('test_total_net_r')):.2f} | "
            f"{finite_float(row.get('test_profit_factor')):.2f} | {finite_float(row.get('full_total_net_r')):.2f} | "
            f"{finite_float(row.get('full_profit_factor')):.2f} | {int(finite_float(row.get('trade_count')))} |"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
