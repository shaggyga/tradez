#!/usr/bin/env python3
"""Backtest the GPT ZAR/MXN ruleset on local OANDA USD-leg data.

There is no direct ZAR_MXN_M1.csv in the local 68-pair candle universe, so this
uses the decomposition from the attached GPT answer:

    ZAR_MXN = USD_MXN / USD_ZAR

Signals use the synthetic cross chart plus the relative-strength leg rule:

    ZAR_strength = -return(USD_ZAR)
    MXN_strength = -return(USD_MXN)
    relative_strength = ZAR_strength - MXN_strength
                      = return(USD_MXN) - return(USD_ZAR)

This is a mid-price research backtest. It does not include direct ZAR/MXN spread,
slippage, swap, or broker-specific financing.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from oanda_one_pair_indicator_ruleset_backtest import (
    CANDLE_ROOT,
    RuleConfig,
    atomic_write_json,
    atomic_write_text,
    config_summary_row,
    finite_float,
    format_float,
    grid_configs,
    load_m1_ohlc,
    resample_ohlc,
    run_config,
    select_top_configs,
    summarize_trades,
    utc_now,
    write_csv,
)


ROOT = Path(__file__).resolve().parent
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports" / "zarmxn_gpt_ruleset"
PAIR = "ZAR_MXN"
NUMERATOR = "USD_MXN"
DENOMINATOR = "USD_ZAR"


def build_synthetic_zarmxn_m1() -> pd.DataFrame:
    numerator = load_m1_ohlc(NUMERATOR).add_prefix("num_")
    denominator = load_m1_ohlc(DENOMINATOR).add_prefix("den_")
    frame = pd.concat([numerator, denominator], axis=1, join="inner").dropna()
    frame = frame[(frame["num_close"] > 0.0) & (frame["den_close"] > 0.0)]

    synthetic = pd.DataFrame(index=frame.index)
    synthetic["open"] = frame["num_open"] / frame["den_open"]
    synthetic["close"] = frame["num_close"] / frame["den_close"]
    rough_high = frame["num_high"] / frame["den_low"]
    rough_low = frame["num_low"] / frame["den_high"]
    synthetic["high"] = pd.concat([synthetic["open"], synthetic["close"], rough_high], axis=1).max(axis=1)
    synthetic["low"] = pd.concat([synthetic["open"], synthetic["close"], rough_low], axis=1).min(axis=1)
    return synthetic.dropna(subset=["open", "high", "low", "close"])


def build_leg_bars(timeframe: str) -> pd.DataFrame:
    usd_mxn = load_m1_ohlc(NUMERATOR)["close"].resample(timeframe, label="right", closed="right").last().rename(NUMERATOR)
    usd_zar = load_m1_ohlc(DENOMINATOR)["close"].resample(timeframe, label="right", closed="right").last().rename(DENOMINATOR)
    return pd.concat([usd_mxn, usd_zar], axis=1).dropna(subset=[NUMERATOR, DENOMINATOR])


def leg_metadata() -> dict[str, Any]:
    return {
        "base_currency": "ZAR",
        "quote_currency": "MXN",
        "base_usd_leg": DENOMINATOR,
        "base_usd_leg_sign": -1.0,
        "quote_usd_leg": NUMERATOR,
        "quote_usd_leg_sign": -1.0,
        "fallback": False,
    }


def markdown_report(payload: dict[str, Any]) -> str:
    summary = payload["default_summary"]
    lines = [
        "# ZAR/MXN GPT Ruleset Backtest",
        "",
        f"Generated: {payload['generated_at_utc']}",
        "",
        "## Data and assumptions",
        "",
        f"- Synthetic pair: `{PAIR} = {NUMERATOR} / {DENOMINATOR}`.",
        f"- M1 range: `{payload['m1_start_utc']}` to `{payload['m1_end_utc']}`.",
        f"- Signal timeframe: `{payload['timeframe']}`; entries/exits simulated on synthetic M1 OHLC.",
        f"- Round-turn execution cost: `{payload['round_turn_cost_bps']}` bps.",
        "- Direct ZAR/MXN spread, slippage, swap, and broker financing are not in the local data.",
        "",
        "## Rules implemented",
        "",
        "- Trend: price vs 50 EMA, 20/50 EMA stack, and 50 EMA slope or 200 EMA alignment.",
        "- Trend quality: ADX(14) above threshold and rising.",
        "- USD-leg relative strength: `return(USD_MXN) - return(USD_ZAR)`.",
        "- Entries: Donchian breakout and/or RSI pullback to the EMA area.",
        "- Exits: ATR stop, ATR trailing stop, R-multiple take profit, opposite signal, max hold.",
        "",
        "## Default config",
        "",
        f"- Trades: `{summary.get('trade_count', 0)}`",
        f"- Total net: `{format_float(summary.get('total_net_bps'))}` bps / `{format_float(summary.get('total_net_r'))}` R",
        f"- Avg trade: `{format_float(summary.get('avg_net_bps'))}` bps / `{format_float(summary.get('avg_net_r'))}` R",
        f"- Win rate: `{format_float(100.0 * finite_float(summary.get('win_rate')))}%`",
        f"- Profit factor: `{format_float(summary.get('profit_factor'))}`",
        f"- Max drawdown: `{format_float(summary.get('max_drawdown_bps'))}` bps / `{format_float(summary.get('max_drawdown_r'))}` R",
        "",
    ]
    if payload["top_grid_configs"]:
        lines.extend(
            [
                "## Top grid configs",
                "",
                "| rank | mode | ADX | strength bars | stop ATR | TP R | all trades | all R | test trades | test R | test PF |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for rank, row in enumerate(payload["top_grid_configs"], start=1):
            lines.append(
                f"| {rank} | {row['entry_mode']} | {format_float(row['adx_min'])} | "
                f"{row['strength_lookback']} | {format_float(row['atr_stop_mult'])} | "
                f"{format_float(row['take_profit_r'])} | {int(row.get('all_trade_count', 0))} | "
                f"{format_float(row.get('all_total_net_r'))} | {int(row.get('test_trade_count', 0))} | "
                f"{format_float(row.get('test_total_net_r'))} | {format_float(row.get('test_profit_factor'))} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Files",
            "",
            f"- JSON: `{payload['json_report']}`",
            f"- Grid CSV: `{payload['grid_csv']}`",
            f"- Default trades CSV: `{payload['trades_csv']}`",
            f"- Default signals CSV: `{payload['signals_csv']}`",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--cost-bps", type=float, default=0.0)
    parser.add_argument("--grid-preset", choices=["none", "compact", "quick", "wide"], default="compact")
    parser.add_argument("--min-trades", type=int, default=8)
    parser.add_argument("--max-grid-configs", type=int, default=0)
    parser.add_argument("--output-dir", type=Path, default=REPORT_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for leg in [NUMERATOR, DENOMINATOR]:
        path = CANDLE_ROOT / f"{leg}_M1.csv"
        if not path.exists():
            raise FileNotFoundError(f"Missing required leg candle file: {path}")

    m1 = build_synthetic_zarmxn_m1()
    bars = resample_ohlc(m1, args.timeframe)
    leg_bars = build_leg_bars(args.timeframe)
    meta = leg_metadata()
    split_time = bars.index[int(len(bars) * 0.70)]

    base_cfg = replace(RuleConfig(), name="default_zarmxn_gpt_h4", timeframe=args.timeframe, round_turn_cost_bps=float(args.cost_bps))
    default_trades, default_signals = run_config(PAIR, m1, bars, leg_bars, meta, base_cfg)
    default_row = config_summary_row(base_cfg, default_trades, split_time)

    configs = grid_configs(base_cfg, args.grid_preset)
    if args.max_grid_configs > 0:
        configs = configs[: args.max_grid_configs]
    grid_rows: list[dict[str, Any]] = []
    for idx, cfg in enumerate(configs, start=1):
        print(f"[grid] {idx:03d}/{len(configs):03d} {cfg.name}", flush=True)
        trades, _signals = run_config(PAIR, m1, bars, leg_bars, meta, cfg)
        grid_rows.append(config_summary_row(cfg, trades, split_time))

    top_configs = select_top_configs([default_row, *grid_rows], args.min_trades)
    output_dir = args.output_dir
    json_path = output_dir / "latest_zarmxn_gpt_ruleset_backtest.json"
    grid_csv = output_dir / "latest_zarmxn_gpt_ruleset_grid.csv"
    trades_csv = output_dir / "latest_zarmxn_gpt_ruleset_default_trades.csv"
    signals_csv = output_dir / "latest_zarmxn_gpt_ruleset_default_signals.csv"
    summary_md = output_dir / "latest_zarmxn_gpt_ruleset_summary.md"

    signal_export = default_signals.reset_index().rename(columns={"index": "signal_time", "time_utc": "signal_time"})
    signal_rows = signal_export[signal_export["signal"].ne(0)].to_dict(orient="records")
    payload: dict[str, Any] = {
        "generated_at_utc": utc_now(),
        "pair": PAIR,
        "formula": f"{PAIR} = {NUMERATOR} / {DENOMINATOR}",
        "timeframe": args.timeframe,
        "round_turn_cost_bps": float(args.cost_bps),
        "m1_start_utc": m1.index.min().isoformat(),
        "m1_end_utc": m1.index.max().isoformat(),
        "bar_count": int(len(bars)),
        "split_time_utc": split_time.isoformat(),
        "leg_metadata": meta,
        "default_config": asdict(base_cfg),
        "default_summary": summarize_trades(default_trades),
        "default_signal_count": int(default_signals["signal"].ne(0).sum()),
        "grid_preset": args.grid_preset,
        "grid_config_count": len(configs),
        "top_grid_configs": top_configs,
        "json_report": str(json_path),
        "grid_csv": str(grid_csv),
        "trades_csv": str(trades_csv),
        "signals_csv": str(signals_csv),
        "summary_md": str(summary_md),
    }

    write_csv(grid_csv, [default_row, *grid_rows])
    write_csv(trades_csv, default_trades)
    write_csv(signals_csv, signal_rows)
    atomic_write_json(json_path, payload)
    atomic_write_text(summary_md, markdown_report(payload))
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
