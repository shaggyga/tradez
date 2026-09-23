#!/usr/bin/env python3
"""Read-only gold signal monitor.

This refreshes the free Yahoo GC=F M1 proxy, runs the mechanical gold signal
engine, and writes the latest signal state to disk. It does not place orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import download_yahoo_gold_futures_m1 as yahoo
import gold_m1_liquidity_scalper as strategy


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = SCRIPT_DIR / "data" / "gold_m1_liquidity_scalper"
DEFAULT_CSV = DATA_ROOT / "candles" / "GC_F_YAHOO_M1_5D.csv"
DEFAULT_RAW = DATA_ROOT / "raw" / "yahoo_GC_F_1m_5d.json"
DEFAULT_STATE_DIR = DATA_ROOT / "live_monitor"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def append_event(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    fields = [
        "observed_utc",
        "status",
        "last_bar_utc",
        "direction",
        "entry",
        "stop",
        "target",
        "rr",
        "expires_utc",
        "signal_time_utc",
    ]
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def refresh_yahoo_csv(args: argparse.Namespace) -> int:
    payload = yahoo.fetch_chart(args.symbol, args.interval, args.range, args.timeout)
    yahoo.atomic_write_text(args.raw_output, json.dumps(payload, separators=(",", ":")))
    return yahoo.write_csv(args.csv, payload)


def evaluate_signal(args: argparse.Namespace) -> dict[str, Any]:
    cfg = strategy.StrategyConfig(
        require_htf_bias=not args.no_htf_bias,
        htf_timeframes=strategy.parse_timeframes(args.htf_timeframes),
        risk_pct=args.risk_pct,
        point_value=args.point_value,
        commission_round_turn=args.commission_round_turn,
        slippage_points_round_turn=args.slippage_points_round_turn,
        max_contracts=args.max_contracts,
        max_trades_per_day=args.max_trades_per_day,
        daily_loss_limit_pct=args.daily_loss_limit_pct,
        min_rr=args.min_rr,
        fallback_rr=args.fallback_rr,
        min_stop_points=args.min_stop_points,
        max_stop_points=args.max_stop_points,
        fvg_min_points=args.fvg_min_points,
        displacement_atr_mult=args.displacement_atr_mult,
    )
    frame = strategy.load_ohlc_csv(args.csv)
    payload = strategy.latest_signal_status(frame, cfg)
    payload["observed_utc"] = utc_now()
    payload["csv"] = str(args.csv)
    payload["config"] = asdict(cfg)
    return payload


def event_row(payload: dict[str, Any]) -> dict[str, Any]:
    signal = payload.get("signal") or {}
    return {
        "observed_utc": payload.get("observed_utc", ""),
        "status": payload.get("status", ""),
        "last_bar_utc": payload.get("last_bar_utc", ""),
        "direction": signal.get("direction", ""),
        "entry": signal.get("entry", ""),
        "stop": signal.get("stop", ""),
        "target": signal.get("target", ""),
        "rr": signal.get("rr", ""),
        "expires_utc": signal.get("expires_utc", ""),
        "signal_time_utc": signal.get("time_utc", ""),
    }


def run_cycle(args: argparse.Namespace) -> dict[str, Any]:
    downloaded_rows = 0
    error = ""
    if not args.skip_refresh:
        try:
            downloaded_rows = refresh_yahoo_csv(args)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

    payload = evaluate_signal(args)
    payload["downloaded_rows"] = downloaded_rows
    if error:
        payload["refresh_error"] = error

    state_dir = args.state_dir
    state_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(state_dir / "latest_signal.json", payload)
    append_event(state_dir / "signal_events.csv", event_row(payload))
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only gold signal monitoring.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--raw-output", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--symbol", default="GC=F")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--range", default="5d")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--max-cycles", type=int, default=0, help="0 means run until stopped.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--skip-refresh", action="store_true")

    parser.add_argument("--htf-timeframes", default="15min,1h")
    parser.add_argument("--no-htf-bias", action="store_true")
    parser.add_argument("--risk-pct", type=float, default=0.50)
    parser.add_argument("--point-value", type=float, default=10.0)
    parser.add_argument("--commission-round-turn", type=float, default=3.00)
    parser.add_argument("--slippage-points-round-turn", type=float, default=0.20)
    parser.add_argument("--max-contracts", type=int, default=10)
    parser.add_argument("--max-trades-per-day", type=int, default=8)
    parser.add_argument("--daily-loss-limit-pct", type=float, default=2.0)
    parser.add_argument("--min-rr", type=float, default=1.50)
    parser.add_argument("--fallback-rr", type=float, default=2.00)
    parser.add_argument("--min-stop-points", type=float, default=1.00)
    parser.add_argument("--max-stop-points", type=float, default=12.00)
    parser.add_argument("--fvg-min-points", type=float, default=0.20)
    parser.add_argument("--displacement-atr-mult", type=float, default=1.20)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    cycles = 0
    while True:
        payload = run_cycle(args)
        signal = payload.get("signal") or {}
        print(
            json.dumps(
                {
                    "observed_utc": payload.get("observed_utc"),
                    "status": payload.get("status"),
                    "last_bar_utc": payload.get("last_bar_utc"),
                    "direction": signal.get("direction", ""),
                    "entry": signal.get("entry", ""),
                    "stop": signal.get("stop", ""),
                    "target": signal.get("target", ""),
                    "rr": signal.get("rr", ""),
                    "refresh_error": payload.get("refresh_error", ""),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        cycles += 1
        if args.once or (args.max_cycles > 0 and cycles >= args.max_cycles):
            return 0
        time.sleep(max(5, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
