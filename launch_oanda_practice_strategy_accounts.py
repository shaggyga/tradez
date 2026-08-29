#!/usr/bin/env python3
"""Launch one all-pairs OANDA practice scanner per strategy/account assignment."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS, DEFAULT_LOG_DIR


ROOT = Path(__file__).resolve().parent
SCANNER = ROOT / "oanda_practice_all_pairs_opportunity_scalper.py"
DEFAULT_ASSIGNMENTS = (
    "pullback:OANDA_ACCOUNT_ID_DUM1",
    "momentum:OANDA_ACCOUNT_ID_DUM2",
    "macd_rsi_reversal:OANDA_ACCOUNT_ID_DUM3",
)


def parse_assignment(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("assignment must be strategy:ACCOUNT_KEY")
    strategy, account_key = [part.strip() for part in value.split(":", 1)]
    if strategy not in {"momentum", "pullback", "macd_rsi_reversal"}:
        raise argparse.ArgumentTypeError(f"unknown strategy: {strategy}")
    if not account_key:
        raise argparse.ArgumentTypeError("missing account key")
    return strategy, account_key


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--duration-sec", type=int, default=28800)
    parser.add_argument("--scan-pause-sec", type=float, default=1.0)
    parser.add_argument("--max-spread-pips", type=float, default=1.8)
    parser.add_argument("--candle-workers", type=int, default=16)
    parser.add_argument("--near-miss-limit", type=int, default=5)
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.25)
    parser.add_argument("--max-session-loss-pct", type=float, default=1.0)
    parser.add_argument("--max-units", type=int, default=250)
    parser.add_argument("--max-trades", type=int, default=5)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--child-log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--assignment",
        action="append",
        type=parse_assignment,
        default=[],
        help="strategy:ACCOUNT_KEY. Repeat to add more. Defaults to one account per strategy.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    assignments = args.assignment or [parse_assignment(value) for value in DEFAULT_ASSIGNMENTS]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    args.log_dir.mkdir(parents=True, exist_ok=True)
    launched: list[dict[str, object]] = []
    for strategy, account_key in assignments:
        stderr_path = args.log_dir / f"practice_{strategy}_{account_key.lower()}_{stamp}.err.log"
        command = [
            sys.executable,
            str(SCANNER),
            "--creds",
            str(args.creds),
            "--account-key",
            account_key,
            "--duration-sec",
            str(args.duration_sec),
            "--scan-pause-sec",
            str(args.scan_pause_sec),
            "--max-spread-pips",
            str(args.max_spread_pips),
            "--candle-workers",
            str(args.candle_workers),
            "--near-miss-limit",
            str(args.near_miss_limit),
            "--strategy",
            strategy,
            "--risk-per-trade-pct",
            str(args.risk_per_trade_pct),
            "--max-session-loss-pct",
            str(args.max_session_loss_pct),
            "--max-units",
            str(args.max_units),
            "--max-trades",
            str(args.max_trades),
            "--log-dir",
            str(args.log_dir),
            "--child-log-dir",
            str(args.child_log_dir),
        ]
        if args.dry_run:
            command.append("--dry-run")
        stderr = stderr_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        launched.append(
            {
                "pid": process.pid,
                "strategy": strategy,
                "account_key": account_key,
                "stderr": str(stderr_path),
            }
        )
    for item in launched:
        print(
            f"pid={item['pid']} strategy={item['strategy']} account_key={item['account_key']} stderr={item['stderr']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
