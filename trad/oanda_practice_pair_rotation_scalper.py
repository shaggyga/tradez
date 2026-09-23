#!/usr/bin/env python3
"""Rotate the guarded OANDA practice scalper across all tradeable FX pairs."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from oanda_practice_eurusd_micro_scalper import (
        BASE_URL,
        DEFAULT_CREDS,
        DEFAULT_LOG_DIR,
        cfg_value,
        normalize_instrument,
        read_text,
    )
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_practice_eurusd_micro_scalper import (
        BASE_URL,
        DEFAULT_CREDS,
        DEFAULT_LOG_DIR,
        cfg_value,
        normalize_instrument,
        read_text,
    )


ROOT = Path(__file__).resolve().parent
SCALPER = ROOT / "oanda_practice_eurusd_micro_scalper.py"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_credentials(creds: Path, account_key: str, account_id: str) -> tuple[str, str]:
    text = read_text(creds)
    token = cfg_value(text, "OANDA_API_KEY", "OANDA_API_TOKEN")
    resolved_account_id = str(account_id or "").strip()
    if not resolved_account_id and account_key:
        resolved_account_id = cfg_value(text, account_key)
    if not resolved_account_id:
        resolved_account_id = cfg_value(text, "OANDA_ACCOUNT_ID_GPT", "OANDA_ACCOUNT_ID_MAJ")
    if not token or not resolved_account_id:
        raise SystemExit("Missing OANDA practice token or account id.")
    return token, resolved_account_id


def oanda_get(token: str, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(
        f"{BASE_URL}{path}",
        params=params,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept-Datetime-Format": "RFC3339",
        },
        timeout=20,
    )
    if response.status_code >= 400:
        raise SystemExit(f"OANDA GET {path} failed: HTTP {response.status_code} {response.text[:300]}")
    return response.json()


def tradeable_currency_instruments(token: str, account_id: str) -> list[str]:
    payload = oanda_get(token, f"/v3/accounts/{account_id}/instruments")
    instruments: list[str] = []
    for instrument in payload.get("instruments") or []:
        if str(instrument.get("type") or "").upper() != "CURRENCY":
            continue
        name = str(instrument.get("name") or "").strip()
        if not name:
            continue
        instruments.append(normalize_instrument(name))
    return sorted(dict.fromkeys(instruments))


def parse_instrument_list(value: str) -> list[str]:
    if not value.strip():
        return []
    return [normalize_instrument(item) for item in re.split(r"[\s,]+", value.strip()) if item]


def log_line(path: Path, event: str, **fields: Any) -> None:
    payload = {"time": utc_now(), "event": event, **fields}
    line = json.dumps(payload, sort_keys=True, default=str)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    print(line, flush=True)


def build_child_args(args: argparse.Namespace, instrument: str) -> list[str]:
    child = [
        sys.executable,
        str(SCALPER),
        "--creds",
        str(args.creds),
        "--account-key",
        args.account_key,
        "--account-id",
        args.account_id,
        "--log-dir",
        str(args.child_log_dir),
        "--instrument",
        instrument,
        "--duration-sec",
        str(args.slice_sec),
        "--max-trades",
        "1",
        "--strategy",
        args.strategy,
        "--shadow-strategies",
        args.shadow_strategies,
        "--risk-per-trade-pct",
        str(args.risk_per_trade_pct),
        "--max-session-loss-pct",
        str(args.max_session_loss_pct),
        "--max-units",
        str(args.max_units),
        "--no-price-stream",
    ]
    if args.dry_run:
        child.append("--dry-run")
    return child


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM3")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--duration-sec", type=int, default=3600)
    parser.add_argument("--slice-sec", type=int, default=45)
    parser.add_argument("--pause-sec", type=float, default=1.0)
    parser.add_argument("--child-log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--rotation-log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--instruments", default="", help="Optional comma/space-separated instrument override.")
    parser.add_argument("--exclude-regex", default="", help="Optional regex to exclude instruments.")
    parser.add_argument("--strategy", choices=("momentum", "pullback", "macd_rsi_reversal"), default="pullback")
    parser.add_argument("--shadow-strategies", default="momentum,macd_rsi_reversal")
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.25)
    parser.add_argument("--max-session-loss-pct", type=float, default=1.0)
    parser.add_argument("--max-units", type=int, default=250)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.child_log_dir.mkdir(parents=True, exist_ok=True)
    args.rotation_log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    selector = re.sub(r"[^a-z0-9]+", "_", str(args.account_key or args.account_id or "default").lower()).strip("_")
    rotation_log = args.rotation_log_dir / f"practice_pair_rotation_{selector}_{stamp}.jsonl"

    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    instruments = parse_instrument_list(args.instruments)
    if not instruments:
        instruments = tradeable_currency_instruments(token, account_id)
    if args.exclude_regex:
        exclude = re.compile(args.exclude_regex)
        instruments = [instrument for instrument in instruments if not exclude.search(instrument)]
    if not instruments:
        raise SystemExit("No instruments selected for rotation.")

    log_line(
        rotation_log,
        "rotation_start",
        account_suffix=account_id[-4:],
        duration_sec=args.duration_sec,
        slice_sec=args.slice_sec,
        instrument_count=len(instruments),
        instruments=instruments,
        child_log_dir=str(args.child_log_dir),
        dry_run=args.dry_run,
    )

    stop_at = time.monotonic() + args.duration_sec
    cycle = 0
    child_runs = 0
    while time.monotonic() < stop_at:
        for instrument in instruments:
            if time.monotonic() >= stop_at:
                break
            child_runs += 1
            started = time.monotonic()
            command = build_child_args(args, instrument)
            log_line(rotation_log, "slice_start", cycle=cycle, child_run=child_runs, instrument=instrument)
            completed = subprocess.run(command, cwd=ROOT.parent)
            elapsed = time.monotonic() - started
            log_line(
                rotation_log,
                "slice_end",
                cycle=cycle,
                child_run=child_runs,
                instrument=instrument,
                return_code=completed.returncode,
                elapsed_sec=round(elapsed, 2),
            )
            if args.pause_sec > 0.0 and time.monotonic() < stop_at:
                time.sleep(args.pause_sec)
        cycle += 1

    log_line(rotation_log, "rotation_end", cycles=cycle, child_runs=child_runs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
