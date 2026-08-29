#!/usr/bin/env python3
"""Run the major GPT checkpoint aggressively on merged practice account -006."""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

try:
    from oanda_practice_pair_rotation_scalper import read_credentials
    from oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_practice_pair_rotation_scalper import read_credentials
    from trad.oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS


ROOT = Path(__file__).resolve().parent
GPT_SCRIPT = ROOT / "oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger.py"
MERGED_DATA = ROOT / "data" / "forex_gpt_manager" / "account_merged_006"
MERGED_MOVEMENTS = ROOT / "data" / "market_movement_ledger" / "account_merged_006"
MERGED_LOCK = ROOT / "data" / "oanda_training_manager" / "state" / "practice_006_merged_order.lock"


def configure_environment() -> str:
    _, account_id = read_credentials(
        DEFAULT_CREDS,
        "OANDA_ACCOUNT_ID_DUM3",
        "",
    )
    if not account_id.endswith("-006"):
        raise SystemExit(
            f"OANDA_ACCOUNT_ID_DUM3 resolved to {account_id[-4:]}, expected -006."
        )
    settings = {
        "OANDA_ENV": "practice",
        "FOREX_ALLOW_LIVE": "false",
        "OANDA_ACCOUNT_ID_GPT": account_id,
        "FOREX_DATA_DIR": str(MERGED_DATA),
        "FOREX_MARKET_MOVEMENT_LEDGER_DIR": str(MERGED_MOVEMENTS),
        "FOREX_ACCOUNT_EXECUTION_LOCK_PATH": str(MERGED_LOCK),
        "FOREX_ACCOUNT_EXECUTION_LOCK_TIMEOUT_SEC": "3",
        "FOREX_ACCOUNT_EXECUTION_LOCK_STALE_SEC": "30",
        "FOREX_EXECUTE_TRADES": "true",
        "FOREX_AUTO_EXECUTE_GPT_ACTIONS": "true",
        "FOREX_SCAN_ON_LAUNCH": "true",
        "FOREX_GPT_CALLS_PER_DAY": "24",
        "FOREX_MIN_MINUTES_BETWEEN_GPT_SCANS": "20",
        "FOREX_LOOP_SLEEP_SECONDS": "15",
        "FOREX_LOCAL_MONITOR_INTERVAL_MINUTES": "10",
        "FOREX_MAX_OPEN_TRADES": "8",
        "FOREX_MAX_NEW_TRADES_PER_SCAN": "4",
        "FOREX_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN": "6.0",
        "FOREX_MAX_GPT_RISK_PCT_PER_TRADE": "1.8",
        "FOREX_MIN_GPT_RISK_PCT": "0.25",
        "FOREX_TARGET_MARGIN_USED_PCT": "68",
        "FOREX_MAX_MARGIN_USED_PCT": "82",
        "FOREX_EMERGENCY_MARGIN_USED_PCT": "90",
        "FOREX_MAX_ONE_CURRENCY_NET_UNITS_PCT": "70",
        "FOREX_ACTION_DEDUPE_MINUTES": "5",
        "FOREX_MAX_ENTRY_SLIPPAGE_PIPS": "0.7",
        "FOREX_BROKER_RECHECK_BEFORE_ACTIONS": "true",
    }
    os.environ.update(settings)
    return account_id


def main() -> None:
    configure_environment()
    sys.argv[0] = str(GPT_SCRIPT)
    runpy.run_path(str(GPT_SCRIPT), run_name="__main__")


if __name__ == "__main__":
    main()
