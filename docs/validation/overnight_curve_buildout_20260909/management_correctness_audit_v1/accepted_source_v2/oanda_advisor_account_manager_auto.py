#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
OANDA Forex GPT Advisor Account Manager — Two-Script Auto Stack v1
====================================

Single-account GPT/advisor script: uses OANDA_ACCOUNT_ID_MAJ (or OANDA_ACCOUNT_ID_GPT) and can route all discovered OANDA currency pairs.

Always-on OANDA forex manager:
- Runs continuously. No Task Scheduler required.
- Makes 16 GPT/news portfolio-management calls per trading day by default on normal trading days.
- Every GPT call manages the whole account: current positions first, then new opens.
- Discovers all tradeable OANDA CURRENCY instruments from the account.
- Strategy split: this file is the GPT/news/advisor account; the separate technical scout file handles local spike capture.
- Loads API credentials from creds/creds.txt only; all tunable bot settings are embedded in this manager file.
- Uses OANDA practice by default unless OANDA_ENV=live and FOREX_ALLOW_LIVE=1 are set.
- GPT decides outlook_confidence and requested risk_pct. The script only enforces account guardrails.
- v5.10 uses global-best selection: evaluate all supplied pairs, then choose the best trades without USD/cross quotas or placeholder candidates.
- v5.11 adds a fast local M5 event scanner: detects synchronized volatile moves across all pairs, logs them, can trigger a GPT management scan, and can optionally place small pre-authorized scout trades.
- v5.14 keeps credentials in creds/creds.txt, embeds all bot settings in this one manager file, and adds Sunday reopen/profit-memory protection.
- v5.15 turns local scout trades into real technical impulse trades: hard-confirmed event scouts no longer need GPT event permission. Sunday reopen blocks only unrelated fresh opens; closes, flips, and confirmed reversal scouts remain allowed.
- v5.17 upgrades scouts from naive chase signals to M1 early-trigger/EV scoring: all pairs are scanned, late exhaustion moves are skipped, positive pair/theme/time profiles can scale risk, and scout fills are logged as event_scout actions.
- v5.17 adds a volatile-pair scout lane so ZAR/TRY/NOK/SEK/CZK/MXN/HKD style moves are not treated like ordinary crosses; it still scans all pairs, but volatile pairs get separate spread, stop, event-age, EV, and risk handling.

Install:
    pip install requests

Typical run:
    python oanda_advisor_account_manager_auto.py

Useful test commands:
    python oanda_forex_gpt_manager_v5_17.py --print-config
    python oanda_forex_gpt_manager_v5_17.py --scan-now --once --dry-run
    python oanda_forex_gpt_manager_v5_17.py --monitor-now --once

Credentials stay in your local creds file. Bot settings are edited in EMBEDDED_CONFIG inside this same manager file.

This is automation code, not financial advice. Leveraged FX can lose money quickly.
Run OANDA practice first and inspect the CSV logs before allowing live execution.
"""

from __future__ import annotations

try:
    import oanda_trade_reconciliation_v1 as trade_reconciliation
except ModuleNotFoundError:
    from trad import oanda_trade_reconciliation_v1 as trade_reconciliation

import argparse
import ast
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import shutil
import sys
import time
import traceback
from dataclasses import dataclass, asdict, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover on very old Python
    ZoneInfo = None


# =============================================================================
# Time / paths
# =============================================================================

UTC = dt.timezone.utc
NY_TZ_NAME = "America/New_York"
NY = ZoneInfo(NY_TZ_NAME) if ZoneInfo else None
SCRIPT_DIR = Path(__file__).resolve().parent


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def ny_now() -> dt.datetime:
    if NY:
        return utc_now().astimezone(NY)
    return dt.datetime.now()


def iso_utc(ts: Optional[dt.datetime] = None) -> str:
    return (ts or utc_now()).astimezone(UTC).isoformat()


def iso_ny(ts: Optional[dt.datetime] = None) -> str:
    t = ts or utc_now()
    return t.astimezone(NY).isoformat() if NY else t.isoformat()


def log(msg: str) -> None:
    print(f"[{iso_ny()}] {msg}", flush=True)


# =============================================================================
# Credentials and embedded bot settings loading. No import/exec of creds.
#
# Credentials are loaded only from creds/creds.txt style files.
# Tunable FOREX_* settings live in EMBEDDED_CONFIG below so this remains a
# single manager file. Environment variables may still override settings for
# emergency/manual runs, but no separate config file is read.
# =============================================================================

def _unique_paths(paths: Iterable[Path]) -> List[Path]:
    out: List[Path] = []
    seen = set()
    for p in paths:
        try:
            key = str(p.expanduser().resolve(strict=False)).lower()
        except Exception:
            key = str(p).lower()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def candidate_creds_paths() -> List[Path]:
    env_path = os.getenv("TRAD_CREDS_PATH", "").strip()
    paths: List[Path] = []
    if env_path:
        paths.append(Path(env_path))
    paths.extend([
        SCRIPT_DIR / "creds",
        SCRIPT_DIR / "creds.txt",
        SCRIPT_DIR / "creds.py",
        Path.cwd() / "creds",
        Path.cwd() / "creds.txt",
        Path.cwd() / "creds.py",
    ])
    return _unique_paths(paths)


def parse_python_literal_creds(text: str, filename: str = "<creds>") -> Dict[str, Any]:
    creds: Dict[str, Any] = {}
    try:
        tree = ast.parse(text, filename=filename)
    except SyntaxError:
        return creds
    for node in tree.body:
        target_name: Optional[str] = None
        value_node: Optional[ast.AST] = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target_name = node.targets[0].id
            value_node = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name = node.target.id
            value_node = node.value
        if not target_name or value_node is None:
            continue
        try:
            creds[target_name] = ast.literal_eval(value_node)
        except Exception:
            # Do not execute/import convenience expressions.
            continue
    return creds


def parse_env_style_creds(text: str) -> Dict[str, Any]:
    creds: Dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if not key or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
            try:
                creds[key] = ast.literal_eval(val)
            except Exception:
                creds[key] = val[1:-1]
        else:
            low = val.lower()
            if low in {"true", "false"}:
                creds[key] = low == "true"
            else:
                try:
                    creds[key] = ast.literal_eval(val)
                except Exception:
                    creds[key] = val
    return creds


CREDS_LOADED_PATH: Optional[Path] = None
CREDS_LOAD_ERROR = ""


def load_creds_file() -> Dict[str, Any]:
    global CREDS_LOADED_PATH, CREDS_LOAD_ERROR
    checked: List[str] = []
    errors: List[str] = []
    for path in candidate_creds_paths():
        checked.append(str(path))
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8-sig")
            creds = parse_python_literal_creds(text, str(path))
            env_creds = parse_env_style_creds(text)
            merged = {**env_creds, **creds}
            if merged:
                CREDS_LOADED_PATH = path
                CREDS_LOAD_ERROR = ""
                return merged
        except Exception as exc:
            errors.append(f"{path}: {type(exc).__name__}: {exc}")
    CREDS_LOAD_ERROR = "; ".join(errors) if errors else "No creds file found. Checked: " + "; ".join(checked)
    return {}


CREDS = load_creds_file()


CONFIG_LOADED_PATH: Optional[Path] = None
CONFIG_LOAD_ERROR = "External config files disabled in v5.17; using EMBEDDED_CONFIG inside this manager script."


# =============================================================================
# Embedded bot settings
# =============================================================================
# Edit values here when you want to change bot behavior.
# Keep API keys/account id in your local creds/creds.txt file only.
EMBEDDED_CONFIG: Dict[str, Any] = {
    # Broker / model mode
    "OANDA_ENV": "practice",                 # practice or live
    "FOREX_ALLOW_LIVE": False,                # must be True for live execution
    "OPENAI_MODEL": "gpt-4.1-mini",
    "ENABLE_OPENAI_WEB_SEARCH": True,
    "FOREX_DISABLE_HTTP_KEEPALIVE": True,
    "FOREX_HTTP_MAX_RETRIES": 3,
    "FOREX_HTTP_RETRY_SLEEP_SECONDS": 2.0,

    # Execution
    "FOREX_EXECUTE_TRADES": True,             # practice executes by default
    "FOREX_AUTO_EXECUTE_GPT_ACTIONS": True,
    "FOREX_SCAN_ON_LAUNCH": True,
    "FOREX_DRY_RUN_STATE_ORDERS": False,

    # Strategy split is GPT/advisor-vs-technical, not USD-vs-non-USD.
    # This script is the GPT/news/advisor account. By default it may evaluate
    # every discovered OANDA currency instrument. Use FOREX_GPT_EXCLUDE_PAIRS
    # only if you explicitly want to block a pair from the GPT account.
    "FOREX_MULTI_ACCOUNT_ENABLED": True,
    "FOREX_MAJOR_USD_PAIRS": [
        "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF",
        "USD_CAD", "AUD_USD", "NZD_USD",
    ],
    "FOREX_GPT_ALL_PAIRS": True,
    "FOREX_GPT_EXCLUDE_PAIRS": [],
    "FOREX_GPT_RELIABLE_PAIRS": [],
    "FOREX_TECHNICAL_SCOUT_PAIRS": [],

    # Scheduled GPT portfolio-management calls: 16/day, session-based NY times.
    "FOREX_GPT_CALLS_PER_DAY": 16,
    "FOREX_GPT_CALL_TIMES_NY": [
        "00:15", "01:45", "03:15", "04:45", "06:15", "07:45",
        "09:15", "10:45", "12:15", "13:45", "15:15", "16:45",
        "18:15", "19:45", "21:15", "22:45",
    ],
    "FOREX_FRIDAY_GPT_CALL_TIMES_NY": [
        "00:15", "01:45", "03:15", "04:45", "06:15", "07:45",
        "09:15", "10:45", "12:15", "13:45", "14:45", "15:45",
    ],
    "FOREX_SCHEDULE_WINDOW_MINUTES": 10,
    "FOREX_MIN_MINUTES_BETWEEN_GPT_SCANS": 75,
    "FOREX_LOOP_SLEEP_SECONDS": 60,
    "FOREX_LOCAL_MONITOR_INTERVAL_MINUTES": 60,

    # Weekend / Sunday-reopen protection. The normal schedule is not enough for
    # weekend gaps: FX is closed, then reprices quickly after Sunday open.
    # These scans are separate from the 12 normal GPT calls/day.
    "FOREX_WEEKEND_SUMMARY_SCAN_ENABLED": True,
    "FOREX_WEEKEND_SUMMARY_SCAN_TIME_NY": "16:45",
    "FOREX_WEEKEND_SUMMARY_SCAN_WINDOW_MINUTES": 20,
    "FOREX_WEEKEND_SUMMARY_USE_GPT": True,

    "FOREX_SUNDAY_REOPEN_MANAGER_ENABLED": True,
    "FOREX_SUNDAY_REOPEN_SCAN_TIME_NY": "17:15",
    "FOREX_SUNDAY_REOPEN_SCAN_WINDOW_MINUTES": 35,
    "FOREX_SUNDAY_REOPEN_USE_GPT": True,
    # Reopen does NOT block all new trades anymore. It blocks only unrelated fresh portfolio expansion.
    # Existing-position actions and defensive FLIP orders remain allowed. Event scouts are allowed after a short spread warmup.
    "FOREX_SUNDAY_REOPEN_BLOCK_UNRELATED_NEW_TRADES_MINUTES": 20,
    "FOREX_SUNDAY_REOPEN_BLOCK_NEW_TRADES_MINUTES": 20,   # backward-compatible alias
    "FOREX_SUNDAY_REOPEN_BLOCK_SCOUTS_MINUTES": 5,
    "FOREX_SUNDAY_REOPEN_EVENT_SCANNER_DELAY_MINUTES": 5,
    "FOREX_SUNDAY_REOPEN_FORCE_EXISTING_FIRST": True,
    "FOREX_REOPEN_ALLOW_FLIPS": True,
    "FOREX_REOPEN_ALLOW_REVERSAL_SCOUTS": True,
    "FOREX_REOPEN_ALLOW_CONFIRMED_EVENT_SCOUTS": True,

    # Hard local protection around reopen. GPT may still think a thesis is valid,
    # but the script can override if a trade gave back real profit or if NAV gaps.
    "FOREX_TRADE_PROFIT_MEMORY_ENABLED": True,
    "FOREX_REOPEN_HARD_PROTECTION_ENABLED": True,
    "FOREX_REOPEN_CLOSE_IF_WAS_PROFIT_NOW_NEGATIVE": True,
    "FOREX_REOPEN_MIN_PRIOR_PROFIT_USD": 0.75,
    "FOREX_REOPEN_NEGATIVE_PL_THRESHOLD_USD": -0.05,
    "FOREX_REOPEN_CLOSE_IF_PROFIT_GIVEBACK_USD": 1.00,
    "FOREX_REOPEN_CLOSE_IF_NAV_DROP_PCT": 6.0,
    "FOREX_REOPEN_TIGHTEN_ALL_EXISTING": True,

    # Aggressive but guarded sizing/margin
    "FOREX_MAX_OPEN_TRADES": 10,
    "FOREX_MAX_NEW_TRADES_PER_SCAN": 5,
    "FOREX_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN": 12.0,
    "FOREX_MAX_GPT_RISK_PCT_PER_TRADE": 6.0,
    "FOREX_MIN_GPT_RISK_PCT": 0.10,
    "FOREX_TARGET_MARGIN_USED_PCT": 65.0,
    "FOREX_MAX_MARGIN_USED_PCT": 85.0,
    "FOREX_EMERGENCY_MARGIN_USED_PCT": 92.0,
    "FOREX_LOCAL_EMERGENCY_CLOSE": True,
    "FOREX_MAX_ONE_CURRENCY_NET_UNITS_PCT": 80.0,

    # Order guardrails
    "FOREX_REQUIRE_STOP_LOSS_ON_OPEN": True,
    "FOREX_REQUIRE_TAKE_PROFIT_ON_OPEN": False,
    "FOREX_ALLOW_TRAILING_STOP_ON_OPEN": True,
    "FOREX_MAX_SPREAD_PIPS_DEFAULT": 4.0,
    "FOREX_MAX_SPREAD_PIPS_EXOTIC": 12.0,
    "FOREX_MAX_SPREAD_TO_STOP_RATIO": 0.20,
    "FOREX_MAX_ENTRY_SLIPPAGE_PIPS": 3.0,
    "FOREX_SCOUT_MAX_SPREAD_TO_STOP_RATIO": 0.45,
    "FOREX_PARTIAL_CLOSE_MIN_PCT": 10.0,
    "FOREX_PARTIAL_CLOSE_MAX_PCT": 75.0,

    # Restart-safety / duplicate action protection
    "FOREX_BROKER_RECHECK_BEFORE_ACTIONS": True,
    "FOREX_ACTION_DEDUPE_MINUTES": 30,
    "FOREX_STARTUP_RECONCILE_KEEP": 50,
    "FOREX_RETRY_UNSAFE_BROKER_WRITES": False,
    "FOREX_REQUIRE_POSITION_ACTION_PER_OPEN_TRADE": True,

    # Daily recaps / memory
    "FOREX_DAILY_RECAP_ENABLED": True,
    "FOREX_DAILY_RECAP_RECENT_DAYS_FOR_PROMPT": 5,
    "FOREX_DAILY_RECAP_LOOKBACK_DAYS": 14,
    "FOREX_REQUIRE_CROSS_PAIR_COVERAGE": True,
    "FOREX_MIN_NON_USD_CANDIDATES": 0,         # no quota; all pairs compete globally
    "FOREX_ALL68_BUCKETED_DISCOVERY_ENABLED": True,
    "FOREX_ALL68_BUCKET_TOP_PER_BUCKET": 4,
    "FOREX_ALL68_BUCKET_MIN_REVIEWED": 1,
    "FOREX_ALL68_BUCKET_REQUIRE_REAL_REVIEW": True,

    # Fast local M1 event scanner and scout trades.
    # v5.17 scans all 68 pairs on completed M1 candles, then scores early continuation EV.
    "FOREX_EVENT_SCANNER_ENABLED": True,
    "FOREX_EVENT_SCAN_INTERVAL_SECONDS": 60,
    "FOREX_EVENT_CANDLE_GRANULARITY": "M1",
    "FOREX_EVENT_CANDLE_COUNT": 120,
    "FOREX_EVENT_WINDOWS_MINUTES": [1, 3, 5, 10, 15, 30],
    "FOREX_EVENT_MIN_BASKET_PAIRS": 2,
    "FOREX_EVENT_MIN_MAJOR_NET_PIPS": 6.0,
    "FOREX_EVENT_MIN_CROSS_NET_PIPS": 10.0,
    "FOREX_EVENT_MIN_EXOTIC_NET_PIPS": 45.0,
    "FOREX_EVENT_MIN_MOVE_TO_SPREAD_RATIO": 2.4,
    "FOREX_EVENT_MAX_SPREAD_PIPS_MAJOR": 5.0,
    "FOREX_EVENT_MAX_SPREAD_PIPS_CROSS": 8.0,
    "FOREX_EVENT_MAX_SPREAD_PIPS_EXOTIC": 180.0,
    "FOREX_EVENT_TRIGGERED_GPT_ENABLED": True,
    "FOREX_EVENT_MIN_MINUTES_BETWEEN_GPT_SCANS": 30,

    # Scout trades are ON and can execute from hard local technical impulse confirmation.
    # GPT permissions are still saved/used as context, but they are no longer required for scouts.
    "FOREX_EVENT_SCOUT_TRADES_ENABLED": True,
    "FOREX_EVENT_REQUIRE_GPT_PERMISSION": False,
    "FOREX_EVENT_SCOUT_RISK_PCT": 0.35,
    "FOREX_EVENT_MAX_TOTAL_SCOUT_RISK_PCT": 2.0,
    "FOREX_EVENT_MAX_SCOUT_TRADES_PER_EVENT": 2,
    "FOREX_EVENT_SCOUT_STOP_PIPS_MAJOR": 14.0,
    "FOREX_EVENT_SCOUT_STOP_PIPS_CROSS": 22.0,
    "FOREX_EVENT_SCOUT_STOP_PIPS_EXOTIC": 250.0,
    "FOREX_EVENT_SCOUT_TAKE_PROFIT_R_MULTIPLE": 2.0,
    "FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_MAJOR": 12.0,
    "FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_CROSS": 20.0,
    "FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_EXOTIC": 200.0,
    "FOREX_EVENT_PERMISSION_DEFAULT_EXPIRY_HOURS": 8.0,

    # v5.17 scout EV / earlyness classifier. This stops the bot from blindly chasing
    # a completed volatility candle. It still considers every OANDA pair, but it
    # sizes/filters by earlyness, basket confirmation, spread regime, and empirical
    # follow-through profile when available.
    "FOREX_SCOUT_EV_SCORING_ENABLED": True,
    "FOREX_SCOUT_MIN_EV_SCORE_TO_TRADE": 72.0,
    "FOREX_SCOUT_MAX_EVENT_AGE_MINUTES_FOR_CONTINUATION": 20.0,
    "FOREX_SCOUT_MAX_REPEAT_TRIGGERS_PER_KEY": 2,
    "FOREX_SCOUT_EVENT_MEMORY_RESET_MINUTES": 180.0,
    "FOREX_SCOUT_SKIP_LATE_EXHAUSTION": True,
    "FOREX_SCOUT_LATE_EXHAUSTION_AGE_MINUTES": 25.0,
    "FOREX_SCOUT_LATE_EXHAUSTION_PROFILE_EV30_MAX": -5.0,
    "FOREX_SCOUT_ALLOW_UNKNOWN_PROFILE_BASE_RISK": True,
    "FOREX_SCOUT_REQUIRE_POSITIVE_PROFILE_FOR_RISK_ABOVE_BASE": True,
    "FOREX_SCOUT_PROFILE_CSV_PATH": "",
    "FOREX_SCOUT_AUTO_LOAD_LATEST_PROFILE": True,
    "FOREX_SCOUT_PROFILE_MIN_ROWS": 3,
    "FOREX_SCOUT_PROFILE_REFRESH_SECONDS": 300,
    "FOREX_SCOUT_RISK_PCT_SCORE_70": 0.25,
    "FOREX_SCOUT_RISK_PCT_SCORE_80": 0.50,
    "FOREX_SCOUT_RISK_PCT_SCORE_90": 0.75,
    "FOREX_SCOUT_RISK_PCT_SCORE_95": 1.00,
    "FOREX_SCOUT_MAX_RISK_PCT_WITH_UNKNOWN_PROFILE": 0.35,
    "FOREX_SCOUT_MAX_RISK_PCT_WITH_NEGATIVE_PROFILE": 0.25,
    "FOREX_SCOUT_PROFILE_FALLBACK": {
        "USD_NOK": {"rows": 16, "avg_follow_15m": 25.17, "win_rate_15m": 0.4375, "avg_follow_30m": 51.91, "win_rate_30m": 0.6875},
        "EUR_NOK": {"rows": 20, "avg_follow_15m": 15.53, "win_rate_15m": 0.40, "avg_follow_30m": 16.49, "win_rate_30m": 0.55},
        "AUD_JPY": {"rows": 12, "avg_follow_15m": 0.87, "win_rate_15m": 0.50, "avg_follow_30m": -1.86, "win_rate_30m": 0.4167},
        "EUR_AUD": {"rows": 9, "avg_follow_15m": 0.52, "win_rate_15m": 0.4444, "avg_follow_30m": -2.88, "win_rate_30m": 0.2222}
    },

    # v5.17 volatile scout lane. These pairs were the repeated largest missed
    # movers in the day-window audits. They still have to pass spread/earlyness/
    # basket/account checks, but they are no longer rejected under normal-cross
    # thresholds such as 8-pip max spread. This is a small-risk participation
    # lane, not permission to size them like EUR/USD.
    "FOREX_SCOUT_VOLATILE_LANE_ENABLED": True,
    "FOREX_SCOUT_VOLATILE_PAIRS": [
        "CHF_ZAR", "EUR_ZAR", "GBP_ZAR", "USD_ZAR",
        "EUR_TRY", "USD_TRY",
        "USD_CZK", "EUR_CZK",
        "USD_NOK", "EUR_NOK",
        "USD_SEK", "EUR_SEK",
        "USD_MXN", "EUR_MXN",
        "HKD_JPY",
        "CHF_HKD", "GBP_HKD", "EUR_HKD", "CAD_HKD", "AUD_HKD", "USD_HKD",
    ],
    "FOREX_SCOUT_VOLATILE_MIN_NET_PIPS": 90.0,
    "FOREX_SCOUT_VOLATILE_MAX_SPREAD_PIPS": 260.0,
    "FOREX_SCOUT_VOLATILE_MIN_MOVE_TO_SPREAD_RATIO": 2.2,
    "FOREX_SCOUT_VOLATILE_MIN_BASKET_PAIRS": 2,
    "FOREX_SCOUT_VOLATILE_MAX_EVENT_AGE_MINUTES": 15.0,
    "FOREX_SCOUT_VOLATILE_REPEAT_TRIGGER_CAP": 2,
    "FOREX_SCOUT_VOLATILE_BASE_RISK_PCT": 0.20,
    "FOREX_SCOUT_VOLATILE_MAX_RISK_PCT": 0.50,
    "FOREX_SCOUT_VOLATILE_ALLOW_UNKNOWN_PROFILE": True,
    "FOREX_SCOUT_VOLATILE_ALLOW_NEGATIVE_PROFILE_IF_EARLY": True,
    "FOREX_SCOUT_VOLATILE_MIN_EV_SCORE_TO_TRADE": 62.0,
    "FOREX_SCOUT_VOLATILE_STOP_PIPS": 320.0,
    "FOREX_SCOUT_VOLATILE_STOP_NET_MULTIPLIER": 0.65,
    "FOREX_SCOUT_VOLATILE_STOP_SPREAD_MULTIPLIER": 3.5,
    "FOREX_SCOUT_VOLATILE_TRAILING_STOP_PIPS": 250.0,
    "FOREX_SCOUT_VOLATILE_MAX_SPREAD_TO_STOP_RATIO": 0.90,

    # Price/candle context for GPT
    "FOREX_PRICE_CHUNK_SIZE": 40,
    "FOREX_CANDLE_GRANULARITY": "H1",
    "FOREX_CANDLE_COUNT": 80,
    "FOREX_INCLUDE_CANDLES": True,
    "FOREX_MAX_CANDLE_INSTRUMENTS": 0,
    "FOREX_ACCOUNT_CURRENCY": "USD",

    # Shutdown / fade-to-flat mode
    "FOREX_SHUTDOWN_MODE": "off",             # off, fade, close_now
    "FOREX_SHUTDOWN_SKIP_GPT": True,
    "FOREX_SHUTDOWN_BLOCK_NEW_TRADES": True,
    "FOREX_SHUTDOWN_MIN_MINUTES_BETWEEN_PASSES": 15,
    "FOREX_SHUTDOWN_PARTIAL_CLOSE_PCT": 25.0,
    "FOREX_SHUTDOWN_MIN_PROFIT_PIPS_TO_PARTIAL": 4.0,
    "FOREX_SHUTDOWN_MIN_PROFIT_USD_TO_PARTIAL": 0.05,
    "FOREX_SHUTDOWN_LOCK_PROFIT_RATIO": 0.55,
    "FOREX_SHUTDOWN_MIN_LOCK_PROFIT_PIPS": 1.5,
    "FOREX_SHUTDOWN_MAX_LOSS_PIPS": 8.0,
    "FOREX_SHUTDOWN_CLOSE_IF_LOSS_PIPS_BEYOND": 18.0,
    "FOREX_SHUTDOWN_FULL_CLOSE_PROFIT_PIPS": 0.0,
    "FOREX_SHUTDOWN_STOP_GAP_PIPS_DEFAULT": 6.0,
    "FOREX_SHUTDOWN_STOP_GAP_PIPS_EXOTIC": 12.0,
    "FOREX_SHUTDOWN_MIN_STOP_GAP_PIPS": 1.5,

    # Paths
    "FOREX_DATA_DIR": str(SCRIPT_DIR / "data" / "forex_gpt_manager"),
}


CONFIG: Dict[str, Any] = dict(EMBEDDED_CONFIG)


CREDENTIAL_SETTING_NAMES = {
    "OPENAI_API_KEY", "openai_api_key", "openaiapi", "OPENAI_KEY",
    "OANDA_API_KEY", "OANDA_API_TOKEN", "oanda_api_key", "oandaapi",
    "OANDA_ACCOUNT_ID", "oanda_account_id", "OANDA_ACCOUNT",
    "OANDA_ACCOUNT_ID_GPT", "OANDA_ACCOUNT_ID_ADVISOR",
    "OANDA_ACCOUNT_ID_MAJ", "OANDA_ACCOUNT_ID_MAJOR", "OANDA_ACCOUNT_ID_USD_MAJORS",
    "OANDA_ACCOUNT_ID_VOL", "OANDA_ACCOUNT_ID_VOLATILE", "OANDA_ACCOUNT_ID_OTHER",
}


def cfg_raw(*names: str, default: Any = None) -> Any:
    for name in names:
        if name in os.environ and str(os.environ[name]).strip() != "":
            return os.environ[name]
        if name in CONFIG and str(CONFIG[name]).strip() != "":
            return CONFIG[name]
        # creds is intentionally only for credentials, not FOREX_* behavior settings.
        if name in CREDENTIAL_SETTING_NAMES and name in CREDS and str(CREDS[name]).strip() != "":
            return CREDS[name]
    return default


def cfg_str(*names: str, default: str = "") -> str:
    val = cfg_raw(*names, default=default)
    return "" if val is None else str(val).strip()


def cfg_bool(*names: str, default: bool = False) -> bool:
    val = cfg_raw(*names, default=default)
    if isinstance(val, bool):
        return val
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def cfg_int(*names: str, default: int = 0) -> int:
    val = cfg_raw(*names, default=default)
    try:
        return int(float(str(val).strip()))
    except Exception:
        return default


def cfg_float(*names: str, default: float = 0.0) -> float:
    val = cfg_raw(*names, default=default)
    try:
        return float(str(val).strip())
    except Exception:
        return default


def cfg_list(*names: str, default: Optional[List[str]] = None) -> List[str]:
    val = cfg_raw(*names, default=None)
    if val is None:
        return list(default or [])
    if isinstance(val, (list, tuple, set)):
        return [str(x).strip() for x in val if str(x).strip()]
    text = str(val).strip()
    if not text:
        return list(default or [])
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple, set)):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except Exception:
        pass
    return [x.strip() for x in re.split(r"[,;\s]+", text) if x.strip()]


def cfg_int_list(*names: str, default: Optional[List[int]] = None) -> List[int]:
    out: List[int] = []
    for raw in cfg_list(*names, default=[str(x) for x in (default or [])]):
        try:
            out.append(int(float(str(raw).strip())))
        except Exception:
            continue
    return out or list(default or [])


def cfg_dict(*names: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    val = cfg_raw(*names, default=None)
    if val is None:
        return dict(default or {})
    if isinstance(val, dict):
        return dict(val)
    text = str(val).strip()
    if not text:
        return dict(default or {})
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, dict):
            return dict(parsed)
    except Exception:
        pass
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return dict(parsed)
    except Exception:
        pass
    return dict(default or {})


# =============================================================================
# Generic utility
# =============================================================================

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    if hi < lo:
        hi = lo
    return max(lo, min(hi, x))


def chunks(xs: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    n = max(1, int(n))
    for i in range(0, len(xs), n):
        yield xs[i:i+n]


def stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def parse_hhmm(s: str) -> Tuple[int, int]:
    h, m = str(s).strip().split(":", 1)
    return int(h), int(m)


def valid_hhmm(s: str) -> bool:
    try:
        h, m = parse_hhmm(s)
        return 0 <= h <= 23 and 0 <= m <= 59
    except Exception:
        return False


def generate_even_call_times(calls_per_day: int, minute_offset: int) -> List[str]:
    calls_per_day = max(1, min(24, calls_per_day))
    minute_offset = max(0, min(59, minute_offset))
    step_minutes = 24 * 60 / calls_per_day
    out = []
    for i in range(calls_per_day):
        total = int(round(i * step_minutes)) + minute_offset
        total %= 24 * 60
        out.append(f"{total // 60:02d}:{total % 60:02d}")
    return sorted(set(out))


DEFAULT_SESSION_CALL_TIMES_NY_8 = [
    "00:30",  # Asia mid-session: JPY/AUD/NZD and overnight risk tone
    "02:45",  # pre-London positioning
    "04:15",  # London open follow-through after spreads/first reaction settle
    "07:45",  # pre-NY / pre-US-data positioning
    "09:45",  # NY open / US data reaction after first impulse
    "11:30",  # London-NY overlap management
    "14:30",  # NY afternoon profit/risk check
    "20:30",  # Asia restart after daily rollover normalizes
]

DEFAULT_FRIDAY_SESSION_CALL_TIMES_NY_8 = [
    "00:30",
    "02:45",
    "04:15",
    "07:45",
    "09:45",
    "11:30",
    "14:30",
    "15:45",  # final Friday reduce/close/tighten check before the 16:59 NY close
]

DEFAULT_SESSION_CALL_TIMES_NY_12 = [
    "00:30",  # Asia mid-session check
    "02:00",  # early London preparation
    "03:15",  # London open follow-through
    "04:45",  # London trend confirmation
    "06:15",  # Europe late-morning risk check
    "07:45",  # pre-NY / pre-US-data positioning
    "09:15",  # post-US data / NY open reaction
    "10:45",  # London-NY overlap continuation
    "12:15",  # NY lunch / profit-protection review
    "13:45",  # NY afternoon management
    "15:30",  # pre-rollover reduce/tighten check
    "20:30",  # Asia restart after daily rollover normalizes
]

DEFAULT_FRIDAY_SESSION_CALL_TIMES_NY_12 = [
    "00:30",
    "02:00",
    "03:15",
    "04:45",
    "06:15",
    "07:45",
    "09:15",
    "10:45",
    "12:15",
    "13:45",
    "14:45",
    "15:45",  # final Friday reduce/close/tighten check before the 16:59 NY close
]


def default_call_times_for_count(calls_per_day: int, minute_offset: int) -> List[str]:
    c = int(calls_per_day)
    if c == 12:
        return list(DEFAULT_SESSION_CALL_TIMES_NY_12)
    if c == 8:
        return list(DEFAULT_SESSION_CALL_TIMES_NY_8)
    return generate_even_call_times(calls_per_day, minute_offset)


def default_friday_call_times_for_count(calls_per_day: int, normal_times: List[str]) -> List[str]:
    c = int(calls_per_day)
    if c == 12:
        return list(DEFAULT_FRIDAY_SESSION_CALL_TIMES_NY_12)
    if c == 8:
        return list(DEFAULT_FRIDAY_SESSION_CALL_TIMES_NY_8)
    return list(normal_times)


def normalize_shutdown_mode(value: Any) -> str:
    """Normalize shutdown/drain mode aliases.

    off       = normal trading.
    fade      = stop opening and gradually protect/exit current trades.
    close_now = close all open trades as soon as the broker is reachable.
    """
    raw = str(value or "off").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"", "0", "false", "no", "n", "off", "normal", "none"}:
        return "off"
    if raw in {"1", "true", "yes", "y", "on", "fade", "drain", "shutdown", "wind_down", "winddown", "graceful", "graceful_shutdown"}:
        return "fade"
    if raw in {"close", "close_all", "close_now", "panic", "liquidate", "liquidate_now"}:
        return "close_now"
    return "off"


def fx_market_closed(t: Optional[dt.datetime] = None) -> bool:
    """Approximate retail FX closed window in New York time."""
    n = t or ny_now()
    weekday = n.weekday()  # Monday=0
    hm = n.hour * 60 + n.minute
    daily_break_start = cfg_int("FOREX_DAILY_BREAK_START_MINUTE_NY", default=16 * 60 + 59)
    daily_break_end = cfg_int("FOREX_DAILY_BREAK_END_MINUTE_NY", default=17 * 60 + 5)
    friday_close = cfg_int("FOREX_FRIDAY_CLOSE_MINUTE_NY", default=16 * 60 + 59)
    sunday_open = cfg_int("FOREX_SUNDAY_OPEN_MINUTE_NY", default=17 * 60 + 5)
    if weekday == 4 and hm >= friday_close:
        return True
    if weekday == 5:
        return True
    if weekday == 6 and hm < sunday_open:
        return True
    if daily_break_start <= hm < daily_break_end:
        return True
    return False


def ny_minute_of_day(t: Optional[dt.datetime] = None) -> int:
    n = t or ny_now()
    return n.hour * 60 + n.minute


def is_sunday_reopen_day(t: Optional[dt.datetime] = None) -> bool:
    n = t or ny_now()
    return n.weekday() == 6


def minutes_since_sunday_open(t: Optional[dt.datetime] = None) -> Optional[int]:
    n = t or ny_now()
    if n.weekday() != 6:
        return None
    sunday_open = cfg_int("FOREX_SUNDAY_OPEN_MINUTE_NY", default=17 * 60 + 5)
    return ny_minute_of_day(n) - sunday_open


def within_hhmm_window(hhmm: str, window_minutes: int, t: Optional[dt.datetime] = None) -> bool:
    n = t or ny_now()
    if not valid_hhmm(hhmm):
        return False
    h, m = parse_hhmm(hhmm)
    scheduled = n.replace(hour=h, minute=m, second=0, microsecond=0)
    late = (n - scheduled).total_seconds() / 60.0
    return 0 <= late <= max(0, window_minutes)


def now_key_date(t: Optional[dt.datetime] = None) -> str:
    return (t or ny_now()).strftime("%Y-%m-%d")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        try:
            backup = path.with_suffix(path.suffix + f".corrupt_{int(time.time())}.bak")
            shutil.copy2(path, backup)
            log(f"[WARN] Corrupt JSON backed up to {backup}")
        except Exception:
            pass
        return default


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(path)


def append_csv(path: Path, row: Dict[str, Any], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    if exists:
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                existing_header = next(csv.reader(f), [])
            if existing_header and existing_header != fieldnames:
                backup = path.with_name(f"{path.stem}.schema_mismatch_{int(time.time())}{path.suffix}.bak")
                shutil.move(str(path), str(backup))
                log(f"[WARN] Moved old CSV with mismatched header to {backup}")
                exists = False
        except Exception:
            exists = False
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(row)


# =============================================================================
# Full event / transaction logging v5.26
# =============================================================================

FULL_EVENT_FIELDS = [
    "time_utc", "time_ny", "run_id", "script_name", "script_version", "account_id", "account_lane",
    "event_type", "event_status", "severity", "instrument", "direction", "trade_id", "order_id",
    "transaction_id", "units", "price", "pl", "nav", "balance", "margin_used_pct", "decision_id",
    "movement_key", "reason", "result", "error_type", "error_message", "raw_json_path", "raw_json",
]

ORDER_CANCEL_FIELDS = [
    "time_utc", "time_ny", "run_id", "script_name", "script_version", "account_id", "account_lane",
    "cancel_source", "transaction_id", "cancelled_order_id", "trade_id", "instrument", "order_type",
    "reason", "client_order_id", "replaced_by_order_id", "linked_movement_key", "raw_json",
]

ORDER_RESULT_FIELDS = [
    "time_utc", "time_ny", "run_id", "script_name", "script_version", "account_id", "account_lane",
    "source", "status", "instrument", "direction", "trade_id", "order_id", "transaction_id", "units", "price",
    "reason", "raw_json",
]

TRADE_LIFECYCLE_FIELDS = [
    "time_utc", "time_ny", "run_id", "script_name", "script_version", "account_id", "account_lane",
    "event", "instrument", "direction", "trade_id", "order_id", "transaction_id", "units", "price", "pl",
    "reason", "raw_json",
]

DECISION_AUDIT_FIELDS = [
    "time_utc", "time_ny", "run_id", "script_name", "script_version", "account_id", "account_lane",
    "decision_id", "decision_type", "action_count", "candidate_count", "portfolio_bias", "portfolio_mode",
    "market_summary", "raw_json_path", "raw_json",
]


def _safe_json_text(obj: Any, limit: int = 12000) -> str:
    try:
        return json.dumps(obj, default=str, sort_keys=True)[:limit]
    except Exception:
        return str(obj)[:limit]


def _raw_event_path(base_dir: Path, run_id: str, event_type: str, payload: Any) -> str:
    try:
        raw_dir = base_dir / "raw_event_json"
        raw_dir.mkdir(parents=True, exist_ok=True)
        key = stable_hash({"event_type": event_type, "payload": _safe_json_text(payload, 5000), "time": iso_utc()})
        path = raw_dir / f"{run_id}_{event_type}_{key}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        return str(path)
    except Exception:
        return ""


class FullEventLogger:
    """Append-only full logging layer shared by both account scripts.

    JSONL keeps the full nested broker/decision payload. CSV mirrors keep the
    rows queryable in Excel/pandas. This logger is intentionally best-effort:
    logging failures should never stop a trade manager loop.
    """

    def __init__(self, cfg: Any):
        self.cfg = cfg
        self.run_id = os.environ.get("FOREX_RUN_ID") or ny_now().strftime("%Y%m%d_%H%M%S")
        self.script_name = Path(__file__).name
        self.script_version = "v5.26_16_calls_full_logging"
        self.account_dir = Path(getattr(cfg, "data_dir", SCRIPT_DIR))
        # cfg.data_dir is normally data/forex_gpt_manager/account_x. Use data/forex/logging as global root.
        default_shared = self.account_dir.parent.parent / "forex" / "logging"
        self.shared_dir = Path(os.environ.get("FOREX_FULL_LOGGING_DIR", str(default_shared)))
        self.account_dir.mkdir(parents=True, exist_ok=True)
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir = self.shared_dir / "raw_event_json"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def _base(self) -> Dict[str, Any]:
        return {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "run_id": self.run_id,
            "script_name": self.script_name,
            "script_version": self.script_version,
            "account_id": getattr(self.cfg, "oanda_account_id", ""),
            "account_lane": getattr(self.cfg, "account_lane", ""),
        }

    def _append_both(self, filename: str, row: Dict[str, Any], fields: List[str]) -> None:
        for root in (self.shared_dir, self.account_dir):
            try:
                append_csv(Path(root) / filename, row, fields)
            except Exception:
                pass

    def log_event(self, event_type: str, status: str = "logged", severity: str = "INFO", **kwargs: Any) -> None:
        row = self._base()
        raw = kwargs.pop("raw", None)
        raw_path = ""
        if raw is not None and len(_safe_json_text(raw, 2000)) >= 1900:
            raw_path = _raw_event_path(self.raw_dir, self.run_id, event_type, raw)
        row.update({
            "event_type": event_type,
            "event_status": status,
            "severity": severity,
            "instrument": normalize_instrument(kwargs.get("instrument", "")),
            "direction": kwargs.get("direction", ""),
            "trade_id": kwargs.get("trade_id", ""),
            "order_id": kwargs.get("order_id", ""),
            "transaction_id": kwargs.get("transaction_id", ""),
            "units": kwargs.get("units", ""),
            "price": kwargs.get("price", ""),
            "pl": kwargs.get("pl", ""),
            "nav": kwargs.get("nav", ""),
            "balance": kwargs.get("balance", ""),
            "margin_used_pct": kwargs.get("margin_used_pct", ""),
            "decision_id": kwargs.get("decision_id", ""),
            "movement_key": kwargs.get("movement_key", ""),
            "reason": str(kwargs.get("reason", ""))[:1000],
            "result": kwargs.get("result", ""),
            "error_type": kwargs.get("error_type", ""),
            "error_message": str(kwargs.get("error_message", ""))[:1000],
            "raw_json_path": raw_path,
            "raw_json": _safe_json_text(raw if raw is not None else kwargs, 12000),
        })
        try:
            line = dict(row)
            line["raw"] = raw if raw is not None else kwargs
            for root in (self.shared_dir, self.account_dir):
                p = Path(root) / "full_event_journal.jsonl"
                p.parent.mkdir(parents=True, exist_ok=True)
                with p.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(line, default=str, sort_keys=True) + "\n")
        except Exception:
            pass
        self._append_both("full_event_journal.csv", row, FULL_EVENT_FIELDS)

    def log_order_result(self, source: str, status: str, action: Dict[str, Any], result: Dict[str, Any]) -> None:
        inst = normalize_instrument(action.get("instrument", result.get("instrument", "")))
        fill = result.get("orderFillTransaction") or {}
        create = result.get("orderCreateTransaction") or result.get("marketOrderTransaction") or {}
        cancel = result.get("orderCancelTransaction") or {}
        tx = fill or cancel or create or {}
        row = self._base()
        row.update({
            "source": source,
            "status": status,
            "instrument": inst,
            "direction": action.get("direction", ""),
            "trade_id": fill.get("tradeOpened", {}).get("tradeID", action.get("trade_id", "")) if isinstance(fill.get("tradeOpened", {}), dict) else action.get("trade_id", ""),
            "order_id": tx.get("orderID", create.get("id", "")),
            "transaction_id": tx.get("id", ""),
            "units": action.get("units", fill.get("units", "")),
            "price": fill.get("price", ""),
            "reason": action.get("reason", action.get("why_now", "")),
            "raw_json": _safe_json_text({"action": action, "result": result}, 12000),
        })
        self._append_both("order_result_ledger.csv", row, ORDER_RESULT_FIELDS)
        self.log_event("order_result", status=status, instrument=inst, direction=action.get("direction", ""), trade_id=row.get("trade_id", ""), order_id=row.get("order_id", ""), transaction_id=row.get("transaction_id", ""), units=row.get("units", ""), price=row.get("price", ""), reason=row.get("reason", ""), raw={"action": action, "result": result})

    def log_cancel(self, cancel_source: str, cancel: Dict[str, Any], *, action: Optional[Dict[str, Any]] = None, reason: str = "", movement_key: str = "") -> None:
        action = action or {}
        inst = normalize_instrument(cancel.get("instrument", action.get("instrument", "")))
        row = self._base()
        row.update({
            "cancel_source": cancel_source,
            "transaction_id": cancel.get("id", cancel.get("transaction_id", "")),
            "cancelled_order_id": cancel.get("orderID", cancel.get("order_id", cancel.get("id", ""))),
            "trade_id": cancel.get("tradeID", cancel.get("trade_id", action.get("trade_id", ""))),
            "instrument": inst,
            "order_type": cancel.get("type", cancel.get("order_type", "")),
            "reason": str(cancel.get("reason", reason or action.get("reason", "")))[:1000],
            "client_order_id": str((cancel.get("clientExtensions") or {}).get("id", "")) if isinstance(cancel.get("clientExtensions"), dict) else "",
            "replaced_by_order_id": cancel.get("replacedByOrderID", cancel.get("replaced_by_order_id", "")),
            "linked_movement_key": movement_key or str(action.get("_movement_key", "")),
            "raw_json": _safe_json_text({"cancel": cancel, "action": action}, 12000),
        })
        self._append_both("order_cancel_ledger.csv", row, ORDER_CANCEL_FIELDS)
        self.log_event("order_cancel", status="logged", instrument=inst, trade_id=row.get("trade_id", ""), order_id=row.get("cancelled_order_id", ""), transaction_id=row.get("transaction_id", ""), movement_key=row.get("linked_movement_key", ""), reason=row.get("reason", ""), raw={"cancel": cancel, "action": action})

    def log_trade_lifecycle(self, event: str, trade_or_tx: Dict[str, Any], *, reason: str = "") -> None:
        inst = normalize_instrument(trade_or_tx.get("instrument", ""))
        row = self._base()
        row.update({
            "event": event,
            "instrument": inst,
            "direction": trade_direction_from_units(trade_or_tx.get("currentUnits", trade_or_tx.get("units", ""))),
            "trade_id": trade_or_tx.get("id", trade_or_tx.get("tradeID", "")),
            "order_id": trade_or_tx.get("orderID", ""),
            "transaction_id": trade_or_tx.get("id", ""),
            "units": trade_or_tx.get("units", trade_or_tx.get("currentUnits", "")),
            "price": trade_or_tx.get("price", ""),
            "pl": trade_or_tx.get("pl", trade_or_tx.get("realizedPL", trade_or_tx.get("unrealizedPL", ""))),
            "reason": reason,
            "raw_json": _safe_json_text(trade_or_tx, 12000),
        })
        self._append_both("trade_lifecycle_ledger.csv", row, TRADE_LIFECYCLE_FIELDS)
        self.log_event("trade_lifecycle", status=event, instrument=inst, direction=row.get("direction", ""), trade_id=row.get("trade_id", ""), order_id=row.get("order_id", ""), transaction_id=row.get("transaction_id", ""), units=row.get("units", ""), price=row.get("price", ""), pl=row.get("pl", ""), reason=reason, raw=trade_or_tx)

    def log_decision_audit(self, decision_id: str, decision_type: str, decision: Dict[str, Any], raw_path: str = "") -> None:
        action_count = len(decision.get("orders_to_execute", []) or []) + len(decision.get("open_position_actions", []) or [])
        candidate_count = len(decision.get("new_trade_candidates", []) or [])
        row = self._base()
        row.update({
            "decision_id": decision_id,
            "decision_type": decision_type,
            "action_count": action_count,
            "candidate_count": candidate_count,
            "portfolio_bias": str(decision.get("portfolio_bias", ""))[:500],
            "portfolio_mode": str(decision.get("portfolio_mode", ""))[:500],
            "market_summary": str(decision.get("market_summary", ""))[:1000],
            "raw_json_path": raw_path,
            "raw_json": _safe_json_text(decision, 12000),
        })
        self._append_both("decision_audit_ledger.csv", row, DECISION_AUDIT_FIELDS)
        self.log_event("decision_audit", status="logged", decision_id=decision_id, reason=str(decision.get("market_summary", ""))[:1000], raw=decision)


def _pending_orders_from_open_trades(open_trades: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    order_fields = ["stopLossOrder", "takeProfitOrder", "trailingStopLossOrder", "guaranteedStopLossOrder"]
    for trade in open_trades or []:
        inst = normalize_instrument(trade.get("instrument", ""))
        tid = str(trade.get("id", ""))
        for field in order_fields:
            order = trade.get(field) or {}
            if not isinstance(order, dict) or not order.get("id"):
                continue
            oid = str(order.get("id"))
            o = dict(order)
            o["trade_id"] = tid
            o["tradeID"] = tid
            o["instrument"] = inst
            o["order_type"] = field
            out[oid] = o
    return out


def error_text(context: str, exc: BaseException) -> str:
    return f"\n[{iso_utc()}] {context}\n{type(exc).__name__}: {exc}\n{traceback.format_exc()}\n"


# =============================================================================
# Config
# =============================================================================

@dataclass
class BotConfig:
    openai_api_key: str
    openai_model: str
    enable_openai_web_search: bool
    openai_timeout_seconds: int

    oanda_api_key: str
    oanda_account_id: str
    account_lane: str
    account_display_name: str
    instrument_filter_mode: str
    major_usd_pairs: List[str]
    gpt_reliable_pairs: List[str]
    technical_scout_pairs: List[str]
    oanda_env: str
    allow_live: bool
    request_timeout_seconds: int
    http_max_retries: int
    http_retry_sleep_seconds: float
    disable_http_keepalive: bool

    execute_trades: bool
    auto_execute_gpt_actions: bool
    scan_on_launch: bool
    mark_past_calls_on_launch: bool  # deprecated/no-op in v5.3; launch scans never consume scheduled slots
    calls_per_trading_day: int
    call_times_ny: List[str]
    friday_call_times_ny: List[str]
    schedule_window_minutes: int
    min_minutes_between_gpt_scans: int
    loop_sleep_seconds: int
    local_monitor_interval_minutes: int

    event_scanner_enabled: bool
    event_scan_interval_seconds: int
    event_candle_granularity: str
    event_candle_count: int
    event_windows_minutes: List[int]
    event_min_basket_pairs: int
    event_min_major_net_pips: float
    event_min_cross_net_pips: float
    event_min_exotic_net_pips: float
    event_min_move_to_spread_ratio: float
    event_max_spread_pips_major: float
    event_max_spread_pips_cross: float
    event_max_spread_pips_exotic: float
    event_trigger_gpt_enabled: bool
    event_min_minutes_between_gpt_scans: int
    event_scout_trades_enabled: bool
    event_require_gpt_permission: bool
    event_scout_risk_pct: float
    event_max_total_scout_risk_pct: float
    event_max_scout_trades_per_event: int
    event_scout_stop_pips_major: float
    event_scout_stop_pips_cross: float
    event_scout_stop_pips_exotic: float
    event_scout_take_profit_r_multiple: float
    event_scout_trailing_stop_pips_major: float
    event_scout_trailing_stop_pips_cross: float
    event_scout_trailing_stop_pips_exotic: float
    event_permission_default_expiry_hours: float

    scout_ev_scoring_enabled: bool
    scout_min_ev_score_to_trade: float
    scout_max_event_age_minutes_for_continuation: float
    scout_max_repeat_triggers_per_key: int
    scout_event_memory_reset_minutes: float
    scout_skip_late_exhaustion: bool
    scout_late_exhaustion_age_minutes: float
    scout_late_exhaustion_profile_ev30_max: float
    scout_allow_unknown_profile_base_risk: bool
    scout_require_positive_profile_for_risk_above_base: bool
    scout_profile_csv_path: str
    scout_auto_load_latest_profile: bool
    scout_profile_min_rows: int
    scout_profile_refresh_seconds: int
    scout_risk_pct_score_70: float
    scout_risk_pct_score_80: float
    scout_risk_pct_score_90: float
    scout_risk_pct_score_95: float
    scout_max_risk_pct_with_unknown_profile: float
    scout_max_risk_pct_with_negative_profile: float
    scout_profile_fallback: Dict[str, Any]
    scout_volatile_lane_enabled: bool
    scout_volatile_pairs: List[str]
    scout_volatile_min_net_pips: float
    scout_volatile_max_spread_pips: float
    scout_volatile_min_move_to_spread_ratio: float
    scout_volatile_min_basket_pairs: int
    scout_volatile_max_event_age_minutes: float
    scout_volatile_repeat_trigger_cap: int
    scout_volatile_base_risk_pct: float
    scout_volatile_max_risk_pct: float
    scout_volatile_allow_unknown_profile: bool
    scout_volatile_allow_negative_profile_if_early: bool
    scout_volatile_min_ev_score_to_trade: float
    scout_volatile_stop_pips: float
    scout_volatile_stop_net_multiplier: float
    scout_volatile_stop_spread_multiplier: float
    scout_volatile_trailing_stop_pips: float
    scout_volatile_max_spread_to_stop_ratio: float

    data_dir: Path
    state_path: Path
    decisions_csv: Path
    candidates_csv: Path
    actions_csv: Path
    monitor_csv: Path
    event_signals_csv: Path
    event_windows_csv: Path
    market_movements_csv: Path
    movement_capture_csv: Path
    errors_log: Path
    raw_dir: Path
    daily_recap_dir: Path
    daily_recap_index: Path


    price_chunk_size: int
    candle_granularity: str
    candle_count: int
    include_candles: bool
    max_candle_instruments: int
    account_currency: str

    max_open_trades: int
    max_new_trades_per_scan: int
    max_total_new_risk_pct_per_scan: float
    max_risk_pct_per_trade: float
    min_risk_pct_per_trade: float
    max_margin_used_pct: float
    target_margin_used_pct: float
    emergency_margin_used_pct: float
    local_emergency_close: bool
    max_one_currency_net_units_pct: float

    require_stop_loss_on_open: bool
    require_take_profit_on_open: bool
    allow_trailing_stop_on_open: bool
    max_spread_pips_default: float
    max_spread_pips_exotic: float
    max_spread_to_stop_ratio: float
    max_entry_slippage_pips: float

    partial_close_min_pct: float
    partial_close_max_pct: float
    dry_run_state_orders: bool

    broker_recheck_before_actions: bool
    action_dedupe_minutes: int
    startup_reconcile_keep: int
    retry_unsafe_broker_writes: bool
    require_position_action_per_open_trade: bool

    daily_recap_enabled: bool
    daily_recap_recent_days_for_prompt: int
    daily_recap_lookback_days: int
    min_non_usd_candidates: int
    require_cross_pair_coverage: bool
    all68_bucketed_discovery_enabled: bool
    all68_bucket_top_per_bucket: int
    all68_bucket_min_reviewed: int
    all68_bucket_require_real_review: bool

    shutdown_mode: str
    shutdown_skip_gpt: bool
    shutdown_block_new_trades: bool
    shutdown_min_minutes_between_passes: int
    shutdown_partial_close_pct: float
    shutdown_min_profit_pips_to_partial: float
    shutdown_min_profit_usd_to_partial: float
    shutdown_lock_profit_ratio: float
    shutdown_min_lock_profit_pips: float
    shutdown_max_loss_pips: float
    shutdown_close_if_loss_pips_beyond: float
    shutdown_full_close_profit_pips: float
    shutdown_stop_gap_pips_default: float
    shutdown_stop_gap_pips_exotic: float
    shutdown_min_stop_gap_pips: float

    @staticmethod
    def load() -> "BotConfig":
        data_dir = Path(cfg_str("FOREX_DATA_DIR", "FOREX_BOT_DATA_DIR", default=str(SCRIPT_DIR / "data" / "forex_gpt_manager")))
        calls = cfg_int("FOREX_GPT_CALLS_PER_DAY", default=16)
        minute_offset = cfg_int("FOREX_GPT_CALL_MINUTE_OFFSET", default=15)
        raw_times = cfg_list("FOREX_GPT_CALL_TIMES_NY", default=[])
        custom_call_times = [t for t in raw_times if valid_hhmm(t)]
        call_times = custom_call_times or default_call_times_for_count(calls, minute_offset)

        raw_friday_times = cfg_list("FOREX_FRIDAY_GPT_CALL_TIMES_NY", "FRIDAY_GPT_CALL_TIMES_NY", default=[])
        custom_friday_times = [t for t in raw_friday_times if valid_hhmm(t)]
        if custom_friday_times:
            friday_call_times = custom_friday_times
        elif custom_call_times:
            friday_call_times = list(call_times)
        else:
            friday_call_times = default_friday_call_times_for_count(calls, call_times)

        oanda_env = cfg_str("OANDA_ENV", "oanda_env", default="practice").lower()
        if oanda_env not in {"practice", "live"}:
            oanda_env = "practice"
        execute_default = True if oanda_env == "practice" else False

        return BotConfig(
            openai_api_key=cfg_str("OPENAI_API_KEY", "openai_api_key", "openaiapi", "OPENAI_KEY", default=""),
            openai_model=cfg_str("OPENAI_MODEL", "openai_model", default="gpt-4.1-mini"),
            enable_openai_web_search=cfg_bool("ENABLE_OPENAI_WEB_SEARCH", "FOREX_ENABLE_OPENAI_WEB_SEARCH", default=True),
            openai_timeout_seconds=cfg_int("OPENAI_TIMEOUT_SECONDS", default=180),

            oanda_api_key=cfg_str("OANDA_API_KEY", "OANDA_API_TOKEN", "oanda_api_key", "oandaapi", default=""),
            oanda_account_id=cfg_str("OANDA_ACCOUNT_ID", "oanda_account_id", "OANDA_ACCOUNT", default=""),
            account_lane=cfg_str("FOREX_ACCOUNT_LANE", default="single"),
            account_display_name=cfg_str("FOREX_ACCOUNT_DISPLAY_NAME", default="OANDA_ACCOUNT_ID"),
            instrument_filter_mode=cfg_str("FOREX_INSTRUMENT_FILTER_MODE", default="all"),
            major_usd_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_MAJOR_USD_PAIRS", default=["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"]) if normalize_instrument(x)],
            gpt_reliable_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_GPT_RELIABLE_PAIRS", default=cfg_list("FOREX_MAJOR_USD_PAIRS", default=["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"])) if normalize_instrument(x)],
            technical_scout_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_TECHNICAL_SCOUT_PAIRS", default=cfg_list("FOREX_SCOUT_VOLATILE_PAIRS", default=[])) if normalize_instrument(x)],
            oanda_env=oanda_env,
            allow_live=cfg_bool("FOREX_ALLOW_LIVE", "ALLOW_LIVE", default=False),
            request_timeout_seconds=cfg_int("REQUEST_TIMEOUT_SECONDS", default=30),
            http_max_retries=cfg_int("FOREX_HTTP_MAX_RETRIES", "HTTP_MAX_RETRIES", default=3),
            http_retry_sleep_seconds=cfg_float("FOREX_HTTP_RETRY_SLEEP_SECONDS", "HTTP_RETRY_SLEEP_SECONDS", default=2.0),
            disable_http_keepalive=cfg_bool("FOREX_DISABLE_HTTP_KEEPALIVE", "DISABLE_HTTP_KEEPALIVE", default=True),

            execute_trades=cfg_bool("FOREX_EXECUTE_TRADES", "EXECUTE_TRADES", default=execute_default),
            auto_execute_gpt_actions=cfg_bool("FOREX_AUTO_EXECUTE_GPT_ACTIONS", "AUTO_EXECUTE_GPT_ACTIONS", default=True),
            scan_on_launch=cfg_bool("FOREX_SCAN_ON_LAUNCH", "SCAN_ON_LAUNCH", default=True),
            mark_past_calls_on_launch=False,
            calls_per_trading_day=calls,
            call_times_ny=call_times,
            friday_call_times_ny=friday_call_times,
            schedule_window_minutes=cfg_int("FOREX_SCHEDULE_WINDOW_MINUTES", "SCHEDULE_WINDOW_MINUTES", default=10),
            min_minutes_between_gpt_scans=cfg_int("FOREX_MIN_MINUTES_BETWEEN_GPT_SCANS", "MIN_MINUTES_BETWEEN_GPT_SCANS", default=75),
            loop_sleep_seconds=cfg_int("FOREX_LOOP_SLEEP_SECONDS", default=60),
            local_monitor_interval_minutes=cfg_int("FOREX_LOCAL_MONITOR_INTERVAL_MINUTES", "LOCAL_MONITOR_INTERVAL_MINUTES", default=60),

            event_scanner_enabled=cfg_bool("FOREX_EVENT_SCANNER_ENABLED", "FOREX_LOCAL_EVENT_SCANNER_ENABLED", default=True),
            event_scan_interval_seconds=cfg_int("FOREX_EVENT_SCAN_INTERVAL_SECONDS", "FOREX_LOCAL_EVENT_SCAN_INTERVAL_SECONDS", default=60),
            event_candle_granularity=cfg_str("FOREX_EVENT_CANDLE_GRANULARITY", default="M5"),
            event_candle_count=cfg_int("FOREX_EVENT_CANDLE_COUNT", default=12),
            event_windows_minutes=cfg_int_list("FOREX_EVENT_WINDOWS_MINUTES", default=[5, 10, 15, 30]),
            event_min_basket_pairs=cfg_int("FOREX_EVENT_MIN_BASKET_PAIRS", default=2),
            event_min_major_net_pips=cfg_float("FOREX_EVENT_MIN_MAJOR_NET_PIPS", default=18.0),
            event_min_cross_net_pips=cfg_float("FOREX_EVENT_MIN_CROSS_NET_PIPS", default=25.0),
            event_min_exotic_net_pips=cfg_float("FOREX_EVENT_MIN_EXOTIC_NET_PIPS", default=120.0),
            event_min_move_to_spread_ratio=cfg_float("FOREX_EVENT_MIN_MOVE_TO_SPREAD_RATIO", default=3.5),
            event_max_spread_pips_major=cfg_float("FOREX_EVENT_MAX_SPREAD_PIPS_MAJOR", default=5.0),
            event_max_spread_pips_cross=cfg_float("FOREX_EVENT_MAX_SPREAD_PIPS_CROSS", default=8.0),
            event_max_spread_pips_exotic=cfg_float("FOREX_EVENT_MAX_SPREAD_PIPS_EXOTIC", default=180.0),
            event_trigger_gpt_enabled=cfg_bool("FOREX_EVENT_TRIGGERED_GPT_ENABLED", default=True),
            event_min_minutes_between_gpt_scans=cfg_int("FOREX_EVENT_MIN_MINUTES_BETWEEN_GPT_SCANS", default=30),
            event_scout_trades_enabled=cfg_bool("FOREX_EVENT_SCOUT_TRADES_ENABLED", default=True),
            event_require_gpt_permission=cfg_bool("FOREX_EVENT_REQUIRE_GPT_PERMISSION", default=False),
            event_scout_risk_pct=cfg_float("FOREX_EVENT_SCOUT_RISK_PCT", default=0.50),
            event_max_total_scout_risk_pct=cfg_float("FOREX_EVENT_MAX_TOTAL_SCOUT_RISK_PCT", default=2.0),
            event_max_scout_trades_per_event=cfg_int("FOREX_EVENT_MAX_SCOUT_TRADES_PER_EVENT", default=2),
            event_scout_stop_pips_major=cfg_float("FOREX_EVENT_SCOUT_STOP_PIPS_MAJOR", default=14.0),
            event_scout_stop_pips_cross=cfg_float("FOREX_EVENT_SCOUT_STOP_PIPS_CROSS", default=22.0),
            event_scout_stop_pips_exotic=cfg_float("FOREX_EVENT_SCOUT_STOP_PIPS_EXOTIC", default=250.0),
            event_scout_take_profit_r_multiple=cfg_float("FOREX_EVENT_SCOUT_TAKE_PROFIT_R_MULTIPLE", default=2.0),
            event_scout_trailing_stop_pips_major=cfg_float("FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_MAJOR", default=12.0),
            event_scout_trailing_stop_pips_cross=cfg_float("FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_CROSS", default=20.0),
            event_scout_trailing_stop_pips_exotic=cfg_float("FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_EXOTIC", default=200.0),
            event_permission_default_expiry_hours=cfg_float("FOREX_EVENT_PERMISSION_DEFAULT_EXPIRY_HOURS", default=8.0),

            scout_ev_scoring_enabled=cfg_bool("FOREX_SCOUT_EV_SCORING_ENABLED", default=True),
            scout_min_ev_score_to_trade=cfg_float("FOREX_SCOUT_MIN_EV_SCORE_TO_TRADE", default=72.0),
            scout_max_event_age_minutes_for_continuation=cfg_float("FOREX_SCOUT_MAX_EVENT_AGE_MINUTES_FOR_CONTINUATION", default=20.0),
            scout_max_repeat_triggers_per_key=cfg_int("FOREX_SCOUT_MAX_REPEAT_TRIGGERS_PER_KEY", default=2),
            scout_event_memory_reset_minutes=cfg_float("FOREX_SCOUT_EVENT_MEMORY_RESET_MINUTES", default=180.0),
            scout_skip_late_exhaustion=cfg_bool("FOREX_SCOUT_SKIP_LATE_EXHAUSTION", default=True),
            scout_late_exhaustion_age_minutes=cfg_float("FOREX_SCOUT_LATE_EXHAUSTION_AGE_MINUTES", default=25.0),
            scout_late_exhaustion_profile_ev30_max=cfg_float("FOREX_SCOUT_LATE_EXHAUSTION_PROFILE_EV30_MAX", default=-5.0),
            scout_allow_unknown_profile_base_risk=cfg_bool("FOREX_SCOUT_ALLOW_UNKNOWN_PROFILE_BASE_RISK", default=True),
            scout_require_positive_profile_for_risk_above_base=cfg_bool("FOREX_SCOUT_REQUIRE_POSITIVE_PROFILE_FOR_RISK_ABOVE_BASE", default=True),
            scout_profile_csv_path=cfg_str("FOREX_SCOUT_PROFILE_CSV_PATH", default=""),
            scout_auto_load_latest_profile=cfg_bool("FOREX_SCOUT_AUTO_LOAD_LATEST_PROFILE", default=True),
            scout_profile_min_rows=cfg_int("FOREX_SCOUT_PROFILE_MIN_ROWS", default=3),
            scout_profile_refresh_seconds=cfg_int("FOREX_SCOUT_PROFILE_REFRESH_SECONDS", default=300),
            scout_risk_pct_score_70=cfg_float("FOREX_SCOUT_RISK_PCT_SCORE_70", default=0.25),
            scout_risk_pct_score_80=cfg_float("FOREX_SCOUT_RISK_PCT_SCORE_80", default=0.50),
            scout_risk_pct_score_90=cfg_float("FOREX_SCOUT_RISK_PCT_SCORE_90", default=0.75),
            scout_risk_pct_score_95=cfg_float("FOREX_SCOUT_RISK_PCT_SCORE_95", default=1.00),
            scout_max_risk_pct_with_unknown_profile=cfg_float("FOREX_SCOUT_MAX_RISK_PCT_WITH_UNKNOWN_PROFILE", default=0.35),
            scout_max_risk_pct_with_negative_profile=cfg_float("FOREX_SCOUT_MAX_RISK_PCT_WITH_NEGATIVE_PROFILE", default=0.25),
            scout_profile_fallback=dict(cfg_dict("FOREX_SCOUT_PROFILE_FALLBACK", default={})),
            scout_volatile_lane_enabled=cfg_bool("FOREX_SCOUT_VOLATILE_LANE_ENABLED", default=True),
            scout_volatile_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_SCOUT_VOLATILE_PAIRS", default=[]) if normalize_instrument(x)],
            scout_volatile_min_net_pips=cfg_float("FOREX_SCOUT_VOLATILE_MIN_NET_PIPS", default=90.0),
            scout_volatile_max_spread_pips=cfg_float("FOREX_SCOUT_VOLATILE_MAX_SPREAD_PIPS", default=260.0),
            scout_volatile_min_move_to_spread_ratio=cfg_float("FOREX_SCOUT_VOLATILE_MIN_MOVE_TO_SPREAD_RATIO", default=2.2),
            scout_volatile_min_basket_pairs=cfg_int("FOREX_SCOUT_VOLATILE_MIN_BASKET_PAIRS", default=2),
            scout_volatile_max_event_age_minutes=cfg_float("FOREX_SCOUT_VOLATILE_MAX_EVENT_AGE_MINUTES", default=15.0),
            scout_volatile_repeat_trigger_cap=cfg_int("FOREX_SCOUT_VOLATILE_REPEAT_TRIGGER_CAP", default=2),
            scout_volatile_base_risk_pct=cfg_float("FOREX_SCOUT_VOLATILE_BASE_RISK_PCT", default=0.20),
            scout_volatile_max_risk_pct=cfg_float("FOREX_SCOUT_VOLATILE_MAX_RISK_PCT", default=0.50),
            scout_volatile_allow_unknown_profile=cfg_bool("FOREX_SCOUT_VOLATILE_ALLOW_UNKNOWN_PROFILE", default=True),
            scout_volatile_allow_negative_profile_if_early=cfg_bool("FOREX_SCOUT_VOLATILE_ALLOW_NEGATIVE_PROFILE_IF_EARLY", default=True),
            scout_volatile_min_ev_score_to_trade=cfg_float("FOREX_SCOUT_VOLATILE_MIN_EV_SCORE_TO_TRADE", default=62.0),
            scout_volatile_stop_pips=cfg_float("FOREX_SCOUT_VOLATILE_STOP_PIPS", default=320.0),
            scout_volatile_stop_net_multiplier=cfg_float("FOREX_SCOUT_VOLATILE_STOP_NET_MULTIPLIER", default=0.65),
            scout_volatile_stop_spread_multiplier=cfg_float("FOREX_SCOUT_VOLATILE_STOP_SPREAD_MULTIPLIER", default=3.5),
            scout_volatile_trailing_stop_pips=cfg_float("FOREX_SCOUT_VOLATILE_TRAILING_STOP_PIPS", default=250.0),
            scout_volatile_max_spread_to_stop_ratio=cfg_float("FOREX_SCOUT_VOLATILE_MAX_SPREAD_TO_STOP_RATIO", default=0.90),

            data_dir=data_dir,
            state_path=data_dir / "state.json",
            decisions_csv=data_dir / "decisions.csv",
            candidates_csv=data_dir / "candidates.csv",
            actions_csv=data_dir / "actions.csv",
            monitor_csv=data_dir / "monitor.csv",
            event_signals_csv=data_dir / "event_signals.csv",
            event_windows_csv=data_dir / "event_windows.csv",
            market_movements_csv=Path(cfg_str("FOREX_MARKET_MOVEMENT_LEDGER_DIR", default=str(SCRIPT_DIR / "data" / "market_movement_ledger"))) / "market_movements.csv",
            movement_capture_csv=Path(cfg_str("FOREX_MARKET_MOVEMENT_LEDGER_DIR", default=str(SCRIPT_DIR / "data" / "market_movement_ledger"))) / "movement_capture_ledger.csv",
            errors_log=data_dir / "errors.log",
            raw_dir=data_dir / "raw_decisions",
            daily_recap_dir=data_dir / "daily_recaps",
            daily_recap_index=data_dir / "daily_recaps.jsonl",

            price_chunk_size=cfg_int("FOREX_PRICE_CHUNK_SIZE", "PRICE_CHUNK_SIZE", default=40),
            candle_granularity=cfg_str("FOREX_CANDLE_GRANULARITY", "CANDLE_GRANULARITY", default="H1"),
            candle_count=cfg_int("FOREX_CANDLE_COUNT", "CANDLE_COUNT", default=80),
            include_candles=cfg_bool("FOREX_INCLUDE_CANDLES", default=True),
            max_candle_instruments=cfg_int("FOREX_MAX_CANDLE_INSTRUMENTS", default=0),
            account_currency=cfg_str("ACCOUNT_CURRENCY", "FOREX_ACCOUNT_CURRENCY", default="USD").upper(),

            max_open_trades=cfg_int("FOREX_MAX_OPEN_TRADES", "MAX_POSITIONS", default=10),
            max_new_trades_per_scan=cfg_int("FOREX_MAX_NEW_TRADES_PER_SCAN", "MAX_NEW_POSITIONS_PER_SCAN", default=5),
            max_total_new_risk_pct_per_scan=cfg_float("FOREX_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN", default=12.0),
            max_risk_pct_per_trade=cfg_float("FOREX_MAX_GPT_RISK_PCT_PER_TRADE", default=4.5),
            min_risk_pct_per_trade=cfg_float("FOREX_MIN_GPT_RISK_PCT", default=0.10),
            max_margin_used_pct=cfg_float("FOREX_MAX_MARGIN_USED_PCT", "MAX_MARGIN_USED_PCT", default=75.0),
            target_margin_used_pct=cfg_float("FOREX_TARGET_MARGIN_USED_PCT", "TARGET_MARGIN_USED_PCT", default=55.0),
            emergency_margin_used_pct=cfg_float("FOREX_EMERGENCY_MARGIN_USED_PCT", default=88.0),
            local_emergency_close=cfg_bool("FOREX_LOCAL_EMERGENCY_CLOSE", default=True),
            max_one_currency_net_units_pct=cfg_float("FOREX_MAX_ONE_CURRENCY_NET_UNITS_PCT", "MAX_ONE_CURRENCY_BIAS_PCT", default=85.0),

            require_stop_loss_on_open=cfg_bool("FOREX_REQUIRE_STOP_LOSS_ON_OPEN", default=True),
            require_take_profit_on_open=cfg_bool("FOREX_REQUIRE_TAKE_PROFIT_ON_OPEN", default=False),
            allow_trailing_stop_on_open=cfg_bool("FOREX_ALLOW_TRAILING_STOP_ON_OPEN", default=True),
            max_spread_pips_default=cfg_float("FOREX_MAX_SPREAD_PIPS_DEFAULT", "DEFAULT_MAX_SPREAD_PIPS", default=4.0),
            max_spread_pips_exotic=cfg_float("FOREX_MAX_SPREAD_PIPS_EXOTIC", "EXOTIC_MAX_SPREAD_PIPS", default=12.0),
            max_spread_to_stop_ratio=cfg_float("FOREX_MAX_SPREAD_TO_STOP_RATIO", default=0.20),
            max_entry_slippage_pips=cfg_float("FOREX_MAX_ENTRY_SLIPPAGE_PIPS", "MAX_ENTRY_SLIPPAGE_PIPS", default=3.0),

            partial_close_min_pct=cfg_float("FOREX_PARTIAL_CLOSE_MIN_PCT", default=10.0),
            partial_close_max_pct=cfg_float("FOREX_PARTIAL_CLOSE_MAX_PCT", default=90.0),
            dry_run_state_orders=cfg_bool("FOREX_DRY_RUN_STATE_ORDERS", default=True),

            broker_recheck_before_actions=cfg_bool("FOREX_BROKER_RECHECK_BEFORE_ACTIONS", default=True),
            action_dedupe_minutes=cfg_int("FOREX_ACTION_DEDUPE_MINUTES", default=30),
            startup_reconcile_keep=cfg_int("FOREX_STARTUP_RECONCILE_KEEP", default=50),
            retry_unsafe_broker_writes=cfg_bool("FOREX_RETRY_UNSAFE_BROKER_WRITES", default=False),
            require_position_action_per_open_trade=cfg_bool("FOREX_REQUIRE_POSITION_ACTION_PER_OPEN_TRADE", default=True),

            daily_recap_enabled=cfg_bool("FOREX_DAILY_RECAP_ENABLED", default=True),
            daily_recap_recent_days_for_prompt=cfg_int("FOREX_DAILY_RECAP_RECENT_DAYS_FOR_PROMPT", default=5),
            daily_recap_lookback_days=cfg_int("FOREX_DAILY_RECAP_LOOKBACK_DAYS", default=14),
            min_non_usd_candidates=cfg_int("FOREX_MIN_NON_USD_CANDIDATES", default=0),
            require_cross_pair_coverage=cfg_bool("FOREX_REQUIRE_CROSS_PAIR_COVERAGE", default=True),
            all68_bucketed_discovery_enabled=cfg_bool("FOREX_ALL68_BUCKETED_DISCOVERY_ENABLED", default=True),
            all68_bucket_top_per_bucket=cfg_int("FOREX_ALL68_BUCKET_TOP_PER_BUCKET", default=4),
            all68_bucket_min_reviewed=cfg_int("FOREX_ALL68_BUCKET_MIN_REVIEWED", default=1),
            all68_bucket_require_real_review=cfg_bool("FOREX_ALL68_BUCKET_REQUIRE_REAL_REVIEW", default=True),

            shutdown_mode=normalize_shutdown_mode(cfg_str("FOREX_SHUTDOWN_MODE", "FOREX_DRAIN_MODE", default="off")),
            shutdown_skip_gpt=cfg_bool("FOREX_SHUTDOWN_SKIP_GPT", "FOREX_DRAIN_SKIP_GPT", default=True),
            shutdown_block_new_trades=cfg_bool("FOREX_SHUTDOWN_BLOCK_NEW_TRADES", "FOREX_DRAIN_BLOCK_NEW_TRADES", default=True),
            shutdown_min_minutes_between_passes=cfg_int("FOREX_SHUTDOWN_MIN_MINUTES_BETWEEN_PASSES", "FOREX_DRAIN_MIN_MINUTES_BETWEEN_PASSES", default=15),
            shutdown_partial_close_pct=cfg_float("FOREX_SHUTDOWN_PARTIAL_CLOSE_PCT", "FOREX_DRAIN_PARTIAL_CLOSE_PCT", default=25.0),
            shutdown_min_profit_pips_to_partial=cfg_float("FOREX_SHUTDOWN_MIN_PROFIT_PIPS_TO_PARTIAL", "FOREX_DRAIN_MIN_PROFIT_PIPS_TO_PARTIAL", default=4.0),
            shutdown_min_profit_usd_to_partial=cfg_float("FOREX_SHUTDOWN_MIN_PROFIT_USD_TO_PARTIAL", "FOREX_DRAIN_MIN_PROFIT_USD_TO_PARTIAL", default=0.05),
            shutdown_lock_profit_ratio=cfg_float("FOREX_SHUTDOWN_LOCK_PROFIT_RATIO", "FOREX_DRAIN_LOCK_PROFIT_RATIO", default=0.55),
            shutdown_min_lock_profit_pips=cfg_float("FOREX_SHUTDOWN_MIN_LOCK_PROFIT_PIPS", "FOREX_DRAIN_MIN_LOCK_PROFIT_PIPS", default=1.5),
            shutdown_max_loss_pips=cfg_float("FOREX_SHUTDOWN_MAX_LOSS_PIPS", "FOREX_DRAIN_MAX_LOSS_PIPS", default=8.0),
            shutdown_close_if_loss_pips_beyond=cfg_float("FOREX_SHUTDOWN_CLOSE_IF_LOSS_PIPS_BEYOND", "FOREX_DRAIN_CLOSE_IF_LOSS_PIPS_BEYOND", default=18.0),
            shutdown_full_close_profit_pips=cfg_float("FOREX_SHUTDOWN_FULL_CLOSE_PROFIT_PIPS", "FOREX_DRAIN_FULL_CLOSE_PROFIT_PIPS", default=0.0),
            shutdown_stop_gap_pips_default=cfg_float("FOREX_SHUTDOWN_STOP_GAP_PIPS_DEFAULT", "FOREX_DRAIN_STOP_GAP_PIPS_DEFAULT", default=6.0),
            shutdown_stop_gap_pips_exotic=cfg_float("FOREX_SHUTDOWN_STOP_GAP_PIPS_EXOTIC", "FOREX_DRAIN_STOP_GAP_PIPS_EXOTIC", default=12.0),
            shutdown_min_stop_gap_pips=cfg_float("FOREX_SHUTDOWN_MIN_STOP_GAP_PIPS", "FOREX_DRAIN_MIN_STOP_GAP_PIPS", default=1.0),
        )


DECISION_FIELDS = [
    "time_utc", "time_ny", "kind", "market_summary", "portfolio_bias", "action_count",
    "context_hash", "raw_path", "raw_json",
]
CANDIDATE_FIELDS = [
    "time_utc", "time_ny", "section", "rank", "instrument", "direction", "action",
    "has_usd", "is_cross_pair", "coverage_bucket", "outlook_confidence", "risk_pct",
    "margin_intensity", "expected_hold_hours", "reason", "why_now", "raw_json",
]
ACTION_FIELDS = [
    "time_utc", "time_ny", "action_type", "status", "instrument", "direction", "trade_id",
    "units", "risk_pct", "risk_usd", "margin_usd", "outlook_confidence", "fill_price",
    "stop_loss", "take_profit", "trailing_stop_pips", "partial_close_pct", "reason",
    "reject_reason", "raw_json",
]
MONITOR_FIELDS = [
    "time_utc", "time_ny", "nav", "balance", "margin_used", "margin_available", "margin_used_pct",
    "open_trade_count", "decision", "reason", "raw_json",
]
EVENT_SIGNAL_FIELDS = [
    "time_utc", "time_ny", "theme", "status", "signal_count", "best_instrument", "best_direction",
    "best_window_minutes", "best_net_pips", "best_spread_pips", "triggered_gpt", "scout_attempts",
    "reason", "raw_json",
]
EVENT_WINDOW_FIELDS = [
    "time_utc", "time_ny", "instrument", "theme", "window_minutes", "direction", "net_pips",
    "mid_move_pips", "spread_avg_pips", "move_to_spread_ratio", "start_utc", "end_utc", "raw_json",
]

MARKET_MOVEMENT_FIELDS = [
    "time_utc", "time_ny", "source_account_lane", "source_account_name", "source_script",
    "movement_key", "instrument", "base_currency", "quote_currency", "direction",
    "theme", "status", "window_minutes", "net_pips", "mid_move_pips",
    "spread_avg_pips", "move_to_spread_ratio", "start_utc", "end_utc",
    "signal_rank", "signal_count", "reason", "raw_json",
]
MOVEMENT_CAPTURE_FIELDS = [
    "time_utc", "time_ny", "source_account_lane", "source_account_name", "source_script",
    "movement_key", "instrument", "base_currency", "quote_currency", "direction",
    "theme", "action_type", "status", "trade_id", "units", "risk_pct",
    "risk_usd", "margin_usd", "fill_price", "reason", "reject_reason", "raw_json",
]


# =============================================================================
# OANDA client
# =============================================================================

@dataclass
class InstrumentMeta:
    name: str
    display_name: str
    type: str
    margin_rate: float
    pip_location: int
    display_precision: int
    trade_units_precision: int
    minimum_trade_size: float

    @property
    def pip_size(self) -> float:
        return 10 ** self.pip_location


class OandaClient:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.base_url = "https://api-fxpractice.oanda.com" if cfg.oanda_env == "practice" else "https://api-fxtrade.oanda.com"
        self.headers = {
            "Authorization": f"Bearer {cfg.oanda_api_key}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "RFC3339",
        }
        if cfg.disable_http_keepalive:
            self.headers["Connection"] = "close"
        self.s = requests.Session()
        self.s.headers.update(self.headers)

    def reset_session(self) -> None:
        try:
            self.s.close()
        except Exception:
            pass
        self.s = requests.Session()
        self.s.headers.update(self.headers)

    def require_auth(self) -> None:
        if not self.cfg.oanda_api_key:
            raise RuntimeError("Missing OANDA_API_KEY / OANDA_API_TOKEN in creds or environment")
        if not self.cfg.oanda_account_id:
            raise RuntimeError("Missing OANDA_ACCOUNT_ID in creds or environment")

    def request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self.require_auth()
        method = method.upper().strip()
        url = f"{self.base_url}{path}"
        unsafe_write = method in {"POST", "PUT", "PATCH", "DELETE"}
        # Broker writes are not safely idempotent. A connection abort after submit can mean
        # OANDA received the order/close even if Python did not receive the response.
        # Default: retry reads, but do not blindly retry order-changing writes.
        max_retries = int(self.cfg.http_max_retries)
        if unsafe_write and not self.cfg.retry_unsafe_broker_writes:
            max_retries = 0
        attempts = max(1, max_retries + 1)
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                if self.cfg.disable_http_keepalive:
                    self.reset_session()
                resp = self.s.request(
                    method,
                    url,
                    params=params,
                    data=json.dumps(body) if body is not None else None,
                    timeout=self.cfg.request_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise RuntimeError(f"OANDA {method} {path} network failure after {attempts} attempt(s); outcome uncertain for broker writes: {exc}") from exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                log(f"[WARN] OANDA transient network error on {method} {path}; retry {attempt}/{attempts - 1} in {wait:.1f}s: {exc}")
                self.reset_session()
                time.sleep(wait)
                continue

            if resp.status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
                if unsafe_write and not self.cfg.retry_unsafe_broker_writes:
                    raise RuntimeError(f"OANDA {method} {path} returned transient HTTP {resp.status_code}; not retrying unsafe broker write automatically: {resp.text[:1600]}")
                if attempt < attempts:
                    wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                    log(f"[WARN] OANDA transient HTTP {resp.status_code} on {method} {path}; retry {attempt}/{attempts - 1} in {wait:.1f}s")
                    time.sleep(wait)
                    continue

            if resp.status_code >= 400:
                raise RuntimeError(f"OANDA {method} {path} failed {resp.status_code}: {resp.text[:1600]}")
            if not resp.text.strip():
                if self.cfg.disable_http_keepalive:
                    resp.close()
                return {}
            try:
                data = resp.json()
                if self.cfg.disable_http_keepalive:
                    resp.close()
                return data
            except ValueError as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise RuntimeError(f"OANDA {method} {path} returned non-JSON after {attempts} attempts: {resp.text[:500]}") from exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                log(f"[WARN] OANDA non-JSON response on {method} {path}; retry {attempt}/{attempts - 1} in {wait:.1f}s")
                time.sleep(wait)
        raise RuntimeError(f"OANDA {method} {path} failed unexpectedly: {last_exc}")

    def get_account_summary(self) -> Dict[str, Any]:
        return self.request("GET", f"/v3/accounts/{self.cfg.oanda_account_id}/summary").get("account", {})

    def get_instruments(self) -> List[Dict[str, Any]]:
        return self.request("GET", f"/v3/accounts/{self.cfg.oanda_account_id}/instruments").get("instruments", [])

    def get_prices(self, instruments: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for chunk in chunks(list(instruments), self.cfg.price_chunk_size):
            data = self.request(
                "GET",
                f"/v3/accounts/{self.cfg.oanda_account_id}/pricing",
                params={"instruments": ",".join(chunk)},
            )
            for p in data.get("prices", []):
                out[str(p.get("instrument", ""))] = p
        return out

    def get_open_trades(self) -> List[Dict[str, Any]]:
        response = self.request("GET", f"/v3/accounts/{self.cfg.oanda_account_id}/openTrades")
        if not isinstance(response, dict) or not isinstance(response.get("trades"), list):
            raise ValueError("complete open-trades response unavailable")
        return response["trades"]

    def get_transactions_since(self, transaction_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/v3/accounts/{self.cfg.oanda_account_id}/transactions/sinceid", params={"id": str(transaction_id)})

    def get_candles(self, instrument: str, granularity: str, count: int, price: str = "M") -> List[Dict[str, Any]]:
        data = self.request(
            "GET",
            f"/v3/instruments/{instrument}/candles",
            params={"granularity": granularity, "count": str(count), "price": str(price or "M")},
        )
        return [c for c in data.get("candles", []) if c.get("complete")]

    def create_market_order(
        self,
        instrument: str,
        units: int,
        price_bound: Optional[float],
        stop_loss: Optional[float],
        take_profit: Optional[float],
        trailing_stop_distance: Optional[float],
        precision: int,
        tag: str,
    ) -> Dict[str, Any]:
        order: Dict[str, Any] = {
            "type": "MARKET",
            "instrument": instrument,
            "units": str(int(units)),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "clientExtensions": {"tag": tag[:128], "comment": "oanda_forex_gpt_manager_v5_17"},
        }
        if price_bound is not None:
            order["priceBound"] = fmt_price(price_bound, precision)
        if stop_loss is not None:
            order["stopLossOnFill"] = {"price": fmt_price(stop_loss, precision), "timeInForce": "GTC"}
        if take_profit is not None:
            order["takeProfitOnFill"] = {"price": fmt_price(take_profit, precision), "timeInForce": "GTC"}
        if trailing_stop_distance is not None and trailing_stop_distance > 0:
            order["trailingStopLossOnFill"] = {"distance": fmt_price(trailing_stop_distance, precision), "timeInForce": "GTC"}
        return self.request("POST", f"/v3/accounts/{self.cfg.oanda_account_id}/orders", body={"order": order})

    def close_trade(self, trade_id: str, units: Optional[int] = None) -> Dict[str, Any]:
        body = {"units": "ALL" if units is None else str(abs(int(units)))}
        return self.request("PUT", f"/v3/accounts/{self.cfg.oanda_account_id}/trades/{trade_id}/close", body=body)

    def set_dependent_orders(
        self,
        trade_id: str,
        precision: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing_stop_distance: Optional[float] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if stop_loss is not None:
            body["stopLoss"] = {"price": fmt_price(stop_loss, precision), "timeInForce": "GTC"}
        if take_profit is not None:
            body["takeProfit"] = {"price": fmt_price(take_profit, precision), "timeInForce": "GTC"}
        if trailing_stop_distance is not None and trailing_stop_distance > 0:
            body["trailingStopLoss"] = {"distance": fmt_price(trailing_stop_distance, precision), "timeInForce": "GTC"}
        if not body:
            return {"skipped": "no dependent order fields supplied"}
        return self.request("PUT", f"/v3/accounts/{self.cfg.oanda_account_id}/trades/{trade_id}/orders", body=body)


def fmt_price(x: float, precision: int) -> str:
    return f"{float(x):.{max(0, int(precision))}f}"


def build_instrument_meta(raw_instruments: List[Dict[str, Any]]) -> Dict[str, InstrumentMeta]:
    out: Dict[str, InstrumentMeta] = {}
    for x in raw_instruments:
        if str(x.get("type", "")).upper() != "CURRENCY":
            continue
        try:
            m = InstrumentMeta(
                name=str(x["name"]),
                display_name=str(x.get("displayName") or x["name"]),
                type=str(x.get("type", "")),
                margin_rate=safe_float(x.get("marginRate"), 0.02),
                pip_location=safe_int(x.get("pipLocation"), -4),
                display_precision=safe_int(x.get("displayPrecision"), 5),
                trade_units_precision=safe_int(x.get("tradeUnitsPrecision"), 0),
                minimum_trade_size=safe_float(x.get("minimumTradeSize"), 1.0),
            )
            out[m.name] = m
        except Exception:
            continue
    return out


def normalize_instrument(instrument: Any) -> str:
    """Normalize common model/user symbol forms to OANDA instrument format.

    Examples:
        USD/JPY -> USD_JPY
        usdjpy  -> USD_JPY
        USD-JPY -> USD_JPY
    """
    raw = str(instrument or "").strip().upper()
    raw = raw.replace("/", "_").replace("-", "_").replace(" ", "_")
    raw = re.sub(r"_+", "_", raw).strip("_")
    if "_" not in raw and re.fullmatch(r"[A-Z]{6}", raw):
        return f"{raw[:3]}_{raw[3:]}"
    return raw


def split_instrument(instrument: str) -> Tuple[str, str]:
    instrument = normalize_instrument(instrument)
    if "_" not in instrument:
        return instrument[:3], instrument[3:]
    return tuple(instrument.split("_", 1))  # type: ignore[return-value]


def instrument_has_usd(instrument: Any) -> bool:
    base, quote = split_instrument(normalize_instrument(instrument))
    return base == "USD" or quote == "USD"


def instrument_is_cross_pair(instrument: Any) -> bool:
    inst = normalize_instrument(instrument)
    return bool(inst and "_" in inst and not instrument_has_usd(inst))


def instrument_is_major_usd_pair(instrument: Any, major_pairs: Optional[Sequence[str]] = None) -> bool:
    inst = normalize_instrument(instrument)
    pairs = major_pairs or ["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"]
    return inst in {normalize_instrument(x) for x in pairs}


def instrument_currency_set(instrument: Any) -> List[str]:
    base, quote = split_instrument(normalize_instrument(instrument))
    return [base, quote] if base and quote else []


VOLATILE_BUCKET_CURRENCIES = {"ZAR", "MXN", "TRY", "NOK", "SEK", "CZK", "HKD", "SGD"}


def instrument_coverage_bucket(
    instrument: Any,
    *,
    major_pairs: Optional[Sequence[str]] = None,
    volatile_pairs: Optional[Sequence[str]] = None,
) -> str:
    inst = normalize_instrument(instrument)
    if not inst:
        return "unknown"
    volatile_set = {normalize_instrument(x) for x in (volatile_pairs or []) if normalize_instrument(x)}
    base, quote = split_instrument(inst)
    if inst in volatile_set or base in VOLATILE_BUCKET_CURRENCIES or quote in VOLATILE_BUCKET_CURRENCIES:
        return "volatile_exotic"
    if instrument_is_major_usd_pair(inst, major_pairs):
        return "usd_majors"
    if instrument_has_usd(inst):
        return "usd_other"
    if "JPY" in {base, quote}:
        return "jpy_crosses"
    if "EUR" in {base, quote}:
        return "eur_crosses"
    if "GBP" in {base, quote}:
        return "gbp_crosses"
    if "CHF" in {base, quote}:
        return "chf_crosses"
    if {base, quote} & {"AUD", "NZD", "CAD"}:
        return "aud_nzd_cad_crosses"
    return "other_crosses"


def bid_ask(price: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    bids = price.get("bids") or []
    asks = price.get("asks") or []
    bid = safe_float(bids[0].get("price"), math.nan) if bids else safe_float(price.get("closeoutBid"), math.nan)
    ask = safe_float(asks[0].get("price"), math.nan) if asks else safe_float(price.get("closeoutAsk"), math.nan)
    return (bid if math.isfinite(bid) else None, ask if math.isfinite(ask) else None)


def mid_price(price: Dict[str, Any]) -> Optional[float]:
    bid, ask = bid_ask(price)
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def price_tradeable(price: Dict[str, Any]) -> bool:
    val = price.get("tradeable", True)
    if isinstance(val, bool):
        return val
    return str(val).lower() in {"true", "1", "yes", "y"}


def spread_pips(price: Dict[str, Any], meta: InstrumentMeta) -> float:
    bid, ask = bid_ask(price)
    if bid is None or ask is None:
        return 999999.0
    return abs(ask - bid) / meta.pip_size


def ccy_to_account_rate(ccy: str, prices: Dict[str, Dict[str, Any]], account_currency: str) -> Optional[float]:
    ccy = ccy.upper()
    account_currency = account_currency.upper()
    if ccy == account_currency:
        return 1.0
    direct = f"{ccy}_{account_currency}"
    inverse = f"{account_currency}_{ccy}"
    if direct in prices:
        p = mid_price(prices[direct])
        if p and p > 0:
            return p
    if inverse in prices:
        p = mid_price(prices[inverse])
        if p and p > 0:
            return 1.0 / p
    return None


def quote_to_account_rate(instrument: str, prices: Dict[str, Dict[str, Any]], account_currency: str) -> Optional[float]:
    _, quote = split_instrument(instrument)
    return ccy_to_account_rate(quote, prices, account_currency)


def base_to_account_rate(instrument: str, prices: Dict[str, Dict[str, Any]], account_currency: str) -> Optional[float]:
    base, _ = split_instrument(instrument)
    return ccy_to_account_rate(base, prices, account_currency)


def side_to_units(direction: str, abs_units: int) -> int:
    d = str(direction).strip().upper()
    if d in {"SHORT", "SELL"}:
        return -abs(abs_units)
    return abs(abs_units)


def trade_direction_from_units(units: Any) -> str:
    return "LONG" if safe_float(units) > 0 else "SHORT"


def trade_entry_price(trade: Dict[str, Any]) -> Optional[float]:
    for key in ("price", "averagePrice", "initialPrice"):
        val = as_optional_float(trade.get(key)) if "as_optional_float" in globals() else None
        if val is not None and val > 0:
            return val
    return None


def existing_stop_price_from_trade(trade: Dict[str, Any]) -> Optional[float]:
    # OANDA openTrade commonly embeds dependent orders as stopLossOrder / trailingStopLossOrder.
    for key in ("stopLossOrder", "guaranteedStopLossOrder"):
        obj = trade.get(key)
        if isinstance(obj, dict):
            val = as_optional_float(obj.get("price")) if "as_optional_float" in globals() else None
            if val is not None and val > 0:
                return val
    return None


def round_stop_to_valid_side(direction: str, stop: float, exit_price: float, min_gap_price: float) -> float:
    if direction == "LONG":
        return min(stop, exit_price - min_gap_price)
    return max(stop, exit_price + min_gap_price)


def stop_is_more_protective(direction: str, candidate: float, current_stop: Optional[float]) -> bool:
    if current_stop is None or current_stop <= 0:
        return True
    if direction == "LONG":
        return candidate > current_stop
    return candidate < current_stop


def round_units(abs_units: float, meta: InstrumentMeta) -> int:
    if meta.trade_units_precision <= 0:
        rounded = math.floor(abs_units)
    else:
        factor = 10 ** meta.trade_units_precision
        rounded = math.floor(abs_units * factor) / factor
    if rounded < meta.minimum_trade_size:
        return 0
    return int(rounded)


# =============================================================================
# Feature packet
# =============================================================================

def candles_to_rows(candles: List[Dict[str, Any]]) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for c in candles:
        m = c.get("mid") or {}
        try:
            rows.append({"o": float(m["o"]), "h": float(m["h"]), "l": float(m["l"]), "c": float(m["c"])})
        except Exception:
            continue
    return rows


def atr_pips(rows: List[Dict[str, float]], pip_size: float, period: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(rows)):
        high = rows[i]["h"]
        low = rows[i]["l"]
        prev_close = rows[i-1]["c"]
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not trs:
        return 0.0
    n = min(period, len(trs))
    return sum(trs[-n:]) / n / pip_size


def candle_snapshot(rows: List[Dict[str, float]], meta: InstrumentMeta) -> Dict[str, Any]:
    if not rows:
        return {"available": False}
    closes = [r["c"] for r in rows]
    last = closes[-1]
    def change(n: int) -> Optional[float]:
        if len(closes) <= n:
            return None
        return (last - closes[-1-n]) / meta.pip_size
    return {
        "available": True,
        "last_close": last,
        "change_3_bars_pips": change(3),
        "change_12_bars_pips": change(12),
        "change_24_bars_pips": change(24),
        "atr_14_pips": atr_pips(rows, meta.pip_size, 14),
    }


def account_summary_for_prompt(account: Dict[str, Any]) -> Dict[str, Any]:
    balance = safe_float(account.get("balance"))
    nav = safe_float(account.get("NAV", account.get("nav", balance)))
    margin_used = safe_float(account.get("marginUsed", account.get("margin_used", 0.0)))
    margin_available = safe_float(account.get("marginAvailable", account.get("margin_available", 0.0)))
    return {
        "id": account.get("id", ""),
        "currency": account.get("currency", ""),
        "balance": balance,
        "nav": nav,
        "unrealized_pl": safe_float(account.get("unrealizedPL")),
        "margin_used": margin_used,
        "margin_available": margin_available,
        "margin_used_pct_of_nav": (margin_used / nav * 100.0) if nav > 0 else 0.0,
        "open_trade_count": safe_int(account.get("openTradeCount")),
        "open_position_count": safe_int(account.get("openPositionCount")),
        "pending_order_count": safe_int(account.get("pendingOrderCount")),
    }


def summarize_trade_for_prompt(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(trade.get("id", "")),
        "instrument": str(trade.get("instrument", "")),
        "direction": trade_direction_from_units(trade.get("currentUnits")),
        "current_units": safe_float(trade.get("currentUnits")),
        "initial_units": safe_float(trade.get("initialUnits")),
        "price": safe_float(trade.get("price")),
        "open_time": trade.get("openTime", ""),
        "unrealized_pl": safe_float(trade.get("unrealizedPL")),
        "realized_pl": safe_float(trade.get("realizedPL")),
        "financing": safe_float(trade.get("financing")),
        "state": trade.get("state", ""),
        "take_profit_order": trade.get("takeProfitOrder", {}),
        "stop_loss_order": trade.get("stopLossOrder", {}),
        "trailing_stop_loss_order": trade.get("trailingStopLossOrder", {}),
    }


def build_currency_exposure(open_trades: List[Dict[str, Any]]) -> Dict[str, float]:
    exposure: Dict[str, float] = {}
    for t in open_trades:
        inst = str(t.get("instrument", ""))
        if "_" not in inst:
            continue
        base, quote = split_instrument(inst)
        units = safe_float(t.get("currentUnits"))
        exposure[base] = exposure.get(base, 0.0) + units
        exposure[quote] = exposure.get(quote, 0.0) - units
    return exposure


# =============================================================================
# OpenAI Responses API
# =============================================================================

DECISION_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "as_of": {"type": "string"},
        "market_summary": {"type": "string"},
        "portfolio_bias": {"type": "string"},
        "portfolio_mode": {"type": "string"},
        "deployment_mode": {"type": "string"},
        "active_margin_target_min_pct": {"type": "number"},
        "active_margin_target_max_pct": {"type": "number"},
        "underdeployment_reason": {"type": "string"},
        "margin_deployment_plan": {"type": "array", "items": {"type": "string"}},
        "currency_ranking": {"type": "array", "items": {"type": "string"}},
        "currency_strength_table": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "currency": {"type": "string"},
                    "bias": {"type": "string"},
                    "evidence": {"type": "string"},
                    "confidence": {"type": "number"},
                    "news_risk": {"type": "number"},
                    "technical_alignment": {"type": "string"},
                },
            },
        },
        "macro_thesis_consistency": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "portfolio_bias_matches_summary": {"type": "boolean"},
                "portfolio_bias_matches_orders": {"type": "boolean"},
                "portfolio_bias_matches_event_permissions": {"type": "boolean"},
                "contradictions": {"type": "array", "items": {"type": "string"}},
                "blocked_or_avoided_theses": {"type": "array", "items": {"type": "string"}},
                "fresh_evidence_required_to_change_bias": {"type": "string"},
            },
        },
        "why_mixed_usd_exposure_is_allowed": {"type": "string"},
        "non_usd_cross_pair_review": {"type": "string"},
        "pair_bucket_coverage_review": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "bucket": {"type": "string"},
                    "reviewed_instruments": {"type": "array", "items": {"type": "string"}},
                    "best_instrument": {"type": ["string", "null"]},
                    "action": {"type": "string", "enum": ["OPEN", "WATCH", "REJECT"]},
                    "reason": {"type": "string"},
                    "execution_quota_note": {"type": "string"},
                },
            },
        },
        "risk_notes": {"type": "array", "items": {"type": "string"}},
        "event_permissions": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "theme": {"type": "string"},
                    "allowed_pairs": {"type": "array", "items": {"type": "string"}},
                    "allowed_directions": {"type": "object", "additionalProperties": {"type": "string"}},
                    "max_scout_risk_pct": {"type": "number"},
                    "expires_hours": {"type": "number"},
                    "requires_basket_confirmation": {"type": "boolean"},
                    "reason": {"type": "string"},
                },
            },
        },
        "open_position_actions": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "trade_id": {"type": ["string", "null"]},
                    "instrument": {"type": "string"},
                    "action": {"type": "string", "enum": ["HOLD", "TIGHTEN", "PARTIAL_CLOSE", "CLOSE", "FLIP"]},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT", "NONE"]},
                    "outlook_confidence": {"type": "number"},
                    "risk_pct": {"type": "number"},
                    "partial_close_pct": {"type": "number"},
                    "stop_loss": {"type": ["number", "null"]},
                    "take_profit": {"type": ["number", "null"]},
                    "trailing_stop_pips": {"type": ["number", "null"]},
                    "reason": {"type": "string"},
                    "what_would_change_my_mind": {"type": "string"},
                },
            },
        },
        "new_trade_candidates": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "rank": {"type": "number"},
                    "instrument": {"type": "string"},
                    "coverage_bucket": {"type": "string"},
                    "action": {"type": "string", "enum": ["OPEN", "WATCH", "REJECT"]},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT", "NONE"]},
                    "outlook_confidence": {"type": "number"},
                    "risk_pct": {"type": "number"},
                    "expected_R": {"type": ["number", "null"]},
                    "concrete_no_trade_blocker": {"type": ["string", "null"]},
                    "margin_intensity": {"type": "string"},
                    "expected_hold_hours": {"type": "number"},
                    "entry_min": {"type": ["number", "null"]},
                    "entry_max": {"type": ["number", "null"]},
                    "stop_loss": {"type": ["number", "null"]},
                    "take_profit": {"type": ["number", "null"]},
                    "tp1": {"type": ["number", "null"]},
                    "tp2": {"type": ["number", "null"]},
                    "trailing_stop_pips": {"type": ["number", "null"]},
                    "why_now": {"type": "string"},
                    "reason": {"type": "string"},
                    "what_would_change_my_mind": {"type": "string"},
                },
            },
        },
        "orders_to_execute": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "instrument": {"type": "string"},
                    "coverage_bucket": {"type": "string"},
                    "action": {"type": "string", "enum": ["OPEN", "SCALE_IN", "FLIP", "CLOSE", "PARTIAL_CLOSE", "TIGHTEN", "HOLD"]},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT", "NONE"]},
                    "trade_id": {"type": ["string", "null"]},
                    "outlook_confidence": {"type": "number"},
                    "risk_pct": {"type": "number"},
                    "expected_R": {"type": ["number", "null"]},
                    "expectancy_critic": {
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "verdict": {"type": "string"},
                            "expected_move_pips": {"type": ["number", "null"]},
                            "invalidation_pips": {"type": ["number", "null"]},
                            "expected_R": {"type": ["number", "null"]},
                            "why_this_is_not_the_same_failed_thesis": {"type": "string"},
                            "flip_or_no_trade_conditions": {"type": "string"},
                            "recommended_stop_pips": {"type": ["number", "null"]},
                            "risk_size_adjustment": {"type": "string"},
                        },
                    },
                    "partial_close_pct": {"type": "number"},
                    "stop_loss": {"type": ["number", "null"]},
                    "take_profit": {"type": ["number", "null"]},
                    "trailing_stop_pips": {"type": ["number", "null"]},
                    "reason": {"type": "string"},
                },
            },
        },
    },
    "required": ["as_of", "market_summary", "portfolio_bias", "currency_strength_table", "macro_thesis_consistency", "non_usd_cross_pair_review", "pair_bucket_coverage_review", "risk_notes", "open_position_actions", "new_trade_candidates", "orders_to_execute"],
}


SYSTEM_PROMPT = """You are the decision engine for an always-on OANDA leveraged forex portfolio manager.

You must evaluate the entire account on every call. This is not just an entry scanner.
Manage existing positions first, then consider new positions across all supplied tradeable forex pairs.

Use live/current macro, central-bank, rates, geopolitical, risk-sentiment, commodities, and country-specific news when web search is available. Do not invent news; if news is uncertain, say uncertainty in reason/risk_notes.

Core job:
1. Decide HOLD, TIGHTEN, PARTIAL_CLOSE, CLOSE, or FLIP for every open trade.
2. open_position_actions is mandatory portfolio management: return exactly one action for every open trade supplied in open_trades. If no change is warranted, return HOLD with the trade_id/instrument/direction and current outlook_confidence. Never omit an open trade just because you have no change.
3. Rank all possible new positions across the supplied pairs before choosing executable orders. USD pairs and non-USD crosses compete in one global ranking. Pair type, bucket, and USD/non-USD status are descriptive labels, not quality penalties; viability is based on edge, expected_R, executable spread/stop geometry, event risk, and portfolio fit.
4. Recommend executable orders only when the outlook is strong enough.
5. Set outlook_confidence from 0 to 100 based on the quality and durability of the current outlook.
6. Set risk_pct directly. The script will cap your requested risk if it exceeds account guardrails.
7. These are paper accounts and should be fairly aggressive by default, but not blindly margin-maxing. Choose deployment_mode as DEFENSIVE, LIGHT, NORMAL, AGGRESSIVE, or EVENT and set active_margin_target_min_pct / active_margin_target_max_pct from the live regime.
8. Use these aggressive regime bands unless there is a concrete reason to override: DEFENSIVE 10-25%, LIGHT 20-35%, NORMAL 35-55%, AGGRESSIVE 50-75%, EVENT 65-85%. Mixed/slight macro can still be LIGHT/NORMAL; strong confirmed macro plus technical alignment can be AGGRESSIVE.
9. If current margin is below the active band and the thesis is still valid, either deploy via the best-ranked non-duplicative OPENs or SCALE_IN an already-profitable same-direction winner. SCALE_IN is only for winners with protective stops; never average down a loser.
10. If you stay under the active band, fill underdeployment_reason with a concrete reason: macro contradiction, poor spread/stop geometry, crowded currency exposure, event risk, no clean stop, or conflicting pair signals. Do not merely say "waiting for confirmation."
11. Avoid redundant exposure and correlated overstacking, but do not hide in one tiny position when a clean directional thesis exists. Use smaller diversified positions when the macro case is broad; use SCALE_IN when the existing winner remains the cleanest expression and risk/reward is still acceptable.
12. Take profit, tighten, partially close, or fully close if the outlook has weakened, reversed, become less asymmetric, or margin can be better used elsewhere.
13. FLIP only when the opposite thesis is clearly stronger than the existing thesis.
14. State portfolio_mode as one of USD_BULLISH, USD_BEARISH, RELATIVE_VALUE, DEFENSIVE, or MIXED_UNCLEAR. If holding both long-USD and short-USD exposures, use RELATIVE_VALUE and explain the currency ranking.
15. Provide non_usd_cross_pair_review explaining the best non-USD crosses considered and why they were accepted, watched, or rejected. If a non-USD cross is objectively viable, rank it and use OPEN/WATCH/REJECT by the same criteria as a USD pair; do not leave it only in review because it is a cross. Do not force cross-pair trades if USD pairs are objectively better.
16. Always return currency_strength_table. Include USD, EUR, GBP, JPY, CHF, CAD, AUD, NZD, and any high-volatility currencies visible in the packet such as MXN, ZAR, TRY, NOK, SEK, CNH, HKD, SGD, PLN, HUF, CZK, THB, or DKK when relevant. Each row needs bias, evidence, confidence, news_risk, and technical_alignment.
17. Always return macro_thesis_consistency. portfolio_bias, market_summary, currency_strength_table, orders_to_execute, open_position_actions, and event_permissions must agree. If they do not agree, choose HOLD/WATCH rather than executing and list the contradiction.
18. Do not say USD_BEARISH while recommending USD-long trades, or USD_BULLISH while recommending USD-short trades, unless portfolio_mode is RELATIVE_VALUE and the trade has a concrete pair-specific exception. Name that exception explicitly.
19. If recent losses show a failed same-currency thesis, quarantine that specific thesis until fresh evidence appears. Fresh evidence means a new central-bank/rates/news catalyst, a clear cross-currency ranking change, or a technical-flow reversal confirmed by the technical_context packet. Price movement alone is not enough. This quarantine is thesis-specific, not an account-wide defensive posture; independent viable pairs can still justify LIGHT/NORMAL/AGGRESSIVE deployment.
20. When staying flat or underdeployed, underdeployment_reason must be one of: same-thesis failed thesis quarantine, macro contradiction, poor spread/stop geometry, crowded exposure, event/news risk, weak expected_R, or no clean pair expression. Recent loss, drawdown, or defensive-posture language alone is not a concrete reason.

Do not use fixed technical rules. Do not require local-model agreement. Price movement alone is not a thesis.
Every OPEN, SCALE_IN, or FLIP must include stop_loss. Include take_profit and/or trailing_stop_pips when useful.
Use OANDA instrument format with underscores, e.g. USD_JPY not USD/JPY. Never return concatenated symbols like USDJPY.
No category quota: if the best trades after the full scan are all USD pairs, orders_to_execute may all be USD pairs. If the best trades are all non-USD crosses, orders_to_execute may all be non-USD crosses. Pair type must never demote a viable candidate; spread/stop, event risk, macro contradiction, exposure, and expected_R can. Do not add placeholder REJECT rows just to show coverage.
If an instrument is already open, manage the original trade through open_position_actions. Do not put a duplicate OPEN for an already-open instrument; use SCALE_IN in orders_to_execute only when intentionally adding to an already-profitable same-direction winner.
If you are unsure about an open trade, HOLD or TIGHTEN; do not omit it.
orders_to_execute must be the final globally ranked executable list after evaluating all supplied possibilities; do not output category-first ordering.
If shutdown_mode is ACTIVE in the user/context packet, do not recommend any new OPEN orders. Manage only existing positions by tightening, taking partial profits, closing weak positions, or holding briefly with protected downside.

Also return event_permissions for fast local event scanning. These are not immediate orders. They are small pre-authorizations for a later local M5 scanner if a synchronized basket move begins before the next scheduled GPT call. Use themes like USD_SELLOFF, USD_RALLY, JPY_STRENGTH, JPY_WEAKNESS, CHF_STRENGTH, CHF_WEAKNESS, RISK_ON, RISK_OFF, COMMODITY_CURRENCY_STRENGTH, COMMODITY_CURRENCY_WEAKNESS. For each permission include allowed_pairs, allowed_directions, max_scout_risk_pct, expires_hours, requires_basket_confirmation, and reason. Only grant permissions that match your current macro thesis.

If technical_context is supplied, use it as flow evidence, not as a standalone trade signal. It should help detect when macro/news thesis and all-pair flow disagree, when volatile-pair spikes are clustered, and when the technical/scout accounts are confirming or rejecting the same currency thesis.

Return JSON only matching the requested schema."""


class OpenAIClient:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.headers = {
            "Authorization": f"Bearer {cfg.openai_api_key}",
            "Content-Type": "application/json",
        }
        if cfg.disable_http_keepalive:
            self.headers["Connection"] = "close"
        self.s = requests.Session()
        self.s.headers.update(self.headers)

    def reset_session(self) -> None:
        try:
            self.s.close()
        except Exception:
            pass
        self.s = requests.Session()
        self.s.headers.update(self.headers)

    def require_auth(self) -> None:
        if not self.cfg.openai_api_key:
            raise RuntimeError("Missing OPENAI_API_KEY/openaiapi in creds or environment")

    def create_decision(self, packet: Dict[str, Any]) -> Dict[str, Any]:
        self.require_auth()
        input_payload = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(packet, indent=2, sort_keys=True, default=str)},
        ]
        body: Dict[str, Any] = {
            "model": self.cfg.openai_model,
            "input": input_payload,
            "store": False,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "forex_portfolio_decision",
                    "schema": DECISION_SCHEMA,
                    "strict": False,
                }
            },
        }
        if self.cfg.enable_openai_web_search:
            body["tools"] = [{"type": "web_search_preview"}]
        try:
            return self._post_response(body)
        except Exception as exc:
            if self.cfg.enable_openai_web_search:
                log(f"[WARN] OpenAI web-search request failed, retrying without web_search tool: {exc}")
                body.pop("tools", None)
                return self._post_response(body)
            raise

    def _post_response(self, body: Dict[str, Any]) -> Dict[str, Any]:
        parsed, _ = self._post_response_with_raw(body)
        return parsed

    def _post_response_with_raw(
        self,
        body: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        attempts = max(1, int(self.cfg.http_max_retries) + 1)
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                if self.cfg.disable_http_keepalive:
                    self.reset_session()
                resp = self.s.post(
                    "https://api.openai.com/v1/responses",
                    data=json.dumps(body),
                    timeout=self.cfg.openai_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise RuntimeError(f"OpenAI responses network failure after {attempts} attempts: {exc}") from exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                log(f"[WARN] OpenAI transient network error; retry {attempt}/{attempts - 1} in {wait:.1f}s: {exc}")
                self.reset_session()
                time.sleep(wait)
                continue

            if resp.status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
                if attempt < attempts:
                    wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                    log(f"[WARN] OpenAI transient HTTP {resp.status_code}; retry {attempt}/{attempts - 1} in {wait:.1f}s")
                    time.sleep(wait)
                    continue

            if resp.status_code >= 400:
                raise RuntimeError(f"OpenAI responses failed {resp.status_code}: {resp.text[:1600]}")
            try:
                data = resp.json()
                if self.cfg.disable_http_keepalive:
                    resp.close()
            except ValueError as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise RuntimeError(f"OpenAI responses returned non-JSON after {attempts} attempts: {resp.text[:500]}") from exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                log(f"[WARN] OpenAI non-JSON response; retry {attempt}/{attempts - 1} in {wait:.1f}s")
                time.sleep(wait)
                continue

            text = extract_response_text(data)
            if not text:
                raise RuntimeError("OpenAI response did not include output text")
            try:
                return json.loads(text), data
            except json.JSONDecodeError as direct_exc:
                # Fallback: extract outermost JSON object if surrounding text appears.
                parse_exc = direct_exc
                m = re.search(r"\{.*\}", text, flags=re.S)
                if m:
                    try:
                        return json.loads(m.group(0)), data
                    except json.JSONDecodeError as extracted_exc:
                        parse_exc = extracted_exc

                last_exc = parse_exc
                if attempt >= attempts:
                    raise parse_exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                incomplete_reason = ""
                if isinstance(data.get("incomplete_details"), dict):
                    incomplete_reason = str(data["incomplete_details"].get("reason") or "")
                detail = f" ({incomplete_reason})" if incomplete_reason else ""
                log(
                    f"[WARN] OpenAI output was invalid or truncated JSON{detail}; "
                    f"retry {attempt}/{attempts - 1} in {wait:.1f}s"
                )
                time.sleep(wait)
                continue
        raise RuntimeError(f"OpenAI responses failed unexpectedly: {last_exc}")


def extract_response_text(data: Dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str):
        return data["output_text"]
    parts: List[str] = []
    for item in data.get("output", []) or []:
        for content in item.get("content", []) or []:
            if content.get("type") in {"output_text", "text"} and isinstance(content.get("text"), str):
                parts.append(content["text"])
    return "\n".join(parts).strip()


# =============================================================================
# Manager
# =============================================================================

class ForexManager:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.oanda = OandaClient(cfg)
        self.openai = OpenAIClient(cfg)
        self.instrument_meta: Dict[str, InstrumentMeta] = {}
        self.instruments: List[str] = []
        self._scout_profile_cache: Dict[str, Dict[str, Any]] = {}
        self._scout_profile_loaded_at: float = 0.0
        self.lane_label = str(getattr(cfg, "account_lane", "single") or "single")
        self.ensure_dirs()
        self.full_logger = FullEventLogger(cfg)
        try:
            self.full_logger.log_event("script_start", status="starting", account_id=getattr(cfg, "oanda_account_id", ""), reason="manager initialized", raw={"config": self.redacted_config()})
        except Exception:
            pass

    def lane_prefix(self) -> str:
        return f"[{self.lane_label}] " if self.lane_label and self.lane_label != "single" else ""

    def ensure_dirs(self) -> None:
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self.cfg.raw_dir.mkdir(parents=True, exist_ok=True)

    def log_error(self, context: str, exc: BaseException) -> None:
        text = error_text(context, exc)
        self.cfg.errors_log.parent.mkdir(parents=True, exist_ok=True)
        with self.cfg.errors_log.open("a", encoding="utf-8") as f:
            f.write(text)
        log(f"{self.lane_prefix()}[ERROR] {context}: {type(exc).__name__}: {exc}")
        try:
            self.full_logger.log_event("exception", status="error", severity="ERROR", reason=context, error_type=type(exc).__name__, error_message=str(exc)[:1000], raw={"traceback": traceback.format_exc()})
        except Exception:
            pass

    def validate_config(self) -> None:
        missing = []
        if not self.cfg.openai_api_key:
            missing.append("OPENAI_API_KEY/openaiapi")
        if not self.cfg.oanda_api_key:
            missing.append("OANDA_API_KEY/OANDA_API_TOKEN/oandaapi")
        if not self.cfg.oanda_account_id:
            missing.append(self.cfg.account_display_name or "OANDA_ACCOUNT_ID")
        if missing:
            details = f"Missing: {', '.join(missing)}. Creds loaded from: {CREDS_LOADED_PATH or 'none'}. {CREDS_LOAD_ERROR}"
            raise RuntimeError(details)
        if self.cfg.oanda_env == "live" and not self.cfg.allow_live and self.cfg.execute_trades:
            raise RuntimeError("OANDA_ENV=live with execution requested, but FOREX_ALLOW_LIVE=1 is not set.")

    def should_execute(self) -> bool:
        if not self.cfg.execute_trades:
            return False
        if not self.cfg.auto_execute_gpt_actions:
            return False
        if self.cfg.oanda_env == "live" and not self.cfg.allow_live:
            return False
        return True

    def shutdown_mode_active(self) -> bool:
        return self.cfg.shutdown_mode in {"fade", "close_now"}

    def shutdown_mode_label(self) -> str:
        return self.cfg.shutdown_mode.upper() if self.shutdown_mode_active() else "OFF"

    def print_config(self) -> None:
        d = asdict(self.cfg)
        d["openai_api_key"] = "***" if self.cfg.openai_api_key else ""
        d["oanda_api_key"] = "***" if self.cfg.oanda_api_key else ""
        for k, v in list(d.items()):
            if isinstance(v, Path):
                d[k] = str(v)
        d["creds_loaded_path"] = str(CREDS_LOADED_PATH) if CREDS_LOADED_PATH else ""
        d["creds_load_error"] = CREDS_LOAD_ERROR
        d["config_loaded_path"] = "EMBEDDED_CONFIG inside manager script"
        d["config_load_error"] = CONFIG_LOAD_ERROR
        d["embedded_config"] = {k: ("***" if k in CREDENTIAL_SETTING_NAMES else v) for k, v in CONFIG.items()}
        print(json.dumps(d, indent=2, sort_keys=True))

    def load_state(self) -> Dict[str, Any]:
        state = read_json(self.cfg.state_path, {
            "last_gpt_runs": {},
            "last_monitor_utc": None,
            "last_gpt_scan_utc": None,
            "last_event_scan_utc": None,
            "last_event_gpt_utc": None,
            "last_shutdown_pass_utc": None,
            "event_permissions": [],
            "dry_run_orders": [],
            "recent_action_keys": {},
            "startup_reconciliations": [],
            "last_known_open_trades": [],
            "last_account_snapshot": {},
        })
        if not isinstance(state, dict):
            state = {}
        state.setdefault("last_gpt_runs", {})
        state.setdefault("last_monitor_utc", None)
        state.setdefault("last_gpt_scan_utc", None)
        state.setdefault("last_event_scan_utc", None)
        state.setdefault("last_event_gpt_utc", None)
        state.setdefault("last_shutdown_pass_utc", None)
        state.setdefault("event_permissions", [])
        state.setdefault("dry_run_orders", [])
        state.setdefault("recent_action_keys", {})
        state.setdefault("startup_reconciliations", [])
        state.setdefault("last_known_open_trades", [])
        state.setdefault("last_account_snapshot", {})
        return state

    def save_state(self, state: Dict[str, Any]) -> None:
        write_json(self.cfg.state_path, state)

    def cleanup_recent_action_keys(self, state: Dict[str, Any]) -> None:
        keys = state.setdefault("recent_action_keys", {})
        if not isinstance(keys, dict):
            state["recent_action_keys"] = {}
            return
        cutoff = utc_now() - dt.timedelta(minutes=max(1, int(self.cfg.action_dedupe_minutes)))
        dead: List[str] = []
        for key, stamp in keys.items():
            try:
                t = dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).astimezone(UTC)
                if t < cutoff:
                    dead.append(key)
            except Exception:
                dead.append(key)
        for key in dead:
            keys.pop(key, None)

    def action_key(self, action_type: str, action: Dict[str, Any], trade_id: str = "") -> str:
        inst = normalize_instrument(action.get("instrument", ""))
        direction = str(action.get("direction", "")).strip().upper()
        act = str(action.get("action", action_type)).strip().upper()
        tid = str(trade_id or action.get("trade_id") or "").strip()
        stable_parts = {
            "type": action_type.upper(),
            "action": act,
            "instrument": inst,
            "direction": direction,
            "trade_id": tid,
            "stop_loss": action.get("stop_loss"),
            "take_profit": action.get("take_profit", action.get("tp1")),
            "trailing_stop_pips": action.get("trailing_stop_pips"),
            "partial_close_pct": action.get("partial_close_pct"),
        }
        return stable_hash(stable_parts)

    def recently_attempted_action(self, action_key: str, state: Optional[Dict[str, Any]] = None) -> bool:
        if self.cfg.action_dedupe_minutes <= 0:
            return False
        state = state or self.load_state()
        self.cleanup_recent_action_keys(state)
        stamp = state.get("recent_action_keys", {}).get(action_key)
        if not stamp:
            self.save_state(state)
            return False
        try:
            t = dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).astimezone(UTC)
            return (utc_now() - t).total_seconds() <= self.cfg.action_dedupe_minutes * 60
        except Exception:
            return False

    def remember_action(self, action_key: str, state: Optional[Dict[str, Any]] = None) -> None:
        if self.cfg.action_dedupe_minutes <= 0 or not action_key:
            return
        state = state or self.load_state()
        self.cleanup_recent_action_keys(state)
        state.setdefault("recent_action_keys", {})[action_key] = iso_utc()
        self.save_state(state)

    def observe_open_trade(self, trade_id: str = "", instrument: str = "", direction: str = "") -> Dict[str, Any]:
        if not self.cfg.broker_recheck_before_actions:
            return trade_reconciliation.unknown("recheck_disabled", observed_epoch=time.time())
        try:
            rows = self.oanda.get_open_trades()
            return trade_reconciliation.observe_trade_rows(rows, trade_id=trade_id,
                instrument=normalize_instrument(instrument), direction=direction, observed_epoch=time.time())
        except Exception as exc:
            self.log_error("broker recheck open trades", exc)
            return trade_reconciliation.unknown("recheck_unavailable", observed_epoch=time.time())

    def find_open_trade(self, trade_id: str = "", instrument: str = "", direction: str = "") -> Optional[Dict[str, Any]]:
        # Compatibility lookup: callers needing proof of absence must use the typed observation.
        observed = self.observe_open_trade(trade_id=trade_id, instrument=instrument, direction=direction)
        return observed["trade"] if observed["status"] == "confirmed_open" else None

    def trade_lookup_blocks_open(self, instrument: str) -> bool:
        observed = self.observe_open_trade(instrument=instrument)
        if observed["status"] == "unknown":
            self.log_action({"instrument": instrument}, "open_recheck", "uncertain_after_recheck",
                reject_reason=observed["reason_code"], raw_extra={"trade_observation": observed})
        return observed["status"] != "confirmed_closed"

    def reconcile_startup_state(self) -> None:
        account = self.oanda.get_account_summary()
        open_trades = self.oanda.get_open_trades()
        summary = account_summary_for_prompt(account)
        trades_summary = [summarize_trade_for_prompt(t) for t in open_trades]
        self.update_trade_profit_memory(account, open_trades, reason="startup_reconcile")
        state = self.load_state()
        state["last_reconciled_utc"] = iso_utc()
        state["last_account_snapshot"] = summary
        state["last_known_open_trades"] = trades_summary
        rec = {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "nav": summary.get("nav"),
            "margin_used_pct": summary.get("margin_used_pct_of_nav"),
            "open_trade_count": len(open_trades),
            "open_trades": trades_summary,
        }
        state.setdefault("startup_reconciliations", []).append(rec)
        keep = max(1, int(self.cfg.startup_reconcile_keep))
        state["startup_reconciliations"] = state["startup_reconciliations"][-keep:]
        self.save_state(state)
        self.write_daily_recaps_around_now()
        log(f"{self.lane_prefix()}Startup reconcile: NAV={summary.get('nav', 0):.2f} margin_used_pct={summary.get('margin_used_pct_of_nav', 0):.2f} open_trades={len(open_trades)}")

    def instrument_allowed_for_lane(self, instrument: Any) -> bool:
        inst = normalize_instrument(instrument)
        mode = str(self.cfg.instrument_filter_mode or "all").strip().lower()
        if not inst:
            return False
        if mode in {"", "all", "single"}:
            return True
        if mode in {"all_except_gpt_exclude", "gpt_all_except_exclude"}:
            blocked = {normalize_instrument(x) for x in cfg_list("FOREX_GPT_EXCLUDE_PAIRS", default=[]) if normalize_instrument(x)}
            return inst not in blocked
        if mode in {"major", "maj", "major_usd", "usd_majors", "gpt", "gpt_reliable", "reliable"}:
            allowed = set(self.cfg.gpt_reliable_pairs or self.cfg.major_usd_pairs or [])
            return inst in allowed
        if mode in {"scout", "technical", "technical_scout", "spike", "spike_scout", "volatile", "vol"}:
            allowed = set(self.cfg.technical_scout_pairs or self.cfg.scout_volatile_pairs or [])
            return inst in allowed
        # Legacy compatibility only: this is no longer the preferred split.
        if mode in {"other", "others", "volatile_other", "non_major", "non_major_usd"}:
            return not instrument_is_major_usd_pair(inst, self.cfg.major_usd_pairs)
        return True

    def refresh_instruments(self) -> None:
        raw = self.oanda.get_instruments()
        meta = build_instrument_meta(raw)
        self.instrument_meta = meta
        routed = [inst for inst in meta.keys() if self.instrument_allowed_for_lane(inst)]
        self.instruments = sorted(routed)
        log(f"{self.lane_prefix()}Discovered {len(meta)} OANDA CURRENCY instruments; routed {len(self.instruments)} to this lane ({self.cfg.instrument_filter_mode}).")

    def todays_call_times(self, n: Optional[dt.datetime] = None) -> List[str]:
        n = n or ny_now()
        if n.weekday() == 4 and self.cfg.friday_call_times_ny:
            return list(self.cfg.friday_call_times_ny)
        return list(self.cfg.call_times_ny)

    def gpt_due(self, n: Optional[dt.datetime] = None, state: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str]]:
        n = n or ny_now()
        state = state or self.load_state()
        if fx_market_closed(n):
            return False, None
        last_scan_utc = state.get("last_gpt_scan_utc")
        if last_scan_utc and self.cfg.min_minutes_between_gpt_scans > 0:
            try:
                last_scan = dt.datetime.fromisoformat(str(last_scan_utc).replace("Z", "+00:00")).astimezone(UTC)
                age_min = (utc_now() - last_scan).total_seconds() / 60.0
                if age_min < self.cfg.min_minutes_between_gpt_scans:
                    return False, None
            except Exception:
                pass
        date_key = n.strftime("%Y-%m-%d")
        last_runs = state.setdefault("last_gpt_runs", {})
        for hhmm in self.todays_call_times(n):
            h, m = parse_hhmm(hhmm)
            scheduled = n.replace(hour=h, minute=m, second=0, microsecond=0)
            late_by_min = (n - scheduled).total_seconds() / 60.0
            # v5.3: no missed-slot backfill. A scheduled scan fires only inside
            # a small current-time window, so startup at 21:56 will not run the 21:15 slot.
            # The window keeps the loop robust if FOREX_LOOP_SLEEP_SECONDS is 60-180s.
            if 0 <= late_by_min <= self.cfg.schedule_window_minutes:
                key = f"{date_key} {hhmm}"
                if not last_runs.get(key):
                    return True, hhmm
        return False, None

    def mark_gpt_run(self, hhmm: Optional[str], state: Dict[str, Any], n: Optional[dt.datetime] = None) -> None:
        if not hhmm:
            return
        n = n or ny_now()
        key = f"{n.strftime('%Y-%m-%d')} {hhmm}"
        state.setdefault("last_gpt_runs", {})[key] = iso_utc()
        self.save_state(state)

    def mark_past_gpt_slots(self, state: Dict[str, Any], n: Optional[dt.datetime] = None, note: str = "launch_scan") -> int:
        """Count a launch/manual scan as the management call for any schedule slots already passed today.

        This prevents an always-on restart from doing a launch GPT scan and then immediately
        backfilling the most recent scheduled slot, which would cause two GPT decisions within
        a few minutes. Future slots are not marked.
        """
        n = n or ny_now()
        date_key = n.strftime("%Y-%m-%d")
        last_runs = state.setdefault("last_gpt_runs", {})
        marked = 0
        stamp = f"{iso_utc()} ({note})"
        for hhmm in self.todays_call_times(n):
            h, m = parse_hhmm(hhmm)
            scheduled = n.replace(hour=h, minute=m, second=0, microsecond=0)
            if scheduled <= n:
                key = f"{date_key} {hhmm}"
                if not last_runs.get(key):
                    last_runs[key] = stamp
                    marked += 1
        self.save_state(state)
        return marked

    # -------------------------------------------------------------------------
    # Weekend / Sunday reopen protection
    # -------------------------------------------------------------------------
    def sunday_reopen_minutes_since_open(self, n: Optional[dt.datetime] = None) -> Optional[int]:
        return minutes_since_sunday_open(n)

    def sunday_reopen_new_trades_blocked(self, n: Optional[dt.datetime] = None) -> bool:
        if not cfg_bool("FOREX_SUNDAY_REOPEN_MANAGER_ENABLED", default=True):
            return False
        mins = self.sunday_reopen_minutes_since_open(n)
        if mins is None:
            return False
        return 0 <= mins < cfg_int("FOREX_SUNDAY_REOPEN_BLOCK_UNRELATED_NEW_TRADES_MINUTES", "FOREX_SUNDAY_REOPEN_BLOCK_NEW_TRADES_MINUTES", default=20)

    def sunday_reopen_event_scanner_blocked(self, n: Optional[dt.datetime] = None) -> bool:
        if not cfg_bool("FOREX_SUNDAY_REOPEN_MANAGER_ENABLED", default=True):
            return False
        mins = self.sunday_reopen_minutes_since_open(n)
        if mins is None:
            return False
        return 0 <= mins < cfg_int("FOREX_SUNDAY_REOPEN_EVENT_SCANNER_DELAY_MINUTES", default=5)

    def weekend_summary_due(self, state: Optional[Dict[str, Any]] = None, n: Optional[dt.datetime] = None) -> bool:
        if not cfg_bool("FOREX_WEEKEND_SUMMARY_SCAN_ENABLED", default=True):
            return False
        n = n or ny_now()
        if n.weekday() != 6:
            return False
        hhmm = cfg_str("FOREX_WEEKEND_SUMMARY_SCAN_TIME_NY", default="16:45")
        window = cfg_int("FOREX_WEEKEND_SUMMARY_SCAN_WINDOW_MINUTES", default=20)
        if not within_hhmm_window(hhmm, window, n):
            return False
        state = state or self.load_state()
        key = n.strftime("%Y-%m-%d")
        return not state.get("last_weekend_summary_by_date", {}).get(key)

    def mark_weekend_summary_done(self, state: Optional[Dict[str, Any]] = None, n: Optional[dt.datetime] = None) -> None:
        n = n or ny_now()
        state = state or self.load_state()
        key = n.strftime("%Y-%m-%d")
        state.setdefault("last_weekend_summary_by_date", {})[key] = iso_utc()
        self.save_state(state)

    def sunday_reopen_due(self, state: Optional[Dict[str, Any]] = None, n: Optional[dt.datetime] = None) -> bool:
        if not cfg_bool("FOREX_SUNDAY_REOPEN_MANAGER_ENABLED", default=True):
            return False
        n = n or ny_now()
        if n.weekday() != 6:
            return False
        hhmm = cfg_str("FOREX_SUNDAY_REOPEN_SCAN_TIME_NY", default="17:15")
        window = cfg_int("FOREX_SUNDAY_REOPEN_SCAN_WINDOW_MINUTES", default=35)
        if not within_hhmm_window(hhmm, window, n):
            return False
        state = state or self.load_state()
        key = n.strftime("%Y-%m-%d")
        return not state.get("last_sunday_reopen_scan_by_date", {}).get(key)

    def mark_sunday_reopen_done(self, state: Optional[Dict[str, Any]] = None, n: Optional[dt.datetime] = None) -> None:
        n = n or ny_now()
        state = state or self.load_state()
        key = n.strftime("%Y-%m-%d")
        state.setdefault("last_sunday_reopen_scan_by_date", {})[key] = iso_utc()
        self.save_state(state)

    def update_trade_profit_memory(self, account: Optional[Dict[str, Any]], open_trades: List[Dict[str, Any]], reason: str = "snapshot") -> None:
        if not cfg_bool("FOREX_TRADE_PROFIT_MEMORY_ENABLED", default=True):
            return
        state = self.load_state()
        mem = state.setdefault("trade_profit_memory", {})
        now = iso_utc()
        for trade in open_trades or []:
            tid = str(trade.get("id", "")).strip()
            if not tid:
                continue
            current_pl = safe_float(trade.get("unrealizedPL"), 0.0)
            rec = mem.setdefault(tid, {})
            rec["instrument"] = normalize_instrument(trade.get("instrument"))
            rec["direction"] = trade_direction_from_units(trade.get("currentUnits"))
            rec["current_units"] = safe_float(trade.get("currentUnits"), 0.0)
            rec["entry_price"] = safe_float(trade.get("price"), 0.0)
            rec["last_unrealized_pl"] = current_pl
            rec["last_seen_utc"] = now
            rec["last_reason"] = reason
            old_max = safe_float(rec.get("max_unrealized_pl"), current_pl)
            old_min = safe_float(rec.get("min_unrealized_pl"), current_pl)
            if current_pl >= old_max:
                rec["max_unrealized_pl_time_utc"] = now
            rec["max_unrealized_pl"] = max(old_max, current_pl)
            rec["min_unrealized_pl"] = min(old_min, current_pl)
            rec["was_profitable"] = bool(rec.get("was_profitable")) or current_pl > 0
        live_ids = {str(t.get("id", "")) for t in open_trades or []}
        for tid, rec in list(mem.items()):
            if tid not in live_ids:
                rec.setdefault("closed_or_missing_first_seen_utc", now)
        if account is not None:
            summary = account_summary_for_prompt(account)
            state["last_profit_memory_account_snapshot"] = {"time_utc": now, "reason": reason, "account": summary}
            if fx_market_closed():
                state["latest_closed_market_reference"] = {
                    "time_utc": now,
                    "time_ny": iso_ny(),
                    "reason": reason,
                    "account": summary,
                    "open_trades": [summarize_trade_for_prompt(t) for t in open_trades or []],
                }
        self.save_state(state)

    def run_weekend_summary_scan(self, reason: str = "weekend_summary") -> None:
        if not cfg_bool("FOREX_WEEKEND_SUMMARY_USE_GPT", default=True):
            log("Weekend summary due, but GPT weekend summary disabled.")
            return
        log(f"Starting weekend summary scan: {reason} (planning only, no broker execution)")
        packet, prices, open_trades, raw_context = self.build_market_packet()
        packet["scan_context"] = {
            "reason": reason,
            "mode": "WEEKEND_SUMMARY_PLANNING_ONLY",
            "market_closed": fx_market_closed(),
            "execution_allowed": False,
            "instructions": [
                "FX market is closed. Do not propose executable broker orders.",
                "Summarize weekend news and gap risk for every open trade.",
                "Return open_position_actions as the intended Sunday reopen plan only.",
                "orders_to_execute must be empty while market is closed.",
            ],
        }
        packet.setdefault("instructions", {})["weekend_summary"] = "Planning only: no execution. Identify likely gap direction, danger trades, and first actions for Sunday reopen."
        ctx_hash = stable_hash(packet)
        decision = self.openai.create_decision(packet)
        decision["orders_to_execute"] = []
        decision = self.normalize_decision(decision, open_trades)
        self.annotate_pair_bucket_coverage(
            decision,
            packet.get("market_snapshots", []) if isinstance(packet.get("market_snapshots"), list) else [],
        )
        raw_path = self.save_raw_decision(decision, ctx_hash)
        self.log_decision(decision, ctx_hash, raw_path)
        self.log_decision_candidates(decision, ctx_hash)
        self.save_event_permissions_from_decision(decision)
        state = self.load_state()
        state["last_weekend_summary_utc"] = iso_utc()
        state["last_weekend_summary_raw_path"] = str(raw_path)
        self.save_state(state)
        self.mark_weekend_summary_done(state)

    def run_sunday_reopen_hard_protection(self, account: Optional[Dict[str, Any]] = None, open_trades: Optional[List[Dict[str, Any]]] = None) -> None:
        if not cfg_bool("FOREX_REOPEN_HARD_PROTECTION_ENABLED", default=True):
            return
        if not is_sunday_reopen_day():
            return
        mins = self.sunday_reopen_minutes_since_open()
        if mins is None or mins < 0:
            return
        if account is None:
            account = self.oanda.get_account_summary()
        if open_trades is None:
            open_trades = self.oanda.get_open_trades()
        if not open_trades:
            return
        state = self.load_state()
        mem = state.get("trade_profit_memory", {}) if isinstance(state.get("trade_profit_memory", {}), dict) else {}
        summary = account_summary_for_prompt(account)
        ref = state.get("latest_closed_market_reference") or {}
        ref_nav = safe_float(((ref.get("account") or {}) if isinstance(ref, dict) else {}).get("nav"), 0.0)
        nav_drop_pct = ((ref_nav - summary["nav"]) / ref_nav * 100.0) if ref_nav > 0 else 0.0
        min_prior = cfg_float("FOREX_REOPEN_MIN_PRIOR_PROFIT_USD", default=0.75)
        neg_threshold = cfg_float("FOREX_REOPEN_NEGATIVE_PL_THRESHOLD_USD", default=-0.05)
        giveback_usd = cfg_float("FOREX_REOPEN_CLOSE_IF_PROFIT_GIVEBACK_USD", default=1.00)
        close_if_profit_to_neg = cfg_bool("FOREX_REOPEN_CLOSE_IF_WAS_PROFIT_NOW_NEGATIVE", default=True)
        close_nav_drop_pct = cfg_float("FOREX_REOPEN_CLOSE_IF_NAV_DROP_PCT", default=6.0)
        log(f"Sunday reopen hard-protection check: minutes_since_open={mins} NAV={summary['nav']:.2f} ref_nav={ref_nav:.2f} nav_drop_pct={nav_drop_pct:.2f} open_trades={len(open_trades)}")
        for trade in list(open_trades):
            tid = str(trade.get("id", ""))
            inst = normalize_instrument(trade.get("instrument"))
            current_pl = safe_float(trade.get("unrealizedPL"), 0.0)
            rec = mem.get(tid, {}) if isinstance(mem.get(tid, {}), dict) else {}
            max_pl = safe_float(rec.get("max_unrealized_pl"), current_pl)
            giveback = max(0.0, max_pl - current_pl)
            action_base = {
                "instrument": inst,
                "trade_id": tid,
                "direction": trade_direction_from_units(trade.get("currentUnits")),
                "outlook_confidence": 0,
                "risk_pct": 0,
            }
            should_close = False
            why = []
            if close_if_profit_to_neg and max_pl >= min_prior and current_pl <= neg_threshold:
                should_close = True
                why.append(f"was +{max_pl:.2f} max unrealized, now {current_pl:.2f}")
            if giveback_usd > 0 and max_pl >= min_prior and giveback >= giveback_usd:
                should_close = True
                why.append(f"profit giveback {giveback:.2f} >= {giveback_usd:.2f}")
            if close_nav_drop_pct > 0 and nav_drop_pct >= close_nav_drop_pct and current_pl < 0:
                should_close = True
                why.append(f"NAV gap/drop {nav_drop_pct:.2f}% and trade negative")
            if should_close:
                akey = self.action_key("SUNDAY_REOPEN_CLOSE", action_base, trade_id=tid)
                if self.recently_attempted_action(akey):
                    self.log_action({**action_base, "action": "CLOSE"}, "reopen_protection", "skipped", reject_reason="duplicate recent reopen close suppressed")
                    continue
                status = self.close_trade(trade, {**action_base, "action": "CLOSE", "reason": "Sunday reopen hard protection: " + "; ".join(why)})
                if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                    self.remember_action(akey)
            else:
                self.log_action({**action_base, "action": "HOLD", "reason": f"Sunday reopen protection checked; current_pl={current_pl:.2f}, max_pl={max_pl:.2f}, giveback={giveback:.2f}"}, "reopen_protection", "logged")

    def coverage_bucket_for_instrument(self, instrument: Any) -> str:
        return instrument_coverage_bucket(
            instrument,
            major_pairs=self.cfg.major_usd_pairs,
            volatile_pairs=self.cfg.scout_volatile_pairs,
        )

    def snapshot_prefilter_score(self, snap: Dict[str, Any]) -> float:
        if not snap.get("tradeable", False):
            return -999999.0
        spread = safe_float(snap.get("spread_pips"), 999999.0)
        bars = snap.get("recent_bars") if isinstance(snap.get("recent_bars"), dict) else {}
        atr = safe_float(bars.get("atr_14_pips"), 0.0) if bars else 0.0
        changes = [
            abs(safe_float(bars.get(key), 0.0))
            for key in ("change_3_bars_pips", "change_12_bars_pips", "change_24_bars_pips")
            if bars and bars.get(key) is not None
        ]
        momentum = max(changes) if changes else 0.0
        if spread >= 9999.0:
            return -999999.0
        return round(momentum + 0.35 * atr - 1.5 * spread, 6)

    def snapshot_direction_hint(self, snap: Dict[str, Any]) -> str:
        bars = snap.get("recent_bars") if isinstance(snap.get("recent_bars"), dict) else {}
        change = safe_float(
            bars.get("change_12_bars_pips", bars.get("change_24_bars_pips", 0.0)),
            0.0,
        )
        if change > 0:
            return "LONG"
        if change < 0:
            return "SHORT"
        return "MIXED"

    def build_all68_bucket_shortlist(self, snapshots: List[Dict[str, Any]]) -> Dict[str, Any]:
        top_per_bucket = max(1, int(self.cfg.all68_bucket_top_per_bucket))
        bucket_rows: Dict[str, List[Dict[str, Any]]] = {}
        for snap in snapshots:
            inst = normalize_instrument(snap.get("instrument", ""))
            if not inst:
                continue
            bucket = self.coverage_bucket_for_instrument(inst)
            bars = snap.get("recent_bars") if isinstance(snap.get("recent_bars"), dict) else {}
            row = {
                "instrument": inst,
                "bucket": bucket,
                "score": self.snapshot_prefilter_score(snap),
                "direction_hint": self.snapshot_direction_hint(snap),
                "tradeable": bool(snap.get("tradeable", False)),
                "spread_pips": snap.get("spread_pips"),
                "atr_14_pips": bars.get("atr_14_pips") if bars else None,
                "change_3_bars_pips": bars.get("change_3_bars_pips") if bars else None,
                "change_12_bars_pips": bars.get("change_12_bars_pips") if bars else None,
                "change_24_bars_pips": bars.get("change_24_bars_pips") if bars else None,
            }
            bucket_rows.setdefault(bucket, []).append(row)

        buckets: Dict[str, Dict[str, Any]] = {}
        for bucket, rows in sorted(bucket_rows.items()):
            ranked = sorted(rows, key=lambda item: safe_float(item.get("score"), -999999.0), reverse=True)
            buckets[bucket] = {
                "available_count": len(rows),
                "review_minimum": min(max(0, int(self.cfg.all68_bucket_min_reviewed)), len(rows)),
                "top_prefiltered": ranked[:top_per_bucket],
            }
        return {
            "enabled": bool(self.cfg.all68_bucketed_discovery_enabled),
            "purpose": (
                "Local all-pair prefilter for GPT review coverage only. It does not "
                "create orders; final orders still come from GPT and broker/risk guards."
            ),
            "top_per_bucket": top_per_bucket,
            "require_real_review": bool(self.cfg.all68_bucket_require_real_review),
            "buckets": buckets,
        }

    def decision_pair_bucket_coverage(
        self,
        decision: Dict[str, Any],
        snapshots: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        available: Dict[str, int] = {}
        for snap in snapshots:
            bucket = self.coverage_bucket_for_instrument(snap.get("instrument", ""))
            available[bucket] = available.get(bucket, 0) + 1

        section_counts: Dict[str, Dict[str, int]] = {}
        touched_by_bucket: Dict[str, set[str]] = {}
        for section in ("new_trade_candidates", "orders_to_execute", "open_position_actions"):
            items = decision.get(section, []) or []
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                inst = normalize_instrument(item.get("instrument", ""))
                if not inst:
                    continue
                bucket = self.coverage_bucket_for_instrument(inst)
                item.setdefault("coverage_bucket", bucket)
                counts = section_counts.setdefault(section, {})
                counts[bucket] = counts.get(bucket, 0) + 1
                touched_by_bucket.setdefault(bucket, set()).add(inst)

        reviews = decision.get("pair_bucket_coverage_review", []) or []
        if isinstance(reviews, list):
            for review in reviews:
                if not isinstance(review, dict):
                    continue
                raw_bucket = str(review.get("bucket") or "").strip()
                reviewed = review.get("reviewed_instruments")
                reviewed_instruments = [
                    normalize_instrument(inst)
                    for inst in (reviewed if isinstance(reviewed, list) else [])
                    if normalize_instrument(inst)
                ]
                best_inst = normalize_instrument(review.get("best_instrument", ""))
                if best_inst and best_inst not in reviewed_instruments:
                    reviewed_instruments.append(best_inst)
                bucket = raw_bucket or (self.coverage_bucket_for_instrument(best_inst) if best_inst else "")
                if not bucket:
                    continue
                counts = section_counts.setdefault("pair_bucket_coverage_review", {})
                counts[bucket] = counts.get(bucket, 0) + max(1, len(reviewed_instruments))
                touched = touched_by_bucket.setdefault(bucket, set())
                if reviewed_instruments:
                    touched.update(reviewed_instruments)
                else:
                    touched.add(f"{bucket}:reviewed")

        candidate_counts = section_counts.get("new_trade_candidates", {})
        order_counts = section_counts.get("orders_to_execute", {})
        review_counts = section_counts.get("pair_bucket_coverage_review", {})
        ignored_buckets = [
            bucket for bucket, count in sorted(available.items())
            if (
                count > 0
                and candidate_counts.get(bucket, 0) == 0
                and order_counts.get(bucket, 0) == 0
                and review_counts.get(bucket, 0) == 0
            )
        ]
        review_minimum = max(0, int(self.cfg.all68_bucket_min_reviewed))
        under_reviewed = [
            bucket for bucket, count in sorted(available.items())
            if count > 0 and len(touched_by_bucket.get(bucket, set())) < min(review_minimum, count)
        ]
        return {
            "enabled": bool(self.cfg.all68_bucketed_discovery_enabled),
            "available_bucket_counts": available,
            "section_bucket_counts": section_counts,
            "touched_instruments_by_bucket": {
                bucket: sorted(values) for bucket, values in sorted(touched_by_bucket.items())
            },
            "ignored_buckets": ignored_buckets,
            "under_reviewed_buckets": under_reviewed,
            "candidate_usd_rows": sum(
                count for bucket, count in candidate_counts.items()
                if str(bucket).startswith("usd_")
            ),
            "candidate_non_usd_cross_rows": sum(
                count for bucket, count in candidate_counts.items()
                if not str(bucket).startswith("usd_")
            ),
        }

    def annotate_pair_bucket_coverage(
        self,
        decision: Dict[str, Any],
        snapshots: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        audit = self.decision_pair_bucket_coverage(decision, snapshots)
        decision["pair_bucket_coverage"] = audit
        return audit

    def build_market_packet(self) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        if not self.instruments:
            self.refresh_instruments()
        account = self.oanda.get_account_summary()
        account_prompt = account_summary_for_prompt(account)
        open_trades = self.oanda.get_open_trades()
        open_trade_instruments = {normalize_instrument(t.get("instrument")) for t in open_trades if normalize_instrument(t.get("instrument"))}
        price_instruments = sorted(set(self.instruments) | open_trade_instruments)
        prices = self.oanda.get_prices(price_instruments)

        snapshots: List[Dict[str, Any]] = []
        candle_limit = self.cfg.max_candle_instruments if self.cfg.max_candle_instruments > 0 else len(self.instruments)
        candle_set = set(self.instruments[:candle_limit])

        for inst in self.instruments:
            meta = self.instrument_meta[inst]
            price = prices.get(inst, {})
            bid, ask = bid_ask(price)
            mid = mid_price(price)
            snap: Dict[str, Any] = {
                "instrument": inst,
                "coverage_bucket": self.coverage_bucket_for_instrument(inst),
                "display_name": meta.display_name,
                "base_currency": split_instrument(inst)[0],
                "quote_currency": split_instrument(inst)[1],
                "tradeable": price_tradeable(price),
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_pips": spread_pips(price, meta) if price else None,
                "pip_size": meta.pip_size,
                "margin_rate": meta.margin_rate,
            }
            if self.cfg.include_candles and inst in candle_set:
                try:
                    candles = self.oanda.get_candles(inst, self.cfg.candle_granularity, self.cfg.candle_count)
                    snap["recent_bars"] = candle_snapshot(candles_to_rows(candles), meta)
                except Exception as exc:
                    snap["recent_bars"] = {"available": False, "error": str(exc)[:200]}
            snapshots.append(snap)

        shutdown_active = self.shutdown_mode_active()
        recent_daily_recaps = self.load_recent_daily_recaps()
        state_for_memory = self.load_state()
        trade_profit_memory = state_for_memory.get("trade_profit_memory", {}) if isinstance(state_for_memory.get("trade_profit_memory", {}), dict) else {}
        weekend_reference = state_for_memory.get("latest_closed_market_reference", {}) if isinstance(state_for_memory.get("latest_closed_market_reference", {}), dict) else {}
        non_usd_pair_count = sum(1 for inst in self.instruments if instrument_is_cross_pair(inst))
        usd_pair_count = max(0, len(self.instruments) - non_usd_pair_count)
        bucket_shortlist = self.build_all68_bucket_shortlist(snapshots)
        packet = {
            "as_of_utc": iso_utc(),
            "as_of_ny": iso_ny(),
            "account_lane": self.cfg.account_lane,
            "account_name": self.cfg.account_display_name,
            "instrument_filter_mode": self.cfg.instrument_filter_mode,
            "major_usd_pairs": self.cfg.major_usd_pairs,
            "gpt_reliable_pairs": self.cfg.gpt_reliable_pairs,
            "technical_scout_pairs": self.cfg.technical_scout_pairs,
            "instructions": {
                "management_cycle": "Manage current open trades first, then rank new opens across all supplied pairs in this account lane only.",
                "account_route": f"This scan is for account lane {self.cfg.account_lane} ({self.cfg.account_display_name}); new OPEN/FLIP orders must stay inside instrument_filter_mode={self.cfg.instrument_filter_mode}.",
                "confidence_source": "outlook_confidence and risk_pct must come from macro/news/outlook judgement, not hard-coded rules.",
                "risk_policy": "You request risk_pct; script caps risk/margin/exposure using config guardrails.",
                "shutdown_mode": (
                    "ACTIVE: Do not open new positions. Fade/drain current positions by tightening stops, taking partial profits, and closing positions when risk/reward no longer justifies holding."
                    if shutdown_active else
                    "off"
                ),
                "cross_pair_policy": "Explicitly compare USD pairs against non-USD crosses. Pair type is not a penalty. Use bucketed discovery to avoid USD tunnel vision, then use global-best selection for execution with no forced trade quota.",
                "all68_bucketed_discovery": "Review each populated bucket in all68_bucket_shortlist. Return real OPEN/WATCH/REJECT candidates from the strongest bucket members or explain a concrete bucket-level blocker in pair_bucket_coverage_review. If a reviewed member is viable, include it in new_trade_candidates and potentially orders_to_execute; review-only coverage is not enough.",
                "daily_memory": "Use recent_daily_recaps as historical context. Do not blindly repeat losing same-day theses; update the thesis if the recap shows drift, drawdown, or repeated rejected opportunities.",
            },
            "account": account_prompt,
            "config_guardrails": {
                "max_open_trades": self.cfg.max_open_trades,
                "max_new_trades_per_scan": self.cfg.max_new_trades_per_scan,
                "max_total_new_risk_pct_per_scan": self.cfg.max_total_new_risk_pct_per_scan,
                "max_risk_pct_per_trade": self.cfg.max_risk_pct_per_trade,
                "max_margin_used_pct": self.cfg.max_margin_used_pct,
                "target_margin_used_pct": self.cfg.target_margin_used_pct,
                "margin_deployment_note": "Paper account should be fairly aggressive, but active target is regime-dependent. If margin is below the active band, deploy with OPEN/SCALE_IN when justified or give a concrete underdeployment_reason.",
                "deployment_mode_bands_pct": {
                    "DEFENSIVE": [10, 25],
                    "LIGHT": [20, 35],
                    "NORMAL": [35, 55],
                    "AGGRESSIVE": [50, 75],
                    "EVENT": [65, 85],
                },
                "scale_in_policy": "orders_to_execute may use SCALE_IN for an existing same-direction winner only; script blocks averaging down and still enforces stop/margin/spread caps.",
                "require_stop_loss_on_open": self.cfg.require_stop_loss_on_open,
                "shutdown_mode": self.cfg.shutdown_mode,
                "shutdown_block_new_trades": self.cfg.shutdown_block_new_trades,
                "selection_mode": "bucketed_discovery_then_global_best_execution",
                "category_quota_policy": "coverage is required in analysis; do not reserve execution slots or risk for any bucket",
                "pair_type_neutrality": "Rank all supplied instruments by viability. USD pair, non-USD cross, minor, or exotic status is not a reason to demote a candidate when expected_R, spread/stop geometry, event risk, and portfolio fit are viable.",
                "event_scanner": {
                    "enabled": self.cfg.event_scanner_enabled,
                    "local_scan_interval_seconds": self.cfg.event_scan_interval_seconds,
                    "windows_minutes": self.cfg.event_windows_minutes,
                    "scout_trades_enabled": self.cfg.event_scout_trades_enabled,
                    "scout_risk_pct_default": self.cfg.event_scout_risk_pct,
                    "permission_note": "If event_permissions are returned, local scanner may use them for small scout trades before the next scheduled GPT call when basket confirmation appears.",
                },
                "cross_pair_review_required": bool(self.cfg.require_cross_pair_coverage),
                "min_non_usd_candidates": 0,
                "bucketed_discovery_enabled": bool(self.cfg.all68_bucketed_discovery_enabled),
                "bucket_review_minimum": max(0, int(self.cfg.all68_bucket_min_reviewed)),
                "correlated_thesis_cap": {
                    "enabled": cfg_bool("FOREX_GPT_CORRELATED_THESIS_CAP_ENABLED", default=False),
                    "focus_currencies": cfg_list("FOREX_GPT_CORRELATED_THESIS_FOCUS_CURRENCIES", default=[]),
                    "max_risk_pct_per_directional_currency_thesis": cfg_float("FOREX_GPT_MAX_CORRELATED_THESIS_RISK_PCT", default=6.0),
                    "note": "Applies to each currency-direction key, e.g. EUR_LONG, USD_SHORT, JPY_LONG.",
                },
                "available_usd_pair_count": usd_pair_count,
                "available_non_usd_cross_pair_count": non_usd_pair_count,
                "account_lane": self.cfg.account_lane,
                "instrument_filter_mode": self.cfg.instrument_filter_mode,
                "route_policy": "GPT/advisor account: evaluate all supplied OANDA pairs and open only when the news/macro/advisor thesis is strong. This split is strategy-based, not USD-vs-non-USD.",
            },
            "recent_daily_recaps": recent_daily_recaps,
            "trade_profit_memory": trade_profit_memory,
            "weekend_closed_market_reference": weekend_reference,
            "currency_exposure_units": build_currency_exposure(open_trades),
            "open_trades": [summarize_trade_for_prompt(t) for t in open_trades],
            "all68_bucket_shortlist": bucket_shortlist,
            "market_snapshots": snapshots,
            "output_contract": {
                "orders_to_execute": "Only include actions you want the script to attempt now, ordered by final global attractiveness across all supplied pairs, not by category.",
                "new_trade_candidates": "Include real OPEN/WATCH/REJECT rankings for the best opportunities after comparing all supplied pairs and all populated buckets. Include viable non-USD/minor/exotic candidates in the same global ranking when they clear expectancy and execution checks. Do not pad with placeholder candidates.",
                "pair_bucket_coverage_review": "Mandatory: for each populated bucket in all68_bucket_shortlist, name the best reviewed instrument(s), final action OPEN/WATCH/REJECT, and concrete reason. Coverage review is not an execution quota, and it is not a substitute for putting a viable reviewed instrument in new_trade_candidates.",
                "open_position_actions": "Mandatory: include exactly one HOLD/TIGHTEN/PARTIAL_CLOSE/CLOSE/FLIP action per open trade. Use HOLD if no change.",
                "currency_strength_table": "Mandatory: per-currency table with bias, evidence, confidence, news_risk, and technical_alignment.",
                "macro_thesis_consistency": "Mandatory: explicitly confirm portfolio_bias, market_summary, currency_strength_table, orders_to_execute, and event_permissions agree; list contradictions if any.",
            },
        }
        raw_context = {"account_raw": account, "prices_raw": prices, "open_trades_raw": open_trades}
        return packet, prices, open_trades, raw_context


    def normalize_decision(self, decision: Dict[str, Any], open_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Normalize GPT output so execution sees OANDA instruments and every open trade has a logged action.

        This does not create broker-changing actions. If GPT omits an open trade, the script injects a HOLD
        with a clear reason so the scan log shows the position was not acted on rather than silently ignored.
        """
        if not isinstance(decision, dict):
            decision = {}
        for key in ("open_position_actions", "new_trade_candidates", "orders_to_execute", "pair_bucket_coverage_review"):
            val = decision.get(key)
            if not isinstance(val, list):
                decision[key] = []
        for key in ("open_position_actions", "new_trade_candidates", "orders_to_execute"):
            for obj in decision.get(key, []) or []:
                if isinstance(obj, dict) and obj.get("instrument"):
                    obj["instrument"] = normalize_instrument(obj.get("instrument"))

        if False and self.cfg.require_cross_pair_coverage and self.instrument_meta:
            candidates = [c for c in decision.get("new_trade_candidates", []) or [] if isinstance(c, dict)]
            seen = {normalize_instrument(c.get("instrument")) for c in candidates if c.get("instrument")}
            cross_candidates = [c for c in candidates if instrument_is_cross_pair(c.get("instrument"))]
            needed = max(0, min(int(self.cfg.min_non_usd_candidates), sum(1 for i in self.instruments if instrument_is_cross_pair(i))) - len(cross_candidates))
            if needed > 0:
                added = 0
                next_rank = max([safe_float(c.get("rank"), 0.0) for c in candidates] + [0.0]) + 1
                open_insts = {normalize_instrument(t.get("instrument")) for t in open_trades}
                for inst in self.instruments:
                    if added >= needed:
                        break
                    if not instrument_is_cross_pair(inst) or inst in seen or inst in open_insts:
                        continue
                    candidates.append({
                        "rank": next_rank + added,
                        "instrument": inst,
                        "action": "REJECT",
                        "direction": "NONE",
                        "outlook_confidence": 0,
                        "risk_pct": 0,
                        "margin_intensity": "none",
                        "expected_hold_hours": 0,
                        "entry_min": None,
                        "entry_max": None,
                        "stop_loss": None,
                        "take_profit": None,
                        "tp1": None,
                        "tp2": None,
                        "trailing_stop_pips": None,
                        "why_now": "v5.9 cross-pair audit placeholder: GPT omitted this non-USD cross from candidates.",
                        "reason": "Included so cross-pair coverage can be audited; not executable.",
                        "what_would_change_my_mind": "Future GPT scan should evaluate this pair explicitly.",
                    })
                    seen.add(inst)
                    added += 1
                if added:
                    decision["new_trade_candidates"] = candidates
                    notes = decision.setdefault("risk_notes", [])
                    if isinstance(notes, list):
                        notes.append(f"v5.9 added {added} REJECT placeholder(s) for omitted non-USD cross-pair audit coverage.")

        if self.cfg.require_position_action_per_open_trade and open_trades:
            actions = [a for a in decision.get("open_position_actions", []) or [] if isinstance(a, dict)]
            trade_by_id = {str(t.get("id", "")).strip(): t for t in open_trades if str(t.get("id", "")).strip()}
            trade_by_inst = {normalize_instrument(t.get("instrument")): t for t in open_trades if normalize_instrument(t.get("instrument"))}

            # If GPT provided an existing-position action, force the symbol/direction to match
            # the broker's current open trade. This prevents log/action contradictions such as
            # a broker LONG being logged as HOLD SHORT because the model guessed direction wrong.
            cleaned_actions: List[Dict[str, Any]] = []
            seen_ids: set = set()
            seen_insts: set = set()
            corrected = 0
            for a in actions:
                tid = str(a.get("trade_id") or "").strip()
                inst = normalize_instrument(a.get("instrument"))
                trade = trade_by_id.get(tid) if tid else None
                if trade is None and inst:
                    trade = trade_by_inst.get(inst)
                if trade is not None:
                    real_tid = str(trade.get("id", "")).strip()
                    real_inst = normalize_instrument(trade.get("instrument"))
                    real_dir = trade_direction_from_units(trade.get("currentUnits"))
                    if a.get("instrument") != real_inst or str(a.get("direction", "")).upper() != real_dir:
                        corrected += 1
                    a["trade_id"] = real_tid
                    a["instrument"] = real_inst
                    a["direction"] = real_dir
                    if real_tid in seen_ids or real_inst in seen_insts:
                        continue
                    seen_ids.add(real_tid)
                    seen_insts.add(real_inst)
                cleaned_actions.append(a)
            actions = cleaned_actions

            covered_ids = {str(a.get("trade_id") or "").strip() for a in actions if str(a.get("trade_id") or "").strip()}
            covered_insts = {normalize_instrument(a.get("instrument")) for a in actions if a.get("instrument")}
            injected = 0
            for t in open_trades:
                tid = str(t.get("id", "")).strip()
                inst = normalize_instrument(t.get("instrument"))
                if (tid and tid in covered_ids) or (inst and inst in covered_insts):
                    continue
                actions.append({
                    "trade_id": tid,
                    "instrument": inst,
                    "action": "HOLD",
                    "direction": trade_direction_from_units(t.get("currentUnits")),
                    "outlook_confidence": 0,
                    "risk_pct": 0,
                    "partial_close_pct": 0,
                    "stop_loss": None,
                    "take_profit": None,
                    "trailing_stop_pips": None,
                    "reason": "GPT omitted this open trade; v5.6 injected HOLD so the position is logged and not silently ignored.",
                    "what_would_change_my_mind": "Next GPT management cycle should explicitly evaluate this trade.",
                })
                injected += 1
            if injected or corrected:
                decision["open_position_actions"] = actions
                notes = decision.setdefault("risk_notes", [])
                if isinstance(notes, list):
                    if injected:
                        notes.append(f"v5.6 injected HOLD for {injected} open trade(s) missing from GPT open_position_actions.")
                    if corrected:
                        notes.append(f"v5.6 corrected {corrected} open-trade action symbol/direction field(s) to match broker state.")
                if injected:
                    log(f"{self.lane_prefix()}[WARN] GPT omitted {injected} open trade action(s); injected HOLD fallback(s).")
                if corrected:
                    log(f"{self.lane_prefix()}[WARN] Corrected {corrected} GPT open-trade action field(s) to match broker state.")
        return decision

    def run_gpt_scan(self, reason: str = "scheduled") -> None:
        if fx_market_closed() and reason != "manual":
            log("FX market appears closed; skipping scheduled GPT scan.")
            return
        if self.shutdown_mode_active() and self.cfg.shutdown_skip_gpt:
            log(f"Shutdown {self.shutdown_mode_label()} active; replacing GPT scan with local shutdown fade pass: {reason}")
            self.run_shutdown_fade_pass(reason=f"gpt_scan_replaced:{reason}", force=True)
            state = self.load_state()
            state["last_gpt_scan_utc"] = iso_utc()
            self.save_state(state)
            return
        log(f"{self.lane_prefix()}Starting GPT portfolio-management scan: {reason}")
        packet, prices, open_trades, raw_context = self.build_market_packet()
        reason_l = str(reason or "").lower()
        no_new = self.sunday_reopen_new_trades_blocked()
        packet["scan_context"] = {
            "reason": reason,
            "market_closed": fx_market_closed(),
            "sunday_reopen_new_trades_blocked": no_new,
            "minutes_since_sunday_open": self.sunday_reopen_minutes_since_open(),
        }
        if reason_l.startswith("sunday_reopen"):
            packet.setdefault("instructions", {})["sunday_reopen"] = (
                "This is the dedicated Sunday reopen management scan. Manage existing positions first. "
                "Closes, partial closes, tightening, and FLIP/reversal actions are allowed if the reopen move proves the existing book wrong. "
                "Only unrelated fresh portfolio-expansion opens are blocked during the short reopen warmup. Use FLIP in open_position_actions for defensive reversals."
            )
        ctx_hash = stable_hash(packet)
        decision = self.openai.create_decision(packet)
        if no_new:
            cleaned_orders = []
            for o in decision.get("orders_to_execute", []) or []:
                if str(o.get("action", "")).upper().strip() == "OPEN":
                    o["action"] = "WATCH"
                    o["reason"] = "Sunday reopen warmup blocks unrelated fresh opens; converted from OPEN to WATCH. Use FLIP in open_position_actions for defensive reversals. " + str(o.get("reason", ""))
                    decision.setdefault("new_trade_candidates", []).append(o)
                else:
                    cleaned_orders.append(o)
            decision["orders_to_execute"] = cleaned_orders
        decision = self.normalize_decision(decision, open_trades)
        raw_path = self.save_raw_decision(decision, ctx_hash)
        self.log_decision(decision, ctx_hash, raw_path)
        self.log_decision_candidates(decision, ctx_hash)
        self.execute_decision(decision, prices, open_trades)
        self.write_daily_recaps_around_now()
        self.save_event_permissions_from_decision(decision)
        state = self.load_state()
        state["last_gpt_scan_utc"] = iso_utc()
        self.save_state(state)

    def save_raw_decision(self, decision: Dict[str, Any], ctx_hash: str) -> Path:
        path = self.cfg.raw_dir / f"decision_{ny_now().strftime('%Y%m%d_%H%M%S')}_{ctx_hash}.json"
        write_json(path, decision)
        return path

    def log_decision(self, decision: Dict[str, Any], ctx_hash: str, raw_path: Path) -> None:
        action_count = len(decision.get("orders_to_execute", []) or []) + len(decision.get("open_position_actions", []) or [])
        append_csv(self.cfg.decisions_csv, {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "kind": "gpt_scan",
            "market_summary": str(decision.get("market_summary", ""))[:1000],
            "portfolio_bias": str(decision.get("portfolio_bias", ""))[:500],
            "action_count": action_count,
            "context_hash": ctx_hash,
            "raw_path": str(raw_path),
            "raw_json": json.dumps(decision, default=str)[:12000],
        }, DECISION_FIELDS)
        try:
            self.full_logger.log_decision_audit(ctx_hash, "portfolio_scan", decision, str(raw_path))
        except Exception:
            pass
        log(f"{self.lane_prefix()}GPT scan complete. action_count={action_count} summary={str(decision.get('market_summary', ''))[:180]}")

    def log_decision_candidates(self, decision: Dict[str, Any], ctx_hash: str) -> None:
        """Write every GPT candidate/action to candidates.csv so USD-vs-cross coverage is auditable."""
        sections = ("new_trade_candidates", "orders_to_execute", "open_position_actions")
        for section in sections:
            items = decision.get(section, []) or []
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                inst = normalize_instrument(item.get("instrument", ""))
                if not inst:
                    continue
                append_csv(self.cfg.candidates_csv, {
                    "time_utc": iso_utc(),
                    "time_ny": iso_ny(),
                    "section": section,
                    "rank": item.get("rank", ""),
                    "instrument": inst,
                    "direction": str(item.get("direction", "")),
                    "action": str(item.get("action", "")),
                    "has_usd": instrument_has_usd(inst),
                    "is_cross_pair": instrument_is_cross_pair(inst),
                    "coverage_bucket": item.get("coverage_bucket") or self.coverage_bucket_for_instrument(inst),
                    "outlook_confidence": item.get("outlook_confidence", ""),
                    "risk_pct": item.get("risk_pct", ""),
                    "margin_intensity": item.get("margin_intensity", ""),
                    "expected_hold_hours": item.get("expected_hold_hours", ""),
                    "reason": str(item.get("reason", ""))[:1000],
                    "why_now": str(item.get("why_now", ""))[:1000],
                    "raw_json": json.dumps({"context_hash": ctx_hash, "item": item}, default=str)[:12000],
                }, CANDIDATE_FIELDS)

        reviews = decision.get("pair_bucket_coverage_review", []) or []
        if isinstance(reviews, list):
            for review in reviews:
                if not isinstance(review, dict):
                    continue
                raw_bucket = str(review.get("bucket") or "").strip()
                reviewed = review.get("reviewed_instruments")
                instruments = [
                    normalize_instrument(inst)
                    for inst in (reviewed if isinstance(reviewed, list) else [])
                    if normalize_instrument(inst)
                ]
                best_inst = normalize_instrument(review.get("best_instrument", ""))
                if best_inst and best_inst not in instruments:
                    instruments.insert(0, best_inst)
                for inst in instruments:
                    bucket = raw_bucket or self.coverage_bucket_for_instrument(inst)
                    append_csv(self.cfg.candidates_csv, {
                        "time_utc": iso_utc(),
                        "time_ny": iso_ny(),
                        "section": "pair_bucket_coverage_review",
                        "rank": "",
                        "instrument": inst,
                        "direction": "",
                        "action": str(review.get("action", "")),
                        "has_usd": instrument_has_usd(inst),
                        "is_cross_pair": instrument_is_cross_pair(inst),
                        "coverage_bucket": bucket,
                        "outlook_confidence": "",
                        "risk_pct": "",
                        "margin_intensity": "",
                        "expected_hold_hours": "",
                        "reason": str(review.get("reason", ""))[:1000],
                        "why_now": "bucketed all-pair coverage review",
                        "raw_json": json.dumps({"context_hash": ctx_hash, "review": review}, default=str)[:12000],
                    }, CANDIDATE_FIELDS)

    def read_csv_dicts(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                return list(csv.DictReader(f))
        except Exception:
            return []

    def ny_date_from_row(self, row: Dict[str, Any]) -> str:
        raw = str(row.get("time_ny") or row.get("time_utc") or "")[:10]
        return raw if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw) else ny_now().strftime("%Y-%m-%d")

    def rows_for_ny_date(self, path: Path, date_key: str) -> List[Dict[str, Any]]:
        return [r for r in self.read_csv_dicts(path) if self.ny_date_from_row(r) == date_key]

    def build_daily_recap(self, date_key: str) -> Dict[str, Any]:
        decisions = self.rows_for_ny_date(self.cfg.decisions_csv, date_key)
        actions = self.rows_for_ny_date(self.cfg.actions_csv, date_key)
        monitors = self.rows_for_ny_date(self.cfg.monitor_csv, date_key)
        candidates = self.rows_for_ny_date(self.cfg.candidates_csv, date_key)

        def fnum(x: Any, default: float = 0.0) -> float:
            try:
                if x in ("", None):
                    return default
                return float(x)
            except Exception:
                return default

        navs = [fnum(r.get("nav"), math.nan) for r in monitors]
        navs = [x for x in navs if math.isfinite(x)]
        margin_pcts = [fnum(r.get("margin_used_pct"), math.nan) for r in monitors]
        margin_pcts = [x for x in margin_pcts if math.isfinite(x)]

        action_counts: Dict[str, int] = {}
        accepted_opens: List[Dict[str, Any]] = []
        skipped_opens: List[Dict[str, Any]] = []
        touched_instruments: Dict[str, int] = {}
        for r in actions:
            key = f"{r.get('action_type','')}:{r.get('status','')}"
            action_counts[key] = action_counts.get(key, 0) + 1
            inst = normalize_instrument(r.get("instrument", ""))
            if inst:
                touched_instruments[inst] = touched_instruments.get(inst, 0) + 1
            if r.get("action_type") == "open" and str(r.get("status", "")).startswith("accepted"):
                accepted_opens.append({"instrument": inst, "direction": r.get("direction"), "risk_pct": r.get("risk_pct"), "margin_usd": r.get("margin_usd")})
            if r.get("action_type") == "open" and r.get("status") == "skipped":
                skipped_opens.append({"instrument": inst, "direction": r.get("direction"), "reject_reason": r.get("reject_reason")})

        usd_candidates = 0
        cross_candidates = 0
        executed_usd = 0
        executed_cross = 0
        candidate_counts: Dict[str, int] = {}
        bucket_candidate_counts: Dict[str, int] = {}
        bucket_order_counts: Dict[str, int] = {}
        bucket_review_counts: Dict[str, int] = {}
        top_candidates: List[Dict[str, Any]] = []
        for r in candidates:
            inst = normalize_instrument(r.get("instrument", ""))
            if not inst:
                continue
            candidate_counts[inst] = candidate_counts.get(inst, 0) + 1
            bucket = str(r.get("coverage_bucket") or self.coverage_bucket_for_instrument(inst))
            is_cross = str(r.get("is_cross_pair", "")).lower() == "true" or instrument_is_cross_pair(inst)
            has_usd = str(r.get("has_usd", "")).lower() == "true" or instrument_has_usd(inst)
            if is_cross:
                cross_candidates += 1
            if has_usd:
                usd_candidates += 1
            if r.get("section") == "new_trade_candidates":
                bucket_candidate_counts[bucket] = bucket_candidate_counts.get(bucket, 0) + 1
            if r.get("section") == "pair_bucket_coverage_review":
                bucket_review_counts[bucket] = bucket_review_counts.get(bucket, 0) + 1
            if r.get("section") == "orders_to_execute":
                bucket_order_counts[bucket] = bucket_order_counts.get(bucket, 0) + 1
                if is_cross:
                    executed_cross += 1
                if has_usd:
                    executed_usd += 1
            if r.get("section") == "new_trade_candidates" and len(top_candidates) < 20:
                top_candidates.append({
                    "rank": r.get("rank"),
                    "instrument": inst,
                    "direction": r.get("direction"),
                    "action": r.get("action"),
                    "confidence": r.get("outlook_confidence"),
                    "reason": str(r.get("reason", ""))[:220],
                })

        summaries = [str(r.get("market_summary", "")) for r in decisions if r.get("market_summary")]
        recap = {
            "date_ny": date_key,
            "generated_at_utc": iso_utc(),
            "scan_count": len(decisions),
            "monitor_count": len(monitors),
            "nav_start": navs[0] if navs else None,
            "nav_end": navs[-1] if navs else None,
            "nav_change": (navs[-1] - navs[0]) if len(navs) >= 2 else None,
            "margin_used_pct_min": min(margin_pcts) if margin_pcts else None,
            "margin_used_pct_max": max(margin_pcts) if margin_pcts else None,
            "action_counts": action_counts,
            "accepted_opens": accepted_opens[-25:],
            "skipped_opens": skipped_opens[-25:],
            "touched_instruments_top": sorted(touched_instruments.items(), key=lambda kv: kv[1], reverse=True)[:25],
            "candidate_coverage": {
                "usd_candidate_rows": usd_candidates,
                "non_usd_cross_candidate_rows": cross_candidates,
                "orders_to_execute_usd_rows": executed_usd,
                "orders_to_execute_non_usd_cross_rows": executed_cross,
                "unique_candidate_instruments": sorted(candidate_counts.keys()),
                "new_trade_candidate_rows_by_bucket": bucket_candidate_counts,
                "orders_to_execute_rows_by_bucket": bucket_order_counts,
                "bucket_coverage_review_rows_by_bucket": bucket_review_counts,
            },
            "top_candidates_sample": top_candidates[:15],
            "market_summary_sample": summaries[-6:],
        }
        return recap

    def write_daily_recap(self, date_key: str) -> Optional[Path]:
        if not self.cfg.daily_recap_enabled:
            return None
        self.cfg.daily_recap_dir.mkdir(parents=True, exist_ok=True)
        recap = self.build_daily_recap(date_key)
        path = self.cfg.daily_recap_dir / f"{date_key}.json"
        write_json(path, recap)

        md = self.cfg.daily_recap_dir / f"{date_key}.md"
        lines = [
            f"# Forex bot daily recap — {date_key}",
            "",
            f"- Scans: {recap.get('scan_count')} | Monitors: {recap.get('monitor_count')}",
            f"- NAV: {recap.get('nav_start')} -> {recap.get('nav_end')} | Change: {recap.get('nav_change')}",
            f"- Margin used pct min/max: {recap.get('margin_used_pct_min')} / {recap.get('margin_used_pct_max')}",
            f"- Candidate coverage: {json.dumps(recap.get('candidate_coverage', {}), default=str)}",
            f"- Accepted opens: {json.dumps(recap.get('accepted_opens', []), default=str)[:2000]}",
            "",
            "## Recent market summaries",
        ]
        for s in recap.get("market_summary_sample", []):
            lines.append(f"- {str(s)[:500]}")
        md.write_text("\n".join(lines) + "\n", encoding="utf-8")

        # Append/update JSONL index by rewriting recent unique dates.
        existing: Dict[str, Dict[str, Any]] = {}
        if self.cfg.daily_recap_index.exists():
            try:
                for line in self.cfg.daily_recap_index.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        obj = json.loads(line)
                        if isinstance(obj, dict) and obj.get("date_ny"):
                            existing[str(obj["date_ny"])] = obj
            except Exception:
                existing = {}
        existing[date_key] = recap
        keys = sorted(existing.keys())[-max(1, int(self.cfg.daily_recap_lookback_days)):]
        self.cfg.daily_recap_index.parent.mkdir(parents=True, exist_ok=True)
        self.cfg.daily_recap_index.write_text("\n".join(json.dumps(existing[k], default=str) for k in keys) + "\n", encoding="utf-8")
        return path

    def write_daily_recaps_around_now(self) -> None:
        if not self.cfg.daily_recap_enabled:
            return
        today = ny_now().date()
        dates = [today, today - dt.timedelta(days=1)]
        for d in dates:
            try:
                self.write_daily_recap(d.strftime("%Y-%m-%d"))
            except Exception as exc:
                self.log_error(f"write daily recap {d}", exc)

    def load_recent_daily_recaps(self) -> List[Dict[str, Any]]:
        if not self.cfg.daily_recap_enabled or self.cfg.daily_recap_recent_days_for_prompt <= 0:
            return []
        out: List[Dict[str, Any]] = []
        if self.cfg.daily_recap_index.exists():
            try:
                for line in self.cfg.daily_recap_index.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            out.append(obj)
            except Exception:
                out = []
        if not out and self.cfg.daily_recap_dir.exists():
            for p in sorted(self.cfg.daily_recap_dir.glob("*.json")):
                try:
                    obj = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(obj, dict):
                        out.append(obj)
                except Exception:
                    pass
        return out[-max(1, int(self.cfg.daily_recap_recent_days_for_prompt)):]

    def currency_direction_keys(self, instrument: str, direction: str) -> List[str]:
        """Return directional currency thesis keys for an order/trade.

        Example: EUR_USD SHORT is ["EUR_SHORT", "USD_LONG"]. This lets the
        manager cap correlated thesis risk, not just duplicate instruments.
        """
        inst = normalize_instrument(instrument)
        if not inst or "_" not in inst:
            return []
        base, quote = split_instrument(inst)
        d = str(direction).upper().strip()
        if d == "LONG":
            return [f"{base}_LONG", f"{quote}_SHORT"]
        if d == "SHORT":
            return [f"{base}_SHORT", f"{quote}_LONG"]
        return []

    def trade_stop_loss_price(self, trade: Dict[str, Any]) -> Optional[float]:
        sl = trade.get("stopLossOrder")
        if isinstance(sl, dict):
            price = as_optional_float(sl.get("price"))
            if price and price > 0:
                return price
        return None

    def estimate_trade_risk_pct(
        self,
        trade: Dict[str, Any],
        prices: Dict[str, Dict[str, Any]],
        nav: float,
    ) -> float:
        """Estimate current stop-defined trade risk as pct of NAV.

        If a stop is missing/unusable, return 0 rather than blocking unrelated
        new trades with a guessed value. Stop-loss is required for new opens, so
        this primarily covers currently managed positions.
        """
        if nav <= 0:
            return 0.0
        inst = normalize_instrument(trade.get("instrument", ""))
        meta = self.instrument_meta.get(inst)
        if not inst or meta is None:
            return 0.0
        entry = trade_entry_price(trade)
        stop = self.trade_stop_loss_price(trade)
        units = abs(safe_float(trade.get("currentUnits", trade.get("initialUnits", 0.0)), 0.0))
        if entry is None or stop is None or units <= 0:
            return 0.0
        quote_rate = quote_to_account_rate(inst, prices, self.cfg.account_currency)
        if quote_rate is None:
            return 0.0
        stop_pips = abs(entry - stop) / max(meta.pip_size, 1e-12)
        pip_value_per_unit = meta.pip_size * quote_rate
        risk_account = units * stop_pips * pip_value_per_unit
        return max(0.0, risk_account / nav * 100.0)

    def correlated_thesis_risk_book(
        self,
        open_trades: List[Dict[str, Any]],
        prices: Dict[str, Dict[str, Any]],
        nav: float,
    ) -> Dict[str, float]:
        book: Dict[str, float] = {}
        for trade in open_trades or []:
            inst = normalize_instrument(trade.get("instrument", ""))
            direction = trade_direction_from_units(trade.get("currentUnits"))
            risk_pct = self.estimate_trade_risk_pct(trade, prices, nav)
            if risk_pct <= 0:
                continue
            for key in self.currency_direction_keys(inst, direction):
                book[key] = book.get(key, 0.0) + risk_pct
        return book

    def correlated_thesis_cap_check(
        self,
        order: Dict[str, Any],
        risk_pct: float,
        thesis_risk: Dict[str, float],
    ) -> Tuple[bool, str]:
        if not cfg_bool("FOREX_GPT_CORRELATED_THESIS_CAP_ENABLED", default=False):
            return True, ""
        action = str(order.get("action", "")).upper().strip()
        if action not in {"OPEN", "SCALE_IN"}:
            return True, ""
        inst = normalize_instrument(order.get("instrument", ""))
        direction = str(order.get("direction", "")).upper().strip()
        keys = self.currency_direction_keys(inst, direction)
        if not keys:
            return True, ""
        cap = max(0.0, cfg_float("FOREX_GPT_MAX_CORRELATED_THESIS_RISK_PCT", default=6.0))
        if cap <= 0:
            return True, ""
        focus_ccys = {x.strip().upper() for x in cfg_list("FOREX_GPT_CORRELATED_THESIS_FOCUS_CURRENCIES", default=[])}
        checked = [key for key in keys if not focus_ccys or key.split("_", 1)[0] in focus_ccys]
        for key in checked:
            current = safe_float(thesis_risk.get(key), 0.0)
            if current + risk_pct > cap + 1e-9:
                return False, (
                    f"correlated thesis cap: {key} risk {current + risk_pct:.2f}% "
                    f"> {cap:.2f}% (existing={current:.2f}% new={risk_pct:.2f}%)"
                )
        return True, ""

    def add_correlated_thesis_risk(
        self,
        order: Dict[str, Any],
        risk_pct: float,
        thesis_risk: Dict[str, float],
    ) -> None:
        for key in self.currency_direction_keys(
            normalize_instrument(order.get("instrument", "")),
            str(order.get("direction", "")).upper().strip(),
        ):
            thesis_risk[key] = thesis_risk.get(key, 0.0) + max(0.0, risk_pct)

    def execute_decision(self, decision: Dict[str, Any], prices: Dict[str, Dict[str, Any]], open_trades: List[Dict[str, Any]]) -> None:
        # Existing positions first.
        by_id = {str(t.get("id")): t for t in open_trades if t.get("id") is not None}
        for action in decision.get("open_position_actions", []) or []:
            try:
                self.handle_position_action(action, by_id, prices)
            except Exception as exc:
                self.log_error("handle_position_action", exc)
                self.log_action(action, "open_position_action", "error", reject_reason=str(exc))

        # Refresh open trades after closes/flips to avoid duplicate openings.
        try:
            open_trades_after = self.oanda.get_open_trades()
        except Exception:
            open_trades_after = open_trades
        open_instruments = {normalize_instrument(t.get("instrument")) for t in open_trades_after}

        orders = decision.get("orders_to_execute", []) or []
        if self.sunday_reopen_new_trades_blocked():
            for order in orders:
                if str(order.get("action", "")).upper().strip() == "OPEN":
                    order["instrument"] = normalize_instrument(order.get("instrument", ""))
                    self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="Sunday reopen warmup blocks unrelated fresh opens; close/flip/reversal scouts remain allowed")
            return
        if self.shutdown_mode_active() and self.cfg.shutdown_block_new_trades:
            for order in orders:
                if str(order.get("action", "")).upper().strip() == "OPEN":
                    order["instrument"] = normalize_instrument(order.get("instrument", ""))
                    self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"shutdown {self.shutdown_mode_label()} mode blocks new opens")
            return
        new_count = 0
        total_new_risk = 0.0
        try:
            account_for_risk = self.oanda.get_account_summary()
            nav_for_risk = safe_float(account_for_risk.get("NAV", account_for_risk.get("balance")), 0.0)
        except Exception:
            nav_for_risk = 0.0
        thesis_risk = self.correlated_thesis_risk_book(open_trades_after, prices, nav_for_risk)
        for order in orders:
            action = str(order.get("action", "")).upper().strip()
            if action not in {"OPEN", "SCALE_IN"}:
                continue
            if new_count >= self.cfg.max_new_trades_per_scan:
                self.log_action(order, "open", "skipped", reject_reason="max_new_trades_per_scan reached")
                continue
            inst = normalize_instrument(order.get("instrument", ""))
            order["instrument"] = inst
            direction = str(order.get("direction", "")).upper().strip()
            if action == "SCALE_IN":
                matching = [
                    trade for trade in open_trades_after
                    if normalize_instrument(trade.get("instrument")) == inst
                    and trade_direction_from_units(trade.get("currentUnits")) == direction
                ]
                if not matching:
                    self.log_action(order, "scale_in", "skipped", reject_reason="SCALE_IN requires an existing same-direction open trade")
                    continue
                best_pl = max(safe_float(trade.get("unrealizedPL", trade.get("unrealized_pl")), 0.0) for trade in matching)
                if best_pl <= 0:
                    self.log_action(order, "scale_in", "skipped", reject_reason="SCALE_IN only allowed for profitable existing winners")
                    continue
                order["_scale_in"] = True
            elif inst in open_instruments:
                self.log_action(order, "open", "skipped", reject_reason="instrument already has open trade")
                continue
            if action != "SCALE_IN" and self.cfg.broker_recheck_before_actions and self.trade_lookup_blocks_open(inst):
                self.log_action(order, "open", "skipped", reject_reason="broker recheck found existing open trade")
                open_instruments.add(inst)
                continue
            akey = self.action_key(action, order)
            if self.recently_attempted_action(akey):
                self.log_action(order, "scale_in" if action == "SCALE_IN" else "open", "skipped", reject_reason="duplicate recent open action suppressed")
                continue
            risk_pct = clamp(safe_float(order.get("risk_pct"), 0.0), 0.0, self.cfg.max_risk_pct_per_trade)
            if risk_pct < self.cfg.min_risk_pct_per_trade:
                self.log_action(order, "scale_in" if action == "SCALE_IN" else "open", "skipped", reject_reason="risk_pct below configured minimum")
                continue
            allowed_thesis, why_thesis = self.correlated_thesis_cap_check(order, risk_pct, thesis_risk)
            if not allowed_thesis:
                self.log_action(order, "scale_in" if action == "SCALE_IN" else "open", "skipped", reject_reason=why_thesis)
                continue
            if total_new_risk + risk_pct > self.cfg.max_total_new_risk_pct_per_scan:
                self.log_action(order, "scale_in" if action == "SCALE_IN" else "open", "skipped", reject_reason="max_total_new_risk_pct_per_scan reached")
                continue
            status = self.open_trade_from_order(order, prices)
            if status.startswith("accepted") or status == "dry_run":
                self.remember_action(akey)
                new_count += 1
                total_new_risk += risk_pct
                self.add_correlated_thesis_risk(order, risk_pct, thesis_risk)
                if action != "SCALE_IN":
                    open_instruments.add(inst)

    def handle_position_action(self, action: Dict[str, Any], by_id: Dict[str, Dict[str, Any]], prices: Dict[str, Dict[str, Any]]) -> None:
        act = str(action.get("action", "HOLD")).upper().strip()
        trade_id = str(action.get("trade_id") or "").strip()
        inst = normalize_instrument(action.get("instrument", ""))
        trade = by_id.get(trade_id)
        if trade and ((trade_id and str(trade.get("id")) != trade_id) or (inst and str(trade.get("instrument")) != inst)):
            self.log_action(action, "position", "skipped", reject_reason="requested trade identity conflicts with retained position")
            return
        if not trade and inst and not trade_id:
            matches = [t for t in by_id.values() if str(t.get("instrument")) == inst]
            if len(matches) == 1:
                trade = matches[0]
                trade_id = str(trade.get("id"))
        if not trade:
            if self.cfg.broker_recheck_before_actions:
                observed = self.observe_open_trade(trade_id=trade_id, instrument=inst)
                if observed["status"] == "unknown":
                    self.log_action(action, "position", "uncertain_after_recheck", reject_reason=observed["reason_code"], raw_extra={"trade_observation": observed})
                    return
                trade = observed["trade"] if observed["status"] == "confirmed_open" else None
                if trade:
                    trade_id = str(trade.get("id"))
            if not trade:
                self.log_action(action, "position", "skipped", reject_reason="open trade not found")
                return
        inst = str(trade.get("instrument", inst))
        if self.cfg.broker_recheck_before_actions:
            observed = self.observe_open_trade(trade_id=str(trade.get("id")), instrument=inst)
            if observed["status"] != "confirmed_open":
                self.log_action(action, "position", "uncertain_after_recheck" if observed["status"] == "unknown" else "skipped",
                    reject_reason="broker trade state not confirmed open", raw_extra={"trade_observation": observed})
                return
            trade = observed["trade"]
            trade_id = str(trade.get("id"))
        if act not in {"HOLD", "WATCH"}:
            akey = self.action_key(act, action, trade_id=trade_id)
            if self.recently_attempted_action(akey):
                self.log_action(action, "position", "skipped", reject_reason="duplicate recent position action suppressed")
                return
        else:
            akey = ""
        inst = str(trade.get("instrument", inst))
        meta = self.instrument_meta.get(inst)
        if not meta:
            self.log_action(action, "position", "skipped", reject_reason="missing instrument metadata")
            return

        if act == "HOLD":
            self.log_action(action, "hold", "logged")
            return
        if act == "TIGHTEN":
            status = self.tighten_trade(trade_id, action, meta)
            if status in {"accepted", "dry_run"}:
                self.remember_action(akey)
            return
        if act == "PARTIAL_CLOSE":
            status = self.partial_close_trade(trade, action)
            if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                self.remember_action(akey)
            return
        if act == "CLOSE":
            status = self.close_trade(trade, action)
            if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                self.remember_action(akey)
            return
        if act == "FLIP":
            status = self.close_trade(trade, {**action, "reason": "FLIP close leg: " + str(action.get("reason", ""))})
            if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                self.remember_action(akey)
                self.open_trade_from_order({**action, "action": "OPEN", "_allow_sunday_reopen_open": cfg_bool("FOREX_REOPEN_ALLOW_FLIPS", default=True), "_reopen_flip": True}, prices)
            else:
                self.log_action(action, "flip", "skipped", reject_reason="close leg did not confirm")
            return
        self.log_action(action, "position", "skipped", reject_reason=f"unsupported action {act}")

    def tighten_trade(self, trade_id: str, action: Dict[str, Any], meta: InstrumentMeta) -> str:
        observed = self.observe_open_trade(trade_id=trade_id, instrument=str(action.get("instrument") or ""))
        if observed["status"] != "confirmed_open":
            self.log_action(action, "tighten", "uncertain_after_recheck" if observed["status"] == "unknown" else "skipped",
                reject_reason="protective trade state not confirmed open", raw_extra={"trade_observation": observed})
            return "uncertain_after_recheck" if observed["status"] == "unknown" else "skipped"
        trade = observed["trade"]
        instrument = trade["instrument"]
        if self.instrument_meta.get(instrument) is not meta:
            self.log_action(action, "tighten", "skipped", reject_reason="protective instrument metadata mismatch")
            return "skipped"
        try:
            prices = self.oanda.get_prices([instrument])
            checked = trade_reconciliation.protective_update(trade, action, prices.get(instrument),
                instrument=instrument, pip_size=meta.pip_size, display_precision=meta.display_precision,
                observed_epoch=time.time())
        except Exception as exc:
            self.log_action(action, "tighten", "skipped", reject_reason="protective recheck refused: " + str(exc)[:240])
            return "skipped"
        if not self.should_execute():
            self.log_action(action, "tighten", "dry_run", raw_extra={"protective_check": checked})
            return "dry_run"
        try:
            res = self.oanda.set_dependent_orders(trade_id, meta.display_precision,
                checked["stop_loss"], checked["take_profit"], checked["trailing_distance"])
            self.log_action(action, "tighten", "accepted", raw_extra=res)
            return "accepted"
        except Exception as exc:
            self.log_action(action, "tighten", "uncertain_after_recheck", reject_reason=str(exc)[:500],
                raw_extra={"protective_check": checked, "confirmation": "dependent order outcome unknown"})
            return "uncertain_after_recheck"

    def partial_close_trade(self, trade: Dict[str, Any], action: Dict[str, Any]) -> str:
        try:
            _, _, signed_units = trade_reconciliation.identity(trade)
            if signed_units != signed_units.to_integral_value():
                raise ValueError("fractional units require an explicit reduction contract")
            current_units = int(signed_units.copy_abs())
        except (ValueError, TypeError, ArithmeticError) as exc:
            self.log_action(action, "partial_close", "skipped", reject_reason=str(exc))
            return "skipped"
        pct = clamp(safe_float(action.get("partial_close_pct"), 50.0), self.cfg.partial_close_min_pct, self.cfg.partial_close_max_pct)
        close_units = min(current_units, max(1, int(math.floor(current_units * pct / 100.0))))
        return self._close_with_state_confirmation(trade, action, kind="partial_close", requested_units=close_units)

    def _close_with_state_confirmation(self, trade: Dict[str, Any], action: Dict[str, Any], *, kind: str, requested_units: Optional[int]) -> str:
        try:
            trade_id, instrument, signed_units = trade_reconciliation.identity(trade)
        except (ValueError, TypeError, ArithmeticError) as exc:
            self.log_action(action, kind, "skipped", reject_reason=str(exc))
            return "skipped"
        intended = signed_units.copy_abs() if requested_units is None else requested_units
        if not self.should_execute():
            self.log_action(action, kind, "dry_run", units=str(intended))
            return "dry_run"
        failure = ""
        response = {}
        try:
            response = self.oanda.close_trade(trade_id, units=requested_units)
        except Exception as exc:
            failure = str(exc)[:500]
        observed = self.observe_open_trade(trade_id=trade_id, instrument=instrument)
        reconciliation = trade_reconciliation.reconcile_reduction(trade, observed, requested_units=requested_units)
        extra = {"broker_response": response, "trade_observation": observed, "reconciliation": reconciliation,
                 "requested_units": str(intended), "confirmation_scope": "position state; no transaction attribution"}
        if reconciliation["status"] == "confirmed_reduction":
            status = "accepted_after_recheck" if failure else "accepted"
            self.log_action(action, kind, status, units=reconciliation["observed_reduction_units"],
                reject_reason=failure, raw_extra=extra)
            return status
        self.log_action(action, kind, "uncertain_after_recheck", units="",
            reject_reason=reconciliation["reason_code"], raw_extra=extra)
        return "uncertain_after_recheck"

    def close_trade(self, trade: Dict[str, Any], action: Dict[str, Any]) -> str:
        return self._close_with_state_confirmation(trade, action, kind="close", requested_units=None)

    def open_trade_from_order(self, order: Dict[str, Any], prices: Dict[str, Dict[str, Any]]) -> str:
        inst = normalize_instrument(order.get("instrument", ""))
        order["instrument"] = inst
        log_type = "event_scout" if order.get("_event_scout") else ("scale_in" if order.get("_scale_in") else "open")
        if not self.instrument_allowed_for_lane(inst):
            self.log_action(order, log_type, "skipped", reject_reason=f"instrument not routed to this account lane ({self.cfg.instrument_filter_mode})")
            return "skipped"
        if self.sunday_reopen_new_trades_blocked():
            allow_reopen_open = bool(order.get("_allow_sunday_reopen_open"))
            allow_reopen_open = allow_reopen_open or (bool(order.get("_event_scout")) and cfg_bool("FOREX_REOPEN_ALLOW_CONFIRMED_EVENT_SCOUTS", default=True))
            if not allow_reopen_open:
                self.log_action(order, log_type, "skipped", reject_reason="Sunday reopen warmup blocks unrelated fresh opens; close/flip/reversal scouts remain allowed")
                return "skipped"
        if self.shutdown_mode_active() and self.cfg.shutdown_block_new_trades:
            self.log_action(order, log_type, "skipped", reject_reason=f"shutdown {self.shutdown_mode_label()} mode blocks new opens")
            return "skipped"
        direction = str(order.get("direction", "")).upper().strip()
        if direction == "BUY":
            direction = "LONG"
        if direction == "SELL":
            direction = "SHORT"
        if inst not in self.instrument_meta:
            self.log_action(order, log_type, "skipped", reject_reason="unknown/non-currency instrument")
            return "skipped"
        if direction not in {"LONG", "SHORT"}:
            self.log_action(order, log_type, "skipped", reject_reason="direction must be LONG or SHORT")
            return "skipped"
        price = prices.get(inst, {})
        if not price or not price_tradeable(price):
            self.log_action(order, log_type, "skipped", reject_reason="price unavailable or not tradeable")
            return "skipped"
        if self.cfg.broker_recheck_before_actions and not order.get("_scale_in") and self.trade_lookup_blocks_open(inst):
            self.log_action(order, log_type, "skipped", reject_reason="broker recheck found existing open trade")
            return "skipped"
        meta = self.instrument_meta[inst]
        spr = spread_pips(price, meta)
        max_spread = self.event_max_spread_pips(inst) if order.get("_event_scout") else self.max_spread_allowed(inst)
        if spr > max_spread:
            self.log_action(order, log_type, "skipped", reject_reason=f"spread too wide {spr:.2f}p > {max_spread:.2f}p")
            return "skipped"
        bid, ask = bid_ask(price)
        if bid is None or ask is None:
            self.log_action(order, log_type, "skipped", reject_reason="bid/ask unavailable")
            return "skipped"
        entry = ask if direction == "LONG" else bid
        stop_loss = as_optional_float(order.get("stop_loss"))
        take_profit = first_optional_float(order.get("take_profit"), order.get("tp1"))
        trailing_pips = as_optional_float(order.get("trailing_stop_pips"))

        if self.cfg.require_stop_loss_on_open and stop_loss is None:
            self.log_action(order, log_type, "skipped", reject_reason="stop_loss required by config")
            return "skipped"
        if self.cfg.require_take_profit_on_open and take_profit is None:
            self.log_action(order, log_type, "skipped", reject_reason="take_profit required by config")
            return "skipped"
        if stop_loss is not None:
            stop_pips = abs(entry - stop_loss) / meta.pip_size
            if stop_pips <= 0:
                self.log_action(order, log_type, "skipped", reject_reason="invalid stop distance")
                return "skipped"
            if order.get("_event_scout") and self.is_volatile_scout_pair(inst):
                spread_stop_limit = self.cfg.scout_volatile_max_spread_to_stop_ratio
            else:
                spread_stop_limit = cfg_float("FOREX_SCOUT_MAX_SPREAD_TO_STOP_RATIO", default=0.45) if order.get("_event_scout") else self.cfg.max_spread_to_stop_ratio
            if spr / max(stop_pips, 0.00001) > spread_stop_limit:
                self.log_action(order, log_type, "skipped", reject_reason=f"spread/stop ratio too high {spr / max(stop_pips, 0.00001):.2f} > {spread_stop_limit:.2f}")
                return "skipped"
            if direction == "LONG" and stop_loss >= entry:
                self.log_action(order, log_type, "skipped", reject_reason="LONG stop_loss must be below entry")
                return "skipped"
            if direction == "SHORT" and stop_loss <= entry:
                self.log_action(order, log_type, "skipped", reject_reason="SHORT stop_loss must be above entry")
                return "skipped"
        else:
            stop_pips = None

        risk_pct = clamp(safe_float(order.get("risk_pct"), 0.0), 0.0, self.cfg.max_risk_pct_per_trade)
        account = self.oanda.get_account_summary()
        nav = safe_float(account.get("NAV", account.get("nav", account.get("balance"))))
        units_abs, risk_usd, margin_usd, reject = self.size_units(inst, direction, entry, stop_loss, risk_pct, prices, nav, account)
        if reject:
            self.log_action(order, log_type, "skipped", reject_reason=reject)
            return "skipped"
        signed_units = side_to_units(direction, units_abs)

        price_bound = None
        slippage_distance = self.cfg.max_entry_slippage_pips * meta.pip_size
        if direction == "LONG":
            price_bound = ask + slippage_distance
        else:
            price_bound = bid - slippage_distance
        trailing_distance = None
        if self.cfg.allow_trailing_stop_on_open and trailing_pips and trailing_pips > 0:
            trailing_distance = trailing_pips * meta.pip_size

        if not self.should_execute():
            self.record_dry_run_order(order, signed_units, entry, risk_usd, margin_usd)
            self.log_action(order, log_type, "dry_run", units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd)
            return "dry_run"
        try:
            res = self.oanda.create_market_order(
                inst,
                signed_units,
                price_bound=price_bound,
                stop_loss=stop_loss,
                take_profit=take_profit,
                trailing_stop_distance=trailing_distance,
                precision=meta.display_precision,
                tag=f"gptfxv5_17_{self.cfg.account_lane[:12]}_{now_key_date().replace('-', '')}",
            )
            status = order_status_from_result(res)
            fill = order_fill_price(res)
            self.log_action(order, log_type, status, units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd, fill_price=fill, raw_extra=res)
            return status
        except Exception as exc:
            refreshed = self.find_open_trade(instrument=inst, direction=direction) if self.cfg.broker_recheck_before_actions else None
            if refreshed:
                self.log_action(order, log_type, "accepted_after_recheck", units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd, reject_reason=str(exc)[:500], raw_extra={"rechecked_trade": summarize_trade_for_prompt(refreshed)})
                return "accepted_after_recheck"
            self.log_action(order, log_type, "error", units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd, reject_reason=str(exc)[:500])
            return "error"

    def size_units(
        self,
        instrument: str,
        direction: str,
        entry: float,
        stop_loss: Optional[float],
        risk_pct: float,
        prices: Dict[str, Dict[str, Any]],
        nav: float,
        account: Dict[str, Any],
    ) -> Tuple[int, float, float, str]:
        meta = self.instrument_meta[instrument]
        if risk_pct < self.cfg.min_risk_pct_per_trade:
            return 0, 0.0, 0.0, "risk_pct below configured minimum"
        if nav <= 0:
            return 0, 0.0, 0.0, "invalid NAV"
        if stop_loss is None:
            return 0, 0.0, 0.0, "cannot size without stop_loss"
        stop_distance_price = abs(entry - stop_loss)
        if stop_distance_price <= 0:
            return 0, 0.0, 0.0, "invalid stop distance"
        quote_rate = quote_to_account_rate(instrument, prices, self.cfg.account_currency)
        if quote_rate is None:
            return 0, 0.0, 0.0, "quote-to-account conversion unavailable"
        pip_value_per_unit = meta.pip_size * quote_rate
        stop_pips = stop_distance_price / meta.pip_size
        risk_usd = nav * risk_pct / 100.0
        risk_units = risk_usd / max(stop_pips * pip_value_per_unit, 1e-12)

        base_rate = base_to_account_rate(instrument, prices, self.cfg.account_currency)
        if base_rate is None:
            return 0, 0.0, 0.0, "base-to-account conversion unavailable"
        margin_per_unit = base_rate * meta.margin_rate
        max_margin_total = nav * self.cfg.max_margin_used_pct / 100.0
        margin_used = safe_float(account.get("marginUsed", 0.0))
        available_margin_budget = max(0.0, max_margin_total - margin_used)
        margin_units = available_margin_budget / max(margin_per_unit, 1e-12)
        units_abs = min(risk_units, margin_units)
        units_abs_int = round_units(units_abs, meta)
        if units_abs_int <= 0:
            return 0, risk_usd, 0.0, "units rounded below minimum trade size or margin unavailable"
        margin_usd = units_abs_int * margin_per_unit
        if margin_usd > available_margin_budget + 1e-9:
            return 0, risk_usd, margin_usd, "margin cap exceeded"
        return units_abs_int, risk_usd, margin_usd, ""

    def max_spread_allowed(self, instrument: str) -> float:
        base, quote = split_instrument(instrument)
        majors = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
        if base in majors and quote in majors:
            return self.cfg.max_spread_pips_default
        return self.cfg.max_spread_pips_exotic

    def record_dry_run_order(self, order: Dict[str, Any], signed_units: int, entry: float, risk_usd: float, margin_usd: float) -> None:
        if not self.cfg.dry_run_state_orders:
            return
        state = self.load_state()
        state.setdefault("dry_run_orders", []).append({
            "time_utc": iso_utc(),
            "order": order,
            "signed_units": signed_units,
            "entry": entry,
            "risk_usd": risk_usd,
            "margin_usd": margin_usd,
        })
        state["dry_run_orders"] = state["dry_run_orders"][-500:]
        self.save_state(state)

    def log_action(
        self,
        action: Dict[str, Any],
        action_type: str,
        status: str,
        *,
        units: Any = "",
        risk_usd: Any = "",
        margin_usd: Any = "",
        fill_price: Any = "",
        reject_reason: str = "",
        raw_extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        raw = {"action": action, "result": raw_extra or {}}
        append_csv(self.cfg.actions_csv, {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "action_type": action_type,
            "status": status,
            "instrument": normalize_instrument(action.get("instrument", "")),
            "direction": action.get("direction", ""),
            "trade_id": action.get("trade_id", ""),
            "units": units,
            "risk_pct": action.get("risk_pct", ""),
            "risk_usd": risk_usd,
            "margin_usd": margin_usd,
            "outlook_confidence": action.get("outlook_confidence", ""),
            "fill_price": fill_price,
            "stop_loss": action.get("stop_loss", ""),
            "take_profit": action.get("take_profit", action.get("tp1", "")),
            "trailing_stop_pips": action.get("trailing_stop_pips", ""),
            "partial_close_pct": action.get("partial_close_pct", ""),
            "reason": str(action.get("reason", action.get("why_now", "")))[:1000],
            "reject_reason": reject_reason,
            "raw_json": json.dumps(raw, default=str)[:12000],
        }, ACTION_FIELDS)
        try:
            self.full_logger.log_event("action_logged", status=status, instrument=normalize_instrument(action.get("instrument", "")), direction=action.get("direction", ""), trade_id=action.get("trade_id", ""), units=units, price=fill_price, movement_key=str(action.get("_movement_key", "")), reason=str(action.get("reason", action.get("why_now", "")))[:1000], result=reject_reason, raw=raw)
            if raw_extra:
                self.full_logger.log_order_result(action_type, status, action, raw_extra)
                cancel_tx = raw_extra.get("orderCancelTransaction") if isinstance(raw_extra, dict) else None
                if isinstance(cancel_tx, dict) and cancel_tx:
                    self.full_logger.log_cancel("submit_response_cancel", cancel_tx, action=action, movement_key=str(action.get("_movement_key", "")))
                related = raw_extra.get("relatedTransactionIDs", []) if isinstance(raw_extra, dict) else []
                if status == "accepted_with_cancel" and related:
                    self.full_logger.log_event("order_result_related_cancel", status=status, instrument=normalize_instrument(action.get("instrument", "")), direction=action.get("direction", ""), trade_id=action.get("trade_id", ""), reason="order response included relatedTransactionIDs with accepted_with_cancel", raw=raw_extra)
        except Exception:
            pass
        try:
            self.log_movement_capture(action, action_type, status, units=units, risk_usd=risk_usd, margin_usd=margin_usd, fill_price=fill_price, reject_reason=reject_reason, raw_extra=raw_extra)
        except Exception as exc:
            try:
                self.cfg.errors_log.parent.mkdir(parents=True, exist_ok=True)
                self.cfg.errors_log.write_text(error_text("log_movement_capture", exc), encoding="utf-8")
            except Exception:
                pass
        msg = f"{self.lane_prefix()}[{action_type.upper()}] {status} {normalize_instrument(action.get('instrument', ''))} {action.get('direction', '')}"
        if reject_reason:
            msg += f" reject={reject_reason}"
        log(msg)

    def shutdown_fade_due(self) -> bool:
        if not self.shutdown_mode_active():
            return False
        state = self.load_state()
        last_raw = state.get("last_shutdown_pass_utc")
        if not last_raw:
            return True
        try:
            last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(UTC)
            age_min = (utc_now() - last).total_seconds() / 60.0
            return age_min >= max(1, self.cfg.shutdown_min_minutes_between_passes)
        except Exception:
            return True

    def mark_shutdown_fade_pass(self) -> None:
        state = self.load_state()
        state["last_shutdown_pass_utc"] = iso_utc()
        self.save_state(state)

    def shutdown_stop_gap_pips(self, instrument: str) -> float:
        base, quote = split_instrument(instrument)
        majors = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
        if base in majors and quote in majors:
            return max(self.cfg.shutdown_min_stop_gap_pips, self.cfg.shutdown_stop_gap_pips_default)
        return max(self.cfg.shutdown_min_stop_gap_pips, self.cfg.shutdown_stop_gap_pips_exotic)

    def compute_shutdown_stop(self, trade: Dict[str, Any], price: Dict[str, Any], meta: InstrumentMeta) -> Tuple[Optional[float], float, str]:
        """Return a non-worsening protective stop for graceful shutdown.

        The stop is intended to let profitable trades breathe a little while making the
        remaining downside small. It never intentionally loosens an existing broker stop.
        """
        direction = trade_direction_from_units(trade.get("currentUnits"))
        entry = trade_entry_price(trade)
        if entry is None or entry <= 0:
            return None, 0.0, "entry price unavailable"
        bid, ask = bid_ask(price)
        if bid is None or ask is None:
            return None, 0.0, "bid/ask unavailable"
        exit_price = bid if direction == "LONG" else ask
        profit_pips = ((exit_price - entry) / meta.pip_size) if direction == "LONG" else ((entry - exit_price) / meta.pip_size)
        spread = spread_pips(price, meta)
        min_gap_pips = max(self.cfg.shutdown_min_stop_gap_pips, spread * 2.0)
        gap_pips = max(self.shutdown_stop_gap_pips(meta.name), min_gap_pips)
        min_gap_price = min_gap_pips * meta.pip_size
        gap_price = gap_pips * meta.pip_size

        if direction == "LONG":
            max_loss_stop = entry - self.cfg.shutdown_max_loss_pips * meta.pip_size
            if profit_pips > 0:
                lock_pips = max(self.cfg.shutdown_min_lock_profit_pips, profit_pips * self.cfg.shutdown_lock_profit_ratio)
                lock_stop = entry + lock_pips * meta.pip_size
                trail_stop = exit_price - gap_price
                candidate = max(max_loss_stop, min(lock_stop, trail_stop))
            else:
                candidate = max_loss_stop
            candidate = round_stop_to_valid_side(direction, candidate, exit_price, min_gap_price)
            existing = existing_stop_price_from_trade(trade)
            if not stop_is_more_protective(direction, candidate, existing):
                return None, profit_pips, "existing stop is already tighter or equal"
            if candidate >= exit_price:
                return None, profit_pips, "computed LONG stop not below market"
            return candidate, profit_pips, ""

        max_loss_stop = entry + self.cfg.shutdown_max_loss_pips * meta.pip_size
        if profit_pips > 0:
            lock_pips = max(self.cfg.shutdown_min_lock_profit_pips, profit_pips * self.cfg.shutdown_lock_profit_ratio)
            lock_stop = entry - lock_pips * meta.pip_size
            trail_stop = exit_price + gap_price
            candidate = min(max_loss_stop, max(lock_stop, trail_stop))
        else:
            candidate = max_loss_stop
        candidate = round_stop_to_valid_side(direction, candidate, exit_price, min_gap_price)
        existing = existing_stop_price_from_trade(trade)
        if not stop_is_more_protective(direction, candidate, existing):
            return None, profit_pips, "existing stop is already tighter or equal"
        if candidate <= exit_price:
            return None, profit_pips, "computed SHORT stop not above market"
        return candidate, profit_pips, ""

    def run_shutdown_fade_pass(
        self,
        *,
        reason: str = "shutdown_fade",
        account: Optional[Dict[str, Any]] = None,
        open_trades: Optional[List[Dict[str, Any]]] = None,
        prices: Optional[Dict[str, Dict[str, Any]]] = None,
        force: bool = False,
    ) -> None:
        if not self.shutdown_mode_active():
            return
        if not force and not self.shutdown_fade_due():
            return
        if account is None:
            account = self.oanda.get_account_summary()
        if open_trades is None:
            open_trades = self.oanda.get_open_trades()
        if not open_trades:
            self.mark_shutdown_fade_pass()
            log(f"Shutdown {self.shutdown_mode_label()}: no open trades left.")
            return
        if not self.instruments:
            self.refresh_instruments()
        insts = sorted({normalize_instrument(t.get("instrument")) for t in open_trades if normalize_instrument(t.get("instrument"))})
        if prices is None:
            prices = self.oanda.get_prices(insts)
        summary = account_summary_for_prompt(account)
        log(f"Shutdown {self.shutdown_mode_label()} pass: reason={reason} NAV={summary.get('nav', 0):.2f} margin_used_pct={summary.get('margin_used_pct_of_nav', 0):.2f} open_trades={len(open_trades)}")

        for trade in list(open_trades):
            inst = normalize_instrument(trade.get("instrument"))
            if inst not in self.instrument_meta:
                self.log_action({"instrument": inst, "trade_id": trade.get("id"), "action": "SHUTDOWN_FADE"}, "shutdown", "skipped", reject_reason="missing instrument metadata")
                continue
            meta = self.instrument_meta[inst]
            price = prices.get(inst, {})
            if not price or not price_tradeable(price):
                self.log_action({"instrument": inst, "trade_id": trade.get("id"), "action": "SHUTDOWN_FADE"}, "shutdown", "skipped", reject_reason="price unavailable or not tradeable")
                continue
            direction = trade_direction_from_units(trade.get("currentUnits"))
            trade_id = str(trade.get("id", ""))
            unrealized_pl = safe_float(trade.get("unrealizedPL"), 0.0)
            action_base = {
                "instrument": inst,
                "trade_id": trade_id,
                "direction": direction,
                "outlook_confidence": 0,
                "risk_pct": 0,
                "reason": f"shutdown fade mode active: {reason}",
            }

            stop_loss, profit_pips, stop_reject = self.compute_shutdown_stop(trade, price, meta)

            if self.cfg.shutdown_mode == "close_now":
                self.close_trade(trade, {**action_base, "action": "CLOSE", "reason": "shutdown close_now mode: close all open trades"})
                continue

            if self.cfg.shutdown_close_if_loss_pips_beyond > 0 and profit_pips <= -abs(self.cfg.shutdown_close_if_loss_pips_beyond):
                self.close_trade(trade, {**action_base, "action": "CLOSE", "reason": f"shutdown fade: loss reached {profit_pips:.1f} pips; closing before larger damage"})
                continue

            if self.cfg.shutdown_full_close_profit_pips > 0 and profit_pips >= self.cfg.shutdown_full_close_profit_pips:
                self.close_trade(trade, {**action_base, "action": "CLOSE", "reason": f"shutdown fade: profit reached {profit_pips:.1f} pips; full close target met"})
                continue

            # Optional staged reduction of winners. Deduping stops it from clipping repeatedly every loop.
            if (
                self.cfg.shutdown_partial_close_pct > 0
                and profit_pips >= self.cfg.shutdown_min_profit_pips_to_partial
                and unrealized_pl >= self.cfg.shutdown_min_profit_usd_to_partial
            ):
                paction = {
                    **action_base,
                    "action": "PARTIAL_CLOSE",
                    "partial_close_pct": self.cfg.shutdown_partial_close_pct,
                    "reason": f"shutdown fade: partial close winner; profit_pips={profit_pips:.1f}, unrealizedPL={unrealized_pl:.2f}",
                }
                akey = self.action_key("SHUTDOWN_PARTIAL_CLOSE", paction, trade_id=trade_id)
                if not self.recently_attempted_action(akey):
                    status = self.partial_close_trade(trade, paction)
                    if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                        self.remember_action(akey)

            if stop_loss is not None:
                taction = {
                    **action_base,
                    "action": "TIGHTEN",
                    "stop_loss": stop_loss,
                    "take_profit": None,
                    "trailing_stop_pips": None,
                    "reason": f"shutdown fade: tighten stop; profit_pips={profit_pips:.1f}",
                }
                akey = self.action_key("SHUTDOWN_TIGHTEN", taction, trade_id=trade_id)
                if self.recently_attempted_action(akey):
                    self.log_action(taction, "shutdown_tighten", "skipped", reject_reason="duplicate recent shutdown tighten suppressed")
                else:
                    status = self.tighten_trade(trade_id, taction, meta)
                    if status in {"accepted", "dry_run"}:
                        self.remember_action(akey)
            else:
                self.log_action({**action_base, "action": "TIGHTEN"}, "shutdown_tighten", "skipped", reject_reason=stop_reject or "no valid tighter stop")

        self.mark_shutdown_fade_pass()


    # -------------------------------------------------------------------------
    # Fast local M5 event scanner
    # -------------------------------------------------------------------------
    def save_event_permissions_from_decision(self, decision: Dict[str, Any]) -> None:
        perms = decision.get("event_permissions", []) if isinstance(decision, dict) else []
        if not isinstance(perms, list):
            perms = []
        cleaned: List[Dict[str, Any]] = []
        now = utc_now()
        for p in perms:
            if not isinstance(p, dict):
                continue
            theme = str(p.get("theme", "")).strip().upper()
            if not theme:
                continue
            allowed_pairs = [normalize_instrument(x) for x in (p.get("allowed_pairs") or []) if normalize_instrument(x)]
            dirs_raw = p.get("allowed_directions") or {}
            allowed_directions: Dict[str, str] = {}
            if isinstance(dirs_raw, dict):
                for k, v in dirs_raw.items():
                    inst = normalize_instrument(k)
                    d = str(v).strip().upper()
                    if inst and d in {"LONG", "SHORT"}:
                        allowed_directions[inst] = d
            expires_h = safe_float(p.get("expires_hours"), self.cfg.event_permission_default_expiry_hours)
            cleaned.append({
                "theme": theme,
                "allowed_pairs": allowed_pairs,
                "allowed_directions": allowed_directions,
                "max_scout_risk_pct": safe_float(p.get("max_scout_risk_pct"), self.cfg.event_scout_risk_pct),
                "expires_utc": (now + dt.timedelta(hours=max(0.25, expires_h))).isoformat(),
                "requires_basket_confirmation": bool(p.get("requires_basket_confirmation", True)),
                "reason": str(p.get("reason", ""))[:500],
                "created_utc": now.isoformat(),
            })
        if cleaned:
            state = self.load_state()
            state["event_permissions"] = cleaned[-50:]
            self.save_state(state)
            log(f"Saved {len(cleaned)} event permission(s) from GPT scan.")

    def is_volatile_scout_pair(self, instrument: str) -> bool:
        inst = normalize_instrument(instrument)
        return bool(self.cfg.scout_volatile_lane_enabled and inst in set(self.cfg.scout_volatile_pairs or []))

    def event_pair_kind(self, instrument: str) -> str:
        inst = normalize_instrument(instrument)
        if self.is_volatile_scout_pair(inst):
            return "volatile"
        base, quote = split_instrument(inst)
        majors = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
        if base in majors and quote in majors:
            return "major"
        if base in majors or quote in majors:
            return "cross"
        return "exotic"

    def event_threshold_pips(self, instrument: str) -> float:
        kind = self.event_pair_kind(instrument)
        if kind == "volatile":
            return self.cfg.scout_volatile_min_net_pips
        if kind == "major":
            return self.cfg.event_min_major_net_pips
        if kind == "cross":
            return self.cfg.event_min_cross_net_pips
        return self.cfg.event_min_exotic_net_pips

    def event_max_spread_pips(self, instrument: str) -> float:
        kind = self.event_pair_kind(instrument)
        if kind == "volatile":
            return self.cfg.scout_volatile_max_spread_pips
        if kind == "major":
            return self.cfg.event_max_spread_pips_major
        if kind == "cross":
            return self.cfg.event_max_spread_pips_cross
        return self.cfg.event_max_spread_pips_exotic

    def event_stop_pips(self, instrument: str, net_pips: float, spread_pips_hint: float = 0.0) -> float:
        kind = self.event_pair_kind(instrument)
        if kind == "volatile":
            return max(
                self.cfg.scout_volatile_stop_pips,
                abs(net_pips) * self.cfg.scout_volatile_stop_net_multiplier,
                max(0.0, spread_pips_hint) * self.cfg.scout_volatile_stop_spread_multiplier,
            )
        base = self.cfg.event_scout_stop_pips_major if kind == "major" else (self.cfg.event_scout_stop_pips_cross if kind == "cross" else self.cfg.event_scout_stop_pips_exotic)
        return max(base, abs(net_pips) * 0.45)

    def event_trailing_pips(self, instrument: str) -> float:
        kind = self.event_pair_kind(instrument)
        if kind == "volatile":
            return self.cfg.scout_volatile_trailing_stop_pips
        if kind == "major":
            return self.cfg.event_scout_trailing_stop_pips_major
        if kind == "cross":
            return self.cfg.event_scout_trailing_stop_pips_cross
        return self.cfg.event_scout_trailing_stop_pips_exotic

    def event_granularity_minutes(self) -> int:
        text = str(self.cfg.event_candle_granularity or "M1").strip().upper()
        if text.startswith("M"):
            try:
                return max(1, int(text[1:]))
            except Exception:
                return 1
        if text.startswith("H"):
            try:
                return max(1, int(text[1:]) * 60)
            except Exception:
                return 60
        return 1

    def parse_bma_candle(self, candle: Dict[str, Any], meta: InstrumentMeta) -> Optional[Dict[str, Any]]:
        try:
            m = candle.get("mid") or {}
            b = candle.get("bid") or {}
            a = candle.get("ask") or {}
            return {
                "time": str(candle.get("time", "")),
                "mid_o": float(m["o"]), "mid_h": float(m.get("h", m["c"])), "mid_l": float(m.get("l", m["c"])), "mid_c": float(m["c"]),
                "bid_o": float(b["o"]), "bid_c": float(b["c"]),
                "ask_o": float(a["o"]), "ask_c": float(a["c"]),
                "spread_o_pips": (float(a["o"]) - float(b["o"])) / meta.pip_size,
                "spread_c_pips": (float(a["c"]) - float(b["c"])) / meta.pip_size,
                "volume": safe_float(candle.get("volume"), 0.0),
            }
        except Exception:
            return None

    def best_recent_event_windows(self, instrument: str, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        meta = self.instrument_meta[instrument]
        rows = [r for c in candles if (r := self.parse_bma_candle(c, meta))]
        if len(rows) < 2:
            return []
        out: List[Dict[str, Any]] = []
        gran_min = self.event_granularity_minutes()
        for minutes in sorted(set(max(1, int(x)) for x in self.cfg.event_windows_minutes)):
            bars = max(1, int(math.ceil(minutes / max(1, gran_min))))
            if len(rows) < bars:
                continue
            window = rows[-bars:]
            start = window[0]
            end = window[-1]
            long_net = (end["bid_c"] - start["ask_o"]) / meta.pip_size
            short_net = (start["bid_o"] - end["ask_c"]) / meta.pip_size
            mid_move = (end["mid_c"] - start["mid_o"]) / meta.pip_size
            if long_net >= short_net:
                direction = "LONG"
                net_pips = long_net
            else:
                direction = "SHORT"
                net_pips = short_net
            avg_spread = (sum(r["spread_o_pips"] + r["spread_c_pips"] for r in window) / max(1, 2 * len(window)))
            avg_range = (sum((r.get("mid_h", r["mid_c"]) - r.get("mid_l", r["mid_c"])) / meta.pip_size for r in window) / max(1, len(window)))
            volume_sum = sum(safe_float(r.get("volume"), 0.0) for r in window)
            out.append({
                "instrument": instrument,
                "window_minutes": minutes,
                "direction": direction,
                "net_pips": net_pips,
                "mid_move_pips": mid_move,
                "spread_avg_pips": avg_spread,
                "avg_range_pips": avg_range,
                "volume_sum": volume_sum,
                "move_to_spread_ratio": net_pips / max(avg_spread, 0.1),
                "start_utc": start["time"],
                "end_utc": end["time"],
            })
        return out

    def classify_event_signal_themes(self, sig: Dict[str, Any]) -> List[str]:
        inst = normalize_instrument(sig.get("instrument"))
        direction = str(sig.get("direction", "")).upper()
        base, quote = split_instrument(inst)
        themes: List[str] = []
        if base == "USD" and direction == "SHORT" or quote == "USD" and direction == "LONG":
            themes.append("USD_SELLOFF")
        if base == "USD" and direction == "LONG" or quote == "USD" and direction == "SHORT":
            themes.append("USD_RALLY")
        if quote == "JPY" and direction == "SHORT" or base == "JPY" and direction == "LONG":
            themes.append("JPY_STRENGTH")
        if quote == "JPY" and direction == "LONG" or base == "JPY" and direction == "SHORT":
            themes.append("JPY_WEAKNESS")
        if quote == "CHF" and direction == "SHORT" or base == "CHF" and direction == "LONG":
            themes.append("CHF_STRENGTH")
        if quote == "CHF" and direction == "LONG" or base == "CHF" and direction == "SHORT":
            themes.append("CHF_WEAKNESS")
        risk_ccy = {"AUD", "NZD", "ZAR", "MXN", "TRY"}
        if (base in risk_ccy and direction == "LONG") or (quote in risk_ccy and direction == "SHORT"):
            themes.append("RISK_ON")
        if (base in risk_ccy and direction == "SHORT") or (quote in risk_ccy and direction == "LONG"):
            themes.append("RISK_OFF")
        commodity_ccy = {"AUD", "NZD", "CAD", "NOK", "ZAR"}
        if (base in commodity_ccy and direction == "LONG") or (quote in commodity_ccy and direction == "SHORT"):
            themes.append("COMMODITY_CURRENCY_STRENGTH")
        if (base in commodity_ccy and direction == "SHORT") or (quote in commodity_ccy and direction == "LONG"):
            themes.append("COMMODITY_CURRENCY_WEAKNESS")
        return sorted(set(themes))

    def event_scan_due(self, state: Optional[Dict[str, Any]] = None) -> bool:
        if not self.cfg.event_scanner_enabled or fx_market_closed() or self.sunday_reopen_event_scanner_blocked():
            return False
        state = state or self.load_state()
        last_raw = state.get("last_event_scan_utc")
        if not last_raw:
            return True
        try:
            last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(UTC)
            return (utc_now() - last).total_seconds() >= max(15, self.cfg.event_scan_interval_seconds)
        except Exception:
            return True

    def active_event_permissions(self) -> List[Dict[str, Any]]:
        state = self.load_state()
        perms = state.get("event_permissions", [])
        if not isinstance(perms, list):
            return []
        now = utc_now()
        out: List[Dict[str, Any]] = []
        for p in perms:
            if not isinstance(p, dict):
                continue
            try:
                exp = dt.datetime.fromisoformat(str(p.get("expires_utc", "")).replace("Z", "+00:00")).astimezone(UTC)
                if exp < now:
                    continue
            except Exception:
                continue
            out.append(p)
        return out

    def permission_allows_signal(self, theme: str, sig: Dict[str, Any]) -> Tuple[bool, float, str]:
        inst = normalize_instrument(sig.get("instrument"))
        direction = str(sig.get("direction", "")).upper()
        if not self.cfg.event_require_gpt_permission:
            return True, self.cfg.event_scout_risk_pct, "permission not required"
        for p in self.active_event_permissions():
            if str(p.get("theme", "")).upper() != str(theme).upper():
                continue
            allowed_pairs = [normalize_instrument(x) for x in (p.get("allowed_pairs") or [])]
            dirs = p.get("allowed_directions") or {}
            if allowed_pairs and inst not in allowed_pairs:
                continue
            if isinstance(dirs, dict):
                pd = str(dirs.get(inst, "")).upper()
                if pd and pd != direction:
                    continue
            return True, min(self.cfg.event_scout_risk_pct, safe_float(p.get("max_scout_risk_pct"), self.cfg.event_scout_risk_pct)), str(p.get("reason", ""))[:400]
        return False, 0.0, "no active GPT event permission for theme/pair/direction"

    def scout_profile_candidate_paths(self) -> List[Path]:
        paths: List[Path] = []
        explicit = str(self.cfg.scout_profile_csv_path or "").strip()
        if explicit:
            paths.append(Path(explicit))
        if self.cfg.scout_auto_load_latest_profile:
            root = self.cfg.data_dir.parent / "forex_m1_scout_triggers"
            try:
                paths.extend(sorted(root.glob("*/pair_score_summary_score70plus.csv"), key=lambda x: str(x), reverse=True)[:3])
            except Exception:
                pass
        return paths

    def load_scout_pair_profiles(self) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        if self._scout_profile_cache and now - self._scout_profile_loaded_at < max(30, self.cfg.scout_profile_refresh_seconds):
            return self._scout_profile_cache
        profiles: Dict[str, Dict[str, Any]] = {}
        for inst, rec in (self.cfg.scout_profile_fallback or {}).items():
            ninst = normalize_instrument(inst)
            if ninst and isinstance(rec, dict):
                profiles[ninst] = dict(rec)
        for path in self.scout_profile_candidate_paths():
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8-sig", newline="") as f:
                    for row in csv.DictReader(f):
                        inst = normalize_instrument(row.get("instrument", ""))
                        if not inst:
                            continue
                        profiles[inst] = {
                            "rows": safe_int(row.get("rows")),
                            "avg_score": safe_float(row.get("avg_score"), 0.0),
                            "avg_best_past_net": safe_float(row.get("avg_best_past_net"), 0.0),
                            "avg_spread": safe_float(row.get("avg_spread"), 0.0),
                            "avg_follow_5m": safe_float(row.get("avg_follow_5m"), 0.0),
                            "win_rate_5m": safe_float(row.get("win_rate_5m"), 0.0),
                            "avg_follow_10m": safe_float(row.get("avg_follow_10m"), 0.0),
                            "win_rate_10m": safe_float(row.get("win_rate_10m"), 0.0),
                            "avg_follow_15m": safe_float(row.get("avg_follow_15m"), 0.0),
                            "win_rate_15m": safe_float(row.get("win_rate_15m"), 0.0),
                            "avg_follow_30m": safe_float(row.get("avg_follow_30m"), 0.0),
                            "win_rate_30m": safe_float(row.get("win_rate_30m"), 0.0),
                            "avg_follow_60m": safe_float(row.get("avg_follow_60m"), 0.0),
                            "win_rate_60m": safe_float(row.get("win_rate_60m"), 0.0),
                            "source_path": str(path),
                        }
                break
            except Exception as exc:
                self.log_error(f"load scout profile {path}", exc)
        self._scout_profile_cache = profiles
        self._scout_profile_loaded_at = now
        return profiles

    def scout_profile_for(self, instrument: str) -> Dict[str, Any]:
        return dict(self.load_scout_pair_profiles().get(normalize_instrument(instrument), {}))

    def scout_profile_is_positive(self, prof: Dict[str, Any]) -> bool:
        rows = safe_int(prof.get("rows"), 0)
        if rows < max(1, self.cfg.scout_profile_min_rows):
            return False
        ev15 = safe_float(prof.get("avg_follow_15m"), 0.0)
        ev30 = safe_float(prof.get("avg_follow_30m"), 0.0)
        wr30 = safe_float(prof.get("win_rate_30m"), 0.0)
        return (ev15 >= 2.0 or ev30 >= 5.0) and wr30 >= 0.45

    def scout_profile_is_negative(self, prof: Dict[str, Any]) -> bool:
        rows = safe_int(prof.get("rows"), 0)
        if rows < max(1, self.cfg.scout_profile_min_rows):
            return False
        ev15 = safe_float(prof.get("avg_follow_15m"), 0.0)
        ev30 = safe_float(prof.get("avg_follow_30m"), 0.0)
        wr15 = safe_float(prof.get("win_rate_15m"), 0.0)
        return ev15 <= -5.0 and ev30 <= self.cfg.scout_late_exhaustion_profile_ev30_max and wr15 <= 0.25

    def event_signal_key(self, theme: str, sig: Dict[str, Any]) -> str:
        return f"{theme}:{normalize_instrument(sig.get('instrument'))}:{str(sig.get('direction','')).upper()}"

    def update_event_signal_memory(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not signals:
            return signals
        state = self.load_state()
        mem = state.get("event_signal_memory", {})
        if not isinstance(mem, dict):
            mem = {}
        now = utc_now()
        reset_min = max(5.0, self.cfg.scout_event_memory_reset_minutes)
        enriched: List[Dict[str, Any]] = []
        for sig in signals:
            theme = str(sig.get("theme", ""))
            key = self.event_signal_key(theme, sig)
            rec = mem.get(key) if isinstance(mem.get(key), dict) else None
            first_seen = now
            trigger_count = 0
            if rec:
                try:
                    first_seen = dt.datetime.fromisoformat(str(rec.get("first_seen_utc", "")).replace("Z", "+00:00")).astimezone(UTC)
                except Exception:
                    first_seen = now
                age_min = (now - first_seen).total_seconds() / 60.0
                if age_min > reset_min:
                    first_seen = now
                    trigger_count = 0
                else:
                    trigger_count = safe_int(rec.get("trigger_count"), 0)
            age_min = max(0.0, (now - first_seen).total_seconds() / 60.0)
            trigger_count += 1
            max_net_seen = max(safe_float((rec or {}).get("max_net_pips"), -1e9), safe_float(sig.get("net_pips"), 0.0))
            mem[key] = {
                "first_seen_utc": first_seen.isoformat(),
                "last_seen_utc": now.isoformat(),
                "trigger_count": trigger_count,
                "max_net_pips": max_net_seen,
            }
            sig2 = dict(sig)
            sig2["event_age_minutes"] = age_min
            sig2["event_trigger_count"] = trigger_count
            sig2["event_key"] = key
            enriched.append(sig2)
        # trim old memory
        trimmed: Dict[str, Any] = {}
        for key, rec in mem.items():
            try:
                last = dt.datetime.fromisoformat(str(rec.get("last_seen_utc", "")).replace("Z", "+00:00")).astimezone(UTC)
                if (now - last).total_seconds() / 60.0 <= reset_min * 2:
                    trimmed[key] = rec
            except Exception:
                continue
        state["event_signal_memory"] = dict(list(trimmed.items())[-1000:])
        self.save_state(state)
        return enriched

    def scout_ev_score_and_risk(self, theme: str, sig: Dict[str, Any], basket_count: int) -> Tuple[bool, float, float, str]:
        if not self.cfg.scout_ev_scoring_enabled:
            return True, self.cfg.event_scout_risk_pct, 75.0, "EV scoring disabled; using base scout risk"
        inst = normalize_instrument(sig.get("instrument"))
        net = safe_float(sig.get("net_pips"), 0.0)
        spread = max(0.1, safe_float(sig.get("spread_avg_pips"), 0.1))
        ratio = safe_float(sig.get("move_to_spread_ratio"), net / spread)
        threshold = max(0.1, self.event_threshold_pips(inst))
        age = safe_float(sig.get("event_age_minutes"), 0.0)
        repeats = safe_int(sig.get("event_trigger_count"), 1)
        volatile = self.is_volatile_scout_pair(inst)
        prof = self.scout_profile_for(inst)
        prof_rows = safe_int(prof.get("rows"), 0)
        prof_pos = self.scout_profile_is_positive(prof)
        prof_neg = self.scout_profile_is_negative(prof)
        ev15 = safe_float(prof.get("avg_follow_15m"), 0.0)
        ev30 = safe_float(prof.get("avg_follow_30m"), 0.0)
        wr30 = safe_float(prof.get("win_rate_30m"), 0.0)

        if volatile:
            if basket_count < max(1, self.cfg.scout_volatile_min_basket_pairs):
                return False, 0.0, 0.0, f"volatile scout basket below min {basket_count} < {self.cfg.scout_volatile_min_basket_pairs}"
            if net < self.cfg.scout_volatile_min_net_pips:
                return False, 0.0, 0.0, f"volatile scout net pips too low {net:.1f} < {self.cfg.scout_volatile_min_net_pips:.1f}"
            if ratio < self.cfg.scout_volatile_min_move_to_spread_ratio:
                return False, 0.0, 0.0, f"volatile scout move/spread too low {ratio:.2f} < {self.cfg.scout_volatile_min_move_to_spread_ratio:.2f}"
            if repeats > max(1, self.cfg.scout_volatile_repeat_trigger_cap):
                return False, 0.0, 0.0, f"volatile repeat trigger cap hit for {sig.get('event_key','event')} repeats={repeats}"
            if age > self.cfg.scout_volatile_max_event_age_minutes:
                return False, 0.0, 0.0, f"volatile event too old for continuation age={age:.1f}m > {self.cfg.scout_volatile_max_event_age_minutes:.1f}m"
            if prof_rows < self.cfg.scout_profile_min_rows and not self.cfg.scout_volatile_allow_unknown_profile:
                return False, 0.0, 0.0, "volatile scout unknown profile blocked"
            if prof_neg and not self.cfg.scout_volatile_allow_negative_profile_if_early:
                return False, 0.0, 0.0, f"volatile scout negative profile blocked ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}"

            move_score = clamp((ratio - self.cfg.scout_volatile_min_move_to_spread_ratio) / 5.0 * 30.0, 0.0, 30.0)
            net_score = clamp(net / max(self.cfg.scout_volatile_min_net_pips, 1.0) * 22.0, 0.0, 28.0)
            basket_score = clamp(12.0 + max(0, basket_count - self.cfg.scout_volatile_min_basket_pairs) * 5.0, 0.0, 22.0)
            early_score = clamp(18.0 - max(0.0, age) * 0.9, 0.0, 18.0)
            if prof_rows >= self.cfg.scout_profile_min_rows:
                # Do not let a stale one-day profile fully veto very early volatile participation.
                profile_score = clamp((ev15 / max(threshold, 1.0)) * 6.0 + (ev30 / max(threshold, 1.0)) * 8.0 + (wr30 - 0.40) * 18.0, -10.0, 16.0)
            else:
                profile_score = 2.0
            score = clamp(move_score + net_score + basket_score + early_score + profile_score, 0.0, 100.0)
            if score < self.cfg.scout_volatile_min_ev_score_to_trade:
                return False, 0.0, score, f"volatile scout score too low {score:.1f} < {self.cfg.scout_volatile_min_ev_score_to_trade:.1f}; age={age:.1f}m ratio={ratio:.2f} ev15={ev15:.1f} ev30={ev30:.1f}"
            risk = self.cfg.scout_volatile_base_risk_pct
            if score >= 90:
                risk = max(risk, min(self.cfg.scout_volatile_max_risk_pct, 0.50))
            elif score >= 80:
                risk = max(risk, min(self.cfg.scout_volatile_max_risk_pct, 0.35))
            elif score >= 70:
                risk = max(risk, min(self.cfg.scout_volatile_max_risk_pct, 0.25))
            # Positive profile can modestly increase risk, but keep volatile cap tight.
            if prof_pos and ev30 >= 10.0 and wr30 >= 0.50:
                risk = min(self.cfg.scout_volatile_max_risk_pct, risk + 0.10)
            if prof_neg:
                risk = min(risk, max(0.10, self.cfg.scout_volatile_base_risk_pct))
            reason = f"volatile_lane ev_score={score:.1f} age={age:.1f}m repeats={repeats} ratio={ratio:.2f} basket={basket_count} profile_rows={prof_rows} ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}"
            return True, max(0.0, min(risk, self.cfg.scout_volatile_max_risk_pct, self.cfg.max_risk_pct_per_trade)), score, reason

        if repeats > max(1, self.cfg.scout_max_repeat_triggers_per_key):
            return False, 0.0, 0.0, f"repeat trigger cap hit for {sig.get('event_key','event')} repeats={repeats}"
        if self.cfg.scout_skip_late_exhaustion and age > self.cfg.scout_late_exhaustion_age_minutes and prof_neg:
            return False, 0.0, 0.0, f"late exhaustion skip: age={age:.1f}m profile_ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}"
        move_score = clamp((ratio - self.cfg.event_min_move_to_spread_ratio) / 4.0 * 25.0, 0.0, 25.0)
        net_score = clamp(net / threshold * 20.0, 0.0, 25.0)
        basket_score = clamp(10.0 + max(0, basket_count - self.cfg.event_min_basket_pairs) * 5.0, 0.0, 20.0)
        early_score = 20.0 if age <= self.cfg.scout_max_event_age_minutes_for_continuation else clamp(20.0 - (age - self.cfg.scout_max_event_age_minutes_for_continuation) * 1.5, 0.0, 20.0)
        if prof_rows >= self.cfg.scout_profile_min_rows:
            profile_score = clamp((ev15 / max(threshold, 1.0)) * 10.0 + (ev30 / max(threshold, 1.0)) * 10.0 + (wr30 - 0.45) * 30.0, -20.0, 20.0)
        else:
            profile_score = 3.0 if self.cfg.scout_allow_unknown_profile_base_risk else -12.0
        score = clamp(move_score + net_score + basket_score + early_score + profile_score, 0.0, 100.0)
        if score < self.cfg.scout_min_ev_score_to_trade:
            return False, 0.0, score, f"scout EV score too low {score:.1f} < {self.cfg.scout_min_ev_score_to_trade:.1f}; age={age:.1f}m ratio={ratio:.2f} ev15={ev15:.1f} ev30={ev30:.1f}"
        if score >= 95:
            risk = self.cfg.scout_risk_pct_score_95
        elif score >= 90:
            risk = self.cfg.scout_risk_pct_score_90
        elif score >= 80:
            risk = self.cfg.scout_risk_pct_score_80
        else:
            risk = self.cfg.scout_risk_pct_score_70
        if prof_rows < self.cfg.scout_profile_min_rows:
            risk = min(risk, self.cfg.scout_max_risk_pct_with_unknown_profile)
        elif prof_neg:
            risk = min(risk, self.cfg.scout_max_risk_pct_with_negative_profile)
        elif self.cfg.scout_require_positive_profile_for_risk_above_base and not prof_pos:
            risk = min(risk, self.cfg.event_scout_risk_pct)
        reason = f"ev_score={score:.1f} age={age:.1f}m repeats={repeats} ratio={ratio:.2f} basket={basket_count} profile_rows={prof_rows} ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}"
        return True, max(0.0, min(risk, self.cfg.max_risk_pct_per_trade)), score, reason

    def log_event_window(self, sig: Dict[str, Any], theme: str = "") -> None:
        append_csv(self.cfg.event_windows_csv, {
            "time_utc": iso_utc(), "time_ny": iso_ny(),
            "instrument": sig.get("instrument", ""), "theme": theme,
            "window_minutes": sig.get("window_minutes", ""), "direction": sig.get("direction", ""),
            "net_pips": sig.get("net_pips", ""), "mid_move_pips": sig.get("mid_move_pips", ""),
            "spread_avg_pips": sig.get("spread_avg_pips", ""), "move_to_spread_ratio": sig.get("move_to_spread_ratio", ""),
            "start_utc": sig.get("start_utc", ""), "end_utc": sig.get("end_utc", ""),
            "raw_json": json.dumps(sig, default=str)[:12000],
        }, EVENT_WINDOW_FIELDS)


    def market_movement_key(self, sig: Dict[str, Any]) -> str:
        """Stable key for the price movement itself, independent of account or strategy."""
        inst = normalize_instrument(sig.get("instrument", ""))
        direction = str(sig.get("direction", "")).upper()
        payload = {
            "instrument": inst,
            "direction": direction,
            "window_minutes": str(sig.get("window_minutes", "")),
            "start_utc": str(sig.get("start_utc", "")),
            "end_utc": str(sig.get("end_utc", "")),
        }
        return stable_hash(payload)

    def log_market_movement(self, theme: str, status: str, sig: Dict[str, Any], *, signal_rank: int = 0, signal_count: int = 0, reason: str = "") -> str:
        """Append one account-independent movement observation to the shared movement ledger."""
        inst = normalize_instrument(sig.get("instrument", ""))
        if not inst:
            return ""
        base, quote = split_instrument(inst)
        movement_key = self.market_movement_key(sig)
        raw = dict(sig)
        raw["movement_key"] = movement_key
        append_csv(self.cfg.market_movements_csv, {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "source_account_lane": getattr(self.cfg, "account_lane", ""),
            "source_account_name": getattr(self.cfg, "account_display_name", ""),
            "source_script": Path(__file__).name,
            "movement_key": movement_key,
            "instrument": inst,
            "base_currency": base,
            "quote_currency": quote,
            "direction": str(sig.get("direction", "")).upper(),
            "theme": theme,
            "status": status,
            "window_minutes": sig.get("window_minutes", ""),
            "net_pips": sig.get("net_pips", ""),
            "mid_move_pips": sig.get("mid_move_pips", ""),
            "spread_avg_pips": sig.get("spread_avg_pips", ""),
            "move_to_spread_ratio": sig.get("move_to_spread_ratio", ""),
            "start_utc": sig.get("start_utc", ""),
            "end_utc": sig.get("end_utc", ""),
            "signal_rank": signal_rank,
            "signal_count": signal_count,
            "reason": reason[:1000],
            "raw_json": json.dumps(raw, default=str)[:12000],
        }, MARKET_MOVEMENT_FIELDS)
        try:
            self.full_logger.log_event("movement_detected", status=status, instrument=inst, direction=str(sig.get("direction", "")).upper(), movement_key=movement_key, reason=reason, raw=raw)
        except Exception:
            pass
        return movement_key

    def log_movement_capture(self, action: Dict[str, Any], action_type: str, status: str, *, units: Any = "", risk_usd: Any = "", margin_usd: Any = "", fill_price: Any = "", reject_reason: str = "", raw_extra: Optional[Dict[str, Any]] = None) -> None:
        """Append account capture/skip evidence for a movement-linked action."""
        movement_key = str(action.get("_movement_key", "") or "").strip()
        if not movement_key:
            return
        inst = normalize_instrument(action.get("instrument", ""))
        base, quote = split_instrument(inst)
        raw = {"action": action, "result": raw_extra or {}}
        append_csv(self.cfg.movement_capture_csv, {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "source_account_lane": getattr(self.cfg, "account_lane", ""),
            "source_account_name": getattr(self.cfg, "account_display_name", ""),
            "source_script": Path(__file__).name,
            "movement_key": movement_key,
            "instrument": inst,
            "base_currency": base,
            "quote_currency": quote,
            "direction": str(action.get("direction", "")).upper(),
            "theme": action.get("_movement_theme", action.get("_event_theme", "")),
            "action_type": action_type,
            "status": status,
            "trade_id": action.get("trade_id", ""),
            "units": units,
            "risk_pct": action.get("risk_pct", ""),
            "risk_usd": risk_usd,
            "margin_usd": margin_usd,
            "fill_price": fill_price,
            "reason": str(action.get("reason", action.get("why_now", "")))[:1000],
            "reject_reason": reject_reason,
            "raw_json": json.dumps(raw, default=str)[:12000],
        }, MOVEMENT_CAPTURE_FIELDS)
        try:
            evt = "movement_captured" if str(status).lower() in {"accepted", "accepted_with_cancel", "accepted_after_recheck", "dry_run"} else "movement_missed"
            self.full_logger.log_event(evt, status=status, instrument=inst, direction=str(action.get("direction", "")).upper(), trade_id=action.get("trade_id", ""), units=units, price=fill_price, movement_key=movement_key, reason=str(action.get("reason", action.get("why_now", "")))[:1000], result=reject_reason, raw=raw)
        except Exception:
            pass

    def log_event_signal(self, theme: str, status: str, signals: List[Dict[str, Any]], *, triggered_gpt: bool = False, scout_attempts: int = 0, reason: str = "") -> None:
        best = max(signals, key=lambda x: safe_float(x.get("net_pips"), 0.0)) if signals else {}
        append_csv(self.cfg.event_signals_csv, {
            "time_utc": iso_utc(), "time_ny": iso_ny(), "theme": theme, "status": status,
            "signal_count": len(signals), "best_instrument": best.get("instrument", ""),
            "best_direction": best.get("direction", ""), "best_window_minutes": best.get("window_minutes", ""),
            "best_net_pips": best.get("net_pips", ""), "best_spread_pips": best.get("spread_avg_pips", ""),
            "triggered_gpt": triggered_gpt, "scout_attempts": scout_attempts, "reason": reason,
            "raw_json": json.dumps({"signals": signals[:25]}, default=str)[:12000],
        }, EVENT_SIGNAL_FIELDS)
        for rank, sig in enumerate(signals, start=1):
            try:
                self.log_market_movement(theme, status, sig, signal_rank=rank, signal_count=len(signals), reason=reason)
            except Exception as exc:
                try:
                    self.cfg.errors_log.parent.mkdir(parents=True, exist_ok=True)
                    self.cfg.errors_log.write_text(error_text("log_market_movement", exc), encoding="utf-8")
                except Exception:
                    pass
        log(f"[EVENT] {status} theme={theme} signals={len(signals)} best={best.get('instrument','')} {best.get('direction','')} {safe_float(best.get('net_pips'),0):.1f}p")

    def maybe_event_trigger_gpt(self, theme: str, signals: List[Dict[str, Any]]) -> bool:
        if not self.cfg.event_trigger_gpt_enabled:
            return False
        state = self.load_state()
        last_raw = state.get("last_event_gpt_utc") or state.get("last_gpt_scan_utc")
        if last_raw:
            try:
                last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(UTC)
                age_min = (utc_now() - last).total_seconds() / 60.0
                if age_min < max(1, self.cfg.event_min_minutes_between_gpt_scans):
                    return False
            except Exception:
                pass
        state["last_event_gpt_utc"] = iso_utc()
        self.save_state(state)
        log(f"Event-triggered GPT scan requested by {theme} basket ({len(signals)} signals).")
        self.run_gpt_scan(reason=f"event_trigger:{theme}")
        return True

    def build_event_scout_order(self, theme: str, sig: Dict[str, Any], prices: Dict[str, Dict[str, Any]], risk_pct: float, reason: str) -> Optional[Dict[str, Any]]:
        inst = normalize_instrument(sig.get("instrument"))
        direction = str(sig.get("direction", "")).upper()
        if inst not in self.instrument_meta or direction not in {"LONG", "SHORT"}:
            return None
        meta = self.instrument_meta[inst]
        price = prices.get(inst, {})
        bid, ask = bid_ask(price)
        if bid is None or ask is None:
            return None
        entry = ask if direction == "LONG" else bid
        stop_pips = self.event_stop_pips(inst, safe_float(sig.get("net_pips"), 0.0), safe_float(sig.get("spread_avg_pips"), 0.0))
        tp_pips = stop_pips * max(0.5, self.cfg.event_scout_take_profit_r_multiple)
        if direction == "LONG":
            stop_loss = entry - stop_pips * meta.pip_size
            take_profit = entry + tp_pips * meta.pip_size
        else:
            stop_loss = entry + stop_pips * meta.pip_size
            take_profit = entry - tp_pips * meta.pip_size
        return {
            "instrument": inst,
            "action": "OPEN",
            "direction": direction,
            "outlook_confidence": 0,
            "risk_pct": max(0.0, min(risk_pct, self.cfg.max_risk_pct_per_trade)),
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop_pips": self.event_trailing_pips(inst),
            "_event_scout": True,
            "_movement_key": self.market_movement_key(sig),
            "_movement_theme": theme,
            "_movement_window_minutes": sig.get("window_minutes", ""),
            "_movement_net_pips": sig.get("net_pips", ""),
            "_movement_start_utc": sig.get("start_utc", ""),
            "_movement_end_utc": sig.get("end_utc", ""),
            "_allow_sunday_reopen_open": cfg_bool("FOREX_REOPEN_ALLOW_CONFIRMED_EVENT_SCOUTS", default=True),
            "reason": f"local M1 event scout {theme}: {safe_float(sig.get('net_pips'),0):.1f} net pips over {sig.get('window_minutes')}m; {reason}",
        }

    def attempt_event_scout_trades(self, theme: str, signals: List[Dict[str, Any]], prices: Dict[str, Dict[str, Any]]) -> int:
        if not self.cfg.event_scout_trades_enabled:
            return 0
        if self.shutdown_mode_active() and self.cfg.shutdown_block_new_trades:
            return 0
        try:
            open_trades = self.oanda.get_open_trades()
        except Exception:
            open_trades = []
        open_instruments = {normalize_instrument(t.get("instrument")) for t in open_trades}
        attempts = 0
        total_risk = 0.0
        ranked: List[Tuple[float, Dict[str, Any], float, str]] = []
        basket_count = len({normalize_instrument(x.get("instrument")) for x in signals})
        for sig in signals:
            inst = normalize_instrument(sig.get("instrument"))
            if not inst or inst in open_instruments:
                continue
            allowed, perm_risk, why_perm = self.permission_allows_signal(theme, sig)
            if not allowed:
                self.log_action({"instrument": inst, "direction": sig.get("direction"), "action": "OPEN", "risk_pct": self.cfg.event_scout_risk_pct, "reason": f"event scout {theme}"}, "event_scout", "skipped", reject_reason=why_perm)
                continue
            ok_score, ev_risk, ev_score, why_score = self.scout_ev_score_and_risk(theme, sig, basket_count)
            if not ok_score:
                self.log_action({"instrument": inst, "direction": sig.get("direction"), "action": "OPEN", "risk_pct": ev_risk or self.cfg.event_scout_risk_pct, "reason": f"event scout {theme}"}, "event_scout", "skipped", reject_reason=why_score)
                continue
            risk_pct = min(ev_risk, perm_risk if self.cfg.event_require_gpt_permission else ev_risk)
            sig2 = dict(sig)
            sig2["scout_ev_score"] = ev_score
            ranked.append((ev_score, sig2, risk_pct, f"{why_perm}; {why_score}"))
        for ev_score, sig, risk_pct, why in sorted(ranked, key=lambda x: (x[0], safe_float(x[1].get("net_pips"), 0.0)), reverse=True):
            if attempts >= max(0, self.cfg.event_max_scout_trades_per_event):
                break
            inst = normalize_instrument(sig.get("instrument"))
            if not inst or inst in open_instruments:
                continue
            if total_risk + risk_pct > self.cfg.event_max_total_scout_risk_pct:
                break
            order = self.build_event_scout_order(theme, sig, prices, risk_pct, why)
            if not order:
                continue
            status = self.open_trade_from_order(order, prices)
            attempts += 1
            if status.startswith("accepted") or status == "dry_run":
                total_risk += risk_pct
                open_instruments.add(inst)
        return attempts

    def run_event_scan(self, reason: str = "interval") -> None:
        if not self.cfg.event_scanner_enabled or fx_market_closed() or self.sunday_reopen_event_scanner_blocked():
            return
        if not self.instruments:
            self.refresh_instruments()
        prices = self.oanda.get_prices(self.instruments)
        all_signals: List[Dict[str, Any]] = []
        for inst in self.instruments:
            meta = self.instrument_meta.get(inst)
            if not meta:
                continue
            price = prices.get(inst, {})
            if not price or not price_tradeable(price):
                continue
            spr_now = spread_pips(price, meta)
            if spr_now > self.event_max_spread_pips(inst):
                continue
            try:
                candles = self.oanda.get_candles(inst, self.cfg.event_candle_granularity, self.cfg.event_candle_count, price="BAM")
            except Exception as exc:
                self.log_error(f"event candles {inst}", exc)
                continue
            windows = self.best_recent_event_windows(inst, candles)
            if not windows:
                continue
            best = max(windows, key=lambda x: safe_float(x.get("net_pips"), -1e9))
            if safe_float(best.get("net_pips"), 0.0) < self.event_threshold_pips(inst):
                continue
            min_ratio = self.cfg.scout_volatile_min_move_to_spread_ratio if self.is_volatile_scout_pair(inst) else self.cfg.event_min_move_to_spread_ratio
            if safe_float(best.get("move_to_spread_ratio"), 0.0) < min_ratio:
                continue
            for theme in self.classify_event_signal_themes(best):
                best2 = dict(best)
                best2["theme"] = theme
                all_signals.append(best2)
                self.log_event_window(best2, theme)
        all_signals = self.update_event_signal_memory(all_signals)
        by_theme: Dict[str, List[Dict[str, Any]]] = {}
        for sig in all_signals:
            by_theme.setdefault(str(sig.get("theme", "")), []).append(sig)
        triggered_any = False
        for theme, signals in sorted(by_theme.items(), key=lambda kv: len(kv[1]), reverse=True):
            # Deduplicate instrument per theme; keep strongest window per instrument.
            best_by_inst: Dict[str, Dict[str, Any]] = {}
            for sig in signals:
                inst = normalize_instrument(sig.get("instrument"))
                if inst not in best_by_inst or safe_float(sig.get("net_pips"), 0.0) > safe_float(best_by_inst[inst].get("net_pips"), 0.0):
                    best_by_inst[inst] = sig
            compact = sorted(best_by_inst.values(), key=lambda x: safe_float(x.get("net_pips"), 0.0), reverse=True)
            if len(compact) < max(1, self.cfg.event_min_basket_pairs):
                self.log_event_signal(theme, "watch", compact, reason=f"basket below min {self.cfg.event_min_basket_pairs}")
                continue
            scout_attempts = self.attempt_event_scout_trades(theme, compact, prices)
            did_gpt = False
            if not triggered_any:
                try:
                    did_gpt = self.maybe_event_trigger_gpt(theme, compact)
                    triggered_any = did_gpt
                except Exception as exc:
                    self.log_error(f"event-triggered GPT {theme}", exc)
            self.log_event_signal(theme, "triggered", compact, triggered_gpt=did_gpt, scout_attempts=scout_attempts, reason=reason)
        state = self.load_state()
        state["last_event_scan_utc"] = iso_utc()
        if all_signals:
            state["last_event_signals"] = all_signals[-100:]
        self.save_state(state)

    def sync_transaction_ledger(self, account: Optional[Dict[str, Any]] = None) -> None:
        """Pull broker transactions since last seen transaction ID and log fills/cancels/closes."""
        state = self.load_state()
        last_id = str(state.get("last_transaction_id", "") or "").strip()
        account = account or {}
        current_last = str(account.get("lastTransactionID", "") or account.get("lastTransactionId", "") or "").strip()
        if not last_id:
            if current_last:
                state["last_transaction_id"] = current_last
                self.save_state(state)
            return
        if current_last and last_id == current_last:
            return
        try:
            data = self.oanda.get_transactions_since(last_id)
        except Exception as exc:
            self.full_logger.log_event("transaction_sync_error", status="error", severity="WARN", error_type=type(exc).__name__, error_message=str(exc)[:1000])
            return
        txs = data.get("transactions", []) or []
        seen = set(str(x) for x in state.get("seen_transaction_ids", [])[-5000:])
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            txid = str(tx.get("id", ""))
            if not txid or txid in seen:
                continue
            seen.add(txid)
            typ = str(tx.get("type", "")).upper()
            self.full_logger.log_event("transaction_seen", status=typ or "seen", transaction_id=txid, instrument=tx.get("instrument", ""), trade_id=tx.get("tradeID", ""), order_id=tx.get("orderID", ""), units=tx.get("units", ""), price=tx.get("price", ""), pl=tx.get("pl", ""), reason=tx.get("reason", ""), raw=tx)
            if "CANCEL" in typ:
                self.full_logger.log_cancel("transaction_sync_cancel", tx, reason=str(tx.get("reason", "")))
            if "FILL" in typ:
                self.full_logger.log_trade_lifecycle("transaction_order_fill", tx, reason=str(tx.get("reason", "")))
            if "CLOSE" in typ or "CLOSED" in typ:
                self.full_logger.log_trade_lifecycle("transaction_trade_close", tx, reason=str(tx.get("reason", "")))
        last_returned = str(data.get("lastTransactionID", "") or data.get("lastTransactionId", "") or current_last or last_id).strip()
        if last_returned:
            state["last_transaction_id"] = last_returned
        state["seen_transaction_ids"] = list(seen)[-5000:]
        self.save_state(state)

    def reconcile_pending_order_state(self, open_trades: List[Dict[str, Any]]) -> None:
        """Log child order cancellations/missing pending orders observed during monitor/reconcile."""
        current = _pending_orders_from_open_trades(open_trades)
        state = self.load_state()
        previous = state.get("known_pending_orders", {}) or {}
        seen_cancelled = set(str(x) for x in state.get("seen_cancelled_order_ids", [])[-5000:])
        for oid, prev in list(previous.items()):
            if oid not in current and oid not in seen_cancelled:
                prev = prev if isinstance(prev, dict) else {"id": oid}
                self.full_logger.log_cancel("reconcile_missing_pending_order", {**prev, "id": oid, "orderID": oid}, reason="previously known pending order no longer present during reconcile")
                seen_cancelled.add(oid)
        state["known_pending_orders"] = current
        state["seen_cancelled_order_ids"] = list(seen_cancelled)[-5000:]
        self.save_state(state)

    def run_local_monitor(self) -> None:
        account = self.oanda.get_account_summary()
        open_trades = self.oanda.get_open_trades()
        try:
            self.sync_transaction_ledger(account)
            self.reconcile_pending_order_state(open_trades)
        except Exception as exc:
            self.log_error("full logging transaction/reconcile pass", exc)
        summary = account_summary_for_prompt(account)
        try:
            self.full_logger.log_event("monitor_start", status="running", nav=summary.get("nav", ""), balance=summary.get("balance", ""), margin_used_pct=summary.get("margin_used_pct_of_nav", ""), raw={"account": summary, "open_trade_count": len(open_trades)})
        except Exception:
            pass
        self.update_trade_profit_memory(account, open_trades, reason="local_monitor")
        if cfg_bool("FOREX_SUNDAY_REOPEN_MANAGER_ENABLED", default=True):
            try:
                self.run_sunday_reopen_hard_protection(account=account, open_trades=open_trades)
            except Exception as exc:
                self.log_error("Sunday reopen hard protection from local monitor", exc)
        decision = "log_only"
        reason = "local monitor snapshot"
        if summary["margin_used_pct_of_nav"] >= self.cfg.emergency_margin_used_pct:
            decision = "emergency_margin_check"
            reason = f"margin_used_pct {summary['margin_used_pct_of_nav']:.2f} >= {self.cfg.emergency_margin_used_pct:.2f}"
            if self.cfg.local_emergency_close and open_trades:
                worst = sorted(open_trades, key=lambda t: safe_float(t.get("unrealizedPL")))[0]
                action = {
                    "instrument": worst.get("instrument"),
                    "trade_id": worst.get("id"),
                    "action": "CLOSE",
                    "direction": trade_direction_from_units(worst.get("currentUnits")),
                    "reason": "local emergency margin close",
                }
                try:
                    self.close_trade(worst, action)
                    decision = "emergency_close_worst_trade"
                except Exception as exc:
                    self.log_error("local emergency close", exc)
        if self.shutdown_mode_active() and open_trades:
            try:
                self.run_shutdown_fade_pass(reason="local_monitor", account=account, open_trades=open_trades)
                decision = "shutdown_fade_active" if decision == "log_only" else decision + "+shutdown_fade_active"
                reason = reason + "; shutdown fade pass active"
            except Exception as exc:
                self.log_error("shutdown fade pass from local monitor", exc)
        append_csv(self.cfg.monitor_csv, {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "nav": summary["nav"],
            "balance": summary["balance"],
            "margin_used": summary["margin_used"],
            "margin_available": summary["margin_available"],
            "margin_used_pct": summary["margin_used_pct_of_nav"],
            "open_trade_count": summary["open_trade_count"],
            "decision": decision,
            "reason": reason,
            "raw_json": json.dumps({"account": summary, "open_trades": [summarize_trade_for_prompt(t) for t in open_trades]}, default=str)[:12000],
        }, MONITOR_FIELDS)
        try:
            self.full_logger.log_event("monitor_result", status=decision, nav=summary.get("nav", ""), balance=summary.get("balance", ""), margin_used_pct=summary.get("margin_used_pct_of_nav", ""), reason=reason, raw={"account": summary, "open_trades": [summarize_trade_for_prompt(t) for t in open_trades]})
        except Exception:
            pass
        state = self.load_state()
        state["last_monitor_utc"] = iso_utc()
        self.save_state(state)
        self.write_daily_recaps_around_now()
        log(f"{self.lane_prefix()}Monitor: NAV={summary['nav']:.2f} margin_used_pct={summary['margin_used_pct_of_nav']:.2f} open_trades={summary['open_trade_count']}")

    def loop(self) -> None:
        self.validate_config()
        self.refresh_instruments()
        try:
            self.reconcile_startup_state()
        except Exception as exc:
            self.log_error("startup reconciliation", exc)
        log(f"{self.lane_prefix()}Starting always-on forex manager loop.")
        log(f"{self.lane_prefix()}Shared daily move comparison report: writer=True interval_minutes={AUTO_DAILY_REPORT_INTERVAL_MINUTES}")
        log(f"{self.lane_prefix()}OANDA env={self.cfg.oanda_env} account_name={self.cfg.account_display_name} instrument_filter={self.cfg.instrument_filter_mode} execution={'ENABLED' if self.should_execute() else 'DRY-RUN'}")
        log(f"{self.lane_prefix()}GPT call times NY={self.cfg.call_times_ny}")
        log(f"{self.lane_prefix()}Minimum minutes between GPT scans={self.cfg.min_minutes_between_gpt_scans}")
        log(f"{self.lane_prefix()}Event scanner enabled={self.cfg.event_scanner_enabled} interval_seconds={self.cfg.event_scan_interval_seconds} scout_trades={'ENABLED' if self.cfg.event_scout_trades_enabled else 'OFF'} require_permission={self.cfg.event_require_gpt_permission}")
        log(f"Sunday reopen manager enabled={cfg_bool('FOREX_SUNDAY_REOPEN_MANAGER_ENABLED', default=True)} summary_time={cfg_str('FOREX_WEEKEND_SUMMARY_SCAN_TIME_NY', default='16:45')} reopen_scan={cfg_str('FOREX_SUNDAY_REOPEN_SCAN_TIME_NY', default='17:15')} block_unrelated_new_trades_minutes={cfg_int('FOREX_SUNDAY_REOPEN_BLOCK_UNRELATED_NEW_TRADES_MINUTES', 'FOREX_SUNDAY_REOPEN_BLOCK_NEW_TRADES_MINUTES', default=20)}")
        if self.shutdown_mode_active():
            log(f"{self.lane_prefix()}Shutdown mode ACTIVE: {self.shutdown_mode_label()} new_opens_blocked={self.cfg.shutdown_block_new_trades} skip_gpt={self.cfg.shutdown_skip_gpt}")
        if self.cfg.friday_call_times_ny != self.cfg.call_times_ny:
            log(f"{self.lane_prefix()}Friday GPT call times NY={self.cfg.friday_call_times_ny}")
        if self.cfg.scan_on_launch and not fx_market_closed():
            try:
                self.run_gpt_scan(reason="launch")
                # v5.3: launch scans are separate from scheduled away-time scans.
                # They never mark, consume, or backfill scheduled GPT slots.
            except Exception as exc:
                self.log_error("launch GPT scan", exc)

        while True:
            try:
                state = self.load_state()
                maybe_auto_daily_move_report(account_lane="gpt", writer=True, force=False)
                if self.weekend_summary_due(state):
                    try:
                        self.run_weekend_summary_scan(reason="weekend_summary_preopen")
                    except Exception as exc:
                        self.log_error("weekend summary scan", exc)
                    state = self.load_state()
                if self.sunday_reopen_due(state):
                    try:
                        account = self.oanda.get_account_summary()
                        open_trades = self.oanda.get_open_trades()
                        self.update_trade_profit_memory(account, open_trades, reason="sunday_reopen_pre_scan")
                        self.run_sunday_reopen_hard_protection(account=account, open_trades=open_trades)
                        if cfg_bool("FOREX_SUNDAY_REOPEN_USE_GPT", default=True):
                            self.run_gpt_scan(reason="sunday_reopen")
                        self.mark_sunday_reopen_done()
                    except Exception as exc:
                        self.log_error("Sunday reopen manager", exc)
                    state = self.load_state()
                last_monitor = state.get("last_monitor_utc")
                due_monitor = True
                if last_monitor:
                    try:
                        last = dt.datetime.fromisoformat(str(last_monitor).replace("Z", "+00:00")).astimezone(UTC)
                        due_monitor = (utc_now() - last).total_seconds() >= self.cfg.local_monitor_interval_minutes * 60
                    except Exception:
                        due_monitor = True
                if due_monitor:
                    self.run_local_monitor()
                    state = self.load_state()

                if self.event_scan_due(state):
                    self.run_event_scan(reason="loop")
                    state = self.load_state()

                due, hhmm = self.gpt_due(state=state)
                if due:
                    self.run_gpt_scan(reason=f"scheduled {hhmm}")
                    state = self.load_state()
                    self.mark_gpt_run(hhmm, state)
            except KeyboardInterrupt:
                log("Stopped by user.")
                return
            except Exception as exc:
                self.log_error("main loop", exc)
            time.sleep(max(5, self.cfg.loop_sleep_seconds))




# =============================================================================
# Two-script automatic daily move/account comparison report
# =============================================================================
# This embedded reporter is intentionally stdlib-only and single-writer locked.
# Both account scripts may check report freshness, but only the configured writer
# generates/overwrites the shared daily comparison files. This keeps the whole
# stack to two runnable scripts while avoiding duplicate daily reports.

AUTO_DAILY_REPORT_ENABLED = True
AUTO_DAILY_REPORT_INTERVAL_MINUTES = 30
AUTO_DAILY_REPORT_STALE_WARN_MINUTES = 90
AUTO_DAILY_REPORT_WRITER_LANE = "gpt"  # advisor writes; technical only checks


def _auto_report_parse_iso(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _auto_report_today_ny() -> str:
    return ny_now().date().isoformat()


def _auto_report_data_root() -> Path:
    return SCRIPT_DIR / "data"


def _auto_report_root() -> Path:
    return _auto_report_data_root() / "forex" / "reports" / "daily_move_comparison"


def _auto_report_status_path() -> Path:
    return _auto_report_root() / "_auto_daily_report_status.json"


def _auto_report_date_from_row(row: Dict[str, Any]) -> str:
    tny = str(row.get("time_ny") or "").strip()
    if len(tny) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", tny[:10]):
        return tny[:10]
    tutc = _auto_report_parse_iso(row.get("time_utc"))
    if tutc:
        try:
            return tutc.astimezone(NY or UTC).date().isoformat()
        except Exception:
            return tutc.date().isoformat()
    return ""


def _auto_report_iter_csv_rows(path: Path, date_ny: str, max_rows: int = 250000):
    if not path.exists() or not path.is_file():
        return
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                if idx >= max_rows:
                    break
                if date_ny and _auto_report_date_from_row(row) != date_ny:
                    continue
                yield row
    except Exception:
        return


def _auto_report_bump(counter: Dict[str, int], key: Any, inc: int = 1) -> None:
    k = str(key or "").strip() or "(blank)"
    counter[k] = counter.get(k, 0) + inc


def _auto_report_top(counter: Dict[str, int], n: int = 20) -> List[Tuple[str, int]]:
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:n]


def _auto_report_write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _auto_report_extract_reason(row: Dict[str, Any]) -> str:
    return str(row.get("result") or row.get("reject_reason") or row.get("reason") or "").strip()


def _auto_report_classify_miss_reason(row: Dict[str, Any]) -> str:
    txt = _auto_report_extract_reason(row).lower()
    status = str(row.get("event_status") or row.get("status") or "").lower()
    if "margin" in txt or "soft cap" in txt:
        return "missed_margin_cap"
    if "minimum" in txt or "min_size" in txt or "rounded below" in txt:
        return "missed_min_units"
    if "spread" in txt or "ratio" in txt:
        return "missed_spread_or_ratio"
    if "opposite" in txt:
        return "missed_existing_opposite_trade"
    if "risk_pct below" in txt or "risk" in txt:
        return "missed_risk_gate"
    if "campaign" in txt or "repeat" in txt or "cooldown" in txt:
        return "missed_campaign_or_churn_gate"
    if "weekend" in txt or "flat" in txt:
        return "missed_weekend_flat"
    if "skip" in status or "miss" in status:
        return "missed_other_skip"
    return "missed_unclassified"


def generate_auto_daily_move_comparison_report(date_ny: Optional[str] = None) -> Path:
    date_ny = date_ny or _auto_report_today_ny()
    data_root = _auto_report_data_root()
    report_dir = _auto_report_root() / date_ny
    report_dir.mkdir(parents=True, exist_ok=True)

    journal_paths = sorted(data_root.rglob("full_event_journal.csv"))
    action_paths = sorted(data_root.rglob("actions.csv"))
    signal_paths = sorted(data_root.rglob("event_signals.csv"))
    scan_paths = sorted(data_root.rglob("event_scan_summary.csv"))

    event_counts: Dict[str, int] = {}
    status_counts: Dict[str, int] = {}
    lane_counts: Dict[str, int] = {}
    instrument_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    action_status_counts: Dict[str, int] = {}
    action_type_counts: Dict[str, int] = {}
    theme_counts: Dict[str, int] = {}
    missed_reason_counts: Dict[str, int] = {}
    movement_rows: List[Dict[str, Any]] = []
    missed_rows: List[Dict[str, Any]] = []
    captured_rows: List[Dict[str, Any]] = []
    order_rows: List[Dict[str, Any]] = []
    cancel_rows: List[Dict[str, Any]] = []

    for path in journal_paths:
        source = str(path.relative_to(data_root)) if str(path).startswith(str(data_root)) else str(path)
        for row in _auto_report_iter_csv_rows(path, date_ny):
            et = str(row.get("event_type") or "").strip()
            es = str(row.get("event_status") or "").strip()
            lane = str(row.get("account_lane") or "").strip() or "unknown"
            inst = str(row.get("instrument") or "").strip()
            direction = str(row.get("direction") or "").strip()
            reason = _auto_report_extract_reason(row)
            _auto_report_bump(event_counts, et)
            _auto_report_bump(status_counts, es)
            _auto_report_bump(lane_counts, lane)
            if inst:
                _auto_report_bump(instrument_counts, inst)
            if reason:
                _auto_report_bump(reason_counts, reason)
            if et in {"movement_detected", "pre_spike_pressure", "movement_campaign"}:
                movement_rows.append({
                    "time_ny": row.get("time_ny", ""), "lane": lane, "event_type": et, "status": es,
                    "instrument": inst, "direction": direction, "movement_key": row.get("movement_key", ""),
                    "reason": reason, "source_file": source,
                })
            if et in {"movement_missed", "action_logged"} and ("skip" in es.lower() or "miss" in et.lower() or "skipped" in es.lower()):
                miss_reason = _auto_report_classify_miss_reason(row)
                _auto_report_bump(missed_reason_counts, miss_reason)
                missed_rows.append({
                    "time_ny": row.get("time_ny", ""), "lane": lane, "instrument": inst, "direction": direction,
                    "event_type": et, "status": es, "miss_reason": miss_reason, "detail": reason,
                    "movement_key": row.get("movement_key", ""), "source_file": source,
                })
            if et == "movement_captured" or es in {"accepted", "accepted_first_entry"}:
                captured_rows.append({
                    "time_ny": row.get("time_ny", ""), "lane": lane, "instrument": inst, "direction": direction,
                    "event_type": et, "status": es, "trade_id": row.get("trade_id", ""),
                    "order_id": row.get("order_id", ""), "units": row.get("units", ""), "price": row.get("price", ""),
                    "movement_key": row.get("movement_key", ""), "reason": reason, "source_file": source,
                })
            if et in {"order_result", "trade_lifecycle"}:
                order_rows.append({
                    "time_ny": row.get("time_ny", ""), "lane": lane, "instrument": inst, "direction": direction,
                    "event_type": et, "status": es, "trade_id": row.get("trade_id", ""),
                    "order_id": row.get("order_id", ""), "transaction_id": row.get("transaction_id", ""),
                    "units": row.get("units", ""), "price": row.get("price", ""), "pl": row.get("pl", ""),
                    "reason": reason, "source_file": source,
                })
            if et == "order_cancel" or "cancel" in es.lower():
                cancel_rows.append({
                    "time_ny": row.get("time_ny", ""), "lane": lane, "instrument": inst,
                    "trade_id": row.get("trade_id", ""), "order_id": row.get("order_id", ""),
                    "transaction_id": row.get("transaction_id", ""), "status": es, "reason": reason,
                    "source_file": source,
                })

    for path in action_paths:
        source = str(path.relative_to(data_root)) if str(path).startswith(str(data_root)) else str(path)
        for row in _auto_report_iter_csv_rows(path, date_ny):
            _auto_report_bump(action_status_counts, row.get("status"))
            _auto_report_bump(action_type_counts, row.get("action_type"))
            status = str(row.get("status") or "").lower()
            if status == "skipped":
                miss_reason = _auto_report_classify_miss_reason(row)
                _auto_report_bump(missed_reason_counts, miss_reason)
                missed_rows.append({
                    "time_ny": row.get("time_ny", ""), "lane": "actions", "instrument": row.get("instrument", ""),
                    "direction": row.get("direction", ""), "event_type": row.get("action_type", ""), "status": row.get("status", ""),
                    "miss_reason": miss_reason, "detail": row.get("reject_reason") or row.get("reason") or "",
                    "movement_key": row.get("movement_key", ""), "source_file": source,
                })

    for path in signal_paths:
        for row in _auto_report_iter_csv_rows(path, date_ny, max_rows=300000):
            th = str(row.get("theme") or row.get("event_theme") or row.get("reason") or "").strip()
            if th:
                _auto_report_bump(theme_counts, th)

    # keep CSVs bounded and readable
    movement_rows = movement_rows[-20000:]
    missed_rows = missed_rows[-20000:]
    captured_rows = captured_rows[-10000:]
    order_rows = order_rows[-10000:]
    cancel_rows = cancel_rows[-10000:]

    _auto_report_write_csv(report_dir / "daily_market_moves.csv", movement_rows,
                           ["time_ny", "lane", "event_type", "status", "instrument", "direction", "movement_key", "reason", "source_file"])
    _auto_report_write_csv(report_dir / "missed_moves.csv", missed_rows,
                           ["time_ny", "lane", "instrument", "direction", "event_type", "status", "miss_reason", "detail", "movement_key", "source_file"])
    _auto_report_write_csv(report_dir / "captured_moves.csv", captured_rows,
                           ["time_ny", "lane", "instrument", "direction", "event_type", "status", "trade_id", "order_id", "units", "price", "movement_key", "reason", "source_file"])
    _auto_report_write_csv(report_dir / "order_trade_timeline.csv", order_rows,
                           ["time_ny", "lane", "instrument", "direction", "event_type", "status", "trade_id", "order_id", "transaction_id", "units", "price", "pl", "reason", "source_file"])
    _auto_report_write_csv(report_dir / "order_cancel_timeline.csv", cancel_rows,
                           ["time_ny", "lane", "instrument", "trade_id", "order_id", "transaction_id", "status", "reason", "source_file"])
    _auto_report_write_csv(report_dir / "miss_reason_summary.csv",
                           [{"miss_reason": k, "count": v} for k, v in _auto_report_top(missed_reason_counts, 100)],
                           ["miss_reason", "count"])
    _auto_report_write_csv(report_dir / "theme_summary.csv",
                           [{"theme": k, "count": v} for k, v in _auto_report_top(theme_counts, 100)],
                           ["theme", "count"])
    _auto_report_write_csv(report_dir / "account_capture_comparison.csv",
                           [{"metric": "event_type:" + k, "count": v} for k, v in _auto_report_top(event_counts, 100)]
                           + [{"metric": "status:" + k, "count": v} for k, v in _auto_report_top(status_counts, 100)]
                           + [{"metric": "lane:" + k, "count": v} for k, v in _auto_report_top(lane_counts, 100)]
                           + [{"metric": "action_status:" + k, "count": v} for k, v in _auto_report_top(action_status_counts, 100)],
                           ["metric", "count"])

    lines = []
    lines.append(f"Daily move/account comparison report — {date_ny}")
    lines.append(f"Generated NY: {iso_ny()}")
    lines.append("")
    lines.append(f"Data root: {data_root}")
    lines.append(f"Journal files: {len(journal_paths)} | action files: {len(action_paths)} | signal files: {len(signal_paths)} | scan files: {len(scan_paths)}")
    lines.append("")
    lines.append("Key counts:")
    for label, counter in [("event_type", event_counts), ("status", status_counts), ("lane", lane_counts), ("action_status", action_status_counts), ("miss_reason", missed_reason_counts)]:
        lines.append(f"  {label}:")
        for k, v in _auto_report_top(counter, 15):
            lines.append(f"    {k}: {v}")
    lines.append("")
    lines.append("Top instruments by logged activity:")
    for k, v in _auto_report_top(instrument_counts, 25):
        lines.append(f"  {k}: {v}")
    lines.append("")
    lines.append("Most common skip/miss details:")
    for k, v in _auto_report_top(reason_counts, 20):
        lines.append(f"  {v}x — {k[:220]}")
    lines.append("")
    lines.append("Output files:")
    for name in ["daily_market_moves.csv", "captured_moves.csv", "missed_moves.csv", "miss_reason_summary.csv", "order_trade_timeline.csv", "order_cancel_timeline.csv", "theme_summary.csv", "account_capture_comparison.csv"]:
        lines.append(f"  {report_dir / name}")
    text = "\n".join(lines) + "\n"
    (report_dir / "daily_move_comparison_report.txt").write_text(text, encoding="utf-8")
    (report_dir / "daily_move_comparison_report.md").write_text("```text\n" + text + "```\n", encoding="utf-8")

    status = {
        "last_run_utc": utc_now().isoformat(),
        "last_run_ny": iso_ny(),
        "date_ny": date_ny,
        "report_dir": str(report_dir),
        "writer_lane": AUTO_DAILY_REPORT_WRITER_LANE,
        "event_counts": event_counts,
        "status_counts": status_counts,
        "lane_counts": lane_counts,
    }
    _auto_report_status_path().parent.mkdir(parents=True, exist_ok=True)
    _auto_report_status_path().write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    return report_dir


def maybe_auto_daily_move_report(account_lane: str = "", writer: bool = False, force: bool = False) -> None:
    if not AUTO_DAILY_REPORT_ENABLED:
        return
    root = _auto_report_root()
    root.mkdir(parents=True, exist_ok=True)
    status_path = _auto_report_status_path()
    now = utc_now()
    stale = True
    last_run = None
    if status_path.exists():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
            last_run = _auto_report_parse_iso(status.get("last_run_utc"))
            stale = not last_run or (now - last_run.astimezone(UTC)).total_seconds() >= AUTO_DAILY_REPORT_INTERVAL_MINUTES * 60
        except Exception:
            stale = True
    if not writer:
        if last_run is None or (now - last_run.astimezone(UTC)).total_seconds() >= AUTO_DAILY_REPORT_STALE_WARN_MINUTES * 60:
            log(f"[{account_lane or 'account'}] Daily comparison report stale/missing; writer lane should update it. status={status_path}")
        return
    if not (force or stale):
        return
    lock_path = root / f"daily_report_{_auto_report_today_ny()}.lock"
    fd = None
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"pid={os.getpid()} time={utc_now().isoformat()} lane={account_lane}\n".encode("utf-8"))
        os.close(fd); fd = None
        report_dir = generate_auto_daily_move_comparison_report(_auto_report_today_ny())
        log(f"[{account_lane or 'writer'}] Wrote shared daily move comparison report: {report_dir}")
    except FileExistsError:
        return
    except Exception as exc:
        log(f"[{account_lane or 'writer'}] Daily comparison report failed: {type(exc).__name__}: {exc}")
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        try:
            if lock_path.exists():
                lock_path.unlink()
        except Exception:
            pass


# =============================================================================
# Result helpers
# =============================================================================

def as_optional_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, str) and x.strip().lower() in {"", "none", "null", "na", "n/a"}:
        return None
    try:
        val = float(x)
        if not math.isfinite(val):
            return None
        return val
    except Exception:
        return None


def first_optional_float(*xs: Any) -> Optional[float]:
    for x in xs:
        val = as_optional_float(x)
        if val is not None:
            return val
    return None


def order_status_from_result(result: Dict[str, Any]) -> str:
    if result.get("orderRejectTransaction"):
        return "rejected"
    if result.get("orderFillTransaction"):
        if result.get("orderCancelTransaction"):
            return "accepted_with_cancel"
        return "accepted"
    if result.get("orderCancelTransaction"):
        return "cancelled"
    if result.get("skipped"):
        return "skipped"
    return "unknown"


def order_fill_price(result: Dict[str, Any]) -> str:
    fill = result.get("orderFillTransaction") or {}
    return str(fill.get("price", ""))


# =============================================================================
# Single-account GPT/advisor all-pairs helpers
# =============================================================================

def with_data_dir(cfg: BotConfig, data_dir: Path) -> BotConfig:
    data_dir = Path(data_dir)
    return replace(
        cfg,
        data_dir=data_dir,
        state_path=data_dir / "state.json",
        decisions_csv=data_dir / "decisions.csv",
        candidates_csv=data_dir / "candidates.csv",
        actions_csv=data_dir / "actions.csv",
        monitor_csv=data_dir / "monitor.csv",
        event_signals_csv=data_dir / "event_signals.csv",
        event_windows_csv=data_dir / "event_windows.csv",
        market_movements_csv=cfg.market_movements_csv,
        movement_capture_csv=cfg.movement_capture_csv,
        errors_log=data_dir / "errors.log",
        raw_dir=data_dir / "raw_decisions",
        daily_recap_dir=data_dir / "daily_recaps",
        daily_recap_index=data_dir / "daily_recaps.jsonl",
    )


def build_account_lane_configs(base_cfg: BotConfig) -> List[BotConfig]:
    """Force this script to the GPT/news/advisor account only.

    Credentials:
        OANDA_ACCOUNT_ID_GPT = "..."   # optional clearer alias
        OANDA_ACCOUNT_ID_MAJ = "..."   # existing supported name

    This is a strategy account, not a major-USD account. By default it routes
    all discovered OANDA currency pairs to GPT/news/advisor management. The
    separate technical scout script may also scan any pair, but it trades for a
    different reason: local spike/continuation capture.
    """
    gpt_id = cfg_str(
        "OANDA_ACCOUNT_ID_GPT",
        "OANDA_ACCOUNT_ID_ADVISOR",
        "OANDA_ACCOUNT_ID_MAJ",
        "OANDA_ACCOUNT_ID_MAJOR",
        "OANDA_ACCOUNT_ID_USD_MAJORS",
        default="",
    )
    if not gpt_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_ID_GPT or OANDA_ACCOUNT_ID_MAJ in creds. This GPT/advisor script intentionally does not fall back to OANDA_ACCOUNT_ID.")

    exclude = {normalize_instrument(x) for x in cfg_list("FOREX_GPT_EXCLUDE_PAIRS", default=[]) if normalize_instrument(x)}
    # The instrument filter remains 'all' so OANDA discovery determines the active universe.
    # Any explicit GPT exclusions are removed after discovery by instrument_allowed_for_lane.
    cfg = with_data_dir(replace(
        base_cfg,
        oanda_account_id=gpt_id,
        account_lane="gpt",
        account_display_name="OANDA_ACCOUNT_ID_GPT/OANDA_ACCOUNT_ID_MAJ",
        instrument_filter_mode="all_except_gpt_exclude" if exclude else "all",
        gpt_reliable_pairs=[],
        technical_scout_pairs=[],
        major_usd_pairs=[normalize_instrument(x) for x in base_cfg.major_usd_pairs if normalize_instrument(x)],
        event_scanner_enabled=False,
        event_scout_trades_enabled=False,
        event_trigger_gpt_enabled=False,
    ), base_cfg.data_dir / "account_gpt_advisor_all_pairs")
    return [cfg]


def ensure_bot_ready(bot: ForexManager, *, reconcile: bool = True) -> None:
    if not bot.instruments:
        bot.refresh_instruments()
        if reconcile:
            try:
                bot.reconcile_startup_state()
            except Exception as exc:
                bot.log_error("startup reconciliation", exc)


def run_bot_loop_iteration(bot: ForexManager) -> None:
    state = bot.load_state()
    if bot.weekend_summary_due(state):
        try:
            bot.run_weekend_summary_scan(reason="weekend_summary_preopen")
        except Exception as exc:
            bot.log_error("weekend summary scan", exc)
        state = bot.load_state()
    if bot.sunday_reopen_due(state):
        try:
            account = bot.oanda.get_account_summary()
            open_trades = bot.oanda.get_open_trades()
            bot.update_trade_profit_memory(account, open_trades, reason="sunday_reopen_pre_scan")
            bot.run_sunday_reopen_hard_protection(account=account, open_trades=open_trades)
            if cfg_bool("FOREX_SUNDAY_REOPEN_USE_GPT", default=True):
                bot.run_gpt_scan(reason="sunday_reopen")
            bot.mark_sunday_reopen_done()
        except Exception as exc:
            bot.log_error("Sunday reopen manager", exc)
        state = bot.load_state()

    last_monitor = state.get("last_monitor_utc")
    due_monitor = True
    if last_monitor:
        try:
            last = dt.datetime.fromisoformat(str(last_monitor).replace("Z", "+00:00")).astimezone(UTC)
            due_monitor = (utc_now() - last).total_seconds() >= bot.cfg.local_monitor_interval_minutes * 60
        except Exception:
            due_monitor = True
    if due_monitor:
        bot.run_local_monitor()
        state = bot.load_state()

    if bot.event_scan_due(state):
        bot.run_event_scan(reason="loop")
        state = bot.load_state()

    due, hhmm = bot.gpt_due(state=state)
    if due:
        bot.run_gpt_scan(reason=f"scheduled {hhmm}")
        state = bot.load_state()
        bot.mark_gpt_run(hhmm, state)


def run_multi_account_loop(bots: List[ForexManager]) -> None:
    # Kept for CLI compatibility; this single-purpose file always has one lane.
    for bot in bots:
        bot.loop()
# =============================================================================
# CLI
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Always-on OANDA forex GPT manager for major USD pairs only.")
    p.add_argument("--print-config", action="store_true", help="Print resolved config without revealing keys.")
    p.add_argument("--scan-now", action="store_true", help="Run one GPT portfolio-management scan immediately.")
    p.add_argument("--monitor-now", action="store_true", help="Run one local monitor pass immediately.")
    p.add_argument("--once", action="store_true", help="Do not enter the permanent loop after explicit actions.")
    p.add_argument("--dry-run", action="store_true", help="Force no broker execution for this run.")
    p.add_argument("--execute", action="store_true", help="Force broker execution for this run, subject to live guard.")
    p.add_argument("--no-scan-on-launch", action="store_true", help="Skip launch scan before loop.")
    p.add_argument("--shutdown-fade", action="store_true", help="Force graceful shutdown/fade mode for this run: no new opens, tighten/partial-close current trades.")
    p.add_argument("--shutdown-close-now", action="store_true", help="Force immediate close-all shutdown mode for this run.")
    p.add_argument("--normal-mode", action="store_true", help="Force normal trading mode for this run, ignoring FOREX_SHUTDOWN_MODE.")
    p.add_argument("--shutdown-pass-now", action="store_true", help="Run one shutdown fade/close pass immediately.")
    p.add_argument("--write-recaps-now", action="store_true", help="Write/update daily recap files from existing CSV logs, then optionally exit with --once.")
    p.add_argument("--daily-report-now", action="store_true", help="Write/check the shared daily move/account comparison report now, then optionally exit with --once.")
    p.add_argument("--event-scan-now", action="store_true", help="Run one local M5 event scanner pass immediately.")
    p.add_argument("--weekend-summary-now", action="store_true", help="Run one weekend summary planning GPT scan now; no broker execution.")
    p.add_argument("--sunday-reopen-now", action="store_true", help="Run Sunday reopen hard protection plus GPT management scan now.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    base_cfg = BotConfig.load()
    if args.dry_run:
        base_cfg.execute_trades = False
    if args.execute:
        base_cfg.execute_trades = True
    if args.no_scan_on_launch:
        base_cfg.scan_on_launch = False
    if args.normal_mode:
        base_cfg.shutdown_mode = "off"
    if args.shutdown_fade:
        base_cfg.shutdown_mode = "fade"
        base_cfg.shutdown_block_new_trades = True
    if args.shutdown_close_now:
        base_cfg.shutdown_mode = "close_now"
        base_cfg.shutdown_block_new_trades = True
        base_cfg.shutdown_skip_gpt = True

    lane_cfgs = build_account_lane_configs(base_cfg)
    bots = [ForexManager(c) for c in lane_cfgs]

    if args.print_config:
        for bot in bots:
            log(f"Resolved config for lane={bot.cfg.account_lane} account_name={bot.cfg.account_display_name}")
            log(f"market_movement_ledger={bot.cfg.market_movements_csv.parent}")
            bot.print_config()
        if not (args.scan_now or args.monitor_now or args.write_recaps_now or args.daily_report_now or args.shutdown_pass_now or args.event_scan_now or args.weekend_summary_now or args.sunday_reopen_now):
            return 0

    did_explicit = False
    if args.write_recaps_now:
        for bot in bots:
            bot.write_daily_recaps_around_now()
            log(f"{bot.lane_prefix()}Wrote daily recap files under {bot.cfg.daily_recap_dir}")
        did_explicit = True
    if args.daily_report_now:
        maybe_auto_daily_move_report(account_lane="gpt", writer=True, force=True)
        did_explicit = True

    explicit_flags = (
        args.scan_now or args.monitor_now or args.write_recaps_now or args.daily_report_now or args.shutdown_pass_now
        or args.event_scan_now or args.weekend_summary_now or args.sunday_reopen_now
    )
    if not args.print_config and not (args.once and args.write_recaps_now and not explicit_flags):
        for bot in bots:
            bot.validate_config()

    if args.monitor_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_local_monitor()
        did_explicit = True
    if args.scan_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_gpt_scan(reason="manual")
        did_explicit = True
    if args.event_scan_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_event_scan(reason="manual")
        did_explicit = True
    if args.weekend_summary_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_weekend_summary_scan(reason="manual_weekend_summary")
        did_explicit = True
    if args.sunday_reopen_now:
        for bot in bots:
            ensure_bot_ready(bot)
            account = bot.oanda.get_account_summary()
            open_trades = bot.oanda.get_open_trades()
            bot.update_trade_profit_memory(account, open_trades, reason="manual_sunday_reopen")
            bot.run_sunday_reopen_hard_protection(account=account, open_trades=open_trades)
            bot.run_gpt_scan(reason="sunday_reopen_manual")
        did_explicit = True
    if args.shutdown_pass_now:
        for bot in bots:
            ensure_bot_ready(bot)
            if not bot.shutdown_mode_active():
                bot.cfg.shutdown_mode = "fade"
            bot.run_shutdown_fade_pass(reason="manual_shutdown_pass", force=True)
        did_explicit = True

    if args.once and did_explicit:
        return 0

    if len(bots) == 1:
        bots[0].loop()
    else:
        run_multi_account_loop(bots)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())


# =============================================================================
# File layout / editing notes
# =============================================================================
# Credentials file only. Keep API secrets here:
#    .\creds
#
# OPENAI_API_KEY = "sk-..."
# OANDA_API_KEY = "..."
# OANDA_ACCOUNT_ID = "..."  # fallback single-account mode
# OANDA_ACCOUNT_ID_MAJ = "..."
# OANDA_ACCOUNT_ID_VOL = "..."
#
# Bot behavior/settings are NOT loaded from creds and no separate config file is
# required. Edit EMBEDDED_CONFIG near the top of this manager file instead.
#
# Fast toggles:
# - To use practice account execution: keep EMBEDDED_CONFIG["OANDA_ENV"] = "practice".
# - To dry-run: set EMBEDDED_CONFIG["FOREX_EXECUTE_TRADES"] = False.
# - To disable scout trades: set EMBEDDED_CONFIG["FOREX_EVENT_SCOUT_TRADES_ENABLED"] = False.
# - Sunday reopen protection: edit FOREX_WEEKEND_SUMMARY_* and FOREX_SUNDAY_REOPEN_* in EMBEDDED_CONFIG.
# - v5.15 scout trades do not require GPT permission by default; set FOREX_EVENT_REQUIRE_GPT_PERMISSION=True to restore permissioned-only scouts.
# - To fade/shutdown: set EMBEDDED_CONFIG["FOREX_SHUTDOWN_MODE"] = "fade".
