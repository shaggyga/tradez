#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
OANDA Forex Technical Scout Account Manager — Two-Script Auto Stack v1
====================================

Single-account TECHNICAL SCOUT script: uses OANDA_ACCOUNT_ID_TECH/OANDA_ACCOUNT_ID_VOL, scans all OANDA currency pairs, and auto-flattens before the FX weekend.

Always-on OANDA forex manager:
- Runs continuously. No Task Scheduler required.
- No scheduled research/portfolio calls in this technical account.
- Local M1 movement scans drive technical scout entries.
- Discovers all tradeable OANDA CURRENCY instruments from the account.
- Technical short-term lane: blocks new scouts before the weekend and closes remaining scout-account positions before Friday FX close.
- Strategy split: this file is the technical scout account; the separate portfolio file handles news and portfolio advice.
- v5.24 adds currency-strength cross inference: pair signals vote currencies up/down, then strongest-vs-weakest direct crosses are promoted as synthetic scout candidates with exposure de-duplication.
- Loads API credentials from creds/creds.txt only; all tunable bot settings are embedded in this manager file.
- Uses OANDA practice by default unless OANDA_ENV=live and FOREX_ALLOW_LIVE=1 are set.
- Technical movement scoring decides candidate priority; the script enforces account guardrails.
- v5.10 uses global-best selection: evaluate all supplied pairs, then choose the best trades without USD/cross quotas or placeholder candidates.
- v5.29 fixes technical practice execution and post-spike capture: practice orders execute by default, signed/negative pip moves become SHORT candidates correctly, drastic moves can override the normal ratio gate with reduced risk, and every scan reports absolute pips.
- v5.14 keeps credentials in creds/creds.txt, embeds all bot settings in this one manager file, and adds Sunday reopen/profit-memory protection.
- v5.15 turns local scout trades into real technical impulse trades: hard-confirmed event scouts no longer need RESEARCH event permission. Sunday reopen blocks only unrelated fresh opens; closes, flips, and confirmed reversal scouts remain allowed.
- v5.17 upgrades scouts from naive chase signals to M1 early-trigger/EV scoring: all pairs are scanned, late exhaustion moves are skipped, positive pair/theme/time profiles can scale risk, and scout fills are logged as event_scout actions.
- v5.17 adds a volatile-pair scout lane so ZAR/TRY/NOK/SEK/CZK/MXN/HKD style moves are not treated like ordinary crosses; it still scans all pairs, but volatile pairs get separate spread, stop, event-age, EV, and risk handling.
- v5.32 adds a non-USD momentum-exhaustion shadow monitor validated across seven rolling weeks. It logs 60/120-minute reversal-risk conditions but cannot create orders.
- v5.34 adds explicit scout audit rows for strict pre-spike pressure, cluster size, value-weighted expectancy, and order/reject outcomes.

Install:
    pip install requests

Typical run:
    python oanda_technical_account_manager_auto.py

Useful test commands:
    python oanda_technical_scout_manager_v5_17.py --print-config
    python oanda_technical_scout_manager_v5_17.py --scan-now --once --dry-run
    python oanda_technical_scout_manager_v5_17.py --monitor-now --once

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
import warnings
from dataclasses import dataclass, asdict, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
try:
    import pandas as pd
except Exception:  # pragma: no cover - pandas is present in the project venv.
    pd = None

warnings.filterwarnings(
    "ignore",
    message="X does not have valid feature names.*",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message="`sklearn\\.utils\\.parallel\\.delayed` should be used.*",
    category=UserWarning,
)

try:
    import joblib
except Exception:  # pragma: no cover
    joblib = None

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
TRAINING_ROOT = SCRIPT_DIR / "data" / "oanda_training_manager"
TECHNICAL_MACRO_BIAS_LATEST_PATH = TRAINING_ROOT / "macro" / "latest_macro_bias.json"

SEVEN_MAJOR_PAIRS = {
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
    "AUD_USD",
    "NZD_USD",
}
JPY_RISK_PAIRS = {
    "AUD_JPY",
    "NZD_JPY",
    "CAD_JPY",
    "GBP_JPY",
    "EUR_JPY",
    "ZAR_JPY",
    "TRY_JPY",
    "SGD_JPY",
    "HKD_JPY",
}
EUR_GBP_CURRENCIES = {"EUR", "GBP"}
CHF_SAFE_HAVEN_PAIRS = {
    "USD_CHF",
    "EUR_CHF",
    "GBP_CHF",
    "AUD_CHF",
    "NZD_CHF",
    "CAD_CHF",
    "CHF_JPY",
    "CHF_HKD",
    "CHF_ZAR",
    "SGD_CHF",
}
COMMODITY_CURRENCIES = {"AUD", "NZD", "CAD", "NOK", "ZAR"}
RISK_CURRENCIES = {"AUD", "NZD", "CAD", "NOK", "ZAR", "MXN", "TRY", "HUF", "CZK", "PLN", "SEK", "THB"}
CURRENCY_STRUCTURAL_CARRY_SCORE = {
    "TRY": 1.00,
    "MXN": 0.90,
    "ZAR": 0.80,
    "HUF": 0.65,
    "PLN": 0.55,
    "NOK": 0.35,
    "NZD": 0.25,
    "AUD": 0.20,
    "CAD": 0.15,
    "USD": 0.10,
    "GBP": 0.05,
    "EUR": -0.10,
    "SEK": -0.20,
    "DKK": -0.25,
    "SGD": -0.30,
    "HKD": -0.35,
    "CHF": -0.70,
    "JPY": -0.80,
}
EXOTIC_OR_VOLATILE_CURRENCIES = {
    "CNH",
    "CZK",
    "DKK",
    "HKD",
    "HUF",
    "MXN",
    "NOK",
    "PLN",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "ZAR",
}
PAIR_TAXONOMY_CLASSES = [
    "usd_major",
    "usd_other",
    "jpy_risk",
    "commodity",
    "eur_gbp_cross",
    "chf_safe_haven",
    "exotic_high_spread",
    "volatile_non_usd",
    "other_cross",
]
REGIME_CLASSES = [
    "usd_trend",
    "risk_on",
    "risk_off",
    "jpy_unwind",
    "chf_safe_haven",
    "commodity_trend",
    "low_vol_chop",
    "high_vol_event",
    "spread_impaired",
    "neutral",
]


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
    "RESEARCH_API_MODEL": "research-4.1-mini",
    "ENABLE_RESEARCH_API_WEB_SEARCH": False,
    "FOREX_DISABLE_HTTP_KEEPALIVE": True,
    "FOREX_HTTP_MAX_RETRIES": 3,
    "FOREX_HTTP_RETRY_SLEEP_SECONDS": 2.0,

    # Execution
    "FOREX_EXECUTE_TRADES": True,             # practice executes by default
    "FOREX_AUTO_EXECUTE_RESEARCH_ACTIONS": True,              # technical practice orders execute by default
    "FOREX_SCAN_ON_LAUNCH": False,
    "FOREX_DRY_RUN_STATE_ORDERS": False,

    # Strategy split is RESEARCH/portfolio-vs-technical, not USD-vs-non-USD.
    # This script is the technical scout account. It scans all discovered OANDA
    # currency instruments and naturally tends to participate where realized
    # volatility and move/spread conditions are strongest. The pair list below
    # is only a special-handling list for known high-volatility instruments; it
    # is not a hard routing restriction.
    "FOREX_MULTI_ACCOUNT_ENABLED": True,
    "FOREX_MAJOR_USD_PAIRS": [
        "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF",
        "USD_CAD", "AUD_USD", "NZD_USD",
    ],
    "FOREX_TECHNICAL_SCAN_ALL_PAIRS": True,
    "FOREX_TECHNICAL_EXCLUDE_PAIRS": [],
    "FOREX_RESEARCH_RELIABLE_PAIRS": [],
    "FOREX_TECHNICAL_SCOUT_PAIRS": [],

    # Scheduled RESEARCH portfolio-management calls: 12/day, session-based NY times.
    "FOREX_RESEARCH_CALLS_PER_DAY": 0,
    "FOREX_RESEARCH_CALL_TIMES_NY": [],
    "FOREX_FRIDAY_RESEARCH_CALL_TIMES_NY": [],
    "FOREX_SCHEDULE_WINDOW_MINUTES": 10,
    "FOREX_MIN_MINUTES_BETWEEN_RESEARCH_SCANS": 75,
    "FOREX_LOOP_SLEEP_SECONDS": 30,
    "FOREX_LOCAL_MONITOR_INTERVAL_MINUTES": 10,

    # Weekend / Sunday-reopen protection. The normal schedule is not enough for
    # weekend gaps: FX is closed, then reprices quickly after Sunday open.
    # These scans are separate from the 12 normal RESEARCH calls/day.
    "FOREX_WEEKEND_SUMMARY_SCAN_ENABLED": False,
    "FOREX_WEEKEND_SUMMARY_SCAN_TIME_NY": "16:45",
    "FOREX_WEEKEND_SUMMARY_SCAN_WINDOW_MINUTES": 20,
    "FOREX_WEEKEND_SUMMARY_USE_RESEARCH": False,

    "FOREX_SUNDAY_REOPEN_MANAGER_ENABLED": False,
    "FOREX_SUNDAY_REOPEN_SCAN_TIME_NY": "17:15",
    "FOREX_SUNDAY_REOPEN_SCAN_WINDOW_MINUTES": 35,
    "FOREX_SUNDAY_REOPEN_USE_RESEARCH": False,
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

    # Hard local protection around reopen. RESEARCH may still think a thesis is valid,
    # but the script can override if a trade gave back real profit or if NAV gaps.
    "FOREX_TRADE_PROFIT_MEMORY_ENABLED": True,
    "FOREX_REOPEN_HARD_PROTECTION_ENABLED": True,
    "FOREX_REOPEN_CLOSE_IF_WAS_PROFIT_NOW_NEGATIVE": True,
    "FOREX_REOPEN_MIN_PRIOR_PROFIT_USD": 0.75,
    "FOREX_REOPEN_NEGATIVE_PL_THRESHOLD_USD": -0.05,
    "FOREX_REOPEN_CLOSE_IF_PROFIT_GIVEBACK_USD": 1.00,
    "FOREX_REOPEN_CLOSE_IF_NAV_DROP_PCT": 6.0,
    "FOREX_REOPEN_TIGHTEN_ALL_EXISTING": True,

    # Volatile short-term weekend policy. This script is meant to capture
    # sudden short-lived moves, not carry volatile/exotic exposure over the
    # weekend gap. Times are New York time. It blocks fresh scout entries first,
    # then repeatedly tries to flatten all open trades in OANDA_ACCOUNT_ID_VOL
    # before the Friday FX close. If the script/broker misses Friday, it will
    # also close anything left after the Sunday open before allowing new scouts.
    "FOREX_VOLATILE_WEEKEND_FLATTEN_ENABLED": True,
    "FOREX_VOLATILE_WEEKEND_BLOCK_NEW_SCOUTS_TIME_NY": "15:00",
    "FOREX_VOLATILE_WEEKEND_FLATTEN_TIME_NY": "15:45",
    "FOREX_VOLATILE_WEEKEND_CLOSE_REPEAT_MINUTES": 10,
    "FOREX_VOLATILE_WEEKEND_SUNDAY_RESUME_TIME_NY": "18:15",
    "FOREX_VOLATILE_WEEKEND_CLOSE_ALL_ACCOUNT_TRADES": True,

    # Aggressive but guarded sizing/margin
    "FOREX_MAX_OPEN_TRADES": 50,
    "FOREX_MAX_NEW_TRADES_PER_SCAN": 5,
    "FOREX_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN": 12.0,
    "FOREX_MAX_RESEARCH_RISK_PCT_PER_TRADE": 4.5,
    "FOREX_MIN_RESEARCH_RISK_PCT": 0.10,
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

    # Fast local M1 event scanner and scout trades.
    # v5.17 scans all 68 pairs on completed M1 candles, then scores early continuation EV.
    "FOREX_EVENT_SCANNER_ENABLED": True,
    "FOREX_EVENT_SCAN_INTERVAL_SECONDS": 30,
    "FOREX_EVENT_CANDLE_GRANULARITY": "M1",
    # 300 completed M1 candles provide enough history to reproduce the
    # study's 240-minute ATR normalization on closed 5-minute bars.
    "FOREX_EVENT_CANDLE_COUNT": 300,
    "FOREX_EVENT_WINDOWS_MINUTES": [1, 3, 5, 10, 15, 30],
    "FOREX_EVENT_LOG_SCAN_SUMMARY_EVERY_SCAN": True,
    "FOREX_EVENT_LOG_TOP_REJECTIONS": True,
    "FOREX_EVENT_TOP_REJECTIONS_TO_LOG": 12,
    "FOREX_EVENT_MIN_BASKET_PAIRS": 1,
    "FOREX_EVENT_MIN_MAJOR_NET_PIPS": 3.5,
    "FOREX_EVENT_MIN_CROSS_NET_PIPS": 6.0,
    "FOREX_EVENT_MIN_EXOTIC_NET_PIPS": 25.0,
    "FOREX_EVENT_MIN_MOVE_TO_SPREAD_RATIO": 1.6,
    "FOREX_TECH_DRASTIC_MOVE_CAPTURE_ENABLED": True,
    "FOREX_TECH_DRASTIC_MOVE_THRESHOLD_MULTIPLIER": 2.0,
    "FOREX_TECH_DRASTIC_MOVE_MIN_RATIO": 0.75,
    "FOREX_TECH_DRASTIC_MOVE_RISK_MULTIPLIER": 0.50,

    # v5.30 campaign compounding layer. This is the early-warning stage before
    # a full event trigger: range compression + acceleration + basket pressure
    # can create a tiny anticipatory scout or a high-priority watch candidate.
    "FOREX_TECH_PRE_SPIKE_PRESSURE_ENABLED": True,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SCORE": 62.0,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_TRADE_SCORE": 78.0,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_RATIO": 0.80,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_PIPS_MULTIPLIER": 0.35,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_RISK_MULTIPLIER": 0.30,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MAX_SCOUTS_PER_SCAN": 3,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_LOG_TOP": 12,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_STRICT_GATE_ENABLED": False,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_CLUSTER_INSTRUMENTS": 0,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_DIRECTION_CLUSTER_INSTRUMENTS": 0,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SIGNAL_VALUE_USD": 0.0,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SIGNAL_RETURN_PCT": 0.0,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_REQUIRE_VALUE_ESTIMATE": False,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_BYPASS_PRODUCTION_MODEL": False,
    "FOREX_TECH_PRE_SPIKE_PRESSURE_ONLY": False,
    # Shadow-only diagnostic for localized EM/high-spread currency clusters.
    # This does not create executable signals. It marks cases where the broad
    # strict pressure gate rejects a move, but a currency-local basket such as
    # ZAR crosses is moving together. The resulting rows can be backtested from
    # scout_audit_ledger.csv before any live gate is loosened.
    "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_SHADOW_ENABLED": False,
    "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_CURRENCIES": "ZAR,TRY,MXN,NOK,SEK,CZK,HKD,THB,CNH,PLN,HUF",
    "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_INSTRUMENTS": 4,
    "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_DIRECTION_INSTRUMENTS": 4,
    "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_RATIO": 1.35,
    "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_SCORE": 80.0,
    # v5.32 rolling-week exhaustion monitor. Seven expanding and six-week
    # rolling holdouts confirmed tail-event concentration, but first-trigger
    # SHORT expectancy remained negative after spread. Keep this telemetry-only
    # until a later validation explicitly passes the execution promotion gate.
    "FOREX_TECH_EXHAUSTION_SHADOW_ENABLED": True,
    "FOREX_TECH_EXHAUSTION_EXCLUDE_USD": True,
    "FOREX_TECH_EXHAUSTION_MIN_RULES": 1,
    "FOREX_TECH_EXHAUSTION_LOG_TOP": 20,
    "FOREX_TECH_EXHAUSTION_M5_ATR_MIN": 0.80,
    "FOREX_TECH_EXHAUSTION_M15_ATR_MIN": 1.35,
    "FOREX_TECH_EXHAUSTION_M30_ATR_MIN": 1.88,
    "FOREX_TECH_EXHAUSTION_STRENGTH_GAP15_MIN": 0.00045,
    # Best-model registry produced continuously by the research manager.
    # The technical account may open new research-backed trades only when a
    # production manifest exists and explicitly activates the model. Shadow and
    # canary manifests are telemetry only.
    "FOREX_TECH_REQUIRE_PRODUCTION_MODEL_FOR_NEW_ENTRIES": True,
    "FOREX_TECH_PROMOTED_MODEL_MANIFEST": str(
        SCRIPT_DIR
        / "data"
        / "oanda_training_manager"
        / "promotions"
        / "technical_production.json"
    ),
    "FOREX_TECH_SEGMENT_GATE_ENABLED": True,
    "FOREX_TECH_MACRO_GATE_ENABLED": True,
    "FOREX_TECH_MACRO_BIAS_PATH": str(TECHNICAL_MACRO_BIAS_LATEST_PATH),
    "FOREX_TECH_MACRO_BIAS_MAX_AGE_HOURS": 36.0,
    "FOREX_TECH_MACRO_NEWS_RISK_REJECT": 0.85,
    "FOREX_TECH_MACRO_EVENT_RISK_REJECT": 0.85,
    "FOREX_TECH_MACRO_CONTRA_BIAS_REJECT": 0.60,
    "FOREX_TECH_SHADOW_MODEL_MANIFEST": str(
        SCRIPT_DIR
        / "data"
        / "oanda_training_manager"
        / "promotions"
        / "shadow_candidate.json"
    ),
    "FOREX_TECH_MAJOR_MOVE_FACTOR_MANIFEST": str(
        SCRIPT_DIR
        / "data"
        / "oanda_training_manager"
        / "promotions"
        / "major_move_factor_production.json"
    ),
    "FOREX_TECH_MAJOR_MOVE_FACTOR_SHADOW_MANIFEST": str(
        SCRIPT_DIR
        / "data"
        / "oanda_training_manager"
        / "promotions"
        / "major_move_factor_shadow.json"
    ),
    "FOREX_EVENT_MAX_SPREAD_PIPS_MAJOR": 6.0,
    "FOREX_EVENT_MAX_SPREAD_PIPS_CROSS": 12.0,
    "FOREX_EVENT_MAX_SPREAD_PIPS_EXOTIC": 220.0,
    "FOREX_EVENT_TRIGGERED_RESEARCH_ENABLED": False,
    "FOREX_EVENT_MIN_MINUTES_BETWEEN_RESEARCH_SCANS": 30,

    # Scout trades are ON and can execute from hard local technical impulse confirmation.
    # RESEARCH permissions are still saved/used as context, but they are no longer required for scouts.
    "FOREX_EVENT_SCOUT_TRADES_ENABLED": True,
    "FOREX_EVENT_REQUIRE_RESEARCH_PERMISSION": False,
    "FOREX_EVENT_SCOUT_RISK_PCT": 0.35,
    "FOREX_EVENT_MAX_TOTAL_SCOUT_RISK_PCT": 4.0,
    "FOREX_EVENT_MAX_SCOUT_TRADES_PER_EVENT": 8,
    "FOREX_EVENT_SCOUT_STOP_PIPS_MAJOR": 14.0,
    "FOREX_EVENT_SCOUT_STOP_PIPS_CROSS": 22.0,
    "FOREX_EVENT_SCOUT_STOP_PIPS_EXOTIC": 250.0,
    "FOREX_EVENT_SCOUT_TAKE_PROFIT_R_MULTIPLE": 2.0,
    "FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_MAJOR": 12.0,
    "FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_CROSS": 20.0,
    "FOREX_EVENT_SCOUT_TRAILING_STOP_PIPS_EXOTIC": 200.0,
    "FOREX_EVENT_PERMISSION_DEFAULT_EXPIRY_HOURS": 8.0,

    # v5.24 currency-strength scout overlay. This turns clusters such as
    # GBP_USD LONG + USD_MXN LONG into a stronger direct-cross idea like
    # GBP_MXN LONG when the pair is tradeable and spreads/margin allow it.
    "FOREX_TECH_CURRENCY_STRENGTH_ENABLED": True,
    "FOREX_TECH_STRENGTH_MAX_SYNTHETIC_CANDIDATES": 8,
    "FOREX_TECH_STRENGTH_MIN_GAP": 10.0,
    "FOREX_TECH_STRENGTH_MIN_CONFIRMING_SIGNALS": 2,
    "FOREX_TECH_STRENGTH_REQUIRE_DIRECT_PRICE": True,
    "FOREX_TECH_STRENGTH_BASE_RISK_MULTIPLIER": 0.75,
    "FOREX_TECH_STRENGTH_MAX_RISK_PCT": 0.45,
    "FOREX_TECH_STRENGTH_OVERSTACK_PENALTY": 0.50,
    "FOREX_TECH_STRENGTH_MAX_SAME_CURRENCY_EXPOSURE": 8,
    "FOREX_TECH_STRENGTH_LOG_TOP": True,
    "FOREX_TECH_SUPPRESS_REPEAT_CAP_LOGS": False,
    "FOREX_TECH_ALLOW_MIN_SIZE_FALLBACK": True,
    "FOREX_TECH_MIN_SIZE_MAX_RISK_USD": 0.35,

    # v5.26 planned turnover/compounding controls. These are logged/configurable
    # knobs for the technical account's next position-health layer. Existing
    # broker-side stops/trailing stops remain active; these settings are kept
    # centralized so the turnover patch can use them without another config move.
    "FOREX_TECH_TURNOVER_ENABLED": True,
    "FOREX_TECH_POSITION_REVIEW_SECONDS": 60,
    "FOREX_TECH_MAX_POSITION_AGE_MINUTES": 240,
    "FOREX_TECH_STALE_AFTER_MINUTES": 30,
    "FOREX_TECH_CUT_IF_STALE_AND_RED": True,
    "FOREX_TECH_CUT_IF_HEALTH_BELOW": 35,
    "FOREX_TECH_TIGHTEN_IF_HEALTH_BELOW": 55,
    "FOREX_TECH_ROTATE_IF_NEW_EDGE_ABOVE": 18,
    "FOREX_TECH_MIN_ROTATION_NET_EDGE_PIPS": 4.0,
    "FOREX_TECH_MIN_ROTATION_EDGE_AFTER_SPREAD_X": 2.5,
    "FOREX_TECH_LOSS_COOLDOWN_MINUTES": 45,
    "FOREX_TECH_SAME_PAIR_REENTRY_COOLDOWN_MINUTES": 30,
    "FOREX_TECH_MAX_CHURN_LOSSES_PER_HOUR": 3,
    "FOREX_TECH_DAILY_LOSS_LOCK_PCT": 8.0,
    "FOREX_TECH_PYRAMID_ENABLED": True,
    "FOREX_TECH_MAX_ADDS_PER_MOVEMENT": 2,
    "FOREX_TECH_ADD_ONLY_IF_CURRENT_PROFIT_USD_GT": 0.10,
    "FOREX_TECH_ADD_ONLY_IF_MFE_RETAINED_PCT_GT": 60,
    "FOREX_TECH_ADD_RISK_MULTIPLIER": 0.50,
    "FOREX_TECH_LOCK_PROFIT_BEFORE_ADD": True,

    # v5.31 movement-campaign controller with rotation/pruning. This replaces blunt repeat-cap churn
    # with one campaign per instrument/direction/theme. Repeated signals update
    # the campaign; only protected/profitable campaigns can add/compound.
    "FOREX_TECH_CAMPAIGN_CONTROL_ENABLED": True,
    "FOREX_TECH_CAMPAIGN_MAX_ACTIVE": 8,
    "FOREX_TECH_CAMPAIGN_MAX_ENTRIES": 2,
    "FOREX_TECH_CAMPAIGN_MAX_ADDS": 1,
    "FOREX_TECH_CAMPAIGN_MIN_SECONDS_BETWEEN_ATTEMPTS": 240,
    "FOREX_TECH_CAMPAIGN_MIN_SECONDS_BETWEEN_ADDS": 420,
    "FOREX_TECH_CAMPAIGN_ADD_MIN_UNREALIZED_USD": 0.10,
    "FOREX_TECH_CAMPAIGN_ADD_RISK_MULTIPLIER": 0.45,
    "FOREX_TECH_CAMPAIGN_MAX_ENTRY_RISK_PCT": 0.28,
    "FOREX_TECH_CAMPAIGN_MARGIN_SOFT_CAP_PCT": 52.0,
    "FOREX_TECH_CAMPAIGN_MARGIN_HARD_CAP_PCT": 64.0,
    "FOREX_TECH_CAMPAIGN_CLOSE_STALE_RED_ENABLED": True,
    "FOREX_TECH_CAMPAIGN_STALE_MINUTES": 25,
    "FOREX_TECH_CAMPAIGN_LOG_HEALTH_SECONDS": 45,
    "FOREX_TECH_CAMPAIGN_EV_REPEAT_COUNT": 1,
    "FOREX_TECH_CAMPAIGN_PRUNE_ENABLED": True,
    "FOREX_TECH_CAMPAIGN_PRUNE_INTERVAL_SECONDS": 60,
    "FOREX_TECH_CAMPAIGN_PRUNE_MAX_CLOSES_PER_PASS": 2,
    "FOREX_TECH_CAMPAIGN_PRUNE_MARGIN_TRIGGER_PCT": 55.0,
    "FOREX_TECH_CAMPAIGN_PRUNE_RED_AFTER_MINUTES": 18,
    "FOREX_TECH_CAMPAIGN_PRUNE_MIN_LOSS_USD": 0.015,
    "FOREX_TECH_CAMPAIGN_PRUNE_GIVEBACK_MIN_MFE_USD": 0.030,
    "FOREX_TECH_CAMPAIGN_PRUNE_PROFIT_GIVEBACK_PCT": 70.0,
    "FOREX_TECH_CAMPAIGN_ALLOW_OPPOSITE_ROTATION": True,
    "FOREX_TECH_CAMPAIGN_ROTATE_OPPOSITE_IF_LOSS_USD": 0.02,
    "FOREX_TECH_CAMPAIGN_ROTATE_OPPOSITE_MIN_SCORE": 82.0,
    "FOREX_TECH_CAMPAIGN_MAX_SAME_PAIR_TRADES": 2,

    # v5.33 broad-regime lock for scout campaigns. When several fresh moves agree
    # on the same currency thesis (for example USD strong / EM weak), do not let
    # one noisy counter-candle flip or prune the whole campaign too early.
    "FOREX_SCOUT_REGIME_LOCK_ENABLED": True,
    "FOREX_SCOUT_REGIME_LOCK_MIN_INSTRUMENTS": 3,
    "FOREX_SCOUT_REGIME_LOCK_MIN_STRENGTH_GAP": 4.5,
    "FOREX_SCOUT_REGIME_LOCK_MIN_PAIR_GAP": 1.5,
    "FOREX_SCOUT_REGIME_LOCK_TTL_MINUTES": 50.0,
    "FOREX_SCOUT_REGIME_LOCK_BLOCK_COUNTER": True,
    "FOREX_SCOUT_REGIME_LOCK_PROTECT_ALIGNED_PRUNE_MINUTES": 60.0,
    "FOREX_SCOUT_REGIME_LOCK_PROTECT_ALIGNED_LOSS_USD": 0.06,
    "FOREX_SCOUT_REGIME_LOCK_LOG_TOP_N": 8,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_ENABLED": True,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_MINUTES": 60.0,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_MIN_GAP": 30.0,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_REPLACEMENT_GAP_MULT": 1.35,
    "FOREX_SCOUT_BROAD_REGIME_TRADE_ONLY": False,
    "FOREX_SCOUT_BROAD_REGIME_MIN_EV_SCORE": 90.0,
    "FOREX_SCOUT_BROAD_REGIME_MIN_STRENGTH_GAP": 30.0,
    "FOREX_SCOUT_BROAD_REGIME_MIN_INSTRUMENTS": 15,
    "FOREX_SCOUT_BROAD_REGIME_MIN_PAIR_EDGE": 1.5,
    "FOREX_SCOUT_BROAD_REGIME_VOLATILE_ONLY": False,
    "FOREX_SCOUT_BROAD_REGIME_ALLOW_SYNTHETIC_CROSSES": False,

    # v5.17 scout EV / earlyness classifier. This stops the bot from blindly chasing
    # a completed volatility candle. It still considers every OANDA pair, but it
    # sizes/filters by earlyness, basket confirmation, spread regime, and empirical
    # follow-through profile when available.
    "FOREX_SCOUT_EV_SCORING_ENABLED": True,
    "FOREX_SCOUT_MIN_EV_SCORE_TO_TRADE": 58.0,
    "FOREX_SCOUT_MAX_EVENT_AGE_MINUTES_FOR_CONTINUATION": 20.0,
    "FOREX_SCOUT_MAX_REPEAT_TRIGGERS_PER_KEY": 4,
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
        "USD_HUF", "EUR_HUF",
        "USD_THB",
        "USD_CZK", "EUR_CZK",
        "USD_PLN", "EUR_PLN", "GBP_PLN",
        "USD_NOK", "EUR_NOK",
        "USD_SEK", "EUR_SEK",
        "USD_DKK", "EUR_DKK",
        "USD_CNH",
        "USD_MXN", "EUR_MXN",
        "HKD_JPY",
        "CHF_HKD", "GBP_HKD", "EUR_HKD", "CAD_HKD", "AUD_HKD", "USD_HKD",
    ],
    "FOREX_SCOUT_VOLATILE_MIN_NET_PIPS": 45.0,
    "FOREX_SCOUT_VOLATILE_MAX_SPREAD_PIPS": 260.0,
    "FOREX_SCOUT_VOLATILE_MIN_MOVE_TO_SPREAD_RATIO": 1.5,
    "FOREX_SCOUT_VOLATILE_MIN_BASKET_PAIRS": 1,
    "FOREX_SCOUT_VOLATILE_MAX_EVENT_AGE_MINUTES": 15.0,
    "FOREX_SCOUT_VOLATILE_REPEAT_TRIGGER_CAP": 4,
    "FOREX_SCOUT_VOLATILE_BASE_RISK_PCT": 0.20,
    "FOREX_SCOUT_VOLATILE_MAX_RISK_PCT": 0.50,
    "FOREX_SCOUT_VOLATILE_ALLOW_UNKNOWN_PROFILE": True,
    "FOREX_SCOUT_VOLATILE_ALLOW_NEGATIVE_PROFILE_IF_EARLY": True,
    "FOREX_SCOUT_VOLATILE_MIN_EV_SCORE_TO_TRADE": 52.0,
    "FOREX_SCOUT_VOLATILE_STOP_PIPS": 320.0,
    "FOREX_SCOUT_VOLATILE_STOP_NET_MULTIPLIER": 0.65,
    "FOREX_SCOUT_VOLATILE_STOP_SPREAD_MULTIPLIER": 3.5,
    "FOREX_SCOUT_VOLATILE_TRAILING_STOP_PIPS": 250.0,
    "FOREX_SCOUT_VOLATILE_MAX_SPREAD_TO_STOP_RATIO": 0.90,

    # Price/candle context for RESEARCH
    "FOREX_PRICE_CHUNK_SIZE": 40,
    "FOREX_CANDLE_GRANULARITY": "H1",
    "FOREX_CANDLE_COUNT": 80,
    "FOREX_INCLUDE_CANDLES": True,
    "FOREX_MAX_CANDLE_INSTRUMENTS": 0,
    "FOREX_ACCOUNT_CURRENCY": "USD",

    # Shutdown / fade-to-flat mode
    "FOREX_SHUTDOWN_MODE": "off",             # off, fade, close_now
    "FOREX_SHUTDOWN_SKIP_RESEARCH": True,
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
    "FOREX_DATA_DIR": str(SCRIPT_DIR / "data" / "technical_scout_manager"),
}


CONFIG: Dict[str, Any] = dict(EMBEDDED_CONFIG)


CREDENTIAL_SETTING_NAMES = {
    "RESEARCH_API_API_KEY", "research_api_api_key", "research_apiapi", "RESEARCH_API_KEY",
    "OANDA_API_KEY", "OANDA_API_TOKEN", "oanda_api_key", "oandaapi",
    "OANDA_ACCOUNT_ID", "oanda_account_id", "OANDA_ACCOUNT",
    "OANDA_ACCOUNT_ID_RESEARCH", "OANDA_ACCOUNT_ID_PORTFOLIO",
    "OANDA_ACCOUNT_ID_TECH", "OANDA_ACCOUNT_ID_TECHNICAL",
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


def predict_positive_probability_named(
    model: Any,
    features: Sequence[str],
    values: Sequence[float],
) -> float:
    """Score sklearn classifiers with feature names when available.

    Several live models were fitted from DataFrames.  Passing a raw list works,
    but sklearn emits a warning for every prediction.  In the live scanner that
    can become hundreds of MB of stderr per session.  Using a one-row DataFrame
    also protects against accidental feature-order drift.
    """
    sample: Any
    if pd is not None:
        sample = pd.DataFrame(
            [{feature: value for feature, value in zip(features, values)}],
            columns=list(features),
        )
    else:
        sample = [list(values)]
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names.*",
            category=UserWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="`sklearn\\.utils\\.parallel\\.delayed` should be used.*",
            category=UserWarning,
        )
        return float(model.predict_proba(sample)[0][1])


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


def hhmm_to_minute(hhmm: str, default: int) -> int:
    try:
        h, m = parse_hhmm(hhmm)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h * 60 + m
    except Exception:
        pass
    return int(default)


def volatile_weekend_policy_enabled() -> bool:
    return cfg_bool("FOREX_VOLATILE_WEEKEND_FLATTEN_ENABLED", default=True)


def volatile_weekend_block_new_scouts_now(t: Optional[dt.datetime] = None) -> bool:
    """Block new volatile/scout entries from Friday afternoon through Sunday warmup."""
    if not volatile_weekend_policy_enabled():
        return False
    n = t or ny_now()
    weekday = n.weekday()  # Monday=0, Friday=4, Sunday=6
    minute = ny_minute_of_day(n)
    block_minute = hhmm_to_minute(cfg_str("FOREX_VOLATILE_WEEKEND_BLOCK_NEW_SCOUTS_TIME_NY", default="15:00"), 15 * 60)
    resume_minute = hhmm_to_minute(cfg_str("FOREX_VOLATILE_WEEKEND_SUNDAY_RESUME_TIME_NY", default="18:15"), 18 * 60 + 15)
    if weekday == 4 and minute >= block_minute:
        return True
    if weekday == 5:
        return True
    if weekday == 6 and minute < resume_minute:
        return True
    return False


def volatile_weekend_flatten_window_now(t: Optional[dt.datetime] = None) -> bool:
    """Return True when the volatile account should be flat/flattened now."""
    if not volatile_weekend_policy_enabled():
        return False
    n = t or ny_now()
    weekday = n.weekday()
    minute = ny_minute_of_day(n)
    flatten_minute = hhmm_to_minute(cfg_str("FOREX_VOLATILE_WEEKEND_FLATTEN_TIME_NY", default="15:45"), 15 * 60 + 45)
    resume_minute = hhmm_to_minute(cfg_str("FOREX_VOLATILE_WEEKEND_SUNDAY_RESUME_TIME_NY", default="18:15"), 18 * 60 + 15)
    # Friday: start closing before the FX close. fx_market_closed() will stop
    # attempts after the broker close/break, but the loop retries until then.
    if weekday == 4 and minute >= flatten_minute and not fx_market_closed(n):
        return True
    # Sunday: if anything survived the weekend, close it after market open and
    # before the scout lane resumes.
    if weekday == 6 and not fx_market_closed(n) and minute < resume_minute:
        return True
    return False


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
    rotate_live_append_log_if_large(path)
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
# Full event / transaction logging v5.25
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


def _env_int(name: str, default: int) -> int:
    try:
        raw = os.getenv(name, "").strip()
        if not raw:
            return default
        return int(float(raw))
    except Exception:
        return default


FULL_EVENT_ROTATE_MAX_BYTES = _env_int("FOREX_FULL_EVENT_ROTATE_MAX_BYTES", 512 * 1024 * 1024)
FULL_EVENT_JSONL_RAW_LIMIT = _env_int("FOREX_FULL_EVENT_JSONL_RAW_LIMIT", 2_000)
LIVE_APPEND_ROTATE_MAX_BYTES = _env_int("FOREX_LIVE_APPEND_ROTATE_MAX_BYTES", 256 * 1024 * 1024)
LIVE_APPEND_ROTATE_FILENAMES = {
    "actions.csv",
    "compound_attempts.csv",
    "event_scan_summary.csv",
    "event_signals.csv",
    "event_windows.csv",
    "full_event_journal.csv",
    "full_event_journal.jsonl",
    "movement_campaigns.csv",
    "position_health.csv",
    "pressure_watchlist.csv",
    "scout_audit_ledger.csv",
}


def _rotate_append_log_if_large(path: Path) -> None:
    """Rotate very large append-only full-event mirrors before the next write.

    These files are operational mirrors, not the canonical model/audit ledgers.
    Rotation keeps the live writer responsive and prevents one multi-GB file
    from slowing status checks. Set FOREX_FULL_EVENT_ROTATE_MAX_BYTES=0 to
    disable rotation for a manual run.
    """
    try:
        limit = FULL_EVENT_ROTATE_MAX_BYTES
        if path.name in LIVE_APPEND_ROTATE_FILENAMES and path.name != "full_event_journal.jsonl":
            limit = LIVE_APPEND_ROTATE_MAX_BYTES
        if limit <= 0 or not path.exists():
            return
        if path.stat().st_size < limit:
            return
        stamp = utc_now().strftime("%Y%m%d_%H%M%S")
        rotated = path.with_name(f"{path.stem}.rotated_{stamp}_pid{os.getpid()}{path.suffix}")
        counter = 1
        while rotated.exists():
            rotated = path.with_name(f"{path.stem}.rotated_{stamp}_pid{os.getpid()}_{counter}{path.suffix}")
            counter += 1
        shutil.move(str(path), str(rotated))
        log(f"[LOG] Rotated oversized live append log {path} -> {rotated}")
    except Exception as exc:
        log(f"[WARN] Live append log rotation failed for {path}: {exc}")


def rotate_live_append_log_if_large(path: Path) -> None:
    """Best-effort rotation for high-volume live operational journals.

    The trainer/report artifacts are intentionally excluded; this applies only
    to live manager append logs whose filename is in LIVE_APPEND_ROTATE_FILENAMES.
    """
    try:
        if path.name not in LIVE_APPEND_ROTATE_FILENAMES:
            return
        _rotate_append_log_if_large(path)
    except Exception:
        return


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
        self.script_version = "v5.34_scout_audit"
        self.account_dir = Path(getattr(cfg, "data_dir", SCRIPT_DIR))
        # cfg.data_dir is normally data/forex_research_manager/account_x. Use data/forex/logging as global root.
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
                path = Path(root) / filename
                if filename == "full_event_journal.csv":
                    _rotate_append_log_if_large(path)
                append_csv(path, row, fields)
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
            raw_payload = raw if raw is not None else kwargs
            line["raw_truncated"] = True
            line["raw"] = _safe_json_text(raw_payload, FULL_EVENT_JSONL_RAW_LIMIT)
            for root in (self.shared_dir, self.account_dir):
                p = Path(root) / "full_event_journal.jsonl"
                p.parent.mkdir(parents=True, exist_ok=True)
                _rotate_append_log_if_large(p)
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
    research_api_api_key: str
    research_api_model: str
    enable_research_api_web_search: bool
    research_api_timeout_seconds: int

    oanda_api_key: str
    oanda_account_id: str
    account_lane: str
    account_display_name: str
    instrument_filter_mode: str
    major_usd_pairs: List[str]
    research_reliable_pairs: List[str]
    technical_scout_pairs: List[str]
    oanda_env: str
    allow_live: bool
    request_timeout_seconds: int
    http_max_retries: int
    http_retry_sleep_seconds: float
    disable_http_keepalive: bool

    execute_trades: bool
    auto_execute_research_actions: bool
    scan_on_launch: bool
    mark_past_calls_on_launch: bool  # deprecated/no-op in v5.3; launch scans never consume scheduled slots
    calls_per_trading_day: int
    call_times_ny: List[str]
    friday_call_times_ny: List[str]
    schedule_window_minutes: int
    min_minutes_between_research_scans: int
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
    event_trigger_research_enabled: bool
    event_min_minutes_between_research_scans: int
    event_scout_trades_enabled: bool
    event_require_research_permission: bool
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

    shutdown_mode: str
    shutdown_skip_research: bool
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
        data_dir = Path(cfg_str("FOREX_DATA_DIR", "FOREX_BOT_DATA_DIR", default=str(SCRIPT_DIR / "data" / "technical_scout_manager")))
        calls = cfg_int("FOREX_RESEARCH_CALLS_PER_DAY", default=12)
        minute_offset = cfg_int("FOREX_RESEARCH_CALL_MINUTE_OFFSET", default=15)
        raw_times = cfg_list("FOREX_RESEARCH_CALL_TIMES_NY", default=[])
        custom_call_times = [t for t in raw_times if valid_hhmm(t)]
        call_times = custom_call_times or default_call_times_for_count(calls, minute_offset)

        raw_friday_times = cfg_list("FOREX_FRIDAY_RESEARCH_CALL_TIMES_NY", "FRIDAY_RESEARCH_CALL_TIMES_NY", default=[])
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
            research_api_api_key=cfg_str("RESEARCH_API_API_KEY", "research_api_api_key", "research_apiapi", "RESEARCH_API_KEY", default=""),
            research_api_model=cfg_str("RESEARCH_API_MODEL", "research_api_model", default="research-4.1-mini"),
            enable_research_api_web_search=cfg_bool("ENABLE_RESEARCH_API_WEB_SEARCH", "FOREX_ENABLE_RESEARCH_API_WEB_SEARCH", default=True),
            research_api_timeout_seconds=cfg_int("RESEARCH_API_TIMEOUT_SECONDS", default=180),

            oanda_api_key=cfg_str("OANDA_API_KEY", "OANDA_API_TOKEN", "oanda_api_key", "oandaapi", default=""),
            oanda_account_id=cfg_str("OANDA_ACCOUNT_ID", "oanda_account_id", "OANDA_ACCOUNT", default=""),
            account_lane=cfg_str("FOREX_ACCOUNT_LANE", default="single"),
            account_display_name=cfg_str("FOREX_ACCOUNT_DISPLAY_NAME", default="OANDA_ACCOUNT_ID"),
            instrument_filter_mode=cfg_str("FOREX_INSTRUMENT_FILTER_MODE", default="all"),
            major_usd_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_MAJOR_USD_PAIRS", default=["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"]) if normalize_instrument(x)],
            research_reliable_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_RESEARCH_RELIABLE_PAIRS", default=cfg_list("FOREX_MAJOR_USD_PAIRS", default=["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"])) if normalize_instrument(x)],
            technical_scout_pairs=[normalize_instrument(x) for x in cfg_list("FOREX_TECHNICAL_SCOUT_PAIRS", default=cfg_list("FOREX_SCOUT_VOLATILE_PAIRS", default=[])) if normalize_instrument(x)],
            oanda_env=oanda_env,
            allow_live=cfg_bool("FOREX_ALLOW_LIVE", "ALLOW_LIVE", default=False),
            request_timeout_seconds=cfg_int("REQUEST_TIMEOUT_SECONDS", default=30),
            http_max_retries=cfg_int("FOREX_HTTP_MAX_RETRIES", "HTTP_MAX_RETRIES", default=3),
            http_retry_sleep_seconds=cfg_float("FOREX_HTTP_RETRY_SLEEP_SECONDS", "HTTP_RETRY_SLEEP_SECONDS", default=2.0),
            disable_http_keepalive=cfg_bool("FOREX_DISABLE_HTTP_KEEPALIVE", "DISABLE_HTTP_KEEPALIVE", default=True),

            execute_trades=cfg_bool("FOREX_EXECUTE_TRADES", "EXECUTE_TRADES", default=execute_default),
            auto_execute_research_actions=cfg_bool("FOREX_AUTO_EXECUTE_RESEARCH_ACTIONS", "AUTO_EXECUTE_RESEARCH_ACTIONS", default=True),
            scan_on_launch=cfg_bool("FOREX_SCAN_ON_LAUNCH", "SCAN_ON_LAUNCH", default=True),
            mark_past_calls_on_launch=False,
            calls_per_trading_day=calls,
            call_times_ny=call_times,
            friday_call_times_ny=friday_call_times,
            schedule_window_minutes=cfg_int("FOREX_SCHEDULE_WINDOW_MINUTES", "SCHEDULE_WINDOW_MINUTES", default=10),
            min_minutes_between_research_scans=cfg_int("FOREX_MIN_MINUTES_BETWEEN_RESEARCH_SCANS", "MIN_MINUTES_BETWEEN_RESEARCH_SCANS", default=75),
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
            event_trigger_research_enabled=cfg_bool("FOREX_EVENT_TRIGGERED_RESEARCH_ENABLED", default=True),
            event_min_minutes_between_research_scans=cfg_int("FOREX_EVENT_MIN_MINUTES_BETWEEN_RESEARCH_SCANS", default=30),
            event_scout_trades_enabled=cfg_bool("FOREX_EVENT_SCOUT_TRADES_ENABLED", default=True),
            event_require_research_permission=cfg_bool("FOREX_EVENT_REQUIRE_RESEARCH_PERMISSION", default=False),
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
            max_risk_pct_per_trade=cfg_float("FOREX_MAX_RESEARCH_RISK_PCT_PER_TRADE", default=4.5),
            min_risk_pct_per_trade=cfg_float("FOREX_MIN_RESEARCH_RISK_PCT", default=0.10),
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

            shutdown_mode=normalize_shutdown_mode(cfg_str("FOREX_SHUTDOWN_MODE", "FOREX_DRAIN_MODE", default="off")),
            shutdown_skip_research=cfg_bool("FOREX_SHUTDOWN_SKIP_RESEARCH", "FOREX_DRAIN_SKIP_RESEARCH", default=True),
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
    "has_usd", "is_cross_pair", "outlook_confidence", "risk_pct", "margin_intensity",
    "expected_hold_hours", "reason", "why_now", "raw_json",
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
    "best_window_minutes", "best_net_pips", "best_spread_pips", "triggered_research", "scout_attempts",
    "reason", "raw_json",
]
EVENT_WINDOW_FIELDS = [
    "time_utc", "time_ny", "instrument", "theme", "window_minutes", "direction", "net_pips",
    "mid_move_pips", "spread_avg_pips", "move_to_spread_ratio", "start_utc", "end_utc", "raw_json",
]

EVENT_SCAN_SUMMARY_FIELDS = [
    "time_utc", "time_ny", "reason", "scanned", "tradeable", "not_tradeable",
    "spread_rejected", "candle_errors", "no_windows", "below_pips", "below_ratio",
    "candidate_windows", "signals", "themes", "watch_basket", "triggered_themes",
    "pressure_watch", "pressure_trade", "exhaustion_shadow", "exhaustion_hybrid",
    "scout_attempts", "top_candidate", "top_reject_reason", "raw_json",
]

PRESSURE_WATCHLIST_FIELDS = [
    "time_utc", "time_ny", "instrument", "direction", "pressure_score",
    "entry_mode", "window_minutes", "net_pips", "threshold_pips", "move_to_spread_ratio",
    "spread_avg_pips", "basket_score", "currency_pressure", "reason", "raw_json",
]

EXHAUSTION_WATCHLIST_FIELDS = [
    "time_utc", "time_ny", "instrument", "direction", "status",
    "rule_count", "rules", "momentum_5_atr", "momentum_15_atr",
    "momentum_30_atr", "strength_gap_15", "atr240_pips",
    "start_utc", "end_utc", "reason", "raw_json",
]

MOVEMENT_CAMPAIGN_FIELDS = [
    "time_utc", "time_ny", "campaign_key", "movement_key", "instrument", "direction", "theme",
    "campaign_status", "campaign_action", "entries", "adds", "attempts", "accepted", "skipped",
    "last_signal_score", "last_net_pips", "last_ratio", "current_unrealized_pl",
    "margin_used_pct", "reason", "raw_json",
]

POSITION_HEALTH_FIELDS = [
    "time_utc", "time_ny", "trade_id", "instrument", "direction", "current_units",
    "unrealized_pl", "max_unrealized_pl", "mfe_retained_pct", "campaign_key",
    "health_score", "health_action", "reason", "raw_json",
]

COMPOUND_ATTEMPT_FIELDS = [
    "time_utc", "time_ny", "campaign_key", "instrument", "direction", "theme",
    "attempt_type", "allowed", "current_unrealized_pl", "entries", "adds",
    "risk_pct", "margin_used_pct", "reason", "raw_json",
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

SCOUT_AUDIT_FIELDS = [
    "time_utc", "time_ny", "run_id", "script_name", "script_version", "account_id", "account_lane",
    "decision_stage", "status", "action_type", "instrument", "direction", "theme", "movement_key",
    "campaign_key", "campaign_attempt_type", "window_minutes", "net_pips", "threshold_pips",
    "move_to_spread_ratio", "spread_avg_pips", "pressure_score", "scout_ev_score",
    "strict_gate_enabled", "strict_gate_passed", "pre_spike_pressure",
    "pressure_cluster_instruments", "pressure_cluster_pairs_dirs", "pressure_cluster_direction_instruments",
    "value_expected_usd", "value_return_pct", "value_net_after_spread_pips", "value_density_pct",
    "risk_pct", "risk_usd", "margin_usd", "units", "fill_price", "reason", "reject_reason", "raw_json",
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


def technical_exhaustion_features_from_rows(
    rows: Sequence[Dict[str, Any]],
    pip_size: float,
) -> Optional[Dict[str, Any]]:
    """Reproduce the study's closed-5m momentum/ATR features without pandas."""
    if pip_size <= 0:
        return None
    buckets: Dict[dt.datetime, Dict[str, Any]] = {}
    for row in rows:
        if not bool(row.get("complete", True)):
            continue
        try:
            timestamp = dt.datetime.fromisoformat(
                str(row.get("time", "")).replace("Z", "+00:00")
            ).astimezone(UTC)
            bucket_time = timestamp.replace(
                minute=timestamp.minute - timestamp.minute % 5,
                second=0,
                microsecond=0,
            )
            current = buckets.get(bucket_time)
            if current is None:
                buckets[bucket_time] = {
                    "time": bucket_time,
                    "open": safe_float(row.get("mid_o")),
                    "high": safe_float(row.get("mid_h", row.get("mid_c"))),
                    "low": safe_float(row.get("mid_l", row.get("mid_c"))),
                    "close": safe_float(row.get("mid_c")),
                    "spread_sum": (
                        safe_float(row.get("spread_o_pips"))
                        + safe_float(row.get("spread_c_pips"))
                    ) / 2.0,
                    "volume": safe_float(row.get("volume")),
                    "count": 1,
                }
            else:
                current["high"] = max(
                    safe_float(current.get("high")),
                    safe_float(row.get("mid_h", row.get("mid_c"))),
                )
                current["low"] = min(
                    safe_float(current.get("low")),
                    safe_float(row.get("mid_l", row.get("mid_c"))),
                )
                current["close"] = safe_float(row.get("mid_c"))
                current["spread_sum"] = safe_float(
                    current.get("spread_sum")
                ) + (
                    safe_float(row.get("spread_o_pips"))
                    + safe_float(row.get("spread_c_pips"))
                ) / 2.0
                current["volume"] = safe_float(
                    current.get("volume")
                ) + safe_float(row.get("volume"))
                current["count"] = safe_int(current.get("count")) + 1
        except Exception:
            continue
    bars = [
        buckets[key]
        for key in sorted(buckets)
        if safe_int(buckets[key].get("count")) >= 5
    ]
    if len(bars) < 49:
        return None
    true_ranges = []
    for prior, current in zip(bars[:-1], bars[1:]):
        previous_close = safe_float(prior.get("close"))
        high = safe_float(current.get("high"))
        low = safe_float(current.get("low"))
        true_ranges.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
            / pip_size
        )
    atr240_pips = sum(true_ranges[-48:]) / 48.0
    if not math.isfinite(atr240_pips) or atr240_pips <= 0:
        return None
    close = safe_float(bars[-1].get("close"))
    closes = [safe_float(bar.get("close")) for bar in bars]
    highs = [safe_float(bar.get("high")) for bar in bars]
    lows = [safe_float(bar.get("low")) for bar in bars]
    spreads = [
        safe_float(bar.get("spread_sum"))
        / max(1, safe_int(bar.get("count")))
        for bar in bars
    ]
    volumes = [safe_float(bar.get("volume")) for bar in bars]

    def momentum(bars_back: int) -> float:
        return ((close - safe_float(bars[-1 - bars_back].get("close"))) / pip_size) / atr240_pips

    def simple_rsi(values: Sequence[float], window: int = 14) -> float:
        changes = [
            values[index] - values[index - 1]
            for index in range(len(values) - window, len(values))
        ]
        gains = [max(change, 0.0) for change in changes]
        losses = [max(-change, 0.0) for change in changes]
        average_gain = sum(gains) / max(len(gains), 1)
        average_loss = sum(losses) / max(len(losses), 1)
        if average_loss <= 0:
            return 100.0
        rs = average_gain / average_loss
        return 100.0 - 100.0 / (1.0 + rs)

    close_15 = safe_float(bars[-4].get("close"))
    if close <= 0 or close_15 <= 0:
        return None
    momentum_15_pips = (closes[-1] - closes[-4]) / pip_size
    prior_momentum_15_pips = (closes[-4] - closes[-7]) / pip_size
    atr15_pips = sum(true_ranges[-3:]) / 3.0
    recent_six = closes[-6:]
    recent_12 = closes[-12:]
    recent_48 = closes[-48:]
    sma7 = sum(closes[-7:]) / 7.0
    sma8 = sum(closes[-8:]) / 8.0
    sma30 = sum(closes[-30:]) / 30.0
    prior_sma30 = sum(closes[-35:-5]) / 30.0
    range_60 = max(recent_12) - min(recent_12)
    range_240 = max(recent_48) - min(recent_48)
    spread_median_60 = sorted(spreads[-12:])[len(spreads[-12:]) // 2]
    volume_window = volumes[-6:]
    volume_mean = sum(volume_window) / len(volume_window)
    volume_variance = sum(
        (value - volume_mean) ** 2 for value in volume_window
    ) / max(1, len(volume_window) - 1)
    volume_std = math.sqrt(volume_variance)

    def ema_series(
        values: Sequence[float],
        span: int,
        min_periods: int,
    ) -> List[float]:
        alpha = 2.0 / (span + 1.0)
        ema_value: Optional[float] = None
        out: List[float] = []
        for index, value in enumerate(values):
            ema_value = value if ema_value is None else (
                alpha * value + (1.0 - alpha) * ema_value
            )
            out.append(ema_value if index + 1 >= min_periods else math.nan)
        return out

    def finite_or_zero(value: float) -> float:
        return value if math.isfinite(value) else 0.0

    ema8 = ema_series(closes, 8, 8)
    ema12 = ema_series(closes, 12, 12)
    ema21 = ema_series(closes, 21, 21)
    ema26 = ema_series(closes, 26, 26)
    ema55 = ema_series(closes, 55, 30)
    macd_values = [
        (a - b) if math.isfinite(a) and math.isfinite(b) else math.nan
        for a, b in zip(ema12, ema26)
    ]
    macd_signal = ema_series(
        [value if math.isfinite(value) else 0.0 for value in macd_values],
        9,
        9,
    )
    macd = finite_or_zero(macd_values[-1])
    macd_hist = finite_or_zero(macd_values[-1] - macd_signal[-1])
    ema21_slope = finite_or_zero(ema21[-1] - ema21[-4])
    ema55_slope = finite_or_zero(ema55[-1] - ema55[-7])
    ma_stack_score = (
        1.0
        if ema8[-1] > ema21[-1] > ema55[-1]
        else (-1.0 if ema8[-1] < ema21[-1] < ema55[-1] else 0.0)
    )

    prior_high_60 = max(highs[-13:-1])
    prior_low_60 = min(lows[-13:-1])
    prior_high_240 = max(highs[-49:-1])
    prior_low_240 = min(lows[-49:-1])

    def donchian_position(prior_high: float, prior_low: float) -> float:
        width = prior_high - prior_low
        if width <= 0:
            return 0.0
        return (((close - prior_low) / width) - 0.5) * 2.0

    def donchian_breakout(prior_high: float, prior_low: float) -> float:
        up = max(0.0, ((close - prior_high) / pip_size) / atr240_pips)
        down = max(0.0, ((prior_low - close) / pip_size) / atr240_pips)
        return up - down

    trend_sign = 1.0 if closes[-1] >= closes[-13] else -1.0
    recent_steps = [
        closes[index] - closes[index - 1]
        for index in range(len(closes) - 12, len(closes))
    ]
    trend_consistency_60 = sum(
        1.0
        for change in recent_steps
        if (change >= 0 and trend_sign >= 0)
        or (change < 0 and trend_sign < 0)
    ) / max(1, len(recent_steps))
    log_returns = [
        math.log(closes[index] / closes[index - 1])
        for index in range(1, len(closes))
        if closes[index] > 0 and closes[index - 1] > 0
    ]

    def sample_std(values: Sequence[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return math.sqrt(
            sum((value - mean) ** 2 for value in values)
            / max(1, len(values) - 1)
        )

    rv30 = sample_std(log_returns[-6:])
    rv240 = sample_std(log_returns[-48:])
    result = {
        "momentum_5_atr": momentum(1),
        "momentum_15_atr": momentum(3),
        "momentum_30_atr": momentum(6),
        "momentum_60_atr": momentum(12),
        "acceleration_15_atr": (
            momentum_15_pips - prior_momentum_15_pips
        ) / atr240_pips,
        "sma7_minus_8_atr": (
            (sma7 - sma8) / pip_size
        ) / atr240_pips,
        "sma30_slope_5_atr": (
            (sma30 - prior_sma30) / pip_size
        ) / atr240_pips,
        "ema8_minus_ema21_atr": finite_or_zero(
            ((ema8[-1] - ema21[-1]) / pip_size) / atr240_pips
        ),
        "ema21_slope_15_atr": (ema21_slope / pip_size) / atr240_pips,
        "ema55_slope_30_atr": (ema55_slope / pip_size) / atr240_pips,
        "macd_atr": (macd / pip_size) / atr240_pips,
        "macd_hist_atr": (macd_hist / pip_size) / atr240_pips,
        "ma_stack_score": ma_stack_score,
        "rsi14_centered": (simple_rsi(closes, 14) - 50.0) / 50.0,
        "atr15_to_atr240": atr15_pips / atr240_pips,
        "compression_30": (
            (max(recent_six) - min(recent_six)) / pip_size
        ) / (atr240_pips * math.sqrt(6)),
        "donchian_60_position_centered": donchian_position(
            prior_high_60,
            prior_low_60,
        ),
        "donchian_60_breakout_atr": donchian_breakout(
            prior_high_60,
            prior_low_60,
        ),
        "donchian_240_position_centered": donchian_position(
            prior_high_240,
            prior_low_240,
        ),
        "donchian_240_breakout_atr": donchian_breakout(
            prior_high_240,
            prior_low_240,
        ),
        "trend_consistency_60": trend_consistency_60,
        "realized_vol_ratio_30_240": (
            rv30 / rv240 if rv240 > 0 else 1.0
        ),
        "range_position_240_centered": (
            (
                (close - min(recent_48)) / range_240
                if range_240 > 0
                else 0.5
            )
            - 0.5
        ) * 2.0,
        "range_position_60_centered": (
            (
                (close - min(recent_12)) / range_60
                if range_60 > 0
                else 0.5
            )
            - 0.5
        ) * 2.0,
        "spread_ratio_60": spreads[-1] / max(spread_median_60, 0.1),
        "spread_pips": spreads[-1],
        "volume_z_30": (
            (volumes[-1] - volume_mean) / volume_std
            if volume_std > 0
            else 0.0
        ),
        "pair_log_return_15": math.log(close / close_15),
        "pair_log_return_60": math.log(
            close / max(safe_float(bars[-13].get("close")), 1e-12)
        ),
        "atr240_pips": atr240_pips,
        "close": close,
        "start_utc": bars[-7]["time"].isoformat(),
        "end_utc": bars[-1]["time"].isoformat(),
    }
    return result


def technical_exhaustion_rule_matches(
    features: Dict[str, Any],
    strength_gap_15: float,
    *,
    m5_min: float = 0.80,
    m15_min: float = 1.35,
    m30_min: float = 1.88,
    strength_gap_min: float = 0.00045,
) -> List[str]:
    """Return only the upper-tail rules stable in both rolling validations."""
    m5 = safe_float(features.get("momentum_5_atr"))
    m15 = safe_float(features.get("momentum_15_atr"))
    m30 = safe_float(features.get("momentum_30_atr"))
    rules = []
    if m30 >= m30_min and strength_gap_15 >= strength_gap_min:
        rules.append("m30_strength15_upper")
    if m15 >= m15_min and strength_gap_15 >= strength_gap_min:
        rules.append("m15_strength15_upper")
    if m5 >= m5_min and strength_gap_15 >= strength_gap_min:
        rules.append("m5_strength15_upper")
    if m15 >= m15_min and m30 >= m30_min:
        rules.append("m15_m30_upper")
    if m5 >= m5_min and m30 >= m30_min:
        rules.append("m5_m30_upper")
    return rules


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
            "clientExtensions": {"tag": tag[:128], "comment": "oanda_technical_scout_manager_v5_17"},
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
# ResearchAPI Responses API
# =============================================================================

DECISION_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "as_of": {"type": "string"},
        "market_summary": {"type": "string"},
        "portfolio_bias": {"type": "string"},
        "portfolio_mode": {"type": "string"},
        "currency_ranking": {"type": "array", "items": {"type": "string"}},
        "why_mixed_usd_exposure_is_allowed": {"type": "string"},
        "non_usd_cross_pair_review": {"type": "string"},
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
                    "action": {"type": "string", "enum": ["OPEN", "WATCH", "REJECT"]},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT", "NONE"]},
                    "outlook_confidence": {"type": "number"},
                    "risk_pct": {"type": "number"},
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
                    "action": {"type": "string", "enum": ["OPEN", "FLIP", "CLOSE", "PARTIAL_CLOSE", "TIGHTEN", "HOLD"]},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT", "NONE"]},
                    "trade_id": {"type": ["string", "null"]},
                    "outlook_confidence": {"type": "number"},
                    "risk_pct": {"type": "number"},
                    "partial_close_pct": {"type": "number"},
                    "stop_loss": {"type": ["number", "null"]},
                    "take_profit": {"type": ["number", "null"]},
                    "trailing_stop_pips": {"type": ["number", "null"]},
                    "reason": {"type": "string"},
                },
            },
        },
    },
    "required": ["as_of", "market_summary", "portfolio_bias", "risk_notes", "open_position_actions", "new_trade_candidates", "orders_to_execute"],
}


SYSTEM_PROMPT = """You are the decision engine for an always-on OANDA leveraged forex portfolio manager.

You must evaluate the entire account on every call. This is not just an entry scanner.
Manage existing positions first, then consider new positions across all supplied tradeable forex pairs.

Use live/current macro, central-bank, rates, geopolitical, risk-sentiment, commodities, and country-specific news when web search is available. Do not invent news; if news is uncertain, say uncertainty in reason/risk_notes.

Core job:
1. Decide HOLD, TIGHTEN, PARTIAL_CLOSE, CLOSE, or FLIP for every open trade.
2. open_position_actions is mandatory portfolio management: return exactly one action for every open trade supplied in open_trades. If no change is warranted, return HOLD with the trade_id/instrument/direction and current outlook_confidence. Never omit an open trade just because you have no change.
3. Rank all possible new positions across the supplied pairs before choosing executable orders. USD pairs and non-USD crosses compete in one global ranking.
4. Recommend executable orders only when the outlook is strong enough.
5. Set outlook_confidence from 0 to 100 based on the quality and durability of the current outlook.
6. Set risk_pct directly. The script will cap your requested risk if it exceeds account guardrails.
7. Use margin aggressively when the outlook is strong, but avoid redundant exposure and correlated overstacking. Do not reserve risk budget by category.
8. If account margin_used_pct is materially below target_margin_used_pct and clean opportunities exist, prefer deploying more risk through the best-ranked non-duplicative trades rather than staying under-allocated. Do not force weak trades just to use margin.
9. Take profit, tighten, partially close, or fully close if the outlook has weakened, reversed, become less asymmetric, or margin can be better used elsewhere.
10. FLIP only when the opposite thesis is clearly stronger than the existing thesis.
11. State portfolio_mode as one of USD_BULLISH, USD_BEARISH, RELATIVE_VALUE, DEFENSIVE, or MIXED_UNCLEAR. If holding both long-USD and short-USD exposures, use RELATIVE_VALUE and explain the currency ranking.
12. Provide non_usd_cross_pair_review explaining the best non-USD crosses considered and why they were accepted, watched, or rejected, but do not force cross-pair trades if USD pairs are objectively better.

Do not use fixed technical rules. Do not require local-model agreement. Price movement alone is not a thesis.
Every OPEN or FLIP must include stop_loss. Include take_profit and/or trailing_stop_pips when useful.
Use OANDA instrument format with underscores, e.g. USD_JPY not USD/JPY. Never return concatenated symbols like USDJPY.
No category quota: if the best trades after the full scan are all USD pairs, orders_to_execute may all be USD pairs. If the best trades are all non-USD crosses, orders_to_execute may all be non-USD crosses. Do not add placeholder REJECT rows just to show coverage.
If an instrument is already open, manage it through open_position_actions, not orders_to_execute. Do not put a duplicate OPEN for an already-open instrument.
If you are unsure about an open trade, HOLD or TIGHTEN; do not omit it.
orders_to_execute must be the final globally ranked executable list after evaluating all supplied possibilities; do not output category-first ordering.
If shutdown_mode is ACTIVE in the user/context packet, do not recommend any new OPEN orders. Manage only existing positions by tightening, taking partial profits, closing weak positions, or holding briefly with protected downside.

Also return event_permissions for fast local event scanning. These are not immediate orders. They are small pre-authorizations for a later local M5 scanner if a synchronized basket move begins before the next scheduled RESEARCH call. Use themes like USD_SELLOFF, USD_RALLY, JPY_STRENGTH, JPY_WEAKNESS, CHF_STRENGTH, CHF_WEAKNESS, RISK_ON, RISK_OFF, COMMODITY_CURRENCY_STRENGTH, COMMODITY_CURRENCY_WEAKNESS. For each permission include allowed_pairs, allowed_directions, max_scout_risk_pct, expires_hours, requires_basket_confirmation, and reason. Only grant permissions that match your current macro thesis.

Return JSON only matching the requested schema."""


class ResearchAPIClient:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.headers = {
            "Authorization": f"Bearer {cfg.research_api_api_key}",
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
        if not self.cfg.research_api_api_key:
            raise RuntimeError("Missing RESEARCH_API_API_KEY/research_apiapi in creds or environment")

    def create_decision(self, packet: Dict[str, Any]) -> Dict[str, Any]:
        self.require_auth()
        input_payload = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(packet, indent=2, sort_keys=True, default=str)},
        ]
        body: Dict[str, Any] = {
            "model": self.cfg.research_api_model,
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
        if self.cfg.enable_research_api_web_search:
            body["tools"] = [{"type": "web_search_preview"}]
        try:
            return self._post_response(body)
        except Exception as exc:
            if self.cfg.enable_research_api_web_search:
                log(f"[WARN] ResearchAPI web-search request failed, retrying without web_search tool: {exc}")
                body.pop("tools", None)
                return self._post_response(body)
            raise

    def _post_response(self, body: Dict[str, Any]) -> Dict[str, Any]:
        attempts = max(1, int(self.cfg.http_max_retries) + 1)
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                if self.cfg.disable_http_keepalive:
                    self.reset_session()
                resp = self.s.post(
                    "https://disabled.local/v1/responses",
                    data=json.dumps(body),
                    timeout=self.cfg.research_api_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise RuntimeError(f"ResearchAPI responses network failure after {attempts} attempts: {exc}") from exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                log(f"[WARN] ResearchAPI transient network error; retry {attempt}/{attempts - 1} in {wait:.1f}s: {exc}")
                self.reset_session()
                time.sleep(wait)
                continue

            if resp.status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
                if attempt < attempts:
                    wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                    log(f"[WARN] ResearchAPI transient HTTP {resp.status_code}; retry {attempt}/{attempts - 1} in {wait:.1f}s")
                    time.sleep(wait)
                    continue

            if resp.status_code >= 400:
                raise RuntimeError(f"ResearchAPI responses failed {resp.status_code}: {resp.text[:1600]}")
            try:
                data = resp.json()
                if self.cfg.disable_http_keepalive:
                    resp.close()
            except ValueError as exc:
                last_exc = exc
                if attempt >= attempts:
                    raise RuntimeError(f"ResearchAPI responses returned non-JSON after {attempts} attempts: {resp.text[:500]}") from exc
                wait = max(0.25, float(self.cfg.http_retry_sleep_seconds)) * attempt
                log(f"[WARN] ResearchAPI non-JSON response; retry {attempt}/{attempts - 1} in {wait:.1f}s")
                time.sleep(wait)
                continue

            text = extract_response_text(data)
            if not text:
                raise RuntimeError("ResearchAPI response did not include output text")
            try:
                return json.loads(text)
            except Exception:
                # Fallback: extract outermost JSON object if surrounding text appears.
                m = re.search(r"\{.*\}", text, flags=re.S)
                if m:
                    return json.loads(m.group(0))
                raise
        raise RuntimeError(f"ResearchAPI responses failed unexpectedly: {last_exc}")


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
        self.research_api = ResearchAPIClient(cfg)
        self.instrument_meta: Dict[str, InstrumentMeta] = {}
        self.instruments: List[str] = []
        self._scout_profile_cache: Dict[str, Dict[str, Any]] = {}
        self._scout_profile_loaded_at: float = 0.0
        self._promoted_model_manifest_mtime: int = -1
        self._promoted_model_manifest: Dict[str, Any] = {}
        self._promoted_model_bundle: Dict[str, Any] = {}
        self._promoted_model_manifest_log_key: str = ""
        self._major_move_factor_manifest_mtime: int = -1
        self._major_move_factor_manifest: Dict[str, Any] = {}
        self._major_move_factor_bundle: Dict[str, Any] = {}
        self._ensemble_shadow_manifest_mtime: int = -1
        self._ensemble_shadow_manifest: Dict[str, Any] = {}
        self._ensemble_shadow_bundle: Dict[str, Any] = {}
        self._research_volatility_cache: Dict[str, Dict[str, float]] = {}
        self._macro_bias_cache: Dict[str, Any] = {}
        self._macro_bias_cache_mtime: int = -1
        self.lane_label = str(getattr(cfg, "account_lane", "single") or "single")
        self.ensure_dirs()
        self.full_logger = FullEventLogger(cfg)
        self.sync_promoted_model_manifest()
        self.sync_major_move_factor_manifest()
        self.sync_ensemble_shadow_manifest()
        try:
            self.full_logger.log_event("script_start", status="starting", account_id=getattr(cfg, "oanda_account_id", ""), reason="manager initialized", raw={"config": self.redacted_config()})
        except Exception:
            pass

    def lane_prefix(self) -> str:
        return f"[{self.lane_label}] " if self.lane_label and self.lane_label != "single" else ""

    def ensure_dirs(self) -> None:
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self.cfg.raw_dir.mkdir(parents=True, exist_ok=True)

    def sync_promoted_model_manifest(self) -> Dict[str, Any]:
        """Host the research champion without activating an ungated model."""
        raw_path = cfg_str(
            "FOREX_TECH_PROMOTED_MODEL_MANIFEST",
            default=str(
                SCRIPT_DIR
                / "data"
                / "oanda_training_manager"
                / "promotions"
                / "technical_production.json"
            ),
        )
        path = Path(raw_path)
        if not path.exists():
            return self._promoted_model_manifest
        try:
            mtime = path.stat().st_mtime_ns
            if mtime == self._promoted_model_manifest_mtime:
                return self._promoted_model_manifest
            manifest = read_json(path, {})
            if not isinstance(manifest, dict):
                return self._promoted_model_manifest
            artifact = Path(str(manifest.get("model_artifact_path", "")))
            hosted = bool(artifact.exists() and artifact.is_file())
            activation = bool(
                manifest.get("technical_account_activation", False)
            )
            production_stage = str(manifest.get("stage", "")).lower() in {
                "production",
                "technical_production",
            }
            canary_shadow_allowed = bool(
                cfg_bool("FOREX_TECH_ALLOW_CANARY_SHADOW_MANIFEST", default=False)
                and str(getattr(self.cfg, "account_lane", "")).lower()
                in {"canary", "primary_challenger"}
                and str(manifest.get("stage", "")).lower()
                in {"shadow", "validated", "research_leader"}
            )
            manifest["hosted_by_technical_account"] = hosted
            manifest["activation_effective"] = bool(
                hosted
                and (
                    (activation and production_stage)
                    or canary_shadow_allowed
                )
            )
            manifest["production_stage_required"] = not canary_shadow_allowed
            manifest["canary_shadow_activation"] = bool(
                hosted and canary_shadow_allowed
            )
            bundle: Dict[str, Any] = {}
            if manifest["activation_effective"] and joblib is not None:
                try:
                    loaded = joblib.load(artifact)
                    if isinstance(loaded, dict):
                        bundle = loaded
                except Exception as exc:
                    self.log_error("load promoted production model", exc)
                    manifest["activation_effective"] = False
                    manifest["model_load_error"] = str(exc)[:500]
            self._promoted_model_manifest = manifest
            self._promoted_model_bundle = bundle
            self._promoted_model_manifest_mtime = mtime
            state = self.load_state()
            state["promoted_model_manifest"] = manifest
            state["promoted_model_manifest_path"] = str(path)
            state["promoted_model_last_sync_utc"] = iso_utc()
            self.save_state(state)
            try:
                log_key = json_dumps({
                    "path": str(path),
                    "experiment_id": manifest.get("experiment_id", ""),
                    "candidate_id": manifest.get("candidate_id", ""),
                    "stage": manifest.get("stage", ""),
                    "model_type": manifest.get("model_type", ""),
                    "target": manifest.get("target", ""),
                    "outcome": manifest.get("outcome", ""),
                    "artifact": manifest.get("model_artifact_path", ""),
                    "activation_effective": manifest.get("activation_effective", False),
                    "hosted": hosted,
                    "activation": activation,
                })
                if log_key != self._promoted_model_manifest_log_key:
                    self._promoted_model_manifest_log_key = log_key
                    self.full_logger.log_event(
                        "promoted_model_manifest",
                        status=(
                            "active"
                            if manifest["activation_effective"]
                            else "hosted_inactive"
                        ),
                        reason=str(manifest.get("reason", ""))[:1000],
                        result=(
                            f"experiment={manifest.get('experiment_id','')} "
                            f"stage={manifest.get('stage','')} "
                            f"hosted={hosted} activation={activation}"
                        ),
                        raw=manifest,
                    )
            except Exception:
                pass
            return manifest
        except Exception as exc:
            self.log_error("sync promoted model manifest", exc)
            return self._promoted_model_manifest

    def sync_major_move_factor_manifest(self) -> Dict[str, Any]:
        """Load the factor bundle for shadow scoring of existing signals."""
        path = Path(
            cfg_str(
                "FOREX_TECH_MAJOR_MOVE_FACTOR_MANIFEST",
                default=str(
                    SCRIPT_DIR
                    / "data"
                    / "oanda_training_manager"
                    / "promotions"
                    / "major_move_factor_production.json"
                ),
            )
        )
        if not path.exists() or joblib is None:
            return self._major_move_factor_manifest
        try:
            mtime = path.stat().st_mtime_ns
            if mtime == self._major_move_factor_manifest_mtime:
                return self._major_move_factor_manifest
            manifest = read_json(path, {})
            artifact = Path(str(manifest.get("model_artifact_path", "")))
            if not artifact.exists():
                return self._major_move_factor_manifest
            bundle = joblib.load(artifact)
            if not isinstance(bundle, dict):
                return self._major_move_factor_manifest
            manifest["hosted_by_technical_account"] = True
            manifest["standalone_execution_allowed"] = False
            self._major_move_factor_manifest = manifest
            self._major_move_factor_bundle = bundle
            self._major_move_factor_manifest_mtime = mtime
            self.full_logger.log_event(
                "major_move_factor_manifest",
                status="shadow_loaded",
                reason=str(manifest.get("reason", ""))[:1000],
                result=(
                    f"experiment={manifest.get('experiment_id','')} "
                    f"modifier={manifest.get('score_modifier_enabled',False)}"
                ),
                raw=manifest,
            )
            return manifest
        except Exception as exc:
            self.log_error("sync major move factor manifest", exc)
            return self._major_move_factor_manifest

    def sync_ensemble_shadow_manifest(self) -> Dict[str, Any]:
        """Load the multi-model ensemble for shadow scoring only.

        This deliberately does not activate entries.  It lets the technical
        account record how a validated model mixture would have scored each
        candidate while the current production champion remains in control.
        """
        path = Path(
            cfg_str(
                "FOREX_TECH_ENSEMBLE_SHADOW_MANIFEST",
                default=str(
                    SCRIPT_DIR
                    / "data"
                    / "oanda_training_manager"
                    / "promotions"
                    / "ensemble_shadow_candidate.json"
                ),
            )
        )
        if not path.exists() or joblib is None:
            return self._ensemble_shadow_manifest
        try:
            mtime = path.stat().st_mtime_ns
            if mtime == self._ensemble_shadow_manifest_mtime:
                return self._ensemble_shadow_manifest
            manifest = read_json(path, {})
            if not isinstance(manifest, dict):
                return self._ensemble_shadow_manifest
            artifact = Path(str(manifest.get("model_artifact_path", "")))
            if not artifact.exists() or not artifact.is_file():
                manifest["hosted_by_technical_account"] = False
                manifest["shadow_scoring_enabled"] = False
                self._ensemble_shadow_manifest = manifest
                self._ensemble_shadow_bundle = {}
                self._ensemble_shadow_manifest_mtime = mtime
                return manifest
            bundle = joblib.load(artifact)
            if not isinstance(bundle, dict):
                return self._ensemble_shadow_manifest
            manifest["hosted_by_technical_account"] = True
            manifest["shadow_scoring_enabled"] = True
            manifest["standalone_execution_allowed"] = False
            manifest["activation_effective"] = False
            self._ensemble_shadow_manifest = manifest
            self._ensemble_shadow_bundle = bundle
            self._ensemble_shadow_manifest_mtime = mtime
            try:
                self.full_logger.log_event(
                    "ensemble_shadow_manifest",
                    status="shadow_loaded",
                    reason=str(manifest.get("promotion_note", ""))[:1000],
                    result=(
                        f"experiment={manifest.get('experiment_id','')} "
                        f"members={len(bundle.get('members') or [])}"
                    ),
                    raw=manifest,
                )
            except Exception:
                pass
            return manifest
        except Exception as exc:
            self.log_error("sync ensemble shadow manifest", exc)
            return self._ensemble_shadow_manifest

    @staticmethod
    def ensemble_subset_ok(snapshot: Dict[str, Any], subset: str) -> bool:
        subset = str(subset or "all").lower()
        mapping = {
            "majors": "is_major_pair",
            "usd_pairs": "is_usd_pair",
            "non_usd": "is_non_usd_pair",
            "volatile": "is_volatile_pair",
            "exotic": "is_exotic_pair",
            "volatile_exotic": "is_volatile_or_exotic_pair",
            "non_usd_volatile": "is_non_usd_volatile_pair",
        }
        key = mapping.get(subset)
        if not key:
            return True
        return safe_float(snapshot.get(key), 0.0) >= 0.5

    def ensemble_shadow_signal_decision(
        self,
        signal: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return shadow-only ensemble score for a candidate signal."""
        manifest = self.sync_ensemble_shadow_manifest()
        bundle = self._ensemble_shadow_bundle
        if not manifest.get("shadow_scoring_enabled", False) or not bundle:
            return {"available": False, "reason": "no ensemble shadow bundle"}
        snapshot = signal.get("_technical_feature_snapshot")
        if not isinstance(snapshot, dict):
            snapshot = {}
        else:
            snapshot = dict(snapshot)
        instrument = normalize_instrument(signal.get("instrument", ""))
        if instrument:
            snapshot.update(self.live_calendar_features())
            snapshot.update(self.instrument_research_subset_features(instrument))
            snapshot["pair_taxonomy_primary"] = self.pair_taxonomy_primary(instrument)
            snapshot.update(self.pair_taxonomy_features(instrument))
            snapshot.update(self.carry_proxy_features(instrument))
            snapshot.update(self.live_regime_features(snapshot, instrument))
            snapshot.update(self.current_macro_feature_snapshot(instrument))
            snapshot.update(self.expanded_indicator_snapshot_features(snapshot))

        weighted_probability = 0.0
        total_weight = 0.0
        scored_members = []
        missing_members = []
        for member in bundle.get("members") or []:
            spec = dict(member.get("spec") or {})
            subset = str(spec.get("instrument_subset") or "all")
            if not self.ensemble_subset_ok(snapshot, subset):
                continue
            model = member.get("model")
            features = list(member.get("features") or [])
            if model is None or not features:
                continue
            values = []
            missing_feature = ""
            for feature in features:
                numeric = safe_float(
                    signal.get(feature, snapshot.get(feature)),
                    float("nan"),
                )
                if not math.isfinite(numeric):
                    missing_feature = feature
                    break
                values.append(numeric)
            if missing_feature:
                missing_members.append({
                    "experiment_id": member.get("experiment_id", ""),
                    "missing_feature": missing_feature,
                })
                continue
            try:
                probability = predict_positive_probability_named(
                    model,
                    features,
                    values,
                )
            except Exception as exc:
                self.log_error("score ensemble shadow member", exc)
                continue
            weight = safe_float(member.get("weight"), 1.0)
            weighted_probability += probability * weight
            total_weight += weight
            scored_members.append({
                "experiment_id": member.get("experiment_id", ""),
                "target": spec.get("target", ""),
                "probability": probability,
                "weight": weight,
            })
        ensemble_probability = (
            weighted_probability / total_weight
            if total_weight > 0
            else float("nan")
        )
        validation = manifest.get("validation", {})
        selected = validation.get("selected_threshold", {})
        threshold = safe_float(selected.get("probability_threshold"), 0.75)
        min_members = safe_int(validation.get("min_members"), 2)
        shadow_approved = bool(
            math.isfinite(ensemble_probability)
            and ensemble_probability >= threshold
            and len(scored_members) >= min_members
        )
        return {
            "available": bool(scored_members),
            "experiment_id": manifest.get("experiment_id", ""),
            "candidate_id": manifest.get("candidate_id", ""),
            "mode": manifest.get("ensemble_mode", "shadow"),
            "probability": ensemble_probability,
            "threshold": threshold,
            "member_count": len(scored_members),
            "min_members": min_members,
            "shadow_approved": shadow_approved,
            "members": scored_members[:10],
            "missing_members": missing_members[:10],
            "reason": (
                "ensemble shadow approved"
                if shadow_approved
                else "ensemble shadow rejected"
            ),
        }

    def production_model_allows_new_entries(self) -> Tuple[bool, str]:
        """Return whether this account may open new research-backed entries."""
        if not cfg_bool(
            "FOREX_TECH_REQUIRE_PRODUCTION_MODEL_FOR_NEW_ENTRIES",
            default=True,
        ):
            return True, "production model gate disabled by config"
        if str(getattr(self.cfg, "account_lane", "")).lower() not in {
            "tech",
            "vol",
            "canary",
        }:
            return True, "not a technical/canary lane"
        manifest = self.sync_promoted_model_manifest()
        if manifest.get("activation_effective", False):
            return True, (
                f"production model active experiment="
                f"{manifest.get('experiment_id','')}"
            )
        return False, (
            "new entries blocked: no active technical_production.json "
            "manifest with technical_account_activation=true"
        )

    def promoted_model_signal_decision(
        self,
        signal: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, Any]]:
        """Score a candidate signal with the active production model."""
        manifest = self.sync_promoted_model_manifest()
        bundle = self._promoted_model_bundle
        if not manifest.get("activation_effective", False) or not bundle:
            return False, {"reason": "no active production model bundle"}
        model = bundle.get("model")
        features = list(bundle.get("features") or manifest.get("features") or [])
        if model is None or not features:
            return False, {"reason": "production model bundle missing model/features"}
        snapshot = signal.get("_technical_feature_snapshot")
        if not isinstance(snapshot, dict):
            snapshot = {}
        else:
            snapshot = dict(snapshot)
        instrument = normalize_instrument(signal.get("instrument", ""))
        if instrument:
            snapshot.update(self.live_calendar_features())
            snapshot.update(self.instrument_research_subset_features(instrument))
            snapshot["pair_taxonomy_primary"] = self.pair_taxonomy_primary(instrument)
            snapshot.update(self.pair_taxonomy_features(instrument))
            snapshot.update(self.carry_proxy_features(instrument))
            snapshot.update(self.live_regime_features(snapshot, instrument))
            snapshot.update(self.current_macro_feature_snapshot(instrument))
            snapshot.update(self.expanded_indicator_snapshot_features(snapshot))

        segment_ok, segment_decision = self.promoted_model_segment_decision(
            manifest,
            snapshot,
        )
        if not segment_ok:
            return False, {
                "experiment_id": manifest.get("experiment_id", ""),
                "candidate_id": manifest.get("candidate_id", ""),
                **segment_decision,
            }
        macro_ok, macro_decision = self.promoted_model_macro_decision(
            signal,
            snapshot,
        )
        if not macro_ok:
            return False, {
                "experiment_id": manifest.get("experiment_id", ""),
                "candidate_id": manifest.get("candidate_id", ""),
                **macro_decision,
            }
        values = []
        for feature in features:
            value = signal.get(feature, snapshot.get(feature))
            numeric = safe_float(value, float("nan"))
            if not math.isfinite(numeric):
                return False, {"reason": f"missing model feature {feature}"}
            values.append(numeric)
        try:
            probability = predict_positive_probability_named(
                model,
                features,
                values,
            )
        except Exception as exc:
            self.log_error("score promoted production model", exc)
            return False, {"reason": f"model score error {type(exc).__name__}"}
        threshold = safe_float(
            (
                manifest.get("validation", {})
                .get("selected_threshold", {})
                .get("threshold")
            ),
            0.65,
        )
        target = str(manifest.get("target", "")).lower()
        signal_direction = str(signal.get("direction", "")).upper()
        m30 = safe_float(snapshot.get("momentum_30_atr", signal.get("momentum_30_atr")))
        if target.startswith("long_"):
            required_direction = "LONG"
        elif target.startswith("short_"):
            required_direction = "SHORT"
        elif target.startswith("continuation_"):
            required_direction = "LONG" if m30 >= 0 else "SHORT"
        elif target.startswith("reversal_"):
            required_direction = "SHORT" if m30 >= 0 else "LONG"
        else:
            required_direction = signal_direction
        direction_ok = not required_direction or required_direction == signal_direction
        allowed = bool(probability >= threshold and direction_ok)
        return allowed, {
            "experiment_id": manifest.get("experiment_id", ""),
            "candidate_id": manifest.get("candidate_id", ""),
            "probability": probability,
            "threshold": threshold,
            "target": manifest.get("target", ""),
            "required_direction": required_direction,
            "signal_direction": signal_direction,
            "direction_ok": direction_ok,
            "segment_gate": segment_decision.get("segment_gate"),
            "macro_gate": macro_decision.get("macro_gate"),
            "reason": (
                "model approved"
                if allowed
                else f"model rejected p={probability:.3f} threshold={threshold:.3f} direction_ok={direction_ok}"
            ),
        }

    def filter_signals_with_promoted_model(
        self,
        signals: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        approved: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for signal in signals:
            allowed, decision = self.promoted_model_signal_decision(signal)
            ensemble_shadow = self.ensemble_shadow_signal_decision(signal)
            enriched = dict(signal)
            decision["ensemble_shadow"] = ensemble_shadow
            enriched["_promoted_model_decision"] = decision
            enriched["_ensemble_shadow_decision"] = ensemble_shadow
            if (
                not allowed
                and cfg_bool(
                    "FOREX_TECH_PRE_SPIKE_PRESSURE_BYPASS_PRODUCTION_MODEL",
                    default=False,
                )
                and bool(signal.get("_strict_pre_spike_pressure_gate"))
                and bool(signal.get("_pre_spike_pressure"))
            ):
                decision = {
                    **decision,
                    "reason": (
                        "strict pre-spike pressure bypass approved "
                        f"cluster={safe_int(signal.get('_pressure_cluster_instruments'), 0)} "
                        f"value=${safe_float(signal.get('_value_weighted_expected_usd'), 0.0):.4f} "
                        f"return={safe_float(signal.get('_value_weighted_return_pct'), 0.0):.3f}%"
                    ),
                    "strict_pre_spike_pressure_bypass": True,
                }
                enriched["_promoted_model_decision"] = decision
                allowed = True
            if not allowed and cfg_bool(
                "FOREX_TECH_ALLOW_ENSEMBLE_SCOUT_FALLBACK",
                default=False,
            ):
                factor = (
                    signal.get("_major_move_factor")
                    if isinstance(signal.get("_major_move_factor"), dict)
                    else {}
                )
                factor_ok = bool(
                    factor
                    and safe_float(factor.get("event_probability"), 0.0)
                    >= max(
                        safe_float(factor.get("event_threshold"), 0.0),
                        cfg_float(
                            "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_FACTOR_MIN_PROB",
                            default=0.12,
                        ),
                    )
                    and bool(factor.get("direction_agreement", False))
                    and bool(factor.get("direction_confidence_gate", False))
                )
                is_pre_spike = bool(signal.get("_pre_spike_pressure")) or str(
                    signal.get("theme", "")
                ).upper().startswith("PRE_SPIKE_")
                is_exhaustion_hybrid = bool(
                    signal.get("_exhaustion_hybrid_scout")
                )
                movement_fallback_ok = cfg_bool(
                    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_ALLOW_MOVEMENT",
                    default=False,
                )
                fallback_candidate_ok = (
                    is_pre_spike or factor_ok or movement_fallback_ok
                )
                ensemble_probability = safe_float(
                    ensemble_shadow.get("probability"),
                    float("nan"),
                )
                fallback_min_probability = cfg_float(
                    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_MIN_PROB",
                    default=0.20,
                )
                if is_pre_spike:
                    fallback_min_probability = min(
                        fallback_min_probability,
                        cfg_float(
                            "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_PRE_SPIKE_MIN_PROB",
                            default=0.12,
                        ),
                    )
                if factor_ok:
                    fallback_min_probability = min(
                        fallback_min_probability,
                        cfg_float(
                            "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_FACTOR_MIN_PROB",
                            default=0.12,
                        ),
                    )
                if is_exhaustion_hybrid:
                    fallback_min_probability = max(
                        fallback_min_probability,
                        cfg_float(
                            "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_EXHAUSTION_MIN_PROB",
                            default=0.20,
                        ),
                    )
                if (
                    fallback_candidate_ok
                    and bool(ensemble_shadow.get("shadow_approved", False))
                    and math.isfinite(ensemble_probability)
                    and ensemble_probability >= fallback_min_probability
                ):
                    decision = {
                        **decision,
                        "reason": (
                            "ensemble scout fallback approved "
                            f"p={ensemble_probability:.3f} "
                            f"min={fallback_min_probability:.3f}"
                        ),
                        "ensemble_fallback_approved": True,
                        "ensemble_fallback_probability": ensemble_probability,
                        "ensemble_fallback_min_probability": fallback_min_probability,
                        "ensemble_fallback_pre_spike": is_pre_spike,
                        "ensemble_fallback_exhaustion_hybrid": is_exhaustion_hybrid,
                        "ensemble_fallback_factor_ok": factor_ok,
                    }
                    enriched["_promoted_model_decision"] = decision
                    enriched["_ensemble_scout_fallback"] = True
                    allowed = True
            if allowed:
                approved.append(enriched)
            else:
                rejected.append(enriched)
        return approved, rejected

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
        research_configured = bool(self.cfg.scan_on_launch or self.cfg.call_times_ny or self.cfg.friday_call_times_ny or self.cfg.event_trigger_research_enabled)
        if research_configured and not self.cfg.research_api_api_key:
            missing.append("RESEARCH_API_API_KEY/research_apiapi")
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
        # Technical/event-scout orders are broker actions, not portfolio actions.
        # Older configs used the portfolio auto-execute flag as a global kill switch;
        # that made this technical account print DRY-RUN even in OANDA practice.
        if self.cfg.account_lane not in {"tech", "vol"} and not self.cfg.auto_execute_research_actions:
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
        if str(getattr(self.cfg, "account_lane", "")).lower() == "tech":
            d = {k: v for k, v in d.items() if "research" not in k.lower() and "research_api" not in k.lower()}
            d["manager_mode"] = "technical_event_strength_scout"
        else:
            d["research_api_api_key"] = "***" if self.cfg.research_api_api_key else ""
        d["oanda_api_key"] = "***" if self.cfg.oanda_api_key else ""
        for k, v in list(d.items()):
            if isinstance(v, Path):
                d[k] = str(v)
        d["creds_loaded_path"] = str(CREDS_LOADED_PATH) if CREDS_LOADED_PATH else ""
        d["creds_load_error"] = CREDS_LOAD_ERROR
        d["config_loaded_path"] = "EMBEDDED_CONFIG inside manager script"
        d["config_load_error"] = CONFIG_LOAD_ERROR
        if str(getattr(self.cfg, "account_lane", "")).lower() == "tech":
            d["embedded_config"] = {
                k: v for k, v in CONFIG.items()
                if "research" not in k.lower() and "research_api" not in k.lower()
            }
        else:
            d["embedded_config"] = {k: ("***" if k in CREDENTIAL_SETTING_NAMES else v) for k, v in CONFIG.items()}
        print(json.dumps(d, indent=2, sort_keys=True))

    def load_state(self) -> Dict[str, Any]:
        state = read_json(self.cfg.state_path, {
            "last_research_runs": {},
            "last_monitor_utc": None,
            "last_research_scan_utc": None,
            "last_event_scan_utc": None,
            "last_event_research_utc": None,
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
        state.setdefault("last_research_runs", {})
        state.setdefault("last_monitor_utc", None)
        state.setdefault("last_research_scan_utc", None)
        state.setdefault("last_event_scan_utc", None)
        state.setdefault("last_event_research_utc", None)
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
        if mode in {"technical_all_except_exclude", "all_except_technical_exclude"}:
            blocked = {normalize_instrument(x) for x in cfg_list("FOREX_TECHNICAL_EXCLUDE_PAIRS", default=[]) if normalize_instrument(x)}
            return inst not in blocked
        if mode in {"major", "maj", "major_usd", "usd_majors", "research", "research_reliable", "reliable"}:
            allowed = set(self.cfg.research_reliable_pairs or self.cfg.major_usd_pairs or [])
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

    def research_due(self, n: Optional[dt.datetime] = None, state: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str]]:
        n = n or ny_now()
        state = state or self.load_state()
        if fx_market_closed(n):
            return False, None
        last_scan_utc = state.get("last_research_scan_utc")
        if last_scan_utc and self.cfg.min_minutes_between_research_scans > 0:
            try:
                last_scan = dt.datetime.fromisoformat(str(last_scan_utc).replace("Z", "+00:00")).astimezone(UTC)
                age_min = (utc_now() - last_scan).total_seconds() / 60.0
                if age_min < self.cfg.min_minutes_between_research_scans:
                    return False, None
            except Exception:
                pass
        date_key = n.strftime("%Y-%m-%d")
        last_runs = state.setdefault("last_research_runs", {})
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

    def mark_research_run(self, hhmm: Optional[str], state: Dict[str, Any], n: Optional[dt.datetime] = None) -> None:
        if not hhmm:
            return
        n = n or ny_now()
        key = f"{n.strftime('%Y-%m-%d')} {hhmm}"
        state.setdefault("last_research_runs", {})[key] = iso_utc()
        self.save_state(state)

    def mark_past_research_slots(self, state: Dict[str, Any], n: Optional[dt.datetime] = None, note: str = "launch_scan") -> int:
        """Count a launch/manual scan as the management call for any schedule slots already passed today.

        This prevents an always-on restart from doing a launch RESEARCH scan and then immediately
        backfilling the most recent scheduled slot, which would cause two RESEARCH decisions within
        a few minutes. Future slots are not marked.
        """
        n = n or ny_now()
        date_key = n.strftime("%Y-%m-%d")
        last_runs = state.setdefault("last_research_runs", {})
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
        if not cfg_bool("FOREX_WEEKEND_SUMMARY_USE_RESEARCH", default=True):
            log("Weekend summary due, but RESEARCH weekend summary disabled.")
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
        decision = self.research_api.create_decision(packet)
        decision["orders_to_execute"] = []
        decision = self.normalize_decision(decision, open_trades)
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
        packet = {
            "as_of_utc": iso_utc(),
            "as_of_ny": iso_ny(),
            "account_lane": self.cfg.account_lane,
            "account_name": self.cfg.account_display_name,
            "instrument_filter_mode": self.cfg.instrument_filter_mode,
            "major_usd_pairs": self.cfg.major_usd_pairs,
            "research_reliable_pairs": self.cfg.research_reliable_pairs,
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
                "cross_pair_policy": "Explicitly compare USD pairs against non-USD crosses, but use global-best selection with no USD/cross quota. All selected trades can be USD pairs if they are truly best after evaluating all pairs.",
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
                "margin_deployment_note": "When margin_used_pct is well below target_margin_used_pct, consider higher risk_pct or more non-correlated opens if the outlook justifies it. The script still caps at max_margin_used_pct.",
                "require_stop_loss_on_open": self.cfg.require_stop_loss_on_open,
                "shutdown_mode": self.cfg.shutdown_mode,
                "shutdown_block_new_trades": self.cfg.shutdown_block_new_trades,
                "selection_mode": "global_best_after_full_pair_scan",
                "category_quota_policy": "none; do not reserve slots or risk for USD pairs or non-USD crosses",
                "event_scanner": {
                    "enabled": self.cfg.event_scanner_enabled,
                    "local_scan_interval_seconds": self.cfg.event_scan_interval_seconds,
                    "windows_minutes": self.cfg.event_windows_minutes,
                    "scout_trades_enabled": self.cfg.event_scout_trades_enabled,
                    "scout_risk_pct_default": self.cfg.event_scout_risk_pct,
                    "permission_note": "If event_permissions are returned, local scanner may use them for small scout trades before the next scheduled RESEARCH call when basket confirmation appears.",
                },
                "cross_pair_review_required": bool(self.cfg.require_cross_pair_coverage),
                "min_non_usd_candidates": 0,
                "available_usd_pair_count": usd_pair_count,
                "available_non_usd_cross_pair_count": non_usd_pair_count,
                "account_lane": self.cfg.account_lane,
                "instrument_filter_mode": self.cfg.instrument_filter_mode,
                "route_policy": "Technical/scout account: RESEARCH trading is disabled; local event/scout logic may trade any supplied OANDA pair when move/spread/earlyness/EV gates pass. This split is strategy-based, not USD-vs-non-USD.",
            },
            "recent_daily_recaps": recent_daily_recaps,
            "trade_profit_memory": trade_profit_memory,
            "weekend_closed_market_reference": weekend_reference,
            "currency_exposure_units": build_currency_exposure(open_trades),
            "open_trades": [summarize_trade_for_prompt(t) for t in open_trades],
            "market_snapshots": snapshots,
            "output_contract": {
                "orders_to_execute": "Only include actions you want the script to attempt now, ordered by final global attractiveness across all supplied pairs, not by category.",
                "new_trade_candidates": "Include real OPEN/WATCH/REJECT rankings for the best opportunities after comparing all supplied pairs. Do not pad with placeholder candidates.",
                "open_position_actions": "Mandatory: include exactly one HOLD/TIGHTEN/PARTIAL_CLOSE/CLOSE/FLIP action per open trade. Use HOLD if no change.",
            },
        }
        raw_context = {"account_raw": account, "prices_raw": prices, "open_trades_raw": open_trades}
        return packet, prices, open_trades, raw_context


    def normalize_decision(self, decision: Dict[str, Any], open_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Normalize RESEARCH output so execution sees OANDA instruments and every open trade has a logged action.

        This does not create broker-changing actions. If RESEARCH omits an open trade, the script injects a HOLD
        with a clear reason so the scan log shows the position was not acted on rather than silently ignored.
        """
        if not isinstance(decision, dict):
            decision = {}
        for key in ("open_position_actions", "new_trade_candidates", "orders_to_execute"):
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
                        "why_now": "v5.9 cross-pair audit placeholder: RESEARCH omitted this non-USD cross from candidates.",
                        "reason": "Included so cross-pair coverage can be audited; not executable.",
                        "what_would_change_my_mind": "Future RESEARCH scan should evaluate this pair explicitly.",
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

            # If RESEARCH provided an existing-position action, force the symbol/direction to match
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
                    "reason": "RESEARCH omitted this open trade; v5.6 injected HOLD so the position is logged and not silently ignored.",
                    "what_would_change_my_mind": "Next RESEARCH management cycle should explicitly evaluate this trade.",
                })
                injected += 1
            if injected or corrected:
                decision["open_position_actions"] = actions
                notes = decision.setdefault("risk_notes", [])
                if isinstance(notes, list):
                    if injected:
                        notes.append(f"v5.6 injected HOLD for {injected} open trade(s) missing from RESEARCH open_position_actions.")
                    if corrected:
                        notes.append(f"v5.6 corrected {corrected} open-trade action symbol/direction field(s) to match broker state.")
                if injected:
                    log(f"{self.lane_prefix()}[WARN] RESEARCH omitted {injected} open trade action(s); injected HOLD fallback(s).")
                if corrected:
                    log(f"{self.lane_prefix()}[WARN] Corrected {corrected} RESEARCH open-trade action field(s) to match broker state.")
        return decision

    def run_research_scan(self, reason: str = "scheduled") -> None:
        if fx_market_closed() and reason != "manual":
            log("FX market appears closed; skipping scheduled RESEARCH scan.")
            return
        if self.shutdown_mode_active() and self.cfg.shutdown_skip_research:
            log(f"Shutdown {self.shutdown_mode_label()} active; replacing RESEARCH scan with local shutdown fade pass: {reason}")
            self.run_shutdown_fade_pass(reason=f"research_scan_replaced:{reason}", force=True)
            state = self.load_state()
            state["last_research_scan_utc"] = iso_utc()
            self.save_state(state)
            return
        log(f"{self.lane_prefix()}Starting RESEARCH portfolio-management scan: {reason}")
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
        decision = self.research_api.create_decision(packet)
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
        state["last_research_scan_utc"] = iso_utc()
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
            "kind": "research_scan",
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
        log(f"{self.lane_prefix()}RESEARCH scan complete. action_count={action_count} summary={str(decision.get('market_summary', ''))[:180]}")

    def log_decision_candidates(self, decision: Dict[str, Any], ctx_hash: str) -> None:
        """Write every RESEARCH candidate/action to candidates.csv so USD-vs-cross coverage is auditable."""
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
                    "outlook_confidence": item.get("outlook_confidence", ""),
                    "risk_pct": item.get("risk_pct", ""),
                    "margin_intensity": item.get("margin_intensity", ""),
                    "expected_hold_hours": item.get("expected_hold_hours", ""),
                    "reason": str(item.get("reason", ""))[:1000],
                    "why_now": str(item.get("why_now", ""))[:1000],
                    "raw_json": json.dumps({"context_hash": ctx_hash, "item": item}, default=str)[:12000],
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
        top_candidates: List[Dict[str, Any]] = []
        for r in candidates:
            inst = normalize_instrument(r.get("instrument", ""))
            if not inst:
                continue
            candidate_counts[inst] = candidate_counts.get(inst, 0) + 1
            is_cross = str(r.get("is_cross_pair", "")).lower() == "true" or instrument_is_cross_pair(inst)
            has_usd = str(r.get("has_usd", "")).lower() == "true" or instrument_has_usd(inst)
            if is_cross:
                cross_candidates += 1
            if has_usd:
                usd_candidates += 1
            if r.get("section") == "orders_to_execute":
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
        for order in orders:
            action = str(order.get("action", "")).upper().strip()
            if action != "OPEN":
                continue
            if new_count >= self.cfg.max_new_trades_per_scan:
                self.log_action(order, "open", "skipped", reject_reason="max_new_trades_per_scan reached")
                continue
            inst = normalize_instrument(order.get("instrument", ""))
            order["instrument"] = inst
            if inst in open_instruments:
                self.log_action(order, "open", "skipped", reject_reason="instrument already has open trade")
                continue
            if self.cfg.broker_recheck_before_actions and self.trade_lookup_blocks_open(inst):
                self.log_action(order, "open", "skipped", reject_reason="broker recheck found existing open trade")
                open_instruments.add(inst)
                continue
            akey = self.action_key("OPEN", order)
            if self.recently_attempted_action(akey):
                self.log_action(order, "open", "skipped", reject_reason="duplicate recent open action suppressed")
                continue
            risk_pct = clamp(safe_float(order.get("risk_pct"), 0.0), 0.0, self.cfg.max_risk_pct_per_trade)
            if risk_pct < self.cfg.min_risk_pct_per_trade:
                self.log_action(order, "open", "skipped", reject_reason="risk_pct below configured minimum")
                continue
            if total_new_risk + risk_pct > self.cfg.max_total_new_risk_pct_per_scan:
                self.log_action(order, "open", "skipped", reject_reason="max_total_new_risk_pct_per_scan reached")
                continue
            status = self.open_trade_from_order(order, prices)
            if status.startswith("accepted") or status == "dry_run":
                self.remember_action(akey)
                new_count += 1
                total_new_risk += risk_pct
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
        self._last_open_trade_result = None
        self._last_open_trade_status = ""
        inst = normalize_instrument(order.get("instrument", ""))
        order["instrument"] = inst
        if not self.instrument_allowed_for_lane(inst):
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"instrument not routed to this account lane ({self.cfg.instrument_filter_mode})")
            return "skipped"
        if self.sunday_reopen_new_trades_blocked():
            allow_reopen_open = bool(order.get("_allow_sunday_reopen_open"))
            allow_reopen_open = allow_reopen_open or (bool(order.get("_event_scout")) and cfg_bool("FOREX_REOPEN_ALLOW_CONFIRMED_EVENT_SCOUTS", default=True))
            if not allow_reopen_open:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="Sunday reopen warmup blocks unrelated fresh opens; close/flip/reversal scouts remain allowed")
                return "skipped"
        if self.volatile_weekend_new_entries_blocked():
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="volatile weekend flat policy blocks new entries before/through weekend")
            return "skipped"
        if self.shutdown_mode_active() and self.cfg.shutdown_block_new_trades:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"shutdown {self.shutdown_mode_label()} mode blocks new opens")
            return "skipped"
        direction = str(order.get("direction", "")).upper().strip()
        if direction == "BUY":
            direction = "LONG"
        if direction == "SELL":
            direction = "SHORT"
        if inst not in self.instrument_meta:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="unknown/non-currency instrument")
            return "skipped"
        if direction not in {"LONG", "SHORT"}:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="direction must be LONG or SHORT")
            return "skipped"
        price = prices.get(inst, {})
        if not price or not price_tradeable(price):
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="price unavailable or not tradeable")
            return "skipped"
        if self.cfg.broker_recheck_before_actions and self.trade_lookup_blocks_open(inst):
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="broker recheck found existing open trade")
            return "skipped"
        meta = self.instrument_meta[inst]
        if self.cfg.broker_recheck_before_actions:
            try:
                fresh_prices = self.oanda.get_prices([inst])
                fresh_price = fresh_prices.get(inst, {})
                if not fresh_price or not price_tradeable(fresh_price):
                    self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="fresh broker price unavailable or not tradeable before submit")
                    return "skipped"
                price = fresh_price
                prices[inst] = fresh_price
            except Exception as exc:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"fresh broker price recheck failed before submit: {str(exc)[:240]}")
                return "skipped"
        spr = spread_pips(price, meta)
        max_spread = self.event_max_spread_pips(inst) if order.get("_event_scout") else self.max_spread_allowed(inst)
        if spr > max_spread:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"spread too wide {spr:.2f}p > {max_spread:.2f}p")
            return "skipped"
        bid, ask = bid_ask(price)
        if bid is None or ask is None:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="bid/ask unavailable")
            return "skipped"
        entry = ask if direction == "LONG" else bid
        stop_loss = as_optional_float(order.get("stop_loss"))
        take_profit = first_optional_float(order.get("take_profit"), order.get("tp1"))
        trailing_pips = as_optional_float(order.get("trailing_stop_pips"))

        if self.cfg.require_stop_loss_on_open and stop_loss is None:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="stop_loss required by config")
            return "skipped"
        if self.cfg.require_take_profit_on_open and take_profit is None:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="take_profit required by config")
            return "skipped"
        if stop_loss is not None:
            stop_pips = abs(entry - stop_loss) / meta.pip_size
            if stop_pips <= 0:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="invalid stop distance")
                return "skipped"
            if order.get("_event_scout") and self.is_volatile_scout_pair(inst):
                spread_stop_limit = self.cfg.scout_volatile_max_spread_to_stop_ratio
            else:
                spread_stop_limit = cfg_float("FOREX_SCOUT_MAX_SPREAD_TO_STOP_RATIO", default=0.45) if order.get("_event_scout") else self.cfg.max_spread_to_stop_ratio
            if spr / max(stop_pips, 0.00001) > spread_stop_limit:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"spread/stop ratio too high {spr / max(stop_pips, 0.00001):.2f} > {spread_stop_limit:.2f}")
                return "skipped"
            if direction == "LONG" and stop_loss >= entry:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="LONG stop_loss must be below entry")
                return "skipped"
            if direction == "SHORT" and stop_loss <= entry:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason="SHORT stop_loss must be above entry")
                return "skipped"
        else:
            stop_pips = None
        if take_profit is not None:
            if direction == "LONG":
                target_pips = (take_profit - entry) / meta.pip_size
            else:
                target_pips = (entry - take_profit) / meta.pip_size
            if target_pips <= 0:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=f"{direction} take_profit is no longer beyond fresh entry")
                return "skipped"
            if order.get("_event_scout"):
                min_target_pips = spr * cfg_float("FOREX_EVENT_SCOUT_MIN_TARGET_SPREAD_MULT", default=0.50)
                if target_pips < min_target_pips:
                    self.log_action(order, "event_scout", "skipped", reject_reason=f"fresh target/spread geometry too weak {target_pips:.2f}p < {min_target_pips:.2f}p")
                    return "skipped"

        risk_pct = clamp(safe_float(order.get("risk_pct"), 0.0), 0.0, self.cfg.max_risk_pct_per_trade)
        account = self.oanda.get_account_summary()
        nav = safe_float(account.get("NAV", account.get("nav", account.get("balance"))))
        units_abs, risk_usd, margin_usd, reject = self.size_units(inst, direction, entry, stop_loss, risk_pct, prices, nav, account)
        if reject:
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "skipped", reject_reason=reject)
            return "skipped"
        signed_units = side_to_units(direction, units_abs)

        price_bound = None
        entry_slippage_pips = self.cfg.max_entry_slippage_pips
        if order.get("_event_scout"):
            audit = order.get("_scout_audit") if isinstance(order.get("_scout_audit"), dict) else {}
            net_after_spread_pips = safe_float(
                audit.get("_value_weighted_net_after_spread_pips")
                or order.get("_value_weighted_net_after_spread_pips"),
                0.0,
            )
            dynamic_bound_pips = max(
                entry_slippage_pips,
                spr * cfg_float("FOREX_EVENT_SCOUT_PRICE_BOUND_SPREAD_MULT", default=0.25),
            )
            if net_after_spread_pips > 0:
                dynamic_bound_pips = min(
                    dynamic_bound_pips,
                    max(
                        entry_slippage_pips,
                        net_after_spread_pips
                        * cfg_float("FOREX_EVENT_SCOUT_PRICE_BOUND_NET_AFTER_MULT", default=0.50),
                    ),
                )
            entry_slippage_pips = min(
                dynamic_bound_pips,
                cfg_float("FOREX_EVENT_SCOUT_PRICE_BOUND_MAX_PIPS", default=15.0),
            )
            if abs(entry_slippage_pips - self.cfg.max_entry_slippage_pips) > 1e-9:
                order["entry_slippage_pips_used"] = round(entry_slippage_pips, 4)
        slippage_distance = entry_slippage_pips * meta.pip_size
        if direction == "LONG":
            price_bound = ask + slippage_distance
        else:
            price_bound = bid - slippage_distance
        trailing_distance = None
        if self.cfg.allow_trailing_stop_on_open and trailing_pips and trailing_pips > 0:
            trailing_distance = trailing_pips * meta.pip_size

        if not self.should_execute():
            self.record_dry_run_order(order, signed_units, entry, risk_usd, margin_usd)
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "dry_run", units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd)
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
                tag=f"researchfxv5_17_{self.cfg.account_lane[:12]}_{now_key_date().replace('-', '')}",
            )
            status = order_status_from_result(res)
            fill = order_fill_price(res)
            self._last_open_trade_result = res
            self._last_open_trade_status = status
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", status, units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd, fill_price=fill, raw_extra=res)
            return status
        except Exception as exc:
            self._last_open_trade_result = {"exception": str(exc)[:1000]}
            self._last_open_trade_status = "error"
            refreshed = self.find_open_trade(instrument=inst, direction=direction) if self.cfg.broker_recheck_before_actions else None
            if refreshed:
                self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "accepted_after_recheck", units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd, reject_reason=str(exc)[:500], raw_extra={"rechecked_trade": summarize_trade_for_prompt(refreshed)})
                return "accepted_after_recheck"
            self.log_action(order, "event_scout" if order.get("_event_scout") else "open", "error", units=signed_units, risk_usd=risk_usd, margin_usd=margin_usd, reject_reason=str(exc)[:500])
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
            if cfg_bool("FOREX_TECH_ALLOW_MIN_SIZE_FALLBACK", default=True):
                min_units = max(1, int(math.ceil(max(1.0, meta.minimum_trade_size))))
                min_margin_usd = min_units * margin_per_unit
                min_risk_usd = min_units * stop_pips * pip_value_per_unit
                max_min_risk = cfg_float("FOREX_TECH_MIN_SIZE_MAX_RISK_USD", default=0.35)
                if min_margin_usd <= available_margin_budget + 1e-9 and min_risk_usd <= max_min_risk:
                    units_abs_int = min_units
                    risk_usd = min_risk_usd
                else:
                    return 0, risk_usd, 0.0, f"units rounded below minimum trade size or margin unavailable; min_size_risk={min_risk_usd:.2f} max={max_min_risk:.2f} min_margin={min_margin_usd:.2f} budget={available_margin_budget:.2f}"
            else:
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
        try:
            if action_type == "event_scout" or bool(action.get("_event_scout")) or bool(action.get("_scout_audit")):
                self.log_scout_audit_action(
                    action,
                    action_type,
                    status,
                    units=units,
                    risk_usd=risk_usd,
                    margin_usd=margin_usd,
                    fill_price=fill_price,
                    reject_reason=reject_reason,
                    raw_extra=raw_extra,
                )
        except Exception as exc:
            try:
                self.cfg.errors_log.parent.mkdir(parents=True, exist_ok=True)
                self.cfg.errors_log.write_text(error_text("log_scout_audit_action", exc), encoding="utf-8")
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
            log(f"Saved {len(cleaned)} event permission(s) from RESEARCH scan.")

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
                "complete": bool(candle.get("complete", True)),
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
            avg_spread = (sum(r["spread_o_pips"] + r["spread_c_pips"] for r in window) / max(1, 2 * len(window)))
            avg_range = (sum((r.get("mid_h", r["mid_c"]) - r.get("mid_l", r["mid_c"])) / meta.pip_size for r in window) / max(1, len(window)))
            volume_sum = sum(safe_float(r.get("volume"), 0.0) for r in window)

            # Direction should follow the actual mid-price impulse. The prior
            # implementation compared signed round-trip long/short net values;
            # during wide-spread hours both could be negative, causing a real
            # downward spike to log as below_pips -104<45 instead of SHORT 104p.
            if mid_move > 0:
                direction = "LONG"
                cost_adjusted_net_pips = long_net
            elif mid_move < 0:
                direction = "SHORT"
                cost_adjusted_net_pips = short_net
            else:
                direction = "LONG" if long_net >= short_net else "SHORT"
                cost_adjusted_net_pips = long_net if direction == "LONG" else short_net
            abs_move_pips = abs(mid_move)
            # Use absolute impulse for detection/ranking, while retaining the
            # cost-adjusted net as a separate diagnostic field.
            net_pips = abs_move_pips
            ratio = abs_move_pips / max(avg_spread, 0.1)
            out.append({
                "instrument": instrument,
                "window_minutes": minutes,
                "direction": direction,
                "net_pips": net_pips,
                "abs_move_pips": abs_move_pips,
                "cost_adjusted_net_pips": cost_adjusted_net_pips,
                "mid_move_pips": mid_move,
                "spread_avg_pips": avg_spread,
                "avg_range_pips": avg_range,
                "volume_sum": volume_sum,
                "move_to_spread_ratio": ratio,
                "start_utc": start["time"],
                "end_utc": end["time"],
            })
        return out

    def technical_pressure_signal(self, instrument: str, candles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Estimate pre-breakout pressure before a full spike threshold is reached.

        This is intentionally technical and local: no external model call. It looks for
        recent directional acceleration emerging from a compressed range, then leaves
        the normal hard event trigger to confirm/compound once the move is obvious.
        """
        if not cfg_bool("FOREX_TECH_PRE_SPIKE_PRESSURE_ENABLED", default=True):
            return None
        meta = self.instrument_meta.get(instrument)
        if not meta:
            return None
        rows = [r for c in candles if (r := self.parse_bma_candle(c, meta))]
        if len(rows) < 35:
            return None

        def mid(i: int) -> float:
            return safe_float(rows[i].get("mid_c"), 0.0)

        def move_pips(bars: int) -> float:
            if len(rows) <= bars:
                return 0.0
            return (mid(-1) - mid(-1 - bars)) / meta.pip_size

        def avg_range(slice_rows: List[Dict[str, Any]]) -> float:
            vals = []
            for r in slice_rows:
                hi = safe_float(r.get("mid_h", r.get("mid_c", 0.0)), 0.0)
                lo = safe_float(r.get("mid_l", r.get("mid_c", 0.0)), 0.0)
                vals.append(max(0.0, (hi - lo) / meta.pip_size))
            return sum(vals) / max(1, len(vals))

        m3 = move_pips(3)
        m5 = move_pips(5)
        m10 = move_pips(10)
        m15 = move_pips(15)
        weighted = m3 * 0.40 + m5 * 0.30 + m10 * 0.20 + m15 * 0.10
        direction = "LONG" if weighted >= 0 else "SHORT"
        abs_weighted = abs(weighted)

        recent_range = avg_range(rows[-5:])
        prior_range = avg_range(rows[-35:-10])
        longer_range = avg_range(rows[-80:-35]) if len(rows) >= 80 else prior_range
        compression_ratio = prior_range / max(longer_range, 0.1)
        expansion_ratio = recent_range / max(prior_range, 0.1)
        accel = max(0.0, abs(m3) - abs(move_pips(8) - move_pips(5)))

        spread = (safe_float(rows[-1].get("spread_o_pips"), 0.0) + safe_float(rows[-1].get("spread_c_pips"), 0.0)) / 2.0
        spread = max(spread, 0.1)
        threshold = self.event_threshold_pips(instrument)
        ratio = abs_weighted / spread

        move_score = clamp(abs_weighted / max(threshold, 1.0) * 45.0, 0.0, 45.0)
        accel_score = clamp(accel / max(threshold * 0.25, 0.5) * 20.0, 0.0, 20.0)
        compression_score = clamp((1.10 - compression_ratio) * 30.0, 0.0, 15.0)
        expansion_score = clamp((expansion_ratio - 1.0) * 14.0, 0.0, 15.0)
        spread_score = clamp((ratio - 0.50) * 12.0, 0.0, 10.0)
        score = clamp(move_score + accel_score + compression_score + expansion_score + spread_score, 0.0, 100.0)

        min_score = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SCORE", default=62.0)
        if score < min_score:
            return None
        return {
            "instrument": instrument,
            "window_minutes": 5,
            "direction": direction,
            "net_pips": abs_weighted,
            "abs_move_pips": abs_weighted,
            "mid_move_pips": weighted,
            "spread_avg_pips": spread,
            "move_to_spread_ratio": ratio,
            "pressure_score": score,
            "pressure_components": {
                "move_score": move_score,
                "accel_score": accel_score,
                "compression_score": compression_score,
                "expansion_score": expansion_score,
                "spread_score": spread_score,
                "m3": m3, "m5": m5, "m10": m10, "m15": m15,
                "recent_range": recent_range,
                "prior_range": prior_range,
                "longer_range": longer_range,
                "compression_ratio": compression_ratio,
                "expansion_ratio": expansion_ratio,
                "accel": accel,
            },
            "threshold_pips": threshold,
            "entry_mode": "pre_spike_pressure",
            "start_utc": rows[-10]["time"],
            "end_utc": rows[-1]["time"],
        }

    def technical_exhaustion_feature_snapshot(
        self,
        instrument: str,
        candles: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Build the validated 5m/ATR snapshot for one instrument."""
        meta = self.instrument_meta.get(instrument)
        if not meta:
            return None
        rows = [r for c in candles if (r := self.parse_bma_candle(c, meta))]
        features = technical_exhaustion_features_from_rows(rows, meta.pip_size)
        if not features:
            return None
        features["instrument"] = normalize_instrument(instrument)
        return features

    def research_volatility_table(self) -> Dict[str, Dict[str, float]]:
        if self._research_volatility_cache:
            return self._research_volatility_cache
        path = (
            SCRIPT_DIR
            / "data"
            / "all68_weekly_move_study"
            / "reports"
            / "instrument_volatility.csv"
        )
        table: Dict[str, Dict[str, float]] = {}
        if path.exists():
            try:
                with path.open("r", encoding="utf-8-sig", newline="") as f:
                    for row in csv.DictReader(f):
                        inst = normalize_instrument(row.get("instrument", ""))
                        if not inst:
                            continue
                        table[inst] = {
                            "instrument_volatility_percentile": safe_float(row.get("volatility_percentile"), 0.0),
                            "instrument_median_abs_60_pips": safe_float(row.get("median_abs_60_pips"), 0.0),
                            "instrument_q995_abs_60_pips": safe_float(row.get("q995_abs_60_pips"), 0.0),
                            "instrument_q995_move_to_spread": safe_float(row.get("q995_move_to_spread"), 0.0),
                        }
            except Exception as exc:
                self.log_error("load research volatility table", exc)
        self._research_volatility_cache = table
        return table

    def live_calendar_features(self, now: Optional[dt.datetime] = None) -> Dict[str, float]:
        now = (now or utc_now()).astimezone(UTC)
        hour_float = now.hour + now.minute / 60.0
        next_month = dt.datetime(
            now.year + (1 if now.month == 12 else 0),
            1 if now.month == 12 else now.month + 1,
            1,
            tzinfo=UTC,
        )
        days_in_month = max(1, (next_month - dt.timedelta(days=1)).day)
        weekday = now.weekday()
        month = now.month
        day = now.day
        day_of_year = now.timetuple().tm_yday
        week_of_year = float(now.isocalendar().week)
        return {
            "hour_sin": math.sin(2.0 * math.pi * hour_float / 24.0),
            "hour_cos": math.cos(2.0 * math.pi * hour_float / 24.0),
            "weekday": float(weekday),
            "weekday_sin": math.sin(2.0 * math.pi * weekday / 7.0),
            "weekday_cos": math.cos(2.0 * math.pi * weekday / 7.0),
            "month": float(month),
            "month_sin": math.sin(2.0 * math.pi * (month - 1) / 12.0),
            "month_cos": math.cos(2.0 * math.pi * (month - 1) / 12.0),
            "day_of_month_sin": math.sin(2.0 * math.pi * (day - 1) / days_in_month),
            "day_of_month_cos": math.cos(2.0 * math.pi * (day - 1) / days_in_month),
            "day_of_year_sin": math.sin(2.0 * math.pi * day_of_year / 366.0),
            "day_of_year_cos": math.cos(2.0 * math.pi * day_of_year / 366.0),
            "week_of_year_sin": math.sin(2.0 * math.pi * week_of_year / 53.0),
            "week_of_year_cos": math.cos(2.0 * math.pi * week_of_year / 53.0),
            "week_of_month": float((day - 1) // 7 + 1),
            "is_month_start": float(day == 1),
            "is_month_end": float(day == days_in_month),
            "is_quarter_end": float(now.month in {3, 6, 9, 12} and day == days_in_month),
            "is_asia_session": float(0.0 <= hour_float < 7.0),
            "is_london_session": float(7.0 <= hour_float < 16.0),
            "is_new_york_session": float(12.0 <= hour_float < 21.0),
            "is_london_ny_overlap": float(12.0 <= hour_float < 16.0),
            "is_rollover_hour": float(21.0 <= hour_float < 22.0),
        }

    def instrument_research_subset_features(self, instrument: str) -> Dict[str, float]:
        inst = normalize_instrument(instrument)
        base, quote = split_instrument(inst)
        vol = self.research_volatility_table().get(inst, {})
        percentile = safe_float(vol.get("instrument_volatility_percentile"), 0.0)
        is_major = inst in SEVEN_MAJOR_PAIRS
        is_usd = base == "USD" or quote == "USD"
        is_jpy = base == "JPY" or quote == "JPY"
        is_exotic = (
            base in EXOTIC_OR_VOLATILE_CURRENCIES
            or quote in EXOTIC_OR_VOLATILE_CURRENCIES
        )
        is_volatile = (
            percentile >= 0.70
            or is_exotic
            or self.is_volatile_scout_pair(inst)
        )
        return {
            "instrument_volatility_percentile": percentile,
            "instrument_median_abs_60_pips": safe_float(vol.get("instrument_median_abs_60_pips"), 0.0),
            "instrument_q995_abs_60_pips": safe_float(vol.get("instrument_q995_abs_60_pips"), 0.0),
            "instrument_q995_move_to_spread": safe_float(vol.get("instrument_q995_move_to_spread"), 0.0),
            "is_major_pair": float(is_major),
            "is_usd_pair": float(is_usd),
            "is_non_usd_pair": float(not is_usd),
            "is_jpy_cross": float(is_jpy),
            "is_exotic_pair": float(is_exotic),
            "is_volatile_pair": float(is_volatile),
            "is_volatile_or_exotic_pair": float(is_volatile or is_exotic),
            "is_non_usd_volatile_pair": float((not is_usd) and is_volatile),
        }

    def pair_taxonomy_primary(self, instrument: str) -> str:
        inst = normalize_instrument(instrument)
        base, quote = split_instrument(inst)
        is_usd = base == "USD" or quote == "USD"
        is_non_usd = not is_usd
        is_exotic = (
            base in EXOTIC_OR_VOLATILE_CURRENCIES
            or quote in EXOTIC_OR_VOLATILE_CURRENCIES
        )
        is_commodity = base in COMMODITY_CURRENCIES or quote in COMMODITY_CURRENCIES
        is_eur_gbp_cross = (
            base in EUR_GBP_CURRENCIES
            and quote in EUR_GBP_CURRENCIES
        )
        is_chf = inst in CHF_SAFE_HAVEN_PAIRS or base == "CHF" or quote == "CHF"
        vol = self.instrument_research_subset_features(inst)
        if inst in SEVEN_MAJOR_PAIRS:
            return "usd_major"
        if inst in JPY_RISK_PAIRS:
            return "jpy_risk"
        if is_exotic:
            return "exotic_high_spread"
        if bool(vol.get("is_volatile_pair")) and is_non_usd:
            return "volatile_non_usd"
        if is_eur_gbp_cross:
            return "eur_gbp_cross"
        if is_chf:
            return "chf_safe_haven"
        if is_commodity:
            return "commodity"
        if is_usd:
            return "usd_other"
        return "other_cross"

    def pair_taxonomy_features(self, instrument: str) -> Dict[str, float]:
        inst = normalize_instrument(instrument)
        base, quote = split_instrument(inst)
        primary = self.pair_taxonomy_primary(inst)
        out = {
            f"pair_class_{name}": float(primary == name)
            for name in PAIR_TAXONOMY_CLASSES
        }
        out.update({
            "base_is_usd": float(base == "USD"),
            "quote_is_usd": float(quote == "USD"),
            "base_is_jpy": float(base == "JPY"),
            "quote_is_jpy": float(quote == "JPY"),
            "base_is_chf": float(base == "CHF"),
            "quote_is_chf": float(quote == "CHF"),
            "base_is_commodity": float(base in COMMODITY_CURRENCIES),
            "quote_is_commodity": float(quote in COMMODITY_CURRENCIES),
            "base_is_risk": float(base in RISK_CURRENCIES),
            "quote_is_risk": float(quote in RISK_CURRENCIES),
        })
        return out

    def carry_proxy_features(self, instrument: str) -> Dict[str, float]:
        inst = normalize_instrument(instrument)
        base, quote = split_instrument(inst)
        base_score = safe_float(CURRENCY_STRUCTURAL_CARRY_SCORE.get(base), 0.0)
        quote_score = safe_float(CURRENCY_STRUCTURAL_CARRY_SCORE.get(quote), 0.0)
        diff = base_score - quote_score
        return {
            "carry_proxy_base_score": base_score,
            "carry_proxy_quote_score": quote_score,
            "carry_proxy_diff": diff,
            "carry_proxy_abs_diff": abs(diff),
            "carry_proxy_positive": float(diff > 0.10),
            "carry_proxy_negative": float(diff < -0.10),
        }

    def current_session_name(self, snapshot: Dict[str, Any]) -> str:
        if safe_float(snapshot.get("is_london_ny_overlap")) >= 0.5:
            return "london_ny_overlap"
        if safe_float(snapshot.get("is_new_york_session")) >= 0.5:
            return "new_york"
        if safe_float(snapshot.get("is_london_session")) >= 0.5:
            return "london"
        if safe_float(snapshot.get("is_asia_session")) >= 0.5:
            return "asia"
        return "off_session"

    def live_regime_features(
        self,
        snapshot: Dict[str, Any],
        instrument: str,
    ) -> Dict[str, Any]:
        inst = normalize_instrument(instrument)
        base, quote = split_instrument(inst)
        m15 = safe_float(snapshot.get("momentum_15_atr"), 0.0)
        m30 = safe_float(snapshot.get("momentum_30_atr"), 0.0)
        m60 = safe_float(snapshot.get("momentum_60_atr"), 0.0)
        accel = safe_float(snapshot.get("acceleration_15_atr"), 0.0)
        atr_ratio = safe_float(snapshot.get("atr15_to_atr240"), 1.0)
        compression = safe_float(snapshot.get("compression_30"), 1.0)
        spread_ratio = safe_float(snapshot.get("spread_ratio_60"), 1.0)
        spread_pips = safe_float(snapshot.get("spread_pips"), 0.0)
        atr240 = max(safe_float(snapshot.get("atr240_pips"), 0.1), 0.1)

        def oriented_strength(currency: str, horizon: int) -> float:
            gap = safe_float(snapshot.get(f"strength_gap_{horizon}"), 0.0)
            if base == currency:
                return gap
            if quote == currency:
                return -gap
            return 0.0

        usd_15 = oriented_strength("USD", 15)
        usd_60 = oriented_strength("USD", 60)
        jpy_15 = oriented_strength("JPY", 15)
        chf_15 = oriented_strength("CHF", 15)
        strength_gap_15 = safe_float(snapshot.get("strength_gap_15"), 0.0)
        risk_strength_15 = (
            (float(base in RISK_CURRENCIES) - float(quote in RISK_CURRENCIES))
            * strength_gap_15
        )
        commodity_strength_15 = (
            (
                float(base in COMMODITY_CURRENCIES)
                - float(quote in COMMODITY_CURRENCIES)
            )
            * strength_gap_15
        )
        risk_haven_pressure = risk_strength_15 - (0.5 * jpy_15 + 0.5 * chf_15)
        trend_strength = (abs(m15) + abs(m30) + abs(m60)) / 3.0
        spread_to_atr = spread_pips / atr240
        flags = {
            "usd_trend": abs(usd_15) >= 0.00035 or abs(usd_60) >= 0.00045,
            "risk_on": risk_haven_pressure >= 0.00025,
            "risk_off": risk_haven_pressure <= -0.00025,
            "jpy_unwind": abs(jpy_15) >= 0.00035,
            "chf_safe_haven": chf_15 >= 0.00025,
            "commodity_trend": abs(commodity_strength_15) >= 0.00025,
            "low_vol_chop": (
                trend_strength <= 0.35
                and atr_ratio <= 0.90
                and compression <= 0.80
            ),
            "high_vol_event": (
                atr_ratio >= 1.35
                or trend_strength >= 1.25
                or abs(accel) >= 0.70
            ),
            "spread_impaired": spread_ratio >= 1.50 or spread_to_atr >= 0.35,
        }
        priority = [
            "spread_impaired",
            "high_vol_event",
            "usd_trend",
            "risk_off",
            "risk_on",
            "jpy_unwind",
            "chf_safe_haven",
            "commodity_trend",
            "low_vol_chop",
        ]
        primary = "neutral"
        for name in priority:
            if flags.get(name):
                primary = name
                break
        out: Dict[str, Any] = {
            "regime_trend_strength": trend_strength,
            "regime_volatility_expansion_score": atr_ratio,
            "regime_compression_score": compression,
            "regime_spread_cost_score": spread_to_atr,
            "regime_usd_strength_15": usd_15,
            "regime_usd_strength_60": usd_60,
            "regime_jpy_strength_15": jpy_15,
            "regime_chf_strength_15": chf_15,
            "regime_risk_on_score": risk_haven_pressure,
            "regime_commodity_score": commodity_strength_15,
            "regime_acceleration_score": accel,
            "regime_is_usd_trend": float(flags["usd_trend"]),
            "regime_is_risk_on": float(flags["risk_on"]),
            "regime_is_risk_off": float(flags["risk_off"]),
            "regime_is_jpy_unwind": float(flags["jpy_unwind"]),
            "regime_is_chf_safe_haven": float(flags["chf_safe_haven"]),
            "regime_is_commodity_trend": float(flags["commodity_trend"]),
            "regime_is_low_vol_chop": float(flags["low_vol_chop"]),
            "regime_is_high_vol_event": float(flags["high_vol_event"]),
            "regime_is_spread_impaired": float(flags["spread_impaired"]),
            "regime_primary": primary,
        }
        for name in REGIME_CLASSES:
            out[f"regime_primary_{name}"] = float(primary == name)
        return out

    def load_macro_bias_payload(self) -> Dict[str, Any]:
        raw_path = cfg_str(
            "FOREX_TECH_MACRO_BIAS_PATH",
            default=str(TECHNICAL_MACRO_BIAS_LATEST_PATH),
        )
        path = Path(raw_path)
        if not path.exists():
            return {}
        try:
            mtime = path.stat().st_mtime_ns
            if mtime == self._macro_bias_cache_mtime:
                return self._macro_bias_cache
            payload = read_json(path, {})
            if not isinstance(payload, dict):
                payload = {}
            self._macro_bias_cache = payload
            self._macro_bias_cache_mtime = mtime
            return payload
        except Exception as exc:
            self.log_error("load macro bias payload", exc)
            return self._macro_bias_cache

    def current_macro_feature_snapshot(self, instrument: str) -> Dict[str, Any]:
        inst = normalize_instrument(instrument)
        base, quote = split_instrument(inst)
        neutral: Dict[str, Any] = {
            "macro_pair_bias": 0.0,
            "macro_usd_bias": 0.0,
            "macro_jpy_bias": 0.0,
            "macro_chf_bias": 0.0,
            "macro_risk_bias": 0.0,
            "macro_commodity_bias": 0.0,
            "macro_rate_diff_bias": 0.0,
            "macro_news_risk": 0.0,
            "macro_event_risk": 0.0,
            "macro_bias_age_hours": 999.0,
            "_macro_bias_current": False,
            "_macro_bias_reason": "no current macro bias payload",
        }
        payload = self.load_macro_bias_payload()
        if not payload:
            return neutral
        updated_raw = payload.get("updated_utc") or payload.get("as_of_utc") or ""
        try:
            updated = dt.datetime.fromisoformat(
                str(updated_raw).replace("Z", "+00:00")
            ).astimezone(UTC)
            age_hours = max(
                0.0,
                (utc_now() - updated).total_seconds() / 3600.0,
            )
        except Exception:
            age_hours = 999.0
        max_age = cfg_float(
            "FOREX_TECH_MACRO_BIAS_MAX_AGE_HOURS",
            default=36.0,
        )
        global_bias = payload.get("global", {})
        if not isinstance(global_bias, dict):
            global_bias = {}
        pairs = payload.get("pairs", {})
        if not isinstance(pairs, dict):
            pairs = {}
        pair_bias = pairs.get(inst, {})
        if not isinstance(pair_bias, dict):
            pair_bias = {}
        currency_rates = payload.get("currency_rates", {})
        if not isinstance(currency_rates, dict):
            currency_rates = {}
        usd_bias = safe_float(global_bias.get("usd_bias"), 0.0)
        jpy_bias = safe_float(global_bias.get("jpy_bias"), 0.0)
        chf_bias = safe_float(global_bias.get("chf_bias"), 0.0)
        risk_bias = safe_float(global_bias.get("risk_bias"), 0.0)
        commodity_bias = safe_float(global_bias.get("commodity_bias"), 0.0)

        def currency_bias(currency: str) -> float:
            score = 0.0
            if currency == "USD":
                score += usd_bias
            if currency == "JPY":
                score += jpy_bias
            if currency == "CHF":
                score += chf_bias
            if currency in RISK_CURRENCIES:
                score += risk_bias
            if currency in COMMODITY_CURRENCIES:
                score += commodity_bias
            return score

        def rate_score(currency: str) -> float:
            entry = currency_rates.get(currency, {})
            if isinstance(entry, dict):
                if entry.get("carry_score", "") != "":
                    return clamp(safe_float(entry.get("carry_score"), 0.0), -1.0, 1.0)
                if entry.get("policy_rate", "") != "":
                    return clamp(safe_float(entry.get("policy_rate"), 0.0) / 10.0, -1.0, 1.0)
            return safe_float(CURRENCY_STRUCTURAL_CARRY_SCORE.get(currency), 0.0)

        inferred_pair_bias = clamp(currency_bias(base) - currency_bias(quote), -1.0, 1.0)
        inferred_rate_diff_bias = clamp(rate_score(base) - rate_score(quote), -1.0, 1.0)
        explicit_pair_bias = pair_bias.get("pair_bias", "")
        explicit_rate_bias = pair_bias.get("rate_diff_bias", "")
        macro_pair_bias = (
            safe_float(explicit_pair_bias, inferred_pair_bias)
            if explicit_pair_bias != ""
            else inferred_pair_bias
        )
        macro_rate_diff_bias = (
            safe_float(explicit_rate_bias, inferred_rate_diff_bias)
            if explicit_rate_bias != ""
            else inferred_rate_diff_bias
        )
        neutral.update({
            "macro_pair_bias": clamp(macro_pair_bias, -1.0, 1.0),
            "macro_usd_bias": clamp(usd_bias, -1.0, 1.0),
            "macro_jpy_bias": clamp(jpy_bias, -1.0, 1.0),
            "macro_chf_bias": clamp(chf_bias, -1.0, 1.0),
            "macro_risk_bias": clamp(risk_bias, -1.0, 1.0),
            "macro_commodity_bias": clamp(commodity_bias, -1.0, 1.0),
            "macro_rate_diff_bias": clamp(macro_rate_diff_bias, -1.0, 1.0),
            "macro_news_risk": clamp(
                max(
                    safe_float(global_bias.get("news_risk"), 0.0),
                    safe_float(pair_bias.get("news_risk"), 0.0),
                ),
                0.0,
                1.0,
            ),
            "macro_event_risk": clamp(
                max(
                    safe_float(global_bias.get("event_risk"), 0.0),
                    safe_float(pair_bias.get("event_risk"), 0.0),
                ),
                0.0,
                1.0,
            ),
            "macro_bias_age_hours": age_hours,
            "_macro_bias_current": age_hours <= max_age,
            "_macro_bias_reason": str(pair_bias.get("reason") or ""),
        })
        return neutral

    def expanded_indicator_snapshot_features(
        self,
        snapshot: Dict[str, Any],
    ) -> Dict[str, float]:
        """Mirror training-time derived indicator features for live scoring.

        The trainer's full technical feature set includes derived momentum,
        liquidity, rule-baseline, macro-alignment, and pressure columns. Live
        model scoring needs those same feature names so a validated full-feature
        scout factor can score instead of being skipped for missing columns.
        """
        eps = 1e-9

        def v(name: str, default: float = 0.0) -> float:
            return safe_float(snapshot.get(name), default)

        def b(condition: bool) -> float:
            return 1.0 if bool(condition) else 0.0

        def pos(value: float) -> float:
            return max(value, 0.0)

        def neg(value: float) -> float:
            return max(-value, 0.0)

        def sign(value: float) -> float:
            if value > 0:
                return 1.0
            if value < 0:
                return -1.0
            return 0.0

        m5 = v("momentum_5_atr")
        m15 = v("momentum_15_atr")
        m30 = v("momentum_30_atr")
        m60 = v("momentum_60_atr")
        spread_ratio = v("spread_ratio_60", 1.0)
        atr_pips = max(v("atr240_pips", 0.1), 0.1)
        spread_pips = abs(v("spread_pips", 0.0))
        compression = clamp(v("compression_30", 1.0), 0.0, 2.0)
        realized_vol_ratio = clamp(v("realized_vol_ratio_30_240", 1.0), 0.0, 10.0)
        rsi = v("rsi14_centered")
        macd_hist = v("macd_hist_atr")
        ma_stack = v("ma_stack_score")
        donchian60 = v("donchian_60_breakout_atr")
        donchian240 = v("donchian_240_breakout_atr")
        range60 = v("range_position_60_centered")
        range240 = v("range_position_240_centered")
        ema8_minus_ema21 = v("ema8_minus_ema21_atr")
        strength_gap_15 = v("strength_gap_15")
        strength_gap_60 = v("strength_gap_60")
        regime_vol_expansion = v("regime_volatility_expansion_score", 1.0)
        regime_spread_cost = v("regime_spread_cost_score", 1.0)
        is_rollover = v("is_rollover_hour")
        is_spread_impaired = v("regime_is_spread_impaired")

        features: Dict[str, float] = {}
        features["abs_momentum_5_atr"] = abs(m5)
        features["abs_momentum_15_atr"] = abs(m15)
        features["abs_momentum_30_atr"] = abs(m30)
        features["abs_momentum_60_atr"] = abs(m60)
        features["momentum_5_15_delta_atr"] = m5 - m15
        features["momentum_15_30_delta_atr"] = m15 - m30
        features["momentum_30_60_delta_atr"] = m30 - m60
        features["momentum_5_to_30_ratio"] = m5 / (abs(m30) + eps)
        features["momentum_15_to_60_ratio"] = m15 / (abs(m60) + eps)
        features["momentum_alignment_long"] = (
            b(m5 > 0) + b(m15 > 0) + b(m30 > 0) + b(m60 > 0)
        )
        features["momentum_alignment_short"] = (
            b(m5 < 0) + b(m15 < 0) + b(m30 < 0) + b(m60 < 0)
        )
        features["momentum_alignment_edge"] = (
            features["momentum_alignment_long"]
            - features["momentum_alignment_short"]
        )
        features["momentum_acceleration_pressure"] = (
            features["momentum_5_15_delta_atr"] * 0.55
            + features["momentum_15_30_delta_atr"] * 0.30
            + features["momentum_30_60_delta_atr"] * 0.15
        )
        features["momentum_exhaustion_pressure"] = (
            clamp(abs(m30), 0.0, 6.0)
            * clamp(-sign(m30) * features["momentum_5_15_delta_atr"], 0.0, 6.0)
        )

        sma7_minus_8 = v("sma7_minus_8_atr")
        sma30_slope = v("sma30_slope_5_atr")
        features["sma7_gt_sma8"] = b(sma7_minus_8 > 0)
        features["sma7_lt_sma8"] = b(sma7_minus_8 < 0)
        features["sma30_slope_gt_1_atr"] = b(sma30_slope > 1.0)
        features["sma30_slope_lt_minus_1_atr"] = b(sma30_slope < -1.0)
        features["ema8_gt_ema21"] = b(ema8_minus_ema21 > 0)
        features["ema8_lt_ema21"] = b(ema8_minus_ema21 < 0)
        features["macd_hist_positive"] = b(macd_hist > 0)
        features["macd_hist_negative"] = b(macd_hist < 0)
        features["macd_hist_abs_atr"] = abs(macd_hist)
        features["trend_pressure_long"] = (
            pos(m5) * 0.25
            + pos(m15) * 0.25
            + pos(m30) * 0.20
            + pos(ema8_minus_ema21) * 0.15
            + pos(macd_hist) * 0.10
            + pos(ma_stack) * 0.20
        )
        features["trend_pressure_short"] = (
            neg(m5) * 0.25
            + neg(m15) * 0.25
            + neg(m30) * 0.20
            + neg(ema8_minus_ema21) * 0.15
            + neg(macd_hist) * 0.10
            + neg(ma_stack) * 0.20
        )
        features["trend_pressure_edge"] = (
            features["trend_pressure_long"] - features["trend_pressure_short"]
        )
        features["rsi_overbought_pressure"] = clamp(rsi - 0.20, 0.0, 2.0)
        features["rsi_oversold_pressure"] = clamp(-rsi - 0.20, 0.0, 2.0)
        features["rsi_extreme_abs"] = abs(rsi)

        features["donchian60_breakout_long"] = b(donchian60 > 0.5)
        features["donchian60_breakout_short"] = b(donchian60 < -0.5)
        features["donchian240_breakout_long"] = b(donchian240 > 0.5)
        features["donchian240_breakout_short"] = b(donchian240 < -0.5)
        features["donchian_breakout_abs_atr"] = max(abs(donchian60), abs(donchian240))
        features["range60_extreme_abs"] = abs(range60)
        features["range240_extreme_abs"] = abs(range240)
        features["near_upper_range_score"] = max(
            pos(range60),
            pos(range240),
            pos(v("donchian_60_position_centered")),
            pos(v("donchian_240_position_centered")),
        )
        features["near_lower_range_score"] = max(
            neg(range60),
            neg(range240),
            neg(v("donchian_60_position_centered")),
            neg(v("donchian_240_position_centered")),
        )
        features["range_breakout_pressure_long"] = (
            features["near_upper_range_score"] * max(m5 + 0.25, 0.0)
        )
        features["range_breakout_pressure_short"] = (
            features["near_lower_range_score"] * max(-m5 + 0.25, 0.0)
        )

        features["strength_gap_abs_15"] = abs(strength_gap_15)
        features["strength_gap_abs_60"] = abs(strength_gap_60)
        features["strength_gap_15_60_delta"] = strength_gap_15 - strength_gap_60
        features["atr_spread_efficiency"] = atr_pips / (spread_pips + eps)
        features["spread_to_atr240"] = spread_pips / atr_pips
        features["spread_cost_pressure"] = spread_ratio * regime_spread_cost
        features["compression_breakout_pressure"] = (
            (1.0 - clamp(compression, 0.0, 1.0)) * realized_vol_ratio
        )
        features["compression_then_expansion"] = b(
            compression <= 0.65 and realized_vol_ratio >= 1.05
        )
        features["volatility_expansion_pressure"] = max(
            realized_vol_ratio - 1.0,
            regime_vol_expansion,
        )
        features["liquidity_quality_score"] = (
            math.log1p(max(features["atr_spread_efficiency"], 0.0))
            - max(spread_ratio, 0.0)
            - is_rollover * 1.25
            - is_spread_impaired * 1.25
        )
        features["anti_chase_pressure"] = max(
            pos(spread_ratio - 1.65),
            pos(abs(m60) - 3.25),
            is_rollover,
            is_spread_impaired,
        )
        features["london_volatility_pressure"] = (
            v("is_london_session") * regime_vol_expansion
        )
        features["ny_volatility_pressure"] = (
            v("is_new_york_session") * regime_vol_expansion
        )
        features["overlap_breakout_pressure"] = (
            v("is_london_ny_overlap") * features["compression_breakout_pressure"]
        )
        features["rollover_spread_penalty"] = is_rollover * spread_ratio
        features["volatile_spread_penalty"] = (
            v("is_volatile_or_exotic_pair") * spread_ratio
        )
        features["risk_on_long_pressure"] = (
            v("regime_risk_on_score") * v("base_is_risk")
        )
        features["risk_off_jpy_chf_pressure"] = (
            v("regime_is_risk_off")
            * (
                v("quote_is_jpy")
                + v("quote_is_chf")
                + v("base_is_jpy")
                + v("base_is_chf")
            )
        )
        features["carry_trend_alignment"] = (
            v("carry_proxy_diff") * sign(features["trend_pressure_edge"])
        )
        features["carry_extreme_pressure"] = (
            v("carry_proxy_abs_diff") * v("is_volatile_or_exotic_pair")
        )
        features["macro_bias_trend_alignment"] = (
            v("macro_pair_bias") * sign(features["trend_pressure_edge"])
        )
        features["macro_event_spread_pressure"] = (
            v("macro_event_risk") * spread_ratio
        )

        liquidity_ok = (
            spread_ratio <= 1.65
            and features["atr_spread_efficiency"] >= 2.0
            and is_rollover < 0.5
            and is_spread_impaired < 0.5
        )
        anti_chase = (
            spread_ratio > 2.0
            or is_rollover > 0.5
            or is_spread_impaired > 0.5
            or abs(m60) > 3.25
        )
        compression_ready = (
            compression <= 0.55
            and realized_vol_ratio <= 1.15
            and spread_ratio <= 1.5
        )
        vol_expanding = (
            realized_vol_ratio >= 1.05
            or regime_vol_expansion >= 0.5
            or v("regime_is_high_vol_event") > 0.5
        )
        active_session = (
            v("is_london_session") > 0.5
            or v("is_new_york_session") > 0.5
            or v("is_london_ny_overlap") > 0.5
        )
        near_upper_range = features["near_upper_range_score"] >= 0.25
        near_lower_range = features["near_lower_range_score"] >= 0.25
        trend_long = (
            features["momentum_alignment_long"] >= 3
            and features["ema8_gt_ema21"] > 0
            and (ma_stack > 0 or features["macd_hist_positive"] > 0)
        )
        trend_short = (
            features["momentum_alignment_short"] >= 3
            and features["ema8_lt_ema21"] > 0
            and (ma_stack < 0 or features["macd_hist_negative"] > 0)
        )
        cross_long = (
            strength_gap_15 > 0
            and strength_gap_60 > -0.25
            and (
                v("macro_pair_bias") >= 0
                or v("regime_risk_on_score") > 0.25
                or v("regime_usd_strength_15") > 0
            )
        )
        cross_short = (
            strength_gap_15 < 0
            and strength_gap_60 < 0.25
            and (
                v("macro_pair_bias") <= 0
                or v("regime_is_risk_off") > 0.5
                or v("regime_usd_strength_15") < 0
            )
        )
        exhaustion_long = (
            near_lower_range
            and m30 < -1.2
            and features["momentum_5_15_delta_atr"] > 0
            and rsi <= -0.15
        )
        exhaustion_short = (
            near_upper_range
            and m30 > 1.2
            and features["momentum_5_15_delta_atr"] < 0
            and rsi >= 0.15
        )

        features["rule_pre_breakout_long"] = b(
            compression_ready and near_upper_range and m5 > -0.20 and liquidity_ok
        )
        features["rule_pre_breakout_short"] = b(
            compression_ready and near_lower_range and m5 < 0.20 and liquidity_ok
        )
        features["rule_breakout_continuation_long"] = b(
            trend_long
            and vol_expanding
            and (
                features["donchian60_breakout_long"] > 0
                or features["donchian240_breakout_long"] > 0
                or v("regime_acceleration_score") > 0.3
            )
            and liquidity_ok
        )
        features["rule_breakout_continuation_short"] = b(
            trend_short
            and vol_expanding
            and (
                features["donchian60_breakout_short"] > 0
                or features["donchian240_breakout_short"] > 0
                or v("regime_acceleration_score") < -0.3
            )
            and liquidity_ok
        )
        features["rule_exhaustion_reversal_long"] = b(exhaustion_long)
        features["rule_exhaustion_reversal_short"] = b(exhaustion_short)
        features["rule_session_timing_long"] = b(
            active_session
            and features["momentum_alignment_long"] >= 2
            and spread_ratio <= 1.8
        )
        features["rule_session_timing_short"] = b(
            active_session
            and features["momentum_alignment_short"] >= 2
            and spread_ratio <= 1.8
        )
        features["rule_cross_pressure_long"] = b(cross_long)
        features["rule_cross_pressure_short"] = b(cross_short)
        features["rule_liquidity_ok"] = b(liquidity_ok)
        features["rule_anti_chase_penalty"] = b(anti_chase)
        features["rule_baseline_long_score"] = (
            features["rule_pre_breakout_long"] * 1.15
            + features["rule_breakout_continuation_long"] * 1.35
            + features["rule_exhaustion_reversal_long"] * 0.85
            + features["rule_session_timing_long"] * 0.55
            + features["rule_cross_pressure_long"] * 0.90
            + features["rule_liquidity_ok"] * 0.35
            - features["rule_anti_chase_penalty"] * 1.40
        )
        features["rule_baseline_short_score"] = (
            features["rule_pre_breakout_short"] * 1.15
            + features["rule_breakout_continuation_short"] * 1.35
            + features["rule_exhaustion_reversal_short"] * 0.85
            + features["rule_session_timing_short"] * 0.55
            + features["rule_cross_pressure_short"] * 0.90
            + features["rule_liquidity_ok"] * 0.35
            - features["rule_anti_chase_penalty"] * 1.40
        )
        features["rule_baseline_direction_score"] = (
            features["rule_baseline_long_score"]
            - features["rule_baseline_short_score"]
        )
        features["rule_baseline_strength_score"] = max(
            features["rule_baseline_long_score"],
            features["rule_baseline_short_score"],
        )
        features["rule_baseline_long_candidate"] = b(
            features["rule_baseline_long_score"] >= 2.0
            and features["rule_baseline_long_score"]
            > features["rule_baseline_short_score"]
        )
        features["rule_baseline_short_candidate"] = b(
            features["rule_baseline_short_score"] >= 2.0
            and features["rule_baseline_short_score"]
            > features["rule_baseline_long_score"]
        )

        return {
            name: (value if math.isfinite(value) else 0.0)
            for name, value in features.items()
        }

    def promoted_model_segment_decision(
        self,
        manifest: Dict[str, Any],
        snapshot: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, Any]]:
        if not cfg_bool("FOREX_TECH_SEGMENT_GATE_ENABLED", default=True):
            return True, {"segment_gate": "disabled"}
        allowed_segments = manifest.get("allowed_segments", {})
        if not isinstance(allowed_segments, dict):
            allowed_segments = {}
        gates = [
            (
                "pair_taxonomy_primary",
                "allowed_pair_families",
                str(snapshot.get("pair_taxonomy_primary", "")),
            ),
            (
                "regime_primary",
                "allowed_regimes",
                str(snapshot.get("regime_primary", "")),
            ),
            (
                "session",
                "allowed_sessions",
                self.current_session_name(snapshot),
            ),
        ]
        decisions: Dict[str, Any] = {}
        for column, key, current in gates:
            allowed_raw = manifest.get(key) or allowed_segments.get(key) or []
            if isinstance(allowed_raw, str):
                allowed_raw = [allowed_raw]
            allowed = {str(x) for x in allowed_raw if str(x)}
            decisions[key] = {
                "current": current,
                "allowed": sorted(allowed),
            }
            if allowed and current not in allowed:
                return False, {
                    "reason": f"segment gate rejected {column}={current}",
                    "segment_gate": decisions,
                }
        return True, {"segment_gate": decisions}

    def promoted_model_macro_decision(
        self,
        signal: Dict[str, Any],
        snapshot: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, Any]]:
        if not cfg_bool("FOREX_TECH_MACRO_GATE_ENABLED", default=True):
            return True, {"macro_gate": "disabled"}
        if not bool(snapshot.get("_macro_bias_current", False)):
            return True, {
                "macro_gate": "stale_or_missing",
                "age_hours": safe_float(snapshot.get("macro_bias_age_hours"), 999.0),
            }
        news_risk = safe_float(snapshot.get("macro_news_risk"), 0.0)
        event_risk = safe_float(snapshot.get("macro_event_risk"), 0.0)
        if news_risk >= cfg_float("FOREX_TECH_MACRO_NEWS_RISK_REJECT", default=0.85):
            return False, {
                "reason": f"macro news risk too high {news_risk:.2f}",
                "macro_gate": "news_risk",
            }
        if event_risk >= cfg_float("FOREX_TECH_MACRO_EVENT_RISK_REJECT", default=0.85):
            return False, {
                "reason": f"macro event risk too high {event_risk:.2f}",
                "macro_gate": "event_risk",
            }
        direction = str(signal.get("direction", "")).upper()
        bias = clamp(
            safe_float(snapshot.get("macro_pair_bias"), 0.0)
            + 0.5 * safe_float(snapshot.get("macro_rate_diff_bias"), 0.0),
            -1.0,
            1.0,
        )
        contra = cfg_float("FOREX_TECH_MACRO_CONTRA_BIAS_REJECT", default=0.60)
        if direction == "LONG" and bias <= -contra:
            return False, {
                "reason": f"macro bias conflicts with LONG {bias:.2f}",
                "macro_gate": "contra_bias",
                "macro_pair_bias": bias,
            }
        if direction == "SHORT" and bias >= contra:
            return False, {
                "reason": f"macro bias conflicts with SHORT {bias:.2f}",
                "macro_gate": "contra_bias",
                "macro_pair_bias": bias,
            }
        return True, {
            "macro_gate": "passed",
            "macro_pair_bias": bias,
            "news_risk": news_risk,
            "event_risk": event_risk,
        }

    def add_cross_pair_factor_features(
        self,
        snapshots: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        currency_returns: Dict[int, Dict[str, List[float]]] = {
            15: {},
            60: {},
        }
        for instrument, snapshot in snapshots.items():
            base, quote = split_instrument(instrument)
            for horizon in [15, 60]:
                pair_return = safe_float(
                    snapshot.get(f"pair_log_return_{horizon}")
                )
                currency_returns[horizon].setdefault(base, []).append(
                    pair_return
                )
                currency_returns[horizon].setdefault(quote, []).append(
                    -pair_return
                )

        def median(values: Sequence[float]) -> float:
            ordered = sorted(value for value in values if math.isfinite(value))
            if not ordered:
                return 0.0
            middle = len(ordered) // 2
            if len(ordered) % 2:
                return ordered[middle]
            return (ordered[middle - 1] + ordered[middle]) / 2.0

        strengths: Dict[int, Dict[str, float]] = {
            horizon: {
                currency: median(values)
                for currency, values in by_currency.items()
            }
            for horizon, by_currency in currency_returns.items()
        }
        ranks: Dict[int, Dict[str, float]] = {}
        for horizon, by_currency in strengths.items():
            ordered = sorted(by_currency.items(), key=lambda item: item[1])
            denominator = max(1, len(ordered) - 1)
            ranks[horizon] = {
                currency: index / denominator
                for index, (currency, _) in enumerate(ordered)
            }
        now = utc_now()
        for instrument, snapshot in snapshots.items():
            base, quote = split_instrument(instrument)
            for horizon in [15, 60]:
                snapshot[f"strength_gap_{horizon}"] = (
                    strengths[horizon].get(base, 0.0)
                    - strengths[horizon].get(quote, 0.0)
                )
                snapshot[f"strength_gap_rank_{horizon}"] = (
                    ranks[horizon].get(base, 0.5)
                    - ranks[horizon].get(quote, 0.5)
                )
            rules = technical_exhaustion_rule_matches(
                snapshot,
                safe_float(snapshot.get("strength_gap_15")),
            )
            snapshot["shadow_m30_strength"] = int(
                "m30_strength15_upper" in rules
            )
            snapshot["shadow_m15_strength"] = int(
                "m15_strength15_upper" in rules
            )
            snapshot["shadow_m5_strength"] = int(
                "m5_strength15_upper" in rules
            )
            snapshot["shadow_m15_m30"] = int("m15_m30_upper" in rules)
            snapshot["shadow_m5_m30"] = int("m5_m30_upper" in rules)
            snapshot["shadow_rule_count"] = len(rules)
            snapshot.update(self.live_calendar_features(now))
            snapshot.update(self.instrument_research_subset_features(instrument))
            snapshot["pair_taxonomy_primary"] = self.pair_taxonomy_primary(instrument)
            snapshot.update(self.pair_taxonomy_features(instrument))
            snapshot.update(self.carry_proxy_features(instrument))
            snapshot.update(self.live_regime_features(snapshot, instrument))
            snapshot.update(self.current_macro_feature_snapshot(instrument))
            snapshot.update(self.expanded_indicator_snapshot_features(snapshot))
        return snapshots

    def major_move_factor_snapshot(
        self,
        snapshot: Dict[str, Any],
        signal_direction: str,
    ) -> Dict[str, Any]:
        self.sync_major_move_factor_manifest()
        bundle = self._major_move_factor_bundle
        manifest = self._major_move_factor_manifest
        if not bundle or not manifest:
            return {}
        try:
            features = list(bundle.get("features") or [])
            values = [safe_float(snapshot.get(feature), float("nan")) for feature in features]
            if not features or any(not math.isfinite(value) for value in values):
                return {}
            event_model = bundle.get("event_model")
            direction_model = bundle.get("direction_model")
            if event_model is None or direction_model is None:
                return {}
            event_probability = predict_positive_probability_named(
                event_model,
                features,
                values,
            )
            calibrator = bundle.get("probability_calibrator")
            if calibrator is not None:
                event_probability = float(
                    calibrator.predict_proba([[event_probability]])[0][1]
                )
            up_probability = predict_positive_probability_named(
                direction_model,
                features,
                values,
            )
            predicted_direction = (
                "LONG"
                if up_probability >= safe_float(
                    bundle.get("direction_threshold"),
                    0.5,
                )
                else "SHORT"
            )
            direction_confidence = max(up_probability, 1.0 - up_probability)
            direction_confidence_threshold = safe_float(
                bundle.get("direction_confidence_threshold"),
                0.5,
            )
            event_threshold = safe_float(
                bundle.get("event_threshold"),
                0.10,
            )
            agreement = predicted_direction == signal_direction.upper()
            modifier_enabled = bool(
                manifest.get("score_modifier_enabled", False)
            )
            score_adjustment = 0.0
            risk_multiplier = 1.0
            confidence_gate = direction_confidence >= direction_confidence_threshold
            if (
                modifier_enabled
                and event_probability >= event_threshold
                and confidence_gate
            ):
                confidence = clamp(
                    (event_probability - event_threshold)
                    / max(1.0 - event_threshold, 0.01),
                    0.0,
                    1.0,
                )
                if agreement:
                    score_adjustment = 3.0 + confidence * 5.0
                    risk_multiplier = 1.0 + confidence * 0.15
                else:
                    score_adjustment = -(5.0 + confidence * 7.0)
                    risk_multiplier = max(0.35, 0.70 - confidence * 0.35)
            return {
                "event_probability": event_probability,
                "event_threshold": event_threshold,
                "up_probability": up_probability,
                "direction_confidence": direction_confidence,
                "direction_confidence_threshold": direction_confidence_threshold,
                "direction_confidence_gate": confidence_gate,
                "predicted_direction": predicted_direction,
                "direction_agreement": agreement,
                "score_adjustment": score_adjustment,
                "risk_multiplier": risk_multiplier,
                "modifier_enabled": modifier_enabled,
                "experiment_id": manifest.get("experiment_id", ""),
            }
        except Exception as exc:
            self.log_error("major move factor snapshot", exc)
            return {}

    def enrich_signals_with_major_move_factor(
        self,
        signals: List[Dict[str, Any]],
        snapshots: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not signals or not snapshots:
            return signals
        self.add_cross_pair_factor_features(snapshots)
        for signal in signals:
            instrument = normalize_instrument(signal.get("instrument"))
            snapshot = snapshots.get(instrument)
            if not snapshot:
                continue
            factor = self.major_move_factor_snapshot(
                snapshot,
                str(signal.get("direction", "")),
            )
            if factor:
                signal["_major_move_factor"] = factor
                signal["major_move_event_probability"] = factor[
                    "event_probability"
                ]
                signal["major_move_direction_probability"] = factor[
                    "up_probability"
                ]
        return signals

    def technical_exhaustion_shadow_signals(
        self,
        snapshots: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Score stable reversal-risk conditions without routing an order."""
        if not cfg_bool("FOREX_TECH_EXHAUSTION_SHADOW_ENABLED", default=True):
            return []
        currency_returns: Dict[str, List[float]] = {}
        for instrument, snapshot in snapshots.items():
            base, quote = split_instrument(instrument)
            pair_return = safe_float(snapshot.get("pair_log_return_15"))
            currency_returns.setdefault(base, []).append(pair_return)
            currency_returns.setdefault(quote, []).append(-pair_return)

        def median(values: Sequence[float]) -> float:
            clean = sorted(value for value in values if math.isfinite(value))
            if not clean:
                return 0.0
            middle = len(clean) // 2
            if len(clean) % 2:
                return clean[middle]
            return (clean[middle - 1] + clean[middle]) / 2.0

        strengths = {
            currency: median(values)
            for currency, values in currency_returns.items()
        }
        m5_min = cfg_float("FOREX_TECH_EXHAUSTION_M5_ATR_MIN", default=0.80)
        m15_min = cfg_float("FOREX_TECH_EXHAUSTION_M15_ATR_MIN", default=1.35)
        m30_min = cfg_float("FOREX_TECH_EXHAUSTION_M30_ATR_MIN", default=1.88)
        gap_min = cfg_float(
            "FOREX_TECH_EXHAUSTION_STRENGTH_GAP15_MIN",
            default=0.00045,
        )
        minimum_rules = max(
            1,
            cfg_int("FOREX_TECH_EXHAUSTION_MIN_RULES", default=1),
        )
        exclude_usd = cfg_bool(
            "FOREX_TECH_EXHAUSTION_EXCLUDE_USD",
            default=True,
        )
        signals: List[Dict[str, Any]] = []
        for instrument, snapshot in snapshots.items():
            if exclude_usd and instrument_has_usd(instrument):
                continue
            base, quote = split_instrument(instrument)
            strength_gap = strengths.get(base, 0.0) - strengths.get(quote, 0.0)
            rules = technical_exhaustion_rule_matches(
                snapshot,
                strength_gap,
                m5_min=m5_min,
                m15_min=m15_min,
                m30_min=m30_min,
                strength_gap_min=gap_min,
            )
            if len(rules) < minimum_rules:
                continue
            signal = dict(snapshot)
            signal.update(
                {
                    "direction": "SHORT",
                    "status": "shadow_only",
                    "entry_mode": "validated_exhaustion_shadow",
                    "strength_gap_15": strength_gap,
                    "rules": rules,
                    "rule_count": len(rules),
                    "reason": (
                        "rolling validation concentrated 60/120m downside tail "
                        "risk, but first-trigger SHORT expectancy was negative; "
                        "telemetry only and no order is permitted"
                    ),
                }
            )
            signals.append(signal)
        return sorted(
            signals,
            key=lambda row: (
                safe_int(row.get("rule_count")),
                safe_float(row.get("momentum_30_atr")),
                safe_float(row.get("strength_gap_15")),
            ),
            reverse=True,
        )

    def log_exhaustion_shadow_watchlist(
        self,
        signals: List[Dict[str, Any]],
    ) -> int:
        """Log each completed 5m shadow condition once per instrument."""
        if not signals:
            return 0
        logged = 0
        state = self.load_state()
        seen = state.get("technical_exhaustion_seen", {})
        if not isinstance(seen, dict):
            seen = {}
        limit = max(1, cfg_int("FOREX_TECH_EXHAUSTION_LOG_TOP", default=20))
        for signal in signals[:limit]:
            instrument = normalize_instrument(signal.get("instrument", ""))
            end_utc = str(signal.get("end_utc", ""))
            if not instrument or seen.get(instrument) == end_utc:
                continue
            row = {
                "time_utc": iso_utc(),
                "time_ny": iso_ny(),
                "instrument": instrument,
                "direction": "SHORT",
                "status": "shadow_only",
                "rule_count": safe_int(signal.get("rule_count")),
                "rules": ",".join(signal.get("rules", [])),
                "momentum_5_atr": safe_float(signal.get("momentum_5_atr")),
                "momentum_15_atr": safe_float(signal.get("momentum_15_atr")),
                "momentum_30_atr": safe_float(signal.get("momentum_30_atr")),
                "strength_gap_15": safe_float(signal.get("strength_gap_15")),
                "atr240_pips": safe_float(signal.get("atr240_pips")),
                "start_utc": signal.get("start_utc", ""),
                "end_utc": end_utc,
                "reason": signal.get("reason", ""),
                "raw_json": json.dumps(signal, default=str)[:12000],
            }
            append_csv(
                self.cfg.data_dir / "exhaustion_shadow_watchlist.csv",
                row,
                EXHAUSTION_WATCHLIST_FIELDS,
            )
            try:
                self.full_logger.log_event(
                    "technical_exhaustion_shadow",
                    status="watch",
                    instrument=instrument,
                    direction="SHORT",
                    reason=str(row["reason"]),
                    result=(
                        f"rules={row['rule_count']} "
                        f"m30_atr={row['momentum_30_atr']:.2f} "
                        f"strength_gap15={row['strength_gap_15']:.6f}"
                    ),
                    raw=signal,
                )
            except Exception:
                pass
            seen[instrument] = end_utc
            logged += 1
        state["technical_exhaustion_seen"] = seen
        self.save_state(state)
        return logged

    def exhaustion_hybrid_scout_signals(
        self,
        shadow_signals: List[Dict[str, Any]],
        prices: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Convert validated exhaustion telemetry into model-gated scout candidates.

        The exhaustion rule remains non-executable by itself. Candidates created
        here are tagged as pre-spike/hybrid inputs and must still pass the active
        production model or ensemble fallback, EV scoring, broad-regime gate,
        spread/stop validation, duplicate controls, and account risk caps.
        """
        if not cfg_bool(
            "FOREX_TECH_EXHAUSTION_HYBRID_SCOUT_ENABLED",
            default=False,
        ):
            return []
        if not shadow_signals:
            return []
        min_rules = max(
            1,
            cfg_int("FOREX_TECH_EXHAUSTION_HYBRID_MIN_RULES", default=2),
        )
        min_score = cfg_float(
            "FOREX_TECH_EXHAUSTION_HYBRID_MIN_SCORE",
            default=72.0,
        )
        min_ratio = cfg_float(
            "FOREX_TECH_EXHAUSTION_HYBRID_MIN_MOVE_TO_SPREAD_RATIO",
            default=1.4,
        )
        min_net_mult = cfg_float(
            "FOREX_TECH_EXHAUSTION_HYBRID_MIN_NET_PIPS_MULTIPLIER",
            default=0.50,
        )
        max_spread_to_atr_stop = cfg_float(
            "FOREX_TECH_EXHAUSTION_HYBRID_MAX_SPREAD_TO_ATR_STOP_RATIO",
            default=0.35,
        )
        stop_atr = cfg_float(
            "FOREX_TECH_EXHAUSTION_HYBRID_STOP_ATR",
            default=0.75,
        )
        volatile_only = cfg_bool(
            "FOREX_TECH_EXHAUSTION_HYBRID_VOLATILE_ONLY",
            default=False,
        )
        max_candidates = max(
            0,
            cfg_int("FOREX_TECH_EXHAUSTION_HYBRID_MAX_SCOUTS_PER_SCAN", default=3),
        )
        if max_candidates <= 0:
            return []

        candidates: List[Dict[str, Any]] = []
        for signal in shadow_signals:
            instrument = normalize_instrument(signal.get("instrument", ""))
            if not instrument or instrument not in self.instrument_meta:
                continue
            if volatile_only and not self.is_volatile_scout_pair(instrument):
                continue
            direction = str(signal.get("direction", "SHORT")).upper()
            if direction != "SHORT":
                continue
            rule_count = safe_int(signal.get("rule_count"), 0)
            if rule_count < min_rules:
                continue
            atr_pips = safe_float(signal.get("atr240_pips"), 0.0)
            if atr_pips <= 0:
                continue
            price = prices.get(instrument, {})
            meta = self.instrument_meta.get(instrument)
            spread = (
                spread_pips(price, meta)
                if price and meta and price_tradeable(price)
                else safe_float(signal.get("spread_pips"), 0.0)
            )
            spread = max(0.1, spread)
            atr_stop_pips = max(0.1, atr_pips * max(0.05, stop_atr))
            spread_to_atr_stop = spread / max(atr_stop_pips, 0.1)
            if spread_to_atr_stop > max_spread_to_atr_stop:
                continue

            m5 = max(0.0, safe_float(signal.get("momentum_5_atr"), 0.0))
            m15 = max(0.0, safe_float(signal.get("momentum_15_atr"), 0.0))
            m30 = max(0.0, safe_float(signal.get("momentum_30_atr"), 0.0))
            impulse_atr = max(m5, m15, m30)
            impulse_pips = impulse_atr * atr_pips
            threshold = self.event_threshold_pips(instrument)
            min_net = threshold * max(0.05, min_net_mult)
            if impulse_pips < min_net:
                continue
            ratio = impulse_pips / spread
            if ratio < min_ratio:
                continue
            rule_score = clamp(rule_count * 9.0, 0.0, 30.0)
            momentum_score = clamp(
                (
                    m30
                    - cfg_float(
                        "FOREX_TECH_EXHAUSTION_M30_ATR_MIN",
                        default=1.88,
                    )
                )
                * 14.0,
                0.0,
                24.0,
            )
            ratio_score = clamp((ratio - min_ratio) * 8.0, 0.0, 18.0)
            net_score = clamp(impulse_pips / max(min_net, 1.0) * 14.0, 0.0, 18.0)
            spread_score = clamp(
                (max_spread_to_atr_stop - spread_to_atr_stop)
                / max(max_spread_to_atr_stop, 0.01)
                * 10.0,
                0.0,
                10.0,
            )
            score = clamp(
                rule_score + momentum_score + ratio_score + net_score + spread_score,
                0.0,
                100.0,
            )
            if score < min_score:
                continue
            enriched = dict(signal)
            rules = signal.get("rules", [])
            if not isinstance(rules, list):
                rules = [str(rules)]
            enriched.update({
                "instrument": instrument,
                "window_minutes": 30,
                "direction": "SHORT",
                "net_pips": impulse_pips,
                "abs_move_pips": impulse_pips,
                "cost_adjusted_net_pips": max(0.0, impulse_pips - spread),
                "mid_move_pips": -impulse_pips,
                "spread_avg_pips": spread,
                "move_to_spread_ratio": ratio,
                "pressure_score": score,
                "threshold_pips": threshold,
                "entry_mode": "hybrid_exhaustion_reversal_scout",
                "theme": "PRE_SPIKE_EXHAUSTION_REVERSAL",
                "status": "model_gated_candidate",
                "_pre_spike_pressure": True,
                "_exhaustion_hybrid_scout": True,
                "_exhaustion_stop_atr": stop_atr,
                "_risk_multiplier": cfg_float(
                    "FOREX_TECH_EXHAUSTION_HYBRID_RISK_MULTIPLIER",
                    default=0.70,
                ),
                "exhaustion_hybrid_score": score,
                "exhaustion_impulse_atr": impulse_atr,
                "exhaustion_impulse_pips": impulse_pips,
                "exhaustion_spread_to_atr_stop": spread_to_atr_stop,
                "reason": (
                    "hybrid exhaustion reversal scout candidate; "
                    f"rules={rule_count} {','.join(str(x) for x in rules[:5])} "
                    f"score={score:.1f} impulse={impulse_pips:.1f}p "
                    f"ratio={ratio:.2f} stop_cap={stop_atr:.2f}ATR"
                ),
            })
            candidates.append(enriched)

        candidates.sort(
            key=lambda row: (
                safe_float(row.get("exhaustion_hybrid_score"), 0.0),
                safe_float(row.get("move_to_spread_ratio"), 0.0),
                safe_float(row.get("net_pips"), 0.0),
            ),
            reverse=True,
        )
        return candidates[:max_candidates]

    def log_pressure_watchlist(self, pressure_rows: List[Dict[str, Any]], reason: str = "") -> None:
        if not pressure_rows:
            return
        try:
            for row_in in pressure_rows[: max(1, cfg_int("FOREX_TECH_PRE_SPIKE_PRESSURE_LOG_TOP", default=12))]:
                row = {
                    "time_utc": iso_utc(),
                    "time_ny": iso_ny(),
                    "instrument": normalize_instrument(row_in.get("instrument", "")),
                    "direction": str(row_in.get("direction", "")).upper(),
                    "pressure_score": f"{safe_float(row_in.get('pressure_score'), 0.0):.2f}",
                    "entry_mode": row_in.get("entry_mode", "pre_spike_pressure"),
                    "window_minutes": row_in.get("window_minutes", ""),
                    "net_pips": f"{safe_float(row_in.get('net_pips'), 0.0):.2f}",
                    "threshold_pips": f"{safe_float(row_in.get('threshold_pips'), 0.0):.2f}",
                    "move_to_spread_ratio": f"{safe_float(row_in.get('move_to_spread_ratio'), 0.0):.2f}",
                    "spread_avg_pips": f"{safe_float(row_in.get('spread_avg_pips'), 0.0):.2f}",
                    "basket_score": row_in.get("basket_score", ""),
                    "currency_pressure": row_in.get("currency_pressure", ""),
                    "reason": reason or row_in.get("reason", ""),
                    "raw_json": json.dumps(row_in, default=str)[:12000],
                }
                append_csv(self.cfg.data_dir / "pressure_watchlist.csv", row, PRESSURE_WATCHLIST_FIELDS)
                try:
                    self.full_logger.log_event("pre_spike_pressure", status="watch", instrument=row["instrument"], direction=row["direction"], reason=row["reason"], result=f"score={row['pressure_score']} pips={row['net_pips']} ratio={row['move_to_spread_ratio']}", raw=row_in)
                except Exception:
                    pass
        except Exception as exc:
            self.log_error("log pre-spike pressure + campaign compounding watchlist", exc)

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
        risk_ccy = RISK_CURRENCIES
        if (base in risk_ccy and direction == "LONG") or (quote in risk_ccy and direction == "SHORT"):
            themes.append("RISK_ON")
        if (base in risk_ccy and direction == "SHORT") or (quote in risk_ccy and direction == "LONG"):
            themes.append("RISK_OFF")
        commodity_ccy = COMMODITY_CURRENCIES
        if (base in commodity_ccy and direction == "LONG") or (quote in commodity_ccy and direction == "SHORT"):
            themes.append("COMMODITY_CURRENCY_STRENGTH")
        if (base in commodity_ccy and direction == "SHORT") or (quote in commodity_ccy and direction == "LONG"):
            themes.append("COMMODITY_CURRENCY_WEAKNESS")
        return sorted(set(themes))

    def event_scan_due(self, state: Optional[Dict[str, Any]] = None) -> bool:
        self.sync_promoted_model_manifest()
        if not self.cfg.event_scanner_enabled or fx_market_closed() or self.sunday_reopen_event_scanner_blocked() or self.volatile_weekend_new_entries_blocked():
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
        if not self.cfg.event_require_research_permission:
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
        return False, 0.0, "no active RESEARCH event permission for theme/pair/direction"

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
            max_net_seen = max(safe_float((rec or {}).get("max_net_pips"), -1e9), abs(safe_float(sig.get("net_pips"), 0.0)))
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

    def scout_signal_value_estimate(
        self,
        sig: Dict[str, Any],
        prices: Dict[str, Dict[str, Any]],
        account: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Estimate account-dollar value of a scout signal after spread and margin.

        Raw pips are a poor objective for high-spread/high-margin exotics. This
        estimate answers: if this move continued by the observed net pips, how
        much account-currency P/L could a fixed margin budget capture after a
        conservative one-spread cost?
        """
        inst = normalize_instrument(sig.get("instrument"))
        meta = self.instrument_meta.get(inst)
        if not inst or meta is None:
            return {"available": False, "reason": "missing instrument metadata"}
        nav = safe_float(account.get("nav", account.get("NAV", account.get("balance", 0.0))), 0.0)
        if nav <= 0:
            return {"available": False, "reason": "invalid account NAV"}
        quote_rate = quote_to_account_rate(inst, prices, self.cfg.account_currency)
        base_rate = base_to_account_rate(inst, prices, self.cfg.account_currency)
        if quote_rate is None or base_rate is None:
            return {"available": False, "reason": "account-currency conversion unavailable"}

        net_pips = abs(safe_float(sig.get("net_pips", sig.get("abs_move_pips", 0.0)), 0.0))
        spread_pips_value = safe_float(sig.get("spread_avg_pips"), 0.0)
        price = prices.get(inst, {})
        if price and price_tradeable(price):
            try:
                spread_pips_value = max(spread_pips_value, spread_pips(price, meta))
            except Exception:
                pass
        spread_cost_mult = max(0.0, cfg_float("FOREX_SCOUT_VALUE_SPREAD_COST_MULT", default=1.0))
        net_after_spread_pips = max(0.0, net_pips - spread_pips_value * spread_cost_mult)
        pip_value_per_unit = meta.pip_size * quote_rate
        margin_per_unit = max(base_rate * meta.margin_rate, 1e-12)

        margin_fraction = clamp(
            cfg_float("FOREX_SCOUT_VALUE_MARGIN_FRACTION", default=0.70),
            0.01,
            1.00,
        )
        configured_budget = nav * margin_fraction
        margin_available = safe_float(account.get("margin_available", account.get("marginAvailable", configured_budget)), configured_budget)
        margin_used = safe_float(account.get("margin_used", account.get("marginUsed", 0.0)), 0.0)
        max_margin_total = nav * max(0.0, self.cfg.max_margin_used_pct) / 100.0
        remaining_margin_budget = max(0.0, max_margin_total - margin_used)
        margin_budget = max(0.0, min(configured_budget, margin_available, remaining_margin_budget))
        units_by_margin = margin_budget / margin_per_unit if margin_budget > 0 else 0.0
        expected_account_pl = units_by_margin * net_after_spread_pips * pip_value_per_unit
        return_pct = (expected_account_pl / nav * 100.0) if nav > 0 else 0.0
        density_pct = (expected_account_pl / margin_budget * 100.0) if margin_budget > 0 else 0.0
        return {
            "available": True,
            "instrument": inst,
            "net_pips": net_pips,
            "spread_pips": spread_pips_value,
            "net_after_spread_pips": net_after_spread_pips,
            "pip_value_per_unit": pip_value_per_unit,
            "margin_per_unit": margin_per_unit,
            "margin_budget": margin_budget,
            "units_by_margin": units_by_margin,
            "expected_account_pl": expected_account_pl,
            "return_pct": return_pct,
            "density_pct": density_pct,
        }

    def enrich_scout_signal_value_weight(
        self,
        sig: Dict[str, Any],
        prices: Dict[str, Dict[str, Any]],
        account: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not cfg_bool("FOREX_SCOUT_VALUE_WEIGHTED_ENABLED", default=False):
            return sig
        sig2 = dict(sig)
        estimate = self.scout_signal_value_estimate(sig2, prices, account)
        sig2["_value_weighted_estimate"] = estimate
        if bool(estimate.get("available")):
            sig2["_value_weighted_expected_usd"] = safe_float(estimate.get("expected_account_pl"), 0.0)
            sig2["_value_weighted_return_pct"] = safe_float(estimate.get("return_pct"), 0.0)
            sig2["_value_weighted_net_after_spread_pips"] = safe_float(estimate.get("net_after_spread_pips"), 0.0)
            sig2["_value_weighted_density_pct"] = safe_float(estimate.get("density_pct"), 0.0)
        return sig2

    def scout_value_weight_score_and_reason(self, sig: Dict[str, Any]) -> Tuple[bool, float, float, str]:
        if not cfg_bool("FOREX_SCOUT_VALUE_WEIGHTED_ENABLED", default=False):
            return True, 0.0, 1.0, ""
        estimate = sig.get("_value_weighted_estimate") if isinstance(sig.get("_value_weighted_estimate"), dict) else {}
        if not bool(estimate.get("available")):
            if cfg_bool("FOREX_SCOUT_VALUE_REQUIRE_ESTIMATE", default=False):
                return False, 0.0, 0.0, f"value estimate unavailable: {estimate.get('reason', 'unknown')}"
            return True, 0.0, 1.0, "value estimate unavailable"
        expected_usd = safe_float(estimate.get("expected_account_pl"), 0.0)
        return_pct = safe_float(estimate.get("return_pct"), 0.0)
        min_expected_usd = max(0.0, cfg_float("FOREX_SCOUT_VALUE_MIN_EXPECTED_USD", default=0.0))
        min_return_pct = max(0.0, cfg_float("FOREX_SCOUT_VALUE_MIN_RETURN_PCT", default=0.0))
        hard_gate = cfg_bool("FOREX_SCOUT_VALUE_HARD_GATE", default=False)
        if hard_gate and expected_usd < min_expected_usd:
            return (
                False,
                0.0,
                0.0,
                "value gate: "
                f"expected ${expected_usd:.4f} < ${min_expected_usd:.4f}; "
                f"net_after_spread={safe_float(estimate.get('net_after_spread_pips'), 0.0):.1f}p "
                f"net={safe_float(estimate.get('net_pips'), 0.0):.1f}p "
                f"spread={safe_float(estimate.get('spread_pips'), 0.0):.1f}p "
                f"margin_budget=${safe_float(estimate.get('margin_budget'), 0.0):.2f}"
            )
        if hard_gate and return_pct < min_return_pct:
            return (
                False,
                0.0,
                0.0,
                "value gate: "
                f"return {return_pct:.3f}% < {min_return_pct:.3f}%; "
                f"expected=${expected_usd:.4f} "
                f"net_after_spread={safe_float(estimate.get('net_after_spread_pips'), 0.0):.1f}p "
                f"margin_budget=${safe_float(estimate.get('margin_budget'), 0.0):.2f}"
            )
        target_return_pct = max(0.01, cfg_float("FOREX_SCOUT_VALUE_TARGET_RETURN_PCT", default=1.0))
        max_score = max(0.0, cfg_float("FOREX_SCOUT_VALUE_SCORE_MAX", default=14.0))
        value_score = clamp((return_pct / target_return_pct) * max_score, 0.0, max_score)
        risk_target = max(0.01, cfg_float("FOREX_SCOUT_VALUE_RISK_TARGET_RETURN_PCT", default=0.75))
        risk_min = max(0.05, cfg_float("FOREX_SCOUT_VALUE_RISK_MULT_MIN", default=0.55))
        risk_max = max(risk_min, cfg_float("FOREX_SCOUT_VALUE_RISK_MULT_MAX", default=1.20))
        risk_mult = clamp(return_pct / risk_target, risk_min, risk_max)
        reason = (
            f"value=${expected_usd:.4f} return={return_pct:.3f}% "
            f"net_after_spread={safe_float(estimate.get('net_after_spread_pips'), 0.0):.1f}p "
            f"margin_budget=${safe_float(estimate.get('margin_budget'), 0.0):.2f} "
            f"value_score={value_score:.1f} risk_mult={risk_mult:.2f}"
        )
        return True, value_score, risk_mult, reason

    def scout_ev_score_and_risk(self, theme: str, sig: Dict[str, Any], basket_count: int) -> Tuple[bool, float, float, str]:
        if not self.cfg.scout_ev_scoring_enabled:
            return True, self.cfg.event_scout_risk_pct, 75.0, "EV scoring disabled; using base scout risk"
        inst = normalize_instrument(sig.get("instrument"))
        net = abs(safe_float(sig.get("net_pips", sig.get("abs_move_pips", 0.0)), 0.0))
        spread = max(0.1, safe_float(sig.get("spread_avg_pips"), 0.1))
        ratio = max(0.0, safe_float(sig.get("move_to_spread_ratio"), net / spread))
        drastic_override = bool(sig.get("_drastic_move_override"))
        exhaustion_hybrid = bool(sig.get("_exhaustion_hybrid_scout"))
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
        factor = (
            sig.get("_major_move_factor")
            if isinstance(sig.get("_major_move_factor"), dict)
            else {}
        )
        factor_adjustment = safe_float(
            factor.get("score_adjustment"),
            0.0,
        )
        factor_risk_multiplier = safe_float(
            factor.get("risk_multiplier"),
            1.0,
        )
        factor_note = (
            f" factor_p={safe_float(factor.get('event_probability')):.3f}"
            f" factor_dir={factor.get('predicted_direction','')}"
            f" factor_adj={factor_adjustment:+.1f}"
            if factor
            else ""
        )
        hybrid_boost = (
            cfg_float("FOREX_TECH_EXHAUSTION_HYBRID_EV_SCORE_BOOST", default=6.0)
            if exhaustion_hybrid
            else 0.0
        )
        signal_risk_multiplier = safe_float(sig.get("_risk_multiplier"), 1.0)
        if not math.isfinite(signal_risk_multiplier) or signal_risk_multiplier <= 0:
            signal_risk_multiplier = 1.0
        value_ok, value_score, value_risk_multiplier, value_note = self.scout_value_weight_score_and_reason(sig)
        if not value_ok:
            return False, 0.0, 0.0, value_note
        pair_kind = self.event_pair_kind(inst)
        return_pct = safe_float(sig.get("_value_weighted_return_pct"), 0.0)
        expected_usd = safe_float(sig.get("_value_weighted_expected_usd"), 0.0)
        unknown_profile = prof_rows < self.cfg.scout_profile_min_rows
        if unknown_profile and cfg_bool("FOREX_SCOUT_UNKNOWN_PROFILE_STRICT_GATE_ENABLED", default=False):
            min_ratio = cfg_float("FOREX_SCOUT_UNKNOWN_PROFILE_MIN_RATIO", default=3.0)
            min_net_multiplier = cfg_float("FOREX_SCOUT_UNKNOWN_PROFILE_MIN_NET_PIPS_MULTIPLIER", default=1.0)
            min_net_abs = cfg_float("FOREX_SCOUT_UNKNOWN_PROFILE_MIN_NET_PIPS", default=0.0)
            min_basket = cfg_int("FOREX_SCOUT_UNKNOWN_PROFILE_MIN_BASKET", default=1)
            min_return_pct = cfg_float("FOREX_SCOUT_UNKNOWN_PROFILE_MIN_VALUE_RETURN_PCT", default=0.0)
            min_expected_usd = cfg_float("FOREX_SCOUT_UNKNOWN_PROFILE_MIN_EXPECTED_USD", default=0.0)
            if pair_kind in {"major", "cross", "volatile"}:
                kind_prefix = f"FOREX_SCOUT_UNKNOWN_PROFILE_{pair_kind.upper()}_"
                min_ratio = cfg_float(f"{kind_prefix}MIN_RATIO", default=min_ratio)
                min_net_multiplier = cfg_float(
                    f"{kind_prefix}MIN_NET_PIPS_MULTIPLIER",
                    default=min_net_multiplier,
                )
                min_net_abs = cfg_float(f"{kind_prefix}MIN_NET_PIPS", default=min_net_abs)
                min_basket = cfg_int(f"{kind_prefix}MIN_BASKET", default=min_basket)
                min_return_pct = cfg_float(
                    f"{kind_prefix}MIN_VALUE_RETURN_PCT",
                    default=min_return_pct,
                )
                min_expected_usd = cfg_float(
                    f"{kind_prefix}MIN_EXPECTED_USD",
                    default=min_expected_usd,
                )
            net_floor = max(min_net_abs, threshold * max(0.0, min_net_multiplier))
            if ratio < min_ratio:
                return False, 0.0, 0.0, (
                    "unknown-profile scout blocked: move/spread "
                    f"{ratio:.2f} < {min_ratio:.2f}"
                )
            if net < net_floor:
                return False, 0.0, 0.0, (
                    "unknown-profile scout blocked: net pips "
                    f"{net:.1f} < {net_floor:.1f}"
                )
            if basket_count < min_basket:
                return False, 0.0, 0.0, (
                    "unknown-profile scout blocked: basket "
                    f"{basket_count} < {min_basket}"
                )
            if return_pct < min_return_pct:
                return False, 0.0, 0.0, (
                    "unknown-profile scout blocked: value return "
                    f"{return_pct:.3f}% < {min_return_pct:.3f}%"
                )
            if expected_usd < min_expected_usd:
                return False, 0.0, 0.0, (
                    "unknown-profile scout blocked: expected value "
                    f"${expected_usd:.4f} < ${min_expected_usd:.4f}"
                )

        if volatile:
            if basket_count < max(1, self.cfg.scout_volatile_min_basket_pairs):
                return False, 0.0, 0.0, f"volatile scout basket below min {basket_count} < {self.cfg.scout_volatile_min_basket_pairs}"
            if net < self.cfg.scout_volatile_min_net_pips:
                return False, 0.0, 0.0, f"volatile scout net pips too low {net:.1f} < {self.cfg.scout_volatile_min_net_pips:.1f}"
            if ratio < self.cfg.scout_volatile_min_move_to_spread_ratio and not drastic_override:
                return False, 0.0, 0.0, f"volatile scout move/spread too low {ratio:.2f} < {self.cfg.scout_volatile_min_move_to_spread_ratio:.2f}"
            if repeats > max(1, self.cfg.scout_volatile_repeat_trigger_cap):
                return False, 0.0, 0.0, f"volatile repeat trigger cap hit for {sig.get('event_key','event')} repeats={repeats}"
            if age > self.cfg.scout_volatile_max_event_age_minutes:
                return False, 0.0, 0.0, f"volatile event too old for continuation age={age:.1f}m > {self.cfg.scout_volatile_max_event_age_minutes:.1f}m"
            if exhaustion_hybrid:
                hybrid_max_age = cfg_float(
                    "FOREX_TECH_EXHAUSTION_HYBRID_MAX_EVENT_AGE_MINUTES",
                    default=10.0,
                )
                hybrid_repeat_cap = max(
                    1,
                    cfg_int("FOREX_TECH_EXHAUSTION_HYBRID_REPEAT_TRIGGER_CAP", default=2),
                )
                if age > hybrid_max_age:
                    return False, 0.0, 0.0, (
                        "hybrid exhaustion scout stale "
                        f"age={age:.1f}m > {hybrid_max_age:.1f}m"
                    )
                if repeats > hybrid_repeat_cap:
                    return False, 0.0, 0.0, (
                        "hybrid exhaustion repeat cap hit "
                        f"repeats={repeats} > {hybrid_repeat_cap}"
                    )
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
            pre_spike_boost = 7.0 if bool(sig.get("_pre_spike_pressure")) else 0.0
            score = clamp(move_score + net_score + basket_score + early_score + profile_score + value_score + (8.0 if drastic_override else 0.0) + pre_spike_boost + factor_adjustment + hybrid_boost, 0.0, 100.0)
            min_score_to_trade = self.cfg.scout_volatile_min_ev_score_to_trade
            if exhaustion_hybrid:
                min_score_to_trade = min(
                    min_score_to_trade,
                    cfg_float(
                        "FOREX_TECH_EXHAUSTION_HYBRID_MIN_EV_SCORE",
                        default=84.0,
                    ),
                )
            if score < min_score_to_trade:
                return False, 0.0, score, f"volatile scout score too low {score:.1f} < {min_score_to_trade:.1f}; age={age:.1f}m ratio={ratio:.2f} ev15={ev15:.1f} ev30={ev30:.1f} hybrid={exhaustion_hybrid}{self.scout_audit_reason_suffix(sig)}"
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
            risk *= max(0.1, min(1.25, factor_risk_multiplier))
            risk *= max(0.1, min(1.25, signal_risk_multiplier))
            risk *= max(0.05, min(1.50, value_risk_multiplier))
            audit_note = self.scout_audit_reason_suffix(sig, include_value=False)
            reason = f"volatile_lane ev_score={score:.1f} age={age:.1f}m repeats={repeats} ratio={ratio:.2f} basket={basket_count} drastic_override={drastic_override} hybrid_exhaustion={exhaustion_hybrid} profile_rows={prof_rows} ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}{factor_note}" + (f" {value_note}" if value_note else "") + audit_note
            return True, max(0.0, min(risk, self.cfg.scout_volatile_max_risk_pct, self.cfg.max_risk_pct_per_trade)), score, reason

        if repeats > max(1, self.cfg.scout_max_repeat_triggers_per_key):
            return False, 0.0, 0.0, f"repeat trigger cap hit for {sig.get('event_key','event')} repeats={repeats}"
        if self.cfg.scout_skip_late_exhaustion and age > self.cfg.scout_late_exhaustion_age_minutes and prof_neg:
            return False, 0.0, 0.0, f"late exhaustion skip: age={age:.1f}m profile_ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}"
        if exhaustion_hybrid:
            hybrid_max_age = cfg_float(
                "FOREX_TECH_EXHAUSTION_HYBRID_MAX_EVENT_AGE_MINUTES",
                default=10.0,
            )
            hybrid_repeat_cap = max(
                1,
                cfg_int("FOREX_TECH_EXHAUSTION_HYBRID_REPEAT_TRIGGER_CAP", default=2),
            )
            if age > hybrid_max_age:
                return False, 0.0, 0.0, (
                    "hybrid exhaustion scout stale "
                    f"age={age:.1f}m > {hybrid_max_age:.1f}m"
                )
            if repeats > hybrid_repeat_cap:
                return False, 0.0, 0.0, (
                    "hybrid exhaustion repeat cap hit "
                    f"repeats={repeats} > {hybrid_repeat_cap}"
                )
        move_score = clamp((ratio - self.cfg.event_min_move_to_spread_ratio) / 4.0 * 25.0, 0.0, 25.0)
        net_score = clamp(net / threshold * 20.0, 0.0, 25.0)
        basket_score = clamp(10.0 + max(0, basket_count - self.cfg.event_min_basket_pairs) * 5.0, 0.0, 20.0)
        early_score = 20.0 if age <= self.cfg.scout_max_event_age_minutes_for_continuation else clamp(20.0 - (age - self.cfg.scout_max_event_age_minutes_for_continuation) * 1.5, 0.0, 20.0)
        if prof_rows >= self.cfg.scout_profile_min_rows:
            profile_score = clamp((ev15 / max(threshold, 1.0)) * 10.0 + (ev30 / max(threshold, 1.0)) * 10.0 + (wr30 - 0.45) * 30.0, -20.0, 20.0)
        else:
            profile_score = 3.0 if self.cfg.scout_allow_unknown_profile_base_risk else -12.0
        pre_spike_boost = 7.0 if bool(sig.get("_pre_spike_pressure")) else 0.0
        score = clamp(move_score + net_score + basket_score + early_score + profile_score + value_score + (8.0 if drastic_override else 0.0) + pre_spike_boost + factor_adjustment + hybrid_boost, 0.0, 100.0)
        min_score_to_trade = self.cfg.scout_min_ev_score_to_trade
        if exhaustion_hybrid:
            min_score_to_trade = min(
                min_score_to_trade,
                cfg_float(
                    "FOREX_TECH_EXHAUSTION_HYBRID_MIN_EV_SCORE",
                    default=84.0,
                ),
            )
        high_value_bypass = False
        if cfg_bool("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_ENABLED", default=False):
            allowed_kinds = {
                x.strip().lower()
                for x in cfg_list("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_PAIR_KINDS", default=["major"])
                if x.strip()
            }
            blocked_themes = {
                x.strip().upper()
                for x in cfg_list("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_BLOCK_THEMES", default=[])
                if x.strip()
            }
            theme_name = str(theme or sig.get("theme", "")).upper()
            bypass_floor = cfg_float("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_SCORE", default=78.0)
            bypass_min_ratio = cfg_float("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_RATIO", default=2.2)
            bypass_min_return = cfg_float("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_RETURN_PCT", default=0.75)
            bypass_min_expected = cfg_float("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_EXPECTED_USD", default=0.05)
            bypass_max_age = cfg_float("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MAX_AGE_MINUTES", default=4.0)
            bypass_min_net_mult = cfg_float("FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_NET_PIPS_MULTIPLIER", default=1.0)
            if (
                pair_kind in allowed_kinds
                and theme_name not in blocked_themes
                and score >= bypass_floor
                and ratio >= bypass_min_ratio
                and return_pct >= bypass_min_return
                and expected_usd >= bypass_min_expected
                and age <= bypass_max_age
                and net >= threshold * max(0.0, bypass_min_net_mult)
            ):
                high_value_bypass = True
                min_score_to_trade = min(min_score_to_trade, bypass_floor)
        if score < min_score_to_trade:
            return False, 0.0, score, f"scout EV score too low {score:.1f} < {min_score_to_trade:.1f}; age={age:.1f}m ratio={ratio:.2f} ev15={ev15:.1f} ev30={ev30:.1f} hybrid={exhaustion_hybrid}{self.scout_audit_reason_suffix(sig)}"
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
        if bool(sig.get("_pre_spike_pressure")):
            risk *= max(0.05, cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_RISK_MULTIPLIER", default=0.30))
        risk *= max(0.1, min(1.25, factor_risk_multiplier))
        risk *= max(0.1, min(1.25, signal_risk_multiplier))
        risk *= max(0.05, min(1.50, value_risk_multiplier))
        audit_note = self.scout_audit_reason_suffix(sig, include_value=False)
        bypass_note = " high_value_ev_bypass=True" if high_value_bypass else ""
        reason = f"ev_score={score:.1f} age={age:.1f}m repeats={repeats} ratio={ratio:.2f} basket={basket_count} drastic_override={drastic_override} pre_spike={bool(sig.get('_pre_spike_pressure'))} hybrid_exhaustion={exhaustion_hybrid}{bypass_note} profile_rows={prof_rows} ev15={ev15:.1f} ev30={ev30:.1f} wr30={wr30:.2f}{factor_note}" + (f" {value_note}" if value_note else "") + audit_note
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

    @staticmethod
    def _scout_audit_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "passed", "pass"}

    @staticmethod
    def _scout_audit_pick(source: Dict[str, Any], *keys: str, default: Any = "") -> Any:
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return value
        return default

    @staticmethod
    def _scout_audit_number(value: Any) -> Any:
        if value in (None, ""):
            return ""
        try:
            number = float(value)
            if not math.isfinite(number):
                return ""
            return round(number, 6)
        except Exception:
            return value

    def scout_audit_reason_suffix(self, sig: Dict[str, Any], *, include_value: bool = True) -> str:
        """Compact evidence string attached to scout reasons when pressure/value gates exist."""
        if not isinstance(sig, dict):
            return ""
        has_pressure_evidence = any(
            sig.get(key) not in (None, "")
            for key in (
                "_pre_spike_pressure",
                "_strict_pre_spike_pressure_gate",
                "_pressure_cluster_instruments",
                "_pressure_cluster_pairs_dirs",
                "_pressure_cluster_direction_instruments",
                "pressure_score",
            )
        )
        has_value_evidence = any(
            sig.get(key) not in (None, "")
            for key in (
                "_value_weighted_expected_usd",
                "_value_weighted_return_pct",
                "_value_weighted_net_after_spread_pips",
                "_value_weighted_density_pct",
            )
        ) or isinstance(sig.get("_value_weighted_estimate"), dict)
        if not has_pressure_evidence and not (include_value and has_value_evidence):
            return ""
        parts: List[str] = []
        if has_pressure_evidence:
            parts.extend(
                [
                    f"strict_gate={int(self._scout_audit_bool(sig.get('_strict_pre_spike_pressure_gate')))}",
                    f"cluster={safe_int(sig.get('_pressure_cluster_instruments'), 0)}",
                    f"dir_cluster={safe_int(sig.get('_pressure_cluster_direction_instruments'), 0)}",
                ]
            )
        if include_value and has_value_evidence:
            estimate = sig.get("_value_weighted_estimate") if isinstance(sig.get("_value_weighted_estimate"), dict) else {}
            expected = self._scout_audit_pick(sig, "_value_weighted_expected_usd", default=estimate.get("expected_account_pl", ""))
            return_pct = self._scout_audit_pick(sig, "_value_weighted_return_pct", default=estimate.get("return_pct", ""))
            if expected not in (None, ""):
                parts.append(f"value=${safe_float(expected, 0.0):.4f}")
            if return_pct not in (None, ""):
                parts.append(f"return={safe_float(return_pct, 0.0):.3f}%")
        return " audit[" + " ".join(parts) + "]" if parts else ""

    def scout_audit_payload(
        self,
        theme: str,
        sig: Dict[str, Any],
        *,
        reason: str = "",
        risk_pct: Any = "",
        decision_stage: str = "",
        status: str = "",
        action_type: str = "event_scout",
    ) -> Dict[str, Any]:
        """Small, stable subset of signal evidence carried into order actions."""
        estimate = sig.get("_value_weighted_estimate") if isinstance(sig.get("_value_weighted_estimate"), dict) else {}
        movement_key = str(sig.get("_movement_key", sig.get("movement_key", "")) or "")
        if not movement_key and sig.get("start_utc") and sig.get("end_utc"):
            try:
                movement_key = self.market_movement_key(sig)
            except Exception:
                movement_key = ""
        return {
            "_audit_theme": theme or sig.get("theme", sig.get("_movement_theme", "")),
            "_audit_reason": reason,
            "_audit_risk_pct": risk_pct,
            "_audit_decision_stage": decision_stage,
            "_audit_status": status,
            "_audit_action_type": action_type,
            "_movement_key": movement_key,
            "instrument": normalize_instrument(sig.get("instrument", "")),
            "direction": str(sig.get("direction", "")).upper(),
            "window_minutes": sig.get("window_minutes", sig.get("_movement_window_minutes", "")),
            "net_pips": sig.get("net_pips", sig.get("_movement_net_pips", "")),
            "threshold_pips": sig.get("threshold_pips", ""),
            "move_to_spread_ratio": sig.get("move_to_spread_ratio", ""),
            "spread_avg_pips": sig.get("spread_avg_pips", ""),
            "pressure_score": sig.get("pressure_score", ""),
            "scout_ev_score": sig.get("scout_ev_score", ""),
            "_pre_spike_pressure": sig.get("_pre_spike_pressure", ""),
            "_strict_pre_spike_pressure_gate": sig.get("_strict_pre_spike_pressure_gate", ""),
            "_pressure_cluster_instruments": sig.get("_pressure_cluster_instruments", ""),
            "_pressure_cluster_pairs_dirs": sig.get("_pressure_cluster_pairs_dirs", ""),
            "_pressure_cluster_direction_instruments": sig.get("_pressure_cluster_direction_instruments", ""),
            "_value_weighted_expected_usd": self._scout_audit_pick(
                sig,
                "_value_weighted_expected_usd",
                default=estimate.get("expected_account_pl", ""),
            ),
            "_value_weighted_return_pct": self._scout_audit_pick(
                sig,
                "_value_weighted_return_pct",
                default=estimate.get("return_pct", ""),
            ),
            "_value_weighted_net_after_spread_pips": self._scout_audit_pick(
                sig,
                "_value_weighted_net_after_spread_pips",
                default=estimate.get("net_after_spread_pips", ""),
            ),
            "_value_weighted_density_pct": self._scout_audit_pick(
                sig,
                "_value_weighted_density_pct",
                default=estimate.get("density_pct", ""),
            ),
            "_campaign_key": sig.get("_campaign_key", sig.get("campaign_key", "")),
            "_campaign_attempt_type": sig.get("_campaign_attempt_type", sig.get("campaign_attempt_type", "")),
        }

    def log_scout_audit_signal(
        self,
        theme: str,
        sig: Dict[str, Any],
        decision_stage: str,
        status: str,
        *,
        action_type: str = "event_scout",
        reason: str = "",
        reject_reason: str = "",
        risk_pct: Any = "",
        risk_usd: Any = "",
        margin_usd: Any = "",
        units: Any = "",
        fill_price: Any = "",
        raw_extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not isinstance(sig, dict):
            return
        payload = self.scout_audit_payload(
            theme,
            sig,
            reason=reason,
            risk_pct=risk_pct,
            decision_stage=decision_stage,
            status=status,
            action_type=action_type,
        )
        # Preserve any explicit audit fields from an order/action payload.
        payload.update({k: v for k, v in sig.items() if k.startswith("_audit_") or k.startswith("_campaign_")})
        if sig.get("_scout_audit") and isinstance(sig.get("_scout_audit"), dict):
            nested = dict(sig.get("_scout_audit") or {})
            nested.update({k: v for k, v in payload.items() if v not in (None, "")})
            payload = nested
        movement_key = str(payload.get("_movement_key", payload.get("movement_key", "")) or "")
        row_base: Dict[str, Any]
        logger = getattr(self, "full_logger", None)
        try:
            row_base = logger._base() if logger else {}
        except Exception:
            row_base = {}
        if not row_base:
            row_base = {
                "time_utc": iso_utc(),
                "time_ny": iso_ny(),
                "run_id": os.environ.get("FOREX_RUN_ID") or "",
                "script_name": Path(__file__).name,
                "script_version": "v5.34_scout_audit",
                "account_id": getattr(self.cfg, "oanda_account_id", ""),
                "account_lane": getattr(self.cfg, "account_lane", ""),
            }
        final_reason = reason or str(payload.get("_audit_reason", ""))
        final_reject = reject_reason
        final_risk_pct = risk_pct if risk_pct not in (None, "") else self._scout_audit_pick(
            payload,
            "_audit_risk_pct",
            "risk_pct",
            default="",
        )
        raw_payload = {"signal": sig, "audit": payload, "raw_extra": raw_extra or {}}
        row = dict(row_base)
        row.update({
            "decision_stage": decision_stage or payload.get("_audit_decision_stage", ""),
            "status": status or payload.get("_audit_status", ""),
            "action_type": action_type or payload.get("_audit_action_type", "event_scout"),
            "instrument": normalize_instrument(payload.get("instrument", "")),
            "direction": str(payload.get("direction", "")).upper(),
            "theme": theme or payload.get("_audit_theme", payload.get("theme", "")),
            "movement_key": movement_key,
            "campaign_key": payload.get("_campaign_key", payload.get("campaign_key", "")),
            "campaign_attempt_type": payload.get("_campaign_attempt_type", payload.get("campaign_attempt_type", "")),
            "window_minutes": payload.get("window_minutes", ""),
            "net_pips": self._scout_audit_number(payload.get("net_pips", "")),
            "threshold_pips": self._scout_audit_number(payload.get("threshold_pips", "")),
            "move_to_spread_ratio": self._scout_audit_number(payload.get("move_to_spread_ratio", "")),
            "spread_avg_pips": self._scout_audit_number(payload.get("spread_avg_pips", "")),
            "pressure_score": self._scout_audit_number(payload.get("pressure_score", "")),
            "scout_ev_score": self._scout_audit_number(payload.get("scout_ev_score", "")),
            "strict_gate_enabled": int(cfg_bool("FOREX_TECH_PRE_SPIKE_PRESSURE_STRICT_GATE_ENABLED", default=False)),
            "strict_gate_passed": int(self._scout_audit_bool(payload.get("_strict_pre_spike_pressure_gate", payload.get("strict_gate_passed", False)))),
            "pre_spike_pressure": int(self._scout_audit_bool(payload.get("_pre_spike_pressure", payload.get("pre_spike_pressure", False)))),
            "pressure_cluster_instruments": self._scout_audit_number(payload.get("_pressure_cluster_instruments", payload.get("pressure_cluster_instruments", ""))),
            "pressure_cluster_pairs_dirs": self._scout_audit_number(payload.get("_pressure_cluster_pairs_dirs", payload.get("pressure_cluster_pairs_dirs", ""))),
            "pressure_cluster_direction_instruments": self._scout_audit_number(payload.get("_pressure_cluster_direction_instruments", payload.get("pressure_cluster_direction_instruments", ""))),
            "value_expected_usd": self._scout_audit_number(payload.get("_value_weighted_expected_usd", payload.get("value_expected_usd", ""))),
            "value_return_pct": self._scout_audit_number(payload.get("_value_weighted_return_pct", payload.get("value_return_pct", ""))),
            "value_net_after_spread_pips": self._scout_audit_number(payload.get("_value_weighted_net_after_spread_pips", payload.get("value_net_after_spread_pips", ""))),
            "value_density_pct": self._scout_audit_number(payload.get("_value_weighted_density_pct", payload.get("value_density_pct", ""))),
            "risk_pct": self._scout_audit_number(final_risk_pct),
            "risk_usd": self._scout_audit_number(risk_usd),
            "margin_usd": self._scout_audit_number(margin_usd),
            "units": units,
            "fill_price": fill_price,
            "reason": str(final_reason)[:1000],
            "reject_reason": str(final_reject)[:1000],
            "raw_json": json.dumps(raw_payload, default=str)[:12000],
        })
        if logger:
            logger._append_both("scout_audit_ledger.csv", row, SCOUT_AUDIT_FIELDS)
        else:
            append_csv(self.cfg.data_dir / "scout_audit_ledger.csv", row, SCOUT_AUDIT_FIELDS)

    def log_scout_audit_action(
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
        audit = action.get("_scout_audit") if isinstance(action.get("_scout_audit"), dict) else {}
        payload = dict(audit)
        payload.setdefault("instrument", normalize_instrument(action.get("instrument", "")))
        payload.setdefault("direction", str(action.get("direction", "")).upper())
        payload.setdefault("_audit_theme", action.get("_movement_theme", action.get("_event_theme", "")))
        payload.setdefault("_movement_key", action.get("_movement_key", ""))
        payload.setdefault("_campaign_key", action.get("_campaign_key", ""))
        payload.setdefault("_campaign_attempt_type", action.get("_campaign_attempt_type", ""))
        payload.setdefault("risk_pct", action.get("risk_pct", ""))
        payload["_audit_action"] = action
        self.log_scout_audit_signal(
            str(payload.get("_audit_theme", "")),
            payload,
            "order_result",
            status,
            action_type=action_type,
            reason=str(action.get("reason", action.get("why_now", payload.get("_audit_reason", "")))),
            reject_reason=reject_reason,
            risk_pct=action.get("risk_pct", payload.get("_audit_risk_pct", "")),
            risk_usd=risk_usd,
            margin_usd=margin_usd,
            units=units,
            fill_price=fill_price,
            raw_extra=raw_extra,
        )

    def log_event_signal(self, theme: str, status: str, signals: List[Dict[str, Any]], *, triggered_research: bool = False, scout_attempts: int = 0, reason: str = "") -> None:
        best = max(signals, key=lambda x: abs(safe_float(x.get("net_pips"), 0.0))) if signals else {}
        append_csv(self.cfg.event_signals_csv, {
            "time_utc": iso_utc(), "time_ny": iso_ny(), "theme": theme, "status": status,
            "signal_count": len(signals), "best_instrument": best.get("instrument", ""),
            "best_direction": best.get("direction", ""), "best_window_minutes": best.get("window_minutes", ""),
            "best_net_pips": best.get("net_pips", ""), "best_spread_pips": best.get("spread_avg_pips", ""),
            "triggered_research": triggered_research, "scout_attempts": scout_attempts, "reason": reason,
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

    def log_event_scan_summary(self, reason: str, stats: Dict[str, Any], top_candidates: List[Dict[str, Any]], rejected: List[Dict[str, Any]]) -> None:
        """Write one compact summary for every technical movement scan."""
        try:
            top_candidate = ""
            if top_candidates:
                t = max(top_candidates, key=lambda x: abs(safe_float(x.get("net_pips"), 0.0)))
                top_candidate = f"{normalize_instrument(t.get('instrument',''))} {str(t.get('direction','')).upper()} {safe_float(t.get('net_pips'),0.0):.1f}p ratio={safe_float(t.get('move_to_spread_ratio'),0.0):.2f}"
            top_reject_reason = ""
            if rejected:
                r = rejected[0]
                top_reject_reason = f"{normalize_instrument(r.get('instrument',''))} {r.get('reason','')} net={safe_float(r.get('net_pips'),0.0):.1f} ratio={safe_float(r.get('move_to_spread_ratio'),0.0):.2f}"
            row = {
                "time_utc": iso_utc(),
                "time_ny": iso_ny(),
                "reason": reason,
                "scanned": stats.get("scanned", 0),
                "tradeable": stats.get("tradeable", 0),
                "not_tradeable": stats.get("not_tradeable", 0),
                "spread_rejected": stats.get("spread_rejected", 0),
                "candle_errors": stats.get("candle_errors", 0),
                "no_windows": stats.get("no_windows", 0),
                "below_pips": stats.get("below_pips", 0),
                "below_ratio": stats.get("below_ratio", 0),
                "candidate_windows": stats.get("candidate_windows", 0),
                "signals": stats.get("signals", 0),
                "themes": stats.get("themes", 0),
                "watch_basket": stats.get("watch_basket", 0),
                "triggered_themes": stats.get("triggered_themes", 0),
                "pressure_watch": stats.get("pressure_watch", 0),
                "pressure_trade": stats.get("pressure_trade", 0),
                "exhaustion_shadow": stats.get("exhaustion_shadow", 0),
                "exhaustion_hybrid": stats.get("exhaustion_hybrid", 0),
                "scout_attempts": stats.get("scout_attempts", 0),
                "top_candidate": top_candidate,
                "top_reject_reason": top_reject_reason,
                "raw_json": json.dumps({"stats": stats, "top_candidates": top_candidates[:12], "rejected": rejected[:12]}, default=str)[:12000],
            }
            append_csv(self.cfg.data_dir / "event_scan_summary.csv", row, EVENT_SCAN_SUMMARY_FIELDS)
            try:
                self.full_logger.log_event("technical_scan_summary", status="logged", reason=reason, result=top_candidate or top_reject_reason, raw=row)
            except Exception:
                pass
            if cfg_bool("FOREX_EVENT_LOG_SCAN_SUMMARY_EVERY_SCAN", default=True):
                log(f"{self.lane_prefix()}[SCAN] scanned={row['scanned']} tradeable={row['tradeable']} candidates={row['candidate_windows']} pressure_watch={row.get('pressure_watch',0)} pressure_trade={row.get('pressure_trade',0)} exhaustion_shadow={row.get('exhaustion_shadow',0)} exhaustion_hybrid={row.get('exhaustion_hybrid',0)} signals={row['signals']} themes={row['themes']} triggered={row['triggered_themes']} attempts={row['scout_attempts']} top={top_candidate or top_reject_reason or 'none'}")
        except Exception as exc:
            self.log_error("log technical scan summary", exc)

    def maybe_event_trigger_research(self, theme: str, signals: List[Dict[str, Any]]) -> bool:
        if not self.cfg.event_trigger_research_enabled:
            return False
        state = self.load_state()
        last_raw = state.get("last_event_research_utc") or state.get("last_research_scan_utc")
        if last_raw:
            try:
                last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(UTC)
                age_min = (utc_now() - last).total_seconds() / 60.0
                if age_min < max(1, self.cfg.event_min_minutes_between_research_scans):
                    return False
            except Exception:
                pass
        state["last_event_research_utc"] = iso_utc()
        self.save_state(state)
        log(f"Event-triggered RESEARCH scan requested by {theme} basket ({len(signals)} signals).")
        self.run_research_scan(reason=f"event_trigger:{theme}")
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
        trailing_stop_pips = self.event_trailing_pips(inst)
        tp_multiple = max(0.5, self.cfg.event_scout_take_profit_r_multiple)
        if bool(sig.get("_exhaustion_hybrid_scout")):
            snapshot = (
                sig.get("_technical_feature_snapshot")
                if isinstance(sig.get("_technical_feature_snapshot"), dict)
                else {}
            )
            atr_pips = safe_float(sig.get("atr240_pips", snapshot.get("atr240_pips")), 0.0)
            spread_hint = max(0.1, safe_float(sig.get("spread_avg_pips"), 0.1))
            stop_atr = max(
                0.05,
                safe_float(
                    sig.get(
                        "_exhaustion_stop_atr",
                        cfg_float("FOREX_TECH_EXHAUSTION_HYBRID_STOP_ATR", default=0.75),
                    ),
                    0.75,
                ),
            )
            atr_stop = atr_pips * stop_atr if atr_pips > 0 else stop_pips
            spread_floor = spread_hint * cfg_float(
                "FOREX_TECH_EXHAUSTION_HYBRID_STOP_SPREAD_FLOOR_MULT",
                default=3.0,
            )
            min_stop = cfg_float(
                "FOREX_TECH_EXHAUSTION_HYBRID_MIN_STOP_PIPS",
                default=max(2.0, spread_floor),
            )
            if atr_stop > 0:
                stop_pips = max(min_stop, spread_floor, min(stop_pips, atr_stop))
            tp_multiple = cfg_float(
                "FOREX_TECH_EXHAUSTION_HYBRID_TP_R_MULTIPLE",
                default=2.0,
            )
            trailing_stop_pips = max(
                min_stop,
                min(
                    trailing_stop_pips,
                    stop_pips
                    * cfg_float(
                        "FOREX_TECH_EXHAUSTION_HYBRID_TRAILING_R_MULTIPLE",
                        default=1.0,
                    ),
                ),
            )
        tp_pips = stop_pips * max(0.5, tp_multiple)
        if direction == "LONG":
            stop_loss = entry - stop_pips * meta.pip_size
            take_profit = entry + tp_pips * meta.pip_size
        else:
            stop_loss = entry + stop_pips * meta.pip_size
            take_profit = entry - tp_pips * meta.pip_size
        audit_suffix = self.scout_audit_reason_suffix(sig)
        final_reason = reason if "audit[" in str(reason) or not audit_suffix else f"{reason}{audit_suffix}"
        return {
            "instrument": inst,
            "action": "OPEN",
            "direction": direction,
            "outlook_confidence": 0,
            "risk_pct": max(0.0, min(risk_pct, self.cfg.max_risk_pct_per_trade)),
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop_pips": trailing_stop_pips,
            "_event_scout": True,
            "_movement_key": self.market_movement_key(sig),
            "_movement_theme": theme,
            "_movement_window_minutes": sig.get("window_minutes", ""),
            "_movement_net_pips": sig.get("net_pips", ""),
            "_movement_start_utc": sig.get("start_utc", ""),
            "_movement_end_utc": sig.get("end_utc", ""),
            "_allow_sunday_reopen_open": cfg_bool("FOREX_REOPEN_ALLOW_CONFIRMED_EVENT_SCOUTS", default=True),
            "_scout_audit": self.scout_audit_payload(
                theme,
                sig,
                reason=final_reason,
                risk_pct=max(0.0, min(risk_pct, self.cfg.max_risk_pct_per_trade)),
                decision_stage="order_built",
                status="pending",
            ),
            "reason": (
                f"currency-strength synthetic scout {theme}: gap={safe_float(sig.get('_synthetic_strength_gap'), 0.0):+.2f}; "
                f"sources={','.join(sig.get('_synthetic_source_instruments', [])[:8])}; {final_reason}"
                if sig.get("_synthetic_strength_cross")
                else f"local M1 event scout {theme}: {safe_float(sig.get('net_pips'),0):.1f} net pips over {sig.get('window_minutes')}m; stop={stop_pips:.1f}p trail={trailing_stop_pips:.1f}p; {final_reason}"
            ),
        }

    def signal_strength_weight(self, sig: Dict[str, Any]) -> float:
        """Weight one M1 signal for currency-strength voting.

        This is intentionally local/technical: no RESEARCH, no news. A clean early move
        with good move/spread ratio counts more; old/exhausted repeats count less.
        """
        net = abs(safe_float(sig.get("net_pips"), 0.0))
        ratio = max(0.0, safe_float(sig.get("move_to_spread_ratio"), 0.0))
        age = max(0.0, safe_float(sig.get("event_age_minutes"), 0.0))
        ev_score = safe_float(sig.get("scout_ev_score"), 0.0)
        # Normalize the very large exotic pip values so they do not overwhelm all majors.
        inst = normalize_instrument(sig.get("instrument"))
        kind = self.event_pair_kind(inst)
        scale = 120.0 if kind == "volatile" else (30.0 if kind in {"cross", "exotic"} else 12.0)
        base = min(3.5, net / max(1.0, scale))
        ratio_bonus = min(2.0, ratio / 2.5)
        ev_bonus = 1.0 + max(0.0, min(30.0, ev_score - 60.0)) / 100.0 if ev_score else 1.0
        age_penalty = 1.0 / (1.0 + age / 20.0)
        return max(0.0, base * ratio_bonus * ev_bonus * age_penalty)

    def build_currency_strength_from_signals(self, signals: List[Dict[str, Any]]) -> Tuple[Dict[str, float], Dict[str, List[str]]]:
        """Convert pair-direction signals into base/quote currency votes.

        LONG BASE_QUOTE means base strengthens and quote weakens.
        SHORT BASE_QUOTE means base weakens and quote strengthens.
        """
        strength: Dict[str, float] = {}
        contributors: Dict[str, List[str]] = {}
        for sig in signals:
            inst = normalize_instrument(sig.get("instrument"))
            direction = str(sig.get("direction", "")).upper().strip()
            if not inst or direction not in {"LONG", "SHORT"}:
                continue
            base, quote = split_instrument(inst)
            if not base or not quote:
                continue
            w = self.signal_strength_weight(sig)
            if w <= 0:
                continue
            if direction == "LONG":
                strength[base] = strength.get(base, 0.0) + w
                strength[quote] = strength.get(quote, 0.0) - w
                contributors.setdefault(base, []).append(f"{inst} LONG +{w:.2f}")
                contributors.setdefault(quote, []).append(f"{inst} LONG -{w:.2f}")
            else:
                strength[base] = strength.get(base, 0.0) - w
                strength[quote] = strength.get(quote, 0.0) + w
                contributors.setdefault(base, []).append(f"{inst} SHORT -{w:.2f}")
                contributors.setdefault(quote, []).append(f"{inst} SHORT +{w:.2f}")
        return strength, contributors

    def active_scout_regime_lock(self, state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Return the current non-expired scout regime lock, if one exists."""
        if not cfg_bool("FOREX_SCOUT_REGIME_LOCK_ENABLED", default=True):
            return {}
        state = state or self.load_state()
        lock = state.get("scout_regime_lock")
        if not isinstance(lock, dict) or not lock:
            return {}
        expires_raw = str(lock.get("expires_utc", "")).strip()
        if expires_raw:
            try:
                expires = dt.datetime.fromisoformat(expires_raw.replace("Z", "+00:00")).astimezone(UTC)
                if expires <= utc_now():
                    return {}
            except Exception:
                return {}
        return lock

    def scout_regime_label(self, strongest: str, weakest: str, strength: Dict[str, float]) -> str:
        """Human-readable regime name for logs and campaign rows."""
        em_risk = set(RISK_CURRENCIES) | {"CNH", "HUF", "CZK", "PLN", "THB", "TRY", "MXN", "ZAR"}
        weak = {ccy for ccy, score in strength.items() if safe_float(score, 0.0) <= -1.0}
        strong = {ccy for ccy, score in strength.items() if safe_float(score, 0.0) >= 1.0}
        if strongest == "USD" and weak & em_risk:
            return "USD_RALLY_RISK_EM_WEAKNESS"
        if weakest == "USD" and strong & em_risk:
            return "USD_SELLOFF_RISK_EM_STRENGTH"
        if strongest in {"JPY", "CHF"} and weak & em_risk:
            return f"{strongest}_SAFE_HAVEN_RISK_OFF"
        if weakest in {"JPY", "CHF"} and strong & em_risk:
            return f"{weakest}_FUNDING_WEAK_RISK_ON"
        return f"{strongest}_STRONG_{weakest}_WEAK"

    def scout_regime_replacement_blocked(self, existing: Dict[str, Any], replacement: Dict[str, Any]) -> Tuple[bool, str]:
        """Prevent immediate flips after an extreme broad-regime lock.

        The backtests showed a recurring failure mode: the scanner correctly locks
        a broad move, then 20-40 minutes later a retracement cluster overwrites it
        and opens reversal scouts too early. This keeps the first extreme regime in
        force unless the new regime is materially stronger or the cooldown has aged.
        """
        if not cfg_bool("FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_ENABLED", default=True):
            return False, ""
        if not isinstance(existing, dict) or not isinstance(replacement, dict):
            return False, ""
        if str(existing.get("label", "")) == str(replacement.get("label", "")):
            return False, ""
        try:
            created = dt.datetime.fromisoformat(str(existing.get("created_utc", "")).replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            return False, ""
        age_min = (utc_now() - created).total_seconds() / 60.0
        cooldown_min = max(0.0, cfg_float("FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_MINUTES", default=60.0))
        if age_min >= cooldown_min:
            return False, ""
        min_gap = max(0.0, cfg_float("FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_MIN_GAP", default=30.0))
        old_gap = safe_float(existing.get("strength_gap"), 0.0)
        new_gap = safe_float(replacement.get("strength_gap"), 0.0)
        if old_gap < min_gap:
            return False, ""
        replacement_mult = max(1.0, cfg_float("FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_REPLACEMENT_GAP_MULT", default=1.35))
        if new_gap >= old_gap * replacement_mult:
            return False, ""
        return True, (
            f"anti-flip kept {existing.get('label')} age={age_min:.1f}m "
            f"old_gap={old_gap:.2f}; blocked replacement {replacement.get('label')} "
            f"new_gap={new_gap:.2f} until {cooldown_min:.0f}m or {replacement_mult:.2f}x stronger"
        )

    def update_scout_regime_lock(self, theme: str, signals: List[Dict[str, Any]], state: Dict[str, Any]) -> Dict[str, Any]:
        """Create/update a short-lived broad-regime lock from the latest scout basket.

        The lock is intentionally simple: it is not a trading signal by itself. It is a
        safety rail that prevents counter-basket churn while a broad currency thesis is
        fresh and supported by several instruments.
        """
        active = self.active_scout_regime_lock(state)
        if not cfg_bool("FOREX_SCOUT_REGIME_LOCK_ENABLED", default=True):
            return active
        strength, contributors = self.build_currency_strength_from_signals(signals)
        strength = {ccy: safe_float(score, 0.0) for ccy, score in strength.items() if abs(safe_float(score, 0.0)) > 0.001}
        ranked = sorted(strength.items(), key=lambda kv: kv[1], reverse=True)
        if len(ranked) < 2:
            return active
        instruments = sorted({
            normalize_instrument(sig.get("instrument"))
            for sig in signals or []
            if normalize_instrument(sig.get("instrument"))
        })
        min_instruments = max(1, cfg_int("FOREX_SCOUT_REGIME_LOCK_MIN_INSTRUMENTS", default=3))
        min_gap = max(0.1, cfg_float("FOREX_SCOUT_REGIME_LOCK_MIN_STRENGTH_GAP", default=4.5))
        gap = safe_float(ranked[0][1], 0.0) - safe_float(ranked[-1][1], 0.0)
        if len(instruments) < min_instruments or gap < min_gap:
            return active
        ttl_minutes = max(5.0, cfg_float("FOREX_SCOUT_REGIME_LOCK_TTL_MINUTES", default=50.0))
        now = utc_now()
        expires = now + dt.timedelta(minutes=ttl_minutes)
        top_n = max(2, cfg_int("FOREX_SCOUT_REGIME_LOCK_LOG_TOP_N", default=8))
        strongest = ranked[0][0]
        weakest = ranked[-1][0]
        label = self.scout_regime_label(strongest, weakest, strength)
        lock = {
            "label": label,
            "theme": self.normalize_campaign_theme(theme),
            "created_utc": iso_utc(now),
            "expires_utc": iso_utc(expires),
            "strongest": strongest,
            "weakest": weakest,
            "strength_gap": round(gap, 4),
            "strength": {ccy: round(score, 4) for ccy, score in ranked},
            "top_strength": [[ccy, round(score, 4)] for ccy, score in ranked[:top_n]],
            "bottom_strength": [[ccy, round(score, 4)] for ccy, score in ranked[-top_n:]],
            "source_instruments": instruments[:40],
            "source_instrument_count": len(instruments),
            "contributors": {ccy: vals[:8] for ccy, vals in contributors.items()},
        }
        blocked, block_reason = self.scout_regime_replacement_blocked(active, lock)
        if blocked:
            try:
                self.full_logger.log_event(
                    "scout_regime_lock",
                    status="anti_flip_kept_existing",
                    instrument="",
                    direction="",
                    reason=block_reason,
                    raw={"existing": active, "replacement": lock},
                )
            except Exception:
                pass
            log(f"{self.lane_prefix()}[REGIME_LOCK] {block_reason}")
            return active
        state["scout_regime_lock"] = lock
        try:
            self.full_logger.log_event(
                "scout_regime_lock",
                status="active",
                instrument="",
                direction="",
                reason=f"{label} gap={gap:.2f} strongest={strongest} weakest={weakest} instruments={len(instruments)}",
                raw=lock,
            )
        except Exception:
            pass
        log(f"{self.lane_prefix()}[REGIME_LOCK] {label} gap={gap:.2f} top={strongest} bottom={weakest} instruments={len(instruments)} ttl={ttl_minutes:.0f}m")
        return lock

    def scout_regime_pair_alignment(self, instrument: str, direction: str, lock: Optional[Dict[str, Any]]) -> Tuple[str, float, str]:
        """Return aligned/counter/neutral for a pair direction against a regime lock."""
        if not isinstance(lock, dict) or not lock:
            return "none", 0.0, "no active scout regime lock"
        strength = lock.get("strength") if isinstance(lock.get("strength"), dict) else {}
        inst = normalize_instrument(instrument)
        direction = str(direction or "").upper().strip()
        base, quote = split_instrument(inst)
        if not base or not quote or direction not in {"LONG", "SHORT"}:
            return "neutral", 0.0, "invalid pair/direction for scout regime check"
        base_score = safe_float(strength.get(base), 0.0)
        quote_score = safe_float(strength.get(quote), 0.0)
        edge = (base_score - quote_score) if direction == "LONG" else (quote_score - base_score)
        min_pair_gap = max(0.1, cfg_float("FOREX_SCOUT_REGIME_LOCK_MIN_PAIR_GAP", default=1.5))
        label = str(lock.get("label", "scout_regime"))
        reason = f"{label}: {inst} {direction} edge={edge:+.2f} {base}={base_score:+.2f} {quote}={quote_score:+.2f}"
        if edge >= min_pair_gap:
            return "aligned", edge, reason
        if edge <= -min_pair_gap:
            return "counter", edge, reason
        return "neutral", edge, reason

    def scout_regime_blocks_signal(self, sig: Dict[str, Any], lock: Optional[Dict[str, Any]]) -> Tuple[bool, str, str, float]:
        """Block fresh scout entries that fade the current broad-regime lock."""
        if not cfg_bool("FOREX_SCOUT_REGIME_LOCK_BLOCK_COUNTER", default=True):
            return False, "scout regime counter blocking disabled", "none", 0.0
        alignment, edge, reason = self.scout_regime_pair_alignment(
            normalize_instrument(sig.get("instrument")),
            str(sig.get("direction", "")).upper(),
            lock,
        )
        if alignment == "counter":
            return True, f"blocked counter-regime scout signal; {reason}", alignment, edge
        return False, reason, alignment, edge

    def scout_broad_regime_trade_gate(self, sig: Dict[str, Any], lock: Optional[Dict[str, Any]], ev_score: float) -> Tuple[bool, str]:
        """Optional deployment gate: trade only extreme broad-regime scout signals."""
        if not cfg_bool("FOREX_SCOUT_BROAD_REGIME_TRADE_ONLY", default=False):
            return True, "broad-regime-only gate disabled"
        inst = normalize_instrument(sig.get("instrument"))
        if bool(sig.get("_synthetic_strength_cross")) and not cfg_bool("FOREX_SCOUT_BROAD_REGIME_ALLOW_SYNTHETIC_CROSSES", default=False):
            return False, "broad-regime gate: synthetic strength-cross entries disabled for live scout"
        if cfg_bool("FOREX_SCOUT_BROAD_REGIME_VOLATILE_ONLY", default=False) and not self.is_volatile_scout_pair(inst):
            return False, f"broad-regime gate: {inst} is not in volatile scout universe"
        if cfg_bool("FOREX_SCOUT_BROAD_REGIME_ALLOW_MODEL_BYPASS", default=False):
            factor = (
                sig.get("_major_move_factor")
                if isinstance(sig.get("_major_move_factor"), dict)
                else {}
            )
            factor_min = cfg_float(
                "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_FACTOR_PROB",
                default=0.12,
            )
            factor_threshold = max(
                factor_min,
                safe_float(factor.get("event_threshold"), 0.0),
            )
            factor_ok = bool(
                factor
                and safe_float(factor.get("event_probability"), 0.0)
                >= factor_threshold
                and bool(factor.get("direction_agreement", False))
                and bool(factor.get("direction_confidence_gate", False))
            )
            is_pre_spike = bool(sig.get("_pre_spike_pressure")) or str(
                sig.get("theme", "")
            ).upper().startswith("PRE_SPIKE_")
            ensemble_fallback = bool(sig.get("_ensemble_scout_fallback"))
            exhaustion_hybrid = bool(sig.get("_exhaustion_hybrid_scout"))
            if is_pre_spike or factor_ok or ensemble_fallback:
                min_score = cfg_float(
                    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_EV_SCORE",
                    default=90.0,
                )
                if exhaustion_hybrid:
                    min_score = min(
                        min_score,
                        cfg_float(
                            "FOREX_TECH_EXHAUSTION_HYBRID_BYPASS_MIN_EV_SCORE",
                            default=84.0,
                        ),
                    )
                score = safe_float(ev_score, 0.0)
                if score < min_score:
                    return False, (
                        "model-bypass gate: score "
                        f"{score:.1f} < {min_score:.1f}"
                    )
                net = abs(
                    safe_float(
                        sig.get("net_pips", sig.get("abs_move_pips", 0.0)),
                        0.0,
                    )
                )
                min_net = cfg_float(
                    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_NET_PIPS",
                    default=40.0 if self.is_volatile_scout_pair(inst) else 8.0,
                )
                if net < min_net:
                    return False, (
                        "model-bypass gate: net pips "
                        f"{net:.1f} < {min_net:.1f}"
                    )
                ratio = safe_float(sig.get("move_to_spread_ratio"), 0.0)
                min_ratio = cfg_float(
                    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_RATIO",
                    default=1.4,
                )
                if ratio < min_ratio:
                    return False, (
                        "model-bypass gate: move/spread "
                        f"{ratio:.2f} < {min_ratio:.2f}"
                    )
                if (
                    not ensemble_fallback
                    and cfg_bool(
                        "FOREX_SCOUT_BROAD_REGIME_BYPASS_REQUIRE_PROMOTED_MODEL",
                        default=True,
                    )
                ):
                    decision = (
                        sig.get("_promoted_model_decision")
                        if isinstance(sig.get("_promoted_model_decision"), dict)
                        else {}
                    )
                    model_probability = safe_float(
                        decision.get("probability"),
                        float("nan"),
                    )
                    model_threshold = safe_float(
                        decision.get("threshold"),
                        0.0,
                    )
                    direction_ok = bool(decision.get("direction_ok", False))
                    if (
                        not math.isfinite(model_probability)
                        or model_probability < model_threshold
                        or not direction_ok
                    ):
                        return False, (
                            "model-bypass gate: promoted model confirmation "
                            "missing or below threshold"
                        )
                return True, (
                    "model-bypass gate passed; "
                    f"pre_spike={is_pre_spike} factor_ok={factor_ok} "
                    f"ensemble_fallback={ensemble_fallback} "
                    f"exhaustion_hybrid={exhaustion_hybrid} "
                    f"score={score:.1f} net={net:.1f} ratio={ratio:.2f}"
                )
        if not isinstance(lock, dict) or not lock:
            return False, "broad-regime gate: no active scout regime lock"
        min_score = cfg_float("FOREX_SCOUT_BROAD_REGIME_MIN_EV_SCORE", default=90.0)
        if safe_float(ev_score, 0.0) < min_score:
            return False, f"broad-regime gate: score {safe_float(ev_score, 0.0):.1f} < {min_score:.1f}"
        gap = safe_float(lock.get("strength_gap"), 0.0)
        min_gap = cfg_float("FOREX_SCOUT_BROAD_REGIME_MIN_STRENGTH_GAP", default=30.0)
        if gap < min_gap:
            return False, f"broad-regime gate: lock gap {gap:.2f} < {min_gap:.2f}"
        count = safe_int(lock.get("source_instrument_count"), 0)
        min_count = cfg_int("FOREX_SCOUT_BROAD_REGIME_MIN_INSTRUMENTS", default=15)
        if count < min_count:
            return False, f"broad-regime gate: lock instruments {count} < {min_count}"
        alignment, edge, reason = self.scout_regime_pair_alignment(inst, str(sig.get("direction", "")).upper(), lock)
        min_edge = cfg_float("FOREX_SCOUT_BROAD_REGIME_MIN_PAIR_EDGE", default=1.5)
        if alignment != "aligned" or edge < min_edge:
            return False, f"broad-regime gate: signal not aligned enough; {reason}; min_edge={min_edge:.2f}"
        return True, f"broad-regime gate passed; {reason}; gap={gap:.2f} instruments={count} score={safe_float(ev_score, 0.0):.1f}"

    def open_trade_currency_exposure(self, open_trades: List[Dict[str, Any]]) -> Dict[str, int]:
        """Simple signed currency exposure count from currently open trades.

        This is not notional-perfect; it is a fast guard to avoid repeatedly stacking
        the same currency thesis when many small scout positions are already open.
        """
        exposure: Dict[str, int] = {}
        for trade in open_trades or []:
            inst = normalize_instrument(trade.get("instrument"))
            if not inst:
                continue
            base, quote = split_instrument(inst)
            d = trade_direction_from_units(trade.get("currentUnits"))
            if d == "LONG":
                exposure[base] = exposure.get(base, 0) + 1
                exposure[quote] = exposure.get(quote, 0) - 1
            elif d == "SHORT":
                exposure[base] = exposure.get(base, 0) - 1
                exposure[quote] = exposure.get(quote, 0) + 1
        return exposure

    def candidate_adds_to_existing_exposure(self, inst: str, direction: str, exposure: Dict[str, int]) -> int:
        base, quote = split_instrument(inst)
        if direction == "LONG":
            return max(0, exposure.get(base, 0)) + max(0, -exposure.get(quote, 0))
        return max(0, -exposure.get(base, 0)) + max(0, exposure.get(quote, 0))

    def build_currency_strength_cross_signals(
        self,
        theme: str,
        signals: List[Dict[str, Any]],
        prices: Dict[str, Dict[str, Any]],
        open_trades: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Promote strongest-vs-weakest direct crosses as synthetic scout signals.

        Example:
            GBP_USD LONG votes GBP up / USD down.
            USD_MXN LONG votes USD up / MXN down.
            Net board can imply GBP > USD > MXN, so GBP_MXN LONG is promoted
            if OANDA has GBP_MXN and the spread is acceptable.
        """
        if not cfg_bool("FOREX_TECH_CURRENCY_STRENGTH_ENABLED", default=True):
            return []
        min_confirm = max(1, cfg_int("FOREX_TECH_STRENGTH_MIN_CONFIRMING_SIGNALS", default=2))
        if len({normalize_instrument(s.get("instrument")) for s in signals if normalize_instrument(s.get("instrument"))}) < min_confirm:
            return []
        strength, contributors = self.build_currency_strength_from_signals(signals)
        if len(strength) < 2:
            return []
        ranked_ccy = sorted(strength.items(), key=lambda kv: kv[1], reverse=True)
        if cfg_bool("FOREX_TECH_STRENGTH_LOG_TOP", default=True):
            top_txt = ", ".join(f"{c}={v:+.2f}" for c, v in ranked_ccy[:4])
            bot_txt = ", ".join(f"{c}={v:+.2f}" for c, v in ranked_ccy[-4:])
            log(f"{self.lane_prefix()}[STRENGTH] theme={theme} strong=[{top_txt}] weak=[{bot_txt}]")
        open_instruments = {normalize_instrument(t.get("instrument")) for t in open_trades or []}
        exposure = self.open_trade_currency_exposure(open_trades)
        min_gap = cfg_float("FOREX_TECH_STRENGTH_MIN_GAP", default=10.0)
        max_candidates = max(0, cfg_int("FOREX_TECH_STRENGTH_MAX_SYNTHETIC_CANDIDATES", default=8))
        max_same_exposure = max(0, cfg_int("FOREX_TECH_STRENGTH_MAX_SAME_CURRENCY_EXPOSURE", default=8))
        risk_mult = cfg_float("FOREX_TECH_STRENGTH_BASE_RISK_MULTIPLIER", default=0.75)
        risk_cap = cfg_float("FOREX_TECH_STRENGTH_MAX_RISK_PCT", default=0.45)
        overstack_penalty = cfg_float("FOREX_TECH_STRENGTH_OVERSTACK_PENALTY", default=0.50)
        source_insts = sorted({normalize_instrument(s.get("instrument")) for s in signals if normalize_instrument(s.get("instrument"))})
        candidates: List[Dict[str, Any]] = []
        for inst in sorted(set(self.instruments)):
            inst = normalize_instrument(inst)
            if not inst or inst in open_instruments or inst not in self.instrument_meta:
                continue
            if cfg_bool("FOREX_TECH_STRENGTH_REQUIRE_DIRECT_PRICE", default=True) and inst not in prices:
                continue
            price = prices.get(inst, {})
            if not price or not price_tradeable(price):
                continue
            meta = self.instrument_meta[inst]
            spr = spread_pips(price, meta)
            if spr > self.event_max_spread_pips(inst):
                continue
            base, quote = split_instrument(inst)
            gap = strength.get(base, 0.0) - strength.get(quote, 0.0)
            if abs(gap) < min_gap:
                continue
            direction = "LONG" if gap > 0 else "SHORT"
            stack = self.candidate_adds_to_existing_exposure(inst, direction, exposure)
            if max_same_exposure > 0 and stack >= max_same_exposure:
                continue
            # Convert the strength gap into a pseudo-pip impulse so existing EV/risk/order
            # code can be reused safely. The actual order still uses live bid/ask and stops.
            pseudo_pips = abs(gap)
            ratio = pseudo_pips / max(spr, 0.01)
            if ratio < (self.cfg.scout_volatile_min_move_to_spread_ratio if self.is_volatile_scout_pair(inst) else self.cfg.event_min_move_to_spread_ratio):
                continue
            base_risk = min(self.cfg.event_scout_risk_pct * risk_mult, risk_cap, self.cfg.max_risk_pct_per_trade)
            if stack > 0:
                base_risk *= max(0.10, min(1.0, overstack_penalty))
            sig = {
                "instrument": inst,
                "theme": f"{theme}_STRENGTH_CROSS",
                "direction": direction,
                "window_minutes": 0,
                "net_pips": pseudo_pips,
                "mid_move_pips": pseudo_pips,
                "spread_avg_pips": spr,
                "move_to_spread_ratio": ratio,
                "event_age_minutes": 0.0,
                "start_utc": iso_utc(),
                "end_utc": iso_utc(),
                "scout_ev_score": min(99.0, 68.0 + abs(gap) * 1.2 + min(8.0, len(source_insts))),
                "_synthetic_strength_cross": True,
                "_synthetic_source_theme": theme,
                "_synthetic_source_instruments": source_insts[:20],
                "_synthetic_strength_gap": gap,
                "_synthetic_strength_board": dict(sorted(strength.items(), key=lambda kv: kv[1], reverse=True)),
                "_synthetic_contributors_base": contributors.get(base, [])[:10],
                "_synthetic_contributors_quote": contributors.get(quote, [])[:10],
                "_risk_pct_override": base_risk,
                "_overstack_count": stack,
            }
            candidates.append(sig)
        candidates.sort(key=lambda s: (safe_float(s.get("scout_ev_score"), 0.0), safe_float(s.get("move_to_spread_ratio"), 0.0)), reverse=True)
        return candidates[:max_candidates]


    # -------------------------------------------------------------------------
    # v5.31 movement-campaign controller with rotation/pruning
    # -------------------------------------------------------------------------

    def normalize_campaign_theme(self, theme: str) -> str:
        t = str(theme or "").upper().strip()
        if t.startswith("PRE_SPIKE_"):
            t = t[len("PRE_SPIKE_"):]
        if t.endswith("_STRENGTH_CROSS"):
            t = t.replace("_STRENGTH_CROSS", "")
        return t or "TECHNICAL_MOVE"

    def campaign_key_for_signal(self, theme: str, sig: Dict[str, Any]) -> str:
        inst = normalize_instrument(sig.get("instrument"))
        direction = str(sig.get("direction", "")).upper().strip()
        ctheme = self.normalize_campaign_theme(theme)
        return f"{ctheme}:{inst}:{direction}"

    def campaign_seconds_since(self, iso_value: Any) -> float:
        if not iso_value:
            return 1e12
        try:
            ts = dt.datetime.fromisoformat(str(iso_value).replace("Z", "+00:00")).astimezone(UTC)
            return max(0.0, (utc_now() - ts).total_seconds())
        except Exception:
            return 1e12

    def open_trade_direction(self, trade: Dict[str, Any]) -> str:
        return trade_direction_from_units(trade.get("currentUnits", trade.get("current_units", 0)))

    def matching_open_trades(self, open_trades: List[Dict[str, Any]], inst: str, direction: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for t in open_trades or []:
            if normalize_instrument(t.get("instrument")) != inst:
                continue
            if self.open_trade_direction(t) == direction:
                out.append(t)
        return out

    def opposite_open_trades(self, open_trades: List[Dict[str, Any]], inst: str, direction: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for t in open_trades or []:
            if normalize_instrument(t.get("instrument")) != inst:
                continue
            d = self.open_trade_direction(t)
            if d and d != direction:
                out.append(t)
        return out

    def trade_unrealized_pl(self, trade: Dict[str, Any]) -> float:
        return safe_float(trade.get("unrealizedPL", trade.get("unrealized_pl", 0.0)), 0.0)

    def campaign_state_from_runtime(self) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        state = self.load_state()
        campaigns = state.get("movement_campaigns")
        if not isinstance(campaigns, dict):
            campaigns = {}
            state["movement_campaigns"] = campaigns
        return state, campaigns

    def save_campaign_state(self, state: Dict[str, Any], campaigns: Dict[str, Dict[str, Any]]) -> None:
        state["movement_campaigns"] = campaigns
        self.save_state(state)

    def log_movement_campaign(self, campaign_key: str, campaign: Dict[str, Any], action: str, reason: str = "", sig: Optional[Dict[str, Any]] = None, margin_used_pct: Any = "", current_unrealized_pl: Any = "") -> None:
        sig = sig or {}
        inst = normalize_instrument(campaign.get("instrument", sig.get("instrument", "")))
        row = {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "campaign_key": campaign_key,
            "movement_key": str(sig.get("_movement_key", sig.get("movement_key", campaign.get("movement_key", ""))))[:80],
            "instrument": inst,
            "direction": str(campaign.get("direction", sig.get("direction", ""))).upper(),
            "theme": campaign.get("theme", sig.get("theme", "")),
            "campaign_status": campaign.get("status", "active"),
            "campaign_action": action,
            "entries": campaign.get("entries", 0),
            "adds": campaign.get("adds", 0),
            "attempts": campaign.get("attempts", 0),
            "accepted": campaign.get("accepted", 0),
            "skipped": campaign.get("skipped", 0),
            "last_signal_score": sig.get("scout_ev_score", sig.get("pressure_score", "")),
            "last_net_pips": sig.get("net_pips", ""),
            "last_ratio": sig.get("move_to_spread_ratio", ""),
            "current_unrealized_pl": current_unrealized_pl,
            "margin_used_pct": margin_used_pct,
            "reason": str(reason)[:1000],
            "raw_json": json.dumps({"campaign": campaign, "signal": sig}, default=str)[:12000],
        }
        append_csv(self.cfg.data_dir / "movement_campaigns.csv", row, MOVEMENT_CAMPAIGN_FIELDS)
        try:
            self.full_logger.log_event("movement_campaign", status=action, instrument=inst, direction=row["direction"], movement_key=row["movement_key"], reason=reason, raw={"campaign": campaign, "signal": sig})
        except Exception:
            pass

    def log_compound_attempt(self, campaign_key: str, sig: Dict[str, Any], allowed: bool, attempt_type: str, risk_pct: float, reason: str, current_unrealized_pl: float = 0.0, margin_used_pct: float = 0.0, campaign: Optional[Dict[str, Any]] = None) -> None:
        campaign = campaign or {}
        row = {
            "time_utc": iso_utc(),
            "time_ny": iso_ny(),
            "campaign_key": campaign_key,
            "instrument": normalize_instrument(sig.get("instrument")),
            "direction": str(sig.get("direction", "")).upper(),
            "theme": sig.get("theme", campaign.get("theme", "")),
            "attempt_type": attempt_type,
            "allowed": bool(allowed),
            "current_unrealized_pl": current_unrealized_pl,
            "entries": campaign.get("entries", 0),
            "adds": campaign.get("adds", 0),
            "risk_pct": risk_pct,
            "margin_used_pct": margin_used_pct,
            "reason": str(reason)[:1000],
            "raw_json": json.dumps({"signal": sig, "campaign": campaign}, default=str)[:12000],
        }
        append_csv(self.cfg.data_dir / "compound_attempts.csv", row, COMPOUND_ATTEMPT_FIELDS)

    def log_position_health_snapshot(self, open_trades: List[Dict[str, Any]], campaigns: Dict[str, Dict[str, Any]], state: Dict[str, Any]) -> None:
        interval = max(15, cfg_int("FOREX_TECH_CAMPAIGN_LOG_HEALTH_SECONDS", default=60))
        if self.campaign_seconds_since(state.get("last_position_health_log_utc")) < interval:
            return
        profit_memory = state.get("trade_profit_memory") if isinstance(state.get("trade_profit_memory"), dict) else {}
        for t in open_trades or []:
            inst = normalize_instrument(t.get("instrument"))
            direction = self.open_trade_direction(t)
            trade_id = str(t.get("id", ""))
            current_pl = self.trade_unrealized_pl(t)
            mem = profit_memory.get(trade_id, {}) if isinstance(profit_memory, dict) else {}
            max_pl = max(current_pl, safe_float(mem.get("max_unrealized_pl", current_pl), current_pl))
            retained = 100.0 if max_pl <= 0 else max(0.0, min(100.0, current_pl / max_pl * 100.0))
            health = 50.0 + max(-25.0, min(25.0, current_pl * 25.0)) + max(-20.0, min(20.0, (retained - 50.0) * 0.4))
            ckey = ""
            for k, c in campaigns.items():
                if normalize_instrument(c.get("instrument")) == inst and str(c.get("direction", "")).upper() == direction:
                    ckey = k
                    break
            action = "KEEP"
            reason = "position healthy enough"
            if current_pl < 0 and retained <= 5:
                action = "WATCH_CUT"
                reason = "red position with no favorable excursion retained"
            elif current_pl > 0 and retained < 45:
                action = "TIGHTEN"
                reason = "winner has given back more than half of peak unrealized profit"
            row = {
                "time_utc": iso_utc(),
                "time_ny": iso_ny(),
                "trade_id": trade_id,
                "instrument": inst,
                "direction": direction,
                "current_units": t.get("currentUnits", t.get("current_units", "")),
                "unrealized_pl": current_pl,
                "max_unrealized_pl": max_pl,
                "mfe_retained_pct": retained,
                "campaign_key": ckey,
                "health_score": round(health, 3),
                "health_action": action,
                "reason": reason,
                "raw_json": json.dumps(summarize_trade_for_prompt(t), default=str)[:12000],
            }
            append_csv(self.cfg.data_dir / "position_health.csv", row, POSITION_HEALTH_FIELDS)
        state["last_position_health_log_utc"] = iso_utc()



    def trade_age_minutes(self, trade: Dict[str, Any]) -> float:
        raw = trade.get("openTime", trade.get("open_time", ""))
        if not raw:
            return 0.0
        try:
            ts = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
            return max(0.0, (utc_now() - ts).total_seconds() / 60.0)
        except Exception:
            return 0.0

    def active_campaign_count(self, campaigns: Dict[str, Dict[str, Any]], open_trades: List[Dict[str, Any]]) -> int:
        active_pairs = {(normalize_instrument(t.get("instrument")), self.open_trade_direction(t)) for t in open_trades or []}
        count = 0
        for c in campaigns.values():
            if not isinstance(c, dict):
                continue
            inst = normalize_instrument(c.get("instrument"))
            direction = str(c.get("direction", "")).upper()
            if (inst, direction) in active_pairs or str(c.get("status", "")).lower() == "active":
                count += 1
        return count

    def run_campaign_prune_pass(self, account_summary: Dict[str, Any], open_trades: List[Dict[str, Any]], state: Dict[str, Any], reason: str = "monitor", regime_lock: Optional[Dict[str, Any]] = None) -> int:
        """Close a small number of weak technical positions to free margin and reduce stale churn.

        This is deliberately conservative: it only cuts red/stale trades or winners that have
        given back most of their max favorable unrealized P/L. It never averages down.
        """
        if not cfg_bool("FOREX_TECH_CAMPAIGN_PRUNE_ENABLED", default=True):
            return 0
        interval = max(15, cfg_int("FOREX_TECH_CAMPAIGN_PRUNE_INTERVAL_SECONDS", default=60))
        if self.campaign_seconds_since(state.get("last_campaign_prune_utc")) < interval:
            return 0
        state["last_campaign_prune_utc"] = iso_utc()
        margin_pct = safe_float(account_summary.get("margin_used_pct_of_nav"), 0.0)
        margin_trigger = cfg_float("FOREX_TECH_CAMPAIGN_PRUNE_MARGIN_TRIGGER_PCT", default=55.0)
        stale_min = cfg_float("FOREX_TECH_CAMPAIGN_PRUNE_RED_AFTER_MINUTES", default=18.0)
        min_loss = cfg_float("FOREX_TECH_CAMPAIGN_PRUNE_MIN_LOSS_USD", default=0.015)
        min_giveback_mfe = abs(cfg_float(
            "FOREX_TECH_CAMPAIGN_PRUNE_GIVEBACK_MIN_MFE_USD",
            default=max(abs(min_loss) * 2.0, 0.03),
        ))
        giveback_pct = cfg_float("FOREX_TECH_CAMPAIGN_PRUNE_PROFIT_GIVEBACK_PCT", default=70.0)
        max_closes = max(0, cfg_int("FOREX_TECH_CAMPAIGN_PRUNE_MAX_CLOSES_PER_PASS", default=2))
        if max_closes <= 0 or not open_trades:
            return 0
        active_lock = regime_lock or self.active_scout_regime_lock(state)
        protect_min = max(stale_min, cfg_float("FOREX_SCOUT_REGIME_LOCK_PROTECT_ALIGNED_PRUNE_MINUTES", default=60.0))
        protect_loss = abs(cfg_float("FOREX_SCOUT_REGIME_LOCK_PROTECT_ALIGNED_LOSS_USD", default=0.06))
        profit_memory = state.get("trade_profit_memory") if isinstance(state.get("trade_profit_memory"), dict) else {}
        candidates: List[Tuple[float, str, Dict[str, Any]]] = []
        for t in open_trades or []:
            tid = str(t.get("id", ""))
            inst = normalize_instrument(t.get("instrument"))
            direction = self.open_trade_direction(t)
            cur_pl = self.trade_unrealized_pl(t)
            age_min = self.trade_age_minutes(t)
            mem = profit_memory.get(tid, {}) if isinstance(profit_memory, dict) else {}
            max_pl = max(cur_pl, safe_float(mem.get("max_unrealized_pl", cur_pl), cur_pl))
            alignment, _edge, alignment_reason = self.scout_regime_pair_alignment(inst, direction, active_lock)
            aligned_protected = (
                alignment == "aligned"
                and age_min < protect_min
                and cur_pl > -protect_loss
                and margin_pct < margin_trigger
            )
            if aligned_protected:
                try:
                    self.full_logger.log_event(
                        "campaign_prune_protected",
                        status="protected",
                        instrument=inst,
                        direction=direction,
                        trade_id=tid,
                        reason=f"{alignment_reason}; pl={cur_pl:.4f} age={age_min:.1f}m protect<{protect_min:.1f}m",
                        raw={"trade": summarize_trade_for_prompt(t), "regime_lock": active_lock, "reason": reason},
                    )
                except Exception:
                    pass
                continue
            # Cut red/stale positions, especially when margin is high.
            if cur_pl <= -abs(min_loss) and (age_min >= stale_min or margin_pct >= margin_trigger):
                priority = abs(cur_pl) + max(0.0, margin_pct - margin_trigger) / 100.0 + age_min / 1000.0
                why = f"campaign prune: red/stale {inst} {direction} pl={cur_pl:.4f} age={age_min:.1f}m margin={margin_pct:.2f}"
                candidates.append((priority, why, t))
                continue
            # Cut winners that already had a favorable excursion but gave most of it back.
            if max_pl > min_giveback_mfe:
                retained = 0.0 if max_pl <= 0 else max(0.0, min(100.0, cur_pl / max_pl * 100.0))
                if retained <= max(0.0, 100.0 - giveback_pct) and age_min >= max(5.0, stale_min / 2.0):
                    priority = (max_pl - cur_pl) + age_min / 1000.0
                    why = f"campaign prune: profit giveback {inst} {direction} current={cur_pl:.4f} max={max_pl:.4f} retained={retained:.1f}%"
                    candidates.append((priority, why, t))
        closed = 0
        for _priority, why, trade in sorted(candidates, key=lambda x: x[0], reverse=True)[:max_closes]:
            action = {
                "instrument": normalize_instrument(trade.get("instrument")),
                "trade_id": str(trade.get("id", "")),
                "action": "CLOSE",
                "direction": self.open_trade_direction(trade),
                "reason": why,
            }
            try:
                status = self.close_trade(trade, action)
                closed += 1 if str(status).startswith("accepted") or status == "dry_run" else 0
                try:
                    self.full_logger.log_event("campaign_prune", status=status, instrument=action["instrument"], direction=action["direction"], trade_id=action["trade_id"], reason=why, raw={"trade": summarize_trade_for_prompt(trade), "reason": reason})
                except Exception:
                    pass
            except Exception as exc:
                self.log_error("campaign prune close", exc)
        return closed

    def campaign_allows_entry(
        self,
        campaign_key: str,
        campaign: Dict[str, Any],
        sig: Dict[str, Any],
        open_trades: List[Dict[str, Any]],
        account_summary: Dict[str, Any],
        risk_pct: float,
    ) -> Tuple[bool, str, str, float]:
        """Return (allowed, attempt_type, reason, adjusted_risk_pct)."""
        if not cfg_bool("FOREX_TECH_CAMPAIGN_CONTROL_ENABLED", default=True):
            return True, "entry", "campaign control disabled", risk_pct
        inst = normalize_instrument(sig.get("instrument"))
        direction = str(sig.get("direction", "")).upper()
        same = self.matching_open_trades(open_trades, inst, direction)
        opposite = self.opposite_open_trades(open_trades, inst, direction)
        margin_pct = safe_float(account_summary.get("margin_used_pct_of_nav"), 0.0)
        soft_cap = cfg_float("FOREX_TECH_CAMPAIGN_MARGIN_SOFT_CAP_PCT", default=60.0)
        hard_cap = cfg_float("FOREX_TECH_CAMPAIGN_MARGIN_HARD_CAP_PCT", default=70.0)
        max_entries = max(1, cfg_int("FOREX_TECH_CAMPAIGN_MAX_ENTRIES", default=3))
        max_adds = max(0, cfg_int("FOREX_TECH_CAMPAIGN_MAX_ADDS", default=2))
        min_attempt_gap = max(0, cfg_int("FOREX_TECH_CAMPAIGN_MIN_SECONDS_BETWEEN_ATTEMPTS", default=180))
        min_add_gap = max(0, cfg_int("FOREX_TECH_CAMPAIGN_MIN_SECONDS_BETWEEN_ADDS", default=300))
        max_entry_risk = cfg_float("FOREX_TECH_CAMPAIGN_MAX_ENTRY_RISK_PCT", default=0.35)
        add_min_pl = cfg_float("FOREX_TECH_CAMPAIGN_ADD_MIN_UNREALIZED_USD", default=0.08)
        add_mult = cfg_float("FOREX_TECH_CAMPAIGN_ADD_RISK_MULTIPLIER", default=0.45)
        max_same_pair_trades = max(1, cfg_int("FOREX_TECH_CAMPAIGN_MAX_SAME_PAIR_TRADES", default=2))
        same_pair_total = len(same) + len(opposite)
        if same_pair_total >= max_same_pair_trades and not same:
            return False, "blocked_same_pair_capacity", f"same-pair trade capacity reached for {inst}: {same_pair_total}>={max_same_pair_trades}", 0.0

        if opposite:
            # v5.31: allow a controlled same-pair rotation only when the new signal is strong
            # and the opposite trade is already losing. This prevents the old permanent
            # "opposite trade open" block while still avoiding noisy flip-churn.
            allow_rotation = cfg_bool("FOREX_TECH_CAMPAIGN_ALLOW_OPPOSITE_ROTATION", default=True)
            min_score = cfg_float("FOREX_TECH_CAMPAIGN_ROTATE_OPPOSITE_MIN_SCORE", default=82.0)
            loss_needed = abs(cfg_float("FOREX_TECH_CAMPAIGN_ROTATE_OPPOSITE_IF_LOSS_USD", default=0.02))
            sig_score = max(safe_float(sig.get("scout_ev_score"), 0.0), safe_float(sig.get("pressure_score"), 0.0))
            worst_opp_pl = min([self.trade_unrealized_pl(t) for t in opposite] or [0.0])
            if allow_rotation and sig_score >= min_score and worst_opp_pl <= -loss_needed and margin_pct < hard_cap:
                closed_any = False
                for ot in opposite:
                    if self.trade_unrealized_pl(ot) <= -loss_needed:
                        action = {
                            "instrument": inst,
                            "trade_id": str(ot.get("id", "")),
                            "action": "CLOSE",
                            "direction": self.open_trade_direction(ot),
                            "reason": f"campaign rotation: close losing opposite before {direction} signal score={sig_score:.1f} pl={self.trade_unrealized_pl(ot):.4f}",
                        }
                        try:
                            st = self.close_trade(ot, action)
                            closed_any = closed_any or str(st).startswith("accepted") or st == "dry_run"
                        except Exception as exc:
                            self.log_error("campaign opposite rotation close", exc)
                if closed_any:
                    return True, "rotation_entry", f"closed losing opposite {inst}; new signal score={sig_score:.1f} worst_opp_pl={worst_opp_pl:.4f}", max(0.0, min(risk_pct * 0.65, max_entry_risk))
            return False, "blocked_opposite", f"opposite {inst} trade already open; rotation requires score>={min_score:.1f} and opposite loss>={loss_needed:.4f}; score={sig_score:.1f} worst_opp_pl={worst_opp_pl:.4f}", 0.0
        if margin_pct >= hard_cap:
            return False, "blocked_margin_hard", f"margin_used_pct {margin_pct:.2f} >= hard cap {hard_cap:.2f}", 0.0
        if self.campaign_seconds_since(campaign.get("last_attempt_utc")) < min_attempt_gap:
            wait = min_attempt_gap - self.campaign_seconds_since(campaign.get("last_attempt_utc"))
            return False, "blocked_campaign_cooldown", f"campaign cooldown active; wait {wait:.0f}s", 0.0
        entries = safe_int(campaign.get("entries"), 0)
        adds = safe_int(campaign.get("adds"), 0)
        if same:
            current_pl = sum(self.trade_unrealized_pl(t) for t in same)
            if not cfg_bool("FOREX_TECH_PYRAMID_ENABLED", default=True):
                return False, "blocked_duplicate", "same instrument/direction already open and pyramiding disabled", 0.0
            if adds >= max_adds or entries >= max_entries:
                return False, "blocked_max_adds", f"campaign max adds/entries reached adds={adds} entries={entries}", 0.0
            if margin_pct >= soft_cap:
                return False, "blocked_margin_soft_add", f"margin_used_pct {margin_pct:.2f} >= soft cap {soft_cap:.2f}; no adds", 0.0
            if current_pl < add_min_pl:
                return False, "blocked_not_profitable", f"existing campaign PL {current_pl:.4f} < add threshold {add_min_pl:.4f}", 0.0
            if self.campaign_seconds_since(campaign.get("last_add_utc")) < min_add_gap:
                wait = min_add_gap - self.campaign_seconds_since(campaign.get("last_add_utc"))
                return False, "blocked_add_cooldown", f"add cooldown active; wait {wait:.0f}s", 0.0
            return True, "compound_add", f"campaign winner protected enough to add; current_pl={current_pl:.4f}", max(0.0, min(risk_pct * add_mult, max_entry_risk))
        else:
            if entries >= max_entries:
                return False, "blocked_campaign_full", f"campaign entries already {entries} >= {max_entries}", 0.0
            if margin_pct >= soft_cap:
                return False, "blocked_margin_soft_entry", f"margin_used_pct {margin_pct:.2f} >= soft cap {soft_cap:.2f}; no fresh campaign", 0.0
            return True, "first_entry", "fresh campaign entry allowed", max(0.0, min(risk_pct, max_entry_risk))

    def attempt_event_scout_trades(self, theme: str, signals: List[Dict[str, Any]], prices: Dict[str, Dict[str, Any]]) -> int:
        if not self.cfg.event_scout_trades_enabled:
            return 0
        if self.volatile_weekend_new_entries_blocked():
            return 0
        if self.shutdown_mode_active() and self.cfg.shutdown_block_new_trades:
            return 0
        try:
            open_trades = self.oanda.get_open_trades()
        except Exception:
            open_trades = []
        try:
            account_raw = self.oanda.get_account_summary()
            account = account_summary_for_prompt(account_raw)
        except Exception:
            account = {"margin_used_pct_of_nav": 0.0, "nav": 0.0}

        state, campaigns = self.campaign_state_from_runtime()
        regime_lock = self.update_scout_regime_lock(theme, signals, state)
        self.log_position_health_snapshot(open_trades, campaigns, state)
        try:
            pruned = self.run_campaign_prune_pass(account, open_trades, state, reason="event_scan", regime_lock=regime_lock)
            if pruned:
                open_trades = self.oanda.get_open_trades()
                account_raw = self.oanda.get_account_summary()
                account = account_summary_for_prompt(account_raw)
        except Exception as exc:
            self.log_error("campaign prune from event scan", exc)

        # Promote strongest-vs-weakest crosses, but campaign control below decides
        # whether those signals become a fresh entry, a protected add, or just an update.
        synthetic_signals: List[Dict[str, Any]] = []
        if not cfg_bool("FOREX_TECH_PRE_SPIKE_PRESSURE_ONLY", default=False):
            synthetic_signals = self.build_currency_strength_cross_signals(theme, signals, prices, open_trades)
        if synthetic_signals:
            log(f"{self.lane_prefix()}[STRENGTH] promoted {len(synthetic_signals)} synthetic strongest-vs-weakest cross candidate(s) for theme={theme}")
            signals = list(signals) + synthetic_signals

        attempts = 0
        total_risk = 0.0
        ranked: List[Tuple[float, float, float, Dict[str, Any], float, str, str, str]] = []
        basket_count = len({normalize_instrument(x.get("instrument")) for x in signals})
        ev_repeat_count = max(1, cfg_int("FOREX_TECH_CAMPAIGN_EV_REPEAT_COUNT", default=1))

        for sig in signals:
            inst = normalize_instrument(sig.get("instrument"))
            direction = str(sig.get("direction", "")).upper()
            if not inst or direction not in {"LONG", "SHORT"}:
                continue
            ckey = self.campaign_key_for_signal(theme, sig)
            campaign = campaigns.get(ckey, {}) if isinstance(campaigns.get(ckey, {}), dict) else {}
            active_lock = self.active_scout_regime_lock(state) or regime_lock
            blocked_by_regime, why_regime, regime_alignment, regime_edge = self.scout_regime_blocks_signal(sig, active_lock)
            sig["_scout_regime_lock_label"] = str(active_lock.get("label", "")) if isinstance(active_lock, dict) else ""
            sig["_scout_regime_alignment"] = regime_alignment
            sig["_scout_regime_edge"] = round(regime_edge, 4)
            if blocked_by_regime:
                sig_for_regime_log = self.enrich_scout_signal_value_weight(dict(sig), prices, account)
                why_regime_log = why_regime
                expected_usd = safe_float(sig_for_regime_log.get("_value_weighted_expected_usd"), 0.0)
                return_pct = safe_float(sig_for_regime_log.get("_value_weighted_return_pct"), 0.0)
                net_after_spread = safe_float(sig_for_regime_log.get("_value_weighted_net_after_spread_pips"), 0.0)
                if expected_usd > 0.0 or net_after_spread > 0.0:
                    why_regime_log = (
                        f"{why_regime}; audit[value=${expected_usd:.4f} "
                        f"return={return_pct:.3f}% net_after_spread={net_after_spread:.1f}p]"
                    )
                campaign["skipped"] = safe_int(campaign.get("skipped"), 0) + 1
                campaign.update({"instrument": inst, "direction": direction, "theme": self.normalize_campaign_theme(theme), "status": "blocked", "last_signal_utc": iso_utc(), "last_skip_reason": why_regime_log})
                campaigns[ckey] = campaign
                self.log_action({"instrument": inst, "direction": direction, "action": "OPEN", "risk_pct": self.cfg.event_scout_risk_pct, "reason": f"event scout {theme}"}, "event_scout", "skipped", reject_reason=why_regime_log)
                self.log_scout_audit_signal(theme, sig_for_regime_log, "regime_lock", "skipped", reason=why_regime_log, reject_reason=why_regime_log, risk_pct=self.cfg.event_scout_risk_pct)
                self.log_movement_campaign(ckey, campaign, "skip_regime_lock", why_regime_log, sig=sig_for_regime_log, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
                continue
            allowed, perm_risk, why_perm = self.permission_allows_signal(theme, sig)
            if not allowed:
                campaign["skipped"] = safe_int(campaign.get("skipped"), 0) + 1
                campaigns[ckey] = {**campaign, "instrument": inst, "direction": direction, "theme": self.normalize_campaign_theme(theme), "status": "blocked"}
                self.log_action({"instrument": inst, "direction": direction, "action": "OPEN", "risk_pct": self.cfg.event_scout_risk_pct, "reason": f"event scout {theme}"}, "event_scout", "skipped", reject_reason=why_perm)
                self.log_scout_audit_signal(theme, sig, "permission_gate", "skipped", reason=why_perm, reject_reason=why_perm, risk_pct=self.cfg.event_scout_risk_pct)
                self.log_movement_campaign(ckey, campaigns[ckey], "skip_permission", why_perm, sig=sig, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
                continue

            # v5.30: campaign controller owns repeats. Give the EV scorer a capped
            # repeat count so valid continuing campaigns are not blocked by old hard repeat caps.
            sig_for_score = dict(sig)
            sig_for_score["event_trigger_count"] = min(safe_int(sig_for_score.get("event_trigger_count"), 1), ev_repeat_count)
            sig_for_score = self.enrich_scout_signal_value_weight(sig_for_score, prices, account)
            if sig_for_score.get("_synthetic_strength_cross"):
                ev_score = safe_float(sig_for_score.get("scout_ev_score"), 0.0)
                ev_risk = safe_float(sig_for_score.get("_risk_pct_override"), self.cfg.event_scout_risk_pct)
                ok_score = ev_score >= cfg_float("FOREX_SCOUT_MIN_EV_SCORE_TO_TRADE", default=72.0) - 4.0
                why_score = f"synthetic_strength_cross score={ev_score:.1f} gap={safe_float(sig_for_score.get('_synthetic_strength_gap'), 0.0):+.2f} overstack={safe_int(sig_for_score.get('_overstack_count'), 0)} sources={','.join(sig_for_score.get('_synthetic_source_instruments', [])[:8])}"
            else:
                ok_score, ev_risk, ev_score, why_score = self.scout_ev_score_and_risk(theme, sig_for_score, basket_count)
            if not ok_score:
                campaign["skipped"] = safe_int(campaign.get("skipped"), 0) + 1
                campaigns[ckey] = {**campaign, "instrument": inst, "direction": direction, "theme": self.normalize_campaign_theme(theme), "status": "watch"}
                if not (cfg_bool("FOREX_TECH_SUPPRESS_REPEAT_CAP_LOGS", default=True) and "repeat trigger cap hit" in str(why_score)):
                    self.log_action({"instrument": inst, "direction": direction, "action": "OPEN", "risk_pct": ev_risk or self.cfg.event_scout_risk_pct, "reason": f"event scout {theme}"}, "event_scout", "skipped", reject_reason=why_score)
                    self.log_scout_audit_signal(theme, sig_for_score, "score_gate", "skipped", reason=why_score, reject_reason=why_score, risk_pct=ev_risk or self.cfg.event_scout_risk_pct)
                self.log_movement_campaign(ckey, campaigns[ckey], "skip_score", why_score, sig=sig_for_score, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
                continue

            broad_allowed, why_broad = self.scout_broad_regime_trade_gate(sig_for_score, active_lock, ev_score)
            if not broad_allowed:
                campaign["skipped"] = safe_int(campaign.get("skipped"), 0) + 1
                campaign.update({"instrument": inst, "direction": direction, "theme": self.normalize_campaign_theme(theme), "status": "watch", "last_signal_utc": iso_utc(), "last_skip_reason": why_broad})
                campaigns[ckey] = campaign
                self.log_action({"instrument": inst, "direction": direction, "action": "OPEN", "risk_pct": ev_risk or self.cfg.event_scout_risk_pct, "reason": f"event scout {theme}"}, "event_scout", "skipped", reject_reason=why_broad)
                self.log_scout_audit_signal(theme, sig_for_score, "broad_regime_gate", "skipped", reason=why_broad, reject_reason=why_broad, risk_pct=ev_risk or self.cfg.event_scout_risk_pct)
                self.log_movement_campaign(ckey, campaign, "skip_broad_regime_gate", why_broad, sig=sig_for_score, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
                continue

            risk_pct = min(ev_risk, perm_risk if self.cfg.event_require_research_permission else ev_risk)
            if cfg_bool("FOREX_SCOUT_BROAD_REGIME_TRADE_ONLY", default=False):
                why_score = f"{why_score}; {why_broad}"
            if sig_for_score.get("_drastic_move_override"):
                risk_pct *= max(0.05, min(1.0, cfg_float("FOREX_TECH_DRASTIC_MOVE_RISK_MULTIPLIER", default=0.50)))
            sig_for_score["scout_ev_score"] = ev_score
            allowed_campaign, attempt_type, why_campaign, adj_risk = self.campaign_allows_entry(ckey, campaign, sig_for_score, open_trades, account, risk_pct)
            current_pl = sum(self.trade_unrealized_pl(t) for t in self.matching_open_trades(open_trades, inst, direction))
            self.log_compound_attempt(ckey, sig_for_score, allowed_campaign, attempt_type, adj_risk, why_campaign, current_unrealized_pl=current_pl, margin_used_pct=safe_float(account.get("margin_used_pct_of_nav"), 0.0), campaign=campaign)
            if not allowed_campaign:
                campaign["skipped"] = safe_int(campaign.get("skipped"), 0) + 1
                campaign.update({"instrument": inst, "direction": direction, "theme": self.normalize_campaign_theme(theme), "status": "watch", "last_signal_utc": iso_utc(), "last_skip_reason": why_campaign})
                campaigns[ckey] = campaign
                self.log_action({"instrument": inst, "direction": direction, "action": "OPEN", "risk_pct": adj_risk or risk_pct, "reason": f"campaign {attempt_type} {theme}"}, "event_scout", "skipped", reject_reason=why_campaign)
                self.log_scout_audit_signal(theme, sig_for_score, "campaign_gate", "skipped", reason=why_campaign, reject_reason=why_campaign, risk_pct=adj_risk or risk_pct)
                self.log_movement_campaign(ckey, campaign, "skip_campaign", why_campaign, sig=sig_for_score, margin_used_pct=account.get("margin_used_pct_of_nav", ""), current_unrealized_pl=current_pl)
                continue
            if adj_risk <= 0:
                continue
            sig2 = dict(sig_for_score)
            sig2["_campaign_key"] = ckey
            sig2["_campaign_attempt_type"] = attempt_type
            if cfg_bool("FOREX_SCOUT_VALUE_WEIGHTED_ENABLED", default=False):
                rank_primary = safe_float(sig2.get("_value_weighted_return_pct"), 0.0)
                rank_secondary = safe_float(sig2.get("_value_weighted_expected_usd"), 0.0)
            else:
                rank_primary = ev_score
                rank_secondary = abs(safe_float(sig2.get("net_pips"), 0.0))
            ranked.append((rank_primary, rank_secondary, ev_score, sig2, adj_risk, f"{why_perm}; {why_score}; {why_campaign}", ckey, attempt_type))

        max_attempts = max(0, self.cfg.event_max_scout_trades_per_event)
        for _rank_primary, _rank_secondary, ev_score, sig, risk_pct, why, ckey, attempt_type in sorted(
            ranked,
            key=lambda x: (x[0], x[1], x[2], abs(safe_float(x[3].get("net_pips"), 0.0))),
            reverse=True,
        ):
            if attempts >= max_attempts:
                break
            if total_risk + risk_pct > self.cfg.event_max_total_scout_risk_pct:
                self.log_movement_campaign(ckey, campaigns.get(ckey, {}), "skip_scan_risk_cap", f"scan risk cap {total_risk + risk_pct:.2f}>{self.cfg.event_max_total_scout_risk_pct:.2f}", sig=sig, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
                self.log_scout_audit_signal(theme, sig, "scan_risk_cap", "skipped", reason=f"scan risk cap {total_risk + risk_pct:.2f}>{self.cfg.event_max_total_scout_risk_pct:.2f}", reject_reason=f"scan risk cap {total_risk + risk_pct:.2f}>{self.cfg.event_max_total_scout_risk_pct:.2f}", risk_pct=risk_pct)
                break
            inst = normalize_instrument(sig.get("instrument"))
            direction = str(sig.get("direction", "")).upper()
            order = self.build_event_scout_order(theme, sig, prices, risk_pct, why)
            if not order:
                self.log_scout_audit_signal(theme, sig, "order_build", "skipped", reason=why, reject_reason="order build returned no order", risk_pct=risk_pct)
                continue
            order["_campaign_key"] = ckey
            order["_campaign_attempt_type"] = attempt_type
            if isinstance(order.get("_scout_audit"), dict):
                order["_scout_audit"]["_campaign_key"] = ckey
                order["_scout_audit"]["_campaign_attempt_type"] = attempt_type
            status = self.open_trade_from_order(order, prices)
            attempts += 1
            campaign = campaigns.get(ckey, {}) if isinstance(campaigns.get(ckey, {}), dict) else {}
            campaign.update({
                "instrument": inst,
                "direction": direction,
                "theme": self.normalize_campaign_theme(theme),
                "status": "active" if (status.startswith("accepted") or status == "dry_run") else "watch",
                "last_attempt_utc": iso_utc(),
                "last_signal_utc": iso_utc(),
                "last_status": status,
                "movement_key": sig.get("_movement_key", self.market_movement_key(sig)),
                "attempts": safe_int(campaign.get("attempts"), 0) + 1,
            })
            if status.startswith("accepted") or status == "dry_run":
                campaign["accepted"] = safe_int(campaign.get("accepted"), 0) + 1
                campaign["entries"] = safe_int(campaign.get("entries"), 0) + 1
                if attempt_type == "compound_add":
                    campaign["adds"] = safe_int(campaign.get("adds"), 0) + 1
                    campaign["last_add_utc"] = iso_utc()
                total_risk += risk_pct
                self.log_movement_campaign(ckey, campaign, "accepted_" + attempt_type, why, sig=sig, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
            else:
                campaign["skipped"] = safe_int(campaign.get("skipped"), 0) + 1
                self.log_movement_campaign(ckey, campaign, "order_" + status, why, sig=sig, margin_used_pct=account.get("margin_used_pct_of_nav", ""))
            campaigns[ckey] = campaign
        self.save_campaign_state(state, campaigns)
        return attempts

    def run_event_scan(self, reason: str = "interval") -> None:
        if not self.cfg.event_scanner_enabled or fx_market_closed() or self.sunday_reopen_event_scanner_blocked() or self.volatile_weekend_new_entries_blocked():
            return
        if not self.instruments:
            self.refresh_instruments()
        stats: Dict[str, Any] = {
            "scanned": 0,
            "tradeable": 0,
            "not_tradeable": 0,
            "spread_rejected": 0,
            "candle_errors": 0,
            "no_windows": 0,
            "below_pips": 0,
            "below_ratio": 0,
            "candidate_windows": 0,
            "signals": 0,
            "themes": 0,
            "watch_basket": 0,
            "triggered_themes": 0,
            "pressure_watch": 0,
            "pressure_trade": 0,
            "exhaustion_shadow": 0,
            "exhaustion_hybrid": 0,
            "scout_attempts": 0,
        }
        rejected: List[Dict[str, Any]] = []
        top_candidates: List[Dict[str, Any]] = []
        prices = self.oanda.get_prices(self.instruments)
        all_signals: List[Dict[str, Any]] = []
        pressure_watch: List[Dict[str, Any]] = []
        pressure_trade_signals: List[Dict[str, Any]] = []
        exhaustion_snapshots: Dict[str, Dict[str, Any]] = {}
        pressure_trade_score = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_TRADE_SCORE", default=78.0)
        pressure_min_ratio = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_RATIO", default=0.80)
        pressure_min_pips_mult = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_PIPS_MULTIPLIER", default=0.35)
        pressure_max_scouts = cfg_int("FOREX_TECH_PRE_SPIKE_PRESSURE_MAX_SCOUTS_PER_SCAN", default=3)
        pressure_strict_gate = cfg_bool("FOREX_TECH_PRE_SPIKE_PRESSURE_STRICT_GATE_ENABLED", default=False)
        pressure_min_cluster_instruments = cfg_int("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_CLUSTER_INSTRUMENTS", default=0)
        pressure_min_direction_cluster_instruments = cfg_int("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_DIRECTION_CLUSTER_INSTRUMENTS", default=0)
        pressure_min_signal_value_usd = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SIGNAL_VALUE_USD", default=0.0)
        pressure_min_signal_return_pct = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SIGNAL_RETURN_PCT", default=0.0)
        pressure_require_value_estimate = cfg_bool("FOREX_TECH_PRE_SPIKE_PRESSURE_REQUIRE_VALUE_ESTIMATE", default=False)
        for inst in self.instruments:
            stats["scanned"] += 1
            meta = self.instrument_meta.get(inst)
            if not meta:
                stats["not_tradeable"] += 1
                continue
            price = prices.get(inst, {})
            if not price or not price_tradeable(price):
                stats["not_tradeable"] += 1
                continue
            stats["tradeable"] += 1
            spr_now = spread_pips(price, meta)
            max_spread = self.event_max_spread_pips(inst)
            if spr_now > max_spread:
                stats["spread_rejected"] += 1
                rejected.append({"instrument": inst, "reason": f"spread {spr_now:.1f}>{max_spread:.1f}", "spread_pips": spr_now})
                continue
            try:
                candles = self.oanda.get_candles(inst, self.cfg.event_candle_granularity, self.cfg.event_candle_count, price="BAM")
            except Exception as exc:
                stats["candle_errors"] += 1
                self.log_error(f"event candles {inst}", exc)
                continue
            exhaustion = self.technical_exhaustion_feature_snapshot(inst, candles)
            if exhaustion:
                exhaustion_snapshots[inst] = exhaustion
            pressure = self.technical_pressure_signal(inst, candles)
            if pressure:
                pressure_watch.append(pressure)
            windows = self.best_recent_event_windows(inst, candles)
            if not windows:
                stats["no_windows"] += 1
                continue
            best = max(windows, key=lambda x: abs(safe_float(x.get("net_pips"), -1e9)))
            threshold = self.event_threshold_pips(inst)
            net_pips = abs(safe_float(best.get("net_pips", best.get("abs_move_pips", 0.0)), 0.0))
            ratio = max(0.0, safe_float(best.get("move_to_spread_ratio"), 0.0))
            min_ratio = self.cfg.scout_volatile_min_move_to_spread_ratio if self.is_volatile_scout_pair(inst) else self.cfg.event_min_move_to_spread_ratio
            drastic_enabled = cfg_bool("FOREX_TECH_DRASTIC_MOVE_CAPTURE_ENABLED", default=True)
            drastic_mult = max(1.0, cfg_float("FOREX_TECH_DRASTIC_MOVE_THRESHOLD_MULTIPLIER", default=2.0))
            drastic_min_ratio = max(0.1, cfg_float("FOREX_TECH_DRASTIC_MOVE_MIN_RATIO", default=0.75))
            drastic_override = bool(drastic_enabled and net_pips >= threshold * drastic_mult and ratio >= drastic_min_ratio)
            best_for_log = dict(best)
            best_for_log["net_pips"] = net_pips
            best_for_log["abs_move_pips"] = net_pips
            best_for_log["threshold_pips"] = threshold
            best_for_log["min_ratio"] = min_ratio
            best_for_log["drastic_override"] = drastic_override
            if drastic_override:
                best_for_log["_drastic_move_override"] = True
                best_for_log["_drastic_min_ratio"] = drastic_min_ratio
                best_for_log["_drastic_threshold_pips"] = threshold * drastic_mult
            if net_pips < threshold:
                stats["below_pips"] += 1
                best_for_log["reason"] = f"below_pips {net_pips:.1f}<{threshold:.1f}"
                rejected.append(best_for_log)
                continue
            if ratio < min_ratio and not drastic_override:
                stats["below_ratio"] += 1
                best_for_log["reason"] = f"below_ratio {ratio:.2f}<{min_ratio:.2f}"
                rejected.append(best_for_log)
                continue
            if drastic_override:
                best_for_log["reason"] = f"drastic_move_override abs={net_pips:.1f}p ratio={ratio:.2f}>={drastic_min_ratio:.2f}"
            stats["candidate_windows"] += 1
            top_candidates.append(best_for_log)
            for theme in self.classify_event_signal_themes(best):
                best2 = dict(best)
                best2["theme"] = theme
                all_signals.append(best2)
                self.log_event_window(best2, theme)
        exhaustion_shadow = self.technical_exhaustion_shadow_signals(
            exhaustion_snapshots
        )
        stats["exhaustion_shadow"] = self.log_exhaustion_shadow_watchlist(
            exhaustion_shadow
        )
        exhaustion_hybrid = self.exhaustion_hybrid_scout_signals(
            exhaustion_shadow,
            prices,
        )
        stats["exhaustion_hybrid"] = len(exhaustion_hybrid)
        for hybrid in exhaustion_hybrid:
            all_signals.append(hybrid)
            self.log_event_window(
                hybrid,
                str(hybrid.get("theme", "PRE_SPIKE_EXHAUSTION_REVERSAL")),
            )
        # Pre-spike pressure candidates can enter earlier than the hard spike threshold,
        # but only when pressure score, spread ratio, and minimum pips all clear.
        pressure_watch.sort(key=lambda x: safe_float(x.get("pressure_score"), 0.0), reverse=True)
        pressure_cluster_instruments = len(
            {
                normalize_instrument(row.get("instrument", ""))
                for row in pressure_watch
                if normalize_instrument(row.get("instrument", ""))
            }
        )
        pressure_cluster_pairs_dirs = len(
            {
                (
                    normalize_instrument(row.get("instrument", "")),
                    str(row.get("direction", "")).upper(),
                )
                for row in pressure_watch
                if normalize_instrument(row.get("instrument", ""))
                and str(row.get("direction", "")).upper() in {"LONG", "SHORT"}
            }
        )
        pressure_direction_instruments: Dict[str, int] = {}
        for row in pressure_watch:
            direction_key = str(row.get("direction", "")).upper()
            if direction_key not in {"LONG", "SHORT"}:
                continue
            pressure_direction_instruments.setdefault(direction_key, 0)
        for direction_key in list(pressure_direction_instruments):
            pressure_direction_instruments[direction_key] = len(
                {
                    normalize_instrument(row.get("instrument", ""))
                    for row in pressure_watch
                    if str(row.get("direction", "")).upper() == direction_key
                    and normalize_instrument(row.get("instrument", ""))
                }
            )
        localized_shadow_enabled = cfg_bool(
            "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_SHADOW_ENABLED",
            default=False,
        )
        localized_focus_raw = cfg_str(
            "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_CURRENCIES",
            default="ZAR,TRY,MXN,NOK,SEK,CZK,HKD,THB,CNH,PLN,HUF",
        )
        localized_focus_currencies = {
            part.strip().upper()
            for part in re.split(r"[,;|\s]+", localized_focus_raw)
            if part.strip()
        }
        localized_min_cluster = cfg_int(
            "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_INSTRUMENTS",
            default=4,
        )
        localized_min_direction_cluster = cfg_int(
            "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_DIRECTION_INSTRUMENTS",
            default=4,
        )
        localized_min_ratio = cfg_float(
            "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_RATIO",
            default=1.35,
        )
        localized_min_score = cfg_float(
            "FOREX_TECH_PRE_SPIKE_LOCALIZED_CLUSTER_MIN_SCORE",
            default=80.0,
        )
        localized_currency_instruments: Dict[str, set[str]] = {}
        localized_currency_direction_instruments: Dict[Tuple[str, str], set[str]] = {}
        if localized_shadow_enabled and localized_focus_currencies:
            for row in pressure_watch:
                instrument = normalize_instrument(row.get("instrument", ""))
                if "_" not in instrument:
                    continue
                base, quote = instrument.split("_", 1)
                direction_key = str(row.get("direction", "")).upper()
                for currency in {base, quote} & localized_focus_currencies:
                    localized_currency_instruments.setdefault(currency, set()).add(
                        instrument,
                    )
                    if direction_key in {"LONG", "SHORT"}:
                        localized_currency_direction_instruments.setdefault(
                            (currency, direction_key),
                            set(),
                        ).add(instrument)

        def localized_cluster_shadow_reason(
            candidate: Dict[str, Any],
            broad_reject_reason: str,
        ) -> str:
            if not localized_shadow_enabled:
                return ""
            instrument = normalize_instrument(candidate.get("instrument", ""))
            if "_" not in instrument:
                return ""
            direction_key = str(candidate.get("direction", "")).upper()
            if direction_key not in {"LONG", "SHORT"}:
                return ""
            score_now = safe_float(candidate.get("pressure_score"), 0.0)
            ratio_now = safe_float(candidate.get("move_to_spread_ratio"), 0.0)
            if score_now < localized_min_score or ratio_now < localized_min_ratio:
                return ""
            base, quote = instrument.split("_", 1)
            best_currency = ""
            best_cluster = 0
            best_direction_cluster = 0
            for currency in {base, quote} & localized_focus_currencies:
                cluster_count = len(localized_currency_instruments.get(currency, set()))
                direction_count = len(
                    localized_currency_direction_instruments.get(
                        (currency, direction_key),
                        set(),
                    )
                )
                if (direction_count, cluster_count) > (
                    best_direction_cluster,
                    best_cluster,
                ):
                    best_currency = currency
                    best_cluster = cluster_count
                    best_direction_cluster = direction_count
            if (
                best_currency
                and best_cluster >= max(0, localized_min_cluster)
                and best_direction_cluster >= max(0, localized_min_direction_cluster)
            ):
                candidate["_localized_cluster_shadow"] = True
                candidate["_localized_cluster_currency"] = best_currency
                candidate["_localized_cluster_instruments"] = best_cluster
                candidate["_localized_direction_cluster_instruments"] = (
                    best_direction_cluster
                )
                return (
                    "localized cluster shadow candidate: "
                    f"{best_currency} cluster={best_cluster} "
                    f"dir_cluster={best_direction_cluster} "
                    f"score={score_now:.1f} ratio={ratio_now:.2f}; "
                    f"broad_gate={broad_reject_reason}"
                )
            return ""

        pressure_value_account: Dict[str, Any] = {}
        if pressure_strict_gate and (
            pressure_min_signal_value_usd > 0.0
            or pressure_min_signal_return_pct > 0.0
            or pressure_require_value_estimate
        ):
            try:
                pressure_value_account = account_summary_for_prompt(
                    self.oanda.get_account_summary()
                )
            except Exception as exc:
                self.log_error("pre-spike pressure value account summary", exc)
                pressure_value_account = {}
        if pressure_watch:
            stats["pressure_watch"] = len(pressure_watch)
            self.log_pressure_watchlist(pressure_watch, reason=reason)
        for pressure in pressure_watch:
            threshold = safe_float(pressure.get("threshold_pips"), self.event_threshold_pips(normalize_instrument(pressure.get("instrument", ""))))
            net_pips = safe_float(pressure.get("net_pips"), 0.0)
            ratio = safe_float(pressure.get("move_to_spread_ratio"), 0.0)
            score = safe_float(pressure.get("pressure_score"), 0.0)
            if len(pressure_trade_signals) >= max(0, pressure_max_scouts):
                break
            if score >= pressure_trade_score and ratio >= pressure_min_ratio and net_pips >= threshold * pressure_min_pips_mult:
                p = dict(pressure)
                p["_pressure_cluster_instruments"] = pressure_cluster_instruments
                p["_pressure_cluster_pairs_dirs"] = pressure_cluster_pairs_dirs
                p["_pressure_cluster_direction_instruments"] = pressure_direction_instruments.get(
                    str(p.get("direction", "")).upper(),
                    0,
                )
                if pressure_strict_gate:
                    if pressure_cluster_instruments < max(0, pressure_min_cluster_instruments):
                        p["reason"] = (
                            "pre_spike_pressure strict gate rejected: "
                            f"cluster instruments {pressure_cluster_instruments} < "
                            f"{pressure_min_cluster_instruments}"
                        )
                        rejected.append(p)
                        localized_reason = localized_cluster_shadow_reason(
                            p,
                            p["reason"],
                        )
                        if localized_reason:
                            self.log_scout_audit_signal(
                                "PRE_SPIKE_LOCALIZED_CLUSTER_SHADOW",
                                p,
                                "localized_cluster_shadow",
                                "shadow",
                                reason=localized_reason,
                                reject_reason=p["reason"],
                            )
                        self.log_scout_audit_signal("PRE_SPIKE_PRESSURE", p, "strict_pressure_gate", "skipped", reason=p["reason"], reject_reason=p["reason"])
                        continue
                    pressure_direction_cluster = safe_int(
                        p.get("_pressure_cluster_direction_instruments"),
                        0,
                    )
                    if pressure_direction_cluster < max(0, pressure_min_direction_cluster_instruments):
                        p["reason"] = (
                            "pre_spike_pressure strict gate rejected: "
                            f"direction cluster instruments {pressure_direction_cluster} < "
                            f"{pressure_min_direction_cluster_instruments}"
                        )
                        rejected.append(p)
                        localized_reason = localized_cluster_shadow_reason(
                            p,
                            p["reason"],
                        )
                        if localized_reason:
                            self.log_scout_audit_signal(
                                "PRE_SPIKE_LOCALIZED_CLUSTER_SHADOW",
                                p,
                                "localized_cluster_shadow",
                                "shadow",
                                reason=localized_reason,
                                reject_reason=p["reason"],
                            )
                        self.log_scout_audit_signal("PRE_SPIKE_PRESSURE", p, "strict_pressure_gate", "skipped", reason=p["reason"], reject_reason=p["reason"])
                        continue
                    if (
                        pressure_min_signal_value_usd > 0.0
                        or pressure_min_signal_return_pct > 0.0
                        or pressure_require_value_estimate
                    ):
                        estimate = self.scout_signal_value_estimate(
                            p,
                            prices,
                            pressure_value_account,
                        )
                        p["_value_weighted_estimate"] = estimate
                        if bool(estimate.get("available")):
                            p["_value_weighted_expected_usd"] = safe_float(
                                estimate.get("expected_account_pl"),
                                0.0,
                            )
                            p["_value_weighted_return_pct"] = safe_float(
                                estimate.get("return_pct"),
                                0.0,
                            )
                            p["_value_weighted_net_after_spread_pips"] = safe_float(
                                estimate.get("net_after_spread_pips"),
                                0.0,
                            )
                            p["_value_weighted_density_pct"] = safe_float(
                                estimate.get("density_pct"),
                                0.0,
                            )
                        elif pressure_require_value_estimate:
                            p["reason"] = (
                                "pre_spike_pressure strict gate rejected: "
                                f"value estimate unavailable {estimate.get('reason', 'unknown')}"
                            )
                            rejected.append(p)
                            self.log_scout_audit_signal("PRE_SPIKE_PRESSURE", p, "strict_pressure_gate", "skipped", reason=p["reason"], reject_reason=p["reason"])
                            continue
                        expected_usd = safe_float(
                            p.get("_value_weighted_expected_usd"),
                            0.0,
                        )
                        return_pct = safe_float(
                            p.get("_value_weighted_return_pct"),
                            0.0,
                        )
                        if expected_usd < max(0.0, pressure_min_signal_value_usd):
                            p["reason"] = (
                                "pre_spike_pressure strict gate rejected: "
                                f"signal value ${expected_usd:.4f} < "
                                f"${pressure_min_signal_value_usd:.4f}"
                            )
                            rejected.append(p)
                            self.log_scout_audit_signal("PRE_SPIKE_PRESSURE", p, "strict_pressure_gate", "skipped", reason=p["reason"], reject_reason=p["reason"])
                            continue
                        if return_pct < max(0.0, pressure_min_signal_return_pct):
                            p["reason"] = (
                                "pre_spike_pressure strict gate rejected: "
                                f"signal return {return_pct:.3f}% < "
                                f"{pressure_min_signal_return_pct:.3f}%"
                            )
                            rejected.append(p)
                            self.log_scout_audit_signal("PRE_SPIKE_PRESSURE", p, "strict_pressure_gate", "skipped", reason=p["reason"], reject_reason=p["reason"])
                            continue
                    p["_strict_pre_spike_pressure_gate"] = True
                p["_pre_spike_pressure"] = True
                p["_risk_multiplier"] = cfg_float("FOREX_TECH_PRE_SPIKE_PRESSURE_RISK_MULTIPLIER", default=0.30)
                value_note = ""
                if p.get("_value_weighted_estimate"):
                    value_note = (
                        f" value=${safe_float(p.get('_value_weighted_expected_usd'), 0.0):.4f}"
                        f" return={safe_float(p.get('_value_weighted_return_pct'), 0.0):.3f}%"
                    )
                p["reason"] = (
                    f"pre_spike_pressure score={score:.1f} ratio={ratio:.2f} "
                    f"net={net_pips:.1f} threshold={threshold:.1f} "
                    f"cluster={pressure_cluster_instruments} "
                    f"dir_cluster={p.get('_pressure_cluster_direction_instruments', 0)}"
                    f"{value_note}"
                )
                pressure_stage = "strict_pressure_gate" if pressure_strict_gate else "pressure_gate"
                self.log_scout_audit_signal("PRE_SPIKE_PRESSURE", p, pressure_stage, "passed", reason=p["reason"])
                pressure_trade_signals.append(p)
                for theme in self.classify_event_signal_themes(p):
                    p2 = dict(p)
                    p2["theme"] = "PRE_SPIKE_" + theme
                    all_signals.append(p2)
                    self.log_event_window(p2, p2["theme"])
        stats["pressure_trade"] = len(pressure_trade_signals)

        all_signals = self.enrich_signals_with_major_move_factor(
            all_signals,
            exhaustion_snapshots,
        )
        for signal in all_signals:
            snapshot = exhaustion_snapshots.get(
                normalize_instrument(signal.get("instrument")),
                {},
            )
            if snapshot:
                signal["_technical_feature_snapshot"] = snapshot
        if cfg_bool("FOREX_TECH_PRE_SPIKE_PRESSURE_ONLY", default=False):
            before_pressure_only = len(all_signals)
            all_signals = [
                signal
                for signal in all_signals
                if bool(signal.get("_strict_pre_spike_pressure_gate"))
                and bool(signal.get("_pre_spike_pressure"))
            ]
            stats["pressure_only_filtered"] = before_pressure_only - len(all_signals)
        all_signals = self.update_event_signal_memory(all_signals)
        by_theme: Dict[str, List[Dict[str, Any]]] = {}
        for sig in all_signals:
            by_theme.setdefault(str(sig.get("theme", "")), []).append(sig)
        stats["signals"] = len(all_signals)
        stats["themes"] = len(by_theme)
        triggered_any = False
        production_entries_allowed, production_gate_reason = (
            self.production_model_allows_new_entries()
        )
        if not production_entries_allowed:
            stats["production_gate_blocked"] = len(all_signals)
        for theme, signals in sorted(by_theme.items(), key=lambda kv: len(kv[1]), reverse=True):
            best_by_inst: Dict[str, Dict[str, Any]] = {}
            for sig in signals:
                inst = normalize_instrument(sig.get("instrument"))
                if inst not in best_by_inst or abs(safe_float(sig.get("net_pips"), 0.0)) > abs(safe_float(best_by_inst[inst].get("net_pips"), 0.0)):
                    best_by_inst[inst] = sig
            compact = sorted(best_by_inst.values(), key=lambda x: abs(safe_float(x.get("net_pips"), 0.0)), reverse=True)
            if len(compact) < max(1, self.cfg.event_min_basket_pairs):
                stats["watch_basket"] += 1
                self.log_event_signal(theme, "watch", compact, reason=f"basket below min {self.cfg.event_min_basket_pairs}")
                continue
            if not production_entries_allowed:
                scout_attempts = 0
                self.log_event_signal(
                    theme,
                    "watch",
                    compact,
                    reason=production_gate_reason,
                )
                continue
            approved_compact, rejected_compact = (
                self.filter_signals_with_promoted_model(compact)
            )
            if rejected_compact:
                self.log_event_signal(
                    theme,
                    "watch",
                    rejected_compact[:10],
                    reason="production model rejected signal(s)",
                )
            bypass_compact: List[Dict[str, Any]] = []
            if cfg_bool("FOREX_SCOUT_BROAD_REGIME_ALLOW_MODEL_BYPASS", default=False):
                bypass_compact = rejected_compact
            if not approved_compact:
                if not bypass_compact:
                    self.log_event_signal(
                        theme,
                        "watch",
                        compact,
                        reason="production model approved no signals",
                    )
                    continue
                self.log_event_signal(
                    theme,
                    "watch",
                    bypass_compact[:10],
                    reason="production model approved no signals; broad-regime bypass will evaluate rejected candidates",
                )
            compact = approved_compact + bypass_compact
            scout_attempts = self.attempt_event_scout_trades(theme, compact, prices)
            stats["scout_attempts"] += scout_attempts
            did_trigger = False
            if not triggered_any:
                try:
                    did_trigger = self.maybe_event_trigger_research(theme, compact)
                    triggered_any = did_trigger
                except Exception as exc:
                    self.log_error(f"event-triggered portfolio {theme}", exc)
            stats["triggered_themes"] += 1
            self.log_event_signal(theme, "triggered", compact, triggered_research=did_trigger, scout_attempts=scout_attempts, reason=reason)
        rejected.sort(key=lambda r: abs(safe_float(r.get("net_pips"), 0.0)), reverse=True)
        self.log_event_scan_summary(reason, stats, top_candidates, rejected)
        state = self.load_state()
        state["last_event_scan_utc"] = iso_utc()
        state["last_event_scan_summary"] = dict(stats)
        if all_signals:
            state["last_event_signals"] = all_signals[-100:]
        self.save_state(state)
    def volatile_weekend_new_entries_blocked(self) -> bool:
        return self.cfg.account_lane in {"vol", "tech"} and volatile_weekend_block_new_scouts_now()

    def volatile_weekend_flatten_due(self, state: Optional[Dict[str, Any]] = None, *, force: bool = False) -> bool:
        if self.cfg.account_lane not in {"vol", "tech"}:
            return False
        if force:
            return True
        if not volatile_weekend_flatten_window_now():
            return False
        state = state or self.load_state()
        last_raw = state.get("last_volatile_weekend_flatten_utc")
        if not last_raw:
            return True
        try:
            last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(UTC)
            repeat_minutes = max(1, cfg_int("FOREX_VOLATILE_WEEKEND_CLOSE_REPEAT_MINUTES", default=10))
            return (utc_now() - last).total_seconds() >= repeat_minutes * 60
        except Exception:
            return True

    def run_volatile_weekend_flatten_pass(self, reason: str = "volatile_weekend_flatten", *, force: bool = False) -> int:
        """Close every open trade in the volatile account before/weekend gap."""
        if self.cfg.account_lane not in {"vol", "tech"}:
            return 0
        state = self.load_state()
        if not self.volatile_weekend_flatten_due(state, force=force):
            return 0
        try:
            open_trades = self.oanda.get_open_trades()
        except Exception as exc:
            self.log_error("volatile weekend flatten get_open_trades", exc)
            return 0
        if not open_trades:
            state["last_volatile_weekend_flatten_utc"] = iso_utc()
            state["last_volatile_weekend_flatten_count"] = 0
            self.save_state(state)
            log(f"{self.lane_prefix()}Weekend flat policy: already flat ({reason}).")
            return 0
        closed = 0
        dry_run = 0
        log(f"{self.lane_prefix()}Weekend flat policy: closing {len(open_trades)} open volatile-account trade(s). reason={reason}")
        for trade in list(open_trades):
            inst = normalize_instrument(trade.get("instrument"))
            action = {
                "instrument": inst,
                "trade_id": str(trade.get("id", "")),
                "action": "CLOSE",
                "direction": trade_direction_from_units(trade.get("currentUnits")),
                "reason": f"volatile weekend flat policy: {reason}",
            }
            try:
                status = self.close_trade(trade, action)
                if status in {"accepted", "accepted_after_recheck"}:
                    closed += 1
                elif status == "dry_run":
                    dry_run += 1
            except Exception as exc:
                self.log_error(f"volatile weekend flatten close {inst}", exc)
                try:
                    self.log_action(action, "position", "error", reject_reason=str(exc)[:500])
                except Exception:
                    pass
        state = self.load_state()
        state["last_volatile_weekend_flatten_utc"] = iso_utc()
        state["last_volatile_weekend_flatten_count"] = closed
        state["last_volatile_weekend_flatten_dry_run_count"] = dry_run
        self.save_state(state)
        log(f"{self.lane_prefix()}Weekend flat policy pass complete: confirmed_closed={closed}/{len(open_trades)} dry_run={dry_run}")
        return closed


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
        try:
            state_for_campaigns, campaigns_for_health = self.campaign_state_from_runtime()
            self.log_position_health_snapshot(open_trades, campaigns_for_health, state_for_campaigns)
            pruned = self.run_campaign_prune_pass(summary, open_trades, state_for_campaigns, reason="local_monitor")
            self.save_campaign_state(state_for_campaigns, campaigns_for_health)
            if pruned:
                account = self.oanda.get_account_summary()
                open_trades = self.oanda.get_open_trades()
                summary = account_summary_for_prompt(account)
        except Exception as exc:
            self.log_error("campaign prune from local monitor", exc)
        try:
            if self.volatile_weekend_flatten_due(reason_state := self.load_state()):
                self.run_volatile_weekend_flatten_pass(reason="local_monitor")
                account = self.oanda.get_account_summary()
                open_trades = self.oanda.get_open_trades()
                summary = account_summary_for_prompt(account)
        except Exception as exc:
            self.log_error("volatile weekend flatten from local monitor", exc)
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
        log(f"{self.lane_prefix()}Shared daily move comparison report: writer=False interval_minutes={AUTO_DAILY_REPORT_INTERVAL_MINUTES}")
        log(f"{self.lane_prefix()}OANDA env={self.cfg.oanda_env} account_name={self.cfg.account_display_name} instrument_filter={self.cfg.instrument_filter_mode} execution={'ENABLED' if self.should_execute() else 'DRY-RUN'}")
        log(f"{self.lane_prefix()}Technical scan cadence seconds={self.cfg.event_scan_interval_seconds} loop_sleep={self.cfg.loop_sleep_seconds}")
        log(f"{self.lane_prefix()}Technical trigger gates: min_basket={self.cfg.event_min_basket_pairs} major={self.cfg.event_min_major_net_pips}p cross={self.cfg.event_min_cross_net_pips}p exotic={self.cfg.event_min_exotic_net_pips}p ratio={self.cfg.event_min_move_to_spread_ratio}")
        log(f"{self.lane_prefix()}Pre-spike pressure: enabled={cfg_bool('FOREX_TECH_PRE_SPIKE_PRESSURE_ENABLED', default=True)} watch_score>={cfg_float('FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SCORE', default=62.0)} trade_score>={cfg_float('FOREX_TECH_PRE_SPIKE_PRESSURE_TRADE_SCORE', default=78.0)} min_ratio={cfg_float('FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_RATIO', default=0.80)} strict={cfg_bool('FOREX_TECH_PRE_SPIKE_PRESSURE_STRICT_GATE_ENABLED', default=False)} pressure_only={cfg_bool('FOREX_TECH_PRE_SPIKE_PRESSURE_ONLY', default=False)} cluster>={cfg_int('FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_CLUSTER_INSTRUMENTS', default=0)} dir_cluster>={cfg_int('FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_DIRECTION_CLUSTER_INSTRUMENTS', default=0)} value>={cfg_float('FOREX_TECH_PRE_SPIKE_PRESSURE_MIN_SIGNAL_VALUE_USD', default=0.0):.4f}")
        log(f"{self.lane_prefix()}Event scanner enabled={self.cfg.event_scanner_enabled} interval_seconds={self.cfg.event_scan_interval_seconds} scout_trades={'ENABLED' if self.cfg.event_scout_trades_enabled else 'OFF'} external_permission_required={self.cfg.event_require_research_permission}")
        log(f"{self.lane_prefix()}Volatile weekend flat policy enabled={volatile_weekend_policy_enabled()} block_new_scouts={cfg_str("FOREX_VOLATILE_WEEKEND_BLOCK_NEW_SCOUTS_TIME_NY", default="15:00")} flatten={cfg_str("FOREX_VOLATILE_WEEKEND_FLATTEN_TIME_NY", default="15:45")} sunday_resume={cfg_str("FOREX_VOLATILE_WEEKEND_SUNDAY_RESUME_TIME_NY", default="18:15")}")
        log(f"Sunday reopen manager enabled={cfg_bool('FOREX_SUNDAY_REOPEN_MANAGER_ENABLED', default=True)} summary_time={cfg_str('FOREX_WEEKEND_SUMMARY_SCAN_TIME_NY', default='16:45')} reopen_scan={cfg_str('FOREX_SUNDAY_REOPEN_SCAN_TIME_NY', default='17:15')} block_unrelated_new_trades_minutes={cfg_int('FOREX_SUNDAY_REOPEN_BLOCK_UNRELATED_NEW_TRADES_MINUTES', 'FOREX_SUNDAY_REOPEN_BLOCK_NEW_TRADES_MINUTES', default=20)}")
        if self.shutdown_mode_active():
            log(f"{self.lane_prefix()}Shutdown mode ACTIVE: {self.shutdown_mode_label()} new_opens_blocked={self.cfg.shutdown_block_new_trades} skip_portfolio={self.cfg.shutdown_skip_research}")
        if self.cfg.friday_call_times_ny != self.cfg.call_times_ny:
            log(f"{self.lane_prefix()}Friday portfolio call times NY={self.cfg.friday_call_times_ny}")
        if self.cfg.scan_on_launch and not fx_market_closed():
            try:
                self.run_research_scan(reason="launch")
                # v5.3: launch scans are separate from scheduled away-time scans.
                # They never mark, consume, or backfill scheduled RESEARCH slots.
            except Exception as exc:
                self.log_error("launch RESEARCH scan", exc)

        while True:
            try:
                state = self.load_state()
                maybe_auto_daily_move_report(account_lane="tech", writer=False, force=False)
                if self.volatile_weekend_flatten_due(state):
                    try:
                        self.run_volatile_weekend_flatten_pass(reason="loop")
                    except Exception as exc:
                        self.log_error("volatile weekend flatten loop", exc)
                    state = self.load_state()
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
                        if cfg_bool("FOREX_SUNDAY_REOPEN_USE_RESEARCH", default=True):
                            self.run_research_scan(reason="sunday_reopen")
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

                due, hhmm = self.research_due(state=state)
                if due:
                    self.run_research_scan(reason=f"scheduled {hhmm}")
                    state = self.load_state()
                    self.mark_research_run(hhmm, state)
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


def _auto_report_log_paths(data_root: Path, basename: str) -> List[Path]:
    """Return active plus rotated live log files for a basename.

    Rotation appends `.rotated_...` before the suffix. Daily reporting must
    include those rotated segments or same-day summaries can undercount after a
    large live file rolls over.
    """
    try:
        base = Path(basename)
        stem = base.stem
        suffix = base.suffix
        search_roots = [
            data_root / "forex",
            data_root / "forex_research_manager",
            data_root / "technical_scout_manager",
            data_root / "archive",
        ]
        existing_roots = [root for root in search_roots if root.exists()]
        if not existing_roots:
            existing_roots = [data_root]
        paths = [
            path
            for root in existing_roots
            for path in root.rglob(f"{stem}*{suffix}")
            if path.is_file()
            and (
                path.name == basename
                or (
                    path.name.startswith(f"{stem}.rotated_")
                    and path.suffix == suffix
                )
            )
        ]
        return sorted(set(paths), key=lambda path: str(path))
    except Exception:
        return sorted(data_root.rglob(basename))


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


def _auto_report_dedupe_rows(rows: List[Dict[str, Any]], key_fields: List[str]) -> List[Dict[str, Any]]:
    """Collapse duplicate report rows emitted through both journal and action CSVs.

    A single live skip can appear once in full_event_journal.csv and once in
    actions.csv.  For reporting, those are one missed opportunity.  The key uses
    second-level time precision so separate scanner loops remain separate rows.
    """
    seen: set[Tuple[Any, ...]] = set()
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        time_key = str(row.get("time_ny") or "")[:19]
        key = tuple([time_key] + [str(row.get(field) or "").strip() for field in key_fields])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


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

    journal_paths = _auto_report_log_paths(data_root, "full_event_journal.csv")
    action_paths = _auto_report_log_paths(data_root, "actions.csv")
    signal_paths = _auto_report_log_paths(data_root, "event_signals.csv")
    scan_paths = _auto_report_log_paths(data_root, "event_scan_summary.csv")

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

    missed_rows = _auto_report_dedupe_rows(
        missed_rows,
        ["instrument", "direction", "status", "miss_reason", "detail", "movement_key"],
    )
    captured_rows = _auto_report_dedupe_rows(
        captured_rows,
        ["instrument", "direction", "status", "trade_id", "order_id", "units", "price", "movement_key", "reason"],
    )
    missed_reason_counts = {}
    for row in missed_rows:
        _auto_report_bump(missed_reason_counts, row.get("miss_reason"))

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
# Single-account technical all-pairs spike-capture helpers
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
    """Force this script to the technical/scout account only.

    Credentials:
        OANDA_ACCOUNT_ID_TECH = "..."  # optional clearer alias
        OANDA_ACCOUNT_ID_VOL = "..."   # existing supported name

    This version is a technical/event-strength scout engine:
    - no launch RESEARCH scan
    - no scheduled RESEARCH scans
    - no event-triggered RESEARCH scans
    - scans all discovered OANDA currency pairs by default
    - opens short-term scout positions only when local move/spread/earlyness gates pass
    - keeps the weekend-flat policy so the technical account does not hold over weekends

    The known high-volatility list is retained only for special thresholds and
    risk handling. It does not limit the technical account to those pairs.
    """
    tech_id = cfg_str(
        "OANDA_ACCOUNT_ID_TECH",
        "OANDA_ACCOUNT_ID_TECHNICAL",
        "OANDA_ACCOUNT_ID_VOL",
        "OANDA_ACCOUNT_ID_VOLATILE",
        "OANDA_ACCOUNT_ID_OTHER",
        default="",
    )
    if not tech_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_ID_TECH or OANDA_ACCOUNT_ID_VOL in creds. This technical/scout script intentionally does not fall back to OANDA_ACCOUNT_ID.")

    special_volatile_pairs = [normalize_instrument(x) for x in (base_cfg.scout_volatile_pairs or base_cfg.technical_scout_pairs) if normalize_instrument(x)]
    exclude = {normalize_instrument(x) for x in cfg_list("FOREX_TECHNICAL_EXCLUDE_PAIRS", default=[]) if normalize_instrument(x)}
    cfg = with_data_dir(replace(
        base_cfg,
        oanda_account_id=tech_id,
        account_lane="tech",
        account_display_name="OANDA_ACCOUNT_ID_TECH/OANDA_ACCOUNT_ID_VOL",
        instrument_filter_mode="technical_all_except_exclude" if exclude else "all",
        technical_scout_pairs=[],
        scout_volatile_pairs=special_volatile_pairs,
        max_open_trades=cfg_int("FOREX_TECHNICAL_MAX_OPEN_TRADES", "FOREX_MAX_OPEN_TRADES", default=14),
        max_margin_used_pct=cfg_float("FOREX_TECH_MAX_MARGIN_USED_PCT", default=62.0),
        target_margin_used_pct=cfg_float("FOREX_TECH_TARGET_MARGIN_USED_PCT", default=45.0),
        min_risk_pct_per_trade=cfg_float("FOREX_TECH_MIN_RISK_PCT", default=0.03),
        event_max_scout_trades_per_event=cfg_int("FOREX_TECHNICAL_MAX_SCOUT_TRADES_PER_EVENT", "FOREX_EVENT_MAX_SCOUT_TRADES_PER_EVENT", default=2),
        event_max_total_scout_risk_pct=cfg_float("FOREX_TECHNICAL_MAX_TOTAL_SCOUT_RISK_PCT", "FOREX_EVENT_MAX_TOTAL_SCOUT_RISK_PCT", default=1.0),
        scan_on_launch=False,
        calls_per_trading_day=0,
        call_times_ny=[],
        friday_call_times_ny=[],
        event_trigger_research_enabled=False,
        min_minutes_between_research_scans=999999,
    ), base_cfg.data_dir / "account_technical_scout_all_pairs")
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

    if bot.volatile_weekend_flatten_due(state):
        bot.run_volatile_weekend_flatten_pass(reason="one_loop_iteration")
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

    # Intentionally no RESEARCH due-check here. This volatile script trades only the
    # local event/scout system because sudden spikes will be stale by the time a
    # news/RESEARCH management cycle returns.


def run_multi_account_loop(bots: List[ForexManager]) -> None:
    # Kept for CLI compatibility; this single-purpose file always has one lane.
    for bot in bots:
        bot.loop()
# =============================================================================
# CLI
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Always-on OANDA technical-pair local spike/scout manager.")
    p.add_argument("--print-config", action="store_true", help="Print resolved config without revealing keys.")
    p.add_argument("--scan-now", action="store_true", help="Run one portfolio-style portfolio-management scan immediately.")
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
    p.add_argument("--daily-report-date", default="", help="Regenerate the shared daily move/account comparison report for YYYY-MM-DD.")
    p.add_argument("--event-scan-now", action="store_true", help="Run one local M1 event scanner pass immediately.")
    p.add_argument("--weekend-flatten-now", action="store_true", help="Immediately close all open trades in the volatile account using the weekend flat policy.")
    p.add_argument("--weekend-summary-now", action="store_true", help="Run one weekend summary planning scan now; no broker execution.")
    p.add_argument("--sunday-reopen-now", action="store_true", help="Run Sunday reopen hard protection plus management scan now.")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    daily_report_date = str(getattr(args, "daily_report_date", "") or "").strip()
    daily_report_requested = bool(args.daily_report_now or daily_report_date)
    report_only = (
        daily_report_requested
        and args.once
        and not (
            args.print_config
            or args.scan_now
            or args.monitor_now
            or args.write_recaps_now
            or args.shutdown_pass_now
            or args.event_scan_now
            or args.weekend_summary_now
            or args.sunday_reopen_now
            or args.weekend_flatten_now
        )
    )
    if report_only:
        if daily_report_date:
            report_dir = generate_auto_daily_move_comparison_report(daily_report_date)
            log(f"[manual] Wrote shared daily move comparison report for {daily_report_date}: {report_dir}")
        else:
            maybe_auto_daily_move_report(account_lane="manual", writer=True, force=True)
        return 0

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
        base_cfg.shutdown_skip_research = True

    lane_cfgs = build_account_lane_configs(base_cfg)
    bots = [ForexManager(c) for c in lane_cfgs]

    if args.print_config:
        for bot in bots:
            log(f"Resolved config for lane={bot.cfg.account_lane} account_name={bot.cfg.account_display_name}")
            log(f"market_movement_ledger={bot.cfg.market_movements_csv.parent}")
            bot.print_config()
        if not (args.scan_now or args.monitor_now or args.write_recaps_now or daily_report_requested or args.shutdown_pass_now or args.event_scan_now or args.weekend_summary_now or args.sunday_reopen_now or args.weekend_flatten_now):
            return 0

    did_explicit = False
    if args.write_recaps_now:
        for bot in bots:
            bot.write_daily_recaps_around_now()
            log(f"{bot.lane_prefix()}Wrote daily recap files under {bot.cfg.daily_recap_dir}")
        did_explicit = True
    if daily_report_requested:
        if daily_report_date:
            report_dir = generate_auto_daily_move_comparison_report(daily_report_date)
            log(f"{bots[0].lane_prefix() if bots else ''}Wrote shared daily move comparison report for {daily_report_date}: {report_dir}")
        else:
            maybe_auto_daily_move_report(account_lane="tech", writer=True, force=True)
        did_explicit = True

    explicit_flags = (
        args.scan_now or args.monitor_now or args.write_recaps_now or daily_report_requested or args.shutdown_pass_now
        or args.event_scan_now or args.weekend_summary_now or args.sunday_reopen_now or args.weekend_flatten_now
    )
    if not args.print_config and not (args.once and args.write_recaps_now and not explicit_flags):
        for bot in bots:
            bot.validate_config()

    if args.weekend_flatten_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_volatile_weekend_flatten_pass(reason="manual_weekend_flatten", force=True)
        did_explicit = True

    if args.monitor_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_local_monitor()
        did_explicit = True
    if args.scan_now:
        log("[SCOUT] --scan-now ignored: this technical spike/scout script intentionally has RESEARCH scans disabled.")
        did_explicit = True
    if args.event_scan_now:
        for bot in bots:
            ensure_bot_ready(bot)
            bot.run_event_scan(reason="manual")
        did_explicit = True
    if args.weekend_summary_now:
        log("[SCOUT] --weekend-summary-now ignored: RESEARCH/weekend summary is disabled in this technical spike/scout script.")
        did_explicit = True
    if args.sunday_reopen_now:
        log("[SCOUT] --sunday-reopen-now ignored: Sunday RESEARCH reopen management is disabled in this technical spike/scout script.")
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
# RESEARCH_API_API_KEY = "sk-..."
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
# - v5.15 scout trades do not require RESEARCH permission by default; set FOREX_EVENT_REQUIRE_RESEARCH_PERMISSION=True to restore permissioned-only scouts.
# - To fade/shutdown: set EMBEDDED_CONFIG["FOREX_SHUTDOWN_MODE"] = "fade".
