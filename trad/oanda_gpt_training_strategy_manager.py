#!/usr/bin/env python3
r"""
oanda_gpt_training_strategy_manager.py

Independent OANDA FX research / strategy manager.

Purpose:
- Runs independently from the MAJ/GPT advisor and VOL technical scripts.
- Uses OANDA_ACCOUNT_ID_DUM1 only, defaulting to 101-001-37981792-004.
- Runs always-on research by default. It pulls/builds local data, tests models,
  asks GPT for candidate ideas, and writes lifecycle manifests. It does not use
  a live/practice account for candidate testing unless a separate explicit
  canary executor is assigned a validated candidate.

Design principles:
- No hard-coded API keys.
- Reads creds from TRAD_PROJECT_ROOT\creds by default, or OANDA_CREDS_PATH when set.
- Practice execution is disabled by default in this process. Research failures
  stay in the registry and are not exposed to DUM or technical execution.
- Does not touch MAJ/GPT or VOL accounts.
- Does not rewrite its own Python file while running.
- Improves by changing strategy profile JSON/model outputs/benchmark records.

Expected optional packages:
    requests pandas numpy scikit-learn joblib openai
Minimum required packages:
    requests pandas numpy

Run:
    python oanda_gpt_training_strategy_manager.py
"""
from __future__ import annotations

import argparse
import ast
import csv
import ctypes
import dataclasses
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
import traceback
import warnings
import zipfile
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from oanda_model_lifecycle import (
    ModelLifecycleRegistry,
    candidate_id as lifecycle_candidate_id,
    experiment_spec_hash,
    normalized_instrument_whitelist,
)

try:
    import requests
except Exception as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: requests. Run: python -m pip install requests") from exc

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: pandas. Run: python -m pip install pandas numpy") from exc

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

try:
    import numpy as np
except Exception as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: numpy. Run: python -m pip install numpy") from exc

try:
    from sklearn.ensemble import (
        ExtraTreesClassifier,
        GradientBoostingClassifier,
        HistGradientBoostingClassifier,
        RandomForestClassifier,
        RandomForestRegressor,
    )
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        average_precision_score,
        brier_score_loss,
        roc_auc_score,
    )
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

try:
    from lightgbm import LGBMClassifier  # type: ignore
    LIGHTGBM_AVAILABLE = True
except Exception:
    LGBMClassifier = None  # type: ignore
    LIGHTGBM_AVAILABLE = False

try:
    from xgboost import XGBClassifier  # type: ignore
    XGBOOST_AVAILABLE = True
except Exception:
    XGBClassifier = None  # type: ignore
    XGBOOST_AVAILABLE = False

try:
    from catboost import CatBoostClassifier  # type: ignore
    CATBOOST_AVAILABLE = True
except Exception:
    CatBoostClassifier = None  # type: ignore
    CATBOOST_AVAILABLE = False

try:
    from ngboost import NGBClassifier  # type: ignore
    NGBOOST_AVAILABLE = True
except Exception:
    NGBClassifier = None  # type: ignore
    NGBOOST_AVAILABLE = False

try:
    import joblib
    JOBLIB_AVAILABLE = True
except Exception:
    JOBLIB_AVAILABLE = False

# -----------------------------------------------------------------------------
# User/project defaults
# -----------------------------------------------------------------------------
SCRIPT_NAME = Path(__file__).name
SCRIPT_VERSION = "dum1_training_manager_v1_1_scout_lead_ensemble"
DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT)))
CREDS_PATH = Path(os.environ.get("OANDA_CREDS_PATH", str(PROJECT_ROOT / "creds")))

TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
BENCHMARK_ROOT = PROJECT_ROOT / "data" / "oanda_strategy_benchmark"
RAW_M1_ROOT = PROJECT_ROOT / "data" / "forex" / "m1_raw"
TECHNICAL_RESEARCH_ROOT = PROJECT_ROOT / "data" / "all68_weekly_move_study" / "rolling_validation"
TECHNICAL_VOLATILITY_PATH = (
    PROJECT_ROOT
    / "data"
    / "all68_weekly_move_study"
    / "reports"
    / "instrument_volatility.csv"
)
TECHNICAL_SHADOW_PATH = (
    PROJECT_ROOT
    / "data"
    / "technical_scout_manager"
    / "account_technical_scout_all_pairs"
    / "exhaustion_shadow_watchlist.csv"
)

ACCOUNT_ROLE = "research_manager"
DEFAULT_DUM1_ACCOUNT = "101-001-37981792-004"
TRUE_ENV_VALUES = {"1", "true", "yes", "on"}


def env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in TRUE_ENV_VALUES

# Turn-on defaults. No command-line flags required.
EXECUTE_TRADES_BY_DEFAULT = False
ALLOW_LIVE_ENV = False  # keep false unless intentionally changed in code later
ALLOW_GPT_CALLS = True
ALLOW_MODEL_TRAINING = True
ALLOW_AUTO_SWITCH_DUM1_STRATEGY = False
ALLOW_AUTO_PROMOTE_TECHNICAL_PRODUCTION = not (
    os.environ.get(
        "OANDA_DISABLE_AUTO_PROMOTE_TECHNICAL_PRODUCTION",
        "",
    ).strip().lower()
    in {"1", "true", "yes", "on"}
)
REQUIRE_PRODUCTION_GATE_FOR_AUTO_SWITCH = True
ALLOW_CANDLE_BACKFILL = True
ALLOW_WEEKDAY_LIGHT_TESTS = True
ALLOW_WEEKEND_HEAVY_TESTS = True
WEEKEND_NON_SCOUT_FOCUS = (
    os.environ.get("OANDA_WEEKEND_NON_SCOUT_FOCUS", "").strip().lower()
    in {"1", "true", "yes", "on"}
)

# Loop cadences
MAIN_LOOP_SECONDS = 60
ACCOUNT_SYNC_SECONDS = 60
MARKET_SCAN_SECONDS = 90
TRAINING_DATASET_SECONDS = 15 * 60
LIGHT_BACKTEST_SECONDS = 30 * 60
GPT_REVIEW_SECONDS = 6 * 60 * 60
FULL_REPORT_SECONDS = 24 * 60 * 60
REPORTING_EXTENSIONS_SECONDS = 30 * 60
EXPORT_SECONDS = 30 * 60
BENCHMARK_SECONDS = 30 * 60
HISTORICAL_BACKFILL_SECONDS = 4 * 60 * 60
TECHNICAL_EVIDENCE_SECONDS = 5 * 60
MISSED_SPIKE_BACKTEST_SECONDS = 30 * 60
PRE_SPIKE_LEAD_BACKTEST_SECONDS = 4 * 60 * 60
ENSEMBLE_SHADOW_REFRESH_SECONDS = 6 * 60 * 60
DEPENDENCY_ERROR_RETRY_SECONDS = 10 * 60
WEEKEND_ROLLING_VALIDATION_MIN_TRAIN_WEEKS = 4
WEEKEND_ROLLING_VALIDATION_MAX_HOLDOUT_WEEKS = 8
WEEKEND_ROLLING_VALIDATION_MAX_TRAIN_ROWS = 250_000
CONTINUOUS_RESEARCH_SLEEP_SECONDS = 20
CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS = 2
CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS = 75_000
CONTINUOUS_RESEARCH_HOLDOUT_WEEKS = 8
CONTINUOUS_RESEARCH_MAX_TRAIN_ROWS = 150_000
CONTINUOUS_RESEARCH_PURGE_MINUTES = 240
CONTINUOUS_RESEARCH_CALIBRATION_WEEKS = 1
TECHNICAL_RESEARCH_DATASET_VERSION = 12
MAJOR_MOVE_CATALOG_HORIZONS = [15, 30, 60, 120, 240]
MAJOR_MOVE_MODEL_HORIZONS = [30, 60, 120]
MAJOR_MOVE_LEAD_MINUTES = [15, 30, 60]
MAJOR_MOVE_EXECUTION_POLICIES = [
    "fixed",
    "trailing",
    "trailing_stop010",
    "trailing_stop012",
    "trailing_stop015",
    "trailing_stop020",
    "trailing_stop025",
    "trailing_stop035",
    "trailing_stop050",
    "trailing_stop075",
    "trailing_stop100",
]
MAJOR_MOVE_MATERIALIZED_EXECUTION_POLICIES = [
    "fixed",
    "trailing",
    "trailing_stop075",
    "trailing_stop100",
]
MAJOR_MOVE_STOP_CAP_ATR_UNITS = {
    "stop010": 0.10,
    "stop012": 0.12,
    "stop015": 0.15,
    "stop020": 0.20,
    "stop025": 0.25,
    "stop035": 0.35,
    "stop050": 0.50,
    "stop075": 0.75,
    "stop100": 1.00,
}
MAJOR_MOVE_QUANTILE = 0.995
MAJOR_MOVE_MIN_ATR_UNITS = 1.5
MAJOR_MOVE_MIN_SPREAD_MULTIPLE = 3.0
MAJOR_MOVE_MIN_HISTORY_ROWS = 1_000
MAJOR_MOVE_NEGATIVE_SAMPLE_RATIO = 20
MAJOR_MOVE_EPISODE_MINUTES = 30
MAJOR_MOVE_MAX_SIGNALS_PER_EPISODE = 3
MAJOR_MOVE_STOP_EXPECTED_UNITS = 0.75
MAJOR_MOVE_TARGET_EXPECTED_UNITS = 1.50
MAJOR_MOVE_TRAIL_ACTIVATION_UNITS = 1.00
MAJOR_MOVE_TRAIL_DISTANCE_UNITS = 0.50
PROFITABLE_PRECURSOR_HORIZONS = [30, 60, 120]
PROFITABLE_PRECURSOR_QUANTILE = 0.98
PROFITABLE_PRECURSOR_MIN_ATR_UNITS = 0.75
PROFITABLE_PRECURSOR_MIN_SPREAD_MULTIPLE = 2.0
RETURN_CURVE_HORIZONS = [30, 60, 120]
RETURN_CURVE_EARLY_HORIZON = {30: 15, 60: 15, 120: 30}
RETURN_CURVE_BEST_WEIGHT = 0.55
RETURN_CURVE_ENDPOINT_WEIGHT = 0.25
RETURN_CURVE_EARLY_WEIGHT = 0.20
RETURN_CURVE_ADVERSE_PENALTY = 0.35
VOLATILE_PAIR_MIN_PERCENTILE = 0.70
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
SAFE_HAVEN_CURRENCIES = {"JPY", "CHF", "USD"}
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
RISK_CURRENCIES = {"AUD", "NZD", "CAD", "NOK", "ZAR", "MXN", "TRY"}
CURRENCY_STRUCTURAL_CARRY_SCORE = {
    # Stable cross-sectional proxy, not a historical policy-rate time series.
    # Current/live macro files can override rate context through macro_rate_diff_bias.
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
MACRO_BIAS_LATEST_PATH = TRAINING_ROOT / "macro" / "latest_macro_bias.json"
MACRO_BIAS_SCHEMA_PATH = TRAINING_ROOT / "macro" / "latest_macro_bias.schema.json"
MACRO_BIAS_MAX_AGE_HOURS = 36
VALID_INSTRUMENT_SUBSETS = {
    "all",
    "majors",
    "usd_pairs",
    "non_usd",
    "volatile",
    "exotic",
    "volatile_exotic",
    "non_usd_volatile",
    *PAIR_TAXONOMY_CLASSES,
}
HISTORICAL_EXPANSION_TARGET_DAYS = 730
RESEARCH_VALIDATION_PROTOCOL_VERSION = "purged_calibrated_v2"

# Account/risk defaults. This is an experimental practice account using margin.
DUM1_MAX_MARGIN_PCT = 60.0
DUM1_HARD_MARGIN_PCT = 72.0
DUM1_RISK_PER_TRADE_PCT = 4.0
DUM1_MAX_OPEN_TRADES = 8
DUM1_DAILY_LOSS_LOCK_PCT = 12.0
DUM1_MIN_TRADE_UNITS = 1
ALLOW_COMPOUND_WINNERS = True
NO_AVERAGING_DOWN = True

# Training/backtest defaults
MIN_BACKTEST_TRADES_FOR_SWITCH = 25
MIN_LIVE_TRADES_FOR_SWITCH = 10
MIN_SCORE_EDGE_TO_SWITCH = 7.5
MIN_PROFIT_FACTOR_TO_SWITCH = 1.20
MIN_VALIDATION_PROFIT_FACTOR = 1.20
MIN_VALIDATION_POSITIVE_WEEK_SHARE = 0.70
MAX_VALIDATION_TOP_PAIR_TRADE_SHARE = 0.35
MAX_DRAWDOWN_PCT_FOR_SWITCH = 18.0
MAX_OUTLIER_DEPENDENCY = 0.55
KEEP_TOP_N_BENCHMARKS_PER_PAIR = 5

# Scanner defaults
DEFAULT_CANDLE_COUNT = 500
DEEP_BACKFILL_DEFAULT_DAYS = 30
DEEP_BACKFILL_BATCH_SIZE = 5000
HISTORICAL_TRAINING_STEP_MINUTES = 15
HISTORICAL_TRAINING_MAX_ROWS_FOR_LIVE_RETRAIN = 25_000
CANDLE_GRANULARITIES = ["M1", "M5"]
MAX_INSTRUMENTS_SCAN = 68
INSTRUMENT_REFRESH_SECONDS = 6 * 60 * 60
PREFERRED_MAJOR_INSTRUMENTS = [
    "EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD", "USD_CHF", "AUD_USD", "NZD_USD",
    "EUR_JPY", "GBP_JPY", "AUD_JPY", "NZD_JPY", "CAD_JPY", "CHF_JPY",
    "EUR_GBP", "EUR_AUD", "EUR_CAD", "GBP_AUD", "GBP_CAD", "AUD_NZD",
    "AUD_CAD", "NZD_CAD", "USD_SGD", "USD_NOK", "USD_SEK", "USD_MXN",
]
PIP_LOCATION_MINUS2 = {
    "AUD_JPY",
    "CAD_JPY",
    "CHF_JPY",
    "EUR_HUF",
    "EUR_JPY",
    "GBP_JPY",
    "NZD_JPY",
    "SGD_JPY",
    "TRY_JPY",
    "USD_HUF",
    "USD_JPY",
    "USD_THB",
    "ZAR_JPY",
}

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
DIRS = {
    "root": TRAINING_ROOT,
    "candles": TRAINING_ROOT / "candles",
    "candles_bam": TRAINING_ROOT / "candles_bam",
    "features": TRAINING_ROOT / "features",
    "training_sets": TRAINING_ROOT / "training_sets",
    "models": TRAINING_ROOT / "models",
    "strategies": TRAINING_ROOT / "strategies",
    "candidates": TRAINING_ROOT / "strategies" / "candidates",
    "backtests": TRAINING_ROOT / "backtests",
    "live": TRAINING_ROOT / "live_dum1",
    "reports": TRAINING_ROOT / "reports",
    "gpt_reviews": TRAINING_ROOT / "gpt_reviews",
    "exports": TRAINING_ROOT / "exports",
    "state": TRAINING_ROOT / "state",
    "raw_json": TRAINING_ROOT / "raw_json",
    "technical_evidence": TRAINING_ROOT / "technical_evidence",
    "macro": TRAINING_ROOT / "macro",
    "research": TRAINING_ROOT / "continuous_research",
    "research_experiments": TRAINING_ROOT / "continuous_research" / "experiments",
    "research_specs": TRAINING_ROOT / "continuous_research" / "gpt_specs",
    "registry": TRAINING_ROOT / "model_lifecycle",
    "promotions": TRAINING_ROOT / "promotions",
}

BENCHMARK_FILES = {
    "master": BENCHMARK_ROOT / "strategy_benchmark_master.csv",
    "top_by_pair": BENCHMARK_ROOT / "strategy_benchmark_top_by_pair.csv",
    "top_by_family": BENCHMARK_ROOT / "strategy_benchmark_top_by_family.csv",
    "history": BENCHMARK_ROOT / "strategy_benchmark_history.jsonl",
    "backups": BENCHMARK_ROOT / "strategy_benchmark_backups",
}

LOG_FILES = {
    "audit": DIRS["live"] / "dum1_training_audit.csv",
    "actions": DIRS["live"] / "dum1_actions.csv",
    "orders_intent": DIRS["live"] / "order_intent_ledger.csv",
    "orders_result": DIRS["live"] / "order_result_ledger.csv",
    "monitor": DIRS["live"] / "monitor.csv",
    "strategy_changes": DIRS["live"] / "strategy_change_log.csv",
    "candidate_queue": DIRS["candidates"] / "candidate_queue.jsonl",
    "errors": DIRS["live"] / "recent_errors.csv",
    "gpt_calls": DIRS["gpt_reviews"] / "gpt_call_ledger.csv",
    "research_experiments": DIRS["research"] / "experiment_ledger.csv",
}

STATE_FILE = DIRS["state"] / "manager_state.json"
RESEARCH_PYTHON_RUNNER = PROJECT_ROOT / "research_python_runner.py"
HISTORY_EXPANSION_LOCK = DIRS["state"] / "history_expansion.lock"
ACTIVE_STRATEGY_FILE = DIRS["strategies"] / "active_dum1_strategy.json"
LATEST_EXPORT_ZIP = DIRS["exports"] / "latest_dum1_training_review_package.zip"
PRODUCTION_GATE_REPORT_CANDIDATES = [
    DIRS["reports"] / "latest_s5_event_meta_model_report.json",
    DIRS["reports"] / "latest_event_meta_model_report.json",
    DIRS["reports"] / "latest_production_model_report.json",
]

CSV_LOCK = threading.RLock()
_TECHNICAL_SPREAD_CACHE: Optional[Dict[str, float]] = None
_TECHNICAL_VOLATILITY_CACHE: Optional[Dict[str, Dict[str, float]]] = None


def project_python_command(script: Path, *args: Any) -> List[str]:
    command = [sys.executable]
    if RESEARCH_PYTHON_RUNNER.exists():
        command.append(str(RESEARCH_PYTHON_RUNNER))
    command.append(str(script))
    command.extend(str(arg) for arg in args)
    return command


def should_refresh_sidecar(
    last: Any,
    latest: Any,
    now: datetime,
    normal_cadence_seconds: int,
) -> bool:
    if not last:
        return True
    elapsed = now - parse_oanda_time(last)
    if elapsed >= timedelta(seconds=normal_cadence_seconds):
        return True
    if elapsed < timedelta(seconds=DEPENDENCY_ERROR_RETRY_SECONDS):
        return False
    if not RESEARCH_PYTHON_RUNNER.exists() or not isinstance(latest, dict):
        return False
    status = str(latest.get("status") or "").strip().lower()
    if status != "error":
        return False
    detail = f"{latest.get('stderr_tail') or ''}\n{latest.get('error') or ''}"
    return "ModuleNotFoundError" in detail or "No module named" in detail

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def ensure_dirs() -> None:
    for p in DIRS.values():
        p.mkdir(parents=True, exist_ok=True)
    BENCHMARK_ROOT.mkdir(parents=True, exist_ok=True)
    BENCHMARK_FILES["backups"].mkdir(parents=True, exist_ok=True)
    ensure_macro_bias_schema()


def ensure_macro_bias_schema() -> None:
    schema = {
        "description": (
            "Structured macro/GPT bias consumed as risk context. Positive "
            "pair_bias means LONG/base-currency supportive; negative means "
            "SHORT/base-currency supportive. This file is not allowed to "
            "trigger trades directly."
        ),
        "updated_utc": "2026-01-01T00:00:00+00:00",
        "global": {
            "usd_bias": 0.0,
            "jpy_bias": 0.0,
            "chf_bias": 0.0,
            "risk_bias": 0.0,
            "commodity_bias": 0.0,
            "news_risk": 0.0,
            "event_risk": 0.0,
        },
        "currency_rates": {
            "USD": {
                "policy_rate": 0.0,
                "carry_score": 0.0,
                "source": "neutral template",
            },
            "JPY": {
                "policy_rate": 0.0,
                "carry_score": 0.0,
                "source": "neutral template",
            },
        },
        "pairs": {
            "EUR_USD": {
                "pair_bias": 0.0,
                "rate_diff_bias": 0.0,
                "news_risk": 0.0,
                "event_risk": 0.0,
                "reason": "neutral template",
            }
        },
    }
    if not MACRO_BIAS_SCHEMA_PATH.exists():
        save_json(MACRO_BIAS_SCHEMA_PATH, schema)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).isoformat()


def now_ny_string() -> str:
    # Avoid external tz deps. New York offset varies; this is for log readability only.
    # Trading timestamps use UTC as source of truth.
    return iso_utc()


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def physical_memory_status() -> Dict[str, Any]:
    """Return compact physical-memory status for trainer resource guards."""

    if os.name != "nt":
        return {"error": "physical memory guard is implemented for Windows hosts"}

    class MemoryStatusEx(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    try:
        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(MemoryStatusEx)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise ctypes.WinError()
        total_mb = round(float(status.ullTotalPhys) / (1024 * 1024), 1)
        available_mb = round(float(status.ullAvailPhys) / (1024 * 1024), 1)
        return {
            "total_mb": total_mb,
            "available_mb": available_mb,
            "used_pct": round(float(status.dwMemoryLoad), 1),
            "available_pct": round((available_mb / total_mb) * 100.0, 1) if total_mb else "",
        }
    except Exception as exc:
        return {"error": str(exc)[:500]}


def safe_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    text = str(x).strip().lower()
    return text in {"1", "true", "yes", "y", "passed", "pass"}


def running_python_process_count(needles: Sequence[str]) -> int:
    """Return the number of running Python commands containing all needles.

    This is used only as a low-risk singleton guard for long research reports.
    If process inspection fails, return 0 and let the normal report path run.
    """
    clean_needles = [str(n) for n in needles if str(n)]
    if not clean_needles:
        return 0
    try:
        if os.name == "nt":
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    (
                        "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
                        "Where-Object { $_.Name -match '^python' -and $_.CommandLine } | "
                        "Select-Object -ExpandProperty CommandLine"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if completed.returncode == 0:
                return sum(
                    1
                    for line in (completed.stdout or "").splitlines()
                    if all(needle in line for needle in clean_needles)
                )
            return 0
        completed = subprocess.run(
            ["ps", "-eo", "args="],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if completed.returncode != 0:
            return 0
        count = 0
        for line in (completed.stdout or "").splitlines():
            if "python" in line.lower() and all(needle in line for needle in clean_needles):
                count += 1
        return count
    except Exception:
        return 0


class InsufficientRollingFolds(RuntimeError):
    """A research spec slice is too sparse for purged rolling validation."""


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(
        f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}"
    )
    tmp.write_text(
        json.dumps(obj, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    last_error: Optional[BaseException] = None
    for attempt in range(6):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.05 * (attempt + 1))
    try:
        tmp.unlink(missing_ok=True)
    except Exception:
        pass
    if last_error:
        raise last_error


def current_production_gate() -> Dict[str, Any]:
    reports = [path for path in PRODUCTION_GATE_REPORT_CANDIDATES if path.exists()]
    if not reports:
        return {
            "passed": False,
            "reason": "no production validation report exists",
            "report_path": "",
        }
    report_path = max(reports, key=lambda path: path.stat().st_mtime)
    report = load_json(report_path, {})
    final = report.get("final", {}) if isinstance(report, dict) else {}
    gate = final.get("production_gate", {}) if isinstance(final, dict) else {}
    return {
        "passed": bool(gate.get("passed", False)),
        "reason": "production gate passed" if gate.get("passed") else "latest production gate failed",
        "report_path": str(report_path),
        "generated_utc": report.get("generated_utc", ""),
        "pipeline_version": report.get("pipeline_version", ""),
        "gate": gate,
    }


def enforce_current_promotion_protocol() -> None:
    """Prevent legacy validation scores from blocking stricter candidates."""
    for name in [
        "technical_model_manifest.json",
        "dum_validation_manifest.json",
    ]:
        path = DIRS["promotions"] / name
        manifest = load_json(path, {})
        if not manifest or manifest.get(
            "validation_protocol_version"
        ) == RESEARCH_VALIDATION_PROTOCOL_VERSION:
            continue
        manifest["technical_account_activation"] = False
        manifest["stage"] = "revalidation_required"
        manifest["legacy_research_score"] = manifest.get("research_score")
        manifest["reason"] = (
            "Legacy champion retained as an artifact but must pass the "
            f"{RESEARCH_VALIDATION_PROTOCOL_VERSION} protocol before use."
        )
        save_json(path, manifest)


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def append_csv(path: Path, row: Dict[str, Any], fieldnames: Optional[List[str]] = None) -> None:
    with CSV_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        if fieldnames is None:
            fieldnames = list(row.keys())
        exists = path.exists() and path.stat().st_size > 0
        if exists:
            try:
                with path.open("r", newline="", encoding="utf-8") as existing_handle:
                    reader = csv.DictReader(existing_handle)
                    existing_fields = list(reader.fieldnames or [])
                    if existing_fields:
                        requested_fields = list(fieldnames)
                        merged_fields = requested_fields + [
                            field for field in existing_fields if field not in requested_fields
                        ]
                        if existing_fields != merged_fields:
                            existing_rows = list(reader)
                            tmp_path = path.with_name(
                                f"{path.stem}.schema_tmp_{os.getpid()}{path.suffix}"
                            )
                            with tmp_path.open("w", newline="", encoding="utf-8") as tmp_handle:
                                writer = csv.DictWriter(
                                    tmp_handle,
                                    fieldnames=merged_fields,
                                    extrasaction="ignore",
                                )
                                writer.writeheader()
                                for existing_row in existing_rows:
                                    writer.writerow(existing_row)
                            os.replace(tmp_path, path)
                            fieldnames = merged_fields
                        else:
                            fieldnames = existing_fields
            except Exception as exc:
                log_error("append_csv_schema_evolution", exc)
                if "existing_fields" in locals() and existing_fields:
                    fieldnames = existing_fields
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerow(row)


def read_csv(path: Path) -> pd.DataFrame:
    try:
        if path.exists() and path.stat().st_size > 0:
            return pd.read_csv(path)
    except Exception:
        pass
    return pd.DataFrame()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def dataframe_fingerprint(frame: pd.DataFrame) -> str:
    if frame.empty:
        return sha256_text("empty")
    columns = sorted(str(column) for column in frame.columns)
    normalized = frame.loc[:, columns].copy()
    hashed = pd.util.hash_pandas_object(normalized, index=False).to_numpy()
    digest = hashlib.sha256()
    digest.update("|".join(columns).encode("utf-8"))
    digest.update(hashed.tobytes())
    return digest.hexdigest()


def csv_last_timestamp(path: Path, column: str) -> Optional[datetime]:
    """Read a CSV's final timestamp without loading the full file."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
            header = next(csv.reader([handle.readline().rstrip("\r\n")]))
        column_index = header.index(column)
        with path.open("rb") as handle:
            size = path.stat().st_size
            handle.seek(max(0, size - 16_384))
            lines = handle.read().decode("utf-8", errors="ignore").splitlines()
        for line in reversed(lines):
            values = next(csv.reader([line]))
            if len(values) > column_index and values[column_index]:
                parsed = pd.to_datetime(
                    values[column_index],
                    errors="coerce",
                    utc=True,
                )
                if not pd.isna(parsed):
                    return parsed.to_pydatetime()
    except Exception:
        return None
    return None


def pips_multiplier(instrument: str) -> float:
    normalized = str(instrument or "").strip().upper().replace("/", "_")
    return 100.0 if normalized in PIP_LOCATION_MINUS2 else 10_000.0


def price_to_pips(instrument: str, price_delta: float) -> float:
    return price_delta * pips_multiplier(instrument)


def pips_to_price(instrument: str, pips: float) -> float:
    return pips / pips_multiplier(instrument)


def historical_spread_pips(instrument: str) -> float:
    global _TECHNICAL_SPREAD_CACHE
    if _TECHNICAL_SPREAD_CACHE is None:
        _TECHNICAL_SPREAD_CACHE = {}
        if TECHNICAL_VOLATILITY_PATH.exists():
            try:
                spread_frame = pd.read_csv(
                    TECHNICAL_VOLATILITY_PATH,
                    usecols=["instrument", "median_spread_pips"],
                )
                spread_frame["median_spread_pips"] = pd.to_numeric(
                    spread_frame["median_spread_pips"],
                    errors="coerce",
                )
                _TECHNICAL_SPREAD_CACHE = {
                    str(row.instrument): max(
                        0.1,
                        float(row.median_spread_pips),
                    )
                    for row in spread_frame.dropna().itertuples()
                }
            except Exception:
                _TECHNICAL_SPREAD_CACHE = {}
    normalized = str(instrument or "").strip().upper().replace("/", "_")
    if normalized in _TECHNICAL_SPREAD_CACHE:
        return _TECHNICAL_SPREAD_CACHE[normalized]
    return 1.2 if normalized in PREFERRED_MAJOR_INSTRUMENTS[:7] else (
        2.0 if normalized in PIP_LOCATION_MINUS2 else 2.5
    )


def split_pair(instrument: str) -> Tuple[str, str]:
    normalized = str(instrument or "").strip().upper().replace("/", "_")
    if "_" in normalized:
        base, quote = normalized.split("_", 1)
        return base[:3], quote[:3]
    return normalized[:3], normalized[3:6]


def load_technical_volatility_table() -> Dict[str, Dict[str, float]]:
    """Cached all-68 volatility metadata used for subset analysis."""
    global _TECHNICAL_VOLATILITY_CACHE
    if _TECHNICAL_VOLATILITY_CACHE is not None:
        return _TECHNICAL_VOLATILITY_CACHE
    table: Dict[str, Dict[str, float]] = {}
    if TECHNICAL_VOLATILITY_PATH.exists():
        try:
            frame = pd.read_csv(TECHNICAL_VOLATILITY_PATH)
            if "instrument" in frame:
                frame["instrument"] = (
                    frame["instrument"].astype(str).str.upper().str.replace("/", "_")
                )
                for column in [
                    "median_abs_60_pips",
                    "q995_abs_60_pips",
                    "median_spread_pips",
                    "q995_move_to_spread",
                    "volatility_percentile",
                ]:
                    if column in frame:
                        frame[column] = pd.to_numeric(
                            frame[column],
                            errors="coerce",
                        )
                if (
                    "volatility_percentile" not in frame
                    or frame["volatility_percentile"].isna().any()
                ) and "q995_abs_60_pips" in frame:
                    ranked = frame["q995_abs_60_pips"].rank(pct=True)
                    frame["volatility_percentile"] = frame.get(
                        "volatility_percentile",
                        ranked,
                    ).fillna(ranked)
                for row in frame.itertuples(index=False):
                    inst = str(getattr(row, "instrument", ""))
                    if not inst:
                        continue
                    table[inst] = {
                        "median_abs_60_pips": safe_float(
                            getattr(row, "median_abs_60_pips", 0.0),
                        ),
                        "q995_abs_60_pips": safe_float(
                            getattr(row, "q995_abs_60_pips", 0.0),
                        ),
                        "median_spread_pips": safe_float(
                            getattr(row, "median_spread_pips", 0.0),
                        ),
                        "q995_move_to_spread": safe_float(
                            getattr(row, "q995_move_to_spread", 0.0),
                        ),
                        "volatility_percentile": safe_float(
                            getattr(row, "volatility_percentile", 0.0),
                        ),
                    }
        except Exception:
            table = {}
    _TECHNICAL_VOLATILITY_CACHE = table
    return table


def instrument_subset_flags(instrument: str) -> Dict[str, float]:
    inst = str(instrument or "").strip().upper().replace("/", "_")
    base, quote = split_pair(inst)
    vol = load_technical_volatility_table().get(inst, {})
    percentile = safe_float(vol.get("volatility_percentile"), 0.0)
    is_major = inst in SEVEN_MAJOR_PAIRS
    is_usd = base == "USD" or quote == "USD"
    is_jpy = base == "JPY" or quote == "JPY"
    is_exotic = base in EXOTIC_OR_VOLATILE_CURRENCIES or quote in EXOTIC_OR_VOLATILE_CURRENCIES
    is_volatile = percentile >= VOLATILE_PAIR_MIN_PERCENTILE or is_exotic
    return {
        "instrument_volatility_percentile": percentile,
        "instrument_median_abs_60_pips": safe_float(
            vol.get("median_abs_60_pips"),
            0.0,
        ),
        "instrument_q995_abs_60_pips": safe_float(
            vol.get("q995_abs_60_pips"),
            0.0,
        ),
        "instrument_q995_move_to_spread": safe_float(
            vol.get("q995_move_to_spread"),
            0.0,
        ),
        "is_major_pair": float(is_major),
        "is_usd_pair": float(is_usd),
        "is_non_usd_pair": float(not is_usd),
        "is_jpy_cross": float(is_jpy),
        "is_exotic_pair": float(is_exotic),
        "is_volatile_pair": float(is_volatile),
        "is_volatile_or_exotic_pair": float(is_volatile or is_exotic),
        "is_non_usd_volatile_pair": float((not is_usd) and is_volatile),
    }


def pair_taxonomy_primary(instrument: str) -> str:
    inst = str(instrument or "").strip().upper().replace("/", "_")
    base, quote = split_pair(inst)
    is_usd = base == "USD" or quote == "USD"
    is_non_usd = not is_usd
    is_exotic = base in EXOTIC_OR_VOLATILE_CURRENCIES or quote in EXOTIC_OR_VOLATILE_CURRENCIES
    is_commodity = base in COMMODITY_CURRENCIES or quote in COMMODITY_CURRENCIES
    is_eur_gbp_cross = base in EUR_GBP_CURRENCIES and quote in EUR_GBP_CURRENCIES
    is_chf = inst in CHF_SAFE_HAVEN_PAIRS or base == "CHF" or quote == "CHF"
    vol = instrument_subset_flags(inst)
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


def pair_taxonomy_features(instrument: str) -> Dict[str, float]:
    primary = pair_taxonomy_primary(instrument)
    inst = str(instrument or "").strip().upper().replace("/", "_")
    base, quote = split_pair(inst)
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


def carry_proxy_features(instrument: str) -> Dict[str, float]:
    """Cross-sectional carry/rate-differential proxy without historical leakage.

    This is intentionally a broad structural bucket.  Exact/current rate context
    should come from the macro bias file and is stored separately as
    macro_rate_diff_bias.
    """
    inst = str(instrument or "").strip().upper().replace("/", "_")
    base, quote = split_pair(inst)
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


def add_trend_channel_features(frame: pd.DataFrame, instrument: str) -> pd.DataFrame:
    """Add trend, channel, and momentum-shape features derived only from past bars."""
    output = frame.copy()
    multiplier = pips_multiplier(instrument)
    close = pd.to_numeric(output["close"], errors="coerce")
    high = pd.to_numeric(output["high"], errors="coerce")
    low = pd.to_numeric(output["low"], errors="coerce")
    atr = pd.to_numeric(output["atr240_pips"], errors="coerce").clip(lower=0.1)

    ema8 = close.ewm(span=8, adjust=False, min_periods=8).mean()
    ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema21 = close.ewm(span=21, adjust=False, min_periods=21).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    ema55 = close.ewm(span=55, adjust=False, min_periods=30).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()

    output["ema8_minus_ema21_atr"] = ((ema8 - ema21) * multiplier) / atr
    output["ema21_slope_15_atr"] = ((ema21 - ema21.shift(3)) * multiplier) / atr
    output["ema55_slope_30_atr"] = ((ema55 - ema55.shift(6)) * multiplier) / atr
    output["macd_atr"] = (macd * multiplier) / atr
    output["macd_hist_atr"] = ((macd - macd_signal) * multiplier) / atr
    output["ma_stack_score"] = np.select(
        [
            (ema8 > ema21) & (ema21 > ema55),
            (ema8 < ema21) & (ema21 < ema55),
        ],
        [1.0, -1.0],
        default=0.0,
    )

    for label, bars in [("60", 12), ("240", 48)]:
        prior_high = high.shift(1).rolling(bars, min_periods=bars).max()
        prior_low = low.shift(1).rolling(bars, min_periods=bars).min()
        width = (prior_high - prior_low).replace(0, np.nan)
        output[f"donchian_{label}_position_centered"] = (
            ((close - prior_low) / width) - 0.5
        ) * 2.0
        breakout_up = ((close - prior_high) * multiplier / atr).clip(lower=0.0)
        breakout_down = ((prior_low - close) * multiplier / atr).clip(lower=0.0)
        output[f"donchian_{label}_breakout_atr"] = breakout_up - breakout_down

    trend_60 = close - close.shift(12)
    step = close.diff()
    trend_sign = np.sign(trend_60)
    same_direction = pd.Series(
        np.where(
            trend_sign >= 0,
            step > 0,
            step < 0,
        ),
        index=output.index,
    ).astype(float)
    output["trend_consistency_60"] = same_direction.rolling(
        12,
        min_periods=6,
    ).mean()
    log_return = np.log(close / close.shift(1))
    rv30 = log_return.rolling(6, min_periods=6).std()
    rv240 = log_return.rolling(48, min_periods=24).std()
    output["realized_vol_ratio_30_240"] = rv30 / rv240.replace(0, np.nan)
    return output


def add_expanded_indicator_features(
    frame: pd.DataFrame,
    instrument: str = "",
) -> pd.DataFrame:
    """Add causal indicator interactions for spike, scout, and exit-curve models.

    These columns are intentionally built only from row-time values or past-bar
    indicators that already exist in the technical research dataset.  They are
    model features and rule baselines, not standalone live execution permission.
    """
    out = frame.copy()
    eps = 1e-9
    definitions: Dict[str, Any] = {}

    def col(name: str, default: float = 0.0) -> pd.Series:
        if name not in out:
            return pd.Series(default, index=out.index, dtype=float)
        return pd.to_numeric(out[name], errors="coerce").fillna(default)

    m5 = col("momentum_5_atr")
    m15 = col("momentum_15_atr")
    m30 = col("momentum_30_atr")
    m60 = col("momentum_60_atr")
    spread_ratio = col("spread_ratio_60", 1.0)
    atr_pips = col("atr240_pips", 0.1).clip(lower=0.1)
    spread_pips = col("spread_pips", 0.0).abs()
    compression = col("compression_30", 1.0).clip(lower=0.0, upper=2.0)
    realized_vol_ratio = col("realized_vol_ratio_30_240", 1.0).clip(
        lower=0.0,
        upper=10.0,
    )
    rsi = col("rsi14_centered")
    macd_hist = col("macd_hist_atr")
    ma_stack = col("ma_stack_score")
    donchian60 = col("donchian_60_breakout_atr")
    donchian240 = col("donchian_240_breakout_atr")
    range60 = col("range_position_60_centered")
    range240 = col("range_position_240_centered")

    definitions["abs_momentum_5_atr"] = m5.abs()
    definitions["abs_momentum_15_atr"] = m15.abs()
    definitions["abs_momentum_30_atr"] = m30.abs()
    definitions["abs_momentum_60_atr"] = m60.abs()
    definitions["momentum_5_15_delta_atr"] = m5 - m15
    definitions["momentum_15_30_delta_atr"] = m15 - m30
    definitions["momentum_30_60_delta_atr"] = m30 - m60
    definitions["momentum_5_to_30_ratio"] = m5 / (m30.abs() + eps)
    definitions["momentum_15_to_60_ratio"] = m15 / (m60.abs() + eps)
    definitions["momentum_alignment_long"] = (
        (m5 > 0).astype(int)
        + (m15 > 0).astype(int)
        + (m30 > 0).astype(int)
        + (m60 > 0).astype(int)
    )
    definitions["momentum_alignment_short"] = (
        (m5 < 0).astype(int)
        + (m15 < 0).astype(int)
        + (m30 < 0).astype(int)
        + (m60 < 0).astype(int)
    )
    definitions["momentum_alignment_edge"] = (
        definitions["momentum_alignment_long"]
        - definitions["momentum_alignment_short"]
    )
    definitions["momentum_acceleration_pressure"] = (
        definitions["momentum_5_15_delta_atr"] * 0.55
        + definitions["momentum_15_30_delta_atr"] * 0.30
        + definitions["momentum_30_60_delta_atr"] * 0.15
    )
    definitions["momentum_exhaustion_pressure"] = (
        m30.abs().clip(lower=0.0, upper=6.0)
        * (-np.sign(m30) * definitions["momentum_5_15_delta_atr"]).clip(
            lower=0.0,
            upper=6.0,
        )
    )

    definitions["sma7_gt_sma8"] = (col("sma7_minus_8_atr") > 0).astype(int)
    definitions["sma7_lt_sma8"] = (col("sma7_minus_8_atr") < 0).astype(int)
    definitions["sma30_slope_gt_1_atr"] = (
        col("sma30_slope_5_atr") > 1.0
    ).astype(int)
    definitions["sma30_slope_lt_minus_1_atr"] = (
        col("sma30_slope_5_atr") < -1.0
    ).astype(int)
    definitions["ema8_gt_ema21"] = (
        col("ema8_minus_ema21_atr") > 0
    ).astype(int)
    definitions["ema8_lt_ema21"] = (
        col("ema8_minus_ema21_atr") < 0
    ).astype(int)
    definitions["macd_hist_positive"] = (macd_hist > 0).astype(int)
    definitions["macd_hist_negative"] = (macd_hist < 0).astype(int)
    definitions["macd_hist_abs_atr"] = macd_hist.abs()
    definitions["trend_pressure_long"] = (
        m5.clip(lower=0.0) * 0.25
        + m15.clip(lower=0.0) * 0.25
        + m30.clip(lower=0.0) * 0.20
        + col("ema8_minus_ema21_atr").clip(lower=0.0) * 0.15
        + macd_hist.clip(lower=0.0) * 0.10
        + ma_stack.clip(lower=0.0) * 0.20
    )
    definitions["trend_pressure_short"] = (
        (-m5).clip(lower=0.0) * 0.25
        + (-m15).clip(lower=0.0) * 0.25
        + (-m30).clip(lower=0.0) * 0.20
        + (-col("ema8_minus_ema21_atr")).clip(lower=0.0) * 0.15
        + (-macd_hist).clip(lower=0.0) * 0.10
        + (-ma_stack).clip(lower=0.0) * 0.20
    )
    definitions["trend_pressure_edge"] = (
        definitions["trend_pressure_long"]
        - definitions["trend_pressure_short"]
    )
    definitions["rsi_overbought_pressure"] = (rsi - 0.20).clip(
        lower=0.0,
        upper=2.0,
    )
    definitions["rsi_oversold_pressure"] = (-rsi - 0.20).clip(
        lower=0.0,
        upper=2.0,
    )
    definitions["rsi_extreme_abs"] = rsi.abs()

    definitions["donchian60_breakout_long"] = (donchian60 > 0.5).astype(int)
    definitions["donchian60_breakout_short"] = (donchian60 < -0.5).astype(int)
    definitions["donchian240_breakout_long"] = (donchian240 > 0.5).astype(int)
    definitions["donchian240_breakout_short"] = (donchian240 < -0.5).astype(int)
    definitions["donchian_breakout_abs_atr"] = np.maximum(
        donchian60.abs(),
        donchian240.abs(),
    )
    definitions["range60_extreme_abs"] = range60.abs()
    definitions["range240_extreme_abs"] = range240.abs()
    definitions["near_upper_range_score"] = np.maximum.reduce([
        range60.clip(lower=0.0),
        range240.clip(lower=0.0),
        col("donchian_60_position_centered").clip(lower=0.0),
        col("donchian_240_position_centered").clip(lower=0.0),
    ])
    definitions["near_lower_range_score"] = np.maximum.reduce([
        (-range60).clip(lower=0.0),
        (-range240).clip(lower=0.0),
        (-col("donchian_60_position_centered")).clip(lower=0.0),
        (-col("donchian_240_position_centered")).clip(lower=0.0),
    ])
    definitions["range_breakout_pressure_long"] = (
        definitions["near_upper_range_score"]
        * (m5.clip(lower=-0.25) + 0.25).clip(lower=0.0)
    )
    definitions["range_breakout_pressure_short"] = (
        definitions["near_lower_range_score"]
        * ((-m5).clip(lower=-0.25) + 0.25).clip(lower=0.0)
    )

    definitions["strength_gap_abs_15"] = col("strength_gap_15").abs()
    definitions["strength_gap_abs_60"] = col("strength_gap_60").abs()
    definitions["strength_gap_15_60_delta"] = (
        col("strength_gap_15") - col("strength_gap_60")
    )
    definitions["atr_spread_efficiency"] = atr_pips / (spread_pips + eps)
    definitions["spread_to_atr240"] = spread_pips / atr_pips
    definitions["spread_cost_pressure"] = (
        spread_ratio * col("regime_spread_cost_score", 1.0)
    )
    definitions["compression_breakout_pressure"] = (
        (1.0 - compression.clip(lower=0.0, upper=1.0))
        * realized_vol_ratio
    )
    definitions["compression_then_expansion"] = (
        (compression <= 0.65).astype(int)
        * (realized_vol_ratio >= 1.05).astype(int)
    )
    definitions["volatility_expansion_pressure"] = np.maximum(
        realized_vol_ratio - 1.0,
        col("regime_volatility_expansion_score"),
    )
    definitions["liquidity_quality_score"] = (
        np.log1p(definitions["atr_spread_efficiency"].clip(lower=0.0))
        - spread_ratio.clip(lower=0.0)
        - col("is_rollover_hour") * 1.25
        - col("regime_is_spread_impaired") * 1.25
    )
    definitions["anti_chase_pressure"] = np.maximum.reduce([
        (spread_ratio - 1.65).clip(lower=0.0),
        (m60.abs() - 3.25).clip(lower=0.0),
        col("is_rollover_hour"),
        col("regime_is_spread_impaired"),
    ])
    definitions["london_volatility_pressure"] = (
        col("is_london_session")
        * col("regime_volatility_expansion_score")
    )
    definitions["ny_volatility_pressure"] = (
        col("is_new_york_session")
        * col("regime_volatility_expansion_score")
    )
    definitions["overlap_breakout_pressure"] = (
        col("is_london_ny_overlap")
        * definitions["compression_breakout_pressure"]
    )
    definitions["rollover_spread_penalty"] = (
        col("is_rollover_hour") * spread_ratio
    )
    definitions["volatile_spread_penalty"] = (
        col("is_volatile_or_exotic_pair") * spread_ratio
    )
    definitions["risk_on_long_pressure"] = (
        col("regime_risk_on_score") * col("base_is_risk")
    )
    definitions["risk_off_jpy_chf_pressure"] = (
        col("regime_is_risk_off")
        * (
            col("quote_is_jpy")
            + col("quote_is_chf")
            + col("base_is_jpy")
            + col("base_is_chf")
        )
    )
    definitions["carry_trend_alignment"] = (
        col("carry_proxy_diff") * np.sign(definitions["trend_pressure_edge"])
    )
    definitions["carry_extreme_pressure"] = (
        col("carry_proxy_abs_diff") * col("is_volatile_or_exotic_pair")
    )
    definitions["macro_bias_trend_alignment"] = (
        col("macro_pair_bias") * np.sign(definitions["trend_pressure_edge"])
    )
    definitions["macro_event_spread_pressure"] = (
        col("macro_event_risk") * spread_ratio
    )

    liquidity_ok = (
        (spread_ratio <= 1.65)
        & (definitions["atr_spread_efficiency"] >= 2.0)
        & (col("is_rollover_hour") < 0.5)
        & (col("regime_is_spread_impaired") < 0.5)
    ).astype(int)
    anti_chase = (
        (spread_ratio > 2.0)
        | (col("is_rollover_hour") > 0.5)
        | (col("regime_is_spread_impaired") > 0.5)
        | (m60.abs() > 3.25)
    ).astype(int)
    compression_ready = (
        (compression <= 0.55)
        & (realized_vol_ratio <= 1.15)
        & (spread_ratio <= 1.5)
    ).astype(int)
    vol_expanding = (
        (realized_vol_ratio >= 1.05)
        | (col("regime_volatility_expansion_score") >= 0.5)
        | (col("regime_is_high_vol_event") > 0.5)
    ).astype(int)
    active_session = (
        (col("is_london_session") > 0.5)
        | (col("is_new_york_session") > 0.5)
        | (col("is_london_ny_overlap") > 0.5)
    ).astype(int)
    near_upper_range = (
        (definitions["near_upper_range_score"] >= 0.25)
    ).astype(int)
    near_lower_range = (
        (definitions["near_lower_range_score"] >= 0.25)
    ).astype(int)
    trend_long = (
        (definitions["momentum_alignment_long"] >= 3)
        & (definitions["ema8_gt_ema21"] > 0)
        & ((ma_stack > 0) | (definitions["macd_hist_positive"] > 0))
    ).astype(int)
    trend_short = (
        (definitions["momentum_alignment_short"] >= 3)
        & (definitions["ema8_lt_ema21"] > 0)
        & ((ma_stack < 0) | (definitions["macd_hist_negative"] > 0))
    ).astype(int)
    cross_long = (
        (col("strength_gap_15") > 0)
        & (col("strength_gap_60") > -0.25)
        & (
            (col("macro_pair_bias") >= 0)
            | (col("regime_risk_on_score") > 0.25)
            | (col("regime_usd_strength_15") > 0)
        )
    ).astype(int)
    cross_short = (
        (col("strength_gap_15") < 0)
        & (col("strength_gap_60") < 0.25)
        & (
            (col("macro_pair_bias") <= 0)
            | (col("regime_is_risk_off") > 0.5)
            | (col("regime_usd_strength_15") < 0)
        )
    ).astype(int)
    exhaustion_long = (
        near_lower_range.astype(bool)
        & (m30 < -1.2)
        & (definitions["momentum_5_15_delta_atr"] > 0)
        & (rsi <= -0.15)
    ).astype(int)
    exhaustion_short = (
        near_upper_range.astype(bool)
        & (m30 > 1.2)
        & (definitions["momentum_5_15_delta_atr"] < 0)
        & (rsi >= 0.15)
    ).astype(int)

    definitions["rule_pre_breakout_long"] = (
        compression_ready.astype(bool)
        & near_upper_range.astype(bool)
        & (m5 > -0.20)
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_pre_breakout_short"] = (
        compression_ready.astype(bool)
        & near_lower_range.astype(bool)
        & (m5 < 0.20)
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_breakout_continuation_long"] = (
        trend_long.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_long"] > 0)
            | (definitions["donchian240_breakout_long"] > 0)
            | (col("regime_acceleration_score") > 0.3)
        )
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_breakout_continuation_short"] = (
        trend_short.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_short"] > 0)
            | (definitions["donchian240_breakout_short"] > 0)
            | (col("regime_acceleration_score") < -0.3)
        )
        & liquidity_ok.astype(bool)
    ).astype(int)
    definitions["rule_exhaustion_reversal_long"] = exhaustion_long
    definitions["rule_exhaustion_reversal_short"] = exhaustion_short
    definitions["rule_session_timing_long"] = (
        active_session.astype(bool)
        & (definitions["momentum_alignment_long"] >= 2)
        & (spread_ratio <= 1.8)
    ).astype(int)
    definitions["rule_session_timing_short"] = (
        active_session.astype(bool)
        & (definitions["momentum_alignment_short"] >= 2)
        & (spread_ratio <= 1.8)
    ).astype(int)
    definitions["rule_cross_pressure_long"] = cross_long
    definitions["rule_cross_pressure_short"] = cross_short
    definitions["rule_liquidity_ok"] = liquidity_ok
    definitions["rule_anti_chase_penalty"] = anti_chase
    value_liquidity_ok = (
        (definitions["atr_spread_efficiency"] >= 1.35)
        & (spread_ratio <= 2.25)
        & (col("is_rollover_hour") < 0.5)
        & (spread_pips <= (atr_pips * 0.90))
    ).astype(int)
    scout_session_ok = (
        active_session.astype(bool)
        | (col("is_asia_session") > 0.5)
        | (col("is_rollover_hour") < 0.5)
    ).astype(int)
    late_momentum_long = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & trend_long.astype(bool)
        & (m5 >= 0.30)
        & (m15 >= 0.45)
        & (m60.abs() <= 4.25)
    ).astype(int)
    late_momentum_short = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & trend_short.astype(bool)
        & (m5 <= -0.30)
        & (m15 <= -0.45)
        & (m60.abs() <= 4.25)
    ).astype(int)
    value_breakout_long = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_long"] > 0)
            | (definitions["donchian240_breakout_long"] > 0)
            | (col("regime_acceleration_score") > 0.35)
            | ((near_upper_range > 0) & (definitions["momentum_alignment_long"] >= 3))
        )
        & (m5 > -0.10)
    ).astype(int)
    value_breakout_short = (
        value_liquidity_ok.astype(bool)
        & scout_session_ok.astype(bool)
        & vol_expanding.astype(bool)
        & (
            (definitions["donchian60_breakout_short"] > 0)
            | (definitions["donchian240_breakout_short"] > 0)
            | (col("regime_acceleration_score") < -0.35)
            | ((near_lower_range > 0) & (definitions["momentum_alignment_short"] >= 3))
        )
        & (m5 < 0.10)
    ).astype(int)
    guarded_exhaustion_long = (
        exhaustion_long.astype(bool)
        & value_liquidity_ok.astype(bool)
        & (m60 > -4.25)
        & (spread_ratio <= 2.00)
    ).astype(int)
    guarded_exhaustion_short = (
        exhaustion_short.astype(bool)
        & value_liquidity_ok.astype(bool)
        & (m60 < 4.25)
        & (spread_ratio <= 2.00)
    ).astype(int)
    definitions["rule_value_liquidity_ok"] = value_liquidity_ok
    definitions["rule_late_momentum_confirmation_long"] = late_momentum_long
    definitions["rule_late_momentum_confirmation_short"] = late_momentum_short
    definitions["rule_value_breakout_long"] = value_breakout_long
    definitions["rule_value_breakout_short"] = value_breakout_short
    definitions["rule_guarded_exhaustion_reversal_long"] = guarded_exhaustion_long
    definitions["rule_guarded_exhaustion_reversal_short"] = guarded_exhaustion_short
    definitions["rule_scout_value_long_score"] = (
        value_breakout_long * 1.35
        + late_momentum_long * 1.15
        + guarded_exhaustion_long * 0.75
        + cross_long * 0.45
        + value_liquidity_ok * 0.40
        - anti_chase * 1.10
    )
    definitions["rule_scout_value_short_score"] = (
        value_breakout_short * 1.35
        + late_momentum_short * 1.15
        + guarded_exhaustion_short * 0.75
        + cross_short * 0.45
        + value_liquidity_ok * 0.40
        - anti_chase * 1.10
    )
    definitions["rule_baseline_long_score"] = (
        definitions["rule_pre_breakout_long"] * 1.15
        + definitions["rule_breakout_continuation_long"] * 1.35
        + definitions["rule_exhaustion_reversal_long"] * 0.85
        + definitions["rule_session_timing_long"] * 0.55
        + definitions["rule_cross_pressure_long"] * 0.90
        + definitions["rule_liquidity_ok"] * 0.35
        + definitions["rule_scout_value_long_score"] * 0.20
        - definitions["rule_anti_chase_penalty"] * 1.40
    )
    definitions["rule_baseline_short_score"] = (
        definitions["rule_pre_breakout_short"] * 1.15
        + definitions["rule_breakout_continuation_short"] * 1.35
        + definitions["rule_exhaustion_reversal_short"] * 0.85
        + definitions["rule_session_timing_short"] * 0.55
        + definitions["rule_cross_pressure_short"] * 0.90
        + definitions["rule_liquidity_ok"] * 0.35
        + definitions["rule_scout_value_short_score"] * 0.20
        - definitions["rule_anti_chase_penalty"] * 1.40
    )
    definitions["rule_baseline_direction_score"] = (
        definitions["rule_baseline_long_score"]
        - definitions["rule_baseline_short_score"]
    )
    definitions["rule_baseline_strength_score"] = np.maximum(
        definitions["rule_baseline_long_score"],
        definitions["rule_baseline_short_score"],
    )
    definitions["rule_baseline_long_candidate"] = (
        (definitions["rule_baseline_long_score"] >= 2.0)
        & (
            definitions["rule_baseline_long_score"]
            > definitions["rule_baseline_short_score"]
        )
    ).astype(int)
    definitions["rule_baseline_short_candidate"] = (
        (definitions["rule_baseline_short_score"] >= 2.0)
        & (
            definitions["rule_baseline_short_score"]
            > definitions["rule_baseline_long_score"]
        )
    ).astype(int)

    feature_columns: Dict[str, pd.Series] = {}
    for name, values in definitions.items():
        series = (
            values.reindex(out.index)
            if isinstance(values, pd.Series)
            else pd.Series(values, index=out.index)
        )
        feature_columns[name] = (
            pd.to_numeric(series, errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
        )
    feature_frame = pd.DataFrame(feature_columns, index=out.index)
    existing = [column for column in feature_frame.columns if column in out.columns]
    if existing:
        out = out.drop(columns=existing)
    return pd.concat([out, feature_frame], axis=1)


def currency_oriented_strength(
    frame: pd.DataFrame,
    instrument: str,
    currency: str,
    horizon: int,
) -> pd.Series:
    inst = str(instrument or "").strip().upper().replace("/", "_")
    base, quote = split_pair(inst)
    gap = pd.to_numeric(
        frame.get(f"strength_gap_{horizon}", pd.Series(0.0, index=frame.index)),
        errors="coerce",
    ).fillna(0.0)
    if base == currency:
        return gap
    if quote == currency:
        return -gap
    return pd.Series(0.0, index=frame.index)


def add_regime_features(frame: pd.DataFrame, instrument: str) -> pd.DataFrame:
    output = frame.copy()
    m15 = pd.to_numeric(output.get("momentum_15_atr"), errors="coerce").fillna(0.0)
    m30 = pd.to_numeric(output.get("momentum_30_atr"), errors="coerce").fillna(0.0)
    m60 = pd.to_numeric(output.get("momentum_60_atr"), errors="coerce").fillna(0.0)
    accel = pd.to_numeric(output.get("acceleration_15_atr"), errors="coerce").fillna(0.0)
    atr_ratio = pd.to_numeric(output.get("atr15_to_atr240"), errors="coerce").fillna(1.0)
    compression = pd.to_numeric(output.get("compression_30"), errors="coerce").fillna(1.0)
    spread_ratio = pd.to_numeric(output.get("spread_ratio_60"), errors="coerce").fillna(1.0)
    spread_pips = pd.to_numeric(output.get("spread_pips"), errors="coerce").fillna(0.0)
    atr240 = pd.to_numeric(output.get("atr240_pips"), errors="coerce").clip(lower=0.1)
    base, quote = split_pair(instrument)

    usd_15 = currency_oriented_strength(output, instrument, "USD", 15)
    usd_60 = currency_oriented_strength(output, instrument, "USD", 60)
    jpy_15 = currency_oriented_strength(output, instrument, "JPY", 15)
    chf_15 = currency_oriented_strength(output, instrument, "CHF", 15)
    base_risk = float(base in RISK_CURRENCIES)
    quote_risk = float(quote in RISK_CURRENCIES)
    base_commodity = float(base in COMMODITY_CURRENCIES)
    quote_commodity = float(quote in COMMODITY_CURRENCIES)
    risk_strength_15 = (
        (base_risk - quote_risk)
        * pd.to_numeric(output.get("strength_gap_15"), errors="coerce").fillna(0.0)
    )
    commodity_strength_15 = (
        (base_commodity - quote_commodity)
        * pd.to_numeric(output.get("strength_gap_15"), errors="coerce").fillna(0.0)
    )
    risk_haven_pressure = risk_strength_15 - (
        0.5 * jpy_15 + 0.5 * chf_15
    )

    trend_strength = (m15.abs() + m30.abs() + m60.abs()) / 3.0
    spread_to_atr = spread_pips / atr240
    output["regime_trend_strength"] = trend_strength
    output["regime_volatility_expansion_score"] = atr_ratio
    output["regime_compression_score"] = compression
    output["regime_spread_cost_score"] = spread_to_atr
    output["regime_usd_strength_15"] = usd_15
    output["regime_usd_strength_60"] = usd_60
    output["regime_jpy_strength_15"] = jpy_15
    output["regime_chf_strength_15"] = chf_15
    output["regime_risk_on_score"] = risk_haven_pressure
    output["regime_commodity_score"] = commodity_strength_15
    output["regime_acceleration_score"] = accel
    output["regime_is_usd_trend"] = ((usd_15.abs() >= 0.00035) | (usd_60.abs() >= 0.00045)).astype(int)
    output["regime_is_risk_on"] = (risk_haven_pressure >= 0.00025).astype(int)
    output["regime_is_risk_off"] = (risk_haven_pressure <= -0.00025).astype(int)
    output["regime_is_jpy_unwind"] = (jpy_15.abs() >= 0.00035).astype(int)
    output["regime_is_chf_safe_haven"] = (chf_15 >= 0.00025).astype(int)
    output["regime_is_commodity_trend"] = (commodity_strength_15.abs() >= 0.00025).astype(int)
    output["regime_is_low_vol_chop"] = (
        (trend_strength <= 0.35)
        & (atr_ratio <= 0.90)
        & (compression <= 0.80)
    ).astype(int)
    output["regime_is_high_vol_event"] = (
        (atr_ratio >= 1.35)
        | (trend_strength >= 1.25)
        | (accel.abs() >= 0.70)
    ).astype(int)
    output["regime_is_spread_impaired"] = (
        (spread_ratio >= 1.50)
        | (spread_to_atr >= 0.35)
    ).astype(int)
    regime_flags = {
        "usd_trend": output["regime_is_usd_trend"],
        "risk_on": output["regime_is_risk_on"],
        "risk_off": output["regime_is_risk_off"],
        "jpy_unwind": output["regime_is_jpy_unwind"],
        "chf_safe_haven": output["regime_is_chf_safe_haven"],
        "commodity_trend": output["regime_is_commodity_trend"],
        "low_vol_chop": output["regime_is_low_vol_chop"],
        "high_vol_event": output["regime_is_high_vol_event"],
        "spread_impaired": output["regime_is_spread_impaired"],
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
    primary = pd.Series("neutral", index=output.index, dtype=object)
    for name in reversed(priority):
        primary = np.where(regime_flags[name].astype(bool), name, primary)
    output["regime_primary"] = primary
    for name in REGIME_CLASSES:
        output[f"regime_primary_{name}"] = (output["regime_primary"] == name).astype(int)
    return output


def neutral_macro_bias_features(instrument: str) -> Dict[str, float]:
    return {
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
    }


def add_calendar_features(frame: pd.DataFrame, time_column: str = "time_utc") -> pd.DataFrame:
    """Add UTC calendar/session features; labels still remain future-only."""
    output = frame.copy()
    ts = pd.to_datetime(output[time_column], errors="coerce", utc=True)
    hour_float = ts.dt.hour + ts.dt.minute / 60.0
    day = ts.dt.day.clip(lower=1)
    days_in_month = ts.dt.days_in_month.replace(0, 31)
    weekday = ts.dt.weekday
    month = ts.dt.month
    day_of_year = ts.dt.dayofyear
    iso = ts.dt.isocalendar()
    week_of_year = iso.week.astype(float)
    output["hour_sin"] = np.sin(2.0 * np.pi * hour_float / 24.0)
    output["hour_cos"] = np.cos(2.0 * np.pi * hour_float / 24.0)
    output["weekday"] = weekday
    output["weekday_sin"] = np.sin(2.0 * np.pi * weekday / 7.0)
    output["weekday_cos"] = np.cos(2.0 * np.pi * weekday / 7.0)
    output["month"] = month
    output["month_sin"] = np.sin(2.0 * np.pi * (month - 1) / 12.0)
    output["month_cos"] = np.cos(2.0 * np.pi * (month - 1) / 12.0)
    output["day_of_month_sin"] = np.sin(2.0 * np.pi * (day - 1) / days_in_month)
    output["day_of_month_cos"] = np.cos(2.0 * np.pi * (day - 1) / days_in_month)
    output["day_of_year_sin"] = np.sin(2.0 * np.pi * day_of_year / 366.0)
    output["day_of_year_cos"] = np.cos(2.0 * np.pi * day_of_year / 366.0)
    output["week_of_year_sin"] = np.sin(2.0 * np.pi * week_of_year / 53.0)
    output["week_of_year_cos"] = np.cos(2.0 * np.pi * week_of_year / 53.0)
    output["week_of_month"] = ((day - 1) // 7 + 1).astype(float)
    output["is_month_start"] = ts.dt.is_month_start.astype(int)
    output["is_month_end"] = ts.dt.is_month_end.astype(int)
    output["is_quarter_end"] = ts.dt.is_quarter_end.astype(int)
    output["is_asia_session"] = ((hour_float >= 0.0) & (hour_float < 7.0)).astype(int)
    output["is_london_session"] = ((hour_float >= 7.0) & (hour_float < 16.0)).astype(int)
    output["is_new_york_session"] = ((hour_float >= 12.0) & (hour_float < 21.0)).astype(int)
    output["is_london_ny_overlap"] = ((hour_float >= 12.0) & (hour_float < 16.0)).astype(int)
    output["is_rollover_hour"] = ((hour_float >= 21.0) & (hour_float < 22.0)).astype(int)
    return output


def apply_instrument_subset(frame: pd.DataFrame, subset: str) -> pd.DataFrame:
    subset = str(subset or "all").lower()
    if subset == "majors":
        return frame[frame["is_major_pair"].astype(float).eq(1.0)]
    if subset == "usd_pairs":
        return frame[frame["is_usd_pair"].astype(float).eq(1.0)]
    if subset == "non_usd":
        return frame[frame["is_non_usd_pair"].astype(float).eq(1.0)]
    if subset == "volatile":
        return frame[frame["is_volatile_pair"].astype(float).eq(1.0)]
    if subset == "exotic":
        return frame[frame["is_exotic_pair"].astype(float).eq(1.0)]
    if subset == "volatile_exotic":
        return frame[frame["is_volatile_or_exotic_pair"].astype(float).eq(1.0)]
    if subset == "non_usd_volatile":
        return frame[frame["is_non_usd_volatile_pair"].astype(float).eq(1.0)]
    if subset in PAIR_TAXONOMY_CLASSES and "pair_taxonomy_primary" in frame:
        return frame[frame["pair_taxonomy_primary"].astype(str).eq(subset)]
    return frame


def apply_instrument_whitelist(
    frame: pd.DataFrame,
    instruments: Any,
) -> pd.DataFrame:
    whitelist = normalized_instrument_whitelist(instruments)
    if not whitelist or "instrument" not in frame:
        return frame
    instrument_series = (
        frame["instrument"]
        .astype(str)
        .str.upper()
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return frame[instrument_series.isin(set(whitelist))]


SEGMENT_FILTER_ALIASES = {
    "pair_family": "pair_families",
    "pair_families": "pair_families",
    "families": "pair_families",
    "regime": "regimes",
    "regimes": "regimes",
    "session": "sessions",
    "sessions": "sessions",
    "exclude_pair_family": "exclude_pair_families",
    "exclude_pair_families": "exclude_pair_families",
    "exclude_families": "exclude_pair_families",
    "exclude_regime": "exclude_regimes",
    "exclude_regimes": "exclude_regimes",
    "exclude_session": "exclude_sessions",
    "exclude_sessions": "exclude_sessions",
}


def normalized_segment_filters(value: Any) -> Dict[str, List[str]]:
    """Normalize optional offline row filters for account-specific research.

    These filters only affect research datasets.  They do not change live
    managers or broker execution.  Supported dimensions intentionally mirror
    scorecard dimensions: pair family, regime, and session.
    """
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        value = parsed
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, List[str]] = {}
    for raw_key, raw_values in value.items():
        key = SEGMENT_FILTER_ALIASES.get(str(raw_key or "").strip().lower())
        if not key:
            continue
        if isinstance(raw_values, str):
            values = [part.strip() for part in raw_values.replace(";", ",").split(",")]
        elif isinstance(raw_values, (list, tuple, set)):
            values = [str(part).strip() for part in raw_values]
        else:
            continue
        cleaned = sorted({
            item.lower().replace("-", "_").replace(" ", "_")
            for item in values
            if item
        })
        if cleaned:
            normalized[key] = cleaned
    return normalized


def segment_filter_columns(filters: Any) -> List[str]:
    normalized = normalized_segment_filters(filters)
    columns = []
    if normalized.get("sessions") or normalized.get("exclude_sessions"):
        columns.extend([
            "is_asia_session",
            "is_london_session",
            "is_new_york_session",
            "is_london_ny_overlap",
            "is_rollover_hour",
        ])
    if normalized.get("regimes") or normalized.get("exclude_regimes"):
        columns.append("regime_primary")
    if normalized.get("pair_families") or normalized.get("exclude_pair_families"):
        columns.append("pair_taxonomy_primary")
    return list(dict.fromkeys(columns))


def _session_mask(frame: pd.DataFrame, sessions: List[str]) -> pd.Series:
    if not sessions:
        return pd.Series(True, index=frame.index)
    false_mask = pd.Series(False, index=frame.index)

    def numeric(column: str) -> pd.Series:
        if column not in frame:
            return false_mask
        return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    asia = numeric("is_asia_session") >= 0.5
    london = numeric("is_london_session") >= 0.5
    new_york = numeric("is_new_york_session") >= 0.5
    overlap = numeric("is_london_ny_overlap") >= 0.5
    rollover = numeric("is_rollover_hour") >= 0.5
    mapping = {
        "asia": asia,
        "london": london & ~overlap,
        "new_york": new_york & ~overlap,
        "london_ny_overlap": overlap,
        "overlap": overlap,
        "off_session": ~(asia | london | new_york | overlap),
        "rollover": rollover,
    }
    mask = false_mask.copy()
    for session in sessions:
        mask = mask | mapping.get(session, false_mask)
    return mask


def apply_segment_filters(frame: pd.DataFrame, filters: Any) -> pd.DataFrame:
    normalized = normalized_segment_filters(filters)
    if not normalized or frame.empty:
        return frame
    mask = pd.Series(True, index=frame.index)
    pair_families = normalized.get("pair_families", [])
    if pair_families and "pair_taxonomy_primary" in frame:
        mask &= frame["pair_taxonomy_primary"].astype(str).str.lower().isin(pair_families)
    exclude_pair_families = normalized.get("exclude_pair_families", [])
    if exclude_pair_families and "pair_taxonomy_primary" in frame:
        mask &= ~frame["pair_taxonomy_primary"].astype(str).str.lower().isin(exclude_pair_families)
    regimes = normalized.get("regimes", [])
    if regimes and "regime_primary" in frame:
        mask &= frame["regime_primary"].astype(str).str.lower().isin(regimes)
    exclude_regimes = normalized.get("exclude_regimes", [])
    if exclude_regimes and "regime_primary" in frame:
        mask &= ~frame["regime_primary"].astype(str).str.lower().isin(exclude_regimes)
    sessions = normalized.get("sessions", [])
    if sessions:
        mask &= _session_mask(frame, sessions)
    exclude_sessions = normalized.get("exclude_sessions", [])
    if exclude_sessions:
        mask &= ~_session_mask(frame, exclude_sessions)
    return frame[mask]


def top_pair_trade_share_limit_for_spec(spec: Dict[str, Any]) -> float:
    whitelist = normalized_instrument_whitelist(spec.get("instrument_whitelist"))
    if len(whitelist) == 1:
        return 1.000001
    if len(whitelist) <= 3 and whitelist:
        return 0.80
    if len(whitelist) <= 5 and whitelist:
        return 0.65
    return MAX_VALIDATION_TOP_PAIR_TRADE_SHARE


def parse_oanda_time(ts: str) -> datetime:
    if not ts:
        return utc_now()
    try:
        # OANDA returns e.g. 2026-06-18T17:40:40.607536835Z
        ts2 = ts.replace("Z", "+00:00")
        if "." in ts2:
            head, tail = ts2.split(".", 1)
            frac, rest = tail[:6], tail[6:]
            rest = rest if rest.startswith("+") or rest.startswith("-") else "+00:00"
            ts2 = head + "." + frac + rest
        return datetime.fromisoformat(ts2).astimezone(timezone.utc)
    except Exception:
        return utc_now()


def market_is_likely_closed(dt: Optional[datetime] = None) -> bool:
    dt = dt or utc_now()
    # Approx: FX closes Friday 21:00 UTC and opens Sunday 21:00 UTC.
    wd = dt.weekday()  # Mon=0
    if wd == 5:
        return True
    if wd == 6 and dt.hour < 21:
        return True
    if wd == 4 and dt.hour >= 21:
        return True
    return False


def load_creds(path: Path) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    if not path.exists():
        return data
    text = path.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        name, val = s.split("=", 1)
        name = name.strip()
        val = val.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            continue
        try:
            data[name] = ast.literal_eval(val)
        except Exception:
            data[name] = val.strip('"').strip("'")
    for k, v in os.environ.items():
        if k.startswith("OANDA_") or k.startswith("OPENAI_"):
            data.setdefault(k, v)
    return data


def resolve_oanda_creds(creds: Dict[str, Any]) -> Tuple[str, str, str]:
    token = (
        creds.get("OANDA_API_KEY")
        or creds.get("OANDA_ACCESS_TOKEN")
        or creds.get("OANDA_TOKEN")
        or creds.get("OANDA_API_TOKEN")
        or os.environ.get("OANDA_API_KEY")
        or os.environ.get("OANDA_ACCESS_TOKEN")
        or ""
    )
    env = str(creds.get("OANDA_ENV") or os.environ.get("OANDA_ENV") or "practice").lower().strip()
    if env not in {"practice", "live"}:
        env = "practice"
    if env == "live" and not ALLOW_LIVE_ENV:
        raise RuntimeError("OANDA_ENV=live detected, but ALLOW_LIVE_ENV=False in script. Refusing to run live.")
    base_url = "https://api-fxpractice.oanda.com" if env == "practice" else "https://api-fxtrade.oanda.com"
    account_id = str(creds.get("OANDA_ACCOUNT_ID_DUM1") or DEFAULT_DUM1_ACCOUNT)
    if not token:
        raise RuntimeError(f"Missing OANDA API key/token in creds: {CREDS_PATH}")
    return token, base_url, account_id


def mask_account(account_id: str) -> str:
    # Account IDs are not as sensitive as tokens, but keep logs concise.
    return account_id

# -----------------------------------------------------------------------------
# OANDA client
# -----------------------------------------------------------------------------
class OandaClient:
    def __init__(self, token: str, base_url: str, account_id: str, timeout: float = 20.0):
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.account_id = account_id
        self.timeout = timeout

    @property
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        url = self.base_url + path
        start = time.time()
        try:
            resp = requests.request(method, url, headers=self.headers, timeout=self.timeout, **kwargs)
            latency_ms = int((time.time() - start) * 1000)
            if resp.text:
                try:
                    data = resp.json()
                except Exception:
                    data = {"raw_text": resp.text}
            else:
                data = {}
            data["_http_status"] = resp.status_code
            data["_latency_ms"] = latency_ms
            if resp.status_code >= 400:
                data["_error"] = True
                data["_error_text"] = resp.text[:1000]
            return data
        except Exception as exc:
            return {"_error": True, "_exception": repr(exc), "_latency_ms": int((time.time() - start) * 1000)}

    def get_account(self) -> Dict[str, Any]:
        return self.request("GET", f"/v3/accounts/{self.account_id}/summary")

    def get_account_details(self) -> Dict[str, Any]:
        return self.request("GET", f"/v3/accounts/{self.account_id}")

    def list_trades(self) -> Dict[str, Any]:
        return self.request("GET", f"/v3/accounts/{self.account_id}/openTrades")

    def list_pending_orders(self) -> Dict[str, Any]:
        return self.request("GET", f"/v3/accounts/{self.account_id}/pendingOrders")

    def list_instruments(self) -> List[str]:
        data = self.request("GET", f"/v3/accounts/{self.account_id}/instruments")
        out = []
        for inst in data.get("instruments", []) or []:
            name = inst.get("name")
            typ = inst.get("type")
            if name and (typ == "CURRENCY" or "_" in name):
                out.append(name)
        return sorted(set(out))

    def pricing(self, instruments: List[str]) -> Dict[str, Any]:
        ins = ",".join(instruments)
        return self.request("GET", f"/v3/accounts/{self.account_id}/pricing", params={"instruments": ins})

    def candles(
        self,
        instrument: str,
        granularity: str = "M1",
        count: int = 500,
        end_time: Optional[datetime] = None,
        price: str = "M",
    ) -> Dict[str, Any]:
        params = {
            "price": price,
            "granularity": granularity,
            "count": str(min(max(count, 10), 5000)),
        }
        if end_time is not None:
            params["to"] = end_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return self.request(
            "GET",
            f"/v3/instruments/{instrument}/candles",
            params=params,
        )

    def create_market_order(
        self,
        instrument: str,
        units: int,
        take_profit_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        trailing_stop_distance: Optional[float] = None,
        client_tag: str = "dum1_training_manager",
    ) -> Dict[str, Any]:
        order: Dict[str, Any] = {
            "type": "MARKET",
            "instrument": instrument,
            "units": str(int(units)),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "clientExtensions": {"tag": client_tag, "comment": SCRIPT_VERSION[:128]},
        }
        if take_profit_price is not None and math.isfinite(take_profit_price):
            order["takeProfitOnFill"] = {"price": f"{take_profit_price:.5f}" if "JPY" not in instrument else f"{take_profit_price:.3f}"}
        if stop_loss_price is not None and math.isfinite(stop_loss_price):
            order["stopLossOnFill"] = {"price": f"{stop_loss_price:.5f}" if "JPY" not in instrument else f"{stop_loss_price:.3f}"}
        if trailing_stop_distance is not None and math.isfinite(trailing_stop_distance) and trailing_stop_distance > 0:
            order["trailingStopLossOnFill"] = {"distance": f"{trailing_stop_distance:.5f}" if "JPY" not in instrument else f"{trailing_stop_distance:.3f}"}
        return self.request("POST", f"/v3/accounts/{self.account_id}/orders", json={"order": order})

    def close_trade(self, trade_id: str, units: str = "ALL") -> Dict[str, Any]:
        return self.request("PUT", f"/v3/accounts/{self.account_id}/trades/{trade_id}/close", json={"units": units})

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return self.request("PUT", f"/v3/accounts/{self.account_id}/orders/{order_id}/cancel")

# -----------------------------------------------------------------------------
# Strategy structures
# -----------------------------------------------------------------------------
@dataclass
class StrategyProfile:
    strategy_id: str
    name: str
    family: str
    version: int
    model_type: str = "rule_profile"
    instrument_scope: str = "ALL"
    theme_scope: str = "ALL"
    entry_score_min: float = 82.0
    high_confidence_score: float = 90.0
    move_spread_ratio_min: float = 2.2
    momentum_pips_min: float = 4.0
    acceleration_min: float = 0.0
    volatility_min: float = 1.0
    max_margin_pct: float = DUM1_MAX_MARGIN_PCT
    hard_margin_pct: float = DUM1_HARD_MARGIN_PCT
    risk_per_trade_pct: float = DUM1_RISK_PER_TRADE_PCT
    max_open_trades: int = DUM1_MAX_OPEN_TRADES
    max_same_pair_trades: int = 1
    same_pair_cooldown_minutes: int = 30
    loss_cooldown_minutes: int = 45
    stop_pips: float = 12.0
    take_profit_pips: float = 24.0
    trailing_stop_pips: float = 10.0
    prune_stale_minutes: int = 45
    prune_red_minutes: int = 20
    allow_compound: bool = True
    add_only_if_profit_usd: float = 0.05
    max_adds_per_pair: int = 1
    gpt_filter: bool = False
    notes: str = ""
    parameters_json: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        d = asdict(self)
        d["parameters_json"] = dict(self.parameters_json or {})
        return d

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "StrategyProfile":
        base = StrategyProfile(strategy_id=d.get("strategy_id", "unknown"), name=d.get("name", "unknown"), family=d.get("family", "unknown"), version=safe_int(d.get("version", 1), 1))
        for f in dataclasses.fields(StrategyProfile):
            if f.name in d:
                setattr(base, f.name, d[f.name])
        return base


def default_active_strategy() -> StrategyProfile:
    return StrategyProfile(
        strategy_id="dum1_learner_best_expression_low_churn_v001",
        name="DUM1 learner best-expression low-churn v1",
        family="best_expression_low_churn",
        version=1,
        instrument_scope="ALL",
        entry_score_min=84.0,
        high_confidence_score=91.0,
        move_spread_ratio_min=2.4,
        momentum_pips_min=5.0,
        max_margin_pct=58.0,
        hard_margin_pct=70.0,
        risk_per_trade_pct=4.0,
        max_open_trades=6,
        max_same_pair_trades=1,
        same_pair_cooldown_minutes=35,
        stop_pips=12.0,
        take_profit_pips=27.0,
        trailing_stop_pips=10.0,
        prune_stale_minutes=45,
        prune_red_minutes=20,
        notes="Default learner strategy; candidate profiles can replace this after backtest/live validation.",
    )


def seed_candidate_profiles(active: StrategyProfile) -> List[StrategyProfile]:
    seeds = [active]
    templates = [
        ("low_churn_high_confidence", 90, 3.0, 6.5, 45, 3.0, 4, 14, 30, 12, "Ultra-selective high confidence profile."),
        ("aggressive_compound", 78, 1.8, 3.5, 68, 5.0, 10, 10, 24, 9, "Aggressive campaign profile; still no averaging down."),
        ("jpy_best_expression", 84, 2.0, 5.0, 62, 4.5, 7, 14, 30, 12, "JPY-aware best-expression profile."),
        ("usd_theme_pressure", 82, 2.2, 4.5, 60, 4.0, 7, 12, 26, 10, "USD theme pressure profile."),
        ("tight_prune_fast_cut", 82, 2.4, 4.0, 55, 3.5, 6, 9, 20, 8, "Fast-cut profile to reduce churn/margin clogging."),
        ("wide_stop_trend_capture", 86, 2.6, 7.0, 52, 3.0, 5, 20, 45, 18, "Wider-stop trend capture profile."),
        ("low_margin_micro_campaign", 83, 2.5, 4.0, 38, 2.0, 4, 10, 22, 8, "Low-margin tiny campaign profile."),
        ("pressure_reversal_fade", 88, 2.8, 6.0, 45, 2.5, 4, 8, 16, 6, "Contrarian/fade seed profile; tested by backtest before use."),
    ]
    for i, (fam, entry, ratio, mom, margin, risk, maxtr, sl, tp, trail, notes) in enumerate(templates, 1):
        seeds.append(StrategyProfile(
            strategy_id=f"dum1_candidate_{fam}_v001",
            name=f"DUM1 candidate {fam} v1",
            family=fam,
            version=1,
            entry_score_min=entry,
            high_confidence_score=max(90, entry + 5),
            move_spread_ratio_min=ratio,
            momentum_pips_min=mom,
            max_margin_pct=margin,
            risk_per_trade_pct=risk,
            max_open_trades=maxtr,
            stop_pips=sl,
            take_profit_pips=tp,
            trailing_stop_pips=trail,
            prune_red_minutes=18 if fam == "tight_prune_fast_cut" else 24,
            prune_stale_minutes=30 if fam == "tight_prune_fast_cut" else 60,
            notes=notes,
        ))
    return seeds

# -----------------------------------------------------------------------------
# Feature building and signals
# -----------------------------------------------------------------------------
@dataclass
class MarketSignal:
    time_utc: str
    instrument: str
    direction: str
    theme: str
    mid: float
    bid: float
    ask: float
    spread_pips: float
    momentum_5_pips: float
    momentum_15_pips: float
    momentum_30_pips: float
    accel_pips: float
    volatility_30_pips: float
    compression_score: float
    move_spread_ratio: float
    signal_score: float
    confidence_bucket: str
    reason: str

    def to_row(self) -> Dict[str, Any]:
        return asdict(self)


def candle_df_from_oanda(data: Dict[str, Any], instrument: str, granularity: str) -> pd.DataFrame:
    rows = []
    for c in data.get("candles", []) or []:
        if not c.get("complete", True):
            continue
        m = c.get("mid") or {}
        b = c.get("bid") or {}
        a = c.get("ask") or {}
        bid_open = safe_float(b.get("o"), float("nan"))
        bid_high = safe_float(b.get("h"), float("nan"))
        bid_low = safe_float(b.get("l"), float("nan"))
        bid_close = safe_float(b.get("c"), float("nan"))
        ask_open = safe_float(a.get("o"), float("nan"))
        ask_high = safe_float(a.get("h"), float("nan"))
        ask_low = safe_float(a.get("l"), float("nan"))
        ask_close = safe_float(a.get("c"), float("nan"))
        mid_close = safe_float(m.get("c"), float("nan"))
        if not math.isfinite(mid_close) and math.isfinite(bid_close) and math.isfinite(ask_close):
            mid_close = (bid_close + ask_close) / 2.0
        mid_open = safe_float(m.get("o"), float("nan"))
        mid_high = safe_float(m.get("h"), float("nan"))
        mid_low = safe_float(m.get("l"), float("nan"))
        if not math.isfinite(mid_open) and b and a:
            mid_open = (safe_float(b.get("o")) + safe_float(a.get("o"))) / 2.0
            mid_high = (safe_float(b.get("h")) + safe_float(a.get("h"))) / 2.0
            mid_low = (safe_float(b.get("l")) + safe_float(a.get("l"))) / 2.0
        spread_pips = (
            (ask_close - bid_close) * pips_multiplier(instrument)
            if math.isfinite(bid_close) and math.isfinite(ask_close)
            else float("nan")
        )
        rows.append({
            "time": c.get("time"),
            "datetime": parse_oanda_time(c.get("time", "")).isoformat(),
            "instrument": instrument,
            "granularity": granularity,
            "open": mid_open,
            "high": mid_high,
            "low": mid_low,
            "close": mid_close,
            "bid_open": bid_open,
            "bid_high": bid_high,
            "bid_low": bid_low,
            "bid_close": bid_close,
            "ask_open": ask_open,
            "ask_high": ask_high,
            "ask_low": ask_low,
            "ask_close": ask_close,
            "spread_pips": spread_pips,
            "volume": safe_int(c.get("volume")),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("datetime").drop_duplicates("datetime")
    return df


def save_candles_csv(
    df: pd.DataFrame,
    instrument: str,
    granularity: str,
    root: Optional[Path] = None,
) -> Path:
    path = (root or DIRS["candles"]) / f"{instrument}_{granularity}.csv"
    if path.exists():
        old = read_csv(path)
        if not old.empty:
            df = pd.concat([old, df], ignore_index=True)
            if "datetime" in df.columns:
                df = df.drop_duplicates("datetime", keep="last").sort_values("datetime")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    os.replace(temp_path, path)
    return path


def load_local_candles(instrument: str, granularity: str = "M1") -> pd.DataFrame:
    path = DIRS["candles"] / f"{instrument}_{granularity}.csv"
    if path.exists():
        return read_csv(path)
    # Also try permanent raw M1 folder.
    alt_names = [
        RAW_M1_ROOT / f"{instrument}_M1.csv",
        RAW_M1_ROOT / f"{instrument.replace('_','')}_M1.csv",
        RAW_M1_ROOT / f"{instrument}_M1.parquet",
        RAW_M1_ROOT / f"{instrument.replace('_','')}_M1.parquet",
    ]
    for p in alt_names:
        try:
            if p.exists():
                if p.suffix.lower() == ".parquet":
                    return pd.read_parquet(p)
                return pd.read_csv(p)
        except Exception:
            continue
    return pd.DataFrame()


def pricing_map(data: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for p in data.get("prices", []) or []:
        inst = p.get("instrument")
        if not inst:
            continue
        bids = p.get("bids") or []
        asks = p.get("asks") or []
        bid = safe_float(bids[0].get("price") if bids else None)
        ask = safe_float(asks[0].get("price") if asks else None)
        if bid and ask:
            out[inst] = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2.0, "spread_pips": abs(ask - bid) * pips_multiplier(inst)}
    return out


def compute_signal(instrument: str, df: pd.DataFrame, px: Dict[str, float], profile: StrategyProfile) -> Optional[MarketSignal]:
    if df is None or df.empty or len(df) < 40:
        return None
    if "close" not in df.columns:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 40:
        return None
    c = close.values.astype(float)
    mid = px.get("mid") or float(c[-1])
    bid = px.get("bid") or mid
    ask = px.get("ask") or mid
    spread_pips = max(px.get("spread_pips", 0.0), 0.01)
    mul = pips_multiplier(instrument)

    def mom(n: int) -> float:
        if len(c) <= n:
            return 0.0
        return (c[-1] - c[-1 - n]) * mul

    m5 = mom(5)
    m15 = mom(15)
    m30 = mom(30)
    accel = m5 - (m15 / 3.0)
    diffs = np.diff(c[-31:]) * mul if len(c) >= 31 else np.diff(c) * mul
    vol = float(np.nanstd(diffs)) if len(diffs) else 0.0
    recent_range = (np.nanmax(c[-15:]) - np.nanmin(c[-15:])) * mul if len(c) >= 15 else 0.0
    prev_range = (np.nanmax(c[-40:-15]) - np.nanmin(c[-40:-15])) * mul if len(c) >= 40 else recent_range
    compression = float(prev_range / max(recent_range, 0.1)) if recent_range > 0 else 1.0
    dominant = m15 if abs(m15) >= abs(m30) * 0.55 else m30
    direction = "LONG" if dominant > 0 else "SHORT"
    move_abs = abs(dominant)
    ratio = move_abs / max(spread_pips, 0.01)

    # Theme inference, deliberately simple/transparent.
    theme = "MOMENTUM"
    if "JPY" in instrument:
        theme = "JPY_STRENGTH" if direction == "SHORT" and instrument.endswith("JPY") else "JPY_WEAKNESS"
    if instrument.startswith("USD") or instrument.endswith("USD"):
        if (instrument.startswith("USD") and direction == "LONG") or (instrument.endswith("USD") and direction == "SHORT"):
            theme = "USD_RALLY"
        else:
            theme = "USD_SELLOFF"
    if instrument[:3] in {"AUD", "NZD", "CAD"} or instrument[-3:] in {"AUD", "NZD", "CAD"}:
        theme = "COMMODITY_FX_" + ("STRENGTH" if direction == "LONG" else "WEAKNESS")

    score = 50.0
    score += min(25.0, ratio * 5.0)
    score += min(15.0, max(0.0, abs(m5) / max(profile.momentum_pips_min, 0.1)) * 5.0)
    score += min(10.0, max(0.0, abs(accel)) * 1.5)
    score += min(8.0, max(0.0, compression - 1.0) * 3.0)
    if vol < 0.2:
        score -= 5.0
    if spread_pips > 4.0 and "JPY" not in instrument:
        score -= min(15.0, spread_pips)
    if spread_pips > 12.0:
        score -= min(20.0, spread_pips / 2)
    score = max(0.0, min(100.0, score))
    bucket = "HIGH" if score >= profile.high_confidence_score else ("MEDIUM" if score >= profile.entry_score_min else "LOW")
    reason = f"theme={theme} m5={m5:.1f} m15={m15:.1f} m30={m30:.1f} ratio={ratio:.2f} spread={spread_pips:.2f} score={score:.1f}"
    return MarketSignal(
        time_utc=iso_utc(), instrument=instrument, direction=direction, theme=theme,
        mid=mid, bid=bid, ask=ask, spread_pips=spread_pips,
        momentum_5_pips=m5, momentum_15_pips=m15, momentum_30_pips=m30,
        accel_pips=accel, volatility_30_pips=vol, compression_score=compression,
        move_spread_ratio=ratio, signal_score=score, confidence_bucket=bucket, reason=reason,
    )


def signal_passes_profile(sig: MarketSignal, profile: StrategyProfile) -> Tuple[bool, str]:
    if sig.signal_score < profile.entry_score_min:
        return False, f"score {sig.signal_score:.1f} < min {profile.entry_score_min:.1f}"
    if sig.move_spread_ratio < profile.move_spread_ratio_min:
        return False, f"move_spread_ratio {sig.move_spread_ratio:.2f} < min {profile.move_spread_ratio_min:.2f}"
    if abs(sig.momentum_15_pips) < profile.momentum_pips_min and abs(sig.momentum_30_pips) < profile.momentum_pips_min:
        return False, f"momentum below min {profile.momentum_pips_min:.1f}"
    if profile.instrument_scope == "JPY_CROSSES" and "JPY" not in sig.instrument:
        return False, "outside JPY_CROSSES scope"
    if profile.instrument_scope == "USD_PAIRS" and "USD" not in sig.instrument:
        return False, "outside USD_PAIRS scope"
    return True, "passed"

# -----------------------------------------------------------------------------
# Account state/risk/execution
# -----------------------------------------------------------------------------
@dataclass
class AccountSnapshot:
    nav: float = 0.0
    balance: float = 0.0
    margin_used: float = 0.0
    margin_available: float = 0.0
    margin_used_pct: float = 0.0
    open_trade_count: int = 0
    pending_order_count: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)


def parse_account_summary(data: Dict[str, Any]) -> AccountSnapshot:
    acct = data.get("account", {}) or {}
    nav = safe_float(acct.get("NAV") or acct.get("nav"))
    balance = safe_float(acct.get("balance"))
    margin_used = safe_float(acct.get("marginUsed") or acct.get("margin_used"))
    margin_available = safe_float(acct.get("marginAvailable") or acct.get("margin_available"))
    pct = (margin_used / nav * 100.0) if nav else 0.0
    return AccountSnapshot(
        nav=nav, balance=balance, margin_used=margin_used, margin_available=margin_available,
        margin_used_pct=pct,
        open_trade_count=safe_int(acct.get("openTradeCount")),
        pending_order_count=safe_int(acct.get("pendingOrderCount")),
        raw=acct,
    )


def open_trades_from_response(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return data.get("trades", []) or data.get("openTrades", []) or []


def trade_direction(tr: Dict[str, Any]) -> str:
    units = safe_float(tr.get("currentUnits") or tr.get("current_units") or tr.get("initialUnits"))
    return "LONG" if units > 0 else "SHORT"


def calculate_units(snapshot: AccountSnapshot, sig: MarketSignal, profile: StrategyProfile, price: float) -> int:
    if snapshot.nav <= 0:
        return 0
    risk_usd = max(0.01, snapshot.nav * profile.risk_per_trade_pct / 100.0)
    stop_pips = max(profile.stop_pips, 1.0)
    # Simple pip value approximation in USD. Good enough for practice sizing; OANDA margin will ultimately enforce.
    if sig.instrument.endswith("USD"):
        pip_value_per_unit = 1.0 / pips_multiplier(sig.instrument)
    elif sig.instrument.startswith("USD"):
        pip_value_per_unit = (1.0 / pips_multiplier(sig.instrument)) / max(price, 0.0001)
    else:
        pip_value_per_unit = 1.0 / pips_multiplier(sig.instrument)
    units = int(risk_usd / max(stop_pips * pip_value_per_unit, 0.000001))
    # Margin guard approximation: cap notional relative to NAV. OANDA practice may allow more/less by instrument.
    max_notional = snapshot.nav * 25.0  # rough upper bound for practice FX margin use
    max_units = int(max_notional / max(price, 0.0001)) if price else units
    units = min(abs(units), abs(max_units))
    if units < DUM1_MIN_TRADE_UNITS:
        return 0
    return units if sig.direction == "LONG" else -units


def existing_pair_trades(open_trades: List[Dict[str, Any]], instrument: str) -> List[Dict[str, Any]]:
    return [t for t in open_trades if t.get("instrument") == instrument]


def can_open_signal(snapshot: AccountSnapshot, open_trades: List[Dict[str, Any]], sig: MarketSignal, profile: StrategyProfile, state: Dict[str, Any]) -> Tuple[bool, str]:
    if market_is_likely_closed():
        return False, "market likely closed"
    if snapshot.margin_used_pct >= profile.hard_margin_pct:
        return False, f"hard margin cap {snapshot.margin_used_pct:.1f}% >= {profile.hard_margin_pct:.1f}%"
    if snapshot.margin_used_pct >= profile.max_margin_pct:
        return False, f"soft margin cap {snapshot.margin_used_pct:.1f}% >= {profile.max_margin_pct:.1f}%"
    if len(open_trades) >= profile.max_open_trades:
        return False, f"open trades {len(open_trades)} >= max {profile.max_open_trades}"
    same = existing_pair_trades(open_trades, sig.instrument)
    if len(same) >= profile.max_same_pair_trades:
        return False, f"same pair trade limit for {sig.instrument}"
    if NO_AVERAGING_DOWN:
        for t in same:
            if trade_direction(t) == sig.direction and safe_float(t.get("unrealizedPL")) < 0:
                return False, f"no averaging down; same pair {sig.direction} is red"
    loss_cooldowns = state.setdefault("loss_cooldowns", {})
    last_loss = loss_cooldowns.get(sig.instrument)
    if last_loss:
        last_dt = parse_oanda_time(last_loss)
        if utc_now() - last_dt < timedelta(minutes=profile.loss_cooldown_minutes):
            return False, f"loss cooldown active for {sig.instrument}"
    return True, "allowed"


def stop_tp_prices(sig: MarketSignal, profile: StrategyProfile) -> Tuple[float, float, float]:
    entry = sig.ask if sig.direction == "LONG" else sig.bid
    sl_delta = pips_to_price(sig.instrument, profile.stop_pips)
    tp_delta = pips_to_price(sig.instrument, profile.take_profit_pips)
    trail_delta = pips_to_price(sig.instrument, profile.trailing_stop_pips)
    if sig.direction == "LONG":
        sl = entry - sl_delta
        tp = entry + tp_delta
    else:
        sl = entry + sl_delta
        tp = entry - tp_delta
    return sl, tp, trail_delta

# -----------------------------------------------------------------------------
# Logging and reports
# -----------------------------------------------------------------------------
AUDIT_FIELDS = [
    "time_utc", "script", "version", "account_id", "account_role", "event_type", "status",
    "instrument", "direction", "theme", "strategy_id", "strategy_family", "signal_score",
    "confidence_bucket", "move_abs_pips", "move_net_pips", "spread_pips", "move_spread_ratio",
    "margin_used_pct", "nav", "balance", "open_trade_count", "reason", "result", "raw_json",
]

ORDER_INTENT_FIELDS = [
    "time_utc", "intent_id", "account_id", "strategy_id", "instrument", "direction", "units",
    "entry_price_ref", "stop_loss", "take_profit", "trailing_stop_distance", "signal_score",
    "reason", "status",
]

ORDER_RESULT_FIELDS = [
    "time_utc", "intent_id", "account_id", "strategy_id", "instrument", "direction", "units",
    "http_status", "latency_ms", "order_id", "trade_id", "fill_price", "status", "reason", "raw_json",
]

MONITOR_FIELDS = [
    "time_utc", "account_id", "nav", "balance", "margin_used", "margin_available",
    "margin_used_pct", "open_trade_count", "pending_order_count", "active_strategy_id",
]

ERROR_FIELDS = ["time_utc", "where", "error_type", "error_message", "traceback"]


def log_audit(row: Dict[str, Any]) -> None:
    base = {k: "" for k in AUDIT_FIELDS}
    base.update(row)
    if not base.get("time_utc"):
        base["time_utc"] = iso_utc()
    if not base.get("script"):
        base["script"] = SCRIPT_NAME
    if not base.get("version"):
        base["version"] = SCRIPT_VERSION
    append_csv(LOG_FILES["audit"], base, AUDIT_FIELDS)


def log_error(where: str, exc: BaseException) -> None:
    row = {
        "time_utc": iso_utc(),
        "where": where,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "traceback": traceback.format_exc(limit=20),
    }
    append_csv(LOG_FILES["errors"], row, ERROR_FIELDS)
    log_audit({"event_type": "error", "status": "logged", "reason": where, "result": str(exc)})


def rotate_invalid_audit_log() -> Optional[Path]:
    """Preserve the old log when the prior timestamp bug made it unusable."""
    path = LOG_FILES["audit"]
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        frame = pd.read_csv(path, usecols=["time_utc"])
        if frame.empty:
            return None
        valid = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True).notna()
        if float(valid.mean()) >= 0.50:
            return None
        backup = path.with_name(
            f"{path.stem}.missing_timestamps_{utc_now().strftime('%Y%m%d_%H%M%S')}{path.suffix}.bak"
        )
        shutil.move(str(path), str(backup))
        return backup
    except Exception:
        return None


def rotate_legacy_research_ledger() -> Optional[Path]:
    path = LOG_FILES["research_experiments"]
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        header = path.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
        columns = [part.strip() for part in header.split(",")]
        required = {
            "candidate_id",
            "evaluation_stage",
            "bootstrap_lower_mean_net_pips",
            "instrument_subset",
        }
        if required.issubset(set(columns)):
            return None
        backup = path.with_name(
            f"{path.stem}.legacy_schema_{utc_now().strftime('%Y%m%d_%H%M%S')}{path.suffix}.bak"
        )
        shutil.move(str(path), str(backup))
        leaderboard = DIRS["research"] / "experiment_leaderboard.csv"
        if leaderboard.exists():
            lb_backup = leaderboard.with_name(
                f"{leaderboard.stem}.legacy_schema_{utc_now().strftime('%Y%m%d_%H%M%S')}{leaderboard.suffix}.bak"
            )
            shutil.move(str(leaderboard), str(lb_backup))
        return backup
    except Exception:
        return None


def summarize_recent(hours: int = 14) -> Dict[str, Any]:
    df = read_csv(LOG_FILES["audit"])
    if df.empty or "time_utc" not in df.columns:
        return {"generated_utc": iso_utc(), "hours": hours, "rows": 0}
    try:
        ts = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
        df = df[ts >= cutoff]
    except Exception:
        pass
    out = {
        "generated_utc": iso_utc(),
        "hours": hours,
        "rows": int(len(df)),
        "event_type_counts": df.get("event_type", pd.Series(dtype=str)).value_counts().head(25).to_dict(),
        "status_counts": df.get("status", pd.Series(dtype=str)).value_counts().head(25).to_dict(),
        "instrument_counts": df.get("instrument", pd.Series(dtype=str)).value_counts().head(25).to_dict(),
        "strategy_counts": df.get("strategy_id", pd.Series(dtype=str)).value_counts().head(25).to_dict(),
    }
    return out


def _csv_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def ingest_technical_exhaustion_evidence() -> Dict[str, Any]:
    """Store technical-account research and live shadow observations locally."""
    DIRS["technical_evidence"].mkdir(parents=True, exist_ok=True)
    DIRS["reports"].mkdir(parents=True, exist_ok=True)
    expanding_path = TECHNICAL_RESEARCH_ROOT / "expanding" / "aggregate_results.csv"
    rolling_path = TECHNICAL_RESEARCH_ROOT / "train_6w" / "aggregate_results.csv"
    output_csv = DIRS["technical_evidence"] / "exhaustion_rule_validation.csv"
    output_json = DIRS["reports"] / "latest_technical_exhaustion_evidence.json"
    if not expanding_path.exists() or not rolling_path.exists():
        payload = {
            "available": False,
            "reason": "technical rolling-validation files are missing",
            "generated_utc": iso_utc(),
        }
        save_json(output_json, payload)
        return payload
    try:
        expanding = pd.read_csv(expanding_path)
        rolling = pd.read_csv(rolling_path)
        expanding["stable_for_shadow"] = _csv_bool(expanding["stable_for_shadow"])
        expanding["stable_for_execution"] = _csv_bool(expanding["stable_for_execution"])
        rolling["stable_for_shadow"] = _csv_bool(rolling["stable_for_shadow"])
        rolling["stable_for_execution"] = _csv_bool(rolling["stable_for_execution"])
        keys = ["horizon_minutes", "rule"]
        merged = expanding.merge(
            rolling,
            on=keys,
            how="outer",
            suffixes=("_expanding", "_train6w"),
        )
        merged["stable_in_both"] = (
            merged["stable_for_shadow_expanding"].fillna(False)
            & merged["stable_for_shadow_train6w"].fillna(False)
        )
        merged["execution_approved"] = (
            merged["stable_for_execution_expanding"].fillna(False)
            & merged["stable_for_execution_train6w"].fillna(False)
        )
        merged["integration_mode"] = np.where(
            merged["execution_approved"],
            "eligible_for_execution_review",
            np.where(merged["stable_in_both"], "shadow_only", "research_only"),
        )
        merged.to_csv(output_csv, index=False)

        live = read_csv(TECHNICAL_SHADOW_PATH)
        live_rows = int(len(live))
        recent_live = []
        if not live.empty:
            recent_live = live.tail(25).to_dict("records")
        stable = merged[merged["stable_in_both"]].copy()
        payload = {
            "available": True,
            "generated_utc": iso_utc(),
            "source_hashes": {
                "expanding": sha256_file(expanding_path),
                "train_6w": sha256_file(rolling_path),
            },
            "stable_rule_count": int(len(stable)),
            "execution_approved_rule_count": int(merged["execution_approved"].sum()),
            "stable_rules": stable[
                [
                    "horizon_minutes",
                    "rule",
                    "pooled_lift_expanding",
                    "pooled_lift_train6w",
                    "spike_short_direction_accuracy_expanding",
                    "spike_short_direction_accuracy_train6w",
                    "trigger_short_mean_threshold_units_expanding",
                    "trigger_short_mean_threshold_units_train6w",
                    "integration_mode",
                ]
            ].to_dict("records"),
            "live_shadow_rows": live_rows,
            "recent_live_shadow": recent_live,
            "stored_validation_csv": str(output_csv),
            "technical_shadow_source": str(TECHNICAL_SHADOW_PATH),
            "decision": (
                "Store as training evidence and GPT-review context. "
                "No direct execution feature is approved because cost-adjusted "
                "first-trigger expectancy is negative."
            ),
        }
        save_json(output_json, payload)
        comparison = TECHNICAL_RESEARCH_ROOT / "comparison.md"
        if comparison.exists():
            shutil.copy2(
                comparison,
                DIRS["technical_evidence"] / "exhaustion_validation_decision.md",
            )
        return payload
    except Exception as exc:
        log_error("ingest_technical_exhaustion_evidence", exc)
        return {
            "available": False,
            "reason": str(exc),
            "generated_utc": iso_utc(),
        }


def write_big_picture_summary(active: StrategyProfile, state: Dict[str, Any]) -> None:
    recent = summarize_recent(14)
    technical = load_json(
        DIRS["reports"] / "latest_technical_exhaustion_evidence.json",
        {},
    )
    weekend_validation = load_json(
        DIRS["reports"] / "latest_weekend_rolling_validation.json",
        {},
    )
    research_ledger = read_csv(LOG_FILES["research_experiments"])
    research_counts = (
        research_ledger["status"].value_counts().to_dict()
        if not research_ledger.empty and "status" in research_ledger.columns
        else {}
    )
    registry_index = load_json(DIRS["registry"] / "index.json", {})
    research_leader = load_json(
        DIRS["promotions"] / "research_leader.json",
        {},
    )
    shadow_candidate = load_json(
        DIRS["promotions"] / "shadow_candidate.json",
        {},
    )
    technical_production = load_json(
        DIRS["promotions"] / "technical_production.json",
        {},
    )
    mon = read_csv(LOG_FILES["monitor"])
    latest_nav = ""
    latest_margin = ""
    if not mon.empty:
        try:
            latest_nav = mon.iloc[-1].get("nav", "")
            latest_margin = mon.iloc[-1].get("margin_used_pct", "")
        except Exception:
            pass
    bench = read_csv(BENCHMARK_FILES["master"])
    top_line = "No benchmark rows yet."
    if not bench.empty and "score" in bench.columns:
        try:
            b = bench.sort_values("score", ascending=False).head(5)
            top_line = "\n".join([f"- {r.get('strategy_id','')} {r.get('instrument','ALL')} score={r.get('score','')} pf={r.get('profit_factor','')} return={r.get('return_pct','')}" for _, r in b.iterrows()])
        except Exception:
            pass
    md = f"""# Forex Research Manager Summary

Generated UTC: `{iso_utc()}`

## Runtime Mode
- Default mode: `research_only_no_broker_execution`
- Legacy DUM live loop requires: `--legacy-dum-live`
- Research strategy template: `{active.strategy_id}`

## Latest Account Snapshot
- NAV: `{latest_nav}`
- Margin used pct: `{latest_margin}`

## Rolling 14h Audit
- Rows: `{recent.get('rows')}`
- Event counts: `{json_dumps(recent.get('event_type_counts', {}))}`
- Status counts: `{json_dumps(recent.get('status_counts', {}))}`

## Top Benchmark Rows
{top_line}

## Latest Strategy Change
`{json_dumps(state.get('latest_strategy_change', {}))}`

## Production Deployment Gate
`{json_dumps(current_production_gate())}`

## Weekend Rolling Validation
- Validated: `{weekend_validation.get('validated', False)}`
- Independent weekly folds: `{weekend_validation.get('fold_count', 0)}`
- Mean AUC: `{weekend_validation.get('mean_auc', '')}`
- Minimum weekly AUC: `{weekend_validation.get('minimum_week_auc', '')}`
- Selected threshold: `{json_dumps(weekend_validation.get('selected_threshold', {}))}`
- Gate: `{json_dumps(weekend_validation.get('production_gate', {}))}`

## Technical Exhaustion Evidence
- Available: `{technical.get('available', False)}`
- Stable shadow rules: `{technical.get('stable_rule_count', 0)}`
- Execution-approved rules: `{technical.get('execution_approved_rule_count', 0)}`
- Live shadow observations stored: `{technical.get('live_shadow_rows', 0)}`
- Decision: `{technical.get('decision', '')}`

## Continuous Research Engine
- Experiments logged: `{len(research_ledger)}`
- Status counts: `{json_dumps(research_counts)}`
- Lifecycle candidates: `{len(registry_index.get('candidates', [])) if isinstance(registry_index, dict) else 0}`
- Research leader: `{research_leader.get('experiment_id', '')}`
- Shadow candidate: `{shadow_candidate.get('experiment_id', '')}`
- Technical production candidate: `{technical_production.get('experiment_id', '')}`
- Technical production activation: `{technical_production.get('technical_account_activation', False)}`

## What this script does
- Runs continuous model research and queues validation only after screen passes.
- Stores every candidate in `data\\oanda_training_manager\\model_lifecycle`.
- Publishes shadow/canary/production manifests as separate stages.
- Does not execute DUM or technical-account trades in default mode.
"""
    path = DIRS["reports"] / "latest_big_picture_summary.md"
    path.write_text(md, encoding="utf-8")
    save_json(DIRS["reports"] / "rolling_14h_summary.json", recent)


def create_export_zip() -> None:
    LATEST_EXPORT_ZIP.parent.mkdir(parents=True, exist_ok=True)
    include_paths = [
        DIRS["reports"] / "latest_big_picture_summary.md",
        DIRS["reports"] / "rolling_14h_summary.json",
        DIRS["reports"] / "latest_weekend_rolling_validation.json",
        DIRS["reports"] / "latest_weekend_rolling_validation_folds.csv",
        DIRS["reports"] / "latest_weekend_rolling_validation_thresholds.csv",
        DIRS["reports"] / "latest_technical_exhaustion_evidence.json",
        LOG_FILES["research_experiments"],
        DIRS["research"] / "experiment_leaderboard.csv",
        DIRS["research"] / "research_state.json",
        DIRS["promotions"] / "technical_model_manifest.json",
        DIRS["promotions"] / "dum_validation_manifest.json",
        ACTIVE_STRATEGY_FILE,
        STATE_FILE,
        LOG_FILES["audit"],
        LOG_FILES["actions"],
        LOG_FILES["orders_intent"],
        LOG_FILES["orders_result"],
        LOG_FILES["monitor"],
        LOG_FILES["strategy_changes"],
        LOG_FILES["errors"],
        LOG_FILES["gpt_calls"],
        BENCHMARK_FILES["master"],
        BENCHMARK_FILES["top_by_pair"],
        BENCHMARK_FILES["top_by_family"],
    ]
    for folder in [
        DIRS["candidates"],
        DIRS["backtests"],
        DIRS["training_sets"],
        DIRS["gpt_reviews"],
        DIRS["technical_evidence"],
    ]:
        if folder.exists():
            for p in folder.glob("*.*"):
                if p.is_file() and p.stat().st_size < 10_000_000:
                    include_paths.append(p)
    tmp = LATEST_EXPORT_ZIP.with_suffix(".tmp.zip")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        for p in include_paths:
            try:
                if p.exists() and p.is_file():
                    if str(CREDS_PATH).lower() in str(p).lower():
                        continue
                    arc = str(p.relative_to(PROJECT_ROOT)) if p.is_relative_to(PROJECT_ROOT) else p.name
                    z.write(p, arcname=arc)
            except Exception:
                continue
    tmp.replace(LATEST_EXPORT_ZIP)

# -----------------------------------------------------------------------------
# Backtest, training, benchmark
# -----------------------------------------------------------------------------
def make_training_rows_from_signals(signals: List[MarketSignal], horizon_minutes: int = 30) -> pd.DataFrame:
    rows = []
    for sig in signals:
        df = load_local_candles(sig.instrument, "M1")
        if df.empty or "datetime" not in df.columns or "close" not in df.columns:
            continue
        try:
            ts = pd.Timestamp(sig.time_utc)
            dft = df.copy()
            dft["dt"] = pd.to_datetime(dft["datetime"], errors="coerce", utc=True)
            dft = dft.dropna(subset=["dt"])
            idx_df = dft[dft["dt"] <= ts]
            if idx_df.empty:
                continue
            idx = idx_df.index[-1]
            pos = list(dft.index).index(idx)
            future = dft.iloc[pos:pos + horizon_minutes + 1]
            if len(future) < 5:
                continue
            start = safe_float(future.iloc[0]["close"])
            closes = pd.to_numeric(future["close"], errors="coerce").dropna().values
            if len(closes) < 2 or start <= 0:
                continue
            direction_mult = 1 if sig.direction == "LONG" else -1
            fut_pips = (closes[-1] - start) * pips_multiplier(sig.instrument) * direction_mult
            mfe = (np.nanmax((closes - start) * pips_multiplier(sig.instrument) * direction_mult))
            mae = (np.nanmin((closes - start) * pips_multiplier(sig.instrument) * direction_mult))
            rows.append({
                **sig.to_row(),
                "future_30m_pips": float(fut_pips),
                "mfe_30m_pips": float(mfe),
                "mae_30m_pips": float(mae),
                "would_profit_30m": int(fut_pips > sig.spread_pips * 1.5),
                "hit_10p_before_minus8p": int(mfe >= 10 and mae > -8),
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def make_historical_training_rows(
    instruments: List[str],
    profile: StrategyProfile,
    step_minutes: int = HISTORICAL_TRAINING_STEP_MINUTES,
    horizon_minutes: int = 30,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    step_minutes = max(1, int(step_minutes))
    for instrument_number, inst in enumerate(instruments, 1):
        df = load_local_candles(inst, "M1")
        if df.empty or len(df) < 100 or "close" not in df.columns:
            continue
        dft = df.copy()
        if "datetime" not in dft.columns:
            dft["datetime"] = dft.get("time")
        dft["dt"] = pd.to_datetime(dft["datetime"], errors="coerce", utc=True)
        dft["close"] = pd.to_numeric(dft["close"], errors="coerce")
        dft = dft.dropna(subset=["dt", "close"]).sort_values("dt").drop_duplicates("dt").reset_index(drop=True)
        if len(dft) < 100:
            continue
        base_spread = historical_spread_pips(inst)
        instrument_rows = 0
        for i in range(60, len(dft) - horizon_minutes - 1, step_minutes):
            history = dft.iloc[max(0, i - 60):i + 1]
            mid = safe_float(dft.iloc[i]["close"])
            px = {
                "mid": mid,
                "bid": mid - pips_to_price(inst, base_spread / 2),
                "ask": mid + pips_to_price(inst, base_spread / 2),
                "spread_pips": base_spread,
            }
            sig = compute_signal(inst, history, px, profile)
            if sig is None:
                continue
            sig.time_utc = dft.iloc[i]["dt"].isoformat()
            future = pd.to_numeric(
                dft.iloc[i:i + horizon_minutes + 1]["close"],
                errors="coerce",
            ).dropna().values
            if len(future) < 5:
                continue
            direction_mult = 1 if sig.direction == "LONG" else -1
            path_pips = (future - mid) * pips_multiplier(inst) * direction_mult
            fut_pips = float(path_pips[-1])
            mfe = float(np.nanmax(path_pips))
            mae = float(np.nanmin(path_pips))
            rows.append({
                **sig.to_row(),
                "future_30m_pips": fut_pips,
                "mfe_30m_pips": mfe,
                "mae_30m_pips": mae,
                "would_profit_30m": int(fut_pips > sig.spread_pips * 1.5),
                "hit_10p_before_minus8p": int(mfe >= 10 and mae > -8),
                "dataset_source": "historical_m1",
            })
            instrument_rows += 1
        print(
            f"[historical-training] {instrument_number}/{len(instruments)} "
            f"{inst}: candles={len(dft)} rows={instrument_rows}",
            flush=True,
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.drop_duplicates(
            subset=["time_utc", "instrument", "direction"],
            keep="last",
        ).sort_values(["time_utc", "instrument"]).reset_index(drop=True)
    return out


def backtest_profile(profile: StrategyProfile, instruments: List[str], max_rows_per_instrument: int = 2500) -> Dict[str, Any]:
    trades: List[Dict[str, Any]] = []
    for inst in instruments[:MAX_INSTRUMENTS_SCAN]:
        df = load_local_candles(inst, "M1")
        if df.empty or len(df) < 150:
            continue
        if len(df) > max_rows_per_instrument:
            df = df.tail(max_rows_per_instrument).reset_index(drop=True)
        if "datetime" not in df.columns:
            if "time" in df.columns:
                df["datetime"] = df["time"]
            else:
                continue
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        if len(df) < 150:
            continue
        # Synthetic tight spread for historical replay, conservative by pair type.
        base_spread = historical_spread_pips(inst)
        for i in range(60, len(df) - 45, 15):
            sub = df.iloc[:i + 1].copy()
            mid = safe_float(sub.iloc[-1]["close"])
            px = {"mid": mid, "bid": mid - pips_to_price(inst, base_spread / 2), "ask": mid + pips_to_price(inst, base_spread / 2), "spread_pips": base_spread}
            sig = compute_signal(inst, sub, px, profile)
            if not sig:
                continue
            ok, _ = signal_passes_profile(sig, profile)
            if not ok:
                continue
            future = df.iloc[i:i + 46]
            start = mid
            closes = pd.to_numeric(future["close"], errors="coerce").dropna().values
            if len(closes) < 10:
                continue
            mult = 1 if sig.direction == "LONG" else -1
            path_pips = (closes - start) * pips_multiplier(inst) * mult
            mfe = float(np.nanmax(path_pips))
            mae = float(np.nanmin(path_pips))
            realized = None
            exit_reason = "timeout"
            for pp in path_pips[1:]:
                if pp <= -profile.stop_pips:
                    realized = -profile.stop_pips - base_spread
                    exit_reason = "stop"
                    break
                if pp >= profile.take_profit_pips:
                    realized = profile.take_profit_pips - base_spread
                    exit_reason = "target"
                    break
            if realized is None:
                realized = float(path_pips[-1] - base_spread)
            trades.append({
                "instrument": inst,
                "direction": sig.direction,
                "theme": sig.theme,
                "signal_score": sig.signal_score,
                "realized_pips": realized,
                "mfe_pips": mfe,
                "mae_pips": mae,
                "exit_reason": exit_reason,
                "time_utc": str(df.iloc[i].get("datetime", "")),
            })
    if not trades:
        return {
            "strategy_id": profile.strategy_id, "strategy_family": profile.family, "total_trades": 0,
            "return_pct": 0.0, "profit_factor": 0.0, "win_rate": 0.0, "max_drawdown_pct": 0.0,
            "churn_loss_rate": 0.0, "capture_rate": 0.0, "score": 0.0, "trades": [],
        }
    pips = [safe_float(t["realized_pips"]) for t in trades]
    gross_win = sum(x for x in pips if x > 0)
    gross_loss = abs(sum(x for x in pips if x < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else (gross_win if gross_win > 0 else 0)
    wins = sum(1 for x in pips if x > 0)
    win_rate = wins / len(pips) * 100.0
    # normalized return proxy; not exact account P/L.
    return_pct = sum(pips) / 100.0
    eq = np.cumsum(pips)
    peak = np.maximum.accumulate(eq)
    dd = peak - eq
    max_dd = float(np.max(dd) / 100.0) if len(dd) else 0.0
    churn_loss_rate = sum(1 for x in pips if -8 <= x < 0) / len(pips) * 100.0
    capture_rate = sum(1 for t in trades if safe_float(t["mfe_pips"]) >= profile.take_profit_pips) / len(trades) * 100.0
    largest = max([abs(x) for x in pips] or [0])
    outlier_dep = largest / max(sum(abs(x) for x in pips), 0.001)
    score = (return_pct * 4.0) + min(40, pf * 12) + (win_rate * 0.2) + (capture_rate * 0.15) - (max_dd * 3.0) - (churn_loss_rate * 0.15) - (outlier_dep * 15.0)
    result = {
        "strategy_id": profile.strategy_id,
        "strategy_name": profile.name,
        "strategy_family": profile.family,
        "model_type": profile.model_type,
        "total_trades": len(trades),
        "return_pct": round(return_pct, 4),
        "profit_factor": round(pf, 4),
        "win_rate": round(win_rate, 3),
        "max_drawdown_pct": round(max_dd, 4),
        "churn_loss_rate": round(churn_loss_rate, 3),
        "capture_rate": round(capture_rate, 3),
        "outlier_dependency": round(outlier_dep, 4),
        "score": round(score, 4),
        "parameters_json": profile.to_json(),
        "trades": trades,
    }
    path = DIRS["backtests"] / f"backtest_{profile.strategy_id}_{utc_now().strftime('%Y%m%d_%H%M%S')}.json"
    save_json(path, result)
    result["evidence_path"] = str(path)
    return result


def benchmark_row_from_result(result: Dict[str, Any], source_type: str = "BACKTEST", instrument: str = "ALL") -> Dict[str, Any]:
    created = iso_utc()
    params = result.get("parameters_json", {})
    bid = f"bench_{created[:10].replace('-','')}_{result.get('strategy_id','unknown')}_{instrument}_{sha256_text(json_dumps(params))[:8]}"
    return {
        "benchmark_id": bid,
        "created_utc": created,
        "updated_utc": created,
        "source_bot_version": SCRIPT_VERSION,
        "source_controller": SCRIPT_NAME,
        "strategy_id": result.get("strategy_id", ""),
        "strategy_name": result.get("strategy_name", result.get("strategy_id", "")),
        "strategy_family": result.get("strategy_family", ""),
        "strategy_version": safe_int((params or {}).get("version", 1), 1) if isinstance(params, dict) else 1,
        "model_type": result.get("model_type", "rule_profile"),
        "model_artifact_path": result.get("model_artifact_path", ""),
        "model_artifact_hash": result.get("model_artifact_hash", ""),
        "instrument": instrument,
        "theme": result.get("theme", "ALL"),
        "timeframe": "M1",
        "lookback_window": "recent_local_candles",
        "test_window_start": result.get("test_window_start", ""),
        "test_window_end": result.get("test_window_end", ""),
        "data_source": "OANDA/local_candle_cache",
        "derivation_source": source_type,
        "rule_description": result.get("rule_description", "strategy profile/backtest result"),
        "parameters_json": json_dumps(params),
        "feature_set_json": json_dumps(result.get("feature_set_json", {})),
        "entry_logic": result.get("entry_logic", "score + move/spread + momentum threshold"),
        "exit_logic": result.get("exit_logic", "stop/take-profit/trailing/prune"),
        "risk_logic": result.get("risk_logic", "risk_pct + margin caps"),
        "margin_logic": result.get("margin_logic", "uses OANDA margin with configured caps"),
        "backtest_trades": safe_int(result.get("total_trades")),
        "live_trades": safe_int(result.get("live_trades")),
        "total_trades": safe_int(result.get("total_trades")) + safe_int(result.get("live_trades")),
        "return_pct": safe_float(result.get("return_pct")),
        "net_pl_per_100_nav": safe_float(result.get("return_pct")),
        "profit_factor": safe_float(result.get("profit_factor")),
        "win_rate": safe_float(result.get("win_rate")),
        "avg_win": safe_float(result.get("avg_win")),
        "avg_loss": safe_float(result.get("avg_loss")),
        "max_drawdown_pct": safe_float(result.get("max_drawdown_pct")),
        "churn_loss_rate": safe_float(result.get("churn_loss_rate")),
        "spread_drag_per_100_nav": safe_float(result.get("spread_drag_per_100_nav")),
        "margin_efficiency": safe_float(result.get("margin_efficiency")),
        "capture_rate": safe_float(result.get("capture_rate")),
        "high_confidence_miss_rate": safe_float(result.get("high_confidence_miss_rate")),
        "replacement_success_rate": safe_float(result.get("replacement_success_rate")),
        "mfe_avg_pips": safe_float(result.get("mfe_avg_pips")),
        "mae_avg_pips": safe_float(result.get("mae_avg_pips")),
        "score": safe_float(result.get("score")),
        "rank_for_pair": "",
        "rank_for_family": "",
        "rank_overall": "",
        "status": result.get("status", "candidate"),
        "promotion_status": result.get("promotion_status", "not_ready"),
        "demotion_reason": result.get("demotion_reason", ""),
        "confidence_tier": confidence_tier(safe_int(result.get("total_trades")), safe_float(result.get("profit_factor"))),
        "evidence_path": result.get("evidence_path", ""),
        "notes": result.get("notes", ""),
    }


def confidence_tier(total_trades: int, pf: float) -> str:
    if total_trades >= 75 and pf >= 1.2:
        return "HIGH"
    if total_trades >= 25 and pf >= 1.05:
        return "MEDIUM"
    return "LOW"


def ensure_benchmark_headers() -> None:
    master_fields = list(benchmark_row_from_result({"strategy_id": "template"}).keys())
    if not BENCHMARK_FILES["master"].exists():
        append_csv(BENCHMARK_FILES["master"], {k: "" for k in master_fields}, master_fields)
        # Remove empty first data row by rewriting header only.
        with BENCHMARK_FILES["master"].open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=master_fields).writeheader()


def update_benchmark(result: Dict[str, Any], source_type: str = "BACKTEST", instrument: str = "ALL") -> None:
    ensure_benchmark_headers()
    row = benchmark_row_from_result(result, source_type, instrument)
    fields = list(row.keys())
    append_csv(BENCHMARK_FILES["master"], row, fields)
    append_jsonl(BENCHMARK_FILES["history"], row)
    recompute_benchmark_views()


def recompute_benchmark_views() -> None:
    df = read_csv(BENCHMARK_FILES["master"])
    if df.empty:
        return
    identity_columns = [
        column
        for column in [
            "strategy_id",
            "instrument",
            "derivation_source",
            "parameters_json",
            "total_trades",
            "return_pct",
            "profit_factor",
            "win_rate",
            "max_drawdown_pct",
            "score",
        ]
        if column in df.columns
    ]
    if identity_columns:
        df = df.drop_duplicates(
            subset=identity_columns,
            keep="last",
        ).reset_index(drop=True)
    if "score" in df.columns:
        df["score_num"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
    else:
        df["score_num"] = 0
    df = df.sort_values("score_num", ascending=False)
    df["rank_overall"] = range(1, len(df) + 1)
    master_columns = [
        column for column in df.columns if column != "score_num"
    ]
    df[master_columns].to_csv(BENCHMARK_FILES["master"], index=False)
    if "instrument" in df.columns:
        top_pair = df.groupby(["instrument"], dropna=False).head(KEEP_TOP_N_BENCHMARKS_PER_PAIR).copy()
        top_pair["rank_for_pair"] = top_pair.groupby("instrument").cumcount() + 1
        top_pair.to_csv(BENCHMARK_FILES["top_by_pair"], index=False)
    if "strategy_family" in df.columns:
        top_fam = df.groupby(["strategy_family"], dropna=False).head(KEEP_TOP_N_BENCHMARKS_PER_PAIR).copy()
        top_fam["rank_for_family"] = top_fam.groupby("strategy_family").cumcount() + 1
        top_fam.to_csv(BENCHMARK_FILES["top_by_family"], index=False)
    # Periodic backup
    try:
        backup = BENCHMARK_FILES["backups"] / f"strategy_benchmark_master_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
        if BENCHMARK_FILES["master"].exists() and random.random() < 0.05:
            shutil.copy2(BENCHMARK_FILES["master"], backup)
    except Exception:
        pass


MODEL_FEATURE_COLUMNS = [
    "spread_pips",
    "momentum_5_pips",
    "momentum_15_pips",
    "momentum_30_pips",
    "accel_pips",
    "volatility_30_pips",
    "compression_score",
    "move_spread_ratio",
    "signal_score",
]


def train_models_from_dataset(
    dataset: pd.DataFrame,
    dataset_hash: str = "",
) -> Dict[str, Any]:
    if not ALLOW_MODEL_TRAINING or not SKLEARN_AVAILABLE or dataset.empty:
        return {"trained": False, "reason": "sklearn unavailable or empty dataset"}
    required = MODEL_FEATURE_COLUMNS
    target = "would_profit_30m"
    if target not in dataset.columns:
        return {"trained": False, "reason": "missing target"}
    df = dataset.copy()
    for c in required + [target]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df = df.dropna(subset=required + [target])
    if len(df) < 50 or df[target].nunique() < 2:
        return {"trained": False, "reason": f"insufficient rows/classes rows={len(df)}"}
    split = int(len(df) * 0.75)
    train, test = df.iloc[:split], df.iloc[split:]
    X_train, y_train = train[required], train[target].astype(int)
    X_test, y_test = test[required], test[target].astype(int)
    clf = RandomForestClassifier(n_estimators=120, max_depth=7, min_samples_leaf=5, random_state=42, class_weight="balanced")
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)
    proba = clf.predict_proba(X_test)[:, 1] if hasattr(clf, "predict_proba") else pred
    acc = float(accuracy_score(y_test, pred))
    auc = None
    try:
        auc = float(roc_auc_score(y_test, proba))
    except Exception:
        pass
    artifact = DIRS["models"] / f"trade_quality_rf_{utc_now().strftime('%Y%m%d_%H%M%S')}.joblib"
    if JOBLIB_AVAILABLE:
        joblib.dump({
            "model": clf,
            "features": required,
            "trained_utc": iso_utc(),
            "accuracy": acc,
            "auc": auc,
            "dataset_hash": dataset_hash,
        }, artifact)
    return {
        "trained": True,
        "model_artifact_path": str(artifact),
        "accuracy": acc,
        "auc": auc,
        "rows": len(df),
        "features": required,
        "dataset_hash": dataset_hash,
    }


def latest_historical_training_path() -> Optional[Path]:
    paths = sorted(
        DIRS["training_sets"].glob("historical_training_set_*.csv"),
        key=lambda path: path.stat().st_mtime,
    )
    return paths[-1] if paths else None


def rolling_week_model_validation(
    dataset_path: Path,
    *,
    min_train_weeks: int = WEEKEND_ROLLING_VALIDATION_MIN_TRAIN_WEEKS,
    max_holdout_weeks: int = WEEKEND_ROLLING_VALIDATION_MAX_HOLDOUT_WEEKS,
    max_train_rows: int = WEEKEND_ROLLING_VALIDATION_MAX_TRAIN_ROWS,
) -> Dict[str, Any]:
    """Train on prior weeks and score each subsequent unseen week."""
    if not SKLEARN_AVAILABLE:
        return {"validated": False, "reason": "scikit-learn unavailable"}
    required = [
        "time_utc",
        "instrument",
        *MODEL_FEATURE_COLUMNS,
        "would_profit_30m",
        "future_30m_pips",
    ]
    frame = pd.read_csv(
        dataset_path,
        usecols=lambda column: column in set(required),
    )
    missing = [column for column in required if column not in frame.columns]
    if missing:
        return {
            "validated": False,
            "reason": f"historical dataset missing columns: {missing}",
        }
    frame["time_utc"] = pd.to_datetime(
        frame["time_utc"],
        errors="coerce",
        utc=True,
    )
    for column in MODEL_FEATURE_COLUMNS + ["would_profit_30m", "future_30m_pips"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(
        subset=["time_utc", *MODEL_FEATURE_COLUMNS, "would_profit_30m", "future_30m_pips"]
    ).sort_values("time_utc")
    if frame.empty:
        return {"validated": False, "reason": "no complete historical rows"}
    frame["week_start"] = (
        frame["time_utc"].dt.normalize()
        - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
    )
    weeks = sorted(frame["week_start"].dropna().unique())
    eligible = weeks[min_train_weeks:]
    if max_holdout_weeks > 0:
        eligible = eligible[-max_holdout_weeks:]
    folds = []
    thresholds = [0.50, 0.55, 0.60, 0.65, 0.70]
    threshold_rows: list[dict[str, Any]] = []
    for fold_number, week_start in enumerate(eligible, 1):
        train = frame[frame["week_start"] < week_start]
        test = frame[frame["week_start"] == week_start]
        if len(train) > max_train_rows:
            train = train.tail(max_train_rows)
        if len(train) < 5_000 or len(test) < 250:
            continue
        y_train = train["would_profit_30m"].astype(int)
        y_test = test["would_profit_30m"].astype(int)
        if y_train.nunique() < 2 or y_test.nunique() < 2:
            continue
        model = RandomForestClassifier(
            n_estimators=160,
            max_depth=8,
            min_samples_leaf=10,
            random_state=42,
            class_weight="balanced",
            n_jobs=-1,
        )
        model.fit(train[MODEL_FEATURE_COLUMNS], y_train)
        probability = model.predict_proba(test[MODEL_FEATURE_COLUMNS])[:, 1]
        prediction = (probability >= 0.50).astype(int)
        auc = float(roc_auc_score(y_test, probability))
        folds.append({
            "fold": fold_number,
            "week_start": pd.Timestamp(week_start).isoformat(),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "test_pairs": int(test["instrument"].nunique()),
            "positive_rate": float(y_test.mean()),
            "accuracy_050": float(accuracy_score(y_test, prediction)),
            "auc": auc,
        })
        net_after_spread = (
            test["future_30m_pips"].to_numpy()
            - test["spread_pips"].to_numpy()
        )
        for threshold in thresholds:
            selected = probability >= threshold
            selected_net = net_after_spread[selected]
            trades = int(selected.sum())
            gross_win = float(selected_net[selected_net > 0].sum()) if trades else 0.0
            gross_loss = float(abs(selected_net[selected_net < 0].sum())) if trades else 0.0
            threshold_rows.append({
                "week_start": pd.Timestamp(week_start).isoformat(),
                "threshold": threshold,
                "trades": trades,
                "mean_net_pips": (
                    float(np.mean(selected_net))
                    if trades
                    else None
                ),
                "win_rate": (
                    float(np.mean(selected_net > 0))
                    if trades
                    else None
                ),
                "profit_factor": (
                    gross_win / gross_loss
                    if gross_loss > 0
                    else (gross_win if gross_win > 0 else 0.0)
                ),
                "total_net_pips": float(selected_net.sum()) if trades else 0.0,
            })
    if not folds:
        return {
            "validated": False,
            "reason": "insufficient independent weekly folds",
        }
    threshold_frame = pd.DataFrame(threshold_rows)
    aggregates = []
    for threshold, group in threshold_frame.groupby("threshold"):
        trades = int(group["trades"].sum())
        total_net = float(group["total_net_pips"].sum())
        weighted_win_rate = (
            float(
                np.average(
                    group["win_rate"].fillna(0.0),
                    weights=group["trades"].clip(lower=0),
                )
            )
            if trades > 0
            else 0.0
        )
        aggregates.append({
            "threshold": float(threshold),
            "weeks": int(len(group)),
            "positive_weeks": int((group["mean_net_pips"].fillna(0.0) > 0).sum()),
            "trades": trades,
            "mean_net_pips": total_net / max(trades, 1),
            "median_week_mean_net_pips": float(group["mean_net_pips"].median()),
            "mean_win_rate": weighted_win_rate,
            "total_net_pips": total_net,
            "median_profit_factor": float(group["profit_factor"].median()),
        })
    threshold_summary = pd.DataFrame(aggregates).sort_values(
        ["median_week_mean_net_pips", "mean_net_pips", "trades"],
        ascending=[False, False, False],
    )
    eligible_thresholds = threshold_summary[
        threshold_summary["trades"] >= 75
    ]
    selected = (
        eligible_thresholds.iloc[0]
        if not eligible_thresholds.empty
        else threshold_summary.iloc[0]
    )
    fold_frame = pd.DataFrame(folds)
    gate = {
        "mean_auc_at_least_0_60": bool(fold_frame["auc"].mean() >= 0.60),
        "minimum_week_auc_at_least_0_52": bool(fold_frame["auc"].min() >= 0.52),
        "selected_trades_at_least_75": bool(selected["trades"] >= 75),
        "selected_mean_net_pips_positive": bool(selected["mean_net_pips"] > 0),
        "selected_positive_in_60pct_weeks": bool(
            selected["positive_weeks"] >= math.ceil(len(fold_frame) * 0.60)
        ),
        "selected_median_profit_factor_at_least_1_05": bool(
            selected["median_profit_factor"] >= 1.05
        ),
    }
    gate["passed"] = all(gate.values())
    dataset_hash = sha256_file(dataset_path)
    result = {
        "validated": True,
        "generated_utc": iso_utc(),
        "dataset": str(dataset_path),
        "dataset_hash": dataset_hash,
        "dataset_rows": int(len(frame)),
        "dataset_start": frame["time_utc"].min().isoformat(),
        "dataset_end": frame["time_utc"].max().isoformat(),
        "folds": folds,
        "fold_count": int(len(folds)),
        "mean_auc": float(fold_frame["auc"].mean()),
        "minimum_week_auc": float(fold_frame["auc"].min()),
        "selected_threshold": selected.to_dict(),
        "threshold_summary": threshold_summary.to_dict("records"),
        "production_gate": gate,
    }
    save_json(DIRS["reports"] / "latest_weekend_rolling_validation.json", result)
    pd.DataFrame(folds).to_csv(
        DIRS["reports"] / "latest_weekend_rolling_validation_folds.csv",
        index=False,
    )
    threshold_frame.to_csv(
        DIRS["reports"] / "latest_weekend_rolling_validation_thresholds.csv",
        index=False,
    )
    return result


EXPANDED_INDICATOR_FEATURES = [
    "abs_momentum_5_atr",
    "abs_momentum_15_atr",
    "abs_momentum_30_atr",
    "abs_momentum_60_atr",
    "momentum_5_15_delta_atr",
    "momentum_15_30_delta_atr",
    "momentum_30_60_delta_atr",
    "momentum_5_to_30_ratio",
    "momentum_15_to_60_ratio",
    "momentum_alignment_long",
    "momentum_alignment_short",
    "momentum_alignment_edge",
    "momentum_acceleration_pressure",
    "momentum_exhaustion_pressure",
    "sma7_gt_sma8",
    "sma7_lt_sma8",
    "sma30_slope_gt_1_atr",
    "sma30_slope_lt_minus_1_atr",
    "ema8_gt_ema21",
    "ema8_lt_ema21",
    "macd_hist_positive",
    "macd_hist_negative",
    "macd_hist_abs_atr",
    "trend_pressure_long",
    "trend_pressure_short",
    "trend_pressure_edge",
    "rsi_overbought_pressure",
    "rsi_oversold_pressure",
    "rsi_extreme_abs",
    "donchian60_breakout_long",
    "donchian60_breakout_short",
    "donchian240_breakout_long",
    "donchian240_breakout_short",
    "donchian_breakout_abs_atr",
    "range60_extreme_abs",
    "range240_extreme_abs",
    "near_upper_range_score",
    "near_lower_range_score",
    "range_breakout_pressure_long",
    "range_breakout_pressure_short",
    "strength_gap_abs_15",
    "strength_gap_abs_60",
    "strength_gap_15_60_delta",
    "atr_spread_efficiency",
    "spread_to_atr240",
    "spread_cost_pressure",
    "compression_breakout_pressure",
    "compression_then_expansion",
    "volatility_expansion_pressure",
    "liquidity_quality_score",
    "anti_chase_pressure",
    "london_volatility_pressure",
    "ny_volatility_pressure",
    "overlap_breakout_pressure",
    "rollover_spread_penalty",
    "volatile_spread_penalty",
    "risk_on_long_pressure",
    "risk_off_jpy_chf_pressure",
    "carry_trend_alignment",
    "carry_extreme_pressure",
    "macro_bias_trend_alignment",
    "macro_event_spread_pressure",
    "rule_pre_breakout_long",
    "rule_pre_breakout_short",
    "rule_breakout_continuation_long",
    "rule_breakout_continuation_short",
    "rule_exhaustion_reversal_long",
    "rule_exhaustion_reversal_short",
    "rule_session_timing_long",
    "rule_session_timing_short",
    "rule_cross_pressure_long",
    "rule_cross_pressure_short",
    "rule_liquidity_ok",
    "rule_anti_chase_penalty",
    "rule_value_liquidity_ok",
    "rule_late_momentum_confirmation_long",
    "rule_late_momentum_confirmation_short",
    "rule_value_breakout_long",
    "rule_value_breakout_short",
    "rule_guarded_exhaustion_reversal_long",
    "rule_guarded_exhaustion_reversal_short",
    "rule_scout_value_long_score",
    "rule_scout_value_short_score",
    "rule_baseline_long_score",
    "rule_baseline_short_score",
    "rule_baseline_direction_score",
    "rule_baseline_strength_score",
    "rule_baseline_long_candidate",
    "rule_baseline_short_candidate",
]


TECHNICAL_MODEL_FEATURES = [
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "acceleration_15_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "ema8_minus_ema21_atr",
    "ema21_slope_15_atr",
    "ema55_slope_30_atr",
    "macd_atr",
    "macd_hist_atr",
    "ma_stack_score",
    "rsi14_centered",
    "atr15_to_atr240",
    "compression_30",
    "range_position_60_centered",
    "range_position_240_centered",
    "donchian_60_position_centered",
    "donchian_60_breakout_atr",
    "donchian_240_position_centered",
    "donchian_240_breakout_atr",
    "trend_consistency_60",
    "realized_vol_ratio_30_240",
    *EXPANDED_INDICATOR_FEATURES,
    "spread_ratio_60",
    "volume_z_30",
    "strength_gap_15",
    "strength_gap_60",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
    "spread_pips",
    "atr240_pips",
    "shadow_m30_strength",
    "shadow_m15_strength",
    "shadow_m5_strength",
    "shadow_m15_m30",
    "shadow_m5_m30",
    "shadow_rule_count",
    "hour_sin",
    "hour_cos",
    "weekday",
    "weekday_sin",
    "weekday_cos",
    "month",
    "month_sin",
    "month_cos",
    "day_of_month_sin",
    "day_of_month_cos",
    "day_of_year_sin",
    "day_of_year_cos",
    "week_of_year_sin",
    "week_of_year_cos",
    "week_of_month",
    "is_month_start",
    "is_month_end",
    "is_quarter_end",
    "is_asia_session",
    "is_london_session",
    "is_new_york_session",
    "is_london_ny_overlap",
    "is_rollover_hour",
    "instrument_volatility_percentile",
    "instrument_median_abs_60_pips",
    "instrument_q995_abs_60_pips",
    "instrument_q995_move_to_spread",
    "is_major_pair",
    "is_usd_pair",
    "is_non_usd_pair",
    "is_jpy_cross",
    "is_exotic_pair",
    "is_volatile_pair",
    "is_volatile_or_exotic_pair",
    "is_non_usd_volatile_pair",
    "pair_class_usd_major",
    "pair_class_usd_other",
    "pair_class_jpy_risk",
    "pair_class_commodity",
    "pair_class_eur_gbp_cross",
    "pair_class_chf_safe_haven",
    "pair_class_exotic_high_spread",
    "pair_class_volatile_non_usd",
    "pair_class_other_cross",
    "base_is_usd",
    "quote_is_usd",
    "base_is_jpy",
    "quote_is_jpy",
    "base_is_chf",
    "quote_is_chf",
    "base_is_commodity",
    "quote_is_commodity",
    "base_is_risk",
    "quote_is_risk",
    "carry_proxy_base_score",
    "carry_proxy_quote_score",
    "carry_proxy_diff",
    "carry_proxy_abs_diff",
    "carry_proxy_positive",
    "carry_proxy_negative",
    "regime_trend_strength",
    "regime_volatility_expansion_score",
    "regime_compression_score",
    "regime_spread_cost_score",
    "regime_usd_strength_15",
    "regime_usd_strength_60",
    "regime_jpy_strength_15",
    "regime_chf_strength_15",
    "regime_risk_on_score",
    "regime_commodity_score",
    "regime_acceleration_score",
    "regime_is_usd_trend",
    "regime_is_risk_on",
    "regime_is_risk_off",
    "regime_is_jpy_unwind",
    "regime_is_chf_safe_haven",
    "regime_is_commodity_trend",
    "regime_is_low_vol_chop",
    "regime_is_high_vol_event",
    "regime_is_spread_impaired",
    "regime_primary_usd_trend",
    "regime_primary_risk_on",
    "regime_primary_risk_off",
    "regime_primary_jpy_unwind",
    "regime_primary_chf_safe_haven",
    "regime_primary_commodity_trend",
    "regime_primary_low_vol_chop",
    "regime_primary_high_vol_event",
    "regime_primary_spread_impaired",
    "regime_primary_neutral",
    "macro_pair_bias",
    "macro_usd_bias",
    "macro_jpy_bias",
    "macro_chf_bias",
    "macro_risk_bias",
    "macro_commodity_bias",
    "macro_rate_diff_bias",
    "macro_news_risk",
    "macro_event_risk",
    "macro_bias_age_hours",
]

TECHNICAL_CORE_FEATURES = [
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "acceleration_15_atr",
    "ema8_minus_ema21_atr",
    "ema21_slope_15_atr",
    "macd_hist_atr",
    "ma_stack_score",
    "rsi14_centered",
    "atr15_to_atr240",
    "compression_30",
    "donchian_60_position_centered",
    "donchian_60_breakout_atr",
    "trend_consistency_60",
    "abs_momentum_30_atr",
    "momentum_alignment_edge",
    "momentum_acceleration_pressure",
    "trend_pressure_edge",
    "donchian_breakout_abs_atr",
    "compression_breakout_pressure",
    "liquidity_quality_score",
    "anti_chase_pressure",
    "rule_baseline_direction_score",
    "rule_baseline_strength_score",
    "rule_liquidity_ok",
    "range_position_240_centered",
    "spread_ratio_60",
    "strength_gap_15",
    "strength_gap_60",
    "shadow_rule_count",
    "hour_sin",
    "hour_cos",
    "weekday",
    "weekday_sin",
    "weekday_cos",
    "month_sin",
    "month_cos",
    "is_london_session",
    "is_new_york_session",
    "is_london_ny_overlap",
    "instrument_volatility_percentile",
    "is_major_pair",
    "is_volatile_pair",
    "pair_class_usd_major",
    "pair_class_jpy_risk",
    "pair_class_commodity",
    "pair_class_chf_safe_haven",
    "pair_class_exotic_high_spread",
    "carry_proxy_diff",
    "carry_proxy_abs_diff",
    "regime_trend_strength",
    "regime_volatility_expansion_score",
    "regime_spread_cost_score",
    "regime_usd_strength_15",
    "regime_risk_on_score",
    "regime_is_low_vol_chop",
    "regime_is_high_vol_event",
    "regime_is_spread_impaired",
    "macro_pair_bias",
    "macro_news_risk",
    "macro_event_risk",
]

CALENDAR_FEATURE_COLUMNS = [
    "hour_sin",
    "hour_cos",
    "weekday",
    "weekday_sin",
    "weekday_cos",
    "month",
    "month_sin",
    "month_cos",
    "day_of_month_sin",
    "day_of_month_cos",
    "day_of_year_sin",
    "day_of_year_cos",
    "week_of_year_sin",
    "week_of_year_cos",
    "week_of_month",
    "is_month_start",
    "is_month_end",
    "is_quarter_end",
    "is_asia_session",
    "is_london_session",
    "is_new_york_session",
    "is_london_ny_overlap",
    "is_rollover_hour",
]

INSTRUMENT_METADATA_FEATURES = [
    "instrument_volatility_percentile",
    "instrument_median_abs_60_pips",
    "instrument_q995_abs_60_pips",
    "instrument_q995_move_to_spread",
    "is_major_pair",
    "is_usd_pair",
    "is_non_usd_pair",
    "is_jpy_cross",
    "is_exotic_pair",
    "is_volatile_pair",
    "is_volatile_or_exotic_pair",
    "is_non_usd_volatile_pair",
]

PAIR_TAXONOMY_FEATURES = [
    *(f"pair_class_{name}" for name in PAIR_TAXONOMY_CLASSES),
    "base_is_usd",
    "quote_is_usd",
    "base_is_jpy",
    "quote_is_jpy",
    "base_is_chf",
    "quote_is_chf",
    "base_is_commodity",
    "quote_is_commodity",
    "base_is_risk",
    "quote_is_risk",
]

CARRY_PROXY_FEATURES = [
    "carry_proxy_base_score",
    "carry_proxy_quote_score",
    "carry_proxy_diff",
    "carry_proxy_abs_diff",
    "carry_proxy_positive",
    "carry_proxy_negative",
]

TREND_CHANNEL_FEATURES = [
    "ema8_minus_ema21_atr",
    "ema21_slope_15_atr",
    "ema55_slope_30_atr",
    "macd_atr",
    "macd_hist_atr",
    "ma_stack_score",
    "donchian_60_position_centered",
    "donchian_60_breakout_atr",
    "donchian_240_position_centered",
    "donchian_240_breakout_atr",
    "trend_consistency_60",
    "realized_vol_ratio_30_240",
]

REGIME_FEATURES = [
    "regime_trend_strength",
    "regime_volatility_expansion_score",
    "regime_compression_score",
    "regime_spread_cost_score",
    "regime_usd_strength_15",
    "regime_usd_strength_60",
    "regime_jpy_strength_15",
    "regime_chf_strength_15",
    "regime_risk_on_score",
    "regime_commodity_score",
    "regime_acceleration_score",
    "regime_is_usd_trend",
    "regime_is_risk_on",
    "regime_is_risk_off",
    "regime_is_jpy_unwind",
    "regime_is_chf_safe_haven",
    "regime_is_commodity_trend",
    "regime_is_low_vol_chop",
    "regime_is_high_vol_event",
    "regime_is_spread_impaired",
    *(f"regime_primary_{name}" for name in REGIME_CLASSES),
]

MACRO_BIAS_FEATURES = [
    "macro_pair_bias",
    "macro_usd_bias",
    "macro_jpy_bias",
    "macro_chf_bias",
    "macro_risk_bias",
    "macro_commodity_bias",
    "macro_rate_diff_bias",
    "macro_news_risk",
    "macro_event_risk",
    "macro_bias_age_hours",
]

SHADOW_TECHNICAL_FEATURES = [
    "shadow_m30_strength",
    "shadow_m15_strength",
    "shadow_m5_strength",
    "shadow_m15_m30",
    "shadow_m5_m30",
    "shadow_rule_count",
]

DERIVED_TECHNICAL_FEATURES = set(
    CALENDAR_FEATURE_COLUMNS
    + INSTRUMENT_METADATA_FEATURES
    + PAIR_TAXONOMY_FEATURES
    + CARRY_PROXY_FEATURES
    + TREND_CHANNEL_FEATURES
    + EXPANDED_INDICATOR_FEATURES
    + REGIME_FEATURES
    + MACRO_BIAS_FEATURES
    + SHADOW_TECHNICAL_FEATURES
)

RAW_TECHNICAL_SOURCE_FEATURES = [
    feature
    for feature in TECHNICAL_MODEL_FEATURES
    if feature not in DERIVED_TECHNICAL_FEATURES
]

EVENT_PROFILE_FEATURES = [
    feature
    for feature in TECHNICAL_MODEL_FEATURES
    if not feature.startswith("shadow_")
]

RESEARCH_LEDGER_FIELDS = [
    "time_utc",
    "experiment_id",
    "candidate_id",
    "spec_hash",
    "source",
    "deployment_lane",
    "account_focus",
    "evaluation_stage",
    "status",
    "dataset_kind",
    "dataset_hash",
    "model_type",
    "target",
    "outcome",
    "feature_set",
    "instrument_subset",
    "instrument_whitelist",
    "segment_filters",
    "specialist_profile",
    "specialist_parent_experiment_id",
    "features",
    "parameters_json",
    "fold_count",
    "mean_auc",
    "minimum_week_auc",
    "selected_threshold",
    "trades",
    "positive_weeks",
    "mean_net_pips",
    "median_week_mean_net_pips",
    "median_profit_factor",
    "bootstrap_lower_mean_net_pips",
    "top_pair_trade_share",
    "score",
    "gate_passed",
    "model_artifact_path",
    "detail_path",
    "error",
]


def path_execution_outcomes(
    frame: pd.DataFrame,
    instrument: str,
    horizon: int,
) -> Dict[str, np.ndarray]:
    """Conservative stop/target and trailing-stop outcomes from future bars."""
    bars = max(1, int(math.ceil(horizon / 5.0)))
    multiplier = pips_multiplier(instrument)
    close = pd.to_numeric(frame["close"], errors="coerce").to_numpy(dtype=float)
    high = pd.to_numeric(frame["high"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(frame["low"], errors="coerce").to_numpy(dtype=float)
    spread = (
        pd.to_numeric(frame["spread_pips"], errors="coerce")
        .fillna(0.0)
        .clip(lower=0.0)
        .to_numpy(dtype=float)
    )
    atr = (
        pd.to_numeric(frame["atr240_pips"], errors="coerce")
        .clip(lower=0.1)
        .to_numpy(dtype=float)
    )
    expected = atr * math.sqrt(horizon / 5.0)
    stop = expected * MAJOR_MOVE_STOP_EXPECTED_UNITS
    target = expected * MAJOR_MOVE_TARGET_EXPECTED_UNITS
    trail_activation = expected * MAJOR_MOVE_TRAIL_ACTIVATION_UNITS
    trail_distance = expected * MAJOR_MOVE_TRAIL_DISTANCE_UNITS
    if (
        f"future_long_net_pips_{horizon}" in frame
        and f"future_short_net_pips_{horizon}" in frame
    ):
        long_endpoint = pd.to_numeric(
            frame[f"future_long_net_pips_{horizon}"],
            errors="coerce",
        ).to_numpy(dtype=float)
        short_endpoint = pd.to_numeric(
            frame[f"future_short_net_pips_{horizon}"],
            errors="coerce",
        ).to_numpy(dtype=float)
    else:
        endpoint_move = pd.to_numeric(
            frame[f"future_move_pips_{horizon}"],
            errors="coerce",
        ).to_numpy(dtype=float)
        long_endpoint = endpoint_move - spread
        short_endpoint = -endpoint_move - spread

    long_fixed = long_endpoint.copy()
    short_fixed = short_endpoint.copy()
    long_trailing = long_endpoint.copy()
    short_trailing = short_endpoint.copy()
    long_fixed_open = np.isfinite(close)
    short_fixed_open = np.isfinite(close)
    long_trail_open = np.isfinite(close)
    short_trail_open = np.isfinite(close)
    long_best = np.zeros(len(frame), dtype=float)
    short_best = np.zeros(len(frame), dtype=float)
    long_trail_active = np.zeros(len(frame), dtype=bool)
    short_trail_active = np.zeros(len(frame), dtype=bool)

    for step in range(1, bars + 1):
        future_high = np.full(len(frame), np.nan)
        future_low = np.full(len(frame), np.nan)
        future_high[:-step] = high[step:]
        future_low[:-step] = low[step:]
        valid = np.isfinite(future_high) & np.isfinite(future_low)
        long_high_net = (future_high - close) * multiplier - spread
        long_low_net = (future_low - close) * multiplier - spread
        short_high_net = (close - future_low) * multiplier - spread
        short_low_net = (close - future_high) * multiplier - spread

        unresolved = long_fixed_open & valid
        hit_long_stop = unresolved & (long_low_net <= -stop)
        hit_long_target = unresolved & ~hit_long_stop & (
            long_high_net >= target
        )
        long_fixed[hit_long_stop] = -stop[hit_long_stop]
        long_fixed[hit_long_target] = target[hit_long_target]
        long_fixed_open[hit_long_stop | hit_long_target] = False

        unresolved = short_fixed_open & valid
        hit_short_stop = unresolved & (short_low_net <= -stop)
        hit_short_target = unresolved & ~hit_short_stop & (
            short_high_net >= target
        )
        short_fixed[hit_short_stop] = -stop[hit_short_stop]
        short_fixed[hit_short_target] = target[hit_short_target]
        short_fixed_open[hit_short_stop | hit_short_target] = False

        unresolved = long_trail_open & valid
        hit_long_hard_stop = unresolved & (long_low_net <= -stop)
        hit_long_trail = (
            unresolved
            & ~hit_long_hard_stop
            & long_trail_active
            & (long_low_net <= long_best - trail_distance)
        )
        long_trailing[hit_long_hard_stop] = -stop[hit_long_hard_stop]
        long_trailing[hit_long_trail] = np.maximum(
            0.0,
            long_best[hit_long_trail] - trail_distance[hit_long_trail],
        )
        long_trail_open[hit_long_hard_stop | hit_long_trail] = False
        surviving = long_trail_open & valid
        long_best[surviving] = np.maximum(
            long_best[surviving],
            long_high_net[surviving],
        )
        long_trail_active[surviving] |= (
            long_best[surviving] >= trail_activation[surviving]
        )

        unresolved = short_trail_open & valid
        hit_short_hard_stop = unresolved & (short_low_net <= -stop)
        hit_short_trail = (
            unresolved
            & ~hit_short_hard_stop
            & short_trail_active
            & (short_low_net <= short_best - trail_distance)
        )
        short_trailing[hit_short_hard_stop] = -stop[hit_short_hard_stop]
        short_trailing[hit_short_trail] = np.maximum(
            0.0,
            short_best[hit_short_trail] - trail_distance[hit_short_trail],
        )
        short_trail_open[hit_short_hard_stop | hit_short_trail] = False
        surviving = short_trail_open & valid
        short_best[surviving] = np.maximum(
            short_best[surviving],
            short_high_net[surviving],
        )
        short_trail_active[surviving] |= (
            short_best[surviving] >= trail_activation[surviving]
        )

    return {
        "long_fixed_pips": long_fixed,
        "short_fixed_pips": short_fixed,
        "long_trailing_pips": long_trailing,
        "short_trailing_pips": short_trailing,
        "expected_pips": expected,
    }


def add_major_move_fields(
    frame: pd.DataFrame,
    instrument: str,
    horizons: Optional[Sequence[int]] = None,
) -> pd.DataFrame:
    """Add future-excursion labels using only thresholds known before each row.

    The future excursion is a label, never a model input. Its rolling q99.5
    threshold is shifted past the full outcome horizon so a row cannot use its
    own move, or an overlapping future move, to define what counted as major.
    """
    output = frame.copy()
    multiplier = pips_multiplier(instrument)
    close = pd.to_numeric(output["close"], errors="coerce")
    high = pd.to_numeric(output["high"], errors="coerce")
    low = pd.to_numeric(output["low"], errors="coerce")
    atr = pd.to_numeric(output["atr240_pips"], errors="coerce").clip(lower=0.1)
    spread = pd.to_numeric(output["spread_pips"], errors="coerce").clip(lower=0.1)
    for horizon in horizons or MAJOR_MOVE_CATALOG_HORIZONS:
        bars = max(1, int(math.ceil(horizon / 5.0)))
        future_high = (
            high.shift(-1)
            .iloc[::-1]
            .rolling(bars, min_periods=bars)
            .max()
            .iloc[::-1]
        )
        future_low = (
            low.shift(-1)
            .iloc[::-1]
            .rolling(bars, min_periods=bars)
            .min()
            .iloc[::-1]
        )
        up_excursion = (future_high - close) * multiplier
        down_excursion = (close - future_low) * multiplier
        peak_excursion = pd.concat(
            [up_excursion, down_excursion],
            axis=1,
        ).max(axis=1)
        expected_move = atr * math.sqrt(horizon / 5.0)
        move_atr = peak_excursion / expected_move
        move_to_spread = peak_excursion / spread
        matured_shift = bars + 1
        historical_threshold = (
            peak_excursion.expanding(
                min_periods=MAJOR_MOVE_MIN_HISTORY_ROWS
            )
            .quantile(MAJOR_MOVE_QUANTILE)
            .shift(matured_shift)
        )
        major = (
            historical_threshold.notna()
            & (peak_excursion >= historical_threshold)
            & (move_atr >= MAJOR_MOVE_MIN_ATR_UNITS)
            & (move_to_spread >= MAJOR_MOVE_MIN_SPREAD_MULTIPLE)
        )
        recent_major = (
            pd.Series(major.astype(int), index=output.index)
            .shift(1)
            .rolling(bars, min_periods=1)
            .max()
            .fillna(0)
            .astype(bool)
        )
        onset = major & ~recent_major
        execution = path_execution_outcomes(output, instrument, horizon)
        output[f"future_up_excursion_pips_{horizon}"] = up_excursion
        output[f"future_down_excursion_pips_{horizon}"] = down_excursion
        output[f"major_move_peak_pips_{horizon}"] = peak_excursion
        output[f"major_move_atr_units_{horizon}"] = move_atr
        output[f"major_move_to_spread_{horizon}"] = move_to_spread
        output[f"major_move_threshold_pips_{horizon}"] = historical_threshold
        output[f"major_event_{horizon}"] = np.where(
            historical_threshold.notna(),
            onset.astype(int),
            np.nan,
        )
        output[f"major_direction_up_{horizon}"] = np.where(
            onset,
            (up_excursion >= down_excursion).astype(int),
            np.nan,
        )
        event_label = pd.Series(
            np.where(historical_threshold.notna(), onset.astype(int), np.nan),
            index=output.index,
        )
        direction_label = pd.Series(
            np.where(onset, (up_excursion >= down_excursion).astype(int), np.nan),
            index=output.index,
        )
        for lead_minutes in MAJOR_MOVE_LEAD_MINUTES:
            if lead_minutes >= horizon:
                continue
            lead_bars = max(1, int(math.ceil(lead_minutes / 5.0)))
            future_events = [
                event_label.shift(-step)
                for step in range(1, lead_bars + 1)
            ]
            future_event_frame = pd.concat(future_events, axis=1)
            has_future_label = future_event_frame.notna().any(axis=1)
            future_event = future_event_frame.fillna(0).max(axis=1)
            pre_event = future_event.gt(0) & ~event_label.fillna(0).astype(bool)
            future_direction = pd.Series(np.nan, index=output.index)
            for step in range(1, lead_bars + 1):
                shifted_event = event_label.shift(-step)
                shifted_direction = direction_label.shift(-step)
                assign = shifted_event.eq(1) & future_direction.isna()
                future_direction.loc[assign] = shifted_direction.loc[assign]
            output[f"major_event_lead_{lead_minutes}_{horizon}"] = np.where(
                has_future_label,
                pre_event.astype(int),
                np.nan,
            )
            output[
                f"major_direction_up_lead_{lead_minutes}_{horizon}"
            ] = np.where(pre_event, future_direction, np.nan)
        output[f"major_up_{horizon}"] = np.where(
            historical_threshold.notna(),
            (onset & (up_excursion >= down_excursion)).astype(int),
            np.nan,
        )
        output[f"major_down_{horizon}"] = np.where(
            historical_threshold.notna(),
            (onset & (down_excursion > up_excursion)).astype(int),
            np.nan,
        )
        expected = np.maximum(execution["expected_pips"], 0.1)
        for direction in ["long", "short"]:
            for policy in ["fixed", "trailing"]:
                pips = execution[f"{direction}_{policy}_pips"]
                output[
                    f"{direction}_{policy}_net_pips_{horizon}"
                ] = pips
                output[
                    f"{direction}_{policy}_net_atr_{horizon}"
                ] = pips / expected
                for cap_name, cap_atr in MAJOR_MOVE_STOP_CAP_ATR_UNITS.items():
                    capped_pips = np.maximum(pips, -atr * float(cap_atr))
                    output[
                        f"{direction}_{policy}_{cap_name}_net_pips_{horizon}"
                    ] = capped_pips
                    output[
                        f"{direction}_{policy}_{cap_name}_net_atr_{horizon}"
                    ] = capped_pips / expected
    return output


def add_profitable_precursor_fields(
    frame: pd.DataFrame,
    instrument: str,
    horizons: Optional[Sequence[int]] = None,
) -> pd.DataFrame:
    """Add labels for the reverse workflow: profitable move first, then precursor.

    These labels use only past expanding thresholds shifted beyond the future
    outcome horizon.  The profitable move itself remains a label, never an
    input feature.
    """
    output = frame.copy()
    atr = pd.to_numeric(output["atr240_pips"], errors="coerce").clip(lower=0.1)
    spread = pd.to_numeric(output["spread_pips"], errors="coerce").clip(lower=0.1)
    for horizon in horizons or PROFITABLE_PRECURSOR_HORIZONS:
        bars = max(1, int(math.ceil(horizon / 5.0)))
        matured_shift = bars + 1
        long_pips = pd.to_numeric(
            output[f"future_long_net_pips_{horizon}"],
            errors="coerce",
        )
        short_pips = pd.to_numeric(
            output[f"future_short_net_pips_{horizon}"],
            errors="coerce",
        )
        long_atr = long_pips / atr
        short_atr = short_pips / atr
        long_to_spread = long_pips / spread
        short_to_spread = short_pips / spread
        long_threshold = (
            long_atr.expanding(min_periods=MAJOR_MOVE_MIN_HISTORY_ROWS)
            .quantile(PROFITABLE_PRECURSOR_QUANTILE)
            .shift(matured_shift)
            .clip(lower=PROFITABLE_PRECURSOR_MIN_ATR_UNITS)
        )
        short_threshold = (
            short_atr.expanding(min_periods=MAJOR_MOVE_MIN_HISTORY_ROWS)
            .quantile(PROFITABLE_PRECURSOR_QUANTILE)
            .shift(matured_shift)
            .clip(lower=PROFITABLE_PRECURSOR_MIN_ATR_UNITS)
        )
        long_profitable = (
            long_threshold.notna()
            & (long_atr >= long_threshold)
            & (long_to_spread >= PROFITABLE_PRECURSOR_MIN_SPREAD_MULTIPLE)
        )
        short_profitable = (
            short_threshold.notna()
            & (short_atr >= short_threshold)
            & (short_to_spread >= PROFITABLE_PRECURSOR_MIN_SPREAD_MULTIPLE)
        )

        def onset(mask: pd.Series) -> pd.Series:
            recent = (
                pd.Series(mask.astype(int), index=output.index)
                .shift(1)
                .rolling(bars, min_periods=1)
                .max()
                .fillna(0)
                .astype(bool)
            )
            return mask & ~recent

        long_onset = onset(long_profitable)
        short_onset = onset(short_profitable)
        any_onset = long_onset | short_onset
        threshold_ready = long_threshold.notna() | short_threshold.notna()
        best_atr = pd.concat([long_atr, short_atr], axis=1).max(axis=1)
        output[f"profitable_long_threshold_atr_{horizon}"] = long_threshold
        output[f"profitable_short_threshold_atr_{horizon}"] = short_threshold
        output[f"profitable_long_to_spread_{horizon}"] = long_to_spread
        output[f"profitable_short_to_spread_{horizon}"] = short_to_spread
        output[f"best_net_atr_{horizon}"] = best_atr
        output[f"best_direction_up_{horizon}"] = np.where(
            any_onset,
            (long_atr >= short_atr).astype(int),
            np.nan,
        )
        output[f"profitable_long_move_{horizon}"] = np.where(
            threshold_ready,
            long_onset.astype(int),
            np.nan,
        )
        output[f"profitable_short_move_{horizon}"] = np.where(
            threshold_ready,
            short_onset.astype(int),
            np.nan,
        )
        output[f"profitable_any_move_{horizon}"] = np.where(
            threshold_ready,
            any_onset.astype(int),
            np.nan,
        )
    return output


def add_return_curve_fields(
    frame: pd.DataFrame,
    instrument: str,
    horizons: Optional[Sequence[int]] = None,
) -> pd.DataFrame:
    """Add path-aware trade labels that value exits inside the horizon.

    Fixed-horizon labels answer "where is price at exactly N minutes?"  These
    curve labels answer the more tradable question: "was there a useful path
    after entry, how much did it give back, and did it move soon enough to be
    actionable?"  They remain labels/outcomes only; no curve column is part of
    the feature lists.
    """
    output = frame.copy()
    atr = pd.to_numeric(output["atr240_pips"], errors="coerce").clip(lower=0.1)
    spread = pd.to_numeric(output["spread_pips"], errors="coerce").clip(lower=0.1)
    m30 = pd.to_numeric(output.get("momentum_30_atr"), errors="coerce").fillna(0.0)
    for horizon in horizons or RETURN_CURVE_HORIZONS:
        early_horizon = RETURN_CURVE_EARLY_HORIZON.get(horizon, max(15, horizon // 4))
        long_endpoint = pd.to_numeric(
            output.get(f"future_long_net_pips_{horizon}"),
            errors="coerce",
        )
        short_endpoint = pd.to_numeric(
            output.get(f"future_short_net_pips_{horizon}"),
            errors="coerce",
        )
        long_early = pd.to_numeric(
            output.get(f"future_long_net_pips_{early_horizon}", long_endpoint),
            errors="coerce",
        )
        short_early = pd.to_numeric(
            output.get(f"future_short_net_pips_{early_horizon}", short_endpoint),
            errors="coerce",
        )
        up_excursion = pd.to_numeric(
            output.get(f"future_up_excursion_pips_{horizon}"),
            errors="coerce",
        )
        down_excursion = pd.to_numeric(
            output.get(f"future_down_excursion_pips_{horizon}"),
            errors="coerce",
        )
        expected = (atr * math.sqrt(horizon / 5.0)).clip(lower=0.1)

        long_best = up_excursion - spread
        short_best = down_excursion - spread
        long_adverse = -down_excursion - spread
        short_adverse = -up_excursion - spread
        long_adverse_cost = (-long_adverse.clip(upper=0.0)).fillna(0.0)
        short_adverse_cost = (-short_adverse.clip(upper=0.0)).fillna(0.0)

        long_curve = (
            RETURN_CURVE_BEST_WEIGHT * long_best
            + RETURN_CURVE_ENDPOINT_WEIGHT * long_endpoint
            + RETURN_CURVE_EARLY_WEIGHT * long_early
            - RETURN_CURVE_ADVERSE_PENALTY * long_adverse_cost
        )
        short_curve = (
            RETURN_CURVE_BEST_WEIGHT * short_best
            + RETURN_CURVE_ENDPOINT_WEIGHT * short_endpoint
            + RETURN_CURVE_EARLY_WEIGHT * short_early
            - RETURN_CURVE_ADVERSE_PENALTY * short_adverse_cost
        )
        long_giveback = (long_best - long_endpoint).clip(lower=0.0)
        short_giveback = (short_best - short_endpoint).clip(lower=0.0)
        long_efficiency = long_curve / long_best.where(long_best > 0)
        short_efficiency = short_curve / short_best.where(short_best > 0)

        output[f"long_curve_best_net_pips_{horizon}"] = long_best
        output[f"short_curve_best_net_pips_{horizon}"] = short_best
        output[f"long_curve_early_net_pips_{horizon}"] = long_early
        output[f"short_curve_early_net_pips_{horizon}"] = short_early
        output[f"long_curve_endpoint_net_pips_{horizon}"] = long_endpoint
        output[f"short_curve_endpoint_net_pips_{horizon}"] = short_endpoint
        output[f"long_curve_mae_pips_{horizon}"] = long_adverse
        output[f"short_curve_mae_pips_{horizon}"] = short_adverse
        output[f"long_curve_giveback_pips_{horizon}"] = long_giveback
        output[f"short_curve_giveback_pips_{horizon}"] = short_giveback
        output[f"long_curve_efficiency_{horizon}"] = long_efficiency
        output[f"short_curve_efficiency_{horizon}"] = short_efficiency
        output[f"long_curve_net_pips_{horizon}"] = long_curve
        output[f"short_curve_net_pips_{horizon}"] = short_curve
        output[f"long_curve_net_atr_{horizon}"] = long_curve / expected
        output[f"short_curve_net_atr_{horizon}"] = short_curve / expected
        output[f"long_curve_profit_{horizon}"] = (
            long_curve > 0
        ).astype(int)
        output[f"short_curve_profit_{horizon}"] = (
            short_curve > 0
        ).astype(int)

        continuation_curve = pd.Series(
            np.where(m30 >= 0, long_curve, short_curve),
            index=output.index,
        )
        reversal_curve = pd.Series(
            np.where(m30 >= 0, short_curve, long_curve),
            index=output.index,
        )
        output[f"continuation_curve_net_pips_{horizon}"] = continuation_curve
        output[f"continuation_curve_net_atr_{horizon}"] = (
            continuation_curve / expected
        )
        output[f"continuation_curve_profit_{horizon}"] = (
            continuation_curve > 0
        ).astype(int)
        output[f"reversal_curve_net_pips_{horizon}"] = reversal_curve
        output[f"reversal_curve_net_atr_{horizon}"] = reversal_curve / expected
        output[f"reversal_curve_profit_{horizon}"] = (
            reversal_curve > 0
        ).astype(int)
        output[f"best_curve_net_atr_{horizon}"] = pd.concat(
            [
                output[f"long_curve_net_atr_{horizon}"],
                output[f"short_curve_net_atr_{horizon}"],
            ],
            axis=1,
        ).max(axis=1)
        output[f"best_curve_direction_up_{horizon}"] = np.where(
            output[f"long_curve_net_atr_{horizon}"]
            >= output[f"short_curve_net_atr_{horizon}"],
            1,
            0,
        )
    return output


def profitable_move_precursor_candidates(
    frame: pd.DataFrame,
    instrument: str,
) -> pd.DataFrame:
    """Return profitable-move onset rows with their pre-move feature values."""
    rows: List[Dict[str, Any]] = []
    profile_features = EVENT_PROFILE_FEATURES
    for horizon in PROFITABLE_PRECURSOR_HORIZONS:
        for direction, outcome_prefix in [
            ("LONG", "long"),
            ("SHORT", "short"),
        ]:
            label_column = (
                f"profitable_{outcome_prefix}_move_{horizon}"
            )
            if label_column not in frame:
                continue
            mask = pd.to_numeric(
                frame[label_column],
                errors="coerce",
            ).fillna(0).astype(int).eq(1)
            for timestamp, row in frame.loc[mask].iterrows():
                pips = safe_float(
                    row.get(f"future_{outcome_prefix}_net_pips_{horizon}"),
                    np.nan,
                )
                atr_units = safe_float(
                    row.get(f"{outcome_prefix}_net_atr_{horizon}"),
                    np.nan,
                )
                threshold = safe_float(
                    row.get(
                        f"profitable_{outcome_prefix}_threshold_atr_{horizon}"
                    ),
                    np.nan,
                )
                record: Dict[str, Any] = {
                    "instrument": instrument,
                    "start_utc": timestamp,
                    "end_utc": timestamp + pd.Timedelta(minutes=horizon),
                    "horizon_minutes": horizon,
                    "direction": direction,
                    "net_pips": pips,
                    "net_atr": atr_units,
                    "threshold_atr": threshold,
                    "severity_vs_threshold": atr_units / max(threshold, 0.1),
                    "move_to_spread": safe_float(
                        row.get(
                            f"profitable_{outcome_prefix}_to_spread_{horizon}"
                        ),
                        np.nan,
                    ),
                    "spread_pips": safe_float(row.get("spread_pips")),
                    "atr240_pips": safe_float(row.get("atr240_pips")),
                    "is_major_pair": safe_float(row.get("is_major_pair")),
                    "is_usd_pair": safe_float(row.get("is_usd_pair")),
                    "is_non_usd_pair": safe_float(row.get("is_non_usd_pair")),
                    "is_exotic_pair": safe_float(row.get("is_exotic_pair")),
                    "is_volatile_pair": safe_float(row.get("is_volatile_pair")),
                    "is_non_usd_volatile_pair": safe_float(
                        row.get("is_non_usd_volatile_pair")
                    ),
                }
                for feature in profile_features:
                    record[feature] = safe_float(row.get(feature), np.nan)
                rows.append(record)
    return pd.DataFrame(rows)


def research_suffix_text() -> str:
    raw = os.environ.get("OANDA_TECHNICAL_RESEARCH_DATASET_SUFFIX", "")
    suffix = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw.strip())
    return suffix.strip("._-")


def suffixed_research_path(stem: str, suffix: str, extension: str) -> Path:
    name = f"{stem}_{suffix}{extension}" if suffix else f"{stem}{extension}"
    return DIRS["research"] / name


def save_profitable_move_precursor_catalog(
    candidates: pd.DataFrame,
    *,
    suffix: str = "",
) -> Dict[str, Any]:
    parquet_path = suffixed_research_path(
        "historical_profitable_move_precursors",
        suffix,
        ".parquet",
    )
    csv_path = suffixed_research_path(
        "historical_profitable_move_precursors",
        suffix,
        ".csv",
    )
    profile_path = (
        suffixed_research_path(
            "historical_profitable_move_precursor_profiles",
            suffix,
            ".csv",
        )
    )
    metadata_path = (
        suffixed_research_path(
            "historical_profitable_move_precursors",
            suffix,
            ".metadata.json",
        )
    )
    catalog = candidates.copy()
    if not catalog.empty:
        catalog = catalog.sort_values(
            ["start_utc", "instrument", "horizon_minutes", "direction"]
        ).reset_index(drop=True)
    catalog.to_parquet(parquet_path, compression="zstd", index=False)
    catalog.to_csv(csv_path, index=False)
    profile_rows: List[Dict[str, Any]] = []
    def flag_subset(column: str) -> pd.DataFrame:
        if catalog.empty or column not in catalog:
            return catalog.iloc[0:0]
        values = pd.to_numeric(catalog[column], errors="coerce").fillna(0.0)
        return catalog[values.eq(1.0)]

    subsets = {
        "all": catalog,
        "majors": flag_subset("is_major_pair"),
        "volatile": flag_subset("is_volatile_pair"),
        "exotic": flag_subset("is_exotic_pair"),
        "non_usd_volatile": flag_subset("is_non_usd_volatile_pair"),
    }
    for subset_name, subset in subsets.items():
        if subset.empty:
            continue
        for (horizon, direction), group in subset.groupby(
            ["horizon_minutes", "direction"]
        ):
            for feature in EVENT_PROFILE_FEATURES:
                if feature not in group:
                    continue
                values = pd.to_numeric(group[feature], errors="coerce").dropna()
                if values.empty:
                    continue
                profile_rows.append({
                    "subset": subset_name,
                    "horizon_minutes": horizon,
                    "direction": direction,
                    "feature": feature,
                    "events": len(values),
                    "mean": float(values.mean()),
                    "q10": float(values.quantile(0.10)),
                    "q25": float(values.quantile(0.25)),
                    "median": float(values.median()),
                    "q75": float(values.quantile(0.75)),
                    "q90": float(values.quantile(0.90)),
                })
    pd.DataFrame(profile_rows).to_csv(profile_path, index=False)
    metadata = {
        "generated_utc": iso_utc(),
        "catalog_role": (
            "Reverse research inventory: first find high-profit future "
            "movement onsets, then measure the pre-move feature ranges. "
            "This is descriptive evidence and model-training input, not "
            "a standalone trading rule."
        ),
        "rows": len(catalog),
        "instruments": (
            int(catalog["instrument"].nunique()) if not catalog.empty else 0
        ),
        "start": catalog["start_utc"].min() if not catalog.empty else None,
        "end": catalog["end_utc"].max() if not catalog.empty else None,
        "horizons": PROFITABLE_PRECURSOR_HORIZONS,
        "criteria": {
            "pair_direction_quantile": PROFITABLE_PRECURSOR_QUANTILE,
            "minimum_atr_units": PROFITABLE_PRECURSOR_MIN_ATR_UNITS,
            "minimum_move_to_spread": PROFITABLE_PRECURSOR_MIN_SPREAD_MULTIPLE,
            "threshold_shifted_past_future_horizon": True,
        },
        "catalog_parquet": str(parquet_path),
        "catalog_csv": str(csv_path),
        "feature_profiles": str(profile_path),
    }
    save_json(metadata_path, metadata)
    return metadata


def major_move_catalog_candidates(
    frame: pd.DataFrame,
    instrument: str,
) -> pd.DataFrame:
    """Return descriptive major moves across all available history."""
    rows: List[Dict[str, Any]] = []
    profile_features = EVENT_PROFILE_FEATURES
    for horizon in MAJOR_MOVE_CATALOG_HORIZONS:
        peak_column = f"major_move_peak_pips_{horizon}"
        peak = pd.to_numeric(frame[peak_column], errors="coerce")
        threshold = float(peak.dropna().quantile(MAJOR_MOVE_QUANTILE))
        if not math.isfinite(threshold) or threshold <= 0:
            continue
        atr_units = pd.to_numeric(
            frame[f"major_move_atr_units_{horizon}"],
            errors="coerce",
        )
        spread_units = pd.to_numeric(
            frame[f"major_move_to_spread_{horizon}"],
            errors="coerce",
        )
        mask = (
            (peak >= threshold)
            & (atr_units >= MAJOR_MOVE_MIN_ATR_UNITS)
            & (spread_units >= MAJOR_MOVE_MIN_SPREAD_MULTIPLE)
        )
        for timestamp, row in frame.loc[mask].iterrows():
            up = safe_float(row.get(f"future_up_excursion_pips_{horizon}"))
            down = safe_float(row.get(f"future_down_excursion_pips_{horizon}"))
            direction = "LONG" if up >= down else "SHORT"
            endpoint = safe_float(row.get(f"future_move_pips_{horizon}"))
            record: Dict[str, Any] = {
                "instrument": instrument,
                "start_utc": timestamp,
                "end_utc": timestamp + pd.Timedelta(minutes=horizon),
                "horizon_minutes": horizon,
                "direction": direction,
                "peak_move_pips": max(up, down),
                "up_excursion_pips": up,
                "down_excursion_pips": down,
                "endpoint_move_pips": endpoint,
                "historical_q995_pips": threshold,
                "severity_vs_q995": max(up, down) / max(threshold, 0.1),
                "move_atr_units": safe_float(
                    row.get(f"major_move_atr_units_{horizon}")
                ),
                "move_to_spread": safe_float(
                    row.get(f"major_move_to_spread_{horizon}")
                ),
                "spread_pips": safe_float(row.get("spread_pips")),
                "atr240_pips": safe_float(row.get("atr240_pips")),
            }
            for feature in profile_features:
                record[feature] = safe_float(row.get(feature), np.nan)
            rows.append(record)
    return pd.DataFrame(rows)


def deduplicate_major_move_catalog(candidates: pd.DataFrame) -> pd.DataFrame:
    """Collapse overlapping horizons and adjacent starts into one move."""
    if candidates.empty:
        return candidates
    selected: List[pd.Series] = []
    for _, group in candidates.groupby("instrument"):
        accepted: List[pd.Series] = []
        for _, row in group.sort_values(
            ["start_utc", "severity_vs_q995"],
            ascending=[True, False],
        ).iterrows():
            if not accepted:
                accepted.append(row)
                continue
            previous = accepted[-1]
            overlap_start = max(row.start_utc, previous.start_utc)
            overlap_end = min(row.end_utc, previous.end_utc)
            overlap_ratio = 0.0
            if overlap_end > overlap_start:
                overlap_seconds = (overlap_end - overlap_start).total_seconds()
                shortest = min(
                    (row.end_utc - row.start_utc).total_seconds(),
                    (previous.end_utc - previous.start_utc).total_seconds(),
                )
                overlap_ratio = overlap_seconds / max(shortest, 1.0)
            starts_close = (
                abs((row.start_utc - previous.start_utc).total_seconds())
                <= 15 * 60
            )
            if overlap_ratio >= 0.30 or starts_close:
                current_rank = (
                    safe_float(row.severity_vs_q995)
                    * safe_float(row.move_atr_units)
                )
                previous_rank = (
                    safe_float(previous.severity_vs_q995)
                    * safe_float(previous.move_atr_units)
                )
                if current_rank > previous_rank:
                    accepted[-1] = row
                continue
            accepted.append(row)
        selected.extend(accepted)
    return pd.DataFrame(selected).sort_values(
        ["start_utc", "instrument"],
    ).reset_index(drop=True)


def save_major_move_catalog(
    candidates: pd.DataFrame,
    *,
    suffix: str = "",
) -> Dict[str, Any]:
    catalog = deduplicate_major_move_catalog(candidates)
    parquet_path = suffixed_research_path("historical_major_moves", suffix, ".parquet")
    csv_path = suffixed_research_path("historical_major_moves", suffix, ".csv")
    profile_path = suffixed_research_path(
        "historical_major_move_feature_profiles",
        suffix,
        ".csv",
    )
    metadata_path = suffixed_research_path(
        "historical_major_moves",
        suffix,
        ".metadata.json",
    )
    catalog.to_parquet(parquet_path, compression="zstd", index=False)
    catalog.to_csv(csv_path, index=False)
    profile_features = EVENT_PROFILE_FEATURES
    profile_rows: List[Dict[str, Any]] = []
    if not catalog.empty:
        for (horizon, direction), group in catalog.groupby(
            ["horizon_minutes", "direction"]
        ):
            for feature in profile_features:
                values = pd.to_numeric(group[feature], errors="coerce").dropna()
                if values.empty:
                    continue
                profile_rows.append({
                    "horizon_minutes": horizon,
                    "direction": direction,
                    "feature": feature,
                    "events": len(values),
                    "q10": float(values.quantile(0.10)),
                    "q25": float(values.quantile(0.25)),
                    "median": float(values.median()),
                    "q75": float(values.quantile(0.75)),
                    "q90": float(values.quantile(0.90)),
                })
    pd.DataFrame(profile_rows).to_csv(profile_path, index=False)
    metadata = {
        "generated_utc": iso_utc(),
        "catalog_role": (
            "descriptive historical event inventory and factor-model labels; "
            "not a standalone technical strategy"
        ),
        "rows": len(catalog),
        "instruments": (
            int(catalog["instrument"].nunique()) if not catalog.empty else 0
        ),
        "start": (
            catalog["start_utc"].min() if not catalog.empty else None
        ),
        "end": catalog["end_utc"].max() if not catalog.empty else None,
        "horizons": MAJOR_MOVE_CATALOG_HORIZONS,
        "criteria": {
            "pair_quantile": MAJOR_MOVE_QUANTILE,
            "minimum_atr_units": MAJOR_MOVE_MIN_ATR_UNITS,
            "minimum_move_to_spread": MAJOR_MOVE_MIN_SPREAD_MULTIPLE,
            "overlap_deduplication": 0.30,
        },
        "catalog_parquet": str(parquet_path),
        "catalog_csv": str(csv_path),
        "feature_profiles": str(profile_path),
    }
    save_json(metadata_path, metadata)
    return metadata


def technical_research_feature_root() -> Path:
    override = os.environ.get("OANDA_TECHNICAL_FEATURE_ROOT", "").strip()
    if override:
        return Path(override)
    return PROJECT_ROOT / "data" / "all68_weekly_move_study" / "features"


def technical_research_dataset_paths() -> tuple[Path, Path, Path, str]:
    suffix = research_suffix_text()
    output = suffixed_research_path("technical_spike_research", suffix, ".parquet")
    metadata_path = suffixed_research_path(
        "technical_spike_research",
        suffix,
        ".metadata.json",
    )
    major_move_catalog_path = suffixed_research_path(
        "historical_major_moves",
        suffix,
        ".parquet",
    )
    return output, metadata_path, major_move_catalog_path, suffix


def technical_research_sample_step() -> int:
    return max(
        1,
        safe_int(os.environ.get("OANDA_TECHNICAL_RESEARCH_SAMPLE_STEP"), 3),
    )


def technical_research_source_signature() -> str:
    feature_root = technical_research_feature_root()
    sample_step = technical_research_sample_step()
    payload = [
        ("dataset_version", TECHNICAL_RESEARCH_DATASET_VERSION),
        ("feature_root", str(feature_root.resolve())),
        ("sample_step", sample_step),
        ("technical_model_features", sorted(TECHNICAL_MODEL_FEATURES)),
        ("expanded_indicator_features", sorted(EXPANDED_INDICATOR_FEATURES)),
        # Stop-cap policies such as trailing_stop050 can be synthesized from
        # existing trailing path pips plus ATR at load time.  Keep the signature
        # tied to materialized columns so adding a tighter cap does not force a
        # multi-GB full feature rebuild during live research monitoring.
        (
            "major_move_execution_policies",
            sorted(MAJOR_MOVE_MATERIALIZED_EXECUTION_POLICIES),
        ),
    ]
    for path in sorted(feature_root.glob("*.parquet")):
        stat = path.stat()
        payload.append((path.name, stat.st_size, stat.st_mtime_ns))
    return sha256_text(json_dumps(payload))


def build_technical_spike_research_dataset() -> Path:
    """Build a reusable all-68 dataset containing the technical shadow features."""
    output, metadata_path, major_move_catalog_path, suffix = (
        technical_research_dataset_paths()
    )
    sample_step = technical_research_sample_step()
    source_signature = technical_research_source_signature()
    metadata = load_json(metadata_path, {})
    if (
        output.exists()
        and major_move_catalog_path.exists()
        and metadata.get("source_signature") == source_signature
    ):
        return output
    feature_root = technical_research_feature_root()
    requested = list(dict.fromkeys([
        "close",
        "high",
        "low",
        *RAW_TECHNICAL_SOURCE_FEATURES,
        *[
            f"future_long_net_pips_{horizon}"
            for horizon in MAJOR_MOVE_CATALOG_HORIZONS
        ],
        *[
            f"future_short_net_pips_{horizon}"
            for horizon in MAJOR_MOVE_CATALOG_HORIZONS
        ],
        *[
            f"future_move_pips_{horizon}"
            for horizon in MAJOR_MOVE_CATALOG_HORIZONS
        ],
    ]))
    required_targets = [
        f"{mode}_{kind}_{horizon}"
        for mode in ["continuation", "reversal", "short", "long"]
        for kind in ["net", "net_atr", "profit"]
        for horizon in [30, 60, 120]
    ]
    required_major_move_targets = [
        f"major_{kind}_{horizon}"
        for kind in ["event", "up", "down"]
        for horizon in MAJOR_MOVE_MODEL_HORIZONS
    ]
    required_major_move_lead_event_targets = []
    required_major_move_lead_direction_targets = []
    for horizon in MAJOR_MOVE_MODEL_HORIZONS:
        for lead_minutes in MAJOR_MOVE_LEAD_MINUTES:
            if lead_minutes >= horizon:
                continue
            required_major_move_lead_event_targets.append(
                f"major_event_lead_{lead_minutes}_{horizon}"
            )
            required_major_move_lead_direction_targets.append(
                f"major_direction_up_lead_{lead_minutes}_{horizon}"
            )
    required_profitable_precursor_targets = [
        f"profitable_{kind}_move_{horizon}"
        for kind in ["long", "short", "any"]
        for horizon in PROFITABLE_PRECURSOR_HORIZONS
    ]
    required_curve_targets = [
        f"{mode}_curve_{kind}_{horizon}"
        for mode in ["continuation", "reversal", "short", "long"]
        for kind in ["net_atr", "profit"]
        for horizon in RETURN_CURVE_HORIZONS
    ]
    execution_outcome_columns = [
        f"{side}_{policy}_net_{unit}_{horizon}"
        for side in ["long", "short"]
        for policy in MAJOR_MOVE_EXECUTION_POLICIES
        for unit in ["pips", "atr"]
        for horizon in MAJOR_MOVE_CATALOG_HORIZONS
    ]
    curve_detail_columns = []
    for horizon in RETURN_CURVE_HORIZONS:
        for side in ["long", "short"]:
            for metric in [
                "best_net_pips",
                "early_net_pips",
                "endpoint_net_pips",
                "mae_pips",
                "giveback_pips",
                "efficiency",
                "net_pips",
                "net_atr",
                "profit",
            ]:
                curve_detail_columns.append(f"{side}_curve_{metric}_{horizon}")
        for mode in ["continuation", "reversal"]:
            for metric in ["net_pips", "net_atr", "profit"]:
                curve_detail_columns.append(f"{mode}_curve_{metric}_{horizon}")
        curve_detail_columns.extend([
            f"best_curve_net_atr_{horizon}",
            f"best_curve_direction_up_{horizon}",
        ])
    profitable_precursor_outcome_columns = [
        f"{name}_{horizon}"
        for horizon in PROFITABLE_PRECURSOR_HORIZONS
        for name in [
            "profitable_long_threshold_atr",
            "profitable_short_threshold_atr",
            "profitable_long_to_spread",
            "profitable_short_to_spread",
            "best_net_atr",
            "best_direction_up",
        ]
    ]
    major_move_profile_columns = [
        f"major_move_{metric}_{horizon}"
        for metric in ["peak_pips", "atr_units", "to_spread", "threshold_pips"]
        for horizon in MAJOR_MOVE_MODEL_HORIZONS
    ] + [
        f"major_direction_up_{horizon}"
        for horizon in MAJOR_MOVE_MODEL_HORIZONS
    ]
    output_columns = list(dict.fromkeys([
        "time_utc",
        "instrument",
        "pair_taxonomy_primary",
        "regime_primary",
        *TECHNICAL_MODEL_FEATURES,
        *required_targets,
        *required_major_move_targets,
        *required_major_move_lead_event_targets,
        *required_major_move_lead_direction_targets,
        *major_move_profile_columns,
        *required_profitable_precursor_targets,
        *profitable_precursor_outcome_columns,
        *required_curve_targets,
        *execution_outcome_columns,
        *curve_detail_columns,
    ]))
    required_research_columns = list(dict.fromkeys([
        "time_utc",
        *TECHNICAL_CORE_FEATURES,
        *required_targets,
        *required_major_move_targets,
        *required_major_move_lead_event_targets,
        *required_profitable_precursor_targets,
        *required_curve_targets,
    ]))
    frames = []
    major_move_candidates = []
    profitable_precursor_candidates = []
    for path in sorted(feature_root.glob("*.parquet")):
        try:
            frame = pd.read_parquet(path, columns=requested)
        except Exception:
            continue
        frame.index = pd.to_datetime(frame.index, errors="coerce", utc=True)
        frame = add_major_move_fields(frame, path.stem)
        frame = add_profitable_precursor_fields(frame, path.stem)
        frame = add_return_curve_fields(frame, path.stem)
        if sample_step > 1:
            frame = frame.iloc[::sample_step].copy()
        else:
            frame = frame.copy()
        frame["time_utc"] = frame.index
        frame["instrument"] = path.stem
        frame = add_trend_channel_features(frame, path.stem)
        m5 = frame["momentum_5_atr"]
        m15 = frame["momentum_15_atr"]
        m30 = frame["momentum_30_atr"]
        strength = frame["strength_gap_15"]
        frame["shadow_m30_strength"] = (
            (m30 >= 1.88) & (strength >= 0.00045)
        ).astype(int)
        frame["shadow_m15_strength"] = (
            (m15 >= 1.35) & (strength >= 0.00045)
        ).astype(int)
        frame["shadow_m5_strength"] = (
            (m5 >= 0.80) & (strength >= 0.00045)
        ).astype(int)
        frame["shadow_m15_m30"] = (
            (m15 >= 1.35) & (m30 >= 1.88)
        ).astype(int)
        frame["shadow_m5_m30"] = (
            (m5 >= 0.80) & (m30 >= 1.88)
        ).astype(int)
        shadow_columns = [
            "shadow_m30_strength",
            "shadow_m15_strength",
            "shadow_m5_strength",
            "shadow_m15_m30",
            "shadow_m5_m30",
        ]
        frame["shadow_rule_count"] = frame[shadow_columns].sum(axis=1)
        frame = add_calendar_features(frame)
        for key, value in instrument_subset_flags(path.stem).items():
            frame[key] = value
        frame["pair_taxonomy_primary"] = pair_taxonomy_primary(path.stem)
        for key, value in pair_taxonomy_features(path.stem).items():
            frame[key] = value
        for key, value in carry_proxy_features(path.stem).items():
            frame[key] = value
        frame = add_regime_features(frame, path.stem)
        for key, value in neutral_macro_bias_features(path.stem).items():
            frame[key] = value
        frame = add_expanded_indicator_features(frame, path.stem)
        frame = frame.copy()
        for horizon in [30, 60, 120]:
            long_net = frame[f"future_long_net_pips_{horizon}"]
            short_net = frame[f"future_short_net_pips_{horizon}"]
            continuation = np.where(m30 >= 0, long_net, short_net)
            reversal = np.where(m30 >= 0, short_net, long_net)
            frame[f"continuation_net_{horizon}"] = continuation
            frame[f"continuation_net_atr_{horizon}"] = (
                continuation / frame["atr240_pips"].clip(lower=0.1)
            )
            frame[f"continuation_profit_{horizon}"] = (
                continuation > 0
            ).astype(int)
            frame[f"reversal_net_{horizon}"] = reversal
            frame[f"reversal_net_atr_{horizon}"] = (
                reversal / frame["atr240_pips"].clip(lower=0.1)
            )
            frame[f"reversal_profit_{horizon}"] = (
                reversal > 0
            ).astype(int)
            frame[f"short_net_{horizon}"] = short_net
            frame[f"short_net_atr_{horizon}"] = (
                short_net / frame["atr240_pips"].clip(lower=0.1)
            )
            frame[f"short_profit_{horizon}"] = (
                short_net > 0
            ).astype(int)
            frame[f"long_net_{horizon}"] = long_net
            frame[f"long_net_atr_{horizon}"] = (
                long_net / frame["atr240_pips"].clip(lower=0.1)
            )
            frame[f"long_profit_{horizon}"] = (
                long_net > 0
            ).astype(int)
        catalog_rows = major_move_catalog_candidates(frame, path.stem)
        if not catalog_rows.empty:
            major_move_candidates.append(catalog_rows)
        precursor_rows = profitable_move_precursor_candidates(frame, path.stem)
        if not precursor_rows.empty:
            profitable_precursor_candidates.append(precursor_rows)
        required_available = [
            column for column in required_research_columns if column in frame
        ]
        if required_available:
            frame = frame.dropna(subset=required_available)
        keep_columns = [column for column in output_columns if column in frame]
        frame = frame.loc[:, keep_columns].copy()
        numeric_columns = [
            column
            for column in frame.columns
            if column
            not in {
                "time_utc",
                "instrument",
                "pair_taxonomy_primary",
                "regime_primary",
            }
        ]
        for column in numeric_columns:
            frame[column] = pd.to_numeric(
                frame[column],
                errors="coerce",
            ).astype("float32")
        frames.append(frame.reset_index(drop=True))
    if not frames:
        raise RuntimeError("No technical feature parquet files were available")
    combined = pd.concat(frames, ignore_index=True, copy=False)
    for column in ["instrument", "pair_taxonomy_primary", "regime_primary"]:
        if column in combined:
            combined[column] = combined[column].astype("category")
    output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output, compression="zstd", index=False)
    major_move_metadata = save_major_move_catalog(
        pd.concat(major_move_candidates, ignore_index=True)
        if major_move_candidates
        else pd.DataFrame(),
        suffix=suffix,
    )
    profitable_precursor_metadata = save_profitable_move_precursor_catalog(
        pd.concat(profitable_precursor_candidates, ignore_index=True)
        if profitable_precursor_candidates
        else pd.DataFrame(),
        suffix=suffix,
    )
    save_json(metadata_path, {
        "generated_utc": iso_utc(),
        "source_signature": source_signature,
        "feature_root": str(feature_root),
        "dataset_suffix": suffix,
        "sample_step": sample_step,
        "rows": len(combined),
        "instruments": int(combined["instrument"].nunique()),
        "start": combined["time_utc"].min(),
        "end": combined["time_utc"].max(),
        "features": TECHNICAL_MODEL_FEATURES,
        "major_move_catalog": major_move_metadata,
        "profitable_move_precursor_catalog": profitable_precursor_metadata,
    })
    return output


def generated_experiment_spec(index: int) -> Dict[str, Any]:
    rng = random.Random(index * 104729 + 17)
    roll = rng.random()
    if roll < 0.55:
        dataset_kind = "technical_spike"
    elif roll < 0.88:
        dataset_kind = "profitable_move_precursor"
    else:
        dataset_kind = "base_trade_quality"
    model_choices = [
        "random_forest",
        "gradient_boosting",
        "extra_trees",
        "hist_gradient_boosting",
    ]
    if LIGHTGBM_AVAILABLE:
        model_choices.extend(["lightgbm", "lightgbm"])
    if XGBOOST_AVAILABLE:
        model_choices.append("xgboost")
    if CATBOOST_AVAILABLE:
        model_choices.extend(["catboost", "catboost"])
    if NGBOOST_AVAILABLE:
        model_choices.append("ngboost")
    model_type = rng.choice(model_choices)
    instrument_subset = "all"
    if dataset_kind in {"technical_spike", "profitable_move_precursor"}:
        instrument_subset = rng.choice([
            "all",
            "all",
            "majors",
            "volatile",
            "volatile",
            "exotic",
            "volatile_exotic",
            "non_usd_volatile",
        ])
    if dataset_kind == "technical_spike":
        horizon = rng.choice([30, 60, 120])
        if rng.random() < 0.45:
            execution_policy = rng.choice([
                "trailing_stop050",
                "trailing_stop075",
                "trailing_stop075",
                "trailing_stop100",
                "trailing",
                "fixed",
            ])
            valid_leads = [
                lead_minutes
                for lead_minutes in MAJOR_MOVE_LEAD_MINUTES
                if lead_minutes < horizon
            ]
            if valid_leads and rng.random() < 0.75:
                lead_minutes = rng.choice(valid_leads)
                target = f"major_event_lead_{lead_minutes}_{horizon}"
                direction_target = (
                    f"major_direction_up_lead_{lead_minutes}_{horizon}"
                )
            else:
                target = f"major_event_{horizon}"
                direction_target = f"major_direction_up_{horizon}"
            outcome = f"two_stage_{execution_policy}_net_atr_{horizon}"
            research_role = "major_move_factor"
        else:
            mode = rng.choice(["continuation", "reversal", "short", "long"])
            if rng.random() < 0.40:
                target = f"{mode}_curve_profit_{horizon}"
                outcome = f"{mode}_curve_net_atr_{horizon}"
                execution_policy = "curve"
                research_role = "return_curve_candidate"
            else:
                target = f"{mode}_profit_{horizon}"
                outcome = f"{mode}_net_atr_{horizon}"
                execution_policy = ""
                research_role = "standalone_candidate"
            direction_target = ""
        feature_set = rng.choice(["technical_core", "technical_full"])
    elif dataset_kind == "profitable_move_precursor":
        horizon = rng.choice(PROFITABLE_PRECURSOR_HORIZONS)
        direction = rng.choice(["long", "short", "any"])
        target = f"profitable_{direction}_move_{horizon}"
        outcome = (
            f"{direction}_net_atr_{horizon}"
            if direction in {"long", "short"}
            else f"best_net_atr_{horizon}"
        )
        feature_set = rng.choice(["technical_core", "technical_full"])
        direction_target = (
            f"best_direction_up_{horizon}" if direction == "any" else ""
        )
        execution_policy = ""
        research_role = "profitable_move_precursor"
    else:
        target = "would_profit_30m"
        outcome = "net_vol_units_30"
        feature_set = "base"
        direction_target = ""
        execution_policy = ""
        research_role = "standalone_candidate"
    if model_type == "random_forest":
        parameters = {
            "n_estimators": rng.choice([100, 140, 180, 240]),
            "max_depth": rng.choice([5, 7, 9, 12, None]),
            "min_samples_leaf": rng.choice([5, 10, 20, 40]),
            "max_features": rng.choice(["sqrt", "log2", 0.7, 1.0]),
            "class_weight": rng.choice(["balanced", "balanced_subsample"]),
        }
    elif model_type == "extra_trees":
        parameters = {
            "n_estimators": rng.choice([160, 240, 360, 500]),
            "max_depth": rng.choice([7, 9, 12, None]),
            "min_samples_leaf": rng.choice([5, 10, 20, 40]),
            "max_features": rng.choice(["sqrt", "log2", 0.7, 1.0]),
            "class_weight": rng.choice(["balanced", "balanced_subsample"]),
        }
    elif model_type == "hist_gradient_boosting":
        parameters = {
            "max_iter": rng.choice([100, 160, 240, 320]),
            "learning_rate": rng.choice([0.025, 0.05, 0.075]),
            "max_leaf_nodes": rng.choice([15, 31, 63]),
            "min_samples_leaf": rng.choice([20, 40, 80]),
            "l2_regularization": rng.choice([0.0, 0.01, 0.05, 0.10]),
        }
    elif model_type == "gradient_boosting":
        parameters = {
            "n_estimators": rng.choice([60, 100, 140, 180]),
            "learning_rate": rng.choice([0.025, 0.05, 0.075, 0.10]),
            "max_depth": rng.choice([2, 3, 4, 5]),
            "min_samples_leaf": rng.choice([10, 20, 40]),
            "subsample": rng.choice([0.65, 0.8, 1.0]),
        }
    elif model_type == "lightgbm":
        parameters = {
            "n_estimators": rng.choice([120, 180, 240, 320]),
            "learning_rate": rng.choice([0.025, 0.05, 0.075]),
            "num_leaves": rng.choice([15, 31, 63]),
            "min_child_samples": rng.choice([20, 40, 80]),
            "subsample": rng.choice([0.70, 0.85, 1.0]),
            "colsample_bytree": rng.choice([0.70, 0.85, 1.0]),
        }
    elif model_type == "xgboost":
        parameters = {
            "n_estimators": rng.choice([120, 180, 240]),
            "learning_rate": rng.choice([0.025, 0.05, 0.075]),
            "max_depth": rng.choice([3, 4, 5]),
            "subsample": rng.choice([0.70, 0.85, 1.0]),
            "colsample_bytree": rng.choice([0.70, 0.85, 1.0]),
        }
    elif model_type == "catboost":
        parameters = {
            "iterations": rng.choice([160, 240, 320]),
            "learning_rate": rng.choice([0.025, 0.05, 0.075]),
            "depth": rng.choice([4, 6, 8]),
            "l2_leaf_reg": rng.choice([1.0, 3.0, 5.0, 9.0]),
            "random_strength": rng.choice([0.5, 1.0, 2.0]),
        }
    else:
        parameters = {
            "n_estimators": rng.choice([160, 240, 320]),
            "learning_rate": rng.choice([0.01, 0.025, 0.05]),
            "minibatch_frac": rng.choice([0.65, 0.8, 1.0]),
            "col_sample": rng.choice([0.65, 0.8, 1.0]),
        }
    return {
        "source": "generated",
        "evaluation_stage": "screen",
        "validation_weeks": CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS,
        "max_train_rows": CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS,
        "dataset_kind": dataset_kind,
        "model_type": model_type,
        "target": target,
        "outcome": outcome,
        "feature_set": feature_set,
        "instrument_subset": instrument_subset,
        "research_role": research_role,
        "direction_target": direction_target,
        "execution_policy": execution_policy,
        "parameters": parameters,
    }


def normalize_gpt_experiment_spec(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    dataset_kind = str(raw.get("dataset_kind", "")).strip()
    model_type = str(raw.get("model_type", "")).strip()
    feature_set = str(raw.get("feature_set", "")).strip()
    instrument_subset = str(raw.get("instrument_subset") or "all").strip().lower()
    target = str(raw.get("target", "")).strip()
    outcome = str(raw.get("outcome", "")).strip()
    research_role = str(raw.get("research_role", "")).strip()
    direction_target = str(raw.get("direction_target", "")).strip()
    execution_policy = str(raw.get("execution_policy", "")).strip()
    instrument_whitelist = normalized_instrument_whitelist(
        raw.get("instrument_whitelist"),
    )
    segment_filters = normalized_segment_filters(raw.get("segment_filters"))
    if dataset_kind not in {
        "technical_spike",
        "profitable_move_precursor",
        "base_trade_quality",
    }:
        return None
    if model_type not in {
        "random_forest",
        "gradient_boosting",
        "extra_trees",
        "hist_gradient_boosting",
        "lightgbm",
        "xgboost",
        "catboost",
        "ngboost",
    }:
        return None
    if feature_set not in {"base", "technical_core", "technical_full"}:
        return None
    if instrument_subset not in VALID_INSTRUMENT_SUBSETS:
        instrument_subset = "all"
    if dataset_kind == "base_trade_quality":
        if (
            feature_set != "base"
            or target != "would_profit_30m"
            or outcome != "net_vol_units_30"
        ):
            return None
        instrument_subset = "all"
    elif dataset_kind == "profitable_move_precursor":
        valid_pairs = {
            (
                f"profitable_{direction}_move_{horizon}",
                (
                    f"{direction}_net_atr_{horizon}"
                    if direction in {"long", "short"}
                    else f"best_net_atr_{horizon}"
                ),
            )
            for direction in ["long", "short", "any"]
            for horizon in PROFITABLE_PRECURSOR_HORIZONS
        }
        if feature_set == "base" or (target, outcome) not in valid_pairs:
            return None
        research_role = "profitable_move_precursor"
        if target.startswith("profitable_any_move_"):
            horizon = safe_int(target.rsplit("_", 1)[-1])
            direction_target = f"best_direction_up_{horizon}"
        else:
            direction_target = ""
    else:
        valid_pairs = {
            (
                f"{mode}_profit_{horizon}",
                f"{mode}_net_atr_{horizon}",
            )
            for mode in ["continuation", "reversal", "short", "long"]
            for horizon in [30, 60, 120]
        }
        valid_pairs.update({
            (
                f"{mode}_curve_profit_{horizon}",
                f"{mode}_curve_net_atr_{horizon}",
            )
            for mode in ["continuation", "reversal", "short", "long"]
            for horizon in RETURN_CURVE_HORIZONS
        })
        valid_pairs.update({
            (f"major_up_{horizon}", f"long_net_atr_{horizon}")
            for horizon in MAJOR_MOVE_MODEL_HORIZONS
        })
        valid_pairs.update({
            (f"major_down_{horizon}", f"short_net_atr_{horizon}")
            for horizon in MAJOR_MOVE_MODEL_HORIZONS
        })
        valid_pairs.update({
            (
                f"major_event_{horizon}",
                f"two_stage_{policy}_net_atr_{horizon}",
            )
            for horizon in MAJOR_MOVE_MODEL_HORIZONS
            for policy in MAJOR_MOVE_EXECUTION_POLICIES
        })
        valid_pairs.update({
            (
                f"major_event_lead_{lead_minutes}_{horizon}",
                f"two_stage_{policy}_net_atr_{horizon}",
            )
            for horizon in MAJOR_MOVE_MODEL_HORIZONS
            for lead_minutes in MAJOR_MOVE_LEAD_MINUTES
            if lead_minutes < horizon
            for policy in MAJOR_MOVE_EXECUTION_POLICIES
        })
        if feature_set == "base" or (target, outcome) not in valid_pairs:
            return None
        if target.startswith("major_event_lead_"):
            research_role = "major_move_factor"
            parts = target.rsplit("_", 2)
            lead_minutes = safe_int(parts[-2])
            horizon = safe_int(parts[-1])
            direction_target = f"major_direction_up_lead_{lead_minutes}_{horizon}"
            execution_policy = next(
                (
                    policy
                    for policy in sorted(
                        MAJOR_MOVE_EXECUTION_POLICIES,
                        key=len,
                        reverse=True,
                    )
                    if f"two_stage_{policy}_net_atr_" in outcome
                ),
                "trailing" if "_trailing_" in outcome else "fixed",
            )
        elif target.startswith("major_event_"):
            research_role = "major_move_factor"
            horizon = safe_int(target.rsplit("_", 1)[-1])
            direction_target = f"major_direction_up_{horizon}"
            execution_policy = next(
                (
                    policy
                    for policy in sorted(
                        MAJOR_MOVE_EXECUTION_POLICIES,
                        key=len,
                        reverse=True,
                    )
                    if f"two_stage_{policy}_net_atr_" in outcome
                ),
                "trailing" if "_trailing_" in outcome else "fixed",
            )
        elif target.startswith("major_"):
            research_role = "major_move_factor_legacy"
        elif "_curve_profit_" in target:
            research_role = "return_curve_candidate"
            execution_policy = "curve"
    if not research_role:
        research_role = "standalone_candidate"
    if not target or not outcome:
        return None
    evaluation_stage = str(raw.get("evaluation_stage") or "screen").lower()
    if evaluation_stage not in {"screen", "validation"}:
        evaluation_stage = "screen"
    if evaluation_stage == "validation":
        # Validation specs are queued internally only after a screen pass.
        evaluation_stage = "screen"
    spec = {
        "source": "gpt",
        "evaluation_stage": evaluation_stage,
        "validation_weeks": safe_int(
            raw.get("validation_weeks"),
            CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS,
        ),
        "max_train_rows": safe_int(
            raw.get("max_train_rows"),
            CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS,
        ),
        "dataset_kind": dataset_kind,
        "model_type": model_type,
        "target": target,
        "outcome": outcome,
        "feature_set": feature_set,
        "instrument_subset": instrument_subset,
        "research_role": research_role,
        "direction_target": direction_target,
        "execution_policy": execution_policy,
        "parameters": dict(raw.get("parameters") or {}),
    }
    if instrument_whitelist:
        spec["instrument_whitelist"] = instrument_whitelist
    if segment_filters:
        spec["segment_filters"] = segment_filters
    return spec


def research_spec_hash(spec: Dict[str, Any]) -> str:
    return experiment_spec_hash(spec)


class ContinuousResearchEngine:
    """Always-on non-repeating model search over historical weekday FX data."""

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.state_path = DIRS["research"] / "research_state.json"
        self.state = load_json(self.state_path, {})
        self.state.setdefault("generated_index", 0)
        self.state.setdefault("completed_spec_hashes", [])
        self._cache_key = ""
        self._cache_frame = pd.DataFrame()
        self._thread: Optional[threading.Thread] = None
        self.registry = ModelLifecycleRegistry(
            DIRS["registry"],
            DIRS["promotions"],
        )

    def seed_exhaustion_hybrid_priority_specs(self) -> None:
        """Keep the validated exhaustion/RF scout family in the always-on queue."""
        flag = "exhaustion_hybrid_priority_specs_seeded_v1"
        if self.state.get(flag):
            return
        queued_hashes = {
            experiment_spec_hash(spec)
            for spec in self.registry.queued_specs()
        }
        completed_hashes = self.completed_hashes()
        model_parameters = {
            "random_forest": {
                "n_estimators": 240,
                "max_depth": 9,
                "min_samples_leaf": 10,
                "max_features": 0.7,
                "class_weight": "balanced_subsample",
            },
            "extra_trees": {
                "n_estimators": 360,
                "max_depth": 12,
                "min_samples_leaf": 10,
                "max_features": 0.7,
                "class_weight": "balanced_subsample",
            },
            "hist_gradient_boosting": {
                "max_iter": 240,
                "learning_rate": 0.05,
                "max_leaf_nodes": 31,
                "min_samples_leaf": 40,
                "l2_regularization": 0.05,
            },
        }
        seeded = 0
        for subset in ["volatile_exotic", "non_usd_volatile", "volatile"]:
            for feature_set in ["technical_full", "technical_core"]:
                for model_type, parameters in model_parameters.items():
                    for policy in [
                        "trailing_stop050",
                        "trailing_stop075",
                        "trailing_stop100",
                    ]:
                        spec = {
                            "source": "operator_exhaustion_hybrid",
                            "evaluation_stage": "screen",
                            "validation_weeks": max(
                                CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS,
                                6,
                            ),
                            "max_train_rows": max(
                                CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS,
                                150000,
                            ),
                            "dataset_kind": "technical_spike",
                            "model_type": model_type,
                            "target": "major_event_lead_30_60",
                            "outcome": f"two_stage_{policy}_net_atr_60",
                            "feature_set": feature_set,
                            "instrument_subset": subset,
                            "research_role": "major_move_factor",
                            "direction_target": "major_direction_up_lead_30_60",
                            "execution_policy": policy,
                            "parameters": dict(parameters),
                        }
                        spec_hash = experiment_spec_hash(spec)
                        if spec_hash in queued_hashes or spec_hash in completed_hashes:
                            continue
                        self.registry.enqueue(
                            spec,
                            reason=(
                                "operator_exhaustion_hybrid_30_60_stopcap: "
                                "RF/trees model-gated scout family seeded from "
                                "validated exhaustion-reversal offline evidence"
                            ),
                        )
                        queued_hashes.add(spec_hash)
                        seeded += 1
        self.state[flag] = iso_utc()
        self.state["exhaustion_hybrid_priority_specs_seeded"] = seeded
        save_json(self.state_path, self.state)
        log_audit({
            "event_type": "exhaustion_hybrid_priority_seed",
            "status": "seeded",
            "reason": "queued model-gated exhaustion scout candidates",
            "result": json_dumps({"seeded": seeded})[:1000],
        })

    def seed_reversal_curve_focus_specs(self) -> None:
        """Seed the strongest observed scout families with realistic validation.

        Recent ledger results show reversal_curve/reversal_profit random forests
        on volatile and non-USD volatile subsets are cleaner than the naive
        lead-stopcap sweep.  Keep testing that area, plus precursor labels that
        ask the inverse question: which pre-move states led to profitable short
        or any-direction movement.
        """
        flag = "reversal_curve_focus_specs_seeded_v1"
        if self.state.get(flag):
            return
        queued_hashes = {
            experiment_spec_hash(spec)
            for spec in self.registry.queued_specs()
        }
        completed_hashes = self.completed_hashes()
        model_parameters = {
            "random_forest": {
                "n_estimators": 240,
                "max_depth": 9,
                "min_samples_leaf": 10,
                "max_features": 0.7,
                "class_weight": "balanced_subsample",
            },
            "extra_trees": {
                "n_estimators": 360,
                "max_depth": 12,
                "min_samples_leaf": 10,
                "max_features": 0.7,
                "class_weight": "balanced_subsample",
            },
            "hist_gradient_boosting": {
                "max_iter": 240,
                "learning_rate": 0.05,
                "max_leaf_nodes": 31,
                "min_samples_leaf": 40,
                "l2_regularization": 0.05,
            },
        }
        technical_pairs = [
            ("reversal_profit_60", "reversal_net_atr_60", "standalone_candidate", ""),
            ("reversal_curve_profit_120", "reversal_curve_net_atr_120", "return_curve_candidate", "curve"),
            ("short_curve_profit_120", "short_curve_net_atr_120", "return_curve_candidate", "curve"),
        ]
        precursor_pairs = [
            ("profitable_short_move_60", "short_net_atr_60", ""),
            ("profitable_any_move_60", "best_net_atr_60", "best_direction_up_60"),
            ("profitable_short_move_120", "short_net_atr_120", ""),
            ("profitable_any_move_120", "best_net_atr_120", "best_direction_up_120"),
        ]
        seeded = 0
        for subset in ["non_usd_volatile", "volatile_exotic", "volatile"]:
            for feature_set in ["technical_full", "technical_core"]:
                for model_type, parameters in model_parameters.items():
                    for target, outcome, role, policy in technical_pairs:
                        spec = {
                            "source": "operator_reversal_curve_focus",
                            "evaluation_stage": "screen",
                            "validation_weeks": max(
                                CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS,
                                6,
                            ),
                            "max_train_rows": max(
                                CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS,
                                150000,
                            ),
                            "dataset_kind": "technical_spike",
                            "model_type": model_type,
                            "target": target,
                            "outcome": outcome,
                            "feature_set": feature_set,
                            "instrument_subset": subset,
                            "research_role": role,
                            "direction_target": "",
                            "execution_policy": policy,
                            "parameters": dict(parameters),
                        }
                        spec_hash = experiment_spec_hash(spec)
                        if spec_hash in queued_hashes or spec_hash in completed_hashes:
                            continue
                        self.registry.enqueue(
                            spec,
                            reason=(
                                "operator_reversal_curve_focus: seed "
                                "episode-aware reversal/short curve candidates"
                            ),
                        )
                        queued_hashes.add(spec_hash)
                        seeded += 1
                for model_type in ["random_forest", "extra_trees"]:
                    parameters = model_parameters[model_type]
                    for target, outcome, direction_target in precursor_pairs:
                        spec = {
                            "source": "operator_reversal_curve_focus",
                            "evaluation_stage": "screen",
                            "validation_weeks": max(
                                CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS,
                                6,
                            ),
                            "max_train_rows": max(
                                CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS,
                                150000,
                            ),
                            "dataset_kind": "profitable_move_precursor",
                            "model_type": model_type,
                            "target": target,
                            "outcome": outcome,
                            "feature_set": feature_set,
                            "instrument_subset": subset,
                            "research_role": "profitable_move_precursor",
                            "direction_target": direction_target,
                            "execution_policy": "",
                            "parameters": dict(parameters),
                        }
                        spec_hash = experiment_spec_hash(spec)
                        if spec_hash in queued_hashes or spec_hash in completed_hashes:
                            continue
                        self.registry.enqueue(
                            spec,
                            reason=(
                                "operator_reversal_curve_focus: seed "
                                "profitable-move precursor candidates"
                            ),
                        )
                        queued_hashes.add(spec_hash)
                        seeded += 1
        self.state[flag] = iso_utc()
        self.state["reversal_curve_focus_specs_seeded"] = seeded
        save_json(self.state_path, self.state)
        log_audit({
            "event_type": "reversal_curve_focus_seed",
            "status": "seeded",
            "reason": "queued episode-aware reversal and precursor candidates",
            "result": json_dumps({"seeded": seeded})[:1000],
        })

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.seed_exhaustion_hybrid_priority_specs()
        self.seed_reversal_curve_focus_specs()
        self._thread = threading.Thread(
            target=self.run_forever,
            name="continuous-forex-research",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def completed_hashes(self) -> set[str]:
        ledger = read_csv(LOG_FILES["research_experiments"])
        logged = (
            set(ledger.get("spec_hash", pd.Series(dtype=str)).dropna().astype(str))
            if not ledger.empty
            else set()
        )
        return logged | set(self.state.get("completed_spec_hashes", []))

    def gpt_specs(self) -> List[Dict[str, Any]]:
        specs = []
        for path in sorted(DIRS["research_specs"].glob("*.json")):
            payload = load_json(path, {})
            for raw in payload.get("model_experiments", []):
                normalized = normalize_gpt_experiment_spec(raw)
                if normalized:
                    specs.append(normalized)
        return specs

    def prioritized_queued_spec(
        self,
        completed_hashes: Iterable[str],
    ) -> Optional[Dict[str, Any]]:
        completed = set(completed_hashes)
        subset_priority = {
            "volatile": 0,
            "non_usd_volatile": 1,
            "exotic": 2,
            "volatile_exotic": 3,
            "jpy_risk": 4,
            "exotic_high_spread": 5,
            "volatile_non_usd": 6,
            "commodity": 7,
            "chf_safe_haven": 8,
            "eur_gbp_cross": 9,
            "usd_other": 10,
            "majors": 11,
            "all": 12,
        }
        target_priority = {
            "major_event_lead_30_60": 0,
            "major_event_lead_15_60": 1,
            # The strongest offline scout evidence so far was 15-minute lead /
            # 120-minute event horizon with a tight stop cap.  Run that family
            # before the weaker 30-minute lead / 120-minute queue variants.
            "major_event_lead_15_120": 2,
            "major_event_lead_30_120": 3,
            "major_event_lead_60_120": 4,
        }
        model_priority = {
            "random_forest": 0,
            "extra_trees": 1,
            "gradient_boosting": 2,
            "hist_gradient_boosting": 3,
            "lightgbm": 4,
            "xgboost": 5,
            "catboost": 6,
            "ngboost": 7,
            "logistic": 8,
        }
        policy_priority = {
            "trailing_stop025": 0,
            "trailing_stop020": 1,
            "trailing_stop015": 2,
            "trailing_stop012": 3,
            "trailing_stop010": 4,
            "trailing_stop035": 5,
            "trailing_stop050": 6,
            "trailing_stop075": 7,
            "trailing_stop100": 8,
            "trailing": 9,
            "fixed": 10,
            "curve": 11,
            "": 12,
        }
        source_priority = {
            "operator_live_missed_spike_subset_followup_v1": -2,
            "operator_live_ev_near_miss_followup_v1": -1,
            "operator_scout_value_capture_v1": 0,
            "operator_live_value_miss_followup_v1": 0,
            "operator_live_unknown_profile_followup_v1": 0,
            "operator_reversal_curve_focus": 0,
            "operator_exhaustion_hybrid": 1,
            "operator_segment_filter_primary_v1": 0,
            "operator_segment_filter_tech_v1": 1,
            "operator_segment_filter_gpt_support_v1": 2,
            "operator_weekend_primary_improvement_v1": 0,
            "operator_weekend_tech_improvement_v1": 1,
            "operator_weekend_gpt_support_v1": 2,
            "seeded_stopcap_priority": 2,
            "model_space_agenda": 3,
            "gpt": 4,
            "generated": 5,
        }
        deployment_lane_priority = {
            # Primary had no account-specific completed model evidence when the
            # account-lane queue was seeded.  Give it first coverage within the
            # live-split research class, while validation specs still retain
            # absolute priority above all new screens.
            "live_primary_challenger_candidate": 0,
            "live_tech_champion_candidate": 1,
            "gpt_live_model_support": 2,
            "": 3,
        }
        queued_specs = self.registry.queued_specs()
        primary_lane_completed = sum(
            1
            for spec in queued_specs
            if str(spec.get("deployment_lane", "")).lower()
            == "live_primary_challenger_candidate"
            and experiment_spec_hash(spec) in completed
        )
        primary_lane_min_completed = int(os.getenv(
            "OANDA_PRIMARY_LANE_MIN_COMPLETED",
            "24",
        ))
        primary_lane_needs_coverage = (
            primary_lane_completed < max(1, primary_lane_min_completed)
        )
        primary_lane_has_completed = any(
            str(spec.get("deployment_lane", "")).lower()
            == "live_primary_challenger_candidate"
            and experiment_spec_hash(spec) in completed
            for spec in queued_specs
        )
        primary_specialist_pending = any(
            str(spec.get("deployment_lane", "")).lower()
            == "live_primary_challenger_candidate"
            and normalized_instrument_whitelist(spec.get("instrument_whitelist"))
            and experiment_spec_hash(spec) not in completed
            for spec in queued_specs
        )
        gpt_support_completed = sum(
            1
            for spec in queued_specs
            if str(spec.get("deployment_lane", "")).lower()
            == "gpt_live_model_support"
            and experiment_spec_hash(spec) in completed
        )
        gpt_support_min_completed = int(os.getenv(
            "OANDA_GPT_SUPPORT_MIN_COMPLETED",
            "6",
        ))
        gpt_support_needs_coverage = (
            gpt_support_completed < max(1, gpt_support_min_completed)
        )
        lane_recent_counts: Dict[str, int] = {}
        account_model_completed_counts: Dict[Tuple[str, str], int] = {}
        lane_transfer_winners: set[str] = set()
        lane_rotation_recent_rows = int(os.getenv(
            "OANDA_LANE_ROTATION_RECENT_ROWS",
            "60",
        ))
        lane_rotation_soft_cap = int(os.getenv(
            "OANDA_LANE_ROTATION_SOFT_CAP",
            "24",
        ))
        sparse_two_stage_error_keys: set[tuple[str, str, str, str]] = set()
        sparse_two_stage_data_keys: set[tuple[str, str, str]] = set()
        runtime_timeout_keys: set[tuple[str, str, str, str]] = set()
        repeated_weak_screen_keys: set[tuple[str, str, str, str]] = set()
        repeated_weak_screen_data_keys: set[tuple[str, str, str]] = set()
        failed_validation_keys: set[tuple[str, str, str, str]] = set()
        recent_live_validation_duplicate_keys: set[
            tuple[str, str, str, str, str, str, str]
        ] = set()
        recent_live_validation_duplicate_relaxed_counts: Dict[
            tuple[str, str, str, str, str],
            int,
        ] = {}
        recent_live_validation_source_counts: Dict[str, int] = {}
        recent_live_validation_total = 0
        short_recent_live_validation_source_counts: Dict[str, int] = {}
        short_recent_live_validation_total = 0
        sparse_segment_error_keys: set[
            tuple[str, str, str, str, str, str]
        ] = set()
        gpt_low_trade_screen_keys: set[tuple[str, str, str, str]] = set()
        gpt_low_trade_model_subset_keys: set[tuple[str, str, str]] = set()
        try:
            ledger = read_csv(LOG_FILES["research_experiments"])
            if not ledger.empty:
                key_columns = [
                    "dataset_kind",
                    "model_type",
                    "instrument_subset",
                    "target",
                ]
                for column in [
                    *key_columns,
                    "error",
                    "evaluation_stage",
                    "status",
                    "mean_auc",
                    "mean_net_pips",
                    "instrument_whitelist",
                    "segment_filters",
                    "source",
                    "deployment_lane",
                    "trades",
                    "validation_profile",
                    "execution_policy",
                    "specialist_profile",
                ]:
                    if column not in ledger.columns:
                        ledger[column] = ""
                recent_lane_rows = ledger.tail(
                    max(10, lane_rotation_recent_rows),
                )
                for _idx, row in recent_lane_rows.iterrows():
                    recent_lane = str(row.get("deployment_lane") or "").lower()
                    recent_source = str(row.get("source") or "").lower()
                    if not recent_lane:
                        if recent_source == "operator_weekend_primary_improvement_v1":
                            recent_lane = "live_primary_challenger_candidate"
                        elif recent_source == "operator_weekend_tech_improvement_v1":
                            recent_lane = "live_tech_champion_candidate"
                        elif recent_source == "operator_weekend_gpt_support_v1":
                            recent_lane = "gpt_live_model_support"
                    if recent_lane in {
                        "live_primary_challenger_candidate",
                        "live_tech_champion_candidate",
                        "gpt_live_model_support",
                    }:
                        lane_recent_counts[recent_lane] = (
                            lane_recent_counts.get(recent_lane, 0) + 1
                        )
                for _idx, row in ledger.iterrows():
                    completed_source = str(row.get("source") or "").lower()
                    completed_model = str(row.get("model_type") or "").lower()
                    if completed_source in {
                        "operator_weekend_primary_improvement_v1",
                        "operator_weekend_tech_improvement_v1",
                    } and completed_model:
                        key = (completed_source, completed_model)
                        account_model_completed_counts[key] = (
                            account_model_completed_counts.get(key, 0) + 1
                        )
                    completed_lane = str(
                        row.get("deployment_lane") or "",
                    ).lower()
                    completed_profile = str(
                        row.get("specialist_profile") or "",
                    ).lower()
                    completed_target = str(row.get("target") or "").lower()
                    completed_gate = safe_bool(row.get("gate_passed"))
                    completed_mean = safe_float(row.get("mean_net_pips"), 0.0)
                    if (
                        completed_lane
                        and "transfer" in completed_profile
                        and completed_target == "profitable_any_move_120"
                        and completed_gate
                        and completed_mean >= 4.0
                    ):
                        lane_transfer_winners.add(completed_lane)
                recent_errors = ledger.tail(250).copy()
                error_mask = recent_errors["error"].astype(str).str.contains(
                    (
                        "InsufficientRollingFolds|"
                        "No valid purged rolling weekly folds|"
                        "No valid purged two-stage rolling folds"
                    ),
                    case=False,
                    na=False,
                    regex=True,
                )
                recent_errors = recent_errors[error_mask]
                if not recent_errors.empty:
                    grouped = recent_errors.groupby(key_columns, dropna=False)
                    for key, group in grouped:
                        if len(group) >= 2:
                            sparse_two_stage_error_keys.add(
                                tuple(str(part or "").lower() for part in key)
                            )
                    data_key_columns = [
                        "dataset_kind",
                        "instrument_subset",
                        "target",
                    ]
                    for key, group in recent_errors.groupby(
                        data_key_columns,
                        dropna=False,
                    ):
                        if len(group) >= 2:
                            sparse_two_stage_data_keys.add(
                                tuple(str(part or "").lower() for part in key)
                            )
                    segment_key_columns = [
                        "dataset_kind",
                        "model_type",
                        "instrument_subset",
                        "target",
                        "instrument_whitelist",
                        "segment_filters",
                    ]
                    for key, group in recent_errors.groupby(
                        segment_key_columns,
                        dropna=False,
                    ):
                        if len(group) >= 2:
                            sparse_segment_error_keys.add(
                                tuple(str(part or "").lower() for part in key)
                            )
                recent_timeouts = ledger.tail(250).copy()
                timeout_mask = recent_timeouts["error"].astype(str).str.contains(
                    "RuntimeTimeout",
                    case=False,
                    na=False,
                )
                recent_timeouts = recent_timeouts[timeout_mask]
                if not recent_timeouts.empty:
                    for key, _group in recent_timeouts.groupby(
                        key_columns,
                        dropna=False,
                    ):
                        runtime_timeout_keys.add(
                            tuple(str(part or "").lower() for part in key)
                        )
                recent_screens = ledger.tail(350).copy()
                recent_screens = recent_screens[
                    recent_screens["evaluation_stage"].astype(str).str.lower()
                    == "screen"
                ]
                recent_screens = recent_screens[
                    recent_screens["status"].astype(str).str.lower()
                    == "unsuccessful"
                ]
                if not recent_screens.empty:
                    recent_screens["mean_auc_num"] = pd.to_numeric(
                        recent_screens["mean_auc"],
                        errors="coerce",
                    )
                    recent_screens["mean_net_pips_num"] = pd.to_numeric(
                        recent_screens["mean_net_pips"],
                        errors="coerce",
                    )
                    weak_mask = (
                        recent_screens["mean_net_pips_num"].fillna(-1.0) <= 0.0
                    ) | (
                        recent_screens["mean_auc_num"].fillna(0.0) < 0.52
                    )
                    grouped = recent_screens[weak_mask].groupby(
                        key_columns,
                        dropna=False,
                    )
                    for key, group in grouped:
                        if len(group) >= 2:
                            repeated_weak_screen_keys.add(
                                tuple(str(part or "").lower() for part in key)
                            )
                    data_key_columns = [
                        "dataset_kind",
                        "instrument_subset",
                        "target",
                    ]
                    grouped = recent_screens[weak_mask].groupby(
                        data_key_columns,
                        dropna=False,
                    )
                    for key, group in grouped:
                        if len(group) >= 3:
                            repeated_weak_screen_data_keys.add(
                                tuple(str(part or "").lower() for part in key)
                            )
                    recent_screens["trades_num"] = pd.to_numeric(
                        recent_screens["trades"],
                        errors="coerce",
                    )
                    gpt_low_trade = recent_screens[
                        (
                            recent_screens["source"]
                            .astype(str)
                            .str.lower()
                            == "operator_weekend_gpt_support_v1"
                        )
                        & (
                            recent_screens["trades_num"].fillna(0.0)
                            < 20.0
                        )
                    ]
                    if not gpt_low_trade.empty:
                        for key, group in gpt_low_trade.groupby(
                            [
                                "model_type",
                                "instrument_subset",
                                "target",
                                "dataset_kind",
                            ],
                            dropna=False,
                        ):
                            if len(group) >= 3:
                                gpt_low_trade_screen_keys.add(
                                    tuple(
                                        str(part or "").lower()
                                        for part in key
                                    )
                                )
                        for key, group in gpt_low_trade.groupby(
                            [
                                "model_type",
                                "instrument_subset",
                                "dataset_kind",
                            ],
                            dropna=False,
                        ):
                            if len(group) >= 6:
                                gpt_low_trade_model_subset_keys.add(
                                    tuple(
                                        str(part or "").lower()
                                        for part in key
                                    )
                                )
                recent_validations = ledger.tail(350).copy()
                recent_validations = recent_validations[
                    recent_validations["evaluation_stage"].astype(str).str.lower()
                    == "validation"
                ]
                recent_validations = recent_validations[
                    recent_validations["status"].astype(str).str.lower()
                    == "unsuccessful"
                ]
                if not recent_validations.empty:
                    recent_validations["mean_net_pips_num"] = pd.to_numeric(
                        recent_validations["mean_net_pips"],
                        errors="coerce",
                    )
                    recent_validations = recent_validations[
                        recent_validations["mean_net_pips_num"].fillna(-1.0)
                        <= 0.0
                    ]
                    for key, _group in recent_validations.groupby(
                        key_columns,
                        dropna=False,
                    ):
                        failed_validation_keys.add(
                            tuple(str(part or "").lower() for part in key)
                        )
                recent_live_validations = ledger.tail(220).copy()
                recent_live_validations = recent_live_validations[
                    recent_live_validations["evaluation_stage"]
                    .astype(str)
                    .str.lower()
                    == "validation"
                ]
                recent_live_validations = recent_live_validations[
                    recent_live_validations["source"].astype(str).str.lower().isin(
                        {
                            "operator_live_value_miss_followup_v1",
                            "operator_live_ev_near_miss_followup_v1",
                            "operator_live_unknown_profile_followup_v1",
                            "operator_live_missed_spike_subset_followup_v1",
                        }
                    )
                ]
                short_recent_live_validations = ledger.tail(60).copy()
                short_recent_live_validations = short_recent_live_validations[
                    short_recent_live_validations["evaluation_stage"]
                    .astype(str)
                    .str.lower()
                    == "validation"
                ]
                short_recent_live_validations = short_recent_live_validations[
                    short_recent_live_validations["source"]
                    .astype(str)
                    .str.lower()
                    .isin(
                        {
                            "operator_live_value_miss_followup_v1",
                            "operator_live_ev_near_miss_followup_v1",
                            "operator_live_unknown_profile_followup_v1",
                            "operator_live_missed_spike_subset_followup_v1",
                        }
                    )
                ]
                for _idx, row in short_recent_live_validations.iterrows():
                    row_source = str(row.get("source") or "").lower()
                    short_recent_live_validation_total += 1
                    short_recent_live_validation_source_counts[row_source] = (
                        short_recent_live_validation_source_counts.get(
                            row_source,
                            0,
                        )
                        + 1
                    )
                for _idx, row in recent_live_validations.iterrows():
                    row_source = str(row.get("source") or "").lower()
                    recent_live_validation_total += 1
                    recent_live_validation_source_counts[row_source] = (
                        recent_live_validation_source_counts.get(row_source, 0)
                        + 1
                    )
                    whitelist = tuple(
                        sorted(
                            normalized_instrument_whitelist(
                                row.get("instrument_whitelist"),
                            )
                        )
                    )
                    if not whitelist:
                        continue
                    segment_filters_key = json_dumps(
                        normalized_segment_filters(
                            row.get("segment_filters"),
                        )
                    ).lower()
                    whitelist_key = ",".join(whitelist).lower()
                    recent_live_validation_duplicate_keys.add(
                        (
                            row_source,
                            str(row.get("model_type") or "").lower(),
                            str(row.get("target") or "").lower(),
                            str(row.get("execution_policy") or "").lower(),
                            str(row.get("validation_profile") or "").lower(),
                            segment_filters_key,
                            whitelist_key,
                        )
                    )
                    relaxed_key = (
                        row_source,
                        str(row.get("model_type") or "").lower(),
                        str(row.get("target") or "").lower(),
                        segment_filters_key,
                        whitelist_key,
                    )
                    recent_live_validation_duplicate_relaxed_counts[
                        relaxed_key
                    ] = (
                        recent_live_validation_duplicate_relaxed_counts.get(
                            relaxed_key,
                            0,
                        )
                        + 1
                    )
        except Exception as exc:
            log_error("queued_spec_result_backoff", exc)
        unseen: List[tuple[int, int, int, int, int, int, int, int, Dict[str, Any]]] = []
        for index, spec in enumerate(queued_specs):
            if experiment_spec_hash(spec) in completed:
                continue
            stage = str(spec.get("evaluation_stage", "screen")).lower()
            source = str(spec.get("source", "")).lower()
            role = str(spec.get("research_role", "")).lower()
            policy = str(spec.get("execution_policy", "")).lower()
            target = str(spec.get("target", "")).lower()
            dataset_kind = str(spec.get("dataset_kind", "")).lower()
            deployment_lane = str(spec.get("deployment_lane", "")).lower()
            is_explicit_specialist = bool(
                normalized_instrument_whitelist(
                    spec.get("instrument_whitelist"),
                )
            )
            is_segment_filtered = bool(
                normalized_segment_filters(spec.get("segment_filters"))
            )
            is_live_split_candidate = deployment_lane in {
                "live_tech_champion_candidate",
                "live_primary_challenger_candidate",
            }
            is_weekend_account_improvement = source in {
                "operator_weekend_primary_improvement_v1",
                "operator_weekend_tech_improvement_v1",
                "operator_weekend_gpt_support_v1",
                "operator_scout_value_capture_v1",
                "operator_live_value_miss_followup_v1",
                "operator_live_ev_near_miss_followup_v1",
                "operator_live_unknown_profile_followup_v1",
                "operator_live_missed_spike_subset_followup_v1",
            }
            is_return_curve = (
                role == "return_curve_candidate"
                or policy == "curve"
                or "_curve_profit_" in target
            )
            is_lead_major_move = (
                role == "major_move_factor"
                and target.startswith("major_event_lead_")
            )
            is_scout_family = (
                role == "major_move_factor"
                or target.startswith("major_event_")
                or target.startswith("major_event_lead_")
                or source in {
                    "operator_reversal_curve_focus",
                    "operator_exhaustion_hybrid",
                    "seeded_stopcap_priority",
                    "operator_scout_value_capture_v1",
                    "operator_live_value_miss_followup_v1",
                    "operator_live_ev_near_miss_followup_v1",
                    "operator_live_unknown_profile_followup_v1",
                    "operator_live_missed_spike_subset_followup_v1",
                }
            )
            if stage == "validation":
                if (
                    is_weekend_account_improvement
                    or deployment_lane in {
                        "live_primary_challenger_candidate",
                        "live_tech_champion_candidate",
                        "gpt_live_model_support",
                    }
                ):
                    priority = -3
                    if (
                        primary_lane_needs_coverage
                        and deployment_lane
                        != "live_primary_challenger_candidate"
                    ):
                        priority = 1
                else:
                    priority = 3
            elif (
                is_explicit_specialist
                and deployment_lane == "live_primary_challenger_candidate"
            ):
                priority = -2 if primary_lane_needs_coverage else 0
            elif (
                is_segment_filtered
                and deployment_lane == "live_primary_challenger_candidate"
            ):
                priority = -2 if primary_lane_needs_coverage else 0
            elif (
                deployment_lane == "gpt_live_model_support"
                and gpt_support_needs_coverage
            ):
                priority = -2
            elif is_explicit_specialist and is_live_split_candidate:
                priority = -1
            elif is_segment_filtered and is_live_split_candidate:
                priority = -1
            elif is_live_split_candidate:
                priority = 1
            elif is_weekend_account_improvement:
                priority = 2
            elif source in {
                "operator_reversal_curve_focus",
                "operator_exhaustion_hybrid",
            }:
                priority = 2
            elif is_lead_major_move:
                priority = 3
            elif is_return_curve:
                priority = 4
            elif role == "major_move_factor":
                priority = 5
            elif dataset_kind == "profitable_move_precursor":
                priority = 6
            else:
                priority = 7
            if WEEKEND_NON_SCOUT_FOCUS:
                if is_scout_family:
                    priority += 20
                elif dataset_kind == "base_trade_quality":
                    priority = min(priority, 1)
                elif is_return_curve:
                    priority = min(priority, 2)
                elif role in {"standalone_candidate", "return_curve_candidate"}:
                    priority = min(priority, 2)
            if (
                deployment_lane == "live_primary_challenger_candidate"
                and (
                    primary_lane_needs_coverage
                    or not primary_lane_has_completed
                )
            ):
                priority = min(priority, 0)
            if (
                stage != "validation"
                and deployment_lane in lane_transfer_winners
                and dataset_kind != "profitable_move_precursor"
            ):
                priority = max(
                    priority
                    + int(os.getenv(
                        "OANDA_TRANSFER_WINNER_SPECIALIST_BACKOFF",
                        "6",
                    )),
                    int(os.getenv(
                        "OANDA_TRANSFER_WINNER_SPECIALIST_MIN_PRIORITY",
                        "20",
                    )),
                )
            if (
                stage != "validation"
                and deployment_lane in lane_transfer_winners
                and dataset_kind == "profitable_move_precursor"
                and "transfer" not in str(
                    spec.get("specialist_profile") or "",
                ).lower()
            ):
                priority = max(
                    priority
                    + int(os.getenv(
                        "OANDA_TRANSFER_WINNER_NONTRANSFER_BACKOFF",
                        "8",
                    )),
                    int(os.getenv(
                        "OANDA_TRANSFER_WINNER_NONTRANSFER_MIN_PRIORITY",
                        "12",
                    )),
                )
            if stage != "validation" and is_weekend_account_improvement:
                rotation_lane = deployment_lane
                if not rotation_lane:
                    if source == "operator_weekend_primary_improvement_v1":
                        rotation_lane = "live_primary_challenger_candidate"
                    elif source == "operator_weekend_tech_improvement_v1":
                        rotation_lane = "live_tech_champion_candidate"
                    elif source == "operator_weekend_gpt_support_v1":
                        rotation_lane = "gpt_live_model_support"
                recent_count = lane_recent_counts.get(rotation_lane, 0)
                undercovered_recent_count = max(3, lane_rotation_soft_cap // 4)
                if recent_count >= lane_rotation_soft_cap:
                    priority += 4
                elif (
                    rotation_lane
                    in {
                        "live_primary_challenger_candidate",
                        "gpt_live_model_support",
                    }
                    and recent_count <= undercovered_recent_count
                ):
                    priority = max(-2, priority - 2)
            sparse_key = (
                dataset_kind,
                str(spec.get("model_type") or "").lower(),
                str(spec.get("instrument_subset") or "all").lower(),
                target,
            )
            sparse_data_key = (
                dataset_kind,
                str(spec.get("instrument_subset") or "all").lower(),
                target,
            )
            sparse_segment_key = (
                dataset_kind,
                str(spec.get("model_type") or "").lower(),
                str(spec.get("instrument_subset") or "all").lower(),
                target,
                ",".join(
                    normalized_instrument_whitelist(
                        spec.get("instrument_whitelist"),
                    )
                ).lower(),
                json_dumps(
                    normalized_segment_filters(spec.get("segment_filters")),
                ).lower(),
            )
            if sparse_segment_key in sparse_segment_error_keys:
                priority += 12
            gpt_low_trade_key = (
                str(spec.get("model_type") or "").lower(),
                str(spec.get("instrument_subset") or "all").lower(),
                target,
                dataset_kind,
            )
            if (
                source == "operator_weekend_gpt_support_v1"
                and gpt_low_trade_key in gpt_low_trade_screen_keys
            ):
                priority += 8
            gpt_low_trade_model_subset_key = (
                str(spec.get("model_type") or "").lower(),
                str(spec.get("instrument_subset") or "all").lower(),
                dataset_kind,
            )
            if (
                source == "operator_weekend_gpt_support_v1"
                and gpt_low_trade_model_subset_key
                in gpt_low_trade_model_subset_keys
            ):
                priority += 12
            if not is_segment_filtered:
                if sparse_data_key in sparse_two_stage_data_keys:
                    priority += 6
                if sparse_data_key in repeated_weak_screen_data_keys:
                    priority += 5
                if sparse_key in sparse_two_stage_error_keys:
                    priority += 4
                if sparse_key in runtime_timeout_keys:
                    priority += 4
                if sparse_key in repeated_weak_screen_keys:
                    priority += 3
                if sparse_key in failed_validation_keys:
                    priority += 4
            if (
                source == "operator_live_ev_near_miss_followup_v1"
                and stage == "validation"
            ):
                # These are compact, high-signal scout follow-ups generated
                # from live candidates that were just below the value/EV gate.
                # Run a small sample promptly instead of letting the larger
                # value-miss validation batch hide them behind lane coverage.
                profile_lower = str(spec.get("specialist_profile") or "").lower()
                priority = min(priority, -4)
                if "current_ev_near_top" in profile_lower:
                    # The current EV-near top basket is economically
                    # interesting, but it has repeated 30/30 non-passing
                    # validation rows across base/calibration variants.  The
                    # base rows originally had usable raw AUC, but the newest
                    # live-window repeats degraded to ~0.43 AUC and still
                    # failed the stability gate. Keep the evidence available,
                    # but stop it from monopolizing first-24h research time.
                    priority = max(priority, 0)
                elif "calibration" in profile_lower:
                    # Current EV-near leaders are economically positive but
                    # fail the detection gate on Brier skill.  Run the compact
                    # boosted calibration variants before more forest repeats.
                    priority = min(priority, -5)
            if (
                source == "operator_live_value_miss_followup_v1"
                and stage == "validation"
            ):
                profile_lower = str(spec.get("specialist_profile") or "").lower()
                if "lead_only_robust" in profile_lower:
                    # A small number of value-miss rows can pass immediate
                    # gates with only two folds.  Their 12-week robust retests
                    # are the decisive promotion blocker/unblocker, so finish
                    # those before spending more first-watch cycles on ordinary
                    # current-value/current-count permutations.
                    priority = min(priority, -6)
                if "calibration_detection_segment_focus" in profile_lower:
                    # The first capped segment-focus batch answered the
                    # question: narrowing to the best scorecard segments kept
                    # economics positive, but did not fix detection quality
                    # (13/14 rows sampled, 0 gates, repeated min-week AUC and
                    # Brier failures, with one sparse/insufficient slice).
                    # Keep the remaining specs as evidence, but stop this
                    # family from outranking broader value/scout follow-ups.
                    priority = max(priority, 0)
                elif "calibration_detection" in profile_lower:
                    # The compact boosted calibration/detection pass has now
                    # sampled the current value-miss candidates enough to
                    # answer the immediate question: it preserved positive
                    # economics but did not repair detection quality (10/10
                    # broad rows, 0 gates, repeated Brier/min-week-AUC
                    # failures). Keep remaining specs as evidence, but rotate
                    # trainer time back toward broader value/scout follow-ups.
                    priority = max(priority, 0)
                if "current_near_value_gate" in profile_lower:
                    # Near-value-gate rejects were initially the most
                    # actionable threshold question, but the first-24h sample
                    # is now large and consistently non-passing: positive net,
                    # weak stability, and frequent sparse folds. Keep the lane
                    # as evidence while rotating priority to broader value and
                    # scout candidates.
                    priority = max(priority, 0)
                if "current_currency" in profile_lower:
                    # Current-day currency-theme baskets answered the immediate
                    # question enough for first-pass monitoring: 80+ rows, no
                    # gate passes, and boosted calibration/detection follow-ups
                    # still failed Brier/min-week stability. They remain useful
                    # evidence, but should no longer preempt stronger count-top,
                    # unknown-profile, and missed-spike follow-ups.
                    priority = max(priority, 0)
                if "current_scan_cost" in profile_lower:
                    # Primary scan rows initially looked worth fast follow-up:
                    # they had positive cost-adjusted movement but failed
                    # before order attempts.  The broad/focused calibration
                    # sweeps have now repeatedly formed folds yet failed gates
                    # due to unstable AUC/min-AUC or too few reliable trades.
                    # Keep the lane available for evidence, but stop letting it
                    # monopolize first-24h research priority over broader
                    # account/model improvement lanes.
                    priority = max(priority, 1)
                if (
                    "current_regime_lock" in profile_lower
                    and policy == "trailing_stop015"
                ):
                    # Regime-lock value-miss baskets are economically
                    # positive, but the first 9 trailing_stop015 rows all
                    # failed stability/concentration gates.  Keep this evidence
                    # queued while rotating first-watch trainer time toward the
                    # tighter trailing_stop012 variants that are still pending
                    # and have better stop-cap leadership in reports.
                    priority = max(priority, 0)
                if "current_value_" in profile_lower and "_pair" in profile_lower:
                    # Single-pair current value-miss validations have become
                    # a sparse-data sink in the first-24h loop: recent AUD_JPY,
                    # EUR_AUD, and USD_NOK pair4w/pair8w specs all failed
                    # rolling-fold construction, and 100+ similar pair specs
                    # remain queued.  Keep the evidence queued, but rotate
                    # research time to broader value baskets and robust live
                    # miss checks that can actually form validation folds.
                    priority = max(priority, 2)
            if (
                source == "operator_live_unknown_profile_followup_v1"
                and stage == "validation"
            ):
                # Unknown-profile misses are the largest live skipped bucket
                # during the first-week watch.  Run the compact whitelisted
                # profile-discovery tests promptly, without loosening live
                # move/spread gates.
                profile_lower = str(spec.get("specialist_profile") or "").lower()
                if (
                    "lead_only_robust" in profile_lower
                    and "nzd_jpy_aud_usd_eur_usd_gbp_jpy" in profile_lower
                ):
                    # The 12-week robust retest for the first unknown-profile
                    # winner has answered the immediate coverage question
                    # negatively: 9/9 rows were positive-net but non-passing
                    # and still only formed three folds. Keep the remaining
                    # specs as evidence, but rotate first-24h research time to
                    # the pending value-miss and missed-spike robust lanes.
                    priority = max(priority, 0)
                elif "current_unknown_profile_near_threshold" in profile_lower:
                    # The near-threshold basket has now repeated without a pass
                    # and with sub-random/unstable AUC in first-24h validation.
                    # Keep it in the ledger queue for evidence, but stop it
                    # from outranking stronger live-miss/scout follow-ups.
                    priority = max(priority, 1)
                elif (
                    "current_unknown_profile_near_" in profile_lower
                    and "_pair4w" in profile_lower
                ):
                    # Single-pair near-profile follow-ups for the current miss
                    # set are repeatedly failing rolling-fold construction
                    # (for example EUR_USD and USD_CAD). Keep them queued as
                    # sparse-data evidence but rotate trainer time to broader
                    # baskets and value/scout follow-ups.
                    priority = max(priority, 2)
                elif (
                    "current_unknown_profile_" in profile_lower
                    and "_pair4w" in profile_lower
                ):
                    # Current single-pair unknown-profile follow-ups have now
                    # produced no passes across 20+ first-24h rows, with
                    # repeated fold-construction failures on fresh pairs.  Keep
                    # broader unknown-profile baskets active because that is
                    # where prior lead-only gates came from, but stop spending
                    # top priority on isolated pair4w slices.
                    priority = max(priority, 1)
                elif "current_unknown_profile_count_top" in profile_lower:
                    # The count-top basket was worth a compact follow-up, but
                    # a full first-24h sample is now consistently non-passing:
                    # positive net, but weak AP/Brier/trade-count stability.
                    # Keep it as evidence while rotating priority to other
                    # value/scout and unknown-profile baskets.
                    priority = max(priority, 0)
                else:
                    priority = min(priority, -5)
                if (
                    "near" in profile_lower
                    and "current_unknown_profile_near_threshold" not in profile_lower
                    and "_pair4w" not in profile_lower
                ):
                    priority = min(priority, -6)
            if (
                source == "operator_live_missed_spike_subset_followup_v1"
                and stage == "validation"
            ):
                # Positive replay subsets were the best initial scout-specific
                # leads, but the generic replay backlog has now produced mostly
                # sparse or non-deployable follow-ups. Keep it available while
                # reserving high priority for proven successful-segment or
                # otherwise specific higher-value missed-spike paths below.
                priority = min(priority, -2)
                profile_lower = str(
                    spec.get("specialist_profile") or "",
                ).lower()
                if "successful_segment" in profile_lower:
                    # Existing segment leaders passed validation while stricter
                    # robust USD_ZAR slices are proving too sparse.  Validate
                    # compact liquidity/unfiltered retests promptly.
                    priority = min(priority, -9)
                elif "strict_pressure_pass" in profile_lower:
                    # Current primary strict-pressure passes were worth a
                    # fast validation pass because they reached the strict
                    # cluster/value gate but did not become order attempts.
                    # The first sample is now negative: 7 rows, 0 gates;
                    # any-move tests are positive-net but weak, single-pair
                    # tests are sparse, and the first directional precursor
                    # had negative net despite high mean AUC. Keep the specs as
                    # evidence, but stop them from outranking broader
                    # scout/value candidates during first-24h monitoring.
                    priority = max(priority, 0)
                elif (
                    "capture_gap" in profile_lower
                    and "directional_precursor" in profile_lower
                ):
                    # This branch answered the first-pass question: the
                    # LONG-biased capture-gap state is detectable, but both
                    # fixed-hold and curve outcomes stayed negative after
                    # costs across RF and the first ET sample. Keep the
                    # remaining specs queued as evidence, but rotate trainer
                    # time back toward broader scout/value candidates.
                    priority = max(priority, 1)
                elif "capture_gap" in profile_lower:
                    # Generic capture-gap major-event screens have now formed
                    # folds repeatedly but still failed deployability gates
                    # because of min-week/Brier/direction instability. Keep
                    # the remaining broad rows as evidence, but rotate trainer
                    # time toward the direction-specific precursor queue.
                    priority = max(priority, -1)
                elif (
                    "usd_zar" in profile_lower
                    and policy == "fixed"
                ):
                    # The fresh USD_ZAR replay lane remains one of the better
                    # scout leads, but fixed-exit rows are now negative across
                    # both sampled lead times (15m and 30m) and produced zero
                    # positive weeks. Keep the evidence queued, but rotate
                    # priority back to the trailing-stop variants that are
                    # positive-net and need stability checks instead of
                    # spending first-watch time on fixed-hold repeats.
                    priority = max(priority, 0)
                elif (
                    "usd_zar" in profile_lower
                    and "fresh_trail_s2_60m" in profile_lower
                    and str(spec.get("model_type") or "").lower() == "extra_trees"
                ):
                    # The USD_ZAR replay-derived ExtraTrees fresh/trail specs
                    # are repeatedly positive-net but fail promotion gates
                    # narrowly on min-AUC/AP/Brier/direction.  Keep them queued
                    # as evidence, but let RandomForest and broader follow-ups
                    # run first after the RF successful-segment retest passed.
                    priority = max(priority, -1)
                elif "top_subset_combo" in profile_lower:
                    # The broad missed-spike combo family is economically
                    # positive but not deployable in current validation: 45
                    # rows, 0 gates, avg AUC/min-AUC around 0.58/0.46. Keep it
                    # in the evidence set, but stop both RF and ET combo
                    # repeats from outranking pair-level USD_ZAR and broader
                    # live scout/value follow-ups.
                    priority = max(priority, -1)
                elif "localized_cluster" in profile_lower and "directional_precursor" in profile_lower:
                    # These use existing long/short profitable-move precursor
                    # labels to test the observed localized basket direction,
                    # avoiding duplicate generic any-move LONG/SHORT profiles.
                    # The broad four-pair ZAR cluster has now produced repeated
                    # validation failures with negative net while USD_ZAR
                    # follow-ups already have successful segment evidence. Keep
                    # this cluster available, but stop it from outranking the
                    # stronger USD_ZAR robust/relaxed follow-up lane.
                    if all(
                        token in profile_lower
                        for token in ("chf_zar", "eur_zar", "usd_zar", "gbp_zar")
                    ):
                        priority = max(priority, -1)
                    else:
                        priority = min(priority, -8)
                elif "localized_cluster" in profile_lower:
                    # Localized same-currency clusters are the newest live
                    # miss hypothesis from primary scout telemetry.  Validate
                    # a compact sample promptly without loosening live gates.
                    if all(
                        token in profile_lower
                        for token in ("chf_zar", "eur_zar", "usd_zar", "gbp_zar")
                    ):
                        priority = max(priority, -1)
                    else:
                        priority = min(priority, -7)
                elif "usdzar_relaxed" in profile_lower:
                    # The strict USD_ZAR follow-up slices were too sparse for
                    # 10-week rolling validation.  Run the relaxed/unfiltered
                    # checks promptly to distinguish signal failure from
                    # over-filtered validation slices.
                    priority = min(priority, -8)
                elif "usdzar_robust" in profile_lower:
                    # Strict robust USD_ZAR slices have repeatedly failed fold
                    # construction, while the successful-segment unfiltered
                    # retest passed. Keep robust specs in the queue, but stop
                    # spending first-24h research priority on sparse filters.
                    priority = max(priority, 0)
            source_rank = source_priority.get(source, 50)
            deployment_lane_rank = deployment_lane_priority.get(deployment_lane, 20)
            subset_rank = subset_priority.get(
                str(spec.get("instrument_subset") or "all").lower(),
                50,
            )
            if (
                source in {
                    "operator_live_value_miss_followup_v1",
                    "operator_live_ev_near_miss_followup_v1",
                    "operator_live_unknown_profile_followup_v1",
                    "operator_live_missed_spike_subset_followup_v1",
                }
                and normalized_instrument_whitelist(
                    spec.get("instrument_whitelist"),
                )
            ):
                if str(spec.get("instrument_subset") or "all").lower() == "all":
                    subset_rank = -1
                else:
                    priority += 6
                canonical_live_validation_key = (
                    source,
                    str(spec.get("model_type") or "").lower(),
                    target,
                    str(spec.get("execution_policy") or "").lower(),
                    str(spec.get("validation_profile") or "").lower(),
                    json_dumps(
                        normalized_segment_filters(
                            spec.get("segment_filters"),
                        )
                    ).lower(),
                    ",".join(
                        sorted(
                            normalized_instrument_whitelist(
                                spec.get("instrument_whitelist"),
                            )
                        )
                    ).lower(),
                )
                relaxed_live_validation_key = (
                    source,
                    str(spec.get("model_type") or "").lower(),
                    target,
                    json_dumps(
                        normalized_segment_filters(
                            spec.get("segment_filters"),
                        )
                    ).lower(),
                    ",".join(
                        sorted(
                            normalized_instrument_whitelist(
                                spec.get("instrument_whitelist"),
                            )
                        )
                    ).lower(),
                )
                relaxed_duplicate_count = (
                    recent_live_validation_duplicate_relaxed_counts.get(
                        relaxed_live_validation_key,
                        0,
                    )
                )
                exact_duplicate = (
                    canonical_live_validation_key
                    in recent_live_validation_duplicate_keys
                )
                if (
                    stage == "validation"
                    and (
                        exact_duplicate
                        or relaxed_duplicate_count >= 2
                    )
                ):
                    duplicate_profile_lower = str(
                        spec.get("specialist_profile") or "",
                    ).lower()
                    # Current live-followup seeders can enqueue the same
                    # basket in different instrument orders as the missed-move
                    # ranking changes intraday.  Those specs have different
                    # raw hashes and ledger rows may omit policy/profile
                    # metadata after restart, but they still produce identical
                    # validation metrics.  Keep them queued as evidence, but
                    # rotate trainer time to genuinely new baskets/models
                    # before rerunning permutations of a recent live
                    # validation.  The penalty scales because a fixed small
                    # penalty was not enough to move 3-5x repeated baskets
                    # behind fresh first-24h validation work.
                    if "lead_only_robust" in duplicate_profile_lower and not exact_duplicate:
                        # Lead-only robust specs intentionally reuse the same
                        # basket/model/target while changing the 12-week
                        # validation context (original/liquidity/unfiltered).
                        # Exact duplicates are still protected above, but do
                        # not let the relaxed-key penalty hide the remaining
                        # robust contexts behind ordinary current-value repeats.
                        priority += 2
                    else:
                        duplicate_penalty_min = safe_int(
                            os.environ.get(
                                "OANDA_LIVE_VALIDATION_RELAXED_DUPLICATE_MIN_PENALTY",
                            ),
                            40,
                        )
                        duplicate_penalty_step = safe_int(
                            os.environ.get(
                                "OANDA_LIVE_VALIDATION_RELAXED_DUPLICATE_STEP_PENALTY",
                            ),
                            18,
                        )
                        duplicate_penalty_cap = safe_int(
                            os.environ.get(
                                "OANDA_LIVE_VALIDATION_RELAXED_DUPLICATE_CAP_PENALTY",
                            ),
                            128,
                        )
                        priority += max(
                            duplicate_penalty_min,
                            min(
                                duplicate_penalty_cap,
                                duplicate_penalty_step
                                * max(relaxed_duplicate_count, 1),
                            ),
                        )
                value_miss_recent_count = (
                    recent_live_validation_source_counts.get(
                        "operator_live_value_miss_followup_v1",
                        0,
                    )
                )
                value_miss_recent_ratio = (
                    value_miss_recent_count
                    / max(recent_live_validation_total, 1)
                )
                value_miss_short_count = (
                    short_recent_live_validation_source_counts.get(
                        "operator_live_value_miss_followup_v1",
                        0,
                    )
                )
                value_miss_short_ratio = (
                    value_miss_short_count
                    / max(short_recent_live_validation_total, 1)
                )
                if (
                    stage == "validation"
                    and source == "operator_live_value_miss_followup_v1"
                    and (
                        (
                            value_miss_recent_count >= 45
                            and value_miss_recent_ratio >= 0.55
                        )
                        or (
                            value_miss_short_count >= 30
                            and value_miss_short_ratio >= 0.70
                        )
                    )
                ):
                    # Value-miss followups are important, but during the first
                    # 24h live-watch pass they can monopolize validation cycles
                    # and starve unknown-profile, EV-near, and missed-spike
                    # subset queues. Keep robust/segment/calibration retests
                    # moving, but rotate ordinary current-value/current-count
                    # baskets behind other live-followup families when the
                    # recent ledger is already value-miss dominated.
                    profile_lower = str(
                        spec.get("specialist_profile") or "",
                    ).lower()
                    if any(
                        token in profile_lower
                        for token in (
                            "lead_only_robust",
                            "segment_focus",
                            "calibration_detection",
                            "scan_cost",
                        )
                    ):
                        priority += 6
                    else:
                        priority += 24
            target_rank = target_priority.get(target, 50)
            model_rank = model_priority.get(
                str(spec.get("model_type") or "").lower(),
                50,
            )
            if source in {
                "operator_weekend_primary_improvement_v1",
                "operator_weekend_tech_improvement_v1",
            }:
                account_hgb_completed = account_model_completed_counts.get(
                    (source, "hist_gradient_boosting"),
                    0,
                )
                cross_family_after = int(os.getenv(
                    "OANDA_ACCOUNT_CROSS_FAMILY_AFTER_HGB",
                    "16",
                ))
                if account_hgb_completed >= cross_family_after:
                    model_rank = {
                        "extra_trees": 0,
                        "random_forest": 1,
                        "hist_gradient_boosting": 2,
                        "gradient_boosting": 3,
                        "lightgbm": 4,
                        "xgboost": 5,
                        "catboost": 6,
                        "ngboost": 7,
                    }.get(str(spec.get("model_type") or "").lower(), model_rank)
                else:
                    model_rank = {
                        "hist_gradient_boosting": 0,
                        "extra_trees": 1,
                        "random_forest": 2,
                        "gradient_boosting": 3,
                        "lightgbm": 4,
                        "xgboost": 5,
                        "catboost": 6,
                        "ngboost": 7,
                    }.get(str(spec.get("model_type") or "").lower(), model_rank)
            elif source == "operator_weekend_gpt_support_v1":
                model_rank = {
                    "hist_gradient_boosting": 0,
                    "extra_trees": 1,
                    "random_forest": 2,
                    "gradient_boosting": 3,
                    "lightgbm": 4,
                    "xgboost": 5,
                    "catboost": 6,
                    "ngboost": 7,
                }.get(str(spec.get("model_type") or "").lower(), model_rank)
                if (
                    str(spec.get("model_type") or "").lower()
                    in {"extra_trees", "random_forest"}
                    and safe_int(
                        (spec.get("parameters") or {}).get("n_estimators"),
                        0,
                    )
                    > 240
                ):
                    priority += 6
            elif source == "operator_live_missed_spike_subset_followup_v1":
                profile_lower_for_rank = str(
                    spec.get("specialist_profile") or "",
                ).lower()
                is_capture_gap_directional_curve = (
                    "capture_gap" in profile_lower_for_rank
                    and "directional_precursor" in profile_lower_for_rank
                    and (
                        "curve" in profile_lower_for_rank
                        or "curve" in str(spec.get("outcome") or "").lower()
                        or str(spec.get("execution_policy") or "").lower()
                        == "curve"
                    )
                )
                if is_capture_gap_directional_curve:
                    # The first RF directional capture-gap curve rows improved
                    # sharply versus fixed exits but remained negative after
                    # costs.  Sample ExtraTrees curve variants before more RF
                    # repeats so the research loop can distinguish a model
                    # family issue from a bad trade objective.
                    model_rank = {
                        "extra_trees": 0,
                        "random_forest": 1,
                        "hist_gradient_boosting": 2,
                        "gradient_boosting": 3,
                        "lightgbm": 4,
                        "xgboost": 5,
                        "catboost": 6,
                        "ngboost": 7,
                    }.get(
                        str(spec.get("model_type") or "").lower(),
                        model_rank,
                    )
            policy_rank = policy_priority.get(policy, 50)
            if source == "operator_scout_value_capture_v1":
                # Fresh first-24h scout-value evidence shows the tighter stop
                # caps are the only economically interesting branch so far:
                # stop010/012/015 have higher mean net than stop025+ while all
                # rows still fail promotion gates.  Keep the wider stop caps
                # queued as evidence, but test the tighter exit curve first.
                policy_rank = {
                    "trailing_stop010": 0,
                    "trailing_stop012": 1,
                    "trailing_stop015": 2,
                    "trailing_stop020": 3,
                    "trailing_stop025": 4,
                    "trailing_stop035": 5,
                    "trailing_stop050": 6,
                    "trailing_stop075": 7,
                    "trailing_stop100": 8,
                    "trailing": 9,
                    "fixed": 10,
                    "curve": 11,
                    "": 12,
                }.get(policy, policy_rank)
            unseen.append((
                priority,
                deployment_lane_rank,
                source_rank,
                policy_rank,
                target_rank,
                subset_rank,
                model_rank,
                index,
                spec,
            ))
        if not unseen:
            return None
        unseen.sort(key=lambda row: row[:8])
        return unseen[0][8]

    def next_spec(self) -> Dict[str, Any]:
        completed = self.completed_hashes()
        queued = self.prioritized_queued_spec(completed)
        if queued is not None:
            return queued
        try:
            from oanda_model_space_plan_runner import run_plan as run_model_space_plan
            plan = run_model_space_plan(
                apply=True,
                max_seed=80,
                include_new_estimators=True,
                min_total_pending=40,
            )
            log_audit({
                "event_type": "model_space_plan",
                "status": "applied",
                "reason": str(plan.get("active_phase") or ""),
                "result": json_dumps({
                    "seeded_this_run": plan.get("seeded_this_run"),
                    "seeded_by_phase": plan.get("seeded_by_phase"),
                    "total_pending_unique": plan.get("total_pending_unique"),
                })[:1000],
            })
            completed = self.completed_hashes()
            queued = self.prioritized_queued_spec(completed)
            if queued is not None:
                return queued
        except Exception as exc:
            log_error("model_space_plan_autoseed", exc)
        for spec in self.gpt_specs():
            self.registry.register_proposal(spec, source="gpt")
            if research_spec_hash(spec) not in completed:
                return spec
        while True:
            index = safe_int(self.state.get("generated_index"), 0)
            self.state["generated_index"] = index + 1
            spec = generated_experiment_spec(index)
            self.registry.register_proposal(spec, source="generated")
            if research_spec_hash(spec) not in completed:
                save_json(self.state_path, self.state)
                return spec

    def load_dataset(
        self,
        spec: Dict[str, Any],
    ) -> tuple[pd.DataFrame, str, Path, List[str]]:
        if spec["dataset_kind"] in {
            "technical_spike",
            "profitable_move_precursor",
        }:
            path = build_technical_spike_research_dataset()
            features = (
                TECHNICAL_CORE_FEATURES
                if spec["feature_set"] == "technical_core"
                else TECHNICAL_MODEL_FEATURES
            )
            extra_columns: List[str] = []
            synthetic_trailing_stop_policy = ""
            synthetic_trailing_stop_horizon = 0
            if spec.get("research_role") == "major_move_factor":
                horizon = safe_int(str(spec["target"]).rsplit("_", 1)[-1])
                policy = str(spec.get("execution_policy") or "trailing")
                stop_cap_name = (
                    policy.removeprefix("trailing_")
                    if policy.startswith("trailing_stop")
                    else ""
                )
                if stop_cap_name in MAJOR_MOVE_STOP_CAP_ATR_UNITS:
                    synthetic_trailing_stop_policy = policy
                    synthetic_trailing_stop_horizon = horizon
                    extra_columns = [
                        spec["direction_target"],
                        f"major_move_peak_pips_{horizon}",
                        f"long_trailing_net_pips_{horizon}",
                        f"short_trailing_net_pips_{horizon}",
                        "atr240_pips",
                    ]
                else:
                    extra_columns = [
                        spec["direction_target"],
                        f"major_move_peak_pips_{horizon}",
                        f"long_{policy}_net_atr_{horizon}",
                        f"short_{policy}_net_atr_{horizon}",
                    ]
            needed = [
                "time_utc",
                "instrument",
                "pair_taxonomy_primary",
                "regime_primary",
                *INSTRUMENT_METADATA_FEATURES,
                *segment_filter_columns(spec.get("segment_filters")),
                *features,
                spec["target"],
                *extra_columns,
            ]
            if not extra_columns:
                needed.append(spec["outcome"])
            needed = list(dict.fromkeys(needed))
            key = f"{path}:{path.stat().st_mtime_ns}:{','.join(needed)}"
            if key != self._cache_key:
                self._cache_frame = pd.read_parquet(path, columns=needed)
                self._cache_key = key
            frame = self._cache_frame.copy()
            if synthetic_trailing_stop_policy:
                cap_name = synthetic_trailing_stop_policy.removeprefix(
                    "trailing_"
                )
                cap_atr = float(MAJOR_MOVE_STOP_CAP_ATR_UNITS[cap_name])
                horizon = synthetic_trailing_stop_horizon
                atr = pd.to_numeric(
                    frame["atr240_pips"],
                    errors="coerce",
                ).clip(lower=0.1)
                expected = (atr * math.sqrt(horizon / 5.0)).clip(lower=0.1)
                for side in ["long", "short"]:
                    pips = pd.to_numeric(
                        frame[f"{side}_trailing_net_pips_{horizon}"],
                        errors="coerce",
                    )
                    capped_pips = np.maximum(pips, -atr * cap_atr)
                    frame[
                        f"{side}_{synthetic_trailing_stop_policy}_net_atr_{horizon}"
                    ] = capped_pips / expected
            frame = apply_instrument_subset(
                frame,
                str(spec.get("instrument_subset") or "all"),
            )
            frame = apply_instrument_whitelist(
                frame,
                spec.get("instrument_whitelist"),
            )
            frame = apply_segment_filters(
                frame,
                spec.get("segment_filters"),
            )
        else:
            path = latest_historical_training_path()
            if path is None:
                raise RuntimeError("No historical training dataset exists")
            features = MODEL_FEATURE_COLUMNS
            needed = [
                "time_utc",
                "instrument",
                *features,
                "would_profit_30m",
                "future_30m_pips",
                "spread_pips",
            ]
            key = f"{path}:{path.stat().st_mtime_ns}:{','.join(needed)}"
            if key != self._cache_key:
                self._cache_frame = pd.read_csv(
                    path,
                    usecols=lambda column: column in set(needed),
                )
                self._cache_frame["net_after_spread_30"] = (
                    pd.to_numeric(
                        self._cache_frame["future_30m_pips"],
                        errors="coerce",
                    )
                    - pd.to_numeric(
                        self._cache_frame["spread_pips"],
                        errors="coerce",
                    )
                )
                self._cache_frame["net_vol_units_30"] = (
                    self._cache_frame["net_after_spread_30"]
                    / pd.to_numeric(
                        self._cache_frame["volatility_30_pips"],
                        errors="coerce",
                    ).clip(lower=0.1)
                )
                self._cache_key = key
            frame = self._cache_frame.copy()
            frame = apply_instrument_whitelist(
                frame,
                spec.get("instrument_whitelist"),
            )
            frame = apply_segment_filters(
                frame,
                spec.get("segment_filters"),
            )
        dataset_hash = sha256_file(path)
        return frame, dataset_hash, path, list(features)

    def estimator(self, spec: Dict[str, Any]) -> Any:
        params = dict(spec.get("parameters") or {})
        research_n_jobs = max(
            1,
            safe_int(os.environ.get("OANDA_CONTINUOUS_RESEARCH_N_JOBS"), 1),
        )
        if spec["model_type"] == "lightgbm":
            if not LIGHTGBM_AVAILABLE or LGBMClassifier is None:
                raise RuntimeError("LightGBM is not installed in this environment")
            return LGBMClassifier(
                random_state=42,
                n_jobs=research_n_jobs,
                objective="binary",
                verbosity=-1,
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "n_estimators",
                        "learning_rate",
                        "num_leaves",
                        "min_child_samples",
                        "subsample",
                        "colsample_bytree",
                        "reg_alpha",
                        "reg_lambda",
                    }
                },
            )
        if spec["model_type"] == "xgboost":
            if not XGBOOST_AVAILABLE or XGBClassifier is None:
                raise RuntimeError("XGBoost is not installed in this environment")
            return XGBClassifier(
                random_state=42,
                n_jobs=research_n_jobs,
                objective="binary:logistic",
                eval_metric="logloss",
                tree_method="hist",
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "n_estimators",
                        "learning_rate",
                        "max_depth",
                        "subsample",
                        "colsample_bytree",
                        "min_child_weight",
                        "reg_alpha",
                        "reg_lambda",
                    }
                },
            )
        if spec["model_type"] == "catboost":
            if not CATBOOST_AVAILABLE or CatBoostClassifier is None:
                raise RuntimeError("CatBoost is not installed in this environment")
            return CatBoostClassifier(
                random_seed=42,
                loss_function="Logloss",
                eval_metric="Logloss",
                verbose=False,
                allow_writing_files=False,
                thread_count=research_n_jobs,
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "iterations",
                        "learning_rate",
                        "depth",
                        "l2_leaf_reg",
                        "random_strength",
                        "subsample",
                        "auto_class_weights",
                    }
                },
            )
        if spec["model_type"] == "ngboost":
            if not NGBOOST_AVAILABLE or NGBClassifier is None:
                raise RuntimeError("NGBoost is not installed in this environment")
            return NGBClassifier(
                random_state=42,
                verbose=False,
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "n_estimators",
                        "learning_rate",
                        "minibatch_frac",
                        "col_sample",
                        "natural_gradient",
                        "tol",
                    }
                },
            )
        if spec["model_type"] == "gradient_boosting":
            return GradientBoostingClassifier(
                random_state=42,
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "n_estimators",
                        "learning_rate",
                        "max_depth",
                        "min_samples_leaf",
                        "subsample",
                    }
                },
            )
        if spec["model_type"] == "extra_trees":
            return ExtraTreesClassifier(
                random_state=42,
                n_jobs=research_n_jobs,
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "n_estimators",
                        "max_depth",
                        "min_samples_leaf",
                        "max_features",
                        "class_weight",
                    }
                },
            )
        if spec["model_type"] == "hist_gradient_boosting":
            return HistGradientBoostingClassifier(
                random_state=42,
                **{
                    key: value
                    for key, value in params.items()
                    if key in {
                        "max_iter",
                        "learning_rate",
                        "max_leaf_nodes",
                        "min_samples_leaf",
                        "l2_regularization",
                    }
                },
            )
        return RandomForestClassifier(
            random_state=42,
            n_jobs=research_n_jobs,
            **{
                key: value
                for key, value in params.items()
                if key in {
                    "n_estimators",
                    "max_depth",
                    "min_samples_leaf",
                    "max_features",
                    "class_weight",
                }
            },
        )

    def rolling_windows(
        self,
        frame: pd.DataFrame,
        holdout_weeks: Optional[int] = None,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
        weeks = [pd.Timestamp(value) for value in sorted(
            frame["week_start"].unique()
        )]
        minimum_index = 4 + CONTINUOUS_RESEARCH_CALIBRATION_WEEKS
        test_weeks = weeks[minimum_index:][
            -safe_int(holdout_weeks, CONTINUOUS_RESEARCH_HOLDOUT_WEEKS):
        ]
        windows = []
        for test_start in test_weeks:
            test_index = weeks.index(test_start)
            calibration_start = weeks[
                test_index - CONTINUOUS_RESEARCH_CALIBRATION_WEEKS
            ]
            purge = pd.Timedelta(minutes=CONTINUOUS_RESEARCH_PURGE_MINUTES)
            windows.append((
                calibration_start - purge,
                calibration_start,
                test_start,
            ))
        return windows

    @staticmethod
    def validation_settings(spec: Dict[str, Any]) -> Tuple[str, int, int]:
        stage = str(spec.get("evaluation_stage") or "screen").lower()
        if stage not in {"screen", "validation"}:
            stage = "screen"
        default_weeks = (
            CONTINUOUS_RESEARCH_HOLDOUT_WEEKS
            if stage == "validation"
            else CONTINUOUS_RESEARCH_SCREEN_HOLDOUT_WEEKS
        )
        default_rows = (
            CONTINUOUS_RESEARCH_MAX_TRAIN_ROWS
            if stage == "validation"
            else CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS
        )
        weeks = max(1, safe_int(spec.get("validation_weeks"), default_weeks))
        rows = max(10_000, safe_int(spec.get("max_train_rows"), default_rows))
        cap_env = (
            "OANDA_CONTINUOUS_RESEARCH_VALIDATION_MAX_TRAIN_ROWS_CAP"
            if stage == "validation"
            else "OANDA_CONTINUOUS_RESEARCH_SCREEN_MAX_TRAIN_ROWS_CAP"
        )
        default_cap = 30_000 if stage == "validation" else 15_000
        row_cap = safe_int(os.environ.get(cap_env), default_cap)
        if row_cap > 0:
            rows = min(rows, max(10_000, row_cap))
        return stage, weeks, rows

    @staticmethod
    def probability_metrics(
        target: np.ndarray,
        probability: np.ndarray,
    ) -> Dict[str, float]:
        target = np.asarray(target, dtype=int)
        probability = np.asarray(probability, dtype=float)
        base_rate = float(target.mean()) if len(target) else 0.0
        auc = (
            float(roc_auc_score(target, probability))
            if len(np.unique(target)) > 1
            else 0.5
        )
        average_precision = (
            float(average_precision_score(target, probability))
            if target.sum() > 0
            else 0.0
        )
        top_count = max(1, int(math.ceil(len(target) * 0.01)))
        top_indices = np.argsort(probability)[-top_count:]
        top_precision = float(target[top_indices].mean())
        top_recall = float(target[top_indices].sum() / max(target.sum(), 1))
        baseline_brier = base_rate * (1.0 - base_rate)
        brier = float(brier_score_loss(target, probability))
        return {
            "auc": auc,
            "average_precision": average_precision,
            "base_rate": base_rate,
            "average_precision_lift": (
                average_precision / max(base_rate, 1e-9)
            ),
            "brier": brier,
            "brier_skill": (
                1.0 - brier / max(baseline_brier, 1e-9)
            ),
            "top_1pct_precision": top_precision,
            "top_1pct_recall": top_recall,
        }

    @staticmethod
    def fit_probability_calibrator(
        probability: np.ndarray,
        target: np.ndarray,
    ) -> Optional[Any]:
        target = np.asarray(target, dtype=int)
        if len(target) < 100 or len(np.unique(target)) < 2:
            return None
        calibrator = LogisticRegression(
            random_state=42,
            max_iter=500,
        )
        calibrator.fit(
            np.asarray(probability, dtype=float).reshape(-1, 1),
            target,
        )
        return calibrator

    @staticmethod
    def apply_probability_calibrator(
        calibrator: Optional[Any],
        probability: np.ndarray,
    ) -> np.ndarray:
        raw = np.asarray(probability, dtype=float)
        if calibrator is None:
            return raw
        return calibrator.predict_proba(raw.reshape(-1, 1))[:, 1]

    @staticmethod
    def outcome_summary(outcome: np.ndarray) -> Dict[str, float]:
        values = np.asarray(outcome, dtype=float)
        values = values[np.isfinite(values)]
        trades = len(values)
        gross_win = float(values[values > 0].sum()) if trades else 0.0
        gross_loss = (
            float(abs(values[values < 0].sum())) if trades else 0.0
        )
        return {
            "trades": float(trades),
            "mean_net_pips": float(values.mean()) if trades else 0.0,
            "win_rate": float((values > 0).mean()) if trades else 0.0,
            "profit_factor": (
                gross_win / gross_loss
                if gross_loss > 0
                else (gross_win if gross_win > 0 else 0.0)
            ),
            "total_net_pips": float(values.sum()) if trades else 0.0,
        }

    @staticmethod
    def bootstrap_lower_mean(
        outcome: Sequence[float],
        *,
        seed: int = 42,
        samples: int = 300,
        quantile: float = 0.05,
    ) -> float:
        values = np.asarray(outcome, dtype=float)
        values = values[np.isfinite(values)]
        if len(values) < 10:
            return -1e9
        rng = np.random.default_rng(seed)
        means = []
        for _ in range(max(50, samples)):
            sample = rng.choice(values, size=len(values), replace=True)
            means.append(float(sample.mean()))
        return float(np.quantile(means, quantile))

    @staticmethod
    def concentration_summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, float]:
        frame = pd.DataFrame(list(rows))
        if frame.empty or "instrument" not in frame:
            return {
                "top_pair_trade_share": 0.0,
                "unique_pairs": 0.0,
            }
        counts = frame["instrument"].astype(str).value_counts()
        total = max(float(counts.sum()), 1.0)
        return {
            "top_pair_trade_share": float(counts.iloc[0] / total),
            "unique_pairs": float(len(counts)),
        }

    @staticmethod
    def selected_row_records(
        frame: pd.DataFrame,
        positions: Sequence[int],
        outcomes: Sequence[float],
    ) -> List[Dict[str, Any]]:
        if not len(positions):
            return []
        rows = frame.iloc[list(positions)].copy()
        out_values = list(outcomes)
        records: List[Dict[str, Any]] = []
        for idx, (_, row) in enumerate(rows.iterrows()):
            instrument = str(row.get("instrument", ""))
            record = {
                "time_utc": row.get("time_utc"),
                "instrument": instrument,
                "outcome": safe_float(out_values[idx] if idx < len(out_values) else 0.0),
                "pair_taxonomy_primary": str(
                    row.get(
                        "pair_taxonomy_primary",
                        pair_taxonomy_primary(instrument),
                    )
                ),
                "regime_primary": str(row.get("regime_primary", "unknown")),
                "session": (
                    "london_ny_overlap"
                    if safe_float(row.get("is_london_ny_overlap")) >= 0.5
                    else (
                        "new_york"
                        if safe_float(row.get("is_new_york_session")) >= 0.5
                        else (
                            "london"
                            if safe_float(row.get("is_london_session")) >= 0.5
                            else (
                                "asia"
                                if safe_float(row.get("is_asia_session")) >= 0.5
                                else "off_session"
                            )
                        )
                    )
                ),
            }
            records.append(record)
        return records

    @staticmethod
    def segment_scorecards(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        frame = pd.DataFrame(list(rows))
        if frame.empty or "outcome" not in frame:
            return {}
        frame["outcome"] = pd.to_numeric(frame["outcome"], errors="coerce")
        frame = frame.dropna(subset=["outcome"])
        if frame.empty:
            return {}

        def summarize(group: pd.DataFrame) -> Dict[str, float]:
            values = group["outcome"].to_numpy(dtype=float)
            gross_win = float(values[values > 0].sum())
            gross_loss = float(abs(values[values < 0].sum()))
            return {
                "trades": float(len(values)),
                "mean_net": float(values.mean()) if len(values) else 0.0,
                "win_rate": float((values > 0).mean()) if len(values) else 0.0,
                "profit_factor": (
                    gross_win / gross_loss
                    if gross_loss > 0
                    else (gross_win if gross_win > 0 else 0.0)
                ),
                "total_net": float(values.sum()) if len(values) else 0.0,
            }

        scorecards: Dict[str, List[Dict[str, Any]]] = {}
        for column in [
            "instrument",
            "pair_taxonomy_primary",
            "regime_primary",
            "session",
        ]:
            if column not in frame:
                continue
            rows_out = []
            for key, group in frame.groupby(column, dropna=False):
                if len(group) < 3:
                    continue
                rows_out.append({
                    "segment": str(key),
                    **summarize(group),
                })
            rows_out.sort(
                key=lambda item: (
                    safe_float(item.get("profit_factor")),
                    safe_float(item.get("mean_net")),
                    safe_float(item.get("trades")),
                ),
                reverse=True,
            )
            scorecards[column] = rows_out[:40]
        return scorecards

    @staticmethod
    def allowed_segments_from_scorecards(
        scorecards: Dict[str, List[Dict[str, Any]]],
        *,
        min_trades: int = 20,
        min_profit_factor: float = 1.20,
        min_mean_net: float = 0.0,
    ) -> Dict[str, List[str]]:
        allowed: Dict[str, List[str]] = {}
        mapping = {
            "allowed_instruments": "instrument",
            "allowed_pair_families": "pair_taxonomy_primary",
            "allowed_regimes": "regime_primary",
            "allowed_sessions": "session",
        }
        for target_key, scorecard_key in mapping.items():
            values = []
            for row in scorecards.get(scorecard_key, []):
                if (
                    safe_float(row.get("trades")) >= min_trades
                    and safe_float(row.get("profit_factor")) >= min_profit_factor
                    and safe_float(row.get("mean_net")) > min_mean_net
                ):
                    values.append(str(row.get("segment", "")))
            allowed[target_key] = sorted({value for value in values if value})
        return allowed

    @staticmethod
    def economic_gate(
        selected: Dict[str, float],
        *,
        stage: str,
        fold_count: int,
        bootstrap_lower_mean: float,
        top_pair_trade_share: float,
        top_pair_trade_share_limit: float = MAX_VALIDATION_TOP_PAIR_TRADE_SHARE,
    ) -> Dict[str, bool]:
        if stage == "screen":
            min_trades = 25
            min_pf = 1.05
            min_positive_weeks = max(1, math.ceil(fold_count * 0.50))
            return {
                "selected_trades_screen_min": bool(
                    selected["trades"] >= min_trades
                ),
                "selected_mean_net_positive": bool(
                    selected["mean_net_pips"] > 0
                ),
                "selected_positive_in_50pct_weeks": bool(
                    selected["positive_weeks"] >= min_positive_weeks
                ),
                "selected_median_profit_factor_screen_min": bool(
                    selected["median_profit_factor"] >= min_pf
                ),
            }
        min_positive_weeks = max(
            1,
            math.ceil(fold_count * MIN_VALIDATION_POSITIVE_WEEK_SHARE),
        )
        return {
            "selected_trades_at_least_75": bool(selected["trades"] >= 75),
            "selected_mean_net_pips_positive": bool(
                selected["mean_net_pips"] > 0
            ),
            "selected_bootstrap_lower_mean_positive": bool(
                bootstrap_lower_mean > 0
            ),
            "selected_positive_in_70pct_weeks": bool(
                selected["positive_weeks"] >= min_positive_weeks
            ),
            "selected_median_profit_factor_at_least_1_20": bool(
                selected["median_profit_factor"] >= MIN_VALIDATION_PROFIT_FACTOR
            ),
            "top_pair_trade_share_at_configured_limit": bool(
                top_pair_trade_share <= top_pair_trade_share_limit
            ),
        }

    def episode_positions(
        self,
        frame: pd.DataFrame,
        probability: np.ndarray,
        selected: np.ndarray,
    ) -> np.ndarray:
        positions = np.flatnonzero(selected)
        if not len(positions):
            return positions
        working = frame.iloc[positions][
            ["time_utc", "instrument"]
        ].copy()
        working["_position"] = positions
        working["_probability"] = probability[positions]
        working["_episode"] = working["time_utc"].dt.floor(
            f"{MAJOR_MOVE_EPISODE_MINUTES}min"
        )
        kept: List[int] = []
        for _, group in working.groupby("_episode"):
            used_currencies: set[str] = set()
            for _, row in group.sort_values(
                "_probability",
                ascending=False,
            ).iterrows():
                instrument = str(row["instrument"])
                currencies = set(instrument.split("_", 1))
                if used_currencies & currencies:
                    continue
                kept.append(int(row["_position"]))
                used_currencies.update(currencies)
                if len(used_currencies) >= (
                    MAJOR_MOVE_MAX_SIGNALS_PER_EPISODE * 2
                ):
                    break
        return np.asarray(sorted(kept), dtype=int)

    def sample_factor_training(
        self,
        train: pd.DataFrame,
        target: str,
        peak_column: str,
        seed: int,
    ) -> pd.DataFrame:
        positive = train[train[target].astype(int).eq(1)]
        negative = train[train[target].astype(int).eq(0)]
        if positive.empty or negative.empty:
            return train
        peak = pd.to_numeric(negative[peak_column], errors="coerce")
        hard_cutoff = peak.quantile(0.90)
        hard_pool = negative[peak >= hard_cutoff].sort_values(
            peak_column,
            ascending=False,
        )
        desired = len(positive) * MAJOR_MOVE_NEGATIVE_SAMPLE_RATIO
        hard = hard_pool.head(min(len(hard_pool), max(len(positive) * 5, 1)))
        remaining = negative.drop(index=hard.index)
        random_count = max(0, min(len(remaining), desired - len(hard)))
        sampled = (
            remaining.sample(n=random_count, random_state=seed)
            if random_count
            else remaining.iloc[0:0]
        )
        return pd.concat([positive, hard, sampled]).sort_values("time_utc")

    def choose_threshold(
        self,
        frame: pd.DataFrame,
        probability: np.ndarray,
        outcome: np.ndarray,
        thresholds: Sequence[float],
        *,
        episode_aware: bool,
        min_trades: int = 5,
    ) -> Tuple[float, Dict[str, float]]:
        rows = []
        for threshold in thresholds:
            selected = probability >= threshold
            positions = (
                self.episode_positions(frame, probability, selected)
                if episode_aware
                else np.flatnonzero(selected)
            )
            summary = self.outcome_summary(outcome[positions])
            rows.append({"threshold": float(threshold), **summary})
        ranked = pd.DataFrame(rows)
        eligible = ranked[ranked["trades"] >= max(1, min_trades)]
        if eligible.empty:
            eligible = ranked
        best = eligible.sort_values(
            ["mean_net_pips", "profit_factor", "trades"],
            ascending=[False, False, False],
        ).iloc[0]
        return float(best["threshold"]), best.to_dict()

    def choose_factor_thresholds(
        self,
        frame: pd.DataFrame,
        event_probability: np.ndarray,
        direction_probability: np.ndarray,
        outcome: np.ndarray,
        thresholds: Sequence[float],
        direction_confidence_thresholds: Sequence[float],
        *,
        episode_aware: bool,
        min_trades: int = 5,
    ) -> Tuple[float, float, Dict[str, float]]:
        """Choose event and direction-confidence thresholds by calibration P/L."""
        event_probability = np.asarray(event_probability, dtype=float)
        direction_probability = np.asarray(direction_probability, dtype=float)
        direction_confidence = np.maximum(
            direction_probability,
            1.0 - direction_probability,
        )
        rows = []
        for threshold in thresholds:
            for direction_threshold in direction_confidence_thresholds:
                selected = (
                    (event_probability >= threshold)
                    & (direction_confidence >= direction_threshold)
                )
                positions = (
                    self.episode_positions(frame, event_probability, selected)
                    if episode_aware
                    else np.flatnonzero(selected)
                )
                summary = self.outcome_summary(outcome[positions])
                rows.append({
                    "threshold": float(threshold),
                    "direction_confidence_threshold": float(direction_threshold),
                    **summary,
                })
        ranked = pd.DataFrame(rows)
        eligible = ranked[ranked["trades"] >= max(1, min_trades)]
        if eligible.empty:
            eligible = ranked
        best = eligible.sort_values(
            ["mean_net_pips", "profit_factor", "trades"],
            ascending=[False, False, False],
        ).iloc[0]
        return (
            float(best["threshold"]),
            float(best["direction_confidence_threshold"]),
            best.to_dict(),
        )

    def evaluate_major_move_factor(
        self,
        spec: Dict[str, Any],
        frame: pd.DataFrame,
        dataset_hash: str,
        dataset_path: Path,
        features: List[str],
    ) -> Dict[str, Any]:
        horizon = safe_int(str(spec["target"]).rsplit("_", 1)[-1])
        policy = str(spec.get("execution_policy") or "trailing")
        direction_target = str(spec["direction_target"])
        evaluation_stage, holdout_weeks, max_train_rows = (
            self.validation_settings(spec)
        )
        peak_column = f"major_move_peak_pips_{horizon}"
        long_outcome_column = f"long_{policy}_net_atr_{horizon}"
        short_outcome_column = f"short_{policy}_net_atr_{horizon}"
        numeric_columns = [
            *features,
            spec["target"],
            direction_target,
            peak_column,
            long_outcome_column,
            short_outcome_column,
        ]
        for column in numeric_columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(
            subset=[
                "time_utc",
                *features,
                spec["target"],
                peak_column,
                long_outcome_column,
                short_outcome_column,
            ]
        ).sort_values("time_utc").reset_index(drop=True)
        frame["week_start"] = (
            frame["time_utc"].dt.normalize()
            - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
        )
        folds: List[Dict[str, Any]] = []
        threshold_rows: List[Dict[str, Any]] = []
        selected_values_all: List[float] = []
        selected_rows_all: List[Dict[str, Any]] = []
        for number, (
            train_end,
            calibration_start,
            test_start,
        ) in enumerate(self.rolling_windows(frame, holdout_weeks), 1):
            purge = pd.Timedelta(minutes=CONTINUOUS_RESEARCH_PURGE_MINUTES)
            train = frame[frame["time_utc"] < train_end]
            calibration = frame[
                (frame["time_utc"] >= calibration_start)
                & (frame["time_utc"] < test_start - purge)
            ]
            test = frame[frame["week_start"] == test_start].reset_index(
                drop=True
            )
            if len(train) > max_train_rows:
                train = train.tail(max_train_rows)
            sampled_train = self.sample_factor_training(
                train,
                spec["target"],
                peak_column,
                seed=number * 1009,
            )
            if (
                len(sampled_train) < 500
                or len(calibration) < 250
                or len(test) < 250
            ):
                continue
            y_train = sampled_train[spec["target"]].astype(int)
            y_calibration = calibration[spec["target"]].astype(int)
            y_test = test[spec["target"]].astype(int)
            if any(
                target.nunique() < 2
                for target in [y_train, y_calibration, y_test]
            ):
                continue
            event_model = self.estimator(spec)
            event_model.fit(sampled_train[features], y_train)
            raw_calibration = event_model.predict_proba(
                calibration[features]
            )[:, 1]
            calibrator = self.fit_probability_calibrator(
                raw_calibration,
                y_calibration.to_numpy(),
            )
            calibration_probability = self.apply_probability_calibrator(
                calibrator,
                raw_calibration,
            )
            test_probability = self.apply_probability_calibrator(
                calibrator,
                event_model.predict_proba(test[features])[:, 1],
            )

            direction_train = train[
                train[spec["target"]].astype(int).eq(1)
                & train[direction_target].notna()
            ]
            if (
                len(direction_train) < 30
                or direction_train[direction_target].nunique() < 2
            ):
                continue
            direction_model = self.estimator(spec)
            direction_model.fit(
                direction_train[features],
                direction_train[direction_target].astype(int),
            )
            calibration_direction = direction_model.predict_proba(
                calibration[features]
            )[:, 1]
            test_direction = direction_model.predict_proba(
                test[features]
            )[:, 1]
            calibration_outcome = np.where(
                calibration_direction >= 0.5,
                calibration[long_outcome_column].to_numpy(dtype=float),
                calibration[short_outcome_column].to_numpy(dtype=float),
            )
            test_outcome = np.where(
                test_direction >= 0.5,
                test[long_outcome_column].to_numpy(dtype=float),
                test[short_outcome_column].to_numpy(dtype=float),
            )
            factor_thresholds = sorted({
                float(np.quantile(calibration_probability, quantile))
                for quantile in [0.90, 0.95, 0.975, 0.99, 0.995]
            })
            (
                threshold,
                direction_confidence_threshold,
                calibration_selection,
            ) = self.choose_factor_thresholds(
                calibration.reset_index(drop=True),
                calibration_probability,
                calibration_direction,
                calibration_outcome,
                factor_thresholds,
                [0.50, 0.55, 0.60, 0.65, 0.70],
                episode_aware=True,
                min_trades=(
                    20 if evaluation_stage == "validation" else 10
                ),
            )
            test_direction_confidence = np.maximum(
                test_direction,
                1.0 - test_direction,
            )
            selected = (
                (test_probability >= threshold)
                & (test_direction_confidence >= direction_confidence_threshold)
            )
            positions = self.episode_positions(
                test,
                test_probability,
                selected,
            )
            selected_summary = self.outcome_summary(test_outcome[positions])
            selected_values = test_outcome[positions]
            selected_values_all.extend(
                [float(value) for value in selected_values if math.isfinite(float(value))]
            )
            if len(positions):
                selected_rows_all.extend(
                    self.selected_row_records(
                        test,
                        positions,
                        selected_values,
                    )
                )
            selected_targets = y_test.to_numpy()[positions]
            probability_result = self.probability_metrics(
                y_test.to_numpy(),
                test_probability,
            )
            actual_event = y_test.to_numpy().astype(bool)
            direction_accuracy = (
                float(
                    (
                        (test_direction[actual_event] >= 0.5).astype(int)
                        == test.loc[
                            actual_event,
                            direction_target,
                        ].astype(int).to_numpy()
                    ).mean()
                )
                if actual_event.sum()
                else 0.0
            )
            folds.append({
                "fold": number,
                "evaluation_stage": evaluation_stage,
                "week_start": test_start.isoformat(),
                "calibration_week": calibration_start.isoformat(),
                "purge_minutes": CONTINUOUS_RESEARCH_PURGE_MINUTES,
                "train_rows": int(len(train)),
                "sampled_train_rows": int(len(sampled_train)),
                "calibration_rows": int(len(calibration)),
                "test_rows": int(len(test)),
                **probability_result,
                "direction_accuracy": direction_accuracy,
                "selected_threshold": threshold,
                "selected_direction_confidence_threshold": (
                    direction_confidence_threshold
                ),
                "selected_event_precision": (
                    float(selected_targets.mean())
                    if len(selected_targets)
                    else 0.0
                ),
                "selected_event_recall": (
                    float(selected_targets.sum() / max(y_test.sum(), 1))
                ),
                "calibration_selection": calibration_selection,
            })
            threshold_rows.append({
                "week_start": test_start.isoformat(),
                "threshold": threshold,
                "direction_confidence_threshold": (
                    direction_confidence_threshold
                ),
                **selected_summary,
            })
        if not folds:
            raise InsufficientRollingFolds(
                "No valid purged two-stage rolling folds; "
                f"rows={len(frame)} holdout_weeks={holdout_weeks} "
                f"target={spec.get('target')} subset={spec.get('instrument_subset', 'all')} "
                "after filters"
            )
        fold_frame = pd.DataFrame(folds)
        threshold_frame = pd.DataFrame(threshold_rows)
        trades = int(threshold_frame["trades"].sum())
        total_net = float(threshold_frame["total_net_pips"].sum())
        selected = {
            "threshold": float(threshold_frame["threshold"].median()),
            "direction_confidence_threshold": float(
                threshold_frame["direction_confidence_threshold"].median()
            ),
            "weeks": float(len(threshold_frame)),
            "positive_weeks": float(
                (threshold_frame["mean_net_pips"] > 0).sum()
            ),
            "trades": float(trades),
            "mean_net_pips": total_net / max(trades, 1),
            "median_week_mean_net_pips": float(
                threshold_frame["mean_net_pips"].median()
            ),
            "median_profit_factor": float(
                threshold_frame["profit_factor"].median()
            ),
            "total_net_pips": total_net,
            "bootstrap_lower_mean_net_pips": self.bootstrap_lower_mean(
                selected_values_all,
                seed=7919,
            ),
        }
        concentration = self.concentration_summary(selected_rows_all)
        selected.update(concentration)
        selected["top_pair_trade_share_limit"] = top_pair_trade_share_limit_for_spec(
            spec,
        )
        scorecards = self.segment_scorecards(selected_rows_all)
        allowed_segments = self.allowed_segments_from_scorecards(scorecards)
        detection_gate = {
            "mean_auc_at_least_0_58": bool(
                fold_frame["auc"].mean() >= 0.58
            ),
            "minimum_week_auc_at_least_0_52": bool(
                fold_frame["auc"].min() >= 0.52
            ),
            "mean_average_precision_lift_at_least_3": bool(
                fold_frame["average_precision_lift"].mean() >= 3.0
            ),
            "mean_brier_skill_positive": bool(
                fold_frame["brier_skill"].mean() > 0
            ),
            "mean_direction_accuracy_at_least_0_52": bool(
                fold_frame["direction_accuracy"].mean() >= 0.52
            ),
        }
        detection_passed = all(detection_gate.values())
        economic_gate = self.economic_gate(
            selected,
            stage=evaluation_stage,
            fold_count=len(fold_frame),
            bootstrap_lower_mean=selected["bootstrap_lower_mean_net_pips"],
            top_pair_trade_share=selected["top_pair_trade_share"],
            top_pair_trade_share_limit=selected["top_pair_trade_share_limit"],
        )
        economic_passed = all(economic_gate.values())
        gate = {
            **detection_gate,
            **economic_gate,
            "detection_passed": detection_passed,
            "economic_passed": economic_passed,
            "passed": detection_passed and economic_passed,
        }
        score = (
            float(fold_frame["auc"].mean()) * 50.0
            + min(
                10.0,
                float(fold_frame["average_precision_lift"].mean()),
            ) * 5.0
            + float(fold_frame["direction_accuracy"].mean()) * 20.0
            + float(np.clip(selected["mean_net_pips"], -5, 5)) * 3.0
            + float(selected["median_profit_factor"]) * 4.0
        )
        return {
            "dataset_hash": dataset_hash,
            "dataset_path": str(dataset_path),
            "dataset_rows": len(frame),
            "evaluation_stage": evaluation_stage,
            "instrument_subset": spec.get("instrument_subset", "all"),
            "instrument_whitelist": normalized_instrument_whitelist(
                spec.get("instrument_whitelist"),
            ),
            "segment_filters": normalized_segment_filters(
                spec.get("segment_filters"),
            ),
            "features": features,
            "folds": folds,
            "fold_count": len(folds),
            "mean_auc": float(fold_frame["auc"].mean()),
            "minimum_week_auc": float(fold_frame["auc"].min()),
            "mean_average_precision": float(
                fold_frame["average_precision"].mean()
            ),
            "mean_average_precision_lift": float(
                fold_frame["average_precision_lift"].mean()
            ),
            "mean_brier_skill": float(fold_frame["brier_skill"].mean()),
            "mean_direction_accuracy": float(
                fold_frame["direction_accuracy"].mean()
            ),
            "selected_threshold": selected,
            "threshold_summary": threshold_rows,
            "scorecards": scorecards,
            "allowed_segments": allowed_segments,
            "gate": gate,
            "score": score,
            "frame": frame,
        }

    def evaluate(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        frame, dataset_hash, dataset_path, features = self.load_dataset(spec)
        evaluation_stage, holdout_weeks, max_train_rows = (
            self.validation_settings(spec)
        )
        frame["time_utc"] = pd.to_datetime(
            frame["time_utc"],
            errors="coerce",
            utc=True,
        )
        if spec.get("research_role") == "major_move_factor":
            return self.evaluate_major_move_factor(
                spec,
                frame,
                dataset_hash,
                dataset_path,
                features,
            )
        for column in [*features, spec["target"], spec["outcome"]]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(
            subset=["time_utc", *features, spec["target"], spec["outcome"]]
        ).sort_values("time_utc").reset_index(drop=True)
        frame["week_start"] = (
            frame["time_utc"].dt.normalize()
            - pd.to_timedelta(frame["time_utc"].dt.weekday, unit="D")
        )
        folds: List[Dict[str, Any]] = []
        threshold_rows: List[Dict[str, Any]] = []
        selected_values_all: List[float] = []
        selected_rows_all: List[Dict[str, Any]] = []
        thresholds_to_test = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
        for number, (
            train_end,
            calibration_start,
            test_start,
        ) in enumerate(self.rolling_windows(frame, holdout_weeks), 1):
            purge = pd.Timedelta(minutes=CONTINUOUS_RESEARCH_PURGE_MINUTES)
            train = frame[frame["time_utc"] < train_end]
            calibration = frame[
                (frame["time_utc"] >= calibration_start)
                & (frame["time_utc"] < test_start - purge)
            ]
            test = frame[frame["week_start"] == test_start].reset_index(
                drop=True
            )
            if len(train) > max_train_rows:
                train = train.tail(max_train_rows)
            if len(train) < 5_000 or len(calibration) < 250 or len(test) < 250:
                continue
            y_train = train[spec["target"]].astype(int)
            y_test = test[spec["target"]].astype(int)
            if y_train.nunique() < 2 or y_test.nunique() < 2:
                continue
            model = self.estimator(spec)
            model.fit(train[features], y_train)
            calibration_probability = model.predict_proba(
                calibration[features]
            )[:, 1]
            episode_aware_selection = spec["dataset_kind"] in {
                "technical_spike",
                "profitable_move_precursor",
            }
            threshold, calibration_selection = self.choose_threshold(
                calibration.reset_index(drop=True),
                calibration_probability,
                calibration[spec["outcome"]].to_numpy(dtype=float),
                thresholds_to_test,
                episode_aware=episode_aware_selection,
                min_trades=(
                    20 if evaluation_stage == "validation" else 10
                ),
            )
            test_probability = model.predict_proba(test[features])[:, 1]
            probability_result = self.probability_metrics(
                y_test.to_numpy(),
                test_probability,
            )
            selected = test_probability >= threshold
            positions = (
                self.episode_positions(
                    test,
                    test_probability,
                    selected,
                )
                if episode_aware_selection
                else np.flatnonzero(selected)
            )
            selected_values = test.iloc[positions][spec["outcome"]].to_numpy(
                dtype=float
            )
            selected_values_all.extend(
                [float(value) for value in selected_values if math.isfinite(float(value))]
            )
            if len(positions):
                selected_rows_all.extend(
                    self.selected_row_records(
                        test,
                        positions,
                        selected_values,
                    )
                )
            selected_summary = self.outcome_summary(
                selected_values
            )
            folds.append({
                "fold": number,
                "evaluation_stage": evaluation_stage,
                "week_start": test_start.isoformat(),
                "calibration_week": calibration_start.isoformat(),
                "purge_minutes": CONTINUOUS_RESEARCH_PURGE_MINUTES,
                "train_rows": int(len(train)),
                "calibration_rows": int(len(calibration)),
                "test_rows": int(len(test)),
                **probability_result,
                "selected_threshold": threshold,
                "episode_aware_selection": bool(episode_aware_selection),
                "calibration_selection": calibration_selection,
            })
            threshold_rows.append({
                "week_start": test_start.isoformat(),
                "threshold": threshold,
                **selected_summary,
            })
        if not folds:
            raise InsufficientRollingFolds(
                "No valid purged rolling weekly folds; "
                f"rows={len(frame)} holdout_weeks={holdout_weeks} "
                f"target={spec.get('target')} subset={spec.get('instrument_subset', 'all')} "
                "after filters"
            )
        fold_frame = pd.DataFrame(folds)
        threshold_frame = pd.DataFrame(threshold_rows)
        trades = int(threshold_frame["trades"].sum())
        total_net = float(threshold_frame["total_net_pips"].sum())
        selected = {
            "threshold": float(threshold_frame["threshold"].median()),
            "weeks": float(len(threshold_frame)),
            "positive_weeks": float(
                (threshold_frame["mean_net_pips"] > 0).sum()
            ),
            "trades": float(trades),
            "mean_net_pips": total_net / max(trades, 1),
            "median_week_mean_net_pips": float(
                threshold_frame["mean_net_pips"].median()
            ),
            "median_profit_factor": float(
                threshold_frame["profit_factor"].median()
            ),
            "total_net_pips": total_net,
            "bootstrap_lower_mean_net_pips": self.bootstrap_lower_mean(
                selected_values_all,
                seed=7927,
            ),
        }
        concentration = self.concentration_summary(selected_rows_all)
        selected.update(concentration)
        selected["top_pair_trade_share_limit"] = top_pair_trade_share_limit_for_spec(
            spec,
        )
        scorecards = self.segment_scorecards(selected_rows_all)
        allowed_segments = self.allowed_segments_from_scorecards(scorecards)
        classifier_gate = {
            "mean_auc_at_least_0_55": bool(
                fold_frame["auc"].mean() >= 0.55
            ),
            "minimum_week_auc_at_least_0_50": bool(
                fold_frame["auc"].min() >= 0.50
            ),
        }
        economic_gate = self.economic_gate(
            selected,
            stage=evaluation_stage,
            fold_count=len(fold_frame),
            bootstrap_lower_mean=selected["bootstrap_lower_mean_net_pips"],
            top_pair_trade_share=selected["top_pair_trade_share"],
            top_pair_trade_share_limit=selected["top_pair_trade_share_limit"],
        )
        gate = {**classifier_gate, **economic_gate}
        gate["passed"] = all(gate.values())
        score = (
            float(fold_frame["auc"].mean()) * 100.0
            + float(fold_frame["auc"].min()) * 10.0
            + float(np.clip(selected["mean_net_pips"], -5, 5)) * 2.0
            + float(selected["median_profit_factor"]) * 5.0
            + float(selected["positive_weeks"]) / len(fold_frame) * 10.0
        )
        return {
            "dataset_hash": dataset_hash,
            "dataset_path": str(dataset_path),
            "dataset_rows": len(frame),
            "evaluation_stage": evaluation_stage,
            "instrument_subset": spec.get("instrument_subset", "all"),
            "instrument_whitelist": normalized_instrument_whitelist(
                spec.get("instrument_whitelist"),
            ),
            "segment_filters": normalized_segment_filters(
                spec.get("segment_filters"),
            ),
            "features": features,
            "folds": folds,
            "fold_count": len(folds),
            "mean_auc": float(fold_frame["auc"].mean()),
            "minimum_week_auc": float(fold_frame["auc"].min()),
            "mean_average_precision": float(
                fold_frame["average_precision"].mean()
            ),
            "mean_brier_skill": float(fold_frame["brier_skill"].mean()),
            "selected_threshold": selected,
            "threshold_summary": threshold_rows,
            "scorecards": scorecards,
            "allowed_segments": allowed_segments,
            "gate": gate,
            "score": score,
            "frame": frame,
        }

    def maybe_auto_promote_technical_production(
        self,
        manifest: Dict[str, Any],
    ) -> bool:
        """Promote a fully validated research leader to technical paper production."""
        if not ALLOW_AUTO_PROMOTE_TECHNICAL_PRODUCTION:
            return False
        if str(manifest.get("stage", "")).lower() != "validated":
            return False
        validation = manifest.get("validation", {})
        if not isinstance(validation, dict):
            return False
        gate = validation.get("gate", {})
        if not isinstance(gate, dict) or not bool(gate.get("passed", False)):
            return False
        artifact = Path(str(manifest.get("model_artifact_path", "")))
        if not artifact.exists() or not artifact.is_file():
            return False
        selected = validation.get("selected_threshold", {})
        if not isinstance(selected, dict):
            selected = {}
        if safe_float(selected.get("trades"), 0.0) < 75:
            return False
        weeks = safe_float(selected.get("weeks"), 0.0)
        positive_weeks_required = max(
            1.0,
            math.ceil(weeks * MIN_VALIDATION_POSITIVE_WEEK_SHARE),
        )
        if safe_float(selected.get("positive_weeks"), 0.0) < positive_weeks_required:
            return False
        allowed_segments = manifest.get("allowed_segments", {})
        if not isinstance(allowed_segments, dict):
            allowed_segments = {}
        allowed_pair_families = (
            manifest.get("allowed_pair_families")
            or allowed_segments.get("allowed_pair_families")
            or []
        )
        allowed_sessions = (
            manifest.get("allowed_sessions")
            or allowed_segments.get("allowed_sessions")
            or []
        )
        if isinstance(allowed_pair_families, str):
            allowed_pair_families = [allowed_pair_families]
        if isinstance(allowed_sessions, str):
            allowed_sessions = [allowed_sessions]
        allowed_pair_families = [
            str(value) for value in allowed_pair_families if str(value)
        ]
        allowed_sessions = [
            str(value) for value in allowed_sessions if str(value)
        ]
        if not allowed_pair_families or not allowed_sessions:
            return False

        production_path = DIRS["promotions"] / "technical_production.json"
        current = load_json(production_path, {})
        current_score = (
            safe_float(current.get("research_score"), -1e18)
            if current.get("validation_protocol_version")
            == RESEARCH_VALIDATION_PROTOCOL_VERSION
            else -1e18
        )
        candidate_score = safe_float(manifest.get("research_score"), -1e18)
        if current_score >= candidate_score:
            return False
        try:
            from oanda_model_metrics import promotion_benchmark_allows_auto_promotion
            benchmark_allows, benchmark_comparison = (
                promotion_benchmark_allows_auto_promotion(manifest)
            )
            manifest["benchmark_comparison"] = benchmark_comparison
            if not benchmark_allows:
                self.state["last_auto_promotion_blocked_reason"] = (
                    benchmark_comparison.get("reason")
                    or "candidate failed benchmark comparison"
                )
                self.state["last_auto_promotion_blocked_candidate_id"] = (
                    manifest.get("candidate_id", "")
                )
                log_audit({
                    "event_type": "technical_auto_promotion",
                    "status": "blocked",
                    "reason": self.state["last_auto_promotion_blocked_reason"],
                    "candidate_id": manifest.get("candidate_id", ""),
                    "experiment_id": manifest.get("experiment_id", ""),
                    "result": json_dumps(benchmark_comparison)[:2000],
                })
                return False
        except Exception as exc:
            log_error("technical_auto_promotion_benchmark", exc)
            return False

        promoted_utc = iso_utc()
        production = {
            **manifest,
            "updated_utc": promoted_utc,
            "stage": "technical_production",
            "target_account_role": "TECHNICAL_PAPER_ACCOUNT",
            "technical_account_activation": True,
            "activation_effective": True,
            "auto_promotion_enabled": True,
            "auto_promoted_utc": promoted_utc,
            "auto_promotion_rule": (
                "validation gate passed, artifact exists, allowed segments "
                "present, research_score exceeds current technical production, "
                "matched ARIMA benchmark is beaten when required, and "
                "shadow/canary production-readiness evidence is present"
            ),
            "production_gate": {
                "source_research_gate_passed": True,
                "source_fold_count": validation.get("fold_count"),
                "mean_auc": validation.get("mean_auc"),
                "minimum_week_auc": validation.get("minimum_week_auc"),
                "selected_threshold": selected,
                "benchmark_comparison": manifest.get(
                    "benchmark_comparison",
                    {},
                ),
                "technical_production_readiness_gate": (
                    manifest.get("benchmark_comparison", {}).get(
                        "technical_production_readiness_gate",
                        {},
                    )
                ),
                "auto_promotion_enabled": True,
            },
            "promoted_from_manifest": str(
                DIRS["promotions"] / "research_leader.json"
            ),
            "reason": (
                "Auto-promoted technical paper production model after full "
                "validation gates passed. GPT advisor account untouched."
            ),
        }
        save_json(production_path, production)
        self.state["last_auto_promoted_technical_utc"] = promoted_utc
        self.state["last_auto_promoted_technical_candidate_id"] = production.get(
            "candidate_id",
            "",
        )
        self.state["last_auto_promoted_technical_experiment_id"] = production.get(
            "experiment_id",
            "",
        )
        return True

    def update_champion(
        self,
        spec: Dict[str, Any],
        result: Dict[str, Any],
        experiment_id: str,
    ) -> str:
        if str(spec.get("research_role", "")).startswith(
            "major_move_factor"
        ):
            return self.update_major_move_factor(
                spec,
                result,
                experiment_id,
            )
        if str(spec.get("evaluation_stage") or "screen") != "validation":
            return ""
        if not bool(result.get("gate", {}).get("passed", False)):
            return ""
        manifest_path = DIRS["promotions"] / "research_leader.json"
        current = load_json(manifest_path, {})
        current_score = (
            safe_float(current.get("research_score"), -1e18)
            if current.get("validation_protocol_version")
            == RESEARCH_VALIDATION_PROTOCOL_VERSION
            else -1e18
        )
        if current_score >= safe_float(
            result.get("score"),
            -1e18,
        ):
            return ""
        frame = result["frame"]
        features = result["features"]
        dataset_end_utc = ""
        try:
            dataset_end_utc = pd.to_datetime(
                frame["time_utc"],
                errors="coerce",
                utc=True,
            ).max().isoformat()
        except Exception:
            dataset_end_utc = ""
        model = self.estimator(spec)
        if len(frame) > CONTINUOUS_RESEARCH_MAX_TRAIN_ROWS:
            frame = frame.tail(CONTINUOUS_RESEARCH_MAX_TRAIN_ROWS)
        model.fit(frame[features], frame[spec["target"]].astype(int))
        artifact = (
            DIRS["models"]
            / f"research_{experiment_id}.joblib"
        )
        joblib.dump({
            "model": model,
            "features": features,
            "spec": spec,
            "validation": {
                key: value
                for key, value in result.items()
                if key not in {"frame"}
            },
            "trained_utc": iso_utc(),
        }, artifact)
        gate_passed = bool(result["gate"].get("passed"))
        cid = lifecycle_candidate_id(spec)
        manifest = {
            "updated_utc": iso_utc(),
            "candidate_id": cid,
            "experiment_id": experiment_id,
            "research_score": result["score"],
            "validation_protocol_version": (
                RESEARCH_VALIDATION_PROTOCOL_VERSION
            ),
            "dataset_hash": result.get("dataset_hash", ""),
            "dataset_end_utc": dataset_end_utc,
            "dataset_kind": spec["dataset_kind"],
            "target": spec["target"],
            "outcome": spec["outcome"],
            "model_type": spec["model_type"],
            "feature_set": spec["feature_set"],
            "instrument_subset": spec.get("instrument_subset", "all"),
            "features": features,
            "parameters": spec["parameters"],
            "model_artifact_path": str(artifact),
            "scorecards": result.get("scorecards", {}),
            "allowed_segments": result.get("allowed_segments", {}),
            "allowed_pair_families": result.get("allowed_segments", {}).get(
                "allowed_pair_families",
                [],
            ),
            "allowed_regimes": result.get("allowed_segments", {}).get(
                "allowed_regimes",
                [],
            ),
            "allowed_sessions": result.get("allowed_segments", {}).get(
                "allowed_sessions",
                [],
            ),
            "validation": {
                "fold_count": result["fold_count"],
                "mean_auc": result["mean_auc"],
                "minimum_week_auc": result["minimum_week_auc"],
                "selected_threshold": result["selected_threshold"],
                "gate": result["gate"],
            },
            "stage": "validated",
            "dum_account": DEFAULT_DUM1_ACCOUNT,
            "technical_account_activation": False,
            "technical_activation_requirements": {
                "research_gate_passed": gate_passed,
                "shadow_validation_passed": True,
                "explicit_canary_assignment_required": True,
                "minimum_dum_live_trades": MIN_LIVE_TRADES_FOR_SWITCH,
                "dum_live_profit_factor_at_least": MIN_VALIDATION_PROFIT_FACTOR,
                "dum_live_mean_net_positive": True,
            },
            "reason": (
                "Best validation-passed research candidate. It is eligible "
                "for shadow observation only; no account may execute it until "
                "an explicit canary assignment and production promotion pass."
            ),
        }
        try:
            from oanda_model_metrics import compare_manifest_to_arima
            manifest["benchmark_comparison"] = compare_manifest_to_arima(
                manifest
            )
            manifest["technical_activation_requirements"][
                "beats_matched_arima_when_available"
            ] = (
                not manifest["benchmark_comparison"].get("arima_available")
                or manifest["benchmark_comparison"].get("candidate_beats_arima")
                is True
            )
        except Exception as exc:
            log_error("research_leader_benchmark_comparison", exc)
        save_json(manifest_path, manifest)
        save_json(
            DIRS["promotions"] / "shadow_candidate.json",
            {
                **manifest,
                "stage": "shadow",
                "target_account_role": "SHADOW_OBSERVATION_ONLY",
                "canary_assignment_required": True,
            },
        )
        save_json(
            DIRS["promotions"] / "technical_model_manifest.json",
            {
                **manifest,
                "stage": "deprecated_research_leader_only",
                "technical_account_activation": False,
                "activation_effective": False,
                "reason": (
                    "Deprecated compatibility file. Technical execution reads "
                    "technical_production.json only."
                ),
            },
        )
        self.maybe_auto_promote_technical_production(manifest)
        return str(artifact)

    def update_major_move_factor(
        self,
        spec: Dict[str, Any],
        result: Dict[str, Any],
        experiment_id: str,
    ) -> str:
        """Store the best spike factor without promoting it as a strategy."""
        if spec.get("research_role") != "major_move_factor":
            return ""
        if str(spec.get("evaluation_stage") or "screen") != "validation":
            return ""
        if not bool(result.get("gate", {}).get("detection_passed", False)):
            return ""
        manifest_path = (
            DIRS["promotions"] / "major_move_factor_shadow.json"
        )
        current = load_json(manifest_path, {})
        current_score = (
            safe_float(current.get("research_score"), -1e18)
            if current.get("validation_protocol_version")
            == RESEARCH_VALIDATION_PROTOCOL_VERSION
            else -1e18
        )
        if current_score >= safe_float(
            result.get("score"),
            -1e18,
        ):
            return ""
        frame = result["frame"]
        features = result["features"]
        dataset_end_utc = ""
        try:
            dataset_end_utc = pd.to_datetime(
                frame["time_utc"],
                errors="coerce",
                utc=True,
            ).max().isoformat()
        except Exception:
            dataset_end_utc = ""
        horizon = safe_int(str(spec["target"]).rsplit("_", 1)[-1])
        peak_column = f"major_move_peak_pips_{horizon}"
        direction_target = str(spec["direction_target"])
        frame = frame.sort_values("time_utc")
        weeks = sorted(frame["week_start"].unique())
        calibrator = None
        if len(weeks) >= 2:
            calibration_start = pd.Timestamp(weeks[-1])
            base = frame[
                frame["time_utc"]
                < calibration_start
                - pd.Timedelta(minutes=CONTINUOUS_RESEARCH_PURGE_MINUTES)
            ]
            calibration = frame[frame["week_start"] == calibration_start]
            sampled_base = self.sample_factor_training(
                base,
                spec["target"],
                peak_column,
                seed=8719,
            )
            calibration_model = self.estimator(spec)
            calibration_model.fit(
                sampled_base[features],
                sampled_base[spec["target"]].astype(int),
            )
            calibrator = self.fit_probability_calibrator(
                calibration_model.predict_proba(
                    calibration[features]
                )[:, 1],
                calibration[spec["target"]].astype(int).to_numpy(),
            )
        if len(frame) > CONTINUOUS_RESEARCH_MAX_TRAIN_ROWS:
            frame = frame.tail(CONTINUOUS_RESEARCH_MAX_TRAIN_ROWS)
        sampled_frame = self.sample_factor_training(
            frame,
            spec["target"],
            peak_column,
            seed=8723,
        )
        event_model = self.estimator(spec)
        event_model.fit(
            sampled_frame[features],
            sampled_frame[spec["target"]].astype(int),
        )
        direction_frame = frame[
            frame[spec["target"]].astype(int).eq(1)
            & frame[direction_target].notna()
        ]
        direction_model = self.estimator(spec)
        direction_model.fit(
            direction_frame[features],
            direction_frame[direction_target].astype(int),
        )
        artifact = (
            DIRS["models"]
            / f"major_move_factor_{experiment_id}.joblib"
        )
        joblib.dump({
            "event_model": event_model,
            "direction_model": direction_model,
            "probability_calibrator": calibrator,
            "features": features,
            "spec": spec,
            "event_threshold": result["selected_threshold"].get(
                "threshold",
                0.10,
            ),
            "direction_threshold": 0.50,
            "direction_confidence_threshold": result["selected_threshold"].get(
                "direction_confidence_threshold",
                0.50,
            ),
            "validation": {
                key: value
                for key, value in result.items()
                if key != "frame"
            },
            "trained_utc": iso_utc(),
        }, artifact)
        manifest = {
            "updated_utc": iso_utc(),
            "candidate_id": lifecycle_candidate_id(spec),
            "experiment_id": experiment_id,
            "research_score": result["score"],
            "validation_protocol_version": (
                RESEARCH_VALIDATION_PROTOCOL_VERSION
            ),
            "dataset_hash": result.get("dataset_hash", ""),
            "dataset_end_utc": dataset_end_utc,
            "research_role": "major_move_factor",
            "dataset_kind": spec["dataset_kind"],
            "target": spec["target"],
            "outcome": spec["outcome"],
            "model_type": spec["model_type"],
            "feature_set": spec["feature_set"],
            "instrument_subset": spec.get("instrument_subset", "all"),
            "features": features,
            "parameters": spec["parameters"],
            "model_artifact_path": str(artifact),
            "scorecards": result.get("scorecards", {}),
            "allowed_segments": result.get("allowed_segments", {}),
            "allowed_pair_families": result.get("allowed_segments", {}).get(
                "allowed_pair_families",
                [],
            ),
            "allowed_regimes": result.get("allowed_segments", {}).get(
                "allowed_regimes",
                [],
            ),
            "allowed_sessions": result.get("allowed_segments", {}).get(
                "allowed_sessions",
                [],
            ),
            "validation": {
                "fold_count": result["fold_count"],
                "mean_auc": result["mean_auc"],
                "minimum_week_auc": result["minimum_week_auc"],
                "mean_average_precision": result.get(
                    "mean_average_precision"
                ),
                "mean_average_precision_lift": result.get(
                    "mean_average_precision_lift"
                ),
                "mean_brier_skill": result.get("mean_brier_skill"),
                "mean_direction_accuracy": result.get(
                    "mean_direction_accuracy"
                ),
                "selected_threshold": result["selected_threshold"],
                "gate": result["gate"],
            },
            "stage": "shadow",
            "technical_account_activation": False,
            "standalone_promotion_allowed": False,
            "score_modifier_enabled": bool(
                result.get("gate", {}).get("economic_passed", False)
            ),
            "production_manifest_required": True,
            "reason": (
                "Two-stage major-move probability and direction bundle. It is "
                "shadow-observed only. It cannot modify live technical risk "
                "unless major_move_factor_production.json is later written "
                "after canary/production promotion."
            ),
        }
        save_json(manifest_path, manifest)
        save_json(
            DIRS["promotions"] / "major_move_factor_manifest.json",
            {
                **manifest,
                "stage": "deprecated_shadow_only",
                "technical_account_activation": False,
                "score_modifier_enabled": False,
                "reason": (
                    "Deprecated compatibility file. Production modifiers read "
                    "major_move_factor_production.json only."
                ),
            },
        )
        return str(artifact)

    def record(
        self,
        spec: Dict[str, Any],
        status: str,
        *,
        result: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> None:
        result = result or {}
        spec_hash = research_spec_hash(spec)
        experiment_id = (
            f"exp_{utc_now().strftime('%Y%m%d_%H%M%S')}_{spec_hash[:10]}"
        )
        detail_path = DIRS["research_experiments"] / f"{experiment_id}.json"
        artifact = ""
        if result:
            artifact = self.update_champion(
                spec,
                result,
                experiment_id,
            )
            detail = {
                "experiment_id": experiment_id,
                "spec": spec,
                "result": {
                    key: value
                    for key, value in result.items()
                    if key != "frame"
                },
                "status": status,
                "error": error,
            }
            save_json(detail_path, detail)
        self.registry.record_evaluation(
            spec,
            experiment_id=experiment_id,
            status=status,
            result=result,
            artifact_path=artifact,
            detail_path=str(detail_path) if result else "",
            error=error,
        )
        production_manifest = load_json(
            DIRS["promotions"] / "technical_production.json",
            {},
        )
        if (
            result
            and str(production_manifest.get("stage", "")).lower()
            == "technical_production"
            and production_manifest.get("candidate_id")
            == lifecycle_candidate_id(spec)
        ):
            try:
                self.registry.set_stage(
                    lifecycle_candidate_id(spec),
                    "production",
                    reason=(
                        "technical_production.json is active for this "
                        "validated candidate"
                    ),
                    payload={
                        "technical_production_path": str(
                            DIRS["promotions"] / "technical_production.json"
                        ),
                        "experiment_id": production_manifest.get(
                            "experiment_id",
                            "",
                        ),
                    },
                )
            except Exception as exc:
                log_error("mark lifecycle production stage", exc)
        selected = result.get("selected_threshold", {})
        append_csv(
            LOG_FILES["research_experiments"],
            {
                "time_utc": iso_utc(),
                "experiment_id": experiment_id,
                "candidate_id": lifecycle_candidate_id(spec),
                "spec_hash": spec_hash,
                "source": spec.get("source", ""),
                "deployment_lane": spec.get("deployment_lane", ""),
                "account_focus": spec.get("account_focus", ""),
                "evaluation_stage": spec.get("evaluation_stage", "screen"),
                "status": status,
                "dataset_kind": spec.get("dataset_kind", ""),
                "dataset_hash": result.get("dataset_hash", ""),
                "model_type": spec.get("model_type", ""),
                "target": spec.get("target", ""),
                "outcome": spec.get("outcome", ""),
                "feature_set": spec.get("feature_set", ""),
                "instrument_subset": spec.get("instrument_subset", "all"),
                "instrument_whitelist": ",".join(
                    normalized_instrument_whitelist(
                        spec.get("instrument_whitelist"),
                    )
                ),
                "segment_filters": json_dumps(
                    normalized_segment_filters(spec.get("segment_filters")),
                ),
                "specialist_profile": spec.get("specialist_profile", ""),
                "specialist_parent_experiment_id": spec.get(
                    "specialist_parent_experiment_id",
                    "",
                ),
                "features": ",".join(result.get("features", [])),
                "parameters_json": json_dumps(spec.get("parameters", {})),
                "fold_count": result.get("fold_count", 0),
                "mean_auc": result.get("mean_auc", ""),
                "minimum_week_auc": result.get("minimum_week_auc", ""),
                "selected_threshold": selected.get("threshold", ""),
                "trades": selected.get("trades", ""),
                "positive_weeks": selected.get("positive_weeks", ""),
                "mean_net_pips": selected.get("mean_net_pips", ""),
                "median_week_mean_net_pips": selected.get(
                    "median_week_mean_net_pips",
                    "",
                ),
                "median_profit_factor": selected.get(
                    "median_profit_factor",
                    "",
                ),
                "bootstrap_lower_mean_net_pips": selected.get(
                    "bootstrap_lower_mean_net_pips",
                    "",
                ),
                "top_pair_trade_share": selected.get(
                    "top_pair_trade_share",
                    "",
                ),
                "score": result.get("score", ""),
                "gate_passed": bool(
                    result.get("gate", {}).get("passed", False)
                ),
                "model_artifact_path": artifact,
                "detail_path": str(detail_path) if result else "",
                "error": error[:2000],
            },
            RESEARCH_LEDGER_FIELDS,
        )
        completed = set(self.state.get("completed_spec_hashes", []))
        completed.add(spec_hash)
        self.state["completed_spec_hashes"] = list(completed)[-10_000:]
        self.state["last_experiment_utc"] = iso_utc()
        self.state["last_experiment_status"] = status
        save_json(self.state_path, self.state)
        ledger = read_csv(LOG_FILES["research_experiments"])
        if not ledger.empty:
            ledger["score_num"] = pd.to_numeric(
                ledger["score"],
                errors="coerce",
            ).fillna(-1e18)
            ledger.sort_values(
                ["gate_passed", "score_num"],
                ascending=[False, False],
            ).drop(columns=["score_num"]).to_csv(
                DIRS["research"] / "experiment_leaderboard.csv",
                index=False,
            )
        try:
            from oanda_model_metrics import write_latest_model_metrics_report
            write_latest_model_metrics_report()
        except Exception as exc:
            log_error("latest_model_metrics_report", exc)

    def run_forever(self) -> None:
        log_audit({
            "event_type": "continuous_research",
            "status": "running",
            "reason": "always-on historical model and strategy search started",
        })
        while not self.stop_event.is_set():
            memory_status = physical_memory_status()
            min_free_mb = max(
                0,
                safe_int(os.environ.get("OANDA_CONTINUOUS_RESEARCH_MIN_FREE_MEMORY_MB"), 4096),
            )
            defer_seconds = max(
                60,
                safe_int(os.environ.get("OANDA_CONTINUOUS_RESEARCH_LOW_MEMORY_SLEEP_SECONDS"), 300),
            )
            available_mb = safe_float(memory_status.get("available_mb"), 0.0)
            if min_free_mb > 0 and available_mb and available_mb < min_free_mb:
                guard = {
                    "time_utc": iso_utc(),
                    "status": "deferred_low_memory",
                    "available_mb": available_mb,
                    "min_free_mb": min_free_mb,
                    "sleep_seconds": defer_seconds,
                    "memory": memory_status,
                }
                self.state["current_experiment_status"] = "deferred_low_memory"
                self.state["last_resource_guard"] = guard
                self.state["last_research_heartbeat_utc"] = iso_utc()
                save_json(self.state_path, self.state)
                log_audit({
                    "event_type": "continuous_research_resource_guard",
                    "status": "deferred_low_memory",
                    "reason": (
                        f"available memory {available_mb:.0f}MB below "
                        f"{min_free_mb:.0f}MB minimum"
                    ),
                    "result": json_dumps(guard)[:1000],
                })
                time.sleep(defer_seconds)
                continue

            spec = self.next_spec()
            try:
                completed = self.completed_hashes()
                pending = sum(
                    1
                    for queued in self.registry.queued_specs()
                    if experiment_spec_hash(queued) not in completed
                )
            except Exception:
                pending = -1
            effective_stage, effective_holdout_weeks, effective_max_train_rows = (
                self.validation_settings(spec)
            )
            state_spec = dict(spec)
            requested_max_train_rows = safe_int(
                spec.get("max_train_rows"),
                effective_max_train_rows,
            )
            state_spec.update(
                {
                    "effective_evaluation_stage": effective_stage,
                    "effective_holdout_weeks": effective_holdout_weeks,
                    "effective_max_train_rows": effective_max_train_rows,
                    "requested_max_train_rows": requested_max_train_rows,
                    "max_train_rows_capped": bool(
                        requested_max_train_rows > effective_max_train_rows
                    ),
                }
            )
            self.state["current_experiment_spec"] = state_spec
            self.state["current_experiment_started_utc"] = iso_utc()
            self.state["current_experiment_status"] = "running"
            self.state["current_experiment_error"] = ""
            self.state["pending_queued_specs"] = pending
            self.state["last_research_heartbeat_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            try:
                result = self.evaluate(spec)
                status = (
                    "successful"
                    if result["gate"].get("passed")
                    else "unsuccessful"
                )
                self.state["current_experiment_status"] = "recording_result"
                self.state["current_experiment_completed_utc"] = iso_utc()
                save_json(self.state_path, self.state)
                self.record(spec, status, result=result)
                log_audit({
                    "event_type": "research_experiment",
                    "status": status,
                    "reason": (
                        f"{spec['dataset_kind']} {spec['model_type']} "
                        f"{spec['target']}"
                    ),
                    "result": json_dumps({
                        "score": result["score"],
                        "mean_auc": result["mean_auc"],
                        "minimum_week_auc": result["minimum_week_auc"],
                        "selected_threshold": result["selected_threshold"],
                        "gate": result["gate"],
                    })[:3000],
                })
                self.state["current_experiment_status"] = status
                self.state["current_experiment_completed_utc"] = iso_utc()
                self.state["current_experiment_error"] = ""
                save_json(self.state_path, self.state)
            except InsufficientRollingFolds as exc:
                reason = f"{type(exc).__name__}: {exc}"
                self.record(
                    spec,
                    "unsuccessful",
                    error=reason,
                )
                self.state["current_experiment_status"] = (
                    "skipped_insufficient_folds"
                )
                self.state["current_experiment_completed_utc"] = iso_utc()
                self.state["current_experiment_error"] = reason[:1000]
                save_json(self.state_path, self.state)
                log_audit({
                    "event_type": "research_experiment",
                    "status": "skipped_insufficient_folds",
                    "reason": reason[:1000],
                    "result": json_dumps({
                        "dataset_kind": spec.get("dataset_kind", ""),
                        "model_type": spec.get("model_type", ""),
                        "target": spec.get("target", ""),
                        "instrument_subset": spec.get(
                            "instrument_subset",
                            "all",
                        ),
                        "instrument_whitelist": spec.get(
                            "instrument_whitelist",
                            [],
                        ),
                        "segment_filters": normalized_segment_filters(
                            spec.get("segment_filters"),
                        ),
                    })[:2000],
                })
            except Exception as exc:
                self.record(
                    spec,
                    "error",
                    error=f"{type(exc).__name__}: {exc}",
                )
                self.state["current_experiment_status"] = "error"
                self.state["current_experiment_completed_utc"] = iso_utc()
                self.state["current_experiment_error"] = (
                    f"{type(exc).__name__}: {exc}"
                )[:1000]
                save_json(self.state_path, self.state)
                log_error("continuous_research_experiment", exc)
            self.stop_event.wait(CONTINUOUS_RESEARCH_SLEEP_SECONDS)

# -----------------------------------------------------------------------------
# GPT review
# -----------------------------------------------------------------------------
def get_openai_key(creds: Dict[str, Any]) -> str:
    return str(creds.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY") or "")


def call_gpt_for_candidates(creds: Dict[str, Any], summary: Dict[str, Any], active: StrategyProfile) -> List[StrategyProfile]:
    if not ALLOW_GPT_CALLS:
        return []
    if env_flag("OANDA_SKIP_GPT_REVIEW"):
        return []
    key = get_openai_key(creds)
    if not key:
        return []
    prompt = {
        "task": (
            "Propose 3 OANDA FX strategy profiles and 3 distinct model "
            "experiments for the continuous historical research engine. "
            "Return strict JSON only."
        ),
        "active_strategy": active.to_json(),
        "rolling_summary": summary,
        "constraints": {
            "account": "research only; no live account assignment",
            "use_margin": False,
            "no_averaging_down": True,
            "compound_only_winners": True,
            "must_backtest_before_activation": True,
            "must_pass_screen_validation_shadow_canary_before_production": True,
            "technical_account_activation_requires_production_manifest": True,
            "reject_if_cost_adjusted_expectancy_negative": True,
            "train_on_all_68_then_rank_by_validated_net_edge": True,
            "separate_results_by_pair_family_regime_session": True,
            "macro_bias_is_context_and_risk_veto_not_a_trade_trigger": True,
            "prefer_models_that_predict_direction_expected_move_mfe_mae_or_spread_clearance": True,
            "arima_is_baseline_only_not_primary_execution_model": True,
            "do_not_repeat_existing_ideas": True,
        },
        "required_json_shape": {
            "candidate_strategies": [{
                "name": "string", "family": "string", "entry_score_min": 80, "move_spread_ratio_min": 2.2,
                "momentum_pips_min": 4.0, "max_margin_pct": 55, "risk_per_trade_pct": 3.5,
                "max_open_trades": 6, "stop_pips": 12, "take_profit_pips": 28, "trailing_stop_pips": 10,
                "prune_red_minutes": 20, "prune_stale_minutes": 45, "why": "string"
            }],
            "model_experiments": [{
                "dataset_kind": "technical_spike, profitable_move_precursor, or base_trade_quality",
                "model_type": "random_forest, extra_trees, hist_gradient_boosting, gradient_boosting, lightgbm, xgboost, catboost, or ngboost; use optional estimators only if available in the environment",
                "target": "for technical: continuation_profit_30/60/120, reversal_profit_30/60/120, short_profit_30/60/120, long_profit_30/60/120, continuation_curve_profit_30/60/120, reversal_curve_profit_30/60/120, short_curve_profit_30/60/120, long_curve_profit_30/60/120, major_event_30/60/120, or major_event_lead_15_60 / major_event_lead_30_60 / major_event_lead_15_120 / major_event_lead_30_120 / major_event_lead_60_120; for precursor: profitable_long_move_30/60/120, profitable_short_move_30/60/120, or profitable_any_move_30/60/120; for base: would_profit_30m",
                "outcome": "matching *_net_atr_* or *_curve_net_atr_* column; major_event and major_event_lead use two_stage_fixed_net_atr_30/60/120, two_stage_trailing_net_atr_30/60/120, two_stage_trailing_stop010_net_atr_30/60/120, two_stage_trailing_stop012_net_atr_30/60/120, two_stage_trailing_stop015_net_atr_30/60/120, two_stage_trailing_stop020_net_atr_30/60/120, two_stage_trailing_stop025_net_atr_30/60/120, two_stage_trailing_stop035_net_atr_30/60/120, two_stage_trailing_stop050_net_atr_30/60/120, two_stage_trailing_stop075_net_atr_30/60/120, or two_stage_trailing_stop100_net_atr_30/60/120; precursor any uses best_net_atr_30/60/120; for base: net_vol_units_30",
                "feature_set": "technical_core, technical_full, or base",
                "instrument_subset": "all, majors, usd_pairs, non_usd, volatile, exotic, volatile_exotic, non_usd_volatile, usd_major, usd_other, jpy_risk, commodity, eur_gbp_cross, chf_safe_haven, exotic_high_spread, volatile_non_usd, or other_cross",
                "research_role": "major_move_factor for major_event/major_event_lead; profitable_move_precursor for precursor; return_curve_candidate for *_curve_profit_*; otherwise standalone_candidate",
                "direction_target": "major_direction_up_30/60/120 when target is major_event; major_direction_up_lead_15_60 etc when target is major_event_lead; best_direction_up_30/60/120 when target is profitable_any_move",
                "execution_policy": "fixed, trailing, trailing_stop010, trailing_stop012, trailing_stop015, trailing_stop020, trailing_stop025, trailing_stop035, trailing_stop050, trailing_stop075, or trailing_stop100 when target is major_event or major_event_lead",
                "required_analysis": "explain expected useful pair families, regimes, sessions, spread/slippage behavior, and whether macro/risk-off context should veto rather than trigger",
                "parameters": {
                    "n_estimators": 140,
                    "max_depth": 7,
                    "min_samples_leaf": 10
                },
                "why": "string"
            }]
        }
    }
    start = time.time()
    try:
        # Use direct HTTP to avoid SDK version issues.
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": os.environ.get("OPENAI_STRATEGY_MODEL", "gpt-4.1-mini"),
                "messages": [
                    {"role": "system", "content": "You are a trading strategy research assistant. Return valid JSON only. Do not include prose."},
                    {"role": "user", "content": json.dumps(prompt, default=str)},
                ],
                "temperature": 0.2,
                "max_tokens": 1800,
            },
            timeout=60,
        )
        latency_ms = int((time.time() - start) * 1000)
        raw = resp.text
        append_csv(LOG_FILES["gpt_calls"], {
            "time_utc": iso_utc(), "purpose": "candidate_strategy_generation", "model": os.environ.get("OPENAI_STRATEGY_MODEL", "gpt-4.1-mini"),
            "http_status": resp.status_code, "latency_ms": latency_ms, "raw_chars": len(raw), "error": "" if resp.ok else raw[:500]
        }, ["time_utc", "purpose", "model", "http_status", "latency_ms", "raw_chars", "error"])
        if not resp.ok:
            return []
        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        content = content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?", "", content).strip()
            content = re.sub(r"```$", "", content).strip()
        obj = json.loads(content)
        model_experiments = []
        for raw_spec in obj.get("model_experiments", [])[:10]:
            normalized = normalize_gpt_experiment_spec(raw_spec)
            if normalized:
                model_experiments.append(normalized)
        if model_experiments:
            save_json(
                DIRS["research_specs"]
                / f"gpt_model_specs_{utc_now().strftime('%Y%m%d_%H%M%S')}.json",
                {
                    "generated_utc": iso_utc(),
                    "model_experiments": model_experiments,
                },
            )
        candidates = []
        for i, c in enumerate(obj.get("candidate_strategies", [])[:5], 1):
            fam = re.sub(r"[^a-zA-Z0-9_]+", "_", str(c.get("family") or c.get("name") or "gpt_candidate")).lower().strip("_")
            sid = f"dum1_gpt_{fam}_{utc_now().strftime('%Y%m%d_%H%M%S')}_{i}"
            candidates.append(StrategyProfile(
                strategy_id=sid,
                name=str(c.get("name") or sid),
                family=fam,
                version=1,
                model_type="gpt_proposed_rule_profile",
                entry_score_min=safe_float(c.get("entry_score_min"), 84),
                high_confidence_score=max(90, safe_float(c.get("entry_score_min"), 84) + 6),
                move_spread_ratio_min=safe_float(c.get("move_spread_ratio_min"), 2.4),
                momentum_pips_min=safe_float(c.get("momentum_pips_min"), 5),
                max_margin_pct=safe_float(c.get("max_margin_pct"), 55),
                risk_per_trade_pct=safe_float(c.get("risk_per_trade_pct"), 3.5),
                max_open_trades=safe_int(c.get("max_open_trades"), 6),
                stop_pips=safe_float(c.get("stop_pips"), 12),
                take_profit_pips=safe_float(c.get("take_profit_pips"), 28),
                trailing_stop_pips=safe_float(c.get("trailing_stop_pips"), 10),
                prune_red_minutes=safe_int(c.get("prune_red_minutes"), 20),
                prune_stale_minutes=safe_int(c.get("prune_stale_minutes"), 45),
                notes=str(c.get("why") or "GPT proposed candidate"),
            ))
        path = DIRS["gpt_reviews"] / f"gpt_candidates_{utc_now().strftime('%Y%m%d_%H%M%S')}.json"
        save_json(path, {
            "raw_content": content,
            "candidates": [x.to_json() for x in candidates],
            "model_experiments": model_experiments,
        })
        return candidates
    except Exception as exc:
        log_error("call_gpt_for_candidates", exc)
        return []

# -----------------------------------------------------------------------------
# Main manager
# -----------------------------------------------------------------------------
class TrainingStrategyManager:
    def __init__(self) -> None:
        ensure_dirs()
        self.creds = load_creds(CREDS_PATH)
        token, base_url, account_id = resolve_oanda_creds(self.creds)
        self.account_id = account_id
        self.env = "practice" if "fxpractice" in base_url else "live"
        self.client = OandaClient(token, base_url, account_id)
        self.state = load_json(STATE_FILE, {})
        self.state.setdefault("started_utc", iso_utc())
        self.state.setdefault("last_account_sync_utc", "")
        self.state.setdefault("last_market_scan_utc", "")
        self.state.setdefault("last_training_dataset_utc", "")
        self.state.setdefault("last_light_backtest_utc", "")
        self.state.setdefault("last_gpt_review_utc", "")
        self.state.setdefault("last_report_utc", "")
        self.state.setdefault("last_export_utc", "")
        self.state.setdefault("last_historical_backfill_utc", "")
        self.state.setdefault("last_technical_evidence_utc", "")
        self.state.setdefault("last_weekend_rolling_validation_utc", "")
        self.state.setdefault("last_weekend_rolling_validation_dataset_hash", "")
        self.state.setdefault("last_training_dataset_hash", "")
        self.state.setdefault("last_trained_dataset_hash", "")
        self.state.setdefault("loss_cooldowns", {})
        self.state.setdefault("known_instruments", [])
        self.state["latest_production_gate"] = current_production_gate()
        self.state["latest_technical_evidence"] = ingest_technical_exhaustion_evidence()
        self.state["last_technical_evidence_utc"] = iso_utc()
        self.active_strategy = self.load_or_create_active_strategy()
        self.instruments = self.load_instruments()
        self.last_snapshot = AccountSnapshot()
        self.last_open_trades: List[Dict[str, Any]] = []
        self.research_engine = ContinuousResearchEngine()
        save_json(STATE_FILE, self.state)

    def load_or_create_active_strategy(self) -> StrategyProfile:
        if ACTIVE_STRATEGY_FILE.exists():
            try:
                return StrategyProfile.from_json(load_json(ACTIVE_STRATEGY_FILE, {}))
            except Exception:
                pass
        prof = default_active_strategy()
        save_json(ACTIVE_STRATEGY_FILE, prof.to_json())
        for cand in seed_candidate_profiles(prof):
            self.save_candidate(cand, source="seed")
        return prof

    def load_instruments(self) -> List[str]:
        known = self.state.get("known_instruments") or []
        if known:
            return list(dict.fromkeys(known))[:MAX_INSTRUMENTS_SCAN]
        try:
            instruments = self.client.list_instruments()
            if not instruments:
                instruments = PREFERRED_MAJOR_INSTRUMENTS
            # Prefer majors/crosses first, then rest.
            instruments = list(dict.fromkeys(PREFERRED_MAJOR_INSTRUMENTS + instruments))[:MAX_INSTRUMENTS_SCAN]
            self.state["known_instruments"] = instruments
            self.state["last_instrument_refresh_utc"] = iso_utc()
            save_json(STATE_FILE, self.state)
            return instruments
        except Exception as exc:
            log_error("load_instruments", exc)
            return PREFERRED_MAJOR_INSTRUMENTS

    def refresh_instruments_if_needed(self) -> None:
        last = self.state.get("last_instrument_refresh_utc")
        if not last or utc_now() - parse_oanda_time(last) > timedelta(seconds=INSTRUMENT_REFRESH_SECONDS):
            self.state["known_instruments"] = []
            self.instruments = self.load_instruments()

    def save_candidate(self, profile: StrategyProfile, source: str = "unknown") -> None:
        path = DIRS["candidates"] / f"{profile.strategy_id}.json"
        save_json(path, {**profile.to_json(), "candidate_source": source, "created_utc": iso_utc()})
        append_jsonl(LOG_FILES["candidate_queue"], {"time_utc": iso_utc(), "source": source, "strategy": profile.to_json()})

    def sync_account(self) -> None:
        data = self.client.get_account()
        if data.get("_error"):
            log_audit({"account_id": self.account_id, "event_type": "account_sync", "status": "error", "result": json_dumps(data)[:1000]})
            return
        self.last_snapshot = parse_account_summary(data)
        trades = self.client.list_trades()
        self.last_open_trades = open_trades_from_response(trades)
        row = {
            "time_utc": iso_utc(), "account_id": self.account_id,
            "nav": self.last_snapshot.nav, "balance": self.last_snapshot.balance,
            "margin_used": self.last_snapshot.margin_used, "margin_available": self.last_snapshot.margin_available,
            "margin_used_pct": self.last_snapshot.margin_used_pct,
            "open_trade_count": len(self.last_open_trades),
            "pending_order_count": self.last_snapshot.pending_order_count,
            "active_strategy_id": self.active_strategy.strategy_id,
        }
        append_csv(LOG_FILES["monitor"], row, MONITOR_FIELDS)
        log_audit({
            "account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "monitor", "status": "ok",
            "nav": self.last_snapshot.nav, "balance": self.last_snapshot.balance, "margin_used_pct": self.last_snapshot.margin_used_pct,
            "open_trade_count": len(self.last_open_trades), "strategy_id": self.active_strategy.strategy_id,
            "strategy_family": self.active_strategy.family,
        })
        self.state["last_account_sync_utc"] = iso_utc()
        self.state["last_account_snapshot"] = row
        save_json(STATE_FILE, self.state)

    def backfill_candles(self, instruments: Optional[List[str]] = None, granularities: Optional[List[str]] = None) -> None:
        if not ALLOW_CANDLE_BACKFILL:
            return
        if HISTORY_EXPANSION_LOCK.exists():
            log_audit({
                "event_type": "candle_backfill",
                "status": "skipped",
                "reason": "historical expansion worker owns candle writes",
            })
            return
        instruments = instruments or self.instruments
        granularities = granularities or CANDLE_GRANULARITIES
        for inst in instruments[:MAX_INSTRUMENTS_SCAN]:
            for gran in granularities:
                try:
                    data = self.client.candles(inst, granularity=gran, count=DEFAULT_CANDLE_COUNT)
                    if data.get("_error"):
                        log_audit({"event_type": "candle_backfill", "status": "error", "instrument": inst, "reason": gran, "result": json_dumps(data)[:1000]})
                        continue
                    df = candle_df_from_oanda(data, inst, gran)
                    if not df.empty:
                        save_candles_csv(df, inst, gran)
                        log_audit({"event_type": "candle_backfill", "status": "ok", "instrument": inst, "reason": gran, "result": f"rows={len(df)}"})
                    time.sleep(0.05)
                except Exception as exc:
                    log_error(f"backfill_candles {inst} {gran}", exc)
        self.state["last_historical_backfill_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)

    def deep_backfill_m1(
        self,
        days: int = DEEP_BACKFILL_DEFAULT_DAYS,
        instruments: Optional[List[str]] = None,
        batch_size: int = DEEP_BACKFILL_BATCH_SIZE,
    ) -> Dict[str, Any]:
        if not ALLOW_CANDLE_BACKFILL:
            return {"completed": False, "reason": "candle backfill disabled"}
        days = max(1, int(days))
        batch_size = min(max(int(batch_size), 100), 5000)
        instruments = (instruments or self.instruments)[:MAX_INSTRUMENTS_SCAN]
        cutoff = utc_now() - timedelta(days=days)
        summary: Dict[str, Any] = {
            "completed": False,
            "days": days,
            "cutoff_utc": cutoff.isoformat(),
            "instruments": {},
            "started_utc": iso_utc(),
        }
        summary_path = DIRS["reports"] / "latest_deep_backfill_summary.json"
        for instrument_number, inst in enumerate(instruments, 1):
            local = load_local_candles(inst, "M1")
            rows_before = len(local)
            local_times = (
                pd.to_datetime(
                    local.get("datetime", local.get("time")),
                    errors="coerce",
                    utc=True,
                )
                if not local.empty
                else pd.Series(dtype="datetime64[ns, UTC]")
            )
            existing_earliest = (
                local_times.min().to_pydatetime()
                if not local_times.empty and not pd.isna(local_times.min())
                else None
            )
            if existing_earliest is not None and existing_earliest <= cutoff:
                summary["instruments"][inst] = {
                    "requests": 0,
                    "rows_before": rows_before,
                    "rows_after": rows_before,
                    "rows_added": 0,
                    "earliest": existing_earliest.isoformat(),
                    "latest": (
                        local_times.max().isoformat()
                        if not local_times.empty
                        else ""
                    ),
                    "error": "",
                    "status": "already_complete",
                }
                save_json(summary_path, summary)
                continue
            batches: List[pd.DataFrame] = []
            end_time = (
                existing_earliest - timedelta(seconds=1)
                if existing_earliest is not None
                else utc_now()
            )
            previous_earliest: Optional[datetime] = None
            request_count = 0
            error = ""
            while True:
                data = self.client.candles(
                    inst,
                    granularity="M1",
                    count=batch_size,
                    end_time=end_time,
                )
                request_count += 1
                if data.get("_error"):
                    error = json_dumps(data)[:1000]
                    break
                batch = candle_df_from_oanda(data, inst, "M1")
                if batch.empty:
                    break
                batches.append(batch)
                earliest = parse_oanda_time(str(batch.iloc[0]["datetime"]))
                if earliest <= cutoff:
                    break
                if previous_earliest is not None and earliest >= previous_earliest:
                    error = "pagination made no backward progress"
                    break
                previous_earliest = earliest
                end_time = earliest - timedelta(seconds=1)
                time.sleep(0.08)
            rows_after = rows_before
            earliest_saved = ""
            latest_saved = ""
            if batches:
                combined = pd.concat(batches, ignore_index=True)
                combined["dt_filter"] = pd.to_datetime(combined["datetime"], errors="coerce", utc=True)
                combined = combined[combined["dt_filter"] >= pd.Timestamp(cutoff)]
                combined = combined.drop(columns=["dt_filter"])
                if not combined.empty:
                    path = save_candles_csv(combined, inst, "M1")
                    saved = read_csv(path)
                    rows_after = len(saved)
                    if not saved.empty:
                        earliest_saved = str(saved.iloc[0].get("datetime", saved.iloc[0].get("time", "")))
                        latest_saved = str(saved.iloc[-1].get("datetime", saved.iloc[-1].get("time", "")))
            summary["instruments"][inst] = {
                "requests": request_count,
                "rows_before": rows_before,
                "rows_after": rows_after,
                "rows_added": max(0, rows_after - rows_before),
                "earliest": earliest_saved,
                "latest": latest_saved,
                "error": error,
                "status": "completed" if not error else "partial_error",
            }
            save_json(summary_path, summary)
            print(
                f"[deep-backfill] {instrument_number}/{len(instruments)} {inst}: "
                f"requests={request_count} rows={rows_after} added={max(0, rows_after - rows_before)}"
                + (f" error={error}" if error else ""),
                flush=True,
            )
        summary["completed_utc"] = iso_utc()
        summary["completed"] = True
        summary["total_rows_added"] = sum(
            safe_int(item.get("rows_added")) for item in summary["instruments"].values()
        )
        save_json(summary_path, summary)
        self.state["last_historical_backfill_utc"] = iso_utc()
        self.state["last_deep_backfill_utc"] = iso_utc()
        self.state["last_deep_backfill_days"] = days
        self.state["last_deep_backfill_summary"] = str(summary_path)
        save_json(STATE_FILE, self.state)
        return summary

    def market_scan(self) -> List[MarketSignal]:
        signals: List[MarketSignal] = []
        prices: Dict[str, Dict[str, float]] = {}
        chunks = [self.instruments[i:i + 50] for i in range(0, len(self.instruments), 50)]
        for ch in chunks:
            data = self.client.pricing(ch)
            prices.update(pricing_map(data))
            time.sleep(0.05)
        for inst in self.instruments[:MAX_INSTRUMENTS_SCAN]:
            try:
                df = load_local_candles(inst, "M1")
                if df.empty or len(df) < 80:
                    data = self.client.candles(inst, "M1", DEFAULT_CANDLE_COUNT)
                    df = candle_df_from_oanda(data, inst, "M1")
                    if not df.empty:
                        save_candles_csv(df, inst, "M1")
                px = prices.get(inst)
                if not px and not df.empty:
                    close = safe_float(df.iloc[-1].get("close"))
                    px = {"mid": close, "bid": close, "ask": close, "spread_pips": 1.5}
                if not px:
                    continue
                sig = compute_signal(inst, df, px, self.active_strategy)
                if not sig:
                    continue
                signals.append(sig)
                log_audit({
                    "account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "signal", "status": "seen",
                    "instrument": sig.instrument, "direction": sig.direction, "theme": sig.theme,
                    "strategy_id": self.active_strategy.strategy_id, "strategy_family": self.active_strategy.family,
                    "signal_score": sig.signal_score, "confidence_bucket": sig.confidence_bucket,
                    "move_abs_pips": max(abs(sig.momentum_15_pips), abs(sig.momentum_30_pips)),
                    "move_net_pips": sig.momentum_15_pips,
                    "spread_pips": sig.spread_pips, "move_spread_ratio": sig.move_spread_ratio,
                    "margin_used_pct": self.last_snapshot.margin_used_pct, "nav": self.last_snapshot.nav,
                    "open_trade_count": len(self.last_open_trades), "reason": sig.reason,
                })
            except Exception as exc:
                log_error(f"market_scan {inst}", exc)
        # Persist training rows for latest scan.
        if signals:
            df = pd.DataFrame([s.to_row() for s in signals])
            path = DIRS["features"] / f"signals_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(path, index=False)
        self.state["last_market_scan_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)
        return signals

    def prune_positions(self) -> None:
        # Lightweight exit manager for DUM1 learner account.
        if not self.last_open_trades:
            return
        for tr in list(self.last_open_trades):
            try:
                trade_id = str(tr.get("id"))
                inst = tr.get("instrument", "")
                pl = safe_float(tr.get("unrealizedPL"))
                open_time = parse_oanda_time(tr.get("openTime", ""))
                age_min = (utc_now() - open_time).total_seconds() / 60.0
                reason = ""
                if pl < -max(0.05, self.last_snapshot.nav * 0.0025) and age_min >= self.active_strategy.prune_red_minutes:
                    reason = f"red prune pl={pl:.4f} age={age_min:.1f}m"
                elif pl <= 0 and age_min >= self.active_strategy.prune_stale_minutes:
                    reason = f"stale red/flat prune pl={pl:.4f} age={age_min:.1f}m"
                if not reason:
                    continue
                intent_id = f"close_{trade_id}_{int(time.time()*1000)}"
                append_csv(LOG_FILES["orders_intent"], {
                    "time_utc": iso_utc(), "intent_id": intent_id, "account_id": self.account_id, "strategy_id": self.active_strategy.strategy_id,
                    "instrument": inst, "direction": "CLOSE", "units": "ALL", "entry_price_ref": "", "stop_loss": "", "take_profit": "",
                    "trailing_stop_distance": "", "signal_score": "", "reason": reason, "status": "queued",
                }, ORDER_INTENT_FIELDS)
                if EXECUTE_TRADES_BY_DEFAULT and self.env == "practice":
                    res = self.client.close_trade(trade_id)
                    status = "submitted" if not res.get("_error") else "error"
                    append_csv(LOG_FILES["orders_result"], {
                        "time_utc": iso_utc(), "intent_id": intent_id, "account_id": self.account_id, "strategy_id": self.active_strategy.strategy_id,
                        "instrument": inst, "direction": "CLOSE", "units": "ALL", "http_status": res.get("_http_status", ""),
                        "latency_ms": res.get("_latency_ms", ""), "order_id": "", "trade_id": trade_id, "fill_price": "", "status": status,
                        "reason": reason, "raw_json": json_dumps(res)[:5000],
                    }, ORDER_RESULT_FIELDS)
                    if status == "submitted" and pl < 0:
                        self.state.setdefault("loss_cooldowns", {})[inst] = iso_utc()
                log_audit({"account_id": self.account_id, "event_type": "position_prune", "status": "accepted", "instrument": inst, "trade_id": trade_id, "reason": reason})
            except Exception as exc:
                log_error("prune_positions", exc)
        save_json(STATE_FILE, self.state)

    def execute_best_signal(self, signals: List[MarketSignal]) -> None:
        if not signals:
            return
        # Sort by score and spread efficiency. Only take top candidate each cycle to control churn.
        ranked = sorted(signals, key=lambda s: (s.signal_score, s.move_spread_ratio, abs(s.momentum_15_pips)), reverse=True)
        for sig in ranked[:10]:
            ok_profile, reason_profile = signal_passes_profile(sig, self.active_strategy)
            if not ok_profile:
                log_audit({
                    "account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "entry_decision", "status": "skipped",
                    "instrument": sig.instrument, "direction": sig.direction, "theme": sig.theme,
                    "strategy_id": self.active_strategy.strategy_id, "strategy_family": self.active_strategy.family,
                    "signal_score": sig.signal_score, "confidence_bucket": sig.confidence_bucket,
                    "spread_pips": sig.spread_pips, "move_spread_ratio": sig.move_spread_ratio,
                    "margin_used_pct": self.last_snapshot.margin_used_pct, "reason": reason_profile,
                })
                continue
            ok_open, reason_open = can_open_signal(self.last_snapshot, self.last_open_trades, sig, self.active_strategy, self.state)
            if not ok_open:
                log_audit({
                    "account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "entry_decision", "status": "blocked",
                    "instrument": sig.instrument, "direction": sig.direction, "theme": sig.theme,
                    "strategy_id": self.active_strategy.strategy_id, "strategy_family": self.active_strategy.family,
                    "signal_score": sig.signal_score, "confidence_bucket": sig.confidence_bucket,
                    "spread_pips": sig.spread_pips, "move_spread_ratio": sig.move_spread_ratio,
                    "margin_used_pct": self.last_snapshot.margin_used_pct, "reason": reason_open,
                })
                continue
            units = calculate_units(self.last_snapshot, sig, self.active_strategy, sig.mid)
            if units == 0:
                log_audit({"account_id": self.account_id, "event_type": "entry_decision", "status": "blocked", "instrument": sig.instrument, "direction": sig.direction, "reason": "units rounded to zero"})
                continue
            sl, tp, trail = stop_tp_prices(sig, self.active_strategy)
            intent_id = f"open_{sig.instrument}_{sig.direction}_{int(time.time()*1000)}"
            append_csv(LOG_FILES["orders_intent"], {
                "time_utc": iso_utc(), "intent_id": intent_id, "account_id": self.account_id, "strategy_id": self.active_strategy.strategy_id,
                "instrument": sig.instrument, "direction": sig.direction, "units": units,
                "entry_price_ref": sig.mid, "stop_loss": sl, "take_profit": tp, "trailing_stop_distance": trail,
                "signal_score": sig.signal_score, "reason": sig.reason, "status": "queued",
            }, ORDER_INTENT_FIELDS)
            log_audit({
                "account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "order_intent", "status": "queued",
                "instrument": sig.instrument, "direction": sig.direction, "theme": sig.theme,
                "strategy_id": self.active_strategy.strategy_id, "strategy_family": self.active_strategy.family,
                "signal_score": sig.signal_score, "confidence_bucket": sig.confidence_bucket,
                "move_abs_pips": max(abs(sig.momentum_15_pips), abs(sig.momentum_30_pips)), "spread_pips": sig.spread_pips,
                "move_spread_ratio": sig.move_spread_ratio, "margin_used_pct": self.last_snapshot.margin_used_pct,
                "reason": sig.reason,
            })
            if EXECUTE_TRADES_BY_DEFAULT and self.env == "practice":
                res = self.client.create_market_order(sig.instrument, units, take_profit_price=tp, stop_loss_price=sl, trailing_stop_distance=trail)
                status = "accepted" if not res.get("_error") and (res.get("orderFillTransaction") or res.get("orderCreateTransaction")) else ("cancelled" if res.get("orderCancelTransaction") else "error")
                order_id = ""
                trade_id = ""
                fill_price = ""
                fill = res.get("orderFillTransaction") or {}
                create = res.get("orderCreateTransaction") or {}
                cancel = res.get("orderCancelTransaction") or {}
                order_id = fill.get("orderID") or create.get("id") or cancel.get("orderID") or ""
                trade_id = (fill.get("tradeOpened") or {}).get("tradeID") or fill.get("id") or ""
                fill_price = fill.get("price", "")
                append_csv(LOG_FILES["orders_result"], {
                    "time_utc": iso_utc(), "intent_id": intent_id, "account_id": self.account_id, "strategy_id": self.active_strategy.strategy_id,
                    "instrument": sig.instrument, "direction": sig.direction, "units": units,
                    "http_status": res.get("_http_status", ""), "latency_ms": res.get("_latency_ms", ""),
                    "order_id": order_id, "trade_id": trade_id, "fill_price": fill_price, "status": status,
                    "reason": cancel.get("reason") or sig.reason, "raw_json": json_dumps(res)[:5000],
                }, ORDER_RESULT_FIELDS)
                log_audit({
                    "account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "order_result", "status": status,
                    "instrument": sig.instrument, "direction": sig.direction, "theme": sig.theme,
                    "strategy_id": self.active_strategy.strategy_id, "strategy_family": self.active_strategy.family,
                    "signal_score": sig.signal_score, "confidence_bucket": sig.confidence_bucket,
                    "result": json_dumps({"order_id": order_id, "trade_id": trade_id, "fill_price": fill_price, "http": res.get("_http_status")}),
                    "raw_json": json_dumps(res)[:1000],
                })
            else:
                log_audit({"account_id": self.account_id, "event_type": "order_result", "status": "dry_run", "instrument": sig.instrument, "direction": sig.direction, "reason": "execution disabled or non-practice env"})
            break

    def build_training_dataset(self, latest_signals: Optional[List[MarketSignal]] = None) -> pd.DataFrame:
        latest_signals = latest_signals or []
        ds = make_training_rows_from_signals(latest_signals)
        # Add recent saved signal files if current scan produced little.
        if len(ds) < 20:
            rows = []
            for p in sorted(DIRS["features"].glob("signals_*.csv"))[-20:]:
                try:
                    rows.append(pd.read_csv(p))
                except Exception:
                    pass
            if rows:
                sigdf = pd.concat(rows, ignore_index=True).drop_duplicates(subset=["time_utc", "instrument", "direction"], keep="last")
                signals = []
                for _, r in sigdf.tail(500).iterrows():
                    try:
                        signals.append(MarketSignal(**{k: r.get(k) for k in dataclasses.asdict(MarketSignal("","","", "",0,0,0,0,0,0,0,0,0,0,0,0,"","")).keys()}))
                    except Exception:
                        continue
                ds2 = make_training_rows_from_signals(signals)
                if not ds2.empty:
                    ds = pd.concat([ds, ds2], ignore_index=True) if not ds.empty else ds2
        if len(ds) < 50:
            historical_paths = sorted(DIRS["training_sets"].glob("historical_training_set_*.csv"))
            if historical_paths:
                try:
                    historical = pd.read_csv(historical_paths[-1])
                    if len(historical) > HISTORICAL_TRAINING_MAX_ROWS_FOR_LIVE_RETRAIN:
                        historical = historical.tail(HISTORICAL_TRAINING_MAX_ROWS_FOR_LIVE_RETRAIN)
                    ds = pd.concat([historical, ds], ignore_index=True) if not ds.empty else historical
                    ds = ds.drop_duplicates(
                        subset=["time_utc", "instrument", "direction"],
                        keep="last",
                    ).sort_values(["time_utc", "instrument"]).reset_index(drop=True)
                except Exception as exc:
                    log_error("load historical training dataset", exc)
        if not ds.empty:
            dataset_hash = dataframe_fingerprint(ds)
            ds.attrs["dataset_hash"] = dataset_hash
            prior_hash = str(self.state.get("last_training_dataset_hash", ""))
            prior_path = Path(str(self.state.get("last_training_dataset_path", "")))
            if dataset_hash != prior_hash or not prior_path.exists():
                path = DIRS["training_sets"] / f"training_set_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
                ds.to_csv(path, index=False)
                self.state["last_training_dataset_path"] = str(path)
                self.state["last_training_dataset_hash"] = dataset_hash
                log_audit({
                    "event_type": "training_dataset",
                    "status": "stored",
                    "result": json_dumps({
                        "path": str(path),
                        "rows": len(ds),
                        "dataset_hash": dataset_hash,
                    }),
                })
            else:
                log_audit({
                    "event_type": "training_dataset",
                    "status": "duplicate_skipped",
                    "reason": "dataset content unchanged",
                    "result": json_dumps({
                        "path": str(prior_path),
                        "rows": len(ds),
                        "dataset_hash": dataset_hash,
                    }),
                })
        self.state["last_training_dataset_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)
        return ds

    def run_backtests_and_maybe_switch(self) -> None:
        candidates = [self.active_strategy]
        # Load candidate JSON files.
        for p in sorted(DIRS["candidates"].glob("*.json"))[-50:]:
            try:
                obj = load_json(p, {})
                candidates.append(StrategyProfile.from_json(obj))
            except Exception:
                continue
        # Deduplicate by ID.
        seen = set()
        uniq = []
        for c in candidates:
            if c.strategy_id not in seen:
                seen.add(c.strategy_id)
                uniq.append(c)
        results = []
        instruments = self.instruments[:MAX_INSTRUMENTS_SCAN]
        for prof in uniq[:20]:
            try:
                res = backtest_profile(prof, instruments)
                results.append(res)
                update_benchmark(res, source_type="BACKTEST", instrument="ALL")
            except Exception as exc:
                log_error(f"backtest {prof.strategy_id}", exc)
        if not results:
            return
        ranked = sorted(results, key=lambda x: safe_float(x.get("score")), reverse=True)
        leaderboard_path = DIRS["backtests"] / "latest_backtest_leaderboard.csv"
        pd.DataFrame([{k: v for k, v in r.items() if k != "trades"} for r in ranked]).to_csv(leaderboard_path, index=False)
        current = next((r for r in ranked if r.get("strategy_id") == self.active_strategy.strategy_id), None)
        best = ranked[0]
        should_switch = False
        reason = ""
        if best.get("strategy_id") != self.active_strategy.strategy_id:
            edge = safe_float(best.get("score")) - safe_float((current or {}).get("score"))
            if safe_int(best.get("total_trades")) >= MIN_BACKTEST_TRADES_FOR_SWITCH and edge >= MIN_SCORE_EDGE_TO_SWITCH and safe_float(best.get("profit_factor")) >= MIN_PROFIT_FACTOR_TO_SWITCH and safe_float(best.get("max_drawdown_pct")) <= MAX_DRAWDOWN_PCT_FOR_SWITCH and safe_float(best.get("outlier_dependency")) <= MAX_OUTLIER_DEPENDENCY:
                should_switch = True
                reason = f"Backtest candidate beat active by score edge {edge:.2f}; pf={best.get('profit_factor')} trades={best.get('total_trades')} dd={best.get('max_drawdown_pct')}"
            else:
                reason = f"Best candidate not switched; edge={edge:.2f}, trades={best.get('total_trades')}, pf={best.get('profit_factor')}, dd={best.get('max_drawdown_pct')}, outlier={best.get('outlier_dependency')}"
        else:
            reason = "Active strategy remains top-ranked in latest backtest."
        production_gate = current_production_gate()
        self.state["latest_production_gate"] = production_gate
        if (
            should_switch
            and REQUIRE_PRODUCTION_GATE_FOR_AUTO_SWITCH
            and not production_gate.get("passed", False)
        ):
            should_switch = False
            reason = (
                f"{reason}; auto-switch blocked because production validation has not passed "
                f"({production_gate.get('report_path') or 'no report'})"
            )
        log_audit({"event_type": "strategy_review", "status": "completed", "strategy_id": self.active_strategy.strategy_id, "reason": reason, "result": json_dumps({k: v for k, v in best.items() if k != "trades"})[:1000]})
        if should_switch and ALLOW_AUTO_SWITCH_DUM1_STRATEGY:
            # Locate candidate profile JSON.
            new_profile = None
            for p in DIRS["candidates"].glob("*.json"):
                obj = load_json(p, {})
                if obj.get("strategy_id") == best.get("strategy_id"):
                    new_profile = StrategyProfile.from_json(obj)
                    break
            if new_profile:
                old_id = self.active_strategy.strategy_id
                self.active_strategy = new_profile
                save_json(ACTIVE_STRATEGY_FILE, self.active_strategy.to_json())
                change = {"time_utc": iso_utc(), "old_strategy_id": old_id, "new_strategy_id": new_profile.strategy_id, "reason": reason, "best_result": {k: v for k, v in best.items() if k != "trades"}}
                self.state["latest_strategy_change"] = change
                append_csv(LOG_FILES["strategy_changes"], {
                    "time_utc": change["time_utc"], "old_strategy_id": old_id, "new_strategy_id": new_profile.strategy_id,
                    "action": "AUTO_SWITCH_DUM1", "reason": reason, "score": best.get("score"), "profit_factor": best.get("profit_factor"), "total_trades": best.get("total_trades"),
                }, ["time_utc", "old_strategy_id", "new_strategy_id", "action", "reason", "score", "profit_factor", "total_trades"])
                log_audit({"event_type": "strategy_change", "status": "auto_switched", "strategy_id": new_profile.strategy_id, "reason": reason})
                save_json(STATE_FILE, self.state)
        self.state["last_light_backtest_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)

    def maybe_gpt_review(self) -> None:
        if env_flag("OANDA_SKIP_GPT_REVIEW"):
            log_audit({
                "event_type": "gpt_review",
                "status": "skipped",
                "reason": "OANDA_SKIP_GPT_REVIEW",
                "result": "network-backed GPT review disabled for this run",
            })
            self.state["last_gpt_review_utc"] = iso_utc()
            save_json(STATE_FILE, self.state)
            return
        summary = summarize_recent(14)
        summary["technical_exhaustion_evidence"] = load_json(
            DIRS["reports"] / "latest_technical_exhaustion_evidence.json",
            {},
        )
        summary["weekend_rolling_validation"] = load_json(
            DIRS["reports"] / "latest_weekend_rolling_validation.json",
            {},
        )
        candidates = call_gpt_for_candidates(self.creds, summary, self.active_strategy)
        for c in candidates:
            self.save_candidate(c, source="gpt_review")
        self.state["last_gpt_review_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)

    def maybe_train_model(self, dataset: pd.DataFrame) -> None:
        if dataset.empty:
            return
        dataset_hash = str(
            dataset.attrs.get("dataset_hash")
            or dataframe_fingerprint(dataset)
        )
        if dataset_hash == str(self.state.get("last_trained_dataset_hash", "")):
            log_audit({
                "event_type": "model_training",
                "status": "duplicate_skipped",
                "reason": "dataset content unchanged since last model",
                "result": json_dumps({
                    "dataset_hash": dataset_hash,
                    "dataset_path": self.state.get("last_training_dataset_path", ""),
                }),
            })
            return
        res = train_models_from_dataset(dataset, dataset_hash=dataset_hash)
        if res.get("trained"):
            self.state["last_trained_dataset_hash"] = dataset_hash
            self.state["last_model_artifact_path"] = res.get(
                "model_artifact_path",
                "",
            )
            self.state["last_model_training_utc"] = iso_utc()
            save_json(STATE_FILE, self.state)
            update_benchmark({
                "strategy_id": f"model_trade_quality_{utc_now().strftime('%Y%m%d_%H%M%S')}",
                "strategy_name": "Trade quality random forest",
                "strategy_family": "ml_trade_quality",
                "model_type": "RandomForestClassifier",
                "model_artifact_path": res.get("model_artifact_path", ""),
                "model_artifact_hash": sha256_text(Path(res.get("model_artifact_path", "")).read_text(errors="ignore") if False else str(res)),
                "total_trades": res.get("rows", 0),
                "profit_factor": 0,
                "return_pct": 0,
                "score": (safe_float(res.get("auc"), 0.5) or 0.5) * 100,
                "feature_set_json": {"features": res.get("features", [])},
                "evidence_path": res.get("model_artifact_path", ""),
                "notes": f"accuracy={res.get('accuracy')} auc={res.get('auc')}",
            }, source_type="MODEL_TRAINING", instrument="ALL")
            log_audit({"event_type": "model_training", "status": "trained", "strategy_id": "ml_trade_quality", "result": json_dumps(res)[:1000]})
        else:
            log_audit({"event_type": "model_training", "status": "skipped", "reason": res.get("reason")})

    def refresh_weekend_historical_dataset_if_needed(self) -> Optional[Path]:
        existing = latest_historical_training_path()
        existing_end = (
            csv_last_timestamp(existing, "time_utc")
            if existing is not None
            else None
        )
        candle_ends = [
            timestamp
            for path in DIRS["candles"].glob("*_M1.csv")
            if (timestamp := csv_last_timestamp(path, "datetime")) is not None
        ]
        if not candle_ends:
            return existing
        target_end = max(candle_ends) - timedelta(minutes=30)
        if existing_end is not None and existing_end >= target_end - timedelta(
            minutes=HISTORICAL_TRAINING_STEP_MINUTES
        ):
            return existing
        dataset = make_historical_training_rows(
            self.instruments[:MAX_INSTRUMENTS_SCAN],
            self.active_strategy,
            step_minutes=HISTORICAL_TRAINING_STEP_MINUTES,
        )
        if dataset.empty:
            return existing
        path = (
            DIRS["training_sets"]
            / f"historical_training_set_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        dataset.to_csv(path, index=False)
        self.state["last_historical_training_rows"] = len(dataset)
        self.state["last_historical_training_path"] = str(path)
        self.state["last_historical_training_refresh_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)
        log_audit({
            "event_type": "historical_training_refresh",
            "status": "stored",
            "result": json_dumps({
                "path": str(path),
                "rows": len(dataset),
                "prior_end": existing_end,
                "new_end": dataset["time_utc"].max(),
            })[:2000],
        })
        return path

    def run_weekend_rolling_validation_if_needed(self) -> Dict[str, Any]:
        dataset_path = self.refresh_weekend_historical_dataset_if_needed()
        if dataset_path is None:
            result = {
                "validated": False,
                "reason": "no historical training dataset exists",
            }
            log_audit({
                "event_type": "weekend_rolling_validation",
                "status": "skipped",
                "reason": result["reason"],
            })
            return result
        dataset_hash = sha256_file(dataset_path)
        if dataset_hash == str(
            self.state.get("last_weekend_rolling_validation_dataset_hash", "")
        ):
            result = load_json(
                DIRS["reports"] / "latest_weekend_rolling_validation.json",
                {},
            )
            log_audit({
                "event_type": "weekend_rolling_validation",
                "status": "duplicate_skipped",
                "reason": "historical dataset unchanged",
                "result": json_dumps({
                    "dataset": str(dataset_path),
                    "dataset_hash": dataset_hash,
                    "existing_report": str(
                        DIRS["reports"]
                        / "latest_weekend_rolling_validation.json"
                    ),
                }),
            })
            self.state["last_weekend_rolling_validation_utc"] = iso_utc()
            save_json(STATE_FILE, self.state)
            return result
        result = rolling_week_model_validation(dataset_path)
        self.state["last_weekend_rolling_validation_utc"] = iso_utc()
        if result.get("validated"):
            self.state[
                "last_weekend_rolling_validation_dataset_hash"
            ] = dataset_hash
            self.state["latest_weekend_rolling_validation"] = {
                "report_path": str(
                    DIRS["reports"]
                    / "latest_weekend_rolling_validation.json"
                ),
                "dataset": str(dataset_path),
                "dataset_hash": dataset_hash,
                "fold_count": result.get("fold_count", 0),
                "mean_auc": result.get("mean_auc"),
                "minimum_week_auc": result.get("minimum_week_auc"),
                "selected_threshold": result.get("selected_threshold", {}),
                "production_gate": result.get("production_gate", {}),
            }
            log_audit({
                "event_type": "weekend_rolling_validation",
                "status": "completed",
                "result": json_dumps(
                    self.state["latest_weekend_rolling_validation"]
                )[:4000],
            })
        else:
            log_audit({
                "event_type": "weekend_rolling_validation",
                "status": "failed",
                "reason": result.get("reason", "validation failed"),
            })
        save_json(STATE_FILE, self.state)
        return result

    def tick(self) -> None:
        now = utc_now()
        market_closed = market_is_likely_closed(now)
        self.refresh_instruments_if_needed()
        if not self.state.get("last_account_sync_utc") or now - parse_oanda_time(self.state.get("last_account_sync_utc")) >= timedelta(seconds=ACCOUNT_SYNC_SECONDS):
            self.sync_account()
        if not market_closed:
            self.prune_positions()
        signals: List[MarketSignal] = []
        if (
            not market_closed
            and (
                not self.state.get("last_market_scan_utc")
                or now
                - parse_oanda_time(self.state.get("last_market_scan_utc"))
                >= timedelta(seconds=MARKET_SCAN_SECONDS)
            )
        ):
            signals = self.market_scan()
            self.execute_best_signal(signals)
        if (
            not self.state.get("last_technical_evidence_utc")
            or now
            - parse_oanda_time(self.state.get("last_technical_evidence_utc"))
            >= timedelta(seconds=TECHNICAL_EVIDENCE_SECONDS)
        ):
            self.state[
                "latest_technical_evidence"
            ] = ingest_technical_exhaustion_evidence()
            self.state["last_technical_evidence_utc"] = iso_utc()
        # Backfill is light during week and heavier on weekends.
        if not self.state.get("last_historical_backfill_utc") or now - parse_oanda_time(self.state.get("last_historical_backfill_utc")) >= timedelta(seconds=HISTORICAL_BACKFILL_SECONDS):
            if market_closed or ALLOW_WEEKDAY_LIGHT_TESTS:
                # During weekday, only top instruments; weekend all.
                insts = self.instruments if market_closed else self.instruments[:12]
                self.backfill_candles(insts, ["M1"])
        dataset = pd.DataFrame()
        if (
            not market_closed
            and (
                not self.state.get("last_training_dataset_utc")
                or now
                - parse_oanda_time(self.state.get("last_training_dataset_utc"))
                >= timedelta(seconds=TRAINING_DATASET_SECONDS)
            )
        ):
            dataset = self.build_training_dataset(signals)
            self.maybe_train_model(dataset)
        if (
            market_closed
            and ALLOW_WEEKEND_HEAVY_TESTS
            and (
                not self.state.get("last_weekend_rolling_validation_utc")
                or now
                - parse_oanda_time(
                    self.state.get("last_weekend_rolling_validation_utc")
                )
                >= timedelta(seconds=HISTORICAL_BACKFILL_SECONDS)
            )
        ):
            self.run_weekend_rolling_validation_if_needed()
        if (
            not market_closed
            and (
                not self.state.get("last_light_backtest_utc")
                or now
                - parse_oanda_time(self.state.get("last_light_backtest_utc"))
                >= timedelta(seconds=LIGHT_BACKTEST_SECONDS)
            )
        ):
            if ALLOW_WEEKDAY_LIGHT_TESTS:
                self.run_backtests_and_maybe_switch()
        if not self.state.get("last_gpt_review_utc") or now - parse_oanda_time(self.state.get("last_gpt_review_utc")) >= timedelta(seconds=GPT_REVIEW_SECONDS):
            self.maybe_gpt_review()
        if not self.state.get("last_report_utc") or now - parse_oanda_time(self.state.get("last_report_utc")) >= timedelta(seconds=FULL_REPORT_SECONDS):
            write_big_picture_summary(self.active_strategy, self.state)
            self.state["last_report_utc"] = iso_utc()
        if not self.state.get("last_export_utc") or now - parse_oanda_time(self.state.get("last_export_utc")) >= timedelta(seconds=EXPORT_SECONDS):
            write_big_picture_summary(self.active_strategy, self.state)
            create_export_zip()
            self.state["last_export_utc"] = iso_utc()
        save_json(STATE_FILE, self.state)

    def run(self) -> int:
        print(f"[{iso_utc()}] Starting {SCRIPT_NAME} {SCRIPT_VERSION}")
        print(f"Project root:   {PROJECT_ROOT}")
        print(f"Training root:  {TRAINING_ROOT}")
        print(f"Benchmark root: {BENCHMARK_ROOT}")
        print(f"Creds file:     {CREDS_PATH} present={CREDS_PATH.exists()}")
        print(f"OANDA env:      {self.env}")
        print(f"DUM1 account:   {mask_account(self.account_id)}")
        print(f"Active strategy:{self.active_strategy.strategy_id}")
        print(f"Review zip:     {LATEST_EXPORT_ZIP}")
        log_audit({"account_id": self.account_id, "account_role": ACCOUNT_ROLE, "event_type": "manager_start", "status": "running", "strategy_id": self.active_strategy.strategy_id})
        self.research_engine.start()
        # First startup reconcile/backfill.
        try:
            self.sync_account()
            self.backfill_candles(self.instruments[:12], ["M1"])
            write_big_picture_summary(self.active_strategy, self.state)
            create_export_zip()
        except Exception as exc:
            log_error("startup", exc)
        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                print(f"[{iso_utc()}] stop signal received; exiting without closing trades")
                self.research_engine.stop()
                log_audit({"account_id": self.account_id, "event_type": "manager_stop", "status": "keyboard_interrupt"})
                return 0
            except Exception as exc:
                log_error("main_loop", exc)
            time.sleep(MAIN_LOOP_SECONDS)


class ResearchOnlyManager:
    """Default always-on process: model research, no broker execution."""

    def __init__(self) -> None:
        ensure_dirs()
        self.creds = load_creds(CREDS_PATH)
        self.state_path = DIRS["state"] / "research_only_state.json"
        self.state = load_json(self.state_path, {})
        self.state.setdefault("started_utc", iso_utc())
        self.state.setdefault("last_gpt_review_utc", "")
        self.state.setdefault("last_report_utc", "")
        self.state.setdefault("last_technical_evidence_utc", "")
        self.state.setdefault("last_missed_spike_backtest_utc", "")
        self.state.setdefault("last_pre_spike_lead_backtest_utc", "")
        self.state.setdefault("last_ensemble_shadow_refresh_utc", "")
        self.state.setdefault("last_reporting_extensions_utc", "")
        self.state["mode"] = "research_only_no_broker_execution"
        self.active_strategy = self.load_or_create_active_strategy()
        self.research_engine = ContinuousResearchEngine()
        save_json(self.state_path, self.state)

    def load_or_create_active_strategy(self) -> StrategyProfile:
        if ACTIVE_STRATEGY_FILE.exists():
            try:
                return StrategyProfile.from_json(load_json(ACTIVE_STRATEGY_FILE, {}))
            except Exception:
                pass
        prof = default_active_strategy()
        save_json(ACTIVE_STRATEGY_FILE, prof.to_json())
        return prof

    def save_candidate(self, profile: StrategyProfile, source: str = "unknown") -> None:
        path = DIRS["candidates"] / f"{profile.strategy_id}.json"
        save_json(path, {**profile.to_json(), "candidate_source": source, "created_utc": iso_utc(), "live_execution_allowed": False})
        append_jsonl(LOG_FILES["candidate_queue"], {"time_utc": iso_utc(), "source": source, "strategy": profile.to_json(), "live_execution_allowed": False})

    def maybe_gpt_review(self) -> None:
        if env_flag("OANDA_SKIP_GPT_REVIEW"):
            log_audit({
                "event_type": "gpt_review",
                "status": "skipped",
                "reason": "OANDA_SKIP_GPT_REVIEW",
                "result": "network-backed GPT review disabled for this run",
            })
            self.state["last_gpt_review_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            return
        summary = summarize_recent(14)
        summary["technical_exhaustion_evidence"] = load_json(
            DIRS["reports"] / "latest_technical_exhaustion_evidence.json",
            {},
        )
        summary["weekend_rolling_validation"] = load_json(
            DIRS["reports"] / "latest_weekend_rolling_validation.json",
            {},
        )
        summary["lifecycle_registry"] = load_json(
            DIRS["registry"] / "index.json",
            {},
        )
        candidates = call_gpt_for_candidates(
            self.creds,
            summary,
            self.active_strategy,
        )
        for c in candidates:
            self.save_candidate(c, source="gpt_research_only")
        self.state["last_gpt_review_utc"] = iso_utc()
        save_json(self.state_path, self.state)

    def refresh_missed_spike_backtest_if_needed(self, now: datetime) -> None:
        last = self.state.get("last_missed_spike_backtest_utc")
        if (
            last
            and now - parse_oanda_time(last)
            < timedelta(seconds=MISSED_SPIKE_BACKTEST_SECONDS)
        ):
            return
        script = PROJECT_ROOT / "oanda_spike_missed_move_backtest.py"
        payload: Dict[str, Any] = {
            "time_utc": iso_utc(),
            "script": str(script),
            "status": "missing_script",
            "execution": "read_only_report",
        }
        if WEEKEND_NON_SCOUT_FOCUS:
            payload = {
                **payload,
                "status": "skipped",
                "execution": "weekend_non_scout_focus",
                "reason": (
                    "OANDA_WEEKEND_NON_SCOUT_FOCUS=1; skip scout-focused "
                    "missed-spike refresh after repeated timeout evidence"
                ),
            }
            self.state["latest_missed_spike_backtest"] = payload
            self.state["last_missed_spike_backtest_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            log_audit({
                "event_type": "missed_spike_backtest",
                "status": "skipped",
                "reason": payload["reason"],
                "result": json_dumps(payload)[:1000],
            })
            return
        if not script.exists():
            self.state["latest_missed_spike_backtest"] = payload
            self.state["last_missed_spike_backtest_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            log_audit({
                "event_type": "missed_spike_backtest",
                "status": "skipped",
                "reason": "script missing",
                "result": json_dumps(payload)[:1000],
            })
            return
        command = project_python_command(
            script,
            "--fetch-recent",
            "--estimate-usd",
            "--max-events",
            "0",
        )
        try:
            completed = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=240,
            )
            summary: Dict[str, Any] = {}
            stdout = (completed.stdout or "").strip()
            if stdout:
                try:
                    summary = json.loads(stdout)
                except Exception:
                    summary = {"stdout_tail": stdout[-1000:]}
            payload = {
                **payload,
                "status": "completed" if completed.returncode == 0 else "error",
                "returncode": completed.returncode,
                "summary": summary,
                "stderr_tail": (completed.stderr or "")[-1000:],
            }
            log_audit({
                "event_type": "missed_spike_backtest",
                "status": payload["status"],
                "reason": "volatile missed-move focus report refreshed",
                "result": json_dumps(payload)[:2000],
            })
        except Exception as exc:
            payload = {
                **payload,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            }
            log_error("missed_spike_backtest_refresh", exc)
        self.state["latest_missed_spike_backtest"] = payload
        self.state["last_missed_spike_backtest_utc"] = iso_utc()
        save_json(self.state_path, self.state)

    def refresh_pre_spike_lead_backtest_if_needed(self, now: datetime) -> None:
        last = self.state.get("last_pre_spike_lead_backtest_utc")
        if not should_refresh_sidecar(
            last,
            self.state.get("latest_pre_spike_lead_backtest"),
            now,
            PRE_SPIKE_LEAD_BACKTEST_SECONDS,
        ):
            return
        script = PROJECT_ROOT / "oanda_pre_spike_lead_backtest.py"
        payload: Dict[str, Any] = {
            "time_utc": iso_utc(),
            "script": str(script),
            "status": "missing_script",
            "execution": "read_only_report",
        }
        if WEEKEND_NON_SCOUT_FOCUS:
            payload = {
                **payload,
                "status": "skipped",
                "execution": "weekend_non_scout_focus",
                "reason": (
                    "OANDA_WEEKEND_NON_SCOUT_FOCUS=1; skip scout-focused "
                    "pre-spike lead refresh after timeout evidence"
                ),
            }
            self.state["latest_pre_spike_lead_backtest"] = payload
            self.state["last_pre_spike_lead_backtest_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            log_audit({
                "event_type": "pre_spike_lead_backtest",
                "status": "skipped",
                "reason": payload["reason"],
                "result": json_dumps(payload)[:1000],
            })
            return
        if not script.exists():
            self.state["latest_pre_spike_lead_backtest"] = payload
            self.state["last_pre_spike_lead_backtest_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            log_audit({
                "event_type": "pre_spike_lead_backtest",
                "status": "skipped",
                "reason": "script missing",
                "result": json_dumps(payload)[:1000],
            })
            return
        report_path = DIRS["reports"] / "latest_pre_spike_lead_trainer_focus.json"
        folds_path = DIRS["reports"] / "latest_pre_spike_lead_trainer_focus_folds.csv"
        running_count = running_python_process_count([
            "oanda_pre_spike_lead_backtest.py",
            report_path.name,
        ])
        if running_count > 0:
            payload = {
                **payload,
                "status": "already_running",
                "running_process_count": running_count,
                "report": str(report_path),
                "folds_csv": str(folds_path),
                "reason": "pre-spike lead report already running; singleton guard skipped duplicate launch",
            }
            self.state["latest_pre_spike_lead_backtest"] = payload
            self.state["last_pre_spike_lead_backtest_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            log_audit({
                "event_type": "pre_spike_lead_backtest",
                "status": "already_running",
                "reason": payload["reason"],
                "result": json_dumps(payload)[:1000],
            })
            return
        command = project_python_command(
            script,
            "--report",
            str(report_path),
            "--folds-csv",
            str(folds_path),
            "--instrument-subset",
            "volatile_exotic",
            "--feature-set",
            "core",
            "--rule-profiles",
            "exhaustion_reversal",
            "--models",
            "random_forest",
            "extra_trees",
            "hist_gradient_boosting",
            "gradient_boosting",
            "logistic",
            "--modes",
            "directional",
            "two_stage",
            "--leads",
            "30",
            "--horizons",
            "60",
            "--max-test-weeks",
            "6",
            "--max-train-rows",
            "100000",
            "--max-rows",
            "400000",
            "--max-combos",
            "10",
            "--lead-stop-atr",
            "0.75",
            "--lead-stop-atrs",
            "0.75",
            "1.0",
            "--cluster-gap-minutes",
            "60",
        )
        try:
            completed = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=900,
            )
            summary: Dict[str, Any] = {}
            stdout = (completed.stdout or "").strip()
            if stdout:
                try:
                    summary = json.loads(stdout)
                except Exception:
                    summary = {"stdout_tail": stdout[-1000:]}
            payload = {
                **payload,
                "status": "completed" if completed.returncode == 0 else "error",
                "returncode": completed.returncode,
                "summary": summary,
                "report": str(report_path),
                "folds_csv": str(folds_path),
                "stderr_tail": (completed.stderr or "")[-1000:],
            }
            log_audit({
                "event_type": "pre_spike_lead_backtest",
                "status": payload["status"],
                "reason": "lead-window volatile spike prediction report refreshed",
                "result": json_dumps(payload)[:2000],
            })
        except Exception as exc:
            payload = {
                **payload,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            }
            log_error("pre_spike_lead_backtest_refresh", exc)
        self.state["latest_pre_spike_lead_backtest"] = payload
        self.state["last_pre_spike_lead_backtest_utc"] = iso_utc()
        save_json(self.state_path, self.state)

    def refresh_ensemble_shadow_if_needed(self, now: datetime) -> None:
        last = self.state.get("last_ensemble_shadow_refresh_utc")
        if not should_refresh_sidecar(
            last,
            self.state.get("latest_ensemble_shadow_refresh"),
            now,
            ENSEMBLE_SHADOW_REFRESH_SECONDS,
        ):
            return
        script = PROJECT_ROOT / "oanda_ensemble_candidate_backtester.py"
        payload: Dict[str, Any] = {
            "time_utc": iso_utc(),
            "script": str(script),
            "status": "missing_script",
            "execution": "shadow_research_only",
        }
        if WEEKEND_NON_SCOUT_FOCUS:
            existing_manifest = DIRS["promotions"] / "ensemble_shadow_candidate.json"
            if existing_manifest.exists():
                try:
                    manifest_mtime = datetime.fromtimestamp(
                        existing_manifest.stat().st_mtime,
                        tz=timezone.utc,
                    )
                    manifest_age = now - manifest_mtime
                except Exception:
                    manifest_age = timedelta.max
                if manifest_age < timedelta(hours=24):
                    payload = {
                        **payload,
                        "status": "skipped",
                        "execution": "weekend_non_scout_focus_existing_ensemble_shadow",
                        "existing_manifest": str(existing_manifest),
                        "existing_manifest_age_hours": round(
                            manifest_age.total_seconds() / 3600.0,
                            3,
                        ),
                        "reason": (
                            "OANDA_WEEKEND_NON_SCOUT_FOCUS=1; reuse fresh "
                            "ensemble shadow candidate instead of rerunning "
                            "timeout-prone full ensemble refresh"
                        ),
                    }
                    self.state["latest_ensemble_shadow_refresh"] = payload
                    self.state["last_ensemble_shadow_refresh_utc"] = iso_utc()
                    save_json(self.state_path, self.state)
                    log_audit({
                        "event_type": "ensemble_shadow_refresh",
                        "status": "skipped",
                        "reason": payload["reason"],
                        "result": json_dumps(payload)[:1000],
                    })
                    return
        if not script.exists():
            self.state["latest_ensemble_shadow_refresh"] = payload
            self.state["last_ensemble_shadow_refresh_utc"] = iso_utc()
            save_json(self.state_path, self.state)
            log_audit({
                "event_type": "ensemble_shadow_refresh",
                "status": "skipped",
                "reason": "script missing",
                "result": json_dumps(payload)[:1000],
            })
            return
        command = project_python_command(
            script,
            "--opportunity-mode",
            "--write-final-bundle",
            "--max-members",
            "6",
            "--min-members",
            "2",
            "--holdout-weeks",
            "6",
            "--max-train-rows",
            "120000",
            "--outcome-horizon",
            "120",
            "--min-calibration-trades",
            "15",
        )
        try:
            completed = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=900,
            )
            summary: Dict[str, Any] = {}
            stdout = (completed.stdout or "").strip()
            if stdout:
                try:
                    summary = json.loads(stdout)
                except Exception:
                    summary = {"stdout_tail": stdout[-1000:]}
            payload = {
                **payload,
                "status": "completed" if completed.returncode == 0 else "error",
                "returncode": completed.returncode,
                "summary": summary,
                "stderr_tail": (completed.stderr or "")[-1000:],
            }
            log_audit({
                "event_type": "ensemble_shadow_refresh",
                "status": payload["status"],
                "reason": "multi-model shadow ensemble evidence refreshed",
                "result": json_dumps(payload)[:2000],
            })
        except subprocess.TimeoutExpired as exc:
            payload = {
                **payload,
                "status": "timeout_skipped",
                "timeout_seconds": 900,
                "command": command,
                "error": f"{type(exc).__name__}: {exc}",
                "reason": (
                    "ensemble refresh exceeded lighter runtime budget; "
                    "keeping existing shadow ensemble artifacts and continuing trainer"
                ),
            }
            log_audit({
                "event_type": "ensemble_shadow_refresh",
                "status": "timeout_skipped",
                "reason": payload["reason"],
                "result": json_dumps(payload)[:2000],
            })
        except Exception as exc:
            payload = {
                **payload,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            }
            log_error("ensemble_shadow_refresh", exc)
        self.state["latest_ensemble_shadow_refresh"] = payload
        self.state["last_ensemble_shadow_refresh_utc"] = iso_utc()
        save_json(self.state_path, self.state)

    def refresh_reporting_extensions_if_needed(self, now: datetime) -> None:
        last = self.state.get("last_reporting_extensions_utc")
        if (
            last
            and now - parse_oanda_time(last)
            < timedelta(seconds=REPORTING_EXTENSIONS_SECONDS)
        ):
            return
        payload: Dict[str, Any] = {
            "time_utc": iso_utc(),
            "status": "started",
            "execution": "reporting_only_no_broker_or_promotion_changes",
        }
        try:
            from oanda_trainer_reporting_extensions import write_reporting_extensions

            report = write_reporting_extensions(PROJECT_ROOT)
            payload = {
                **payload,
                "status": "completed",
                "generated_utc": report.get("generated_utc"),
                "outputs": report.get("outputs", {}),
                "trainer_status": report.get("trainer_status", {}),
                "live_shadow": {
                    "latest_scan_time_utc": report.get("live_shadow", {}).get(
                        "latest_scan_time_utc", ""
                    ),
                    "latest_scan_top_candidate": report.get("live_shadow", {}).get(
                        "latest_scan_top_candidate", ""
                    ),
                    "recent_scan_totals": report.get("live_shadow", {}).get(
                        "recent_scan_totals", {}
                    ),
                },
                "portfolio_margin_proxy": {
                    "latest_margin_used_pct": report.get(
                        "portfolio_margin_proxy", {}
                    ).get("latest_margin_used_pct", ""),
                    "max_margin_used_pct_recent": report.get(
                        "portfolio_margin_proxy", {}
                    ).get("max_margin_used_pct_recent", ""),
                    "latest_open_trade_count": report.get(
                        "portfolio_margin_proxy", {}
                    ).get("latest_open_trade_count", ""),
                },
            }
            log_audit(
                {
                    "event_type": "trainer_reporting_extensions",
                    "status": "completed",
                    "reason": "reporting-only trainer diagnostics refreshed",
                    "result": json_dumps(payload)[:2000],
                }
            )
        except Exception as exc:
            payload = {
                **payload,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            }
            log_error("trainer_reporting_extensions", exc)
        self.state["latest_reporting_extensions"] = payload
        self.state["last_reporting_extensions_utc"] = iso_utc()
        save_json(self.state_path, self.state)

    def tick(self) -> None:
        now = utc_now()
        if (
            not self.state.get("last_technical_evidence_utc")
            or now
            - parse_oanda_time(self.state.get("last_technical_evidence_utc"))
            >= timedelta(seconds=TECHNICAL_EVIDENCE_SECONDS)
        ):
            self.state[
                "latest_technical_evidence"
            ] = ingest_technical_exhaustion_evidence()
            self.state["last_technical_evidence_utc"] = iso_utc()
        self.refresh_missed_spike_backtest_if_needed(now)
        self.refresh_pre_spike_lead_backtest_if_needed(now)
        self.refresh_ensemble_shadow_if_needed(now)
        self.refresh_reporting_extensions_if_needed(now)
        if (
            not self.state.get("last_gpt_review_utc")
            or now - parse_oanda_time(self.state.get("last_gpt_review_utc"))
            >= timedelta(seconds=GPT_REVIEW_SECONDS)
        ):
            self.maybe_gpt_review()
        if (
            not self.state.get("last_report_utc")
            or now - parse_oanda_time(self.state.get("last_report_utc"))
            >= timedelta(seconds=FULL_REPORT_SECONDS)
        ):
            state_for_report = {
                **self.state,
                "latest_production_gate": current_production_gate(),
                "lifecycle_registry": load_json(
                    DIRS["registry"] / "index.json",
                    {},
                ),
            }
            write_big_picture_summary(self.active_strategy, state_for_report)
            self.state["last_report_utc"] = iso_utc()
        self.state["last_tick_utc"] = iso_utc()
        self.state["execution_enabled"] = False
        save_json(self.state_path, self.state)

    def run(self) -> int:
        print(f"[{iso_utc()}] Starting {SCRIPT_NAME} {SCRIPT_VERSION}")
        print("Mode:           research-only; broker execution disabled")
        print(f"Project root:   {PROJECT_ROOT}")
        print(f"Training root:  {TRAINING_ROOT}")
        print(f"Review root:    {DIRS['registry']}")
        print(f"Weekend focus:  non-scout={WEEKEND_NON_SCOUT_FOCUS}")
        log_audit({
            "account_role": ACCOUNT_ROLE,
            "event_type": "manager_start",
            "status": "running",
            "reason": "research-only mode; no DUM or technical execution",
            "result": json_dumps({
                "weekend_non_scout_focus": WEEKEND_NON_SCOUT_FOCUS,
            }),
        })
        self.research_engine.start()
        while True:
            try:
                self.tick()
            except KeyboardInterrupt:
                print(f"[{iso_utc()}] stop signal received")
                self.research_engine.stop()
                log_audit({
                    "event_type": "manager_stop",
                    "status": "keyboard_interrupt",
                    "reason": "research-only mode",
                })
                return 0
            except Exception as exc:
                log_error("research_only_main_loop", exc)
            time.sleep(MAIN_LOOP_SECONDS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OANDA DUM1 training and strategy manager")
    parser.add_argument(
        "--legacy-dum-live",
        action="store_true",
        help="Run the old DUM account trading loop. Default is research-only.",
    )
    parser.add_argument(
        "--expand-training-data",
        action="store_true",
        help="Backfill paginated M1 history, build historical labels, train one model, then exit.",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=DEEP_BACKFILL_DEFAULT_DAYS,
        help=f"Calendar days of M1 history to retain during expansion (default: {DEEP_BACKFILL_DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--training-step-minutes",
        type=int,
        default=HISTORICAL_TRAINING_STEP_MINUTES,
        help=f"Minutes between historical training samples (default: {HISTORICAL_TRAINING_STEP_MINUTES}).",
    )
    parser.add_argument(
        "--instrument-limit",
        type=int,
        default=MAX_INSTRUMENTS_SCAN,
        help=f"Maximum number of configured instruments to expand (default: {MAX_INSTRUMENTS_SCAN}).",
    )
    return parser.parse_args()


def main() -> int:
    ensure_dirs()
    enforce_current_promotion_protocol()
    rotated_audit = rotate_invalid_audit_log()
    rotated_research = rotate_legacy_research_ledger()
    args = parse_args()
    if rotated_audit:
        log_audit({
            "event_type": "audit_log_repair",
            "status": "rotated",
            "reason": "prior audit rows were missing timestamps",
            "result": str(rotated_audit),
        })
    if rotated_research:
        log_audit({
            "event_type": "research_ledger_schema_migration",
            "status": "rotated",
            "reason": "research experiment ledger schema now includes lifecycle fields",
            "result": str(rotated_research),
        })
    if args.expand_training_data:
        mgr = TrainingStrategyManager()
        instruments = mgr.instruments[:max(1, min(args.instrument_limit, MAX_INSTRUMENTS_SCAN))]
        summary = mgr.deep_backfill_m1(days=args.history_days, instruments=instruments)
        dataset = make_historical_training_rows(
            instruments,
            mgr.active_strategy,
            step_minutes=args.training_step_minutes,
        )
        dataset_path = DIRS["training_sets"] / f"historical_training_set_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
        if not dataset.empty:
            dataset.to_csv(dataset_path, index=False)
            mgr.state["last_training_dataset_path"] = str(dataset_path)
            mgr.state["last_training_dataset_utc"] = iso_utc()
            mgr.state["last_historical_training_rows"] = len(dataset)
            save_json(STATE_FILE, mgr.state)
        model_result = train_models_from_dataset(dataset)
        if model_result.get("trained"):
            log_audit({
                "event_type": "model_training",
                "status": "trained",
                "strategy_id": "ml_trade_quality",
                "result": json_dumps(model_result)[:1000],
            })
        else:
            log_audit({
                "event_type": "model_training",
                "status": "skipped",
                "reason": model_result.get("reason"),
            })
        result = {
            "backfill": summary,
            "training_rows": len(dataset),
            "training_dataset": str(dataset_path) if not dataset.empty else "",
            "model": model_result,
        }
        save_json(DIRS["reports"] / "latest_training_data_expansion.json", result)
        print(json.dumps({
            "total_rows_added": summary.get("total_rows_added", 0),
            "training_rows": len(dataset),
            "training_dataset": str(dataset_path) if not dataset.empty else "",
            "model": model_result,
        }, indent=2, default=str), flush=True)
        return 0
    if args.legacy_dum_live:
        return TrainingStrategyManager().run()
    return ResearchOnlyManager().run()


if __name__ == "__main__":
    raise SystemExit(main())
