#!/usr/bin/env python3
"""Primary-account forecast rotation bot.

This bot turns the all-68 indicator-alignment research output into a constantly
ranked opportunity book.  It is routed to the primary live account credentials,
but defaults to advice/paper behavior and requires explicit live flags before it
can write broker orders.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import math
import os
import pickle
import re
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import requests
except Exception:  # pragma: no cover - handled at runtime for online modes.
    requests = None  # type: ignore[assignment]

from oanda_indicator_alignment_research import (
    add_base_features,
    add_cross_currency_features,
    event_defs,
    instrument_parts,
    list_candle_instruments,
    pips_size,
    read_candles,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CREDS_PATH = Path(os.environ.get("OANDA_CREDS_PATH", str(PROJECT_ROOT / "creds")))
CONFIG_PATH = PROJECT_ROOT / "config" / "primary_forecast_rotation_bot.json"
TRAINING_ROOT = PROJECT_ROOT / "data" / "oanda_training_manager"
ALIGNMENT_BEST_CSV = (
    TRAINING_ROOT
    / "reports"
    / "indicator_alignment"
    / "latest_all68_best_indicators_per_instrument.csv"
)
HGB_SCORE_012_CANDIDATES_CSV = (
    TRAINING_ROOT
    / "reports"
    / "live_like_matrix_hgb_score_ge_012"
    / "candidate_rows.csv"
)
HGB_FULL_EXPERIMENT_JSON = (
    TRAINING_ROOT
    / "continuous_research"
    / "experiments"
    / "exp_20260702_022306_e09b53ec8d.json"
)
DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "technical_scout_manager"
    / "account_live_primary_forecast_rotation"
)

SCRIPT_VERSION = "primary_forecast_rotation_v0_1"
LIVE_CONFIRM_TEXT = "PRIMARY_FORECAST_ROTATION_LIVE"
BROKER_MODES = {"demo", "live"}

DEFAULT_CONFIG: Dict[str, Any] = {
    "schema_version": 1,
    "bot_id": "primary_forecast_rotation",
    "account_role": "primary_live_challenger",
    "account_aliases": ["OANDA_ACCOUNT_LIVE_PRIMARY", "OANDA_ACCOUNT_ID_PRIMARY_LIVE"],
    "api_key_aliases": ["OANDA_LIVE_API_KEY", "OANDA_API_KEY_LIVE", "OANDA_LIVE_API_TOKEN"],
    "default_mode": "advice",
    "default_data_source": "local",
    "demo_execution_enabled": False,
    "demo_new_entries_enabled": True,
    "demo_require_empty_account_on_first_start": True,
    "live_execution_enabled": False,
    "live_new_entries_enabled": True,
    "live_confirmation_text": LIVE_CONFIRM_TEXT,
    "practice_api_key_aliases": ["OANDA_API_KEY", "OANDA_API_TOKEN"],
    "practice_account_aliases": ["OANDA_ACCOUNT_ID_GPT", "OANDA_ACCOUNT_ID_MAJ"],
    "practice_account_suffix": "",
    "oanda_request_timeout_seconds": 20.0,
    "oanda_request_retries": 3,
    "oanda_request_backoff_seconds": 1.25,
    "loop_sleep_seconds": 60,
    "oanda_candle_count": 650,
    "local_candle_count": 1200,
    "broker_close_transaction_lookback_ids": 250,
    "horizons": [15, 30, 60, 120],
    "forecast_source": "alignment",
    "alignment_best_csv": str(ALIGNMENT_BEST_CSV),
    "model_stream": {
        "name": "live_like_matrix_hgb_score_ge_012",
        "enabled": False,
        "candidate_rows_csv": str(HGB_SCORE_012_CANDIDATES_CSV),
        "min_raw_rank_score": 0.12,
        "rank_score_scale": 0.10,
        "train_row_cap": 30000,
        "train_lookback_days": 90,
        "model_cache_dir": str(DATA_DIR / "model_cache"),
        "m1_overlay": {
            "enabled": False,
            "model_path": "",
            "hold_threshold": 0.45,
            "quick_threshold": 0.0,
            "feature_timeframe": "1min",
            "oanda_candle_granularity": "M1",
            "oanda_candle_count": 650,
            "local_candle_count": 1200,
            "scope": "all_loaded",
            "fail_closed": True,
        },
        "models": [
            {
                "name": "hgb_core_reversal_120_score_ge_012",
                "experiment_id": "synthetic_latest_hgb_core_all68",
                "spec_json": str(HGB_FULL_EXPERIMENT_JSON),
                "feature_set": "technical_core",
            },
            {
                "name": "hgb_full_reversal_120_score_ge_012",
                "experiment_id": "exp_20260702_022306_e09b53ec8d",
                "spec_json": str(HGB_FULL_EXPERIMENT_JSON),
                "feature_set": "technical_full",
            },
        ],
    },
    "data_dir": str(DATA_DIR),
    "min_instruments_required": 0,
    "top_alignment_rows_per_pair": 50,
    "max_active_events_per_pair": 8,
    "min_sample_count": 75,
    "min_week_count": 3,
    "probability_prior": 0.50,
    "probability_prior_weight": 250.0,
    "min_probability": 0.515,
    "min_positive_week_rate": 0.45,
    "min_profit_factor": 1.05,
    "min_edge_pips": 0.25,
    "min_rank_score": 0.012,
    "spread_cost_multiplier": 1.25,
    "slippage_pips_default": 0.20,
    "slippage_pips_exotic": 2.00,
    "max_spread_pips_major": 4.0,
    "max_spread_pips_cross": 8.0,
    "max_spread_pips_exotic": 180.0,
    "max_spread_to_edge_ratio": 0.55,
    "atr_stop_multiplier": 1.15,
    "mae_stop_multiplier": 1.20,
    "min_stop_spread_multiple": 4.0,
    "take_profit_edge_capture": 0.85,
    "take_profit_min_r_multiple": 1.10,
    "trailing_stop_r_multiple": 0.85,
    "max_open_positions": 6,
    "use_position_count_cap": True,
    "target_active_positions": 3,
    "max_new_positions_per_cycle": 2,
    "max_replacements_per_hour": 2,
    "replacement_on_pressure": False,
    "replacement_min_score_improvement": 0.003,
    "replacement_min_score_multiplier": 1.35,
    "opposite_min_score_multiplier": 1.20,
    "hold_score_floor_fraction": 0.45,
    "hold_same_signal_past_horizon": False,
    "stale_horizon_fraction": 0.80,
    "min_hold_minutes": 8,
    "replacement_min_age_minutes": 8,
    "defer_weak_exits_when_under_target": False,
    "pair_cooldown_minutes_after_loss": 30,
    "pair_cooldown_minutes_after_rotation": 10,
    "pair_cooldown_minutes_after_broker_side_close": 2,
    "pair_cooldown_minutes_after_broker_error": 15,
    "max_same_pair_positions": 1,
    "max_same_direction_currency_exposure": 4,
    "min_expected_profit_usd": 0.01,
    "min_take_profit_usd": 0.01,
    "min_profit_per_margin": 0.001,
    "size_to_min_profit_usd": True,
    "max_trade_risk_pct": 3.0,
    "max_trade_risk_remaining_day_loss_fraction": 0.50,
    "above_target_min_rank_score": 0.018,
    "target_open_risk_pct": 4.8,
    "max_open_risk_pct": 6.5,
    "max_currency_risk_pct": 3.0,
    "risk_pct_min": 0.25,
    "risk_pct_max": 1.75,
    "risk_pct_score_full": 0.020,
    "drawdown_risk_throttle_start_pct": 10.0,
    "drawdown_risk_throttle_full_pct": 22.0,
    "drawdown_risk_min_multiplier": 0.30,
    "drawdown_open_risk_min_multiplier": 0.55,
    "drawdown_entry_halt_pct": 0.0,
    "drawdown_halt_bypass_rank_score": 0.018,
    "max_pair_daily_loss_pct": 1.8,
    "max_pair_total_loss_pct": 6.5,
    "pair_loss_streak_cooldown_trades": 3,
    "pair_loss_cooldown_minutes": 300,
    "pair_large_loss_cooldown_pct": 1.2,
    "unstopped_position_risk_nav_pct": 3.0,
    "max_position_margin_used_pct": 0.0,
    "target_margin_used_pct": 55.0,
    "max_margin_used_pct": 72.0,
    "emergency_margin_used_pct": 86.0,
    "broker_margin_buffer_pct": 0.0,
    "broker_close_replacement_before_open": False,
    "broker_precheck_protective_orders": False,
    "broker_precheck_min_distance_pips": 0.1,
    "broker_precheck_min_spread_multiple": 0.25,
    "model_feature_age_new_entry_block": True,
    "max_model_feature_age_minutes": 480.0,
    "refresh_live_state_after_actions": False,
    "default_margin_rate": 0.0333,
    "use_broker_margin_rates": False,
    "paper_starting_equity": 10000.0,
    "disable_kill_switches": False,
    "close_positions_on_kill_switch": False,
    "daily_loss_kill_pct": 6.0,
    "drawdown_kill_pct": 12.0,
    "weekend_new_entry_block": True,
    "rollover_new_entry_block": True,
    "rollover_start_hour_utc": 21.0,
    "rollover_end_hour_utc": 21.5,
    "journal_top_candidates": 50,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        pass
    return default


def append_csv(path: Path, row: Dict[str, Any], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    fields = list(fieldnames)
    rows_to_rewrite: List[Dict[str, Any]] = []
    if exists:
        try:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                current_fields = list(reader.fieldnames or [])
                if current_fields and current_fields != fields:
                    for name in current_fields:
                        if name not in fields:
                            fields.append(name)
                    rows_to_rewrite = list(reader)
                    exists = False
        except Exception:
            rows_to_rewrite = []
    mode = "a" if exists else "w"
    with path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        if not exists:
            writer.writeheader()
            for old_row in rows_to_rewrite:
                writer.writerow(old_row)
        writer.writerow(row)


def load_creds(path: Path = CREDS_PATH) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    if path.exists():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            name, value = stripped.split("=", 1)
            name = name.strip()
            value = value.strip()
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
                continue
            try:
                data[name] = ast.literal_eval(value)
            except Exception:
                data[name] = value.strip("\"'")
    for key, value in os.environ.items():
        if key.startswith("OANDA_") or key.startswith("FOREX_"):
            data.setdefault(key, value)
    return data


def first_setting(settings: Dict[str, Any], names: Sequence[str], default: str = "") -> str:
    for name in names:
        value = settings.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def setting_bool(settings: Dict[str, Any], name: str, default: bool = False) -> bool:
    value = settings.get(name)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config(path: Path = CONFIG_PATH) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    if path.exists():
        loaded = read_json(path, {})
        if isinstance(loaded, dict):
            cfg.update(loaded)
    return cfg


def resolve_project_path(value: Any, default: Path) -> Path:
    text = str(value or "").strip()
    path = Path(text) if text else default
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def write_default_config(path: Path = CONFIG_PATH) -> None:
    if not path.exists():
        atomic_write_json(path, DEFAULT_CONFIG)


def config_list_or_fallback(cfg: Dict[str, Any], key: str, fallback_key: str) -> list[str]:
    if key in cfg:
        value = cfg.get(key)
    else:
        value = cfg.get(fallback_key)
    return [str(x) for x in (value or [])]


def discover_practice_account_id_by_suffix(token: str, suffix: str, *, timeout: float = 20.0) -> str:
    suffix_digits = re.sub(r"\D", "", str(suffix or ""))
    if not suffix_digits:
        return ""
    suffix_digits = suffix_digits[-3:].zfill(3)
    request = urllib.request.Request(
        "https://api-fxpractice.oanda.com/v3/accounts",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=max(1.0, timeout)) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"Practice account discovery failed: HTTP {exc.code} {body}") from exc
    except Exception as exc:
        raise RuntimeError(f"Practice account discovery failed: {exc}") from exc
    matches = [
        str(account.get("id") or "")
        for account in payload.get("accounts", [])
        if str(account.get("id") or "").endswith(f"-{suffix_digits}")
    ]
    matches = [account_id for account_id in matches if account_id]
    if not matches:
        raise RuntimeError(f"No OANDA practice account ending -{suffix_digits} is visible to this token.")
    if len(matches) > 1:
        raise RuntimeError(f"Multiple OANDA practice accounts ending -{suffix_digits} are visible; use an explicit alias.")
    return matches[0]


def resolve_oanda(cfg: Dict[str, Any], creds: Dict[str, Any], mode: str) -> Dict[str, str]:
    if mode == "demo":
        api_aliases = config_list_or_fallback(cfg, "practice_api_key_aliases", "api_key_aliases")
        account_aliases = config_list_or_fallback(cfg, "practice_account_aliases", "account_aliases")
        token = first_setting(creds, api_aliases)
        account_id = first_setting(creds, account_aliases)
        if not token:
            raise RuntimeError("Missing OANDA practice API key in creds.")
        if not account_id:
            suffix = str(cfg.get("practice_account_suffix") or "").strip()
            account_id = discover_practice_account_id_by_suffix(
                token,
                suffix,
                timeout=safe_float(cfg.get("oanda_request_timeout_seconds"), 20.0),
            )
        if not account_id:
            raise RuntimeError("Missing OANDA practice account id in creds.")
        return {
            "token": token,
            "account_id": account_id,
            "base_url": "https://api-fxpractice.oanda.com",
            "env": "practice",
        }

    token = first_setting(creds, [str(x) for x in cfg.get("api_key_aliases", [])])
    account_id = first_setting(creds, [str(x) for x in cfg.get("account_aliases", [])])
    if not token:
        raise RuntimeError("Missing primary OANDA live API key in creds.")
    if not account_id:
        raise RuntimeError("Missing primary OANDA account id in creds.")
    return {
        "token": token,
        "account_id": account_id,
        "base_url": "https://api-fxtrade.oanda.com",
        "env": "live",
    }


def resolve_primary_oanda(cfg: Dict[str, Any], creds: Dict[str, Any]) -> Dict[str, str]:
    return resolve_oanda(cfg, creds, "live")


def mask_token(value: str) -> str:
    if not value:
        return ""
    return value[:4] + "..." + value[-4:]


def direction_to_int(value: Any) -> int:
    text = str(value or "").lower()
    if text in {"long", "buy", "1"}:
        return 1
    if text in {"short", "sell", "-1"}:
        return -1
    return 0


def direction_name(direction: int) -> str:
    return "long" if direction > 0 else "short"


def pair_bucket(instrument: str) -> str:
    base, quote = instrument_parts(instrument)
    if base in {"TRY", "HUF", "ZAR", "MXN", "NOK", "SEK", "PLN", "CZK", "HKD", "CNH", "THB"}:
        return "exotic"
    if quote in {"TRY", "HUF", "ZAR", "MXN", "NOK", "SEK", "PLN", "CZK", "HKD", "CNH", "THB"}:
        return "exotic"
    if base == "USD" or quote == "USD":
        return "major"
    return "cross"


def max_spread_for(cfg: Dict[str, Any], instrument: str) -> float:
    bucket = pair_bucket(instrument)
    if bucket == "exotic":
        return safe_float(cfg.get("max_spread_pips_exotic"), 180.0)
    if bucket == "cross":
        return safe_float(cfg.get("max_spread_pips_cross"), 8.0)
    return safe_float(cfg.get("max_spread_pips_major"), 4.0)


def slippage_for(cfg: Dict[str, Any], instrument: str) -> float:
    if pair_bucket(instrument) == "exotic":
        return safe_float(cfg.get("slippage_pips_exotic"), 2.0)
    return safe_float(cfg.get("slippage_pips_default"), 0.2)


def min_trailing_stop_for(cfg: Dict[str, Any], instrument: str, spread_pips: float) -> float:
    bucket = pair_bucket(instrument)
    if bucket == "exotic":
        floor = safe_float(cfg.get("min_trailing_stop_pips_exotic"), 25.0)
    elif bucket == "cross":
        floor = safe_float(cfg.get("min_trailing_stop_pips_cross"), 8.0)
    else:
        floor = safe_float(cfg.get("min_trailing_stop_pips_major"), 5.0)
    spread_floor = spread_pips * safe_float(cfg.get("min_trailing_spread_multiple"), 2.0)
    return max(floor, spread_floor, 0.1)


def market_closed(dt: Optional[datetime] = None) -> bool:
    dt = dt or utc_now()
    wd = dt.weekday()
    if wd == 5:
        return True
    if wd == 6 and dt.hour < 21:
        return True
    if wd == 4 and dt.hour >= 21:
        return True
    return False


def in_rollover_window(cfg: Dict[str, Any], dt: Optional[datetime] = None) -> bool:
    dt = dt or utc_now()
    hour = dt.hour + dt.minute / 60.0
    start = safe_float(cfg.get("rollover_start_hour_utc"), 21.0)
    end = safe_float(cfg.get("rollover_end_hour_utc"), 21.5)
    return start <= hour < end


@dataclass(frozen=True)
class AlignmentRow:
    instrument: str
    event: str
    family: str
    direction: int
    horizon_minutes: int
    sample_count: int
    week_count: int
    positive_week_rate: float
    win_rate: float
    mean_net_pips: float
    median_net_pips: float
    profit_factor: float
    mean_mfe_pips: float
    mean_mae_pips: float
    alignment_score: float


@dataclass
class Forecast:
    forecast_id: str
    generated_utc: str
    instrument: str
    event: str
    family: str
    direction: int
    horizon_minutes: int
    sample_count: int
    week_count: int
    probability: float
    win_rate_raw: float
    positive_week_rate: float
    profit_factor: float
    mean_net_pips: float
    edge_pips: float
    risk_pips: float
    stop_pips: float
    take_profit_pips: float
    trailing_stop_pips: float
    spread_pips: float
    mid: float
    bid: float
    ask: float
    entry_ref: float
    stop_loss: float
    take_profit: float
    rank_score: float
    expected_r_multiple: float
    reject_reason: str = ""
    model_target_direction: int = 0
    execution_side_policy: str = "target_semantics"
    source_stream: str = ""
    model_feature_timeframe: str = ""
    model_feature_time_utc: str = ""
    model_threshold: float = 0.0
    model_expected_move_atr: float = 0.0
    model_raw_rank_score: float = 0.0
    model_score_percentile: float = 0.0
    model_atr240_pips: float = 0.0
    model_momentum_30_atr: float = 0.0
    pair_threshold_rank_score: float = 0.0
    pair_report_rank_score: float = 0.0
    m1_hold_prob: float = 0.0
    m1_quick_prob: float = 0.0
    m1_overlay_threshold: float = 0.0
    m1_overlay_status: str = ""
    m1_overlay_feature_time_utc: str = ""

    def direction_name(self) -> str:
        return direction_name(self.direction)


@dataclass
class PaperPosition:
    position_id: str
    opened_utc: str
    instrument: str
    direction: int
    units: int
    entry_price: float
    stop_loss: float
    take_profit: float
    trailing_stop_pips: float
    horizon_minutes: int
    opened_score: float
    event: str
    family: str
    expected_edge_pips: float
    risk_pips: float
    risk_usd_estimate: float = 0.0
    estimated_margin_required: float = 0.0
    last_score: float = 0.0
    unrealized_pips: float = 0.0
    unrealized_usd: float = 0.0


class OandaGateway:
    def __init__(
        self,
        token: str,
        base_url: str,
        account_id: str,
        timeout: float = 20.0,
        request_retries: int = 3,
        retry_backoff_seconds: float = 1.25,
    ):
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.account_id = account_id
        self.timeout = timeout
        self.request_retries = max(0, int(request_retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "RFC3339",
        }

    def request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        url = self.base_url + path
        if requests is not None:
            attempts = self.request_retries + 1
            last_exception: Optional[Exception] = None
            for attempt in range(attempts):
                try:
                    resp = requests.request(method, url, headers=self.headers, timeout=self.timeout, **kwargs)
                    if resp.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                        time.sleep(self.retry_backoff_seconds * (attempt + 1))
                        continue
                    try:
                        data = resp.json() if resp.text else {}
                    except Exception:
                        data = {"raw_text": resp.text}
                    data["_http_status"] = resp.status_code
                    if resp.status_code >= 400:
                        data["_error"] = True
                        data["_error_text"] = resp.text[:1000]
                    return data
                except Exception as exc:
                    last_exception = exc
                    if attempt + 1 < attempts:
                        time.sleep(self.retry_backoff_seconds * (attempt + 1))
                        continue
            return {"_error": True, "_exception": repr(last_exception), "_http_status": 0}

        params = kwargs.get("params") or {}
        if params:
            url = url + "?" + urllib.parse.urlencode(params)
        body = kwargs.get("json")
        encoded_body = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(url, data=encoded_body, headers=self.headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                text = response.read().decode("utf-8", errors="replace")
                data = json.loads(text) if text else {}
                data["_http_status"] = int(response.status)
                return data
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(text) if text else {}
            except Exception:
                data = {"raw_text": text}
            data["_http_status"] = int(exc.code)
            data["_error"] = True
            data["_error_text"] = text[:1000]
            return data
        except Exception as exc:
            return {"_error": True, "_exception": repr(exc), "_http_status": 0}

    def get_account_summary(self) -> Dict[str, Any]:
        data = self.request("GET", f"/v3/accounts/{self.account_id}/summary")
        return data.get("account", {}) if isinstance(data, dict) else {}

    def get_instruments(self) -> List[Dict[str, Any]]:
        data = self.request("GET", f"/v3/accounts/{self.account_id}/instruments")
        return data.get("instruments", []) if isinstance(data, dict) else []

    def get_prices(self, instruments: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        chunk_size = 40
        for idx in range(0, len(instruments), chunk_size):
            chunk = list(instruments)[idx : idx + chunk_size]
            if not chunk:
                continue
            data = self.request(
                "GET",
                f"/v3/accounts/{self.account_id}/pricing",
                params={"instruments": ",".join(chunk), "includeHomeConversions": "true"},
            )
            for price in data.get("prices", []) or []:
                inst = str(price.get("instrument") or "")
                if inst:
                    out[inst] = price
        return out

    def get_open_trades(self) -> List[Dict[str, Any]]:
        data = self.request("GET", f"/v3/accounts/{self.account_id}/openTrades")
        return data.get("trades", []) if isinstance(data, dict) else []

    def get_pending_orders(self) -> List[Dict[str, Any]]:
        data = self.request("GET", f"/v3/accounts/{self.account_id}/pendingOrders")
        return data.get("orders", []) if isinstance(data, dict) else []

    def get_transactions_since(self, transaction_id: str) -> Dict[str, Any]:
        return self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/transactions/sinceid",
            params={"id": str(transaction_id)},
        )

    def get_candles(self, instrument: str, count: int, granularity: str = "M1") -> List[Dict[str, Any]]:
        data = self.request(
            "GET",
            f"/v3/instruments/{instrument}/candles",
            params={
                "granularity": str(granularity or "M1").upper(),
                "price": "M",
                "count": str(max(50, min(5000, int(count)))),
            },
        )
        return [item for item in data.get("candles", []) or [] if item.get("complete")]

    def create_market_order(
        self,
        forecast: Forecast,
        units: int,
        precision: int,
        tag: str,
    ) -> Dict[str, Any]:
        order: Dict[str, Any] = {
            "type": "MARKET",
            "instrument": forecast.instrument,
            "units": str(int(units)),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "clientExtensions": {
                "tag": tag[:128],
                "id": forecast.forecast_id[:128],
                "comment": SCRIPT_VERSION,
            },
            "stopLossOnFill": {
                "price": fmt_price(forecast.stop_loss, precision),
                "timeInForce": "GTC",
            },
            "takeProfitOnFill": {
                "price": fmt_price(forecast.take_profit, precision),
                "timeInForce": "GTC",
            },
        }
        if forecast.trailing_stop_pips > 0:
            order["trailingStopLossOnFill"] = {
                "distance": fmt_price(forecast.trailing_stop_pips * pips_size(forecast.instrument), precision),
                "timeInForce": "GTC",
            }
        return self.request("POST", f"/v3/accounts/{self.account_id}/orders", json={"order": order})

    def close_trade(self, trade_id: str) -> Dict[str, Any]:
        return self.request("PUT", f"/v3/accounts/{self.account_id}/trades/{trade_id}/close", json={"units": "ALL"})


def parse_broker_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except Exception:
        return None


def extract_order_fill_ids(response: Dict[str, Any]) -> Dict[str, str]:
    fill = response.get("orderFillTransaction") or {}
    create = response.get("orderCreateTransaction") or {}
    opened = fill.get("tradeOpened") or {}
    trade_id = str(opened.get("tradeID") or opened.get("id") or "").strip()
    return {
        "broker_order_id": str(create.get("id") or fill.get("orderID") or "").strip(),
        "broker_fill_id": str(fill.get("id") or "").strip(),
        "broker_trade_id": trade_id,
    }


class ProcessLock:
    def __init__(self, path: Path, description: str):
        self.path = path
        self.description = description
        self.handle: Any = None
        self.lock_impl = ""

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self.handle = self.path.open("r+b")
        if self.path.stat().st_size == 0:
            self.handle.write(b"\0")
            self.handle.flush()
        try:
            import msvcrt

            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            self.lock_impl = "msvcrt"
        except ImportError:
            import fcntl

            self.handle.seek(0)
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_impl = "fcntl"
        except OSError as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError(f"Another {self.description} process already holds {self.path}") from exc

        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(
            (
                f"pid={os.getpid()}\n"
                f"description={self.description}\n"
                f"started_utc={iso_utc()}\n"
            ).encode("utf-8")
        )
        self.handle.flush()

    def release(self) -> None:
        if not self.handle:
            return
        try:
            self.handle.seek(0)
            if self.lock_impl == "msvcrt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            elif self.lock_impl == "fcntl":
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def fmt_price(value: float, precision: int) -> str:
    return f"{float(value):.{max(0, int(precision))}f}"


def price_from_oanda(price: Dict[str, Any], fallback_mid: float, instrument: str) -> Dict[str, float]:
    bids = price.get("bids") or []
    asks = price.get("asks") or []
    bid = safe_float(bids[0].get("price") if bids else None, fallback_mid)
    ask = safe_float(asks[0].get("price") if asks else None, fallback_mid)
    if bid <= 0 or ask <= 0:
        bid = fallback_mid
        ask = fallback_mid
    mid = (bid + ask) / 2.0
    spread_pips = abs(ask - bid) / pips_size(instrument)
    return {"bid": bid, "ask": ask, "mid": mid, "spread_pips": spread_pips}


def price_from_frame(frame: pd.DataFrame, instrument: str) -> Dict[str, float]:
    close = safe_float(frame["close"].iloc[-1])
    spread = safe_float(frame.get("spread_pips", pd.Series([0.0])).iloc[-1], 0.0)
    if spread <= 0:
        spread = 0.1 if pair_bucket(instrument) != "exotic" else 5.0
    half = spread * pips_size(instrument) / 2.0
    return {"bid": close - half, "ask": close + half, "mid": close, "spread_pips": spread}


def candles_to_frame(instrument: str, candles: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for candle in candles:
        mid = candle.get("mid") or {}
        rows.append(
            {
                "datetime": candle.get("time"),
                "instrument": instrument,
                "open": safe_float(mid.get("o")),
                "high": safe_float(mid.get("h")),
                "low": safe_float(mid.get("l")),
                "close": safe_float(mid.get("c")),
                "volume": safe_float(candle.get("volume")),
                "spread_pips": np.nan,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["datetime", "open", "high", "low", "close"])
    return frame.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)


def load_alignment_rows(cfg: Dict[str, Any]) -> Dict[str, List[AlignmentRow]]:
    path = resolve_project_path(cfg.get("alignment_best_csv"), ALIGNMENT_BEST_CSV)
    if not path.exists():
        raise FileNotFoundError(f"Missing alignment report: {path}")
    per_pair_limit = safe_int(cfg.get("top_alignment_rows_per_pair"), 50)
    out: Dict[str, List[AlignmentRow]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            row = AlignmentRow(
                instrument=str(raw.get("instrument") or ""),
                event=str(raw.get("event") or ""),
                family=str(raw.get("family") or ""),
                direction=direction_to_int(raw.get("direction")),
                horizon_minutes=safe_int(raw.get("horizon_minutes")),
                sample_count=safe_int(raw.get("sample_count")),
                week_count=safe_int(raw.get("week_count")),
                positive_week_rate=safe_float(raw.get("positive_week_rate")),
                win_rate=safe_float(raw.get("win_rate")),
                mean_net_pips=safe_float(raw.get("mean_net_pips")),
                median_net_pips=safe_float(raw.get("median_net_pips")),
                profit_factor=safe_float(raw.get("profit_factor")),
                mean_mfe_pips=safe_float(raw.get("mean_mfe_pips")),
                mean_mae_pips=safe_float(raw.get("mean_mae_pips")),
                alignment_score=safe_float(raw.get("alignment_score")),
            )
            if not row.instrument or not row.event or row.direction == 0:
                continue
            out.setdefault(row.instrument, []).append(row)
    for inst in list(out):
        out[inst].sort(key=lambda item: item.alignment_score, reverse=True)
        out[inst] = out[inst][:per_pair_limit]
    return out


def shrink_probability(win_rate: float, sample_count: int, prior: float, prior_weight: float) -> float:
    n = max(0.0, float(sample_count))
    return (win_rate * n + prior * prior_weight) / max(1.0, n + prior_weight)


def horizon_minutes_from_names(*names: str) -> int:
    for name in names:
        match = re.search(r"_(\d+)$", str(name or ""))
        if match:
            return max(1, int(match.group(1)))
    return 120


def model_direction_from_row(
    row: Dict[str, Any],
    target: str,
    outcome: str,
    execution_side_policy: str = "target_semantics",
) -> int:
    policy = str(execution_side_policy or "target_semantics").strip().lower()

    def target_semantic_direction() -> int:
        name = f"{target} {outcome}".lower()
        if "short" in name and "long" not in name.split("short", 1)[0]:
            return -1
        if "long" in name:
            return 1
        momentum = safe_float(row.get("momentum_30_atr"), 0.0)
        if "continuation" in name:
            return 1 if momentum >= 0.0 else -1
        if "reversal" in name:
            return -1 if momentum >= 0.0 else 1
        return 1 if momentum >= 0.0 else -1

    if policy in {"target_semantics", "target", "model_target"}:
        return target_semantic_direction()
    if policy in {"invert_target_semantics", "inverse_target", "opposite_target"}:
        return -target_semantic_direction()
    if policy in {"follow_momentum", "continuation", "momentum"}:
        momentum = safe_float(row.get("momentum_30_atr"), 0.0)
        return 1 if momentum >= 0.0 else -1
    if policy in {"fade_momentum", "reversal"}:
        momentum = safe_float(row.get("momentum_30_atr"), 0.0)
        return -1 if momentum >= 0.0 else 1
    if policy in {"candidate_direction", "row_direction"}:
        direction = direction_to_int(row.get("direction"))
        if direction:
            return direction
        return target_semantic_direction()
    raise ValueError(f"Unsupported execution_side_policy: {execution_side_policy}")


def validate_execution_side_policy(policy: str) -> str:
    normalized = str(policy or "target_semantics").strip().lower()
    model_direction_from_row({"momentum_30_atr": 1.0}, "", "", normalized)
    return normalized


def _numeric_series(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in frame:
        return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series(default, index=frame.index, dtype=float)


def _median_finite(values: Sequence[float]) -> float:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return 0.0
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


MODEL_TIMEFRAME_ALIASES = {
    "M1": "1min",
    "M5": "5min",
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D",
}


def normalize_model_timeframe(value: Any) -> str:
    text = str(value or "5min").strip()
    if not text:
        return "5min"
    return MODEL_TIMEFRAME_ALIASES.get(text.upper(), text)


def model_timeframe_minutes(value: Any) -> int:
    delta = pd.Timedelta(normalize_model_timeframe(value))
    minutes = int(delta.total_seconds() // 60)
    if minutes <= 0:
        raise ValueError(f"Unsupported model feature timeframe: {value}")
    return minutes


def model_bars_for_minutes(minutes: int, base_minutes: int) -> int:
    return max(1, int(round(float(minutes) / float(max(base_minutes, 1)))))


def build_live_model_base_frame(
    frame: pd.DataFrame,
    instrument: str,
    price: Dict[str, float],
    *,
    feature_timeframe: Any = "5min",
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    raw = frame.copy()
    if "datetime" in raw:
        raw["datetime"] = pd.to_datetime(raw["datetime"], errors="coerce", utc=True)
    elif "time_utc" in raw:
        raw["datetime"] = pd.to_datetime(raw["time_utc"], errors="coerce", utc=True)
    else:
        raw["datetime"] = pd.to_datetime(raw.index, errors="coerce", utc=True)
    raw = raw.dropna(subset=["datetime", "open", "high", "low", "close"])
    if raw.empty:
        return pd.DataFrame()
    raw = raw.sort_values("datetime").drop_duplicates("datetime").set_index("datetime")
    for column in ["open", "high", "low", "close"]:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw = raw.dropna(subset=["open", "high", "low", "close"])
    if raw.empty:
        return pd.DataFrame()
    spread_default = max(0.0, safe_float(price.get("spread_pips"), 0.0))
    raw["spread_pips"] = _numeric_series(raw, "spread_pips", spread_default).fillna(spread_default)
    raw["volume"] = _numeric_series(raw, "volume", 0.0).fillna(0.0)

    timeframe = normalize_model_timeframe(feature_timeframe)
    base_minutes = model_timeframe_minutes(timeframe)
    deltas = raw.index.to_series().diff().dropna().dt.total_seconds()
    median_delta = float(deltas.median()) if not deltas.empty else base_minutes * 60.0
    if median_delta < (base_minutes * 60.0 * 0.75):
        counts = raw["close"].resample(timeframe).count()
        bars = pd.DataFrame(
            {
                "open": raw["open"].resample(timeframe).first(),
                "high": raw["high"].resample(timeframe).max(),
                "low": raw["low"].resample(timeframe).min(),
                "close": raw["close"].resample(timeframe).last(),
                "volume": raw["volume"].resample(timeframe).sum(),
                "spread_pips": raw["spread_pips"].resample(timeframe).median(),
                "_count": counts,
            }
        )
        source_minutes = max(1.0, median_delta / 60.0)
        expected_source_bars = max(1.0, float(base_minutes) / source_minutes)
        min_count = max(1, int(math.ceil(expected_source_bars * 0.70)))
        bars = bars[bars["_count"] >= min_count].drop(columns=["_count"])
    else:
        bars = raw[["open", "high", "low", "close", "volume", "spread_pips"]].copy()
    bars = bars.dropna(subset=["open", "high", "low", "close"])
    if len(bars) < 60:
        return pd.DataFrame()

    try:
        from oanda_gpt_training_strategy_manager import (
            add_calendar_features,
            add_trend_channel_features,
        )
    except Exception as exc:
        raise RuntimeError(f"Unable to import technical feature builders: {exc}") from exc

    pip = pips_size(instrument)
    multiplier = 1.0 / max(pip, 1e-12)
    close = pd.to_numeric(bars["close"], errors="coerce")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    spread = pd.to_numeric(bars["spread_pips"], errors="coerce").fillna(spread_default).clip(lower=0.0)
    volume = pd.to_numeric(bars["volume"], errors="coerce").fillna(0.0)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1) * multiplier
    atr240_window = model_bars_for_minutes(240, base_minutes)
    atr240 = true_range.rolling(atr240_window, min_periods=atr240_window).mean().clip(lower=0.1)

    out = bars.copy()
    out["atr240_pips"] = atr240

    def momentum(minutes: int) -> pd.Series:
        bars_back = model_bars_for_minutes(minutes, base_minutes)
        return ((close - close.shift(bars_back)) * multiplier) / atr240

    out["momentum_5_atr"] = momentum(5)
    out["momentum_15_atr"] = momentum(15)
    out["momentum_30_atr"] = momentum(30)
    out["momentum_60_atr"] = momentum(60)
    momentum_15_bars = model_bars_for_minutes(15, base_minutes)
    momentum_15_pips = (close - close.shift(momentum_15_bars)) * multiplier
    prior_momentum_15_pips = (
        close.shift(momentum_15_bars) - close.shift(momentum_15_bars * 2)
    ) * multiplier
    out["acceleration_15_atr"] = (momentum_15_pips - prior_momentum_15_pips) / atr240
    sma7 = close.rolling(7, min_periods=7).mean()
    sma8 = close.rolling(8, min_periods=8).mean()
    sma30 = close.rolling(30, min_periods=30).mean()
    out["sma7_minus_8_atr"] = ((sma7 - sma8) * multiplier) / atr240
    out["sma30_slope_5_atr"] = (
        (sma30 - sma30.shift(model_bars_for_minutes(5, base_minutes))) * multiplier
    ) / atr240
    atr15_window = model_bars_for_minutes(15, base_minutes)
    atr15 = true_range.rolling(atr15_window, min_periods=atr15_window).mean()
    out["atr15_to_atr240"] = atr15 / atr240
    range_30 = close.rolling(6, min_periods=6)
    out["compression_30"] = (
        (range_30.max() - range_30.min()) * multiplier
    ) / (atr240 * math.sqrt(6.0))
    for label, minutes in [("60", 60), ("240", 240)]:
        window = max(2, model_bars_for_minutes(minutes, base_minutes))
        rolling = close.rolling(window, min_periods=window)
        low_window = rolling.min()
        high_window = rolling.max()
        width = (high_window - low_window).replace(0.0, np.nan)
        out[f"range_position_{label}_centered"] = (((close - low_window) / width) - 0.5) * 2.0
    spread_window = model_bars_for_minutes(60, base_minutes)
    spread_median = spread.rolling(spread_window, min_periods=spread_window).median().clip(lower=0.1)
    out["spread_ratio_60"] = spread / spread_median
    volume_window = max(2, model_bars_for_minutes(30, base_minutes))
    volume_mean = volume.rolling(volume_window, min_periods=volume_window).mean()
    volume_std = volume.rolling(volume_window, min_periods=volume_window).std().replace(0.0, np.nan)
    out["volume_z_30"] = (volume - volume_mean) / volume_std
    out["pair_log_return_15"] = np.log(close / close.shift(model_bars_for_minutes(15, base_minutes)))
    out["pair_log_return_60"] = np.log(close / close.shift(model_bars_for_minutes(60, base_minutes)))

    out = add_trend_channel_features(out, instrument)
    out["time_utc"] = out.index
    out["instrument"] = instrument
    out = add_calendar_features(out, "time_utc")
    numeric_columns = [column for column in out.columns if column not in {"time_utc", "instrument"}]
    out[numeric_columns] = out[numeric_columns].replace([np.inf, -np.inf], np.nan)
    return out


def enrich_live_model_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    try:
        from oanda_gpt_training_strategy_manager import (
            add_expanded_indicator_features,
            add_regime_features,
            carry_proxy_features,
            instrument_subset_flags,
            neutral_macro_bias_features,
            pair_taxonomy_features,
            pair_taxonomy_primary,
        )
    except Exception as exc:
        raise RuntimeError(f"Unable to import model row enrichers: {exc}") from exc

    out = rows.copy().reset_index(drop=True)
    for horizon in [15, 60]:
        currency_returns: Dict[str, List[float]] = {}
        for _, row in out.iterrows():
            inst = str(row.get("instrument") or "")
            base, quote = instrument_parts(inst)
            pair_return = safe_float(row.get(f"pair_log_return_{horizon}"), 0.0)
            currency_returns.setdefault(base, []).append(pair_return)
            currency_returns.setdefault(quote, []).append(-pair_return)
        strengths = {currency: _median_finite(values) for currency, values in currency_returns.items()}
        ordered = sorted(strengths.items(), key=lambda item: item[1])
        denominator = max(1, len(ordered) - 1)
        ranks = {currency: index / denominator for index, (currency, _) in enumerate(ordered)}
        for idx, row in out.iterrows():
            base, quote = instrument_parts(str(row.get("instrument") or ""))
            out.at[idx, f"strength_gap_{horizon}"] = strengths.get(base, 0.0) - strengths.get(quote, 0.0)
            out.at[idx, f"strength_gap_rank_{horizon}"] = ranks.get(base, 0.5) - ranks.get(quote, 0.5)

    enriched: List[pd.DataFrame] = []
    for _, raw_row in out.iterrows():
        inst = str(raw_row.get("instrument") or "")
        one = pd.DataFrame([raw_row.to_dict()])
        for key, value in instrument_subset_flags(inst).items():
            one[key] = value
        one["pair_taxonomy_primary"] = pair_taxonomy_primary(inst)
        for key, value in pair_taxonomy_features(inst).items():
            one[key] = value
        for key, value in carry_proxy_features(inst).items():
            one[key] = value
        one = add_regime_features(one, inst)
        for key, value in neutral_macro_bias_features(inst).items():
            one[key] = value
        one = add_expanded_indicator_features(one, inst)
        enriched.append(one)
    return pd.concat(enriched, ignore_index=True) if enriched else pd.DataFrame()


@dataclass
class ModelStreamCalibration:
    threshold: float
    expected_move_atr: float
    sample_count: int
    win_rate: float
    profit_factor: float
    score_percentile: float
    pair_report_rank_score: float
    pair_threshold_rank_score: float = 0.0


@dataclass
class ModelStreamBundle:
    name: str
    experiment_id: str
    feature_set: str
    feature_timeframe: str
    source_stream: str
    candidate_rows_path: str
    execution_side_policy: str
    dataset_path: str
    instrument_subset: str
    instrument_whitelist: Any
    segment_filters: Any
    target: str
    outcome: str
    features: List[str]
    estimator: Any
    calibration: Dict[str, ModelStreamCalibration]
    train_rows: int
    train_lookback_days: int


class M1OverlayScorer:
    CANDIDATE_FEATURES = {
        "probability",
        "confidence",
        "expected_move_atr",
        "rank_score",
        "score_percentile",
        "threshold",
        "spread_pips",
        "atr240_pips",
        "momentum_30_atr",
        "pair_threshold_rank_score",
        "pair_report_rank_score",
    }

    def __init__(self, cfg: Dict[str, Any]):
        stream_cfg = dict(cfg.get("model_stream") or {})
        raw_cfg = stream_cfg.get("m1_overlay") or cfg.get("m1_overlay") or {}
        self.cfg = dict(raw_cfg or {})
        self.enabled = bool(self.cfg.get("enabled", False))
        self.model_path = resolve_project_path(self.cfg.get("model_path"), PROJECT_ROOT / "__missing_m1_overlay__.joblib")
        self.hold_threshold = safe_float(self.cfg.get("hold_threshold"), 0.45)
        self.quick_threshold = safe_float(self.cfg.get("quick_threshold"), 0.0)
        self.feature_timeframe = normalize_model_timeframe(self.cfg.get("feature_timeframe") or "1min")
        self.oanda_candle_granularity = str(self.cfg.get("oanda_candle_granularity") or "M1").upper()
        self.oanda_candle_count = safe_int(self.cfg.get("oanda_candle_count"), 650)
        self.local_candle_count = safe_int(self.cfg.get("local_candle_count"), 1200)
        self.scope = str(self.cfg.get("scope") or "all_loaded").strip().lower()
        self.fail_closed = bool(self.cfg.get("fail_closed", True))
        self.features: List[str] = []
        self.hold_model: Any = None
        self.quick_model: Any = None
        self.bundle_meta: Dict[str, Any] = {}
        if self.enabled:
            if not self.model_path.exists():
                raise FileNotFoundError(f"Missing M1 overlay model: {self.model_path}")
            try:
                import joblib
            except Exception as exc:
                raise RuntimeError(f"M1 overlay requires joblib: {exc}") from exc
            bundle = joblib.load(self.model_path)
            if not isinstance(bundle, dict):
                raise RuntimeError(f"Unexpected M1 overlay bundle type: {type(bundle).__name__}")
            self.features = list(bundle.get("features") or [])
            self.hold_model = bundle.get("hold_model")
            self.quick_model = bundle.get("quick_model")
            if not self.features or self.hold_model is None:
                raise RuntimeError(f"Incomplete M1 overlay bundle: {self.model_path}")
            self.bundle_meta = {
                "trained_rows": safe_int(bundle.get("trained_rows"), 0),
                "trained_start_utc": str(bundle.get("trained_start_utc") or ""),
                "trained_end_utc": str(bundle.get("trained_end_utc") or ""),
                "hold_positive_rate": safe_float(bundle.get("hold_positive_rate"), 0.0),
                "quick_positive_rate": safe_float(bundle.get("quick_positive_rate"), 0.0),
                "feature_count": len(self.features),
            }

    def meta(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "model_path": str(self.model_path) if self.enabled else "",
            "hold_threshold": self.hold_threshold,
            "quick_threshold": self.quick_threshold,
            "feature_timeframe": self.feature_timeframe,
            "oanda_candle_granularity": self.oanda_candle_granularity,
            "oanda_candle_count": self.oanda_candle_count,
            "local_candle_count": self.local_candle_count,
            "scope": self.scope,
            "fail_closed": self.fail_closed,
            **self.bundle_meta,
        }

    def _source_bucket(self, forecast: Forecast) -> str:
        text = f"{forecast.source_stream} {forecast.model_feature_timeframe}".strip().lower()
        if "m30" in text or "30min" in text:
            return "m30"
        if "h4" in text or "4h" in text:
            return "h4"
        if "h1" in text or "1h" in text:
            return "h1"
        return str(forecast.source_stream or forecast.model_feature_timeframe or "").strip().lower()

    def _predict_positive_proba(self, model: Any, x_live: pd.DataFrame) -> float:
        if model is None:
            return 0.0
        proba = model.predict_proba(x_live)[0]
        classes = getattr(model, "classes_", None)
        if classes is not None:
            for idx, label in enumerate(classes):
                try:
                    if int(label) == 1:
                        return safe_float(proba[idx], 0.0)
                except Exception:
                    continue
        return safe_float(proba[-1] if len(proba) else 0.0, 0.0)

    def _feature_frame(self, forecast: Forecast, m1_row: Dict[str, Any]) -> pd.DataFrame:
        source_bucket = self._source_bucket(forecast)
        direction_label = "LONG" if forecast.direction > 0 else "SHORT"
        candidate = {
            "probability": forecast.probability,
            "confidence": forecast.probability,
            "expected_move_atr": forecast.model_expected_move_atr,
            "rank_score": forecast.model_raw_rank_score,
            "score_percentile": forecast.model_score_percentile,
            "threshold": forecast.model_threshold,
            "spread_pips": forecast.spread_pips,
            "atr240_pips": forecast.model_atr240_pips,
            "momentum_30_atr": forecast.model_momentum_30_atr,
            "pair_threshold_rank_score": forecast.pair_threshold_rank_score,
            "pair_report_rank_score": forecast.pair_report_rank_score,
        }
        values: Dict[str, float] = {}
        for feature in self.features:
            if feature.startswith("candidate__source_stream_"):
                expected = feature.removeprefix("candidate__source_stream_").lower()
                values[feature] = 1.0 if expected == source_bucket else 0.0
            elif feature.startswith("candidate__direction_"):
                expected = feature.removeprefix("candidate__direction_").upper()
                values[feature] = 1.0 if expected == direction_label else 0.0
            elif feature == "candidate__direction_sign":
                values[feature] = 1.0 if forecast.direction > 0 else -1.0
            elif feature.startswith("candidate__"):
                key = feature.removeprefix("candidate__")
                values[feature] = safe_float(candidate.get(key), 0.0)
            elif feature.startswith("m1__"):
                key = feature.removeprefix("m1__")
                values[feature] = safe_float(m1_row.get(key), 0.0)
            else:
                values[feature] = 0.0
        x_live = pd.DataFrame([values], columns=self.features)
        return x_live.replace([np.inf, -np.inf], np.nan)

    def apply(self, forecast: Forecast, m1_row: Optional[Dict[str, Any]]) -> None:
        if not self.enabled or forecast.reject_reason:
            return
        forecast.m1_overlay_threshold = self.hold_threshold
        if not m1_row:
            forecast.m1_overlay_status = "missing_m1_row"
            if self.fail_closed:
                forecast.reject_reason = "m1_overlay_missing_row"
            return
        try:
            x_live = self._feature_frame(forecast, m1_row)
            hold_prob = self._predict_positive_proba(self.hold_model, x_live)
            quick_prob = self._predict_positive_proba(self.quick_model, x_live) if self.quick_model is not None else 0.0
            forecast.m1_hold_prob = round(hold_prob, 6)
            forecast.m1_quick_prob = round(quick_prob, 6)
            forecast.m1_overlay_status = "scored"
            feature_time = parse_broker_time(m1_row.get("time_utc"))
            forecast.m1_overlay_feature_time_utc = feature_time.isoformat() if feature_time else ""
            if hold_prob < self.hold_threshold:
                forecast.reject_reason = f"m1_hold_probability_below_gate:{hold_prob:.3f}<{self.hold_threshold:.3f}"
            elif self.quick_threshold > 0.0 and quick_prob < self.quick_threshold:
                forecast.reject_reason = f"m1_quick_probability_below_gate:{quick_prob:.3f}<{self.quick_threshold:.3f}"
        except Exception as exc:
            forecast.m1_overlay_status = f"error:{type(exc).__name__}:{str(exc)[:120]}"
            if self.fail_closed:
                forecast.reject_reason = "m1_overlay_score_error"


class ModelStreamForecaster:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.stream_cfg = dict(cfg.get("model_stream") or {})
        if not bool(self.stream_cfg.get("enabled", False)):
            raise RuntimeError("forecast_source=model_stream requires model_stream.enabled=true")
        self.name = str(self.stream_cfg.get("name") or "model_stream")
        self.min_raw_rank_score = safe_float(self.stream_cfg.get("min_raw_rank_score"), 0.12)
        self.rank_score_scale = safe_float(self.stream_cfg.get("rank_score_scale"), 0.10)
        self.train_row_cap = safe_int(self.stream_cfg.get("train_row_cap"), 30000)
        self.train_lookback_days = safe_int(self.stream_cfg.get("train_lookback_days"), 90)
        self.feature_timeframe = normalize_model_timeframe(self.stream_cfg.get("feature_timeframe") or "5min")
        self.cache_dir = resolve_project_path(self.stream_cfg.get("model_cache_dir"), DATA_DIR / "model_cache")
        self.candidate_rows_path = resolve_project_path(
            self.stream_cfg.get("candidate_rows_csv"),
            HGB_SCORE_012_CANDIDATES_CSV,
        )
        self.m1_overlay = M1OverlayScorer(cfg)
        self.bundles = self._load_bundles()

    def instruments(self, max_pairs: int = 0) -> List[str]:
        instruments = sorted(
            {
                instrument
                for bundle in self.bundles
                for instrument in bundle.calibration.keys()
            }
        )
        available = set(list_candle_instruments())
        instruments = [instrument for instrument in instruments if instrument in available]
        if max_pairs > 0:
            instruments = instruments[:max_pairs]
        return instruments

    def _load_spec(self, model_cfg: Dict[str, Any]) -> Dict[str, Any]:
        path = resolve_project_path(model_cfg.get("spec_json"), HGB_FULL_EXPERIMENT_JSON)
        payload = read_json(path, {})
        spec = payload.get("spec")
        if not isinstance(spec, dict):
            raise RuntimeError(f"Missing experiment spec in {path}")
        spec = json.loads(json.dumps(spec))
        feature_set = str(model_cfg.get("feature_set") or spec.get("feature_set") or "")
        if feature_set:
            spec["feature_set"] = feature_set
        instrument_subset = str(
            model_cfg.get("instrument_subset")
            or self.stream_cfg.get("instrument_subset")
            or ""
        ).strip().lower()
        if instrument_subset:
            spec["instrument_subset"] = instrument_subset
        if self.train_row_cap > 0:
            current = safe_int(spec.get("max_train_rows"), self.train_row_cap)
            spec["max_train_rows"] = min(current, self.train_row_cap)
        return spec

    def _dataset_path_for(self, model_cfg: Dict[str, Any]) -> Path:
        return resolve_project_path(
            model_cfg.get("dataset_path") or self.stream_cfg.get("dataset_path"),
            TRAINING_ROOT / "continuous_research" / "technical_spike_research.parquet",
        )

    def _candidate_rows_path_for(self, model_cfg: Dict[str, Any]) -> Path:
        return resolve_project_path(
            model_cfg.get("candidate_rows_csv") or self.stream_cfg.get("candidate_rows_csv"),
            HGB_SCORE_012_CANDIDATES_CSV,
        )

    def _feature_timeframe_for(self, model_cfg: Dict[str, Any]) -> str:
        return normalize_model_timeframe(
            model_cfg.get("feature_timeframe")
            or self.stream_cfg.get("feature_timeframe")
            or self.feature_timeframe
        )

    def _execution_side_policy_for(self, model_cfg: Dict[str, Any]) -> str:
        return validate_execution_side_policy(
            str(
                model_cfg.get("execution_side_policy")
                or self.stream_cfg.get("execution_side_policy")
                or "target_semantics"
            )
        )

    def _train_lookback_days_for(self, model_cfg: Dict[str, Any]) -> int:
        return safe_int(
            model_cfg.get("train_lookback_days"),
            self.train_lookback_days,
        )

    def _bundle_cache_path(
        self,
        experiment_id: str,
        spec: Dict[str, Any],
        dataset_path: Path,
        candidate_rows_path: Path,
        feature_timeframe: str,
        execution_side_policy: str,
        train_lookback_days: int,
    ) -> Path:
        payload = {
            "script_version": SCRIPT_VERSION,
            "stream_name": self.name,
            "experiment_id": experiment_id,
            "spec": spec,
            "candidate_rows": str(candidate_rows_path.resolve()),
            "candidate_rows_mtime_ns": candidate_rows_path.stat().st_mtime_ns if candidate_rows_path.exists() else 0,
            "dataset_path": str(dataset_path.resolve()),
            "dataset_mtime_ns": dataset_path.stat().st_mtime_ns if dataset_path.exists() else 0,
            "feature_timeframe": feature_timeframe,
            "execution_side_policy": execution_side_policy,
            "train_row_cap": self.train_row_cap,
            "train_lookback_days": train_lookback_days,
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:20]
        return self.cache_dir / f"{experiment_id}_{spec.get('feature_set', 'features')}_{digest}.pkl"

    def _load_cached_bundle(self, cache_path: Path) -> Optional[ModelStreamBundle]:
        if not cache_path.exists():
            return None
        try:
            with cache_path.open("rb") as handle:
                bundle = pickle.load(handle)
            if isinstance(bundle, ModelStreamBundle):
                for required in (
                    "feature_timeframe",
                    "dataset_path",
                    "source_stream",
                    "candidate_rows_path",
                    "execution_side_policy",
                    "train_lookback_days",
                ):
                    if not hasattr(bundle, required):
                        return None
                calibrations = getattr(bundle, "calibration", {})
                needs_pair_threshold_repair = any(
                    not hasattr(calibration, "pair_threshold_rank_score")
                    for calibration in calibrations.values()
                ) or (
                    bool(calibrations)
                    and not any(
                        abs(safe_float(getattr(calibration, "pair_threshold_rank_score", 0.0), 0.0)) > 0.0
                        for calibration in calibrations.values()
                    )
                )
                if needs_pair_threshold_repair:
                    try:
                        repaired = self._calibration_for(
                            bundle.experiment_id,
                            Path(bundle.candidate_rows_path),
                        )
                    except Exception:
                        repaired = {}
                    for instrument, calibration in calibrations.items():
                        setattr(
                            calibration,
                            "pair_threshold_rank_score",
                            safe_float(
                                getattr(repaired.get(instrument), "pair_threshold_rank_score", 0.0),
                                0.0,
                            ),
                        )
                return bundle
        except Exception:
            return None
        return None

    def _write_cached_bundle(self, cache_path: Path, bundle: ModelStreamBundle) -> None:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
            with tmp.open("wb") as handle:
                pickle.dump(bundle, handle, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, cache_path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)  # type: ignore[name-defined]
            except Exception:
                pass

    def _load_training_frame(
        self,
        spec: Dict[str, Any],
        dataset_path: Path,
        train_lookback_days: int,
    ) -> Tuple[pd.DataFrame, List[str]]:
        try:
            from oanda_gpt_training_strategy_manager import (
                TECHNICAL_CORE_FEATURES,
                TECHNICAL_MODEL_FEATURES,
                apply_instrument_subset,
                apply_instrument_whitelist,
                apply_segment_filters,
                segment_filter_columns,
            )
        except Exception as exc:
            raise RuntimeError(f"Unable to import training dataset helpers: {exc}") from exc

        if str(spec.get("dataset_kind") or "") not in {"technical_spike", "profitable_move_precursor"}:
            raise RuntimeError(f"Unsupported live model dataset kind: {spec.get('dataset_kind')}")
        features = (
            TECHNICAL_CORE_FEATURES
            if str(spec.get("feature_set") or "") == "technical_core"
            else TECHNICAL_MODEL_FEATURES
        )
        target = str(spec.get("target") or "")
        outcome = str(spec.get("outcome") or "")
        path = dataset_path
        if not path.exists():
            raise FileNotFoundError(f"Missing technical research parquet: {path}")
        needed = list(
            dict.fromkeys(
                [
                    "time_utc",
                    "instrument",
                    "pair_taxonomy_primary",
                    "regime_primary",
                    *segment_filter_columns(spec.get("segment_filters")),
                    *features,
                    target,
                    outcome,
                ]
            )
        )
        if train_lookback_days > 0:
            times = pd.read_parquet(path, columns=["time_utc"])
            max_time = pd.to_datetime(times["time_utc"], errors="coerce", utc=True).max()
            cutoff = max_time - pd.Timedelta(days=train_lookback_days)
            try:
                frame = pd.read_parquet(path, columns=needed, filters=[("time_utc", ">=", cutoff)])
            except Exception:
                frame = pd.read_parquet(path, columns=needed)
                frame["time_utc"] = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
                frame = frame[frame["time_utc"] >= cutoff]
        else:
            frame = pd.read_parquet(path, columns=needed)
        frame = apply_instrument_subset(frame, str(spec.get("instrument_subset") or "all"))
        frame = apply_instrument_whitelist(frame, spec.get("instrument_whitelist"))
        frame = apply_segment_filters(frame, spec.get("segment_filters"))
        return frame, list(features)

    def _calibration_for(
        self,
        experiment_id: str,
        candidate_rows_path: Path,
    ) -> Dict[str, ModelStreamCalibration]:
        if not candidate_rows_path.exists():
            raise FileNotFoundError(f"Missing model stream candidate rows: {candidate_rows_path}")
        frame = pd.read_csv(candidate_rows_path)
        subset = frame[frame["experiment_id"].astype(str).eq(experiment_id)].copy()
        if subset.empty:
            raise RuntimeError(f"No candidate calibration rows for {experiment_id} in {candidate_rows_path}")
        subset["time_utc"] = pd.to_datetime(subset["time_utc"], errors="coerce", utc=True)
        subset = subset.sort_values("time_utc")
        out: Dict[str, ModelStreamCalibration] = {}
        for instrument, group in subset.groupby("instrument"):
            latest = group.iloc[-1]
            realized = pd.to_numeric(group.get("realized_outcome_atr"), errors="coerce").dropna()
            wins = realized[realized > 0.0]
            losses = realized[realized < 0.0]
            gross_profit = float(wins.sum()) if len(wins) else 0.0
            gross_loss = abs(float(losses.sum())) if len(losses) else 0.0
            out[str(instrument)] = ModelStreamCalibration(
                threshold=safe_float(latest.get("threshold"), 0.50),
                expected_move_atr=max(0.0, safe_float(latest.get("expected_move_atr"), 0.0)),
                sample_count=int(len(group)),
                win_rate=float((realized > 0.0).mean()) if len(realized) else 0.0,
                profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0),
                score_percentile=safe_float(latest.get("score_percentile"), 0.0),
                pair_report_rank_score=safe_float(latest.get("pair_report_rank_score"), 0.0),
                pair_threshold_rank_score=safe_float(latest.get("pair_threshold_rank_score"), 0.0),
            )
        return out

    def _load_bundles(self) -> List[ModelStreamBundle]:
        try:
            from oanda_gpt_training_strategy_manager import ContinuousResearchEngine
        except Exception as exc:
            raise RuntimeError(f"Unable to import continuous research engine: {exc}") from exc

        model_cfgs = self.stream_cfg.get("models") or []
        if not isinstance(model_cfgs, list) or not model_cfgs:
            raise RuntimeError("model_stream.models must contain at least one model")
        engine = ContinuousResearchEngine()
        bundles: List[ModelStreamBundle] = []
        for raw_model_cfg in model_cfgs:
            model_cfg = dict(raw_model_cfg or {})
            experiment_id = str(model_cfg.get("experiment_id") or "").strip()
            if not experiment_id:
                raise RuntimeError("model_stream model is missing experiment_id")
            spec = self._load_spec(model_cfg)
            dataset_path = self._dataset_path_for(model_cfg)
            candidate_rows_path = self._candidate_rows_path_for(model_cfg)
            feature_timeframe = self._feature_timeframe_for(model_cfg)
            execution_side_policy = self._execution_side_policy_for(model_cfg)
            train_lookback_days = self._train_lookback_days_for(model_cfg)
            source_stream = str(model_cfg.get("source_stream") or model_cfg.get("feature_timeframe") or feature_timeframe)
            cache_path = self._bundle_cache_path(
                experiment_id,
                spec,
                dataset_path,
                candidate_rows_path,
                feature_timeframe,
                execution_side_policy,
                train_lookback_days,
            )
            cached = self._load_cached_bundle(cache_path)
            if cached is not None:
                bundles.append(cached)
                continue
            frame, features = self._load_training_frame(spec, dataset_path, train_lookback_days)
            target = str(spec.get("target") or "")
            outcome = str(spec.get("outcome") or "")
            train = frame.dropna(subset=[target]).copy()
            if self.train_row_cap > 0 and len(train) > self.train_row_cap:
                train = train.sort_values("time_utc").tail(self.train_row_cap)
            if train.empty:
                raise RuntimeError(f"No training rows for {experiment_id}")
            for feature in features:
                if feature not in train:
                    train[feature] = np.nan
            x_train = train[features].replace([np.inf, -np.inf], np.nan)
            y_train = pd.to_numeric(train[target], errors="coerce").fillna(0).astype(int)
            estimator = engine.estimator(spec)
            estimator.fit(x_train, y_train)
            bundle = ModelStreamBundle(
                name=str(model_cfg.get("name") or experiment_id),
                experiment_id=experiment_id,
                feature_set=str(spec.get("feature_set") or ""),
                feature_timeframe=feature_timeframe,
                source_stream=source_stream,
                candidate_rows_path=str(candidate_rows_path),
                execution_side_policy=execution_side_policy,
                dataset_path=str(dataset_path),
                instrument_subset=str(spec.get("instrument_subset") or "all"),
                instrument_whitelist=spec.get("instrument_whitelist"),
                segment_filters=spec.get("segment_filters"),
                target=target,
                outcome=outcome,
                features=list(features),
                estimator=estimator,
                calibration=self._calibration_for(experiment_id, candidate_rows_path),
                train_rows=int(len(train)),
                train_lookback_days=train_lookback_days,
            )
            self._write_cached_bundle(cache_path, bundle)
            bundles.append(bundle)
        return bundles

    def current_rows(
        self,
        frames: Dict[str, pd.DataFrame],
        prices: Dict[str, Dict[str, float]],
        feature_timeframe: Optional[str] = None,
    ) -> pd.DataFrame:
        latest_rows: List[pd.Series] = []
        timeframe = normalize_model_timeframe(feature_timeframe or self.feature_timeframe)
        for inst, frame in frames.items():
            built = build_live_model_base_frame(
                frame,
                inst,
                prices.get(inst, {}),
                feature_timeframe=timeframe,
            )
            if built.empty:
                continue
            usable = built.dropna(subset=["momentum_30_atr", "atr240_pips"])
            if usable.empty:
                continue
            latest_rows.append(usable.iloc[-1])
        if not latest_rows:
            return pd.DataFrame()
        return enrich_live_model_rows(pd.DataFrame(latest_rows).reset_index(drop=True))

    def forecast(
        self,
        frames: Dict[str, pd.DataFrame],
        prices: Dict[str, Dict[str, float]],
        generated_utc: str,
    ) -> Tuple[List[Forecast], Dict[str, Any]]:
        rows_by_timeframe: Dict[str, pd.DataFrame] = {}
        for bundle in self.bundles:
            timeframe = normalize_model_timeframe(bundle.feature_timeframe)
            if timeframe not in rows_by_timeframe:
                rows_by_timeframe[timeframe] = self.current_rows(
                    frames,
                    prices,
                    feature_timeframe=timeframe,
                )
        if not any(not frame.empty for frame in rows_by_timeframe.values()):
            return [], {
                "model_stream": self.name,
                "model_rows": 0,
                "model_error": "no_current_feature_rows",
            }
        try:
            from oanda_gpt_training_strategy_manager import (
                apply_instrument_subset,
                apply_instrument_whitelist,
                apply_segment_filters,
            )
        except Exception as exc:
            raise RuntimeError(f"Unable to import instrument subset filter: {exc}") from exc

        forecasts: List[Forecast] = []
        rejected = 0
        rejected_by_reason: Dict[str, int] = {}
        rejected_preview: List[Dict[str, Any]] = []
        latest_feature_time = ""
        latest_times: List[pd.Timestamp] = []
        for frame in rows_by_timeframe.values():
            if not frame.empty and "time_utc" in frame:
                latest_ts = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True).max()
                if pd.notna(latest_ts):
                    latest_times.append(latest_ts)
        if latest_times:
            latest_feature_time = max(latest_times).isoformat()
        for bundle in self.bundles:
            rows = rows_by_timeframe.get(normalize_model_timeframe(bundle.feature_timeframe), pd.DataFrame())
            if rows.empty:
                continue
            model_rows = apply_instrument_subset(rows.copy(), bundle.instrument_subset)
            model_rows = apply_instrument_whitelist(model_rows, bundle.instrument_whitelist)
            model_rows = apply_segment_filters(model_rows, bundle.segment_filters)
            for _, row_series in model_rows.iterrows():
                row = row_series.to_dict()
                inst = str(row.get("instrument") or "")
                calibration = bundle.calibration.get(inst)
                if calibration is None:
                    continue
                for feature in bundle.features:
                    row.setdefault(feature, np.nan)
                x_live = pd.DataFrame([{feature: row.get(feature, np.nan) for feature in bundle.features}])
                x_live = x_live.replace([np.inf, -np.inf], np.nan)
                proba = bundle.estimator.predict_proba(x_live)[0]
                probability = safe_float(proba[1] if len(proba) > 1 else proba[0], 0.0)
                raw_rank_score = calibration.expected_move_atr * probability
                forecast = self._build_forecast(
                    bundle=bundle,
                    row=row,
                    price=prices.get(inst, {}),
                    calibration=calibration,
                    probability=probability,
                    raw_rank_score=raw_rank_score,
                    generated_utc=generated_utc,
                )
                forecasts.append(forecast)
                if forecast.reject_reason:
                    rejected += 1
                    reason_code = forecast.reject_reason.split(":", 1)[0]
                    rejected_by_reason[reason_code] = rejected_by_reason.get(reason_code, 0) + 1
                    if len(rejected_preview) < 25:
                        rejected_preview.append(
                            {
                                "instrument": forecast.instrument,
                                "event": forecast.event,
                                "direction": forecast.direction_name(),
                                "horizon_minutes": forecast.horizon_minutes,
                                "rank_score": forecast.rank_score,
                                "probability": forecast.probability,
                                "edge_pips": forecast.edge_pips,
                                "risk_pips": forecast.risk_pips,
                                "spread_pips": forecast.spread_pips,
                                "reject_reason": forecast.reject_reason,
                            }
                        )
        forecasts.sort(key=lambda item: item.rank_score, reverse=True)
        meta = {
            "model_stream": self.name,
            "model_rows": int(sum(len(frame) for frame in rows_by_timeframe.values())),
            "model_rows_by_timeframe": {
                timeframe: int(len(frame))
                for timeframe, frame in rows_by_timeframe.items()
            },
            "latest_model_feature_time_utc": latest_feature_time,
            "models": [
                {
                    "name": bundle.name,
                    "experiment_id": bundle.experiment_id,
                    "feature_set": bundle.feature_set,
                    "feature_timeframe": bundle.feature_timeframe,
                    "source_stream": bundle.source_stream,
                    "dataset_path": bundle.dataset_path,
                    "candidate_rows_path": bundle.candidate_rows_path,
                    "execution_side_policy": bundle.execution_side_policy,
                    "instrument_subset": bundle.instrument_subset,
                    "target": bundle.target,
                    "outcome": bundle.outcome,
                    "features": len(bundle.features),
                    "train_rows": bundle.train_rows,
                    "train_lookback_days": bundle.train_lookback_days,
                    "calibrated_pairs": len(bundle.calibration),
                }
                for bundle in self.bundles
            ],
            "rejected_active_count": rejected,
            "rejected_by_reason": rejected_by_reason,
            "rejected_preview": rejected_preview,
        }
        return forecasts, meta

    def m1_overlay_target_instruments(
        self,
        forecasts: Sequence[Forecast],
        all_loaded_instruments: Sequence[str],
    ) -> List[str]:
        if not self.m1_overlay.enabled:
            return []
        if self.m1_overlay.scope in {"active", "active_candidates", "tradable"}:
            instruments = sorted({forecast.instrument for forecast in forecasts if not forecast.reject_reason})
        else:
            instruments = sorted({str(item) for item in all_loaded_instruments if str(item)})
        return instruments

    def apply_m1_overlay(
        self,
        forecasts: Sequence[Forecast],
        overlay_frames: Dict[str, pd.DataFrame],
        prices: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        meta = self.m1_overlay.meta()
        if not self.m1_overlay.enabled:
            return meta
        if not overlay_frames:
            missing = sum(1 for forecast in forecasts if not forecast.reject_reason)
            if self.m1_overlay.fail_closed:
                for forecast in forecasts:
                    if not forecast.reject_reason:
                        forecast.m1_overlay_threshold = self.m1_overlay.hold_threshold
                        forecast.m1_overlay_status = "missing_overlay_frames"
                        forecast.reject_reason = "m1_overlay_missing_frames"
            return {**meta, "overlay_rows": 0, "scored_forecasts": 0, "missing_forecasts": missing, "new_rejections": missing if self.m1_overlay.fail_closed else 0}
        rows = self.current_rows(
            overlay_frames,
            prices,
            feature_timeframe=self.m1_overlay.feature_timeframe,
        )
        by_instrument: Dict[str, Dict[str, Any]] = {}
        if not rows.empty:
            rows = rows.sort_values("time_utc")
            for _, row_series in rows.iterrows():
                by_instrument[str(row_series.get("instrument") or "")] = row_series.to_dict()
        scored = 0
        missing = 0
        new_rejections = 0
        for forecast in forecasts:
            if forecast.reject_reason:
                continue
            before = forecast.reject_reason
            row = by_instrument.get(forecast.instrument)
            if row is None:
                missing += 1
            self.m1_overlay.apply(forecast, row)
            if forecast.m1_overlay_status == "scored":
                scored += 1
            if not before and forecast.reject_reason:
                new_rejections += 1
        return {
            **meta,
            "overlay_rows": int(len(rows)),
            "overlay_instruments_loaded": int(len(overlay_frames)),
            "scored_forecasts": int(scored),
            "missing_forecasts": int(missing),
            "new_rejections": int(new_rejections),
        }

    def _build_forecast(
        self,
        *,
        bundle: ModelStreamBundle,
        row: Dict[str, Any],
        price: Dict[str, float],
        calibration: ModelStreamCalibration,
        probability: float,
        raw_rank_score: float,
        generated_utc: str,
    ) -> Forecast:
        cfg = self.cfg
        inst = str(row.get("instrument") or "")
        spread = max(0.0, safe_float(price.get("spread_pips"), row.get("spread_pips", 0.0)))
        atr_pips = max(0.1, safe_float(row.get("atr240_pips"), 0.1))
        gross_edge_pips = calibration.expected_move_atr * atr_pips
        cost_pips = spread * safe_float(cfg.get("spread_cost_multiplier"), 1.0) + slippage_for(cfg, inst)
        edge_pips = max(0.0, gross_edge_pips - cost_pips)
        risk_pips = max(
            atr_pips * safe_float(cfg.get("atr_stop_multiplier"), 0.85),
            spread * safe_float(cfg.get("min_stop_spread_multiple"), 2.0),
            0.1,
        )
        model_target_direction = model_direction_from_row(row, bundle.target, bundle.outcome, "target_semantics")
        direction = model_direction_from_row(row, bundle.target, bundle.outcome, bundle.execution_side_policy)
        rank_score = max(0.0, raw_rank_score * self.rank_score_scale)
        expected_r = edge_pips / max(risk_pips, 0.1)

        def reject(code: str, detail: str) -> str:
            return f"{code}:{detail}"

        reject_reason = ""
        if spread > max_spread_for(cfg, inst):
            reject_reason = reject("spread_above_gate", f"{spread:.2f}>{max_spread_for(cfg, inst):.2f}")
        elif probability < calibration.threshold:
            reject_reason = reject("model_probability_below_gate", f"{probability:.3f}<{calibration.threshold:.3f}")
        elif raw_rank_score < self.min_raw_rank_score:
            reject_reason = reject("model_raw_score_below_gate", f"{raw_rank_score:.3f}<{self.min_raw_rank_score:.3f}")
        elif edge_pips < safe_float(cfg.get("min_edge_pips"), 0.25):
            reject_reason = reject("edge_below_gate", f"{edge_pips:.2f}<{safe_float(cfg.get('min_edge_pips'), 0.25):.2f}")
        elif spread / max(edge_pips, 0.01) > safe_float(cfg.get("max_spread_to_edge_ratio"), 1.0):
            reject_reason = reject(
                "spread_edge_ratio_above_gate",
                f"{(spread / max(edge_pips, 0.01)):.3f}>{safe_float(cfg.get('max_spread_to_edge_ratio'), 1.0):.3f}",
            )
        elif rank_score < safe_float(cfg.get("min_rank_score"), 0.012):
            reject_reason = reject("score_below_gate", f"{rank_score:.3f}<{safe_float(cfg.get('min_rank_score'), 0.012):.3f}")

        entry = safe_float(price.get("ask") if direction > 0 else price.get("bid"), safe_float(price.get("mid"), 0.0))
        if entry <= 0:
            entry = safe_float(row.get("close"), 0.0)
        pip = pips_size(inst)
        stop_pips = risk_pips
        take_profit_pips = max(
            edge_pips * safe_float(cfg.get("take_profit_edge_capture"), 0.45),
            risk_pips * safe_float(cfg.get("take_profit_min_r_multiple"), 0.35),
        )
        trailing_stop_pips = max(
            risk_pips * safe_float(cfg.get("trailing_stop_r_multiple"), 0.35),
            min_trailing_stop_for(cfg, inst, spread),
        )
        if direction > 0:
            stop_loss = entry - stop_pips * pip
            take_profit = entry + take_profit_pips * pip
        else:
            stop_loss = entry + stop_pips * pip
            take_profit = entry - take_profit_pips * pip
        feature_dt = parse_broker_time(row.get("time_utc"))
        return Forecast(
            forecast_id=f"pfr_{uuid.uuid4().hex[:20]}",
            generated_utc=generated_utc,
            instrument=inst,
            event=f"{bundle.name}:{bundle.target}",
            family=f"model_stream:{bundle.feature_set}",
            direction=direction,
            horizon_minutes=horizon_minutes_from_names(bundle.target, bundle.outcome),
            sample_count=calibration.sample_count,
            week_count=max(1, calibration.sample_count // 20),
            probability=round(probability, 6),
            win_rate_raw=round(calibration.win_rate, 6),
            positive_week_rate=round(calibration.score_percentile, 6),
            profit_factor=round(calibration.profit_factor, 6),
            mean_net_pips=round(edge_pips, 6),
            edge_pips=round(edge_pips, 6),
            risk_pips=round(risk_pips, 6),
            stop_pips=round(stop_pips, 6),
            take_profit_pips=round(take_profit_pips, 6),
            trailing_stop_pips=round(trailing_stop_pips, 6),
            spread_pips=round(spread, 6),
            mid=safe_float(price.get("mid"), entry),
            bid=safe_float(price.get("bid"), entry),
            ask=safe_float(price.get("ask"), entry),
            entry_ref=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            rank_score=round(rank_score, 6),
            expected_r_multiple=round(expected_r, 6),
            reject_reason=reject_reason,
            model_target_direction=model_target_direction,
            execution_side_policy=bundle.execution_side_policy,
            source_stream=bundle.source_stream,
            model_feature_timeframe=bundle.feature_timeframe,
            model_feature_time_utc=feature_dt.isoformat() if feature_dt else "",
            model_threshold=round(calibration.threshold, 6),
            model_expected_move_atr=round(calibration.expected_move_atr, 6),
            model_raw_rank_score=round(raw_rank_score, 6),
            model_score_percentile=round(calibration.score_percentile, 6),
            model_atr240_pips=round(atr_pips, 6),
            model_momentum_30_atr=round(safe_float(row.get("momentum_30_atr"), 0.0), 6),
            pair_threshold_rank_score=round(safe_float(getattr(calibration, "pair_threshold_rank_score", 0.0), 0.0), 6),
            pair_report_rank_score=round(calibration.pair_report_rank_score, 6),
        )


class ForecastEngine:
    def __init__(self, cfg: Dict[str, Any], gateway: Optional[OandaGateway] = None):
        self.cfg = cfg
        self.gateway = gateway
        self.forecast_source = str(cfg.get("forecast_source") or "alignment").strip().lower()
        self.model_stream = ModelStreamForecaster(cfg) if self.forecast_source == "model_stream" else None
        self.alignment_rows = load_alignment_rows(cfg) if self.forecast_source != "model_stream" else {}

    def instruments(self, max_pairs: int = 0) -> List[str]:
        if self.model_stream is not None:
            return self.model_stream.instruments(max_pairs=max_pairs)
        available = set(list_candle_instruments())
        rows = [inst for inst in sorted(self.alignment_rows) if inst in available]
        if max_pairs > 0:
            rows = rows[:max_pairs]
        return rows

    def load_m1_overlay_frames(
        self,
        *,
        source: str,
        instruments: Sequence[str],
        overlay: M1OverlayScorer,
    ) -> Dict[str, pd.DataFrame]:
        frames: Dict[str, pd.DataFrame] = {}
        if not instruments:
            return frames
        if source == "oanda":
            if not self.gateway:
                raise RuntimeError("OANDA source requested without gateway.")
            for inst in instruments:
                try:
                    candles = self.gateway.get_candles(
                        inst,
                        overlay.oanda_candle_count,
                        granularity=overlay.oanda_candle_granularity,
                    )
                    frame = candles_to_frame(inst, candles)
                    if not frame.empty:
                        frames[inst] = add_base_features(frame, inst)
                except Exception:
                    continue
        else:
            for inst in instruments:
                try:
                    frame = read_candles(inst, max_rows=overlay.local_candle_count)
                    frames[inst] = add_base_features(frame, inst)
                except Exception:
                    continue
        return add_cross_currency_features(frames)

    @staticmethod
    def forecast_rejection_meta(forecasts: Sequence[Forecast]) -> Dict[str, Any]:
        rejected_by_reason: Dict[str, int] = {}
        rejected_preview: List[Dict[str, Any]] = []
        rejected = 0
        for forecast in forecasts:
            if not forecast.reject_reason:
                continue
            rejected += 1
            reason_code = forecast.reject_reason.split(":", 1)[0]
            rejected_by_reason[reason_code] = rejected_by_reason.get(reason_code, 0) + 1
            if len(rejected_preview) < 25:
                rejected_preview.append(
                    {
                        "instrument": forecast.instrument,
                        "event": forecast.event,
                        "direction": forecast.direction_name(),
                        "horizon_minutes": forecast.horizon_minutes,
                        "rank_score": forecast.rank_score,
                        "probability": forecast.probability,
                        "edge_pips": forecast.edge_pips,
                        "risk_pips": forecast.risk_pips,
                        "spread_pips": forecast.spread_pips,
                        "m1_hold_prob": forecast.m1_hold_prob,
                        "m1_quick_prob": forecast.m1_quick_prob,
                        "reject_reason": forecast.reject_reason,
                    }
                )
        return {
            "rejected_active_count": rejected,
            "rejected_by_reason": rejected_by_reason,
            "rejected_preview": rejected_preview,
        }

    def load_frames(
        self,
        *,
        source: str,
        instruments: Sequence[str],
    ) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, float]]]:
        frames: Dict[str, pd.DataFrame] = {}
        prices: Dict[str, Dict[str, float]] = {}
        if source == "oanda":
            if not self.gateway:
                raise RuntimeError("OANDA source requested without gateway.")
            candle_granularity = str(
                (self.cfg.get("model_stream") or {}).get("oanda_candle_granularity")
                or self.cfg.get("oanda_candle_granularity")
                or "M1"
            ).upper()
            raw_prices = self.gateway.get_prices(instruments)
            for inst in instruments:
                candles = self.gateway.get_candles(
                    inst,
                    safe_int(self.cfg.get("oanda_candle_count"), 650),
                    granularity=candle_granularity,
                )
                frame = candles_to_frame(inst, candles)
                if frame.empty:
                    continue
                frames[inst] = add_base_features(frame, inst)
                prices[inst] = price_from_oanda(raw_prices.get(inst, {}), safe_float(frame["close"].iloc[-1]), inst)
        else:
            for inst in instruments:
                try:
                    frame = read_candles(inst, max_rows=safe_int(self.cfg.get("local_candle_count"), 1200))
                    frames[inst] = add_base_features(frame, inst)
                    prices[inst] = price_from_frame(frame, inst)
                except Exception:
                    continue
        frames = add_cross_currency_features(frames)
        return frames, prices

    def forecast(
        self,
        *,
        source: str,
        max_pairs: int = 0,
    ) -> Tuple[List[Forecast], Dict[str, Any]]:
        instruments = self.instruments(max_pairs=max_pairs)
        min_instruments = safe_int(self.cfg.get("min_instruments_required"), 0)
        if min_instruments > 0 and len(instruments) < min_instruments:
            raise RuntimeError(
                f"Instrument universe below required minimum: {len(instruments)}<{min_instruments}"
            )
        frames, prices = self.load_frames(source=source, instruments=instruments)
        generated = iso_utc()
        if self.model_stream is not None:
            forecasts, model_meta = self.model_stream.forecast(frames, prices, generated)
            if self.model_stream.m1_overlay.enabled:
                overlay_instruments = self.model_stream.m1_overlay_target_instruments(
                    forecasts,
                    frames.keys(),
                )
                overlay_frames = self.load_m1_overlay_frames(
                    source=source,
                    instruments=overlay_instruments,
                    overlay=self.model_stream.m1_overlay,
                )
                overlay_meta = self.model_stream.apply_m1_overlay(forecasts, overlay_frames, prices)
                model_meta["m1_overlay"] = overlay_meta
                model_meta.update(self.forecast_rejection_meta(forecasts))
            meta = {
                "generated_utc": generated,
                "source": source,
                "forecast_source": self.forecast_source,
                "instruments_requested": len(instruments),
                "instruments_loaded": len(frames),
                "forecast_count": len(forecasts),
                "tradable_forecast_count": sum(1 for item in forecasts if not item.reject_reason),
                **model_meta,
            }
            return forecasts, meta
        forecasts: List[Forecast] = []
        rejected = 0
        rejected_by_reason: Dict[str, int] = {}
        rejected_preview: List[Dict[str, Any]] = []
        for inst in sorted(frames):
            frame = frames[inst]
            if frame.empty:
                continue
            events = {event.name: event for event in event_defs(frame)}
            active_count = 0
            for row in self.alignment_rows.get(inst, []):
                event = events.get(row.event)
                if event is None:
                    continue
                active = bool(event.series.fillna(False).astype(bool).iloc[-1])
                if not active:
                    continue
                forecast = self._score_active_row(row, frame, prices.get(inst, price_from_frame(frame, inst)), generated)
                forecasts.append(forecast)
                active_count += 1
                if forecast.reject_reason:
                    rejected += 1
                    reason_code = forecast.reject_reason.split(":", 1)[0]
                    rejected_by_reason[reason_code] = rejected_by_reason.get(reason_code, 0) + 1
                    if len(rejected_preview) < 25:
                        rejected_preview.append(
                            {
                                "instrument": forecast.instrument,
                                "event": forecast.event,
                                "direction": forecast.direction_name(),
                                "horizon_minutes": forecast.horizon_minutes,
                                "rank_score": forecast.rank_score,
                                "probability": forecast.probability,
                                "edge_pips": forecast.edge_pips,
                                "risk_pips": forecast.risk_pips,
                                "spread_pips": forecast.spread_pips,
                                "reject_reason": forecast.reject_reason,
                            }
                        )
                else:
                    pass
                if active_count >= safe_int(self.cfg.get("max_active_events_per_pair"), 8):
                    break
        forecasts.sort(key=lambda item: item.rank_score, reverse=True)
        meta = {
            "generated_utc": generated,
            "source": source,
            "instruments_requested": len(instruments),
            "instruments_loaded": len(frames),
            "forecast_count": len(forecasts),
            "tradable_forecast_count": sum(1 for item in forecasts if not item.reject_reason),
            "rejected_active_count": rejected,
            "rejected_by_reason": rejected_by_reason,
            "rejected_preview": rejected_preview,
        }
        return forecasts, meta

    def _score_active_row(
        self,
        row: AlignmentRow,
        frame: pd.DataFrame,
        price: Dict[str, float],
        generated_utc: str,
    ) -> Forecast:
        cfg = self.cfg
        inst = row.instrument
        spread = max(0.0, safe_float(price.get("spread_pips")))
        cost_pips = spread * safe_float(cfg.get("spread_cost_multiplier"), 1.25) + slippage_for(cfg, inst)
        edge_pips = row.mean_net_pips - cost_pips
        probability = shrink_probability(
            row.win_rate,
            row.sample_count,
            safe_float(cfg.get("probability_prior"), 0.50),
            safe_float(cfg.get("probability_prior_weight"), 250.0),
        )
        atr30 = safe_float(frame.get("atr_30_pips", pd.Series([0.0])).iloc[-1], 0.0)
        risk_pips = max(
            row.mean_mae_pips * safe_float(cfg.get("mae_stop_multiplier"), 1.20),
            atr30 * safe_float(cfg.get("atr_stop_multiplier"), 1.15),
            spread * safe_float(cfg.get("min_stop_spread_multiple"), 4.0),
            0.1,
        )
        pf_quality = min(1.5, max(0.0, row.profit_factor - 1.0)) / 1.5
        sample_confidence = min(1.0, math.log1p(max(0, row.sample_count)) / math.log1p(2500.0))
        prob_edge = max(0.0, probability - 0.50) * 2.0
        expected_r = edge_pips / max(risk_pips, 0.1)
        rank_score = expected_r * (0.40 + 0.60 * prob_edge) * (0.50 + 0.50 * pf_quality)
        rank_score *= (0.30 + 0.70 * row.positive_week_rate) * (0.35 + 0.65 * sample_confidence)
        rank_score = max(0.0, rank_score)

        def reject(code: str, detail: str) -> str:
            return f"{code}:{detail}"

        reject_reason = ""
        if row.sample_count < safe_int(cfg.get("min_sample_count"), 75):
            reject_reason = reject("sample_below_gate", f"{row.sample_count}<{safe_int(cfg.get('min_sample_count'), 75)}")
        elif row.week_count < safe_int(cfg.get("min_week_count"), 3):
            reject_reason = reject("week_count_below_gate", f"{row.week_count}<{safe_int(cfg.get('min_week_count'), 3)}")
        elif row.positive_week_rate < safe_float(cfg.get("min_positive_week_rate"), 0.45):
            reject_reason = reject(
                "positive_week_rate_below_gate",
                f"{row.positive_week_rate:.3f}<{safe_float(cfg.get('min_positive_week_rate'), 0.45):.3f}",
            )
        elif row.profit_factor < safe_float(cfg.get("min_profit_factor"), 1.05):
            reject_reason = reject(
                "profit_factor_below_gate",
                f"{row.profit_factor:.3f}<{safe_float(cfg.get('min_profit_factor'), 1.05):.3f}",
            )
        elif spread > max_spread_for(cfg, inst):
            reject_reason = reject("spread_above_gate", f"{spread:.2f}>{max_spread_for(cfg, inst):.2f}")
        elif edge_pips < safe_float(cfg.get("min_edge_pips"), 0.25):
            reject_reason = reject("edge_below_gate", f"{edge_pips:.2f}<{safe_float(cfg.get('min_edge_pips'), 0.25):.2f}")
        elif probability < safe_float(cfg.get("min_probability"), 0.515):
            reject_reason = reject(
                "probability_below_gate",
                f"{probability:.3f}<{safe_float(cfg.get('min_probability'), 0.515):.3f}",
            )
        elif spread / max(edge_pips, 0.01) > safe_float(cfg.get("max_spread_to_edge_ratio"), 0.55):
            reject_reason = reject(
                "spread_edge_ratio_above_gate",
                f"{(spread / max(edge_pips, 0.01)):.3f}>{safe_float(cfg.get('max_spread_to_edge_ratio'), 0.55):.3f}",
            )
        elif rank_score < safe_float(cfg.get("min_rank_score"), 0.18):
            reject_reason = reject("score_below_gate", f"{rank_score:.3f}<{safe_float(cfg.get('min_rank_score'), 0.18):.3f}")

        entry = safe_float(price["ask"] if row.direction > 0 else price["bid"])
        pip = pips_size(inst)
        stop_pips = risk_pips
        take_profit_pips = max(
            edge_pips * safe_float(cfg.get("take_profit_edge_capture"), 0.85),
            risk_pips * safe_float(cfg.get("take_profit_min_r_multiple"), 1.10),
        )
        trailing_stop_pips = max(
            risk_pips * safe_float(cfg.get("trailing_stop_r_multiple"), 0.85),
            min_trailing_stop_for(cfg, inst, spread),
        )
        if row.direction > 0:
            stop_loss = entry - stop_pips * pip
            take_profit = entry + take_profit_pips * pip
        else:
            stop_loss = entry + stop_pips * pip
            take_profit = entry - take_profit_pips * pip

        return Forecast(
            forecast_id=f"pfr_{uuid.uuid4().hex[:20]}",
            generated_utc=generated_utc,
            instrument=inst,
            event=row.event,
            family=row.family,
            direction=row.direction,
            horizon_minutes=row.horizon_minutes,
            sample_count=row.sample_count,
            week_count=row.week_count,
            probability=round(probability, 6),
            win_rate_raw=row.win_rate,
            positive_week_rate=row.positive_week_rate,
            profit_factor=row.profit_factor,
            mean_net_pips=row.mean_net_pips,
            edge_pips=round(edge_pips, 6),
            risk_pips=round(risk_pips, 6),
            stop_pips=round(stop_pips, 6),
            take_profit_pips=round(take_profit_pips, 6),
            trailing_stop_pips=round(trailing_stop_pips, 6),
            spread_pips=round(spread, 6),
            mid=safe_float(price["mid"]),
            bid=safe_float(price["bid"]),
            ask=safe_float(price["ask"]),
            entry_ref=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            rank_score=round(rank_score, 6),
            expected_r_multiple=round(expected_r, 6),
            reject_reason=reject_reason,
        )


class PortfolioController:
    def __init__(
        self,
        cfg: Dict[str, Any],
        *,
        mode: str,
        source: str,
        gateway: Optional[OandaGateway] = None,
        execute_live: bool = False,
    ):
        self.cfg = cfg
        self.mode = mode
        self.source = source
        self.gateway = gateway
        self.execute_live = execute_live
        self.broker_tag = str(cfg.get("bot_id") or "primary_forecast_rotation")
        self.data_dir = Path(str(cfg.get("data_dir") or DATA_DIR))
        self.state_path = self.data_dir / "state.json"
        self.latest_forecasts_path = self.data_dir / "latest_forecasts.csv"
        self.latest_decision_path = self.data_dir / "latest_decision.json"
        self.action_journal = self.data_dir / "actions.csv"
        self.state = self._load_state()
        self.live_trades_cache: List[Dict[str, Any]] = []
        self.live_account_summary: Dict[str, Any] = {}
        self.live_positions_index: Dict[str, PaperPosition] = {}
        self.forecasts_by_instrument: Dict[str, Forecast] = {}
        self.instrument_margin_rates: Dict[str, float] = {}
        self.instrument_price_precisions: Dict[str, int] = {}
        self.entry_blocks: List[Dict[str, Any]] = []
        self.current_meta: Dict[str, Any] = {}
        self.exit_fade_counts: Dict[str, int] = {}

    def _load_state(self) -> Dict[str, Any]:
        state = read_json(
            self.state_path,
            {
                "paper_equity": safe_float(self.cfg.get("paper_starting_equity"), 10000.0),
                "paper_high_water": safe_float(self.cfg.get("paper_starting_equity"), 10000.0),
                "day_start_equity": safe_float(self.cfg.get("paper_starting_equity"), 10000.0),
                "day_start_date": utc_now().date().isoformat(),
                "paper_positions": [],
                "closed_positions": [],
                "cooldowns": {},
                "pair_daily_pnl": {},
                "pair_total_pnl": {},
                "pair_loss_streak": {},
                "replacement_times": [],
                "live_forecast_by_id": {},
            },
        )
        if not isinstance(state, dict):
            state = {}
        state.setdefault("paper_positions", [])
        state.setdefault("closed_positions", [])
        state.setdefault("cooldowns", {})
        state.setdefault("pair_daily_pnl", {})
        state.setdefault("pair_total_pnl", {})
        state.setdefault("pair_loss_streak", {})
        state.setdefault("replacement_times", [])
        state.setdefault("live_forecast_by_id", {})
        state.setdefault("paper_equity", safe_float(self.cfg.get("paper_starting_equity"), 10000.0))
        state.setdefault("paper_high_water", state["paper_equity"])
        state.setdefault("day_start_date", utc_now().date().isoformat())
        state.setdefault("day_start_equity", state["paper_equity"])
        return state

    def save_state(self) -> None:
        atomic_write_json(self.state_path, self.state)

    def _broker_mode(self) -> bool:
        return self.mode in BROKER_MODES

    def run_cycle(self, forecasts: List[Forecast], meta: Dict[str, Any]) -> Dict[str, Any]:
        self._roll_day_if_needed()
        self.current_meta = meta if isinstance(meta, dict) else {}
        prices: Dict[str, Forecast] = {}
        for forecast in forecasts:
            prices.setdefault(forecast.instrument, forecast)
        self.forecasts_by_instrument = prices
        if self._broker_mode():
            self._refresh_broker_margin_rates()
            self._enrich_broker_conversion_prices(prices)
            self.forecasts_by_instrument = prices
            self._refresh_live_state(prices)
            open_positions = list(self.live_positions_index.values())
        else:
            open_positions = self._open_positions()
            self._mark_paper_positions(prices)
            open_positions = self._open_positions()
        self.entry_blocks = []
        exits = self._exit_actions(open_positions, forecasts)
        entries = self._entry_actions(open_positions, forecasts, exits)
        actions = exits + entries
        for action in actions:
            self._apply_action(action, forecasts)
        if self._broker_mode() and self.cfg.get("refresh_live_state_after_actions", False):
            self._refresh_live_state(prices)
        decision = {
            "generated_utc": iso_utc(),
            "mode": self.mode,
            "source": self.source,
            "meta": meta,
            "open_positions": [asdict(pos) for pos in self._open_positions()],
            "live_managed_positions": [asdict(pos) for pos in self.live_positions_index.values()] if self._broker_mode() else [],
            "live_open_trade_count": len(self.live_trades_cache) if self._broker_mode() else 0,
            "actions": actions,
            "entry_blocks": self.entry_blocks[: safe_int(self.cfg.get("journal_top_candidates"), 50)],
            "top_forecasts": [self._forecast_log_row(forecast) for forecast in forecasts[: safe_int(self.cfg.get("journal_top_candidates"), 50)]],
            "safety": self._safety_snapshot(),
        }
        self._write_forecasts(forecasts)
        atomic_write_json(self.latest_decision_path, decision)
        self.save_state()
        return decision

    def _roll_day_if_needed(self) -> None:
        today = utc_now().date().isoformat()
        if self.state.get("day_start_date") != today:
            self.state["day_start_date"] = today
            self.state["day_start_equity"] = safe_float(self.state.get("paper_equity"), 0.0)
            self.state["pair_daily_pnl"] = {}

    def _open_positions(self) -> List[PaperPosition]:
        positions: List[PaperPosition] = []
        for item in self.state.get("paper_positions", []) or []:
            try:
                positions.append(PaperPosition(**item))
            except Exception:
                continue
        return positions

    def _write_positions(self, positions: List[PaperPosition]) -> None:
        self.state["paper_positions"] = [asdict(pos) for pos in positions]

    def _refresh_live_state(self, forecast_by_instrument: Dict[str, Forecast]) -> None:
        if not self.gateway:
            self.live_trades_cache = []
            self.live_account_summary = {}
            self.live_positions_index = {}
            return
        self.live_account_summary = self.gateway.get_account_summary()
        self._refresh_broker_margin_rates()
        self.live_trades_cache = self.gateway.get_open_trades()
        positions: Dict[str, PaperPosition] = {}
        open_trade_ids = {str(trade.get("id") or "") for trade in self.live_trades_cache if trade.get("id")}
        live_trade_by_id = self.state.setdefault("live_trade_by_id", {})
        for trade in self.live_trades_cache:
            trade_id = str(trade.get("id") or "")
            tag = str(((trade.get("clientExtensions") or {}).get("tag")) or "")
            mapped = live_trade_by_id.get(trade_id) if trade_id else None
            if tag == self.broker_tag and trade_id and not mapped:
                forecast_id = str(((trade.get("clientExtensions") or {}).get("id")) or "")
                live_trade_by_id[trade_id] = {
                    "forecast_id": forecast_id,
                    "instrument": str(trade.get("instrument") or ""),
                    "units": str(trade.get("currentUnits") or trade.get("initialUnits") or ""),
                    "opened_utc": str(trade.get("openTime") or ""),
                    "mapped_utc": iso_utc(),
                    "source": "client_extensions",
                }
                mapped = live_trade_by_id.get(trade_id)
            if not mapped:
                inferred = self._infer_live_trade_mapping(trade)
                if inferred and trade_id:
                    live_trade_by_id[trade_id] = inferred
                    mapped = inferred
            if tag != self.broker_tag and not mapped:
                continue
            position = self._live_trade_to_position(trade, forecast_by_instrument)
            if position:
                positions[position.position_id] = position
        closed_mapped_trades = {
            str(trade_id): data
            for trade_id, data in live_trade_by_id.items()
            if str(trade_id) and str(trade_id) not in open_trade_ids
        }
        for trade_id, mapped in closed_mapped_trades.items():
            if isinstance(mapped, dict):
                self._journal_broker_side_close(trade_id, mapped)
                cooldown_minutes = safe_float(self.cfg.get("pair_cooldown_minutes_after_broker_side_close"), 0.0)
                instrument = str(mapped.get("instrument") or "")
                if instrument and cooldown_minutes > 0:
                    self._set_pair_cooldown(instrument, cooldown_minutes, "broker_side_close_detected")
        self.state["live_trade_by_id"] = {
            str(trade_id): data for trade_id, data in live_trade_by_id.items() if str(trade_id) in open_trade_ids
        }
        self.live_positions_index = positions
        nav = safe_float(self.live_account_summary.get("NAV") or self.live_account_summary.get("nav"), 0.0)
        if nav > 0:
            account_id = str(self.gateway.account_id if self.gateway else "")
            today = utc_now().date().isoformat()
            if self.state.get("live_baseline_account_id") != account_id:
                self.state["live_baseline_account_id"] = account_id
                self.state["live_baseline_utc"] = iso_utc()
                self.state["day_start_date"] = today
                self.state["day_start_equity"] = nav
                self.state["paper_high_water"] = nav
            else:
                day_start = safe_float(self.state.get("day_start_equity"), 0.0)
                high_water = safe_float(self.state.get("paper_high_water"), 0.0)
                if day_start <= 0 or day_start / nav < 0.50 or day_start / nav > 2.0:
                    self.state["day_start_date"] = today
                    self.state["day_start_equity"] = nav
                    self.state["live_day_start_repaired_utc"] = iso_utc()
                if high_water <= 0 or high_water / nav < 0.50 or high_water / nav > 2.0:
                    self.state["paper_high_water"] = nav
                    self.state["live_high_water_repaired_utc"] = iso_utc()
            self.state["paper_equity"] = nav
            self.state["paper_high_water"] = max(safe_float(self.state.get("paper_high_water"), nav), nav)
            if self.state.get("day_start_date") == today:
                self.state.setdefault("day_start_equity", nav)

    def _refresh_broker_margin_rates(self) -> None:
        if not self.cfg.get("use_broker_margin_rates", False) or not self.gateway:
            return
        if self.instrument_margin_rates:
            return
        try:
            instruments = self.gateway.get_instruments()
        except Exception as exc:
            self.state["broker_margin_rate_error"] = f"{type(exc).__name__}: {exc}"
            return
        rates: Dict[str, float] = {}
        precisions: Dict[str, int] = {}
        for item in instruments:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "")
            rate = safe_float(item.get("marginRate"), 0.0)
            if name and rate > 0:
                rates[name] = rate
            precision = safe_int(item.get("displayPrecision"), -1)
            if name and precision >= 0:
                precisions[name] = precision
        if rates:
            self.instrument_margin_rates = rates
            self.state["broker_margin_rates"] = {key: round(value, 6) for key, value in sorted(rates.items())}
            self.state["broker_margin_rates_updated_utc"] = iso_utc()
        if precisions:
            self.instrument_price_precisions = precisions
            self.state["broker_price_precisions"] = {key: int(value) for key, value in sorted(precisions.items())}
            self.state["broker_price_precisions_updated_utc"] = iso_utc()

    def _margin_rate_for_instrument(self, instrument: str) -> float:
        if self.cfg.get("use_broker_margin_rates", False):
            rate = safe_float(self.instrument_margin_rates.get(instrument), 0.0)
            if rate > 0:
                return rate
            stored = self.state.get("broker_margin_rates") if isinstance(self.state, dict) else {}
            if isinstance(stored, dict):
                rate = safe_float(stored.get(instrument), 0.0)
                if rate > 0:
                    return rate
        return safe_float(self.cfg.get("default_margin_rate"), 0.0333)

    def _price_precision_for_instrument(self, instrument: str) -> int:
        precision = safe_int(self.instrument_price_precisions.get(instrument), -1)
        if precision >= 0:
            return precision
        stored = self.state.get("broker_price_precisions") if isinstance(self.state, dict) else {}
        if isinstance(stored, dict):
            precision = safe_int(stored.get(instrument), -1)
            if precision >= 0:
                return precision
        return price_precision(instrument)

    def _available_broker_instruments(self) -> set[str]:
        available = set(self.instrument_margin_rates)
        stored = self.state.get("broker_margin_rates") if isinstance(self.state, dict) else {}
        if isinstance(stored, dict):
            available.update(str(item) for item in stored if item)
        return available

    def _enrich_broker_conversion_prices(self, prices: Dict[str, Forecast]) -> None:
        if not self.gateway:
            return
        available = self._available_broker_instruments()
        requested: List[str] = []
        missing_quotes: List[str] = []
        for instrument in list(prices):
            base, quote = instrument_parts(instrument)
            if not quote or quote == "USD" or base == "USD":
                continue
            direct = f"{quote}_USD"
            inverse = f"USD_{quote}"
            if direct in prices or inverse in prices:
                continue
            if direct in available:
                requested.append(direct)
            elif inverse in available:
                requested.append(inverse)
            else:
                missing_quotes.append(quote)
        requested = sorted(set(requested))
        if missing_quotes:
            self.state["broker_conversion_missing_quotes"] = sorted(set(missing_quotes))
        else:
            self.state.pop("broker_conversion_missing_quotes", None)
        if not requested:
            return
        try:
            raw_prices = self.gateway.get_prices(requested)
        except Exception as exc:
            self.state["broker_conversion_price_error"] = f"{type(exc).__name__}: {exc}"
            return
        added: Dict[str, float] = {}
        generated = iso_utc()
        for instrument, raw_price in raw_prices.items():
            current = price_from_oanda(raw_price, 0.0, instrument)
            mid = safe_float(current.get("mid"), 0.0)
            if mid <= 0:
                continue
            prices[instrument] = Forecast(
                forecast_id=f"broker_conversion_{instrument}",
                generated_utc=str(raw_price.get("time") or generated),
                instrument=instrument,
                event="broker_conversion_price",
                family="broker_price",
                direction=0,
                horizon_minutes=0,
                sample_count=0,
                week_count=0,
                probability=0.0,
                win_rate_raw=0.0,
                positive_week_rate=0.0,
                profit_factor=0.0,
                mean_net_pips=0.0,
                edge_pips=0.0,
                risk_pips=0.0,
                stop_pips=0.0,
                take_profit_pips=0.0,
                trailing_stop_pips=0.0,
                spread_pips=safe_float(current.get("spread_pips"), 0.0),
                mid=mid,
                bid=safe_float(current.get("bid"), mid),
                ask=safe_float(current.get("ask"), mid),
                entry_ref=mid,
                stop_loss=0.0,
                take_profit=0.0,
                rank_score=0.0,
                expected_r_multiple=0.0,
                reject_reason="broker_conversion_price",
                source_stream="broker_price",
            )
            added[instrument] = round(mid, 8)
        if added:
            previous = self.state.get("broker_conversion_prices")
            if isinstance(previous, dict):
                previous.update(added)
                self.state["broker_conversion_prices"] = dict(sorted(previous.items()))
            else:
                self.state["broker_conversion_prices"] = dict(sorted(added.items()))
            self.state["broker_conversion_prices_updated_utc"] = generated
            self.state.pop("broker_conversion_price_error", None)

    def _set_pair_cooldown(self, instrument: str, minutes: float, reason: str = "") -> None:
        if not instrument or minutes <= 0:
            return
        cooldown_until = utc_now() + timedelta(minutes=float(minutes))
        cooldowns = self.state.setdefault("cooldowns", {})
        existing_text = cooldowns.get(instrument)
        if existing_text:
            try:
                existing = datetime.fromisoformat(str(existing_text))
                if existing.tzinfo is None:
                    existing = existing.replace(tzinfo=timezone.utc)
                if existing > cooldown_until:
                    return
            except Exception:
                pass
        cooldowns[instrument] = cooldown_until.isoformat()
        if reason:
            self.state.setdefault("cooldown_reasons", {})[instrument] = reason

    def _record_closed_pnl(
        self,
        instrument: str,
        realized_usd: float,
        *,
        risk_usd: float = 0.0,
        reason: str = "",
    ) -> None:
        if not instrument:
            return
        realized = safe_float(realized_usd, 0.0)
        pair_daily = self.state.setdefault("pair_daily_pnl", {})
        pair_total = self.state.setdefault("pair_total_pnl", {})
        pair_daily[instrument] = round(safe_float(pair_daily.get(instrument), 0.0) + realized, 6)
        pair_total[instrument] = round(safe_float(pair_total.get(instrument), 0.0) + realized, 6)
        streaks = self.state.setdefault("pair_loss_streak", {})
        if realized < 0:
            streaks[instrument] = safe_int(streaks.get(instrument), 0) + 1
            self._set_pair_cooldown(
                instrument,
                safe_float(self.cfg.get("pair_cooldown_minutes_after_loss"), 30.0),
                "loss_close",
            )
            streak_limit = safe_int(self.cfg.get("pair_loss_streak_cooldown_trades"), 0)
            if streak_limit > 0 and safe_int(streaks.get(instrument), 0) >= streak_limit:
                self._set_pair_cooldown(
                    instrument,
                    safe_float(self.cfg.get("pair_loss_cooldown_minutes"), 300.0),
                    "loss_streak_cooldown",
                )
            nav = max(self._account_nav(), 1e-9)
            large_loss_pct = safe_float(self.cfg.get("pair_large_loss_cooldown_pct"), 0.0)
            realized_loss_pct = abs(realized) / nav * 100.0
            risk_loss_pct = abs(realized) / max(safe_float(risk_usd), 1e-9) * 100.0 if risk_usd > 0 else 0.0
            if large_loss_pct > 0 and realized_loss_pct >= large_loss_pct:
                self._set_pair_cooldown(
                    instrument,
                    safe_float(self.cfg.get("pair_loss_cooldown_minutes"), 300.0),
                    f"large_loss_cooldown {realized_loss_pct:.2f}%_nav",
                )
            elif large_loss_pct > 0 and risk_loss_pct >= 100.0:
                self._set_pair_cooldown(
                    instrument,
                    safe_float(self.cfg.get("pair_loss_cooldown_minutes"), 300.0),
                    f"full_risk_loss_cooldown {risk_loss_pct:.1f}%_risk",
                )
        elif realized > 0:
            streaks[instrument] = 0
        if reason:
            self.state.setdefault("last_close_reasons", {})[instrument] = reason

    def _journal_broker_side_close(self, trade_id: str, mapped: Dict[str, Any]) -> None:
        close_detail = self._broker_close_detail_for_trade(trade_id)
        row = {
            "time_utc": iso_utc(),
            "mode": self.mode,
            "action": "close",
            "position_id": f"live_{trade_id}",
            "forecast_id": mapped.get("forecast_id", ""),
            "instrument": mapped.get("instrument", ""),
            "direction": direction_name(1 if safe_int(mapped.get("units"), 0) > 0 else -1),
            "units": mapped.get("units", ""),
            "reason": "broker_side_close_detected",
            "status": "broker_closed_external",
            "broker_trade_id": trade_id,
        }
        row.update(close_detail)
        if "realized_pl" in close_detail:
            self._record_closed_pnl(
                str(mapped.get("instrument") or ""),
                safe_float(close_detail.get("realized_pl"), 0.0),
                reason="broker_side_close_detected",
            )
        append_csv(self.action_journal, row, self._action_journal_fields())

    def _broker_close_detail_for_trade(self, trade_id: str) -> Dict[str, Any]:
        if not self.gateway:
            return {}
        last_id = safe_int(
            self.live_account_summary.get("lastTransactionID")
            or self.live_account_summary.get("lastTransactionId"),
            0,
        )
        if last_id <= 0:
            return {}
        lookback = max(25, safe_int(self.cfg.get("broker_close_transaction_lookback_ids"), 250))
        since_id = max(0, last_id - lookback)
        try:
            data = self.gateway.get_transactions_since(str(since_id))
        except Exception as exc:
            return {"broker_response": f"broker_close_detail_error {type(exc).__name__}: {exc}"[:1500]}
        transactions = data.get("transactions") if isinstance(data, dict) else []
        if not isinstance(transactions, list):
            return {}
        for tx in reversed(transactions):
            if not isinstance(tx, dict) or str(tx.get("type") or "") != "ORDER_FILL":
                continue
            closed_items = tx.get("tradesClosed")
            if isinstance(closed_items, list):
                for closed in closed_items:
                    if isinstance(closed, dict) and str(closed.get("tradeID") or "") == str(trade_id):
                        return {
                            "realized_pl": closed.get("realizedPL", tx.get("pl", "")),
                            "financing": closed.get("financing", ""),
                            "broker_close_reason": tx.get("reason", ""),
                            "broker_transaction_id": tx.get("id", ""),
                        }
            reduced = tx.get("tradeReduced")
            if isinstance(reduced, dict) and str(reduced.get("tradeID") or "") == str(trade_id):
                return {
                    "realized_pl": reduced.get("realizedPL", tx.get("pl", "")),
                    "financing": reduced.get("financing", ""),
                    "broker_close_reason": tx.get("reason", ""),
                    "broker_transaction_id": tx.get("id", ""),
                }
        return {}

    @staticmethod
    def _action_journal_fields() -> List[str]:
        return [
            "time_utc",
            "mode",
            "action",
            "position_id",
            "forecast_id",
            "instrument",
            "direction",
            "planned_units",
            "units",
            "event",
            "family",
            "horizon_minutes",
            "rank_score",
            "probability",
            "edge_pips",
            "risk_pips",
            "expected_profit_usd",
            "take_profit_usd",
            "risk_usd_estimate",
            "estimated_margin_required",
            "profit_per_margin",
            "allocation_score",
            "open_risk_pct_before",
            "open_risk_pct_after",
            "target_open_risk_pct_effective",
            "max_open_risk_pct_effective",
            "drawdown_risk_multiplier",
            "margin_used_pct_after",
            "risk_guard_reject_reason",
            "currency_risk_violations",
            "entry_ref",
            "stop_loss",
            "take_profit",
            "trailing_stop_pips",
            "reason",
            "status",
            "replacement_position_id",
            "replacement_instrument",
            "replacement_direction",
            "replacement_reason",
            "broker_trade_id",
            "broker_order_id",
            "broker_fill_id",
            "broker_cancel_reason",
            "realized_pips_estimate",
            "realized_pl",
            "financing",
            "broker_close_reason",
            "broker_transaction_id",
            "unrealized_usd_before_close",
            "broker_response",
        ]

    def _live_trade_to_position(
        self,
        trade: Dict[str, Any],
        forecast_by_instrument: Dict[str, Forecast],
    ) -> Optional[PaperPosition]:
        trade_id = str(trade.get("id") or "")
        inst = str(trade.get("instrument") or "")
        if not trade_id or not inst:
            return None
        units = int(safe_float(trade.get("currentUnits") or trade.get("initialUnits"), 0.0))
        if units == 0:
            return None
        direction = 1 if units > 0 else -1
        entry = safe_float(trade.get("price"), 0.0)
        mapped = (self.state.get("live_trade_by_id") or {}).get(trade_id, {}) or {}
        forecast_id = str(((trade.get("clientExtensions") or {}).get("id")) or mapped.get("forecast_id") or "")
        stored = {}
        if forecast_id:
            stored = (self.state.get("live_forecast_by_id") or {}).get(forecast_id, {}) or {}
        mapped_risk_usd = safe_float(mapped.get("risk_usd_estimate"), 0.0) if isinstance(mapped, dict) else 0.0
        mapped_margin = safe_float(mapped.get("estimated_margin_required"), 0.0) if isinstance(mapped, dict) else 0.0
        current = next(
            (
                forecast
                for forecast in forecast_by_instrument.values()
                if forecast.instrument == inst and forecast.direction == direction
            ),
            None,
        )
        pip = pips_size(inst)
        stop_order = trade.get("stopLossOrder") or {}
        take_order = trade.get("takeProfitOrder") or {}
        trail_order = trade.get("trailingStopLossOrder") or {}
        stop_loss = safe_float(stop_order.get("price"), 0.0)
        take_profit = safe_float(take_order.get("price"), 0.0)
        trailing_stop_pips = safe_float(trail_order.get("distance"), 0.0) / pip if trail_order else 0.0
        risk_pips = abs(entry - stop_loss) / pip if stop_loss > 0 else safe_float(stored.get("risk_pips"), 0.0)
        horizon = safe_int(stored.get("horizon_minutes"), safe_int(current.horizon_minutes if current else 60))
        opened_score = safe_float(stored.get("rank_score"), safe_float(current.rank_score if current else 0.0))
        mid = safe_float(current.mid if current else entry, entry)
        unrealized_pips = (mid - entry) / pip * direction if entry > 0 else 0.0
        return PaperPosition(
            position_id=f"live_{trade_id}",
            opened_utc=str(trade.get("openTime") or iso_utc()),
            instrument=inst,
            direction=direction,
            units=units,
            entry_price=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop_pips=trailing_stop_pips,
            horizon_minutes=horizon,
            opened_score=opened_score,
            event=str(stored.get("event") or (current.event if current else "")),
            family=str(stored.get("family") or (current.family if current else "")),
            expected_edge_pips=safe_float(stored.get("edge_pips"), safe_float(current.edge_pips if current else 0.0)),
            risk_pips=risk_pips,
            risk_usd_estimate=safe_float(stored.get("risk_usd_estimate"), mapped_risk_usd),
            estimated_margin_required=safe_float(stored.get("estimated_margin_required"), mapped_margin),
            last_score=safe_float(current.rank_score if current else 0.0),
            unrealized_pips=round(unrealized_pips, 6),
            unrealized_usd=safe_float(trade.get("unrealizedPL"), 0.0),
        )

    def _infer_live_trade_mapping(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        trade_id = str(trade.get("id") or "")
        inst = str(trade.get("instrument") or "")
        units = int(safe_float(trade.get("currentUnits") or trade.get("initialUnits"), 0.0))
        if not trade_id or not inst or units == 0:
            return {}
        direction_text = direction_name(1 if units > 0 else -1)
        open_dt = parse_broker_time(trade.get("openTime"))
        mapped_forecast_ids = {
            str(data.get("forecast_id") or "")
            for data in (self.state.get("live_trade_by_id") or {}).values()
            if isinstance(data, dict)
        }
        candidates: List[Tuple[float, str, Dict[str, Any]]] = []
        for forecast_id, stored in (self.state.get("live_forecast_by_id") or {}).items():
            if not isinstance(stored, dict):
                continue
            if str(stored.get("instrument") or "") != inst:
                continue
            if str(stored.get("direction") or "") != direction_text:
                continue
            if str(forecast_id) in mapped_forecast_ids:
                continue
            generated = parse_broker_time(stored.get("generated_utc"))
            seconds = abs((open_dt - generated).total_seconds()) if open_dt and generated else 0.0
            if seconds <= 600.0:
                candidates.append((seconds, str(forecast_id), stored))
        if not candidates:
            return {}
        _, forecast_id, stored = sorted(
            candidates,
            key=lambda item: (item[0], -safe_float(item[2].get("rank_score"), 0.0)),
        )[0]
        return {
            "forecast_id": forecast_id,
            "instrument": inst,
            "units": str(units),
            "opened_utc": str(trade.get("openTime") or ""),
            "mapped_utc": iso_utc(),
            "source": "inferred_from_state",
        }

    def _mark_paper_positions(self, forecast_by_instrument: Dict[str, Forecast]) -> None:
        positions = self._open_positions()
        equity = safe_float(self.state.get("paper_equity"), 0.0)
        for pos in positions:
            forecast = forecast_by_instrument.get(pos.instrument)
            if not forecast:
                continue
            pip = pips_size(pos.instrument)
            mid = forecast.mid
            pnl_pips = (mid - pos.entry_price) / pip * pos.direction
            pip_value = pip_value_per_unit_usd(pos.instrument, forecast.mid, forecast_by_instrument)
            pos.unrealized_pips = round(pnl_pips, 6)
            pos.unrealized_usd = round(pnl_pips * pip_value * abs(pos.units), 6)
            same = [f for f in forecast_by_instrument.values() if f.instrument == pos.instrument and f.direction == pos.direction]
            pos.last_score = same[0].rank_score if same else 0.0
        realized_equity = equity
        self.state["paper_high_water"] = max(safe_float(self.state.get("paper_high_water"), realized_equity), realized_equity)
        self._write_positions(positions)

    def _exit_actions(self, positions: List[PaperPosition], forecasts: List[Forecast]) -> List[Dict[str, Any]]:
        actions: List[Dict[str, Any]] = []
        if (
            positions
            and self._broker_mode()
            and self.cfg.get("close_positions_on_kill_switch", False)
            and self._safety_snapshot().get("kill_switch_active")
        ):
            return [
                {
                    "action": "close",
                    "position_id": pos.position_id,
                    "instrument": pos.instrument,
                    "direction": direction_name(pos.direction),
                    "reason": "kill_switch_active",
                    "rank_score": pos.last_score,
                }
                for pos in positions
            ]
        best_same: Dict[Tuple[str, int], Forecast] = {}
        best_opposite: Dict[Tuple[str, int], Forecast] = {}
        for forecast in forecasts:
            same_key = (forecast.instrument, forecast.direction)
            if same_key not in best_same:
                best_same[same_key] = forecast
            opp_key = (forecast.instrument, -forecast.direction)
            if opp_key not in best_opposite:
                best_opposite[opp_key] = forecast
        active_position_ids = {str(pos.position_id) for pos in positions}
        for key in list(self.exit_fade_counts):
            if key not in active_position_ids:
                self.exit_fade_counts.pop(key, None)
        fade_confirm_cycles = max(1, safe_int(self.cfg.get("exit_fade_confirm_cycles"), 1))
        signal_gone_loser_policy = str(
            self.cfg.get("signal_gone_loser_policy") or "allow"
        ).strip().lower()
        signal_gone_loser_buffer = max(
            0.0,
            safe_float(self.cfg.get("signal_gone_loser_buffer_pips"), 0.0),
        )
        for pos in positions:
            age_minutes = self._age_minutes(pos.opened_utc)
            current = best_same.get((pos.instrument, pos.direction))
            opposite = best_opposite.get((pos.instrument, pos.direction))
            reason = ""
            min_hold = safe_float(self.cfg.get("min_hold_minutes"), 8.0)
            current_tradable = bool(current and not current.reject_reason and current.edge_pips > 0)
            current_economics_ok = True
            if current:
                current_economics_ok = self._forecast_economics_allows(
                    current,
                    pos.units,
                    allow_zero_margin=True,
                )
            score_faded = (current is None) or bool(current.reject_reason) or (
                current.rank_score < pos.opened_score * safe_float(self.cfg.get("hold_score_floor_fraction"), 0.45)
            )
            opposite_stronger = bool(
                opposite
                and not opposite.reject_reason
                and opposite.rank_score
                > max(
                    max(pos.last_score, pos.opened_score)
                    + safe_float(self.cfg.get("replacement_min_score_improvement"), 0.35),
                    max(pos.last_score, pos.opened_score)
                    * safe_float(self.cfg.get("opposite_min_score_multiplier"), 1.0),
                )
            )
            fade_key = str(pos.position_id)
            fade_basis = score_faded or opposite_stronger
            if fade_basis:
                fade_count = self.exit_fade_counts.get(fade_key, 0) + 1
            else:
                fade_count = 0
            self.exit_fade_counts[fade_key] = fade_count
            fade_confirmed = fade_count >= fade_confirm_cycles
            profit_lock_age = safe_float(self.cfg.get("profit_lock_min_age_minutes", min_hold), min_hold)
            profit_lock_pips = max(
                safe_float(self.cfg.get("profit_lock_min_pips"), 0.0),
                abs(pos.risk_pips) * safe_float(self.cfg.get("profit_lock_r_multiple"), 0.0),
            )
            profit_lock_hit = (
                age_minutes >= profit_lock_age
                and profit_lock_pips > 0
                and pos.unrealized_pips >= profit_lock_pips
                and fade_confirmed
            )
            signal_gone_exit_age = safe_float(self.cfg.get("signal_gone_exit_minutes", min_hold), min_hold)
            signal_gone_exit_age = max(
                signal_gone_exit_age,
                max(1, pos.horizon_minutes) * safe_float(self.cfg.get("signal_gone_min_horizon_fraction", 0.0), 0.0),
            )
            economics_exit_age = max(
                min_hold,
                max(1, pos.horizon_minutes) * safe_float(self.cfg.get("economics_exit_min_horizon_fraction", 0.0), 0.0),
            )
            soft_loss_exit_age = max(
                min_hold,
                max(1, pos.horizon_minutes) * safe_float(self.cfg.get("soft_loss_exit_min_horizon_fraction", 0.0), 0.0),
            )
            if pos.unrealized_pips <= -abs(pos.risk_pips):
                reason = "paper_stop_hit"
            elif pos.unrealized_pips >= abs(pos.take_profit - pos.entry_price) / pips_size(pos.instrument):
                reason = "paper_take_profit_hit"
            elif profit_lock_hit:
                reason = "profit_lock_churn"
            elif age_minutes >= signal_gone_exit_age and not current_tradable:
                losing_signal_gone = pos.unrealized_pips < -signal_gone_loser_buffer
                defer_loser = (
                    signal_gone_loser_policy in {
                        "defer_losers",
                        "defer_losers_unless_opposite",
                        "defer_losers_until_stop",
                    }
                    and losing_signal_gone
                )
                allow_opposite_override = (
                    signal_gone_loser_policy == "defer_losers_unless_opposite"
                    and opposite_stronger
                )
                if fade_confirmed and (not defer_loser or allow_opposite_override):
                    reason = "signal_no_longer_positive"
            elif age_minutes >= economics_exit_age and current_tradable and not current_economics_ok:
                reason = "economics_below_floor"
            elif (
                age_minutes >= soft_loss_exit_age
                and pos.unrealized_pips <= -abs(pos.risk_pips) * safe_float(self.cfg.get("soft_loss_r_multiple"), 1.0)
                and fade_confirmed
            ):
                reason = "soft_loss_signal_fade"
            elif age_minutes >= max(1, pos.horizon_minutes):
                hold_same_signal = (
                    bool(self.cfg.get("hold_same_signal_past_horizon", False))
                    and current_tradable
                    and current_economics_ok
                    and not score_faded
                    and not opposite_stronger
                )
                if not hold_same_signal:
                    reason = "horizon_expired"
            elif age_minutes >= min_hold:
                stale_age = age_minutes >= pos.horizon_minutes * safe_float(self.cfg.get("stale_horizon_fraction"), 0.80)
                if stale_age and score_faded and fade_confirmed:
                    reason = "stale_forecast_decay"
                elif opposite_stronger and fade_confirmed:
                    reason = "opposite_forecast_stronger"
            if reason:
                weak_exit_reasons = {
                    "signal_no_longer_positive",
                    "economics_below_floor",
                    "soft_loss_signal_fade",
                    "horizon_expired",
                    "stale_forecast_decay",
                }
                target_active_positions = (
                    safe_int(self.cfg.get("target_active_positions"), 0)
                    if self._broker_mode() and not self._use_position_count_cap()
                    else 0
                )
                if (
                    reason in weak_exit_reasons
                    and self.cfg.get("defer_weak_exits_when_under_target", False)
                    and target_active_positions > 0
                    and len(positions) <= target_active_positions
                ):
                    continue
                actions.append(
                    {
                        "action": "close",
                        "position_id": pos.position_id,
                        "instrument": pos.instrument,
                        "direction": direction_name(pos.direction),
                        "reason": reason,
                        "rank_score": pos.last_score,
                    }
                )
        return actions

    def _entry_actions(
        self,
        open_positions: List[PaperPosition],
        forecasts: List[Forecast],
        exits: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if self.mode == "live" and self.cfg.get("live_new_entries_enabled") is False:
            return []
        if self.mode == "demo" and self.cfg.get("demo_new_entries_enabled") is False:
            for forecast in forecasts[: max(1, safe_int(self.cfg.get("journal_top_candidates"), 50))]:
                self._block_forecast(
                    forecast,
                    "demo_new_entries_disabled",
                    "shadow collection and existing-position management remain active",
                )
            return []
        block_reason, block_detail = self._new_entry_block_reason()
        if block_reason:
            for forecast in forecasts[: max(1, safe_int(self.cfg.get("journal_top_candidates"), 50))]:
                self._block_forecast(forecast, block_reason, block_detail)
            return []
        exiting_ids = {str(action.get("position_id")) for action in exits}
        remaining = [pos for pos in open_positions if pos.position_id not in exiting_ids]
        actions: List[Dict[str, Any]] = []
        use_position_cap = self._use_position_count_cap()
        target_active_positions = (
            safe_int(self.cfg.get("target_active_positions"), 0)
            if self._broker_mode() and not use_position_cap
            else 0
        )
        available_slots = (
            max(0, safe_int(self.cfg.get("max_open_positions"), 6) - len(remaining))
            if use_position_cap
            else (
                max(0, target_active_positions - len(remaining))
                if target_active_positions > 0
                else safe_int(self.cfg.get("max_new_positions_per_cycle"), 2)
            )
        )
        max_new = safe_int(self.cfg.get("max_new_positions_per_cycle"), 2)
        reserved_margin = 0.0
        reserved_risk_usd = 0.0
        reserved_replacement_ids: set[str] = set()
        ordered_forecasts = list(forecasts)
        if self._broker_mode() and not use_position_cap:
            ordered_forecasts.sort(key=self._preallocation_score, reverse=True)
        for forecast in ordered_forecasts:
            if len(actions) >= max_new:
                break
            if not self._forecast_can_open(forecast, remaining, actions):
                continue
            replacement: Optional[PaperPosition] = None
            released_margin = 0.0
            if available_slots <= 0:
                replacement = self._replacement_target(forecast, remaining, reserved_replacement_ids)
                if not replacement:
                    continue
                released_margin = self._estimated_position_margin_required(replacement)
            planned_units = self._size_units(
                forecast,
                reserved_margin=reserved_margin,
                reserved_risk_usd=reserved_risk_usd,
                released_margin=released_margin,
                use_target_margin=not use_position_cap and self._broker_mode(),
            )
            if planned_units == 0 and not replacement and self.cfg.get("replacement_on_pressure", False):
                replacement = self._replacement_target(forecast, remaining, reserved_replacement_ids)
                if replacement:
                    released_margin = self._estimated_position_margin_required(replacement)
                    planned_units = self._size_units(
                        forecast,
                        reserved_margin=reserved_margin,
                        reserved_risk_usd=reserved_risk_usd,
                        released_margin=released_margin,
                        use_target_margin=not use_position_cap and self._broker_mode(),
                    )
            if planned_units == 0:
                continue
            fitted_units = self._fit_units_to_portfolio_risk(forecast, planned_units, remaining, actions, replacement)
            if fitted_units == 0:
                self._block_forecast(forecast, "portfolio_risk_capacity_zero")
                continue
            planned_units = fitted_units
            economics = self._forecast_economics(forecast, planned_units)
            economics_block = self._forecast_economics_block_detail(forecast, planned_units, economics=economics)
            if economics_block:
                self._block_forecast(forecast, "economics_below_floor", economics_block)
                continue
            if (
                not replacement
                and self.cfg.get("replacement_on_pressure", False)
                and self._replacement_pressure(forecast, economics, remaining, actions)
            ):
                replacement = self._replacement_target(forecast, remaining, reserved_replacement_ids)
                if replacement:
                    released_margin = self._estimated_position_margin_required(replacement)
                    planned_units = self._size_units(
                        forecast,
                        reserved_margin=reserved_margin,
                        reserved_risk_usd=reserved_risk_usd,
                        released_margin=released_margin,
                        use_target_margin=not use_position_cap and self._broker_mode(),
                    )
                    if planned_units == 0:
                        continue
                    fitted_units = self._fit_units_to_portfolio_risk(
                        forecast,
                        planned_units,
                        remaining,
                        actions,
                        replacement,
                    )
                    if fitted_units == 0:
                        self._block_forecast(forecast, "portfolio_risk_capacity_zero")
                        continue
                    planned_units = fitted_units
                    economics = self._forecast_economics(forecast, planned_units)
                    economics_block = self._forecast_economics_block_detail(
                        forecast,
                        planned_units,
                        economics=economics,
                    )
                    if economics_block:
                        self._block_forecast(forecast, "economics_below_floor", economics_block)
                        continue
            risk_ok, risk_detail = self._portfolio_risk_guard(
                forecast,
                planned_units,
                economics,
                remaining,
                actions,
                replacement,
            )
            if (
                not risk_ok
                and not replacement
                and self.cfg.get("replacement_on_pressure", False)
                and str(risk_detail.get("risk_guard_reject_reason") or "")
                in {
                    "below_quality_for_above_target_risk",
                    "open_risk_cap",
                    "below_quality_for_above_target_margin",
                    "hard_margin_cap",
                    "emergency_margin_cap",
                    "currency_risk_cap",
                }
            ):
                replacement = self._replacement_target(forecast, remaining, reserved_replacement_ids)
                if replacement:
                    released_margin = self._estimated_position_margin_required(replacement)
                    planned_units = self._size_units(
                        forecast,
                        reserved_margin=reserved_margin,
                        reserved_risk_usd=reserved_risk_usd,
                        released_margin=released_margin,
                        use_target_margin=not use_position_cap and self._broker_mode(),
                    )
                    if planned_units != 0:
                        fitted_units = self._fit_units_to_portfolio_risk(
                            forecast,
                            planned_units,
                            remaining,
                            actions,
                            replacement,
                        )
                        if fitted_units != 0:
                            planned_units = fitted_units
                            economics = self._forecast_economics(forecast, planned_units)
                            economics_block = self._forecast_economics_block_detail(
                                forecast,
                                planned_units,
                                economics=economics,
                            )
                            if not economics_block:
                                risk_ok, risk_detail = self._portfolio_risk_guard(
                                    forecast,
                                    planned_units,
                                    economics,
                                    remaining,
                                    actions,
                                    replacement,
                                )
            if not risk_ok:
                self._block_forecast(
                    forecast,
                    str(risk_detail.get("risk_guard_reject_reason") or "portfolio_risk_guard"),
                    json.dumps(risk_detail, sort_keys=True),
                )
                continue
            economics.update(risk_detail)
            if not replacement and available_slots > 0:
                action = self._open_action(forecast, "open_slot")
                action["planned_units"] = planned_units
                action.update(economics)
                actions.append(action)
                reserved_margin += self._estimated_margin_required(forecast, planned_units)
                reserved_risk_usd += max(0.0, safe_float(economics.get("risk_usd_estimate"), 0.0))
                if use_position_cap or target_active_positions > 0:
                    available_slots -= 1
                continue
            if replacement:
                action = self._open_action(forecast, "replacement")
                action["planned_units"] = planned_units
                action.update(economics)
                action["replacement_position_id"] = replacement.position_id
                action["replacement_instrument"] = replacement.instrument
                action["replacement_direction"] = direction_name(replacement.direction)
                replacement_reference_score = max(replacement.last_score, replacement.opened_score * 0.50)
                action[
                    "replacement_reason"
                ] = (
                    f"rotate_to_better_forecast {forecast.instrument} "
                    f"{forecast.rank_score:.3f}>{replacement_reference_score:.3f} "
                    f"(last={replacement.last_score:.3f}, opened={replacement.opened_score:.3f})"
                )
                actions.append(action)
                reserved_replacement_ids.add(str(replacement.position_id))
                self.state.setdefault("replacement_times", []).append(iso_utc())
                break
        return actions

    def _forecast_can_open(
        self,
        forecast: Forecast,
        positions: Sequence[PaperPosition],
        pending_actions: Sequence[Dict[str, Any]],
    ) -> bool:
        if forecast.reject_reason:
            return self._block_forecast(forecast, "forecast_rejected", forecast.reject_reason)
        stale_detail = self._forecast_feature_age_block_detail(forecast)
        if stale_detail:
            return self._block_forecast(forecast, "stale_model_features", stale_detail)
        if forecast.rank_score < safe_float(self.cfg.get("min_rank_score"), 0.18):
            return self._block_forecast(forecast, "below_min_rank_score", f"{forecast.rank_score:.4f}")
        if not self._forecast_loss_limits_allow(forecast):
            return False
        if self._size_units(forecast) == 0:
            return self._block_forecast(forecast, "zero_units")
        same_pair_positions = [pos for pos in positions if pos.instrument == forecast.instrument]
        same_pair_opens = [
            action
            for action in pending_actions
            if action.get("instrument") == forecast.instrument and action.get("action") == "open"
        ]
        max_same_pair = max(1, safe_int(self.cfg.get("max_same_pair_positions"), 1))
        if same_pair_opens:
            return self._block_forecast(forecast, "same_pair_pending_open")
        if len(same_pair_positions) >= max_same_pair:
            return self._block_forecast(forecast, "same_pair_cap")
        if any(pos.direction != forecast.direction for pos in same_pair_positions):
            return self._block_forecast(forecast, "opposite_pair_position_open")
        cooldown_until = self.state.get("cooldowns", {}).get(forecast.instrument)
        if cooldown_until:
            try:
                if datetime.fromisoformat(str(cooldown_until)) > utc_now():
                    return self._block_forecast(forecast, "pair_cooldown", str(cooldown_until))
            except Exception:
                pass
        if not self._currency_exposure_allows(forecast, positions):
            return self._block_forecast(forecast, "currency_count_exposure_cap")
        if self._broker_mode() and self.cfg.get("broker_precheck_protective_orders", False):
            ok, _, _ = self._broker_protective_order_precheck(forecast)
            if not ok:
                return self._block_forecast(forecast, "protective_order_precheck_failed")
        return True

    def _currency_exposure_allows(self, forecast: Forecast, positions: Sequence[PaperPosition]) -> bool:
        base, quote = instrument_parts(forecast.instrument)
        counts: Dict[Tuple[str, int], int] = {}
        for pos in positions:
            p_base, p_quote = instrument_parts(pos.instrument)
            counts[(p_base, pos.direction)] = counts.get((p_base, pos.direction), 0) + 1
            counts[(p_quote, -pos.direction)] = counts.get((p_quote, -pos.direction), 0) + 1
        max_count = safe_int(self.cfg.get("max_same_direction_currency_exposure"), 4)
        return counts.get((base, forecast.direction), 0) < max_count and counts.get((quote, -forecast.direction), 0) < max_count

    def _replacement_target(
        self,
        forecast: Forecast,
        positions: Sequence[PaperPosition],
        excluded_position_ids: Sequence[str] = (),
    ) -> Optional[PaperPosition]:
        if not self._replacement_allowed():
            return None
        replacement_min_age = safe_float(
            self.cfg.get("replacement_min_age_minutes"),
            safe_float(self.cfg.get("min_hold_minutes"), 8.0),
        )
        excluded = {str(item) for item in excluded_position_ids}
        mature = [
            pos
            for pos in positions
            if str(pos.position_id) not in excluded and self._age_minutes(pos.opened_utc) >= replacement_min_age
        ]
        if not mature:
            return None
        ranked = sorted(
            mature,
            key=lambda pos: (
                pos.last_score,
                -self._age_minutes(pos.opened_utc),
                pos.unrealized_pips,
            ),
        )
        target = ranked[0]
        improvement = forecast.rank_score - max(target.last_score, target.opened_score * 0.50)
        reference_score = max(target.last_score, target.opened_score * 0.50)
        required_score = max(
            reference_score + safe_float(self.cfg.get("replacement_min_score_improvement"), 0.35),
            reference_score * safe_float(self.cfg.get("replacement_min_score_multiplier"), 1.0),
        )
        if improvement >= safe_float(self.cfg.get("replacement_min_score_improvement"), 0.35) and forecast.rank_score >= required_score:
            return target
        return None

    def _replacement_pressure(
        self,
        forecast: Forecast,
        economics: Dict[str, Any],
        positions: Sequence[PaperPosition],
        pending_actions: Sequence[Dict[str, Any]],
    ) -> bool:
        if not self._broker_mode():
            return False
        nav = self._account_nav()
        if nav <= 0:
            return False
        risk_usd = max(0.0, safe_float(economics.get("risk_usd_estimate"), 0.0))
        open_risk_after_pct = (self._open_risk_usd(positions, pending_actions) + risk_usd) / nav * 100.0
        if open_risk_after_pct > self._effective_open_risk_pct("target_open_risk_pct"):
            return True
        margin_used = safe_float(self.live_account_summary.get("marginUsed"), 0.0)
        margin_after = (
            margin_used
            + self._pending_margin_required(pending_actions)
            + max(0.0, safe_float(economics.get("estimated_margin_required"), 0.0))
        )
        margin_after_pct = margin_after / nav * 100.0
        if margin_after_pct > safe_float(self.cfg.get("target_margin_used_pct"), 55.0):
            return True
        return False

    def _replacement_allowed(self) -> bool:
        cutoff = utc_now() - timedelta(hours=1)
        kept: List[str] = []
        count = 0
        for text in self.state.get("replacement_times", []) or []:
            try:
                dt = datetime.fromisoformat(str(text))
            except Exception:
                continue
            if dt >= cutoff:
                kept.append(str(text))
                count += 1
        self.state["replacement_times"] = kept
        return count < safe_int(self.cfg.get("max_replacements_per_hour"), 2)

    def _open_action(self, forecast: Forecast, reason: str) -> Dict[str, Any]:
        return {
            "action": "open",
            "forecast_id": forecast.forecast_id,
            "instrument": forecast.instrument,
            "direction": forecast.direction_name(),
            "event": forecast.event,
            "family": forecast.family,
            "horizon_minutes": forecast.horizon_minutes,
            "rank_score": forecast.rank_score,
            "probability": forecast.probability,
            "edge_pips": forecast.edge_pips,
            "risk_pips": forecast.risk_pips,
            "entry_ref": forecast.entry_ref,
            "stop_loss": forecast.stop_loss,
            "take_profit": forecast.take_profit,
            "trailing_stop_pips": forecast.trailing_stop_pips,
            "reason": reason,
        }

    def _broker_protective_order_precheck(self, forecast: Forecast) -> Tuple[bool, str, Dict[str, Any]]:
        if not self._broker_mode() or not self.gateway or not self.cfg.get("broker_precheck_protective_orders", False):
            return True, "", {}
        prices = self.gateway.get_prices([forecast.instrument])
        raw_price = prices.get(forecast.instrument) or {}
        if not raw_price:
            return True, "", {}
        current = price_from_oanda(raw_price, forecast.mid, forecast.instrument)
        bid = safe_float(current.get("bid"), 0.0)
        ask = safe_float(current.get("ask"), 0.0)
        if bid <= 0 or ask <= 0:
            return True, "", {}
        pip = pips_size(forecast.instrument)
        spread_distance = abs(ask - bid)
        min_distance = max(
            safe_float(self.cfg.get("broker_precheck_min_distance_pips"), 0.1) * pip,
            spread_distance * safe_float(self.cfg.get("broker_precheck_min_spread_multiple"), 0.25),
        )
        detail = {
            "bid": round(bid, 8),
            "ask": round(ask, 8),
            "stop_loss": round(forecast.stop_loss, 8),
            "take_profit": round(forecast.take_profit, 8),
            "min_distance": round(min_distance, 8),
        }
        if forecast.direction > 0:
            if forecast.take_profit <= ask + min_distance:
                return False, "precheck_take_profit_would_reject", detail
            if forecast.stop_loss >= bid - min_distance:
                return False, "precheck_stop_loss_would_reject", detail
        else:
            if forecast.take_profit >= bid - min_distance:
                return False, "precheck_take_profit_would_reject", detail
            if forecast.stop_loss <= ask + min_distance:
                return False, "precheck_stop_loss_would_reject", detail
        return True, "", detail

    def _apply_action(self, action: Dict[str, Any], forecasts: Sequence[Forecast]) -> None:
        followup_close: Optional[Dict[str, Any]] = None
        preclose: Optional[Dict[str, Any]] = None
        if action.get("action") == "close":
            self._close_position(action)
        elif action.get("action") == "open":
            forecast = next((item for item in forecasts if item.forecast_id == action.get("forecast_id")), None)
            if forecast:
                close_before_replacement = (
                    self._broker_mode()
                    and bool(action.get("replacement_position_id"))
                    and bool(self.cfg.get("broker_close_replacement_before_open", False))
                )
                if close_before_replacement:
                    ok, reason, detail = self._broker_protective_order_precheck(forecast)
                    if not ok:
                        action["status"] = "skipped_invalid_protective_order"
                        action["broker_cancel_reason"] = reason
                        action["broker_response"] = json.dumps({"protective_precheck": detail}, sort_keys=True)[:1500]
                    else:
                        action["_protective_precheck_done"] = True
                        preclose = {
                            "action": "close",
                            "position_id": action.get("replacement_position_id"),
                            "instrument": action.get("replacement_instrument"),
                            "direction": action.get("replacement_direction"),
                            "reason": action.get("replacement_reason") or f"replacement_preclose {forecast.instrument}",
                            "rank_score": "",
                        }
                        self._close_position(preclose)
                        if preclose.get("status") in {"filled", "sent"}:
                            self._refresh_live_state(self.forecasts_by_instrument or {})
                            self._open_position(action, forecast)
                        else:
                            action["status"] = "skipped_replacement_close_failed"
                            action["broker_cancel_reason"] = str(
                                preclose.get("broker_cancel_reason") or preclose.get("status") or ""
                            )
                else:
                    self._open_position(action, forecast)
                if (
                    not close_before_replacement
                    and action.get("replacement_position_id")
                    and action.get("status") in {"filled", "paper_opened"}
                ):
                    followup_close = {
                        "action": "close",
                        "position_id": action.get("replacement_position_id"),
                        "instrument": action.get("replacement_instrument"),
                        "direction": action.get("replacement_direction"),
                        "reason": action.get("replacement_reason") or f"replacement_filled {forecast.instrument}",
                        "rank_score": "",
                    }
                    self._close_position(followup_close)
        fields = self._action_journal_fields()
        if preclose:
            close_row = dict(preclose)
            close_row["time_utc"] = iso_utc()
            close_row["mode"] = self.mode
            append_csv(self.action_journal, close_row, fields)
        row = dict(action)
        row["time_utc"] = iso_utc()
        row["mode"] = self.mode
        append_csv(self.action_journal, row, fields)
        if followup_close:
            close_row = dict(followup_close)
            close_row["time_utc"] = iso_utc()
            close_row["mode"] = self.mode
            append_csv(self.action_journal, close_row, fields)

    def _open_position(self, action: Dict[str, Any], forecast: Forecast) -> None:
        planned_units = safe_int(action.get("planned_units"), 0)
        units = planned_units if planned_units else self._size_units(forecast)
        action["units"] = units
        if units == 0:
            action["status"] = "skipped_zero_units"
            return
        if self._broker_mode():
            if not self.execute_live or not self.gateway:
                action["status"] = "advice_only_broker_not_enabled"
                return
            if not action.get("_protective_precheck_done"):
                ok, reason, detail = self._broker_protective_order_precheck(forecast)
                if not ok:
                    action["status"] = "skipped_invalid_protective_order"
                    action["broker_cancel_reason"] = reason
                    action["broker_response"] = json.dumps({"protective_precheck": detail}, sort_keys=True)[:1500]
                    return
            forecast_record = self._forecast_log_row(forecast)
            forecast_record.update(
                {
                    "risk_usd_estimate": safe_float(action.get("risk_usd_estimate"), 0.0),
                    "estimated_margin_required": safe_float(action.get("estimated_margin_required"), 0.0),
                    "planned_units": units,
                }
            )
            self.state.setdefault("live_forecast_by_id", {})[forecast.forecast_id] = forecast_record
            response = self.gateway.create_market_order(
                forecast,
                units,
                self._price_precision_for_instrument(forecast.instrument),
                self.broker_tag,
            )
            if response.get("_error"):
                action["status"] = "broker_error"
                error_reason = (
                    response.get("errorCode")
                    or response.get("errorMessage")
                    or ((response.get("orderRejectTransaction") or {}).get("rejectReason"))
                    or response.get("_error_text")
                    or ""
                )
                action["broker_cancel_reason"] = str(error_reason)
                cooldown_minutes = safe_float(
                    self.cfg.get("pair_cooldown_minutes_after_broker_error"),
                    safe_float(self.cfg.get("pair_cooldown_minutes_after_rotation"), 10.0),
                )
                if cooldown_minutes > 0:
                    cooldown = utc_now() + timedelta(minutes=cooldown_minutes)
                    self.state.setdefault("cooldowns", {})[forecast.instrument] = cooldown.isoformat()
            elif response.get("orderCancelTransaction"):
                action["status"] = "broker_canceled"
                action["broker_cancel_reason"] = str((response.get("orderCancelTransaction") or {}).get("reason") or "")
                cooldown_minutes = safe_float(self.cfg.get("pair_cooldown_minutes_after_rotation"), 10.0)
                if action["broker_cancel_reason"] == "MARKET_HALTED":
                    cooldown_minutes = safe_float(
                        self.cfg.get("pair_cooldown_minutes_after_market_halted"),
                        max(cooldown_minutes, 60.0),
                    )
                elif "FIFO" in action["broker_cancel_reason"]:
                    cooldown_minutes = safe_float(
                        self.cfg.get("pair_cooldown_minutes_after_broker_error"),
                        max(cooldown_minutes, 15.0),
                    )
                cooldown = utc_now() + timedelta(minutes=cooldown_minutes)
                self.state.setdefault("cooldowns", {})[forecast.instrument] = cooldown.isoformat()
            elif response.get("orderFillTransaction"):
                action["status"] = "filled"
                fill_ids = extract_order_fill_ids(response)
                action.update({key: value for key, value in fill_ids.items() if value})
                broker_trade_id = fill_ids.get("broker_trade_id", "")
                if broker_trade_id:
                    self.state.setdefault("live_trade_by_id", {})[broker_trade_id] = {
                        "forecast_id": forecast.forecast_id,
                        "instrument": forecast.instrument,
                        "units": str(units),
                        "risk_usd_estimate": safe_float(action.get("risk_usd_estimate"), 0.0),
                        "estimated_margin_required": safe_float(action.get("estimated_margin_required"), 0.0),
                        "opened_utc": iso_utc(),
                        "mapped_utc": iso_utc(),
                        "source": "order_fill",
                    }
            else:
                action["status"] = "sent"
                fill_ids = extract_order_fill_ids(response)
                action.update({key: value for key, value in fill_ids.items() if value})
            action["broker_response"] = json.dumps(response, sort_keys=True)[:1500]
            return
        if self.mode == "advice":
            action["status"] = "advice_only"
            return
        positions = self._open_positions()
        positions.append(
            PaperPosition(
                position_id=f"paper_{uuid.uuid4().hex[:16]}",
                opened_utc=iso_utc(),
                instrument=forecast.instrument,
                direction=forecast.direction,
                units=units,
                entry_price=forecast.entry_ref,
                stop_loss=forecast.stop_loss,
                take_profit=forecast.take_profit,
                trailing_stop_pips=forecast.trailing_stop_pips,
                horizon_minutes=forecast.horizon_minutes,
                opened_score=forecast.rank_score,
                event=forecast.event,
                family=forecast.family,
                expected_edge_pips=forecast.edge_pips,
                risk_pips=forecast.risk_pips,
                risk_usd_estimate=safe_float(action.get("risk_usd_estimate"), 0.0),
                estimated_margin_required=safe_float(action.get("estimated_margin_required"), 0.0),
                last_score=forecast.rank_score,
            )
        )
        self._write_positions(positions)
        action["status"] = "paper_opened"

    def _close_position(self, action: Dict[str, Any]) -> None:
        positions = self._open_positions()
        target = next((pos for pos in positions if pos.position_id == action.get("position_id")), None)
        if self._broker_mode():
            position_id = str(action.get("position_id") or "")
            target = self.live_positions_index.get(position_id)
            if not target:
                action["status"] = "skipped_missing_live_position"
                return
            if not self.execute_live or not self.gateway:
                action["status"] = "advice_only_broker_close"
                return
            trade_id = position_id.replace("live_", "", 1)
            action["broker_trade_id"] = trade_id
            action["units"] = target.units
            response = self.gateway.close_trade(trade_id)
            action["realized_pips_estimate"] = round(target.unrealized_pips, 6)
            action["unrealized_usd_before_close"] = round(target.unrealized_usd, 6)
            fill = response.get("orderFillTransaction") if isinstance(response.get("orderFillTransaction"), dict) else {}
            if fill:
                action["broker_transaction_id"] = fill.get("id", "")
                action["broker_close_reason"] = fill.get("reason", "")
                closed_items = fill.get("tradesClosed")
                if isinstance(closed_items, list) and closed_items:
                    closed = closed_items[0] if isinstance(closed_items[0], dict) else {}
                    action["realized_pl"] = closed.get("realizedPL", fill.get("pl", ""))
                    action["financing"] = closed.get("financing", "")
                elif isinstance(fill.get("tradeReduced"), dict):
                    reduced = fill.get("tradeReduced") or {}
                    action["realized_pl"] = reduced.get("realizedPL", fill.get("pl", ""))
                    action["financing"] = reduced.get("financing", "")
            if response.get("_error"):
                action["status"] = "broker_error"
            elif response.get("orderCancelTransaction"):
                action["status"] = "broker_canceled"
                action["broker_cancel_reason"] = str((response.get("orderCancelTransaction") or {}).get("reason") or "")
            elif response.get("orderFillTransaction"):
                action["status"] = "filled"
            else:
                action["status"] = "sent"
            action["broker_response"] = json.dumps(response, sort_keys=True)[:1500]
            if action["status"] in {"filled", "sent"}:
                self.state.setdefault("live_trade_by_id", {}).pop(trade_id, None)
                self.exit_fade_counts.pop(str(target.position_id), None)
            if action["status"] == "filled":
                realized = safe_float(action.get("realized_pl"), safe_float(target.unrealized_usd, 0.0))
                self._record_closed_pnl(
                    target.instrument,
                    realized,
                    risk_usd=self._estimated_position_risk_usd(target),
                    reason=str(action.get("reason") or ""),
                )
            elif target.unrealized_usd < 0:
                self._set_pair_cooldown(
                    target.instrument,
                    safe_float(self.cfg.get("pair_cooldown_minutes_after_loss"), 30.0),
                    "loss_close_pending_broker_fill",
                )
            return
        if target is None:
            action["status"] = "skipped_missing_position"
            return
        positions = [pos for pos in positions if pos.position_id != target.position_id]
        equity = safe_float(self.state.get("paper_equity"), 0.0) + target.unrealized_usd
        self.state["paper_equity"] = round(equity, 6)
        self.state["paper_high_water"] = max(safe_float(self.state.get("paper_high_water"), equity), equity)
        closed = asdict(target)
        closed["closed_utc"] = iso_utc()
        closed["close_reason"] = action.get("reason", "")
        self.state.setdefault("closed_positions", []).append(closed)
        self._record_closed_pnl(
            target.instrument,
            target.unrealized_usd,
            risk_usd=self._estimated_position_risk_usd(target),
            reason=str(action.get("reason") or ""),
        )
        if "rotate" in str(action.get("reason", "")):
            self._set_pair_cooldown(
                target.instrument,
                safe_float(self.cfg.get("pair_cooldown_minutes_after_rotation"), 10.0),
                "rotation_close",
            )
        self._write_positions(positions)
        self.exit_fade_counts.pop(str(target.position_id), None)
        action["realized_usd"] = round(target.unrealized_usd, 6)
        action["realized_pips"] = round(target.unrealized_pips, 6)
        action["status"] = "paper_closed"

    def _pip_value_per_unit_usd(self, forecast: Forecast) -> float:
        return pip_value_per_unit_usd(forecast.instrument, forecast.mid, self.forecasts_by_instrument or {forecast.instrument: forecast})

    def _forecast_economics(self, forecast: Forecast, units: int) -> Dict[str, Any]:
        abs_units = abs(int(units))
        pip_value = self._pip_value_per_unit_usd(forecast)
        expected_profit_usd = max(0.0, forecast.edge_pips) * pip_value * abs_units
        take_profit_usd = max(0.0, forecast.take_profit_pips) * pip_value * abs_units
        risk_usd = max(0.0, forecast.stop_pips) * pip_value * abs_units
        margin_required = self._estimated_margin_required(forecast, units)
        profit_per_margin = expected_profit_usd / margin_required if margin_required > 0 else 0.0
        allocation_score = expected_profit_usd * (1.0 + max(0.0, forecast.rank_score) * 20.0) * (
            1.0 + min(5.0, max(0.0, profit_per_margin) * 100.0)
        )
        return {
            "expected_profit_usd": round(expected_profit_usd, 6),
            "take_profit_usd": round(take_profit_usd, 6),
            "risk_usd_estimate": round(risk_usd, 6),
            "estimated_margin_required": round(margin_required, 6),
            "profit_per_margin": round(profit_per_margin, 6),
            "allocation_score": round(allocation_score, 6),
        }

    def _forecast_economics_allows(
        self,
        forecast: Forecast,
        units: int,
        *,
        economics: Optional[Dict[str, Any]] = None,
        allow_zero_margin: bool = False,
    ) -> bool:
        return not self._forecast_economics_block_detail(
            forecast,
            units,
            economics=economics,
            allow_zero_margin=allow_zero_margin,
        )

    def _forecast_economics_block_detail(
        self,
        forecast: Forecast,
        units: int,
        *,
        economics: Optional[Dict[str, Any]] = None,
        allow_zero_margin: bool = False,
    ) -> str:
        if units == 0:
            return "units=0"
        economics = economics or self._forecast_economics(forecast, units)
        expected_profit = safe_float(economics.get("expected_profit_usd"), 0.0)
        min_expected = safe_float(self.cfg.get("min_expected_profit_usd"), 0.0)
        if expected_profit < min_expected:
            return (
                f"expected_profit_usd:{expected_profit:.6f}<{min_expected:.6f};"
                f"units={int(units)};edge_pips={forecast.edge_pips:.6f}"
            )
        take_profit = safe_float(economics.get("take_profit_usd"), 0.0)
        min_take_profit = safe_float(self.cfg.get("min_take_profit_usd"), 0.0)
        if take_profit < min_take_profit:
            return (
                f"take_profit_usd:{take_profit:.6f}<{min_take_profit:.6f};"
                f"units={int(units)};take_profit_pips={forecast.take_profit_pips:.6f}"
            )
        margin_required = safe_float(economics.get("estimated_margin_required"), 0.0)
        if margin_required <= 0 and allow_zero_margin:
            return ""
        profit_per_margin = safe_float(economics.get("profit_per_margin"), 0.0)
        min_profit_per_margin = safe_float(self.cfg.get("min_profit_per_margin"), 0.0)
        if profit_per_margin < min_profit_per_margin:
            return (
                f"profit_per_margin:{profit_per_margin:.6f}<{min_profit_per_margin:.6f};"
                f"expected_profit_usd={expected_profit:.6f};margin_required={margin_required:.6f};"
                f"units={int(units)}"
            )
        return ""

    def _block_forecast(self, forecast: Forecast, reason: str, detail: str = "") -> bool:
        if len(self.entry_blocks) < max(50, safe_int(self.cfg.get("journal_top_candidates"), 50) * 4):
            self.entry_blocks.append(
                {
                    "generated_utc": iso_utc(),
                    "instrument": forecast.instrument,
                    "direction": forecast.direction_name(),
                    "forecast_id": forecast.forecast_id,
                    "rank_score": forecast.rank_score,
                    "probability": forecast.probability,
                    "reason": reason,
                    "detail": detail,
                }
            )
        return False

    def _account_nav(self) -> float:
        if self._broker_mode() and self.live_account_summary:
            nav = safe_float(self.live_account_summary.get("NAV") or self.live_account_summary.get("nav"), 0.0)
        else:
            nav = safe_float(self.state.get("paper_equity"), 0.0)
        if nav <= 0:
            nav = safe_float(self.cfg.get("paper_starting_equity"), 10000.0)
        return max(0.0, nav)

    def _current_drawdown_pct(self) -> float:
        equity = self._account_nav()
        high_water = safe_float(self.state.get("paper_high_water"), equity)
        return max(0.0, (high_water - equity) / high_water * 100.0) if high_water > 0 else 0.0

    def _drawdown_risk_multiplier(self) -> float:
        drawdown = self._current_drawdown_pct()
        start = max(0.0, safe_float(self.cfg.get("drawdown_risk_throttle_start_pct"), 0.0))
        full = max(start + 0.01, safe_float(self.cfg.get("drawdown_risk_throttle_full_pct"), start + 0.01))
        floor = min(1.0, max(0.0, safe_float(self.cfg.get("drawdown_risk_min_multiplier"), 1.0)))
        if drawdown <= start:
            return 1.0
        if drawdown >= full:
            return floor
        progress = (drawdown - start) / (full - start)
        return 1.0 - progress * (1.0 - floor)

    def _effective_open_risk_pct(self, key: str) -> float:
        configured = safe_float(self.cfg.get(key), 0.0)
        if configured <= 0:
            return 1e9
        floor = min(1.0, max(0.0, safe_float(self.cfg.get("drawdown_open_risk_min_multiplier"), 1.0)))
        multiplier = max(floor, self._drawdown_risk_multiplier())
        return configured * multiplier

    def _position_mid(self, position: PaperPosition) -> float:
        forecast = self.forecasts_by_instrument.get(position.instrument)
        mid = safe_float(forecast.mid if forecast else 0.0, 0.0)
        return mid if mid > 0 else safe_float(position.entry_price, 0.0)

    def _estimated_position_risk_usd(self, position: PaperPosition) -> float:
        stored = safe_float(position.risk_usd_estimate, 0.0)
        if stored > 0:
            return stored
        risk_pips = abs(safe_float(position.risk_pips, 0.0))
        pip = pips_size(position.instrument)
        if risk_pips <= 0 and position.stop_loss > 0 and position.entry_price > 0 and pip > 0:
            risk_pips = abs(position.entry_price - position.stop_loss) / pip
        if risk_pips <= 0:
            fallback_pct = safe_float(
                self.cfg.get("unstopped_position_risk_nav_pct"),
                safe_float(self.cfg.get("max_trade_risk_pct"), 3.0),
            )
            return self._account_nav() * max(0.0, fallback_pct) / 100.0
        pip_value = pip_value_per_unit_usd(
            position.instrument,
            self._position_mid(position),
            self.forecasts_by_instrument or {},
        )
        return max(0.0, risk_pips * pip_value * abs(int(position.units)))

    def _open_risk_usd(
        self,
        positions: Sequence[PaperPosition],
        pending_actions: Sequence[Dict[str, Any]] = (),
    ) -> float:
        total = sum(self._estimated_position_risk_usd(position) for position in positions)
        for action in pending_actions:
            if action.get("action") == "open":
                total += max(0.0, safe_float(action.get("risk_usd_estimate"), 0.0))
        return max(0.0, total)

    def _pending_margin_required(self, pending_actions: Sequence[Dict[str, Any]]) -> float:
        return sum(
            max(0.0, safe_float(action.get("estimated_margin_required"), 0.0))
            for action in pending_actions
            if action.get("action") == "open"
        )

    def _currency_risk_usd(
        self,
        currency: str,
        positions: Sequence[PaperPosition],
        pending_actions: Sequence[Dict[str, Any]] = (),
    ) -> float:
        if not currency:
            return 0.0
        total = 0.0
        for position in positions:
            base, quote = instrument_parts(position.instrument)
            if currency in {base, quote}:
                total += self._estimated_position_risk_usd(position)
        for action in pending_actions:
            if action.get("action") != "open":
                continue
            base, quote = instrument_parts(str(action.get("instrument") or ""))
            if currency in {base, quote}:
                total += max(0.0, safe_float(action.get("risk_usd_estimate"), 0.0))
        return max(0.0, total)

    def _fit_units_to_portfolio_risk(
        self,
        forecast: Forecast,
        units: int,
        positions: Sequence[PaperPosition],
        pending_actions: Sequence[Dict[str, Any]],
        replacement: Optional[PaperPosition],
    ) -> int:
        if units == 0:
            return 0
        nav = self._account_nav()
        if nav <= 0:
            return 0
        risk_per_unit = max(forecast.stop_pips * self._pip_value_per_unit_usd(forecast), 1e-9)
        released_risk = self._estimated_position_risk_usd(replacement) if replacement else 0.0
        capacity_usd = self._effective_open_risk_pct("max_open_risk_pct") * nav / 100.0
        capacity_usd -= max(0.0, self._open_risk_usd(positions, pending_actions) - released_risk)
        max_currency_pct = safe_float(self.cfg.get("max_currency_risk_pct"), 0.0)
        if max_currency_pct > 0:
            for currency in instrument_parts(forecast.instrument):
                currency_capacity = max_currency_pct * nav / 100.0
                released_currency_risk = (
                    self._estimated_position_risk_usd(replacement)
                    if replacement and currency in set(instrument_parts(replacement.instrument))
                    else 0.0
                )
                currency_capacity -= max(
                    0.0,
                    self._currency_risk_usd(currency, positions, pending_actions) - released_currency_risk,
                )
                capacity_usd = min(capacity_usd, currency_capacity)
        if capacity_usd <= 0:
            return 0
        fitted_abs_units = min(abs(int(units)), int(capacity_usd / risk_per_unit))
        if fitted_abs_units < 1:
            return 0
        return fitted_abs_units if forecast.direction > 0 else -fitted_abs_units

    def _forecast_loss_limits_allow(self, forecast: Forecast) -> bool:
        nav = max(self._account_nav(), 1e-9)
        pair_daily = self.state.get("pair_daily_pnl") if isinstance(self.state, dict) else {}
        pair_total = self.state.get("pair_total_pnl") if isinstance(self.state, dict) else {}
        pair_daily_loss_pct = max(0.0, -safe_float((pair_daily or {}).get(forecast.instrument), 0.0) / nav * 100.0)
        pair_total_loss_pct = max(0.0, -safe_float((pair_total or {}).get(forecast.instrument), 0.0) / nav * 100.0)
        max_pair_daily = safe_float(self.cfg.get("max_pair_daily_loss_pct"), 0.0)
        if max_pair_daily > 0 and pair_daily_loss_pct >= max_pair_daily:
            return self._block_forecast(forecast, "pair_daily_loss_stop", f"{pair_daily_loss_pct:.2f}%")
        max_pair_total = safe_float(self.cfg.get("max_pair_total_loss_pct"), 0.0)
        if max_pair_total > 0 and pair_total_loss_pct >= max_pair_total:
            return self._block_forecast(forecast, "pair_total_loss_stop", f"{pair_total_loss_pct:.2f}%")
        streak_limit = safe_int(self.cfg.get("pair_loss_streak_cooldown_trades"), 0)
        streak = safe_int((self.state.get("pair_loss_streak") or {}).get(forecast.instrument), 0)
        if streak_limit > 0 and streak >= streak_limit:
            return self._block_forecast(forecast, "pair_loss_streak_stop", str(streak))
        entry_halt = safe_float(self.cfg.get("drawdown_entry_halt_pct"), 0.0)
        if entry_halt > 0 and self._current_drawdown_pct() >= entry_halt:
            bypass = safe_float(self.cfg.get("drawdown_halt_bypass_rank_score"), 1e9)
            if forecast.rank_score < bypass:
                return self._block_forecast(
                    forecast,
                    "drawdown_entry_halt",
                    f"{self._current_drawdown_pct():.2f}%",
                )
        return True

    def _portfolio_risk_guard(
        self,
        forecast: Forecast,
        units: int,
        economics: Dict[str, Any],
        positions: Sequence[PaperPosition],
        pending_actions: Sequence[Dict[str, Any]],
        replacement: Optional[PaperPosition],
    ) -> Tuple[bool, Dict[str, Any]]:
        nav = self._account_nav()
        if nav <= 0 or units == 0:
            return False, {"risk_guard_reject_reason": "invalid_nav_or_units"}
        risk_usd = max(0.0, safe_float(economics.get("risk_usd_estimate"), 0.0))
        released_risk = self._estimated_position_risk_usd(replacement) if replacement else 0.0
        open_risk_before_usd = self._open_risk_usd(positions, pending_actions)
        open_risk_after_usd = max(0.0, open_risk_before_usd - released_risk) + risk_usd
        open_risk_before_pct = open_risk_before_usd / nav * 100.0
        open_risk_after_pct = open_risk_after_usd / nav * 100.0
        target_open_pct = self._effective_open_risk_pct("target_open_risk_pct")
        max_open_pct = self._effective_open_risk_pct("max_open_risk_pct")
        above_target_min_rank = safe_float(self.cfg.get("above_target_min_rank_score"), 1e9)
        detail = {
            "open_risk_pct_before": round(open_risk_before_pct, 6),
            "open_risk_pct_after": round(open_risk_after_pct, 6),
            "target_open_risk_pct_effective": round(target_open_pct, 6),
            "max_open_risk_pct_effective": round(max_open_pct, 6),
            "drawdown_risk_multiplier": round(self._drawdown_risk_multiplier(), 6),
        }
        if open_risk_after_pct > target_open_pct and forecast.rank_score < above_target_min_rank:
            detail["risk_guard_reject_reason"] = "below_quality_for_above_target_risk"
            return False, detail
        if open_risk_after_pct > max_open_pct:
            detail["risk_guard_reject_reason"] = "open_risk_cap"
            return False, detail
        released_margin = self._estimated_position_margin_required(replacement) if replacement else 0.0
        margin_used = safe_float(self.live_account_summary.get("marginUsed"), 0.0) if self._broker_mode() else 0.0
        margin_after = (
            margin_used
            + self._pending_margin_required(pending_actions)
            - max(0.0, released_margin)
            + max(0.0, safe_float(economics.get("estimated_margin_required"), 0.0))
        )
        margin_after_pct = margin_after / nav * 100.0 if nav > 0 else 0.0
        detail["margin_used_pct_after"] = round(margin_after_pct, 6)
        target_margin_pct = safe_float(self.cfg.get("target_margin_used_pct"), 55.0)
        if margin_after_pct > target_margin_pct and forecast.rank_score < above_target_min_rank:
            detail["risk_guard_reject_reason"] = "below_quality_for_above_target_margin"
            return False, detail
        max_margin_pct = safe_float(self.cfg.get("max_margin_used_pct"), 72.0)
        if margin_after_pct > max_margin_pct:
            detail["risk_guard_reject_reason"] = "hard_margin_cap"
            return False, detail
        emergency_margin_pct = safe_float(self.cfg.get("emergency_margin_used_pct"), 86.0)
        if margin_after_pct > emergency_margin_pct:
            detail["risk_guard_reject_reason"] = "emergency_margin_cap"
            return False, detail
        currency_cap_pct = safe_float(self.cfg.get("max_currency_risk_pct"), 0.0)
        if currency_cap_pct > 0:
            violations: List[str] = []
            for currency in instrument_parts(forecast.instrument):
                released_currency_risk = (
                    self._estimated_position_risk_usd(replacement)
                    if replacement and currency in set(instrument_parts(replacement.instrument))
                    else 0.0
                )
                currency_after_usd = (
                    self._currency_risk_usd(currency, positions, pending_actions)
                    - max(0.0, released_currency_risk)
                    + risk_usd
                )
                currency_after_pct = max(0.0, currency_after_usd) / nav * 100.0
                detail[f"{currency.lower()}_risk_pct_after"] = round(currency_after_pct, 6)
                if currency_after_pct > currency_cap_pct:
                    violations.append(f"{currency}:{currency_after_pct:.2f}>{currency_cap_pct:.2f}")
            if violations:
                detail["risk_guard_reject_reason"] = "currency_risk_cap"
                detail["currency_risk_violations"] = ",".join(violations)
                return False, detail
        return True, detail

    def _preallocation_score(self, forecast: Forecast) -> float:
        if forecast.reject_reason:
            return -1.0
        units = self._size_units(forecast, use_target_margin=True)
        if units == 0:
            return -1.0
        economics = self._forecast_economics(forecast, units)
        if not self._forecast_economics_allows(forecast, units, economics=economics):
            return -1.0
        return safe_float(economics.get("allocation_score"), 0.0)

    def _size_units(
        self,
        forecast: Forecast,
        *,
        reserved_margin: float = 0.0,
        reserved_risk_usd: float = 0.0,
        released_margin: float = 0.0,
        use_target_margin: bool = False,
    ) -> int:
        if self._broker_mode() and self.live_account_summary:
            nav = safe_float(self.live_account_summary.get("NAV") or self.live_account_summary.get("nav"), 0.0)
            margin_used = safe_float(self.live_account_summary.get("marginUsed"), 0.0)
        else:
            nav = safe_float(self.state.get("paper_equity"), safe_float(self.cfg.get("paper_starting_equity"), 10000.0))
            margin_used = 0.0
        risk_pct = self._risk_pct(forecast)
        risk_usd = nav * risk_pct / 100.0
        daily_loss_risk_cap_usd: Optional[float] = None
        if self._broker_mode():
            day_start = safe_float(self.state.get("day_start_equity"), nav)
            daily_loss_kill_pct = safe_float(self.cfg.get("daily_loss_kill_pct"), 0.0)
            remaining_fraction = safe_float(self.cfg.get("max_trade_risk_remaining_day_loss_fraction"), 0.0)
            if day_start > 0 and daily_loss_kill_pct > 0 and remaining_fraction > 0:
                max_day_loss_usd = day_start * daily_loss_kill_pct / 100.0
                current_day_loss_usd = max(0.0, day_start - nav)
                remaining_loss_budget = max(0.0, max_day_loss_usd - current_day_loss_usd)
                daily_loss_risk_cap_usd = max(0.0, remaining_loss_budget * remaining_fraction - max(0.0, reserved_risk_usd))
                risk_usd = min(risk_usd, daily_loss_risk_cap_usd)
        pip_value = self._pip_value_per_unit_usd(forecast)
        units = int(risk_usd / max(forecast.stop_pips * pip_value, 1e-9))
        if self.cfg.get("size_to_min_profit_usd", True):
            min_profit_units = 0
            min_expected = safe_float(self.cfg.get("min_expected_profit_usd"), 0.0)
            min_take_profit = safe_float(self.cfg.get("min_take_profit_usd"), 0.0)
            if min_expected > 0 and forecast.edge_pips > 0 and pip_value > 0:
                min_profit_units = max(min_profit_units, math.ceil(min_expected / max(forecast.edge_pips * pip_value, 1e-9)))
            if min_take_profit > 0 and forecast.take_profit_pips > 0 and pip_value > 0:
                min_profit_units = max(
                    min_profit_units,
                    math.ceil(min_take_profit / max(forecast.take_profit_pips * pip_value, 1e-9)),
                )
            if min_profit_units > 0:
                max_trade_risk_usd = nav * safe_float(self.cfg.get("max_trade_risk_pct"), safe_float(self.cfg.get("risk_pct_max"), 1.75)) / 100.0
                if daily_loss_risk_cap_usd is not None:
                    max_trade_risk_usd = min(max_trade_risk_usd, daily_loss_risk_cap_usd)
                max_risk_units = int(max_trade_risk_usd / max(forecast.stop_pips * pip_value, 1e-9))
                if max_risk_units <= 0:
                    units = 0
                else:
                    units = min(max(units, min_profit_units), max_risk_units)
        if self._broker_mode():
            margin_rate = self._margin_rate_for_instrument(forecast.instrument)
            margin_mid_usd = self._margin_mid_usd(forecast.instrument, forecast.mid)
            hard_margin_total = nav * safe_float(self.cfg.get("max_margin_used_pct"), 72.0) / 100.0
            target_margin_total = nav * safe_float(self.cfg.get("target_margin_used_pct"), 55.0) / 100.0
            margin_limit = min(hard_margin_total, target_margin_total) if use_target_margin else hard_margin_total
            remaining_margin = max(
                0.0,
                margin_limit - margin_used + max(0.0, released_margin) - max(0.0, reserved_margin),
            )
            position_margin_pct = safe_float(self.cfg.get("max_position_margin_used_pct"), 0.0)
            if position_margin_pct > 0:
                remaining_margin = min(remaining_margin, nav * position_margin_pct / 100.0)
            margin_buffer_pct = max(0.0, min(95.0, safe_float(self.cfg.get("broker_margin_buffer_pct"), 0.0)))
            if margin_buffer_pct > 0:
                remaining_margin *= max(0.0, 1.0 - margin_buffer_pct / 100.0)
            margin_units = int(remaining_margin / max(margin_mid_usd * margin_rate, 1e-9))
            units = min(units, max(0, margin_units))
        if units < 1:
            return 0
        return units if forecast.direction > 0 else -units

    def _margin_mid_usd(self, instrument: str, mid: float) -> float:
        return max(0.0, safe_float(mid, 0.0)) * quote_to_usd_rate(
            instrument,
            safe_float(mid, 0.0),
            self.forecasts_by_instrument or {},
        )

    def _estimated_margin_required(self, forecast: Forecast, units: int) -> float:
        if not self._broker_mode():
            return 0.0
        margin_rate = self._margin_rate_for_instrument(forecast.instrument)
        return abs(int(units)) * max(self._margin_mid_usd(forecast.instrument, forecast.mid), 1e-9) * margin_rate

    def _estimated_position_margin_required(self, position: PaperPosition) -> float:
        if not self._broker_mode():
            return 0.0
        stored = safe_float(position.estimated_margin_required, 0.0)
        if stored > 0:
            return stored
        margin_rate = self._margin_rate_for_instrument(position.instrument)
        return abs(int(position.units)) * max(self._margin_mid_usd(position.instrument, position.entry_price), 1e-9) * margin_rate

    def _use_position_count_cap(self) -> bool:
        if self._broker_mode() and self.cfg.get("use_position_count_cap") is False:
            return False
        return True

    def _risk_pct(self, forecast: Forecast) -> float:
        lo = safe_float(self.cfg.get("risk_pct_min"), 0.25)
        hi = safe_float(self.cfg.get("risk_pct_max"), 1.75)
        full = max(0.01, safe_float(self.cfg.get("risk_pct_score_full"), 2.50))
        scale = min(1.0, max(0.0, forecast.rank_score / full))
        base_risk = lo + (hi - lo) * scale
        multiplier = self._drawdown_risk_multiplier()
        if multiplier < 1.0:
            base_risk = max(lo * multiplier, base_risk * multiplier)
        return round(max(0.0, base_risk), 4)

    def _model_feature_age_block_detail(self) -> str:
        if not self.cfg.get("model_feature_age_new_entry_block", True):
            return ""
        forecast_source = str(
            self.current_meta.get("forecast_source") or self.cfg.get("forecast_source") or ""
        ).strip().lower()
        if forecast_source != "model_stream":
            return ""
        max_age = safe_float(self.cfg.get("max_model_feature_age_minutes"), 0.0)
        if max_age <= 0:
            return ""
        latest_text = str(self.current_meta.get("latest_model_feature_time_utc") or "").strip()
        if not latest_text:
            return f"latest_model_feature_time_utc missing;max_age_minutes={max_age:.1f}"
        latest_dt = parse_broker_time(latest_text)
        if latest_dt is None:
            return f"latest_model_feature_time_utc unparsable:{latest_text};max_age_minutes={max_age:.1f}"
        age_minutes = max(0.0, (utc_now() - latest_dt).total_seconds() / 60.0)
        if age_minutes > max_age:
            return (
                f"latest_model_feature_time_utc={latest_dt.isoformat()};"
                f"age_minutes={age_minutes:.1f}>max_model_feature_age_minutes={max_age:.1f}"
            )
        return ""

    def _forecast_feature_age_block_detail(self, forecast: Forecast) -> str:
        if not self.cfg.get("model_feature_age_new_entry_block", True):
            return ""
        forecast_source = str(
            self.current_meta.get("forecast_source") or self.cfg.get("forecast_source") or ""
        ).strip().lower()
        if forecast_source != "model_stream":
            return ""
        max_age = safe_float(self.cfg.get("max_model_feature_age_minutes"), 0.0)
        if max_age <= 0:
            return ""
        latest_text = str(forecast.model_feature_time_utc or "").strip()
        if not latest_text:
            return f"forecast_model_feature_time_utc missing;max_age_minutes={max_age:.1f}"
        latest_dt = parse_broker_time(latest_text)
        if latest_dt is None:
            return f"forecast_model_feature_time_utc unparsable:{latest_text};max_age_minutes={max_age:.1f}"
        age_minutes = max(0.0, (utc_now() - latest_dt).total_seconds() / 60.0)
        if age_minutes > max_age:
            return (
                f"forecast_model_feature_time_utc={latest_dt.isoformat()};"
                f"age_minutes={age_minutes:.1f}>max_model_feature_age_minutes={max_age:.1f};"
                f"source_stream={forecast.source_stream};feature_timeframe={forecast.model_feature_timeframe}"
            )
        return ""

    def _new_entry_block_reason(self) -> Tuple[str, str]:
        if self.cfg.get("weekend_new_entry_block", True) and market_closed():
            return "market_closed", "weekend_new_entry_block"
        if self.cfg.get("rollover_new_entry_block", True) and in_rollover_window(self.cfg):
            return "rollover_window", "rollover_new_entry_block"
        stale_detail = self._model_feature_age_block_detail()
        if stale_detail:
            return "stale_model_features", stale_detail
        safety = self._safety_snapshot()
        if safe_float(safety.get("margin_used_pct"), 0.0) >= safe_float(self.cfg.get("max_margin_used_pct"), 72.0):
            return "max_margin_used", (
                f"margin_used_pct={safe_float(safety.get('margin_used_pct'), 0.0):.2f}>="
                f"{safe_float(self.cfg.get('max_margin_used_pct'), 72.0):.2f}"
            )
        if safety.get("kill_switch_active"):
            return "kill_switch_active", ""
        return "", ""

    def _new_entries_blocked(self) -> bool:
        reason, _ = self._new_entry_block_reason()
        return bool(reason)

    def _safety_snapshot(self) -> Dict[str, Any]:
        if self._broker_mode() and self.live_account_summary:
            equity = safe_float(self.live_account_summary.get("NAV") or self.live_account_summary.get("nav"), 0.0)
            margin_used = safe_float(self.live_account_summary.get("marginUsed"), 0.0)
            margin_used_pct = margin_used / equity * 100.0 if equity > 0 else 0.0
        else:
            equity = safe_float(self.state.get("paper_equity"), 0.0)
            margin_used = 0.0
            margin_used_pct = 0.0
        day_start = safe_float(self.state.get("day_start_equity"), equity)
        high_water = safe_float(self.state.get("paper_high_water"), equity)
        day_loss_pct = ((day_start - equity) / day_start * 100.0) if day_start > 0 else 0.0
        drawdown_pct = ((high_water - equity) / high_water * 100.0) if high_water > 0 else 0.0
        positions = list(self.live_positions_index.values()) if self._broker_mode() else self._open_positions()
        open_risk_usd = self._open_risk_usd(positions)
        open_risk_pct = open_risk_usd / equity * 100.0 if equity > 0 else 0.0
        raw_kill = (
            day_loss_pct >= safe_float(self.cfg.get("daily_loss_kill_pct"), 6.0)
            or drawdown_pct >= safe_float(self.cfg.get("drawdown_kill_pct"), 12.0)
            or margin_used_pct >= safe_float(self.cfg.get("emergency_margin_used_pct"), 86.0)
        )
        kill = False if self.cfg.get("disable_kill_switches", False) else raw_kill
        return {
            "paper_equity": round(equity, 6),
            "margin_used": round(margin_used, 6),
            "margin_used_pct": round(margin_used_pct, 6),
            "open_risk_usd": round(open_risk_usd, 6),
            "open_risk_pct": round(open_risk_pct, 6),
            "target_open_risk_pct_effective": round(self._effective_open_risk_pct("target_open_risk_pct"), 6),
            "max_open_risk_pct_effective": round(self._effective_open_risk_pct("max_open_risk_pct"), 6),
            "drawdown_risk_multiplier": round(self._drawdown_risk_multiplier(), 6),
            "day_loss_pct": round(day_loss_pct, 6),
            "drawdown_pct": round(drawdown_pct, 6),
            "raw_kill_switch_active": raw_kill,
            "kill_switch_active": kill,
            "kill_switches_disabled": bool(self.cfg.get("disable_kill_switches", False)),
            "market_closed": market_closed(),
            "rollover_window": in_rollover_window(self.cfg),
        }

    def _write_forecasts(self, forecasts: Sequence[Forecast]) -> None:
        self.latest_forecasts_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [self._forecast_log_row(item) for item in forecasts]
        if not rows:
            self.latest_forecasts_path.write_text("", encoding="utf-8")
            return
        with self.latest_forecasts_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def _forecast_log_row(self, forecast: Forecast) -> Dict[str, Any]:
        row = asdict(forecast)
        row["direction"] = forecast.direction_name()
        row["model_target_direction"] = direction_name(forecast.model_target_direction) if forecast.model_target_direction else ""
        return row

    @staticmethod
    def _age_minutes(opened_utc: str) -> float:
        try:
            opened = datetime.fromisoformat(str(opened_utc))
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            return max(0.0, (utc_now() - opened.astimezone(timezone.utc)).total_seconds() / 60.0)
        except Exception:
            return 0.0


def pip_value_per_unit_usd(
    instrument: str,
    mid: float,
    forecasts_by_instrument: Dict[str, Forecast],
) -> float:
    base, quote = instrument_parts(instrument)
    pip = pips_size(instrument)
    if quote == "USD":
        return pip
    if base == "USD":
        return pip / max(mid, 1e-9)
    cross = forecasts_by_instrument.get(f"{quote}_USD")
    if cross:
        return pip * max(cross.mid, 1e-9)
    inverse = forecasts_by_instrument.get(f"USD_{quote}")
    if inverse:
        return pip / max(inverse.mid, 1e-9)
    return pip


def quote_to_usd_rate(
    instrument: str,
    mid: float,
    forecasts_by_instrument: Dict[str, Forecast],
) -> float:
    base, quote = instrument_parts(instrument)
    if quote == "USD":
        return 1.0
    if base == "USD":
        return 1.0 / max(mid, 1e-9)
    cross = forecasts_by_instrument.get(f"{quote}_USD")
    if cross:
        return max(cross.mid, 1e-9)
    inverse = forecasts_by_instrument.get(f"USD_{quote}")
    if inverse:
        return 1.0 / max(inverse.mid, 1e-9)
    return 1.0


def price_precision(instrument: str) -> int:
    return 3 if instrument.endswith("_JPY") or instrument.endswith("JPY") else 5


def build_gateway_if_needed(mode: str, source: str, cfg: Dict[str, Any]) -> Tuple[Optional[OandaGateway], Dict[str, str]]:
    creds = load_creds()
    resolved = resolve_oanda(cfg, creds, "demo" if mode == "demo" else "live")
    if source == "oanda" or mode in BROKER_MODES:
        gateway = OandaGateway(
            token=resolved["token"],
            base_url=resolved["base_url"],
            account_id=resolved["account_id"],
            timeout=safe_float(cfg.get("oanda_request_timeout_seconds"), 20.0),
            request_retries=safe_int(cfg.get("oanda_request_retries"), 3),
            retry_backoff_seconds=safe_float(cfg.get("oanda_request_backoff_seconds"), 1.25),
        )
        return gateway, resolved
    return None, resolved


def print_resolved_config(cfg: Dict[str, Any], resolved: Dict[str, str]) -> None:
    printable = dict(cfg)
    printable["resolved_account_id"] = resolved.get("account_id", "")
    printable["resolved_base_url"] = resolved.get("base_url", "")
    printable["resolved_token"] = mask_token(resolved.get("token", ""))
    print(json.dumps(printable, indent=2, sort_keys=True, default=str))


def live_execution_allowed(cfg: Dict[str, Any], args: argparse.Namespace, creds: Dict[str, Any]) -> bool:
    return bool(
        args.mode == "live"
        and args.execute
        and cfg.get("live_execution_enabled") is True
        and args.confirm_live == str(cfg.get("live_confirmation_text") or LIVE_CONFIRM_TEXT)
        and setting_bool(creds, "FOREX_ALLOW_LIVE", False)
        and setting_bool(creds, "FOREX_LIVE_EXECUTE", False)
    )


def demo_execution_allowed(cfg: Dict[str, Any], args: argparse.Namespace, resolved: Dict[str, str]) -> bool:
    return bool(
        args.mode == "demo"
        and args.execute
        and cfg.get("demo_execution_enabled") is True
        and resolved.get("env") == "practice"
        and "api-fxpractice.oanda.com" in resolved.get("base_url", "")
    )


def enforce_empty_demo_account_on_first_start(cfg: Dict[str, Any], gateway: Optional[OandaGateway]) -> None:
    if not cfg.get("demo_require_empty_account_on_first_start", True):
        return
    if gateway is None:
        return
    data_dir = resolve_project_path(cfg.get("data_dir"), DATA_DIR)
    state_path = data_dir / "state.json"
    if state_path.exists():
        return
    open_trades = gateway.get_open_trades()
    pending_orders = gateway.get_pending_orders()
    if open_trades or pending_orders:
        raise RuntimeError(
            "Demo account preflight failed: account is not empty on first start "
            f"(open_trades={len(open_trades)}, pending_orders={len(pending_orders)}). "
            "Use a different free practice account or disable demo_require_empty_account_on_first_start explicitly."
        )


def validate_demo_model_stream_contract(cfg: Dict[str, Any]) -> None:
    if not cfg.get("demo_requires_vault_selected_stream", False):
        return
    if str(cfg.get("forecast_source") or "").strip().lower() != "model_stream":
        raise RuntimeError("Demo model contract failed: forecast_source must be model_stream.")
    stream = cfg.get("model_stream") if isinstance(cfg.get("model_stream"), dict) else {}
    stream_name = str(stream.get("name") or "")
    expected_stream = str(cfg.get("required_model_stream_name") or "fresh_fullhist_m30_h1_h4_continuation_oanda_20260707")
    if stream_name != expected_stream:
        raise RuntimeError(f"Demo model contract failed: model_stream.name={stream_name!r}, expected {expected_stream!r}.")
    overlay = stream.get("m1_overlay") if isinstance(stream.get("m1_overlay"), dict) else {}
    if not overlay.get("enabled"):
        raise RuntimeError("Demo model contract failed: selected vault stream requires m1_overlay.enabled=true.")
    models = stream.get("models") if isinstance(stream.get("models"), list) else []
    if not models:
        raise RuntimeError("Demo model contract failed: model_stream.models is empty.")
    for index, model in enumerate(models):
        if not isinstance(model, dict):
            raise RuntimeError(f"Demo model contract failed: model #{index} is not an object.")
        if str(model.get("feature_set") or "") != "technical_full":
            raise RuntimeError(f"Demo model contract failed: model #{index} is not technical_full.")
        if safe_int(model.get("train_lookback_days"), safe_int(stream.get("train_lookback_days"), -1)) != 0:
            raise RuntimeError(f"Demo model contract failed: model #{index} is not full-history.")
        if str(model.get("execution_side_policy") or stream.get("execution_side_policy") or "") != "follow_momentum":
            raise RuntimeError(f"Demo model contract failed: model #{index} execution_side_policy is not the selected stream policy.")
        candidate_path = str(model.get("candidate_rows_csv") or stream.get("candidate_rows_csv") or "")
        if "fresh_rebuild_m30_h1_h4_fullhist_20260707" not in candidate_path:
            raise RuntimeError(f"Demo model contract failed: model #{index} candidate rows are not from the selected vault rebuild.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Primary forecast rotation bot.")
    parser.add_argument("--mode", choices=["advice", "paper", "demo", "live"], default=None)
    parser.add_argument("--source", choices=["local", "oanda"], default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-pairs", type=int, default=0)
    parser.add_argument("--config", default="", help="Path to a JSON config. Defaults to primary_forecast_rotation_bot.json.")
    parser.add_argument("--print-config", action="store_true")
    parser.add_argument("--write-default-config", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Enable broker execution for demo/live, subject to mode-specific gates.")
    parser.add_argument("--confirm-live", default="")
    parser.add_argument(
        "--practice-account-suffix",
        default="",
        help="Demo mode only: select a visible OANDA practice account by three-digit suffix, e.g. 019.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config) if args.config else CONFIG_PATH
    write_default_config(config_path)
    cfg = load_config(config_path)
    if args.practice_account_suffix:
        cfg["practice_account_aliases"] = []
        cfg["practice_account_suffix"] = args.practice_account_suffix
    if args.write_default_config:
        atomic_write_json(config_path, cfg)
        print(json.dumps({"wrote": str(config_path)}, indent=2))
        return 0
    mode = args.mode or str(cfg.get("default_mode") or "advice")
    source = args.source or str(cfg.get("default_data_source") or "local")
    args.mode = mode
    creds = load_creds()
    gateway, resolved = build_gateway_if_needed(mode, source, cfg)
    execute_live = live_execution_allowed(cfg, args, creds) or demo_execution_allowed(cfg, args, resolved)
    if args.print_config:
        print_resolved_config(cfg, resolved)
        if args.once is False:
            return 0
    if mode == "live" and not execute_live:
        print(
            json.dumps(
                {
                    "warning": "live mode selected but live execution is disabled; actions will be advice-only",
                    "requires": [
                        "--execute",
                        f"--confirm-live {cfg.get('live_confirmation_text')}",
                        "config live_execution_enabled=true",
                        "FOREX_ALLOW_LIVE=true",
                        "FOREX_LIVE_EXECUTE=true",
                    ],
                },
                indent=2,
            )
        )
    if mode == "demo" and not execute_live:
        print(
            json.dumps(
                {
                    "warning": "demo mode selected but demo broker execution is disabled; actions will be advice-only",
                    "requires": [
                        "--execute",
                        "config demo_execution_enabled=true",
                        "resolved env=practice",
                    ],
                },
                indent=2,
            )
        )
    if mode == "demo" and execute_live:
        validate_demo_model_stream_contract(cfg)
        enforce_empty_demo_account_on_first_start(cfg, gateway)
    engine = ForecastEngine(cfg, gateway=gateway)
    controller = PortfolioController(cfg, mode=mode, source=source, gateway=gateway, execute_live=execute_live)
    lock = ProcessLock(controller.data_dir / "process.lock", f"{mode}:{source}:{cfg.get('bot_id', 'primary_forecast_rotation')}")
    lock.acquire()
    try:
        while True:
            forecasts, meta = engine.forecast(source=source, max_pairs=max(0, int(args.max_pairs)))
            decision = controller.run_cycle(forecasts, meta)
            print(
                json.dumps(
                    {
                        "generated_utc": decision["generated_utc"],
                        "mode": mode,
                        "source": source,
                        "forecast_count": meta["forecast_count"],
                        "tradable_forecast_count": meta.get("tradable_forecast_count", 0),
                        "open_positions": len(decision["live_managed_positions"] if mode in BROKER_MODES else decision["open_positions"]),
                        "actions": decision["actions"][:5],
                        "top": decision["top_forecasts"][:5],
                        "outputs": {
                            "latest_decision": str(controller.latest_decision_path),
                            "latest_forecasts": str(controller.latest_forecasts_path),
                            "state": str(controller.state_path),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
            if args.once:
                return 0
            time.sleep(max(5, safe_int(cfg.get("loop_sleep_seconds"), 60)))
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
