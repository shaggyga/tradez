#!/usr/bin/env python3
"""Live GPT manager driven by technical position snapshots.

This wrapper reuses the existing OANDA GPT advisor execution engine, but points
it at an OANDA live account and gives GPT a compact technical setup
snapshot instead of the macro/news-heavy advisor mandate.

Default live account aliases:
    OANDA_ACCOUNT_LIVE_GPT_TECHNICAL
    OANDA_ACCOUNT_LIVE_GPT_TECH
    OANDA_ACCOUNT_ID_GPT_TECH_LIVE
    OANDA_ACCOUNT_LIVE_TECH
    OANDA_ACCOUNT_LIVE_MAIN

The original GPT advisor, GPT experimental, and live GPT production managers are
not modified or restarted by this file.
"""

from __future__ import annotations

import argparse
import json
import os
import math
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Tuple

import oanda_advisor_account_manager_auto as advisor


LIVE_CONFIRM_PHRASE = "GPT_TECH_SNAPSHOT_LIVE_REAL_MONEY"
LEGACY_LIVE_CONFIRM_PHRASE = "GPT_PROD_LIVE_REAL_MONEY"

TECH_LIVE_DATA_DIR = (
    advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_technical_snapshot_live"
)

GPT_PROD_LIVE_LOCK_PATH = (
    advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_prod_live"
    / "account_process.lock"
)

TECH_PROD_LIVE_LOCK_PATH = (
    advisor.SCRIPT_DIR
    / "data"
    / "technical_scout_manager"
    / "account_live_tech_broad_regime_scout"
    / "account_process.lock"
)


TECHNICAL_SYSTEM_PROMPT_APPEND = """

Technical-snapshot live lane mandate:
- This account is GPT_TECHNICAL_SNAPSHOT_LIVE and is connected to an OANDA live
  account.
- Your primary input is technical_position_snapshot. Use broker prices, candles,
  indicators, spread/ATR context, and potential_position templates as the basis
  for new opens.
- This lane is not a macro/news advisor. Do not require web/news confirmation
  before acting on a clean technical setup. Because this is live money, reject
  marginal setups and require clean stop geometry, spread control, and technical
  alignment.
- Manage existing open trades first. open_position_actions must return exactly
  one HOLD, TIGHTEN, PARTIAL_CLOSE, CLOSE, or FLIP action for every open trade.
- For new OPEN or SCALE_IN orders, choose only instruments and directions that
  appear in technical_position_snapshot.potential_positions unless you are
  flipping an existing open trade through open_position_actions.
- Any OPEN, SCALE_IN, or FLIP must include a stop_loss on the correct price
  scale for the instrument. Use take_profit and/or trailing_stop_pips when the
  technical setup supports it.
- Treat potential_position templates as executable geometry suggestions, not
  mandatory trades. You may tighten, reduce risk, or WATCH/REJECT any setup.
- Prefer fewer high-quality technical trades over broad exposure. Avoid opening
  multiple positions that express the same currency move unless the setups are
  independent and risk is small.
- If no setup is technically clean, return no executable OPEN orders and explain
  the blocker in underdeployment_reason: weak technical score, bad stop geometry,
  spread too wide, conflicting trend/momentum, crowded exposure, or market closed.
- Return JSON only matching the requested schema.
"""


def _cred_or_env(*names: str, default: str = "") -> str:
    for name in names:
        value = advisor.os.environ.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    for name in names:
        value = advisor.CREDS.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _cred_or_env_with_name(*names: str, default: str = "") -> Tuple[str, str]:
    for name in names:
        value = advisor.os.environ.get(name)
        if value is not None and str(value).strip():
            return str(value).strip(), name
    for name in names:
        value = advisor.CREDS.get(name)
        if value is not None and str(value).strip():
            return str(value).strip(), name
    return default, ""


def _setting_raw(name: str) -> Any:
    value = advisor.os.environ.get(name)
    if value is not None:
        return value
    return advisor.CREDS.get(name)


def _setting_str(name: str, default: str = "") -> str:
    value = _setting_raw(name)
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _setting_bool(name: str, default: bool = False) -> bool:
    value = _setting_raw(name)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _setting_float(name: str, default: float) -> float:
    value = _setting_raw(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def _setting_int(name: str, default: int) -> int:
    value = _setting_raw(name)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _tech_snapshot_min_score_default() -> float:
    return _setting_float(
        "FOREX_TECH_LIVE_MIN_SCORE",
        _setting_float("FOREX_TECH_SNAPSHOT_MIN_SCORE", 62.0),
    )


def _tech_snapshot_min_direction_edge_default() -> float:
    return _setting_float(
        "FOREX_TECH_LIVE_MIN_DIRECTION_EDGE",
        _setting_float("FOREX_TECH_SNAPSHOT_MIN_DIRECTION_EDGE", 6.0),
    )


def _technical_call_times() -> List[str]:
    interval = max(15, min(240, _setting_int("FOREX_TECH_LIVE_SCAN_INTERVAL_MINUTES", 30)))
    offset = max(0, min(59, _setting_int("FOREX_TECH_LIVE_SCAN_MINUTE_OFFSET", 15)))
    out: List[str] = []
    minute = offset
    while minute < 24 * 60:
        out.append(f"{minute // 60:02d}:{minute % 60:02d}")
        minute += interval
    return out or ["00:15"]


def _lock_file_busy(path: Path) -> bool:
    path = Path(path)
    if not path.exists():
        return False
    handle = None
    try:
        handle = path.open("r+b")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            return False
        except OSError:
            return True
    except OSError:
        return True
    finally:
        try:
            if handle is not None:
                handle.close()
        except Exception:
            pass


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if math.isfinite(number) else default


def _sma(values: List[float], period: int) -> float | None:
    if len(values) < period or period <= 0:
        return None
    return sum(values[-period:]) / period


def _ema(values: List[float], period: int) -> float | None:
    if len(values) < period or period <= 0:
        return None
    alpha = 2.0 / (period + 1.0)
    value = sum(values[:period]) / period
    for item in values[period:]:
        value = (item * alpha) + (value * (1.0 - alpha))
    return value


def _rsi(values: List[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for prev, cur in zip(values[-period - 1 : -1], values[-period:]):
        change = cur - prev
        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss <= 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _change_pips(values: List[float], bars: int, pip_size: float) -> float | None:
    if bars <= 0 or len(values) <= bars or pip_size <= 0:
        return None
    return (values[-1] - values[-1 - bars]) / pip_size


def _round_price(price: float, precision: int) -> float:
    return round(float(price), max(0, int(precision)))


def _pair_stop_floor_pips(instrument: str, spread_pips: float) -> float:
    base, quote = advisor.split_instrument(instrument)
    majors = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
    if base in majors and quote in majors:
        base_floor = 8.0
    elif base in majors or quote in majors:
        base_floor = 16.0
    else:
        base_floor = 24.0
    return max(base_floor, spread_pips * 4.0)


def _score_direction(tech: Dict[str, Any], direction: str) -> Tuple[float, List[str], str]:
    direction = direction.upper()
    trend = str(tech.get("trend") or "mixed")
    rsi = _finite(tech.get("rsi14"), 50.0)
    close_vs_ema20 = _finite(tech.get("close_vs_ema20_pips"), 0.0)
    close_vs_ema50 = _finite(tech.get("close_vs_ema50_pips"), 0.0)
    change_4 = _finite(tech.get("change_4_bars_pips"), 0.0)
    change_12 = _finite(tech.get("change_12_bars_pips"), 0.0)
    change_24 = _finite(tech.get("change_24_bars_pips"), 0.0)
    range_pos = _finite(tech.get("range_position_48"), 0.5)
    atr = max(_finite(tech.get("atr14_pips"), 0.0), 1e-9)

    signed = 1.0 if direction == "LONG" else -1.0
    score = 50.0
    reasons: List[str] = []

    if (direction == "LONG" and trend == "up") or (direction == "SHORT" and trend == "down"):
        score += 15.0
        reasons.append(f"{trend} EMA stack")
    elif trend != "mixed":
        score -= 12.0
        reasons.append(f"counter-trend versus {trend} EMA stack")
    else:
        reasons.append("mixed EMA stack")

    fast_momentum = signed * change_4
    session_momentum = signed * change_12
    broader_momentum = signed * change_24
    if fast_momentum > atr * 0.15:
        score += 6.0
        reasons.append("fast momentum agrees")
    elif fast_momentum < -atr * 0.15:
        score -= 6.0
        reasons.append("fast momentum disagrees")
    if session_momentum > atr * 0.35:
        score += 8.0
        reasons.append("session momentum agrees")
    elif session_momentum < -atr * 0.35:
        score -= 8.0
        reasons.append("session momentum disagrees")
    if broader_momentum > atr * 0.55:
        score += 6.0
        reasons.append("broader momentum agrees")
    elif broader_momentum < -atr * 0.55:
        score -= 6.0
        reasons.append("broader momentum disagrees")

    ema_alignment = signed * close_vs_ema20 + 0.5 * signed * close_vs_ema50
    if ema_alignment > atr * 0.20:
        score += 8.0
        reasons.append("price is on the favorable side of EMA20/EMA50")
    elif ema_alignment < -atr * 0.20:
        score -= 8.0
        reasons.append("price is on the wrong side of EMA20/EMA50")

    if direction == "LONG":
        if 45.0 <= rsi <= 68.0:
            score += 6.0
            reasons.append("RSI constructive without being overbought")
        elif rsi > 76.0:
            score -= 7.0
            reasons.append("RSI extended")
        elif rsi < 34.0 and fast_momentum > 0:
            score += 4.0
            reasons.append("possible long mean-reversion turn")
        if range_pos > 0.62:
            score += 4.0
            reasons.append("close is high in recent range")
        elif range_pos < 0.28:
            score -= 4.0
            reasons.append("close is low in recent range")
    else:
        if 32.0 <= rsi <= 55.0:
            score += 6.0
            reasons.append("RSI constructive for shorts without being oversold")
        elif rsi < 24.0:
            score -= 7.0
            reasons.append("RSI extended oversold")
        elif rsi > 66.0 and fast_momentum < 0:
            score += 4.0
            reasons.append("possible short mean-reversion turn")
        if range_pos < 0.38:
            score += 4.0
            reasons.append("close is low in recent range")
        elif range_pos > 0.72:
            score -= 4.0
            reasons.append("close is high in recent range")

    setup = "trend_continuation"
    if "possible long mean-reversion turn" in reasons or "possible short mean-reversion turn" in reasons:
        setup = "mean_reversion_turn"
    elif trend == "mixed":
        setup = "momentum_breakout" if score >= 60.0 else "mixed_watch"

    return max(0.0, min(100.0, score)), reasons[:8], setup


def live_confirmation_ok(args: argparse.Namespace) -> bool:
    confirmation = str(_setting_raw("FOREX_LIVE_CONFIRM") or "").strip()
    return bool(getattr(args, "i_understand_live_risk", False)) or confirmation in {
        LIVE_CONFIRM_PHRASE,
        LEGACY_LIVE_CONFIRM_PHRASE,
    }


def enforce_shared_live_account_guard(bot: "TechnicalSnapshotGPTManager") -> None:
    if not bot.should_execute():
        return
    if _setting_bool("FOREX_TECH_LIVE_ALLOW_SHARED_ACCOUNT", False):
        advisor.log(
            f"{bot.lane_prefix()}Shared live-account guard bypassed by "
            "FOREX_TECH_LIVE_ALLOW_SHARED_ACCOUNT=True."
        )
        return
    prod_live_account = _cred_or_env(
        "OANDA_ACCOUNT_ID_GPT_LIVE",
        "OANDA_ACCOUNT_LIVE_MAIN",
        "OANDA_LIVE_ACCOUNT_ID_GPT",
        "OANDA_ACCOUNT_ID_LIVE",
        "OANDA_LIVE_ACCOUNT_ID",
    )
    if (
        prod_live_account
        and str(prod_live_account).strip() == str(bot.cfg.oanda_account_id).strip()
        and _lock_file_busy(GPT_PROD_LIVE_LOCK_PATH)
    ):
        raise RuntimeError(
            "Existing GPT live account process lock is active for the same "
            f"OANDA live account ({bot.cfg.oanda_account_id}). Stop "
            "oanda_gpt_prod_live_account_manager.py or set "
            "FOREX_TECH_LIVE_ALLOW_SHARED_ACCOUNT=True before allowing this "
            "technical snapshot lane to execute live orders."
        )
    tech_prod_account = _cred_or_env(
        "OANDA_ACCOUNT_LIVE_TECH",
        "OANDA_ACCOUNT_ID_TECH_LIVE",
        "OANDA_ACCOUNT_ID_LIVE_TECH",
    )
    if (
        tech_prod_account
        and str(tech_prod_account).strip() == str(bot.cfg.oanda_account_id).strip()
        and _lock_file_busy(TECH_PROD_LIVE_LOCK_PATH)
    ):
        raise RuntimeError(
            "Existing technical live scout process lock is active for the same "
            f"OANDA live account ({bot.cfg.oanda_account_id}). Stop "
            "oanda_tech_prod_live_account_manager.py or set "
            "FOREX_TECH_LIVE_ALLOW_SHARED_ACCOUNT=True before allowing this "
            "technical snapshot lane to execute live orders."
        )


class TechnicalSnapshotProcessLock:
    def __init__(self, path: Path, description: str):
        self.path = path
        self.description = description
        self.handle: Any = None
        self.lock_impl = ""

    def _read_owner(self) -> str:
        try:
            self.handle.seek(0)
            return self.handle.read().decode("utf-8", errors="replace").strip()
        except Exception:
            return ""

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self.handle = self.path.open("r+b")
        if self.path.stat().st_size == 0:
            self.handle.write(b"\0")
            self.handle.flush()
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                self.lock_impl = "msvcrt"
            else:
                import fcntl

                self.handle.seek(0)
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.lock_impl = "fcntl"
        except OSError as exc:
            owner = self._read_owner()
            self.handle.close()
            self.handle = None
            detail = f" Existing owner: {owner}" if owner else ""
            raise RuntimeError(
                f"Another {self.description} process already holds {self.path}.{detail}"
            ) from exc

        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(
            (
                f"pid={os.getpid()}\n"
                f"account={self.description}\n"
                f"started_utc={advisor.iso_utc()}\n"
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


def build_technical_snapshot_config(
    base_cfg: advisor.BotConfig,
    *,
    execute_requested: bool,
    dry_run_requested: bool,
    confirmation_ok: bool,
) -> advisor.BotConfig:
    live_api_key = _cred_or_env(
        "OANDA_LIVE_API_KEY",
        "OANDA_API_KEY_LIVE",
        "OANDA_LIVE_API_TOKEN",
        "OANDA_API_TOKEN_LIVE",
    )
    account_id, account_alias = _cred_or_env_with_name(
        "OANDA_ACCOUNT_LIVE_GPT_TECHNICAL",
        "OANDA_ACCOUNT_LIVE_GPT_TECH",
        "OANDA_ACCOUNT_ID_GPT_TECH_LIVE",
        "OANDA_ACCOUNT_ID_GPT_TECHNICAL_LIVE",
        "OANDA_ACCOUNT_LIVE_TECH",
        "OANDA_ACCOUNT_ID_TECH_LIVE",
        "OANDA_ACCOUNT_ID_LIVE_TECH",
        "OANDA_ACCOUNT_LIVE_MAIN",
    )
    if not account_id:
        raise RuntimeError(
            "Missing OANDA_ACCOUNT_LIVE_GPT_TECHNICAL, OANDA_ACCOUNT_LIVE_TECH, "
            "or OANDA_ACCOUNT_LIVE_MAIN in creds."
        )

    execute_from_creds = _setting_bool("FOREX_LIVE_EXECUTE", False)
    max_risk_default = min(
        _setting_float("FOREX_LIVE_MAX_RISK_PCT_PER_TRADE", min(max(base_cfg.max_risk_pct_per_trade, 0.75), 2.0)),
        _setting_float("FOREX_TECH_LIVE_HARD_MAX_RISK_PCT_PER_TRADE", 1.0),
    )
    max_total_risk_default = min(
        _setting_float("FOREX_LIVE_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN", min(max(base_cfg.max_total_new_risk_pct_per_scan, 2.0), 4.0)),
        _setting_float("FOREX_TECH_LIVE_HARD_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN", 2.0),
    )
    allow_live = bool(_setting_bool("FOREX_ALLOW_LIVE", False) and confirmation_ok)
    call_times = _technical_call_times()
    cfg = replace(
        base_cfg,
        oanda_env="live",
        allow_live=allow_live,
        oanda_api_key=live_api_key,
        oanda_account_id=account_id,
        account_lane="gpt_tech_snapshot_live",
        account_display_name=f"{account_alias or 'OANDA_LIVE_ACCOUNT'} / GPT_TECHNICAL_SNAPSHOT_LIVE",
        instrument_filter_mode="all",
        gpt_reliable_pairs=[],
        technical_scout_pairs=[],
        enable_openai_web_search=False,
        scan_on_launch=base_cfg.scan_on_launch,
        calls_per_trading_day=len(call_times),
        call_times_ny=call_times,
        friday_call_times_ny=call_times,
        schedule_window_minutes=_setting_int("FOREX_TECH_LIVE_SCHEDULE_WINDOW_MINUTES", 5),
        min_minutes_between_gpt_scans=_setting_int("FOREX_TECH_LIVE_MIN_MINUTES_BETWEEN_GPT_SCANS", 30),
        loop_sleep_seconds=_setting_int("FOREX_TECH_LIVE_LOOP_SLEEP_SECONDS", 30),
        local_monitor_interval_minutes=_setting_int("FOREX_TECH_LIVE_LOCAL_MONITOR_INTERVAL_MINUTES", 5),
        execute_trades=bool((execute_requested or execute_from_creds) and confirmation_ok and not dry_run_requested),
        max_open_trades=_setting_int("FOREX_TECH_LIVE_MAX_OPEN_TRADES", _setting_int("FOREX_LIVE_MAX_OPEN_TRADES", min(max(base_cfg.max_open_trades, 6), 10))),
        max_new_trades_per_scan=_setting_int(
            "FOREX_TECH_LIVE_MAX_NEW_TRADES_PER_SCAN",
            _setting_int("FOREX_LIVE_MAX_NEW_TRADES_PER_SCAN", min(max(base_cfg.max_new_trades_per_scan, 2), 4)),
        ),
        max_total_new_risk_pct_per_scan=_setting_float(
            "FOREX_TECH_LIVE_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN",
            max_total_risk_default,
        ),
        max_risk_pct_per_trade=_setting_float(
            "FOREX_TECH_LIVE_MAX_RISK_PCT_PER_TRADE",
            max_risk_default,
        ),
        min_risk_pct_per_trade=_setting_float(
            "FOREX_TECH_LIVE_MIN_RISK_PCT",
            _setting_float("FOREX_LIVE_MIN_RISK_PCT", max(base_cfg.min_risk_pct_per_trade, 0.05)),
        ),
        target_margin_used_pct=_setting_float(
            "FOREX_TECH_LIVE_TARGET_MARGIN_USED_PCT",
            _setting_float("FOREX_LIVE_TARGET_MARGIN_USED_PCT", min(max(base_cfg.target_margin_used_pct, 25.0), 45.0)),
        ),
        max_margin_used_pct=_setting_float(
            "FOREX_TECH_LIVE_MAX_MARGIN_USED_PCT",
            _setting_float("FOREX_LIVE_MAX_MARGIN_USED_PCT", min(max(base_cfg.max_margin_used_pct, 45.0), 65.0)),
        ),
        emergency_margin_used_pct=_setting_float(
            "FOREX_TECH_LIVE_EMERGENCY_MARGIN_USED_PCT",
            _setting_float("FOREX_LIVE_EMERGENCY_MARGIN_USED_PCT", min(max(base_cfg.emergency_margin_used_pct, 70.0), 80.0)),
        ),
        max_one_currency_net_units_pct=_setting_float(
            "FOREX_TECH_LIVE_MAX_ONE_CURRENCY_NET_UNITS_PCT",
            _setting_float("FOREX_LIVE_MAX_ONE_CURRENCY_NET_UNITS_PCT", min(base_cfg.max_one_currency_net_units_pct, 45.0)),
        ),
        require_stop_loss_on_open=True,
        require_take_profit_on_open=True,
        allow_trailing_stop_on_open=True,
        max_spread_pips_default=_setting_float(
            "FOREX_TECH_LIVE_MAX_SPREAD_PIPS_DEFAULT",
            _setting_float("FOREX_LIVE_MAX_SPREAD_PIPS_DEFAULT", base_cfg.max_spread_pips_default),
        ),
        max_spread_pips_exotic=_setting_float(
            "FOREX_TECH_LIVE_MAX_SPREAD_PIPS_EXOTIC",
            _setting_float("FOREX_LIVE_MAX_SPREAD_PIPS_EXOTIC", base_cfg.max_spread_pips_exotic),
        ),
        max_spread_to_stop_ratio=_setting_float(
            "FOREX_TECH_LIVE_MAX_SPREAD_TO_STOP_RATIO",
            _setting_float("FOREX_LIVE_MAX_SPREAD_TO_STOP_RATIO", min(base_cfg.max_spread_to_stop_ratio, 0.18)),
        ),
        max_entry_slippage_pips=_setting_float(
            "FOREX_TECH_LIVE_MAX_ENTRY_SLIPPAGE_PIPS",
            _setting_float("FOREX_LIVE_MAX_ENTRY_SLIPPAGE_PIPS", min(base_cfg.max_entry_slippage_pips, 2.0)),
        ),
        require_cross_pair_coverage=False,
        min_non_usd_candidates=0,
        event_scanner_enabled=False,
        event_trigger_gpt_enabled=False,
        event_scout_trades_enabled=False,
        event_require_gpt_permission=True,
        broker_recheck_before_actions=True,
        retry_unsafe_broker_writes=False,
    )
    return advisor.with_data_dir(cfg, TECH_LIVE_DATA_DIR)


class TechnicalSnapshotGPTManager(advisor.ForexManager):
    """Advisor engine with an added technical potential-position packet."""

    def __init__(self, cfg: advisor.BotConfig):
        super().__init__(cfg)
        self._last_candidate_instruments: set[str] = set()
        self._last_candidate_templates: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._last_handoff_manage_only: bool = False
        self._last_handoff_manage_only_reasons: List[str] = []

    def _valid_instruments(self) -> set[str]:
        return {
            advisor.normalize_instrument(inst)
            for inst in self.instruments
            if advisor.normalize_instrument(inst)
        }

    def _handoff_manage_only_state(
        self,
        account: Dict[str, Any],
        open_trades: List[Dict[str, Any]],
    ) -> Tuple[bool, List[str], Dict[str, Any]]:
        enabled = _setting_bool("FOREX_TECH_LIVE_HANDOFF_MANAGE_ONLY_ENABLED", True)
        max_open_trades = max(0, _setting_int("FOREX_TECH_LIVE_HANDOFF_FRESH_OPEN_MAX_OPEN_TRADES", 3))
        max_margin_pct = max(
            0.0,
            _setting_float(
                "FOREX_TECH_LIVE_HANDOFF_FRESH_OPEN_MAX_MARGIN_USED_PCT",
                self.cfg.target_margin_used_pct,
            ),
        )
        margin_used_pct = _finite(
            account.get("margin_used_pct_of_nav", account.get("margin_used_pct")),
            0.0,
        )
        account_open_count = max(
            len(open_trades or []),
            advisor.safe_int(account.get("open_trade_count", account.get("openTradeCount")), 0),
        )
        reasons: List[str] = []
        if enabled:
            if account_open_count > max_open_trades:
                reasons.append(
                    f"open_trades {account_open_count} > handoff fresh-open max {max_open_trades}"
                )
            if margin_used_pct >= max_margin_pct:
                reasons.append(
                    f"margin_used_pct {margin_used_pct:.2f} >= handoff fresh-open max {max_margin_pct:.2f}"
                )
        return bool(reasons), reasons, {
            "enabled": enabled,
            "manage_only": bool(reasons),
            "current_open_trades": account_open_count,
            "current_margin_used_pct": margin_used_pct,
            "fresh_open_max_open_trades": max_open_trades,
            "fresh_open_max_margin_used_pct": max_margin_pct,
            "reasons": reasons,
            "policy": (
                "When manage_only is true, GPT may manage existing open trades but "
                "fresh OPEN/SCALE_IN orders are converted to WATCH before execution."
            ),
        }

    def _snapshot_by_instrument(self, packet: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for snap in packet.get("market_snapshots", []) or []:
            if not isinstance(snap, dict):
                continue
            inst = advisor.normalize_instrument(snap.get("instrument"))
            if inst:
                out[inst] = snap
        return out

    def _indicator_state(
        self,
        instrument: str,
        rows: List[Dict[str, float]],
        meta: advisor.InstrumentMeta,
        market_snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        closes = [float(row["c"]) for row in rows if "c" in row]
        highs = [float(row["h"]) for row in rows if "h" in row]
        lows = [float(row["l"]) for row in rows if "l" in row]
        if not closes:
            return {"available": False}

        ema20 = _ema(closes, 20)
        ema50 = _ema(closes, 50)
        ema100 = _ema(closes, 100)
        sma20 = _sma(closes, 20)
        rsi14 = _rsi(closes, 14)
        atr14 = advisor.atr_pips(rows, meta.pip_size, 14)
        last_close = closes[-1]

        trend = "mixed"
        if ema20 is not None and ema50 is not None and ema100 is not None:
            if ema20 > ema50 > ema100:
                trend = "up"
            elif ema20 < ema50 < ema100:
                trend = "down"

        lookback = min(48, len(rows))
        recent_high = max(highs[-lookback:]) if highs and lookback else last_close
        recent_low = min(lows[-lookback:]) if lows and lookback else last_close
        span = max(recent_high - recent_low, 1e-12)
        range_position = (last_close - recent_low) / span

        return {
            "available": True,
            "instrument": instrument,
            "last_close": last_close,
            "bid": market_snapshot.get("bid"),
            "ask": market_snapshot.get("ask"),
            "mid": market_snapshot.get("mid"),
            "spread_pips": market_snapshot.get("spread_pips"),
            "atr14_pips": atr14,
            "sma20": sma20,
            "ema20": ema20,
            "ema50": ema50,
            "ema100": ema100,
            "close_vs_ema20_pips": ((last_close - ema20) / meta.pip_size) if ema20 else None,
            "close_vs_ema50_pips": ((last_close - ema50) / meta.pip_size) if ema50 else None,
            "rsi14": rsi14,
            "trend": trend,
            "range_high_48": recent_high,
            "range_low_48": recent_low,
            "range_position_48": range_position,
            "change_4_bars_pips": _change_pips(closes, 4, meta.pip_size),
            "change_12_bars_pips": _change_pips(closes, 12, meta.pip_size),
            "change_24_bars_pips": _change_pips(closes, 24, meta.pip_size),
        }

    def _position_template(
        self,
        instrument: str,
        direction: str,
        tech: Dict[str, Any],
        market_snapshot: Dict[str, Any],
        meta: advisor.InstrumentMeta,
    ) -> Dict[str, Any] | None:
        bid = advisor.as_optional_float(market_snapshot.get("bid"))
        ask = advisor.as_optional_float(market_snapshot.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None

        direction = direction.upper()
        entry = ask if direction == "LONG" else bid
        spread = max(_finite(market_snapshot.get("spread_pips"), 0.0), 0.0)
        atr = max(_finite(tech.get("atr14_pips"), 0.0), 1.0)
        stop_pips = max(atr * 1.15, _pair_stop_floor_pips(instrument, spread))
        take_profit_r = 1.8
        trail_pips = max(atr * 0.75, stop_pips * 0.45)
        spread_to_stop_ratio = spread / stop_pips if stop_pips > 0 else 999.0

        if direction == "LONG":
            stop_loss = entry - stop_pips * meta.pip_size
            take_profit = entry + stop_pips * take_profit_r * meta.pip_size
        else:
            stop_loss = entry + stop_pips * meta.pip_size
            take_profit = entry - stop_pips * take_profit_r * meta.pip_size

        score, reasons, setup = _score_direction(tech, direction)
        risk = max(
            self.cfg.min_risk_pct_per_trade,
            min(self.cfg.max_risk_pct_per_trade, (score - 52.0) / 35.0 * self.cfg.max_risk_pct_per_trade),
        )
        if score < 54.0:
            risk = 0.0
        quality_blocks: List[str] = []
        max_spread = self.max_spread_allowed(instrument)
        if spread > max_spread:
            risk = 0.0
            quality_blocks.append(f"spread {spread:.2f}p exceeds max {max_spread:.2f}p")
        if spread_to_stop_ratio > self.cfg.max_spread_to_stop_ratio:
            risk = 0.0
            quality_blocks.append(
                f"spread/stop {spread_to_stop_ratio:.3f} exceeds max {self.cfg.max_spread_to_stop_ratio:.3f}"
            )

        return {
            "instrument": instrument,
            "direction": direction,
            "technical_score": round(score, 2),
            "setup_type": setup,
            "suggested_entry": _round_price(entry, meta.display_precision),
            "suggested_stop_loss": _round_price(stop_loss, meta.display_precision),
            "suggested_take_profit": _round_price(take_profit, meta.display_precision),
            "suggested_trailing_stop_pips": round(trail_pips, 2),
            "stop_pips": round(stop_pips, 2),
            "reward_r_multiple": take_profit_r,
            "spread_pips": round(spread, 3),
            "max_spread_pips": round(max_spread, 3),
            "spread_to_stop_ratio": round(spread_to_stop_ratio, 4),
            "max_spread_to_stop_ratio": round(self.cfg.max_spread_to_stop_ratio, 4),
            "atr14_pips": round(atr, 3),
            "risk_pct_suggestion": round(risk, 3),
            "quality_blocks": quality_blocks,
            "candidate_status": "WATCH_ONLY" if quality_blocks or risk <= 0 else "OPEN_ELIGIBLE",
            "technical_reasons": reasons,
            "invalidations": [
                "trend/momentum reverses against the setup",
                "spread widens enough to damage stop geometry",
                "price breaks the suggested stop-loss area",
            ],
            "allowed_order_template": {
                "instrument": instrument,
                "action": "OPEN",
                "direction": direction,
                "risk_pct": round(risk, 3),
                "stop_loss": _round_price(stop_loss, meta.display_precision),
                "take_profit": _round_price(take_profit, meta.display_precision),
                "trailing_stop_pips": round(trail_pips, 2),
            },
        }

    def _build_technical_position_snapshot(
        self,
        packet: Dict[str, Any],
        open_trades: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        granularity = _setting_str("FOREX_TECH_SNAPSHOT_CANDLE_GRANULARITY", "M15")
        candle_count = _setting_int("FOREX_TECH_SNAPSHOT_CANDLE_COUNT", 160)
        max_candidates = _setting_int("FOREX_TECH_SNAPSHOT_MAX_CANDIDATES", 50)
        min_score = _tech_snapshot_min_score_default()
        min_direction_edge = _tech_snapshot_min_direction_edge_default()

        snapshots = self._snapshot_by_instrument(packet)
        candidates: List[Dict[str, Any]] = []
        pair_table: List[Dict[str, Any]] = []
        candle_errors: List[Dict[str, str]] = []
        open_instruments = {
            advisor.normalize_instrument(trade.get("instrument"))
            for trade in open_trades
            if advisor.normalize_instrument(trade.get("instrument"))
        }

        for instrument in self.instruments:
            inst = advisor.normalize_instrument(instrument)
            meta = self.instrument_meta.get(inst)
            market_snapshot = snapshots.get(inst, {})
            if not inst or not meta or not market_snapshot:
                continue
            if not advisor.price_tradeable({"tradeable": market_snapshot.get("tradeable", True)}):
                continue
            try:
                candles = self.oanda.get_candles(inst, granularity, candle_count)
                rows = advisor.candles_to_rows(candles)
            except Exception as exc:
                if len(candle_errors) < 12:
                    candle_errors.append({"instrument": inst, "error": str(exc)[:160]})
                continue
            if len(rows) < 55:
                continue

            tech = self._indicator_state(inst, rows, meta, market_snapshot)
            if not tech.get("available"):
                continue
            long_candidate = self._position_template(inst, "LONG", tech, market_snapshot, meta)
            short_candidate = self._position_template(inst, "SHORT", tech, market_snapshot, meta)
            long_score = _finite((long_candidate or {}).get("technical_score"), 0.0)
            short_score = _finite((short_candidate or {}).get("technical_score"), 0.0)
            best_direction = "LONG" if long_score >= short_score else "SHORT"

            pair_table.append(
                {
                    "instrument": inst,
                    "trend": tech.get("trend"),
                    "rsi14": round(_finite(tech.get("rsi14"), 0.0), 2),
                    "atr14_pips": round(_finite(tech.get("atr14_pips"), 0.0), 2),
                    "spread_pips": round(_finite(market_snapshot.get("spread_pips"), 0.0), 3),
                    "change_4_bars_pips": round(_finite(tech.get("change_4_bars_pips"), 0.0), 2),
                    "change_12_bars_pips": round(_finite(tech.get("change_12_bars_pips"), 0.0), 2),
                    "change_24_bars_pips": round(_finite(tech.get("change_24_bars_pips"), 0.0), 2),
                    "range_position_48": round(_finite(tech.get("range_position_48"), 0.5), 3),
                    "long_score": round(long_score, 2),
                    "short_score": round(short_score, 2),
                    "best_direction": best_direction,
                    "already_open": inst in open_instruments,
                }
            )

            for candidate in (long_candidate, short_candidate):
                if not candidate:
                    continue
                direction = str(candidate.get("direction") or "").upper().strip()
                score = _finite(candidate.get("technical_score"), 0.0)
                opposite_score = short_score if direction == "LONG" else long_score
                direction_edge = score - opposite_score
                candidate["opposite_direction_score"] = round(opposite_score, 2)
                candidate["directional_edge"] = round(direction_edge, 2)
                candidate["min_directional_edge"] = round(min_direction_edge, 2)
                if direction_edge < min_direction_edge and inst not in open_instruments:
                    risk_blocks = candidate.setdefault("quality_blocks", [])
                    if isinstance(risk_blocks, list):
                        risk_blocks.append(
                            f"directional edge {direction_edge:.2f} below min {min_direction_edge:.2f}"
                        )
                    candidate["risk_pct_suggestion"] = 0.0
                    if isinstance(candidate.get("allowed_order_template"), dict):
                        candidate["allowed_order_template"]["risk_pct"] = 0.0
                    candidate["candidate_status"] = "WATCH_ONLY"
                if _finite(candidate.get("technical_score"), 0.0) >= min_score or inst in open_instruments:
                    candidates.append(candidate)

        candidates.sort(
            key=lambda item: (
                advisor.normalize_instrument(item.get("instrument")) in open_instruments,
                _finite(item.get("technical_score"), 0.0),
                _finite(item.get("risk_pct_suggestion"), 0.0),
            ),
            reverse=True,
        )
        pair_table.sort(
            key=lambda item: max(_finite(item.get("long_score"), 0.0), _finite(item.get("short_score"), 0.0)),
            reverse=True,
        )
        selected = candidates[: max(1, max_candidates)]
        self._last_candidate_instruments = {
            advisor.normalize_instrument(item.get("instrument"))
            for item in selected
            if advisor.normalize_instrument(item.get("instrument"))
        } | open_instruments
        self._last_candidate_templates = {
            (
                advisor.normalize_instrument(item.get("instrument")),
                str(item.get("direction") or "").upper().strip(),
            ): item
            for item in selected
            if advisor.normalize_instrument(item.get("instrument"))
            and str(item.get("direction") or "").upper().strip() in {"LONG", "SHORT"}
        }

        snapshot = {
            "source": "OANDA live pricing and candles",
            "granularity": granularity,
            "candle_count_requested": candle_count,
            "candidate_count": len(selected),
            "min_score": min_score,
            "min_directional_edge": min_direction_edge,
            "open_eligible_count": sum(
                1
                for item in selected
                if str(item.get("candidate_status") or "").upper() == "OPEN_ELIGIBLE"
                and _finite(item.get("risk_pct_suggestion"), 0.0) > 0
            ),
            "potential_positions": selected,
            "pair_technical_table": pair_table[: max(1, max_candidates)],
            "candle_errors": candle_errors,
            "usage": {
                "new_opens": (
                    "For OPEN/SCALE_IN orders, choose from potential_positions and copy or "
                    "tighten the suggested stop/take/trailing geometry."
                ),
                "existing_positions": (
                    "Existing open trades may be held, tightened, partially closed, closed, "
                    "or flipped even when their instrument is not a top new-open candidate."
                ),
            },
        }
        self._write_technical_snapshot_monitor(snapshot, open_trades)
        return snapshot

    def _write_technical_snapshot_monitor(
        self,
        snapshot: Dict[str, Any],
        open_trades: List[Dict[str, Any]],
    ) -> None:
        try:
            positions = [
                item
                for item in (snapshot.get("potential_positions") or [])
                if isinstance(item, dict)
            ]
            open_eligible = [
                item
                for item in positions
                if str(item.get("candidate_status") or "").upper() == "OPEN_ELIGIBLE"
                and _finite(item.get("risk_pct_suggestion"), 0.0) > 0
            ]
            watch_only = [
                item
                for item in positions
                if str(item.get("candidate_status") or "").upper() != "OPEN_ELIGIBLE"
                or _finite(item.get("risk_pct_suggestion"), 0.0) <= 0
            ]
            summary = {
                "time_utc": advisor.iso_utc(),
                "time_ny": advisor.iso_ny(),
                "account_lane": self.cfg.account_lane,
                "oanda_account_id": self.cfg.oanda_account_id,
                "granularity": snapshot.get("granularity"),
                "candle_count_requested": snapshot.get("candle_count_requested"),
                "min_score": snapshot.get("min_score"),
                "min_directional_edge": snapshot.get("min_directional_edge"),
                "candidate_count": len(positions),
                "open_eligible_count": len(open_eligible),
                "watch_only_count": len(watch_only),
                "open_trade_count": len(open_trades or []),
                "candle_error_count": len(snapshot.get("candle_errors") or []),
                "top_candidates": [
                    {
                        "instrument": item.get("instrument"),
                        "direction": item.get("direction"),
                        "candidate_status": item.get("candidate_status"),
                        "technical_score": item.get("technical_score"),
                        "opposite_direction_score": item.get("opposite_direction_score"),
                        "directional_edge": item.get("directional_edge"),
                        "risk_pct_suggestion": item.get("risk_pct_suggestion"),
                        "spread_pips": item.get("spread_pips"),
                        "spread_to_stop_ratio": item.get("spread_to_stop_ratio"),
                        "stop_pips": item.get("stop_pips"),
                        "quality_blocks": item.get("quality_blocks") or [],
                    }
                    for item in positions[:12]
                ],
            }
            latest = dict(summary)
            latest["potential_positions"] = positions
            latest["pair_technical_table"] = snapshot.get("pair_technical_table") or []
            data_dir = self.cfg.data_dir
            advisor.write_json(data_dir / "latest_technical_snapshot.json", latest)
            ledger_path = data_dir / "technical_snapshot_ledger.jsonl"
            ledger_path.parent.mkdir(parents=True, exist_ok=True)
            with ledger_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(summary, sort_keys=True, default=str) + "\n")
        except Exception as exc:
            self.log_error("write technical snapshot monitor", exc)

    def build_market_packet(
        self,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        packet, prices, open_trades, raw_context = super().build_market_packet()

        instructions = packet.setdefault("instructions", {})
        instructions["technical_snapshot_lane"] = (
            "This scan is for a live GPT technical-snapshot lane. Use the "
            "technical_position_snapshot potential positions as the primary source "
            "for new trade selection."
        )
        instructions["new_open_constraint"] = (
            "New OPEN/SCALE_IN orders must use instruments and directions from "
            "technical_position_snapshot.potential_positions and must only use "
            "templates where candidate_status=OPEN_ELIGIBLE and risk_pct_suggestion "
            "is positive. Use WATCH/REJECT when technical quality, directional edge, "
            "spread, or stop geometry is not good enough."
        )
        instructions["existing_trade_management"] = (
            "Manage every supplied open trade explicitly. You may close, tighten, "
            "partial-close, or flip if the technical setup has degraded or reversed."
        )
        instructions["news_policy"] = (
            "OpenAI web search is disabled for this lane; do not pretend to have "
            "fresh external news. Base decisions on the supplied broker snapshot."
        )

        guardrails = packet.setdefault("config_guardrails", {})
        guardrails["route_policy"] = (
            "GPT technical snapshot live: all supplied OANDA currency pairs are "
            "eligible, but new opens must come from the technical potential-position snapshot."
        )
        guardrails["technical_snapshot"] = {
            "web_search_enabled": self.cfg.enable_openai_web_search,
            "max_candidates": _setting_int("FOREX_TECH_SNAPSHOT_MAX_CANDIDATES", 50),
            "min_score": _tech_snapshot_min_score_default(),
            "min_directional_edge": _tech_snapshot_min_direction_edge_default(),
            "open_eligibility": (
                "Potential positions marked WATCH_ONLY or risk_pct_suggestion=0 "
                "are not executable live opens."
            ),
            "event_scout_trades_enabled": False,
        }
        if isinstance(guardrails.get("event_scanner"), dict):
            guardrails["event_scanner"]["enabled"] = False
            guardrails["event_scanner"]["scout_trades_enabled"] = False

        manage_only, handoff_reasons, handoff = self._handoff_manage_only_state(
            packet.get("account", {}),
            open_trades,
        )
        self._last_handoff_manage_only = manage_only
        self._last_handoff_manage_only_reasons = list(handoff_reasons)
        guardrails["technical_snapshot"]["handoff_manage_only"] = handoff
        if manage_only:
            reason_text = "; ".join(handoff_reasons)
            instructions["handoff_manage_only"] = (
                "The live technical account is in handoff manage-only mode because "
                f"{reason_text}. Manage existing positions first. Do not submit fresh "
                "OPEN or SCALE_IN orders until the handoff limits clear."
            )

        packet["technical_position_snapshot"] = self._build_technical_position_snapshot(packet, open_trades)
        output_contract = packet.setdefault("output_contract", {})
        output_contract["orders_to_execute"] = (
            "Only include executable OPEN/SCALE_IN orders selected from "
            "technical_position_snapshot.potential_positions with "
            "candidate_status=OPEN_ELIGIBLE and positive risk_pct_suggestion. "
            "Include stop_loss and same-scale take_profit/trailing_stop_pips when opening."
        )
        output_contract["new_trade_candidates"] = (
            "Use OPEN/WATCH/REJECT to explain the strongest technical setups from "
            "technical_position_snapshot.potential_positions. Do not invent symbols."
        )
        if manage_only:
            output_contract["handoff_manage_only"] = (
                "Fresh OPEN/SCALE_IN orders are not executable while handoff_manage_only "
                "is true; return WATCH/REJECT candidates and only manage supplied open trades."
            )
        return packet, prices, open_trades, raw_context

    def _trade_profit_pips(
        self,
        trade: Dict[str, Any],
        price: Dict[str, Any],
        meta: advisor.InstrumentMeta,
    ) -> Tuple[float, float, str]:
        direction = advisor.trade_direction_from_units(trade.get("currentUnits"))
        entry = advisor.trade_entry_price(trade)
        if entry is None or entry <= 0:
            return 0.0, 0.0, "entry price unavailable"
        bid, ask = advisor.bid_ask(price)
        if bid is None or ask is None:
            return 0.0, 0.0, "bid/ask unavailable"
        exit_price = bid if direction == "LONG" else ask
        if direction == "LONG":
            profit_pips = (exit_price - entry) / meta.pip_size
        else:
            profit_pips = (entry - exit_price) / meta.pip_size
        return profit_pips, exit_price, ""

    def _profit_guard_stop(
        self,
        trade: Dict[str, Any],
        price: Dict[str, Any],
        meta: advisor.InstrumentMeta,
        profit_pips: float,
        exit_price: float,
    ) -> Tuple[float | None, str]:
        direction = advisor.trade_direction_from_units(trade.get("currentUnits"))
        entry = advisor.trade_entry_price(trade)
        if entry is None or entry <= 0:
            return None, "entry price unavailable"
        spread = advisor.spread_pips(price, meta)
        min_gap_pips = max(1.0, spread * 2.0)
        lock_fraction = _setting_float("FOREX_TECH_LIVE_PROFIT_LOCK_RATIO", 0.35)
        min_lock_pips = _setting_float("FOREX_TECH_LIVE_MIN_LOCK_PROFIT_PIPS", 1.0)
        lock_pips = max(min_lock_pips, profit_pips * max(0.0, min(lock_fraction, 0.8)))
        min_gap_price = min_gap_pips * meta.pip_size

        if direction == "LONG":
            candidate = entry + lock_pips * meta.pip_size
            candidate = advisor.round_stop_to_valid_side(direction, candidate, exit_price, min_gap_price)
            if candidate >= exit_price:
                return None, "computed LONG stop not below market"
        else:
            candidate = entry - lock_pips * meta.pip_size
            candidate = advisor.round_stop_to_valid_side(direction, candidate, exit_price, min_gap_price)
            if candidate <= exit_price:
                return None, "computed SHORT stop not above market"

        existing = advisor.existing_stop_price_from_trade(trade)
        if not advisor.stop_is_more_protective(direction, candidate, existing):
            return None, "existing stop already tighter or equal"
        return candidate, ""

    def run_technical_live_profit_guard(
        self,
        *,
        reason: str = "technical_live_profit_guard",
    ) -> None:
        if not _setting_bool("FOREX_TECH_LIVE_PROFIT_GUARD_ENABLED", True):
            return
        try:
            open_trades = self.oanda.get_open_trades()
        except Exception as exc:
            self.log_error("technical live profit guard open trades", exc)
            return
        if not open_trades:
            return
        instruments = sorted(
            {
                advisor.normalize_instrument(trade.get("instrument"))
                for trade in open_trades
                if advisor.normalize_instrument(trade.get("instrument"))
            }
        )
        try:
            prices = self.oanda.get_prices(instruments)
        except Exception as exc:
            self.log_error("technical live profit guard prices", exc)
            return

        partial_profit_pips = _setting_float("FOREX_TECH_LIVE_PARTIAL_PROFIT_PIPS", 5.0)
        full_profit_pips = _setting_float("FOREX_TECH_LIVE_FULL_PROFIT_PIPS", 12.0)
        max_loss_pips = abs(_setting_float("FOREX_TECH_LIVE_MAX_LOSS_PIPS", 9.0))
        min_profit_usd = _setting_float("FOREX_TECH_LIVE_MIN_PROFIT_USD_TO_ACT", 0.03)
        partial_pct = _setting_float("FOREX_TECH_LIVE_PROFIT_PARTIAL_CLOSE_PCT", 50.0)
        tighten_profit_pips = _setting_float("FOREX_TECH_LIVE_TIGHTEN_PROFIT_PIPS", 3.0)

        for trade in open_trades:
            inst = advisor.normalize_instrument(trade.get("instrument"))
            trade_id = str(trade.get("id") or "").strip()
            meta = self.instrument_meta.get(inst)
            price = prices.get(inst, {})
            if not inst or not trade_id or not meta or not price:
                continue
            profit_pips, exit_price, reject = self._trade_profit_pips(trade, price, meta)
            direction = advisor.trade_direction_from_units(trade.get("currentUnits"))
            unrealized_pl = advisor.safe_float(trade.get("unrealizedPL"), 0.0)
            action_base = {
                "instrument": inst,
                "trade_id": trade_id,
                "direction": direction,
                "outlook_confidence": 0,
                "risk_pct": 0,
            }
            if reject:
                self.log_action(
                    {**action_base, "action": "HOLD", "reason": f"{reason}: {reject}"},
                    "profit_guard",
                    "skipped",
                    reject_reason=reject,
                )
                continue

            if full_profit_pips > 0 and profit_pips >= full_profit_pips and unrealized_pl >= min_profit_usd:
                action = {
                    **action_base,
                    "action": "CLOSE",
                    "reason": f"{reason}: full close winner profit_pips={profit_pips:.1f}, unrealizedPL={unrealized_pl:.2f}",
                }
                akey = self.action_key("TECH_PROFIT_GUARD_CLOSE_WINNER", action, trade_id=trade_id)
                if not self.recently_attempted_action(akey):
                    status = self.close_trade(trade, action)
                    if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                        self.remember_action(akey)
                continue

            if max_loss_pips > 0 and profit_pips <= -max_loss_pips and unrealized_pl < 0:
                action = {
                    **action_base,
                    "action": "CLOSE",
                    "reason": f"{reason}: close loser before full stop profit_pips={profit_pips:.1f}, unrealizedPL={unrealized_pl:.2f}",
                }
                akey = self.action_key("TECH_PROFIT_GUARD_CLOSE_LOSER", action, trade_id=trade_id)
                if not self.recently_attempted_action(akey):
                    status = self.close_trade(trade, action)
                    if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                        self.remember_action(akey)
                continue

            if partial_profit_pips > 0 and profit_pips >= partial_profit_pips and unrealized_pl >= min_profit_usd:
                action = {
                    **action_base,
                    "action": "PARTIAL_CLOSE",
                    "partial_close_pct": partial_pct,
                    "reason": f"{reason}: partial close winner profit_pips={profit_pips:.1f}, unrealizedPL={unrealized_pl:.2f}",
                }
                akey = self.action_key("TECH_PROFIT_GUARD_PARTIAL", action, trade_id=trade_id)
                if not self.recently_attempted_action(akey):
                    status = self.partial_close_trade(trade, action)
                    if status in {"accepted", "dry_run", "accepted_after_recheck"}:
                        self.remember_action(akey)

            if tighten_profit_pips > 0 and profit_pips >= tighten_profit_pips:
                stop_loss, stop_reject = self._profit_guard_stop(trade, price, meta, profit_pips, exit_price)
                if stop_loss is None:
                    self.log_action(
                        {**action_base, "action": "TIGHTEN", "reason": f"{reason}: profit_pips={profit_pips:.1f}"},
                        "profit_guard_tighten",
                        "skipped",
                        reject_reason=stop_reject,
                    )
                    continue
                action = {
                    **action_base,
                    "action": "TIGHTEN",
                    "stop_loss": stop_loss,
                    "take_profit": None,
                    "trailing_stop_pips": None,
                    "reason": f"{reason}: lock profit with tighter stop; profit_pips={profit_pips:.1f}",
                }
                akey = self.action_key("TECH_PROFIT_GUARD_TIGHTEN", action, trade_id=trade_id)
                if not self.recently_attempted_action(akey):
                    status = self.tighten_trade(trade_id, action, meta)
                    if status in {"accepted", "dry_run", "uncertain_after_recheck"}:
                        self.remember_action(akey)

    def run_local_monitor(self) -> None:
        super().run_local_monitor()
        try:
            self.run_technical_live_profit_guard(reason="local_monitor")
        except Exception as exc:
            self.log_error("technical live profit guard from local monitor", exc)

    def run_gpt_scan(self, reason: str = "scheduled") -> None:
        if not (advisor.fx_market_closed() and reason != "manual"):
            try:
                self.run_technical_live_profit_guard(reason=f"pre_gpt_scan:{reason}")
            except Exception as exc:
                self.log_error("technical live profit guard before GPT scan", exc)
        return super().run_gpt_scan(reason=reason)

    def normalize_decision(self, decision: Dict[str, Any], open_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        decision = super().normalize_decision(decision, open_trades)
        valid_instruments = self._valid_instruments()
        candidate_instruments = set(self._last_candidate_instruments or set())
        candidate_templates = dict(self._last_candidate_templates or {})
        min_live_score = _tech_snapshot_min_score_default()
        notes = decision.setdefault("risk_notes", [])
        if not isinstance(notes, list):
            notes = []
            decision["risk_notes"] = notes

        def add_note(text: str) -> None:
            notes.append(text)
            advisor.log(f"{self.lane_prefix()}[WARN] {text}")

        blocked_handoff_fresh = 0
        handoff_reason = "; ".join(self._last_handoff_manage_only_reasons[:3])
        if not handoff_reason:
            handoff_reason = "handoff exposure limit active"

        for key in ("new_trade_candidates", "orders_to_execute"):
            kept: List[Dict[str, Any]] = []
            dropped_invalid = 0
            dropped_off_snapshot = 0
            for obj in decision.get(key, []) or []:
                if not isinstance(obj, dict):
                    continue
                inst = advisor.normalize_instrument(obj.get("instrument"))
                if inst not in valid_instruments:
                    dropped_invalid += 1
                    continue
                obj["instrument"] = inst
                action = str(obj.get("action", "")).upper().strip()
                if self._last_handoff_manage_only and action in {"OPEN", "SCALE_IN"}:
                    blocked_handoff_fresh += 1
                    watch = dict(obj)
                    watch["action"] = "WATCH"
                    watch["risk_pct"] = 0
                    watch["reason"] = (
                        f"Blocked by technical handoff manage-only mode: {handoff_reason}. "
                        + str(obj.get("reason", ""))
                    )
                    if key == "orders_to_execute":
                        decision.setdefault("new_trade_candidates", []).append(watch)
                        continue
                    obj = watch
                    action = "WATCH"
                if key == "orders_to_execute" and action in {"OPEN", "SCALE_IN"}:
                    direction = str(obj.get("direction") or "").upper().strip()
                    template = candidate_templates.get((inst, direction))
                    template_score = _finite((template or {}).get("technical_score"), 0.0)
                    template_risk = _finite((template or {}).get("risk_pct_suggestion"), 0.0)
                    if inst not in candidate_instruments or not template:
                        dropped_off_snapshot += 1
                        watch = dict(obj)
                        watch["action"] = "WATCH"
                        watch["risk_pct"] = 0
                        watch["reason"] = (
                            "Blocked by technical-snapshot gate: exact instrument+direction was not in "
                            "potential_positions for this scan. " + str(obj.get("reason", ""))
                        )
                        decision.setdefault("new_trade_candidates", []).append(watch)
                        continue
                    if template_score < min_live_score or template_risk <= 0:
                        dropped_off_snapshot += 1
                        watch = dict(obj)
                        watch["action"] = "WATCH"
                        watch["risk_pct"] = 0
                        watch["reason"] = (
                            f"Blocked by technical-snapshot quality gate: score={template_score:.2f}, "
                            f"risk_suggestion={template_risk:.3f}. " + str(obj.get("reason", ""))
                        )
                        decision.setdefault("new_trade_candidates", []).append(watch)
                        continue
                    requested_risk = _finite(obj.get("risk_pct"), 0.0)
                    capped_risk = min(requested_risk, template_risk, self.cfg.max_risk_pct_per_trade)
                    if capped_risk < requested_risk:
                        obj["risk_pct"] = round(capped_risk, 3)
                        obj["reason"] = (
                            f"technical live risk cap reduced risk {requested_risk:.3f}% -> {capped_risk:.3f}%; "
                            + str(obj.get("reason", ""))
                        )
                    if obj.get("stop_loss") in {None, ""}:
                        obj["stop_loss"] = template.get("suggested_stop_loss")
                    if obj.get("take_profit") in {None, ""} and obj.get("tp1") in {None, ""}:
                        obj["take_profit"] = template.get("suggested_take_profit")
                    if obj.get("trailing_stop_pips") in {None, ""}:
                        obj["trailing_stop_pips"] = template.get("suggested_trailing_stop_pips")
                kept.append(obj)
            if dropped_invalid:
                add_note(f"gpt_tech_snapshot dropped {dropped_invalid} {key} item(s) with invalid/non-OANDA instruments.")
            if dropped_off_snapshot:
                add_note(
                    "gpt_tech_snapshot converted "
                    f"{dropped_off_snapshot} off-snapshot OPEN/SCALE_IN order(s) to WATCH."
                )
            decision[key] = kept
        if blocked_handoff_fresh:
            add_note(
                "gpt_tech_snapshot handoff manage-only blocked "
                f"{blocked_handoff_fresh} fresh OPEN/SCALE_IN item(s): {handoff_reason}."
            )

        open_trade_ids = {str(t.get("id", "")).strip() for t in open_trades if str(t.get("id", "")).strip()}
        open_trade_insts = {
            advisor.normalize_instrument(t.get("instrument"))
            for t in open_trades
            if advisor.normalize_instrument(t.get("instrument"))
        }
        kept_actions: List[Dict[str, Any]] = []
        dropped_actions = 0
        for obj in decision.get("open_position_actions", []) or []:
            if not isinstance(obj, dict):
                continue
            tid = str(obj.get("trade_id") or "").strip()
            inst = advisor.normalize_instrument(obj.get("instrument"))
            if tid not in open_trade_ids and inst not in open_trade_insts:
                dropped_actions += 1
                continue
            if inst:
                obj["instrument"] = inst
            kept_actions.append(obj)
        if dropped_actions:
            add_note(
                f"gpt_tech_snapshot dropped {dropped_actions} invented open_position_action(s) "
                "not matching broker open trades."
            )
        decision["open_position_actions"] = kept_actions
        decision["event_permissions"] = []
        return decision

    def save_event_permissions_from_decision(self, decision: Dict[str, Any]) -> None:
        decision["event_permissions"] = []
        super().save_event_permissions_from_decision(decision)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = advisor.build_arg_parser()
    parser.add_argument(
        "--i-understand-live-risk",
        action="store_true",
        help=(
            "Required, unless FOREX_LIVE_CONFIRM is set to the confirmation "
            "phrase, before --execute can place live broker orders."
        ),
    )
    return parser


def main() -> int:
    if TECHNICAL_SYSTEM_PROMPT_APPEND not in advisor.SYSTEM_PROMPT:
        advisor.SYSTEM_PROMPT = advisor.SYSTEM_PROMPT + TECHNICAL_SYSTEM_PROMPT_APPEND

    args = build_arg_parser().parse_args()
    base_cfg = advisor.BotConfig.load()
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

    confirmation_ok = live_confirmation_ok(args)
    if args.execute and not confirmation_ok:
        raise RuntimeError(
            "--execute for live GPT technical snapshot requires "
            "--i-understand-live-risk or FOREX_LIVE_CONFIRM="
            f"{LIVE_CONFIRM_PHRASE}. The existing {LEGACY_LIVE_CONFIRM_PHRASE} "
            "confirmation phrase is also accepted for compatibility."
        )

    bot = TechnicalSnapshotGPTManager(
        build_technical_snapshot_config(
            base_cfg,
            execute_requested=bool(args.execute),
            dry_run_requested=bool(args.dry_run),
            confirmation_ok=confirmation_ok,
        )
    )

    if args.print_config:
        advisor.log(
            f"Resolved config for lane={bot.cfg.account_lane} "
            f"account_name={bot.cfg.account_display_name}"
        )
        advisor.log(f"data_dir={bot.cfg.data_dir}")
        bot.print_config()

    explicit_flags = (
        args.scan_now
        or args.monitor_now
        or args.write_recaps_now
        or args.daily_report_now
        or args.shutdown_pass_now
        or args.event_scan_now
        or args.weekend_summary_now
        or args.sunday_reopen_now
    )
    if not args.print_config or explicit_flags:
        bot.validate_config()
        enforce_shared_live_account_guard(bot)
    process_lock: TechnicalSnapshotProcessLock | None = None
    if not args.print_config or explicit_flags:
        lock_name = (
            f"{bot.cfg.account_lane} "
            f"{str(bot.cfg.oanda_account_id)[-8:] if bot.cfg.oanda_account_id else 'unknown'}"
        )
        process_lock = TechnicalSnapshotProcessLock(bot.cfg.data_dir / "account_process.lock", lock_name)
        process_lock.acquire()
        advisor.log(f"{bot.lane_prefix()}Acquired account process lock: {process_lock.path}")

    try:
        did_explicit = False
        if args.write_recaps_now:
            bot.write_daily_recaps_around_now()
            advisor.log(f"{bot.lane_prefix()}Wrote daily recap files under {bot.cfg.daily_recap_dir}")
            did_explicit = True
        if args.daily_report_now:
            advisor.maybe_auto_daily_move_report(account_lane=bot.cfg.account_lane, writer=False, force=True)
            did_explicit = True
        if args.monitor_now:
            advisor.ensure_bot_ready(bot)
            bot.run_local_monitor()
            did_explicit = True
        if args.scan_now:
            advisor.ensure_bot_ready(bot)
            bot.run_gpt_scan(reason="manual")
            did_explicit = True
        if args.event_scan_now:
            advisor.log(f"{bot.lane_prefix()}Event scanner is disabled for the technical snapshot GPT lane.")
            did_explicit = True
        if args.weekend_summary_now:
            advisor.ensure_bot_ready(bot)
            bot.run_weekend_summary_scan(reason="manual_weekend_summary")
            did_explicit = True
        if args.sunday_reopen_now:
            advisor.ensure_bot_ready(bot)
            account = bot.oanda.get_account_summary()
            open_trades = bot.oanda.get_open_trades()
            bot.update_trade_profit_memory(account, open_trades, reason="manual_sunday_reopen")
            bot.run_sunday_reopen_hard_protection(account=account, open_trades=open_trades)
            bot.run_gpt_scan(reason="sunday_reopen_manual")
            did_explicit = True
        if args.shutdown_pass_now:
            advisor.ensure_bot_ready(bot)
            if not bot.shutdown_mode_active():
                bot.cfg.shutdown_mode = "fade"
            bot.run_shutdown_fade_pass(reason="manual_shutdown_pass", force=True)
            did_explicit = True

        if args.once and (did_explicit or args.print_config):
            return 0

        bot.loop()
        return 0
    finally:
        if process_lock is not None:
            process_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
