#!/usr/bin/env python3
"""Guarded live wrapper for the technical broad-regime scout account.

This runs the technical/scout engine on OANDA live account
OANDA_ACCOUNT_LIVE_TECH / 001-001-21715580-003 without changing the practice
technical manager or the GPT live manager.

Deployment profile:
- strict broad-regime entries only;
- score >= 90;
- regime gap >= 30 with >= 15 participating instruments;
- no synthetic strength-cross live entries;
- anti-flip protection after extreme broad locks;
- aggressive margin posture, but still with stop-loss, spread, weekend-flat,
  duplicate, and emergency-margin guardrails.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict

from oanda_live_scout_guardrails import LiveScoutGuardMixin
import oanda_technical_account_manager_auto as tech


LIVE_DATA_DIR = (
    tech.SCRIPT_DIR
    / "data"
    / "technical_scout_manager"
    / "account_live_tech_broad_regime_scout"
)


LIVE_TECH_RUNTIME_OVERRIDES: Dict[str, Any] = {
    # This live lane favors high-value technical scouts. Broad-regime evidence
    # is preferred, but major-pair value setups can trade when they clear the
    # live EV, value, quarantine, and risk controls.
    "FOREX_TECH_REQUIRE_PRODUCTION_MODEL_FOR_NEW_ENTRIES": False,
    "FOREX_SCOUT_BROAD_REGIME_TRADE_ONLY": False,
    "FOREX_SCOUT_BROAD_REGIME_MIN_EV_SCORE": 95.0,
    "FOREX_SCOUT_BROAD_REGIME_MIN_STRENGTH_GAP": 30.0,
    "FOREX_SCOUT_BROAD_REGIME_MIN_INSTRUMENTS": 15,
    "FOREX_SCOUT_BROAD_REGIME_MIN_PAIR_EDGE": 1.5,
    "FOREX_SCOUT_BROAD_REGIME_VOLATILE_ONLY": False,
    "FOREX_SCOUT_BROAD_REGIME_ALLOW_SYNTHETIC_CROSSES": False,
    "FOREX_SCOUT_BROAD_REGIME_ALLOW_MODEL_BYPASS": True,
    "FOREX_SCOUT_BROAD_REGIME_BYPASS_REQUIRE_PROMOTED_MODEL": False,
    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_EV_SCORE": 95.0,
    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_NET_PIPS": 40.0,
    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_RATIO": 1.4,
    "FOREX_SCOUT_BROAD_REGIME_BYPASS_MIN_FACTOR_PROB": 0.12,
    "FOREX_TECH_STRENGTH_MAX_SYNTHETIC_CANDIDATES": 0,
    "FOREX_TECH_ALLOW_ENSEMBLE_SCOUT_FALLBACK": True,
    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_ALLOW_MOVEMENT": True,
    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_MIN_PROB": 0.20,
    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_PRE_SPIKE_MIN_PROB": 0.12,
    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_FACTOR_MIN_PROB": 0.12,
    "FOREX_TECH_ENSEMBLE_SCOUT_FALLBACK_EXHAUSTION_MIN_PROB": 0.20,

    # Rank scout opportunities by estimated account-dollar capture after spread
    # and margin, not by raw pip count. This de-emphasizes headline TRY pips
    # when ZAR/MXN-style moves offer better value per margin dollar.
    "FOREX_SCOUT_VALUE_WEIGHTED_ENABLED": True,
    "FOREX_SCOUT_VALUE_HARD_GATE": True,
    "FOREX_SCOUT_VALUE_REQUIRE_ESTIMATE": False,
    "FOREX_SCOUT_VALUE_MARGIN_FRACTION": 0.70,
    "FOREX_SCOUT_VALUE_SPREAD_COST_MULT": 1.0,
    "FOREX_SCOUT_VALUE_MIN_EXPECTED_USD": 0.006,
    "FOREX_SCOUT_VALUE_MIN_RETURN_PCT": 0.06,
    "FOREX_SCOUT_VALUE_TARGET_RETURN_PCT": 0.90,
    "FOREX_SCOUT_VALUE_SCORE_MAX": 16.0,
    "FOREX_SCOUT_VALUE_RISK_TARGET_RETURN_PCT": 0.75,
    "FOREX_SCOUT_VALUE_RISK_MULT_MIN": 0.55,
    "FOREX_SCOUT_VALUE_RISK_MULT_MAX": 1.25,

    # The 2026-06-25/26 loss review showed 90-94 broad/ensemble scouts were
    # negative while the limited 95+ sample was positive. Unknown-profile
    # candidates now need very clean movement/value evidence instead of just
    # headline raw pips.
    "FOREX_SCOUT_UNKNOWN_PROFILE_STRICT_GATE_ENABLED": True,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MIN_RATIO": 4.50,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MIN_NET_PIPS_MULTIPLIER": 0.85,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MIN_BASKET": 4,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MIN_VALUE_RETURN_PCT": 1.20,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MAJOR_MIN_RATIO": 2.25,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MAJOR_MIN_NET_PIPS_MULTIPLIER": 0.95,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MAJOR_MIN_BASKET": 1,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MAJOR_MIN_VALUE_RETURN_PCT": 0.18,
    "FOREX_SCOUT_UNKNOWN_PROFILE_MAJOR_MIN_EXPECTED_USD": 0.012,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_ENABLED": False,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_PAIR_KINDS": "major",
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_BLOCK_THEMES": "RISK_OFF,PRE_SPIKE_RISK_OFF",
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_SCORE": 95.0,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_RATIO": 2.25,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_RETURN_PCT": 0.20,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_EXPECTED_USD": 0.020,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MAX_AGE_MINUTES": 3.0,
    "FOREX_SCOUT_HIGH_VALUE_EV_BYPASS_MIN_NET_PIPS_MULTIPLIER": 0.95,

    # Use the validated exhaustion-reversal rule as a model-gated scout factor,
    # not as a standalone trigger. These candidates still need model/ensemble,
    # EV, spread, duplicate, broad-regime, stop, and account-risk approval.
    "FOREX_TECH_EXHAUSTION_HYBRID_SCOUT_ENABLED": True,
    "FOREX_TECH_EXHAUSTION_HYBRID_VOLATILE_ONLY": True,
    "FOREX_TECH_EXHAUSTION_HYBRID_MIN_RULES": 2,
    "FOREX_TECH_EXHAUSTION_HYBRID_MIN_SCORE": 72.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_MIN_MOVE_TO_SPREAD_RATIO": 1.4,
    "FOREX_TECH_EXHAUSTION_HYBRID_MIN_NET_PIPS_MULTIPLIER": 0.50,
    "FOREX_TECH_EXHAUSTION_HYBRID_MAX_SPREAD_TO_ATR_STOP_RATIO": 0.35,
    "FOREX_TECH_EXHAUSTION_HYBRID_MAX_SCOUTS_PER_SCAN": 3,
    "FOREX_TECH_EXHAUSTION_HYBRID_STOP_ATR": 0.75,
    "FOREX_TECH_EXHAUSTION_HYBRID_STOP_SPREAD_FLOOR_MULT": 3.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_TP_R_MULTIPLE": 2.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_TRAILING_R_MULTIPLE": 1.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_EV_SCORE_BOOST": 8.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_MIN_EV_SCORE": 95.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_BYPASS_MIN_EV_SCORE": 95.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_MAX_EVENT_AGE_MINUTES": 10.0,
    "FOREX_TECH_EXHAUSTION_HYBRID_REPEAT_TRIGGER_CAP": 2,
    "FOREX_TECH_EXHAUSTION_HYBRID_RISK_MULTIPLIER": 0.70,

    # Keep the first extreme regime from being replaced by a retracement cluster.
    "FOREX_SCOUT_REGIME_LOCK_ENABLED": True,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_ENABLED": True,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_MINUTES": 60.0,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_MIN_GAP": 30.0,
    "FOREX_SCOUT_REGIME_LOCK_ANTI_FLIP_REPLACEMENT_GAP_MULT": 1.35,
    "FOREX_SCOUT_REGIME_LOCK_PROTECT_ALIGNED_PRUNE_MINUTES": 30.0,
    "FOREX_SCOUT_REGIME_LOCK_PROTECT_ALIGNED_LOSS_USD": 0.025,

    # Churn review on 2026-07-02 showed low-score high-value bypasses and
    # >70% margin churn were the live loss source. Keep the scout active, but
    # only let the proven 95+ score bucket build a small basket.
    "FOREX_TECH_CAMPAIGN_MAX_ACTIVE": 4,
    "FOREX_TECH_CAMPAIGN_MAX_ENTRIES": 1,
    "FOREX_TECH_CAMPAIGN_MAX_ADDS": 0,
    "FOREX_TECH_CAMPAIGN_MAX_ENTRY_RISK_PCT": 0.35,
    "FOREX_TECH_CAMPAIGN_MARGIN_SOFT_CAP_PCT": 38.0,
    "FOREX_TECH_CAMPAIGN_MARGIN_HARD_CAP_PCT": 52.0,
    "FOREX_TECH_CAMPAIGN_ADD_MIN_UNREALIZED_USD": 0.060,
    "FOREX_TECH_CAMPAIGN_ADD_RISK_MULTIPLIER": 0.55,
    "FOREX_TECH_CAMPAIGN_PRUNE_MARGIN_TRIGGER_PCT": 45.0,
    "FOREX_TECH_CAMPAIGN_PRUNE_RED_AFTER_MINUTES": 20.0,
    "FOREX_TECH_CAMPAIGN_PRUNE_MIN_LOSS_USD": 0.010,
    "FOREX_TECH_CAMPAIGN_PRUNE_GIVEBACK_MIN_MFE_USD": 0.006,
    "FOREX_TECH_CAMPAIGN_ALLOW_OPPOSITE_ROTATION": False,
    "FOREX_TECH_CAMPAIGN_ROTATE_OPPOSITE_MIN_SCORE": 99.0,
    "FOREX_TECH_CAMPAIGN_ROTATE_OPPOSITE_IF_LOSS_USD": 0.020,
}


def _raw_setting(name: str) -> Any:
    value = os.environ.get(name)
    if value is not None and str(value).strip() != "":
        return value
    value = tech.CREDS.get(name)
    if value is not None and str(value).strip() != "":
        return value
    return None


def _cred_or_env(*names: str, default: str = "") -> str:
    for name in names:
        value = _raw_setting(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _setting_bool(name: str, default: bool = False) -> bool:
    value = _raw_setting(name)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def apply_live_runtime_overrides() -> None:
    tech.CONFIG.update(LIVE_TECH_RUNTIME_OVERRIDES)
    # Tech has the same account-local score-demotion guardrail as primary, but
    # scoped to the champion lane.  It only blocks future new entries in score
    # buckets that the daily recap marks as degraded; it does not close trades.
    os.environ.setdefault("FOREX_TECH_LIVE_SCORE_DEMOTION_ENABLED", "1")
    os.environ.setdefault("FOREX_TECH_LIVE_GROUP_QUARANTINE_ENABLED", "1")
    os.environ.setdefault(
        "FOREX_TECH_LIVE_GROUP_QUARANTINE_DIMENSIONS",
        "score_bucket,model_source,theme,instrument",
    )
    os.environ.setdefault("FOREX_TECH_LIVE_GROUP_QUARANTINE_MIN_TRADES", "5")
    os.environ.setdefault("FOREX_TECH_LIVE_GROUP_QUARANTINE_MIN_DATES", "3")


def build_live_tech_config(base_cfg: tech.BotConfig, *, execute: bool, dry_run: bool) -> tech.BotConfig:
    live_api_key = _cred_or_env(
        "OANDA_LIVE_API_KEY",
        "OANDA_API_KEY_LIVE",
        "OANDA_LIVE_API_TOKEN",
        "OANDA_API_TOKEN_LIVE",
    )
    live_account_id = _cred_or_env(
        "OANDA_ACCOUNT_LIVE_TECH",
        "OANDA_ACCOUNT_ID_TECH_LIVE",
        "OANDA_LIVE_ACCOUNT_ID_TECH",
        "OANDA_ACCOUNT_ID_LIVE_TECH",
    )
    allow_live = _setting_bool("FOREX_ALLOW_LIVE", False)
    if not live_account_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_LIVE_TECH / OANDA_ACCOUNT_ID_TECH_LIVE")
    if not live_api_key:
        raise RuntimeError("Missing OANDA_LIVE_API_KEY / OANDA_API_KEY_LIVE")

    cfg = replace(
        base_cfg,
        oanda_env="live",
        oanda_api_key=live_api_key,
        oanda_account_id=live_account_id,
        account_lane="tech",
        account_display_name="OANDA_ACCOUNT_LIVE_TECH / TECH_BROAD_REGIME_SCOUT",
        instrument_filter_mode="all",
        research_reliable_pairs=[],
        technical_scout_pairs=[],
        execute_trades=bool(execute and not dry_run),
        auto_execute_research_actions=True,
        allow_live=allow_live,
        scan_on_launch=False,
        calls_per_trading_day=0,
        call_times_ny=[],
        friday_call_times_ny=[],
        event_scanner_enabled=True,
        event_scan_interval_seconds=30,
        event_candle_granularity="M1",
        event_candle_count=max(base_cfg.event_candle_count, 300),
        event_windows_minutes=[1, 3, 5, 10, 15, 30],
        event_min_basket_pairs=1,
        event_min_major_net_pips=5.0,
        event_min_cross_net_pips=8.0,
        event_min_exotic_net_pips=35.0,
        event_min_move_to_spread_ratio=2.0,
        event_trigger_research_enabled=False,
        event_require_research_permission=False,
        event_scout_trades_enabled=True,
        event_scout_risk_pct=0.28,
        event_max_total_scout_risk_pct=1.2,
        event_max_scout_trades_per_event=2,
        event_scout_stop_pips_major=12.0,
        event_scout_stop_pips_cross=20.0,
        event_scout_stop_pips_exotic=220.0,
        event_scout_take_profit_r_multiple=2.2,
        event_scout_trailing_stop_pips_major=10.0,
        event_scout_trailing_stop_pips_cross=18.0,
        event_scout_trailing_stop_pips_exotic=180.0,
        scout_ev_scoring_enabled=True,
        scout_min_ev_score_to_trade=90.0,
        scout_max_event_age_minutes_for_continuation=18.0,
        scout_max_repeat_triggers_per_key=3,
        scout_event_memory_reset_minutes=180.0,
        scout_skip_late_exhaustion=True,
        scout_volatile_lane_enabled=True,
        scout_volatile_min_net_pips=40.0,
        scout_volatile_max_spread_pips=260.0,
        scout_volatile_min_move_to_spread_ratio=1.4,
        scout_volatile_min_basket_pairs=1,
        scout_volatile_max_event_age_minutes=15.0,
        scout_volatile_repeat_trigger_cap=4,
        scout_volatile_base_risk_pct=0.35,
        scout_volatile_max_risk_pct=0.85,
        scout_volatile_allow_unknown_profile=True,
        scout_volatile_allow_negative_profile_if_early=True,
        scout_volatile_min_ev_score_to_trade=90.0,
        scout_volatile_stop_pips=320.0,
        scout_volatile_stop_net_multiplier=0.65,
        scout_volatile_stop_spread_multiplier=3.5,
        scout_volatile_trailing_stop_pips=250.0,
        scout_volatile_max_spread_to_stop_ratio=0.90,
        max_open_trades=4,
        max_new_trades_per_scan=3,
        max_total_new_risk_pct_per_scan=5.0,
        max_risk_pct_per_trade=0.8,
        min_risk_pct_per_trade=0.05,
        target_margin_used_pct=38.0,
        max_margin_used_pct=52.0,
        emergency_margin_used_pct=85.0,
        local_emergency_close=True,
        max_one_currency_net_units_pct=85.0,
        require_stop_loss_on_open=True,
        require_take_profit_on_open=False,
        allow_trailing_stop_on_open=True,
        local_monitor_interval_minutes=5,
        loop_sleep_seconds=20,
        shutdown_skip_research=True,
        shutdown_block_new_trades=True,
    )
    return tech.with_data_dir(cfg, LIVE_DATA_DIR)


class LiveTechScoutManager(LiveScoutGuardMixin, tech.ForexManager):
    live_guardrail_profile = "tech"


class LiveAccountProcessLock:
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
                f"started_utc={tech.iso_utc()}\n"
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Live OANDA technical broad-regime scout manager.")
    parser.add_argument("--print-config", action="store_true")
    parser.add_argument("--event-scan-now", action="store_true")
    parser.add_argument("--monitor-now", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Explicitly enable live execution. Default is enabled unless --dry-run is passed.")
    parser.add_argument("--no-scan-on-launch", action="store_true", help="Accepted for symmetry; this live wrapper always starts without launch scan.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_live_runtime_overrides()
    base_cfg = tech.BotConfig.load()
    execute = bool(args.execute or not args.dry_run)
    cfg = build_live_tech_config(base_cfg, execute=execute, dry_run=args.dry_run)
    bot = LiveTechScoutManager(cfg)
    explicit_flags = args.event_scan_now or args.monitor_now
    process_lock: LiveAccountProcessLock | None = None
    if not args.print_config or explicit_flags:
        lock_name = (
            f"{bot.cfg.account_lane} "
            f"{str(bot.cfg.oanda_account_id)[-8:] if bot.cfg.oanda_account_id else 'unknown'}"
        )
        process_lock = LiveAccountProcessLock(bot.cfg.data_dir / "account_process.lock", lock_name)
        process_lock.acquire()
        tech.log(f"[{bot.cfg.account_lane}] Acquired account process lock: {process_lock.path}")

    try:
        if args.print_config:
            tech.log(f"Resolved live tech config account={cfg.oanda_account_id} lane={cfg.account_lane} execute={bot.should_execute()}")
            bot.print_config()
            if not explicit_flags:
                return 0

        if not args.print_config or explicit_flags:
            bot.maybe_write_live_scout_recap("startup")
        if explicit_flags:
            tech.ensure_bot_ready(bot, reconcile=True)
        if args.monitor_now:
            bot.run_local_monitor()
        if args.event_scan_now:
            bot.run_event_scan(reason="manual_live_tech_event_scan")
        if args.once:
            return 0

        bot.loop()
        return 0
    finally:
        if process_lock is not None:
            process_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
