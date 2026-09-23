#!/usr/bin/env python3
"""Dedicated OANDA spike-scout account.

This is a separate paper-account lane for catching unusually large M1 moves.
It reuses the technical manager's event scanner, pre-spike pressure features,
EV/profile scoring, active technical production model gate, and weekend-flat
policy, but runs on DUM3 by default so it does not interfere with GPT, GPT_EXP,
technical production, or DUM1 canary.

Default account:
    OANDA_ACCOUNT_ID_DUM3 / 101-001-37981792-006
"""

from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import oanda_technical_account_manager_auto as tech


SCOUT_DATA_DIR = (
    tech.SCRIPT_DIR
    / "data"
    / "technical_scout_manager"
    / "account_spike_scout_major_moves"
)


def _cred_or_env(*names: str, default: str = "") -> str:
    for name in names:
        value = tech.os.environ.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    for name in names:
        value = tech.CREDS.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


class SpikeScoutManager(tech.ForexManager):
    """Technical manager variant for isolated high-volatility scout trading."""

    def should_execute(self) -> bool:
        if not self.cfg.execute_trades:
            return False
        if self.cfg.oanda_env == "live" and not self.cfg.allow_live:
            return False
        return True

    def scout_bounds_cooldown_key(self, order: Dict[str, Any]) -> str:
        inst = tech.normalize_instrument(order.get("instrument"))
        direction = str(order.get("direction", "")).upper().strip()
        return f"{inst}|{direction}"

    def scout_bounds_cooldown_active(self, order: Dict[str, Any]) -> Tuple[bool, str]:
        if not tech.cfg_bool("FOREX_SPIKE_SCOUT_BOUNDS_COOLDOWN_ENABLED", default=True):
            return False, ""
        key = self.scout_bounds_cooldown_key(order)
        state = self.load_state()
        cooldowns = state.setdefault("scout_broker_cancel_cooldowns", {})
        if not isinstance(cooldowns, dict):
            state["scout_broker_cancel_cooldowns"] = {}
            self.save_state(state)
            return False, ""
        rec = cooldowns.get(key)
        if not isinstance(rec, dict):
            return False, ""
        until_raw = rec.get("until_utc")
        try:
            until = dt.datetime.fromisoformat(str(until_raw).replace("Z", "+00:00")).astimezone(tech.UTC)
        except Exception:
            cooldowns.pop(key, None)
            self.save_state(state)
            return False, ""
        now = tech.utc_now()
        if until <= now:
            cooldowns.pop(key, None)
            self.save_state(state)
            return False, ""
        minutes_left = max(0.0, (until - now).total_seconds() / 60.0)
        return True, (
            f"spike scout broker-bounds cooldown active for {key}; "
            f"{minutes_left:.1f}m remaining after recent BOUNDS_VIOLATION"
        )

    def mark_scout_bounds_violation(self, order: Dict[str, Any], result: Dict[str, Any], *, note: str = "") -> None:
        key = self.scout_bounds_cooldown_key(order)
        state = self.load_state()
        cooldowns = state.setdefault("scout_broker_cancel_cooldowns", {})
        if not isinstance(cooldowns, dict):
            cooldowns = {}
            state["scout_broker_cancel_cooldowns"] = cooldowns
        old = cooldowns.get(key) if isinstance(cooldowns.get(key), dict) else {}
        now = tech.utc_now()
        reset_minutes = max(1.0, tech.cfg_float("FOREX_SPIKE_SCOUT_BOUNDS_COUNT_RESET_MINUTES", default=90.0))
        last_raw = old.get("last_cancel_utc") if isinstance(old, dict) else None
        count = 0
        if last_raw:
            try:
                last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(tech.UTC)
                if (now - last).total_seconds() <= reset_minutes * 60.0:
                    count = int(old.get("count", 0))
            except Exception:
                count = 0
        count += 1
        base_minutes = max(1.0, tech.cfg_float("FOREX_SPIKE_SCOUT_BOUNDS_COOLDOWN_MINUTES", default=12.0))
        max_minutes = max(base_minutes, tech.cfg_float("FOREX_SPIKE_SCOUT_BOUNDS_MAX_COOLDOWN_MINUTES", default=45.0))
        cooldown_minutes = min(max_minutes, base_minutes * max(1, min(4, count)))
        until = now + dt.timedelta(minutes=cooldown_minutes)
        cooldowns[key] = {
            "count": count,
            "last_cancel_utc": tech.iso_utc(),
            "until_utc": until.isoformat().replace("+00:00", "Z"),
            "last_reason": self.scout_cancel_reason(result) or "BOUNDS_VIOLATION",
            "last_movement_key": str(order.get("_movement_key", "")),
            "last_note": note[:300],
        }
        self.save_state(state)
        tech.log(
            f"{self.lane_prefix()}Spike scout BOUNDS_VIOLATION cooldown set for {key}: "
            f"{cooldown_minutes:.1f}m count={count} note={note[:120]}"
        )

    def clear_scout_bounds_cooldown(self, order: Dict[str, Any]) -> None:
        key = self.scout_bounds_cooldown_key(order)
        state = self.load_state()
        cooldowns = state.get("scout_broker_cancel_cooldowns")
        if isinstance(cooldowns, dict) and key in cooldowns:
            cooldowns.pop(key, None)
            self.save_state(state)

    def scout_cancel_reason(self, result: Dict[str, Any]) -> str:
        if not isinstance(result, dict):
            return ""
        tx = result.get("orderCancelTransaction") or result.get("orderRejectTransaction") or {}
        if not isinstance(tx, dict):
            return ""
        return str(tx.get("reason", tx.get("rejectReason", ""))).upper().strip()

    def scout_bounds_violation(self, result: Dict[str, Any]) -> bool:
        reason = self.scout_cancel_reason(result)
        return reason == "BOUNDS_VIOLATION" or "BOUNDS_VIOLATION" in reason

    def refresh_scout_price(
        self,
        inst: str,
        prices: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not tech.cfg_bool("FOREX_SPIKE_SCOUT_REFRESH_PRICE_BEFORE_OPEN", default=True):
            return prices.get(inst, {})
        try:
            fresh = self.oanda.get_prices([inst])
            if isinstance(fresh, dict) and fresh.get(inst):
                prices.update(fresh)
        except Exception as exc:
            self.log_error(f"spike scout price refresh {inst}", exc)
        return prices.get(inst, {})

    def reanchor_event_scout_order(
        self,
        order: Dict[str, Any],
        price: Dict[str, Any],
        meta: tech.InstrumentMeta,
    ) -> None:
        if not tech.cfg_bool("FOREX_SPIKE_SCOUT_REANCHOR_PROTECTIVE_ORDERS", default=True):
            return
        direction = str(order.get("direction", "")).upper().strip()
        bid, ask = tech.bid_ask(price)
        if bid is None or ask is None or direction not in {"LONG", "SHORT"}:
            return
        entry = ask if direction == "LONG" else bid
        spread = max(0.0, tech.spread_pips(price, meta))
        net = abs(tech.safe_float(order.get("_movement_net_pips"), 0.0))
        stop_pips = self.event_stop_pips(meta.name, net, spread)
        tp_pips = stop_pips * max(0.5, self.cfg.event_scout_take_profit_r_multiple)
        if direction == "LONG":
            order["stop_loss"] = entry - stop_pips * meta.pip_size
            order["take_profit"] = entry + tp_pips * meta.pip_size
        else:
            order["stop_loss"] = entry + stop_pips * meta.pip_size
            order["take_profit"] = entry - tp_pips * meta.pip_size
        order["trailing_stop_pips"] = self.event_trailing_pips(meta.name)
        order["_scout_repriced_entry"] = entry
        order["_scout_repriced_spread_pips"] = spread
        order["_scout_repriced_at_utc"] = tech.iso_utc()

    def dynamic_scout_slippage_pips(
        self,
        order: Dict[str, Any],
        price: Dict[str, Any],
        meta: tech.InstrumentMeta,
        *,
        retry: bool = False,
        previous: Optional[float] = None,
    ) -> float:
        base_cfg_slippage = max(0.0, float(self.cfg.max_entry_slippage_pips))
        spread = max(0.0, tech.spread_pips(price, meta)) if price else 0.0
        net = abs(tech.safe_float(order.get("_movement_net_pips"), 0.0))
        kind = self.event_pair_kind(meta.name)
        if self.is_volatile_scout_pair(meta.name) or kind == "exotic":
            cap = tech.cfg_float(
                "FOREX_SPIKE_SCOUT_RETRY_MAX_SLIPPAGE_PIPS" if retry else "FOREX_SPIKE_SCOUT_MAX_SLIPPAGE_PIPS",
                default=260.0 if retry else 160.0,
            )
            floor = tech.cfg_float("FOREX_SPIKE_SCOUT_MIN_SLIPPAGE_PIPS", default=25.0)
            dyn = max(
                base_cfg_slippage,
                floor,
                spread * (1.35 if retry else 0.75),
                net * (0.28 if retry else 0.16),
            )
        elif kind == "cross":
            cap = tech.cfg_float(
                "FOREX_SPIKE_SCOUT_RETRY_MAX_CROSS_SLIPPAGE_PIPS" if retry else "FOREX_SPIKE_SCOUT_MAX_CROSS_SLIPPAGE_PIPS",
                default=40.0 if retry else 24.0,
            )
            dyn = max(
                base_cfg_slippage,
                12.0 if retry else 8.0,
                spread * (2.5 if retry else 1.5),
                net * (0.12 if retry else 0.06),
            )
        else:
            cap = tech.cfg_float(
                "FOREX_SPIKE_SCOUT_RETRY_MAX_MAJOR_SLIPPAGE_PIPS" if retry else "FOREX_SPIKE_SCOUT_MAX_MAJOR_SLIPPAGE_PIPS",
                default=12.0 if retry else 8.0,
            )
            dyn = max(
                base_cfg_slippage,
                6.0 if retry else 4.0,
                spread * (2.0 if retry else 1.2),
                net * (0.08 if retry else 0.04),
            )
        if retry and previous is not None:
            dyn = max(dyn, previous * tech.cfg_float("FOREX_SPIKE_SCOUT_RETRY_SLIPPAGE_MULTIPLIER", default=1.75))
        return max(base_cfg_slippage, min(max(base_cfg_slippage, cap), dyn))

    def execute_event_scout_attempt(
        self,
        order: Dict[str, Any],
        prices: Dict[str, Dict[str, Any]],
        *,
        slippage_pips: float,
        attempt: int,
    ) -> str:
        old_slippage = self.cfg.max_entry_slippage_pips
        try:
            self.cfg.max_entry_slippage_pips = slippage_pips
            order["_scout_dynamic_slippage_pips"] = slippage_pips
            order["_scout_execution_attempt"] = attempt
            return super().open_trade_from_order(order, prices)
        finally:
            self.cfg.max_entry_slippage_pips = old_slippage

    def open_trade_from_order(
        self,
        order: Dict[str, Any],
        prices: Dict[str, Dict[str, Any]],
    ) -> str:
        if not order.get("_event_scout"):
            return super().open_trade_from_order(order, prices)
        inst = tech.normalize_instrument(order.get("instrument"))
        meta = self.instrument_meta.get(inst)
        if meta is None:
            return super().open_trade_from_order(order, prices)

        cooldown_active, cooldown_reason = self.scout_bounds_cooldown_active(order)
        if cooldown_active:
            self.log_action(order, "event_scout", "skipped", reject_reason=cooldown_reason)
            return "skipped"

        price = self.refresh_scout_price(inst, prices)
        self.reanchor_event_scout_order(order, price, meta)
        dynamic_slippage = self.dynamic_scout_slippage_pips(order, price, meta, retry=False)
        status = self.execute_event_scout_attempt(
            order,
            prices,
            slippage_pips=dynamic_slippage,
            attempt=1,
        )
        result = getattr(self, "_last_open_trade_result", {}) or {}
        if status in {"accepted", "accepted_with_cancel", "accepted_after_recheck"}:
            self.clear_scout_bounds_cooldown(order)
            return status
        if not self.scout_bounds_violation(result):
            return status
        if not tech.cfg_bool("FOREX_SPIKE_SCOUT_RETRY_BOUNDS_VIOLATION", default=True):
            self.mark_scout_bounds_violation(order, result, note="retry disabled")
            return status

        delay = max(0.0, tech.cfg_float("FOREX_SPIKE_SCOUT_BOUNDS_RETRY_DELAY_SECONDS", default=0.75))
        if delay > 0:
            tech.time.sleep(delay)
        price = self.refresh_scout_price(inst, prices)
        self.reanchor_event_scout_order(order, price, meta)
        retry_slippage = self.dynamic_scout_slippage_pips(
            order,
            price,
            meta,
            retry=True,
            previous=dynamic_slippage,
        )
        order["_scout_retry_after_bounds_violation"] = True
        retry_status = self.execute_event_scout_attempt(
            order,
            prices,
            slippage_pips=retry_slippage,
            attempt=2,
        )
        retry_result = getattr(self, "_last_open_trade_result", {}) or {}
        if retry_status in {"accepted", "accepted_with_cancel", "accepted_after_recheck"}:
            self.clear_scout_bounds_cooldown(order)
            return retry_status
        self.mark_scout_bounds_violation(
            order,
            retry_result if isinstance(retry_result, dict) and retry_result else result,
            note=f"retry_status={retry_status}",
        )
        return retry_status

    def production_model_allows_new_entries(self) -> Tuple[bool, str]:
        manifest = self.sync_promoted_model_manifest()
        if manifest.get("activation_effective", False):
            return True, (
                "spike scout using active technical production model "
                f"experiment={manifest.get('experiment_id', '')}"
            )
        if tech.cfg_bool("FOREX_SCOUT_ALLOW_EV_ONLY_WITHOUT_MODEL", default=False):
            return True, "spike scout EV-only fallback enabled; no active production model"
        return False, (
            "new entries blocked: no active technical production model; "
            "set FOREX_SCOUT_ALLOW_EV_ONLY_WITHOUT_MODEL=1 only for explicit EV-only experiments"
        )

    def filter_signals_with_promoted_model(
        self,
        signals: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        manifest = self.sync_promoted_model_manifest()
        if manifest.get("activation_effective", False) and self._promoted_model_bundle:
            approved, rejected = super().filter_signals_with_promoted_model(signals)
            if not tech.cfg_bool("FOREX_SPIKE_SCOUT_ALLOW_MODEL_DISAGREEMENT_OVERRIDE", default=True):
                return approved, rejected
            override_approved: List[Dict[str, Any]] = []
            still_rejected: List[Dict[str, Any]] = []
            for signal in rejected:
                if self.scout_override_allows_model_reject(signal):
                    enriched = dict(signal)
                    enriched["_scout_model_override"] = True
                    enriched["_major_move_factor"] = {
                        **(
                            signal.get("_major_move_factor")
                            if isinstance(signal.get("_major_move_factor"), dict)
                            else {}
                        ),
                        "score_adjustment": 0.0,
                        "risk_multiplier": min(
                            0.40,
                            tech.cfg_float(
                                "FOREX_SPIKE_SCOUT_MODEL_REJECT_RISK_MULTIPLIER",
                                default=0.35,
                            ),
                        ),
                        "model_override": True,
                        "model_reject_reason": str(
                            (signal.get("_promoted_model_decision") or {}).get("reason", "")
                        )[:300],
                    }
                    decision = dict(enriched.get("_promoted_model_decision") or {})
                    decision["scout_override"] = True
                    decision["scout_override_reason"] = self.scout_override_reason(signal)
                    enriched["_promoted_model_decision"] = decision
                    override_approved.append(enriched)
                else:
                    still_rejected.append(signal)
            return approved + override_approved, still_rejected
        if tech.cfg_bool("FOREX_SCOUT_ALLOW_EV_ONLY_WITHOUT_MODEL", default=False):
            approved = []
            for signal in signals:
                enriched = dict(signal)
                enriched["_promoted_model_decision"] = {
                    "reason": "EV-only scout fallback; no active production model",
                    "fallback": True,
                }
                approved.append(enriched)
            return approved, []
        rejected = []
        for signal in signals:
            enriched = dict(signal)
            enriched["_promoted_model_decision"] = {
                "reason": "no active production model for spike scout",
                "fallback": False,
            }
            rejected.append(enriched)
        return [], rejected

    def scout_override_reason(self, signal: Dict[str, Any]) -> str:
        inst = tech.normalize_instrument(signal.get("instrument"))
        net = abs(tech.safe_float(signal.get("net_pips", signal.get("abs_move_pips", 0.0)), 0.0))
        ratio = tech.safe_float(signal.get("move_to_spread_ratio"), 0.0)
        cost = tech.safe_float(signal.get("cost_adjusted_net_pips"), 0.0)
        return (
            f"spike scout reduced-risk model-disagreement override: "
            f"{inst} net={net:.1f}p ratio={ratio:.2f} cost_adj={cost:.1f}p "
            f"drastic={bool(signal.get('_drastic_move_override'))} "
            f"pre_spike={bool(signal.get('_pre_spike_pressure'))}"
        )

    def scout_override_allows_model_reject(self, signal: Dict[str, Any]) -> bool:
        inst = tech.normalize_instrument(signal.get("instrument"))
        if not inst:
            return False
        net = abs(tech.safe_float(signal.get("net_pips", signal.get("abs_move_pips", 0.0)), 0.0))
        ratio = tech.safe_float(signal.get("move_to_spread_ratio"), 0.0)
        cost = tech.safe_float(signal.get("cost_adjusted_net_pips"), 0.0)
        threshold = max(0.1, self.event_threshold_pips(inst))
        volatile = self.is_volatile_scout_pair(inst)
        drastic = bool(signal.get("_drastic_move_override"))
        pre_spike = bool(signal.get("_pre_spike_pressure"))
        if pre_spike and cost > 0.0 and ratio >= 1.2:
            return True
        if volatile and drastic and cost > 0.0 and ratio >= 0.95 and net >= threshold * 1.75:
            return True
        if volatile and cost >= 10.0 and ratio >= 1.25 and net >= max(threshold * 1.25, 50.0):
            return True
        if not volatile and drastic and cost > 0.0 and ratio >= 2.4:
            return True
        return False

    def volatile_weekend_new_entries_blocked(self) -> bool:
        return tech.volatile_weekend_block_new_scouts_now()

    def volatile_weekend_flatten_due(
        self,
        state: Dict[str, Any] | None = None,
        *,
        force: bool = False,
    ) -> bool:
        if force:
            return True
        if not tech.volatile_weekend_flatten_window_now():
            return False
        state = state or self.load_state()
        last_raw = state.get("last_volatile_weekend_flatten_utc")
        if not last_raw:
            return True
        try:
            last = dt.datetime.fromisoformat(str(last_raw).replace("Z", "+00:00")).astimezone(tech.UTC)
            repeat_minutes = max(1, tech.cfg_int("FOREX_VOLATILE_WEEKEND_CLOSE_REPEAT_MINUTES", default=10))
            return (tech.utc_now() - last).total_seconds() >= repeat_minutes * 60
        except Exception:
            return True

    def run_volatile_weekend_flatten_pass(
        self,
        reason: str = "spike_scout_weekend_flatten",
        *,
        force: bool = False,
    ) -> int:
        state = self.load_state()
        if not self.volatile_weekend_flatten_due(state, force=force):
            return 0
        try:
            open_trades = self.oanda.get_open_trades()
        except Exception as exc:
            self.log_error("spike scout weekend flatten get_open_trades", exc)
            return 0
        if not open_trades:
            state["last_volatile_weekend_flatten_utc"] = tech.iso_utc()
            state["last_volatile_weekend_flatten_count"] = 0
            self.save_state(state)
            tech.log(f"{self.lane_prefix()}Weekend flat policy: already flat ({reason}).")
            return 0
        closed = 0
        tech.log(
            f"{self.lane_prefix()}Weekend flat policy: closing "
            f"{len(open_trades)} spike-scout trade(s). reason={reason}"
        )
        for trade in list(open_trades):
            inst = tech.normalize_instrument(trade.get("instrument"))
            action = {
                "instrument": inst,
                "trade_id": str(trade.get("id", "")),
                "action": "CLOSE",
                "direction": tech.trade_direction_from_units(trade.get("currentUnits")),
                "reason": f"spike scout weekend flat policy: {reason}",
            }
            try:
                status = self.close_trade(trade, action)
                if status not in {"skipped", "error"}:
                    closed += 1
            except Exception as exc:
                self.log_error(f"spike scout weekend flatten close {inst}", exc)
                try:
                    self.log_action(action, "position", "error", reject_reason=str(exc)[:500])
                except Exception:
                    pass
        state = self.load_state()
        state["last_volatile_weekend_flatten_utc"] = tech.iso_utc()
        state["last_volatile_weekend_flatten_count"] = closed
        self.save_state(state)
        tech.log(
            f"{self.lane_prefix()}Weekend flat policy pass complete: "
            f"closed_attempts={closed}/{len(open_trades)}"
        )
        return closed


def build_scout_config(base_cfg: tech.BotConfig) -> tech.BotConfig:
    account_id = _cred_or_env(
        "OANDA_ACCOUNT_ID_SCOUT",
        "OANDA_ACCOUNT_ID_DUM3",
        default="101-001-37981792-006",
    )
    if not account_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_ID_DUM3 / OANDA_ACCOUNT_ID_SCOUT")

    special_volatile_pairs = [
        tech.normalize_instrument(x)
        for x in (base_cfg.scout_volatile_pairs or base_cfg.technical_scout_pairs)
        if tech.normalize_instrument(x)
    ]

    cfg = replace(
        base_cfg,
        oanda_account_id=account_id,
        account_lane="scout",
        account_display_name="OANDA_ACCOUNT_ID_DUM3 / SPIKE_SCOUT_MAJOR_MOVES",
        instrument_filter_mode="all",
        research_reliable_pairs=[],
        technical_scout_pairs=[],
        scout_volatile_pairs=special_volatile_pairs,
        execute_trades=True,
        auto_execute_research_actions=True,
        scan_on_launch=False,
        calls_per_trading_day=0,
        call_times_ny=[],
        friday_call_times_ny=[],
        event_scanner_enabled=True,
        event_scan_interval_seconds=45,
        event_candle_granularity="M1",
        event_candle_count=max(base_cfg.event_candle_count, 120),
        event_windows_minutes=[1, 3, 5, 10, 15, 30],
        event_min_basket_pairs=1,
        event_min_major_net_pips=5.0,
        event_min_cross_net_pips=8.0,
        event_min_exotic_net_pips=35.0,
        event_min_move_to_spread_ratio=2.0,
        event_max_spread_pips_major=5.0,
        event_max_spread_pips_cross=10.0,
        event_max_spread_pips_exotic=260.0,
        event_trigger_research_enabled=False,
        event_min_minutes_between_research_scans=999999,
        event_scout_trades_enabled=True,
        event_require_research_permission=False,
        event_scout_risk_pct=0.25,
        event_max_total_scout_risk_pct=1.50,
        event_max_scout_trades_per_event=3,
        event_scout_stop_pips_major=12.0,
        event_scout_stop_pips_cross=20.0,
        event_scout_stop_pips_exotic=220.0,
        event_scout_take_profit_r_multiple=2.2,
        event_scout_trailing_stop_pips_major=10.0,
        event_scout_trailing_stop_pips_cross=18.0,
        event_scout_trailing_stop_pips_exotic=180.0,
        scout_ev_scoring_enabled=True,
        scout_min_ev_score_to_trade=60.0,
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
        scout_volatile_base_risk_pct=0.20,
        scout_volatile_max_risk_pct=0.60,
        scout_volatile_allow_unknown_profile=True,
        scout_volatile_allow_negative_profile_if_early=True,
        scout_volatile_min_ev_score_to_trade=52.0,
        scout_volatile_stop_pips=320.0,
        scout_volatile_stop_net_multiplier=0.65,
        scout_volatile_stop_spread_multiplier=3.5,
        scout_volatile_trailing_stop_pips=250.0,
        scout_volatile_max_spread_to_stop_ratio=0.90,
        max_open_trades=8,
        max_new_trades_per_scan=3,
        max_total_new_risk_pct_per_scan=3.0,
        max_risk_pct_per_trade=0.75,
        min_risk_pct_per_trade=0.02,
        max_margin_used_pct=58.0,
        target_margin_used_pct=38.0,
        emergency_margin_used_pct=72.0,
        max_one_currency_net_units_pct=50.0,
        local_monitor_interval_minutes=15,
        loop_sleep_seconds=30,
        shutdown_skip_research=True,
        shutdown_block_new_trades=True,
    )
    return tech.with_data_dir(cfg, SCOUT_DATA_DIR)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Always-on OANDA spike scout account manager.")
    parser.add_argument("--print-config", action="store_true", help="Print resolved config without revealing keys.")
    parser.add_argument("--scan-now", action="store_true", help="Alias for --event-scan-now.")
    parser.add_argument("--event-scan-now", action="store_true", help="Run one local M1 spike scout scan immediately.")
    parser.add_argument("--monitor-now", action="store_true", help="Run one local monitor pass immediately.")
    parser.add_argument("--once", action="store_true", help="Do not enter the permanent loop after explicit actions.")
    parser.add_argument("--dry-run", action="store_true", help="Disable broker execution for this run.")
    parser.add_argument("--execute", action="store_true", help="Force broker execution for this run, subject to live guard.")
    parser.add_argument("--shutdown-fade", action="store_true", help="No new opens; tighten/partial-close current trades.")
    parser.add_argument("--shutdown-close-now", action="store_true", help="Immediate close-all shutdown mode.")
    parser.add_argument("--normal-mode", action="store_true", help="Force normal trading mode.")
    parser.add_argument("--shutdown-pass-now", action="store_true", help="Run one shutdown pass now.")
    parser.add_argument("--weekend-flatten-now", action="store_true", help="Immediately close scout-account trades.")
    parser.add_argument("--write-recaps-now", action="store_true", help="Write/update daily recap files.")
    parser.add_argument("--daily-report-now", action="store_true", help="Write/check shared daily move comparison report.")
    return parser


def run_scout_loop(bot: SpikeScoutManager) -> None:
    tech.log(f"{bot.lane_prefix()}Starting always-on spike scout loop.")
    tech.log(
        f"{bot.lane_prefix()}OANDA env={bot.cfg.oanda_env} "
        f"account_name={bot.cfg.account_display_name} "
        f"instrument_filter={bot.cfg.instrument_filter_mode} "
        f"execution={'ENABLED' if bot.should_execute() else 'DRY-RUN'}"
    )
    tech.log(
        f"{bot.lane_prefix()}Event scanner interval_seconds={bot.cfg.event_scan_interval_seconds} "
        f"scout_trades={'ENABLED' if bot.cfg.event_scout_trades_enabled else 'OFF'} "
        f"production_model_gate=required"
    )
    while True:
        try:
            tech.run_bot_loop_iteration(bot)
        except KeyboardInterrupt:
            tech.log("Stopped by user.")
            return
        except Exception as exc:
            bot.log_error("spike scout loop", exc)
        tech.time.sleep(max(5, bot.cfg.loop_sleep_seconds))


def main() -> int:
    args = build_arg_parser().parse_args()
    base_cfg = tech.BotConfig.load()
    if args.dry_run:
        base_cfg.execute_trades = False
    if args.execute:
        base_cfg.execute_trades = True
    if args.normal_mode:
        base_cfg.shutdown_mode = "off"
    if args.shutdown_fade:
        base_cfg.shutdown_mode = "fade"
        base_cfg.shutdown_block_new_trades = True
    if args.shutdown_close_now:
        base_cfg.shutdown_mode = "close_now"
        base_cfg.shutdown_block_new_trades = True
        base_cfg.shutdown_skip_research = True

    bot = SpikeScoutManager(build_scout_config(base_cfg))

    if args.print_config:
        tech.log(
            f"Resolved config for lane={bot.cfg.account_lane} "
            f"account_name={bot.cfg.account_display_name}"
        )
        tech.log(f"market_movement_ledger={bot.cfg.market_movements_csv.parent}")
        bot.print_config()

    explicit = (
        args.scan_now
        or args.event_scan_now
        or args.monitor_now
        or args.shutdown_pass_now
        or args.weekend_flatten_now
        or args.write_recaps_now
        or args.daily_report_now
    )
    if not args.print_config or explicit:
        bot.validate_config()

    did_explicit = False
    if args.write_recaps_now:
        bot.write_daily_recaps_around_now()
        tech.log(f"{bot.lane_prefix()}Wrote daily recap files under {bot.cfg.daily_recap_dir}")
        did_explicit = True
    if args.daily_report_now:
        tech.maybe_auto_daily_move_report(account_lane=bot.cfg.account_lane, writer=False, force=True)
        did_explicit = True
    if args.monitor_now:
        tech.ensure_bot_ready(bot)
        bot.run_local_monitor()
        did_explicit = True
    if args.scan_now or args.event_scan_now:
        tech.ensure_bot_ready(bot)
        bot.run_event_scan(reason="manual")
        did_explicit = True
    if args.shutdown_pass_now:
        tech.ensure_bot_ready(bot)
        if not bot.shutdown_mode_active():
            bot.cfg.shutdown_mode = "fade"
        bot.run_shutdown_fade_pass(reason="manual_shutdown_pass", force=True)
        did_explicit = True
    if args.weekend_flatten_now:
        tech.ensure_bot_ready(bot)
        bot.run_volatile_weekend_flatten_pass(reason="manual_weekend_flatten", force=True)
        did_explicit = True

    if args.once and (did_explicit or args.print_config):
        return 0

    bot.validate_config()
    tech.ensure_bot_ready(bot)
    run_scout_loop(bot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
