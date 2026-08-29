#!/usr/bin/env python3
"""Experimental broad-news GPT advisor account.

This wrapper reuses the production GPT/advisor manager, but runs it as a
separate account lane on a dummy OANDA practice account.  The purpose is to
reduce USD-news bias by making the GPT call explicitly audit every represented
currency and all OANDA FX pairs before choosing positions.

Default account:
    OANDA_ACCOUNT_ID_DUM2 / 101-001-37981792-005

The original GPT advisor account is not modified or restarted by this file.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Tuple

import oanda_advisor_account_manager_auto as advisor
import oanda_gpt_prod_live_account_manager as live_prod


EXP_DATA_DIR = (
    advisor.SCRIPT_DIR
    / "data"
    / "forex_gpt_manager"
    / "account_gpt_exp_all_pairs_news"
)


EXP_SYSTEM_PROMPT_APPEND = """

Aggressive breaking-news technical demo mandate:
- This account is GPT_NEWS_TECH_DEMO on OANDA practice account 005. It is the
  fully enabled demo proving lane for the production breaking-news strategy.
- Trade aggressively when a setup is viable, but news is watch-only context.
  A headline can prioritize currencies and increase scan frequency; it cannot
  select direction or authorize an order.
- Direction, entry, stop, target, scaling, and exits must come from current
  technical structure and flow. For a news-related OPEN or SCALE_IN, include
  news_watch_id and a CONFIRMED M1/M5/M15 technical_confirmation. If price has
  not confirmed or the first impulse is exhausted, return WATCH.
- Prefer a reduced-risk probe on the first clean confirmation over repeatedly
  waiting for perfect conditions. SCALE_IN only after the original position is
  profitable and current structure reconfirms.
- Treat the supplied OANDA instrument list and open-trade contracts as
  authoritative. Never invent instruments, inverted symbols, trades, or IDs.
- Compare all supplied pairs globally. There is no USD/non-USD quota; choose the
  cleanest technical expression of the watched currency move after spread,
  stop geometry, expected R, and correlated exposure checks.
- Every OPEN or SCALE_IN requires expected_R >= 1.0, a valid stop, a concrete
  technical invalidation, and a non-exhaustion check. News sentiment alone is
  never technical confirmation.
"""


def _cred_or_env(*names: str, default: str = "") -> str:
    """Read a value from env first, then the imported creds mapping."""
    for name in names:
        value = advisor.os.environ.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    for name in names:
        value = advisor.CREDS.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def build_exp_config(
    base_cfg: advisor.BotConfig,
    *,
    execute_requested: bool = True,
) -> advisor.BotConfig:
    practice_api_key = _cred_or_env("OANDA_API_KEY", "OANDA_API_TOKEN")
    account_id = _cred_or_env(
        "OANDA_ACCOUNT_ID_GPT_EXP",
        "OANDA_ACCOUNT_ID_DUM2",
        default="101-001-37981792-005",
    )
    if not account_id:
        raise RuntimeError("Missing OANDA_ACCOUNT_ID_DUM2 / OANDA_ACCOUNT_ID_GPT_EXP")

    cfg = replace(
        base_cfg,
        oanda_env="practice",
        oanda_api_key=practice_api_key,
        oanda_account_id=account_id,
        account_lane="gpt_exp",
        account_display_name="OANDA_ACCOUNT_ID_DUM2 / GPT_NEWS_TECH_DEMO",
        instrument_filter_mode="all",
        gpt_reliable_pairs=[],
        technical_scout_pairs=[],
        execute_trades=bool(execute_requested),
        allow_live=False,
        # More room for diversified GPT macro/news ideas than the original lane,
        # while preserving hard caps and emergency margin protection.
        max_open_trades=max(base_cfg.max_open_trades, 14),
        max_new_trades_per_scan=max(base_cfg.max_new_trades_per_scan, 6),
        max_total_new_risk_pct_per_scan=max(base_cfg.max_total_new_risk_pct_per_scan, 14.0),
        max_risk_pct_per_trade=min(max(base_cfg.max_risk_pct_per_trade, 4.0), 4.5),
        target_margin_used_pct=max(base_cfg.target_margin_used_pct, 65.0),
        max_margin_used_pct=max(base_cfg.max_margin_used_pct, 85.0),
        emergency_margin_used_pct=min(max(base_cfg.emergency_margin_used_pct, 90.0), 92.0),
        max_one_currency_net_units_pct=min(base_cfg.max_one_currency_net_units_pct, 60.0),
        require_cross_pair_coverage=False,
        min_non_usd_candidates=0,
        # Frequent headline discovery arms a limited pair watch. M1 movement may
        # trigger the full GPT review, but never a local scout order.
        event_scanner_enabled=True,
        event_trigger_gpt_enabled=True,
        event_scout_trades_enabled=False,
        event_require_gpt_permission=True,
        event_scan_interval_seconds=live_prod._setting_int(
            "FOREX_LIVE_NEWS_TECH_SCAN_INTERVAL_SECONDS",
            int(live_prod.LIVE_RUNTIME_OVERRIDES["FOREX_LIVE_NEWS_TECH_SCAN_INTERVAL_SECONDS"]),
        ),
        event_candle_granularity="M1",
        event_candle_count=live_prod._setting_int("FOREX_LIVE_NEWS_TECH_CANDLE_COUNT", 20),
        event_windows_minutes=[1, 3, 5, 10, 15],
        event_min_basket_pairs=live_prod._setting_int("FOREX_LIVE_NEWS_MIN_BASKET_PAIRS", 2),
        event_min_major_net_pips=live_prod._setting_float(
            "FOREX_LIVE_NEWS_MIN_MAJOR_NET_PIPS",
            10.0,
        ),
        event_min_cross_net_pips=live_prod._setting_float(
            "FOREX_LIVE_NEWS_MIN_CROSS_NET_PIPS",
            14.0,
        ),
        event_min_exotic_net_pips=live_prod._setting_float(
            "FOREX_LIVE_NEWS_MIN_EXOTIC_NET_PIPS",
            80.0,
        ),
        event_min_move_to_spread_ratio=live_prod._setting_float(
            "FOREX_LIVE_NEWS_MIN_MOVE_TO_SPREAD_RATIO",
            4.0,
        ),
        event_min_minutes_between_gpt_scans=live_prod._setting_int(
            "FOREX_LIVE_NEWS_MIN_MINUTES_BETWEEN_GPT_SCANS",
            5,
        ),
    )
    return advisor.with_data_dir(cfg, EXP_DATA_DIR)


class BroadNewsForexManager(live_prod.LiveGPTProdManager):
    """Advisor manager with packet-level emphasis on all-currency news coverage."""

    def _valid_instruments(self) -> set[str]:
        return {advisor.normalize_instrument(inst) for inst in self.instruments if advisor.normalize_instrument(inst)}

    def _current_mid_by_instrument(self, packet: Dict[str, Any]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for snap in packet.get("market_snapshots", []) or []:
            if not isinstance(snap, dict):
                continue
            inst = advisor.normalize_instrument(snap.get("instrument"))
            mid = advisor.safe_float(snap.get("mid"), float("nan"))
            if inst and mid == mid and mid > 0:
                out[inst] = mid
        return out

    def build_market_packet(
        self,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        packet, prices, open_trades, raw_context = super().build_market_packet()

        represented_currencies = sorted(
            {
                currency
                for inst in self.instruments
                for currency in advisor.split_instrument(inst)
                if currency
            }
        )
        non_usd_pairs = [
            inst for inst in self.instruments
            if advisor.normalize_instrument(inst) and advisor.instrument_is_cross_pair(inst)
        ]

        instructions = packet.setdefault("instructions", {})
        instructions["experimental_lane"] = (
            "This is the fully enabled GPT_NEWS_TECH_DEMO lane on OANDA practice "
            "account 005. News arms watches; current technical evidence controls trades."
        )
        instructions["currency_news_coverage"] = (
            "Use active_news_watches to prioritize fresh central-bank, rates, data, "
            "political/geopolitical, commodity, and risk-sentiment catalysts. Do not "
            "convert inferred headline sentiment directly into a position."
        )
        instructions["required_decision_sequence"] = [
            "1. Manage open_trades exactly by trade_id/instrument.",
            "2. Inspect active news watches and their affected currencies.",
            "3. Require matching current M1/M5/M15 structure and non-exhausted flow.",
            "4. Rank the cleanest globally available pair expressions.",
            "5. Execute a controlled probe or scale a profitable winner only after every gate clears.",
        ]
        instructions["candidate_coverage"] = (
            "There is no pair-category quota. Rank USD majors, crosses, minors, and "
            "exotics together by technical quality, expected R, costs, and exposure."
        )
        instructions["risk_diversification"] = (
            "Use several independent positions when justified, but keep currency concentration "
            "controlled. Avoid different symbols that are all the same hidden USD bet."
        )
        instructions["hard_validity_rules"] = (
            "Use only instruments from valid_oanda_instruments. Use only exact open trade IDs from "
            "open_trade_contracts for open_position_actions. Do not invent inverted instruments or "
            "open trade IDs. Any invalid symbol, invented trade id, or wrong price scale will be rejected."
        )
        instructions["non_usd_audit_required"] = (
            "Review non-USD crosses when they are relevant, but do not manufacture "
            "candidates to meet a quota. Global technical quality controls ranking."
        )

        guardrails = packet.setdefault("config_guardrails", {})
        if isinstance(guardrails.get("live_money_profile"), dict):
            guardrails["live_money_profile"].update(
                {
                    "mode": "practice_demo_fully_enabled",
                    "execute_trades": self.cfg.execute_trades,
                    "allow_live": False,
                    "operator_note": (
                        "This is OANDA practice account 005. Practice execution is "
                        "enabled; no live-money authorization is present or required."
                    ),
                }
            )
        instructions["live_money_profile"] = (
            "This is a fully enabled OANDA practice/demo proving account, not a "
            "live-money account. Exercise the complete strategy and execution path."
        )
        guardrails["route_policy"] = (
            "Aggressive practice proving lane: breaking news creates a watch and M1 "
            "technical confirmation may trigger the full GPT execution path."
        )
        guardrails["min_non_usd_candidates"] = self.cfg.min_non_usd_candidates
        guardrails["max_one_currency_net_units_pct"] = self.cfg.max_one_currency_net_units_pct
        guardrails["event_scanner"]["scout_trades_enabled"] = False
        guardrails["event_scanner"]["permission_note"] = (
            "This lane may use local event scans to trigger GPT review, but local scout "
            "orders are disabled; all entries must come from GPT decisions."
        )

        packet["experimental_lane_profile"] = {
            "lane": self.cfg.account_lane,
            "name": self.cfg.account_display_name,
            "primary_goal": "Prove breaking-news discovery with technical-only entry and management.",
            "represented_currencies": represented_currencies,
            "non_usd_pair_count": len(non_usd_pairs),
            "usd_bias_control": (
                "Pair type is neutral. Use the technically cleanest expression of the "
                "watched currency move without redundant same-thesis exposure."
            ),
        }
        packet["valid_oanda_instruments"] = sorted(self._valid_instruments())
        packet["open_trade_contracts"] = [
            {
                "trade_id": str(t.get("id", "")).strip(),
                "instrument": advisor.normalize_instrument(t.get("instrument")),
                "direction": advisor.trade_direction_from_units(t.get("currentUnits")),
            }
            for t in open_trades
            if str(t.get("id", "")).strip() and advisor.normalize_instrument(t.get("instrument"))
        ]
        packet["price_scale_reference"] = {
            inst: {
                "mid": mid,
                "valid_stop_loss_take_profit_scale": (
                    f"Prices for {inst} must be near {mid:g}, not on another pair's/inverted pair's scale."
                ),
            }
            for inst, mid in sorted(self._current_mid_by_instrument(packet).items())
        }

        packet.setdefault("output_contract", {})["new_trade_candidates"] = (
            "Rank the best real opportunities after comparing all supplied pairs. For "
            "news-related ideas, include the active watch ID and current technical "
            "confirmation, or leave the idea as WATCH."
        )
        packet.setdefault("output_contract", {})["event_permissions"] = (
            "allowed_pairs must be a subset of valid_oanda_instruments. allowed_directions, when used, "
            "must map OANDA instrument symbols to LONG or SHORT."
        )
        return packet, prices, open_trades, raw_context

    def normalize_decision(self, decision: Dict[str, Any], open_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        decision = super().normalize_decision(decision, open_trades)
        valid_instruments = self._valid_instruments()
        open_trade_ids = {str(t.get("id", "")).strip() for t in open_trades if str(t.get("id", "")).strip()}
        open_trade_insts = {
            advisor.normalize_instrument(t.get("instrument"))
            for t in open_trades
            if advisor.normalize_instrument(t.get("instrument"))
        }

        notes = decision.setdefault("risk_notes", [])
        if not isinstance(notes, list):
            notes = []
            decision["risk_notes"] = notes

        def note(text: str) -> None:
            notes.append(text)
            advisor.log(f"{self.lane_prefix()}[WARN] {text}")

        for key in ("new_trade_candidates", "orders_to_execute"):
            kept: List[Dict[str, Any]] = []
            dropped = 0
            for obj in decision.get(key, []) or []:
                if not isinstance(obj, dict):
                    continue
                inst = advisor.normalize_instrument(obj.get("instrument"))
                if inst not in valid_instruments:
                    dropped += 1
                    continue
                obj["instrument"] = inst
                kept.append(obj)
            if dropped:
                note(f"gpt_exp dropped {dropped} {key} item(s) with invalid/non-OANDA instruments.")
            decision[key] = kept

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
            note(f"gpt_exp dropped {dropped_actions} invented open_position_action(s) not matching broker open trades.")
        decision["open_position_actions"] = kept_actions

        for perm in decision.get("event_permissions", []) or []:
            if not isinstance(perm, dict):
                continue
            pairs = [
                advisor.normalize_instrument(inst)
                for inst in (perm.get("allowed_pairs") or [])
                if advisor.normalize_instrument(inst) in valid_instruments
            ]
            perm["allowed_pairs"] = sorted(set(pairs))
            dirs = perm.get("allowed_directions") or {}
            if isinstance(dirs, dict):
                perm["allowed_directions"] = {
                    advisor.normalize_instrument(inst): str(direction).strip().upper()
                    for inst, direction in dirs.items()
                    if advisor.normalize_instrument(inst) in valid_instruments
                    and str(direction).strip().upper() in {"LONG", "SHORT"}
                }

        broad_candidate_count = 0
        major_usd_pairs = {advisor.normalize_instrument(inst) for inst in self.cfg.major_usd_pairs}
        for candidate in decision.get("new_trade_candidates", []) or []:
            if not isinstance(candidate, dict):
                continue
            inst = advisor.normalize_instrument(candidate.get("instrument"))
            if inst and (advisor.instrument_is_cross_pair(inst) or inst not in major_usd_pairs):
                broad_candidate_count += 1
        required_broad = max(0, int(self.cfg.min_non_usd_candidates or 0))
        if required_broad and broad_candidate_count < required_broad:
            kept_orders: List[Dict[str, Any]] = []
            blocked = 0
            for order in decision.get("orders_to_execute", []) or []:
                if not isinstance(order, dict):
                    continue
                inst = advisor.normalize_instrument(order.get("instrument"))
                action = str(order.get("action", "")).upper().strip()
                if action == "OPEN" and inst in major_usd_pairs:
                    blocked += 1
                    watch = dict(order)
                    watch["instrument"] = inst
                    watch["action"] = "WATCH"
                    watch["risk_pct"] = 0
                    watch["reason"] = (
                        f"gpt_exp broad-coverage gate: only {broad_candidate_count}/"
                        f"{required_broad} broad non-USD/regional candidates were supplied, "
                        "so this USD-major open is held for review. "
                        + str(order.get("reason", ""))
                    )
                    decision.setdefault("new_trade_candidates", []).append(watch)
                    continue
                kept_orders.append(order)
            if blocked:
                note(
                    "gpt_exp broad-coverage gate blocked "
                    f"{blocked} USD-major OPEN order(s); candidate coverage was "
                    f"{broad_candidate_count}/{required_broad}."
                )
            decision["orders_to_execute"] = kept_orders
        return decision

    def save_event_permissions_from_decision(self, decision: Dict[str, Any]) -> None:
        valid_instruments = self._valid_instruments()
        perms = decision.get("event_permissions", []) if isinstance(decision, dict) else []
        if isinstance(perms, list):
            cleaned = []
            dropped = 0
            for perm in perms:
                if not isinstance(perm, dict):
                    continue
                allowed = [
                    advisor.normalize_instrument(inst)
                    for inst in (perm.get("allowed_pairs") or [])
                    if advisor.normalize_instrument(inst) in valid_instruments
                ]
                if not allowed:
                    dropped += 1
                    continue
                perm = dict(perm)
                perm["allowed_pairs"] = sorted(set(allowed))
                cleaned.append(perm)
            if dropped:
                advisor.log(f"{self.lane_prefix()}[WARN] Dropped {dropped} event permission(s) without valid OANDA pairs.")
            decision["event_permissions"] = cleaned
        super().save_event_permissions_from_decision(decision)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = advisor.build_arg_parser()
    parser.description = (
        "Fully enabled OANDA practice breaking-news watch plus technical-entry manager."
    )
    parser.add_argument(
        "--news-watch-now",
        action="store_true",
        help="Run one watch-only breaking-news web check immediately.",
    )
    return parser


def main() -> int:
    live_prod.apply_live_runtime_overrides()
    live_prod.apply_live_decision_schema_extensions()
    if EXP_SYSTEM_PROMPT_APPEND not in advisor.SYSTEM_PROMPT:
        advisor.SYSTEM_PROMPT = advisor.SYSTEM_PROMPT + EXP_SYSTEM_PROMPT_APPEND

    args = build_arg_parser().parse_args()
    base_cfg = advisor.BotConfig.load()
    base_cfg.execute_trades = not args.dry_run
    if args.no_scan_on_launch:
        base_cfg.scan_on_launch = False
    else:
        base_cfg.scan_on_launch = True
    if args.normal_mode:
        base_cfg.shutdown_mode = "off"
    if args.shutdown_fade:
        base_cfg.shutdown_mode = "fade"
        base_cfg.shutdown_block_new_trades = True
    if args.shutdown_close_now:
        base_cfg.shutdown_mode = "close_now"
        base_cfg.shutdown_block_new_trades = True
        base_cfg.shutdown_skip_gpt = True

    bot = BroadNewsForexManager(
        build_exp_config(base_cfg, execute_requested=not args.dry_run)
    )
    explicit_flags = (
        args.scan_now or args.monitor_now or args.write_recaps_now or args.daily_report_now
        or args.shutdown_pass_now or args.event_scan_now or args.weekend_summary_now
        or args.sunday_reopen_now or args.news_watch_now
    )
    process_lock: live_prod.LiveAccountProcessLock | None = None
    if not args.print_config or explicit_flags:
        process_lock = live_prod.LiveAccountProcessLock(
            bot.cfg.data_dir / "account_process.lock",
            f"{bot.cfg.account_lane} practice-{str(bot.cfg.oanda_account_id)[-3:]}",
        )
        process_lock.acquire()
        advisor.log(f"{bot.lane_prefix()}Acquired account process lock: {process_lock.path}")

    try:
        if args.print_config:
            advisor.log(
                f"Resolved config for lane={bot.cfg.account_lane} "
                f"account_name={bot.cfg.account_display_name}"
            )
            advisor.log(f"practice_execution_enabled={bot.should_execute()}")
            advisor.log(f"market_movement_ledger={bot.cfg.market_movements_csv.parent}")
            bot.print_config()

        if not args.print_config or explicit_flags:
            bot.validate_config()

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
        if args.news_watch_now:
            advisor.ensure_bot_ready(bot)
            bot.run_news_watch_scan(reason="manual_demo", force=True)
            did_explicit = True
        if args.scan_now:
            advisor.ensure_bot_ready(bot)
            bot.run_gpt_scan(reason="manual_demo")
            did_explicit = True
        if args.event_scan_now:
            advisor.ensure_bot_ready(bot)
            bot.run_event_scan(reason="manual_demo")
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
