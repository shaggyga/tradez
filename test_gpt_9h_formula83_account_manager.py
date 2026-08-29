from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest import mock

import oanda_advisor_account_manager_auto as advisor
import oanda_gpt_9h_formula83_account_manager as botmod


class NineHourFormula83Tests(unittest.TestCase):
    def test_supervisor_explicitly_enables_practice_execution(self) -> None:
        supervisor = (Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1").read_text(
            encoding="utf-8-sig"
        )
        start = supervisor.index('-Name "practice_013_formula83"')
        end = supervisor.index("$managed +=", start)
        formula83_block = supervisor[start:end]

        self.assertIn("oanda_gpt_9h_formula83_account_manager.py", formula83_block)
        self.assertIn('"--execute"', formula83_block)

    def test_formula_reconstructs_historical_83_score(self) -> None:
        components = botmod.formula_83_components(
            move_spread_ratio=2.770870337477246,
            momentum_5_pips=7.800000000000296,
            acceleration_pips=6.066666666667213,
            compression_score=2.2790697674418756,
            volatility_30_pips=3.121580796113962,
            spread_pips=1.8766666666667693,
            instrument="SGD_JPY",
        )

        self.assertAlmostEqual(components["score"], 83.29156098971292)
        self.assertAlmostEqual(components["momentum_5_component"], 6.5)
        self.assertEqual(components["low_volatility_penalty"], 0)
        self.assertEqual(components["wide_spread_penalty"], 0)

    def test_currency_flags_translate_usd_base_and_quote_directions(self) -> None:
        flags = botmod.currency_flags(
            [
                {
                    "available": True,
                    "flagged": True,
                    "instrument": "USD_JPY",
                    "direction": "LONG",
                    "score": 91.0,
                    "theme": "USD_RALLY",
                },
                {
                    "available": True,
                    "flagged": True,
                    "instrument": "EUR_USD",
                    "direction": "LONG",
                    "score": 84.0,
                    "theme": "USD_SELLOFF",
                },
            ]
        )

        self.assertEqual(flags["usd_flag"]["state"], "CONFLICT")
        self.assertEqual(flags["usd_flag"]["strength_score"], 91.0)
        self.assertEqual(flags["usd_flag"]["weakness_score"], 84.0)

    def test_config_is_pinned_to_clean_practice_013_and_requires_execute_flag(self) -> None:
        base = advisor.BotConfig.load()

        dry = botmod.build_config(base, execute_requested=False)
        execute = botmod.build_config(base, execute_requested=True)

        self.assertEqual(dry.oanda_env, "practice")
        self.assertEqual(dry.oanda_account_id, "101-001-37981792-013")
        self.assertEqual(dry.account_lane, "gpt_9h_formula83")
        self.assertFalse(dry.allow_live)
        self.assertFalse(dry.execute_trades)
        self.assertTrue(execute.execute_trades)
        self.assertEqual(execute.max_risk_pct_per_trade, 2.0)
        self.assertEqual(execute.max_new_trades_per_scan, 5)
        self.assertEqual(execute.max_total_new_risk_pct_per_scan, 8.0)
        self.assertEqual(execute.min_minutes_between_gpt_scans, 12)
        self.assertEqual(execute.call_times_ny[:4], ["00:15", "00:45", "01:15", "01:45"])
        self.assertEqual(execute.call_times_ny[-1], "23:45")
        self.assertEqual(len(execute.call_times_ny), 48)
        self.assertEqual(execute.friday_call_times_ny, execute.call_times_ny)

    def test_lane_runtime_overrides_use_one_loss_failed_thesis_cooldown(self) -> None:
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_LOCAL_MONITOR_INTERVAL_MINUTES"], 5)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_LOOP_SLEEP_SECONDS"], 30)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_MIN_MINUTES_BETWEEN_GPT_SCANS"], 12)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_LIVE_FAILED_THESIS_MAX_LOSSES"], 1)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_LIVE_MAX_SAME_THESIS_LOSSES"], 1)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_LIVE_SAME_PAIR_LOSS_CAP_MAX_LOSING_CLOSES"], 1)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_LIVE_DECISION_QUALITY_MAX_RETRIES"], 2)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_GPT9H_MAX_OPEN_LOSS_ACCOUNT_CCY"], 150)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_GPT9H_MAX_TOTAL_OPEN_LOSS_ACCOUNT_CCY"], 275)
        self.assertEqual(botmod.LANE_RUNTIME_OVERRIDES["FOREX_GPT9H_MAX_LOSS_GUARD_CLOSES_PER_PASS"], 1)
        with mock.patch.dict(advisor.os.environ, {}, clear=False):
            old_creds = dict(advisor.CREDS)
            try:
                botmod.apply_lane_runtime_overrides()
                self.assertEqual(advisor.CREDS["FOREX_LOCAL_MONITOR_INTERVAL_MINUTES"], "5")
                self.assertEqual(advisor.CREDS["FOREX_LOOP_SLEEP_SECONDS"], "30")
                self.assertEqual(advisor.CREDS["FOREX_MIN_MINUTES_BETWEEN_GPT_SCANS"], "12")
                self.assertEqual(advisor.CREDS["FOREX_LIVE_FAILED_THESIS_MAX_LOSSES"], "1")
                self.assertEqual(advisor.CREDS["FOREX_LIVE_MAX_SAME_THESIS_LOSSES"], "1")
                self.assertEqual(advisor.CREDS["FOREX_LIVE_SAME_PAIR_LOSS_CAP_MAX_LOSING_CLOSES"], "1")
                self.assertEqual(advisor.CREDS["FOREX_LIVE_DECISION_QUALITY_MAX_RETRIES"], "2")
                self.assertEqual(advisor.CREDS["FOREX_GPT9H_MAX_OPEN_LOSS_ACCOUNT_CCY"], "150")
                self.assertEqual(advisor.CREDS["FOREX_GPT9H_MAX_TOTAL_OPEN_LOSS_ACCOUNT_CCY"], "275")
                self.assertEqual(advisor.CREDS["FOREX_GPT9H_MAX_LOSS_GUARD_CLOSES_PER_PASS"], "1")
            finally:
                advisor.CREDS.clear()
                advisor.CREDS.update(old_creds)

    def test_aggressive_loss_guard_closes_worst_trade_on_caps(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=True)
        )
        manager.full_logger = mock.Mock()
        manager.close_trade = mock.Mock(return_value="accepted")
        trades = [
            {
                "id": "67",
                "instrument": "AUD_CAD",
                "currentUnits": "223311",
                "unrealizedPL": "-128.21",
            },
            {
                "id": "72",
                "instrument": "NZD_SGD",
                "currentUnits": "185752",
                "unrealizedPL": "-178.83",
            },
        ]

        closed = manager.run_formula83_aggressive_loss_guard(
            reason="test_guard",
            open_trades=trades,
        )

        self.assertEqual(closed, 1)
        closed_trade, action = manager.close_trade.call_args.args
        self.assertEqual(closed_trade["id"], "72")
        self.assertEqual(action["action"], "CLOSE")
        self.assertIn("per_trade_cap=150.00", action["reason"])

    def test_aggressive_loss_guard_holds_when_below_caps(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=True)
        )
        manager.full_logger = mock.Mock()
        manager.close_trade = mock.Mock(return_value="accepted")

        closed = manager.run_formula83_aggressive_loss_guard(
            open_trades=[
                {
                    "id": "67",
                    "instrument": "AUD_CAD",
                    "currentUnits": "223311",
                    "unrealizedPL": "-80.00",
                },
                {
                    "id": "72",
                    "instrument": "NZD_SGD",
                    "currentUnits": "185752",
                    "unrealizedPL": "-90.00",
                },
            ],
        )

        self.assertEqual(closed, 0)
        manager.close_trade.assert_not_called()

    def test_aggressive_review_score_bucket_is_risk_capped(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )

        context = manager.live_expectancy_context()

        buckets = {
            item.get("bucket")
            for item in context.get("weak_score_buckets", [])
            if isinstance(item, dict)
        }
        self.assertIn("70_79", buckets)

    def test_live_expectancy_audit_annotation_is_idempotent(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "orders_to_execute": [
                {
                    "action": "OPEN",
                    "instrument": "CHF_JPY",
                    "direction": "LONG",
                    "risk_pct": 1.0,
                    "live_expectancy_critic": {
                        "local_verdict": "revise_risk",
                        "notes": [
                            "expectancy critic capped weak score bucket 80_89 risk 1.2% -> 1%"
                        ],
                    },
                }
            ]
        }

        self.assertTrue(manager.live_expectancy_critic_already_applied(decision))

    def test_config_rejects_account_alias_drift(self) -> None:
        with mock.patch.dict(
            advisor.os.environ,
            {botmod.ACCOUNT_ALIAS: "101-001-37981792-015"},
        ):
            with self.assertRaisesRegex(RuntimeError, "pinned"):
                botmod.build_config(advisor.BotConfig.load(), execute_requested=False)

    def test_prompt_and_schema_require_nine_hour_and_usd_flag_review(self) -> None:
        botmod.apply_decision_schema_extensions()

        self.assertIn("nine_hour_forecast", advisor.DECISION_SCHEMA["properties"])
        self.assertIn("usd_flag_review", advisor.DECISION_SCHEMA["properties"])
        self.assertIn("score of 83 is a technical flag", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn('Do not describe any formula_83 flag as "high probability"', botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("aggressive_review candidates", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("Explicitly review usd_flag", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("orders_to_execute", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("A USD_RALLY thesis supports", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("opposite-USD expressions", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("blocks both USD_LONG and USD_SHORT", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn("best three non-USD cross setups", botmod.SYSTEM_PROMPT_APPEND)
        self.assertIn(
            "Event permissions must match the declared portfolio USD thesis",
            botmod.SYSTEM_PROMPT_APPEND,
        )
        self.assertIn("Do not emit USD_SELLOFF", botmod.SYSTEM_PROMPT_APPEND)

    def test_mixed_usd_thesis_blocks_directional_usd_event_permission(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "portfolio_bias": "MIXED_UNCLEAR",
            "portfolio_mode": "MIXED_UNCLEAR",
            "market_summary": "USD signals are conflicting across pairs.",
            "orders_to_execute": [],
            "event_permissions": [
                {
                    "theme": "USD_SELLOFF",
                    "allowed_directions": {"USD_THB": "SHORT"},
                    "allowed_pairs": ["USD_THB"],
                },
                {
                    "theme": "JPY_WEAKNESS",
                    "allowed_directions": {"CHF_JPY": "LONG"},
                    "allowed_pairs": ["CHF_JPY"],
                },
            ],
        }

        review = manager.live_macro_consistency_review(decision)

        self.assertEqual(review["local_verdict"], "block_conflicting_new_exposure")
        self.assertEqual(len(review["blocked_event_permissions"]), 1)
        self.assertEqual(review["blocked_event_permissions"][0]["theme"], "USD_SELLOFF")
        self.assertIn("mixed or undeclared", review["blocked_event_permissions"][0]["reason"])

    def test_usd_weak_bias_allows_usd_selloff_event_permission(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "portfolio_bias": "USD_WEAK",
            "portfolio_mode": "MIXED_UNCLEAR",
            "market_summary": "USD selloff pressure remains dominant.",
            "orders_to_execute": [],
            "underdeployment_reason": "Existing USD short exposure is being managed.",
            "event_permissions": [
                {
                    "theme": "USD_SELLOFF",
                    "allowed_directions": {"USD_THB": "SHORT"},
                    "allowed_pairs": ["USD_THB"],
                }
            ],
        }

        review = manager.live_macro_consistency_review(decision)

        self.assertEqual(review["portfolio_usd_thesis"], "USD_SHORT")
        self.assertEqual(review["blocked_event_permissions"], [])

    def test_decision_quality_flags_hold_against_declared_usd_thesis(self) -> None:
        review = botmod.live_prod.live_decision_quality_review(
            {
                "portfolio_bias": "USD_BULLISH",
                "portfolio_mode": "RELATIVE_VALUE",
                "market_summary": (
                    "Strong USD_RALLY phase across the formula-83 scan. "
                    "The existing USD_THB short is described as local USD weakness "
                    "despite broader USD strength."
                ),
                "orders_to_execute": [],
                "underdeployment_reason": "Existing position is being managed.",
                "open_position_actions": [
                    {
                        "action": "HOLD",
                        "instrument": "USD_THB",
                        "direction": "SHORT",
                        "reason": "Keep short because of local thesis despite broad USD strength.",
                    }
                ],
            }
        )

        self.assertEqual(review["local_verdict"], "retry_decision_quality")
        self.assertEqual(len(review["open_position_thesis_contradictions"]), 1)
        contradiction = review["open_position_thesis_contradictions"][0]
        self.assertEqual(contradiction["position_usd_thesis"], "USD_SHORT")
        self.assertEqual(contradiction["portfolio_usd_thesis"], "USD_LONG")

    def test_failed_thesis_cooldown_counts_as_concrete_watch_blocker(self) -> None:
        review = botmod.live_prod.live_decision_quality_review(
            {
                "portfolio_bias": "USD_BULLISH",
                "portfolio_mode": "RELATIVE_VALUE",
                "underdeployment_reason": (
                    "USD_JPY is blocked by active failed thesis cooldown."
                ),
                "new_trade_candidates": [
                    {
                        "action": "WATCH",
                        "instrument": "USD_JPY",
                        "direction": "LONG",
                        "outlook_confidence": 70,
                        "risk_pct": 1.0,
                        "expected_R": 1.3,
                        "entry_min": 161.5,
                        "entry_max": 162.0,
                        "stop_loss": 159.0,
                        "take_profit": 166.0,
                        "concrete_no_trade_blocker": (
                            "Active failed thesis cooldown restricts new USD_LONG "
                            "entries despite strong momentum."
                        ),
                        "reason": "Await cooldown expiration and macro confirmation.",
                    }
                ]
            }
        )

        self.assertEqual(review["watch_accountability"], [])
        self.assertEqual(review["issue_count"], 0)

    def test_unresolved_contradictory_hold_converts_to_partial_close(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.instruments = ["USD_THB"]
        manager.full_logger = mock.Mock()
        decision = {
            "portfolio_bias": "USD_BULLISH",
            "portfolio_mode": "RELATIVE_VALUE",
            "market_summary": (
                "Strong USD_RALLY phase. The existing USD_THB short is described "
                "as local USD weakness despite broader USD strength."
            ),
            "orders_to_execute": [],
            "underdeployment_reason": "Existing position is being managed.",
            "open_position_actions": [
                {
                    "action": "HOLD",
                    "instrument": "USD_THB",
                    "direction": "SHORT",
                    "trade_id": "21",
                    "reason": "Keep short because of local thesis despite broad USD strength.",
                }
            ],
        }

        manager.enforce_unresolved_open_position_thesis_contradictions(decision)

        action = decision["open_position_actions"][0]
        self.assertEqual(action["action"], "PARTIAL_CLOSE")
        self.assertEqual(action["partial_close_pct"], 50.0)
        self.assertIn("Local defensive reduction", action["reason"])
        self.assertEqual(len(decision["local_defensive_contradictory_hold_reductions"]), 1)

    def test_blocked_event_permissions_clear_stale_state(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.instruments = ["USD_THB"]
        manager.full_logger = mock.Mock()
        saved_states = []
        old_state = {
            "event_permissions": [
                {
                    "theme": "USD_RALLY",
                    "allowed_directions": {"USD_THB": "LONG"},
                }
            ]
        }
        decision = {
            "portfolio_bias": "MIXED_UNCLEAR",
            "portfolio_mode": "MIXED_UNCLEAR",
            "market_summary": "USD signals are conflicting.",
            "orders_to_execute": [],
            "event_permissions": [
                {
                    "theme": "USD_RALLY",
                    "allowed_directions": {"USD_THB": "LONG"},
                    "allowed_pairs": ["USD_THB"],
                }
            ],
        }

        with mock.patch.object(manager, "load_state", return_value=dict(old_state)):
            with mock.patch.object(manager, "save_state", side_effect=saved_states.append):
                manager.save_event_permissions_from_decision(decision)

        self.assertEqual(decision["event_permissions"], [])
        self.assertEqual(saved_states[-1]["event_permissions"], [])

    def test_news_watch_numeric_directional_bias_is_preserved(self) -> None:
        now = advisor.dt.datetime(2026, 7, 14, 9, 24, tzinfo=advisor.UTC)
        watch = botmod.live_prod.normalize_news_watch_item(
            {
                "headline": "US Dollar Strengthens Amid Renewed Strait of Hormuz Tensions",
                "source_name": "FXStreet",
                "source_url": "https://www.fxstreet.com/news/forex-today-us-dollar-surges",
                "severity": 80,
                "currencies": ["USD", "THB"],
                "directional_bias": {"USD": 1, "THB": -1},
                "movement_potential": "HIGH",
                "pair_hints": ["USD/THB"],
                "published_utc": "2026-07-14T09:20:00+00:00",
                "reported_update_utc": "2026-07-14T09:24:00+00:00",
            },
            now=now,
            min_severity=70,
            lookback_minutes=20,
            verified_citations=None,
            available_instruments=["USD_THB"],
        )

        self.assertIsNotNone(watch)
        assert watch is not None
        self.assertEqual(watch["directional_bias"]["USD"], "BULLISH")
        self.assertEqual(watch["directional_bias"]["THB"], "BEARISH")

    def test_active_news_watch_repairs_stored_unknown_numeric_bias(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.instruments = ["USD_THB"]
        state = {
            "active_news_watches": [
                {
                    "watch_id": "news_test",
                    "currencies": ["USD", "THB"],
                    "directional_bias": {"USD": "UNKNOWN"},
                    "expires_utc": "2026-07-14T10:00:00+00:00",
                    "source_verified": True,
                    "pair_hints": ["USD_THB"],
                    "raw": {"directional_bias": {"USD": 1, "THB": -1}},
                }
            ]
        }
        with mock.patch.object(botmod.live_prod.advisor, "utc_now", return_value=advisor.dt.datetime(2026, 7, 14, 9, 30, tzinfo=advisor.UTC)):
            active = manager.active_news_watches(state)

        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["directional_bias"]["USD"], "BULLISH")
        self.assertEqual(active[0]["directional_bias"]["THB"], "BEARISH")

    def test_conflicting_news_watch_caps_event_permission_risk(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.full_logger = mock.Mock()
        manager.active_news_watches = mock.Mock(
            return_value=[
                {
                    "watch_id": "news_usd_bullish",
                    "directional_bias": {"USD": "BULLISH"},
                }
            ]
        )
        decision = {
            "event_permissions": [
                {
                    "theme": "USD_SELLOFF",
                    "allowed_directions": {"USD_THB": "SHORT"},
                    "allowed_pairs": ["USD_THB"],
                    "max_scout_risk_pct": 1.5,
                    "requires_basket_confirmation": False,
                    "reason": "USD selloff permission.",
                }
            ]
        }

        manager.adjust_event_permissions_for_active_news_watch_conflicts(decision)

        permission = decision["event_permissions"][0]
        self.assertEqual(permission["max_scout_risk_pct"], 0.35)
        self.assertTrue(permission["requires_basket_confirmation"])
        self.assertIn("conflicting USD bias", permission["reason"])
        self.assertEqual(
            permission["active_news_watch_conflict_adjustment"]["permission_usd_thesis"],
            "USD_SHORT",
        )

    def test_failed_thesis_blocks_matching_event_permission_and_clears_stale_state(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.full_logger = mock.Mock()
        manager.instruments = ["USD_CHF"]
        manager.active_news_watches = mock.Mock(return_value=[])
        saved_states = []
        old_state = {
            "event_permissions": [
                {
                    "theme": "USD_RALLY",
                    "allowed_directions": {"USD_CHF": "LONG"},
                }
            ]
        }
        decision = {
            "portfolio_bias": "USD_BULLISH",
            "portfolio_mode": "MIXED_UNCLEAR",
            "market_summary": "USD rally, but same-thesis cooldown is active.",
            "orders_to_execute": [],
            "live_failed_thesis": {
                "active": True,
                "blocked_keys": ["USD_LONG"],
                "threshold_losses": 1,
            },
            "event_permissions": [
                {
                    "theme": "USD_RALLY",
                    "allowed_directions": {"USD_CHF": "LONG"},
                    "allowed_pairs": ["USD_CHF"],
                    "max_scout_risk_pct": 0.35,
                    "requires_basket_confirmation": True,
                    "reason": "USD-long permission should not survive cooldown.",
                }
            ],
        }

        with mock.patch.object(manager, "load_state", return_value=dict(old_state)):
            with mock.patch.object(manager, "save_state", side_effect=saved_states.append):
                manager.save_event_permissions_from_decision(decision)

        self.assertEqual(decision["event_permissions"], [])
        self.assertEqual(saved_states[-1]["event_permissions"], [])
        self.assertEqual(
            decision["blocked_event_permissions"][0]["permission_usd_thesis"],
            "USD_LONG",
        )
        self.assertIn(
            "live failed-thesis guard",
            decision["blocked_event_permissions"][0]["live_failed_thesis_blocked"],
        )

    def test_failed_thesis_converts_matching_open_candidate_in_place(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.live_failed_thesis_status = mock.Mock(
            return_value={
                "active": True,
                "blocked_keys": ["USD_LONG"],
                "threshold_losses": 1,
                "opposite_keys_for_review": [],
                "cooldowns": {},
                "recent_losing_closes": [],
            }
        )
        decision = {
            "orders_to_execute": [
                {
                    "instrument": "USD_CHF",
                    "action": "OPEN",
                    "direction": "LONG",
                    "reason": "Executable but same USD thesis.",
                }
            ],
            "new_trade_candidates": [
                {
                    "rank": 1,
                    "instrument": "USD_CAD",
                    "action": "OPEN",
                    "direction": "LONG",
                    "reason": "Confirmed USD rally continuation.",
                },
                {
                    "rank": 2,
                    "instrument": "AUD_USD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "reason": "Already watching.",
                },
            ],
        }

        manager.apply_live_failed_thesis_policy(decision)

        self.assertEqual(decision["orders_to_execute"], [])
        candidate = decision["new_trade_candidates"][0]
        self.assertEqual(candidate["action"], "WATCH")
        self.assertIn("USD_LONG", candidate["live_failed_thesis_blocked"])
        self.assertIn("USD_LONG", candidate["concrete_no_trade_blocker"])
        self.assertEqual(decision["new_trade_candidates"][1]["action"], "WATCH")
        order_block = decision["new_trade_candidates"][2]
        self.assertEqual(order_block["instrument"], "USD_CHF")
        self.assertEqual(order_block["action"], "WATCH")
        self.assertIn("USD_LONG", order_block["concrete_no_trade_blocker"])
        self.assertEqual(
            decision["live_failed_thesis_candidate_adjustments"][0]["instrument"],
            "USD_CAD",
        )

    def test_both_usd_blocks_require_non_usd_cross_candidate_accountability(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.live_missed_entry_audit = mock.Mock(return_value=[])
        decision = {
            "orders_to_execute": [],
            "underdeployment_reason": "USD directions are blocked; cross candidates need confirmation.",
            "live_failed_thesis": {
                "active": True,
                "blocked_keys": ["USD_LONG", "USD_SHORT"],
            },
            "new_trade_candidates": [
                {
                    "instrument": "AUD_CAD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "Waiting for breakout.",
                }
            ],
            "pair_bucket_coverage_review": [
                {
                    "bucket": "aud_nzd_cad_crosses",
                    "best_instrument": "AUD_CAD",
                    "reviewed_instruments": ["AUD_CAD", "AUD_NZD", "NZD_CAD"],
                },
                {
                    "bucket": "chf_crosses",
                    "best_instrument": "CAD_CHF",
                    "reviewed_instruments": ["CAD_CHF"],
                },
            ],
        }

        review = manager.annotate_live_decision_quality(
            decision,
            {},
            [{"id": "45", "instrument": "CHF_JPY", "currentUnits": "61933"}],
        )

        self.assertEqual(review["local_verdict"], "retry_decision_quality")
        self.assertTrue(
            any("Both USD_LONG and USD_SHORT are blocked" in issue for issue in review["issues"])
        )
        self.assertEqual(
            review["non_usd_fallback_accountability"]["non_usd_candidates"],
            1,
        )
        self.assertEqual(
            review["non_usd_fallback_accountability"]["target_non_usd_candidates"],
            3,
        )

    def test_both_usd_blocks_accept_three_non_usd_cross_candidates(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.live_missed_entry_audit = mock.Mock(return_value=[])
        decision = {
            "orders_to_execute": [],
            "underdeployment_reason": (
                "Both USD directions are blocked by live_failed_thesis; the three "
                "non-USD cross candidates are WATCH because each lacks confirmed "
                "technical breakout."
            ),
            "live_failed_thesis": {
                "active": True,
                "blocked_keys": ["USD_LONG", "USD_SHORT"],
            },
            "new_trade_candidates": [
                {
                    "instrument": "AUD_CAD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "Waiting for breakout.",
                },
                {
                    "instrument": "EUR_CAD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "Waiting for breakout.",
                },
                {
                    "instrument": "GBP_CAD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "Waiting for breakout.",
                },
            ],
            "pair_bucket_coverage_review": [
                {
                    "bucket": "crosses",
                    "reviewed_instruments": ["AUD_CAD", "EUR_CAD", "GBP_CAD"],
                }
            ],
        }

        review = manager.annotate_live_decision_quality(decision, {}, [])

        self.assertEqual(review["local_verdict"], "ok")
        self.assertNotIn("non_usd_fallback_accountability", review)

    def test_both_usd_blocks_accept_managed_non_usd_open_positions(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.live_missed_entry_audit = mock.Mock(return_value=[])
        decision = {
            "orders_to_execute": [],
            "underdeployment_reason": "No additional order: existing non-USD cross position is already deployed and managed; wait for margin risk to free up or a fresh confirmed breakout.",
            "live_failed_thesis": {
                "active": True,
                "blocked_keys": ["USD_LONG", "USD_SHORT"],
            },
            "open_position_actions": [
                {
                    "instrument": "CHF_JPY",
                    "action": "HOLD",
                    "direction": "LONG",
                    "trade_id": "45",
                    "reason": "Managed non-USD cross exposure.",
                }
            ],
            "new_trade_candidates": [],
            "pair_bucket_coverage_review": [
                {
                    "bucket": "crosses",
                    "reviewed_instruments": ["CHF_JPY", "EUR_CAD", "GBP_CAD"],
                }
            ],
        }

        review = manager.annotate_live_decision_quality(decision, {}, [])

        self.assertEqual(review["local_verdict"], "ok")
        self.assertNotIn("non_usd_fallback_accountability", review)

    def test_open_candidate_for_existing_position_is_converted_to_hold(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "orders_to_execute": [],
            "new_trade_candidates": [
                {
                    "instrument": "AUD_NZD",
                    "action": "OPEN",
                    "direction": "LONG",
                    "reason": "Still attractive.",
                    "risk_pct": 1.0,
                },
                {
                    "instrument": "GBP_CAD",
                    "action": "OPEN",
                    "direction": "LONG",
                    "reason": "Fresh cross.",
                    "risk_pct": 1.0,
                },
            ],
        }

        with mock.patch.object(
            botmod.gpt_exp.BroadNewsForexManager,
            "normalize_decision",
            autospec=True,
            side_effect=lambda _self, next_decision, _open_trades: next_decision,
        ):
            normalized = manager.normalize_decision(
                decision,
                [{"id": "49", "instrument": "AUD_NZD", "currentUnits": "142721"}],
            )

        aud_nzd = normalized["new_trade_candidates"][0]
        self.assertEqual(aud_nzd["action"], "HOLD")
        self.assertIn("Position already open", aud_nzd["concrete_no_trade_blocker"])
        self.assertEqual(normalized["new_trade_candidates"][1]["action"], "WATCH")
        self.assertIn(
            "No matching executable order",
            normalized["new_trade_candidates"][1]["concrete_no_trade_blocker"],
        )
        self.assertEqual(
            normalized["duplicate_open_candidate_cleanup"][0]["instrument"],
            "AUD_NZD",
        )

    def test_dual_usd_blocks_prioritize_true_non_usd_candidates(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "orders_to_execute": [],
            "live_failed_thesis": {
                "active": True,
                "blocked_keys": ["USD_LONG", "USD_SHORT"],
            },
            "new_trade_candidates": [
                {
                    "instrument": "AUD_USD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "USD directional thesis is blocked.",
                },
                {
                    "instrument": "CAD_HKD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "Spread too wide.",
                },
                {
                    "instrument": "EUR_AUD",
                    "action": "WATCH",
                    "direction": "SHORT",
                    "concrete_no_trade_blocker": "Needs breakout confirmation.",
                },
            ],
        }

        with mock.patch.object(
            botmod.gpt_exp.BroadNewsForexManager,
            "normalize_decision",
            autospec=True,
            side_effect=lambda _self, next_decision, _open_trades: next_decision,
        ):
            normalized = manager.normalize_decision(decision, [])

        self.assertEqual(
            [item["instrument"] for item in normalized["new_trade_candidates"]],
            ["CAD_HKD", "EUR_AUD", "AUD_USD"],
        )
        self.assertEqual(
            normalized["non_usd_candidate_priority_adjustment"]["original_order"],
            ["AUD_USD", "CAD_HKD", "EUR_AUD"],
        )

    def test_unmatched_open_candidate_is_converted_to_watch(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "orders_to_execute": [],
            "new_trade_candidates": [
                {
                    "instrument": "CAD_JPY",
                    "action": "OPEN",
                    "direction": "LONG",
                    "expected_R": 1.1,
                    "reason": "Looks viable but no order survived cleanup.",
                }
            ],
        }

        with mock.patch.object(
            botmod.gpt_exp.BroadNewsForexManager,
            "normalize_decision",
            autospec=True,
            side_effect=lambda _self, next_decision, _open_trades: next_decision,
        ):
            normalized = manager.normalize_decision(decision, [])

        candidate = normalized["new_trade_candidates"][0]
        self.assertEqual(candidate["action"], "WATCH")
        self.assertIn("No matching executable order", candidate["concrete_no_trade_blocker"])
        self.assertEqual(
            normalized["unmatched_open_candidate_cleanup"][0]["instrument"],
            "CAD_JPY",
        )

    def test_duplicate_blocked_watch_candidates_are_deduped(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        blocker = "same-pair loss cap blocks AUD_NZD after 2 losing close(s) today"
        decision = {
            "orders_to_execute": [],
            "new_trade_candidates": [
                {
                    "instrument": "AUD_NZD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": blocker,
                    "same_pair_loss_cap_blocked": blocker,
                },
                {
                    "instrument": "AUD_NZD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": blocker,
                    "same_pair_loss_cap_blocked": blocker,
                },
            ],
        }

        with mock.patch.object(
            botmod.gpt_exp.BroadNewsForexManager,
            "normalize_decision",
            autospec=True,
            side_effect=lambda _self, next_decision, _open_trades: next_decision,
        ):
            normalized = manager.normalize_decision(decision, [])

        self.assertEqual(len(normalized["new_trade_candidates"]), 1)
        self.assertEqual(
            normalized["duplicate_blocked_candidate_cleanup"][0]["instrument"],
            "AUD_NZD",
        )

    def test_event_permission_removes_pairs_with_candidate_blockers(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "new_trade_candidates": [
                {
                    "instrument": "CAD_HKD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "Wide spread and exotic pair risk.",
                },
                {
                    "instrument": "AUD_NZD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "concrete_no_trade_blocker": "",
                },
                {
                    "instrument": "AUD_CAD",
                    "action": "WATCH",
                    "direction": "LONG",
                    "reason": "Technical breakout required for entry confirmation.",
                },
            ],
            "event_permissions": [
                {
                    "theme": "COMMODITY_CURRENCY_STRENGTH",
                    "allowed_pairs": ["AUD_NZD", "CAD_HKD", "AUD_CAD"],
                    "allowed_directions": {
                        "AUD_NZD": "LONG",
                        "CAD_HKD": "LONG",
                        "AUD_CAD": "LONG",
                    },
                }
            ],
        }

        manager.sanitize_blocked_candidate_event_permissions(decision)

        self.assertEqual(decision["event_permissions"][0]["allowed_pairs"], ["AUD_NZD"])
        self.assertEqual(
            decision["event_permissions"][0]["allowed_directions"],
            {"AUD_NZD": "LONG"},
        )
        blocked = decision["blocked_event_permissions"][0]
        removed = {
            item["instrument"]: item["concrete_no_trade_blocker"]
            for item in blocked["removed_pairs"]
        }
        self.assertIn("Wide spread", removed["CAD_HKD"])
        self.assertIn("Technical breakout required", removed["AUD_CAD"])

    def test_open_trade_correlation_removes_event_permission_pairs(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "event_permissions": [
                {
                    "theme": "JPY_WEAKNESS",
                    "allowed_pairs": ["AUD_JPY", "EUR_JPY", "GBP_JPY"],
                    "allowed_directions": {
                        "AUD_JPY": "LONG",
                        "EUR_JPY": "LONG",
                        "GBP_JPY": "LONG",
                    },
                    "reason": "JPY weakness basket permission.",
                }
            ]
        }
        open_trades = [
            {
                "id": "93",
                "instrument": "EUR_JPY",
                "currentUnits": "43067",
            }
        ]

        manager.sanitize_correlated_open_trade_event_permissions(decision, open_trades)

        self.assertEqual(decision["event_permissions"], [])
        blocked = decision["blocked_event_permissions"][0]
        removed = {
            item["instrument"]: item["overlapping_active_thesis_keys"]
            for item in blocked["removed"]
        }
        self.assertEqual(removed["AUD_JPY"], ["JPY_SHORT"])
        self.assertEqual(removed["EUR_JPY"], ["EUR_LONG", "JPY_SHORT"])
        self.assertEqual(removed["GBP_JPY"], ["JPY_SHORT"])

    def test_undersized_trailing_stop_removed_against_recommended_stop(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        decision = {
            "orders_to_execute": [
                {
                    "action": "OPEN",
                    "instrument": "AUD_NZD",
                    "direction": "LONG",
                    "risk_pct": 1.0,
                    "stop_loss": 1.185,
                    "take_profit": 1.215,
                    "trailing_stop_pips": 15,
                    "expectancy_critic": {
                        "recommended_stop_pips": 115,
                    },
                    "reason": "Probe with trailing stop.",
                }
            ],
            "new_trade_candidates": [
                {
                    "action": "OPEN",
                    "instrument": "AUD_NZD",
                    "direction": "LONG",
                    "trailing_stop_pips": 15,
                    "expectancy_critic": {
                        "recommended_stop_pips": 115,
                    },
                    "reason": "Candidate with same undersized trailing stop.",
                }
            ]
        }

        manager.apply_live_stop_policy(decision)

        order = decision["orders_to_execute"][0]
        self.assertIsNone(order["trailing_stop_pips"])
        self.assertIn("removed undersized trailing_stop_pips", order["reason"])
        self.assertIn("lane_trailing_stop_policy_adjustment", order)
        candidate = decision["new_trade_candidates"][0]
        self.assertIsNone(candidate["trailing_stop_pips"])
        self.assertIn("removed undersized trailing_stop_pips", candidate["reason"])

    def test_active_news_watch_conflict_reduces_open_position_once(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.full_logger = mock.Mock()
        state = {}
        saved_states = []
        manager.load_state = mock.Mock(side_effect=lambda: dict(state))

        def save_state(next_state: dict) -> None:
            state.clear()
            state.update(next_state)
            saved_states.append(dict(next_state))

        manager.save_state = mock.Mock(side_effect=save_state)
        manager.active_news_watches = mock.Mock(
            return_value=[
                {
                    "watch_id": "news_usd_bullish",
                    "severity": 80,
                    "directional_bias": {"USD": "BULLISH"},
                }
            ]
        )
        manager.partial_close_trade = mock.Mock(return_value="accepted")
        manager.refresh_state_after_profit_guard_action = mock.Mock()
        trade = {
            "id": "21",
            "instrument": "USD_THB",
            "currentUnits": "-1000",
            "initialUnits": "-1000",
            "price": "33.508",
            "unrealizedPL": "-25.0",
            "state": "OPEN",
        }

        manager.run_active_news_watch_position_conflict_guard([trade], reason="unit_test")
        manager.run_active_news_watch_position_conflict_guard([trade], reason="unit_test")

        manager.partial_close_trade.assert_called_once()
        action = manager.partial_close_trade.call_args.args[1]
        self.assertEqual(action["action"], "PARTIAL_CLOSE")
        self.assertEqual(action["partial_close_pct"], 25.0)
        self.assertIn("active verified news watch USD bias", action["reason"])
        self.assertTrue(saved_states)
        reductions = state["news_watch_position_conflict_reductions"]
        self.assertEqual(len(reductions), 1)

    def test_same_pair_loss_cap_closes_losing_residual_position(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.full_logger = mock.Mock()
        state = {}
        saved_states = []
        manager.load_state = mock.Mock(side_effect=lambda: dict(state))

        def save_state(next_state: dict) -> None:
            state.clear()
            state.update(next_state)
            saved_states.append(dict(next_state))

        manager.save_state = mock.Mock(side_effect=save_state)
        manager.same_pair_losing_close_counts_today = mock.Mock(
            return_value={
                "USD_THB": {
                    "loss_count": 3,
                    "realized_pl": -87.58,
                    "recent_losses": [{"trade_id": "21", "realized_pl": -29.0}],
                }
            }
        )
        manager.close_trade = mock.Mock(return_value="accepted")
        manager.refresh_state_after_profit_guard_action = mock.Mock()
        trade = {
            "id": "21",
            "instrument": "USD_THB",
            "currentUnits": "-32298",
            "unrealizedPL": "-26.27",
        }

        manager.run_same_pair_loss_cap_guard([trade], reason="unit_test")
        manager.run_same_pair_loss_cap_guard([trade], reason="unit_test")

        manager.close_trade.assert_called_once()
        action = manager.close_trade.call_args.args[1]
        self.assertEqual(action["action"], "CLOSE")
        self.assertIn("same-pair loss cap hit", action["reason"])
        self.assertTrue(saved_states)
        caps = state["same_pair_loss_cap_closes"]
        self.assertEqual(len(caps), 1)
        cap = next(iter(caps.values()))
        self.assertEqual(cap["loss_count"], 3)
        self.assertEqual(cap["status"], "accepted")

    def test_same_pair_loss_cap_blocks_reentry_orders_and_permissions(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.same_pair_losing_close_counts_today = mock.Mock(
            return_value={
                "USD_THB": {
                    "loss_count": 3,
                    "realized_pl": -87.58,
                    "recent_losses": [],
                }
            }
        )
        decision = {
            "orders_to_execute": [
                {
                    "action": "OPEN",
                    "instrument": "USD_THB",
                    "direction": "LONG",
                    "risk_pct": 0.5,
                    "reason": "Try a re-entry.",
                },
                {
                    "action": "OPEN",
                    "instrument": "EUR_GBP",
                    "direction": "LONG",
                    "risk_pct": 0.5,
                    "reason": "Unrelated cross.",
                },
            ],
            "new_trade_candidates": [
                {
                    "action": "OPEN",
                    "instrument": "USD_THB",
                    "direction": "LONG",
                    "risk_pct": 0.5,
                    "reason": "Candidate re-entry should also be blocked.",
                }
            ],
            "event_permissions": [
                {
                    "theme": "USD_RALLY",
                    "allowed_pairs": ["USD_THB"],
                    "allowed_directions": {"USD_THB": "LONG"},
                },
                {
                    "theme": "EUR_GBP_RELATIVE",
                    "allowed_pairs": ["EUR_GBP"],
                    "allowed_directions": {"EUR_GBP": "LONG"},
                },
            ],
        }

        manager.apply_same_pair_loss_cap_policy(decision)

        self.assertEqual(len(decision["orders_to_execute"]), 1)
        self.assertEqual(decision["orders_to_execute"][0]["instrument"], "EUR_GBP")
        blocked = decision["new_trade_candidates"][0]
        self.assertEqual(blocked["instrument"], "USD_THB")
        self.assertEqual(blocked["action"], "WATCH")
        self.assertIn("same-pair loss cap blocks USD_THB", blocked["same_pair_loss_cap_blocked"])
        adjusted = decision["new_trade_candidates"][1]
        self.assertEqual(adjusted["instrument"], "USD_THB")
        self.assertEqual(adjusted["action"], "WATCH")
        self.assertIn("same-pair loss cap blocks USD_THB", adjusted["same_pair_loss_cap_blocked"])
        self.assertEqual(
            decision["same_pair_loss_cap_candidate_adjustments"][0]["instrument"],
            "USD_THB",
        )
        self.assertEqual(len(decision["event_permissions"]), 1)
        self.assertEqual(decision["event_permissions"][0]["theme"], "EUR_GBP_RELATIVE")
        self.assertIn(
            "same-pair loss cap blocks USD_THB",
            decision["blocked_event_permissions"][0]["same_pair_loss_cap_blocked"],
        )

    def test_commodity_currency_permission_blocks_inverted_or_ambiguous_aud_longs(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        manager.full_logger = mock.Mock()
        decision = {
            "event_permissions": [
                {
                    "theme": "COMMODITY_CURRENCY_STRENGTH",
                    "allowed_pairs": ["AUD_CAD", "EUR_AUD", "GBP_AUD"],
                    "allowed_directions": {
                        "AUD_CAD": "LONG",
                        "EUR_AUD": "LONG",
                        "GBP_AUD": "LONG",
                    },
                    "reason": "Supports commodity currency longs.",
                }
            ]
        }

        manager.sanitize_commodity_currency_event_permission_directions(decision)

        self.assertEqual(decision["event_permissions"], [])
        blocked = decision["blocked_event_permissions"][0]
        removed = blocked["removed"]
        self.assertEqual(
            {(item["instrument"], item["direction"]) for item in removed},
            {("AUD_CAD", "LONG"), ("EUR_AUD", "LONG"), ("GBP_AUD", "LONG")},
        )
        self.assertIn("contradicted", blocked["commodity_currency_direction_blocked"])

    def test_post_action_reconcile_forces_live_recap_refresh(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        account = {
            "id": botmod.ACCOUNT_ID,
            "currency": "USD",
            "balance": "99572.6061",
            "NAV": "99572.6061",
            "unrealizedPL": "0.0",
            "marginUsed": "0.0",
            "marginAvailable": "99572.6061",
            "openTradeCount": 0,
            "openPositionCount": 0,
            "pendingOrderCount": 0,
        }
        saved_states = []
        manager.oanda = mock.Mock()
        manager.oanda.get_account_summary.return_value = account
        manager.oanda.get_open_trades.return_value = []
        manager.load_state = mock.Mock(return_value={})
        manager.save_state = mock.Mock(side_effect=saved_states.append)
        manager.sync_transaction_ledger = mock.Mock()
        manager.reconcile_pending_order_state = mock.Mock()
        manager.maybe_write_live_gpt_recap = mock.Mock()

        manager.refresh_state_after_profit_guard_action("unit_test_close")

        self.assertTrue(saved_states)
        self.assertEqual(saved_states[-1]["last_account_snapshot"]["open_trade_count"], 0)
        manager.maybe_write_live_gpt_recap.assert_called_once_with(
            "post_action:unit_test_close",
            force=True,
        )

    def test_live_local_monitor_persists_refreshed_account_snapshot(self) -> None:
        manager = botmod.NineHourFormula83Manager(
            botmod.build_config(advisor.BotConfig.load(), execute_requested=False)
        )
        account = {
            "id": botmod.ACCOUNT_ID,
            "currency": "USD",
            "balance": "100000.0",
            "NAV": "99950.0",
            "unrealizedPL": "-50.0",
            "marginUsed": "1000.0",
            "marginAvailable": "98950.0",
            "openTradeCount": 1,
            "openPositionCount": 1,
            "pendingOrderCount": 2,
        }
        trades = [
            {
                "id": "21",
                "instrument": "USD_THB",
                "currentUnits": "-1000",
                "initialUnits": "-1000",
                "price": "33.508",
                "unrealizedPL": "-50.0",
                "state": "OPEN",
            }
        ]
        saved_states = []
        manager.oanda = mock.Mock()
        manager.oanda.get_account_summary.return_value = account
        manager.oanda.get_open_trades.return_value = trades
        manager.load_state = mock.Mock(return_value={})
        manager.save_state = mock.Mock(side_effect=saved_states.append)
        manager.evaluate_live_failsafe = mock.Mock(return_value={"active": False})
        manager.run_active_news_watch_position_conflict_guard = mock.Mock()
        manager.run_same_pair_loss_cap_guard = mock.Mock()
        manager.run_live_profit_guard = mock.Mock()
        manager.maybe_write_live_gpt_recap = mock.Mock()

        with mock.patch.object(botmod.live_prod.advisor.ForexManager, "run_local_monitor", return_value=None):
            manager.run_local_monitor()

        self.assertTrue(saved_states)
        latest = saved_states[-1]
        self.assertEqual(latest["last_account_snapshot"]["nav"], 99950.0)
        self.assertEqual(latest["last_known_open_trades"][0]["instrument"], "USD_THB")
        self.assertIn("last_reconciled_utc", latest)

    def test_compact_signal_tape_preserves_all_pair_ranking_without_components(self) -> None:
        tape = botmod.compact_signal_tape(
            [
                {
                    "instrument": "EUR_USD",
                    "direction": "SHORT",
                    "theme": "USD_RALLY",
                    "score": 84.12345,
                    "flagged": True,
                    "spread_pips": 0.876,
                    "momentum_5_pips": -3.14159,
                    "momentum_15_pips": -9.99,
                    "components": {"large": "omitted"},
                }
            ]
        )

        self.assertEqual(tape[0]["instrument"], "EUR_USD")
        self.assertEqual(tape[0]["score"], 84.123)
        self.assertTrue(tape[0]["flagged"])
        self.assertTrue(tape[0]["aggressive_review"])
        self.assertNotIn("components", tape[0])

    def test_registry_declares_formula83_and_current_dum4_attachment(self) -> None:
        registry_path = advisor.SCRIPT_DIR / "config" / "accounts_registry.json"
        registry = json.loads(registry_path.read_text(encoding="utf-8"))

        role = registry["roles"]["gpt_9h_formula83"]
        self.assertTrue(role["account_id"].endswith("-013"))
        self.assertEqual(role["execution"], "practice_demo_explicit_execute_only")

        accounts = registry["account_inventory"]["practice_accounts"]
        self.assertEqual(accounts["007"]["active_attachments"][0]["lane"], "primary_signal_system_007")
        self.assertEqual(
            accounts["007"]["active_attachments"][0]["selection"],
            "best_consolidated_signals_from_unified_feed_with_holdout_calibration",
        )
        self.assertEqual(accounts["013"]["status"], "assigned_active")
        self.assertEqual(accounts["015"]["status"], "available_pristine")


if __name__ == "__main__":
    unittest.main()
