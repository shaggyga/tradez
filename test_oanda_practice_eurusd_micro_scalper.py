import json
import math
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import requests

from trad.oanda_practice_eurusd_micro_scalper import (
    OandaApiError,
    PracticeScalper,
    calculate_sizing,
    extract_trade_close,
    parse_args,
    parse_rfc3339,
    quote_from_price_payload,
)


class FakeResponse:
    def __init__(self, status_code, payload, request_id="req-1"):
        self.status_code = status_code
        self.payload = payload
        self.text = json.dumps(payload)
        self.headers = {"RequestID": request_id}

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def request_test_bot(outcomes):
    bot = PracticeScalper.__new__(PracticeScalper)
    bot.args = SimpleNamespace(
        api_retries=2,
        api_retry_base_sec=0.0,
        request_timeout_sec=1.0,
    )
    bot.account_id = "101-001-00000000-002"
    bot.token = "test-token"
    bot.session = FakeSession(outcomes)
    bot.log_events = []
    bot.log = lambda event, **fields: bot.log_events.append((event, fields))
    bot.reset_calls = []
    bot._reset_session = lambda *, reload_credentials: bot.reset_calls.append(reload_credentials)
    return bot


def candle_set(values):
    return [{"mid": {"c": f"{value:.6f}"}} for value in values]


class ParsingTests(unittest.TestCase):
    def test_parses_oanda_nanosecond_timestamp(self):
        parsed = parse_rfc3339("2026-07-13T14:59:00.123456789Z")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.microsecond, 123456)

    def test_quote_payload_tracks_execution_sides(self):
        quote = quote_from_price_payload(
            {
                "time": "2026-07-13T14:59:00Z",
                "tradeable": True,
                "bids": [{"price": "1.14020"}],
                "asks": [{"price": "1.14035"}],
            },
            "test",
        )
        self.assertAlmostEqual(quote.bid, 1.14020)
        self.assertAlmostEqual(quote.ask, 1.14035)
        self.assertAlmostEqual(quote.spread_pips, 1.5)
        self.assertTrue(quote.tradeable)


class SizingTests(unittest.TestCase):
    def test_dynamic_sizing_respects_risk_margin_and_unit_caps(self):
        decision = calculate_sizing(
            balance=57.3134,
            margin_available=57.3134,
            quote_mid=1.1400,
            stop_pips=5.0,
            risk_per_trade_pct=0.25,
            fixed_units=0,
            max_units=250,
            minimum_units=1,
            margin_rate=0.02,
            max_margin_fraction=0.25,
        )
        self.assertEqual(decision.units, 250)
        self.assertEqual(decision.risk_limited_units, 286)
        self.assertGreater(decision.margin_limited_units, decision.units)
        self.assertAlmostEqual(decision.actual_risk, 0.125)
        self.assertLess(decision.actual_risk_pct, 0.25)
        self.assertIn("max_units_cap", decision.reason)

    def test_fixed_units_cannot_bypass_risk_cap(self):
        decision = calculate_sizing(
            balance=50.0,
            margin_available=50.0,
            quote_mid=1.1000,
            stop_pips=10.0,
            risk_per_trade_pct=0.25,
            fixed_units=1000,
            max_units=2000,
            minimum_units=1,
            margin_rate=0.02,
            max_margin_fraction=1.0,
        )
        self.assertEqual(decision.units, 125)
        self.assertIn("fixed_units_capped", decision.reason)

    def test_non_usd_quote_risk_is_converted_to_account_currency(self):
        decision = calculate_sizing(
            balance=50.0,
            margin_available=50.0,
            quote_mid=0.8600,
            quote_to_account_rate=1.3000,
            stop_pips=10.0,
            risk_per_trade_pct=0.262,
            fixed_units=0,
            max_units=2000,
            minimum_units=1,
            margin_rate=0.0333,
            max_margin_fraction=1.0,
        )
        self.assertEqual(decision.risk_limited_units, 100)
        self.assertAlmostEqual(decision.actual_risk, 0.13)


class ReconciliationTests(unittest.TestCase):
    def test_extracts_exact_trade_close_from_fill_transaction(self):
        close = extract_trade_close(
            [
                {"id": "9", "type": "HEARTBEAT"},
                {
                    "id": "10",
                    "type": "ORDER_FILL",
                    "reason": "STOP_LOSS_ORDER",
                    "price": "1.14019",
                    "time": "2026-07-13T15:00:01Z",
                    "accountBalance": "57.3134",
                    "tradesClosed": [
                        {"tradeID": "9952", "realizedPL": "-0.1250"}
                    ],
                },
            ],
            "9952",
        )
        self.assertIsNotNone(close)
        self.assertEqual(close["reason"], "STOP_LOSS_ORDER")
        self.assertEqual(close["transaction_id"], "10")
        self.assertAlmostEqual(close["realized_pl"], -0.125)


class RequestSafetyTests(unittest.TestCase):
    def test_read_reloads_credentials_and_retries_after_401(self):
        bot = request_test_bot(
            [
                FakeResponse(401, {"errorMessage": "Insufficient authorization"}, "bad-auth"),
                FakeResponse(200, {"account": {"balance": "57.31"}}, "ok-auth"),
            ]
        )
        payload = bot.request("GET", f"/v3/accounts/{bot.account_id}/summary")
        self.assertEqual(payload["account"]["balance"], "57.31")
        self.assertEqual(len(bot.session.calls), 2)
        self.assertEqual(bot.reset_calls, [True])
        self.assertEqual(bot.log_events[0][0], "api_auth_retry")

    def test_order_network_failure_is_not_retried(self):
        bot = request_test_bot([requests.ConnectionError("response lost")])
        with self.assertRaises(OandaApiError) as captured:
            bot.request(
                "POST",
                f"/v3/accounts/{bot.account_id}/orders",
                body={"order": {"type": "MARKET"}},
            )
        self.assertTrue(captured.exception.outcome_uncertain)
        self.assertEqual(len(bot.session.calls), 1)
        self.assertEqual(bot.reset_calls, [])

    def test_order_http_500_is_not_retried(self):
        bot = request_test_bot([FakeResponse(500, {"errorMessage": "temporary"})])
        with self.assertRaises(OandaApiError) as captured:
            bot.request(
                "POST",
                f"/v3/accounts/{bot.account_id}/orders",
                body={"order": {"type": "MARKET"}},
            )
        self.assertTrue(captured.exception.outcome_uncertain)
        self.assertEqual(len(bot.session.calls), 1)


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.bot = PracticeScalper.__new__(PracticeScalper)
        self.bot.args = parse_args([])

    def test_momentum_buy_path(self):
        closes = [1.1000] * 40
        quote = quote_from_price_payload(
            {
                "time": "2026-07-13T15:00:00Z",
                "tradeable": True,
                "bids": [{"price": "1.10002"}],
                "asks": [{"price": "1.10004"}],
            },
            "test",
        )
        common = {
            "r1_pips": 0.5,
            "r3_pips": 5.0,
            "r5_pips": 5.0,
            "m5_r1_pips": 1.0,
            "m5_r3_pips": 2.0,
            "pos20": 0.8,
            "required_signal_pips": 4.8,
        }
        direction, meta = self.bot.evaluate_strategy(
            "momentum",
            quote,
            {"M1": candle_set(closes), "M5": candle_set(closes)},
            common,
        )
        self.assertEqual(direction, "buy")
        self.assertEqual(meta["strategy"], "momentum")

    def test_pullback_buy_path(self):
        m1_values = [1.1000] * 38 + [1.0998, 1.1002]
        m5_values = [1.0960 + index * 0.0001 for index in range(40)]
        quote = quote_from_price_payload(
            {
                "time": "2026-07-13T15:00:00Z",
                "tradeable": True,
                "bids": [{"price": "1.10022"}],
                "asks": [{"price": "1.10024"}],
            },
            "test",
        )
        common = {
            "r1_pips": 4.0,
            "r3_pips": 2.0,
            "r5_pips": 2.0,
            "m5_r1_pips": 1.0,
            "m5_r3_pips": 3.0,
            "pos20": 0.6,
            "required_signal_pips": 4.8,
        }
        direction, meta = self.bot.evaluate_strategy(
            "pullback",
            quote,
            {"M1": candle_set(m1_values), "M5": candle_set(m5_values)},
            common,
        )
        self.assertEqual(direction, "buy")
        self.assertIn("m1_ema9", meta)

    def test_multitimeframe_strategy_records_indicators(self):
        values = [1.1000 + math.sin(index / 3.0) * 0.0002 for index in range(50)]
        quote = quote_from_price_payload(
            {
                "time": "2026-07-13T15:00:00Z",
                "tradeable": True,
                "bids": [{"price": "1.10000"}],
                "asks": [{"price": "1.10002"}],
            },
            "test",
        )
        common = {
            "r1_pips": 0.0,
            "r3_pips": 0.0,
            "r5_pips": 0.0,
            "m5_r1_pips": 0.0,
            "m5_r3_pips": 0.0,
            "pos20": 0.5,
            "required_signal_pips": 4.8,
        }
        _, meta = self.bot.evaluate_strategy(
            "macd_rsi_reversal",
            quote,
            {
                "M1": candle_set(values),
                "M5": candle_set(values),
                "M10": candle_set(values),
                "M30": candle_set(values),
            },
            common,
        )
        self.assertIn("m1_rsi14", meta)
        self.assertIn("m10_rsi14", meta)
        self.assertIn("m1_macd_hist", meta)


class SchedulingTests(unittest.TestCase):
    def test_next_check_is_after_the_following_candle_closes(self):
        bot = PracticeScalper.__new__(PracticeScalper)
        bot.args = SimpleNamespace(candle_close_delay_sec=2.0, signal_check_fallback_sec=5.0)
        candle_time = "2026-07-13T15:48:00Z"
        candle_start = datetime(2026, 7, 13, 15, 48, tzinfo=timezone.utc).timestamp()
        with patch(
            "trad.oanda_practice_eurusd_micro_scalper.time.time",
            return_value=candle_start + 61.0,
        ):
            next_check = bot.next_candle_check_time(candle_time)
        self.assertEqual(next_check, candle_start + 122.0)

    def test_next_check_uses_oanda_clock_when_local_clock_is_behind(self):
        bot = PracticeScalper.__new__(PracticeScalper)
        bot.args = SimpleNamespace(candle_close_delay_sec=2.0, signal_check_fallback_sec=5.0)
        candle_time = "2026-07-13T15:48:00Z"
        candle_start = datetime(2026, 7, 13, 15, 48, tzinfo=timezone.utc).timestamp()
        local_now = candle_start + 41.0
        with patch(
            "trad.oanda_practice_eurusd_micro_scalper.time.time",
            return_value=local_now,
        ):
            quote = quote_from_price_payload(
                {
                    "time": "2026-07-13T15:49:01Z",
                    "tradeable": True,
                    "bids": [{"price": "1.10000"}],
                    "asks": [{"price": "1.10002"}],
                },
                "test",
            )
            next_check = bot.next_candle_check_time(candle_time, quote)
        self.assertAlmostEqual(next_check, candle_start + 102.0, delta=0.05)


class ArgumentTests(unittest.TestCase):
    def test_v9_defaults_are_bounded_and_r_based(self):
        args = parse_args([])
        self.assertEqual(args.instrument, "EUR_USD")
        self.assertEqual(args.max_units, 250)
        self.assertAlmostEqual(args.risk_per_trade_pct, 0.25)
        self.assertAlmostEqual(args.take_profit_r, 1.6)
        self.assertAlmostEqual(args.profit_lock_r, 0.75)
        self.assertEqual(args.strategy, "pullback")
        self.assertEqual(args.shadow_strategies, "momentum,macd_rsi_reversal")
        self.assertAlmostEqual(args.max_spread_pips, 1.8)
        self.assertAlmostEqual(args.max_spread_atr_fraction, 0.55)
        self.assertAlmostEqual(args.min_signal_to_spread, 2.5)
        self.assertAlmostEqual(args.max_signal_age_sec, 20.0)
        self.assertTrue(args.use_price_stream)
        self.assertTrue(math.isclose(args.stop_loss_pips, 5.0))

    def test_instrument_argument_is_normalized(self):
        args = parse_args(["--instrument", "gbp/usd"])
        self.assertEqual(args.instrument, "GBP_USD")

    def test_account_selection_arguments(self):
        args = parse_args(["--account-key", "OANDA_ACCOUNT_ID_VOL", "--account-id", "practice-account"])
        self.assertEqual(args.account_key, "OANDA_ACCOUNT_ID_VOL")
        self.assertEqual(args.account_id, "practice-account")


if __name__ == "__main__":
    unittest.main()
