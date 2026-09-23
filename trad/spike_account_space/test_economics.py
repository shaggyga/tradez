import math
import unittest

from trad.spike_account_space.economics import (
    AccountState,
    ConversionUnavailableError,
    CostHooks,
    EconomicsError,
    InstrumentMeta,
    InvalidQuoteError,
    MarketQuote,
    MissingMetadataError,
    build_instrument_metadata,
    calculate_margin_state,
    commission_per_million,
    convert_currency,
    entry_fill,
    exit_fill,
    margin_closeout_percent,
    margin_required,
    market_fill,
    pip_value_in_account,
    pnl_conversion_charge_bps,
    position_notional_in_account,
    resolve_conversion,
    round_trip_pnl,
    size_position,
    spread_pips,
)


def meta(
    name="EUR_USD",
    pip_location=-4,
    margin_rate=0.02,
    minimum_trade_size=1,
    maximum_order_units=None,
):
    return InstrumentMeta(
        name=name,
        pip_location=pip_location,
        margin_rate=margin_rate,
        minimum_trade_size=minimum_trade_size,
        maximum_order_units=maximum_order_units,
    )


class InstrumentMetadataTests(unittest.TestCase):
    def test_pip_location_is_explicit_and_not_a_jpy_heuristic(self):
        usd_jpy = InstrumentMeta.from_oanda(
            {
                "name": "USD_JPY",
                "type": "CURRENCY",
                "pipLocation": -2,
                "marginRate": "0.03",
            }
        )
        hkd_jpy = InstrumentMeta.from_oanda(
            {
                "name": "HKD_JPY",
                "type": "CURRENCY",
                "pipLocation": -4,
                "marginRate": "0.05",
            }
        )
        self.assertEqual(usd_jpy.pip_size, 0.01)
        self.assertEqual(hkd_jpy.pip_size, 0.0001)

    def test_missing_pip_location_or_margin_rate_is_rejected(self):
        with self.assertRaises(MissingMetadataError):
            InstrumentMeta.from_oanda({"name": "EUR_USD", "marginRate": "0.02"})
        with self.assertRaises(MissingMetadataError):
            InstrumentMeta.from_oanda({"name": "EUR_USD", "pipLocation": -4})

    def test_metadata_builder_filters_non_currency_and_rejects_duplicates(self):
        rows = [
            {
                "name": "EUR_USD",
                "type": "CURRENCY",
                "pipLocation": -4,
                "marginRate": "0.02",
            },
            {
                "name": "XAU_USD",
                "type": "METAL",
                "pipLocation": -2,
                "marginRate": "0.05",
            },
        ]
        self.assertEqual(set(build_instrument_metadata(rows)), {"EUR_USD"})
        with self.assertRaises(MissingMetadataError):
            build_instrument_metadata([rows[0], rows[0]])


class QuoteAndFillTests(unittest.TestCase):
    def test_oanda_and_flat_quote_shapes(self):
        oanda = MarketQuote.from_mapping(
            {"bids": [{"price": "1.1000"}], "asks": [{"price": "1.1002"}]}
        )
        flat = MarketQuote.from_mapping({"bid_c": 1.1, "ask_c": 1.1002})
        self.assertEqual(oanda, flat)
        self.assertAlmostEqual(oanda.mid, 1.1001)
        with self.assertRaises(InvalidQuoteError):
            MarketQuote(1.2, 1.1)

    def test_native_bid_ask_entry_exit_and_adverse_slippage(self):
        quote = MarketQuote(1.1000, 1.1002)
        self.assertEqual(entry_fill(quote, "long"), 1.1002)
        self.assertEqual(exit_fill(quote, "long"), 1.1000)
        self.assertEqual(entry_fill(quote, "short"), 1.1000)
        self.assertEqual(exit_fill(quote, "short"), 1.1002)
        self.assertAlmostEqual(
            market_fill(quote, 100, adverse_slippage_price=0.0001), 1.1003
        )
        self.assertAlmostEqual(
            market_fill(quote, -100, adverse_slippage_price=0.0001), 1.0999
        )
        self.assertAlmostEqual(spread_pips(quote, meta()), 2.0)


class ConversionTests(unittest.TestCase):
    def test_direct_conversion_distinguishes_asset_and_liability(self):
        quotes = {"EUR_USD": MarketQuote(1.10, 1.11)}
        asset = resolve_conversion(100, "EUR", "USD", quotes)
        liability = resolve_conversion(-100, "EUR", "USD", quotes)
        self.assertAlmostEqual(asset.target_amount, 110.0)
        self.assertAlmostEqual(liability.target_amount, -111.0)
        self.assertEqual(asset.instrument_path, ("EUR_USD",))

    def test_inverse_conversion_uses_ask_for_asset_and_bid_for_liability(self):
        quotes = {"USD_JPY": MarketQuote(150.00, 150.02)}
        positive_jpy = convert_currency(150.02, "JPY", "USD", quotes)
        negative_jpy = convert_currency(-150.00, "JPY", "USD", quotes)
        self.assertAlmostEqual(positive_jpy, 1.0)
        self.assertAlmostEqual(negative_jpy, -1.0)

    def test_multi_hop_conversion_is_auditable(self):
        quotes = {
            "EUR_USD": MarketQuote(1.10, 1.11),
            "USD_JPY": MarketQuote(150.00, 150.02),
        }
        result = resolve_conversion(2, "EUR", "JPY", quotes)
        self.assertAlmostEqual(result.target_amount, 2 * 1.10 * 150.00)
        self.assertEqual(result.currency_path, ("EUR", "USD", "JPY"))
        self.assertEqual(result.instrument_path, ("EUR_USD", "USD_JPY"))

    def test_same_currency_is_one_but_missing_cross_never_is(self):
        same = resolve_conversion(7, "USD", "USD", {})
        self.assertEqual(same.rate, 1.0)
        self.assertEqual(same.target_amount, 7.0)
        with self.assertRaises(ConversionUnavailableError):
            resolve_conversion(7, "NOK", "USD", {"EUR_USD": MarketQuote(1.1, 1.2)})

    def test_pip_value_uses_quote_currency_conversion_side(self):
        eur_usd = meta()
        self.assertAlmostEqual(pip_value_in_account(eur_usd, "USD", {}, units=10_000), 1.0)
        usd_jpy = meta("USD_JPY", pip_location=-2, margin_rate=0.03)
        quotes = {"USD_JPY": MarketQuote(150.00, 150.02)}
        profit_value = pip_value_in_account(usd_jpy, "USD", quotes, units=1)
        loss_value = pip_value_in_account(
            usd_jpy, "USD", quotes, units=1, liability=True
        )
        self.assertAlmostEqual(profit_value, 0.01 / 150.02)
        self.assertAlmostEqual(loss_value, 0.01 / 150.00)
        self.assertGreater(loss_value, profit_value)


class MarginAndSizingTests(unittest.TestCase):
    def setUp(self):
        self.meta = meta()
        self.quotes = {"EUR_USD": MarketQuote(1.0998, 1.1002)}
        self.account = AccountState("USD", balance=10_000, nav=10_000)

    def test_notional_margin_and_mco_formula(self):
        notional = position_notional_in_account(
            self.meta, 100_000, "USD", self.quotes
        )
        self.assertAlmostEqual(notional, 110_000)
        self.assertAlmostEqual(
            margin_required(self.meta, 100_000, "USD", self.quotes), 2_200
        )
        self.assertAlmostEqual(margin_closeout_percent(10_000, 2_000), 0.1)
        state = calculate_margin_state(1_000, 2_000)
        self.assertEqual(state.margin_available, -1_000)
        self.assertEqual(state.margin_closeout_percent, 1.0)
        self.assertTrue(state.in_margin_closeout)
        self.assertTrue(math.isinf(margin_closeout_percent(-1, 10)))

    def test_risk_sizing_returns_signed_integer_units(self):
        sized = size_position(
            self.meta,
            self.account,
            self.quotes,
            side="long",
            entry_price=1.1002,
            stop_price=1.0902,
            risk_fraction=0.01,
            max_margin_fraction=0.50,
        )
        self.assertTrue(sized.accepted)
        self.assertIsInstance(sized.units, int)
        self.assertEqual(sized.units, 10_000)
        self.assertAlmostEqual(sized.expected_stop_loss_account, 100.0)
        self.assertAlmostEqual(sized.margin_required_account, 220.0)
        self.assertEqual(sized.limiting_factor, "risk")

        short = size_position(
            self.meta,
            self.account,
            self.quotes,
            side="short",
            entry_price=1.0998,
            stop_price=1.1098,
            risk_amount_account=100,
            max_margin_fraction=0.50,
        )
        self.assertEqual(short.units, -10_000)

    def test_margin_and_max_order_caps_are_applied_before_integer_floor(self):
        margin_limited = size_position(
            self.meta,
            self.account,
            self.quotes,
            side="long",
            entry_price=1.1002,
            stop_price=1.0992,
            risk_amount_account=1_000,
            max_margin_fraction=0.01,
        )
        self.assertEqual(margin_limited.units, 4_545)
        self.assertEqual(margin_limited.limiting_factor, "margin")

        capped_meta = meta(maximum_order_units=1234)
        order_limited = size_position(
            capped_meta,
            self.account,
            self.quotes,
            side="long",
            entry_price=1.1002,
            stop_price=1.0992,
            risk_amount_account=1_000,
        )
        self.assertEqual(order_limited.units, 1234)
        self.assertEqual(order_limited.limiting_factor, "maximum_order_units")

    def test_no_unsafe_minimum_size_fallback(self):
        large_minimum = meta(minimum_trade_size=100)
        sized = size_position(
            large_minimum,
            self.account,
            self.quotes,
            side="long",
            entry_price=1.1002,
            stop_price=1.0902,
            risk_amount_account=0.50,
        )
        self.assertFalse(sized.accepted)
        self.assertEqual(sized.units, 0)
        self.assertIn("below integer broker minimum", sized.rejection_reason)

    def test_invalid_stop_side_and_ambiguous_risk_budget_are_rejected(self):
        with self.assertRaises(EconomicsError):
            size_position(
                self.meta,
                self.account,
                self.quotes,
                side="long",
                entry_price=1.1,
                stop_price=1.2,
                risk_fraction=0.01,
            )
        with self.assertRaises(EconomicsError):
            size_position(
                self.meta,
                self.account,
                self.quotes,
                side="long",
                entry_price=1.1,
                stop_price=1.0,
                risk_fraction=0.01,
                risk_amount_account=100,
            )


class CostHookAndRoundTripTests(unittest.TestCase):
    def test_bid_ask_slippage_and_all_cost_hooks_reconcile(self):
        instrument = meta()
        result = round_trip_pnl(
            instrument,
            100_000,
            MarketQuote(1.1000, 1.1002),
            MarketQuote(1.1010, 1.1012),
            "USD",
            {},
            entry_slippage_pips=1,
            exit_slippage_pips=1,
            holding_days=1,
            cost_hooks=CostHooks(
                commission=lambda context: 10,
                conversion_charge=lambda context: 2,
                financing=lambda context: -3,
            ),
        )
        self.assertAlmostEqual(result.entry_fill, 1.1003)
        self.assertAlmostEqual(result.exit_fill, 1.1009)
        self.assertAlmostEqual(result.ideal_mid_pnl_quote, 100.0)
        self.assertAlmostEqual(result.spread_cost_quote, 20.0)
        self.assertAlmostEqual(result.slippage_cost_quote, 20.0)
        self.assertAlmostEqual(result.gross_pnl_quote, 60.0)
        self.assertAlmostEqual(result.net_pnl_account, 45.0)

    def test_non_usd_pnl_is_converted_at_executable_side(self):
        instrument = meta("USD_JPY", pip_location=-2, margin_rate=0.03)
        exit_book = MarketQuote(150.10, 150.12)
        result = round_trip_pnl(
            instrument,
            1_000,
            MarketQuote(150.00, 150.02),
            exit_book,
            "USD",
            {"USD_JPY": exit_book},
        )
        self.assertAlmostEqual(result.gross_pnl_quote, 80.0)
        self.assertAlmostEqual(result.gross_pnl_account, 80.0 / 150.12)

    def test_cost_factories_and_hook_validation(self):
        instrument = meta()
        result = round_trip_pnl(
            instrument,
            100_000,
            MarketQuote(1.1000, 1.1002),
            MarketQuote(1.1010, 1.1012),
            "USD",
            {"EUR_USD": MarketQuote(1.1010, 1.1012)},
            cost_hooks=CostHooks(
                commission=commission_per_million(50),
                conversion_charge=pnl_conversion_charge_bps(10),
            ),
        )
        # $110,110 entry notional at supplied conversion mid, twice, at $50/mm.
        self.assertAlmostEqual(result.commission_account, 11.011)
        # Quote currency is already USD, so no conversion charge.
        self.assertEqual(result.conversion_charge_account, 0.0)

        with self.assertRaises(EconomicsError):
            round_trip_pnl(
                instrument,
                100,
                MarketQuote(1.1, 1.1002),
                MarketQuote(1.101, 1.1012),
                "USD",
                {},
                cost_hooks=CostHooks(commission=lambda context: -1),
            )


if __name__ == "__main__":
    unittest.main()
