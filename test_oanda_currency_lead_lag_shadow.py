import unittest

import numpy as np

try:
    import oanda_currency_lead_lag_shadow as shadow
except ModuleNotFoundError:
    from trad import oanda_currency_lead_lag_shadow as shadow


class CurrencyLeadLagShadowTests(unittest.TestCase):
    def test_latent_currency_returns_recover_relative_move(self) -> None:
        epochs = np.asarray([0.0, 60.0, 120.0])
        sets = {}
        changes = {"EUR_USD": 0.0010, "GBP_USD": 0.0005, "EUR_GBP": 0.0005,
                   "AUD_USD": -0.0002, "EUR_AUD": 0.0012, "GBP_AUD": 0.0007}
        for pair, change in changes.items():
            mid = np.asarray([1.0, 1.0 + change, (1.0 + change) ** 2])
            sets[pair] = {"epoch": epochs, "mid": mid, "bid": mid - 0.00005, "ask": mid + 0.00005}
        currencies, values = shadow.latent_currency_returns(sets, max_input_spread_pips=3.0)
        self.assertIn(60, values)
        index = {currency: i for i, currency in enumerate(currencies)}
        self.assertGreater(values[60][index["EUR"]], values[60][index["USD"]])

    def test_network_features_are_strictly_lagged(self) -> None:
        latent = {60: np.asarray([1.0, 2.0]), 360: np.asarray([3.0, 4.0]),
                  480: np.asarray([5.0, 6.0]), 540: np.asarray([7.0, 8.0]),
                  600: np.asarray([9.0, 10.0])}
        result = shadow.network_features(600, ["EUR", "USD"], latent)
        self.assertIsNotNone(result)
        np.testing.assert_allclose(result[:2], [9.0, 10.0])

    def test_rows_pay_round_trip_spread(self) -> None:
        n = 300
        epochs = np.arange(n, dtype=float) * 60.0
        mid = 1.0 + np.arange(n, dtype=float) * 0.00001
        candles = {"epoch": epochs, "mid": mid, "bid": mid - 0.00005, "ask": mid + 0.00005}
        latent = {int(epoch): np.asarray([0.0, 0.0]) for epoch in epochs}
        rows = shadow.build_pair_rows("EUR_USD", candles, ["EUR", "USD"], latent, 5)
        self.assertGreater(len(rows["long_net"]), 0)
        self.assertAlmostEqual(float(rows["long_net"][0]), -0.5, places=6)


if __name__ == "__main__":
    unittest.main()
