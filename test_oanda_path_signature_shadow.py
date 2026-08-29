import unittest

import numpy as np

try:
    import oanda_path_signature_shadow as shadow
except ModuleNotFoundError:
    from trad import oanda_path_signature_shadow as shadow


class PathSignatureShadowTests(unittest.TestCase):
    def test_level_two_signature_straight_line(self) -> None:
        path = np.asarray([[0.0, 0.0], [0.5, 1.0], [1.0, 2.0]])
        result = shadow.level_two_signature(path)
        np.testing.assert_allclose(result[:2], [1.0, 2.0])
        np.testing.assert_allclose(result[2:].reshape(2, 2), [[0.5, 1.0], [1.0, 2.0]])

    def test_order_changes_antisymmetric_area(self) -> None:
        first = np.asarray([[0.0, 0.0], [1.0, 0.0], [1.0, 1.0]])
        second = np.asarray([[0.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
        sig_first = shadow.level_two_signature(first)[2:].reshape(2, 2)
        sig_second = shadow.level_two_signature(second)[2:].reshape(2, 2)
        self.assertGreater(sig_first[0, 1] - sig_first[1, 0], 0.0)
        self.assertLess(sig_second[0, 1] - sig_second[1, 0], 0.0)

    def test_bid_ask_outcomes_include_round_trip_cost(self) -> None:
        epochs = np.arange(200, dtype=float) * 60.0
        mid = 1.0 + np.arange(200, dtype=float) * 0.00001
        candles = {
            "epoch": epochs,
            "bid": mid - 0.00005,
            "ask": mid + 0.00005,
            "mid": mid,
        }
        rows = shadow.build_rows("EUR_USD", candles, 5)
        self.assertTrue(rows["long_net"])
        self.assertAlmostEqual(rows["long_net"][0], -0.5, places=6)
        self.assertAlmostEqual(rows["short_net"][0], -1.5, places=6)


if __name__ == "__main__":
    unittest.main()
