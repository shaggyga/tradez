import unittest

try:
    from oanda_order_position_book_collector import permanently_unavailable, summarize_book
    from oanda_signal_combination_audit import build_signal_vector
except ModuleNotFoundError:
    from trad.oanda_order_position_book_collector import permanently_unavailable, summarize_book
    from trad.oanda_signal_combination_audit import build_signal_vector


class OrderPositionBookCollectorTests(unittest.TestCase):
    def test_only_permanent_invalid_instrument_errors_are_retired(self):
        self.assertTrue(
            permanently_unavailable(
                {
                    "_http_status": 400,
                    "_error_text": "EUR_NOK is not a valid instrument",
                }
            )
        )
        self.assertFalse(
            permanently_unavailable(
                {"_http_status": 503, "_error_text": "temporary service failure"}
            )
        )

    def test_summarizes_nearby_client_order_distribution(self):
        payload = {
            "orderBook": {
                "time": "2026-07-21T12:00:00Z",
                "price": "1.1000",
                "bucketWidth": "0.0005",
                "buckets": [
                    {"price": "1.0995", "longCountPercent": "4", "shortCountPercent": "1"},
                    {"price": "1.1000", "longCountPercent": "3", "shortCountPercent": "3"},
                    {"price": "1.1005", "longCountPercent": "1", "shortCountPercent": "5"},
                ],
            }
        }
        row = summarize_book("EUR_USD", "order", payload)

        self.assertEqual(row["order_book_bucket_count"], 3)
        self.assertAlmostEqual(row["order_book_near_5_imbalance"], -1.0 / 17.0)
        self.assertGreater(row["order_book_below_25_net"], 0.0)
        self.assertLess(row["order_book_above_25_net"], 0.0)

    def test_book_and_pricing_depth_stay_out_of_structural_combination_vector(self):
        closes = [1.1000 + index * 0.00001 for index in range(80)]
        features = {
            "closes": closes,
            "m5_closes": closes,
            "m15_closes": closes,
            "h1_closes": closes,
            "highs": [value + 0.00005 for value in closes],
            "lows": [value - 0.00005 for value in closes],
            "opens": closes,
            "volumes": [100.0] * len(closes),
            "pip": 0.0001,
            "m1_atr14_pips": 2.0,
            "m5_atr14_pips": 5.0,
            "depth_imbalance": 0.25,
            "depth_top_imbalance": 0.10,
            "depth_log_total_liquidity": 15.0,
            "microprice_offset_pips": 0.2,
            "order_book_near_10_imbalance": -0.3,
            "position_book_near_10_imbalance": 0.4,
            "order_book_available": 1.0,
            "position_book_available": 1.0,
        }
        vector = build_signal_vector(features)

        self.assertNotIn("depth_imbalance", vector)
        self.assertNotIn("order_book_near_10_imbalance", vector)
        self.assertNotIn("position_book_near_10_imbalance", vector)
        self.assertNotIn("order_book_available", vector)
        self.assertEqual(vector["volume_ratio_12"], 1.0)


if __name__ == "__main__":
    unittest.main()
