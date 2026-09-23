import unittest

try:
    from oanda_intrahour_forecast_contract import (
        FORECAST_CONTEXT_TIMEFRAMES,
        FORECAST_HORIZONS_SEC,
        filter_context_views,
        flatten_context_views,
        is_forecast_cell,
    )
except ModuleNotFoundError:
    from trad.oanda_intrahour_forecast_contract import (
        FORECAST_CONTEXT_TIMEFRAMES,
        FORECAST_HORIZONS_SEC,
        filter_context_views,
        flatten_context_views,
        is_forecast_cell,
    )


class IntrahourForecastContractTests(unittest.TestCase):
    def test_contract_is_four_context_timeframes_and_next_hour_only(self):
        self.assertEqual(FORECAST_CONTEXT_TIMEFRAMES, ("M1", "M30", "H1", "H4"))
        self.assertEqual(FORECAST_HORIZONS_SEC[-1], 3600)
        self.assertTrue(is_forecast_cell("M1", 60))
        self.assertTrue(is_forecast_cell("H4", 3600))
        self.assertFalse(is_forecast_cell("S5", 60))
        self.assertFalse(is_forecast_cell("M1", 7200))

    def test_structural_views_keep_tick_volume_and_remove_live_depth_books(self):
        views = {
            "S5": {"current_volume": 3.0},
            "M1": {
                "current_volume": 80.0,
                "volume_ratio_12": 1.2,
                "depth_imbalance": 0.4,
                "order_book_near_10_imbalance": -0.2,
            },
            "M30": {"current_volume": 1500.0},
            "H1": {"current_volume": 3000.0},
            "H4": {"current_volume": 12000.0},
            "D1": {"current_volume": 50000.0},
        }

        filtered = filter_context_views(views)
        flattened = flatten_context_views(views)

        self.assertEqual(set(filtered), {"M1", "M30", "H1", "H4"})
        self.assertEqual(filtered["M1"]["current_volume"], 80.0)
        self.assertNotIn("depth_imbalance", filtered["M1"])
        self.assertNotIn("order_book_near_10_imbalance", filtered["M1"])
        self.assertEqual(flattened["m1__volume_ratio_12"], 1.2)
        self.assertNotIn("m1__depth_imbalance", flattened)


if __name__ == "__main__":
    unittest.main()
