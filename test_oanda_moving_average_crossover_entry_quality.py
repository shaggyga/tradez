import unittest

import numpy as np
import pandas as pd

from oanda_moving_average_crossover_entry_quality import (
    entry_path_outcomes,
)
from oanda_moving_average_crossover_sweep import SignalEvents


def exact_frame(rows):
    index = pd.date_range("2026-01-01", periods=len(rows), freq="1min", tz="UTC")
    frame = pd.DataFrame(rows, index=index)
    frame["open"] = frame.get("open", 1.1000)
    frame["high"] = frame.get("high", frame["open"])
    frame["low"] = frame.get("low", frame["open"])
    frame["close"] = frame.get("close", frame["open"])
    frame["volume"] = 1
    frame["spread_pips"] = 2.0
    frame["bid_close"] = frame.get("bid_close", frame["bid_open"])
    frame["ask_close"] = frame.get("ask_close", frame["ask_open"])
    return frame


class EntryPathTests(unittest.TestCase):
    def test_long_target_before_stop_is_win(self):
        frame = exact_frame(
            [
                {
                    "bid_open": 1.0999,
                    "ask_open": 1.1001,
                    "bid_high": 1.1000,
                    "bid_low": 1.0999,
                    "ask_high": 1.1002,
                    "ask_low": 1.1000,
                },
                {
                    "bid_open": 1.1000,
                    "ask_open": 1.1002,
                    "bid_high": 1.1004,
                    "bid_low": 1.1000,
                    "ask_high": 1.1006,
                    "ask_low": 1.1002,
                },
                {
                    "bid_open": 1.1002,
                    "ask_open": 1.1004,
                    "bid_high": 1.1003,
                    "bid_low": 1.0997,
                    "ask_high": 1.1005,
                    "ask_low": 1.0999,
                },
                {
                    "bid_open": 1.1000,
                    "ask_open": 1.1002,
                    "bid_high": 1.1001,
                    "bid_low": 1.0999,
                    "ask_high": 1.1003,
                    "ask_low": 1.1001,
                },
            ]
        )
        batch = entry_path_outcomes(
            frame,
            SignalEvents(np.array([0]), np.array([1], dtype=np.int8)),
            [3],
            [2.5],
            0.0001,
            2.0,
        )
        self.assertTrue(batch.valid[0, 0])
        self.assertTrue(batch.first_touch_wins[0, 0, 0])
        self.assertFalse(batch.first_touch_losses[0, 0, 0])
        self.assertAlmostEqual(batch.mfe_pips[0, 0], 3.0)

    def test_same_bar_collision_is_conservative_loss(self):
        frame = exact_frame(
            [
                {
                    "bid_open": 1.0999,
                    "ask_open": 1.1001,
                    "bid_high": 1.1003,
                    "bid_low": 1.0999,
                    "ask_high": 1.1005,
                    "ask_low": 1.1001,
                },
                {
                    "bid_open": 1.1000,
                    "ask_open": 1.1002,
                    "bid_high": 1.1001,
                    "bid_low": 1.0998,
                    "ask_high": 1.1003,
                    "ask_low": 1.1000,
                },
            ]
        )
        batch = entry_path_outcomes(
            frame,
            SignalEvents(np.array([0]), np.array([1], dtype=np.int8)),
            [1],
            [2.0],
            0.0001,
            2.0,
        )
        self.assertFalse(batch.first_touch_wins[0, 0, 0])
        self.assertTrue(batch.first_touch_losses[0, 0, 0])

    def test_short_uses_bid_entry_and_ask_path(self):
        frame = exact_frame(
            [
                {
                    "bid_open": 1.1000,
                    "ask_open": 1.1002,
                    "bid_high": 1.1001,
                    "bid_low": 1.0999,
                    "ask_high": 1.1002,
                    "ask_low": 1.1001,
                },
                {
                    "bid_open": 1.0998,
                    "ask_open": 1.1000,
                    "bid_high": 1.0999,
                    "bid_low": 1.0995,
                    "ask_high": 1.1001,
                    "ask_low": 1.0997,
                },
                {
                    "bid_open": 1.0999,
                    "ask_open": 1.1001,
                    "bid_high": 1.1000,
                    "bid_low": 1.0998,
                    "ask_high": 1.1002,
                    "ask_low": 1.1000,
                },
            ]
        )
        batch = entry_path_outcomes(
            frame,
            SignalEvents(np.array([0]), np.array([-1], dtype=np.int8)),
            [2],
            [2.5],
            0.0001,
            2.0,
        )
        self.assertTrue(batch.first_touch_wins[0, 0, 0])
        self.assertAlmostEqual(batch.mfe_pips[0, 0], 3.0)

    def test_proxy_prices_charge_spread(self):
        index = pd.date_range("2026-01-01", periods=2, freq="1min", tz="UTC")
        frame = pd.DataFrame(
            {
                "open": [1.1000, 1.1000],
                "high": [1.1002, 1.1004],
                "low": [1.0998, 1.0999],
                "close": [1.1000, 1.1002],
                "volume": [1, 1],
                "spread_pips": [2.0, 2.0],
                "bid_open": [np.nan, np.nan],
                "bid_high": [np.nan, np.nan],
                "bid_low": [np.nan, np.nan],
                "bid_close": [np.nan, np.nan],
                "ask_open": [np.nan, np.nan],
                "ask_high": [np.nan, np.nan],
                "ask_low": [np.nan, np.nan],
                "ask_close": [np.nan, np.nan],
            },
            index=index,
        )
        batch = entry_path_outcomes(
            frame,
            SignalEvents(np.array([0]), np.array([1], dtype=np.int8)),
            [1],
            [1.0],
            0.0001,
            2.0,
        )
        self.assertAlmostEqual(batch.mfe_pips[0, 0], 0.0)
        self.assertTrue(batch.first_touch_losses[0, 0, 0])


if __name__ == "__main__":
    unittest.main()
