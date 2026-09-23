from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from oanda_moving_average_crossover_sweep import (
    SignalEvents,
    completed_higher_tf_alignment,
    detect_crosses,
    fixed_horizon_outcomes,
    infer_pip_size,
    map_signal_entries,
    parse_window_pairs,
    split_for_entries,
)


class MovingAverageCrossoverSweepTests(unittest.TestCase):
    def test_detect_crosses_finds_both_directions(self) -> None:
        fast = np.array([0.0, 0.5, 2.0, 0.5, -1.0, 2.0])
        slow = np.ones(6)
        positions, directions = detect_crosses(fast, slow)
        np.testing.assert_array_equal(positions, np.array([2, 3, 5]))
        np.testing.assert_array_equal(directions, np.array([1, -1, 1], dtype=np.int8))

    def test_completed_m5_signal_enters_at_next_m1_open(self) -> None:
        m1_index = pd.date_range("2026-01-05T10:00:00Z", periods=20, freq="1min")
        m5_index = pd.date_range("2026-01-05T10:00:00Z", periods=3, freq="5min")
        events = map_signal_entries(
            m5_index,
            np.array([1]),
            np.array([1], dtype=np.int8),
            timeframe_minutes=5,
            m1_index=m1_index,
        )
        self.assertEqual(events.entry_positions.tolist(), [10])
        self.assertEqual(m1_index[events.entry_positions[0]], pd.Timestamp("2026-01-05T10:10:00Z"))

    def test_higher_timeframe_confirmation_uses_only_completed_bar(self) -> None:
        higher_index = pd.date_range("2026-01-05T10:00:00Z", periods=2, freq="5min")
        decision_ns = pd.DatetimeIndex(
            [
                pd.Timestamp("2026-01-05T10:04:00Z"),
                pd.Timestamp("2026-01-05T10:05:00Z"),
                pd.Timestamp("2026-01-05T10:10:00Z"),
            ]
        ).asi8
        aligned = completed_higher_tf_alignment(
            decision_ns,
            np.ones(3, dtype=np.int8),
            higher_index,
            higher_minutes=5,
            higher_fast=np.array([2.0, 0.0]),
            higher_slow=np.array([1.0, 1.0]),
        )
        np.testing.assert_array_equal(aligned, np.array([False, True, False]))

    def test_exact_bid_ask_outcome_pays_recorded_spread(self) -> None:
        index = pd.date_range("2026-01-05T10:00:00Z", periods=5, freq="1min")
        frame = pd.DataFrame(
            {
                "open": [1.0000, 1.0000, 1.0002, 1.0003, 1.0004],
                "close": [1.0000, 1.0001, 1.0002, 1.0003, 1.0005],
                "bid_open": [0.9999, 0.9999, 1.0001, 1.0002, 1.0003],
                "ask_open": [1.0001, 1.0001, 1.0003, 1.0004, 1.0005],
                "bid_close": [0.9999, 1.0000, 1.0001, 1.0002, 1.0004],
                "ask_close": [1.0001, 1.0002, 1.0003, 1.0004, 1.0006],
            },
            index=index,
        )
        events = SignalEvents(
            entry_positions=np.array([1]),
            directions=np.array([1], dtype=np.int8),
        )
        outcomes = fixed_horizon_outcomes(
            frame,
            events,
            horizon_minutes=2,
            pip_size=0.0001,
            proxy_spread_pips=99.0,
        )
        self.assertAlmostEqual(outcomes.gross_pips[0], 3.0)
        self.assertAlmostEqual(outcomes.net_pips[0], 1.0)
        self.assertTrue(outcomes.exact_cost[0])

    def test_proxy_cost_is_used_when_bid_ask_is_missing(self) -> None:
        index = pd.date_range("2026-01-05T10:00:00Z", periods=5, freq="1min")
        frame = pd.DataFrame(
            {
                "open": [1.0000, 1.0000, 1.0002, 1.0003, 1.0004],
                "close": [1.0000, 1.0001, 1.0002, 1.0003, 1.0005],
                "bid_open": np.nan,
                "ask_open": np.nan,
                "bid_close": np.nan,
                "ask_close": np.nan,
            },
            index=index,
        )
        events = SignalEvents(
            entry_positions=np.array([1]),
            directions=np.array([1], dtype=np.int8),
        )
        outcomes = fixed_horizon_outcomes(
            frame,
            events,
            horizon_minutes=2,
            pip_size=0.0001,
            proxy_spread_pips=1.5,
        )
        self.assertAlmostEqual(outcomes.gross_pips[0], 3.0)
        self.assertAlmostEqual(outcomes.net_pips[0], 1.5)
        self.assertFalse(outcomes.exact_cost[0])

    def test_split_is_chronological(self) -> None:
        result = split_for_entries(
            np.array([0, 59, 60, 79, 80, 99]),
            total_rows=100,
            development_fraction=0.60,
            validation_fraction=0.20,
        )
        np.testing.assert_array_equal(result, np.array([0, 0, 1, 1, 2, 2], dtype=np.int8))

    def test_custom_window_pairs_are_validated_and_deduplicated(self) -> None:
        self.assertEqual(parse_window_pairs("8:21,5-13,8/21"), [(5, 13), (8, 21)])
        with self.assertRaises(Exception):
            parse_window_pairs("21:8")

    def test_pip_size_uses_recorded_venue_spread_for_non_jpy_point_zero_one(self) -> None:
        frame = pd.DataFrame(
            {
                "spread_pips": np.full(12, 1.9),
                "bid_open": np.full(12, 33.01),
                "ask_open": np.full(12, 33.029),
            }
        )
        self.assertEqual(infer_pip_size("USD_THB", frame), 0.01)
        self.assertEqual(infer_pip_size("EUR_USD", None), 0.0001)

    def test_fade_direction_mode_is_explicit(self) -> None:
        from oanda_moving_average_crossover_sweep import parse_args

        self.assertEqual(parse_args(["--direction-mode", "fade"]).direction_mode, "fade")


if __name__ == "__main__":
    unittest.main()
