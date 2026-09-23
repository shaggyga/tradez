import unittest

import numpy as np
import pandas as pd

from oanda_distinct_strategy_backtest import (
    derive_distinct_thresholds,
    london_breakout_direction,
    rolling_sweep_direction,
    strategy_direction,
)


class DistinctStrategyBacktestTests(unittest.TestCase):
    def sample(self, periods: int = 80) -> pd.DataFrame:
        index = pd.date_range("2024-01-02", periods=periods, freq="5min", tz="UTC")
        values = np.zeros(periods)
        return pd.DataFrame(
            {
                "high": 1.1002 + values,
                "low": 1.0998 + values,
                "close": 1.1000 + values,
                "spread_pips": np.full(periods, 1.0),
                "momentum_5_atr": values.copy(),
                "momentum_15_atr": values.copy(),
                "momentum_60_atr": values.copy(),
                "sma7_minus_8_atr": values.copy(),
                "sma30_slope_5_atr": values.copy(),
                "atr15_to_atr240": np.full(periods, 1.0),
                "compression_30": np.full(periods, 1.0),
                "range_position_60_centered": values.copy(),
                "spread_ratio_60": np.full(periods, 0.8),
                "volume_z_30": np.full(periods, 0.5),
                "atr240_pips": np.full(periods, 10.0),
                "strength_gap_rank_15": values.copy(),
                "strength_gap_rank_60": values.copy(),
            },
            index=index,
        )

    def test_confirmed_pullback_requires_resume_and_trend_stack(self):
        frame = self.sample()
        index = -1
        frame.iloc[index, frame.columns.get_loc("momentum_60_atr")] = 2.0
        frame.iloc[index, frame.columns.get_loc("momentum_15_atr")] = -2.0
        frame.iloc[index, frame.columns.get_loc("momentum_5_atr")] = 2.0
        frame.iloc[index, frame.columns.get_loc("sma7_minus_8_atr")] = 1.0
        frame.iloc[index, frame.columns.get_loc("sma30_slope_5_atr")] = 1.0
        thresholds = derive_distinct_thresholds(frame)
        direction = strategy_direction(frame, "confirmed_pullback_resume", thresholds, "EUR_USD")
        self.assertEqual(int(direction[-1]), 1)
        frame.iloc[index, frame.columns.get_loc("sma30_slope_5_atr")] = -1.0
        direction = strategy_direction(frame, "confirmed_pullback_resume", thresholds, "EUR_USD")
        self.assertEqual(int(direction[-1]), 0)

    def test_sweep_requires_excursion_and_close_back_inside(self):
        frame = self.sample()
        prior_high = frame["high"].iloc[-2]
        frame.iloc[-1, frame.columns.get_loc("high")] = prior_high + 0.0002
        frame.iloc[-1, frame.columns.get_loc("close")] = prior_high - 0.0001
        direction = rolling_sweep_direction(frame, "EUR_USD")
        self.assertEqual(int(direction[-1]), -1)
        frame.iloc[-1, frame.columns.get_loc("close")] = prior_high + 0.0001
        direction = rolling_sweep_direction(frame, "EUR_USD")
        self.assertEqual(int(direction[-1]), 0)

    def test_london_breakout_uses_completed_local_range(self):
        index = pd.date_range("2024-01-02 00:00", "2024-01-02 09:00", freq="5min", tz="Europe/London").tz_convert("UTC")
        frame = self.sample(len(index))
        frame.index = index
        london_nine = np.flatnonzero((index.tz_convert("Europe/London").hour == 9) & (index.minute == 0))[0]
        frame.iloc[london_nine, frame.columns.get_loc("close")] = 1.1010
        frame.iloc[london_nine, frame.columns.get_loc("high")] = 1.1011
        direction = london_breakout_direction(frame, "EUR_USD")
        self.assertEqual(int(direction[london_nine]), 1)

    def test_expansion_gate_uses_strength_not_price_direction(self):
        frame = self.sample()
        frame.iloc[-1, frame.columns.get_loc("atr15_to_atr240")] = 3.0
        frame.iloc[-1, frame.columns.get_loc("momentum_15_atr")] = -2.0
        frame.iloc[-1, frame.columns.get_loc("strength_gap_rank_15")] = 2.0
        thresholds = derive_distinct_thresholds(frame)
        direction = strategy_direction(frame, "expansion_strength_direction", thresholds, "EUR_USD")
        self.assertEqual(int(direction[-1]), 1)

    def test_compression_is_a_prior_setup_not_same_bar_requirement(self):
        frame = self.sample()
        frame.iloc[-4, frame.columns.get_loc("compression_30")] = 0.01
        frame.iloc[-1, frame.columns.get_loc("compression_30")] = 2.0
        frame.iloc[-1, frame.columns.get_loc("range_position_60_centered")] = 2.0
        frame.iloc[-1, frame.columns.get_loc("momentum_15_atr")] = 2.0
        thresholds = derive_distinct_thresholds(frame.iloc[:-1])
        direction = strategy_direction(frame, "compression_confirmed_breakout", thresholds, "EUR_USD")
        self.assertEqual(int(direction[-1]), 1)


if __name__ == "__main__":
    unittest.main()
