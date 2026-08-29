import unittest

import pandas as pd

from trad.oanda_strategy_lab_historical_backtest import (
    build_pair_history,
    parse_horizons,
    select_cycle_times,
    setup_outcomes,
)


def synthetic_frame(rows: int = 260) -> pd.DataFrame:
    index = pd.date_range("2026-07-01T00:00:00Z", periods=rows, freq="1min")
    close = [1.1000 + position * 0.00001 for position in range(rows)]
    frame = pd.DataFrame(index=index)
    frame["open"] = close
    frame["high"] = [value + 0.00005 for value in close]
    frame["low"] = [value - 0.00005 for value in close]
    frame["close"] = close
    frame["volume"] = 100
    frame["bid_open"] = [value - 0.0001 for value in close]
    frame["bid_close"] = [value - 0.0001 for value in close]
    frame["ask_open"] = [value + 0.0001 for value in close]
    frame["ask_close"] = [value + 0.0001 for value in close]
    return frame


class HistoricalBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.history = build_pair_history("EUR_USD", synthetic_frame())

    def test_feature_windows_end_before_next_minute_entry(self):
        decision = self.history.m1_times[220]
        windows = self.history.candle_windows(decision)
        assert windows is not None
        self.assertEqual(windows["M1"][-1]["time"], decision.isoformat().replace("+00:00", "Z"))
        self.assertEqual(
            windows["M5"][-1]["time"],
            (decision - pd.Timedelta(minutes=4)).floor("5min").isoformat().replace("+00:00", "Z"),
        )
        entry_position = self.history.entry_position(decision)
        assert entry_position is not None
        self.assertEqual(self.history.m1_times[entry_position], decision + pd.Timedelta(minutes=1))

    def test_horizon_uses_exact_future_close_and_pays_spread(self):
        decision = self.history.m1_times[220]
        entry_position = self.history.entry_position(decision)
        assert entry_position is not None
        outcomes = setup_outcomes(self.history, entry_position, "buy", [60, 180])
        self.assertAlmostEqual(outcomes[60]["theoretical_pips"], -2.0)
        self.assertAlmostEqual(outcomes[180]["theoretical_pips"], -1.8)

    def test_cycle_limit_selects_latest_cycles(self):
        cycles = select_cycle_times({"EUR_USD": self.history}, None, None, 5, 3)
        self.assertEqual(len(cycles), 3)
        self.assertEqual(cycles[-1], self.history.m1_times[-1].floor("5min"))

    def test_horizons_must_be_whole_minutes(self):
        self.assertEqual(parse_horizons("60,300,180"), [60, 180, 300])
        with self.assertRaises(Exception):
            parse_horizons("30")


if __name__ == "__main__":
    unittest.main()
