import argparse
import unittest

from trad.oanda_timeframe_horizon_sweep import (
    DEFAULT_HORIZONS,
    DEFAULT_TIMEFRAMES,
    build_runs,
)


class TimeframeHorizonSweepTests(unittest.TestCase):
    def setUp(self):
        self.args = argparse.Namespace(
            horizons=list(DEFAULT_HORIZONS),
            cycles=500,
            hourly_cycles=400,
            long_cycles=300,
            minimum_step_seconds=300,
            warmup_rows=380,
            s5_supplement_max_timeframe_sec=14400,
        )

    def test_runs_cover_full_matrix_and_s5_30_second_supplements(self):
        runs = build_runs(["S5", "M1", "M15", "H4"], self.args)
        by_key = {(run.timeframe, run.source, run.supplemental): run for run in runs}

        self.assertEqual(by_key[("S5", "s5", False)].horizons, DEFAULT_HORIZONS)
        self.assertNotIn(30, by_key[("M1", "m1", False)].horizons)
        self.assertEqual(by_key[("M1", "s5", True)].horizons, (30,))
        self.assertEqual(by_key[("M15", "s5", True)].horizons, (30,))
        self.assertEqual(by_key[("H4", "s5", True)].horizons, (30,))
        self.assertIn(120, by_key[("H4", "m1", False)].horizons)

    def test_low_timeframes_are_spread_over_a_longer_historical_window(self):
        run = build_runs(["S5"], self.args)[0]

        self.assertEqual(run.step_seconds, 300)
        self.assertGreater(run.tail_rows, 30_000)

    def test_default_run_plan_covers_every_timeframe_horizon_cell(self):
        runs = build_runs(list(DEFAULT_TIMEFRAMES), self.args)
        planned = {
            (run.timeframe, horizon)
            for run in runs
            for horizon in run.horizons
        }
        expected = {
            (timeframe, horizon)
            for timeframe in DEFAULT_TIMEFRAMES
            for horizon in DEFAULT_HORIZONS
        }

        self.assertEqual(planned, expected)
        self.assertEqual(
            len(planned),
            len(DEFAULT_TIMEFRAMES) * len(DEFAULT_HORIZONS),
        )


if __name__ == "__main__":
    unittest.main()
