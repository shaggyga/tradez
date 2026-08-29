import unittest
from datetime import date, timedelta

from trad.oanda_ahl_multihorizon_trend_backtest import (
    DailyBar,
    backtest_pair,
    fx_session_date_from_utc_text,
)


class AhlMultihorizonBacktestTests(unittest.TestCase):
    def test_new_york_session_boundary_respects_dst(self):
        session_cache = {}
        boundary_cache = {}
        self.assertEqual(
            fx_session_date_from_utc_text(
                "2026-07-30T20:59:00+00:00", session_cache, boundary_cache
            ),
            date(2026, 7, 29),
        )
        self.assertEqual(
            fx_session_date_from_utc_text(
                "2026-07-30T21:00:00+00:00", session_cache, boundary_cache
            ),
            date(2026, 7, 30),
        )
        self.assertEqual(
            fx_session_date_from_utc_text(
                "2026-01-15T21:59:00+00:00", session_cache, boundary_cache
            ),
            date(2026, 1, 14),
        )
        self.assertEqual(
            fx_session_date_from_utc_text(
                "2026-01-15T22:00:00+00:00", session_cache, boundary_cache
            ),
            date(2026, 1, 15),
        )

    def test_next_session_fill_profits_from_persistent_uptrend_without_costs(self):
        start = date(2025, 1, 1)
        bars = [
            DailyBar(
                session=start + timedelta(days=index),
                open=1.0 + index * 0.001,
                close=1.0005 + index * 0.001,
            )
            for index in range(90)
        ]
        metrics, series = backtest_pair(
            "EUR_USD",
            bars,
            target_annualized_volatility=0.10,
            volatility_lookback=20,
            max_volatility_scalar=2.0,
            round_trip_cost_pips=0.0,
        )

        self.assertGreater(metrics["decisions"], 0)
        self.assertGreater(metrics["total_return_pct"], 0.0)
        self.assertEqual(metrics["direction_hit_rate_pct"], 100.0)
        self.assertEqual(
            set(series),
            {
                "multi_5_10_21_42",
                "lookback_5",
                "lookback_10",
                "lookback_21",
                "lookback_42",
            },
        )


if __name__ == "__main__":
    unittest.main()
