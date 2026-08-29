import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from trad.oanda_pattern_count_forecast import (
    MINUTE_NS,
    PatternCountForecaster,
    build_cache,
    build_instrument_history,
    pattern_label,
    rolling_median_scale,
)


def candle_rows(start_minute: int, end_minute: int) -> list[dict[str, object]]:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    return [
        {
            "time": (start + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
            "mid": {"c": str(1.1000 + index * 0.0001)},
        }
        for index in range(start_minute, end_minute + 1)
    ]


class PatternHistoryTests(unittest.TestCase):
    def test_history_counts_exact_patterns_and_horizons(self):
        returns = np.asarray(([1.0, -1.0, 2.0, -2.0] * 30)[:100])
        closes = 1.1 + np.r_[0.0, np.cumsum(returns)] * 0.0001
        times = np.arange(len(closes), dtype=np.int64) * MINUTE_NS
        history = build_instrument_history(times, closes, 0.0001)
        self.assertEqual(history["baseline"]["1"]["count"], len(closes) - 1)
        self.assertEqual(
            sum(row["count"] for row in history["patterns"]["sign:3:1"].values()),
            len(closes) - 4,
        )
        self.assertGreater(len(history["patterns"]["magnitude:3:1"]), 0)

    def test_rolling_scale_uses_only_prior_returns(self):
        values = np.asarray([1.0] * 20 + [100.0])
        scales = rolling_median_scale(values, window=60, minimum=20)
        self.assertTrue(np.isnan(scales[19]))
        self.assertEqual(scales[20], 1.0)

    def test_pattern_labels_are_human_readable(self):
        self.assertEqual(pattern_label("sign", 3, 5), "U D U")
        self.assertEqual(pattern_label("magnitude", 3, 4), "D2 D2 U2")

    def test_csv_cache_normalizes_pandas_timestamp_resolution_to_nanoseconds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candles = root / "candles"
            candles.mkdir()
            start = datetime(2026, 7, 1, tzinfo=timezone.utc)
            rows = ["datetime,close,bid_close,ask_close,spread_pips"]
            for index in range(30):
                timestamp = (start + timedelta(minutes=index)).isoformat().replace("+00:00", "Z")
                close = 1.1 + index * 0.0001
                rows.append(f"{timestamp},{close},{close - 0.00005},{close + 0.00005},1")
            (candles / "EUR_USD_M1.csv").write_text("\n".join(rows) + "\n", encoding="ascii")
            payload = build_cache(candles, root / "cache.json.gz", workers=1)
            history = payload["instruments"]["EUR_USD"]
            self.assertGreater(history["start_time_ns"], 1_000_000_000_000_000_000)
            self.assertEqual(history["baseline"]["1"]["count"], 29)


class HistoricalLiveBlendTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.history_path = root / "history.json.gz"
        self.live_path = root / "live.json.gz"
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        end_ns = int((start + timedelta(minutes=10)).timestamp() * 1_000_000_000)
        payload = {
            "schema_version": 1,
            "generated_utc": "2026-07-01T00:11:00+00:00",
            "instruments": {
                "EUR_USD": {
                    "row_count": 11,
                    "start_time_ns": int(start.timestamp() * 1_000_000_000),
                    "end_time_ns": end_ns,
                    "pip_size": 0.0001,
                    "baseline": {
                        str(horizon): {"count": 1000, "up": 500, "zero": 0, "sum": 0, "abs_sum": 1000}
                        for horizon in (1, 3, 5)
                    },
                    "patterns": {
                        "sign:3:1": {
                            "7": {"count": 100, "up": 60, "zero": 0, "sum": 20, "abs_sum": 200}
                        }
                    },
                }
            },
        }
        with gzip.open(self.history_path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle)
        self.model = PatternCountForecaster(self.history_path, self.live_path)

    def tearDown(self):
        self.temporary.cleanup()

    def test_forecast_exposes_probability_counts_and_coefficient(self):
        forecast = self.model.forecast(
            "EUR_USD", candle_rows(0, 22), 0.0001, mode="sign", order=3, horizon_min=1
        )
        self.assertIsNotNone(forecast)
        self.assertEqual(forecast["pattern"], "U U U")
        self.assertEqual(forecast["historical_pattern_count"], 100)
        self.assertEqual(forecast["live_pattern_count"], 0)
        self.assertAlmostEqual(forecast["probability_up"], 0.583333, places=6)
        self.assertAlmostEqual(forecast["movement_coefficient"], 1.833333, places=6)

    def test_live_updates_only_origins_with_matured_targets(self):
        candles = candle_rows(0, 22)
        observed = self.model.observe_candles("EUR_USD", candles, 0.0001)
        self.assertEqual(observed, 27)
        live = self.model.live["instruments"]["EUR_USD"]
        self.assertEqual(live["baseline"]["1"]["count"], 11)
        self.assertEqual(live["baseline"]["3"]["count"], 9)
        self.assertEqual(live["baseline"]["5"]["count"], 7)
        self.assertEqual(self.model.observe_candles("EUR_USD", candles, 0.0001), 0)
        forecast = self.model.forecast(
            "EUR_USD", candles, 0.0001, mode="sign", order=3, horizon_min=1
        )
        self.assertEqual(forecast["live_pattern_count"], 11)
        self.assertGreater(forecast["probability_up"], 0.58)

    def test_live_only_state_bootstraps_without_historical_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = PatternCountForecaster(
                root / "missing_history.json.gz",
                root / "live.json.gz",
            )
            self.assertFalse(model.ready)

            observed = model.observe_candles(
                "EUR_USD", candle_rows(0, 22), 0.0001
            )
            model.save()
            forecast = model.forecast(
                "EUR_USD",
                candle_rows(0, 22),
                0.0001,
                mode="sign",
                order=3,
                horizon_min=1,
            )

            self.assertGreater(observed, 0)
            self.assertTrue(model.ready)
            self.assertIsNotNone(forecast)
            self.assertEqual(forecast["historical_pattern_count"], 0)
            self.assertGreater(forecast["live_pattern_count"], 0)
            self.assertEqual(forecast["data_sources"], ["completed_live_m1"])
            reloaded = PatternCountForecaster(
                root / "missing_history.json.gz",
                root / "live.json.gz",
            )
            self.assertTrue(reloaded.ready)


if __name__ == "__main__":
    unittest.main()
