import unittest
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from trad.oanda_s5_timeframe_strategy_replay import (
    TimeframeHistory,
    load_m1_history,
    load_history_with_fallback,
    parse_timeframe,
    read_csv_tail,
    read_parquet_tail,
    seconds_label,
    select_times,
    setup_outcomes,
)


class TimeframeReplayTests(unittest.TestCase):
    def test_corrupt_m1_uses_explicit_s5_integrity_fallback(self):
        expected = SimpleNamespace(instrument="USD_JPY")
        with (
            patch(
                "trad.oanda_s5_timeframe_strategy_replay.load_m1_history",
                side_effect=OSError("CRC error"),
            ),
            patch(
                "trad.oanda_s5_timeframe_strategy_replay.load_history",
                return_value=expected,
            ) as fallback,
        ):
            history, source, warning = load_history_with_fallback(
                source="m1",
                m1_dir=Path("m1"),
                m1_cache_dir=Path("cache"),
                s5_dir=Path("s5"),
                instrument="USD_JPY",
                timeframe_seconds=900,
                tail_rows=0,
            )

        self.assertIs(history, expected)
        self.assertEqual(source, "s5_integrity_fallback")
        self.assertIn("CRC error", warning)
        fallback.assert_called_once_with(Path("s5"), "USD_JPY", 900, 0)

    def test_transient_m1_permission_error_retries_before_fallback(self):
        expected = SimpleNamespace(instrument="USD_JPY")
        with (
            patch(
                "trad.oanda_s5_timeframe_strategy_replay.load_m1_history",
                side_effect=[PermissionError("temporarily locked"), expected],
            ) as loader,
            patch("trad.oanda_s5_timeframe_strategy_replay.time.sleep") as sleep,
            patch("trad.oanda_s5_timeframe_strategy_replay.load_history") as fallback,
        ):
            history, source, warning = load_history_with_fallback(
                source="m1",
                m1_dir=Path("m1"),
                m1_cache_dir=Path("cache"),
                s5_dir=Path("s5"),
                instrument="USD_JPY",
                timeframe_seconds=900,
                tail_rows=0,
            )

        self.assertIs(history, expected)
        self.assertEqual(source, "m1_deep")
        self.assertEqual(warning, "")
        self.assertEqual(loader.call_count, 2)
        sleep.assert_called_once_with(0.5)
        fallback.assert_not_called()

    def test_slower_input_can_score_shorter_exact_horizon(self):
        execution_times = list(
            pd.date_range("2026-07-16T10:00:00Z", periods=181, freq="5s")
        )
        execution = pd.DataFrame(
            {
                "bid_open": [1.1000] * len(execution_times),
                "ask_open": [1.1002] * len(execution_times),
                "bid_close": [1.1003] * len(execution_times),
                "ask_close": [1.1005] * len(execution_times),
            },
            index=execution_times,
        )
        primary_times = [execution_times[0], execution_times[60]]
        empty = pd.DataFrame(index=primary_times)
        history = TimeframeHistory(
            instrument="EUR_USD",
            pip=0.0001,
            primary_seconds=300,
            primary=empty,
            higher=empty,
            primary_times=primary_times,
            higher_times=primary_times,
            primary_candles=[],
            higher_candles=[],
            positions={timestamp: index for index, timestamp in enumerate(primary_times)},
            execution=execution,
            execution_seconds=5,
            execution_times=execution_times,
            execution_positions={
                timestamp: index for index, timestamp in enumerate(execution_times)
            },
            spread_mode="observed_bid_ask",
        )

        entry = history.entry_position(primary_times[0])
        outcomes = setup_outcomes(history, entry, "buy", [60, 300])

        self.assertIsNotNone(entry)
        self.assertEqual(set(outcomes), {60, 300})

    def test_hour_labels_and_inputs_are_supported(self):
        self.assertEqual(parse_timeframe("H3"), 10800)
        self.assertEqual(seconds_label(10800), "H3")

    def test_csv_tail_reader_does_not_include_older_rows(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "history.csv"
            pd.DataFrame(
                {"time": range(20), "close": [100 + value for value in range(20)]}
            ).to_csv(path, index=False)

            frame = read_csv_tail(path, ["time", "close"], 5)

            self.assertEqual(frame["time"].tolist(), [15, 16, 17, 18, 19])
            self.assertEqual(frame["close"].tolist(), [115, 116, 117, 118, 119])

    def test_parquet_tail_reader_only_returns_requested_rows(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "history.parquet"
            pd.DataFrame({"value": range(30)}).to_parquet(
                path, index=False, row_group_size=5
            )

            frame = read_parquet_tail(path, 7)

            self.assertEqual(frame["value"].tolist(), [23, 24, 25, 26, 27, 28, 29])

    def test_exit_position_can_use_bounded_next_quote(self):
        decision = pd.Timestamp("2026-01-01T00:00:00Z")
        history = SimpleNamespace(
            execution_seconds=5,
            execution_times=[
                decision,
                decision + timedelta(seconds=15),
                decision + timedelta(seconds=70),
            ],
            execution_positions={},
            execution=pd.DataFrame(
                index=[
                    decision,
                    decision + timedelta(seconds=15),
                    decision + timedelta(seconds=70),
                ]
            ),
            max_exit_delay_seconds=10,
        )

        located = TimeframeHistory.exit_position(history, 0, 10)

        self.assertEqual(located, 1)
        history.max_exit_delay_seconds = 4
        self.assertIsNone(TimeframeHistory.exit_position(history, 0, 10))

    def test_fresh_m1_cache_is_loaded_without_opening_raw_csv(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            m1_dir = root / "raw"
            cache_dir = root / "cache"
            m1_dir.mkdir()
            cache_dir.mkdir()
            times = pd.date_range("2026-01-01T00:00:00Z", periods=400, freq="1min")
            frame = pd.DataFrame(
                {
                    "time": times.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "datetime": times.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.0,
                    "volume": 10,
                    "bid_open": 0.9999,
                    "bid_high": 1.0999,
                    "bid_low": 0.8999,
                    "bid_close": 0.9999,
                    "ask_open": 1.0001,
                    "ask_high": 1.1001,
                    "ask_low": 0.9001,
                    "ask_close": 1.0001,
                    "spread_pips": 2.0,
                }
            )
            frame.to_parquet(cache_dir / "USD_JPY_M1.parquet", index=False)

            with patch(
                "trad.oanda_s5_timeframe_strategy_replay.pd.read_csv",
                side_effect=AssertionError("raw CSV must not be opened"),
            ):
                history = load_m1_history(
                    m1_dir,
                    "USD_JPY",
                    60,
                    0,
                    cache_dir,
                    compact=True,
                )

        self.assertEqual(len(history.primary), 400)

    def test_uniform_sampling_spans_the_available_history(self):
        times = list(pd.date_range("2024-01-01T00:00:00Z", periods=100, freq="1min"))
        history = SimpleNamespace(
            primary_times=times,
            execution_times=times,
            primary_seconds=60,
        )

        uniform = select_times(
            {"EUR_USD": history},
            step_seconds=60,
            max_cycles=5,
            sampling="uniform",
        )
        latest = select_times(
            {"EUR_USD": history},
            step_seconds=60,
            max_cycles=5,
            sampling="latest",
        )

        self.assertEqual(uniform[0], times[0])
        self.assertEqual(uniform[-1], times[-2])
        self.assertGreater(uniform[-1] - uniform[0], latest[-1] - latest[0])

    def test_compact_history_uses_lazy_candles_and_index_positions(self):
        times = pd.date_range("2026-07-16T10:00:00Z", periods=70, freq="1min")
        frame = pd.DataFrame(
            {
                "open": 1.1000,
                "high": 1.1004,
                "low": 1.0998,
                "close": 1.1002,
                "volume": 10.0,
                "bid_open": 1.0999,
                "bid_high": 1.1003,
                "bid_low": 1.0997,
                "bid_close": 1.1001,
                "ask_open": 1.1001,
                "ask_high": 1.1005,
                "ask_low": 1.0999,
                "ask_close": 1.1003,
            },
            index=times,
        )
        history = TimeframeHistory(
            instrument="EUR_USD",
            pip=0.0001,
            primary_seconds=60,
            primary=frame,
            higher=frame,
            primary_times=frame.index,
            higher_times=frame.index,
            primary_candles=[],
            higher_candles=[],
            positions={},
            execution=frame,
            execution_seconds=60,
            execution_times=frame.index,
            execution_positions={},
            spread_mode="observed_bid_ask",
        )

        windows = history.windows(times[60])
        entry = history.entry_position(times[60])
        exit_position = history.exit_position(entry, 300)

        self.assertEqual(len(windows["M1"]), 61)
        self.assertEqual(windows["M1"][-1]["time"], times[60].isoformat().replace("+00:00", "Z"))
        self.assertEqual(entry, 61)
        self.assertEqual(exit_position, 65)


if __name__ == "__main__":
    unittest.main()
