import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fresh_m1_intrahour.src.practice_007_monitor_final_analysis import (
    execution_log_summary,
    technical_freshness_summary,
)


class ExecutionLogSummaryTests(unittest.TestCase):
    def test_reverse_scan_is_time_bounded_and_chronological(self):
        rows = [
            {
                "time": "2026-07-27T17:00:00+00:00",
                "event": "execution_skipped",
                "reason": "before_window",
            },
            {
                "time": "2026-07-27T18:00:00+00:00",
                "event": "lab_evaluation_cycle",
                "payload": "x" * 100_000,
            },
            {
                "time": "2026-07-27T18:10:00+00:00",
                "event": "execution_skipped",
                "reason": "no_qualified_signals",
            },
            {
                "time": "2026-07-27T18:20:00+00:00",
                "event": "practice_order_not_filled",
                "instrument": "EUR_USD",
            },
            {
                "time": "2026-07-27T19:00:00+00:00",
                "event": "execution_selected",
                "instrument": "GBP_USD",
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "strategy.jsonl"
            path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            start = datetime(
                2026, 7, 27, 18, 5, tzinfo=timezone.utc
            ).timestamp()
            end = datetime(
                2026, 7, 27, 18, 30, tzinfo=timezone.utc
            ).timestamp()
            result = execution_log_summary(path, start, end)

        self.assertEqual(
            result["event_counts"],
            {"practice_order_not_filled": 1, "execution_skipped": 1},
        )
        self.assertEqual(
            [event["event"] for event in result["events"]],
            ["execution_skipped", "practice_order_not_filled"],
        )
        self.assertEqual(
            result["skip_reasons"],
            {"no_qualified_signals": 1},
        )

    def test_technical_freshness_counts_completed_bars_behind(self):
        import sqlite3

        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            CREATE TABLE technical_samples (
                captured_epoch REAL,
                candle_utc TEXT,
                timeframe TEXT
            )
            """
        )
        captured = datetime(
            2026, 7, 27, 19, 47, 24, tzinfo=timezone.utc
        ).timestamp()
        connection.executemany(
            "INSERT INTO technical_samples VALUES (?, ?, ?)",
            [
                (captured, "2026-07-27T19:46:00Z", "M1"),
                (captured, "2026-07-27T19:32:00Z", "M1"),
                (captured, "2026-07-27T19:40:00Z", "M5"),
                (captured, "2026-07-27T18:20:00Z", "M5"),
            ],
        )

        result = technical_freshness_summary(
            connection,
            captured - 1.0,
            captured + 1.0,
        )
        connection.close()

        self.assertEqual(
            result["by_timeframe"]["M1"]["maximum_completed_bars_behind"],
            14,
        )
        self.assertEqual(
            result["by_timeframe"]["M5"]["maximum_completed_bars_behind"],
            16,
        )


if __name__ == "__main__":
    unittest.main()
