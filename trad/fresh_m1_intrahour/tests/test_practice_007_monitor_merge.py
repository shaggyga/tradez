from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fresh_m1_intrahour.src.practice_007_forecast_outcome_monitor import (
    connect_database,
)
from fresh_m1_intrahour.src.practice_007_monitor_merge import (
    merge_monitor_databases,
)


class Practice007MonitorMergeTests(unittest.TestCase):
    def test_merges_and_deduplicates_monitor_rows(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first_path = root / "first.sqlite"
            second_path = root / "second.sqlite"
            output_path = root / "combined.sqlite"

            first = connect_database(first_path)
            first.execute(
                """
                INSERT INTO monitor_samples (
                    captured_epoch, captured_utc, raw_candidate_count,
                    consolidated_signal_count, qualified_signal_count,
                    stable_generation
                ) VALUES (1, 'one', 1, 1, 0, 1)
                """
            )
            first.commit()
            first.close()

            second = connect_database(second_path)
            second.executemany(
                """
                INSERT INTO monitor_samples (
                    captured_epoch, captured_utc, raw_candidate_count,
                    consolidated_signal_count, qualified_signal_count,
                    stable_generation
                ) VALUES (?, ?, 1, 1, 0, 1)
                """,
                [(1, "duplicate"), (2, "two")],
            )
            second.commit()
            second.close()

            summary = merge_monitor_databases(
                [first_path, second_path],
                output_path,
            )
            combined = connect_database(output_path)
            rows = combined.execute(
                """
                SELECT captured_epoch, captured_utc
                FROM monitor_samples
                ORDER BY captured_epoch
                """
            ).fetchall()
            combined.close()

        self.assertEqual(summary["source_count"], 2)
        self.assertEqual(summary["totals"]["monitor_samples"], 2)
        self.assertEqual(
            [(row["captured_epoch"], row["captured_utc"]) for row in rows],
            [(1.0, "one"), (2.0, "two")],
        )


if __name__ == "__main__":
    unittest.main()
