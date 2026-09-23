from __future__ import annotations

import json
import tempfile
import unittest
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import oanda_depth_parquet_collector as collector
except ModuleNotFoundError:
    from trad import oanda_depth_parquet_collector as collector


def row(timestamp: str, instrument: str = "EUR_USD") -> dict[str, object]:
    return {
        "collected_utc": timestamp,
        "broker_time_utc": timestamp,
        "instrument": instrument,
        "bid": 1.1,
        "ask": 1.1001,
        "mid": 1.10005,
    }


class DepthParquetCollectorTests(unittest.TestCase):
    def test_flush_writes_immutable_parts_without_reading_partition_dataset(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = [row("2026-07-15T10:00:00+00:00")]
            second = [row("2026-07-15T10:00:01+00:00", "GBP_USD")]
            with patch.object(collector, "ROOT", root), patch.object(
                collector.pq, "read_table", side_effect=AssertionError("must not read existing parts")
            ):
                self.assertEqual(collector.flush(first), 1)
                self.assertEqual(collector.flush(second), 1)

            parts = sorted((root / "date=20260715" / "hour=10").glob("*.parquet"))
            self.assertEqual(len(parts), 2)
            self.assertEqual(sum(pq.ParquetFile(path).metadata.num_rows for path in parts), 2)

    def test_partition_directory_uses_utc_timestamp(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(collector, "ROOT", Path(temporary)):
                directory = collector.partition_directory(row(datetime.now(timezone.utc).isoformat()))
            self.assertIn("date=", str(directory))
            self.assertIn("hour=", str(directory))

    def test_save_state_retries_transient_windows_reader_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path = root / "collector_state.json"
            real_replace = collector.os.replace
            calls = 0

            def flaky_replace(source, destination):
                nonlocal calls
                calls += 1
                if calls < 3:
                    raise PermissionError("destination briefly pinned")
                return real_replace(source, destination)

            with patch.object(collector, "ROOT", root), patch.object(
                collector, "STATE_PATH", state_path
            ), patch.object(collector.os, "replace", side_effect=flaky_replace), patch.object(
                collector.time, "sleep"
            ):
                collector.save_state({"status": "ok"})

            self.assertEqual(calls, 3)
            self.assertEqual(json.loads(state_path.read_text(encoding="utf-8")), {"status": "ok"})


if __name__ == "__main__":
    unittest.main()
