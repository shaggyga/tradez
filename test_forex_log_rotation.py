from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oanda_practice_shadow_strategy_lab import close_log_handles, log_line


class ForexLogRotationTests(unittest.TestCase):
    def tearDown(self) -> None:
        close_log_handles()

    def test_jsonl_rotates_without_losing_events(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "worker.jsonl"
            with patch.dict(os.environ, {"FOREX_LOG_MAX_BYTES": "256"}):
                for sequence in range(20):
                    log_line(path, "rotation_test", sequence=sequence, payload="x" * 48)
                close_log_handles()
            parts = sorted(path.parent.glob("worker*.jsonl"))
            self.assertGreater(len(parts), 1)
            observed = []
            for part in parts:
                for line in part.read_text(encoding="utf-8").splitlines():
                    observed.append(json.loads(line)["sequence"])
            self.assertEqual(sorted(observed), list(range(20)))

    def test_rotation_rename_denial_keeps_logging_to_active_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "worker.jsonl"
            with (
                patch.dict(os.environ, {"FOREX_LOG_MAX_BYTES": "256"}),
                patch(
                    "oanda_practice_shadow_strategy_lab.os.replace",
                    side_effect=PermissionError("temporarily busy"),
                ),
            ):
                for sequence in range(20):
                    log_line(
                        path,
                        "rotation_test",
                        sequence=sequence,
                        payload="x" * 48,
                    )
                close_log_handles()
            observed = [
                json.loads(line)["sequence"]
                for line in path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(observed, list(range(20)))


if __name__ == "__main__":
    unittest.main()
