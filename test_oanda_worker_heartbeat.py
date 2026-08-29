import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from trad.oanda_worker_heartbeat import WorkerHeartbeat, write_json_atomic


class WorkerHeartbeatTests(unittest.TestCase):
    def test_atomic_write_retries_transient_replace_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "heartbeat.json"
            real_replace = __import__("os").replace
            attempts = 0

            def replace_after_transient_failure(source, destination):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError("busy")
                return real_replace(source, destination)

            with mock.patch(
                "trad.oanda_worker_heartbeat.os.replace",
                side_effect=replace_after_transient_failure,
            ) as replace:
                write_json_atomic(path, {"status": "running"})

            payload = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(payload, {"status": "running"})
        self.assertEqual(replace.call_count, 2)

    def test_atomic_write_cleans_unique_temp_after_persistent_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "heartbeat.json"
            with mock.patch(
                "trad.oanda_worker_heartbeat.os.replace",
                side_effect=PermissionError("busy"),
            ):
                with self.assertRaises(PermissionError):
                    write_json_atomic(path, {"status": "running"})
            self.assertFalse(path.exists())
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_background_heartbeat_updates_without_main_loop_activity(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "heartbeat.json"
            heartbeat = WorkerHeartbeat(
                path,
                worker="test-worker",
                role="tracker",
                interval_sec=1.0,
            ).start()
            first = path.stat().st_mtime_ns
            heartbeat.update(phase="draining", pending=123)
            time.sleep(1.2)
            second = path.stat().st_mtime_ns
            payload = json.loads(path.read_text(encoding="utf-8"))
            heartbeat.close()

        self.assertGreater(second, first)
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["phase"], "draining")
        self.assertIn("phase_updated_at", payload)
        self.assertGreaterEqual(payload["phase_age_sec"], 1.0)
        self.assertEqual(payload["details"]["pending"], 123)

    def test_phase_age_resets_only_when_progress_phase_changes(self):
        heartbeat = WorkerHeartbeat(
            Path("unused.json"),
            worker="test-worker",
        )
        heartbeat.update(phase="opening_signal_feed")
        changed_at = heartbeat._phase_updated_monotonic
        # Windows' monotonic clock can advance in ~15.6 ms ticks.
        time.sleep(0.03)
        heartbeat.update(phase="opening_signal_feed", attempt=2)
        self.assertEqual(heartbeat._phase_updated_monotonic, changed_at)

        heartbeat.update(phase="streaming")
        self.assertGreater(heartbeat._phase_updated_monotonic, changed_at)
        self.assertEqual(heartbeat.phase, "streaming")

    def test_progress_age_and_sequence_are_independent_from_liveness(self):
        heartbeat = WorkerHeartbeat(
            Path("unused.json"),
            worker="test-worker",
        )
        first = heartbeat._payload("running")
        heartbeat.update(phase="loading", rows_seen=10)
        second = heartbeat._payload("running")
        heartbeat.mark_progress(phase="loading", rows_seen=20)
        third = heartbeat._payload("running")

        self.assertEqual(first["progress_sequence"], 0)
        self.assertEqual(second["progress_sequence"], 0)
        self.assertEqual(third["progress_sequence"], 1)
        self.assertEqual(third["details"]["rows_seen"], 20)
        self.assertLessEqual(third["progress_age_sec"], second["progress_age_sec"])


if __name__ == "__main__":
    unittest.main()
