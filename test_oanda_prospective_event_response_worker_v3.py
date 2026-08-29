from __future__ import annotations

import copy
import datetime as dt
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from forex_system.ingestion.prospective_event_response_v3 import load_contract  # noqa: E402
import oanda_prospective_event_response_worker_v3 as worker  # noqa: E402
from oanda_prospective_event_response_worker_v3 import (  # noqa: E402
    choose_cadence,
    run_worker,
)


UTC = dt.timezone.utc


class ProspectiveEventResponseWorkerV3Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = copy.deepcopy(
            load_contract(ROOT / "config" / "prospective_event_response_capture_v3.json")
        )
        self.now = dt.datetime(2026, 8, 17, 5, 0, 0, tzinfo=UTC)

    def test_active_window_uses_one_second_cadence(self):
        for delta in (-2, 0, 45):
            mode, cadence = choose_cadence(
                self.now,
                [self.now - dt.timedelta(seconds=delta)],
                self.contract,
            )
            self.assertEqual(mode, "active_target_window")
            self.assertEqual(cadence, 1.0)

    def test_idle_wakes_at_target_minus_two_seconds(self):
        target = self.now + dt.timedelta(seconds=7)
        mode, cadence = choose_cadence(self.now, [target], self.contract)
        self.assertEqual(mode, "idle_waiting_for_target")
        self.assertEqual(cadence, 5.0)

    def test_idle_is_bounded_to_fifteen_seconds(self):
        target = self.now + dt.timedelta(hours=1)
        mode, cadence = choose_cadence(self.now, [target], self.contract)
        self.assertEqual(mode, "idle_waiting_for_target")
        self.assertEqual(cadence, 15.0)
        mode, cadence = choose_cadence(self.now, [], self.contract)
        self.assertEqual(mode, "idle_no_near_target")
        self.assertEqual(cadence, 15.0)

    def test_outside_target_lag_returns_idle(self):
        target = self.now - dt.timedelta(seconds=46)
        mode, cadence = choose_cadence(self.now, [target], self.contract)
        self.assertEqual(mode, "idle_no_near_target")
        self.assertEqual(cadence, 15.0)

    def test_blocking_catalog_sync_cannot_delay_active_target_sampling(self):
        blocker_started = threading.Event()
        release_blocker = threading.Event()

        def deliberately_blocking_catalog_sync():
            blocker_started.set()
            release_blocker.wait(timeout=2.0)

        background = threading.Thread(
            target=deliberately_blocking_catalog_sync,
            name="deliberately-blocking-independent-catalog-sync",
            daemon=True,
        )
        background.start()
        self.assertTrue(blocker_started.wait(timeout=0.25))
        response = {
            "status": "captured_without_catalog_wait",
            "supported_execution_decision": "no_trade",
        }
        started = time.perf_counter()
        try:
            with tempfile.TemporaryDirectory() as directory, mock.patch.object(
                worker, "pending_target_times", return_value=[self.now]
            ), mock.patch.object(
                worker, "choose_cadence", return_value=("active_target_window", 1.0)
            ), mock.patch.object(
                worker.collector, "run_once", return_value=response
            ) as capture:
                result = run_worker(
                    duration_sec=0.0,
                    worker_state_path=Path(directory) / "worker_state.json",
                )
        finally:
            release_blocker.set()
            background.join(timeout=0.5)
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 0.5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["status"], response["status"])
        self.assertEqual(result[0]["worker_cycle_count"], 1)
        self.assertEqual(result[0]["worker_mode"], "active_target_window")
        capture.assert_called_once()
        source = (ROOT / "oanda_prospective_event_response_worker_v3.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("synchronize_catalog(", source)
        self.assertNotIn("import oanda_news_event_tagger", source)

    def test_one_hundred_thousand_cycles_retain_only_latest_result(self):
        response = {
            "status": "idle",
            "supported_execution_decision": "no_trade",
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            worker, "pending_target_times", return_value=[]
        ), mock.patch.object(
            worker, "choose_cadence", return_value=("idle_no_near_target", 15.0)
        ), mock.patch.object(
            worker.collector, "run_once", return_value=response
        ) as capture, mock.patch.object(
            worker, "_atomic_write_json"
        ), mock.patch.object(
            worker.time, "sleep"
        ), mock.patch.object(
            worker.time, "monotonic", return_value=0.0
        ), mock.patch("builtins.print"):
            result = run_worker(
                duration_sec=1.0,
                maximum_cycles=100_000,
                worker_state_path=Path(directory) / "worker_state.json",
            )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["worker_cycle_count"], 100_000)
        self.assertEqual(capture.call_count, 100_000)


if __name__ == "__main__":
    unittest.main()
