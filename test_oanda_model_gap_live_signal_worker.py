import tempfile
import unittest
from os import replace as real_replace
from pathlib import Path
from unittest.mock import patch

try:
    from oanda_model_gap_live_signal_worker import (
        LiveForecastLedger,
        WorkerProcessLock,
        archive_feature_snapshot,
        atomic_json,
        parse_args,
    )
    from oanda_signal_contribution_feed import SignalContributionFeed
except ModuleNotFoundError:
    from trad.oanda_model_gap_live_signal_worker import (
        LiveForecastLedger,
        WorkerProcessLock,
        archive_feature_snapshot,
        atomic_json,
        parse_args,
    )
    from trad.oanda_signal_contribution_feed import SignalContributionFeed


class ModelGapLiveSignalWorkerTests(unittest.TestCase):
    def test_all_pair_snapshot_timing_has_a_ten_minute_floor(self):
        args = parse_args(
            [
                "--ttl-sec",
                "180",
                "--max-snapshot-age-sec",
                "180",
                "--max-outcome-delay-sec",
                "180",
            ]
        )

        self.assertEqual(args.ttl_sec, 600.0)
        self.assertEqual(args.max_snapshot_age_sec, 600.0)
        self.assertEqual(args.max_outcome_delay_sec, 600.0)

    def test_worker_process_lock_rejects_a_duplicate(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "worker.lock"
            first = WorkerProcessLock(path)
            second = WorkerProcessLock(path)
            self.assertTrue(first.acquire())
            self.assertFalse(second.acquire())
            first.release()
            self.assertTrue(second.acquire())
            second.release()

    def test_atomic_json_retries_transient_windows_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "state.json"
            attempts = 0

            def flaky_replace(source, destination):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError("transient reader lock")
                real_replace(source, destination)

            with (
                patch(
                    f"{atomic_json.__module__}.os.replace",
                    side_effect=flaky_replace,
                ),
                patch(f"{atomic_json.__module__}.time.sleep"),
            ):
                atomic_json(target, {"status": "ok"})

            self.assertEqual(attempts, 2)
            self.assertIn('"status": "ok"', target.read_text(encoding="utf-8"))

    def test_feature_archive_is_deduplicated(self):
        snapshot = {
            "snapshot_id": "unit-snapshot",
            "generated_epoch": 1000.0,
            "generated_utc": "1970-01-01T00:16:40+00:00",
            "instruments": {
                "EUR_USD": {
                    "feature_origin_utc": "1970-01-01T00:16:00Z",
                    "quote": {"bid": 1.1, "ask": 1.1001, "time": "t", "source": "unit"},
                    "features": {"pip": 0.0001, "depth_imbalance": 0.2},
                    "timeframe_features": {
                        "H1": {"pip": 0.0001, "depth_imbalance": 0.2},
                        "H4": {"pip": 0.0001, "depth_imbalance": -0.1},
                    },
                    "unified_forecast_features": {
                        "m1__current_volume": 80.0,
                        "h1__r1_pips": 1.2,
                    },
                    "microstructure": {"bid_levels": 2},
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            first = archive_feature_snapshot(snapshot, Path(temporary))
            second = archive_feature_snapshot(snapshot, Path(temporary))
            self.assertEqual(first, second)
            self.assertTrue(first.is_file())
            import pyarrow.parquet as pq

            frame = pq.read_table(first).to_pandas()
            self.assertEqual(len(frame), 3)
            self.assertEqual(
                set(frame["input_timeframe"]),
                {"H1", "H4", "UNIFIED_MTF"},
            )
            unified = frame[frame["input_timeframe"] == "UNIFIED_MTF"].iloc[0]
            self.assertEqual(unified["m1__current_volume"], 80.0)

    def test_ledger_matures_against_executable_bid_ask(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            feed = SignalContributionFeed(root / "feed.sqlite")
            candidate = feed.normalize_forecast(
                {
                    "model_id": "ngboost",
                    "instrument": "EUR_USD",
                    "input_timeframe": "H1",
                    "generated_epoch": 1000.0,
                    "bid": 1.1000,
                    "ask": 1.1001,
                    "pip": 0.0001,
                    "forecast_curve": {
                        "60": {
                            "probability_up": 0.65,
                            "predicted_signed_pips": 3.0,
                            "predicted_magnitude_pips": 3.0,
                        },
                        "300": {"probability_up": 0.58},
                    },
                    "producer_metadata": {"artifact_sha256": "abc"},
                },
                "unit",
                now=1000.0,
            )
            ledger = LiveForecastLedger(root / "ledger.sqlite")
            self.assertEqual(ledger.register([candidate], "snapshot-1"), 2)
            result = ledger.mature(
                {
                    "generated_epoch": 1060.0,
                    "instruments": {
                        "EUR_USD": {"quote": {"bid": 1.1003, "ask": 1.1004}}
                    },
                },
                max_delay_sec=5.0,
                batch_size=1,
            )
            summary = ledger.summary(window_days=100000)
            ledger.close()
            feed.close()

        self.assertEqual(result, {"matured": 1, "censored": 0})
        self.assertEqual(summary["pending"], 1)
        self.assertEqual(len(summary["cells"]), 1)
        self.assertEqual(len(summary["cost_bucket_cells"]), 1)
        self.assertEqual(
            summary["cost_bucket_cells"][0]["cost_bucket"],
            "liquid_le_3pips",
        )
        self.assertAlmostEqual(summary["cells"][0]["average_net_pips"], 2.0)
        self.assertEqual(summary["cells"][0]["direction_accuracy"], 1.0)
        self.assertAlmostEqual(summary["cells"][0]["signed_pip_mae"], 0.0)
        self.assertAlmostEqual(summary["cells"][0]["magnitude_pip_mae"], 0.0)

    def test_ledger_prioritizes_timely_outcomes_over_expired_backlog(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            feed = SignalContributionFeed(root / "feed.sqlite")
            candidates = []
            for generated in (900.0, 1000.0):
                candidates.append(
                    feed.normalize_forecast(
                        {
                            "model_id": "ngboost",
                            "instrument": "EUR_USD",
                            "input_timeframe": "H1",
                            "generated_epoch": generated,
                            "bid": 1.1000,
                            "ask": 1.1001,
                            "pip": 0.0001,
                            "forecast_curve": {"60": {"probability_up": 0.65}},
                            "producer_metadata": {"artifact_sha256": "abc"},
                        },
                        "unit",
                        now=generated,
                    )
                )
            ledger = LiveForecastLedger(root / "ledger.sqlite")
            self.assertEqual(ledger.register(candidates, "snapshot-1"), 2)
            result = ledger.mature(
                {
                    "generated_epoch": 1060.0,
                    "instruments": {
                        "EUR_USD": {"quote": {"bid": 1.1003, "ask": 1.1004}}
                    },
                },
                max_delay_sec=5.0,
                batch_size=1,
            )
            statuses = ledger.connection.execute(
                "SELECT status FROM outcomes ORDER BY target_epoch"
            ).fetchall()
            pending = ledger.connection.execute(
                """
                SELECT COUNT(*) FROM predictions p
                LEFT JOIN outcomes o
                  ON o.candidate_id = p.candidate_id AND o.horizon_sec = p.horizon_sec
                WHERE o.candidate_id IS NULL
                """
            ).fetchone()[0]
            ledger.close()
            feed.close()

        self.assertEqual(result, {"matured": 1, "censored": 0})
        self.assertEqual(statuses, [("matured",)])
        self.assertEqual(pending, 1)

    def test_ledger_can_leave_missing_quotes_pending(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            feed = SignalContributionFeed(root / "feed.sqlite")
            candidates = []
            for instrument in ("EUR_USD", "GBP_USD"):
                candidates.append(
                    feed.normalize_forecast(
                        {
                            "model_id": "ngboost",
                            "instrument": instrument,
                            "input_timeframe": "H1",
                            "generated_epoch": 1000.0,
                            "bid": 1.1000,
                            "ask": 1.1001,
                            "pip": 0.0001,
                            "forecast_curve": {"60": {"probability_up": 0.65}},
                            "producer_metadata": {"artifact_sha256": "abc"},
                        },
                        "unit",
                        now=1000.0,
                    )
                )
            ledger = LiveForecastLedger(root / "ledger.sqlite")
            self.assertEqual(ledger.register(candidates, "snapshot-1"), 2)
            result = ledger.mature(
                {
                    "generated_epoch": 1060.0,
                    "instruments": {
                        "EUR_USD": {"quote": {"bid": 1.1003, "ask": 1.1004}}
                    },
                },
                max_delay_sec=5.0,
                censor_missing_quotes=False,
            )
            pending_instruments = ledger.connection.execute(
                "SELECT instrument FROM predictions ORDER BY instrument"
            ).fetchall()
            ledger.close()
            feed.close()

        self.assertEqual(result, {"matured": 1, "censored": 0})
        self.assertEqual(pending_instruments, [("GBP_USD",)])

    def test_ledger_skips_expired_points_during_registration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            feed = SignalContributionFeed(root / "feed.sqlite")
            candidate = feed.normalize_forecast(
                {
                    "model_id": "ngboost",
                    "instrument": "EUR_USD",
                    "input_timeframe": "M1",
                    "generated_epoch": 1000.0,
                    "bid": 1.1000,
                    "ask": 1.1001,
                    "pip": 0.0001,
                    "forecast_curve": {
                        "60": {"probability_up": 0.55},
                        "300": {"probability_up": 0.60},
                    },
                },
                "unit",
                now=1000.0,
            )
            ledger = LiveForecastLedger(root / "ledger.sqlite")
            inserted = ledger.register(
                [candidate],
                "snapshot-1",
                minimum_target_epoch=1100.0,
            )
            pending = ledger.connection.execute(
                "SELECT horizon_sec FROM predictions"
            ).fetchall()
            stats = dict(ledger.last_register_stats)
            ledger.close()
            feed.close()

        self.assertEqual(inserted, 1)
        self.assertEqual(pending, [(300,)])
        self.assertEqual(stats["points"], 2)
        self.assertEqual(stats["expired_points_skipped"], 1)


if __name__ == "__main__":
    unittest.main()
