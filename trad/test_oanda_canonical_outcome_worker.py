import json
import sqlite3
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from trad import oanda_canonical_outcome_worker as worker
from trad.oanda_shadow_outcome_store import ShadowOutcomeStore


class CanonicalOutcomeWorkerTests(unittest.TestCase):
    def test_due_rows_are_prioritized_over_bounded_path_sampling(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = worker.MaturityQueue(Path(directory) / "queue.sqlite")
            now = worker.utc_now()
            future = worker.utc_iso(now)
            for index in range(5):
                queue.insert(
                    rowid=index + 1, event_id=f"future-{index}", recorded_utc=future,
                    forecast_json="{}", remaining=[3600], path_complete=True,
                    observed_utc=future,
                )
            due_recorded = worker.utc_iso(now - timedelta(seconds=61))
            queue.insert(
                rowid=99, event_id="due", recorded_utc=due_recorded,
                forecast_json="{}", remaining=[60], path_complete=False,
                observed_utc=future,
            )
            queue.commit()
            rows = queue.relevant_rows(now.timestamp(), due_limit=10, path_limit=2)
            self.assertEqual(rows[0]["event_id"], "due")
            self.assertEqual(len(rows), 3)
            queue.close()

    def test_executable_pips_include_entry_and_exit_spread(self):
        self.assertAlmostEqual(
            worker.theoretical_pips("buy", 0.0001, 1.1000, 1.1002, 1.1006, 1.1008),
            4.0,
        )
        self.assertAlmostEqual(
            worker.theoretical_pips("sell", 0.0001, 1.1000, 1.1002, 1.0994, 1.0996),
            4.0,
        )

    def test_worker_matures_from_forecast_time_and_is_observation_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.sqlite"
            queue_path = root / "queue.sqlite"
            store = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            recorded = worker.utc_now() - timedelta(seconds=61)
            forecast = {
                "id": "f1",
                "model_version": "m1",
                "feature_version": "x1",
                "data_cutoff_utc": worker.utc_iso(recorded),
                "lane_id": "lane",
                "family": "volatility_squeeze_breakout",
                "profile": "research",
                "model_id": "model",
                "input_timeframe": "M1",
                "training_timeframe": "M1",
                "kind": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": worker.utc_iso(recorded),
                "entry_bid": 1.1000,
                "entry_ask": 1.1002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
                "stop_loss_pips": 5.0,
                "take_profit_r": 2.0,
            }
            store.observe_forecast(forecast, horizons=[60], track_outcome=True)
            store.flush(force=True)
            store.close()
            source = sqlite3.connect(evidence_path)
            source.execute("PRAGMA query_only=ON")
            queue = worker.MaturityQueue(queue_path)
            sink = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            ingested, missed = worker.ingest_forecasts(
                source,
                queue,
                sink,
                grace_sec=15.0,
                path_complete_sec=10.0,
                batch_size=100,
            )
            self.assertEqual((ingested, missed), (1, 0))
            recorded_text = queue.rows()[0]["recorded_utc"]
            recorded_at = worker.parse_time(recorded_text)
            self.assertIsNotNone(recorded_at)
            now = recorded_at + timedelta(seconds=61)
            snapshot = {
                "generated_utc": worker.utc_iso(now),
                "_generated_datetime": now,
                "quotes": {
                    "EUR_USD": {
                        "bid": 1.1006,
                        "ask": 1.1008,
                        "time": worker.utc_iso(now),
                    }
                },
            }
            with patch.object(worker, "utc_now", return_value=now):
                counts = worker.process_snapshot(
                    snapshot,
                    "",
                    queue,
                    sink,
                    grace_sec=15.0,
                    max_snapshot_age_sec=15.0,
                )
            sink.flush(force=True)
            row = sink.connection.execute(
                "SELECT theoretical_pips,outcome_delay_sec,diagnostics_json FROM canonical_outcomes"
            ).fetchone()
            self.assertEqual(counts["matured"], 1)
            self.assertAlmostEqual(row[0], 4.0)
            self.assertLess(row[1], 2.0)
            diagnostics = json.loads(row[2])
            self.assertTrue(diagnostics["research_only"])
            self.assertEqual(diagnostics["maturity_worker"], "canonical_quote_snapshot_v1")
            self.assertEqual(queue.count(), 0)
            sink.close()
            queue.close()
            source.close()

    def test_pre_boundary_snapshot_waits_and_provider_time_is_diagnostic_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.sqlite"
            store = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            recorded = worker.utc_now() - timedelta(seconds=61)
            forecast = {
                "id": "boundary",
                "model_version": "m1",
                "feature_version": "x1",
                "data_cutoff_utc": worker.utc_iso(recorded),
                "lane_id": "lane",
                "family": "seed",
                "profile": "research",
                "model_id": "model",
                "kind": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": worker.utc_iso(recorded),
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
            }
            store.observe_forecast(forecast, horizons=[60], track_outcome=True)
            store.close()
            source = sqlite3.connect(evidence_path)
            source.execute("PRAGMA query_only=ON")
            queue = worker.MaturityQueue(root / "queue.sqlite")
            sink = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            worker.ingest_forecasts(
                source, queue, sink, grace_sec=15.0, path_complete_sec=10.0, batch_size=10
            )
            recorded = worker.parse_time(queue.rows()[0]["recorded_utc"])
            pre_boundary = recorded + timedelta(seconds=59)
            now = recorded + timedelta(seconds=61)
            snapshot = {
                "generated_utc": worker.utc_iso(pre_boundary),
                "_generated_datetime": pre_boundary,
                "quotes": {
                    "EUR_USD": {
                        "bid": 1.1006,
                        "ask": 1.1008,
                        "time": worker.utc_iso(now + timedelta(seconds=60)),
                    }
                },
            }
            with patch.object(worker, "utc_now", return_value=now):
                counts = worker.process_snapshot(
                    snapshot, "", queue, sink, grace_sec=15.0, max_snapshot_age_sec=15.0
                )
            self.assertEqual(counts["matured"], 0)
            self.assertEqual(queue.count(), 1)

            boundary = recorded + timedelta(seconds=62)
            provider_time = boundary + timedelta(seconds=60)
            snapshot["generated_utc"] = worker.utc_iso(boundary)
            snapshot["_generated_datetime"] = boundary
            snapshot["quotes"]["EUR_USD"]["time"] = worker.utc_iso(provider_time)
            with patch.object(worker, "utc_now", return_value=boundary):
                counts = worker.process_snapshot(
                    snapshot, "", queue, sink, grace_sec=15.0, max_snapshot_age_sec=15.0
                )
            sink.flush(force=True)
            exit_time, diagnostics_json = sink.connection.execute(
                "SELECT exit_time,diagnostics_json FROM canonical_outcomes"
            ).fetchone()
            diagnostics = json.loads(diagnostics_json)
            self.assertEqual(counts["matured"], 1)
            self.assertEqual(exit_time, worker.utc_iso(boundary))
            self.assertEqual(diagnostics["provider_quote_time"], worker.utc_iso(provider_time))
            sink.close()
            queue.close()
            source.close()

    def test_late_quote_becomes_integrity_miss_not_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.sqlite"
            queue_path = root / "queue.sqlite"
            store = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            forecast = {
                "id": "late",
                "model_version": "m1",
                "feature_version": "x1",
                "data_cutoff_utc": worker.utc_iso(),
                "lane_id": "lane",
                "family": "seed",
                "profile": "research",
                "model_id": "model",
                "kind": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": worker.utc_iso(),
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
            }
            store.observe_forecast(forecast, horizons=[60], track_outcome=True)
            store.close()
            source = sqlite3.connect(evidence_path)
            source.execute("PRAGMA query_only=ON")
            queue = worker.MaturityQueue(queue_path)
            sink = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            worker.ingest_forecasts(
                source, queue, sink, grace_sec=15.0, path_complete_sec=10.0, batch_size=10
            )
            recorded = worker.parse_time(queue.rows()[0]["recorded_utc"])
            now = recorded + timedelta(seconds=76)
            snapshot = {
                "generated_utc": worker.utc_iso(now),
                "_generated_datetime": now,
                "quotes": {"EUR_USD": {"bid": 1.101, "ask": 1.1012, "time": worker.utc_iso(now)}},
            }
            with patch.object(worker, "utc_now", return_value=now):
                counts = worker.process_snapshot(
                    snapshot, "", queue, sink, grace_sec=15.0, max_snapshot_age_sec=15.0
                )
            sink.flush(force=True)
            self.assertEqual(counts["matured"], 0)
            self.assertEqual(counts["missed"], 1)
            self.assertEqual(
                sink.connection.execute("SELECT COUNT(*) FROM canonical_outcomes").fetchone()[0],
                0,
            )
            self.assertEqual(
                sink.connection.execute("SELECT COUNT(*) FROM forecast_integrity_events").fetchone()[0],
                1,
            )
            sink.close()
            queue.close()
            source.close()

    def test_in_memory_path_cache_preserves_mfe_until_maturity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.sqlite"
            store = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            forecast = {
                "id": "path",
                "model_version": "m1",
                "feature_version": "x1",
                "data_cutoff_utc": worker.utc_iso(),
                "lane_id": "lane",
                "family": "seed",
                "profile": "research",
                "model_id": "model",
                "kind": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": worker.utc_iso(),
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
            }
            store.observe_forecast(forecast, horizons=[60], track_outcome=True)
            store.close()
            source = sqlite3.connect(evidence_path)
            source.execute("PRAGMA query_only=ON")
            queue = worker.MaturityQueue(root / "queue.sqlite")
            sink = ShadowOutcomeStore(evidence_path, batch_size=64, flush_sec=0.1)
            worker.ingest_forecasts(
                source, queue, sink, grace_sec=15.0, path_complete_sec=10.0, batch_size=10
            )
            recorded = worker.parse_time(queue.rows()[0]["recorded_utc"])
            cache = {}
            first_time = recorded + timedelta(seconds=5)
            first = {
                "generated_utc": worker.utc_iso(first_time),
                "_generated_datetime": first_time,
                "quotes": {"EUR_USD": {"bid": 1.1008, "ask": 1.1010, "time": worker.utc_iso(first_time)}},
            }
            with patch.object(worker, "utc_now", return_value=first_time):
                worker.process_snapshot(
                    first,
                    "",
                    queue,
                    sink,
                    grace_sec=15.0,
                    max_snapshot_age_sec=15.0,
                    path_cache=cache,
                )
            self.assertEqual(queue.rows()[0]["path_samples"], 0)
            final_time = recorded + timedelta(seconds=61)
            final = {
                "generated_utc": worker.utc_iso(final_time),
                "_generated_datetime": final_time,
                "quotes": {"EUR_USD": {"bid": 1.1004, "ask": 1.1006, "time": worker.utc_iso(final_time)}},
            }
            with patch.object(worker, "utc_now", return_value=final_time):
                worker.process_snapshot(
                    final,
                    "",
                    queue,
                    sink,
                    grace_sec=15.0,
                    max_snapshot_age_sec=15.0,
                    path_cache=cache,
                )
            sink.flush(force=True)
            outcome = sink.connection.execute(
                "SELECT max_favorable_pips,path_samples FROM canonical_outcomes"
            ).fetchone()
            self.assertAlmostEqual(outcome[0], 6.0)
            self.assertEqual(outcome[1], 2)
            sink.close()
            queue.close()
            source.close()


if __name__ == "__main__":
    unittest.main()
