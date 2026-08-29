import sqlite3
import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trad.oanda_shadow_outcome_store import ShadowOutcomeStore, sampled_detail


class ShadowOutcomeStoreTests(unittest.TestCase):
    def test_future_shifted_forecast_is_detected_and_not_matured(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            store.observe_forecast(
                {
                    "id": "future-1",
                    "model_version": "model-v1",
                    "feature_version": "features-v1",
                    "data_cutoff_utc": "2026-08-01T12:01:00+00:00",
                    "entry_time": "2026-08-01T12:00:00+00:00",
                    "lane_id": "future",
                    "family": "future",
                    "profile": "proof",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                },
                horizons=[3600],
                track_outcome=True,
            )
            store.flush(force=True)
            forecast = store.connection.execute(
                "SELECT kind,blocked_reason,miss_class,track_outcome FROM canonical_forecasts"
            ).fetchone()
            self.assertEqual(forecast, ("miss", "future_data_cutoff", "timestamp_violation", 0))
            self.assertEqual(
                store.connection.execute(
                    "SELECT event_type FROM forecast_integrity_events"
                ).fetchone()[0],
                "forecast_timestamp_violation",
            )
            store.close()

    def test_nonmaturing_reject_keeps_causal_contract_without_path_bloat(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            forecast = {
                "id": "reject-1",
                "model_version": "model-v1",
                "feature_version": "features-v1",
                "data_cutoff_utc": "2026-08-01T12:00:00+00:00",
                "lane_id": "momentum.fast",
                "family": "momentum",
                "profile": "fast",
                "model_id": "momentum.fast",
                "kind": "miss",
                "instrument": "EUR_USD",
                "direction": "buy",
                "blocked_reason": "spread_absolute",
                "miss_class": "hard_reject",
                "entry_time": "2026-08-01T12:00:01+00:00",
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
                "forecast_diagnostics": {
                    "signal": {"r5_pips": 1.2},
                    "blockers": ["spread_absolute"],
                },
                "path_samples": 99999,
                "microstructure_at_entry": {"order_book_available": 0.0},
            }
            store.observe_forecast(forecast, horizons=[300], track_outcome=False)
            store.flush(force=True)
            payload = json.loads(
                store.connection.execute(
                    "SELECT forecast_json FROM canonical_forecasts"
                ).fetchone()[0]
            )
            self.assertEqual(payload["storage_contract"], "compact_nonmaturing_forecast_v4")
            self.assertNotIn("forecast_diagnostics", payload)
            self.assertEqual(
                payload["rejection_diagnostics"]["blockers"],
                ["spread_absolute"],
            )
            self.assertNotIn("path_samples", payload)
            self.assertNotIn("microstructure_at_entry", payload)
            self.assertNotIn("instrument", payload)
            self.assertNotIn("entry_bid", payload)
            self.assertNotIn("model_version", payload)
            self.assertNotIn("feature_version", payload)
            self.assertNotIn("predicted_magnitude_pips", payload)
            self.assertNotIn("probability_up", payload)
            self.assertNotIn("remaining_horizons", payload)
            stored = store.connection.execute(
                "SELECT event_id,instrument,entry_bid,horizons_json,track_outcome,"
                "blocked_reason FROM canonical_forecasts"
            ).fetchone()
            self.assertEqual(
                stored,
                ("reject-1", "EUR_USD", 1.1, "[300]", 0, "spread_absolute"),
            )
            self.assertLess(len(json.dumps(payload, sort_keys=True)), 900)
            store.close()

    def test_maturing_forecast_drops_only_empty_placeholders_and_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            now = datetime.now(timezone.utc).isoformat()
            store.observe_forecast(
                {
                    "id": "compact-maturing",
                    "model_version": "model-v1",
                    "feature_version": "features-v1",
                    "data_cutoff_utc": now,
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "model_id": "momentum.fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "entry_time": now,
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                    "adverse_hits": {},
                    "favorable_hits": {},
                    "pattern_prediction": None,
                    "microstructure_at_entry": {
                        "order_book_available": 0.0,
                        "position_book_available": 0.0,
                        "bid_total_liquidity": 0.0,
                        "ask_total_liquidity": 0.0,
                        "quote_receive_age_sec": 0.12,
                        "live_spread_pips": 2.0,
                    },
                    "sma_filter": {
                        "ready": False,
                        "reason": "sma_filter_model_disabled",
                        "feature_count": 1501,
                    },
                },
                horizons=[300],
                track_outcome=True,
            )
            store.flush(force=True)
            payload_text = store.connection.execute(
                "SELECT forecast_json FROM canonical_forecasts"
            ).fetchone()[0]
            payload = json.loads(payload_text)
            self.assertEqual(payload["storage_contract"], "compact_maturing_forecast_v2")
            self.assertNotIn("adverse_hits", payload)
            self.assertNotIn("favorable_hits", payload)
            self.assertNotIn("pattern_prediction", payload)
            self.assertNotIn("sma_filter", payload)
            self.assertEqual(
                payload["microstructure_at_entry"],
                {"live_spread_pips": 2.0, "quote_receive_age_sec": 0.12},
            )
            recovered = store.recover_pending([300])
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0]["instrument"], "EUR_USD")
            self.assertEqual(recovered[0]["max_favorable_pips"], 0.0)
            self.assertLess(len(payload_text), 900)
            store.close()

    @staticmethod
    def payload() -> dict:
        return {
            "id": "event-1",
            "horizon_sec": 300,
            "lane_id": "momentum.fast",
            "family": "momentum",
            "profile": "fast",
            "kind": "signal",
            "instrument": "EUR_USD",
            "direction": "buy",
            "theoretical_pips": 1.2,
            "entry_bid": 1.1,
            "entry_ask": 1.1002,
            "exit_bid": 1.10032,
            "exit_ask": 1.10052,
            "pip": 0.0001,
            "entry_spread_pips": 2.0,
            "max_favorable_pips": 1.2,
            "max_adverse_pips": 0.5,
            "first_positive_sec": 12.5,
            "path_samples": 60,
            "positive_path_samples": 20,
        }

    def test_outcomes_are_buffered_and_committed_in_one_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            payload = self.payload()
            store.observe(payload)
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0],
                0,
            )
            self.assertEqual(store.flush(force=True), 1)
            store.observe(payload)
            self.assertEqual(store.flush(force=True), 0)
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0],
                1,
            )
            stored = store.connection.execute(
                "SELECT entry_spread_pips, first_positive_sec, positive_path_samples "
                "FROM outcomes"
            ).fetchone()
            self.assertEqual(stored, (2.0, 12.5, 20))
            store.close()

    def test_lock_contention_keeps_buffer_for_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(
                path,
                batch_size=64,
                flush_sec=60.0,
                busy_timeout_ms=100,
            )
            blocker = sqlite3.connect(path)
            blocker.execute("BEGIN IMMEDIATE")
            store.observe(self.payload())
            store.last_flush_monotonic -= 61.0

            self.assertEqual(store.flush(), 0)
            self.assertEqual(len(store.pending), 1)
            self.assertEqual(store.summary()["lock_retries"], 1)
            self.assertGreater(store.summary()["retry_after_sec"], 0.0)
            self.assertEqual(store.flush(), 0)
            self.assertEqual(store.summary()["lock_retries"], 1)

            blocker.rollback()
            blocker.close()
            self.assertEqual(store.flush(force=True), 1)
            store.close()

    def test_detail_sampling_is_stable(self):
        first = sampled_detail("event-1", 300, 0.25)
        self.assertEqual(first, sampled_detail("event-1", 300, 0.25))
        self.assertFalse(sampled_detail("event-1", 300, 0.0))
        self.assertTrue(sampled_detail("event-1", 300, 1.0))

    def test_canonical_forecast_and_outcome_are_immutable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            forecast = {
                "id": "forecast-1",
                "model_version": "model-v1",
                "feature_version": "features-v1",
                "data_cutoff_utc": "2026-08-01T12:00:00+00:00",
                "lane_id": "momentum.fast",
                "family": "momentum",
                "profile": "fast",
                "model_id": "momentum.fast",
                "kind": "signal",
                "instrument": "EUR_USD",
                "direction": "buy",
                "entry_time": "2026-08-01T12:00:01+00:00",
                "entry_bid": 1.1,
                "entry_ask": 1.1002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
                "remaining_horizons": [300],
            }
            store.observe_forecast(forecast, horizons=[300], track_outcome=True)
            outcome = self.payload()
            outcome["id"] = "forecast-1"
            store.observe(outcome)
            store.flush(force=True)
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM canonical_forecasts"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM canonical_outcomes"
                ).fetchone()[0],
                1,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                store.connection.execute(
                    "UPDATE canonical_forecasts SET family='changed' WHERE event_id='forecast-1'"
                )
            store.connection.rollback()
            store.close()

    def test_declared_recovery_keeps_only_nearest_horizon(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            now = datetime.now(timezone.utc).isoformat()
            store.observe_forecast(
                {
                    "id": "declared-recovery",
                    "model_version": "model-v1",
                    "feature_version": "features-v1",
                    "data_cutoff_utc": now,
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "model_id": "momentum.fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "entry_time": now,
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                    "signal_reference_horizon_sec": 900,
                },
                horizons=[60, 300, 900, 3600],
                track_outcome=True,
            )
            store.flush(force=True)
            recovered = store.recover_pending(
                [60, 300, 900, 3600],
                horizon_mode="declared",
            )
            self.assertEqual(recovered[0]["remaining_horizons"], [900])
            store.close()

    def test_recovery_uses_entry_time_queue_and_excludes_matured_horizon(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            now = datetime.now(timezone.utc).isoformat()
            store.observe_forecast(
                {
                    "id": "indexed-recovery",
                    "model_version": "model-v1",
                    "feature_version": "features-v1",
                    "data_cutoff_utc": now,
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "model_id": "momentum.fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "entry_time": now,
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                },
                horizons=[60, 300],
                track_outcome=True,
            )
            outcome = self.payload()
            outcome.update(
                {
                    "id": "indexed-recovery",
                    "horizon_sec": 60,
                    "entry_time": now,
                }
            )
            store.observe(outcome)
            store.flush(force=True)
            statements: list[str] = []
            store.connection.set_trace_callback(statements.append)

            recovered = store.recover_pending([60, 300], horizon_mode="all")

            store.connection.set_trace_callback(None)
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0]["remaining_horizons"], [300])
            self.assertTrue(
                any(
                    "INDEXED BY canonical_recovery_index_v2_entry_time" in statement
                    for statement in statements
                )
            )
            self.assertTrue(
                any("FROM canonical_outcomes" in statement for statement in statements)
            )
            self.assertTrue(any(statement == "BEGIN" for statement in statements))
            store.close()

    def test_recovery_uses_entry_time_when_causal_cutoff_precedes_h24_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            current = datetime.now(timezone.utc)
            entry = current - timedelta(seconds=86_400)
            cutoff = entry - timedelta(days=3, seconds=200)
            store.observe_forecast(
                {
                    "id": "h24-stale-causal-cutoff",
                    "model_version": "model-v1",
                    "feature_version": "features-v1",
                    "data_cutoff_utc": cutoff.isoformat(),
                    "lane_id": "momentum.h24",
                    "family": "momentum",
                    "profile": "h24",
                    "model_id": "momentum.h24",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "entry_time": entry.isoformat(),
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                },
                horizons=[86_400],
                track_outcome=True,
            )
            store.flush(force=True)
            store.close()
            # Emulate a pre-upgrade canonical row: no derived recovery index
            # and no migration marker exist yet.
            legacy = sqlite3.connect(path)
            legacy.executescript(
                """
                DROP TRIGGER IF EXISTS canonical_recovery_index_v2_no_update;
                DROP TRIGGER IF EXISTS canonical_recovery_index_v2_no_delete;
                DELETE FROM canonical_recovery_index_v2;
                DELETE FROM canonical_recovery_migrations;
                """
            )
            legacy.commit()
            legacy.close()
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)

            # A narrow first caller must still seed the fixed H24 migration
            # coverage; otherwise its global marker would omit this row.
            self.assertEqual(store.recover_pending([300]), [])
            recovered = store.recover_pending([86_400], horizon_mode="all")

            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0]["id"], "h24-stale-causal-cutoff")
            self.assertEqual(recovered[0]["remaining_horizons"], [86_400])
            migration = store.connection.execute(
                "SELECT migration_id FROM canonical_recovery_migrations"
            ).fetchone()
            self.assertEqual(
                migration[0], "canonical_recovery_entry_time_exact_v2_20260817"
            )
            store.close()

    def test_recovery_index_is_canonical_hash_bound_immutable_and_not_counted_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            store = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            now = datetime.now(timezone.utc).isoformat()
            baseline = {
                "id": "canonical-recovery-identity",
                "model_version": "model-v1",
                "feature_version": "features-v1",
                "data_cutoff_utc": now,
                "lane_id": "momentum.fast",
                "family": "momentum",
                "profile": "fast",
                "model_id": "momentum.fast",
                "kind": "signal",
                "instrument": "CAD_HKD",
                "direction": "buy",
                "entry_time": now,
                "entry_bid": 5.7,
                "entry_ask": 5.7002,
                "pip": 0.0001,
                "entry_spread_pips": 2.0,
            }
            store.observe_forecast(baseline, horizons=[300], track_outcome=True)
            store.flush(force=True)
            self.assertEqual(store.committed_forecasts, 1)

            conflicting = dict(baseline)
            conflicting.update({
                "family": "new_conflicting_family",
                "instrument": "EUR_USD",
                "direction": "sell",
            })
            store.observe_forecast(
                conflicting, horizons=[300], track_outcome=True
            )
            store.flush(force=True)
            self.assertEqual(store.committed_forecasts, 1)

            recovered = store.recover_pending([300])
            self.assertEqual(len(recovered), 1)
            self.assertEqual(recovered[0]["family"], "momentum")
            self.assertEqual(recovered[0]["instrument"], "CAD_HKD")
            self.assertEqual(recovered[0]["direction"], "buy")
            with self.assertRaises(sqlite3.IntegrityError):
                store.connection.execute(
                    """
                    UPDATE canonical_recovery_index_v2
                    SET entry_time='2099-01-01T00:00:00+00:00'
                    WHERE event_id='canonical-recovery-identity'
                    """
                )
            store.connection.rollback()
            store.close()

    def test_simultaneous_first_recovery_migrations_recheck_marker_under_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outcomes.sqlite"
            seed = ShadowOutcomeStore(path, batch_size=64, flush_sec=60.0)
            now = datetime.now(timezone.utc).isoformat()
            seed.observe_forecast(
                {
                    "id": "concurrent-legacy-recovery",
                    "model_version": "model-v1",
                    "feature_version": "features-v1",
                    "data_cutoff_utc": now,
                    "lane_id": "momentum.fast",
                    "family": "momentum",
                    "profile": "fast",
                    "model_id": "momentum.fast",
                    "kind": "signal",
                    "instrument": "EUR_USD",
                    "direction": "buy",
                    "entry_time": now,
                    "entry_bid": 1.1,
                    "entry_ask": 1.1002,
                    "pip": 0.0001,
                    "entry_spread_pips": 2.0,
                },
                horizons=[300],
                track_outcome=True,
            )
            seed.flush(force=True)
            seed.close()
            legacy = sqlite3.connect(path)
            legacy.executescript(
                """
                DROP TRIGGER IF EXISTS canonical_recovery_index_v2_no_update;
                DROP TRIGGER IF EXISTS canonical_recovery_index_v2_no_delete;
                DELETE FROM canonical_recovery_index_v2;
                DELETE FROM canonical_recovery_migrations;
                """
            )
            legacy.commit()
            legacy.close()

            barrier = threading.Barrier(2)
            failures: list[BaseException] = []
            recovered_counts: list[int] = []

            def recover() -> None:
                local = None
                try:
                    local = ShadowOutcomeStore(
                        path, batch_size=64, flush_sec=60.0,
                        busy_timeout_ms=250,
                    )
                    barrier.wait(timeout=5.0)
                    recovered_counts.append(len(local.recover_pending([300])))
                except BaseException as exc:
                    failures.append(exc)
                finally:
                    if local is not None:
                        local.close()

            threads = [threading.Thread(target=recover) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=15.0)

            self.assertEqual(failures, [])
            self.assertEqual(sorted(recovered_counts), [1, 1])
            audit = sqlite3.connect(path)
            marker_count = audit.execute(
                "SELECT COUNT(*) FROM canonical_recovery_migrations"
            ).fetchone()[0]
            audit.close()
            self.assertEqual(marker_count, 1)


if __name__ == "__main__":
    unittest.main()
