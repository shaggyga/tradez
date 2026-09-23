"""Independent synthetic checks of the additive technical dataset store.

No feature kernel, live input, broker, collector or existing research ledger is
opened. All database files belong to unittest temporary directories.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import zlib

import numpy as np

from oanda_rolling_technical_store_v1 import TechnicalStore, RevisionDetected


BASE = 1_700_000_040  # Exactly aligned UTC minute start.
PAIR = "EUR_USD"


def synthetic_rows(prices, offsets=None):
    close = np.asarray(prices, dtype=np.float64)
    offsets = np.arange(len(close)) if offsets is None else np.asarray(offsets)
    return {
        "time": BASE + offsets.astype(np.int64) * 60,
        "open": close.copy(), "high": close + .05,
        "low": close - .05, "close": close.copy(),
        "bid_close": close - .1, "ask_close": close + .1,
        "volume": np.ones(len(close), dtype=np.float64) * 10,
    }


def synthetic_features(count):
    return {"quiet": np.zeros(count), "unavailable": np.full(count, np.nan)}


class TechnicalStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "new_research.sqlite"
        self.contract = {
            "feature_names": ["quiet", "unavailable"],
            "source_bindings": {"synthetic_kernel": "version-one"},
            "pairs": {PAIR: .0001, "GBP_USD": .0001},
        }
        self.store = TechnicalStore(self.path, self.contract)

    def tearDown(self):
        if self.store is not None:
            self.store.close()
        self.directory.cleanup()

    def append(self, data, *, pair=PAIR, observed=None, keep_from=None):
        observed = float(data["time"][-1] + 65) if observed is None else observed
        receipt = {"observed_epoch": observed, "fixture": "synthetic-only"}
        self.store.ingest(pair, data, synthetic_features(len(data["time"])),
                          receipt, keep_from=keep_from, published_epoch=observed + 1)
        return observed + 2

    def outcome(self, t=BASE, h=5):
        row = self.store.connection.execute(
            "SELECT state,body,resolved FROM outcomes WHERE pair=? AND t=? AND horizon=?",
            (PAIR, t, h)).fetchone()
        return None if row is None else (row["state"], json.loads(row["body"]), row["resolved"])

    def test_up_move_long_short_bidask_endpoint_arithmetic(self):
        now = self.append(synthetic_rows([100, 101, 102, 103, 104, 105]))
        self.assertEqual(self.store.settle(PAIR, now_epoch=now), 1)
        state, body, _ = self.outcome()
        self.assertEqual(state, "available")
        self.assertEqual(body["direction"], 1)
        self.assertAlmostEqual(body["return_bps"], 500)
        self.assertAlmostEqual(body["long_net_bps"], 480)
        self.assertAlmostEqual(body["short_net_bps"], -520)
        self.assertIn("no_fills_or_slippage", body["outcome_scope"])
        self.assertEqual(body["target_bar_end_epoch"], BASE + 6 * 60)

    def test_down_move_short_costs_and_close_only_excursions(self):
        now = self.append(synthetic_rows([100, 102, 99, 101, 98, 95]))
        self.store.settle(PAIR, now_epoch=now)
        _, body, _ = self.outcome()
        self.assertEqual(body["direction"], -1)
        self.assertAlmostEqual(body["long_net_bps"], -520)
        self.assertAlmostEqual(body["short_net_bps"], 480)
        self.assertAlmostEqual(body["max_favorable_long_close_bps"], 180)
        self.assertAlmostEqual(body["max_adverse_long_close_bps"], -520)
        self.assertAlmostEqual(body["max_favorable_short_close_bps"], 480)
        self.assertAlmostEqual(body["max_adverse_short_close_bps"], -220)

    def test_quiet_rows_and_unavailable_features_are_retained(self):
        now = self.append(synthetic_rows([100] * 6))
        self.store.settle(PAIR, now_epoch=now)
        latest = self.store.latest(PAIR)
        self.assertEqual(latest["values"], {"quiet": 0.0, "unavailable": None})
        self.assertEqual(latest["available_features"], 1)
        self.assertEqual(latest["feature_count"], 2)
        self.assertEqual(self.store.counts()["observations"], 6)
        _, body, _ = self.outcome()
        self.assertEqual(body["direction"], 0)
        self.assertEqual(body["absolute_return_bps"], 0)
        self.assertAlmostEqual(body["long_net_bps"], -20)
        self.assertAlmostEqual(body["short_net_bps"], -20)

    def test_pending_outcome_waits_for_completed_target_bar(self):
        now = self.append(synthetic_rows([100] * 5))
        self.assertEqual(self.store.settle(PAIR, now_epoch=now), 0)
        self.assertIsNone(self.outcome())
        now = self.append(synthetic_rows([100] * 6))
        self.assertEqual(self.store.settle(PAIR, now_epoch=now), 1)
        self.assertEqual(self.outcome()[0], "available")

    def test_missing_intermediate_minute_is_not_silently_filled(self):
        now = self.append(synthetic_rows([100] * 5, offsets=[0, 1, 3, 4, 5]))
        self.store.settle(PAIR, now_epoch=now)
        state, body, _ = self.outcome()
        self.assertEqual(state, "gap_in_path")
        self.assertEqual(body["path_missing_bars"], 1)
        self.assertNotIn("long_net_bps", body)
        self.assertNotIn("direction", body)

    def test_missing_target_is_terminal_and_not_rewritten_by_later_recovery(self):
        now = self.append(synthetic_rows([100] * 6, offsets=[0, 1, 2, 3, 4, 6]))
        self.store.settle(PAIR, now_epoch=now)
        frozen = self.outcome()
        self.assertEqual(frozen[0], "missing_target")
        later = self.append(synthetic_rows([100] * 7), observed=now + 100, keep_from=BASE + 7*60)
        self.store.settle(PAIR, now_epoch=later)
        self.assertEqual(self.outcome(), frozen)

    def test_delayed_source_keeps_actual_availability_separate(self):
        data = synthetic_rows([100] * 6)
        observed = float(BASE + 10_000)
        now = self.append(data, observed=observed)
        self.store.settle(PAIR, now_epoch=now)
        _, body, resolved = self.outcome()
        self.assertEqual(body["outcome_inputs_observed_epoch"], observed)
        self.assertGreaterEqual(resolved, observed)
        self.assertLess(body["target_bar_end_epoch"], observed)
        self.assertIn("retrospective", body["availability_basis"])
        self.assertEqual(self.store.latest(PAIR)["published_epoch"], observed + 1)

    def test_duplicate_ingest_preserves_observation_and_first_availability(self):
        data = synthetic_rows([100] * 6)
        now = self.append(data)
        self.store.settle(PAIR, now_epoch=now)
        frozen = self.store.latest(PAIR)
        first_seen = self.store.connection.execute(
            "SELECT first_observed FROM bars WHERE pair=? AND t=?", (PAIR, BASE)).fetchone()[0]
        self.append(data, observed=now + 1_000)
        self.assertEqual(self.store.latest(PAIR), frozen)
        self.assertEqual(self.store.counts()["bars"], 6)
        self.assertEqual(self.store.counts()["observations"], 6)
        self.assertEqual(self.store.settle(PAIR, now_epoch=now + 1_002), 0)
        self.assertEqual(self.store.connection.execute(
            "SELECT first_observed FROM bars WHERE pair=? AND t=?", (PAIR, BASE)).fetchone()[0], first_seen)

    def test_changed_input_is_recorded_and_latches_pair(self):
        data = synthetic_rows([100] * 6)
        now = self.append(data)
        frozen = self.store.latest(PAIR)
        changed = {k: v.copy() for k, v in data.items()}
        changed["close"][2] += .01
        with self.assertRaisesRegex(RevisionDetected, "original_input_revision_detected"):
            self.append(changed, observed=now + 10)
        self.assertEqual(self.store.latest(PAIR), frozen)
        self.assertEqual(self.store.counts()["revisions"], 1)
        with self.assertRaisesRegex(RevisionDetected, "unresolved_original_input_revision"):
            self.append(synthetic_rows([100] * 7), observed=now + 100)
        self.assertEqual(self.store.counts()["bars"], 6)

    def test_warmup_inputs_remain_revision_checked_without_observation(self):
        data = synthetic_rows([100] * 6)
        now = self.append(data, keep_from=BASE + 5 * 60)
        self.assertEqual(self.store.counts()["bars"], 6)
        self.assertEqual(self.store.counts()["observations"], 1)
        data["volume"][0] += 1
        with self.assertRaises(RevisionDetected):
            self.append(data, observed=now + 100, keep_from=BASE + 5 * 60)

    def test_restart_matches_contract_and_refuses_changed_kernel(self):
        self.append(synthetic_rows([100] * 6))
        frozen = self.store.latest(PAIR)
        self.store.close()
        self.store = None
        resumed = TechnicalStore(self.path, copy.deepcopy(self.contract))
        self.assertEqual(resumed.latest(PAIR), frozen)
        resumed.close()
        changed = copy.deepcopy(self.contract)
        changed["source_bindings"]["synthetic_kernel"] = "version-two"
        with self.assertRaisesRegex(ValueError, "dataset_contract_changed"):
            TechnicalStore(self.path, changed)

    def test_schema_alignment_and_incomplete_bar_refuse_before_rows(self):
        data = synthetic_rows([100, 100])
        with self.assertRaisesRegex(ValueError, "ordered_feature_schema_mismatch"):
            self.store.ingest(PAIR, data, {"unavailable": np.zeros(2), "quiet": np.zeros(2)},
                              {"observed_epoch": BASE + 130}, published_epoch=BASE + 131)
        with self.assertRaisesRegex(ValueError, "unaligned"):
            self.store.ingest(PAIR, data, {"quiet": np.zeros(1), "unavailable": np.zeros(2)},
                              {"observed_epoch": BASE + 130}, published_epoch=BASE + 131)
        with self.assertRaisesRegex(ValueError, "incomplete_bar"):
            self.store.ingest(PAIR, data, synthetic_features(2),
                              {"observed_epoch": BASE + 119}, published_epoch=BASE + 120)
        self.assertEqual(self.store.counts()["bars"], 0)
        self.assertEqual(self.store.counts()["observations"], 0)

    def test_size_bound_refuses_crossing_batch_without_partial_rows(self):
        # One spare byte is less than one SQLite page. The bound must apply to
        # the committing batch, not just its size before admission.
        self.store.max_bytes = self.store.size_bytes() + 1
        before = self.store.counts()
        with self.assertRaisesRegex(ValueError, "storage_bound"):
            self.append(synthetic_rows([100] * 6))
        self.assertEqual(self.store.counts(), before)

    def test_removed_overlapping_bar_latches_pair_without_erasing_original(self):
        data = synthetic_rows([100] * 6)
        now = self.append(data)
        rewritten = {key: value[[0, 1, 3, 4, 5]] for key, value in data.items()}
        with self.assertRaisesRegex(RevisionDetected, "original_input_revision_detected"):
            self.append(rewritten, observed=now + 100)
        self.assertEqual(self.store.counts()["bars"], 6)
        self.assertEqual(self.store.counts()["observations"], 6)
        revision = self.store.connection.execute(
            "SELECT t FROM revisions WHERE pair=?", (PAIR,)).fetchone()
        self.assertEqual(revision["t"], BASE + 120)
        with self.assertRaisesRegex(RevisionDetected, "unresolved_original_input_revision"):
            self.append(data, observed=now + 200)

    def test_rolling_tail_dropping_prefix_is_not_a_deleted_overlap(self):
        data = synthetic_rows([100] * 6)
        now = self.append(data)
        later = synthetic_rows([100] * 6, offsets=[3, 4, 5, 6, 7, 8])
        self.append(later, observed=now + 300)
        self.assertEqual(self.store.counts()["revisions"], 0)
        self.assertEqual(self.store.counts()["bars"], 9)

    def test_publication_clock_is_sampled_after_values_commit(self):
        data = synthetic_rows([100] * 6)
        observed = float(BASE + 1_000)
        samples = []

        def clock():
            samples.append((self.store.counts()["observations"], self.store.connection.in_transaction))
            return observed + len(samples)

        with mock.patch("oanda_rolling_technical_store_v1.time.time", side_effect=clock):
            self.store.ingest(PAIR, data, synthetic_features(6), {"observed_epoch": observed})
        self.assertEqual(samples, [(0, False), (6, False)])
        self.assertEqual(self.store.latest(PAIR)["published_epoch"], observed + 2)

    def crash_before_publication_marker(self, data):
        connection = self.store.connection

        class FailPublication:
            def __getattr__(self, name):
                return getattr(connection, name)

            def __enter__(self):
                connection.__enter__()
                return self

            def __exit__(self, *args):
                return connection.__exit__(*args)

            def executemany(self, statement, values):
                if statement.startswith("UPDATE observations SET published="):
                    raise RuntimeError("synthetic_crash_after_values_commit")
                return connection.executemany(statement, values)

        self.store.connection = FailPublication()
        try:
            with self.assertRaisesRegex(RuntimeError, "synthetic_crash_after_values_commit"):
                self.append(data)
        finally:
            self.store.connection = connection

    def test_crash_committed_rows_hidden_and_identical_restart_recovers(self):
        data = synthetic_rows([100] * 6)
        self.crash_before_publication_marker(data)
        self.assertEqual(self.store.counts()["observations"], 6)
        self.assertIsNone(self.store.latest(PAIR))
        self.assertIsNone(self.store.latest_time(PAIR))
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 10_000), 0)
        self.store.close()
        self.store = TechnicalStore(self.path, self.contract)
        self.assertIsNone(self.store.latest(PAIR))
        now = self.append(data, observed=BASE + 20_000)
        self.assertEqual(self.store.counts()["observations"], 6)
        self.assertEqual(self.store.latest(PAIR)["published_epoch"], BASE + 20_001)
        self.assertEqual(self.store.settle(PAIR, now_epoch=now), 1)

    def test_crash_recovery_refuses_changed_feature_context(self):
        data = synthetic_rows([100] * 6)
        self.crash_before_publication_marker(data)
        changed = synthetic_features(6)
        changed["quiet"][0] = 1
        with self.assertRaisesRegex(ValueError, "unpublished_observation_input_context_changed"):
            self.store.ingest(PAIR, data, changed, {"observed_epoch": BASE + 20_000},
                              published_epoch=BASE + 20_001)
        self.assertIsNone(self.store.latest(PAIR))
        self.assertEqual(self.store.counts()["observations"], 6)

    def test_postcommit_clock_rollback_leaves_observations_unpublished(self):
        data = synthetic_rows([100] * 6)
        observed = float(BASE + 1_000)
        with mock.patch("oanda_rolling_technical_store_v1.time.time", side_effect=[observed + 1, observed - 1]):
            with self.assertRaises(ValueError):
                self.store.ingest(PAIR, data, synthetic_features(6), {"observed_epoch": observed})
        self.assertIsNone(self.store.latest(PAIR))
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 20_000), 0)

    def test_panel_duplicate_publication_keeps_original_clock_and_bound_sources(self):
        self.append(synthetic_rows([100] * 6))
        observation = self.store.latest(PAIR)
        panel = {"target_bar_start_epoch": BASE + 300, "synthetic_peer_value": 2.0}
        with mock.patch("oanda_rolling_technical_store_v1.time.time", return_value=BASE + 10_000):
            original = self.store.publish_panel(panel, {PAIR: observation})
        with mock.patch("oanda_rolling_technical_store_v1.time.time", return_value=BASE + 20_000):
            repeated = self.store.publish_panel(panel, {PAIR: observation})
        self.assertEqual(repeated, original)
        self.assertEqual(self.store.counts()["panels"], 1)
        row = self.store.connection.execute("SELECT body FROM panels WHERE id=?", (original["id"],)).fetchone()
        preserved = json.loads(zlib.decompress(row["body"]))
        self.assertEqual(preserved["panel"], panel)
        self.assertEqual(preserved["source_observations"][PAIR]["feature_hash"], observation["feature_hash"])
        self.assertEqual(preserved["source_observations"][PAIR]["published_epoch"], observation["published_epoch"])

    def test_late_peer_arrival_is_new_panel_generation_without_rewriting_first(self):
        data = synthetic_rows([100] * 6)
        self.append(data)
        first_observation = self.store.latest(PAIR)
        panel = {"target_bar_start_epoch": BASE + 300, "synthetic_peer_value": None}
        first = self.store.publish_panel(panel, {PAIR: first_observation, "GBP_USD": None})
        self.append(data, pair="GBP_USD", observed=BASE + 1_000)
        second_panel = {"target_bar_start_epoch": BASE + 300, "synthetic_peer_value": 1.0}
        second = self.store.publish_panel(second_panel,
            {PAIR: first_observation, "GBP_USD": self.store.latest("GBP_USD")})
        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual(self.store.counts()["panels"], 2)
        retained = self.store.connection.execute("SELECT published,body FROM panels WHERE id=?", (first["id"],)).fetchone()
        self.assertEqual(retained["published"], first["published_epoch"])
        self.assertIsNone(json.loads(zlib.decompress(retained["body"]))["panel"]["synthetic_peer_value"])

    def test_panel_refuses_unknown_tampered_or_unpublished_source(self):
        data = synthetic_rows([100] * 6)
        self.append(data)
        observation = self.store.latest(PAIR)
        panel = {"target_bar_start_epoch": BASE + 300}
        for altered in (
                {**observation, "feature_hash": "0" * 64},
                {**observation, "published_epoch": observation["published_epoch"] + 1},
                {**observation, "bar_start_epoch": BASE + 30_000}):
            with self.assertRaisesRegex(ValueError, "panel_source_not_published_observation"):
                self.store.publish_panel(panel, {PAIR: altered})
        self.store.connection.execute("UPDATE observations SET published=0 WHERE pair=?", (PAIR,))
        self.store.connection.commit()
        with self.assertRaisesRegex(ValueError, "panel_source_not_published_observation"):
            self.store.publish_panel(panel, {PAIR: observation})
        self.assertEqual(self.store.counts()["panels"], 0)

    def test_panel_clock_cannot_precede_its_source_publication(self):
        self.append(synthetic_rows([100] * 6))
        observation = self.store.latest(PAIR)
        panel = {"target_bar_start_epoch": BASE + 300}
        with mock.patch("oanda_rolling_technical_store_v1.time.time", return_value=observation["published_epoch"] - 1):
            with self.assertRaises(ValueError):
                self.store.publish_panel(panel, {PAIR: observation})
        visible = self.store.connection.execute("SELECT COUNT(*) FROM panels WHERE published>0").fetchone()[0]
        self.assertEqual(visible, 0)

    def test_settlement_waits_until_observation_publication_time(self):
        self.append(synthetic_rows([100] * 6), observed=BASE + 10_000)
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 10_000.5), 0)
        self.assertIsNone(self.outcome())
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 10_001.5), 1)
        self.assertEqual(self.outcome()[0], "available")

    def test_settlement_waits_for_delayed_target_actual_first_observation(self):
        self.append(synthetic_rows([100] * 5), observed=BASE + 365)
        self.append(synthetic_rows([100] * 6), observed=BASE + 1_000, keep_from=BASE + 99_000)
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 900), 0)
        self.assertIsNone(self.outcome())
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 1_002), 1)
        state, body, resolved = self.outcome()
        self.assertEqual(state, "available")
        self.assertEqual(body["outcome_inputs_observed_epoch"], BASE + 1_000)
        self.assertGreaterEqual(resolved, body["outcome_inputs_observed_epoch"])

    def test_missing_target_cannot_use_a_future_observed_high_watermark(self):
        self.append(synthetic_rows([100] * 5), observed=BASE + 365)
        self.append(synthetic_rows([100] * 6, offsets=[0, 1, 2, 3, 4, 6]),
                    observed=BASE + 1_000, keep_from=BASE + 99_000)
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 900), 0)
        self.assertIsNone(self.outcome())
        self.store.settle(PAIR, now_epoch=BASE + 1_002)
        self.assertEqual(self.outcome()[0], "missing_target")

    def test_future_observed_recovered_intermediate_bar_leaves_outcome_pending(self):
        self.append(synthetic_rows([100] * 5, offsets=[0, 1, 3, 4, 5]), observed=BASE + 365)
        self.append(synthetic_rows([100] * 6), observed=BASE + 1_000, keep_from=BASE + 99_000)
        # The target was already observed at +365, so a target-only/watermark
        # check cannot detect the later availability of the recovered middle.
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 900), 0)
        self.assertIsNone(self.outcome())
        self.assertEqual(self.store.settle(PAIR, now_epoch=BASE + 1_002), 1)
        self.assertEqual(self.outcome()[0], "available")
        self.assertEqual(self.outcome()[1]["outcome_inputs_observed_epoch"], BASE + 1_000)

    def test_revision_latch_blocks_new_settlement_without_rewriting_old_outcomes(self):
        data = synthetic_rows([100] * 6)
        now = self.append(data)
        self.store.settle(PAIR, now_epoch=now)
        original = self.outcome()
        data["volume"][2] += 1
        with self.assertRaises(RevisionDetected):
            self.append(data, observed=now + 100)
        with self.assertRaisesRegex(RevisionDetected, "unresolved_original_input_revision"):
            self.store.settle(PAIR, now_epoch=now + 101)
        self.assertEqual(self.outcome(), original)


if __name__ == "__main__":
    unittest.main()
