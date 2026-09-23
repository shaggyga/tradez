"""Synthetic worker boundary checks; never launches or reads live collectors."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import oanda_rolling_technical_worker_v1 as worker
from oanda_rolling_technical_store_v1 import TechnicalStore, SCHEMA
from test_oanda_rolling_technical_store_v1 import BASE, synthetic_rows, synthetic_features


class TechnicalWorkerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.output = self.root / "data" / "new_dataset"
        self.output.mkdir(parents=True)
        self.candles = self.root / "original_candles"
        self.candles.mkdir()
        currencies = ("AUD", "CAD", "CHF", "EUR", "GBP", "HKD", "JPY", "NZD", "SGD", "USD")
        pairs = {a + "_" + b: .0001 for a in currencies for b in currencies if a != b}
        self.config = {
            "schema": SCHEMA, "can_place_orders": False, "can_promote": False,
            "pairs": dict(list(pairs.items())[:68]), "source_bindings": {},
            "output_root": str(self.output), "candle_root": str(self.candles),
            "maximum_dataset_bytes": 64 * 1024 * 1024,
            "minimum_free_bytes": 0, "maximum_bar_age_seconds": 90,
            "tail_rows": 1024, "bootstrap_rows": 240, "interval_seconds": 60,
        }
        for name in worker.SOURCES:
            body = ("synthetic source " + name).encode()
            (self.root / name).write_bytes(body)
            self.config["source_bindings"][name] = hashlib.sha256(body).hexdigest()
        self.config_path = self.root / "synthetic_config.json"
        self.save_config()

    def tearDown(self):
        self.directory.cleanup()

    def save_config(self):
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")

    def test_valid_source_bound_68_pair_research_config(self):
        with mock.patch.object(worker, "ROOT", self.root):
            self.assertEqual(worker.read_config(self.config_path), self.config)

    def test_changed_source_or_incomplete_binding_refused(self):
        first = next(iter(self.config["source_bindings"]))
        with mock.patch.object(worker, "ROOT", self.root):
            (self.root / first).write_text("changed synthetic source", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "rolling_dataset_source_changed"):
                worker.read_config(self.config_path)
            del self.config["source_bindings"][first]
            self.save_config()
            with self.assertRaisesRegex(ValueError, "complete_source_bindings_required"):
                worker.read_config(self.config_path)

    def test_execution_flag_or_incomplete_universe_refused(self):
        with mock.patch.object(worker, "ROOT", self.root):
            self.config["can_place_orders"] = True
            self.save_config()
            with self.assertRaisesRegex(ValueError, "research_only_dataset_config_required"):
                worker.read_config(self.config_path)
            self.config["can_place_orders"] = False
            self.config["pairs"].pop(next(iter(self.config["pairs"])))
            self.save_config()
            with self.assertRaisesRegex(ValueError, "explicit_68_pair_pip_metadata_required"):
                worker.read_config(self.config_path)

    def test_second_writer_lock_refused_then_released(self):
        with worker.owner_lock(self.output):
            with self.assertRaises((RuntimeError, OSError)):
                with worker.owner_lock(self.output):
                    self.fail("second writer acquired the same output directory")
        with worker.owner_lock(self.output):
            pass

    def test_config_mutation_after_start_prevents_first_cycle(self):
        original_write = worker.atomic_json

        def mutate_after_registry(path, value):
            original_write(path, value)
            if Path(path).name == "feature_registry.json":
                self.config["interval_seconds"] = 61
                self.save_config()

        contract = {"feature_names": ["quiet", "unavailable"], "source_bindings": {}, "pairs": {"EUR_USD": .0001}}
        with (mock.patch.object(worker, "ROOT", self.root),
              mock.patch.object(worker, "contract", return_value=contract),
              mock.patch.object(worker, "atomic_json", side_effect=mutate_after_registry),
              mock.patch.object(worker, "run_cycle") as cycle):
            with self.assertRaisesRegex(ValueError, "running_dataset_config_changed"):
                worker.main(["--config", str(self.config_path), "--once"])
            cycle.assert_not_called()
        status = json.loads((self.output / "status.json").read_text("utf-8"))
        self.assertEqual(status["status"], "error")
        self.assertFalse(status["can_place_orders"])

    def test_stale_and_source_unavailable_values_hidden_in_fresh_envelope(self):
        pair_names = ("EUR_USD", "GBP_USD", "AUD_USD")
        config = {**self.config, "pairs": dict.fromkeys(pair_names, .0001)}
        contract = {"feature_names": ["quiet", "unavailable"], "source_bindings": {}, "pairs": config["pairs"]}
        store = TechnicalStore(self.output / "only_fixture.sqlite", contract)
        cache = {}
        try:
            for index, pair in enumerate(pair_names):
                data = synthetic_rows([100, 100])
                if pair == "GBP_USD":
                    data["time"] -= 3_600
                store.ingest(pair, data, synthetic_features(2),
                    {"observed_epoch": BASE + 125, "pair": pair}, published_epoch=BASE + 126)
                if pair != "AUD_USD":
                    source = self.candles / (pair + "_M1.csv")
                    source.write_text("not parsed: the source identity is unchanged", encoding="utf-8")
                    stat = source.stat()
                    cache[pair] = (stat.st_size, stat.st_mtime_ns, stat.st_ino)
            registry = [{"name": "quiet", "family": "synthetic"}, {"name": "unavailable", "family": "synthetic"}]
            with (mock.patch.object(worker.kernel, "feature_registry", return_value=registry),
                  mock.patch.object(worker.time, "time", return_value=BASE + 130),
                  mock.patch.object(worker, "read_pair") as reader):
                report = worker.run_cycle(config, store, cache)
                reader.assert_not_called()
            latest = json.loads((self.output / "latest_features.json").read_text("utf-8"))
            self.assertTrue(latest["envelope_time_is_not_feature_freshness"])
            self.assertEqual(report["pairs"]["EUR_USD"]["status"], "current")
            self.assertEqual(report["pairs"]["GBP_USD"]["status"], "stale")
            self.assertEqual(report["pairs"]["AUD_USD"]["status"], "unavailable")
            self.assertIsNotNone(latest["pairs"]["EUR_USD"]["observation"])
            self.assertIsNone(latest["pairs"]["GBP_USD"]["observation"])
            self.assertIsNone(latest["pairs"]["AUD_USD"]["observation"])
            self.assertEqual(report["current_pairs"], 1)
            self.assertEqual(report["peer_panel"]["accepted_clock_pair_count"], 1)
        finally:
            store.close()

    def run_resume_fixture(self, source_offsets, *, expected_reads, old_count=603, range_truncated=True):
        pair = "EUR_USD"
        config = {**self.config, "pairs": {pair: .0001}}
        contract = {"feature_names": ["quiet", "unavailable"], "source_bindings": {}, "pairs": config["pairs"]}
        store = TechnicalStore(self.output / "resume_fixture.sqlite", contract)
        old = synthetic_rows([100] * old_count)
        old_observed = float(old["time"][-1] + 65)
        store.ingest(pair, old, synthetic_features(old_count), {"observed_epoch": old_observed},
                     published_epoch=old_observed + 1)
        (self.candles / (pair + "_M1.csv")).write_text("bounded synthetic read is injected", encoding="utf-8")
        incoming = [synthetic_rows([100] * len(offsets), offsets=offsets) for offsets in source_offsets]
        now = float(max(old["time"][-1], max(data["time"][-1] for data in incoming)) + 80)
        reads = [(data, {"observed_epoch": now - 1, "pair": pair, "range_truncated": range_truncated}) for data in incoming]
        registry = [{"name": "quiet", "family": "synthetic"}, {"name": "unavailable", "family": "synthetic"}]
        try:
            with (mock.patch.object(worker.kernel, "feature_registry", return_value=registry),
                  mock.patch.object(worker.kernel, "max_lookback_bars", return_value=603),
                  mock.patch.object(worker.kernel, "compute_features",
                                    side_effect=lambda data, pair, pip: synthetic_features(len(data["time"]))) as compute,
                  mock.patch.object(worker.time, "time", return_value=now),
                  mock.patch.object(worker, "read_pair", side_effect=reads) as reader):
                report = worker.run_cycle(config, store, {})
                self.assertEqual(reader.call_count, expected_reads)
                calls = [call.args[-1] for call in reader.call_args_list]
                computed_rows = [len(call.args[0]["time"]) for call in compute.call_args_list]
            latest = json.loads((self.output / "latest_features.json").read_text("utf-8"))
            return report, latest, store.latest_time(pair), calls, computed_rows
        finally:
            store.close()

    def test_source_latest_bar_regression_refused_without_republishing_old_value(self):
        report, latest, last, calls, computed = self.run_resume_fixture([range(602)], expected_reads=1)
        self.assertIn("source_latest_bar_regressed", report["pairs"]["EUR_USD"]["reason"])
        self.assertEqual(last, BASE + 602 * 60)
        self.assertEqual(computed, [])
        self.assertIsNone(latest["pairs"]["EUR_USD"]["observation"])

    def test_resume_extends_source_tail_to_cover_first_new_feature_warmup(self):
        report, latest, last, calls, computed = self.run_resume_fixture(
            [range(400, 1004), range(1004)], expected_reads=2)
        self.assertEqual(calls, [1024, 1626])
        self.assertEqual(computed, [1004])
        self.assertEqual(last, BASE + 1003 * 60)
        self.assertEqual(report["inserted_this_cycle"], 401)
        self.assertEqual(report["pairs"]["EUR_USD"]["status"], "current")
        self.assertIsNotNone(latest["pairs"]["EUR_USD"]["observation"])

    def test_resume_refuses_when_extended_tail_still_lacks_warmup(self):
        report, latest, last, calls, computed = self.run_resume_fixture(
            [range(400, 1004), range(400, 1004)], expected_reads=2)
        self.assertIn("backlog_context_exceeds_tail", report["pairs"]["EUR_USD"]["reason"])
        self.assertEqual(calls, [1024, 1626])
        self.assertEqual(computed, [])
        self.assertEqual(last, BASE + 602 * 60)
        self.assertIsNone(latest["pairs"]["EUR_USD"]["observation"])

    def test_resume_refuses_gap_when_entire_previous_boundary_is_outside_tail(self):
        report, latest, last, calls, computed = self.run_resume_fixture([range(1000, 1006)], expected_reads=1)
        self.assertIn("backlog_exceeds_tail", report["pairs"]["EUR_USD"]["reason"])
        self.assertEqual(last, BASE + 602 * 60)
        self.assertEqual(computed, [])
        self.assertIsNone(latest["pairs"]["EUR_USD"]["observation"])

    def test_full_short_source_retains_legitimate_warmup_rows(self):
        report, latest, last, calls, computed = self.run_resume_fixture(
            [range(6)], expected_reads=1, old_count=3, range_truncated=False)
        self.assertEqual(calls, [1024])
        self.assertEqual(computed, [6])
        self.assertEqual(last, BASE + 5 * 60)
        self.assertEqual(report["inserted_this_cycle"], 3)
        self.assertEqual(report["pairs"]["EUR_USD"]["status"], "current")
        self.assertEqual(latest["pairs"]["EUR_USD"]["observation"]["values"]["unavailable"], None)

    def test_future_bar_skip_is_reconsidered_without_source_identity_change(self):
        pair = "EUR_USD"
        config = {**self.config, "pairs": {pair: .0001}}
        contract = {"feature_names": ["quiet", "unavailable"], "pairs": config["pairs"]}
        store = TechnicalStore(self.output / "future_bar_fixture.sqlite", contract)
        (self.candles / (pair + "_M1.csv")).write_text("same file bytes in both cycles", encoding="utf-8")
        clock = [float(BASE + 200)]
        reads = [
            (synthetic_rows([100] * 3), {"observed_epoch": BASE + 190, "range_truncated": False,
                                      "skipped_rows": {"bar_end_after_observation": 1}}),
            (synthetic_rows([100] * 4), {"observed_epoch": BASE + 250, "range_truncated": False,
                                      "skipped_rows": {}}),
        ]
        registry = [{"name": "quiet", "family": "synthetic"}, {"name": "unavailable", "family": "synthetic"}]
        cache = {}
        try:
            with (mock.patch.object(worker.kernel, "feature_registry", return_value=registry),
                  mock.patch.object(worker.kernel, "max_lookback_bars", return_value=603),
                  mock.patch.object(worker.kernel, "compute_features",
                                    side_effect=lambda data, pair, pip: synthetic_features(len(data["time"]))),
                  mock.patch.object(worker.time, "time", side_effect=lambda: clock[0]),
                  mock.patch.object(worker, "read_pair", side_effect=reads) as reader):
                first = worker.run_cycle(config, store, cache)
                self.assertEqual(first["inserted_this_cycle"], 3)
                self.assertNotIn(pair, cache)
                clock[0] = float(BASE + 260)
                second = worker.run_cycle(config, store, cache)
                self.assertEqual(second["inserted_this_cycle"], 1)
                self.assertEqual(reader.call_count, 2)
                self.assertIn(pair, cache)
                self.assertEqual(store.latest_time(pair), BASE + 180)
        finally:
            store.close()

    def test_pair_aging_out_during_cycle_is_hidden_at_envelope_publication(self):
        pair = "EUR_USD"
        config = {**self.config, "pairs": {pair: .0001}}
        contract = {"feature_names": ["quiet", "unavailable"], "pairs": config["pairs"]}
        store = TechnicalStore(self.output / "aging_fixture.sqlite", contract)
        store.ingest(pair, synthetic_rows([100] * 2), synthetic_features(2),
                     {"observed_epoch": BASE + 125}, published_epoch=BASE + 126)
        source = self.candles / (pair + "_M1.csv")
        source.write_text("unchanged source", encoding="utf-8")
        stat = source.stat()
        cache = {pair: (stat.st_size, stat.st_mtime_ns, stat.st_ino)}
        samples = iter([BASE + 130, BASE + 130, BASE + 300])
        registry = [{"name": "quiet", "family": "synthetic"}, {"name": "unavailable", "family": "synthetic"}]
        try:
            with (mock.patch.object(worker.kernel, "feature_registry", return_value=registry),
                  mock.patch.object(worker.time, "time", side_effect=lambda: next(samples, BASE + 300))):
                report = worker.run_cycle(config, store, cache)
            self.assertEqual(report["pairs"][pair]["status"], "stale")
            self.assertEqual(report["current_pairs"], 0)
            self.assertIsNone(report["peer_panel"])
            latest = json.loads((self.output / "latest_features.json").read_text("utf-8"))
            self.assertIsNone(latest["pairs"][pair]["observation"])
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
