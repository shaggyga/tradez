"""Meaningful synthetic boundary tests; no live files or processes are changed."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import numpy as np

import oanda_rolling_technical_worker_v2 as worker
from oanda_rolling_technical_store_v1 import TechnicalStore, RevisionDetected

BASE = 1700000040


def candles(n=2300, *, gap=None, flat=False):
    rng = np.random.default_rng(7102)
    close = np.full(n, 1.12) if flat else 1.12 + np.cumsum(rng.normal(0, .00004, n))
    clocks = BASE + np.arange(n, dtype=np.int64) * 60
    if gap is not None:
        clocks[gap:] += 60
    return {'time': clocks, 'open': close.copy(), 'high': close + .00003,
            'low': close - .00003, 'close': close.copy(),
            'bid_close': close - .00008, 'ask_close': close + .00008,
            'volume': rng.integers(0, 150, n).astype(float)}


def subset(data, start=None, end=None):
    return {key: values[start:end] for key, values in data.items()}


class IncrementalComputationTests(unittest.TestCase):
    def assert_same(self, expected, actual, start):
        self.assertEqual(tuple(expected), tuple(actual))
        for name in expected:
            left, right = expected[name][start:], actual[name][start:]
            self.assertTrue(np.array_equal(np.isnan(left), np.isnan(right)), name)
            finite = np.isfinite(left)
            self.assertTrue(np.array_equal(left[finite].view(np.uint64), right[finite].view(np.uint64)), name)

    def test_single_new_minute_exact_float_bits_with_full_602_bar_context(self):
        data = candles()
        expected = worker.original.kernel.compute_features(data, 'EUR_USD', .0001)
        actual, info = worker.incremental_features(data, 'EUR_USD', .0001, data['time'][-2])
        self.assert_same(expected, actual, len(data['time']) - 1)
        self.assertEqual(info['computed_rows'], worker.original.kernel.max_lookback_bars())
        self.assertEqual(info['new_rows'], 1)

    def test_multiple_new_minutes_exact_bits_across_gap_and_flat_inputs(self):
        for data in (candles(gap=2250), candles(flat=True)):
            with self.subTest(gap=bool(np.any(np.diff(data['time']) != 60))):
                expected = worker.original.kernel.compute_features(data, 'EUR_USD', .0001)
                actual, info = worker.incremental_features(data, 'EUR_USD', .0001, data['time'][-40])
                self.assert_same(expected, actual, len(data['time']) - 39)
                self.assertEqual(info['new_rows'], 39)

    def test_bootstrap_retains_entire_feature_population(self):
        data = candles(700)
        full = worker.original.kernel.compute_features(data, 'EUR_USD', .0001)
        actual, info = worker.incremental_features(data, 'EUR_USD', .0001, None)
        self.assert_same(full, actual, 0)
        self.assertEqual(info['computed_rows'], 700)

    def test_unchanged_bar_performs_no_kernel_work(self):
        data = candles(700)
        with mock.patch.object(worker.original.kernel, 'compute_features') as calculate:
            actual, info = worker.incremental_features(data, 'EUR_USD', .0001, data['time'][-1])
        calculate.assert_not_called()
        self.assertEqual(info['computed_rows'], 0)
        self.assertTrue(all(np.isnan(values).all() for values in actual.values()))

    def test_revised_consumed_prefix_still_refused_even_outside_computed_suffix(self):
        with tempfile.TemporaryDirectory() as directory:
            data = candles(2300)
            old = subset(data, end=2299)
            old_features = worker.original.kernel.compute_features(old, 'EUR_USD', .0001)
            store = TechnicalStore(Path(directory) / 'fixture.sqlite', {'feature_names': list(old_features), 'pairs': {'EUR_USD': .0001}})
            try:
                observed = float(data['time'][-1] + 70)
                store.ingest('EUR_USD', old, old_features, {'observed_epoch': observed - 60}, keep_from=int(old['time'][-3]), published_epoch=observed - 59)
                changed = copy.deepcopy(data)
                changed['volume'][100] += 1
                features, _ = worker.incremental_features(changed, 'EUR_USD', .0001, old['time'][-1])
                with self.assertRaises(RevisionDetected):
                    store.ingest('EUR_USD', changed, features, {'observed_epoch': observed}, keep_from=int(old['time'][-1])+1, published_epoch=observed + 1)
                self.assertEqual(store.latest_time('EUR_USD'), old['time'][-1])
            finally:
                store.close()

    def test_incremental_ingest_preserves_old_observations_and_new_matches_full(self):
        with tempfile.TemporaryDirectory() as directory:
            data = candles(720)
            full = worker.original.kernel.compute_features(data, 'EUR_USD', .0001)
            old = subset(data, end=717)
            old_features = worker.original.kernel.compute_features(old, 'EUR_USD', .0001)
            store = TechnicalStore(Path(directory) / 'fixture.sqlite', {'feature_names': list(full), 'pairs': {'EUR_USD': .0001}})
            try:
                observed = float(data['time'][-1] + 70)
                store.ingest('EUR_USD', old, old_features, {'observed_epoch': observed - 180}, keep_from=int(old['time'][-10]), published_epoch=observed - 179)
                before = [tuple(r) for r in store.connection.execute('SELECT * FROM observations ORDER BY t')]
                features, _ = worker.incremental_features(data, 'EUR_USD', .0001, old['time'][-1])
                store.ingest('EUR_USD', data, features, {'observed_epoch': observed}, keep_from=int(old['time'][-1])+1, published_epoch=observed + 1)
                after = [tuple(r) for r in store.connection.execute('SELECT * FROM observations WHERE t<=? ORDER BY t', (int(old['time'][-1]),))]
                self.assertEqual(before, after)
                latest = store.latest('EUR_USD')
                for name, value in latest['values'].items():
                    expected = full[name][-1]
                    self.assertEqual(value, None if np.isnan(expected) else expected, name)
                self.assertEqual(store.counts()['observations'], 13)
            finally:
                store.close()


class OperationalBoundaryTests(unittest.TestCase):
    def test_adaptive_context_reads_recover_bounded_backlog_without_silent_skip(self):
        data = candles(4096)
        latest = int(data['time'][2500])
        samples = [(subset(data, 2048), {'range_truncated': True}), (data, {'range_truncated': True})]
        with mock.patch.object(worker.original, 'read_pair', side_effect=samples) as reader:
            actual, receipt = worker.read_with_context(Path('synthetic.csv'), 'EUR_USD', latest,
                {'tail_rows': 2048}, {'maximum_tail_rows': 8192})
        self.assertEqual([c.args[-1] for c in reader.call_args_list], [2048, 4096])
        self.assertEqual(len(actual['time']), 4096)
        self.assertEqual(len(receipt['operations_read_attempts']), 2)

    def test_exhausted_context_refuses_instead_of_claiming_continuity(self):
        data = candles(100)
        with mock.patch.object(worker.original, 'read_pair', return_value=(data, {'range_truncated': True})):
            with self.assertRaisesRegex(ValueError, 'bounded_backlog_context_exhausted'):
                worker.read_with_context(Path('synthetic.csv'), 'EUR_USD', int(data['time'][10]),
                    {'tail_rows': 2048}, {'maximum_tail_rows': 2048})

    def test_regressed_source_refused(self):
        data = candles(100)
        with mock.patch.object(worker.original, 'read_pair', return_value=(data, {'range_truncated': False})):
            with self.assertRaisesRegex(ValueError, 'source_latest_bar_regressed'):
                worker.read_with_context(Path('synthetic.csv'), 'EUR_USD', int(data['time'][-1] + 60),
                    {'tail_rows': 2048}, {'maximum_tail_rows': 8192})

    def test_complete_source_starting_after_persisted_boundary_is_refused(self):
        data = candles(100)
        with mock.patch.object(worker.original, 'read_pair', return_value=(data, {'range_truncated': False})):
            with self.assertRaisesRegex(ValueError, 'persisted_boundary_missing'):
                worker.read_with_context(Path('synthetic.csv'), 'EUR_USD', BASE - 60,
                    {'tail_rows': 2048}, {'maximum_tail_rows': 8192})

    def test_freshness_boundary_and_future_bar_are_explicit(self):
        features = {r['name']: 1. for r in worker.original.kernel.feature_registry()}
        row = {'values': features, 'bar_start_epoch': BASE, 'bar_end_epoch': BASE + 60,
               'published_epoch': BASE + 61, 'available_features': 216, 'feature_count': 216, 'feature_hash': 'hash'}
        self.assertEqual(worker.describe(row, 180, BASE + 240)['status'], 'current')
        self.assertEqual(worker.describe(row, 180, BASE + 240.1)['status'], 'stale')
        self.assertEqual(worker.describe(row, 180, BASE + 59)['status'], 'stale')
        row['published_epoch'] = BASE + 90
        self.assertEqual(worker.describe(row, 180, BASE + 80)['status'], 'stale')

    def test_aligned_publication_future_after_clock_rollback_is_not_exposed(self):
        local = {'bar_start_epoch': BASE, 'published_epoch': BASE + 80}
        candidate = {'source_observations': {'EUR_USD': local}}
        publication = {'target_bar_start_epoch': BASE, 'published_epoch': BASE + 100,
                       'id': 'synthetic', 'panel': {'by_pair': {'EUR_USD': {'bar_start_epoch': BASE}}}}
        self.assertIsNone(worker.attach_aligned_state(publication, candidate, 'EUR_USD', {}, BASE + 99, 180))
        state = worker.attach_aligned_state(publication, candidate, 'EUR_USD', {}, BASE + 101, 180)
        self.assertEqual(state['availability_epoch'], BASE + 100)
        self.assertIsNone(worker.attach_aligned_state(publication, candidate, 'EUR_USD', {}, BASE + 241, 180))
        local['published_epoch'] = BASE + 110
        self.assertIsNone(worker.attach_aligned_state(publication, candidate, 'EUR_USD', {}, BASE + 101, 180))

    def test_cycle_uses_same_clock_local_and_peer_and_preserves_newer_local(self):
        pairs = ('EUR_USD', 'EUR_GBP', 'EUR_JPY', 'GBP_USD', 'AUD_USD')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); output = root / 'output'; output.mkdir(); sources = root / 'candles'; sources.mkdir()
            base = {'pairs': dict.fromkeys(pairs, .0001), 'output_root': str(output), 'candle_root': str(sources),
                    'tail_rows': 2048, 'bootstrap_rows': 3, 'minimum_free_bytes': 0,
                    'maximum_dataset_bytes': 512 * 1024**2, 'maximum_bar_age_seconds': 180}
            operations = {'maximum_tail_rows': 8192, 'maximum_candidate_minutes': 8}
            source_data = {pair: candles(702 if pair == 'EUR_USD' else 701) for pair in pairs}
            now = float(BASE + 702 * 60 + 10)
            def reader(path, pair, when, requested):
                return source_data[pair], {'observed_epoch': now - 2, 'range_truncated': False,
                    'instrument': pair, 'retained_rows': len(source_data[pair]['time'])}
            for pair in pairs:
                (sources / (pair + '_M1.csv')).write_text('synthetic source stub')
            contract = {'feature_names': [r['name'] for r in worker.original.kernel.feature_registry()], 'pairs': base['pairs']}
            store = TechnicalStore(output / 'fixture.sqlite', contract)
            try:
                with mock.patch.object(worker.original, 'read_pair', side_effect=reader), mock.patch.object(worker.time, 'time', return_value=now):
                    report = worker.run_cycle(operations, base, store, {}, runtime_id='synthetic-runtime')
                latest = json.loads((output / 'latest_features.json').read_bytes())
                row = latest['pairs']['EUR_USD']; state = row['aligned_state']
                self.assertIsNotNone(state, str(row['availability']) + ' ' + str(row['peer_alignment']))
                self.assertEqual(state['peer']['status'], 'available')
                self.assertEqual(state['observation']['bar_start_epoch'], state['peer']['bar_start_epoch'])
                self.assertEqual(row['observation']['bar_start_epoch'] - state['bar_start_epoch'], 60)
                self.assertEqual(report['publication_generation'], latest['publication_generation'])
                self.assertFalse(report['can_place_orders'])
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
