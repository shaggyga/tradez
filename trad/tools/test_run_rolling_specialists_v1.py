"""Clock, matrix and artifact tests for the isolated specialist orchestrator."""
from __future__ import annotations
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow.parquet as pq
from tools import run_rolling_specialists_v1 as run


def fixture():
    day = 86400
    cuts = np.array([14, 21, 28, 35, 42])*day
    times = np.array([cuts[0]-31*60, cuts[0]-32*60, cuts[0], cuts[1]-31*60,
                      cuts[1]-32*60, cuts[1], cuts[3], cuts[4]-31*60, cuts[4], cuts[4]+day])
    n = len(times)
    data = {'time': times, 'split': np.where(times < cuts[-1], 0, 1).astype(np.int8),
            'fold_cutoffs': cuts, 'pair_id': np.zeros(n, dtype=np.int16), 'pair_names': ['EUR_USD'],
            'raw_x': np.arange(n*50, dtype=float).reshape(n, 50),
            'entry_long': np.full(n, .4), 'entry_short': np.full(n, .6),
            'feature_names': ['f'+str(i) for i in range(50)],
            'normalizers': {'count': np.full((5, 1, 50), 100), 'mean': np.zeros((5, 1, 50)),
                            'scale': np.ones((5, 1, 50)), 'supported': np.ones((5, 1, 50), bool)}}
    for h in (30, 60):
        data[f'y_{h}'] = np.linspace(-2., 2., n)
        data[f'valid_{h}'] = np.ones(n, bool)
        data[f'long_{h}'] = data[f'y_{h}']-.4-.7
        data[f'short_{h}'] = -data[f'y_{h}']-.6-.9
    return data


class RunnerTests(unittest.TestCase):
    def test_final_readback_rejects_changed_earlier_recorded_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); p = root/'model.json'; p.write_text('original')
            record = {'nested': [{'path': 'model.json', 'sha256': run.file_sha(p)}]}
            run.check_recorded_artifacts(root, record)
            p.write_text('changed')
            with self.assertRaises(ValueError): run.check_recorded_artifacts(root, record)

    def test_raw_and_calibrated_probability_scores_are_separate(self):
        data = fixture(); rows = np.flatnonzero(data['split'] == 1)
        for name in ('arima_30', 'momentum_30'):
            data[name] = np.zeros(len(data['time']))
        data['eligible_30'] = np.ones(len(data['time']), bool)
        data['strict_30'] = np.ones(len(data['time']), bool)
        data['y_30'][rows] = [0., 2.]
        result = run.probability_assessment(data, rows, 30, np.zeros(2), np.array([.8, .2]), np.array([.2, .8]), np.full(2, .5))
        c = result['periods']['validation']['full_endpoint']
        self.assertEqual(c['raw']['positive_rows'], 1)
        self.assertAlmostEqual(c['raw']['brier'], .64)
        self.assertAlmostEqual(c['calibrated']['brier'], .04)
        self.assertAlmostEqual(c['TRAIN_pair_prior']['brier'], .25)

    def test_inherited_hashes_cannot_be_replaced_by_current_source(self):
        with patch.object(run, 'file_sha', return_value='current'):
            with self.assertRaisesRegex(ValueError, 'bound_source_changed'):
                run.merge_source_bindings(({}, {'old.py': 'original'}), ())
            with self.assertRaisesRegex(ValueError, 'inherited_binding_conflict'):
                run.merge_source_bindings(({'old.py': 'original'}, {'old.py': 'other'}), ())
            self.assertEqual(run.merge_source_bindings(({'old.py': 'current'},), ('new.py',)),
                             {'old.py': 'current', 'new.py': 'current'})

    def test_strict_target_end_purge_uses_original_clock(self):
        data = fixture()
        selected = run.fit_mask(data, 30, data['fold_cutoffs'][0])
        self.assertFalse(selected[0])  # END equals cutoff.
        self.assertTrue(selected[1])
        self.assertFalse(selected[2])
        data['valid_30'][1] = False
        self.assertFalse(run.fit_mask(data, 30, data['fold_cutoffs'][0]).any())

    def test_oof_issuance_preserves_invalid_labels_and_purges_each_block(self):
        data = fixture()
        rows, fold = run.oof_population(data)
        np.testing.assert_array_equal(rows, np.arange(2, 8))
        np.testing.assert_array_equal(fold, [0, 0, 0, 1, 3, 3])
        mask = run.oof_fit_mask(data, rows, fold, 30)
        np.testing.assert_array_equal(mask, [True, False, True, True, True, False])
        data['valid_30'][:] = False
        for expected, got in zip((rows, fold), run.oof_population(data)):
            np.testing.assert_array_equal(expected, got)
        self.assertFalse(run.oof_fit_mask(data, rows, fold, 30).any())

    def test_prefix_matrix_ignores_later_scaler_and_future_labels(self):
        data = fixture(); rows = np.array([0, 1])
        before = run.base_matrix(data, rows, 'compact38', 0)
        changed = copy.deepcopy(data)
        changed['normalizers']['mean'][4] = 1e6
        changed['raw_x'][2:] = -1e8
        changed['y_30'][:] = np.nan
        changed['long_30'][:] = np.inf
        np.testing.assert_array_equal(before, run.base_matrix(changed, rows, 'compact38', 0))
        self.assertEqual(before.shape, (2, 41))
        np.testing.assert_array_equal(before[:, -3:], [[.4, .6, 0], [.4, .6, 0]])
        self.assertFalse(np.array_equal(before, run.base_matrix(changed, rows, 'compact38', 4)))
        changed['normalizers']['supported'][0, 0, 0] = False
        self.assertTrue(np.isnan(run.base_matrix(changed, rows, 'compact50', 0)[:, 0]).all())
        with self.assertRaises(ValueError): run.base_matrix(data, rows, 'combined228', 0)

    def test_asymmetric_exit_labels_and_target_identity(self):
        data = fixture(); rows = np.arange(5)
        a, b = run.exit_labels(data, rows, 30)
        np.testing.assert_allclose(a, .7)
        np.testing.assert_allclose(b, .9)
        rec = run.selection_record(data, rows, 30)
        changed = copy.deepcopy(data); changed['long_30'][0] += .01
        self.assertNotEqual(rec['targets_y_exit_long_exit_short_sha256'], run.selection_record(changed, rows, 30)['targets_y_exit_long_exit_short_sha256'])
        self.assertEqual(rec['pair_clock_sha256'], run.selection_record(changed, rows, 30)['pair_clock_sha256'])
        changed['long_30'][0] += 5
        with self.assertRaises(ValueError): run.exit_labels(changed, rows, 30)

    def test_chunks_keep_original_rows_and_sum_clips(self):
        data = fixture(); rows = np.array([0, 3, 5, 6])
        def fake(bundle, x):
            return {n: x[:, 0]+k for k, n in enumerate(run.HEAD_NAMES)}, {'positive': len(x)}
        with patch.object(run, 'CHUNK', 2), patch.object(run.specialist, 'predict_heads', side_effect=fake):
            heads, clips = run.predict_heads_chunks({}, data, rows, 'compact38', 0)
        np.testing.assert_array_equal(heads['probability'], data['raw_x'][rows, 0])
        self.assertEqual(clips, {'positive': 4})

    def test_oof_and_head_artifact_full_bit_readback(self):
        data = fixture(); rows = np.arange(4)
        heads = {n: np.array([-0., 0., 1e-100, 1e100]) for n in run.HEAD_NAMES}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            payload = {'time': data['time'][rows], 'raw': np.array([np.nan, -0., 1e-100, 1e100])}
            run.save_npz(path/'oof.npz', payload)
            run.save_heads(path/'heads.parquet', data, rows, heads)
            got = pq.ParquetFile(path/'heads.parquet').read()
            np.testing.assert_array_equal(got['direct'].to_numpy().view(np.uint64), heads['direct'].view(np.uint64))


if __name__ == '__main__':
    unittest.main()
