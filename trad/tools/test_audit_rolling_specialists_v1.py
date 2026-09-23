import copy
import hashlib

import numpy as np
import pytest

from tools import audit_rolling_specialists_v1 as audit


def test_oof_key_membership_and_purge_are_original_clock_only():
    cutoffs = np.arange(5, dtype=np.int64) * 7 * 86400 + 1800000000
    times = np.array([cutoffs[0] - 60, cutoffs[0], cutoffs[1] - 60, cutoffs[1], cutoffs[-1] - 60, cutoffs[-1]])
    split = np.array([0, 0, 0, 0, 0, 1], dtype=np.int8)
    rows, fold = audit.fold_population(times, split, cutoffs)
    assert rows.tolist() == [1, 2, 3, 4] and fold.tolist() == [0, 0, 1, 3]
    data = {'time': np.array([cutoffs[0] - 31 * 60, cutoffs[0] - 32 * 60, cutoffs[0] - 90 * 60]),
            'split': np.array([0, 0, 0]), 'valid_30': np.array([True, True, False])}
    assert audit.mature_rows(data, 30, cutoffs[0]).tolist() == [1]


def test_independent_normalization_preserves_missing_and_ignores_unsupported():
    x = np.array([[3., np.nan, 8.], [1., 9., 6.]])
    p = {'mean': np.array([1., 5., 0.]), 'scale': np.array([2., 2., 0.]), 'supported': np.array([True, True, False])}
    z = audit.independent_normalize(x, p)
    assert z.dtype == np.float32
    np.testing.assert_equal(z, [[1., np.nan, np.nan], [0., 2., np.nan]])
    p['scale'][0] = 0
    with pytest.raises(ValueError, match='finite_supported'):
        audit.independent_normalize(x, p)


def test_selection_hash_contains_asymmetric_exit_targets_not_just_midpoint():
    data = {'time': np.array([1800000000, 1800000060]), 'pair_id': np.array([0, 1]),
            'pair_names': ['EUR_USD', 'USD_JPY'], 'entry_long': np.array([.2, .3]),
            'entry_short': np.array([.4, .5]), 'y_30': np.array([1., -2.]),
            'long_30': np.array([.1, -3.1]), 'short_30': np.array([-2.3, .5])}
    record = audit.selection(data, np.arange(2), 30)
    assert record['maximum_target_end_epoch'] == 1800000060 + 31 * 60
    changed = copy.deepcopy(data)
    changed['long_30'][0] -= .1
    other = audit.selection(changed, np.arange(2), 30)
    assert record['pair_clock_sha256'] == other['pair_clock_sha256']
    assert record['targets_y_exit_long_exit_short_sha256'] != other['targets_y_exit_long_exit_short_sha256']
    changed['short_30'][0] = 9
    with pytest.raises(ValueError, match='asymmetric_training_targets'):
        audit.selection(changed, np.arange(2), 30)


def heads():
    return {'probability': np.array([.25, .8]), 'direct': np.array([-.2, 1.]),
            'positive': np.array([2., 4.]), 'nonpositive': np.array([3., 1.]),
            'long_exit': np.array([.5, .6]), 'short_exit': np.array([.7, .8])}


def test_meta_reconstruction_uses_uncalibrated_raw_head_identities():
    h = heads()
    context = np.arange(16, dtype=np.float32).reshape(2, 8)
    m = audit.raw_meta(h, context, [.1, .2], [.3, .4])
    assert m.shape == (2, 18) and m.dtype == np.float64
    np.testing.assert_array_equal(m[:, 6], h['probability'] * h['positive'] - (1 - h['probability']) * h['nonpositive'])
    np.testing.assert_array_equal(m[:, 7], h['probability'] * h['positive'] + (1 - h['probability']) * h['nonpositive'])
    np.testing.assert_array_equal(m[:, 10:], context)
    changed = dict(h, probability=np.array([.6, .9]))
    assert not np.array_equal(audit.raw_meta(changed, context, [.1, .2], [.3, .4]), m)


def test_saved_platt_parameter_application_is_independent_and_bounded():
    raw = np.array([0., .25, .5, .75, 1.])
    identity = {'clip': 1e-6, 'slope': 1., 'intercept': 0.}
    np.testing.assert_allclose(audit.apply_platt_independently(raw, identity), np.clip(raw, 1e-6, 1 - 1e-6), atol=1e-15)
    constant = dict(identity, slope=0., intercept=np.log(3.))
    np.testing.assert_allclose(audit.apply_platt_independently(raw, constant), .75)


def test_head_replay_preserves_probability_class_and_negative_clipping():
    class Model:
        classes_ = np.array([0, 1])
        def predict_proba(self, x):
            return np.column_stack((1 - x[:, 0], x[:, 0]))
        def predict(self, x):
            return x[:, 1]
    bundle = {'models': {name: Model() for name in audit.HEADS}}
    replay = audit.replay_heads(bundle, np.array([[.2, -1.], [.8, 2.]]))
    np.testing.assert_array_equal(replay['probability'], [.2, .8])
    np.testing.assert_array_equal(replay['direct'], [-1., 2.])
    for name in audit.HEADS[2:]:
        np.testing.assert_array_equal(replay[name], [0., 2.])
    bundle['models']['probability'].classes_ = np.array([1, 0])
    with pytest.raises(ValueError, match='class_identity'):
        audit.replay_heads(bundle, np.zeros((2, 2)))


def test_meta_replay_reconstructs_saved_ridge_scaler_and_pair_context():
    class Model:
        def predict(self, x):
            return x @ np.array([2., 4., 8., 16.])
    raw = np.zeros((2, 18)); raw[:, 1] = [3., np.nan]
    bundle = {'arm': 'direct_ridge', 'columns': [1], 'selected_meta_names': ['direct'],
              'pair_count': 2, 'scaler': {'mean': np.array([1.]), 'scale': np.array([2.]), 'supported': np.array([True])},
              'model': Model()}
    np.testing.assert_array_equal(audit.replay_meta(bundle, raw, np.array([0, 1])), [10., 20.])
    bundle['selected_meta_names'] = ['probability']
    with pytest.raises(ValueError, match='column_identity'):
        audit.replay_meta(bundle, raw, np.array([0, 1]))


def test_reference_inventory_rejects_silent_rehash(tmp_path):
    p = tmp_path / 'data.json'; p.write_bytes(b'original')
    digest = hashlib.sha256(b'original').hexdigest()
    inventory = {'data.json': {'sha256': digest}}
    reference = {'nested': [{'path': 'data.json', 'sha256': digest}]}
    assert audit.check_references(tmp_path, reference, inventory) == 1
    inventory['data.json']['sha256'] = 'changed'
    with pytest.raises(ValueError, match='reference_inventory'):
        audit.check_references(tmp_path, reference, inventory)


def test_independent_both_gate_audit_detects_changed_expected_gate_decision():
    from oanda_rolling_specialist_scoring_v1 import score_specialist_variant
    n = 300; t = 1800000000 + np.arange(n) * 60
    data = {'time': t, 'pair_id': np.zeros(n, dtype=np.int16), 'pair_names': ['EUR_USD'],
            'split': np.r_[np.ones(150), np.full(150, 2)].astype(np.int8), 'spread': np.ones(n)}
    y = np.sin(np.arange(n)) * 4
    for stem, value in {'y': y, 'long': y - .9, 'short': -y - 1.2, 'valid': np.arange(n) % 7 != 0,
                        'strict': np.arange(n) % 3 != 0, 'eligible': np.arange(n) % 17 != 0,
                        'arima': np.ones(n), 'momentum': np.ones(n), 'prior': np.ones(n),
                        'delay_long': y - 1.1, 'delay_short': -y - 1.4, 'delay_valid': np.arange(n) % 11 != 0}.items():
        data[stem + '_30'] = value
    data['strict_30'] &= data['valid_30']
    prediction = np.cos(np.arange(n)) * 3
    el, es = np.full(n, .4), np.full(n, .6)
    h = {'long_exit': np.full(n, .7), 'short_exit': np.full(n, 1.2)}
    metrics = score_specialist_variant(data, np.arange(n), prediction, 30, data['prior_30'], el, es, h['long_exit'], h['short_exit'])
    assert audit.audit_gate_scores(data, prediction, 30, metrics, el, es, h) > 0
    metrics['secondary_expected_cost']['validation']['primary']['policies']['expected_cost_threshold']['decisions'] += 1
    with pytest.raises(ValueError, match='independent_decision_count'):
        audit.audit_gate_scores(data, prediction, 30, metrics, el, es, h)
