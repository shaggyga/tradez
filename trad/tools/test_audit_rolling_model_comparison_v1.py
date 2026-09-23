import hashlib
import struct

import numpy as np
import pytest

from tools import audit_rolling_model_comparison_v1 as audit


def test_independent_holding_reserves_every_candidate_before_future_masks():
    clocks = np.array([0, 60, 120, 300, 0, 60, 300], dtype=np.int64)
    pairs = np.array([0, 0, 0, 0, 1, 1, 1], dtype=np.int16)
    prediction = np.array([2., 8., -3., -4., -2., 4., 3.])
    spread = np.full(7, .5)
    signs, masks = audit.independent_decisions(clocks, pairs, prediction, spread, 5)
    assert masks['origin_spread_threshold'].all()
    # Row0 would have a missing future endpoint in a scorer. No future-mask
    # argument exists, so it still reserves the full interval and blocks rows1/2.
    assert np.flatnonzero(masks['origin_spread_threshold_nonoverlap']).tolist() == [0, 3, 4, 6]
    assert signs.tolist() == [1, 1, -1, -1, -1, 1, 1]


def test_independent_decisions_missing_spread_flat_nan_and_exact_threshold():
    prediction = np.array([0., np.nan, 2., 2., -2., -3.])
    spread = np.array([0., 0., np.nan, 1., -.1, 1.])
    _, masks = audit.independent_decisions(np.arange(6) * 60, np.zeros(6, dtype=np.int16), prediction, spread, 5)
    assert np.flatnonzero(masks['all_issued_sign_diagnostic']).tolist() == [2, 3, 4, 5]
    assert np.flatnonzero(masks['origin_spread_threshold']).tolist() == [5]


def test_decision_hash_matches_independent_scalar_packed_contract():
    names = ['AUD_CAD', 'EUR_USD']
    clocks = np.array([1800000000, 1800000060, 1800000000], dtype=np.int64)
    pairs = np.array([0, 0, 1], dtype=np.int16)
    signs = np.array([-1, 1, 1], dtype=np.int8)
    mask = np.array([True, False, True])
    expected = hashlib.sha256(struct.pack('<7sqb', b'AUD_CAD', 1800000000, -1)
                              + struct.pack('<7sqb', b'EUR_USD', 1800000000, 1)).hexdigest()
    assert audit.decision_sha(mask, clocks, pairs, signs, names) == expected


def test_independent_learner_representation_and_exact_float_bits():
    z = np.array([[1., np.nan], [np.nan, -2.]], dtype=np.float32)
    pairs = np.array([0, 1])
    ridge = audit.learner_matrix(z, pairs, 'ridge', 2)
    assert ridge.dtype == np.float64
    assert ridge.tolist() == [[1., 0., 0., 1., 1., 0.], [0., -2., 1., 0., 0., 1.]]
    hgb = audit.learner_matrix(z, pairs, 'hgb', 2)
    assert hgb.dtype == np.float32 and np.isnan(hgb[0, 1]) and hgb[:, -1].tolist() == [0., 1.]
    assert audit.finite_bits_equal([0., -0., np.nan], [0., -0., np.nan])
    assert not audit.finite_bits_equal([0.], [-0.])
    assert not audit.finite_bits_equal([0.], [0., 1.])


def test_audit_metric_check_catches_material_difference_and_accepts_null():
    audit.close_number(None, None, 'none')
    audit.close_number(1. + 1e-12, 1., 'tolerance')
    with pytest.raises(ValueError, match='numeric_metric_mismatch'):
        audit.close_number(1.1, 1., 'incorrect')
    with pytest.raises(ValueError, match='expected_null'):
        audit.close_number(0., None, 'missing')


def test_contained_artifact_digest_rejects_changed_bytes_and_path_escape(tmp_path):
    path = tmp_path / 'item.json'
    path.write_bytes(b'one')
    digest = audit.sha(path)
    assert audit.checked(tmp_path, 'item.json', digest) == path
    path.write_bytes(b'two')
    with pytest.raises(ValueError, match='sha256_mismatch'):
        audit.checked(tmp_path, 'item.json', digest)
    with pytest.raises(ValueError, match='contained_artifact'):
        audit.checked(tmp_path, '../outside.json', digest)


def test_independent_aggregate_audit_catches_corrupted_report():
    from tools import run_rolling_model_comparison_v1 as runner
    n = 600
    data = {'pair_names': ['EUR_USD', 'USD_JPY'], 'pair_id': np.repeat([0, 1], 300),
            'time': np.tile(1800000000 + np.arange(300) * 60, 2),
            'split': np.tile(np.r_[np.ones(150), np.full(150, 2)], 2).astype(np.int8),
            'spread': np.full(n, .5)}
    actual = np.sin(np.arange(n)) * 3
    for stem, value in {'y': actual, 'long': actual - .5, 'short': -actual - .5,
                        'arima': np.ones(n), 'momentum': np.ones(n), 'prior': np.ones(n),
                        'valid': np.arange(n) % 11 != 0, 'strict': np.arange(n) % 3 != 0,
                        'eligible': np.arange(n) % 13 != 0, 'delay_valid': np.arange(n) % 7 != 0,
                        'delay_long': actual - .8, 'delay_short': -actual - .8}.items():
        data[f'{stem}_5'] = value
    data['strict_5'] &= data['valid_5']
    prediction = np.cos(np.arange(n)) * 3
    reports = runner.score_variant(data, np.arange(n), prediction, 5, data['prior_5'])
    assert audit.audit_scores(data, prediction, 5, reports) > 0
    reports['validation']['primary']['policies']['origin_spread_threshold']['decisions'] += 1
    with pytest.raises(ValueError, match='independent_decision_count'):
        audit.audit_scores(data, prediction, 5, reports)
