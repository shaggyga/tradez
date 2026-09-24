import copy
import math

import numpy as np
import pytest

from contracts import fingerprint
from magnitude_layer_v2 import (fit_snapshot, apply_snapshot, weighted_ridge, join_predictions,
                                feature_vector, score, sensitivity)


def fixture(n=12, pairs=20):
    rows, outcomes = [], {}
    target = 'technical_endpoint_midpoint_elapsed_15m'
    for i in range(n):
        epoch = 86460+21600*i
        for j in range(pairs):
            pair = f'PAIR_{j}'; rid = pair+':'+str(epoch)
            signed = float(j%5-2); magnitude = float(1+i%4)
            row = {'record_id': rid, 'instrument': pair, 'target_id': target, 'decision_epoch': epoch,
                   'available_epoch': epoch+2, 'group': 'legacy26', 'procedure': 'frozen',
                   'selected_fit_id': 'original', 'selected_fit_cutoff': 0,
                   'base_model_ids': {m: fingerprint({'fit_id': 'original', 'method': m}) for m in ('ridge', 'recovered_hgb')},
                   'base_model_ready_epochs': {'ridge': 0, 'recovered_hgb': 0},
                   'ridge_prediction_bps': signed, 'recovered_hgb_prediction_bps': signed,
                   'blend_prediction_bps': signed, 'zero_prediction_bps': 0.0,
                   'source_records': {'ridge': fingerprint([rid, 'ridge']), 'recovered_hgb': fingerprint([rid, 'hgb'])},
                   'magnitude_prediction_bps': {'ridge': magnitude, 'recovered_hgb': magnitude},
                   'absolute_forecast_id': fingerprint([rid, 'absolute']), 'absolute_fit_id': 'a'*64,
                   'absolute_record_sha256': fingerprint([rid, 'absolute_record'])}
            rows.append(row)
            outcomes[rid, target] = {'record_id': rid, 'target_id': target, 'label_end_epoch': epoch+900,
                                     'available_epoch': epoch+900, 'value': 3*signed*magnitude}
    c = {'minimum_distinct_origins': 8, 'minimum_distinct_utc_days': 3, 'minimum_distinct_pairs': 20,
         'layer_fit': {'ridge_lambda': 20.0}}
    return rows, outcomes, c


def fit(rows, labels, c, cutoff=10**9):
    return fit_snapshot(rows, labels, cutoff, ['legacy26', 'technical_endpoint_midpoint_elapsed_15m', 'frozen'], 15, c)


def current_row(row, epoch=500000):
    row = copy.deepcopy(row); row.update(decision_epoch=epoch, available_epoch=epoch+2,
                                       record_id=row['instrument']+':'+str(epoch))
    return row


def test_weighted_ridge_known_shrinkage_and_unpenalized_intercept():
    m = weighted_ridge([[-1], [0], [1]], [3, 5, 7], [1, 1, 1], 3)
    assert m['mean'] == [0]
    assert m['coefficient'][0] == pytest.approx(5)
    assert m['coefficient'][1]/m['scale'][0] == pytest.approx(1)


def test_interaction_learns_constructed_signal_beyond_signed_only():
    rows, labels, c = fixture(); snapshot = fit(rows, labels, c, cutoff=400000)
    row = current_row(rows[4]); row['magnitude_prediction_bps'] = dict(ridge=4., recovered_hgb=4.)
    result = apply_snapshot(row, snapshot, 'expanding_prefix')
    for r in result:
        assert abs(r['predictions']['magnitude_interaction']-24) < abs(r['predictions']['signed_only']-24)
        assert r['production_available_epoch'] is None and not r['native_policy_admitted']
    assert snapshot['support']['rows'] == 240


def test_future_features_labels_and_current_origin_cannot_change_snapshot():
    rows, labels, c = fixture(); cutoff = 400000
    before = fit(rows, labels, c, cutoff)
    future = current_row(rows[0], cutoff); future['ridge_prediction_bps'] = float('nan')
    future['magnitude_prediction_bps']['ridge'] = float('nan')
    modified = {**labels, (future['record_id'], future['target_id']): {'value': float('nan')}}
    assert fit(rows+[future], modified, c, cutoff) == before


def test_unmatured_outcome_value_does_not_enter_fit():
    rows, labels, c = fixture(); cutoff = 400000
    key = next(iter(labels)); labels[key]['available_epoch'] = cutoff+1
    before = fit(rows, labels, c, cutoff)
    labels[key]['value'] = float('nan')
    assert fit(rows, labels, c, cutoff) == before


@pytest.mark.parametrize('n,pairs', [(7, 20), (8, 20), (12, 19)])
def test_insufficient_distinct_support_has_no_layer_fallback(n, pairs):
    rows, labels, c = fixture(n, pairs); snapshot = fit(rows, labels, c, 400000)
    assert snapshot['status'] == 'insufficient_distinct_support' and snapshot['parameters'] is None
    assert apply_snapshot(current_row(rows[0]), snapshot, 'expanding_prefix') == []


@pytest.mark.parametrize('mutation', ['cutoff', 'duplicate', 'negative_magnitude', 'target_clock'])
def test_invalid_training_provenance_refuses(mutation):
    rows, labels, c = fixture()
    if mutation == 'cutoff': rows[0]['selected_fit_cutoff'] = rows[0]['decision_epoch']
    elif mutation == 'duplicate': rows.append(copy.deepcopy(rows[0]))
    elif mutation == 'negative_magnitude': rows[0]['magnitude_prediction_bps']['ridge'] = -1
    else: labels[next(iter(labels))]['label_end_epoch'] += 1
    with pytest.raises(ValueError): fit(rows, labels, c)


def test_snapshot_corruption_and_future_application_refuse():
    rows, labels, c = fixture(); snapshot = fit(rows, labels, c, 400000)
    with pytest.raises(ValueError, match='future_or_current'): apply_snapshot(rows[0], snapshot, 'expanding_prefix')
    snapshot['parameters']['ridge']['signed_only']['coefficient'][0] += 1
    with pytest.raises(ValueError, match='identity'): apply_snapshot(current_row(rows[0]), snapshot, 'expanding_prefix')


def test_degenerate_features_are_finite_and_weight_sum_matches_rows():
    model = weighted_ridge([[2, 0], [2, 0], [2, 0]], [1, 2, 3], [1.5, 1, .5], 20)
    assert model['scale'] == [1, 1] and all(map(math.isfinite, model['coefficient']))
    assert model['weight_sum'] == 3 and model['coefficient'][0] == pytest.approx(5/3)


def absolute_record(base):
    a = {k: base[k] for k in ('record_id', 'instrument', 'decision_epoch', 'available_epoch', 'procedure', 'selected_fit_cutoff')}
    a.update(target_id=base['target_id'].replace('midpoint', 'absolute'), original_signed_fit_id=base['selected_fit_id'],
             signed_control_records=base['source_records'], scheduled_model_ready_epoch=0, fit_id='a'*64,
             predictions={'absolute_ridge': 2., 'absolute_hgb': 3.})
    a['forecast_id'] = fingerprint(a); return a


@pytest.mark.parametrize('mutation', ['none', 'clock', 'source', 'target', 'hash', 'missing'])
def test_exact_signed_absolute_join(mutation):
    rows, _, _ = fixture(); base = rows[0]; a = absolute_record(base)
    if mutation == 'none':
        result = join_predictions([base], [a], ['legacy26', base['target_id'], 'frozen'])
        assert result[0]['magnitude_prediction_bps'] == {'ridge': 2., 'recovered_hgb': 3.}; return
    if mutation == 'clock': a['available_epoch'] += 1
    elif mutation == 'source': a['signed_control_records'] = {'ridge': 'bad', 'recovered_hgb': 'bad'}
    elif mutation == 'target': a['target_id'] = 'wrong'
    elif mutation == 'hash': a['forecast_id'] = '0'*64
    if mutation != 'hash': a['forecast_id'] = fingerprint({k: v for k, v in a.items() if k != 'forecast_id'})
    with pytest.raises(ValueError): join_predictions([base], [] if mutation == 'missing' else [a], ['legacy26', base['target_id'], 'frozen'])


def test_mature_scoring_tail_and_degenerate_block_refusal():
    rows, labels, c = fixture(); snapshot = fit(rows, labels, c, 400000)
    output = []
    for i in range(4):
        current = current_row(rows[0], 500000+21600*i)
        generated = apply_snapshot(current, snapshot, 'expanding_prefix')[0]; output.append(generated)
        labels[current['record_id'], current['target_id']] = {'record_id': current['record_id'], 'target_id': current['target_id'],
          'label_end_epoch': current['decision_epoch']+900, 'available_epoch': current['decision_epoch']+900, 'value': 5.}
    s = score(output, labels, 15, 10**9)
    assert s['mature_rows'] == 4 and s['overall']['methods']['zero']['p95_absolute_error_bps'] == 5
    assert score(output, labels, 15, 500899)['mature_rows'] == 0
    intervals = sensitivity(output, labels, 15, 10**9, {'block_lengths': [4, 8], 'draws': 20, 'seed': 1})
    assert intervals[0]['status'] == 'single_circular_block_no_resampling_variation'
    assert intervals[1]['status'] == 'insufficient_origins'


def test_interaction_overflow_refuses():
    rows, _, _ = fixture(); rows[0]['ridge_prediction_bps'] = 1e308; rows[0]['magnitude_prediction_bps']['ridge'] = 1e308
    with pytest.raises(ValueError, match='nonfinite_magnitude_interaction'): feature_vector(rows[0], 'ridge', 'magnitude_interaction')


def test_constant_resampling_deltas_have_no_interval():
    rows, labels, _ = fixture()
    for row in rows:
        row['predictions'] = {'zero': 0., 'raw_signed': 1., 'signed_only': 1., 'magnitude_interaction': 0.}
        labels[row['record_id'], row['target_id']]['value'] = 0.
    result = sensitivity(rows, labels, 15, 10**9, {'block_lengths': [4], 'draws': 100, 'seed': 1})
    assert result[0]['mae_delta_intervals'] == {'raw_signed': None, 'signed_only': None}
    assert result[0]['degenerate_controls'] == ['raw_signed', 'signed_only']
