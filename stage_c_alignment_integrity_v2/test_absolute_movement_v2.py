import copy
import io

import joblib
import numpy as np
import pytest

from absolute_movement_models_v2 import METHODS, fit_pair, predict_values, score, target, transform_outcomes
from contracts import fingerprint
import matched_campaign_models_v2 as signed


def fixture():
    c = signed.contract()
    c.update(training_start=1000, fit_cutoffs=[10000], evaluation_asof=30000, minimum_training_rows=8)
    c['hgb_parameters'] = {**c['hgb_parameters'], 'max_iter': 3, 'min_samples_leaf': 4}
    obs, labels = [], []
    for i in range(40):
        origin = 1000+i*60
        rid = 'EUR_USD:'+str(origin)
        obs.append({'record_id': rid, 'instrument': 'EUR_USD', 'origin_epoch': origin,
                    'available_epoch': origin, 'features': [0.0]*len(signed.FEATURES)})
        labels.append({'record_id': rid, 'target_id': 'technical_endpoint_midpoint_elapsed_15m',
                       'label_end_epoch': origin+900, 'available_epoch': origin+900, 'value': 10.0 if i%2 else -10.0})
    return obs, labels, c


def fits():
    obs, labels, c = fixture()
    original, _ = signed.fit_pair(obs, labels, ['EUR_USD'], 15, 10000, c)
    meta, tree = fit_pair(obs, labels, ['EUR_USD'], 15, 10000, c, original)
    return obs, labels, c, original, meta, tree


def test_conditional_magnitude_is_not_absolute_conditional_mean():
    obs, labels, c, original, meta, tree = fits()
    values = predict_values(meta, tree, obs)
    assert original['history_mean'] == 0
    assert original['ridge']['coefficient'][0] == pytest.approx(0)
    assert meta['history_mean'] == meta['history_median'] == 10
    assert all(v['absolute_ridge'] == pytest.approx(10) and v['absolute_hgb'] == pytest.approx(10) for v in values.values())
    assert meta['target'] == target(15)
    assert meta['training_population_sha256'] != original['training_population_sha256']
    assert meta['ridge']['mean'] == original['ridge']['mean']
    assert meta['training_rows'] == original['training_rows']


def test_future_labels_and_future_features_cannot_change_fitted_weights():
    obs, labels, c, original, meta, tree = fits()
    future = copy.deepcopy(obs[-1]); future.update(record_id='future', origin_epoch=11000, available_epoch=11000, features=[float('nan')]*26)
    label = {'record_id': 'future', 'target_id': labels[0]['target_id'], 'available_epoch': 12000,
             'label_end_epoch': 11900, 'value': float('nan')}
    other, payload = fit_pair(obs+[future], labels+[label], ['EUR_USD'], 15, 10000, c, original)
    assert other == meta and payload == tree


def test_training_support_mismatch_refuses_before_comparison():
    obs, labels, c, original, _, _ = fits()
    original['training_population_sha256'] = '0'*64
    with pytest.raises(ValueError, match='original_training_support'):
        fit_pair(obs, labels, ['EUR_USD'], 15, 10000, c, original)


def test_training_feature_statistics_mismatch_refuses():
    obs, labels, c, original, _, _ = fits()
    original['ridge']['mean'][0] = 1
    with pytest.raises(ValueError, match='feature_statistics'):
        fit_pair(obs, labels, ['EUR_USD'], 15, 10000, c, original)


def test_exact_maturity_boundary_and_null_preservation():
    obs, labels, _ = fixture()
    labels[0]['value'] = None
    transformed = transform_outcomes(labels, 15)
    assert transformed[0]['value'] is None
    assert transformed[1]['available_epoch'] == labels[1]['available_epoch']
    assert transformed[1]['original_outcome_sha256'] == fingerprint(labels[1])
    row = {'record_id': labels[1]['record_id'], 'decision_epoch': obs[1]['origin_epoch'],
           'available_epoch': obs[1]['origin_epoch']+2, 'predictions': {m: 5 for m in METHODS}}
    assert score([row], labels, 15, labels[1]['available_epoch']-1)['mature_rows'] == 0
    s = score([row], labels, 15, labels[1]['available_epoch'])
    assert s['mature_rows'] == 1
    assert all(r['mae_bps'] == 5 and r['mse_bps2'] == 25 for r in s['metrics'])


def test_clipping_preserves_raw_prediction():
    obs, _, _, _, meta, tree = fits()
    meta['ridge']['coefficient'] = [-5]+[0]*26
    meta['ridge']['model_id'] = fingerprint({k: v for k, v in meta['ridge'].items() if k != 'model_id'})
    meta['fit_id'] = fingerprint({k: v for k, v in meta.items() if k != 'fit_id'})
    value = predict_values(meta, tree, obs[:1])[obs[0]['record_id']]
    assert value['raw_absolute_ridge'] == -5 and value['absolute_ridge'] == 0


@pytest.mark.parametrize('mutation', ['tree', 'meta', 'future_feature', 'missing_feature'])
def test_invalid_inference_inputs_refuse(mutation):
    obs, _, _, _, meta, tree = fits()
    if mutation == 'tree': tree += b'altered'
    elif mutation == 'meta': meta['history_mean'] += 1
    elif mutation == 'future_feature': obs[0]['available_epoch'] += 1
    else: obs[0]['features'] = None
    with pytest.raises(ValueError): predict_values(meta, tree, obs[:1])


@pytest.mark.parametrize('mutation', ['missing_method', 'negative', 'duplicate', 'clock'])
def test_invalid_matched_assessment_refuses(mutation):
    obs, labels, _ = fixture()
    row = {'record_id': labels[0]['record_id'], 'decision_epoch': obs[0]['origin_epoch'],
           'available_epoch': obs[0]['origin_epoch']+2, 'predictions': {m: 5 for m in METHODS}}
    rows = [row]
    if mutation == 'missing_method': row['predictions'].pop(METHODS[-1])
    elif mutation == 'negative': row['predictions'][METHODS[-1]] = -1
    elif mutation == 'duplicate': rows.append(copy.deepcopy(row))
    else: labels[0]['label_end_epoch'] += 1
    with pytest.raises(ValueError): score(rows, labels, 15, 30000)


def test_original_outcomes_remain_unchanged():
    _, labels, _ = fixture()
    before = copy.deepcopy(labels)
    transform_outcomes(labels, 15)
    assert labels == before


def test_duplicate_original_labels_refuse():
    _, labels, _ = fixture()
    with pytest.raises(ValueError, match='duplicate_original_outcome'):
        transform_outcomes(labels+[labels[0]], 15)


def test_operator_budget_does_not_inherit_no_fit_parent_controls():
    from absolute_movement_operator_v2 import configuration
    c = {'experiment': {'horizon_minutes': list(range(7)), 'fit_cutoffs': [1, 2]},
         'resources': {'fit_pair_count_cap': 14, 'new_estimator_count_cap': 28, 'workers': 1}}
    parent = {'configuration': {'universe': ['EUR_USD'], 'fit_count_cap': 0, 'model_load_count_cap': 0}}
    result = configuration(parent, c)
    assert result['new_estimator_count_cap'] == 28 and result['original_model_fit_count_cap'] == 0
    assert 'fit_count_cap' not in result and 'model_load_count_cap' not in result


@pytest.mark.parametrize('key,value', [('fit_pair_count_cap', 13), ('new_estimator_count_cap', 27), ('workers', 2)])
def test_operator_refuses_unbudgeted_fit_population(key, value):
    from absolute_movement_operator_v2 import configuration
    c = {'experiment': {'horizon_minutes': list(range(7)), 'fit_cutoffs': [1, 2]},
         'resources': {'fit_pair_count_cap': 14, 'new_estimator_count_cap': 28, 'workers': 1}}
    c['resources'][key] = value
    with pytest.raises(ValueError, match='fit_budget_or_worker_limit'):
        configuration({'configuration': {'universe': ['EUR_USD']}}, c)
