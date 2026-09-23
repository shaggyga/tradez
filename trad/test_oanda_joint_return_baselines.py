"""Synthetic clock fixtures only: no historical availability is manufactured."""

from copy import deepcopy
import json
import random

import numpy as np
import pytest

from trad.oanda_joint_return_baselines import (
    ARMS, CONTRACT_ID, ORIENTATION, PAIRS, PEERS, fit_joint_return_baselines,
)


def bars_from_returns(oriented_returns):
    """Prices and observation receipts both originate in this synthetic fixture."""
    values = np.asarray(oriented_returns, dtype=float)
    prices = np.asarray([1.1, 1.3, 0.7, 0.6, 150.0, 0.9, 1.2])
    rows = []
    for index, returns in enumerate(values):
        signs = np.asarray([ORIENTATION[pair] for pair in PAIRS])
        prices = prices * np.exp(returns * signs / 10_000.0)
        start = index * 60
        for column, pair in enumerate(PAIRS):
            rows.append(dict(instrument=pair, start_epoch=start, end_epoch=start+60,
                             close_mid=float(prices[column]), available_epoch=start+60.1,
                             complete=True))
    return rows


def sample_bars(n=200, seed=13):
    return bars_from_returns(np.random.default_rng(seed).normal(0, 1, (n, 7)))


def fit(bars, **changes):
    reference = max((row['end_epoch'] for row in bars
                     if isinstance(row, dict) and type(row.get('end_epoch')) in (int, float)), default=60)
    parameters = dict(decision_epoch=reference+0.2, reference_epoch=reference,
                      horizon_sec=300, lag_minutes=(1, 5), min_training_rows=30,
                      max_training_rows=1000)
    parameters.update(changes)
    return fit_joint_return_baselines(bars, **parameters)


def assert_abstention(result, reason):
    assert result['status'] == 'abstain'
    assert reason in result['reasons']
    for arm in result['arms'].values():
        assert arm['status'] == 'abstain'
        assert arm['predicted_return_bps'] is None
        assert arm['side'] == 0
    assert result['proof_eligible'] is False


def test_fixed_separate_contract_records_full_parameters_and_has_no_authority():
    result = fit(sample_bars())
    assert result['status'] == 'predicted'
    assert result['contract_id'] == CONTRACT_ID
    assert result['instrument'] == 'EUR_USD'
    assert result['pairs'] == list(PAIRS)
    assert set(result['arms']) == set(ARMS)
    assert result['parameters']['lag_minutes'] == [1, 5]
    assert result['parameters']['alpha'] == 10
    assert result['parameters']['regression'] == 'ridge_direct_target_return_no_recursive_VAR'
    for flag in ('collection_enabled', 'can_place_orders', 'can_promote', 'can_authorize',
                 'account_eligible', 'proof_eligible', 'issue_and_commit_clocks_attested'):
        assert result[flag] is False
    assert result['independent_sample_count'] is None
    assert result['overlapping_training_labels'] is True
    assert result['availability_evidence'] == 'caller_observed_clocks_not_independently_attested'
    json.dumps(result, allow_nan=False)


def test_all_arms_use_identical_strictly_mature_available_training_rows():
    result = fit(sample_bars())
    assert result['selected_training_rows'] == 190
    assert result['eligible_training_rows'] == 190
    assert len(result['training_rows']) == 190
    issue = result['decision_epoch']
    for row in result['training_rows']:
        assert row['target_epoch'] == row['reference_epoch'] + 300
        assert row['target_epoch'] < issue
        assert row['label_available_epoch'] < issue
        assert row['features_available_epoch'] < issue
    assert result['training_label_maturity_max_epoch'] == result['reference_epoch']
    assert result['training_labels_available_max_epoch'] < issue
    assert result['training_features_available_max_epoch'] < issue
    assert result['current_features_available_epoch'] < issue


def test_delayed_decision_does_not_move_reference_or_target():
    rows = sample_bars()
    earlier = fit(rows)
    later = fit(rows, decision_epoch=earlier['reference_epoch']+299)
    assert later['reference_epoch'] == earlier['reference_epoch']
    assert later['target_epoch'] == earlier['target_epoch']
    assert later['target_epoch'] == later['reference_epoch'] + 300
    assert later['arms'] == earlier['arms']


@pytest.mark.parametrize('delta', [300, 301, -1])
def test_reference_decision_target_order_cannot_accept_elapsed_or_future_target(delta):
    rows = sample_bars()
    reference = rows[-1]['end_epoch']
    with pytest.raises(ValueError, match='original reference'):
        fit(rows, decision_epoch=reference+delta)


@pytest.mark.parametrize('offset', [0, 1, 1000])
def test_label_or_input_unavailable_at_original_decision_is_rejected_not_backfilled(offset):
    rows = sample_bars()
    decision = rows[-1]['end_epoch'] + 0.2
    rows[7]['available_epoch'] = decision + offset
    with pytest.raises(ValueError, match='actually available strictly before decision'):
        fit(rows)


def test_conservative_actual_capture_time_is_accepted_without_historical_clock_inference():
    rows = sample_bars()
    observation = rows[-1]['end_epoch']+0.1
    for row in rows:
        row['available_epoch'] = observation
    result = fit(rows)
    assert result['training_labels_available_max_epoch'] == observation
    assert result['training_features_available_max_epoch'] == observation
    assert result['all_supplied_data_available_max_epoch'] == observation


def test_future_bar_cannot_enter_even_when_its_time_is_before_delayed_decision():
    rows = sample_bars()
    reference = rows[-1]['end_epoch'] - 60
    with pytest.raises(ValueError, match='after original reference'):
        fit(rows, reference_epoch=reference, decision_epoch=reference+90)


def test_disordered_inputs_reproduce_sorted_result_without_mutation():
    rows = sample_bars()
    original = deepcopy(rows)
    expected = fit(rows)
    random.Random(2).shuffle(rows)
    shuffled = deepcopy(rows)
    assert fit(rows) == expected
    assert rows == shuffled
    assert sorted(rows,key=lambda row:(row['end_epoch'],PAIRS.index(row['instrument']))) == original


@pytest.mark.parametrize('changed', [False, True])
def test_duplicate_pair_minutes_are_rejected_not_extra_training_weight(changed):
    rows = sample_bars()
    duplicate = deepcopy(rows[0])
    if changed:
        duplicate['close_mid'] *= 2
    rows.append(duplicate)
    with pytest.raises(ValueError, match='duplicate instrument-minute'):
        fit(rows)


def test_missing_peer_at_one_minute_is_not_silent_intersection_or_zero_fill():
    rows = sample_bars()
    del rows[100]
    with pytest.raises(ValueError, match='partial cross-pair minute'):
        fit(rows)


def test_gap_inside_horizon_excludes_training_even_when_exact_endpoints_exist():
    rows = sample_bars()
    missing = 60*80
    rows = [row for row in rows if row['end_epoch'] != missing]
    result = fit(rows)
    assert result['status'] == 'predicted'
    assert result['whole_minute_gap_count'] == 1
    assert result['training_exclusion_counts']['gap_in_training_label_interval'] > 0
    for row in result['training_rows']:
        assert not row['reference_epoch'] - 5*60 < missing <= row['reference_epoch']
        assert not row['reference_epoch'] < missing <= row['target_epoch']


def test_missing_exact_target_is_not_replaced_by_later_bar():
    rows = sample_bars()
    missing_target = 60*80
    rows = [row for row in rows if row['end_epoch'] != missing_target]
    result = fit(rows)
    assert result['training_exclusion_counts']['missing_exact_training_target'] > 0
    assert all(row['reference_epoch'] != missing_target-300 for row in result['training_rows'])


def test_current_feature_gap_abstains_even_if_old_training_is_plentiful():
    rows = sample_bars()
    gap = rows[-1]['end_epoch']-120
    rows = [row for row in rows if row['end_epoch'] != gap]
    assert_abstention(fit(rows), 'insufficient_contiguous_current_feature_history')


def test_missing_reference_bar_does_not_forecast_from_stale_reference():
    rows = sample_bars()
    reference = rows[-1]['end_epoch']
    rows = rows[:-7]
    assert_abstention(fit(rows, reference_epoch=reference, decision_epoch=reference+0.2),
                      'missing_synchronized_reference_bar')


def test_empty_and_insufficient_history_are_explicit_all_arm_abstentions():
    assert_abstention(fit([]), 'missing_synchronized_reference_bar')
    assert_abstention(fit(sample_bars(5)), 'insufficient_contiguous_current_feature_history')
    result = fit(sample_bars(20), min_training_rows=30)
    assert_abstention(result, 'insufficient_mature_available_training_rows')
    assert result['selected_training_rows'] == 10


def test_orientation_normalizes_usd_first_pairs_before_peer_factor():
    rows = bars_from_returns(np.ones((80, 7)))
    result = fit(rows)
    assert result['status'] == 'predicted'
    values = result['current_feature_values']
    assert values['own_price_lags'] == pytest.approx([1, 5], abs=1e-10)
    assert values['own_and_peer_lags'] == pytest.approx([1, 5]*7, abs=1e-10)
    assert values['own_and_peer_usd_factor'] == pytest.approx([1, 5, 1, 5], abs=1e-10)
    assert set(result['peer_factor_components']) == set(PEERS)
    assert 'EUR_USD' not in result['peer_factor_components']
    assert result['peer_factor_components']['USD_JPY'] == -1/6
    assert result['peer_factor_components']['GBP_USD'] == 1/6


def test_leave_target_out_factor_has_no_eurusd_self_confirmation():
    original = sample_bars()
    changed = deepcopy(original)
    changed[-7]['close_mid'] *= 1.01  # Latest EUR/USD only.
    old = fit(original)['current_feature_values']
    new = fit(changed)['current_feature_values']
    assert old['own_price_lags'] != new['own_price_lags']
    assert old['own_and_peer_usd_factor'][2:] == new['own_and_peer_usd_factor'][2:]
    assert old['own_and_peer_lags'][2:] == new['own_and_peer_lags'][2:]


def test_quote_units_and_pair_price_scale_do_not_change_normalized_features_or_forecasts():
    original = sample_bars()
    changed = deepcopy(original)
    scale = {pair: 10.0**(index-3) for index,pair in enumerate(PAIRS)}
    for row in changed:
        row['close_mid'] *= scale[row['instrument']]
    baseline = fit(original)
    rescaled = fit(changed)
    for name in ARMS:
        assert rescaled['current_feature_values'][name] == pytest.approx(baseline['current_feature_values'][name], abs=1e-9)
        assert rescaled['arms'][name]['predicted_return_bps'] == pytest.approx(baseline['arms'][name]['predicted_return_bps'], abs=1e-8)


def test_scalers_and_weights_exclude_current_peer_feature_even_for_extreme_outlier():
    rows = sample_bars()
    changed = deepcopy(rows)
    changed[-6]['close_mid'] *= 2  # Latest GBP/USD changes features, not EUR/USD labels.
    baseline = fit(rows)
    outlier = fit(changed)
    for name in ARMS:
        before, after = baseline['arms'][name], outlier['arms'][name]
        for key in ('training_scaler_mean','training_scaler_scale','standardized_coefficients_bps','intercept_bps'):
            assert after[key] == before[key]
    assert outlier['arms']['own_price_lags']['predicted_return_bps'] == baseline['arms']['own_price_lags']['predicted_return_bps']
    assert outlier['arms']['own_and_peer_lags']['predicted_return_bps'] != baseline['arms']['own_and_peer_lags']['predicted_return_bps']


def test_scaler_statistics_are_calculated_from_exact_selected_training_rows_only():
    rows = sample_bars()
    result = fit(rows, max_training_rows=40)
    assert result['selected_training_rows'] == 40
    assert result['eligible_training_rows'] == 190
    euro = {row['end_epoch']:row['close_mid'] for row in rows if row['instrument']=='EUR_USD'}
    x = np.asarray([[10_000*(np.log(euro[row['reference_epoch']])-np.log(euro[row['reference_epoch']-60*lag]))
                     for lag in (1,5)] for row in result['training_rows']])
    assert result['arms']['own_price_lags']['training_scaler_mean'] == pytest.approx(x.mean(axis=0), abs=1e-10)
    assert result['arms']['own_price_lags']['training_scaler_scale'] == pytest.approx(x.std(axis=0), abs=1e-10)


def test_constant_training_features_cannot_acquire_unlearned_outlier_exposure():
    rows = bars_from_returns(np.zeros((80, 7)))
    rows[-6]['close_mid'] *= 2
    result = fit(rows)
    for arm in result['arms'].values():
        assert arm['predicted_return_bps'] == 0
        assert arm['side'] == 0
        assert arm['constant_training_feature_names'] == arm['feature_names']


def test_synthetic_peer_leads_target_and_added_peer_lags_recover_the_signal():
    # The relation and every clock are synthetic; only prefixes are supplied.
    rng = np.random.default_rng(17)
    returns = rng.normal(0, 1, (280, 7))
    returns[1:,0] = 0.9*returns[:-1,1] + rng.normal(0,0.02,279)
    rows = bars_from_returns(returns)
    errors = {arm:[] for arm in ARMS}
    for n in range(230, 260):
        result = fit(rows[:n*7], horizon_sec=60, lag_minutes=(1,), min_training_rows=80, alpha=0.01)
        actual = returns[n,0]
        for arm in ARMS:
            errors[arm].append((result['arms'][arm]['predicted_return_bps']-actual)**2)
    assert np.mean(errors['own_and_peer_lags']) < 0.02*np.mean(errors['own_price_lags'])


def test_synthetic_peer_currency_factor_predicts_target_without_target_inside_factor():
    rng = np.random.default_rng(18)
    common = rng.normal(0, 1, 280)
    returns = np.zeros((280,7))
    returns[:,1:] = common[:,None] + rng.normal(0,0.1,(280,6))
    returns[1:,0] = 0.8*np.mean(returns[:-1,1:],axis=1) + rng.normal(0,0.01,279)
    rows = bars_from_returns(returns)
    own_error, factor_error = [], []
    for n in range(230,260):
        result = fit(rows[:n*7], horizon_sec=60, lag_minutes=(1,), min_training_rows=80, alpha=0.01)
        actual = returns[n,0]
        own_error.append((result['arms']['own_price_lags']['predicted_return_bps']-actual)**2)
        factor_error.append((result['arms']['own_and_peer_usd_factor']['predicted_return_bps']-actual)**2)
    assert np.mean(factor_error) < 0.02*np.mean(own_error)


@pytest.mark.parametrize('field', ['instrument','start_epoch','end_epoch','close_mid','available_epoch'])
def test_required_input_fields_cannot_be_inferred(field):
    rows = sample_bars()
    del rows[0][field]
    with pytest.raises(ValueError):
        fit(rows)


@pytest.mark.parametrize('field,value', [
    ('instrument','EUR_GBP'), ('instrument',None), ('start_epoch',True),
    ('start_epoch',1), ('end_epoch',120), ('end_epoch','60'),
    ('available_epoch',59), ('available_epoch',float('inf')), ('available_epoch',float('nan')),
    ('close_mid',True), ('close_mid',0), ('close_mid',-1), ('close_mid',float('nan')),
    ('complete',False), ('complete',1),
])
def test_malformed_rows_are_rejected_before_any_fit(field,value):
    rows = sample_bars()
    rows[0][field] = value
    with pytest.raises(ValueError):
        fit(rows)


@pytest.mark.parametrize('changes', [
    {'horizon_sec':59}, {'horizon_sec':61}, {'horizon_sec':True}, {'horizon_sec':3600.0},
    {'lag_minutes':()}, {'lag_minutes':(5,1)}, {'lag_minutes':(1,1)}, {'lag_minutes':(0,)},
    {'lag_minutes':(1,True)}, {'lag_minutes':(241,)}, {'lag_minutes':tuple(range(1,10))},
    {'alpha':0}, {'alpha':float('nan')}, {'alpha':1e-9}, {'alpha':True},
    {'min_training_rows':1}, {'max_training_rows':20}, {'max_training_rows':20_001},
    {'reference_epoch':1}, {'decision_epoch':float('inf')},
])
def test_protocol_parameters_are_explicit_validated_and_not_internally_tuned(changes):
    with pytest.raises(ValueError):
        fit(sample_bars(), **changes)
