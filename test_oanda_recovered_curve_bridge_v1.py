"""Bridge provenance/recomputation checks using the retained coefficients and synthetic inputs."""
from copy import deepcopy
from pathlib import Path

import pytest

import oanda_recovered_second_curve_v1 as recovered
from oanda_recovered_curve_bridge_v1 import prepare_recovered_computation
from oanda_forecast_curve_contract_v1 import CurveContractError, issue_curve, make_policy, validate_prepared

MODEL = Path(r'D:\ForexRecovery\revamp_20260908T1353Z\artifacts\legacy_model_components\state\second_ridge_models_v1.json')


@pytest.fixture(scope='module')
def model():
    if not MODEL.exists(): pytest.skip('retained coefficient artifact unavailable')
    return recovered.load_model(MODEL)


def evidence(model, *, scope='current_research', convention='official_midpoint', sampling='exact_grid'):
    base = (int(model.fitted_epoch)//5)*5+120
    labels = [base+i*5 for i in range(13)]
    if sampling == 'retained_fit_window': labels = [label+(5 if i >= 5 else 0) for i,label in enumerate(labels)]
    observed = labels[-1]+5.1
    rows = []
    for i, label in enumerate(labels):
        midpoint = 1.10+i*0.00001
        rows.append(dict(bar_start_epoch=label, available_epoch=observed, complete=True,
            bid_close=midpoint-0.0001, ask_close=midpoint+0.0001,
            mid_close=midpoint, mid_high=midpoint+0.00001, mid_low=midpoint-0.00001,
            spread_pips=2, volume=10+i))
    capture = recovered.capture_s5_rows(rows, instrument='EUR_USD', pip_size=.0001,
        source_sha256='7'*64, clock=lambda:observed, scope=scope,
        sampling_policy=sampling, price_convention=convention)
    clocks = iter((observed+.1,observed+.2))
    result = recovered.predict_curve(capture, model, clock=lambda:next(clocks))
    policy = make_policy(native_horizons_sec=list(recovered.HORIZONS), maximum_reference_age_sec=30,
        maximum_build_sec=10, maximum_issue_delay_sec=5, maximum_publication_delay_sec=5,
        maximum_decision_age_sec=14500)
    sources = {**result['original_source_bindings'], 'bridge_fixture.py':'9'*64}
    return capture, result, policy, sources


def prepared(model, **kwargs):
    capture, result, policy, sources = evidence(model, **kwargs)
    answer = prepare_recovered_computation(capture, result, model,
        source_bindings=sources, forecast_cohort='synthetic_bridge_unit_case', policy=policy)
    return answer, capture, result, sources


@pytest.mark.parametrize('convention', ['official_midpoint','ba_derived_midpoint'])
@pytest.mark.parametrize('sampling', ['exact_grid','retained_fit_window'])
def test_all_native_points_and_real_time_support_are_preserved(model, convention, sampling):
    answer, capture, result, sources = prepared(model, convention=convention, sampling=sampling)
    assert len(answer['nodes']) == 13
    assert [n['horizon_sec'] for n in answer['nodes']] == list(recovered.HORIZONS)
    assert answer['input_context']['price_convention'] == convention
    assert answer['input_context']['historical_ingestion_equivalence_proven'] is False
    assert answer['input_context']['sampling_metadata'] == result['sampling_metadata']
    assert answer['input_context']['sampling_metadata']['missing_s5_intervals'] == (1 if sampling == 'retained_fit_window' else 0)
    assert answer['target_selection_policy'] == dict(kind='first_complete_bar_at_or_after_nominal', maximum_delay_sec=7)
    assert answer['computation_sha256'] == result['result_sha256']
    assert answer['input_capture_sha256'] == capture['capture_sha256']
    assert validate_prepared(answer, expected_source_bindings=sources) == answer


@pytest.mark.parametrize('mutate', [
    lambda r: r['points'][0].update(predicted_signed_pips=r['points'][0]['predicted_signed_pips']+1),
    lambda r: r.update(price_convention='ba_derived_midpoint'),
    lambda r: r.update(can_place_orders=True),
    lambda r: r['points'][0].update(target_epoch=r['points'][0]['target_epoch']+5),
    lambda r: r.update(source_sha256='8'*64),
])
def test_resealed_computation_tampering_is_caught_by_numerical_replay(model, mutate):
    capture, result, policy, sources = evidence(model)
    mutate(result)
    result['result_sha256'] = recovered._hash({k:v for k,v in result.items() if k != 'result_sha256'})
    with pytest.raises(CurveContractError, match='recovered_computation_content_mismatch'):
        prepare_recovered_computation(capture, result, model, source_bindings=sources,
            forecast_cohort='synthetic_bridge_unit_case', policy=policy)


def test_original_model_transform_source_closure_is_required(model):
    capture, result, policy, _ = evidence(model)
    with pytest.raises(CurveContractError, match='original_source_closure_missing'):
        prepare_recovered_computation(capture, result, model, source_bindings={'fake.py':'0'*64},
            forecast_cohort='synthetic_bridge_unit_case', policy=policy)


def test_bridge_cannot_promote_engineering_replay_to_prospective_issue(model):
    answer, _, result, sources = prepared(model, scope='engineering_replay')
    assert answer['scope'] == 'engineering_replay'
    with pytest.raises(CurveContractError, match='nonprospective_computation_cannot_issue'):
        issue_curve(answer, expected_source_bindings=sources, clock=lambda:result['computed_epoch']+.1)


def test_changed_capture_is_not_accepted_merely_because_result_hash_matches_itself(model):
    capture, result, policy, sources = evidence(model)
    capture['rows'][0]['volume'] += 1
    with pytest.raises(CurveContractError, match='recovered_computation_replay_failed'):
        prepare_recovered_computation(capture, result, model, source_bindings=sources,
            forecast_cohort='synthetic_bridge_unit_case', policy=policy)
