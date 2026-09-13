"""Synthetic valid original chains; no market effectiveness evidence or GET."""
from copy import deepcopy
from decimal import Context, localcontext

import pytest

import oanda_curve_risk_attachment_v1 as a
import oanda_forecast_curve_contract_v1 as c
import oanda_m1_risk_distribution_contract_v1 as r
import test_oanda_m1_risk_distribution_contract_v1 as rf
from test_oanda_m1_risk_distribution_contract_v1 import inputs

REF = rf.REFERENCE
CS = {'oanda_forecast_curve_contract_v1.py': 'c' * 64}


def curve_chain(risk, **overrides):
    ref = overrides.pop('reference_epoch', REF)
    duration = overrides.get('bar_duration_sec', 5)
    horizons = [900, 1800, 3600]
    policy = c.make_policy(native_horizons_sec=horizons, maximum_reference_age_sec=30,
        maximum_build_sec=10, maximum_issue_delay_sec=5, maximum_publication_delay_sec=5,
        maximum_decision_age_sec=4000, minimum_remaining_sec=1)
    data = dict(instrument=risk['instrument'], pip_size='0.0001', forecast_cohort='exact_join_fixture',
        model_sha256='1' * 64, feature_version='synthetic_fixture_only', source_bindings=CS,
        input_capture_sha256='2' * 64, input_available_epoch=REF + 1,
        reference_epoch=ref, reference_label_epoch=ref-duration, reference_price=risk['reference_mid'],
        reference_price_kind=f'official_midpoint_{"S5" if duration == 5 else "M1"}_close',
        bar_duration_sec=duration, model_fitted_epoch=REF - 100000,
        computation_started_epoch=REF+1.1, computed_epoch=REF+1.2,
        points=[dict(horizon_sec=h, target_epoch=ref+h, target_label_epoch=ref-duration+h,
            model_id='fixture_model', predicted_signed_pips='2', probability_up='.6',
            probability_scope='original_return_uncalibrated', uncertainty_scope='residual_only')
            for h in horizons], policy=policy, computation_sha256='3' * 64,
        input_context={'price_convention': 'official_midpoint'})
    data.update(overrides)
    prepared = c.prepare_curve(**data)
    curve = c.issue_curve(prepared, expected_source_bindings=CS, clock=lambda: REF+2)
    pub = c.publication_receipt(curve, persisted_bytes_sha256=c.content_hash(curve),
        publication_started_epoch=REF+2.1, expected_source_bindings=CS, clock=lambda: REF+2.2)
    consume = c.consume_curve(curve, pub, expected_source_bindings=CS, clock=lambda: REF+2.3)
    return curve, pub, consume


def refresh_reads(args, kw, *, start=REF+7):
    kw['attachment_read_receipt'] = {'schema_version': a.READ_SCHEMA, 'objects': {
        name: dict(canonical_sha256=r.digest(value), read_started_epoch=start+i*.2,
                   read_completed_epoch=start+i*.2+.1)
        for i, (name, value) in enumerate(zip(a.OBJECT_KEYS, args))}}


def bundle(inputs, **curve_overrides):
    risk, rp, rc = rf.chain(inputs)
    args = (*curve_chain(risk, **curve_overrides), risk, rp, rc)
    p = args[0]['prepared_curve']
    kw = dict(risk_input_raw=inputs['input_raw'], risk_input_receipt=inputs['input_receipt'],
        metadata=inputs['metadata'], fit_raw=inputs['fit_raw'], expected_curve_sources=CS,
        expected_risk_sources=inputs['expected_source_bindings'],
        expected_curve_identity={k: p[k] for k in ('forecast_cohort', 'model_sha256')},
        decision_epoch=REF+10, target_epoch=REF+900, side_context=dict(role='candidate', side=1))
    kw['expected_curve_identity']['policy_sha256'] = p['policy']['policy_sha256']
    refresh_reads(args, kw)
    return args, kw


def run(args, kw):
    return a.attach_risk_for_target(*args, **kw, clock=lambda: kw['decision_epoch']+.5)


def test_exact_s5_m1_effective_event_keeps_labels_and_both_methods(inputs):
    args, kw = bundle(inputs)
    old = deepcopy((args, kw))
    result = run(args, kw)
    assert result['status'] == 'attached_original_horizon_diagnostic'
    assert len(result['risk_nodes']) == 16
    assert {n['method'] for n in result['risk_nodes']} == set(r.METHODS)
    assert result['curve_event']['reference_label_epoch'] == REF-5
    assert result['risk_event']['reference_label_epoch'] == REF-60
    assert result['curve_event']['reference_price_epoch'] == result['risk_event']['reference_price_epoch']
    for n in result['risk_nodes']:
        assert n in args[3]['nodes']
    assert result['interpretation']['conditional_remaining_distribution'] is False
    assert result['interpretation']['actual_position_entry_basis_verified'] is False
    assert result['attachment_computed_epoch'] > result['decision_epoch']
    assert (args, kw) == old
    assert a.validate_attachment(result, *args, **kw) == result


@pytest.mark.parametrize('duration', [5, 60])
def test_explicit_bar_kind_can_differ_but_effective_event_cannot(inputs, duration):
    args, kw = bundle(inputs, bar_duration_sec=duration)
    assert run(args, kw)['status'] == 'attached_original_horizon_diagnostic'


@pytest.mark.parametrize('overrides, reason', [
    ({'reference_epoch': REF-5}, 'reference_price_epoch_mismatch'),
    ({'reference_price': '9.99'}, 'reference_price_mismatch'),
    ({'input_context': {'price_convention': 'ba_derived_midpoint'}}, 'reference_price_convention_mismatch'),
    ({'input_context': {}}, 'reference_price_convention_mismatch'),
    ({'reference_price_kind': 'unknown_mid'}, 'reference_price_kind_unproven'),
    ({'target_selection_policy': {'kind': 'first_complete_bar_at_or_after_nominal', 'maximum_delay_sec': 7}},
     'target_selection_window_mismatch'),
    ({'instrument': 'GBP_USD'}, 'instrument_mismatch'),
    ({'pip_size': '0.001'}, 'pip_size_mismatch'),
])
def test_valid_but_different_original_events_refuse_without_risk_zero(inputs, overrides, reason):
    args, kw = bundle(inputs, **overrides)
    result = run(args, kw)
    assert result['status'] == 'unavailable' and reason in result['reason_codes']
    assert result['risk_nodes'] == []
    assert result['interpretation']['missing_is_measured_zero'] is False
    assert a.validate_attachment(result, *args, **kw) == result


def test_reference_decimal_value_equality_does_not_round(inputs):
    risk = rf.chain(inputs)[0]
    args, kw = bundle(inputs, reference_price=risk['reference_mid']+'0')
    assert run(args, kw)['status'] == 'attached_original_horizon_diagnostic'


@pytest.mark.parametrize('side,role,n', [(1,'candidate',16), (-1,'incumbent',16), (0,'distribution_context',8)])
def test_side_context_selects_explicit_label_family_without_direction_claim(inputs, side, role, n):
    args, kw = bundle(inputs)
    kw['side_context'] = dict(role=role, side=side)
    result = run(args, kw)
    assert len(result['risk_nodes']) == n
    assert result['side_context'] == kw['side_context']
    assert not result['interpretation']['candidate_direction_verified']
    assert not result['interpretation']['live_position_verified']
    assert not any(v['label'].startswith('long_') for v in result['risk_nodes']) if side <= 0 else True
    assert not any(v['label'].startswith('short_') for v in result['risk_nodes']) if side >= 0 else True


@pytest.mark.parametrize('value', [None, {}, {'role':'candidate','side':0}, {'role':'incumbent','side':True},
                                  {'role':'distribution_context','side':1}, {'role':'chosen_winner','side':1}])
def test_invalid_side_context_rejects(inputs, value):
    args, kw = bundle(inputs); kw['side_context'] = value
    with pytest.raises(ValueError, match='attachment_'):
        run(args, kw)


@pytest.mark.parametrize('kind', ['hash','future','before_original','reordered','missing','extra'])
def test_downstream_each_object_identity_and_clock_required(inputs, kind):
    args, kw = bundle(inputs)
    rows = kw['attachment_read_receipt']['objects']
    if kind == 'hash': rows['risk_issue']['canonical_sha256'] = 'f'*64
    elif kind == 'future': rows['risk_consumption']['read_completed_epoch'] = REF+11
    elif kind == 'before_original': rows['curve']['read_started_epoch'] = REF+1
    elif kind == 'reordered': rows['risk_consumption']['read_started_epoch'] = REF+7
    elif kind == 'missing': rows.pop('curve_consumption')
    else: rows['other'] = deepcopy(rows['curve'])
    with pytest.raises(ValueError, match='attachment_'):
        run(args, kw)


@pytest.mark.parametrize('index,field', [(0,'curve_sha256'),(1,'publication_sha256'),(2,'consumption_sha256'),
                                      (3,'issued_sha256'),(4,'publication_sha256'),(5,'consumption_sha256')])
def test_original_chain_seals_cannot_be_bypassed_by_downstream_rehash(inputs, index, field):
    args, kw = bundle(inputs)
    args[index][field] = 'f'*64
    refresh_reads(args, kw)
    with pytest.raises(ValueError): run(args, kw)


@pytest.mark.parametrize('index', range(6))
def test_all_original_authority_flags_remain_inert(inputs, index):
    args, kw = bundle(inputs)
    args[index]['can_place_orders'] = True
    refresh_reads(args, kw)
    with pytest.raises(ValueError): run(args, kw)


@pytest.mark.parametrize('kind', ['curve_sources','risk_sources','model','cohort','policy','raw','fit'])
def test_registry_raw_and_fit_identity_checked_before_mismatch_shortcut(inputs, kind):
    args, kw = bundle(inputs, reference_price='9.9')
    if kind == 'curve_sources': kw['expected_curve_sources'] = {'oanda_forecast_curve_contract_v1.py':'f'*64}
    elif kind == 'risk_sources': kw['expected_risk_sources'] = {'oanda_m1_risk_distribution_contract_v1.py':'f'*64}
    elif kind in ('model','cohort','policy'):
        kw['expected_curve_identity'][{'model':'model_sha256','cohort':'forecast_cohort','policy':'policy_sha256'}[kind]] = 'f'*64
    elif kind == 'raw': kw['risk_input_raw'] += b' '
    else: kw['fit_raw'] += b' '
    with pytest.raises(ValueError): run(args, kw)


@pytest.mark.parametrize('delay,reason', [(900,'original_target_elapsed_at_decision'),
                                       (899.5,'curve_minimum_remaining_not_met'),
                                       (5000,'curve_too_old_at_decision')])
def test_expiry_is_checked_at_original_decision(inputs, delay, reason):
    args, kw = bundle(inputs); kw['decision_epoch'] = REF+delay
    result = run(args, kw)
    assert reason in result['reason_codes'] and not result['risk_nodes']


def test_native_horizon_absence_is_not_interpolated(inputs):
    args, kw = bundle(inputs); kw['target_epoch'] = REF+1200
    result = run(args, kw)
    assert result['reason_codes'] == ['curve_native_target_unavailable','risk_exact_native_target_unavailable']


def test_resealed_output_change_and_authority_rejected_by_full_replay(inputs):
    args, kw = bundle(inputs)
    result = run(args, kw)
    result['risk_nodes'][0]['quantile_bps'][0] -= .01
    result['attachment_sha256'] = r.digest({k:v for k,v in result.items() if k!='attachment_sha256'})
    with pytest.raises(ValueError, match='attachment_replay_mismatch'):
        a.validate_attachment(result, *args, **kw)
    result = run(args, kw); result['can_promote'] = True
    with pytest.raises(ValueError, match='authority'):
        a.validate_attachment(result, *args, **kw)


def test_output_is_detached_and_ambient_decimal_context_is_irrelevant(inputs):
    args, kw = bundle(inputs)
    normal = run(args, kw)
    with localcontext(Context(prec=2)):
        low = run(args, kw)
    assert normal == low
    normal['risk_nodes'][0]['quantile_bps'][0] = 100
    normal['policy']['risk_methods'].clear()
    normal['attachment_read_receipt']['objects'].clear()
    assert run(args, kw) == low


@pytest.mark.parametrize('clock', [REF+9, True, float('inf')])
def test_new_computation_clock_never_backdates(inputs, clock):
    args, kw = bundle(inputs)
    with pytest.raises(ValueError):
        a.attach_risk_for_target(*args, **kw, clock=lambda: clock)
