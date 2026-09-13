"""Semantic/causal fixtures only. No market outcomes or policy optimization."""
from copy import deepcopy
from decimal import localcontext

import pytest

import oanda_curve_direction_admission_v1 as a
import oanda_forecast_curve_contract_v1 as c
from test_oanda_forecast_curve_contract_v1 import SOURCES, chain, inputs, reseal
from test_oanda_curve_management_adapter_v1 import METADATA, quote


def fixture(*, pips='3', **overrides):
    values = inputs()
    points = deepcopy(values['points'])
    for point in points:
        point['predicted_signed_pips'] = pips
    _, curve, pub, consumption = chain(points=points)
    arguments = dict(decision_epoch=1010, target_epoch=1035, quote=quote(),
                     metadata=deepcopy(METADATA), expected_source_bindings=deepcopy(SOURCES),
                     maximum_quote_age_sec=5)
    arguments.update(overrides)
    return curve, pub, consumption, arguments


def held(side=1, **overrides):
    result = dict(instrument='EUR_USD', side=side, original_target_epoch=1035,
                  observed_epoch=1008, state_sha256='a'*64)
    result.update(overrides)
    return result


def admit(**overrides):
    curve, pub, consumption, arguments = fixture(**overrides)
    return a.admit_candidate_for_target(curve, pub, consumption, **arguments)


@pytest.mark.parametrize('pips,bid,ask,side', [('3','1.1000','1.1002',1), ('-3','1.1000','1.1002',-1)])
def test_same_direction_meets_only_semantic_condition(pips,bid,ask,side):
    result = admit(pips=pips, quote=quote(bid=bid,ask=ask))
    assert result['original_forecast_side'] == result['rebased_remaining_side'] == side
    assert result['new_entry']['semantic_admitted'] is True
    assert result['new_entry']['candidate'] == result['candidate']
    assert result['continuation']['status'] == 'available_unassigned_estimate'
    assert result['execution_eligible'] is False
    assert result['fresh_input_update_verified'] is False
    assert 'necessary' in result['scope']


def test_synthetic_episode02_geometry_rejects_old_short_as_new_long_and_keeps_value():
    result = admit(pips='-3', quote=quote(bid='1.0994',ask='1.0996'), incumbent=held(-1))
    assert result['original_forecast_side'] == -1 and result['rebased_remaining_side'] == 1
    assert result['candidate']['expected_terminal_price'] == '1.0997'
    assert result['candidate']['expected_remaining_move_pips'] == '2'
    assert result['new_entry'] == dict(semantic_admitted=False,
        reason_code='countertrend_from_original_terminal_crossing',candidate=None)
    assert result['opposite_side_rotation']['semantic_admitted'] is False
    assert result['continuation']['status'] == 'available_for_incumbent'
    assert result['continuation']['incumbent_signed_remaining_move_pips'] == '-2'
    assert result['continuation']['candidate'] == result['candidate']


def test_opposite_original_long_crossing_is_symmetric():
    result = admit(quote=quote(bid='1.1004',ask='1.1006'),incumbent=held(1))
    assert result['original_forecast_side'] == 1 and result['rebased_remaining_side'] == -1
    assert result['new_entry']['semantic_admitted'] is False
    assert result['continuation']['incumbent_signed_remaining_move_pips'] == '-2'


def test_preexisting_countertrend_position_keeps_positive_continuation_without_new_entry_admission():
    result = admit(pips='-3', quote=quote(bid='1.0994',ask='1.0996'), incumbent=held(1))
    assert not result['new_entry']['semantic_admitted']
    assert result['continuation']['status'] == 'available_for_incumbent'
    assert result['continuation']['incumbent_signed_remaining_move_pips'] == '2'
    assert result['opposite_side_rotation']['semantic_admitted'] is False


@pytest.mark.parametrize('bid,ask,remaining_side', [('1.0998','1.1000',1),('1.1000','1.1002',-1),('1.0999','1.1001',0)])
def test_neutral_original_never_becomes_entry_evidence_by_drift(bid,ask,remaining_side):
    result = admit(pips='0',quote=quote(bid=bid,ask=ask),incumbent=held(-1))
    assert result['original_forecast_side'] == 0 and result['rebased_remaining_side'] == remaining_side
    assert result['new_entry']['reason_code'] == 'neutral_original_forecast_is_not_new_directional_evidence'
    assert result['new_entry']['candidate'] is None
    assert result['continuation']['status'] == 'available_for_incumbent'


def test_zero_remaining_is_valid_continuation_not_an_entry():
    result = admit(quote=quote(bid='1.1002',ask='1.1004'),incumbent=held(1))
    assert result['rebased_remaining_side'] == 0
    assert result['new_entry']['reason_code'] == 'zero_remaining_move'
    assert result['continuation']['incumbent_signed_remaining_move_pips'] == '0'


@pytest.mark.parametrize('side,admitted,reason', [(1,False,'same_as_incumbent_side_not_rotation'),
                                                (-1,True,'original_direction_consistent_opposite_side')])
def test_rotation_requires_own_original_direction_consistency_and_opposite_incumbent(side,admitted,reason):
    result = admit(incumbent=held(side))
    assert result['opposite_side_rotation']['semantic_admitted'] is admitted
    assert result['opposite_side_rotation']['reason_code'] == reason
    assert result['new_entry']['semantic_admitted'] is True


def test_legitimate_independently_supplied_original_forecast_can_have_opposite_issued_direction():
    old = admit(pips='-3',quote=quote(bid='1.0994',ask='1.0996'))
    new = admit(pips='3',quote=quote(bid='1.0994',ask='1.0996'),incumbent=held(-1))
    assert old['original_curve_sha256'] != new['original_curve_sha256']
    assert not old['new_entry']['semantic_admitted'] and new['new_entry']['semantic_admitted']
    assert new['opposite_side_rotation']['semantic_admitted']
    # A distinct receipt alone is not proof that independent input information advanced.
    assert not new['fresh_input_update_verified']
    assert new['forecast_update_status'] == 'not_assessed_by_direction_gate'


def test_reread_hash_refresh_never_changes_original_direction_or_claims_updated_inputs():
    curve,pub,old,args = fixture(pips='-3',quote=quote(bid='1.0994',ask='1.0996'))
    first = a.admit_candidate_for_target(curve,pub,old,**args)
    reread = c.consume_curve(curve,pub,expected_source_bindings=SOURCES,clock=lambda:1009.5)
    second = a.admit_candidate_for_target(curve,pub,reread,**args)
    assert first['original_consumption_sha256'] != second['original_consumption_sha256']
    assert first['input_candidate_sha256'] != second['input_candidate_sha256']
    assert first['original_forecast_side'] == second['original_forecast_side'] == -1
    assert not second['new_entry']['semantic_admitted'] and not second['fresh_input_update_verified']


def test_reissuing_same_computation_is_not_certified_as_a_fresh_update():
    curve,pub,consumption,args = fixture(pips='-3',quote=quote(bid='1.0994',ask='1.0996'))
    changed = c.issue_curve(curve['prepared_curve'],expected_source_bindings=SOURCES,clock=lambda:1007)
    newpub = c.publication_receipt(changed,persisted_bytes_sha256=c.content_hash(changed),publication_started_epoch=1007.1,
        expected_source_bindings=SOURCES,clock=lambda:1007.2)
    seen = c.consume_curve(changed,newpub,expected_source_bindings=SOURCES,clock=lambda:1007.3)
    result = a.admit_candidate_for_target(changed,newpub,seen,**args)
    assert not result['fresh_input_update_verified'] and not result['new_entry']['semantic_admitted']


@pytest.mark.parametrize('overrides,reason', [
    ({'decision_epoch':1096},'curve_too_old_at_decision'),
    ({'decision_epoch':1035},'native_target_elapsed_before_decision'),
    ({'target_epoch':1040},'requested_native_target_unavailable'),
    ({'quote':None},'decision_quote_missing'),
    ({'quote':quote(tradeable=False)},'decision_quote_not_tradeable'),
    ({'maximum_quote_age_sec':.5},'decision_quote_stale'),
])
def test_frozen_operational_refusals_remain_unavailable_without_zero_forecasts(overrides,reason):
    result = admit(**overrides)
    assert result['status'] == 'unavailable' and result['reason_code'] == reason
    assert result['original_forecast_side'] is None and result['rebased_remaining_side'] is None
    assert result['new_entry']['candidate'] is result['continuation']['candidate'] is None


@pytest.mark.parametrize('overrides', [
    {'decision_epoch':1005.9}, {'decision_epoch':True}, {'decision_epoch':float('nan')},
    {'quote':quote(available_epoch=1011)}, {'quote':quote(instrument='GBP_USD')},
    {'expected_source_bindings':{'wrong.py':'1'*64}}, {'maximum_quote_age_sec':True},
    {'incumbent':held(side=True)}, {'incumbent':held(side=0)}, {'incumbent':held(observed_epoch=1011)},
    {'incumbent':held(state_sha256='bad')}, {'incumbent':{'side':1}},
    {'incumbent':held(instrument='')}, {'incumbent':held(instrument='USD_USD')},
    {'incumbent':held(instrument='EUR_USD extra')},
])
def test_invalid_clocks_identity_and_state_descriptors_raise(overrides):
    with pytest.raises(c.CurveContractError): admit(**overrides)


@pytest.mark.parametrize('descriptor,reason',[ (held(original_target_epoch=1065),'incumbent_native_target_mismatch'),
                                               (held(instrument='GBP_USD'),'incumbent_instrument_mismatch')])
def test_incomparable_incumbent_does_not_get_silent_retargeting(descriptor,reason):
    result = admit(incumbent=descriptor)
    assert result['continuation']['status'] == 'incomparable'
    assert result['continuation']['reason_code'] == reason
    assert result['continuation']['candidate'] is None
    assert result['new_entry']['semantic_admitted']  # A separate flat-entry semantic question.
    assert not result['opposite_side_rotation']['semantic_admitted']


def test_input_and_output_tampering_is_not_saved_by_resealing():
    curve,pub,consumption,args = fixture(quote=quote(bid='1.1004',ask='1.1006'))
    result = a.admit_candidate_for_target(curve,pub,consumption,**args)
    result['new_entry']['semantic_admitted'] = True
    result['admission_sha256'] = c.content_hash({k:v for k,v in result.items() if k!='admission_sha256'})
    with pytest.raises(c.CurveContractError,match='direction_admission_semantic_mismatch'):
        a.validate_admission(result,curve,pub,consumption,**args)
    curve['prepared_curve']['nodes'][1]['predicted_signed_pips'] = '-3'
    reseal(curve,'curve_sha256','curve_id','forecast_curve_v1_')
    with pytest.raises(c.CurveContractError): a.admit_candidate_for_target(curve,pub,consumption,**args)


def test_original_uncertainty_is_preserved_without_new_conditional_probability_or_risk_attachment():
    result = admit(quote=quote(bid='1.1004',ask='1.1006'))
    assert result['original_prediction']['original_probability_up'] == '0.7'
    assert result['original_prediction']['probability_is_remaining_move_probability'] is False
    assert result['candidate']['original_residual_std_pips'] == '2.5'
    assert result['risk_distribution_attachment'] is None
    assert all(result[k] is v for k,v in c.AUTHORITY.items())


def test_input_immutability_detached_channels_hash_and_low_decimal_context():
    curve,pub,consumption,args = fixture(incumbent=held())
    before = deepcopy((curve,pub,consumption,args))
    result = a.admit_candidate_for_target(curve,pub,consumption,**args)
    assert (curve,pub,consumption,args) == before
    with localcontext() as ctx:
        ctx.prec = 3
        low = a.admit_candidate_for_target(curve,pub,consumption,**args)
    assert low == result
    assert result['admission_sha256'] == c.content_hash({k:v for k,v in result.items() if k!='admission_sha256'})
    validated = a.validate_admission(result,curve,pub,consumption,**args)
    validated['continuation']['candidate']['side'] = -1
    assert result['continuation']['candidate']['side'] == 1
    result['candidate']['side'] = -1
    assert result['new_entry']['candidate']['side'] == result['continuation']['candidate']['side'] == 1
