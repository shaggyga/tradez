"""Clock and provenance boundary fixtures; these are not market-performance evidence."""
from copy import deepcopy
from decimal import Decimal, localcontext

import pytest

import oanda_forecast_curve_contract_v1 as c


SOURCES = {'retained_model_adapter.py': '1'*64, 'curve_contract.py': '2'*64}


def inputs(**overrides):
    result = dict(
        instrument='EUR_USD', pip_size='0.0001', forecast_cohort='unit_fixture',
        model_sha256='3'*64, feature_version='fixture_14', source_bindings=SOURCES,
        input_capture_sha256='4'*64, input_available_epoch=1005.2,
        reference_epoch=1005, reference_label_epoch=1000, reference_price='1.1000',
        reference_price_kind='fixture_mid_close', bar_duration_sec=5,
        model_fitted_epoch=900, computation_started_epoch=1005.3, computed_epoch=1005.4,
        points=[dict(horizon_sec=h, target_epoch=1005+h, target_label_epoch=1000+h,
                     model_id='retained_fixture_model', predicted_signed_pips='3',
                     probability_up='0.7', probability_scope='original_return_uncalibrated',
                     residual_std_pips='2.5', uncertainty_scope='training_residual_only')
                for h in (15, 30, 60)],
        policy=c.make_policy(native_horizons_sec=[15, 30, 60], maximum_reference_age_sec=30,
            maximum_build_sec=10, maximum_issue_delay_sec=25, maximum_publication_delay_sec=5,
            maximum_decision_age_sec=90, minimum_remaining_sec=0),
        computation_sha256='5'*64)
    result['points'][1].update(quantile_low_pips='-2', quantile_high_pips='7',
                              quantile_levels=['0.1', '0.9'], interval_scope='original_unconditional_return')
    result.update(overrides)
    return result


def chain(**overrides):
    prepared = c.prepare_curve(**inputs(**overrides))
    curve = c.issue_curve(prepared, expected_source_bindings=SOURCES, clock=lambda: 1005.5)
    publication = c.publication_receipt(curve, persisted_bytes_sha256=c.content_hash(curve),
        publication_started_epoch=1005.6, expected_source_bindings=SOURCES, clock=lambda: 1005.8)
    consumption = c.consume_curve(curve, publication, expected_source_bindings=SOURCES, clock=lambda: 1006)
    return prepared, curve, publication, consumption


def reseal(record, key, id_key=None, prefix=''):
    """An attacker can recalculate hashes; semantic validation must still reject contradictions."""
    body = {k: v for k, v in record.items() if k not in (key, id_key)}
    record[key] = c.content_hash(body)
    if id_key:
        record[id_key] = prefix+record[key]


def test_native_targets_and_uncertainty_survive_all_receipts():
    prepared, curve, publication, consumption = chain()
    assert [n['original_target_epoch'] for n in prepared['nodes']] == [1020., 1035., 1065.]
    assert [n['target_label_epoch'] for n in prepared['nodes']] == [1015., 1030., 1060.]
    assert {n['expected_terminal_price'] for n in prepared['nodes']} == {'1.1003'}
    assert prepared['nodes'][0]['interval'] is None  # sigma is not a fabricated quantile interval.
    assert prepared['nodes'][1]['interval'] == dict(low_pips='-2', high_pips='7',
        levels=['0.1', '0.9'], scope='original_unconditional_return')
    assert (curve['issued_epoch'], publication['publication_completed_epoch'], consumption['available_epoch']) == (1005.5, 1005.8, 1006.)
    assert c.validate_consumption(curve, publication, consumption, expected_source_bindings=SOURCES) == consumption
    for record in (prepared, curve, publication, consumption, *prepared['nodes']):
        assert record['research_only'] is True
        assert all(record[k] is False for k in c.AUTHORITY if k != 'research_only')


@pytest.mark.parametrize('overrides,reason', [
    ({'reference_label_epoch': 1001}, 'reference_label_price_clock_mismatch'),
    ({'reference_epoch': 1000}, 'reference_label_price_clock_mismatch'),
    ({'input_available_epoch': 1004.9}, 'input_model_computation_clock_order'),
    ({'input_available_epoch': 1005.35}, 'input_model_computation_clock_order'),
    ({'computed_epoch': 1005.1}, 'input_model_computation_clock_order'),
    ({'model_fitted_epoch': 1005.25}, 'current_input_precedes_model_fit'),
    ({'model_fitted_epoch': 1006}, 'input_model_computation_clock_order'),
    ({'computed_epoch': 1016}, 'computation_too_slow'),
    ({'computation_started_epoch': 1035, 'computed_epoch': 1036}, 'reference_stale_at_computation'),
    ({'pip_size': '0'}, 'nonpositive_number'),
    ({'pip_size': '1'}, 'pip_scale_invalid'),
    ({'instrument': 'USD_USD'}, 'identical_currencies'),
])
def test_bad_computation_boundaries(overrides, reason):
    with pytest.raises(c.CurveContractError, match=reason): c.prepare_curve(**inputs(**overrides))


@pytest.mark.parametrize('value', [True, float('nan'), float('inf'), '-Infinity', '0e-100000000', '1e-100000000', '1e21'])
def test_number_limits_reject_without_unbounded_decimal_formatting(value):
    with pytest.raises(c.CurveContractError): c.prepare_curve(**inputs(pip_size=value))


@pytest.mark.parametrize('horizons', [[15,15,60], [30,15,60], [True,30], [15,{}], [], [0,15], [15.0,30]])
def test_native_inventory_has_no_duplicates_sorting_or_boolean_shortcuts(horizons):
    with pytest.raises(c.CurveContractError, match='invalid_native_horizons'):
        c.make_policy(native_horizons_sec=horizons, maximum_reference_age_sec=30,
            maximum_build_sec=10, maximum_issue_delay_sec=10, maximum_publication_delay_sec=5,
            maximum_decision_age_sec=60)


@pytest.mark.parametrize('mutation,reason', [
    (lambda p: p['points'].pop(), 'native_node_inventory_required'),
    (lambda p: p['points'][0].update(horizon_sec=True), 'node_shape'),
    (lambda p: p['points'][0].update(target_epoch=1020.5), 'native_target_changed'),
    (lambda p: p['points'][0].update(probability_up='1.01'), 'probability_out_of_range'),
    (lambda p: p['points'][0].update(predicted_signed_pips='-12000'), 'nonpositive_terminal_price'),
    (lambda p: p['points'][1].update(quantile_high_pips=None), 'incomplete_interval'),
    (lambda p: p['points'][1].update(quantile_low_pips='8'), 'interval_reversed'),
    (lambda p: p['points'][1].update(quantile_levels=['0.9','0.1']), 'quantile_levels_invalid'),
    (lambda p: p['points'][0].update(nested={'can_place_orders': True}), 'authority_not_false'),
])
def test_node_boundaries(mutation, reason):
    args = inputs(); mutation(args)
    with pytest.raises(c.CurveContractError, match=reason): c.prepare_curve(**args)


@pytest.mark.parametrize('scope', ['engineering_replay', 'synthetic_fixture'])
def test_replay_is_never_issued_as_prospective(scope):
    prepared = c.prepare_curve(**inputs(scope=scope))
    with pytest.raises(c.CurveContractError, match='nonprospective_computation_cannot_issue'):
        c.issue_curve(prepared, expected_source_bindings=SOURCES, clock=lambda: 1005.5)


def test_late_issue_withholds_elapsed_short_node_without_moving_long_targets():
    prepared = c.prepare_curve(**inputs())
    curve = c.issue_curve(prepared, expected_source_bindings=SOURCES, clock=lambda: 1021)
    assert [n['status'] for n in curve['node_admission']] == ['withheld', 'admitted', 'admitted']
    assert [n['original_target_epoch'] for n in curve['prepared_curve']['nodes']] == [1020,1035,1065]


def test_unavailable_is_preserved_and_not_a_zero_prediction():
    args = inputs(); args['points'][0].update(status='unavailable', reason_code='model_missing')
    prepared = c.prepare_curve(**args)
    assert 'predicted_signed_pips' not in prepared['nodes'][0]
    curve = c.issue_curve(prepared, expected_source_bindings=SOURCES, clock=lambda: 1005.5)
    assert curve['node_admission'][0]['reason_code'] == 'model_missing'


def test_resealed_target_tampering_is_still_rejected():
    prepared = c.prepare_curve(**inputs())
    prepared['nodes'][0]['original_target_epoch'] += 1
    reseal(prepared['nodes'][0], 'node_sha256', 'node_id', 'curve_node_v1_')
    reseal(prepared, 'prepared_sha256')
    with pytest.raises(c.CurveContractError, match='native_target_changed'):
        c.validate_prepared(prepared, expected_source_bindings=SOURCES)


def test_source_mismatch_and_unbound_extra_fields_are_rejected():
    prepared = c.prepare_curve(**inputs())
    with pytest.raises(c.CurveContractError, match='unregistered_source_bindings'):
        c.validate_prepared(prepared, expected_source_bindings={'adapter.py': '9'*64})
    prepared['extra_policy'] = 'ignore_targets'
    reseal(prepared, 'prepared_sha256')
    with pytest.raises(c.CurveContractError, match='prepared_shape'):
        c.validate_prepared(prepared, expected_source_bindings=SOURCES)


@pytest.mark.parametrize('issue,reason', [(1005.3,'issue_clock_or_delay'), (1031,'issue_clock_or_delay')])
def test_issue_time_bounds(issue, reason):
    with pytest.raises(c.CurveContractError, match=reason):
        c.issue_curve(c.prepare_curve(**inputs()), expected_source_bindings=SOURCES, clock=lambda: issue)


@pytest.mark.parametrize('start,finish,checksum,reason', [
    (1005.6,1005.8,'0'*64,'persisted_curve_bytes_mismatch'),
    (1005.4,1005.8,None,'publication_clock_order'),
    (1005.8,1005.7,None,'publication_clock_order'),
    (1005.6,1011,None,'publication_too_slow')])
def test_publication_must_bind_actual_persisted_bytes_and_clock(start, finish, checksum, reason):
    _, curve, _, _ = chain()
    with pytest.raises(c.CurveContractError, match=reason):
        c.publication_receipt(curve, persisted_bytes_sha256=checksum or c.content_hash(curve),
            publication_started_epoch=start, expected_source_bindings=SOURCES, clock=lambda: finish)


@pytest.mark.parametrize('observed,reason', [(1005.7,'consumption_precedes_publication'), (1096,'curve_too_old_at_consumption')])
def test_consumer_cannot_know_unpublished_or_stale_curve(observed, reason):
    _, curve, publication, _ = chain()
    with pytest.raises(c.CurveContractError, match=reason):
        c.consume_curve(curve, publication, expected_source_bindings=SOURCES, clock=lambda: observed)


def test_arithmetic_is_independent_of_callers_decimal_context():
    ordinary = c.prepare_curve(**inputs(reference_price='1.10000123'))
    with localcontext() as ctx:
        ctx.prec = 3
        low_precision = c.prepare_curve(**inputs(reference_price='1.10000123'))
    assert low_precision == ordinary
    assert ordinary['nodes'][0]['expected_terminal_price'] == '1.10030123'


def test_consumer_receipt_cannot_be_relabelled_for_different_publication():
    _, curve, publication, consumption = chain()
    consumption['publication_sha256'] = '0'*64
    reseal(consumption, 'consumption_sha256')
    with pytest.raises(c.CurveContractError, match='consumption_semantic_mismatch'):
        c.validate_consumption(curve, publication, consumption, expected_source_bindings=SOURCES)


def test_generator_inventory_is_not_eagerly_exhausted():
    def forbidden_generator():
        raise AssertionError('must not consume this potentially unbounded iterator')
        yield 15
    with pytest.raises(c.CurveContractError, match='invalid_native_horizons'):
        c.make_policy(native_horizons_sec=forbidden_generator(), maximum_reference_age_sec=30,
            maximum_build_sec=10, maximum_issue_delay_sec=10, maximum_publication_delay_sec=5,
            maximum_decision_age_sec=60)


def test_nonstring_keys_cannot_alias_valid_json_key_hashes():
    with pytest.raises(c.CurveContractError, match='json_string_keys_required'): c.content_hash({1:'value'})
    assert c.content_hash({'1':'value'})


@pytest.mark.parametrize('field', ['nodes', 'policy', 'input_available_epoch', 'target_selection_policy'])
def test_missing_resealed_fields_return_bounded_errors(field):
    prepared = c.prepare_curve(**inputs())
    prepared.pop(field); reseal(prepared, 'prepared_sha256')
    with pytest.raises(c.CurveContractError, match='record_required_fields_missing'):
        c.validate_prepared(prepared, expected_source_bindings=SOURCES)


@pytest.mark.parametrize('policy', [
    {'kind':'exact_price_epoch','maximum_delay_sec':7},
    {'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':0},
    {'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':True},
    {'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7,'retimestamp':True}])
def test_target_selection_policy_is_not_silently_broadened(policy):
    with pytest.raises(c.CurveContractError): c.prepare_curve(**inputs(target_selection_policy=policy))


def test_training_target_tolerance_remains_anchored_at_original_nominal_time():
    prepared, curve, publication, consumption = chain(target_selection_policy={
        'kind':'first_complete_bar_at_or_after_nominal','maximum_delay_sec':7})
    assert [n['original_target_epoch'] for n in prepared['nodes']] == [1020.,1035.,1065.]
    assert [n['target_price_window_end_epoch'] for n in prepared['nodes']] == [1027.,1042.,1072.]
    assert c.validate_consumption(curve, publication, consumption, expected_source_bindings=SOURCES) == consumption
