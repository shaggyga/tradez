"""Independent price/clock boundary fixtures, not performance evidence."""
from copy import deepcopy
from decimal import localcontext

import pytest

from oanda_curve_management_adapter_v1 import candidate_for_target
from oanda_forecast_curve_contract_v1 import CurveContractError, content_hash
from test_oanda_forecast_curve_contract_v1 import SOURCES, chain


METADATA = dict(instrument='EUR_USD', base_currency='EUR', quote_currency='USD', pip_size='0.0001')


def candidate(*, chain_overrides=None, **overrides):
    _, curve, publication, consumption = chain(**(chain_overrides or {}))
    args = dict(decision_epoch=1010, target_epoch=1035,
        quote=dict(instrument='EUR_USD', quote_id='quote_1009', bid='1.1000', ask='1.1002',
                   market_epoch=1009, available_epoch=1009.1, tradeable=True),
        metadata=METADATA, expected_source_bindings=SOURCES, maximum_quote_age_sec=5)
    args.update(overrides)
    return candidate_for_target(curve, publication, consumption, **args)


def quote(**overrides):
    answer = dict(instrument='EUR_USD', quote_id='quote_1009', bid='1.1000', ask='1.1002',
                  market_epoch=1009, available_epoch=1009.1, tradeable=True)
    answer.update(overrides)
    return answer


def test_remaining_move_rebases_price_only_and_preserves_original_probability_event():
    result = candidate()
    assert result['status'] == 'available'
    assert result['scope'] == 'current_research'
    assert result['side'] == 1
    assert result['reference_price'] == '1.1000'
    assert result['expected_terminal_price'] == '1.1003'
    assert result['expected_remaining_price_change'] == '0.0002'
    assert result['expected_remaining_move_pips'] == '2'
    assert result['remaining_sec'] == 25
    assert result['original_probability_up'] == '0.7'
    assert result['original_probability_event'] == dict(reference_price='1.1000', target_epoch=1035.)
    assert result['probability_is_remaining_move_probability'] is False
    assert result['available_epoch'] == 1009.1  # latest actual quote/curve receipt, not original model clock.


def test_price_overtaking_terminal_retains_negative_incumbent_continuation():
    result = candidate(quote=quote(bid='1.1004', ask='1.1006'))
    assert result['status'] == 'available'
    assert result['side'] == -1
    assert result['expected_remaining_move_pips'] == '-2'
    assert result['original_probability_up'] == '0.7'
    # A manager holding long must see this negative continuation instead of losing the candidate.


def test_legitimate_zero_is_distinct_from_missing_forecast():
    result = candidate(quote=quote(bid='1.1002', ask='1.1004'))
    assert result['status'] == 'available'
    assert result['side'] == 0
    assert result['expected_remaining_move_pips'] == '0'
    missing = candidate(target_epoch=1040)
    assert missing['status'] == 'unavailable'
    assert 'expected_remaining_move_pips' not in missing


def test_declared_instrument_pips_are_converted_from_original_model_units():
    result = candidate(metadata={**METADATA, 'pip_size': '0.00001'})
    assert result['pip_units_converted'] is True
    assert result['native_prediction_pip_size'] == '0.0001'
    assert result['pip_size'] == '0.00001'
    assert result['original_predicted_signed_pips'] == '3'
    assert result['expected_terminal_price'] == '1.1003'
    assert result['expected_remaining_move_pips'] == '20'


def test_original_quantiles_are_retained_without_conditional_claims():
    interval = candidate()['terminal_interval']
    assert interval == dict(expected_terminal_price_low='1.0998', expected_terminal_price_high='1.1007',
        remaining_price_change_low='-0.0003', remaining_price_change_high='0.0006',
        quantile_levels=['0.1', '0.9'], original_scope='original_unconditional_return', conditional_recalibration=False)
    assert candidate(target_epoch=1020)['terminal_interval'] is None


@pytest.mark.parametrize('overrides,reason', [
    ({'decision_epoch': 1005.9}, 'decision_precedes_curve_consumption'),
    ({'quote': quote(available_epoch=1010.1)}, 'decision_quote_not_yet_available'),
    ({'quote': quote(market_epoch=1009.2)}, 'decision_quote_not_yet_available'),
    ({'quote': quote(bid='1.1003')}, 'decision_quote_crossed'),
    ({'quote': quote(instrument='GBP_USD')}, 'decision_quote_instrument_mismatch'),
    ({'quote': quote(bid=1.1000)}, 'exact_quote_decimal_required'),
    ({'quote': quote(ask=True)}, 'exact_quote_decimal_required'),
    ({'quote': quote(bid='NaN')}, 'nonfinite_or_excessive_number'),
    ({'quote': quote(quote_id='contains private text')}, 'decision_quote_identity_invalid'),
    ({'maximum_quote_age_sec': 61}, 'invalid_quote_age_policy'),
    ({'maximum_quote_age_sec': True}, 'invalid_quote_age_policy'),
    ({'metadata': {**METADATA,'instrument':'GBP_USD'}}, 'instrument_metadata_mismatch'),
    ({'metadata': {**METADATA,'base_currency':'USD'}}, 'instrument_currency_metadata_mismatch'),
    ({'metadata': {**METADATA,'pip_size':'1'}}, 'instrument_pip_metadata_invalid'),
    ({'target_window_policy':'assume_exact'}, 'target_window_management_policy_invalid')])
def test_invalid_evidence_is_rejected(overrides, reason):
    with pytest.raises(CurveContractError, match=reason): candidate(**overrides)


@pytest.mark.parametrize('overrides,reason', [
    ({'decision_epoch': 1020,'target_epoch':1020}, 'native_target_elapsed_before_decision'),
    ({'decision_epoch': 1096}, 'curve_too_old_at_decision'),
    ({'target_epoch': 1036}, 'requested_native_target_unavailable'),
    ({'quote': None}, 'decision_quote_missing'),
    ({'quote': quote(tradeable=False)}, 'decision_quote_not_tradeable'),
    ({'quote': quote(tradeable=1)}, 'decision_quote_not_tradeable'),
    ({'maximum_quote_age_sec':0.5}, 'decision_quote_stale')])
def test_operational_missingness_is_explicit(overrides, reason):
    result = candidate(**overrides)
    assert result['status'] == 'unavailable'
    assert result['reason_code'] == reason
    assert 'side' not in result and 'expected_terminal_price' not in result
    assert result['can_place_orders'] is False


def test_uncertain_training_target_window_requires_explicit_management_approximation():
    policy = dict(kind='first_complete_bar_at_or_after_nominal', maximum_delay_sec=7)
    default = candidate(chain_overrides={'target_selection_policy': policy})
    assert default['reason_code'] == 'nonexact_model_target_requires_explicit_policy'
    opted_in = candidate(chain_overrides={'target_selection_policy':policy}, target_window_policy='nominal_management_boundary')
    assert opted_in['target_is_exact'] is False
    assert opted_in['original_target_epoch'] == 1035
    assert opted_in['target_price_window_end_epoch'] == 1042
    assert opted_in['target_selection_policy'] == policy
    assert opted_in['remaining_sec'] == 25  # never quietly extended by the target tolerance.


def test_quote_private_payload_is_not_exported_and_final_dto_is_hash_bound():
    result = candidate(quote=quote(raw_private_payload='do_not_export_this'))
    assert 'do_not_export_this' not in str(result)
    assert content_hash({k:v for k,v in result.items() if k != 'candidate_sha256'}) == result['candidate_sha256']


def test_ambient_precision_does_not_change_rebased_value_or_hash():
    ordinary = candidate(quote=quote(bid='1.10001234', ask='1.10021234'))
    with localcontext() as context:
        context.prec = 3
        other = candidate(quote=quote(bid='1.10001234', ask='1.10021234'))
    assert other == ordinary
    assert ordinary['expected_remaining_move_pips'] == '1.8766'
