"""Pure research-only semantic direction admission for native curve candidates.

An old terminal crossing is arithmetic, not newly issued countertrend evidence.
This module adds no age, cost, confidence, sizing or trading policy. It does not
certify newly updated inputs, authenticate I/O, or modify the frozen adapter.
Entry/rotation admission and incumbent continuation deliberately remain separate.
"""
from copy import deepcopy
from decimal import Context, localcontext
import re

from oanda_curve_management_adapter_v1 import candidate_for_target
from oanda_forecast_curve_contract_v1 import (
    AUTHORITY, CurveContractError, content_hash, decimal_number, decimal_text, epoch,
)

SCHEMA = 'curve_original_direction_admission_v1_20260909'
POLICY = {
    'kind': 'original_direction_consistency_necessary_condition',
    'original_direction': 'sign_of_issued_node_prediction_from_original_reference',
    'remaining_direction': 'sign_of_original_terminal_minus_current_midpoint',
    'neutral_original': 'no_new_directional_entry_evidence',
    'opposite_remaining_direction': 'no_new_countertrend_entry_evidence',
    'incumbent_continuation': 'preserve_existing_side_value_independently_of_entry_admission',
    'fresh_forecast_update': 'not_certified_by_this_semantic_gate',
    'additional_age_cost_confidence_or_sizing_thresholds': False,
}
INCUMBENT_KEYS = {'instrument', 'side', 'original_target_epoch', 'observed_epoch', 'state_sha256'}


def _need(condition, reason):
    if not condition:
        raise CurveContractError(reason)


def _sign(value):
    return 1 if value > 0 else -1 if value < 0 else 0


def _seal(body):
    return {**body, 'admission_sha256': content_hash(body)}


def _incumbent(value, decision):
    if value is None:
        return None
    _need(type(value) is dict and set(value) == INCUMBENT_KEYS, 'incumbent_descriptor_shape')
    _need(type(value['side']) is int and value['side'] in (-1, 1), 'incumbent_side_invalid')
    instrument = value['instrument']
    _need(isinstance(instrument, str) and re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', instrument) is not None
          and instrument[:3] != instrument[4:], 'incumbent_instrument_invalid')
    observed, target = epoch(value['observed_epoch']), epoch(value['original_target_epoch'])
    _need(observed <= decision, 'incumbent_not_observed_at_decision')
    pin = value['state_sha256']
    _need(isinstance(pin, str) and len(pin) == 64 and all(c in '0123456789abcdef' for c in pin),
          'incumbent_state_hash_invalid')
    return {**deepcopy(value), 'observed_epoch': observed, 'original_target_epoch': target}


def admit_candidate_for_target(curve, publication, consumption, *, decision_epoch,
                               target_epoch, quote, metadata, expected_source_bindings,
                               maximum_quote_age_sec, target_window_policy='exact_only',
                               incumbent=None):
    """Classify one verified native node without changing its numerical candidate.

    `incumbent` is an optional caller-observed state descriptor with exact keys
    instrument, side, original_target_epoch, observed_epoch and state_sha256.
    Its identity/clock are checked here; authenticating the state read remains
    the caller's job. This is not a position-state constructor or trade selector.

    Malformed evidence raises. Operational unavailability remains unavailable.
    A semantic entry refusal never erases an available continuation estimate.
    The output is a new schema and must not be passed to the frozen v1 selector.
    """
    decision, target = epoch(decision_epoch), epoch(target_epoch)
    held = _incumbent(incumbent, decision)
    candidate = candidate_for_target(curve, publication, consumption,
        decision_epoch=decision, target_epoch=target, quote=quote, metadata=metadata,
        expected_source_bindings=expected_source_bindings,
        maximum_quote_age_sec=maximum_quote_age_sec, target_window_policy=target_window_policy)
    body = dict(schema_version=SCHEMA, decision_epoch=decision, original_target_epoch=target,
                instrument=curve['prepared_curve']['instrument'], policy=deepcopy(POLICY),
                input_candidate_sha256=candidate.get('candidate_sha256', candidate.get('refusal_sha256')),
                original_curve_sha256=curve['curve_sha256'], original_publication_sha256=publication['publication_sha256'],
                original_consumption_sha256=consumption['consumption_sha256'],
                expected_source_bindings=deepcopy(expected_source_bindings), incumbent=held,
                scope='necessary_direction_semantics_only_not_complete_trade_eligibility',
                receipt_scope='validated_forecast_receipts_and_caller_attested_state_not_independent_IO_authentication',
                derived_clock_scope='decision_context_only_not_an_actual_computation_or_publication_receipt',
                fresh_input_update_verified=False, forecast_update_status='not_assessed_by_direction_gate',
                risk_distribution_attachment=None, **AUTHORITY)
    if candidate['status'] != 'available':
        body.update(status='unavailable', reason_code=candidate['reason_code'], original_forecast_side=None,
                    rebased_remaining_side=None, original_prediction=None, candidate=deepcopy(candidate),
                    new_entry=dict(semantic_admitted=False, reason_code='candidate_unavailable', candidate=None),
                    opposite_side_rotation=dict(semantic_admitted=False, reason_code='candidate_unavailable', candidate=None),
                    continuation=dict(status='unavailable', reason_code=candidate['reason_code'], candidate=None,
                                      incumbent_signed_remaining_move_pips=None))
        return _seal(body)

    with localcontext(Context(prec=192)):
        original_pips = decimal_number(candidate['original_predicted_signed_pips'])
        reference = decimal_number(candidate['reference_price'], positive=True)
        terminal = decimal_number(candidate['expected_terminal_price'], positive=True)
        original_side = _sign(original_pips)
        _need(original_side == _sign(terminal - reference), 'original_direction_price_basis_mismatch')
        remaining = decimal_number(candidate['expected_remaining_move_pips'])
        remaining_side = _sign(remaining)
        _need(remaining_side == candidate['side'], 'remaining_direction_mismatch')
        admitted = original_side != 0 and remaining_side == original_side
        entry_reason = ('original_and_remaining_direction_consistent' if admitted else
                        'neutral_original_forecast_is_not_new_directional_evidence' if original_side == 0 else
                        'zero_remaining_move' if remaining_side == 0 else
                        'countertrend_from_original_terminal_crossing')
        continuation = dict(status='available_unassigned_estimate', reason_code=None,
                            candidate=deepcopy(candidate), incumbent_signed_remaining_move_pips=None,
                            valuation_scope='fixed_terminal_arithmetic_not_updated_conditional_forecast')
        rotation_reason = 'no_incumbent'
        rotation = False
        if held is not None:
            if held['instrument'] != candidate['instrument']:
                continuation.update(status='incomparable', reason_code='incumbent_instrument_mismatch', candidate=None)
                rotation_reason = 'incumbent_instrument_mismatch'
            elif held['original_target_epoch'] != target:
                continuation.update(status='incomparable', reason_code='incumbent_native_target_mismatch', candidate=None)
                rotation_reason = 'incumbent_native_target_mismatch'
            else:
                continuation.update(status='available_for_incumbent',
                    incumbent_signed_remaining_move_pips=decimal_text(held['side'] * remaining))
                rotation = admitted and remaining_side != held['side']
                rotation_reason = (entry_reason if not admitted else
                                   'original_direction_consistent_opposite_side' if rotation else
                                   'same_as_incumbent_side_not_rotation')
        body.update(status='classified', reason_code=entry_reason, original_forecast_side=original_side,
                    rebased_remaining_side=remaining_side,
                    original_prediction=dict(predicted_signed_pips=candidate['original_predicted_signed_pips'],
                        native_pip_size=candidate['native_prediction_pip_size'], reference_price=candidate['reference_price'],
                        reference_epoch=candidate['reference_epoch'], issued_epoch=candidate['issued_epoch'],
                        expected_terminal_price=candidate['expected_terminal_price'],
                        original_probability_up=candidate['original_probability_up'],
                        original_probability_scope=candidate['original_probability_scope'],
                        probability_is_remaining_move_probability=False),
                    candidate=deepcopy(candidate),
                    new_entry=dict(semantic_admitted=admitted, reason_code=entry_reason,
                                   candidate=deepcopy(candidate) if admitted else None),
                    opposite_side_rotation=dict(semantic_admitted=rotation, reason_code=rotation_reason,
                                                candidate=deepcopy(candidate) if rotation else None),
                    continuation=continuation)
    return _seal(body)


def validate_admission(admission, curve, publication, consumption, **arguments):
    """Rebuild against the complete original evidence; a recomputed hash is insufficient."""
    expected = admit_candidate_for_target(curve, publication, consumption, **arguments)
    _need(type(admission) is dict and admission == expected, 'direction_admission_semantic_mismatch')
    return deepcopy(expected)
