"""Pure native-curve to research-management adapter; no interpolation or broker I/O.

The expected terminal price remains anchored to the original model input. Only
the signed distance from the decision midpoint is rebased. Original probabilities
and intervals do not become calibrated conditional remaining-return forecasts.
"""
from __future__ import annotations

from decimal import Context, localcontext
import math
import re

from oanda_forecast_curve_contract_v1 import (
    AUTHORITY, CurveContractError, content_hash, decimal_number, decimal_text,
    epoch, validate_consumption,
)

SCHEMA = 'native_curve_management_candidate_v1_20260909'
REFUSAL_SCHEMA = 'native_curve_management_refusal_v1_20260909'


def _sealed(body, key):
    return {**body, key: content_hash(body)}


def _refusal(reason, *, curve, decision, target):
    return _sealed({'schema_version': REFUSAL_SCHEMA, 'status': 'unavailable',
        'reason_code': reason, 'instrument': curve['prepared_curve']['instrument'],
        'curve_id': curve['curve_id'], 'curve_sha256': curve['curve_sha256'],
        'decision_epoch': decision, 'original_target_epoch': target,
        **AUTHORITY}, 'refusal_sha256')


def candidate_for_target(curve, publication, consumption, *, decision_epoch,
                         target_epoch, quote, metadata, expected_source_bindings,
                         maximum_quote_age_sec, target_window_policy='exact_only'):
    """Return one exact-target candidate or an explicit operational refusal.

    Malformed/unbound evidence raises CurveContractError. Missing, expired or
    operationally unavailable inputs return an inert refusal without a made-up
    numerical forecast. The caller must supply an actual independently observed
    publication/consumption chain; this pure function does not authenticate I/O.
    """
    validate_consumption(curve, publication, consumption,
                         expected_source_bindings=expected_source_bindings)
    decision, target = epoch(decision_epoch), epoch(target_epoch)
    prepared = curve['prepared_curve']
    instrument = prepared['instrument']
    refuse = lambda reason: _refusal(reason, curve=curve, decision=decision, target=target)
    if decision < consumption['available_epoch']:
        raise CurveContractError('decision_precedes_curve_consumption')
    if decision-curve['issued_epoch'] > prepared['policy']['maximum_decision_age_sec']:
        return refuse('curve_too_old_at_decision')
    nodes = [node for node in prepared['nodes'] if node['original_target_epoch'] == target]
    if not nodes:
        return refuse('requested_native_target_unavailable')
    node = nodes[0]
    if node['status'] != 'forecast':
        return refuse(node['reason_code'])
    admission = next(row for row in curve['node_admission'] if row['node_id'] == node['node_id'])
    if admission['status'] != 'admitted':
        return refuse(admission['reason_code'])
    if target-decision <= prepared['policy']['minimum_remaining_sec']:
        return refuse('native_target_elapsed_before_decision')
    if target_window_policy not in ('exact_only', 'nominal_management_boundary'):
        raise CurveContractError('target_window_management_policy_invalid')
    target_policy = prepared['target_selection_policy']
    if target_policy['maximum_delay_sec'] > 0 and target_window_policy == 'exact_only':
        return refuse('nonexact_model_target_requires_explicit_policy')
    if (type(maximum_quote_age_sec) not in (int, float)
            or not math.isfinite(maximum_quote_age_sec) or not 0 <= maximum_quote_age_sec <= 60):
        raise CurveContractError('invalid_quote_age_policy')
    if not isinstance(metadata, dict) or metadata.get('instrument') != instrument:
        raise CurveContractError('instrument_metadata_mismatch')
    base, counter = metadata.get('base_currency'), metadata.get('quote_currency')
    if base != instrument[:3] or counter != instrument[4:]:
        raise CurveContractError('instrument_currency_metadata_mismatch')
    pip = decimal_number(metadata.get('pip_size'), positive=True)
    if pip >= 1:
        raise CurveContractError('instrument_pip_metadata_invalid')
    if quote is None:
        return refuse('decision_quote_missing')
    if not isinstance(quote, dict) or quote.get('instrument') != instrument:
        raise CurveContractError('decision_quote_instrument_mismatch')
    quote_id = quote.get('quote_id')
    if not isinstance(quote_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:/+\-]{1,240}', quote_id):
        raise CurveContractError('decision_quote_identity_invalid')
    market, available = epoch(quote.get('market_epoch')), epoch(quote.get('available_epoch'))
    if not market <= available <= decision:
        raise CurveContractError('decision_quote_not_yet_available')
    if type(quote.get('bid')) not in (str, int) or type(quote.get('ask')) not in (str, int):
        raise CurveContractError('exact_quote_decimal_required')
    bid, ask = decimal_number(quote.get('bid'), positive=True), decimal_number(quote.get('ask'), positive=True)
    if bid > ask:
        raise CurveContractError('decision_quote_crossed')
    if quote.get('tradeable') is not True:
        return refuse('decision_quote_not_tradeable')
    if decision-market > maximum_quote_age_sec:
        return refuse('decision_quote_stale')
    # All arithmetic uses its own precision, independent of another module's context.
    with localcontext(Context(prec=192)):
        midpoint = (bid+ask)/2
        terminal = decimal_number(node['expected_terminal_price'], positive=True)
        change = terminal-midpoint
        remaining_pips = change/pip
        native_pip = decimal_number(prepared['pip_size'], positive=True)
        bounds = None
        if node['interval'] is not None:
            interval = node['interval']
            reference = decimal_number(prepared['reference_price'], positive=True)
            low = reference+decimal_number(interval['low_pips'])*native_pip
            high = reference+decimal_number(interval['high_pips'])*native_pip
            if low <= 0:
                raise CurveContractError('interval_terminal_price_nonpositive')
            bounds = dict(expected_terminal_price_low=decimal_text(low),
                expected_terminal_price_high=decimal_text(high),
                remaining_price_change_low=decimal_text(low-midpoint),
                remaining_price_change_high=decimal_text(high-midpoint),
                quantile_levels=list(interval['levels']), original_scope=interval['scope'],
                conditional_recalibration=False)
    # Select evidence fields explicitly: no arbitrary quote metadata or raw payload.
    quote_evidence = dict(instrument=instrument, quote_id=quote_id, bid=decimal_text(bid),
        ask=decimal_text(ask), midpoint=decimal_text(midpoint), market_epoch=market,
        available_epoch=available, tradeable=True)
    body = dict(schema_version=SCHEMA, status='available', scope=prepared['scope'], instrument=instrument,
        side=1 if change > 0 else -1 if change < 0 else 0,
        curve_id=curve['curve_id'], curve_sha256=curve['curve_sha256'],
        node_id=node['node_id'], node_sha256=node['node_sha256'], model_id=node['model_id'],
        model_sha256=prepared['model_sha256'], forecast_cohort=prepared['forecast_cohort'],
        input_context=prepared['input_context'],
        reference_epoch=prepared['reference_epoch'], reference_label_epoch=prepared['reference_label_epoch'],
        reference_price=prepared['reference_price'], reference_price_kind=prepared['reference_price_kind'],
        pip_size=decimal_text(pip), native_prediction_pip_size=prepared['pip_size'],
        pip_units_converted=pip != native_pip, original_predicted_signed_pips=node['predicted_signed_pips'],
        issued_epoch=curve['issued_epoch'], available_epoch=max(consumption['available_epoch'], available),
        decision_epoch=decision, original_target_epoch=target,
        target_label_epoch=node['target_label_epoch'], horizon_sec=node['horizon_sec'],
        target_selection_policy=dict(target_policy), target_window_policy=target_window_policy,
        target_is_exact=target_policy['maximum_delay_sec'] == 0,
        target_price_window_end_epoch=node['target_price_window_end_epoch'],
        remaining_sec=target-decision, expected_terminal_price=node['expected_terminal_price'],
        expected_remaining_price_change=decimal_text(change),
        expected_remaining_move_pips=decimal_text(remaining_pips),
        source_bindings=dict(prepared['source_bindings']),
        computation_sha256=prepared['computation_sha256'],
        input_capture_sha256=prepared['input_capture_sha256'],
        publication_sha256=publication['publication_sha256'],
        consumption_sha256=consumption['consumption_sha256'],
        decision_quote=quote_evidence, decision_quote_sha256=content_hash(quote_evidence),
        metadata=dict(instrument=instrument, base_currency=base, quote_currency=counter, pip_size=decimal_text(pip)),
        original_probability_up=node['probability_up'],
        original_probability_scope=node['probability_scope'],
        original_probability_event=dict(reference_price=prepared['reference_price'], target_epoch=target),
        probability_is_remaining_move_probability=False,
        original_residual_std_pips=node['residual_std_pips'],
        original_uncertainty_scope=node['uncertainty_scope'], terminal_interval=bounds,
        **AUTHORITY)
    return _sealed(body, 'candidate_sha256')
