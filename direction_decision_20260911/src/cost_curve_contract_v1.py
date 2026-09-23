"""Pure four-endpoint research economics for a retained signed-mean forecast.

No account, order, service, file, or network APIs. This does not refit/recalibrate
the forecast when a quote changes. It measures the remaining price difference
to the originally retained terminal expectation and preserves the original
target. Caller-supplied hashes/clocks identify inputs; they do not attest them.

Reference is the completed input-price epoch, not the start label of its bar.
Feature availability follows that reference and precedes forecast availability.
Exit cost heads are nonnegative expected one-way liquidation costs in bps of
the ORIGINAL reference midpoint. Current entry costs are ask-minus-provider-mid
for a long and provider-mid-minus-bid for a short, retaining asymmetric candle
midpoints. Future spread, slippage and financing are not observed here.

Lineage: oanda_curve_management_replay_v1.py remaining-terminal and bid/ask
economics. New contract: four new mean/exit-cost heads and fixed 1 bp research
margin. No import of old broker policy or execution path.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re

VERSION = 'direction_cost_curve_contract_v1_20260911'
HORIZONS_MINUTES = (5, 15, 30, 60)
MAX_CURVE_AGE_SEC = 60
MAX_QUOTE_AGE_SEC = 30
FIXED_MARGIN_BPS = Decimal('1')

_CURVE_FIELDS = {
    'pair', 'reference_epoch', 'reference_mid', 'available_epoch',
    'feature_available_epoch', 'trained_label_max_epoch',
    'calibration_label_max_epoch', 'model_sha256', 'feature_sha256', 'heads',
}
_HEAD_FIELDS = {
    'horizon_minutes', 'expected_return_bps', 'expected_long_exit_cost_bps',
    'expected_short_exit_cost_bps', 'probability_up',
}
_QUOTE_FIELDS = {'pair', 'bid', 'ask', 'mid', 'observed_epoch', 'price_epoch', 'source_sha256', 'price_kind'}
_PRICE_KINDS = {'synchronized_quote', 'retained_candle_close_proxy'}


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _shape(value, fields, reason):
    _require(type(value) is dict and set(value) == fields, reason)


def _number(value, name, *, positive=False, nonnegative=False):
    _require(type(value) in (str, int, float), f'{name}:finite_number_required')
    _require(not isinstance(value, str) or (0 < len(value) <= 128), f'{name}:number_text_length')
    try:
        result = Decimal(str(value))
        finite_float = math.isfinite(float(result))
    except (InvalidOperation, OverflowError, ValueError):
        raise ValueError(f'{name}:finite_number_required') from None
    _require(result.is_finite() and finite_float, f'{name}:finite_number_required')
    _require(not positive or result > 0, f'{name}:positive_required')
    _require(not nonnegative or result >= 0, f'{name}:nonnegative_required')
    return result


def _sha(value, name):
    _require(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None,
             f'{name}:sha256_required')
    return value


def _pair(value):
    _require(type(value) is str and re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', value) is not None
             and value[:3] != value[4:], 'pair:invalid')
    return value


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _seal(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _text(value):
    _require(value.is_finite() and math.isfinite(float(value)), 'computed_value:nonfinite')
    return format(value, 'f')


def assess_curve(curve, quote, *, decision_epoch, margin_bps=1):
    """Assess all four original endpoints; invalid/stale inputs raise ValueError.

    Each head returns ``candidate`` or ``wait`` and a selected side (+1/-1/0).
    Candidate means the retained mean less current entry and predicted exit
    cost exceeds the fixed 1 bp margin; it is never trade authorization. Both
    side economics remain available. No best-horizon or portfolio selection is
    performed. A candidate must retain the original mean's direction; passing
    the old expected terminal does not invent a new reversal forecast. Forecast
    probability is retained unchanged, not used as an EV multiplier and not
    rebased to a new quote/reference.

    Strict training < calibration < reference clocks are required. The public
    availability clocks are assertions supplied by the caller; this function
    neither inspects the original source observations nor certifies calibration.
    """
    _shape(curve, _CURVE_FIELDS, 'curve:exact_fields_required')
    _shape(quote, _QUOTE_FIELDS, 'quote:exact_fields_required')
    pair = _pair(curve['pair'])
    _require(quote['pair'] == pair, 'quote:pair_mismatch')
    _require(type(quote['price_kind']) is str and quote['price_kind'] in _PRICE_KINDS,
             'quote:explicit_price_kind_required')
    for key in ('model_sha256', 'feature_sha256'):
        _sha(curve[key], key)
    _sha(quote['source_sha256'], 'source_sha256')
    _require(type(curve['heads']) is list and len(curve['heads']) == 4, 'heads:four_native_heads_required')
    _require(_number(margin_bps, 'margin_bps') == FIXED_MARGIN_BPS, 'margin:fixed_one_bp_required')

    with localcontext() as context:
        context.prec = 50
        decision = _number(decision_epoch, 'decision_epoch', nonnegative=True)
        reference = _number(curve['reference_epoch'], 'reference_epoch', nonnegative=True)
        available = _number(curve['available_epoch'], 'available_epoch', nonnegative=True)
        feature_available = _number(curve['feature_available_epoch'], 'feature_available_epoch', nonnegative=True)
        trained = _number(curve['trained_label_max_epoch'], 'trained_label_max_epoch', nonnegative=True)
        calibrated = _number(curve['calibration_label_max_epoch'], 'calibration_label_max_epoch', nonnegative=True)
        _require(trained < calibrated < reference, 'curve:training_calibration_reference_order')
        _require(reference <= feature_available <= available <= decision, 'curve:availability_order')
        _require(decision - reference <= MAX_CURVE_AGE_SEC, 'curve:reference_stale')
        price_epoch = _number(quote['price_epoch'], 'price_epoch', nonnegative=True)
        observed = _number(quote['observed_epoch'], 'observed_epoch', nonnegative=True)
        _require(price_epoch <= observed <= decision, 'quote:availability_order')
        _require(decision - price_epoch <= MAX_QUOTE_AGE_SEC and decision - observed <= MAX_QUOTE_AGE_SEC,
                 'quote:stale')
        ref_mid = _number(curve['reference_mid'], 'reference_mid', positive=True)
        bid = _number(quote['bid'], 'bid', positive=True)
        ask = _number(quote['ask'], 'ask', positive=True)
        mid = _number(quote['mid'], 'mid', positive=True)
        _require(ask >= bid, 'quote:crossed')
        # OANDA's retained midpoint candle close need not be the arithmetic mean
        # of its independent bid and ask closes. Never overwrite that price.
        _require(bid <= mid <= ask, 'quote:mid_outside_bid_ask')
        long_entry_bps = (ask - mid) / mid * 10000
        short_entry_bps = (mid - bid) / mid * 10000
        heads = []
        seen = set()
        for head in curve['heads']:
            _shape(head, _HEAD_FIELDS, 'head:exact_fields_required')
            horizon = head['horizon_minutes']
            _require(type(horizon) is int and horizon in HORIZONS_MINUTES and horizon not in seen,
                     'head:duplicate_or_unsupported_horizon')
            seen.add(horizon)
            expected_return = _number(head['expected_return_bps'], 'expected_return_bps')
            probability = _number(head['probability_up'], 'probability_up')
            _require(0 <= probability <= 1, 'probability_up:out_of_bounds')
            long_exit = _number(head['expected_long_exit_cost_bps'], 'expected_long_exit_cost_bps', nonnegative=True)
            short_exit = _number(head['expected_short_exit_cost_bps'], 'expected_short_exit_cost_bps', nonnegative=True)
            target = reference + horizon * 60
            terminal = ref_mid * (1 + expected_return / 10000)
            _require(terminal > 0, 'head:nonpositive_expected_terminal')
            retained_change = terminal - mid
            long_gross = retained_change / mid * 10000
            short_gross = -long_gross
            long_exit_current_bps = long_exit * ref_mid / mid
            short_exit_current_bps = short_exit * ref_mid / mid
            long_net = long_gross - long_entry_bps - long_exit_current_bps
            short_net = short_gross - short_entry_bps - short_exit_current_bps
            side = 1 if long_net > short_net else -1 if short_net > long_net else 0
            original_side = 1 if expected_return > 0 else -1 if expected_return < 0 else 0
            best_net = max(long_net, short_net)
            candidate = decision < target and side != 0 and side == original_side and best_net > FIXED_MARGIN_BPS
            reason = ('retained_mean_clears_fixed_cost_margin' if candidate else
                      'original_target_expired' if decision >= target else
                      'original_mean_neutral' if original_side == 0 else
                      'remaining_edge_opposes_original_mean' if side != 0 and side != original_side else
                      'edge_not_above_fixed_margin')
            heads.append(dict(
                horizon_minutes=horizon, original_target_epoch=_text(target),
                remaining_sec=_text(target - decision), status='candidate' if candidate else 'wait',
                side=side if candidate else 0, reason=reason,
                original_mean_side=original_side, economically_preferred_side=side,
                original_expected_return_bps=_text(expected_return),
                original_probability_up=_text(probability), expected_terminal_price=_text(terminal),
                remaining_price_difference_to_retained_expectation=_text(retained_change),
                long_gross_bps=_text(long_gross), short_gross_bps=_text(short_gross),
                current_long_entry_cost_bps=_text(long_entry_bps),
                current_short_entry_cost_bps=_text(short_entry_bps),
                expected_long_exit_cost_bps_original_reference=_text(long_exit),
                expected_short_exit_cost_bps_original_reference=_text(short_exit),
                expected_long_exit_cost_bps_current_mid=_text(long_exit_current_bps),
                expected_short_exit_cost_bps_current_mid=_text(short_exit_current_bps),
                long_net_bps=_text(long_net), short_net_bps=_text(short_net),
                selected_net_bps=_text(best_net) if candidate else None,
                selected_net_over_margin_bps=_text(best_net - FIXED_MARGIN_BPS) if candidate else None,
            ))
        _require(seen == set(HORIZONS_MINUTES), 'heads:four_native_heads_required')
        heads.sort(key=lambda row: row['horizon_minutes'])
        result = dict(
            schema_version=1, version=VERSION, pair=pair,
            research_only=True, can_place_orders=False, can_promote=False,
            execution_eligible=False, source_provenance_attested=False,
            future_exit_cost_is_prediction=True,
            expectation_scope='retained_original_reference_mean_not_recomputed_conditional_forecast',
            probability_scope='retained_original_reference_probability_not_rebased',
            price_kind=quote['price_kind'],
            cost_scope=('supplied_synchronized_bid_ask_entry_plus_predicted_exit_cost' if quote['price_kind'] == 'synchronized_quote'
                        else 'hypothetical_retained_bid_ask_candle_close_entry_plus_predicted_exit_cost_no_fill_proof'),
            additional_slippage_or_financing_modeled=False,
            decision_epoch=_text(decision), reference_epoch=_text(reference),
            curve_available_epoch=_text(available), feature_available_epoch=_text(feature_available),
            quote_price_epoch=_text(price_epoch), quote_observed_epoch=_text(observed),
            reference_mid=_text(ref_mid), current_mid=_text(mid), margin_bps=_text(FIXED_MARGIN_BPS),
            model_sha256=curve['model_sha256'], feature_sha256=curve['feature_sha256'],
            quote_source_sha256=quote['source_sha256'],
            input_curve_sha256=_seal(curve), input_quote_sha256=_seal(quote),
            candidate_heads=sum(row['status'] == 'candidate' for row in heads), heads=heads,
        )
    result['assessment_sha256'] = _seal(result)
    return result


def validate_assessment(assessment, curve, quote, *, decision_epoch, margin_bps=1):
    """Recompute from the supplied originals; refuse edited assessment fields.

    This checks consistency against the caller's original arguments. It cannot
    authenticate a different but self-consistent source/model/forecast payload.
    """
    _require(type(assessment) is dict, 'assessment:dict_required')
    expected = assess_curve(curve, quote, decision_epoch=decision_epoch, margin_bps=margin_bps)
    _require(assessment == expected, 'assessment:does_not_match_original_inputs')
    return expected
