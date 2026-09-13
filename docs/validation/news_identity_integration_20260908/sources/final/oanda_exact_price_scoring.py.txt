"""Versioned, pure price scoring; deliberately not wired into any live worker.

Labels and monetary price differences use exact Decimal arithmetic on the
supplied prices. Strings/Decimal preserve supplied decimal precision; finite
floats are interpreted through str(float), which cannot recover lost vendor
precision. No tick tolerance or epsilon turns a real small move into a flat.

Ratios and derived metrics use 80 significant decimal digits, ROUND_HALF_EVEN,
independent of the caller's Decimal context. Return values remain Decimal;
to_jsonable converts them to strings for explicit, lossless JSON transport.
This module assesses prices only, not clocks, eligibility or execution proof.
"""
from collections.abc import Mapping
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
import math
import re

SCORING_VERSION = 'exact_decimal_bidask_scoring_v1_20260906'
METRIC_PRECISION = 80
_NUMBER = re.compile(r'^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$')
_ZERO = Decimal(0)
_ONE = Decimal(1)
_TWO = Decimal(2)
_BPS = Decimal(10000)


def _decimal(value, name):
    if type(value) not in (str, int, float, Decimal):
        raise ValueError(f'{name}: expected decimal, string, integer or finite float')
    if type(value) is float and not math.isfinite(value):
        raise ValueError(f'{name}: must be finite')
    text = str(value)
    if len(text) > 1024 or not _NUMBER.fullmatch(text):
        raise ValueError(f'{name}: invalid decimal representation')
    number = Decimal(text)
    # Bound work and ensure the exact-price context below cannot round a valid
    # additive price calculation (maximum accepted decimal span < 1,300 digits).
    if not number.is_finite() or len(number.as_tuple().digits) > 256 or abs(number.as_tuple().exponent) > 512:
        raise ValueError(f'{name}: outside supported finite decimal range')
    return number


def _quote(value, name):
    if not isinstance(value, Mapping) or 'bid' not in value or 'ask' not in value:
        raise ValueError(f'{name}: bid and ask required')
    bid = _decimal(value['bid'], f'{name}.bid')
    ask = _decimal(value['ask'], f'{name}.ask')
    if bid <= 0 or ask < bid:
        raise ValueError(f'{name}: require 0 < bid <= ask')
    return bid, ask


def decimal_value(value, name='value'):
    """Validate a bounded decimal input without float coercion."""
    return _decimal(value, name)


def quote_midpoint(quote):
    """Validate bid/ask and return their exact midpoint."""
    bid, ask = _quote(quote, 'quote')
    with localcontext(Context(prec=4096, rounding=ROUND_HALF_EVEN)):
        return (bid + ask) / _TWO


def _cost_key(value):
    if value == 0:
        return '0'
    text = format(value, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def score_prediction(reference_quote, target_quote, *, direction, probability_up,
                     entry_quote=None, expected_signed_pips=None, pip_size=None,
                     predicted_return_bps=None, extra_cost_stress_bps=()):
    """Score a forecast direction against reference→target and entry→target.

    Each quote is a mapping containing bid/ask. direction is an explicit integer
    -1 (sell), 0 (abstain), or 1 (buy); it is never silently replaced by the
    probability's direction. entry_quote defaults to reference_quote.

    actual_return_bps and prediction errors use the reference midpoint. net_bps
    uses the executable entry midpoint and buy entry-ask/exit-bid or sell
    entry-bid/exit-ask. Flat outcomes are direction misses. The binary Brier
    label is explicitly up-vs-not-up, so flat has label 0. Abstention has zero
    gross/net/stress exposure, while its probability still receives a Brier
    score. Optional pip_size is explicit; no instrument naming heuristic is
    used. It is required when expected_signed_pips is provided.
    Alternatively predicted_return_bps may supply the signed magnitude directly;
    providing both magnitude forms is invalid.

    Core price differences are exact. Ratios are rounded only to the declared
    METRIC_PRECISION; none of those rounded values determines a class or win.
    Invalid inputs raise ValueError; inputs are never mutated.
    """
    if type(direction) is not int or direction not in (-1, 0, 1):
        raise ValueError('direction: expected integer -1, 0 or 1')
    probability = _decimal(probability_up, 'probability_up')
    if not 0 <= probability <= 1:
        raise ValueError('probability_up: require 0 <= probability <= 1')
    reference_bid, reference_ask = _quote(reference_quote, 'reference_quote')
    target_bid, target_ask = _quote(target_quote, 'target_quote')
    entry_bid, entry_ask = _quote(reference_quote if entry_quote is None else entry_quote, 'entry_quote')
    pip = None if pip_size is None else _decimal(pip_size, 'pip_size')
    if pip is not None and pip <= 0:
        raise ValueError('pip_size: must be positive')
    expected = None if expected_signed_pips is None else _decimal(expected_signed_pips, 'expected_signed_pips')
    if expected is not None and pip is None:
        raise ValueError('pip_size: required with expected_signed_pips')
    supplied_bps = None if predicted_return_bps is None else _decimal(predicted_return_bps, 'predicted_return_bps')
    if expected is not None and supplied_bps is not None:
        raise ValueError('supply only one prediction magnitude form')
    if not isinstance(extra_cost_stress_bps, (list, tuple)) or len(extra_cost_stress_bps) > 32:
        raise ValueError('extra_cost_stress_bps: expected at most 32 list/tuple values')
    costs = [_decimal(value, 'extra_cost_stress_bps') for value in extra_cost_stress_bps]
    if any(value < 0 for value in costs) or len(set(costs)) != len(costs):
        raise ValueError('extra_cost_stress_bps: require unique nonnegative costs')

    with localcontext(Context(prec=4096, rounding=ROUND_HALF_EVEN)):
        reference_mid = (reference_bid + reference_ask) / _TWO
        target_mid = (target_bid + target_ask) / _TWO
        entry_mid = (entry_bid + entry_ask) / _TWO
        move = target_mid - reference_mid
        gross = direction * move
        net = (target_bid - entry_ask if direction > 0 else
               entry_bid - target_ask if direction < 0 else _ZERO)
        entry_spread = entry_ask - entry_bid
        target_spread = target_ask - target_bid
        spread_drag = (entry_spread + target_spread) / _TWO if direction else _ZERO
        expected_move = None if expected is None else expected * pip
        # Compute this numerator exactly before the metric context can round.
        error_move = None if expected_move is None else expected_move - move
        direct_error_numerator = None if supplied_bps is None else supplied_bps * reference_mid - _BPS * move

    up_label = int(move > 0)
    probability_direction = 1 if probability > Decimal('.5') else -1 if probability < Decimal('.5') else 0
    with localcontext(Context(prec=METRIC_PRECISION, rounding=ROUND_HALF_EVEN)):
        actual_bps = _BPS * move / reference_mid
        net_bps = _BPS * net / entry_mid
        predicted_bps = None if expected_move is None else _BPS * expected_move / reference_mid
        error_bps = None if error_move is None else _BPS * error_move / reference_mid
        if supplied_bps is not None:
            predicted_bps = supplied_bps
            error_bps = direct_error_numerator / reference_mid
        result = {
            'scoring_version': SCORING_VERSION,
            'metric_precision_digits': METRIC_PRECISION,
            'reference_mid': reference_mid, 'entry_mid': entry_mid, 'target_mid': target_mid,
            'actual_midpoint_move': move, 'actual_return_bps': actual_bps,
            'outcome_class': 'up' if move > 0 else 'down' if move < 0 else 'flat',
            'up_label': up_label, 'direction': direction,
            'probability_up': probability, 'probability_direction': probability_direction,
            'emitted_side_differs_from_probability_direction': direction != probability_direction,
            'direction_correct': bool(direction and gross > 0),
            'gross_signed_reference_move': gross,
            'gross_signed_reference_bps': _BPS * gross / reference_mid,
            'net_price_move': net, 'net_bps': net_bps,
            'positive_after_spread': bool(direction and net > 0),
            'entry_spread_price': entry_spread, 'target_spread_price': target_spread,
            'roundtrip_spread_price': spread_drag,
            'pip_size': pip, 'expected_signed_pips': expected,
            'actual_signed_pips': None if pip is None else move / pip,
            'gross_directional_pips': None if pip is None else gross / pip,
            'net_pips': None if pip is None else net / pip,
            'predicted_return_bps': predicted_bps,
            'absolute_error_bps': None if error_bps is None else abs(error_bps),
            'squared_error_bps': None if error_bps is None else error_bps * error_bps,
            'brier_up_vs_not_up': (probability - up_label) ** 2,
            'stress_net_bps': {_cost_key(cost): net_bps - (cost if direction else _ZERO) for cost in costs},
        }
    return result


def to_jsonable(value):
    """Recursively preserve Decimal values as strings, without float coercion."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError('cannot serialize nonfinite Decimal')
        return str(value)
    if value is None or type(value) in (str, int, bool):
        return value
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError('JSON mapping keys must be strings')
        return {key: to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [to_jsonable(item) for item in value]
    raise ValueError('unsupported JSON value; monetary/metric values must remain Decimal')
