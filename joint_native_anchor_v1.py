"""Immutable native-close values; no input authentication, scoring or order I/O.

Callers bind supplied values to their verified source capture/model result.
Binary64 endpoint representation does not recover decimal broker prices.
"""
from dataclasses import dataclass
import hashlib
import json
import math
import re

SCHEMA = 'joint_native_close_h1_anchor_v1_20260913'
REMAINING_SCHEMA = 'joint_static_native_endpoint_remaining_view_v1_20260913'
NUMERIC_SOURCE_SHA256 = 'eb153acb966dc04ad950a0bbfcc730e78a9a0da1d8a24e91d641f473f5cfbc23'
ENDPOINT_RECIPE = 'binary64_product_expected_pips_times_pip_then_add_original_mid_no_fma_v1'
PROBABILITY_EVENT = 'native_target_close_mid_strictly_greater_than_native_origin_close_mid_ties_not_up'
PROBABILITY_SCOPE = 'original_uncalibrated_shrunk_model_estimate_not_verified_accuracy'
MAX_EPOCH = 32503680000
_TOKEN = object()


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('ascii')


def real(value, name, *, positive=False):
    need(type(value) is float and math.isfinite(value), name + '_finite_original_binary64_required')
    need(not positive or value > 0, name + '_positive_required')
    return value


def clock(value, name):
    need(type(value) in (int, float) and 0 < value < MAX_EPOCH, name + '_bounded_positive_clock_required')
    need(math.isfinite(value), name + '_finite_clock_required')
    return value


def direction(value):
    return 1 if value > 0 else -1 if value < 0 else 0


@dataclass(frozen=True, slots=True, init=False)
class NativeAnchor:
    """Immutable descriptive value, never a source receipt or runtime permission."""
    _raw: bytes

    def __init__(self, token=None, raw=None):
        need(token is _TOKEN and type(raw) is bytes, 'native_anchor_factory_required')
        object.__setattr__(self, '_raw', raw)

    @property
    def sha256(self):
        return hashlib.sha256(self._raw).hexdigest()

    def as_dict(self):
        return json.loads(self._raw)


def make_native_anchor(*, instrument, pip_size, origin_bar_start_epoch, origin_mid,
                       expected_signed_pips, probability_up, source_anchor_complete,
                       source_observed_epoch, feature_decision_epoch,
                       computation_started_epoch, computation_completed_epoch, issued_epoch,
                       emitted_side=None):
    """Represent the unchanged model target using already verified caller values.

    The completion Boolean/order are checked, not independently authenticated.
    Source/capture hashes, model ownership, 900s age and 120s build limits remain
    the caller's ledger admission duties. This contract cannot authorize issue.
    """
    need(type(instrument) is str and re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', instrument)
         and instrument[:3] != instrument[4:], 'distinct_currency_pair_required')
    pip = real(pip_size, 'pip_size', positive=True)
    need(pip in (.0001, .001, .01), 'original_supported_pip_required')
    start = origin_bar_start_epoch
    need(type(start) is int and 0 < start < MAX_EPOCH - 3660 and start % 60 == 0,
         'exact_bounded_minute_start_required')
    need(source_anchor_complete is True, 'completed_source_anchor_required')
    mid = real(origin_mid, 'origin_mid', positive=True)
    delta = real(expected_signed_pips, 'expected_signed_pips')
    probability = real(probability_up, 'probability_up')
    need(.25 <= probability <= .75, 'original_shrunk_probability_domain')
    source = clock(source_observed_epoch, 'source_observed')
    decision = clock(feature_decision_epoch, 'feature_decision')
    began = clock(computation_started_epoch, 'computation_started')
    completed = clock(computation_completed_epoch, 'computation_completed')
    issued = clock(issued_epoch, 'issued')
    close = start + 60; target = start + 3660
    need(close <= source <= decision <= began <= completed < issued < target,
         'native_completed_anchor_issue_availability_order')
    side = direction(delta)
    if emitted_side is not None:
        need(type(emitted_side) is int and emitted_side in (-1, 0, 1) and emitted_side == side,
             'original_expected_delta_side_mismatch')
    # Two distinct binary64 operations. No current quote enters this recipe.
    move = delta * pip
    need(math.isfinite(move), 'native_expected_move_overflow')
    endpoint = mid + move
    need(math.isfinite(endpoint) and endpoint > 0, 'native_expected_endpoint_invalid')
    body = {'schema_version': SCHEMA, 'instrument': instrument,
        'numeric_source_sha256_declared_not_authenticated': NUMERIC_SOURCE_SHA256,
        'origin_bar_start_epoch': start, 'origin_close_epoch': close,
        'target_bar_start_epoch': start + 3600, 'target_close_epoch': target,
        'native_horizon_sec': 3600, 'source_anchor_complete': True,
        'source_observed_epoch': source, 'feature_decision_epoch': decision,
        'computation_started_epoch': began, 'computation_completed_epoch': completed,
        'issued_epoch': issued, 'origin_mid_hex': mid.hex(), 'pip_size_hex': pip.hex(),
        'expected_signed_pips_hex': delta.hex(), 'expected_move_hex': move.hex(),
        'expected_endpoint_mid_hex': endpoint.hex(), 'endpoint_arithmetic_recipe': ENDPOINT_RECIPE,
        'native_probability_up_hex': probability.hex(), 'native_probability_event': PROBABILITY_EVENT,
        'native_probability_scope': PROBABILITY_SCOPE, 'native_expected_delta_side': side,
        'target_outcome_status': 'awaiting_exact_authenticated_completed_native_target_close',
        'source_authenticated': False, 'can_authorize_issue': False, 'can_place_orders': False,
        'remaining_probability_available': False, 'new_cohort_required': True}
    return NativeAnchor(_TOKEN, encoded(body))


def static_remaining_view(anchor, *, live_mid, live_market_epoch, live_available_epoch,
                          decision_epoch, reference_quote_id):
    """Static difference to the original endpoint, not a new conditional forecast."""
    need(type(anchor) is NativeAnchor, 'native_anchor_value_required')
    body = anchor.as_dict()
    mid = real(live_mid, 'live_mid', positive=True)
    market = clock(live_market_epoch, 'live_market')
    available = clock(live_available_epoch, 'live_available')
    decision = clock(decision_epoch, 'decision')
    need(body['origin_close_epoch'] <= market <= available <= decision
         and body['issued_epoch'] <= decision < body['target_close_epoch'],
         'static_current_reference_clock_order_or_target_expired')
    need(type(reference_quote_id) is str and re.fullmatch('[0-9a-f]{64}', reference_quote_id),
         'explicit_descriptive_quote_identity_required')
    endpoint = float.fromhex(body['expected_endpoint_mid_hex'])
    remaining = endpoint - mid
    need(math.isfinite(remaining), 'static_remaining_difference_overflow')
    return {'schema_version': REMAINING_SCHEMA, 'native_anchor_sha256': anchor.sha256,
        'instrument': body['instrument'], 'native_origin_close_epoch': body['origin_close_epoch'],
        'native_target_close_epoch': body['target_close_epoch'],
        'native_expected_endpoint_mid_hex': body['expected_endpoint_mid_hex'],
        'live_mid_hex': mid.hex(), 'live_market_epoch': market, 'live_available_epoch': available,
        'decision_epoch': decision, 'reference_quote_id': reference_quote_id,
        'remaining_seconds_at_decision': body['target_close_epoch'] - decision,
        'static_remaining_difference_hex': remaining.hex(), 'static_remaining_side': direction(remaining),
        'probability_up': None, 'conditional_forecast': False, 'refreshed_horizon': False,
        'can_authorize_issue': False, 'can_place_orders': False, 'quote_authenticated': False,
        'scope': 'arithmetic_difference_to_unchanged_native_endpoint_not_remaining_expectation_or_risk'}
