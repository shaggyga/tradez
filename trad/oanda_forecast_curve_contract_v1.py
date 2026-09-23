"""Pure research curve receipts with native price targets and separate clocks.

This module never fits a model, reads market data, writes a feed or calls a broker.
The publication receipt must be created by a writer after verifying persisted
bytes; consumption records an independently observed copy. A hash is provenance,
not proof of predictive quality or of an asserted historical availability time.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Context, Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re
import time
from typing import Callable, Mapping

POLICY_SCHEMA = 'forecast_curve_policy_v1_20260909'
PREPARED_SCHEMA = 'prepared_forecast_curve_v1_20260909'
CURVE_SCHEMA = 'issued_forecast_curve_v1_20260909'
PUBLICATION_SCHEMA = 'forecast_curve_publication_v1_20260909'
CONSUMPTION_SCHEMA = 'forecast_curve_consumption_v1_20260909'
MAX_BYTES = 2 * 1024 * 1024
MAX_POINTS = 64
AUTHORITY = {'research_only': True, 'can_place_orders': False, 'can_promote': False,
             'can_authorize': False, 'account_eligible': False, 'execution_eligible': False,
             'proof_eligible': False}
_AUTHORITY_FALSE_KEYS = frozenset((*[k for k in AUTHORITY if k != 'research_only'],
                                  'live_execution_enabled', 'orders_supported'))
_HEX = re.compile(r'^[0-9a-f]{64}$')
_TOKEN = re.compile(r'^[A-Za-z0-9_.:/+\-]{1,240}$')


class CurveContractError(ValueError):
    """A bounded reason code, never raw source content or credentials."""


def canonical_bytes(value) -> bytes:
    budget = [0, 0]
    def check(item, depth=0):
        budget[0] += 1
        if depth > 18 or budget[0] > 16384:
            raise CurveContractError('json_structure_limit')
        if isinstance(item, dict):
            for key, member in item.items():
                if type(key) is not str:
                    raise CurveContractError('json_string_keys_required')
                check(key, depth+1)
                check(member, depth+1)
        elif type(item) is list:
            for member in item:
                check(member, depth+1)
        elif type(item) is str:
            budget[1] += len(item)
            if budget[1] > MAX_BYTES:
                raise CurveContractError('curve_byte_limit')
        elif item is None or type(item) is bool:
            pass
        elif type(item) is int:
            if abs(item) >= 10**96:
                raise CurveContractError('json_integer_limit')
        elif type(item) is float:
            if not math.isfinite(item):
                raise CurveContractError('not_bounded_json')
        else:
            raise CurveContractError('not_bounded_json')
    check(value)
    try:
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'),
                         ensure_ascii=True, allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise CurveContractError('not_bounded_json') from None
    if len(raw) > MAX_BYTES:
        raise CurveContractError('curve_byte_limit')
    return raw


def content_hash(value) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def decimal_number(value, *, positive=False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise CurveContractError('invalid_number')
    text = str(value)
    if len(text) > 96:
        raise CurveContractError('number_text_limit')
    try:
        result = Decimal(text)
    except InvalidOperation:
        raise CurveContractError('invalid_number') from None
    if not result.is_finite() or result.copy_abs() > Decimal('1e20'):
        raise CurveContractError('nonfinite_or_excessive_number')
    if len(result.as_tuple().digits) > 64 or not -60 <= result.as_tuple().exponent <= 60:
        raise CurveContractError('number_precision_limit')
    if positive and result <= 0:
        raise CurveContractError('nonpositive_number')
    return result


def decimal_text(value, *, positive=False) -> str:
    result = decimal_number(value, positive=positive)
    if result and result.adjusted() < -30:
        raise CurveContractError('number_precision_limit')
    return format(result, 'f')


def epoch(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CurveContractError('invalid_clock')
    if not math.isfinite(value) or not 0 < value < 1e12:
        raise CurveContractError('invalid_clock')
    return float(value)


def _token(value, reason='invalid_identity') -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise CurveContractError(reason)
    return value


def _hash_text(value) -> str:
    if not isinstance(value, str) or not _HEX.fullmatch(value):
        raise CurveContractError('invalid_sha256')
    return value


def _inert(value, depth=0):
    if depth > 14:
        raise CurveContractError('curve_depth_limit')
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _AUTHORITY_FALSE_KEYS and item is not False:
                raise CurveContractError('authority_not_false')
            if key == 'research_only' and item is not True:
                raise CurveContractError('research_only_not_true')
            _inert(item, depth+1)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _inert(item, depth+1)


def _seal(body: dict, key: str, *, id_key=None, prefix='') -> dict:
    value = json.loads(canonical_bytes(body))
    checksum = content_hash(value)
    value[key] = checksum
    if id_key:
        value[id_key] = prefix + checksum
    return value


def _verify(value, key, *, id_key=None, prefix='') -> dict:
    if not isinstance(value, dict):
        raise CurveContractError('record_object_required')
    _inert(value)
    excluded = {key, id_key} if id_key else {key}
    body = {k: v for k, v in value.items() if k not in excluded}
    checksum = content_hash(body)
    if value.get(key) != checksum or (id_key and value.get(id_key) != prefix+checksum):
        raise CurveContractError('record_hash_mismatch')
    if any(value.get(k) is not v for k, v in AUTHORITY.items()):
        raise CurveContractError('record_authority_fields')
    return body


def _require_fields(value, names):
    if not isinstance(value, dict) or not set(names) <= set(value):
        raise CurveContractError('record_required_fields_missing')


def _sources(value) -> dict:
    if not isinstance(value, dict) or not 1 <= len(value) <= 64:
        raise CurveContractError('source_bindings_required')
    answer = {}
    for name in value:
        _token(name, 'source_name_invalid')
    for name, checksum in sorted(value.items()):
        _token(name, 'source_name_invalid')
        if '..' in name.split('/') or '\\' in name:
            raise CurveContractError('source_name_invalid')
        answer[name] = _hash_text(checksum)
    return answer


def make_policy(*, native_horizons_sec, maximum_reference_age_sec,
                maximum_build_sec, maximum_issue_delay_sec,
                maximum_publication_delay_sec, maximum_decision_age_sec,
                minimum_remaining_sec=0) -> dict:
    if not isinstance(native_horizons_sec, (list, tuple)) or len(native_horizons_sec) > MAX_POINTS:
        raise CurveContractError('invalid_native_horizons')
    horizons = list(native_horizons_sec)
    if (not horizons or len(horizons) > MAX_POINTS
            or any(type(h) is not int or not 0 < h <= 604800 for h in horizons)
            or horizons != sorted(set(horizons))):
        raise CurveContractError('invalid_native_horizons')
    limits = {'maximum_reference_age_sec': maximum_reference_age_sec,
              'maximum_build_sec': maximum_build_sec,
              'maximum_issue_delay_sec': maximum_issue_delay_sec,
              'maximum_publication_delay_sec': maximum_publication_delay_sec,
              'maximum_decision_age_sec': maximum_decision_age_sec,
              'minimum_remaining_sec': minimum_remaining_sec}
    for key, value in limits.items():
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise CurveContractError('invalid_policy_limit')
        if key != 'minimum_remaining_sec' and value <= 0:
            raise CurveContractError('invalid_policy_limit')
    return _seal({'schema_version': POLICY_SCHEMA, 'native_horizons_sec': horizons,
                  **limits, **AUTHORITY}, 'policy_sha256')


def validate_policy(policy) -> dict:
    _verify(policy, 'policy_sha256')
    if policy.get('schema_version') != POLICY_SCHEMA:
        raise CurveContractError('policy_schema')
    _require_fields(policy, ('native_horizons_sec', 'maximum_reference_age_sec', 'maximum_build_sec',
        'maximum_issue_delay_sec', 'maximum_publication_delay_sec', 'maximum_decision_age_sec', 'minimum_remaining_sec'))
    rebuilt = make_policy(**{key: policy[key] for key in (
        'native_horizons_sec', 'maximum_reference_age_sec', 'maximum_build_sec',
        'maximum_issue_delay_sec', 'maximum_publication_delay_sec',
        'maximum_decision_age_sec', 'minimum_remaining_sec')})
    if rebuilt != policy:
        raise CurveContractError('policy_shape')
    return deepcopy(policy)


def prepare_curve(*, instrument, pip_size, forecast_cohort, model_sha256,
                  feature_version, source_bindings, input_capture_sha256,
                  input_available_epoch, reference_epoch, reference_label_epoch,
                  reference_price, reference_price_kind, bar_duration_sec,
                  model_fitted_epoch, computation_started_epoch, computed_epoch,
                  points, policy, computation_sha256, scope='current_research',
                  target_selection_policy=None, input_context=None) -> dict:
    policy = validate_policy(policy)
    if not isinstance(instrument, str) or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', instrument):
        raise CurveContractError('instrument_invalid')
    if instrument[:3] == instrument[4:]:
        raise CurveContractError('identical_currencies')
    if scope not in ('current_research', 'engineering_replay', 'synthetic_fixture'):
        raise CurveContractError('scope_invalid')
    reference = epoch(reference_epoch)
    label = epoch(reference_label_epoch)
    if type(bar_duration_sec) is not int or not 0 <= bar_duration_sec <= 86400:
        raise CurveContractError('bar_duration_invalid')
    if reference != label+bar_duration_sec or (bar_duration_sec and label % bar_duration_sec):
        raise CurveContractError('reference_label_price_clock_mismatch')
    target_policy = deepcopy(target_selection_policy) if target_selection_policy is not None else {
        'kind': 'exact_price_epoch', 'maximum_delay_sec': 0}
    if not isinstance(target_policy, dict) or set(target_policy) != {'kind', 'maximum_delay_sec'}:
        raise CurveContractError('target_selection_policy_shape')
    delay = target_policy['maximum_delay_sec']
    if type(delay) is not int or not 0 <= delay <= 60:
        raise CurveContractError('target_selection_delay_invalid')
    if not ((target_policy['kind'] == 'exact_price_epoch' and delay == 0) or (
            target_policy['kind'] == 'first_complete_bar_at_or_after_nominal' and delay > 0 and bar_duration_sec > 0)):
        raise CurveContractError('target_selection_policy_invalid')
    available, started, completed, fitted = map(epoch, (
        input_available_epoch, computation_started_epoch, computed_epoch, model_fitted_epoch))
    if not reference <= available <= started <= completed or fitted > started:
        raise CurveContractError('input_model_computation_clock_order')
    if scope == 'current_research' and reference < fitted:
        raise CurveContractError('current_input_precedes_model_fit')
    if completed-started > policy['maximum_build_sec']:
        raise CurveContractError('computation_too_slow')
    if scope == 'current_research' and completed-reference > policy['maximum_reference_age_sec']:
        raise CurveContractError('reference_stale_at_computation')
    pip = decimal_text(pip_size, positive=True)
    if decimal_number(pip) >= 1:
        raise CurveContractError('pip_scale_invalid')
    price = decimal_text(reference_price, positive=True)
    sources = _sources(source_bindings)
    context = {} if input_context is None else deepcopy(input_context)
    if not isinstance(context, dict):
        raise CurveContractError('input_context_object_required')
    _inert(context)
    canonical_bytes(context)
    if not isinstance(points, (list, tuple)) or len(points) != len(policy['native_horizons_sec']):
        raise CurveContractError('native_node_inventory_required')
    nodes = []
    for raw, expected_horizon in zip(points, policy['native_horizons_sec']):
        if not isinstance(raw, dict) or type(raw.get('horizon_sec')) is not int:
            raise CurveContractError('node_shape')
        _inert(raw)
        horizon = raw['horizon_sec']
        if horizon != expected_horizon:
            raise CurveContractError('node_horizon_order_or_membership')
        target, target_label = reference+horizon, label+horizon
        if epoch(raw.get('target_epoch')) != target or epoch(raw.get('target_label_epoch')) != target_label:
            raise CurveContractError('native_target_changed')
        body = {'horizon_sec': horizon, 'original_target_epoch': target,
                'target_label_epoch': target_label, 'target_price_kind': reference_price_kind,
                'target_price_window_end_epoch': target+delay,
                'producer_node_sha256': content_hash(raw),
                'status': raw.get('status', 'forecast'), **AUTHORITY}
        if body['status'] == 'unavailable':
            body['reason_code'] = _token(raw.get('reason_code'), 'missing_node_reason')
        elif body['status'] == 'forecast':
            move = decimal_text(raw.get('predicted_signed_pips'))
            with localcontext(Context(prec=192)):
                terminal = decimal_number(price)+decimal_number(move)*decimal_number(pip)
            if terminal <= 0:
                raise CurveContractError('nonpositive_terminal_price')
            probability = raw.get('probability_up')
            if probability is not None:
                probability = decimal_text(probability)
                if not 0 <= decimal_number(probability) <= 1:
                    raise CurveContractError('probability_out_of_range')
            sigma = raw.get('residual_std_pips')
            sigma = decimal_text(sigma, positive=True) if sigma is not None else None
            bounds = None
            lower, upper = raw.get('quantile_low_pips'), raw.get('quantile_high_pips')
            if (lower is None) != (upper is None):
                raise CurveContractError('incomplete_interval')
            if lower is not None:
                lower, upper = decimal_text(lower), decimal_text(upper)
                levels = raw.get('quantile_levels')
                if not isinstance(levels, list) or len(levels) != 2:
                    raise CurveContractError('quantile_levels_required')
                levels = [decimal_text(v) for v in levels]
                if not 0 < decimal_number(levels[0]) < decimal_number(levels[1]) < 1:
                    raise CurveContractError('quantile_levels_invalid')
                if decimal_number(lower) > decimal_number(upper):
                    raise CurveContractError('interval_reversed')
                bounds = {'low_pips': lower, 'high_pips': upper, 'levels': levels,
                          'scope': _token(raw.get('interval_scope'), 'interval_scope_required')}
            body.update(model_id=_token(raw.get('model_id')), predicted_signed_pips=move,
                        expected_terminal_price=decimal_text(terminal, positive=True),
                        probability_up=probability,
                        probability_scope=_token(raw.get('probability_scope', 'not_provided')),
                        residual_std_pips=sigma,
                        uncertainty_scope=_token(raw.get('uncertainty_scope', 'not_provided')),
                        interval=bounds)
        else:
            raise CurveContractError('node_status_invalid')
        nodes.append(_seal(body, 'node_sha256', id_key='node_id', prefix='curve_node_v1_'))
    body = {'schema_version': PREPARED_SCHEMA, 'scope': scope,
            'status': 'computed_not_issued', 'instrument': instrument, 'pip_size': pip,
            'forecast_cohort': _token(forecast_cohort), 'feature_version': _token(feature_version),
            'model_sha256': _hash_text(model_sha256), 'model_fitted_epoch': fitted,
            'source_bindings': sources, 'input_capture_sha256': _hash_text(input_capture_sha256),
            'computation_sha256': _hash_text(computation_sha256),
            'input_available_epoch': available, 'reference_epoch': reference,
            'reference_label_epoch': label, 'reference_price': price,
            'reference_price_kind': _token(reference_price_kind), 'bar_duration_sec': bar_duration_sec,
            'target_selection_policy': target_policy,
            'input_context': context,
            'computation_started_epoch': started, 'computed_epoch': completed,
            'policy': policy, 'nodes': nodes, **AUTHORITY}
    return _seal(body, 'prepared_sha256')


def validate_prepared(prepared, *, expected_source_bindings) -> dict:
    _verify(prepared, 'prepared_sha256')
    if prepared.get('schema_version') != PREPARED_SCHEMA or prepared.get('status') != 'computed_not_issued':
        raise CurveContractError('prepared_schema')
    _require_fields(prepared, ('policy', 'nodes', 'instrument', 'pip_size', 'forecast_cohort', 'model_sha256',
        'feature_version', 'source_bindings', 'input_capture_sha256', 'input_available_epoch', 'reference_epoch',
        'reference_label_epoch', 'reference_price', 'reference_price_kind', 'bar_duration_sec',
        'model_fitted_epoch', 'computation_started_epoch', 'computed_epoch', 'computation_sha256', 'scope',
        'target_selection_policy', 'input_context'))
    if not isinstance(prepared['nodes'], list) or not 1 <= len(prepared['nodes']) <= MAX_POINTS:
        raise CurveContractError('native_node_inventory_required')
    validate_policy(prepared['policy'])
    if prepared.get('source_bindings') != _sources(expected_source_bindings):
        raise CurveContractError('unregistered_source_bindings')
    # Full semantic replay validates mutated-but-resealed targets, units and clocks.
    nodes = []
    for node in prepared['nodes']:
        _verify(node, 'node_sha256', id_key='node_id', prefix='curve_node_v1_')
        _require_fields(node, ('horizon_sec', 'original_target_epoch', 'target_label_epoch', 'status', 'producer_node_sha256'))
        _require_fields(node, ('reason_code',) if node['status'] == 'unavailable' else (
            'model_id', 'predicted_signed_pips', 'probability_up', 'probability_scope', 'residual_std_pips',
            'uncertainty_scope', 'interval'))
        raw = {'horizon_sec': node['horizon_sec'], 'target_epoch': node['original_target_epoch'],
               'target_label_epoch': node['target_label_epoch'], 'status': node['status']}
        if node['status'] == 'unavailable':
            raw['reason_code'] = node['reason_code']
        else:
            raw.update({k: node[k] for k in ('model_id', 'predicted_signed_pips', 'probability_up',
                       'probability_scope', 'residual_std_pips', 'uncertainty_scope')})
            if node['interval'] is not None:
                _require_fields(node['interval'], ('low_pips', 'high_pips', 'levels', 'scope'))
                raw.update(quantile_low_pips=node['interval']['low_pips'],
                           quantile_high_pips=node['interval']['high_pips'],
                           quantile_levels=node['interval']['levels'], interval_scope=node['interval']['scope'])
        nodes.append(raw)
    keys = ('instrument', 'pip_size', 'forecast_cohort', 'model_sha256', 'feature_version',
            'source_bindings', 'input_capture_sha256', 'input_available_epoch', 'reference_epoch',
            'reference_label_epoch', 'reference_price', 'reference_price_kind', 'bar_duration_sec',
            'model_fitted_epoch', 'computation_started_epoch', 'computed_epoch', 'policy',
            'computation_sha256', 'scope', 'target_selection_policy', 'input_context')
    rebuilt = prepare_curve(**{k: prepared[k] for k in keys}, points=nodes)
    # Producer node hashes retain original upstream byte identity, not the replay projection.
    for old, new in zip(prepared['nodes'], rebuilt['nodes']):
        _hash_text(old['producer_node_sha256'])
        if set(old) != set(new):
            raise CurveContractError('prepared_node_shape')
        for key in new:
            if key not in ('producer_node_sha256', 'node_sha256', 'node_id') and old.get(key) != new[key]:
                raise CurveContractError('prepared_node_semantic_mismatch')
    if set(prepared) != set(rebuilt):
        raise CurveContractError('prepared_shape')
    for key in rebuilt:
        if key not in ('nodes', 'prepared_sha256') and prepared.get(key) != rebuilt[key]:
            raise CurveContractError('prepared_semantic_mismatch')
    return deepcopy(prepared)


def issue_curve(prepared, *, expected_source_bindings, clock: Callable = time.time) -> dict:
    prepared = validate_prepared(prepared, expected_source_bindings=expected_source_bindings)
    issued = epoch(clock())
    if prepared['scope'] != 'current_research':
        raise CurveContractError('nonprospective_computation_cannot_issue')
    policy = prepared['policy']
    if not 0 <= issued-prepared['computed_epoch'] <= policy['maximum_issue_delay_sec']:
        raise CurveContractError('issue_clock_or_delay')
    if issued-prepared['reference_epoch'] > policy['maximum_reference_age_sec']:
        raise CurveContractError('reference_stale_at_issue')
    admission = []
    for node in prepared['nodes']:
        reason = node.get('reason_code') if node['status'] != 'forecast' else None
        if reason is None and node['original_target_epoch']-issued <= policy['minimum_remaining_sec']:
            reason = 'native_target_elapsed_before_issue'
        admission.append({'node_id': node['node_id'], 'status': 'withheld' if reason else 'admitted',
                          'reason_code': reason})
    if not any(row['status'] == 'admitted' for row in admission):
        raise CurveContractError('no_unelapsed_native_forecast_nodes')
    return _seal({'schema_version': CURVE_SCHEMA, 'prepared_curve': prepared,
                  'issued_epoch': issued, 'node_admission': admission, **AUTHORITY},
                 'curve_sha256', id_key='curve_id', prefix='forecast_curve_v1_')


def validate_curve(curve, *, expected_source_bindings) -> dict:
    _verify(curve, 'curve_sha256', id_key='curve_id', prefix='forecast_curve_v1_')
    if curve.get('schema_version') != CURVE_SCHEMA:
        raise CurveContractError('curve_schema')
    _require_fields(curve, ('prepared_curve', 'issued_epoch'))
    rebuilt = issue_curve(curve['prepared_curve'], expected_source_bindings=expected_source_bindings,
                          clock=lambda: curve['issued_epoch'])
    if rebuilt != curve:
        raise CurveContractError('curve_semantic_mismatch')
    return deepcopy(curve)


def publication_receipt(curve, *, persisted_bytes_sha256, publication_started_epoch,
                        expected_source_bindings, clock: Callable = time.time) -> dict:
    """Called only after the publishing writer verifies the immutable file bytes."""
    curve = validate_curve(curve, expected_source_bindings=expected_source_bindings)
    complete = epoch(clock())
    started = epoch(publication_started_epoch)
    expected = hashlib.sha256(canonical_bytes(curve)).hexdigest()
    if persisted_bytes_sha256 != expected:
        raise CurveContractError('persisted_curve_bytes_mismatch')
    if not curve['issued_epoch'] <= started <= complete:
        raise CurveContractError('publication_clock_order')
    if complete-curve['issued_epoch'] > curve['prepared_curve']['policy']['maximum_publication_delay_sec']:
        raise CurveContractError('publication_too_slow')
    return _seal({'schema_version': PUBLICATION_SCHEMA, 'curve_id': curve['curve_id'],
                  'curve_sha256': curve['curve_sha256'], 'persisted_bytes_sha256': expected,
                  'issued_epoch': curve['issued_epoch'], 'publication_started_epoch': started,
                  'publication_completed_epoch': complete, **AUTHORITY}, 'publication_sha256')


def consume_curve(curve, publication, *, expected_source_bindings, clock: Callable = time.time) -> dict:
    """Record observation, not decision eligibility; the adapter rechecks each target."""
    curve = validate_curve(curve, expected_source_bindings=expected_source_bindings)
    _verify(publication, 'publication_sha256')
    _require_fields(publication, ('persisted_bytes_sha256', 'publication_started_epoch', 'publication_completed_epoch'))
    rebuilt = publication_receipt(curve, persisted_bytes_sha256=publication['persisted_bytes_sha256'],
        publication_started_epoch=publication['publication_started_epoch'],
        expected_source_bindings=expected_source_bindings,
        clock=lambda: publication['publication_completed_epoch'])
    if rebuilt != publication:
        raise CurveContractError('publication_semantic_mismatch')
    observed = epoch(clock())
    if observed < publication['publication_completed_epoch']:
        raise CurveContractError('consumption_precedes_publication')
    policy = curve['prepared_curve']['policy']
    if observed-curve['issued_epoch'] > policy['maximum_decision_age_sec']:
        raise CurveContractError('curve_too_old_at_consumption')
    return _seal({'schema_version': CONSUMPTION_SCHEMA, 'curve_id': curve['curve_id'],
                  'curve_sha256': curve['curve_sha256'],
                  'publication_sha256': publication['publication_sha256'],
                  'available_epoch': observed, 'observed_epoch': observed,
                  'scope': 'observed_copy_not_node_decision_eligibility',
                  **AUTHORITY}, 'consumption_sha256')


def validate_consumption(curve, publication, consumption, *, expected_source_bindings) -> dict:
    _verify(consumption, 'consumption_sha256')
    _require_fields(consumption, ('observed_epoch',))
    rebuilt = consume_curve(curve, publication, expected_source_bindings=expected_source_bindings,
                            clock=lambda: consumption['observed_epoch'])
    if consumption != rebuilt:
        raise CurveContractError('consumption_semantic_mismatch')
    return deepcopy(consumption)
