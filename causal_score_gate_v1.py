"""Pure static-origin score gate. No marks, outcomes, model fitting or I/O.

The caller supplies authenticated prepared continuation and original forecast
score evidence. This numerical adapter does not authenticate that provenance.
It preserves continuation rows and gates only their exact entry subset.
"""
from copy import deepcopy
from decimal import Decimal, Context, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
import hashlib
import json
import math
import re

SCHEMA = 'chronological_static_origin_score_gate_v1_20260913'
POLICY_FIELDS = {'schema', 'session_id', 'scopes', 'lookback_sec', 'minimum_history',
                 'maximum_history', 'maximum_seen', 'entry_percentile'}
SCOPE_FIELDS = {'scope_id', 'instrument', 'model_id', 'model_sha256', 'forecast_cohort',
                'source_bindings', 'horizon_sec', 'score_recipe'}
SAMPLE_FIELDS = SCOPE_FIELDS | {'sample_id', 'curve_id', 'curve_sha256', 'node_id', 'node_sha256',
                              'reference_epoch', 'original_target_epoch', 'source_available_epoch', 'static_score'}
IDENTITY_FIELDS = ('scope_id', 'model_id', 'forecast_cohort', 'curve_id', 'node_id')
FORBIDDEN = {'realized_outcome_atr', 'realized_pnl', 'terminal_outcome', 'terminal_observed_price',
             'future_prices', 'execution_quotes', 'feedback_quotes'}
MAX_NUMERIC_TEXT = 128
MAX_SIGNIFICANT_DIGITS = 96
MAX_DECIMAL_EXPONENT = 128
DECIMAL_TEXT = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,3})?\Z')

def need(condition, reason):
    if not condition:
        raise ValueError(reason)

def number(value):
    need(type(value) in (str, int, float, Decimal), 'finite_number_required')
    if type(value) is int:
        need(value.bit_length() <= 426, 'bounded_numeric_magnitude_required')
    if type(value) is Decimal:
        result = value
    else:
        text = str(value)
        need(len(text) <= MAX_NUMERIC_TEXT and DECIMAL_TEXT.fullmatch(text) is not None,
             'bounded_decimal_text_required')
        try:
            result = Decimal(text)
        except Exception as error:
            raise ValueError('finite_number_required') from error
    need(result.is_finite(), 'finite_number_required')
    parts = result.as_tuple()
    need(len(parts.digits) <= MAX_SIGNIFICANT_DIGITS and abs(parts.exponent) <= MAX_DECIMAL_EXPONENT and
         abs(result.adjusted()) <= MAX_DECIMAL_EXPONENT, 'bounded_decimal_precision_and_exponent_required')
    return result

def epoch(value):
    need(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 8_000_000_000,
         'finite_epoch_required')
    return value

def _text(value):
    need(type(value) is str and 1 <= len(value) <= 240, 'bounded_identity_required')
    return value

def _sha(value):
    need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None, 'sha256_required')
    return value

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False, default=lambda item: format(number(item), 'f') if isinstance(item, Decimal)
                      else (_ for _ in ()).throw(TypeError('unsupported_value'))).encode()

def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def _sources(value):
    need(type(value) is dict and 1 <= len(value) <= 64, 'bounded_source_bindings_required')
    for name, value_hash in value.items():
        _text(name); _sha(value_hash)

def _scope(scope):
    need(type(scope) is dict and set(scope) == SCOPE_FIELDS, 'exact_scope_fields_required')
    for key in ('scope_id', 'model_id', 'forecast_cohort', 'score_recipe'):
        _text(scope[key])
    need(type(scope['instrument']) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', scope['instrument']) is not None,
         'instrument_required')
    need(scope['instrument'][:3] != scope['instrument'][4:], 'distinct_currencies_required')
    _sha(scope['model_sha256']); _sources(scope['source_bindings'])
    need(type(scope['horizon_sec']) is int and 1 <= scope['horizon_sec'] <= 7 * 86400, 'bounded_horizon_required')

def validate_policy(policy):
    need(type(policy) is dict and set(policy) == POLICY_FIELDS and policy['schema'] == SCHEMA, 'exact_gate_policy_required')
    _text(policy['session_id'])
    need(type(policy['scopes']) is list and 1 <= len(policy['scopes']) <= 68, 'bounded_scopes_required')
    scopes = {}
    for scope in policy['scopes']:
        _scope(scope)
        need(scope['scope_id'] not in scopes, 'duplicate_scope')
        scopes[scope['scope_id']] = scope
    need(type(policy['lookback_sec']) is int and 1 <= policy['lookback_sec'] <= 366 * 86400, 'bounded_lookback_required')
    need(type(policy['minimum_history']) is int and type(policy['maximum_history']) is int and
         1 <= policy['minimum_history'] <= policy['maximum_history'] <= 100000, 'bounded_history_counts_required')
    need(type(policy['maximum_seen']) is int and policy['maximum_history'] <= policy['maximum_seen'] <= 100000,
         'bounded_session_identity_inventory_required')
    need(0 <= number(policy['entry_percentile']) <= 1, 'percentile_between_zero_and_one_required')
    return scopes

def sample_identity(sample):
    return digest({key: sample[key] for key in IDENTITY_FIELDS})

def _sample(sample, scopes):
    need(type(sample) is dict and set(sample) == SAMPLE_FIELDS, 'exact_static_score_fields_required')
    scope = scopes.get(sample['scope_id'])
    need(scope is not None and canonical({key: sample[key] for key in SCOPE_FIELDS}) == canonical(scope), 'score_scope_mismatch')
    for key in ('curve_id', 'node_id'):
        _text(sample[key])
    for key in ('curve_sha256', 'node_sha256'):
        _sha(sample[key])
    reference, target, available = (epoch(sample[key]) for key in ('reference_epoch', 'original_target_epoch', 'source_available_epoch'))
    need(reference <= available < target and target-reference == sample['horizon_sec'], 'score_native_clock_mismatch')
    number(sample['static_score'])
    need(sample['sample_id'] == sample_identity(sample), 'score_sample_identity_mismatch')

def _seal_state(body):
    return {**body, 'state_sha256': digest(body)}

def initial_state(policy):
    validate_policy(policy)
    return _seal_state({'schema': SCHEMA, 'policy_sha256': digest(policy), 'last_decision_epoch': None, 'seen': {}})

def _state(state, policy, scopes):
    need(type(state) is dict and set(state) == {'schema', 'policy_sha256', 'last_decision_epoch', 'seen', 'state_sha256'}, 'exact_gate_state_required')
    body = {key: value for key, value in state.items() if key != 'state_sha256'}
    need(state['state_sha256'] == digest(body) and state['schema'] == SCHEMA and state['policy_sha256'] == digest(policy), 'gate_state_binding_mismatch')
    seen = state['seen']
    need(type(seen) is dict and len(seen) <= policy['maximum_seen'], 'bounded_seen_inventory_required')
    last = state['last_decision_epoch']
    need(last is not None or not seen, 'unstarted_state_must_be_empty')
    if last is not None:
        epoch(last)
    for identity, row in seen.items():
        need(type(row) is dict and set(row) == {'sample', 'first_seen_decision_epoch'}, 'exact_seen_record_required')
        _sample(row['sample'], scopes)
        need(identity == row['sample']['sample_id'], 'seen_identity_mismatch')
        observed = epoch(row['first_seen_decision_epoch'])
        need(row['sample']['source_available_epoch'] <= observed <= last and observed < row['sample']['original_target_epoch'],
             'seen_observation_clock_mismatch')

def _check_no_outcomes(value):
    if isinstance(value, dict):
        need(not FORBIDDEN.intersection(value), 'future_or_outcome_field_refused')
        for item in value.values():
            _check_no_outcomes(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_no_outcomes(item)

def _bind(sample, row, decision):
    raw = row.get('source_payload')
    need(type(raw) is dict, 'original_prepared_source_required')
    for field in ('instrument', 'model_id', 'model_sha256', 'forecast_cohort', 'source_bindings', 'curve_id',
                  'curve_sha256', 'node_id', 'node_sha256', 'reference_epoch', 'original_target_epoch'):
        need(canonical(raw.get(field)) == canonical(sample[field]), 'original_score_binding_mismatch:' + field)
    need(row.get('kind') == 'curve' and row.get('candidate_id') == sample['node_id'] and
         row.get('instrument') == sample['instrument'], 'prepared_candidate_identity_mismatch')
    need(epoch(row.get('decision_epoch')) == decision and sample['source_available_epoch'] <=
         epoch(row.get('available_epoch')) <= decision < epoch(row.get('original_target_epoch')), 'score_not_currently_available')
    need(row['original_target_epoch'] == sample['original_target_epoch'], 'prepared_original_target_mismatch')
    need(epoch(raw.get('issued_epoch')) <= sample['source_available_epoch'], 'score_precedes_original_issue')

def build_score_channels(prepared_continuation, current_score_records, state, *, decision_epoch, policy):
    """Freeze prior history for the entire frame; append unique originals last.

    Repriced candidate IDs/quotes do not add samples. Window selection uses
    first-seen time and identity only; the session-wide inventory is never
    evicted. Conflicts refuse entries while leaving continuation unchanged.
    """
    scopes = validate_policy(policy); decision = epoch(decision_epoch)
    _state(state, policy, scopes)
    need(state['last_decision_epoch'] is None or decision > state['last_decision_epoch'], 'strictly_increasing_gate_decisions_required')
    need(type(prepared_continuation) in (list, tuple) and len(prepared_continuation) <= 68, 'bounded_continuation_required')
    need(type(current_score_records) in (list, tuple) and len(current_score_records) <= 136, 'bounded_current_scores_required')
    _check_no_outcomes(prepared_continuation)
    rows = deepcopy(list(prepared_continuation)); by_pair = {}
    for row in rows:
        need(type(row) is dict and type(row.get('instrument')) is str and row['instrument'] not in by_pair, 'unique_prepared_instruments_required')
        need(epoch(row.get('decision_epoch')) == decision and epoch(row.get('available_epoch')) <= decision,
             'continuation_decision_clock_mismatch')
        by_pair[row['instrument']] = row
    groups = {}; refusals = []; invalid_identities = set()
    for original in current_score_records:
        try:
            _sample(original, scopes)
            pair = original['instrument']; row = by_pair.get(pair)
            need(row is not None, 'unmatched_score_record')
            _bind(original, row, decision)
            groups.setdefault(original['sample_id'], []).append(deepcopy(original))
        except (ValueError, KeyError, TypeError) as error:
            if isinstance(original, dict) and type(original.get('sample_id')) is str:
                invalid_identities.add(original['sample_id'])
            refusals.append({'sample_id': original.get('sample_id') if isinstance(original, dict) else None,
                             'reason': str(error)})
    entries = []; receipts = []; new = {}; admitted_pairs = set()
    for identity in sorted(groups):
        versions = groups[identity]; sample = versions[0]
        old = state['seen'].get(identity)
        if identity in invalid_identities or any(canonical(item) != canonical(sample) for item in versions) or (old is not None and canonical(old['sample']) != canonical(sample)):
            refusals.append({'sample_id': identity, 'reason': 'conflicting_static_forecast_sample'}); continue
        # Exclude the candidate's own earlier observation from its calibration.
        prior = [(key, item) for key, item in state['seen'].items() if key != identity and
                 item['sample']['scope_id'] == sample['scope_id'] and
                 decision-policy['lookback_sec'] <= item['first_seen_decision_epoch'] < decision]
        prior.sort(key=lambda item: (item[1]['first_seen_decision_epoch'], item[0]))
        prior = prior[-policy['maximum_history']:]
        score = number(sample['static_score']); count = len(prior)
        less = sum(number(item['sample']['static_score']) < score for _, item in prior)
        equal = sum(number(item['sample']['static_score']) == score for _, item in prior)
        numerator_twice = 2*less + 2 + equal
        denominator_twice = 2*(count+1)
        with localcontext(Context(prec=50, rounding=ROUND_HALF_EVEN)):
            percentile = Decimal(numerator_twice)/Decimal(denominator_twice) if count >= policy['minimum_history'] else None
        allowed = percentile is not None and Fraction(numerator_twice, denominator_twice) >= Fraction(number(policy['entry_percentile']))
        pair = sample['instrument']; row = by_pair[pair]
        if type(row.get('side')) is not int or row['side'] not in (-1, 0, 1):
            raise ValueError('prepared_side_required')
        allowed = allowed and row['side'] != 0
        reason = ('calibration_history_insufficient' if percentile is None else
                  'neutral_continuation' if row['side'] == 0 else 'entry_admitted' if allowed else 'below_past_score_percentile')
        if allowed:
            need(pair not in admitted_pairs, 'ambiguous_scores_for_prepared_row')
            entries.append(deepcopy(row)); admitted_pairs.add(pair)
        receipts.append({'sample_id': identity, 'scope_id': sample['scope_id'], 'history_count': count,
            'history_sample_ids': [key for key, _ in prior], 'percentile': None if percentile is None else str(percentile),
            'rank_numerator_twice': numerator_twice, 'rank_denominator_twice': denominator_twice,
            'entry_admitted': allowed, 'reason': reason, 'counted_before': old is not None})
        if old is None:
            new[identity] = {'sample': sample, 'first_seen_decision_epoch': decision}
    need(len(state['seen']) + len(new) <= policy['maximum_seen'], 'session_seen_inventory_full')
    next_seen = deepcopy(state['seen']); next_seen.update(new)
    next_state = _seal_state({'schema': SCHEMA, 'policy_sha256': digest(policy),
                              'last_decision_epoch': decision, 'seen': next_seen})
    # Canonical ordering removes arrival-order dependence without repricing rows.
    entries.sort(key=lambda row: (row['instrument'], row['candidate_id']))
    refusals.sort(key=canonical)
    return {'schema': SCHEMA, 'decision_epoch': decision, 'entry_rows': entries, 'continuation_rows': rows,
            'refusals': refusals, 'percentile_receipts': receipts, 'next_score_state': next_state,
            'prior_state_sha256': state['state_sha256'], 'policy_sha256': digest(policy),
            'research_only': True, 'can_place_orders': False, 'provenance_authenticated_here': False}
