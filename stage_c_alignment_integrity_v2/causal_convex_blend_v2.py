"""One constrained scalar second layer fitted on mature prequential prefixes."""
from collections import Counter, defaultdict
import math
import random

from contracts import fingerprint

PREDICTIONS = {'ridge': 'ridge_prediction_bps', 'recovered_hgb': 'recovered_hgb_prediction_bps',
               'fixed_equal_half_blend': 'blend_prediction_bps', 'zero_return_diagnostic': 'zero_prediction_bps',
               'causal_convex_blend': 'learned_prediction_bps'}
BASES = tuple(k for k in PREDICTIONS if k != 'causal_convex_blend')


def scope(row):
    return [row['group'], row['target_id'], row['procedure']]


def label(row, outcomes, horizon):
    key = row['record_id'], row['target_id']
    value = outcomes.get(key)
    if value is None or value['record_id'] != row['record_id'] or value['target_id'] != row['target_id']:
        raise ValueError('convex_exact_original_label_required')
    if value['label_end_epoch'] != row['decision_epoch'] + horizon * 60:
        raise ValueError('convex_target_maturity_mismatch')
    if value['available_epoch'] < value['label_end_epoch']:
        raise ValueError('convex_label_readiness_precedes_endpoint')
    if row['available_epoch'] >= value['label_end_epoch']:
        raise ValueError('convex_base_forecast_not_pre_target')
    return value


def validate_row(row, expected_scope):
    if scope(row) != list(expected_scope):
        raise ValueError('convex_scope_mismatch')
    if row['record_id'] != f"{row['instrument']}:{row['decision_epoch']}":
        raise ValueError('convex_canonical_record_required')
    if not row['selected_fit_cutoff'] < row['decision_epoch'] <= row['available_epoch']:
        raise ValueError('convex_in_sample_or_backdated_base')
    for method in ('ridge', 'recovered_hgb'):
        if row['base_model_ids'][method] != fingerprint({'fit_id': row['selected_fit_id'], 'method': method}):
            raise ValueError('convex_model_identity_mismatch')
        if not row['selected_fit_cutoff'] <= row['base_model_ready_epochs'][method] <= row['decision_epoch']:
            raise ValueError('convex_model_readiness_mismatch')
    for name in ('ridge_prediction_bps', 'recovered_hgb_prediction_bps', 'blend_prediction_bps'):
        if type(row[name]) not in (int, float) or not math.isfinite(row[name]):
            raise ValueError('convex_finite_base_required')


def fit_snapshot(rows, outcomes, cutoff, expected_scope, horizon, contract):
    if type(cutoff) is not int:
        raise ValueError('convex_integer_cutoff_required')
    eligible, seen = [], set()
    for row in rows:
        validate_row(row, expected_scope)
        if row['record_id'] in seen:
            raise ValueError('convex_duplicate_market_event')
        seen.add(row['record_id'])
        outcome = label(row, outcomes, horizon)
        if (row['decision_epoch'] >= cutoff or row['available_epoch'] > cutoff
                or outcome['available_epoch'] > cutoff or outcome['value'] is None):
            continue
        if type(outcome['value']) not in (int, float) or not math.isfinite(outcome['value']):
            raise ValueError('convex_finite_training_label_required')
        eligible.append((row, outcome))
    eligible.sort(key=lambda item: (item[0]['decision_epoch'], item[0]['instrument']))
    counts = Counter(row['decision_epoch'] for row, _ in eligible)
    days = {origin // 86400 for origin in counts}
    pairs = {row['instrument'] for row, _ in eligible}
    membership = [{'record_id': row['record_id'], 'origin_epoch': row['decision_epoch'],
                   'base_prediction_sha256': fingerprint(row), 'label_sha256': fingerprint(outcome),
                   'forecast_available_epoch': row['available_epoch'], 'label_available_epoch': outcome['available_epoch']}
                  for row, outcome in eligible]
    support = {'rows': len(eligible), 'distinct_origins': len(counts), 'distinct_utc_days': len(days),
               'distinct_pairs': len(pairs), 'training_population_sha256': fingerprint(membership),
               'maximum_forecast_available_epoch': max((r['available_epoch'] for r, _ in eligible), default=None),
               'maximum_label_available_epoch': max((o['available_epoch'] for _, o in eligible), default=None)}
    ready = (len(counts) >= contract['minimum_distinct_origins'] and len(days) >= contract['minimum_distinct_utc_days']
             and len(pairs) >= contract['minimum_distinct_pairs'])
    status, parameters = 'insufficient_distinct_support', None
    if ready:
        # Common scaling protects the sufficient statistics from overflow; q is
        # equal total weight per origin, equal pair weight within that origin.
        scale = max(abs(v) for row, outcome in eligible for v in
                    (row['ridge_prediction_bps'], row['recovered_hgb_prediction_bps'], outcome['value'])) or 1.0
        terms = [(1 / counts[row['decision_epoch']],
                  row['ridge_prediction_bps'] / scale - row['recovered_hgb_prediction_bps'] / scale,
                  outcome['value'] / scale - row['recovered_hgb_prediction_bps'] / scale)
                 for row, outcome in eligible]
        denominator = math.fsum(q * d * d for q, d, t in terms)
        numerator = math.fsum(q * d * t for q, d, t in terms)
        if denominator == 0:
            status = 'unidentifiable_equal_base_predictions'
        else:
            weight = min(1.0, max(0.0, numerator / denominator))
            status = 'fitted'
            parameters = {'ridge_weight': weight, 'recovered_hgb_weight': 1.0 - weight,
                          'weight_at_boundary': weight in (0.0, 1.0), 'scaled_numerator': numerator,
                          'scaled_denominator': denominator, 'common_scale': scale}
    result = {'schema_version': 'forex_causal_convex_snapshot.v1', 'scope': list(expected_scope),
              'cutoff_epoch': cutoff, 'status': status, 'parameters': parameters, 'support': support,
              'training_membership': membership, 'contract_sha256': fingerprint(contract),
              'base_models_refitted': False, 'actual_historical_issue_receipts_qualified': False}
    result['layer_id'] = fingerprint(result)
    return result


def apply_snapshot(row, snapshot, mode):
    if snapshot['layer_id'] != fingerprint({k: v for k, v in snapshot.items() if k != 'layer_id'}):
        raise ValueError('convex_snapshot_identity_mismatch')
    validate_row(row, snapshot['scope'])
    if snapshot['cutoff_epoch'] > row['decision_epoch']:
        raise ValueError('convex_future_snapshot')
    if any(item['record_id'] == row['record_id'] for item in snapshot['training_membership']):
        raise ValueError('convex_current_example_in_training')
    if mode not in ('frozen_prefix', 'expanding_prefix'):
        raise ValueError('convex_unknown_update_mode')
    if snapshot['status'] != 'fitted':
        return None
    weight = snapshot['parameters']['ridge_weight']
    if not math.isfinite(weight) or not 0 <= weight <= 1:
        raise ValueError('convex_weight_out_of_bounds')
    value = weight * row['ridge_prediction_bps'] + (1 - weight) * row['recovered_hgb_prediction_bps']
    if not math.isfinite(value):
        raise ValueError('convex_nonfinite_prediction')
    return {**row, 'mode': mode, 'layer_id': snapshot['layer_id'], 'layer_cutoff_epoch': snapshot['cutoff_epoch'],
            'ridge_weight': weight, 'learned_prediction_bps': value, 'outcomes_revealed': False,
            'production_available_epoch': None, 'native_policy_admitted': False,
            'scope': 'offline_modeled_clock_prequential_convex_diagnostic'}


def assessed(rows, outcomes, horizon, asof):
    selected = []
    for row in rows:
        outcome = label(row, outcomes, horizon)
        if row['available_epoch'] > asof or outcome['available_epoch'] > asof or outcome['value'] is None:
            continue
        if not math.isfinite(outcome['value']):
            raise ValueError('convex_nonfinite_assessment')
        selected.append({**row, 'outcome_bps': outcome['value']})
    return selected


def metrics(rows):
    if not rows:
        return None
    result = {'rows': len(rows), 'distinct_origins': len({r['decision_epoch'] for r in rows}),
              'distinct_pairs': len({r['instrument'] for r in rows}), 'methods': {}, 'paired_deltas': {}}
    errors = {name: [r[key] - r['outcome_bps'] for r in rows] for name, key in PREDICTIONS.items()}
    for name, values in errors.items():
        result['methods'][name] = {'mae_bps': math.fsum(map(abs, values)) / len(rows),
                                  'mse_bps2': math.fsum(v * v for v in values) / len(rows),
                                  'bias_bps': math.fsum(values) / len(rows)}
    for name in BASES:
        pairs = list(zip(errors['causal_convex_blend'], errors[name]))
        result['paired_deltas'][name] = {'mae_bps': math.fsum(abs(a) - abs(b) for a, b in pairs) / len(rows),
                                         'mse_bps2': math.fsum(a*a - b*b for a, b in pairs) / len(rows)}
    return result


def score(rows, outcomes, horizon, asof):
    selected = assessed(rows, outcomes, horizon, asof)
    by_origin, by_day = defaultdict(list), defaultdict(list)
    for row in selected:
        by_origin[row['decision_epoch']].append(row)
        by_day[row['decision_epoch'] // 86400].append(row)
    return {'forecast_rows': len(rows), 'mature_rows': len(selected), 'overall': metrics(selected),
            'by_origin': [{'origin_epoch': k, 'metrics': metrics(v)} for k, v in sorted(by_origin.items())],
            'by_utc_day': [{'utc_day': k, 'metrics': metrics(v)} for k, v in sorted(by_day.items())]}


def sensitivity(rows, outcomes, horizon, asof):
    selected = assessed(rows, outcomes, horizon, asof)
    grouped = defaultdict(list)
    for row in selected: grouped[row['decision_epoch']].append(row)
    origins = sorted(grouped)
    totals = {o: {name: math.fsum(abs(r['learned_prediction_bps'] - r['outcome_bps']) -
                                 abs(r[PREDICTIONS[name]] - r['outcome_bps']) for r in group)
                  for name in BASES} for o, group in grouped.items()}
    result = []
    for length in (4, 8, 20):
        reason = ('insufficient_distinct_origins' if len(origins) < length else
                  'single_circular_block_no_resampling_variation' if len(origins) == length else
                  'irregular_origin_grid' if any(b-a != 21600 for a, b in zip(origins, origins[1:])) else None)
        if reason:
            result.append({'block_length': length, 'status': reason, 'interval': None}); continue
        rng = random.Random(20260922 + length)
        deltas = {name: [] for name in BASES}
        for _ in range(2000):
            draw = []
            while len(draw) < len(origins):
                start = rng.randrange(len(origins))
                draw.extend(origins[(start+j) % len(origins)] for j in range(length))
            draw = draw[:len(origins)]
            count = sum(len(grouped[o]) for o in draw)
            for name in BASES: deltas[name].append(math.fsum(totals[o][name] for o in draw) / count)
        result.append({'block_length': length, 'status': 'descriptive_only', 'seed': 20260922+length,
                       'replicates': 2000, 'paired_mae_delta_intervals_10_90':
                       {name: [sorted(values)[199], sorted(values)[1799]] for name, values in deltas.items()},
                       'effective_n_or_p_value': None})
    return result
