"""Fresh native qualification for fixed currency-factor forecast projections.

The projection diagnostic is immutable evidence.  This bridge only uses it to check
the freshly recomputed values and source lineage; it never imports its availability
time into a newly issued native packet.
"""
import math
import time

from contracts import fingerprint
from currency_projection_v2 import BASES, project_frame


VARIANTS = ('direct', 'currency_projection', 'half_residual')


def _identity(item):
    if item.get('forecast_id') != fingerprint({k: v for k, v in item.items() if k != 'forecast_id'}):
        raise ValueError('projection_native_parent_forecast_identity')


def _by(items, key, duplicate_reason):
    result = {}
    for item in items:
        value = key(item)
        if value in result:
            raise ValueError(duplicate_reason)
        result[value] = item
    return result


def _fresh_raw_frame(origin, horizon, raw_frame, values, contract):
    """Validate fresh base values while retaining the original raw record identity."""
    if raw_frame['origin_epoch'] != origin or raw_frame['horizon_minutes'] != horizon:
        raise ValueError('projection_native_parent_raw_frame_registration')
    expected = {(pair, base) for pair in contract['universe'] for base in BASES}
    coverage = [x for x in raw_frame['coverage'] if x['variant'] == 'raw_unrestricted']
    if len(coverage) != len(expected) or {(x['instrument'], x['base_method']) for x in coverage} != expected:
        raise ValueError('projection_native_raw_coverage_required')
    raw = [x for x in raw_frame['predictions'] if x['variant'] == 'raw_unrestricted']
    indexed = _by(raw, lambda x: (x['instrument'], x['base_method']), 'projection_native_duplicate_raw_forecast')
    eligible = {(x['instrument'], x['base_method']) for x in coverage if x['reason'] == 'eligible'}
    if set(indexed) != eligible:
        raise ValueError('projection_native_raw_eligibility_changed')
    for item in raw:
        _identity(item)
        fresh = values.get(item['record_id'])
        if fresh is None or item['base_method'] not in fresh['signed']:
            raise ValueError('projection_native_fresh_base_support_changed')
        value = fresh['signed'][item['base_method']]
        if not math.isfinite(value) or value != item['prediction_bps']:
            raise ValueError('projection_native_fresh_base_values_changed')
    return raw_frame


def _compare_projection(fresh, saved):
    """Require exact numeric and diagnostic equivalence, never timestamp equivalence."""
    if fresh['origin_epoch'] != saved['origin_epoch'] or fresh['horizon_minutes'] != saved['horizon_minutes']:
        raise ValueError('projection_native_saved_projection_registration')
    a = _by(fresh['coverage'], lambda x: (x['instrument'], x['method']), 'projection_native_fresh_coverage_duplicate')
    b = _by(saved['coverage'], lambda x: (x['instrument'], x['method']), 'projection_native_saved_coverage_duplicate')
    if a != b:
        raise ValueError('projection_native_projection_coverage_changed')
    a = _by(fresh['predictions'], lambda x: (x['instrument'], x['base_method'], x['variant']), 'projection_native_fresh_prediction_duplicate')
    b = _by(saved['predictions'], lambda x: (x['instrument'], x['base_method'], x['variant']), 'projection_native_saved_prediction_duplicate')
    if set(a) != set(b):
        raise ValueError('projection_native_projection_inventory_changed')
    for key, item in a.items():
        prior = b[key]
        _identity(prior)
        # The diagnostic available epoch is intentionally not compared to a native
        # availability epoch. It remains provenance in the output below.
        fields = ('prediction_bps', 'source_forecast_id', 'source_model_id', 'signed_fit_id',
                  'constituent_snapshot_sha256', 'layer_definition_sha256', 'target_epoch',
                  'target_id', 'horizon_minutes', 'record_id')
        if any(item[field] != prior[field] for field in fields):
            raise ValueError('projection_native_projection_value_or_lineage_changed')


def _native_prediction(projected, diagnostic, direct_parent, observation, origin, cohort, model_ready_epoch):
    _identity(projected)
    _identity(diagnostic)
    if diagnostic['forecast_id'] != projected['forecast_id'] and diagnostic['prediction_bps'] != projected['prediction_bps']:
        raise ValueError('projection_native_diagnostic_value_changed')
    if projected['source_forecast_id'] != direct_parent['diagnostic_forecast_id']:
        raise ValueError('projection_native_direct_parent_lineage_changed')
    if projected['variant'] == 'direct':
        model_id = projected['source_model_id']
        eligibility = None
    else:
        model_id = fingerprint({
            'kind': 'fixed_currency_projection_native.v1',
            'base_method': projected['base_method'],
            'variant': projected['variant'],
            'source_model_id': projected['source_model_id'],
            'layer_definition_sha256': projected['layer_definition_sha256'],
            'constituent_snapshot_sha256': projected['constituent_snapshot_sha256'],
        })
        eligibility = projected['constituent_snapshot_sha256']
    result = {
        'record_id': projected['record_id'],
        'instrument': projected['instrument'],
        'decision_epoch': origin,
        'target_epoch': projected['target_epoch'],
        'target_id': projected['target_id'],
        'available_epoch': origin + 2,
        'base_method': projected['base_method'],
        'variant': projected['variant'],
        'prediction_bps': projected['prediction_bps'],
        'absolute_prediction_bps': direct_parent['absolute_prediction_bps'],
        'model_id': model_id,
        'model_ready_epoch': model_ready_epoch,
        'computation_started_epoch': origin,
        'signed_fit_id': projected['signed_fit_id'],
        'absolute_fit_id': direct_parent['absolute_fit_id'],
        'base_prediction_sha256': direct_parent['base_prediction_sha256'],
        'observation_sha256': fingerprint(observation),
        'eligibility_snapshot_id': eligibility,
        'diagnostic_forecast_id': diagnostic['forecast_id'],
        'diagnostic_available_epoch': diagnostic['available_epoch'],
        'diagnostic_source_forecast_id': projected['source_forecast_id'],
        'projection_definition_sha256': projected['layer_definition_sha256'],
        'projection_constituent_snapshot_sha256': projected['constituent_snapshot_sha256'],
        'cohort': cohort,
        'timing_qualification': 'fresh_saved_base_inference_and_fixed_currency_projection',
        'production_available_epoch': None,
        'observed_publication': False,
        'observed_execution': False,
    }
    result['forecast_id'] = fingerprint(result)
    return result


def build_frame(cohort, origin, predictor, native, observations, raw_frame, saved_projection,
                qualified_direct, market, projection_contract, solver, contract):
    """Recompute then issue one exact common-target native projection frame."""
    began = time.monotonic()
    target = cohort['target']
    horizon = (target - origin) // 60
    if origin not in cohort['policy_origins'] or horizon != raw_frame['horizon_minutes']:
        raise ValueError('projection_native_registered_frame_required')
    current = sorted((x for x in observations if x['origin_epoch'] == origin), key=lambda x: x['instrument'])
    if [x['instrument'] for x in current] != contract['universe']:
        raise ValueError('projection_native_all68_observations_required')
    values = predictor.predict(horizon, current, origin)
    raw_frame = _fresh_raw_frame(origin, horizon, raw_frame, values, projection_contract)
    fresh_projection = project_frame(raw_frame, projection_contract, solver)
    _compare_projection(fresh_projection, saved_projection)
    phase_seconds = time.monotonic() - began
    if phase_seconds > contract['resources']['fresh_base_projection_seconds']:
        raise ValueError('projection_native_fresh_base_projection_slot_exceeded:' + str(phase_seconds))
    direct = _by(qualified_direct['predictions'], lambda x: (x['diagnostic_forecast_id'], x['base_method']),
                 'projection_native_duplicate_qualified_direct')
    points = [x for x in market['rows'] if x['price_epoch'] == origin]
    references = _by(points, lambda x: x['instrument'], 'projection_native_duplicate_reference')
    if set(references) != set(contract['universe']):
        raise ValueError('projection_native_all_reference_slots_required')
    coverage, predictions, packets = [], [], []
    native_contract = {**contract['parent_surface_contract'], 'common_target_epoch': target,
                       'policy_origins': cohort['policy_origins']}
    by_prediction = _by(fresh_projection['predictions'], lambda x: (x['instrument'], x['base_method'], x['variant']),
                        'projection_native_duplicate_projection_prediction')
    saved_by_prediction = _by(saved_projection['predictions'], lambda x: (x['instrument'], x['base_method'], x['variant']),
                              'projection_native_duplicate_saved_projection_prediction')
    for row in fresh_projection['coverage']:
        instrument, method = row['instrument'], row['method']
        base, variant = method.split('__', 1)
        reason = row['reason']
        if reason == 'eligible' and references[instrument]['status'] != 'valid_candle_close_pair':
            reason = 'reference_' + references[instrument]['status']
        entry = {**row, 'base_method': base, 'variant': variant, 'cohort': cohort['name'], 'reason': reason,
                 'source_reason': row['reason']}
        coverage.append(entry)
        if reason != 'eligible':
            continue
        projected = by_prediction[instrument, base, variant]
        parent = direct.get((projected['source_forecast_id'], base))
        observation = next(x for x in current if x['instrument'] == instrument)
        if parent is None:
            raise ValueError('projection_native_missing_qualified_direct_parent')
        ready = predictor.available_epoch if variant == 'direct' else origin
        pred = _native_prediction(projected, saved_by_prediction[instrument, base, variant], parent, observation, origin, cohort['name'], ready)
        predictions.append(pred)
        packets.append(native.prepare(pred, observation, references[instrument], market['metadata'][instrument], native_contract))
    native.verify()
    elapsed = time.monotonic() - began
    if elapsed > contract['resources']['native_frame_seconds']:
        raise ValueError('projection_native_complete_slot_exceeded:' + str(elapsed))
    if len(coverage) != len(contract['universe']) * len(BASES) * len(VARIANTS):
        raise ValueError('projection_native_full_coverage_required')
    return ({'cohort': cohort['name'], 'origin_epoch': origin, 'target_epoch': target, 'horizon_minutes': horizon,
             'coverage': coverage, 'predictions': predictions, 'packets': packets,
             'fresh_projection_sha256': fingerprint(fresh_projection),
             'saved_projection_sha256': fingerprint(saved_projection),
             'observed_publication': False, 'observed_execution': False},
            {'cohort': cohort['name'], 'origin_epoch': origin,
             'fresh_base_projection_seconds': phase_seconds,
             'complete_native_preparation_seconds': elapsed,
             'base_model_fits': 0, 'scientific_parameters_changed': False})
