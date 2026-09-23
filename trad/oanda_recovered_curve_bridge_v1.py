"""Verify retained-model computation, then prepare a provenance-preserving curve.

This pure bridge replays inference at the recorded computation clocks. It never
issues, publishes, fits, fetches data or converts an old capture into a new one.
The registered caller verifies raw source bytes and the module source closure.
"""
from __future__ import annotations

from copy import deepcopy

import oanda_recovered_second_curve_v1 as recovered
from oanda_forecast_curve_contract_v1 import CurveContractError, prepare_curve


def prepare_recovered_computation(capture, result, model, *, source_bindings,
                                  forecast_cohort, policy):
    if not isinstance(result, dict) or result.get('status') != 'computed_not_issued':
        raise CurveContractError('recovered_computation_required')
    try:
        clocks = iter((result['computation_started_epoch'], result['computed_epoch']))
        replayed = recovered.predict_curve(capture, model, clock=lambda: next(clocks))
    except (ValueError, KeyError, TypeError, StopIteration):
        raise CurveContractError('recovered_computation_replay_failed') from None
    if replayed != result:
        raise CurveContractError('recovered_computation_content_mismatch')
    if not isinstance(source_bindings, dict) or any(source_bindings.get(name) != digest
            for name, digest in result['original_source_bindings'].items()):
        raise CurveContractError('original_source_closure_missing')
    training = result['target_selection_policy']
    if (training['minimum_delay_sec'] != 0 or training['maximum_delay_sec'] != 7
            or training['selector'] != 'first_real_bar_label_at_or_after_nominal_searchsorted_left'
            or training['target_price_epoch_offset_from_selected_label_sec'] != 5):
        raise CurveContractError('unsupported_recovered_target_policy')
    if policy['native_horizons_sec'] != list(recovered.HORIZONS):
        raise CurveContractError('recovered_native_inventory_policy_mismatch')
    context = {key: deepcopy(result[key]) for key in (
        'sampling_policy', 'sampling_metadata', 'price_convention',
        'historical_ingestion_equivalence_proven', 'feature_names', 'features_sha256',
        'training_timeframe', 'computed_input_timeframe', 'original_live_input_timeframe',
        'first_observed_epoch', 'reference_available_epoch', 'source_sha256',
        'input_after_model_fit')}
    context.update(original_target_selection_policy=deepcopy(training),
        native_feature_version=result['feature_version'],
        numerical_verification='full_retained_model_recomputation_at_recorded_clocks',
        raw_capture_binding_scope='registered_caller_verifies_source_bytes_and_mapping',
        original_feature_time_labels_scope='record_count_formulas_with_actual_elapsed_support_retained')
    points = []
    for point in result['points']:
        if point['training_target_max_delay_sec'] != 7 or point['actual_future_target_epoch'] is not None:
            raise CurveContractError('recovered_future_target_not_unknown')
        points.append(deepcopy(point))
    return prepare_curve(
        instrument=result['instrument'], pip_size=result['pip_size'], forecast_cohort=forecast_cohort,
        model_sha256=result['model_sha256'], feature_version=result['feature_version'],
        source_bindings=source_bindings, input_capture_sha256=result['capture_sha256'],
        input_available_epoch=max(result['input_available_epoch'], result['first_observed_epoch']),
        reference_epoch=result['reference_epoch'], reference_label_epoch=result['reference_label_epoch'],
        reference_price=result['reference_price'], reference_price_kind=result['price_convention']+'_S5_close',
        bar_duration_sec=5, model_fitted_epoch=result['model_fitted_epoch'],
        computation_started_epoch=result['computation_started_epoch'], computed_epoch=result['computed_epoch'],
        points=points, policy=policy, computation_sha256=result['result_sha256'], scope=result['scope'],
        target_selection_policy=dict(kind='first_complete_bar_at_or_after_nominal', maximum_delay_sec=7),
        input_context=context)
