"""Typed research point admission shared by intake and direct registration.

Current source kinds supply no supported midpoint probability. This boundary
does not create a new midpoint target or allow a generic qualification flag.
"""
import json
from oanda_signal_probability_semantics_v2 import CONTRACT,COHORT,validate_point,original_generation_clock,finite
from oanda_typed_signal_feed_adapter_v2 import normalize_forecast_v2

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def validate_envelope(candidate):
    # Validate without substituting the adapter's generated ID for the original
    # source ID. Additional receipt clocks remain original caller provenance.
    normalize_forecast_v2(candidate,source='outcome_probability_v4_admission')
    generated,_=original_generation_clock(candidate)
    if candidate.get('forecast_generated_epoch') is not None and finite(candidate['forecast_generated_epoch'],'forecast_generated_epoch')!=generated:
        raise ValueError('forecast_origin_fields_disagree')
    if not isinstance(candidate.get('id'),str) or not candidate['id'] or len(candidate['id'])>256:
        raise ValueError('original_forecast_identity_required')
    if candidate['id'].startswith('model-gap-side-v2-'):
        raise ValueError('raw_model_gap_origin_id_requires_typed_source_wrapper')
    if not isinstance(candidate.get('family'),str) or not candidate['family']:
        raise ValueError('forecast_family_required')
    if len(candidate['forecast_curve'])>64:raise ValueError('bounded_forecast_curve_required')
    return candidate

def point_fields(point):
    value=validate_point(point)
    if value['probability_up'] is not None:
        raise ValueError('unsupported_nonnull_midpoint_probability')
    direction=value['direction']
    if direction not in ('buy','sell','flat','hold','unavailable'):
        raise ValueError('unsupported_typed_direction')
    signed=value['predicted_signed_pips'];magnitude=value['predicted_magnitude_pips']
    return {'direction':direction,'probability_up':None,
        'predicted_signed_pips':signed,'predicted_magnitude_pips':magnitude,
        'prediction_output_kind':'classification_only' if signed is None and magnitude is None else 'declared_magnitude_outputs',
        'probability_semantics':canonical(value.get('probability_target')),
        'probability_calibration_status':'unavailable_no_supported_midpoint_probability',
        'signal_semantics_contract':CONTRACT,'source_point_json':canonical(value)}

def verify_stored_point(row):
    expected=point_fields(json.loads(row['source_point_json']))
    for key,value in expected.items():
        if row.get(key)!=value:raise ValueError('stored_typed_point_identity_mismatch:'+key)
    source=json.loads(row['source_point_json'])
    if source['horizon_sec']!=row['horizon_sec'] or source['cohort_id']!=COHORT:
        raise ValueError('stored_typed_point_scope_mismatch')
    return expected
