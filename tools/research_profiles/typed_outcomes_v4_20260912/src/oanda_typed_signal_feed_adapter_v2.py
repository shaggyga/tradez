"""Pure intake for a separate typed cohort. No legacy writer/reader dispatch.

This is an envelope adapter, not a replacement SQLite service. A deployment
must supply a separately reviewed nullable sink; legacy publish/recent/ledger
and consensus entry points are intentionally refused by the boundary below.
"""
from __future__ import annotations
from collections.abc import Mapping
import re
try:
    from oanda_signal_probability_semantics_v2 import CONTRACT,COHORT,digest,finite,json_copy,validate_point,refuse_legacy_consumer,original_generation_clock
except ModuleNotFoundError:
    from .oanda_signal_probability_semantics_v2 import CONTRACT,COHORT,digest,finite,json_copy,validate_point,refuse_legacy_consumer,original_generation_clock

def normalize_forecast_v2(raw,*,source,policy_account_eligible=False):
    if not isinstance(raw,Mapping) or raw.get('signal_semantics_contract')!=CONTRACT or raw.get('cohort_id')!=COHORT:
        raise ValueError('explicit typed v2 producer envelope required')
    if type(policy_account_eligible) is not bool:raise ValueError('literal policy flag required')
    # A policy flag cannot provide the missing probability target/calibration.
    if not isinstance(source,str) or not source:raise ValueError('source required')
    model_id=raw.get('model_id');instrument=raw.get('instrument');timeframe=raw.get('input_timeframe')
    if not isinstance(model_id,str) or not model_id:raise ValueError('model identity required')
    if not isinstance(instrument,str) or re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',instrument) is None:raise ValueError('pair required')
    if not isinstance(timeframe,str) or re.fullmatch(r'(?:S\d+|M\d+|H\d+|D\d+|MULTI)',timeframe) is None:raise ValueError('timeframe required')
    generated,_=original_generation_clock(raw)
    curve=raw.get('forecast_curve')
    if not isinstance(curve,Mapping) or not curve:raise ValueError('typed forecast curve required')
    points={}
    for key,value in curve.items():
        point=validate_point(value);expected=str(point['horizon_sec'])
        if not isinstance(key,str) or key!=expected:raise ValueError('canonical exact horizon key required')
        if point['source_kind']=='side_classifier_pair':
            metadata=raw.get('producer_metadata')
            if not isinstance(metadata,Mapping):raise ValueError('source producer metadata required')
            if any(metadata.get(outer)!=point['source_input'][inner] for outer,inner in (
                ('artifact_sha256','artifact_sha256'),('source_report_sha256','report_sha256'),('source_target','target'))):
                raise ValueError('point source identity disagrees with producer envelope')
        points[expected]=point
    points={key:points[key] for key in sorted(points,key=int)}
    if raw.get('account_eligible') is not False or raw.get('research_only') is not True:
        raise ValueError('current typed source cohort cannot request account qualification')
    for field in ('probability_up','signal_confidence','calibrated_probability_up','calibrated_brier','calibration_n'):
        if raw.get(field) is not None:raise ValueError('typed side cohort has no top-level midpoint probability')
    anchor_horizon=min((int(key) for key in points),key=lambda value:(abs(value-300),value))
    anchor=points[str(anchor_horizon)]
    for field in ('predicted_signed_pips','predicted_magnitude_pips','projected_net_pips'):
        if raw.get(field) is not None:finite(raw[field],field)
    for field in ('direction','predicted_signed_pips','predicted_magnitude_pips','projected_net_pips'):
        if raw.get(field) is not None and raw[field]!=anchor.get(field):
            raise ValueError('top-level '+field+' disagrees with typed anchor')
    # Bind probability meaning and content. Retain the original producer ID as
    # provenance, never reuse an old candidate identity for a changed transform.
    identity={'contract':CONTRACT,'cohort':COHORT,'source':source,'model':model_id,'pair':instrument,
        'timeframe':timeframe,'original_generated_epoch':generated,'points':points,
        'original_producer_envelope':json_copy(dict(raw))}
    value=json_copy(dict(raw));value.update(id='typed-v2-'+digest(identity),
        feed_dedupe_key='typed-v2-latest-'+digest({'contract':CONTRACT,'cohort':COHORT,
            'source':source,'model':model_id,'pair':instrument,'timeframe':timeframe,
            'target_transform_scopes':[(int(h),p.get('probability_target'),p.get('score_transform')) for h,p in points.items()]}),
        original_producer_id=raw.get('id'),signal_semantics_contract=CONTRACT,cohort_id=COHORT,
        source=source,forecast_curve=points,probability_up=None,signal_confidence=None,
        calibrated_probability_up=None,calibrated_brier=None,calibration_n=None,
        account_eligible=False,research_only=True,
        direction=anchor['direction'],direction_rule=anchor['direction_rule'],
        signal_reference_horizon_sec=anchor_horizon,
        predicted_signed_pips=anchor['predicted_signed_pips'],
        predicted_magnitude_pips=anchor['predicted_magnitude_pips'],
        projected_net_pips=anchor['projected_net_pips'],
        requested_policy_account_eligible=policy_account_eligible,
        entry_qualification='unsupported_midpoint_probability_target_or_calibration_contract',
        legacy_consumer_compatible=False)
    return value

def to_legacy_feed(value):
    return refuse_legacy_consumer(value)

def to_legacy_outcome_ledger(value):
    return refuse_legacy_consumer(value)

def to_probability_consensus(value):
    return refuse_legacy_consumer(value)

def entry_qualification(value):
    # Fail closed for every currently supported source, regardless of an old
    # registered policy, promoted cell or caller-supplied truthy flag.
    if not isinstance(value,Mapping) or value.get('signal_semantics_contract')!=CONTRACT:
        raise ValueError('typed v2 envelope required')
    return {'qualified':False,'reason':'unsupported_midpoint_probability_target_or_calibration_contract'}
