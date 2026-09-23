"""Typed research signals. Source estimates never manufacture midpoint P(up).

No fit, storage, account, network or promotion interface. Current source targets
are deliberately unqualified. This adapter does not establish historical truth.
"""
from __future__ import annotations
import copy
import hashlib
import json
import math
from collections.abc import Mapping
from datetime import datetime,timezone
from pathlib import Path
from types import ModuleType

CONTRACT='typed_signal_probability_v2'
COHORT='typed_signal_probability_v2_research_20260912'
SIDE_SCORE_TRANSFORM='sigmoid_logit_long_minus_logit_short_clip_1e6_logit30_v1'
POSITIVE_HELPER_SHA256='28d5365ba4d52635e42c32ae83bc3332a2a3e04cde6da8ee20145b45b17df191'
TARGETS={
    'target_profitable':{'event':'specified_side_terminal_net_pips_strictly_positive',
        'side_relation':'separate_marginal_estimates_not_assumed_complements',
        'zero_rule':'zero_net_pips_is_class_zero'},
    'target_best_side':{'event':'specified_side_has_higher_terminal_net_pips_with_long_tie',
        'side_relation':'source_labels_complementary_but_fitted_marginals_not_forced_complementary',
        'zero_rule':'equal_long_short_net_pips_assigns_long_class_one'},
}
_positive=None

def finite(value,field,*,optional=False):
    if value is None and optional:return None
    if isinstance(value,bool) or not isinstance(value,(int,float)):
        raise ValueError('finite numeric '+field+' required')
    try:number=float(value)
    except (OverflowError,ValueError):raise ValueError('finite numeric '+field+' required') from None
    if not math.isfinite(number):raise ValueError('finite numeric '+field+' required')
    return number

def original_generation_clock(snapshot):
    """Validate supplied clocks only. No host-now fallback or inferred offset."""
    if not isinstance(snapshot,Mapping):raise ValueError('snapshot object required')
    epoch=finite(snapshot.get('generated_epoch'),'generated_epoch')
    text=snapshot.get('generated_utc')
    if epoch<=0 or not isinstance(text,str) or not text.strip():raise ValueError('explicit original generation clocks required')
    try:parsed=datetime.fromisoformat(text.replace('Z','+00:00'))
    except ValueError:raise ValueError('invalid generated_utc') from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:raise ValueError('aware generated_utc required')
    parsed=parsed.astimezone(timezone.utc)
    if abs(parsed.timestamp()-epoch)>0.001:raise ValueError('generation clocks disagree beyond1ms precision')
    return epoch,parsed

def count(value,field):
    if value is None:return None
    number=finite(value,field)
    if number<0 or number!=int(number):raise ValueError('nonnegative integer '+field+' required')
    return int(number)

def probability(value,field):
    value=finite(value,field)
    if not 0<=value<=1:raise ValueError(field+' outside probability range')
    return value

def json_copy(value):
    # Preserve source evidence exactly within ordinary JSON; no default=str.
    return json.loads(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False))

def canonical_json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def horizon(value):
    number=finite(value,'horizon_sec')
    if number!=int(number) or not 0<number<=7*86400:raise ValueError('invalid exact horizon')
    return int(number)

def positive_probability(model,frame,expected_classes=None):
    """Reuse exact reviewed ensemble helper bytes; no new class-mapping code."""
    global _positive
    if _positive is None:
        path=Path(__file__).resolve().parent/'dependencies'/'oanda_ensemble_probability_contract_v2.py'
        with path.open('rb') as handle:raw=handle.read(1024*1024+1)
        if len(raw)>1024*1024 or hashlib.sha256(raw).hexdigest()!=POSITIVE_HELPER_SHA256:
            raise ValueError('positive probability dependency identity mismatch')
        module=ModuleType('_typed_signal_positive_probability_pinned_v2')
        module.__file__=str(path)
        exec(compile(raw,str(path),'exec'),module.__dict__)
        _positive=module.positive_probability
    return _positive(model,frame,expected_classes)

def class_order(model):
    values=getattr(model,'classes_',None)
    values=values.tolist() if hasattr(values,'tolist') else values
    if not isinstance(values,list) or len(values)!=2 or any(type(v) is not int for v in values) or set(values)!={0,1}:
        raise ValueError('explicit binary integer class order required')
    return list(values)

def typed_probability_batch(estimator,calibrator,frame):
    """Bind exposed mapping to the exact source probability call, without fitting."""
    estimator_order=class_order(estimator)
    raw=positive_probability(estimator,frame,expected_classes=estimator_order)
    if class_order(estimator)!=estimator_order:raise ValueError('estimator class order changed during inference')
    calibrator_order=None
    if calibrator is not None:
        calibrator_order=class_order(calibrator)
        raw=positive_probability(calibrator,raw.reshape(-1,1),expected_classes=calibrator_order)
        if class_order(calibrator)!=calibrator_order:raise ValueError('calibrator class order changed during inference')
    return raw,{'estimator_classes':estimator_order,'calibrator_classes':calibrator_order,
        'positive_class':1,'helper_sha256':POSITIVE_HELPER_SHA256,'historical_mapping_verified':False}

def relative_side_logit_score(long_value,short_value):
    """Preserved legacy ranking transform, explicitly not a probability event."""
    left=min(1-1e-6,max(1e-6,probability(long_value,'long')))
    right=min(1-1e-6,max(1e-6,probability(short_value,'short')))
    difference=max(-30.,min(30.,math.log(left/(1-left))-math.log(right/(1-right))))
    return 1/(1+math.exp(-difference))

def _base(horizon_sec,source_kind,source_input):
    return {'signal_semantics_contract':CONTRACT,'cohort_id':COHORT,'horizon_sec':horizon(horizon_sec),
        'source_kind':source_kind,'source_input':json_copy(source_input),
        'probability_up':None,'calibrated_probability_up':None,'signal_confidence':None,
        'calibrated_brier':None,'calibration_n':None,'predicted_signed_pips':None,
        'predicted_magnitude_pips':None,'projected_net_pips':None,
        'account_eligible':False,'research_only':True,
        'entry_qualification':'unsupported_midpoint_probability_target_or_calibration_contract'}

def side_classifier_point(*,horizon_sec,target,long_probability,short_probability,metrics,
                          artifact_sha256,report_sha256,calibrator_applied,class_mapping):
    if not isinstance(target,str) or target not in TARGETS:raise ValueError('unsupported artifact target')
    if not isinstance(metrics,Mapping):raise ValueError('source metrics object required')
    if type(calibrator_applied) is not bool:raise ValueError('literal calibrator_applied required')
    if not isinstance(class_mapping,Mapping):raise ValueError('source class mapping required')
    expected_mapping={'positive_class':1,'helper_sha256':POSITIVE_HELPER_SHA256,'historical_mapping_verified':False}
    for key in ('estimator_classes','calibrator_classes'):
        order=class_mapping.get(key)
        if key=='calibrator_classes' and not calibrator_applied:
            if order is not None:raise ValueError('unexpected calibrator mapping')
        elif not isinstance(order,list) or len(order)!=2 or any(type(v) is not int for v in order) or set(order)!={0,1}:
            raise ValueError('invalid source class mapping')
        expected_mapping[key]=order
    if canonical_json(dict(class_mapping))!=canonical_json(expected_mapping):raise ValueError('source class mapping identity mismatch')
    for value in (artifact_sha256,report_sha256):
        if not isinstance(value,str) or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('exact source SHA256 required')
    left=probability(long_probability,'long');right=probability(short_probability,'short')
    brier=finite(metrics.get('brier'),'source brier',optional=True)
    if brier is not None and not 0<=brier<=1:raise ValueError('source brier outside binary range')
    rows=count(metrics.get('side_rows'),'side_rows');events=count(metrics.get('events'),'events')
    if rows is not None and events is not None and events>rows:raise ValueError('events exceed side rows')
    if brier is not None and (rows is None or rows==0):raise ValueError('source Brier requires its positive side-row denominator')
    if metrics.get('target') not in (None,target):raise ValueError('reported metric target disagrees with artifact')
    source={'horizon_sec':horizon(horizon_sec),'target':target,'long_probability':left,'short_probability':right,
        'metrics':json_copy(dict(metrics)),'artifact_sha256':artifact_sha256,'report_sha256':report_sha256,
        'calibrator_applied':calibrator_applied,'class_mapping':json_copy(class_mapping)}
    point=_base(horizon_sec,'side_classifier_pair',source)
    point.update(direction='buy' if left>right else 'sell' if left<right else 'flat',
        direction_rule='higher_estimated_specified_side_class_one_probability',
        probability_status='unavailable_midpoint_target_not_estimated',
        probability_target={'source_target':target,'positive_class':1,**TARGETS[target]},
        side_probabilities={'long':left,'short':right},
        relative_side_logit_score=relative_side_logit_score(left,right),
        score_transform=SIDE_SCORE_TRANSFORM,
        source_calibration={'applied':calibrator_applied,'target':target,
            'status':'applied_source_target_calibrator_not_midpoint_calibration' if calibrator_applied else 'source_estimator_uncalibrated',
            'training_rows':None,'historical_label_availability_verified':False},
        side_validation={'source_reported_brier':brier,'brier_rows':rows,'distinct_events':events,
            'split':'source_reported_holdout','target_association':'artifact_declared_target_report_does_not_independently_declare_target',
            'artifact_declared_target':target,'metric_unit':'mean_squared_error_on_side_class_one',
            'report_sha256':report_sha256,'midpoint_calibration_evidence':False})
    return point

def legacy_directional_point(raw,*,default_direction=''):
    """Explicit legacy intake; never silently upgrades unknown target metadata."""
    if not isinstance(raw,Mapping):raise ValueError('legacy point object required')
    source=json_copy(dict(raw));h=horizon(source.get('horizon_sec'))
    numeric_fields=('probability_up','calibrated_probability_up','raw_probability_up','signal_confidence','confidence',
        'predicted_signed_pips','expected_signed_move_pips','predicted_magnitude_pips','projected_net_pips',
        'quantile_low_pips','quantile_high_pips','calibrated_brier','calibration_n')
    values={key:finite(source.get(key),key,optional=True) for key in numeric_fields}
    for key in ('probability_up','calibrated_probability_up','raw_probability_up'):
        if values[key] is not None:probability(values[key],key)
    if values['predicted_magnitude_pips'] is not None and values['predicted_magnitude_pips']<0:
        raise ValueError('negative magnitude')
    signed=values['predicted_signed_pips'];alias=values['expected_signed_move_pips']
    if signed is not None and alias is not None and signed!=alias:raise ValueError('signed aliases disagree')
    signed=signed if signed is not None else alias
    declared=source.get('direction')
    if declared is not None and not isinstance(declared,str):raise ValueError('invalid declared direction')
    explicit=declared or default_direction
    if not isinstance(explicit,str) or explicit.lower() not in ('','buy','sell','flat','hold','unavailable'):
        raise ValueError('invalid declared direction')
    explicit=explicit.lower()
    direction=explicit or ('buy' if signed>0 else 'sell' if signed<0 else 'flat') if signed is not None else (explicit or 'unavailable')
    point=_base(h,'legacy_directional_signal',{'raw':source,'default_direction':default_direction})
    point.update(direction=direction,direction_rule='source_declared_direction' if explicit else
        'signed_move_sign_not_probability' if signed is not None else 'unavailable',
        probability_status='unavailable_legacy_target_and_calibration_unverified',
        probability_target={'source_target':'legacy_unverified'},
        source_probability_reported_as_up=values['probability_up'],
        source_confidence=values['signal_confidence'] if values['signal_confidence'] is not None else values['confidence'],
        predicted_signed_pips=signed,predicted_magnitude_pips=values['predicted_magnitude_pips'],
        projected_net_pips=values['projected_net_pips'],
        legacy_reported_validation={'calibrated_brier':values['calibrated_brier'],'calibration_n':values['calibration_n'],
            'status':'unverified_source_labels_not_midpoint_calibration'})
    return point

def validate_point(point):
    if not isinstance(point,Mapping) or point.get('signal_semantics_contract')!=CONTRACT or point.get('cohort_id')!=COHORT:
        raise ValueError('typed v2 point required; legacy normalization refused')
    source=point.get('source_input')
    if not isinstance(source,Mapping):raise ValueError('source input required')
    kind=point.get('source_kind')
    if kind=='side_classifier_pair':expected=side_classifier_point(**source)
    elif kind=='legacy_directional_signal':expected=legacy_directional_point(**source)
    else:raise ValueError('unsupported typed point kind')
    if canonical_json(point)!=canonical_json(expected):raise ValueError('typed point derived fields disagree with source input')
    return expected

def require_midpoint_probability(point):
    validate_point(point)
    raise ValueError('current v2 source targets do not supply calibrated midpoint probability')

def refuse_legacy_consumer(value):
    if isinstance(value,Mapping) and value.get('signal_semantics_contract')==CONTRACT:
        raise ValueError('typed v2 signal requires reviewed nullable consumer; legacy transport refused')
    raise ValueError('unsupported signal contract')
