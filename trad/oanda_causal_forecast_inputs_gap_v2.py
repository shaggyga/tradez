"""Observed-now source capture with exact-time training across valid segments.

The original source adapter and registration remain intact. Missing minutes
are neither filled nor compressed into shorter elapsed-time labels.
"""
from __future__ import annotations
import base64
import csv
import hashlib
from types import ModuleType
import math
import re
from pathlib import Path
import time
from oanda_causal_forecast_inputs import (
    PAIRS,FAMILIES,_read_tail,_parse_source,_clock,_digest,dependency_versions,
)

SCHEMA='causal_forecast_observed_m1_inputs_gap_v2_20260907'
NUMERICAL_SOURCE_SHA256='0ed224abcd0e7b53a26921acc29af208f074701fe02ce5cf4e0f1ee6f84c7e09'
NUMERICAL_MODULE='oanda_gap_aware_four_family_models'
MIN_CURRENT_COMMON_BARS=61
MAX_COMMON_BAR_AGE_SEC=900

def _parse_exact_source(source,pair,observed):
    # datetime.fromisoformat in the shared, frozen parser truncates digits
    # beyond microseconds. Inspect original timestamp text before that parser
    # so a nonzero nanosecond cannot silently become an exact M1 boundary.
    # Integral timezone offsets remain supported by the shared epoch parser.
    header=base64.b64decode(source['header_base64'],validate=True)
    tail=base64.b64decode(source['tail_base64'],validate=True)
    if (hashlib.sha256(header).hexdigest()!=source['header_sha256'] or
        hashlib.sha256(tail).hexdigest()!=source['tail_sha256'] or
        hashlib.sha256(header+tail).hexdigest()!=source['captured_bytes_sha256']):
        raise ValueError('captured_source_hash_mismatch')
    fields=next(csv.reader([header.decode('utf-8-sig').strip()]))
    time_columns=[index for index,name in enumerate(fields) if name in ('datetime','time')]
    for line in tail.decode('utf-8').splitlines():
        cells=next(csv.reader([line],strict=True))
        if len(cells)!=len(fields): raise ValueError('invalid_csv_row_width')
        for index in time_columns:
            if any(any(digit!='0' for digit in fraction)
                   for fraction in re.findall(r'[.,]([0-9]+)',cells[index])):
                raise ValueError('fractional_bar_start_not_minute_aligned')
    return _parse_source(source,pair,observed)


def _window(rows,observed):
    common=set.intersection(*(set(rows[p]) for p in PAIRS))
    if not common: raise ValueError('no_common_reference')
    reference=max(common)
    if not 0<=observed-reference-60<=MAX_COMMON_BAR_AGE_SEC:
        raise ValueError('stale_or_future_common_reference')
    consecutive=0
    while reference-consecutive*60 in common:
        consecutive+=1
    if consecutive<MIN_CURRENT_COMMON_BARS:
        raise ValueError(f'current_common_warmup:{consecutive}<{MIN_CURRENT_COMMON_BARS}')
    # Every historical sample keeps its real timestamp. Retain pre-weekend
    # rows from the bounded source tails, rather than a calendar-time suffix.
    trimmed={p:{t:price for t,price in rows[p].items() if t<=reference} for p in PAIRS}
    return reference,trimmed,consecutive

def _derived(rows,observed):
    reference,trimmed,consecutive=_window(rows,observed)
    return {'series':{p:[[t,v] for t,v in sorted(trimmed[p].items())] for p in PAIRS},
        'current_common_start_epochs':[reference-i*60 for i in reversed(range(MIN_CURRENT_COMMON_BARS))],
        'current_common_bars':consecutive,'required_current_common_bars':MIN_CURRENT_COMMON_BARS,
        'retained_real_rows_by_pair':{p:len(trimmed[p]) for p in PAIRS},
        'reference_start_epoch':reference,'max_bar_close_epoch':reference+60,
        'common_bar_age_sec':observed-reference-60,
        'training_label_maturity_max_epoch':reference+60,
        'training_labels_available_max_epoch':observed,
        'feature_cutoff_epoch':reference+60,'features_available_epoch':observed}

def _seal(result):
    result['source_capture_sha256']=_digest(result)
    return result

def capture_inputs(candle_root,*,clock=time.time):
    result={'schema_version':SCHEMA,'status':'abstain','reasons':[],
        'pairs':list(PAIRS),'output_instrument':'EUR_USD','sources':{},'series':{},
        'model_source_sha256':NUMERICAL_SOURCE_SHA256,'dependency_versions':dependency_versions(),
        'availability_semantics':'conservative_consumer_observation_after_stable_read',
        'training_policy':'Actual timestamp-addressed windows and exact H1 labels from valid retained segments; no imputation.',
        'research_only':True,'account_eligible':False,'can_place_orders':False}
    try:
        begun=_clock(clock);previous=begun
        for pair in PAIRS:
            try:
                source=_read_tail(Path(candle_root)/f'{pair}_M1.csv')
                source['first_observed_epoch']=_clock(clock)
                if source['first_observed_epoch']<previous: raise ValueError('observation_clock_moved_backwards')
                previous=source['first_observed_epoch'];result['sources'][pair]=source
            except (OSError,ValueError) as exc:
                reason=str(exc) if isinstance(exc,ValueError) else 'source_unreadable'
                result['reasons'].append(f'{pair}:{type(exc).__name__}:{reason}')
        observed=_clock(clock);result['first_observed_epoch']=observed
        if observed<previous or observed<begun: raise ValueError('observation_clock_moved_backwards')
        if result['reasons']: return _seal(result)
        rows={p:_parse_exact_source(result['sources'][p],p,result['sources'][p]['first_observed_epoch']) for p in PAIRS}
        result.update(_derived(rows,observed));result['status']='ready'
    except (OSError,ValueError,KeyError,UnicodeError,csv.Error) as exc:
        result['reasons'].append(f'{type(exc).__name__}:{exc}')
    return _seal(result)

def _verified_rows(capture):
    if capture.get('schema_version')!=SCHEMA or capture.get('status')!='ready': raise ValueError('input_not_ready')
    if _digest({k:v for k,v in capture.items() if k!='source_capture_sha256'})!=capture.get('source_capture_sha256'):
        raise ValueError('input_capture_hash_mismatch')
    if capture.get('model_source_sha256')!=NUMERICAL_SOURCE_SHA256 or capture.get('dependency_versions')!=dependency_versions():
        raise ValueError('model_or_dependency_binding_changed')
    if capture.get('pairs')!=list(PAIRS) or set(capture.get('sources',{}))!=set(PAIRS):
        raise ValueError('fixed_input_universe_required')
    if (capture.get('output_instrument')!='EUR_USD' or
        capture.get('availability_semantics')!='conservative_consumer_observation_after_stable_read'):
        raise ValueError('fixed_output_and_availability_required')
    if capture.get('research_only') is not True or any(capture.get(k) is not False for k in ('account_eligible','can_place_orders')):
        raise ValueError('inert_capture_required')
    observed=_clock(lambda:capture['first_observed_epoch']);rows={};previous=None
    for pair in PAIRS:
        source=capture['sources'][pair]
        first=_clock(lambda:source['first_observed_epoch'])
        if first>observed: raise ValueError('source_observed_after_capture')
        if previous is not None and first<previous: raise ValueError('observation_clock_moved_backwards')
        previous=first
        if len(source['tail_base64'])>1400000 or len(source['header_base64'])>11000:
            raise ValueError('captured_source_exceeds_bounds')
        rows[pair]=_parse_exact_source(source,pair,first)
        if len(rows[pair])>1024: raise ValueError('captured_source_exceeds_bounds')
    derived=_derived(rows,observed)
    for name,value in derived.items():
        if capture.get(name)!=value: raise ValueError('derived_input_binding_mismatch:'+name)
    reference=derived['reference_start_epoch']
    return {p:{t:v for t,v in rows[p].items() if t<=reference} for p in PAIRS}

def validate_capture(capture):
    _verified_rows(capture)

def _numerical_module():
    path=Path(__file__).with_name(NUMERICAL_MODULE+'.py')
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=NUMERICAL_SOURCE_SHA256:
        raise ValueError('numerical_source_binding_changed')
    # Execute exactly the verified bytes; cached modules and stale pyc files
    # must not replace the source registered for this study.
    module=ModuleType(NUMERICAL_MODULE)
    module.__file__=str(path)
    exec(compile(raw,str(path),'exec'),module.__dict__)
    return module

def compute_predictions(capture,*,clock=time.time):
    output={'status':'abstain','reasons':[],'predictions':{},
        'model_source_sha256':NUMERICAL_SOURCE_SHA256,
        'source_capture_sha256':capture.get('source_capture_sha256'),
        'dependency_versions':dependency_versions()}
    try:
        rows=_verified_rows(capture)
        predictions=_numerical_module().predict_all(rows,capture['reference_start_epoch'])
        for family in FAMILIES:
            if family not in predictions: raise ValueError('family_prediction_unavailable:'+family)
            expected,probability,diagnostics=predictions[family]
            if not math.isfinite(expected) or not math.isfinite(probability) or not 0<=probability<=1:
                raise ValueError('invalid_family_prediction:'+family)
            output['predictions'][family]={'expected_signed_pips':float(expected),'probability_up':float(probability),
                'side':1 if expected>=0 else -1,'diagnostics':diagnostics}
        completed=_clock(clock)
        if completed<capture['first_observed_epoch']: raise ValueError('computation_clock_moved_backwards')
        output.update(status='ready',computed_epoch=completed)
    except Exception as exc:
        output['predictions']={};output['reasons'].append(f'{type(exc).__name__}:{exc}')
    return output
