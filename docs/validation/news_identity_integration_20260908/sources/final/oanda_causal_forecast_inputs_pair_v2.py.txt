"""Bounded observed-now v2 pair capture; immutable bytes and actual clocks.

Only reads the selected local CSV. No account, broker, worker, ledger, forecast
publication, training cache or runtime writer is imported or invoked.
"""
from __future__ import annotations
import base64
import csv
import hashlib
import math
import os
from pathlib import Path
import time
from types import ModuleType
from oanda_causal_forecast_inputs import _clock,_digest,dependency_versions
from oanda_causal_forecast_inputs_gap_v2 import _parse_exact_source

SCHEMA='causal_forecast_observed_m1_inputs_pair_v2_20260907'
FAMILIES=('ridge_return_repaired','probabilistic_state_space')
NUMERICAL_MODULE='oanda_pair_local_models_v2'
NUMERICAL_SOURCE_SHA256='294c63bf0ed873395c4850a8722a2622eaa2bc563ceb7ecf9886764c63d8bf24'
MAX_SOURCE_ROWS=4096
MAX_TAIL_BYTES=4*1024*1024
MAX_COMMON_BAR_AGE_SEC=900
AVAILABILITY='conservative_consumer_observation_after_stable_read'
TRAINING_POLICY='Exact real UTC timestamps and +3600 H1 endpoints; explicit missingness; no price imputation or session-gap bridging.'

def _hash(raw):return hashlib.sha256(raw).hexdigest()
def _signature(stat):return stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns
def _numerical_module():
    path=Path(__file__).with_name(NUMERICAL_MODULE+'.py');raw=path.read_bytes()
    if _hash(raw)!=NUMERICAL_SOURCE_SHA256:raise ValueError('numerical_source_binding_changed')
    module=ModuleType(NUMERICAL_MODULE);module.__file__=str(path)
    exec(compile(raw,str(path),'exec'),module.__dict__)
    return module

def _read_tail(path):
    """Independent bounded reader; never changes the frozen v1 reader."""
    with path.open('rb') as handle:
        before=path.stat();header=handle.readline(8193)
        if not header.endswith(b'\n') or len(header)>8192:raise ValueError('invalid_or_oversized_header')
        handle.seek(0,2);size=handle.tell();offset=max(len(header),size-MAX_TAIL_BYTES)
        handle.seek(offset);raw=handle.read(MAX_TAIL_BYTES)
        if offset>len(header):
            split=raw.find(b'\n')
            if split<0:raise ValueError('oversized_csv_row')
            offset+=split+1;raw=raw[split+1:]
        after=path.stat()
        if _signature(before)!=_signature(after) or _signature(after)!=_signature(os.fstat(handle.fileno())):
            raise ValueError('source_changed_during_read')
    if not raw or not raw.endswith(b'\n'):raise ValueError('empty_or_partial_csv_tail')
    lines=raw.splitlines(keepends=True)
    if len(lines)>MAX_SOURCE_ROWS:
        offset+=sum(map(len,lines[:-MAX_SOURCE_ROWS]));raw=b''.join(lines[-MAX_SOURCE_ROWS:])
    return {'path':str(path.resolve()),'file_size_bytes':size,'tail_byte_offset':offset,
        'tail_byte_length':len(raw),'header_base64':base64.b64encode(header).decode('ascii'),
        'tail_base64':base64.b64encode(raw).decode('ascii'),'header_sha256':_hash(header),
        'tail_sha256':_hash(raw),'captured_bytes_sha256':_hash(header+raw)}

def _source_rows(source,observed,instrument):
    if len(source['tail_base64'])>4*((MAX_TAIL_BYTES+2)//3) or len(source['header_base64'])>10924:
        raise ValueError('captured_source_exceeds_bounds')
    header=base64.b64decode(source['header_base64'],validate=True);tail=base64.b64decode(source['tail_base64'],validate=True)
    if not 0<len(header)<=8192 or not 0<len(tail)<=MAX_TAIL_BYTES:raise ValueError('captured_source_exceeds_bounds')
    if not header.endswith(b'\n') or not tail.endswith(b'\n'):raise ValueError('partial_captured_source')
    if any(type(source.get(k)) is not int for k in ('file_size_bytes','tail_byte_offset','tail_byte_length')):
        raise ValueError('invalid_captured_byte_geometry')
    if (source['tail_byte_length']!=len(tail) or source['tail_byte_offset']<len(header)
        or source['tail_byte_offset']+len(tail)!=source['file_size_bytes']):raise ValueError('invalid_captured_byte_geometry')
    rows=_parse_exact_source(source,instrument,observed)
    if not 1<=len(rows)<=MAX_SOURCE_ROWS:raise ValueError('captured_source_exceeds_bounds')
    return {int(t):p for t,p in rows.items()}

def _derived(rows,observed,instrument,pip_size,module):
    reference=max(rows)
    if not 0<=observed-reference-60<=MAX_COMMON_BAR_AGE_SEC:raise ValueError('stale_or_future_pair_reference')
    readiness=module.family_readiness(rows,reference,instrument=instrument,pip_size=pip_size)
    count=sum(r['ready'] for r in readiness.values())
    return {'series':{instrument:[[t,p] for t,p in rows.items()]},'retained_real_rows_by_pair':{instrument:len(rows)},
        'reference_start_epoch':reference,'max_bar_close_epoch':reference+60,
        'common_bar_age_sec':observed-reference-60,'training_label_maturity_max_epoch':reference+60,
        'training_labels_available_max_epoch':observed,'feature_cutoff_epoch':reference+60,
        'features_available_epoch':observed,'family_readiness':readiness,
        'available_families':[f for f in FAMILIES if readiness[f]['ready']],
        'readiness_status':'ready' if count==len(FAMILIES) else 'partial' if count else 'abstain'}

def _seal(value):value['source_capture_sha256']=_digest(value);return value

def capture_inputs(candle_root,instrument,*,pip_size,clock=time.time):
    module=_numerical_module();instrument,pip_size=module.validate_identity(instrument,pip_size)
    result={'schema_version':SCHEMA,'status':'abstain','readiness_status':'abstain','reasons':[],
        'pairs':[instrument],'output_instrument':instrument,'pip_size':pip_size,'sources':{},'series':{},
        'model_source_sha256':NUMERICAL_SOURCE_SHA256,'dependency_versions':dependency_versions(),
        'availability_semantics':AVAILABILITY,'training_policy':TRAINING_POLICY,
        'family_readiness':{},'available_families':[],'research_only':True,
        'account_eligible':False,'can_place_orders':False,'can_promote':False}
    try:
        begun=_clock(clock);root=Path(candle_root).resolve();path=root/f'{instrument}_M1.csv'
        if path.resolve().parent!=root:raise ValueError('source_path_outside_candle_root')
        source=_read_tail(path);first=_clock(clock);source['first_observed_epoch']=first
        result['sources'][instrument]=source
        if first<begun:raise ValueError('observation_clock_moved_backwards')
        observed=_clock(clock);result['first_observed_epoch']=observed
        if observed<first:raise ValueError('observation_clock_moved_backwards')
        rows=_source_rows(source,first,instrument)
        result.update(_derived(rows,observed,instrument,pip_size,module));result['status']='ready'
    except (OSError,ValueError,KeyError,UnicodeError,csv.Error) as exc:
        result['reasons'].append(f"{instrument}:{type(exc).__name__}:{'source_unreadable' if isinstance(exc,OSError) else str(exc)}")
    return _seal(result)

def _verified_rows(capture,*,instrument=None,pip_size=None):
    if capture.get('schema_version')!=SCHEMA or capture.get('status')!='ready':raise ValueError('input_not_ready')
    if _digest({k:v for k,v in capture.items() if k!='source_capture_sha256'})!=capture.get('source_capture_sha256'):
        raise ValueError('input_capture_hash_mismatch')
    if capture.get('model_source_sha256')!=NUMERICAL_SOURCE_SHA256 or capture.get('dependency_versions')!=dependency_versions():
        raise ValueError('model_or_dependency_binding_changed')
    module=_numerical_module();pair,pip=module.validate_identity(capture.get('output_instrument'),capture.get('pip_size'))
    if instrument is not None and pair!=instrument:raise ValueError('registered_instrument_binding_mismatch')
    if pip_size is not None and module.validate_identity(pair,pip_size)[1]!=pip:raise ValueError('registered_pip_size_binding_mismatch')
    if capture.get('pairs')!=[pair] or set(capture.get('sources',{}))!={pair}:raise ValueError('fixed_pair_input_universe_required')
    if capture.get('availability_semantics')!=AVAILABILITY or capture.get('training_policy')!=TRAINING_POLICY or capture.get('reasons')!=[]:
        raise ValueError('fixed_availability_and_policy_required')
    if capture.get('research_only') is not True or any(capture.get(k) is not False for k in ('account_eligible','can_place_orders','can_promote')):
        raise ValueError('inert_capture_required')
    observed=_clock(lambda:capture['first_observed_epoch']);source=capture['sources'][pair];first=_clock(lambda:source['first_observed_epoch'])
    if first>observed:raise ValueError('source_observed_after_capture')
    rows=_source_rows(source,first,pair)
    for name,value in _derived(rows,observed,pair,pip,module).items():
        if capture.get(name)!=value:raise ValueError('derived_input_binding_mismatch:'+name)
    return rows,module

def validate_capture(capture,*,instrument=None,pip_size=None):
    _verified_rows(capture,instrument=instrument,pip_size=pip_size)

def compute_predictions(capture,*,families=None,clock=time.time):
    output={'status':'abstain','reasons':[],'predictions':{},'family_readiness':{},
        'requested_families':[],'available_families':[],
        'model_source_sha256':NUMERICAL_SOURCE_SHA256,'source_capture_sha256':capture.get('source_capture_sha256'),
        'output_instrument':capture.get('output_instrument'),'pip_size':capture.get('pip_size'),
        'dependency_versions':dependency_versions()}
    try:
        started=_clock(clock)
        rows,module=_verified_rows(capture)
        if started<capture['first_observed_epoch']:raise ValueError('computation_started_before_capture_observation')
        output['computation_started_epoch']=started
        if not 0<=started-capture['max_bar_close_epoch']<=MAX_COMMON_BAR_AGE_SEC:
            raise ValueError('stale_or_future_pair_reference_at_computation_start')
        selected=module._selected(families);output['requested_families']=list(selected)
        predictions,readiness=module.predict_with_readiness(rows,capture['reference_start_epoch'],
            instrument=capture['output_instrument'],pip_size=capture['pip_size'],families=selected)
        output['family_readiness']=readiness
        for family in selected:
            if family not in predictions:
                output['reasons'].extend(family+':'+reason for reason in readiness[family]['reasons']);continue
            expected,probability,diagnostics=predictions[family]
            if type(expected) not in (int,float) or not math.isfinite(expected) or type(probability) not in (int,float) or not .25<=probability<=.75:
                raise ValueError('invalid_family_prediction:'+family)
            output['predictions'][family]={'expected_signed_pips':expected,'probability_up':probability,
                'side':1 if expected>0 else -1 if expected<0 else 0,'diagnostics':diagnostics}
        completed=_clock(clock)
        output['computed_epoch']=completed
        if completed<started:raise ValueError('computation_clock_moved_backwards')
        if not 0<=completed-capture['max_bar_close_epoch']<=MAX_COMMON_BAR_AGE_SEC:
            raise ValueError('expired_pair_reference_at_computation_completion')
        count=len(output['predictions'])
        output.update(computed_epoch=completed,available_families=list(output['predictions']),
            status='ready' if count==len(selected) else 'partial' if count else 'abstain')
    except Exception as exc:
        output['status']='abstain';output['predictions']={};output['available_families']=[]
        output['reasons'].append(type(exc).__name__+':'+str(exc))
    return output
