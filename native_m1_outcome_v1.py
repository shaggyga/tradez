"""Exact completed-M1 source capture and native scoring primitives.

Actual acquisition is local CSV only. Source reads are retained; no numerical
model, account, broker, price substitution or outcome-database I/O occurs here.
The new ledger owns durable admission and its source-bound clock provider.
"""
from datetime import datetime
import base64
import csv
import hashlib
import json
import math
import os
from pathlib import Path, PureWindowsPath
import stat
import time

import oanda_causal_forecast_inputs_pair_v2 as prices
import joint_native_anchor_v1 as native

SCHEMA = 'native_joint_exact_m1_source_capture_v1_20260913'
SCORE_SCHEMA = 'native_joint_exact_m1_pip_score_v1_20260913'
SCORE_RECIPE = 'original_binary64_target_minus_origin_divided_by_pip_v1'
MAX_DOCUMENT = 8 * 1024 * 1024
MAX_STATE = 128 * 1024
BOUND = {
    'oanda_causal_forecast_inputs.py': 'f12dbce89a36356c63d6d633c652ad3c31cd2d09828d367c7512108b1e4a7281',
    'oanda_causal_forecast_inputs_gap_v2.py': '539b3eed7fe4df48b4034b41bc82101c6276d88e3bb7e06e9931d3ce02234edd',
    'oanda_causal_forecast_inputs_pair_v2.py': '8e58491a49fca967e6d8d2a797cc77f142053576c92a724a2e7336d855fb4899',
    'joint_native_anchor_v1.py': '21649812cb168265ac6fa25254418e47ea3db690dd23e2a0e3ac87e957879f42'}
need = native.need


def encode(value, maximum=MAX_DOCUMENT):
    out = bytearray()
    for text in json.JSONEncoder(sort_keys=True,separators=(',',':'),allow_nan=False).iterencode(value):
        part = text.encode('ascii'); need(len(out)+len(part)<=maximum,'native_document_byte_bound');out.extend(part)
    return bytes(out)


def digest(value): return hashlib.sha256(encode(value)).hexdigest()


def plain_path(value, *, missing=False):
    text = os.fspath(value); p = PureWindowsPath(text)
    need(p.is_absolute() and p.drive.lower()=='c:' and '..' not in p.parts and
         all(':' not in part for part in p.parts[1:]),'native_plain_absolute_c_path_required')
    path = Path(text)
    for member in (*reversed(path.parents),path):
        try: info = member.lstat()
        except FileNotFoundError:
            need(missing,'native_source_missing'); break
        need(not stat.S_ISLNK(info.st_mode) and
             not getattr(info,'st_file_attributes',0)&stat.FILE_ATTRIBUTE_REPARSE_POINT,
             'native_reparse_path_refused')
    return path


def signature(info): return [info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns]


def source_bindings():
    root=plain_path(prices.__file__).parent; values={}
    for name,expected in BOUND.items():
        path=plain_path(native.__file__ if name=='joint_native_anchor_v1.py' else root/name)
        before=path.stat();need(before.st_size<=2*1024*1024,'native_source_code_bound')
        raw=path.read_bytes();need(signature(before)==signature(path.stat()),'native_code_changed_during_read')
        actual=hashlib.sha256(raw).hexdigest();need(actual==expected,'native_original_source_binding_changed')
        values[name]=actual
    path=plain_path(__file__);before=path.stat();raw=path.read_bytes()
    need(len(raw)<=2*1024*1024 and signature(before)==signature(path.stat()),'native_outcome_source_changed')
    values[path.name]=hashlib.sha256(raw).hexdigest()
    return values


def _clock(clock): return native.clock(clock(),'native_observation')


def _pair(value):
    need(type(value) is str and native.re.fullmatch('[A-Z]{3}_[A-Z]{3}',value)
         and value[:3]!=value[4:],'native_distinct_pair_required')
    return value


def _row_evidence(source, rows):
    """Raw lexemes remain separate from their original parsed binary64 values."""
    header=base64.b64decode(source['header_base64'],validate=True)
    tail=base64.b64decode(source['tail_base64'],validate=True)
    fields=next(csv.reader([header.decode('utf-8-sig').strip()]))
    clock_field='datetime' if 'datetime' in fields else 'time'
    evidence={}
    for line in tail.splitlines(keepends=True):
        cells=next(csv.reader([line.decode('utf-8')],strict=True)); row=dict(zip(fields,cells,strict=True))
        epoch=int(datetime.fromisoformat(row[clock_field].strip().replace('Z','+00:00')).timestamp())
        need(epoch in rows and float(row['close']).hex()==rows[epoch].hex(),'native_row_mapping_changed')
        evidence[str(epoch)]={'close_lexeme':row['close'],'close_hex':rows[epoch].hex(),
            'row_sha256':hashlib.sha256(line).hexdigest(),
            'complete_flag':'explicit_true' if 'complete' in row else 'absent_clock_completion_only'}
    need(len(evidence)==len(rows),'native_complete_row_accounting')
    return evidence


def capture_source(path, instrument, *, clock=time.time):
    """Read one exact pair tail, even when current prediction readiness is absent.

    This callable performs the actual read. The ledger calls it directly, never
    accepts a caller-authored equivalent dictionary as an observed admission.
    Malformed captured source bytes remain in the refused result when available.
    """
    pair=_pair(instrument); bindings=source_bindings(); path=plain_path(path,missing=True)
    need(path.name==pair+'_M1.csv','native_fixed_pair_filename_required')
    began=_clock(clock); source=None; before=None; read=None
    try:
        path=plain_path(path);before=signature(path.stat())
        source=prices._read_tail(path);read=_clock(clock)
        need(began<=read,'native_capture_clock_regression')
        need(before==signature(plain_path(path).stat()),'native_source_identity_changed')
        rows=prices._source_rows(source,read,pair)
        evidence=_row_evidence(source,rows)
        completed=_clock(clock);need(read<=completed,'native_validation_clock_regression')
        need(before==signature(plain_path(path).stat()),'native_source_changed_during_validation')
        need(source_bindings()==bindings,'native_source_closure_changed')
        body={'schema_version':SCHEMA,'status':'captured','instrument':pair,'source':source,
            'source_signature':before,'read_started_epoch':began,'read_completed_epoch':read,
            'validated_epoch':completed,'source_bindings':bindings,'row_evidence':evidence,
            'first_bar_start_epoch':min(rows),'last_bar_start_epoch':max(rows),'row_count':len(rows),
            'price_convention':'original_source_close_parsed_binary64',
            'provider_query_completeness_claimed':False,'original_availability_reconstructed':False,
            'current_prediction_readiness_required':False,'can_place_orders':False}
    except (OSError,ValueError,KeyError,UnicodeError,csv.Error,OverflowError) as exc:
        completed=_clock(clock)
        need(began<=completed and (read is None or read<=completed),'native_failed_capture_clock_regression')
        body={'schema_version':SCHEMA,'status':'refused','instrument':pair,'source':source,
            'source_signature':before,'read_started_epoch':began,'read_completed_epoch':read,
            'validated_epoch':completed,'source_bindings':bindings,'reason_code':
            'source_unreadable' if isinstance(exc,OSError) else type(exc).__name__+':'+str(exc)[:256],
            'current_prediction_readiness_required':False,'can_place_orders':False}
    encode(body);return {**body,'capture_sha256':digest(body)}


def verify_capture(capture):
    """Reconstruct the stored source values only; does not authenticate live I/O."""
    need(type(capture) is dict and capture.get('schema_version')==SCHEMA,'native_capture_schema')
    encode(capture)
    need(capture.get('capture_sha256')==digest({k:v for k,v in capture.items() if k!='capture_sha256'}),'native_capture_digest')
    need(capture.get('source_bindings')==source_bindings(),'native_capture_source_generation')
    need(capture.get('status')=='captured','native_capture_not_valid')
    need(set(capture)=={'schema_version','status','instrument','source','source_signature','read_started_epoch',
        'read_completed_epoch','validated_epoch','source_bindings','row_evidence','first_bar_start_epoch',
        'last_bar_start_epoch','row_count','price_convention','provider_query_completeness_claimed',
        'original_availability_reconstructed','current_prediction_readiness_required','can_place_orders',
        'capture_sha256'},'native_exact_capture_shape')
    pair=_pair(capture['instrument']);started=native.clock(capture['read_started_epoch'],'read_started')
    read=native.clock(capture['read_completed_epoch'],'read_completed');validated=native.clock(capture['validated_epoch'],'validated')
    need(started<=read<=validated,'native_capture_clock_order')
    sig=capture['source_signature']
    need(type(sig) is list and len(sig)==4 and all(type(x) is int and x>=0 for x in sig),
         'native_exact_source_identity_required')
    source=capture['source'];need(type(source) is dict and
        source.get('file_size_bytes')==sig[2],'native_source_geometry_identity')
    path=PureWindowsPath(source['path'])
    need(path.is_absolute() and path.drive.lower()=='c:' and '..' not in path.parts and
         all(':' not in part for part in path.parts[1:]) and path.name==pair+'_M1.csv',
         'native_retained_source_path_contract')
    rows=prices._source_rows(capture['source'],read,pair)
    evidence=_row_evidence(capture['source'],rows)
    expected={'row_evidence':evidence,'first_bar_start_epoch':min(rows),'last_bar_start_epoch':max(rows),'row_count':len(rows),
        'price_convention':'original_source_close_parsed_binary64','provider_query_completeness_claimed':False,
        'original_availability_reconstructed':False,'current_prediction_readiness_required':False,'can_place_orders':False}
    need(all(encode(capture.get(key))==encode(value) for key,value in expected.items()),'native_capture_derived_identity')
    return rows


def anchor_value(document):
    """Exact value reconstruction, not source/capture/publication authentication."""
    need(type(document) is dict,'native_anchor_document_required')
    def f(key):
        text=document[key];need(type(text) is str and len(text)<=40,'native_canonical_float_hex_required')
        value=float.fromhex(text);need(value.hex()==text,'native_canonical_float_hex_required');return value
    value=native.make_native_anchor(instrument=document['instrument'],pip_size=f('pip_size_hex'),
        origin_bar_start_epoch=document['origin_bar_start_epoch'],origin_mid=f('origin_mid_hex'),
        expected_signed_pips=f('expected_signed_pips_hex'),probability_up=f('native_probability_up_hex'),
        source_anchor_complete=document['source_anchor_complete'],source_observed_epoch=document['source_observed_epoch'],
        feature_decision_epoch=document['feature_decision_epoch'],computation_started_epoch=document['computation_started_epoch'],
        computation_completed_epoch=document['computation_completed_epoch'],issued_epoch=document['issued_epoch'],
        emitted_side=document['native_expected_delta_side'])
    need(encode(value.as_dict())==encode(document),'native_anchor_exact_value_mismatch')
    return value


def select_exact_target(anchor, capture):
    value=anchor_value(anchor);body=value.as_dict();rows=verify_capture(capture)
    return _select_from_verified(body,capture,rows)


def _select_from_verified(body,capture,rows):
    """Owner-local batch projection after one complete capture verification."""
    need(capture['instrument']==body['instrument'],'native_target_pair_mismatch')
    target=body['target_bar_start_epoch']
    if capture['read_completed_epoch']<body['target_close_epoch']:
        return {'status':'pending','reason_code':'target_not_mature_at_source_read'}
    if target not in rows:
        reason='source_domain_too_new' if min(rows)>target else 'target_not_in_observed_tail'
        return {'status':'unknown','reason_code':reason}
    evidence=capture['row_evidence'][str(target)]
    origin=body['origin_bar_start_epoch']
    return {'status':'exact_target','target_bar_start_epoch':target,'target_close_epoch':target+60,
        'target_mid_hex':rows[target].hex(),'target_row_evidence':evidence,
        'source_capture_sha256':capture['capture_sha256'],'source_read_completed_epoch':capture['read_completed_epoch'],
        'source_validated_epoch':capture['validated_epoch'],'original_origin_preserved':True,
        'later_origin_present':origin in rows,'later_origin_equals_original':
            None if origin not in rows else rows[origin].hex()==body['origin_mid_hex']}


def score_native(anchor, target_mid):
    body=anchor_value(anchor).as_dict();target=native.real(target_mid,'target_mid',positive=True)
    origin=float.fromhex(body['origin_mid_hex']);pip=float.fromhex(body['pip_size_hex'])
    expected=float.fromhex(body['expected_signed_pips_hex']);p=float.fromhex(body['native_probability_up_hex'])
    move=target-origin;actual=move/pip;error=expected-actual
    endpoint=float.fromhex(body['expected_endpoint_mid_hex']);endpoint_error=endpoint-target
    squared=error*error
    need(all(math.isfinite(x) for x in (move,actual,error,endpoint_error,squared)),'native_score_nonfinite')
    actual_side=native.direction(move);predicted_side=body['native_expected_delta_side'];up=int(target>origin)
    return {'schema_version':SCORE_SCHEMA,'score_recipe':SCORE_RECIPE,
        'native_anchor_sha256':digest(body),'original_origin_mid_hex':body['origin_mid_hex'],
        'target_mid_hex':target.hex(),'original_expected_pips_hex':body['expected_signed_pips_hex'],
        'original_probability_up_hex':body['native_probability_up_hex'],
        'actual_signed_pips_hex':actual.hex(),'signed_error_pips_hex':error.hex(),
        'absolute_error_pips':abs(error),'squared_error_pips':squared,'zero_baseline_absolute_error_pips':abs(actual),
        'actual_direction':actual_side,'predicted_direction':predicted_side,'strict_up_label':up,
        'direction_denominator_eligible':bool(actual_side and predicted_side),
        'direction_correct':bool(actual_side==predicted_side) if actual_side and predicted_side else None,
        'brier':(p-up)**2,'coinflip_brier':.25,
        'endpoint_representation_error_hex':endpoint_error.hex(),
        'endpoint_error_is_not_training_target_error':True,'probability_is_calibrated':False,
        'executable_result':None,'account_return':None,'can_place_orders':False}
