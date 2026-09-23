"""Temporary-source audit checks for partial-family v2 captures; no broker."""
import base64
from copy import deepcopy
import csv
from datetime import datetime,timezone
import hashlib
import math
from types import SimpleNamespace
import pytest
import oanda_causal_forecast_inputs_pair_v2 as inputs

START=int(datetime(2026,9,4,10,tzinfo=timezone.utc).timestamp())
STATE='probabilistic_state_space';RIDGE='ridge_return_repaired'
def archive(root,count=480,*,pair='EUR_USD',step=1,mutate=None):
    path=root/f'{pair}_M1.csv';last=None
    with path.open('w',encoding='utf-8',newline='') as handle:
        writer=csv.writer(handle);writer.writerow(['datetime','instrument','granularity','complete','close'])
        for i in range(count):
            last=START+i*step*60
            row=[datetime.fromtimestamp(last,timezone.utc).isoformat(),pair,'M1','true',1.1+.0001*(i*.01+math.sin(i/7.))]
            if mutate:row=mutate(i,row)
            if row is not None:writer.writerow(row)
    return path,last+61
def capture(root,clock):return inputs.capture_inputs(root,'EUR_USD',pip_size=.0001,clock=lambda:clock)
def reseal(cap):
    cap.pop('source_capture_sha256',None);return inputs._seal(cap)
def compute(cap,**kwargs):
    now=cap['first_observed_epoch']+1
    return inputs.compute_predictions(cap,clock=lambda:now,**kwargs)

def test_partial_family_capture_and_selected_compute_are_valid(tmp_path):
    _,clock=archive(tmp_path,25);cap=capture(tmp_path,clock)
    assert cap['status']=='ready' and cap['readiness_status']=='partial'
    assert cap['available_families']==[STATE]
    inputs.validate_capture(cap,instrument='EUR_USD',pip_size=.0001)
    all_result=compute(cap)
    assert all_result['status']=='partial' and set(all_result['predictions'])=={STATE}
    assert all_result['computation_started_epoch']>=cap['first_observed_epoch']
    assert all_result['computed_epoch']>=all_result['computation_started_epoch']
    assert compute(cap,families=(STATE,))['status']=='ready'
    ridge=compute(cap,families=(RIDGE,))
    assert ridge['status']=='abstain' and ridge['predictions']=={} and 'computed_epoch' in ridge

def test_valid_small_capture_exposes_independent_reasons(tmp_path):
    _,clock=archive(tmp_path,4);cap=capture(tmp_path,clock)
    assert cap['status']=='ready' and cap['readiness_status']=='abstain'
    inputs.validate_capture(cap)
    result=compute(cap)
    assert result['status']=='abstain' and result['predictions']=={} and result['reasons']
    assert all(not x['ready'] for x in cap['family_readiness'].values())

def test_capture_uses_gaps_and_actual_available_bytes(tmp_path):
    _,clock=archive(tmp_path,480,mutate=lambda i,row:None if i%7==2 else row)
    cap=capture(tmp_path,clock);result=compute(cap)
    assert result['status']=='ready'
    expected_times={START+i*60 for i in range(480) if i%7!=2}
    assert {r[0] for r in cap['series']['EUR_USD']}==expected_times
    assert cap['first_observed_epoch']==clock
    assert cap['max_bar_close_epoch']<=clock
    for prediction in result['predictions'].values():
        assert prediction['diagnostics']['training_label_maturity_max_epoch'] is None or prediction['diagnostics']['training_label_maturity_max_epoch']<=clock

def test_cached_capture_keeps_observation_and_uses_immutable_old_bytes(tmp_path):
    path,clock=archive(tmp_path);cap=capture(tmp_path,clock);before=compute(cap)
    path.write_text('changed after capture\n',encoding='utf-8')
    later=inputs.compute_predictions(cap,clock=lambda:clock+60)
    assert later['predictions']==before['predictions']
    assert cap['first_observed_epoch']==clock
    assert later['computation_started_epoch']==clock+60 and later['computed_epoch']==clock+60

@pytest.mark.parametrize('field',['family_readiness','series','reference_start_epoch','available_families','features_available_epoch','readiness_status'])
def test_resealed_derived_metadata_tamper_rejected(tmp_path,field):
    _,clock=archive(tmp_path);cap=capture(tmp_path,clock)
    if field=='family_readiness':cap[field][STATE]['ready']=False
    elif field=='series':cap[field]['EUR_USD'][0][1]*=1.01
    elif field=='available_families':cap[field]=[STATE]
    elif field=='readiness_status':cap[field]='partial'
    else:cap[field]+=60
    reseal(cap)
    with pytest.raises(ValueError,match='derived_input_binding'):inputs.validate_capture(cap)

@pytest.mark.parametrize('field',['tail_sha256','tail_base64','tail_byte_offset','file_size_bytes'])
def test_source_bytes_and_geometry_tamper_rejected(tmp_path,field):
    _,clock=archive(tmp_path);cap=capture(tmp_path,clock);source=cap['sources']['EUR_USD']
    if field=='tail_sha256':source[field]='0'*64
    elif field=='tail_base64':source[field]=base64.b64encode(b'changed\n').decode()
    else:source[field]+=1
    reseal(cap)
    with pytest.raises(ValueError):inputs.validate_capture(cap)

@pytest.mark.parametrize('kind',['naive','nanosecond','wrong_pair','wrong_granularity','incomplete','nonfinite','duplicate','disordered'])
def test_exact_parser_rejects_bad_source(tmp_path,kind):
    def change(i,row):
        if i!=10:return row
        if kind=='naive':row[0]=row[0].replace('+00:00','')
        if kind=='nanosecond':row[0]=row[0].replace('+00:00','.000000001+00:00')
        if kind=='wrong_pair':row[1]='GBP_USD'
        if kind=='wrong_granularity':row[2]='M5'
        if kind=='incomplete':row[3]='false'
        if kind=='nonfinite':row[4]='nan'
        if kind in ('duplicate','disordered'):row[0]=datetime.fromtimestamp(START+(9 if kind=='duplicate' else 8)*60,timezone.utc).isoformat()
        return row
    _,clock=archive(tmp_path,mutate=change)
    assert capture(tmp_path,clock)['status']=='abstain'

def test_integral_timezone_offsets_keep_same_epoch(tmp_path):
    from datetime import timedelta
    _,clock=archive(tmp_path,mutate=lambda i,r:[datetime.fromisoformat(r[0]).astimezone(timezone(timedelta(hours=-4))).isoformat(),*r[1:]])
    cap=capture(tmp_path,clock)
    assert cap['status']=='ready' and cap['series']['EUR_USD'][0][0]==START

def test_independent_reader_retains4096_rows_not_frozen1024(tmp_path):
    _,clock=archive(tmp_path,4200);cap=capture(tmp_path,clock)
    assert cap['status']=='ready' and cap['retained_real_rows_by_pair']=={'EUR_USD':4096}
    assert cap['series']['EUR_USD'][0][0]==START+(4200-4096)*60

@pytest.mark.parametrize('clock_kind',['before_complete','stale','regression','invalid'])
def test_capture_clocks_fail_closed(tmp_path,clock_kind):
    _,now=archive(tmp_path,25)
    if clock_kind=='before_complete':clock=lambda:now-2
    elif clock_kind=='stale':clock=lambda:now+901
    elif clock_kind=='invalid':clock=lambda:float('nan')
    else:
        sequence=iter([now,now-1,now]);clock=lambda:next(sequence)
    cap=inputs.capture_inputs(tmp_path,'EUR_USD',pip_size=.0001,clock=clock)
    assert cap['status']=='abstain'

@pytest.mark.parametrize('times',[(0,1),(10,9)])
def test_actual_computation_clocks_cannot_predate_capture_or_regress(tmp_path,times):
    _,now=archive(tmp_path,25);cap=capture(tmp_path,now)
    sequence=iter([now-1,now]) if times==(0,1) else iter([now+10,now+9])
    result=inputs.compute_predictions(cap,clock=lambda:next(sequence))
    assert result['status']=='abstain' and result['predictions']=={}

def test_flat_model_output_is_neutral_side(tmp_path):
    _,now=archive(tmp_path,480,mutate=lambda i,r:[*r[:4],1.1]);result=compute(capture(tmp_path,now))
    assert result['status']=='ready'
    assert all(p['side']==0 and p['probability_up']==.5 for p in result['predictions'].values())

def test_source_handle_replacement_is_rejected(tmp_path,monkeypatch):
    _,now=archive(tmp_path);real=inputs.os.fstat
    def changed(fd):
        stat=real(fd)
        return SimpleNamespace(st_dev=stat.st_dev,st_ino=stat.st_ino+1,st_size=stat.st_size,st_mtime_ns=stat.st_mtime_ns)
    monkeypatch.setattr(inputs.os,'fstat',changed)
    cap=capture(tmp_path,now)
    assert cap['status']=='abstain' and 'source_changed_during_read' in cap['reasons'][0]

def test_unfinished_append_and_oversized_header_fail_closed(tmp_path):
    path,now=archive(tmp_path)
    with path.open('ab') as handle:handle.write(b'partial row')
    assert capture(tmp_path,now)['status']=='abstain'
    path.write_bytes(b'x'*8193+b'\n')
    assert capture(tmp_path,now)['status']=='abstain'

def test_model_and_dependency_tampering_not_accepted(tmp_path,monkeypatch):
    _,now=archive(tmp_path);cap=capture(tmp_path,now)
    monkeypatch.setattr(inputs,'NUMERICAL_SOURCE_SHA256','0'*64)
    with pytest.raises(ValueError):inputs.validate_capture(cap)

@pytest.mark.parametrize('start_age,end_age,expected_status',[(899,900,'ready'),(900,900,'ready'),(901,902,'abstain'),(899,901,'abstain')])
def test_cached_input_deadline_uses_actual_start_and_completion(tmp_path,start_age,end_age,expected_status):
    _,now=archive(tmp_path,25);cap=capture(tmp_path,now);close=cap['max_bar_close_epoch']
    sequence=iter([close+start_age,close+end_age])
    result=inputs.compute_predictions(cap,families=(STATE,),clock=lambda:next(sequence))
    assert result['status']==expected_status
    assert result['computation_started_epoch']==close+start_age
    if start_age<=900:assert result['computed_epoch']==close+end_age
    if expected_status=='abstain':assert result['predictions']=={}

def test_bounds_revalidation_rejects_more_than4096_resealed_rows(tmp_path):
    path,now=archive(tmp_path,4200);cap=capture(tmp_path,now)
    raw=path.read_bytes();header,tail=raw.split(b'\n',1);header+=b'\n';source=cap['sources']['EUR_USD']
    source.update(file_size_bytes=len(raw),tail_byte_offset=len(header),tail_byte_length=len(tail),
        header_base64=base64.b64encode(header).decode(),tail_base64=base64.b64encode(tail).decode(),
        header_sha256=hashlib.sha256(header).hexdigest(),tail_sha256=hashlib.sha256(tail).hexdigest(),
        captured_bytes_sha256=hashlib.sha256(raw).hexdigest())
    reseal(cap)
    with pytest.raises(ValueError,match='captured_source_exceeds_bounds'):inputs.validate_capture(cap)

def test_source_directory_escape_is_rejected_before_open(tmp_path,monkeypatch):
    from pathlib import Path
    _,now=archive(tmp_path,25);real=Path.resolve
    def redirect(path,*args,**kwargs):
        if path.name=='EUR_USD_M1.csv':return tmp_path.parent/'outside.csv'
        return real(path,*args,**kwargs)
    monkeypatch.setattr(Path,'resolve',redirect)
    cap=capture(tmp_path,now)
    assert cap['status']=='abstain' and 'source_path_outside_candle_root' in cap['reasons'][0]
