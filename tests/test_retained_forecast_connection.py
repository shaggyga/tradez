import copy
import json
from pathlib import Path
import sys
import subprocess

import numpy as np
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_retained_forecast_connection_v1 as m


def test_display_reader_does_not_import_scipy_or_sklearn():
    script="import sys; sys.path.insert(0, 'trad'); import oanda_retained_forecast_connection_v1; assert not any(n=='scipy' or n.startswith('scipy.') or n=='sklearn' or n.startswith('sklearn.') for n in sys.modules)"
    subprocess.run([sys.executable,'-c',script],cwd=Path(__file__).resolve().parents[1],check=True,timeout=20)


def connection():
    c=object.__new__(m.SavedConnection)
    c.registry={'pairs':['EUR_USD'], 'connections':[{'id':'saved','horizon_minutes':5,'selection_scope':'development'}],
                'unconnected_targets':[{'target':'next_daily_close'}],'limits':['not a universal winner']}
    c.registry_sha256='a'*64
    c.predict_entry=lambda e,p,r:np.array([1.25]*len(p))
    return c


def row():
    return {'bar_start_epoch':960,'published_epoch':1021,'captured_epoch':1022,'reference_mid':1.1,
            'feature_hash':'f','receipt_id':'r','input_hash':'i'}


def test_issue_uses_actual_completion_clock_and_target_is_not_shifted():
    ticks=iter([1023,1025,1026]);v=connection().predict({'EUR_USD':row()},{},clock=lambda:next(ticks))
    f=v['forecasts'][0]
    assert (f['reference_epoch'],f['issued_epoch'],f['target_epoch'])==(1020,1025,1320)
    assert f['no_change_control_bps']==0 and f['can_place_orders'] is False


@pytest.mark.parametrize('field,value,reason',[
    ('published_epoch',1019,'input_clock_order'),('captured_epoch',1019,'input_clock_order'),
    ('bar_start_epoch',720,'input_clock_order')])
def test_wrong_input_clocks_are_withheld(field,value,reason):
    r=row();r[field]=value
    v=connection().predict({'EUR_USD':r},{},clock=lambda:1023)
    # An old but internally ordered row is rejected as stale.
    assert not v['forecasts'] and v['coverage'][0]['status'] in (reason,'input_stale')


@pytest.mark.parametrize('completed,reason',[(1201,'input_expired_during_inference'),(1320,'target_matured_during_inference'),(1022,'publication_clock_reversed')])
def test_inference_completion_rechecks_freshness(completed,reason):
    ticks=iter([1023,completed,max(completed,1023)])
    v=connection().predict({'EUR_USD':row()},{},clock=lambda:next(ticks))
    assert not v['forecasts'] and v['coverage'][0]['status']==reason


def test_bad_model_does_not_hide_other_connection():
    c=connection();c.registry['connections'].append({'id':'good','horizon_minutes':15,'selection_scope':'development'})
    def predict(e,p,r):
        if e['id']=='saved':raise ValueError('bad artifact')
        return np.array([.5])
    c.predict_entry=predict
    v=c.predict({'EUR_USD':row()},{},clock=lambda:1023)
    assert [f['connection'] for f in v['forecasts']]==['good']
    assert v['coverage'][0]['status']=='inference_unavailable'


def test_nonfinite_output_is_not_published():
    c=connection();c.predict_entry=lambda *args:np.array([np.nan])
    assert c.predict({'EUR_USD':row()},{},clock=lambda:1023)['forecasts']==[]


def test_missing_pair_remains_explicit():
    v=connection().predict({}, {'pairs':{'EUR_USD':{'status':'not_tradeable'}}},clock=lambda:1023)
    assert v['coverage'][0]['status']=='not_tradeable' and not v['forecasts']


def test_legacy26_refusal_preserves_existing_connection():
    c=connection();c.registry['connections'].append({'id':'legacy','kind':'legacy26_extra_trees',
        'horizon_minutes':240,'selection_scope':'development','fit_metadata':{'ready_epoch':1}})
    r=row();r['legacy26']={'status':'unavailable','reason':'legacy26_session_boundary_unproven'}
    v=c.predict({'EUR_USD':r},{},clock=lambda:1023)
    assert [f['connection'] for f in v['forecasts']]==['saved']
    assert v['coverage'][1]['status']=='legacy26_session_boundary_unproven'


def test_legacy26_uses_own_features_and_history_identity():
    c=connection();c.registry['connections']=[{'id':'legacy','kind':'legacy26_extra_trees',
        'horizon_minutes':240,'selection_scope':'development','fit_metadata':{'ready_epoch':1},'feature_names':['x']}]
    r=row();r['legacy26']={'status':'available','values':{'x':3},'feature_hash':'legacy_features',
        'input_hash':'history','provenance':{'rows':80}}
    def predict(e,p,rs):
        assert rs['EUR_USD']['values']=={'x':3}
        return np.array([2.5])
    c.predict_entry=predict
    f=c.predict({'EUR_USD':r},{},clock=lambda:1023)['forecasts'][0]
    assert f['input_hash']=='history' and f['feature_hash']=='legacy_features'
    assert f['target_epoch']==1020+240*60 and f['legacy26_provenance']=={'rows':80}


def test_legacy26_model_readiness_is_enforced():
    c=connection();c.registry['connections']=[{'id':'legacy','kind':'legacy26_extra_trees','horizon_minutes':240,
        'selection_scope':'development','fit_metadata':{'ready_epoch':2000}}]
    r=row();r['legacy26']={'status':'available','values':{},'feature_hash':'l','input_hash':'h','provenance':{}}
    v=c.predict({'EUR_USD':r},{},clock=lambda:1023)
    assert not v['forecasts'] and v['coverage'][0]['status']=='model_not_ready'


def test_input_refusal_reason_and_generation_are_retained():
    report={'pairs':{'EUR_USD':{'status':'unavailable','features':{'reason':'rolling_publication_generation_mismatch'}}},
            'source_errors':{'rolling_binding':'rolling_publication_generation_mismatch'},'rolling_publication_generation':'generation'}
    v=connection().predict({},report,clock=lambda:1023)
    assert v['coverage'][0]['input_reason']=='rolling_publication_generation_mismatch'
    assert v['input_diagnostics']['source_errors']==report['source_errors']
    assert v['input_diagnostics']['rolling_publication_generation']=='generation'


def test_publication_transition_retry_requires_matching_generation(monkeypatch):
    sequence=iter(['old','new','old','new','new','new'])
    monkeypatch.setattr(m.availability,'read_json',lambda *args:({'publication_generation':next(sequence)},{}))
    monkeypatch.setattr(m.time,'sleep',lambda delay:None)
    envelope,ref,status=m.read_feature_publication(Path('.'))
    assert status['publication_generation']==envelope['publication_generation']=='new'


def test_mixed_publication_never_accepted_after_retry_bound(monkeypatch):
    reads=[]
    def read(path,*args):
        reads.append(path)
        return {'publication_generation':'new' if path.name=='latest_features.json' else 'old'},{}
    monkeypatch.setattr(m.availability,'read_json',read)
    monkeypatch.setattr(m.time,'sleep',lambda delay:None)
    assert m.read_feature_publication(Path('.'))[2] is None
    assert len(reads)==15


def test_publication_transition_after_original_retry_window(monkeypatch):
    sleeps=[];calls=[]
    def read(path,*args):
        calls.append(path)
        attempt=(len(calls)-1)//3
        generation='new' if path.name=='latest_features.json' or attempt>=3 else 'old'
        return {'publication_generation':generation},{}
    monkeypatch.setattr(m.availability,'read_json',read)
    monkeypatch.setattr(m.time,'sleep',sleeps.append)
    envelope,ref,status=m.read_feature_publication(Path('.'))
    assert status['publication_generation']==envelope['publication_generation']=='new'
    assert ref['publication_read']['attempts']==4 and sleeps==[.05,.1,.2]


def test_transient_sharing_failure_restarts_whole_read(monkeypatch):
    calls=[]
    def read(path,*args):
        calls.append(path.name)
        if len(calls)==2:raise PermissionError('sharing violation')
        return {'publication_generation':'old' if len(calls)==1 else 'new'},{}
    monkeypatch.setattr(m.availability,'read_json',read)
    monkeypatch.setattr(m.time,'sleep',lambda delay:None)
    envelope,ref,status=m.read_feature_publication(Path('.'))
    assert calls==['status.json','latest_features.json','status.json','latest_features.json','status.json']
    assert status['publication_generation']==envelope['publication_generation']=='new'
    assert ref['publication_read']['transient_read_errors']==['PermissionError:sharing violation']


def test_persistent_read_failure_is_bounded(monkeypatch):
    sleeps=[];calls=[]
    def read(*args):
        calls.append(1);raise PermissionError('locked')
    monkeypatch.setattr(m.availability,'read_json',read)
    monkeypatch.setattr(m.time,'sleep',sleeps.append)
    with pytest.raises(PermissionError,match='locked'):m.read_feature_publication(Path('.'))
    assert len(calls)==5 and sum(sleeps)==.75


def test_invalid_publication_is_not_retried(monkeypatch):
    calls=[]
    def read(*args):
        calls.append(1);raise ValueError('source_byte_bound')
    monkeypatch.setattr(m.availability,'read_json',read)
    with pytest.raises(ValueError,match='source_byte_bound'):m.read_feature_publication(Path('.'))
    assert len(calls)==1


def test_producer_error_survives_prediction_diagnostics(monkeypatch):
    def read(path,*args):
        return ({'publication_generation':'old'} if path.name=='latest_features.json' else
                {'status':'error','reason':'PermissionError:replace','generated_utc':'2026-10-01T00:00:00Z'}),{}
    monkeypatch.setattr(m.availability,'read_json',read)
    monkeypatch.setattr(m.time,'sleep',lambda delay:None)
    envelope,ref,status=m.read_feature_publication(Path('.'))
    assert status is None
    report={'publication_read':ref['publication_read']}
    value=connection().predict({},report,clock=lambda:1023)
    assert value['input_diagnostics']['publication_read']['statuses'][0]['reason']=='PermissionError:replace'
    assert not value['forecasts']


def test_real_two_file_publication_transition(tmp_path):
    import threading
    import time
    (tmp_path/'status.json').write_text(json.dumps({'publication_generation':'old'}))
    (tmp_path/'latest_features.json').write_text(json.dumps({'publication_generation':'new'}))
    def publish_status():
        time.sleep(.18)
        from oanda_rolling_technical_worker_v2 import atomic_json
        atomic_json(tmp_path/'status.json',{'publication_generation':'new'})
    thread=threading.Thread(target=publish_status)
    thread.start()
    try:
        envelope,ref,status=m.read_feature_publication(tmp_path)
        assert envelope['publication_generation']==status['publication_generation']=='new'
        assert ref['publication_read']['matched']
    finally:thread.join(timeout=5)



def test_selected_input_support_is_visible_without_changing_prediction():
    c=connection();c.registry['connections'][0]['feature_names']=['one','two','three']
    r=row();r['values']={'one':1.,'two':None,'three':float('nan')}
    value=c.predict({'EUR_USD':r},{},clock=lambda:1023)
    f=value['forecasts'][0]
    assert f['input_support']['finite']==1 and f['input_support']['expected']==3
    assert f['expected_return_bps']==1.25


def test_required_feature_name_is_not_silently_imputed():
    c=connection();e={'feature_names':['expected'],'id':'saved','kind':'rich_pipeline'}
    with pytest.raises(ValueError,match='required_feature_keys_missing'):
        m.SavedConnection.predict_entry(c,e,['EUR_USD'],{'EUR_USD':{'values':{'other':1}}})


def test_artifact_hash_and_path_containment(tmp_path):
    p=tmp_path/'model';p.write_bytes(b'original')
    assert m.checked(tmp_path,{'path':'model','sha256':m.sha(b'original')})==b'original'
    with pytest.raises(ValueError,match='artifact_hash'):
        m.checked(tmp_path,{'path':'model','sha256':'0'*64})
    with pytest.raises(ValueError,match='contained_artifact'):
        m.checked(tmp_path,{'path':'../model','sha256':'0'*64})


def publication(tmp_path):
    c=connection();value=c.predict({'EUR_USD':row()},{},clock=lambda:1023)
    reg={**c.registry,'source_bindings':{}}
    registry=tmp_path/'registry.json';registry.write_text(json.dumps(reg))
    digest=m.sha(registry.read_bytes());value['registry_sha256']=digest
    for f in value['forecasts']:f['registry_sha256']=digest
    value['payload_sha256']=m.sha(m.encoded(value))
    output=tmp_path/'current.json';output.write_bytes(m.encoded(value))
    return output,registry,value


def test_consumer_expires_inputs_without_relabelling_horizon(tmp_path):
    output,registry,value=publication(tmp_path)
    assert len(m.read_current(output,registry,now=1024)['forecasts'])==1
    assert not m.read_current(output,registry,now=1201)['forecasts']


@pytest.mark.parametrize('mutation',['hash','target','flag','duplicate','source'])
def test_consumer_rejects_changed_publication(tmp_path,mutation):
    output,registry,v=publication(tmp_path)
    if mutation=='hash':v['forecasts'][0]['expected_return_bps']=8
    if mutation=='target':v['forecasts'][0]['target_epoch']+=60
    if mutation=='flag':v['forecasts'][0]['can_place_orders']=True
    if mutation=='duplicate':v['forecasts']*=2
    if mutation=='source':
        r=json.loads(registry.read_bytes());r['source_bindings']={'missing.py':'0'*64};registry.write_text(json.dumps(r))
    if mutation!='hash':v['payload_sha256']=m.sha(m.encoded({k:x for k,x in v.items() if k!='payload_sha256'}))
    output.write_bytes(m.encoded(v))
    assert m.read_current(output,registry,now=1024)['status']=='unavailable'
