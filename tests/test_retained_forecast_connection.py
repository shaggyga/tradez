import copy
import json
from pathlib import Path
import sys

import numpy as np
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_retained_forecast_connection_v1 as m


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
    assert len(reads)==9


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
