import copy
import hashlib
import json
from pathlib import Path
import shutil
import sys
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'trad'))
from test_retained_projection import setup as projection_setup
import oanda_retained_projection_v1 as projection
import oanda_retained_learned_residual_v1 as layer
import oanda_retained_forecast_tracking_v1 as tracking


def setup(tmp_path):
    reg,forecasts=projection_setup(tmp_path)
    shutil.copy2(ROOT/'stage_c_alignment_integrity_v2/currency_projection_residual_layer_v2.py',tmp_path/'stage_c_alignment_integrity_v2/currency_projection_residual_layer_v2.py')
    fp=projection.load_original(tmp_path).fingerprint
    cfg={'states':{},'source_result_sha256':'a'*64,'cutoff_epoch':1000,'equivalent_connections':{}}
    reg['learned_residual']=cfg
    for parent,binding in reg['projection']['parents'].items():
        e=next(e for e in reg['connections'] if e['id']==parent)
        members=[{'record_id':'synthetic','origin_epoch':1}]
        s={'base_method':e['arm'],'horizon_minutes':e['horizon_minutes'],'cutoff_epoch':1000,'status':'fitted',
           'residual_weight':0. if e['horizon_minutes']==360 else .02,'base_models_refitted':False,'outcomes_revealed':False,
           'contract':layer.CONTRACT,'contract_sha256':fp(layer.CONTRACT),
           'support':{'rows':1,'distinct_origins':8,'distinct_utc_days':3,'distinct_pairs':20,'training_membership':members,'membership_sha256':fp(members)}}
        s['layer_id']=fp(s)
        v={'schema':'retained_learned_residual_state.v1','parent':binding,'source_result_sha256':'a'*64,'snapshot':s,'maximum_training_outcome_available_epoch':999}
        path=tmp_path/(parent+'.json');path.write_text(json.dumps(v),encoding='utf-8')
        cfg['states'][parent]={'path':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
        if s['residual_weight']==0:cfg['equivalent_connections'][parent]=parent+'__currency_projection'
        else:reg['connections'].append({'id':parent+'__learned_residual','kind':layer.KIND,'parent':parent,
             'layer_id':s['layer_id'],'original_model_id':e['original_model_id'],'horizon_minutes':e['horizon_minutes'],
             'models':[],'selection_scope':'synthetic'})
    projection.Projection(tmp_path,reg).append(forecasts,[],'fixture',clock=lambda:1022.)
    return reg,forecasts


def test_actual_apply_no_fit_and_zero_aliases(tmp_path,monkeypatch):
    reg,fs=setup(tmp_path);a=layer.LearnedResidual(tmp_path,reg)
    monkeypatch.setattr(a.original,'fit_snapshot',lambda *a,**k:pytest.fail('fit forbidden'))
    before=copy.deepcopy(fs);slots=[];a.append(fs,slots,'fixture',clock=lambda:1023.)
    assert fs[:len(before)]==before and len(fs)-len(before)==136 and len(slots)==136
    assert len(reg['learned_residual']['equivalent_connections'])==2
    assert all(f['projection']['learned_residual'] and not f['can_place_orders'] for f in fs[len(before):])


@pytest.mark.parametrize('fault',['hash','membership','parent','future_label','contract','scope','alias','missing'])
def test_state_and_inventory_refusals(tmp_path,fault):
    reg,_=setup(tmp_path);parent=next(iter(reg['learned_residual']['states']));rec=reg['learned_residual']['states'][parent];p=tmp_path/rec['path'];v=json.loads(p.read_bytes())
    if fault=='missing':del reg['learned_residual']['states'][parent]
    elif fault=='alias':reg['learned_residual']['equivalent_connections'].clear()
    else:
        if fault=='hash':v['snapshot']['residual_weight']=.5
        elif fault=='membership':v['snapshot']['support']['training_membership'].append({})
        elif fault=='parent':v['parent']['original_model_id']='wrong'
        elif fault=='future_label':v['maximum_training_outcome_available_epoch']=1001
        elif fault=='contract':v['snapshot']['contract']['minimum_distinct_pairs']=1
        elif fault=='scope':v['snapshot']['horizon_minutes']=5
        p.write_text(json.dumps(v),encoding='utf-8')
        if fault!='hash':rec['sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ValueError):layer.LearnedResidual(tmp_path,reg)


@pytest.mark.parametrize('fault',['future','stale','duplicate','mixed_origin','parent_hash','nan','orders','snapshot_future'])
def test_bad_controls_preserve_original_and_unaffected_scope(tmp_path,fault):
    reg,fs=setup(tmp_path);a=layer.LearnedResidual(tmp_path,reg);parent=a.entries[0]['parent']
    f=next(f for f in fs if f['connection']==parent)
    if fault=='future':f['issued_epoch']=1100
    elif fault=='stale':f['reference_epoch']=800
    elif fault=='duplicate':fs.append(copy.deepcopy(f))
    elif fault=='mixed_origin':f['reference_epoch']+=1;f['target_epoch']+=1
    elif fault=='parent_hash':next(f for f in fs if f['connection']==parent+'__half_residual')['projection']['parent_forecast_sha256']='wrong'
    elif fault=='nan':f['expected_return_bps']=float('nan')
    elif fault=='orders':f['can_place_orders']=True
    else:a.states[parent]['cutoff_epoch']=1021
    count=len(fs);slots=[];a.append(fs,slots,'fixture',clock=lambda:1023.)
    assert len(fs)-count==68 and sum(s['status']=='residual_inference_unavailable' for s in slots)==68


def test_missing_controls_and_expired_computation_do_not_invent_rows(tmp_path):
    reg,fs=setup(tmp_path);a=layer.LearnedResidual(tmp_path,reg)
    fs=[f for f in fs if f['instrument']!='EUR_USD'];count=len(fs);slots=[]
    a.append(fs,slots,'fixture',clock=lambda:1023.)
    assert len(fs)-count==134 and sum(s['status']=='exact_controls_unavailable' for s in slots)==2
    fs=fs[:count];slots=[];ticks=iter([1023,1201,1201]);a.append(fs,slots,'fixture',clock=lambda:next(ticks))
    assert len(fs)==count and all(s['status']=='residual_inference_unavailable' for s in slots)


def test_reissuance_does_not_duplicate_tracking(tmp_path):
    reg,fs=setup(tmp_path);a=layer.LearnedResidual(tmp_path,reg);one=copy.deepcopy(fs);two=copy.deepcopy(fs)
    a.append(one,[],'fixture',clock=lambda:1023.)
    for f in two:f['issued_epoch']+=.1
    a.append(two,[],'fixture',clock=lambda:1024.)
    assert [tracking.prediction_key(f) for f in one[len(fs):]]==[tracking.prediction_key(f) for f in two[len(fs):]]
