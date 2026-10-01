"""Consumer contract tests use synthetic metadata, never fit a model."""
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import numpy as np
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_retained_forecast_connection_v1 as m
import oanda_retained_legacy26_v1 as legacy


def fixture(tmp_path,monkeypatch,arm='ridge',mutate=None):
    root=tmp_path; (root/'trad').mkdir()
    monkeypatch.setattr(m,'ROOT',root/'trad')
    model=SimpleNamespace(feature_names_in_=np.array(legacy.FEATURES),predict=lambda x:np.full(len(x),4.5))
    monkeypatch.setattr(m.joblib,'load',lambda stream:model)
    target={'target_id':'technical_endpoint_midpoint_elapsed_360m','horizon_seconds':21600}
    ridge={'mean':[0.]*26,'scale':[2.]*26,'coefficient':[1.]+[2.]*26,'target':target,
        'feature_count':26,'training_population_sha256':'p','maximum_outcome_available_epoch':50,
        'ready_epoch':100,'training_view':{'fit_cutoff_epoch':90}}
    ridge['model_id']=m.sha(m.encoded(ridge))
    meta={'schema_version':'forex_matched_fit_pair.v1','target':target,'feature_schema_sha256':m.sha(m.encoded(legacy.FEATURES)),
          'tree_sha256':m.sha(b'synthetic'),'maximum_outcome_available_epoch':50,'fit_cutoff':90,
          'ready_epoch':100,'ridge':ridge,'training_population_sha256':'p','training_view':ridge['training_view']}
    meta['fit_id']=m.sha(m.encoded(meta))
    entry={'id':'matched','kind':'legacy26_matched','arm':arm,'horizon_minutes':360,'feature_names':legacy.FEATURES,
           'models':[{'path':'model','sha256':m.sha(b'synthetic')}],
           'selection_scope':'synthetic','evidence':{'synthetic':True},
           'original_model_id':m.sha(m.encoded({'fit_id':meta['fit_id'],'method':arm}))}
    if mutate:mutate(entry,meta)
    (root/'model').write_bytes(b'synthetic');(root/'meta').write_bytes(m.encoded(meta))
    entry['fit_metadata']={'path':'meta','sha256':m.sha((root/'meta').read_bytes())}
    reg={'schema':m.SCHEMA,'models_fitted':0,'pairs':['EUR_USD']+[f'PAIR_{i}' for i in range(67)],
         'connections':[entry],'source_bindings':{},'normalizers':{},'unconnected_targets':[],'limits':[]}
    p=root/'registry.json';p.write_bytes(m.encoded(reg))
    return p,entry,model


def input_row():
    values=dict.fromkeys(legacy.FEATURES,1.)
    return {'bar_start_epoch':960,'published_epoch':1021,'captured_epoch':1022,'reference_mid':1.1,
        'receipt_id':'receipt','feature_hash':'original','input_hash':'raw','entry_long':1.,'entry_short':1.,
        'values':dict.fromkeys(legacy.FEATURES,999.),
        'legacy26':{'status':'available','values':values,'feature_hash':'legacy','input_hash':'history','provenance':{}}}


@pytest.mark.parametrize('arm,expected',[('ridge',27.),('recovered_hgb',4.5)])
def test_real_consumer_uses_original_inputs_and_exact_target(tmp_path,monkeypatch,arm,expected):
    p,e,_=fixture(tmp_path,monkeypatch,arm);c=m.SavedConnection(p)
    f=c.predict({'EUR_USD':input_row()},{},clock=lambda:1023)['forecasts'][0]
    assert f['expected_return_bps']==expected
    assert f['target_epoch']==1020+21600 and f['original_model_id']==e['original_model_id']
    assert f['feature_hash']=='legacy' and f['input_hash']=='history' and not f['can_place_orders']


@pytest.mark.parametrize('mutation',[
    lambda e,d:e.update(horizon_minutes=1080),
    lambda e,d:e.update(arm='extra_trees'),
    lambda e,d:e.update(original_model_id='wrong'),
    lambda e,d:e.update(feature_names=list(reversed(e['feature_names']))),
    lambda e,d:d.update(tree_sha256='wrong'),
    lambda e,d:d.update(ready_epoch=1),
    lambda e,d:d.update(maximum_outcome_available_epoch=1000),
    lambda e,d:d['ridge']['scale'].__setitem__(0,0.),
    lambda e,d:d['ridge'].update(target={'target_id':'other','horizon_seconds':21600}),
    lambda e,d:d['ridge'].update(training_population_sha256='different'),
])
def test_altered_contract_cannot_load(tmp_path,monkeypatch,mutation):
    p,_,_=fixture(tmp_path,monkeypatch,mutate=mutation)
    with pytest.raises(ValueError):m.SavedConnection(p)


def test_resealed_invalid_ridge_rejected(tmp_path,monkeypatch):
    def mutate(e,d):
        d['ridge']['scale'][0]=0
        d['ridge']['model_id']=m.sha(m.encoded({k:v for k,v in d['ridge'].items() if k!='model_id'}))
        d['fit_id']=m.sha(m.encoded({k:v for k,v in d.items() if k!='fit_id'}))
        e['original_model_id']=m.sha(m.encoded({'fit_id':d['fit_id'],'method':e['arm']}))
    p,_,_=fixture(tmp_path,monkeypatch,mutate=mutate)
    with pytest.raises(ValueError,match='ridge_parameters'):m.SavedConnection(p)


def test_matched_missing_history_and_changed_model_columns_refused(tmp_path,monkeypatch):
    p,e,model=fixture(tmp_path,monkeypatch,'recovered_hgb');c=m.SavedConnection(p)
    r=input_row();r['legacy26']={'status':'unavailable','reason':'history_unavailable'}
    result=c.predict({'EUR_USD':r},{},clock=lambda:1023)
    assert not result['forecasts'] and result['coverage'][0]['status']=='history_unavailable'
    model.feature_names_in_=model.feature_names_in_[::-1]
    result=c.predict({'EUR_USD':input_row()},{},clock=lambda:1023)
    assert not result['forecasts'] and result['coverage'][0]['status']=='inference_unavailable'


def test_changed_metadata_bytes_rejected_before_load(tmp_path,monkeypatch):
    p,_,_=fixture(tmp_path,monkeypatch);(tmp_path/'meta').write_text('{}')
    with pytest.raises(ValueError,match='artifact_hash'):m.SavedConnection(p)
