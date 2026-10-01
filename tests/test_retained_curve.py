from pathlib import Path
import copy,hashlib,json,shutil,sys
from types import SimpleNamespace
import pytest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'trad'))
import oanda_retained_curve_v1 as layer
import oanda_retained_projection_v1 as projection
import oanda_retained_forecast_connection_v1 as connection
import oanda_retained_forecast_tracking_v1 as tracking


def setup(tmp_path):
    for name in ('contracts.py','currency_projection_v2.py','curve_shape_layer_v2.py','CURVE_SHAPE_LAYER_CONTRACT_V2.json'):
        p=tmp_path/'stage_c_alignment_integrity_v2'/name;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(ROOT/'stage_c_alignment_integrity_v2'/name,p)
    fp=projection.load_original(tmp_path).fingerprint;cpath=tmp_path/'stage_c_alignment_integrity_v2/CURVE_SHAPE_LAYER_CONTRACT_V2.json';contract=json.loads(cpath.read_bytes())
    pairs=['EUR_USD','GBP_USD'];reg={'pairs':pairs,'connections':[]};parents={};fs=[]
    for base in ('ridge','recovered_hgb'):
        for h in layer.HORIZONS:
            name=base+str(h);(tmp_path/(name+'.json')).write_text(json.dumps({'fit_id':name}),encoding='utf-8')
            p={'arm':base,'horizon_minutes':h,'original_model_id':name,'fit_id':name,'fit_metadata':{'path':name+'.json','sha256':'fixture'}};parents[name]=p
            reg['connections'].append({'id':name,'kind':'legacy26_matched',**p})
            for pair in pairs:fs.append({'instrument':pair,'connection':name,'horizon_minutes':h,'reference_epoch':1020,'issued_epoch':1021.,'target_epoch':1020+h*60,'original_model_id':name,'registry_sha256':'fixture','expected_return_bps':h/100,'reference_mid':1.1,'feature_hash':'feature','input_hash':pair+name,'panel_sha256':None,'can_place_orders':False,'can_promote':False,'research_only':True,'models_fitted':0})
    states={};gates={}
    for base in ('ridge','recovered_hgb'):
        s={'cutoff_epoch':1000,'status':'fitted','contract':contract['layer_fit'],'contract_sha256':fp(contract['layer_fit']),
           'parameters':{'mean':[0.]*8,'scale':[1.]*8,'coefficient':[0.,1.]+[.1]*7},'support':{'membership_sha256':fp([])},'training_membership':[]}
        s['layer_id']=fp(s);states[base]=s
        reg['connections'].append({'id':base+'curve','kind':layer.KIND,'arm':base,'parent':base+'360','horizon_minutes':360,'models':[],'layer_id':s['layer_id'],'original_model_id':base+'360','selection_scope':'synthetic test'})
    for h in layer.HORIZONS:
        s={'status':'fitted','scope':['legacy26',f'technical_endpoint_midpoint_elapsed_{h}m','frozen'],'cutoff_epoch':1000};s['layer_id']=fp(s);gates[str(h)]=s
    bundle={'schema':'retained_curve_state_bundle.v1','contract':contract,'contract_sha256':fp(contract),'source_variant':'raw_matched_expanding','source_result_sha256':'a'*64,'cutoff_epoch':1000,'parents':parents,'eligibility_snapshots':gates,'states':states}
    p=tmp_path/'states.json';p.write_text(json.dumps(bundle),encoding='utf-8');reg['curve']={'state':{'path':'states.json','sha256':hashlib.sha256(p.read_bytes()).hexdigest()},'source_result_sha256':'a'*64,'contract_sha256':hashlib.sha256(cpath.read_bytes()).hexdigest()}
    return reg,fs


def test_original_join_apply_and_tracking_identity(tmp_path,monkeypatch):
    reg,fs=setup(tmp_path);a=layer.Curve(tmp_path,reg)
    monkeypatch.setattr(a.original,'fit_snapshot',lambda *a,**k:pytest.fail('no fits'))
    one=copy.deepcopy(fs);two=copy.deepcopy(fs);slots=[];a.append(one,slots,'fixture',clock=lambda:1022.)
    assert len(one)-len(fs)==4 and len(slots)==4 and all(s['status']=='eligible' for s in slots)
    assert all(abs(f['expected_return_bps']-(3.6+sum(h/100-3.6 for h in layer.HORIZONS[1:])*.1))<1e-12 for f in one[len(fs):])
    for f in two:f['issued_epoch']+=.5
    a.append(two,[],'fixture',clock=lambda:1023.)
    assert [tracking.prediction_key(f) for f in one[len(fs):]]==[tracking.prediction_key(f) for f in two[len(fs):]]
    assert one[:len(fs)]==fs


@pytest.mark.parametrize('fault',['missing_horizon','whole_base_missing','mixed_origin','bad_model','bad_target','future_issue','duplicate','orders','nan','reference_price'])
def test_exact_panel_refusals(tmp_path,fault):
    reg,fs=setup(tmp_path);a=layer.Curve(tmp_path,reg)
    if fault=='missing_horizon':fs=[f for f in fs if f['connection']!='ridge720']
    elif fault=='whole_base_missing':fs=[f for f in fs if not f['connection'].startswith('ridge')]
    elif fault=='mixed_origin':fs[0]['reference_epoch']-=10;fs[0]['target_epoch']-=10
    elif fault=='bad_model':fs[0]['original_model_id']='bad'
    elif fault=='bad_target':fs[0]['target_epoch']+=1
    elif fault=='future_issue':fs[0]['issued_epoch']=1100
    elif fault=='duplicate':fs.append(copy.deepcopy(fs[0]))
    elif fault=='orders':fs[0]['can_place_orders']=True
    elif fault=='nan':fs[0]['expected_return_bps']=float('nan')
    else:fs[0]['reference_mid']=2.
    count=len(fs);slots=[];a.append(fs,slots,'fixture',clock=lambda:1022.)
    assert len(fs)-count==(3 if fault=='mixed_origin' else 2)
    assert len(slots)==4 and any(s['status']!='eligible' for s in slots)


@pytest.mark.parametrize('fault',['state_hash','parent','gate_missing','gate_not_fitted','state_future','shape','contract','entry_parent'])
def test_saved_bundle_refused(tmp_path,fault):
    reg,_=setup(tmp_path);p=tmp_path/'states.json';v=json.loads(p.read_bytes())
    if fault=='state_hash':v['states']['ridge']['parameters']['coefficient'][0]=2.
    elif fault=='parent':reg['connections'][0]['original_model_id']='wrong'
    elif fault=='gate_missing':del v['eligibility_snapshots']['720']
    elif fault=='gate_not_fitted':v['eligibility_snapshots']['720']['status']='unavailable'
    elif fault=='state_future':v['states']['ridge']['cutoff_epoch']=1050
    elif fault=='shape':v['states']['ridge']['parameters']['scale']=[1.]
    elif fault=='contract':v['contract']['feature_definition']['horizons_minutes']=[360]
    else:reg['connections'][-1]['parent']='ridge360'
    p.write_text(json.dumps(v),encoding='utf-8')
    if fault!='state_hash':reg['curve']['state']['sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ValueError):layer.Curve(tmp_path,reg)


def test_expiry_during_curve_refuses_outputs(tmp_path):
    reg,fs=setup(tmp_path);a=layer.Curve(tmp_path,reg);count=len(fs);ticks=iter([1022.,1201.,1201.]);slots=[]
    a.append(fs,slots,'fixture',clock=lambda:next(ticks));assert len(fs)==count and all(s['status']=='curve_inference_unavailable' for s in slots)


def test_publication_budget_bounded_and_oversize_not_issued(tmp_path,monkeypatch):
    path=tmp_path/'current.json';connection.atomic(path,{'data':'x'*(4*1024**2)});before=path.read_bytes()
    with pytest.raises(ValueError,match='publication_size'):connection.atomic(path,{'data':'x'*(8*1024**2)})
    assert path.read_bytes()==before
    monkeypatch.setattr(connection,'capture_rows',lambda:({},{}))
    monkeypatch.setattr(connection.shutil,'disk_usage',lambda p:SimpleNamespace(free=64*1024**3))
    fake=SimpleNamespace(verify_sources=lambda:None,registry={'connections':[]},predict=lambda r,p:{'data':'x'*(8*1024**2),'generated_epoch':1022})
    with pytest.raises(ValueError,match='publication_size'):connection.publish_once(fake,tmp_path)
    assert not (tmp_path/'issued.sqlite').exists()

def test_model_artifact_limit_remains_64_mib(tmp_path,monkeypatch):
    path=tmp_path/'model';path.write_bytes(b'fixture');original=Path.stat;mode=path.stat().st_mode
    def oversized(self,*args,**kwargs):
        return SimpleNamespace(st_size=64*1024**2+1,st_mode=mode) if self==path else original(self,*args,**kwargs)
    monkeypatch.setattr(Path,'stat',oversized)
    with pytest.raises(ValueError,match='artifact_size_bound'):
        connection.checked(tmp_path,{'path':'model','sha256':hashlib.sha256(b'fixture').hexdigest()})
