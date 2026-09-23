import copy,json,math,os,shutil,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
TRAD=Path(os.environ.get('FOREX_CURVE_TRAD',str(ROOT.parent/'trad')));sys.path.insert(0,str(TRAD))
import curve_capacity_operator_v2 as op
from curve_capacity_runner_v2 import prepare,curve,parity,ORIGINS,required,identity_for,validate_resources
from curve_capacity_predict_v2 import SharedCurvePredictor
from joint_readiness_predict_v2 import RetainedPredictor
from joint_readiness_schedule_v2 import select_fit,GROUPS,HORIZONS,PROCEDURES
from publication import RunPublisher
E=ROOT/'evidence/timed_20260922_022952';PF=Path(os.environ.get('FOREX_CURVE_PATHS',str(E/'curve_capacity_step/PATHS.json')));RECIPE=ROOT/'CURVE_CAPACITY_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),read(PF),TRAD,runs)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_CURVE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('curve-capacity');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r;return Path(r['run_path'])
@pytest.fixture(scope='session')
def inputs():return prepare(read(PF),read(RECIPE))

def test_all_original_values_coverage_and_ready_selection(completed,inputs):
    reader,c,sched,by_origin,views,refs=inputs;count=coverage=0
    for t in ORIGINS:
        a=read(completed/f'reference_{t}.json');b=read(completed/f'shared_{t}.json');parity(a,b);count+=len(b['forecasts']);coverage+=len(b['coverage'])
        assert len({(x['instrument'],x['group'],x['horizon_minutes'],x['procedure'],x['method']) for x in b['coverage']})==7616
        for row in b['forecasts']:
            task=select_fit(sched,row['group'],row['horizon_minutes'],row['procedure'],t);assert task['fit_id']==row['selected_fit_id'] and task['joint_ready_epoch']<=t
            p='frozen' if task['fit_cutoff']==c['fit_cutoffs'][0] else 'adaptive';old=refs[row['group'],row['horizon_minutes'],p][row['record_id'],row['method']]
            assert math.isclose(row['prediction'],old['forecast']['prediction'],rel_tol=1e-12,abs_tol=1e-10)
            assert row['original_forecast_id']==old['forecast']['forecast_id']
    assert count==143904 and coverage==152320
    report=read(completed/'run_report.json');assert report['models_fitted']==report['new_forecasts_issued']==0 and report['old_forecast_tape_unchanged']
    assert len(read(completed/'COMPLETION_MANIFEST.json')['payloads'])==44

def test_resources_preserve_all_samples_and_never_promote(completed):
    r=read(completed/'curve_resources.json');validate_resources(r)
    assert len(r['origins'])==40 and r['live_deadline_guarantee'] is False
    for index,t in enumerate(ORIGINS):
        expected=['reference','shared'] if index%2==0 else ['shared','reference']
        assert all(x['comparison_order']==expected for x in r['origins'] if x['origin_epoch']==t)
    bad=copy.deepcopy(r);bad['origins'][0]['live_ready']=True
    with pytest.raises(ValueError,match='cannot_promote'):validate_resources(bad)
    bad=copy.deepcopy(r);bad['origins'][0]['elapsed_seconds']=31
    with pytest.raises(ValueError,match='resource_limit'):validate_resources(bad)

def test_measured_two_second_miss_is_diagnostic_not_waived(completed):
    r=read(completed/'curve_resources.json');r['origins'][0]['elapsed_seconds']=3;r['origins'][0]['diagnostic_target_met']=False;validate_resources(r)
    r['origins'][0]['diagnostic_target_met']=True
    with pytest.raises(ValueError,match='cannot_promote'):validate_resources(r)

def test_shared_curve_exact_parity_without_fitting(inputs,monkeypatch):
    from sklearn.linear_model import Ridge
    from sklearn.ensemble import HistGradientBoostingRegressor
    def forbidden(*a,**k):raise AssertionError('capacity comparison cannot fit models')
    monkeypatch.setattr(Ridge,'fit',forbidden);monkeypatch.setattr(HistGradientBoostingRegressor,'fit',forbidden)
    reader,c,sched,by_origin,views,refs=inputs;t=ORIGINS[1];predictor=SharedCurvePredictor(reader)
    a=curve(RetainedPredictor(reader),t,by_origin[t],views,sched,c,refs);b=curve(predictor,t,by_origin[t],views,sched,c,refs);parity(a,b)
    assert predictor.predict_calls==28 and b['prediction_requests']==56 and predictor.transform_calls<=21

def test_same_origin_input_mutation_and_cross_origin_cache_refused(inputs):
    reader,c,sched,by_origin,views,refs=inputs;t=ORIGINS[1];p=SharedCurvePredictor(reader);p.begin_origin(t);task=select_fit(sched,'legacy26',60,'frozen',t);obs=by_origin[t]
    before=p.predict(task,obs,views);assert p.predict(task,obs,views)==before and p.predict_calls==1
    changed=copy.deepcopy(obs);next(o for o in changed if o['features'] is not None)['features'][0]+=1
    with pytest.raises(ValueError,match='input_changed'):p.predict(task,changed,views)
    with pytest.raises(ValueError,match='origin_mismatch'):p.predict(task,by_origin[ORIGINS[2]],views)
    p.begin_origin(ORIGINS[2]);p.predict(task,by_origin[ORIGINS[2]],views);assert p.predict_calls==1

def test_future_weight_not_loaded_and_future_view_poisoning(inputs):
    reader,c,sched,by_origin,views,refs=inputs;t=ORIGINS[1];p=SharedCurvePredictor(reader);p.begin_origin(t);task=select_fit(sched,'compact38_cost2',60,'frozen',t)
    late={**task,'joint_ready_epoch':t+1}
    with pytest.raises(ValueError,match='not_ready'):p.predict(late,by_origin[t],views)
    assert not p.cache
    a=p.predict(task,by_origin[t],views);changed=copy.deepcopy(views)
    future=next(o for o in by_origin[ORIGINS[2]] if o['features'] is not None);changed[future['record_id']]['compact38_cost2']['values'][0]=1e9
    q=SharedCurvePredictor(reader);q.begin_origin(t);assert q.predict(task,by_origin[t],changed)==a

def test_feature_order_and_actual_transform_tamper_refused(inputs):
    reader,c,sched,by_origin,views,refs=inputs;t=ORIGINS[1];task=select_fit(sched,'compact38_cost2',60,'frozen',t);p=SharedCurvePredictor(reader);p.begin_origin(t)
    changed=copy.deepcopy(views);o=next(x for x in by_origin[t] if x['features'] is not None);changed[o['record_id']]['compact38_cost2']['feature_names'].reverse()
    with pytest.raises(ValueError,match='feature_order'):p.predict(task,by_origin[t],changed)
    q=SharedCurvePredictor(reader);q.begin_origin(t);RetainedPredictor.predict(q,task,[],{});q.cache[task['fit_id']][1]['ridge'].steps[0][1].statistics_[0]+=1
    with pytest.raises(ValueError,match='actual_transform'):q.predict(task,by_origin[t],views)

def test_model_blob_read_hash_refused(inputs,monkeypatch):
    reader,*_=inputs;p=RetainedPredictor(reader);name=next(n for n in reader.dependencies['baseline']['payloads'] if n.endswith('.joblib'));h=reader.dependencies['baseline']['payloads'][name]
    monkeypatch.setitem(reader.dependencies['baseline']['payloads'],name,'0'*64)
    with pytest.raises(ValueError,match='consumed_bytes_changed'):p.blob('baseline',name)

def test_source_and_recipe_drift_before_numerical_import(completed,tmp_path,monkeypatch):
    assert op.operate('status',RECIPE,'0'*64,read(PF),TRAD,tmp_path)['status']=='review_required'
    old=op.sha;monkeypatch.setattr(op,'sha',lambda p:'0'*64 if p.name=='curve_capacity_predict_v2.py' else old(p))
    assert call('status',tmp_path)['reason']=='curve_source_drift_before_import'

def test_completed_run_does_not_rebenchmark(completed,monkeypatch):
    import curve_capacity_runner_v2 as runner
    monkeypatch.setattr(runner,'prepare',lambda *a:(_ for _ in ()).throw(AssertionError('rebenchmark forbidden')))
    assert call('run',completed.parent)['status']=='completed_verified'

def test_payload_tampering_and_one_writer(completed,tmp_path):
    root=tmp_path/read(RECIPE)['run_id'];shutil.copytree(completed,root);(root/'run_report.json').write_text('{}');assert call('verify',tmp_path)['status']=='review_required'
    other=tmp_path/'writer';r=read(RECIPE);p=RunPublisher(other,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',other)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_actual_process_crash_resume_scientific_equality(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'curve_capacity_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PF),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=900);assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable';cmd=cmd[:-2];cmd[4]='resume';p=subprocess.run(cmd,capture_output=True,text=True,timeout=900);assert p.returncode==0,p.stdout+p.stderr
    root=tmp_path/read(RECIPE)['run_id'];assert all(op.sha(root/n)==op.sha(completed/n) for n in required() if n!='curve_resources.json');validate_resources(read(root/'curve_resources.json'))

def test_preflight_does_not_import_numerical_libraries():
    code="""import sys,importlib.abc,json
sys.path.insert(0,sys.argv[1])
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'numpy','pandas','scipy','sklearn','joblib'}:raise AssertionError('numerical preflight import:'+fullname)
sys.meta_path.insert(0,Deny())
from pathlib import Path
import curve_capacity_operator_v2 as op
p=Path(sys.argv[2]);op.preflight(p,op.sha(p),json.loads(Path(sys.argv[3]).read_text()),Path(sys.argv[4]))
"""
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),str(PF),str(TRAD)],capture_output=True,text=True,timeout=90);assert p.returncode==0,p.stdout+p.stderr
