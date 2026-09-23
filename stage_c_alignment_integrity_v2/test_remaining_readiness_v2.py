import copy,json,os,shutil,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import remaining_readiness_operator_v2 as op
import remaining_readiness_native_v2 as native
from remaining_readiness_runner_v2 import prepare,consumed,required,identity_for,EPOCHS,TARGET
from contracts import fingerprint
from publication import RunPublisher
E=ROOT/'evidence/timed_20260922_022952';PF=Path(os.environ.get('FOREX_REMAINING_READINESS_PATHS',str(E/'native_readiness_step/PATHS.json')));TRAD=Path(os.environ.get('FOREX_REMAINING_READINESS_TRAD',str(ROOT.parent/'trad')));RECIPE=ROOT/'REMAINING_READINESS_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),read(PF),TRAD,runs)
@pytest.fixture(scope='session')
def inputs():
    r=read(RECIPE);paths=read(PF);return paths,r,prepare(paths,r)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_REMAINING_READINESS_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('remaining-ready');result=call('verify' if supplied else 'run',runs);assert result['status']=='completed_verified',result;return Path(result['run_path'])
def actual(inputs,index=1):
    paths,r,(meta,plan,observations,refs,original)=inputs;t=EPOCHS[index];h=(TARGET-t)//60;tree=consumed(paths,r,'remaining',f'fit_{h}.joblib');return t,meta[h],tree,observations[t],refs[str(t)],original,plan,TRAD

def test_schedule_is_one_worker_eight_full_fit_slots(inputs):
    _,_,(meta,plan,_,_,_)=inputs;assert len(plan['tasks'])==8 and plan['joint_ready_epoch']==plan['fit_cutoff']+240
    assert all(b['slot_start_epoch']==a['slot_end_epoch'] for a,b in zip(plan['tasks'],plan['tasks'][1:]))
    assert plan['joint_ready_epoch']>EPOCHS[0] and plan['joint_ready_epoch']<EPOCHS[1]
    with pytest.raises(ValueError,match='all_eight'):native.schedule({k:v for k,v in meta.items() if k!=360})

def test_unready_first_origin_has_all68_abstentions_and_no_prediction(inputs,monkeypatch,tmp_path):
    args=list(actual(inputs,0));args[2]=None
    monkeypatch.setattr(native,'predict',lambda *a,**k:(_ for _ in ()).throw(AssertionError('future_model_prediction')))
    result=native.build_origin(*args);assert len(result['coverage'])==136 and not result['forecasts'] and not result['native_packets']
    assert {x['reason'] for x in result['coverage']}=={'model_set_not_joint_ready'}
    import remaining_readiness_runner_v2 as runner
    original_consume=runner.consumed
    def no_weight(paths,r,alias,name):
        assert not name.endswith('.joblib'),'no_weight_consumption_before_joint_readiness'
        return original_consume(paths,r,alias,name)
    def stop_after_first(*a):
        assert a[0]==EPOCHS[0] and a[2] is None
        assert native.build_origin(*a)==result
        raise RuntimeError('first_origin_checked')
    monkeypatch.setattr(runner,'consumed',no_weight);monkeypatch.setattr(runner,'build_origin',stop_after_first)
    with pytest.raises(RuntimeError,match='first_origin_checked'):runner.run(inputs[0],TRAD,inputs[1],tmp_path)

def test_every_actual_value_target_and_coverage_is_preserved_or_explicitly_withheld(completed,inputs):
    paths,r,(meta,plan,obs,refs,original)=inputs;source={(x['record_id'],x['method']):x for x in original if x['method'] in native.METHODS};total=0
    for t in EPOCHS:
        data=read(completed/f'origin_{t}.json');assert len(data['coverage'])==136 and {x['instrument'] for x in data['coverage']}==set(r['universe'])
        assert len(data['forecasts'])==len(data['native_packets'])==sum(x['reason']=='eligible' for x in data['coverage'])
        for row in data['forecasts']:
            old=source[row['record_id'],row['method']];f=row['forecast'];assert f['prediction']==old['forecast']['prediction'] and row['original_forecast_id']==old['forecast']['forecast_id']
            assert f['model_ready_epoch']==plan['joint_ready_epoch']<=t and f['available_epoch']==t+2 and f['forecast_id']!=old['forecast']['forecast_id'];total+=1
        for packet in data['native_packets']:
            assert packet['original_target_epoch']==TARGET and packet['conditioning_epoch']==t and packet['observed_publication'] is False
            node=packet['prepared_curve']['nodes'][0];assert node['original_target_epoch']==TARGET
    report=read(completed/'run_report.json');assert total==report['forecast_rows'] and report['joint_unready_coverage_rows']==136 and report['coverage_rows']==1088

def test_all_prepared_curves_validate_and_actual_issue_consumer_refuses(completed):
    contract,_=native.load_native(TRAD);count=0
    for t in EPOCHS:
        for packet in read(completed/f'origin_{t}.json')['native_packets']:
            prepared=packet['prepared_curve'];assert contract.validate_prepared(prepared,expected_source_bindings=packet['source_bindings'])==prepared
            with pytest.raises(Exception,match='nonprospective_computation_cannot_issue'):contract.issue_curve(prepared,expected_source_bindings=packet['source_bindings'],clock=lambda:t+2)
            count+=1
    assert count==read(completed/'run_report.json')['native_packets'] and count>0

def test_no_base_fit_during_actual_rebuild(inputs,monkeypatch):
    import matched_campaign_models_v2 as models
    monkeypatch.setattr(models,'fit_pair',lambda *a,**k:(_ for _ in ()).throw(AssertionError('forbidden_fit')))
    result=native.build_origin(*actual(inputs));assert result['forecasts']

def test_original_forecast_tamper_refused(inputs):
    args=list(actual(inputs));args[5]=copy.deepcopy(args[5]);row=next(x for x in args[5] if x['method']=='ridge' and x['forecast']['decision_epoch']==args[0]);row['forecast']['prediction']+=1
    with pytest.raises(ValueError,match='recomputation_mismatch'):native.build_origin(*args)

def test_reference_binding_and_wrong_target_refused(inputs):
    args=list(actual(inputs));args[4]=copy.deepcopy(args[4]);pair=next(iter(args[4]));args[4][pair]['price_epoch']+=60
    with pytest.raises(ValueError,match='reference_binding'):native.build_origin(*args)
    args=list(actual(inputs));args[1]=copy.deepcopy(args[1]);args[1]['target']['horizon_seconds']+=60
    with pytest.raises(ValueError,match='exact_remaining_original_target'):native.build_origin(*args)

def test_future_feature_availability_excludes_only_that_observation(inputs):
    args=list(actual(inputs));baseline=native.build_origin(*args);args[3]=copy.deepcopy(args[3]);o=next(x for x in args[3] if x['features'] is not None);o['available_epoch']=args[0]+1;changed=native.build_origin(*args)
    assert len(changed['forecasts'])==len(baseline['forecasts'])-2
    assert [x for x in changed['coverage'] if x['record_id']==o['record_id']]==[{**x,'reason':'feature_not_ready'} for x in baseline['coverage'] if x['record_id']==o['record_id']]
    assert [x for x in baseline['forecasts'] if x['record_id']!=o['record_id']]==changed['forecasts']

def test_corrupt_weights_and_consumed_hash_refused(inputs,tmp_path):
    args=list(actual(inputs));args[2]+=b'x'
    with pytest.raises(ValueError,match='tree_identity'):native.build_origin(*args)
    paths,r,_=inputs;bad={**paths,'remaining':str(tmp_path)};(tmp_path/'fit_360.joblib').write_bytes(b'bad')
    with pytest.raises(ValueError,match='consumed_bytes_changed'):consumed(bad,r,'remaining','fit_360.joblib')

def test_completed_idempotence_resource_bounds_and_false_completion(completed,tmp_path,monkeypatch):
    import remaining_readiness_runner_v2 as runner
    monkeypatch.setattr(runner,'prepare',lambda *a:(_ for _ in ()).throw(AssertionError('no rerun')));assert call('run',completed.parent)['status']=='completed_verified'
    resources=read(completed/'prediction_resources.json');assert len(resources['origins'])==8 and all(0<=x['elapsed_seconds']<=30 for x in resources['origins'])
    dest=tmp_path/completed.name;shutil.copytree(completed,dest);(dest/'schedule.json').write_text('{}');assert call('verify',tmp_path)['status']=='review_required'

def test_source_recipe_and_native_predecessor_guards(tmp_path,monkeypatch):
    assert op.operate('status',RECIPE,'0'*64,read(PF),TRAD,tmp_path)['status']=='review_required'
    old=op.sha;monkeypatch.setattr(op,'sha',lambda p:'0'*64 if p.name=='remaining_readiness_native_v2.py' else old(p));assert 'source_drift_before_import' in call('status',tmp_path)['reason']
    monkeypatch.setattr(op,'sha',old);bad=tmp_path/'trad';bad.mkdir()
    for n in op.PREDECESSORS:shutil.copyfile(TRAD/n,bad/n)
    changed=bad/next(iter(op.PREDECESSORS));changed.write_bytes(changed.read_bytes()+b'\n')
    assert 'native_predecessor_changed' in op.operate('status',RECIPE,op.sha(RECIPE),read(PF),bad,tmp_path/'runs')['reason']

def test_one_writer(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('resume',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_real_crash_resume_preserves_scientific_bytes(boundary,tmp_path,completed):
    cmd=[sys.executable,'-I','-B',str(ROOT/'remaining_readiness_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PF),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)];p=subprocess.run(cmd,capture_output=True,text=True,timeout=300);assert p.returncode==93,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable';r=call('resume',tmp_path);assert r['status']=='completed_verified',r
    names=[n for n in required() if n!='prediction_resources.json'];assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in names)
    if boundary==0:assert read(Path(r['run_path'])/'prediction_resources.json')['origins'][0]['existing_payload_validation_only'] is False

def test_stdlib_preflight_no_numerical_import():
    code='import sys,importlib.abc;from pathlib import Path;sys.path.insert(0,sys.argv[1]);import remaining_readiness_operator_v2 as op;op.preflight(Path(sys.argv[2]),sys.argv[3],op.read(Path(sys.argv[4])),Path(sys.argv[5]));assert "numpy" not in sys.modules and "sklearn" not in sys.modules'
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),op.sha(RECIPE),str(PF),str(TRAD)],capture_output=True,text=True,timeout=60);assert p.returncode==0,p.stdout+p.stderr
