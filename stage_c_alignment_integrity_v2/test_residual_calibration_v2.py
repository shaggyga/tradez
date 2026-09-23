import copy,hashlib,json,os,shutil,subprocess,sys,zipfile
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import residual_calibration_operator_v2 as op
from prequential_residual_calibration_v2 import contract,fit_snapshot,apply_snapshot,score_rows,weighted_quantile,pinball,visible_calibration
from residual_calibration_runner_v2 import consumed,outcome_index,required,identity_for,ORIGINS
from contracts import fingerprint
from publication import RunPublisher
E=ROOT/'evidence/timed_20260922_022952';PF=Path(os.environ.get('FOREX_CALIBRATION_PATHS',str(E/'calibration_step/PATHS.json')));RECIPE=ROOT/'RESIDUAL_CALIBRATION_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),read(PF),runs)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_CALIBRATION_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('residual-calibration');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r;return Path(r['run_path'])

TARGET='technical_endpoint_midpoint_elapsed_15m';SCOPE=['legacy26',TARGET,'frozen','ridge'];BASE=1700000040
def synthetic():
    rows=[];out={}
    for i in range(10):
        origin=BASE+i*28800
        for j in range(1 if i==0 else 20):
            pair=f'SYN_{j:02}';rid=f'{pair}:{origin}'
            f={'schema_version':'forecast.v2','forecast_id':fingerprint([rid,'forecast']),'instrument':pair,'decision_epoch':origin,'available_epoch':origin+2,'model_id':'m','model_ready_epoch':BASE-1,'training_view_fingerprint':'past','target_id':TARGET,'prediction':10.,'coverage_reason':'eligible'}
            rows.append({'record_id':rid,'group':'legacy26','method':'ridge','procedure':'frozen','forecast':f})
            out[rid,TARGET]={'record_id':rid,'target_id':TARGET,'label_end_epoch':origin+900,'available_epoch':origin+900,'value':10.+i}
    return rows,out,BASE+10*28800

def test_origin_balanced_mean_and_empirical_quantile_oracle():
    rows,out,cutoff=synthetic();s=fit_snapshot(rows,out,cutoff,SCOPE)
    assert s['status']=='fitted' and s['support']['rows']==181 and s['support']['distinct_origins']==10
    assert s['parameters']=={'mean_residual_bps':4.5,'lower_residual_bps':0.,'upper_residual_bps':8.}
    assert weighted_quantile([(0,1),(1,1),(2,1),(3,1)],.5)==1

def test_future_and_current_label_poisoning_cannot_change_prefix():
    rows,out,cutoff=synthetic();cutoff=BASE+8*28800;s=fit_snapshot(rows,out,cutoff,SCOPE);changed=copy.deepcopy(out)
    for o in changed.values():
        if o['available_epoch']>cutoff:o['value']=1e9
    assert fit_snapshot(rows,changed,cutoff,SCOPE)==s
    altered=copy.deepcopy(rows)
    for row in altered:
        if row['forecast']['decision_epoch']>=cutoff:row['forecast']['prediction']=-1e9
    assert fit_snapshot(altered,changed,cutoff,SCOPE)==s

def test_availability_delayed_between_origin_and_publication_excluded():
    rows,out,cutoff=synthetic();chosen=rows[0];changed=copy.deepcopy(rows);changed[0]['forecast']['available_epoch']=BASE+899
    # Target remains in the future; neither original nor delayed issue is mature at this cutoff.
    assert fit_snapshot(changed,out,BASE+100,SCOPE)['support']['rows']==0
    delayed=copy.deepcopy(out);delayed[chosen['record_id'],TARGET]['available_epoch']=cutoff+1
    s=fit_snapshot(rows,delayed,cutoff,SCOPE);assert chosen['forecast']['forecast_id'] not in s['training_forecast_ids']

def test_minimum_support_is_distinct_not_raw_row_count():
    rows,out,cutoff=synthetic();subset=[r for r in rows if r['forecast']['decision_epoch']>=BASE+8*28800];s=fit_snapshot(subset,out,cutoff,SCOPE)
    assert s['support']['rows']==40 and s['status']=='insufficient_distinct_support' and s['parameters'] is None

def test_duplicate_alias_scope_and_target_clock_refused():
    rows,out,cutoff=synthetic()
    with pytest.raises(ValueError,match='unique_calibration'):fit_snapshot(rows+[rows[0]],out,cutoff,SCOPE)
    bad=copy.deepcopy(rows);bad[0]['record_id']='alias'
    with pytest.raises(ValueError,match='canonical_calibration_event'):fit_snapshot(bad,out,cutoff,SCOPE)
    with pytest.raises(ValueError,match='fixed_calibration_scope'):fit_snapshot(rows,out,cutoff,['other',TARGET,'frozen','ridge'])
    bad=copy.deepcopy(out);bad[rows[0]['record_id'],TARGET]['label_end_epoch']+=60
    with pytest.raises(ValueError,match='exact_calibration_target'):fit_snapshot(rows,bad,cutoff,SCOPE)

def test_current_example_future_snapshot_and_tampering_refused():
    rows,out,cutoff=synthetic();s=fit_snapshot(rows,out,cutoff,SCOPE)
    with pytest.raises(ValueError,match='future_calibrator'):apply_snapshot(rows[-1],s,'expanding_prefix')
    bad=copy.deepcopy(s);bad['parameters']['mean_residual_bps']+=1
    with pytest.raises(ValueError,match='identity_mismatch'):apply_snapshot(rows[-1],bad,'expanding_prefix')

def test_application_pinball_and_asof_have_no_future_outcome():
    rows,out,cutoff=synthetic();s=fit_snapshot(rows,out,cutoff,SCOPE);row=copy.deepcopy(rows[-1]);f=row['forecast'];f['decision_epoch']=cutoff;f['available_epoch']=cutoff+2;f['forecast_id']='new';row['record_id']=f"{f['instrument']}:{cutoff}"
    result=apply_snapshot(row,s,'expanding_prefix');assert result['adjusted_mean_bps']==14.5 and result['empirical_lower_bps']==10 and result['empirical_upper_bps']==18
    assert result['production_available_epoch'] is None and visible_calibration(result,cutoff)['record'] is None
    assert visible_calibration(result,cutoff+2)['outcomes_included'] is False
    assert pinball(10,20,.1)==1 and pinball(20,10,.1)==9

def test_scores_only_mature_supported_shared_rows():
    rows,out,cutoff=synthetic();s=fit_snapshot(rows,out,cutoff,SCOPE);row=copy.deepcopy(rows[-1]);f=row['forecast'];f.update(decision_epoch=cutoff,available_epoch=cutoff+2,forecast_id='later');row['record_id']=f"{f['instrument']}:{cutoff}";record=apply_snapshot(row,s,'expanding_prefix')
    label={'value':15.,'available_epoch':cutoff+900};lookup={(record['record_id'],TARGET):label}
    c={**contract(),'assessment_asof':cutoff+899};assert score_rows([record],lookup,TARGET,c)['metrics'] is None
    c['assessment_asof']+=1;score=score_rows([record],lookup,TARGET,c);assert score['metrics']['base_mse_bps2']==25 and score['metrics']['adjusted_mse_bps2']==.25 and score['metrics']['empirical_interval_coverage']==1

def test_every_snapshot_is_prior_mature_and_every_attempt_retained(completed):
    recipe=read(RECIPE);paths=read(PF);outcomes=outcome_index(paths,recipe);count=snapshots=0;support=set()
    for p in sorted(completed.glob('calibration_*.json')):
        if p.name=='calibration_contract.json':continue
        data=read(p);original=consumed(Path(paths['joint']),recipe['dependencies']['joint'],'forecasts_'+p.name.removeprefix('calibration_'));by={r['forecast']['forecast_id']:r for r in original};snapshots+=len(data['snapshots']);count+=len(data['rows'])
        for item in data['snapshots']:
            s=item['snapshot'];selected=[by[fid] for fid in s['training_forecast_ids']]
            assert all(r['forecast']['decision_epoch']<s['cutoff_epoch'] and r['forecast']['available_epoch']<=s['cutoff_epoch'] and outcomes[r['record_id'],r['forecast']['target_id']]['available_epoch']<=s['cutoff_epoch'] for r in selected)
            assert s['support']['rows']==len(selected)
        for row in data['rows']:
            assert row['base_forecast_id'] in by and row['base_prediction_bps']==by[row['base_forecast_id']]['forecast']['prediction'];support.add(row['instrument'])
            if row['status']=='fitted':assert row['calibration_cutoff']<=row['origin_epoch'] and row['empirical_lower_bps']<=row['empirical_upper_bps']
            else:assert row['adjusted_mean_bps'] is None
    assert count==287808 and snapshots==2352 and support==set(recipe['universe'])
    report=read(completed/'run_report.json');assert report['coverage_rows']==304640 and report['base_models_fitted']==0 and report['score_groups']==224
    assert len(read(completed/'COMPLETION_MANIFEST.json')['payloads'])==116

def test_actual_snapshot_direct_recalculation_and_long_horizon_abstention(completed):
    recipe=read(RECIPE);paths=read(PF);outcomes=outcome_index(paths,recipe);name='legacy26_60_frozen.json';data=read(completed/('calibration_'+name));original=consumed(Path(paths['joint']),recipe['dependencies']['joint'],'forecasts_'+name)
    for item in data['snapshots']:
        s=item['snapshot'];source=[r for r in original if r['method']==s['scope'][-1]];assert fit_snapshot(source,outcomes,s['cutoff_epoch'],s['scope'])==s
    long=read(completed/'calibration_legacy26_7200_frozen.json');assert all(x['snapshot']['status']=='insufficient_distinct_support' for x in long['snapshots'])

def test_source_pin_and_consumed_bytes_refused(completed,tmp_path,monkeypatch):
    assert op.operate('status',RECIPE,'0'*64,read(PF),tmp_path)['status']=='review_required'
    old=op.sha;monkeypatch.setattr(op,'sha',lambda p:'0'*64 if p.name=='prequential_residual_calibration_v2.py' else old(p));assert call('status',tmp_path)['reason']=='calibration_source_drift_before_import'
    p=tmp_path/'x.json';p.write_text('{}')
    with pytest.raises(ValueError,match='consumed_bytes_changed'):consumed(tmp_path,{'payloads':{'x.json':'0'*64}},'x.json')

def test_completed_run_is_idempotent_and_false_completion_refused(completed,tmp_path,monkeypatch):
    import residual_calibration_runner_v2 as runner
    monkeypatch.setattr(runner,'outcome_index',lambda *a:(_ for _ in ()).throw(AssertionError('no repeated computation')))
    assert call('run',completed.parent)['status']=='completed_verified'
    root=tmp_path/read(RECIPE)['run_id'];shutil.copytree(completed,root);(root/'scores.json').write_text('[]');assert call('verify',tmp_path)['status']=='review_required'

def test_one_writer(completed,tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_process_death_resume_exact(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'residual_calibration_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PF),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable';cmd=cmd[:-2];cmd[4]='resume';p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==0,p.stdout+p.stderr
    root=tmp_path/read(RECIPE)['run_id'];assert all(op.sha(root/n)==op.sha(completed/n) for n in required())

def test_isolated_preflight_no_numerical_or_estimator_import():
    code="""import sys,importlib.abc,json
sys.path.insert(0,sys.argv[1])
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in {'numpy','pandas','scipy','sklearn','joblib'}:raise AssertionError('preflight import:'+fullname)
sys.meta_path.insert(0,Deny())
from pathlib import Path
import residual_calibration_operator_v2 as op
p=Path(sys.argv[2]);op.preflight(p,op.sha(p),json.loads(Path(sys.argv[3]).read_text()))
"""
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),str(PF)],capture_output=True,text=True,timeout=90);assert p.returncode==0,p.stdout+p.stderr

def capsule_example(tmp_path,mutation=None):
    import residual_calibration_checkpoint_v2 as cp
    blobs={'source/'+n:b'# reader fixture only; never executed\n' for n in cp.ALLOWLIST}
    blobs['source/RESIDUAL_CALIBRATION_OPERATOR_RECIPE.json']=json.dumps({'dependencies':{'joint':{'payloads':{}},'technical':{'payloads':{}}}}).encode()
    for alias in ('joint','technical'):
        for n in ('RUN_IDENTITY.json','COMPLETION_MANIFEST.json'):blobs['capsule/'+alias+'/'+n]=b'{}'
    for n in ('EXPECTED_REPLAY.json','ORIGINAL_OPERATOR_RECEIPT.json','LINEAGE_CHECKPOINTS.json','CAPSULE_SCOPE.json'):blobs[n]=b'{}'
    if mutation=='traversal':blobs['../escape']=b'x'
    if mutation=='undeclared':blobs['extra.json']=b'{}'
    if mutation=='oversize':blobs['CAPSULE_SCOPE.json']=b'x'*(8*1024*1024+1)
    m={'schema_version':cp.SCHEMA,'source_allowlist':list(cp.ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(blobs.items())]}
    if mutation=='tamper':blobs['CAPSULE_SCOPE.json']=b'[]'
    archive=tmp_path/'reader_fixture.zip'
    with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in blobs.items():z.writestr(n,b)
        z.writestr(cp.MANIFEST,json.dumps(m))
    return cp,archive

def test_capsule_reader_exact_allowlist_without_execution(tmp_path):
    cp,p=capsule_example(tmp_path);data=cp.inspect_capsule(p,op.sha(p));assert 'capsule/joint/RUN_IDENTITY.json' in data

def test_capsule_reader_external_pin_required(tmp_path):
    cp,p=capsule_example(tmp_path)
    with pytest.raises(ValueError,match='pinned_record_capsule'):cp.inspect_capsule(p,'0'*64)

@pytest.mark.parametrize('mutation',['traversal','undeclared','oversize','tamper'])
def test_capsule_reader_refuses_unsafe_or_changed_members(tmp_path,mutation):
    cp,p=capsule_example(tmp_path,mutation)
    with pytest.raises(ValueError):cp.inspect_capsule(p,op.sha(p))
