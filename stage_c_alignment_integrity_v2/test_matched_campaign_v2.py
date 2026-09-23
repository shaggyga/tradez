import copy,json,os,sys,shutil,subprocess
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import matched_campaign_operator_v2 as op
from matched_campaign_models_v2 import contract,fit_pair,predict,score
from matched_campaign_runner_v2 import load_inputs,identity_for
from contracts import validate_forecast
from publication import RunPublisher
INPUT=Path(os.environ.get('FOREX_MATCHED_INPUT',str(ROOT/'evidence/timed_20260922_022952/technical_adapter_step/runs_v2/causal-technical-inputs')))
RECIPE=ROOT/'MATCHED_CAMPAIGN_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text())
def call(action,runs,recipe=RECIPE,root=INPUT):return op.operate(action,recipe,op.sha(recipe),root,runs)
@pytest.fixture(scope='session')
def baseline(tmp_path_factory):
    supplied=os.environ.get('FOREX_MATCHED_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('matched-baseline');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r
    return runs/read(RECIPE)['run_id']
@pytest.fixture(scope='session')
def inputs():return load_inputs(INPUT,read(RECIPE))
def test_all68_seven_targets_and_controls(baseline):
    r=read(baseline/'run_report.json');assert r['coverage_rows']==76160 and r['forecast_rows']==75488 and r['models_fitted']==28 and r['score_groups']==56
    assert r['selected_model'] is None and r['policy_frames']==0
    assert all(v.startswith('blocked') for v in r['calendar_target_status'].values())

def test_fitted_serialization_ignores_equal_string_aliases(baseline,inputs):
    import io,joblib,numpy as np,pandas as pd
    from threadpoolctl import threadpool_limits
    from matched_campaign_models_v2 import canonicalize_model_strings,FEATURES
    raw=(baseline/'fit_60_1721606400.joblib').read_bytes()
    a=joblib.load(io.BytesIO(raw));b=joblib.load(io.BytesIO(raw))
    # Reproduce warm-process loading versus fresh-process default string aliases.
    a[-1].scoring=next(k for k in a[-1].__dict__ if k=='loss')
    b[-1].scoring=bytes([108,111,115,115]).decode()
    assert a[-1].scoring==b[-1].scoring
    obs,_=inputs;x=pd.DataFrame([o['features'] for o in obs if o['features'] is not None][:68],columns=FEATURES)
    with threadpool_limits(limits=1):before=a.predict(x)
    saved=[]
    for model in (a,b):
        canonicalize_model_strings(model);buf=io.BytesIO();joblib.dump(model,buf,compress=0,protocol=5);saved.append(buf.getvalue())
        with threadpool_limits(limits=1):np.testing.assert_array_equal(before,model.predict(x))
    assert saved[0]==saved[1]
def test_exact_matched_training_population_and_maturity(baseline):
    for p in baseline.glob('fit_*.json'):
        if p.name=='fit_resources.json':continue
        m=read(p);assert m['training_population_sha256']==m['ridge']['training_population_sha256']
        assert m['training_rows']==m['ridge']['training_rows'] and m['maximum_outcome_available_epoch']<=m['fit_cutoff']
        assert m['tree_parameters']['early_stopping'] is False and m['equal_training_population_and_measure'] is True

def test_future_data_cannot_change_prior_fit(inputs):
    obs,out=inputs;c=contract();universe=read(RECIPE)['universe'];a,tree_a=fit_pair(obs,out,universe,60,c['fit_cutoffs'][0],c)
    changed=copy.deepcopy(out)
    for o in changed:
        if o['available_epoch']>c['fit_cutoffs'][0]:o['value']={'future':'must not inspect'}
    changed_obs=copy.deepcopy(obs)
    for o in changed_obs:
        if o['origin_epoch']>=c['fit_cutoffs'][0]:o['features']=['future poison']
    b,tree_b=fit_pair(changed_obs,changed,universe,60,c['fit_cutoffs'][0],c)
    assert a==b and tree_a==tree_b

def test_model_readiness_prevents_early_issue(baseline,inputs):
    m=read(baseline/'fit_60_1721606400.json');tree=(baseline/'fit_60_1721606400.joblib').read_bytes();r=copy.deepcopy(next(o for o in inputs[0] if o['features'] is not None));r['origin_epoch']=m['ready_epoch']-1;r['available_epoch']=r['origin_epoch']
    f,cov=predict(m,tree,[r],procedure='frozen',c=contract());assert not f and all(x['reason']=='model_not_ready' for x in cov)

def test_forecast_schema_has_no_future_labels(baseline):
    rows=read(baseline/'forecasts.json')
    for r in rows:validate_forecast(r['forecast'])
    assert not any('outcome' in k for r in rows for k in r)

def test_frozen_and_scheduled_update_clocks(baseline):
    rows=read(baseline/'forecasts.json');inventory=read(baseline/'model_inventory.json');ready={x['ready_epoch'] for x in inventory}
    assert len(ready)==2
    assert {r['forecast']['model_ready_epoch'] for r in rows if r['procedure']=='frozen'}=={1721606430}
    assert {r['forecast']['model_ready_epoch'] for r in rows if r['procedure']=='adaptive' and r['forecast']['decision_epoch']>=1721779260}=={1721779230}

def test_scoring_reconciles_and_support_matches(baseline,inputs):
    scores=score(read(baseline/'forecasts.json'),inputs[1],contract());assert scores==read(baseline/'scores.json')
    grouped={}
    for s in scores:grouped.setdefault((s['target_id'],s['procedure']),set()).add((s['rows'],s['support_sha256']))
    assert all(len(v)==1 for v in grouped.values())

def test_tree_tamper_refused_before_load(baseline,inputs):
    m=read(baseline/'fit_60_1721606400.json')
    with pytest.raises(ValueError,match='tree_identity'):predict(m,b'not a pickle',inputs[0][:1],procedure='frozen',c=contract())

def test_status_and_verify_without_run(tmp_path):
    assert call('status',tmp_path/'runs')['status']=='ready' and not (tmp_path/'runs').exists()
    assert call('verify',tmp_path/'runs')['status']=='review_required'

def test_source_drift_before_import(tmp_path):
    s=tmp_path/'source';s.mkdir()
    for n in op.SOURCES:shutil.copyfile(ROOT/n,s/n)
    shutil.copyfile(RECIPE,s/RECIPE.name);(s/'matched_campaign_models_v2.py').write_text("raise RuntimeError('MUST_NOT_IMPORT')\n")
    p=subprocess.run([sys.executable,'-I',str(s/'matched_campaign_operator_v2.py'),'run','--recipe',str(s/RECIPE.name),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True,timeout=30)
    assert p.returncode==2 and 'before_import' in p.stdout and 'MUST_NOT_IMPORT' not in p.stdout+p.stderr

def test_input_drift_refused(tmp_path):
    root=tmp_path/'inputs';shutil.copytree(INPUT,root);(root/'pair_EUR_USD.json').write_text('{}')
    assert call('run',tmp_path/'runs',root=root)['status']=='review_required'

def test_completed_payload_tamper(baseline,tmp_path):
    root=tmp_path/baseline.name;shutil.copytree(baseline,root);(root/'scores.json').write_text('[]');assert call('verify',tmp_path)['status']=='review_required'

def test_fit_latency_receipts_bounded(baseline):
    resources=read(baseline/'fit_resources.json');assert len({x['fit_id'] for x in resources})==14 and all(0<=x['elapsed_seconds']<=30 for x in resources)

def test_writer_exclusion(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_real_process_death_resume_exact_scientific_payloads(baseline,tmp_path,boundary):
    p=subprocess.run([sys.executable,str(ROOT/'matched_campaign_runner_v2.py'),'--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--input-root',str(INPUT),'--runs-dir',str(tmp_path),'--crash-after',str(boundary)],capture_output=True,text=True,timeout=180)
    assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable' and call('resume',tmp_path)['status']=='completed_verified'
    hashes=lambda p:{x['path']:x['sha256'] for x in read(p/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='fit_resources.json'}
    assert hashes(baseline)==hashes(tmp_path/baseline.name)

def test_batched_ridge_matches_existing_scalar_consumer(baseline,inputs):
    from fitted_consumer_v2 import issue
    m=read(baseline/'fit_60_1721606400.json');tree=(baseline/'fit_60_1721606400.joblib').read_bytes()
    obs=[o for o in inputs[0] if o['origin_epoch']==1721606460 and o['features'] is not None]
    f,_=predict(m,tree,obs,procedure='frozen',c=contract());by={r['record_id']:r['forecast']['prediction'] for r in f if r['method']=='ridge'}
    for o in obs:
        scalar,reason=issue(m['ridge'],o,decision_epoch=o['origin_epoch'],available_epoch=o['origin_epoch']+2,procedure='frozen')
        assert reason=='eligible' and by[o['record_id']]==pytest.approx(scalar['prediction'],rel=1e-12,abs=1e-12)
