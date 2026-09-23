import copy,io,json,os,shutil,subprocess,sys
from pathlib import Path
import joblib,numpy as np,pytest
from threadpoolctl import threadpool_limits
ROOT=Path(__file__).resolve().parent
TRAD=Path(os.environ.get('FOREX_INTERACTION_TRAD',str(ROOT.parent/'trad')))
sys.path.insert(0,str(TRAD));sys.path.insert(0,str(ROOT))
import interaction_operator_v2 as op
from interaction_models_v2 import contract,fit_pair,predict,frame,transform_state,validate_fit,score_chunk,GROUPS
from interaction_runner_v2 import load_inputs,identity_for,required,checked
from contracts import validate_forecast,fingerprint
from publication import RunPublisher
E=ROOT/'evidence/timed_20260922_022952'
RICH=Path(os.environ.get('FOREX_INTERACTION_RICH',str(E/'rich_campaign_inputs_step/runs/rich-campaign-inputs')))
BASE=Path(os.environ.get('FOREX_INTERACTION_BASE',str(E/'matched_remaining_step/campaign_reissued/runs_v2/matched-development-campaign')))
RECIPE=ROOT/'INTERACTION_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),RICH,BASE,TRAD,runs)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_INTERACTION_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('family-baseline');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])
@pytest.fixture(scope='session')
def inputs():return load_inputs(RICH,read(RECIPE))
def fit_files(root,group='legacy26_interactions10',minutes=60,cutoff=1721606400):
    n=f'fit_{group}_{minutes}_{cutoff}';return read(root/(n+'.json')),{m:(root/(n+'_'+m+'.joblib')).read_bytes() for m in ('ridge','recovered_hgb')}

def test_all_declared_attempts_and_original_baseline_preserved(completed):
    r=read(completed/'run_report.json');assert r['models_fitted']==32 and r['models_reused']==28 and r['new_score_groups']==28
    assert r['new_forecast_rows']==37744 and r['new_coverage_rows']==38080 and r['selected_model'] is None
    b=read(completed/'baseline_reference.json');assert b['baseline_refitted'] is False and b['scores']==read(BASE/'scores.json')
    assert all(op.sha(BASE/n)==h for n,h in b['payloads'].items())
    assert len(read(completed/'COMPLETION_MANIFEST.json')['payloads'])==len(required())==77
    assert read(completed/'experiment_contract.json')=={**contract(),'ordered_feature_sets':read(RECIPE)['feature_sets']}

def test_post_payload_resume_reuses_fixture_and_exact_timing(completed,tmp_path,monkeypatch):
    cmd=[sys.executable,'-I','-B',str(ROOT/'interaction_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--rich',str(RICH),'--baseline',str(BASE),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after','0']
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=900);assert p.returncode==91,p.stdout+p.stderr
    run=tmp_path/read(RECIPE)['run_id'];before=(run/'fit_resources.json').read_bytes()
    import interaction_positive_fixture_v2 as fixture
    def forbidden():raise AssertionError('completed synthetic fixture must not be refitted on post-payload resume')
    monkeypatch.setattr(fixture,'build',forbidden)
    assert call('resume',tmp_path)['status']=='completed_verified'
    assert (run/'fit_resources.json').read_bytes()==before
    assert (run/'interaction_positive_fixture.json').read_bytes()==(completed/'interaction_positive_fixture.json').read_bytes()

def test_every_fit_matches_original_mature_population_and_transforms(completed):
    for m in read(completed/'model_inventory.json'):
        h=m['target']['horizon_seconds']//60;old=read(BASE/f"fit_{h}_{m['fit_cutoff']}.json")
        assert m['training_population_sha256']==old['training_population_sha256'] and m['training_rows']==old['training_rows']
        assert m['maximum_outcome_available_epoch']<=m['fit_cutoff'] and m['baseline_fit_id']==old['fit_id']
        meta,models=fit_files(completed,m['group'],h,m['fit_cutoff']);validate_fit(meta,models)
        assert all(transform_state(joblib.load(io.BytesIO(b)),m['feature_names'])==m['fitted_transform'] for b in models.values())
        assert m['fitted_transform']['training_rows']==m['training_rows']

def test_future_features_labels_do_not_change_prefix_fit(inputs):
    obs,out,views=inputs;c=contract();cut=c['fit_cutoffs'][0];base=read(BASE/f'fit_60_{cut}.json');names=read(RECIPE)['feature_sets']['legacy26_interactions10']
    a,ma=fit_pair(obs,out,views,'legacy26_interactions10',names,60,cut,base,c)
    changed=copy.deepcopy(out);changed_obs=copy.deepcopy(obs);changed_views=copy.deepcopy(views)
    for o in changed:
        if o['available_epoch']>cut:o['value']={'future':'poison'}
    for o in changed_obs:
        if o['origin_epoch']>=cut:
            o['features']=['future poison'];changed_views[o['record_id']]={'poison':'must not inspect'}
    b,mb=fit_pair(changed_obs,changed,changed_views,'legacy26_interactions10',names,60,cut,base,c)
    assert a==b and ma==mb

def test_train_only_imputation_hand_calculation_and_empty_constant_columns():
    from sklearn.pipeline import Pipeline
    from sklearn.linear_model import Ridge
    import retained_signed_cost_models_v1 as recovered
    names=['varies','constant','empty'];x=frame([[1,5,None],[None,5,None],[3,5,None]],names)
    model=Pipeline(recovered.pipeline(False,contract()['hgb_parameters']).steps[:-1]+[('ridge',Ridge(alpha=20.,solver='cholesky'))])
    model.fit(x,np.array([1.,2.,3.]));state=transform_state(model,names)
    assert state['imputer_statistics']==[2.,5.,0.] and state['indicator_indices']==[0,2]
    np.testing.assert_allclose(state['scaler_mean'],[2.,5.,0.,1/3,1.])
    assert state['transformed_width']==5 and state['scaler_scale'][1]==1 and state['scaler_scale'][2]==1
    before=copy.deepcopy(state);pred=model.predict(frame([[10000,5,999]],names));assert np.isfinite(pred).all() and transform_state(model,names)==before

@pytest.mark.parametrize('bad',[[[float('inf')]],[[1,2]]])
def test_invalid_feature_matrix_refused(bad):
    with pytest.raises(ValueError,match='registered_features'):frame(bad,['a'])

def test_population_drift_refused_before_fit(inputs):
    obs,out,views=inputs;c=contract();base=read(BASE/'fit_60_1721606400.json');base['training_population_sha256']='0'*64
    with pytest.raises(ValueError,match='baseline_training_population_mismatch'):fit_pair(obs,out,views,'legacy26_interactions10',read(RECIPE)['feature_sets']['legacy26_interactions10'],60,c['fit_cutoffs'][0],base,c)

def test_model_and_metadata_tamper_refused_before_unpickle(completed,inputs):
    meta,models=fit_files(completed);models['ridge']=b'not a pickle'
    with pytest.raises(ValueError,match='model_bytes_changed'):predict(meta,models,inputs[0][:1],inputs[2],procedure='frozen',c=contract())
    meta['fit_cutoff']+=1
    with pytest.raises(ValueError,match='metadata_identity_mismatch'):validate_fit(meta,models)

def test_early_model_and_feature_readiness(completed,inputs):
    meta,models=fit_files(completed);r=copy.deepcopy(next(x for x in inputs[0] if x['features'] is not None))
    r['origin_epoch']=meta['ready_epoch']-1;r['available_epoch']=r['origin_epoch'];f,cov=predict(meta,models,[r],inputs[2],procedure='frozen',c=contract());assert not f and {x['reason'] for x in cov}=={'model_not_ready'}
    r['origin_epoch']=meta['ready_epoch']+1;r['available_epoch']=r['origin_epoch']+1;f,cov=predict(meta,models,[r],inputs[2],procedure='frozen',c=contract());assert not f and {x['reason'] for x in cov}=={'feature_not_ready'}

def test_forecast_schema_clocks_and_no_outcome_access(completed):
    for p in completed.glob('forecasts_*.json'):
        for r in read(p):
            f=r['forecast'];validate_forecast(f);assert not any('outcome' in k for k in r)
            expected=1721779230 if r['procedure']=='adaptive' and f['decision_epoch']>=1721779260 else 1721606430
            assert f['model_ready_epoch']==expected and f['available_epoch']==f['decision_epoch']+2

def test_scores_recompute_and_match_all_baseline_support(completed,inputs):
    base=read(BASE/'scores.json');scores=[]
    for p in sorted(completed.glob('forecasts_*.json')):scores.extend(score_chunk(read(p),inputs[1],base,contract()))
    key=lambda s:(s['group'],s['target_id'],s['procedure'],s['method'])
    assert sorted(scores,key=key)==sorted(read(completed/'scores.json'),key=key)

def test_one_forecast_matches_direct_fitted_pipeline(completed,inputs):
    meta,models=fit_files(completed);rows=read(completed/'forecasts_legacy26_interactions10_60_frozen.json');obs={x['record_id']:x for x in inputs[0]}
    for row in rows[:2]:
        v=inputs[2][row['record_id']]['legacy26_interactions10'];model=joblib.load(io.BytesIO(models[row['method']]))
        with threadpool_limits(limits=1):value=model.predict(frame([v['values']],v['feature_names']))[0]
        assert row['forecast']['prediction']==pytest.approx(value,rel=1e-12,abs=1e-12)

def test_source_drift_before_numerical_import(tmp_path,monkeypatch):
    for n in op.SOURCES:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/'interaction_models_v2.py').write_text("raise RuntimeError('MUST_NOT_IMPORT')")
    monkeypatch.setattr(op,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='source_drift_before_import'):op.preflight(RECIPE,op.sha(RECIPE),RICH,BASE,TRAD)

def test_consumed_dependency_guard(tmp_path):
    (tmp_path/'scores.json').write_text('[]')
    with pytest.raises(ValueError,match='consumed_dependency_changed'):checked(tmp_path,read(RECIPE)['baseline'],'scores.json')

def test_one_writer_and_false_completion(tmp_path):
    p=RunPublisher(tmp_path,read(RECIPE)['run_id'],identity_for(read(RECIPE)));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()
    assert call('verify',tmp_path)['status']=='review_required'

@pytest.mark.parametrize('boundary',[1,0])
def test_real_process_death_resume_exact_scientific_payloads(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'interaction_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--rich',str(RICH),'--baseline',str(BASE),'--trad-root',str(TRAD),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=900);assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable'
    resume=cmd[:-2];resume[4]='resume';q=subprocess.run(resume,capture_output=True,text=True,timeout=900);assert q.returncode==0,q.stdout+q.stderr
    hashes=lambda path:{x['path']:x['sha256'] for x in read(path/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='fit_resources.json'}
    assert hashes(completed)==hashes(tmp_path/completed.name)


def test_synthetic_interaction_is_detected_without_marginal_screen(completed):
    f=read(completed/'interaction_positive_fixture.json')
    assert f['scope']=='synthetic_algorithm_positive_control_not_market_evidence'
    assert f['training_rows']==256 and f['held_forward_rows']==32 and f['synthetic_learner_fits']==4
    assert f['marginal_feature_outcome_cross_moments']==[0.,0.] and f['marginal_screen_used'] is False
    linear=next(x for x in f['metrics'] if x['family']=='marginal_baseline' and x['method']=='ridge')
    joint=next(x for x in f['metrics'] if x['family']=='explicit_interaction' and x['method']=='ridge')
    assert linear['mse']==1. and joint['mse']==pytest.approx((20/276)**2,rel=1e-12)
    assert f['positive_control_detected'] and not f['independent_market_confirmation']

def test_fixed_products_and_original_fields_reused_without_screen(inputs):
    from interaction_features_v2 import PAIRS,names_for,expand
    obs,out,views=inputs;legacy=read(RECIPE)['legacy_features'];group=GROUPS[0]
    assert len(PAIRS)==10 and len(names_for(group,legacy))==36
    for row in obs:
        v=views[row['record_id']][group]
        assert v['values'][:26]==(row['features'] if row['features'] is not None else [None]*26)
        if row['features'] is not None:
            by=dict(zip(legacy,row['features']));assert v['values'][26:]==[by[a]*by[b] for a,b in PAIRS]
        else:assert v['values']==[None]*36 and not v['shared_legacy_population_eligible']
    synthetic=[0.]*26;synthetic[legacy.index(PAIRS[0][0])]=2.;synthetic[legacy.index(PAIRS[0][1])]=-3.
    assert expand(synthetic,legacy)[26]==-6.

def test_market_and_synthetic_attempts_remain_separate(completed):
    r=read(completed/'run_report.json');assert r['market_models_fitted']==28 and r['synthetic_models_fitted']==4 and r['models_fitted']==32
    assert len(read(completed/'model_inventory.json'))==14
    resources=read(completed/'fit_resources.json');assert len(resources)==15
    assert sum(x['scope']=='synthetic_baseline_and_interaction_four_learner_fits' for x in resources)==1


def test_isolated_preflight_resolves_source_without_numerical_imports(tmp_path):
    code="""import sys,builtins,json
from pathlib import Path
sys.path.insert(0,sys.argv[1]);import interaction_operator_v2 as op
sys.path.remove(sys.argv[1]);original=builtins.__import__
def guard(name,*args,**kwargs):
 if name.split('.')[0] in {'numpy','pandas','scipy','sklearn','joblib'}:raise AssertionError('numerical_import_before_preflight:'+name)
 return original(name,*args,**kwargs)
builtins.__import__=guard
r=op.preflight(Path(sys.argv[2]),sys.argv[3],Path(sys.argv[4]),Path(sys.argv[5]),Path(sys.argv[6]))
assert r['run_id']=='matched-fixed-interactions'
assert not any(x.split('.')[0] in {'numpy','pandas','scipy','sklearn','joblib'} for x in sys.modules)
print('isolated standard-library preflight verified')
"""
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),op.sha(RECIPE),str(RICH),str(BASE),str(TRAD)],capture_output=True,text=True,cwd=tmp_path,timeout=60)
    assert p.returncode==0,p.stdout+p.stderr
