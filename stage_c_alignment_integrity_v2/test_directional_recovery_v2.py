"""Real retained recovery, refusal, publication and killed-process regressions."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import directional_recovery_operator_v2 as op
from directional_recovery_runner_v2 import identity_for
from publication import RunPublisher,verify_completed_run
RETAINED=Path(os.environ.get('FOREX_DIRECTIONAL_RETAINED',str(ROOT.parent/'direction_decision_20260911/local_restoration_001')))
RECIPE=ROOT/'DIRECTIONAL_RECOVERY_OPERATOR_RECIPE.json'

def read(p):return json.loads(p.read_text())
def call(action,runs,recipe=RECIPE,digest=None,root=RETAINED):return op.operate(action,recipe,digest or op.sha(recipe),root,runs)

@pytest.fixture(scope='session')
def baseline(tmp_path_factory):
    supplied=os.environ.get('FOREX_DIRECTIONAL_RUNS')
    runs=Path(supplied) if supplied else tmp_path_factory.mktemp('directional-baseline')
    receipt=call('verify' if supplied else 'run',runs)
    assert receipt['status']=='completed_verified',receipt
    return runs/read(RECIPE)['run_id']

def test_original_and_label_free_recreation(baseline):
    old=read(baseline/'original_recreation.json');r=read(baseline/'run_report.json')
    assert old['maximum_absolute_difference']==0 and old['numeric_values_recreated']==718074
    assert old['replay_heads_checked']==1536 and old['actual_forecasts_published']==0
    assert r['label_free_numeric_values_recreated']==718074 and r['models_refitted']==0
    assert r['estimators_recovered']==48 and r['calibrators_recovered']==8

def test_all68_coverage_keeps_missing(baseline):
    c=read(baseline/'native_coverage.json');r=read(baseline/'run_report.json')
    assert len({x['instrument'] for x in c})==68
    assert len(c)==175*68*4 and sum(x['status']=='retained_assessment_row' for x in c)==39893
    assert r['missing_coverage_rows']==7707

def test_no_unsupported_horizon_or_campaign_admission(baseline):
    a=read(baseline/'campaign_admission.json');native={t['target'] for t in a['targets'] if t['native_saved_horizon_present']}
    assert native=={'15m','1h'} and not any(t['new_campaign_admitted'] for t in a['targets'])
    assert a['refit_performed'] is False

def test_fitted_parameters_and_column_counts(baseline):
    models=read(baseline/'model_inventory.json')
    assert {m['feature_count'] for m in models}=={94,178}
    assert all(e['early_stopping'] is False and e['iterations']==100 for m in models for e in m['estimators'])
    assert all(not m['fit_ready_observed'] for m in models)

def test_feature_population_not_registry_names_only(baseline):
    f=read(baseline/'feature_population.json')
    assert len(f)==178*3 and all(r['rows']==r['finite_rows']+r['missing_rows'] for r in f)
    assert any(r['distinct_finite_values']>2 for r in f if r['feature_family']=='tech')

def test_status_does_not_run(tmp_path):
    r=call('status',tmp_path/'runs');assert r['status']=='ready' and not (tmp_path/'runs').exists()

def test_verify_requires_completion(tmp_path):
    r=call('verify',tmp_path/'runs');assert r['status']=='review_required' and r['reason']=='completed_run_required'

def test_bad_external_recipe_hash(tmp_path):
    r=call('run',tmp_path/'runs',digest='0'*64);assert r['status']=='review_required' and not (tmp_path/'runs').exists()

def test_environment_drift_before_import(tmp_path):
    r=read(RECIPE);r['environment']['numpy']='0.0.invalid';p=tmp_path/'recipe.json';p.write_text(json.dumps(r))
    result=call('run',tmp_path/'runs',recipe=p)
    assert result['status']=='review_required' and 'before_runtime_import' in result['reason']
    assert not (tmp_path/'runs').exists()

def test_source_drift_before_runtime_import(tmp_path):
    source=tmp_path/'source';source.mkdir()
    for n in op.SOURCES:shutil.copyfile(ROOT/n,source/n)
    shutil.copyfile(RECIPE,source/RECIPE.name)
    (source/'directional_recovery_audit_v2.py').write_text("raise RuntimeError('MUST_NOT_IMPORT')\n")
    cmd=[sys.executable,str(source/'directional_recovery_operator_v2.py'),'run','--recipe',str(source/RECIPE.name),'--recipe-sha256',op.sha(RECIPE),'--retained-root',str(RETAINED),'--runs-dir',str(tmp_path/'runs')]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=30)
    assert p.returncode==2 and 'before_runtime_import' in p.stdout and 'MUST_NOT_IMPORT' not in p.stdout+p.stderr
    assert not (tmp_path/'runs').exists()

def test_retained_model_drift_refused_before_deserialization(tmp_path):
    root=tmp_path/'retained';shutil.copytree(RETAINED,root)
    artifact=root/'direction_decision_20260911/evaluation_001/h15_combined.joblib'
    artifact.write_bytes(b'not a model')
    r=call('run',tmp_path/'runs',root=root)
    assert r['status']=='review_required' and 'retained_file_drift' in r['reason']
    assert not (tmp_path/'runs').exists()

@pytest.mark.parametrize('name',['native_coverage.json','original_recreation.json'])
def test_completed_payload_tamper(baseline,tmp_path,name):
    root=tmp_path/baseline.name;shutil.copytree(baseline,root);(root/name).write_text('{}')
    r=call('verify',tmp_path);assert r['status']=='review_required'

def test_completion_inventory_tamper(baseline,tmp_path):
    root=tmp_path/baseline.name;shutil.copytree(baseline,root);p=root/'COMPLETION_MANIFEST.json';m=read(p);m['required_payloads'].pop();p.write_text(json.dumps(m))
    assert call('verify',tmp_path)['status']=='review_required'

def test_writer_exclusion(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:
        result=call('run',tmp_path);assert result['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_real_process_death_and_exact_resume(baseline,tmp_path,boundary):
    command=[sys.executable,str(ROOT/'directional_recovery_runner_v2.py'),'--retained-root',str(RETAINED),'--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--runs-dir',str(tmp_path),'--crash-after',str(boundary)]
    p=subprocess.run(command,capture_output=True,text=True,timeout=180);assert p.returncode==91,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable'
    assert call('resume',tmp_path)['status']=='completed_verified'
    root=tmp_path/baseline.name
    a={x['path']:x['sha256'] for x in read(baseline/'COMPLETION_MANIFEST.json')['payloads']}
    b={x['path']:x['sha256'] for x in read(root/'COMPLETION_MANIFEST.json')['payloads']}
    assert a==b

def test_isolated_python_entrypoint(tmp_path):
    command=[sys.executable,'-I','-B',str(ROOT/'directional_recovery_operator_v2.py'),'status','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--retained-root',str(RETAINED),'--runs-dir',str(tmp_path/'runs')]
    p=subprocess.run(command,capture_output=True,text=True,timeout=30)
    assert p.returncode==0 and json.loads(p.stdout)['status']=='ready',p.stdout+p.stderr
