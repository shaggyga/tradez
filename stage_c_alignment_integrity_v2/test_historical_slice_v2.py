"""Causal, recovery and refusal tests on the pinned prepared development inputs."""
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from historical_slice_v2 import load_inputs,fit_one,issue_all,settle,scores,policy_admission,PROCEDURES,control_model,validate_contract
from historical_runner_v2 import run,jobs

INPUT=Path(os.environ.get('FOREX_HISTORICAL_TEST_INPUT',str(ROOT/'evidence/timed_20260922_001617/historical_preflight')))
CONTRACT=ROOT/'HISTORICAL_SLICE_CONTRACT.json'

@pytest.fixture(scope='module')
def data():
    c=json.loads(CONTRACT.read_text());o,y,_=load_inputs(INPUT,c)
    m=[fit_one(o,y,c,t,k) for t,k in jobs(c)]
    return c,o,y,m,issue_all(o,m,c)

def test_actual_history_has_all68_coverage_and_no_outcome_selected_issuance(data):
    c,o,y,m,r=data
    assert len(m)==9 and all(x['status']=='fitted' for x in m)
    assert len(r['coverage'])==28*68*3*4
    assert {x['instrument'] for x in r['coverage']}==set(c['universe'])
    assert any(x['reason']=='missing_origin_observation' for x in r['coverage'])
    assert all('actual_bps' not in x for x in r['forecasts'])
    assert issue_all(o,m,c)==r  # issuance function has no outcomes argument

def test_future_label_poison_does_not_change_first_model_or_early_forecasts(data):
    c,o,y,m,r=data;cutoff=c['fit_cutoffs'][0];poison=deepcopy(y)
    for row in poison:
        if row['available_epoch']>cutoff:row['value']=1e10
    original=[model for model in m if model['training_view']['fit_cutoff_epoch']==cutoff]
    after=[fit_one(o,poison,c,t,cutoff) for t in c['targets']]
    assert original==after
    a=issue_all(o,original,c);b=issue_all(o,after,c);assert a==b

def test_training_transforms_and_control_mean_use_only_mature_population(data):
    c,o,y,m,r=data;model=m[0];cutoff=c['fit_cutoffs'][0];target=c['targets'][0]
    by_id={row['record_id']:row for row in o}
    labels=[row['value'] for row in y if row['target_id']==target['target_id'] and row['available_epoch']<=cutoff and row['value'] is not None
      and by_id[row['record_id']]['features'] is not None and c['training_origin_start']<=by_id[row['record_id']]['origin_epoch']<cutoff]
    assert model['training_rows']==len(labels)==2303
    assert model['coefficient'][0]==pytest.approx(sum(labels)/len(labels),abs=1e-10)
    assert all(x==0 for x in control_model(model,'no_change')['coefficient'])
    assert all(x==0 for x in control_model(model,'training_mean')['coefficient'][1:])

def test_frozen_stays_frozen_and_adaptive_waits_for_readiness(data):
    c,o,y,m,r=data
    for target in c['targets']:
        first=next(x for x in m if x['target']==target and x['training_view']['fit_cutoff_epoch']==c['fit_cutoffs'][0])
        fids={x['forecast_id'] for x in r['coverage'] if x['target_id']==target['target_id'] and x['procedure']=='frozen'}
        assert {x['model_id'] for x in r['forecasts'] if x['forecast_id'] in fids}=={first['model_id']}
    pending=deepcopy(c);pending['decision_epochs']=[c['fit_cutoffs'][1]];pending['fit_cutoffs']=c['fit_cutoffs'][:2]
    rows=[dict(x,origin_epoch=c['fit_cutoffs'][1],available_epoch=c['fit_cutoffs'][1]) for x in o if x['origin_epoch']==c['fit_cutoffs'][1]+60]
    rr=issue_all(rows,m,pending)
    adaptive={x['forecast_id'] for x in rr['coverage'] if x['procedure']=='adaptive'}
    assert all(next(mm for mm in m if mm['model_id']==f['model_id'])['training_view']['fit_cutoff_epoch']==c['fit_cutoffs'][0]
      for f in rr['forecasts'] if f['forecast_id'] in adaptive)

def test_settlement_cutoff_changes_scores_not_predictions(data):
    c,o,y,m,r=data;earlier=deepcopy(c);earlier['evaluation_asof_epoch']=c['decision_epochs'][0]
    s=settle(r['forecasts'],r['coverage'],y,earlier)
    assert {x['status'] for x in s}=={'right_censored_at_evaluation_asof'}
    assert all(x['actual_bps'] is None for x in s)
    assert all(x['matched_rows']==0 for x in scores(s,c))

def test_score_arithmetic_uses_exact_matched_population(data):
    c,*_=data;t=c['targets'][0]['target_id'];rows=[]
    for p in PROCEDURES:
        rows += [dict(target_id=t,procedure=p,instrument='EUR_USD',decision_epoch=100,status='matured',prediction_bps=2,actual_bps=5),
                 dict(target_id=t,procedure=p,instrument='GBP_USD',decision_epoch=100,status='matured',prediction_bps=1,actual_bps=-3)]
    rows.append(dict(target_id=t,procedure='adaptive',instrument='AUD_USD',decision_epoch=100,status='matured',prediction_bps=1000,actual_bps=0))
    for row in scores(rows,c)[:4]:
        assert row['matched_rows']==2 and row['mae_bps']==3.5 and row['rmse_bps']==pytest.approx((25/2)**.5)

def test_policy_admission_does_not_invent_executable_forecast(data):
    c,*_=data;r=policy_admission(c)
    assert r['status']=='blocked' and r['eligible_policy_frames']==0 and len(r['required'])==4

@pytest.mark.parametrize('change',['mode','broker','target','schedule','schema'])
def test_configuration_refusals(data,change):
    c=deepcopy(data[0])
    if change=='mode':c['mode']='offline_synthetic_qualification'
    if change=='broker':c['broker_access']=True
    if change=='target':c['targets'][0]['horizon_seconds']=1
    if change=='schedule':c['fit_cutoffs'].reverse()
    if change=='schema':c['feature_schema'].reverse()
    with pytest.raises(ValueError):validate_contract(c)

@pytest.mark.parametrize('boundary',[1,5,9,0])
def test_process_crash_resume_keeps_fit_artifacts_and_scientific_outputs(data,tmp_path,boundary):
    c,*_=data
    cmd=[sys.executable,'-I','-B',str(ROOT/'historical_runner_v2.py'),'--input-root',str(INPUT),'--contract',str(CONTRACT),
      '--runs-dir',str(tmp_path),'--run-id','partial']
    crashed=subprocess.run(cmd+['--test-crash-after-fit',str(boundary)],capture_output=True,text=True,timeout=60)
    assert crashed.returncode==91,crashed.stderr
    cached={p.name:p.read_bytes() for p in (tmp_path/'partial').glob('fit_*.json')}
    resumed=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=60)
    assert resumed.returncode==0,resumed.stderr
    assert all((tmp_path/'partial'/n).read_bytes()==raw for n,raw in cached.items())
    run(INPUT,c,run_id='clean',runs_dir=tmp_path)
    for p in (tmp_path/'clean').glob('*'):
        if p.name in {'models.json','run_report.json','policy_admission.json'} or p.name.startswith(('forecasts_','coverage_','settlements_')):
            assert p.read_bytes()==(tmp_path/'partial'/p.name).read_bytes()

def test_resume_after_last_fit_never_refits(data,tmp_path,monkeypatch):
    import historical_runner_v2 as runner
    c,*_=data
    cmd=[sys.executable,'-I','-B',str(ROOT/'historical_runner_v2.py'),'--input-root',str(INPUT),'--contract',str(CONTRACT),
      '--runs-dir',str(tmp_path),'--run-id','partial','--test-crash-after-fit','9']
    assert subprocess.run(cmd,capture_output=True,timeout=60).returncode==91
    monkeypatch.setattr(runner,'fit_one',lambda *a:pytest.fail('completed fit repeated'))
    assert runner.run(INPUT,c,run_id='partial',runs_dir=tmp_path,resume=True)['status']=='completed'

def test_operator_frozen_recipe_and_corrupt_output_refusal(data,tmp_path):
    from historical_operator_v2 import operate,sha
    recipe=ROOT/'HISTORICAL_OPERATOR_RECIPE.json';h=sha(recipe)
    assert operate('status',recipe,h,INPUT,CONTRACT,tmp_path)['status']=='ready'
    r=operate('run',recipe,h,INPUT,CONTRACT,tmp_path);assert r['status']=='completed_verified',r
    assert operate('verify',recipe,h,INPUT,CONTRACT,tmp_path)['status']=='completed_verified'
    (Path(r['run_path'])/'models.json').write_text('[]')
    assert operate('verify',recipe,h,INPUT,CONTRACT,tmp_path)['status']=='review_required'

def test_operator_refuses_source_drift_before_runtime_import(tmp_path):
    from historical_operator_v2 import SOURCES,sha
    for name in (*SOURCES,'HISTORICAL_OPERATOR_RECIPE.json','HISTORICAL_SLICE_CONTRACT.json'):shutil.copyfile(ROOT/name,tmp_path/name)
    with (tmp_path/'historical_slice_v2.py').open('a') as f:f.write('\nraise RuntimeError("UNTRUSTED_RUNTIME_EXECUTED")\n')
    recipe=tmp_path/'HISTORICAL_OPERATOR_RECIPE.json'
    r=subprocess.run([sys.executable,'-I','-B',str(tmp_path/'historical_operator_v2.py'),'run','--recipe',str(recipe),'--recipe-sha256',sha(recipe),
      '--input-root',str(INPUT),'--contract',str(tmp_path/'HISTORICAL_SLICE_CONTRACT.json'),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True,timeout=30)
    assert r.returncode==2 and 'drift' in r.stdout and 'UNTRUSTED_RUNTIME_EXECUTED' not in r.stderr and not (tmp_path/'runs').exists()

@pytest.mark.parametrize('which',['contract','input','recipe'])
def test_operator_refuses_changed_external_identity(data,tmp_path,which):
    from historical_operator_v2 import operate,sha
    recipe=ROOT/'HISTORICAL_OPERATOR_RECIPE.json';input_root=INPUT;contract=CONTRACT;digest=sha(recipe)
    if which=='contract':contract=tmp_path/'contract.json';contract.write_bytes(CONTRACT.read_bytes()+b'\n')
    if which=='input':
        input_root=tmp_path/'inputs';input_root.mkdir()
        from historical_runner_v2 import INPUT_NAMES
        for n in INPUT_NAMES:shutil.copyfile(INPUT/n,input_root/n)
        with (input_root/'historical_observations.jsonl').open('a') as f:f.write('\n')
    if which=='recipe':digest='0'*64
    r=operate('run',recipe,digest,input_root,contract,tmp_path/'runs')
    assert r['status']=='review_required' and not (tmp_path/'runs').exists()
