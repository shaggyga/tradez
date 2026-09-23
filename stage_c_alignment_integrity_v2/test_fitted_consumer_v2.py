"""Actual fit/forecast consumer regressions for design B/C, R01–03/R10."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import numpy as np
import pytest
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from fitted_consumer_v2 import evaluate,fit_model,issue
from fitted_fixture_v2 import fixture
from contracts import TrainingView,validate_forecast,fingerprint


def test_real_fits_preserve_all68_coverage_daily_and_multiday():
    data=fixture();r=evaluate(data)
    assert len(r['models'])==9
    assert len(r['coverage'])==2856
    assert len(r['forecasts'])==2442
    assert {m['target']['horizon_seconds'] for m in r['models']}=={86400,172800,432000}
    assert all(m['maximum_outcome_available_epoch']<=m['training_view']['fit_cutoff_epoch'] for m in r['models'])
    assert all(len({x['instrument'] for x in r['coverage'] if x['decision_epoch']==epoch and x['target_id']==target['target_id'] and x['procedure']==procedure})==68
        for epoch in data['contract']['decision_epochs'] for target in data['contract']['targets'] for procedure in ('frozen','adaptive'))
    for f in r['forecasts']:validate_forecast(f)
    assert all('actual' not in f and 'outcome' not in f for f in r['forecasts'])


def test_missing_future_outcomes_do_not_select_forecast_issuance():
    data=fixture();a=evaluate(data)
    last_fit=max(data['contract']['fit_cutoffs'])
    data['outcomes']=[o for o in data['outcomes'] if o['available_epoch']<=last_fit]
    b=evaluate(data)
    assert a['forecasts']==b['forecasts'] and a['coverage']==b['coverage']


def test_future_values_cannot_change_earlier_transform_model_or_forecast():
    data=fixture();a=evaluate(data);cutoff=data['contract']['fit_cutoffs'][0]
    for row in data['observations']:
        if row['origin_epoch']>cutoff+86400:row['features']=[99999,88888,77777]
    for row in data['outcomes']:
        if row['available_epoch']>cutoff:row['value']=1e12
    b=evaluate(data)
    assert [m for m in a['models'] if m['training_view']['fit_cutoff_epoch']==cutoff]==[m for m in b['models'] if m['training_view']['fit_cutoff_epoch']==cutoff]
    assert [f for f in a['forecasts'] if f['decision_epoch']<=cutoff+86400]==[f for f in b['forecasts'] if f['decision_epoch']<=cutoff+86400]


def test_later_adaptive_refit_uses_newly_matured_rows_and_pending_fit_uses_prior_model():
    data=fixture();r=evaluate(data);target=data['contract']['targets'][0]['target_id'];cutoff=data['contract']['fit_cutoffs'][0]
    models=sorted([m for m in r['models'] if m['target']['target_id']==target],key=lambda m:m['ready_epoch'])
    assert models[0]['training_rows']<models[1]['training_rows']<models[2]['training_rows']
    assert models[0]['mean']!=models[1]['mean']
    pending=[f for f in r['forecasts'] if f['target_id']==target and f['decision_epoch']==cutoff+2*86400]
    assert {f['model_id'] for f in pending}=={models[0]['model_id']}
    activated=[f for f in r['forecasts'] if f['target_id']==target and f['decision_epoch']==cutoff+3*86400]
    assert {f['model_id'] for f in activated}=={models[0]['model_id'],models[1]['model_id']}


def test_global_maturity_and_invalid_label_clock_block_leak():
    data=fixture();cutoff=data['contract']['fit_cutoffs'][0];target=data['contract']['targets'][1]
    view=TrainingView(data['contract']['training_origin_start'],cutoff,cutoff,cutoff,cutoff+86400)
    kwargs={'universe':data['universe'],'target':target,'view':view,'ready_epoch':cutoff+60}
    a=fit_model(data['observations'],data['outcomes'],**kwargs)
    assert a['training_rows']==544
    poisoned=deepcopy(data['outcomes'])
    for row in poisoned:
        if row['available_epoch']>cutoff:row['value']=1e99
    assert a==fit_model(data['observations'],poisoned,**kwargs)
    row=next(o for o in poisoned if o['target_id']==target['target_id'] and o['label_end_epoch']>cutoff)
    row['available_epoch']=cutoff
    with pytest.raises(ValueError,match='outcome_available_before_target'):fit_model(data['observations'],poisoned,**kwargs)


def test_model_hash_readiness_and_feature_availability_refusals():
    data=fixture();r=evaluate(data);m=r['models'][0]
    row=next(o for o in data['observations'] if o['origin_epoch']==data['contract']['decision_epochs'][1])
    assert issue(m,row,decision_epoch=m['ready_epoch']-1,available_epoch=m['ready_epoch'],procedure='test')[1]=='model_not_ready'
    row['available_epoch']=row['origin_epoch']+1
    assert issue(m,row,decision_epoch=row['origin_epoch'],available_epoch=row['origin_epoch']+2,procedure='test')[1]=='feature_not_ready'
    m['coefficient'][0]+=1
    with pytest.raises(ValueError,match='model_identity_mismatch'):issue(m,row,decision_epoch=row['origin_epoch'],available_epoch=row['origin_epoch']+2,procedure='test')


def test_input_order_does_not_change_fitted_artifact_or_issuance():
    data=fixture();a=evaluate(data)
    data['observations'].reverse();data['outcomes'].reverse();data['universe'].reverse()
    b=evaluate(data)
    assert a==b


@pytest.mark.parametrize('boundary',[1,3,6,0])
def test_actual_crash_resume_reuses_verified_fit_result(boundary,tmp_path):
    from fitted_runner_v2 import run
    run(fixture(),run_id='clean',runs_dir=tmp_path)
    cmd=[sys.executable,'-I','-B',str(ROOT/'fitted_runner_v2.py'),'--run-id','crash','--runs-dir',str(tmp_path)]
    result=subprocess.run(cmd+['--test-crash-after-payload',str(boundary)],capture_output=True,text=True,timeout=30)
    assert result.returncode==91,result.stderr
    cached=(tmp_path/'crash/qualified_result.json').read_bytes()
    result=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stderr
    assert cached==(tmp_path/'crash/qualified_result.json').read_bytes()
    for row in json.loads((tmp_path/'clean/COMPLETION_MANIFEST.json').read_text())['payloads']:
        assert (tmp_path/'clean'/row['path']).read_bytes()==(tmp_path/'crash'/row['path']).read_bytes()


def test_resume_does_not_refit_cached_completed_fit_on_report_recovery(tmp_path,monkeypatch):
    import fitted_runner_v2 as runner
    from publication import RunIdentityMismatch
    data=fixture();runner.run(data,run_id='one',runs_dir=tmp_path)
    monkeypatch.setattr(runner,'evaluate',lambda _:pytest.fail('completed fit must not repeat'))
    assert runner.run(data,run_id='one',runs_dir=tmp_path,resume=True)['status']=='verified_completed'
    (tmp_path/'one/models.json').write_text('[]')
    with pytest.raises(RunIdentityMismatch):runner.run(data,run_id='one',runs_dir=tmp_path,resume=True)


def test_incomplete_report_recovery_never_calls_fit_again(tmp_path,monkeypatch):
    import fitted_runner_v2 as runner
    result=subprocess.run([sys.executable,'-I','-B',str(ROOT/'fitted_runner_v2.py'),'--run-id','partial','--runs-dir',str(tmp_path),
        '--test-crash-after-payload','1'],capture_output=True,text=True,timeout=30)
    assert result.returncode==91,result.stderr
    monkeypatch.setattr(runner,'evaluate',lambda _:pytest.fail('verified fit result must be reused'))
    assert runner.run(fixture(),run_id='partial',runs_dir=tmp_path,resume=True)['status']=='completed'


def test_frozen_fitted_operator_runs_and_refuses_payload_corruption(tmp_path):
    from forex_operator_v2 import operate,sha
    from reference_accounting_adapter_v2 import DEFAULT_TRAD
    p=ROOT/'FITTED_OPERATOR_RECIPE.json';h=sha(p)
    assert operate('status',p,h,tmp_path,DEFAULT_TRAD)['status']=='ready'
    result=operate('run',p,h,tmp_path,DEFAULT_TRAD)
    assert result['status']=='completed_verified',result
    assert operate('verify',p,h,tmp_path,DEFAULT_TRAD)['status']=='completed_verified'
    (tmp_path/'operator-fitted-consumer/models.json').write_text('[]')
    assert operate('verify',p,h,tmp_path,DEFAULT_TRAD)['status']=='review_required'


def test_frozen_fitted_operator_refuses_source_drift(tmp_path):
    import shutil
    from forex_operator_v2 import sha
    recipe=json.loads((ROOT/'FITTED_OPERATOR_RECIPE.json').read_text())
    for name in [*recipe['sources'],'FITTED_OPERATOR_RECIPE.json']:shutil.copyfile(ROOT/name,tmp_path/name)
    with (tmp_path/'fitted_fixture_v2.py').open('a') as f:f.write('\n# stale recipe\n')
    p=tmp_path/'FITTED_OPERATOR_RECIPE.json'
    result=subprocess.run([sys.executable,'-I','-B',str(tmp_path/'forex_operator_v2.py'),'run','--recipe',str(p),
        '--recipe-sha256',sha(p),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True)
    assert result.returncode==2 and not (tmp_path/'runs').exists()


@pytest.mark.parametrize('mutation',['unsafe','unknown','duplicate_target'])
def test_configuration_changes_cannot_silently_bypass_contract(mutation):
    data=fixture()
    if mutation=='unsafe':data['contract']['broker_access']=True
    elif mutation=='unknown':data['contract']['hidden_override']=True
    else:data['contract']['targets'].append(data['contract']['targets'][0])
    with pytest.raises(ValueError):evaluate(data)
