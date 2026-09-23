from copy import deepcopy
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parent))
from contracts import TrainingView, fingerprint
from fitted_consumer_v2 import fit_model
from remaining_horizon_v2 import issue_remaining, coverage


def model(horizon=10, cutoff=50, ready=51):
    observations=[{'record_id':str(t),'instrument':'EUR_USD','origin_epoch':t,
        'available_epoch':t,'features':[float(t)]} for t in range(1,30)]
    outcomes=[{'record_id':str(t),'target_id':'direct_'+str(horizon),
        'label_end_epoch':t+horizon,'available_epoch':t+horizon,'value':float(t)*2} for t in range(1,30)]
    return fit_model(observations,outcomes,universe=['EUR_USD'],
        target={'target_id':'direct_'+str(horizon),'horizon_seconds':horizon},
        view=TrainingView(0,cutoff,cutoff,cutoff,200),ready_epoch=ready)


def obs(epoch=100,value=1):
    return {'record_id':str(epoch),'instrument':'EUR_USD','origin_epoch':epoch,
            'available_epoch':epoch,'features':[value]}


def run(models=None, observation=None, **kwargs):
    return issue_remaining(models or [model()], observation or obs(),
        **({'decision_epoch':100,'target_epoch':110,'available_epoch':102}|kwargs))


def test_direct_remaining_uses_fresh_features_and_preserves_original_target():
    first,_=run(observation=obs(value=1));second,_=run(observation=obs(value=20))
    assert first['forecast']['prediction']!=second['forecast']['prediction']
    assert first['original_target_epoch']==second['original_target_epoch']==110
    assert first['policy_admission'].startswith('blocked')


def test_later_origin_requires_new_exact_horizon_not_static_rebase():
    assert run(observation=obs(101),decision_epoch=101)[1]=='exact_remaining_horizon_model_unavailable'
    r,reason=run(models=[model(9)],observation=obs(101),decision_epoch=101)
    assert reason=='eligible' and r['remaining_seconds']==9 and r['original_target_epoch']==110


@pytest.mark.parametrize('change,reason',[
    ({'target_epoch':102},'target_not_future_at_availability'),
    ({'target_epoch':111},'exact_remaining_horizon_model_unavailable')])
def test_unsupported_targets_refuse(change,reason):
    assert run(**change)[1]==reason


def test_future_model_unavailable_and_tamper_refused():
    assert run(models=[model(ready=101)])[1]=='model_not_ready'
    m=model();m['coefficient'][0]+=1
    with pytest.raises(ValueError,match='identity_mismatch'):run(models=[m])


def test_stale_observation_is_not_rebased():
    with pytest.raises(ValueError,match='exact_origin'):run(observation=obs(99))


def test_adaptive_selection_and_frozen_preservation():
    a=model();b=model(cutoff=60,ready=61)
    assert run(models=[a,b],procedure='frozen')[0]['forecast']['model_id']==a['model_id']
    assert run(models=[a,b])[0]['forecast']['model_id']==b['model_id']


@pytest.mark.parametrize('procedure',['frozen','adaptive'])
def test_ambiguous_model_fit_is_not_arbitrarily_selected(procedure):
    a=model();b=deepcopy(a);b['coefficient'][0]+=1;b.pop('model_id');b['model_id']=fingerprint(b)
    with pytest.raises(ValueError,match='ambiguous'):run(models=[a,b],procedure=procedure)


def test_all68_missing_support_rows_remain_visible():
    universe=['EUR_USD']+[f'PAIR_{n}' for n in range(67)]
    result=coverage([model()],[obs()],universe=universe,decision_epochs=[100,101],target_epoch=110)
    assert len(result['coverage'])==272 and len(result['forecasts'])==2
    assert sum(r['reason']=='missing_origin_observation' for r in result['coverage'])==134
    assert sum(r['reason']=='exact_remaining_horizon_model_unavailable' for r in result['coverage'])==136
    assert result['policy_frames']==0


def test_future_model_does_not_change_prior_forecast():
    a=model();before=run(models=[a])[0]
    assert run(models=[a,model(cutoff=90,ready=101)])[0]==before


@pytest.fixture
def historical_inputs():
    import os
    value=os.environ.get('FOREX_REMAINING_TEST_INPUT')
    if not value:pytest.skip('explicit verified historical input directory required')
    return Path(value)


@pytest.mark.parametrize('boundary',[1,0])
def test_actual_remaining_process_death_and_resume(historical_inputs,tmp_path,boundary):
    import json,subprocess
    from remaining_runner_v2 import run as research_run
    root=Path(__file__).resolve().parent
    c=json.loads((root/'REMAINING_CONTRACT.json').read_text())
    research_run(historical_inputs,c,runs_dir=tmp_path,run_id='clean')
    cmd=[sys.executable,'-I','-B',str(root/'remaining_runner_v2.py'),'--input-root',str(historical_inputs),
         '--contract',str(root/'REMAINING_CONTRACT.json'),'--runs-dir',str(tmp_path),'--run-id','partial']
    dead=subprocess.run(cmd+['--test-crash-after-fit',str(boundary)],capture_output=True,text=True,timeout=60)
    assert dead.returncode==91,dead.stderr
    resumed=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=60)
    assert resumed.returncode==0,resumed.stderr
    for name in ['models.json','forecasts.json','coverage.json','run_report.json']:
        assert (tmp_path/'clean'/name).read_bytes()==(tmp_path/'partial'/name).read_bytes()


def test_frozen_remaining_operator_refuses_source_before_import(historical_inputs,tmp_path):
    import json,shutil,subprocess
    from remaining_operator_v2 import sha
    root=Path(__file__).resolve().parent;recipe=json.loads((root/'REMAINING_OPERATOR_RECIPE.json').read_text())
    for name in set(recipe['sources'])|{'REMAINING_OPERATOR_RECIPE.json','REMAINING_CONTRACT.json'}:
        shutil.copyfile(root/name,tmp_path/name)
    with (tmp_path/'remaining_runner_v2.py').open('a') as f:f.write('\nraise RuntimeError("DRIFT_EXECUTED")\n')
    p=tmp_path/'REMAINING_OPERATOR_RECIPE.json'
    result=subprocess.run([sys.executable,'-I','-B',str(tmp_path/'remaining_operator_v2.py'),'run',
        '--recipe',str(p),'--recipe-sha256',sha(p),'--input-root',str(historical_inputs),
        '--contract',str(tmp_path/'REMAINING_CONTRACT.json'),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True)
    assert result.returncode==2 and 'drift' in result.stdout and 'DRIFT_EXECUTED' not in result.stderr
    assert not (tmp_path/'runs').exists()
