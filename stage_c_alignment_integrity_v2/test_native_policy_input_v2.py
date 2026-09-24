from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from native_policy_fixture_v2 import fixture,packet,SOURCES
from native_policy_input_v2 import prepare_frame,load_native,TIER
from policy_continuation_v2 import PolicyReplay
from reference_accounting_adapter_v2 import DEFAULT_TRAD

def setup():
    contract,frames=fixture();return contract,frames,PolicyReplay(contract)

def test_native_original_and_current_theses_reach_actual_policy_ledger():
    contract,frames,p=setup()
    for f in frames:p.apply(f)
    assert len(p.decisions)==30
    first=next(d for d in p.decisions if d['arm']=='continuation' and d['action']=='ENTER')
    assert first['candidate']['kind']=='curve'
    original=p.memory['fixed_hold']['episodes'][0]['original_thesis']
    assert original['source_payload']['curve_id']==first['candidate']['source_payload']['curve_id']
    assert original['source_payload']['conditional_information_epoch']==998
    assert all(not p.book.state['arms'][a]['lots'] for a in contract['policies'])
    assert all(d['action']=='WAIT' for d in p.decisions if d['arm']=='recovered')
    assert any(d['reason']=='recovered_selector_native_curve_fields_unavailable' for d in p.decisions)

@pytest.mark.parametrize('kind',['stale','legacy','target','future','corrupt','duplicate'])
def test_native_chain_refusals_are_retained_without_invented_forecast(kind):
    contract,frames,p=setup();f=deepcopy(frames[0]);q=f['native_packets'][0]
    if kind=='stale':f['native_packets'][0]=packet('EUR_USD',1000,173000,'1.1001',100,conditioning_epoch=997)
    if kind=='legacy':q['curve']['prepared_curve']['input_context']={}
    if kind=='target':f['target_epoch']-=1
    if kind=='future':
        c,_=load_native();q['consumption']=c.consume_curve(q['curve'],q['publication'],expected_source_bindings=SOURCES,clock=lambda:1001)
    if kind=='corrupt':q['curve']['prepared_curve']['model_sha256']='0'*64
    if kind=='duplicate':f['native_packets'].append(deepcopy(q))
    adapted=prepare_frame(f,p.book.config)
    assert adapted['native_refusals']
    assert not any(c['instrument']=='EUR_USD' for c in adapted['candidates'])

def test_missing_conditioning_tier_blocks_before_any_ledger_event():
    c,frames,p=setup();f=deepcopy(frames[0]);f['native_input_tier']='historical_gross_midpoint'
    with pytest.raises(ValueError,match='tier_required'):p.apply(f)
    assert p.events==[]

def test_post_admission_candidate_tampering_cannot_reach_ledger():
    c,frames,p=setup();f=deepcopy(frames[0]);f['candidates'][0]['expected_terminal_price']='100'
    with pytest.raises(ValueError,match='does_not_match_original_packets'):p.apply(f)
    assert p.events==[]

@pytest.mark.parametrize('move',[0,-20])
def test_zero_and_negative_native_incumbent_estimates_remain_distinct_from_missing(move):
    c,frames,p=setup();p.apply(frames[0]);p.apply(frames[1]);f=deepcopy(frames[2])
    f['native_packets'][0]=packet('EUR_USD',f['epoch'],f['target_epoch'],'1.1001',move)
    f=prepare_frame(f,p.book.config);p.apply(f)
    decision=next(d for d in p.decisions if d['arm']=='continuation' and d['epoch']==f['epoch'])
    assert decision['values']['hold_value_usd'] is not None
    assert decision['current_thesis']['side']==(0 if move==0 else -1)
    assert decision['original_thesis']['side']==1
    assert decision['reason']!='incumbent_forecast_unavailable'

def test_native_pipeline_keeps_exit_pending_until_partial_close_reconciles():
    c,frames,p=setup()
    for f in frames[:3]:p.apply(f)
    f=deepcopy(frames[3]);f['fills']['continuation']['units']=1;p.apply(f)
    assert p.memory['continuation']['pending'] is not None and p.position('continuation')['instrument']=='EUR_USD'
    p.apply(frames[4]);d=next(d for d in p.decisions if d['arm']=='continuation' and d['epoch']==frames[4]['epoch'])
    assert d['action']=='WAIT'

@pytest.mark.parametrize('boundary',[2,6,0])
def test_native_actual_process_resume_matches_every_payload(tmp_path,boundary):
    from policy_runner_v2 import run
    c,frames=fixture();run(c,frames,run_id='clean',runs_dir=tmp_path)
    inp=tmp_path/'input.json';inp.write_text(json.dumps({'contract':c,'frames':frames}))
    cmd=[sys.executable,'-I','-B',str(ROOT/'policy_runner_v2.py'),'--run-id','partial','--runs-dir',str(tmp_path),'--input',str(inp)]
    failed=subprocess.run(cmd+['--test-crash-after-frame',str(boundary)],capture_output=True,text=True,timeout=60)
    assert failed.returncode==91,failed.stderr
    recovered=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=60)
    assert recovered.returncode==0,recovered.stderr
    for row in json.loads((tmp_path/'clean/COMPLETION_MANIFEST.json').read_text())['payloads']:
        assert (tmp_path/'clean'/row['path']).read_bytes()==(tmp_path/'partial'/row['path']).read_bytes()

def test_current_native_operator_recipe_and_tamper_refusal(tmp_path):
    from native_policy_operator_v2 import operate,sha,recipe_for
    # Historical approved recipes retain their original source bytes. A current
    # runtime integration test freezes a separate synthetic-only test recipe;
    # it must never overwrite or relabel the historical approval.
    recipe=tmp_path/'CURRENT_SYNTHETIC_TEST_RECIPE.json'
    recipe.write_text(json.dumps(recipe_for(DEFAULT_TRAD)),encoding='utf-8')
    digest=sha(recipe)
    r=operate('run',recipe,digest,tmp_path,DEFAULT_TRAD);assert r['status']=='completed_verified',r
    assert operate('verify',recipe,digest,tmp_path,DEFAULT_TRAD)['status']=='completed_verified'
    (tmp_path/'native-policy-reference/policy_decisions.jsonl').write_text('{}')
    assert operate('verify',recipe,digest,tmp_path,DEFAULT_TRAD)['status']=='review_required'

def test_native_predecessor_drift_refused_before_import(tmp_path):
    from native_policy_input_v2 import PREDECESSORS
    for name in PREDECESSORS:shutil.copyfile(DEFAULT_TRAD/name,tmp_path/name)
    with (tmp_path/'oanda_forecast_curve_contract_v1.py').open('a') as f:f.write('\nraise RuntimeError("UNREVIEWED_SOURCE_EXECUTED")\n')
    with pytest.raises(ValueError,match='drift_before_import'):load_native(tmp_path)

def test_native_operator_refuses_local_runtime_drift_before_import(tmp_path):
    from native_policy_operator_v2 import sha
    recipe=json.loads((ROOT/'NATIVE_POLICY_OPERATOR_RECIPE.json').read_text())
    for n in set(recipe['sources'])|set(recipe['base_recipe']['sources'])|{'NATIVE_POLICY_OPERATOR_RECIPE.json'}:shutil.copyfile(ROOT/n,tmp_path/n)
    with (tmp_path/'forex_operator_v2.py').open('a') as f:f.write('\nraise RuntimeError("UNREVIEWED_OPERATOR_EXECUTED")\n')
    p=tmp_path/'NATIVE_POLICY_OPERATOR_RECIPE.json'
    r=subprocess.run([sys.executable,'-I','-B',str(tmp_path/'native_policy_operator_v2.py'),'run','--recipe',str(p),'--recipe-sha256',sha(p),
      '--runs-dir',str(tmp_path/'runs'),'--trad-root',str(DEFAULT_TRAD)],capture_output=True,text=True)
    assert r.returncode==2 and 'drift' in r.stdout and 'UNREVIEWED_OPERATOR_EXECUTED' not in r.stderr and not (tmp_path/'runs').exists()
