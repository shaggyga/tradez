"""Design R06/R08/R10/R11/R14; TST01,25–30,48,51–53 policy consumer tests."""
from copy import deepcopy
from decimal import Decimal, localcontext
import json
from pathlib import Path
import subprocess
import sys
import pytest

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from policy_continuation_v2 import PolicyReplay
from policy_fixture_v2 import fixture
from policy_runner_v2 import run,identity_for
from accounting_event_fixtures_v2 import panel
from accounting_event_audit_v2 import audit_accounting
from publication import verify_completed_run,RunIdentityMismatch
from reference_accounting_adapter_v2 import DEFAULT_TRAD


def prefix(count=2,contract=None,frames=None,engine='reference'):
    c,f=fixture(); c=contract or c; f=frames or f
    replay=PolicyReplay(c,engine=engine)
    for row in f[:count]:replay.apply(row)
    return replay,c,f


def decision(replay,arm,epoch):
    return next(d for d in replay.decisions if d['arm']==arm and d['epoch']==epoch)


def test_actual_five_policy_ladder_and_cash_use_event_ledger_with_no_rejections():
    r,c,f=prefix(11)
    assert all(row['receipt']['status']!='rejected' for row in r.rows)
    assert all(not a['lots'] and not sum(r.book._remaining(o) for o in a['orders'].values()) for a in r.book.state['arms'].values())
    assert decision(r,'fixed_hold',20000)['action']=='HOLD'
    assert decision(r,'continuation',20000)['action']=='REPLACE'
    assert decision(r,'hysteresis',20000)['action']=='HOLD'
    assert decision(r,'hysteresis',20100)['action']=='REPLACE'
    assert all(d['action']=='WAIT' for d in r.decisions if d['arm']=='cash')
    audit=audit_accounting(c['ledger'],r.events,r.rows)
    assert Decimal(audit['maximum_residual_usd'])==0


def test_common_executable_baseline_and_no_sunk_entry_cost_double_charge():
    r,c,f=prefix(3)
    d=decision(r,'continuation',20000); v=d['values']
    assert v['common_target_epoch']==173000
    assert v['exit_value_usd']==0
    # 908 EUR, +10 pip continuation less declared financing. No spread or paid-entry fee again.
    assert v['hold_value_usd']==Decimal('0.86260')
    assert v['baseline_liquidation_wealth_usd']==Decimal('9999.6184')
    assert v['alternatives'][0]['value_usd']==Decimal('23.55035')
    original=deepcopy(r.memory['fixed_hold']['original_thesis'])
    for row in f[3:7]:r.apply(row)
    assert r.memory['fixed_hold']['original_thesis']==original
    assert r.memory['fixed_hold']['current_thesis']['candidate_id']!=original['candidate_id']
    assert r.memory['fixed_hold']['episodes'][0]['original_thesis']==original


def test_mismatched_challenger_horizon_is_explicit_rejection():
    r,c,f=prefix()
    f[2]['candidates'][1]['original_target_epoch']+=86400
    r.apply(f[2]); d=decision(r,'continuation',20000)
    assert d['action']=='HOLD'
    assert d['rejected_candidates'][0]['reason']=='incomparable_or_elapsed_native_target'


def test_future_candidate_unavailable_and_future_frame_perturbation():
    r,c,f=prefix()
    f[2]['candidates'][1]['available_epoch']=20001
    r.apply(f[2]); assert decision(r,'continuation',20000)['action']=='HOLD'
    assert 'not_available' in decision(r,'continuation',20000)['rejected_candidates'][0]['reason']
    baseline,_,_=prefix(3)
    c,changed=fixture();changed[-1]['quotes']=panel(173000,'9','10')
    altered,_,_=prefix(3,frames=changed)
    assert baseline.decisions==altered.decisions and baseline.book.state==altered.book.state


def test_partial_exit_does_not_release_all_capacity_or_start_replacement():
    r,c,f=prefix(3)
    f[3]['fills']['continuation']['units']=100
    r.apply(f[3]);old=r.position('continuation');assert old['base_units']==808
    used=r.book.reconcile(r.book.state['arms']['continuation'],f[3]['quotes'],20060)['used_margin_usd']
    assert used==Decimal('88.896160')
    r.apply(f[4]); assert decision(r,'continuation',20100)['action']=='WAIT'
    assert len(r.memory['continuation']['episodes'])==1
    r.apply(f[5]);assert r.position('continuation') is None
    r.apply(f[6]);assert decision(r,'continuation',20200)['action']=='ENTER'
    r.apply(f[7]);assert r.memory['continuation']['episodes'][1]['replacement_parent']['decision_id']=='continuation-20000'


def test_persistence_resets_on_same_pair_new_material_thesis():
    r,c,f=prefix(4)
    f[4]['candidates'][1]['thesis_version']='changed-thesis'
    r.apply(f[4]);assert decision(r,'hysteresis',20100)['action']=='HOLD'
    assert r.memory['hysteresis']['streak']==1


def test_risk_precedes_grace_and_original_position_is_not_required_to_recover():
    c,f=fixture();c['grace_fraction']='1';c['ledger']['maximum_gross_currency_usd']='1500'
    r,_,_=prefix(2,contract=c)
    f[2]['quotes']=panel(20000,'1.8','1.8002')
    r.apply(f[2]);d=decision(r,'hysteresis',20000)
    assert d['action']=='EXIT' and d['reason']=='risk_or_predeclared_deadline'


def test_arm_memory_and_state_are_isolated():
    r,c,f=prefix()
    original=deepcopy(r.memory['fixed_hold'])
    r.memory['continuation']['episodes'][0]['original_thesis']['source_payload']['score']='999'
    r.book.state['arms']['continuation']['fees_usd']+=1
    assert r.memory['fixed_hold']==original
    assert r.book.state['arms']['fixed_hold']['fees_usd']==Decimal('.10')


def test_cost_stress_recomputes_decisions_and_does_not_force_top_rank_entry():
    c,f=fixture();c['fee_usd_per_fill']='100'
    r,_,_=prefix(1,contract=c)
    assert decision(r,'continuation',1000)['action']=='WAIT'
    assert decision(r,'hysteresis',1000)['action']=='WAIT'
    assert not r.book.state['arms']['continuation']['orders']


def test_preactivation_cancel_request_can_still_fill_but_ack_blocks():
    r,c,f=prefix(1)
    r.apply({'kind':'order_event','event':'cancel_request','arms':['continuation'],'epoch':1030,'quotes':panel(1030)})
    r.apply(f[1]);assert r.position('continuation') is not None
    r,c,f=prefix(1)
    r.apply({'kind':'order_event','event':'cancel_request','arms':['continuation'],'epoch':1030,'quotes':panel(1030)})
    r.apply({'kind':'order_event','event':'cancel_ack','arms':['continuation'],'epoch':1040,'quotes':panel(1040)})
    r.apply(f[1]);assert r.position('continuation') is None


@pytest.mark.parametrize('engine',['reference','optimized'])
@pytest.mark.parametrize('crash',[1,3,4,6,9,0])
def test_actual_process_resume_all_payloads_identical(engine,crash,tmp_path):
    c,f=fixture(); run(c,f,run_id='clean',runs_dir=tmp_path,engine=engine)
    cmd=[sys.executable,'-I','-B',str(ROOT/'policy_runner_v2.py'),'--run-id','crash','--runs-dir',str(tmp_path),'--engine',engine]
    out=subprocess.run(cmd+['--test-crash-after-frame',str(crash)],capture_output=True,text=True,timeout=30)
    assert out.returncode==91,out.stderr
    out=subprocess.run(cmd+['--resume'],capture_output=True,text=True,timeout=30)
    assert out.returncode==0,out.stderr
    manifest=json.loads((tmp_path/'clean/COMPLETION_MANIFEST.json').read_text())
    for p in manifest['payloads']:
        assert (tmp_path/'clean'/p['path']).read_bytes()==(tmp_path/'crash'/p['path']).read_bytes()


def test_reference_optimized_decisions_events_and_states_match(tmp_path):
    c,f=fixture()
    for engine in ('reference','optimized'):run(c,f,run_id=engine,runs_dir=tmp_path,engine=engine)
    for n in ('policy_decisions.jsonl','policy_state.json','event_ledger.jsonl','final_state.json','accounting_audit.json'):
        assert (tmp_path/'reference'/n).read_bytes()==(tmp_path/'optimized'/n).read_bytes()


def test_completed_run_checks_corruption_and_changed_config(tmp_path):
    c,f=fixture();run(c,f,run_id='one',runs_dir=tmp_path)
    c['switch_buffer_usd']='100'
    with pytest.raises(RunIdentityMismatch):run(c,f,run_id='one',runs_dir=tmp_path,resume=True)
    c,f=fixture();(tmp_path/'one/policy_state.json').write_text('{}')
    with pytest.raises(RunIdentityMismatch):run(c,f,run_id='one',runs_dir=tmp_path,resume=True)


def test_operator_frozen_policy_recipe_runs_verifies_and_refuses_corruption(tmp_path):
    from forex_operator_v2 import operate,sha
    p=ROOT/'POLICY_OPERATOR_RECIPE.json'; h=sha(p)
    assert operate('status',p,h,tmp_path,DEFAULT_TRAD)['status']=='ready'
    result=operate('run',p,h,tmp_path,DEFAULT_TRAD)
    assert result['status']=='completed_verified',result
    manifests={str(p):p.read_bytes() for p in tmp_path.rglob('COMPLETION_MANIFEST.json')}
    assert operate('resume',p,h,tmp_path,DEFAULT_TRAD)['status']=='completed_verified'
    assert manifests=={str(p):p.read_bytes() for p in tmp_path.rglob('COMPLETION_MANIFEST.json')}
    (tmp_path/'operator-policy-reference/policy_state.json').write_text('{}')
    assert operate('verify',p,h,tmp_path,DEFAULT_TRAD)['status']=='review_required'


def test_operator_refuses_stale_policy_source_without_reapproving(tmp_path):
    import shutil
    from forex_operator_v2 import SOURCES,sha
    for n in (*SOURCES,'policy_continuation_v2.py','policy_fixture_v2.py','policy_runner_v2.py','native_policy_input_v2.py','POLICY_OPERATOR_RECIPE.json'):
        shutil.copyfile(ROOT/n,tmp_path/n)
    with (tmp_path/'policy_fixture_v2.py').open('a') as handle:handle.write('\n# changed fixture\n')
    p=tmp_path/'POLICY_OPERATOR_RECIPE.json'
    result=subprocess.run([sys.executable,'-I','-B',str(tmp_path/'forex_operator_v2.py'),'run','--recipe',str(p),
        '--recipe-sha256',sha(p),'--trad-root',str(DEFAULT_TRAD),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True)
    assert result.returncode==2
    assert json.loads(result.stdout)['detail']=='source_dependency_environment_or_recipe_drift'
    assert not (tmp_path/'runs').exists()


def test_serialization_key_order_cannot_change_economic_path():
    c,f=fixture();a,_,_=prefix(11,contract=c,frames=f)
    c=json.loads(json.dumps(c,sort_keys=True)); f=json.loads(json.dumps(f,sort_keys=True))
    b,_,_=prefix(11,contract=c,frames=f)
    assert a.events==b.events and a.decisions==b.decisions and a.memory==b.memory


def test_mfe_mae_only_reflect_observed_marks_and_paid_fees_stay_in_ledger():
    r,c,f=prefix(2)
    assert r.memory['continuation']['observed_mfe_usd']==0
    assert r.memory['continuation']['observed_mae_usd']==Decimal('0.1816')
    # A much larger forecast is not an observed excursion.
    f[2]['candidates'][0]['expected_move_pips']='10000'
    r.apply(f[2])
    assert r.memory['continuation']['observed_mfe_usd']==0
    assert r.book.state['arms']['continuation']['fees_usd']==Decimal('.10')


def test_challenger_persistence_cannot_cross_closed_or_replaced_episode():
    r,c,f=prefix(5)
    assert r.memory['hysteresis']['streak']==2
    r.apply(f[5])
    assert r.position('hysteresis') is None
    assert r.memory['hysteresis']['streak']==0 and r.memory['hysteresis']['challenger_key'] is None
    for frame in f[6:]:r.apply(frame)
    assert r.memory['hysteresis']['streak']==0
    assert len(r.memory['hysteresis']['episodes'])==2


@pytest.mark.parametrize('pair,side',[('EUR_USD',-1),('USD_JPY',1)])
def test_short_and_cross_currency_continuation_have_independent_numerical_oracles(pair,side):
    c,f=fixture()
    for frame in (f[0],f[2]):
        frame['candidates']=[{**frame['candidates'][0],'instrument':pair,'side':side,'expected_move_pips':'100',
            'remaining_financing_usd_per_base_unit':{'long':'0','short':'0'}}]
    r,_,_=prefix(3,contract=c,frames=f)
    with localcontext(r.ref.CTX):
        expected=Decimal('9.08') if pair=='EUR_USD' else Decimal('980')/Decimal('150.02')+Decimal('20')/Decimal('150.00')
        assert abs(decision(r,'continuation',20000)['values']['hold_value_usd']-expected)<Decimal('1e-40')
