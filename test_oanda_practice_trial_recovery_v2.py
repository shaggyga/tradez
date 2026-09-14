"""Synthetic recovery decisions, durable spawn claims and Windows ownership."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pytest
import oanda_practice_trial_recovery_v2 as m
import oanda_practice_trial_runner_v2 as runner

NOW=1800000000.0


def config():return {'trial_id':'practice007_native_v7_test','start_epoch':NOW-1,'stop_epoch':NOW+3600,
                     'config_sha256':'a'*64,'enabled':True}
def trial():return {'exists':False,'initialized':False,'completed':False,'unresolved_intents':0}
def processes():return {'workers':[],'worker_group_count':0,'conflicting_worker_pids':[]}


@pytest.mark.parametrize('change,source,expected',[
    ({},True,'run'),({'stop_epoch':NOW},True,'withheld_no_initialized_trial'),
    ({'start_epoch':NOW+1},True,'awaiting_trial_start'),({'enabled':False},True,'withheld_no_initialized_trial'),
    ({},False,'withheld_no_initialized_trial')])
def test_uninitialized_trial_cannot_start_when_withheld(change,source,expected):
    assert m.decision({**config(),**change},trial(),processes(),{'blocks_launch':False},source,NOW)==expected


@pytest.mark.parametrize('why',['expired','source_failed','disabled','manual_stop','loss_stop'])
def test_existing_incomplete_trial_recovers_close_only(why):
    cfg=config();state={**trial(),'exists':True,'initialized':True};valid=True
    if why=='expired':cfg['stop_epoch']=NOW
    if why=='source_failed':valid=False
    if why=='disabled':cfg['enabled']=False
    if why=='manual_stop':state['stop_requested']=True
    if why=='loss_stop':state['loss_stop_latched']=True
    assert m.decision(cfg,state,processes(),{'blocks_launch':False},valid,NOW)=='close_only'


def test_live_or_ambiguous_account_owner_never_stolen():
    assert m.decision(config(),trial(),processes(),{'blocks_launch':True},True,NOW)=='account_owner_blocks_launch'


def test_existing_worker_is_observed_without_duplicate_or_kill():
    p={**processes(),'workers':[{'pid':3}],'worker_group_count':1}
    assert m.decision(config(),trial(),p,{'blocks_launch':True},False,NOW)=='already_running'
    p['worker_group_count']=2
    assert m.decision(config(),trial(),p,{'blocks_launch':False},True,NOW)=='conflicting_worker_refused'


def test_completed_history_prevents_new_trial_relaunch():
    assert m.decision(config(),{**trial(),'completed':True},processes(),{'blocks_launch':False},True,NOW)=='completed_flat'


@pytest.mark.parametrize('mode',list(m.BUDGETS))
def test_restart_budget_cooldown_hourly_total_and_clock_rollback(mode):
    cap=m.BUDGETS[mode];rows=[{'mode':mode,'epoch':NOW-1}]
    assert m.launch_budget(rows,mode,NOW)=='restart_cooldown'
    assert m.launch_budget(rows,mode,NOW-2)=='clock_rollback_refused'
    rows=[{'mode':mode,'epoch':NOW-100*(i+1)} for i in reversed(range(cap['hourly_limit']))]
    assert m.launch_budget(rows,mode,NOW)=='restart_hourly_limit'
    rows=[{'mode':mode,'epoch':NOW-4000-i} for i in reversed(range(cap['total_limit']))]
    assert m.launch_budget(rows,mode,NOW)=='restart_total_limit_manual_review'
    assert m.launch_budget(rows,'close_only' if mode=='run' else 'run',NOW)=='ready'


def test_exact_worker_chain_counts_as_one_group_wrong_config_conflicts(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'ROOT',tmp_path);path=tmp_path/'trial.json';worker=tmp_path/'oanda_practice_trial_runner_v2.py'
    def row(pid,ppid,path=path):return SimpleNamespace(info={'pid':pid,'ppid':ppid,'name':'python.exe',
        'cmdline':['python',str(worker),'--config',str(path),'--run']})
    value=m.inventory(path,[row(1,0),row(2,1)])
    assert value['worker_group_count']==1 and len(value['workers'])==2
    value=m.inventory(path,[row(1,0),row(3,0)])
    assert value['worker_group_count']==2
    assert m.inventory(path,[row(1,0,path=tmp_path/'other.json')])['conflicting_worker_pids']==[1]


def test_spawn_is_claimed_durably_before_launch_and_failure_uses_budget(tmp_path,monkeypatch):
    cfg=config();manifest={'manifest_sha256':'b'*64};state=tmp_path/'state';state.mkdir()
    attempts={'schema_version':m.SCHEMA,'binding':{'trial_id':cfg['trial_id'],'config_sha256':cfg['config_sha256'],
        'manifest_sha256':manifest['manifest_sha256']},'attempts':[]}
    report={'phase':'run','observed_epoch':NOW}
    monkeypatch.setattr(m,'inspection',lambda *args,**kwargs:(deepcopy(report),cfg,manifest,deepcopy(attempts),state))
    called=[]
    def failure(mode,*args):
        durable=m.read_attempts(state/'recovery_attempts.json',cfg,manifest)
        assert durable['attempts']==[{'epoch':NOW,'mode':'run'}];called.append(mode)
        raise OSError('synthetic no process launch')
    result=m.cycle('manifest','config',clock=lambda:NOW,spawn=failure)
    assert result['phase']=='spawn_failed_counted' and called==['run']
    assert json.loads((state/'recovery_status.json').read_text())['phase']=='spawn_failed_counted'


def test_fresh_inspection_source_failure_prevents_earlier_run_decision(tmp_path,monkeypatch):
    cfg=config();manifest={};state=tmp_path/'state';reports=iter([{'phase':'run','observed_epoch':NOW},
        {'phase':'withheld_no_initialized_trial','observed_epoch':NOW}])
    monkeypatch.setattr(m,'inspection',lambda *args,**kwargs:(next(reports),cfg,manifest,{'attempts':[]},state))
    result=m.cycle('manifest','config',clock=lambda:NOW,spawn=lambda *args:pytest.fail('must not launch'))
    assert result['phase']=='withheld_no_initialized_trial' and not (state/'recovery_attempts.json').exists()


def test_hidden_exact_runner_argv_has_no_shell_or_forecast_override(tmp_path,monkeypatch):
    calls=[];monkeypatch.setattr(m.subprocess,'Popen',lambda *args,**kwargs:calls.append((args,kwargs)) or SimpleNamespace(pid=123))
    path=tmp_path/'config.json'
    assert m.spawn_runner('close_only',path,tmp_path)==123
    args,kw=calls[0];assert args[0][-1]=='--close-only' and '--run' not in args[0]
    assert kw.get('shell',False) is False and kw['stdout']==m.subprocess.DEVNULL
    if sys.platform=='win32':assert kw['creationflags'] & m.subprocess.CREATE_NO_WINDOW


def test_sealed_manifest_binds_actual_wrapper_python_trial_and_loaded_bytes(tmp_path,monkeypatch):
    source_root=m.ROOT;cfg=config();config_path=tmp_path/'config/trial.json';config_path.parent.mkdir()
    config_path.write_text(json.dumps(cfg));pins={}
    for name in m.SOURCES:
        raw=(source_root/name).read_bytes();(tmp_path/name).write_bytes(raw);pins[name]=hashlib.sha256(raw).hexdigest()
    manifest={'schema_version':m.SCHEMA,'enabled':True,'project_root':str(tmp_path),'python_path':sys.executable,
        'budgets':m.BUDGETS,'source_bindings':pins,'config_path':'config/trial.json',
        'config_file_sha256':hashlib.sha256(config_path.read_bytes()).hexdigest(),'trial_id':cfg['trial_id']}
    manifest['manifest_sha256']=m.digest(manifest);path=tmp_path/'manifest.json';path.write_text(json.dumps(manifest))
    monkeypatch.setattr(m,'ROOT',tmp_path)
    validations=[]
    monkeypatch.setattr(m.runner,'validate_config',lambda *args,**kwargs:validations.append(kwargs) or cfg)
    assert m.bindings(path,config_path)==(cfg,manifest)
    assert validations==[{'verify_forecast_source':False}]
    config_path.write_text(json.dumps({**cfg,'stop_epoch':cfg['stop_epoch']+1}))
    with pytest.raises(ValueError,match='config_bytes'):m.bindings(path,config_path)
    config_path.write_text(json.dumps(cfg))
    (tmp_path/'start_oanda_practice_recovery_v2.ps1').write_bytes(b'changed')
    with pytest.raises(ValueError,match='source_changed'):m.bindings(path,config_path)


def test_changed_restart_records_are_refused_instead_of_reset(tmp_path):
    cfg=config();manifest={'manifest_sha256':'b'*64};path=tmp_path/'attempts.json'
    value=m.read_attempts(path,cfg,manifest);value['attempts']=[{'epoch':NOW,'mode':'run'}]
    value['payload_sha256']=m.digest(value);path.write_text(json.dumps(value))
    assert m.read_attempts(path,cfg,manifest)['attempts']==[{'epoch':NOW,'mode':'run'}]
    value['attempts']=[];path.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='attempt_identity'):m.read_attempts(path,cfg,manifest)


def test_local_trial_is_readonly_and_never_infers_incomplete_flatness(tmp_path,monkeypatch):
    monkeypatch.setattr(m,'ROOT',tmp_path);state=tmp_path/'trial';cfg=config()
    assert m.local_trial(state,cfg)['exists'] is False
    ledger=runner.Ledger(state/'trial.sqlite')
    try:
        ledger.set('trial_window',{k:cfg[k] for k in ('trial_id','start_epoch','stop_epoch')})
        ledger.set('config_sha256',cfg['config_sha256']);ledger.set('initial',{'epoch':NOW,'NAV':'40'})
        before=ledger.db.total_changes;value=m.local_trial(state,cfg)
        assert value['initialized'] and not value['completed'] and ledger.db.total_changes==before
        changed={**cfg,'stop_epoch':cfg['stop_epoch']+1}
        with pytest.raises(ValueError,match='window_changed'):m.local_trial(state,changed)
        ledger.set('completed_flat',{'epoch':NOW,'account':{'openTradeCount':0,'openPositionCount':0,'pendingOrderCount':0}})
        assert m.local_trial(state,cfg)['completed'] is True
    finally:ledger.close()


@pytest.mark.skipif(sys.platform!='win32',reason='requires actual Windows deny-delete sharing')
def test_competing_dead_marker_observation_cannot_rename_new_live_owner(tmp_path,monkeypatch):
    import psutil
    path=tmp_path/'order.lock';dead=99999999;path.write_text(str(dead)+' stale\n')
    checked=threading.Event();release=threading.Event();results=[];original=psutil.pid_exists
    def exists(pid):
        if pid==dead:
            if threading.current_thread().name=='delayed_contender':
                checked.set();assert release.wait(5)
            return False
        return original(pid)
    monkeypatch.setattr(psutil,'pid_exists',exists)
    def contender():
        try:
            with runner.AccountLock(path):results.append('acquired')
        except Exception as exc:results.append(exc)
    thread=threading.Thread(target=contender,name='delayed_contender');thread.start();assert checked.wait(5)
    try:
        with runner.AccountLock(path) as owner:
            marker=path.read_bytes();release.set();thread.join(5)
            assert not thread.is_alive() and len(results)==1 and isinstance(results[0],PermissionError)
            assert results[0].winerror==32
            assert path.read_bytes()==marker;owner.check()
    finally:release.set();thread.join(5)
