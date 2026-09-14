"""Bounded native practice recovery. Default inspection performs no broker I/O.

Research has a separate supervisor. This launcher never starts/stops research,
never kills a worker and never moves the account marker. Only the separately
validated runner can manage its own practice orders and account lock.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import psutil
import oanda_practice_trial_runner_v2 as runner
from oanda_practice_forecast_adapter_v2 import plain, raw_json, validate_source_config
from oanda_practice_trial_runtime_v3 import atomic_status

ROOT=Path(__file__).resolve().parent
SCHEMA='practice_native_recovery_v2_20260913'
CONFIG=ROOT/'config/practice007_native_v7_20260913_v1.json'
MANIFEST=ROOT/'config/practice_native_recovery_v2_20260913.json'
SOURCES={'oanda_practice_trial_recovery_v2.py','start_oanda_practice_recovery_v2.ps1'}
LOADED_SHA=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
BUDGETS={'run':{'cooldown_sec':90,'hourly_limit':4,'total_limit':12},
         'close_only':{'cooldown_sec':60,'hourly_limit':6,'total_limit':24}}


def need(ok,reason):
    if not ok:raise ValueError(reason)


def digest(value):return runner.digest(value)


def safe_clock(value):
    need(type(value) in (int,float) and math.isfinite(value) and value>0,'invalid_recovery_clock')
    return float(value)


def bindings(manifest_path,config_path):
    raw,manifest=raw_json(manifest_path,65536)
    sealed=dict(manifest);seal=sealed.pop('manifest_sha256',None)
    need(seal==digest(sealed),'recovery_manifest_seal')
    need(manifest.get('schema_version')==SCHEMA and manifest.get('enabled') is True
         and Path(manifest.get('project_root','')).resolve()==ROOT
         and Path(manifest.get('python_path','')).resolve()==Path(sys.executable).resolve()
         and manifest.get('budgets')==BUDGETS,'recovery_manifest_scope')
    need(set(manifest.get('source_bindings',{}))==SOURCES,'recovery_source_inventory')
    for name,expected in manifest['source_bindings'].items():
        path=plain(ROOT,name)
        need(hashlib.sha256(path.read_bytes()).hexdigest()==expected,'recovery_source_changed')
    need(manifest['source_bindings']['oanda_practice_trial_recovery_v2.py']==LOADED_SHA,'loaded_recovery_changed')
    expected_config=plain(ROOT,manifest['config_path'])
    need(Path(config_path).resolve()==expected_config.resolve(),'recovery_config_path')
    raw_config=expected_config.read_bytes()
    need(hashlib.sha256(raw_config).hexdigest()==manifest.get('config_file_sha256'),'recovery_config_bytes')
    config=runner.validate_config(expected_config,verify_forecast_source=False)
    need(config['trial_id']==manifest.get('trial_id'),'recovery_trial_identity')
    need(expected_config.read_bytes()==raw_config,'recovery_config_changed_during_read')
    return config,manifest


def local_trial(state,config):
    """Read only durable ownership history; an incomplete trial is never assumed flat."""
    path=state/'trial.sqlite'
    if not path.exists():return {'exists':False,'initialized':False,'completed':False,'unresolved_intents':0}
    plain(ROOT,str(path.relative_to(ROOT)))
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        values={}
        for key in ('trial_window','config_sha256','initial','completed_flat','stop_requested','loss_stop_latched'):
            row=db.execute('SELECT CASE WHEN length(value)<=65536 THEN value ELSE NULL END FROM meta WHERE key=?',(key,)).fetchone()
            if row is not None:
                need(row[0] is not None,'trial_meta_bound');values[key]=json.loads(row[0])
        window={key:config[key] for key in ('trial_id','start_epoch','stop_epoch')}
        need(values.get('trial_window',window)==window,'recovery_trial_window_changed')
        need(values.get('config_sha256',config['config_sha256'])==config['config_sha256'],'recovery_trial_config_changed')
        unresolved=db.execute("SELECT count(*) FROM intents WHERE status NOT IN ('not_filled','not_submitted','filled_closed')").fetchone()[0]
        count=db.execute('SELECT count(*) FROM intents').fetchone()[0]
        need(0<=unresolved<=count<=1000,'recovery_intent_bound')
    initialized=type(values.get('initial')) is dict
    completed=values.get('completed_flat')
    if completed is not None:
        need(initialized and not unresolved and type(completed) is dict,'recovery_completion_conflict')
        account=completed.get('account',{})
        need(account.get('openTradeCount')==account.get('openPositionCount')==account.get('pendingOrderCount')==0,
             'recovery_completion_not_confirmed_flat')
    return {'exists':True,'initialized':initialized,'completed':completed is not None,'unresolved_intents':unresolved,
            'stop_requested':values.get('stop_requested') is True,'loss_stop_latched':values.get('loss_stop_latched') is True}


def inventory(config_path,processes=None):
    rows=list(psutil.process_iter(['pid','ppid','name','cmdline'])) if processes is None else processes
    need(len(rows)<=8192,'process_inventory_bound')
    workers=[];conflicts=[]
    expected=(ROOT/'oanda_practice_trial_runner_v2.py').resolve()
    for process in rows:
        try:
            info=process.info;args=info.get('cmdline') or []
            if not (info.get('name') or '').lower().startswith('python'):continue
            scripts=[word for word in args[1:] if Path(word).name.lower()=='oanda_practice_trial_runner_v2.py']
            if not scripts:continue
            values=[args[i+1] for i,arg in enumerate(args[:-1]) if arg=='--config']
            selected=Path(values[0]).resolve() if len(values)==1 else CONFIG.resolve() if not values else None
            modes=[mode for mode in ('--run','--close-only') if mode in args]
            if len(scripts)!=1 or Path(scripts[0]).resolve()!=expected or selected!=Path(config_path).resolve() or len(modes)!=1:
                conflicts.append(info['pid']);continue
            workers.append({'pid':info['pid'],'ppid':info['ppid'],'mode':modes[0]})
        except (psutil.NoSuchProcess,psutil.AccessDenied):
            # Unknown process metadata prevents a safe duplicate-launch decision.
            raise ValueError('process_inventory_unavailable') from None
    ids={row['pid'] for row in workers}
    groups=[row['pid'] for row in workers if row['ppid'] not in ids]
    return {'workers':workers,'worker_group_count':len(groups),'conflicting_worker_pids':conflicts}


def account_marker():
    path=ROOT/'data/oanda_training_manager/state/practice_007_order.lock'
    if not path.exists():return {'present':False,'blocks_launch':False}
    plain(ROOT,str(path.relative_to(ROOT)))
    with path.open('rb') as stream:raw=stream.read(257)
    words=raw.split()
    if len(raw)>256 or len(words)!=2 or not words[0].isdigit():return {'present':True,'blocks_launch':True,'reason':'unrecognized_account_marker'}
    pid=int(words[0]);alive=psutil.pid_exists(pid)
    return {'present':True,'blocks_launch':alive,'pid':pid,'reason':'live_owner' if alive else 'dead_marker_runner_must_verify'}


def read_attempts(path,config,manifest):
    binding={'trial_id':config['trial_id'],'config_sha256':config['config_sha256'],'manifest_sha256':manifest['manifest_sha256']}
    if not path.exists():return {'schema_version':SCHEMA,'binding':binding,'attempts':[]}
    _,value=raw_json(path,65536);body=dict(value);seal=body.pop('payload_sha256',None)
    need(seal==digest(body) and value.get('binding')==binding and value.get('schema_version')==SCHEMA,'recovery_attempt_identity')
    records=value.get('attempts');need(type(records) is list and len(records)<=36,'recovery_attempt_bound')
    previous=0
    for record in records:
        at=safe_clock(record['epoch']);need(at>=previous and record['mode'] in BUDGETS,'recovery_attempt_clock');previous=at
    return body


def launch_budget(records,mode,now):
    safe_clock(now);need(mode in BUDGETS,'recovery_mode')
    if records and now<records[-1]['epoch']:return 'clock_rollback_refused'
    selected=[record for record in records if record['mode']==mode];limits=BUDGETS[mode]
    if len(selected)>=limits['total_limit']:return 'restart_total_limit_manual_review'
    if sum(now-record['epoch']<3600 for record in selected)>=limits['hourly_limit']:return 'restart_hourly_limit'
    if selected and now-selected[-1]['epoch']<limits['cooldown_sec']:return 'restart_cooldown'
    return 'ready'


def decision(config,trial,processes,marker,source_valid,now):
    safe_clock(now)
    if trial['completed']:return 'completed_flat'
    if processes['conflicting_worker_pids'] or processes['worker_group_count']>1:return 'conflicting_worker_refused'
    if processes['workers']:return 'already_running'
    if marker['blocks_launch']:return 'account_owner_blocks_launch'
    if trial.get('stop_requested') or trial.get('loss_stop_latched'):
        return 'close_only' if trial['initialized'] else 'never_initialized_no_new_entry'
    if now<config['start_epoch']:return 'awaiting_trial_start'
    if now>=config['stop_epoch'] or not config['enabled'] or not source_valid:
        return 'close_only' if trial['initialized'] else 'withheld_no_initialized_trial'
    return 'run'


def inspection(manifest_path,config_path,*,clock=time.time):
    config,manifest=bindings(manifest_path,config_path);state=ROOT/'data/oanda_training_manager'/config['trial_id']
    trial=local_trial(state,config);processes=inventory(config_path);marker=account_marker()
    source_valid=True;source_error=None
    try:validate_source_config(ROOT,config['forecast_source'])
    except Exception:source_valid=False;source_error='native_registered_source_unavailable'
    now=safe_clock(clock());phase=decision(config,trial,processes,marker,source_valid,now)
    attempts=read_attempts(state/'recovery_attempts.json',config,manifest)
    if phase in BUDGETS:
        budget=launch_budget(attempts['attempts'],phase,now)
        if budget!='ready':phase=budget
        elif phase=='run' and not runner.process_preflight()['ready']:phase='awaiting_safe_research_supervisor'
    status_age=None
    if (state/'status.json').exists():
        try:
            _,status=raw_json(state/'status.json',512*1024)
            need(status.get('trial_id')==config['trial_id'] and status.get('schema_version')==runner.SCHEMA,'worker_status_identity')
            status_age=now-safe_clock(status['observed_epoch']);need(status_age>=0,'worker_status_future')
        except Exception:status_age=None
    report={'schema_version':SCHEMA,'observed_epoch':now,'pid':__import__('os').getpid(),'phase':phase,
        'trial_id':config['trial_id'],'stop_epoch':config['stop_epoch'],'trial':trial,'processes':processes,
        'account_marker':marker,'source_valid':source_valid,'source_error':source_error,
        'worker_status_age_sec':status_age,'worker_status_fresh':status_age is not None and status_age<=180,
        'restart_counts':dict(Counter(record['mode'] for record in attempts['attempts'])),
        'budgets':BUDGETS,'config_sha256':config['config_sha256'],'manifest_sha256':manifest['manifest_sha256'],
        'broker_io_performed_by_recovery':False,'research_started_or_stopped':False}
    return report,config,manifest,attempts,state


def spawn_runner(mode,config_path,state):
    need(mode in BUDGETS,'recovery_mode')
    flags=getattr(subprocess,'CREATE_NO_WINDOW',0)
    startup=None
    if sys.platform=='win32':
        startup=subprocess.STARTUPINFO();startup.dwFlags|=subprocess.STARTF_USESHOWWINDOW;startup.wShowWindow=0
    # Suppress growing raw console logs; runner owns bounded sanitized status
    # and durable attempt/reconciliation records. No shell or broker calls here.
    child=subprocess.Popen([sys.executable,'-B',str(ROOT/'oanda_practice_trial_runner_v2.py'),
        '--config',str(Path(config_path).resolve()),'--run' if mode=='run' else '--close-only'],
        cwd=ROOT,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
        creationflags=flags,startupinfo=startup,close_fds=True)
    return child.pid


def cycle(manifest_path,config_path,*,clock=time.time,spawn=spawn_runner):
    report,config,manifest,attempts,state=inspection(manifest_path,config_path,clock=clock)
    state.mkdir(parents=True,exist_ok=True)
    if report['phase'] in BUDGETS:
        # Repeat source, process, lock, clock and durable-budget reads immediately
        # before claiming a spawn. The child also validates its own entire scope.
        fresh,new_config,new_manifest,current,_=inspection(manifest_path,config_path,clock=clock)
        if fresh['phase']==report['phase'] and new_config==config and new_manifest==manifest:
            mode=fresh['phase'];current['attempts'].append({'epoch':fresh['observed_epoch'],'mode':mode})
            current['payload_sha256']=digest(current)
            atomic_status(state/'recovery_attempts.json',current,clock=clock)
            report=fresh
            try:report.update(phase='spawned_'+mode,spawned_pid=spawn(mode,config_path,state))
            except Exception:report.update(phase='spawn_failed_counted',spawn_error='worker_launch_unavailable')
        else:report=fresh
    atomic_status(state/'recovery_status.json',report,clock=clock)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--watch',action='store_true');mode.add_argument('--check',action='store_true')
    parser.add_argument('--config',type=Path,default=CONFIG);parser.add_argument('--manifest',type=Path,default=MANIFEST)
    args=parser.parse_args()
    if not args.watch:
        report,*_=inspection(args.manifest,args.config);print(json.dumps(report));return 0
    config,_=bindings(args.manifest,args.config);state=ROOT/'data/oanda_training_manager'/config['trial_id']
    state.mkdir(parents=True,exist_ok=True)
    # Different marker from the broker-account lock; it serializes launchers.
    with runner.AccountLock(ROOT/'data/oanda_training_manager/state/practice_native_recovery_v2.lock') as lease:
        while True:
            lease.check()
            try:
                result=cycle(args.manifest,args.config)
            except Exception:
                result={'schema_version':SCHEMA,'observed_epoch':time.time(),'phase':'inspection_refused',
                    'reason':'source_config_or_local_state_unavailable','broker_io_performed_by_recovery':False}
                atomic_status(state/'recovery_status.json',result)
            if result['phase']=='completed_flat':return 0
            if result['phase']=='withheld_no_initialized_trial' and time.time()>=config['stop_epoch']:return 0
            time.sleep(15)


if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception:
        print(json.dumps({'schema_version':SCHEMA,'phase':'recovery_refused','broker_io_performed_by_recovery':False}));raise SystemExit(2)
