"""Supervise only this release's passive worker; never start upstream services."""
import argparse
import json
import math
import os
import time

from meter_capture_v1 import ROOT, FLAGS, load_profile, verify_release
from meter_runtime_support_v1 import ProcessLock, atomic_json, run_bounded
from meter_worker_v1 import iso, write_event


def supervise(*,worker_duration_seconds=3600,max_sessions=0,restart_delay_seconds=30):
    if type(worker_duration_seconds) not in (int,float) or not math.isfinite(worker_duration_seconds) or not 0<worker_duration_seconds<=3600:raise ValueError('invalid_worker_duration')
    if type(max_sessions) is not int or max_sessions<0:raise ValueError('invalid_max_sessions')
    if type(restart_delay_seconds) not in (int,float) or not math.isfinite(restart_delay_seconds) or not 0<=restart_delay_seconds<=300:
        raise ValueError('invalid_restart_delay')
    release=verify_release();profile=load_profile();runtime=ROOT/'runtime';runtime.mkdir(exist_ok=True)
    lock=ProcessLock(runtime/'supervisor.lock')
    if not lock.acquire():return {'status':'already_supervised','exit_code':3}
    state={'schema_version':'continuous_currency_meter_supervisor_v1_20260912','pid':os.getpid(),
           'release_sha256':release,'started_utc':iso(time.time()),'sessions':0,'rapid_failures':0,
           'owns_only_new_passive_worker':True,'upstream_services_started':False,**FLAGS}
    def publish(phase):
        state['phase']=phase;state['observed_utc']=iso(time.time())
        atomic_json(runtime/'supervisor_status.json',state)
    try:
        while True:
            if (runtime/'STOP').exists() or (max_sessions and state['sessions']>=max_sessions):
                publish('stopped');return {'status':'stopped','exit_code':0}
            verify_release();state['sessions']+=1;publish('running_worker_session')
            began=time.monotonic()
            outcome=run_bounded([profile['python'],str(ROOT/'src/meter_worker_v1.py'),
                                 '--duration-seconds',str(worker_duration_seconds)],cwd=ROOT,
                                timeout_seconds=worker_duration_seconds+300,
                                memory_bytes=1024*1024*1024,max_processes=4)
            state['last_session']={'completed_utc':iso(time.time()),'exit_code':outcome['exit_code'],
                                   'reason':outcome['reason'],'counters':outcome['counters'],'worker':outcome.get('stdout')}
            try:write_event(runtime/'supervisor_events.jsonl',{'observed_utc':iso(time.time()),'event':'worker_session_ended',
                                                            'session':state['sessions'],'reason':outcome['reason'],'exit_code':outcome['exit_code']})
            except (OSError,ValueError):state['diagnostic_write_errors']=state.get('diagnostic_write_errors',0)+1
            elapsed=time.monotonic()-began
            payload=outcome.get('stdout')
            graceful=(outcome['exit_code']==0 and outcome['reason']=='completed' and isinstance(payload,dict)
                      and payload.get('status')=='stopped' and ((runtime/'STOP').exists() or elapsed>=worker_duration_seconds))
            if graceful:
                state['rapid_failures']=0;continue
            if outcome['exit_code']==3 and isinstance(payload,dict) and payload.get('status')=='already_running':
                publish('another_worker_owns_lock');return {'status':'another_worker_owns_lock','exit_code':3}
            if outcome['exit_code']==0:state['last_session']['supervisor_reason']='unexpected_clean_worker_exit'
            state['rapid_failures']=state['rapid_failures']+1 if elapsed<600 else 1
            if state['rapid_failures']>=3:
                publish('blocked_restart_limit');return {'status':'blocked_restart_limit','exit_code':2}
            publish('restart_backoff')
            deadline=time.monotonic()+restart_delay_seconds
            while time.monotonic()<deadline:
                if (runtime/'STOP').exists():break
                time.sleep(min(1,max(0,deadline-time.monotonic())))
    finally:lock.release()


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--worker-duration-seconds',type=float,default=3600)
    parser.add_argument('--max-sessions',type=int,default=0)
    args=parser.parse_args()
    try:result=supervise(worker_duration_seconds=args.worker_duration_seconds,max_sessions=args.max_sessions)
    except Exception as exc:result={'status':'supervisor_failed','error_type':type(exc).__name__,'exit_code':2}
    print(json.dumps(result),flush=True);raise SystemExit(result['exit_code'])
