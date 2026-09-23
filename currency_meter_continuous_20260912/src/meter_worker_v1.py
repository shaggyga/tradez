"""Single-writer supervisor for bounded passive currency-state capture attempts."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
import time
import uuid

from meter_capture_v1 import ROOT, FLAGS, COHORT, load_profile, verify_release, compact_capture
import meter_store_v1 as store
from meter_runtime_support_v1 import ProcessLock, ClockGuard, choose_bucket, atomic_json, run_bounded

STATUS_SCHEMA='continuous_currency_meter_worker_status_v1_20260912'
MAX_EVENT_BYTES=8*1024*1024


def iso(stamp):return datetime.fromtimestamp(stamp,timezone.utc).isoformat()


def prior_state(path,release_sha):
    if not path.exists():return {}
    with path.open('rb') as f:raw=f.read(128*1024+1)
    if len(raw)>128*1024:raise ValueError('prior_status_oversized')
    value=json.loads(raw)
    if value.get('schema_version')!=STATUS_SCHEMA or value.get('release_sha256')!=release_sha:
        raise ValueError('prior_status_identity_mismatch')
    if any(value.get(k) is not v for k,v in FLAGS.items()):raise ValueError('prior_status_flags_changed')
    return value


def write_event(path,event):
    raw=json.dumps(event,sort_keys=True,separators=(',',':'),allow_nan=False).encode()+b'\n'
    if len(raw)>8192:raise ValueError('diagnostic_event_oversized')
    if path.exists() and path.stat().st_size+len(raw)>MAX_EVENT_BYTES:raise ValueError('diagnostic_capacity_reached')
    with path.open('ab') as f:f.write(raw);f.flush();os.fsync(f.fileno())


def run(*,duration_seconds=0,poll_seconds=2):
    if type(duration_seconds) not in (int,float) or not math.isfinite(duration_seconds) or duration_seconds<0:
        raise ValueError('invalid_duration')
    if type(poll_seconds) not in (int,float) or not math.isfinite(poll_seconds) or not 0<poll_seconds<=30:
        raise ValueError('invalid_poll_interval')
    release=verify_release();profile=load_profile()
    runtime=ROOT/'runtime';runtime.mkdir(exist_ok=True)
    lock=ProcessLock(runtime/'worker.lock')
    if not lock.acquire():return {'status':'already_running','exit_code':3}
    status_path=runtime/'worker_status.json';event_path=runtime/'worker_events.jsonl'
    state=None
    try:
        previous=prior_state(status_path,release)
        highwater=previous.get('wall_highwater_epoch',0)
        for value in (previous.get('heartbeat_utc'),(previous.get('last_capture') or {}).get('observed_ready_utc'),(previous.get('last_attempt') or {}).get('completed_utc')):
            if value:highwater=max(highwater,datetime.fromisoformat(value).timestamp())
        guard=ClockGuard(prior_highwater=highwater)
        started=time.monotonic();run_id=uuid.uuid4().hex
        state={'schema_version':STATUS_SCHEMA,'release_sha256':release,'pid':os.getpid(),'run_id':run_id,
               'started_utc':iso(time.time()),'phase':'starting','last_capture':previous.get('last_capture'),
               'last_attempt':previous.get('last_attempt'),'attempts':previous.get('attempts',0),
               'published_captures':previous.get('published_captures',0),'capture_errors':previous.get('capture_errors',0),
               'heartbeat_write_errors':previous.get('heartbeat_write_errors',0),'diagnostic_write_errors':previous.get('diagnostic_write_errors',0),
               'clock_errors':previous.get('clock_errors',0),'observation_note':'collection status is not forecast success',**FLAGS}
        last_attempted_bucket=previous.get('last_attempted_bucket')
        recovery_bucket=None
        database=(ROOT/profile['database']).resolve()
        if database.exists() and last_attempted_bucket is not None:
            saved=store.find_bucket_status(database,last_attempted_bucket,cohort_id=COHORT)
            if saved:
                header=store.publication_header(database,saved['capture_id'])
                highwater=max(highwater,datetime.fromisoformat(header['computation_completed_utc']).timestamp())
                if header['published']:highwater=max(highwater,datetime.fromisoformat(header['observed_ready_utc']).timestamp())
                # Full payload recovery remains inside the bounded child process.
                recovery_bucket=last_attempted_bucket
            state['storage']=store.stats(database)
            state['published_captures']=state['storage']['published_captures']
        guard=ClockGuard(prior_highwater=highwater)
        last_heartbeat=-1e9
        def event(code,**fields):
            try:write_event(event_path,{'observed_utc':iso(time.time()),'run_id':run_id,'event':code,**fields})
            except (OSError,ValueError):state['diagnostic_write_errors']+=1
        def heartbeat(*,required=False):
            now=time.time();state['heartbeat_utc']=iso(now)
            capture=state.get('last_capture')
            if capture:
                age=now-datetime.fromisoformat(capture['context_clock_utc']).timestamp()
                ready=datetime.fromisoformat(capture['observed_ready_utc']).timestamp()
                state['latest_capture_current']=0<=age<=300 and ready<=now and capture['available'] and state['phase']!='blocked_clock_integrity'
                state['latest_capture_age_seconds']=age
            else:state['latest_capture_current']=False
            try:atomic_json(status_path,state)
            except OSError:
                state['heartbeat_write_errors']+=1
                if required:raise
        event('worker_started',pid=os.getpid())
        while True:
            wall=time.time();mono=time.monotonic()
            try:
                guard.check(wall,mono)
                state['wall_highwater_epoch']=max(previous.get('wall_highwater_epoch',0),state.get('wall_highwater_epoch',0),wall)
            except ValueError:
                state['clock_errors']+=1;state['phase']='blocked_clock_integrity'
                event('clock_integrity_blocked');heartbeat()
                return {'status':'blocked_clock_integrity','exit_code':2}
            if (runtime/'STOP').exists() or (duration_seconds>0 and mono-started>=duration_seconds):
                state['phase']='stopped';event('worker_stopped');heartbeat()
                return {'status':'stopped','exit_code':0}
            due=choose_bucket(wall,last_attempted_bucket)
            if recovery_bucket is not None:
                due={'bucket_epoch':recovery_bucket,'next_due_epoch':wall,'reason':'recover_pending_publication'}
                recovery_bucket=None
            state['next_due_utc']=iso(due['next_due_epoch'])
            if due['bucket_epoch'] is not None:
                bucket=due['bucket_epoch'];state['phase']='capturing';state['attempts']+=1
                # Persist the attempted bucket before child dispatch; restart cannot duplicate attempts.
                state['last_attempted_bucket']=bucket;last_attempted_bucket=bucket
                state['last_attempt']={'bucket_epoch':bucket,'started_utc':iso(wall),'status':'in_progress'}
                heartbeat(required=True)
                environment=dict(os.environ);environment['FOREX_ALLOW_LIVE']='0';environment['FOREX_LIVE_EXECUTE']='0'
                os.environ.update({key:environment[key] for key in ('FOREX_ALLOW_LIVE','FOREX_LIVE_EXECUTE')})
                result=run_bounded([sys.executable,str(ROOT/'src/meter_capture_v1.py'),'--bucket-epoch',str(bucket),
                                    '--dispatch-wall',str(wall),'--dispatch-monotonic',str(mono)],
                                   cwd=ROOT,timeout_seconds=75,memory_bytes=768*1024*1024)
                payload=result.get('stdout')
                state['last_attempt'].update(completed_utc=iso(time.time()),runtime=result.get('counters',{}))
                if result.get('exit_code')==0 and isinstance(payload,dict) and payload.get('status') in {'published','existing_bucket'}:
                    state['last_capture']=payload['capture'];state['last_attempt']['status']=payload['status']
                    state['storage']=payload.get('storage',state.get('storage'))
                    if state['storage']:state['published_captures']=state['storage']['published_captures']
                    state['phase']='collecting';event('capture_completed',capture_id=payload['capture']['capture_id'],reason=payload['capture']['reason'])
                else:
                    reason=(payload or {}).get('reason',result.get('reason','child_failed')) if isinstance(payload,dict) else result.get('reason','child_failed')
                    state['capture_errors']+=1;state['phase']='blocked_last_attempt'
                    state['last_attempt'].update(status='failed',reason=reason)
                    event('capture_failed',reason=reason)
                heartbeat();last_heartbeat=time.monotonic()
            elif mono-last_heartbeat>=30:
                if state['phase'] in {'starting','collecting'}:state['phase']='waiting_for_current_bucket'
                heartbeat();last_heartbeat=mono
            time.sleep(poll_seconds)
    finally:
        lock.release()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--duration-seconds',type=float,default=0)
    args=parser.parse_args()
    try:result=run(duration_seconds=args.duration_seconds)
    except Exception as exc:
        result={'status':'worker_start_or_runtime_failed','error_type':type(exc).__name__,'exit_code':2}
    print(json.dumps(result),flush=True);return result['exit_code']


if __name__=='__main__':raise SystemExit(main())
