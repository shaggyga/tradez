#!/usr/bin/env python3
"""Durably collect actual sampled EUR_USD bid/ask messages until an exact UTC cutoff."""
from __future__ import annotations
import argparse
import csv
import ctypes
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
from oanda_eurusd_stream import credentials, parse_quote, STREAM

FIELDS = ['recording_id','segment_id','connection_id','sequence','instrument',
          'broker_time','received_time','bid','ask','mid','spread','spread_pips',
          'tradeable','initial_snapshot','timestamp_to_receipt_ms']


def utc():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    temp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    with temp.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--end-utc', required=True)
    args = parser.parse_args()
    end = datetime.fromisoformat(args.end_utc.replace('Z','+00:00'))
    if end.tzinfo is None:
        parser.error('--end-utc must specify timezone')
    deadline = end.timestamp()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    # OS lock is released on exit/crash. It prevents duplicate writers on restart.
    lock = (out/'writer.lock').open('a+b')
    lock.seek(0); lock.write(b'1'); lock.flush(); lock.seek(0)
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print('Another recorder owns this dataset', flush=True)
        return 0
    contract_path = out/'recording.json'
    if contract_path.exists():
        contract = json.loads(contract_path.read_text(encoding='utf-8'))
        if contract['end_utc'] != end.isoformat() or contract['instrument'] != 'EUR_USD':
            raise ValueError('Existing recording contract does not match requested cutoff/instrument')
        if contract.get('user_stopped'):
            print('Recording was stopped by user; refusing restart', flush=True)
            return 0
    else:
        contract = dict(recording_id='eurusd-'+uuid.uuid4().hex[:12], instrument='EUR_USD',
                        source='OANDA practice v20 sampled pricing stream',
                        created_utc=utc(), end_utc=end.isoformat(), user_stopped=False,
                        maximum_quotes_per_second=4, sampling_window_ms=250,
                        regular_grid=False, historical_backfill=False,
                        columns=FIELDS)
        atomic_json(contract_path, contract)
    status_path = out/'status.json'
    if time.time() >= deadline:
        prior=json.loads(status_path.read_text()) if status_path.exists() else {}
        prior.update(state='cutoff_reached', updated_utc=utc(), pid=None)
        atomic_json(status_path, prior)
        print('Cutoff reached; no new stream started', flush=True)
        return 0
    token, account = credentials()
    segment = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    csv_path = out/('quotes-'+segment+'.csv')
    events_path = out/'events.jsonl'
    source_hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in
                   [Path(__file__),Path(__file__).with_name('oanda_eurusd_stream.py')]}
    state=dict(recording_id=contract['recording_id'], segment_id=segment, pid=os.getpid(),
               state='starting', started_utc=utc(), updated_utc=utc(), end_utc=end.isoformat(),
               csv_file=csv_path.name, segment_rows=0, connections=0, invalid_records=0,
               last_quote_time=None, last_receive_time=None, last_heartbeat_time=None,
               source_sha256=source_hashes, keep_awake=False)
    if os.name == 'nt':
        # Temporary process-scoped system-awake request; does not keep display on.
        state['keep_awake']=bool(ctypes.windll.kernel32.SetThreadExecutionState(0x80000001))
    with csv_path.open('x',newline='',encoding='utf-8') as data, events_path.open('a',encoding='utf-8') as journal:
        writer=csv.DictWriter(data,fieldnames=FIELDS); writer.writeheader(); data.flush()
        def event(kind, **fields):
            journal.write(json.dumps(dict(utc=utc(),event=kind,segment_id=segment,**fields))+'\n')
            journal.flush(); os.fsync(journal.fileno())
        def checkpoint():
            data.flush(); os.fsync(data.fileno())
            state['updated_utc']=utc(); atomic_json(status_path,state)
        event('segment_started',csv_file=csv_path.name,source_sha256=source_hashes)
        checkpoint()
        failures=0; last_save=time.monotonic()
        try:
            while time.time()<deadline:
                if (out/'STOP').exists():
                    contract['user_stopped']=True; atomic_json(contract_path,contract)
                    state['state']='user_stopped'; break
                try:
                    state['state']='connecting'; checkpoint()
                    request=urllib.request.Request(STREAM+'/v3/accounts/'+urllib.parse.quote(account,safe='')+
                        '/pricing/stream?instruments=EUR_USD&snapshot=true',
                        headers={'Authorization':'Bearer '+token,'Accept':'application/json'},method='GET')
                    with urllib.request.urlopen(request,timeout=max(.1,min(12,deadline-time.time()))) as response:
                        state['connections']+=1; connection=state['connections']; initial=True
                        state['state']='recording'; event('connected',connection_id=connection); checkpoint()
                        while time.time()<deadline and not (out/'STOP').exists():
                            raw=response.readline(262145); received=time.time()
                            if received>=deadline: break
                            if not raw: raise EOFError()
                            if len(raw)>262144: raise ValueError('oversized_record')
                            payload=json.loads(raw)
                            if not isinstance(payload,dict):
                                state['invalid_records']+=1; continue
                            if payload.get('type')=='HEARTBEAT':
                                state['last_heartbeat_time']=utc()
                            else:
                                q=parse_quote(payload,received,connection,initial)
                                if q is None:
                                    state['invalid_records']+=1
                                    event('invalid_quote')
                                else:
                                    bid=Decimal(payload['bids'][0]['price']); ask=Decimal(payload['asks'][0]['price'])
                                    state['segment_rows']+=1
                                    stamp=datetime.fromtimestamp(received,timezone.utc).isoformat()
                                    writer.writerow(dict(recording_id=contract['recording_id'],segment_id=segment,
                                        connection_id=connection,sequence=state['segment_rows'],instrument='EUR_USD',
                                        broker_time=q['broker_time'],received_time=stamp,
                                        bid=str(bid),ask=str(ask),mid=str((bid+ask)/2),spread=str(ask-bid),
                                        spread_pips=str((ask-bid)/Decimal('0.0001')),tradeable=q['tradeable'],
                                        initial_snapshot=initial,timestamp_to_receipt_ms=round(q['timestamp_age_ms'],3)))
                                    # Make every row immediately accessible, fsync at checkpoint.
                                    data.flush()
                                    state['last_quote_time']=q['broker_time']; state['last_receive_time']=stamp
                                    initial=False; failures=0
                            if time.monotonic()-last_save>=5:
                                checkpoint(); last_save=time.monotonic()
                    if time.time()<deadline and not (out/'STOP').exists():
                        raise EOFError()
                except Exception as exc:
                    failures+=1; state['state']='reconnecting'
                    detail=('HTTP '+str(exc.code)) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
                    event('disconnected',reason=detail,connection_id=state['connections'])
                    checkpoint()
                    time.sleep(max(0,min(15,2**min(failures-1,4),deadline-time.time())))
            if (out/'STOP').exists():
                contract['user_stopped']=True; atomic_json(contract_path,contract); state['state']='user_stopped'
            elif time.time()>=deadline:
                state['state']='cutoff_reached'
        except KeyboardInterrupt:
            state['state']='interrupted'; event('process_interrupted')
        finally:
            state['pid']=None; checkpoint(); event('segment_closed',rows=state['segment_rows'],state=state['state'])
            if os.name=='nt': ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
    print(json.dumps(state),flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
