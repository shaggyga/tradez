"""Read-only current runtime status; fresh heartbeats do not prove forecasts."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import time
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parent


def observation_epoch(value):
    for key in ('generated_epoch','generated_utc','updated_at','updated_utc','observed_utc','report_epoch'):
        if key not in value: continue
        raw=value[key]
        if isinstance(raw,bool): raise ValueError('invalid observation clock')
        if isinstance(raw,(int,float)): epoch=float(raw)
        else:
            parsed=datetime.fromisoformat(str(raw).replace('Z','+00:00'))
            if parsed.tzinfo is None: raise ValueError('observation clock must include timezone')
            epoch=parsed.timestamp()
        if not math.isfinite(epoch) or epoch<=0: raise ValueError('invalid observation clock')
        return epoch
    raise ValueError('embedded observation clock unavailable')


def capture(profile_path=ROOT/'config/operational_runtime_current_20260930.json', *, now=None):
    now=time.time() if now is None else now
    profile_raw=Path(profile_path).read_bytes()
    profile=json.loads(profile_raw)
    records=[]
    for service in profile['services']:
        path=Path(service['heartbeat'])
        row={'name':service['name'],'path':str(path),'fresh':False}
        try:
            stat=path.stat()
            if stat.st_size>16*1024*1024:raise ValueError('heartbeat_exceeds_bound')
            value=json.loads(path.read_bytes())
            matches=str(value.get(service.get('heartbeat_field','schema_version')))==str(service['heartbeat_schema'])
            age=now-observation_epoch(value)
            row.update(age_sec=round(age,2),schema_matches=matches,fresh=matches and 0<=age<=service['max_age_sec'])
            for key in ('schema_version','status','phase','errors','last_error','successful_cycles','attempt_count',
                        'generated_utc','updated_at','new_forecasts_enabled','studies','scheduling_version','last_failure','result','counts','isolated_reason'):
                if key in value:row[key]=value[key]
        except (OSError,ValueError,TypeError,KeyError) as exc:row['read_error']=str(exc)[:200]
        records.append(row)
    controllers=[]
    for name,filename,schema in (
        ('supervisor','operational_supervisor_v2.json','operational_supervisor_v6_20260916'),
        ('watchdog','oanda_supervisor_watchdog_v2.json','oanda_supervisor_watchdog_v6')):
        path=ROOT/'data/oanda_training_manager/state'/filename
        row={'name':name,'path':str(path),'fresh':False,'profile_matches':False}
        try:
            if path.stat().st_size>1024*1024:raise ValueError('controller_receipt_exceeds_bound')
            value=json.loads(path.read_bytes())
            age=now-observation_epoch(value)
            matches=value.get('operational_profile_sha256')==hashlib.sha256(profile_raw).hexdigest()
            row.update(age_sec=round(age,2),profile_matches=matches,
                fresh=matches and value.get('schema_version')==schema and 0<=age<=150)
            for key in ('error','operational_health','action','recovery_allowed','restart_circuit_open'):
                if key in value:row[key]=value[key]
        except (OSError,ValueError,TypeError,KeyError) as exc:row['read_error']=str(exc)[:200]
        controllers.append(row)
    return {'captured_utc':datetime.fromtimestamp(now,timezone.utc).isoformat(),
            'profile_path':str(Path(profile_path).resolve()),'services':records,
            'controllers':controllers,
            'recovery_until_utc':profile.get('recovery_until_utc'),
            'research_only':profile.get('research_only'),'can_place_orders':profile.get('can_place_orders'),
            'scope':'Heartbeat freshness and reported errors; not forecast success or trading readiness.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',type=Path,default=ROOT/'config/operational_runtime_current_20260930.json')
    parser.add_argument('--output',type=Path,help='Optional new receipt; refuses overwrite')
    args=parser.parse_args();text=json.dumps(capture(args.profile),indent=2,allow_nan=False)
    if args.output:
        with args.output.open('x',encoding='utf-8') as handle:handle.write(text+'\n')
    print(text)
