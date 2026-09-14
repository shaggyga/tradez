"""Bounded read-only evidence snapshot for the current operational profile."""
import json
from pathlib import Path
import time
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parent


def capture():
    profile=json.loads((ROOT/'config/operational_runtime_v3_20260913.json').read_bytes())
    records=[]
    for service in profile['services']:
        path=Path(service['heartbeat'])
        row={'name':service['name'],'path':str(path)}
        try:
            stat=path.stat()
            if stat.st_size>16*1024*1024:raise ValueError('heartbeat_exceeds_bound')
            value=json.loads(path.read_bytes())
            row.update(age_sec=round(time.time()-stat.st_mtime,2),schema_matches=value.get('schema_version')==service['heartbeat_schema'])
            for key in ('schema_version','status','phase','errors','last_error','successful_cycles','attempt_count',
                        'generated_utc','updated_at','new_forecasts_enabled','studies','scheduling_version','last_failure','result','counts'):
                if key in value:row[key]=value[key]
        except (OSError,ValueError) as exc:row['read_error']=str(exc)[:200]
        records.append(row)
    return {'captured_utc':datetime.now(timezone.utc).isoformat(),'services':records}


if __name__=='__main__':
    result=capture()
    path=ROOT.parent/'operational_repairs_20260913/LIVE_CURRENT.json'
    path.write_text(json.dumps(result,indent=2,allow_nan=False),encoding='utf-8')
    print(json.dumps([{'name':r['name'],**{k:r[k] for k in ('age_sec','schema_matches','status','phase','errors','last_error','read_error','successful_cycles','attempt_count','last_failure') if k in r}} for r in result['services']],indent=2))
