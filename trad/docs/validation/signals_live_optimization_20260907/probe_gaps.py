"""Bounded GET-only check of known missing minute candles. No archive writes."""
from datetime import datetime,timedelta,timezone
import hashlib,json
from pathlib import Path
import sys,time

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True
sys.path.insert(0,str(ROOT))
import oanda_all68_m1_forward_updater as updater

client,scope=updater.resolve_readonly_oanda_client()
assert client.base_url=='https://api-fxpractice.oanda.com'
original=client.request
def get_only(method,path,**kwargs):
    assert method=='GET' and path in {'/v3/instruments/AUD_USD/candles','/v3/instruments/NZD_USD/candles'}
    return original(method,path,**kwargs)
client.request=get_only
destination=OUT/'gap_probe'
destination.mkdir(exist_ok=False)
results=[]
for pair,stamp in [('AUD_USD','2026-09-07T10:26:00+00:00'),('NZD_USD','2026-09-07T12:35:00+00:00'),('AUD_USD','2026-09-07T13:26:00+00:00')]:
    target=datetime.fromisoformat(stamp)
    params={'price':'BAM','granularity':'M1','from':(target-timedelta(minutes=2)).isoformat(),
            'to':(target+timedelta(minutes=3)).isoformat(),'includeFirst':'true','smooth':'false'}
    requested=time.time()
    payload=client.request('GET',f'/v3/instruments/{pair}/candles',params=params)
    observed=time.time()
    artifact={'request':{'method':'GET','instrument':pair,'params':params,'requested_epoch':requested},
        'available_epoch':observed,'response':payload,'scope':'Provider response observed now; not historical availability proof.'}
    path=destination/f'{pair}_{target.strftime("%H%M")}.json'
    path.write_text(json.dumps(artifact,indent=2)+'\n',encoding='utf-8')
    returned=[{'time':c.get('time'),'complete':c.get('complete'),'volume':c.get('volume')} for c in payload.get('candles',[])]
    matched=[c for c in returned if datetime.fromisoformat(c['time'].replace('Z','+00:00'))==target]
    results.append({'instrument':pair,'missing_local_minute':stamp,'http_status':payload.get('_http_status'),
        'provider_returned_target':bool(matched),'target':matched,'nearby_candles':returned,
        'response_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'artifact':str(path.relative_to(OUT))})
receipt={'observed_utc':datetime.now(timezone.utc).isoformat(),'requests':3,'method':'GET',
    'practice_only':True,'archive_writes':0,'results':results}
(destination/'SUMMARY.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt))
