"""Bounded read-only comparison of producer files, fresh reader and cached API."""
from datetime import datetime,timezone
from hashlib import sha256
import json,os,sys,time,urllib.request
from pathlib import Path

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad';DATA=ROOT/'data/oanda_training_manager'
sys.path.insert(0,str(ROOT))
import oanda_practice_live_dashboard as dashboard
began=time.time();stamp=datetime.fromtimestamp(began,timezone.utc).strftime('%Y%m%dT%H%M%SZ')
run=OUT/f'publication_generation_{stamp}';run.mkdir(exist_ok=False)
def digest(raw):return sha256(raw).hexdigest()
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def stat(value):return {'size':value.st_size,'mtime_ns':value.st_mtime_ns,'device':value.st_dev,'inode':value.st_ino}
def observed_file(path,target):
    started=time.time()
    with path.open('rb') as handle:
        before=os.fstat(handle.fileno());raw=handle.read(2*1024*1024+1);after=os.fstat(handle.fileno())
    current=path.stat();finished=time.time()
    assert len(raw)<=2*1024*1024
    with target.open('xb') as handle:handle.write(raw)
    return json.loads(raw),{'path':str(path),'copy_path':str(target),'raw_sha256':digest(raw),
      'started_epoch':started,'finished_epoch':finished,'handle_before':stat(before),'handle_after':stat(after),'path_after':stat(current),
      'identity_and_size_stable':stat(before)==stat(after)==stat(current)}
def projection(value):
    return {k:v for k,v in value.items() if k not in ('rows','source_bindings')}
samples=[]
for index in range(4):
    summary,smeta=observed_file(DATA/'joint_price_news_study_v2/summary.json',run/f'{index}_summary.json')
    heartbeat,hmeta=observed_file(DATA/'joint_price_news_study_v2/heartbeat.json',run/f'{index}_heartbeat.json')
    sealed={k:v for k,v in summary.items() if k!='payload_sha256'}
    fresh_started=time.time();fresh=dashboard.summarize_joint_price_news_forecasts(DATA);fresh_finished=time.time()
    api_started=time.time()
    with urllib.request.urlopen('http://127.0.0.1:8765/api/main',timeout=20) as response:api_raw=response.read(8*1024*1024+1)
    api_finished=time.time();assert len(api_raw)<=8*1024*1024
    api=json.loads(api_raw);joint=api.get('joint_price_news_forecasts',{})
    samples.append({'index':index,'producer':{'summary_file':smeta,'heartbeat_file':hmeta,
      'summary_generated_epoch':summary.get('generated_epoch'),'heartbeat_generated_epoch':heartbeat.get('generated_epoch'),
      'summary_canonical_sha256':digest(canonical(summary)),'heartbeat_bound_summary_sha256':heartbeat.get('summary_sha256'),
      'captured_pair_hashes_match':digest(canonical(summary))==heartbeat.get('summary_sha256'),
      'summary_payload_seal_valid':digest(canonical(sealed))==summary.get('payload_sha256'),
      'heartbeat_errors':heartbeat.get('errors'),'counts':heartbeat.get('counts')},
      'fresh_reader':{'call_started_epoch':fresh_started,'call_finished_epoch':fresh_finished,'result':projection(fresh)},
      'api':{'request_started_epoch':api_started,'response_finished_epoch':api_finished,'raw_response_sha256':digest(api_raw),
      'top_time':api.get('time'),'joint':projection(joint)}})
    if index<3:time.sleep(2)
result={'schema_version':'bounded_publication_generation_diagnosis_v1_20260907','started_epoch':began,'finished_epoch':time.time(),
 'source_bindings':{name:digest((ROOT/name).read_bytes()) for name in ('oanda_practice_live_dashboard.py','oanda_joint_price_news_forecast_study_v2.py')},
 'samples':samples,'runtime_or_broker_mutations':False,
 'scope':'Four bounded observations, not an uptime monitor. Exact summary/heartbeat bytes and file stats retained. API excludes account data and keeps response hash plus forecast observation.',
 'limits':['Producer files, fresh reader and HTTP response have separate clocks; later disk bytes cannot retrospectively establish the exact prior failed API generation.',
           'An API response may legitimately retain an earlier observed read; this record does not relabel that old observation as current.']}
path=run/'PUBLICATION_GENERATION_DIAGNOSIS.json'
with path.open('x',encoding='utf-8') as handle:json.dump(result,handle,indent=2,allow_nan=False);handle.write('\n')
print(json.dumps({'path':str(path),'sha256':digest(path.read_bytes()),'samples':[{'index':s['index'],
 'producer_matches':s['producer']['captured_pair_hashes_match'],'summary_epoch':s['producer']['summary_generated_epoch'],
 'heartbeat_epoch':s['producer']['heartbeat_generated_epoch'],'fresh_status':s['fresh_reader']['result'].get('status'),
 'fresh_reason':s['fresh_reader']['result'].get('reason'),'api_status':s['api']['joint'].get('status'),
 'api_reason':s['api']['joint'].get('reason'),'api_observed_epoch':s['api']['joint'].get('observed_epoch'),
 'api_response_epoch':s['api']['response_finished_epoch'],'api_top_time':s['api']['top_time']}
 for s in samples]},indent=2))
