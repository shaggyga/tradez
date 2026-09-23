"""Bounded readonly inspection of current immutable shared-capture cadence."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import statistics
import time
import zlib

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent
STUDY=ROOT/'data/oanda_training_manager/joint_price_news_study_v3'
started=time.time()
def read_json(path,limit):
    raw=path.read_bytes()
    if len(raw)>limit:raise ValueError('bounded_read_limit')
    return json.loads(raw),{'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
heartbeat,hb_binding=read_json(STUDY/'heartbeat.json',65536)
producer,producer_binding=read_json(ROOT/'data/oanda_training_manager/local_news_sentiment_repair_v1/heartbeat.json',65536)
summary,summary_binding=read_json(STUDY/'summary.json',1024*1024)
readyraw=(STUDY/'readiness.jsonl').read_bytes()
if len(readyraw)>1024*1024:raise ValueError('readiness_bound')
readiness=[json.loads(line) for line in readyraw.splitlines() if line]
all_files=sorted((STUDY/'news_captures').glob('*.json.gz'),key=lambda path:path.stat().st_mtime_ns)
selected=all_files[-32:];captures=[]
for path in selected:
    before=path.stat();stored=path.read_bytes()
    if len(stored)>8*1024*1024:raise ValueError('compressed_read_bound')
    decoder=zlib.decompressobj(16+zlib.MAX_WBITS);raw=decoder.decompress(stored,16*1024*1024+1)
    if len(raw)>16*1024*1024 or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:raise ValueError('canonical_read_bound')
    value=json.loads(raw);after=path.stat()
    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise ValueError('source_changed_during_read')
    compact={'file':path.name,'stored_bytes':len(stored),'canonical_bytes':len(raw),'stored_sha256':hashlib.sha256(stored).hexdigest(),
        'first_observed_epoch':value['first_observed_epoch'],'news_evidence_epoch':value['news_evidence_epoch'],
        'news_generated_epoch':value['news_generated_epoch'],'current_snapshot_sha256':value['current_snapshot_sha256'],
        'history_diagnostics':value['history_diagnostics'],'file_mtime_ns':before.st_mtime_ns}
    captures.append(compact)
captures.sort(key=lambda row:row['first_observed_epoch'])
completion=[row['event_observed_epoch'] for row in readiness]
completion_intervals=[b-a for a,b in zip(completion,completion[1:])]
observed_to_done=[row['event_observed_epoch']-row['observed_epoch'] for row in readiness if row.get('observed_epoch') is not None]
capture_intervals=[b['first_observed_epoch']-a['first_observed_epoch'] for a,b in zip(captures,captures[1:])]
def stats(values):return {'count':len(values),'min':min(values),'median':statistics.median(values),'max':max(values)} if values else None
span=captures[-1]['first_observed_epoch']-captures[0]['first_observed_epoch'] if len(captures)>1 else 0
bytes_per_sec=sum(row['stored_bytes'] for row in captures[1:])/span if span else None
record={'schema':'live_research_capture_cadence_diagnostic_v1_20260908','status':'completed_readonly',
    'observed_start_utc':datetime.fromtimestamp(started,timezone.utc).isoformat(),'observed_end_utc':datetime.now(timezone.utc).isoformat(),
    'heartbeat':heartbeat,'heartbeat_binding':hb_binding,'producer_heartbeat':producer,'producer_binding':producer_binding,'summary_binding':summary_binding,
    'readiness_records':readiness,'ready_capture_completions':len(readiness),'completion_interval_sec':stats(completion_intervals),
    'joint_capture_observation_to_completion_sec':stats(observed_to_done),'shared_capture_interval_sec':stats(capture_intervals),
    'shared_capture_files_total':len(all_files),'files_inspected':len(selected),'total_current_stored_bytes':sum(path.stat().st_size for path in all_files),
    'distinct_current_snapshots':len({row['current_snapshot_sha256'] for row in captures}),
    'distinct_news_evidence_epochs':len({row['news_evidence_epoch'] for row in captures}),
    'captures':captures,'estimated_stored_mib_per_hour_at_observed_rate':bytes_per_sec*3600/1024**2 if bytes_per_sec else None,
    'rate_scope':'Short initial-run interval; linear continuation illustration, not a storage forecast or completion ETA.',
    'interval_assessment':{'current_sec':15,'candidate_sec':60,'unchanged_maximum_news_age_sec':300,
      'compatible_with_source_clocks':True,
      'expected_effect':'Allows multiple captures/fit operations to reuse the same publication when history signatures also remain unchanged.',
      'limits':['History DB/WAL changes independently invalidate the current capture key, so60s publication alone does not guarantee60s reuse.',
        'Pair-specific feature projection and fitting still have costs after a shared-news cache hit.',
        'Producer heartbeat limit90s leaves about30s after a60s interval for cycle work; verify actual cycle duration and fresh publication after any restart.',
        'Original300s news freshness and member expiry remain enforced; slowing publication does not extend them.']},
    'source_or_runtime_changes':0,'broker_calls':0,'database_connections':0}
path=OUT/'LIVE_CAPTURE_CADENCE_DIAGNOSTIC_20260908.json'
with path.open('x',encoding='utf-8') as handle:json.dump(record,handle,indent=2,sort_keys=True);handle.write('\n')
print(json.dumps({key:record[key] for key in ('observed_end_utc','ready_capture_completions','completion_interval_sec',
 'joint_capture_observation_to_completion_sec','shared_capture_interval_sec','shared_capture_files_total','total_current_stored_bytes',
 'distinct_current_snapshots','estimated_stored_mib_per_hour_at_observed_rate')}))
print(json.dumps({'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'heartbeat_counts':heartbeat['counts']}))
