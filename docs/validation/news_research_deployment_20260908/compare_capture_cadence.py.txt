"""One-shot readonly comparison after an explicitly supplied reload completion.

This helper does not wait, watch, change a process, import study modules or open
a database. Run once after the requested observation window has elapsed.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json
import statistics
import time
import zlib

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent
STUDY=ROOT/'data/oanda_training_manager/joint_price_news_study_v3'

def binding(path,raw):
    return {'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}

def bounded(path,limit):
    with path.open('rb') as handle:raw=handle.read(limit+1)
    if len(raw)>limit:raise ValueError('bounded_read_limit:'+path.name)
    return raw

def stats(values):
    return {'count':len(values),'min':min(values),'median':statistics.median(values),'max':max(values)} if values else None

def iso(value):return datetime.fromtimestamp(value,timezone.utc).isoformat()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--since',type=float,required=True)
    parser.add_argument('--minimum-window-sec',type=float,default=180)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();now=time.time()
    if args.since<=0 or now-args.since<args.minimum_window_sec:raise ValueError('minimum_after_window_not_elapsed')
    target=args.output.resolve()
    if target.parent!=OUT.resolve() or target.exists():raise ValueError('new_output_in_evidence_directory_required')
    baseline_path=OUT/'LIVE_CAPTURE_CADENCE_DIAGNOSTIC_20260908.json'
    baseline_raw=bounded(baseline_path,2*1024*1024);baseline=json.loads(baseline_raw)
    ready_path=STUDY/'readiness.jsonl';ready_raw=bounded(ready_path,2*1024*1024)
    complete_lines=ready_raw.splitlines()
    partial_tail=None
    if ready_raw and not ready_raw.endswith(b'\n'):
        partial_tail=complete_lines.pop().decode('utf-8',errors='replace')
    readiness=[json.loads(line) for line in complete_lines if line]
    readiness=[row for row in readiness if args.since<=row['event_observed_epoch']<=now]
    all_files=sorted((STUDY/'news_captures').glob('*.json.gz'),key=lambda path:path.stat().st_mtime_ns)
    # Only immutable artifacts whose filesystem completion could fall in the
    # window are opened; selection below uses the actual source-observed clock.
    selected=[path for path in all_files if path.stat().st_mtime>=args.since-30]
    if len(selected)>96:raise ValueError('bounded_after_capture_file_count')
    captures=[];crossing=[]
    for path in selected:
        before=path.stat();stored=bounded(path,8*1024*1024)
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS);raw=decoder.decompress(stored,16*1024*1024+1)
        if len(raw)>16*1024*1024 or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:raise ValueError('canonical_read_bound')
        value=json.loads(raw);after=path.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise ValueError('immutable_source_changed_during_read')
        row={'file':path.name,'stored_bytes':len(stored),'canonical_bytes':len(raw),'stored_sha256':hashlib.sha256(stored).hexdigest(),
            'first_observed_epoch':value['first_observed_epoch'],'news_evidence_epoch':value['news_evidence_epoch'],
            'news_generated_epoch':value['news_generated_epoch'],'current_snapshot_sha256':value['current_snapshot_sha256'],
            'history_rows':value['history_diagnostics']['selected_rows'],'history_raw_bytes':value['history_diagnostics']['raw_payload_bytes'],
            'history_read_duration_sec':value['history_diagnostics']['read_duration_sec'],
            'pre_reload_publication':value['news_generated_epoch']<args.since}
        if args.since<=row['first_observed_epoch']<=now:captures.append(row)
        elif row['first_observed_epoch']<args.since<=before.st_mtime:crossing.append(row)
    captures.sort(key=lambda row:row['first_observed_epoch'])
    heartbeats={};bindings=[]
    for label,path in [('joint',STUDY/'heartbeat.json'),('producer',ROOT/'data/oanda_training_manager/local_news_sentiment_repair_v1/heartbeat.json')]:
        raw=bounded(path,65536);heartbeats[label]=json.loads(raw);bindings.append(binding(path,raw))
    completion=[row['event_observed_epoch'] for row in readiness]
    intervals=[b-a for a,b in zip(completion,completion[1:])]
    observed_to_done=[row['event_observed_epoch']-row['observed_epoch'] for row in readiness if row.get('observed_epoch') is not None]
    elapsed=now-args.since;stored=sum(row['stored_bytes'] for row in captures)
    after_metrics={'window_sec':elapsed,'shared_captures':len(captures),'distinct_publications':len({row['current_snapshot_sha256'] for row in captures}),
        'completed_pair_captures':len(readiness),'distinct_completed_pairs':len({row['instrument'] for row in readiness}),
        'shared_files_per_completed_pair_capture':len(captures)/len(readiness) if readiness else None,
        'completed_pair_captures_per_minute':len(readiness)*60/elapsed,'stored_bytes':stored,'stored_mib_per_hour_at_window_rate':stored/1024**2*3600/elapsed,
        'completion_spacing_sec':stats(intervals),'joint_observation_to_ready_sec':stats(observed_to_done),
        'pre_reload_publication_captures':sum(row['pre_reload_publication'] for row in captures),
        'readiness_started_before_reload':sum(row.get('observed_epoch') is not None and row['observed_epoch']<args.since for row in readiness),
        'ready_status_counts':{status:sum(row['status']==status for row in readiness) for status in sorted({row['status'] for row in readiness})},
        'joint_error_delta_from_original_baseline':heartbeats['joint'].get('errors',0)-baseline['heartbeat'].get('errors',0),
        'producer_current_process_errors':heartbeats['producer'].get('errors'),'joint_counts':heartbeats['joint'].get('counts')}
    before_files=baseline['files_inspected'];before_pairs=baseline['ready_capture_completions']
    before_metrics={'completed_pair_captures':before_pairs,'shared_captures':before_files,
        'shared_files_per_completed_pair_capture':before_files/before_pairs if before_pairs else None,
        'completion_spacing_sec':baseline['completion_interval_sec'],'stored_mib_per_hour_at_observed_rate':baseline['estimated_stored_mib_per_hour_at_observed_rate'],
        'window_note':'Baseline initial-run rate uses between-capture span; after rate uses full explicit reload-to-observation window, including startup and in-flight work.'}
    record={'schema':'research_capture_cadence_after60_comparison_v1_20260908','status':'completed_readonly',
        'reload_completed_epoch':args.since,'reload_completed_utc':iso(args.since),'observed_window_end_epoch':now,'observed_window_end_utc':iso(now),
        'inspection_completed_utc':iso(time.time()),'baseline_binding':binding(baseline_path,baseline_raw),
        'before_metrics':before_metrics,'after_metrics':after_metrics,'filtered_readiness':readiness,'filtered_shared_captures':captures,
        'inflight_pre_reload_shared_artifacts':crossing,'readiness_partial_tail_preserved':partial_tail,
        'readiness_binding':binding(ready_path,ready_raw),'heartbeat_observations':heartbeats,'heartbeat_bindings':bindings,
        'all_captures_currently_on_disk':len(all_files),'files_opened':len(selected),
        'limitations':['All after rows satisfy their required actual clock cutoff; in-flight crossings are explicitly identified.',
            'Completion spacing includes capture, fit and scheduler work, not isolated model-fit latency.',
            'A short before/after operational observation is not a guarantee of future throughput, storage growth or forecast accuracy.',
            'Governance DB/WAL changes may invalidate capture cache even within a60-second current publication.'],
        'source_or_runtime_changes':0,'broker_calls':0,'database_connections':0}
    with target.open('x',encoding='utf-8') as handle:json.dump(record,handle,indent=2,sort_keys=True);handle.write('\n')
    print(json.dumps({'path':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'before':before_metrics,'after':after_metrics}))

if __name__=='__main__':main()
