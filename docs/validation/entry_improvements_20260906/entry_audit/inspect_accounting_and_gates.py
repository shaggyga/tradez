"""Bounded saved account records and executor gating, with no broker access."""
from collections import Counter,defaultdict
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sqlite3

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent.parent/'trad'
STATE=ROOT/'data/oanda_training_manager/state'
LOG=ROOT/'data/oanda_training_manager/logs'
START='2026-08-30T21:00:00+00:00';END='2026-09-04T21:00:00+00:00'
parse=lambda s:datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
lo,hi=parse(START),parse(END)
path=STATE/'governed_practice_accounting_v1.sqlite'
before=path.stat()
assert before.st_size<200_000_000
assert not path.with_name(path.name+'-wal').exists(), 'Require no WAL for immutable read'
database_sha=hashlib.sha256(path.read_bytes()).hexdigest()
with sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)as db:
    db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
    calls=[0]
    def bounded():
        calls[0]+=1
        return int(calls[0]>3000)
    db.set_progress_handler(bounded,1000)
    schema=[dict(row)for row in db.execute("SELECT type,name,sql FROM sqlite_master WHERE type IN ('table','index')")]
    counts={table:db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]for table in
            ('account_snapshots','execution_attributions','routeability_sentinels','account_snapshot_validity')}
    assert counts['account_snapshots']<=5000 and counts['execution_attributions']<=100
    executions=[dict(row)for row in db.execute('SELECT client_id,candidate_id,submitted_epoch,status,trade_id,accounting_bucket,family,attribution_valid,attribution_reason FROM execution_attributions')]
    snapshots=[dict(row)for row in db.execute('''SELECT s.snapshot_id,s.observed_utc,s.last_transaction_id,s.balance,s.nav,s.cumulative_pl,
        s.open_trade_count,s.pending_order_count,v.is_valid,v.snapshot_state,v.reason FROM account_snapshots s
        JOIN account_snapshot_validity v USING(snapshot_id) ORDER BY s.observed_utc''')]
    sentinels=[dict(row)for row in db.execute('SELECT sentinel_date,observed_utc,passed,submission_attempted FROM routeability_sentinels WHERE sentinel_date>=? AND sentinel_date<=?',(START[:10],END[:10]))]
after=path.stat();assert(before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns)
week_executions=[row for row in executions if lo<=row['submitted_epoch']<hi]
week_snapshots=[row for row in snapshots if lo<=parse(row['observed_utc'])<hi]
valid=[row for row in snapshots if row['is_valid']==1 and row['snapshot_state']=='current']
valid_week=[row for row in valid if lo<=parse(row['observed_utc'])<hi]
before_week=max((row for row in valid if parse(row['observed_utc'])<lo),key=lambda r:r['observed_utc'],default=None)
after_week=min((row for row in valid if parse(row['observed_utc'])>=hi),key=lambda r:r['observed_utc'],default=None)
small_capture={}
for name in ('practice_007_top_executor_heartbeat_v1.json','practice_007_governed_canary_authorization_v1.json',
             'practice_007_signal_snapshot_v1.json','governed_practice_accounting_v1.json'):
    small=STATE/name;raw=small.read_bytes();payload=json.loads(raw)
    # Do not include the full account identifier in the review capture.
    for key in ('account_id','account_scope'):payload.pop(key,None)
    small_capture[name]={'sha256':hashlib.sha256(raw).hexdigest(),'payload':payload}
accounting={'database':str(path.relative_to(ROOT)),'database_sha256':database_sha,'bytes':before.st_size,
    'query_mode':'mode=ro&immutable=1; query_only; no WAL; bounded tables after metadata/schema inspection',
    'progress_vm_step_limit':3_000_000,'counts':counts,'schema':schema,
    'week_execution_attributions':week_executions,'all_execution_status_counts':dict(Counter(row['status']for row in executions)),
    'latest_submission_utc':datetime.fromtimestamp(max(row['submitted_epoch']for row in executions),timezone.utc).isoformat(),
    'week_snapshot_count':len(week_snapshots),'valid_current_week_snapshot_count':len(valid_week),
    'week_snapshot_state_counts':dict(Counter(str(row['snapshot_state'])for row in week_snapshots)),
    'valid_week_transaction_ids':dict(Counter(row['last_transaction_id']for row in valid_week)),
    'valid_week_balance_values':sorted({row['balance']for row in valid_week}),
    'valid_week_nav_values':sorted({row['nav']for row in valid_week}),
    'first_valid_week_snapshot':valid_week[0]if valid_week else None,
    'last_valid_week_snapshot':valid_week[-1]if valid_week else None,
    'nearest_valid_snapshot_before_week':before_week,'nearest_valid_snapshot_after_week':after_week,
    'week_routeability_sentinels':sentinels,
    'canary_consumption_database_exists':(STATE/'practice_007_canary_consumptions_v1.sqlite').exists(),
    'saved_current_records':small_capture}
inventory=json.loads((OUT/'executor_log_scan.json').read_text())
blocks=Counter();block_ids=defaultdict(set);selected_ids=set();qualified_ids=set();source_counts=Counter()
summary_counts=Counter();cache_states=Counter();examples={};nonconflicting=[];starts=[];exit_ids=Counter();exit_sources=Counter();selection_times=[]
observed_promotions=Counter();selected_candidate_records=[];skip_sample=[]
for info in inventory['files']:
    if not info['week_rows']:continue
    with(LOG/info['file']).open(encoding='utf-8')as handle:
        for line_number,line in enumerate(handle,1):
            row=json.loads(line)
            if not lo<=parse(row['time'])<hi:continue
            if row['event']=='execution_skipped':
                for candidate in row['candidate_blocks']:
                    reason=candidate['reason'];blocks[reason]+=1;block_ids[reason].add(candidate.get('id'))
                    if reason not in examples:examples[reason]={'file':info['file'],'line':line_number,'time':row['time'],'candidate':candidate}
                    if reason!='direction_conflict_shadow_only':skip_sample.append({'time':row['time'],'candidate':candidate})
            elif row['event']=='execution_selection_summary':
                selection_times.append(parse(row['time']));summary_counts['total']+=1
                summary_counts['with_any_feed']+=bool(row['signal_feed_candidates'])
                summary_counts['with_any_qualified']+=bool(row['qualified_candidates'])
                summary_counts['with_any_nonconflicting_qualified']+=bool(row['nonconflicting_qualified_candidates'])
                summary_counts['with_preliminary_selection']+=bool(row['selected_id'])
                if row['selected_id']:selected_ids.add(row['selected_id'])
                cache_states[str(row.get('feed_cache',{}).get('state'))]+=1
                source_counts.update(row.get('feed_cache',{}).get('source_counts',{}))
                if row['nonconflicting_qualified_candidates']:
                    nonconflicting.append({'file':info['file'],'line':line_number,'time':row['time'],
                        'qualified_candidates':row['qualified_candidates'],
                        'nonconflicting':[x for x in row['top_lanes']if not x.get('direction_conflict')]})
                for top in row['top_lanes']:
                    if top.get('id'):qualified_ids.add(top['id'])
                    if top.get('signal_eligible'):
                        selected_candidate_records.append({'time':row['time'],'id':top.get('id'),'instrument':top.get('instrument'),
                            'direction':top.get('direction'),'direction_conflict':top.get('direction_conflict'),
                            'strategy_independence_shadow_only':top.get('strategy_independence_shadow_only'),
                            'execution_validation':top.get('execution_validation')})
                for top in row['observed_top_lanes']:
                    for reason in top.get('blocked_by',[]):observed_promotions[reason]+=1
            elif row['event']=='fast_executor_start':
                starts.append({'time':row['time'],'account_suffix':row.get('account_suffix'),'practice_execution':row.get('practice_execution'),
                               'governed_canary_gate':row.get('governed_canary_gate')})
            elif row['event']=='practice_trade_exit_observed':
                exit_ids[row['trade_id']]+=1;exit_sources[row.get('source')]+=1
gaps=sorted([b-a for a,b in zip(sorted(selection_times),sorted(selection_times)[1:])],reverse=True)
detail={'window_start_inclusive':START,'window_end_exclusive':END,'sampled_summary_counts':dict(summary_counts),
    'sampled_preliminary_selected_unique_ids':len(selected_ids),'sampled_top_lane_unique_ids':len(qualified_ids),
    'block_candidate_observation_counts':dict(blocks),'unique_blocked_ids_per_reason':{k:len(v)for k,v in block_ids.items()},
    'block_examples':examples,'nonconflicting_summary_examples':nonconflicting,'cost_block_occurrences':skip_sample,
    'sampled_feed_cache_states':dict(cache_states),'sampled_feed_source_candidate_observations':dict(source_counts),
    'observed_promotion_top_evidence_block_occurrences':dict(observed_promotions),
    'in_week_starts':starts,'recovered_exit_source_counts':dict(exit_sources),'recovered_unique_trade_ids':len(exit_ids),
    'recovered_trade_id_repetition_counts':dict(Counter(exit_ids.values())),
    'selection_summary_spacing_sec':{'median':__import__('statistics').median(gaps),'largest_10':gaps[:10]},
    'warning':'Selection summaries are sampled at >=60s, skip logging is throttled, and IDs can recur. Counts are observations, not exact loop cycles or all unique upstream forecasts.'}
for filename,value in [('accounting_week_review.json',accounting),('executor_gate_detail.json',detail),('sampled_eligible_top_lane_records.json',selected_candidate_records)]:
    with(OUT/filename).open('x',encoding='utf-8')as handle:json.dump(value,handle,indent=2);handle.write('\n')
print(json.dumps({k:v for k,v in accounting.items()if k not in ('schema','saved_current_records')},indent=2))
print(json.dumps({k:v for k,v in detail.items()if k not in ('in_week_starts','nonconflicting_summary_examples')},indent=2))
