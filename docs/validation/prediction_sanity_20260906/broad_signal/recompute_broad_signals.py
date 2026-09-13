"""Independent bounded read-only raw top-signal sanity check."""
import os,sys,json,sqlite3,hashlib,time,math,statistics
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import unquote,urlsplit
ROOT=Path(__file__).resolve().parents[2]/'trad'
OUT=Path(__file__).resolve().parent
DB=ROOT/'data/oanda_training_manager/state/top_signal_position_ledger_v1.sqlite'
VERSION='raw_all_signal_consensus_v5_strict_horizon_lineage'
sys.dont_write_bytecode=True

def guard(event,args):
    if event in {'socket.connect','socket.connect_ex','socket.bind','subprocess.Popen','os.system','os.startfile'}:
        raise RuntimeError('Independent sanity check forbids network/process actions')
    if event=='sqlite3.connect':
        uri=str(args[0]);parsed=urlsplit(uri);name=unquote(parsed.path)
        if os.name=='nt' and name.startswith('/') and len(name)>2 and name[2]==':':name=name[1:]
        if Path(name).resolve()!=DB.resolve() or parsed.query!='mode=ro':raise RuntimeError('Only exact read-only small ledger allowed')
    if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
        path,mode,flags=args
        writing=(isinstance(mode,str) and any(c in mode for c in 'wax+')) or (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):raise RuntimeError('Writes only to independent evidence directory')
sys.addaudithook(guard)

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()
def file_info(path):
    st=path.stat();return {'bytes':st.st_size,'mtime_ns':st.st_mtime_ns,'sha256':sha(path)}
def emit(name,value):
    with (OUT/name).open('x',encoding='utf-8') as f:json.dump(value,f,indent=2,allow_nan=False)
def epoch(value):return datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def stats(values):
    values=sorted(values)
    return {'n':len(values),'min':values[0],'median':statistics.median(values),'p95':values[min(len(values)-1,int(.95*len(values)))],'max':values[-1],'mean':statistics.fmean(values)} if values else {'n':0}
def utc(value):return datetime.fromtimestamp(value,timezone.utc).isoformat()

files=[DB,DB.with_name(DB.name+'-wal')]
before={p.name:file_info(p) for p in files}
con=sqlite3.connect(DB.resolve().as_uri()+'?mode=ro',uri=True,timeout=3)
con.row_factory=sqlite3.Row
con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
started=time.monotonic();con.set_progress_handler(lambda:1 if time.monotonic()-started>20 else 0,1000)
columns=[r[1] for r in con.execute('PRAGMA table_info(positions)') if r[1]!='direction_conflict_details_json']
query='SELECT '+','.join(columns)+" FROM positions WHERE status='matured' AND measurement_version=? AND maturity_valid=1 LIMIT 20001"
plan=[list(r) for r in con.execute('EXPLAIN QUERY PLAN '+query,(VERSION,))]
assert any('SEARCH positions USING INDEX' in str(r) for r in plan),plan
rows=[dict(r) for r in con.execute(query,(VERSION,))]
if len(rows)>20000:raise RuntimeError('Population exceeds bounded extraction limit')
status_counts={}
for status in ['open','matured','quarantined_maturity']:
    status_counts[status]=[dict(r) for r in con.execute('SELECT maturity_valid,COUNT(*) AS n FROM positions WHERE status=? AND measurement_version=? GROUP BY maturity_valid',(status,VERSION))]
revision=[dict(r) for r in con.execute('SELECT * FROM ledger_revision WHERE singleton=1')]
query_seconds=time.monotonic()-started
con.rollback();con.close()
after={p.name:file_info(p) for p in files}
assert before==after,'database or WAL changed during read-only snapshot'
with (OUT/'selected_rows.jsonl').open('x',encoding='utf-8') as f:
    for row in rows:f.write(json.dumps(row,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n')

mismatches=defaultdict(list)
recomputed=[]
for r in rows:
    def check(name,bad):
        if bad:mismatches[name].append(r['id'])
    pip=.01 if r['instrument'].endswith('_JPY') else .0001
    entry=(r['entry_bid']+r['entry_ask'])/2
    exit=(r['exit_bid']+r['exit_ask'])/2
    sign=1 if r['direction']=='buy' else -1
    gross=sign*(exit-entry)/pip
    net=((r['exit_bid']-r['entry_ask']) if sign==1 else (r['entry_bid']-r['exit_ask']))/pip
    spread_entry=(r['entry_ask']-r['entry_bid'])/pip
    spread_exit=(r['exit_ask']-r['exit_bid'])/pip
    check('invalid_direction',r['direction'] not in ['buy','sell'])
    check('invalid_bid_ask',not(0<r['entry_bid']<=r['entry_ask'] and 0<r['exit_bid']<=r['exit_ask']))
    check('entry_mid',abs(entry-r['entry_mid'])>1e-12)
    check('exit_mid',abs(exit-r['exit_mid'])>1e-12)
    check('entry_spread_pips',abs(spread_entry-r['entry_spread_pips'])>1e-7)
    check('gross_pips',abs(gross-r['gross_pips'])>1e-7)
    check('net_pips',abs(net-r['net_pips'])>1e-7)
    check('win',int(net>0)!=r['win'])
    check('spread_identity',abs(net-(gross-(spread_entry+spread_exit)/2))>1e-7)
    check('target_equals_original_entry_plus_horizon',abs(r['target_epoch']-r['opened_epoch']-r['horizon_sec'])>1e-6)
    check('exit_quote_in_target_window',not 0<=r['outcome_quote_epoch']-r['target_epoch']<=60)
    check('closed_equals_provider_quote',r['closed_epoch']!=r['outcome_quote_epoch'])
    check('distance_matches_provider_clock',abs(r['target_quote_distance_sec']-(r['outcome_quote_epoch']-r['target_epoch']))>1e-6)
    check('strict_lineage',r['signal_lineage_contract_id']!='all_signal_horizon_lineage_v1' or not r['signal_id'].startswith('aggregate_signal_'))
    check('opened_text_clock',abs(epoch(r['opened_at'])-r['opened_epoch'])>1e-6)
    check('outcome_text_clock',abs(epoch(r['outcome_quote_at'])-r['outcome_quote_epoch'])>1e-6)
    z=dict(r,gross_recomputed=gross,net_recomputed=net,raw_midpoint_move=exit-entry,spread_exit_pips=spread_exit,spread_cost=(spread_entry+spread_exit)/2,
        observed_snapshot_minus_entry_sec=epoch(r['observed_at'])-r['opened_epoch'],
        outcome_delay_sec=r['outcome_quote_epoch']-r['target_epoch'],
        gross_bps=10000*sign*(exit-entry)/entry,net_bps=10000*net*pip/entry)
    recomputed.append(z)

def summarize(group):
    n=len(group);up=sum(r['raw_midpoint_move']>0 for r in group);down=sum(r['raw_midpoint_move']<0 for r in group);flat=n-up-down
    hits=sum(r['gross_recomputed']>0 for r in group);wins=sum(r['net_recomputed']>0 for r in group)
    buy=sum(r['direction']=='buy' for r in group)
    return {'n':n,'direction_hits':hits,'direction_accuracy':hits/n,'net_positive':wins,'net_win_rate':wins/n,
        'up_moves':up,'down_moves':down,'flat_moves':flat,'nonflat_direction_accuracy':hits/(n-flat) if n>flat else None,
        'buy_predictions':buy,'sell_predictions':n-buy,'constant_buy_accuracy_descriptive':up/n,
        'constant_sell_accuracy_descriptive':down/n,'hindsight_majority_accuracy_descriptive':max(up,down)/n,
        'direction_accuracy_minus_hindsight_majority':(hits-max(up,down))/n,
        'correct_directions_that_do_not_clear_spread':sum(r['gross_recomputed']>0 and r['net_recomputed']<=0 for r in group),
        'mean_gross_pips_diagnostic':statistics.fmean(r['gross_recomputed'] for r in group),
        'mean_net_pips_diagnostic':statistics.fmean(r['net_recomputed'] for r in group),
        'mean_gross_bps_equal_row_descriptive':statistics.fmean(r['gross_bps'] for r in group),
        'mean_net_bps_equal_row_descriptive':statistics.fmean(r['net_bps'] for r in group),
        'entry_spread_pips':stats([r['entry_spread_pips'] for r in group]),
        'roundtrip_spread_drag_pips':stats([r['spread_cost'] for r in group]),
        'first_entry_quote_utc':utc(min(r['opened_epoch'] for r in group)),
        'last_entry_quote_utc':utc(max(r['opened_epoch'] for r in group))}
def grouped(key):
    buckets=defaultdict(list)
    for r in recomputed:buckets[r[key]].append(r)
    return {str(k):summarize(v) for k,v in sorted(buckets.items())}

dupes={}
for keys in [('id',),('cohort_key',),('signal_id',),('source_signal_id',),('signal_id','horizon_sec'),('source_signal_id','horizon_sec'),('instrument','opened_epoch','target_epoch','direction'),('instrument','opened_epoch','target_epoch')]:
    c=Counter(tuple(r[k] for k in keys) for r in rows)
    dupes['|'.join(keys)]={'unique':len(c),'repeated_keys':sum(n>1 for n in c.values()),'extra_rows_beyond_unique':len(rows)-len(c),'max_repetitions':max(c.values())}
quality=json.loads((ROOT/'FOREX_PREDICTION_QUALITY_20260906.json').read_bytes())
saved=quality['current_top_signal_shadow']
report={'generated_utc':datetime.now(timezone.utc).isoformat(),'scope':'Independent raw read-only verification of exact saved current top-signal population; not a tradable prediction proof',
    'database':str(DB),'query':query,'parameters':[VERSION],'query_plan':plan,'query_seconds':query_seconds,'query_row_limit':20001,
    'selected_columns':columns,'excluded_large_metadata_column':'direction_conflict_details_json','database_and_wal_before':before,'database_and_wal_after':after,'unchanged':before==after,
    'shm_caveat':'Read-only SQLite may use SHM read marks; no byte-identity assertion for SHM. No production SQL writes executed.',
    'ledger_revision':revision,'version_status_counts':status_counts,'overall':summarize(recomputed),
    'matches_saved_counts':len(rows)==saved['overall']['raw_matured_rows'] and sum(r['gross_recomputed']>0 for r in recomputed)==saved['overall']['direction_hits'] and sum(r['net_recomputed']>0 for r in recomputed)==saved['overall']['after_cost_wins'],
    'arithmetic_mismatches':{k:{'n':len(v),'example_ids':v[:20]} for k,v in mismatches.items()},
    'by_horizon':grouped('horizon_sec'),'by_policy':grouped('policy_state'),'by_instrument':grouped('instrument'),
    'family_counts':dict(Counter(r['family'] for r in rows)),'direction_source_counts':dict(Counter(r['direction_source'] for r in rows)),
    'eligibility_counts':dict(Counter(str((r['signal_eligible'],r['validated'],r['direction_conflict'])) for r in rows)),
    'maturity_reason_counts':dict(Counter(r['maturity_reason'] for r in rows)),
    'duplicates':dupes,'target_quote_delay_sec':stats([r['outcome_delay_sec'] for r in recomputed]),
    'signal_snapshot_time_minus_entry_quote_sec':stats([r['observed_snapshot_minus_entry_sec'] for r in recomputed]),
    'entry_quotes_before_signal_snapshot_time':sum(r['observed_snapshot_minus_entry_sec']>0 for r in recomputed),
    'signal_snapshot_more_than_90sec_old_at_entry_quote':sum(r['observed_snapshot_minus_entry_sec'] < -90 for r in recomputed),
    'first_signal_snapshot_time':min(r['observed_at'] for r in rows),'last_signal_snapshot_time':max(r['observed_at'] for r in rows),
    'horizon_1_to_3min_weight':sum(r['horizon_sec']<=180 for r in rows)/len(rows),
    'missing_availability_fields':'No original forecast issue/committed availability, successor consumer observation, entry/exit local receipt, or per-quote tradeable status is retained in this ledger; observed_at is signal snapshot updated_at/generated_utc, not actual observation/commit.',
    'population_caveats':['One chosen horizon candidate while previous horizon position remains open; selected diagnostic rows, not all model forecasts.','Horizon rows/pairs and reused signals are correlated; independent N and valid inference intervals unavailable.','A v5 lineage and maturity_valid=1 label verifies only the implemented filters, not original publication timing or execution feasibility.','Majority direction and equal-row bps scores are descriptive of these same observed rows, not pre-registered deployable baselines.'],
    'selected_rows_sha256':sha(OUT/'selected_rows.jsonl'),
    'source_bindings':{p.name:sha(p) for p in [ROOT/'oanda_top_signal_position_ledger.py',ROOT/'FOREX_PREDICTION_QUALITY_20260906.json']}}
emit('broad_signal_raw_verification.json',report)
print(json.dumps({k:report[k] for k in ['overall','matches_saved_counts','arithmetic_mismatches','duplicates','family_counts','direction_source_counts','target_quote_delay_sec','signal_snapshot_time_minus_entry_quote_sec','entry_quotes_before_signal_snapshot_time','signal_snapshot_more_than_90sec_old_at_entry_quote','horizon_1_to_3min_weight','query_seconds']},indent=2))
