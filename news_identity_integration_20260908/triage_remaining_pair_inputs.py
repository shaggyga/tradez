"""Readonly current history-clock diagnosis + two computed-only pair fits."""
from pathlib import Path
from datetime import datetime, timezone
from contextlib import closing
import hashlib
import json
import sqlite3
import sys
import time

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent/'remaining_pair_input_triage'
OUT.mkdir(exist_ok=False)
DATA=ROOT/'data/oanda_training_manager'
PAIRS=('GBP_NZD','NZD_HKD')
sys.dont_write_bytecode=True
sys.path.insert(0,str(ROOT))
import oanda_causal_forecast_inputs_joint_news_v2 as inputs

def iso(value):return datetime.fromtimestamp(value,timezone.utc).isoformat()
def bind(path):
    path=Path(path);raw=path.read_bytes()
    return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
def selected_summary():
    path=DATA/'joint_price_news_study_v3/summary.json';raw=path.read_bytes()
    if len(raw)>1024*1024:raise ValueError('summary_byte_bound')
    value=json.loads(raw)
    return {'binding':bind(path),'generated_epoch':value.get('generated_epoch'),
        'rows':[row for row in value.get('rows',[]) if row.get('instrument') in PAIRS]}

begun=time.time();registry_path=ROOT/'config/joint_price_news_study_v3_20260908.json'
registry=json.loads(registry_path.read_bytes())
for name,expected in registry['source_bindings'].items():
    assert bind(ROOT/name)['sha256']==expected,name
record={'schema':'remaining_pair_current_input_triage_v1_20260908','status':'incomplete','observed_start_utc':iso(begun),
    'pairs':list(PAIRS),'registry_binding':bind(registry_path),'source_bindings':registry['source_bindings'],
    'before_summary':selected_summary(),'fits':[],
    'actions':{'original_database_writes':0,'ledger_connections':0,'forecast_issues':0,'broker_calls':0,'source_edits':0,'worker_actions':0}}
try:
    database=DATA/'state/source_governance_v1.sqlite';pre=time.time();cutoff=iso(pre-48*3600)
    deadline=time.monotonic()+8
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as con:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        con.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
        query='''SELECT count(*) selected_rows,min(v.mapping_visible_utc) first_visibility,max(v.mapping_visible_utc) last_visibility,
          sum(julianday(v.mapping_visible_utc)>julianday(?)) visibility_after_preopen_clock,
          sum(julianday(e.effective_from_utc)>julianday(v.mapping_visible_utc)) effective_after_visibility,
          sum(julianday(v.mapping_visible_utc) IS NULL OR julianday(e.effective_from_utc) IS NULL) invalid_clock_rows
          FROM news_fast_lane_mappings_v3 m JOIN source_events e USING(source_event_id)
          JOIN news_fast_lane_batches_v3 b USING(batch_seq) JOIN news_fast_lane_visibility_v3 v USING(batch_seq)
          WHERE v.mapping_visible_utc>=?'''
        counts=dict(con.execute(query,(iso(pre),cutoff)).fetchone())
        examples=[dict(row) for row in con.execute('''SELECT e.source_event_id,e.effective_from_utc,v.mapping_visible_utc
          FROM news_fast_lane_mappings_v3 m JOIN source_events e USING(source_event_id)
          JOIN news_fast_lane_visibility_v3 v USING(batch_seq) WHERE v.mapping_visible_utc>=? AND
          (julianday(v.mapping_visible_utc)>julianday(?) OR julianday(e.effective_from_utc)>julianday(v.mapping_visible_utc)) LIMIT 20''',(cutoff,iso(pre)))]
        post=time.time()
    record['current_history_clock_observation']={'preopen_epoch':pre,'completed_epoch':post,'counts':counts,'violation_examples':examples,
        'read_transaction_pinned':True,'query_only':True,'datetime_comparison':'SQLite julianday; retained original ISO clocks shown in examples',
        'source':str(database)}
    record['clock_boundary_analysis']={
        'source_lines':{'read_epoch_before_history_open':243,'history_invocation':247,'lower_only_sql_selection':157,'two_way_future_rejection':173},
        'mechanism':'The retained observed_limit is sampled before the history connection and first pinned SQL read. A mapping committed in that gap with visibility later than the earlier limit can be selected and rejected; effective>visibility also uses the same error.',
        'historical_claim':'The old GBP_NZD readiness record retains only future_news_mapping, not the exact rejected row/observed_limit. Its historical cause cannot be reconstructed from that short record.',
        'prospective_repair_scope':'No frozen source change performed. A future version could pin/select <=actual observed cutoff with explicit excluded-future counts, or sample a separate history-observation cutoff after the pinned read and enforce it consistently; either requires tests/new source bindings.'}
    news_started=time.perf_counter();descriptor=inputs.capture_news_inputs(DATA,storage_root=OUT/'news_captures')
    news=inputs._load_news(descriptor)
    record['observed_news']={'descriptor':descriptor,'capture_duration_sec':time.perf_counter()-news_started,
        'first_observed_epoch':news['first_observed_epoch'],'evidence_epoch':news['news_evidence_epoch'],
        'generated_epoch':news['news_generated_epoch'],'history_diagnostics':news['history_diagnostics']}
    for pair in PAIRS:
        start=time.time();clock_start=time.perf_counter();pip=registry['pairs'][pair]['pip_size']
        capture=inputs.capture_inputs(DATA/'candles',pair,pip_size=pip,news_capture=descriptor)
        result=inputs.compute_predictions(capture)
        complete=time.time()
        bundle={'scope':'actual current engineering capture/fit, computed not issued; no ledger/consumer/entry/outcome',
            'pair':pair,'capture':capture,'result':result,'research_only':True,'can_place_orders':False,'can_promote':False,
            'can_authorize':False,'account_eligible':False,'proof_eligible':False}
        path=OUT/(pair+'_COMPUTED_NOT_ISSUED_20260908.json');path.write_bytes(inputs._encoded(bundle))
        prediction=result.get('predictions',{}).get(inputs.FAMILY)
        record['fits'].append({'instrument':pair,'pip_size':pip,'started_epoch':start,'completed_epoch':complete,
            'duration_sec':time.perf_counter()-clock_start,'capture_status':capture['status'],'capture_reasons':capture['reasons'],
            'status':result['status'],'reasons':result.get('reasons',[]),'readiness':capture.get('family_readiness'),
            'artifact':bind(path),'prediction':prediction,'computed_epoch':result.get('computed_epoch'),
            'issued':False,'ledger_opened':False})
    record['after_summary']=selected_summary()
    for name,expected in registry['source_bindings'].items():
        assert bind(ROOT/name)['sha256']==expected,name
    record['status']='completed_readonly_current_diagnosis'
except Exception as exc:
    record['status']='failed';record['error']=type(exc).__name__+':'+str(exc)
finally:
    record['observed_end_utc']=iso(time.time())
    path=OUT/'REMAINING_PAIR_CURRENT_INPUT_TRIAGE_20260908.json'
    with path.open('x',encoding='utf-8') as handle:json.dump(record,handle,indent=2,sort_keys=True);handle.write('\n')
    print(json.dumps({'record':bind(path),'status':record['status'],'error':record.get('error'),
        'history':record.get('current_history_clock_observation'),
        'fits':[{key:row[key] for key in ('instrument','status','reasons','capture_status','duration_sec','readiness','artifact')} for row in record['fits']]}))
if record['status']=='failed':raise SystemExit(1)
