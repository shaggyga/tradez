"""One bounded read-only current/schema/sample audit. No workers or database writes."""
from pathlib import Path
from contextlib import contextmanager
from collections import Counter
import datetime as dt
import hashlib,json,sqlite3,sys,time

BASE=Path(__file__).resolve().parents[2]
ROOT=BASE/'trad'; DATA=ROOT/'data/oanda_training_manager'; OUT=Path(__file__).resolve().parent
sys.dont_write_bytecode=True;sys.path.insert(0,str(ROOT))
import oanda_entry_news_feature_join_v1 as join
import oanda_local_news_sentiment_repair_v1 as producer
import oanda_source_governance as governance

def rawsha(raw):return hashlib.sha256(raw).hexdigest()
def save(name,value):
    raw=json.dumps(value,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
    with (OUT/name).open('xb') as f:f.write(raw)
    return {'path':name,'sha256':rawsha(raw),'bytes':len(raw)}

@contextmanager
def ro(path):
    start=time.monotonic();con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1)
    try:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        con.set_progress_handler(lambda:int(time.monotonic()-start>6),1000)
        yield con
    finally:con.close()

def schema(con,tables):
    return {name:{'columns':[dict(r) for r in con.execute('PRAGMA table_info('+name+')')],
        'indexes':[dict(r) for r in con.execute('PRAGMA index_list('+name+')')]} for name in tables}

started=time.time();bindings=producer.source_bindings();refs=[]
path=DATA/'local_news_sentiment_repair_v1/current_news_v1.json'
before=path.stat();raw=path.read_bytes();after=path.stat();read_completed=time.time()
assert before.st_size==after.st_size==len(raw) and before.st_mtime_ns==after.st_mtime_ns
assert len(raw)<=16*1024*1024
with (OUT/'CURRENT_NEWS_RAW_20260909.json').open('xb') as f:f.write(raw)
refs.append({'path':'CURRENT_NEWS_RAW_20260909.json','sha256':rawsha(raw),'bytes':len(raw)})
snapshot=json.loads(raw)
current={'read_completed_epoch':read_completed,'raw_sha256':rawsha(raw)}
try:
    producer.validate_repaired_snapshot(snapshot)
    observed=time.time();asof=join.utc(snapshot['as_of_utc'])
    envelope=dict(complete=True,guard_version=snapshot['guard_version'],classification_version=snapshot['classification_version'],
        source_bindings=bindings,raw_source_sha256=rawsha(raw),as_of_epoch=asof,
        source_visible_epoch=read_completed,consumer_observed_epoch=observed,
        topics=snapshot['topics'],topics_sha256=join.digest(snapshot['topics']))
    current.update(status='source_replay_verified',diagnostic_cutoff_epoch=observed,
        source_as_of_epoch=asof,source_generated_epoch=join.utc(snapshot['generated_utc']),
        source_age_sec=observed-asof,topics=len(snapshot['topics']))
except (ValueError,KeyError,TypeError) as exc:
    observed=time.time();envelope=None;current.update(status='source_withheld',reason=str(exc),diagnostic_cutoff_epoch=observed)

govpath=DATA/'state/source_governance_v1.sqlite';governed=[]
with ro(govpath) as con:
    govschema=schema(con,['news_fast_lane_mappings_v3','news_fast_lane_batches_v3','news_fast_lane_visibility_v3','source_events'])
    batches=[r[0] for r in con.execute('SELECT batch_seq FROM news_fast_lane_visibility_v3 ORDER BY batch_seq DESC LIMIT 32')]
    query='''SELECT e.source_event_id,e.raw_payload_sha256,e.payload_json,e.effective_from_utc,
      m.raw_payload_sha256 mapped_sha,v.mapping_visible_utc,v.availability_basis,v.consumer_first_observation_required,b.contract_id
      FROM news_fast_lane_mappings_v3 m JOIN source_events e USING(source_event_id)
      JOIN news_fast_lane_batches_v3 b USING(batch_seq) JOIN news_fast_lane_visibility_v3 v USING(batch_seq)
      WHERE m.batch_seq IN ('''+','.join('?' for _ in batches)+''') ORDER BY m.batch_seq DESC,e.source_event_id LIMIT 128'''
    plan=[list(r) for r in con.execute('EXPLAIN QUERY PLAN '+query,batches)] if batches else []
    rows=[dict(r) for r in con.execute(query,batches)] if batches else []
    assert sum(len(r['payload_json'].encode()) for r in rows)<=8*1024*1024
    for row in rows:
        payload=row.pop('payload_json');assert len(payload.encode())<=1024*1024
        material={**row,'payload_json':payload}
        verified=(governance.source_event_substantive_sha256(material)==row['raw_payload_sha256']==row['mapped_sha']
            and row['contract_id']=='news_source_governance_fast_lane_v3_committed_visibility_20260905'
            and row['availability_basis']=='post_commit_independent_mapping_read' and row['consumer_first_observation_required']==1)
        member=json.loads(payload).get('raw_payload',{})
        governed.append({**row,'payload_bytes':len(payload.encode()),'payload_text_sha256':rawsha(payload.encode()),
            'substantive_mapping_verified':verified,'event_id':member.get('event_id'),
            'classification_version':member.get('classification_version'),
            'published_utc':member.get('published_utc'),'first_seen_utc':member.get('first_seen_utc'),
            'original_observed_available_utc':member.get('observed_available_utc'),
            'observation_clock_trusted':member.get('observation_clock_trusted'),
            'currency_scores':member.get('currency_scores'),'reports_prior_market_move':member.get('reports_prior_market_move')})
gov_observed=time.time()

factorpath=DATA/'state/spike_blurb_factor_reconstruction_v1.sqlite'
with ro(factorpath) as con:
    fschema=schema(con,['factor_contracts','factor_observations','source_attributions',
        'factor_response_analog_contracts','factor_response_observations'])
    contracts=[dict(r) for r in con.execute('SELECT * FROM factor_response_analog_contracts LIMIT 16')]
    counts=[]
    for contract in contracts:
        row=dict(con.execute('''SELECT COUNT(*) n,COUNT(DISTINCT factor_id) factors,
            COUNT(DISTINCT underlying_event_id) underlying_events,
            SUM(CASE WHEN prequential_correct IS NOT NULL THEN 1 ELSE 0 END) prequential_prediction_n,
            MIN(baseline_epoch) first_baseline,MAX(target_epoch) last_target
            FROM factor_response_observations WHERE contract_id=?''',(contract['contract_id'],)).fetchone())
        counts.append({'contract_id':contract['contract_id'],**row})
    contract_id='spike_blurb_factor_response_analogs_v2_20260819'
    response_rows=[dict(r) for r in con.execute('''SELECT * FROM factor_response_observations
        WHERE contract_id=? ORDER BY analog_key,horizon_min,baseline_epoch LIMIT 128''',(contract_id,))]
    factor_rows=[dict(r) for r in con.execute('''SELECT f.*,s.source_id,s.source_event_id,s.underlying_event_id,
        s.raw_payload_sha256,s.story_cluster_id FROM factor_observations f
        JOIN source_attributions s USING(source_evidence_id) ORDER BY f.factor_id LIMIT 128''')]
factor_observed=time.time()
factor_inputs=[];memory_inputs=[]
for r in factor_rows:
    factor_inputs.append(dict(record_id=r['factor_id'],underlying_event_id=r['underlying_event_id'],currency=r['currency'],
        source_sha256=r['raw_payload_sha256'],source_version=r['factor_contract_id'],
        original_event_epoch=join.utc(r['published_utc']) if r['published_utc'] else None,
        original_first_seen_epoch=join.utc(r['first_seen_utc']) if r['first_seen_utc'] else None,
        source_visible_epoch=None,observed_available_epoch=None,feature_available_epoch=None,
        source_availability_basis='not_retained_in_sampled_factor_schema',selection_basis='unknown',
        numeric_measurement_state=r['numeric_measurement_state'],signed_factor_score=r['signed_factor_score']))
for r in response_rows:
    memory_inputs.append(dict(record_id=r['response_id'],underlying_event_id=r['underlying_event_id'],currency=r['currency'],
        source_sha256=join.digest(r),source_version=r['contract_id'],
        original_event_epoch=join.utc(r['published_utc']),original_first_seen_epoch=None,
        reported_known_epoch=join.utc(r['known_utc']),
        source_visible_epoch=None,observed_available_epoch=None,feature_available_epoch=None,
        source_availability_basis='not_retained_in_sampled_response_schema',selection_basis='retrospective_discovery',
        response_start_epoch=r['baseline_epoch'],response_target_epoch=r['target_epoch'],
        response_available_epoch=None,response_source_sha256=None,response_value_bps=r['currency_response_bps']))
cutoff=time.time()
joins={pair:join.join_entry_news_features(instrument=pair,cutoff_epoch=cutoff,current_news=envelope,
    factor_records=factor_inputs,response_records=memory_inputs,expected_source_bindings=bindings)
    for pair in ('EUR_USD','GBP_USD','USD_JPY')}
refs.append(save('BOUNDED_ORIGINAL_VISIBILITY_SAMPLE_20260909.json',dict(observed_epoch=gov_observed,
    scope='At most128 rows from latest32 observed committed batches; not a complete history population.',
    database_path=str(govpath),database_bytes=govpath.stat().st_size,query_plan=plan,schema=govschema,rows=governed)))
refs.append(save('BOUNDED_FACTOR_MEMORY_SAMPLE_20260909.json',dict(observed_epoch=factor_observed,
    scope='At most128 ordered factors and128 v2 responses; per-contract aggregate only. Not source-first validation.',
    database_path=str(factorpath),database_bytes=factorpath.stat().st_size,schema=fschema,contracts=contracts,
    contract_counts=counts,factor_rows=factor_rows,response_rows=response_rows,
    normalized_factor_inputs=factor_inputs,normalized_response_inputs=memory_inputs)))
refs.append(save('ENTRY_NEWS_FEATURE_JOINS_20260909.json',joins))
assert bindings==producer.source_bindings()
summary=dict(status='completed',started_epoch=started,completed_epoch=time.time(),current_snapshot=current,
    cutoff_epoch=cutoff,source_bindings=bindings,helper_sha256=rawsha(Path(__file__).read_bytes()),
    join_source_sha256=rawsha((ROOT/'oanda_entry_news_feature_join_v1.py').read_bytes()),
    original_visibility_sample=dict(rows=len(governed),verified=sum(r['substantive_mapping_verified'] for r in governed),
        versions=dict(Counter(r['classification_version'] for r in governed)),
        nonempty_score_maps=sum(bool(r['currency_scores']) for r in governed),
        reports_prior_move=sum(r['reports_prior_market_move'] is True for r in governed)),
    factor_response_contract_counts=counts,
    pairs={pair:dict(current_status=j['current_news']['status'],current_reason=j['current_news'].get('reason'),
        context_members=len(j['current_news']['context']),vetted_topics=len(j['current_news']['vetted']),
        post_move_discovery_members=len(j['current_news']['discovery']),
        pair_context_support=j['news_features']['context_balance']['support_count'],
        pair_forward_support=j['news_features']['vetted_balance']['support_count'],
        factor_sample_n=j['entry_factors']['input_record_count'],eligible_factor_n=len(j['entry_factors']['eligible']),
        memory_sample_n=j['matured_response_memory']['input_record_count'],eligible_memory_n=len(j['matured_response_memory']['eligible']),
        factor_reasons=dict(Counter(r['reason'] for r in j['entry_factors']['rejections'])),
        memory_reasons=dict(Counter(r['reason'] for r in j['matured_response_memory']['rejections']))) for pair,j in joins.items()},
    evidence_files=refs,no_runtime_or_database_writes=True,no_fitting_or_predictions=True,
    limitations=['Database observations have separate clocks and are bounded samples, not one atomic whole-project snapshot.',
        'No original arrival is assigned from this new read; absent historical visibility/outcome-known fields stay missing.',
        'Actual factor sample metadata does not certify a source-first universe or make retrospective response rows causal memory.'])
ref=save('ENTRY_NEWS_ACTUAL_SAMPLE_RECEIPT_20260909.json',summary)
print(json.dumps({'receipt':ref,'pairs':summary['pairs'],'visibility':summary['original_visibility_sample'],'analog':counts}))
