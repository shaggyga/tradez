"""Re-evaluate the already captured bounded sample; no database or runtime reads."""
from pathlib import Path
from collections import Counter
import hashlib,json,sys,time
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[1]/'trad'
sys.dont_write_bytecode=True;sys.path.insert(0,str(ROOT))
import oanda_entry_news_feature_join_v1 as join
def sha(raw):return hashlib.sha256(raw).hexdigest()
def save(name,value):
    raw=json.dumps(value,indent=2,sort_keys=True,allow_nan=False).encode()+b'\n'
    with (HERE/name).open('xb') as f:f.write(raw)
    return dict(path=name,sha256=sha(raw),bytes=len(raw))
receipt=json.loads((HERE/'ENTRY_NEWS_ACTUAL_SAMPLE_RECEIPT_20260909.json').read_bytes())
for ref in receipt['evidence_files']:
    raw=(HERE/ref['path']).read_bytes();assert sha(raw)==ref['sha256'] and len(raw)==ref['bytes']
snapshot=json.loads((HERE/'CURRENT_NEWS_RAW_20260909.json').read_bytes())
historical=json.loads((HERE/'BOUNDED_FACTOR_MEMORY_SAMPLE_20260909.json').read_bytes())
prior=receipt['current_snapshot'];bindings=receipt['source_bindings']
envelope=dict(complete=True,guard_version=snapshot['guard_version'],classification_version=snapshot['classification_version'],
    source_bindings=bindings,raw_source_sha256=prior['raw_sha256'],as_of_epoch=prior['source_as_of_epoch'],
    source_visible_epoch=prior['read_completed_epoch'],consumer_observed_epoch=prior['diagnostic_cutoff_epoch'],
    topics=snapshot['topics'],topics_sha256=join.digest(snapshot['topics']))
factors=historical['normalized_factor_inputs'];memory=historical['normalized_response_inputs']
for normalized,raw in zip(factors,historical['factor_rows']):normalized['factor_type']=raw['factor_type']
for normalized,raw in zip(memory,historical['response_rows']):
    normalized['factor_type']=raw['factor_type']
    normalized['original_first_seen_epoch']=None
    normalized['reported_known_epoch']=join.utc(raw['known_utc'])
observed=time.time();results={pair:join.join_entry_news_features(instrument=pair,
    cutoff_epoch=receipt['cutoff_epoch'],current_news=envelope,factor_records=factors,response_records=memory,
    expected_source_bindings=bindings,clock=lambda:observed) for pair in ('EUR_USD','GBP_USD','USD_JPY')}
ref=save('ENTRY_NEWS_FINAL_JOINS_20260909.json',results)
members=[m for topic in snapshot['topics'] for m in topic['causal_aggregation_guard']['members']]
summary=dict(status='completed',diagnostic_observed_epoch=observed,original_feature_cutoff_epoch=receipt['cutoff_epoch'],
    source_snapshot_as_of_epoch=prior['source_as_of_epoch'],source_snapshot_generated_epoch=prior['source_generated_epoch'],
    source_snapshot_read_completed_epoch=prior['read_completed_epoch'],source_snapshot_consumer_observed_epoch=prior['diagnostic_cutoff_epoch'],
    current_raw_sha256=prior['raw_sha256'],guarded_topics=len(snapshot['topics']),original_member_occurrences=len(members),
    distinct_original_member_ids=len({m['event_id'] for m in members}),
    classification_versions=dict(Counter(m['classification_version'] for m in members)),
    member_prior_move_flag_count=sum(m.get('reports_prior_market_move') is True for m in members),
    pairs={pair:dict(current_status=r['current_news']['status'],context_members=len(r['current_news']['context']),
        context_only_members=len(r['current_news']['context_only']),
        forward_context_overlap_member_count=r['current_news']['forward_context_overlap_member_count'],
        vetted_topics=len(r['current_news']['vetted']),post_move_discovery_members=len(r['current_news']['discovery']),
        current_reason=r['current_news'].get('reason'),
        rejections=dict(Counter(x['reason'] for x in r['current_news']['rejections'])),
        pair_context_support=r['news_features']['context_balance']['support_count'],
        pair_vetted_support=r['news_features']['vetted_balance']['support_count'],
        eligible_factor_sample_count=len(r['entry_factors']['eligible']),
        eligible_memory_sample_count=len(r['matured_response_memory']['eligible']),
        missing_factor_clock_fields=dict(Counter(k for x in r['entry_factors']['rejections'] for k in x.get('missing_clock_fields',[]))),
        missing_memory_clock_fields=dict(Counter(k for x in r['matured_response_memory']['rejections'] for k in x.get('missing_clock_fields',[]))),
        factor_rejections=dict(Counter(x['reason'] for x in r['entry_factors']['rejections'])),
        memory_rejections=dict(Counter(x['reason'] for x in r['matured_response_memory']['rejections']))) for pair,r in results.items()},
    original_capture_receipt_sha256=sha((HERE/'ENTRY_NEWS_ACTUAL_SAMPLE_RECEIPT_20260909.json').read_bytes()),
    join_source_sha256=sha((ROOT/'oanda_entry_news_feature_join_v1.py').read_bytes()),
    helper_sha256=sha(Path(__file__).read_bytes()),evidence_files=[ref],
    scope='Same frozen source rows and original observation/cutoff; newly evaluated after draft optional-clock parser repair. No new historical availability assigned.',
    no_database_reads=True,no_runtime_writes=True,no_fitting=True)
out=save('ENTRY_NEWS_FINAL_SAMPLE_RECEIPT_20260909.json',summary)
print(json.dumps({'receipt':out,'pairs':summary['pairs'],'members':len(members)}))
