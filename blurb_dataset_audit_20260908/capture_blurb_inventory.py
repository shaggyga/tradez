"""One bounded read-only inventory of recovered blurb and factor evidence."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import csv,hashlib,json,sqlite3,time,zipfile

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
DATA=ROOT/'data/oanda_training_manager'
OUT=Path(__file__).resolve().parent
BUNDLE=Path(r'C:\Users\zmoor\OneDrive\thevault\PROJECT_COMMONS\artifacts\FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip')
def sha(raw):return hashlib.sha256(raw).hexdigest()
def bind(p):
    b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':sha(b)}
def now():return datetime.now(timezone.utc).isoformat()
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

QUERIES={
'spike_blurb_factor_reconstruction_v1.sqlite':{
 'base_table_counts':'''SELECT 'movement_candidates' AS table_name,COUNT(*) AS rows FROM movement_candidates
 UNION ALL SELECT 'source_attributions',COUNT(*) FROM source_attributions
 UNION ALL SELECT 'factor_observations',COUNT(*) FROM factor_observations
 UNION ALL SELECT 'movement_factor_links',COUNT(*) FROM movement_factor_links
 UNION ALL SELECT 'factor_response_observations',COUNT(*) FROM factor_response_observations
 UNION ALL SELECT 'legacy_attribution_cases',COUNT(*) FROM legacy_attribution_cases
 UNION ALL SELECT 'legacy_executable_price_windows',COUNT(*) FROM legacy_executable_price_windows
 UNION ALL SELECT 'representative_cases',COUNT(*) FROM representative_cases
 UNION ALL SELECT 'unmatched_research_episodes',COUNT(*) FROM unmatched_research_episodes
 UNION ALL SELECT 'verified_external_source_cases',COUNT(*) FROM verified_external_source_cases
 UNION ALL SELECT 'verified_external_source_documents',COUNT(*) FROM verified_external_source_documents''',
 'factor_state_counts':'''SELECT factor_contract_id,causal_state,numeric_measurement_state,currency_direction_state,COUNT(*) AS rows FROM factor_observations GROUP BY 1,2,3,4''',
 'factor_time_ranges':'''SELECT MIN(published_utc) AS first_published,MAX(published_utc) AS last_published,MIN(first_seen_utc) AS first_seen,MAX(first_seen_utc) AS last_seen,MIN(known_utc) AS first_known,MAX(known_utc) AS last_known,COUNT(DISTINCT currency) AS currencies,SUM(known_utc IS NULL OR known_utc='') AS missing_known,SUM(provenance_sha256 IS NULL OR provenance_sha256='') AS missing_provenance,SUM(forecast_proof_eligible) AS forecast_proof_eligible,SUM(execution_eligible) AS execution_eligible FROM factor_observations''',
 'source_population_counts':'''SELECT source_population,source_priority_class,causal_state,COUNT(*) AS rows FROM source_attributions GROUP BY 1,2,3''',
 'movement_link_timing':'''SELECT relation,decision_time_eligible,forecast_proof_eligible,COUNT(*) AS rows FROM movement_factor_links GROUP BY 1,2,3''',
 'movement_scope':'''SELECT COUNT(DISTINCT instrument) AS pairs,COUNT(DISTINCT market_episode_id) AS market_episodes,MIN(entry_utc) AS first_entry,MAX(exit_utc) AS last_exit,MIN(horizon_min) AS min_horizon_minutes,MAX(horizon_min) AS max_horizon_minutes,SUM(outcome_selected) AS outcome_selected,SUM(forecast_proof_eligible) AS forecast_proof_eligible FROM movement_candidates''',
 'legacy_state_counts':'''SELECT causal_use_state,price_coverage_state,COUNT(*) AS rows,SUM(legacy_predictive_flag) AS legacy_flagged_predictive,SUM(forecast_proof_eligible) AS forecast_proof_eligible FROM legacy_attribution_cases GROUP BY 1,2''',
 'legacy_scope':'''SELECT COUNT(DISTINCT instrument) AS pairs,MIN(start_utc) AS first_start,MAX(end_utc) AS last_end,SUM(primary_source_verified) AS source_verified,SUM(primary_headline IS NOT NULL AND primary_headline<>'') AS headline_present,SUM(primary_source_url IS NOT NULL AND primary_source_url<>'') AS source_url_present,SUM(availability_to_move_lead_minutes IS NOT NULL) AS availability_lead_present FROM legacy_attribution_cases''',
 'prequential_analog_coverage':'''SELECT contract_id,COUNT(*) AS rows,COUNT(DISTINCT factor_id) AS factors,COUNT(DISTINCT source_batch_id) AS source_batches,SUM(prior_matured_analog_n>0) AS rows_with_prior_matured_analogs,SUM(prequential_prediction_sign IS NOT NULL AND prequential_prediction_sign<>0) AS nonzero_predictions,SUM(prequential_correct IS NOT NULL) AS scored_predictions,MIN(known_utc) AS first_known,MAX(known_utc) AS last_known,MIN(horizon_min) AS min_horizon_minutes,MAX(horizon_min) AS max_horizon_minutes,SUM(execution_eligible) AS execution_eligible FROM factor_response_observations GROUP BY contract_id''',
 'factor_contract_provenance':'''SELECT factor_contract_id,movement_build_contract_id,parent_contract_id,created_utc,builder_sha256,source_database_path,source_database_bytes,macro_database_path,macro_database_bytes FROM factor_contracts''',
 'legacy_contract_provenance':'''SELECT contract_id,price_contract_id,created_utc,builder_sha256,tag_file_sha256 FROM legacy_attribution_contracts''',
 'movement_contract_provenance':'''SELECT build_contract_id,parent_contract_id,cohort_id,created_utc,config_sha256,builder_sha256,price_manifest_sha256 FROM reconstruction_contracts''',
},
'movement_news_episode_research_v1.sqlite':{
 'base_table_counts':'''SELECT 'movement_episodes' AS table_name,COUNT(*) AS rows FROM movement_episodes UNION ALL SELECT 'episode_source_links',COUNT(*) FROM episode_source_links''',
 'episode_scope':'''SELECT contract_id,COUNT(*) AS episodes,COUNT(DISTINCT instrument) AS pairs,COUNT(DISTINCT market_episode_id) AS market_episodes,MIN(entry_utc) AS first_entry,MAX(exit_utc) AS last_exit,MIN(horizon_min) AS min_horizon_minutes,MAX(horizon_min) AS max_horizon_minutes,SUM(pre_entry_source_count>0) AS episodes_with_pre_entry_source,SUM(pre_entry_directional_count>0) AS episodes_with_pre_entry_directional FROM movement_episodes GROUP BY contract_id''',
 'link_timing':'''SELECT relation,source_population,causal_entry_eligible,COUNT(*) AS rows FROM episode_source_links GROUP BY 1,2,3''',
 'contract_provenance':'''SELECT contract_id,created_utc,config_sha256,source_code_sha256,price_manifest_sha256,source_event_highwater_utc FROM research_contracts''',
}}
started=now();dbs={}
for name,queries in QUERIES.items():
    p=DATA/'state'/name
    c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True,timeout=5)
    c.row_factory=sqlite3.Row
    record={'path':str(p),'observed_started_utc':now(),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'query_results':{}}
    try:
        c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
        record['query_only']=c.execute('PRAGMA query_only').fetchone()[0]
        schemas=[dict(r) for r in c.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY type,name")]
        record['schema_sha256']=sha(canonical(schemas))
        for key,query in queries.items():
            deadline=time.monotonic()+12
            c.set_progress_handler(lambda: int(time.monotonic()>deadline),10000)
            t=time.monotonic()
            rows=[dict(r) for r in c.execute(query)]
            record['query_results'][key]={'sql':query,'rows':rows,'rows_sha256':sha(canonical(rows)),'duration_sec':round(time.monotonic()-t,4)}
    finally:c.rollback();c.close()
    record['observed_finished_utc']=now()
    record['scope']='Coherent read-only SQLite transaction; hash binds returned row results and schema, not entire DB or its external raw payloads.'
    dbs[name]=record

tag_path=DATA/'news_event_tags/significant_move_news_tags.csv'
tags=[]
with tag_path.open(encoding='utf-8-sig',newline='') as f:
    reader=csv.DictReader(f);fields=reader.fieldnames
    for row in reader:tags.append(row)
tag={'source':bind(tag_path),'rows':len(tags),'columns':fields,
     'pairs':len({r.get('instrument') for r in tags}),'first_start':min(r.get('start_utc','') for r in tags),
     'last_end':max(r.get('end_utc','') for r in tags),
     'causal_relation_counts':dict(Counter(r.get('primary_causal_relation') for r in tags)),
     'news_match_status_counts':dict(Counter(r.get('news_match_status') for r in tags)),
     'nonempty_primary_headlines':sum(bool(r.get('primary_headline','').strip()) for r in tags),
     'scope':'Retained movement-linked tags; stored labels are retrospective attribution candidates, not independent prospective predictions.'}
with zipfile.ZipFile(BUNDLE) as z:
    members=[{'name':i.filename,'bytes':i.file_size,'sha256':sha(z.read(i.filename))} for i in z.infolist() if not i.is_dir()]
    assert z.testzip() is None
archive={'source':bind(BUNDLE),'file_count':len(members),'members':members,
         'python_files':sum(m['name'].endswith('.py') for m in members),
         'xlsx_files':sum(m['name'].endswith('.xlsx') for m in members),
         'csv_jsonl_parquet_sqlite_corpus_files':[m['name'] for m in members if m['name'].endswith(('.csv','.jsonl','.parquet','.sqlite','.db'))],
         'scope':'Recovered research source package plus worked workbook/docs; not the later bulk blurb/episode database.'}
sources=[ROOT/'config/pure_change_strategy_space_v1.json',ROOT/'config/news_blurb_episode_schema_v1.json',
 ROOT/'config/spike_blurb_factor_reconstruction_v1.json',ROOT/'oanda_spike_blurb_factor_ledger.py',
 ROOT/'oanda_spike_blurb_factor_reconstruction.py',ROOT/'oanda_spike_blurb_movement_inventory.py',
 ROOT/'oanda_spike_blurb_legacy_attribution_ledger.py',ROOT/'oanda_spike_blurb_factor_response_analogs.py',
 ROOT/'oanda_spike_blurb_factor_response_analogs_v2.py',
 DATA/'reports/news_blurb_recovery_20260809/NEWS_BLURB_RECOVERY_AUDIT_20260809.md']
result={'schema':'retained_blurb_dataset_audit_v1','status':'completed_with_scope_limits',
        'observed_started_utc':started,'observed_finished_utc':now(),'archive':archive,'legacy_tags':tag,'databases':dbs,
        'source_bindings':[bind(p) for p in sources],
        'method':'No source imports, fitting, deserialization, network requests, source/runtime mutation or DB writes. Fixed bounded queries in mode=ro/query_only transactions; no all-experiment sweep.',
        'limits':['No claim that the recovered source ZIP includes a bulk historical blurb corpus.',
                  'Earlier constant macro fields in the227/220matrix and absent news columns in795 remain true; these separate nonempty blurb/factor datasets do not imply those specific fitted models consumed them.',
                  'Outcome-selected movements, pre-entry eligibility labels and retrospective prequential replays do not establish prospective profitable forecasts.']}
target=OUT/'BLURB_DATASET_INVENTORY_20260908.json'
with target.open('x',encoding='utf-8',newline='\n') as f:json.dump(result,f,indent=2,sort_keys=True,allow_nan=False);f.write('\n')
print(json.dumps({'file':str(target),'sha256':sha(target.read_bytes()),'bytes':target.stat().st_size,
 'archive_files':len(members),'tags_rows':len(tags),
 'database_results':{n:{k:v['rows'] for k,v in d['query_results'].items() if k not in ('source_population_counts',)} for n,d in dbs.items()}},indent=2))
