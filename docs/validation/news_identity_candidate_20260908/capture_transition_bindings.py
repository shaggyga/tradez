"""Static source/registry transition inspection; no imports, DBs or processes."""
from pathlib import Path
from datetime import datetime,timezone
import ast,hashlib,json

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
ISOLATED=Path(r'C:\Users\zmoor\Documents\forex\revamp_baseline_20260908\workspace\trad')
OUT=Path(__file__).resolve().parent
def sha(raw):return hashlib.sha256(raw).hexdigest()
def bind(path):
    if not path.exists():return {'path':str(path),'exists':False}
    raw=path.read_bytes();return {'path':str(path),'exists':True,'bytes':len(raw),'sha256':sha(raw)}
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def literal(path,key):
    for node in ast.parse(path.read_text(encoding='utf-8-sig')).body:
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id==key for t in node.targets):return ast.literal_eval(node.value)
    raise KeyError(key)
registry_path=ROOT/'config/joint_price_news_study_v2_20260907.json'
reg=json.loads(registry_path.read_bytes())
checks=[]
for name,expected in reg['source_bindings'].items():
    current=bind(ROOT/name);checks.append({**current,'registered_sha256':expected,'matches':current['sha256']==expected})
contracts=[]
for pair,item in reg['pairs'].items():
    for family,slot in item['families'].items():
        contract=slot['contract'];contracts.append({'instrument':pair,'family':family,
            'cohort_id':contract['cohorts'][family],'contract_sha256':slot['contract_sha256'],
            'contract_hash_matches':sha(encoded(contract))==slot['contract_sha256'],
            'source_bindings_match_registry':contract['source_bindings']==reg['source_bindings'],
            'inert_flags':{k:contract.get(k) for k in ('research_only','can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')}})
files=['oanda_joint_price_news_forecast_study_v2.py','oanda_causal_forecast_inputs_joint_news_v1.py',
 'oanda_causal_forecast_ledger_joint_news_v1.py','oanda_fixed_forecast_evaluation_joint_news_v1.py',
 'oanda_local_news_sentiment.py','oanda_official_release_fast_mapper.py','oanda_source_governance_news_fast_lane.py',
 'oanda_news_causal_aggregation_guard_v1.py','oanda_always_on_supervisor.ps1','start_oanda_research_collection.ps1']
plans=[
 {'path':'oanda_local_news_sentiment_repair_v1.py','role':'New repaired producer or explicit wrapper. Bind its own bytes and any imported frozen producer/helper dependencies. Keep original canonical producer file unchanged.','required_for_changed_producer':True},
 {'path':'oanda_causal_forecast_inputs_joint_news_v2.py','role':'New capture/validation closure accepts exact repaired producer source identity, explicit classifier versions, separate current/history paths and original clocks. No old capture migration.','required_for_changed_producer':True},
 {'path':'oanda_causal_forecast_ledger_joint_news_v2.py','role':'Versioned ledger dispatches validate_capture to new adapter; preserve publication/consumption/entry/target semantics. Reuse unchanged scorer if protocol semantics unchanged.','required_for_changed_producer':True},
 {'path':'oanda_joint_price_news_forecast_study_v3.py','role':'Versioned worker imports new adapter/ledger; unique registry/current/heartbeat/study paths and complete source closure. Keep fair scheduling.','required_for_changed_producer':True},
 {'path':'config/joint_price_news_study_v3_20260908.json','role':'New inert registration after final source freeze, unique actual-clock cohort suffix and new per-pair ledgers. Never alter v1/v2 registrations.','required_for_changed_producer':True},
 {'path':'oanda_official_release_fast_mapper_v4.py','role':'Needed only if repaired classifier participates in official mapping. Current mapper imports frozen canonical producer; new mapping must have separate ledger/output identity.','required_for_changed_producer':'conditional'},
 {'path':'oanda_source_governance_news_fast_lane_v4.py','role':'Versioned receipt bridge or equivalently explicit new ingestion contract for repaired news into isolated governance history; preserve true first observation/postcommit visibility. No newer classifier records inserted into old reader-selected mapping tables.','required_for_changed_producer':'conditional_on_new_governed_member_ingestion'},
]
result={'schema':'joint_news_source_transition_review_v1','observed_utc':datetime.now(timezone.utc).isoformat(),
 'status':'completed_readonly','registry':bind(registry_path),'registry_id':reg['registry_id'],'schema_version':reg['schema_version'],
 'registry_inert_flags':{k:reg.get(k) for k in ('collection_enabled','research_only','can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')},
 'registered_source_count':len(checks),'registered_source_checks':checks,'registered_contracts':contracts,
 'inspected_bindings':[bind(ROOT/name) for name in files]+[bind(ROOT.parent/'joint_price_news_20260907/scheduler_v2'/name) for name in ('prepare_registry.py','start_registered_joint_research.ps1')],
 'isolated_producer_observation':bind(ISOLATED/'oanda_local_news_sentiment.py'),
 'fixed_current_classifier_version':literal(ROOT/'oanda_causal_forecast_inputs_joint_news_v1.py','CLASSIFICATION_VERSION'),
 'fixed_guard_version':literal(ROOT/'oanda_causal_forecast_inputs_joint_news_v1.py','GUARD_VERSION'),
 'findings':[
  'Worker source set is fixed16 and every source hash is checked against registry; per-pair contracts contain same bindings.',
  'Input current snapshot requires exact v165 identity, guard identity, three producer hashes and independently replayed guard output; capture closure hashes its own source dependencies.',
  'Frozen ledger validate_capture dispatcher imports adapterv1 by fixed name; a new adapter alone cannot transparently replace its validation.',
  'History query selects all recent joined mapping rows, then rejects original classifier outsidev164/v165; mixing v166 into old selected history can block old forecasts despite separate current JSON files.',
  'Read-only diagnosis or byte-preserving archival observer can coexist without a newforecast cohort. A repaired producer changing accepted forecast evidence requires new source-bound cohort even if intended change is operational only.',
  'If classifier behavior is unchanged, an implementation-version/source-hash transition may preserve member classification semantics; never label changed semanticsv165 merely to pass old reader.',
  'Existing exact evaluator permits a distinct suffix under joint_price_news_v1_20260907.PAIR.ridge_price_news_v1; no scoring fork is needed solely for producer/adapter version changes.',
 ],
 'minimal_plan_proposed_names':plans,
 'unchanged_reusable_sources':['oanda_joint_price_news_models_v1.py','oanda_fixed_forecast_evaluation_joint_news_v1.py','oanda_pair_local_models_v2.py','oanda_causal_forecast_inputs_pair_v2.py','oanda_exact_price_scoring.py','oanda_causal_prediction_baselines.py'],
 'proposed_cohort_example':'joint_price_news_v1_20260907.EUR_USD.ridge_price_news_v1.news_repair_20260908_v1.ACTUAL_REGISTRATION_TOKEN',
 'coexistence_paths':{'old':'Current canonical producer/output/mapping history and all v1/v2 registry/ledger bytes preserved.',
 'new_news':'data/oanda_training_manager/local_news_sentiment_repair_v1/ (new current snapshot, heartbeat, classifier mapping outputs)',
 'new_history':'Separate governed new-version history with its own actual activation/visibility receipts; new adapter may read retained old history separately without changing its clocks.',
 'new_study':'data/oanda_training_manager/joint_price_news_study_v3/pairs/PAIR/ridge_price_news_v1/study.sqlite'},
 'launch_review':['Supervisor research-only allowlist/gate, exact heartbeat schema/classifier and unique workername/needle must be added only for finalized newregistration.',
 'Newworker can be prepared and preflighted offline; activation creates newempty ledgers atactualclock withzero previouspublication/outcome evidence.',
 'Existing launcher script is a dated receipt-bound execution example with PID22348; never replay its stale process identifiers.',
 'Coordinate later dashboard/runtime-health/storage inventory separately; no existing primary source fallback should mask unavailable newstudy.',
 'All research flags true/collection explicitly enabled only on approvednewregistry; order/promotion/authorization/account/proof flags remain strictfalse.'],
 'actions':{'source_edits':0,'registrations_changed':0,'process_actions':0,'database_connections':0,'broker_calls':0}}
target=OUT/'JOINT_NEWS_VERSION_TRANSITION_20260908.json'
with target.open('x',encoding='utf-8',newline='\n') as f:json.dump(result,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({'path':str(target),'sha256':sha(target.read_bytes()),'bytes':target.stat().st_size,'sources':len(checks),
 'all_registered_source_hashes_match':all(x['matches'] for x in checks),'contract_count':len(contracts),
 'all_contracts_match':all(x['contract_hash_matches'] and x['source_bindings_match_registry'] for x in contracts)}))
