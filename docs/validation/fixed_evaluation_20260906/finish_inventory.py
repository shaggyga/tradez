"""Bind the stopped source checkpoint and describe clocks; no SQL access."""
from pathlib import Path
import collections
import datetime as dt
import hashlib
import json
import statistics
import time

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]/'trad'
STATE=ROOT/'data/oanda_training_manager/state'
def load(path): return json.loads(path.read_text(encoding='utf-8'))
def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(16*1024*1024),b''): h.update(b)
    return h.hexdigest()
def stat(path):
    s=path.stat();return {'exists':True,'size':s.st_size,'mtime_ns':s.st_mtime_ns}
def epoch(value): return dt.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def quantiles(vals):
    vals=sorted(vals)
    return {'n':len(vals),'min':vals[0] if vals else None,'median':statistics.median(vals) if vals else None,'p95':vals[int((len(vals)-1)*.95)] if vals else None,'max':vals[-1] if vals else None}

inventory=load(OUT/'inventory.json')
rows=load(OUT/'selected_candidate_rows.json')
families=collections.defaultdict(list)
for row in rows:families[row['family']].append(row)
summary={}
for family,items in families.items():
    latencies=[];quote_ages=[];target_lags=[];recorded_exit_lags=[];missing=collections.Counter()
    for row in items:
        payload=json.loads(row['forecast_json']);reference=epoch(row['entry_time']);recorded=epoch(row['recorded_utc'])
        latencies.append(recorded-reference)
        quote_ages.append(recorded-epoch(payload['provider_quote_time']))
        for field in ('forecast_committed_utc','forecast_published_utc','features_available_utc','max_training_label_available_utc','max_training_label_maturity_utc'):
            if field not in payload: missing[field]+=1
        if row['outcome']:
            target_lags.append(epoch(row['outcome']['exit_time'])-reference-3600)
            recorded_exit_lags.append(epoch(row['outcome']['exit_time'])-recorded-3600)
    summary[family]={'forecast_universe':len(items),'matched_matured':len(target_lags),'payload_hash_verified':sum(x['payload_verified'] for x in items),'recorded_minus_original_reference_sec':quantiles(latencies),'recorded_minus_entry_provider_quote_sec':quantiles(quote_ages),'exit_minus_original_reference_plus_hour_sec':quantiles(target_lags),'exit_minus_recorded_plus_hour_sec':quantiles(recorded_exit_lags),'missing_explicit_clock_field_counts':dict(missing),'model_versions':sorted(set(x['model_version'] for x in items)),'feature_versions':sorted(set(x['feature_version'] for x in items))}
source_paths=[ROOT/p for p in inventory['database_after']]
bindings=[]
for path in source_paths:
    print('Hashing stopped checkpoint '+path.name,flush=True)
    started=time.monotonic();before=stat(path)
    if before != inventory['database_after'][str(path.relative_to(ROOT))]:raise RuntimeError('Source differs from read checkpoint')
    first_hash=sha(path);middle=stat(path);second_hash=sha(path);after=stat(path)
    if before!=middle or before!=after or first_hash!=second_hash:raise RuntimeError('Source changed during hash verification')
    bindings.append({'path':str(path.relative_to(ROOT)).replace('\\','/'),'sha256':first_hash,'second_sha256':second_hash,'stat_before':before,'stat_after':after,'hash_seconds':time.monotonic()-started})
sources=['oanda_proof_shadow_predictors.py','oanda_shadow_outcome_store.py','oanda_canonical_outcome_worker.py','data/oanda_training_manager/state/proof_cohort_registry_v1.json']
registry=load(STATE/'proof_cohort_registry_v1.json')
selected_contracts=[x for x in registry['cohorts'] if x['cohort_id'] in registry['active_cohorts'].values()]
(OUT/'fixed_cohort_contracts.json').write_text(json.dumps(selected_contracts,indent=2,sort_keys=True)+'\n',encoding='utf-8')
record={'schema':'fixed_proof_forecast_availability_inventory_v1','generated_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'fixed_pair':'EUR_USD','fixed_horizon_sec':3600,'selection_before_scores':True,'minimum_original_reference':'2026-08-29T12:36:36.550804+00:00','maximum_original_reference':'2026-09-05T02:09:17+00:00','boundary':inventory['boundary'],'database_file_bindings':bindings,'source_code_and_registry':[{'path':p,'sha256':sha(ROOT/p)} for p in sources],'extract_binding':{'path':'selected_candidate_rows.json','sha256':sha(OUT/'selected_candidate_rows.json'),'bytes':(OUT/'selected_candidate_rows.json').stat().st_size,'forecasts':len(rows)},'family_clock_diagnostics':summary,'clock_semantics':{'entry_time':'Feature snapshot generated time copied to forecast before model build; original reference time, not publication or execution.', 'recorded_utc':'utc_now sampled while appending forecast to pending Python list before flush; lower-bound queue recording clock, not a postcommit visibility certificate.', 'data_cutoff_utc':'Per-pair feature_origin_utc, or snapshot generated fallback; feature timestamp, not proof of dataset first availability.', 'training_cutoff_utc':'Same per-pair feature origin; no per-label first-availability or maturity ledger linked by retained hash.', 'entry_bid_ask':'Captured before model calculation in shared feature snapshot. Original provider quote time retained, no exact executable quote first received after forecast publication.', 'exit_time':'Quote snapshot local generation timestamp selected at recorded_utc+3600; differs from original entry_time+3600.', 'outcome_observed_utc':'Timestamp as completed result is queued for storage; not forecast-time availability.'},'strict_evaluation_feasible':False,'strict_blockers':['No retained postcommit forecast visibility certificate.','No linked historical feature first-availability / maximum training-label maturity and availability proof.','Entry quote predates computed forecast recording; no postpublication execution quote.','Stored outcome targets recorded_utc+3600 rather than original reference_time+3600.'],'archived_diagnostic_feasible':True,'archived_diagnostic_limit':'Original immutable probabilities and quotes can support a labeled archival diagnostic, but not exact fixed-horizon causal prediction or tradable execution proof.','simple_baseline_availability':{'constant_probability_0_5':'Available without fitting; score only on identical archival outcome set or future strict set.','zero_signed_move':'Available without fitting; magnitude comparator only, direction neutral.','always_up_and_always_down':'Available without fitting as transparent class-balance diagnostics.','no_trade':'Available without fitting; executable net0 and0coverage.','rolling_class_rate':'Needs prior outcomes with proven label availability before each decision; current source lacks complete strict availability certificates.','last_hour_price_momentum':'Requires input quote clocks/receipt proof and fixed lookback tolerance; snapshots for full original feature datasets are not retained in forecast payload.'}}
(OUT/'availability_diagnostic.json').write_text(json.dumps(record,indent=2,sort_keys=True)+'\n',encoding='utf-8')
print(json.dumps({'families':summary,'output':str(OUT/'availability_diagnostic.json')},indent=2))
