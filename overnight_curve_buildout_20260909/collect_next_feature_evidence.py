"""Bounded reuse of existing audits; no fitting, DB/network or source changes."""
from datetime import datetime,timezone
import csv
import hashlib
import io
import json
from pathlib import Path
import time

BASE=Path('C:/Users/zmoor/Documents/forex')
PROJECT=BASE/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
OUT=BASE/'overnight_curve_buildout_20260909/richer_inputs'
OUT.mkdir(exist_ok=True)
EVIDENCE=PROJECT/'docs/validation/existing_feature_horizon_20260908'
model=json.loads((EVIDENCE/'EXISTING_CURVE_MODEL_EVIDENCE_20260908.json').read_text())
ma=json.loads((EVIDENCE/'MA_GRID_EXISTING_CURVE_AUDIT_20260908.json').read_text())
blurb_path=PROJECT/'docs/validation/blurb_dataset_20260908/BLURB_DATASET_INVENTORY_20260908.json'
blurb=json.loads(blurb_path.read_text())


def binding(path,expected=None):
    path=Path(path)
    if not path.exists():
        return {'path':str(path),'exists':False,'expected_sha256_from_prior_audit':expected}
    info=path.stat()
    value={'path':str(path),'exists':True,'bytes':info.st_size,
        'mtime_utc':datetime.fromtimestamp(info.st_mtime,timezone.utc).isoformat()}
    if path.is_file() and info.st_size<=32*1024*1024:
        value['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
        if expected:
            value['matches_prior_audit']=value['sha256']==expected
    else:
        value['sha256']=None
        value['hash_scope']='Not rehashed; bounded reuse audit, not a new full DB/matrix inventory.'
        if expected:
            value['prior_audit_sha256']=expected
    return value


sources={}
for name in ('oanda_ma_feature_grid.py','oanda_ma_feature_grid_fit.py','oanda_multihorizon_panel_model.py',
             'oanda_signal_combination_audit.py','oanda_joint_price_news_models_v1.py',
             'oanda_causal_forecast_inputs_joint_news_v2.py','oanda_continuous_narrative_meter.py',
             'oanda_execution_policy.py','oanda_practice_shadow_strategy_lab.py',
             'oanda_practice_top_signal_executor.py','oanda_forecast_curve_contract_v1.py',
             'oanda_curve_management_adapter_v1.py','oanda_forecast_curve_file_store_v1.py',
             'oanda_research_quote_receipt_v1.py'):
    sources[name]=binding(PROJECT/name)
for key in ('source/D_unified_forecast.py','source/D_htf_corrected_rebuild.py','source/D_feature_forecast_benchmark.py'):
    row=model['source_and_report_bindings'][key]
    sources[key]=binding(row['original_path'],row['sha256'])

artifacts={
    'unified_795':binding(model['unified_795']['artifact']['path'],model['unified_795']['artifact']['sha256']),
    'ma_grid':binding(ma['artifact']['path'],ma['artifact']['sha256']),
    'second_ridge':binding(model['second_ridge']['artifact']['path'],model['second_ridge']['artifact']['sha256']),
}
for key,row in model['feature_forecast_v4']['target_bundles'].items():
    artifacts[key]=binding(row['canonical_C']['path'],row['canonical_C']['sha256'])
for key in ('htf/exp_20260702_022306_e09b53ec8d.json','htf/HTF_CORRECTED_REBUILD_REPORT.json',
            'htf/HTF_CORRECTED_FINAL_SUMMARY.json','features/run_manifest.json',
            'features/offline_research_forecast_bundle_index.json','unified/UNIFIED_FORECAST_RUN_COMPLETE.json',
            'unified/UNIFIED_FORECAST_VALIDATION.json','unified/unified_feature_registry.csv'):
    row=model['source_and_report_bindings'][key]
    artifacts[key]=binding(row['original_path'],row['sha256'])

documents=[VAULT/'README.md',VAULT/'SYSTEM_GUIDE.md',VAULT/'FEATURE_DICTIONARY_CURRENT.md',
    VAULT/'EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md',VAULT/'BLURB_DATASET_AUDIT_CURRENT.md',
    VAULT/'REVAMP_BASELINE_RECOVERY_CURRENT.md',VAULT/'NEWS_RESEARCH_DEPLOYMENT_CURRENT.md',
    PROJECT/'FOREX_PENDING_IMPROVEMENTS.md',PROJECT/'docs/PENDING_IMPROVEMENTS.md',
    PROJECT/'docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md',
    PROJECT/'docs/FOREX_REVAMP_BASELINE_RECOVERY_20260908.md',PROJECT/'docs/FOREX_BLURB_DATASET_AUDIT_20260908.md',
    EVIDENCE/'EXISTING_CURVE_MODEL_EVIDENCE_20260908.json',EVIDENCE/'MA_GRID_EXISTING_CURVE_AUDIT_20260908.json',
    EVIDENCE/'FEATURE_IMPLEMENTATION_LINEAGE_AUDIT_20260908.json',blurb_path,
    PROJECT/'docs/validation/blurb_dataset_20260908/BLURB_MODEL_USE_REVIEW_20260908.md']
doc_bindings=[binding(path) for path in documents]
recent=sorted([path for path in VAULT.iterdir() if path.is_file()],key=lambda path:path.stat().st_mtime_ns,reverse=True)[:15]
vault_recent=[{'path':str(path),'mtime_utc':datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat(),
    'bytes':path.stat().st_size} for path in recent]

tails=[]
for pair in ('EUR_USD','GBP_USD','USD_JPY'):
    path=PROJECT/f'data/oanda_training_manager/candles/{pair}_M1.csv'
    start=time.time()
    stat_before=path.stat()
    with path.open('rb') as handle:
        header=handle.readline()
        offset=max(len(header),stat_before.st_size-2*1024*1024)
        handle.seek(offset)
        if offset>len(header):
            handle.readline()
        raw=handle.read(2*1024*1024+1)
    stat_after=path.stat()
    complete=time.time()
    stable=(stat_before.st_size,stat_before.st_mtime_ns)==(stat_after.st_size,stat_after.st_mtime_ns)
    rows=list(csv.DictReader(io.StringIO((header+raw).decode())))
    observed={}
    for row in rows:
        label=datetime.fromisoformat(row['time'].replace('Z','+00:00')).timestamp()
        if label+60<=complete and row['instrument']==pair and float(row['close'])>0:
            observed[label]=float(row['close'])
    labels=sorted(observed)
    selected=labels[-204:]
    gaps=[b-a for a,b in zip(selected,selected[1:]) if b-a!=60]
    origin=labels[-1]
    tails.append({'instrument':pair,'source_path':str(path),'read_started_epoch':start,'read_completed_epoch':complete,
        'source_stat_stable':stable,'retained_tail_bytes':len(raw),'retained_tail_sha256':hashlib.sha256(raw).hexdigest(),
        'complete_real_rows_in_bounded_tail':len(labels),'first_label_epoch':labels[0],'latest_label_epoch':origin,
        'latest_price_epoch':origin+60,'latest_price_age_sec':complete-(origin+60),
        'has_204_real_positive_m1_closes':len(selected)==204,'last204_non60second_gap_count':len(gaps),
        'last204_max_gap_sec':max(gaps,default=60),'exact_own_return_endpoint_presence':
            {str(sec):origin-sec in observed for sec in (60,900,3600)},
        'scope':'Current bounded rows, not prior causal availability or full multi-timeframe feature parity. Gaps are not filled.'})

notes={}
for key in ('htf/exp_20260702_022306_e09b53ec8d.json','features/run_manifest.json','unified/UNIFIED_FORECAST_RUN_COMPLETE.json'):
    path=Path(model['source_and_report_bindings'][key]['original_path'])
    value=json.loads(path.read_text())
    notes[key]={k:v for k,v in value.items() if any(word in k.lower() for word in ('generated','created','completed','started','fit_utc','timestamp')) and not isinstance(v,(dict,list))}

value={'schema_version':'next_feature_integration_evidence_v1_20260909','observed_utc':datetime.now(timezone.utc).isoformat(),
    'status':'bounded_read_only_choices','canonical_backlog':str(PROJECT/'FOREX_PENDING_IMPROVEMENTS.md'),
    'document_instructions_scope':'Read as dated project context; no older stopped/activation instructions executed.',
    'documents':doc_bindings,'latest_top_level_vault_edits':vault_recent,'sources':sources,'artifacts':artifacts,
    'retained_fit_clock_metadata':notes,'ma_report_generated_at':ma['reported_generated_at'],
    'second_ridge_fitted_utc':model['second_ridge']['fitted_utc'],'unified_retained_split':model['unified_795']['split'],
    'current_m1_tail_observations':tails,'blurb_legacy_inventory':blurb['legacy_tags'],
    'blurb_database_inventory_scope':{'source_report':str(blurb_path),'reopened':False,
        'note':'Previously recorded SQLite sizes/counts only; no current DB-byte hash claimed.'},
    'current_C_intrahour_source_exists':(PROJECT/'fresh_m1_intrahour/src').exists(),
    'readiness_scope':'Current data presence is not a same-feature forward inference check. No serialized artifact loaded, fit, evaluation, activation, source/vault/runtime/DB/broker write occurred.',
    'next_actions':[
        {'priority':1,'name':'MA M1 feature and artifact parity companion','executable_scope':'Use retained source and hash-pinned MA artifact in isolated offline context; first compare feature names and finite vectors on204 actual completed M1 closes for the pilot pairs. Only after dependency/schema parity, emit inert native-horizon receipts. Retain gaps/original clocks and compare matched mature outcomes, not the old passing subsets.'},
        {'priority':2,'name':'Real cross-window currency/co-movement capture','executable_scope':'A separate feature-only capture computes actual1/15/60-minute endpoint returns on a declared same-time currency universe. Missing endpoints are missing, never replaced by1-minute returns. Record pair membership, actual source-observation time and pooled missingness. Add synthetic divergent-window/peer-future fixtures before any new joint fit.'},
        {'priority':3,'name':'Causal event/blurb eligibility and joint ablation inputs','executable_scope':'Join only entry-available guarded topic/factor versions to the same technical snapshot. Prior response memory requires a prior visible source and already mature response at fit cutoff. Retrospective explanations remain discovery data. Emit a coverage/readiness report and matched technical/news/combined design, not invented nonzero analog scores.'}
    ]}
dest=OUT/'NEXT_FEATURE_INTEGRATION_CHOICES_20260909.json'
with dest.open('x',encoding='utf-8') as handle:
    json.dump(value,handle,indent=2,allow_nan=False)
    handle.write('\n')
print(json.dumps({'path':str(dest),'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),
    'tail_readiness':tails,'fit_clock_metadata':notes,'latest_vault_edit':vault_recent[0] if vault_recent else None}))
