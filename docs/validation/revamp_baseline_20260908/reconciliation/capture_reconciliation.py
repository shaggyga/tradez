"""Static read-only reconciliation; no imports of Forex code or model execution."""
from pathlib import Path
from datetime import datetime,timezone
import ast,hashlib,json,math

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
ARCH=Path(r'D:\forex\trad')
LOCAL=Path(r'C:\Users\zmoor\AppData\Local\ForexResearchData\unified_intrahour_v1')
OUT=Path(__file__).resolve().parent
PRIOR=ROOT.parent/'feature_horizon_audit_20260908'
def digest(raw):return hashlib.sha256(raw).hexdigest()
def bind(p):
    if not p.exists():return {'path':str(p),'exists':False}
    raw=p.read_bytes();return {'path':str(p),'exists':True,'bytes':len(raw),'sha256':digest(raw)}
def readj(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def literal(path,name):
    for node in ast.parse(path.read_text(encoding='utf-8-sig')).body:
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in node.targets):
            return ast.literal_eval(node.value)
    raise KeyError(name)

before=datetime.now(timezone.utc).isoformat()
dictionary_receipt=readj(ROOT/'FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json')
dictionary_checks=[]
for name,expected in dictionary_receipt['file_sha256'].items():
    current=bind(ROOT/name);dictionary_checks.append({**current,'expected_sha256':expected,'matches':current.get('sha256')==expected})
comparisons=[]
for name in ['oanda_second_forecast.py','oanda_second_forecast_fit.py','oanda_second_forecast_runner.py','oanda_ma_feature_grid.py','oanda_ma_feature_grid_fit.py']:
    c,d=bind(ROOT/name),bind(ARCH/name)
    comparisons.append({'name':name,'canonical':c,'archived':d,'equal':c.get('sha256')==d.get('sha256')})
second_path=ARCH/'data/oanda_training_manager/state/second_ridge_models_v1.json'
second=readj(second_path)
second_report_path=ARCH/'data/oanda_training_manager/reports/second_ridge_fit_v1.json'
second_report=readj(second_report_path)
expected_names=literal(ROOT/'oanda_second_forecast.py','FEATURE_NAMES')
bad=[];surfaces=0;pooled=[]
for pair,models in second['models'].items():
    for horizon,model in models.items():
        surfaces+=1
        for field in ('coefficients','feature_means','feature_scales'):
            v=model.get(field)
            if not isinstance(v,list) or len(v)!=14 or any(type(x) not in (int,float) or not math.isfinite(x) for x in v):bad.append([pair,horizon,field])
        if any(x<=0 for x in model['feature_scales']):bad.append([pair,horizon,'nonpositive_scale'])
        if type(model.get('intercept')) not in (int,float) or not math.isfinite(model['intercept']):bad.append([pair,horizon,'intercept'])
        if model.get('fit_provenance')!='pair_specific':pooled.append([pair,horizon,model.get('fit_provenance')])
ma_path=ARCH/'data/oanda_training_manager/models/ma_feature_grid/ma_feature_grid_latest.joblib'
ma_report_path=ARCH/'data/oanda_training_manager/reports/ma_feature_grid/ma_feature_grid_latest.json'
ma_report=readj(ma_report_path);ma_binding=bind(ma_path)
unified_path=LOCAL/'models/unified_intrahour_forecast_latest.joblib'
unified_report_path=ARCH/'fresh_m1_intrahour/reports/unified_forecast_full_validation_native_pip_20260725/UNIFIED_FORECAST_VALIDATION.json'
unified_report=readj(unified_report_path);unified_binding=bind(unified_path)
unified_source=ARCH/'fresh_m1_intrahour/src/unified_forecast.py'
matrix_manifest=readj(LOCAL/'matrix_manifest.json')
prior_evidence=readj(PRIOR/'EXISTING_CURVE_MODEL_EVIDENCE_20260908.json')
prior_files=[PRIOR/'FEATURE_IMPLEMENTATION_LINEAGE_AUDIT_20260908.json',PRIOR/'EXISTING_CURVE_MODEL_EVIDENCE_20260908.json',PRIOR/'MA_GRID_EXISTING_CURVE_AUDIT_20260908.json',
 ROOT.parent/'blurb_dataset_audit_20260908/BLURB_DATASET_INVENTORY_20260908.json',
 ROOT/'docs/FOREX_EXISTING_FEATURE_HORIZON_AUDIT_20260908.md',ROOT/'docs/FOREX_BLURB_DATASET_AUDIT_20260908.md',
 ROOT/'FOREX_PENDING_IMPROVEMENTS.md',ROOT/'docs/PENDING_IMPROVEMENTS.md',ROOT/'docs/FOREX_FEATURE_DICTIONARY_CURRENT.md',ROOT/'docs/FOREX_FEATURE_DICTIONARY_CURRENT.json',
 ROOT/'FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json',ROOT/'docs/FOREX_JOINT_PRICE_NEWS_20260907.md']
source_checks=[]
for name in ['oanda_joint_price_news_models_v1.py','oanda_causal_forecast_inputs_joint_news_v1.py','oanda_pair_local_models_v2.py']:
    prior=readj(PRIOR/'FEATURE_IMPLEMENTATION_LINEAGE_AUDIT_20260908.json')
    expected=next(x['sha256'] for x in prior['source_bindings'] if Path(x['path']).name==name)
    actual=bind(ROOT/name);source_checks.append({**actual,'expected_sha256':expected,'matches':actual.get('sha256')==expected})

rows=[
 ['Explanatory feature dictionary / scope correction','completed_documentation','251 design entries,34 current inputs,six historical schemas/2169 versioned fields; dictionary receipt bytes rechecked.','Preserve dictionary/source checks during recovery; counts are not independent predictors.'],
 ['Current joint news/price inputs, member clocks, partial-price models and fair scheduler','implemented_predictive_acceptance_unproven','Current34-input code and inspected source hashes retained; previous tests/runtime are dated evidence.','Use fresh root runtime/performance baseline; do not claim present operation from static audit.'],
 ['Existing horizon source/artifact recovery','disconnected_but_recoverable_in_parts','Second-ridge and MA currentC/Dsources match; expected canonical artifacts absent. Unifiedv4artifact exists but Dsourcev5differs.','First isolated second-ridge deterministic inference comparator; then trusted MA dependency/schema replay; unified exact-source reconstruction.'],
 ['Duplicated 15m cross features and native-pip cross-pair ranking','open_confirmed_defects','Prior actual795matrix had five15m/1m identical columns; native-pip ranking is not comparable economic magnitude.','Preserve oldmodel/data; repair in newversion and compare matched observations; never relabel oldperformance.'],
 ['Blurb/factor data and earlier news/technical comparisons','implemented_data_and_research_disconnected_from_oldwidefits','7048legacyrows,2935factors,6465distinctmarketepisodes exist; base analog tables had0scored forecasts.','Reuse entry-time-valid subset and prior comparisons; separate late/ex-post and known clocks; do not recollect knownassets.'],
 ['Broader learnable news / co-movement / analog pooling','partial_existing_machinery_unproven_extension','Historical cross-currency builders exist; current34features omit peers and rawtext, use scored-member context; originalwide macrofieldsconstant.','Test information increment and sparse factor/event-family pooling in newregistrations; no claim morecolumns aloneimprove.'],
 ['Magnitude, calibration, spread cost and independent outcomes','unproven','Older curves often fail direction/aftercost baselines; earlier currentH1samples tiny/overlapping.','Matched denominators, originaltargets, comparablebps/dollars, time/eventblocked uncertainty and laterprospective samples.'],
 ['News freshness / publication mismatch / provider failures','partial_open_operational_acceptance','Current backlog retains300snews freshness, intermittentjointsummary mismatch, SCB/GDELT/HKMA followups.','Reproduce from rootfreshbaseline; preserve originalclocks/hashchecks; no new failure inferred from oldreport.'],
 ['Training-history breadth, input bounds and storage growth','partial_open','48hour/5000news-row caps and compression implemented; multiday independenttraining and longrunstorageacceptance pending.','Track boundedcohorts/storage; retain immutablecaptures and do notdelete oldevidence tofitbudget.'],
 ['Prior joint scheduler retirement','pending_evidence_review','Current queue says preserve originaloutcomes and comparison before retirement.','Root reviews currentbaseline first; no stop/restart in this phase.'],
]
record={'schema':'revamp_backlog_reconciliation_v1','observed_started_utc':before,'observed_finished_utc':datetime.now(timezone.utc).isoformat(),
 'status':'completed_static_readonly','source_bindings':[bind(p) for p in prior_files],
 'dictionary_validation_recheck':dictionary_checks,'current_registered_source_recheck':source_checks,'canonical_archived_source_comparisons':comparisons,
 'dictionary_coverage':readj(ROOT/'docs/FOREX_FEATURE_DICTIONARY_CURRENT.json')['coverage'],
 'candidate_compatibility':{
 'second_ridge':{'priority':1,'artifact':bind(second_path),'report':bind(second_report_path),'canonical_artifact':bind(ROOT/'data/oanda_training_manager/state/second_ridge_models_v1.json'),
 'source_feature_names':list(expected_names),'artifact_feature_names':second['feature_names'],'exact_feature_name_order_match':tuple(second['feature_names'])==expected_names,
 'schema_version':second['schema_version'],'model_family':second['model_family'],'input_timeframe':second['input_timeframe'],'training_timeframe':second['training_timeframe'],
 'horizons_sec':second['horizons_sec'],'parameter_surfaces_inspected':surfaces,'parameter_shape_errors':bad,'pooled_surfaces':pooled,
 'report_artifact_same_fit_time':second['fitted_utc']==second_report['fitted_utc'],'report_artifact_same_coverage':all(second[k]==second_report[k] for k in ('model_count','pair_specific_model_count','proxy_model_count','horizons_sec','feature_names')),
 'retained_input_example':{'path':str(ARCH/'data/oanda_training_manager/candles_s5_bam/EUR_USD_S5.parquet'),'exists':(ARCH/'data/oanda_training_manager/candles_s5_bam/EUR_USD_S5.parquet').exists()},
 'verdict':'Best first deterministic offline inference comparator: transparent finite parameters, ordered schema and matchingC/Dsource. No inference/fittingperformed; report/JSON do not bind exact fit-time sourcehash/dependencylock or prove fulltrainingreproduction.'},
 'ma_grid':{'priority':2,'artifact':ma_binding,'report':bind(ma_report_path),'artifact_report_hash_match':ma_binding['sha256']==ma_report['artifact_sha256'],
 'canonical_artifact':bind(ROOT/'data/oanda_training_manager/models/ma_feature_grid/ma_feature_grid_latest.joblib'),'planned_cells':ma_report['planned_cell_count'],'fitted_cells':ma_report['fitted_cell_count'],'unsupported_cells':ma_report['unsupported_cell_count'],
 'verdict':'Strong first broadfeature follow-on: matchingC/Dsource and boundartifact/report. Opaquemodel/dependency/embeddedfeature-schema compatibility not executed or establishedbythisread-onlycheck.'},
 'unified_795':{'priority':3,'artifact':unified_binding,'report':bind(unified_report_path),'archived_source':bind(unified_source),
 'matrix_manifest':bind(LOCAL/'matrix_manifest.json'),'saved_schema':matrix_manifest['feature_schema_version'],
 'current_archived_schema':literal(unified_source,'FEATURE_SCHEMA_VERSION'),'canonical_package_exists':(ROOT/'fresh_m1_intrahour/src').exists(),
 'verdict':'Exact v4source/artifact compatibility unresolved. CurrentDsourcev5and missingCpackage preclude declaringplug-inrecovery; preserve v4matrix/weights and reconstructschema before separatebugfixversion.'}},
 'backlog_matrix':[dict(zip(('item','status','evidence','remaining_acceptance'),r)) for r in rows],
 'first_recovery_milestone':{
 'name':'Isolated second-ridge inference reproducibility comparator','status':'proposed_not_executed',
 'acceptance':['Hashbind preservedJSON/source/report and selectedinputfixture; no live source/config/ledger paths.',
 'Validate14orderedfinitefeatures,884parameter shapes, schema andpair/horizonidentity; pooledTRY_JPY remains explicitly proxy and researchonly.',
 'On fixed own-pairfixture, verify direct normalizeddot+intercept equals originalcompiled prediction arithmetic at selected5m/15m/60m horizons within numericaltolerance; norefit.',
 'Replay originalS5feature windows andmissingness; do notpretend S5training/S1live inputsidentical or compressgaps. Preserve documentedendpoint+7stolerance; itisnotexactH1ledgersemantics.',
 'Emit only newoffline comparison evidence withoriginalmodeldate, inputclock,raw/calibratedexpectedpips,uncalibratedprobability,originalhorizonandallmissingreasons; allorder/promotionflagsdisabled.',
 'Keep magnitude/direction/costgates andpriornegative baselinesvisible; deterministicreproductionis not improvedaccuracy orliveactivation.'],
 'follow_on':'Trusted isolated MA M1-input horizon subset for broadfeature comparator after dependency/schema proof; unifiedv4versionrecovery separately.'},
 'limits':['Static files and prioraudit evidence only; no newruntime healthclaim.', 'No data matrix scan, unknownobjectloading, sourceimports, modelinference, fitting, scoring, databasewrite, runtimeorbrokeraction.', 'C/Dsourceequality andartifact/report agreement are evidence ofrecoverability, not cryptographicattestationoforiginalfit-time source or dependencies.']}
target=OUT/'BACKLOG_RECONCILIATION_20260908.json'
with target.open('x',encoding='utf-8',newline='\n') as f:json.dump(record,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({'path':str(target),'sha256':digest(target.read_bytes()),'bytes':target.stat().st_size,'dictionary_matches':all(x['matches'] for x in dictionary_checks),
 'registered_source_matches':all(x['matches'] for x in source_checks),'second_parameter_surfaces':surfaces,'second_shape_errors':bad,'unified_versions':[matrix_manifest['feature_schema_version'],literal(unified_source,'FEATURE_SCHEMA_VERSION')]}))
