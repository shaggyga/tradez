"""Read-only feature lineage audit. No project imports, fits, pickle, or DB writes."""
from pathlib import Path
from collections import Counter
import ast,csv,hashlib,json,struct,time
from datetime import datetime,timezone
import numpy as np
import pyarrow.parquet as pq

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
ARCH=Path(r'D:\forex\trad')
LOCAL=Path(r'C:\Users\zmoor\AppData\Local\ForexResearchData\unified_intrahour_v1')
OUT=Path(__file__).resolve().parent
def sha(raw):return hashlib.sha256(raw).hexdigest()
def bind(path):
    raw=path.read_bytes()
    return {'path':str(path),'bytes':len(raw),'sha256':sha(raw)}
def readj(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def footer_binding(path):
    before=path.stat()
    with path.open('rb') as f:
        f.seek(-8,2);tail=f.read(8)
        assert tail[4:]==b'PAR1'
        size=struct.unpack('<I',tail[:4])[0]
        assert 0<size<=32*1024*1024
        f.seek(-8-size,2);raw=f.read(size+8)
    after=path.stat()
    assert (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns)
    return {'path':str(path),'bytes':after.st_size,'mtime_ns':after.st_mtime_ns,
            'parquet_footer_and_trailer_bytes':len(raw),'parquet_footer_and_trailer_sha256':sha(raw),
            'hash_scope':'Parquet metadata footer and trailer, not entire dataset'}
def metadata_stats(path,selected):
    f=pq.ParquetFile(path); names=f.schema.names; data={}
    for name in selected:
        if name not in names:data[name]={'absent':True};continue
        stats=[f.metadata.row_group(i).column(names.index(name)).statistics for i in range(f.metadata.num_row_groups)]
        populated=[s for s in stats if s is not None and s.has_min_max]
        data[name]={'all_rowgroup_stats_available':all(s is not None for s in stats),
                    'min':min((s.min for s in populated),default=None),'max':max((s.max for s in populated),default=None),
                    'null_count':sum(s.null_count for s in stats if s is not None),
                    'rows':f.metadata.num_rows}
    valid=[(n,s) for n,s in data.items() if not s.get('absent')]
    return {'source':footer_binding(path),'rows':f.metadata.num_rows,'row_groups':f.metadata.num_row_groups,
            'total_columns':f.metadata.num_columns,'selected_columns':len(selected),
            'absent':[n for n,s in data.items() if s.get('absent')],
            'entirely_null':[n for n,s in valid if s['null_count']==s['rows']],
            'constant_nonnull':[n for n,s in valid if s['min'] is not None and s['min']==s['max']],
            'all_stats_available':all(s.get('all_rowgroup_stats_available',False) for _,s in valid),
            'stats':data}
def named_literal(path,name):
    tree=ast.parse(path.read_text(encoding='utf-8-sig'))
    for node in tree.body:
        targets=node.targets if isinstance(node,ast.Assign) else [node.target] if isinstance(node,ast.AnnAssign) else []
        if any(isinstance(t,ast.Name) and t.id==name for t in targets):return ast.literal_eval(node.value)
    raise KeyError(name)

started=time.time()
registry=LOCAL/'unified_feature_registry.csv'
registered=list(csv.DictReader(registry.open(encoding='utf-8-sig',newline='')))
features=[r['feature_name'] for r in registered]
assert len(features)==len(set(features))==795
unified=metadata_stats(LOCAL/'unified_training_matrix.parquet',features)
stems=['base_strength','quote_strength','base_minus_quote_strength','pair_return_rank','dispersion']
selected=['cross__'+stem+'_'+str(h) for stem in stems for h in (1,15)]
f=pq.ParquetFile(LOCAL/'unified_training_matrix.parquet')
table=f.read(columns=selected)
duplicates=[]
for stem in stems:
    left,right='cross__'+stem+'_1','cross__'+stem+'_15'
    a,b=table[left].to_numpy(),table[right].to_numpy()
    duplicates.append({'left':left,'right':right,'rows_compared':len(a),
                       'exact_equal_including_same_nan':bool(np.array_equal(a,b,equal_nan=True)),
                       'left_values_sha256':sha(a.tobytes()),'right_values_sha256':sha(b.tobytes())})
assert unified['source']==footer_binding(LOCAL/'unified_training_matrix.parquet')
unified['numeric_feature_groups']=dict(Counter(r['source_timeframe'] for r in registered))
unified['registry_feature_families']=dict(Counter(r['feature_family'] for r in registered))
unified['registry']=bind(registry)
unified['manifest']=readj(LOCAL/'matrix_manifest.json')
unified['nominal_15m_cross_features_duplicate_1m']=duplicates
unified['m1_return_15_pips_column_present']='m1__return_15_pips' in f.schema.names
unified['news_macro_sentiment_registry_features']=[n for n in features if any(t in n.lower() for t in ('news','sentiment','macro'))]
unified['book_depth_registry_features']=[n for n in features if any(t in n.lower() for t in ('order_book','position_book','microprice','depth'))]
unified['inference_limits']=['Metadata proves nonnull/nonconstant historical values, not causality or independent information.',
 'Only ten selected numeric columns were decoded; no full 506 MB scan or model loading.',
 'Retained matrix schema v4_native_pip differs from current D builder v5_opportunity; observed duplicate columns are proven directly, current-source fallback is the apparent mechanism rather than exact historical source attestation.']
experiment=ARCH/'data/oanda_training_manager/continuous_research/experiments/exp_20260702_022306_e09b53ec8d.json'
exp=readj(experiment);original=exp['result']['features'];assert len(original)==227
unsafe=named_literal(ARCH/'fresh_m1_intrahour/src/htf_corrected_rebuild.py','UNSAFE_GLOBAL_FEATURES')
safe=[n for n in original if n not in unsafe];assert len(safe)==220
htf={}
for filename in ('technical_spike_research.parquet','technical_spike_research_1h_step1.parquet'):
    htf[filename]=metadata_stats(ARCH/'data/oanda_training_manager/continuous_research'/filename,original)
    htf[filename]['macro_stats']={n:s for n,s in htf[filename]['stats'].items() if n.startswith('macro_')}
    del htf[filename]['stats']
snapshotpath=ROOT/'data/oanda_training_manager/state/live_model_feature_snapshot_v1.json'
snapshot=readj(snapshotpath)
panelpath=ROOT/'data/oanda_training_manager/reports/unique_predictor_watch_20260804/multihorizon_currency_panel_v3.json'
panel=readj(panelpath)
panel_audits={h:v['feature_audit'] for h,v in panel['horizons'].items()}
canarypath=ROOT/'data/oanda_training_manager/model_space/validation_fixtures/panel_m1_compact_canary_20260718.parquet'
canary_manifest=readj(canarypath.with_suffix('.parquet.manifest.json'))
canary=metadata_stats(canarypath,canary_manifest['model_contract']['numeric_features'])
del canary['stats']
report_path=ROOT/'fresh_m1_intrahour/reports/feature_forecast_full_20260713_v4/FEATURE_FORECAST_REPORT.json'
feature_forecast=readj(report_path)
spec=readj(ROOT/'config/model_feature_space.json')
modelsrc=ROOT/'oanda_joint_price_news_models_v1.py'
jointnews=named_literal(modelsrc,'NEWS_FEATURES')
sources=[ROOT/'MODEL_FEATURE_SPACE.md',ROOT/'config/model_feature_space.json',ROOT/'oanda_model_feature_space.py',
 registry,LOCAL/'matrix_manifest.json',ARCH/'fresh_m1_intrahour/src/unified_forecast.py',
 ARCH/'fresh_m1_intrahour/reports/unified_forecast_full_validation_native_pip_20260725/UNIFIED_FORECAST_VALIDATION.json',
 ROOT/'oanda_unified_intrahour_shadow_producer.py',ROOT/'oanda_intrahour_forecast_contract.py',
 ROOT/'oanda_practice_shadow_strategy_lab.py',ROOT/'oanda_signal_combination_audit.py',
 ROOT/'oanda_multihorizon_panel_model.py',panelpath,ROOT/'oanda_shared_timeframe_horizon_panel.py',
 ROOT/'oanda_shared_panel_model_benchmark.py',canarypath.with_suffix('.parquet.manifest.json'),
 ARCH/'oanda_gpt_training_strategy_manager.py',experiment,
 ARCH/'fresh_m1_intrahour/src/htf_corrected_rebuild.py',ARCH/'fresh_m1_intrahour/src/htf_validation_replay.py',
 ARCH/'fresh_m1_intrahour/src/feature_forecast_benchmark.py',report_path,
 ROOT/'fresh_m1_intrahour/reports/htf_validation_replay_corrected_execution_20260710_v2/HTF_FEATURE_TIMING_AUDIT.json',
 ROOT/'fresh_m1_intrahour/reports/htf_corrected_arima_features_tier1_2026_20260712/HTF_CORRECTED_REBUILD_REPORT.json',
 modelsrc,ROOT/'oanda_pair_local_models_v2.py',ROOT/'oanda_causal_forecast_inputs_joint_news_v1.py',snapshotpath]
data={'schema':'feature_implementation_lineage_audit_v1','observed_utc':datetime.now(timezone.utc).isoformat(),
 'status':'completed_with_findings','method':'read-only source/JSON/CSV inspection, Parquet footer statistics and ten selected matrix columns; no fits, model deserialization, network, runtime or database mutation',
 'source_bindings':[bind(p) for p in sources],
 'design_inventory':{'generated_utc':spec['generated_utc'],'counts':spec['counts'],'feature_block_count':len(spec['feature_blocks']),
   'scope':'declared inventory, not proof of fitted or currently populated inputs; 208 strategy profile lanes/209 with AHL are separate from feature counts'},
 'unified_fitted_795':unified,
 'archived_htf_227_corrected_220':{'experiment':str(experiment),'features':len(original),'corrected_safe_features':len(safe),
   'removed_unsafe_global_features':sorted(unsafe),'macro_feature_names':[n for n in original if n.startswith('macro_')],
   'historical_population_metadata':htf,'corrected_plus_arima_candidate_count':270,
   'scope':'Historical 227 claimed model had timing defects; selected corrected model220. Additional50 ARIMA inputs were candidate space, not selected winner.'},
 'feature_forecast_technical_interactions':{'report':str(report_path),'data_audit':feature_forecast.get('data_audit'),
   'explicit_interactions':named_literal(ARCH/'fresh_m1_intrahour/src/feature_forecast_benchmark.py','INTERACTION_INPUTS'),
   'scope':'Saved separate offline two-hour bundles use safe technical family plus explicit technical/cross-currency interactions and identity/calendar columns; macro constants supply no trained news variation.'},
 'multihorizon_currency_panel':{'report_generated_at':panel['generated_at'],'horizon_feature_audits':panel_audits,
   'lineage':'SignalCombination features_zlib -> train-coverage/variance selection -> spread scale, currency exposure, calendar, absolute and signed-square bases -> separate direct horizons; historical research, not current curve producer'},
 'shared_panel_canary':{'metadata':canary,'manifest_panel':canary_manifest['panel'],
   'numeric_features':len(canary_manifest['model_contract']['numeric_features']),
   'categorical_features':len(canary_manifest['model_contract']['categorical_features']),
   'scope':'Actual compact M1 validation fixture; optional book/microstructure schema is not evidence of populated training history.'},
 'current_legacy_feature_snapshot':{'source':bind(snapshotpath),'generated_utc':snapshot['generated_utc'],
   'instrument_count':snapshot['instrument_count'],'coverage':{k:v for k,v in snapshot['coverage'].items() if k!='quote_exclusions'},
   'quote_exclusion_reasons':dict(Counter(r['reason'] for r in snapshot['coverage']['quote_exclusions'])),
   'local_C_src_package_exists':(ROOT/'fresh_m1_intrahour/src').exists(),
   'scope':'The observed legacy snapshot is stale and empty; its Sept4 quote exclusions do not describe current market/feed availability.'},
 'current_joint_h1_feature_lineage':{'news_features':jointnews,'news_feature_count':len(jointnews),
   'price_feature_count':24,'learned_price_news_interactions':2,'total_numeric_features':34,
   'scope':'Separate new per-pair H1 Ridge uses own-pair time-aware prices plus captured news context/vetted channels and two learned interactions; no cross-pair strength or legacy795 model inputs.'},
 'duration_sec':round(time.time()-started,4)}
target=OUT/'FEATURE_IMPLEMENTATION_LINEAGE_AUDIT_20260908.json'
with target.open('x',encoding='utf-8',newline='\n') as f:json.dump(data,f,indent=2,sort_keys=True,allow_nan=False);f.write('\n')
print(json.dumps({'output':str(target),'sha256':sha(target.read_bytes()),'bytes':target.stat().st_size,
 'unified_groups':unified['numeric_feature_groups'],'unified_constants':unified['constant_nonnull'],
 'htf_constants':{k:v['constant_nonnull'] for k,v in htf.items()},'elapsed':data['duration_sec']}))
