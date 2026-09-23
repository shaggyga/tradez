"""Bounded population of the preserved rolling schema, with no model fitting."""
from collections import Counter
import hashlib,io
from datetime import datetime,timezone
import re
import numpy as np
from contracts import fingerprint
import oanda_rolling_technical_features_v1 as kernel
import oanda_rolling_technical_panel_v1 as peers

def definitions():
    original=kernel.feature_registry()+peers.panel_registry()
    if len(original)!=228 or len({r['name'] for r in original})!=228:raise ValueError('canonical_228_registry_required')
    rows=[]
    for r in original:
        peer=r['family']=='currency_peer'
        rows.append({**r,'concept_id':r['family']+':'+re.sub(r'\d+','{window}',r['name']),
            'variant_id':r['name'],'information_layer':'observed_market_state',
            'upstream_inputs':[r['source_feature'],'exact_other_pair_bar_starts'] if peer else list(kernel.INPUT_COLUMNS),
            'upstream_scope':'exact_peer_return_contract' if peer else 'conservative_kernel_input_superset_not_minimal_formula_dependencies',
            'normalization':'deterministic_formula_only; learned_normalizers_require_separate_training_prefix_artifact',
            'computation_cost':'shared_pure_kernel_or_panel; measured_total_not_independent_per_column',
            'weight_compatibility':'not_inferred_from_names; model_target_fit_clock_and_transform_identity_required'})
    return rows

def pair_features(pair,frame,pip,source_sha,origins):
    if list(frame.columns)!=list(kernel.INPUT_COLUMNS):raise ValueError('exact_rolling_raw_columns_required')
    t=frame.time.to_numpy()
    if t.dtype.kind not in 'iu' or (t<0).any() or (t%60!=0).any():raise ValueError('integer_original_minute_starts_required')
    duplicates=frame.time.duplicated(keep=False);frame=frame.loc[~duplicates].sort_values('time').reset_index(drop=True)
    data={n:frame[n].to_numpy() for n in kernel.INPUT_COLUMNS};columns=kernel.compute_features(data,pair,pip)
    positions={int(t):i for i,t in enumerate(frame.time)};rows=[]
    for origin in origins:
        epoch=origin-60;i=positions.get(epoch)
        values={n:float(v[i]) if i is not None and np.isfinite(v[i]) else None for n,v in columns.items()}
        rows.append({'record_id':f'{pair}:{origin}','instrument':pair,'origin_epoch':origin,'available_epoch':origin,
            'source_bar_start_epoch':epoch,'exact_reference_present':i is not None,'source_member_sha256':source_sha,
            'clock_status':'assumed_completed_bar_end_not_observed_arrival','values':values})
    return {'instrument':pair,'observations':rows,'quality':{'duplicate_rows_excluded':int(duplicates.sum()),'raw_rows':len(t),'retained_rows':len(frame)}}

def join_peers(parts,origins):
    by={p['instrument']:p for p in parts};output={k:[] for k in by}
    for j,origin in enumerate(origins):
        rows={pair:{'bar_start_epoch':p['observations'][j]['source_bar_start_epoch'] if p['observations'][j]['exact_reference_present'] else None,
                    'values':p['observations'][j]['values']} for pair,p in by.items()}
        panel=peers.compute_panel(rows,origin-60)
        for pair,p in by.items():
            original=p['observations'][j];q=panel['by_pair'][pair]
            record={**original,'values':{**original['values'],**q['values']},'peer_provenance':q,
                'feature_schema_sha256':fingerprint([r['name'] for r in definitions()])}
            record['record_sha256']=fingerprint(record);output[pair].append(record)
    return [{'instrument':pair,'observations':output[pair],'quality':by[pair]['quality']} for pair in sorted(by)]

def counts(values):
    finite=[v for v in values if v is not None]
    unique=len(set(finite))
    return {'rows':len(values),'finite':len(finite),'missing':len(values)-len(finite),'finite_rate':len(finite)/len(values) if values else None,
        'distinct_finite_values':unique,'nonconstant_on_observed_support':unique>1,'constant_test_support':len(finite),
        'constant_status':'insufficient_finite_support' if len(finite)<2 else 'nonconstant' if unique>1 else 'constant'}

def populated_registry(parts,lineage):
    observations=[r for p in parts for r in p['observations']];registry=[]
    historical_groups=lineage['rolling_groups']
    for definition in definitions():
        name=definition['name'];by_pair={p['instrument']:counts([r['values'][name] for r in p['observations']]) for p in parts}
        dates=sorted({datetime.fromtimestamp(r['origin_epoch'],timezone.utc).strftime('%Y-%m-%d') for r in observations})
        by_date={d:counts([r['values'][name] for r in observations if datetime.fromtimestamp(r['origin_epoch'],timezone.utc).strftime('%Y-%m-%d')==d]) for d in dates}
        by_band={str(h):counts([r['values'][name] for r in observations if datetime.fromtimestamp(r['origin_epoch'],timezone.utc).hour//6*6==h]) for h in (0,6,12,18)}
        registry.append({**definition,'population':counts([r['values'][name] for r in observations]),'by_instrument':by_pair,'by_utc_date':by_date,
            'by_utc_hour_band':by_band,'regime_scope':'fixed_UTC_six_hour_bands_not_qualified_venue_sessions_or_learned_regimes',
            'prior_selected_groups':[g for g,names in historical_groups.items() if name in names],
            'prior_experiments':['rolling_period_replication_20260915_a_v2','rolling_period_replication_20260915_b_v2'] if name in historical_groups['compact50'] else [],
            'selection_interpretation':'actual_original_selected_schema; no independent_signal_or_current_weight_admission_claim'})
    return {'schema_version':'forex_populated_rolling_registry.v1','feature_count':len(registry),'columns':registry,'historical_groups':historical_groups,
        'aliases_as_extra_columns':0,'families':dict(Counter(r['family'] for r in registry)),'origin_rows':len(observations),'fit_performed':False}

def consume(record,*,asof,feature_names):
    if type(asof) is not int or asof<record['available_epoch']:raise ValueError('feature_not_available_asof')
    if record['record_sha256']!=fingerprint({k:v for k,v in record.items() if k!='record_sha256'}):raise ValueError('feature_record_identity_mismatch')
    expected=[r['name'] for r in definitions()]
    if record['feature_schema_sha256']!=fingerprint(expected) or set(record['values'])!=set(expected):raise ValueError('canonical_feature_schema_required')
    if not feature_names or len(set(feature_names))!=len(feature_names) or any(n not in expected for n in feature_names):raise ValueError('unique_canonical_requested_fields_required')
    return {'record_id':record['record_id'],'original_record_sha256':record['record_sha256'],'feature_names':list(feature_names),
        'values':[record['values'][n] for n in feature_names],'missing_mask':[record['values'][n] is None for n in feature_names],
        'normalization':'none; original deterministic values','observed_arrival':False,'fit_performed':False,'outcomes_included':False}

def verify_normalizer(raw,metadata,feature_names,pairs,latest_origin):
    if hashlib.sha256(raw).hexdigest()!=metadata['artifact']['sha256']:raise ValueError('normalizer_artifact_identity_mismatch')
    with np.load(io.BytesIO(raw),allow_pickle=False) as arrays:
        if set(arrays.files)!=set(metadata['arrays']):raise ValueError('normalizer_array_set_mismatch')
        for name,expected in metadata['arrays'].items():
            a=arrays[name]
            if list(a.shape)!=expected['shape'] or str(a.dtype)!=expected['dtype'] or hashlib.sha256(a.tobytes(order='C')).hexdigest()!=expected['c_order_payload_sha256']:raise ValueError('normalizer_array_contract_mismatch')
        if arrays['feature_names'].tolist()!=feature_names or arrays['pair_names'].tolist()!=pairs:raise ValueError('normalizer_order_mismatch')
        cuts=arrays['fold_cutoffs_epoch'].tolist()
        if cuts!=metadata['fold_cutoffs_epoch'] or len(cuts)!=5 or cuts!=sorted(set(cuts)):raise ValueError('normalizer_fit_clock_mismatch')
    return {'arrays_verified':len(metadata['arrays']),'shape':[5,68,50],'original_scope':metadata['normalizer_scope'],
        'original_inference_sequence':metadata['inference_sequence'],'fit_cutoffs_epoch':cuts,
        'admitted_for_current_population':False,'reason':'future_fit_cutoffs' if min(cuts)>latest_origin else 'separate_model_target_and_transform_admission_required',
        'normalizers_refit':False}
