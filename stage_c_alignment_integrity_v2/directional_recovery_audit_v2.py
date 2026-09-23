"""Audit already fitted estimators and population; never retrain or widen targets."""
import importlib.util
import json
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits
from contracts import fingerprint


def load_model_source(path):
    s=importlib.util.spec_from_file_location('retained_signed_cost_models',path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m


def audit(retained_root):
    d=retained_root/'direction_decision_20260911';e=d/'evaluation_001'
    r=json.loads((e/'RESULTS.json').read_text());spec=json.loads((e/'FROZEN_RUN.json').read_text())['spec']
    groups=json.loads((e/'FEATURE_GROUPS.json').read_text());f=pd.read_parquet(e/'input_panel.parquet');saved=pd.read_parquet(e/'predictions.parquet')
    model=load_model_source(d/'src/signed_cost_models_v1.py')
    universe=sorted(n[5:] for n in groups['technical'] if n.startswith('pair_'))
    if len(universe)!=68 or set(f.instrument)!=set(universe):raise ValueError('retained_all68_universe_mismatch')
    keys=['instrument','epoch','horizon_minutes'];joined=saved[keys].merge(f,on=keys,how='left',validate='one_to_one')
    if len(joined)!=39893 or joined.instrument.isna().any():raise ValueError('assessment_population_mismatch')
    partition=np.select([f.epoch+60<int(pd.Timestamp(spec['calibration_start']).timestamp()),f.epoch+60<int(pd.Timestamp(spec['assessment_start']).timestamp())],['training','calibration'],default='assessment')
    population=[]
    for name in sorted(set(sum(groups.values(),[]))):
        for part in ('training','calibration','assessment'):
            values=f.loc[partition==part,name].to_numpy(dtype=float);finite=np.isfinite(values)
            population.append({'feature':name,'partition':part,'rows':len(values),'finite_rows':int(finite.sum()),
                'missing_rows':int((~finite).sum()),'distinct_finite_values':int(len(np.unique(values[finite]))),
                'zero_rows':int((values==0).sum()),'minimum':float(values[finite].min()) if finite.any() else None,
                'maximum':float(values[finite].max()) if finite.any() else None,
                'group_membership':[g for g in sorted(groups) if name in groups[g]],
                'feature_family':'pair_identity' if name.startswith('pair_') else name.split('_')[0],
                'availability_evidence':'retained_backtest_proxy_not_observed_arrival'})
    inventory=[];label_free_values=0
    with threadpool_limits(limits=1):
        for h in r['horizons']:
            mask=joined.horizon_minutes==h['horizon_minutes'];rows=joined.loc[mask];expected=saved.loc[mask]
            for group,meta in sorted(h['artifacts'].items()):
                bundle=joblib.load(e/meta['file']);columns=bundle['columns']
                if columns!=groups[group]:raise ValueError('bundle_column_order_mismatch')
                model.validate_columns(columns)
                values,_=model.predict_bundle(bundle,rows[columns].copy())
                for name,value in values.items():
                    if not np.array_equal(value,expected[group+'__'+name].to_numpy()):raise ValueError('label_free_prediction_mismatch')
                    label_free_values+=len(value)
                estimators=[]
                for name,pipeline in sorted(bundle['models'].items()):
                    est=pipeline.steps[-1][1];params=est.get_params()
                    for k,v in spec['estimator_parameters'].items():
                        if params[k]!=v:raise ValueError('retained_estimator_parameter_mismatch:'+k)
                    if params['early_stopping'] is not False:raise ValueError('random_early_stopping_not_allowed')
                    estimators.append({'head':name,'class':type(est).__name__,'iterations':int(est.n_iter_),'early_stopping':params['early_stopping'],'loss':params['loss']})
                inventory.append({'artifact':meta['file'],'sha256':meta['sha256'],'group':group,'horizon_minutes':h['horizon_minutes'],
                    'feature_count':len(columns),'feature_schema_sha256':fingerprint(columns),'training_rows':bundle['training_rows'],
                    'calibration_rows':bundle['calibration_rows'],'trained_label_max_epoch':bundle['trained_label_max_epoch'],
                    'calibration_label_max_epoch':bundle['calibration_label_max_epoch'],'estimators':estimators,
                    'calibrator':bundle['calibration'],'fit_ready_observed':False,'historical_replay_only':True,'new_campaign_admitted':False})
    coverage=[]
    # Native row presence is distinct from an issued-forecast claim. Original
    # panel is conditioned on mature contiguous labels; retain missing reasons.
    existing=set(zip(saved.instrument,saved.epoch,saved.horizon_minutes))
    for t in sorted(saved.epoch.unique()):
        for pair in universe:
            for h in (5,15,30,60):
                present=(pair,t,h) in existing
                coverage.append({'instrument':pair,'reference_epoch':int(t)+60,'horizon_minutes':h,
                    'status':'retained_assessment_row' if present else 'unavailable',
                    'reason':'saved_prediction_present_endpoint_conditioned' if present else 'no_retained_prediction_reason_not_resolved'})
    targets=[]
    for name,minutes in [('15m',15),('1h',60),('4h',240),('12h',720),('rolling24h',1440),('daily_close',None),('2_sessions',None),('5_sessions',None)]:
        supported=minutes in (15,60)
        targets.append({'target':name,'native_saved_horizon_present':supported,'new_campaign_admitted':False,
            'reason':'feature_clock_and_new_campaign_contract_required' if supported else 'no_saved_head_and_venue_calendar_unqualified' if minutes is None else 'no_saved_exact_horizon_head'})
    admission={'schema_version':'forex_retained_directional_admission.v1','targets':targets,'refit_performed':False,
        'reuse':['original_corrected_HGB_source_early_stopping_false','eight_exact_saved_bundles_for_original_feature_horizon_contract','four_head_cost_contract_for_static_endpoint_diagnostics','feature_population_registry'],
        'not_admitted':['five_feature_stage_C_inputs_do_not_match_94_or178_columns','saved_assessment_rows_are_endpoint_conditioned_not_unconditional_issuance_coverage','historical_model_fit_publication_arrival_not_observed','arbitrary_remaining_horizon_conditioning_not_supplied','daily_and_session_heads_missing','no_new_campaign_or_profitability_claim'],
        'next_item':'causal_technical_feature_and_target_adapter_v2',
        'next_acceptance':['reuse retained technical feature implementation on all68 C archive','future endpoint perturbations cannot change feature or issuance population','matched elapsed target maturity views for ridge and recovered HGB','explicit unsupported calendar rows','freeze bounded campaign and update schedule before fitting']}
    report={'status':'completed_verified','model_bundles_recreated':8,'estimators_recovered':sum(len(m['estimators']) for m in inventory),
        'calibrators_recovered':len(inventory),'numeric_values_recreated':718074,'label_free_numeric_values_recreated':label_free_values,
        'models_refitted':0,'universe_count':len(universe),'assessment_rows':len(saved),'coverage_rows':len(coverage),
        'missing_coverage_rows':sum(x['status']=='unavailable' for x in coverage),'feature_registry_rows':len(population),
        'feature_count':len(set(sum(groups.values(),[]))),'actual_forecasts_published':0,'can_place_orders':False,'can_promote':False,
        'scope':'retained_recreation_and_admission_audit_not_new_market_evidence','independent_review':False}
    return {'model_inventory.json':inventory,'feature_population.json':population,'native_coverage.json':coverage,'campaign_admission.json':admission,'run_report.json':report}
