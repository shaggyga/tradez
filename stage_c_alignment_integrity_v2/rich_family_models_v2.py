"""Matched wider inputs using preserved training admission and preprocessing."""
import hashlib,io,json
import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from threadpoolctl import threadpool_limits
from contracts import TrainingView,fingerprint,forecast_record
from matched_campaign_models_v2 import contract as baseline_contract,population,canonicalize_model_strings
import retained_signed_cost_models_v1 as recovered

GROUPS=('compact38_cost2','compact50_cost2','full228_cost2')
METHODS=('ridge','recovered_hgb')

def contract():
    c=baseline_contract()
    c['baseline_features']=c.pop('features')
    return {**c,'schema_version':'forex_rich_family_comparison.v1','groups':list(GROUPS),'methods':list(METHODS),
        'preprocessing':'retained_median_imputation_missing_indicators_standard_scaler_train_only',
        'baseline':'reuse_original_legacy26_models_forecasts_controls_without_refit',
        'pair_context':'none_added','forecast_layout':'one_payload_per_group_target_procedure',
        'next_item':'rich_family_dependence_and_attempt_accounting_v2'}

def frame(values,names):
    if not names or len(set(names))!=len(names):raise ValueError('unique_registered_features_required')
    x=np.asarray(values,dtype=np.float64)
    if x.ndim!=2 or x.shape[1]!=len(names) or np.isinf(x).any():raise ValueError('finite_or_missing_registered_features_required')
    return pd.DataFrame(x,columns=names)

def transform_state(model,names):
    imp,scale=model.steps[0][1],model.steps[1][1]
    state={'feature_names':names,'imputer_statistics':imp.statistics_.tolist(),'indicator_indices':imp.indicator_.features_.tolist(),
        'scaler_mean':scale.mean_.tolist(),'scaler_scale':scale.scale_.tolist(),'scaler_variance':scale.var_.tolist(),
        'raw_width':len(names),'transformed_width':int(scale.n_features_in_),'training_rows':int(scale.n_samples_seen_)}
    state['sha256']=fingerprint(state);return state

def fit_pair(observations,outcomes,views,group,names,minutes,cutoff,baseline_meta,c,registered_groups=GROUPS):
    if group not in registered_groups:raise ValueError('registered_rich_group_required')
    target={'target_id':f'technical_endpoint_midpoint_elapsed_{minutes}m','horizon_seconds':minutes*60}
    tv=TrainingView(c['training_start'],cutoff,cutoff,cutoff,c['evaluation_asof'])
    rows=population(observations,outcomes,target,tv)
    if len(rows)<c['minimum_training_rows']:raise ValueError('shared_mature_support_missing')
    if fingerprint(rows)!=baseline_meta['training_population_sha256'] or baseline_meta['target']!=target or baseline_meta['fit_cutoff']!=cutoff:raise ValueError('baseline_training_population_mismatch')
    selected=[views[r['record_id']][group] for r,_ in rows]
    if any(v['feature_names']!=names or not v['shared_legacy_population_eligible'] for v in selected):raise ValueError('registered_training_view_mismatch')
    x=frame([v['values'] for v in selected],names);y=np.asarray([o['value'] for _,o in rows])
    with threadpool_limits(limits=1):
        ridge=Pipeline(recovered.pipeline(False,c['hgb_parameters']).steps[:-1]+[('ridge',Ridge(alpha=c['ridge_lambda'],solver='cholesky',fit_intercept=True))])
        ridge.fit(x,y)
        tree=recovered.fit_one(x,names,y,c['hgb_parameters'],sample_weights=np.ones(len(rows)))
    state=transform_state(ridge,names)
    if state!=transform_state(tree,names):raise ValueError('learners_must_share_fitted_information')
    models={}
    for method,model in [('ridge',ridge),('recovered_hgb',tree)]:
        canonicalize_model_strings(model);b=io.BytesIO();joblib.dump(model,b,compress=0,protocol=5);models[method]=b.getvalue()
    meta={'schema_version':'forex_rich_family_fit.v1','group':group,'target':target,'fit_cutoff':cutoff,'ready_epoch':cutoff+c['fit_latency_seconds'],
        'training_view':tv.identity(),'training_rows':len(rows),'training_population_sha256':fingerprint(rows),'raw_rich_population_sha256':fingerprint(selected),
        'baseline_fit_id':baseline_meta['fit_id'],'maximum_outcome_available_epoch':max(o['available_epoch'] for _,o in rows),
        'feature_names':names,'feature_schema_sha256':fingerprint(names),'fitted_transform':state,
        'model_sha256':{m:hashlib.sha256(b).hexdigest() for m,b in models.items()},
        'equal_training_population_and_measure':True,'unit_row_weights':True,'probability_scope':'not_provided'}
    meta['fit_id']=fingerprint(meta);return meta,models

def validate_fit(meta,models):
    if meta['fit_id']!=fingerprint({k:v for k,v in meta.items() if k!='fit_id'}):raise ValueError('rich_fit_metadata_identity_mismatch')
    if set(models)!=set(METHODS) or any(hashlib.sha256(b).hexdigest()!=meta['model_sha256'][m] for m,b in models.items()):raise ValueError('rich_model_bytes_changed')
    if meta['maximum_outcome_available_epoch']>meta['fit_cutoff']:raise ValueError('unmatured_rich_fit_labels')

def predict(meta,models,observations,views,*,procedure,c):
    validate_fit(meta,models);valid=[];coverage=[];forecasts=[];group=meta['group']
    for r in observations:
        reason='eligible'
        if meta['ready_epoch']>r['origin_epoch']:reason='model_not_ready'
        elif r['available_epoch']>r['origin_epoch']:reason='feature_not_ready'
        elif r['features'] is None:reason='missing_features'
        elif not views[r['record_id']][group]['shared_legacy_population_eligible']:raise ValueError('unmatched_rich_assessment_population')
        if reason=='eligible':valid.append(r)
        coverage.extend({'record_id':r['record_id'],'instrument':r['instrument'],'decision_epoch':r['origin_epoch'],'target_id':meta['target']['target_id'],'group':group,'method':m,'procedure':procedure,'reason':reason} for m in METHODS)
    if valid:
        selected=[views[r['record_id']][group] for r in valid]
        if any(v['feature_names']!=meta['feature_names'] for v in selected):raise ValueError('rich_prediction_schema_mismatch')
        x=frame([v['values'] for v in selected],meta['feature_names'])
        with threadpool_limits(limits=1):values={m:joblib.load(io.BytesIO(b)).predict(x) for m,b in models.items()}
        if not all(np.isfinite(a).all() for a in values.values()):raise ValueError('nonfinite_rich_forecast')
        tv=TrainingView(**{k:v for k,v in meta['training_view'].items() if k!='schema'})
        for method in METHODS:
            model_id=fingerprint({'fit_id':meta['fit_id'],'method':method})
            for r,v,value in zip(valid,selected,values[method]):
                f=forecast_record(forecast_id=fingerprint({'model':model_id,'original_rich_sha256':v['original_record_sha256'],'procedure':procedure}),instrument=r['instrument'],decision_epoch=r['origin_epoch'],available_epoch=r['origin_epoch']+c['prediction_latency_seconds'],model_id=model_id,model_ready_epoch=meta['ready_epoch'],training_view=tv,target_id=meta['target']['target_id'],prediction=float(value))
                forecasts.append({'record_id':r['record_id'],'group':group,'method':method,'procedure':procedure,'original_rich_sha256':v['original_record_sha256'],'forecast':f})
    return forecasts,coverage

def score_chunk(forecasts,outcomes,baseline_scores,c):
    lookup={(o['record_id'],o['target_id']):o for o in outcomes};groups={};result=[]
    for row in forecasts:
        f=row['forecast'];o=lookup.get((row['record_id'],f['target_id']))
        if o is None or o['value'] is None or o['available_epoch']>c['evaluation_asof']:continue
        key=(row['group'],f['target_id'],row['procedure'],row['method']);groups.setdefault(key,[]).append((row['record_id'],f['prediction'],o['value']))
    for (group,target,procedure,method),rows in sorted(groups.items()):
        support=fingerprint(sorted(r[0] for r in rows));base=[s for s in baseline_scores if s['target_id']==target and s['procedure']==procedure]
        if len(base)!=4 or any(s['support_sha256']!=support or s['rows']!=len(rows) for s in base):raise ValueError('rich_baseline_score_support_mismatch')
        pred=np.asarray([r[1] for r in rows]);actual=np.asarray([r[2] for r in rows]);mae=float(np.mean(abs(pred-actual)));mse=float(np.mean((pred-actual)**2))
        result.append({'group':group,'target_id':target,'procedure':procedure,'method':method,'rows':len(rows),'support_sha256':support,'mae_bps':mae,'mse_bps2':mse,
            'paired_error_deltas':{s['method']:{'mae_bps':mae-s['mae_bps'],'mse_bps2':mse-s['mse_bps2']} for s in base},
            'scope':'overlapping_inspected_development_errors_no_independent_confirmation'})
    if len(result)!=2:raise ValueError('both_rich_score_methods_required')
    return result
