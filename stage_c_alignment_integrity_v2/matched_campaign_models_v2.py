"""Matched causal training population; reuse existing ridge and corrected HGB."""
import io,json,sys
import joblib
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits
from contracts import TrainingView,fingerprint,forecast_record
from fitted_consumer_v2 import fit_model
import retained_signed_cost_models_v1 as recovered
FEATURES=list(recovered.TECHNICAL)+['known_entry_long_cost_bps','known_entry_short_cost_bps']
METHODS=('zero','history_mean','ridge','recovered_hgb')
HORIZONS=(15,60,240,720,1440,2880,7200)
PARAMS={'max_iter':100,'max_leaf_nodes':15,'max_depth':3,'learning_rate':.05,'min_samples_leaf':100,'l2_regularization':10.0,'early_stopping':False,'random_state':37}

def canonicalize_model_strings(value, memo=None):
    """Normalize string aliases before pickle; numerical model state is untouched.

    Pickle memoizes object identity. A deserialized sklearn parameter can equal
    a dictionary key without sharing its identity, unlike the default parameter
    in a fresh process. Intern only strings throughout the fitted sklearn graph
    so warm-process resume and fresh-process fitting produce identical bytes.
    Preserve mutable aliases and reject cycles through immutable containers.
    """
    if isinstance(value,str):return sys.intern(value)
    if value is None or isinstance(value,(bool,int,float,bytes,np.generic)):return value
    if memo is None:memo={}
    key=id(value)
    if key in memo:
        if memo[key] is None:raise ValueError('unsupported_immutable_model_cycle')
        return memo[key]
    memo[key]=value
    if isinstance(value,dict):
        items=[(canonicalize_model_strings(k,memo),canonicalize_model_strings(v,memo)) for k,v in value.items()]
        value.clear();value.update(items)
    elif isinstance(value,list):value[:]=[canonicalize_model_strings(v,memo) for v in value]
    elif isinstance(value,tuple):
        memo[key]=None;value=tuple(canonicalize_model_strings(v,memo) for v in value);memo[key]=value
    elif isinstance(value,np.ndarray) and value.dtype.kind=='O':
        for i in np.ndindex(value.shape):value[i]=canonicalize_model_strings(value[i],memo)
    elif type(value).__module__.startswith('sklearn.') and hasattr(value,'__dict__'):
        canonicalize_model_strings(value.__dict__,memo)
    return value

def contract():
    return {'schema_version':'forex_matched_development_campaign.v1','training_start':1720396860,'fit_cutoffs':[1721606400,1721779200],
        'decision_epochs':list(range(1721606460,1722038400,21600)),'evaluation_asof':1722556800,'fit_latency_seconds':30,
        'prediction_latency_seconds':2,'ridge_lambda':20.,'minimum_training_rows':100,'horizon_minutes':list(HORIZONS),
        'features':FEATURES,'methods':list(METHODS),'procedures':['frozen','adaptive'],'hgb_parameters':PARAMS,
        'training_measure':'equal_weight_per_eligible_pair_origin_row_for_both_models','selection':'report_all_no_winner_promotion_or_parameter_search',
        'scope':'previously_inspected_development_endpoint_forecast_comparison','broker_access':False,'policy_evaluated':False}

def population(observations,outcomes,target,view):
    # The existing ridge is the admission authority. This extraction must match
    # its exact population fingerprint before the recovered tree can be fitted.
    mature={o['record_id']:o for o in outcomes if o['target_id']==target['target_id'] and o['available_epoch']<=view.fit_cutoff_epoch}
    rows=[]
    for r in observations:
        t=r['origin_epoch']
        if not view.origin_start_epoch<=t<view.origin_end_epoch or r['available_epoch']>t:continue
        o=mature.get(r['record_id'])
        if o is None or r['features'] is None or o['value'] is None:continue
        if not np.isfinite(r['features']).all() or not np.isfinite(o['value']):continue
        if view.eligible(np.array([t]),np.array([o['available_epoch']]),np.array([o['label_end_epoch']]))[0]:rows.append((r,o))
    return sorted(rows,key=lambda x:(x[0]['origin_epoch'],x[0]['instrument'],x[0]['record_id']))

def fit_pair(observations,outcomes,universe,minutes,cutoff,c):
    target={'target_id':f'technical_endpoint_midpoint_elapsed_{minutes}m','horizon_seconds':minutes*60}
    view=TrainingView(c['training_start'],cutoff,cutoff,cutoff,c['evaluation_asof'])
    with threadpool_limits(limits=1):
        ridge=fit_model(observations,outcomes,universe=universe,target=target,view=view,ready_epoch=cutoff+c['fit_latency_seconds'],ridge_lambda=c['ridge_lambda'],min_rows=c['minimum_training_rows'])
        if ridge['status']!='fitted':raise ValueError('predeclared_training_support_missing')
        rows=population(observations,outcomes,target,view)
        if fingerprint(rows)!=ridge['training_population_sha256']:raise ValueError('ridge_tree_training_population_mismatch')
        x=pd.DataFrame([r['features'] for r,_ in rows],columns=FEATURES);x['epoch']=[r['origin_epoch'] for r,_ in rows];y=np.asarray([o['value'] for _,o in rows])
        tree=recovered.fit_one(x,FEATURES,y,c['hgb_parameters'],sample_weights=np.ones(len(rows)))
        canonicalize_model_strings(tree)
        b=io.BytesIO();joblib.dump(tree,b,compress=0,protocol=5);tree_bytes=b.getvalue()
    import hashlib
    metadata={'schema_version':'forex_matched_fit_pair.v1','target':target,'fit_cutoff':cutoff,'ready_epoch':cutoff+c['fit_latency_seconds'],
        'training_view':view.identity(),'training_rows':len(rows),'training_population_sha256':fingerprint(rows),
        'maximum_outcome_available_epoch':max(o['available_epoch'] for _,o in rows),'feature_schema_sha256':fingerprint(FEATURES),
        'ridge':ridge,'history_mean':float(y.mean()),'tree_sha256':hashlib.sha256(tree_bytes).hexdigest(),
        'tree_parameters':c['hgb_parameters'],'equal_training_population_and_measure':True,'probability_scope':'not_provided'}
    metadata['fit_id']=fingerprint(metadata);return metadata,tree_bytes

def validate_fit(meta,tree_bytes):
    import hashlib
    if meta['fit_id']!=fingerprint({k:v for k,v in meta.items() if k!='fit_id'}):raise ValueError('fit_metadata_identity_mismatch')
    if hashlib.sha256(tree_bytes).hexdigest()!=meta['tree_sha256']:raise ValueError('fitted_tree_identity_mismatch')
    if meta['maximum_outcome_available_epoch']>meta['fit_cutoff']:raise ValueError('unmatured_fit_labels')

def predict(meta,tree_bytes,observations,*,procedure,c):
    validate_fit(meta,tree_bytes);valid=[];coverage=[];forecasts=[]
    for r in observations:
        reason='eligible'
        if meta['ready_epoch']>r['origin_epoch']:reason='model_not_ready'
        elif r['available_epoch']>r['origin_epoch']:reason='feature_not_ready'
        elif r['features'] is None:reason='missing_features'
        elif len(r['features'])!=len(FEATURES) or not np.isfinite(r['features']).all():raise ValueError('invalid_campaign_features')
        if reason=='eligible':valid.append(r)
        coverage.extend({'record_id':r['record_id'],'instrument':r['instrument'],'decision_epoch':r['origin_epoch'],'target_id':meta['target']['target_id'],'method':method,'procedure':procedure,'reason':reason} for method in METHODS)
    if valid:
        x=np.asarray([r['features'] for r in valid]);ridge=meta['ridge'];tree=joblib.load(io.BytesIO(tree_bytes))
        with threadpool_limits(limits=1):
            values={'zero':np.zeros(len(valid)),'history_mean':np.full(len(valid),meta['history_mean']),
                'ridge':np.column_stack((np.ones(len(valid)),(x-np.asarray(ridge['mean']))/np.asarray(ridge['scale'])))@np.asarray(ridge['coefficient']),
                'recovered_hgb':tree.predict(pd.DataFrame(x,columns=FEATURES))}
        fields={k:v for k,v in meta['training_view'].items() if k!='schema'}
        for method in METHODS:
            model_id=fingerprint({'fit_id':meta['fit_id'],'method':method})
            for r,value in zip(valid,values[method]):
                f=forecast_record(forecast_id=fingerprint({'model':model_id,'observation':r,'procedure':procedure}),instrument=r['instrument'],decision_epoch=r['origin_epoch'],available_epoch=r['origin_epoch']+c['prediction_latency_seconds'],model_id=model_id,model_ready_epoch=meta['ready_epoch'],training_view=TrainingView(**fields),target_id=meta['target']['target_id'],prediction=float(value))
                forecasts.append({'record_id':r['record_id'],'method':method,'procedure':procedure,'forecast':f})
    return forecasts,coverage

def score(forecasts,outcomes,c):
    lookup={(o['record_id'],o['target_id']):o for o in outcomes};groups={};matched={}
    for row in forecasts:
        f=row['forecast'];key=(row['record_id'],f['target_id']);o=lookup.get(key)
        if o is None or o['value'] is None or o['available_epoch']>c['evaluation_asof']:continue
        group=(f['target_id'],row['procedure'],row['method']);groups.setdefault(group,[]).append((row['record_id'],f['prediction'],o['value']))
    results=[]
    for (target,procedure,method),rows in sorted(groups.items()):
        support=sorted(r[0] for r in rows);k=(target,procedure)
        if k in matched and matched[k]!=support:raise ValueError('unmatched_scoring_population')
        matched[k]=support;pred=np.asarray([r[1] for r in rows]);actual=np.asarray([r[2] for r in rows])
        results.append({'target_id':target,'procedure':procedure,'method':method,'rows':len(rows),'support_sha256':fingerprint(support),
            'mae_bps':float(np.mean(abs(pred-actual))),'mse_bps2':float(np.mean((pred-actual)**2)),
            'scope':'overlapping_development_endpoint_errors_not_portfolio_returns'})
    if len(results)!=len(HORIZONS)*len(METHODS)*2:raise ValueError('declared_score_group_missing')
    return results
