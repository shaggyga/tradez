"""Shared prefix-only fitter for declared matched control feature views.

Reuses original population, preprocessing, transform inspection and serialization.
The frozen caller owns group/feature definitions; this numerical helper does not
choose candidates or alter source observations/outcomes.
"""
import hashlib,io
import joblib,numpy as np
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from threadpoolctl import threadpool_limits
from contracts import TrainingView,fingerprint
from matched_campaign_models_v2 import population,canonicalize_model_strings
from rich_family_models_v2 import frame,transform_state
import retained_signed_cost_models_v1 as recovered

def fit_pair(observations,outcomes,views,group,names,minutes,cutoff,baseline_meta,c):
    if group not in c['groups']:raise ValueError('declared_matched_view_group_required')
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
    meta={'schema_version':'forex_matched_control_fit.v1','group':group,'target':target,'fit_cutoff':cutoff,'ready_epoch':cutoff+c['fit_latency_seconds'],
        'training_view':tv.identity(),'training_rows':len(rows),'training_population_sha256':fingerprint(rows),'control_view_population_sha256':fingerprint(selected),
        'baseline_fit_id':baseline_meta['fit_id'],'maximum_outcome_available_epoch':max(o['available_epoch'] for _,o in rows),
        'feature_names':names,'feature_schema_sha256':fingerprint(names),'fitted_transform':state,
        'model_sha256':{m:hashlib.sha256(b).hexdigest() for m,b in models.items()},
        'equal_training_population_and_measure':True,'unit_row_weights':True,'probability_scope':'not_provided'}
    meta['fit_id']=fingerprint(meta);return meta,models
