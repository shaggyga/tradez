"""Maturity-qualified pooled ridge consumer of the existing TrainingView contract.

Reuses the normalization/ridge algebra from all68_endpoint_baseline._fit_predict,
but publishes a persistent model fitted only on a permitted training population.
Issuance never depends on the existence or value of its future outcome.
"""
from dataclasses import asdict
from math import isfinite
import numpy as np
from contracts import TrainingView, fingerprint, forecast_record


def integer(value,name):
    if type(value) is not int or value<0:raise ValueError('invalid_clock:'+name)
    return value


def fit_model(observations,outcomes,*,universe,target,view,ready_epoch,ridge_lambda=20.0,min_rows=8):
    if not isinstance(view,TrainingView):raise ValueError('explicit_training_view_required')
    integer(ready_epoch,'ready_epoch')
    if ready_epoch<view.fit_cutoff_epoch:raise ValueError('model_ready_before_fit_cutoff')
    if not isfinite(ridge_lambda) or ridge_lambda<=0:raise ValueError('positive_ridge_penalty_required')
    if type(min_rows) is not int or min_rows<2:raise ValueError('minimum_training_support_required')
    selected=[]; seen=set(); reasons={}
    # Filter by known-at clocks before inspecting future feature/label values.
    relevant={}
    for o in outcomes:
        if o.get('target_id')!=target['target_id'] or o['available_epoch']>view.fit_cutoff_epoch:continue
        key=o['record_id']
        if key in relevant:raise ValueError('duplicate_mature_outcome')
        relevant[key]=o
    for row in observations:
        origin=integer(row['origin_epoch'],'origin')
        if not view.origin_start_epoch<=origin<view.origin_end_epoch:continue
        if row['instrument'] not in universe:raise ValueError('unknown_training_instrument')
        key=row['record_id']
        if key in seen:raise ValueError('duplicate_training_observation')
        seen.add(key)
        o=relevant.get(key)
        reason=None
        if row['available_epoch']>origin:reason='feature_not_ready_at_origin'
        elif o is None:reason='outcome_not_matured'
        elif integer(o['label_end_epoch'],'label_end')!=origin+target['horizon_seconds']:raise ValueError('label_target_clock_mismatch')
        elif integer(o['available_epoch'],'outcome_available')<o['label_end_epoch']:raise ValueError('outcome_available_before_target')
        elif not view.eligible(np.array([origin]),np.array([o['available_epoch']]),np.array([o['label_end_epoch']]))[0]:reason='outside_allowed_training_view'
        elif row['features'] is None or not all(isfinite(float(x)) for x in row['features']):reason='unavailable_training_features'
        elif o['value'] is None or not isfinite(float(o['value'])):reason='unavailable_training_outcome'
        if reason:reasons[reason]=reasons.get(reason,0)+1
        else:selected.append((row,o))
    selected.sort(key=lambda item:(item[0]['origin_epoch'],item[0]['instrument'],item[0]['record_id']))
    if len(selected)<min_rows:
        return {'status':'insufficient_mature_training_rows','eligible_rows':len(selected),'rejections':reasons}
    if len(selected)>50000:raise ValueError('bounded_fit_rows_exceeded')
    widths={len(row['features']) for row,_ in selected}
    if len(widths)!=1 or not 1<=next(iter(widths))<=64:raise ValueError('fixed_bounded_feature_schema_required')
    x=np.asarray([row['features'] for row,_ in selected],dtype=np.float64)
    y=np.asarray([o['value'] for _,o in selected],dtype=np.float64)
    mean=x.mean(axis=0);scale=x.std(axis=0);scale[scale==0]=1.0
    normalized=(x-mean)/scale;design=np.column_stack((np.ones(len(x)),normalized))
    penalty=np.eye(design.shape[1])*ridge_lambda;penalty[0,0]=0.0
    coefficient=np.linalg.solve(design.T@design+penalty,design.T@y)
    if not np.isfinite(coefficient).all():raise ValueError('nonfinite_fitted_model')
    model={'schema_version':'forex_qualified_ridge.v2','status':'fitted','target':target,
        'training_view':view.identity(),'ready_epoch':ready_epoch,'feature_count':x.shape[1],
        'mean':mean.tolist(),'scale':scale.tolist(),'coefficient':coefficient.tolist(),'ridge_lambda':ridge_lambda,
        'training_rows':len(selected),'training_population_sha256':fingerprint(selected),'rejections':reasons,
        'maximum_outcome_available_epoch':max(o['available_epoch'] for _,o in selected),
        'numpy_version':np.__version__,'implementation_reuse':'stage_c_all68_20260921/all68_endpoint_baseline.py::_fit_predict algebra'}
    model['model_id']=fingerprint(model)
    return model


def issue(model,observation,*,decision_epoch,available_epoch,procedure):
    integer(decision_epoch,'decision');integer(available_epoch,'forecast_available')
    if model is None:return None,'model_not_ready'
    expected={k:v for k,v in model.items() if k!='model_id'}
    if model['model_id']!=fingerprint(expected):raise ValueError('fitted_model_identity_mismatch')
    if model['ready_epoch']>decision_epoch:return None,'model_not_ready'
    if observation is None:return None,'missing_origin_observation'
    if observation['origin_epoch']!=decision_epoch:raise ValueError('exact_origin_observation_required')
    if observation['available_epoch']>decision_epoch:return None,'feature_not_ready'
    x=observation['features']
    if x is None or not all(isfinite(float(v)) for v in x):return None,'missing_features'
    if len(x)!=model['feature_count']:raise ValueError('prediction_feature_schema_mismatch')
    prediction=float(np.r_[1.0,(np.asarray(x,dtype=np.float64)-np.asarray(model['mean']))/np.asarray(model['scale'])]@np.asarray(model['coefficient']))
    fields={k:v for k,v in model['training_view'].items() if k!='schema'}
    row=forecast_record(forecast_id=fingerprint({'model':model['model_id'],'observation':observation,'procedure':procedure}),
        instrument=observation['instrument'],decision_epoch=decision_epoch,available_epoch=available_epoch,
        model_id=model['model_id'],model_ready_epoch=model['ready_epoch'],training_view=TrainingView(**fields),
        target_id=model['target']['target_id'],prediction=prediction)
    return row,'eligible'


def evaluate(inputs):
    """Frozen and scheduled adaptive fits with explicit latency and all-pair coverage."""
    if set(inputs)!={'schema_version','universe','observations','outcomes','contract'} or inputs['schema_version']!='forex_fitted_fixture.v2':
        raise ValueError('exact_fitted_input_contract_required')
    universe=inputs['universe'];observations=inputs['observations'];outcomes=inputs['outcomes'];contract=inputs['contract']
    if set(contract)!={'mode','broker_access','training_origin_start','fit_cutoffs','decision_epochs','fit_latency_seconds',
                       'prediction_latency_seconds','evaluation_window_seconds','ridge_lambda','min_training_rows','targets'}:
        raise ValueError('exact_fitted_configuration_required')
    if len(observations)>50000 or len(outcomes)>400000:raise ValueError('bounded_input_population_exceeded')
    integer(contract['training_origin_start'],'training_origin_start')
    if integer(contract['evaluation_window_seconds'],'evaluation_window')<1:raise ValueError('positive_evaluation_window_required')
    integer(contract['prediction_latency_seconds'],'prediction_latency')
    if len({t['target_id'] for t in contract['targets']})!=len(contract['targets']):raise ValueError('unique_targets_required')
    if len(universe)!=68 or len(set(universe))!=68:raise ValueError('explicit_all68_universe_required')
    origins=contract['decision_epochs'];schedule=contract['fit_cutoffs']
    if not origins or origins!=sorted(set(origins)) or schedule!=sorted(set(schedule)) or not schedule:
        raise ValueError('ordered_unique_decisions_and_fit_schedule_required')
    if len(origins)>128 or len(schedule)>32 or len(contract['targets'])>8:raise ValueError('bounded_fitted_recipe_required')
    if any(type(t) is not int for t in origins+schedule):raise ValueError('integer_schedule_required')
    if type(contract['fit_latency_seconds']) is not int or contract['fit_latency_seconds']<1:raise ValueError('positive_declared_fit_latency_required')
    if contract['mode']!='offline_synthetic_qualification' or contract['broker_access'] is not False:raise ValueError('offline_qualification_only')
    models=[];attempts=[];forecasts=[];coverage=[]
    for target in contract['targets']:
        if type(target['horizon_seconds']) is not int or target['horizon_seconds']<1:raise ValueError('positive_target_horizon_required')
        for cutoff in schedule:
            view=TrainingView(contract['training_origin_start'],cutoff,cutoff,cutoff,cutoff+contract['evaluation_window_seconds'])
            model=fit_model(observations,outcomes,universe=universe,target=target,view=view,
                ready_epoch=cutoff+contract['fit_latency_seconds'],ridge_lambda=contract['ridge_lambda'],min_rows=contract['min_training_rows'])
            attempts.append({'target_id':target['target_id'],'fit_cutoff_epoch':cutoff,'status':model['status'],'eligible_rows':model.get('training_rows',model.get('eligible_rows'))})
            if model['status']=='fitted':models.append(model)
        fitted=[m for m in models if m['target']==target]
        for epoch in origins:
            current={}
            for row in observations:
                if row['origin_epoch']!=epoch:continue
                if row['instrument'] in current:raise ValueError('duplicate_prediction_observation')
                if row['instrument'] not in universe:raise ValueError('unknown_prediction_instrument')
                current[row['instrument']]=row
            for procedure in ('frozen','adaptive'):
                ready=[m for m in fitted if m['ready_epoch']<=epoch and (procedure=='adaptive' or m['training_view']['fit_cutoff_epoch']==schedule[0])]
                model=max(ready,key=lambda m:m['ready_epoch']) if ready else None
                for pair in sorted(universe):
                    row,reason=issue(model,current.get(pair),decision_epoch=epoch,available_epoch=epoch+contract['prediction_latency_seconds'],procedure=procedure)
                    coverage.append({'instrument':pair,'decision_epoch':epoch,'target_id':target['target_id'],'procedure':procedure,'reason':reason,'forecast_id':None if row is None else row['forecast_id']})
                    if row is not None:forecasts.append(row)
    return {'models':models,'attempts':attempts,'forecasts':forecasts,'coverage':coverage,
        'status':'synthetic_fitted_consumer_qualification_only','engineering_ready':False,'forecast_evidence_status':'synthetic_only_no_market_evidence',
        'policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted'}
