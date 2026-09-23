"""Bounded retrospective gross-midpoint research; never an execution-policy input.

Uses the reviewed fit_model/issue primitives without relabeling their synthetic runner.
All input provenance is externally pinned by the operator before this module is loaded.
"""
from copy import deepcopy
import json
from math import sqrt
from pathlib import Path
from contracts import TrainingView, fingerprint, validate_forecast
from fitted_consumer_v2 import fit_model, issue
from historical_inputs_v2 import verify

PROCEDURES=('frozen','adaptive','no_change','training_mean')
LIMITATIONS=['previously_inspected_development_period_not_confirmation',
 'assumed_bar_close_feature_and_label_availability_not_recorded_arrival',
 'declared_simulated_fit_and_prediction_latency',
 'elapsed_day_targets_not_trading_sessions_or_daily_close',
 'gross_midpoint_return_bps_not_executable_profit',
 'dependent_overlapping_rows_and_shared_currencies_no_independence_claim',
 'no_model_promotion_or_trading_authorization']

def load_inputs(root,contract):
    receipt=verify(root)
    observations=[json.loads(x) for x in (root/'historical_observations.jsonl').read_text(encoding='utf-8').splitlines()]
    outcomes=[json.loads(x) for x in (root/'historical_outcomes.jsonl').read_text(encoding='utf-8').splitlines()]
    validate_contract(contract)
    if set(x['instrument'] for x in observations)!=set(contract['universe']):raise ValueError('historical_universe_mismatch')
    return observations,outcomes,receipt

def validate_contract(c):
    expected={'schema_version','mode','broker_access','universe','training_origin_start','fit_cutoffs',
      'decision_epochs','evaluation_asof_epoch','fit_latency_seconds','prediction_latency_seconds',
      'ridge_lambda','min_training_rows','targets','feature_schema','procedures','limitations'}
    if set(c)!=expected or c['schema_version']!='forex_historical_slice.v1':raise ValueError('exact_historical_contract_required')
    if c['mode']!='retrospective_gross_midpoint_development' or c['broker_access'] is not False:raise ValueError('historical_research_only')
    if c['procedures']!=list(PROCEDURES) or c['limitations']!=LIMITATIONS:raise ValueError('research_scope_mismatch')
    if len(c['universe'])!=68 or len(set(c['universe']))!=68:raise ValueError('all68_required')
    if c['feature_schema']!=['return_60m_bps','return_240m_bps','return_1440m_bps','spread_bps','utc_hour_sine']:raise ValueError('feature_schema_mismatch')
    for key in ['fit_cutoffs','decision_epochs']:
        values=c[key]
        if not values or values!=sorted(set(values)) or any(type(t) is not int or t<0 for t in values):raise ValueError('ordered_integer_schedule_required')
    if len(c['fit_cutoffs'])>32 or len(c['decision_epochs'])>128 or len(c['targets'])!=3:raise ValueError('bounded_historical_recipe_required')
    if c['targets']!=[{'target_id':f'historical_gross_midpoint_elapsed_{m}m','horizon_seconds':m*60} for m in (1440,2880,7200)]:raise ValueError('elapsed_target_identity_mismatch')
    for key in ['training_origin_start','evaluation_asof_epoch','fit_latency_seconds','prediction_latency_seconds','min_training_rows']:
        if type(c[key]) is not int or c[key]<0:raise ValueError('integer_configuration_required')
    if not c['training_origin_start']<c['fit_cutoffs'][0]<=c['decision_epochs'][0]<c['evaluation_asof_epoch']:raise ValueError('historical_window_order_mismatch')
    if c['fit_cutoffs'][-1]>c['decision_epochs'][-1] or c['decision_epochs'][-1]>=c['evaluation_asof_epoch']:raise ValueError('historical_window_order_mismatch')
    if c['fit_latency_seconds']<1 or c['min_training_rows']<8:raise ValueError('readiness_or_support_required')

def jobs(contract):
    return [(t,cutoff) for t in contract['targets'] for cutoff in contract['fit_cutoffs']]

def fit_one(observations,outcomes,contract,target,cutoff):
    view=TrainingView(contract['training_origin_start'],cutoff,cutoff,cutoff,contract['evaluation_asof_epoch'])
    return fit_model(observations,outcomes,universe=contract['universe'],target=target,view=view,
        ready_epoch=cutoff+contract['fit_latency_seconds'],ridge_lambda=contract['ridge_lambda'],min_rows=contract['min_training_rows'])

def control_model(model,kind):
    """The standardized unpenalized ridge intercept equals its training-label mean."""
    result=deepcopy(model);result.pop('model_id');result['parent_model_id']=model['model_id'];result['control']=kind
    result['coefficient']=[model['coefficient'][0] if kind=='training_mean' else 0.0]+[0.0]*model['feature_count']
    result['model_id']=fingerprint(result)
    return result

def issue_all(observations,models,contract):
    validate_contract(contract)
    by_origin={}
    for o in observations:
        key=(o['origin_epoch'],o['instrument'])
        if key in by_origin:raise ValueError('duplicate_historical_origin')
        if o['features'] is not None and len(o['features'])!=5:raise ValueError('historical_feature_width')
        by_origin[key]=o
    fitted=[m for m in models if m['status']=='fitted'];controls=[];forecasts=[];coverage=[]
    for target in contract['targets']:
        current=[m for m in fitted if m['target']==target]
        first=next((m for m in current if m['training_view']['fit_cutoff_epoch']==contract['fit_cutoffs'][0]),None)
        baselines={kind:control_model(first,kind) for kind in ('no_change','training_mean')} if first else {}
        controls.extend(baselines.values())
        for epoch in contract['decision_epochs']:
            ready=[m for m in current if m['ready_epoch']<=epoch]
            selected={'adaptive':max(ready,key=lambda m:m['ready_epoch']) if ready else None,'frozen':first,**baselines}
            for procedure in PROCEDURES:
                for pair in sorted(contract['universe']):
                    record,reason=issue(selected.get(procedure),by_origin.get((epoch,pair)),decision_epoch=epoch,
                        available_epoch=epoch+contract['prediction_latency_seconds'],procedure=procedure)
                    coverage.append({'instrument':pair,'decision_epoch':epoch,'target_id':target['target_id'],
                        'procedure':procedure,'reason':reason,'forecast_id':record['forecast_id'] if record else None})
                    if record:forecasts.append(record)
    return {'models':models+controls,'forecasts':forecasts,'coverage':coverage}

def settle(forecasts,coverage,outcomes,contract):
    """Separate later-label join; predictions and issuance masks are already fixed."""
    lookup={}
    for o in outcomes:
        key=(o['record_id'],o['target_id'])
        if key in lookup:raise ValueError('duplicate_settlement_outcome')
        lookup[key]=o
    procedures={r['forecast_id']:r['procedure'] for r in coverage if r['forecast_id']}
    settled=[]
    for row in forecasts:
        validate_forecast(row)
        key=(f"{row['instrument']}:{row['decision_epoch']}",row['target_id']);o=lookup.get(key)
        if o is None:status='missing_outcome_record'
        elif o['available_epoch']>contract['evaluation_asof_epoch']:status='right_censored_at_evaluation_asof'
        elif o['value'] is None:status='unavailable_endpoint'
        else:status='matured'
        settled.append({'forecast_id':row['forecast_id'],'procedure':procedures[row['forecast_id']],
          'instrument':row['instrument'],'decision_epoch':row['decision_epoch'],'target_id':row['target_id'],
          'status':status,'outcome_available_epoch':o['available_epoch'] if o else None,
          'prediction_bps':row['prediction'],'actual_bps':o['value'] if status=='matured' else None})
    return settled

def scores(settled,contract):
    result=[]
    for target in contract['targets']:
        rows=[r for r in settled if r['target_id']==target['target_id'] and r['status']=='matured']
        keys={p:{(r['instrument'],r['decision_epoch']) for r in rows if r['procedure']==p} for p in PROCEDURES}
        matched=set.intersection(*keys.values())
        for p in PROCEDURES:
            group=[r for r in rows if r['procedure']==p and (r['instrument'],r['decision_epoch']) in matched]
            errors=[r['prediction_bps']-r['actual_bps'] for r in group]
            result.append({'target_id':target['target_id'],'procedure':p,'matched_rows':len(group),
              'instruments':len({r['instrument'] for r in group}),'origin_days':len({r['decision_epoch']//86400 for r in group}),
              'mae_bps':sum(abs(v) for v in errors)/len(errors) if errors else None,
              'rmse_bps':sqrt(sum(v*v for v in errors)/len(errors)) if errors else None,
              'independent_sample_count':None,'scope':'matched_dependent_development_rows_only'})
    return result

def policy_admission(contract):
    """Explicitly withhold gross return forecasts from executable continuation logic."""
    validate_contract(contract)
    return {'status':'blocked','eligible_policy_frames':0,'reason':'gross_midpoint_forecasts_are_not_executable_common_target_values',
      'required':['remaining_horizon_forecasts_to_original_thesis_target','qualified_bid_ask_and_conversion_at_decision_and_execution',
       'signed_remaining_financing_and_fill_evidence','explicit_recorded_or_scenario_arrival_latency_contract'],
      'synthetic_policy_fixture_is_not_historical_evidence':True}
