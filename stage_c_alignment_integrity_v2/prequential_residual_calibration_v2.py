"""Continuous-return residual diagnostics under explicit modeled availability clocks."""
from collections import Counter
import math,re
from contracts import fingerprint,validate_forecast

def contract():
    return {'schema_version':'forex_modeled_clock_residual_calibration.v1','minimum_distinct_origins':8,'minimum_distinct_utc_days':3,'minimum_distinct_pairs':20,'frozen_cutoff':1721865600,'assessment_asof':1722556800,'modes':['frozen_prefix','expanding_prefix'],'lower_probability':.1,'upper_probability':.9,'weights':'equal_total_weight_per_origin_equal_pair_weight_within_origin','maturity':'prior_forecast_origin_before_cutoff_forecast_available_and_label_available_at_or_before_cutoff','scope':'continuous_elapsed_endpoint_residuals_not_event_probabilities','clock_qualification':'modeled_joint_readiness_and_assumed_label_bar_end_not_actual_committed_issue_receipts','uncertainty_claim':'descriptive_weighted_empirical_interval_no_distribution_free_or_conditional_coverage_guarantee','cross_section':'pooled_all68_within_fixed_group_target_base_procedure_method_no_independence_claim'}

def weighted_quantile(pairs,probability):
    if not 0<=probability<=1 or not pairs:raise ValueError('weighted_quantile_input_required')
    ordered=sorted(pairs);total=math.fsum(w for _,w in ordered)
    if any(not math.isfinite(x) or not math.isfinite(w) or w<=0 for x,w in ordered):raise ValueError('finite_positive_weight_required')
    threshold=probability*total;running=0.;compensation=0.
    for value,weight in ordered:
        increment=weight-compensation;updated=running+increment
        compensation=(updated-running)-increment;running=updated
        if running>=threshold:return value
    return ordered[-1][0]

def fit_snapshot(forecasts,outcomes,cutoff,scope,c=None):
    c=c or contract();eligible=[];ids=set()
    if type(cutoff) is not int or cutoff<0:raise ValueError('integer_calibration_cutoff_required')
    for row in forecasts:
        f=row['forecast'];validate_forecast(f)
        if (row['group'],f['target_id'],row['procedure'],row['method'])!=tuple(scope):raise ValueError('fixed_calibration_scope_required')
        key=(row['record_id'],f['target_id']);o=outcomes.get(key)
        if row['record_id']!=f"{f['instrument']}:{f['decision_epoch']}":raise ValueError('canonical_calibration_event_identity_required')
        if key in ids:raise ValueError('unique_calibration_market_event_required')
        ids.add(key)
        if o is None:raise ValueError('original_calibration_outcome_missing')
        target=re.fullmatch(r'technical_endpoint_midpoint_elapsed_([0-9]+)m',f['target_id'])
        if target is None or o['target_id']!=f['target_id'] or o['label_end_epoch']!=f['decision_epoch']+int(target[1])*60:raise ValueError('exact_calibration_target_maturity_required')
        if o['available_epoch']<o['label_end_epoch'] or o['label_end_epoch']<=f['decision_epoch']:raise ValueError('calibration_label_clock_invalid')
        if f['available_epoch']>=o['label_end_epoch']:raise ValueError('base_forecast_must_precede_target_maturity')
        if f['decision_epoch']>=cutoff or f['available_epoch']>cutoff or o['available_epoch']>cutoff or o['value'] is None:continue
        if not math.isfinite(o['value']):raise ValueError('finite_observed_calibration_label_required')
        eligible.append({'record_id':row['record_id'],'origin_epoch':f['decision_epoch'],'instrument':f['instrument'],'forecast_id':f['forecast_id'],'model_id':f['model_id'],'forecast_available_epoch':f['available_epoch'],'label_available_epoch':o['available_epoch'],'original_forecast_sha256':fingerprint(row),'original_outcome_sha256':fingerprint(o),'residual_bps':o['value']-f['prediction']})
    eligible.sort(key=lambda x:(x['origin_epoch'],x['instrument']));counts=Counter(x['origin_epoch'] for x in eligible)
    days={t//86400 for t in counts};pairs={x['instrument'] for x in eligible}
    support={'rows':len(eligible),'distinct_origins':len(counts),'distinct_utc_days':len(days),'distinct_pairs':len(pairs),'origin_pair_counts':{str(k):v for k,v in sorted(counts.items())},'maximum_forecast_available_epoch':max((x['forecast_available_epoch'] for x in eligible),default=None),'maximum_label_available_epoch':max((x['label_available_epoch'] for x in eligible),default=None),'training_population_sha256':fingerprint(eligible)}
    ready=len(counts)>=c['minimum_distinct_origins'] and len(days)>=c['minimum_distinct_utc_days'] and len(pairs)>=c['minimum_distinct_pairs']
    parameters=None
    if ready:
        weighted=[(x['residual_bps'],1/counts[x['origin_epoch']]) for x in eligible];denominator=math.fsum(w for _,w in weighted)
        parameters={'mean_residual_bps':math.fsum(v*w for v,w in weighted)/denominator,'lower_residual_bps':weighted_quantile(weighted,c['lower_probability']),'upper_residual_bps':weighted_quantile(weighted,c['upper_probability'])}
    result={'scope':list(scope),'cutoff_epoch':cutoff,'contract_sha256':fingerprint(c),'support':support,'status':'fitted' if ready else 'insufficient_distinct_support','parameters':parameters,'training_forecast_ids':[x['forecast_id'] for x in eligible],'base_models_refitted':False}
    result['calibrator_id']=fingerprint(result);return result

def apply_snapshot(row,snapshot,mode,c=None):
    c=c or contract();f=row['forecast'];validate_forecast(f)
    if snapshot['calibrator_id']!=fingerprint({k:v for k,v in snapshot.items() if k!='calibrator_id'}):raise ValueError('calibrator_snapshot_identity_mismatch')
    if mode not in c['modes'] or tuple(snapshot['scope'])!=(row['group'],f['target_id'],row['procedure'],row['method']):raise ValueError('calibrator_application_scope_mismatch')
    if snapshot['cutoff_epoch']>f['decision_epoch']:raise ValueError('future_calibrator_cannot_be_applied')
    if f['forecast_id'] in snapshot['training_forecast_ids']:raise ValueError('current_example_cannot_train_its_calibrator')
    p=snapshot['parameters'];result={'record_id':row['record_id'],'instrument':f['instrument'],'origin_epoch':f['decision_epoch'],'base_forecast_id':f['forecast_id'],'base_model_id':f['model_id'],'base_forecast_available_epoch':f['available_epoch'],'base_prediction_bps':f['prediction'],'mode':mode,'calibrator_id':snapshot['calibrator_id'],'calibration_cutoff':snapshot['cutoff_epoch'],'status':snapshot['status'],'adjusted_mean_bps':None,'empirical_lower_bps':None,'empirical_upper_bps':None,'production_available_epoch':None,'scope':'offline_modeled_clock_residual_diagnostic'}
    if p is not None:
        result.update(adjusted_mean_bps=f['prediction']+p['mean_residual_bps'],empirical_lower_bps=f['prediction']+p['lower_residual_bps'],empirical_upper_bps=f['prediction']+p['upper_residual_bps'])
        if result['empirical_lower_bps']>result['empirical_upper_bps']:raise ValueError('ordered_empirical_interval_required')
    return result

def pinball(prediction,value,probability):
    error=value-prediction;return max(probability*error,(probability-1)*error)

def visible_calibration(row,asof):
    if type(asof) is not int:raise ValueError('integer_calibration_inspection_asof_required')
    if asof<row['base_forecast_available_epoch']:return {'status':'modeled_base_forecast_not_available','record':None,'actual_publication_qualified':False}
    return {'status':row['status'],'record':dict(row),'actual_publication_qualified':False,'outcomes_included':False}

def score_rows(rows,outcomes,target,c=None):
    c=c or contract();eligible=[]
    for row in rows:
        o=outcomes[row['record_id'],target]
        if row['status']!='fitted' or o['value'] is None or o['available_epoch']>c['assessment_asof']:continue
        eligible.append((row,o['value']))
    if not eligible:return {'rows':0,'status':'no_mature_supported_assessment','metrics':None}
    n=len(eligible);base=[r['base_prediction_bps']-y for r,y in eligible];adjusted=[r['adjusted_mean_bps']-y for r,y in eligible]
    return {'rows':n,'status':'descriptive_assessment','support_sha256':fingerprint(sorted(r['record_id'] for r,_ in eligible)),'distinct_origins':len({r['origin_epoch'] for r,_ in eligible}),'distinct_utc_days':len({r['origin_epoch']//86400 for r,_ in eligible}),'metrics':{'base_mae_bps':math.fsum(abs(x) for x in base)/n,'adjusted_mae_bps':math.fsum(abs(x) for x in adjusted)/n,'base_mse_bps2':math.fsum(x*x for x in base)/n,'adjusted_mse_bps2':math.fsum(x*x for x in adjusted)/n,'base_bias_bps':math.fsum(base)/n,'adjusted_bias_bps':math.fsum(adjusted)/n,'lower_pinball':math.fsum(pinball(r['empirical_lower_bps'],y,c['lower_probability']) for r,y in eligible)/n,'upper_pinball':math.fsum(pinball(r['empirical_upper_bps'],y,c['upper_probability']) for r,y in eligible)/n,'empirical_interval_coverage':sum(r['empirical_lower_bps']<=y<=r['empirical_upper_bps'] for r,y in eligible)/n,'mean_interval_width_bps':math.fsum(r['empirical_upper_bps']-r['empirical_lower_bps'] for r,_ in eligible)/n},'dependence':'overlapping_targets_and_shared_currencies_no_effective_n_or_coverage_guarantee'}
