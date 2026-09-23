"""Conservative joint readiness for the isolated eight-fit remaining-target scope."""
from decimal import Decimal,Context,localcontext
import math
from contracts import fingerprint
from matched_campaign_models_v2 import predict,contract as campaign_contract,validate_fit
from matched_remaining_native_v2 import ORIGIN,TARGET,EPOCHS,NEW_MINUTES,REUSE_MINUTES,METHODS
from native_policy_input_v2 import load_native

def schedule(metadata):
    horizons=sorted(NEW_MINUTES+REUSE_MINUTES)
    if set(metadata)!=set(horizons):raise ValueError('all_eight_original_remaining_fits_required')
    cutoffs={m['fit_cutoff'] for m in metadata.values()}
    if len(cutoffs)!=1:raise ValueError('matched_remaining_cutoff_required')
    cutoff=cutoffs.pop();slot=campaign_contract()['fit_latency_seconds'];tasks=[]
    for index,h in enumerate(horizons):
        m=metadata[h]
        if m['target']['horizon_seconds']!=h*60 or m['ready_epoch']!=cutoff+slot:raise ValueError('original_remaining_fit_clock_required')
        tasks.append({'minutes':h,'fit_id':m['fit_id'],'slot_start_epoch':cutoff+index*slot,'slot_end_epoch':cutoff+(index+1)*slot})
    return {'schema_version':'forex_isolated_remaining_joint_readiness.v1','fit_cutoff':cutoff,'joint_ready_epoch':cutoff+len(tasks)*slot,'one_worker':True,'fit_pair_slot_seconds':slot,'tasks':tasks,'scope':'isolated_eight_fit_remaining_target_workload_not_full_research_campaign','clock_qualification':'modeled_reserved_budget_not_observed_historical_fit_completion','new_models_fitted':0}

def build_origin(epoch,meta,tree,observations,references,original_forecasts,plan,trad):
    if epoch not in EPOCHS or meta['target']['horizon_seconds']!=TARGET-epoch:raise ValueError('exact_remaining_original_target_required')
    if len(observations)!=68 or len({o['instrument'] for o in observations})!=68 or any(o['origin_epoch']!=epoch for o in observations):raise ValueError('all68_exact_origin_observations_required')
    task=next((x for x in plan['tasks'] if x['fit_id']==meta['fit_id']),None)
    if task is None or task['minutes']*60!=TARGET-epoch:raise ValueError('registered_remaining_fit_required')
    if epoch<plan['joint_ready_epoch']:
        return {'epoch':epoch,'original_target_epoch':TARGET,'forecasts':[],'native_packets':[],'coverage':[{'record_id':o['record_id'],'instrument':o['instrument'],'decision_epoch':epoch,'target_id':meta['target']['target_id'],'method':m,'reason':'model_set_not_joint_ready','joint_ready_epoch':plan['joint_ready_epoch']} for o in observations for m in METHODS]}
    validate_fit(meta,tree);forecasts,coverage=predict(meta,tree,observations,procedure='frozen',c=campaign_contract());coverage=[{**x,'joint_ready_epoch':plan['joint_ready_epoch']} for x in coverage if x['method'] in METHODS];selected=[x for x in forecasts if x['method'] in METHODS];source={(x['record_id'],x['method']):x for x in original_forecasts if x['method'] in METHODS};by={o['record_id']:o for o in observations};native,_=load_native(trad);packets=[];new=[]
    for row in selected:
        old=source[row['record_id'],row['method']]
        # A changed admitted batch can alter BLAS rounding at ~1e-14. Preserve
        # the original canonical tape value only after exact identity and the
        # already-used whole-curve numerical tolerance independently verify it.
        def without_value(x):return {**x,'forecast':{k:v for k,v in x['forecast'].items() if k!='prediction'}}
        if without_value(old)!=without_value(row) or not math.isclose(old['forecast']['prediction'],row['forecast']['prediction'],rel_tol=1e-12,abs_tol=1e-10):raise ValueError('original_remaining_forecast_recomputation_mismatch')
        row=old
        f={**row['forecast'],'model_ready_epoch':plan['joint_ready_epoch']};f['forecast_id']=fingerprint({'original_forecast_id':row['forecast']['forecast_id'],'joint_schedule':fingerprint(plan),'modeled_available_epoch':f['available_epoch']})
        revised={**row,'forecast':f,'original_forecast_id':row['forecast']['forecast_id'],'readiness_schedule_sha256':fingerprint(plan)};o=by[row['record_id']];ref=references[o['instrument']]
        if ref['record_sha256']!=fingerprint({k:v for k,v in ref.items() if k!='record_sha256'}) or ref['price_epoch']!=epoch or ref['source_member_sha256']!=o['source_member_sha256']:raise ValueError('original_remaining_reference_binding_required')
        with localcontext(Context(prec=192)):pips=Decimal(ref['reference_close'])*Decimal(str(f['prediction']))/10000/Decimal(ref['pip_size'])
        bindings={'fitted_model':f['model_id'],'fit_pair':meta['fit_id'],'feature_observation':fingerprint(o),'reference_price':ref['record_sha256'],'forecast':f['forecast_id'],'readiness_schedule':fingerprint(plan)}
        curve=native.prepare_curve(instrument=o['instrument'],pip_size=ref['pip_size'],forecast_cohort='joint-ready-isolated-remaining-'+row['method'],model_sha256=f['model_id'],feature_version='retained_technical24_current_cost2.v1',source_bindings=bindings,input_capture_sha256=fingerprint({'observation':o,'reference':ref}),input_available_epoch=epoch,reference_epoch=epoch,reference_label_epoch=epoch-60,reference_price=ref['reference_close'],reference_price_kind='retained_M1_close_binary_roundtrip_decimal',bar_duration_sec=60,model_fitted_epoch=plan['joint_ready_epoch'],computation_started_epoch=epoch,computed_epoch=f['available_epoch'],points=[{'horizon_sec':TARGET-epoch,'target_epoch':TARGET,'target_label_epoch':TARGET-60,'model_id':f['model_id'],'predicted_signed_pips':str(pips)}],policy=native.make_policy(native_horizons_sec=[TARGET-epoch],maximum_reference_age_sec=2,maximum_build_sec=2,maximum_issue_delay_sec=1,maximum_publication_delay_sec=1,maximum_decision_age_sec=2,minimum_remaining_sec=0),computation_sha256=fingerprint(revised),scope='engineering_replay',input_context={'input_tier':'joint_ready_original_remaining_records.v1','conditioning_kind':'fresh_original_features_direct_remaining_horizon_model','conditioning_epoch':epoch,'observed_publication':False})
        native.validate_prepared(curve,expected_source_bindings=bindings)
        packet={'schema_version':'forex_joint_ready_remaining_prepared.v1','method':row['method'],'prepared_curve':curve,'forecast':revised,'source_bindings':bindings,'reference_point':ref,'original_target_epoch':TARGET,'conditioning_epoch':epoch,'observed_publication':False,'observed_execution':False};packet['packet_sha256']=fingerprint(packet);packets.append(packet);new.append(revised)
    return {'epoch':epoch,'original_target_epoch':TARGET,'forecasts':new,'native_packets':packets,'coverage':coverage}
