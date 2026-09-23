"""All scheduled origins, unchanged retained sampled-close labels and explicit maturity."""
from collections import Counter
import math
from causal_technical_adapter_v2 import clean,contract as original_contract
from retained_rolling_path_labels_v1 import compute_outcomes,FIELDS,NUMERIC_OUTCOMES,SCHEMA_VERSION

def contract():
    old=original_contract()
    return {'schema_version':'forex_sampled_path_reconciliation.v1','origin_start':old['origin_start'],'origin_end':old['origin_end'],'cadence_seconds':old['cadence_seconds'],'coverage_end_epoch':old['data_end'],'horizon_minutes':old['endpoint_minutes'],'canonical_schema':SCHEMA_VERSION,
        'convention':'retained_signed_future_close_extrema_excluding_origin_no_zero_clipping_bps_of_origin_mid','availability':'target_bar_start_plus60_assumption_not_observed_receipt','path':'exact_contiguous_minutes_no_interpolation','execution':'bidask_close_proxy_no_fills_latency_slippage_financing_commission','first_passage':'unavailable_intrabar_order_and_continuous_extrema_not_identified','fit_allowed':False,'broker_access':False,'next_item':'joint_curve_serving_capacity_characterization_v2'}

def native(x):
    if hasattr(x,'item'):x=x.item()
    if isinstance(x,float) and not math.isfinite(x):return None
    return x

def pair_records(pair,raw,original,source_sha,c=None):
    c=c or contract();frame,quality=clean(raw)
    data={'time':frame.epoch.to_numpy(),'close':frame.mid.to_numpy(),'bid_close':frame.bid.to_numpy(),'ask_close':frame.ask.to_numpy()}
    result=compute_outcomes(data,c['horizon_minutes'],coverage_end_epoch=c['coverage_end_epoch']) if len(frame) else {}
    positions={int(t):i for i,t in enumerate(frame.epoch)};old={ (x['record_id'],x['target_id']):x for x in original['outcomes']};old_path={(x['record_id'],x['target_id']):x for x in original['path_outcomes']}
    rows=[];matched=0;path_matches=0
    for origin in range(c['origin_start'],c['origin_end'],c['cadence_seconds']):
        key=f'{pair}:{origin}';position=positions.get(origin-60)
        for h in c['horizon_minutes']:
            if position is None:
                values={n:None for n in FIELDS};values.update(target_bar_start_epoch=origin+(h-1)*60,target_bar_end_epoch=origin+h*60,assumed_available_epoch=origin+h*60,state='missing_exact_reference',path_expected_bars=h+1,midpoint_valid=False,bidask_endpoint_valid=False,bidask_excursion_valid=False)
            else:values={n:native(result[f'label__{h}m__{n}'][position]) for n in FIELDS}
            previous=old[(key,f'technical_endpoint_midpoint_elapsed_{h}m')]
            equal=None
            if values['midpoint_valid']:
                equal=values['return_bps']==previous['value']
                if not equal:raise ValueError('sampled_path_original_endpoint_mismatch')
                matched+=1
            path=old_path.get((key,f'technical_contiguous_midpoint_elapsed_{h}m'))
            if path is not None:
                if values['return_bps']!=path['value']:raise ValueError('sampled_path_original_contiguous_endpoint_mismatch')
                path_matches+=1
            rows.append({'record_id':key,'instrument':pair,'origin_epoch':origin,'horizon_minutes':h,'target_id':f'retained_signed_bidask_close_path_elapsed_{h}m','source_member_sha256':source_sha,'values':values,'original_endpoint_value':previous['value'],'shared_support_exact_match':equal,'is_achieved_return':False})
    return {'instrument':pair,'quality':quality,'rows':rows,'shared_endpoint_values_exact':matched,'original_contiguous_values_exact':path_matches,'models_fitted':0}

def asof(record,epoch):
    if isinstance(epoch,bool) or not isinstance(epoch,int) or epoch<record['origin_epoch']:raise ValueError('integer_asof_at_or_after_origin_required')
    available=record['values']['assumed_available_epoch']
    if epoch<available:return {'record_id':record['record_id'],'target_id':record['target_id'],'available_epoch':available,'status':'pending_maturity','values':None}
    return {'record_id':record['record_id'],'target_id':record['target_id'],'available_epoch':available,'status':record['values']['state'],'values':dict(record['values'])}

def summarize(parts,c):
    rows=[r for p in parts for r in p['rows']];support={}
    for h in c['horizon_minutes']:
        selected=[r for r in rows if r['horizon_minutes']==h]
        support[str(h)]={'rows':len(selected),'states':dict(Counter(r['values']['state'] for r in selected)),'midpoint_valid':sum(r['values']['midpoint_valid'] for r in selected),'bidask_endpoint_valid':sum(r['values']['bidask_endpoint_valid'] for r in selected),'bidask_excursion_valid':sum(r['values']['bidask_excursion_valid'] for r in selected),'original_endpoint_available':sum(r['original_endpoint_value'] is not None for r in selected),'shared_endpoint_exact':sum(r['shared_support_exact_match'] is True for r in selected)}
    return {'status':'completed_sampled_path_reconciliation','instruments':len(parts),'scheduled_origins_per_pair':len(range(c['origin_start'],c['origin_end'],c['cadence_seconds'])),'rows':len(rows),'target_support':support,'shared_endpoint_values_exact':sum(p['shared_endpoint_values_exact'] for p in parts),'original_contiguous_values_exact':sum(p['original_contiguous_values_exact'] for p in parts),'models_fitted':0,'engineering_ready':False,'forecast_evidence_status':'retrospective_target_reconciliation_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,'next_item':c['next_item'],'limitations':['assumed_close_availability_not_observed_receipt','future_close_samples_not_continuous_extrema_or_intrabar_barriers','quoted_spread_only_not_achieved_profit','elapsed_targets_not_qualified_sessions','inspected_development_data_no_model_fit_or_confirmation']}
