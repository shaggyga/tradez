"""All declared scopes; preserve missing support, frozen and expanding diagnostics."""
from collections import Counter,defaultdict
import hashlib,json,os
from pathlib import Path
from contracts import fingerprint
from publication import RunPublisher,effective_run_identity,verify_completed_run
from prequential_residual_calibration_v2 import contract,fit_snapshot,apply_snapshot,score_rows
GROUPS=('legacy26','compact38_cost2','compact50_cost2','full228_cost2');HORIZONS=(15,60,240,720,1440,2880,7200);PROCEDURES=('frozen','adaptive');METHODS=('ridge','recovered_hgb');ORIGINS=tuple(range(1721606460,1722038400,21600))
def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def chunk(g,h,p):return f'{g}_{h}_{p}.json'
def required():return sorted([prefix+chunk(g,h,p) for g in GROUPS for h in HORIZONS for p in PROCEDURES for prefix in ('calibration_','coverage_')]+['calibration_contract.json','scores.json','run_report.json','source_references.json'])
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})
def consumed(root,dependency,name):
    if Path(name).name!=name or name not in dependency['payloads']:raise ValueError('registered_calibration_payload_required')
    raw=(root/name).read_bytes()
    if len(raw)>64*1024*1024 or hashlib.sha256(raw).hexdigest()!=dependency['payloads'][name]:raise ValueError('calibration_consumed_bytes_changed')
    return json.loads(raw)
def outcome_index(paths,r):
    outcomes={};root=Path(paths['technical'])
    for pair in r['universe']:
        part=consumed(root,r['dependencies']['technical'],'pair_'+pair+'.json')
        for o in part['outcomes']:
            key=(o['record_id'],o['target_id'])
            if key in outcomes:raise ValueError('unique_original_calibration_outcomes_required')
            outcomes[key]=o
    return outcomes

def build_chunk(g,h,procedure,forecasts,coverage,outcomes,c):
    target=f'technical_endpoint_midpoint_elapsed_{h}m';rows=[];snapshots=[];status_by={};scores=[]
    for method in METHODS:
        source=[x for x in forecasts if x['method']==method];scope=[g,target,procedure,method];by_origin=defaultdict(list)
        for row in source:by_origin[row['forecast']['decision_epoch']].append(row)
        frozen=fit_snapshot(source,outcomes,c['frozen_cutoff'],scope,c);snapshots.append({'mode':'frozen_prefix','snapshot':frozen})
        for origin in ORIGINS:
            expanding=fit_snapshot(source,outcomes,origin,scope,c);snapshots.append({'mode':'expanding_prefix','snapshot':expanding})
            for mode,snapshot in [('frozen_prefix',frozen),('expanding_prefix',expanding)]:
                status='calibration_phase_not_started' if mode=='frozen_prefix' and origin<c['frozen_cutoff'] else snapshot['status']
                status_by[method,origin,mode]=status
                for row in by_origin[origin]:
                    if status=='calibration_phase_not_started':
                        f=row['forecast'];rows.append({'record_id':row['record_id'],'instrument':f['instrument'],'origin_epoch':origin,'base_forecast_id':f['forecast_id'],'base_model_id':f['model_id'],'base_forecast_available_epoch':f['available_epoch'],'base_prediction_bps':f['prediction'],'mode':mode,'calibrator_id':None,'calibration_cutoff':None,'status':status,'adjusted_mean_bps':None,'empirical_lower_bps':None,'empirical_upper_bps':None,'production_available_epoch':None,'scope':'offline_modeled_clock_residual_diagnostic'})
                    else:rows.append(apply_snapshot(row,snapshot,mode,c))
        source_ids={v['forecast']['forecast_id'] for v in source}
        for mode in c['modes']:
            selected=[x for x in rows if x['mode']==mode and x['base_forecast_id'] in source_ids]
            scores.append({'group':g,'target_id':target,'base_procedure':procedure,'method':method,'calibration_mode':mode,**score_rows(selected,outcomes,target,c)})
    cov=[]
    for row in coverage:
        for mode in c['modes']:
            status=status_by[row['method'],row['decision_epoch'],mode] if row['reason']=='eligible' else 'base_'+row['reason']
            cov.append({**row,'calibration_mode':mode,'calibration_status':status})
    rows.sort(key=lambda x:(x['origin_epoch'],x['instrument'],x['base_model_id'],x['mode']))
    return {'group':g,'target_id':target,'base_procedure':procedure,'snapshots':snapshots,'rows':rows,'scores':scores,'all_attempts_retained':True},cov

def run(paths,r,runs,*,resume=False,crash_after=None):
    i=identity_for(r);p=RunPublisher(runs,r['run_id'],i)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,i);return
    p.acquire(recover=resume)
    try:
        for key,value in paths.items():verify_completed_run(Path(value),r['dependencies'][key]['identity'])
        c=contract();outcomes=outcome_index(paths,r);payloads=[p.write_or_validate_payload('calibration_contract.json',encoded(c))];scores=[];counts=Counter();support=Counter();index=0
        for g in GROUPS:
            for h in HORIZONS:
                for procedure in PROCEDURES:
                    name=chunk(g,h,procedure);a=p.read_verified_payload('calibration_'+name);b=p.read_verified_payload('coverage_'+name)
                    if a is None or b is None:
                        forecasts=consumed(Path(paths['joint']),r['dependencies']['joint'],'forecasts_'+name);coverage=consumed(Path(paths['joint']),r['dependencies']['joint'],'coverage_'+name)
                        data,cov=build_chunk(g,h,procedure,forecasts,coverage,outcomes,c);a=encoded(data);b=encoded(cov)
                    data=json.loads(a);cov=json.loads(b);scores.extend(data['scores']);counts['base_forecast_rows']+=len(data['rows'])//2;counts['diagnostic_records']+=len(data['rows']);counts['coverage_rows']+=len(cov)
                    for item in data['snapshots']:counts['snapshot_'+item['snapshot']['status']]+=1
                    for row in cov:support[row['calibration_status']]+=1
                    payloads.extend([p.write_or_validate_payload('calibration_'+name,a),p.write_or_validate_payload('coverage_'+name,b)]);index+=1
                    if crash_after==index:os._exit(91)
        report={'status':'completed_modeled_clock_residual_calibration','base_forecast_rows':counts['base_forecast_rows'],'diagnostic_records':counts['diagnostic_records'],'coverage_rows':counts['coverage_rows'],'score_groups':len(scores),'snapshot_attempts':{k.removeprefix('snapshot_'):v for k,v in counts.items() if k.startswith('snapshot_')},'coverage_statuses':dict(sorted(support.items())),'base_models_fitted':0,'calibration_principle':'origin-balanced_mean_residual_and_weighted_empirical_10_90_percentiles','calibration_update_procedures':c['modes'],'all_fixed_attempts_retained':True,'selected_model':None,'engineering_ready':False,'forecast_evidence_status':'modeled_clock_prequential_development_diagnostic','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,'actual_original_issue_receipts_qualified':False,'next_item':'calibration_original_record_inspector_v2','limitations':['assumed_label_availability_and_modeled_forecast_publication','feature_family_chosen_on_inspected_development','overlapping_targets_shared_currency_dependence','empirical_residual_interval_not_distribution_free_or_conditional_guarantee','no_event_probability_conversion','no_production_calibrator_latency_or_policy_admission']}
        refs={'dependencies':r['dependencies'],'preserved_original_records':True,'base_models_refitted':False,'input_tier':r['input_tier']}
        for name,obj in [('scores.json',scores),('run_report.json',report),('source_references.json',refs)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
