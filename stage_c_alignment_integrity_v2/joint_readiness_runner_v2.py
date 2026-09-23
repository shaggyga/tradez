"""Durable joint-readiness projection from preserved artifacts and original records."""
from collections import Counter,defaultdict
import json,math,os,time
from contracts import fingerprint,validate_forecast
from publication import RunPublisher,verify_completed_run,effective_run_identity
from campaign_inspector_v2 import CampaignReader
from rich_family_runner_v2 import load_inputs
from joint_readiness_schedule_v2 import schedule,select_fit,visible_forecast,GROUPS,METHODS,PROCEDURES,HORIZONS
from joint_readiness_predict_v2 import RetainedPredictor

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def chunk(g,h,p):return f'{g}_{h}_{p}.json'
def required():return sorted([prefix+chunk(g,h,p) for g in GROUPS for h in HORIZONS for p in PROCEDURES for prefix in ('forecasts_','coverage_')]+['schedule.json','scores.json','run_report.json','prediction_resources.json','source_references.json'])
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**r['predecessors'],**{k:v['identity']['fingerprint'] for k,v in r['dependencies'].items()}})

def source_rows(reader,g,h,procedure):
    target=f'technical_endpoint_midpoint_elapsed_{h}m'
    alias,name=('baseline','forecasts.json') if g=='legacy26' else ('family','forecasts_'+chunk(g,h,procedure))
    rows=[x for x in reader.read(alias,name) if x['procedure']==procedure and x['forecast']['target_id']==target and x['method'] in METHODS]
    result={(x['record_id'],x['method']):x for x in rows}
    if len(result)!=len(rows):raise ValueError('unique_scheduled_source_forecasts_required')
    return result,alias,name

def project_row(source,task,slot,policy_sha,procedure,alias,name):
    f=source['forecast'];model_id=fingerprint({'fit_id':task['fit_id'],'method':source['method']})
    if f['model_id']!=model_id or f['decision_epoch']!=slot['origin_epoch']:raise ValueError('selected_original_forecast_identity_mismatch')
    original_sha=fingerprint(source)
    result={**f,'forecast_id':fingerprint({'original_record_sha256':original_sha,'policy_sha256':policy_sha,'procedure':procedure,'joint_ready_epoch':task['joint_ready_epoch'],'available_epoch':slot['end_epoch']}),
        'model_ready_epoch':task['joint_ready_epoch'],'available_epoch':slot['end_epoch']}
    validate_forecast(result)
    return {'record_id':source['record_id'],'group':task['group'],'method':source['method'],'procedure':procedure,'forecast':result,
        'source_reference':{'dependency':alias,'payload':name,'original_record_sha256':original_sha,'original_forecast_id':f['forecast_id'],'source_procedure':source['procedure']},
        'reservation_policy_sha256':policy_sha,'selected_fit_id':task['fit_id'],'selected_fit_cutoff':task['fit_cutoff'],'original_per_fit_ready_epoch':f['model_ready_epoch']}

def score_rows(rows,outcomes,original,asof):
    grouped=defaultdict(list)
    for row in rows:
        f=row['forecast'];o=outcomes.get((row['record_id'],f['target_id']))
        if o is None or o['value'] is None or o['available_epoch']>asof:continue
        if f['available_epoch']>asof or o['available_epoch']<o['label_end_epoch']:raise ValueError('scheduled_score_clock_invalid')
        old=original[row['record_id'],row['method']]['forecast'];y=o['value'];grouped[row['method']].append((row['record_id'],f['prediction'],y,old['prediction']))
    result=[]
    for method,values in sorted(grouped.items()):
        n=len(values);result.append({'method':method,'rows':n,'support_sha256':fingerprint(sorted(x[0] for x in values)),
            'mae_bps':math.fsum(abs(p-y) for _,p,y,_ in values)/n,'mse_bps2':math.fsum((p-y)**2 for _,p,y,_ in values)/n,
            'paired_mae_delta_vs_zero':math.fsum(abs(p-y)-abs(y) for _,p,y,_ in values)/n,
            'paired_mse_delta_vs_zero':math.fsum((p-y)**2-y*y for _,p,y,_ in values)/n,
            'paired_mae_delta_vs_original_procedure':math.fsum(abs(p-y)-abs(old-y) for _,p,y,old in values)/n,
            'paired_mse_delta_vs_original_procedure':math.fsum((p-y)**2-(old-y)**2 for _,p,y,old in values)/n,
            'comparison_scope':'same_actual_projected_support; original_procedure_is_per_fit_counterfactual_not_jointly_executable'})
    if len(result)!=2:raise ValueError('both_projected_methods_require_mature_score_support')
    return result

def run(paths,recipe,runs,*,resume=False,crash_after=None):
    identity=identity_for(recipe);p=RunPublisher(runs,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        reader=CampaignReader(paths,recipe['dependencies']);c=reader.read('family','experiment_contract.json')
        fits=[]
        for alias in ('baseline','family'):
            for m in reader.read(alias,'model_inventory.json'):
                fits.append({'group':m.get('group','legacy26'),'horizon_minutes':m['target']['horizon_seconds']//60,'fit_cutoff':m['fit_cutoff'],
                    'fit_id':m['fit_id'],'maximum_outcome_available_epoch':m['maximum_outcome_available_epoch']})
        sched=schedule(fits,c['decision_epochs'],c['fit_cutoffs']);payloads=[p.write_or_validate_payload('schedule.json',encoded(sched))]
        family_recipe=recipe['dependencies']['family']['identity']['contract']['recipe']
        observations,labels,views=load_inputs(reader.paths['rich'],family_recipe)
        by_origin=defaultdict(list)
        for o in observations:
            if o['origin_epoch'] in c['decision_epochs']:by_origin[o['origin_epoch']].append(o)
        for obs in by_origin.values():obs.sort(key=lambda x:x['instrument'])
        if any([o['instrument'] for o in obs]!=recipe['configuration']['universe'] for obs in by_origin.values()):raise ValueError('original_all68_observations_required')
        outcomes={(o['record_id'],o['target_id']):o for o in labels}
        if len(outcomes)!=len(labels):raise ValueError('unique_original_outcomes_required')
        predictor=RetainedPredictor(reader);slots={(x['group'],x['horizon_minutes'],x['procedure'],x['origin_epoch']):x for x in sched['prediction_tasks']}
        timing_path=p.root/'PREDICTION_TIMINGS.jsonl';timings=[json.loads(x) for x in timing_path.read_text(encoding='utf-8').splitlines()] if timing_path.exists() else []
        timing_map={x['batch_id']:x for x in timings};scores=[];counts=Counter();index=0
        for g in GROUPS:
            for h in HORIZONS:
                for procedure in PROCEDURES:
                    name=chunk(g,h,procedure);cached_f=p.read_verified_payload('forecasts_'+name);cached_c=p.read_verified_payload('coverage_'+name)
                    batch_ids=[fingerprint(slots[g,h,procedure,t]) for t in c['decision_epochs']]
                    if cached_f is not None and cached_c is not None and all(k in timing_map for k in batch_ids):
                        forecasts=json.loads(cached_f);coverage=json.loads(cached_c)
                    else:
                        forecasts=[];coverage=[]
                        for origin in c['decision_epochs']:
                            slot=slots[g,h,procedure,origin];task=select_fit(sched,g,h,procedure,origin);batch_id=fingerprint(slot)
                            obs=by_origin[origin];rows={};alias=source_name=None
                            latest_due=max(t for t in c['fit_cutoffs'] if t<=origin)
                            fallback=task is not None and procedure=='adaptive' and task['fit_cutoff']<latest_due
                            if task is not None:
                                source_procedure='frozen' if task['fit_cutoff']==c['fit_cutoffs'][0] else 'adaptive'
                                rows,alias,source_name=source_rows(reader,g,h,source_procedure)
                            start=time.monotonic();actual=predictor.predict(task,obs,views) if task is not None else {}
                            for o in obs:
                                feature_reason='missing_features' if o['features'] is None else 'feature_not_ready' if o['available_epoch']>origin else 'eligible'
                                reason='joint_model_not_ready' if task is None else feature_reason
                                for method in METHODS:
                                    cov={'record_id':o['record_id'],'instrument':o['instrument'],'decision_epoch':origin,'target_id':f'technical_endpoint_midpoint_elapsed_{h}m',
                                        'group':g,'method':method,'procedure':procedure,'reason':reason,'feature_reason':feature_reason,'prior_ready_artifact_fallback':fallback,
                                        'selected_fit_cutoff':task['fit_cutoff'] if task else None,'joint_model_ready_epoch':task['joint_ready_epoch'] if task else None,'reserved_publication_epoch':slot['end_epoch']}
                                    coverage.append(cov)
                                    if reason!='eligible':continue
                                    source=rows.get((o['record_id'],method))
                                    if source is None or (o['record_id'],method) not in actual:raise ValueError('original_selected_forecast_or_prediction_missing')
                                    if not math.isclose(actual[o['record_id'],method],source['forecast']['prediction'],rel_tol=1e-12,abs_tol=1e-10):raise ValueError('retained_weight_prediction_parity_failed')
                                    forecasts.append(project_row(source,task,slot,sched['policy_sha256'],procedure,alias,source_name))
                            elapsed=time.monotonic()-start
                            if task is not None and elapsed>2:raise ValueError('joint_prediction_reservation_exceeded')
                            timing={'batch_id':batch_id,'group':g,'horizon_minutes':h,'procedure':procedure,'origin_epoch':origin,'selected_fit_id':task['fit_id'] if task else None,
                                'status':'measured_retained_weight_prediction' if task else 'not_run_no_ready_model','elapsed_seconds':elapsed if task else None,'limit_seconds':2,'forecast_values_checked':len(actual),
                                'scope':'model_read_verify_load_if_not_cached_transform_predict_lineage_validate; prepared_features_and_original_source_index_preloaded'}
                            with timing_path.open('ab') as f:f.write(encoded(timing));f.flush();os.fsync(f.fileno())
                            timing_map[batch_id]=timing
                        forecasts.sort(key=lambda x:(x['forecast']['decision_epoch'],x['forecast']['instrument'],x['method']))
                        coverage.sort(key=lambda x:(x['decision_epoch'],x['instrument'],x['method']))
                        cached_f=encoded(forecasts);cached_c=encoded(coverage)
                    payloads.extend([p.write_or_validate_payload('forecasts_'+name,cached_f),p.write_or_validate_payload('coverage_'+name,cached_c)])
                    original,_,_=source_rows(reader,g,h,procedure)
                    scores.extend({'group':g,'target_id':f'technical_endpoint_midpoint_elapsed_{h}m','procedure':procedure,**x} for x in score_rows(forecasts,outcomes,original,c['evaluation_asof']))
                    counts['forecast_rows']+=len(forecasts);counts['coverage_rows']+=len(coverage)
                    for row in coverage:
                        counts[row['reason']]+=1
                        if row['prior_ready_artifact_fallback'] and row['reason']=='eligible':counts['fallback_forecast_rows']+=1
                    for row in forecasts:counts['native_2s_'+visible_forecast(row,row['forecast']['available_epoch'],maximum_conditioning_age_seconds=2)['status']]+=1
                    index+=1
                    if crash_after==index:os._exit(91)
        resources=[timing_map[fingerprint(x)] for x in sched['prediction_tasks']]
        if len(resources)!=1120 or len(scores)!=112 or counts['coverage_rows']!=152320:raise ValueError('complete_joint_scope_required')
        report={'status':'verified_joint_readiness_projection','schedule_id':sched['schedule_id'],'reservation_policy_sha256':sched['policy_sha256'],'counts':dict(sorted(counts.items())),
            'fit_pairs_reused':56,'model_artifacts_reused':112,'models_fitted':0,'prediction_slots':1120,'score_groups':len(scores),
            'full_fit_reservation_seconds_per_cutoff':[{ 'fit_cutoff':t,'seconds':max(x['joint_ready_epoch'] for x in sched['fit_tasks'] if x['fit_cutoff']==t)-t} for t in c['fit_cutoffs']],
            'readiness_scope':'deterministic_single_worker_reservations_with_measured_prediction_batches; not_live_deadline_guarantee',
            'native_policy_scope':'two_second_conditioning_age_enforced; no_native_curve_or_policy_admission_claim','selected_model':None,'engineering_ready':False,
            'forecast_evidence_status':'scheduled_inspected_development_projection','policy_evidence_status':'not_evaluated_freshness_refusals_explicit','demo_authorization_status':'not_granted','independent_review':False,
            'limitations':['prepared_inputs_assumed_available','full_ingestion_feature_startup_and_OS_load_not_timed','short_dependent_inspected_development','later_research_feature_schema_selection','original_per_fit_experiment_remains_unchanged','not_confirmation']}
        refs={'dependencies':{k:{'run_identity':v['identity']['fingerprint'],'payloads':v['payloads']} for k,v in recipe['dependencies'].items()},'original_experiments_unchanged':True,'timing_receipts_excluded_from_scientific_byte_equality':True}
        for name,obj in [('scores.json',scores),('run_report.json',report),('prediction_resources.json',resources),('source_references.json',refs)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
