"""Durable fixed-interaction chunks and a separate synthetic positive fixture."""
import hashlib,json,os,time
from publication import RunPublisher,effective_run_identity,verify_completed_run
from interaction_models_v2 import GROUPS,METHODS,contract,fit_pair,validate_fit,predict,score_chunk

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def fit_name(g,h,t):return f'fit_{g}_{h}_{t}'
def chunk_name(g,h,p):return f'{g}_{h}_{p}'
def required():
    c=contract();names=[]
    for g in GROUPS:
        for h in c['horizon_minutes']:
            for t in c['fit_cutoffs']:names.extend(fit_name(g,h,t)+s for s in ('.json','_ridge.joblib','_recovered_hgb.joblib'))
            for p in c['procedures']:names.extend(prefix+chunk_name(g,h,p)+'.json' for prefix in ('forecasts_','coverage_'))
    return sorted(names+['scores.json','model_inventory.json','baseline_reference.json','run_report.json','fit_resources.json','experiment_contract.json','interaction_positive_fixture.json'])
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required()},dependency_hashes={**r['sources'],**r['predecessors'],'rich':r['rich']['identity']['fingerprint'],'baseline':r['baseline']['identity']['fingerprint']})
def checked(root,dependency,name):
    p=root/name
    if p.stat().st_size>64*1024*1024:raise ValueError('bounded_rich_dependency_reader_required')
    raw=p.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=dependency['payloads'][name]:raise ValueError('rich_family_consumed_dependency_changed')
    return json.loads(raw)
def load_inputs(root,r):
    from interaction_features_v2 import views_for
    verify_completed_run(root,r['rich']['identity']);observations=[];outcomes=[];views={}
    names=r['legacy_features']
    for pair in r['universe']:
        part=checked(root,r['rich'],'pair_'+pair+'.json')
        for record in part['observations']:
            observations.append(record['original_legacy_observation']);views[record['record_id']]=views_for(record,names)
        outcomes.extend(checked(root,r['rich'],'outcomes_'+pair+'.json')['outcomes'])
    return observations,outcomes,views

def run(rich,baseline,r,runs,*,resume=False,crash_after=None):
    c=contract();identity=identity_for(r);p=RunPublisher(runs,r['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        payloads=[p.write_or_validate_payload('experiment_contract.json',encoded({**c,'ordered_feature_sets':r['feature_sets']}))]
        obs,outcomes,views=load_inputs(rich,r);base_scores=checked(baseline,r['baseline'],'scores.json');fits={};index=0
        for g in GROUPS:
            for h in c['horizon_minutes']:
                for cutoff in c['fit_cutoffs']:
                    name=fit_name(g,h,cutoff);raw=p.read_verified_payload(name+'.json');models={m:p.read_verified_payload(name+'_'+m+'.joblib') for m in METHODS}
                    timings=[json.loads(x) for x in (p.root/'FIT_TIMINGS.jsonl').read_text(encoding='utf-8').splitlines()] if (p.root/'FIT_TIMINGS.jsonl').exists() else []
                    if raw is None or any(b is None for b in models.values()) or not any(x['fit_id']==json.loads(raw)['fit_id'] for x in timings):
                        base=checked(baseline,r['baseline'],f'fit_{h}_{cutoff}.json');start=time.monotonic()
                        meta,models=fit_pair(obs,outcomes,views,g,r['feature_sets'][g],h,cutoff,base,c);elapsed=time.monotonic()-start
                        if elapsed>c['fit_latency_seconds']:raise ValueError('declared_rich_fit_latency_exceeded')
                        raw=encoded(meta)
                        with (p.root/'FIT_TIMINGS.jsonl').open('ab') as log:
                            log.write(encoded({'fit_id':meta['fit_id'],'elapsed_seconds':elapsed,'limit_seconds':30,'scope':'joint_ridge_hgb_train_transform_serialize'}));log.flush();os.fsync(log.fileno())
                    meta=json.loads(raw);validate_fit(meta,models)
                    payloads.append(p.write_or_validate_payload(name+'.json',raw))
                    for method,b in models.items():payloads.append(p.write_or_validate_payload(name+'_'+method+'.joblib',b))
                    fits[(g,h,cutoff)]=(meta,models);index+=1
                    if crash_after==index:os._exit(91)
        scores=[];forecast_count=coverage_count=0
        for g in GROUPS:
            for h in c['horizon_minutes']:
                for procedure in c['procedures']:
                    forecasts=[];coverage=[]
                    for cutoff in c['fit_cutoffs']:
                        if procedure=='frozen' and cutoff!=c['fit_cutoffs'][0]:continue
                        current=[]
                        for o in obs:
                            t=o['origin_epoch']
                            if t not in c['decision_epochs']:continue
                            ready=[cut for cut in c['fit_cutoffs'] if cut+c['fit_latency_seconds']<=t]
                            chosen=max(ready) if procedure=='adaptive' else c['fit_cutoffs'][0]
                            if chosen==cutoff:current.append(o)
                        start=time.monotonic();f,cov=predict(*fits[(g,h,cutoff)],current,views,procedure=procedure,c=c)
                        if time.monotonic()-start>c['prediction_latency_seconds']:raise ValueError('declared_rich_prediction_latency_exceeded')
                        forecasts.extend(f);coverage.extend(cov)
                    forecasts.sort(key=lambda x:(x['forecast']['decision_epoch'],x['forecast']['instrument'],x['method']))
                    coverage.sort(key=lambda x:(x['decision_epoch'],x['instrument'],x['method']))
                    scores.extend(score_chunk(forecasts,outcomes,base_scores,c));forecast_count+=len(forecasts);coverage_count+=len(coverage)
                    for prefix,obj in [('forecasts_',forecasts),('coverage_',coverage)]:payloads.append(p.write_or_validate_payload(prefix+chunk_name(g,h,procedure)+'.json',encoded(obj)))
        resources=[json.loads(x) for x in (p.root/'FIT_TIMINGS.jsonl').read_text(encoding='utf-8').splitlines()]
        by_id={x['fit_id']:x for x in resources}
        if set(by_id)!={meta['fit_id'] for meta,models in fits.values()} or any(not 0<=x['elapsed_seconds']<=30 for x in resources):raise ValueError('rich_fit_resource_evidence_missing')
        resources=[by_id[meta['fit_id']] for meta,models in fits.values()]
        reference={'identity':r['baseline']['identity']['fingerprint'],'payloads':r['baseline']['payloads'],'models_reused':28,'forecast_rows_reused':75488,'scores':base_scores,'baseline_refitted':False}
        report={'status':'completed_fixed_interaction_diagnostics','instruments':68,'models_fitted':32,'market_models_fitted':28,'synthetic_models_fitted':4,'models_reused':28,'groups':{g:len(r['feature_sets'][g]) for g in GROUPS},'new_score_groups':len(scores),'baseline_score_groups':len(base_scores),'new_forecast_rows':forecast_count,'new_coverage_rows':coverage_count,'scoring_support_matched':True,'selected_model':None,'engineering_ready':False,'forecast_evidence_status':'matched_inspected_development_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,'next_item':c['next_item'],
            'limitations':['already_inspected_development_dates','shared_legacy_eligible_population_only','elapsed_targets_not_sessions','no_policy_or_confirmation_claim','fixed_hyperparameters_no_winner_selection','ten_predeclared_products_no_screen_or_search','synthetic_interaction_fixture_is_not_market_confirmation']}
        if len(scores)!=28 or forecast_count!=37744 or coverage_count!=38080:raise ValueError('declared_rich_population_incomplete')
        saved_fixture=p.read_verified_payload('interaction_positive_fixture.json')
        saved_resources=p.read_verified_payload('fit_resources.json')
        if saved_fixture is not None and saved_resources is not None:
            fixture=json.loads(saved_fixture);prior=json.loads(saved_resources)
            if len(prior)!=15 or prior[:-1]!=resources:raise ValueError('interaction_saved_resources_disagree')
            timing=prior[-1]
            if timing['fit_id']!=fixture['interaction_fit_id'] or timing['scope']!='synthetic_baseline_and_interaction_four_learner_fits' or timing['limit_seconds']!=30 or not 0<=timing['elapsed_seconds']<=30:raise ValueError('interaction_saved_fixture_timing_invalid')
        else:
            from interaction_positive_fixture_v2 import build as positive_fixture
            start=time.monotonic();fixture=positive_fixture();fixture_elapsed=time.monotonic()-start
            if fixture_elapsed>30:raise ValueError('interaction_fixture_resource_limit')
            timing={'fit_id':fixture['interaction_fit_id'],'elapsed_seconds':fixture_elapsed,'limit_seconds':30,'scope':'synthetic_baseline_and_interaction_four_learner_fits'}
        resources.append(timing)
        payloads.append(p.write_or_validate_payload('interaction_positive_fixture.json',encoded(fixture)))
        for name,obj in [('scores.json',scores),('model_inventory.json',[meta for meta,models in fits.values()]),('baseline_reference.json',reference),('run_report.json',report),('fit_resources.json',resources)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
