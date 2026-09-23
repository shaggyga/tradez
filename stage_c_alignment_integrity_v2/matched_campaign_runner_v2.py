"""Resumable matched model campaign, original forecasts separate from scoring."""
import json,os,time,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from publication import RunPublisher,effective_run_identity,verify_completed_run
from matched_campaign_models_v2 import contract,fit_pair,predict,score,validate_fit

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def fit_names(c):return [f'fit_{h}_{t}' for h in c['horizon_minutes'] for t in c['fit_cutoffs']]
def required(c):return sorted([n+s for n in fit_names(c) for s in ('.json','.joblib')]+['forecasts.json','coverage.json','scores.json','run_report.json','model_inventory.json','fit_resources.json'])
def identity_for(recipe):return effective_run_identity(contract={'recipe':recipe,'required_payloads':required(contract())},dependency_hashes={**recipe['sources'],'prepared_input_identity':recipe['input_identity']['fingerprint']})
def load_inputs(root,recipe):
    verify_completed_run(root,recipe['input_identity']);parts=[json.loads((root/('pair_'+p+'.json')).read_text()) for p in recipe['universe']]
    return [o for p in parts for o in p['observations']],[o for p in parts for o in p['outcomes']]
def run(root,recipe,*,runs_dir,resume=False,crash_after=None):
    c=contract();identity=identity_for(recipe);p=RunPublisher(runs_dir,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    observations,outcomes=load_inputs(root,recipe);p.acquire(recover=resume)
    try:
        payloads=[];fits={};index=0
        for h in c['horizon_minutes']:
            for cutoff in c['fit_cutoffs']:
                name=f'fit_{h}_{cutoff}';raw=p.read_verified_payload(name+'.json');tree=p.read_verified_payload(name+'.joblib');timings=[json.loads(line) for line in (p.root/'FIT_TIMINGS.jsonl').read_text().splitlines()] if (p.root/'FIT_TIMINGS.jsonl').exists() else []
                if raw is None or tree is None or not any(t['fit_id']==json.loads(raw)['fit_id'] for t in timings):
                    start=time.monotonic();meta,tree=fit_pair(observations,outcomes,recipe['universe'],h,cutoff,c);elapsed=time.monotonic()-start
                    if elapsed>c['fit_latency_seconds']:raise ValueError('declared_fit_latency_exceeded')
                    raw=encoded(meta)
                    with (p.root/'FIT_TIMINGS.jsonl').open('ab') as log:
                        log.write(encoded({'fit_id':meta['fit_id'],'elapsed_seconds':elapsed,'limit_seconds':c['fit_latency_seconds'],'scope':'joint_ridge_tree_fit_with_publication_preparation'}));log.flush();os.fsync(log.fileno())
                meta=json.loads(raw);validate_fit(meta,tree)
                for suffix,b in [('.json',raw),('.joblib',tree)]:payloads.append(p.write_or_validate_payload(name+suffix,b))
                fits[(h,cutoff)]=(meta,tree);index+=1
                if crash_after==index:os._exit(91)
        forecasts=[];coverage=[]
        for h in c['horizon_minutes']:
            for procedure in c['procedures']:
                for cutoff in c['fit_cutoffs']:
                    if procedure=='frozen' and cutoff!=c['fit_cutoffs'][0]:continue
                    current=[]
                    for o in observations:
                        t=o['origin_epoch']
                        if t not in c['decision_epochs']:continue
                        ready=[cut for cut in c['fit_cutoffs'] if cut+c['fit_latency_seconds']<=t]
                        chosen=max(ready) if procedure=='adaptive' else c['fit_cutoffs'][0]
                        if chosen==cutoff:current.append(o)
                    f,cov=predict(*fits[(h,cutoff)],current,procedure=procedure,c=c);forecasts.extend(f);coverage.extend(cov)
        forecasts.sort(key=lambda r:(r['forecast']['decision_epoch'],r['forecast']['instrument'],r['forecast']['target_id'],r['procedure'],r['method']))
        coverage.sort(key=lambda r:(r['decision_epoch'],r['instrument'],r['target_id'],r['procedure'],r['method']))
        scores=score(forecasts,outcomes,c);inventory=[{k:v for k,v in m.items() if k!='ridge'} for (m,b) in fits.values()]
        comparisons=[]
        for row in scores:
            if row['method'] not in ('ridge','recovered_hgb'):continue
            zero=next(s for s in scores if s['target_id']==row['target_id'] and s['procedure']==row['procedure'] and s['method']=='zero')
            comparisons.append({'target_id':row['target_id'],'procedure':row['procedure'],'method':row['method'],'mse_delta_vs_zero':row['mse_bps2']-zero['mse_bps2'],'mae_delta_vs_zero':row['mae_bps']-zero['mae_bps']})
        report={'status':'completed_bounded_development_forecast_campaign','instruments':68,'fit_pairs':len(fits),'models_fitted':2*len(fits),'target_count':7,'procedures':c['procedures'],'coverage_rows':len(coverage),'forecast_rows':len(forecasts),'score_groups':len(scores),'scoring_support_matched':True,'comparisons':comparisons,'calendar_target_status':{n:'blocked_venue_calendar_not_qualified' for n in ('daily_close','2_sessions','5_sessions')},'policy_frames':0,'engineering_ready':False,'forecast_evidence_status':'bounded_matched_development_only','policy_evidence_status':'not_evaluated_for_this_campaign','demo_authorization_status':'not_granted','independent_review':False,'selected_model':None,'fit_reuse':'existing_ridge_and_unchanged_corrected_HGB_source; retained_weights_incompatible_with26feature_contract','next_item':'matched_campaign_remaining_horizon_policy_bridge_v2'}
        resources=[json.loads(line) for line in (p.root/'FIT_TIMINGS.jsonl').read_text().splitlines()]
        if {r['fit_id'] for r in resources}!={m['fit_id'] for m,b in fits.values()} or any(not 0<=r['elapsed_seconds']<=c['fit_latency_seconds'] for r in resources):raise ValueError('fit_resource_evidence_missing')
        for n,obj in [('fit_resources.json',resources),('forecasts.json',forecasts),('coverage.json',coverage),('scores.json',scores),('model_inventory.json',inventory),('run_report.json',report)]:payloads.append(p.write_or_validate_payload(n,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required(c)))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
if __name__=='__main__':
    import argparse
    a=argparse.ArgumentParser()
    for n in ('recipe','input-root','runs-dir'):a.add_argument('--'+n,type=Path,required=True)
    a.add_argument('--recipe-sha256',required=True);a.add_argument('--crash-after',type=int);x=a.parse_args()
    from matched_campaign_operator_v2 import preflight
    r=preflight(x.recipe,x.recipe_sha256,x.input_root);run(x.input_root,r,runs_dir=x.runs_dir,resume=True,crash_after=x.crash_after)
