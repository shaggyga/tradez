"""Reuse three matched fits, fill five direct-horizon gaps, prepare native nodes."""
import json,os,time,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import pandas as pd
from publication import RunPublisher,effective_run_identity,verify_completed_run
from contracts import fingerprint
from causal_technical_adapter_v2 import clean
from retained_endpoint_targets_v1 import endpoint_outcomes
from matched_campaign_models_v2 import contract as campaign_contract,fit_pair,validate_fit
from matched_remaining_native_v2 import ORIGIN,TARGET,EPOCHS,NEW_MINUTES,REUSE_MINUTES,prepared_packets

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def required():return sorted(['fit_'+str(h)+suffix for h in sorted(NEW_MINUTES+REUSE_MINUTES) for suffix in ('.json','.joblib')]+['remaining_outcomes.json','references.json','forecasts.json','coverage.json','native_packets.json','run_report.json','fit_resources.json'])
def identity_for(recipe):return effective_run_identity(contract={'recipe':recipe,'required_payloads':required()},dependency_hashes={**recipe['sources'],'technical_input':recipe['technical_identity']['fingerprint'],'matched_models':recipe['matched_identity']['fingerprint'],'slices':recipe['slices_manifest_sha256']})
def prepare_inputs(slices,technical,recipe):
    verify_completed_run(technical,recipe['technical_identity']);manifest=json.loads((slices/'SLICES_MANIFEST.json').read_text());parts=[json.loads((technical/('pair_'+p+'.json')).read_text()) for p in recipe['universe']]
    observations=[o for p in parts for o in p['observations']];outcomes=[o for p in parts for o in p['outcomes']];references={str(t):{} for t in EPOCHS};extra=[]
    by_pair={p['instrument']:p['observations'] for p in parts}
    for r in manifest['members']:
        frame,_=clean(pd.read_parquet(slices/r['path']));pos={int(t):i for i,t in enumerate(frame.epoch)}
        data={'time':frame.epoch.to_numpy(),'close':frame.mid.to_numpy(),'bid_close':frame.bid.to_numpy(),'ask_close':frame.ask.to_numpy()}
        labels={h:endpoint_outcomes(data,h) for h in NEW_MINUTES} if len(frame) else {}
        for o in by_pair[r['instrument']]:
            i=pos.get(o['source_bar_start_epoch'])
            for h in NEW_MINUTES:
                v=None if i is None else labels[h]['midpoint_return_bps'][i]
                value=None if v is None or pd.isna(v) else float(v)
                extra.append({'record_id':o['record_id'],'target_id':f'technical_endpoint_midpoint_elapsed_{h}m','label_end_epoch':o['origin_epoch']+h*60,'available_epoch':o['origin_epoch']+h*60,'value':value,'status':'exact_endpoints_available' if value is not None else 'missing_exact_endpoint','source_member_sha256':r['source_member_sha256'],'execution_status':'not_qualified','path_continuity_required':False})
            if o['origin_epoch'] in EPOCHS and i is not None and o['features'] is not None:
                row=frame.iloc[i];ref={'instrument':r['instrument'],'price_epoch':o['origin_epoch'],'source_member_sha256':r['source_member_sha256'],'reference_close':repr(float(row.mid)),'bid':repr(float(row.bid)),'ask':repr(float(row.ask)),'pip_size':str(r['pip_size'])}
                ref['record_sha256']=fingerprint(ref);references[str(o['origin_epoch'])][r['instrument']]=ref
    return observations,outcomes+extra,extra,references

def run(slices,technical,matched,trad,recipe,*,runs_dir,resume=False,crash_after=None):
    verify_completed_run(matched,recipe['matched_identity']);identity=identity_for(recipe);p=RunPublisher(runs_dir,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    observations,outcomes,extra,references=prepare_inputs(slices,technical,recipe);p.acquire(recover=resume)
    try:
        payloads=[];fits={};c=campaign_contract();cutoff=c['fit_cutoffs'][0]
        for index,h in enumerate(sorted(NEW_MINUTES+REUSE_MINUTES)):
            name='fit_'+str(h);raw=p.read_verified_payload(name+'.json');tree=p.read_verified_payload(name+'.joblib')
            if h in REUSE_MINUTES:
                raw=(matched/f'fit_{h}_{cutoff}.json').read_bytes();tree=(matched/f'fit_{h}_{cutoff}.joblib').read_bytes()
            elif raw is None or tree is None:
                start=time.monotonic();meta,tree=fit_pair(observations,outcomes,recipe['universe'],h,cutoff,c);elapsed=time.monotonic()-start
                if elapsed>c['fit_latency_seconds']:raise ValueError('remaining_fit_latency_exceeded')
                raw=encoded(meta)
                with (p.root/'FIT_TIMINGS.jsonl').open('ab') as log:log.write(encoded({'fit_id':meta['fit_id'],'minutes':h,'elapsed_seconds':elapsed,'limit_seconds':30}));log.flush();os.fsync(log.fileno())
            meta=json.loads(raw);validate_fit(meta,tree)
            for suffix,b in [('.json',raw),('.joblib',tree)]:payloads.append(p.write_or_validate_payload(name+suffix,b))
            fits[h]=(meta,tree)
            if crash_after==index+1:os._exit(91)
        forecasts=[];coverage=[];packets=[]
        for epoch in EPOCHS:
            minutes=(TARGET-epoch)//60;current=[o for o in observations if o['origin_epoch']==epoch]
            native,predictions=prepared_packets(*fits[minutes],current,references[str(epoch)],trad_root=trad)
            packets.extend(native);forecasts.extend(predictions['forecasts']);coverage.extend(predictions['coverage'])
        resources=[json.loads(line) for line in (p.root/'FIT_TIMINGS.jsonl').read_text().splitlines()]
        if {x['minutes'] for x in resources}!=set(NEW_MINUTES) or any(not 0<=x['elapsed_seconds']<=30 for x in resources):raise ValueError('remaining_fit_resource_evidence_missing')
        report={'status':'completed_matched_remaining_native_inputs','instruments':68,'original_origin':ORIGIN,'original_target':TARGET,'decision_epochs':EPOCHS,'new_fit_pairs':len(NEW_MINUTES),'reused_fit_pairs':len(REUSE_MINUTES),'new_models_fitted':2*len(NEW_MINUTES),'reused_models':2*len(REUSE_MINUTES),'coverage_rows':len(coverage),'forecast_rows':len(forecasts),'native_packets':len(packets),'methods':['ridge','recovered_hgb'],'original_target_preserved':True,'conditioning':'fresh26feature_direct_exact_remaining_horizon','observed_publication':False,'observed_execution':False,'policy_frames':0,'engineering_ready':False,'forecast_evidence_status':'bounded_development_remaining_forecasts','policy_evidence_status':'native_preparation_only','demo_authorization_status':'not_granted','independent_review':False,'next_item':'matched_campaign_remaining_horizon_policy_bridge_v2'}
        for name,obj in [('remaining_outcomes.json',extra),('references.json',references),('forecasts.json',forecasts),('coverage.json',coverage),('native_packets.json',packets),('run_report.json',report),('fit_resources.json',resources)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required()))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser()
    for n in ('recipe','slices','technical','matched','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--crash-after',type=int);a=p.parse_args()
    from matched_remaining_operator_v2 import preflight
    r=preflight(a.recipe,a.recipe_sha256,a.slices,a.technical,a.matched,a.trad_root);run(a.slices,a.technical,a.matched,a.trad_root,r,runs_dir=a.runs_dir,resume=True,crash_after=a.crash_after)
