"""Per-fit resumable historical research publication using the existing RunPublisher."""
import argparse
import json
import os
from pathlib import Path
import sys
import time
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import numpy as np
from historical_slice_v2 import load_inputs,validate_contract,jobs,fit_one,issue_all,settle,scores,policy_admission,LIMITATIONS
from publication import RunPublisher,effective_run_identity,sha256_file,verify_completed_run
from contracts import fingerprint

SOURCES=('historical_runner_v2.py','historical_slice_v2.py','historical_inputs_v2.py','fitted_consumer_v2.py','publication.py','contracts.py')
INPUT_NAMES=('PREFLIGHT_CONTRACT.json','PREFLIGHT_REPORT.json','historical_observations.jsonl','historical_outcomes.jsonl')

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def required(c):
    return {'run_inputs.json','models.json','run_report.json','policy_admission.json',
      *(f'fit_{i:02d}.json' for i in range(len(jobs(c)))),*(f'fit_{i:02d}_timing.json' for i in range(len(jobs(c)))),
      *(f'{kind}_{i}.jsonl' for i in range(len(c['targets'])) for kind in ('forecasts','coverage','settlements'))}

def identity_for(input_root,c):
    validate_contract(c)
    return effective_run_identity(contract={'schema_version':'forex_historical_run.v1','config':c,
      'python':'.'.join(map(str,sys.version_info[:3])),'numpy':np.__version__,'required_payloads':sorted(required(c))},
      dependency_hashes={**{n:sha256_file(ROOT/n) for n in SOURCES},**{'inputs/'+n:sha256_file(input_root/n) for n in INPUT_NAMES}})

def run(input_root,c,*,run_id,runs_dir,resume=False,crash_after=None):
    identity=identity_for(input_root,c);publisher=RunPublisher(runs_dir,run_id,identity)
    if (publisher.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(publisher.root,identity)
        return {'status':'verified_completed','identity':identity['fingerprint']}
    observations,outcomes,input_receipt=load_inputs(input_root,c)
    publisher.acquire(recover=resume)
    try:
        payloads=[publisher.write_or_validate_payload('run_inputs.json',encoded({'contract':c,'input_hashes':{n:sha256_file(input_root/n) for n in INPUT_NAMES},'input_verification':input_receipt}))]
        models=[];timings=[]
        for i,(target,cutoff) in enumerate(jobs(c)):
            name=f'fit_{i:02d}.json';timing_name=f'fit_{i:02d}_timing.json'
            cached=publisher.read_verified_payload(name);timing_raw=publisher.read_verified_payload(timing_name)
            if cached is None:
                if timing_raw is not None:raise ValueError('orphan_fit_timing')
                began=time.monotonic();model=fit_one(observations,outcomes,c,target,cutoff);elapsed=time.monotonic()-began
                if elapsed>c['fit_latency_seconds']:raise ValueError('actual_fit_exceeds_declared_simulated_latency')
                artifact={'target':target,'cutoff_epoch':cutoff,'model':model,'fit_seconds':elapsed}
                cached=encoded(artifact)
            else:
                artifact=json.loads(cached);model=artifact['model']
                if artifact['target']!=target or artifact['cutoff_epoch']!=cutoff:raise ValueError('cached_fit_job_mismatch')
                if model['status']=='fitted' and model['model_id']!=fingerprint({k:v for k,v in model.items() if k!='model_id'}):raise ValueError('cached_model_identity_mismatch')
            payloads.append(publisher.write_or_validate_payload(name,cached))
            timing={'job':i,'fit_seconds':artifact['fit_seconds'],'declared_simulated_latency_seconds':c['fit_latency_seconds']}
            payloads.append(publisher.write_or_validate_payload(timing_name,encoded(timing)))
            timings.append(timing);models.append(model)
            if crash_after==i+1:os._exit(91)
        result=issue_all(observations,models,c)
        settlements=settle(result['forecasts'],result['coverage'],outcomes,c)
        payloads.append(publisher.write_or_validate_payload('models.json',encoded(result['models'])))
        for i,target in enumerate(c['targets']):
            for kind,rows in [('forecasts',result['forecasts']),('coverage',result['coverage']),('settlements',settlements)]:
                raw=b''.join(encoded(row) for row in rows if row['target_id']==target['target_id'])
                payloads.append(publisher.write_or_validate_payload(f'{kind}_{i}.jsonl',raw))
        admission=policy_admission(c)
        report={'schema_version':'forex_historical_report.v1','status':'retrospective_development_only',
          'universe_count':68,'fit_jobs':len(models),'fitted_models':sum(m['status']=='fitted' for m in models),
          'coverage_rows':len(result['coverage']),'forecast_rows':len(result['forecasts']),
          'coverage_reasons':{reason:sum(r['reason']==reason for r in result['coverage']) for reason in sorted({r['reason'] for r in result['coverage']})},
          'settlement_statuses':{status:sum(r['status']==status for r in settlements) for status in sorted({r['status'] for r in settlements})},
          'matched_scores':scores(settlements,c),'limitations':LIMITATIONS,'policy_admission':admission,
          'engineering_ready':False,'forecast_evidence_status':'retrospective_development_slice_not_predictive_acceptance',
          'policy_evidence_status':'blocked_missing_executable_common_target_inputs','demo_authorization_status':'not_granted'}
        for name,obj in [('run_report.json',report),('policy_admission.json',admission)]:payloads.append(publisher.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        publisher.complete(payloads,required(c))
    except BaseException:
        if publisher._owner_token is not None:publisher.release()
        raise
    return {'status':'completed','identity':identity['fingerprint'],'forecasts':len(result['forecasts']),'fits':len(models)}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input-root',type=Path,required=True);p.add_argument('--contract',type=Path,required=True)
    p.add_argument('--runs-dir',type=Path,required=True);p.add_argument('--run-id',required=True);p.add_argument('--resume',action='store_true')
    p.add_argument('--test-crash-after-fit',type=int,help=argparse.SUPPRESS);a=p.parse_args()
    print(json.dumps(run(a.input_root,json.loads(a.contract.read_text(encoding='utf-8')),runs_dir=a.runs_dir,run_id=a.run_id,resume=a.resume,crash_after=a.test_crash_after_fit)))

if __name__=='__main__':main()
