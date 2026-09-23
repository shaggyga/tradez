"""Bounded remaining-horizon development; reuse fits and per-fit publication."""
import json
import os
from pathlib import Path
import sys
import time
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from contracts import TrainingView, fingerprint
from fitted_consumer_v2 import fit_model
from historical_inputs_v2 import verify
from remaining_horizon_v2 import coverage
from publication import RunPublisher, effective_run_identity, sha256_file, verify_completed_run

SOURCES=('remaining_runner_v2.py','remaining_horizon_v2.py','fitted_consumer_v2.py',
         'historical_inputs_v2.py','contracts.py','publication.py')
INPUTS=('PREFLIGHT_CONTRACT.json','PREFLIGHT_REPORT.json','historical_observations.jsonl',
        'historical_outcomes.jsonl','reused_models.json')

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def validate(c):
    if c != {'schema_version':'forex_remaining_development.v1','origin_epoch':1721606460,
             'target_epoch':1721779260,'cadence_seconds':21600,'fit_cutoff':1721606400,
             'training_start':1719792000,'evaluation_asof':1722643200,'ridge_lambda':20,
             'minimum_rows':100,'fit_latency_seconds':60,'prediction_latency_seconds':2,
             'new_minutes':[360,720,1080,1800,2160,2520],'reuse_minutes':[1440,2880],
             'scope':'previously_inspected_gross_midpoint_development','broker_access':False}:
        raise ValueError('frozen_remaining_contract_required')

def required(c):
    return {'models.json','coverage.json','forecasts.json','run_report.json',
            *(f'fit_{m}.json' for m in c['new_minutes'])}

def identity_for(root,c):
    validate(c)
    return effective_run_identity(contract={'config':c,'required_payloads':sorted(required(c))},
        dependency_hashes={**{n:sha256_file(ROOT/n) for n in SOURCES},
                           **{'inputs/'+n:sha256_file(root/n) for n in INPUTS}})

def run(root,c,*,runs_dir,run_id='remaining-development',resume=False,crash_after=None):
    identity=identity_for(root,c);p=RunPublisher(runs_dir,run_id,identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(p.root,identity);return {'status':'completed_verified'}
    verify(root)
    observations=[json.loads(s) for s in (root/'historical_observations.jsonl').read_text().splitlines()]
    outcomes=[json.loads(s) for s in (root/'historical_outcomes.jsonl').read_text().splitlines()]
    universe=sorted({o['instrument'] for o in observations})
    models=[m for m in json.loads((root/'reused_models.json').read_text())
        if m.get('status')=='fitted' and not m.get('control') and
        m['training_view']['fit_cutoff_epoch']==c['fit_cutoff'] and
        m['target']['horizon_seconds'] in [x*60 for x in c['reuse_minutes']]]
    if len(models)!=2:raise ValueError('two_existing_fits_required')
    for m in models:
        if m['model_id']!=fingerprint({k:v for k,v in m.items() if k!='model_id'}):raise ValueError('reused_fit_identity_mismatch')
    p.acquire(recover=resume)
    try:
        payloads=[]
        for i,minutes in enumerate(c['new_minutes']):
            name=f'fit_{minutes}.json';raw=p.read_verified_payload(name)
            if raw is None:
                start=time.monotonic()
                m=fit_model(observations,outcomes,universe=universe,
                    target={'target_id':f'historical_gross_midpoint_elapsed_{minutes}m','horizon_seconds':minutes*60},
                    view=TrainingView(c['training_start'],c['fit_cutoff'],c['fit_cutoff'],c['fit_cutoff'],c['evaluation_asof']),
                    ready_epoch=c['fit_cutoff']+c['fit_latency_seconds'],ridge_lambda=c['ridge_lambda'],min_rows=c['minimum_rows'])
                elapsed=time.monotonic()-start
                if elapsed>c['fit_latency_seconds']:raise ValueError('fit_latency_exceeded')
                raw=encoded({'model':m,'fit_seconds':elapsed})
            artifact=json.loads(raw);m=artifact['model']
            if m['status']=='fitted' and m['model_id']!=fingerprint({k:v for k,v in m.items() if k!='model_id'}):raise ValueError('cached_fit_identity_mismatch')
            payloads.append(p.write_or_validate_payload(name,raw));models.append(m)
            if crash_after==i+1:os._exit(91)
        epochs=list(range(c['origin_epoch'],c['target_epoch'],c['cadence_seconds']))
        result=coverage(models,observations,universe=universe,decision_epochs=epochs,
                        target_epoch=c['target_epoch'],prediction_latency_seconds=c['prediction_latency_seconds'])
        report={'status':result['status'],'coverage_rows':len(result['coverage']),
            'forecasts':len(result['forecasts']),'instruments':len(universe),
            'new_fits':6,'reused_fits':2,'policy_frames':0,
            'reasons':{r:sum(x['reason']==r for x in result['coverage']) for r in sorted({x['reason'] for x in result['coverage']})},
            'scope':'single_two_elapsed_day_cohort_at_six_hour_updates_not_full_campaign',
            'limitations':['gross_midpoint_not_executable','assumed_bar_close_availability',
                'no_fresh_probability_calibration','no_native_policy_packet','no_performance_or_promotion_claim']}
        for name,obj in [('models.json',models),('coverage.json',result['coverage']),
                         ('forecasts.json',result['forecasts']),('run_report.json',report)]:
            payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,required(c))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
    return {'status':'completed_verified','identity':identity['fingerprint'],**report}


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('input-root','contract','runs-dir'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--run-id',default='remaining-development')
    parser.add_argument('--resume',action='store_true')
    parser.add_argument('--test-crash-after-fit',type=int,help=argparse.SUPPRESS)
    args=parser.parse_args()
    print(json.dumps(run(args.input_root,json.loads(args.contract.read_text()),
        runs_dir=args.runs_dir,run_id=args.run_id,resume=args.resume,
        crash_after=args.test_crash_after_fit)))
