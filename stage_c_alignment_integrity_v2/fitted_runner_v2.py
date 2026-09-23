"""Publish/resume a qualified fit without refitting a verified completed result."""
import argparse
import json
import os
from pathlib import Path
import sys
import numpy as np
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from fitted_consumer_v2 import evaluate
from fitted_fixture_v2 import fixture
from publication import RunPublisher,effective_run_identity,sha256_file,verify_completed_run
from contracts import fingerprint

SOURCES=('fitted_consumer_v2.py','fitted_fixture_v2.py','fitted_runner_v2.py','FITTED_FIXTURE_UNIVERSE.json','contracts.py','publication.py')
REQUIRED={'run_inputs.json','qualified_result.json','models.json','forecasts.jsonl','coverage.jsonl','fit_attempts.json','run_report.json'}

def encoded(value):return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def identity_for(inputs):
    return effective_run_identity(contract={'schema_version':'forex_fitted_run.v2','input_sha256':fingerprint(inputs),
        'python':'.'.join(map(str,sys.version_info[:3])),'numpy':np.__version__,'required_payloads':sorted(REQUIRED)},
        dependency_hashes={n:sha256_file(ROOT/n) for n in SOURCES})

def run(inputs,*,run_id,runs_dir,resume=False,crash_after=None):
    identity=identity_for(inputs);p=RunPublisher(runs_dir,run_id,identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():
        verify_completed_run(p.root,identity);return {'status':'verified_completed','identity':identity['fingerprint']}
    p.acquire(recover=resume)
    try:
        payloads=[p.write_or_validate_payload('run_inputs.json',encoded(inputs))]
        cached=p.read_verified_payload('qualified_result.json')
        result=evaluate(inputs) if cached is None else json.loads(cached)
        payloads.append(p.write_or_validate_payload('qualified_result.json',encoded(result)))
        if crash_after==1:os._exit(91)
        report={k:result[k] for k in ('status','engineering_ready','forecast_evidence_status','policy_evidence_status','demo_authorization_status')}
        report.update(universe_count=len(inputs['universe']),coverage_rows=len(result['coverage']),forecast_rows=len(result['forecasts']),
            models=len(result['models']),fit_attempts=len(result['attempts']),targets=inputs['contract']['targets'],
            procedures=['frozen','adaptive'],reason_counts={reason:sum(r['reason']==reason for r in result['coverage']) for reason in sorted({r['reason'] for r in result['coverage']})},
            qualification={'training_maturity_enforced':True,'forecast_outcomes_separate':True,'model_readiness_enforced':True},
            limitations=['synthetic_observations_and_labels','elapsed_days_not_trading_session_targets','no_historical_skill_claim','no_execution_policy_input_qualification'])
        output={'models.json':encoded(result['models']),'forecasts.jsonl':b''.join(encoded(r) for r in result['forecasts']),
                'coverage.jsonl':b''.join(encoded(r) for r in result['coverage']),'fit_attempts.json':encoded(result['attempts']),'run_report.json':encoded(report)}
        for index,(name,raw) in enumerate(output.items(),2):
            payloads.append(p.write_or_validate_payload(name,raw))
            if crash_after==index:os._exit(91)
        if crash_after==0:os._exit(91)
        p.complete(payloads,REQUIRED)
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
    return {'status':'completed','identity':identity['fingerprint'],'forecasts':len(result['forecasts'])}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-id',required=True);p.add_argument('--runs-dir',type=Path,required=True)
    p.add_argument('--resume',action='store_true');p.add_argument('--input',type=Path);p.add_argument('--test-crash-after-payload',type=int,help=argparse.SUPPRESS)
    a=p.parse_args();inputs=json.loads(a.input.read_text(encoding='utf-8')) if a.input else fixture()
    print(json.dumps(run(inputs,run_id=a.run_id,runs_dir=a.runs_dir,resume=a.resume,crash_after=a.test_crash_after_payload)))

if __name__=='__main__':main()
