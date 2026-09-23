"""Resumable original-model recreation and campaign admission publication."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from contracts import fingerprint
from publication import RunPublisher,effective_run_identity,verify_completed_run
REQUIRED={'original_recreation.json','original_curve_example.json','model_inventory.json','feature_population.json','native_coverage.json','campaign_admission.json','run_report.json'}

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def identity_for(recipe):
    return effective_run_identity(contract={'recipe':recipe,'required_payloads':sorted(REQUIRED)},dependency_hashes={**recipe['sources'],'retained_manifest':recipe['retained_manifest_sha256']})

def run(root,recipe,*,runs_dir,resume=False,crash_after=None):
    from directional_recovery_operator_v2 import verify_retained
    verify_retained(root)
    identity=identity_for(recipe);p=RunPublisher(runs_dir,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        payloads=[];old=p.read_verified_payload('original_recreation.json');example=p.read_verified_payload('original_curve_example.json')
        if old is None or example is None:
            with tempfile.TemporaryDirectory(prefix='retained-recreation-',dir=p.root) as scratch:
                d=root/'direction_decision_20260911';out=Path(scratch)/'RESULT.json'
                command=[sys.executable,str(d/'verify_saved_cost_study.py'),'--evaluation',str(d/'evaluation_001'),'--output',str(out)]
                proc=subprocess.run(command,capture_output=True,text=True,timeout=240)
                if proc.returncode:raise RuntimeError('original_recreation_failed:'+proc.stderr[-2000:])
                old=encoded(json.loads(out.read_text()));example=encoded(json.loads(out.with_name('FOUR_HEAD_REPLAY_EXAMPLE.json').read_text()))
        receipt=json.loads(old)
        if not receipt['verified'] or receipt['model_bundles_recreated']!=8 or receipt['numeric_values_recreated']!=718074 or receipt['maximum_absolute_difference']!=0:raise ValueError('original_recreation_acceptance_failed')
        for n,b in [('original_recreation.json',old),('original_curve_example.json',example)]:payloads.append(p.write_or_validate_payload(n,b))
        if crash_after==1:os._exit(91)
        from directional_recovery_audit_v2 import audit
        pending={n:p.read_verified_payload(n) for n in REQUIRED-{'original_recreation.json','original_curve_example.json'}}
        computed=audit(root) if any(b is None for b in pending.values()) else {}
        for n in sorted(pending):payloads.append(p.write_or_validate_payload(n,pending[n] if pending[n] is not None else encoded(computed[n])))
        if crash_after==0:os._exit(91)
        p.complete(payloads,REQUIRED)
    except BaseException:
        if p._owner_token is not None:p.release()
        raise

if __name__=='__main__':
    import argparse
    a=argparse.ArgumentParser();a.add_argument('--retained-root',type=Path,required=True);a.add_argument('--recipe',type=Path,required=True);a.add_argument('--recipe-sha256',required=True);a.add_argument('--runs-dir',type=Path,required=True);a.add_argument('--crash-after',type=int);x=a.parse_args()
    from directional_recovery_operator_v2 import preflight
    r=preflight(x.recipe,x.recipe_sha256,x.retained_root);run(x.retained_root,r,runs_dir=x.runs_dir,resume=True,crash_after=x.crash_after)
