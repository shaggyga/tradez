"""One-writer, per-instrument durable reuse of the original feature/label code."""
import json,os,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import pandas as pd
from contracts import fingerprint
from publication import RunPublisher,effective_run_identity,verify_completed_run
from causal_technical_adapter_v2 import contract,pair_records,summarize

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def identity_for(recipe):return effective_run_identity(contract={'recipe':recipe,'required_payloads':sorted(['pair_'+p+'.json' for p in recipe['universe']]+['run_report.json','input_contract.json'])},dependency_hashes={**recipe['sources'],'input_manifest':recipe['input_manifest_sha256']})
def run(root,recipe,*,runs_dir,resume=False,crash_after=None):
    from causal_technical_operator_v2 import validate_inputs
    m=validate_inputs(root);c=contract()
    if c!=m['contract']:raise ValueError('frozen_technical_contract_required')
    identity=identity_for(recipe);p=RunPublisher(runs_dir,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        parts=[];payloads=[]
        for index,r in enumerate(sorted(m['members'],key=lambda x:x['instrument'])):
            name='pair_'+r['instrument']+'.json';raw=p.read_verified_payload(name)
            if raw is None:raw=encoded(pair_records(r['instrument'],pd.read_parquet(root/r['path']),r['pip_size'],r['source_member_sha256'],c))
            part=json.loads(raw)
            if part['instrument']!=r['instrument']:raise ValueError('cached_pair_identity_mismatch')
            parts.append(part);payloads.append(p.write_or_validate_payload(name,raw))
            if crash_after==index+1:os._exit(91)
        report=summarize(parts,c);payloads.append(p.write_or_validate_payload('run_report.json',encoded(report)))
        payloads.append(p.write_or_validate_payload('input_contract.json',encoded(c)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,{x['path'] for x in payloads})
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser()
    for n in ('recipe','input-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--crash-after',type=int);a=p.parse_args()
    from causal_technical_operator_v2 import preflight
    r=preflight(a.recipe,a.recipe_sha256,a.input_root);run(a.input_root,r,runs_dir=a.runs_dir,resume=True,crash_after=a.crash_after)
