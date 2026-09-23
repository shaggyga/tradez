"""One writer, authenticated per-pair checkpoints, no estimator import or fitting."""
import hashlib,io,json,os
import pandas as pd
from publication import RunPublisher,effective_run_identity,verify_completed_run
from sampled_path_adapter_v2 import contract,pair_records,summarize

def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def required(r):return sorted(['pair_'+p+'.json' for p in r['universe']]+['run_report.json','path_contract.json'])
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':required(r)},dependency_hashes={**r['sources'],'input_manifest':r['input_manifest_sha256'],'technical':r['technical']['identity']['fingerprint']})
def checked(path,digest):
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('sampled_path_consumed_input_changed')
    return raw

def run(inputs,technical,r,runs,*,resume=False,crash_after=None):
    identity=identity_for(r);p=RunPublisher(runs,r['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        m=json.loads(checked(inputs/'SLICES_MANIFEST.json',r['input_manifest_sha256']));c=contract();parts=[];payloads=[]
        for index,row in enumerate(sorted(m['members'],key=lambda x:x['instrument']),1):
            name='pair_'+row['instrument']+'.json';raw=p.read_verified_payload(name)
            if raw is None:
                source=checked(inputs/row['path'],row['sha256']);original=json.loads(checked(technical/name,r['technical']['payloads'][name]))
                raw=encoded(pair_records(row['instrument'],pd.read_parquet(io.BytesIO(source)),original,row['source_member_sha256'],c))
            part=json.loads(raw)
            if part['instrument']!=row['instrument']:raise ValueError('sampled_path_cached_instrument_mismatch')
            parts.append(part);payloads.append(p.write_or_validate_payload(name,raw))
            if crash_after==index:os._exit(91)
        payloads.append(p.write_or_validate_payload('path_contract.json',encoded(c)));payloads.append(p.write_or_validate_payload('run_report.json',encoded(summarize(parts,c))))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required(r)))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
