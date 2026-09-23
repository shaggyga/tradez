"""Compact standalone recovery of the fixed official context and original inputs."""
import argparse,hashlib,json,os,stat,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import official_context_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_official_context_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(op.SOURCES)|{'official_context_checkpoint_v2.py','portable_checkpoint_v2.py','OFFICIAL_CONTEXT_OPERATOR_RECIPE.json','OFFICIAL_CONTEXT_CONTRACT_V2.md','test_official_context_v2.py'}))
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def invoke(source,inputs,runs,action):
    recipe=source/'OFFICIAL_CONTEXT_OPERATOR_RECIPE.json'
    cmd=[sys.executable,'-I','-B',str(source/'official_context_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--retained',str(inputs/'retained_v5'),'--audit-inputs',str(inputs/'audit_inputs'),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
    if p.returncode:raise ValueError('official_checkpoint_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('verified_official_context_required')
    return r,{n:op.sha(Path(r['run_path'])/n) for n in op.REQUIRED}
def export(package,inputs,runs):
    result,expected=invoke(ROOT,inputs,runs,'verify');recipe=read(ROOT/'OFFICIAL_CONTEXT_OPERATOR_RECIPE.json')
    payloads={'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST}
    for name,digest in recipe['retained'].items():
        raw=(inputs/'retained_v5'/name).read_bytes()
        if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('official_export_input_changed')
        payloads['inputs/retained_v5/'+name]=raw
    for name,digest in recipe['audit_inputs'].items():
        raw=(inputs/'audit_inputs'/name).read_bytes()
        if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('official_export_audit_changed')
        payloads['inputs/audit_inputs/'+name]=raw
    payloads['EXPECTED.json']=encoded(expected)
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'report_payloads':len(expected),'standalone':True,'requires_external_raw_archive':False}
def restore(package,digest,destination,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for name,raw in data.items():
        p=destination.joinpath(*safe_member(name).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(raw)
    source=destination/'source';inputs=destination/'inputs';runs=destination/'runs'
    archive=inputs/'retained_v5/data/oanda_training_manager/source_archives/official_fact_adapter_v4/immutable_event_clock_v2_a5276c696eb6ff3.sqlite'
    # The retained V5 contract requires this one restored archive to be read-only.
    archive.chmod(stat.S_IREAD)
    result,actual=invoke(source,inputs,runs,'run')
    if actual!=read(destination/'EXPECTED.json'):raise ValueError('official_relocated_payload_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_OFFICIAL_INPUTS=str(inputs),FOREX_OFFICIAL_RUNS=str(runs))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_official_context_v2.py')],capture_output=True,text=True,env=env,timeout=300)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('official_relocated_tests_failed:'+p.stdout+p.stderr)
    receipt={'status':'VERIFIED','checkpoint_sha256':digest,'payloads_identical':len(actual),'relocated_tests_passed':run_tests,'standalone_inputs':True,'operator':result,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8');return receipt
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','inputs','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.inputs,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
