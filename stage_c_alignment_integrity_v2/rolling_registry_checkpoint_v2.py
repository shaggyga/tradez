"""Portable bounded OHLC/lineage registry replay using existing ZIP guards."""
import argparse,hashlib,importlib.metadata,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import rolling_registry_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded,environment_lock
SCHEMA='forex_rolling_registry_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(op.SOURCES)|{'portable_checkpoint_v2.py','rolling_registry_checkpoint_v2.py','ROLLING_REGISTRY_OPERATOR_RECIPE.json','ROLLING_REGISTRY_CONTRACT_V2.md','test_rolling_registry_v2.py'}))
def lock():return {**environment_lock(),'registry_packages':{n:importlib.metadata.version(n) for n in ('pandas','pytest')}}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def invoke(source,root,lineage,trad,runs,action):
    recipe=source/'ROLLING_REGISTRY_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'rolling_registry_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--input-root',str(root),'--lineage-root',str(lineage),'--trad-root',str(trad),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=180)
    if p.returncode:raise ValueError('registry_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('registry_checkpoint_requires_complete_run')
    expected={x['path']:x['sha256'] for x in read(Path(r['run_path'])/'COMPLETION_MANIFEST.json')['payloads']}
    return r,expected
def export(package,root,lineage,trad,runs):
    r,expected=invoke(ROOT,root,lineage,trad,runs,'verify');m,lm=op.validate_inputs(root,lineage,trad)
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},**{'trad/'+n:(trad/n).read_bytes() for n in op.PREDECESSORS},
        **{'inputs/'+n:(root/n).read_bytes() for n in ['INPUT_MANIFEST.json']+[x['path'] for x in m['members']]},
        **{'lineage/'+n:(lineage/n).read_bytes() for n in ['LINEAGE_MANIFEST.json']+[x['path'] for x in lm['members']]},
        'EXPECTED_REPLAY.json':encoded(expected),'DEPENDENCIES.json':encoded(lock())}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'payloads':len(expected),'bundled_raw_rows':sum(x['rows'] for x in m['members']),'legacy_model_weights_bundled':False}
def restore(package,digest,destination,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['DEPENDENCIES.json'])!=lock():raise ValueError('registry_restore_environment_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    r,actual=invoke(destination/'source',destination/'inputs',destination/'lineage',destination/'trad',destination/'runs','run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('registry_relocated_payload_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_REGISTRY_INPUT=str(destination/'inputs'),FOREX_REGISTRY_LINEAGE=str(destination/'lineage'),FOREX_REGISTRY_TRAD=str(destination/'trad'),FOREX_REGISTRY_RUNS=str(destination/'runs'))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_rolling_registry_v2.py')],capture_output=True,text=True,timeout=300,env=env)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('registry_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'payload_count':len(actual),'relocated_tests_passed':run_tests,'actual_operator':r,'scope':'raw_OHLC_to_populated_registry_and_feature_consumer; preserved_legacy_evidence_not_full_legacy_model_replay'}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2),encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','input-root','lineage-root','trad-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.input_root,a.lineage_root,a.trad_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
