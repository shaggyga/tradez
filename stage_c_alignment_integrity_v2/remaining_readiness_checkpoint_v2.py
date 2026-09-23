"""Joint-ready native preparation recovery with original raw-slice fit companion."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import matched_remaining_checkpoint_v2 as original
import remaining_readiness_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file
SCHEMA='forex_remaining_readiness_checkpoint.v1';EXTERNAL={'remaining':'2fca12f0c7d6ab5085718203c1c4b8e1598db748479a749d7deca51f2a13cbb8'}
ALLOWLIST=tuple(sorted(set(original.ALLOWLIST)|set(op.SOURCES)|{'remaining_readiness_checkpoint_v2.py','REMAINING_READINESS_OPERATOR_RECIPE.json','REMAINING_READINESS_CONTRACT_V2.md','test_remaining_readiness_v2.py'}))
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def invoke(source,paths,trad,runs,action):
    recipe=source/'REMAINING_READINESS_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'remaining_readiness_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths),'--trad-root',str(trad),'--runs-dir',str(runs)];p=subprocess.run(cmd,capture_output=True,text=True,timeout=300)
    if p.returncode:raise ValueError('remaining_readiness_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified':raise ValueError('remaining_readiness_checkpoint_incomplete')
    return receipt,{x['path']:x['sha256'] for x in read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='prediction_resources.json'}
def export(package,paths,trad,runs):
    receipt,expected=invoke(ROOT,paths,trad,runs,'verify');values={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXTERNAL.json':encoded(EXTERNAL),'EXPECTED_SCIENTIFIC.json':encoded(expected),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt)};manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(values.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'scientific_payloads':len(expected),'required_companions':EXTERNAL,'timings':'authenticated_and_bounded_not_identical'}
def restore(package,digest,destination,remaining_package,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['EXTERNAL.json'])!=EXTERNAL or sha256_file(remaining_package)!=EXTERNAL['remaining']:raise ValueError('remaining_readiness_companion_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    dependency=original.restore(remaining_package,EXTERNAL['remaining'],destination/'original',False)
    paths={'remaining':str(destination/'original/runs/matched-remaining-native-inputs'),'technical':str(destination/'original/technical_runs/causal-technical-inputs')};pf=destination/'PATHS.json';pf.write_text(json.dumps(paths,indent=2)+'\n',encoding='utf-8');trad=destination/'original/trad';source=destination/'source';runs=destination/'runs';receipt,actual=invoke(source,pf,trad,runs,'run')
    if actual!=read(destination/'EXPECTED_SCIENTIFIC.json'):raise ValueError('remaining_readiness_relocated_scientific_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_REMAINING_READINESS_PATHS=str(pf),FOREX_REMAINING_READINESS_TRAD=str(trad),FOREX_REMAINING_READINESS_RUNS=str(runs));p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_remaining_readiness_v2.py')],capture_output=True,text=True,env=env,timeout=600);(destination/'tests.stdout').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('remaining_readiness_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'relocated_tests_passed':run_tests,'original_dependency':dependency,'actual_operator':receipt,'new_models_fitted':0,'upstream_full_suites_rerun':False,'timings':'authenticated_bounded_not_identical','independent_review':False};(destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','paths','trad-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','remaining-package'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args();print(json.dumps(export(a.package,a.paths,a.trad_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.remaining_package,a.run_tests),sort_keys=True))
