"""Small inspector checkpoint with a pinned original calibration capsule companion."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import calibration_inspector_operator_v2 as op
import residual_calibration_checkpoint_v2 as calibration
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file
SCHEMA='forex_calibration_inspector_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(calibration.ALLOWLIST)|set(op.SOURCES)|{'calibration_inspector_checkpoint_v2.py','CALIBRATION_INSPECTOR_OPERATOR_RECIPE.json','CALIBRATION_INSPECTOR_CONTRACT_V2.md','test_calibration_inspector_v2.py'}))
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def invoke(source,paths,runs,action):
    recipe=source/'CALIBRATION_INSPECTOR_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'calibration_inspector_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=300)
    if p.returncode:raise ValueError('inspector_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified':raise ValueError('completed_inspector_checkpoint_required')
    return receipt,{x['path']:x['sha256'] for x in read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json')['payloads']}
def export(package,paths,runs,calibration_package):
    receipt,expected=invoke(ROOT,paths,runs,'verify');digest=sha256_file(calibration_package)
    # Validate the companion's own capsule schema and relation to this input tape.
    data=calibration.inspect_capsule(calibration_package,digest)
    if json.loads(data['ORIGINAL_OPERATOR_RECEIPT.json'])['run_identity']!=read(ROOT/'CALIBRATION_INSPECTOR_OPERATOR_RECIPE.json')['dependencies']['calibration']['identity']['fingerprint']:raise ValueError('inspector_companion_lineage_mismatch')
    values={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXPECTED_REPLAY.json':encoded(expected),'EXTERNAL.json':encoded({'calibration':digest}),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt)}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(values.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    sha=op.sha(package);inspect_package(package,sha,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':sha,'scientific_payloads':len(expected),'required_companions':{'calibration':digest},'base_models_fitted':0}
def restore(package,digest,destination,calibration_package,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA);external=json.loads(data['EXTERNAL.json'])
    if set(external)!={'calibration'} or sha256_file(calibration_package)!=external['calibration']:raise ValueError('inspector_companion_hash_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    dependency=calibration.restore(calibration_package,external['calibration'],destination/'calibration',False)
    paths={'calibration':str(destination/'calibration/runs/modeled-clock-residual-calibration'),'joint':str(destination/'calibration/capsule/joint'),'technical':str(destination/'calibration/capsule/technical')};pf=destination/'PATHS.json';pf.write_text(json.dumps(paths,indent=2)+'\n',encoding='utf-8');source=destination/'source';runs=destination/'runs'
    receipt,actual=invoke(source,pf,runs,'run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('inspector_relocated_output_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_CALIBRATION_INSPECTOR_PATHS=str(pf),FOREX_CALIBRATION_INSPECTOR_RUNS=str(runs))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_calibration_inspector_v2.py')],capture_output=True,text=True,env=env,timeout=600)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('inspector_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'relocated_tests_passed':run_tests,'calibration_dependency':dependency,'base_models_fitted':0,'inspection_calibrators_fitted':0,'dependency_residual_snapshots_replayed':True,'upstream_full_suite_rerun':False,'operator':receipt,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','paths','runs-dir','calibration-package'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','calibration-package'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.runs_dir,a.calibration_package) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.calibration_package,a.run_tests),sort_keys=True))
