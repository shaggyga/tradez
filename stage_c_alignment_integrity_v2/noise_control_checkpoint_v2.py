"""Portable richer-model comparison with preserved model/raw companions."""
import argparse,hashlib,importlib.metadata,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import matched_remaining_checkpoint_v2 as model
import rich_campaign_checkpoint_v2 as rich
import noise_control_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file
SCHEMA='forex_noise_control_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(model.ALLOWLIST)|set(rich.ALLOWLIST)|set(op.SOURCES)|{'noise_control_checkpoint_v2.py','NOISE_CONTROL_CONTRACT_V2.md','NOISE_CONTROL_OPERATOR_RECIPE.json','test_noise_control_v2.py'}))
EXTERNAL={'model':'2fca12f0c7d6ab5085718203c1c4b8e1598db748479a749d7deca51f2a13cbb8','rich':'251d1a39778b4cd95aa2d061d9c03a80c961a1e478fbec128be82efb6746f4e3','archive':'aca163639a18e22b2c60dc4ff36d55d82c655af617836a6d351dd1339e5a6006'}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def lock():return {**rich.lock(),'model_packages':{n:importlib.metadata.version(n) for n in ('scipy','scikit-learn','joblib','threadpoolctl')}}
def invoke(source,richroot,baseline,trad,runs,action):
    recipe=source/'NOISE_CONTROL_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'noise_control_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--rich',str(richroot),'--baseline',str(baseline),'--trad-root',str(trad),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=900)
    if p.returncode:raise ValueError('noise_control_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified':raise ValueError('verified_noise_control_required')
    hashes={x['path']:x['sha256'] for x in read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='fit_resources.json'}
    return receipt,hashes
def export(package,richroot,baseline,trad,runs):
    receipt,expected=invoke(ROOT,richroot,baseline,trad,runs,'verify')
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXTERNAL.json':encoded(EXTERNAL),'EXPECTED_SCIENTIFIC.json':encoded(expected),'DEPENDENCIES.json':encoded(lock())}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'scientific_payloads':len(expected),'timings':'bounded_not_identical','required_companions':EXTERNAL}
def restore(package,digest,destination,model_package,rich_package,archive,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['EXTERNAL.json'])!=EXTERNAL or json.loads(data['DEPENDENCIES.json'])!=lock():raise ValueError('noise_control_restore_contract_environment_mismatch')
    for name,p in [('model',model_package),('rich',rich_package),('archive',archive)]:
        if sha256_file(p)!=EXTERNAL[name]:raise ValueError('noise_control_external_hash_mismatch:'+name)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    mr=model.restore(model_package,EXTERNAL['model'],destination/'model',False)
    rr=rich.restore(rich_package,EXTERNAL['rich'],destination/'rich',archive,False)
    baseline=destination/'model/matched_runs/matched-development-campaign';richroot=destination/'rich/runs/rich-campaign-inputs';trad=destination/'rich/trad';source=destination/'source';runs=destination/'runs'
    receipt,actual=invoke(source,richroot,baseline,trad,runs,'run')
    if actual!=read(destination/'EXPECTED_SCIENTIFIC.json'):raise ValueError('noise_control_relocated_scientific_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_NOISE_RICH=str(richroot),FOREX_NOISE_BASE=str(baseline),FOREX_NOISE_TRAD=str(trad),FOREX_NOISE_RUNS=str(runs))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_noise_control_v2.py')],capture_output=True,text=True,timeout=2400,env=env)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('noise_control_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'relocated_tests_passed':run_tests,'model_dependency':mr,'rich_dependency':rr,'actual_operator':receipt,'timings':'bounded_not_identical','raw_archive_duplicated':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2),encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','rich','baseline','trad-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','model-package','rich-package','archive'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.rich,a.baseline,a.trad_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.model_package,a.rich_package,a.archive,a.run_tests),sort_keys=True))
