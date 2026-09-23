"""Report reconstruction from pinned existing model/input checkpoints."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import rich_family_checkpoint_v2 as family
import rich_dependence_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file
SCHEMA='forex_rich_dependence_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(family.ALLOWLIST)|set(op.SOURCES)|{'rich_dependence_checkpoint_v2.py','RICH_DEPENDENCE_OPERATOR_RECIPE.json','RICH_DEPENDENCE_CONTRACT_V2.md','test_paired_blocks_v2.py','test_report_synthetic_v2.py','test_rich_dependence_v2.py'}))
EXTERNAL={'family':'2f05332911e24daec483602618125dfaf36b6d7b74c4d1de2c748152d8bdd336',**family.EXTERNAL}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def invoke(source,paths_file,runs,action):
    recipe=source/'RICH_DEPENDENCE_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'rich_dependence_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths_file),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600)
    if p.returncode:raise ValueError('paired_checkpoint_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('verified_paired_report_required')
    return r,{n:op.sha(Path(r['run_path'])/n) for n in op.REQUIRED}
def export(package,paths_file,runs):
    r,expected=invoke(ROOT,paths_file,runs,'verify')
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXTERNAL.json':encoded(EXTERNAL),'EXPECTED_REPORT.json':encoded(expected),'DEPENDENCIES.json':encoded(family.lock())}
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'report_payloads':len(expected),'required_companions':EXTERNAL}
def restore(package,digest,destination,family_package,model_package,rich_package,archive,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['EXTERNAL.json'])!=EXTERNAL or json.loads(data['DEPENDENCIES.json'])!=family.lock():raise ValueError('paired_checkpoint_environment_contract_mismatch')
    for name,p in [('family',family_package),('model',model_package),('rich',rich_package),('archive',archive)]:
        if sha256_file(p)!=EXTERNAL[name]:raise ValueError('paired_checkpoint_external_hash_mismatch:'+name)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    dep=family.restore(family_package,EXTERNAL['family'],destination/'family',model_package,rich_package,archive,False)
    paths={'family':str(destination/'family/runs/matched-rich-family-comparison'),'baseline':str(destination/'family/model/matched_runs/matched-development-campaign'),'rich':str(destination/'family/rich/runs/rich-campaign-inputs')}
    paths_file=destination/'PATHS.json';paths_file.write_text(json.dumps(paths,indent=2),encoding='utf-8');source=destination/'source';runs=destination/'runs'
    r,actual=invoke(source,paths_file,runs,'run')
    if actual!=read(destination/'EXPECTED_REPORT.json'):raise ValueError('paired_relocated_report_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_DEPENDENCE_PATHS=str(paths_file),FOREX_DEPENDENCE_RUNS=str(runs))
        tests=['test_paired_blocks_v2.py','test_report_synthetic_v2.py','test_rich_dependence_v2.py']
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',*[str(source/n) for n in tests]],capture_output=True,text=True,timeout=1800,env=env)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('paired_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'report_payloads_identical':len(actual),'relocated_report_tests_passed':run_tests,'upstream_model_suites_rerun':False,'family_reconstruction':dep,'report_operator':r,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2),encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','paths','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','family-package','model-package','rich-package','archive'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.family_package,a.model_package,a.rich_package,a.archive,a.run_tests),sort_keys=True))
