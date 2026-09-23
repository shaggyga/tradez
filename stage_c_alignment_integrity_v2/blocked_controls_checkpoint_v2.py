"""Pinned original model/input reconstruction followed by blocked-time/leak controls."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import rich_family_checkpoint_v2 as family
import blocked_controls_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file
SCHEMA='forex_blocked_controls_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(family.ALLOWLIST)|set(op.SOURCES)|{'blocked_controls_checkpoint_v2.py','BLOCKED_CONTROLS_OPERATOR_RECIPE.json','BLOCKED_CONTROLS_CONTRACT_V2.md','test_blocked_time_controls_v2.py','test_blocked_controls_v2.py'}))
EXTERNAL={'family':'2f05332911e24daec483602618125dfaf36b6d7b74c4d1de2c748152d8bdd336',**family.EXTERNAL}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def invoke(source,paths,raw,trad,runs,action):
    recipe=source/'BLOCKED_CONTROLS_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'blocked_controls_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths),'--raw-root',str(raw),'--trad-root',str(trad),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=1800)
    if p.returncode:raise ValueError('controls_checkpoint_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('verified_controls_projection_required')
    actual={x['path']:x['sha256'] for x in read(Path(r['run_path'])/'COMPLETION_MANIFEST.json')['payloads'] }
    return r,actual
def export(package,paths,raw,trad,runs):
    r,expected=invoke(ROOT,paths,raw,trad,runs,'verify')
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXTERNAL.json':encoded(EXTERNAL),'EXPECTED_SCIENTIFIC.json':encoded(expected),'DEPENDENCIES.json':encoded(family.lock())}
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'scientific_payloads':len(expected),'required_companions':EXTERNAL}
def restore(package,digest,destination,family_package,model_package,rich_package,archive,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['EXTERNAL.json'])!=EXTERNAL or json.loads(data['DEPENDENCIES.json'])!=family.lock():raise ValueError('controls_checkpoint_environment_contract_mismatch')
    for name,p in [('family',family_package),('model',model_package),('rich',rich_package),('archive',archive)]:
        if sha256_file(p)!=EXTERNAL[name]:raise ValueError('controls_checkpoint_external_hash_mismatch:'+name)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    dep=family.restore(family_package,EXTERNAL['family'],destination/'family',model_package,rich_package,archive,False)
    paths={'family':str(destination/'family/runs/matched-rich-family-comparison'),'baseline':str(destination/'family/model/matched_runs/matched-development-campaign'),'rich':str(destination/'family/rich/runs/rich-campaign-inputs')}
    pf=destination/'PATHS.json';pf.write_text(json.dumps(paths,indent=2),encoding='utf-8');trad=destination/'family/rich/trad';source=destination/'source';runs=destination/'runs'
    raw=destination/'family/rich/inputs'
    r,actual=invoke(source,pf,raw,trad,runs,'run')
    if actual!=read(destination/'EXPECTED_SCIENTIFIC.json'):raise ValueError('controls_relocated_scientific_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_CONTROLS_PATHS=str(pf),FOREX_CONTROLS_RAW=str(raw),FOREX_CONTROLS_TRAD=str(trad),FOREX_CONTROLS_RUNS=str(runs))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_blocked_time_controls_v2.py'),str(source/'test_blocked_controls_v2.py')],capture_output=True,text=True,timeout=2400,env=env)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('controls_relocated_tests_failed:'+p.stdout+p.stderr)
    receipt={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'relocated_tests_passed':run_tests,'upstream_model_suites_rerun':False,'family_reconstruction':dep,'operator':r,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8');return receipt
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','paths','raw-root','trad-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','family-package','model-package','rich-package','archive'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.raw_root,a.trad_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.family_package,a.model_package,a.rich_package,a.archive,a.run_tests),sort_keys=True))
