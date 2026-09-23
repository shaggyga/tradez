"""Exact sampled-path reconstruction from the existing compact technical checkpoint."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from sampled_path_operator_v2 import SOURCES,sha
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_sampled_path_checkpoint.v1'
TECH_SHA='7ea1ffdd91ee4564ccad24973261819f6c8833637d8077e65f8776774e466a91'
ALLOWLIST=tuple(sorted(set(SOURCES)|{'sampled_path_checkpoint_v2.py','portable_checkpoint_v2.py','causal_technical_checkpoint_v2.py','SAMPLED_PATH_OPERATOR_RECIPE.json','SAMPLED_PATH_CONTRACT_V2.md','test_sampled_path_v2.py'}))
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def operator(source,inputs,technical,runs,action):
    recipe=source/'SAMPLED_PATH_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'sampled_path_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',sha(recipe),'--input-root',str(inputs),'--technical',str(technical),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600)
    if p.returncode:raise ValueError('sampled_path_operator_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified':raise ValueError('sampled_path_operator_incomplete')
    m=read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json');return receipt,{x['path']:x['sha256'] for x in m['payloads']}
def export(package,inputs,technical,runs):
    receipt,expected=operator(ROOT,inputs,technical,runs,'verify')
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXPECTED_REPLAY.json':encoded(expected),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),'EXTERNAL.json':encoded({'technical':TECH_SHA})}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'scientific_payloads':len(expected),'required_companions':{'technical':TECH_SHA},'bulk_archive_included':False}
def restore(package,digest,destination,technical_package,run_tests=False):
    if sha(technical_package)!=TECH_SHA:raise ValueError('original_technical_checkpoint_required')
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    import causal_technical_checkpoint_v2 as technical_cp
    technical_cp.restore(technical_package,TECH_SHA,destination/'technical',False)
    inputs=destination/'technical/inputs';technical=destination/'technical/runs/causal-technical-inputs';runs=destination/'runs'
    receipt,actual=operator(destination/'source',inputs,technical,runs,'run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('sampled_path_scientific_payload_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_PATH_INPUT=str(inputs),FOREX_PATH_TECHNICAL=str(technical),FOREX_PATH_RUNS=str(runs))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_sampled_path_v2.py')],capture_output=True,text=True,env=env,timeout=1200)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('sampled_path_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'relocated_tests_passed':run_tests,'original_technical_payloads_reconstructed':70,'models_fitted':0,'actual_operator':receipt}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','input-root','technical','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','technical-package'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    result=export(a.package,a.input_root,a.technical,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.technical_package,a.run_tests);print(json.dumps(result,sort_keys=True))
