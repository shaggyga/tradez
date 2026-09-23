"""Reconstruct causal inputs then refit/replay the frozen matched campaign."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from matched_campaign_operator_v2 import SOURCES,sha
from causal_technical_checkpoint_v2 import ALLOWLIST as TECHNICAL_SOURCES,operator as technical_operator
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_matched_campaign_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(SOURCES)|set(TECHNICAL_SOURCES)|{'matched_campaign_checkpoint_v2.py','MATCHED_CAMPAIGN_OPERATOR_RECIPE.json','MATCHED_CAMPAIGN_CONTRACT_V2.md','test_matched_campaign_v2.py'}))
def read(p):return json.loads(p.read_text())
def operator(source,inputs,runs,action):
    recipe=source/'MATCHED_CAMPAIGN_OPERATOR_RECIPE.json'
    p=subprocess.run([sys.executable,'-I','-B',str(source/'matched_campaign_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',sha(recipe),'--input-root',str(inputs),'--runs-dir',str(runs)],capture_output=True,text=True,timeout=360)
    if p.returncode:raise ValueError('matched_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('matched_operator_incomplete')
    m=read(Path(r['run_path'])/'COMPLETION_MANIFEST.json');return r,{x['path']:x['sha256'] for x in m['payloads'] if x['path']!='fit_resources.json'}
def export(package,slices,technical_runs,runs):
    original,technical=technical_operator(ROOT,slices,technical_runs,'verify')
    receipt,expected=operator(ROOT,Path(original['run_path']),runs,'verify')
    names=['SLICES_MANIFEST.json']+[r['path'] for r in read(slices/'SLICES_MANIFEST.json')['members']]
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},**{'inputs/'+n:(slices/n).read_bytes() for n in names},'EXPECTED_TECHNICAL.json':encoded(technical),'EXPECTED_SCIENTIFIC.json':encoded(expected),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt)}
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'technical_payloads':len(technical),'scientific_payloads':len(expected),'excluded_parity':['fit_resources.json actual timing; still verified by completion and30sec fit limits']}
def restore(package,digest,destination,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    tech,actual=technical_operator(destination/'source',destination/'inputs',destination/'technical_runs','run')
    if actual!=read(destination/'EXPECTED_TECHNICAL.json'):raise ValueError('technical_reconstruction_mismatch')
    receipt,scientific=operator(destination/'source',Path(tech['run_path']),destination/'runs','run')
    if scientific!=read(destination/'EXPECTED_SCIENTIFIC.json'):raise ValueError('matched_scientific_replay_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_MATCHED_INPUT=tech['run_path'],FOREX_MATCHED_RUNS=str(destination/'runs'))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_matched_campaign_v2.py')],capture_output=True,text=True,timeout=360,env=env)
        (destination/'tests.stdout').write_text(p.stdout);(destination/'tests.stderr').write_text(p.stderr)
        if p.returncode:raise ValueError('matched_relocated_tests_failed:'+p.stdout+p.stderr)
    r={'status':'VERIFIED','checkpoint_sha256':digest,'technical_payload_count':len(actual),'scientific_payload_count':len(scientific),'relocated_tests_passed':run_tests,'actual_operator':receipt,'fit_timings':'bounded_not_bit_identical','full_raw_slice_reconstruction_and_refit':True}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(r,indent=2)+'\n');return r
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','slices','technical-runs','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    result=export(a.package,a.slices,a.technical_runs,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests);print(json.dumps(result,sort_keys=True))
