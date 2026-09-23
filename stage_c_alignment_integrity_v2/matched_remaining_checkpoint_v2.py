"""Raw-slice-to-native portable reconstruction of the matched remaining recipe."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from matched_remaining_operator_v2 import SOURCES,PREDECESSORS,sha
from matched_campaign_checkpoint_v2 import ALLOWLIST as CAMPAIGN_SOURCES,operator as matched_operator,technical_operator
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_matched_remaining_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(SOURCES)|set(CAMPAIGN_SOURCES)|{'matched_remaining_checkpoint_v2.py','MATCHED_REMAINING_OPERATOR_RECIPE.json','MATCHED_REMAINING_CONTRACT_V2.md','test_matched_remaining_v2.py'}))
def read(p):return json.loads(p.read_text())
def operator(source,slices,technical,matched,trad,runs,action):
    recipe=source/'MATCHED_REMAINING_OPERATOR_RECIPE.json'
    cmd=[sys.executable,'-I','-B',str(source/'matched_remaining_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',sha(recipe),'--slices',str(slices),'--technical',str(technical),'--matched',str(matched),'--trad-root',str(trad),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=360)
    if p.returncode:raise ValueError('remaining_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('remaining_operator_incomplete')
    m=read(Path(r['run_path'])/'COMPLETION_MANIFEST.json');return r,{x['path']:x['sha256'] for x in m['payloads'] if x['path']!='fit_resources.json'}
def export(package,slices,technical_runs,matched_runs,runs,trad):
    tech,th=technical_operator(ROOT,slices,technical_runs,'verify');matched,mh=matched_operator(ROOT,Path(tech['run_path']),matched_runs,'verify')
    receipt,expected=operator(ROOT,slices,Path(tech['run_path']),Path(matched['run_path']),trad,runs,'verify')
    names=['SLICES_MANIFEST.json']+[r['path'] for r in read(slices/'SLICES_MANIFEST.json')['members']]
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},**{'trad/'+n:(trad/n).read_bytes() for n in PREDECESSORS},**{'inputs/'+n:(slices/n).read_bytes() for n in names},'EXPECTED_TECHNICAL.json':encoded(th),'EXPECTED_MATCHED.json':encoded(mh),'EXPECTED_REMAINING.json':encoded(expected),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt)}
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'technical_payloads':len(th),'matched_payloads':len(mh),'remaining_payloads':len(expected),'timing_parity_excluded':True}
def restore(package,digest,destination,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    tech,th=technical_operator(destination/'source',destination/'inputs',destination/'technical_runs','run')
    if th!=read(destination/'EXPECTED_TECHNICAL.json'):raise ValueError('technical_replay_mismatch')
    matched,mh=matched_operator(destination/'source',Path(tech['run_path']),destination/'matched_runs','run')
    if mh!=read(destination/'EXPECTED_MATCHED.json'):raise ValueError('matched_replay_mismatch')
    receipt,remaining=operator(destination/'source',destination/'inputs',Path(tech['run_path']),Path(matched['run_path']),destination/'trad',destination/'runs','run')
    if remaining!=read(destination/'EXPECTED_REMAINING.json'):raise ValueError('remaining_scientific_replay_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_REMAINING_SLICES=str(destination/'inputs'),FOREX_REMAINING_TECHNICAL=tech['run_path'],FOREX_REMAINING_MATCHED=matched['run_path'],FOREX_REMAINING_TRAD=str(destination/'trad'),FOREX_REMAINING_RUNS=str(destination/'runs'))
        env.update(FOREX_MATCHED_INPUT=tech['run_path'],FOREX_MATCHED_RUNS=str(destination/'matched_runs'))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_matched_campaign_v2.py'),str(destination/'source/test_matched_remaining_v2.py')],capture_output=True,text=True,timeout=360,env=env)
        (destination/'tests.stdout').write_text(p.stdout);(destination/'tests.stderr').write_text(p.stderr)
        if p.returncode:raise ValueError('remaining_relocated_tests_failed:'+p.stdout+p.stderr)
    r={'status':'VERIFIED','checkpoint_sha256':digest,'technical_payload_count':len(th),'matched_payload_count':len(mh),'remaining_payload_count':len(remaining),'relocated_tests_passed':run_tests,'actual_operator':receipt,'full_raw_slice_reconstruction':True,'fit_timings':'bounded_not_bit_identical'}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(r,indent=2)+'\n');return r
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','slices','technical-runs','matched-runs','runs-dir','trad-root'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    result=export(a.package,a.slices,a.technical_runs,a.matched_runs,a.runs_dir,a.trad_root) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests);print(json.dumps(result,sort_keys=True))
