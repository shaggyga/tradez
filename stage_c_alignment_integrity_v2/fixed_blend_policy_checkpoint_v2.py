"""Portable prepared-model policy replay with existing strict ZIP guards."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from historical_policy_checkpoint_v2 import ALLOWLIST as HISTORICAL_SOURCES
from accounting_checkpoint_v2 import PREDECESSORS,dependency_lock
from matched_policy_checkpoint_v2 import ALLOWLIST as PARENT_ALLOWLIST
from fixed_blend_policy_operator_v2 import NEW,sha
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_fixed_remaining_policy_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(PARENT_ALLOWLIST)|set(NEW)|{'fixed_blend_policy_checkpoint_v2.py','fixed_blend_policy_report_v2.py','FIXED_BLEND_POLICY_OPERATOR_RECIPE_REVIEWED_V2.json','FIXED_BLEND_REMAINING_POLICY_V2.md','test_fixed_blend_remaining_policy_v2.py'}))
def read(p):return json.loads(p.read_text())
def operator(source,input_path,runs,trad,action):
    recipe=source/'FIXED_BLEND_POLICY_OPERATOR_RECIPE_REVIEWED_V2.json';cmd=[sys.executable,'-I','-B',str(source/'fixed_blend_policy_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',sha(recipe),'--input',str(input_path),'--runs-dir',str(runs),'--trad-root',str(trad)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=1200)
    if p.returncode:raise ValueError('matched_policy_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('matched_policy_not_complete')
    return r,{x['run_id']:{p['path']:p['sha256'] for p in read(Path(x['path'])/'COMPLETION_MANIFEST.json')['payloads']} for x in r['runs']}
def export(package,input_path,runs,trad):
    receipt,expected=operator(ROOT,input_path,runs,trad,'verify')
    from fixed_blend_policy_report_v2 import build
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},**{'trad/'+n:(trad/n).read_bytes() for n in PREDECESSORS},'inputs/MATCHED_POLICY_INPUTS.json':input_path.read_bytes(),'EXPECTED_REPLAY.json':encoded(expected),'DEPENDENCIES.json':encoded(dependency_lock()),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),
        'MODEL_LINEAGE.json':encoded({'checkpoint':'MATCHED_REMAINING_20260922_041223/checkpoint/forex_matched_remaining.zip','sha256':'2fca12f0c7d6ab5085718203c1c4b8e1598db748479a749d7deca51f2a13cbb8','required_for':'raw-slice-to-fitted-model lineage reconstruction; current prepared-policy replay is self-contained'})}
    payloads['EXPECTED_REPORT.json']=encoded(build(runs,{x['run_id']:x['run_identity'] for x in receipt['runs']}))
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'runs':len(expected),'payload_count':sum(map(len,expected.values()))}
def restore(package,digest,destination,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['DEPENDENCIES.json'])!=dependency_lock():raise ValueError('matched_policy_environment_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    receipt,actual=operator(destination/'source',destination/'inputs/MATCHED_POLICY_INPUTS.json',destination/'runs',destination/'trad','run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('matched_policy_relocated_payload_mismatch')
    report_receipt=destination/'REPORT_OPERATOR_RECEIPT.json';report_receipt.write_bytes(encoded(receipt))
    report_command=[sys.executable,'-I','-B',str(destination/'source/fixed_blend_policy_report_v2.py'),'--runs-dir',str(destination/'runs'),'--output',str(destination/'report'),'--operator-receipt',str(report_receipt),'--receipt-sha256',sha(report_receipt)]
    reported=subprocess.run(report_command,capture_output=True,text=True,timeout=120)
    if reported.returncode or read(destination/'report/RESULT_SUMMARY.json')!=read(destination/'EXPECTED_REPORT.json'):
        raise ValueError('fixed_blend_relocated_report_mismatch:'+reported.stdout+reported.stderr)
    if run_tests:
        env=dict(os.environ,FOREX_MATCHED_POLICY_INPUT=str(destination/'inputs/MATCHED_POLICY_INPUTS.json'),FOREX_MATCHED_POLICY_TRAD=str(destination/'trad'))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_fixed_blend_remaining_policy_v2.py')],capture_output=True,text=True,timeout=1200,env=env)
        (destination/'tests.stdout').write_text(p.stdout);(destination/'tests.stderr').write_text(p.stderr)
        if p.returncode:raise ValueError('matched_policy_relocated_tests_failed:'+p.stdout+p.stderr)
    r={'status':'VERIFIED','checkpoint_sha256':digest,'actual_operator':receipt,'run_count':len(actual),'payload_count':sum(map(len,actual.values())),'relocated_report_identical':True,'relocated_tests_passed':run_tests}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(r,indent=2)+'\n');return r
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','input','runs-dir','trad-root'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.input,a.runs_dir,a.trad_root) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
