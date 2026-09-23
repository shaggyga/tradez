"""Portable historical slice: pinned inputs, frozen recipe, scientific replay hashes.

Wall-clock fit timings are retained run evidence, not deterministic replay targets.
Restore uses the existing guarded ZIP boundary before loading packaged runtime code.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded,environment_lock

SCHEMA='forex_historical_checkpoint.v1'
ALLOWLIST=('historical_checkpoint_v2.py','historical_operator_v2.py','historical_runner_v2.py','historical_slice_v2.py',
 'historical_inputs_v2.py','fitted_consumer_v2.py','publication.py','contracts.py','portable_checkpoint_v2.py','test_historical_slice_v2.py')
INPUT_NAMES=('PREFLIGHT_CONTRACT.json','PREFLIGHT_REPORT.json','historical_observations.jsonl','historical_outcomes.jsonl')
SCIENTIFIC=('run_inputs.json','models.json','run_report.json','policy_admission.json',
 *(f'{kind}_{i}.jsonl' for i in range(3) for kind in ('forecasts','coverage','settlements')))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))

def operator(root,input_root,runs,action='verify'):
    recipe=root/'HISTORICAL_OPERATOR_RECIPE.json'
    cmd=[sys.executable,'-I','-B',str(root/'historical_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',sha(recipe),
      '--input-root',str(input_root),'--contract',str(root/'HISTORICAL_SLICE_CONTRACT.json'),'--runs-dir',str(runs)]
    result=subprocess.run(cmd,capture_output=True,text=True,timeout=180)
    if result.returncode:raise ValueError('packaged_historical_operator_failed:'+result.stdout+result.stderr)
    receipt=json.loads(result.stdout)
    if action in ('run','verify') and receipt['status']!='completed_verified':raise ValueError('historical_completion_required')
    return receipt

def export(package,input_root,runs):
    receipt=operator(ROOT,input_root,runs)
    run=Path(receipt['run_path']);expected={name:sha(run/name) for name in SCIENTIFIC}
    payloads={**{'engine/'+name:(ROOT/name).read_bytes() for name in (*ALLOWLIST,'HISTORICAL_OPERATOR_RECIPE.json','HISTORICAL_SLICE_CONTRACT.json','HISTORICAL_CONTRACT_V2.md')},
      **{'inputs/'+name:(input_root/name).read_bytes() for name in INPUT_NAMES},
      'EXPECTED_SCIENTIFIC_HASHES.json':encoded(expected),'DEPENDENCIES.json':encoded(environment_lock()),
      'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),
      'README.md':b'Historical development slice only. Verify external ZIP SHA before bootstrapping trusted historical_checkpoint_v2.py restore. No bulk archive, broker access, credentials, execution evidence or campaign authorization. See engine/HISTORICAL_CONTRACT_V2.md.\n'}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()} for name,raw in sorted(payloads.items())]}
    package.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as archive:
        for name,raw in sorted(payloads.items()):archive.writestr(name,raw)
        archive.writestr(MANIFEST,encoded(manifest))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'members':len(payloads),'scientific_payloads':len(expected)}

def restore(package,destination,digest,run_tests=False):
    payloads=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for name,raw in payloads.items():
        path=destination.joinpath(*safe_member(name).parts);path.parent.mkdir(parents=True,exist_ok=True)
        with path.open('xb') as f:f.write(raw)
    receipt=operator(destination/'engine',destination/'inputs',destination/'runs','run')
    verified=operator(destination/'engine',destination/'inputs',destination/'runs','verify')
    expected=read(destination/'EXPECTED_SCIENTIFIC_HASHES.json');run=Path(receipt['run_path'])
    if set(expected)!=set(SCIENTIFIC):raise ValueError('scientific_inventory_mismatch')
    for name,digest_expected in expected.items():
        if sha(run/name)!=digest_expected:raise ValueError('relocated_scientific_payload_mismatch:'+name)
    if run_tests:
        env=dict(os.environ,FOREX_HISTORICAL_TEST_INPUT=str(destination/'inputs'))
        result=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'engine/test_historical_slice_v2.py')],env=env,capture_output=True,text=True,timeout=360)
        (destination/'tests.stdout').write_text(result.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(result.stderr,encoding='utf-8')
        if result.returncode:raise ValueError('relocated_historical_tests_failed:'+result.stdout+result.stderr)
    report={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(expected),'actual_frozen_operator':verified,
      'relocated_tests_passed':run_tests,'timing_scope':'fit elapsed times verified within declared latency but not required bit-identical across runs'}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='action',required=True)
    e=sub.add_parser('export');e.add_argument('--package',type=Path,required=True);e.add_argument('--input-root',type=Path,required=True);e.add_argument('--runs-dir',type=Path,required=True)
    r=sub.add_parser('restore');r.add_argument('--package',type=Path,required=True);r.add_argument('--destination',type=Path,required=True);r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true')
    a=p.parse_args();result=export(a.package,a.input_root,a.runs_dir) if a.action=='export' else restore(a.package,a.destination,a.sha256,a.run_tests)
    print(json.dumps(result,sort_keys=True))
