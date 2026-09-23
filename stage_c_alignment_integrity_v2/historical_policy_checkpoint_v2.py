"""Guarded portable candle-policy qualification with exact four-run replay."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from accounting_checkpoint_v2 import SOURCE_ALLOWLIST as BASE,PREDECESSORS,dependency_lock
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded

SCHEMA='forex_historical_policy_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(BASE)|{'historical_policy_checkpoint_v2.py','historical_policy_operator_v2.py',
    'historical_policy_fixture_v2.py','historical_native_input_v2.py','historical_market_inputs_v2.py',
    'remaining_horizon_v2.py','all68_neutral_runner_v2.py','quote_input_qualification_v2.py',
    'HISTORICAL_POLICY_OPERATOR_RECIPE.json','HISTORICAL_POLICY_CONTRACT_V2.md','test_historical_policy_v2.py'}))


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path):return json.loads(path.read_text(encoding='utf-8'))


def operator(source,input_path,runs,trad,action):
    recipe=source/'HISTORICAL_POLICY_OPERATOR_RECIPE.json'
    cmd=[sys.executable,'-I','-B',str(source/'historical_policy_operator_v2.py'),action,
        '--recipe',str(recipe),'--recipe-sha256',sha(recipe),'--input',str(input_path),
        '--runs-dir',str(runs),'--trad-root',str(trad)]
    proc=subprocess.run(cmd,capture_output=True,text=True,timeout=360)
    if proc.returncode:raise ValueError('historical_policy_operator_failed:'+proc.stdout+proc.stderr)
    receipt=json.loads(proc.stdout)
    if receipt['status']!='completed_verified':raise ValueError('historical_policy_not_completed')
    expected={}
    for row in receipt['runs']:
        root=Path(row['path']);manifest=read(root/'COMPLETION_MANIFEST.json')
        expected[row['run_id']]={p['path']:sha(root/p['path']) for p in manifest['payloads']}
    return receipt,expected


def export(package,input_path,runs,trad):
    receipt,expected=operator(ROOT,input_path,runs,trad,'verify')
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},
        **{'trad/'+n:(trad/n).read_bytes() for n in PREDECESSORS},
        'inputs/POLICY_INPUTS.json':input_path.read_bytes(),
        'EXPECTED_REPLAY.json':encoded(expected),'DEPENDENCIES.json':encoded(dependency_lock()),
        'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),
        'README.md':b'Offline historical candle scenarios, not observed execution or broker authorization. Verify the external ZIP SHA before bootstrapping trusted source/historical_policy_checkpoint_v2.py restore. Use --run-tests. Source and pinned predecessors, prepared inputs and exact replay hashes are included. Raw archive, credentials, environment and full run snapshots are excluded; restore regenerates runs. See source/HISTORICAL_POLICY_CONTRACT_V2.md.\n'}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),
        'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    package.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for name,raw in sorted(payloads.items()):z.writestr(name,raw)
        z.writestr(MANIFEST,encoded(manifest))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'members':len(payloads),'run_count':len(expected),
            'raw_validation_runs':str(runs),'raw_runs_included':False}


def restore(package,destination,digest,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['DEPENDENCIES.json'])!=dependency_lock():raise ValueError('checkpoint_environment_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for name,raw in data.items():
        p=destination.joinpath(*safe_member(name).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(raw)
    receipt,actual=operator(destination/'source',destination/'inputs/POLICY_INPUTS.json',destination/'runs',destination/'trad','run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('historical_restored_payload_parity_failed')
    if run_tests:
        env=dict(os.environ,FOREX_HISTORICAL_POLICY_INPUT=str(destination/'inputs/POLICY_INPUTS.json'))
        result=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',
            str(destination/'source/test_historical_policy_v2.py')],env=env,capture_output=True,text=True,timeout=360)
        (destination/'tests.stdout').write_text(result.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(result.stderr,encoding='utf-8')
        if result.returncode:raise ValueError('historical_restored_tests_failed:'+result.stdout+result.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'actual_frozen_operator':receipt,
        'run_count':len(actual),'matched_payload_count':sum(len(x) for x in actual.values()),
        'relocated_tests_passed':run_tests,'observed_historical_execution':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='action',required=True)
    e=sub.add_parser('export')
    for n in ('package','input','runs-dir','trad-root'):e.add_argument('--'+n,type=Path,required=True)
    r=sub.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    result=export(a.package,a.input,a.runs_dir,a.trad_root) if a.action=='export' else restore(a.package,a.destination,a.sha256,a.run_tests)
    print(json.dumps(result,sort_keys=True))
