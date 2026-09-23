"""Standalone matched-population replay using the existing bounded checkpoint reader."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import macro_matched_operator_v2 as op
from portable_checkpoint_v2 import inspect_package, _plain_destination, safe_member, MANIFEST

SCHEMA = 'forex_macro_matched_checkpoint.v2'
ALLOWLIST = tuple(sorted(set(op.SOURCES) | {'macro_matched_checkpoint_v2.py', 'portable_checkpoint_v2.py',
                         'MACRO_MATCHED_OPERATOR_RECIPE.json', 'MACRO_MATCHED_CONTRACT_V2.md', 'test_macro_matched_v2.py'}))


def inventory(root):
    return {x['path']: x['sha256'] for x in op.read(root/'COMPLETION_MANIFEST.json')['payloads']}


def invoke(source, inputs, runs, action):
    recipe = source/'MACRO_MATCHED_OPERATOR_RECIPE.json'
    p = subprocess.run([sys.executable, '-I', '-B', str(source/'macro_matched_operator_v2.py'), action,
                        '--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--inputs',str(inputs),
                        '--runs-dir',str(runs)], capture_output=True,text=True,timeout=120)
    if p.returncode:
        raise ValueError('macro_matched_replay_failed:'+p.stdout+p.stderr)
    receipt = json.loads(p.stdout)
    if receipt['status'] != 'completed_verified':
        raise ValueError('macro_matched_replay_incomplete')
    return receipt


def inspect(package,digest):
    data = inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    expected = {'source/'+n for n in ALLOWLIST} | {'inputs/'+n for n in json.loads(data['source/MACRO_MATCHED_OPERATOR_RECIPE.json'])['inputs']} | {MANIFEST,'EXPECTED_REPLAY.json'}
    if set(data) != expected:
        raise ValueError('exact_macro_matched_checkpoint_inventory_required')
    return data


def export(package,inputs,runs):
    receipt = invoke(ROOT,inputs,runs,'verify')
    values = {'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST}
    values.update({'inputs/'+n:(inputs/n).read_bytes() for n in op.input_names(inputs)})
    values['EXPECTED_REPLAY.json'] = op.encoded(inventory(Path(receipt['run_path'])))
    manifest = {'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),
                'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(values.items()):z.writestr(n,b)
        z.writestr(MANIFEST,op.encoded(manifest))
    digest = op.sha(package)
    inspect(package,digest)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'members':len(values)+1}


def restore(package,digest,destination,run_tests=False):
    data = inspect(package,digest)
    _plain_destination(destination)
    destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p = destination.joinpath(*safe_member(n).parts)
        p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    receipt = invoke(destination/'source',destination/'inputs',destination/'runs','run')
    actual = inventory(Path(receipt['run_path']))
    if actual != json.loads(data['EXPECTED_REPLAY.json']):
        raise ValueError('macro_matched_relocated_payload_mismatch')
    if run_tests:
        env = dict(os.environ,FOREX_MACRO_MATCHED_INPUTS=str(destination/'inputs'),FOREX_MACRO_MATCHED_RUNS=str(destination/'runs'))
        p = subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_macro_matched_v2.py')],
                           capture_output=True,text=True,env=env,timeout=180)
        (destination/'tests.log').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('macro_matched_relocated_tests_failed:'+p.stdout+p.stderr)
    result = {'status':'VERIFIED','checkpoint_sha256':digest,'payloads_identical':len(actual),'relocated_tests_passed':run_tests,'operator':receipt,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(op.encoded(result))
    return result


if __name__ == '__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True)
    e=s.add_parser('export')
    for n in ('package','inputs','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.inputs,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests)))
