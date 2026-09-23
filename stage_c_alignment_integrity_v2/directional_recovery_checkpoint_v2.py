"""Small companion plus the original pinned retained archive; exact fresh replay."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import zipfile
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
from directional_recovery_operator_v2 import SOURCES,MANIFEST_SHA,ARCHIVE_SHA,sha,verify_retained
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_retained_directional_companion.v1'
ALLOWLIST=tuple(sorted(set(SOURCES)|{'directional_recovery_checkpoint_v2.py','portable_checkpoint_v2.py','DIRECTIONAL_RECOVERY_OPERATOR_RECIPE.json','DIRECTIONAL_RECOVERY_CONTRACT_V2.md','test_directional_recovery_v2.py'}))

def read(p):return json.loads(p.read_text())
def operator(source,retained,runs,action):
    recipe=source/'DIRECTIONAL_RECOVERY_OPERATOR_RECIPE.json'
    p=subprocess.run([sys.executable,'-I','-B',str(source/'directional_recovery_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',sha(recipe),'--retained-root',str(retained),'--runs-dir',str(runs)],capture_output=True,text=True,timeout=360)
    if p.returncode:raise ValueError('retained_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('retained_operator_incomplete')
    manifest=read(Path(r['run_path'])/'COMPLETION_MANIFEST.json')
    return r,{x['path']:x['sha256'] for x in manifest['payloads']}

def extract_retained(archive,destination):
    if sha(archive)!=ARCHIVE_SHA:raise ValueError('retained_archive_hash_mismatch')
    _plain_destination(destination)
    with zipfile.ZipFile(archive) as z:
        infos=z.infolist();names=[i.filename for i in infos]
        if len(names)!=len(set(n.casefold() for n in names)):raise ValueError('duplicate_retained_member')
        if sum(i.file_size for i in infos)>128*1024*1024 or any(i.file_size>24*1024*1024 for i in infos):raise ValueError('retained_archive_size_bound')
        for i in infos:
            safe_member(i.filename)
            if i.is_dir() or stat.S_ISLNK(i.external_attr>>16):raise ValueError('retained_nonregular_member')
        raw=z.read('MANIFEST.json')
        if hashlib.sha256(raw).hexdigest()!=MANIFEST_SHA:raise ValueError('retained_manifest_hash_mismatch')
        m=json.loads(raw)
        if set(names)!=set(m['files'])|{'MANIFEST.json'}:raise ValueError('retained_inventory_mismatch')
        # Verify every member before writing anything; archive is bounded98MiB.
        for i in infos:
            if i.filename=='MANIFEST.json':continue
            expected=m['files'][i.filename];b=z.read(i)
            if len(b)!=expected['bytes'] or hashlib.sha256(b).hexdigest()!=expected['sha256']:raise ValueError('retained_member_hash_mismatch')
        destination.mkdir(parents=True,exist_ok=True)
        for i in infos:
            p=destination.joinpath(*safe_member(i.filename).parts);p.parent.mkdir(parents=True,exist_ok=True)
            with p.open('xb') as f:f.write(z.read(i))
    verify_retained(destination)

def export(package,retained,runs):
    receipt,expected=operator(ROOT,retained,runs,'verify')
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXPECTED_REPLAY.json':encoded(expected),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),'RETAINED_ARCHIVE.json':encoded({'filename':'local_restoration_001.zip','sha256':ARCHIVE_SHA,'manifest_sha256':MANIFEST_SHA,'required':True})}
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'required_retained_archive_sha256':ARCHIVE_SHA,'payloads':len(expected)}

def restore(package,digest,archive,destination,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if sha(archive)!=ARCHIVE_SHA:raise ValueError('retained_archive_hash_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    extract_retained(archive,destination/'retained')
    receipt,actual=operator(destination/'source',destination/'retained',destination/'runs','run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('retained_restored_payload_parity_failed')
    if run_tests:
        env=dict(os.environ,FOREX_DIRECTIONAL_RETAINED=str(destination/'retained'),FOREX_DIRECTIONAL_RUNS=str(destination/'runs'))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_directional_recovery_v2.py')],capture_output=True,text=True,timeout=360,env=env)
        (destination/'tests.stdout').write_text(p.stdout);(destination/'tests.stderr').write_text(p.stderr)
        if p.returncode:raise ValueError('relocated_directional_tests_failed:'+p.stdout+p.stderr)
    r={'status':'VERIFIED','companion_sha256':digest,'retained_archive_sha256':ARCHIVE_SHA,'actual_operator':receipt,'matched_payload_count':len(actual),'relocated_tests_passed':run_tests,'models_refitted':0}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(r,indent=2)+'\n');return r

if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','retained-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','archive','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    result=export(a.package,a.retained_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.archive,a.destination,a.run_tests)
    print(json.dumps(result,sort_keys=True))
