"""Compact rich-input recovery; reuse the preserved raw archive instead of copying it."""
import argparse,hashlib,importlib.metadata,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import rich_campaign_operator_v2 as op
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded,environment_lock
from publication import package_completed_run,restore_completed_run
SCHEMA='forex_rich_campaign_inputs_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(op.SOURCES)|{'portable_checkpoint_v2.py','rich_campaign_checkpoint_v2.py','RICH_CAMPAIGN_INPUT_OPERATOR_RECIPE.json','RICH_CAMPAIGN_INPUT_CONTRACT_V2.md','ORIGINAL_ROLLING_SAMPLE_HASHES.json','test_rich_campaign_inputs_v2.py'}))
ARCHIVE_SHA='aca163639a18e22b2c60dc4ff36d55d82c655af617836a6d351dd1339e5a6006'
def lock():return {**environment_lock(),'rich_packages':{n:importlib.metadata.version(n) for n in ('pandas','pytest')}}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def guard_nested(package):
    with zipfile.ZipFile(package) as z:
        infos=z.infolist()
        if len(infos)!=72 or sum(x.file_size for x in infos)>32*1024*1024 or any(x.file_size>8*1024*1024 for x in infos):raise ValueError('bounded_original_technical_package_required')
        if any(Path(x.filename).name!=x.filename for x in infos):raise ValueError('plain_original_technical_members_required')
def invoke(source,root,contract,lineage,technical,trad,runs,action):
    recipe=source/'RICH_CAMPAIGN_INPUT_OPERATOR_RECIPE.json';cmd=[sys.executable,'-I','-B',str(source/'rich_campaign_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--input-root',str(root),'--preparation-contract',str(contract),'--lineage',str(lineage),'--technical',str(technical),'--trad-root',str(trad),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600)
    if p.returncode:raise ValueError('rich_checkpoint_operator_failed:'+p.stdout+p.stderr)
    r=json.loads(p.stdout)
    if r['status']!='completed_verified':raise ValueError('rich_checkpoint_requires_verified_run')
    return r,{x['path']:x['sha256'] for x in read(Path(r['run_path'])/'COMPLETION_MANIFEST.json')['payloads']}
def export(package,root,contract,lineage,technical,trad,runs):
    receipt,expected=invoke(ROOT,root,contract,lineage,technical,trad,runs,'verify')
    nested=package.with_name(package.stem+'_technical.zip');package_completed_run(technical,nested);guard_nested(nested)
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},**{'trad/'+n:(trad/n).read_bytes() for n in op.PREDECESSORS},
        'support/PREPARATION_CONTRACT.json':contract.read_bytes(),'support/LINEAGE_SUMMARY.json':lineage.read_bytes(),'support/LINEAGE_REFERENCE.json':lineage.with_name('LINEAGE_REFERENCE.json').read_bytes(),
        'technical_dependency.zip':nested.read_bytes(),'EXPECTED_REPLAY.json':encoded(expected),'DEPENDENCIES.json':encoded(lock()),
        'REQUIRED_ARCHIVE.json':encoded({'sha256':ARCHIVE_SHA,'vault_path':'RECOVERY_CHECKPOINT_20260921/inputs/long_m1_68.zip','included':False,'reason':'reuse existing sealed archive; reconstruct exact richer slices without bulk duplication'})}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'rich_payloads':len(expected),'preserved_technical_payloads':70,'required_archive_sha256':ARCHIVE_SHA,'raw_archive_duplicated':False}
def restore(package,digest,destination,archive,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['DEPENDENCIES.json'])!=lock():raise ValueError('rich_restore_environment_mismatch')
    from publication import sha256_file
    if json.loads(data['REQUIRED_ARCHIVE.json'])['sha256']!=ARCHIVE_SHA or sha256_file(archive)!=ARCHIVE_SHA:raise ValueError('required_rich_raw_archive_hash_mismatch')
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    source=destination/'source';support=destination/'support';recipe=read(source/'RICH_CAMPAIGN_INPUT_OPERATOR_RECIPE.json');nested=destination/'technical_dependency.zip';guard_nested(nested)
    restore_completed_run(nested,destination/'technical',recipe['technical_identity'],expected_sha256=op.sha(nested))
    code="from pathlib import Path;import sys,json;sys.path.insert(0,sys.argv[1]);from rich_campaign_prepare_v2 import prepare;print(json.dumps(prepare(Path(sys.argv[2]),Path(sys.argv[3]),Path(sys.argv[4]))))"
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(source),str(archive),str(support/'PREPARATION_CONTRACT.json'),str(destination/'inputs')],capture_output=True,text=True,timeout=360)
    (destination/'preparation.stdout').write_text(p.stdout,encoding='utf-8');(destination/'preparation.stderr').write_text(p.stderr,encoding='utf-8')
    if p.returncode:raise ValueError('rich_relocated_raw_preparation_failed:'+p.stdout+p.stderr)
    if op.sha(destination/'inputs/INPUT_MANIFEST.json')!=recipe['raw_manifest_sha256']:raise ValueError('rich_reconstructed_input_manifest_mismatch')
    r,actual=invoke(source,destination/'inputs',support/'PREPARATION_CONTRACT.json',support/'LINEAGE_SUMMARY.json',destination/'technical',destination/'trad',destination/'runs','run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('rich_relocated_payload_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_RICH_INPUT=str(destination/'inputs'),FOREX_RICH_CONTRACT=str(support/'PREPARATION_CONTRACT.json'),FOREX_RICH_LINEAGE=str(support/'LINEAGE_SUMMARY.json'),FOREX_RICH_TECHNICAL=str(destination/'technical'),FOREX_RICH_TRAD=str(destination/'trad'),FOREX_RICH_RUNS=str(destination/'runs'))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_rich_campaign_inputs_v2.py')],capture_output=True,text=True,timeout=1800,env=env)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('rich_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'payload_count':len(actual),'technical_payloads_restored_verified':70,'raw_slice_manifest_reproduced':True,'relocated_tests_passed':run_tests,'actual_operator':r,'raw_archive_copied':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2),encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','input-root','preparation-contract','lineage','technical','trad-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination','archive'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.input_root,a.preparation_contract,a.lineage,a.technical,a.trad_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.archive,a.run_tests),sort_keys=True))
