"""Portable zero-fit replay of the frozen later policy matrix."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import stat
import subprocess
import sys
import zipfile

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import chronological_policy_operator_v2 as op
from portable_checkpoint_v2 import _plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file,verify_completed_run

SCHEMA='forex_chronological_policy_checkpoint.v1'


def allowlist():
    return sorted(set(op.source_names())|{op.RECIPE,'chronological_policy_checkpoint_v2.py','portable_checkpoint_v2.py',
        'chronological_policy_report_v2.py','test_chronological_policy_v2.py','test_chronological_policy_report_v2.py','CHRONOLOGICAL_POLICY_V2.md'})


def expected_paths(recipe):
    paths={'source/'+n for n in allowlist()}|{'EXPECTED_REPLAY.json','EXPECTED_REPORT.json','ORIGINAL_OPERATOR_RECEIPT.json','CAPSULE_SCOPE.json'}
    for alias,dep in recipe['dependencies'].items():
        paths.update('capsule/'+alias+'/'+n for n in set(dep['payloads'])|{'RUN_IDENTITY.json','COMPLETION_MANIFEST.json'})
    paths.update('trad/'+n for n in recipe['predecessors'])
    return paths


def inspect_capsule(package,digest):
    if package.is_symlink() or package.stat().st_size>128*1024**2 or sha256_file(package)!=digest:
        raise ValueError('chronological_policy_bounded_pinned_capsule_required')
    with zipfile.ZipFile(package) as z:
        infos=z.infolist();names=[r.filename for r in infos]
        if len(names)>512 or len(set(names))!=len(names) or sum(r.file_size for r in infos)>384*1024**2:
            raise ValueError('chronological_policy_bounded_unique_capsule_inventory_required')
        for info in infos:
            safe_member(info.filename)
            if info.file_size>16*1024**2 or info.is_dir() or stat.S_ISLNK(info.external_attr>>16) or info.flag_bits&1:
                raise ValueError('chronological_policy_plain_bounded_capsule_member_required')
        if MANIFEST not in names or z.getinfo(MANIFEST).file_size>1048576:raise ValueError('chronological_policy_bounded_manifest_required')
        manifest=json.loads(z.read(MANIFEST));rows=manifest['members'];expected={r['path']:r for r in rows}
        if manifest['schema_version']!=SCHEMA or manifest['source_allowlist']!=allowlist():raise ValueError('chronological_policy_capsule_schema_allowlist_mismatch')
        if len(expected)!=len(rows) or set(names)!=set(expected)|{MANIFEST}:raise ValueError('chronological_policy_capsule_exact_members_required')
        data={}
        for n,r in expected.items():
            raw=z.read(n)
            if len(raw)!=r['bytes'] or hashlib.sha256(raw).hexdigest()!=r['sha256']:raise ValueError('chronological_policy_capsule_member_changed')
            data[n]=raw
        recipe=json.loads(data['source/'+op.RECIPE])
        if set(data)!=expected_paths(recipe):raise ValueError('chronological_policy_declared_capsule_paths_required')
        data[MANIFEST]=z.read(MANIFEST);return data


def invoke(source,paths,runs,action):
    recipe=source/op.RECIPE
    command=[sys.executable,'-X','utf8','-I','-B',str(source/'chronological_policy_operator_v2.py'),action,
        '--recipe',str(recipe),'--recipe-sha256',sha256_file(recipe),'--paths',str(paths),'--runs-dir',str(runs)]
    result=subprocess.run(command,capture_output=True,text=True,timeout=4800)
    if result.returncode:raise ValueError('chronological_policy_checkpoint_operator_failed:'+result.stdout+result.stderr)
    receipt=json.loads(result.stdout)
    if receipt['status']!='completed_verified' or len(receipt['runs'])!=112:raise ValueError('chronological_policy_checkpoint_incomplete_matrix')
    if any(receipt[k]!=0 for k in ('base_model_fits','base_model_loads','layer_fits','api_calls')):
        raise ValueError('chronological_policy_checkpoint_no_model_work_required')
    expected={}
    for row in receipt['runs']:
        root=Path(row['path']);identity=op.read(root/'RUN_IDENTITY.json')
        if identity['fingerprint']!=row['run_identity']:raise ValueError('chronological_policy_checkpoint_run_identity_mismatch')
        manifest=verify_completed_run(root,identity)
        expected[row['run_id']]={r['path']:r['sha256'] for r in manifest['payloads']}
    return receipt,expected


def report(source,runs,receipt_path,output):
    result=subprocess.run([sys.executable,'-X','utf8','-I','-B',str(source/'chronological_policy_report_v2.py'),
        '--runs-dir',str(runs),'--operator-receipt',str(receipt_path),'--receipt-sha256',sha256_file(receipt_path),
        '--output',str(output)],capture_output=True,text=True,timeout=600)
    if result.returncode:raise ValueError('chronological_policy_checkpoint_report_failed:'+result.stdout+result.stderr)
    return {n:sha256_file(output/n) for n in ('RESULT_SUMMARY.json','RESULT_SUMMARY.md')}


def export(package,paths,runs,report_directory):
    receipt,expected=invoke(ROOT,paths,runs,'verify');recipe=op.read(ROOT/op.RECIPE);inputs=op.read(paths)
    values={'source/'+n:(ROOT/n).read_bytes() for n in allowlist()}
    for alias,dep in recipe['dependencies'].items():
        for n in set(dep['payloads'])|{'RUN_IDENTITY.json','COMPLETION_MANIFEST.json'}:
            values['capsule/'+alias+'/'+n]=(Path(inputs[alias])/n).read_bytes()
    for n in recipe['predecessors']:values['trad/'+n]=(Path(inputs['trad'])/n).read_bytes()
    expected_report={n:sha256_file(report_directory/n) for n in ('RESULT_SUMMARY.json','RESULT_SUMMARY.md')}
    values.update({'EXPECTED_REPLAY.json':encoded(expected),'EXPECTED_REPORT.json':encoded(expected_report),
        'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),'CAPSULE_SCOPE.json':encoded({
            'matrix_runs':112,'base_model_fits':0,'base_model_loads':0,'layer_fits':0,
            'inputs':'selected_native_frames_and_extension_records_bound_to_original_complete_manifests; subsets_are_not_complete_parent_runs',
            'outputs':'all_policy_state_snapshots_replayed_locally_not_stored_as_duplicate_archive',
            'test_pytest_version':importlib.metadata.version('pytest'),
            'scope':'same_machine_relocated_replay; no_other_machine_or_cloud_sync_claim'})})
    manifest={'schema_version':SCHEMA,'source_allowlist':allowlist(),
        'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(values.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=sha256_file(package);inspect_capsule(package,digest)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'files':len(values)+1,'policy_runs':112,
        'scientific_payloads':sum(len(v) for v in expected.values()),'base_fits_allowed':False,'base_loads_allowed':False,
        'expected_report':expected_report}


def restore(package,digest,destination,run_tests=False):
    data=inspect_capsule(package,digest);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True);destination=destination.resolve()
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    paths={k:str(destination/'capsule'/k) for k in ('native','extension')};paths['trad']=str(destination/'trad')
    pf=destination/'PATHS.json';pf.write_bytes(encoded(paths))
    receipt,actual=invoke(destination/'source',pf,destination/'runs','run')
    if actual!=op.read(destination/'EXPECTED_REPLAY.json'):raise ValueError('chronological_policy_relocated_payload_mismatch')
    rp=destination/'RESTORE_OPERATOR_RECEIPT.json';rp.write_bytes(encoded(receipt))
    actual_report=report(destination/'source',destination/'runs',rp,destination/'report')
    if actual_report!=op.read(destination/'EXPECTED_REPORT.json'):raise ValueError('chronological_policy_relocated_report_mismatch')
    if run_tests:
        if importlib.metadata.version('pytest')!=op.read(destination/'CAPSULE_SCOPE.json')['test_pytest_version']:
            raise ValueError('chronological_policy_test_framework_environment_mismatch')
        result=subprocess.run([sys.executable,'-X','utf8','-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',
            str(destination/'source/test_chronological_policy_v2.py'),str(destination/'source/test_chronological_policy_report_v2.py')],
            capture_output=True,text=True,timeout=180)
        (destination/'tests.stdout').write_text(result.stdout+result.stderr,encoding='utf-8')
        if result.returncode:raise ValueError('chronological_policy_relocated_tests_failed:'+result.stdout+result.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'runs_identical':len(actual),
        'scientific_payloads_identical':sum(len(v) for v in actual.values()),'report_hashes':actual_report,
        'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'relocated_tests_passed':run_tests,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(encoded(result));return result


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','paths','runs-dir','report-directory'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.runs_dir,a.report_directory) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
