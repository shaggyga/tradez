"""Portable native timing qualification of saved chronological forecasts; no base refitting."""
import argparse,hashlib,importlib.metadata,json,stat,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import chronological_native_operator_v2 as op
from portable_checkpoint_v2 import safe_member,_plain_destination,MANIFEST,encoded
from publication import sha256_file,verify_completed_run
SCHEMA='forex_chronological_native_checkpoint.v1'


def sources():return sorted(set(op.source_names())|{op.RECIPE,'chronological_native_checkpoint_v2.py','portable_checkpoint_v2.py',
    'test_chronological_native_v2.py','test_chronological_layer_v2.py','test_magnitude_layer_v2.py','CHRONOLOGICAL_NATIVE_V2.md'})


def members(r):return {'source/'+n for n in sources()}|{'inputs/'+a+'/'+n for a,d in r['inputs'].items() for n in d['files']}|{'trad/'+n for n in r['predecessors']}|{'EXPECTED.json','SCOPE.json'}


def inspect(package,pin):
    if package.is_symlink() or package.stat().st_size>134217728 or sha256_file(package)!=pin:raise ValueError('chronological_checkpoint_pin_or_size')
    with zipfile.ZipFile(package) as z:
        infos=z.infolist();names=[i.filename for i in infos]
        if len(names)>512 or len(set(n.casefold() for n in names))!=len(names) or sum(i.file_size for i in infos)>268435456:
            raise ValueError('chronological_checkpoint_inventory_bounds')
        for i in infos:
            safe_member(i.filename)
            if i.file_size>16777216 or i.is_dir() or stat.S_ISLNK(i.external_attr>>16) or i.flag_bits&1:raise ValueError('chronological_checkpoint_plain_members')
        if MANIFEST not in names or z.getinfo(MANIFEST).file_size>1048576:raise ValueError('chronological_checkpoint_manifest_bounds')
        manifest=json.loads(z.read(MANIFEST));records={x['path']:x for x in manifest['members']}
        if manifest['schema_version']!=SCHEMA or len(records)!=len(manifest['members']) or set(names)!=set(records)|{MANIFEST}:raise ValueError('chronological_checkpoint_exact_inventory')
        data={}
        for n,r in records.items():
            raw=z.read(n)
            if len(raw)!=r['bytes'] or hashlib.sha256(raw).hexdigest()!=r['sha256']:raise ValueError('chronological_checkpoint_member_changed')
            data[n]=raw
        recipe=json.loads(data['source/'+op.RECIPE])
        if set(data)!=members(recipe):raise ValueError('chronological_checkpoint_declared_members')
        data[MANIFEST]=z.read(MANIFEST);return data


def invoke(source,paths,runs,action):
    recipe=source/op.RECIPE
    p=subprocess.run([sys.executable,'-X','utf8','-I','-B',str(source/'chronological_native_operator_v2.py'),action,
        '--recipe',str(recipe),'--recipe-sha256',sha256_file(recipe),'--paths',str(paths),'--runs-dir',str(runs)],capture_output=True,text=True,timeout=1500)
    if p.returncode:raise ValueError('chronological_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified' or any(receipt[k] for k in ('base_model_fits','policy_replays','api_calls')):raise ValueError('chronological_checkpoint_zero_model_completion_required')
    if action=='run' and (receipt['base_model_loads']!=16 or receipt['qualification_regressions']!=64):raise ValueError('chronological_checkpoint_full_replay_required')
    root=runs/op.read(recipe)['run_id'];identity=op.read(root/'RUN_IDENTITY.json')
    if identity['fingerprint']!=receipt['run_identity']:raise ValueError('chronological_checkpoint_identity_mismatch')
    m=verify_completed_run(root,identity);return receipt,{x['path']:x['sha256'] for x in m['payloads']}


def export(package,paths,runs):
    receipt,expected=invoke(ROOT,paths,runs,'verify');r=op.read(ROOT/op.RECIPE);locations=op.read(paths)
    values={'source/'+n:(ROOT/n).read_bytes() for n in sources()}
    for alias,d in r['inputs'].items():
        for n in d['files']:values['inputs/'+alias+'/'+n]=op.checked(locations,r,alias,n)
    for n,h in r['predecessors'].items():
        raw=(Path(locations['trad'])/n).read_bytes()
        if hashlib.sha256(raw).hexdigest()!=h:raise ValueError('chronological_native_checkpoint_external_source_changed')
        values['trad/'+n]=raw
    values['EXPECTED.json']=encoded(expected)
    values['SCOPE.json']=encoded({'inputs':'authenticated_selected_original_inputs_and_saved_base_models; not_complete_parent_copies',
        'policy_replays':0,'base_model_fits':0,'base_model_loads':16,'layer_fits':'exact_saved_expanding_snapshot_reconstruction_for_timing_only; frozen_snapshots_reused','pytest_version':importlib.metadata.version('pytest'),
        'original_run_identity':receipt['run_identity'],'scope':'same_machine_relocation; no_other_machine_or_cloud_sync_claim'})
    manifest={'schema_version':SCHEMA,'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(values.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    pin=sha256_file(package);inspect(package,pin)
    return {'status':'VERIFIED_EXPORT','sha256':pin,'members':len(values)+1,'payloads':len(expected),'run_identity':receipt['run_identity']}


def restore(package,pin,destination,tests=False):
    data=inspect(package,pin);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True);destination=destination.resolve()
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    del data
    source=destination/'source';r=op.read(source/op.RECIPE);locations={a:str(destination/'inputs'/a) for a in r['inputs']};locations['trad']=str(destination/'trad')
    paths=destination/'PATHS.json';paths.write_bytes(encoded(locations));receipt,actual=invoke(source,paths,destination/'runs','run')
    if actual!=op.read(destination/'EXPECTED.json'):raise ValueError('chronological_relocated_payload_mismatch')
    if tests:
        if importlib.metadata.version('pytest')!=op.read(destination/'SCOPE.json')['pytest_version']:raise ValueError('chronological_test_environment_mismatch')
        p=subprocess.run([sys.executable,'-X','utf8','-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_chronological_native_v2.py'),str(source/'test_magnitude_layer_v2.py')],capture_output=True,text=True,timeout=180)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('chronological_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':pin,'run_identity':receipt['run_identity'],'scientific_payloads_identical':len(actual),
        'relocated_tests_passed':tests,'policy_replays':0,'base_model_fits':0,'base_model_loads':receipt['base_model_loads'],
        'qualification_regressions':receipt['qualification_regressions'],'scientific_layer_parameters_changed':False,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(encoded(result));return result


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export');r=s.add_parser('restore')
    for n in ('package','paths','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
