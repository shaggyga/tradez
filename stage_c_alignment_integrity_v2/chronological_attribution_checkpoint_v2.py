"""Portable authenticated input subsets and deterministic zero-model attribution."""
import argparse,hashlib,importlib.metadata,json,stat,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import chronological_attribution_operator_v2 as op
from portable_checkpoint_v2 import safe_member,_plain_destination,MANIFEST,encoded
from publication import sha256_file,verify_completed_run
SCHEMA='forex_chronological_attribution_checkpoint.v1'


def sources():return sorted(set(op.SOURCES)|{op.RECIPE,'chronological_attribution_checkpoint_v2.py','portable_checkpoint_v2.py',
    'test_chronological_attribution_v2.py','CHRONOLOGICAL_ATTRIBUTION_V2.md'})


def members(r):return {'source/'+n for n in sources()}|{'inputs/'+a+'/'+n for a,d in r['inputs'].items() for n in d['files']}|{'EXPECTED.json','SCOPE.json'}


def inspect(package,pin):
    if package.is_symlink() or package.stat().st_size>268435456 or sha256_file(package)!=pin:raise ValueError('attribution_checkpoint_pin_or_size')
    with zipfile.ZipFile(package) as z:
        infos=z.infolist();names=[i.filename for i in infos]
        if len(names)>640 or len(set(n.casefold() for n in names))!=len(names) or sum(i.file_size for i in infos)>1342177280:
            raise ValueError('attribution_checkpoint_inventory_bounds')
        for i in infos:
            safe_member(i.filename)
            if i.file_size>16777216 or i.is_dir() or stat.S_ISLNK(i.external_attr>>16) or i.flag_bits&1:raise ValueError('attribution_checkpoint_plain_members')
        if MANIFEST not in names or z.getinfo(MANIFEST).file_size>1048576:raise ValueError('attribution_checkpoint_manifest_bounds')
        manifest=json.loads(z.read(MANIFEST));records={x['path']:x for x in manifest['members']}
        if manifest['schema_version']!=SCHEMA or len(records)!=len(manifest['members']) or set(names)!=set(records)|{MANIFEST}:
            raise ValueError('attribution_checkpoint_exact_inventory')
        for n,r in records.items():
            digest=hashlib.sha256();size=0
            with z.open(n) as f:
                for block in iter(lambda:f.read(1048576),b''):
                    size+=len(block);digest.update(block)
            if size!=r['bytes'] or digest.hexdigest()!=r['sha256']:raise ValueError('attribution_checkpoint_member_changed')
        recipe=json.loads(z.read('source/'+op.RECIPE))
        if set(records)!=members(recipe):raise ValueError('attribution_checkpoint_declared_members')
        return manifest



def invoke(source,paths,runs,action):
    recipe=source/op.RECIPE
    command=[sys.executable,'-X','utf8','-I','-B',str(source/'chronological_attribution_operator_v2.py'),action,
        '--recipe',str(recipe),'--recipe-sha256',sha256_file(recipe),'--paths',str(paths),'--runs-dir',str(runs)]
    result=subprocess.run(command,capture_output=True,text=True,timeout=900)
    if result.returncode:raise ValueError('attribution_checkpoint_operator_failed:'+result.stdout+result.stderr)
    receipt=json.loads(result.stdout)
    if receipt['status']!='completed_verified' or any(receipt[k] for k in ('base_model_fits','base_model_loads','layer_fits','api_calls')):
        raise ValueError('attribution_checkpoint_complete_zero_model_result_required')
    root=runs/op.read(recipe)['run_id'];identity=op.read(root/'RUN_IDENTITY.json')
    if identity['fingerprint']!=receipt['run_identity']:raise ValueError('attribution_checkpoint_identity_mismatch')
    m=verify_completed_run(root,identity)
    return receipt,{x['path']:x['sha256'] for x in m['payloads']}


def export(package,paths,runs):
    receipt,expected=invoke(ROOT,paths,runs,'verify');r=op.read(ROOT/op.RECIPE);locations=op.read(paths)
    records=[]
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        def put(n,b):
            z.writestr(n,b);records.append({'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
        for n in sources():put('source/'+n,(ROOT/n).read_bytes())
        for alias,d in r['inputs'].items():
            for n in d['files']:put('inputs/'+alias+'/'+n,op.checked_bytes(locations,r,alias,n))
        put('EXPECTED.json',encoded(expected))
        put('SCOPE.json',encoded({'inputs':'authenticated_selected_payloads_of_previously_verified_complete_runs; not_full_parent_copies',
            'policy_replays':0,'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'pytest_version':importlib.metadata.version('pytest'),
            'original_run_identity':receipt['run_identity'],'scope':'same_machine_relocation; no_other_machine_or_cloud_sync_claim'}))
        manifest={'schema_version':SCHEMA,'members':sorted(records,key=lambda x:x['path'])}
        z.writestr(MANIFEST,encoded(manifest))
    digest=sha256_file(package);inspect(package,digest)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'members':len(records)+1,'payloads':len(expected),'run_identity':receipt['run_identity']}



def restore(package,pin,destination,tests=False):
    manifest=inspect(package,pin);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True);destination=destination.resolve()
    with zipfile.ZipFile(package) as z:
        for n in [x['path'] for x in manifest['members']]+[MANIFEST]:
            p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
            with p.open('xb') as out,z.open(n) as inp:
                for block in iter(lambda:inp.read(1048576),b''):out.write(block)
    if sha256_file(package)!=pin:raise ValueError('attribution_checkpoint_changed_during_extract')
    source=destination/'source';r=op.read(source/op.RECIPE)
    locations={a:str(destination/'inputs'/a) for a in r['inputs']};paths=destination/'PATHS.json';paths.write_bytes(encoded(locations))
    receipt,actual=invoke(source,paths,destination/'runs','run')
    if actual!=op.read(destination/'EXPECTED.json'):raise ValueError('attribution_relocated_payload_mismatch')
    if tests:
        if importlib.metadata.version('pytest')!=op.read(destination/'SCOPE.json')['pytest_version']:raise ValueError('attribution_test_environment_mismatch')
        p=subprocess.run([sys.executable,'-X','utf8','-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_chronological_attribution_v2.py')],capture_output=True,text=True,timeout=180)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('attribution_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':pin,'run_identity':receipt['run_identity'],'scientific_payloads_identical':len(actual),
        'relocated_tests_passed':tests,'policy_replays':0,'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(encoded(result));return result


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export');r=s.add_parser('restore')
    for n in ('package','paths','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
