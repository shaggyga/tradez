"""Data-only bounded capsule of the exact original warm-curve dependencies."""
import argparse,hashlib,json,shutil,stat,zipfile
from pathlib import Path,PurePosixPath
SCHEMA='forex_warm_curve_original_inputs.v1';MANIFEST='CAPSULE_MANIFEST.json';CONTRACT='CAPSULE_CONTRACT.json';ALIASES={'family','baseline','rich'}
MAX_COMPRESSED=128*1024*1024;MAX_EXPANDED=384*1024*1024;MAX_MEMBER=64*1024*1024;MAX_FILES=512
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()
def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def safe(name):
    q=PurePosixPath(name)
    if not name or q.is_absolute() or '\\' in name or ':' in name or any(p in ('','..','.') or p.endswith(('.', ' ')) for p in name.split('/')):raise ValueError('capsule_plain_relative_path_required')
    if any(not all(c.isalnum() or c in '_.-' for c in p) or p.split('.')[0].upper() in {'CON','PRN','AUX','NUL',*('COM'+str(i) for i in range(1,10)),*('LPT'+str(i) for i in range(1,10))} for p in q.parts):raise ValueError('capsule_portable_path_required')
    return q
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def inventory(root):
    identity=read(root/'RUN_IDENTITY.json');m=read(root/'COMPLETION_MANIFEST.json');rows=m['payloads']
    if m['run_identity']!=identity or len({x['path'] for x in rows})!=len(rows) or len(m['required_payloads'])!=len(rows) or set(m['required_payloads'])!={x['path'] for x in rows}:raise ValueError('capsule_completed_original_run_required')
    for x in rows:
        n=x['path'];safe(n);p=root/n
        if len(PurePosixPath(n).parts)!=1 or p.is_symlink() or p.stat().st_size!=x['bytes'] or sha(p)!=x['sha256']:raise ValueError('capsule_original_payload_drift')
    return {'identity':identity,'payloads':{x['path']:x['sha256'] for x in rows},'completion_sha256':sha(root/'COMPLETION_MANIFEST.json'),'identity_sha256':sha(root/'RUN_IDENTITY.json')}
def inspect(package,digest):
    if package.stat().st_size>MAX_COMPRESSED or sha(package)!=digest:raise ValueError('bounded_pinned_input_capsule_required')
    with zipfile.ZipFile(package) as z:
        infos=z.infolist();names=[i.filename for i in infos]
        if len(infos)>MAX_FILES or len(names)!=len(set(names)) or sum(i.file_size for i in infos)>MAX_EXPANDED:raise ValueError('bounded_unique_input_capsule_required')
        for i in infos:
            safe(i.filename)
            if i.is_dir() or i.file_size>MAX_MEMBER or stat.S_ISLNK(i.external_attr>>16) or i.flag_bits&1:raise ValueError('plain_bounded_input_capsule_member_required')
        if MANIFEST not in names or CONTRACT not in names or z.getinfo(MANIFEST).file_size>1048576 or z.getinfo(CONTRACT).file_size>1048576:raise ValueError('bounded_input_capsule_metadata_required')
        manifest=json.loads(z.read(MANIFEST));contract=json.loads(z.read(CONTRACT))
        if manifest['schema_version']!=SCHEMA or contract['schema_version']!=SCHEMA or set(contract['dependencies'])!=ALIASES:raise ValueError('exact_original_input_capsule_schema_required')
        rows=manifest['members'];by={x['path']:x for x in rows}
        if len(rows)!=len(by) or set(names)!=set(by)|{MANIFEST}:raise ValueError('exact_input_capsule_inventory_required')
        allowed={CONTRACT}
        for alias,dep in contract['dependencies'].items():allowed.update('inputs/'+alias+'/'+n for n in set(dep['payloads'])|{'RUN_IDENTITY.json','COMPLETION_MANIFEST.json'})
        if set(by)!=allowed:raise ValueError('only_declared_original_inputs_allowed')
        for name,row in by.items():
            raw=z.read(name)
            if len(raw)!=row['bytes'] or hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('input_capsule_member_hash_mismatch')
        for alias,dep in contract['dependencies'].items():
            prefix='inputs/'+alias+'/';identity=json.loads(z.read(prefix+'RUN_IDENTITY.json'));completion=json.loads(z.read(prefix+'COMPLETION_MANIFEST.json'))
            if identity!=dep['identity'] or completion['run_identity']!=identity or by[prefix+'RUN_IDENTITY.json']['sha256']!=dep['identity_sha256'] or by[prefix+'COMPLETION_MANIFEST.json']['sha256']!=dep['completion_sha256']:raise ValueError('input_capsule_original_identity_mismatch')
            if len(completion['payloads'])!=len(dep['payloads']) or len(completion['required_payloads'])!=len(dep['payloads']) or {x['path']:x['sha256'] for x in completion['payloads']}!=dep['payloads'] or set(completion['required_payloads'])!=set(dep['payloads']):raise ValueError('input_capsule_original_completion_mismatch')
            if any(by[prefix+n]['sha256']!=h for n,h in dep['payloads'].items()):raise ValueError('input_capsule_original_payload_pin_mismatch')
        return {'manifest':manifest,'contract':contract,'expanded_bytes':sum(i.file_size for i in infos),'members':len(infos)}
def export(package,paths):
    if set(paths)!=ALIASES:raise ValueError('exact_original_input_aliases_required')
    deps={k:inventory(Path(v)) for k,v in paths.items()}
    if {k:len(v['payloads']) for k,v in deps.items()}!={'family':216,'baseline':34,'rich':210}:raise ValueError('full_original_curve_population_required')
    contract={'schema_version':SCHEMA,'dependencies':deps,'scope':'original_artifacts_and_records_only_no_code_execution_or_new_fit','baseline_recipe_sha256':'ff1ebcdc6ea46b181144a991bdd30b89242bade1e5d469f547fbdbb482516413','model_loading_authority':'external_approved_operator_preflight_still_required','source_lineage_checkpoints':{'family':'2f05332911e24daec483602618125dfaf36b6d7b74c4d1de2c748152d8bdd336','baseline_remaining':'2fca12f0c7d6ab5085718203c1c4b8e1598db748479a749d7deca51f2a13cbb8','rich':'251d1a39778b4cd95aa2d061d9c03a80c961a1e478fbec128be82efb6746f4e3'},'original_timing_payloads_preserved':True,'new_models_fitted':0,'independent_review':False};members=[]
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        raw=encoded(contract);z.writestr(CONTRACT,raw);members.append({'path':CONTRACT,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
        for alias in sorted(ALIASES):
            root=Path(paths[alias])
            for n in sorted(set(deps[alias]['payloads'])|{'RUN_IDENTITY.json','COMPLETION_MANIFEST.json'}):
                raw=(root/n).read_bytes();name='inputs/'+alias+'/'+n;z.writestr(name,raw);members.append({'path':name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
        z.writestr(MANIFEST,encoded({'schema_version':SCHEMA,'members':members}))
    digest=sha(package);result=inspect(package,digest)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'input_files':466,'members':result['members'],'expanded_bytes':result['expanded_bytes'],'compressed_bytes':package.stat().st_size,'model_artifacts_or_records_recomputed':False}
def restore(package,digest,destination):
    result=inspect(package,digest)
    if destination.is_symlink() or destination.is_junction() or destination.exists():raise ValueError('new_empty_input_capsule_destination_required')
    for p in destination.parents:
        if p.exists() and (p.is_symlink() or p.is_junction()):raise ValueError('plain_input_capsule_parent_required')
    destination.mkdir(parents=True)
    with zipfile.ZipFile(package) as z:
        for info in z.infolist():
            p=destination.joinpath(*safe(info.filename).parts);p.parent.mkdir(parents=True,exist_ok=True)
            with z.open(info) as source,p.open('xb') as target:shutil.copyfileobj(source,target,1048576)
    for row in result['manifest']['members']:
        p=destination/row['path']
        if p.stat().st_size!=row['bytes'] or sha(p)!=row['sha256']:raise ValueError('restored_original_input_bytes_changed')
    paths={k:str(destination/'inputs'/k) for k in sorted(ALIASES)}
    if {k:inventory(Path(v)) for k,v in paths.items()}!=result['contract']['dependencies']:raise ValueError('restored_original_inputs_not_identical')
    (destination/'PATHS.json').write_bytes(encoded(paths));receipt={'status':'VERIFIED','checkpoint_sha256':digest,'exact_input_files':sum(len(x['payloads'])+2 for x in result['contract']['dependencies'].values()),'paths':paths,'models_fitted_or_loaded':0,'model_execution_authorized':False,'independent_review':False};(destination/'RESTORE_RECEIPT.json').write_bytes(encoded(receipt));return receipt
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export');e.add_argument('--package',type=Path,required=True);e.add_argument('--paths',type=Path,required=True);r=s.add_parser('restore');r.add_argument('--package',type=Path,required=True);r.add_argument('--sha256',required=True);r.add_argument('--destination',type=Path,required=True);a=p.parse_args();print(json.dumps(export(a.package,read(a.paths)) if a.action=='export' else restore(a.package,a.sha256,a.destination),sort_keys=True))
