"""Bounded later causal input preparation, with per-pair authenticated recovery."""
import argparse,ast,hashlib,importlib.metadata,json,os,platform,shutil,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
CONTRACT='LATER_INPUT_EXTENSION_CONTRACT_V2.json';RECIPE='LATER_INPUT_EXTENSION_OPERATOR_RECIPE_V2.json'
read=lambda p:json.loads(p.read_bytes());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
encoded=lambda v:(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
PACKAGES=('numpy','pandas','pyarrow','psutil','scipy','scikit-learn','joblib','threadpoolctl','tzdata')


def source_names():
    pending=['later_input_extension_operator_v2.py','later_input_extension_v2.py'];seen=set()
    while pending:
        name=pending.pop()
        if name in seen:continue
        seen.add(name);tree=ast.parse((ROOT/name).read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            mods=([node.module] if isinstance(node,ast.ImportFrom) and node.module else
                  [a.name for a in node.names] if isinstance(node,ast.Import) else [])
            for module in mods:
                candidate=module.split('.')[0]+'.py'
                if (ROOT/candidate).is_file() and candidate not in seen:pending.append(candidate)
    return sorted(seen|{CONTRACT})


def checked(paths,r,alias,name):
    descriptor=r['inputs'][alias]['files'][name];root=Path(paths[alias]);p=root/name
    if Path(name).name!=name or root.is_symlink() or root.is_junction() or p.is_symlink() or p.is_junction():raise ValueError('extension_plain_input_required')
    if p.stat().st_size!=descriptor['bytes'] or descriptor['bytes']>r['contract']['resources']['max_member_bytes']:raise ValueError('extension_input_size_changed')
    raw=p.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=descriptor['sha256']:raise ValueError('extension_input_hash_changed')
    return raw


def load(paths,r,alias,name):return json.loads(checked(paths,r,alias,name))


def preflight(path,pin,paths):
    if path.is_symlink() or sha(path)!=pin:raise ValueError('extension_recipe_pin_mismatch')
    r=read(path)
    if set(r['sources'])!=set(source_names()) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('extension_source_drift_before_preparation_import')
    env={'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in PACKAGES}}
    if r['environment']!=env or r['contract']!=read(ROOT/CONTRACT):raise ValueError('extension_environment_contract_mismatch')
    if set(paths)!=set(r['inputs'])|{'trad'}:raise ValueError('extension_exact_path_map_required')
    if sum(f['bytes'] for d in r['inputs'].values() for f in d['files'].values())>r['contract']['resources']['max_input_bytes']:raise ValueError('extension_input_budget')
    for n,h in r['predecessors'].items():
        if sha(Path(paths['trad'])/n)!=h:raise ValueError('extension_native_source_changed_before_import')
    if any((Path(paths['trad'])/n).exists() for n in r['absent_import_paths']):raise ValueError('extension_native_shadow_import_present')
    sys.path.insert(0,str(ROOT));from publication import validate_run_identity,_validate_inventory
    for alias,dep in r['inputs'].items():
        for n in dep['files']:checked(paths,r,alias,n)
        if alias=='slices':
            m=load(paths,r,alias,'SLICES_MANIFEST.json')
            if sha(Path(paths[alias])/'SLICES_MANIFEST.json')!=r['contract']['slice_manifest_sha256'] or sorted(x['instrument'] for x in m['members'])!=sorted(r['contract']['universe']):raise ValueError('extension_original_all68_manifest')
            for member in m['members']:
                if member['path']!=member['instrument']+'.parquet' or dep['files'][member['path']]!={k:member[k] for k in ('bytes','sha256')}:raise ValueError('extension_slice_descriptor_mismatch')
        else:
            identity=load(paths,r,alias,'RUN_IDENTITY.json');manifest=load(paths,r,alias,'COMPLETION_MANIFEST.json');validate_run_identity(identity)
            if identity['fingerprint']!=dep['original_run_identity'] or manifest['run_identity']!=identity:raise ValueError('extension_original_parent_identity')
            _validate_inventory(manifest['required_payloads'],manifest['payloads'],identity);inventory={x['path']:x for x in manifest['payloads']}
            for n,d in dep['files'].items():
                if n not in ('RUN_IDENTITY.json','COMPLETION_MANIFEST.json') and inventory.get(n)!={'path':n,**d}:raise ValueError('extension_subset_manifest_mismatch')
    return r


def required(r):return sorted(['pair_'+p+'.json' for p in r['contract']['universe']]+['market_'+c['name']+'.json' for c in r['contract']['cohorts']]+['input_contract.json','run_report.json'])


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':r,'required_payloads':required(r)},dependency_hashes={**r['sources'],
        'slices':r['contract']['slice_manifest_sha256'],**{a:d['original_run_identity'] for a,d in r['inputs'].items() if a!='slices'}})


def operate(action,path,pin,paths,runs,crash_after=None):
    began=time.monotonic();r=preflight(path,pin,paths);c=r['contract']
    from publication import RunPublisher,verify_completed_run
    from later_input_extension_v2 import prepare_pair,prepare_market,report
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import psutil
    identity=identity_for(r);pub=RunPublisher(runs,r['run_id'],identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        manifest=verify_completed_run(pub.root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(manifest['payloads']),
          'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'policy_replays':0,'api_calls':0,'reused_completed':True}
    if action in ('status','verify'):
        if action=='verify':raise ValueError('extension_completed_run_required')
        return {'status':'resumable' if pub.root.exists() else 'ready','run_identity':identity['fingerprint']}
    pub.acquire(recover=action=='resume');payloads=[]
    def guard(phase):
        sample={'phase':phase,'elapsed_seconds':time.monotonic()-began,'rss_bytes':psutil.Process().memory_info().rss,
          'scratch_bytes':sum(p.stat().st_size for p in pub.root.rglob('*') if p.is_file()),'free_bytes':shutil.disk_usage(runs).free}
        lim=c['resources']
        if sample['elapsed_seconds']>lim['main_wall_seconds'] or sample['rss_bytes']>lim['max_rss_bytes'] or sample['scratch_bytes']>lim['max_scratch_bytes'] or sample['free_bytes']<lim['minimum_disk_free_bytes']:raise ValueError('extension_resource_limit:'+json.dumps(sample))
        with (pub.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(sample))
        return sample
    def put(name,value):payloads.append(pub.write_or_validate_payload(name,encoded(value)))
    try:
        guard('before_inputs');m=load(paths,r,'slices','SLICES_MANIFEST.json');extra=load(paths,r,'remaining','remaining_outcomes.json')
        by_pair={pair:[] for pair in c['universe']}
        for row in extra:by_pair[row['record_id'].split(':')[0]].append(row)
        parts=[]
        for i,member in enumerate(sorted(m['members'],key=lambda x:x['instrument'])):
            name='pair_'+member['instrument']+'.json';raw=pub.read_verified_payload(name)
            if raw is None:
                table=pq.read_table(pa.BufferReader(checked(paths,r,'slices',member['path'])))
                if set(table.column_names)!= {'epoch','mid','bid','ask'} or table.num_rows!=member['rows']:raise ValueError('extension_original_slice_schema_rows')
                part=prepare_pair(member,table.to_pandas(),load(paths,r,'technical',name),by_pair[member['instrument']],c)
            else:part=json.loads(raw)
            if part['instrument']!=member['instrument']:raise ValueError('extension_cached_pair_mismatch')
            put(name,part);parts.append(part);guard('after_'+member['instrument'])
            if crash_after==i+1:os._exit(91)
        markets=[];original=load(paths,r,'surface','market.json')
        for cohort in c['cohorts']:
            name='market_'+cohort['name']+'.json';raw=pub.read_verified_payload(name)
            value=prepare_market(paths['slices'],m,cohort,original,Path(paths['trad'])) if raw is None else json.loads(raw)
            put(name,value);markets.append(value);guard('after_market_'+cohort['name'])
        summary=report(parts,markets,c);put('run_report.json',summary);put('input_contract.json',c)
        if any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('extension_source_changed_during_run')
        if any(sha(Path(paths['trad'])/n)!=h for n,h in r['predecessors'].items()):raise ValueError('extension_native_source_changed_during_run')
        last=guard('final_precommit');pub.complete(payloads,set(required(r)));verify_completed_run(pub.root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(payloads),'resource_last':last,
          'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'policy_replays':0,'api_calls':0}
    finally:
        if pub._guard_handle is not None:pub.release()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    try:result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after)
    except Exception as exc:result={'status':'review_required','reason':str(exc)}
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
