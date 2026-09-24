"""Bounded zero-model attribution with authenticated subsets and journal recovery."""
import argparse,hashlib,importlib.metadata,json,os,platform,shutil,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
RECIPE='CURRENCY_PROJECTION_OPERATOR_RECIPE_V2.json'
CONTRACT='CURRENCY_PROJECTION_CONTRACT_V2.json'
SOURCES=[CONTRACT,'currency_projection_operator_v2.py','currency_projection_v2.py','contracts.py','publication.py']
read=lambda p:json.loads(p.read_bytes())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
encoded=lambda x:(json.dumps(x,sort_keys=True,separators=(',',':'))+'\n').encode()


def preflight(path,pin,paths):
    if path.is_symlink() or sha(path)!=pin:raise ValueError('attribution_recipe_pin_mismatch')
    r=read(path)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=v for n,v in r['sources'].items()):
        raise ValueError('attribution_source_drift_before_analysis_import')
    environment={'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','psutil')}}
    if r['environment']!=environment or r['contract']!=read(ROOT/CONTRACT):raise ValueError('attribution_environment_contract_mismatch')
    if set(paths)!=set(r['inputs'])|{'solver'}:raise ValueError('attribution_exact_parent_map_required')
    if sum(f['bytes'] for d in r['inputs'].values() for f in d['files'].values())>r['contract']['resources']['max_input_bytes']:
        raise ValueError('attribution_input_budget')
    sys.path.insert(0,str(ROOT))
    from publication import validate_run_identity,_validate_inventory
    for alias,dep in r['inputs'].items():
        identity=load(paths,r,alias,'RUN_IDENTITY.json');manifest=load(paths,r,alias,'COMPLETION_MANIFEST.json')
        validate_run_identity(identity)
        if identity['fingerprint']!=dep['original_run_identity'] or manifest['run_identity']!=identity:
            raise ValueError('attribution_original_parent_identity_mismatch')
        _validate_inventory(manifest['required_payloads'],manifest['payloads'],identity)
        records={p['path']:p for p in manifest['payloads']}
        for n,descriptor in dep['files'].items():
            if n in ('RUN_IDENTITY.json','COMPLETION_MANIFEST.json'):continue
            if records.get(n)!={'path':n,**descriptor}:raise ValueError('attribution_subset_not_in_original_manifest')
            checked_bytes(paths,r,alias,n)
    for n,d in r['predecessors'].items():
        root=Path(paths['solver']).resolve();p=(root/n).resolve()
        if not p.is_relative_to(root) or p.is_symlink() or p.stat().st_size!=d['bytes'] or sha(p)!=d['sha256']:
            raise ValueError('projection_preserved_solver_changed_before_import')
    return r


def checked_bytes(paths,r,alias,name):
    dep=r['inputs'][alias]['files'][name]
    if Path(name).name!=name or dep['bytes']>r['contract']['resources']['max_member_bytes']:
        raise ValueError('attribution_bounded_plain_member_required')
    root=Path(paths[alias]);p=root/name
    if root.is_symlink() or root.is_junction() or p.is_symlink() or p.is_junction():raise ValueError('attribution_plain_input_required')
    if p.stat().st_size!=dep['bytes']:raise ValueError('attribution_input_size_changed')
    data=p.read_bytes()
    if hashlib.sha256(data).hexdigest()!=dep['sha256']:raise ValueError('attribution_input_hash_changed')
    return data


def load(paths,r,alias,name):return json.loads(checked_bytes(paths,r,alias,name))


def required(r):return [*[f'projection_{i:03}.json' for i in range(len(r['contract']['frames']))],'report.json','report.md','source_references.json']


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'experiment':r['contract'],'recipe':r,'required_payloads':required(r)},
        dependency_hashes={**r['sources'],**{a:d['original_run_identity'] for a,d in r['inputs'].items()}})


def render(report):
    lines=['# Fixed currency-factor forecast projection','','Preserved direct forecasts versus full projection and fixed half-residual retention. Inspected development; no policy replay.',
        '','| Learner | Horizon minutes | Variant | Matched rows | MAE delta bps | MSE delta bps2 |','|---|---:|---|---:|---:|---:|']
    for x in report['matched_comparisons']:
        lines.append('| '+' | '.join(map(str,[x['base_method'],x['horizon_minutes'],x['variant'],x['direct']['mature_rows'],x['mae_delta_bps'],x['mse_delta_bps2']]))+' |')
    lines+=['','Negative deltas mean lower error on exact matched support. Currency edges, horizons and dates overlap; these are not independent trials. Missing/future outcomes are excluded explicitly. Factor diagnostics are forecast projections, not observed currency responses or calibrated predictive uncertainty. No protected confirmation, native issuance or trading authorization.']
    return '\n'.join(lines)+'\n'



def operate(action,path,pin,paths,runs,crash_after=None):
    started=time.monotonic();r=preflight(path,pin,paths)
    from publication import RunPublisher,verify_completed_run
    from currency_projection_v2 import load_solver,project_frame,assess
    import psutil
    c=r['contract'];root=runs/r['run_id'];identity=identity_for(r)
    def guard(phase):
        sample={'phase':phase,'elapsed_seconds':time.monotonic()-started,'rss_bytes':psutil.Process().memory_info().rss,
          'scratch_bytes':sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),'free_bytes':shutil.disk_usage(runs).free}
        lim=c['resources']
        if sample['elapsed_seconds']>lim['main_wall_seconds'] or sample['rss_bytes']>lim['max_rss_bytes'] or sample['scratch_bytes']>lim['max_scratch_bytes'] or sample['free_bytes']<lim['minimum_disk_free_bytes']:
            raise ValueError('attribution_resource_limit:'+json.dumps(sample))
        with (root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(sample))
        return sample
    if (root/'COMPLETION_MANIFEST.json').exists():
        manifest=verify_completed_run(root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(manifest['payloads']),
          'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'api_calls':0,'policy_replays':0,'reused_completed':True}
    if action in ('status','verify'):
        if action=='verify':raise ValueError('attribution_completed_run_required')
        return {'status':'resumable' if root.exists() else 'ready','run_identity':identity['fingerprint']}
    publisher=RunPublisher(runs,r['run_id'],identity);publisher.acquire(recover=action=='resume');payloads=[]
    def put(name,obj,raw=False):payloads.append(publisher.write_or_validate_payload(name,obj if raw else encoded(obj)))
    try:
        guard('before_inputs');solver=load_solver(paths['solver'],r['predecessors']);frames=[]
        for i,source_name in enumerate(c['frames']):
            guard('before_frame_'+str(i));name=f'projection_{i:03}.json';cached=publisher.read_verified_payload(name)
            if cached is None:
                began=time.monotonic();source=load(paths,r,'chronological',source_name);value=project_frame(source,c,solver)
                elapsed=time.monotonic()-began
                if elapsed>c['resources']['projection_frame_seconds']:raise ValueError('projection_compute_slot_exceeded:'+str(elapsed))
                with (root/'PROJECTION_TIMINGS.jsonl').open('ab') as f:f.write(encoded({'frame':source_name,'seconds':elapsed}))
            else:value=json.loads(cached)
            put(name,value);frames.append(value)
            if crash_after==i+1:os._exit(91)
        outcomes=[]
        for n in r['inputs']['extension']['files']:
            if n.startswith('pair_'):outcomes.extend(load(paths,r,'extension',n)['outcomes'])
        report=assess(frames,outcomes,c);report.update(c['readiness'])
        if report['frames']!=192 or report['coverage_slots']!=78336:raise ValueError('projection_full_frame_coverage')
        put('report.json',report);put('report.md',render(report).encode(),raw=True)
        put('source_references.json',{'original_parent_identities':{k:v['original_run_identity'] for k,v in r['inputs'].items()},
            'source_hashes':r['sources'],'preserved_solver':r['predecessors'],'base_model_fits':0,'base_model_loads':0,'learned_layer_fits':0,'policy_replays':0})
        if any(sha(ROOT/n)!=v for n,v in r['sources'].items()):raise ValueError('attribution_source_changed_during_run')
        last=guard('final_precommit');publisher.complete(payloads,set(required(r)));verify_completed_run(root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(payloads),
          'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'api_calls':0,'policy_replays':0,'resource_last':last}
    finally:
        if publisher._guard_handle is not None:publisher.release()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    try:result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after)
    except Exception as exc:result={'status':'review_required','reason':str(exc)}
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
