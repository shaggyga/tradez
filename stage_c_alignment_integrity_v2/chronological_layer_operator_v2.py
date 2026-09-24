"""Bounded saved-model extension; authenticated inputs before any deserialization."""
import argparse,ast,hashlib,importlib.metadata,json,os,platform,shutil,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
CONTRACT='CHRONOLOGICAL_LAYER_CONTRACT_V2.json';RECIPE='CHRONOLOGICAL_LAYER_OPERATOR_RECIPE_V2.json'
read=lambda p:json.loads(p.read_bytes());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
encoded=lambda v:(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
PACKAGES=('numpy','pandas','pyarrow','psutil','scipy','scikit-learn','joblib','threadpoolctl','tzdata')


def source_names():
    pending=['chronological_layer_operator_v2.py','chronological_layer_v2.py'];seen=set()
    while pending:
        name=pending.pop()
        if name in seen:continue
        seen.add(name);tree=ast.parse((ROOT/name).read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            modules=([node.module] if isinstance(node,ast.ImportFrom) and node.module else [a.name for a in node.names] if isinstance(node,ast.Import) else [])
            for module in modules:
                candidate=module.split('.')[0]+'.py'
                if (ROOT/candidate).is_file() and candidate not in seen:pending.append(candidate)
    return sorted(seen|{CONTRACT})


def checked(paths,r,alias,name):
    d=r['inputs'][alias]['files'][name];root=Path(paths[alias]);p=root/name
    if Path(name).name!=name or root.is_symlink() or root.is_junction() or p.is_symlink() or p.is_junction():raise ValueError('chronological_plain_input_required')
    if p.stat().st_size!=d['bytes'] or d['bytes']>r['contract']['resources']['max_member_bytes']:raise ValueError('chronological_input_size_changed')
    raw=p.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=d['sha256']:raise ValueError('chronological_input_hash_changed')
    return raw


def load(paths,r,alias,name):return json.loads(checked(paths,r,alias,name))


def preflight(path,pin,paths):
    if path.is_symlink() or sha(path)!=pin:raise ValueError('chronological_recipe_pin_mismatch')
    r=read(path)
    if set(r['sources'])!=set(source_names()) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('chronological_source_changed_before_model_import')
    env={'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in PACKAGES}}
    if r['environment']!=env or r['contract']!=read(ROOT/CONTRACT):raise ValueError('chronological_environment_contract_mismatch')
    if set(paths)!=set(r['inputs']):raise ValueError('chronological_exact_path_map_required')
    if sum(f['bytes'] for d in r['inputs'].values() for f in d['files'].values())>r['contract']['resources']['max_input_bytes']:raise ValueError('chronological_input_budget')
    sys.path.insert(0,str(ROOT));from publication import validate_run_identity,_validate_inventory
    for alias,d in r['inputs'].items():
        for n in d['files']:checked(paths,r,alias,n)
        identity=load(paths,r,alias,'RUN_IDENTITY.json');m=load(paths,r,alias,'COMPLETION_MANIFEST.json');validate_run_identity(identity)
        if identity['fingerprint']!=d['original_run_identity'] or m['run_identity']!=identity:raise ValueError('chronological_original_parent_identity')
        _validate_inventory(m['required_payloads'],m['payloads'],identity);inventory={x['path']:x for x in m['payloads']}
        for n,desc in d['files'].items():
            if n not in ('RUN_IDENTITY.json','COMPLETION_MANIFEST.json') and inventory.get(n)!={'path':n,**desc}:raise ValueError('chronological_subset_manifest_mismatch')
    return r


def required(r):
    c=r['contract'];return sorted([prefix+str(h)+'.json' for h in c['horizons_minutes'] for prefix in ('overlap_','new_base_','frozen_')]+
        [f'frame_{h}_{t}.json' for h in c['horizons_minutes'] for t in c['new_origins']]+['schedule.json','training_proof.json','assessment.json','run_report.json','source_references.json'])


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':r,'required_payloads':required(r)},dependency_hashes={**r['sources'],**{a:d['original_run_identity'] for a,d in r['inputs'].items()}})


def operate(action,path,pin,paths,runs,crash_after=None):
    started=time.monotonic();r=preflight(path,pin,paths);c=r['contract'];parent=c['parent_surface_contract']
    from publication import RunPublisher,verify_completed_run
    from contracts import fingerprint
    from later_surface_models_v2 import SavedModels,prequential_chunk,schedule
    from magnitude_layer_v2 import join_predictions
    from chronological_layer_v2 import verify_population,combine,frozen_snapshot,frame,assessment
    import psutil
    identity=identity_for(r);pub=RunPublisher(runs,r['run_id'],identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        manifest=verify_completed_run(pub.root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(manifest['payloads']),'base_model_fits':0,'base_model_loads':0,'actual_layer_regression_fits':0,'api_calls':0,'policy_replays':0,'reused_completed':True}
    if action in ('status','verify'):
        if action=='verify':raise ValueError('chronological_completed_run_required')
        return {'status':'resumable' if pub.root.exists() else 'ready','run_identity':identity['fingerprint']}
    pub.acquire(recover=action=='resume');payloads=[];actual_fits=0;new_snapshot_attempts=0;phase_receipts=[]
    def put(name,value):payloads.append(pub.write_or_validate_payload(name,encoded(value)))
    def cached(name):
        raw=pub.read_verified_payload(name);return None if raw is None else json.loads(raw)
    def measured(row):
        phase_receipts.append(row)
        with (pub.root/'TIMING_ATTEMPTS.jsonl').open('ab') as f:
            f.write(encoded(row));f.flush();os.fsync(f.fileno())
    def guard(phase):
        row={'phase':phase,'elapsed_seconds':time.monotonic()-started,'rss_bytes':psutil.Process().memory_info().rss,'scratch_bytes':sum(p.stat().st_size for p in pub.root.iterdir() if p.is_file()),'free_bytes':shutil.disk_usage(runs).free}
        lim=c['resources']
        if row['elapsed_seconds']>lim['main_wall_seconds'] or row['rss_bytes']>lim['max_rss_bytes'] or row['scratch_bytes']>lim['max_scratch_bytes'] or row['free_bytes']<lim['minimum_disk_free_bytes']:raise ValueError('chronological_resource_limit:'+json.dumps(row))
        with (pub.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(row))
        return row
    try:
        guard('before_inputs');observations=[];outcomes=[]
        for pair in c['universe']:
            part=load(paths,r,'extension','pair_'+pair+'.json');observations.extend(part['observations']);outcomes.extend(part['outcomes'])
        if len({o['record_id'] for o in observations})!=len(observations):raise ValueError('chronological_unique_observations_required')
        lookup={(o['record_id'],o['target_id']):o for o in outcomes}
        if len(lookup)!=len(outcomes):raise ValueError('chronological_unique_outcomes_required')
        artifacts={};proof=[]
        for h in c['horizons_minutes']:
            signed=load(paths,r,'remaining',f'fit_{h}.json');binary=checked(paths,r,'remaining',f'fit_{h}.joblib')
            artifacts['signed',h]=(signed,binary);proof.append(verify_population(signed,binary,observations,outcomes))
            alias='surface' if h in parent['new_absolute_horizons'] else 'absolute'
            stem=f'absolute_{h}' if alias=='surface' else f'fit_{h}_{parent["fit_cutoff"]}'
            meta=load(paths,r,alias,stem+'.json');binary=checked(paths,r,alias,stem+'.joblib')
            artifacts['absolute',h]=(meta,binary);proof.append(verify_population(meta,binary,observations,outcomes,signed))
        put('training_proof.json',{'saved_fit_pairs':proof,'base_refits':0})
        original_schedule=load(paths,r,'surface','schedule.json')
        if original_schedule!=schedule(parent):raise ValueError('chronological_original_schedule_changed')
        predictor=SavedModels(artifacts,parent,original_schedule)
        extended={**original_schedule,'prediction_windows':original_schedule['prediction_windows']+[[t,t+16] for t in c['new_origins']],
            'layer_windows':[[t+16,t+136] for t in c['new_origins']],'parent_schedule_id':original_schedule['schedule_id'],
            'scope':'single_worker_modeled_diagnostic_extension; original_records_keep_original_schedule; no_historical_issuance'}
        extended.pop('schedule_id');extended['schedule_id']=fingerprint(extended);put('schedule.json',extended)
        new_contract={**parent,'prequential_origins':c['new_origins'],'policy_origins':[]};frames=[];frozen_reused=0
        for hi,h in enumerate(c['horizons_minutes']):
            original=load(paths,r,'surface',f'prequential_{h}.json');name=f'overlap_{h}.json';overlap=cached(name)
            if overlap is None:
                predictor.reservations=original_schedule
                reproduced,timing=prequential_chunk(predictor,observations,lookup,h,parent)
                reproduced['joined_rows']=join_predictions(reproduced['base_rows'],reproduced['absolute_rows'],['legacy26',f'technical_endpoint_midpoint_elapsed_{h}m','frozen'])
                if reproduced!=original:raise ValueError('chronological_original_prequential_payload_changed')
                overlap={'horizon_minutes':h,'original_prequential_sha256':fingerprint(original),'original_origins':len(c['original_origins']),'predictions_identical':len(original['joined_rows']),'exact_payload_match':True}
                for x in timing:measured({'phase':'original_overlap',**x})
            put(name,overlap);name=f'new_base_{h}.json';new=cached(name)
            if new is None:
                predictor.reservations=extended
                new,timing=prequential_chunk(predictor,observations,lookup,h,new_contract)
                new['joined_rows']=join_predictions(new['base_rows'],new['absolute_rows'],['legacy26',f'technical_endpoint_midpoint_elapsed_{h}m','frozen'])
                for x in timing:measured({'phase':'new_base',**x})
            put(name,new);rows=combine(original,new,h,c)
            old_origin=parent['common_target_epoch']-h*60;oldframe=load(paths,r,'surface',f'frame_{old_origin}.json')
            name=f'frozen_{h}.json';frozen=cached(name)
            if frozen is None:
                began=time.monotonic();snapshot,reused=frozen_snapshot(oldframe,rows,lookup,h,c)
                elapsed=time.monotonic()-began
                if elapsed>15:raise ValueError('chronological_frozen_snapshot_slot_exceeded')
                frozen={'snapshot':snapshot,'original_snapshot_reused':reused};new_snapshot_attempts+=not reused;actual_fits+=4*(not reused and snapshot['status']=='fitted')
                measured({'phase':'frozen','horizon':h,'elapsed_seconds':elapsed,'reused':reused,'regression_fits':4*(not reused and snapshot['status']=='fitted')})
            put(name,frozen);frozen_reused+=frozen['original_snapshot_reused'];guard('after_base_'+str(h))
            if crash_after==hi+1:os._exit(91)
            for t in c['new_origins']:
                name=f'frame_{h}_{t}.json';item=cached(name)
                if item is None:
                    began=time.monotonic();item=frame(h,t,rows,observations,lookup,frozen['snapshot'],c);elapsed=time.monotonic()-began
                    if elapsed>15:raise ValueError('chronological_layer_slot_exceeded:'+str(elapsed))
                    new_snapshot_attempts+=1;actual_fits+=4*(item['expanding_snapshot']['status']=='fitted')
                    measured({'phase':'layer','horizon':h,'origin':t,'elapsed_seconds':elapsed,'limit_seconds':15,'regression_fits':4*(item['expanding_snapshot']['status']=='fitted')})
                put(name,item);frames.append(item);guard('after_frame_'+str(h)+'_'+str(t))
            if actual_fits>c['resources']['layer_regression_fit_cap'] or new_snapshot_attempts>c['resources']['snapshot_fit_attempt_cap']:raise ValueError('chronological_layer_fit_cap')
        result=assessment(frames,lookup,c);put('assessment.json',result)
        coverage=sum(len(f['coverage']) for f in frames)
        if len(frames)!=192 or coverage!=182784 or frozen_reused!=7:raise ValueError('chronological_complete_scope_required')
        put('run_report.json',{'status':'completed_chronological_layer_comparison','frames':len(frames),'coverage_slots':coverage,'coverage_reasons':result['coverage_reasons'],
            'forecast_rows':sum(len(f['predictions']) for f in frames),'new_base_origin_count':24,'original_base_origins_preserved':20,
            'saved_base_fit_pairs':16,'original_frozen_snapshots_reused':frozen_reused,'expanding_snapshots':192,
            'supported_expanding_snapshots':sum(f['expanding_snapshot']['status']=='fitted' for f in frames),
            'base_model_fits':0,'api_calls':0,'policy_replays':0,'confirmation':False,'independent_review':False,**c['readiness']})
        put('source_references.json',{'parent_identities':c['parent_identities'],'source_hashes':r['sources'],'saved_fit_ids':{family+'_'+str(h):meta['fit_id'] for (family,h),(meta,_) in artifacts.items()},'contract_sha256':fingerprint(c)})
        if any(sha(ROOT/n)!=digest for n,digest in r['sources'].items()):raise ValueError('chronological_source_changed_during_run')
        last=guard('final_precommit');pub.complete(payloads,set(required(r)));verify_completed_run(pub.root,identity)
        receipt={'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(payloads),'base_model_fits':0,'base_model_loads':predictor.loaded,
            'actual_layer_regression_fits':actual_fits,'actual_snapshot_attempts':new_snapshot_attempts,'api_calls':0,'policy_replays':0,'resource_last':last,
            'prewarm':predictor.receipt,'phase_receipts':phase_receipts}
        with (pub.root/'COMPUTATION_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(receipt))
        return receipt
    finally:
        if pub._guard_handle is not None:pub.release()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    try:result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after)
    except Exception as exc:result={'status':'review_required','reason':str(exc)}
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
