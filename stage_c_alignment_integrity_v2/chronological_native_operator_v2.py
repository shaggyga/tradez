"""Bounded saved-model extension; authenticated inputs before any deserialization."""
import argparse,ast,hashlib,importlib.metadata,json,os,platform,shutil,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
CONTRACT='CHRONOLOGICAL_NATIVE_CONTRACT_V2.json';RECIPE='CHRONOLOGICAL_NATIVE_OPERATOR_RECIPE_REVIEWED_V2.json'
read=lambda p:json.loads(p.read_bytes());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
encoded=lambda v:(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
PACKAGES=('numpy','pandas','pyarrow','psutil','scipy','scikit-learn','joblib','threadpoolctl','tzdata')


def source_names():
    pending=['chronological_native_operator_v2.py','chronological_native_v2.py'];seen=set()
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
    if set(paths)!=set(r['inputs'])|{'trad'}:raise ValueError('chronological_exact_path_map_required')
    if sum(f['bytes'] for d in r['inputs'].values() for f in d['files'].values())>r['contract']['resources']['max_input_bytes']:raise ValueError('chronological_input_budget')
    for n,h in r['predecessors'].items():
        if sha(Path(paths['trad'])/n)!=h:raise ValueError('chronological_native_external_source_changed_before_import')
    if any((Path(paths['trad'])/n).exists() for n in r['absent_import_paths']):raise ValueError('chronological_native_shadow_import_present')
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
    return sorted([prefix+cohort['name']+'_'+str(t)+'.json' for cohort in r['contract']['cohorts'] for t in cohort['policy_origins'] for prefix in ('frame_','consumer_')]+['training_proof.json','source_references.json','run_report.json'])


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'recipe':r,'required_payloads':required(r)},dependency_hashes={**r['sources'],**r['predecessors'],**{a:d['original_run_identity'] for a,d in r['inputs'].items()}})


def operate(action,path,pin,paths,runs,crash_after=None):
    started=time.monotonic();r=preflight(path,pin,paths);c=r['contract'];parent=c['parent_surface_contract']
    from collections import Counter
    from publication import RunPublisher,verify_completed_run
    from contracts import fingerprint
    from later_surface_models_v2 import SavedModels,schedule
    from later_surface_native_v2 import NativeBatch,consume_verified
    from chronological_layer_v2 import verify_population,combine
    from chronological_native_v2 import build_frame
    import psutil
    identity=identity_for(r);pub=RunPublisher(runs,r['run_id'],identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        m=verify_completed_run(pub.root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(m['payloads']),'base_model_fits':0,'base_model_loads':0,'qualification_regressions':0,'api_calls':0,'policy_replays':0,'reused_completed':True}
    if action in ('status','verify'):
        if action=='verify':raise ValueError('chronological_native_completed_run_required')
        return {'status':'resumable' if pub.root.exists() else 'ready','run_identity':identity['fingerprint']}
    pub.acquire(recover=action=='resume');payloads=[];qualification_fits=0;timings=[]
    def put(n,v):payloads.append(pub.write_or_validate_payload(n,encoded(v)))
    def cached(n):
        b=pub.read_verified_payload(n);return None if b is None else json.loads(b)
    def measured(v):
        timings.append(v)
        with (pub.root/'TIMING_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(v));f.flush();os.fsync(f.fileno())
    def guard(phase):
        v={'phase':phase,'elapsed_seconds':time.monotonic()-started,'rss_bytes':psutil.Process().memory_info().rss,'scratch_bytes':sum(p.stat().st_size for p in pub.root.iterdir() if p.is_file()),'free_bytes':shutil.disk_usage(runs).free};lim=c['resources']
        if v['elapsed_seconds']>lim['main_wall_seconds'] or v['rss_bytes']>lim['max_rss_bytes'] or v['scratch_bytes']>lim['max_scratch_bytes'] or v['free_bytes']<lim['minimum_disk_free_bytes']:raise ValueError('chronological_native_resource_limit:'+json.dumps(v))
        with (pub.root/'RESOURCE_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(v))
        return v
    try:
        guard('before_inputs');observations=[];outcomes=[]
        for pair in c['universe']:
            part=load(paths,r,'extension','pair_'+pair+'.json');observations.extend(part['observations']);outcomes.extend(part['outcomes'])
        obs={o['record_id']:o for o in observations};lookup={(o['record_id'],o['target_id']):o for o in outcomes}
        if len(obs)!=len(observations) or len(lookup)!=len(outcomes):raise ValueError('chronological_native_unique_inputs_required')
        artifacts={};proof=[];rows={};frozen={}
        for h in c['horizons_minutes']:
            signed=load(paths,r,'remaining',f'fit_{h}.json');binary=checked(paths,r,'remaining',f'fit_{h}.joblib');artifacts['signed',h]=(signed,binary);proof.append(verify_population(signed,binary,observations,outcomes))
            alias='surface' if h in parent['new_absolute_horizons'] else 'absolute';stem=f'absolute_{h}' if alias=='surface' else f'fit_{h}_{parent["fit_cutoff"]}'
            meta=load(paths,r,alias,stem+'.json');binary=checked(paths,r,alias,stem+'.joblib');artifacts['absolute',h]=(meta,binary);proof.append(verify_population(meta,binary,observations,outcomes,signed))
            old=load(paths,r,'surface',f'prequential_{h}.json');new=load(paths,r,'chronological',f'new_base_{h}.json')
            rows[h]=combine(old,new,h,c['chronological_contract']);frozen[h]=load(paths,r,'chronological',f'frozen_{h}.json')['snapshot']
            del old,new
        put('training_proof.json',{'saved_fit_pairs':proof,'base_refits':0})
        reservations=load(paths,r,'surface','schedule.json')
        if reservations!=schedule(parent):raise ValueError('chronological_native_original_schedule_changed')
        prewarm_started=time.monotonic();predictor=SavedModels(artifacts,parent,reservations);native=NativeBatch(Path(paths['trad']));prewarm=time.monotonic()-prewarm_started
        if prewarm>c['resources']['prewarm_seconds']:raise ValueError('chronological_native_total_prewarm_exceeded')
        measured({'phase':'prewarm','elapsed_seconds':prewarm,'base_model_loads':predictor.loaded,'base_model_fits':0,'native_source_authenticated':True})
        counts=Counter();coverage=Counter();frames=0;forecast_count=0;cohort_counts={}
        for cohort in c['cohorts']:
            market=load(paths,r,'extension','market_'+cohort['name']+'.json')['market'];refs={(r['instrument'],r['price_epoch']):r for r in market['rows']};local=Counter()
            native_contract={**parent,'common_target_epoch':cohort['target'],'policy_origins':cohort['policy_origins']}
            for t in cohort['policy_origins']:
                h=(cohort['target']-t)//60;stem=cohort['name']+'_'+str(t);name='frame_'+stem+'.json';item=cached(name)
                if item is None:
                    saved=load(paths,r,'chronological',f'frame_{h}_{t}.json')
                    item,timing=build_frame(cohort,t,predictor,native,observations,rows[h],lookup,saved,frozen[h],market,c)
                    measured({'phase':'native_frame',**timing});qualification_fits+=timing['qualification_regressions']
                put(name,item);name='consumer_'+stem+'.json';consumers=cached(name)
                if consumers is None:
                    began=time.monotonic();consumers=[]
                    for pred,packet in zip(item['predictions'],item['packets']):
                        try:
                            candidate=consume_verified(packet,pred,obs[pred['record_id']],refs[pred['instrument'],t],market,native_contract,Path(paths['trad']))
                            consumers.append({'forecast_id':pred['forecast_id'],'status':'admitted','candidate':candidate})
                        except ValueError as exc:
                            if str(exc)!='financing_conversion_unavailable':raise
                            consumers.append({'forecast_id':pred['forecast_id'],'status':'financing_conversion_unavailable','candidate':None})
                    measured({'phase':'consumer_outside_native_build_clock','cohort':cohort['name'],'origin_epoch':t,'elapsed_seconds':time.monotonic()-began,'rows':len(consumers)})
                put(name,consumers)
                if len(item['coverage'])!=952 or len(item['predictions'])!=len(item['packets']) or len(consumers)!=len(item['predictions']):raise ValueError('chronological_native_complete_frame_required')
                counts.update(x['status'] for x in consumers);local.update(x['status'] for x in consumers);coverage.update(x['reason'] for x in item['coverage']);forecast_count+=len(consumers);frames+=1
                if qualification_fits>c['resources']['qualification_regression_cap']:raise ValueError('chronological_native_qualification_fit_cap')
                guard('after_'+stem)
                if crash_after==frames:os._exit(91)
            cohort_counts[cohort['name']]=dict(local)
        if frames!=16:raise ValueError('chronological_native_both_cohorts_required')
        put('run_report.json',{'status':'completed_chronological_native_inputs','frames':frames,'coverage_slots':frames*952,'native_packets':forecast_count,
            'consumer_statuses':dict(counts),'cohort_consumer_statuses':cohort_counts,'coverage_reasons':dict(coverage),'base_model_fits':0,'scientific_layer_parameters_changed':False,
            'qualification_snapshot_inventory':16,'qualification_regression_inventory':64,'api_calls':0,'policy_replays':0,'confirmation':False,'independent_review':False,**c['readiness']})
        put('source_references.json',{'parent_identities':c['parent_identities'],'source_hashes':r['sources'],'native_source_hashes':r['predecessors'],
            'saved_fit_ids':{family+'_'+str(h):meta['fit_id'] for (family,h),(meta,_) in artifacts.items()},'contract_sha256':fingerprint(c)})
        if any(sha(ROOT/n)!=digest for n,digest in r['sources'].items()) or any(sha(Path(paths['trad'])/n)!=digest for n,digest in r['predecessors'].items()):raise ValueError('chronological_native_source_changed_during_run')
        last=guard('final_precommit');pub.complete(payloads,set(required(r)));verify_completed_run(pub.root,identity)
        result={'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(payloads),'base_model_fits':0,'base_model_loads':predictor.loaded,'qualification_regressions':qualification_fits,'api_calls':0,'policy_replays':0,'resource_last':last,'phase_receipts':timings}
        with (pub.root/'COMPUTATION_ATTEMPTS.jsonl').open('ab') as f:f.write(encoded(result))
        return result
    finally:
        if pub._guard_handle is not None:pub.release()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    try:result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after)
    except Exception as exc:result={'status':'review_required','reason':str(exc)}
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)

