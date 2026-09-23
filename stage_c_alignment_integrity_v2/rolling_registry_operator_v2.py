"""Pinned offline rolling-registry recipe; numerical imports follow preflight."""
import argparse,gzip,hashlib,importlib.metadata,io,json,platform,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCES=('rolling_registry_operator_v2.py','rolling_registry_runner_v2.py','rolling_registry_adapter_v2.py','contracts.py','publication.py')
PREDECESSORS={'oanda_rolling_technical_features_v1.py':'074a7b4fc138ef25a1e99b503ea693e0c31bc2e51a2e8f3753aab7556a6fb657','oanda_rolling_technical_panel_v1.py':'2426d0cf85dc1f1026d9ab49b0ade8925f8ba3a23730da0440a6b4c9293fb24e'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def checked_bytes(root,row):
    name=row['path']
    if Path(name).name!=name:raise ValueError('plain_registered_input_name_required')
    p=root/name
    if p.is_symlink() or p.stat().st_size!=row['bytes'] or row['bytes']>8*1024*1024:raise ValueError('bounded_input_size_required')
    raw=p.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('input_consumed_bytes_changed')
    return raw
def validate_inputs(root,lineage,trad):
    m=read(root/'INPUT_MANIFEST.json');rows=m['members']
    if len(rows)!=68 or len({r['instrument'] for r in rows})!=68 or m['support_bars']!=603 or m['origins']!=[1721606460+i*21600 for i in range(8)]:raise ValueError('frozen_all68_rolling_population_required')
    for r in rows:
        if r['path']!=r['instrument']+'.parquet':raise ValueError('rolling_pair_path_mismatch')
        checked_bytes(root,r)
    lm=read(lineage/'LINEAGE_MANIFEST.json')
    if len({r['path'] for r in lm['members']})!=len(lm['members']):raise ValueError('duplicate_lineage_members')
    for r in lm['members']:
        raw=checked_bytes(lineage,r)
        if r['encoding']=='gzip':
            if r['original_bytes']>64*1024*1024:raise ValueError('decoded_lineage_size_limit')
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:decoded=f.read(r['original_bytes']+1)
            if len(decoded)!=r['original_bytes'] or hashlib.sha256(decoded).hexdigest()!=r['original_sha256']:raise ValueError('compressed_lineage_original_mismatch')
        elif r['encoding']!='identity':raise ValueError('unsupported_lineage_encoding')
    for n,h in PREDECESSORS.items():
        if sha(trad/n)!=h:raise ValueError('canonical_rolling_predecessor_changed')
    return m,lm
def recipe_for(root,lineage,trad):
    m,lm=validate_inputs(root,lineage,trad)
    return {'schema_version':'forex_rolling_registry_recipe.v1','sources':{n:sha(ROOT/n) for n in SOURCES},'predecessors':PREDECESSORS,
        'raw_manifest_sha256':sha(root/'INPUT_MANIFEST.json'),'lineage_manifest_sha256':sha(lineage/'LINEAGE_MANIFEST.json'),
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in ('numpy','pandas','pyarrow')}},
        'universe':sorted(r['instrument'] for r in m['members']),'origins':m['origins'],'run_id':'populated-rolling-registry','fit_allowed':False,'broker_access':False}
def preflight(recipe,digest,root,lineage,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('frozen_registry_recipe_hash_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):raise ValueError('registry_source_drift_before_import')
    if r!=recipe_for(root,lineage,trad):raise ValueError('registry_dependency_environment_drift')
    return r
def operate(action,recipe,digest,root,lineage,trad,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_registry_action')
        r=preflight(recipe,digest,root,lineage,trad);sys.path.insert(0,str(trad));sys.path.insert(0,str(ROOT))
        from rolling_registry_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        i=identity_for(r);p=runs/r['run_id']
        if action in {'run','resume'}:run(root,lineage,trad,r,runs,resume=action=='resume',crash_after=crash_after)
        if (p/'COMPLETION_MANIFEST.json').exists():
            manifest=verify_completed_run(p,i)
            if {x['path'] for x in manifest['payloads']}!=set(required(r)):raise ValueError('registry_complete_payload_set_required')
            report=read(p/'run_report.json')
            if report['instruments']!=68 or report['feature_count']!=228 or report['origin_rows']!=544 or report['models_fitted']!=0:raise ValueError('registry_acceptance_missing')
            status='completed_verified'
        elif p.exists():
            if read(p/'RUN_IDENTITY.json')!=i:raise ValueError('registry_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_registry_required')
        return {'status':status,'run_identity':i['fingerprint'],'run_path':str(p),'recipe_sha256':digest,
            'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'feature_population_and_prior_lineage_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as e:return {'status':'review_required','reason':str(e),'next_action':'preserve_evidence_and_escalate'}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input-root','lineage-root','trad-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.input_root,a.lineage_root,a.trad_root,a.runs_dir,a.test_crash_after);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
