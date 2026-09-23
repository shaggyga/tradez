"""Pinned C-drive retained-model recovery; no fitting or broker entry point."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path, PurePosixPath
import platform
import sys
ROOT=Path(__file__).resolve().parent
SOURCES=('directional_recovery_operator_v2.py','directional_recovery_runner_v2.py','directional_recovery_audit_v2.py','contracts.py','publication.py')
PACKAGES=('numpy','pandas','scipy','scikit-learn','joblib','pyarrow','threadpoolctl')
MANIFEST_SHA='55b6b496c677f6dfbb39a1827e3353be0dbcbe427c4ba04e2b90c6b7f06e65d5'
ARCHIVE_SHA='a9f29a3d51e40c114655d27465ec03b269cba5c26c5bb24105c97da096d86fd9'

def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()

def verify_retained(root):
    if root.is_symlink() or root.is_junction():raise ValueError('retained_root_link_refused')
    p=root/'MANIFEST.json'
    if sha(p)!=MANIFEST_SHA:raise ValueError('retained_manifest_drift')
    m=json.loads(p.read_text());seen=set()
    for name,d in m['files'].items():
        rel=PurePosixPath(name)
        if rel.is_absolute() or '..' in rel.parts or '\\' in name or ':' in name or name.casefold() in seen:raise ValueError('retained_path_refused')
        seen.add(name.casefold());p=root.joinpath(*rel.parts)
        if any(x.is_symlink() or x.is_junction() for x in [p,*p.parents] if x!=root.parent):raise ValueError('retained_link_refused')
        if p.stat().st_size!=d['bytes'] or sha(p)!=d['sha256']:raise ValueError('retained_file_drift:'+name)
    return m

def recipe_for(root):
    verify_retained(root)
    return {'schema_version':'forex_retained_directional_recovery.v1','sources':{n:sha(ROOT/n) for n in SOURCES},
        'retained_manifest_sha256':MANIFEST_SHA,'retained_archive_sha256':ARCHIVE_SHA,
        'environment':{'python':platform.python_version(),**{n:importlib.metadata.version(n) for n in PACKAGES}},
        'run_id':'retained-directional-recovery','expected_bundles':8,'expected_numeric_values':718074,
        'scope':'retained_development_recreation_and_campaign_admission_not_new_market_evidence',
        'broker_access':False,'fit_allowed':False,'independent_review':False}

def preflight(recipe_path,digest,root):
    if recipe_path.is_symlink() or sha(recipe_path)!=digest:raise ValueError('frozen_recipe_hash_mismatch')
    r=json.loads(recipe_path.read_text())
    if r!=recipe_for(root):raise ValueError('source_input_or_environment_drift_before_runtime_import')
    return r

def operate(action,recipe_path,digest,root,runs_dir):
    try:
        if action not in {'status','run','resume','verify'}:raise ValueError('unsupported_action')
        r=preflight(recipe_path,digest,root)
        sys.path.insert(0,str(ROOT))
        from directional_recovery_runner_v2 import run,identity_for
        from publication import verify_completed_run
        identity=identity_for(r);out=runs_dir/r['run_id']
        if action in {'run','resume'}:run(root,r,runs_dir=runs_dir,resume=True)
        if (out/'COMPLETION_MANIFEST.json').exists():
            verify_completed_run(out,identity);report=json.loads((out/'run_report.json').read_text())
            if report['model_bundles_recreated']!=8 or report['numeric_values_recreated']!=718074 or report['models_refitted']!=0 or report['universe_count']!=68:raise ValueError('recovery_acceptance_missing')
            status='completed_verified'
        elif out.exists():
            if json.loads((out/'RUN_IDENTITY.json').read_text())!=identity:raise ValueError('partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('completed_run_required')
        return {'status':status,'run_identity':identity['fingerprint'],'run_path':str(out),'recipe_sha256':digest,
            'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
            'engineering_ready':False,'forecast_evidence_status':'retained_development_recreation_only',
            'policy_evidence_status':'unqualified_for_new_campaign','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','retained-root','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);a=p.parse_args()
    r=operate(a.action,a.recipe,a.recipe_sha256,a.retained_root,a.runs_dir);print(json.dumps(r,sort_keys=True));raise SystemExit(2 if r['status']=='review_required' else 0)
