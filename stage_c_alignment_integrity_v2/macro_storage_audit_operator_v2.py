"""Frozen offline receipt population; bounded data expansion and single writer."""
import argparse
import hashlib
import importlib.metadata
import json
import platform
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = ('macro_storage_audit_operator_v2.py','macro_storage_audit_v2.py','macro_version_text_v2.py','macro_text_layer_v2.py',
           'macro_receipt_population_v2.py','contracts.py','publication.py')
BASE_INPUTS = ('retained_versions.json','replacement_text_diffs.json','event_boundary_histories.json','source_ledger.py','upsert_fragment.py','STORAGE_CAPTURE.json','STORAGE_PLAN.json')
REQUIRED = ('source_order_evidence.json','frozen_projection_guard_comparison.json','original_guard_challenges.json','representation_repair_requirements.json','storage_audit_report.json')
MAX_INPUT_BYTES = 8*1024*1024
MAX_TOTAL_INPUT_BYTES = 32*1024*1024


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def encoded(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def input_names(inputs):return BASE_INPUTS


def recipe_for(inputs):
    names = input_names(inputs)
    total = 0
    for name in names:
        p = inputs/name
        if not p.is_file() or p.is_symlink() or p.stat().st_size > MAX_INPUT_BYTES:
            raise ValueError('plain_bounded_receipt_inputs_required')
        total += p.stat().st_size
    if total > MAX_TOTAL_INPUT_BYTES:
        raise ValueError('receipt_total_input_limit')
    return {'schema':'macro_storage_audit_recipe.v2','run_id':'storage-audit-20260914-20260921',
            'sources':{n:sha(ROOT/n) for n in SOURCES},'inputs':{n:sha(inputs/n) for n in names},
            'environment':{'python':platform.python_version(),'numpy':importlib.metadata.version('numpy')},
            'configuration':{'network_allowed':False,'live_database_access':False,'broker_access':False,
                             'historical_forecast_admission':False,'max_wall_seconds':120,
                             'max_compressed_member_bytes':MAX_INPUT_BYTES,'max_expanded_json_member_bytes':32*1024*1024,
                             'max_expanded_json_total_bytes':64*1024*1024},'required_payloads':list(REQUIRED)}


def preflight(recipe,digest,inputs):
    if recipe.is_symlink() or sha(recipe)!=digest:
        raise ValueError('receipt_recipe_pin_mismatch')
    r=read(recipe)
    if set(r['sources'])!=set(SOURCES) or any(sha(ROOT/n)!=h for n,h in r['sources'].items()):
        raise ValueError('receipt_source_drift')
    if r!=recipe_for(inputs):
        raise ValueError('receipt_input_or_environment_drift')
    return r


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract=r,dependency_hashes={**r['sources'],**r['inputs']})


def consumed(inputs,r):
    blobs={}
    for name,h in r['inputs'].items():
        b=(inputs/name).read_bytes()
        if len(b)>MAX_INPUT_BYTES or hashlib.sha256(b).hexdigest()!=h:
            raise ValueError('consumed_receipt_input_drift')
        blobs[name]=b
    if sum(map(len,blobs.values()))>MAX_TOTAL_INPUT_BYTES:
        raise ValueError('consumed_receipt_total_limit')
    return blobs


def operate(action,recipe,digest,inputs,runs,crash_after=None):
    try:
        if action not in {'status','run','resume','verify'}:
            raise ValueError('unsupported_receipt_action')
        r=preflight(recipe,digest,inputs)
        sys.path.insert(0,str(ROOT))
        from publication import RunPublisher,verify_completed_run
        from macro_storage_audit_v2 import build
        identity=identity_for(r);root=runs/r['run_id']
        if action in {'run','resume'} and not (root/'COMPLETION_MANIFEST.json').exists():
            publisher=RunPublisher(runs,r['run_id'],identity);publisher.acquire(recover=action=='resume')
            try:
                started=time.monotonic();blobs=consumed(inputs,r)
                outputs=build(blobs)
                if time.monotonic()-started>r['configuration']['max_wall_seconds']:
                    raise ValueError('receipt_wall_budget_exceeded_before_publication')
                payloads=[]
                for index,name in enumerate(REQUIRED,1):
                    payloads.append(publisher.write_or_validate_payload(name,encoded(outputs[name])))
                    if crash_after==index:
                        import os
                        os._exit(91)
                publisher.complete(payloads,set(REQUIRED))
            except BaseException:
                if publisher._owner_token is not None:publisher.release()
                raise
        if (root/'COMPLETION_MANIFEST.json').exists():
            manifest=verify_completed_run(root,identity)
            if {p['path'] for p in manifest['payloads']}!=set(REQUIRED):raise ValueError('exact_receipt_inventory_required')
            report=read(root/'storage_audit_report.json')
            if report['source_database_writes'] or report['base_models_fitted']!=0 or report['forecast_improvement_proven'] or report['forecast_features_admitted']!=0:
                raise ValueError('receipt_scope_violation')
            status='completed_verified'
        elif root.exists():
            if read(root/'RUN_IDENTITY.json')!=identity:raise ValueError('receipt_partial_identity_mismatch')
            status='resumable'
        else:status='ready'
        if action=='verify' and status!='completed_verified':raise ValueError('receipt_completion_required')
        return {'status':status,'run_path':str(root),'run_identity':identity['fingerprint'],'recipe_sha256':digest,
                'next_action':'record_receipt_and_stop' if status=='completed_verified' else 'resume' if status=='resumable' else 'run',
                'engineering_ready':False,'forecast_evidence_status':'offline_scoped_linguistic_candidates_not_verified_policy_facts',
                'policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    except Exception as exc:
        return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_escalate'}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','inputs','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    result=operate(a.action,a.recipe,a.recipe_sha256,a.inputs,a.runs_dir,a.test_crash_after)
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
