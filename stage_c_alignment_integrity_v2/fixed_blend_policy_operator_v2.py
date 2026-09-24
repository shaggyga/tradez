"""Frozen three-forecast comparison using the existing policy replay and ledgers."""
import argparse
import hashlib
import importlib.metadata
import json
import sys
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
PARENT='MATCHED_POLICY_OPERATOR_RECIPE.json'
PARENT_SHA='46080ca7c5fa35dc0d837fb5168d4cf4a2b6a34cd2967161c143ee1ab9f225b3'
METHODS=('ridge','recovered_hgb','fixed_equal_half_blend')
SCENARIOS=('candle_zero_slippage_financing','candle_one_bp_slippage_rollover')
PARITY=('policy_decisions.jsonl','policy_state.json','event_ledger.jsonl','final_state.json','accounting_audit.json')
NEW=('fixed_blend_policy_operator_v2.py','fixed_blend_remaining_policy_v2.py','FIXED_BLEND_REMAINING_CONTRACT.json','FIXED_BLEND_REMAINING_CONTRACT_REVIEWED_V2.json',PARENT)
read=lambda p:json.loads(p.read_bytes())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()


def recipe_for(input_path,trad):
    if sha(ROOT/PARENT)!=PARENT_SHA:raise ValueError('fixed_blend_parent_recipe_drift')
    parent=read(ROOT/PARENT);contract=read(ROOT/'FIXED_BLEND_REMAINING_CONTRACT_REVIEWED_V2.json')
    if contract['parent_recipe_sha256']!=PARENT_SHA or sha(input_path)!=parent['input_sha256'] or contract['input_sha256']!=parent['input_sha256']:
        raise ValueError('fixed_blend_original_input_required')
    inherited={**parent['base_recipe']['sources'],**parent['sources']}
    for n,h in inherited.items():
        if n not in ('matched_policy_input_v2.py','policy_runner_v2.py') and sha(ROOT/n)!=h:
            raise ValueError('fixed_blend_unrelated_predecessor_drift:'+n)
    from forex_operator_v2 import approved_recipe
    base=approved_recipe(trad,'synthetic_policy_continuation')
    if any(base[k]!=parent['base_recipe'][k] for k in ('predecessors','absent_import_paths','environment')):
        raise ValueError('fixed_blend_accounting_or_environment_drift')
    numerical={n:importlib.metadata.version(n) for n in parent['numerical_environment']}
    if numerical!=parent['numerical_environment']:raise ValueError('fixed_blend_numerical_environment_drift')
    return {'schema_version':'forex_fixed_remaining_policy_recipe.v1','sources':{n:sha(ROOT/n) for n in sorted(set(inherited)|set(NEW))},
            'base_recipe':base,'input_sha256':parent['input_sha256'],'contract':contract,
            'numerical_environment':numerical,'psutil':importlib.metadata.version('psutil'),
            'model_refit':False,'authenticated_model_inference':True,'broker_access':False}


def preflight(recipe,digest,input_path,trad):
    if recipe.is_symlink() or sha(recipe)!=digest:raise ValueError('fixed_blend_recipe_pin_mismatch')
    r=read(recipe)
    for n,h in r['sources'].items():
        if Path(n).name!=n or sha(ROOT/n)!=h:raise ValueError('fixed_blend_source_drift_before_import')
    if r!=recipe_for(input_path,trad):raise ValueError('fixed_blend_recipe_input_environment_drift')
    return r


def guard(runs,started,limits,phase):
    import psutil
    process=psutil.Process();rss=process.memory_info().rss
    for child in process.children(recursive=True):
        try:rss+=child.memory_info().rss
        except psutil.NoSuchProcess:pass
    size=sum(p.stat().st_size for p in runs.rglob('*') if p.is_file()) if runs.exists() else 0
    row={'phase':phase,'elapsed_seconds':time.monotonic()-started,'aggregate_rss_bytes':rss,'scratch_bytes':size}
    if row['elapsed_seconds']>limits['main_wall_seconds'] or rss>limits['max_rss_bytes'] or size+1048576>limits['max_scratch_bytes']:
        raise ValueError('fixed_blend_resource_limit:'+json.dumps(row))
    return row


def operate(action,recipe,digest,input_path,runs,trad,crash_run=None,crash_frame=None):
    try:
        if action not in ('status','run','resume','verify'):raise ValueError('fixed_blend_unknown_action')
        r=preflight(recipe,digest,input_path,trad)
        from matched_policy_fixture_v2 import fixture as original_fixture
        from fixed_blend_remaining_policy_v2 import fixture as blend_fixture
        from policy_runner_v2 import run,identity_for,required
        from publication import verify_completed_run
        data=read(input_path);records=[];observations=[];started=time.monotonic()
        originals={x['run_id']:{p['path']:p['sha256'] for p in x['payloads']} for x in r['contract']['baseline_runs']}
        for method in METHODS:
            for scenario in SCENARIOS:
                observations.append(guard(runs,started,r['contract']['resources'],'before_'+method+'_'+scenario))
                c,frames,coverage=blend_fixture(data,scenario,trad) if method==METHODS[-1] else original_fixture(data,method,scenario,trad)
                if len(coverage)!=544 or len({x['instrument'] for x in coverage})!=68:raise ValueError('fixed_blend_full_coverage_required')
                admitted=sum(x['status']=='admitted' for x in coverage)
                refusals=[x for f in frames for x in f.get('historical_refusals',[])]
                if admitted!=536 or len(refusals)!=2:raise ValueError('fixed_blend_paired_support_changed')
                for engine in r['contract']['engines']:
                    name=method+'-'+scenario+'-'+engine;path=runs/name;identity=identity_for(c,frames,trad,engine)
                    if action in ('run','resume'):
                        run(c,frames,run_id=name,runs_dir=runs,trad_root=trad,engine=engine,resume=action=='resume',
                            crash_after=crash_frame if name==crash_run else None)
                    if (path/'COMPLETION_MANIFEST.json').exists():
                        m=verify_completed_run(path,identity)
                        if {p['path'] for p in m['payloads']}!=required(len(frames)):raise ValueError('fixed_blend_payload_inventory')
                        report=read(path/'run_report.json')
                        if report['accounting_oracle']['status']!='verified' or report['decision_count']!=54:raise ValueError('fixed_blend_accounting_required')
                        if report['native_admission']['accepted']!=admitted or report['native_admission']['refused']!=len(refusals):raise ValueError('fixed_blend_admission_mismatch')
                        if any(a['open_lot_count'] or a['pending_order_units'] or a['rejected_events'] for a in report['arms'].values()):raise ValueError('fixed_blend_terminal_reconciliation')
                        if any(report['actions'][arm]['ENTER'] for arm in ('cash','recovered')):raise ValueError('fixed_blend_unsupported_arm_entry')
                        if method!=METHODS[-1] and any(sha(path/n)!=originals[name][n] for n in PARITY):raise ValueError('fixed_blend_original_economic_regression')
                        status='completed_verified'
                    elif path.exists():
                        if read(path/'RUN_IDENTITY.json')!=identity:raise ValueError('fixed_blend_partial_identity_mismatch')
                        status='resumable'
                    else:status='ready'
                    records.append({'run_id':name,'method':method,'scenario':scenario,'engine':engine,'path':str(path),'status':status,
                                    'run_identity':identity['fingerprint'],'coverage':coverage,'admitted':admitted,'refused':len(refusals)})
                    observations.append(guard(runs,started,r['contract']['resources'],'after_'+name))
                pair=[x for x in records if x['method']==method and x['scenario']==scenario]
                if all(x['status']=='completed_verified' for x in pair):
                    for n in PARITY:
                        if sha(Path(pair[0]['path'])/n)!=sha(Path(pair[1]['path'])/n):raise ValueError('fixed_blend_engine_parity:'+n)
        complete=all(x['status']=='completed_verified' for x in records)
        if action=='verify' and not complete:raise ValueError('fixed_blend_completed_runs_required')
        return {'status':'completed_verified' if complete else 'resumable' if any(x['status']=='resumable' for x in records) else 'ready',
                'runs':records,'recipe_sha256':digest,'resource_observations':observations,
                'resource_scope':'phase_boundary_samples_not_continuous_quota','base_model_fits':0,'api_calls':0,
                **r['contract']['readiness'],'independent_review':False}
    except Exception as exc:return {'status':'review_required','reason':str(exc),'next_action':'preserve_evidence_and_resolve'}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','input','runs-dir','trad-root'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-run');p.add_argument('--test-crash-frame',type=int)
    a=p.parse_args();result=operate(a.action,a.recipe,a.recipe_sha256,a.input,a.runs_dir,a.trad_root,a.test_crash_run,a.test_crash_frame)
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
