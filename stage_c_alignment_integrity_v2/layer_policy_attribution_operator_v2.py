"""Bounded zero-model attribution with authenticated subsets and journal recovery."""
import argparse,hashlib,importlib.metadata,json,os,platform,shutil,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
RECIPE='LAYER_POLICY_ATTRIBUTION_OPERATOR_RECIPE_FINAL_V2.json'
CONTRACT='LAYER_POLICY_ATTRIBUTION_CONTRACT_V2.json'
SOURCES=[CONTRACT,'layer_policy_attribution_operator_v2.py','layer_policy_attribution_v2.py','contracts.py','publication.py']
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
    if set(paths)!=set(r['inputs']):raise ValueError('attribution_exact_parent_map_required')
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


def required(r):return ['forecasts.json',*[f'path_{i:02}.json' for i in range(28)],'report.json','report.md']


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'experiment':r['contract'],'recipe':r,'required_payloads':required(r)},
        dependency_hashes={**r['sources'],**{a:d['original_run_identity'] for a,d in r['inputs'].items()}})


def render(summary):
    lines=['# Layer policy failure attribution','','Descriptive inspected development. All paths retained; no new model, counterfactual policy or causal claim.',
      '','| Forecast | Cost | Arm | Net USD | Fees USD | Financing USD | Financing applied | Episodes | Largest realized loss pair | Missing liquidation decisions |',
      '|---|---|---|---:|---:|---:|---|---:|---|---:|']
    for p in summary['paths']:
        for a in p['arms']:
            if a['arm'] in ('cash','recovered'):continue
            loss=min(a['realized_usd_by_instrument'],key=lambda k:float(a['realized_usd_by_instrument'][k]),default=None)
            if loss is not None and float(a['realized_usd_by_instrument'][loss])>=0:loss=None
            lines.append('| '+' | '.join(map(str,[p['method'],p['scenario'].removeprefix('later_candle_'),a['arm'],
                format(float(a['net_account_pnl_usd']),'.4f'),format(float(a['fill_fees_usd']),'.4f'),format(float(a['financing_usd']),'.4f'),
                'complete' if a['financing_application_complete'] else 'REJECTED',a['episode_count'],loss or 'none',a['unavailable_liquidation_decisions']]))+' |')
    lines+=['','Exact Decimal values, all six arms, pair/currency participation, unique selected forecast errors, filled episodes and reasons are in JSON.',
      'Episode participation is not concurrent or time-weighted exposure. Errors are midpoint basis points; account P&L includes simulated execution and applied costs. Rejected financing is not a valid zero charge. Selected-but-unfilled forecasts are separated from filled-episode forecast errors. No unrun alternative is assigned a P&L.']
    return '\n'.join(lines)+'\n'


def operate(action,path,pin,paths,runs,crash_after=None):
    started=time.monotonic();r=preflight(path,pin,paths)
    from publication import RunPublisher,verify_completed_run
    from layer_policy_attribution_v2 import forecast_table,path_attribution,metrics,D
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
          'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'api_calls':0,'reused_completed':True}
    if action in ('status','verify'):
        if action=='verify':raise ValueError('attribution_completed_run_required')
        return {'status':'resumable' if root.exists() else 'ready','run_identity':identity['fingerprint']}
    publisher=RunPublisher(runs,r['run_id'],identity);publisher.acquire(recover=action=='resume');payloads=[]
    def put(name,obj,raw=False):payloads.append(publisher.write_or_validate_payload(name,obj if raw else encoded(obj)))
    try:
        guard('before_inputs');frames={str(t):load(paths,r,'surface',f'frame_{t}.json') for t in c['origins']}
        outcomes=[]
        for name in r['inputs']['technical']['files']:
            if name.startswith('pair_'):outcomes.extend(load(paths,r,'technical',name)['outcomes'])
        outcomes.extend(load(paths,r,'remaining','remaining_outcomes.json'))
        forecasts,predictions,assessment=forecast_table(frames,outcomes,c)
        parent=load(paths,r,'surface','assessment.json')
        for score in parent['scores']:
            rows=[x for x in forecasts['rows'] if x['method']==score['base_method']+'__'+score['variant'] and (x['target_epoch']-x['origin_epoch'])//60==score['horizon_minutes']]
            actual=metrics(rows)
            if actual['mature_rows']!=score['mature_rows']:raise ValueError('attribution_parent_metric_support_mismatch')
            for key in ('mae_bps','mse_bps2','bias_bps'):
                if actual[key] is None or score[key] is None:
                    if actual[key]!=score[key]:raise ValueError('attribution_parent_metric_missingness_mismatch')
                elif abs(D(actual[key])-D(score[key]))>D(c['tolerances']['source_float_metric_absolute'])+abs(D(score[key]))*D(c['tolerances']['source_float_metric_relative']):
                    raise ValueError('attribution_parent_metric_mismatch')
        put('forecasts.json',forecasts);del frames,outcomes
        paths_out=[];aliases=sorted(a for a in r['inputs'] if a.startswith('policy_'))
        if len(aliases)!=28:raise ValueError('attribution_full28path_inventory')
        for i,alias in enumerate(aliases):
            guard('before_'+alias);name=f'path_{i:02}.json';cached=publisher.read_verified_payload(name)
            if cached is None:
                decisions=[json.loads(x) for x in checked_bytes(paths,r,alias,'policy_decisions.jsonl').splitlines()]
                events=[json.loads(x) for x in checked_bytes(paths,r,alias,'event_ledger.jsonl').splitlines()]
                value=path_attribution(alias.removeprefix('policy_'),load(paths,r,alias,'run_inputs.json'),decisions,events,
                    load(paths,r,alias,'policy_state.json'),load(paths,r,alias,'run_report.json'),predictions,assessment,c)
            else:value=json.loads(cached)
            put(name,value);paths_out.append(value)
            if crash_after==i+1:os._exit(91)
        report={'schema_version':'forex_layer_policy_attribution.v1','paths':paths_out,'forecast_metrics':forecasts['metrics'],
          'coverage_slots':len(forecasts['coverage']),'eligible_forecasts':len(forecasts['rows']),
          'reconciled_accounts':sum(len(x['arms']) for x in paths_out),'confirmation':False,'independent_review':False,
          'scope':c['uncertainty'],**c['readiness']}
        if report['reconciled_accounts']!=168 or report['coverage_slots']!=7616:raise ValueError('attribution_full_account_coverage_required')
        put('report.json',report);put('report.md',render(report).encode(),raw=True)
        if any(sha(ROOT/n)!=v for n,v in r['sources'].items()):raise ValueError('attribution_source_changed_during_run')
        last=guard('final_precommit');publisher.complete(payloads,set(required(r)));verify_completed_run(root,identity)
        return {'status':'completed_verified','run_identity':identity['fingerprint'],'payloads':len(payloads),
          'base_model_fits':0,'base_model_loads':0,'layer_fits':0,'api_calls':0,'resource_last':last}
    finally:
        if publisher._guard_handle is not None:publisher.release()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['status','run','resume','verify'])
    for n in ('recipe','paths','runs-dir'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--recipe-sha256',required=True);p.add_argument('--test-crash-after',type=int);a=p.parse_args()
    try:result=operate(a.action,a.recipe,a.recipe_sha256,read(a.paths),a.runs_dir,a.test_crash_after)
    except Exception as exc:result={'status':'review_required','reason':str(exc)}
    print(json.dumps(result,sort_keys=True));raise SystemExit(2 if result['status']=='review_required' else 0)
