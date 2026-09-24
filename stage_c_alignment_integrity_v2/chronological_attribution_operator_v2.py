"""Bounded zero-model attribution with authenticated subsets and journal recovery."""
import argparse,hashlib,importlib.metadata,json,os,platform,shutil,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
RECIPE='CHRONOLOGICAL_ATTRIBUTION_OPERATOR_RECIPE_REVIEWED_V2.json'
CONTRACT='CHRONOLOGICAL_ATTRIBUTION_CONTRACT_V2.json'
SOURCES=[CONTRACT,'chronological_attribution_operator_v2.py','chronological_attribution_v2.py','layer_policy_attribution_v2.py','chronological_policy_report_v2.py','chronological_policy_scenario_v2.py','CHRONOLOGICAL_POLICY_CONTRACT_V2.json','contracts.py','publication.py']
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


def required(r):return [*[f'forecasts_{c}.json' for c in r['contract']['cohorts']],*[f'path_{i:02}.json' for i in range(56)],'report.json','report.md']


def identity_for(r):
    from publication import effective_run_identity
    return effective_run_identity(contract={'experiment':r['contract'],'recipe':r,'required_payloads':required(r)},
        dependency_hashes={**r['sources'],**{a:d['original_run_identity'] for a,d in r['inputs'].items()}})


def render(summary):
    lines=['# Chronological forecast and policy attribution','','Descriptive inspected development. All paths retained; no new model, counterfactual policy or causal claim.',
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
    lines+=['','Both overlapping cohorts, exact Decimal values, all six arms, pair/currency participation, unique selected forecast errors, filled episodes and reasons are in JSON.',
      'Episode participation is not concurrent or time-weighted exposure. Errors are midpoint basis points; account P&L includes simulated execution and applied costs. Rejected financing is not a valid zero charge. Selected-but-unfilled forecasts are separated from filled-episode forecast errors. Delayed openings retain their original decision-to-target forecast error, distinct from actual fill-to-exit return. Endpoint ranks use original candidate sets and matured labels; they are retrospective midpoint diagnostics. No unrun alternative is assigned a P&L.']
    return '\n'.join(lines)+'\n'


def operate(action,path,pin,paths,runs,crash_after=None):
    started=time.monotonic();r=preflight(path,pin,paths)
    from publication import RunPublisher,verify_completed_run
    from chronological_attribution_v2 import forecast_table,path_attribution,metrics,D
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
        guard('before_inputs');outcomes=[]
        for name in r['inputs']['extension']['files']:
            if name.startswith('pair_'):outcomes.extend(load(paths,r,'extension',name)['outcomes'])
        tables={};lookups={};assessments={}
        for cohort,scope in c['cohorts'].items():
            local={**c,**scope,'cohort':cohort}
            frames={str(t):load(paths,r,'native',f'frame_{cohort}_{t}.json') for t in scope['origins']}
            tables[cohort],lookups[cohort],assessments[cohort]=forecast_table(frames,outcomes,local)
            support_checks=[]
            for base in ('ridge','recovered_hgb'):
                for mode in ('frozen','expanding'):
                    methods=[base+'__'+v+'_'+mode for v in ('raw_matched','signed_only','magnitude_interaction')]
                    supports=[{(x['instrument'],x['origin_epoch'],x['target_epoch']) for x in tables[cohort]['rows'] if x['method']==m} for m in methods]
                    if not supports[0]==supports[1]==supports[2]:raise ValueError('chronological_attribution_matched_support_changed')
                    support_checks.append({'base':base,'mode':mode,'matched_forecasts_per_method':len(supports[0])})
            tables[cohort]['matched_support_checks']=support_checks
            put('forecasts_'+cohort+'.json',tables[cohort])
        del frames,outcomes
        paths_out=[];aliases=sorted(a for a in r['inputs'] if a.startswith('policy_'))
        if len(aliases)!=56:raise ValueError('attribution_full56path_inventory')
        for i,alias in enumerate(aliases):
            guard('before_'+alias);name=f'path_{i:02}.json';cached=publisher.read_verified_payload(name)
            cohort=r['inputs'][alias]['cohort'];local={**c,**c['cohorts'][cohort],'cohort':cohort}
            if cached is None:
                decisions=[json.loads(x) for x in checked_bytes(paths,r,alias,'policy_decisions.jsonl').splitlines()]
                events=[json.loads(x) for x in checked_bytes(paths,r,alias,'event_ledger.jsonl').splitlines()]
                value=path_attribution(alias.removeprefix('policy_'),load(paths,r,alias,'run_inputs.json'),decisions,events,
                    load(paths,r,alias,'policy_state.json'),load(paths,r,alias,'run_report.json'),lookups[cohort],assessments[cohort],local)
            else:value=json.loads(cached)
            put(name,value);paths_out.append(value)
            if crash_after==i+1:os._exit(91)
        report={'schema_version':'forex_chronological_attribution.v1','paths':paths_out,'forecast_metrics':{k:v['metrics'] for k,v in tables.items()},
          'coverage_slots':sum(len(v['coverage']) for v in tables.values()),'eligible_forecasts':sum(len(v['rows']) for v in tables.values()),
          'reconciled_accounts':sum(len(x['arms']) for x in paths_out),'confirmation':False,'independent_review':False,
          'scope':c['uncertainty'],**c['readiness']}
        if report['reconciled_accounts']!=336 or report['coverage_slots']!=15232 or report['eligible_forecasts']!=11048:raise ValueError('attribution_full_account_coverage_required')
        totals={k:sum(a[k] for p in paths_out for a in p['arms']) for k in ('selected_decisions','filled_selected_decisions','selected_without_open_episode','delayed_open_fills')}
        if totals!=c['parent_execution_totals']:raise ValueError('chronological_attribution_parent_execution_totals_changed')
        report['execution_totals']=totals
        report['matched_support_checks']={k:v['matched_support_checks'] for k,v in tables.items()}
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
