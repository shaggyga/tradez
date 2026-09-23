"""Durable rich feature generation with exact original observation/target alignment."""
import io,json,os
import numpy as np
import pandas as pd
from publication import RunPublisher,effective_run_identity,verify_completed_run
from contracts import fingerprint
from rolling_registry_adapter_v2 import pair_features,join_peers,populated_registry
from rolling_registry_operator_v2 import checked_bytes
from rich_campaign_consumer_v2 import feature_sets,view
def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def required(r):return [prefix+p+'.json' for prefix in ('core_','pair_','outcomes_') for p in r['universe']]+['populated_registry.json','lineage_summary.json','input_contract.json','consumer_checks.json','feature_sets.json','run_report.json']
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':sorted(required(r))},dependency_hashes={**r['sources'],**r['predecessors'],'raw_manifest':r['raw_manifest_sha256'],'technical':r['technical_identity']['fingerprint'],'lineage':r['lineage_sha256']})
def original(technical,recipe,name):
    raw=(technical/name).read_bytes()
    import hashlib
    if hashlib.sha256(raw).hexdigest()!=recipe['technical_payloads'][name]:raise ValueError('original_technical_consumed_bytes_changed')
    return json.loads(raw)
def quotes(frame,origins):
    result={}
    for origin in origins:
        rows=frame.loc[frame.time==origin-60];costs=[None,None]
        if len(rows)==1:
            row=rows.iloc[0];mid,bid,ask=map(float,(row.close,row.bid_close,row.ask_close))
            if np.isfinite([mid,bid,ask]).all() and 0<bid<=mid<=ask:costs=[(ask-mid)/mid*10000,(mid-bid)/mid*10000]
        result[str(origin)]=costs
    return result
def run(root,lineage,technical,recipe,runs,*,resume=False,crash_after=None):
    m=json.loads((root/'INPUT_MANIFEST.json').read_text());identity=identity_for(recipe);p=RunPublisher(runs,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        cores=[];payloads=[]
        for index,row in enumerate(sorted(m['members'],key=lambda x:x['instrument']),1):
            name='core_'+row['instrument']+'.json';raw=p.read_verified_payload(name)
            if raw is None:
                frame=pd.read_parquet(io.BytesIO(checked_bytes(root,row)))
                core=pair_features(row['instrument'],frame,row['pip_size'],row['source_member_sha256'],m['origins']);core['known_entry_costs']=quotes(frame,m['origins']);raw=encoded(core)
            core=json.loads(raw)
            if core['instrument']!=row['instrument']:raise ValueError('rich_cached_pair_identity_mismatch')
            cores.append(core);payloads.append(p.write_or_validate_payload(name,raw))
            if crash_after==index:os._exit(91)
        parts=join_peers(cores,m['origins']);core_by={x['instrument']:x for x in cores}
        import hashlib
        raw=lineage.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=recipe['lineage_sha256']:raise ValueError('rich_lineage_consumed_bytes_changed')
        ls=json.loads(raw);legacy_contract=original(technical,recipe,'input_contract.json');groups=feature_sets(ls,legacy_contract['features']);checks=[];eligible=0;endpoint_count=0
        for part in parts:
            pair=part['instrument'];prior=original(technical,recipe,'pair_'+pair+'.json');old={r['record_id']:r for r in prior['observations']}
            if set(old)!={r['record_id'] for r in part['observations']}:raise ValueError('rich_original_population_keys_mismatch')
            for record in part['observations']:
                legacy=old[record['record_id']]
                if (legacy['instrument'],legacy['origin_epoch'],legacy['source_bar_start_epoch'],legacy['source_member_sha256'])!=(pair,record['origin_epoch'],record['source_bar_start_epoch'],record['source_member_sha256']):raise ValueError('rich_original_source_clock_mismatch')
                costs=core_by[pair]['known_entry_costs'][str(record['origin_epoch'])]
                if legacy['features'] is not None and costs!=legacy['features'][-2:]:raise ValueError('rich_original_entry_cost_mismatch')
                original_hash=record.pop('record_sha256');record.update(original_rich_record_sha256=original_hash,original_legacy_observation=legacy,
                    known_entry_costs_bps=costs,legacy_population_eligible=legacy['features'] is not None,
                    legacy_source={'run_identity':recipe['technical_identity']['fingerprint'],'payload_sha256':recipe['technical_payloads']['pair_'+pair+'.json']})
                record['record_sha256']=fingerprint(record);eligible+=record['legacy_population_eligible']
                for group in groups:
                    v=view(record,asof=record['available_epoch'],group=group,lineage=ls,legacy_names=legacy_contract['features'])
                    checks.append({'record_id':record['record_id'],'group':group,'view_sha256':fingerprint(v),'finite_fields':sum(not x for x in v['missing_mask'])})
            payloads.append(p.write_or_validate_payload('pair_'+pair+'.json',encoded(part)))
            outcomes={k:prior[k] for k in ('outcomes','path_outcomes','calendar_coverage')};endpoint_count+=len(outcomes['outcomes'])
            payloads.append(p.write_or_validate_payload('outcomes_'+pair+'.json',encoded(outcomes)))
        registry=populated_registry(parts,ls)
        report={'status':'verified_matched_rich_campaign_inputs','instruments':len(parts),'origin_rows':registry['origin_rows'],'feature_count':228,
            'shared_legacy_eligible_rows':eligible,'endpoint_outcome_rows':endpoint_count,'outcomes_recomputed':False,'models_fitted':0,'feature_groups':{k:len(v) for k,v in groups.items()},
            'consumer_views_checked':len(checks),'finite_feature_cells':sum(x['population']['finite'] for x in registry['columns']),
            'original_observation_and_outcome_identities_preserved':True,'engineering_ready':False,'forecast_evidence_status':'matched_inputs_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,
            'limitations':['already_inspected_development_dates','historical_bar_end_availability_assumed','partial_rich_features_require_train_only_transforms','elapsed_endpoints_not_qualified_sessions_or_paths','future2026weights_and_normalizers_not_admitted'],
            'next_item':'matched_rich_feature_family_comparison_v2'}
        for name,obj in [('populated_registry.json',registry),('lineage_summary.json',ls),('input_contract.json',m),('consumer_checks.json',checks),('feature_sets.json',groups),('run_report.json',report)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required(recipe)))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
