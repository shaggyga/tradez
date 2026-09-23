"""Exclusive, durable per-pair rolling feature publication and consumer checks."""
import io,json,os
import pandas as pd
from publication import RunPublisher,effective_run_identity,verify_completed_run
from contracts import fingerprint
from rolling_registry_adapter_v2 import pair_features,join_peers,populated_registry,consume,verify_normalizer
def encoded(x):return (json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def required(r):return ['core_'+p+'.json' for p in r['universe']]+['pair_'+p+'.json' for p in r['universe']]+['populated_registry.json','lineage_summary.json','input_contract.json','consumer_checks.json','run_report.json']
def identity_for(r):return effective_run_identity(contract={'recipe':r,'required_payloads':sorted(required(r))},dependency_hashes={**r['sources'],**r['predecessors'],'raw_manifest':r['raw_manifest_sha256'],'lineage_manifest':r['lineage_manifest_sha256']})
def run(root,lineage,trad,recipe,runs,*,resume=False,crash_after=None):
    from rolling_registry_operator_v2 import validate_inputs,checked_bytes
    m,lm=validate_inputs(root,lineage,trad);identity=identity_for(recipe);p=RunPublisher(runs,recipe['run_id'],identity)
    if (p.root/'COMPLETION_MANIFEST.json').exists():verify_completed_run(p.root,identity);return
    p.acquire(recover=resume)
    try:
        parts=[];payloads=[]
        for index,row in enumerate(sorted(m['members'],key=lambda r:r['instrument']),1):
            name='core_'+row['instrument']+'.json';raw=p.read_verified_payload(name)
            if raw is None:raw=encoded(pair_features(row['instrument'],pd.read_parquet(io.BytesIO(checked_bytes(root,row))),row['pip_size'],row['source_member_sha256'],m['origins']))
            part=json.loads(raw)
            if part['instrument']!=row['instrument']:raise ValueError('cached_rolling_pair_mismatch')
            parts.append(part);payloads.append(p.write_or_validate_payload(name,raw))
            if crash_after==index:os._exit(91)
        parts=join_peers(parts,m['origins'])
        lineage_rows={r['path']:r for r in lm['members']}
        ls=json.loads(checked_bytes(lineage,lineage_rows['LINEAGE_SUMMARY.json']))
        ls['normalizer_verification']={k:verify_normalizer(checked_bytes(lineage,lineage_rows['normalizers_'+k+'.npz']),json.loads(checked_bytes(lineage,lineage_rows['normalizers_'+k+'.json'])),ls['rolling_groups']['compact50'],recipe['universe'],max(m['origins'])) for k in ('a','b')}
        registry=populated_registry(parts,ls)
        consumer=[]
        for part in parts:
            payloads.append(p.write_or_validate_payload('pair_'+part['instrument']+'.json',encoded(part)))
            for record in part['observations']:
                view=consume(record,asof=record['origin_epoch'],feature_names=ls['rolling_groups']['compact50'])
                if view['outcomes_included'] or view['fit_performed'] or len(view['values'])!=50:raise ValueError('feature_consumer_contract_failed')
                consumer.append({'record_id':record['record_id'],'view_sha256':fingerprint(view),'finite_selected_fields':sum(not x for x in view['missing_mask'])})
        report={'status':'verified_populated_rolling_registry','instruments':len(parts),'feature_count':registry['feature_count'],'origin_rows':registry['origin_rows'],
            'nonmissing_feature_cells':sum(r['population']['finite'] for r in registry['columns']),'fully_missing_columns':[r['name'] for r in registry['columns'] if r['population']['finite']==0],
            'aliases_not_extra_inputs':13,'families':registry['families'],'models_fitted':0,'consumer_rows_checked':len(consumer),'selected_consumer_fields':50,
            'prior_artifacts_rehashed':sum(x['artifacts_rehashed'] for x in ls['rolling_runs'].values()),'engineering_ready':False,
            'forecast_evidence_status':'bounded_feature_population_only','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False,
            'limitations':['eight_origins_two_dates_not_full_period_materialization','historical_close_availability_assumed','pip_conventions_applied_stationary_from2026metadata','no_legacy_weights_admitted_into2024','fixed_UTC_bands_not_venue_sessions','prior_text_and_artifact_hash_recovery_not_new_legacy_model_replay'],
            'next_item':'rich_feature_campaign_input_expansion_v2'}
        for name,obj in [('populated_registry.json',registry),('lineage_summary.json',ls),('input_contract.json',m),('consumer_checks.json',consumer),('run_report.json',report)]:payloads.append(p.write_or_validate_payload(name,encoded(obj)))
        if crash_after==0:os._exit(91)
        p.complete(payloads,set(required(recipe)))
    except BaseException:
        if p._owner_token is not None:p.release()
        raise
