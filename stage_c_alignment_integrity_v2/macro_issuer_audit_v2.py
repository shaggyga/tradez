"""Offline source association audit; current configuration is not past attestation."""
from collections import Counter,defaultdict
import hashlib,json
from urllib.parse import urlsplit
from macro_receipt_population_v2 import require


def host(value):
    if not isinstance(value,str) or not value:return None
    try:
        u=urlsplit(value)
        if u.scheme not in ('https','http') or u.username is not None or u.password is not None:return None
        return u.hostname.lower().rstrip('.') if u.hostname else None
    except ValueError:return None


def host_allowed(value,domains):
    h=host(value)
    if h is None:return False
    return any(isinstance(d,str) and (h==d.lower().strip().rstrip('.') or h.endswith('.'+d.lower().strip().rstrip('.'))) for d in domains if d)


def authority_links(mapping):
    result=defaultdict(list)
    for row in mapping['currencies']:
        for key,role in [('release_source_ids','policy_release'),('communication_source_ids','policy_communication'),
                         ('statistical_release_source_ids','statistical_release'),('calendar_source_ids','calendar')]:
            for sid in row.get(key,[]):result[sid].append({'authority_id':row['authority_id'],'authority':row['authority'],
                'currency':row['currency'],'configured_role':role})
    return result


def provenance(row,binding,config,links):
    fields=row['fields'];sid=binding['source_id']
    require(fields['source_id']==sid and row['content_sha256']==binding['content_sha256'],'source_projection_binding_mismatch')
    configured=config.get(sid);domains=configured.get('trusted_domains',[]) if configured else []
    urls={k:{'url':fields.get(k),'host':host(fields.get(k)),'matches_current_trusted_domains':host_allowed(fields.get(k),domains)}
          for k in ('source_url','publisher_url','detail_source_url') if fields.get(k)}
    associations=links.get(sid,[])
    return {'version_id':row['version_id'],'source_id':sid,'source_contract_id':fields.get('source_contract_id'),
        'source_cohort_id':fields.get('source_cohort_id'),'retained_verified_flag':fields.get('source_verified'),
        'retained_source_role':fields.get('source_role'),'retained_policy_class':fields.get('policy_document_type'),
        'current_configuration_present':configured is not None,'current_trusted_domains':domains,'url_checks':urls,
        'current_authority_associations':associations,
        'associated_currencies_overlap_retained_mapping':bool(set(binding['currencies']) & {a['currency'] for a in associations}),
        'retained_issuer_binding_flag':fields.get('issuer_bound_policy_communication') is True or fields.get('issuer_bound_policy_attachment') is True,
        'retained_binding_contract':fields.get('issuer_bound_policy_communication_contract_id'),
        'retained_binding_identity':fields.get('issuer_bound_policy_communication_source_identity'),
        'retained_binding_method':fields.get('issuer_bound_policy_communication_binding_method'),
        'retained_detail_hash_present':bool(fields.get('detail_content_sha256')),
        'configuration_historical_asof_proven':False,'independent_issuer_attestation':False,
        'original_extraction_ready_epoch':None,'policy_fact_admission':False,'forecast_admission':False}


def build(blobs):
    read=lambda n:json.loads(blobs[n])
    for n,h in read('ISSUER_PREDECESSOR.json')['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'issuer_predecessor_pin_mismatch')
    bindings=read('version_bindings.json');projection=read('source_projection.json');scoped=read('scoped_text_cache.json')
    versions={b['version_id']:b for b in bindings};projected={r['version_id']:r for r in projection}
    require(len(versions)==len(bindings) and len(projected)==len(projection) and set(versions)==set(projected),'issuer_projection_population_mismatch')
    source_rows=read('news_sources_v1.json')['sources'];config={s['source_id']:s for s in source_rows}
    require(len(config)==len(source_rows),'duplicate_source_configuration')
    links=authority_links(read('official_central_bank_source_map_v1.json'));provenances=[provenance(projected[vid],b,config,links) for vid,b in sorted(versions.items())]
    bysource=defaultdict(list)
    for p in provenances:bysource[p['source_id']].append(p)
    inventory=[]
    for sid,rows in sorted(bysource.items()):
        inventory.append({'source_id':sid,'versions':len(rows),'current_configuration_present':sid in config,
            'current_authority_associations':links.get(sid,[]),'current_trusted_domains':config.get(sid,{}).get('trusted_domains',[]),
            'retained_source_roles':dict(sorted(Counter(r['retained_source_role'] or 'missing' for r in rows).items())),
            'source_url_host_matches':sum(r['url_checks'].get('source_url',{}).get('matches_current_trusted_domains',False) for r in rows),
            'retained_issuer_binding_flag_versions':sum(r['retained_issuer_binding_flag'] for r in rows),
            'historical_config_proven':False,'independent_issuer_attestation':False})
    candidates={r['cache_key']:r for r in scoped if r['action_candidate'] is not None or any(not c['guard_reasons'] for c in r['claim_evidence'])}
    selected=read('selected_scoped_versions.json');trace=[];pindex={p['version_id']:p for p in provenances}
    for vid,b in sorted(versions.items()):
        if b['cache_key'] not in candidates:continue
        r=candidates[b['cache_key']];cutoffs=[]
        for snap in selected:
            sameevent=[d for d in snap['events'] if d['event_id']==b['event_id']]
            own=next((d for d in sameevent if vid in d['version_ids']),None)
            reason=own['exclusion'] if own else 'not_selected_version_at_cutoff' if sameevent else 'event_not_visible_at_cutoff'
            cutoffs.append({'cutoff':snap['cutoff'],'selected_version':own is not None,'eligible_linguistic_context':own is not None and own['eligible_linguistic_context'],
                           'exclusion_or_absence':reason})
        trace.append({'version_id':vid,'event_id':b['event_id'],'cache_key':b['cache_key'],'source_id':b['source_id'],
            'headline':projected[vid]['fields'].get('headline'),'action_candidate':r['action_candidate'],
            'asserted_claim_dimensions':sorted({c['dimension'] for c in r['claim_evidence'] if not c['guard_reasons']}),
            'retained_kind':b['kind'],'retained_bootstrap':b['listing_bootstrap'],'published_time_inferred':b['published_time_inferred'],
            'provenance':pindex[vid],'cutoff_trace':cutoffs,'forecast_admission':False})
    gaps=[{'gate':'publisher_host','status':'current_configuration_comparison_only','needed':'historically pinned source configuration and archived transport/redirect receipt'},
          {'gate':'issuer_subject','status':'retained_flags_not_independent_assertion_proof','needed':'bind exact current decision clause to issuing authority and release identity; mixed/third-party context abstains'},
          {'gate':'publication_and_extraction_readiness','status':'not_proven','needed':'original publisher time evidence and extraction completion receipts; never use current config time as historical readiness'},
          {'gate':'numeric_units_and_vintage','status':'pending_audit','needed':'verify retained actual/prior/unit/reference fields and exact evidence before surprise or policy numeric use'},
          {'gate':'forecast_admission','status':'closed','needed':'qualified frozen features and protected evaluation; negative event-count trial unchanged'}]
    report={'schema':'macro_semantic_issuer_qualification_audit.v2','versions':len(provenances),'sources':len(inventory),
        'configured_sources':sum(s['current_configuration_present'] for s in inventory),'sources_with_authority_associations':sum(bool(s['current_authority_associations']) for s in inventory),
        'source_url_host_match_versions':sum(s['source_url_host_matches'] for s in inventory),
        'retained_issuer_binding_flag_versions':sum(p['retained_issuer_binding_flag'] for p in provenances),
        'candidate_versions':len(trace),'action_candidate_versions':sum(t['action_candidate'] is not None for t in trace),
        'action_candidate_eligible_cutoff_rows':sum(c['eligible_linguistic_context'] for t in trace if t['action_candidate'] is not None for c in t['cutoff_trace']),
        'candidate_cutoff_exclusions':dict(sorted(Counter(c['exclusion_or_absence'] or 'eligible_context' for t in trace for c in t['cutoff_trace']).items())),
        'independent_issuer_attestations':0,'historical_config_proven':False,'policy_facts_admitted':0,'forecast_features_admitted':0,
        'base_models_fitted':0,'forecast_improvement_proven':False,'current_configuration_capture_utc':read('ISSUER_CAPTURE.json')['utc'],
        'limitations':['current configuration is a diagnostic association, never a historically available feature',
            'verified/source role/issuer bound booleans are retained collector assertions, not independent verification',
            'matching configured host does not prove content authenticity, redirects, release identity or issuer subject',
            'candidate cutoff rows repeat events and texts; no independent statistical sample count',
            'no network, live collector, model, price/outcome or broker access']}
    return {'version_source_provenance.json':provenances,'source_association_inventory.json':inventory,
            'candidate_exclusion_trace.json':trace,'qualification_gaps.json':gaps,'issuer_audit_report.json':report}
