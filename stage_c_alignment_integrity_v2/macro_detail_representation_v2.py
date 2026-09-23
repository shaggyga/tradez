"""Retained detail representation alongside latest raw observations, offline only."""
from collections import Counter
import hashlib,json,types
from contracts import fingerprint
from macro_version_text_v2 import select_versions
from macro_scoped_asof_v2 import validate_caches,snapshot as scoped_snapshot,pair_views
from macro_text_layer_v2 import epoch
from macro_receipt_population_v2 import require


def validate_metadata(bindings,rows):
    index={r['version_id']:r for r in rows};versions={b['version_id']:b for b in bindings}
    require(len(index)==len(rows) and set(index)==set(versions),'representation_metadata_population_mismatch')
    for vid,r in index.items():
        require(r['content_sha256']==versions[vid]['content_sha256'],'representation_metadata_content_mismatch')
        require(type(r['detail_enriched']) is bool,'representation_flag_must_be_boolean')
        # The frozen schema cannot authenticate a correction or retraction.
        # Encountering a supplied assertion requires a separately reviewed gate.
        require(r.get('verified_retraction_evidence') is None,'unsupported_retraction_evidence_requires_review')
    return index


def detail_snapshot(bindings,texts,scoped,metadata,cutoff,currencies,max_age_days=35):
    detail_bindings=[b for b in bindings if metadata[b['version_id']]['detail_enriched']]
    raw=select_versions(bindings,cutoff)
    details,states=scoped_snapshot(detail_bindings,texts,scoped,cutoff,currencies,max_age_days)
    raw_index={r['event_id']:r for r in raw};detail_index={d['event_id']:d for d in details};representations=[]
    for event,r in sorted(raw_index.items()):
        d=detail_index.get(event);raw_ids=r['contributing_version_ids'];detail_ids=d['version_ids'] if d else []
        separated=bool(d) and set(raw_ids)!=set(detail_ids)
        representations.append({'event_id':event,'raw_currencies':r['currencies'],'latest_raw_version_ids':raw_ids,
            'raw_version_status':r['version_status'],'detail_version_ids':detail_ids,
            'detail_context':d,'detail_is_separate_from_latest_raw':separated,
            'replacement_semantics':'unresolved_later_raw_representation' if separated else 'same_representation' if d else 'no_visible_detail',
            'publisher_correction_or_retraction_absence_proven':False,'policy_fact_admission':False,'forecast_admission':False})
    for s in states:
        s['raw_visible_events']=sum(s['currency'] in r['raw_currencies'] for r in representations)
        s['separate_raw_detail_event_ids']=sorted(r['event_id'] for r in representations if r['detail_is_separate_from_latest_raw']
            and r['detail_context'] is not None and s['currency'] in r['detail_context']['currencies'])
        s['representation_scope']='visible_events_and_candidates_are_retained_enriched_context_not_latest_raw_or_verified_policy_facts'
        s['correction_retraction_status']='not_established_by_retained_schema'
        s.pop('state_sha256');s['state_sha256']=fingerprint(s)
    return representations,states


def original_guard_evidence(raw):
    ledger=types.ModuleType('pinned_original_source_ledger');exec(compile(raw,'<pinned-original-ledger>','exec'),ledger.__dict__)
    tests=[]
    for label,previous,incoming,expected in [('protect_enriched_from_listing',{'detail_enriched':True},{'detail_enriched':False},False),
            ('accept_new_detail',{}, {'detail_enriched':True},True),('enriched_from_listing',{'detail_enriched':False},{'detail_enriched':True},True)]:
        actual=ledger.select_observation({'eligible':True},previous,incoming)
        require(actual==expected,'inherited_detail_guard_changed')
        tests.append({'case':label,'original_projection_selection':actual,'expected':expected})
    return {'exact_original_guard_cases':tests,'reuse_scope':'detail-preservation intent; original raw selector and collector unchanged',
        'differences':['offline representation view uses existing same-clock ambiguity abstention, not original hash-order winner',
          'keeps both raw and enriched representations, does not claim to reconstruct historical canonical projection',
          'later true detail revisions supersede earlier details regardless of text length; publication and context screens still apply']}


def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('REPAIR_PLAN.json')
    for n,h in plan['input_pins'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'detail_repair_input_pin_mismatch')
    bindings=read('version_bindings.json');texts,scoped=validate_caches(read('extraction_cache.json'),read('scoped_text_cache.json'),bindings)
    metadata=validate_metadata(bindings,read('representation_metadata.json'));config=read('SCOPED_ASOF_PLAN.json');universe=read('universe.json')
    require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required')
    currencies=sorted({c for p in universe for c in p.split('_')});reps=[];snapshots=[];pairs=[]
    for clock in config['cutoffs']:
        cutoff=epoch(clock);r,s=detail_snapshot(bindings,texts,scoped,metadata,cutoff,currencies,config['max_age_days'])
        reps.append({'cutoff':clock,'events':r});snapshots.append({'cutoff':clock,'states':s});pairs.extend(pair_views(s,universe,cutoff))
    prior=read('PRIOR_SCOPED_REPORT.json')
    report={'schema':'macro_detail_representation_repair.v2','versions':len(bindings),'unique_texts':len(texts),
        'detail_enriched_versions':sum(r['detail_enriched'] for r in metadata.values()),
        'detail_flags_by_context_kind':dict(sorted(Counter(b['kind'] for b in bindings if metadata[b['version_id']]['detail_enriched']).items())),
        'cutoffs':len(snapshots),'currency_rows':sum(len(s['states']) for s in snapshots),'pair_rows':len(pairs),
        'raw_detail_separated_event_cutoff_rows':sum(e['detail_is_separate_from_latest_raw'] for s in reps for e in s['events']),
        'currency_action_candidate_rows':sum(s['linguistic_action_candidate'] is not None for snap in snapshots for s in snap['states']),
        'currency_claim_candidate_rows':sum(bool(s['claim_dimension_counts_by_unique_text']) for snap in snapshots for s in snap['states']),
        'eligible_currency_context_rows':sum(s['eligible_events'] for snap in snapshots for s in snap['states']),
        'prior_raw_selector_action_rows':prior['currency_action_candidate_rows'],'prior_raw_selector_claim_rows':prior['currency_claim_candidate_rows'],
        'policy_facts_admitted':0,'forecast_features_admitted':0,'base_models_fitted':0,'forecast_improvement_proven':False,
        'historical_extraction_ready_proven':False,'live_collector_modified':False,
        'limitations':['retained enriched context is not a certified current policy fact or historical feature issuance',
            'later raw representations and correction/retraction uncertainty remain explicit; supplied retraction evidence requires review',
            'detail flag alone is not context eligibility: calendar/bootstrap/publication/staleness/missing screens remain',
            'same events/texts recur across cutoffs and pairs; coverage gains are not independent forecast evidence',
            'all previous model results and published selector sources remain unchanged; no outcome-driven tuning']}
    return {'document_representations.json':reps,'detail_currency_states.json':snapshots,'detail_pair_views.json':pairs,
            'original_guard_reuse.json':original_guard_evidence(blobs['source_ledger.py']),'detail_representation_report.json':report}
