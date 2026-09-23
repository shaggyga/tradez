"""Candidate-version loss audit at frozen receipt boundaries, without fallback."""
from collections import Counter,defaultdict
import hashlib,json
from macro_version_text_v2 import select_versions
from macro_scoped_asof_v2 import validate_caches,exclusion
from macro_text_layer_v2 import epoch
from macro_receipt_population_v2 import require


def text_relation(before,after):
    if before==after:return 'identical_text'
    if not after.strip():return 'replacement_missing_text'
    if before.startswith(after):return 'replacement_exact_prefix_shortening'
    if after.startswith(before):return 'replacement_exact_prefix_extension'
    if after in before:return 'replacement_exact_substring_shortening'
    if before in after:return 'replacement_contains_prior_text'
    return 'different_text_unresolved_revision_or_extraction'


def boundaries(bindings,end):
    return sorted({max(o['known_epoch'],o['available_epoch']) for b in bindings for o in b['observations']
        if o['clock_status']=='valid_attested_observation' and o['known_epoch'] is not None and o['available_epoch'] is not None
        and max(o['known_epoch'],o['available_epoch'])<=end})


def replay_event(bindings,texts,scoped,end,max_age_days):
    clocks=boundaries(bindings,end);rows=[]
    for clock in clocks:
        selected=select_versions(bindings,clock);require(len(selected)==1,'single_event_replay_required')
        d=selected[0];reason=exclusion(d,clock,texts,max_age_days)
        rows.append({'start_epoch':clock,'end_epoch_exclusive':None,'selected_version_ids':d['contributing_version_ids'],
            'version_status':d['version_status'],'cache_key':d.get('cache_key'),'exclusion':reason,
            'action_candidate':scoped[d['cache_key']]['action_candidate'] if reason is None else None,
            'forecast_admission':False})
    for i,row in enumerate(rows):row['end_epoch_exclusive']=rows[i+1]['start_epoch'] if i+1<len(rows) else end
    return rows


def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('SUPERSESSION_PLAN.json')
    for n,h in plan['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'supersession_predecessor_pin_mismatch')
    bindings=read('version_bindings.json');texts,scoped=validate_caches(read('extraction_cache.json'),read('scoped_text_cache.json'),bindings)
    source={r['version_id']:r for r in read('source_projection.json')};config=read('SCOPED_ASOF_PLAN.json');end=epoch(config['cutoffs'][-1])
    byevent=defaultdict(list)
    for b in bindings:byevent[b['event_id']].append(b)
    require(sorted(byevent)==plan['event_ids'],'candidate_event_subset_mismatch')
    histories=[];diffs=[];visibility=[];inventory=[]
    for event,bs in sorted(byevent.items()):
        rows=replay_event(bs,texts,scoped,end,config['max_age_days']);histories.append({'event_id':event,'boundary_states':rows})
        for b in bs:
            t=texts[b['cache_key']];f=source[b['version_id']]['fields']
            inventory.append({'event_id':event,'version_id':b['version_id'],'source_id':b['source_id'],'cache_key':b['cache_key'],
                'text_characters':len(t['retained_text']),'text_sha256':t['text_sha256'],'kind':b['kind'],'bootstrap':b['listing_bootstrap'],
                'detail_content_sha256':f.get('detail_content_sha256'),'source_url':f.get('source_url'),'policy_class':b['policy_document_class'],
                'action_candidate':scoped[b['cache_key']]['action_candidate'],'receipt_boundaries':boundaries([b],end)})
        for a,z in zip(rows,rows[1:]):
            if a['selected_version_ids']==z['selected_version_ids']:continue
            if a['cache_key'] is None or z['cache_key'] is None:
                relation='ambiguous_material_no_text_comparison';before=after=None
            else:
                before=texts[a['cache_key']]['retained_text'];after=texts[z['cache_key']]['retained_text'];relation=text_relation(before,after)
            oldhashes={source[v]['fields'].get('detail_content_sha256') for v in a['selected_version_ids']}
            newhashes={source[v]['fields'].get('detail_content_sha256') for v in z['selected_version_ids']}
            diffs.append({'event_id':event,'change_epoch':z['start_epoch'],'before_version_ids':a['selected_version_ids'],'after_version_ids':z['selected_version_ids'],
                'text_relation':relation,'before_characters':len(before) if before is not None else None,'after_characters':len(after) if after is not None else None,
                'same_nonempty_retained_detail_hash':len(oldhashes)==1 and oldhashes==newhashes and None not in oldhashes and '' not in oldhashes,
                'before_action_candidate':a['action_candidate'],'after_action_candidate':z['action_candidate'],
                'verified_publisher_revision':False,'replacement_cause':'unproven_without_original_transport_and_parser_receipts','stale_fallback_applied':False})
        for c in read('candidate_trace.json'):
            if c['event_id']!=event:continue
            intervals=[{'start_epoch':r['start_epoch'],'end_epoch_exclusive':r['end_epoch_exclusive'],'exclusion':r['exclusion']}
                       for r in rows if c['version_id'] in r['selected_version_ids']]
            visibility.append({'event_id':event,'version_id':c['version_id'],'source_id':c['source_id'],'action_candidate':c['action_candidate'],
                'selected_retained_boundary_intervals':intervals,'eligible_interval_count':sum(i['exclusion'] is None and i['end_epoch_exclusive']>i['start_epoch'] for i in intervals),
                'sampled_midnight_eligible_rows':sum(r['eligible_linguistic_context'] for r in c['cutoff_trace']),
                'interval_scope':'selector reconstruction at retained first/last observation boundaries; not complete continuous collector history',
                'historical_extraction_ready_proven':False,'forecast_admission':False})
    report={'schema':'macro_candidate_version_supersession_audit.v2','events':len(byevent),'versions':len(bindings),'texts':len(texts),
        'retained_boundary_states':sum(len(h['boundary_states']) for h in histories),'selected_version_changes':len(diffs),
        'replacement_relations':dict(sorted(Counter(d['text_relation'] for d in diffs).items())),
        'same_detail_hash_changes':sum(d['same_nonempty_retained_detail_hash'] for d in diffs),
        'action_candidates_with_intraday_eligible_intervals':sum(v['action_candidate'] is not None and v['eligible_interval_count']>0 for v in visibility),
        'action_candidates_with_midnight_eligibility':sum(v['action_candidate'] is not None and v['sampled_midnight_eligible_rows']>0 for v in visibility),
        'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'stale_fallback_applied':False,
        'limitations':['six semantic candidate events selected without prices/outcomes; not full-cohort replacement prevalence',
            'boundary replay follows existing latest-first-visible-version selector, not every repeated ingestion',
            'same retained detail hash is metadata evidence, not newly verified archived HTTP body',
            'text shortening does not alone establish publisher revision or authorize carrying old claims forward',
            'no models or outcome-conditioned repair; historical extraction readiness and forecast admission remain closed']}
    return {'event_boundary_histories.json':histories,'replacement_text_diffs.json':diffs,'candidate_visibility_intervals.json':visibility,
            'event_version_inventory.json':inventory,'supersession_report.json':report}
