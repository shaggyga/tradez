"""Receipt-asof integration of guarded linguistic evidence; no FX semantics."""
from collections import Counter,defaultdict
import hashlib,json
from contracts import fingerprint
from macro_version_text_v2 import select_versions
from macro_text_layer_v2 import epoch
from macro_receipt_population_v2 import require


def validate_caches(texts,scoped,bindings):
    text_index={r['cache_key']:r for r in texts};scope_index={r['cache_key']:r for r in scoped}
    require(len(text_index)==len(texts) and len(scope_index)==len(scoped),'duplicate_scoped_cache_identity')
    require(set(text_index)==set(scope_index),'scoped_cache_population_mismatch')
    require(len({b['version_id'] for b in bindings})==len(bindings),'duplicate_version_identity')
    for k,r in scope_index.items():
        t=text_index[k];text=t['retained_text']
        require(hashlib.sha256(text.encode()).hexdigest()==t['text_sha256']==r['text_sha256'],'scoped_text_identity_mismatch')
        require(not r['forecast_admission'] and not r['policy_fact_admission'] and not r['issuer_identity_verified'],'premature_semantic_admission')
        for e in r['action_evidence']+r['claim_evidence']:
            require('rate_currency_sign' not in e,'fx_sign_not_allowed')
            if e['start'] is not None:
                require(0<=e['context_start']<=e['start']<e['end']<=e['context_end']<=len(text),'invalid_scoped_span_bounds')
                require(text[e['start']:e['end']]==e['quote'],'scoped_evidence_identity_mismatch')
    require(all(b['cache_key'] in text_index for b in bindings),'unbound_scoped_version')
    return text_index,scope_index


def exclusion(d,cutoff,texts,max_age_days):
    if d['version_status']=='ambiguous_same_clock_material':return 'ambiguous_same_clock_material'
    if d['kind']!='retained_parsed_detail':return d['kind']
    if d['listing_bootstrap']:return 'listing_bootstrap'
    if d['publication_clock_status']!='unambiguous_envelope' or d['published_epoch'] is None:return 'unresolved_publication_clock'
    if d['published_time_inferred']:return 'inferred_publication_clock'
    if d['published_epoch']>cutoff:return 'future_publication_clock'
    if cutoff-d['published_epoch']>max_age_days*86400:return 'stale_context'
    if not texts[d['cache_key']]['retained_text'].strip():return 'missing_text'
    return None


def evidence_reference(d,scoped):
    r=scoped[d['cache_key']]
    return {'event_id':d['event_id'],'version_ids':d['contributing_version_ids'],'cache_key':d['cache_key'],
        'text_sha256':r['text_sha256'],'scoped_evidence_sha256':fingerprint(r),'source_id':d['source_id'],
        'document_class':d['policy_document_class'],'published_epoch':d['published_epoch'],
        'action_candidate':r['action_candidate'],'candidate_status':r['status'],
        'asserted_claim_dimensions':sorted({c['dimension'] for c in r['claim_evidence'] if not c['guard_reasons']}),
        'policy_fact_admission':False,'issuer_verified':False,'historical_extraction_ready_epoch':None,'forecast_admission':False}


def transitions(references,ambiguous):
    if ambiguous:return []
    lanes=defaultdict(list)
    for r in references:
        if r['document_class']:lanes[r['source_id'],r['document_class']].append(r)
    result=[]
    for (source,kind),docs in sorted(lanes.items()):
        times=sorted({d['published_epoch'] for d in docs});now=[d for d in docs if d['published_epoch']==times[-1]]
        prior=[d for d in docs if len(times)>1 and d['published_epoch']==times[-2]]
        unique=len(now)==len(prior)==1 and now[0]['event_id']!=prior[0]['event_id']
        supported=unique and now[0]['action_candidate'] is not None and prior[0]['action_candidate'] is not None
        result.append({'source_id':source,'document_class':kind,'current_event_ids':sorted(d['event_id'] for d in now),
            'prior_event_ids':sorted(d['event_id'] for d in prior),
            'categorical_action_transition':[prior[0]['action_candidate'],now[0]['action_candidate']] if supported else None,
            'status':'linguistic_transition_only' if supported else 'no_unique_supported_prior',
            'stance_change':None,'policy_fact_admission':False,'forecast_admission':False})
    return result


def snapshot(bindings,texts,scoped,cutoff,currencies,max_age_days=35):
    selected=select_versions(bindings,cutoff);screened=[];states=[]
    for d in selected:
        reason=exclusion(d,cutoff,texts,max_age_days)
        screened.append({'event_id':d['event_id'],'version_ids':d['contributing_version_ids'],'currencies':d['currencies'],
            'version_status':d['version_status'],'exclusion':reason,'eligible_linguistic_context':reason is None,
            'evidence_reference':evidence_reference(d,scoped) if reason is None else None})
    for currency in currencies:
        docs=[d for d in screened if currency in d['currencies']];eligible=[d['evidence_reference'] for d in docs if d['eligible_linguistic_context']]
        reasons=Counter(d['exclusion'] for d in docs if d['exclusion']);ambiguous=bool(reasons['ambiguous_same_clock_material'])
        unique={r['cache_key']:r for r in eligible};actions={r['action_candidate'] for r in unique.values() if r['action_candidate'] is not None}
        candidate=next(iter(actions)) if len(actions)==1 and not ambiguous else None
        state={'currency':currency,'cutoff_epoch':cutoff,'visible_events':len(docs),'eligible_events':len(eligible),
            'unique_texts':len(unique),'exclusions':dict(sorted(reasons.items())),
            'ambiguous_version_ids':sorted({v for d in docs if d['exclusion']=='ambiguous_same_clock_material' for v in d['version_ids']}),
            'candidate_action_counts_by_unique_text':dict(sorted(Counter(r['action_candidate'] for r in unique.values() if r['action_candidate']).items())),
            'claim_dimension_counts_by_unique_text':dict(sorted(Counter(c for r in unique.values() for c in r['asserted_claim_dimensions']).items())),
            'linguistic_action_candidate':candidate,
            'candidate_status':'ambiguous_or_conflicting_context' if ambiguous or len(actions)>1 else 'linguistic_candidate_only' if actions else 'no_supported_action',
            'evidence_references':eligible,'categorical_transitions':transitions(eligible,ambiguous),
            'policy_stance':None,'stance_change':None,'market_expectation':None,'numeric_surprise':None,
            'policy_fact_admission':False,'historical_extraction_ready_epoch':None,'forecast_admission':False}
        state['state_sha256']=fingerprint(state);states.append(state)
    return screened,states


def pair_views(states,universe,cutoff):
    index={s['currency']:s for s in states};result=[]
    for pair in universe:
        base,quote=pair.split('_');a,b=index[base],index[quote]
        result.append({'pair':pair,'cutoff_epoch':cutoff,'base_state_sha256':a['state_sha256'],'quote_state_sha256':b['state_sha256'],
            'base_linguistic_action_candidate':a['linguistic_action_candidate'],'quote_linguistic_action_candidate':b['linguistic_action_candidate'],
            'base_unique_texts':a['unique_texts'],'quote_unique_texts':b['unique_texts'],
            'directional_difference':None,'forecast_admission':False,'can_place_orders':False,
            'shared_events_are_independent_votes':False})
    return result


def build(blobs):
    read=lambda n:json.loads(blobs[n])
    for n,h in read('SCOPED_PREDECESSOR.json')['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'scoped_predecessor_pin_mismatch')
    bindings=read('version_bindings.json');texts,scoped=validate_caches(read('extraction_cache.json'),read('scoped_text_cache.json'),bindings)
    plan=read('SCOPED_ASOF_PLAN.json');universe=read('universe.json');require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required')
    currencies=sorted({c for pair in universe for c in pair.split('_')});selected=[];snapshots=[];pairs=[];lanes=[]
    for clock in plan['cutoffs']:
        cutoff=epoch(clock);visible,states=snapshot(bindings,texts,scoped,cutoff,currencies,plan['max_age_days'])
        selected.append({'cutoff':clock,'events':visible});snapshots.append({'cutoff':clock,'states':states});pairs.extend(pair_views(states,universe,cutoff))
        lanes.extend({'cutoff':clock,'currency':s['currency'],**t} for s in states for t in s['categorical_transitions'])
    report={'schema':'macro_scoped_semantic_source_asof.v2','versions':len(bindings),'unique_texts':len(texts),'cutoffs':len(snapshots),
        'currency_rows':sum(len(s['states']) for s in snapshots),'pair_rows':len(pairs),
        'eligible_context_rows':sum(s['eligible_events'] for snap in snapshots for s in snap['states']),
        'currency_action_candidate_rows':sum(s['linguistic_action_candidate'] is not None for snap in snapshots for s in snap['states']),
        'currency_claim_candidate_rows':sum(bool(s['claim_dimension_counts_by_unique_text']) for snap in snapshots for s in snap['states']),
        'supported_linguistic_transition_rows':sum(t['categorical_action_transition'] is not None for t in lanes),
        'exclusions':dict(sorted(Counter(d['exclusion'] for snap in selected for d in snap['events'] if d['exclusion']).items())),
        'policy_facts_admitted':0,'forecast_features_admitted':0,'base_models_fitted':0,'price_outcomes_consumed':False,
        'historical_extraction_ready_proven':False,'forecast_improvement_proven':False,
        'limitations':['candidate support is retrospective linguistic evidence, not issuer policy facts or historical feature issuance',
            'currency mapping and source identity remain retained metadata; external issuer qualification is separate',
            'counts share repeated texts and events across cutoffs/currencies/pairs; not independent evidence',
            'categorical rate actions have no automatic stance, surprise, change magnitude or FX direction',
            'same-clock ambiguous versions abstain; unsupported remains null; original selector reused unchanged',
            'no new fit or retuning to negative event-layer forecasts']}
    return {'selected_scoped_versions.json':selected,'scoped_currency_states.json':snapshots,'scoped_pair_views.json':pairs,
            'linguistic_transitions.json':lanes,'scoped_asof_report.json':report}
