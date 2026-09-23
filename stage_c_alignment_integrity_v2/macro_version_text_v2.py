"""One immutable lexical cache, source-receipt as-of states and shared pair views.

Source-time reconstruction is retrospective reprocessing, not proof that these
new features existed historically. No market outcomes are consumed here.
"""
from collections import Counter,defaultdict
import copy
import hashlib
import json
import re
from contracts import fingerprint
from macro_text_layer_v2 import extract,epoch,AXES
from macro_receipt_population_v2 import load_compressed,require


def cache_documents(documents,versions,observations,audit,rules,extractor_identity):
    content={v['version_id']:v for v in versions};audits={a['observation_id']:a for a in audit}
    require(len(content)==len(versions) and len(audits)==len(audit),'duplicate_source_identity')
    byversion=defaultdict(list)
    for o in observations:
        require(o['observation_id'] in audits,'missing_prior_clock_audit')
        a=audits[o['observation_id']]
        require(a['version_id']==o['version_id'] and a['clock_status']==o['clock_status'],'source_clock_audit_mismatch')
        env=json.loads(o['envelope_json'])
        published=epoch(env['published_utc']) if a['clock_status']=='valid_attested_observation' else None
        byversion[o['version_id']].append({'observation_id':o['observation_id'],'sequence':o['observation_seq'],
            'known_epoch':o['known_epoch'],'available_epoch':a['available_epoch'],'published_epoch':published,
            'clock_status':a['clock_status']})
    require(set(byversion)==set(content),'version_observation_population_mismatch')
    cache={};bindings=[]
    for d in documents:
        v=content[d['version_id']];c=json.loads(v['content_json'])
        require(hashlib.sha256(v['content_json'].encode()).hexdigest()==d['content_sha256']==v['content_sha256'],'text_source_content_mismatch')
        require(c.get('summary','')==d['summary'] and c.get('headline','')==d['headline'],'retained_text_projection_mismatch')
        text=d['summary']
        require(isinstance(text,str),'retained_summary_must_be_text')
        text_sha=hashlib.sha256(text.encode()).hexdigest();key=fingerprint([text_sha,extractor_identity])
        if key not in cache:
            extracted=extract(text,rules)
            words=Counter(re.findall(r'\b[A-Za-z]{2,}\b',text.lower()))
            cache[key]={'cache_key':key,'text_sha256':text_sha,'retained_text':text,'extractor_identity':extractor_identity,
                'extraction':extracted,'token_counts':dict(sorted(words.items())),'token_total':sum(words.values()),
                'language':'not_verified_English_lexical_rules_only','actual_historical_extraction_ready_epoch':None}
        bindings.append({'version_id':d['version_id'],'event_id':d['canonical_event_id'],'content_sha256':d['content_sha256'],
            'cache_key':key,'currencies':d['currencies'],'source_id':d['source_id'],'kind':d['kind'],
            'listing_bootstrap':d['listing_bootstrap'],'published_time_inferred':d['published_time_inferred'],
            'policy_document_class':c.get('policy_document_type',''),'headline':d['headline'],
            'observations':sorted(byversion[d['version_id']],key=lambda x:x['sequence'])})
    require(len(bindings)==len(content) and len({b['version_id'] for b in bindings})==len(content),'text_binding_population_mismatch')
    return cache,sorted(bindings,key=lambda x:x['version_id'])


def select_versions(bindings,cutoff):
    """Gate receipt envelopes before resolving versions; no whole-cohort flags."""
    byevent=defaultdict(list)
    for b in bindings:
        valid=[o for o in b['observations'] if o['clock_status']=='valid_attested_observation'
               and o['available_epoch'] is not None and o['known_epoch'] is not None
               and o['known_epoch']<=cutoff and o['available_epoch']<=cutoff]
        if not valid:continue
        latest_clock=max(o['known_epoch'] for o in valid)
        latest=[o for o in valid if o['known_epoch']==latest_clock]
        publications={o['published_epoch'] for o in latest}
        current=min(latest,key=lambda o:o['sequence'])
        item={k:copy.deepcopy(v) for k,v in b.items() if k!='observations'}
        item.update(first_available_epoch=min(o['available_epoch'] for o in valid),
                    published_epoch=current['published_epoch'] if len(publications)==1 else None,
                    publication_clock_status='unambiguous_envelope' if len(publications)==1 else 'ambiguous_same_clock_envelopes',
                    source_observation_ids=sorted(o['observation_id'] for o in latest))
        byevent[b['event_id']].append(item)
    selected=[]
    for event,versions in sorted(byevent.items()):
        latest_time=max(v['first_available_epoch'] for v in versions)
        latest=[v for v in versions if v['first_available_epoch']==latest_time]
        material={fingerprint([v[k] for k in ('cache_key','source_id','policy_document_class','kind','listing_bootstrap','published_time_inferred','currencies','published_epoch')]) for v in latest}
        if len(material)==1:
            item=copy.deepcopy(min(latest,key=lambda v:v['version_id']))
            item.update(version_status='identical_material_mirrors' if len(latest)>1 else 'unambiguous_version',
                        contributing_version_ids=sorted(v['version_id'] for v in latest))
        else:
            item={'event_id':event,'version_id':None,'version_status':'ambiguous_same_clock_material',
                  'contributing_version_ids':sorted(v['version_id'] for v in latest),
                  'currencies':sorted({c for v in latest for c in v['currencies']}),'first_available_epoch':latest_time}
        selected.append(item)
    return selected


def changes(eligible,cache):
    lanes=defaultdict(list)
    for d in eligible:
        if d['policy_document_class'] and d['published_epoch'] is not None:
            lanes[d['source_id'],d['policy_document_class']].append(d)
    result=[]
    for (source,kind),docs in sorted(lanes.items()):
        times=sorted({d['published_epoch'] for d in docs});latest=[d for d in docs if d['published_epoch']==times[-1]]
        previous=[d for d in docs if len(times)>1 and d['published_epoch']==times[-2]]
        unique=len(latest)==1 and len(previous)==1 and latest[0]['event_id']!=previous[0]['event_id']
        axes={}
        for axis in AXES:
            now=cache[latest[0]['cache_key']]['extraction']['axes'][axis]['value'] if unique else None
            before=cache[previous[0]['cache_key']]['extraction']['axes'][axis]['value'] if unique else None
            axes[axis]={'value':now-before if now is not None and before is not None else None,
                        'status':'comparable_lexical_change' if now is not None and before is not None else 'no_unique_supported_comparable_prior'}
        result.append({'source_id':source,'document_class':kind,'current_event_ids':sorted(d['event_id'] for d in latest),
                       'prior_event_ids':sorted(d['event_id'] for d in previous),'axes':axes})
    return result


def currency_states(bindings,cache,cutoff,currencies,max_age_days=35):
    selected=select_versions(bindings,cutoff);states=[]
    for currency in currencies:
        docs=[d for d in selected if currency in d['currencies']];eligible=[];exclusions=Counter();ambiguous=[]
        for d in docs:
            if d['version_status']=='ambiguous_same_clock_material':
                exclusions['ambiguous_same_clock_material']+=1;ambiguous.extend(d['contributing_version_ids']);continue
            if d['kind']!='retained_parsed_detail':exclusions[d['kind']]+=1;continue
            if d['listing_bootstrap']:exclusions['listing_bootstrap']+=1;continue
            if cutoff-(d['published_epoch'] if d['published_epoch'] is not None else d['first_available_epoch'])>max_age_days*86400:
                exclusions['stale_context']+=1;continue
            if not cache[d['cache_key']]['retained_text'].strip():exclusions['missing_text']+=1;continue
            eligible.append(d)
        repeats=Counter(d['cache_key'] for d in eligible);words=Counter();weight_total=0.;axes={}
        for d in eligible:
            c=cache[d['cache_key']];weight=1/repeats[d['cache_key']];weight_total+=weight
            for word,count in c['token_counts'].items():words[word]+=weight*count/max(c['token_total'],1)
        for axis in AXES:
            values=[];statuses=Counter()
            for d in eligible:
                a=cache[d['cache_key']]['extraction']['axes'][axis];statuses[a['status']]+=1
                if a['value'] is not None:values.append((a['value'],1/repeats[d['cache_key']]))
            signs={1 if x>0 else -1 if x<0 else 0 for x,w in values}
            conflict=len(signs)>1 or statuses['conflicting_phrases']>0 or bool(ambiguous)
            value=None if not values or conflict else sum(x*w for x,w in values)/sum(w for x,w in values)
            axes[axis]={'value':value,'status':'ambiguous_or_conflicting_context' if conflict else 'weighted_lexical_proxy' if values else 'no_supported_phrase',
                        'supporting_documents':len(values),'document_status_counts':dict(sorted(statuses.items())),
                        'observed_neutral':value==0 if value is not None else False}
        states.append({'currency':currency,'cutoff_epoch':cutoff,'status':'retained_detail_context' if eligible else 'no_eligible_detail_context',
            'visible_event_count':len(docs),'eligible_event_count':len(eligible),'unique_text_count':len(repeats),
            'source_count':len({d['source_id'] for d in eligible}),'exclusions':dict(sorted(exclusions.items())),
            'contributing_version_ids':sorted({v for d in eligible for v in d['contributing_version_ids']}),
            'ambiguous_version_ids':sorted(ambiguous),'minimum_source_age_seconds':min((cutoff-d['first_available_epoch'] for d in eligible),default=None),
            'words':[{'token':w,'normalized_count':v/weight_total} for w,v in sorted(words.items(),key=lambda x:(-x[1],x[0]))[:30]] if weight_total else [],
            'meter_axes':axes,'comparable_changes':[] if ambiguous else changes(eligible,cache),
            'comparable_changes_status':'blocked_by_ambiguous_visible_material' if ambiguous else 'unique_publication_lanes_only',
            'numeric_surprise':None,'expectation':None,'observed_reaction':None,'forecast_admission':False,
            'scope':'retrospective_reprocessing_at_source_asof_not_historical_extraction_readiness'})
    return states


def pair_features(states,universe,cutoff):
    index={s['currency']:s for s in states};result=[]
    for pair in universe:
        base,quote=pair.split('_');a,b=index[base],index[quote];features={}
        for axis in AXES:
            av,bv=a['meter_axes'][axis]['value'],b['meter_axes'][axis]['value']
            features[axis]={'base':av,'quote':bv,'base_minus_quote':av-bv if av is not None and bv is not None else None,
                            'both_supported':av is not None and bv is not None}
        result.append({'pair':pair,'cutoff_epoch':cutoff,'base_currency':base,'quote_currency':quote,
            'base_visible_events':a['visible_event_count'],'quote_visible_events':b['visible_event_count'],
            'base_eligible_events':a['eligible_event_count'],'quote_eligible_events':b['eligible_event_count'],
            'features':features,'directional_forecast':None,'can_place_orders':False,'forecast_admission':False,
            'source_asof_only':True,'price_outcomes_consumed':False,'shared_events_are_independent_votes':False})
    return result


def build(blobs,rules,extractor_identity):
    read=lambda n:json.loads(blobs[n]);capture=read('CAPTURE_RECEIPT.json');plan=read('VERSION_TEXT_PLAN.json')
    for n,h in read('TEXT_PREDECESSOR.json')['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'text_predecessor_identity_mismatch')
    versions,_=load_compressed(blobs['versions.json.gz'],capture['inputs']['versions.json'])
    observations,_=load_compressed(blobs['observations.json.gz'],capture['inputs']['observations.json'])
    documents=read('documents.json');audit=read('clock_audit.json');universe=read('universe.json')
    require(len(documents)==capture['versions'] and len(observations)==len(audit)==capture['observations'],'original_receipt_population_mismatch')
    require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required')
    cache,bindings=cache_documents(documents,versions,observations,audit,rules,extractor_identity)
    currencies=sorted({c for pair in universe for c in pair.split('_')});snapshots=[];pairs=[]
    for clock in plan['cutoffs']:
        cutoff=epoch(clock);states=currency_states(bindings,cache,cutoff,currencies,plan['max_age_days'])
        snapshots.append({'cutoff':clock,'states':states});pairs.extend(pair_features(states,universe,cutoff))
    report={'schema':'macro_causal_version_text_state.v2','versions':len(bindings),'unique_extraction_cache_entries':len(cache),
        'extraction_calls_avoided_for_identical_text':len(bindings)-len(cache),'universe_count':68,'currency_count':len(currencies),'cutoffs':len(snapshots),'pair_rows':len(pairs),
        'evidence_span_states':dict(sorted(Counter(e['state'] for c in cache.values() for e in c['extraction']['evidence']).items())),
        'non_null_currency_axes':sum(a['value'] is not None for s in snapshots for c in s['states'] for a in c['meter_axes'].values()),
        'comparable_change_values':sum(a['value'] is not None for s in snapshots for c in s['states'] for lane in c['comparable_changes'] for a in lane['axes'].values()),
        'supported_pair_axis_differences':sum(a['both_supported'] for p in pairs for a in p['features'].values()),
        'base_models_fitted':0,'price_inputs_consumed':False,'gpt_calls':0,'historical_extraction_ready_proven':False,'forecast_improvement_proven':False,
        'limitations':['new lexical extraction is retrospective reprocessing, not historical feature issuance',
            'English phrase/token proxies are not validated multilingual NLP, policy facts or market expectations',
            'source flags and canonical events are retained metadata, not a complete external release census',
            'full-cohort resolved event flags and price outcomes are excluded from this feature pipeline',
            'same-clock ambiguous material and unsupported axes abstain; missing is not neutral',
            'calendar/headline/bootstrap/stale records remain in coverage but outside the detail meter',
            'exact repeated text shares total weight1; no learned or outcome-tuned boilerplate weights',
            'no numeric surprise, independent confirmation, forecast gain or broker authorization']}
    return {'extraction_cache.json':[cache[k] for k in sorted(cache)],'version_bindings.json':bindings,
            'currency_states.json':snapshots,'pair_features.json':pairs,'semantic_report.json':report}
