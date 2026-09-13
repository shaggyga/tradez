"""Pure revision-aware news feature selection; no DB, price, model or order I/O.

The supplied transport bundle is validated by one fixed consumer implementation.
Pure code does not authenticate that its caller actually performed the DB read.
An eventual I/O integration must call that component's read_observations itself.
"""
import bisect
import weakref
import copy
import datetime as dt
import hashlib
import json
import math
import re

import projection_revision_consumer_v1 as consumer
import oanda_causal_forecast_inputs_joint_news_v3 as original
import oanda_news_causal_aggregation_guard_v2 as guard

SCHEMA='joint_revision_news_features_v1_20260913'
CONTEXT_SCHEMA='joint_revision_news_context_v2_20260913'
MAX_CONTEXT_BYTES=96*1024*1024
MAX_ORIGINS=256
MAX_CONTEXT_MEMBERS=5000
MAX_FRAME_BYTES=16*1024*1024
MAX_ORIGIN_RESULT_BYTES=16*1024*1024
NEWS_FEATURES=('context_balance','context_volume_log','context_signed_fraction','context_mean_age_hours',
               'vetted_balance','vetted_volume_log','vetted_conflict_fraction','vetted_remaining_hours')
NEUTRAL_NEWS=(0.,0.,0.,1.,0.,0.,0.,0.)
BOUND_POLICY_SOURCES={'oanda_local_news_sentiment.py': '44e66d85f82b52d6dc82e2e277bad31d2ee8c16304bec9c6a083a8b17eeb6c50', 'oanda_local_news_sentiment_repair_v2.py': '542e287e44330d2e83ef2ec1a9b363bc716cf5581cf8b05bce87de84c7fa4124', 'oanda_news_causal_aggregation_guard_v1.py': '2a7a05204dee5191cb3f53d3d5a8082b7e725842e6af65aa6dd1ae7f6292dcf9', 'oanda_news_causal_aggregation_guard_v2.py': 'c62a26721694f6e98e65e25ddb476d175a01fef0320f353fe92234e46d273aaf', 'oanda_news_classification_contract.py': 'f792528f398fab3e11be10f68edefd9b40fac33edd86d525534bc557943d0eae', 'oanda_news_classification_observation_v1.py': 'fd50ede0e2f5c769963e1e564a7283e0a1b105010498020bb69d894a82ffe293', 'oanda_news_collector_contract.py': '41c30599a953d0fd967bc68c5f178d46fc21dd2382ac9fdca78bc3c64ac9f68f', 'oanda_news_event_tagger.py': '3657872fa84a6f081903307df7209350fed4f713d866735b9387ce375a5e4f3f', 'oanda_news_source_observation_ledger_v1.py': '0e570e853645e67707cc22b8923a70c7860d939483bcbf5fd70c1dd45c2ad284', 'oanda_news_topic_identity_reconciliation_v2.py': 'cd239119826a33f406d4241bf43e09ee2446016eb7238cb775a11d7c88825d23', 'projection_revision_reader_v1.py': 'e8c2b927aa6f9524ddbf8e71bd7844ca7eae44a24f54095f76057f54803650f9'}
BOUND_GENERATION={'consumer': '3fc28130a764e06e16552e719a59d58dc7e6cc7799a6ca5719c272dfe4c6091c', 'publisher': '27e2cfafc2b0925a8f107ef890e2121a425518467d5636ab01af66205fb61563', 'original_input': 'd40cf670520e09ad18527324608e60c4ace40afa6a27ca37b2fc9d80f846a4fd', 'numeric_model': 'eb153acb966dc04ad950a0bbfcc730e78a9a0da1d8a24e91d641f473f5cfbc23', 'guard': 'c62a26721694f6e98e65e25ddb476d175a01fef0320f353fe92234e46d273aaf'}
AUTHENTICATION='supplied_transport_bytes_validated_not_database_read_authenticated_by_pure_adapter'
INERT={'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
       'account_eligible':False,'proof_eligible':False,'joint_model_consumption_proven':False}


def need(value,reason):
    if not value:raise ValueError(reason)


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode()
def sha(value):return hashlib.sha256(encoded(value)).hexdigest()


def epoch(value):
    need(type(value) in (int,float) and math.isfinite(value) and value>0,'finite_positive_origin_required')
    return float(value)


class RevisionContext:
    """Opaque factory identity; private state contains immutable tuples/bytes."""
    __slots__=('__weakref__',)
    def __new__(cls,*args,**kwargs):raise ValueError('revision_context_factory_required')
    def __setattr__(self,name,value):raise AttributeError('immutable_revision_context')
    @property
    def bundle_sha256(self):return _state(self)[1]
    @property
    def source_policy_sha256(self):return _state(self)[2]
    @property
    def consumer_id(self):return _state(self)[3]


def _context_storage():
    registry=weakref.WeakKeyDictionary()
    def make(value):
        context=object.__new__(RevisionContext);registry[context]=value;return context
    def get(context):
        need(type(context) is RevisionContext and context in registry,'immutable_revision_context_required')
        return registry[context]
    return make,get


_make_context,_state=_context_storage()


def _generation(metadata):
    profile=metadata['consumer_profile'];publication=metadata['publication_profile']
    need(metadata['schema_version']==consumer.CONTEXT_SCHEMA and
         metadata['validation_scope']=='complete_supplied_history_validated_not_database_capture_authenticated',
         'complete_bound_consumer_validation_required')
    need(metadata['source_bindings']=={'consumer':BOUND_GENERATION['consumer'],
         'publisher':BOUND_GENERATION['publisher'],'reader':BOUND_POLICY_SOURCES['projection_revision_reader_v1.py']},
         'exact_bound_consumer_publisher_reader_generation_required')
    need(profile.get('consumer_sha256')==BOUND_GENERATION['consumer'] and
         profile.get('publisher_sha256')==publication.get('publisher_sha256')==BOUND_GENERATION['publisher'],
         'exact_bound_consumer_publisher_generation_required')
    need(type(profile.get('max_scan_age_sec')) is int and profile['max_scan_age_sec']==300 and
         profile.get('scan_age_basis')=='source_read_started_epoch','unchanged_scan_freshness_policy_required')
    need(encoded(publication['policy']['source_bindings'])==encoded(BOUND_POLICY_SOURCES),
         'exact_current_collector_reader_policy_generation_required')
    need(profile.get('observation_semantics')=='this_named_consumer_independently_read_committed_exact_prefix' and
         profile.get('joint_model_consumption_proven') is False and profile.get('old_history_imported') is False,
         'explicit_pure_transport_authentication_scope_required')
    return sha({'generation':BOUND_GENERATION,'policy_sources':BOUND_POLICY_SOURCES})


def _selection(context,decision):
    prepared,_,_,consumer_id,_,_=_state(context)
    # Fixed reviewed implementation; no caller-selected validator or mutable cache.
    result=consumer.latest_consumed_from_context(prepared,decision)
    need(type(result) is dict and result.get('decision_epoch')==decision and result.get('consumer_id')==consumer_id and result.get('schema_version')==consumer.SCHEMA and
         result.get('consumer_observation_required') is True,'typed_consumer_selection_required')
    need(type(result.get('coverage_usable')) is bool,'typed_scan_coverage_required')
    if result['coverage_usable']:
        need(result.get('original_source_story_classification_clocks_unchanged') is True,
             'source_clock_preservation_contract_required')
        need(result.get('status')=='observed_current_complete' and type(result.get('members')) is list and
             type(result.get('canonical_event_count')) is int and result['canonical_event_count']==len(result['members']),
             'complete_current_consumer_selection_required')
    else:
        need(result.get('status')=='coverage_unavailable' and result.get('members') is None and
             result.get('canonical_event_count') is None,'unknown_coverage_cannot_encode_neutral')
    return result


def _selected_timing(context,decision,selected):
    """Fixed private index projection; not a receipt ID or new DB observation."""
    prepared,digest,_,consumer_id,generation,_=_state(context)
    # Source-bound consumer factory already authenticated and froze this tuple.
    _,_,indexed_consumer,epochs,rows,_,_=consumer._prepared(prepared)
    need(indexed_consumer==consumer_id,'selected_timing_consumer_identity')
    at=bisect.bisect_right(epochs,decision)-1
    observed=prefix=coverage=coverage_digest=None
    if at>=0:
        observed,prefix,coverage_body=rows[at]
        observed=epoch(observed)
        coverage=json.loads(coverage_body)
        need(observed<=decision and epochs[at]==observed,'selected_observation_after_decision')
        need(epoch(coverage['read_started_epoch'])<=epoch(coverage['read_completed_epoch'])<=observed,
             'selected_scan_observation_clock_order')
        need(type(prefix) is int and prefix>=0,'selected_publication_prefix_type')
        need(encoded(coverage)==encoded(selected['coverage']),'selected_timing_coverage_mismatch')
        if selected['coverage_usable']:
            need(prefix==selected['observed_publication_prefix'],'selected_timing_prefix_mismatch')
        coverage_digest=sha(coverage)
    else:
        need(selected['coverage'] is None and selected['coverage_usable'] is False,
             'selected_timing_before_observation_mismatch')
    return {'schema_version':'joint_revision_selected_observation_timing_v1_20260913',
            'context_sha256':digest,'source_generation_sha256':generation,'consumer_id':consumer_id,
            'decision_epoch':decision,'transport_selection_sha256':sha(selected),
            'index_row_ordinal':at if at>=0 else None,'consumer_observed_epoch':observed,
            'observed_publication_prefix':prefix,'coverage':coverage,'coverage_sha256':coverage_digest,
            'coverage_usable':selected['coverage_usable'],
            'timing_scope':'selected_original_consumer_observation_not_ack_or_new_database_read'}


def prepare_revision_context(consumer_bundle):
    """Validate the complete supplied stream once using the pinned consumer.

    That consumer freezes bytes and owns its immutable selection index. This
    pure boundary validates evidence; actual DB capture remains the I/O caller's
    responsibility. Failed current scans must be propagated by that caller.
    """
    need(type(consumer_bundle) is dict,'transport_bundle_required')
    prepared=consumer.prepare_consumed_context(consumer_bundle)
    metadata=consumer.prepared_context_metadata(prepared)
    need(type(metadata['readback_bytes']) is int and 0<=metadata['readback_bytes']<=MAX_CONTEXT_BYTES,
         'complete_context_byte_bound')
    generation=_generation(metadata)
    profile=metadata['consumer_profile'];need(type(profile['consumer_id']) is str and profile['consumer_id'],'consumer_identity_required')
    digest=metadata['readback_sha256'];need(type(digest) is str and re.fullmatch('[0-9a-f]{64}',digest),'consumer_readback_digest_required')
    for key in ('observation_count','publication_count','expanded_scan_bytes_checked','member_bytes'):
        need(type(metadata[key]) is int and metadata[key]>=0,'typed_prepared_context_counts_required')
    summary={'schema_version':CONTEXT_SCHEMA,'context_sha256':digest,
             'source_generation_sha256':generation,'authentication_scope':AUTHENTICATION,
             **{key:metadata[key] for key in ('readback_bytes','observation_count','publication_count','expanded_scan_bytes_checked','member_bytes')}}
    return _make_context((prepared,digest,sha(metadata['publication_profile']['policy']),
                          profile['consumer_id'],generation,encoded(summary)))


def revision_context_metadata(context):
    """Return fresh small metadata; never expose underlying data or indexes."""
    return json.loads(_state(context)[5])


def _grouped(members):
    groups=[]
    for entry in members:
        if entry['structured_event']:
            groups.append([entry]);continue
        group=next((group for group in groups if not any(v['structured_event'] for v in group) and
                    all(guard.same_claim(entry['member'],v['member']) for v in group)),None)
        if group is None:groups.append([entry])
        else:group.append(entry)
    return groups


def frame_as_of(context,decision_epoch):
    decision=epoch(decision_epoch);state=_state(context)
    selected=_selection(context,decision)
    timing=_selected_timing(context,decision,selected)
    basis={'schema_version':SCHEMA,'decision_epoch':decision,'context_sha256':context.bundle_sha256,
           'consumer_id':context.consumer_id,'source_policy_sha256':context.source_policy_sha256,
           'source_generation_sha256':state[4],
           'transport_selection_sha256':sha(selected),'authentication_scope':AUTHENTICATION,
           'transport_timing':timing,'transport_timing_sha256':sha(timing),
           'selection_before_relevance_score_and_headline_filters':True,
           'global_headline_deduplication_used':False,'coverage_is_feature':False,**INERT}
    if not selected['coverage_usable']:
        return {**basis,'status':'unavailable','coverage_usable':False,'frame':None,
                'reason':'actual_consumer_observation_and_current_complete_scan_coverage_required'}
    latest=selected['members'];need(len(latest)<=MAX_CONTEXT_MEMBERS,'complete_canonical_context_count_bound')
    seen=set();included=[];provenance=[];excluded=[]
    for entry in sorted(latest,key=lambda v:v['canonical_event_id']):
        ident=entry['canonical_event_id'];need(type(ident) is str and ident and ident not in seen,'duplicate_canonical_selection')
        seen.add(ident)
        payload=entry['payload'];need(type(payload) is dict and payload.get('event_id')==ident,'selected_payload_identity')
        available=epoch(entry['effective_transport_available_epoch']);observed=epoch(entry['consumer_observed_epoch'])
        need(observed<=available<=decision,'selected_consumer_availability_after_origin')
        need(type(payload.get('relevant')) is bool and type(payload.get('structured_event')) is bool,'typed_relevance_and_structure_required')
        record={'canonical_event_id':ident,'projection_id':entry['projection_id'],'projection_seq':entry['projection_seq'],
                'payload_sha256':sha(payload),'consumer_observed_epoch':observed,
                'effective_transport_available_epoch':available,'structured_event':payload['structured_event']}
        provenance.append(record)
        if not payload['relevant']:
            excluded.append({'canonical_event_id':ident,'reason':'latest_irrelevant'});continue
        scores=payload.get('currency_scores')
        need(type(scores) is dict and len(scores)<=32,'selected_score_map_invalid')
        need(all(type(c) is str and re.fullmatch('[A-Z]{3}',c) and type(v) in (int,float) and
                 math.isfinite(v) and -1<=v<=1 for c,v in scores.items()),'selected_score_value_invalid')
        if not scores:
            excluded.append({'canonical_event_id':ident,'reason':'latest_empty_score_map'});continue
        member={key:copy.deepcopy(payload.get(key)) for key in guard.MEMBER_KEYS}
        known,derived=original._member_clocks(member)
        available=max(available,derived)
        if known>decision or available>decision:
            excluded.append({'canonical_event_id':ident,'reason':'latest_source_or_derived_clock_after_origin'});continue
        if known<decision-3600:
            excluded.append({'canonical_event_id':ident,'reason':'latest_original_story_outside_context_window'});continue
        # Derived view only: exact raw payload remains sealed and unchanged.
        prior=original._epoch(member['observed_available_utc']) if member.get('observed_available_utc') else 0.
        member['observed_available_utc']=original._iso(max(prior,available))
        included.append({'canonical_event_id':ident,'member':member,'known_epoch':known,
                         'available_epoch':available,'structured_event':payload['structured_event']})
    directional=[];groups=[]
    for group in _grouped(included):
        need(len(group)<=guard.MAX_MEMBERS,'complete_claim_group_member_bound')
        ids=[v['canonical_event_id'] for v in group]
        need(len(guard._canonical([v['member'] for v in group]))<=guard.MAX_EVIDENCE_BYTES,'complete_claim_group_byte_bound')
        topic=guard.guard_topic({'topic_id':'joint_revision_claim_'+sha(ids)[:24]},
                                [v['member'] for v in group],as_of=dt.datetime.fromtimestamp(decision,dt.timezone.utc))
        groups.append({'canonical_event_ids':ids,'guard_sha256':topic['causal_aggregation_guard']['sha256'],
                       'structured_group':group[0]['structured_event'],
                       'directional_publish_eligible':topic.get('directional_publish_eligible') is True})
        if topic.get('directional_publish_eligible'):
            directional.append({'scores':topic['currency_scores'],'expires_epoch':original._epoch(topic['direction_expires_utc'])})
    frame={'members':[{'scores':v['member']['currency_scores'],'known_epoch':v['known_epoch']} for v in included],
           'directional':directional,'available_max_epoch':max((v['available_epoch'] for v in included),default=0.),
           'guard_version':guard.GUARD_VERSION}
    result={**basis,'status':'available','coverage_usable':True,'frame':frame,
            'selected_canonical_count':len(latest),'context_member_count':len(included),
            'selected_projection_provenance':provenance,'excluded_latest':excluded,'claim_groups':groups,
            'frame_sha256':sha(frame),'full_history_window_proven':False}
    need(len(encoded(result))<=MAX_FRAME_BYTES,'complete_frame_byte_bound')
    return result


def pair_features_as_of(context,instrument,decision_epoch):
    need(type(instrument) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}',instrument) and
         instrument[:3]!=instrument[4:],'explicit_distinct_currency_pair_required')
    output=frame_as_of(context,decision_epoch)
    if not output['coverage_usable']:
        return {**output,'instrument':instrument,'feature_names':list(NEWS_FEATURES),'features':None,'expires_epoch':None}
    values,expires=original._pair_features(output['frame'],instrument,output['decision_epoch'])
    need(type(values) is list and len(values)==8 and all(type(v) in (int,float) and math.isfinite(v) for v in values),'original_eight_feature_shape')
    return {**output,'instrument':instrument,'feature_names':list(NEWS_FEATURES),
            'features':values,'expires_epoch':expires,'numerical_formula':'unchanged_original_joint_v3_pair_features'}


def frames_for_origins(context,instrument,origins):
    need(type(origins) in (list,tuple) and len(origins)<=MAX_ORIGINS,'registered_origin_count_bound')
    need(all(type(t) is int and t>0 and t%900==0 for t in origins) and len(set(origins))==len(origins),
         'unique_registered_fifteen_minute_bar_starts_required')
    records=[];frames={};unavailable=[];record_bytes=2
    for origin in sorted(origins):
        result=pair_features_as_of(context,instrument,origin+60)
        record_bytes+=len(encoded(result))+(1 if records else 0)
        need(record_bytes<=MAX_ORIGIN_RESULT_BYTES,'complete_origin_result_byte_bound')
        records.append(result)
        if result['coverage_usable']:frames[str(origin)]=result['features']
        else:unavailable.append(origin)
    result={'schema_version':SCHEMA,'context_sha256':context.bundle_sha256,'instrument':instrument,
            'requested_origin_count':len(origins),'available_origin_count':len(frames),
            'unavailable_origin_count':len(unavailable),'unavailable_origins':unavailable,
            'news_frames':frames,'records':records,'neutral_defaults_for_unknown_coverage':False,
            'price_or_label_maturity_evaluated':False,'readiness_thresholds_changed':False,**INERT}

    need(len(encoded(result))<=MAX_ORIGIN_RESULT_BYTES,'complete_origin_result_byte_bound')
    return result


def features_for_origin_requests(context,requests):
    """Compute each distinct origin's frame once, then reuse the original formula.

    Requests map up to 68 distinct currency pairs to their original bar starts.
    The union is at most 256 starts. Every requested cell is retained; missing
    coverage differs from an unrequested cell and never defaults to neutral.
    No target, future label, price maturity or model readiness is evaluated.
    """
    _state(context)
    need(type(requests) is dict and 1<=len(requests)<=68,'registered_pair_count_bound')
    owned={};union=set()
    for instrument,origins in requests.items():
        need(type(instrument) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}',instrument) and
             instrument[:3]!=instrument[4:],'explicit_distinct_currency_pair_required')
        need(type(origins) in (list,tuple) and len(origins)<=MAX_ORIGINS,'registered_origin_count_bound')
        need(all(type(t) is int and t>0 and t%900==0 for t in origins) and len(set(origins))==len(origins),
             'unique_registered_fifteen_minute_bar_starts_required')
        owned[instrument]=frozenset(origins);union.update(origins)
        need(len(union)<=MAX_ORIGINS,'registered_origin_union_bound')
    instruments=sorted(owned);origins=sorted(union);records=[];available=unavailable=not_requested=0;record_bytes=2
    for origin in origins:
        result=frame_as_of(context,origin+60)
        statuses=[];values=[];expiries=[]
        for instrument in instruments:
            if origin not in owned[instrument]:
                statuses.append('not_requested');values.append(None);expiries.append(None);not_requested+=1
            elif not result['coverage_usable']:
                statuses.append('unavailable');values.append(None);expiries.append(None);unavailable+=1
            else:
                features,expires=original._pair_features(result['frame'],instrument,result['decision_epoch'])
                need(type(features) is list and len(features)==8 and all(type(v) in (int,float) and math.isfinite(v) for v in features),'original_eight_feature_shape')
                statuses.append('available');values.append(features);expiries.append(expires);available+=1
        record={'origin':origin,'decision_epoch':origin+60,'statuses':statuses,'features':values,
                'expires_epoch':expiries,'shared_frame':result}
        record_bytes+=len(encoded(record))+(1 if records else 0)
        need(record_bytes<=MAX_ORIGIN_RESULT_BYTES,'complete_origin_result_byte_bound')
        records.append(record)
    requested=sum(map(len,owned.values()))
    need(requested==available+unavailable and requested+not_requested==len(origins)*len(instruments),
         'complete_origin_pair_accounting_required')
    result={'schema_version':'joint_revision_news_origin_pair_batch_v1_20260913','context_sha256':context.bundle_sha256,
            'instruments':instruments,'origins':origins,'feature_names':list(NEWS_FEATURES),'records':records,
            'requested_cell_count':requested,'available_cell_count':available,'unavailable_cell_count':unavailable,
            'not_requested_cell_count':not_requested,'distinct_origin_count':len(origins),
            'neutral_defaults_for_unknown_coverage':False,'price_or_label_maturity_evaluated':False,
            'readiness_thresholds_changed':False,'numerical_formula':'unchanged_original_joint_v3_pair_features',**INERT}
    need(len(encoded(result))<=MAX_ORIGIN_RESULT_BYTES,'complete_origin_result_byte_bound')
    return result
