"""Retained event identities and exact-endpoint research support, never forecasts."""
from collections import Counter,defaultdict
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import numpy as np
import pandas as pd
from causal_technical_adapter_v2 import clean
from retained_endpoint_targets_v1 import endpoint_outcomes


def require(ok,reason):
    if not ok:raise ValueError(reason)


def epoch(value):
    try:
        x=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
        return x.timestamp() if x.tzinfo is not None else None
    except (ValueError,TypeError,AttributeError,OverflowError):return None


def resolve_events(documents):
    groups=defaultdict(list);versions=set()
    for d in documents:
        require(d['version_id'] not in versions,'duplicate_document_version')
        versions.add(d['version_id']);groups[d['canonical_event_id']].append(d)
    result=[]
    for event,docs in sorted(groups.items()):
        clocks=sorted({p for d in docs for p in d['publication_clocks']})
        parsed={epoch(p) for p in clocks}
        published=next(iter(parsed)) if len(parsed)==1 and None not in parsed else None
        available=[d['available_epoch'] for d in docs if d['available_epoch'] is not None]
        kinds=sorted({d['kind'] for d in docs});bootstrap=any(d['listing_bootstrap'] for d in docs)
        inferred=any(d['published_time_inferred'] is not False for d in docs)
        reasons=[]
        if 'calendar_or_schedule_listing' in kinds:reasons.append('calendar_or_schedule_listing')
        if bootstrap:reasons.append('retained_listing_bootstrap')
        if inferred:reasons.append('inferred_or_unknown_publication_clock')
        if published is None:reasons.append('conflicting_or_unresolved_publication_clock')
        if not available:reasons.append('no_collector_attested_available_version')
        if kinds!=['retained_parsed_detail']:reasons.append('not_exclusively_parsed_detail')
        # Even candidates still lack verified original extraction latency,
        # complete external event resolution and historical quote receipts.
        result.append({'event_id':event,'version_ids':sorted(d['version_id'] for d in docs),
            'source_ids':sorted({d['source_id'] for d in docs}),'source_urls':sorted({d['source_url'] for d in docs if d['source_url']}),
            'currencies':sorted({c for d in docs for c in d['currencies']}),'kinds':kinds,
            'publication_epoch':published,'publication_clocks':clocks,
            'available_epoch':min(available) if available else None,
            'release_candidate_within_retained_metadata':not reasons,'release_limitations':reasons,
            'timing_class':'retained_source_available_research_only' if available else 'unresolved',
            'original_extraction_ready_proven':False,'independent_announcement_identity_proven':False,
            'forecast_admission':False,'expectations':None,'resolution_uses_full_cohort_retrospectively':True})
    return result


def decode_candles(raw,descriptor,pair,plan):
    require(hashlib.sha256(raw).hexdigest()==descriptor['compressed_sha256'],'compressed_candle_hash_mismatch')
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:expanded=f.read(8*1024*1024+1)
    require(len(expanded)<=8*1024*1024,'expanded_candle_member_limit')
    require(len(expanded)==descriptor['expanded_bytes'] and hashlib.sha256(expanded).hexdigest()==descriptor['expanded_sha256'],'expanded_candle_hash_mismatch')
    rows=[];start=epoch(plan['raw_interval_start']);end=epoch(plan['raw_interval_end_exclusive'])
    require(start is not None and end is not None and start<end,'invalid_candle_capture_interval')
    reader=csv.DictReader(io.StringIO(expanded.decode('utf-8-sig')))
    for r in reader:
        require(r['instrument']==pair and r['granularity']=='M1','candle_pair_or_granularity_mismatch')
        t=epoch(r['time']);other=epoch(r['datetime'])
        require(t is not None and t==other and t%60==0 and start<=t<end,'candle_clock_mismatch')
        def number(k):
            try:return float(r[k])
            except (ValueError,TypeError):return float('nan')
        rows.append({'epoch':int(t),'mid':number('close'),'bid':number('bid_close'),'ask':number('ask_close')})
    require(len(rows)==descriptor['rows'],'candle_row_count_mismatch')
    return pd.DataFrame(rows,columns=['epoch','mid','bid','ask']).astype({'epoch':'int64','mid':'float64','bid':'float64','ask':'float64'})


def anchor_epoch(event,anchor):
    if anchor=='single_unambiguous_publication_clock':return event['publication_epoch']
    if anchor=='earliest_valid_captured_available_clock':return event['available_epoch']
    raise ValueError('unknown_anchor')


def pair_rows(pair,raw,events,plan,source_sha):
    frame,quality=clean(raw)
    data={'time':frame.epoch.to_numpy(),'close':frame.mid.to_numpy(),'bid_close':frame.bid.to_numpy(),'ask_close':frame.ask.to_numpy()}
    horizons=plan['horizon_minutes'];labels={h:endpoint_outcomes(data,h) for h in horizons} if len(frame) else {}
    positions={int(t):i for i,t in enumerate(frame.epoch)};raw_times=set(int(x) for x in raw.epoch)
    currencies=set(pair.split('_'));mapped=[e for e in events if currencies & set(e['currencies'])]
    rows=[]
    for e in mapped:
        for anchor in plan['anchors']:
            value=anchor_epoch(e,anchor)
            decision=math.ceil(value/60)*60 if value is not None else None
            reference=decision-60 if decision is not None else None
            i=positions.get(reference)
            for h in horizons:
                target=reference+h*60 if reference is not None else None
                movement=None;returned=None;quote_valid=False;long_net=short_net=None
                if reference is None:state='unresolved_anchor'
                elif i is None:state='invalid_or_duplicate_reference' if reference in raw_times else 'no_exact_reference'
                elif target not in positions:state='invalid_or_duplicate_target' if target in raw_times else 'no_exact_target'
                else:
                    returned=float(labels[h]['midpoint_return_bps'][i])
                    require(math.isfinite(returned),'nonfinite_original_endpoint')
                    state='observed_midpoint_endpoints'
                    movement='no_move' if abs(returned)<=10 else 'positive_move' if returned>0 else 'negative_move'
                    quote_valid=bool(labels[h]['endpoint_quote_valid'][i])
                    if quote_valid:
                        long_net=float(labels[h]['long_endpoint_net_bps'][i]);short_net=float(labels[h]['short_endpoint_net_bps'][i])
                rows.append({'event_id':e['event_id'],'pair':pair,'anchor':anchor,'anchor_epoch':value,
                    'decision_epoch':decision,'reference_bar_start_epoch':reference,'target_bar_start_epoch':target,
                    'assumed_outcome_available_epoch':target+60 if target is not None else None,'horizon_minutes':h,
                    'state':state,'midpoint_return_bps':returned,'movement_label':movement,
                    'bidask_endpoint_valid':quote_valid,'long_endpoint_proxy_bps':long_net,'short_endpoint_proxy_bps':short_net,
                    'release_candidate_within_retained_metadata':e['release_candidate_within_retained_metadata'],
                    'source_slice_sha256':source_sha,'historical_quote_receipt_proven':False,
                    'forecast_admission':False,'independent_observation':False})
    return {'pair':pair,'quality':quality,'mapped_events':len(mapped),'unmapped_events':len(events)-len(mapped),
            'rows':rows,'valid_bars':len(frame),'captured_rows':len(raw),
            'first_valid_bar_epoch':int(frame.epoch.iloc[0]) if len(frame) else None,
            'last_valid_bar_epoch':int(frame.epoch.iloc[-1]) if len(frame) else None}


def reveal(row,asof):
    require(type(asof) in (int,float) and math.isfinite(asof),'finite_asof_required')
    available=row['assumed_outcome_available_epoch']
    if available is None or asof<available:
        return {'event_id':row['event_id'],'pair':row['pair'],'status':'unresolved_anchor' if available is None else 'pending_assumed_maturity',
                'midpoint_return_bps':None,'movement_label':None,'long_endpoint_proxy_bps':None,'short_endpoint_proxy_bps':None}
    return {'event_id':row['event_id'],'pair':row['pair'],'status':row['state'],
            **{k:row[k] for k in ('midpoint_return_bps','movement_label','long_endpoint_proxy_bps','short_endpoint_proxy_bps')}}


def summarize(events,parts,plan):
    groups=[]
    for anchor in plan['anchors']:
        for h in plan['horizon_minutes']:
            rows=[r for p in parts for r in p['rows'] if r['anchor']==anchor and r['horizon_minutes']==h]
            observed=[r for r in rows if r['midpoint_return_bps'] is not None]
            candidates=[r for r in rows if r['release_candidate_within_retained_metadata']]
            groups.append({'anchor':anchor,'horizon_minutes':h,'rows':len(rows),
                'support_states':dict(sorted(Counter(r['state'] for r in rows).items())),
                'movement_labels':dict(sorted(Counter(r['movement_label'] for r in observed).items())),
                'events_with_supported_pair':len({r['event_id'] for r in observed}),
                'no_move_events_any_mapped_pair':len({r['event_id'] for r in observed if r['movement_label']=='no_move'}),
                'bidask_endpoint_rows':sum(r['bidask_endpoint_valid'] for r in rows),
                'release_candidate_rows':len(candidates),'release_candidate_supported_rows':sum(r['midpoint_return_bps'] is not None for r in candidates)})
    return {'schema':'macro_matched_population.v2','events':len(events),'versions':sum(len(e['version_ids']) for e in events),
        'universe_count':len(parts),'pair_rows':sum(len(p['rows']) for p in parts),'groups':groups,
        'release_candidates_within_retained_metadata':sum(e['release_candidate_within_retained_metadata'] for e in events),
        'events_without_any_currency_mapping':sum(not e['currencies'] for e in events),
        'release_limitation_counts':dict(sorted(Counter(x for e in events for x in e['release_limitations']).items())),
        'earliest_captured_bar':min((p['first_valid_bar_epoch'] for p in parts if p['first_valid_bar_epoch'] is not None),default=None),
        'latest_captured_bar':max((p['last_valid_bar_epoch'] for p in parts if p['last_valid_bar_epoch'] is not None),default=None),
        'source_rows_captured':sum(p['captured_rows'] for p in parts), 'observed_no_move_comparison':'fixed_descriptive_10bps_midpoint_band_includes_all_supported_rows',
        'complete_external_all_release_calendar':False,'historical_forecast_admission':False,'base_models_fitted':0,
        'independent_review':False,'forecast_improvement_proven':False,
        'limitations':['source selection frozen before cohort prices; all mapped events retained regardless of outcome',
            'full-cohort event resolution is retrospective and cannot enter earlier feature states',
            'canonical news IDs are not independently resolved announcements; mirrors/translations may remain linked imperfectly',
            'calendar/bootstrap/headline/inferred-clock records are reported separately and not admitted as releases',
            'event-pair rows and overlapping horizons are dependent, not independent release counts',
            'ceil-to-minute and target-bar-end maturity are assumptions, not historical receipts or extraction readiness',
            'missing/invalid reference or target is not zero return or no move',
            'endpoint midpoints and bidask proxies do not establish continuous paths, fills, slippage or profits',
            'no optimized threshold, trained model, economic causality, protected confirmation or forecast gain']}


def build_inputs(blobs):
    read=lambda n:json.loads(blobs[n])
    plan=read('MATCHED_POPULATION_PLAN.json');capture=read('CANDLE_CAPTURE.json');universe=read('universe.json')
    require(plan['horizon_minutes']==[60,1440] and plan['anchors']==['single_unambiguous_publication_clock','earliest_valid_captured_available_clock'],'undeclared_horizon_or_anchor')
    require(capture['plan_sha256']==hashlib.sha256(blobs['MATCHED_POPULATION_PLAN.json']).hexdigest(),'candle_capture_plan_mismatch')
    require(capture['status']=='complete' and not capture['source_writes'] and capture['broker_or_api_calls']==0,'invalid_candle_capture_scope')
    require(len(universe)==68 and sorted(set(universe))==universe,'fixed_all68_universe_required')
    predecessor=read('INPUT_PREDECESSOR.json')
    for n,digest in predecessor['payloads'].items():
        require(hashlib.sha256(blobs[n]).hexdigest()==digest,'receipt_predecessor_payload_mismatch')
    documents=read('documents.json');report=read('population_report.json');audit=read('clock_audit.json')
    require(len(documents)==report['versions'] and len(audit)==report['boundary_receipts'],'receipt_predecessor_population_mismatch')
    audit_map={a['observation_id']:a for a in audit}
    for d in documents:
        observations=[audit_map[x] for x in d['observation_ids']]
        require(all(x['version_id']==d['version_id'] for x in observations),'document_receipt_lineage_mismatch')
        clocks=[x['available_epoch'] for x in observations if x['available_epoch'] is not None]
        require(d['available_epoch']==(min(clocks) if clocks else None),'document_available_clock_mismatch')
    events=resolve_events(documents)
    require(len(events)==report['canonical_events'],'canonical_event_population_mismatch')
    descriptors={x['pair']:x for x in capture['pairs']}
    require(len(descriptors)==len(capture['pairs']) and sorted(descriptors)==universe,'captured_all68_population_mismatch')
    require(sum(x.get('expanded_bytes',0) for x in descriptors.values())<=128*1024*1024,'expanded_candle_total_limit')
    parts=[]
    for pair in universe:
        descriptor=descriptors[pair]
        if descriptor['status']=='source_missing':
            raw=pd.DataFrame({'epoch':pd.Series(dtype='int64'),'mid':pd.Series(dtype='float64'),'bid':pd.Series(dtype='float64'),'ask':pd.Series(dtype='float64')})
            digest=None
        else:
            require(descriptor['status']=='captured' and descriptor['input']==pair+'.csv.gz','invalid_captured_member')
            raw=decode_candles(blobs[descriptor['input']],descriptor,pair,plan);digest=descriptor['expanded_sha256']
        parts.append(pair_rows(pair,raw,events,plan,digest))
    outputs={p['pair']+'.json':p for p in parts}
    outputs['events.json']=events
    outputs['matched_report.json']=summarize(events,parts,plan)
    outputs['pair_coverage.json']=[{k:v for k,v in p.items() if k!='rows'} for p in parts]
    return outputs
