"""Typed numeric source observations; no surprise, prior-vintage or FX inference."""
from collections import Counter,defaultdict
import copy,hashlib,json
from contracts import fingerprint
from macro_numeric_audit_v2 import number,unit_schema,reference_schema,clock_value
from macro_receipt_population_v2 import require
from macro_version_text_v2 import select_versions
from macro_text_layer_v2 import epoch


def decimal_text(value):
    parsed=number(value);return str(parsed) if parsed is not None else None


def cell(component_id,fields,reference):
    names={'actual':'actual_value','reported_previous':'previous_value','reported_revised_previous':'revised_previous_value',
        'reported_unrevised_previous':'unrevised_previous_value','reported_revision':'revision_raw','unqualified_consensus':'consensus_value'}
    result={'component_id':component_id,'values':{k:decimal_text(fields.get(v)) for k,v in names.items()},
        'raw_unit':fields.get('unit'),'unit':unit_schema(fields.get('unit')),'reference':reference_schema(reference),
        'original_prior_vintage_verified':False,'numeric_surprise':None,'numeric_fact_admission':False,'forecast_admission':False}
    result['cell_sha256']=fingerprint(result);return result


def cells_for(fields):
    cells=[]
    if any(number(fields.get(n)) is not None for n in ('actual_value','previous_value','revised_previous_value','consensus_value')):
        cells.append(cell('headline',fields,fields.get('reference_period')))
    components=fields.get('release_components') or [];ids=[c.get('component_id') for c in components]
    require(all(isinstance(i,str) and i for i in ids) and len(set(ids))==len(ids),'numeric_component_identity_invalid')
    native=fields.get('source_native_components') or {}
    for c in components:
        original=native.get(c['component_id'],{})
        reference=c.get('reference_period') or original.get('reference_period')
        # Headline period must not leak into heterogeneous component dates.
        cells.append(cell(c['component_id'],c,reference))
    return cells


def prepare(bindings,projection,clock_rows):
    byversion={b['version_id']:b for b in bindings};fields={r['version_id']:r for r in projection}
    require(len(byversion)==len(bindings) and len(fields)==len(projection) and set(fields)==set(byversion),'numeric_state_population_mismatch')
    clocks={r['observation_id']:r for r in clock_rows};require(len(clocks)==len(clock_rows),'duplicate_numeric_observation_identity')
    observed=set();material={};numeric_bindings=[]
    for b in bindings:
        r=fields[b['version_id']];f=r['fields']
        require(r['content_sha256']==b['content_sha256'] and f.get('source_id')==b['source_id'],'numeric_state_content_binding_mismatch')
        for o in b['observations']:
            require(o['observation_id'] in clocks,'missing_numeric_observation_projection');c=clocks[o['observation_id']]
            require(c['version_id']==b['version_id'] and c['known_epoch']==o['known_epoch'] and c['clock_status']==o['clock_status'],'numeric_observation_projection_mismatch')
            observed.add(o['observation_id'])
        cells=cells_for(f)
        if not cells:continue
        key=fingerprint(['numeric_content.v2',b['content_sha256'],cells])
        value={'numeric_cache_key':key,'text_cache_key':b['cache_key'],'content_sha256':b['content_sha256'],'source_id':b['source_id'],
            'event_series_id':f.get('event_series_id'),'numeric_extraction_contract_id':f.get('numeric_extraction_contract_id'),
            'cells':cells,'extraction_verification':'retained_collector_fields_not_independent_publisher_binding','forecast_admission':False}
        if key in material:require(material[key]==value,'numeric_cache_identity_collision')
        material[key]=value;nb=copy.deepcopy(b);nb['text_cache_key']=nb['cache_key'];nb['cache_key']=key;numeric_bindings.append(nb)
    require(set(clocks)==observed,'orphan_numeric_observation_projection')
    return numeric_bindings,material,clocks


def selected_readiness(selected,bindings,clocks,cutoff):
    if selected['version_status']=='ambiguous_same_clock_material':return 'ambiguous_numeric_material',None,[]
    if selected['publication_clock_status']!='unambiguous_envelope' or selected['published_epoch'] is None:return 'ambiguous_or_missing_publication_clock',None,[]
    if selected['published_epoch']>cutoff:return 'future_publication_clock',None,[]
    latest=[]
    for vid in selected['contributing_version_ids']:
        valid=[o for o in bindings[vid]['observations'] if o['clock_status']=='valid_attested_observation' and o['known_epoch'] is not None
               and o['available_epoch'] is not None and o['known_epoch']<=cutoff and o['available_epoch']<=cutoff]
        require(bool(valid),'selected_numeric_version_without_receipt');last=max(o['known_epoch'] for o in valid)
        latest.extend(o for o in valid if o['known_epoch']==last)
    numbers=[clock_value(clocks[o['observation_id']]['numeric_causal_known_utc']) for o in latest]
    ids=sorted(o['observation_id'] for o in latest)
    if any(n is None for n in numbers):return 'missing_or_invalid_numeric_clock',None,ids
    if len(set(numbers))!=1:return 'ambiguous_same_clock_numeric_readiness',None,ids
    ready=max([*numbers,*(o['known_epoch'] for o in latest),*(o['available_epoch'] for o in latest)])
    return ('numeric_clock_not_ready' if ready>cutoff else 'retained_numeric_observation_ready'),ready,ids


def snapshot(all_bindings,numeric_bindings,material,clocks,cutoff,currencies,max_age_days=35):
    selected=select_versions(numeric_bindings,cutoff);index={b['version_id']:b for b in numeric_bindings}
    raw={r['event_id']:r for r in select_versions(all_bindings,cutoff)};events=[]
    for d in selected:
        status,ready,ids=selected_readiness(d,index,clocks,cutoff)
        if status=='retained_numeric_observation_ready' and cutoff-d['published_epoch']>max_age_days*86400:status='stale_numeric_context'
        usable=status=='retained_numeric_observation_ready';latest=raw[d['event_id']]
        events.append({'event_id':d['event_id'],'currencies':d['currencies'],'selected_numeric_version_ids':d['contributing_version_ids'],
            'latest_raw_version_ids':latest['contributing_version_ids'],'latest_raw_status':latest['version_status'],
            'separate_from_latest_raw':set(d['contributing_version_ids'])!=set(latest['contributing_version_ids']),
            'status':status,'source_numeric_available_epoch':ready,'selected_observation_ids':ids,
            'numeric_cache_key':d.get('cache_key') if usable else None,
            'numeric_cells':copy.deepcopy(material[d['cache_key']]['cells']) if usable else [],
            'source_id':d.get('source_id'),'publication_time_inferred':d.get('published_time_inferred'),
            'correction_retraction_status':'not_established_by_retained_schema','fallback_to_older_numeric_version':False,
            'original_extraction_ready_independently_proven':False,'forecast_admission':False})
    states=[]
    for currency in currencies:
        rows=[r for r in events if currency in r['currencies']]
        state={'currency':currency,'cutoff_epoch':cutoff,'numeric_events_visible':len(rows),
            'numeric_events_ready':sum(r['status']=='retained_numeric_observation_ready' for r in rows),
            'status_counts':dict(sorted(Counter(r['status'] for r in rows).items())),
            'event_references':[{'event_id':r['event_id'],'version_ids':r['selected_numeric_version_ids'],'numeric_cache_key':r['numeric_cache_key'],
                'status':r['status'],'source_numeric_available_epoch':r['source_numeric_available_epoch']} for r in rows],
            'numeric_surprise':None,'cross_series_numeric_aggregate':None,'original_prior_vintage_verified':False,'forecast_admission':False}
        state['state_sha256']=fingerprint(state);states.append(state)
    return events,states


def build(blobs):
    read=lambda n:json.loads(blobs[n])
    for n,h in read('NUMERIC_STATE_PLAN.json')['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'numeric_state_input_pin_mismatch')
    bindings=read('version_bindings.json');numeric,material,clocks=prepare(bindings,read('numeric_projection.json'),read('numeric_clock_projection.json'))
    universe=read('universe.json');require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required')
    config=read('SCOPED_ASOF_PLAN.json');currencies=sorted({c for p in universe for c in p.split('_')});snapshots=[];states=[];pairs=[]
    for clock in config['cutoffs']:
        cutoff=epoch(clock);events,rows=snapshot(bindings,numeric,material,clocks,cutoff,currencies,config['max_age_days'])
        snapshots.append({'cutoff':clock,'events':events});states.append({'cutoff':clock,'states':rows});index={s['currency']:s for s in rows}
        for pair in universe:
            a,b=(index[c] for c in pair.split('_'));pairs.append({'pair':pair,'cutoff_epoch':cutoff,'base_state_sha256':a['state_sha256'],
                'quote_state_sha256':b['state_sha256'],'base_ready_numeric_events':a['numeric_events_ready'],'quote_ready_numeric_events':b['numeric_events_ready'],
                'base_minus_quote_numeric_value':None,'directional_forecast':None,'forecast_admission':False,'shared_events_are_independent_votes':False})
    report={'schema':'macro_numeric_observation_asof_state.v2','versions':len(bindings),'numeric_versions':len(numeric),'numeric_cache_entries':len(material),
        'cutoffs':len(snapshots),'currency_rows':sum(len(s['states']) for s in states),'pair_rows':len(pairs),
        'event_status_counts':dict(sorted(Counter(r['status'] for s in snapshots for r in s['events']).items())),
        'ready_cell_event_cutoff_rows':sum(len(r['numeric_cells']) for s in snapshots for r in s['events']),
        'components_with_missing_or_unresolved_reference':sum(c['reference']['period'] is None for m in material.values() for c in m['cells'] if c['component_id']!='headline'),
        'numeric_surprises_computed':0,'forecast_features_admitted':0,'base_models_fitted':0,'forecast_improvement_proven':False,
        'limitations':['typed values are source-observed collector fields, not independently verified numeric facts',
            'latest selected numeric version with missing/future/conflicting numeric clocks abstains; no old-value fallback',
            'material identity includes numeric source content, so equal text cannot hide differing numeric values',
            'component references never inherit heterogeneous headline periods; reported priors remain current-snapshot claims',
            'no cross-series/currency arithmetic, unverified consensus surprise, historical issuance or price/outcome input']}
    return {'numeric_material_cache.json':[material[k] for k in sorted(material)],'numeric_source_asof.json':snapshots,
            'numeric_currency_states.json':states,'numeric_pair_views.json':pairs,'numeric_state_report.json':report}
