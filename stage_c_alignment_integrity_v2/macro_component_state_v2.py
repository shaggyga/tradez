"""Hash-bound repaired component reconstruction over existing source-asof gates."""
import copy,datetime as dt,hashlib,json
from collections import Counter
from contracts import fingerprint

def require(value,message):
    if not value:raise ValueError(message)

def validate_parent(blobs,manifest_name,payloads):
    m=json.loads(blobs[manifest_name]);rows={p['path']:p for p in m['payloads']}
    require(len(rows)==len(m['payloads']),'duplicate_parent_payload')
    for name in payloads:
        require(name in rows and rows[name]['sha256']==hashlib.sha256(blobs[name]).hexdigest() and rows[name]['bytes']==len(blobs[name]),'component_parent_payload_binding_mismatch')
    identity=m['run_identity'];body={k:v for k,v in identity.items() if k!='fingerprint'}
    require(fingerprint(body)==identity['fingerprint'],'component_parent_identity_mismatch')
    return identity['fingerprint']

def evidence_index(rows):
    index={}
    for r in rows:
        require(r['version_id'] not in index,'duplicate_repaired_version')
        for c in r['adapter']['cells']:
            require(fingerprint({k:v for k,v in c.items() if k!='evidence_sha256'})==c['evidence_sha256'],'component_cell_hash_mismatch')
            require(c['numeric_fact_admission'] is False and c['forecast_admission'] is False and c['original_extraction_readiness_proven'] is False,'component_qualification_scope_violation')
        index[r['version_id']]=r
    return index

def join_event(event,index,material,cutoff):
    result={'event_id':event['event_id'],'currencies':event['currencies'],'original_numeric_event':copy.deepcopy(event),
        'reconstructed_component_cells':[],'component_evidence_version_ids':[],'status':'no_repaired_component_version',
        'adapter_activation_status':'no_activation_receipt','adapter_available_epoch':None,'runtime_feature_cells':[],
        'historical_adapter_activation_backdated':False,'forecast_admission':False}
    ids=event['selected_numeric_version_ids'];matched=[v for v in ids if v in index]
    if not matched:return result
    if event['status']!='retained_numeric_observation_ready':result['status']='blocked_by_original_numeric_readiness';return result
    ready=event['source_numeric_available_epoch']
    require(isinstance(ready,(int,float)) and ready<=cutoff,'component_source_readiness_after_cutoff')
    if len(matched)!=len(ids):result['status']='incomplete_selected_version_evidence';return result
    key=event['numeric_cache_key'];require(key in material,'component_numeric_material_missing')
    for vid in matched:
        require(index[vid]['content_sha256']==material[key]['content_sha256'] and index[vid]['source_id']==material[key]['source_id'],'component_selected_content_mismatch')
    signatures={fingerprint(index[v]['adapter']['cells']) for v in matched}
    if len(signatures)!=1:result['status']='conflicting_selected_component_evidence';return result
    adapter=index[matched[0]]['adapter']
    if not adapter['cells']:
        result['status']='component_adapter_abstained';result['abstention_reason']=adapter['status'];return result
    result.update(status='retrospective_source_asof_component_evidence',reconstructed_component_cells=copy.deepcopy(adapter['cells']),component_evidence_version_ids=sorted(matched))
    return result

def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('COMPONENT_STATE_PLAN.json')
    require(set(plan['payloads'])==set(blobs)-{'COMPONENT_STATE_PLAN.json'},'component_state_input_inventory_mismatch')
    require(all(hashlib.sha256(blobs[n]).hexdigest()==h for n,h in plan['payloads'].items()),'component_state_input_pin_mismatch')
    require(plan['adapter_activation_receipt'] is None,'activation_receipt_schema_not_implemented_no_backdating')
    parent_ids=[validate_parent(blobs,'numeric_parent_manifest.json',['numeric_source_asof.json','numeric_material_cache.json']),validate_parent(blobs,'repair_parent_manifest.json',['component_repair_evidence.json'])]
    index=evidence_index(read('component_repair_evidence.json'));material={r['numeric_cache_key']:r for r in read('numeric_material_cache.json')}
    universe=read('universe.json');require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required')
    currencies=sorted({c for pair in universe for c in pair.split('_')});snapshots=[];states=[];pairs=[]
    for snap in read('numeric_source_asof.json'):
        clock=dt.datetime.fromisoformat(snap['cutoff'].replace('Z','+00:00'));require(clock.tzinfo is not None,'aware_component_cutoff_required');cutoff=clock.timestamp()
        events=[join_event(e,index,material,cutoff) for e in snap['events']];snapshots.append({'cutoff':snap['cutoff'],'events':events})
        rows=[]
        for currency in currencies:
            selected=[e for e in events if currency in e['currencies']];row={'currency':currency,'cutoff_epoch':cutoff,
                'component_event_references':[{'event_id':e['event_id'],'version_ids':e['component_evidence_version_ids'],'status':e['status'],
                    'component_evidence_sha256':[c['evidence_sha256'] for c in e['reconstructed_component_cells']]} for e in selected],
                'reconstructed_component_cells':sum(len(e['reconstructed_component_cells']) for e in selected),'runtime_feature_cells':0,
                'numeric_surprise':None,'cross_series_numeric_aggregate':None,'forecast_admission':False}
            row['state_sha256']=fingerprint(row);rows.append(row)
        states.append({'cutoff':snap['cutoff'],'states':rows});bycurrency={r['currency']:r for r in rows}
        for pair in universe:
            a,b=(bycurrency[c] for c in pair.split('_'));pairs.append({'pair':pair,'cutoff_epoch':cutoff,'base_state_sha256':a['state_sha256'],'quote_state_sha256':b['state_sha256'],
                'base_reconstructed_cells':a['reconstructed_component_cells'],'quote_reconstructed_cells':b['reconstructed_component_cells'],
                'base_minus_quote_numeric_value':None,'shared_events_are_independent_votes':False,'runtime_feature_cells':0,'forecast_admission':False})
    report={'cutoffs':len(snapshots),'currency_rows':sum(len(s['states']) for s in states),'pair_rows':len(pairs),'component_versions':len(index),
        'reconstructed_cell_event_cutoff_rows':sum(len(e['reconstructed_component_cells']) for s in snapshots for e in s['events']),
        'event_status_counts':dict(sorted(Counter(e['status'] for s in snapshots for e in s['events']).items())),'parent_run_identities':parent_ids,
        'runtime_feature_cells':0,'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'independent_review':False,
        'next_item':'macro_numeric_qualification_gap_disposition_v2'}
    return {'component_evidence_cache.json':[index[k] for k in sorted(index)],'repaired_component_source_asof.json':snapshots,
        'repaired_component_currency_states.json':states,'repaired_component_pair_views.json':pairs,'component_state_report.json':report}
