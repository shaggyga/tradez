"""Retained reference/provenance binding. Never converts current code into historic readiness."""
import ast,copy,hashlib,json,re
from decimal import Decimal
from contracts import fingerprint
from macro_component_state_v2 import validate_parent

TARGETS={'98d66f4b8ee0a6822b6314d0abf1219cd754600c77e257321dec9e6d494ec2fa',
 '8a7d77280ba91f388e3527dcddb3f506b89357dca099c627ae785691ad4ea542',
 '95821564413987e52a4b1152d555934f987e7c2857a842fb72ce080e0eb0cabd'}
QUARTERS={'March':1,'June':2,'September':3,'December':4}
PERIOD=r'(?P<month>March|June|September|December) quarter(?: (?P<year>(?:19|20)\d{2}))?'

def require(ok,reason):
    if not ok:raise ValueError(reason)

def quarter_reference(fields):
    """Only directly bound reference/title grammar; clocks, URLs and unrelated prose are excluded."""
    matches=[]
    for field,pattern in [('reference_period',PERIOD),('headline',r'Media Release - Australian economy (?:grew|contracted) \d+(?:\.\d+)?% in the '+PERIOD)]:
        text=fields.get(field,'');m=re.fullmatch(pattern,text)
        if m:
            matches.append({'field':field,'start':m.start('month'),'end':m.end(),'quote':text[m.start('month'):m.end()],
                            'quarter':QUARTERS[m['month']],'year':int(m['year']) if m['year'] else None})
    if not matches:return {'status':'unsupported_reference','quarter':None,'year':None,'period':None,'evidence':[]}
    quarters={m['quarter'] for m in matches};years={m['year'] for m in matches if m['year'] is not None}
    if len(quarters)!=1 or len(years)>1:return {'status':'conflicting_reference','quarter':None,'year':None,'period':None,'evidence':matches}
    q=next(iter(quarters));year=next(iter(years)) if years else None
    return {'status':'explicit_quarter' if year else 'quarter_without_year','quarter':q,'year':year,
            'period':f'{year}-Q{q}' if year else None,'evidence':matches}

def parser_lineage(code):
    tree=ast.parse(code);fn=tree.body[0]
    require(isinstance(fn,ast.FunctionDef) and fn.name=='parse_bls_timeseries','expected_bls_parser_required')
    dictionaries=[n for n in ast.walk(fn) if isinstance(n,ast.Dict) and any(isinstance(k,ast.Constant) and k.value=='actual_value' for k in n.keys)]
    require(len(dictionaries)==1,'ambiguous_bls_output_schema')
    row=dictionaries[0];require(all(isinstance(k,ast.Constant) and isinstance(k.value,str) for k in row.keys),'dynamic_output_keys_unresolved')
    keys=sorted(k.value for k in row.keys)
    return {'code_sha256':hashlib.sha256(code).hexdigest(),'output_ast_sha256':hashlib.sha256(ast.dump(row,include_attributes=False).encode()).hexdigest(),
            'output_keys':keys,'missing_output_fields':[k for k in ['numeric_extraction_contract_id','numeric_causal_known_utc'] if k not in keys],
            'evidence_kind':'frozen_current_parser_static_schema','historical_deployment_proven':False,'historical_contract_recovered':False}

def bind(version,rule,lineage):
    require(hashlib.sha256(version['content_json'].encode()).hexdigest()==version['content_sha256'],'version_content_mismatch')
    f=json.loads(version['content_json']);source=f['source_id'];gaps=[];native=None;ref=None
    if source=='abs_latest_releases':
        require(f.get('event_series_id')==rule['event_series_id'] and f.get('unit')==rule['unit'],'abs_series_unit_mismatch')
        m=re.fullmatch(rule['pattern'],f.get('headline',''));require(m is not None,'native_abs_headline_mismatch')
        value=Decimal(m['value'])*(-1 if m['verb'] in rule['negative_verbs'] else 1)
        require(value==Decimal(str(f['actual_value'])) and m['period']==f.get('reference_period'),'native_abs_value_reference_mismatch')
        native={'actual':str(value),'reference_period':m['period'],'pattern':rule['pattern'],'rule_ast_sha256':rule['rule_ast_sha256'],
                'current_collector_sha256':rule['source_sha256'],'historical_deployment_proven':False}
        ref=quarter_reference(f)
        if ref['status']!='explicit_quarter':gaps.append(ref['status'])
        parser={'evidence_kind':'exact_native_headline_rule_reproduction','historical_deployment_proven':False}
    else:
        require(source=='bls_major_timeseries_batch_v1' and f.get('event_series_id') in {'WPSFD4','WPSFD49116'},'unsupported_provenance_source')
        parser=copy.deepcopy(lineage);gaps.append('original_bls_level_payload_missing')
    contract=f.get('numeric_extraction_contract_id') or None;clock=f.get('numeric_causal_known_utc') or None
    if not contract:gaps.append('original_numeric_extraction_contract_missing')
    if not clock:gaps.append('original_numeric_readiness_clock_missing')
    gaps.append('historical_adapter_activation_unproven')
    result={'version_id':version['version_id'],'content_sha256':version['content_sha256'],'source_id':source,
        'event_series_id':f.get('event_series_id'),'original_actual':f.get('actual_value'),'original_reference_period':f.get('reference_period'),
        'retained_numeric_extraction_contract_id':contract,'retained_numeric_causal_known_utc':clock,
        'reference_binding':ref,'native_reproduction':native,'parser_lineage':parser,'unresolved':gaps,
        'inferred_clock':None,'inferred_contract':None,'forecast_admission':False,'original_fields_modified':False,'adapter_available_epoch':None}
    result['binding_sha256']=fingerprint(result);return result

def join_provenance(event,index,material,cutoff):
    original=copy.deepcopy(event);bindings=[];status='no_scoped_selected_version'
    ids=event.get('selected_numeric_version_ids',[])
    if event.get('status')!='retained_numeric_observation_ready':status='original_numeric_gate_blocked'
    elif not isinstance(event.get('source_numeric_available_epoch'),(int,float)) or not event['source_numeric_available_epoch']<=cutoff:status='numeric_readiness_missing_or_future'
    elif ids and any(vid not in index for vid in ids):status='incomplete_selected_version_evidence'
    else:
        for vid in ids:
            if vid not in index:continue
            b=index[vid]
            # Material identity is independently bound; no borrowing another version's provenance.
            candidates=[m for m in material if m['numeric_cache_key']==event.get('numeric_cache_key') and m['content_sha256']==b['content_sha256'] and m['source_id']==b['source_id']==event.get('source_id')]
            if len(candidates)!=1:status='selected_content_binding_missing';bindings=[];break
            bindings.append(b['binding_sha256']);status='retrospective_provenance_only'
    return {'original_numeric_event':original,'status':status,'selected_provenance_bindings':bindings,
            'forecast_admission':False,'runtime_feature_cells':[],'adapter_available_epoch':None}

def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('PROVENANCE_INPUT_PLAN.json')
    require(set(plan['payloads'])==set(blobs)-{'PROVENANCE_INPUT_PLAN.json'},'provenance_input_inventory_mismatch')
    require(all(hashlib.sha256(blobs[n]).hexdigest()==h for n,h in plan['payloads'].items()),'provenance_input_pin_mismatch')
    require(plan['adapter_activation_receipt'] is None and plan['raw_bls_levels_available'] is False,'unsupported_provenance_attestation')
    versions=read('target_versions.json');require(len(versions)==3 and {v['version_id'] for v in versions}==TARGETS==set(plan['target_version_ids']),'exact_three_versions_required')
    lineage=parser_lineage(blobs['parse_bls_timeseries.py']);rows=[bind(v,read('ABS_GDP_RULE.json'),lineage) for v in versions]
    validate_parent(blobs,'numeric_parent_manifest.json',['numeric_source_asof.json','numeric_material_cache.json'])
    import datetime as dt
    index={r['version_id']:r for r in rows};material=read('numeric_material_cache.json');snaps=[]
    for s in read('numeric_source_asof.json'):
        cutoff=dt.datetime.fromisoformat(s['cutoff'].replace('Z','+00:00')).timestamp()
        snaps.append({'cutoff':s['cutoff'],'events':[join_provenance(e,index,material,cutoff) for e in s['events']]})
    report={'versions':3,'abs_missing_year':sum(r['reference_binding'] is not None and r['reference_binding']['year'] is None for r in rows),
        'missing_retained_contracts':sum(r['retained_numeric_extraction_contract_id'] is None for r in rows),
        'missing_retained_numeric_clocks':sum(r['retained_numeric_causal_known_utc'] is None for r in rows),
        'cutoffs':len(snaps),'provenance_binding_event_rows':sum(bool(e['selected_provenance_bindings']) for s in snaps for e in s['events']),
        'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'independent_review':False}
    return {'provenance_bindings.json':rows,'provenance_source_asof.json':snaps,'provenance_report.json':report}
