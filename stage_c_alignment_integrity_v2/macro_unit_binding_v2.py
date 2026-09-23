"""Scoped source-native unit/subject evidence, with original records preserved."""
import ast,calendar,copy,datetime as dt,hashlib,json,re,types
from collections import Counter
from collections.abc import Mapping
from decimal import Decimal,InvalidOperation,localcontext
from contracts import fingerprint
from macro_component_state_v2 import evidence_index,join_event,validate_parent

ADAPTER='source_native_unit_subject_binding.v2'
MONTH='(?:January|February|March|April|May|June|July|August|September|October|November|December)'
N=r'\d+(?:\.\d+)?'
COUNT=r'(?:\d{1,3}(?:,\d{3})+|\d+)'
MONEY=COUNT+r'(?:\.\d+)?'

def require(value,message):
    if not value:raise ValueError(message)

def strict_decimal(value):
    require(not isinstance(value,bool) and isinstance(value,(str,int,float,Decimal)),'invalid_numeric_value')
    text=str(value);require(len(text)<=64 and bool(re.fullmatch(r'[+-]?\d+(?:\.\d+)?',text)),'invalid_numeric_value')
    try:v=Decimal(text)
    except InvalidOperation:raise ValueError('invalid_numeric_value')
    require(v.is_finite(),'invalid_numeric_value');return v

def month_id(name,year):return f'{int(year):04d}-{list(calendar.month_name).index(name.capitalize()):02d}'
def previous_month(period):
    year,month=map(int,period.split('-'));return f'{year-1 if month==1 else year:04d}-{12 if month==1 else month-1:02d}'
def reference(raw):
    m=re.fullmatch(rf'(?P<month>{MONTH}) (?P<year>20\d{{2}})',str(raw),re.I)
    require(m is not None,'explicit_reference_month_required');return month_id(m['month'],m['year'])
def span(match,text):return {'start':match.start(),'end':match.end(),'quote':match[0],'coordinate_system':'normalized_summary'}
def signed(value,verb):return str(strict_decimal(value)*(-1 if verb.lower() in {'below','down'} else 1))

def native_tools(blobs):
    module=types.ModuleType('frozen_unit_helpers');exec(compile(blobs['pure_numeric_helpers.py'],'pure_numeric_helpers.py','exec'),module.__dict__)
    module.Mapping=Mapping
    original=ast.parse(blobs['parse_bls_timeseries.py']).body[0]
    # Reuse exact original filtering/sorting/formula statements, without acquisition or row formatting.
    start=next(i for i,n in enumerate(original.body) if isinstance(n,ast.Assign) and isinstance(n.value,ast.ListComp))
    end=next(i for i,n in enumerate(original.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='series_id' for t in n.targets))
    fn=ast.parse('def original_bls_calculation(observations, source):\n    pass\n').body[0]
    fn.body=copy.deepcopy(original.body[start:end])+ast.parse("return {'actual':actual_value,'previous':previous_value,'observations':observations}").body
    tree=ast.fix_missing_locations(ast.Module(body=[fn],type_ignores=[]));exec(compile(tree,'frozen_bls_calculation_fragment','exec'),module.__dict__)
    module.formula_ast_sha256=hashlib.sha256(ast.dump(tree,include_attributes=False).encode()).hexdigest()
    rss=ast.parse(blobs['parse_rss.py']);assignment=next(n for n in ast.walk(rss) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='value_matches' for t in n.targets))
    call=next(n for n in ast.walk(assignment) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='finditer')
    module.census_pattern=ast.literal_eval(call.args[0])
    rule=ast.parse(blobs['STATCAN_NUMERIC_RELEASE_RULES.py']).body[0];module.statcan_rules=ast.literal_eval(rule.value)
    return module

def guarded_bls(observations,config,expected_series,actual_series):
    require(expected_series==actual_series==config.get('series_id'),'bls_series_identity_mismatch')
    require(config.get('value_transform')=='percent_change_1','unsupported_bls_transform')
    require(isinstance(observations,list) and len(observations)<=4096,'bounded_bls_observations_required')
    rows={}
    for row in observations:
        require(isinstance(row,dict),'invalid_bls_observation')
        p=row.get('period');require(isinstance(p,str),'invalid_bls_period')
        if p=='M13':continue # Annual average cannot be a monthly denominator.
        require(bool(re.fullmatch(r'M(?:0[1-9]|1[0-2])',p)),'invalid_bls_period')
        year=str(row.get('year'));require(bool(re.fullmatch(r'(?:19|20)\d{2}',year)),'invalid_bls_year')
        period=year+'-'+p[1:];require(period not in rows,'duplicate_bls_month')
        v=strict_decimal(row.get('value'));require(v>0,'nonpositive_bls_index_level');rows[period]=v
    ordered=sorted(rows,reverse=True);require(len(ordered)>=3,'three_month_levels_required')
    latest,prior,older=ordered[:3]
    require(previous_month(latest)==prior and previous_month(prior)==older,'nonadjacent_bls_months')
    with localcontext() as ctx:
        ctx.prec=50
        actual=str((rows[latest]/rows[prior]-1)*100);previous=str((rows[prior]/rows[older]-1)*100)
    return {'actual':actual,'reported_previous_change':previous,
        'reference_month':latest,'previous_month':prior,'comparison':'month_over_month','seasonal_adjustment':'not_verified',
        'original_vintage_verified':False,'forecast_admission':False}

def evidence_cell(cid,value,ref,evidence,**extra):
    c={'component_id':cid,'actual':str(value),'unit':'percent_change','reference':ref,'evidence':evidence,'adapter_id':ADAPTER,
        'numeric_fact_admission':False,'forecast_admission':False,'original_extraction_readiness_proven':False,
        'original_prior_vintage_verified':False,'raw_response_binding_verified':False,**extra}
    c['evidence_sha256']=fingerprint(c);return c

def unique_matches(pattern,text):
    matches=list(re.finditer(pattern,text,re.I));require(len(matches)==1,'missing_or_ambiguous_subject_sentence');return matches[0]

def census_binding(fields,native):
    require(fields.get('source_id')=='census_economic_indicators' and fields.get('source_verified') is True and fields.get('source_direct') is True,'census_source_scope_rejected')
    text=native.clean_text(fields.get('summary'));period=reference(fields.get('reference_period'));previous=previous_month(period)
    matches=list(re.finditer(native.census_pattern,text,re.I));require(bool(matches),'native_census_footer_missing')
    first=matches[0];require(reference(first[1])==period and strict_decimal(first[2])==strict_decimal(fields['actual_value']),'retained_native_footer_mismatch')
    require(sum(reference(m[1])==period for m in matches)==1,'ambiguous_current_reference_footer')
    native_result={'retained_actual':str(strict_decimal(fields['actual_value'])),'native_actual':first[2],'reference_period':first[1],
        'footer_matches':[span(m,text) for m in matches],'native_pattern':native.census_pattern,'full_rss_replay':False}
    ref={'month':period,'comparison_month':previous,'comparison':'month_over_month'};series=fields.get('event_series_id');cells=[]
    if series=='retail_sales':
        require(fields.get('headline')=='Advance Monthly Sales for Retail and Food Services','census_title_series_mismatch')
        m=unique_matches(rf'U\.S\. retail and food services sales for (?P<month>{MONTH}) (?P<year>20\d{{2}}) were \${N} billion, (?P<verb>up|down) (?P<value>{N}) percent \(\+/-\s*{N} percent\) from the previous month\.',text)
        require(month_id(m['month'],m['year'])==period and strict_decimal(signed(m['value'],m['verb']))==strict_decimal(first[2]),'retail_subject_value_or_period_mismatch')
        cells=[evidence_cell('census_retail_sales_monthly_change',signed(m['value'],m['verb']),ref,[span(m,text),span(first,text)],seasonal_adjustment='not_explicit_in_retained_summary',subject='retail_and_food_services_sales')]
    elif series=='housing_starts':
        require(fields.get('headline')=='New Residential Construction','census_title_series_mismatch')
        m=unique_matches(rf'Privately-owned housing starts in (?P<month>{MONTH}) (?P<year>20\d{{2}}) were at a seasonally adjusted annual rate of (?P<level>{COUNT})\. This is (?P<value>{N}) percent \(\+/-\s*{N}%\)\*? (?P<verb>below|above) the revised (?P<prior_month>{MONTH}) (?P<prior_year>20\d{{2}}) estimate of (?P<prior_level>{COUNT})\.',text)
        require(month_id(m['month'],m['year'])==period and month_id(m['prior_month'],m['prior_year'])==previous,'housing_comparison_not_adjacent_month')
        value=signed(m['value'],m['verb']);require(strict_decimal(value)==strict_decimal(first[2]),'housing_footer_direction_mismatch')
        cells=[evidence_cell('census_housing_starts_monthly_change',value,ref,[span(m,text),span(first,text)],
            seasonal_adjustment='explicit_seasonally_adjusted_annual_rate_levels',level_annualized=True,comparison_annualized=False,
            reported_level=m['level'].replace(',',''),reported_revised_prior_level=m['prior_level'].replace(',',''),subject='privately_owned_housing_starts')]
    elif series=='business_sales':
        require(fields.get('headline')=='Manufacturing and Trade Inventories and Sales','census_title_series_mismatch')
        inventory=unique_matches(rf'U\.S\. total business end-of-month inventories for (?P<month>{MONTH}) (?P<year>20\d{{2}}) were \${MONEY} billion, (?P<verb>up|down) (?P<value>{N}) percent \(\+/-\s*{N} percent\) from last month\.',text)
        sales=unique_matches(rf'U\.S\. total business sales were \${MONEY} billion, (?P<verb>up|down) (?P<value>{N}) percent \(\+/-\s*{N} percent\) from last month\.',text)
        # Restrict the shared-period relationship to these adjacent sentences, not arbitrary later prose.
        require(not text[inventory.end():sales.start()].strip() and sales.start()>=inventory.end(),'business_subject_sentences_not_adjacent')
        require(month_id(inventory['month'],inventory['year'])==period,'business_reference_mismatch')
        require(re.match(r'\s+in Inventories\b',text[first.end():],re.I) is not None,'inventory_footer_subject_required')
        require(strict_decimal(signed(inventory['value'],inventory['verb']))==strict_decimal(first[2]),'inventory_footer_value_mismatch')
        cells=[evidence_cell('census_business_inventories_monthly_change',signed(inventory['value'],inventory['verb']),ref,[span(inventory,text),span(first,text)],
            subject='business_inventories',seasonal_adjustment='not_explicit_in_retained_summary',retained_series_subject_mismatch=True),
            evidence_cell('census_business_sales_monthly_change',signed(sales['value'],sales['verb']),ref,[span(inventory,text),span(sales,text)],
            subject='business_sales',seasonal_adjustment='not_explicit_in_retained_summary',value_from='explicit_sales_sentence_not_inventory_footer')]
        native_result['subject_defect']='inventory_footer_value_assigned_to_business_sales_series'
    else:raise ValueError('unsupported_census_series')
    return {'status':'scoped_summary_subject_unit_evidence','cells':cells,'normalized_text':text,'native_reproduction':native_result,
        'raw_response_binding_verified':False,'original_numeric_fields_modified':False}

def source_binding(fields,configs,native):
    source=fields['source_id'];config=configs[source]
    if source=='census_economic_indicators':
        try:return census_binding(fields,native)
        except ValueError as exc:return {'status':'census_binding_abstained','reason':str(exc),'cells':[]}
    if source=='bls_major_timeseries_batch_v1':
        series=[s for s in config['series'] if s.get('series_id')==fields.get('event_series_id')];require(len(series)==1,'bls_config_series_binding_ambiguous')
        return {'status':'raw_bls_levels_unavailable','cells':[],'retained_actual':fields['actual_value'],'configured_series':series[0],
            'configured_transform':series[0].get('value_transform'),'comparison':'successive_available_observations_not_verified_adjacent_months',
            'seasonal_adjustment':'not_verified','current_configuration_is_historical_proof':False,'original_formula_ast_sha256':native.formula_ast_sha256,
            'raw_response_binding_verified':False,'required_evidence':'exact three original monthly index levels, month adjacency, series identity and original receipt/vintage'}
    require(source=='statcan_daily_releases','unsupported_unit_binding_source')
    text=native.clean_text(fields['summary']);rules=[r for r in native.statcan_rules if r['event_series_id']==fields.get('event_series_id')]
    require(len(rules)==1,'statcan_rule_binding_ambiguous');rule=rules[0]
    require(re.search(rule['title_pattern'],fields['headline'],re.I) is not None,'statcan_rule_title_mismatch')
    m=re.search(rule['value_pattern'],text,re.I);require(m is not None,'statcan_native_match_missing')
    negative={'edged down','fell','decreased','declined','contracted'};value=str(strict_decimal(m['value'])*(-1 if m['verb'].lower() in negative else 1))
    require(strict_decimal(value)==strict_decimal(fields['actual_value']),'statcan_native_value_mismatch')
    return {'status':'comparison_and_seasonality_not_explicit','cells':[],'native_actual':value,'rule':rule,'native_span':span(m,text),'normalized_text':text,
        'reference_month':reference(fields['reference_period']),'comparison':'unresolved','seasonal_adjustment':'unresolved','raw_response_binding_verified':False}

def probes(native):
    rows=[{'year':'2026','period':p,'value':v} for p,v in [('M08','110'),('M07','100'),('M06','90')]];config={'series_id':'TEST','value_transform':'percent_change_1'};cases=[]
    for name,observations in [('adjacent_months',rows),('missing_month',[rows[0],{**rows[1],'period':'M06'},{**rows[2],'period':'M05'}]),
        ('duplicate_month',[rows[0],{**rows[1],'period':'M08'},rows[2]]),('zero_denominator',[rows[0],{**rows[1],'value':'0'},rows[2]])]:
        original=native.original_bls_calculation(copy.deepcopy(observations),config)
        try:guard={'status':'accepted_scoped_calculation','result':guarded_bls(observations,config,'TEST','TEST')}
        except ValueError as e:guard={'status':'rejected','reason':str(e)}
        cases.append({'case':name,'synthetic':True,'observations':observations,'original_result':original,'guarded_result':guard})
    return cases

def consumer(source_rows,blobs):
    read=lambda n:json.loads(blobs[n]);validate_parent(blobs,'numeric_parent_manifest.json',['numeric_source_asof.json','numeric_material_cache.json'])
    index=evidence_index(source_rows);material={r['numeric_cache_key']:r for r in read('numeric_material_cache.json')};universe=read('universe.json')
    require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required');currencies=sorted({c for p in universe for c in p.split('_')});snapshots=[];states=[];pairs=[]
    for snap in read('numeric_source_asof.json'):
        cutoff=dt.datetime.fromisoformat(snap['cutoff'].replace('Z','+00:00')).timestamp();events=[join_event(e,index,material,cutoff) for e in snap['events']];snapshots.append({'cutoff':snap['cutoff'],'events':events});rows=[]
        for currency in currencies:
            selected=[e for e in events if currency in e['currencies']];state={'currency':currency,'cutoff_epoch':cutoff,'evidence_references':[{'event_id':e['event_id'],'status':e['status'],'version_ids':e['component_evidence_version_ids'],'cell_sha256':[c['evidence_sha256'] for c in e['reconstructed_component_cells']]} for e in selected],
                'reconstructed_cells':sum(len(e['reconstructed_component_cells']) for e in selected),'runtime_feature_cells':0,'cross_series_numeric_aggregate':None,'forecast_admission':False}
            state['state_sha256']=fingerprint(state);rows.append(state)
        states.append({'cutoff':snap['cutoff'],'states':rows});bycurrency={s['currency']:s for s in rows}
        for pair in universe:
            a,b=(bycurrency[c] for c in pair.split('_'));pairs.append({'pair':pair,'cutoff_epoch':cutoff,'base_state_sha256':a['state_sha256'],'quote_state_sha256':b['state_sha256'],
                'base_reconstructed_cells':a['reconstructed_cells'],'quote_reconstructed_cells':b['reconstructed_cells'],'base_minus_quote_numeric_value':None,'runtime_feature_cells':0,'forecast_admission':False,'shared_events_are_independent_votes':False})
    return snapshots,states,pairs

def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('UNIT_BINDING_INPUT_PLAN.json')
    require(set(plan['payloads'])==set(blobs)-{'UNIT_BINDING_INPUT_PLAN.json'},'unit_binding_input_inventory_mismatch')
    require(all(hashlib.sha256(blobs[n]).hexdigest()==h for n,h in plan['payloads'].items()),'unit_binding_input_pin_mismatch')
    require(plan['adapter_activation_receipt'] is None and plan['raw_api_rss_payloads_bound'] is False,'unsupported_source_or_activation_attestation')
    native=native_tools(blobs);configs={s['source_id']:s for s in read('source_configs.json')};versions=read('target_versions.json');targets=read('NEXT_WORK_PACKAGE.json')['target_version_ids']
    require(len(versions)==len(targets)==len(set(targets))==9 and {v['version_id'] for v in versions}==set(targets),'exact_nine_target_versions_required')
    rows=[]
    for v in versions:
        require(hashlib.sha256(v['content_json'].encode()).hexdigest()==v['content_sha256'],'target_version_content_mismatch');fields=json.loads(v['content_json'])
        rows.append({'version_id':v['version_id'],'content_sha256':v['content_sha256'],'source_id':fields['source_id'],'headline':fields['headline'],
            'original_event_series_id':fields.get('event_series_id'),'original_actual':fields.get('actual_value'),'original_unit':fields.get('unit'),
            'adapter':source_binding(fields,configs,native)})
    snapshots,states,pairs=consumer(rows,blobs);synthetic=probes(native)
    gaps={'raw_api_rss_payloads_bound':False,'original_issuer_binding_verified':False,'adapter_activation_receipt':None,'seasonality_not_inferred_from_series_names':True,
        'unresolved_versions':[{'version_id':r['version_id'],'status':r['adapter']['status']} for r in rows if not r['adapter']['cells']],
        'next_engineering_item':'macro_numeric_reference_and_extraction_provenance_binding_v2','next_scope':'Resolve the remaining explicit reference-period and missing extraction-contract provenance records from the numeric disposition, using retained exact evidence. Keep missing original raw payloads, issuer, expectations and activation gates explicit.'}
    report={'versions':len(rows),'sources':len(configs),'scoped_evidence_cells':sum(len(r['adapter']['cells']) for r in rows),'source_status_counts':dict(sorted(Counter(r['adapter']['status'] for r in rows).items())),
        'business_series_subject_mismatch_versions':sum(r['adapter'].get('native_reproduction',{}).get('subject_defect') is not None for r in rows),
        'synthetic_formula_cases':len(synthetic),'cutoffs':len(snapshots),'currency_rows':sum(len(s['states']) for s in states),'pair_rows':len(pairs),
        'reconstructed_cell_event_cutoff_rows':sum(len(e['reconstructed_component_cells']) for s in snapshots for e in s['events']),
        'runtime_feature_cells':0,'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'independent_review':False}
    return {'unit_source_bindings.json':rows,'unit_formula_probes.json':synthetic,'unit_source_asof.json':snapshots,'unit_currency_states.json':states,
        'unit_pair_views.json':pairs,'unit_unresolved_evidence.json':gaps,'unit_binding_report.json':report}
