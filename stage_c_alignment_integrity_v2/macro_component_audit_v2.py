"""Offline reproduction of preserved component parsers; evidence, not acceptance."""
import ast,datetime as dt,hashlib,json,re,types

def require(value,message):
    if not value:raise ValueError(message)

def recovered(blob):
    module=types.ModuleType('frozen_component_parser')
    exec(compile(blob,'recovered_component_parsers.py','exec'),module.__dict__)
    def no_clock():raise ValueError('implicit_current_clock_forbidden')
    module.utc_now=no_clock
    return module

def patterns(blob):
    tree=ast.parse(blob);fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='official_numeric_release_fields')
    result=[]
    for n in ast.walk(fn):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='add_component':
            result.append({'component_id':ast.literal_eval(n.args[0]),'pattern':ast.literal_eval(n.args[1]),**{k.arg:ast.literal_eval(k.value) for k in n.keywords}})
    require(len(result)==5,'unexpected_ons_component_pattern_inventory');return result

def match_record(m,text):
    a,b=m.span();return {'start':a,'end':b,'quote':text[a:b],'groups':m.groupdict(),
        'context':text[max(0,a-160):min(len(text),b+200)]}

def ons_audit(native,definitions,fields,first_seen):
    raw={**fields,'title':fields['headline']};text=native.clean_text(fields.get('summary'))
    result=native.official_numeric_release_fields(raw,first_seen=first_seen)
    spans=[]
    for p in definitions:
        matches=list(re.finditer(p['pattern'],text,re.I));items=[]
        for m in matches:
            record=match_record(m,text);a,b=m.span('value');following=text[b:b+12]
            record.update(value_start=a,value_end=b,numeric_token=text[a:b],
                integer_prefix_of_decimal=p['unit'] in {'persons','positions'} and bool(re.match(r'\.\d',following)),
                count_captures_percent=p['unit'] in {'persons','positions'} and bool(re.match(r'(?:\.\d+)?\s*%',following)))
            items.append(record)
        spans.append({**p,'native_selection':'first_match_only','matches':items,'match_count':len(items)})
    return {'headline':fields['headline'],'native_output':result,'retained_components':fields.get('release_components'),
        'components_reproduced':result.get('release_components')==fields.get('release_components'),
        'regional_document':bool(re.search(r'\bregions?\b',fields['headline'],re.I)),
        'normalized_text':text,'normalization':'original clean_text: HTML unescape, tag strip, whitespace collapse',
        'span_coordinate_system':'normalized_text, not original HTML bytes','patterns':spans,'numeric_fact_admission':False}

class TraceRegex:
    def __init__(self):self.calls=[]
    def __getattr__(self,key):return getattr(re,key)
    def finditer(self,pattern,text,flags=0):
        matches=list(re.finditer(pattern,text,flags));self.calls.append({'pattern':pattern,'matches':[match_record(m,text) for m in matches]});return iter(matches)

def dol_parse(native,text,source,event):
    trace=TraceRegex();original=native.re;native.re=trace
    try:
        result=native.parse_dol_weekly_claims_pdf_text(text,source=source,event=event)
        return {'status':'parsed','output':result,'traces':trace.calls}
    except ValueError as e:return {'status':'rejected','reason':str(e),'traces':trace.calls}
    finally:native.re=original

def build(blobs):
    plan=json.loads(blobs['COMPONENT_AUDIT_PLAN.json'])
    require(set(plan['payloads'])==set(blobs)-{'COMPONENT_AUDIT_PLAN.json'},'component_input_inventory_mismatch')
    require(all(hashlib.sha256(blobs[n]).hexdigest()==h for n,h in plan['payloads'].items()),'component_input_pin_mismatch')
    native=recovered(blobs['recovered_component_parsers.py']);definitions=patterns(blobs['recovered_component_parsers.py'])
    versions=json.loads(blobs['component_versions.json']);bindings={b['version_id']:b for b in json.loads(blobs['version_bindings.json'])}
    require(len(versions)==len(bindings)==3,'component_population_mismatch')
    configs={s['source_id'] if 'source_id' in s else s['id']:s for s in json.loads(blobs['source_configs.json'])}
    ons=[];dol=None
    for v in versions:
        require(hashlib.sha256(v['content_json'].encode()).hexdigest()==v['content_sha256'],'component_content_hash_mismatch')
        b=bindings[v['version_id']];require(b['content_sha256']==v['content_sha256'],'component_binding_mismatch')
        fields=json.loads(v['content_json'])
        if fields['source_id']=='ons_published_releases':
            first=min(o['known_epoch'] for o in b['observations'] if o['known_epoch'] is not None)
            result=ons_audit(native,definitions,fields,dt.datetime.fromtimestamp(first,dt.timezone.utc));result['version_id']=v['version_id'];ons.append(result)
        else:
            source=configs[fields['source_id']];event={'scheduled_utc':fields['scheduled_utc'],'reference_period':fields['reference_period']}
            require(hashlib.sha256(blobs['dol_claims.pdf']).hexdigest()==fields['detail_content_sha256'],'dol_pdf_content_binding_mismatch')
            text=blobs['dol_claims.txt'].decode();capture=json.loads(blobs['PDF_CAPTURE.json'])
            require(capture['text_sha256']==hashlib.sha256(blobs['dol_claims.txt']).hexdigest(),'dol_text_capture_mismatch')
            dol={'version_id':v['version_id'],'pdf_sha256':fields['detail_content_sha256'],'capture':capture,
                 'native_pdf_reparse':dol_parse(native,text,source,event),'generated_summary_reparse':dol_parse(native,fields['summary'],source,event),
                 'retained_components':fields['release_components'],'retained_native_components':fields['source_native_components'],
                 'original_http_receipt_proven':False,'original_extraction_readiness_proven':False,'numeric_fact_admission':False}
            output=dol['native_pdf_reparse'].get('output',{});dol['components_reproduced']=output.get('release_components')==fields['release_components']
            dol['native_components_reproduced']=output.get('source_native_components')==fields['source_native_components']
    challenges=[]
    base={'headline':'UK Labour Market: September 2026','source_id':'ons_published_releases','source_verified':True,'source_direct':True,'detail_enriched':True,'detail_enrichment_kind':'ons_release_bundle'}
    cases=[('percentage_as_count','Payrolled employees fell by 0.3% (101,000 employees) in August 2026.'),
           ('ambiguous_two_counts','Payrolled employees fell by 101,000 in August 2026. Payrolled employees rose by 50,000 in July 2026.'),
           ('negative_rate_sign','The unemployment rate was -0.3%.'),
           ('vacancies_percentage','Vacancies fell by 0.8% (8,000 positions).')]
    for name,text in cases:
        r=ons_audit(native,definitions,{**base,'summary':text},dt.datetime(2026,9,22,tzinfo=dt.timezone.utc));challenges.append({'case':name,**r})
    require(dol is not None,'dol_record_missing')
    defects=[]
    for r in ons:
        if r['regional_document'] and r['native_output']:defects.append({'version_id':r['version_id'],'defect':'regional_document_admitted_to_national_series'})
        for p in r['patterns']:
            if p['matches'] and p['matches'][0]['count_captures_percent']:defects.append({'version_id':r['version_id'],'component_id':p['component_id'],'defect':'percentage_numeric_prefix_mislabeled_as_count','evidence':p['matches'][0]})
            if not p['matches']:continue
            m=p['matches'][0];following=r['normalized_text'][m['value_end']:m['value_end']+180]
            if p['component_id']=='regular_earnings_growth' and re.match(r'\s*%\s+for\s+total earnings\b',following,re.I):
                defects.append({'version_id':r['version_id'],'component_id':p['component_id'],'defect':'total_earnings_value_assigned_to_regular_earnings','evidence':m})
            interval=re.search(r'between\s+([A-Za-z]+)\s+(20\d{2})\s+and\s+([A-Za-z]+)\s+(20\d{2})',following,re.I)
            if p['component_id']=='payroll_employees_monthly_change' and interval and interval[1].lower()==interval[3].lower() and int(interval[4])-int(interval[2])==1:
                defects.append({'version_id':r['version_id'],'component_id':p['component_id'],'defect':'annual_payroll_change_assigned_to_monthly_component','evidence':m,'interval_quote':interval[0]})
    report={'versions':3,'ons_versions':len(ons),'retained_components':sum(len(r['retained_components']) for r in ons)+len(dol['retained_components']),
        'ons_component_lists_reproduced':sum(r['components_reproduced'] for r in ons),'dol_components_reproduced':dol['components_reproduced'],
        'dol_native_components_reproduced':dol['native_components_reproduced'],'demonstrated_real_defects':len(defects),'synthetic_challenges':len(challenges),
        'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_improvement_proven':False,'forecast_features_admitted':0,
        'next_item':'macro_component_extraction_guard_repair_v2','independent_review':False}
    return {'ons_component_spans.json':ons,'dol_component_reproduction.json':dol,'component_parser_challenges.json':challenges,
            'component_extraction_defects.json':defects,'component_audit_report.json':report}
