"""Retained numeric evidence audit; reported values are not verified surprises."""
from collections import Counter,defaultdict
from decimal import Decimal,InvalidOperation,ROUND_HALF_EVEN
import calendar,datetime as dt,hashlib,json,re
from macro_receipt_population_v2 import require

FIELDS=('actual_value','previous_value','revised_previous_value','consensus_value')
TOKEN=re.compile(r'(?<![\w.,])[-+\u2212]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?![\w]|[.,]\d)')

def number(value):
    if value is None or value=='':return None
    if isinstance(value,bool):return None
    if not isinstance(value,(int,float,str)):return None
    text=str(value).strip().replace('\u2212','-')
    if not re.fullmatch(r'[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][-+]?\d+)?',text):return None
    try:
        value=Decimal(text.replace(',',''));return value if value.is_finite() else None
    except InvalidOperation:return None


def unit_schema(unit):
    raw=unit or '';key=raw.strip().lower() if isinstance(raw,str) else ''
    known={'year_percent_change':('percent_change','year'), 'quarter_percent_change':('percent_change','quarter'),
           'period_percent_change':('percent_change',None),'percent change':('percent_change',None),
           'percent':('percentage_level',None),'claims':('count_claims',None),'index_2023_100':('index_level','base2023_100')}
    kind,period=known.get(key,(None,None))
    return {'raw':raw,'kind':kind,'comparison_period_or_base':period,
        'status':'unknown_unit' if kind is None else 'comparison_period_unspecified' if kind=='percent_change' and period is None else 'retained_unit_classified',
        'seasonal_adjustment_verified':False,'unit_magnitude_converted':False}


def reference_schema(value):
    raw=value or '';text=str(raw).strip();year=month=None
    m=re.fullmatch(r'(20\d{2})(?:M|-)?(\d{2})',text)
    if m:year,month=map(int,m.groups())
    months={v.lower():i for i in range(1,13) for v in (calendar.month_name[i],calendar.month_abbr[i])}
    for pattern,reverse in [(r'(20\d{2})\s+([A-Za-z]+)',False),(r'([A-Za-z]+)\s+(20\d{2})',True)]:
        m=re.fullmatch(pattern,text)
        if m:
            a,b=m.groups();year=int(b if reverse else a);month=months.get((a if reverse else b).lower())
    if year and month and 1<=month<=12:return {'raw':raw,'status':'explicit_month','period':f'{year:04d}-{month:02d}','frequency':'month'}
    try:
        if re.fullmatch(r'20\d{2}-\d{2}-\d{2}',text):
            d=dt.date.fromisoformat(text);return {'raw':raw,'status':'explicit_date_not_inferred_economic_frequency','period':d.isoformat(),'frequency':None}
    except ValueError:pass
    return {'raw':raw,'status':'missing_reference' if not text else 'unresolved_reference','period':None,'frequency':None}


def display_relation(value,display):
    actual,shown=number(value),number(display)
    if actual is None:return 'missing_or_invalid_value'
    if shown is None:return 'no_numeric_display'
    if actual==shown:return 'exact_display_value'
    try:
        if actual.quantize(Decimal(1).scaleb(shown.as_tuple().exponent),rounding=ROUND_HALF_EVEN)==shown:return 'rounded_display_consistent'
    except InvalidOperation:pass
    return 'display_value_mismatch'


def occurrences(text,value,display=None):
    target,shown=number(value),number(display);matches=[]
    if target is None:return {'count':0,'spans':[],'scope':'numeric_occurrence_only_not_semantic_field_binding'}
    for m in TOKEN.finditer(text):
        found=number(m.group());kind='exact_numeric_value' if found==target else 'rounded_display_only' if shown is not None and found==shown and display_relation(value,display)=='rounded_display_consistent' else None
        if kind:matches.append({'start':m.start(),'end':m.end(),'quote':m.group(),'match':kind})
    return {'count':len(matches),'spans':matches[:32],'truncated':len(matches)>32,'scope':'numeric_occurrence_only_not_semantic_field_binding'}


def clock_value(raw):
    if not isinstance(raw,str) or not raw:return None
    try:
        d=dt.datetime.fromisoformat(raw.replace('Z','+00:00'))
        return d.timestamp() if d.tzinfo is not None else None
    except ValueError:return None


def audit_row(row,binding,text,clocks):
    f=row['fields'];require(f.get('source_id')==binding['source_id'] and row['content_sha256']==binding['content_sha256'],'numeric_projection_identity_mismatch')
    values={};display_names={'actual_value':'actual','previous_value':'previous','revised_previous_value':'revised_previous','consensus_value':'consensus'}
    for name in FIELDS:
        value=f.get(name);parsed=number(value);display=f.get(display_names[name])
        values[name]={'raw':value,'finite_decimal':str(parsed) if parsed is not None else None,'display_relation':display_relation(value,display),
            'headline_occurrences':occurrences(f.get('headline',''),value,display),'summary_occurrences':occurrences(text,value,display)}
    valid=[c for c in clocks if c['clock_status']=='valid_attested_observation' and c['known_epoch'] is not None]
    reported=[clock_value(c['numeric_causal_known_utc']) for c in valid];parsed=[t for t in reported if t is not None]
    prior_present=number(f.get('previous_value')) is not None or number(f.get('revised_previous_value')) is not None
    components=f.get('release_components') or []
    return {'version_id':row['version_id'],'event_id':binding['event_id'],'cache_key':binding['cache_key'],'source_id':binding['source_id'],
        'event_series_id':f.get('event_series_id'),'numeric_extraction_contract_id':f.get('numeric_extraction_contract_id'),
        'retained_numeric_verification_state':f.get('numeric_verification_state'),'values':values,'unit':unit_schema(f.get('unit')),
        'reference':reference_schema(f.get('reference_period')),'release_stage':f.get('release_stage'),
        'original_prior_release_vintage_verified':False,'prior_status':'reported_prior_in_current_snapshot_not_original_vintage' if prior_present else 'no_reported_prior',
        'consensus_status':'unqualified_retained_value' if number(f.get('consensus_value')) is not None else 'no_retained_consensus',
        'numeric_surprise':None,'native_components':f.get('source_native_components') or {},'release_components':components,
        'recorded_clock_summary':{'retained_valid_observations':len(valid),'parseable_numeric_clocks':len(parsed),
            'missing_or_invalid_numeric_clocks':len(reported)-len(parsed),'first_retained_receipt_epoch':min((c['known_epoch'] for c in valid),default=None),
            'earliest_reported_numeric_epoch':min(parsed,default=None),'latest_reported_numeric_epoch':max(parsed,default=None),
            'clock_authentication_scope':'retained_collector_claim_not_independent_extraction_receipt'},
        'text_evidence_scope':'retained headline/summary may be collector-generated; numeric occurrence is not publisher field verification',
        'forecast_admission':False,'numeric_fact_admission':False}


def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('NUMERIC_AUDIT_PLAN.json')
    for n,h in plan['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'numeric_audit_input_pin_mismatch')
    bindings=read('version_bindings.json');index={b['version_id']:b for b in bindings};projection=read('numeric_projection.json')
    require(len(index)==len(bindings) and len(projection)==len(index) and {r['version_id'] for r in projection}==set(index),'numeric_population_mismatch')
    texts={t['cache_key']:t for t in read('extraction_cache.json')}
    for t in texts.values():require(hashlib.sha256(t['retained_text'].encode()).hexdigest()==t['text_sha256'],'numeric_text_identity_mismatch')
    clocks=defaultdict(list)
    for c in read('numeric_clock_projection.json'):
        require(c['version_id'] in index,'numeric_orphan_clock');clocks[c['version_id']].append(c)
    rows=[];presence=[]
    for r in projection:
        f=r['fields'];finite=[n for n in FIELDS if number(f.get(n)) is not None]
        presence.append({'version_id':r['version_id'],'source_id':f.get('source_id'),'finite_fields':finite,'release_component_count':len(f.get('release_components') or [])})
        if finite or f.get('release_components'):
            b=index[r['version_id']];rows.append(audit_row(r,b,texts[b['cache_key']]['retained_text'],clocks[r['version_id']]))
    component_rows=[{'version_id':r['version_id'],'source_id':r['source_id'],'component':c,
        'original_prior_vintage_verified':False,'forecast_admission':False} for r in rows for c in r['release_components']]
    gaps={'numeric_occurrences_are_not_source_field_binding':True,'source_native_api_summary_may_be_generated':True,
        'prior_vintage_gate':'current reported previous/revised previous is not the historical original release; need exact earlier version evidence',
        'consensus_gate':'no archived expectation value/known-at proof; leave surprise disabled',
        'unit_gate':'unspecified percent-change frequency and seasonal definition require source-specific qualification',
        'clock_gate':'use each retained observation clock plus numeric_causal_known floor before source-asof use; no retrospective extraction-readiness claim',
        'next':'macro_numeric_observation_asof_state_v2'}
    report={'schema':'macro_numeric_units_vintage_evidence_audit.v2','versions':len(projection),'numeric_versions':len(rows),'sources':len({r['source_id'] for r in rows}),
        'finite_field_counts':dict(sorted(Counter(n for p in presence for n in p['finite_fields']).items())),
        'unit_status_counts':dict(sorted(Counter(r['unit']['status'] for r in rows).items())),
        'reference_status_counts':dict(sorted(Counter(r['reference']['status'] for r in rows).items())),
        'missing_extraction_contract_versions':sum(not r['numeric_extraction_contract_id'] for r in rows),
        'blank_numeric_verification_versions':sum(not r['retained_numeric_verification_state'] for r in rows),
        'actual_summary_numeric_occurrence_versions':sum(r['values']['actual_value']['summary_occurrences']['count']>0 for r in rows),
        'rounded_actual_display_versions':sum(r['values']['actual_value']['display_relation']=='rounded_display_consistent' for r in rows),
        'release_component_rows':len(component_rows),'numeric_surprises_computed':0,'original_prior_vintages_verified':0,
        'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,
        'limitations':['all fields retain original raw values and diagnostic occurrences; no inferred units or prior vintages',
            'retained source text may be generated from numeric values; occurrence is not independent semantic verification',
            'reference without year is unresolved; ISO date does not establish economic frequency',
            'no source/outcome filtering, model fit, collector execution or live DB access']}
    return {'numeric_version_presence.json':presence,'numeric_evidence_audit.json':rows,'numeric_component_inventory.json':component_rows,
            'numeric_qualification_gaps.json':gaps,'numeric_audit_report.json':report}
