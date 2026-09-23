"""Narrow, auditable offline component evidence adapters. No historical admission."""
import calendar,datetime as dt,hashlib,json,re
from decimal import Decimal
from contracts import fingerprint
from macro_component_audit_v2 import build as audited,recovered,dol_parse,require,match_record

ADAPTER='component_subject_unit_period_guard.v2'
MONTH='(?:January|February|March|April|May|June|July|August|September|October|November|December)'
COUNT=r'(?:\d{1,3}(?:,\d{3})+|\d+)'
DEC=r'-?\d+(?:\.\d+)?'
VERB='(?:decreased|increased|fell|rose)'
def month(name):return list(calendar.month_name).index(name.capitalize())
def month_id(name,year):return f'{int(year):04d}-{month(name):02d}'
def month_delta(start,end):
    a,b=map(int,start.split('-'));c,d=map(int,end.split('-'));return (c-a)*12+d-b
def signed(value,verb):return str(Decimal(value.replace(',',''))*(-1 if verb.lower() in {'fell','decreased'} else 1))
def cell(cid,value,unit,reference,spans,**extra):
    result={'component_id':cid,'actual':str(value),'unit':unit,'reference':reference,'evidence':spans,'adapter_id':ADAPTER,
        'numeric_fact_admission':False,'forecast_admission':False,'original_prior_vintage_verified':False,'original_extraction_readiness_proven':False,**extra}
    result['evidence_sha256']=fingerprint(result);return result

def consolidate(candidates):
    groups={}
    for c in candidates:groups.setdefault((c['component_id'],fingerprint(c['reference'])),[]).append(c)
    accepted=[];blocked=[]
    for (cid,ref),rows in sorted(groups.items()):
        signatures={fingerprint({k:v for k,v in r.items() if k not in {'evidence','evidence_sha256'}}) for r in rows}
        if len(signatures)!=1:blocked.append({'component_id':cid,'reference':rows[0]['reference'],'reason':'conflicting_same_period_component_values','candidates':rows});continue
        first=dict(rows[0]);first['evidence']=sorted([span for r in rows for span in r['evidence']],key=lambda s:(s['start'],s['end']))
        first.pop('evidence_sha256');first['evidence_sha256']=fingerprint(first);accepted.append(first)
    return accepted,blocked

def ons_adapter(native,fields):
    title=fields.get('headline','')
    if not (fields.get('source_id')=='ons_published_releases' and fields.get('source_verified') is True and fields.get('source_direct') is True and fields.get('detail_enriched') is True
        and fields.get('detail_enrichment_kind') in {'ons_release_bundle','ons_release_bulletin'} and re.fullmatch(rf'UK Labour Market:\s+{MONTH}\s+20\d{{2}}',title,re.I)):
        return {'status':'source_or_national_document_scope_rejected','cells':[],'blocked':[]}
    text=native.clean_text(fields.get('summary'));candidates=[];unqualified=[]
    def add(m,cid,value,unit,reference,**extra):candidates.append(cell(cid,value,unit,reference,[match_record(m,text)],**extra))
    # Full named-subject sentence binds two coordinated changes. Annual and monthly values are never interchangeable.
    prefix=rf'(?:Estimates for payrolled employees in the UK|The early estimate of payrolled employees for {MONTH}\s+20\d{{2}})'
    payroll=rf'{prefix}\s+{VERB}\s+by\s+{COUNT}\s+\({DEC}%\)\s+(?:between {MONTH}\s+20\d{{2}} and {MONTH}\s+20\d{{2}}|on the year), and (?P<verb>{VERB}) by (?P<value>{COUNT})\s+\({DEC}%\) between (?P<start>{MONTH}) and (?P<end>{MONTH}) (?P<year>20\d{{2}})(?: to {DEC} million)?\.'
    for m in re.finditer(payroll,text,re.I):
        a=month_id(m['start'],m['year']);b=month_id(m['end'],m['year'])
        if month_delta(a,b)!=1:unqualified.append({'reason':'payroll_interval_not_one_month','evidence':match_record(m,text)});continue
        add(m,'payroll_employees_monthly_change',signed(m['value'],m['verb']),'persons_change',{'start_month':a,'end_month':b,'comparison':'month_over_month'},provisional=m[0].lower().startswith('the early estimate'))
    earnings=rf'Annual growth in (?P<subject>employees\x27 average regular earnings \(excluding bonuses\)|total earnings \(including bonuses\)) was (?P<value>{DEC})% in (?P<start>{MONTH}) to (?P<end>{MONTH}) (?P<year>20\d{{2}})\b'
    for m in re.finditer(earnings,text,re.I):
        a=month_id(m['start'],m['year']);b=month_id(m['end'],m['year'])
        if month_delta(a,b)!=2:unqualified.append({'reason':'earnings_interval_not_three_months','evidence':match_record(m,text)});continue
        cid='regular_earnings_growth' if m['subject'].lower().startswith('employees') else 'total_earnings_growth'
        add(m,cid,Decimal(m['value']),'year_percent_change',{'start_month':a,'end_month':b,'comparison':'year_over_year_three_month_average'})
    unemployment=rf'The UK unemployment rate for people aged 16 years and over was estimated at (?P<value>{DEC})% in (?P<start>{MONTH}) to (?P<end>{MONTH}) (?P<year>20\d{{2}})\.'
    for m in re.finditer(unemployment,text,re.I):
        a=month_id(m['start'],m['year']);b=month_id(m['end'],m['year']);value=Decimal(m['value'])
        if month_delta(a,b)!=2 or not 0<=value<=100:unqualified.append({'reason':'unemployment_period_or_level_invalid','evidence':match_record(m,text)});continue
        add(m,'unemployment_rate',value,'percent_level',{'start_month':a,'end_month':b,'comparison':'level_three_month_average'},population='UK age16plus')
    vacancy=rf'The early estimate of the number of vacancies in the UK (?P<verb>{VERB}) by (?P<value>{COUNT})\s+\({DEC}%\) to {COUNT} in (?P<start>{MONTH}) to (?P<end>{MONTH}) (?P<year>20\d{{2}}), compared with (?P<priorstart>{MONTH}) to (?P<priorend>{MONTH}) (?P<prioryear>20\d{{2}})\.'
    for m in re.finditer(vacancy,text,re.I):
        a=month_id(m['start'],m['year']);b=month_id(m['end'],m['year']);pa=month_id(m['priorstart'],m['prioryear']);pb=month_id(m['priorend'],m['prioryear'])
        if month_delta(a,b)!=2 or month_delta(pa,pb)!=2 or month_delta(pb,a)!=1:unqualified.append({'reason':'vacancy_comparison_not_adjacent_quarters','evidence':match_record(m,text)});continue
        add(m,'vacancies_change',signed(m['value'],m['verb']),'positions_change',{'start_month':a,'end_month':b,'prior_start_month':pa,'prior_end_month':pb,'comparison':'quarter_over_quarter'},provisional=True)
    cells,blocked=consolidate(candidates)
    return {'status':'scoped_text_evidence_only' if cells else 'no_qualified_component_grammar','cells':cells,'blocked':blocked+unqualified,
        'normalized_text':text,'span_coordinate_system':'original_clean_text_normalized_string','unsupported_or_absent_components_abstain':True}

def bounded_week(raw,release):
    parsed=dt.datetime.strptime(raw+' 2000','%B %d %Y');candidates=[]
    for year in (release.year-1,release.year):
        try:d=dt.date(year,parsed.month,parsed.day)
        except ValueError:continue
        if 0<=(release-d).days<=21:candidates.append(d)
    require(len(candidates)==1,'component_week_not_uniquely_bounded_by_release');return candidates[0]

def dol_adapter(native,text,source,event):
    scheduled=dt.datetime.fromisoformat(event['scheduled_utc'].replace('Z','+00:00'));require(scheduled.tzinfo is not None,'aware_dol_schedule_required')
    result=dol_parse(native,text,source,event);require(result['status']=='parsed','dol_native_document_guard_rejected:'+result.get('reason',''))
    out=result['output'];release=dt.datetime.fromisoformat(out['embedded_release_utc']).date();cs=out['source_native_components']
    dates={k:bounded_week(v['reference_period'],release) for k,v in cs.items()}
    require((release-dates['initial_claims_sa']).days<=7 and (dates['initial_claims_sa']-dates['insured_unemployment_sa']).days==7 and dates['insured_unemployment_rate_sa']==dates['insured_unemployment_sa'],'dol_component_week_relationship_invalid')
    cells=[]
    for cid,trace in zip(('initial_claims_sa','insured_unemployment_sa','insured_unemployment_rate_sa'),result['traces'][1:]):
        span=trace['matches'][0];g=span['groups'];original=cs[cid];values={k:str(v) for k,v in original.items() if k not in {'actual','unit','reference_period'}}
        if cid=='insured_unemployment_rate_sa':
            actual=Decimal(g['actual']);change=Decimal('0') if g['unchanged'] else Decimal(g['rate_change'])*(-1 if g['rate_change_direction'].lower()=='decrease' else 1)
            previous=Decimal(g['previous']) if g['previous'] is not None else actual-change
            require(0<=actual<=100 and 0<=previous<=100 and actual-previous==change,'dol_decimal_rate_arithmetic_invalid')
            values={'previous_unrevised':str(previous),'weekly_change':str(change),'previous_value_derivation':'explicit' if g['previous'] is not None else 'actual_minus_signed_change'}
        else:actual=Decimal(str(original['actual']))
        cells.append(cell(cid,actual,'percent_level' if cid.endswith('rate_sa') else 'claims_count',{'week_ending':dates[cid].isoformat(),'comparison':'weekly_level'},[span],reported_values=values,retained_native_component=original))
    return {'status':'archived_pdf_component_evidence_only','cells':cells,'embedded_release_utc':out['embedded_release_utc'],'original_http_receipt_proven':False,'original_schedule_availability_proven':False,'forecast_admission':False}

def build(blobs):
    baseline=audited(blobs);native=recovered(blobs['recovered_component_parsers.py']);sources={s['source_id']:s for s in json.loads(blobs['source_configs.json'])};rows=[]
    for v in json.loads(blobs['component_versions.json']):
        f=json.loads(v['content_json'])
        result=ons_adapter(native,f) if f['source_id']=='ons_published_releases' else dol_adapter(native,blobs['dol_claims.txt'].decode(),sources[f['source_id']],{'scheduled_utc':f['scheduled_utc'],'reference_period':f['reference_period']})
        rows.append({'version_id':v['version_id'],'content_sha256':v['content_sha256'],'headline':f['headline'],'source_id':f['source_id'],'retained_components':f['release_components'],'adapter':result})
    cells=[{'version_id':r['version_id'],'content_sha256':r['content_sha256'],**c} for r in rows for c in r['adapter']['cells']]
    report={'source_versions':3,'component_evidence_cells':len(cells),'ons_evidence_cells':sum(len(r['adapter']['cells']) for r in rows if r['source_id']=='ons_published_releases'),
        'rejected_documents':sum(r['adapter']['status']=='source_or_national_document_scope_rejected' for r in rows),'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'independent_review':False,
        'next_item':'macro_repaired_component_asof_integration_v2'}
    return {'component_repair_evidence.json':rows,'qualified_component_cells.json':cells,'preserved_component_defects.json':baseline['component_extraction_defects.json'],
        'component_repair_boundaries.json':{'adapter_id':ADAPTER,'original_state_mutated':False,'historical_activation_backdated':False,'unsupported_grammar':'abstain','independent_review':False,'forecast_admission':False},'component_repair_report.json':report}
