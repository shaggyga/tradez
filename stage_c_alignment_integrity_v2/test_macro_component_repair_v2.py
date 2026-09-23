import copy,datetime as dt,json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_component_repair_v2 as core
import macro_component_repair_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_COMPONENT_REPAIR_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_component_repair/inputs')))
RECIPE=ROOT/'MACRO_COMPONENT_REPAIR_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_COMPONENT_REPAIR_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('component-repair')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def native():return core.recovered((INPUTS/'recovered_component_parsers.py').read_bytes())
def fields(text):return {'headline':'UK Labour Market: September 2026','summary':text,'source_id':'ons_published_releases','source_verified':True,'source_direct':True,'detail_enriched':True,'detail_enrichment_kind':'ons_release_bundle'}
EARN="Annual growth in employees' average regular earnings (excluding bonuses) was 3.5% in May to July 2026."
PAY='Estimates for payrolled employees in the UK fell by 101,000 (0.3%) between July 2025 and July 2026, and decreased by 19,000 (0.1%) between June and July 2026.'

def test_real_values_periods_and_regional_rejection(completed):
    report=op.read(completed/'component_repair_report.json');assert report['component_evidence_cells']==9 and report['ons_evidence_cells']==6 and report['rejected_documents']==1
    cells=op.read(completed/'qualified_component_cells.json');pay=[c for c in cells if c['component_id']=='payroll_employees_monthly_change']
    assert {(c['actual'],c['reference']['end_month']) for c in pay}=={('-19000','2026-07'),('-26000','2026-08')}
    byid={c['component_id']:c for c in cells};assert byid['regular_earnings_growth']['actual']=='3.5' and byid['total_earnings_growth']['actual']=='3.9' and byid['unemployment_rate']['actual']=='4.9'
    rate=byid['insured_unemployment_rate_sa'];assert rate['reported_values']['previous_unrevised']=='1.2' and rate['retained_native_component']['previous_unrevised']==1.2000000000000002
    assert rate['reference']['week_ending']=='2026-09-05' and byid['initial_claims_sa']['reference']['week_ending']=='2026-09-12'
    assert all(not c['forecast_admission'] and not c['original_extraction_readiness_proven'] for c in cells)

@pytest.mark.parametrize('field,value',[('source_verified',False),('source_direct',False),('detail_enriched',False),('headline','Labour market in the regions of the UK: September 2026'),('source_id','unknown'),('detail_enrichment_kind','calendar')])
def test_scope_gates(field,value):
    f=fields(EARN);f[field]=value;assert not core.ons_adapter(native(),f)['cells']

def test_annual_count_not_monthly_and_decimal_count_rejected():
    r=core.ons_adapter(native(),fields(PAY));assert len(r['cells'])==1 and r['cells'][0]['actual']=='-19000'
    for text in [PAY.replace('19,000','0.3'),PAY.replace('19,000','19,00'),PAY.replace('between June and July','between May and July')]:assert not core.ons_adapter(native(),fields(text))['cells']

def test_negative_earnings_sign_preserved_and_subject_required():
    c=core.ons_adapter(native(),fields(EARN.replace('3.5%','-0.3%')))['cells'];assert c[0]['actual']=='-0.3'
    assert not core.ons_adapter(native(),fields('Regular earnings were discussed and 3.9% for total earnings.'))['cells']

def test_conflicting_same_period_abstains_without_first_match_fallback():
    r=core.ons_adapter(native(),fields(EARN+' '+EARN.replace('3.5%','3.6%')));assert not r['cells'] and r['blocked'][0]['reason']=='conflicting_same_period_component_values'
    same=core.ons_adapter(native(),fields(EARN+' '+EARN));assert len(same['cells'])==1 and len(same['cells'][0]['evidence'])==2

def test_missing_or_wrong_earnings_period_abstains():
    for text in [EARN.replace('2026',''),EARN.replace('May to July','June to July'),EARN.replace('May to July','July to May')]:assert not core.ons_adapter(native(),fields(text))['cells']

def test_spans_and_hashes_bind_exact_evidence(completed):
    for r in op.read(completed/'component_repair_evidence.json'):
        if 'normalized_text' not in r['adapter']:continue
        text=r['adapter']['normalized_text']
        for c in r['adapter']['cells']:
            h=c.pop('evidence_sha256');assert core.fingerprint(c)==h
            for s in c['evidence']:assert text[s['start']:s['end']]==s['quote']

def test_bounded_date_year_rollover_and_future_rejection():
    assert core.bounded_week('December 28',dt.date(2026,1,2))==dt.date(2025,12,28)
    for raw in ['January 3','November 20']:
        with pytest.raises(ValueError,match='component_week_not_uniquely_bounded'):core.bounded_week(raw,dt.date(2026,1,2))

def dol_inputs():
    f=next(json.loads(v['content_json']) for v in op.read(INPUTS/'component_versions.json') if 'dol_' in json.loads(v['content_json'])['source_id'])
    s=next(s for s in op.read(INPUTS/'source_configs.json') if s['source_id']==f['source_id'])
    return (INPUTS/'dol_claims.txt').read_text(encoding='utf-8'),s,{'scheduled_utc':f['scheduled_utc'],'reference_period':f['reference_period']}

def test_dol_aware_clock_required():
    text,s,e=dol_inputs();e['scheduled_utc']='2026-09-17T12:30:00'
    with pytest.raises(ValueError,match='aware_dol_schedule_required'):core.dol_adapter(native(),text,s,e)

def test_dol_component_dates_cannot_inherit_parent():
    text,s,e=dol_inputs()
    with pytest.raises(ValueError,match='dol_component_week_relationship_invalid'):core.dol_adapter(native(),text.replace('September 5','September 6'),s,e)

def test_original_input_drift_rejected():
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['component_versions.json']+=b' '
    with pytest.raises(ValueError,match='component_input_pin_mismatch'):core.build(blobs)

def test_crash_resume_identical(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_component_repair_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91;r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified'
    assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
