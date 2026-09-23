import copy,datetime as dt,json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_component_audit_v2 as core
import macro_component_audit_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_COMPONENT_AUDIT_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_component_audit/inputs')))
RECIPE=ROOT/'MACRO_COMPONENT_AUDIT_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_COMPONENT_AUDIT_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('component-audit')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

@pytest.fixture
def native():return core.recovered((INPUTS/'recovered_component_parsers.py').read_bytes())

def dol_inputs():
    fields=next(json.loads(v['content_json']) for v in op.read(INPUTS/'component_versions.json') if 'dol_' in json.loads(v['content_json'])['source_id'])
    source=next(s for s in op.read(INPUTS/'source_configs.json') if (s.get('id') or s.get('source_id'))==fields['source_id'])
    return (INPUTS/'dol_claims.txt').read_text(encoding='utf-8'),source,{'scheduled_utc':fields['scheduled_utc'],'reference_period':fields['reference_period']}

def test_actual_population_reproduces_every_component(completed):
    r=op.read(completed/'component_audit_report.json')
    assert r['retained_components']==9 and r['ons_component_lists_reproduced']==2 and r['dol_components_reproduced'] and r['dol_native_components_reproduced']
    assert r['demonstrated_real_defects']==4 and r['forecast_features_admitted']==r['numeric_surprises_computed']==r['base_models_fitted']==0

def test_exact_normalized_spans_and_defects(completed):
    rows=op.read(completed/'ons_component_spans.json')
    for r in rows:
        for p in r['patterns']:
            for m in p['matches']:
                assert r['normalized_text'][m['start']:m['end']]==m['quote']
                assert r['normalized_text'][m['value_start']:m['value_end']]==m['numeric_token']
        assert not r['numeric_fact_admission']
    assert {r['defect'] for r in op.read(completed/'component_extraction_defects.json')}=={'regional_document_admitted_to_national_series','percentage_numeric_prefix_mislabeled_as_count','total_earnings_value_assigned_to_regular_earnings','annual_payroll_change_assigned_to_monthly_component'}

def test_adversarial_percentage_ambiguity_and_sign_reproduce_defects(completed):
    rows={r['case']:r for r in op.read(completed/'component_parser_challenges.json')}
    assert rows['percentage_as_count']['native_output']['release_components'][0]['actual_value']==0
    assert rows['vacancies_percentage']['native_output']['release_components'][0]['actual_value']==0
    assert rows['ambiguous_two_counts']['patterns'][0]['match_count']==2
    assert rows['negative_rate_sign']['native_output']['release_components'][0]['actual_value']==0.3

def test_dol_pdf_not_generated_summary_and_distinct_dates(completed):
    d=op.read(completed/'dol_component_reproduction.json');assert d['generated_summary_reparse']['status']=='rejected'
    out=d['native_pdf_reparse']['output'];cs=out['source_native_components']
    assert cs['initial_claims_sa']['reference_period']=='September 12' and cs['insured_unemployment_sa']['reference_period']=='September 5'
    assert out['embedded_release_utc']=='2026-09-17T12:30:00+00:00'
    assert not d['original_http_receipt_proven'] and not d['original_extraction_readiness_proven']
    assert all(len(t['matches'])==1 for t in d['native_pdf_reparse']['traces'])

@pytest.mark.parametrize('old,new,reason',[
    ('196,000','195,000','weekly-change arithmetic'),
    ('1,774,000','1,775,000','revision arithmetic'),
    ('September 17, 2026','September 16, 2026','does not match schedule'),
    ('September 12, the advance','September 11, the advance','reference period does not match'),
])
def test_dol_rejects_arithmetic_identity_and_period_mutations(native,old,new,reason):
    text,s,e=dol_inputs();assert old in text
    result=core.dol_parse(native,text.replace(old,new),s,e)
    assert result['status']=='rejected' and reason in result['reason']

def test_duplicate_dol_publication_clock_rejected(native):
    text,s,e=dol_inputs();result=core.dol_parse(native,text+' EMBARGOED UNTIL 8:30 A.M. (Eastern) Thursday, September 17, 2026',s,e)
    assert result['status']=='rejected' and 'ambiguous' in result['reason']

def test_missing_schedule_rejected(native):
    text,s,e=dol_inputs();e['scheduled_utc']='';assert core.dol_parse(native,text,s,e)['status']=='rejected'

def test_no_implicit_clock_and_unverified_source(native):
    with pytest.raises(ValueError,match='implicit_current_clock_forbidden'):native.iso_utc()
    assert native.official_numeric_release_fields({'source_verified':False},first_seen=dt.datetime(2026,9,22,tzinfo=dt.timezone.utc))=={}

def test_pinned_input_drift_rejected():
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['dol_claims.txt']+=b' '
    with pytest.raises(ValueError,match='component_input_pin_mismatch'):core.build(blobs)

def test_crash_resume_exact_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_component_audit_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
