import os,subprocess,sys
from decimal import Decimal
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_numeric_audit_v2 as core
import macro_numeric_audit_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_NUMERIC_AUDIT_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_numeric_audit/inputs')))
RECIPE=ROOT/'MACRO_NUMERIC_AUDIT_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_NUMERIC_AUDIT_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('numeric')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

@pytest.mark.parametrize('value,expected',[(0,'0'),('-2.6','-2.6'),('196,000','196000'),('\u22121.2','-1.2'),('1e3','1E+3'),
    (True,None),(None,None),('',None),('NaN',None),('inf',None),('1,7',None),('3.3%',None)])
def test_numeric_scalar_parsing_never_uses_boolean_or_ambiguous_locale(value,expected):
    assert core.number(value)==(Decimal(expected) if expected is not None else None)

def test_units_preserve_frequency_and_scale_gaps():
    assert core.unit_schema('year_percent_change')['comparison_period_or_base']=='year'
    assert core.unit_schema('percent change')['status']=='comparison_period_unspecified'
    assert core.unit_schema('percent')['kind']=='percentage_level' and not core.unit_schema('claims')['seasonal_adjustment_verified']
    assert core.unit_schema('index_2023_100')['comparison_period_or_base']=='base2023_100'
    assert core.unit_schema('basis_points')['status']=='unknown_unit'

@pytest.mark.parametrize('value,period',[('2026M08','2026-08'),('202608','2026-08'),('2026-08','2026-08'),('2026 Jul','2026-07'),('August 2026','2026-08'),('2026-09-12','2026-09-12'),('June quarter',None),('202613',None),('2026-02-30',None),('',None)])
def test_reference_normalization_does_not_invent_year_or_frequency(value,period):
    assert core.reference_schema(value)['period']==period
    assert core.reference_schema('2026-09-12')['frequency'] is None

def test_display_rounding_and_mismatch_are_distinct():
    assert core.display_relation(.3999132564547425,'0.400')=='rounded_display_consistent'
    assert core.display_relation(1.2,'+1.2')=='exact_display_value'
    assert core.display_relation(1.2,'1.3')=='display_value_mismatch'

def test_occurrences_keep_exact_original_spans_without_field_binding():
    text='Claims were 196,000, not 1196000. Inflation 3.3%. Earlier 3.3.'
    r=core.occurrences(text,196000);assert r['count']==1 and r['spans'][0]['quote']=='196,000'
    r=core.occurrences(text,3.3);assert r['count']==2
    for s in r['spans']:assert text[s['start']:s['end']]==s['quote']
    assert r['scope']=='numeric_occurrence_only_not_semantic_field_binding'
    assert core.occurrences('Value 1,7',1)['count']==0
    assert core.occurrences('0.400',.3999132564547425,'0.400')['spans'][0]['match']=='rounded_display_only'

def test_numeric_clocks_need_explicit_timezone():
    assert core.clock_value('2026-09-12T12:00:00') is None
    assert core.clock_value('2026-09-12T12:00:00Z')==core.clock_value('2026-09-12T14:00:00+02:00')

def test_full_population_preserves_component_only_rows_and_missing_consensus(completed):
    r=op.read(completed/'numeric_audit_report.json')
    assert r['versions']==2696 and r['numeric_versions']==31 and r['sources']==18
    assert r['finite_field_counts']=={'actual_value':29,'previous_value':16,'revised_previous_value':1}
    assert r['rounded_actual_display_versions']==2 and r['missing_extraction_contract_versions']==2
    assert r['release_component_rows']==9 and r['reference_status_counts']['unresolved_reference']==1
    assert r['numeric_surprises_computed']==r['original_prior_vintages_verified']==r['forecast_features_admitted']==0

def test_all_reported_priors_remain_current_snapshot_claims(completed):
    for r in op.read(completed/'numeric_evidence_audit.json'):
        assert not r['original_prior_release_vintage_verified'] and r['numeric_surprise'] is None and not r['forecast_admission']
        assert r['consensus_status']=='no_retained_consensus'
        if r['values']['previous_value']['finite_decimal'] is not None:
            assert r['prior_status']=='reported_prior_in_current_snapshot_not_original_vintage'

def test_input_pin_and_crash_resume(tmp_path,completed):
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['numeric_projection.json']+=b' '
    with pytest.raises(ValueError,match='numeric_audit_input_pin_mismatch'):core.build(blobs)
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_numeric_audit_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','4'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
