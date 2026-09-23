import json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_text_coverage_v2 as core
import macro_text_coverage_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_TEXT_COVERAGE_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_text_coverage/inputs')))
RECIPE=ROOT/'MACRO_TEXT_COVERAGE_OPERATOR_RECIPE.json';RULES=op.read(ROOT/'MACRO_TEXT_RULES_V2.json')['rules']

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_TEXT_COVERAGE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('coverage')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def test_full_scan_finds_late_phrase_with_exact_original_offsets():
    text=('Background words. '*300)+'The policy\nrate will be raised.'
    full=core.raw_rule_spans(text,RULES);assert len(full)==1
    assert full[0]['start']>4000 and text[full[0]['start']:full[0]['end']]==full[0]['quote']
    assert full[0]['raw_match_only_not_assertion_or_forecast']

def test_whitespace_normalization_preserves_offsets():
    value,offsets=core.full_normalized(' A\n  B\tC ')
    assert value=='A B C' and [' A\n  B\tC '[offsets[i]] for i in (0,2,4)]==['A','B','C']

def test_native_cleaned_evidence_does_not_invent_raw_span():
    result=core.locate_evidence('Inflation&nbsp;increased.','inflation increased')
    assert result['start'] is None and result['status']=='native_cleaned_span_not_resolved_in_raw_text'
    result=core.locate_evidence('Inflation\n increased.','inflation increased')
    assert result['quote']=='Inflation\n increased'

def test_recovered_function_pin_is_checked_before_execution():
    raw=(INPUTS/'recovered_policy_functions.py').read_bytes();p=op.read(INPUTS/'RECOVERED_FUNCTIONS.json')
    native=core.load_native(raw,p);assert native.explicit_policy_decision_action('The Committee lowered the policy rate.')=='cut'
    with pytest.raises(ValueError,match='recovered_function_pin_mismatch'):core.load_native(raw+b'\nraise RuntimeError()',p)

def test_real_long_body_coverage_zero_is_preserved(completed):
    r=op.read(completed/'coverage_report.json')
    assert r['unique_texts']==545 and r['versions']==2696 and r['texts_over4000_normalized']==127
    assert r['texts_truncated_by_inherited_scope']==127 and r['inherited_supported_spans']==r['whole_retained_text_same_rule_matches']==0
    assert r['native_action_texts']==8 and r['native_claim_texts']==10 and r['native_claim_spans']==11

@pytest.mark.parametrize('case',['conditional','negation','quotation','historical_recap','conflicting_actions','decimal_condition','foreign_and_current','negated_semantic_claim'])
def test_unsafe_native_challenges_are_reported_as_failures_not_success(completed,case):
    rows=op.read(completed/'native_function_challenges.json');r=next(x for x in rows if x['case']==case)
    assert not r['oracle_passed']
    assert not op.read(completed/'coverage_report.json')['native_semantic_admission']

def test_original_numeric_fields_not_converted_to_surprise(completed):
    r=op.read(completed/'structured_field_report.json')
    assert r['numeric_scalar_counts']['actual_value']==29 and r['numeric_scalar_counts']['previous_value']==16
    assert r['numeric_scalar_counts']['revised_previous_value']==1
    assert 'consensus_value' not in r['present_counts'] and not r['numeric_surprise_computed']

def test_native_claim_spans_and_all_outputs_remain_diagnostic(completed):
    cache={c['cache_key']:c for c in op.read(INPUTS/'extraction_cache.json')}
    for row in op.read(completed/'text_coverage.json'):
        assert not row['native_output_admitted']
        for c in row['native_semantic_claim_diagnostics']:
            assert not c['assertion_validated'] and not c['fx_direction_admitted']
            e=c['raw_evidence']
            if e['start'] is not None:assert cache[row['cache_key']]['retained_text'][e['start']:e['end']]==e['quote']
    assert not any(n in op.read(RECIPE)['inputs'] for n in ('forecasts.json','outcomes.json','scores.json'))

def test_process_death_resume_and_external_pin(tmp_path,completed):
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'bad')['status']=='review_required'
    r=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_text_coverage_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(r),'--test-crash-after','3'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,r);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
