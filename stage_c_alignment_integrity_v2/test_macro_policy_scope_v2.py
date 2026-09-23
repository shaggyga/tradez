import json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_policy_scope_v2 as core
import macro_policy_scope_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_POLICY_SCOPE_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_policy_scope/inputs')))
RECIPE=ROOT/'MACRO_POLICY_SCOPE_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def native():
    raw=(INPUTS/'recovered_policy_functions.py').read_bytes()
    return core.load_native(raw,op.read(INPUTS/'RECOVERED_FUNCTIONS.json')),core.recovered_patterns(raw)

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_POLICY_SCOPE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('scope')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

@pytest.mark.parametrize('text,expected',[
 ('The Committee lowered the policy rate.','cut'),
 ('The Committee raised the policy rate.','hike'),
 ('The Committee kept the policy rate unchanged.','hold'),
 ("At today's meeting the policy board lowered the policy rate.",'cut'),
 ('Last year the Committee lowered the policy rate. Today the Committee kept the policy rate unchanged.','hold'),
 ('The Committee lowered the policy rate and raised the bank rate.',None),
 ('If inflation drops to 2.5 percent; the Committee lowered the policy rate.',None),
 ('If e.g. inflation slows, the Committee lowered the policy rate.',None),
 ("The Committee didn't lower the policy rate.",None),
 ("'The Committee lowered the policy rate'",None),
 ('“The Committee lowered the policy rate”',None),
 ('Did the Committee lower the policy rate?',None),
 ('According to analysts the Committee lowered the policy rate.',None),
 ('Analysts said the Committee lowered the policy rate.',None),
 ('Last Monday the Committee lowered the policy rate.',None),
 ('The Federal Reserve Committee lowered the policy rate. The ECB Governing Council kept the policy rate unchanged.',None),
 ('A Monetáris Tanács az alapkamatot nem változtatta.','hold'),
 ('A Monetáris Tanács az alapkamatot nem csökkentette.',None),
])
def test_assertion_scoping_adversarial(text,expected,native):
    assert core.scoped(text,*native)['action_candidate']==expected

def test_original_oracles_and_actual_population(completed):
    cases=op.read(completed/'guard_regression.json');assert len(cases)==12 and all(c['guarded_oracle_passed'] for c in cases)
    r=op.read(completed/'scoping_report.json')
    assert r['original_failed_cases_repaired']==8 and r['unique_texts']==545 and r['versions']==2696
    assert r['guarded_action_candidate_texts']==2 and r['asserted_claim_spans']==9
    assert r['native_pattern_sha256']=='ea254f25516ddbe9b3600702e9d2df509859f887f210f10ea15bc9e9c8e218fa'

def test_raw_span_bounds_and_no_fx_claim_sign(completed):
    texts={c['cache_key']:c['retained_text'] for c in op.read(INPUTS/'extraction_cache.json')}
    for r in op.read(completed/'scoped_text_cache.json'):
        assert not r['forecast_admission'] and not r['policy_fact_admission'] and not r['issuer_identity_verified']
        for e in r['action_evidence']+r['claim_evidence']:
            assert 'rate_currency_sign' not in e
            if e['start'] is not None:
                assert e['context_start']<=e['start']<e['end']<=e['context_end']
                assert texts[r['cache_key']][e['start']:e['end']]==e['quote']
    for r in op.read(completed/'version_scope_inventory.json'):
        assert not r['forecast_admission'] and r['original_extraction_ready_epoch'] is None
        if not r['detail_context_eligible']:assert r['action_candidate'] is None and not r['asserted_claim_dimensions']

def test_negated_claim_and_full_body_offsets(native):
    r=core.scoped('Inflation did not increase.',*native)
    assert all(c['guard_reasons'] for c in r['claim_evidence'])
    text='Background. '*500+'The Committee\n lowered the policy rate.'
    r=core.scoped(text,*native);assert r['action_candidate']=='cut'
    assert all(e['start']>4000 and text[e['start']:e['end']]==e['quote'] for e in r['action_evidence'])

def test_ast_assignment_cannot_execute_call(native):
    raw=(INPUTS/'recovered_policy_functions.py').read_bytes()
    with pytest.raises(ValueError,match='native_pattern_assignment_shape'):
        core.recovered_patterns(raw.replace(b'subject = (',b'subject = str('))

def test_source_pin_and_crash_resume(tmp_path,completed):
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'bad')['status']=='review_required'
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_policy_scope_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','3'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
