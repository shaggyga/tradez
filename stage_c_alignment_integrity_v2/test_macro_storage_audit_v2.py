import os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_storage_audit_v2 as core
import macro_storage_audit_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_STORAGE_AUDIT_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_storage_audit/inputs')))
RECIPE=ROOT/'MACRO_STORAGE_AUDIT_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_STORAGE_AUDIT_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('storage')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def test_actual_action_losses_rejected_by_existing_projection_guard(completed):
    r=op.read(completed/'storage_audit_report.json')
    assert r['retained_versions']==15 and r['replacement_pairs']==9
    assert r['enriched_to_non_enriched_pairs']==r['original_guard_rejected_downgrades']==5
    assert r['action_loss_pairs_rejected_by_original_guard']==2 and r['original_guard_challenges_passed']==5
    assert not r['live_collector_executed'] and not r['live_canonical_projection_inspected'] and not r['source_database_writes']

def test_exact_source_order_before_projection_guard(completed):
    r=op.read(completed/'source_order_evidence.json')
    assert r['record_raw_observation_line']==17080 and r['select_projection_observation_line']==17092
    assert r['record_raw_observation_line']<r['select_projection_observation_line']<r['bind_projection_line']

def test_wrong_source_order_cannot_be_reported_as_supported():
    raw=b'def f():\n select_source_observation()\n record_source_observation()\n bind_active_source_version()\n'
    with pytest.raises(ValueError,match='unexpected_collector_observation_selection_order'):core.source_order(raw,1)

def test_exact_original_guard_cases_and_scope(completed):
    cases=op.read(completed/'original_guard_challenges.json');assert len(cases)==5 and all(c['passed'] for c in cases)
    for c in op.read(completed/'frozen_projection_guard_comparison.json'):
        if c['original_detail_guard_exercised']:assert c['original_projection_accepts_downgrade'] is False
        else:assert c['original_projection_accepts_downgrade'] is None
        assert not c['forecast_admission'] and not c['historical_live_projection_reconstructed']

def test_source_pin_before_recovered_code_execution():
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['source_ledger.py']+=b'\nraise RuntimeError("must not execute")'
    with pytest.raises(ValueError,match='storage_predecessor_pin_mismatch'):core.build(blobs)

def test_process_death_resume_matches(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_storage_audit_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','1'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
