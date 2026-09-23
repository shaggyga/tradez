import copy,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_numeric_disposition_v2 as core
import macro_numeric_disposition_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_NUMERIC_DISPOSITION_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_numeric_disposition/inputs')))
RECIPE=ROOT/'MACRO_NUMERIC_DISPOSITION_OPERATOR_RECIPE.json'
@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_NUMERIC_DISPOSITION_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('disposition')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def test_exact_next_population_and_all_sources(completed):
    r=op.read(completed/'numeric_disposition_report.json');assert (r['versions'],r['sources'],r['unit_binding_target_versions'],r['component_evidence_cells'])==(31,18,9,9)
    q=op.read(completed/'numeric_next_work_package.json');assert len(q['target_version_ids'])==len(set(q['target_version_ids']))==9
    assert q['target_sources']==['bls_major_timeseries_batch_v1','census_economic_indicators','statcan_daily_releases']

def test_component_only_scope_does_not_inherit_headline_gap(completed):
    rows=[r for r in op.read(completed/'numeric_qualification_disposition.json') if r['source_id']=='ons_published_releases' and r['headline_unit_status']=='not_applicable_component_only']
    assert len(rows)==2 and all(r['headline_unit_status']=='not_applicable_component_only' and not r['eligible_offline_tasks'] for r in rows)
    assert {r['component_status'] for r in rows}=={'rejected_document_scope','repaired_scoped_evidence'}

def test_missing_consensus_never_blocks_text_or_becomes_surprise(completed):
    for r in op.read(completed/'numeric_qualification_disposition.json'):
        assert not r['text_event_path_blocked_by_missing_consensus'] and not r['forecast_admission'] and not r['original_prior_vintage_verified'] and r['surprise_status']=='disabled_no_verified_archived_expectations'

def test_explicit_unit_does_not_remove_other_provenance_gates():
    row=op.read(INPUTS/'numeric_evidence_audit.json')[0];r=core.disposition(row)
    assert 'recover_source_native_unit_frequency_binding' not in r['eligible_offline_tasks'] and not r['independent_issuer_binding_verified']
    row['numeric_extraction_contract_id']='';row['reference']['period']=None;r=core.disposition(row)
    assert set(r['eligible_offline_tasks'])=={'recover_missing_extraction_contract_provenance','recover_explicit_reference_period_evidence'}

def test_input_drift_rejected():
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['numeric_evidence_audit.json']+=b' '
    with pytest.raises(ValueError,match='disposition_input_pin_mismatch'):core.build(blobs)

def test_crash_resume_exact_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_numeric_disposition_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91;r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified'
    assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
