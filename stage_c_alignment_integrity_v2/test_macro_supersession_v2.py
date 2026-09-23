import os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_supersession_v2 as core
import macro_supersession_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_SUPERSESSION_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_supersession/inputs')))
RECIPE=ROOT/'MACRO_SUPERSESSION_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_SUPERSESSION_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('supersession')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

@pytest.mark.parametrize('a,b,relation',[
 ('abc','abc','identical_text'),('abc','','replacement_missing_text'),('abcdef','abc','replacement_exact_prefix_shortening'),
 ('abc','abcdef','replacement_exact_prefix_extension'),('abcdef','bcd','replacement_exact_substring_shortening'),
 ('bcd','abcdef','replacement_contains_prior_text'),('abcd','xyzw','different_text_unresolved_revision_or_extraction')])
def test_exact_text_change_classification_does_not_claim_revision(a,b,relation):assert core.text_relation(a,b)==relation

def test_boundaries_respect_both_clocks_and_cutoff():
    observations=[{'clock_status':'valid_attested_observation','known_epoch':10,'available_epoch':20},
        {'clock_status':'valid_attested_observation','known_epoch':40,'available_epoch':30},
        {'clock_status':'unproven_observation','known_epoch':5,'available_epoch':5},
        {'clock_status':'valid_attested_observation','known_epoch':None,'available_epoch':5}]
    assert core.boundaries([{'observations':observations}],35)==[20]

def test_actual_intraday_action_visibility_is_not_midnight_support(completed):
    r=op.read(completed/'supersession_report.json')
    assert (r['events'],r['versions'],r['texts'])==(6,15,10)
    assert r['retained_boundary_states']==23 and r['selected_version_changes']==9
    assert r['action_candidates_with_intraday_eligible_intervals']==2 and r['action_candidates_with_midnight_eligibility']==0
    assert r['replacement_relations']['replacement_missing_text']==2 and r['same_detail_hash_changes']==0
    rows=op.read(completed/'candidate_visibility_intervals.json')
    for row in rows:
        assert not row['forecast_admission'] and not row['historical_extraction_ready_proven']
        if row['action_candidate']:assert row['eligible_interval_count']>0 and row['sampled_midnight_eligible_rows']==0

def test_intervals_and_replacement_lineage_are_consistent(completed):
    histories=op.read(completed/'event_boundary_histories.json');versions={v['version_id']:v for v in op.read(completed/'event_version_inventory.json')}
    for h in histories:
        rows=h['boundary_states']
        for i,r in enumerate(rows):
            assert r['start_epoch']<=r['end_epoch_exclusive'] and not r['forecast_admission']
            if i+1<len(rows):assert r['end_epoch_exclusive']==rows[i+1]['start_epoch']
            assert all(versions[v]['event_id']==h['event_id'] for v in r['selected_version_ids'])
    diffs=op.read(completed/'replacement_text_diffs.json')
    for d in diffs:
        assert not d['stale_fallback_applied'] and not d['verified_publisher_revision']
        assert all(versions[v]['event_id']==d['event_id'] for v in d['before_version_ids']+d['after_version_ids'])
    losses=[d for d in diffs if d['before_action_candidate']]
    assert sorted((d['before_characters'],d['after_characters']) for d in losses)==[(22150,0),(49375,100)]

def test_pin_and_crash_resume(tmp_path,completed):
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['candidate_trace.json']+=b' '
    with pytest.raises(ValueError,match='supersession_predecessor_pin_mismatch'):core.build(blobs)
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_supersession_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','3'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
