import json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_issuer_audit_v2 as core
import macro_issuer_audit_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_ISSUER_AUDIT_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_issuer_audit/inputs')))
RECIPE=ROOT/'MACRO_ISSUER_AUDIT_OPERATOR_RECIPE.json'

@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_ISSUER_AUDIT_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('issuer')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

@pytest.mark.parametrize('url,expected',[
 ('https://www.example.gov/path',True),('https://EXAMPLE.GOV./path',True),('http://example.gov/path',True),
 ('https://example.gov.evil.org/path',False),('https://evilexample.gov/path',False),
 ('https://example.gov@evil.org/path',False),('https://evil.org@example.gov/path',False),
 ('file:///example.gov/path',False),('/relative',False),('https://[broken',False),('',False)])
def test_configured_host_comparison_is_boundary_aware(url,expected):
    assert core.host_allowed(url,['example.gov'])==expected

def test_statistical_and_calendar_roles_are_not_policy_release():
    m={'currencies':[{'currency':'USD','authority':'Fed','authority_id':'fed','release_source_ids':['policy'],
        'statistical_release_source_ids':['stats'],'calendar_source_ids':['calendar']}]}
    result=core.authority_links(m)
    assert result['policy'][0]['configured_role']=='policy_release' and result['stats'][0]['configured_role']=='statistical_release'
    assert result['calendar'][0]['configured_role']=='calendar'

def test_missing_config_and_collector_binding_flags_never_prove_issuer():
    b={'source_id':'x','content_sha256':'hash','currencies':['USD']}
    row={'version_id':'v','content_sha256':'hash','fields':{'source_id':'x','source_verified':True,'issuer_bound_policy_attachment':True,'source_url':'https://example.gov'}}
    p=core.provenance(row,b,{},{});assert not p['current_configuration_present'] and p['retained_issuer_binding_flag']
    assert not p['independent_issuer_attestation'] and not p['forecast_admission'] and p['original_extraction_ready_epoch'] is None
    row['fields']['source_id']='different'
    with pytest.raises(ValueError,match='source_projection_binding_mismatch'):core.provenance(row,b,{},{})

def test_actual_source_and_candidate_population(completed):
    r=op.read(completed/'issuer_audit_report.json')
    assert r['versions']==2696 and r['sources']==r['configured_sources']==131
    assert r['source_url_host_match_versions']==2587 and r['retained_issuer_binding_flag_versions']==18
    assert r['candidate_versions']==6 and r['action_candidate_versions']==2 and r['action_candidate_eligible_cutoff_rows']==0
    assert r['independent_issuer_attestations']==r['forecast_features_admitted']==0
    trace=op.read(completed/'candidate_exclusion_trace.json')
    assert {t['source_id'] for t in trace if t['action_candidate']}=={'tcmb_press','boe_news'}
    for t in trace:
        assert len(t['cutoff_trace'])==8 and not t['forecast_admission']
        if t['action_candidate']:
            assert all(not c['eligible_linguistic_context'] for c in t['cutoff_trace'])
            assert any(c['exclusion_or_absence']=='not_selected_version_at_cutoff' for c in t['cutoff_trace'])

def test_every_version_retains_closed_admission_and_provenance(completed):
    for p in op.read(completed/'version_source_provenance.json'):
        assert not p['configuration_historical_asof_proven'] and not p['policy_fact_admission'] and not p['forecast_admission']
    assert not any(n in op.read(RECIPE)['inputs'] for n in ['forecasts.json','outcomes.json','scores.json'])

def test_pin_and_process_death_resume(tmp_path,completed):
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'bad')['status']=='review_required'
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['news_sources_v1.json']+=b' '
    with pytest.raises(ValueError,match='issuer_predecessor_pin_mismatch'):core.build(blobs)
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_issuer_audit_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91
    result=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert result['status']=='completed_verified'
    assert all(op.sha(Path(result['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
