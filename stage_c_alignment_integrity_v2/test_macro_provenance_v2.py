import copy,json,os,shutil,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_provenance_v2 as core
import macro_provenance_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_PROVENANCE_INPUTS',str(ROOT/'evidence/four_next_20260923_040615/provenance/inputs')))
RECIPE=ROOT/'MACRO_PROVENANCE_OPERATOR_RECIPE.json'
def blobs():return {n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS}
@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_PROVENANCE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('provenance')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])
def fields():return {'headline':'Media Release - Australian economy grew 0.4% in the June quarter','reference_period':'June quarter'}
def test_actual_three_records_preserved_and_gaps_not_fabricated(completed):
    rows=op.read(completed/'provenance_bindings.json');assert len(rows)==3
    original={v['version_id']:json.loads(v['content_json']) for v in op.read(INPUTS/'target_versions.json')}
    for r in rows:
        assert r['original_actual']==original[r['version_id']]['actual_value']
        assert not r['forecast_admission'] and r['inferred_clock'] is r['inferred_contract'] is None
        assert core.fingerprint({k:v for k,v in r.items() if k!='binding_sha256'})==r['binding_sha256']
        if r['source_id']=='abs_latest_releases':
            assert r['reference_binding']['status']=='quarter_without_year' and r['reference_binding']['quarter']==2
            assert r['native_reproduction']['actual']=='0.4' and r['reference_binding']['period'] is None
        else:
            assert r['retained_numeric_extraction_contract_id'] is r['retained_numeric_causal_known_utc'] is None
            assert r['parser_lineage']['missing_output_fields']==['numeric_extraction_contract_id','numeric_causal_known_utc']
            assert not r['parser_lineage']['historical_deployment_proven']
@pytest.mark.parametrize('extra',[{'published_utc':'2026-06-30'},{'source_url':'https://example.test/2026/gdp'},{'summary':'Copyright 2026. Published 2026.'},{'numeric_parser_activated_utc':'2026-08-19'}])
def test_unrelated_year_is_never_reference_year(extra):
    r=core.quarter_reference({**fields(),**extra});assert r['year'] is None and r['period'] is None
@pytest.mark.parametrize('quarter,number',[('March',1),('June',2),('September',3),('December',4)])
def test_explicit_bound_reference_supported(quarter,number):
    f={'reference_period':quarter+' quarter 2025','headline':'irrelevant'};r=core.quarter_reference(f)
    assert r['period']==f'2025-Q{number}'
    for e in r['evidence']:assert f[e['field']][e['start']:e['end']]==e['quote']
@pytest.mark.parametrize('ref',['June quarter 2025','March quarter 2026'])
def test_conflicting_year_or_quarter_abstains(ref):
    f=fields();f['headline']+=' 2026';f['reference_period']=ref
    assert core.quarter_reference(f)['status']=='conflicting_reference'
@pytest.mark.parametrize('ref',['June quarter 26','June quarter 2026 or 2025','February quarter','2026 June quarter'])
def test_unsupported_grammar_abstains(ref):assert core.quarter_reference({'reference_period':ref})['status']=='unsupported_reference'
def test_real_consumer_preserves_events(completed):
    for old,new in zip(op.read(INPUTS/'numeric_source_asof.json'),op.read(completed/'provenance_source_asof.json')):
        assert old['events']==[e['original_numeric_event'] for e in new['events']]
        assert all(not e['runtime_feature_cells'] and e['adapter_available_epoch'] is None for e in new['events'])
def test_selected_identity_time_and_no_fallback(completed):
    b=op.read(completed/'provenance_bindings.json')[0];index={b['version_id']:b};m={'numeric_cache_key':'key','content_sha256':b['content_sha256'],'source_id':b['source_id']}
    event={'status':'retained_numeric_observation_ready','selected_numeric_version_ids':[b['version_id']],'source_numeric_available_epoch':10,'numeric_cache_key':'key','source_id':b['source_id']}
    assert core.join_provenance(event,index,[m],10)['selected_provenance_bindings']==[b['binding_sha256']]
    for patch in [{'status':'numeric_clock_not_ready'},{'source_numeric_available_epoch':11},{'source_numeric_available_epoch':None},{'source_numeric_available_epoch':float('nan')},{'selected_numeric_version_ids':['unknown']},{'selected_numeric_version_ids':[b['version_id'],'unknown']},{'numeric_cache_key':'other'},{'source_id':'other'}]:
        assert not core.join_provenance({**event,**patch},index,[m],10)['selected_provenance_bindings']
    assert not core.join_provenance(event,index,[{**m,'content_sha256':'wrong'}],10)['selected_provenance_bindings']
def test_pin_scope_and_false_completion_refused(tmp_path,completed):
    bad=blobs();bad['target_versions.json']+=b' '
    with pytest.raises(ValueError,match='provenance_input_pin_mismatch'):core.build(bad)
    for action in ['verify','unsupported']:assert op.operate(action,RECIPE,op.sha(RECIPE),INPUTS,tmp_path/'runs')['status']=='review_required'
    assert op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'runs')['status']=='review_required'
    copied=tmp_path/'inputs';shutil.copytree(INPUTS,copied);(copied/'target_versions.json').write_text('[]')
    assert op.operate('run',RECIPE,op.sha(RECIPE),copied,tmp_path/'runs')['status']=='review_required'
    poisoned=tmp_path/'poisoned';shutil.copytree(completed,poisoned/op.read(RECIPE)['run_id']);(poisoned/op.read(RECIPE)['run_id']/'provenance_report.json').write_text('{}')
    assert op.operate('verify',RECIPE,op.sha(RECIPE),INPUTS,poisoned)['status']=='review_required'
def test_crash_resume_exact_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_provenance_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91;r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified'
    assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
