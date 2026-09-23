import copy,json,os,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_component_state_v2 as core
import macro_component_state_operator_v2 as op
INPUTS=Path(os.environ.get('FOREX_MACRO_COMPONENT_STATE_INPUTS',str(ROOT/'evidence/timed_20260922_194458/macro_component_state/inputs')))
RECIPE=ROOT/'MACRO_COMPONENT_STATE_OPERATOR_RECIPE.json'
@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_COMPONENT_STATE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('component-state')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def fixture():
    index=core.evidence_index(op.read(INPUTS/'component_repair_evidence.json'));material={r['numeric_cache_key']:r for r in op.read(INPUTS/'numeric_material_cache.json')}
    e=next(e for s in op.read(INPUTS/'numeric_source_asof.json') for e in s['events'] if any(v in index and index[v]['adapter']['cells'] for v in e['selected_numeric_version_ids']) and e['status']=='retained_numeric_observation_ready')
    return e,index,material

def test_actual_integrated_population_and_no_activation(completed):
    r=op.read(completed/'component_state_report.json');assert (r['cutoffs'],r['currency_rows'],r['pair_rows'],r['reconstructed_cell_event_cutoff_rows'])==(8,168,544,48)
    assert r['event_status_counts']['component_adapter_abstained']==6 and r['runtime_feature_cells']==0
    original=op.read(INPUTS/'numeric_source_asof.json');rebuilt=op.read(completed/'repaired_component_source_asof.json')
    for old,new in zip(original,rebuilt):
        assert [e['original_numeric_event'] for e in new['events']]==old['events']
        assert all(e['runtime_feature_cells']==[] and e['adapter_available_epoch'] is None and not e['forecast_admission'] for e in new['events'])

@pytest.mark.parametrize('status',['numeric_clock_not_ready','missing_or_invalid_numeric_clock','ambiguous_numeric_material','stale_numeric_context'])
def test_original_readiness_blocks_repair_without_fallback(status):
    e,i,m=fixture();e['status']=status;r=core.join_event(e,i,m,2e9);assert r['status']=='blocked_by_original_numeric_readiness' and not r['reconstructed_component_cells']

def test_future_readiness_rejected():
    e,i,m=fixture()
    with pytest.raises(ValueError,match='component_source_readiness_after_cutoff'):core.join_event(e,i,m,e['source_numeric_available_epoch']-1)

def test_unknown_new_selected_version_cannot_reuse_old_repair():
    e,i,m=fixture();e['selected_numeric_version_ids']=['unknown_new_version'];r=core.join_event(e,i,m,2e9)
    assert r['status']=='no_repaired_component_version' and not r['reconstructed_component_cells']

def test_missing_selected_version_abstains():
    e,i,m=fixture();e['selected_numeric_version_ids'].append('missing');assert core.join_event(e,i,m,2e9)['status']=='incomplete_selected_version_evidence'

def test_wrong_content_cannot_join_even_same_source():
    e,i,m=fixture();m[e['numeric_cache_key']]['content_sha256']='wrong'
    with pytest.raises(ValueError,match='component_selected_content_mismatch'):core.join_event(e,i,m,2e9)

def test_conflicting_selected_evidence_abstains():
    e,i,m=fixture();vid=e['selected_numeric_version_ids'][0];new=copy.deepcopy(i[vid]);new['adapter']['cells'][0]['actual']='99';i['second']=new;e['selected_numeric_version_ids'].append('second')
    assert core.join_event(e,i,m,2e9)['status']=='conflicting_selected_component_evidence'

def test_cell_hash_and_parent_payload_drift_rejected():
    rows=op.read(INPUTS/'component_repair_evidence.json');next(r for r in rows if r['adapter']['cells'])['adapter']['cells'][0]['actual']='99'
    with pytest.raises(ValueError,match='component_cell_hash_mismatch'):core.evidence_index(rows)
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};blobs['numeric_source_asof.json']+=b' '
    with pytest.raises(ValueError,match='component_parent_payload_binding_mismatch'):core.validate_parent(blobs,'numeric_parent_manifest.json',['numeric_source_asof.json'])

def test_no_activation_receipt_backdating():
    blobs={n:(INPUTS/n).read_bytes() for n in op.BASE_INPUTS};p=json.loads(blobs['COMPONENT_STATE_PLAN.json']);p['adapter_activation_receipt']={'utc':'2026-09-01T00:00:00Z','verified':True};blobs['COMPONENT_STATE_PLAN.json']=json.dumps(p).encode()
    with pytest.raises(ValueError,match='activation_receipt_schema_not_implemented'):core.build(blobs)

def test_shared_currency_pair_hashes_and_no_arithmetic(completed):
    index={}
    for s in op.read(completed/'repaired_component_currency_states.json'):
        for row in s['states']:
            h=row.pop('state_sha256');assert h==core.fingerprint(row);index[h]=row
    for p in op.read(completed/'repaired_component_pair_views.json'):
        a,b=index[p['base_state_sha256']],index[p['quote_state_sha256']]
        assert p['pair']==a['currency']+'_'+b['currency'] and a['cutoff_epoch']==b['cutoff_epoch']==p['cutoff_epoch']
        assert p['base_minus_quote_numeric_value'] is None and not p['shared_events_are_independent_votes'] and not p['forecast_admission']

def test_crash_resume_exact_outputs(tmp_path,completed):
    runs=tmp_path/'runs';p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_component_state_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(runs),'--test-crash-after','2'],capture_output=True,text=True,timeout=120)
    assert p.returncode==91;r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,runs);assert r['status']=='completed_verified'
    assert all(op.sha(Path(r['run_path'])/n)==op.sha(completed/n) for n in op.REQUIRED)
