import copy,json,os,shutil,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import calibration_inspector_operator_v2 as op
from calibration_record_inspector_v2 import OriginalRecordReader,validate_query,ORIGINS
from calibration_inspector_runner_v2 import PROFILES,MODES,query,compact,required,identity_for
from contracts import fingerprint
from publication import RunPublisher
E=ROOT/'evidence/timed_20260922_022952';PF=Path(os.environ.get('FOREX_CALIBRATION_INSPECTOR_PATHS',str(E/'calibration_inspector_step/PATHS.json')));RECIPE=ROOT/'CALIBRATION_INSPECTOR_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def reader():return OriginalRecordReader(read(PF),read(RECIPE))
def call(action,runs,q=None):return op.operate(action,RECIPE,op.sha(RECIPE),read(PF),runs,q)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_CALIBRATION_INSPECTOR_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('inspector');result=call('verify' if supplied else 'run',runs);assert result['status']=='completed_verified',result;return Path(result['run_path'])
def sample(r,mode='expanding_prefix',h=60):
    profile=('legacy26',h,'frozen','ridge')
    for pair in r.recipe['universe']:
        q=query(profile,pair,mode,1);result=r.inspect(q)
        if result['record'] is not None:return q,result
    raise AssertionError('real_original_forecast_required')

def test_exact_registered_query_and_explicit_reveal_fields():
    r=reader();q,_=sample(r);validate_query(q,r.recipe['universe'])
    for key,value in [('asof_epoch',True),('horizon_minutes',True),('reveal_outcome',1),('instrument','UNKNOWN'),('method','winner'),('origin_epoch',ORIGINS[-1]+1),('asof_epoch',ORIGINS[-1]-1)]:
        bad={**q,key:value}
        with pytest.raises(ValueError):validate_query(bad,r.recipe['universe'])
    with pytest.raises(ValueError):validate_query({**q,'extra':1},r.recipe['universe'])
    with pytest.raises(ValueError):validate_query({**q,'outcome_asof_epoch':q['origin_epoch']},r.recipe['universe'])
    with pytest.raises(ValueError):validate_query({**q,'reveal_outcome':True},r.recipe['universe'])

def test_modeled_availability_boundary_hides_values_snapshot_and_lineage():
    r=reader();q,visible=sample(r);available=visible['record']['base_forecast_available_epoch']
    before=r.inspect({**q,'asof_epoch':available-1});at=r.inspect({**q,'asof_epoch':available})
    assert before['status']=='modeled_base_forecast_not_available'
    assert all(before[k] is None for k in ('record','snapshot','base_forecast','coverage','outcome'))
    assert at['record']==visible['record'] and at['snapshot']==visible['snapshot'] and at['actual_publication_qualified'] is False

def test_default_has_no_outcome_even_with_late_forecast_asof():
    r=reader();q,_=sample(r);result=r.inspect({**q,'asof_epoch':2000000000})
    assert result['outcome'] is None and result['outcome_status']=='not_requested'
    assert 'value' not in result['record'] and result['production_calibrator_available_epoch'] is None

def test_explicit_outcome_own_maturity_boundary_and_unchanged_forecast():
    r=reader();q,original=sample(r);maturity=q['origin_epoch']+q['horizon_minutes']*60
    early=r.inspect({**q,'reveal_outcome':True,'outcome_asof_epoch':maturity-1});at=r.inspect({**q,'reveal_outcome':True,'outcome_asof_epoch':maturity})
    assert early['outcome'] is None and early['outcome_status']=='not_yet_mature'
    assert at['outcome']['available_epoch']==maturity and at['outcome_status'] in ('observed','unavailable_original_label')
    assert early['record']==at['record']==original['record'] and early['snapshot']==at['snapshot']==original['snapshot']

def test_exact_original_record_snapshot_and_training_identity():
    r=reader();q,result=sample(r);data=r.payload('calibration','calibration_legacy26_60_frozen.json')
    assert result['record'] in data['rows'] and {'mode':q['calibration_mode'],'snapshot':result['snapshot']} in data['snapshots']
    s=result['snapshot'];assert s['calibrator_id']==fingerprint({k:v for k,v in s.items() if k!='calibrator_id'})
    assert s['cutoff_epoch']<=q['origin_epoch'] and result['record']['base_forecast_id'] not in s['training_forecast_ids']

def test_five_day_insufficient_support_visible_without_invented_interval():
    r=reader();q,result=sample(r,h=7200)
    assert result['status']=='insufficient_distinct_support' and result['snapshot']['parameters'] is None
    assert result['record']['adjusted_mean_bps'] is None and result['record']['empirical_lower_bps'] is None

def test_before_frozen_prefix_keeps_explicit_phase_without_future_snapshot():
    r=reader();q,_=sample(r,mode='frozen_prefix');q.update(origin_epoch=ORIGINS[0],asof_epoch=ORIGINS[0]+112)
    result=r.inspect(q);assert result['status']=='calibration_phase_not_started' and result['snapshot'] is None
    assert result['record']['calibrator_id'] is None and result['record']['adjusted_mean_bps'] is None

def test_unavailable_original_base_is_not_replaced():
    r=reader();q=query(('full228_cost2',60,'adaptive','ridge'),r.recipe['universe'][0],'expanding_prefix',1);q.update(origin_epoch=ORIGINS[0],asof_epoch=ORIGINS[0]+112)
    result=r.inspect(q);assert result['status'].startswith('base_') and result['coverage']['reason']!='eligible'
    assert result['record'] is None and result['base_forecast'] is None and result['snapshot'] is None

def test_missing_original_outcome_remains_null():
    r=reader()
    for pair in r.recipe['universe']:
        for o in r.payload('technical','pair_'+pair+'.json')['outcomes']:
            if o['value'] is None and int(o['record_id'].split(':')[1]) in ORIGINS:
                h=int(o['target_id'].removeprefix('technical_endpoint_midpoint_elapsed_').removesuffix('m'));origin=int(o['record_id'].split(':')[1]);q=query(('legacy26',h,'frozen','ridge'),pair,'expanding_prefix',2);q.update(origin_epoch=origin,asof_epoch=origin+112,outcome_asof_epoch=o['available_epoch']);result=r.inspect(q)
                assert result['outcome_status']=='unavailable_original_label' and result['outcome']['value'] is None;return
    raise AssertionError('expected_retained_missing_labels')

def test_returned_json_mutation_cannot_change_reader_cache():
    r=reader();q,result=sample(r);original=copy.deepcopy(result);result['record']['base_prediction_bps']=1e100;result['snapshot']['training_forecast_ids'].clear()
    assert r.inspect(q)==original

def test_snapshot_tamper_refused_before_visible_return():
    r=reader();q,result=sample(r);data=r.payload('calibration','calibration_legacy26_60_frozen.json')
    item=next(x for x in data['snapshots'] if x['mode']==q['calibration_mode'] and x['snapshot']['calibrator_id']==result['snapshot']['calibrator_id']);item['snapshot']['support']['rows']+=1
    with pytest.raises(ValueError,match='identity_mismatch'):r.inspect(q)

def test_consumed_bytes_drift_refused(tmp_path):
    r=reader();paths=read(PF);source=Path(paths['calibration']);name='coverage_legacy26_60_frozen.json';(tmp_path/name).write_bytes((source/name).read_bytes()+b' ');paths['calibration']=str(tmp_path);bad=OriginalRecordReader(paths,read(RECIPE))
    with pytest.raises(ValueError,match='consumed_bytes_changed'):bad.inspect(query(PROFILES[0],r.recipe['universe'][0],MODES[0],1))

def test_no_fit_import_or_call_during_all_smoke_inspection(completed,monkeypatch):
    import prequential_residual_calibration_v2 as core
    def forbidden(*a,**k):raise AssertionError('no_calibration_fit_during_inspection')
    monkeypatch.setattr(core,'fit_snapshot',forbidden);r=reader();count=0
    for n,profile in enumerate(PROFILES):
        for mode in MODES:
            expected=read(completed/('inspection_'+str(n)+'_'+mode+'.json'));actual=[compact(r.inspect(query(profile,pair,mode,state))) for pair in r.recipe['universe'] for state in range(3)]
            assert actual==expected;count+=len(actual)
    assert count==1224 and 'sklearn' not in sys.modules

def test_report_inventory_and_verify_idempotence(completed):
    before={p.name:op.sha(p) for p in completed.iterdir() if p.is_file()};result=call('run',completed.parent)
    assert result['status']=='completed_verified' and before=={p.name:op.sha(p) for p in completed.iterdir() if p.is_file()}
    assert set(required())=={x['path'] for x in read(completed/'COMPLETION_MANIFEST.json')['payloads']}
    report=read(completed/'run_report.json');assert report['inspection_cases']==1224 and report['base_models_fitted']==report['calibrators_fitted']==0

def test_actual_operator_inspect_requires_completed_run_and_query(completed,tmp_path):
    q,_=sample(reader());assert call('inspect',tmp_path,q)['status']=='review_required'
    result=call('inspect',completed.parent,q);assert result['inspection']==reader().inspect(q)
    assert call('inspect',completed.parent,None)['status']=='review_required'

def test_source_and_recipe_drift_guard_before_import(tmp_path,monkeypatch):
    assert op.operate('status',RECIPE,'0'*64,read(PF),tmp_path)['status']=='review_required'
    first=op.SOURCES[0];(tmp_path/first).write_bytes((ROOT/first).read_bytes()+b'\n');monkeypatch.setattr(op,'ROOT',tmp_path)
    result=call('status',tmp_path/'runs');assert 'source_drift_before_import' in result['reason']
    monkeypatch.setattr(op,'ROOT',ROOT);version=op.importlib.metadata.version
    monkeypatch.setattr(op.importlib.metadata,'version',lambda n:'0.0.drift' if n=='numpy' else version(n))
    assert 'input_environment_drift' in call('status',tmp_path/'runs')['reason']

def test_partial_wrong_identity_and_false_completion_refused(tmp_path):
    recipe=read(RECIPE);root=tmp_path/recipe['run_id'];root.mkdir();(root/'RUN_IDENTITY.json').write_text('{}',encoding='utf-8')
    assert call('resume',tmp_path)['status']=='review_required'
    other=tmp_path/'false';dest=other/recipe['run_id'];dest.mkdir(parents=True);(dest/'RUN_IDENTITY.json').write_text(json.dumps(identity_for(recipe)),encoding='utf-8');(dest/'COMPLETION_MANIFEST.json').write_text('{}',encoding='utf-8')
    assert call('verify',other)['status']=='review_required'

def test_one_writer_refuses_foreign_live_owner(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('resume',tmp_path)['status']=='review_required'
    finally:p.release()

@pytest.mark.parametrize('boundary',[1,0])
def test_real_process_crash_and_exact_resume(boundary,tmp_path,completed):
    cmd=[sys.executable,'-I','-B',str(ROOT/'calibration_inspector_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PF),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)];p=subprocess.run(cmd,capture_output=True,text=True,timeout=300);assert p.returncode==92,p.stdout+p.stderr
    assert call('status',tmp_path)['status']=='resumable';result=call('resume',tmp_path);assert result['status']=='completed_verified',result
    root=Path(result['run_path']);assert {n:op.sha(root/n) for n in required()}=={n:op.sha(completed/n) for n in required()}

def test_stdlib_only_preflight_in_isolated_process():
    code='import sys,json;from pathlib import Path;sys.path.insert(0,sys.argv[1]);import calibration_inspector_operator_v2 as op;r=op.preflight(Path(sys.argv[2]),sys.argv[3],op.read(Path(sys.argv[4])));assert "numpy" not in sys.modules and "sklearn" not in sys.modules;print(r["schema_version"])'
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),op.sha(RECIPE),str(PF)],capture_output=True,text=True,timeout=60);assert p.returncode==0,p.stdout+p.stderr
