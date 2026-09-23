import json,os,shutil,subprocess,sys
from decimal import Decimal,localcontext
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import rich_dependence_operator_v2 as op
from publication import RunPublisher
from campaign_inspector_v2 import CampaignReader
E=ROOT/'evidence/timed_20260922_022952'
PATHS_FILE=Path(os.environ.get('FOREX_DEPENDENCE_PATHS',str(E/'rich_dependence_step/PATHS.json')))
RECIPE=ROOT/'RICH_DEPENDENCE_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),read(PATHS_FILE),runs)
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_DEPENDENCE_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('paired-report-baseline');r=call('verify' if supplied else 'run',runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])
@pytest.fixture(scope='session')
def reader():return CampaignReader(read(PATHS_FILE),read(RECIPE)['dependencies'])

def test_all_matched_comparisons_origins_and_attempts(completed):
    r=read(completed/'run_report.json');assert r['paired_comparisons']==336 and r['origin_panel_rows']==5616 and r['block_sensitivity_rows']==1008
    assert r['assessment_origin_counts']==[12,16,18,19,20] and r['assessment_date_counts']==[3,4,5] and r['models_fitted']==0 and r['selected_model'] is None
    assert r['distinct_actual_origin_grids']==6
    assert r['effective_sample_size'] is None and r['independent_observations_claimed'] is False
    ledger=read(completed/'attempt_ledger.json');assert len(ledger['attempts'])==112 and ledger['new_fixed_model_fits']==84 and ledger['reused_baseline_fits']==28
    assert sum(r['universe_currency_incidence_not_exposure'].values())==136

def test_real_first_origin_decimal_paired_error_oracle(completed,reader):
    panel=next(x for x in read(completed/'paired_origin_panels.json') if x['group']=='full228_cost2' and x['method']=='ridge' and x['control']=='zero' and x['procedure']=='frozen' and x['target_id'].endswith('_15m'))
    rows=[x for x in reader.read('family','forecasts_full228_cost2_15_frozen.json') if x['method']=='ridge' and x['forecast']['decision_epoch']==panel['origin_epoch']]
    labels={}
    for pair in read(RECIPE)['configuration']['universe']:
        labels.update({(o['record_id'],o['target_id']):o for o in reader.read('rich','outcomes_'+pair+'.json')['outcomes']})
    absolute=[];squared=[]
    with localcontext() as ctx:
        ctx.prec=60
        for row in rows:
            y=labels[row['record_id'],panel['target_id']]['value']
            if y is None:continue
            a=Decimal(str(row['forecast']['prediction']));y=Decimal(str(y));absolute.append(abs(a-y)-abs(y));squared.append((a-y)**2-y**2)
        assert float(sum(absolute))==pytest.approx(panel['absolute_error_delta_sum'],rel=1e-12,abs=1e-8)
        assert float(sum(squared))==pytest.approx(panel['squared_error_delta_sum'],rel=1e-12,abs=1e-8)
        assert len(absolute)==panel['rows']

def test_joint_block_indices_and_short_window_limit(completed):
    rows=read(completed/'block_sensitivity.json')
    for length in (4,8):
        selected=[x for x in rows if x['block_length_origins']==length]
        assert len(selected)==336 and len({x['origin_grid_sha256'] for x in selected})==6
        assert len({x['joint_draw_identity'] for x in selected})==6
        for grid in {x['origin_grid_sha256'] for x in selected}:
            same_grid=[x for x in selected if x['origin_grid_sha256']==grid]
            assert len({x['indices_sha256'] for x in same_grid})==1
            assert len({x['joint_draw_identity'] for x in same_grid})==1
        assert all(x['origin_spacing_seconds']==[21600] and x['observed_block_span_seconds']==[(length-1)*21600] for x in selected)
        assert all(x['row_iid_assumed'] is False and x['effective_sample_size'] is None for x in selected)
    whole=[x for x in rows if x['block_length_origins']==20];assert len(whole)==336
    assert all(x['status']=='insufficient_distinct_time_blocks' and x['interval'] is None and x['replicates']==0 for x in whole)

def test_original_label_intervals_and_support(completed,reader):
    scores=reader.read('family','scores.json')
    for s in read(completed/'paired_comparison_scope.json'):
        expected=next(x for x in scores if (x['group'],x['target_id'],x['procedure'],x['method'])==(s['group'],s['target_id'],s['procedure'],s['method']))
        assert s['paired_support_sha256']==expected['support_sha256'] and s['raw_paired_rows']==expected['rows']
        assert s['forecast_rows']==s['raw_paired_rows']+sum(s['excluded_outcome_rows'].values())
        assert sorted(set(s['origin_epochs'])|set(s['origins_without_mature_rows']))==s['forecast_origin_epochs']
        minutes=int(s['target_id'].rsplit('_',1)[1][:-1]);assert s['maximum_label_span_seconds']==minutes*60

def test_source_drift_refused_before_import(tmp_path,monkeypatch):
    for n in op.SOURCES:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/'rich_dependence_report_v2.py').write_text("raise RuntimeError('MUST_NOT_IMPORT')")
    monkeypatch.setattr(op,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='source_drift_before_import'):op.preflight(RECIPE,op.sha(RECIPE),read(PATHS_FILE))

def test_reader_consumed_byte_guard_without_model_loading(tmp_path):
    # Reuse the existing reader's consumed-byte guard independently of preflight.
    r=CampaignReader(read(PATHS_FILE),read(RECIPE)['dependencies']);r.paths['family']=tmp_path
    (tmp_path/'run_report.json').write_text('{}')
    with pytest.raises(ValueError,match='consumed_bytes_changed'):r.read('family','run_report.json')

def test_one_writer_and_false_completion(tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],op.identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()
    assert call('verify',tmp_path)['status']=='review_required'

def test_report_runs_with_estimator_imports_denied(completed,tmp_path):
    code="""import sys,json,builtins
from pathlib import Path
sys.path.insert(0,sys.argv[1])
original=builtins.__import__
def guarded(name,*args,**kwargs):
 if name.split('.')[0] in {'sklearn','joblib'}:raise AssertionError('estimator_import_forbidden:'+name)
 return original(name,*args,**kwargs)
builtins.__import__=guarded
import rich_dependence_operator_v2 as op
r=op.operate('run',Path(sys.argv[2]),sys.argv[3],json.loads(Path(sys.argv[4]).read_text(encoding='utf-8')),Path(sys.argv[5]))
assert r['status']=='completed_verified',r
assert not any(x.split('.')[0] in {'sklearn','joblib'} for x in sys.modules)
print(json.dumps(r))
"""
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),op.sha(RECIPE),str(PATHS_FILE),str(tmp_path)],capture_output=True,text=True,timeout=600)
    assert p.returncode==0,p.stdout+p.stderr
    for name in op.REQUIRED:assert (completed/name).read_bytes()==(tmp_path/completed.name/name).read_bytes()

@pytest.mark.parametrize('boundary',[1,0])
def test_real_report_death_resume_exact_payloads(completed,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'rich_dependence_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PATHS_FILE),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600);assert p.returncode==91,p.stdout+p.stderr
    assert call('resume',tmp_path)['status']=='completed_verified'
    for name in op.REQUIRED:assert (completed/name).read_bytes()==(tmp_path/completed.name/name).read_bytes()
