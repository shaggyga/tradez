import copy,json,os,shutil,subprocess,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import campaign_inspector_operator_v2 as op
from campaign_inspector_v2 import CampaignReader
from publication import RunPublisher
from contracts import fingerprint
BASE=ROOT/'evidence/timed_20260922_022952/campaign_report_step'
PATHS_FILE=Path(os.environ.get('FOREX_INSPECTOR_PATHS',str(BASE/'PATHS.json')))
RECIPE=ROOT/'CAMPAIGN_INSPECTOR_OPERATOR_RECIPE.json'
QUERY={'pair':'EUR_USD','origin':1721606460,'target':'technical_endpoint_midpoint_elapsed_2880m','method':'ridge','procedure':'frozen'}
def read(p):return json.loads(p.read_text())

@pytest.mark.parametrize('flag',['false','true',1,0,None])
def test_reveal_requires_json_boolean(reader,flag):
    with pytest.raises(ValueError,match='explicit_boolean_outcome_reveal_required'):
        reader.forecast(**QUERY,reveal_outcomes=flag,outcome_asof=1722556800)
    with pytest.raises(ValueError,match='explicit_boolean_outcome_reveal_required'):
        reader.decision(method='ridge',scenario='candle_one_bp_slippage_rollover',arm='fixed_hold',epoch=1721606462,reveal_outcomes=flag,outcome_asof=1721779260)
@pytest.fixture(scope='module')
def paths():return read(PATHS_FILE)
@pytest.fixture(scope='module')
def reader(paths):return CampaignReader(paths,read(RECIPE)['dependencies'])
@pytest.fixture(scope='module')
def baseline(paths,tmp_path_factory):
    supplied=os.environ.get('FOREX_INSPECTOR_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('inspector-baseline')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),paths,runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def test_original_forecast_default_never_joins_outcomes(reader,monkeypatch):
    original=reader.read
    def no_outcomes(alias,name):
        assert alias!='technical','default inspection must not join labels'
        return original(alias,name)
    monkeypatch.setattr(reader,'read',no_outcomes)
    result=reader.forecast(**QUERY)
    assert not result['outcomes_revealed'] and result['outcome'] is None
    assert result['forecast_source']['original_record_sha256']==fingerprint(result['original_forecast'])
    assert result['original_forecast']['forecast']['decision_epoch']==QUERY['origin']

def test_reveal_obeys_label_availability_and_preserves_original(reader):
    blind=reader.forecast(**QUERY)
    early=reader.forecast(**QUERY,reveal_outcomes=True,outcome_asof=QUERY['origin']+1)
    later=reader.forecast(**QUERY,reveal_outcomes=True,outcome_asof=1721779260)
    assert early['outcome']=={'status':'not_yet_available','available_epoch':1721779260}
    assert later['outcome']['available_epoch']==1721779260 and later['outcome']['value'] is not None
    assert blind['original_forecast']==early['original_forecast']==later['original_forecast']

@pytest.mark.parametrize('query',[{'method':'unknown'},{'pair':'../secret'},{'origin':0},{'target':'daily_close'}])
def test_unregistered_queries_are_not_silent_empty_success(reader,query):
    with pytest.raises(ValueError,match='registered_coverage'):reader.forecast(**{**QUERY,**query})

@pytest.mark.parametrize('asof',[None,True,0,1721606460.0])
def test_explicit_integer_later_asof_required(reader,asof):
    with pytest.raises(ValueError,match='explicit_later'):reader.forecast(**QUERY,reveal_outcomes=True,outcome_asof=asof)

def test_asof_without_reveal_refused(reader):
    with pytest.raises(ValueError,match='explicit_reveal'):reader.forecast(**QUERY,outcome_asof=1721779260)

def test_returned_record_mutation_does_not_change_original(reader):
    before=reader.forecast(**QUERY);changed=reader.forecast(**QUERY);changed['original_forecast']['forecast']['prediction']=9999
    assert reader.forecast(**QUERY)==before

def test_policy_hidden_and_revealed_original_events(reader):
    query={'method':'ridge','scenario':'candle_one_bp_slippage_rollover','arm':'fixed_hold','epoch':1721606462}
    a=reader.decision(**query);b=reader.decision(**query,reveal_outcomes=True,outcome_asof=1721779260)
    assert a['outcome'] is None and a['original_decision']['action']=='ENTER'
    assert a['original_decision']==b['original_decision'] and b['outcome']['last_event_epoch']==1721779260
    assert b['outcome']['arm_state']['open_lot_count']==0 and b['outcome']['subsequent_arm_events']

def test_report_retains_all_scores_policies_and_movement_limits(reader,baseline):
    r=read(baseline/'campaign_report.json')
    assert r['original_forecast_coverage']['instrument_count']==68 and r['original_forecast_coverage']['coverage_rows']==76160
    assert len(r['original_scores'])==56 and len(r['policies'])==4 and r['selected_model'] is None
    assert r['movement_support']['contiguous_path_support']['1440']==0
    assert r['movement_support']['intrabar_barrier_order'].startswith('unsupported')
    assert r['observed_episode_movement'] and all('observed_replay_frame' in x['movement_scope'] for x in r['observed_episode_movement'])
    assert any(g['report']['valuation_coverage']['fixed_hold']['event_marks_unavailable'] for g in r['policies'])

def test_consumed_bytes_rechecked_after_initial_verification(paths,tmp_path):
    root=tmp_path/'matched';shutil.copytree(paths['matched'],root);changed={**paths,'matched':str(root)}
    r=CampaignReader(changed,read(RECIPE)['dependencies']);(root/'coverage.json').write_text('[]')
    with pytest.raises(ValueError,match='consumed_bytes_changed'):r.forecast(**QUERY)

def test_source_drift_before_import(paths,tmp_path):
    for n in (*op.SOURCES,RECIPE.name):shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/'campaign_inspector_v2.py').write_text("raise RuntimeError('DO_NOT_IMPORT')\n")
    r=tmp_path/RECIPE.name;p=subprocess.run([sys.executable,'-I',str(tmp_path/'campaign_inspector_operator_v2.py'),'run','--recipe',str(r),'--recipe-sha256',op.sha(r),'--paths',str(PATHS_FILE),'--runs-dir',str(tmp_path/'runs')],capture_output=True,text=True,timeout=30)
    assert p.returncode==2 and 'source_drift_before_import' in p.stdout and 'DO_NOT_IMPORT' not in p.stderr

def test_writer_exclusion_and_false_completion(paths,tmp_path):
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],op.identity_for(r));p.acquire()
    try:assert op.operate('run',RECIPE,op.sha(RECIPE),paths,tmp_path)['status']=='review_required'
    finally:p.release()
    assert op.operate('verify',RECIPE,op.sha(RECIPE),paths,tmp_path)['status']=='review_required'

@pytest.mark.parametrize('boundary',[1,0])
def test_real_report_process_death_and_resume(paths,baseline,tmp_path,boundary):
    cmd=[sys.executable,'-I','-B',str(ROOT/'campaign_inspector_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PATHS_FILE),'--runs-dir',str(tmp_path)]
    dead=subprocess.run(cmd+['--test-crash-after',str(boundary)],capture_output=True,text=True,timeout=120);assert dead.returncode==91,dead.stdout+dead.stderr
    r=op.operate('resume',RECIPE,op.sha(RECIPE),paths,tmp_path);assert r['status']=='completed_verified',r
    for n in op.REQUIRED:assert (baseline/n).read_bytes()==(Path(r['run_path'])/n).read_bytes()

def test_windows_launcher_propagates_success_and_pin_refusal(paths,baseline,tmp_path):
    config=tmp_path/'machine.json';values={'python':sys.executable,'recipe_sha256':op.sha(RECIPE),'paths':str(PATHS_FILE),'runs_dir':str(baseline.parent)};config.write_text(json.dumps(values))
    cmd=[str(ROOT/'Run-Forex-Campaign.cmd'),'-Action','verify','-Config',str(config)]
    good=subprocess.run(cmd,capture_output=True,text=True,timeout=120);assert good.returncode==0,good.stdout+good.stderr
    assert json.loads(good.stdout)['status']=='completed_verified'
    values['recipe_sha256']='0'*64;config.write_text(json.dumps(values));bad=subprocess.run(cmd,capture_output=True,text=True,timeout=30)
    assert bad.returncode==2 and 'Pinned recipe changed' in bad.stderr
