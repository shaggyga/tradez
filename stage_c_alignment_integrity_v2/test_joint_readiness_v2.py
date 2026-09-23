import copy,json,math,os,shutil,subprocess,sys
from pathlib import Path
from decimal import Decimal,localcontext
import pytest
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import joint_readiness_operator_v2 as op
from contracts import fingerprint,validate_forecast
from publication import RunPublisher
from joint_readiness_schedule_v2 import GROUPS,HORIZONS,PROCEDURES
H=ROOT/'evidence/timed_20260922_022952/joint_readiness_step'
PATHS_FILE=Path(os.environ.get('FOREX_JOINT_PATHS',str(H/'PATHS.json')))
TRAD=Path(os.environ.get('FOREX_JOINT_TRAD',str(ROOT.parent/'trad')))
RECIPE=ROOT/'JOINT_READINESS_OPERATOR_RECIPE.json'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def command(action,runs):return [sys.executable,'-I','-B',str(ROOT/'joint_readiness_operator_v2.py'),action,'--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--paths',str(PATHS_FILE),'--trad-root',str(TRAD),'--runs-dir',str(runs)]
def call(action,runs):return op.operate(action,RECIPE,op.sha(RECIPE),read(PATHS_FILE),TRAD,runs)
def scientific(root):return {x['path']:x['sha256'] for x in read(root/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='prediction_resources.json'}
@pytest.fixture(scope='session')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_JOINT_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('joint')
    if not supplied:
        p=subprocess.run(command('run',runs),capture_output=True,text=True,timeout=1800);assert p.returncode==0,p.stdout+p.stderr
    r=call('verify',runs);assert r['status']=='completed_verified',r
    return Path(r['run_path'])

def test_actual_complete_scope_and_measured_prediction_batches(completed):
    r=read(completed/'run_report.json');timings=read(completed/'prediction_resources.json')
    assert r['fit_pairs_reused']==56 and r['model_artifacts_reused']==112 and r['models_fitted']==0
    assert r['counts']['coverage_rows']==152320 and r['score_groups']==112 and len(timings)==1120
    assert [x['seconds'] for x in r['full_fit_reservation_seconds_per_cutoff']]==[952,952]
    measured=[x for x in timings if x['status']=='measured_retained_weight_prediction'];skipped=[x for x in timings if x['status']=='not_run_no_ready_model']
    assert len(measured)==1068 and len(skipped)==52
    assert all(0<=x['elapsed_seconds']<=2 for x in measured) and all(x['elapsed_seconds'] is None for x in skipped)
    assert sum(x['forecast_values_checked'] for x in measured)==r['counts']['forecast_rows']

def test_every_projected_forecast_preserves_selected_original_value_and_valid_clock(completed):
    paths={k:Path(v) for k,v in read(PATHS_FILE).items()};cache={};count=0
    for path in completed.glob('forecasts_*.json'):
        for row in read(path):
            f=row['forecast'];validate_forecast(f);ref=row['source_reference'];key=(ref['dependency'],ref['payload'])
            if key not in cache:cache[key]={x['forecast']['forecast_id']:x for x in read(paths[key[0]]/key[1])}
            original=cache[key][ref['original_forecast_id']]
            assert fingerprint(original)==ref['original_record_sha256']
            old=original['forecast']
            assert f['prediction']==old['prediction'] and f['model_id']==old['model_id'] and f['training_view_fingerprint']==old['training_view_fingerprint']
            assert f['target_id']==old['target_id'] and f['decision_epoch']==old['decision_epoch']
            assert f['forecast_id']!=old['forecast_id'] and old['model_ready_epoch']<=f['model_ready_epoch']<=f['decision_epoch']
            assert f['available_epoch']>=old['available_epoch'];count+=1
    assert count==read(completed/'run_report.json')['counts']['forecast_rows']

def test_actual_first_use_and_adaptive_previous_ready_fallback(completed):
    recipe=read(RECIPE);c=read(Path(read(PATHS_FILE)['family'])/'experiment_contract.json');t0,t1=[x+60 for x in c['fit_cutoffs']]
    rows=read(completed/'coverage_full228_cost2_15_adaptive.json')
    assert all(x['reason']=='joint_model_not_ready' for x in rows if x['decision_epoch']==t0)
    eligible=[x for x in rows if x['decision_epoch']==t1 and x['feature_reason']=='eligible']
    assert eligible and all(x['reason']=='eligible' and x['prior_ready_artifact_fallback'] and x['selected_fit_cutoff']==c['fit_cutoffs'][0] for x in eligible)
    projected=[x for x in read(completed/'forecasts_full228_cost2_15_adaptive.json') if x['forecast']['decision_epoch']==t1]
    assert all(x['source_reference']['source_procedure']=='frozen' for x in projected)
    original=read(Path(read(PATHS_FILE)['family'])/'forecasts_full228_cost2_15_adaptive.json');updated={(x['record_id'],x['method']):x['forecast']['prediction'] for x in original if x['forecast']['decision_epoch']==t1}
    assert any(x['forecast']['prediction']!=updated[x['record_id'],x['method']] for x in projected)

def test_every_coverage_chunk_retains_all68_and_two_methods(completed):
    universe=set(read(RECIPE)['configuration']['universe'])
    for path in completed.glob('coverage_*.json'):
        rows=read(path);origins={x['decision_epoch'] for x in rows};assert len(origins)==20 and len(rows)==2720
        assert len({(x['record_id'],x['method']) for x in rows})==len(rows)
        for origin in origins:
            assert {x['instrument'] for x in rows if x['decision_epoch']==origin}==universe

def test_score_support_and_decimal_oracle(completed):
    root=Path(read(PATHS_FILE)['rich']);labels={}
    for pair in read(RECIPE)['configuration']['universe']:
        for o in read(root/('outcomes_'+pair+'.json'))['outcomes']:labels[o['record_id'],o['target_id']]=o
    asof=read(Path(read(PATHS_FILE)['family'])/'experiment_contract.json')['evaluation_asof']
    scores=read(completed/'scores.json')
    for g in GROUPS:
        for h in HORIZONS:
            for procedure in PROCEDURES:
                rows=read(completed/f'forecasts_{g}_{h}_{procedure}.json')
                for method in ('ridge','recovered_hgb'):
                    pairs=[(x,labels[x['record_id'],x['forecast']['target_id']]) for x in rows if x['method']==method]
                    pairs=[(x,o) for x,o in pairs if o['value'] is not None and o['available_epoch']<=asof]
                    s=next(x for x in scores if (x['group'],x['target_id'],x['procedure'],x['method'])==(g,f'technical_endpoint_midpoint_elapsed_{h}m',procedure,method))
                    assert s['rows']==len(pairs) and s['support_sha256']==fingerprint(sorted(x['record_id'] for x,o in pairs))
                    # Independent Decimal arithmetic on all legacy15m score rows.
                    if g=='legacy26' and h==15:
                        with localcontext() as ctx:
                            ctx.prec=60;errors=[Decimal(str(x['forecast']['prediction']))-Decimal(str(o['value'])) for x,o in pairs]
                            assert s['mse_bps2']==pytest.approx(float(sum(e*e for e in errors)/len(errors)),rel=1e-12,abs=1e-8)

def test_actual_inspection_before_publication_and_native_staleness(completed):
    row=read(completed/'forecasts_full228_cost2_15_adaptive.json')[0];f=row['forecast']
    args=dict(group=row['group'],horizon=15,procedure=row['procedure'],method=row['method'],instrument=f['instrument'],origin=f['decision_epoch'])
    before=op.inspect_forecast(RECIPE,op.sha(RECIPE),read(PATHS_FILE),TRAD,completed.parent,asof=f['available_epoch']-1,**args)
    assert before['status']=='not_yet_available' and before['forecast'] is None and before['source_reference'] is None
    after=op.inspect_forecast(RECIPE,op.sha(RECIPE),read(PATHS_FILE),TRAD,completed.parent,asof=f['available_epoch'],**args)
    assert after['status']=='available' and after['native_freshness']=='stale_conditioning' and after['outcomes_included'] is False

def test_source_drift_refused_before_import(tmp_path,monkeypatch):
    for n in op.SOURCES:shutil.copyfile(ROOT/n,tmp_path/n)
    (tmp_path/'joint_readiness_runner_v2.py').write_text("raise RuntimeError('DO_NOT_IMPORT')",encoding='utf-8');monkeypatch.setattr(op,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='source_drift_before_import'):op.preflight(RECIPE,op.sha(RECIPE),read(PATHS_FILE),TRAD)

def test_consumed_model_bytes_are_guarded(tmp_path):
    sys.path.insert(0,str(TRAD))
    from joint_readiness_predict_v2 import RetainedPredictor
    from types import SimpleNamespace
    (tmp_path/'model.joblib').write_bytes(b'not a model')
    p=RetainedPredictor(SimpleNamespace(paths={'family':tmp_path},dependencies={'family':{'payloads':{'model.joblib':'0'*64}}}))
    with pytest.raises(ValueError,match='consumed_bytes_changed'):p.blob('family','model.joblib')

def test_one_writer_and_false_completion(tmp_path):
    sys.path.insert(0,str(TRAD));from joint_readiness_runner_v2 import identity_for
    r=read(RECIPE);p=RunPublisher(tmp_path,r['run_id'],identity_for(r));p.acquire()
    try:assert call('run',tmp_path)['status']=='review_required'
    finally:p.release()
    assert call('verify',tmp_path)['status']=='review_required'

@pytest.mark.parametrize('boundary',[1,0])
def test_real_projection_death_resume_scientific_identity(completed,tmp_path,boundary):
    p=subprocess.run(command('run',tmp_path)+['--test-crash-after',str(boundary)],capture_output=True,text=True,timeout=1800)
    assert p.returncode==91,p.stdout+p.stderr
    p=subprocess.run(command('resume',tmp_path),capture_output=True,text=True,timeout=1800);assert p.returncode==0,p.stdout+p.stderr
    assert scientific(completed)==scientific(tmp_path/completed.name)

def test_actual_projection_cannot_fit_or_connect_network(completed,tmp_path):
    code='''import sys,json,socket
from pathlib import Path
sys.path.insert(0,sys.argv[1]);sys.path.insert(0,sys.argv[5])
from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge
from sklearn.ensemble import HistGradientBoostingRegressor
import matched_campaign_models_v2 as base,rich_family_models_v2 as rich,fitted_consumer_v2 as fitted
def deny(*args,**kwargs):raise AssertionError('fitting_or_network_forbidden')
Pipeline.fit=Ridge.fit=HistGradientBoostingRegressor.fit=deny;base.fit_pair=rich.fit_pair=fitted.fit_model=deny;socket.socket=deny
import joint_readiness_operator_v2 as op
r=op.operate('run',Path(sys.argv[2]),sys.argv[3],op.read(Path(sys.argv[4])),Path(sys.argv[5]),Path(sys.argv[6]))
assert r['status']=='completed_verified',r
print(json.dumps(r))
'''
    p=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),str(RECIPE),op.sha(RECIPE),str(PATHS_FILE),str(TRAD),str(tmp_path)],capture_output=True,text=True,timeout=1800)
    assert p.returncode==0,p.stdout+p.stderr
    assert scientific(completed)==scientific(tmp_path/completed.name)
