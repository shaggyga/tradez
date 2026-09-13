"""Authorized isolated deterministic inference replay; no fit/ledger/runtime."""
import sys
sys.dont_write_bytecode=True
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,localcontext
import hashlib,importlib,json,math,shutil,sqlite3,time

WORK=Path(__file__).resolve().parent
ISOLATED=WORK.parent/'workspace/trad'
RECOVERY=Path(r'D:\ForexRecovery\revamp_20260908T1353Z').resolve()
ARTIFACT=RECOVERY/'artifacts/legacy_model_components/state/second_ridge_models_v1.json'
SOURCE_INPUT=Path(r'D:\forex\trad\data\oanda_training_manager\candles_s5_bam\EUR_USD_S5.parquet')
INPUT=(RECOVERY/'inputs/second_ridge/EUR_USD_S5.parquet').resolve()
EXPECTED={
 'oanda_second_forecast.py':'3ac9f38bbe7140aae7bf391d1778f658b599199601d811d97d3386f32d727063',
 'oanda_second_forecast_fit.py':'238b8e6d83536aa287dc6282a364ccbe7ba016f2e00221e3b95381edd8d4aba8',
}
def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()
def bind(p):return {'path':str(p),'bytes':p.stat().st_size,'sha256':sha(p)}
def utc(epoch=None):return datetime.fromtimestamp(time.time() if epoch is None else epoch,timezone.utc).isoformat()
def no_sqlite(*args,**kwargs):raise AssertionError('SQLite connections prohibited in inference smoke')
sqlite3.connect=no_sqlite
started=time.time();assert INPUT.is_relative_to(RECOVERY) and not INPUT.exists()
assert sha(ARTIFACT)=='85ff37fd77f85fa6ae990990ead19eb7d4e20389d99b774023f32c13bf62658c'
source_bindings=[]
for name,expected in EXPECTED.items():
    assert sha(ISOLATED/name)==expected
    source_bindings.append(bind(ISOLATED/name))
before=SOURCE_INPUT.stat()
INPUT.parent.mkdir(parents=True,exist_ok=True)
with SOURCE_INPUT.open('rb') as source,INPUT.open('xb') as target:shutil.copyfileobj(source,target,length=1024*1024)
shutil.copystat(SOURCE_INPUT,INPUT)
assert (before.st_size,before.st_mtime_ns)==(SOURCE_INPUT.stat().st_size,SOURCE_INPUT.stat().st_mtime_ns)
original_input=bind(SOURCE_INPUT);preserved_input=bind(INPUT)
assert original_input['sha256']==preserved_input['sha256']
sys.path.insert(0,str(ISOLATED))
runtime=importlib.import_module('oanda_second_forecast')
transform=importlib.import_module('oanda_second_forecast_fit')
assert Path(runtime.__file__).resolve()==(ISOLATED/'oanda_second_forecast.py').resolve()
assert Path(transform.__file__).resolve()==(ISOLATED/'oanda_second_forecast_fit.py').resolve()

# This is the original pure transform; it returns arrays and never invokes a fit.
artifact=json.loads(ARTIFACT.read_text())
max_horizon=max(artifact['horizons_sec'])
dataset=transform.feature_matrix(INPUT,sample_step=artifact['sample_step_bars'],max_horizon_sec=max_horizon)
assert dataset['instrument']=='EUR_USD' and len(dataset['features'])>0
index=int(dataset['indices'][-1]);values=dataset['features'][-1]
feature_vector={name:float(v) for name,v in zip(runtime.FEATURE_NAMES,values)}
assert len(feature_vector)==14 and all(math.isfinite(v) for v in feature_vector.values())
first_epoch=int(dataset['times'][0])/1e9;last_epoch=int(dataset['times'][-1])/1e9;origin_epoch=int(dataset['times'][index])/1e9
fit_epoch=datetime.fromisoformat(artifact['fitted_utc']).timestamp()
assert first_epoch<=origin_epoch<last_epoch<fit_epoch
snapshot=runtime.SecondRidgeSnapshot(ARTIFACT)
assert snapshot.ready
first=snapshot.predict('EUR_USD',feature_vector)
second=snapshot.predict('EUR_USD',feature_vector)
assert len(first)==len(second)==13
assert {r['horizon_sec'] for r in first}==set(artifact['horizons_sec'])
assert first==second
diagnostics=[]
with localcontext() as ctx:
    ctx.prec=80
    D=lambda v:Decimal(str(v))
    for row in sorted(first,key=lambda r:r['horizon_sec']):
        h=row['horizon_sec'];model=artifact['models']['EUR_USD'][str(h)]
        raw=D(model['intercept'])+sum(D(coef)*(D(feature_vector[name])-D(mean))/max(D(scale),D('1e-9'))
            for name,mean,scale,coef in zip(runtime.FEATURE_NAMES,model['feature_means'],model['feature_scales'],model['coefficients']))
        calibration=max(0.,min(10.,float(model.get('magnitude_calibration',1.))))
        predicted=raw*D(calibration)
        raw_error=abs(float(raw)-row['raw_score']);pred_error=abs(float(predicted)-row['predicted_signed_pips'])
        raw_tolerance=1e-10*(1+abs(float(raw)));pred_tolerance=1e-10*(1+abs(float(predicted)))
        assert raw_error<=raw_tolerance and pred_error<=pred_tolerance
        residual=max(.05,float(model.get('residual_std_pips',1.)))
        p=1/(1+math.exp(-max(-8.,min(8.,float(predicted)/residual))))
        probability_error=abs(p-row['probability_up']);assert probability_error<=1e-10
        diagnostics.append({'instrument':'EUR_USD','horizon_sec':h,'model_id':row['model_id'],
          'raw_score_pips':row['raw_score'],'calibrated_expected_pips':row['predicted_signed_pips'],
          'uncalibrated_probability_up':row['probability_up'],'independent_decimal_raw_pips':str(raw),
          'independent_decimal_calibrated_pips':str(predicted),'raw_absolute_error':raw_error,'calibrated_absolute_error':pred_error,
          'raw_tolerance':raw_tolerance,'calibrated_tolerance':pred_tolerance,'probability_absolute_error':probability_error,
          'engineering_reference_epoch':origin_epoch,'nominal_target_epoch':origin_epoch+h,
          'model_fitted_utc':artifact['fitted_utc'],'fit_provenance':row['fit_provenance'],
          'research_only':True,'account_eligible':False,'can_place_orders':False,'can_authorize':False,'can_promote':False,
          'proof_eligible':False,'forecast_issued':False,'execution_profiles_included':False})
assert all(sha(ISOLATED/name)==expected for name,expected in EXPECTED.items())
assert sha(ARTIFACT)=='85ff37fd77f85fa6ae990990ead19eb7d4e20389d99b774023f32c13bf62658c'
assert sha(INPUT)==preserved_input['sha256'] and sha(SOURCE_INPUT)==original_input['sha256']
result={'schema':'second_ridge_isolated_engineering_smoke_v1','status':'passed','started_utc':utc(started),'completed_utc':utc(),
 'scope':'Deterministic source/artifact/feature/inference compatibility only. Pre-fit retained historical inputs; potentially in-sample, exact training membership not reconstructed. Not accuracy, prospective availability, profitability or live readiness.',
 'model_artifact':bind(ARTIFACT),'source_bindings':source_bindings,'input_original':original_input,'input_preserved':preserved_input,
 'input_summary':{'source_rows':int(dataset['source_rows']),'eligible_transform_rows':len(dataset['features']),
   'first_source_utc':utc(first_epoch),'last_source_utc':utc(last_epoch),'selected_feature_utc':utc(origin_epoch),'model_fitted_utc':artifact['fitted_utc'],
   'all_input_rows_precede_model_fit':True,'selection':'latest row eligible under original transform and maximum14400-second horizon; no outcome selection',
   'sample_step_bars':artifact['sample_step_bars'],'source_granularity':'S5','model_live_input_granularity':'S1',
   'first_60sec_continuity_ns':int(dataset['times'][index]-dataset['times'][index-12])},
 'feature_names':list(runtime.FEATURE_NAMES),'feature_vector':feature_vector,'diagnostics':diagnostics,
 'checks':{'horizons_predicted_each_run':13,'prediction_runs':2,'repeat_outputs_exactly_equal':True,'independent_coefficient_formula':'80-digit Decimal intercept+sum(beta*(x-mean)/max(scale,1e-9)); then retained calibration; logistic separately',
 'all_raw_and_calibrated_predictions_within_tolerance':True,'all_probability_comparisons_within_tolerance':True,'source_artifact_input_hashes_unchanged':True},
 'environment':{'python':sys.version,'executable':sys.executable,'numpy':transform.np.__version__,'pyarrow':importlib.import_module('pyarrow').__version__},
 'prohibited_actions_observed':{'training_runs':0,'model_pickle_loads':0,'sqlite_connections':0,'runtime_instances':0,'broker_calls':0,'forecasts_issued':0},
 'output_safety':{'research_only':True,'account_eligible':False,'can_place_orders':False,'can_authorize':False,'can_promote':False,'proof_eligible':False,'execution_profiles_included':False},
 'helper':bind(Path(__file__).resolve())}
target=WORK/'SECOND_RIDGE_ENGINEERING_SMOKE_20260908.json'
with target.open('x',encoding='utf-8',newline='\n') as f:json.dump(result,f,indent=2,sort_keys=True,allow_nan=False);f.write('\n')
print(json.dumps({'status':'passed','path':str(target),'sha256':sha(target),'horizons':13,'runs':2,
 'input_sha256':preserved_input['sha256'],'input_last_utc':utc(last_epoch),'selected_feature_utc':utc(origin_epoch),
 'fit_utc':artifact['fitted_utc'],'max_raw_error':max(d['raw_absolute_error'] for d in diagnostics),'max_calibrated_error':max(d['calibrated_absolute_error'] for d in diagnostics),
 'elapsed_sec':round(time.time()-started,3)}))
