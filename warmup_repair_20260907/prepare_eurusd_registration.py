"""Register a separate EURUSD-only comparison without changing the paired study."""
import copy,hashlib,json,os,sys,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
for name in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[name]='1'
import oanda_causal_forecast_study_eurusd_v1 as worker
import oanda_causal_forecast_inputs_eurusd_v1 as inputs
from oanda_causal_forecast_ledger_eurusd_v1 import digest,encoded,validate_contract,SCHEMA
paired=json.loads((ROOT/'config/causal_forecast_study_gap_v2_20260907.json').read_bytes())
new=copy.deepcopy(paired)
sources={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in sorted(worker.REQUIRED_SOURCE_BINDINGS)}
identity=digest({'numeric_source':inputs.NUMERICAL_SOURCE_SHA256,'feature_source':sources['oanda_causal_forecast_inputs_eurusd_v1.py'],
    'dependencies':inputs.dependency_versions()})
cohorts={f:'causal_eurusd_two_family_v1_20260907.'+f+'.'+identity[:16] for f in inputs.FAMILIES}
now=datetime.now(timezone.utc)
new.update(schema_version=SCHEMA,contract_id='causal_eurusd_two_family_collection_v1_20260907',
    prepared_utc=now.isoformat(),source_bindings=sources,dependency_versions=inputs.dependency_versions(),
    cohorts=cohorts,numeric_model_source_sha256=inputs.NUMERICAL_SOURCE_SHA256,
    model_version='sha256:'+identity,feature_version='sha256:'+sources['oanda_causal_forecast_inputs_eurusd_v1.py'],
    input_pairs=['EUR_USD'],companion_contract_sha256=digest(paired),
    operational_revision='Separate local-model comparison: EURUSD ridge and EWMA use only their own required observations. Other pairs cannot withhold these forecasts. The gap_v2 four-family cohort remains separate and active; no partial batches or duplicate model stand-ins are imported.',
    input_policy={'alignment':'actual EURUSD UTC minute starts','minimum_current_common_bars':61,
        'maximum_source_rows_per_pair':1024,'maximum_common_bar_age_sec':900,
        'gap_policy':'Require 61 consecutive EURUSD closes; ridge training uses complete real 121-price windows with exact H1 labels; EWMA resets at the latest EURUSD gap. No peer inputs, imputation or compressed timestamps.',
        'availability':'conservative observed clock after stable source read; training computation completed before issuance'})
new.pop('predecessor_contract_sha256',None)
new['evaluation_protocol'].update(contract_id='causal_eurusd_two_family_evaluation_v1_20260907',
    historical_start_utc=now.isoformat(),historical_end_utc=(now+timedelta(days=365)).isoformat(),
    cohorts=cohorts,model_version=new['model_version'],feature_version=new['feature_version'],
    missingness='Retain all attempts, abstentions, missing publication/entries/targets and both local families; no return-based selection.')
validate_contract(new)
destination=ROOT/'config/causal_forecast_study_eurusd_v1_20260907.json'
with destination.open('xb') as f:f.write(encoded(new)+b'\n')
assert worker.load_contract(destination)==new
started=time.perf_counter();capture=inputs.capture_inputs(ROOT/'data/oanda_training_manager/candles')
capture_sec=time.perf_counter()-started
started=time.perf_counter();result=inputs.compute_predictions(capture);fit_sec=time.perf_counter()-started
assert capture['status']==result['status']=='ready',(capture.get('reasons'),result.get('reasons'))
receipt={'prepared_epoch':now.timestamp(),'activated':False,'contract_sha256':digest(new),'source_bindings':sources,
    'config_file_sha256':hashlib.sha256(destination.read_bytes()).hexdigest(),'cohorts':cohorts,
    'readonly_live_probe':{'capture_status':capture['status'],'result_status':result['status'],
        'capture_sec':capture_sec,'fit_sec':fit_sec,'current_common_bars':capture.get('current_common_bars'),
        'source_capture_sha256':capture['source_capture_sha256'],
        'model_source_sha256':result['model_source_sha256'],'computed_not_issued':True,
        'families':{f:{'side':p['side'],'expected_signed_pips':p['expected_signed_pips'],'diagnostics':p['diagnostics']} for f,p in result['predictions'].items()}},
    'orders':0}
with (OUT/'EURUSD_REGISTRATION_PREPARED.json').open('x',encoding='utf8') as f:json.dump(receipt,f,indent=2)
print(json.dumps({'contract_sha256':digest(new),'ready':True,'capture_sec':capture_sec,'fit_sec':fit_sec,'families':list(result['predictions'])}))
