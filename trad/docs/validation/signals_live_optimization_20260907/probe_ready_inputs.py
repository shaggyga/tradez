"""Read-only readiness/performance measurement; these are not issued forecasts."""
import hashlib,json,os,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent/'trad'
sys.path.insert(0,str(ROOT))
for name in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[name]='1'
from oanda_causal_forecast_inputs_gap_v2 import capture_inputs,compute_predictions
from oanda_market_overview import build_market_overview
output=Path(__file__).resolve().parent/'live_input_probe'
output.mkdir(exist_ok=True)
started=time.perf_counter()
capture=capture_inputs(ROOT/'data/oanda_training_manager/candles')
capture_sec=time.perf_counter()-started
start_fit=time.perf_counter()
result=compute_predictions(capture) if capture['status']=='ready' else {'status':'abstain','reasons':capture['reasons']}
fit_sec=time.perf_counter()-start_fit
raw=json.dumps(capture,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
(output/'observed_inputs.json').write_bytes(raw)
(output/'computed_not_issued.json').write_text(json.dumps(result,sort_keys=True,allow_nan=False),encoding='utf8')
market_timings=[]
for index in range(3):
    before=time.perf_counter(); market=build_market_overview(ROOT/'data/oanda_training_manager')
    market_timings.append(time.perf_counter()-before)
report={'observed_epoch':time.time(),'capture_status':capture['status'],'result_status':result['status'],
 'reasons':result.get('reasons',[]),'capture_sec':capture_sec,'fit_sec':fit_sec,
 'current_common_bars':capture.get('current_common_bars'),
 'retained_real_rows_by_pair':capture.get('retained_real_rows_by_pair'),
 'source_capture_sha256':capture.get('source_capture_sha256'),
 'capture_file_sha256':hashlib.sha256(raw).hexdigest(),
 'families':{k:{'side':v['side'],'training_rows':v['diagnostics'].get('training_rows')} for k,v in result.get('predictions',{}).items()},
 'market_timings_sec':market_timings,'market_status':market['status'],
 'current_pair_count':market.get('current_pair_count'), 'technical_pair_count':market.get('technical_pair_count'),
 'forecasts_issued':False,'broker_requests':0,'orders':0}
(output/'READINESS_PROBE.json').write_text(json.dumps(report,indent=2,allow_nan=False),encoding='utf8')
print(json.dumps(report,allow_nan=False))
