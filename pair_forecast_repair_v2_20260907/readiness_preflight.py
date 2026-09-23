"""One read-only v2 input-readiness census; no fitting, ledger or broker call."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import hashlib,json,sys,time
sys.dont_write_bytecode=True
OUT=Path(__file__).resolve().parent;ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
import oanda_causal_forecast_inputs_pair_v2 as inputs
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    started=time.time();registry_path=ROOT/'config/pair_local_forecast_study_v1_20260907.json'
    registry=json.loads(registry_path.read_text(encoding='utf-8'));names=['oanda_pair_local_models_v2.py','oanda_causal_forecast_inputs_pair_v2.py']
    source={name:sha(ROOT/name) for name in names};rows=[]
    for pair,item in sorted(registry['pairs'].items()):
        capture=inputs.capture_inputs(ROOT/'data/oanda_training_manager/candles',pair,pip_size=item['pip_size'])
        rows.append({'instrument':pair,'pip_size':item['pip_size'],'capture_status':capture['status'],
          'capture_reasons':capture['reasons'],'readiness_status':capture['readiness_status'],
          'family_readiness':capture['family_readiness'],'available_families':capture['available_families'],
          'first_observed_epoch':capture.get('first_observed_epoch'),'source_capture_sha256':capture['source_capture_sha256'],
          'captured_bytes_sha256':capture.get('sources',{}).get(pair,{}).get('captured_bytes_sha256'),
          'reference_start_epoch':capture.get('reference_start_epoch'),'last_bar_age_sec':capture.get('common_bar_age_sec'),
          'retained_real_rows_by_pair':capture.get('retained_real_rows_by_pair')})
    assert source=={name:sha(ROOT/name) for name in names}
    report={'schema_version':'v2_pair_input_readiness_preflight_20260907','status':'captured',
       'started_utc':datetime.fromtimestamp(started,timezone.utc).isoformat(),
       'finished_utc':datetime.now(timezone.utc).isoformat(),'duration_sec':time.time()-started,
       'source_bindings':source,'registry_metadata_source':{'path':str(registry_path),'sha256':sha(registry_path)},
       'pair_count':len(rows),'capture_counts':dict(Counter(r['capture_status'] for r in rows)),
       'readiness_counts':dict(Counter(r['readiness_status'] for r in rows)),
       'ready_by_family':{family:sum(r['family_readiness'].get(family,{}).get('ready',False) for r in rows) for family in inputs.FAMILIES},
       'rows':rows,'model_fits':0,'forecasts_issued':0,'new_study_activation':False,'broker_requests':0,'ledger_writes':0,
       'limitation':'Only local structural/input readiness, using draft v2 constants; no model predictions, performance measurement or publication authorization. Quotes may independently block a real attempt.'}
    path=OUT/'MODELS_INPUTS_V2_READONLY_PREFLIGHT_20260907.json'
    with path.open('x',encoding='utf-8') as handle:json.dump(report,handle,indent=2,allow_nan=False)
    print(json.dumps({'path':str(path),'sha256':sha(path),'duration_sec':report['duration_sec'],
                      'capture_counts':report['capture_counts'],'readiness_counts':report['readiness_counts'],'ready_by_family':report['ready_by_family']}))
if __name__=='__main__':main()
