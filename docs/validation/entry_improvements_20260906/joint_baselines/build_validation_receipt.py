"""Preserve offline joint-baseline test and synthetic timing evidence."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
import xml.etree.ElementTree as ET

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
import numpy as np
import pytest
from oanda_joint_return_baselines import fit_joint_return_baselines, PAIRS, ORIENTATION

def bind(path):
    return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size}

suite=ET.parse(OUT/'integration_tests.xml').getroot().find('testsuite')
assert suite is not None and suite.attrib['tests']=='68'
assert all(suite.attrib[field]=='0' for field in ('errors','failures','skipped'))
contract=json.loads((ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json').read_text(encoding='utf-8'))
registered={name:bind(ROOT/name)['sha256'] for name in contract['source_bindings']}
assert registered==contract['source_bindings']

# Artificial prices and artificial receipt times; no market data is loaded.
rng=np.random.default_rng(29)
prices=np.asarray([1.1,1.3,0.7,0.6,150,0.9,1.2])
bars=[]
for index in range(600):
    prices*=np.exp(rng.normal(0,1,7)*np.asarray([ORIENTATION[p] for p in PAIRS])/10_000)
    for column,pair in enumerate(PAIRS):
        bars.append({'instrument':pair,'start_epoch':index*60,'end_epoch':(index+1)*60,
                     'close_mid':float(prices[column]),'available_epoch':(index+1)*60+0.1})
times=[]
for repeat in range(3):
    start=time.perf_counter()
    result=fit_joint_return_baselines(bars,decision_epoch=36_000.2,reference_epoch=36_000)
    times.append(time.perf_counter()-start)
assert result['status']=='predicted' and result['selected_training_rows']==480
report={
    'schema_version':'joint_return_baselines_offline_validation_v1',
    'generated_utc':datetime.now(timezone.utc).isoformat(),
    'status':'implemented_offline_not_activated',
    'source_files':[bind(ROOT/name) for name in ('oanda_joint_return_baselines.py','test_oanda_joint_return_baselines.py')],
    'test_result':{'tests':68,'passed':68,'errors':0,'failures':0,'skipped':0,
                   'junit_suite_elapsed_seconds':float(suite.attrib['time']),
                   'pytest_reported_elapsed_seconds':2.26,'junit':bind(OUT/'integration_tests.xml'),
                   'harness':bind(OUT/'run_tests.py'),
                   'invocation':r"C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe -B C:\Users\zmoor\Documents\forex\entry_improvements_20260906\joint_baselines\run_tests.py test_oanda_joint_return_baselines.py"},
    'runtime':{'executable':sys.executable,'python':sys.version,'numpy_version':np.__version__,
               'numpy_path':np.__file__,'pytest_version':pytest.__version__,'pytest_path':pytest.__file__},
    'fixed_defaults':result['parameters'],
    'synthetic_timing':{'common_minute_bars':600,'pair_rows':4200,'selected_training_rows':480,
                        'three_fit_seconds':times,'median_fit_seconds':statistics.median(times),
                        'scope':'synthetic pure function and result construction only; not project-cycle speed'},
    'registered_source_hashes_unchanged':registered,
    'historical_real_data_accuracy_established':False,
    'historical_availability_reconstructed':False,
    'new_historical_model_selection_or_tuning':False,
    'runtime_integration':False,'project_start_commands_issued':False,
    'database_connections':0,'broker_or_network_access':False,
    'proof_eligible':False,'account_eligible':False,'can_place_orders':False,
    'limits':['Training/model/feature clocks supplied by caller are checked, not independently attested.',
              'Historical candles do not retain the original actual availability required for a causal historical accuracy claim.',
              'Synthetic lead-lag tests verify implementation only; no market profitability or future alpha is established.',
              'Direct-horizon ridge multivariate lag regression is not a recursive VAR system, VECM, cointegration or HMM.',
              'Overlapping training rows do not establish independent samples.',
              'No calibrated probability, spread-adjusted outcome, portfolio P/L, significance or promotion is computed.']}
destination=OUT/'JOINT_BASELINE_VALIDATION_20260906.json'
with destination.open('x',encoding='utf-8') as handle:
    json.dump(report,handle,indent=2,sort_keys=True,allow_nan=False)
    handle.write('\n')
print(json.dumps({'receipt':str(destination),'sha256':bind(destination)['sha256'],
                  'tests':68,'median_synthetic_fit_seconds':statistics.median(times),'source_files':report['source_files']}))
