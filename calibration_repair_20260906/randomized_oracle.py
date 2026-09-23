"""Independent randomized full-scan oracle preserved from the read-only review.

Runs only the pure V2 evaluator. Writes one exclusively-created result JSON next
 to this script; no database, worker, network, or source mutations are performed.
"""
from pathlib import Path
import datetime
import hashlib
import json
import sys

fixture=Path(__file__).resolve().parent
source_path=fixture.parent/'trad'/'oanda_timeframe_matrix_calibration_v2.py'
source_sha_before=hashlib.sha256(source_path.read_bytes()).hexdigest()
sys.path.insert(0,str(source_path.parent))

# Original independent oracle executed during read-only review, unchanged.
import random
from oanda_timeframe_matrix_calibration_v2 import CONTRACT_ID, CalibrationScope, evaluate_calibration
scope=CalibrationScope('EUR_USD','review','fresh',60)
checked=0
for seed in range(50):
    rng=random.Random(seed)
    forecasts=[]; labels=[]
    for i in range(100):
        reference=float(i*10)
        issue=reference+rng.randint(0,55)
        base=dict(contract_id=CONTRACT_ID,event_id=str(i),instrument='EUR_USD',horizon_sec=60,reference_epoch=reference,target_epoch=reference+60,reference_mid=1.1)
        forecasts.append(dict(base,forecast_id=str(i),lane_id='review',cohort_id='fresh',issued_epoch=issue,committed_available_epoch=issue+1,raw_probability_up=rng.choice([0,.1,.5,.8,1])))
        if rng.random()<.85:
            maturity=reference+60+rng.randint(0,60)
            labels.append(dict(base,target_mid=rng.choice([1.0,1.1,1.2]),target_quote_epoch=maturity,committed_available_epoch=maturity+rng.randint(0,400)))
    rng.shuffle(forecasts); rng.shuffle(labels)
    report=evaluate_calibration(forecasts,labels,scope=scope)
    by_id={f['forecast_id']:f for f in forecasts}; by_event={l['event_id']:l for l in labels}
    for row in report['rows']:
        eligible=[]
        for f in forecasts:
            l=by_event.get(f['event_id'])
            if l and max(l['target_epoch'],l['target_quote_epoch'],l['committed_available_epoch'],f['committed_available_epoch'])<row['issued_epoch']:
                eligible.append((f,l))
        eligible.sort(key=lambda pair:(pair[1]['target_epoch'],pair[1]['committed_available_epoch'],('EUR_USD',60,pair[0]['reference_epoch'],pair[0]['target_epoch'])))
        assert row['training_forecast_ids']==[f['forecast_id'] for f,l in eligible]
        assert row['forecast_id'] not in row['training_forecast_ids']
        raw=row['raw_probability_up']; index=min(9,int(raw*10))
        bin_rows=[(f,l) for f,l in eligible if min(9,int(f['raw_probability_up']*10))==index]
        expected=raw if len(eligible)<40 else (sum(l['target_mid']>l['reference_mid'] for f,l in bin_rows)+20*((index+.5)/10))/(len(bin_rows)+20)
        assert row['calibrated_probability_up']==expected
        checked+=1
print('PASS: randomized independent full-scan oracle matched sweep for',checked,'forecast rows across 50 seeds; no own-label training')
# End original oracle.

source_sha_after=hashlib.sha256(source_path.read_bytes()).hexdigest()
assert source_sha_before==source_sha_after, 'Evaluator source changed during verification'
result={
    'schema_version':1,
    'completed_utc':datetime.datetime.now(datetime.UTC).isoformat(),
    'status':'passed',
    'contract_id':CONTRACT_ID,
    'oracle':'independent_full_scan_training_selection_and_bin_posterior',
    'seed_count':50,
    'forecasts_per_seed':100,
    'forecast_rows_checked':checked,
    'checks':['exact_training_forecast_id_order','no_own_label_training','exact_calibrated_probability'],
    'input_variation':['shuffled_forecasts_and_labels','delayed_label_availability','missing_labels','varied_issue_clocks','probability_boundaries_and_ties','flat_outcomes'],
    'python_version':sys.version,
    'bytecode_writes_disabled':sys.dont_write_bytecode,
    'script_path':str(Path(__file__).resolve()),
    'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'evaluator_path':str(source_path),
    'evaluator_sha256_before':source_sha_before,
    'evaluator_sha256_after':source_sha_after,
    'database_accessed':False,
    'network_accessed':False,
    'worker_runtime_accessed':False,
}
output=fixture/'randomized_oracle_result.json'
n=1
while True:
    try:
        with output.open('x',encoding='utf-8',newline='\n') as stream:
            json.dump(result,stream,indent=2)
            stream.write('\n')
        break
    except FileExistsError:
        n+=1
        output=fixture/f'randomized_oracle_result_{n}.json'
print(json.dumps({'result_path':str(output),'result_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'script_sha256':result['script_sha256'],'evaluator_sha256':source_sha_after}))
