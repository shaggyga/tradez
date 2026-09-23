"""Small independent synthetic boundary probe, no project/runtime/file mutations."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

OUT = Path(__file__).resolve().parent
PROJECT = OUT.parent/'trad'
sys.path.insert(0, str(PROJECT))
import oanda_forecast_curve_contract_v1 as c
import oanda_curve_management_adapter_v1 as adapter

paths = [PROJECT/'oanda_forecast_curve_contract_v1.py',PROJECT/'oanda_curve_management_adapter_v1.py']
hashes = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
SOURCES = {'fixture.py':'a'*64}
results = []


def reject(label, action):
    try:
        action()
    except c.CurveContractError as exc:
        results.append({'case':label,'status':'passed','reason_code':str(exc)})
    except Exception as exc:
        results.append({'case':label,'status':'failed','exception_type':type(exc).__name__})
    else:
        results.append({'case':label,'status':'failed','reason':'unexpected_acceptance'})


def policy(**overrides):
    args = dict(native_horizons_sec=[60,120], maximum_reference_age_sec=30,
        maximum_build_sec=10,maximum_issue_delay_sec=10,maximum_publication_delay_sec=10,
        maximum_decision_age_sec=300,minimum_remaining_sec=0)
    args.update(overrides)
    return c.make_policy(**args)


prepared = c.prepare_curve(instrument='EUR_USD',pip_size='0.0001',forecast_cohort='fixture',
    model_sha256='b'*64,feature_version='fixture',source_bindings=SOURCES,
    input_capture_sha256='c'*64,input_available_epoch=1201,reference_epoch=1200,
    reference_label_epoch=1140,reference_price='1.1000',reference_price_kind='mid_close',
    bar_duration_sec=60,model_fitted_epoch=1000,computation_started_epoch=1201,
    computed_epoch=1202,points=[dict(horizon_sec=h,target_epoch=1200+h,target_label_epoch=1140+h,
        model_id='fixture',predicted_signed_pips='3',probability_up='0.6',
        probability_scope='original_unconditional_uncalibrated') for h in (60,120)],
    policy=policy(),computation_sha256='d'*64)
curve = c.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:1203)
pub = c.publication_receipt(curve,persisted_bytes_sha256=c.content_hash(curve),
    publication_started_epoch=1204,expected_source_bindings=SOURCES,clock=lambda:1205)
consumption = c.consume_curve(curve,pub,expected_source_bindings=SOURCES,clock=lambda:1261)
candidate = adapter.candidate_for_target(curve,pub,consumption,decision_epoch=1262,target_epoch=1260,
    quote=None,metadata=None,expected_source_bindings=SOURCES,maximum_quote_age_sec=30)
assert candidate['status']=='unavailable' and candidate['reason_code']=='native_target_elapsed_before_decision'
results.append({'case':'historical_consumption_does_not_authorize_elapsed_target','status':'passed',
    'consumption_scope':consumption.get('scope'),'refusal_code':candidate['reason_code']})

for missing in prepared:
    if missing == 'prepared_sha256':
        continue
    altered = deepcopy(prepared)
    del altered[missing]
    altered['prepared_sha256'] = c.content_hash({k:v for k,v in altered.items() if k!='prepared_sha256'})
    reject('prepared_missing_'+missing,lambda value=altered:c.validate_prepared(value,expected_source_bindings=SOURCES))

for key in c.AUTHORITY:
    for invalid in (True,False,0,1,None):
        if invalid is c.AUTHORITY[key]:
            continue
        altered=deepcopy(prepared)
        altered[key]=invalid
        altered['prepared_sha256']=c.content_hash({k:v for k,v in altered.items() if k!='prepared_sha256'})
        reject('authority_'+key+'_'+repr(invalid),lambda value=altered:c.validate_prepared(value,expected_source_bindings=SOURCES))

reject('nonstring_json_key',lambda:c.canonical_bytes({1:'x'}))
reject('nonfinite_json_float',lambda:c.canonical_bytes({'value':float('inf')}))
reject('oversized_json_integer',lambda:c.canonical_bytes(10**100))
reject('lazy_horizon_iterable',lambda:policy(native_horizons_sec=iter(range(1,66))))
reject('oversized_native_inventory',lambda:policy(native_horizons_sec=list(range(1,66))))
reject('mixed_source_keys',lambda:c.issue_curve(prepared,expected_source_bindings={1:'a'*64,'valid.py':'b'*64},clock=lambda:1203))
reject('future_computation_at_issue',lambda:c.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:1201))
reject('publication_before_issue',lambda:c.publication_receipt(curve,persisted_bytes_sha256=c.content_hash(curve),
    publication_started_epoch=1202,expected_source_bindings=SOURCES,clock=lambda:1205))
reject('consumption_before_publication',lambda:c.consume_curve(curve,pub,expected_source_bindings=SOURCES,clock=lambda:1204))

after = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
report = {'schema_version':'independent_curve_contract_boundary_probe_v1_20260909',
    'generated_utc':datetime.now(timezone.utc).isoformat(),'source_hashes_before':hashes,
    'source_hashes_after':after,'source_stable':hashes==after,'case_count':len(results),
    'status':'passed' if hashes==after and all(r['status']=='passed' for r in results) else 'failed',
    'results':results,'scope':'Synthetic pure receipts only; no runtime or model-performance validation. Actual persisted I/O and expected cohort/model/policy identity remain obligations of the registered caller.'}
dest=OUT/('CURVE_CONTRACT_INDEPENDENT_BOUNDARIES_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'.json')
with dest.open('x',encoding='utf-8') as handle:
    json.dump(report,handle,indent=2,allow_nan=False)
    handle.write('\n')
print(json.dumps({'path':str(dest),'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),
    'status':report['status'],'case_count':len(results),'failures':[row for row in results if row['status']!='passed'],
    'source_hashes':hashes,'source_stable':hashes==after}))
