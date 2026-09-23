"""Verify current collector health after the immutable source export."""
from pathlib import Path
from urllib.request import urlopen
import hashlib,json,sys,time
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
VAULT=Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
sys.path.insert(0,str(ROOT))
from oanda_pair_local_forecast_study_v1 import load_registry,digest
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
registry=load_registry(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
export=load(OUT/'VAULT_EXPORT_VERIFICATION.json')
receipt=ROOT/'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json'
assert sha(receipt)==export['pair_forecast_coverage_receipt_sha256']
for row in load(receipt)['source_bindings']:assert sha(ROOT/row['path'])==row['sha256'],row['path']
with urlopen('http://127.0.0.1:8765/api/main',timeout=30) as response:api=json.load(response)
coverage=api['pair_local_forecasts'];assert coverage['status']=='current', {key:coverage.get(key) for key in ('status','reason','reason_label')}
assert len(coverage['rows'])==68 and coverage['counts']['forecast']>=20
heartbeat=load(ROOT/'data/oanda_training_manager/pair_local_forecast_study_v1/heartbeat.json')
assert 0<=time.time()-heartbeat['generated_epoch']<=90
assert heartbeat['errors']==heartbeat['heartbeat_publication_errors']==0
assert heartbeat['can_place_orders'] is False and heartbeat['can_promote'] is False
account=load(ROOT/'data/oanda_training_manager/state/account_007_dashboard_v1.json')
aggregate=account['aggregate']
assert aggregate['openTradeCount']==aggregate['pendingOrderCount']==0
report={'schema_version':'pair_coverage_post_export_runtime_verification_v1_20260907','status':'passed',
    'observed_epoch':time.time(),'registry_sha256':digest(registry),'validation_sha256':sha(receipt),
    'all_86_source_and_evidence_bindings_unchanged':True,'source_archive':export['source'],
    'local_vault_record_count':export['record_count'],'dashboard_counts':coverage['counts'],
    'preliminary_api_observation':'One earlier post-export sample was unavailable; an immediate independent read was current. The failed sample body was not retained, so its cause is not asserted.',
    'collector_heartbeat':heartbeat,'account_observation_time':account['time'],
    'account_aggregate':aggregate,'can_place_orders':False,'prediction_improvement_demonstrated':False,
    'cloud_sync_completion_observed':False}
raw=(json.dumps(report,indent=2)+'\n').encode()
for path in (OUT/'POST_EXPORT_RUNTIME_VERIFICATION.json',VAULT/'maintenance/PAIR_FORECAST_COVERAGE_RUNTIME_VERIFICATION_20260907.json'):
    with path.open('xb') as handle:handle.write(raw)
print(json.dumps({'status':'passed','dashboard_counts':coverage['counts'],'collector_errors':0,
    'source_archive':export['source']['archive'],'local_vault_records':export['record_count'],'open_positions':0,'pending_orders':0}))
