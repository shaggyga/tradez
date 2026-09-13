from pathlib import Path
import json,hashlib
BASE=Path(__file__).resolve().parent/'actual_live_api_table_001'
raw=(BASE/'private_raw/ledger_full.json').read_bytes();value=json.loads(raw)
missing=[]
for row in value['observer_report']['summary']['rows']:
    slot=row['families']['ridge_price_news_v1']
    if slot['status']!='forecast':
        missing.append({'instrument':row['instrument'],'status':slot['status'],'reason':slot['reason'],'original_ledger_observed_epoch':slot['observed_epoch'],'last_attempt':slot['last_attempt'],'latest_original_target_epoch':(slot.get('latest_forecast') or {}).get('target_epoch'),'current_input_readiness':slot['current_readiness']})
out={'schema_version':'live_ledger_missing_rows_same_bytes_v1_20260909','status':'retained_observation','full_response_raw_sha256':hashlib.sha256(raw).hexdigest(),'observer_report_sha256':value['observer_report_sha256'],'response_observed_epoch':value['consumer']['observed_epoch'],'original_report_completed_epoch':value['observer_report']['completed_epoch'],'verified_ledger_pairs':value['consumer']['verified_ledger_pairs'],'active_forecast_pairs':value['consumer']['current_forecast_pairs'],'missing_rows':missing,'interpretation':'AUD_HKD was withheld because its read-only ledger verification reached the per-pair time budget. This is neither a proven expired original target nor a missing-input diagnosis. TRY_JPY and USD_TRY had no verified original publication. The observer does not measure current input readiness. The table withholds the budget row with generic Original ledger forecast withheld; the full endpoint preserves its exact reason.','new_HTTP_requests':0,'live_source_changes':False}
p=BASE/'LIVE_MISSING_LEDGER_REASONS_20260909.json'
with p.open('xb') as f:f.write((json.dumps(out,indent=2,sort_keys=True)+'\n').encode())
print(json.dumps({'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'verified_ledgers':out['verified_ledger_pairs'],'forecasts':out['active_forecast_pairs']}))
