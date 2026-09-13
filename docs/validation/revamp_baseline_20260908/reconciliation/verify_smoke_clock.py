from pathlib import Path
from datetime import datetime,timezone
import hashlib,json
import pyarrow.parquet as pq

work=Path(__file__).resolve().parent
smoke_path=work/'SECOND_RIDGE_ENGINEERING_SMOKE_20260908.json'
smoke=json.loads(smoke_path.read_text())
source=Path(smoke['input_preserved']['path'])
table=pq.read_table(source,columns=['dt'])
def parsed(value):return datetime.fromisoformat(value.replace('Z','+00:00')) if isinstance(value,str) else value
first,last=parsed(table['dt'][0].as_py()),parsed(table['dt'][-1].as_py())
expected_first=datetime.fromisoformat(smoke['input_summary']['first_source_utc'])
expected_last=datetime.fromisoformat(smoke['input_summary']['last_source_utc'])
assert first.timestamp()==expected_first.timestamp() and last.timestamp()==expected_last.timestamp()
target=datetime.fromisoformat(smoke['input_summary']['selected_feature_utc'])
# Decode only the timestamp column. No second feature transformation/model run.
matched=sum(parsed(value).timestamp()==target.timestamp() for value in table['dt'].to_pylist())
assert matched==1
result={'schema':'second_ridge_smoke_clock_check_v1','status':'passed','observed_utc':datetime.now(timezone.utc).isoformat(),
 'smoke_receipt_sha256':hashlib.sha256(smoke_path.read_bytes()).hexdigest(),
 'observed_original_transform_warning':'UserWarning: no explicit representation of timezones available for np.datetime64',
 'warning_source':'isolated oanda_second_forecast_fit.py:192 original column_numpy datetime64 conversion',
 'retained_arrow_timestamp_type':str(table.schema.field('dt').type),'first_arrow_utc':first.isoformat(),'last_arrow_utc':last.isoformat(),
 'first_last_epoch_equal_original_transform':True,'selected_original_timestamp_occurrences':matched,
 'scope':'The NumPy conversion warning concerns the timezone label on retained timestamp text; independently parsed original UTC epochs match transformed endpoints and selected input time. This does not independently attest historical collection availability.',
 'new_model_predictions':0,'new_training_runs':0,'database_writes':0}
out=work/'SECOND_RIDGE_ENGINEERING_CLOCK_CHECK_20260908.json'
with out.open('x',encoding='utf-8',newline='\n') as f:json.dump(result,f,indent=2,sort_keys=True);f.write('\n')
print(json.dumps({'status':'passed','file':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'arrow_type':str(table.schema.field('dt').type)}))
