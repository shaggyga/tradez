"""Read-only EUR/USD source/receipt audit; writes only this evidence directory."""
import sys
sys.dont_write_bytecode = True
import base64, csv, hashlib, io, json, math, sqlite3, time, zlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import oanda_all68_m1_forward_updater as updater
from oanda_causal_forecast_inputs import _read_tail
from oanda_causal_forecast_inputs_gap_v2 import _parse_exact_source

def sha(raw): return hashlib.sha256(raw).hexdigest()
def iso(epoch): return datetime.fromtimestamp(epoch, timezone.utc).isoformat()
def stamp(value): return int(datetime.fromisoformat(value.replace('Z','+00:00')).timestamp())
def write(name, value):
    raw=(json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+'\n').encode()
    path=OUT/name
    with path.open('xb') as handle: handle.write(raw)
    return {'path':str(path),'sha256':sha(raw),'bytes':len(raw)}
def json_snapshot(path):
    raw=path.read_bytes()
    return {'path':str(path),'sha256':sha(raw),'observed_epoch':time.time(),'payload':json.loads(raw)}
def window(rows, reference):
    wanted=list(range(reference-60*60,reference+1,60))
    suffix=0
    while reference-suffix*60 in rows: suffix+=1
    return {'reference_start_epoch':reference,'reference_start_utc':iso(reference),
        'last_61_calendar_minute_slots':len(wanted),'present_count':sum(t in rows for t in wanted),
        'missing_minute_starts_utc':[iso(t) for t in wanted if t not in rows],
        'consecutive_real_suffix':suffix,
        'slots':[{'start_epoch':t,'start_utc':iso(t),'present':t in rows,'close':rows.get(t)} for t in wanted]}

begun=time.time()
report={'schema':'eurusd_minute_gap_readonly_audit_v1_20260907','started_epoch':begun,
        'started_utc':iso(begun),'scope':'EUR/USD current and 18:00-attempt continuity plus existing all-68 recovery receipts; no broad coverage recomputation',
        'research_only':True,'can_place_orders':False,'archive_writes':0,'runtime_changes':0,
        'forecast_backfills':0,'credential_values_recorded':False,'read_attempts':[]}
source_names=['oanda_all68_m1_forward_updater.py','oanda_gpt_training_strategy_manager.py',
              'oanda_causal_forecast_inputs.py','oanda_causal_forecast_inputs_gap_v2.py',
              'oanda_causal_forecast_inputs_eurusd_v1.py']
report['source_hashes_before']={n:sha((ROOT/n).read_bytes()) for n in source_names}
state=ROOT/'data/oanda_training_manager'
report['updater_heartbeat']=json_snapshot(state/'state/all68_m1_forward_update_heartbeat_v1.json')
update=json_snapshot(state/'state/all68_m1_forward_update_v1.json')
report['updater_cycle_snapshot']=write('MINUTE_GAP_UPDATER_CYCLE_SNAPSHOT.json',update)
payload=update['payload']
report['updater_cycle_summary']={k:v for k,v in payload.items() if k not in ('pairs','source')}
report['updater_cycle_summary']['gap_status_counts']=dict(Counter(p.get('gap_recovery',{}).get('status') for p in payload['pairs']))
report['updater_cycle_summary']['eurusd']=next(p for p in payload['pairs'] if p['instrument']=='EUR_USD')

path=state/'candles/EUR_USD_M1.csv'
for number in range(1,4):
    at=time.time()
    try:
        capture=_read_tail(path); observed=time.time(); rows=_parse_exact_source(capture,'EUR_USD',observed)
        report['read_attempts'].append({'attempt':number,'begun_epoch':at,'observed_epoch':observed,'status':'coherent'})
        break
    except Exception as exc:
        report['read_attempts'].append({'attempt':number,'begun_epoch':at,'observed_epoch':time.time(),'status':'failed','reason':type(exc).__name__+':'+str(exc)})
        if number==3: raise
report['eurusd_source_snapshot']=write('MINUTE_GAP_EURUSD_SOURCE_CAPTURE.json',{'observed_epoch':observed,**capture})
report['current_local_window']=window(rows,int(max(rows)))
report['current_local_window']['observation_epoch']=observed
report['current_local_window']['last_close_age_seconds']=observed-max(rows)-60

report['eurusd_ledgers']={}
for name,dbpath in [('eurusd_original',state/'causal_forecast_study_eurusd_v1/study.sqlite'),
                    ('pair_local_eurusd',state/'pair_local_forecast_study_v1/pairs/EUR_USD/study.sqlite')]:
    connection=sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True)
    connection.row_factory=sqlite3.Row
    try:
        connection.execute('pragma query_only=on');connection.execute('begin')
        entry={'observed_epoch':time.time(),'path':str(dbpath)}
        entry['attempts']=[dict(r) for r in connection.execute('select * from attempts order by epoch desc limit 8')]
        entry['diagnostics']=[{**dict(r),'payload':json.loads(r['payload'])} for r in connection.execute('select * from diagnostics order by epoch desc limit 8')]
        entry['retained_successful_captures']=[]
        for r in connection.execute('select id,payload from inputs order by rowid desc limit 8'):
            old=json.loads(zlib.decompress(r['payload']))
            entry['retained_successful_captures'].append({k:old.get(k) for k in ('source_capture_sha256','status','first_observed_epoch','current_common_bars','reference_start_epoch','max_bar_close_epoch')})
        report['eurusd_ledgers'][name]=entry
    finally: connection.close()

receipt_root=state/'candles/.gap_recovery_v1'
receipts=sorted(receipt_root.glob('*/*.observed.json'))
if len(receipts)>10000: raise ValueError('receipt audit bound exceeded')
receipt_summary=[]
columns=next(csv.reader([base64.b64decode(capture['header_base64']).decode().strip()]))
eur_receipts=[]
for file in receipts:
    raw=file.read_bytes()
    if len(raw)>256*1024: raise ValueError('receipt size bound exceeded')
    r=json.loads(raw);response=r['response'];epoch=r['candle_epoch'];instrument=r['instrument']
    digest=sha(json.dumps(response,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
    if digest!=r['response_sha256']: raise ValueError('retained response hash mismatch')
    candles=response.get('candles',[])
    matches=[c for c in candles if stamp(c['time'])==epoch]
    verdict='requested_minute_absent' if not matches else 'requested_minute_returned'
    validation_error=None
    if matches:
        try: updater._actual_gap_row(response,instrument,epoch,datetime.fromisoformat(r['response_observed_utc']),columns)
        except Exception as exc: verdict='returned_but_rejected';validation_error=type(exc).__name__+':'+str(exc)
    item={'path':str(file),'sha256':sha(raw),'instrument':instrument,'requested_minute_utc':r['candle_utc'],
          'response_observed_utc':r['response_observed_utc'],'response_sha256':digest,
          'response_candle_count':len(candles),'verdict':verdict,'validation_error':validation_error,
          'broker_error':bool(response.get('_error'))}
    receipt_summary.append(item)
    if instrument=='EUR_USD':
        eur_receipts.append({**item,'returned_minute_starts_utc':[iso(stamp(c['time'])) for c in candles]})
report['recovery_receipt_summary']={'observed_receipts':len(receipts),'pair_count':len({r['instrument'] for r in receipt_summary}),
                                  'verdict_counts':dict(Counter(r['verdict'] for r in receipt_summary)),
                                  'broker_error_count':sum(r['broker_error'] for r in receipt_summary),
                                  'eurusd_receipts':eur_receipts,
                                  'all_receipt_bindings':receipt_summary}

print('Captured local EUR source, attempt ledgers and existing recovery responses. Requesting one bounded EUR/USD practice candle GET.',flush=True)
client,meta=updater.resolve_readonly_oanda_client()
if meta['environment']!='practice' or meta['base_url']!='https://api-fxpractice.oanda.com': raise ValueError('practice-only route required')
request_time=time.time();end=int(request_time//60)*60
response=client.candles('EUR_USD',granularity='M1',count=180,end_time=datetime.fromtimestamp(end,timezone.utc),price='BAM')
response_time=time.time()
if response.get('_error'):
    retained={'_error':True,'_http_status':response.get('_http_status')}
else:
    retained={k:response[k] for k in ('instrument','granularity','candles') if k in response}
request={'method':'GET','route':'/v3/instruments/EUR_USD/candles','base_url':meta['base_url'],
         'params':{'granularity':'M1','price':'BAM','count':180,'to':iso(end)},
         'requested_epoch':request_time,'response_observed_epoch':response_time,
         'request_duration_seconds':response_time-request_time,
         'response':retained,'historical_availability_asserted':False}
report['fresh_broker_response']=write('MINUTE_GAP_EURUSD_FRESH_BROKER_GET.json',request)
if response.get('_error'): raise RuntimeError('bounded fresh broker GET failed; retained only sanitized status')
broker_rows={stamp(c['time']):float(c['mid']['c']) for c in retained['candles'] if c.get('complete') is True and stamp(c['time'])+60<=response_time}
normalized=updater.manager_module().candle_df_from_oanda(retained,'EUR_USD','M1')
normalized_epochs={stamp(s) for s in normalized['datetime']}
common_window=set(range(int(max(rows))-3600,int(max(rows))+1,60))
report['fresh_comparison']={
    'complete_returned_candles':len(broker_rows),'returned_start_utc':iso(min(broker_rows)),'returned_end_utc':iso(max(broker_rows)),
    'normalizer_dropped_complete_minutes':[iso(t) for t in sorted(set(broker_rows)-normalized_epochs)],
    'local_window_missing_but_broker_present':[iso(t) for t in sorted((common_window-set(rows))&set(broker_rows))],
    'local_window_missing_and_broker_absent':[iso(t) for t in sorted(common_window-set(rows)-set(broker_rows))],
    'local_window_present_but_broker_absent':[iso(t) for t in sorted((common_window&set(rows))-set(broker_rows))],
    'broker_newer_than_local':[iso(t) for t in sorted(broker_rows) if t>max(rows)],
    'broker_window_at_local_reference':window(broker_rows,int(max(rows))),
    'broker_current_window':window(broker_rows,max(broker_rows)),
    'price_mismatches':[{'start_utc':iso(t),'local_close':rows[t],'broker_close':broker_rows[t]} for t in sorted(common_window&set(rows)&set(broker_rows)) if rows[t]!=broker_rows[t]]}
after=_read_tail(path)
report['local_snapshot_unchanged_after_get']=capture['captured_bytes_sha256']==after['captured_bytes_sha256']
report['local_snapshot_after_get']={k:v for k,v in after.items() if not k.endswith('_base64')}
report['source_hashes_after']={n:sha((ROOT/n).read_bytes()) for n in source_names}
report['source_hashes_unchanged']=report['source_hashes_before']==report['source_hashes_after']
report['ended_epoch']=time.time();report['duration_seconds']=report['ended_epoch']-begun
report['status']='read_only_observations_complete'
report['limitations']=['Fresh broker responses prove observed-now availability or omission only; they do not establish what the server returned to earlier forward requests.',
    'Broker omission does not prove a minute had zero market activity; no synthetic candles or missing-price inference performed.',
    'All-68 recovery aggregation reads existing receipts and the latest complete cycle, not all current pair source archives.']
receipt=write('MINUTE_GAP_AUDIT.json',report)
print(json.dumps({'status':report['status'],'report':receipt,
    'local_window':{k:v for k,v in report['current_local_window'].items() if k!='slots'},
    'fresh_comparison':{k:v for k,v in report['fresh_comparison'].items() if 'window' not in k},
    'all68_verdict_counts':report['recovery_receipt_summary']['verdict_counts']},indent=2))
