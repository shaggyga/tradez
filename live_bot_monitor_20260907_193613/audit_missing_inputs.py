"""One-shot current missing-forecast input audit; no fitting or issuance."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import hashlib,json,sys,time
from urllib.request import urlopen
sys.dont_write_bytecode=True
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad';DATA=ROOT/'data/oanda_training_manager'
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
from oanda_causal_forecast_inputs_pair_v1 import capture_inputs,_source_rows
from oanda_pair_local_models_v1 import _training_epochs
from oanda_pair_local_forecast_study_v1 import load_registry,read_snapshot,pair_quote
def iso(v):return datetime.fromtimestamp(v,timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def json_source(p):
    raw=p.read_bytes();return json.loads(raw),{'path':str(p),'sha256':sha(raw),'observed_epoch':time.time()}
def main():
    begun=time.time();registry=load_registry(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
    bindings={name:sha((ROOT/name).read_bytes()) for name in registry['source_bindings']}
    started=time.time()
    with urlopen('http://127.0.0.1:8765/api/main',timeout=5) as response:raw=response.read(16*1024*1024+1)
    if len(raw)>16*1024*1024:raise ValueError('api_body_bound')
    api=json.loads(raw);api_observed=time.time();forecast=api['pair_local_forecasts']
    api_rows={row['instrument']:row for row in forecast['rows']}
    if set(api_rows)!=set(registry['pairs']):raise ValueError('api_pair_universe_mismatch')
    quotes,quote_observed,quote_hash=read_snapshot(DATA/'state/practice_007_market_quotes_v1.json')
    cycle,cycle_binding=json_source(DATA/'state/all68_m1_forward_update_v1.json')
    cycle_pairs={p['instrument']:p for p in cycle['pairs']}
    prior_path=OUT/'COLLECTION_20260907_2002.json'
    prior=load(prior_path);prior_rows={r['instrument']:r for r in prior['archive_pairs']}
    results=[]
    for pair,item in registry['pairs'].items():
        cap=capture_inputs(DATA/'candles',pair,pip_size=item['pip_size'])
        row={'instrument':pair,'api_row':api_rows[pair],
             'current_capture_status':cap['status'],'current_capture_reasons':cap['reasons'],
             'capture_observed_epoch':cap.get('first_observed_epoch'),'capture_sha256':cap['source_capture_sha256']}
        try:
            own=cap['sources'][pair];rows=_source_rows(own,own['first_observed_epoch'],pair);reference=int(max(rows))
            suffix=0
            while reference-suffix*60 in rows:suffix+=1
            missing=[iso(t) for t in range(reference-3600,reference+1,60) if t not in rows]
            training=_training_epochs(rows,reference)
            row.update({'source_read_status':'coherent','source_sha256':own['captured_bytes_sha256'],
                'reference_start_utc':iso(reference),'reference_start_epoch':reference,
                'last_completed_bar_age_seconds':cap['first_observed_epoch']-reference-60,
                'current_own_consecutive_bars':suffix,'missing_of_last61_slots':len(missing),
                'missing_last61_minute_starts_utc':missing,'mature_ridge_training_rows':len(training),
                'retained_real_rows':len(rows),'all_minimum_input_conditions_now':cap['status']=='ready' and len(training)>=24,
                'capture_2002_consecutive_bars':prior_rows[pair].get('own_consecutive_real_bars'),
                'capture_2002_reference_start_utc':prior_rows[pair].get('latest_minute_start_utc')})
        except Exception as exc:row.update(source_read_status='failed',source_read_reason=type(exc).__name__+':'+str(exc))
        try:
            q=pair_quote(quotes,quote_observed,quote_hash,pair,item['pip_size'])
            row.update(quote_now_status='eligible',quote_now_age_seconds=quote_observed-q['market_epoch'])
        except Exception as exc:row.update(quote_now_status='ineligible',quote_now_reason=type(exc).__name__+':'+str(exc))
        gap=cycle_pairs[pair].get('gap_recovery',{})
        row['latest_updater']={k:cycle_pairs[pair].get(k) for k in ('before_last','after_last','rows_appended','error')}
        row['latest_updater']['gap']={k:gap.get(k) for k in ('status','unresolved_minutes','requests','rows_recovered','requested_minute_utc','response_observed_utc','observation_receipt','error')}
        receipt=gap.get('observation_receipt')
        if receipt:
            value,binding=json_source(Path(receipt));response=value['response']
            actual=sha(json.dumps(response,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
            returned=[datetime.fromisoformat(c['time'].replace('Z','+00:00')).timestamp() for c in response.get('candles',[])]
            row['verified_retained_provider_receipt']={**binding,'response_hash_matches':actual==value['response_sha256'],
                'requested_minute_utc':iso(value['candle_epoch']),'response_observed_utc':value['response_observed_utc'],
                'requested_minute_returned':value['candle_epoch'] in returned,'broker_error':bool(response.get('_error')),
                'requested_minute_in_current_missing61':iso(value['candle_epoch']) in row.get('missing_last61_minute_starts_utc',[])}
        results.append(row)
    groups={}
    for row in results:
        reason=row['api_row'].get('reason') or ''
        if row['api_row']['status']=='forecast':group='displayed_active_forecast'
        elif 'current_common_warmup' in reason:group='own_61_contiguous_minute_requirement'
        elif any(s in reason for s in ('no_fresh_quote','no_pair_quote','retained_quote','market_closed_or_no_stream_quote')):group='fresh_quote_requirement'
        elif 'ridge' in reason:group='ridge_model_unavailable'
        elif 'stale_or_future_pair_reference' in reason:group='stale_archive_reference'
        else:group='other:'+reason
        groups.setdefault(group,[]).append(row['instrument'])
    ready_missing=[r['instrument'] for r in results if r['api_row']['status']!='forecast' and r.get('all_minimum_input_conditions_now') and r['quote_now_status']=='eligible']
    after={name:sha((ROOT/name).read_bytes()) for name in bindings}
    assert after==bindings
    result={'schema':'missing_pair_forecast_input_diagnosis_v1_20260907','status':'captured','started_utc':iso(begun),'finished_utc':iso(time.time()),
      'api':{'request_started_epoch':started,'observed_epoch':api_observed,'response_sha256':sha(raw),
             'time':api.get('time'),'pair_forecast_summary':forecast,'market_overview':api['market_overview']},
      'quote_snapshot':{'observed_epoch':quote_observed,'sha256':quote_hash,'eligibility_rule_seconds':60},
      'latest_updater_cycle':{**cycle_binding,'summary':{k:v for k,v in cycle.items() if k not in ('pairs','source')}},
      'previous_collection_snapshot':{'path':str(prior_path),'sha256':sha(prior_path.read_bytes()),'observed_utc':prior['observed_utc']},
      'registered_source_hashes_unchanged':bindings,'api_group_pairs':groups,'api_group_counts':{k:len(v) for k,v in groups.items()},
      'ready_now_but_no_displayed_forecast':ready_missing,
      'all_pair_input_condition_counts':dict(Counter('input_minima_met' if r.get('all_minimum_input_conditions_now') else 'input_minima_not_met' for r in results)),
      'read_failures':sum(r['source_read_status']!='coherent' for r in results),'rows':results,
      'forecast_computations':0,'forecasts_published':0,'broker_requests':0,'runtime_changes':False,'orders':0,
      'limitations':['API, quote snapshot and pair captures have explicit separate observation times; they are not a simultaneous global snapshot.',
       'Input minima are necessary eligibility checks, not a guarantee that a numerical fit succeeds or that a model is accurate.',
       'Stored provider responses establish omission only for their named requested minute and observation time.',
       'A current forecast can remain visible until its H1 target even if a later gap now blocks its replacement.']}
    target=OUT/'MISSING_FORECAST_INPUTS_SNAPSHOT_20260907.json'
    with target.open('x',encoding='utf-8') as handle:json.dump(result,handle,indent=2,allow_nan=False)
    print(json.dumps({'path':str(target),'sha256':sha(target.read_bytes()),'api_groups':result['api_group_counts'],
         'group_pairs':groups,'ready_now_missing':ready_missing,'all_minima':result['all_pair_input_condition_counts'],
         'read_failures':result['read_failures'],'started_utc':result['started_utc'],'finished_utc':result['finished_utc']},indent=2))
if __name__=='__main__':main()
