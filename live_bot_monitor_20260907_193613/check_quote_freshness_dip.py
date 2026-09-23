"""Two bounded read-only observations ten seconds apart; no watcher or broker call."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
from urllib.request import urlopen
import hashlib,json,time,sys
sys.dont_write_bytecode=True
OUT=Path(__file__).resolve().parent;ROOT=OUT.parent/'trad'
STATE=ROOT/'data/oanda_training_manager/state'
def iso(v):return datetime.fromtimestamp(v,timezone.utc).isoformat()
def epoch(s):return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p):
    b=p.read_bytes();return json.loads(b),{'path':str(p),'sha256':sha(b),'bytes':len(b),'observed_epoch':time.time()}
def capture():
    q,qb=read(STATE/'practice_007_market_quotes_v1.json');h,hb=read(STATE/'practice_007_quote_stream_heartbeat_v1.json')
    now=qb['observed_epoch'];ages=[];pairs=[]
    for pair,value in sorted(q['quotes'].items()):
        age=now-epoch(value['time']);ages.append(age)
        pairs.append({'instrument':pair,'provider_time':value['time'],'age_sec':round(age,3),
                      'bid':value['bid'],'ask':value['ask'],'tradeable':value.get('tradeable'),
                      'source':value.get('source'),'within60_seconds':0<=age<=60})
    stream=h['details']['stream'];transport=stream.get('research_snapshot_transport',{})
    started=time.time()
    with urlopen('http://127.0.0.1:8765/api/main',timeout=5) as response:api=json.load(response)
    market=api['market_overview'];observed=time.time()
    return {'observed_utc':iso(now),'quote_source':qb,'heartbeat_source':hb,
      'snapshot_generated_utc':q['generated_utc'],'snapshot_age_sec':now-epoch(q['generated_utc']),
      'heartbeat_generated_utc':h['updated_at'],'heartbeat_age_sec':hb['observed_epoch']-epoch(h['updated_at']),
      'worker_pid':h['pid'],'worker_phase':h['phase'],'subscription_instrument_count':h['details']['instrument_count'],
      'stream':{k:stream.get(k) for k in ('connected','quoted_instruments','metadata_instruments','updates','reconnects','connection_generation','quote_observer_errors','last_event_age_sec','clock_sync_status')},
      'transport':{k:transport.get(k) for k in ('enabled','last_error','mirror_error','thread_alive','last_success_age_sec','submitted_generation','written_generation','coalesced_snapshots','pending','writing')},
      'coverage':q['coverage'],'quote_age_groups':{'0_to15':sum(0<=a<=15 for a in ages),'15_to60':sum(15<a<=60 for a in ages),
          '60_to120':sum(60<a<=120 for a in ages),'120_to600':sum(120<a<=600 for a in ages),'over600':sum(a>600 for a in ages),'future':sum(a<0 for a in ages)},
      'freshest_provider_price_age_sec':min(ages),'oldest_provider_price_age_sec':max(ages),
      'within60_tradeable_count':sum(r['within60_seconds'] and r['tradeable'] is True for r in pairs),
      'pairs':pairs,'api':{'observed_epoch':observed,'duration_sec':observed-started,
         'market':{k:market.get(k) for k in ('status','reason','observed_epoch','current_pair_count','quoted_pair_count','quote_max_age_sec','source_quote_epoch','source_quote_sha256')},
         'market_row_status_counts':dict(Counter(r['status'] for r in market['rows'])),
         'forecasts':api['pair_local_forecasts']['counts']}}
def main():
    target=OUT/'QUOTE_FRESHNESS_DIP_DIAGNOSIS_20260907.json'
    if target.exists():raise ValueError('new_evidence_path_required')
    first=capture();time.sleep(10);second=capture()
    old={r['instrument']:r for r in first['pairs']}
    advanced=[r['instrument'] for r in second['pairs'] if epoch(r['provider_time'])>epoch(old[r['instrument']]['provider_time'])]
    prices=[r['instrument'] for r in second['pairs'] if (r['bid'],r['ask'])!=(old[r['instrument']]['bid'],old[r['instrument']]['ask'])]
    unchanged=[r['instrument'] for r in second['pairs'] if r['instrument'] not in advanced]
    transport_active=all(s['stream']['connected'] and s['stream']['quote_observer_errors']==0 and s['transport']['thread_alive'] and not s['transport']['last_error'] and not s['transport']['mirror_error'] for s in (first,second))
    result={'schema':'quote_freshness_dip_readonly_diagnosis_v1_20260907',
      'status':'observed_active_stream_with_uneven_pair_updates' if transport_active and advanced else 'requires_followup',
      'observations':[first,second],'interval_seconds':second['quote_source']['observed_epoch']-first['quote_source']['observed_epoch'],
      'price_update_counter_delta':second['stream']['updates']-first['stream']['updates'],
      'connection_generation_unchanged':first['stream']['connection_generation']==second['stream']['connection_generation'],
      'reconnect_count_unchanged':first['stream']['reconnects']==second['stream']['reconnects'],
      'provider_price_timestamp_advanced_pairs':advanced,'bid_or_ask_changed_pairs':prices,'no_new_pair_price_timestamp':unchanged,
      'last_event_counter_semantics':'Source inspection confirms last_event_age_sec includes HEARTBEAT events, while updates increments only when an actual price payload updates an instrument. Freshest provider quote time and counter delta independently test price delivery.',
      'coverage_semantics':'current_quote_count is coverage within the current stream connection generation; each quote still requires its own age/tradeability test. It does not mean68 fresh prices.',
      'assessment':'Subscription and metadata include68 instruments; low market-current count can occur while the connection keeps delivering new prices for only a subset. Old per-pair provider timestamps are retained transparently, not made fresh by snapshot heartbeat updates.',
      'limitations':['This bounded observation does not prove why any individual provider quote is quiet or rule out selective upstream loss. No new broker request or independent price source was used.',
                     'Three TRY instruments are explicitly nontradeable in the provider snapshot. Do not infer a global outage from their stale last price.',
                     'API and file observations have separate clocks and cache ages; their counts may differ during changing quote arrivals.'],
      'source_inspected':{'path':str(ROOT/'oanda_practice_shadow_strategy_lab.py'),'sha256':sha((ROOT/'oanda_practice_shadow_strategy_lab.py').read_bytes())},
      'broker_requests':0,'restarts':0,'source_changes':False,'runtime_changes':False,'watchers_started':0}
    with target.open('x',encoding='utf-8') as handle:json.dump(result,handle,indent=2,allow_nan=False)
    print(json.dumps({'path':str(target),'sha256':sha(target.read_bytes()),'status':result['status'],
       'times':[s['observed_utc'] for s in (first,second)],'price_update_counter_delta':result['price_update_counter_delta'],
       'advanced_pair_count':len(advanced),'changed_bid_ask_pair_count':len(prices),
       'observations':[{k:s[k] for k in ('quote_age_groups','within60_tradeable_count','freshest_provider_price_age_sec','snapshot_age_sec','heartbeat_age_sec','stream','api')} for s in (first,second)]},indent=2))
if __name__=='__main__':main()
