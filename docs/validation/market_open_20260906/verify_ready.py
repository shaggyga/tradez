"""Verify observed research startup without opening databases or sending orders."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from urllib.request import urlopen

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
STATE = ROOT / 'data/oanda_training_manager/state'
NOW = time.time()

def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def age(value):
    epoch = float(value) if isinstance(value, (int, float)) else datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
    return time.time() - epoch

observation_name = sys.argv[1] if len(sys.argv) > 1 else 'observation_final_confirmed.json'
output_name = sys.argv[2] if len(sys.argv) > 2 else 'runtime_verified.json'
observation = load(OUT / observation_name)
assert 0 <= age(observation['observed_utc']) < 120
assert 0 <= age(observation['supervisor_heartbeat_utc']) < 120
supervisors = [p for p in observation['processes'] if p['script'] == 'oanda_always_on_supervisor.ps1']
assert len(supervisors) == 1 and supervisors[0]['research_collection_only'] is True
supervisor_pid = supervisors[0]['pid']
expected = {
    'account_snapshot':'oanda_account_snapshot_writer.py',
    'live_dashboard':'oanda_practice_live_dashboard.py',
    'local_news_sentiment':'oanda_local_news_sentiment.py',
    'official_release_fast_lane':'oanda_official_release_fast_lane.py',
    'official_release_fast_mapper':'oanda_official_release_fast_mapper.py',
    'source_governance_news_fast_lane':'oanda_source_governance_news_fast_lane.py',
    'practice_007_quote_stream':'oanda_practice_quote_stream.py',
    'clock_integrity_monitor':'oanda_clock_integrity_monitor.py',
    'all68_m1_forward_archive':'oanda_all68_m1_forward_updater.py',
    'project_integrity_audit':'oanda_project_integrity_audit.py',
    'storage_headroom_guard':'oanda_storage_headroom_guard.py',
    'causal_forecast_study_v1':'oanda_causal_forecast_study.py',
}
managed = observation['managed_running']
assert len(managed) == len(expected) and {p['name'] for p in managed} == set(expected)
processes = {p['pid']:p for p in observation['processes']}
expected_pids = {supervisor_pid}
for row in managed:
    assert row['freshness']['fresh'] is True, row
    assert row['pids'], row
    for pid in row['pids']:
        assert pid in processes and processes[pid]['script'] == expected[row['name']], row
        cursor, seen = pid, set()
        while cursor != supervisor_pid:
            assert cursor not in seen and cursor in processes, row
            seen.add(cursor)
            cursor = processes[cursor]['parent_pid']
        expected_pids.add(pid)
unexpected = [p for pid,p in processes.items() if pid not in expected_pids]
assert not unexpected, unexpected
assert observation['supervisor_error_count'] == 0
assert len(observation['forex_tasks']) == 2 and all(p['state'] == 'Disabled' for p in observation['forex_tasks'])

study = load(ROOT/'data/oanda_training_manager/causal_forecast_study_v1_io_r2/heartbeat.json')
preflight = load(OUT/'preflight.json')
contract_path = ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json'
contract = load(contract_path)
assert sha(contract_path) == preflight['contract_file_sha256']
assert study['contract_sha256'] == preflight['registered_contract_payload_sha256']
assert study['activated_epoch'] == 1788708624.9526446
for name,digest in contract['source_bindings'].items(): assert sha(ROOT/name) == digest, name
assert 0 <= age(study['generated_epoch']) <= 90
assert study['errors'] == 0 and study['heartbeat_publication_errors'] == 0
assert study['can_place_orders'] is False and study['supported_decision'] == 'no_trade'
assert study['phase'] == 'waiting_for_tradeable_quote'
assert study['counts']['forecasts'] == 0 and study['counts']['outcomes'] == 0

quote = load(STATE/'practice_007_quote_stream_heartbeat_v1.json')
stream = quote['details']['stream']
assert 0 <= age(quote['updated_at']) <= 90
assert stream['connected'] is True and stream['quoted_instruments'] == 68
assert stream['last_event_age_sec'] <= 90 and stream['quote_observer_errors'] == 0
assert quote['details']['can_place_orders'] is False
transport = stream['research_snapshot_transport']
assert transport['thread_alive'] is True and not transport['last_error'] and not transport['mirror_error']
prices = load(STATE/'practice_007_market_quotes_v1.json')
quotes = prices['quotes']
assert sum(q.get('tradeable') is True for q in quotes.values()) == 0

account_payload = load(STATE/'account_007_dashboard_v1.json')
account = next(row for row in account_payload['accounts'] if row.get('role') == 'practice_007')
assert 0 <= age(account['verified_at_utc']) <= 90 and account['env'] == 'practice'
assert all(account[flag] is True for flag in ('ok','account_values_current','positions_current','orders_current'))
assert account['openTradeCount'] == 0 and account['pendingOrderCount'] == 0
with urlopen('http://127.0.0.1:8765/',timeout=15) as response:
    page_http = response.status
    assert page_http == 200
with urlopen('http://127.0.0.1:8765/api/main',timeout=30) as response:
    api_http = response.status
    api = json.load(response)
collection = api['collection_status']
assert api_http == 200 and collection['status'] == 'running', collection
assert collection['registered_study_trading_enabled'] is False
assert collection['strategy_or_execution_activation_changed'] is False
assert api['active'] is False
assert collection['warmup']['required_common_m1_bars'] == 335

integrity = load(STATE/'project_integrity_audit_v1.json')
storage = load(STATE/'storage_headroom_v1.json')
clock = load(STATE/'clock_integrity_v1.json')
assert storage['status'] == 'ok'
assert integrity['can_place_orders'] is False and integrity['supported_decision'] == 'no_trade'
result = {
    'schema_version':'forex_market_open_runtime_verification_v1',
    'observed_utc':datetime.now(timezone.utc).isoformat(),
    'scope':'Existing registered research collection; not trading authorization or predictive acceptance.',
    'research_supervisor_count':len(supervisors), 'research_supervisor_pid':supervisor_pid,
    'managed_worker_count':len(managed), 'managed_workers':managed,
    'observed_process_count':len(processes), 'unexpected_project_workers':unexpected,
    'supervisor_error_count':observation['supervisor_error_count'],
    'legacy_tasks':observation['forex_tasks'],
    'quote_stream':{'connected':True,'quoted_instruments':stream['quoted_instruments'],
        'observed_utc':quote['updated_at'],'event_age_sec':stream['last_event_age_sec'],
        'reconnects':stream['reconnects'],'quote_observer_errors':stream['quote_observer_errors'],
        'transport_thread_alive':transport['thread_alive'],
        'published_tradeable_quote_count':sum(q.get('tradeable') is True for q in quotes.values()),
        'published_quote_count':len(quotes)},
    'account':{'current':True,'environment':'practice','role':'practice_007',
        'observed_utc':account['verified_at_utc'],
        'open_trades':account['openTradeCount'],'pending_orders':account['pendingOrderCount'],
        'last_transaction_id':account['lastTransactionID']},
    'dashboard':{'http_status':page_http,'main_api_http_status':api_http,
        'active':api['active'],'collection_status':collection},
    'study':study, 'registered_source_bindings_unchanged':True,
    'integrity':{k:integrity.get(k) for k in ('audit_finished_utc','status','supported_decision','can_place_orders','failures')},
    'storage':{k:storage.get(k) for k in ('generated_utc','status','disk','automatic_evidence_deletion','automatic_vacuum')},
    'clock':{k:clock.get(k) for k in ('generated_utc','status','host_clock_synchronized',
        'broker_clock_lead_sec','broker_clock_sample_count','clock_discontinuity_active','reasons')},
    'observation_binding':{'path':observation_name,'sha256':sha(OUT/observation_name)},
    'limitations':['Current observations cannot guarantee future availability or opening prices.',
        'No trading workers or new offline models were enabled.',
        'The broad integrity audit remains degraded and is not interpreted as a trading pass.'],
}
with (OUT/output_name).open('x',encoding='utf-8') as f:
    json.dump(result,f,indent=2); f.write('\n')
print(json.dumps({'status':'verified','output':output_name,'workers':len(managed),
    'collection_status':collection['status'],'practice_trading_enabled':False,
    'study_phase':study['phase'],'integrity_status':integrity['status']}))
