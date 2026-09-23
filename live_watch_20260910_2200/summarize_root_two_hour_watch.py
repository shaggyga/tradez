"""Summarize retained finite observer evidence; never contact the broker."""
import datetime as dt
import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

out = Path(__file__).resolve().parent
progress = json.loads((out / 'progress_extension.json').read_text(encoding='utf-8'))
if not progress.get('complete'):
    raise SystemExit('The requested observation window has not completed.')
window = json.loads((out / 'WATCH_WINDOW_EXTENDED_120MIN.json').read_text(encoding='utf-8'))
logs = [out / 'root_observations.jsonl', out / 'root_extension_observations.jsonl']
log_bindings = [{'path': str(p), 'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in logs]
rows = sorted([json.loads(line) for p in logs for line in p.read_bytes().splitlines() if line.strip()], key=lambda r: r['epoch'])
brokers = [r for r in rows if 'broker' in r]
broker_errors = [r for r in rows if 'broker_error' in r]
statuses = [(r['epoch'],r['runtime']['status']['value']) for r in rows if 'value' in r.get('runtime',{}).get('status',{})]
read_errors = [{'epoch':r['epoch'],'component':k,'error_type':v.get('error_type')} for r in rows for k,v in r.get('runtime',{}).items() if 'value' not in v]
pid_transitions = []
for epoch,s in statuses:
    if not pid_transitions or pid_transitions[-1]['pid'] != s.get('pid'):
        pid_transitions.append({'epoch':epoch,'utc':dt.datetime.fromtimestamp(epoch,dt.timezone.utc).isoformat(),'pid':s.get('pid')})
first,last = brokers[0],brokers[-1]
summary = {
    'schema':'forex_root_live_watch_two_hours_summary_20260911',
    'scope':'Completed finite root observer: periodic local state/process observations and read-only broker requests. No new order, policy change, source deployment or automation.',
    'window':window,'final_progress':progress,
    'logs': log_bindings,
    'samples':len(rows),'observer_versions':dict(Counter(r.get('observer_version','root_v1') for r in rows)),
    'first_sample_utc':dt.datetime.fromtimestamp(rows[0]['epoch'],dt.timezone.utc).isoformat(),
    'last_sample_utc':dt.datetime.fromtimestamp(rows[-1]['epoch'],dt.timezone.utc).isoformat(),
    'maximum_sample_gap_sec':max(b['epoch']-a['epoch'] for a,b in zip(rows,rows[1:])),
    'broker_observations':len(brokers),'broker_read_errors':broker_errors,'local_read_errors':read_errors,
    'first_account':first['broker']['account'],'last_account':last['broker']['account'],
    'balance_change_during_watch':str(Decimal(last['broker']['account']['balance'])-Decimal(first['broker']['account']['balance'])),
    'nav_change_during_watch':str(Decimal(last['broker']['account']['NAV'])-Decimal(first['broker']['account']['NAV'])),
    'maximum_open_trades':max(len(r['broker']['open_trades']['rows']) for r in brokers),
    'maximum_pending_orders':max(len(r['broker']['pending_orders']['rows']) for r in brokers),
    'new_transaction_rows_after_baseline':sum(len(r['broker']['transactions']['rows']) for r in brokers if not r['broker'].get('baseline_history')),
    'baseline_history_transaction_rows':sum(len(r['broker']['transactions']['rows']) for r in brokers if r['broker'].get('baseline_history')),
    'worker_pid_observations':pid_transitions,
    'final_status':statuses[-1][1],
    'limits':['Periodic samples cannot establish every between-sample transient.','Initial transaction batch is historical context, not trades during this watch.','Local source/event receipts identify restart/error chronology; broker flatness does not diagnose Windows errors.','No new accuracy evaluation was run; publication/outcome counts belong to sibling forecast evidence.'],
}
dest = out / 'ROOT_TWO_HOUR_WATCH_COMPLETED_SUMMARY_20260911.json'
dest.write_text(json.dumps(summary,indent=2,ensure_ascii=True)+'\n',encoding='utf-8')
print(json.dumps({k:summary[k] for k in ('samples','broker_observations','maximum_sample_gap_sec','balance_change_during_watch','maximum_open_trades','maximum_pending_orders','new_transaction_rows_after_baseline','worker_pid_observations')}))
print(json.dumps({'summary':str(dest),'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),'local_read_errors':len(read_errors),'broker_read_errors':len(broker_errors)}))
