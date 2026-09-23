"""Finite read-only live watch; writes evidence only outside the trading project."""
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from oanda_practice_trial_runner_v1 import CONFIG, STATE, TRIAL_ID, validate_config
from oanda_practice_trial_broker_v1 import PracticeTrialBroker

def write(name, value):
    raw = json.dumps(value, ensure_ascii=True, sort_keys=True).encode() + b'\n'
    with (OUT / name).open('ab') as handle:
        handle.write(raw)

def state_read(name):
    path = STATE / (name + '.json')
    try:
        if path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError('status exceeds size bound')
        raw = path.read_bytes()
        value = json.loads(raw.decode('utf-8-sig'))
        return {'read_completed_epoch': time.time(), 'source_sha256': hashlib.sha256(raw).hexdigest(), 'value': value}
    except Exception as exc:
        return {'read_completed_epoch': time.time(), 'error_type': type(exc).__name__}

def processes():
    command = "Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|powershell' -and $_.CommandLine -match 'forex' } | ForEach-Object { $w = ''; if ($_.CommandLine -match '([A-Za-z0-9_]+\\.(?:py|ps1))') { $w=$matches[1] }; [pscustomobject]@{pid=$_.ProcessId; parent_pid=$_.ParentProcessId; name=$_.Name; script=$w; created=$_.CreationDate.ToUniversalTime().ToString('o')} } | ConvertTo-Json -Depth 3 -Compress"
    cp = subprocess.run([os.environ['SystemRoot'] + r'\System32\WindowsPowerShell\v1.0\powershell.exe', '-NoProfile', '-Command', command], capture_output=True, text=True, timeout=25, creationflags=subprocess.CREATE_NO_WINDOW)
    if cp.returncode:
        return {'error': 'process_inventory_failed', 'returncode': cp.returncode}
    return {'rows': json.loads(cp.stdout or '[]'), 'completed_epoch': time.time()}

config = validate_config(CONFIG, require_enabled=True)
broker = PracticeTrialBroker.from_credentials(ROOT / 'creds', expected_account_sha256=config['account_sha256'], trial_id=TRIAL_ID)
start = time.time()
mono_start = time.monotonic()
duration = 1800.0
deadline = start + duration
metadata = {'started_epoch': start, 'deadline_epoch': deadline, 'duration_sec': duration,
            'started_utc': dt.datetime.fromtimestamp(start, dt.timezone.utc).isoformat(),
            'deadline_utc': dt.datetime.fromtimestamp(deadline, dt.timezone.utc).isoformat(),
            'scope': 'GET-only broker and process/status observations; all writes in separate evidence directory.',
            'config_sha256': hashlib.sha256(CONFIG.read_bytes()).hexdigest()}
(OUT / 'WATCH_WINDOW.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
print(json.dumps({'event': 'watch_started', **metadata}), flush=True)
next_slow = -1.0
cursor = '2461'
last_summary = None
iteration = 0
try:
    while True:
        loop = time.monotonic()
        elapsed = loop - mono_start
        final = elapsed >= duration
        iteration += 1
        runtime = {name: state_read(name) for name in ['status', 'watchdog', 'bootstrap']}
        record = {'epoch': time.time(), 'elapsed_sec': elapsed, 'iteration': iteration, 'runtime': runtime}
        if elapsed >= next_slow or final:
            record['processes'] = processes()
            try:
                account = broker.account_summary()
                trades = broker.open_trades()
                orders = broker.pending_orders()
                transactions = broker.transactions_since(cursor)
                record['broker'] = {'account': account, 'open_trades': trades, 'pending_orders': orders,
                                    'transactions': transactions, 'since_id': cursor,
                                    'baseline_history': iteration == 1}
                cursor = transactions['lastTransactionID']
                summary = {'event': 'broker_observation', 'epoch': time.time(), 'elapsed_sec': round(elapsed, 1),
                           'balance': account['balance'], 'NAV': account['NAV'], 'open_trades': len(trades['rows']),
                           'pending_orders': len(orders['rows']), 'transaction_cursor': cursor,
                           'new_transaction_count': len(transactions['rows']), 'baseline_history': iteration == 1,
                           'trades': [{'id': r['id'], 'instrument': r['instrument'], 'units': r['currentUnits'],
                                       'stop_state': (r.get('stopLossOrder') or {}).get('state')} for r in trades['rows']]}
                print(json.dumps(summary), flush=True)
            except Exception as exc:
                record['broker_error'] = {'type': type(exc).__name__, 'code': getattr(exc, 'code', None)}
                print(json.dumps({'event': 'broker_read_error', **record['broker_error']}), flush=True)
            next_slow = elapsed + 120
        status = runtime['status'].get('value', {})
        compact = {k: status.get(k) for k in ['enabled', 'state', 'pid', 'entry_halt_latched', 'loss_stop_latched', 'stop_requested', 'unresolved_intents']}
        compact.update(entries_today=status.get('session', {}).get('entries_today'), day_utc=status.get('session', {}).get('day_utc'))
        if compact != last_summary:
            print(json.dumps({'event': 'status_change', 'epoch': time.time(), **compact}), flush=True)
            last_summary = compact
        write('root_observations.jsonl', record)
        progress = {'observed_epoch': time.time(), 'elapsed_sec': time.monotonic()-mono_start,
                    'remaining_sec': max(0, duration-(time.monotonic()-mono_start)), 'iteration': iteration,
                    'status': compact, 'last_transaction_id': cursor, 'complete': final}
        (OUT / 'progress.json').write_text(json.dumps(progress), encoding='utf-8')
        if final:
            break
        time.sleep(max(0, min(30 - (time.monotonic()-loop), duration-(time.monotonic()-mono_start))))
finally:
    broker.close()
print(json.dumps({'event': 'watch_complete', 'completed_epoch': time.time(), 'elapsed_sec': time.monotonic()-mono_start, 'iterations': iteration}), flush=True)
