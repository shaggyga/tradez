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
from shared_windows_read_v1 import read_shared
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
        raw = read_shared(path, 2 * 1024 * 1024 + 1)
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError('status read bound exceeded')
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
metadata = json.loads((OUT / 'WATCH_WINDOW.json').read_text(encoding='utf-8'))
start = metadata['started_epoch']
deadline = metadata['deadline_epoch']
duration = metadata['duration_sec']
mono_start = time.monotonic() - (time.time() - start)
prior = json.loads((OUT / 'progress.json').read_text(encoding='utf-8'))
print(json.dumps({'event': 'observer_resumed_shared_delete', 'epoch': time.time(), **metadata}), flush=True)
next_slow = -1.0
cursor = prior['last_transaction_id']
last_summary = None
iteration = prior['iteration']
try:
    while True:
        loop = time.monotonic()
        elapsed = loop - mono_start
        final = elapsed >= duration
        iteration += 1
        runtime = {name: state_read(name) for name in ['status', 'watchdog', 'bootstrap']}
        record = {'observer_version': 'root_v2_shared_delete', 'epoch': time.time(), 'elapsed_sec': elapsed, 'iteration': iteration, 'runtime': runtime}
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
