"""Bounded read-only local artifact/process assessment; no broker/audit execution."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time

BASE = Path('C:/Users/zmoor/Documents/forex')
PROJECT = BASE / 'trad'
DATA = PROJECT / 'data/oanda_training_manager'
OUT = Path(__file__).resolve().parent
FLAGS = ('can_place_orders', 'can_promote', 'can_authorize', 'account_eligible', 'proof_eligible', 'historical_rows_imported')
sources = {}
artifacts = {}
started = time.time()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def epoch(value):
    if isinstance(value, (float, int)):
        return float(value)
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        assert dt.tzinfo is not None
        return dt.timestamp()
    return None


def load(relative, *, limit=2*1024*1024):
    path = DATA / relative
    before = path.stat()
    with path.open('rb') as f:
        raw = f.read(limit + 1)
    after = path.stat()
    observed = time.time()
    assert len(raw) <= limit
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    value = json.loads(raw)
    artifacts[relative] = {'path': str(path), 'sha256': digest(raw), 'bytes': len(raw),
                           'observed_epoch': observed}
    return value, observed


def select(value, names):
    return {name: value.get(name) for name in names}


for name in ('oanda_project_runtime_health.py', 'oanda_project_integrity_audit.py',
             'oanda_always_on_supervisor.ps1', 'oanda_account_snapshot_writer.py',
             'oanda_practice_live_dashboard.py'):
    path = PROJECT / name
    sources[name] = {'path': str(path), 'sha256': digest(path.read_bytes())}

spec = importlib.util.spec_from_file_location('assessment_runtime_health', PROJECT / 'oanda_project_runtime_health.py')
health = importlib.util.module_from_spec(spec)
spec.loader.exec_module(health)
runtime = health.read_supervisor_observation(DATA / 'logs')
assert sources['oanda_project_runtime_health.py']['sha256'] == digest((PROJECT / 'oanda_project_runtime_health.py').read_bytes())
supervision = select(runtime, ('schema_version', 'status', 'observed_epoch', 'generated_epoch', 'supervisor_age_sec',
                              'running_worker_count', 'expected_worker_count', 'reasons', 'output_unchecked_workers', 'source', 'scope'))
supervision['workers'] = {name: {**select(row, ('running', 'pids', 'supervisor_check_ok', 'explicitly_inactive')),
                               'freshness': select(row['freshness'], ('fresh', 'age_sec', 'reason'))}
                          for name, row in runtime.get('workers', {}).items()}

audit, audit_observed = load('state/project_integrity_audit_v1.json')
integrity = select(audit, ('schema_version', 'generated_utc', 'audit_started_utc', 'audit_finished_utc', 'status',
                         'checks', 'failures', 'check_scopes', 'inactive_component_failures',
                         'active_shared_or_unknown_failures', 'live_runtime_assertions', 'publication_status',
                         'publication_latency_sec', 'maximum_publication_latency_sec', 'snapshot_fresh_at_publication',
                         'research_only', 'can_place_orders', 'can_promote', 'real_money_routing'))
integrity['observed_epoch'] = audit_observed
integrity['age_sec'] = audit_observed - epoch(audit['generated_utc'])
integrity['current_age_under_180_sec'] = 0 <= integrity['age_sec'] < 180
integrity['check_counts'] = {'all': len(audit['checks']), 'passed': sum(x is True for x in audit['checks'].values()),
                            'failed': sum(x is False for x in audit['checks'].values())}
integrity['stored_runtime_generated_epoch'] = audit.get('runtime_health', {}).get('generated_epoch')
integrity['rescope_against_current_supervisor'] = health.scope_integrity_checks(audit['checks'], runtime)

account, account_observed = load('state/account_dashboard_v1.json')
account_age = account_observed - epoch(account['time'])
aggregate = account.get('aggregate', {})
accounts = []
for row in account.get('accounts', []):
    verified = epoch(row.get('verified_at_utc'))
    current = (row.get('ok') is True and row.get('positions_current') is True
               and row.get('orders_current') is True and verified is not None
               and 0 <= account_age < 90 and 0 <= account_observed-verified < 90)
    trades = row.get('trades')
    accounts.append({'ok': row.get('ok'), 'verified_at_utc': row.get('verified_at_utc'),
                     'verified_age_sec': account_observed-verified if verified else None,
                     'positions_orders_observed_current': current,
                     'open_trade_count': row.get('openTradeCount') if current else None,
                     'pending_order_count': row.get('pendingOrderCount') if current else None,
                     'trade_rows_count': len(trades) if current and isinstance(trades, list) else None,
                     'open_position_pair_count': len({t.get('instrument') for t in trades}) if current and isinstance(trades, list) else None})
account_summary = {'snapshot_time_utc': account['time'], 'observed_epoch': account_observed, 'age_sec': account_age,
                   'freshness_threshold_sec': 90, 'environment': account.get('environment'),
                   'aggregate': select(aggregate, ('account_count', 'ok_count', 'snapshot_state', 'positions_current', 'orders_current')),
                   'accounts_without_identifiers': accounts,
                   'privacy': 'No account IDs, NAV/balance, transaction IDs, trade IDs, credentials or individual positions retained.'}

worker, worker_observed = load('joint_price_news_study_v3/heartbeat.json')
worker_summary = select(worker, ('schema_version', 'generated_epoch', 'phase', 'pair_count', 'family_count',
                                'pairs_with_forecast', 'counts', 'errors', 'heartbeat_publication_errors', 'registry_sha256',
                                'summary_sha256', 'research_only', *FLAGS))
worker_summary.update(observed_epoch=worker_observed, age_sec=worker_observed-worker['generated_epoch'])
news, news_observed = load('local_news_sentiment_repair_v1/heartbeat.json')
news_summary = select(news, ('schema_version', 'status', 'source_status', 'phase', 'generated_epoch', 'generated_utc',
                            'snapshot_generated_utc', 'publication_epoch', 'snapshot_sha256', 'errors',
                            'research_only', 'execution_eligible', *FLAGS))
news_summary.update(observed_epoch=news_observed, age_sec=news_observed-news['generated_epoch'])

registry_path = PROJECT / 'config/joint_price_news_study_v3_20260908.json'
registry_raw = registry_path.read_bytes()
registry = json.loads(registry_raw)
contract_count = 0
bad_flags = []
for pair, value in registry['pairs'].items():
    for family, item in value['families'].items():
        contract_count += 1
        contract = item['contract']
        if contract.get('research_only') is not True or any(contract.get(f) is not False for f in FLAGS):
            bad_flags.append(pair + ':' + family)
bindings_ok = {name: digest((PROJECT/name).read_bytes()) == value for name, value in registry['source_bindings'].items()}
registry_summary = {'path': str(registry_path), 'sha256': digest(registry_raw), 'registry_id': registry.get('registry_id'),
                    'pair_count': len(registry['pairs']), 'contract_count': contract_count,
                    'registry_flags': select(registry, ('research_only', *FLAGS)), 'unsafe_contracts': bad_flags,
                    'source_binding_checks': bindings_ok}

clock_state, clock_observed = load('state/clock_integrity_v1.json')
clock_summary = select(clock_state, ('generated_utc', 'status', 'host_clock_synchronized', 'timestamp_normalization_trusted',
                                   'source_fresh', 'source_age_sec', 'clock_sources_consistent', 'clock_discontinuity_active',
                                   'broker_clock_lead_sec', 'research_only', 'can_place_orders', 'changes_system_time'))
clock_summary.update(observed_epoch=clock_observed, age_sec=clock_observed-epoch(clock_state['generated_utc']))
storage, storage_observed = load('state/storage_headroom_v1.json')
storage_summary = select(storage, ('generated_utc', 'status', 'disk', 'totals', 'growth_projection', 'database_inventory',
                                 'automatic_evidence_deletion', 'automatic_vacuum', 'research_only', 'can_place_orders', 'can_promote'))
storage_summary.update(observed_epoch=storage_observed, age_sec=storage_observed-epoch(storage['generated_utc']))

# Process command lines are examined in memory only. The output contains just
# identity metadata and matching public source-module names; never full arguments.
ps = r'''$rows = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|powershell|pwsh' } | ForEach-Object {
    $mods = @([regex]::Matches([string]$_.CommandLine, 'oanda_[A-Za-z0-9_]+\.(?:py|ps1)') | ForEach-Object { $_.Value } | Select-Object -Unique)
    if ($mods.Count -gt 0) { [pscustomobject]@{pid=$_.ProcessId; parent_pid=$_.ParentProcessId; executable_name=$_.Name; created_utc=$_.CreationDate.ToUniversalTime().ToString('o'); modules=$mods} }
}); ConvertTo-Json -InputObject $rows -Depth 5 -Compress'''
result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', ps], capture_output=True, text=True, check=True)
processes = json.loads(result.stdout)
pid_map = {row['pid']: row for row in processes}
active = {name: row for name, row in supervision['workers'].items() if row['running']}
os_checks = {name: {'reported_pids': row['pids'], 'all_pids_observed': all(pid in pid_map for pid in row['pids']),
                    'observed_processes': [pid_map[pid] for pid in row['pids'] if pid in pid_map]}
             for name, row in active.items()}
forbidden = [row for row in processes if any(re.search('executor|auto_promotion|lane_promotion|authorization', name)
                                             for name in row['modules'])]

value = {'schema_version': 'program_operational_assessment_readonly_v1_20260909',
         'capture_started_epoch': started, 'capture_finished_epoch': time.time(),
         'scope': 'Local retained artifacts, bounded fresh supervisor observation and one independent OS process inventory. No broker requests, worker starts/stops, source edits, heavy audit or database queries.',
         'source_bindings': sources, 'observed_artifact_bindings': artifacts,
         'supervision': supervision, 'os_process_identity_checks': os_checks,
         'observed_execution_promotion_authorization_processes': forbidden,
         'integrity': integrity, 'account': account_summary, 'joint_v3_worker': worker_summary,
         'repaired_news_producer': news_summary, 'registered_research_controls': registry_summary,
         'clock': clock_summary, 'storage': storage_summary,
         'limits': ['Supervisor health and recent heartbeats do not prove advancing publications or predictive quality.',
                    'Integrity booleans are artifact checks at their original audit clock; inactive failures remain failed.',
                    'This does not establish profitable forecasts, promotion eligibility or live trading readiness.',
                    'No model ledger outcomes were independently rescored in this bounded operational inspection.']}
encoded = (json.dumps(value, indent=2, ensure_ascii=False) + '\n').encode('utf-8')
assert not re.search(rb'\b\d{3}-\d{3}-\d{7}-\d{3}\b', encoded)
target = OUT / 'OPERATIONAL_CURRENT_ASSESSMENT_20260909.json'
with target.open('xb') as f:
    f.write(encoded)
print(json.dumps({'path': str(target), 'sha256': digest(encoded), 'bytes': len(encoded),
                  'observed_epoch': value['capture_finished_epoch'], 'supervision': supervision['status'],
                  'running': supervision.get('running_worker_count'), 'os_matches': sum(r['all_pids_observed'] for r in os_checks.values()),
                  'active_failures': integrity['active_shared_or_unknown_failures'], 'inactive_failures': len(integrity['inactive_component_failures']),
                  'audit_age_sec': integrity['age_sec'], 'account': account_summary, 'joint_v3_worker': worker_summary,
                  'news': news_summary, 'clock': clock_summary,
                  'storage_status': storage_summary['status'], 'forbidden_process_count': len(forbidden)}, indent=2))
