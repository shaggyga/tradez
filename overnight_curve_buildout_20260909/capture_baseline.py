"""Preserve a bounded source baseline before the overnight add-only changes."""
from pathlib import Path
from datetime import datetime, timezone
from hashlib import sha256
import json
import subprocess

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent / 'trad'

def digest(p):
    return sha256(p.read_bytes()).hexdigest()

def main():
    destination = HERE/'baseline'
    destination.mkdir(exist_ok=False)
    registry = PROJECT/'config/joint_price_news_study_v3_20260908.json'
    reg = json.loads(registry.read_bytes())
    paths = set(reg['source_bindings'])
    paths.update([
        'config/joint_price_news_study_v3_20260908.json',
        'config/joint_forecast_primary_current.json',
        'oanda_second_forecast.py', 'oanda_second_forecast_fit.py',
        'oanda_signal_contribution_feed.py', 'oanda_lane_promotion.py',
        'oanda_practice_shadow_strategy_lab.py', 'oanda_practice_top_signal_executor.py',
        'oanda_execution_policy.py', 'oanda_always_on_supervisor.ps1',
        'oanda_project_runtime_health.py', 'oanda_storage_headroom_guard.py',
        'oanda_project_integrity_audit.py', 'oanda_joint_price_news_models_v1.py',
        'src/forex_system/research/sequential_portfolio_replay_v1.py',
        'src/forex_system/research/sequential_all68_policy_challenger_v1.py',
        'src/forex_system/research/sequential_all68_policy_expansion_v1.py',
        'FOREX_PENDING_IMPROVEMENTS.md', 'FOREX_PROJECT_LOG.md', 'README.md',
        'forex_model_vault_sync.py', 'docs/VAULT_SYSTEM_GUIDE.md',
    ])
    records = []
    for name in sorted(paths):
        source = PROJECT/name
        payload = source.read_bytes()
        target = destination/'sources'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as f:
            f.write(payload)
        actual = sha256(payload).hexdigest()
        assert digest(source) == digest(target) == actual
        if name in reg['source_bindings']:
            assert actual == reg['source_bindings'][name]
        records.append({'source': name, 'copy': str(target.relative_to(HERE)),
                        'size': len(payload), 'sha256': actual})
    for name, args in [('git_status_porcelain.txt', ['status', '--porcelain=v1']),
                       ('existing_git_diff.patch', ['diff', '--binary'])]:
        result = subprocess.run(['git', *args], cwd=PROJECT, capture_output=True, check=True)
        with (destination/name).open('xb') as f:
            f.write(result.stdout)
    state = {'schema_version': 'overnight_curve_baseline_20260909',
             'captured_utc': datetime.now(timezone.utc).isoformat(),
             'deadline_utc': '2026-09-09T13:00:00Z',
             'registered_sources_checked': len(reg['source_bindings']),
             'source_count': len(records), 'records': records,
             'git_status_sha256': digest(destination/'git_status_porcelain.txt'),
             'existing_diff_sha256': digest(destination/'existing_git_diff.patch'),
             'scope': 'Existing selected dependencies and code/doc bytes. Not a full runtime or database backup.',
             'runtime_changed': False, 'broker_actions': False}
    with (destination/'BASELINE_MANIFEST.json').open('x', encoding='utf-8') as f:
        json.dump(state, f, indent=2, allow_nan=False)
        f.write('\n')
    print(json.dumps({'status': 'passed', 'sources': len(records),
                      'registered_sources': len(reg['source_bindings']),
                      'manifest_sha256': digest(destination/'BASELINE_MANIFEST.json')}))

if __name__ == '__main__':
    main()
