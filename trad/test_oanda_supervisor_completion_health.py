"""Exercise the real inert PowerShell health reader without supervisor actions."""
import json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parent


def read_health(tmp_path, payload):
    source = (ROOT / 'oanda_always_on_supervisor.ps1').read_text(encoding='utf-8-sig')
    functions = source[source.index('function Read-JsonFileWithRetry {'):source.index('function Start-ManagedProcess {')]
    heartbeat = tmp_path / 'heartbeat.json'
    heartbeat.write_text(payload if isinstance(payload, str) else json.dumps(payload))
    script = tmp_path / 'read.ps1'
    script.write_text('param([string]$Heartbeat)\n$ErrorActionPreference="Stop"\n' + functions +
                      '\nTest-FreshOutput -LiteralPath $Heartbeat -InspectOperationalStatus | ConvertTo-Json -Compress\n')
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(script),
                             '-Heartbeat', str(heartbeat)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize('status', ['running', 'starting', 'ok'])
def test_retry_does_not_retire_failed_completion(tmp_path, status):
    result = read_health(tmp_path, dict(status=status, last_failure=dict(observed_epoch=200, reason='capacity'),
                                       last_success_epoch=100))
    assert result['fresh'] is True
    assert result['reported_failure'] is True
    assert result['unrecovered_cycle_failure'] is True


def test_later_success_retires_historical_failure(tmp_path):
    result = read_health(tmp_path, dict(status='running', last_failure=dict(observed_epoch=100, reason='old'),
                                       last_success_epoch=200))
    assert result['reported_failure'] is False
    assert result['unrecovered_cycle_failure'] is False


def test_first_failure_survives_retry_without_any_success(tmp_path):
    result = read_health(tmp_path, dict(status='running', last_failure=dict(observed_epoch=100, reason='first')))
    assert result['reported_failure'] is True


def test_unrelated_worker_without_completion_clocks_remains_valid(tmp_path):
    result = read_health(tmp_path, dict(status='ok', phase='idle'))
    assert result['fresh'] is True
    assert result['reported_failure'] is False


@pytest.mark.parametrize('payload', ['{invalid', dict(status='ok', last_failure=dict(observed_epoch='NaN'))])
def test_unreadable_operational_status_fails_closed(tmp_path, payload):
    result = read_health(tmp_path, payload)
    assert result['fresh'] is False
    assert result['reason'] == 'runtime_contract_unreadable'
