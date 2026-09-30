"""Verify the actual subprocess environment guard without exposing real keys."""
from pathlib import Path
import subprocess


def test_optional_provider_keys_removed_but_broker_context_preserved():
    contract = Path(__file__).with_name('oanda_operational_recovery_contract_v6.ps1')
    command = """
    . '%s'
    foreach ($name in @('FRED_API_KEY','ALPHA_VANTAGE_API_KEY','FINNHUB_API_KEY','TRADING_ECONOMICS_API_KEY','TE_API_KEY')) {
        [Environment]::SetEnvironmentVariable($name,'synthetic-fixture','Process')
    }
    $env:OANDA_CREDS_PATH='synthetic-path'
    Clear-OperationalOptionalNewsCredentials
    foreach ($name in @('FRED_API_KEY','ALPHA_VANTAGE_API_KEY','FINNHUB_API_KEY','TRADING_ECONOMICS_API_KEY','TE_API_KEY')) {
        if ([Environment]::GetEnvironmentVariable($name,'Process')) { throw 'credential still inherited' }
    }
    if ($env:OANDA_CREDS_PATH -ne 'synthetic-path') { throw 'broker context changed' }
    Write-Output 'PASS'
    """ % str(contract).replace("'", "''")
    result = subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass',
                             '-Command', command], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'PASS'


def test_joint_recovery_replaces_only_the_isolation_role():
    contract = Path(__file__).with_name('oanda_operational_recovery_contract_v6.ps1')
    command = """
    . '%s'
    $old=Get-OperationalRecoveryServices
    $new=Get-OperationalRecoveryServices -EnableJointForecasts
    if ($old.Count -ne 18 -or $new.Count -ne 18) { throw 'role count changed' }
    if ($new.ContainsKey('joint_price_news_isolation_status_v1')) { throw 'isolation still selected' }
    if ($new['joint_price_news_study_v9'] -ne 'oanda_joint_price_news_forecast_study_v9.py') { throw 'wrong worker' }
    foreach ($name in $old.Keys) {
        if ($name -ne 'joint_price_news_isolation_status_v1' -and $new[$name] -ne $old[$name]) { throw 'unrelated role changed' }
    }
    Write-Output 'PASS'
    """ % str(contract).replace("'", "''")
    result = subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass',
                             '-Command', command], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'PASS'
