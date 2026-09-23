param()
$ErrorActionPreference = 'Stop'
$profile = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'PROFILE.json') -Raw | ConvertFrom-Json
$runtimeDirectory = Join-Path $PSScriptRoot 'runtime'
if (Test-Path -LiteralPath (Join-Path $runtimeDirectory 'STOP')) {
    throw 'The passive collector STOP marker is present. Review it before restarting.'
}
$env:FOREX_ALLOW_LIVE = '0'
$env:FOREX_LIVE_EXECUTE = '0'
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssfffZ')
$process = Start-Process -FilePath $profile.python `
    -ArgumentList @((Join-Path $PSScriptRoot 'src\meter_supervisor_v1.py')) `
    -WorkingDirectory $PSScriptRoot -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $runtimeDirectory ('launcher_' + $stamp + '.out.log')) `
    -RedirectStandardError (Join-Path $runtimeDirectory ('launcher_' + $stamp + '.err.log')) `
    -PassThru
[pscustomobject]@{StartedUtc=(Get-Date).ToUniversalTime().ToString('o');ProcessId=$process.Id;Scope='passive_currency_meter_only';UpstreamServicesStarted=$false;BrokerWorkerStarted=$false} | ConvertTo-Json
