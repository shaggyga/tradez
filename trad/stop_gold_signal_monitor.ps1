$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$StateDir = Join-Path $Root "data\gold_m1_liquidity_scalper\live_monitor"
$PidFile = Join-Path $StateDir "monitor.pid"

if (!(Test-Path $PidFile)) {
    Write-Output "not_running"
    exit 0
}

$pidText = (Get-Content $PidFile -Raw).Trim()
if (!$pidText) {
    Remove-Item -Path $PidFile -Force
    Write-Output "not_running"
    exit 0
}

$process = Get-Process -Id ([int]$pidText) -ErrorAction SilentlyContinue
if (!$process) {
    Remove-Item -Path $PidFile -Force
    Write-Output "not_running stale_pid=$pidText"
    exit 0
}

Stop-Process -Id $process.Id -Force
Remove-Item -Path $PidFile -Force
Write-Output "stopped pid=$pidText"
