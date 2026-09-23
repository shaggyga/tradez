param(
    [int]$IntervalSeconds = 60,
    [switch]$RelaxedScan
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = Split-Path -Parent $Root
$Python = Join-Path $Root "data\oanda_training_manager\.research_py313\Scripts\python.exe"
$Script = Join-Path $Root "gold_signal_monitor.py"
$StateDir = Join-Path $Root "data\gold_m1_liquidity_scalper\live_monitor"
$LogDir = Join-Path $Root "data\gold_m1_liquidity_scalper\logs"
$PidFile = Join-Path $StateDir "monitor.pid"
$OutLog = Join-Path $LogDir "gold_signal_monitor.out.log"
$ErrLog = Join-Path $LogDir "gold_signal_monitor.err.log"

New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if (Test-Path $PidFile) {
    $existingPid = (Get-Content $PidFile -Raw).Trim()
    if ($existingPid) {
        $existing = Get-Process -Id ([int]$existingPid) -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Output "already_running pid=$existingPid"
            exit 0
        }
    }
}

$argList = @(
    $Script,
    "--interval-seconds", "$IntervalSeconds",
    "--state-dir", $StateDir
)

if ($RelaxedScan) {
    $argList += @(
        "--no-htf-bias",
        "--displacement-atr-mult", "0.7",
        "--fvg-min-points", "0.1",
        "--min-rr", "1.0",
        "--max-stop-points", "20"
    )
}

$process = Start-Process `
    -FilePath $Python `
    -ArgumentList $argList `
    -WorkingDirectory $Repo `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError $ErrLog `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $PidFile -Value $process.Id -Encoding ASCII
Write-Output "started pid=$($process.Id)"
Write-Output "state_dir=$StateDir"
Write-Output "stdout=$OutLog"
Write-Output "stderr=$ErrLog"
