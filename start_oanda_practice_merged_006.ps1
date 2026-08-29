[CmdletBinding()]
param(
    [int]$DurationSec = 604800
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$WorkerPython = Join-Path $Root 'data\oanda_training_manager\.research_py313\Scripts\python.exe'
$GuardPython = 'C:\Users\zmoor\AppData\Local\Programs\Python\Python312\python.exe'
$GuardScript = Join-Path $Root 'oanda_disk_event7_guard.py'
$VaultGuardDir = 'C:\Users\zmoor\OneDrive\thevault\projects\forex\artifacts\runtime_guards'
$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$LatestEvent = Get-WinEvent -FilterHashtable @{LogName='System'; Id=7} -MaxEvents 1 -ErrorAction SilentlyContinue
$BaselineRecord = if ($LatestEvent) { [long]$LatestEvent.RecordId } else { 0 }

if (-not (Test-Path -LiteralPath $WorkerPython)) {
    throw "Missing D: project Python: $WorkerPython"
}
if (-not (Test-Path -LiteralPath $GuardPython)) {
    throw "Missing independent disk-guard Python: $GuardPython"
}
New-Item -ItemType Directory -Force -Path $VaultGuardDir | Out-Null

$GptReport = Join-Path $VaultGuardDir "practice_006_merged_gpt_guard_$Timestamp.json"
$RelayReport = Join-Path $VaultGuardDir "practice_006_merged_relay_guard_$Timestamp.json"
$GptScript = Join-Path $Root 'oanda_practice_merged_006_gpt.py'
$RelayScript = Join-Path $Root 'oanda_practice_merged_signal_relay.py'

$GptArgs = @(
    $GuardScript,
    '--baseline-record', "$BaselineRecord",
    '--poll-sec', '2',
    '--report', $GptReport,
    '--',
    $WorkerPython,
    $GptScript,
    '--normal-mode'
)
$RelayArgs = @(
    $GuardScript,
    '--baseline-record', "$BaselineRecord",
    '--poll-sec', '2',
    '--report', $RelayReport,
    '--',
    $WorkerPython,
    $RelayScript,
    '--duration-sec', "$DurationSec"
)

$GptProcess = Start-Process -FilePath $GuardPython -ArgumentList $GptArgs -WorkingDirectory $Root -WindowStyle Hidden -PassThru
$RelayProcess = Start-Process -FilePath $GuardPython -ArgumentList $RelayArgs -WorkingDirectory $Root -WindowStyle Hidden -PassThru

[pscustomobject]@{
    account = 'practice -006'
    mode = 'aggressive GPT + consolidated -007 relay'
    baseline_event_record = $BaselineRecord
    gpt_guard_pid = $GptProcess.Id
    relay_guard_pid = $RelayProcess.Id
    gpt_guard_report = $GptReport
    relay_guard_report = $RelayReport
}
