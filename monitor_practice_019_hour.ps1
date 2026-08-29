param(
    [int]$Iterations = 60,
    [int]$SleepSeconds = 60
)

$ErrorActionPreference = "Continue"

$Repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $Repo "data\oanda_training_manager\.research_py313\Scripts\python.exe"
$LaneDir = Join-Path $Repo "data\technical_scout_manager\account_demo_019_primary_forecast_rotation"
$LogPath = Join-Path $LaneDir "monitor_019_hour.log"
$ErrPath = Join-Path $LaneDir "monitor_019_hour.err.log"
$LockPath = Join-Path $LaneDir "process.lock"

New-Item -ItemType Directory -Force -Path $LaneDir | Out-Null

for ($i = 0; $i -lt $Iterations; $i++) {
    $ts = Get-Date -Format o
    try {
        $status = & $Python (Join-Path $Repo "oanda_live_account_readonly_status.py") --all-practice |
            Select-String -Pattern "practice_019" |
            Select-Object -First 1
        $statusText = if ($status) { $status.ToString().Trim() } else { "practice_019 status missing" }
        $lockExists = Test-Path $LockPath
        $botProc = Get-CimInstance Win32_Process |
            Where-Object {
                $_.CommandLine -match "oanda_primary_forecast_rotation_bot.py" -and
                $_.CommandLine -match "primary_forecast_rotation_demo_019|primary_forecast_rotation_demo_019.json" -and
                $_.CommandLine -notmatch "monitor_practice_019_hour"
            } |
            Select-Object -First 1
        $botPid = if ($botProc) { [string]$botProc.ProcessId } else { "" }
        "$ts | $statusText | lock=$lockExists | bot_pid=$botPid" |
            Out-File -FilePath $LogPath -Append -Encoding utf8
    } catch {
        "$ts | ERROR | $($_.Exception.Message)" |
            Out-File -FilePath $ErrPath -Append -Encoding utf8
    }
    if ($i + 1 -lt $Iterations) {
        Start-Sleep -Seconds $SleepSeconds
    }
}
