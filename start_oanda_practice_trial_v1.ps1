param(
    [string]$PythonExe = 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'
)
$ErrorActionPreference = 'Stop'
$TrialRoot = $PSScriptRoot
$TrialState = Join-Path $TrialRoot 'data\oanda_training_manager\practice007_joint_v3_20260909_v1'
$Worker = Join-Path $TrialRoot 'oanda_practice_trial_runner_v1.py'
New-Item -ItemType Directory -Force -Path $TrialState | Out-Null
$TrialMutex = [System.Threading.Mutex]::new($false, 'Local\ForexPractice007JointV3Trial20260909')
$Acquired = $false
try {
    try { $Acquired = $TrialMutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $Acquired = $true }
    if (-not $Acquired) { exit 0 }
    $Failures = 0
    while ($true) {
        $StatusPath = Join-Path $TrialState 'status.json'
        if (Test-Path -LiteralPath $StatusPath) {
            try {
                $TrialStatus = Get-Content -LiteralPath $StatusPath -Raw | ConvertFrom-Json
                if ($TrialStatus.state -eq 'completed_flat') { break }
            } catch { }
        }
        $OutLog = Join-Path $TrialState 'worker.stdout.log'
        $ErrLog = Join-Path $TrialState 'worker.stderr.log'
        $Child = Start-Process -FilePath $PythonExe -ArgumentList @('"' + $Worker + '"', '--run') -WorkingDirectory $TrialRoot -WindowStyle Hidden -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog -PassThru
        $WatchdogRecord = @{ watchdog_pid = $PID; worker_pid = $Child.Id; observed_utc = [DateTime]::UtcNow.ToString('o'); restarts = $Failures; practice_only = $true }
        $WatchdogRecord | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $TrialState 'watchdog.json') -Encoding UTF8
        while (-not $Child.WaitForExit(5000)) { }
        if ($Child.ExitCode -eq 0) { break }
        $Failures += 1
        # Keep crash recovery alive after the deadline: the worker then runs
        # close-only until exact broker flatness is confirmed.
        Start-Sleep -Seconds ([Math]::Min(60, 5 * $Failures))
    }
} finally {
    if ($Acquired) { $TrialMutex.ReleaseMutex() }
    $TrialMutex.Dispose()
}
