param(
    [switch]$CheckOnly,
    [string]$Config = (Join-Path $PSScriptRoot 'config/practice007_native_v7_20260913_v1.json'),
    [string]$Manifest = (Join-Path $PSScriptRoot 'config/practice_native_recovery_v2_20260913.json')
)
$ErrorActionPreference = 'Stop'
$RecoveryPython = 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'
$RecoveryWorker = Join-Path $PSScriptRoot 'oanda_practice_trial_recovery_v2.py'
$RecoveryMode = if ($CheckOnly) { '--check' } else { '--watch' }
# Task action must use PowerShell -WindowStyle Hidden; child runner also sets
# CREATE_NO_WINDOW. The Python owner validates every source before any spawn.
& $RecoveryPython -B $RecoveryWorker $RecoveryMode --config $Config --manifest $Manifest
exit $LASTEXITCODE
