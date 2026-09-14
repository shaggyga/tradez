param([switch]$CheckOnly)
$ErrorActionPreference = 'Stop'
$RecoveryPython = 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'
$RecoveryWorker = Join-Path $PSScriptRoot 'oanda_practice_trial_recovery_v2.py'
$RecoveryMode = if ($CheckOnly) { '--check' } else { '--watch' }
# Task action must use PowerShell -WindowStyle Hidden; child runner also sets
# CREATE_NO_WINDOW. The Python owner validates every source before any spawn.
& $RecoveryPython -B $RecoveryWorker $RecoveryMode
exit $LASTEXITCODE
