# Prepared first-start operation. Registration/activation must already exist.
# No activation is performed here. Run only after the bound sources are frozen.
$ErrorActionPreference='Stop'
$projectRoot='C:\Users\zmoor\Documents\forex\trad'
$evidenceRoot=Split-Path -Parent $PSCommandPath
if ([IO.Path]::GetFullPath($evidenceRoot) -ne 'C:\Users\zmoor\Documents\forex\pair_coverage_20260907') { throw 'Unexpected reload workspace' }
$python='C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'
$powershell="$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$stamp=Get-Date -Format 'yyyyMMdd_HHmmss_fff'
$gateRaw=& $powershell -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File (Join-Path $evidenceRoot 'review_pair_gate.ps1')
if($LASTEXITCODE -ne 0){throw 'Isolated closed-gate review failed'}
$gate=$gateRaw | ConvertFrom-Json
if($gate.status -ne 'passed' -or $gate.allowed_worker_count -ne 14 -or $gate.process_starts -ne 0 -or $gate.process_stops -ne 0){throw 'Invalid closed-gate review'}
$gateRaw | Set-Content -LiteralPath (Join-Path $evidenceRoot "PAIR_GATE_RELOAD_${stamp}.json") -Encoding utf8
& $python (Join-Path $evidenceRoot 'verify_pair_reload.py') --output (Join-Path $evidenceRoot "PAIR_RELOAD_PREFLIGHT_${stamp}.json")
if($LASTEXITCODE -ne 0){throw 'Frozen sources or pre-existing zero-evidence activations failed validation'}

$supervisorPath=Join-Path $projectRoot 'oanda_always_on_supervisor.ps1'
$launcherPath=Join-Path $projectRoot 'start_oanda_research_collection.ps1'
if((Get-FileHash -LiteralPath $supervisorPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $gate.supervisor_sha256){throw 'Supervisor changed after review'}
if((Get-FileHash -LiteralPath $launcherPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $gate.launcher_sha256){throw 'Launcher changed after review'}
$all=@(Get-CimInstance Win32_Process)
$supervisorPattern=[regex]::Escape($supervisorPath)
$supervisor=@($all | Where-Object { $_.Name -match '^(powershell|pwsh)\.exe$' -and $_.CommandLine -match "\s-File\s+`"?$supervisorPattern`"?(?:\s|$)" })
if($supervisor.Count -ne 1 -or $supervisor[0].CommandLine -notmatch '\s-ResearchCollectionOnly(?:\s|$)' -or $supervisor[0].CommandLine -notmatch '\s-SafeCoreOnly(?:\s|$)'){throw 'Expected exactly one research-only safe-core supervisor'}
function Find-ExactPythonScript {
    param([string]$Name)
    $pattern=[regex]::Escape((Join-Path $projectRoot $Name))
    return @($all | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match "(?:^|\s)`"?$pattern`"?(?:\s|$)" })
}
$newWorker=@(Find-ExactPythonScript 'oanda_pair_local_forecast_study_v1.py')
if($newWorker.Count){throw 'Initial pair reload refuses an already-running pair worker'}
$dashboard=@(Find-ExactPythonScript 'oanda_practice_live_dashboard.py')
if($dashboard.Count -ne 2){throw 'Expected dashboard launcher and child'}
$preserved=@()
foreach($script in @('oanda_causal_forecast_study_eurusd_v1.py','oanda_causal_forecast_study_gap_v2.py','oanda_practice_quote_stream.py','oanda_all68_m1_forward_updater.py')){
    $matches=@(Find-ExactPythonScript $script)
    if($matches.Count -ne 2){throw "Expected preserved launcher and child: $script"}
    $preserved += $matches
}
function Stop-VerifiedProcess {
    param($Observed,[switch]$AllowAlreadyExited)
    $current=@(Get-CimInstance Win32_Process -Filter ('ProcessId = '+[int]$Observed.ProcessId))
    if($AllowAlreadyExited -and $current.Count -eq 0){return}
    if($current.Count -ne 1 -or $current[0].Name -ne $Observed.Name -or $current[0].CommandLine -ne $Observed.CommandLine -or $current[0].CreationDate -ne $Observed.CreationDate){throw 'Process identity changed before controlled stop'}
    Stop-Process -Id $Observed.ProcessId -Force
}
Stop-VerifiedProcess $supervisor[0]
Wait-Process -Id $supervisor[0].ProcessId -Timeout 10 -ErrorAction SilentlyContinue
foreach($process in $dashboard){Stop-VerifiedProcess $process -AllowAlreadyExited}
$launch=& $powershell -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $launcherPath
if($LASTEXITCODE -ne 0){throw 'Research collection launcher failed'}
$after=@(Get-CimInstance Win32_Process)
foreach($process in $preserved){
    $current=@($after | Where-Object {$_.ProcessId -eq $process.ProcessId})
    if($current.Count -ne 1 -or $current[0].CommandLine -ne $process.CommandLine -or $current[0].CreationDate -ne $process.CreationDate){throw 'A preserved collection process changed during reload'}
}
[ordered]@{schema_version='pair_collection_runtime_reload_v1_20260907';observed_utc=(Get-Date).ToUniversalTime().ToString('o');prior_supervisor=$supervisor[0].ProcessId;stopped_dashboard_pids=@($dashboard.ProcessId);preserved_collection_pids=@($preserved.ProcessId);launch_result=$launch;allowed_worker_count=14;existing_studies_quotes_archive_preserved=$true;orders_enabled=$false;activation_performed=$false} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $evidenceRoot "PAIR_RUNTIME_RELOAD_${stamp}.json") -Encoding utf8
$launch
