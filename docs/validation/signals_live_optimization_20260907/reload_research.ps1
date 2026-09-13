$ErrorActionPreference='Stop'
$projectRoot='C:\Users\zmoor\Documents\forex\trad'
$outRoot=Split-Path -Parent $PSCommandPath
$supervisorFile=Join-Path $projectRoot 'oanda_always_on_supervisor.ps1'
$supervisorPattern=[regex]::Escape($supervisorFile)
$processes=@(Get-CimInstance Win32_Process)
$supervisors=@($processes | Where-Object { $_.Name -eq 'powershell.exe' -and $_.CommandLine -match "\s-File\s+`"?$supervisorPattern`"?(?:\s|$)" })
if($supervisors.Count -ne 1 -or $supervisors[0].CommandLine -notmatch '\s-ResearchCollectionOnly(?:\s|$)' -or $supervisors[0].CommandLine -notmatch '\s-SafeCoreOnly(?:\s|$)'){throw 'Expected exactly one research-only supervisor'}
$archiveHeartbeat=Get-Content -LiteralPath (Join-Path $projectRoot 'data\oanda_training_manager\state\all68_m1_forward_update_heartbeat_v1.json') -Raw | ConvertFrom-Json
if($archiveHeartbeat.phase -ne 'sleeping' -or $archiveHeartbeat.phase_age_sec -gt 150){throw 'Archive writer must be between passes before reload'}
$targets=@('oanda_causal_forecast_study.py','oanda_all68_m1_forward_updater.py','oanda_practice_live_dashboard.py')
$selected=@()
foreach($scriptName in $targets){
    $pattern=[regex]::Escape((Join-Path $projectRoot $scriptName))
    $matched=@($processes | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match "(?:^|\s)`"?$pattern`"?(?:\s|$)" })
    if($matched.Count -ne 2){throw "Expected one launcher/child for $scriptName"}
    foreach($proc in $matched){$selected+=@{pid=$proc.ProcessId;parent=$proc.ParentProcessId;script=$scriptName}}
}
Stop-Process -Id $supervisors[0].ProcessId -Force
foreach($row in ($selected | Sort-Object pid -Descending)){Stop-Process -Id $row.pid -Force -ErrorAction SilentlyContinue}
foreach($row in $selected){if(Get-Process -Id $row.pid -ErrorAction SilentlyContinue){throw 'Target process still present'}}
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' (Join-Path $outRoot 'activate_registration.py')
if($LASTEXITCODE -ne 0){throw 'Activation failed; research launcher not run'}
$launch=& (Join-Path $projectRoot 'start_oanda_research_collection.ps1')
if($LASTEXITCODE -ne 0){throw 'Research launcher failed'}
[ordered]@{observed_utc=(Get-Date).ToUniversalTime().ToString('o');prior_supervisor=$supervisors[0].ProcessId;stopped_workers=$selected;archive_phase_at_stop=$archiveHeartbeat.phase;launch_result=$launch;untargeted_workers_preserved=$true;orders_enabled=$false} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outRoot 'RUNTIME_RELOAD.json') -Encoding utf8
$launch
