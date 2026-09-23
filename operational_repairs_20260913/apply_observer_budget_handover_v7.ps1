$ErrorActionPreference='Stop'
$projectPath='C:\Users\zmoor\Documents\forex\trad'
$stagePath='C:\Users\zmoor\Documents\forex\operational_repairs_20260913\observer_publication_budget_repair_v1'
$manifest=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'HANDOVER_MANIFEST.json') | ConvertFrom-Json
$installs=@($manifest.install.PSObject.Properties)+@($manifest.config_install.PSObject.Properties)
foreach($entry in $installs){
    $item=$entry.Value
    if(-not [IO.Path]::GetFullPath($item.target).StartsWith($projectPath+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Target outside canonical root'}
    if((Get-FileHash -Algorithm SHA256 -LiteralPath $item.source).Hash.ToLowerInvariant() -ne $item.sha256){throw 'Candidate source changed'}
    if($item.previous_sha256 -and (Get-FileHash -Algorithm SHA256 -LiteralPath $item.target).Hash.ToLowerInvariant() -ne $item.previous_sha256){throw 'Predecessor changed'}
}
if((Get-FileHash -Algorithm SHA256 -LiteralPath $manifest.base_profile).Hash.ToLowerInvariant() -ne $manifest.base_profile_sha256){throw 'Prior profile changed'}
if((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $projectPath 'config\operational_dashboard_current.json')).Hash.ToLowerInvariant() -ne $manifest.previous_dashboard_pointer_sha256){throw 'Prior pointer changed'}
if(Test-Path -LiteralPath (Split-Path -Parent $manifest.future_archive_root)){throw 'Successor generation already exists'}
foreach($entry in $manifest.unchanged_frozen_sources.PSObject.Properties){
    if((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $projectPath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value){throw 'Frozen forward source changed'}
}
$expected=@{14964='oanda_always_on_supervisor.ps1';25080='oanda_supervisor_watchdog.ps1';15728='oanda_research_feature_observation_worker_v2.py';16992='oanda_research_feature_observation_worker_v2.py';19280='oanda_feature_forward_worker_v1.py';11624='oanda_feature_forward_worker_v1.py'}
$processes=@(Get-CimInstance Win32_Process)
foreach($key in $expected.Keys){
    $owned=@($processes | Where-Object {$_.ProcessId -eq $key})
    if($owned.Count -ne 1 -or [string]$owned[0].CommandLine -notlike ('*'+$projectPath+'\'+$expected[$key]+'*')){throw 'Expected process ownership changed'}
}
$taskName='Forex Operational Research Recovery 20260913'
$oldTask=Get-ScheduledTask -TaskName $taskName
$oldAction=$oldTask.Actions[0]
$oldArgs=[string]$oldAction.Arguments
if(-not $oldArgs.Contains([string]$manifest.base_profile)){throw 'Unexpected task selection'}
$retained=Join-Path $stagePath 'retained_before_live_handover'
if(Test-Path -LiteralPath $retained){throw 'Handover already attempted'}
New-Item -ItemType Directory -Path $retained | Out-Null
foreach($entry in $installs){if(Test-Path -LiteralPath $entry.Value.target){Copy-Item -LiteralPath $entry.Value.target -Destination (Join-Path $retained ([IO.Path]::GetFileName($entry.Value.target)))}}
Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $retained 'RESEARCH_TASK_BEFORE.xml') -Encoding utf8
$stopped=$false
try {
    Disable-ScheduledTask -TaskName $taskName | Out-Null
    $stopped=$true
    Stop-Process -Id 25080,14964 -Force
    Stop-Process -Id 16992,11624 -Force
    Stop-Process -Id 15728,19280 -Force -ErrorAction SilentlyContinue
    foreach($key in $expected.Keys){$remaining=Get-Process -Id $key -ErrorAction SilentlyContinue;if($remaining -and -not $remaining.WaitForExit(5000)){throw 'Old owner has not exited'}}
    $oldFiles=@(Get-ChildItem -LiteralPath $manifest.previous_feature_generation -File -Recurse | Where-Object {$_.Name -like '*.sqlite' -or $_.Name -like '*.sqlite-wal' -or $_.Name -like '*.json.gz' -or $_.Name -like '*.publication.json'} | ForEach-Object {
        $stream=[IO.File]::Open($_.FullName,[IO.FileMode]::Open,[IO.FileAccess]::Read,([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete))
        $hasher=[Security.Cryptography.SHA256]::Create()
        try{$hash=[BitConverter]::ToString($hasher.ComputeHash($stream)).Replace('-','').ToLowerInvariant()}finally{$hasher.Dispose();$stream.Dispose()}
        @{path=$_.FullName;bytes=$_.Length;sha256=$hash}
    })
    foreach($entry in $installs){Copy-Item -LiteralPath $entry.Value.source -Destination $entry.Value.target -Force}
    foreach($entry in $installs){if((Get-FileHash -Algorithm SHA256 -LiteralPath $entry.Value.target).Hash.ToLowerInvariant() -ne $entry.Value.sha256){throw 'Installed source mismatch'}}
    & (Join-Path $projectPath 'start_oanda_operational_research.ps1') -Root (Split-Path -Parent $projectPath) -OperationalProfilePath $manifest.research_profile_selection -ValidateOnly | Out-Null
    $newAction=New-ScheduledTaskAction -Execute $oldAction.Execute -Argument $oldArgs.Replace([string]$manifest.base_profile,[string]$manifest.research_profile_selection) -WorkingDirectory $oldAction.WorkingDirectory
    Set-ScheduledTask -TaskName $taskName -Action $newAction | Out-Null
    & (Join-Path $projectPath 'start_oanda_operational_research.ps1') -Root (Split-Path -Parent $projectPath) -OperationalProfilePath $manifest.research_profile_selection
    Enable-ScheduledTask -TaskName $taskName | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $stagePath 'RESEARCH_TASK_AFTER.xml') -Encoding utf8
    @{status='installed_explicit_research_launch_requested';utc=[datetime]::UtcNow.ToString('o');profile=$manifest.research_profile_selection;handover_manifest_sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stagePath 'HANDOVER_MANIFEST.json')).Hash.ToLowerInvariant();stopped_pids=@($expected.Keys);retained_previous_generation_files=$oldFiles;practice_changed=$false;native_generation_changed=$false;restart_circuit_changed=$false} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $stagePath 'ACTUAL_HANDOVER.json') -Encoding utf8
    'Installed bounded feature publication successor; explicit supervisor launch requested, recovery circuit retained.'
} catch {
    $reason=$_.Exception.Message
    if($stopped){
        foreach($entry in $installs){$backup=Join-Path $retained ([IO.Path]::GetFileName($entry.Value.target));if(Test-Path -LiteralPath $backup){Copy-Item -LiteralPath $backup -Destination $entry.Value.target -Force}}
        Set-ScheduledTask -TaskName $taskName -Action $oldAction | Out-Null
        & (Join-Path $projectPath 'start_oanda_operational_research.ps1') -Root (Split-Path -Parent $projectPath) -OperationalProfilePath $manifest.base_profile
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Start-ScheduledTask -TaskName $taskName
    }
    @{status='failed';utc=[datetime]::UtcNow.ToString('o');reason=$reason;prior_sources_and_task_restored=$stopped} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stagePath 'HANDOVER_FAILURE.json') -Encoding utf8
    throw
}
