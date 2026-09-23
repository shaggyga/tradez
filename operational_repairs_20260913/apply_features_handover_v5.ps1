$ErrorActionPreference='Stop'
$tradPath='C:\Users\zmoor\Documents\forex\trad'
$stagePath='C:\Users\zmoor\Documents\forex\operational_repairs_20260913\dashboard_availability_repair_v1'
$manifest=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'HANDOVER_MANIFEST.json') | ConvertFrom-Json
$freeze=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'SOURCE_MANIFEST.json') | ConvertFrom-Json
foreach($entry in $freeze.files_sha256.PSObject.Properties){
    if((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stagePath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value){throw 'Frozen candidate changed'}
}
$installs=@($manifest.install.PSObject.Properties)+@($manifest.config_install.PSObject.Properties)
foreach($entry in $installs){
    $item=$entry.Value
    $resolved=[IO.Path]::GetFullPath($item.target)
    if(-not $resolved.StartsWith($tradPath+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Target outside canonical root'}
    if((Get-FileHash -Algorithm SHA256 -LiteralPath $item.source).Hash.ToLowerInvariant() -ne $item.sha256){throw 'Candidate file changed'}
    if($item.previous_sha256 -and (Get-FileHash -Algorithm SHA256 -LiteralPath $item.target).Hash.ToLowerInvariant() -ne $item.previous_sha256){throw 'Predecessor source changed'}
}
if((Get-FileHash -Algorithm SHA256 -LiteralPath $manifest.base_profile).Hash.ToLowerInvariant() -ne $manifest.base_profile_sha256){throw 'Installed base profile changed'}
$pointer=Join-Path $tradPath 'config\operational_dashboard_current.json'
if((Get-FileHash -Algorithm SHA256 -LiteralPath $pointer).Hash.ToLowerInvariant() -ne $manifest.previous_dashboard_pointer_sha256){throw 'Current dashboard pointer changed'}
if(Test-Path -LiteralPath (Join-Path $tradPath 'data\oanda_training_manager\operational_repair_20260914_features_v1')){throw 'Fresh feature cohort must not exist before handover'}
[xml]$checks=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'ROOT_HANDOVER_TESTS.xml')
if($checks.testsuites.testsuite.tests -ne '32' -or $checks.testsuites.testsuite.failures -ne '0' -or $checks.testsuites.testsuite.errors -ne '0'){throw 'Root acceptance failed'}
$expected=@{23724='oanda_supervisor_watchdog.ps1';11836='oanda_always_on_supervisor.ps1';25120='oanda_research_feature_observation_worker_v2.py';23532='oanda_research_feature_observation_worker_v2.py';18452='oanda_feature_forward_worker_v1.py';22304='oanda_feature_forward_worker_v1.py';10724='oanda_practice_live_dashboard.py';4816='oanda_practice_live_dashboard.py'}
$processes=@(Get-CimInstance Win32_Process)
foreach($key in $expected.Keys){
    $owned=@($processes | Where-Object {$_.ProcessId -eq $key})
    if($owned.Count -ne 1 -or [string]$owned[0].CommandLine -notlike ('*'+$tradPath+'\'+$expected[$key]+'*')){throw 'Process ownership changed'}
}
foreach($legacy in @('oanda_model_gap_live_signal_worker.py','oanda_practice_shadow_strategy_lab.py')){
    if(@($processes | Where-Object {[string]$_.CommandLine -like ('*'+$tradPath+'\'+$legacy+'*')}).Count){throw 'Legacy imported normalizer owner requires reviewed restart'}
}
$taskName='Forex Operational Research Recovery 20260913'
$oldTask=Get-ScheduledTask -TaskName $taskName
$oldAction=$oldTask.Actions[0]
$oldArgs=[string]$oldAction.Arguments
if(-not $oldArgs.Contains([string]$manifest.base_profile)){throw 'Unexpected task profile selection'}
$retained=Join-Path $stagePath 'retained_before_live_handover_003'
if(Test-Path -LiteralPath $retained){throw 'Actual handover has already been attempted'}
New-Item -ItemType Directory -Path $retained | Out-Null
foreach($entry in $installs){if(Test-Path -LiteralPath $entry.Value.target){Copy-Item -LiteralPath $entry.Value.target -Destination (Join-Path $retained ([IO.Path]::GetFileName($entry.Value.target)))}}
Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $retained 'RESEARCH_TASK_BEFORE.xml') -Encoding utf8
$stopped=$false
try {
    Disable-ScheduledTask -TaskName $taskName | Out-Null
    $stopped=$true
    Stop-Process -Id 23724,11836 -Force
    Stop-Process -Id 23532,22304,4816 -Force
    Stop-Process -Id 25120,18452,10724 -Force -ErrorAction SilentlyContinue
    foreach($key in $expected.Keys){
        $remaining=Get-Process -Id $key -ErrorAction SilentlyContinue
        if($remaining -and -not $remaining.WaitForExit(5000)){throw 'Old owner has not exited'}
    }
    $oldForward=Join-Path $tradPath 'data\oanda_training_manager\operational_repair_20260913_v1\feature_forward_v3'
    # Owner-lock and SQLite shared-memory bytes are not retained research data.
    # Hash only the database and any original WAL after the writer has exited.
    $oldFiles=@(Get-ChildItem -LiteralPath $oldForward -File | Where-Object {$_.Name -like '*.sqlite' -or $_.Name -like '*.sqlite-wal'} | ForEach-Object {
        $stream=[IO.File]::Open($_.FullName,[IO.FileMode]::Open,[IO.FileAccess]::Read,([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete))
        $hasher=[Security.Cryptography.SHA256]::Create()
        try{$hash=[BitConverter]::ToString($hasher.ComputeHash($stream)).Replace('-','').ToLowerInvariant()}finally{$hasher.Dispose();$stream.Dispose()}
        @{name=$_.Name;bytes=$_.Length;sha256=$hash}
    })
    foreach($entry in $installs){Copy-Item -LiteralPath $entry.Value.source -Destination $entry.Value.target -Force}
    foreach($entry in $installs){if((Get-FileHash -Algorithm SHA256 -LiteralPath $entry.Value.target).Hash.ToLowerInvariant() -ne $entry.Value.sha256){throw 'Installed file mismatch'}}
    $newAction=New-ScheduledTaskAction -Execute $oldAction.Execute -Argument $oldArgs.Replace([string]$manifest.base_profile,[string]$manifest.research_profile_selection) -WorkingDirectory $oldAction.WorkingDirectory
    Set-ScheduledTask -TaskName $taskName -Action $newAction | Out-Null
    Enable-ScheduledTask -TaskName $taskName | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $stagePath 'RESEARCH_TASK_AFTER.xml') -Encoding utf8
    @{status='installed_recovery_started';utc=[datetime]::UtcNow.ToString('o');profile=$manifest.research_profile_selection;source_manifest_sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stagePath 'SOURCE_MANIFEST.json')).Hash.ToLowerInvariant();stopped_pids=@($expected.Keys);old_forward_files_after_writer_stop=$oldFiles;practice_changed=$false;native_generation_changed=$false;new_forward_source_identity=$manifest.future_forward_source_identity} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $stagePath 'ACTUAL_HANDOVER.json') -Encoding utf8
    'Installed deterministic feature cohort and selected recovery profile; practice/native unchanged.'
} catch {
    $reason=$_.Exception.Message
    if($stopped){
        foreach($entry in $installs){$backup=Join-Path $retained ([IO.Path]::GetFileName($entry.Value.target));if(Test-Path -LiteralPath $backup){Copy-Item -LiteralPath $backup -Destination $entry.Value.target -Force}}
        Set-ScheduledTask -TaskName $taskName -Action $oldAction | Out-Null
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Start-ScheduledTask -TaskName $taskName
    }
    @{status='failed';utc=[datetime]::UtcNow.ToString('o');reason=$reason;prior_sources_and_task_restored=$stopped} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stagePath 'HANDOVER_FAILURE_003.json') -Encoding utf8
    throw
}
