param([Parameter(Mandatory=$true)][string]$SourceStage,
      [Parameter(Mandatory=$true)][string]$CapacityReceipt,
      [Parameter(Mandatory=$true)][string]$FlatReceipt)
$ErrorActionPreference='Stop'
$projectPath='C:\Users\zmoor\Documents\forex\trad'
$areaPath='C:\Users\zmoor\Documents\forex\operational_repairs_20260913'
$pythonPath='C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'
$stagePath=[IO.Path]::GetFullPath($SourceStage)
if((Split-Path -Parent $stagePath) -ine $areaPath){throw 'Owned frozen source stage required'}
$source=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'SOURCE_STAGE.json') | ConvertFrom-Json
$registration=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'PROSPECTIVE_REGISTRATION.json') | ConvertFrom-Json
$runtimePath=Join-Path $stagePath 'runtime_handover_v8'
$runtime=Get-Content -Raw -LiteralPath (Join-Path $runtimePath 'RUNTIME_HANDOVER.json') | ConvertFrom-Json
$capacity=Get-Content -Raw -LiteralPath $CapacityReceipt | ConvertFrom-Json
$flat=Get-Content -Raw -LiteralPath $FlatReceipt | ConvertFrom-Json
if($capacity.all_checks_passed -ne $true -or [IO.Path]::GetFullPath($capacity.source_stage) -ine (Join-Path $stagePath 'kit')){throw 'Final full-capture capacity acceptance required'}
if($capacity.consumer_or_publication_writes_performed -ne $false -or $capacity.actual_market_health_proven -ne $false){throw 'Unexpected synthetic capacity evidence scope'}
if(@($capacity.source_bindings.PSObject.Properties).Count -ne @($source.source_bindings.PSObject.Properties).Count){throw 'Capacity source inventory mismatch'}
foreach($entry in $source.source_bindings.PSObject.Properties){if($capacity.source_bindings.($entry.Name) -ne $entry.Value){throw 'Capacity did not test this exact source kit'}}
[xml]$tests=Get-Content -Raw -LiteralPath (Join-Path $stagePath 'ROOT_INTEGRATION_TESTS.xml')
if($tests.testsuites.testsuite.tests -ne '14' -or $tests.testsuites.testsuite.failures -ne '0' -or $tests.testsuites.testsuite.errors -ne '0'){throw 'Final fourteen integration tests required'}
if(-not $flat.summary.ready -or -not $flat.summary.flat -or -not $flat.original_trial_unchanged -or $flat.original_trial.all_intents -ne 0 -or -not $flat.original_trial.completed_flat_epoch){throw 'Fresh original flat zero-intent completion required'}
if(([datetimeoffset]::UtcNow-[datetimeoffset]::Parse($flat.completed_utc)).TotalSeconds -gt 60){throw 'Independent broker preflight must be refreshed within sixty seconds'}
foreach($entry in $source.original_source_bindings.PSObject.Properties){if((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $projectPath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value){throw 'Original native source changed'}}
foreach($entry in $source.source_bindings.PSObject.Properties){if(-not $source.original_source_bindings.PSObject.Properties[$entry.Name] -and (Test-Path -LiteralPath (Join-Path $projectPath $entry.Name))){throw 'New helper path unexpectedly exists'}}
foreach($entry in $runtime.unchanged_runtime_sources.PSObject.Properties){if((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $projectPath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value){throw 'Independent runtime repair changed'}}
if((Get-FileHash -Algorithm SHA256 -LiteralPath $runtime.original_profile_path).Hash.ToLowerInvariant() -ne $runtime.original_profile_sha256){throw 'Current profile changed'}
$installs=@()
foreach($entry in $source.source_bindings.PSObject.Properties){$installs+=@{source=(Join-Path (Join-Path $stagePath 'kit') $entry.Name);target=(Join-Path $projectPath $entry.Name);sha256=$entry.Value}}
foreach($entry in $registration.configurations.PSObject.Properties){$installs+=@{source=(Join-Path (Join-Path $stagePath 'prospective_config') $entry.Value.filename);target=(Join-Path (Join-Path $projectPath 'config') $entry.Value.filename);sha256=$entry.Value.sha256};if(Test-Path -LiteralPath $installs[-1].target){throw 'New native configuration already exists'}}
foreach($entry in $runtime.files.PSObject.Properties){$installs+=@{source=(Join-Path $runtimePath $entry.Name);target=(Join-Path $projectPath $entry.Name);sha256=$entry.Value};if(Test-Path -LiteralPath $installs[-1].target){throw 'New runtime profile already exists'}}
foreach($item in $installs){
    if(-not [IO.Path]::GetFullPath($item.target).StartsWith($projectPath+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Install target outside canonical root'}
    if((Get-FileHash -Algorithm SHA256 -LiteralPath $item.source).Hash.ToLowerInvariant() -ne $item.sha256){throw 'Frozen install source changed'}
}
if(Test-Path -LiteralPath $registration.prospective_runtime_root){throw 'Fresh native data generation already exists'}
$expected=@{12684='oanda_supervisor_watchdog.ps1';25008='oanda_always_on_supervisor.ps1';24456='oanda_joint_price_news_forecast_study_v7.py';6064='oanda_joint_price_news_forecast_study_v7.py';8036='revision_transport_v4.py';18856='revision_transport_v4.py';24520='oanda_research_feature_observation_worker_v2.py';15404='oanda_research_feature_observation_worker_v2.py';14828='oanda_practice_live_dashboard.py';2864='oanda_practice_live_dashboard.py'}
$inventory=Get-Content -Raw -LiteralPath (Join-Path $areaPath 'native_compact_handover_owner_inventory_stage007_20260914.json') | ConvertFrom-Json
$processes=@(Get-CimInstance Win32_Process)
foreach($key in $expected.Keys){
    $owned=@($processes | Where-Object {$_.ProcessId -eq $key})
    $prior=@($inventory.processes | Where-Object {$_.pid -eq $key})
    if($owned.Count -ne 1 -or $prior.Count -ne 1 -or [string]$owned[0].CommandLine -notlike ('*'+$projectPath+'\'+$expected[$key]+'*')){throw 'Expected process ownership changed'}
    if([math]::Abs(($owned[0].CreationDate.ToUniversalTime()-[datetimeoffset]::Parse($prior[0].started_utc).UtcDateTime).TotalSeconds) -gt .001){throw 'Process creation identity changed'}
}
if(@($processes | Where-Object {[string]$_.CommandLine -like ('*'+$projectPath+'\oanda_practice_trial_runner_v2.py*') -or [string]$_.CommandLine -like ('*'+$projectPath+'\oanda_practice_trial_recovery_v2.py*')}).Count){throw 'Practice owner still running'}
$researchTask='Forex Operational Research Recovery 20260913'
$practiceTask='Forex Practice Research Recovery 20260910'
$researchAction=(Get-ScheduledTask -TaskName $researchTask).Actions[0]
if(-not ([string]$researchAction.Arguments).Contains([string]$runtime.original_profile_path)){throw 'Research task changed'}
$retained=Join-Path $stagePath 'actual_handover'
if(Test-Path -LiteralPath $retained){throw 'Actual handover already attempted; inspect retained phase before resuming'}
New-Item -ItemType Directory -Path $retained | Out-Null
Export-ScheduledTask -TaskName $researchTask | Set-Content -LiteralPath (Join-Path $retained 'RESEARCH_TASK_BEFORE.xml') -Encoding utf8
Export-ScheduledTask -TaskName $practiceTask | Set-Content -LiteralPath (Join-Path $retained 'PRACTICE_TASK_BEFORE.xml') -Encoding utf8
foreach($item in $installs){if(Test-Path -LiteralPath $item.target){Copy-Item -LiteralPath $item.target -Destination (Join-Path $retained ([IO.Path]::GetFileName($item.target)))}}
$phase='prepared'
try {
    Disable-ScheduledTask -TaskName $researchTask | Out-Null
    Disable-ScheduledTask -TaskName $practiceTask | Out-Null
    Stop-Process -Id 12684,25008 -Force
    Stop-Process -Id 6064,18856,15404,2864 -Force
    Stop-Process -Id 24456,8036,24520,14828 -Force -ErrorAction SilentlyContinue
    foreach($key in $expected.Keys){$remaining=Get-Process -Id $key -ErrorAction SilentlyContinue;if($remaining -and -not $remaining.WaitForExit(5000)){throw 'Old owner has not exited'}}
    $phase='old_affected_owners_stopped'
    foreach($item in $installs){Copy-Item -LiteralPath $item.source -Destination $item.target -Force}
    foreach($item in $installs){if((Get-FileHash -Algorithm SHA256 -LiteralPath $item.target).Hash.ToLowerInvariant() -ne $item.sha256){throw 'Installed source mismatch'}}
    $phase='new_sources_and_configurations_installed'
    & $pythonPath -B (Join-Path $areaPath 'activate_compact_native_v4.py')
    if($LASTEXITCODE -ne 0){throw 'Fresh native activation refused'}
    $phase='new_native_empty_activation_complete'
    & $pythonPath -B (Join-Path $areaPath 'prepare_native_practice_handover_v1.py') --source-stage $stagePath
    if($LASTEXITCODE -ne 0){throw 'Original-window practice successor preparation refused'}
    $practiceFolder=Join-Path $stagePath 'practice_handover'
    $practice=Get-Content -Raw -LiteralPath (Join-Path $practiceFolder 'PRACTICE_HANDOVER.json') | ConvertFrom-Json
    foreach($entry in $practice.files.PSObject.Properties){
        $targetPath=[IO.Path]::GetFullPath((Join-Path $projectPath $entry.Name))
        if(-not $targetPath.StartsWith($projectPath+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Practice target outside root'}
        $candidatePath=Join-Path $practiceFolder $entry.Name
        if((Get-FileHash -Algorithm SHA256 -LiteralPath $candidatePath).Hash.ToLowerInvariant() -ne $entry.Value){throw 'Prepared practice source changed'}
        Copy-Item -LiteralPath $candidatePath -Destination $targetPath -Force
        if((Get-FileHash -Algorithm SHA256 -LiteralPath $targetPath).Hash.ToLowerInvariant() -ne $entry.Value){throw 'Practice installed source mismatch'}
    }
    $phase='source_bound_practice_and_dashboard_selection_installed'
    $newProfile=Join-Path $projectPath 'config\operational_runtime_v8_20260914.json'
    & (Join-Path $projectPath 'start_oanda_operational_research.ps1') -Root (Split-Path -Parent $projectPath) -OperationalProfilePath $newProfile -ValidateOnly | Out-Null
    $action=New-ScheduledTaskAction -Execute $researchAction.Execute -Argument ([string]$researchAction.Arguments).Replace([string]$runtime.original_profile_path,$newProfile) -WorkingDirectory $researchAction.WorkingDirectory
    Set-ScheduledTask -TaskName $researchTask -Action $action | Out-Null
    $practiceArgs='-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File "'+$projectPath+'\start_oanda_practice_recovery_v2.ps1" -Config "'+$projectPath+'\config\practice007_native_v7_20260913_v2.json" -Manifest "'+$projectPath+'\config\practice_native_recovery_v3_20260914.json"'
    $priorPracticeAction=(Get-ScheduledTask -TaskName $practiceTask).Actions[0]
    $action=New-ScheduledTaskAction -Execute $priorPracticeAction.Execute -Argument $practiceArgs -WorkingDirectory $priorPracticeAction.WorkingDirectory
    Set-ScheduledTask -TaskName $practiceTask -Action $action | Out-Null
    & (Join-Path $projectPath 'start_oanda_operational_research.ps1') -Root (Split-Path -Parent $projectPath) -OperationalProfilePath $newProfile
    Enable-ScheduledTask -TaskName $researchTask | Out-Null
    Start-ScheduledTask -TaskName $researchTask
    $phase='explicit_new_research_launch_requested_practice_pending_GET_preflight'
    @{status=$phase;utc=[datetime]::UtcNow.ToString('o');profile=$newProfile;source_stage_sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stagePath 'SOURCE_STAGE.json')).Hash.ToLowerInvariant();capacity_receipt_sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $CapacityReceipt).Hash.ToLowerInvariant();flat_receipt_sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $FlatReceipt).Hash.ToLowerInvariant();stopped_pids=@($expected.Keys);practice_started=$false;restart_circuit_changed=$false;original_trial_window_preserved=$true} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $retained 'ACTUAL_HANDOVER.json') -Encoding utf8
    'New native sources activated; research launch requested. Practice task remains disabled pending actual successor preflight.'
} catch {
    @{status='failed_requires_phase_review';last_completed_phase=$phase;utc=[datetime]::UtcNow.ToString('o');reason=$_.Exception.Message;automatic_source_rollback_performed=$false} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $retained 'FAILURE.json') -Encoding utf8
    throw
}
