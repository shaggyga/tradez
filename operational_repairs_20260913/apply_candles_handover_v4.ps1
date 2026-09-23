$ErrorActionPreference = 'Stop'
$tradPath = 'C:\Users\zmoor\Documents\forex\trad'
$stagePath = 'C:\Users\zmoor\Documents\forex\operational_repairs_20260913\candles_handover_v4'
$taskName = 'Forex Operational Research Recovery 20260913'
$priorProfile = Join-Path $tradPath 'config\operational_runtime_v3_20260913.json'
$nextProfile = Join-Path $tradPath 'config\operational_runtime_v4_20260914_candles.json'
$receipt = Get-Content -Raw -LiteralPath (Join-Path $stagePath 'CANDLES_HANDOVER.json') | ConvertFrom-Json
foreach ($entry in $receipt.files.PSObject.Properties) {
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stagePath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value) { throw 'Staged source changed' }
}
foreach ($entry in $receipt.original_files.PSObject.Properties) {
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $tradPath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value) { throw 'Original source changed' }
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $priorProfile).Hash.ToLowerInvariant() -ne $receipt.original_profile_sha256) { throw 'Original profile changed' }
[xml]$checks = Get-Content -Raw -LiteralPath (Join-Path $stagePath 'CANDLES_HANDOVER_TESTS.xml')
if ($checks.testsuites.testsuite.tests -ne '39' -or $checks.testsuites.testsuite.failures -ne '0' -or $checks.testsuites.testsuite.errors -ne '0') { throw 'Acceptance tests not passed' }
$statePath = Join-Path $tradPath 'data\oanda_training_manager\state'
$settled = Get-Content -Raw -LiteralPath (Join-Path $statePath 'retained_price_settlement_v1.json') | ConvertFrom-Json
if ($settled.status -ne 'settled' -or @($settled.studies | Where-Object { $_.unresolved -ne 0 }).Count) { throw 'Retained studies not settled' }
$expected = @{8256='oanda_supervisor_watchdog.ps1';13856='oanda_always_on_supervisor.ps1';22584='oanda_all68_m1_forward_updater.py';5592='oanda_all68_m1_forward_updater.py';14092='oanda_retained_price_settlement_v1.py';12336='oanda_retained_price_settlement_v1.py'}
$processes = @(Get-CimInstance Win32_Process)
foreach ($key in $expected.Keys) {
    $owned = @($processes | Where-Object { $_.ProcessId -eq $key })
    if ($owned.Count -ne 1 -or [string]$owned[0].CommandLine -notlike ('*'+$tradPath+'\'+$expected[$key]+'*')) { throw 'Process ownership changed' }
}
$oldTask = Get-ScheduledTask -TaskName $taskName
$oldAction = $oldTask.Actions[0]
$oldArgs = [string]$oldAction.Arguments
if (-not $oldArgs.Contains($priorProfile)) { throw 'Unexpected recovery task selection' }
Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $stagePath 'RESEARCH_TASK_BEFORE.xml') -Encoding utf8
$report = Get-Content -Raw -LiteralPath (Join-Path $statePath 'all68_m1_forward_update_v1.json') | ConvertFrom-Json
$archiveRoot = [IO.Path]::GetFullPath((Join-Path $tradPath 'data\oanda_training_manager\candles'))
$paths = @($report.pairs | ForEach-Object { [IO.Path]::GetFullPath([string]$_.path) } | Sort-Object -Unique)
if ($paths.Count -ne 68) { throw 'Expected 68 archive paths' }
foreach ($path in $paths) {
    if ([IO.Path]::GetDirectoryName($path) -ne $archiveRoot -or [IO.Path]::GetFileName($path) -notmatch '^[A-Z]{3}_[A-Z]{3}_M1\.csv$') { throw 'Archive path outside exact candle scope' }
    $item = Get-Item -LiteralPath $path
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Archive path is a reparse point' }
}
# FileShare.Read allows existing readers but refuses an active writer and denies
# new writes/deletes. Acquire every original archive before terminating its owner.
# No file contents or availability clocks are changed by this handover.
$leases = [Collections.Generic.List[IO.FileStream]]::new()
$managerStopped = $false
$installed = $false
$handoverClock = [Diagnostics.Stopwatch]::StartNew()
try {
    $locked = $false
    while (-not $locked -and $handoverClock.Elapsed.TotalSeconds -lt 20) {
        try {
            foreach ($path in $paths) { $leases.Add([IO.File]::Open($path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)) }
            $locked = $true
        } catch [IO.IOException] {
            foreach ($lease in $leases) { $lease.Dispose() }
            $leases.Clear()
            Start-Sleep -Milliseconds 100
        }
    }
    if (-not $locked) { throw 'No write-free archive interval acquired; no manager or worker stopped' }
    $leaseStart = [datetime]::UtcNow
    Disable-ScheduledTask -TaskName $taskName | Out-Null
    $managerStopped = $true
    Stop-Process -Id 8256,13856 -Force
    Stop-Process -Id 22584 -Force
    Stop-Process -Id 5592 -Force -ErrorAction SilentlyContinue
    Stop-Process -Id 14092 -Force
    Stop-Process -Id 12336 -Force -ErrorAction SilentlyContinue
    $tails = @()
    foreach ($lease in $leases) {
        $offset = [Math]::Max(0L, $lease.Length - 65536L)
        $null = $lease.Seek($offset, [IO.SeekOrigin]::Begin)
        $buffer = [byte[]]::new([int]($lease.Length - $offset))
        $count = $lease.Read($buffer, 0, $buffer.Length)
        if ($count -ne $buffer.Length -or $count -eq 0 -or $buffer[$count-1] -ne 10) { throw 'Archive tail incomplete; keep writer stopped for inspection' }
        $textTail = [Text.Encoding]::UTF8.GetString($buffer)
        $last = ($textTail.TrimEnd("`r", "`n") -split "`n")[-1].TrimEnd("`r")
        if (($last -split ',').Count -ne 18) { throw 'Unexpected archive tail columns; keep writer stopped for inspection' }
        $sha = [Security.Cryptography.SHA256]::Create()
        try { $tailHash = [BitConverter]::ToString($sha.ComputeHash($buffer)).Replace('-', '').ToLowerInvariant() } finally { $sha.Dispose() }
        $tails += @{file=[IO.Path]::GetFileName($lease.Name); bytes=$lease.Length; tail_sha256=$tailHash; final_row_complete=$true}
    }
    foreach ($lease in $leases) { $lease.Dispose() }
    $leases.Clear()
    $leaseSeconds = ([datetime]::UtcNow - $leaseStart).TotalSeconds
    foreach ($entry in $receipt.files.PSObject.Properties) {
        Copy-Item -LiteralPath (Join-Path $stagePath $entry.Name) -Destination (Join-Path $tradPath $entry.Name) -Force
    }
    $installed = $true
    foreach ($entry in $receipt.files.PSObject.Properties) {
        if ((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $tradPath $entry.Name)).Hash.ToLowerInvariant() -ne $entry.Value) { throw 'Installed source hash mismatch' }
    }
    $newAction = New-ScheduledTaskAction -Execute $oldAction.Execute -Argument $oldArgs.Replace($priorProfile,$nextProfile) -WorkingDirectory $oldAction.WorkingDirectory
    Set-ScheduledTask -TaskName $taskName -Action $newAction | Out-Null
    Enable-ScheduledTask -TaskName $taskName | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $stagePath 'RESEARCH_TASK_AFTER.xml') -Encoding utf8
    @{status='installed_recovery_started';utc=[datetime]::UtcNow.ToString('o');profile=$nextProfile;stopped_pids=@(8256,13856,22584,5592,14092,12336);practice_changed=$false;native_generation_changed=$false;archive_handover='all_68_original_files_read_locked_before_writer_stop';lease_seconds=$leaseSeconds;tails=$tails;files=$receipt.files} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $stagePath 'ACTUAL_HANDOVER.json') -Encoding utf8
    'Installed candle/health/settlement handover; selected recovery task started.'
} catch {
    $errorText = $_.Exception.Message
    foreach ($lease in $leases) { $lease.Dispose() }
    $leases.Clear()
    if ($managerStopped) {
        foreach ($entry in $receipt.original_files.PSObject.Properties) {
            Copy-Item -LiteralPath (Join-Path $stagePath ('retained_prior\'+$entry.Name)) -Destination (Join-Path $tradPath $entry.Name) -Force
        }
        Set-ScheduledTask -TaskName $taskName -Action $oldAction | Out-Null
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        # Archive-tail failure requires inspection before authorizing any writer.
        if ($errorText -notlike 'Archive tail*' -and $errorText -notlike 'Unexpected archive tail*') { Start-ScheduledTask -TaskName $taskName }
    }
    @{status='failed';utc=[datetime]::UtcNow.ToString('o');error=$errorText;manager_stopped=$managerStopped;files_install_completed=$installed;old_sources_and_task_restored=$managerStopped} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stagePath 'HANDOVER_FAILURE.json') -Encoding utf8
    throw
} finally {
    foreach ($lease in $leases) { $lease.Dispose() }
}
