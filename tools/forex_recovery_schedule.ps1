[CmdletBinding()]
param([switch]$Apply, [string]$EvidenceDirectory)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$profile = Get-Content -LiteralPath (Join-Path $root 'trad/config/operational_runtime_current_20260930.json') -Raw | ConvertFrom-Json
$expiry = [DateTimeOffset]::Parse($profile.recovery_until_utc)
$now = [DateTimeOffset]::UtcNow
if ($expiry -le $now.AddMinutes(10)) { throw 'Recovery expiry must leave at least ten minutes; do not silently extend it.' }
& (Join-Path $PSScriptRoot 'forex_pipeline.ps1') -Action Validate | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Existing recovery profile failed validation.' }
$names = @('Forex Operational Research Recovery 20260913', 'BIGTRIAD Unified Trading Dashboard')
$tasks = @($names | ForEach-Object { Get-ScheduledTask -TaskName $_ -ErrorAction Stop })
$pipeline = Join-Path $PSScriptRoot 'forex_pipeline.ps1'
if ($tasks[0].Actions.Count -ne 1 -or $tasks[0].Actions.Arguments -notlike ('*"'+$pipeline+'" -Action Start')) {
    throw 'Existing recovery action differs; reconcile instead of replacing it.'
}
$dashboard = Join-Path $root 'trad/oanda_practice_live_dashboard.py'
$boundary = Join-Path $root 'trad/oanda_dashboard_exclusive_server_v1.py'
$launcher = Join-Path $root 'trad/start_oanda_dashboard_logon_v1.ps1'
if ($tasks[1].Actions.Count -ne 1 -or
    ($tasks[1].Actions.Arguments -notlike ('*'+$dashboard+'*') -and
     $tasks[1].Actions.Arguments -notlike ('*'+$launcher+'*'))) { throw 'Unexpected dashboard task action.' }
# Reviewed source at f7bde8d; a future edit requires a new review, not automatic re-pinning.
$dashboardPin = 'df3fd6292bafb4582aebfbd357bf0572ecb37e2473feb0d875674e03d2806749'
$boundaryPin = '99e64617b717116f262c65a136e56395b3c433fb60b01d3fc9ec22982a5c5570'
& $launcher -ExpectedDashboardSha256 $dashboardPin -ExpectedBoundarySha256 $boundaryPin -CheckOnly | Out-Null
if ($Apply) {
    if (-not $EvidenceDirectory) { throw 'An evidence directory is required to preserve previous task XML.' }
    $evidence = [IO.Path]::GetFullPath($EvidenceDirectory)
    New-Item -ItemType Directory -Force -Path $evidence | Out-Null
    foreach ($task in $tasks) {
        $backup = Join-Path $evidence ($task.TaskName+'.before.xml')
        if (Test-Path -LiteralPath $backup) { throw 'Preserve prior task backup; use a new evidence directory.' }
        Export-ScheduledTask -TaskName $task.TaskName | Set-Content -LiteralPath $backup -Encoding Unicode
    }
    foreach ($task in $tasks) {
        $trigger = New-ScheduledTaskTrigger -Once -At ([DateTime]::Now.AddMinutes(1)) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration ($expiry-$now)
        $trigger.EndBoundary = $expiry.UtcDateTime.ToString('yyyy-MM-ddTHH:mm:ssZ')
        $trigger.Repetition.StopAtDurationEnd = $false
        # Preserve logon recovery; replace only earlier timed retries on reapply.
        $triggers = @($task.Triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger' }) + @($trigger)
        if ($task.TaskName -eq $names[1]) {
            $args = '-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "'+$launcher+'" -ExpectedDashboardSha256 '+$dashboardPin+' -ExpectedBoundarySha256 '+$boundaryPin
            $action = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument $args -WorkingDirectory (Join-Path $root 'trad')
            Set-ScheduledTask -TaskName $task.TaskName -Trigger $triggers -Action $action | Out-Null
        } else {
            Set-ScheduledTask -TaskName $task.TaskName -Trigger $triggers | Out-Null
        }
    }
}
$result = foreach ($name in $names) {
    $task = Get-ScheduledTask -TaskName $name
    $info = Get-ScheduledTaskInfo -TaskName $name
    [pscustomobject]@{name=$name;state=[string]$task.State;next_run=$info.NextRunTime;
        triggers=@($task.Triggers | ForEach-Object { @{kind=$_.CimClass.CimClassName;interval=$_.Repetition.Interval;end=$_.EndBoundary} })}
}
[pscustomobject]@{applied=[bool]$Apply;expiry_utc=$expiry.ToString('o');tasks=@($result);can_place_orders=$false} | ConvertTo-Json -Depth 6
