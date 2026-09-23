param([string]$OutputName = 'stopped_state_final.json')
$workerRows = @()
$reviewShells = @()
foreach ($process in Get-CimInstance Win32_Process) {
    if ($process.ProcessId -eq $PID) { continue }
    if ($process.Name -match '^pythonw?\.exe$' -and $process.CommandLine -match '(?i)(?:^|[\s"\\/])oanda_[a-z0-9_]+\.py') {
        $workerRows += [pscustomobject]@{ ProcessId=$process.ProcessId; Name=$process.Name; Kind='python_project_entrypoint' }
    } elseif ($process.Name -match '^(powershell|pwsh)\.exe$' -and $process.CommandLine -match '(?i)(oanda_always_on_supervisor\.ps1|ForexGptOrigConstantRotationDemoWatchdog)') {
        if ($process.CommandLine -match '(?i)(Get-Content|Select-String|rg )' -and $process.CommandLine -notmatch '(?i)-File\s') {
            $reviewShells += [pscustomobject]@{ ProcessId=$process.ProcessId; Name=$process.Name; Kind='read_only_review_shell' }
        } else {
            $workerRows += [pscustomobject]@{ ProcessId=$process.ProcessId; Name=$process.Name; Kind='supervision_candidate' }
        }
    }
}
$taskRows = @(Get-ScheduledTask | Where-Object { $_.TaskName -match '^Forex' } | ForEach-Object {
    [pscustomobject]@{Name=$_.TaskName;State=$_.State.ToString()}
})
$receipt = [ordered]@{
    observed_utc=[DateTime]::UtcNow.ToString('o')
    runtime_processes=$workerRows
    excluded_read_only_review_shells=$reviewShells
    forex_tasks=$taskRows
    supervisor_executed=$false
    broker_requests=0
    inspection='Process entrypoint/command metadata and task state; no service or broker call'
}
$receipt | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $PSScriptRoot $OutputName) -Encoding utf8
$receipt | ConvertTo-Json -Depth 6
if ($workerRows.Count -gt 0) { throw 'A potential Forex runtime process requires inspection.' }
if (@($taskRows | Where-Object { $_.State -ne 'Disabled' }).Count -gt 0) { throw 'A Forex task is unexpectedly enabled.' }
