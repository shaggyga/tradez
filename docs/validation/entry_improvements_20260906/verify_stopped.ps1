param([string]$OutputName = 'runtime_verification.json')
$ErrorActionPreference = 'Stop'
$projectRoot = 'C:\Users\zmoor\Documents\forex\trad'
$processes = @(Get-CimInstance Win32_Process | Where-Object {
    $_.ProcessId -ne $PID -and $_.Name -match '^(pythonw?|powershell|pwsh)\.exe$' -and
    ([string]$_.CommandLine -match [regex]::Escape($projectRoot)) -and
    [string]$_.CommandLine -notmatch '(?i)(entry_improvements_20260906|Get-CimInstance|pytest|Get-Content)'
} | ForEach-Object {
    $scriptMatch = [regex]::Match([string]$_.CommandLine, '(?i)([a-z0-9_]+\.(py|ps1))(?:"|\s|$)')
    [pscustomobject]@{pid=$_.ProcessId; name=$_.Name; script=$scriptMatch.Groups[1].Value}
})
$tasks = @(Get-ScheduledTask | Where-Object TaskName -match '^Forex' | ForEach-Object {
    [pscustomobject]@{name=$_.TaskName;state=$_.State.ToString()}
})
$result = [ordered]@{
    schema_version='forex_stopped_verification_v1'; observed_utc=[DateTime]::UtcNow.ToString('o')
    canonical_project=$projectRoot;remaining_project_processes=$processes.Count
    project_processes=$processes;scheduled_tasks=$tasks
    read_only_observation=$true;runtime_started=$false;orders_submitted=0
    scope='Canonical project Python/PowerShell process commands; this audit/test harness excluded. No process or task mutation.'
}
$outputPath = Join-Path $PSScriptRoot $OutputName
if (Test-Path -LiteralPath $outputPath) { throw 'Choose a new receipt filename.' }
$result | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $outputPath -Encoding utf8
$result | ConvertTo-Json -Depth 5
if ($processes.Count -ne 0 -or @($tasks | Where-Object state -ne 'Disabled').Count -ne 0) { exit 1 }
