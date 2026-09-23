$ErrorActionPreference = 'Stop'
$repairTrad = 'C:\Users\zmoor\Documents\forex\trad'
$repairOut = 'C:\Users\zmoor\Documents\forex\pair_forecast_repair_v2_20260907'
$repairAll = @(Get-CimInstance Win32_Process)
$repairSupPath = Join-Path $repairTrad 'oanda_always_on_supervisor.ps1'
$repairDashPath = Join-Path $repairTrad 'oanda_practice_live_dashboard.py'
$repairSupervisors = @($repairAll | Where-Object {
    $_.Name -match '^(powershell|pwsh)\.exe$' -and $_.CommandLine -match [regex]::Escape($repairSupPath)
})
if ($repairSupervisors.Count -ne 1 -or $repairSupervisors[0].ProcessId -ne 22400 -or
    $repairSupervisors[0].CommandLine -notmatch '\s-ResearchCollectionOnly(?:\s|$)' -or
    $repairSupervisors[0].CommandLine -notmatch '\s-SafeCoreOnly(?:\s|$)') {
    throw 'Expected one verified research supervisor PID22400; no processes stopped.'
}
$repairDash = @($repairAll | Where-Object {
    $_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -match [regex]::Escape($repairDashPath)
})
if ($repairDash.Count -ne 2 -or (@($repairDash.ProcessId | Sort-Object) -join ',') -ne '11604,27724') {
    throw 'Dashboard process generation changed; no processes stopped.'
}
$repairBefore = @($repairAll | Where-Object {
    $_.Name -match '^(python(w)?|powershell|pwsh)\.exe$' -and $_.CommandLine -match [regex]::Escape($repairTrad)
} | Select-Object ProcessId,ParentProcessId,CreationDate,@{Name='script';Expression={
    [regex]::Match($_.CommandLine,'oanda_[a-z0-9_]+\.(py|ps1)').Value
}})
@{observed_utc=[DateTime]::UtcNow.ToString('o');processes=$repairBefore} | ConvertTo-Json -Depth 5 |
    Set-Content -LiteralPath (Join-Path $repairOut 'RUNTIME_BEFORE_RELOAD.json') -Encoding UTF8
Stop-Process -Id 22400 -Force
foreach ($repairPid in @(11604,27724)) { Stop-Process -Id $repairPid -Force -ErrorAction SilentlyContinue }
& (Join-Path $repairTrad 'start_oanda_research_collection.ps1') -Root 'C:\Users\zmoor\Documents\forex'
if ($LASTEXITCODE -ne 0 -and $null -ne $LASTEXITCODE) { throw 'Research launcher returned a failure' }
@{observed_utc=[DateTime]::UtcNow.ToString('o');stopped_supervisor=22400;stopped_dashboard=@(11604,27724);
    quote_news_account_v1_v2_collectors_preserved=$true;requested_mode='ResearchCollectionOnly'} |
    ConvertTo-Json | Set-Content -LiteralPath (Join-Path $repairOut 'RUNTIME_RELOAD_DISPATCH.json') -Encoding UTF8
