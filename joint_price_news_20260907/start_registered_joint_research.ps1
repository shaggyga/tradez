$ErrorActionPreference = 'Stop'
$jointTrad = 'C:\Users\zmoor\Documents\forex\trad'
$jointOut = 'C:\Users\zmoor\Documents\forex\joint_price_news_20260907'
$jointAll = @(Get-CimInstance Win32_Process)
$jointSupervisors = @($jointAll | Where-Object {
    $_.Name -match '^(powershell|pwsh)\.exe$' -and
    $_.CommandLine -match [regex]::Escape((Join-Path $jointTrad 'oanda_always_on_supervisor.ps1')) -and
    $_.CommandLine -match '\s-File\s'
})
if ($jointSupervisors.Count -ne 1 -or $jointSupervisors[0].ProcessId -ne 29708 -or
    $jointSupervisors[0].CommandLine -notmatch '\s-ResearchCollectionOnly(?:\s|$)' -or
    $jointSupervisors[0].CommandLine -notmatch '\s-SafeCoreOnly(?:\s|$)') {
    throw 'Expected exact research supervisor29708; no process stopped.'
}
if (-not (Test-Path -LiteralPath (Join-Path $jointOut 'ACTIVATION_RECEIPT_20260907.json'))) {
    throw 'Explicit activation receipt missing; no process stopped.'
}
$jointBefore = @($jointAll | Where-Object {
    $_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -match [regex]::Escape($jointTrad)
} | Select-Object ProcessId,ParentProcessId,CreationDate,@{Name='script';Expression={
    [regex]::Match($_.CommandLine,'oanda_[a-z0-9_]+\.py').Value
}})
@{observed_utc=[DateTime]::UtcNow.ToString('o');supervisor=29708;python_processes=$jointBefore} |
    ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $jointOut 'RUNTIME_BEFORE_JOINT_START_20260907.json') -Encoding utf8
Stop-Process -Id 29708 -Force
& (Join-Path $jointTrad 'start_oanda_research_collection.ps1') -Root 'C:\Users\zmoor\Documents\forex'
@{observed_utc=[DateTime]::UtcNow.ToString('o');stopped_supervisor=29708;stopped_python_workers=@();
    requested_mode='ResearchCollectionOnly';joint_ledgers_previously_initialized=$true;orders_enabled=$false} |
    ConvertTo-Json -Depth 3 | Set-Content -LiteralPath (Join-Path $jointOut 'JOINT_START_DISPATCH_20260907.json') -Encoding utf8
