param([string]$ReceiptName = 'dashboard_restart.json')
$ErrorActionPreference = 'Stop'
$Trad = 'C:\Users\zmoor\Documents\forex\trad'
$ReceiptPath = Join-Path $PSScriptRoot $ReceiptName
if (Test-Path -LiteralPath $ReceiptPath) { throw 'Restart receipt already exists.' }
$all = @(Get-CimInstance Win32_Process)
$supervisor = @($all | Where-Object {
    $_.Name -match '^(powershell|pwsh)\.exe$' -and $_.ProcessId -ne $PID -and
    $_.CommandLine -match 'oanda_always_on_supervisor\.ps1' -and
    $_.CommandLine -match '\s-ResearchCollectionOnly(?:\s|$)'
})
if ($supervisor.Count -ne 1 -or $supervisor[0].ProcessId -ne 14508) { throw 'Expected research supervisor changed.' }
$dash = @($all | Where-Object {
    $_.Name -match '^pythonw?\.exe$' -and
    $_.CommandLine -match ([regex]::Escape((Join-Path $Trad 'oanda_practice_live_dashboard.py'))+'(?:"|\s|$)')
})
if ($dash.Count -ne 2) { throw 'Expected one dashboard launcher/child pair.' }
$launcher = @($dash | Where-Object ParentProcessId -eq $supervisor[0].ProcessId)
if ($launcher.Count -ne 1) { throw 'Dashboard parent is not the research supervisor.' }
$child = @($dash | Where-Object ParentProcessId -eq $launcher[0].ProcessId)
if ($child.Count -ne 1) { throw 'Dashboard child ancestry differs.' }
$receipt = [ordered]@{
    requested_utc=[DateTime]::UtcNow.ToString('o')
    reason='Reload tested collection-status dashboard code; supervisor and collection workers remain running.'
    supervisor_pid=$supervisor[0].ProcessId
    before=@($dash | ForEach-Object { [pscustomobject]@{pid=$_.ProcessId;parent_pid=$_.ParentProcessId;created_utc=$_.CreationDate.ToUniversalTime().ToString('o')} })
    dashboard_source_sha256=(Get-FileHash -LiteralPath (Join-Path $Trad 'oanda_practice_live_dashboard.py') -Algorithm SHA256).Hash.ToLower()
    expected_recovery='Existing supervisor relaunches only live_dashboard on its next check.'
    stopped_pids=@($launcher[0].ProcessId,$child[0].ProcessId)
}
Stop-Process -Id $launcher[0].ProcessId -ErrorAction Stop
Stop-Process -Id $child[0].ProcessId -ErrorAction Stop
$receipt.stopped_utc=[DateTime]::UtcNow.ToString('o')
$receipt | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $ReceiptPath -Encoding utf8
$receipt | ConvertTo-Json -Depth 6
