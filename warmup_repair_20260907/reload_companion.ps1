$ErrorActionPreference='Stop'
$projectRoot='C:\Users\zmoor\Documents\forex\trad'
$outRoot=Split-Path -Parent $PSCommandPath
$supervisorPattern=[regex]::Escape((Join-Path $projectRoot 'oanda_always_on_supervisor.ps1'))
$all=@(Get-CimInstance Win32_Process)
$supervisor=@($all | Where-Object { $_.Name -eq 'powershell.exe' -and $_.CommandLine -match "\s-File\s+`"?$supervisorPattern`"?(?:\s|$)" })
if($supervisor.Count -ne 1 -or $supervisor[0].CommandLine -notmatch '\s-ResearchCollectionOnly(?:\s|$)'){throw 'Expected research-only supervisor'}
$dashboardPattern=[regex]::Escape((Join-Path $projectRoot 'oanda_practice_live_dashboard.py'))
$dashboard=@($all | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match "(?:^|\s)`"?$dashboardPattern`"?(?:\s|$)" })
if($dashboard.Count -ne 2){throw 'Expected dashboard launcher/child'}
Stop-Process -Id $supervisor[0].ProcessId -Force
foreach($proc in $dashboard){Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue}
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' (Join-Path $outRoot 'activate_eurusd.py')
if($LASTEXITCODE -ne 0){throw 'Activation failed'}
$launch=& (Join-Path $projectRoot 'start_oanda_research_collection.ps1')
if($LASTEXITCODE -ne 0){throw 'Research launcher failed'}
[ordered]@{observed_utc=(Get-Date).ToUniversalTime().ToString('o');prior_supervisor=$supervisor[0].ProcessId;stopped_dashboard_pids=@($dashboard.ProcessId);launch_result=$launch;existing_studies_quotes_news_archive_preserved=$true;allowed_worker_count=13;orders_enabled=$false} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $outRoot 'COMPANION_RUNTIME_RELOAD.json') -Encoding utf8
$launch
