$ErrorActionPreference = "Stop"

$Root = "D:\forex\trad"
$Python = "C:\Users\zmoor\AppData\Roaming\uv\python\cpython-3.12-windows-x86_64-none\python.exe"
$SitePackages = Join-Path $Root "..venv\Lib\site-packages"
$RuntimeLogs = Join-Path $Root "data\runtime_logs"
$LauncherPath = Join-Path $Root "run_oanda_tech_prod_live_account_manager.py"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$Out = Join-Path $RuntimeLogs "manual_restart_tech_manager_network_${Stamp}.out.log"
$Err = Join-Path $RuntimeLogs "manual_restart_tech_manager_network_${Stamp}.err.log"
$Summary = Join-Path $RuntimeLogs "manual_restart_tech_manager_network_latest.json"

New-Item -ItemType Directory -Force -Path $RuntimeLogs | Out-Null

$env:TRAD_PROJECT_ROOT = $Root
$env:PYTHONUTF8 = "1"

$proc = Start-Process -FilePath $Python -ArgumentList @("-S", $LauncherPath, "--no-scan-on-launch") -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $Out -RedirectStandardError $Err -PassThru

[ordered]@{
    time = (Get-Date).ToString("o")
    pid = $proc.Id
    script = $LauncherPath
    stdout = $Out
    stderr = $Err
} | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $Summary -Encoding UTF8

Write-Output ("started pid={0} stdout={1} stderr={2}" -f $proc.Id, $Out, $Err)
