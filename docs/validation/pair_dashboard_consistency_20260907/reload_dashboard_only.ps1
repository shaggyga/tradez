param([Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-f]{64}$')][string]$ExpectedDashboardSha256,
      [ValidatePattern('^[A-Z0-9_]+\.json$')][string]$VerificationName='DASHBOARD_CONSISTENCY_RUNTIME_VERIFICATION_20260907.json')
$ErrorActionPreference='Stop'
$projectRoot='C:\Users\zmoor\Documents\forex\trad'
$evidenceRoot='C:\Users\zmoor\Documents\forex\pair_dashboard_consistency_20260907'
if([IO.Path]::GetFullPath($PSScriptRoot) -ne $evidenceRoot){throw 'Unexpected follow-up workspace'}
if(Test-Path -LiteralPath (Join-Path $evidenceRoot $VerificationName)){throw 'Verification receipt already exists; select a new receipt name before reloading'}
$python='C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe'
$dashboardPath=Join-Path $projectRoot 'oanda_practice_live_dashboard.py'
if((Get-FileHash -LiteralPath $dashboardPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $ExpectedDashboardSha256){throw 'Dashboard differs from tested source'}
$stamp=Get-Date -Format 'yyyyMMdd_HHmmss_fff'
$baselinePath=Join-Path $evidenceRoot "DASHBOARD_RELOAD_BEFORE_${stamp}.json"
& $python (Join-Path $evidenceRoot 'verify_runtime.py') --preflight-only --output $baselinePath
if($LASTEXITCODE -ne 0){throw 'Read-only source and process preflight failed'}
$baseline=Get-Content -LiteralPath $baselinePath -Raw | ConvertFrom-Json
$all=@(Get-CimInstance Win32_Process)
$dashboardPattern=[regex]::Escape($dashboardPath)
$dashboard=@($all | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match "(?:^|\s)`"?$dashboardPattern`"?(?:\s|$)"})
if($dashboard.Count -ne 2 -or (Compare-Object @($baseline.process_checks.dashboard_pids | Sort-Object) @($dashboard.ProcessId | Sort-Object))){throw 'Dashboard identity changed after preflight'}
function Stop-VerifiedDashboard {
    param($Observed)
    $current=@(Get-CimInstance Win32_Process -Filter ('ProcessId = '+[int]$Observed.ProcessId))
    if($current.Count -eq 0){return} # The paired launcher/child may already exit.
    if($current.Count -ne 1 -or $current[0].Name -ne $Observed.Name -or $current[0].CommandLine -ne $Observed.CommandLine -or $current[0].CreationDate -ne $Observed.CreationDate){throw 'Dashboard PID identity changed'}
    Stop-Process -Id $Observed.ProcessId -Force
}
# The existing supervisor replaces the dashboard. No other process is stopped
# and no direct process launch, study activation or supervisor restart occurs.
foreach($process in $dashboard){Stop-VerifiedDashboard $process}
$deadline=(Get-Date).AddSeconds(30)
$replacement=@()
do {
    $current=@(Get-CimInstance Win32_Process)
    $supervisor=@($current | Where-Object {$_.ProcessId -eq 22400})
    if($supervisor.Count -ne 1){throw 'Preserved supervisor disappeared'}
    $replacement=@($current | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match "(?:^|\s)`"?$dashboardPattern`"?(?:\s|$)" -and $_.ProcessId -notin $dashboard.ProcessId})
    if($replacement.Count -eq 2){
        try {
            $apiCheck=Invoke-WebRequest -Uri 'http://127.0.0.1:8765/api/main' -UseBasicParsing -TimeoutSec 2
            $apiCheckValue=$apiCheck.Content | ConvertFrom-Json
            if($apiCheckValue.pair_local_forecasts.status -eq 'current'){break}
        } catch { }
    }
    Start-Sleep -Milliseconds 500
} while((Get-Date) -lt $deadline)
if($replacement.Count -ne 2){throw 'Existing supervisor has not replaced dashboard within 30 seconds; no other process changed by this script'}
if((Get-FileHash -LiteralPath $dashboardPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $ExpectedDashboardSha256){throw 'Dashboard source changed during reload'}
$verificationPath=Join-Path $evidenceRoot $VerificationName
& $python (Join-Path $evidenceRoot 'verify_runtime.py') --baseline $baselinePath --samples 1 --output $verificationPath
if($LASTEXITCODE -ne 0){throw 'Dashboard restarted; read-only verification failed; no automatic second reload performed'}
[ordered]@{schema_version='pair_dashboard_only_reload_v1_20260907';status='passed';observed_utc=(Get-Date).ToUniversalTime().ToString('o');preserved_supervisor_pid=22400;prior_dashboard_pids=@($dashboard.ProcessId);new_dashboard_pids=@($replacement.ProcessId);dashboard_source_sha256=$ExpectedDashboardSha256;before_receipt=$baselinePath;after_receipt=$verificationPath;supervisor_restart_performed=$false;study_activation_performed=$false;direct_process_launches=0;orders_enabled=$false} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $evidenceRoot "DASHBOARD_ONLY_RELOAD_${stamp}.json") -Encoding utf8
