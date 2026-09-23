param([switch]$Execute)
$ErrorActionPreference = 'Stop'
$project = 'C:\Users\zmoor\Documents\forex\trad'
$workspace = Split-Path -Parent $project
$supervisorPath = Join-Path $project 'oanda_always_on_supervisor.ps1'
$receiptPath = Join-Path $PSScriptRoot 'SERVICE_RELOAD_RECEIPT_20260908.json'
$activationPath = Join-Path $PSScriptRoot 'DEPLOYMENT_ACTIVATION_20260908.json'
$targets = @('oanda_practice_live_dashboard.py','oanda_project_integrity_audit.py','oanda_storage_headroom_guard.py')
$targetPaths = @($targets | ForEach-Object { Join-Path $project $_ })

function Get-ProjectProcesses {
    @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^(python|pythonw|pwsh|powershell)(\.exe)?$' } | ForEach-Object {
        $match=[regex]::Match([string]$_.CommandLine,'(?i)(?:"([^"]+\.(?:py|ps1))"|([^\s"]+\.(?:py|ps1)))(?:\s|$)')
        $path=if($match.Groups[1].Success){$match.Groups[1].Value}else{$match.Groups[2].Value}
        if($path.StartsWith($project+'\',[StringComparison]::OrdinalIgnoreCase)) {
            [pscustomobject]@{ Process=$_; Script=$path }
        }
    })
}
function Public-Identity($row) {
    @{pid=$row.Process.ProcessId;parent_pid=$row.Process.ParentProcessId;script=$row.Script;
      executable=$row.Process.ExecutablePath;created_utc=$row.Process.CreationDate.ToUniversalTime().ToString('o')}
}
function Assert-SameProcess($row) {
    $fresh=Get-CimInstance Win32_Process -Filter ('ProcessId='+$row.Process.ProcessId)
    if($null -eq $fresh) {return $null}
    if($fresh.CreationDate -ne $row.Process.CreationDate -or $fresh.CommandLine -cne $row.Process.CommandLine -or $fresh.ExecutablePath -cne $row.Process.ExecutablePath) {
        throw 'Process identity changed; targeted reload refused'
    }
    return $fresh
}

$all=Get-ProjectProcesses
$supervisors=@($all | Where-Object { $_.Script -ieq $supervisorPath })
if($supervisors.Count -ne 1) {throw 'Exactly one canonical supervisor required'}
$supervisor=$supervisors[0]
if($supervisor.Process.CommandLine -notmatch '(?i)(?:^|\s)-ResearchCollectionOnly(?:\s|$)' -or
   $supervisor.Process.CommandLine -notmatch '(?i)(?:^|\s)-SafeCoreOnly(?:\s|$)') {throw 'Existing research-only safe-core mode required'}
if(@($all | Where-Object { $_.Script -match '(?i)watchdog\.ps1$' }).Count) {throw 'Concurrent watchdog requires separate coordination'}
$restart=@($all | Where-Object { $targetPaths -contains $_.Script })
foreach($name in $targets) {
    if(@($restart | Where-Object { (Split-Path -Leaf $_.Script) -eq $name }).Count -notin @(1,2)) {throw 'Unexpected service process topology'}
}
$preserve=@($all | Where-Object { $_.Script -ine $supervisorPath -and $targetPaths -notcontains $_.Script -and (Split-Path -Parent $_.Script) -ieq $project -and (Split-Path -Leaf $_.Script) -like 'oanda_*.py' })
$plan=@{schema_version='targeted_research_service_reload_20260908';research_only=$true;can_place_orders=$false;
    can_promote=$false;can_authorize=$false;supervisor=(Public-Identity $supervisor);
    restart=@($restart|ForEach-Object {Public-Identity $_});preserve=@($preserve|ForEach-Object {Public-Identity $_});
    model_quote_account_news_collectors_restarted=$false;raw_command_lines_retained=$false}
if(-not $Execute) {$plan|ConvertTo-Json -Depth 8;exit 0}
if(Test-Path -LiteralPath $receiptPath) {throw 'Reload receipt already exists; do not replay'}
$activation=Get-Content -LiteralPath $activationPath -Raw|ConvertFrom-Json
if($activation.activation.status -ne 'activated_empty' -or @($activation.activation.ledgers).Count -ne 68) {throw 'Verified 68-ledger activation required'}
$configPath=Join-Path $project 'config\joint_price_news_study_v3_20260908.json'
if((Get-FileHash -LiteralPath $configPath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $activation.activation.registry_file_sha256) {throw 'Activated registry changed'}
foreach($binding in $activation.activation.source_bindings.PSObject.Properties) {
    if((Get-FileHash -LiteralPath (Join-Path $project $binding.Name) -Algorithm SHA256).Hash.ToLowerInvariant() -cne $binding.Value) {throw 'Activated source changed'}
}
$events=[Collections.Generic.List[object]]::new()
$plan.started_utc=[DateTime]::UtcNow.ToString('o')
try {
    $same=Assert-SameProcess $supervisor
    if($null -eq $same) {throw 'Supervisor exited before coordinated stop'}
    Stop-Process -Id $same.ProcessId -Force -ErrorAction Stop
    $events.Add(@{action='stopped_supervisor';pid=$same.ProcessId;utc=[DateTime]::UtcNow.ToString('o')})
    # Stop only observed display/health/storage processes. Leave all study and input workers running.
    foreach($row in $restart) {
        $same=Assert-SameProcess $row
        if($null -ne $same) {
            Stop-Process -Id $same.ProcessId -Force -ErrorAction Stop
            $events.Add(@{action='stopped_service';pid=$same.ProcessId;script=$row.Script;utc=[DateTime]::UtcNow.ToString('o')})
        }
    }
    $remaining=@(Get-ProjectProcesses|Where-Object { $_.Script -ieq $supervisorPath -or $targetPaths -contains $_.Script })
    if($remaining.Count) {throw 'A selected service remained or raced the reload'}
    foreach($row in $preserve) {if($null -eq (Assert-SameProcess $row)) {throw 'Preserved process unexpectedly exited'}}
    $stamp=[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
    $arguments=@('-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',$supervisorPath,
        '-Root',$workspace,'-AccountKey','OANDA_ACCOUNT_ID_DUM4','-RunLabel','unified-signal-confidence-matrix-v10',
        '-IntervalSec','30','-ChildDurationSec','604800','-SafeCoreOnly','-ResearchCollectionOnly')
    $launched=Start-Process -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -ArgumentList $arguments `
        -WorkingDirectory $project -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $PSScriptRoot "supervisor_${stamp}.out.log") `
        -RedirectStandardError (Join-Path $PSScriptRoot "supervisor_${stamp}.err.log")
    $events.Add(@{action='started_research_supervisor';pid=$launched.Id;utc=[DateTime]::UtcNow.ToString('o')})
    $plan.status='launched_pending_live_verification'
} catch {
    $plan.status='partial_reload_requires_review'
    $plan.error=$_.Exception.Message
    throw
} finally {
    $plan.events=@($events.ToArray())
    $plan.completed_utc=[DateTime]::UtcNow.ToString('o')
    $plan|ConvertTo-Json -Depth 10|Set-Content -LiteralPath $receiptPath -Encoding UTF8
}
$plan|ConvertTo-Json -Depth 10
