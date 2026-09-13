param([switch]$CheckOnly, [switch]$Once)

$ErrorActionPreference = 'Stop'
$RecoverySchema = 'practice_recovery_launcher_v1_20260910'
$RecoveryRoot = [IO.Path]::GetFullPath($PSScriptRoot)
$RecoveryTrial = 'practice007_joint_v3_20260909_v1'
$RecoveryStop = 1789159500.0
$RecoveryState = Join-Path $RecoveryRoot ('data\oanda_training_manager\' + $RecoveryTrial)
$RecoveryManifest = Join-Path $RecoveryRoot 'config\practice_recovery_launcher_20260910.json'

function Stop-RecoveryCheck([string]$Code) { throw [InvalidOperationException]::new($Code) }
function Get-RecoveryEpoch { return ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0) }
function Get-RecoveryNormalizedPath([string]$Path) {
    return [IO.Path]::GetFullPath($Path.Replace('/', '\')).TrimEnd('\').ToLowerInvariant()
}
function Assert-RecoveryPlainPath([string]$Path) {
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -LiteralPath $cursor -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { Stop-RecoveryCheck 'reparse_path_refused' }
        }
        $parent = [IO.Path]::GetDirectoryName($cursor)
        if ($parent -eq $cursor) { break }
        $cursor = $parent
    }
}
function Read-RecoveryBytes([string]$Path, [int]$Limit = 4194304) {
    Assert-RecoveryPlainPath $Path
    $stream = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, ([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete))
    try {
        if ($stream.Length -gt $Limit) { Stop-RecoveryCheck 'file_bound_exceeded' }
        $memory = [IO.MemoryStream]::new()
        try {
            $buffer = New-Object byte[] 65536
            while (($count = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                if ($memory.Length + $count -gt $Limit) { Stop-RecoveryCheck 'file_bound_exceeded' }
                $memory.Write($buffer, 0, $count)
            }
            return ,$memory.ToArray()
        } finally { $memory.Dispose() }
    } finally { $stream.Dispose() }
}
function Get-RecoveryHash([byte[]]$Raw) {
    $hasher = [Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($hasher.ComputeHash($Raw))).Replace('-', '').ToLowerInvariant() }
    finally { $hasher.Dispose() }
}
function Convert-RecoveryJson([byte[]]$Raw) {
    return ([Text.Encoding]::UTF8.GetString($Raw).TrimStart([char]0xFEFF) | ConvertFrom-Json)
}
function Get-RecoveryBindings([string]$Root, [string]$ManifestPath) {
    $manifestRaw = Read-RecoveryBytes $ManifestPath 65536
    $manifest = Convert-RecoveryJson $manifestRaw
    $names = @('start_oanda_practice_recovery_v1.ps1','start_oanda_research_collection.ps1','start_oanda_practice_trial_v1.ps1','oanda_always_on_supervisor.ps1')
    $actual = @($manifest.source_bindings.PSObject.Properties.Name | Sort-Object)
    if (($actual -join '|') -cne (($names | Sort-Object) -join '|')) { Stop-RecoveryCheck 'manifest_source_inventory' }
    if ($manifest.schema_version -cne 'practice_recovery_launcher_v1_20260910' -or
        $manifest.enabled -isnot [bool] -or -not $manifest.enabled -or
        (Get-RecoveryNormalizedPath $manifest.project_root) -cne (Get-RecoveryNormalizedPath $Root) -or
        $manifest.trial_id -cne 'practice007_joint_v3_20260909_v1' -or
        $manifest.stop_epoch -is [bool] -or $manifest.stop_epoch -ne 1789159500.0) { Stop-RecoveryCheck 'manifest_scope_or_disabled' }
    foreach ($name in $names) {
        $expected = [string]$manifest.source_bindings.$name
        if ($expected -cnotmatch '^[0-9a-f]{64}$' -or (Get-RecoveryHash (Read-RecoveryBytes (Join-Path $Root $name))) -cne $expected) {
            Stop-RecoveryCheck 'launcher_source_binding_changed'
        }
    }
    $configPath = Join-Path $Root 'config\practice007_joint_v3_20260909_v1.json'
    $configRaw = Read-RecoveryBytes $configPath 65536
    if ([string]$manifest.trial_config_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
        (Get-RecoveryHash $configRaw) -cne $manifest.trial_config_sha256) { Stop-RecoveryCheck 'trial_config_binding_changed' }
    $config = Convert-RecoveryJson $configRaw
    if ($config.schema_version -cne 'practice_trial_runner_v1_20260909' -or
        $config.trial_id -cne 'practice007_joint_v3_20260909_v1' -or $config.environment -cne 'practice' -or
        $config.account_label -cne 'practice_007' -or $config.enabled -isnot [bool] -or -not $config.enabled -or
        $config.user_authorized_practice_trial -isnot [bool] -or -not $config.user_authorized_practice_trial -or
        $config.stop_epoch -is [bool] -or $config.stop_epoch -ne 1789159500.0) { Stop-RecoveryCheck 'trial_scope_or_disabled' }
    return @{manifest_sha256=(Get-RecoveryHash $manifestRaw);trial_config_sha256=(Get-RecoveryHash $configRaw);source_bindings=$manifest.source_bindings}
}
function Get-RecoveryTokens([string]$CommandLine) {
    return @([regex]::Matches($CommandLine, '"[^"\r\n]*"|[^\s"]+') | ForEach-Object { $_.Value.Trim('"') })
}
function Get-RecoveryInventory([string]$Root, [object[]]$Processes) {
    $supervisors = @(); $conflicts = @(); $watchdogs = @(); $workers = @()
    $expectedRoot = Get-RecoveryNormalizedPath ([IO.Path]::GetDirectoryName($Root))
    foreach ($process in $Processes) {
        $tokens = @(Get-RecoveryTokens ([string]$process.CommandLine))
        if ($process.Name -match '^(powershell|pwsh)\.exe$') {
            for ($i=0; $i -lt ($tokens.Count-1); $i++) {
                if ($tokens[$i] -ine '-File') { continue }
                $file = $tokens[$i+1].Replace('/', '\')
                if ([IO.Path]::GetFileName($file) -ieq 'oanda_always_on_supervisor.ps1') {
                    $exact = (Get-RecoveryNormalizedPath $file) -ceq (Get-RecoveryNormalizedPath (Join-Path $Root 'oanda_always_on_supervisor.ps1'))
                    $rootValues = @(); for ($j=0; $j -lt ($tokens.Count-1); $j++) { if ($tokens[$j] -ieq '-Root') { $rootValues += (Get-RecoveryNormalizedPath $tokens[$j+1]) } }
                    if ($exact -and $tokens -icontains '-ResearchCollectionOnly' -and $tokens -icontains '-SafeCoreOnly' -and $rootValues.Count -eq 1 -and $rootValues[0] -ceq $expectedRoot) { $supervisors += [int]$process.ProcessId }
                    else { $conflicts += [int]$process.ProcessId }
                }
                if ([IO.Path]::GetFileName($file) -ieq 'start_oanda_practice_trial_v1.ps1' -and
                    (Get-RecoveryNormalizedPath $file) -ceq (Get-RecoveryNormalizedPath (Join-Path $Root 'start_oanda_practice_trial_v1.ps1'))) { $watchdogs += [int]$process.ProcessId }
            }
        }
        if ($process.Name -match '^pythonw?\.exe$') {
            foreach ($token in $tokens) {
                if ($token.Replace('/', '\') -match '(?i)\\oanda_practice_trial_runner_v1\.py$' -and
                    (Get-RecoveryNormalizedPath $token) -ceq (Get-RecoveryNormalizedPath (Join-Path $Root 'oanda_practice_trial_runner_v1.py'))) { $workers += [int]$process.ProcessId }
            }
        }
    }
    return @{supervisor_pids=@($supervisors | Sort-Object -Unique);conflicting_supervisor_pids=@($conflicts | Sort-Object -Unique);
        trial_watchdog_pids=@($watchdogs | Sort-Object -Unique);trial_worker_pids=@($workers | Sort-Object -Unique)}
}
function Get-RecoveryDecision([hashtable]$Inventory, [double]$Now, [bool]$Completed) {
    if ([double]::IsNaN($Now) -or [double]::IsInfinity($Now) -or $Now -le 0) { Stop-RecoveryCheck 'invalid_observation_clock' }
    if ($Completed) { return 'completed_flat' }
    if ($Inventory.conflicting_supervisor_pids.Count -gt 0 -or $Inventory.supervisor_pids.Count -gt 1) { return 'conflicting_supervisor_refused' }
    if ($Inventory.trial_watchdog_pids.Count -gt 1 -or $Inventory.trial_worker_pids.Count -gt 2) { return 'duplicate_trial_refused' }
    if ($Inventory.supervisor_pids.Count -eq 0 -and $Now -lt 1789159500.0) { return 'start_research' }
    if ($Inventory.trial_watchdog_pids.Count -gt 0 -or $Inventory.trial_worker_pids.Count -gt 0) { return 'already_running' }
    if ($Now -ge 1789159500.0) { return 'start_trial_close_only' }
    if ($Inventory.supervisor_pids.Count -eq 1) { return 'start_trial' }
    return 'awaiting_research_supervisor'
}
function Get-RecoveryInspection {
    $binding = Get-RecoveryBindings $RecoveryRoot $RecoveryManifest
    if ($binding.source_bindings.'start_oanda_practice_recovery_v1.ps1' -cne $RecoveryLoadedSourceSha) { Stop-RecoveryCheck 'running_launcher_source_changed' }
    $completed = $false
    $statusPath = Join-Path $RecoveryState 'status.json'
    if (Test-Path -LiteralPath $statusPath) {
        $status = Convert-RecoveryJson (Read-RecoveryBytes $statusPath 1048576)
        $now = Get-RecoveryEpoch
        if ($status.schema_version -cne 'practice_trial_runner_v1_20260909' -or $status.trial_id -cne $RecoveryTrial -or
            $status.environment -cne 'practice' -or $status.account_label -cne 'practice_007' -or
            $status.observed_epoch -is [bool] -or $status.observed_epoch -le 0 -or $status.observed_epoch -gt $now -or
            $status.stop_epoch -ne $RecoveryStop) { Stop-RecoveryCheck 'trial_status_identity_or_clock' }
        $completed = $status.state -ceq 'completed_flat'
    }
    $inventory = Get-RecoveryInventory $RecoveryRoot @(Get-CimInstance Win32_Process -ErrorAction Stop)
    $now = Get-RecoveryEpoch
    return @{schema_version=$RecoverySchema;observed_epoch=(Get-RecoveryEpoch);pid=$PID;practice_only=$true;
        phase=(Get-RecoveryDecision $inventory $now $completed);inventory=$inventory;bindings=$binding;stop_epoch=$RecoveryStop;
        check_only=[bool]($CheckOnly -or $Once);broker_action_performed=$false}
}
function Write-RecoveryStatus([hashtable]$Status) {
    Assert-RecoveryPlainPath $RecoveryState
    [IO.Directory]::CreateDirectory($RecoveryState) | Out-Null
    $path = Join-Path $RecoveryState 'bootstrap.json'
    $temp = Join-Path $RecoveryState ('bootstrap.' + $PID + '.tmp')
    Assert-RecoveryPlainPath $path; Assert-RecoveryPlainPath $temp
    $raw = [Text.UTF8Encoding]::new($false).GetBytes(($Status | ConvertTo-Json -Depth 10 -Compress))
    $stream = [IO.File]::Open($temp,[IO.FileMode]::Create,[IO.FileAccess]::Write,[IO.FileShare]::None)
    try { $stream.Write($raw,0,$raw.Length);$stream.Flush($true) } finally { $stream.Dispose() }
    Move-Item -LiteralPath $temp -Destination $path -Force
}
function Start-RecoveryChild([string]$File) {
    if ([IO.Path]::GetFileName($File) -ceq 'start_oanda_research_collection.ps1' -and (Get-RecoveryEpoch) -ge $RecoveryStop) { Stop-RecoveryCheck 'research_deadline_elapsed' }
    Start-Process -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -ArgumentList @(
        '-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-WindowStyle','Hidden','-File',('"'+$File+'"')) -WorkingDirectory $RecoveryRoot -WindowStyle Hidden | Out-Null
}

$RecoveryLoadedSourceSha = Get-RecoveryHash (Read-RecoveryBytes $PSCommandPath)
if ($CheckOnly -or $Once) {
    try { Get-RecoveryInspection | ConvertTo-Json -Depth 10; exit 0 }
    catch { @{schema_version=$RecoverySchema;phase='inspection_refused';reason_code='binding_scope_or_inspection_failed';check_only=$true;observed_epoch=(Get-RecoveryEpoch);broker_action_performed=$false} | ConvertTo-Json; exit 2 }
}
$RecoveryMutex = [Threading.Mutex]::new($false,'Local\ForexPracticeRecovery20260910V1')
$RecoveryOwned = $false
try {
    try { $RecoveryOwned=$RecoveryMutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $RecoveryOwned=$true }
    if (-not $RecoveryOwned) { exit 0 }
    $previous = 0.0
    while ($true) {
        try {
            $inspection = Get-RecoveryInspection
            if ($inspection.observed_epoch -lt $previous) { Stop-RecoveryCheck 'observation_clock_rollback' }
            $previous = $inspection.observed_epoch
            Write-RecoveryStatus $inspection
            if ($inspection.phase -eq 'completed_flat') { break }
            if ($inspection.phase -in @('start_research','start_trial','start_trial_close_only')) {
                # Re-read identities/config/sources immediately before spawning.
                $fresh = Get-RecoveryInspection
                if ($fresh.phase -eq $inspection.phase) {
                    if ($fresh.phase -eq 'start_research') { Start-RecoveryChild (Join-Path $RecoveryRoot 'start_oanda_research_collection.ps1') }
                    else { Start-RecoveryChild (Join-Path $RecoveryRoot 'start_oanda_practice_trial_v1.ps1') }
                }
            }
        } catch {
            $reason='binding_scope_or_inspection_failed'
            if ($_.Exception.Message -eq 'observation_clock_rollback') { $reason='observation_clock_rollback' }
            Write-RecoveryStatus @{schema_version=$RecoverySchema;phase='recovery_refused';reason_code=$reason;observed_epoch=(Get-RecoveryEpoch);pid=$PID;practice_only=$true;broker_action_performed=$false}
        }
        Start-Sleep -Seconds 15
    }
} finally { if ($RecoveryOwned) { $RecoveryMutex.ReleaseMutex() };$RecoveryMutex.Dispose() }
