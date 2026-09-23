param(
    [string]$Root = "",
    [int]$IntervalSec = 30,
    [int]$SupervisorHeartbeatMaxAgeSec = 150,
    [int]$AccountStateMaxAgeSec = 600,
    [int]$StartupGraceSec = 180,
    [int]$RestartWindowMinutes = 30,
    [int]$MaximumRestartsPerWindow = 3,
    [string]$OperationalProfilePath = "",
    [string]$RecoveryUntilUtc = "2026-09-20T23:59:00Z",
    [switch]$Once,
    [switch]$NoRecovery
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot 'oanda_operational_recovery_contract_v2.ps1')

if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}
$Root = [System.IO.Path]::GetFullPath($Root)
$Trad = [System.IO.Path]::GetFullPath((Join-Path $Root "trad"))
$ExpectedTrad = [System.IO.Path]::GetFullPath($PSScriptRoot)
if ($Trad -ne $ExpectedTrad) {
    throw "Watchdog root does not resolve to the canonical project: $Trad"
}
if ($Trad -match '^[dD]:\\') {
    throw "The D: legacy workspace is never a valid watchdog target."
}

$State = Join-Path $Trad "data\oanda_training_manager\state"
$Logs = Join-Path $Trad "data\oanda_training_manager\logs"
$Supervisor = Join-Path $Trad "oanda_operational_supervisor_v2.ps1"
$Launcher = Join-Path $Trad "start_oanda_safe_core.ps1"
if (-not $OperationalProfilePath) { throw 'V2 watchdog requires explicit operational profile.' }
$OperationalBinding = $null
if ($OperationalProfilePath) {
    $OperationalBinding = Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc
    $OperationalProfilePath = $OperationalBinding.path
    $Launcher = Join-Path $Trad "start_oanda_operational_research_v2.ps1"
}
$Heartbeat = Join-Path $State "oanda_supervisor_watchdog_v2.json"
$Lease = Join-Path $State "oanda_supervisor_watchdog_lease_v2.json"
$IncidentLog = Join-Path $Logs "oanda_supervisor_watchdog_incidents_v2.jsonl"
$AccountState = Join-Path $State "account_007_dashboard_v1.json"
$OperationalHeartbeat=Join-Path $State 'operational_supervisor_v2.json'
$RestartLedger=Join-Path $State 'operational_watchdog_restarts_v2.json'
$BoundedIncidentDirectory=Join-Path $Logs 'operational_watchdog_v2'

$sha256 = [Security.Cryptography.SHA256]::Create()
try {
    $rootDigest = [BitConverter]::ToString(
        $sha256.ComputeHash(
            [Text.Encoding]::UTF8.GetBytes($Trad.ToLowerInvariant())
        )
    ).Replace("-", "")
} finally {
    $sha256.Dispose()
}
$MutexName = "Local\ForexSupervisorWatchdogV1_" + $rootDigest.Substring(0, 16)

New-Item -ItemType Directory -Force -Path $State,$Logs | Out-Null
$mutex = [Threading.Mutex]::new($false, $MutexName)
$ownsMutex = $false

function Write-AtomicJson {
    param(
        [Parameter(Mandatory=$true)][string]$LiteralPath,
        [Parameter(Mandatory=$true)][object]$Value
    )
    $directory = Split-Path -Parent $LiteralPath
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $temporary = Join-Path $directory (
        "." + [IO.Path]::GetFileName($LiteralPath) + "." +
        [Guid]::NewGuid().ToString("N") + ".tmp"
    )
    try {
        $Value | ConvertTo-Json -Depth 12 -Compress |
            Set-Content -LiteralPath $temporary -Encoding UTF8
        Move-Item -LiteralPath $temporary -Destination $LiteralPath -Force
    } finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        }
    }
}

function Get-FileAgeSec {
    param([string]$LiteralPath)
    if (-not (Test-Path -LiteralPath $LiteralPath)) {
        return [double]::PositiveInfinity
    }
    return [Math]::Max(
        0.0,
        ((Get-Date).ToUniversalTime() -
            (Get-Item -LiteralPath $LiteralPath).LastWriteTimeUtc).TotalSeconds
    )
}

function Get-Supervisors {
    $escaped = [regex]::Escape($Supervisor)
    return @(
        Get-CimInstance Win32_Process -ErrorAction Stop |
            Where-Object {
                $_.Name -match '^(?:powershell|pwsh)\.exe$' -and
                (Test-OperationalOwnerCommand -CommandLine ([string]$_.CommandLine) -Trad $Trad -Role supervisor)
            } |
            Sort-Object CreationDate,ProcessId
    )
}

function Get-LatestSupervisorLog {
    return Get-Item -LiteralPath $OperationalHeartbeat -ErrorAction SilentlyContinue
}

function Get-ProjectPythonInventory {
    $escaped = [regex]::Escape($Trad)
    $rows = @(
        Get-CimInstance Win32_Process -ErrorAction Stop |
            Where-Object {
                $_.Name -match '^python(?:w)?\.exe$' -and
                [string]$_.CommandLine -match $escaped
            }
    )
    $scripts = @(
        foreach ($row in $rows) {
            $match = [regex]::Match(
                [string]$row.CommandLine,
                '(?i)([A-Za-z0-9_\-]+\.py)(?:\"|\s|$)'
            )
            if ($match.Success) { $match.Groups[1].Value }
        }
    )
    return @{
        process_count = $rows.Count
        script_count = @($scripts | Sort-Object -Unique).Count
        scripts = @($scripts | Sort-Object -Unique)
    }
}

function Get-RecentRestartCount {
    $attempts=@()
    if (Test-Path -LiteralPath $RestartLedger) { $attempts=@(Get-Content -LiteralPath $RestartLedger -Raw|ConvertFrom-Json) }
    $decision=Get-OperationalRestartDecision -Attempts $attempts -NowEpoch ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0) -WindowSeconds ($RestartWindowMinutes*60) -Limit $MaximumRestartsPerWindow
    if (-not $decision.allowed) { return $MaximumRestartsPerWindow }
    return $decision.attempts.Count
}

function Write-Incident {
    param([string]$Reason,[string]$Action,[object]$Details)
    $row = [ordered]@{
        schema_version = "oanda_supervisor_watchdog_incident_v1"
        incident_id = [Guid]::NewGuid().ToString("N")
        observed_utc = (Get-Date).ToUniversalTime().ToString("o")
        reason = $Reason
        action = $Action
        details = $Details
        watchdog_pid = $PID
        real_money_enabled = $false
    }
    $null=Write-OperationalBoundedEvent -Directory $BoundedIncidentDirectory -Prefix 'incidents' -Value $row -SegmentBytes 1048576 -TotalBytes 16777216
    return $row
}

function Start-SafeCoreSupervisor {
    $attempts=@()
    if(Test-Path -LiteralPath $RestartLedger) { $attempts=@(Get-Content -LiteralPath $RestartLedger -Raw|ConvertFrom-Json) }
    $nowEpoch=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0
    $budget=Get-OperationalRestartDecision -Attempts $attempts -NowEpoch $nowEpoch -WindowSeconds ($RestartWindowMinutes*60) -Limit $MaximumRestartsPerWindow
    if(-not $budget.allowed) { throw $budget.reason }
    Write-OperationalAtomicJson -LiteralPath $RestartLedger -Value (@($budget.attempts)+@($nowEpoch))
    $arguments = @(
        "-NoLogo", "-NoProfile", "-NonInteractive",
        "-ExecutionPolicy", "Bypass",
        "-File", $Launcher,
        "-Root", $Root
    )
    if ($OperationalProfilePath) {
        # Validation is repeated immediately before the child launcher. The
        # launcher and supervisor also validate, and never select a broader lane.
        $current = Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc
        if ($current.sha256 -cne $OperationalBinding.sha256) { throw 'Operational recovery profile changed.' }
        $arguments += @('-OperationalProfilePath',$OperationalProfilePath,'-RecoveryUntilUtc',$RecoveryUntilUtc)
        $arguments = @($arguments | ForEach-Object { ConvertTo-OperationalProcessArgument $_ })
    }
    Start-Process `
        -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" `
        -ArgumentList $arguments `
        -WorkingDirectory $Trad `
        -WindowStyle Hidden | Out-Null
}

try {
    $ownsMutex = $mutex.WaitOne(0)
    if (-not $ownsMutex) {
        throw "Another canonical supervisor watchdog already owns the lease."
    }
    $firstObservedByPid = @{}
    while ($true) {
        $now = (Get-Date).ToUniversalTime()
        $profileError = ""
        if ($OperationalProfilePath) {
            try {
                $currentBinding = Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc
                if ($currentBinding.sha256 -cne $OperationalBinding.sha256) { throw 'Operational recovery profile changed.' }
            } catch { $profileError = $_.Exception.Message }
        }
        try {
            $supervisors = @(Get-Supervisors)
            $inventory = Get-ProjectPythonInventory
        } catch {
            Write-AtomicJson -LiteralPath $Heartbeat -Value @{
                schema_version='oanda_supervisor_watchdog_v2';generated_utc=$now.ToString('o');watchdog_pid=$PID;
                health_reason='process_inventory_unavailable';action='no_recovery_without_process_inventory';
                operational_profile_path=$OperationalProfilePath;operational_profile_sha256=$OperationalBinding.sha256;
                error=$_.Exception.Message;can_place_orders=$false;existing_processes_preserved=$true
            }
            if ($Once -or [DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse($OperationalBinding.expires_utc)) { break }
            Start-Sleep -Seconds ([Math]::Max(5,$IntervalSec))
            continue
        }
        $profileConflicts = @()
        if ($OperationalProfilePath) {
            $profileConflicts = @($supervisors | Where-Object {
                -not (Test-OperationalSupervisorCommand -CommandLine ([string]$_.CommandLine) -ProfilePath $OperationalProfilePath)
            })
        }
        foreach ($row in $supervisors) {
            if (-not $firstObservedByPid.ContainsKey([int]$row.ProcessId)) {
                $firstObservedByPid[[int]$row.ProcessId] = $now
            }
        }
        foreach ($key in @($firstObservedByPid.Keys)) {
            if ($key -notin @($supervisors | ForEach-Object { [int]$_.ProcessId })) {
                $firstObservedByPid.Remove($key)
            }
        }

        $latestLog = Get-LatestSupervisorLog
        $supervisorLogAgeSec = if ($null -ne $latestLog) {
            Get-FileAgeSec $latestLog.FullName
        } else { [double]::PositiveInfinity }
        $accountAgeSec = Get-FileAgeSec $AccountState
        $reason = ""
        if ($profileError) {
            $reason = "operational_profile_validation_failed"
        } elseif ($profileConflicts.Count -gt 0) {
            $reason = "conflicting_supervisor_profile"
        } elseif ($supervisors.Count -eq 0) {
            $reason = "supervisor_process_missing"
        } elseif ($supervisors.Count -gt 1) {
            $reason = "duplicate_supervisor_processes"
        } else {
            $pidValue = [int]$supervisors[0].ProcessId
            $observedAge = ($now - $firstObservedByPid[$pidValue]).TotalSeconds
            if (
                $observedAge -ge $StartupGraceSec -and
                $supervisorLogAgeSec -gt $SupervisorHeartbeatMaxAgeSec
            ) {
                $reason = "supervisor_heartbeat_stale"
            } elseif ($observedAge -ge $StartupGraceSec) {
                $identity=$null
                try { $identity=Get-Content -LiteralPath $OperationalHeartbeat -Raw|ConvertFrom-Json } catch {}
                if (-not (Test-OperationalSupervisorIdentity -Heartbeat $identity -ProcessId $pidValue -ProfileHash $OperationalBinding.sha256 -NowEpoch ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0) -MaxAgeSec $SupervisorHeartbeatMaxAgeSec)) {
                    $reason='supervisor_heartbeat_identity_invalid'
                }
            }
        }

        $restartCount = Get-RecentRestartCount
        $action = "none"
        $circuitOpen = $restartCount -ge $MaximumRestartsPerWindow
        if ($reason -and -not $NoRecovery) {
          try {
            if ($profileError -or $profileConflicts.Count -gt 0) {
                $action = "operational_recovery_refused"
                Write-Incident $reason $action @{
                    error = $profileError
                    profile_path = $OperationalProfilePath
                    conflicting_pids = @($profileConflicts | ForEach-Object { [int]$_.ProcessId })
                    existing_processes_preserved = $true
                } | Out-Null
            } elseif ($circuitOpen) {
                $action = "restart_circuit_open"
                Write-Incident $reason $action @{
                    restart_count = $restartCount
                    restart_window_minutes = $RestartWindowMinutes
                } | Out-Null
            } else {
                if ($reason -eq "duplicate_supervisor_processes") {
                    foreach ($extra in @($supervisors | Select-Object -Skip 1)) {
                        if (-not (Stop-OperationalVerifiedSupervisor -Process $extra)) { throw 'Supervisor identity changed before duplicate cleanup.' }
                    }
                    $action = "duplicate_supervisors_stopped"
                    Write-Incident $reason $action @{
                        retained_pid = [int]$supervisors[0].ProcessId
                        stopped_pids = @(
                            $supervisors | Select-Object -Skip 1 |
                                ForEach-Object { [int]$_.ProcessId }
                        )
                    } | Out-Null
                } else {
                    if ($supervisors.Count -eq 1) {
                        if (-not (Stop-OperationalVerifiedSupervisor -Process $supervisors[0])) { throw 'Supervisor identity changed before recovery.' }
                    }
                    Start-SafeCoreSupervisor
                    $action = "supervisor_restarted"
                    Write-Incident $reason $action @{
                        prior_pids = @(
                            $supervisors | ForEach-Object { [int]$_.ProcessId }
                        )
                        python_children_preserved = $true
                    } | Out-Null
                }
            }
          } catch {
            $action='recovery_action_failed'
            Write-Incident $reason $action @{error=$_.Exception.Message;automatic_broadening_permitted=$false} | Out-Null
          }
        } elseif ($reason) {
            $action = "diagnostic_only_no_recovery"
        }

        $ownerHealth=$null
        try { $ownerHealth=Get-Content -LiteralPath $OperationalHeartbeat -Raw|ConvertFrom-Json } catch {}
        $ownerHealthBound=Test-OperationalSupervisorIdentity -Heartbeat $ownerHealth -ProcessId $(if($supervisors.Count -eq 1){[int]$supervisors[0].ProcessId}else{-1}) -ProfileHash $OperationalBinding.sha256 -NowEpoch ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0) -MaxAgeSec $SupervisorHeartbeatMaxAgeSec
        $roleFailures=@()
        $roleMissing=@()
        $ownerError=''
        if ($ownerHealthBound) {
            $ownerError=[string]$ownerHealth.error
            $roleFailures=@($ownerHealth.managed | Where-Object { -not $_.freshness.fresh -or $_.freshness.reported_failure } | ForEach-Object {
                @{name=$_.name;running=$_.running;fresh=$_.freshness.fresh;reason=$_.freshness.reason;
                    reported_failure=$_.freshness.reported_failure;reported_error=$_.freshness.reported_error;
                    recovery_reason=$_.freshness.recovery_reason}
            })
            $presentNames=@($ownerHealth.managed | ForEach-Object {$_.name})
            $roleMissing=@($OperationalBinding.services | Where-Object {$_ -notin $presentNames})
        }
        $payload = [ordered]@{
            schema_version = "oanda_supervisor_watchdog_v2"
            contract_id = "external_supervisor_lease_recovery_v1_20260901"
            generated_utc = $now.ToString("o")
            watchdog_pid = $PID
            canonical_root = $Trad
            mutex_name = $MutexName
            lease_owned = $true
            supervisor_process_count = $supervisors.Count
            supervisor_pids = @(
                $supervisors | ForEach-Object { [int]$_.ProcessId }
            )
            supervisor_log = if ($null -ne $latestLog) {
                $latestLog.FullName
            } else { "" }
            supervisor_heartbeat_age_sec = if ([double]::IsInfinity($supervisorLogAgeSec)) {
                $null
            } else { [Math]::Round($supervisorLogAgeSec, 3) }
            account_freshness_is_recovery_criterion = $false
            owned_role_health_scope = 'Process/heartbeat liveness and last successful work are separate. Reported role failures do not restart a healthy supervisor.'
            owned_role_health_bound = $ownerHealthBound
            owned_role_failures = $roleFailures
            owned_roles_missing_from_heartbeat = $roleMissing
            supervisor_reported_error = $ownerError
            operational_health = $(if(-not $ownerHealthBound){'owner_unverified'}elseif($ownerError -or $roleFailures.Count -or $roleMissing.Count){'degraded'}else{'role_liveness_current'})
            account_state_age_sec = if ([double]::IsInfinity($accountAgeSec)) {
                $null
            } else { [Math]::Round($accountAgeSec, 3) }
            health_reason = $reason
            action = $action
            restart_count_in_window = $restartCount
            restart_window_minutes = $RestartWindowMinutes
            restart_circuit_open = $circuitOpen
            orphan_inventory = $inventory
            safe_core_only = $true
            preserves_python_children = $true
            can_place_orders = $false
            can_change_authorization = $false
            real_money_enabled = $false
            research_collection_only = [bool]$OperationalProfilePath
            operational_profile_path = $OperationalProfilePath
            operational_profile_sha256 = $(if ($OperationalBinding) { $OperationalBinding.sha256 } else { $null })
            operational_profile_validation_error = $profileError
            recovery_until_utc = $(if ($OperationalBinding) { $OperationalBinding.expires_utc } else { $null })
            recovery_expiry_scope = $(if ($OperationalBinding) { 'Future recovery attempts only; healthy supervisor and children are not stopped at expiry.' } else { 'Legacy no-profile recovery behavior.' })
        }
        Write-AtomicJson -LiteralPath $Heartbeat -Value $payload
        Write-AtomicJson -LiteralPath $Lease -Value @{
            schema_version = "oanda_supervisor_watchdog_lease_v2"
            lease_owner_pid = $PID
            lease_renewed_utc = $now.ToString("o")
            mutex_name = $MutexName
            canonical_root = $Trad
        }
        if ($Once -or ($OperationalBinding -and [DateTimeOffset]::UtcNow -ge [DateTimeOffset]::Parse($OperationalBinding.expires_utc))) { break }
        Start-Sleep -Seconds ([Math]::Max(5, $IntervalSec))
    }
} finally {
    if ($ownsMutex) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
