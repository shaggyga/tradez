param(
    [string]$Root = "",
    [int]$IntervalSec = 30,
    [int]$SupervisorHeartbeatMaxAgeSec = 150,
    [int]$AccountStateMaxAgeSec = 600,
    [int]$StartupGraceSec = 180,
    [int]$RestartWindowMinutes = 30,
    [int]$MaximumRestartsPerWindow = 3,
    [switch]$Once,
    [switch]$NoRecovery
)

$ErrorActionPreference = "Stop"

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
$Supervisor = Join-Path $Trad "oanda_always_on_supervisor.ps1"
$Launcher = Join-Path $Trad "start_oanda_safe_core.ps1"
$Heartbeat = Join-Path $State "oanda_supervisor_watchdog_v1.json"
$Lease = Join-Path $State "oanda_supervisor_watchdog_lease_v1.json"
$IncidentLog = Join-Path $Logs "oanda_supervisor_watchdog_incidents_v1.jsonl"
$AccountState = Join-Path $State "account_007_dashboard_v1.json"
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
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Name -match '^(?:powershell|pwsh)\.exe$' -and
                [string]$_.CommandLine -match "\s-File\s+`"?$escaped`"?(?:\s|$)"
            } |
            Sort-Object CreationDate,ProcessId
    )
}

function Get-LatestSupervisorLog {
    return Get-ChildItem -LiteralPath $Logs -Filter "always_on_supervisor_*.jsonl" `
        -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Get-ProjectPythonInventory {
    $escaped = [regex]::Escape($Trad)
    $rows = @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
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
    $cutoff = (Get-Date).ToUniversalTime().AddMinutes(-$RestartWindowMinutes)
    if (-not (Test-Path -LiteralPath $IncidentLog)) { return 0 }
    $count = 0
    foreach ($line in Get-Content -LiteralPath $IncidentLog -ErrorAction SilentlyContinue) {
        try {
            $row = $line | ConvertFrom-Json
            $at = [DateTimeOffset]::Parse([string]$row.observed_utc).UtcDateTime
            if ($row.action -eq "supervisor_restarted" -and $at -ge $cutoff) {
                $count += 1
            }
        } catch {
            # A malformed historical incident is retained but never counted as
            # restart authority.
        }
    }
    return $count
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
    Add-Content -LiteralPath $IncidentLog `
        -Value ($row | ConvertTo-Json -Depth 10 -Compress) `
        -Encoding UTF8
    return $row
}

function Start-SafeCoreSupervisor {
    $arguments = @(
        "-NoLogo", "-NoProfile", "-NonInteractive",
        "-ExecutionPolicy", "Bypass",
        "-File", $Launcher,
        "-Root", $Root
    )
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
        $supervisors = @(Get-Supervisors)
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
        if ($supervisors.Count -eq 0) {
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
            } elseif (
                $observedAge -ge $StartupGraceSec -and
                $accountAgeSec -gt $AccountStateMaxAgeSec
            ) {
                $reason = "account_aggregate_stale"
            }
        }

        $restartCount = Get-RecentRestartCount
        $action = "none"
        $circuitOpen = $restartCount -ge $MaximumRestartsPerWindow
        if ($reason -and -not $NoRecovery) {
            if ($circuitOpen) {
                $action = "restart_circuit_open"
                Write-Incident $reason $action @{
                    restart_count = $restartCount
                    restart_window_minutes = $RestartWindowMinutes
                } | Out-Null
            } else {
                if ($reason -eq "duplicate_supervisor_processes") {
                    foreach ($extra in @($supervisors | Select-Object -Skip 1)) {
                        Stop-Process -Id ([int]$extra.ProcessId) -Force -ErrorAction Stop
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
                        Stop-Process -Id ([int]$supervisors[0].ProcessId) `
                            -Force -ErrorAction Stop
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
        } elseif ($reason) {
            $action = "diagnostic_only_no_recovery"
        }

        $inventory = Get-ProjectPythonInventory
        $payload = [ordered]@{
            schema_version = "oanda_supervisor_watchdog_v1"
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
        }
        Write-AtomicJson -LiteralPath $Heartbeat -Value $payload
        Write-AtomicJson -LiteralPath $Lease -Value @{
            schema_version = "oanda_supervisor_watchdog_lease_v1"
            lease_owner_pid = $PID
            lease_renewed_utc = $now.ToString("o")
            mutex_name = $MutexName
            canonical_root = $Trad
        }
        if ($Once) { break }
        Start-Sleep -Seconds ([Math]::Max(5, $IntervalSec))
    }
} finally {
    if ($ownsMutex) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
