param(
    [string]$Root = '',
    [Parameter(Mandatory=$true)][string]$OperationalProfilePath,
    [Parameter(Mandatory=$true)][string]$RecoveryUntilUtc,
    [int]$IntervalSec = 30,
    [int]$ChildDurationSec = 604800,
    [switch]$SafeCoreOnly,
    [switch]$ResearchCollectionOnly,
    [switch]$ValidateOnly
)
# V6: adds the M1 collector successor and explicit all-pair availability reader.
# Frozen V1 sources and every existing data generation remain unchanged.
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'oanda_operational_recovery_contract_v6.ps1')
if (-not $Root) { $Root = Split-Path -Parent $PSScriptRoot }
$Root = Resolve-OperationalRecoveryPath $Root
$Trad = Resolve-OperationalRecoveryPath (Join-Path $Root 'trad')
if ($Trad -ine $PSScriptRoot -or -not $SafeCoreOnly -or -not $ResearchCollectionOnly) { throw 'Explicit canonical safe research profile required.' }
if ($IntervalSec -lt 15 -or $IntervalSec -gt 120 -or $ChildDurationSec -ne 604800) { throw 'Bounded operational scheduling required.' }
$binding = Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc
$OperationalProfilePath=$binding.path
$OperationalProfile=$binding.profile
$OperationalProfileSha256=$binding.sha256
$opNames=@($binding.services)
if ($ValidateOnly) { $binding | ConvertTo-Json -Depth 8; exit 0 }
$DataRoot=Join-Path $Trad 'data\oanda_training_manager'
$State=Join-Path $DataRoot 'state'
$Logs=Join-Path $DataRoot 'logs'
$BoundedChildLogs=Join-Path $Logs 'operational_children_v2'
$BoundedEvents=Join-Path $Logs 'operational_supervisor_v2'
$SupervisorHeartbeat=Join-Path $State 'operational_supervisor_v2.json'
$RestartLedgerPath=Join-Path $State 'operational_supervisor_restart_v2.json'
$CoreTimeseriesPython=Join-Path $env:LOCALAPPDATA 'CodexRuntimes\timeseries312\Scripts\python.exe'
$Python=$CoreTimeseriesPython
$env:OANDA_CREDS_PATH=Join-Path $Trad 'creds'
$env:TRAD_CREDS_PATH=$env:OANDA_CREDS_PATH
$env:TRAD_PROJECT_ROOT=$Trad
$env:FOREX_ALLOW_LIVE='0'
$env:FOREX_LIVE_EXECUTE='0'
# This Forex-only run uses public news; optional keyed providers stay disabled.
Clear-OperationalOptionalNewsCredentials
$SupervisorMutex=[Threading.Mutex]::new($false,'Global\ForexOandaAlwaysOnSupervisorV1')
$ownsMutex=$false
try { $ownsMutex=$SupervisorMutex.WaitOne(0) } catch [Threading.AbandonedMutexException] { $ownsMutex=$true }
if (-not $ownsMutex) { throw 'Another Forex supervisor owns the existing global mutex; no cutover performed.' }
$SupervisorStartedUtc=[DateTime]::UtcNow
$script:MatchingPythonProcessSnapshot=@()
$script:ArtifactSizeHistory=@{}
$script:RestartLedger=@{}
$script:RecoveryAllowed=$true
$script:EventDrops=0
if (Test-Path -LiteralPath $RestartLedgerPath) {
    $saved=Get-Content -LiteralPath $RestartLedgerPath -Raw|ConvertFrom-Json
    $budgetNames=@($opNames | ForEach-Object { Get-OperationalRestartBudgetKey -Name $_ })
    # Preserve retired transport, paused research and mutually exclusive joint-role histories.
    # Selection changes neither erase history nor grant fresh retry allowances.
    $budgetNames+=@('joint_price_news_study_v8','joint_price_news_isolation_status_v1','revision_news_transport_v5','research_feature_forward_v2','research_feature_forward_cached_v2')
    foreach($property in $saved.PSObject.Properties) {
        if ($property.Name -notin $budgetNames -or @($property.Value).Count -gt 3) { throw 'Invalid bounded restart ledger.' }
        $script:RestartLedger[$property.Name]=@($property.Value)
    }
}
function Write-SupervisorEvent {
    param([string]$Event,[hashtable]$Fields=@{})
    $payload=@{time=[DateTime]::UtcNow.ToString('o');event=$Event;supervisor_pid=$PID}
    foreach($key in $Fields.Keys) { $payload[$key]=$Fields[$key] }
    try { if (-not (Write-OperationalBoundedEvent -Directory $BoundedEvents -Prefix 'events' -Value $payload)) { $script:EventDrops++ } }
    catch { $script:EventDrops++ }
}

function Refresh-MatchingPythonProcessSnapshot {
    # Win32_Process enumeration is materially more expensive than exact
    # in-memory matching. Capture one root-scoped snapshot per supervision
    # cycle instead of repeating the same CIM query for every managed worker.
    # Keep relative Python commands in the snapshot too: their working directory
    # cannot be proven through CIM, so they block a duplicate start rather than
    # being silently invisible. Exact canonical workers alone can be adopted.
    # A failed refresh aborts that cycle rather than treating an empty or stale
    # view as authority to launch duplicates.
    $script:MatchingPythonProcessSnapshot = @(
        Get-CimInstance Win32_Process -ErrorAction Stop |
            Where-Object {
                $commandLine = [string]$_.CommandLine
                $_.Name -eq "python.exe" -and
                $commandLine -notmatch '(?i)\s-m\s+(pytest|py_compile)(?:\s|$)'
            }
    )
    return $script:MatchingPythonProcessSnapshot.Count
}

function Add-StartedProcessToMatchingSnapshot {
    param([int]$ProcessId)
    try {
        $startedProcess = Get-CimInstance Win32_Process `
            -Filter ("ProcessId = " + $ProcessId) `
            -ErrorAction Stop
        if ($null -ne $startedProcess) {
            $script:MatchingPythonProcessSnapshot = @(
                $script:MatchingPythonProcessSnapshot
            ) + @($startedProcess)
            return
        }
    } catch {
        # A later match in this cycle must not rely on a snapshot that is known
        # to omit a just-started process. Fail the cycle instead of duplicating.
    }
    throw "Unable to add started process $ProcessId to supervision snapshot"
}

function Get-MatchingPython {
    param([string]$Needle)
    $simpleScriptNeedle = $Needle -match '^[^*?]+\.py$'
    $scriptPattern = if ($simpleScriptNeedle) {
        [regex]::Escape((Join-Path $Trad $Needle))
    } else {
        ""
    }
    $matches = @($script:MatchingPythonProcessSnapshot |
        Where-Object {
            $commandLine = [string]$_.CommandLine
            if (Test-OperationalRelayCommand -CommandLine $commandLine -Trad $Trad) { return $false }
            $needleMatched = if ($simpleScriptNeedle) {
                $commandLine -match (
                    '(?i)(?:^|\s)"?' +
                    $scriptPattern +
                    '"?(?:\s|$)'
                )
            } else {
                $commandLine -like ("*" + $Needle + "*")
            }
            if (
                $_.Name -ne "python.exe" -or
                $commandLine -notlike ("*" + $Root + "*") -or
                -not $needleMatched -or
                $commandLine -match '(?i)\s-m\s+(pytest|py_compile)(?:\s|$)'
            ) {
                return $false
            }
            $runtimeProcess = Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue
            return $null -ne $runtimeProcess -and -not $runtimeProcess.HasExited
        })
    # The bundled virtual environment's python.exe is a launcher which stays
    # alive while its child interpreter executes the same command line.  They
    # are one worker, not two competing workers.  Supervise the leaf process;
    # separate launches remain separate leaves and are still deduplicated.
    $matchingIds = [Collections.Generic.HashSet[int]]::new()
    foreach ($proc in $matches) { [void]$matchingIds.Add([int]$proc.ProcessId) }
    $wrapperIds = [Collections.Generic.HashSet[int]]::new()
    foreach ($proc in $matches) {
        if ($matchingIds.Contains([int]$proc.ParentProcessId)) {
            [void]$wrapperIds.Add([int]$proc.ParentProcessId)
        }
    }
    return @($matches | Where-Object { -not $wrapperIds.Contains([int]$_.ProcessId) })
}

function Stop-MatchingPython {
    param(
        [string]$Name,
        [string]$Needle,
        [object[]]$Processes,
        [string]$Reason
    )
    $targets=@{}
    foreach($proc in $Processes) { $targets[[int]$proc.ProcessId]=$proc }
    $remaining = @($Processes)
    $identityChanged=$false
    for ($pass = 0; $pass -lt 3 -and $remaining.Count -gt 0; $pass++) {
        foreach ($proc in @($remaining | Sort-Object CreationDate -Descending)) {
            try {
                # Never expand a duplicate-cleanup request to all matching
                # workers. Revalidate process identity immediately before stop;
                # PID reuse must not confer authority over a replacement.
                $current=Get-CimInstance Win32_Process -Filter ('ProcessId = '+[int]$proc.ProcessId) -ErrorAction Stop
                if ($null -eq $current) { continue }
                if ($current.Name -ine 'python.exe' -or
                    [datetime]$current.CreationDate -ne [datetime]$proc.CreationDate -or
                    [string]$current.CommandLine -cne [string]$proc.CommandLine) {
                    Write-SupervisorEvent 'process_stop_identity_changed' @{name=$Name;pid=$proc.ProcessId;reason=$Reason}
                    $identityChanged=$true
                    continue
                }
                Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
                Write-SupervisorEvent "process_stopped" @{
                    name = $Name
                    pid = $proc.ProcessId
                    reason = $Reason
                    cleanup_pass = $pass
                }
            } catch {
                Write-SupervisorEvent "process_stop_error" @{
                    name = $Name
                    pid = $proc.ProcessId
                    reason = $Reason
                    cleanup_pass = $pass
                    error = $_.Exception.Message
                }
            }
        }
        Start-Sleep -Milliseconds 200
        $remaining = @(Get-MatchingPython -Needle $Needle | Where-Object {
            $original=$targets[[int]$_.ProcessId]
            $null -ne $original -and [datetime]$_.CreationDate -eq [datetime]$original.CreationDate -and
            [string]$_.CommandLine -ceq [string]$original.CommandLine
        })
    }
    if ($identityChanged -or $remaining.Count) { throw 'Exact worker stop was not confirmed; no replacement may be started this cycle.' }
}

function Get-LatestMatchingFile {
    param(
        [string]$Directory,
        [string]$Filter
    )
    if (-not (Test-Path -LiteralPath $Directory)) {
        return $null
    }
    return Get-ChildItem -LiteralPath $Directory -Filter $Filter -File |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Measure-ManagedArtifact {
    param(
        [string]$Name,
        [string]$LiteralPath,
        [long]$WarningBytes,
        [string]$AbsentStatus = "missing"
    )
    $now = (Get-Date).ToUniversalTime()
    $item = Get-Item -LiteralPath $LiteralPath -ErrorAction SilentlyContinue
    if ($null -eq $item) {
        return @{
            name = $Name
            path = $LiteralPath
            status = $AbsentStatus
            bytes = 0
            delta_bytes = $null
            growth_bytes_per_sec = $null
        }
    }
    $key = $item.FullName.ToLowerInvariant()
    $previous = $script:ArtifactSizeHistory[$key]
    $deltaBytes = $null
    $growth = $null
    if ($null -ne $previous) {
        $elapsed = [math]::Max(
            0.001,
            ($now - $previous.time).TotalSeconds
        )
        $deltaBytes = [long]$item.Length - [long]$previous.bytes
        $growth = [math]::Round($deltaBytes / $elapsed, 3)
    }
    $script:ArtifactSizeHistory[$key] = @{
        time = $now
        bytes = [long]$item.Length
    }
    return @{
        name = $Name
        path = $item.FullName
        status = if (
            $WarningBytes -gt 0 -and
            [long]$item.Length -ge $WarningBytes
        ) { "warning" } else { "normal" }
        bytes = [long]$item.Length
        delta_bytes = $deltaBytes
        growth_bytes_per_sec = $growth
        last_write_utc = $item.LastWriteTimeUtc.ToString("o")
    }
}

function Read-JsonFileWithRetry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath,
        [int]$MaximumAttempts = 4,
        [int]$InitialDelayMs = 20
    )
    $lastError = $null
    for ($attempt = 1; $attempt -le $MaximumAttempts; $attempt++) {
        try {
            $content = [System.IO.File]::ReadAllText(
                $LiteralPath,
                [System.Text.Encoding]::UTF8
            )
            return $content | ConvertFrom-Json -ErrorAction Stop
        } catch {
            $lastError = $_
            if ($attempt -lt $MaximumAttempts) {
                $delayMs = [math]::Min(
                    250,
                    $InitialDelayMs * [math]::Pow(2, $attempt - 1)
                )
                Start-Sleep -Milliseconds ([int]$delayMs)
            }
        }
    }
    throw $lastError.Exception
}

function Test-FreshOutput {
    param(
        [string]$LiteralPath = "",
        [string]$Directory = "",
        [string]$Filter = "",
        [int]$MaxAgeSec = 180,
        [int]$StartupGraceSec = 0,
        [int]$MaxPhaseAgeSec = 0,
        [string[]]$WatchedPhases = @(),
        [int]$MaxProgressAgeSec = 0,
        [string[]]$ProgressPhases = @(),
        [string]$ExpectedJsonField = "",
        [string]$ExpectedJsonValue = "",
        [switch]$InspectOperationalStatus
    )
    $item = $null
    if ($LiteralPath -and (Test-Path -LiteralPath $LiteralPath)) {
        $item = Get-Item -LiteralPath $LiteralPath
    } elseif ($Directory -and $Filter) {
        $item = Get-LatestMatchingFile -Directory $Directory -Filter $Filter
    }
    if ($null -eq $item) {
        return @{ fresh = $false; age_sec = $null; path = ""; reason = "missing_output" }
    }
    $age = ((Get-Date).ToUniversalTime() - $item.LastWriteTimeUtc).TotalSeconds
    $result = @{
        fresh = ($age -ge -1 -and $age -le $MaxAgeSec)
        age_sec = [math]::Round($age, 1)
        path = $item.FullName
        reason = if ($age -ge -1 -and $age -le $MaxAgeSec) { "fresh" } else { "stale_output" }
    }
    if ($result.fresh -and $LiteralPath -and (
        ($MaxPhaseAgeSec -gt 0 -and $WatchedPhases.Count -gt 0) -or
        ($MaxProgressAgeSec -gt 0 -and $ProgressPhases.Count -gt 0) -or
        ($ExpectedJsonField -and $ExpectedJsonValue) -or $InspectOperationalStatus
    )) {
        try {
            # Atomic publisher replacement can be briefly unreadable on
            # Windows when another scanner opens the destination without
            # delete sharing.  Retry that narrow transient window; persistent
            # I/O or JSON errors still fail closed below.
            $heartbeat = Read-JsonFileWithRetry -LiteralPath $item.FullName
            $phase = [string]$heartbeat.phase
            $result.phase = $phase
            if ($InspectOperationalStatus) {
                # Liveness and a reported failed cycle are different facts.
                # Keep this process available to retry its inputs, while the
                # health reader reports the failure instead of a green light.
                $result.reported_status = [string]$heartbeat.status
                $result.reported_phase = $phase
                $result.reported_error = [string]$heartbeat.last_error
                if (-not $result.reported_error -and $heartbeat.last_failure) {
                    $result.reported_error = [string]$heartbeat.last_failure.reason
                }
                # Starting a retry does not clear the last failed completion.
                # Only a later successful completion retires that failure.
                $unrecoveredFailure = $false
                if ($null -ne $heartbeat.last_failure.observed_epoch) {
                    $failureEpoch = [double]$heartbeat.last_failure.observed_epoch
                    $successEpoch = if ($null -ne $heartbeat.last_success_epoch) {
                        [double]$heartbeat.last_success_epoch
                    } else { 0.0 }
                    if ([double]::IsNaN($failureEpoch) -or [double]::IsInfinity($failureEpoch) -or
                        [double]::IsNaN($successEpoch) -or [double]::IsInfinity($successEpoch) -or
                        $failureEpoch -le 0 -or $successEpoch -lt 0) {
                        throw "invalid_operational_completion_clock"
                    }
                    $unrecoveredFailure = ($failureEpoch -ge $successEpoch)
                }
                $result.unrecovered_cycle_failure = $unrecoveredFailure
                $result.reported_failure = ($heartbeat.status -in @("failed", "error", "unavailable", "stopped", "partial_unavailable") -or
                    $phase -in @("failed", "cycle_failed", "stopped") -or
                    $unrecoveredFailure -or
                    ($heartbeat.errors -is [System.Array] -and $heartbeat.errors.Count -gt 0) -or
                    ($heartbeat.shared_history_prepared -ceq $false -and -not [string]::IsNullOrWhiteSpace($result.reported_error)))
            }
            if ($MaxPhaseAgeSec -gt 0 -and $WatchedPhases.Count -gt 0) {
                $phaseAgeSec = [double]$heartbeat.phase_age_sec
                $result.phase_age_sec = [math]::Round($phaseAgeSec, 1)
                if ($phase -in $WatchedPhases -and $phaseAgeSec -gt $MaxPhaseAgeSec) {
                    $result.fresh = $false
                    $result.reason = "stuck_phase"
                    $result.max_phase_age_sec = $MaxPhaseAgeSec
                }
            }
            if (
                $result.fresh -and
                $MaxProgressAgeSec -gt 0 -and
                $ProgressPhases.Count -gt 0 -and
                $phase -in $ProgressPhases
            ) {
                $progressAgeSec = [double]$heartbeat.progress_age_sec
                $result.progress_age_sec = [math]::Round($progressAgeSec, 1)
                $result.progress_sequence = [long]$heartbeat.progress_sequence
                if ($progressAgeSec -gt $MaxProgressAgeSec) {
                    $result.fresh = $false
                    $result.reason = "stalled_progress"
                    $result.max_progress_age_sec = $MaxProgressAgeSec
                }
            }
            if (
                $result.fresh -and
                $ExpectedJsonField -and
                $ExpectedJsonValue
            ) {
                $observedJsonValue = [string]$heartbeat.$ExpectedJsonField
                $result.expected_json_field = $ExpectedJsonField
                $result.expected_json_value = $ExpectedJsonValue
                $result.observed_json_value = $observedJsonValue
                if ($observedJsonValue -ne $ExpectedJsonValue) {
                    $result.fresh = $false
                    $result.reason = "runtime_contract_mismatch"
                }
            }
        } catch {
            if (($ExpectedJsonField -and $ExpectedJsonValue) -or $InspectOperationalStatus) {
                # A version-gated worker must fail closed when its heartbeat
                # cannot prove the code contract loaded by the live process.
                $result.fresh = $false
                $result.reason = "runtime_contract_unreadable"
            }
        }
    }
    return $result
}

function Start-ManagedProcess {
    param(
        [string]$Name,
        [string]$Needle,
        [string[]]$Arguments,
        [string[]]$InterpreterArguments = @(),
        [hashtable]$Freshness = @{},
        [int]$StartupDelaySec = 0,
        [string]$Executable = "",
        [ValidateSet("AboveNormal", "Normal", "BelowNormal", "Idle")]
        [string]$PriorityClass = "Normal"
    )
    # An operational profile is an exact, reviewed worker set.  The historic
    # supervisor body below still contains older maintenance/research entries;
    # allowing those entries to run in the same pass gives two owners the same
    # scripts and heartbeats.  Do not start or stop anything from that legacy
    # list here: the profile loop later in this pass owns each approved worker.
    # Existing matching profile workers must remain alive until that owner has
    # made its normal freshness decision.
    if ($OperationalProfile -and $Name -notin $opNames) {
        return @{
            name = $Name
            running = $false
            started = $false
            pids = @()
            freshness = @{
                fresh = $true
                age_sec = $null
                path = ""
                reason = "operational_profile_exclusive"
            }
        }
    }
    $relativeConflicts=@($script:MatchingPythonProcessSnapshot | Where-Object {
        Test-OperationalRelativeWorkerCommand -CommandLine ([string]$_.CommandLine) -ScriptName ([IO.Path]::GetFileName($Arguments[0]))
    })
    if ($relativeConflicts.Count) {
        return @{name=$Name;running=$null;started=$false;pids=@();freshness=@{fresh=$false;
            reason='relative_worker_identity_unverified';conflicting_pids=@($relativeConflicts | ForEach-Object {$_.ProcessId});existing_processes_preserved=$true}}
    }
    $selection=Select-OperationalRoleCandidates -Candidates @(Get-MatchingPython -Needle $Needle) -ServiceName $Name -ScriptPath $Arguments[0] -Arguments @($Arguments | Select-Object -Skip 1) -Profile $OperationalProfile
    $existing=@($selection.owned)
    $conflicts=@($selection.conflicts)
    if ($conflicts.Count) {
        return @{name=$Name;running=$true;started=$false;pids=@($existing | ForEach-Object {$_.ProcessId});
            freshness=@{fresh=$false;reason='conflicting_worker_arguments';conflicting_pids=@($conflicts | ForEach-Object {$_.ProcessId});existing_processes_preserved=$true}}
    }
    $nowEpoch = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()/1000.0
    $budgetKey = Get-OperationalRestartBudgetKey -Name $Name
    $attempts = if ($script:RestartLedger.ContainsKey($budgetKey)) { @($script:RestartLedger[$budgetKey]) } else { @() }
    $budget = Get-OperationalRestartDecision -Attempts $attempts -NowEpoch $nowEpoch
    $freshBefore = if ($Freshness.Count) { Test-FreshOutput @Freshness } else { @{fresh=$true} }
    $needsAction = $existing.Count -ne 1 -or -not $freshBefore.fresh
    if ($needsAction) {
        if (-not $script:RecoveryAllowed -or -not $budget.allowed) {
            $freshBefore.recovery_reason = if (-not $script:RecoveryAllowed) { 'recovery_expiry_reached' } else { $budget.reason }
            return @{name=$Name;running=($existing.Count -gt 0);started=$false;
                pids=@($existing | ForEach-Object {$_.ProcessId});freshness=$freshBefore}
        }
    }
    # A stale supervisor generation can leave the same worker command line
    # running under a second interpreter.  Those processes race to publish
    # the same heartbeat/archive and make a fresh file an unreliable health
    # signal.  Retain one newest instance and remove only exact duplicates;
    # the normal freshness/start path below still handles a missing or stale
    # survivor.
    if ($existing.Count -gt 1) {
        $keeper = @(
            $existing |
                Sort-Object @{ Expression = { $_.CreationDate }; Descending = $true },
                            @{ Expression = { $_.ProcessId }; Descending = $true } |
                Select-Object -First 1
        )
        $duplicates = @(
            $existing | Where-Object {
                $_.ProcessId -ne $keeper[0].ProcessId
            }
        )
        if ($duplicates.Count -gt 0) {
            Stop-MatchingPython -Name $Name -Needle $Needle -Processes $duplicates -Reason "duplicate_worker_generation"
        }
        $selection=Select-OperationalRoleCandidates -Candidates @(Get-MatchingPython -Needle $Needle) -ServiceName $Name -ScriptPath $Arguments[0] -Arguments @($Arguments | Select-Object -Skip 1) -Profile $OperationalProfile
        if ($selection.conflicts.Count) { throw 'Worker arguments changed during duplicate reconciliation.' }
        $existing=@($selection.owned)
    }
    $fresh = @{ fresh = $true; age_sec = $null; path = ""; reason = "not_checked" }
    if ($Freshness.Count -gt 0) {
        $fresh = Test-FreshOutput @Freshness
    }
    if ($existing.Count -gt 0) {
        if (-not $fresh.fresh) {
            $startupGraceSec = if ($Freshness.ContainsKey("StartupGraceSec")) {
                [int]$Freshness.StartupGraceSec
            } elseif ($Freshness.ContainsKey("MaxAgeSec")) {
                [math]::Min([int]$Freshness.MaxAgeSec, 300)
            } else {
                0
            }
            $oldestCreation = @(
                $existing |
                    Where-Object { $null -ne $_.CreationDate } |
                    Sort-Object CreationDate |
                    Select-Object -First 1
            )
            $processAgeSec = if ($oldestCreation.Count -gt 0) {
                ((Get-Date) - [datetime]$oldestCreation[0].CreationDate).TotalSeconds
            } else {
                [double]::PositiveInfinity
            }
            if ($startupGraceSec -gt 0 -and $processAgeSec -le $startupGraceSec) {
                $fresh = @{} + $fresh
                $fresh.fresh = $true
                $fresh.reason = "startup_grace"
                $fresh.process_age_sec = [math]::Round($processAgeSec, 1)
                $fresh.startup_grace_sec = $startupGraceSec
            } else {
                Stop-MatchingPython -Name $Name -Needle $Needle -Processes $existing -Reason $fresh.reason
                Start-Sleep -Seconds 2
                $selection=Select-OperationalRoleCandidates -Candidates @(Get-MatchingPython -Needle $Needle) -ServiceName $Name -ScriptPath $Arguments[0] -Arguments @($Arguments | Select-Object -Skip 1) -Profile $OperationalProfile
                if ($selection.conflicts.Count) { throw 'Worker arguments changed while prior generation exited.' }
                $existing=@($selection.owned)
                if ($existing.Count) {
                    return @{name=$Name;running=$true;started=$false;pids=@($existing | ForEach-Object {$_.ProcessId});
                        freshness=@{fresh=$false;reason='previous_generation_still_exiting';existing_processes_preserved=$true}}
                }
            }
        }
        if ($existing.Count -gt 0) {
            return @{
                name = $Name
                running = $true
                started = $false
                pids = @($existing | ForEach-Object { $_.ProcessId })
                freshness = $fresh
            }
        }
    }
    $supervisorAgeSec = (
        (Get-Date).ToUniversalTime() - $SupervisorStartedUtc
    ).TotalSeconds
    if ($StartupDelaySec -gt 0 -and $supervisorAgeSec -lt $StartupDelaySec) {
        return @{
            name = $Name
            running = $false
            started = $false
            pids = @()
            freshness = @{
                fresh = $true
                age_sec = $null
                path = ""
                reason = "startup_stagger"
                starts_in_sec = [math]::Ceiling(
                    $StartupDelaySec - $supervisorAgeSec
                )
            }
        }
    }
    # Reserve before launch; a crash or failed launch still consumes its attempt.
    $script:RestartLedger[$budgetKey] = @($budget.attempts) + @($nowEpoch)
    Write-OperationalAtomicJson -LiteralPath $RestartLedgerPath -Value $script:RestartLedger
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss_fff"
    $stdout = Join-Path $Logs ($Name + "_supervised_" + $stamp + ".out.log")
    $stderr = Join-Path $Logs ($Name + "_supervised_" + $stamp + ".err.log")
    $ProcessExecutable = if ($Executable) { $Executable } else { $Python }
    $scriptPath = if ($Arguments.Count -gt 0) { [string]$Arguments[0] } else { "" }
    $missingTarget = if (-not (Test-Path -LiteralPath $ProcessExecutable)) {
        $ProcessExecutable
    } elseif ($scriptPath -match '(?i)\.py$' -and -not (Test-Path -LiteralPath $scriptPath)) {
        $scriptPath
    } else {
        ""
    }
    if ($missingTarget) {
        Write-SupervisorEvent "process_start_blocked_missing_target" @{
            name = $Name
            executable = $ProcessExecutable
            script = $scriptPath
            missing_target = $missingTarget
        }
        return @{
            name = $Name
            running = $false
            started = $false
            pids = @()
            freshness = @{
                fresh = $false
                age_sec = $null
                path = $missingTarget
                reason = "missing_start_target"
            }
        }
    }
    try {
        $relayArguments = @('-B',(Join-Path $Trad 'oanda_operational_log_relay_v2.py'),
            '--log-directory',(Join-Path $BoundedChildLogs $Name),'--priority-class',$PriorityClass,
            '--',$ProcessExecutable) + $InterpreterArguments + $Arguments
        $quotedArguments = @($relayArguments | ForEach-Object { ConvertTo-OperationalProcessArgument $_ })
        $proc = Start-Process -FilePath $ProcessExecutable `
            -ArgumentList $quotedArguments `
            -WorkingDirectory $Root `
            -WindowStyle Hidden `
            -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr `
            -PassThru `
            -ErrorAction Stop
    } catch {
        Write-SupervisorEvent "process_start_error" @{
            name = $Name
            executable = $ProcessExecutable
            script = $scriptPath
            error = $_.Exception.Message
        }
        return @{
            name = $Name
            running = $false
            started = $false
            pids = @()
            freshness = @{
                fresh = $false
                age_sec = $null
                path = $scriptPath
                reason = "process_start_error"
            }
        }
    }
    try {
        $proc.PriorityClass = $PriorityClass
    } catch {
        Write-SupervisorEvent "process_priority_warning" @{
            name = $Name
            pid = $proc.Id
            requested_priority = $PriorityClass
            error = $_.Exception.Message
        }
    }
    Write-SupervisorEvent "process_started" @{
        name = $Name
        pid = $proc.Id
        stdout = $stdout
        stderr = $stderr
        priority = $PriorityClass
        executable = $ProcessExecutable
    }
    Add-StartedProcessToMatchingSnapshot -ProcessId $proc.Id
    return @{ name = $Name; running = $true; started = $true; pids = @($proc.Id); freshness = $fresh }
}



try {
    Write-SupervisorEvent 'supervisor_started' @{operational_profile_path=$OperationalProfilePath;operational_profile_sha256=$OperationalProfileSha256;research_only=$true;can_place_orders=$false}
    while ($true) {
        $managed=@()
        $errorText=''
        try {
            $current=Read-OperationalRecoveryProfile -Trad $Trad -ProfilePath $OperationalProfilePath -RecoveryUntilUtc $RecoveryUntilUtc -AllowExpired
            if ($current.sha256 -cne $OperationalProfileSha256) { throw 'Operational profile changed while supervisor was running.' }
            $script:RecoveryAllowed=$current.recovery_allowed
            $null=Refresh-MatchingPythonProcessSnapshot
            foreach($service in $OperationalProfile.services) {
                $arguments=@((Join-Path $Trad $service.script))+@($service.arguments | ForEach-Object {[string]$_})
                $freshness=@{LiteralPath=[string]$service.heartbeat;MaxAgeSec=[int]$service.max_age_sec;
                    StartupGraceSec=[int]$service.startup_grace_sec;ExpectedJsonField=[string]$service.heartbeat_field;
                    ExpectedJsonValue=[string]$service.heartbeat_schema;InspectOperationalStatus=$true}
                $interpreterArguments=@(Get-OperationalInterpreterArguments -Service $service -DataRoot $DataRoot)
                $managed+=Start-ManagedProcess -Name $service.name -Needle $service.needle -Arguments $arguments `
                    -InterpreterArguments $interpreterArguments -Executable $CoreTimeseriesPython -PriorityClass (Get-OperationalRolePriority -Service $service) -Freshness $freshness
            }
        } catch { $errorText=$_.Exception.Message; Write-SupervisorEvent 'supervisor_error' @{error=$errorText} }
        $heartbeat=@{schema_version='operational_supervisor_v6_20260916';generated_utc=[DateTime]::UtcNow.ToString('o');
            supervisor_pid=$PID;operational_profile_path=$OperationalProfilePath;operational_profile_sha256=$OperationalProfileSha256;
            recovery_until_utc=$RecoveryUntilUtc;recovery_allowed=$script:RecoveryAllowed;managed=$managed;error=$errorText;
            event_records_dropped=$script:EventDrops;research_only=$true;can_place_orders=$false;
            recovery_expiry_scope='No new start/stop/restart after expiry; healthy adopted children and their original bounded lifetimes are preserved.';
            adopted_log_scope='Existing child log handles are unchanged; bounded relay applies only to future starts.'}
        Write-OperationalAtomicJson -LiteralPath $SupervisorHeartbeat -Value $heartbeat
        Write-SupervisorEvent 'heartbeat' @{managed=$managed;error=$errorText;recovery_allowed=$script:RecoveryAllowed}
        Start-Sleep -Seconds $IntervalSec
    }
} finally { if ($ownsMutex) { $SupervisorMutex.ReleaseMutex() }; $SupervisorMutex.Dispose() }
