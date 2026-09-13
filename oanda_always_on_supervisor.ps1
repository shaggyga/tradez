param(
    [string]$Root = "",
    [string]$AccountKey = "OANDA_ACCOUNT_ID_DUM4",
    [string]$RunLabel = "unified-signal-confidence-matrix-v10",
    [int]$IntervalSec = 30,
    [int]$ChildDurationSec = 604800,
    [string]$PythonPath = "",
    [string]$CredsPath = "",
    [switch]$EnableCrypto,
    [switch]$EnableModelGapLive,
    [switch]$EnableSubMinuteResearch,
    [switch]$EnableLegacyFlatPracticeBots,
    [switch]$SafeCoreOnly,
    [switch]$ResearchCollectionOnly
)

$ErrorActionPreference = "Stop"

if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}

$Trad = Join-Path $Root "trad"
$ProjectPython = Join-Path $Trad "data\oanda_training_manager\.research_py313\Scripts\python.exe"
$CoreTimeseriesPython = Join-Path $env:LOCALAPPDATA "CodexRuntimes\timeseries312\Scripts\python.exe"
$Python = if ($PythonPath) {
    $PythonPath
} elseif (Test-Path -LiteralPath $ProjectPython) {
    $ProjectPython
} elseif (Test-Path -LiteralPath $CoreTimeseriesPython) {
    $CoreTimeseriesPython
} else {
    throw "No project or core time-series Python runtime is available."
}
$ModelGapPython = if (Test-Path -LiteralPath $CoreTimeseriesPython) {
    $CoreTimeseriesPython
} else {
    $Python
}
$ResolvedCreds = if ($CredsPath) { $CredsPath } else { Join-Path $Trad "creds" }
$env:OANDA_CREDS_PATH = $ResolvedCreds
$env:TRAD_CREDS_PATH = $ResolvedCreds
$env:TRAD_PROJECT_ROOT = $Trad
$env:FOREX_ALLOW_LIVE = "0"
$env:FOREX_LIVE_EXECUTE = "0"
# The desktop app and a long-running supervisor do not automatically inherit
# credentials saved to the current user's environment after they start.  Load
# only the named research-data credentials into this supervisor process so
# newly started collectors inherit them.  Never log or copy their values.
foreach ($credentialName in @(
    "FRED_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "FINNHUB_API_KEY",
    "TRADING_ECONOMICS_API_KEY",
    "TE_API_KEY"
)) {
    $processValue = [Environment]::GetEnvironmentVariable($credentialName, "Process")
    if ([string]::IsNullOrWhiteSpace($processValue)) {
        $userValue = [Environment]::GetEnvironmentVariable($credentialName, "User")
        if (-not [string]::IsNullOrWhiteSpace($userValue)) {
            [Environment]::SetEnvironmentVariable(
                $credentialName,
                $userValue,
                "Process"
            )
        }
    }
}
$DataRoot = Join-Path $Trad "data\oanda_training_manager"
$Logs = Join-Path $DataRoot "logs"
$State = Join-Path $DataRoot "state"
$Reports = Join-Path $DataRoot "reports"
$DepthState = Join-Path $DataRoot "prospective_depth_parquet\collector_state.json"
$BookState = Join-Path $DataRoot "prospective_order_position_book\collector_state.json"
$ModelGapLiveState = Join-Path $State "model_gap_live_worker_v1.json"
New-Item -ItemType Directory -Force -Path $Logs | Out-Null

$SafeCoreSkippedNames = @(
    "crypto_shadow",
    "hgb_adaptive_fit",
    "arima_multiframe_sweep",
    "forex_model_vault_sync",
    "continuous_improvement",
    "depth_parquet",
    "order_position_book",
    "model_predictor_auto_promotion",
    "model_gap_live_signal",
    "move_alert_monitor",
    "signal_combination_fit",
    "signal_interaction_deep_fit",
    "signal_combination_historical_fit",
    # This policy compiler depends on manually generated model-gap validation
    # artifacts. Safe-core mode must not restart it continuously when those
    # offline inputs are intentionally absent.
    "post_gap_execution_policy",
    "second_forecast_fit",
    # The retained S1/sub-minute tracker is not part of the current decision
    # surface.  Safe-core recovery should not recreate its high-write ledger.
    "second_forecast_tracker"
)
# Closed research allow-list, independent of promotion gates. Registered
# study workers write isolated research ledgers only. Execution,
# authorization and promotion remain excluded; unknown names stay disabled.
$ResearchCollectionNames = @(
    "account_snapshot",
    "live_dashboard",
    "local_news_sentiment",
    "official_release_fast_lane",
    "official_release_fast_mapper",
    "source_governance_news_fast_lane",
    "practice_007_quote_stream",
    "research_feature_observations_v1",
    "research_feature_forward_v1",
    "clock_integrity_monitor",
    "all68_m1_forward_archive",
    "project_integrity_audit",
    "storage_headroom_guard",
    "causal_forecast_study_v1",
    "eurusd_local_forecast_study",
    "pair_local_forecast_study_v1",
    "pair_local_forecast_study_v2",
    "joint_price_news_study_v1",
    "joint_price_news_study_v2",
    "local_news_sentiment_repair_v1",
    "joint_price_news_study_v3"
)
# The sole collection-mode publisher records its dedicated-stream producer
# identity. Preserve the pre-restart snapshot in the restart receipt; never
# seed old prices as new observations or race an executor on this destination.
$DedicatedQuoteSnapshot = if ($ResearchCollectionOnly) {
    Join-Path $State "practice_007_market_quotes_v1.json"
} else {
    Join-Path $State "practice_007_market_quotes_transport_check_v1.json"
}
# These research collectors have exhausted or obsolete inputs.  Keep their code
# and immutable ledgers as historical negative controls, but do not spend live
# CPU, I/O, or signal-feed capacity refreshing them.  The evidence and reasons
# are frozen in config/shadow_runtime_retirements_v1.json.  Reopening any one
# requires a materially new contract/cohort and a code-reviewed allow-list edit.
$DisabledNames = @(
    # September 9: original joint V1/V2 obligations are terminal in all 136
    # ledgers. Preserve the original registries/records; selected V3 continues.
    # Separate dated disposition: config/joint_study_runtime_retirement_v1_20260909.json.
    "joint_price_news_study_v1",
    "joint_price_news_study_v2",
    # September 6 clock audit: preserve old output as diagnostics. The separate
    # causal study has its own immutable input/publication/entry ledger.
    "proof_shadow_predictors",
    "canonical_outcome_worker",
    "timeframe_matrix_calibration",
    "practice_019_hgb_rotation",
    "hgb_live_outcomes",
    "hgb_adaptive_fit",
    "manager_decision_outcome_ledger",
    # Cohort B is immutable and terminally incomplete.  The live source
    # registry has legitimately advanced, so reopening the producer would
    # contaminate lineage and the read-only verifier now publishes its final
    # failed audit from the exact committed frozen bytes.  Preserve both
    # database and verifier artifacts; a retry requires a new cohort ID.
    "official_event_paired_evaluator_v1",
    "official_event_paired_evaluator_verifier_v1",
    # V1 is a superseded exact-headline baseline.  V2 and V3 are terminally
    # invalid because their knowledge-time parser truncated subsecond mapping
    # receipts, allowing a receipt just after move onset to appear causal.
    # Preserve every immutable ledger/report byte; the clean V4 cohort starts
    # with zero imports and no backfill under a subsecond-precise clock.
    "move_first_operational_mapping_alignment_v1",
    "move_first_operational_mapping_alignment_v2",
    "move_first_operational_mapping_alignment_v3"
)
if (-not $EnableCrypto) {
    $DisabledNames += "crypto_shadow"
}
if (-not $EnableModelGapLive) {
    # Offline model-gap completion is bounded and is run manually.  Do not
    # keep the large live forecast ledger and promotion reader on the
    # always-on critical path unless an operator explicitly opts in.
    $DisabledNames += @(
        "model_predictor_auto_promotion",
        "model_gap_live_signal"
    )
}
if (-not $EnableSubMinuteResearch) {
    # Sub-minute forecasting is retained on disk for later research, but it is
    # not part of the active practice-007 signal or execution path.
    $DisabledNames += @(
        "micro_pattern",
        "second_forecast_fit",
        "second_forecast_hot",
        "second_forecast_microstructure"
    )
}
if (-not $EnableLegacyFlatPracticeBots) {
    $DisabledNames += @(
        "practice_003_technical",
        "practice_005_gpt_exp",
        "practice_013_formula83"
    )
}
# Practice-006 is retired as an account/routing idea, not merely disabled by
# the default legacy switch. Its historical state remains on disk as evidence.
$DisabledNames += "practice_006_spike_scout"
$CausalStudyConfig = Join-Path $Trad "config\causal_forecast_study_gap_v2_20260907.json"
$CausalStudyEnabled = $false
if (Test-Path -LiteralPath $CausalStudyConfig) {
    try {
        $studyConfig = Get-Content -LiteralPath $CausalStudyConfig -Raw | ConvertFrom-Json
        $CausalStudyEnabled = ($studyConfig.collection_enabled -eq $true)
    } catch { $CausalStudyEnabled = $false }
}
if (-not $CausalStudyEnabled) { $DisabledNames += "causal_forecast_study_v1" }
$EurusdStudyConfig = Join-Path $Trad "config\causal_forecast_study_eurusd_v1_20260907.json"
$EurusdStudyEnabled = $false
if (Test-Path -LiteralPath $EurusdStudyConfig) {
    try {
        $localStudyConfig = Get-Content -LiteralPath $EurusdStudyConfig -Raw | ConvertFrom-Json
        $EurusdStudyEnabled = ($localStudyConfig.collection_enabled -eq $true)
    } catch { $EurusdStudyEnabled = $false }
}
if (-not $EurusdStudyEnabled) { $DisabledNames += "eurusd_local_forecast_study" }
$PairStudyConfig = Join-Path $Trad "config\pair_local_forecast_study_v1_20260907.json"
$PairStudyEnabled = $false
if (Test-Path -LiteralPath $PairStudyConfig) {
    try {
        $pairStudyConfigValue = Get-Content -LiteralPath $PairStudyConfig -Raw | ConvertFrom-Json
        $PairStudyEnabled = (
            $pairStudyConfigValue.schema_version -ceq "pair_local_forecast_registry_v1_20260907" -and
            $pairStudyConfigValue.collection_enabled -is [bool] -and $pairStudyConfigValue.collection_enabled -eq $true -and
            $pairStudyConfigValue.research_only -is [bool] -and $pairStudyConfigValue.research_only -eq $true
        )
        foreach ($pairStudyInertFlag in @("can_place_orders", "can_promote", "can_authorize", "account_eligible", "proof_eligible", "historical_rows_imported")) {
            if ($pairStudyConfigValue.$pairStudyInertFlag -isnot [bool] -or $pairStudyConfigValue.$pairStudyInertFlag -ne $false) {
                $PairStudyEnabled = $false
            }
        }
    } catch { $PairStudyEnabled = $false }
}
# The worker separately verifies frozen source/dependency bindings and requires
# matching pre-existing activation receipts. Supervision never creates them.
if (-not $PairStudyEnabled) { $DisabledNames += "pair_local_forecast_study_v1" }
$PairStudyV2Config = Join-Path $Trad "config\pair_local_forecast_study_v2_20260907.json"
$PairStudyV2Enabled = $false
if (Test-Path -LiteralPath $PairStudyV2Config) {
    try {
        $pairV2ConfigValue = Get-Content -LiteralPath $PairStudyV2Config -Raw | ConvertFrom-Json
        $PairStudyV2Enabled = (
            $pairV2ConfigValue.schema_version -ceq "pair_local_forecast_registry_v2_20260907" -and
            $pairV2ConfigValue.collection_enabled -is [bool] -and $pairV2ConfigValue.collection_enabled -eq $true -and
            $pairV2ConfigValue.research_only -is [bool] -and $pairV2ConfigValue.research_only -eq $true
        )
        foreach ($pairV2InertFlag in @("can_place_orders", "can_promote", "can_authorize", "account_eligible", "proof_eligible", "historical_rows_imported")) {
            if ($pairV2ConfigValue.$pairV2InertFlag -isnot [bool] -or $pairV2ConfigValue.$pairV2InertFlag -ne $false) {
                $PairStudyV2Enabled = $false
            }
        }
    } catch { $PairStudyV2Enabled = $false }
}
if (-not $PairStudyV2Enabled) { $DisabledNames += "pair_local_forecast_study_v2" }
$JointPriceNewsConfig = Join-Path $Trad "config\joint_price_news_study_v1_20260907.json"
$JointPriceNewsEnabled = $false
if (Test-Path -LiteralPath $JointPriceNewsConfig) {
    try {
        $jointNewsConfigValue = Get-Content -LiteralPath $JointPriceNewsConfig -Raw | ConvertFrom-Json
        $JointPriceNewsEnabled = (
            $jointNewsConfigValue.schema_version -ceq "joint_price_news_registry_v1_20260907" -and
            $jointNewsConfigValue.collection_enabled -is [bool] -and $jointNewsConfigValue.collection_enabled -eq $true -and
            $jointNewsConfigValue.research_only -is [bool] -and $jointNewsConfigValue.research_only -eq $true
        )
        foreach ($jointNewsInertFlag in @("can_place_orders", "can_promote", "can_authorize", "account_eligible", "proof_eligible", "historical_rows_imported")) {
            if ($jointNewsConfigValue.$jointNewsInertFlag -isnot [bool] -or $jointNewsConfigValue.$jointNewsInertFlag -ne $false) {
                $JointPriceNewsEnabled = $false
            }
        }
    } catch { $JointPriceNewsEnabled = $false }
}
if (-not $JointPriceNewsEnabled) { $DisabledNames += "joint_price_news_study_v1" }
# Retire superseded live attempts only after explicit selection of the new
# registered primary. Their contracts, source and evidence stay untouched;
# the prior pair study continues as the separately labeled comparison.
$PairPrimaryConfig = Join-Path $Trad "config\pair_forecast_primary_current.json"
if ($PairStudyV2Enabled -and (Test-Path -LiteralPath $PairPrimaryConfig)) {
    try {
        $pairPrimaryValue = Get-Content -LiteralPath $PairPrimaryConfig -Raw | ConvertFrom-Json
        # Use .NET directly: desktop-launched Windows PowerShell may inherit
        # another PowerShell version's module path and cannot autoload Get-FileHash.
        $pairV2Hasher = [Security.Cryptography.SHA256]::Create()
        try {
            $pairV2RegistryHash = ([BitConverter]::ToString($pairV2Hasher.ComputeHash([IO.File]::ReadAllBytes($PairStudyV2Config)))).Replace("-", "").ToLowerInvariant()
        } finally { $pairV2Hasher.Dispose() }
        $pairPrimaryEpoch = $pairPrimaryValue.activated_epoch
        $pairPrimaryEpochValid = (
            ($pairPrimaryEpoch -is [int] -or $pairPrimaryEpoch -is [long] -or $pairPrimaryEpoch -is [double] -or $pairPrimaryEpoch -is [decimal]) -and
            -not [double]::IsNaN([double]$pairPrimaryEpoch) -and -not [double]::IsInfinity([double]$pairPrimaryEpoch) -and
            $pairPrimaryEpoch -gt 0 -and $pairPrimaryEpoch -le ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0)
        )
        if ($pairPrimaryValue.schema_version -ceq "pair_forecast_primary_selection_v1_20260907" -and
            $pairPrimaryValue.selected -ceq "v2" -and $pairPrimaryValue.registry_sha256 -ceq $pairV2RegistryHash -and
            $pairPrimaryEpochValid) {
            $DisabledNames += @("causal_forecast_study_v1", "eurusd_local_forecast_study")
        }
    } catch { }
}

# Keep v1 collecting its already issued outcomes as a separate comparison.
# The fair-scheduler successor requires its own inert source-bound registry.
$JointPriceNewsV2Config = Join-Path $Trad "config\joint_price_news_study_v2_20260907.json"
$JointPriceNewsV2Enabled = $false
if (Test-Path -LiteralPath $JointPriceNewsV2Config) {
    try {
        $jointNewsV2ConfigValue = Get-Content -LiteralPath $JointPriceNewsV2Config -Raw | ConvertFrom-Json
        $JointPriceNewsV2Enabled = (
            $jointNewsV2ConfigValue.schema_version -ceq "joint_price_news_registry_v2_20260907" -and
            $jointNewsV2ConfigValue.collection_enabled -is [bool] -and $jointNewsV2ConfigValue.collection_enabled -eq $true -and
            $jointNewsV2ConfigValue.research_only -is [bool] -and $jointNewsV2ConfigValue.research_only -eq $true
        )
        foreach ($jointNewsV2InertFlag in @("can_place_orders", "can_promote", "can_authorize", "account_eligible", "proof_eligible", "historical_rows_imported")) {
            if ($jointNewsV2ConfigValue.$jointNewsV2InertFlag -isnot [bool] -or $jointNewsV2ConfigValue.$jointNewsV2InertFlag -ne $false) {
                $JointPriceNewsV2Enabled = $false
            }
        }
    } catch { $JointPriceNewsV2Enabled = $false }
}
if (-not $JointPriceNewsV2Enabled) { $DisabledNames += "joint_price_news_study_v2" }

# Separately registered repaired news publication and its new forecast cohort.
# Existing collectors and v1/v2 forecast records keep their original sources.
$JointPriceNewsV3Config = Join-Path $Trad "config\joint_price_news_study_v3_20260908.json"
$JointPriceNewsV3Enabled = $false
if (Test-Path -LiteralPath $JointPriceNewsV3Config) {
    try {
        $jointNewsV3ConfigValue = Get-Content -LiteralPath $JointPriceNewsV3Config -Raw | ConvertFrom-Json
        $JointPriceNewsV3Enabled = (
            $jointNewsV3ConfigValue.schema_version -ceq "joint_price_news_registry_v3_20260908" -and
            $jointNewsV3ConfigValue.collection_enabled -is [bool] -and $jointNewsV3ConfigValue.collection_enabled -eq $true -and
            $jointNewsV3ConfigValue.research_only -is [bool] -and $jointNewsV3ConfigValue.research_only -eq $true
        )
        foreach ($jointNewsV3InertFlag in @("can_place_orders", "can_promote", "can_authorize", "account_eligible", "proof_eligible", "historical_rows_imported")) {
            if ($jointNewsV3ConfigValue.$jointNewsV3InertFlag -isnot [bool] -or $jointNewsV3ConfigValue.$jointNewsV3InertFlag -ne $false) {
                $JointPriceNewsV3Enabled = $false
            }
        }
    } catch { $JointPriceNewsV3Enabled = $false }
}
if (-not $JointPriceNewsV3Enabled) { $DisabledNames += @("local_news_sentiment_repair_v1", "joint_price_news_study_v3") }

$SupervisorMutex = [System.Threading.Mutex]::new(
    $false,
    "Global\ForexOandaAlwaysOnSupervisorV1"
)
$SupervisorMutexAcquired = $false
try {
    $SupervisorMutexAcquired = $SupervisorMutex.WaitOne(0)
} catch [System.Threading.AbandonedMutexException] {
    $SupervisorMutexAcquired = $true
}
if (-not $SupervisorMutexAcquired) {
    Write-Output "Another Forex OANDA supervisor already owns the global mutex."
    exit 3
}

$SupervisorLog = Join-Path $Logs ("always_on_supervisor_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".jsonl")
$ArtifactSizeHistory = @{}
$script:MatchingPythonProcessSnapshot = @()

function Write-SupervisorEvent {
    param([string]$Event, [hashtable]$Fields = @{})
    $payload = [ordered]@{
        time = (Get-Date).ToUniversalTime().ToString("o")
        event = $Event
    }
    foreach ($key in $Fields.Keys) {
        $payload[$key] = $Fields[$key]
    }
    $line = $payload | ConvertTo-Json -Compress -Depth 6
    for ($attempt = 1; $attempt -le 4; $attempt++) {
        try {
            Add-Content `
                -LiteralPath $SupervisorLog `
                -Value $line `
                -Encoding utf8 `
                -ErrorAction Stop
            return
        } catch {
            if ($attempt -lt 4) {
                Start-Sleep -Milliseconds (100 * $attempt)
            }
        }
    }
    # Logging is diagnostic and must never terminate process supervision.
    # The next successful event will restore the durable stream.
}

function Refresh-MatchingPythonProcessSnapshot {
    # Win32_Process enumeration is materially more expensive than exact
    # in-memory matching. Capture one root-scoped snapshot per supervision
    # cycle instead of repeating the same CIM query for every managed worker.
    # A failed refresh aborts that cycle rather than treating an empty or stale
    # view as authority to launch duplicates.
    $script:MatchingPythonProcessSnapshot = @(
        Get-CimInstance Win32_Process -ErrorAction Stop |
            Where-Object {
                $commandLine = [string]$_.CommandLine
                $_.Name -eq "python.exe" -and
                $commandLine -like ("*" + $Root + "*") -and
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
    @($script:MatchingPythonProcessSnapshot |
        Where-Object {
            $commandLine = [string]$_.CommandLine
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
}

function Stop-MatchingPython {
    param(
        [string]$Name,
        [string]$Needle,
        [object[]]$Processes,
        [string]$Reason
    )
    $remaining = @($Processes)
    for ($pass = 0; $pass -lt 3 -and $remaining.Count -gt 0; $pass++) {
        foreach ($proc in @($remaining | Sort-Object CreationDate -Descending)) {
            try {
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
        $remaining = @(Get-MatchingPython -Needle $Needle)
    }
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
        [string]$ExpectedJsonValue = ""
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
        fresh = ($age -le $MaxAgeSec)
        age_sec = [math]::Round($age, 1)
        path = $item.FullName
        reason = if ($age -le $MaxAgeSec) { "fresh" } else { "stale_output" }
    }
    if ($result.fresh -and $LiteralPath -and (
        ($MaxPhaseAgeSec -gt 0 -and $WatchedPhases.Count -gt 0) -or
        ($MaxProgressAgeSec -gt 0 -and $ProgressPhases.Count -gt 0) -or
        ($ExpectedJsonField -and $ExpectedJsonValue)
    )) {
        try {
            # Atomic publisher replacement can be briefly unreadable on
            # Windows when another scanner opens the destination without
            # delete sharing.  Retry that narrow transient window; persistent
            # I/O or JSON errors still fail closed below.
            $heartbeat = Read-JsonFileWithRetry -LiteralPath $item.FullName
            $phase = [string]$heartbeat.phase
            $result.phase = $phase
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
            if ($ExpectedJsonField -and $ExpectedJsonValue) {
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
        [hashtable]$Freshness = @{},
        [int]$StartupDelaySec = 0,
        [string]$Executable = "",
        [ValidateSet("AboveNormal", "Normal", "BelowNormal", "Idle")]
        [string]$PriorityClass = "Normal"
    )
    $disabledReason = if ($ResearchCollectionOnly -and $Name -notin $ResearchCollectionNames) {
        "research_collection_only"
    } elseif ($Name -in $DisabledNames) {
        "disabled"
    } elseif ($SafeCoreOnly -and $Name -in $SafeCoreSkippedNames) {
        "safe_core_only"
    } else {
        ""
    }
    if ($disabledReason) {
        $existing = @(Get-MatchingPython -Needle $Needle)
        if ($existing.Count -gt 0) {
            Stop-MatchingPython -Name $Name -Needle $Needle -Processes $existing -Reason $disabledReason
        }
        return @{
            name = $Name
            running = $false
            started = $false
            pids = @()
            freshness = @{
                fresh = $true
                age_sec = $null
                path = ""
                reason = $disabledReason
            }
        }
    }
    $existing = @(Get-MatchingPython -Needle $Needle)
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
                $existing = @()
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
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
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
        $proc = Start-Process -FilePath $ProcessExecutable `
            -ArgumentList $Arguments `
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

$SupervisorStartedUtc = (Get-Date).ToUniversalTime()
Write-SupervisorEvent "supervisor_started" @{
    root = $Root
    account_key = $AccountKey
    run_label = $RunLabel
    interval_sec = $IntervalSec
    child_duration_sec = $ChildDurationSec
    python = $Python
    model_gap_python = $ModelGapPython
    creds_path = $ResolvedCreds
    research_collection_only = [bool]$ResearchCollectionOnly
    research_collection_names = $(if ($ResearchCollectionOnly) { $ResearchCollectionNames } else { @() })
    dedicated_quote_snapshot = $DedicatedQuoteSnapshot
}

while ($true) {
    try {
        $null = Refresh-MatchingPythonProcessSnapshot
        $managed = @()
        $managed += Start-ManagedProcess `
            -Name "account_snapshot" `
            -Needle "account_007_dashboard_v1.json" `
            -Arguments @(
                (Join-Path $Trad "oanda_account_snapshot_writer.py"),
                "--creds", $ResolvedCreds,
                "--account-key", "$AccountKey",
                "--output", (Join-Path $State "account_007_dashboard_v1.json"),
                "--mirror-output", (Join-Path $State "account_dashboard_v1.json"),
                "--interval-sec", "1",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "account_007_dashboard_v1.json")
                MaxAgeSec = 60
                StartupGraceSec = 60
            }
        $managed += Start-ManagedProcess `
            -Name "live_dashboard" `
            -Needle "oanda_practice_live_dashboard.py" `
            -Arguments @(
                (Join-Path $Trad "oanda_practice_live_dashboard.py"),
                "--port", "8765",
                "--max-runs", "20",
                "--max-lines", "20000"
            )
        $managed += Start-ManagedProcess `
            -Name "local_news_sentiment" `
            -Needle "oanda_local_news_sentiment.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_local_news_sentiment.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                # Liveness is intentionally separate from the last completed
                # evidence snapshot. A full official-source pass can exceed
                # 30 minutes on a memory-constrained research host while its
                # database remains active and healthy.
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\collector_heartbeat_v1.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
                MaxProgressAgeSec = 900
                ProgressPhases = @(
                    "starting_cycle",
                    "opening_database",
                    "collecting_sources",
                    "postprocessing_evidence",
                    "publishing_derived_views",
                    "refreshing_event_catalog",
                    "publishing_completed_snapshot"
                )
                ExpectedJsonField = "classification_version"
                ExpectedJsonValue = "local_fx_news_rules_20260907_v165_causal_member_admission"
            }
        # Preserve low-latency first-seen clocks for the mapped 21-currency
        # central-bank release transports independently of the broad news
        # collector's heavier enrichment and clustering cycle. This worker is
        # raw-observation-only and has no broker, scoring, authorization or
        # promotion surface.
        $managed += Start-ManagedProcess `
            -Name "official_release_fast_lane" `
            -Needle "oanda_official_release_fast_lane.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_release_fast_lane.py"),
                "--interval-sec", "15",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_release_fast_lane_heartbeat_v4.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ProgressPhases = @(
                    "starting_cycle",
                    "polling_official_sources"
                )
            }
        $managed += Start-ManagedProcess `
            -Name "official_release_fast_mapper" `
            -Needle "oanda_official_release_fast_mapper.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_release_fast_mapper.py"),
                "--interval-sec", "5",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_release_fast_mapping_heartbeat_v3.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ProgressPhases = @(
                    "loading_observations",
                    "mapping_observations"
                )
                ExpectedJsonField = "required_classification_version"
                ExpectedJsonValue = "local_fx_news_rules_20260907_v165_causal_member_admission"
            }
        # Preserve a second, independent raw-event quote boundary at per-pair
        # resolution. The legacy exact-all-68 sidecar remains unchanged. This
        # cohort requires explicit OANDA tradeability and freshness for each
        # pair, so a stale/nontradeable exotic cannot erase valid major-pair
        # research evidence. It reads no semantic mapping and cannot execute,
        # authorize or promote.
        $managed += Start-ManagedProcess `
            -Name "official_event_pair_quote_capture_v1" `
            -Needle "oanda_official_event_pair_quote_capture_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_pair_quote_capture_v1.py"),
                "--interval-sec", "1",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_pair_quote_capture_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
                MaxProgressAgeSec = 60
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "oanda_official_event_pair_quote_capture_v1"
            }
        $managed += Start-ManagedProcess `
            -Name "official_event_pair_quote_capture_v1_verifier" `
            -Needle "oanda_official_event_pair_quote_capture_v1_verifier.py" `
            -StartupDelaySec 3 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_pair_quote_capture_v1_verifier.py"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_pair_quote_capture_verifier_heartbeat_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxProgressAgeSec = 120
                ExpectedJsonField = "status"
                ExpectedJsonValue = "verified"
            }
        # V2's first untouched event proved dual-ledger identity deduplication,
        # but also proved its worker-cycle timestamp could precede the quote
        # snapshot it actually read. Preserve its files/database and stop only
        # its obsolete runtime; V3 starts empty and uses the post-read clock.
        $retiredOfficialEventV2 = @(
            Get-MatchingPython -Needle "oanda_official_event_pair_quote_capture_v2.py"
        )
        if ($retiredOfficialEventV2.Count -gt 0) {
            Stop-MatchingPython `
                -Name "official_event_pair_quote_capture_v2_retired" `
                -Needle "oanda_official_event_pair_quote_capture_v2.py" `
                -Processes $retiredOfficialEventV2 `
                -Reason "v3_quote_snapshot_read_clock_cutover"
        }
        $retiredOfficialEventV2Verifier = @(
            Get-MatchingPython -Needle "oanda_official_event_pair_quote_capture_v2_verifier.py"
        )
        if ($retiredOfficialEventV2Verifier.Count -gt 0) {
            Stop-MatchingPython `
                -Name "official_event_pair_quote_capture_v2_verifier_retired" `
                -Needle "oanda_official_event_pair_quote_capture_v2_verifier.py" `
                -Processes $retiredOfficialEventV2Verifier `
                -Reason "v3_quote_snapshot_read_clock_cutover"
        }
        $managed += Start-ManagedProcess `
            -Name "official_event_pair_quote_capture_v3" `
            -Needle "oanda_official_event_pair_quote_capture_v3.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_pair_quote_capture_v3.py"),
                "--interval-sec", "1",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_pair_quote_capture_heartbeat_v3.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
                MaxProgressAgeSec = 60
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "oanda_official_event_pair_quote_capture_v3"
            }
        $managed += Start-ManagedProcess `
            -Name "official_event_pair_quote_capture_v3_verifier" `
            -Needle "oanda_official_event_pair_quote_capture_v3_verifier.py" `
            -StartupDelaySec 3 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_pair_quote_capture_v3_verifier.py"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_pair_quote_capture_verifier_heartbeat_v3.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxProgressAgeSec = 120
                ExpectedJsonField = "status"
                ExpectedJsonValue = "verified"
            }
        # Mature the valid per-pair T0 observations under a separate frozen
        # prospective contract. Each pair receives one terminal executable
        # bid/ask attempt at 1/5/15/30/60 minutes; an invalid exotic cannot
        # erase another pair. Both directions are research counterfactuals,
        # and this path cannot promote, authorize or execute.
        $managed += Start-ManagedProcess `
            -Name "official_event_pair_horizon_capture_v1" `
            -Needle "oanda_official_event_pair_horizon_capture_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_pair_horizon_capture_v1.py"),
                "--interval-sec", "1",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_pair_horizon_capture_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
                MaxProgressAgeSec = 60
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "official_event_pair_horizon_capture_v1_per_pair_terminal_20260902"
            }
        $managed += Start-ManagedProcess `
            -Name "official_event_pair_horizon_capture_v1_verifier" `
            -Needle "oanda_official_event_pair_horizon_capture_v1_verifier.py" `
            -StartupDelaySec 3 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_pair_horizon_capture_v1_verifier.py"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_pair_horizon_capture_verifier_heartbeat_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxProgressAgeSec = 120
                ExpectedJsonField = "status"
                ExpectedJsonValue = "verified"
            }
        # Complete the event-clock market path prospectively. The raw fast
        # lane owns T0; this isolated worker makes one terminal all-68 bid/ask
        # attempt at each frozen horizon and never reacquires an entry quote.
        # It is research-only and has no broker, lifecycle or authorization
        # dependency.
        $managed += Start-ManagedProcess `
            -Name "official_event_quote_horizon_capture_v1" `
            -Needle "oanda_official_event_quote_horizon_capture_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_quote_horizon_capture_v1.py"),
                "--interval-sec", "5",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_quote_horizon_capture_heartbeat_v1.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "official_event_quote_horizon_capture_v1_all68_append_only_20260830"
            }
        # Preserve a second, explicitly separate market clock for scheduled
        # releases.  This closes the magnitude-evidence gap when a direct
        # publisher surface is blocked or its statement arrives after the
        # known release minute.  Calendar clocks assign no direction and this
        # append-only worker has no broker, lifecycle or authorization access.
        $managed += Start-ManagedProcess `
            -Name "scheduled_event_quote_capture_v1" `
            -Needle "oanda_scheduled_event_quote_capture_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_scheduled_event_quote_capture_v1.py"),
                "--interval-sec", "2",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\scheduled_event_quote_capture_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
                MaxProgressAgeSec = 60
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "scheduled_event_quote_capture_v1_all68_prospective_20260902"
            }
        # V1 preserves broker tick clocks but its first live event proved that
        # an unchanged thin-pair tick can invalidate otherwise current event
        # evidence. V2 is a separate prospective cohort: one read-only OANDA
        # Practice pricing GET confirms the complete current 68-row surface,
        # preserves every broker tick time as a diagnostic, and counts only
        # explicitly tradeable rows as executable proof. It has no order,
        # lifecycle, promotion or authorization method.
        $managed += Start-ManagedProcess `
            -Name "scheduled_event_quote_capture_v2" `
            -Needle "oanda_scheduled_event_quote_capture_v2.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_scheduled_event_quote_capture_v2.py"),
                "--creds", (Join-Path $Trad "creds"),
                "--account-key", $AccountKey,
                "--interval-sec", "2",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\scheduled_event_quote_capture_heartbeat_v2.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
                MaxProgressAgeSec = 60
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "scheduled_event_quote_capture_v2_oanda_rest_current_snapshot_20260902"
            }
        # Convert each future scheduled-event clock into one prospective,
        # research-only continuation hypothesis only after the +1 minute
        # cross-pair currency factor is available. The clock itself assigns no
        # direction; the worker cannot trade, authorize, promote or mutate the
        # Practice-007 lifecycle.
        $managed += Start-ManagedProcess `
            -Name "scheduled_event_factor_reaction_v1" `
            -Needle "oanda_scheduled_event_factor_reaction_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_scheduled_event_factor_reaction_v1.py"),
                "--config", (Join-Path $Trad "config\scheduled_event_factor_reaction_v1_20260902b.json"),
                "--interval-sec", "2",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "scheduled_event_factor_reaction_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
                MaxProgressAgeSec = 60
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "scheduled_event_factor_reaction_v1"
            }
        # Independently reconstruct the source lineage, due-horizon grid,
        # exact quote arithmetic and fail-closed safety state. This process
        # imports no producer or broker code and only writes verifier health.
        $managed += Start-ManagedProcess `
            -Name "official_event_quote_horizon_capture_verifier_v1" `
            -Needle "oanda_official_event_quote_horizon_capture_v1_verifier.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_quote_horizon_capture_v1_verifier.py"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_quote_horizon_capture_verifier_heartbeat_v1.json")
                MaxAgeSec = 180
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ExpectedJsonField = "status"
                ExpectedJsonValue = "verified"
            }
        # Seal one outcome-blind official-event decision (or explicit
        # abstention) before H1, then compare the frozen official, price-only,
        # confirmed, flipped and no-trade arms on the exact Horizon V1 rows.
        # The event-T0 entry is a research counterfactual clock, not an order;
        # this worker has no broker, lifecycle or authorization surface.
        $managed += Start-ManagedProcess `
            -Name "official_event_paired_evaluator_v1" `
            -Needle "oanda_official_event_paired_evaluator_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_paired_evaluator_v1.py"),
                "--interval-sec", "5",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_paired_evaluator_heartbeat_v1_20260830b.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ExpectedJsonField = "cohort_id"
                ExpectedJsonValue = "official_event_paired_evaluator_v1_20260830b"
            }
        # Verify the decision-time seal, issuer-only pair binding, exact
        # Cartesian schedule, upstream bytes, executable math and append-only
        # boundary without importing the paired producer.
        $managed += Start-ManagedProcess `
            -Name "official_event_paired_evaluator_verifier_v1" `
            -Needle "oanda_official_event_paired_evaluator_v1_verifier.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_event_paired_evaluator_v1_verifier.py"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_event_paired_evaluator_verifier_heartbeat_v1_20260830b.json")
                MaxAgeSec = 180
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ExpectedJsonField = "cohort_id"
                ExpectedJsonValue = "official_event_paired_evaluator_v1_20260830b"
            }
        # Collect only exact, source-specific monetary-policy decision facts.
        # The first adapter is the separately reviewed SARB parser and remains
        # research-only: incomplete facts abstain, generic vote regexes are
        # forbidden, and the worker has no broker/authorization surface.
        $managed += Start-ManagedProcess `
            -Name "official_policy_decision_fact" `
            -Needle "oanda_official_policy_decision_fact_v1.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_policy_decision_fact_v1.py"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "official_policy_decision_fact_heartbeat_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 180
                MaxProgressAgeSec = 900
            }
        # Convert only mapper-certified prospective official observations into
        # immutable 1/5/15/30/60/120-minute executable-quote response watches. Technical
        # direction is retained as an aligned/conflicted diagnostic; this
        # worker cannot mutate the canonical watchlist or contact a broker.
        $managed += Start-ManagedProcess `
            -Name "official_release_fast_response_watch" `
            -Needle "oanda_official_release_fast_response_watch.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_release_fast_response_watch.py"),
                "--interval-sec", "5",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment\official_release_fast_response_watch_heartbeat_v3.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
                MaxProgressAgeSec = 180
                ProgressPhases = @("loading_inputs")
                ExpectedJsonField = "required_classification_version"
                ExpectedJsonValue = "local_fx_news_rules_20260904_v164_conflict_duration_recap_guard"
            }
        # V1/V2 are sealed cohorts, V3 preserves the CBRT contamination, V4 is
        # the immutable V149 relevance-gated cohort, V5 is the immutable V150
        # pair-extreme cohort, and V6 is the immutable V151 pair-breakout
        # boundary. V7 preserves the V152 subject-binding baseline and V8 is a
        # parallel exact-source BOJ market-structure taxonomy overlay.
        # No prior rows move. Historical cutover: v6_v151_pair_breakout_recap_cutover.
        $legacyCausalSourceMapV1 = @(
            Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v1.py"
        )
        if ($legacyCausalSourceMapV1.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v1_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v1.py" `
                -Processes $legacyCausalSourceMapV1 `
                -Reason "v7_v152_subject_bound_release_policy_targets_cutover"
        }
        $legacyCausalSourceMapV2 = @(
            Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v2.py"
        )
        if ($legacyCausalSourceMapV2.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v2_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v2.py" `
                -Processes $legacyCausalSourceMapV2 `
                -Reason "v7_v152_subject_bound_release_policy_targets_cutover"
        }
        $legacyCausalSourceMapV3 = @(
            Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v3.py"
        )
        if ($legacyCausalSourceMapV3.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v3_contaminated_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v3.py" `
                -Processes $legacyCausalSourceMapV3 `
                -Reason "v7_v152_subject_bound_release_policy_targets_cutover"
        }
        $legacyCausalSourceMapV4 = @(
            Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v4.py"
        )
        if ($legacyCausalSourceMapV4.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v4_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v4.py" `
                -Processes $legacyCausalSourceMapV4 `
                -Reason "v7_v152_subject_bound_release_policy_targets_cutover"
        }
        $legacyCausalSourceMapV5 = @(
            Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v5.py"
        )
        if ($legacyCausalSourceMapV5.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v5_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v5.py" `
                -Processes $legacyCausalSourceMapV5 `
                -Reason "v7_v152_subject_bound_release_policy_targets_cutover"
        }
        $legacyCausalSourceMapV6 = @(
            Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v6.py"
        )
        if ($legacyCausalSourceMapV6.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v6_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v6.py" `
                -Processes $legacyCausalSourceMapV6 `
                -Reason "v7_v152_subject_bound_release_policy_targets_cutover"
        }
        # Learn neutral, source-native factor-to-currency responses without
        # requiring an upstream semantic direction.  Effective evidence is
        # deduplicated by the underlying market episode, entries are exact
        # point-in-time bid/ask snapshots, and every output remains research-
        # only with no lifecycle, authorization, promotion, or broker surface.
        # V7/V8 are frozen diagnostics after the publication-availability audit.
        # Preserve the old ledger as diagnostics; future collection uses a new contract.
        $retiredPublicationWorker = @(Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v7.py")
        if ($retiredPublicationWorker.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v7_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v7.py" `
                -Processes $retiredPublicationWorker `
                -Reason "publication_availability_contract_20260905"
        }
        # Preserve the old ledger as diagnostics; future collection uses a new contract.
        $retiredPublicationWorker = @(Get-MatchingPython -Needle "oanda_causal_source_factor_response_map_v8.py")
        if ($retiredPublicationWorker.Count -gt 0) {
            Stop-MatchingPython `
                -Name "causal_source_factor_response_map_v8_preserved" `
                -Needle "oanda_causal_source_factor_response_map_v8.py" `
                -Processes $retiredPublicationWorker `
                -Reason "publication_availability_contract_20260905"
        }
        # Compare source-only, price-only, and source-plus-price timing as
        # isolated counterfactuals. This adapter consumes only proof-eligible
        # V9 source forecasts and cannot route or authorize.
        $legacySourceConditionedRankV1 = @(
            Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v1.py"
        )
        if ($legacySourceConditionedRankV1.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v1_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v1.py" `
                -Processes $legacySourceConditionedRankV1 `
                -Reason "v5_source_input_adapter_cutover"
        }
        $legacySourceConditionedRankV2 = @(
            Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v2.py"
        )
        if ($legacySourceConditionedRankV2.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v2_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v2.py" `
                -Processes $legacySourceConditionedRankV2 `
                -Reason "v5_source_input_adapter_cutover"
        }
        $legacySourceConditionedRankV3 = @(
            Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v3.py"
        )
        if ($legacySourceConditionedRankV3.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v3_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v3.py" `
                -Processes $legacySourceConditionedRankV3 `
                -Reason "v5_source_input_adapter_cutover"
        }
        $legacySourceConditionedRankV4 = @(
            Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v4.py"
        )
        if ($legacySourceConditionedRankV4.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v4_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v4.py" `
                -Processes $legacySourceConditionedRankV4 `
                -Reason "v6_source_input_adapter_cutover"
        }
        $legacySourceConditionedRankV5 = @(
            Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v5.py"
        )
        if ($legacySourceConditionedRankV5.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v5_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v5.py" `
                -Processes $legacySourceConditionedRankV5 `
                -Reason "v7_source_input_adapter_cutover"
        }
        # V6/V7 rank ledgers remain frozen diagnostics with their original clocks.
        # Preserve the old ledger as diagnostics; future collection uses a new contract.
        $retiredPublicationWorker = @(Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v6.py")
        if ($retiredPublicationWorker.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v6_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v6.py" `
                -Processes $retiredPublicationWorker `
                -Reason "publication_availability_contract_20260905"
        }
        # Preserve the old ledger as diagnostics; future collection uses a new contract.
        $retiredPublicationWorker = @(Get-MatchingPython -Needle "oanda_source_conditioned_currency_rank_v7.py")
        if ($retiredPublicationWorker.Count -gt 0) {
            Stop-MatchingPython `
                -Name "source_conditioned_currency_rank_v7_preserved" `
                -Needle "oanda_source_conditioned_currency_rank_v7.py" `
                -Processes $retiredPublicationWorker `
                -Reason "publication_availability_contract_20260905"
        }
        # These manifests ship disabled. An enabled source must separately hold
        # its hash-bound future activation receipt; neither CLI creates one.
        $publicationSourceConfig = Read-JsonFileWithRetry -LiteralPath (Join-Path $Trad "config\source_factor_response_v9.json")
        $publicationRankConfig = Read-JsonFileWithRetry -LiteralPath (Join-Path $Trad "config\source_conditioned_currency_rank_v8.json")
        if ($publicationSourceConfig.collection_enabled -eq $true) {
            $managed += Start-ManagedProcess `
                -Name "causal_source_factor_response_map_v9" `
                -Needle "oanda_causal_source_factor_response_map_v9.py" `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_causal_source_factor_response_map_v9.py"),
                    "--config", (Join-Path $Trad "config\source_factor_response_v9.json"),
                    "--interval-sec", "5", "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $DataRoot "local_news_sentiment\causal_source_factor_response_map_latest_v9.json")
                    MaxAgeSec = 180
                    StartupGraceSec = 600
                    ExpectedJsonField = "contract_id"
                    ExpectedJsonValue = "causal_source_factor_response_map_v9_publication_availability_20260905"
                }
        }
        if (($publicationSourceConfig.collection_enabled -eq $true) -and ($publicationRankConfig.collection_enabled -eq $true)) {
            $managed += Start-ManagedProcess `
                -Name "source_conditioned_currency_rank_v8" `
                -Needle "oanda_source_conditioned_currency_rank_v8.py" `
                -StartupDelaySec 10 `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_source_conditioned_currency_rank_v8.py"),
                    "--config", (Join-Path $Trad "config\source_conditioned_currency_rank_v8.json"),
                    "--interval-sec", "5", "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $State "source_conditioned_currency_rank_v8.json")
                    MaxAgeSec = 180
                    StartupGraceSec = 600
                    ExpectedJsonField = "contract_id"
                    ExpectedJsonValue = "source_conditioned_currency_rank_v8_observed_publication_entry_20260905"
                }
        }
        # Test whether already-known news direction becomes economically useful
        # only after a causal frozen level resolves and completed quote-change
        # intensity agrees.  All arms remain immutable research counterfactuals.
        $managed += Start-ManagedProcess `
            -Name "news_band_resolution_flow_h15" `
            -Needle "oanda_news_band_resolution_flow_h15.py" `
            -StartupDelaySec 15 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_news_band_resolution_flow_h15.py"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "news_band_resolution_flow_h15_heartbeat_v1.json")
                MaxAgeSec = 180
                StartupGraceSec = 600
            }
        # V11 is a frozen evidence cohort.  At the supervised V12 cutover,
        # stop only its research meter worker so the legacy database can no
        # longer acquire future/incomplete bucket rows.
        $legacyNarrativeMeters = @(
            Get-MatchingPython -Needle "oanda_continuous_narrative_meter.py"
        )
        if ($legacyNarrativeMeters.Count -gt 0) {
            Stop-MatchingPython `
                -Name "continuous_narrative_meter_v11_frozen" `
                -Needle "oanda_continuous_narrative_meter.py" `
                -Processes $legacyNarrativeMeters `
                -Reason "v12_sealed_contract_cutover"
        }
        $managed += Start-ManagedProcess `
            -Name "continuous_narrative_meter" `
            -Needle "oanda_continuous_narrative_meter_v12.py" `
            -StartupDelaySec 15 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_continuous_narrative_meter_v12.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "continuous_narrative_meter_v12.json")
                MaxAgeSec = 240
                StartupGraceSec = 360
            }
        $teApiKey = [Environment]::GetEnvironmentVariable("TRADING_ECONOMICS_API_KEY")
        if ([string]::IsNullOrWhiteSpace($teApiKey)) {
            $teApiKey = [Environment]::GetEnvironmentVariable("TRADING_ECONOMICS_API_KEY", "User")
            if (-not [string]::IsNullOrWhiteSpace($teApiKey)) {
                [Environment]::SetEnvironmentVariable("TRADING_ECONOMICS_API_KEY", $teApiKey, "Process")
            }
        }
        if ([string]::IsNullOrWhiteSpace($teApiKey)) {
            $teApiKey = [Environment]::GetEnvironmentVariable("TE_API_KEY")
        }
        if (-not [string]::IsNullOrWhiteSpace($teApiKey)) {
            $managed += Start-ManagedProcess `
                -Name "macro_consensus_prospective" `
                -Needle "oanda_macro_consensus_prospective.py" `
                -StartupDelaySec 15 `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_macro_consensus_prospective.py"),
                    "--interval-sec", "7200",
                    "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $State "macro_consensus_prospective_v1.json")
                    MaxAgeSec = 9000
                    StartupGraceSec = 300
                }
        } else {
            $existingConsensus = @(Get-MatchingPython -Needle "oanda_macro_consensus_prospective.py")
            if ($existingConsensus.Count -gt 0) {
                Stop-MatchingPython -Name "macro_consensus_prospective" -Needle "oanda_macro_consensus_prospective.py" -Processes $existingConsensus -Reason "missing_credential"
            }
            $managed += @{
                name = "macro_consensus_prospective"
                running = $false
                started = $false
                pids = @()
                freshness = @{
                    fresh = $true
                    age_sec = $null
                    path = (Join-Path $State "macro_consensus_prospective_v1.json")
                    reason = "missing_credential"
                }
            }
        }
        $managed += Start-ManagedProcess `
            -Name "macro_surprise_ledger" `
            -Needle "oanda_macro_surprise_ledger.py" `
            -StartupDelaySec 30 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_macro_surprise_ledger.py"),
                "--news-database", (Join-Path $DataRoot "local_news_sentiment\local_news_sentiment_v1.sqlite"),
                "--ledger-database", (Join-Path $State "macro_surprise_v1.sqlite"),
                "--state", (Join-Path $State "macro_surprise_v1.json"),
                "--heartbeat", (Join-Path $State "macro_surprise_heartbeat_v1.json"),
                "--consensus-jsonl", (Join-Path $State "macro_consensus_import_v1.jsonl"),
                "--consensus-archive", (Join-Path $DataRoot "source_archives\macro_consensus_prospective_v1"),
                "--quote-snapshot", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "macro_surprise_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 120
                MaxProgressAgeSec = 600
                ProgressPhases = @(
                    "ingesting",
                    "opening_databases",
                    "querying_structured_candidates",
                    "processing_structured_candidates"
                )
            }
        $managed += Start-ManagedProcess `
            -Name "cftc_positioning_shadow" `
            -Needle "oanda_cftc_positioning_shadow.py" `
            -StartupDelaySec 45 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_cftc_positioning_shadow.py"),
                "--output", (Join-Path $State "cftc_currency_positioning_shadow_v1.json"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "cftc_currency_positioning_shadow_v1.json")
                MaxAgeSec = 25200
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "us_treasury_yield_prospective" `
            -Needle "oanda_us_treasury_yield_prospective.py" `
            -StartupDelaySec 75 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_us_treasury_yield_prospective.py"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "us_treasury_yield_prospective_v1.json")
                MaxAgeSec = 25200
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "official_daily_rate_context" `
            -Needle "oanda_official_daily_rate_context.py" `
            -StartupDelaySec 85 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_daily_rate_context.py"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "official_daily_rate_context_v1.json")
                MaxAgeSec = 25200
                StartupGraceSec = 900
            }
        $managed += Start-ManagedProcess `
            -Name "official_currency_source_depth" `
            -Needle "oanda_official_currency_source_depth.py" `
            -StartupDelaySec 85 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_currency_source_depth.py"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "reports\official_currency_source_depth\OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json")
                MaxAgeSec = 25200
                StartupGraceSec = 900
            }
        # V2 separates configured transports from exact family parsers,
        # numeric facts, prospective causal inputs, matured outcomes, and
        # semantic direction.  It is a read-only readiness census and cannot
        # authorize, promote, or execute anything.
        $managed += Start-ManagedProcess `
            -Name "official_currency_source_depth_readiness_v2" `
            -Needle "oanda_official_currency_source_depth_readiness_v2.py" `
            -StartupDelaySec 90 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_currency_source_depth_readiness_v2.py"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "reports\official_currency_source_depth_readiness_v2\OFFICIAL_CURRENCY_SOURCE_DEPTH_READINESS_V2_CURRENT.json")
                MaxAgeSec = 25200
                StartupGraceSec = 900
            }
        $fredApiKey = [Environment]::GetEnvironmentVariable("FRED_API_KEY")
        if ([string]::IsNullOrWhiteSpace($fredApiKey)) {
            $fredApiKey = [Environment]::GetEnvironmentVariable("FRED_API_KEY", "User")
            if (-not [string]::IsNullOrWhiteSpace($fredApiKey)) {
                [Environment]::SetEnvironmentVariable("FRED_API_KEY", $fredApiKey, "Process")
            }
        }
        if ($fredApiKey -match '^[a-z0-9]{32}$') {
            $managed += Start-ManagedProcess `
                -Name "alfred_vintage_prospective" `
                -Needle "oanda_alfred_vintage_prospective.py" `
                -StartupDelaySec 90 `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_alfred_vintage_prospective.py"),
                    "--interval-sec", "21600",
                    "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $State "alfred_vintage_prospective_v1.json")
                    MaxAgeSec = 25200
                    StartupGraceSec = 600
                }
            $managed += Start-ManagedProcess `
                -Name "alfred_short_rate_prospective" `
                -Needle "oanda_alfred_short_rate_prospective.py" `
                -StartupDelaySec 95 `
                -PriorityClass "BelowNormal" `
                -Arguments @(
                    (Join-Path $Trad "oanda_alfred_short_rate_prospective.py"),
                    "--interval-sec", "21600",
                    "--duration-sec", "$ChildDurationSec"
                ) `
                -Freshness @{
                    LiteralPath = (Join-Path $State "alfred_short_rate_prospective_v1.json")
                    MaxAgeSec = 25200
                    StartupGraceSec = 600
                }
        } else {
            $existingAlfred = @(Get-MatchingPython -Needle "oanda_alfred_vintage_prospective.py")
            if ($existingAlfred.Count -gt 0) {
                Stop-MatchingPython -Name "alfred_vintage_prospective" -Needle "oanda_alfred_vintage_prospective.py" -Processes $existingAlfred -Reason "missing_credential"
            }
            $managed += @{
                name = "alfred_vintage_prospective"
                running = $false
                started = $false
                pids = @()
                freshness = @{
                    fresh = $true
                    age_sec = $null
                    path = (Join-Path $State "alfred_vintage_prospective_v1.json")
                    reason = "missing_credential"
                }
            }
            $existingAlfredRates = @(Get-MatchingPython -Needle "oanda_alfred_short_rate_prospective.py")
            if ($existingAlfredRates.Count -gt 0) {
                Stop-MatchingPython -Name "alfred_short_rate_prospective" -Needle "oanda_alfred_short_rate_prospective.py" -Processes $existingAlfredRates -Reason "missing_credential"
            }
            $managed += @{
                name = "alfred_short_rate_prospective"
                running = $false
                started = $false
                pids = @()
                freshness = @{
                    fresh = $true
                    age_sec = $null
                    path = (Join-Path $State "alfred_short_rate_prospective_v1.json")
                    reason = "missing_credential"
                }
            }
        }
        $managed += Start-ManagedProcess `
            -Name "signal_news_monitor" `
            -Needle "oanda_signal_news_monitor.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_news_monitor.py"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "reports\practice_007_signal_news_monitor\LATEST.json")
                MaxAgeSec = 180
                StartupGraceSec = 300
            }
        $managed += Start-ManagedProcess `
            -Name "news_technical_watchlist" `
            -Needle "oanda_news_technical_watchlist.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_news_technical_watchlist.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "news_technical_watchlist_v1.json")
                MaxAgeSec = 240
                StartupGraceSec = 360
                ExpectedJsonField = "required_news_classification_version"
                ExpectedJsonValue = "local_fx_news_rules_20260904_v164_conflict_duration_recap_guard"
            }
        $managed += Start-ManagedProcess `
            -Name "event_technical_preflight" `
            -Needle "oanda_event_technical_preflight.py" `
            -StartupDelaySec 120 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_event_technical_preflight.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "event_technical_preflight_v1.json")
                MaxAgeSec = 240
                StartupGraceSec = 360
            }
        $managed += Start-ManagedProcess `
            -Name "direct_source_response" `
            -Needle "oanda_direct_source_response.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_direct_source_response.py"),
                "--progress-heartbeat", (Join-Path $State "direct_source_response_progress_heartbeat_v1.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "direct_source_response_progress_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 120
                MaxProgressAgeSec = 900
                ProgressPhases = @(
                    "starting_cycle",
                    "loading_inputs",
                    "opening_response_database",
                    "ingesting_macro",
                    "invalidating_legacy_rows",
                    "ingesting_daily_rates",
                    "maturing_targets",
                    "summarizing",
                    "publishing_cycle",
                    "cycle_complete"
                )
            }
        $managed += Start-ManagedProcess `
            -Name "internal_macro_expectation" `
            -Needle "oanda_internal_macro_expectation.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_internal_macro_expectation.py"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "internal_macro_expectation_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "policy_statement_breakout_research" `
            -Needle "oanda_policy_statement_breakout_research.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_policy_statement_breakout_research.py"),
                "--interval-sec", "30",
                "--heartbeat", (Join-Path $State "policy_statement_breakout_research_heartbeat_v1.json"),
                "--heartbeat-sec", "15",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                # The projection can take many minutes as immutable history
                # grows. Supervise its separate liveness pulse, not result age.
                LiteralPath = (Join-Path $State "policy_statement_breakout_research_heartbeat_v1.json")
                MaxAgeSec = 60
                StartupGraceSec = 360
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "policy_statement_breakout_research_heartbeat_v1"
            }
          $managed += Start-ManagedProcess `
             -Name "macro_release_breakout_research" `
            -Needle "oanda_macro_release_breakout_research.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_macro_release_breakout_research.py"),
                "--heartbeat", (Join-Path $State "macro_release_breakout_research_heartbeat_v1.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "macro_release_breakout_research_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 120
                MaxProgressAgeSec = 600
                ProgressPhases = @(
                    "querying_bls_candidates",
                    "processing_bls_candidates",
                    "evaluating_events",
                    "maturing_entries"
                )
             }
          $managed += Start-ManagedProcess `
              -Name "currency_macro_release_breakout_research" `
              -Needle "oanda_currency_macro_release_breakout_research.py" `
              -PriorityClass "BelowNormal" `
              -Arguments @(
                  (Join-Path $Trad "oanda_currency_macro_release_breakout_research.py"),
                  "--heartbeat", (Join-Path $State "currency_macro_release_breakout_research_heartbeat_v1.json"),
                  "--interval-sec", "30",
                  "--duration-sec", "$ChildDurationSec"
              ) `
              -Freshness @{
                  LiteralPath = (Join-Path $State "currency_macro_release_breakout_research_heartbeat_v1.json")
                  MaxAgeSec = 30
                  StartupGraceSec = 120
                  MaxProgressAgeSec = 600
                  ProgressPhases = @(
                      "querying_currency_macro_candidates",
                      "processing_currency_macro_candidates",
                      "evaluating_currency_events",
                      "maturing_entries"
                  )
              }
          $managed += Start-ManagedProcess `
              -Name "official_rate_relative_strength_prospective" `
              -Needle "oanda_rate_relative_strength_prospective.py" `
              -PriorityClass "BelowNormal" `
              -Arguments @(
                  (Join-Path $Trad "oanda_rate_relative_strength_prospective.py"),
                  "--interval-sec", "30",
                  "--duration-sec", "$ChildDurationSec"
              ) `
              -Freshness @{
                  LiteralPath = (Join-Path $State "rate_relative_strength_prospective_v1.json")
                  MaxAgeSec = 180
                  StartupGraceSec = 360
              }
          $managed += Start-ManagedProcess `
              -Name "direct_cpi_acceleration_prospective" `
              -Needle "oanda_direct_cpi_acceleration_prospective.py" `
              -PriorityClass "BelowNormal" `
              -Arguments @(
                  (Join-Path $Trad "oanda_direct_cpi_acceleration_prospective.py"),
                  "--interval-sec", "60",
                  "--duration-sec", "$ChildDurationSec"
              ) `
              -Freshness @{
                  LiteralPath = (Join-Path $State "direct_cpi_acceleration_prospective_v1.json")
                  MaxAgeSec = 240
                  StartupGraceSec = 360
              }
          $managed += Start-ManagedProcess `
              -Name "alfred_unemployment_relative_prospective" `
              -Needle "oanda_alfred_unemployment_relative_prospective.py" `
              -PriorityClass "BelowNormal" `
              -Arguments @(
                  (Join-Path $Trad "oanda_alfred_unemployment_relative_prospective.py"),
                  "--heartbeat-state", (Join-Path $State "alfred_unemployment_relative_prospective_heartbeat_v1.json"),
                  "--interval-sec", "60",
                  "--duration-sec", "$ChildDurationSec"
              ) `
              -Freshness @{
                  LiteralPath = (Join-Path $State "alfred_unemployment_relative_prospective_heartbeat_v1.json")
                  MaxAgeSec = 120
                  StartupGraceSec = 360
                  MaxPhaseAgeSec = 1200
                  WatchedPhases = @("running_cycle")
              }
          $managed += Start-ManagedProcess `
              -Name "alfred_short_rate_relative_prospective" `
              -Needle "oanda_alfred_short_rate_relative_prospective.py" `
              -PriorityClass "BelowNormal" `
              -Arguments @(
                  (Join-Path $Trad "oanda_alfred_short_rate_relative_prospective.py"),
                  "--interval-sec", "60",
                  "--duration-sec", "$ChildDurationSec"
              ) `
              -Freshness @{
                  LiteralPath = (Join-Path $State "alfred_short_rate_relative_prospective_v1.json")
                  MaxAgeSec = 240
                  StartupGraceSec = 360
              }
          $managed += Start-ManagedProcess `
            -Name "executable_opportunity_proof" `
            -Needle "oanda_executable_opportunity_prospective.py" `
            -PriorityClass "Normal" `
            -Arguments @(
                (Join-Path $Trad "oanda_executable_opportunity_prospective.py"),
                "--interval-sec", "30",
                "--mature-only",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "executable_opportunity_prospective_v1.json")
                MaxAgeSec = 300
                StartupGraceSec = 900
            }
        $managed += Start-ManagedProcess `
            -Name "gdelt_attention_magnitude_proof" `
            -Needle "oanda_gdelt_attention_prospective.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_gdelt_attention_prospective.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "gdelt_attention_magnitude_prospective_v1.json")
                MaxAgeSec = 240
                StartupGraceSec = 360
            }
        $managed += Start-ManagedProcess `
            -Name "signal_feed_wal_maintenance" `
            -Needle "oanda_signal_feed_wal_maintenance.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_feed_wal_maintenance.py"),
                "--database", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--state", (Join-Path $State "practice_007_signal_feed_wal_maintenance_v1.json"),
                "--interval-sec", "30",
                "--threshold-bytes", "33554432",
                "--urgent-threshold-bytes", "134217728",
                "--minimum-checkpoint-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_signal_feed_wal_maintenance_v1.json")
                MaxAgeSec = 180
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "market_sentiment_ticker" `
            -Needle "oanda_market_sentiment_ticker.py" `
            -Arguments @(
                (Join-Path $Trad "oanda_market_sentiment_ticker.py"),
                "--source", "quotes",
                "--history-minutes", "1500",
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "market_sentiment_ticker\LATEST.json")
                MaxAgeSec = 180
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "all68_m1_forward_archive" `
            -Needle "oanda_all68_m1_forward_updater.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_all68_m1_forward_updater.py"),
                "--bootstrap-all-priced",
                "--max-requests-per-pair", "1",
                "--backfill-requests-per-pair", "0",
                "--batch-size", "5000",
                "--pause-seconds", "0.10",
                # Current-bar freshness is the live research dependency.  A
                # bounded gap scan is retained, but only every tenth pass so
                # it cannot turn every refresh into an extra 68 REST calls.
                "--gap-recovery-every-cycles", "10",
                "--report", (Join-Path $State "all68_m1_forward_update_v1.json"),
                "--heartbeat", (Join-Path $State "all68_m1_forward_update_heartbeat_v1.json"),
                # A current-only all-68 pass is measured in under two minutes.
                # Re-run at two minutes; completed M1 bars remain an observed
                # source and are never fabricated across broker omissions.
                "--interval-sec", "120",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "all68_m1_forward_update_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 120
                MaxProgressAgeSec = 180
                ProgressPhases = @("updating_pairs")
            }
        # Isolated research-only support/resistance observer.  It consumes the
        # completed all-68 BAM M1 archive and fresh read-only quote snapshot,
        # freezes adaptive bands before a future M5 entry exists, and writes no
        # signal-feed, lifecycle, authorization, broker, or execution state.
        $managed += Start-ManagedProcess `
            -Name "causal_level_band_prospective" `
            -Needle "oanda_causal_level_band_prospective.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_causal_level_band_prospective.py"),
                "--update-report", (Join-Path $State "all68_m1_forward_update_v1.json"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--clock", (Join-Path $State "clock_integrity_v1.json"),
                "--ledger", (Join-Path $State "causal_level_band_prospective_v1.sqlite"),
                "--runtime", (Join-Path $State "causal_level_band_runtime_v1.json"),
                "--output", (Join-Path $State "causal_level_band_prospective_v1.json"),
                "--heartbeat", (Join-Path $State "causal_level_band_prospective_heartbeat_v1.json"),
                "--progress-heartbeat", (Join-Path $State "causal_level_band_prospective_progress_heartbeat_v1.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "causal_level_band_prospective_progress_heartbeat_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 120
                MaxProgressAgeSec = 1200
                ProgressPhases = @(
                    "starting_cycle",
                    "loading_inputs",
                    "refreshing_contexts",
                    "recording_forecasts",
                    "maturing_outcomes",
                    "summarizing_ledger",
                    "publishing_cycle",
                    "cycle_complete"
                )
            }
        $managed += Start-ManagedProcess `
            -Name "practice_007_latest_moves" `
            -Needle "oanda_latest_moves.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_latest_moves.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_latest_moves_v1.json")
                MaxAgeSec = 240
                StartupGraceSec = 300
            }
        # V5 joined one latest narrative JSON state to movers whose starts
        # could be earlier. V6R2 repaired the causal V12 start-clock join.
        # V7R3 retains that join, freezes the five-minute onset factor, and
        # represents later same-primary interval bridges with append-only
        # transitive root aliases. Freeze every older chain; V7R3/V4R3/V5R3
        # remain research-only and have no broker or authorization surface.
        $legacyMoveNews = @(Get-MatchingPython -Needle "oanda_live_move_news_snapshot.py")
        if ($legacyMoveNews.Count -gt 0) {
            Stop-MatchingPython -Name "live_move_news_snapshot_v5_frozen" -Needle "oanda_live_move_news_snapshot.py" -Processes $legacyMoveNews -Reason "v6_v12_start_clock_cutover"
        }
        $legacyMoveOutcomes = @(Get-MatchingPython -Needle "oanda_live_move_news_outcomes.py")
        if ($legacyMoveOutcomes.Count -gt 0) {
            Stop-MatchingPython -Name "live_move_news_outcomes_v1_frozen" -Needle "oanda_live_move_news_outcomes.py" -Processes $legacyMoveOutcomes -Reason "v2_v12_start_clock_cutover"
        }
        $legacyPersistentContext = @(Get-MatchingPython -Needle "oanda_live_move_persistent_news_context.py")
        if ($legacyPersistentContext.Count -gt 0) {
            Stop-MatchingPython -Name "live_move_persistent_news_context_v2_frozen" -Needle "oanda_live_move_persistent_news_context.py" -Processes $legacyPersistentContext -Reason "v3_v12_start_clock_cutover"
        }
        $frozenV6MoveNews = @(Get-MatchingPython -Needle "oanda_live_move_news_snapshot_v6.py")
        if ($frozenV6MoveNews.Count -gt 0) {
            Stop-MatchingPython -Name "live_move_news_snapshot_v6r2_frozen" -Needle "oanda_live_move_news_snapshot_v6.py" -Processes $frozenV6MoveNews -Reason "v7r3_transitive_root_union_cutover"
        }
        $frozenV2MoveOutcomes = @(Get-MatchingPython -Needle "oanda_live_move_news_outcomes_v2.py")
        if ($frozenV2MoveOutcomes.Count -gt 0) {
            Stop-MatchingPython -Name "live_move_news_outcomes_v2r2_frozen" -Needle "oanda_live_move_news_outcomes_v2.py" -Processes $frozenV2MoveOutcomes -Reason "v4r3_transitive_root_union_cutover"
        }
        $frozenV3PersistentContext = @(Get-MatchingPython -Needle "oanda_live_move_persistent_news_context_v3.py")
        if ($frozenV3PersistentContext.Count -gt 0) {
            Stop-MatchingPython -Name "live_move_persistent_news_context_v3r2_frozen" -Needle "oanda_live_move_persistent_news_context_v3.py" -Processes $frozenV3PersistentContext -Reason "v5r3_transitive_root_union_cutover"
        }
        $managed += Start-ManagedProcess `
            -Name "live_move_news_snapshot" `
            -Needle "oanda_live_move_news_snapshot_v7r3.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_live_move_news_snapshot_v7r3.py"),
                "--narrative-database", (Join-Path $State "continuous_narrative_meter_v12.sqlite"),
                "--interval-sec", "120",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "live_move_news_snapshot_v7r3.json")
                MaxAgeSec = 360
                StartupGraceSec = 300
            }
        # Mature only the quote-bound research arms produced by the live
        # movement/news diagnostic. Outcomes use later executable bid/ask M1
        # paths and remain permanently disconnected from broker routing.
        $managed += Start-ManagedProcess `
            -Name "live_move_news_outcomes" `
            -Needle "oanda_live_move_news_outcomes_v4.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_live_move_news_outcomes_v4.py"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "live_move_news_outcomes_v4r3.json")
                MaxAgeSec = 600
                StartupGraceSec = 600
            }
        # News outcome diagnoses are an on-demand append-only project record.
        # They are intentionally not a recurring worker: a future review reads
        # the immutable ledgers and appends only evidence not already recorded.
        # Preserve still-active policy/geopolitical factors for their frozen
        # declared research horizon. This companion diagnostic never changes
        # the narrow causal mover vote and cannot route or authorize trades.
        $managed += Start-ManagedProcess `
            -Name "live_move_persistent_news_context" `
            -Needle "oanda_live_move_persistent_news_context_v5.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_live_move_persistent_news_context_v5.py"),
                "--interval-sec", "120",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "live_move_persistent_news_context_v5r3.json")
                MaxAgeSec = 360
                StartupGraceSec = 300
            }
        $managed += Start-ManagedProcess `
            -Name "crypto_shadow" `
            -Needle "crypto_shadow_live_tracker.py" `
            -Arguments @(
                (Join-Path $Trad "crypto_shadow_live_tracker.py"),
                "--data-dir", (Join-Path $DataRoot "crypto_shadow")
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "crypto_shadow\coinbase_shadow_state.json")
                MaxAgeSec = 30
                StartupGraceSec = 300
            }
        # Retired from always-on runtime on 2026-08-18.  This legacy local-only
        # Practice-002 loop had no unique evidence consumer and duplicated the
        # canonical 68-pair quote/news/move collectors.  Its code and historical
        # files remain intact for reproducibility; Practice-002 was verified flat
        # before the supervisor entry was removed.
        $managed += Start-ManagedProcess `
            -Name "practice_003_technical" `
            -Needle "oanda_technical_account_manager_auto.py" `
            -Arguments @((Join-Path $Trad "oanda_technical_account_manager_auto.py"), "--no-scan-on-launch", "--normal-mode")
        $managed += Start-ManagedProcess `
            -Name "practice_005_gpt_exp" `
            -Needle "oanda_gpt_exp_account_manager.py" `
            -Arguments @((Join-Path $Trad "oanda_gpt_exp_account_manager.py"), "--no-scan-on-launch", "--normal-mode")
        $managed += Start-ManagedProcess `
            -Name "practice_013_formula83" `
            -Needle "oanda_gpt_9h_formula83_account_manager.py" `
            -Arguments @((Join-Path $Trad "oanda_gpt_9h_formula83_account_manager.py"), "--no-scan-on-launch", "--normal-mode", "--execute")
        $managed += Start-ManagedProcess `
            -Name "arima_multiframe_sweep" `
            -Needle "oanda_arima_multiframe_sweep.py" `
            -StartupDelaySec 3600 `
            -PriorityClass "Idle" `
            -Arguments @(
                (Join-Path $Trad "oanda_arima_multiframe_sweep.py"),
                "--pairs", "majors",
                "--windows", "2",
                "--spec-limit", "4",
                "--repeat-hours", "24"
            )
        $managed += Start-ManagedProcess `
            -Name "improvement_control" `
            -Needle "oanda_improvement_control_engine.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_improvement_control_engine.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "improvement_control_engine_v1.json")
                MaxAgeSec = 240
                StartupGraceSec = 360
            }
        $managed += Start-ManagedProcess `
            -Name "continuous_improvement" `
            -Needle "oanda_continuous_improvement_loop.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 120 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_continuous_improvement_loop.py"),
                "--root", $Root,
                "--state", (Join-Path $State "continuous_improvement_loop_v1.json"),
                "--vault", (Join-Path $env:USERPROFILE "OneDrive\thevault\projects\forex"),
                "--interval-sec", "60",
                "--validation-interval-sec", "3600",
                "--vault-interval-sec", "14400",
                "--defer-initial-vault-sync",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "continuous_improvement_loop_v1.json")
                # Panel/model refresh subprocesses can legitimately run for tens of minutes.
                MaxAgeSec = 3600
                StartupGraceSec = 1800
            }
        $managed += Start-ManagedProcess `
            -Name "micro_pattern" `
            -Needle "oanda_practice_micro_pattern_lab.py" `
            -Arguments @(
                (Join-Path $Trad "oanda_practice_micro_pattern_lab.py"),
                "--creds", $ResolvedCreds,
                "--account-key", $AccountKey,
                "--duration-sec", "$ChildDurationSec",
                "--retention-hours", "72",
                "--prune-interval-sec", "3600"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "micro_pattern_dashboard_v1.json")
                MaxAgeSec = 180
                StartupGraceSec = 900
            }
        $managed += Start-ManagedProcess `
            -Name "depth_parquet" `
            -Needle "oanda_depth_parquet_collector.py" `
            -Arguments @(
                (Join-Path $Trad "oanda_depth_parquet_collector.py"),
                "--all-tradeable",
                "--interval-seconds", "1",
                "--flush-rows", "4096",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = $DepthState
                MaxAgeSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "order_position_book" `
            -Needle "oanda_order_position_book_collector.py" `
            -StartupDelaySec 30 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_order_position_book_collector.py"),
                "--all-tradeable",
                "--interval-sec", "900",
                "--request-pause-sec", "0.10",
                "--duration-sec", "$ChildDurationSec",
                "--latest-state", (Join-Path $State "oanda_order_position_book_latest_v1.json")
            ) `
            -Freshness @{
                LiteralPath = $BookState
                MaxAgeSec = 1800
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "model_predictor_auto_promotion" `
            -Needle "oanda_model_predictor_auto_promotion.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 60 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_model_predictor_auto_promotion.py"),
                "--source-ledger", (Join-Path $State "model_gap_live_forecasts_v1.sqlite"),
                "--database", (Join-Path $State "model_predictor_auto_promotion_v1.sqlite"),
                "--state", (Join-Path $State "model_predictor_auto_promotion_v1.json"),
                "--chunk-rows", "250000",
                "--max-chunks", "2",
                "--interval-sec", "300"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "model_predictor_auto_promotion_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 1800
            }
        $managed += Start-ManagedProcess `
            -Name "model_gap_live_signal" `
            -Needle "oanda_model_gap_live_signal_worker.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 90 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_model_gap_live_signal_worker.py"),
                "--feature-snapshot", (Join-Path $State "live_model_feature_snapshot_v1.json"),
                "--signal-feed", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--ledger", (Join-Path $State "model_gap_live_forecasts_v1.sqlite"),
                "--state", $ModelGapLiveState,
                "--feature-archive-root", (Join-Path $DataRoot "prospective_live_model_features"),
                "--model-root", (Join-Path $DataRoot "models\modern_model_gap"),
                "--model-report", (Join-Path $DataRoot "reports\modern_model_gap\shared_panel_model_benchmark_latest.json"),
                "--promotion-state", (Join-Path $State "model_predictor_auto_promotion_v1.json"),
                "--promotion-max-age-sec", "1800",
                "--promotion-reload-sec", "60",
                "--models", "logistic_baseline,hist_gradient_boosting,catboost,ngboost",
                "--interval-sec", "1",
                "--ttl-sec", "600",
                "--max-snapshot-age-sec", "600",
                "--max-outcome-delay-sec", "600",
                "--maturity-batch-size", "3000",
                "--summary-interval-sec", "900",
                "--prune-interval-sec", "86400",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = $ModelGapLiveState
                # Whole-ledger summaries are deferred so live scoring stays responsive.
                MaxAgeSec = 1200
                StartupGraceSec = 900
            }
        $managed += Start-ManagedProcess `
            -Name "one_hour_shadow_signal" `
            -Needle "oanda_one_hour_shadow_signal.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 120 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_one_hour_shadow_signal.py"),
                "--feature-snapshot", (Join-Path $State "live_model_feature_snapshot_v1.json"),
                "--signal-feed", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--ledger", (Join-Path $State "one_hour_shadow_forecasts_v1.sqlite"),
                "--state", (Join-Path $State "one_hour_shadow_signal_v1.json"),
                "--process-lock", (Join-Path $State "one_hour_shadow_signal_v1.lock"),
                "--interval-sec", "2",
                "--ttl-sec", "600",
                "--max-snapshot-age-sec", "600",
                "--max-outcome-delay-sec", "600",
                "--maturity-batch-size", "5000",
                "--mature-only",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "one_hour_shadow_signal_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 900
            }
        $managed += Start-ManagedProcess `
            -Name "move_alert_monitor" `
            -Needle "oanda_move_alert_monitor.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 180 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_alert_monitor.py"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--signals", (Join-Path $State "practice_007_signal_snapshot_v1.json"),
                "--signal-feed", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--heartbeat", (Join-Path $State "move_alert_monitor_v1.json"),
                "--process-lock", (Join-Path $State "move_alert_monitor_v1.lock"),
                "--interval-sec", "5",
                "--max-quote-age-sec", "90",
                "--publish-interval-sec", "60",
                "--report-interval-sec", "3600",
                "--heartbeat-interval-sec", "300",
                "--retention-days", "7",
                "--publish-terms",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "move_alert_monitor_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 900
            }
        # Cohort B is immutable evidence but its single-process hot/cold loop
        # missed capture boundaries. Preserve its files and stop only its two
        # retired runtime processes before activating the clean cohort C.
        $legacyExecutableMoveCensusB = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v1.py"
        )
        if ($legacyExecutableMoveCensusB.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_v1_invalid_preserved" `
                -Needle "oanda_executable_move_census_v1.py" `
                -Processes $legacyExecutableMoveCensusB `
                -Reason "cohort_c_cold_work_separated_cutover"
        }
        $legacyExecutableMoveVerifierB = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v1_verifier.py"
        )
        if ($legacyExecutableMoveVerifierB.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_verifier_v1_invalid_preserved" `
                -Needle "oanda_executable_move_census_v1_verifier.py" `
                -Processes $legacyExecutableMoveVerifierB `
                -Reason "cohort_c_cold_work_separated_cutover"
        }
        # Cohort C remains immutable evidence, but its frozen upstream quote
        # producer identity was superseded by the tradeability-contract fix.
        # Stop only its runtime processes; never rewrite or import its rows.
        $legacyExecutableMoveCaptureC = @(
            Get-MatchingPython -Needle "--worker-id census_capture_20260901c"
        )
        if ($legacyExecutableMoveCaptureC.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_capture_v2c_superseded" `
                -Needle "--worker-id census_capture_20260901c" `
                -Processes $legacyExecutableMoveCaptureC `
                -Reason "cohort_d_upstream_source_identity_cutover"
        }
        $legacyExecutableMoveEvaluatorC = @(
            Get-MatchingPython -Needle "--worker-id census_evaluator_20260901c"
        )
        if ($legacyExecutableMoveEvaluatorC.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_evaluator_v2c_superseded" `
                -Needle "--worker-id census_evaluator_20260901c" `
                -Processes $legacyExecutableMoveEvaluatorC `
                -Reason "cohort_d_upstream_source_identity_cutover"
        }
        $legacyExecutableMoveVerifierC = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v2_verifier.py"
        )
        if ($legacyExecutableMoveVerifierC.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_verifier_v2c_superseded" `
                -Needle "oanda_executable_move_census_v2_verifier.py" `
                -Processes $legacyExecutableMoveVerifierC `
                -Reason "cohort_d_upstream_source_identity_cutover"
        }
        # Cohort D is immutable failed evidence: it was frozen against source
        # schema 2 after the tradeability-aware quote snapshot had moved to
        # schema 3. Stop only its workers and preserve every rejected row.
        $legacyExecutableMoveCaptureD = @(
            Get-MatchingPython -Needle "--worker-id census_capture_20260901d"
        )
        if ($legacyExecutableMoveCaptureD.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_capture_v2d_invalid" `
                -Needle "--worker-id census_capture_20260901d" `
                -Processes $legacyExecutableMoveCaptureD `
                -Reason "cohort_e_source_schema_contract_cutover"
        }
        $legacyExecutableMoveEvaluatorD = @(
            Get-MatchingPython -Needle "--worker-id census_evaluator_20260901d"
        )
        if ($legacyExecutableMoveEvaluatorD.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_evaluator_v2d_invalid" `
                -Needle "--worker-id census_evaluator_20260901d" `
                -Processes $legacyExecutableMoveEvaluatorD `
                -Reason "cohort_e_source_schema_contract_cutover"
        }
        $legacyExecutableMoveVerifierD = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v2d_verifier.py"
        )
        if ($legacyExecutableMoveVerifierD.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_verifier_v2d_invalid" `
                -Needle "oanda_executable_move_census_v2d_verifier.py" `
                -Processes $legacyExecutableMoveVerifierD `
                -Reason "cohort_e_source_schema_contract_cutover"
        }
        # Cohort E remains immutable failed evidence after its open-market
        # 20:24 UTC frame was missed. Stop only its workers and never backfill
        # or relabel its surviving rows.
        $legacyExecutableMoveCaptureE = @(
            Get-MatchingPython -Needle "--worker-id census_capture_20260901e"
        )
        if ($legacyExecutableMoveCaptureE.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_capture_v2e_invalid" `
                -Needle "--worker-id census_capture_20260901e" `
                -Processes $legacyExecutableMoveCaptureE `
                -Reason "cohort_f_capture_reliability_cutover"
        }
        $legacyExecutableMoveEvaluatorE = @(
            Get-MatchingPython -Needle "--worker-id census_evaluator_20260901e"
        )
        if ($legacyExecutableMoveEvaluatorE.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_evaluator_v2e_invalid" `
                -Needle "--worker-id census_evaluator_20260901e" `
                -Processes $legacyExecutableMoveEvaluatorE `
                -Reason "cohort_f_capture_reliability_cutover"
        }
        $legacyExecutableMoveVerifierE = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v2e_verifier.py"
        )
        if ($legacyExecutableMoveVerifierE.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_verifier_v2e_invalid" `
                -Needle "oanda_executable_move_census_v2e_verifier.py" `
                -Processes $legacyExecutableMoveVerifierE `
                -Reason "cohort_f_capture_reliability_cutover"
        }
        # Cohort F remains immutable failed evidence after the 10:46 UTC frame
        # was rejected during a canonical quote-snapshot ownership collision.
        # Stop only its workers; never backfill, delete, or relabel its rows.
        $legacyExecutableMoveCaptureF = @(
            Get-MatchingPython -Needle "--worker-id census_capture_20260901f"
        )
        if ($legacyExecutableMoveCaptureF.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_capture_v3f_invalid" `
                -Needle "--worker-id census_capture_20260901f" `
                -Processes $legacyExecutableMoveCaptureF `
                -Reason "cohort_g_exclusive_quote_owner_cutover"
        }
        $legacyExecutableMoveEvaluatorF = @(
            Get-MatchingPython -Needle "--worker-id census_evaluator_20260901f"
        )
        if ($legacyExecutableMoveEvaluatorF.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_evaluator_v3f_invalid" `
                -Needle "--worker-id census_evaluator_20260901f" `
                -Processes $legacyExecutableMoveEvaluatorF `
                -Reason "cohort_g_exclusive_quote_owner_cutover"
        }
        $legacyExecutableMoveVerifierF = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v3f_verifier.py"
        )
        if ($legacyExecutableMoveVerifierF.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_verifier_v3f_invalid" `
                -Needle "oanda_executable_move_census_v3f_verifier.py" `
                -Processes $legacyExecutableMoveVerifierF `
                -Reason "cohort_g_exclusive_quote_owner_cutover"
        }
        # The explicit full-project pause left open-market frames missing in G.
        # Preserve G as permanently invalid and prevent it from restarting.
        $legacyExecutableMoveCaptureG = @(
            Get-MatchingPython -Needle "--worker-id census_capture_20260902g"
        )
        if ($legacyExecutableMoveCaptureG.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_capture_v3g_invalid" `
                -Needle "--worker-id census_capture_20260902g" `
                -Processes $legacyExecutableMoveCaptureG `
                -Reason "cohort_h_explicit_pause_cutover"
        }
        $legacyExecutableMoveEvaluatorG = @(
            Get-MatchingPython -Needle "--worker-id census_evaluator_20260902g"
        )
        if ($legacyExecutableMoveEvaluatorG.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_evaluator_v3g_invalid" `
                -Needle "--worker-id census_evaluator_20260902g" `
                -Processes $legacyExecutableMoveEvaluatorG `
                -Reason "cohort_h_explicit_pause_cutover"
        }
        $legacyExecutableMoveVerifierG = @(
            Get-MatchingPython -Needle "oanda_executable_move_census_v3g_verifier.py"
        )
        if ($legacyExecutableMoveVerifierG.Count -gt 0) {
            Stop-MatchingPython `
                -Name "executable_move_census_verifier_v3g_invalid" `
                -Needle "oanda_executable_move_census_v3g_verifier.py" `
                -Processes $legacyExecutableMoveVerifierG `
                -Reason "cohort_h_explicit_pause_cutover"
        }
        # Cohort H starts with zero imported history after the explicit pause.
        # It remains research-only and cannot authorize, promote, or trade.
        $managed += Start-ManagedProcess `
            -Name "executable_move_census_capture_v3h" `
            -Needle "--worker-id census_capture_20260902h" `
            -Executable $Python `
            -PriorityClass "AboveNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_executable_move_census_v3.py"),
                "capture",
                "--worker-id", "census_capture_20260902h",
                "--config", (Join-Path $Trad "config\executable_move_census_v3_20260902h.json"),
                "--database", (Join-Path $State "executable_move_census_v3_20260902h.sqlite"),
                "--heartbeat", (Join-Path $State "executable_move_census_capture_heartbeat_v3_20260902h.json"),
                "--incident-log", (Join-Path $Logs "executable_move_census_capture_incidents_v3_20260902h.jsonl"),
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "executable_move_census_capture_heartbeat_v3_20260902h.json")
                MaxAgeSec = 20
                StartupGraceSec = 120
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "executable_move_census_capture_heartbeat_v3"
            }
        # Cohort H evaluation is deliberately isolated so cold
        # reconciliation cannot delay or contaminate the next capture.
        $managed += Start-ManagedProcess `
            -Name "executable_move_census_evaluator_v3h" `
            -Needle "--worker-id census_evaluator_20260902h" `
            -Executable $Python `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_executable_move_census_v3.py"),
                "evaluate",
                "--worker-id", "census_evaluator_20260902h",
                "--config", (Join-Path $Trad "config\executable_move_census_v3_20260902h.json"),
                "--database", (Join-Path $State "executable_move_census_v3_20260902h.sqlite"),
                "--output", (Join-Path $State "executable_move_census_latest_v3_20260902h.json"),
                "--heartbeat", (Join-Path $State "executable_move_census_evaluator_heartbeat_v3_20260902h.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "executable_move_census_evaluator_heartbeat_v3_20260902h.json")
                # A full append-only reconciliation now takes several minutes.
                # Minute capture is independent, so let a healthy bounded scan
                # finish instead of repeatedly killing it mid-rebuild.
                MaxAgeSec = 600
                StartupGraceSec = 900
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "executable_move_census_evaluator_heartbeat_v3"
            }
        # Independently reconstruct and verify cohort H without importing its
        # producer. Verification is research-only and fail-closed.
        $managed += Start-ManagedProcess `
            -Name "executable_move_census_verifier_v3h" `
            -Needle "oanda_executable_move_census_v3h_verifier.py" `
            -Executable $Python `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_executable_move_census_v3h_verifier.py"),
                "--config", (Join-Path $Trad "config\executable_move_census_v3_20260902h.json"),
                "--database", (Join-Path $State "executable_move_census_v3_20260902h.sqlite"),
                "--latest", (Join-Path $State "executable_move_census_latest_v3_20260902h.json"),
                "--producer", (Join-Path $Trad "oanda_executable_move_census_v3.py"),
                "--output", (Join-Path $State "executable_move_census_verifier_latest_v3_20260902h.json"),
                "--checkpoint", (Join-Path $State "executable_move_census_verifier_checkpoint_v3_20260902h.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "executable_move_census_verifier_latest_v3_20260902h.json")
                MaxAgeSec = 900
                # A cold independent reconstruction scans the complete
                # append-only frame/evaluation surface before its first output.
                # Keep the evidence expectation fail-closed, but do not kill a
                # verified-live process before that bounded scan can finish.
                StartupGraceSec = 1200
                ExpectedJsonField = "verified"
                ExpectedJsonValue = "True"
            }
        # The census shares the canonical 21-currency strength solver.  Use the
        # supervisor's resolved runtime (which carries that validated dependency)
        # so removal of an optional research environment cannot disable coverage.
        $managed += Start-ManagedProcess `
            -Name "major_move_gap_census" `
            -Needle "oanda_major_move_gap_census.py" `
            -Executable $Python `
            -StartupDelaySec 1800 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_major_move_gap_census.py"),
                "--heartbeat", (Join-Path $State "major_move_gap_census_heartbeat_v1.json"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                # Liveness and completed evidence freshness are deliberately
                # separate.  The heartbeat prevents an active hour-long
                # rebuild from being killed; the integrity audit still fails
                # closed when the last completed census itself is stale.
                LiteralPath = (Join-Path $State "major_move_gap_census_heartbeat_v1.json")
                MaxAgeSec = 120
                StartupGraceSec = 300
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "oanda_major_move_gap_census"
            }
        # Rebuild the movement-conditioned news audit after the upstream move
        # census.  This remains research-only/no-trade and makes the retained
        # miss/source mapping a continuously refreshed diagnostic instead of a
        # stale manual artifact.
        $managed += Start-ManagedProcess `
            -Name "move_first_news_case_audit" `
            -Needle "oanda_move_first_news_case_audit.py" `
            -Executable $Python `
            -StartupDelaySec 2700 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_news_case_audit.py"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_news_case_audit\MOVE_FIRST_NEWS_CASE_AUDIT_CURRENT.json")
                MaxAgeSec = 28800
                StartupGraceSec = 5400
            }
        # V3 sampled the mutable historical audit only every six hours.  Keep
        # its database as frozen evidence, but stop its worker: the V4 cohort
        # below seals cases directly from the append-only live mover database.
        $legacyMoveFirstCohorts = @(
            Get-MatchingPython -Needle "oanda_move_first_news_case_cohort_v3.py"
        )
        if ($legacyMoveFirstCohorts.Count -gt 0) {
            Stop-MatchingPython `
                -Name "move_first_news_case_cohort_v3_frozen" `
                -Needle "oanda_move_first_news_case_cohort_v3.py" `
                -Processes $legacyMoveFirstCohorts `
                -Reason "v4_direct_append_only_live_case_capture_cutover"
        }
        $managed += Start-ManagedProcess `
            -Name "move_first_live_case_capture_v4" `
            -Needle "oanda_move_first_live_case_capture_v4.py" `
            -Executable $Python `
            -StartupDelaySec 30 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_live_case_capture_v4.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_live_case_capture_v4\MOVE_FIRST_LIVE_CASE_CAPTURE_CURRENT.json")
                MaxAgeSec = 240
                StartupGraceSec = 600
            }
        # Compare the frozen pre-move narrative/context arms on the direct V4
        # ledger. This report is explicitly conditioned on realized movers;
        # label-derived continuation controls are separated and it has no
        # promotion, authorization, or broker path.
        $managed += Start-ManagedProcess `
            -Name "move_first_live_arm_alignment_v1" `
            -Needle "oanda_move_first_live_arm_alignment_v1.py" `
            -Executable $Python `
            -StartupDelaySec 75 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_live_arm_alignment_v1.py"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_live_arm_alignment_v1\MOVE_FIRST_LIVE_ARM_ALIGNMENT_CURRENT.json")
                MaxAgeSec = 240
                StartupGraceSec = 600
            }
        # Preserve the frozen V7R3 mover interpretation, but prospectively
        # seal a second explanation arm that admits semantic inputs only when
        # an exact current fast-lane mapping receipt was available before move
        # onset. This remains realized-move-conditioned, research-only, and
        # has no authorization or broker surface.
        $managed += Start-ManagedProcess `
            -Name "move_first_operational_mapping_alignment_v1" `
            -Needle "oanda_move_first_operational_mapping_alignment_v1.py*--config*move_first_operational_mapping_alignment_v1_20260901.json" `
            -Executable $Python `
            -StartupDelaySec 90 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_operational_mapping_alignment_v1.py"),
                "--config", (Join-Path $Trad "config\move_first_operational_mapping_alignment_v1_20260901.json"),
                "--ledger", (Join-Path $State "move_first_operational_mapping_alignment_v1_20260901.sqlite"),
                "--json-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v1\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"),
                "--md-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v1\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.md"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_operational_mapping_alignment_v1\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json")
                MaxAgeSec = 240
                StartupGraceSec = 600
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "move_first_operational_mapping_alignment_v1_receipt_backed_prospective_20260901T140000Z"
            }
        # V2 starts a new prospective cohort rather than altering V1.  It keeps
        # every raw receipt but collapses exact syndicated headlines whose only
        # difference is the trailing publisher attribution, preventing wire
        # repetition from masquerading as independent narrative consensus.
        $managed += Start-ManagedProcess `
            -Name "move_first_operational_mapping_alignment_v2" `
            -Needle "oanda_move_first_operational_mapping_alignment_v1.py*--config*move_first_operational_mapping_alignment_v2_20260901.json" `
            -Executable $Python `
            -StartupDelaySec 90 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_operational_mapping_alignment_v1.py"),
                "--config", (Join-Path $Trad "config\move_first_operational_mapping_alignment_v2_20260901.json"),
                "--ledger", (Join-Path $State "move_first_operational_mapping_alignment_v2_20260901.sqlite"),
                "--json-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v2\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"),
                "--md-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v2\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.md"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_operational_mapping_alignment_v2\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json")
                MaxAgeSec = 240
                StartupGraceSec = 600
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "move_first_operational_mapping_alignment_v2_receipt_backed_syndication_dedup_prospective_20260901T150000Z"
            }
        # V3 is a separately frozen, post-17:00Z cohort.  It collapses
        # conservative time-bounded headline-similarity components to their
        # first causally available member and exponentially decays the broad
        # narrative vote with a predeclared 30-minute half-life.  V2 remains
        # unchanged as the exact-syndication comparison baseline.
        $managed += Start-ManagedProcess `
            -Name "move_first_operational_mapping_alignment_v3" `
            -Needle "oanda_move_first_operational_mapping_alignment_v1.py*--config*move_first_operational_mapping_alignment_v3_20260901.json" `
            -Executable $Python `
            -StartupDelaySec 90 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_operational_mapping_alignment_v1.py"),
                "--config", (Join-Path $Trad "config\move_first_operational_mapping_alignment_v3_20260901.json"),
                "--ledger", (Join-Path $State "move_first_operational_mapping_alignment_v3_20260901.sqlite"),
                "--json-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v3\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"),
                "--md-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v3\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.md"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_operational_mapping_alignment_v3\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json")
                MaxAgeSec = 240
                StartupGraceSec = 600
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "move_first_operational_mapping_alignment_v3_receipt_backed_narrative_family_age_decay_prospective_20260901T170000Z"
            }
        # V4 is the zero-import successor to the terminally invalid V2/V3
        # cohorts.  It preserves fractional-second source and receipt clocks,
        # retains the frozen narrative-family/age-decay rule, and remains a
        # realized-move-conditioned research sidecar with no execution surface.
        $managed += Start-ManagedProcess `
            -Name "move_first_operational_mapping_alignment_v4" `
            -Needle "oanda_move_first_operational_mapping_alignment_v1.py*--config*move_first_operational_mapping_alignment_v4_20260902.json" `
            -Executable $Python `
            -StartupDelaySec 90 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_operational_mapping_alignment_v1.py"),
                "--config", (Join-Path $Trad "config\move_first_operational_mapping_alignment_v4_20260902.json"),
                "--ledger", (Join-Path $State "move_first_operational_mapping_alignment_v4_20260902.sqlite"),
                "--json-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v4\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"),
                "--md-report", (Join-Path $Reports "move_first_operational_mapping_alignment_v4\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.md"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec",
                "--quiet"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_operational_mapping_alignment_v4\MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json")
                MaxAgeSec = 240
                StartupGraceSec = 600
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "move_first_operational_mapping_alignment_v4_receipt_backed_subsecond_causal_narrative_family_prospective_20260902T121500Z"
            }
        # Calendar-flow hypotheses are labelled in a separate prospective
        # append-only side ledger.  This preserves the frozen V3 case bytes and
        # prevents the retrospective August CAD recap from being backfilled as
        # prospective month-end evidence.
        $managed += Start-ManagedProcess `
            -Name "move_first_calendar_episode_labels_v1" `
            -Needle "oanda_move_first_calendar_episode_labels_v1.py" `
            -Executable $Python `
            -StartupDelaySec 45 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_move_first_calendar_episode_labels_v1.py"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $Reports "move_first_calendar_episode_labels_v1\MOVE_FIRST_CALENDAR_EPISODE_LABELS_CURRENT.json")
                MaxAgeSec = 28800
                StartupGraceSec = 6000
            }
        $managed += Start-ManagedProcess `
            -Name "signal_combination_fit" `
            -Needle "oanda_signal_combination_fit.py*--state*signal_combination_audit_v1.json" `
            -StartupDelaySec 1200 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_combination_fit.py"),
                "--database", (Join-Path $State "signal_combination_audit_v1.sqlite"),
                "--state", (Join-Path $State "signal_combination_audit_v1.json"),
                "--horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--max-rows-per-horizon", "12000",
                "--max-conditions", "3",
                "--beam-width", "24",
                "--expansion-conditions", "20",
                "--maximum-base-features", "24",
                "--conditions-per-feature", "2",
                "--chronological-validation-blocks", "2",
                "--horizon-batch-size", "1",
                "--cursor-state", (Join-Path $State "signal_combination_audit_cursor_v1.json"),
                "--interval-sec", "300"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "signal_combination_audit_v1.json")
                MaxAgeSec = 1800
                StartupGraceSec = 3600
            }
        $managed += Start-ManagedProcess `
            -Name "signal_interaction_deep_fit" `
            -Needle "oanda_signal_combination_fit.py*--state*signal_combination_deep_v1.json" `
            -StartupDelaySec 1800 `
            -PriorityClass "Idle" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_combination_fit.py"),
                "--database", (Join-Path $State "signal_combination_audit_v1.sqlite"),
                "--state", (Join-Path $State "signal_combination_deep_v1.json"),
                "--horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--max-rows-per-horizon", "3000",
                "--minimum-train-support", "40",
                "--minimum-holdout-support", "15",
                "--max-rules-per-horizon", "40",
                "--max-conditions", "10",
                "--beam-width", "12",
                "--expansion-conditions", "32",
                "--maximum-base-features", "12",
                "--conditions-per-feature", "8",
                "--chronological-validation-blocks", "2",
                "--horizon-batch-size", "1",
                "--cursor-state", (Join-Path $State "signal_combination_deep_cursor_v1.json"),
                "--interval-sec", "60"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "signal_combination_deep_v1.json")
                MaxAgeSec = 3600
                StartupGraceSec = 7200
            }
        $managed += Start-ManagedProcess `
            -Name "signal_combination_historical_fit" `
            -Needle "oanda_signal_combination_fit.py*--state*signal_combination_historical_v1.json" `
            -StartupDelaySec 2700 `
            -PriorityClass "Idle" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_combination_fit.py"),
                "--database", (Join-Path $State "signal_combination_audit_v1.sqlite"),
                "--state", (Join-Path $State "signal_combination_historical_v1.json"),
                "--horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--max-rows-per-horizon", "60000",
                "--minimum-train-support", "80",
                "--minimum-holdout-support", "30",
                "--max-rules-per-horizon", "80",
                "--max-conditions", "3",
                "--beam-width", "24",
                "--expansion-conditions", "20",
                "--maximum-base-features", "24",
                "--conditions-per-feature", "2",
                "--chronological-validation-blocks", "2",
                "--horizon-batch-size", "1",
                "--cursor-state", (Join-Path $State "signal_combination_historical_cursor_v1.json"),
                "--interval-sec", "300"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "signal_combination_historical_v1.json")
                MaxAgeSec = 7200
                StartupGraceSec = 14400
            }
        $managed += Start-ManagedProcess `
            -Name "timeframe_matrix_calibration" `
            -Needle "oanda_timeframe_matrix_calibration.py" `
            -StartupDelaySec 600 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_timeframe_matrix_calibration.py"),
                "--source-database", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--calibration-database", (Join-Path $State "timeframe_matrix_calibration_v1.sqlite"),
                "--state", (Join-Path $State "timeframe_matrix_calibration_v1.json"),
                "--batch-size", "50000",
                "--interval-sec", "300"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "timeframe_matrix_calibration_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 3600
            }
        $managed += Start-ManagedProcess `
            -Name "strategy_exit_fit" `
            -Needle "oanda_strategy_exit_fit_worker.py" `
            -StartupDelaySec 900 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_strategy_exit_fit_worker.py"),
                "--database", (Join-Path $State "strategy_exit_fit_v1.sqlite"),
                "--state", (Join-Path $State "strategy_exit_fit_v1.json"),
                "--source-shadow-database", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--cursor-state", (Join-Path $State "strategy_exit_fit_worker_cursor_v1.json"),
                "--horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--ranking-horizon-sec", "300",
                "--max-fit-rows", "50000",
                "--interval-sec", "900"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "strategy_exit_fit_v1.json")
                MaxAgeSec = 1800
                StartupGraceSec = 3600
            }
        $managed += Start-ManagedProcess `
            -Name "shadow_outcome_compactor" `
            -Needle "oanda_shadow_outcome_compactor.py" `
            -StartupDelaySec 60 `
            -PriorityClass "Idle" `
            -Arguments @(
                (Join-Path $Trad "oanda_shadow_outcome_compactor.py"),
                "--source", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--source", (Join-Path $State "second_forecast_shadow_outcomes_v1.sqlite"),
                "--rollup-database", (Join-Path $State "shadow_outcome_rollups_v1.sqlite"),
                "--archive-root", (Join-Path $DataRoot "shadow_outcome_archive"),
                "--state", (Join-Path $State "shadow_outcome_compactor_v1.json"),
                "--retention-days", "1",
                "--batch-size", "25000",
                "--source-busy-timeout-ms", "2000",
                "--archive-pause-sec", "0.10",
                "--max-rollup-batches", "16",
                "--max-archive-batches", "16",
                "--interval-sec", "3600"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "shadow_outcome_compactor_v1.json")
                MaxAgeSec = 7200
                StartupGraceSec = 7200
            }
        $managed += Start-ManagedProcess `
            -Name "lane_promotion_fit" `
            -Needle "oanda_lane_promotion_fit.py" `
            -StartupDelaySec 120 `
            -PriorityClass "Idle" `
            -Arguments @(
                (Join-Path $Trad "oanda_lane_promotion_fit.py"),
                "--source-database", (Join-Path $State "strategy_exit_fit_v1.sqlite"),
                "--database", (Join-Path $State "lane_promotion_v1.sqlite"),
                "--state", (Join-Path $State "lane_promotion_v1.json"),
                "--additional-database", (Join-Path $State "second_forecast_promotion_v1.sqlite"),
                "--horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                # Keep the historical promotion catch-up incremental. Large catch-up
                # bursts contend with the live strategy writer and can delay fresh
                # signal publication even though the fitter runs at Idle priority.
                "--max-chunks", "1",
                "--interval-sec", "900"
            )
        $managed += Start-ManagedProcess `
            -Name "post_gap_execution_policy" `
            -Needle "oanda_post_gap_execution_pipeline.py" `
            -StartupDelaySec 1200 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_post_gap_execution_pipeline.py"),
                "--root", $Root,
                "--report-dir", (Join-Path $DataRoot "reports\modern_model_gap"),
                "--market-report", (Join-Path $DataRoot "reports\modern_model_gap\remaining_market_validation_latest.json"),
                "--exit-state", (Join-Path $State "strategy_exit_fit_v1.json"),
                "--exit-sweep", (Join-Path $DataRoot "reports\exit_policy_s5_walkforward_v1.json"),
                "--signal-feed", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--incumbent-state", (Join-Path $State "practice_007_execution_policy_v1.json"),
                "--challenger-state", (Join-Path $State "practice_007_execution_policy_challenger_v1.json"),
                "--account-auto-promotion",
                "--freeze-hours", "6",
                "--interval-sec", "900"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "reports\modern_model_gap\post_gap_execution_pipeline_latest.json")
                MaxAgeSec = 1800
                StartupGraceSec = 3600
            }
        $managed += Start-ManagedProcess `
            -Name "second_forecast_fit" `
            -Needle "oanda_second_forecast_fit.py" `
            -StartupDelaySec 240 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_second_forecast_fit.py"),
                "--source", (Join-Path $DataRoot "candles_s5_bam"),
                "--pairs", "all",
                "--horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400",
                "--smoothing-lambdas", "0,0.05,0.15,0.35,0.75,1.5",
                "--sample-step", "4",
                "--output", (Join-Path $State "second_ridge_models_v1.json"),
                "--report", (Join-Path $DataRoot "reports\second_ridge_fit_v1.json"),
                "--interval-sec", "21600"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "second_ridge_models_v1.json")
                MaxAgeSec = 25200
            }
        $managed += Start-ManagedProcess `
            -Name "second_forecast_hot" `
            -Needle "oanda_second_forecast_runner.py*--runner-role*hot" `
            -Arguments @(
                (Join-Path $Trad "oanda_second_forecast_runner.py"),
                "--creds", $ResolvedCreds,
                "--runner-role", "hot",
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "second_forecast_hot_heartbeat_v1.json"),
                "--duration-sec", "$ChildDurationSec",
                "--scan-pause-sec", "0.10",
                "--second-forecast-model", (Join-Path $State "second_ridge_models_v1.json"),
                "--second-forecast-database", (Join-Path $State "second_forecast_hot_unused.sqlite"),
                "--second-forecast-state", (Join-Path $State "second_forecast_hot_v1.json"),
                "--second-forecast-cadence-sec", "1",
                "--second-forecast-sample-sec", "5",
                "--second-forecast-signal-interval-sec", "5",
                "--execution-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400",
                "--execution-min-samples", "30",
                "--execution-min-average-pips", "0.10",
                "--execution-min-median-pips", "0.00",
                "--execution-min-win-rate", "52",
                "--execution-min-lower-confidence-pips", "0.00",
                "--execution-use-fitted-exits",
                "--execution-signal-feed-database", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--execution-policy-state", (Join-Path $State "practice_007_execution_policy_v1.json"),
                "--execution-feed-source", "second_forecast_hot",
                "--execution-min-signal-confidence", "0.56",
                "--execution-min-signal-expected-net-pips", "1.00",
                "--execution-prediction-quality",
                "--execution-prediction-quality-min-samples", "30",
                "--execution-prediction-quality-max-weight", "0.75",
                "--execution-prediction-quality-negative-veto",
                "--execution-second-curve-min-net-pips", "0.05",
                "--execution-second-curve-min-aligned-fraction", "0.5",
                "--no-execution-second-curve-entry-veto",
                "--no-execution-second-curve-profit-exit",
                "--no-execution-allow-unvalidated-signals",
                "--execution-paper-consensus",
                "--execution-paper-consensus-min-families", "3",
                "--execution-paper-consensus-min-aligned-weight-pct", "75",
                "--execution-paper-consensus-min-confidence", "0.56",
                "--execution-paper-consensus-min-net-pips", "1.00",
                "--execution-paper-consensus-min-gross-to-spread", "2.50",
                "--execution-paper-consensus-max-horizon-sec", "3600",
                "--execution-cooldown-sec", "10",
                "--execution-reentry-cooldown-sec", "3600",
                "--execution-instrument-reentry-cooldown-sec", "1800",
                "--execution-jpy-factor-cooldown-sec", "3600",
                "--execution-max-open-jpy-factor-positions", "1",
                "--execution-max-open-positions", "4",
                "--execution-max-currency-direction-margin-pct", "45",
                "--execution-target-margin-used-pct", "55",
                "--execution-high-confidence-margin-used-pct", "68",
                "--execution-hard-margin-used-pct", "75",
                "--execution-open-ended-profit",
                "--execution-min-trailing-pips", "2.0",
                "--execution-trailing-spread-multiple", "2.0",
                "--execution-trailing-stop-r", "0.45",
                "--execution-trailing-activation-r", "0.70",
                "--execution-trailing-activation-spread-multiple", "0.25",
                "--execution-profit-lock-trigger-pips", "2.0",
                "--execution-profit-lock-floor-pips", "0.2",
                "--execution-profit-lock-spread-multiple", "1.5",
                "--execution-profit-lock-step-pips", "0.25",
                "--no-execution-manage-trades",
                "--execution-lock-path", (Join-Path $State "practice_007_order.lock"),
                "--exit-fit-database", (Join-Path $State "second_forecast_exit_fit_v1.sqlite"),
                "--exit-fit-state", (Join-Path $State "second_forecast_exit_fit_v1.json"),
                "--promotion-database", (Join-Path $State "lane_promotion_v1.sqlite"),
                "--promotion-state", (Join-Path $State "lane_promotion_v1.json")
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "second_forecast_hot_heartbeat_v1.json")
                MaxAgeSec = 300
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "second_forecast_tracker" `
            -Needle "oanda_second_forecast_runner.py*--runner-role*tracker" `
            -Arguments @(
                (Join-Path $Trad "oanda_second_forecast_runner.py"),
                "--creds", $ResolvedCreds,
                "--runner-role", "tracker",
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "second_forecast_tracker_heartbeat_v1.json"),
                "--duration-sec", "$ChildDurationSec",
                "--scan-pause-sec", "0.10",
                "--second-forecast-model", (Join-Path $State "second_ridge_models_v1.json"),
                "--second-forecast-database", (Join-Path $State "second_forecast_live_v1.sqlite"),
                "--second-forecast-state", (Join-Path $State "second_forecast_live_v1.json"),
                "--second-forecast-cadence-sec", "1",
                "--second-forecast-sample-sec", "30",
                "--second-forecast-signal-interval-sec", "5",
                "--execution-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400",
                "--shadow-outcome-database", (Join-Path $State "second_forecast_shadow_outcomes_v1.sqlite"),
                "--shadow-outcome-batch-size", "4096",
                "--shadow-outcome-flush-sec", "1",
                "--shadow-outcome-log-sample-rate", "0.01",
                "--execution-signal-feed-database", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--track-shared-forecast-feed",
                "--shared-forecast-ledger", (Join-Path $State "all_signal_live_forecasts_v2.sqlite"),
                "--shared-forecast-state", (Join-Path $State "all_signal_live_forecasts_v1.json"),
                "--shared-forecast-retention-days", "7",
                "--shared-forecast-scan-sec", "0.5",
                "--shared-forecast-max-delay-sec", "5",
                "--shared-forecast-summary-sec", "300",
                "--shared-forecast-maturity-batch-size", "5000",
                "--exit-fit-database", (Join-Path $State "second_forecast_exit_fit_v1.sqlite"),
                "--exit-fit-state", (Join-Path $State "second_forecast_exit_fit_v1.json"),
                "--promotion-database", (Join-Path $State "second_forecast_promotion_v1.sqlite"),
                "--promotion-state", (Join-Path $State "lane_promotion_v1.json")
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "second_forecast_tracker_heartbeat_v1.json")
                MaxAgeSec = 300
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "second_forecast_microstructure" `
            -Needle "oanda_second_forecast_runner.py*--runner-role*microstructure" `
            -Arguments @(
                (Join-Path $Trad "oanda_second_forecast_runner.py"),
                "--creds", $ResolvedCreds,
                "--runner-role", "microstructure",
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "second_forecast_microstructure_heartbeat_v1.json"),
                "--duration-sec", "$ChildDurationSec",
                "--scan-pause-sec", "0.10",
                "--second-forecast-model", (Join-Path $State "second_ridge_models_v1.json"),
                "--second-forecast-database", (Join-Path $State "second_forecast_microstructure_v1.sqlite"),
                "--second-forecast-state", (Join-Path $State "second_forecast_microstructure_v1.json"),
                "--second-forecast-cadence-sec", "1",
                "--second-forecast-sample-sec", "5",
                "--second-forecast-signal-interval-sec", "5",
                "--execution-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400",
                "--exit-fit-database", (Join-Path $State "second_forecast_microstructure_exit_unused.sqlite"),
                "--exit-fit-state", (Join-Path $State "second_forecast_exit_fit_v1.json"),
                "--promotion-database", (Join-Path $State "second_forecast_promotion_v1.sqlite"),
                "--promotion-state", (Join-Path $State "lane_promotion_v1.json")
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "second_forecast_microstructure_heartbeat_v1.json")
                MaxAgeSec = 300
                StartupGraceSec = 600
            }
        $managed += Start-ManagedProcess `
            -Name "practice_007_quote_stream" `
            -Needle "oanda_practice_quote_stream.py" `
            -StartupDelaySec 15 `
            -PriorityClass "AboveNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_practice_quote_stream.py"),
                "--creds", $ResolvedCreds,
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "practice_007_quote_stream_heartbeat_v1.json"),
                # The fast executor is the canonical writer for the shared
                # research quote snapshot.  Keep this independent stream as
                # a transport cross-check without allowing two asynchronous
                # publishers to race on the same SQLite/mirror files.
                "--research-market-quote-snapshot", $DedicatedQuoteSnapshot,
                "--research-market-quote-snapshot-sec", "2",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_quote_stream_heartbeat_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxPhaseAgeSec = 90
                WatchedPhases = @(
                    "reading_practice_credentials",
                    "loading_practice_instruments",
                    "starting_price_stream"
                )
            }
        # Local feature observations reuse reviewed calculators, without the
        # legacy strategy/model loop. Both workers independently require a
        # verified synchronized clock before new research publication.
        $managed += Start-ManagedProcess `
            -Name "research_feature_observations_v1" `
            -Needle "oanda_research_feature_observation_worker_v1.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 60 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_research_feature_observation_worker_v1.py"),
                "--quote-snapshot", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--candle-root", (Join-Path $DataRoot "candles"),
                "--book-snapshot", (Join-Path $State "oanda_order_position_book_latest_v1.json"),
                "--clock-state", (Join-Path $State "clock_integrity_v1.json"),
                "--archive-root", (Join-Path $DataRoot "feature_observations_v1"),
                "--heartbeat", (Join-Path $State "research_feature_observations_heartbeat_v1.json"),
                "--interval-sec", "60",
                "--max-cycle-sec", "30",
                "--max-daily-archive-mib", "4096",
                "--minimum-free-mib", "4096",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "research_feature_observations_heartbeat_v1.json")
                MaxAgeSec = 150
                StartupGraceSec = 180
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "oanda_research_feature_observation_worker_v1"
            }
        $managed += Start-ManagedProcess `
            -Name "research_feature_forward_v1" `
            -Needle "oanda_feature_forward_worker_v1.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 75 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_feature_forward_worker_v1.py"),
                "--archive-root", (Join-Path $DataRoot "feature_observations_v1"),
                "--quote-path", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--directory", (Join-Path $DataRoot "feature_forward_v2"),
                "--max-ledger-mib", "4096",
                "--minimum-free-mib", "4096",
                "--clock-state", (Join-Path $State "clock_integrity_v1.json"),
                "--duration-sec", "172800"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "feature_forward_v2\feature_forward_status_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "worker"
                ExpectedJsonValue = "research_feature_forward_v1"
            }
        $managed += Start-ManagedProcess `
            -Name "clock_integrity_monitor" `
            -Needle "oanda_clock_integrity_monitor.py" `
            -StartupDelaySec 45 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_clock_integrity_monitor.py"),
                "--source", (Join-Path $State "practice_007_quote_stream_heartbeat_v1.json"),
                "--output", (Join-Path $State "clock_integrity_v1.json"),
                "--history", (Join-Path $Logs "clock_integrity_samples_v1.jsonl"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "clock_integrity_v1.json")
                MaxAgeSec = 120
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "causal_forecast_study_v1" `
            -Needle "oanda_causal_forecast_study_gap_v2.py" `
            -Executable $ModelGapPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_causal_forecast_study_gap_v2.py"),
                "--config", $CausalStudyConfig,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "causal_forecast_study_gap_v2\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "causal_forecast_study_heartbeat_v1"
            }
        $managed += Start-ManagedProcess `
            -Name "eurusd_local_forecast_study" `
            -Needle "oanda_causal_forecast_study_eurusd_v1.py" `
            -Executable $ModelGapPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_causal_forecast_study_eurusd_v1.py"),
                "--config", $EurusdStudyConfig,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "causal_forecast_study_eurusd_v1\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "causal_forecast_study_heartbeat_v1"
            }
        $managed += Start-ManagedProcess `
            -Name "pair_local_forecast_study_v1" `
            -Needle "oanda_pair_local_forecast_study_v1.py" `
            -Executable $CoreTimeseriesPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_pair_local_forecast_study_v1.py"),
                "--config", $PairStudyConfig,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "pair_local_forecast_study_v1\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "pair_local_forecast_heartbeat_v1_20260907"
            }
        $managed += Start-ManagedProcess `
            -Name "pair_local_forecast_study_v2" `
            -Needle "oanda_pair_local_forecast_study_v2.py" `
            -Executable $CoreTimeseriesPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_pair_local_forecast_study_v2.py"),
                "--config", $PairStudyV2Config,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "pair_local_forecast_study_v2\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "pair_local_forecast_heartbeat_v2_20260907"
            }
        $managed += Start-ManagedProcess `
            -Name "joint_price_news_study_v1" `
            -Needle "oanda_joint_price_news_forecast_study_v1.py" `
            -Executable $CoreTimeseriesPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_joint_price_news_forecast_study_v1.py"),
                "--config", $JointPriceNewsConfig,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "joint_price_news_study_v1\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "joint_price_news_forecast_heartbeat_v1_20260907"
            }
        $managed += Start-ManagedProcess `
            -Name "joint_price_news_study_v2" `
            -Needle "oanda_joint_price_news_forecast_study_v2.py" `
            -Executable $CoreTimeseriesPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_joint_price_news_forecast_study_v2.py"),
                "--config", $JointPriceNewsV2Config,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "joint_price_news_study_v2\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "joint_price_news_forecast_heartbeat_v2_20260907"
            }
        $managed += Start-ManagedProcess `
            -Name "local_news_sentiment_repair_v1" `
            -Needle "oanda_local_news_sentiment_repair_v1.py" `
            -Executable $CoreTimeseriesPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_local_news_sentiment_repair_v1.py"),
                "--data-root", $DataRoot,
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "local_news_sentiment_repair_v1\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "repaired_joint_news_heartbeat_v1_20260908"
            }
        $managed += Start-ManagedProcess `
            -Name "joint_price_news_study_v3" `
            -Needle "oanda_joint_price_news_forecast_study_v3.py" `
            -Executable $CoreTimeseriesPython `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_joint_price_news_forecast_study_v3.py"),
                "--config", $JointPriceNewsV3Config,
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $DataRoot "joint_price_news_study_v3\heartbeat.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                ExpectedJsonField = "schema_version"
                ExpectedJsonValue = "joint_price_news_forecast_heartbeat_v3_20260908"
            }
        $managed += Start-ManagedProcess `
            -Name "canonical_outcome_worker" `
            -Needle "oanda_canonical_outcome_worker.py" `
            -StartupDelaySec 20 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_canonical_outcome_worker.py"),
                "--forecast-database", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--queue-database", (Join-Path $State "canonical_outcome_worker_v1.sqlite"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--heartbeat", (Join-Path $State "canonical_outcome_worker_v1.json"),
                "--poll-sec", "0.5",
                "--ingest-sec", "1",
                "--ingest-batch-size", "1000",
                "--maturity-grace-sec", "15",
                "--max-snapshot-age-sec", "15",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "canonical_outcome_worker_v1.json")
                MaxAgeSec = 300
                StartupGraceSec = 1200
            }
        $managed += Start-ManagedProcess `
            -Name "proof_shadow_predictors" `
            -Needle "oanda_proof_shadow_predictors.py" `
            -StartupDelaySec 45 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_proof_shadow_predictors.py"),
                "--snapshot", (Join-Path $State "live_model_feature_snapshot_v1.json"),
                "--database", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--heartbeat", (Join-Path $State "proof_shadow_predictors_v1.json"),
                "--cadence-sec", "900",
                "--max-snapshot-age-sec", "300",
                "--poll-sec", "5",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "proof_shadow_predictors_v1.json")
                # A full 68-pair graph/tabular proof build has been observed taking
                # about 4.5 minutes while the immutable database is busy.  Keep a
                # ten-minute research-only window so ordinary load variance cannot
                # kill a healthy cadence and reset process-local diagnostics.
                MaxAgeSec = 600
                StartupGraceSec = 240
            }
        $managed += Start-ManagedProcess `
            -Name "edge_evidence_worker" `
            -Needle "oanda_edge_evidence_worker.py" `
            -StartupDelaySec 75 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_edge_evidence_worker.py"),
                "--source-database", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--evidence-database", (Join-Path $State "edge_evidence_v1.sqlite"),
                "--output-json", (Join-Path $State "edge_evidence_v1.json"),
                "--output-markdown", (Join-Path $Reports "edge_evidence\EDGE_EVIDENCE_CURRENT.md"),
                "--registry", (Join-Path $Trad "docs\BACKTEST_AND_MODEL_REGISTRY.md"),
                "--news-database", (Join-Path $DataRoot "local_news_sentiment\local_news_sentiment_v1.sqlite"),
                "--heartbeat", (Join-Path $State "edge_evidence_worker_v1.json"),
                "--heartbeat-sec", "15",
                "--interval-sec", "900",
                "--minimum-rebuild-interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "edge_evidence_worker_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxProgressAgeSec = 1800
                ProgressPhases = @(
                    "inventorying_source",
                    "loading_rows",
                    "loading_rows_complete",
                    "inventorying_forecast_contract",
                    "inventorying_forecast_contract_complete",
                    "building_cells",
                    "building_archetypes",
                    "building_economic_labels",
                    "building_candidate_replication",
                    "building_governance",
                    "assembling_report",
                    "persisting_evidence",
                    "writing_report"
                )
            }
        # Keep the five-minute frozen allocator proof clock independent from
        # the much larger lifecycle rebuild. The runner imports the unchanged
        # allocator implementation, so this operational split does not alter
        # its immutable model or cohort hash.
        $managed += Start-ManagedProcess `
            -Name "allocator_proof_worker" `
            -Needle "oanda_allocator_proof_worker.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_allocator_proof_worker.py"),
                "--heartbeat", (Join-Path $State "allocator_proof_worker_heartbeat_v1.json"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "allocator_proof_worker_heartbeat_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxProgressAgeSec = 900
                ProgressPhases = @(
                    "running_cycle",
                    "idle_between_cycles",
                    "cycle_error"
                )
            }
        $managed += Start-ManagedProcess `
            -Name "evidence_operations_worker" `
            -Needle "oanda_evidence_operations_worker.py" `
            -StartupDelaySec 120 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_evidence_operations_worker.py"),
                "--heartbeat", (Join-Path $State "evidence_operations_worker_v1.json"),
                "--liveness-heartbeat", (Join-Path $State "evidence_operations_worker_heartbeat_v2.json"),
                "--heartbeat-sec", "15",
                "--report", (Join-Path $Reports "evidence_operations\EVIDENCE_OPERATIONS_CURRENT.md"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                # Keep liveness separate from the last complete lifecycle and
                # allocator snapshot. The four-stage cycle can exceed twenty
                # minutes while making valid research-only progress.
                LiteralPath = (Join-Path $State "evidence_operations_worker_heartbeat_v2.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
                MaxProgressAgeSec = 1800
                ProgressPhases = @(
                    "running_lifecycle",
                    "running_allocator",
                    "running_accounting",
                    "running_opportunity_monitor",
                    "publishing_final_state"
                )
            }
        # Prospectively append only newly observed collector rows into source
        # governance and record when their governed mapping became usable.
        # The complete 30-minute reconciliation below remains the audit
        # backstop. This lane is research-only and cannot route or authorize.
        $managed += Start-ManagedProcess `
            -Name "source_governance_news_fast_lane" `
            -Needle "oanda_source_governance_news_fast_lane.py" `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_source_governance_news_fast_lane.py"),
                "--news-database", (Join-Path $DataRoot "local_news_sentiment\local_news_sentiment_v1.sqlite"),
                "--database", (Join-Path $State "source_governance_v1.sqlite"),
                "--state", (Join-Path $State "source_governance_news_fast_lane_v3.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "source_governance_news_fast_lane_v3.json")
                MaxAgeSec = 150
                StartupGraceSec = 360
                ExpectedJsonField = "contract_id"
                ExpectedJsonValue = "news_source_governance_fast_lane_v3_committed_visibility_20260905"
            }
        $managed += Start-ManagedProcess `
            -Name "source_governance" `
            -Needle "oanda_source_governance.py" `
            -StartupDelaySec 180 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_source_governance.py"),
                "--database", (Join-Path $State "source_governance_v1.sqlite"),
                "--state", (Join-Path $State "source_governance_v1.json"),
                "--report", (Join-Path $Reports "source_governance\SOURCE_GOVERNANCE_CURRENT.md"),
                "--interval-sec", "1800",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "source_governance_v1.json")
                MaxAgeSec = 7200
                StartupGraceSec = 1200
            }
        $managed += Start-ManagedProcess `
            -Name "research_genealogy" `
            -Needle "oanda_research_genealogy.py" `
            -StartupDelaySec 210 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_research_genealogy.py"),
                "--database", (Join-Path $State "research_genealogy_v1.sqlite"),
                "--state", (Join-Path $State "research_genealogy_v1.json"),
                "--report", (Join-Path $Reports "research_genealogy\RESEARCH_GENEALOGY_CURRENT.md"),
                # The evidence-operations lifecycle handoff now synchronizes and
                # reconciles governed cells before publication. This broad static
                # genealogy scan is intentionally hourly so its long append-only
                # transaction does not continuously contend with that handoff.
                "--interval-sec", "3600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "research_genealogy_v1.json")
                MaxAgeSec = 7200
                StartupGraceSec = 1200
            }
        $managed += Start-ManagedProcess `
            -Name "independent_evidence_verifier" `
            -Needle "oanda_independent_evidence_verifier.py" `
            -StartupDelaySec 240 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_independent_evidence_verifier.py"),
                "--state", (Join-Path $State "independent_evidence_verifier_v1.json"),
                "--report", (Join-Path $Reports "independent_evidence_verifier\INDEPENDENT_EVIDENCE_VERIFIER_CURRENT.md"),
                "--evidence-operations-worker-state", (Join-Path $State "evidence_operations_worker_heartbeat_v2.json"),
                "--maximum-state-age-sec", "1800",
                "--maximum-lifecycle-snapshot-lag-sec", "7200",
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "independent_evidence_verifier_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 1200
            }
        $managed += Start-ManagedProcess `
            -Name "project_integrity_audit" `
            -Needle "oanda_project_integrity_audit.py" `
            -StartupDelaySec 270 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_project_integrity_audit.py"),
                "--output", (Join-Path $State "project_integrity_audit_v1.json"),
                "--history", (Join-Path $Logs "project_integrity_audit_v1.jsonl"),
                "--report", (Join-Path $Reports "project_integrity\PROJECT_INTEGRITY_CURRENT.md"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "project_integrity_audit_v1.json")
                # Fast cycles use a metadata-bound full-integrity attestation,
                # while a mandatory fresh full-database scan still runs at
                # least every six hours and takes roughly 12 minutes. Keep the
                # liveness bound above that measured full-scan path but well
                # below the former 45-minute tolerance. This does not relax the
                # audit's own 180-second publication-freshness failure.
                MaxAgeSec = 1200
                StartupGraceSec = 1500
                ExpectedJsonField = "classification_version"
                ExpectedJsonValue = "local_fx_news_rules_20260907_v165_causal_member_admission"
            }
        $managed += Start-ManagedProcess `
            -Name "storage_headroom_guard" `
            -Needle "oanda_storage_headroom_guard.py" `
            -StartupDelaySec 285 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_storage_headroom_guard.py"),
                "--output", (Join-Path $State "storage_headroom_v1.json"),
                "--history", (Join-Path $Logs "storage_headroom_v1.jsonl"),
                "--report", (Join-Path $Reports "storage\STORAGE_HEADROOM_CURRENT.md"),
                "--interval-sec", "300",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "storage_headroom_v1.json")
                MaxAgeSec = 900
                StartupGraceSec = 1200
            }
        $managed += Start-ManagedProcess `
            -Name "shadow_archive_integrity" `
            -Needle "oanda_shadow_archive_integrity.py" `
            -Executable $ModelGapPython `
            -StartupDelaySec 300 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_shadow_archive_integrity.py"),
                "--archive", (Join-Path $DataRoot "shadow_outcome_archive"),
                "--state", (Join-Path $State "shadow_archive_integrity_v1.json"),
                "--report", (Join-Path $Reports "storage\SHADOW_ARCHIVE_INTEGRITY_CURRENT.md"),
                "--rollup", (Join-Path $State "shadow_outcome_rollups_v1.sqlite"),
                "--source", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "shadow_archive_integrity_v1.json")
                MaxAgeSec = 25200
                StartupGraceSec = 1800
            }
        $managed += Start-ManagedProcess `
            -Name "verified_log_archiver" `
            -Needle "oanda_verified_log_archiver.py" `
            -StartupDelaySec 300 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_verified_log_archiver.py"),
                "--root", $Logs,
                "--state", (Join-Path $State "verified_log_archiver_v1.json"),
                # Active integrity snapshots are deliberately complete and
                # therefore highly compressible, but they rotate frequently.
                # A six-hour raw retention window plus a four-GiB verified
                # batch keeps up with measured production while preserving
                # byte-for-byte recoverability before any raw part is removed.
                "--minimum-age-hours", "6",
                "--maximum-source-gib", "4",
                "--interval-sec", "21600",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "verified_log_archiver_v1.json")
                MaxAgeSec = 25200
                StartupGraceSec = 1800
            }
        $managed += Start-ManagedProcess `
            -Name "practice_007_quote_transport_crosscheck" `
            -Needle "oanda_quote_transport_crosscheck.py" `
            -StartupDelaySec 60 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_quote_transport_crosscheck.py"),
                "--canonical", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--comparison", (Join-Path $State "practice_007_market_quotes_transport_check_v1.json"),
                "--state", (Join-Path $State "practice_007_quote_transport_crosscheck_v1.json"),
                "--interval-sec", "30",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_quote_transport_crosscheck_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "official_document_sla" `
            -Needle "oanda_official_document_sla.py" `
            -StartupDelaySec 15 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_official_document_sla.py"),
                "--database", (Join-Path $DataRoot "local_news_sentiment\local_news_sentiment_v1.sqlite"),
                "--state", (Join-Path $State "official_document_sla_v1.json"),
                "--report", (Join-Path $Reports "source_governance\OFFICIAL_DOCUMENT_SLA_CURRENT.md"),
                "--interval-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "official_document_sla_v1.json")
                MaxAgeSec = 180
                StartupGraceSec = 300
            }
        $managed += Start-ManagedProcess `
            -Name "practice_007_signal_feed_availability" `
            -Needle "oanda_signal_feed_availability_monitor.py" `
            -StartupDelaySec 15 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_feed_availability_monitor.py"),
                "--heartbeat", (Join-Path $State "practice_007_top_executor_heartbeat_v1.json"),
                "--state", (Join-Path $State "practice_007_signal_feed_availability_v1.json"),
                "--events", (Join-Path $Logs "practice_007_signal_feed_availability_v1.jsonl"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--source", "strategy_lab",
                "--comparison-source", "strategy_lab|strategy_lab_partial_shadow",
                "--interval-sec", "2",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_signal_feed_availability_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 90
            }
        $managed += Start-ManagedProcess `
            -Name "practice_007_reentry_shadow" `
            -Needle "oanda_practice_reentry_shadow.py" `
            -StartupDelaySec 15 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_practice_reentry_shadow.py"),
                "--creds", $ResolvedCreds,
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "practice_007_reentry_shadow_heartbeat_v1.json"),
                "--output", (Join-Path $State "practice_007_reentry_cooldown_shadow_v1.json"),
                "--since-id", "2315",
                "--cooldowns-sec", "60,300,900,1800",
                "--poll-sec", "60",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_reentry_shadow_heartbeat_v1.json")
                MaxAgeSec = 180
                StartupGraceSec = 300
                MaxPhaseAgeSec = 120
                WatchedPhases = @("reading_practice_transactions")
            }
        $managed += Start-ManagedProcess `
            -Name "practice_007_fast_executor" `
            -Needle "oanda_practice_top_signal_executor.py" `
            -PriorityClass "AboveNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_practice_top_signal_executor.py"),
                "--creds", $ResolvedCreds,
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "practice_007_top_executor_heartbeat_v1.json"),
                "--duration-sec", "$ChildDurationSec",
                "--scan-pause-sec", "0.50",
                "--use-price-stream",
                "--run-label", $RunLabel,
                "--outcome-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--execute-top-signals",
                "--execution-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--execution-min-samples", "30",
                "--execution-min-average-pips", "0.10",
                "--execution-min-median-pips", "0.00",
                "--execution-min-win-rate", "52",
                "--execution-min-lower-confidence-pips", "0.00",
                "--execution-use-fitted-exits",
                "--execution-signal-feed-database", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--execution-policy-state", (Join-Path $State "practice_007_execution_policy_v1.json"),
                "--governed-canary-authorization", (Join-Path $State "practice_007_governed_canary_authorization_v1.json"),
                "--governed-canary-maximum-age-sec", "900",
                "--governed-lifecycle-database", (Join-Path $State "evidence_lifecycle_v1.sqlite"),
                "--governed-independent-verifier-state", (Join-Path $State "independent_evidence_verifier_v1.json"),
                "--governed-canary-consumption-database", (Join-Path $State "practice_007_canary_consumptions_v1.sqlite"),
                # Preserve the independent stream's last-known coverage across
                # weekend restarts. Retained rows remain research-only and are
                # never inserted into the executor's live/fresh price map.
                "--research-market-quote-seed-snapshot", (Join-Path $State "practice_007_market_quotes_transport_check_v1.json"),
                # The fast executor owns the canonical all-68 quote publication.
                # Keep the dedicated stream as an independent transport check,
                # but do not leave canonical research consumers on a stale file
                # after a clean supervisor restart.
                "--research-market-quote-snapshot", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--research-market-quote-snapshot-sec", "1",
                "--execution-feed-source", "practice_007_fast_executor",
                "--execution-feed-consumer-only",
                "--execution-signal-feed-ttl-sec", "90",
                "--execution-signal-feed-cache-refresh-sec", "5",
                "--execution-signal-feed-cache-ttl-sec", "90",
                "--execution-intrahour-cost-gate",
                "--execution-intrahour-cost-gate-max-horizon-sec", "3600",
                "--execution-intrahour-max-spread-pips", "2.0",
                "--execution-intrahour-min-liquidity-quality", "0.70",
                "--execution-intrahour-min-confidence", "0.56",
                "--execution-intrahour-min-gross-to-spread", "2.50",
                "--execution-intrahour-min-after-cost-pips", "1.00",
                "--execution-multihour-max-spread-pips", "3.0",
                "--execution-multihour-min-liquidity-quality", "0.65",
                "--execution-multihour-min-confidence", "0.55",
                "--execution-multihour-min-gross-to-spread", "2.00",
                "--execution-multihour-min-after-cost-pips", "1.00",
                "--execution-horizon-scaled-protection",
                "--execution-signal-snapshot", (Join-Path $State "practice_007_signal_snapshot_v1.json"),
                # Execution reads directly from SQLite; the JSON file serves
                # dashboards and derived observers, whose freshness budget is
                # much wider than the 0.5-second scan loop.
                "--execution-signal-snapshot-sec", "5",
                # Preserve one consolidated curve for every canonical pair so
                # the causal move ledger can archive pre-move forecasts even
                # when a pair is not among the few highest-ranked signals.
                "--execution-snapshot-top-signals", "68",
                "--execution-min-signal-confidence", "0.56",
                "--execution-min-signal-expected-net-pips", "1.00",
                "--execution-prediction-quality",
                "--execution-prediction-quality-min-samples", "30",
                "--execution-prediction-quality-max-weight", "0.75",
                "--execution-prediction-quality-negative-veto",
                "--execution-second-curve-min-net-pips", "0.05",
                "--execution-second-curve-min-aligned-fraction", "0.5",
                "--no-execution-second-curve-entry-veto",
                "--no-execution-second-curve-profit-exit",
                "--no-execution-allow-unvalidated-signals",
                "--no-execution-paper-consensus",
                "--execution-cooldown-sec", "10",
                "--execution-reentry-cooldown-sec", "3600",
                "--execution-instrument-reentry-cooldown-sec", "1800",
                "--execution-jpy-factor-cooldown-sec", "3600",
                "--execution-max-open-jpy-factor-positions", "1",
                "--execution-max-open-positions", "4",
                "--execution-max-currency-direction-margin-pct", "45",
                "--execution-target-margin-used-pct", "55",
                "--execution-high-confidence-margin-used-pct", "68",
                "--execution-hard-margin-used-pct", "75",
                "--execution-open-ended-profit",
                "--execution-manage-interval-sec", "0.25",
                "--execution-min-trailing-pips", "2.0",
                "--execution-trailing-spread-multiple", "2.0",
                "--execution-trailing-stop-r", "0.45",
                "--execution-trailing-activation-r", "0.70",
                "--execution-trailing-activation-spread-multiple", "0.25",
                "--execution-profit-lock-trigger-pips", "2.0",
                "--execution-profit-lock-floor-pips", "0.2",
                "--execution-profit-lock-spread-multiple", "1.5",
                "--execution-profit-lock-step-pips", "0.25",
                "--execution-manage-trades",
                "--exit-fit-database", (Join-Path $State "strategy_exit_fit_v1.sqlite"),
                "--exit-fit-state", (Join-Path $State "strategy_exit_fit_v1.json"),
                "--promotion-database", (Join-Path $State "lane_promotion_v1.sqlite"),
                "--promotion-state", (Join-Path $State "lane_promotion_v1.json"),
                "--execution-lock-path", (Join-Path $State "practice_007_order.lock"),
                "--no-enable-second-forecasts"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "practice_007_top_executor_heartbeat_v1.json")
                # Model initialization and a complete cross-horizon ranking pass
                # can exceed three minutes on the guarded D: volume. Preserve
                # recovery without recycling a healthy paper executor mid-pass.
                MaxAgeSec = 420
                StartupGraceSec = 600
                MaxPhaseAgeSec = 420
                WatchedPhases = @(
                    "opening_signal_feed",
                    "initializing_practice_account",
                    "starting_price_stream"
                )
            }
        $managed += Start-ManagedProcess `
            -Name "top_signal_position_ledger" `
            -Needle "oanda_top_signal_position_ledger.py" `
            -Arguments @(
                (Join-Path $Trad "oanda_top_signal_position_ledger.py"),
                "--signals", (Join-Path $State "practice_007_signal_snapshot_research_v1.json"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--database", (Join-Path $State "top_signal_position_ledger_v1.sqlite"),
                "--state", (Join-Path $State "top_signal_position_ledger_v1.json"),
                "--interval-sec", "5",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "top_signal_position_ledger_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "manager_decision_outcome_ledger" `
            -Needle "oanda_manager_decision_outcome_ledger.py" `
            -StartupDelaySec 15 `
            -PriorityClass "BelowNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_manager_decision_outcome_ledger.py"),
                "--decisions", (Join-Path $Reports "LIVE_ACCOUNT_MANAGER_FORECASTS_20260810.jsonl"),
                "--quotes-database", (Join-Path $State "practice_007_market_quotes_v1.json.sqlite"),
                "--database", (Join-Path $State "manager_decision_outcomes_v1.sqlite"),
                "--state", (Join-Path $State "manager_decision_outcomes_v1.json"),
                "--interval-sec", "2",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "manager_decision_outcomes_v1.json")
                MaxAgeSec = 30
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "canonical_signal_trials" `
            -Needle "oanda_signal_trial_ledger.py" `
            -Arguments @(
                (Join-Path $Trad "oanda_signal_trial_ledger.py"),
                "--signals", (Join-Path $State "practice_007_signal_snapshot_research_v1.json"),
                "--quotes", (Join-Path $State "practice_007_market_quotes_v1.json"),
                "--news-context", (Join-Path $DataRoot "news_event_tags\latest_pair_news_context.json"),
                "--database", (Join-Path $State "canonical_signal_trials_v1.sqlite"),
                "--state", (Join-Path $State "canonical_signal_trials_v1.json"),
                "--interval-sec", "5",
                "--duration-sec", "$ChildDurationSec"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "canonical_signal_trials_v1.json")
                MaxAgeSec = 90
                StartupGraceSec = 180
            }
        $managed += Start-ManagedProcess `
            -Name "strategy_lab" `
            -Needle "oanda_practice_shadow_strategy_lab.py" `
            -PriorityClass "AboveNormal" `
            -Arguments @(
                (Join-Path $Trad "oanda_practice_shadow_strategy_lab.py"),
                "--creds", $ResolvedCreds,
                "--account-key", $AccountKey,
                "--heartbeat-state", (Join-Path $State "strategy_lab_heartbeat_v1.json"),
                "--duration-sec", "$ChildDurationSec",
                "--scan-pause-sec", "0.25",
                "--run-label", $RunLabel,
                "--outcome-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--shadow-outcome-horizon-mode", "declared",
                "--timeframe-matrix-outcome-mode", "native",
                "--execution-horizons", "60,120,180,300,600,900,1800,3600,7200,10800,14400,21600,28800,43200,86400",
                "--execution-min-samples", "30",
                "--execution-min-average-pips", "0.10",
                "--execution-min-median-pips", "0.00",
                "--execution-min-win-rate", "52",
                "--execution-min-lower-confidence-pips", "0.00",
                "--execution-use-fitted-exits",
                "--execution-signal-feed-database", (Join-Path $State "practice_007_signal_feed_v1.sqlite"),
                "--execution-policy-state", (Join-Path $State "practice_007_execution_policy_v1.json"),
                "--execution-feed-source", "strategy_lab",
                "--execution-signal-feed-ttl-sec", "90",
                "--execution-signal-snapshot", (Join-Path $State "practice_007_signal_snapshot_research_v1.json"),
                # Keep strategy-lab research transport physically separate from
                # the canonical Practice-007 quote snapshot owned by the fast
                # executor. Producer identity is part of the evidence contract.
                "--research-market-quote-snapshot", (Join-Path $State "strategy_lab_market_quotes_research_v1.json"),
                "--execution-snapshot-top-signals", "68",
                "--execution-min-signal-confidence", "0.56",
                "--execution-min-signal-expected-net-pips", "1.00",
                "--execution-prediction-quality",
                "--execution-prediction-quality-min-samples", "30",
                "--execution-prediction-quality-max-weight", "0.75",
                "--execution-prediction-quality-negative-veto",
                "--execution-second-curve-min-net-pips", "0.05",
                "--execution-second-curve-min-aligned-fraction", "0.5",
                "--no-execution-second-curve-entry-veto",
                "--no-execution-second-curve-profit-exit",
                "--no-execution-allow-unvalidated-signals",
                "--execution-paper-consensus",
                "--execution-paper-consensus-min-families", "3",
                "--execution-paper-consensus-min-aligned-weight-pct", "75",
                "--execution-paper-consensus-min-confidence", "0.56",
                "--execution-paper-consensus-min-net-pips", "1.00",
                "--execution-paper-consensus-min-gross-to-spread", "2.50",
                "--execution-paper-consensus-max-horizon-sec", "3600",
                "--execution-cooldown-sec", "10",
                "--execution-max-open-positions", "4",
                "--execution-max-currency-direction-margin-pct", "45",
                "--execution-target-margin-used-pct", "55",
                "--execution-high-confidence-margin-used-pct", "68",
                "--execution-hard-margin-used-pct", "75",
                "--execution-open-ended-profit",
                "--execution-manage-interval-sec", "0.25",
                "--execution-min-trailing-pips", "2.0",
                "--execution-trailing-spread-multiple", "2.0",
                "--execution-trailing-stop-r", "0.45",
                "--execution-trailing-activation-r", "0.70",
                "--execution-trailing-activation-spread-multiple", "0.25",
                "--execution-profit-lock-trigger-pips", "2.0",
                "--execution-profit-lock-floor-pips", "0.2",
                "--execution-profit-lock-spread-multiple", "1.5",
                "--execution-profit-lock-step-pips", "0.25",
                "--execution-manage-trades",
                "--combination-database", (Join-Path $State "signal_combination_audit_v1.sqlite"),
                "--combination-state", (Join-Path $State "signal_combination_audit_v1.json"),
                "--combination-deep-state", (Join-Path $State "signal_combination_deep_v1.json"),
                "--combination-historical-state", (Join-Path $State "signal_combination_historical_v1.json"),
                "--sma-filter-batch-size", "16384",
                "--exit-fit-database", (Join-Path $State "strategy_exit_fit_v1.sqlite"),
                "--exit-fit-state", (Join-Path $State "strategy_exit_fit_v1.json"),
                "--no-exit-fit-record-outcomes",
                "--shadow-outcome-database", (Join-Path $State "strategy_shadow_outcomes_v1.sqlite"),
                "--shadow-outcome-batch-size", "4096",
                "--shadow-outcome-flush-sec", "1",
                "--shadow-outcome-log-sample-rate", "0.01",
                "--hard-reject-outcome-sample-rate", "0.002",
                "--near-threshold-outcome-sample-rate", "0.10",
                # A full 209-lane feature cycle can take roughly two minutes and
                # enqueue several thousand horizon outcomes.  A 64-row cap
                # created a multi-hour maturity backlog, making nominal M1-H1
                # labels non-causal.  Drain at least one full cycle per pass;
                # the canonical evaluator still excludes any late endpoint.
                "--outcome-max-per-cycle", "8192",
                "--outcome-max-scan-per-cycle", "262144",
                "--timeframe-matrix-calibration-state", (Join-Path $State "timeframe_matrix_calibration_v1.json"),
                "--live-model-feature-snapshot", (Join-Path $State "live_model_feature_snapshot_v1.json"),
                "--order-book-feature-state", (Join-Path $State "oanda_order_position_book_latest_v1.json"),
                "--order-book-feature-max-age-sec", "1800",
                "--promotion-database", (Join-Path $State "lane_promotion_v1.sqlite"),
                "--promotion-state", (Join-Path $State "lane_promotion_v1.json"),
                "--execution-lock-path", (Join-Path $State "practice_007_order.lock"),
                "--no-enable-second-forecasts"
            ) `
            -Freshness @{
                LiteralPath = (Join-Path $State "strategy_lab_heartbeat_v1.json")
                MaxAgeSec = 600
                StartupGraceSec = 900
                MaxPhaseAgeSec = 900
                WatchedPhases = @("initializing", "building_features")
            }
        Write-SupervisorEvent "heartbeat" @{
            managed = $managed
            storage = @(
                Measure-ManagedArtifact `
                    -Name "signal_feed_wal" `
                    -LiteralPath (Join-Path $State "practice_007_signal_feed_v1.sqlite-wal") `
                    -WarningBytes 134217728
                Measure-ManagedArtifact `
                    -Name "news_feed_wal" `
                    -LiteralPath (Join-Path $DataRoot "local_news_sentiment\local_news_sentiment_v1.sqlite-wal") `
                    -WarningBytes 134217728
                Measure-ManagedArtifact `
                    -Name "event_catalog_wal" `
                    -LiteralPath (Join-Path $DataRoot "news_event_tags\news_event_tags.sqlite-wal") `
                    -WarningBytes 134217728 `
                    -AbsentStatus "checkpointed_absent"
                Measure-ManagedArtifact `
                    -Name "canonical_shadow_ledger" `
                    -LiteralPath (Join-Path $State "strategy_shadow_outcomes_v1.sqlite") `
                    -WarningBytes 8589934592
                Measure-ManagedArtifact `
                    -Name "edge_evidence_database" `
                    -LiteralPath (Join-Path $State "edge_evidence_v1.sqlite") `
                    -WarningBytes 1073741824
            )
        }
    } catch {
        Write-SupervisorEvent "supervisor_error" @{
            error = $_.Exception.Message
        }
    }
    Start-Sleep -Seconds $IntervalSec
}
