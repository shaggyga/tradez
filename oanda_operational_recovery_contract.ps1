# Pure validation/argument helpers. Dot-sourcing does not start or stop services.
$OperationalRecoverySchema = "forex_operational_runtime_v1_20260913"

function Get-OperationalRecoveryServices {
    return @{
        revision_news_collector_v2 = "oanda_local_news_sentiment.py"
        local_news_sentiment_repair_v2 = "oanda_local_news_sentiment_repair_v2.py"
        revision_news_transport_v4 = "revision_transport_v4.py"
        joint_price_news_study_v7 = "oanda_joint_price_news_forecast_study_v7.py"
        pair_local_forecast_study_v3 = "oanda_pair_local_forecast_study_v3.py"
        retained_price_settlement_v1 = "oanda_retained_price_settlement_v1.py"
        native_feature_candles_v1 = "oanda_native_feature_candle_updater_v1.py"
        research_feature_observations_v2 = "oanda_research_feature_observation_worker_v2.py"
        research_feature_forward_v2 = "oanda_feature_forward_worker_v1.py"
        official_pair_horizon_v2 = "oanda_official_event_pair_horizon_capture_v2.py"
    }
}

function Get-OperationalRecoverySourceHash([string]$LiteralPath) {
    # Native Windows PowerShell may inherit a PS7 module path that hides
    # Get-FileHash. Use the runtime directly so recovery has no module dependency.
    $stream = [IO.File]::OpenRead($LiteralPath)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-','').ToLowerInvariant() }
    finally { $sha.Dispose(); $stream.Dispose() }
}

function Resolve-OperationalRecoveryPath([string]$LiteralPath) {
    if ([string]::IsNullOrWhiteSpace($LiteralPath) -or $LiteralPath -match '[\r\n"]') {
        throw "Invalid operational recovery path."
    }
    $absolute = [IO.Path]::GetFullPath($LiteralPath)
    if ($absolute -match '^[dD]:\\') { throw "Legacy D paths are not operational recovery targets." }
    $current = $absolute
    while ($current) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Operational recovery path contains a reparse point."
            }
        }
        $parent = [IO.Path]::GetDirectoryName($current)
        if ($parent -eq $current) { break }
        $current = $parent
    }
    return $absolute
}

function Read-OperationalRecoveryProfile {
    param([string]$Trad,[string]$ProfilePath,[string]$RecoveryUntilUtc,[DateTimeOffset]$Now = [DateTimeOffset]::UtcNow)
    $project = Resolve-OperationalRecoveryPath $Trad
    $path = Resolve-OperationalRecoveryPath $ProfilePath
    if (-not $path.StartsWith(($project + '\'), [StringComparison]::OrdinalIgnoreCase)) {
        throw "Operational profile must be inside the canonical project."
    }
    if ($RecoveryUntilUtc -notmatch '(Z|[+]00:00)$') { throw "Explicit UTC recovery expiry required." }
    $expiry = [DateTimeOffset]::Parse($RecoveryUntilUtc)
    if ($expiry -le $Now) { throw "Operational recovery expiry reached." }
    if (($expiry-$Now).TotalDays -gt 8) { throw "Operational recovery expiry exceeds eight-day bound." }
    $file = Get-Item -LiteralPath $path
    if ($file.Length -le 0 -or $file.Length -gt 131072) { throw "Operational profile size invalid." }
    $bytes = [IO.File]::ReadAllBytes($path)
    $profile = [Text.Encoding]::UTF8.GetString($bytes).TrimStart([char]0xFEFF) | ConvertFrom-Json
    if ($profile.schema_version -cne $OperationalRecoverySchema -or
        $profile.research_only -isnot [bool] -or -not $profile.research_only -or
        $profile.can_place_orders -isnot [bool] -or $profile.can_place_orders) {
        throw "Operational recovery requires an explicit research-only no-orders profile."
    }
    $allowed = Get-OperationalRecoveryServices
    $services = @($profile.services)
    $names = @($services | ForEach-Object { [string]$_.name })
    if ($services.Count -ne $allowed.Count -or @($names | Sort-Object -Unique).Count -ne $allowed.Count) {
        throw "Operational recovery requires exactly ten distinct managed services."
    }
    $data = Join-Path $project 'data\oanda_training_manager'
    foreach ($service in $services) {
        if (-not $allowed.ContainsKey([string]$service.name) -or
            $service.script -cne $allowed[[string]$service.name] -or
            $service.source_sha256 -cnotmatch '^[a-f0-9]{64}$') {
            throw "Unregistered operational recovery service."
        }
        $source = Resolve-OperationalRecoveryPath (Join-Path $project $service.script)
        if ((Get-OperationalRecoverySourceHash $source) -cne $service.source_sha256) {
            throw "Operational worker source changed: $($service.name)"
        }
        $expectedNeedle = if ($service.name -eq 'revision_news_collector_v2') {
            'oanda_local_news_sentiment.py*' + (Join-Path $data 'market_open_20260913_v1\local_news_sentiment')
        } elseif ($service.name -eq 'research_feature_forward_v2') {
            'oanda_feature_forward_worker_v1.py*' + (Join-Path $data 'operational_repair_20260913_v1\feature_forward_v3')
        } else { [string]$service.script }
        if ($service.needle -cne $expectedNeedle) { throw 'Operational worker identity is not exact.' }
        if (($service.max_age_sec -isnot [int] -and $service.max_age_sec -isnot [long]) -or
            $service.max_age_sec -lt 15 -or $service.max_age_sec -gt 900 -or
            ($service.startup_grace_sec -isnot [int] -and $service.startup_grace_sec -isnot [long]) -or
            $service.startup_grace_sec -lt 15 -or $service.startup_grace_sec -gt 1800 -or
            $service.arguments -isnot [System.Array] -or $service.arguments.Count -gt 40 -or
            @($service.arguments | Where-Object { $_ -isnot [string] -or $_ -match '[\r\n"]' }).Count -gt 0) {
            throw "Invalid bounded operational recovery parameters."
        }
        $heartbeat = Resolve-OperationalRecoveryPath ([string]$service.heartbeat)
        if (-not $heartbeat.StartsWith(($data + '\'), [StringComparison]::OrdinalIgnoreCase) -or
            [string]::IsNullOrWhiteSpace([string]$service.heartbeat_schema)) {
            throw "Operational heartbeat must be a declared project data path."
        }
    }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $digest = [BitConverter]::ToString($sha.ComputeHash($bytes)).Replace('-','').ToLowerInvariant() }
    finally { $sha.Dispose() }
    # Binding is computed from exactly the bytes parsed above; detect replacement.
    if ((Get-OperationalRecoverySourceHash $path) -cne $digest) {
        throw "Operational profile changed during validation."
    }
    return @{path=$path;sha256=$digest;schema=$OperationalRecoverySchema;
        expires_utc=$expiry.ToUniversalTime().ToString('o');services=$names;profile=$profile}
}

function Get-OperationalResearchSupervisorArguments {
    param([string]$Root,[string]$ProfilePath)
    $project = Resolve-OperationalRecoveryPath (Join-Path $Root 'trad')
    $profile = Resolve-OperationalRecoveryPath $ProfilePath
    return @('-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
        '-File',(Join-Path $project 'oanda_always_on_supervisor.ps1'),
        '-Root',[IO.Path]::GetFullPath($Root),'-SafeCoreOnly','-ResearchCollectionOnly',
        '-OperationalProfilePath',$profile,'-IntervalSec','30','-ChildDurationSec','604800')
}

function ConvertTo-OperationalProcessArgument([string]$Value) {
    if ($Value -match '[\r\n"]') { throw 'Unsafe process argument.' }
    if ($Value.Length -eq 0) { return '""' }
    if ($Value -match '\s') {
        # Windows command-line quoting doubles trailing slashes before quote.
        return '"' + ($Value -replace '(\\+)$','$1$1') + '"'
    }
    return $Value
}

function Test-OperationalSupervisorCommand {
    param([string]$CommandLine,[string]$ProfilePath)
    $tokens = @([regex]::Matches($CommandLine, '"[^"\r\n]*"|[^\s"]+') | ForEach-Object { $_.Value.Trim('"') })
    if ($tokens -inotcontains '-SafeCoreOnly' -or $tokens -inotcontains '-ResearchCollectionOnly') { return $false }
    $profiles = @()
    for ($i=0; $i -lt $tokens.Count-1; $i++) {
        if ($tokens[$i] -ieq '-OperationalProfilePath') { $profiles += $tokens[$i+1] }
    }
    if ($profiles.Count -ne 1) { return $false }
    return [IO.Path]::GetFullPath($profiles[0]) -ieq [IO.Path]::GetFullPath($ProfilePath)
}
