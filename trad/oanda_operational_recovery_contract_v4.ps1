# Pure validation/argument helpers. Dot-sourcing does not start or stop services.
$OperationalRecoverySchema = "forex_operational_runtime_v4_20260916"

function Get-OperationalRecoveryServices {
    return @{
        all68_m1_cadence_v2 = "oanda_all68_m1_cadence_v2.py"
        practice_quote_stream_v1 = "oanda_practice_quote_stream.py"
        clock_integrity_monitor_v1 = "oanda_clock_integrity_monitor.py"
        rolling_technical_operations_v2 = "oanda_rolling_technical_worker_v2.py"
        research_feature_forward_cached_v2 = "oanda_feature_forward_worker_v2.py"
        default_news_collector_v1 = "oanda_local_news_sentiment.py"
        revision_news_collector_v2 = "oanda_local_news_sentiment.py"
        local_news_sentiment_repair_v2 = "oanda_local_news_sentiment_repair_v2.py"
        revision_news_transport_v6 = "revision_transport_v6.py"
        joint_price_news_study_v9 = "oanda_joint_price_news_forecast_study_v9.py"
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

function Get-OperationalForwardNeedle {
    param([object]$Arguments,[string]$DataRoot)
    if ($Arguments -isnot [System.Array] -or $Arguments.Count -gt 40) {
        throw 'Bounded forward argument array required.'
    }
    $indexes = @()
    for ($index=0; $index -lt $Arguments.Count; $index++) {
        if ($Arguments[$index] -isnot [string]) { throw 'String forward arguments required.' }
        if ($Arguments[$index] -imatch '^--directory(?:=|$)') {
            if ($Arguments[$index] -cne '--directory') { throw 'Exact forward directory option required.' }
            $indexes += $index
        }
    }
    if ($indexes.Count -ne 1 -or $indexes[0] -ge $Arguments.Count-1) {
        throw 'Exactly one forward directory argument required.'
    }
    $directory = [string]$Arguments[$indexes[0]+1]
    if ([string]::IsNullOrWhiteSpace($directory) -or $directory -notmatch '^[a-zA-Z]:\\' -or
        $directory -match '[\x00-\x1F"*?\[\]]' -or $directory.Contains("'") -or $directory.Contains('`') -or
        $directory.Substring(2).Contains(':')) {
        throw 'Literal absolute forward directory required.'
    }
    $absolute = [IO.Path]::GetFullPath($directory)
    $data = [IO.Path]::GetFullPath($DataRoot).TrimEnd('\')
    if ($directory -cne $absolute -or
        -not $absolute.StartsWith(($data+'\'),[StringComparison]::OrdinalIgnoreCase)) {
        throw 'Normalized forward directory must remain within canonical project data.'
    }
    $current = $absolute
    while ($current) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'Forward directory contains a reparse point.'
            }
            if ($current -ceq $absolute -and -not $item.PSIsContainer) {
                throw 'Forward directory must be a directory.'
            }
        }
        $parent = [IO.Path]::GetDirectoryName($current)
        if ($parent -eq $current) { break }
        $current = $parent
    }
    return 'oanda_feature_forward_worker_v1.py*'+$absolute
}


function Read-OperationalRecoveryProfile {
    param([string]$Trad,[string]$ProfilePath,[string]$RecoveryUntilUtc,[DateTimeOffset]$Now = [DateTimeOffset]::UtcNow,[switch]$AllowExpired)
    $project = Resolve-OperationalRecoveryPath $Trad
    $path = Resolve-OperationalRecoveryPath $ProfilePath
    if (-not $path.StartsWith(($project + '\'), [StringComparison]::OrdinalIgnoreCase)) {
        throw "Operational profile must be inside the canonical project."
    }
    if ($RecoveryUntilUtc -notmatch '(Z|[+]00:00)$') { throw "Explicit UTC recovery expiry required." }
    $expiry = [DateTimeOffset]::Parse($RecoveryUntilUtc)
    if ($expiry -le $Now -and -not $AllowExpired) { throw "Operational recovery expiry reached." }
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
    if ([string]$profile.recovery_until_utc -cne $RecoveryUntilUtc) { throw "Profile and launcher recovery expiry differ." }
    $allowed = Get-OperationalRecoveryServices
    $services = @($profile.services)
    $names = @($services | ForEach-Object { [string]$_.name })
    if ($services.Count -ne $allowed.Count -or @($names | Sort-Object -Unique).Count -ne $allowed.Count) {
        throw "Operational recovery requires exactly sixteen distinct managed services."
    }
    $data = Join-Path $project 'data\oanda_training_manager'
    $allData = Join-Path $project 'data'
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
            Get-OperationalForwardNeedle -Arguments $service.arguments -DataRoot $data
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
        if ([string]$service.heartbeat_field -notin @('schema_version','worker','operations_schema')) {
            throw "Explicit supported heartbeat identity field required."
        }
        $null = Get-OperationalRolePriority -Service $service
        $expectedInterpreter = if ($service.name -eq 'revision_news_transport_v6') { 'cold_no_bytecode_unique_prefix' } else { 'no_bytecode' }
        if ([string]$service.interpreter_mode -cne $expectedInterpreter) { throw 'Exact operational interpreter mode required.' }
        $heartbeat = Resolve-OperationalRecoveryPath ([string]$service.heartbeat)
        if (-not $heartbeat.StartsWith(($allData + '\'), [StringComparison]::OrdinalIgnoreCase) -or
            [string]::IsNullOrWhiteSpace([string]$service.heartbeat_schema)) {
            throw "Operational heartbeat must be a declared project data path."
        }
    }
    $requiredSupport = @('oanda_operational_recovery_contract_v4.ps1','oanda_operational_supervisor_v4.ps1',
        'oanda_supervisor_watchdog_v4.ps1','start_oanda_operational_research_v4.ps1',
        'start_oanda_supervisor_watchdog_v4.ps1','oanda_operational_log_relay_v2.py')
    if (@($profile.support_sources.PSObject.Properties).Count -ne $requiredSupport.Count) { throw 'Exact recovery support source bindings required.' }
    foreach ($name in $requiredSupport) {
        $expected = [string]$profile.support_sources.$name
        if ($expected -cnotmatch '^[a-f0-9]{64}$' -or
            (Get-OperationalRecoverySourceHash (Resolve-OperationalRecoveryPath (Join-Path $project $name))) -cne $expected) {
            throw ('Operational recovery support source changed: '+$name)
        }
    }
    if ($null -eq $profile.config_dependencies -or @($profile.config_dependencies.PSObject.Properties).Count -gt 64) { throw 'Bounded configuration dependencies required.' }
    foreach ($property in $profile.config_dependencies.PSObject.Properties) {
        $dependency = Resolve-OperationalRecoveryPath (Join-Path $project $property.Name)
        if (-not $dependency.StartsWith((Join-Path $project 'config')+'\',[StringComparison]::OrdinalIgnoreCase) -or
            [IO.Path]::GetExtension($dependency) -ine '.json' -or
            [string]$property.Value -cnotmatch '^[a-f0-9]{64}$' -or
            (Get-OperationalRecoverySourceHash $dependency) -cne [string]$property.Value) { throw 'Operational configuration dependency changed.' }
    }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $digest = [BitConverter]::ToString($sha.ComputeHash($bytes)).Replace('-','').ToLowerInvariant() }
    finally { $sha.Dispose() }
    # Binding is computed from exactly the bytes parsed above; detect replacement.
    if ((Get-OperationalRecoverySourceHash $path) -cne $digest) {
        throw "Operational profile changed during validation."
    }
    return @{path=$path;sha256=$digest;schema=$OperationalRecoverySchema;
        expires_utc=$expiry.ToUniversalTime().ToString('o');services=$names;profile=$profile;recovery_allowed=($expiry -gt $Now)}
}

function Get-OperationalResearchSupervisorArguments {
    param([string]$Root,[string]$ProfilePath,[string]$RecoveryUntilUtc)
    $project = Resolve-OperationalRecoveryPath (Join-Path $Root 'trad')
    $profile = Resolve-OperationalRecoveryPath $ProfilePath
    return @('-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
        '-File',(Join-Path $project 'oanda_operational_supervisor_v4.ps1'),
        '-Root',[IO.Path]::GetFullPath($Root),'-SafeCoreOnly','-ResearchCollectionOnly',
        '-OperationalProfilePath',$profile,'-RecoveryUntilUtc',$RecoveryUntilUtc,'-IntervalSec','30','-ChildDurationSec','604800')
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
    $fileIndexes=@(for($i=0;$i -lt $tokens.Count-1;$i++) { if($tokens[$i] -ieq '-File') { $i } })
    if ($fileIndexes.Count -ne 1 -or $tokens[[int]$fileIndexes[0]+1] -ine (Join-Path $PSScriptRoot 'oanda_operational_supervisor_v4.ps1')) { return $false }
    if ($tokens -inotcontains '-SafeCoreOnly' -or $tokens -inotcontains '-ResearchCollectionOnly') { return $false }
    $profiles = @()
    for ($i=0; $i -lt $tokens.Count-1; $i++) {
        if ($tokens[$i] -ieq '-OperationalProfilePath') { $profiles += $tokens[$i+1] }
    }
    if ($profiles.Count -ne 1) { return $false }
    return [IO.Path]::GetFullPath($profiles[0]) -ieq [IO.Path]::GetFullPath($ProfilePath)
}

function Test-OperationalOwnerCommand {
    param([string]$CommandLine,[string]$Trad,[ValidateSet('supervisor','watchdog')][string]$Role)
    $names = if ($Role -eq 'supervisor') { @('oanda_always_on_supervisor.ps1','oanda_operational_supervisor_v2.ps1','oanda_operational_supervisor_v3.ps1','oanda_operational_supervisor_v4.ps1') }
        else { @('oanda_supervisor_watchdog.ps1','oanda_supervisor_watchdog_v2.ps1','oanda_supervisor_watchdog_v3.ps1','oanda_supervisor_watchdog_v4.ps1') }
    foreach ($name in $names) {
        $path=[regex]::Escape((Join-Path $Trad $name))
        # Recognize the old -Command invocation too; it is a conflict, never an
        # excuse to create a competing owner. Exact path boundaries are required.
        if ($CommandLine -match ('(?i)(?:^|\s|["''])(?:'+$path+')(?:["'']|\s|$)')) { return $true }
    }
    return $false
}

function Test-OperationalWorkerArguments {
    param([string]$CommandLine,[string]$ScriptPath,[string[]]$Arguments)
    $tokens=@([regex]::Matches($CommandLine, '"[^"\r\n]*"|[^\s"]+') | ForEach-Object { $_.Value.Trim('"') })
    $indexes=@(for($i=0;$i -lt $tokens.Count;$i++) { if ($tokens[$i] -ieq $ScriptPath) { $i } })
    if ($indexes.Count -ne 1) { return $false }
    $start=[int]$indexes[0]+1
    if ($tokens.Count-$start -ne $Arguments.Count) { return $false }
    for($i=0;$i -lt $Arguments.Count;$i++) { if($tokens[$start+$i] -cne $Arguments[$i]) { return $false } }
    return $true
}

function Test-OperationalRelativeWorkerCommand {
    param([string]$CommandLine,[string]$ScriptName)
    $tokens=@([regex]::Matches($CommandLine, '"[^"\r\n]*"|[^\s"]+') | ForEach-Object { $_.Value.Trim('"') })
    foreach($token in $tokens) {
        if ($token -match ('(?i)(?:^|[\\/])'+[regex]::Escape($ScriptName)+'$') -and
            -not [IO.Path]::IsPathRooted($token)) { return $true }
    }
    return $false
}

function Test-OperationalRelayCommand {
    param([string]$CommandLine,[string]$Trad)
    $tokens=@([regex]::Matches($CommandLine, '"[^"\r\n]*"|[^\s"]+') | ForEach-Object { $_.Value.Trim('"') })
    foreach($token in $tokens) {
        if ($token -imatch '\.py$') {
            return $token -ieq (Join-Path $Trad 'oanda_operational_log_relay_v1.py') -or
                $token -ieq (Join-Path $Trad 'oanda_operational_log_relay_v2.py')
        }
    }
    return $false
}

function Get-OperationalInterpreterArguments {
    param([object]$Service,[string]$DataRoot)
    if ($Service.interpreter_mode -ceq 'no_bytecode') { return @('-B') }
    if ($Service.name -cne 'revision_news_transport_v6' -or $Service.interpreter_mode -cne 'cold_no_bytecode_unique_prefix') { throw 'Unregistered cold interpreter requirement.' }
    $root = Resolve-OperationalRecoveryPath $DataRoot
    $prefix = Join-Path $root ('unused_bytecode_v2_'+[Guid]::NewGuid().ToString('N'))
    if (Test-Path -LiteralPath $prefix) { throw 'Cold bytecode prefix must not exist.' }
    return @('-B','-X',('pycache_prefix='+$prefix))
}

function Get-OperationalRolePriority {
    param([object]$Service)
    if ($null -eq $Service.PSObject.Properties['priority_class']) { return 'BelowNormal' }
    if ($Service.priority_class -isnot [string] -or $Service.priority_class -cnotin @('Normal','BelowNormal')) {
        throw 'Explicit operational priority must be Normal or BelowNormal.'
    }
    return [string]$Service.priority_class
}

function Get-OperationalRestartBudgetKey {
    param([string]$Name)
    # A successor role keeps its predecessor's durable launch history. Schema
    # and controller generation changes never grant a fresh retry allowance.
    if ($Name -ceq 'revision_news_transport_v6') { return 'revision_news_transport_v5' }
    if ($Name -ceq 'joint_price_news_study_v9') { return 'joint_price_news_study_v8' }
    return $Name
}

function Select-OperationalRoleCandidates {
    param([object[]]$Candidates,[string]$ServiceName,[string]$ScriptPath,[string[]]$Arguments,[object]$Profile)
    $owned=@();$conflicts=@();$otherDeclared=@()
    foreach($candidate in $Candidates) {
        $command=[string]$candidate.CommandLine
        if (Test-OperationalWorkerArguments $command $ScriptPath $Arguments) { $owned+=$candidate;continue }
        $declaredElsewhere=$false
        foreach($other in $Profile.services) {
            if ($other.name -eq $ServiceName -or $other.script -cne [IO.Path]::GetFileName($ScriptPath)) { continue }
            if (Test-OperationalWorkerArguments $command $ScriptPath @($other.arguments)) { $declaredElsewhere=$true;break }
        }
        if ($declaredElsewhere) { $otherDeclared+=$candidate } else { $conflicts+=$candidate }
    }
    return @{owned=$owned;conflicts=$conflicts;other_declared_roles=$otherDeclared}
}

function Stop-OperationalVerifiedSupervisor {
    param([object]$Process)
    $current=Get-CimInstance Win32_Process -Filter ('ProcessId = '+[int]$Process.ProcessId) -ErrorAction Stop
    if ($null -eq $current) { return $true }
    if ($current.Name -notmatch '^(?:powershell|pwsh)\.exe$' -or
        [datetime]$current.CreationDate -ne [datetime]$Process.CreationDate -or
        [string]$current.CommandLine -cne [string]$Process.CommandLine) { return $false }
    Stop-Process -Id ([int]$Process.ProcessId) -Force -ErrorAction Stop
    return $true
}


function Write-OperationalAtomicJson {
    param([string]$LiteralPath,[object]$Value)
    $path = Resolve-OperationalRecoveryPath $LiteralPath
    $directory = [IO.Path]::GetDirectoryName($path)
    [void][IO.Directory]::CreateDirectory($directory)
    $temporary = $path + '.' + [Guid]::NewGuid().ToString('N') + '.tmp'
    try {
        [IO.File]::WriteAllText($temporary,($Value | ConvertTo-Json -Depth 16 -Compress),[Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $temporary -Destination $path -Force
    } finally {
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force }
    }
}

function Write-OperationalBoundedEvent {
    param([string]$Directory,[string]$Prefix,[object]$Value,[long]$SegmentBytes=8388608,[long]$TotalBytes=134217728)
    if ($Prefix -cnotmatch '^[a-z0-9_]+$' -or $SegmentBytes -lt 1 -or $TotalBytes -lt $SegmentBytes) { throw 'Invalid diagnostic log limits.' }
    $directory = Resolve-OperationalRecoveryPath $Directory
    [void][IO.Directory]::CreateDirectory($directory)
    $files = @(Get-ChildItem -LiteralPath $directory -Filter ($Prefix+'_*.jsonl') -File | Sort-Object Name)
    $total = 0L
    foreach ($file in $files) {
        $null = Resolve-OperationalRecoveryPath $file.FullName
        $total += $file.Length
    }
    $line = ($Value | ConvertTo-Json -Depth 12 -Compress) + [Environment]::NewLine
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($line)
    if ($bytes.Length -gt $SegmentBytes -or $total + $bytes.Length -gt $TotalBytes) { return $false }
    $last = if ($files.Count) { $files[-1] } else { $null }
    if ($null -eq $last -or $last.Length + $bytes.Length -gt $SegmentBytes) {
        $target = Join-Path $directory ($Prefix+'_'+[DateTime]::UtcNow.ToString('yyyyMMdd_HHmmss_fffffff')+'_'+[Guid]::NewGuid().ToString('N')+'.jsonl')
    } else { $target = $last.FullName }
    $stream = [IO.File]::Open($target,[IO.FileMode]::Append,[IO.FileAccess]::Write,[IO.FileShare]::Read)
    try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush() } finally { $stream.Dispose() }
    return $true
}

function Get-OperationalRestartDecision {
    param([object[]]$Attempts,[double]$NowEpoch,[int]$WindowSeconds=1800,[int]$Limit=3)
    if ([double]::IsNaN($NowEpoch) -or [double]::IsInfinity($NowEpoch) -or $NowEpoch -le 0) { throw 'Invalid recovery clock.' }
    $recent = @()
    foreach ($attempt in $Attempts) {
        $value = [double]$attempt
        if ([double]::IsNaN($value) -or [double]::IsInfinity($value) -or $value -le 0) { throw 'Invalid retained recovery clock.' }
        if ($value -gt $NowEpoch + 1) { return @{allowed=$false;reason='recovery_clock_regressed';attempts=@($Attempts)} }
        if ($value -gt $NowEpoch-$WindowSeconds) { $recent += $value }
    }
    return @{allowed=($recent.Count -lt $Limit);reason=$(if($recent.Count -ge $Limit){'restart_circuit_open'}else{'allowed'});attempts=$recent}
}

function Test-OperationalSupervisorIdentity {
    param([object]$Heartbeat,[int]$ProcessId,[string]$ProfileHash,[double]$NowEpoch,[int]$MaxAgeSec=150)
    try {
        $at = [DateTimeOffset]::Parse([string]$Heartbeat.generated_utc).ToUnixTimeMilliseconds()/1000.0
        return ($Heartbeat.schema_version -ceq 'operational_supervisor_v4_20260916' -and
            [int]$Heartbeat.supervisor_pid -eq $ProcessId -and
            [string]$Heartbeat.operational_profile_sha256 -ceq $ProfileHash -and
            $at -le $NowEpoch+1 -and $NowEpoch-$at -le $MaxAgeSec)
    } catch { return $false }
}
