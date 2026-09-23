param(
    [int]$DurationHours = 4,
    [int]$IntervalSeconds = 45,
    [int]$StaleMinutes = 8
)

$ErrorActionPreference = "Continue"
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSCommandPath
$Python = Join-Path $Root "data\oanda_training_manager\.research_py313\Scripts\python.exe"
$BotScript = Join-Path $Root "oanda_primary_forecast_rotation_bot.py"
$MonitorScript = Join-Path $Root "primary_forecast_rotation_monitor.py"
$Config = Join-Path $Root "config\gpt_orig_constant_rotation_demo.json"
$DataDir = Join-Path $Root "data\forex_gpt_manager\account_gpt_orig_constant_rotation_demo"
$RuntimeDir = Join-Path $Root "data\runtime_logs"
$StartedAt = Get-Date
$Stamp = $StartedAt.ToString("yyyyMMdd_HHmmss")
$WatchLog = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_watchdog_$Stamp.log"
$LatestJson = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_watchdog_latest.json"
$BotLaunchJson = Join-Path $DataDir "latest_launch.json"
$MonitorLaunchJson = Join-Path $DataDir "latest_monitor_launch.json"
$LatestDecision = Join-Path $DataDir "latest_decision.json"
$MonitorLatest = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_monitor_latest.json"

New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

function Write-WatchLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $WatchLog -Value $line
}

function Get-DemoProcesses {
    param([string]$Kind)
    $launchPath = if ($Kind -eq "monitor") { $MonitorLaunchJson } else { $BotLaunchJson }
    $launch = Read-JsonFile -Path $launchPath
    if ($null -eq $launch -or -not $launch.pid) {
        return @()
    }
    try {
        $proc = Get-Process -Id ([int]$launch.pid) -ErrorAction SilentlyContinue
        if ($proc) {
            return @([pscustomobject]@{ ProcessId = $proc.Id })
        }
    } catch {
        Write-WatchLog ("process_lookup_failed kind={0} pid={1} error={2}" -f $Kind, $launch.pid, $_.Exception.Message)
    }
    return @()
}

function Stop-DemoProcesses {
    param([string]$Kind)
    foreach ($proc in Get-DemoProcesses -Kind $Kind) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
            Write-WatchLog ("stopped kind={0} pid={1}" -f $Kind, $proc.ProcessId)
        } catch {
            Write-WatchLog ("stop_failed kind={0} pid={1} error={2}" -f $Kind, $proc.ProcessId, $_.Exception.Message)
        }
    }
}

function Get-FileAgeMinutes {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $item = Get-Item -LiteralPath $Path
    return [math]::Round(((Get-Date) - $item.LastWriteTime).TotalMinutes, 2)
}

function Get-IsoAgeMinutes {
    param([string]$Value)
    if (-not $Value) {
        return $null
    }
    try {
        $parsed = [datetime]::Parse($Value)
        return [math]::Round(((Get-Date).ToUniversalTime() - $parsed.ToUniversalTime()).TotalMinutes, 2)
    } catch {
        return $null
    }
}

function Read-JsonFile {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    try {
        return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json)
    } catch {
        Write-WatchLog ("json_read_failed path={0} error={1}" -f $Path, $_.Exception.Message)
        return $null
    }
}

function Start-DemoBot {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $out = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_$stamp.stdout.log"
    $err = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_$stamp.stderr.log"
    $args = @($BotScript, "--config", $Config, "--mode", "demo", "--source", "oanda", "--execute")
    $env:PYTHONUTF8 = "1"
    $proc = Start-Process -FilePath $Python -ArgumentList $args -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
    [ordered]@{
        pid = $proc.Id
        started_utc = [DateTime]::UtcNow.ToString("o")
        python = $Python
        script = $BotScript
        config = $Config
        data_dir = $DataDir
        stdout = $out
        stderr = $err
        mode = "demo"
        source = "oanda"
        execute = $true
        started_by = "gpt_orig_constant_rotation_demo_watchdog"
    } | ConvertTo-Json | Set-Content -LiteralPath $BotLaunchJson -Encoding UTF8
    $proc.Id | Set-Content -LiteralPath (Join-Path $DataDir "latest.pid") -Encoding ASCII
    Write-WatchLog ("bot_started pid={0} out={1} err={2}" -f $proc.Id, $out, $err)
}

function Start-DemoMonitor {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $out = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_monitor_$stamp.stdout.log"
    $err = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_monitor_$stamp.stderr.log"
    $jsonl = Join-Path $RuntimeDir "gpt_orig_constant_rotation_demo_monitor_$stamp.jsonl"
    $durationSeconds = [math]::Max(3600, $DurationHours * 3600 + 600)
    $args = @(
        $MonitorScript,
        "--duration-seconds", [string]$durationSeconds,
        "--interval-seconds", "30",
        "--data-dir", $DataDir,
        "--config", $Config,
        "--output", $jsonl,
        "--latest-output", $MonitorLatest
    )
    $env:PYTHONUTF8 = "1"
    $proc = Start-Process -FilePath $Python -ArgumentList $args -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
    [ordered]@{
        pid = $proc.Id
        started_utc = [DateTime]::UtcNow.ToString("o")
        duration_seconds = $durationSeconds
        interval_seconds = 30
        output = $jsonl
        latest = $MonitorLatest
        stdout = $out
        stderr = $err
        started_by = "gpt_orig_constant_rotation_demo_watchdog"
    } | ConvertTo-Json | Set-Content -LiteralPath $MonitorLaunchJson -Encoding UTF8
    Write-WatchLog ("monitor_started pid={0} out={1} err={2}" -f $proc.Id, $out, $err)
}

function Get-DecisionSummary {
    $decision = Read-JsonFile -Path $LatestDecision
    if ($null -eq $decision) {
        return $null
    }
    return [ordered]@{
        generated_utc = $decision.generated_utc
        instruments_requested = $decision.meta.instruments_requested
        instruments_loaded = $decision.meta.instruments_loaded
        tradable_forecast_count = $decision.meta.tradable_forecast_count
        live_open_trade_count = $decision.live_open_trade_count
        actions = @($decision.actions | ForEach-Object {
            "{0}:{1}:{2}:{3}:{4}:{5}" -f $_.action, $_.status, $_.instrument, $_.units, $_.reason, $_.broker_cancel_reason
        })
    }
}

function Get-MonitorSummary {
    $monitor = Read-JsonFile -Path $MonitorLatest
    if ($null -eq $monitor) {
        return $null
    }
    return [ordered]@{
        snapshot_utc = $monitor.snapshot_utc
        flags = $monitor.flags
        margin_used_pct = $monitor.safety.margin_used_pct
    }
}

$endAt = $StartedAt.AddHours($DurationHours)
Write-WatchLog ("watchdog_started duration_hours={0} interval_seconds={1} stale_minutes={2} end_at={3}" -f $DurationHours, $IntervalSeconds, $StaleMinutes, $endAt.ToString("o"))

while ((Get-Date) -lt $endAt) {
    $botProcs = @(Get-DemoProcesses -Kind "bot")
    $monitorProcs = @(Get-DemoProcesses -Kind "monitor")
    $decisionAge = Get-FileAgeMinutes -Path $LatestDecision
    $monitorAge = Get-FileAgeMinutes -Path $MonitorLatest
    $botLaunch = Read-JsonFile -Path $BotLaunchJson
    $monitorLaunch = Read-JsonFile -Path $MonitorLaunchJson
    $botStartAge = if ($botLaunch) { Get-IsoAgeMinutes -Value $botLaunch.started_utc } else { $null }
    $monitorStartAge = if ($monitorLaunch) { Get-IsoAgeMinutes -Value $monitorLaunch.started_utc } else { $null }
    $status = "ok"
    $reasons = @()

    if ($botProcs.Count -eq 0) {
        $status = "restarted"
        $reasons += "bot_missing"
        Start-DemoBot
        Start-Sleep -Seconds 2
        $botProcs = @(Get-DemoProcesses -Kind "bot")
    } elseif (($null -eq $decisionAge -or $decisionAge -gt $StaleMinutes) -and ($null -eq $botStartAge -or $botStartAge -gt $StaleMinutes)) {
        $status = "restarted"
        $reasons += "decision_stale"
        Stop-DemoProcesses -Kind "bot"
        Start-Sleep -Seconds 2
        Start-DemoBot
        Start-Sleep -Seconds 2
        $botProcs = @(Get-DemoProcesses -Kind "bot")
        $decisionAge = Get-FileAgeMinutes -Path $LatestDecision
        $botLaunch = Read-JsonFile -Path $BotLaunchJson
        $botStartAge = if ($botLaunch) { Get-IsoAgeMinutes -Value $botLaunch.started_utc } else { $null }
    } elseif ($null -eq $decisionAge -or $decisionAge -gt $StaleMinutes) {
        $status = "warming"
        $reasons += "decision_stale_startup_grace"
    }

    if ($monitorProcs.Count -eq 0) {
        if ($status -eq "ok") { $status = "restarted" }
        $reasons += "monitor_missing"
        Start-DemoMonitor
        Start-Sleep -Seconds 2
        $monitorProcs = @(Get-DemoProcesses -Kind "monitor")
    } elseif (($null -eq $monitorAge -or $monitorAge -gt $StaleMinutes) -and ($null -eq $monitorStartAge -or $monitorStartAge -gt $StaleMinutes)) {
        if ($status -eq "ok") { $status = "restarted" }
        $reasons += "monitor_stale"
        Stop-DemoProcesses -Kind "monitor"
        Start-Sleep -Seconds 2
        Start-DemoMonitor
        Start-Sleep -Seconds 2
        $monitorProcs = @(Get-DemoProcesses -Kind "monitor")
        $monitorAge = Get-FileAgeMinutes -Path $MonitorLatest
        $monitorLaunch = Read-JsonFile -Path $MonitorLaunchJson
        $monitorStartAge = if ($monitorLaunch) { Get-IsoAgeMinutes -Value $monitorLaunch.started_utc } else { $null }
    } elseif ($null -eq $monitorAge -or $monitorAge -gt $StaleMinutes) {
        if ($status -eq "ok") { $status = "warming" }
        $reasons += "monitor_stale_startup_grace"
    }

    if ($reasons.Count -eq 0) {
        $reasons += "healthy"
    }

    $snapshot = [ordered]@{
        time = (Get-Date).ToString("o")
        endAt = $endAt.ToString("o")
        status = $status
        reasons = $reasons
        botPids = @($botProcs | ForEach-Object { $_.ProcessId })
        monitorPids = @($monitorProcs | ForEach-Object { $_.ProcessId })
        botStartAgeMinutes = $botStartAge
        monitorStartAgeMinutes = $monitorStartAge
        decisionAgeMinutes = $decisionAge
        monitorAgeMinutes = $monitorAge
        latestDecision = Get-DecisionSummary
        latestMonitor = Get-MonitorSummary
    }
    $snapshot | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $LatestJson -Encoding UTF8
    Write-WatchLog ("snapshot status={0} reasons={1} bot_pids={2} monitor_pids={3} decision_age={4} monitor_age={5}" -f $status, ($reasons -join ","), (($snapshot.botPids) -join ","), (($snapshot.monitorPids) -join ","), $decisionAge, $monitorAge)
    Start-Sleep -Seconds ([math]::Max(10, $IntervalSeconds))
}

Write-WatchLog "watchdog_finished"
