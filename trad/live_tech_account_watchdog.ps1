$ErrorActionPreference = "Continue"

$Root = "D:\forex\trad"
$Python = "C:\Users\zmoor\AppData\Roaming\uv\python\cpython-3.12-windows-x86_64-none\python.exe"
$SitePackages = Join-Path $Root "..venv\Lib\site-packages"
$RuntimeLogs = Join-Path $Root "data\runtime_logs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$WatchLog = Join-Path $RuntimeLogs "live_tech_account_watchdog_${Stamp}.log"
$SummaryJson = Join-Path $RuntimeLogs "live_tech_account_watchdog_latest.json"
$ConfiguredEndAt = $env:LIVE_TECH_WATCHDOG_END_AT
$DurationHours = 4
$IntervalSeconds = 120
$StaleMinutes = 20

New-Item -ItemType Directory -Force -Path $RuntimeLogs | Out-Null

function Write-WatchLog {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date).ToString("s"), $Message
    Add-Content -LiteralPath $WatchLog -Value $line
}

function Get-TechProcess {
    try {
        Get-CimInstance Win32_Process -ErrorAction Stop |
            Where-Object {
                $_.Name -match '^python(\.exe)?$' -and
                (
                    $_.CommandLine -like "*oanda_tech_prod_live_account_manager.py*" -or
                    $_.CommandLine -like "*run_oanda_tech_prod_live_account_manager.py*"
                )
            } |
            Select-Object -First 1
    } catch {
        Write-WatchLog ("process_query_failed error={0}" -f $_.Exception.Message)
        return [pscustomobject]@{
            ProcessId = $null
            QueryFailed = $true
            Error = $_.Exception.Message
        }
    }
}

function Get-StateAgeMinutes {
    $path = Join-Path $Root "data\technical_scout_manager\account_live_tech_broad_regime_scout\state.json"
    if (-not (Test-Path -LiteralPath $path)) {
        return $null
    }
    $item = Get-Item -LiteralPath $path
    return [math]::Round(((Get-Date) - $item.LastWriteTime).TotalMinutes, 2)
}

function Get-LatestMonitorSnapshot {
    $path = Join-Path $Root "data\technical_scout_manager\account_live_tech_broad_regime_scout\monitor.csv"
    if (-not (Test-Path -LiteralPath $path)) {
        return $null
    }
    try {
        $header = Get-Content -LiteralPath $path -TotalCount 1
        $line = Get-Content -LiteralPath $path -Tail 1
        if (-not $header -or -not $line -or $line -eq $header) {
            return $null
        }
        $row = $line | ConvertFrom-Csv -Header ($header -split ',')
    } catch {
        return $null
    }

    $raw = $null
    try {
        $raw = $row.raw_json | ConvertFrom-Json
    } catch {
        $raw = $null
    }

    $unprotected = @()
    $tradeSummary = @()
    if ($raw -and $raw.open_trades) {
        foreach ($trade in $raw.open_trades) {
            $missing = @()
            if (-not $trade.stop_loss_order) { $missing += "STOP_LOSS" }
            if (-not $trade.take_profit_order) { $missing += "TAKE_PROFIT" }
            if (-not $trade.trailing_stop_loss_order) { $missing += "TRAILING_STOP_LOSS" }
            if ($missing.Count -gt 0) {
                $unprotected += $trade.id
            }
            $tradeSummary += ("{0} {1} uPL={2}" -f $trade.instrument, $trade.current_units, $trade.unrealized_pl)
        }
    }

    $pendingOrderCount = $null
    $unrealizedPl = $null
    if ($raw -and $raw.account) {
        $pendingOrderCount = $raw.account.pending_order_count
        $unrealizedPl = $raw.account.unrealized_pl
    }

    return [ordered]@{
        timeUtc = $row.time_utc
        timeNy = $row.time_ny
        nav = $row.nav
        balance = $row.balance
        marginUsed = $row.margin_used
        marginUsedPct = $row.margin_used_pct
        openTradeCount = $row.open_trade_count
        pendingOrderCount = $pendingOrderCount
        unrealizedPl = $unrealizedPl
        decision = $row.decision
        reason = $row.reason
        unprotectedTradeIds = $unprotected
        trades = $tradeSummary
    }
}

function Start-TechLane {
    $scriptPath = Join-Path $Root "run_oanda_tech_prod_live_account_manager.py"
    $out = Join-Path $RuntimeLogs ("tech_watchdog_restart_{0}.out.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
    $err = Join-Path $RuntimeLogs ("tech_watchdog_restart_{0}.err.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
    $env:TRAD_PROJECT_ROOT = $Root
    $env:PYTHONUTF8 = "1"
    $proc = Start-Process -FilePath $Python -ArgumentList @("-S", $scriptPath, "--no-scan-on-launch") -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
    Write-WatchLog ("restart_started pid={0} out={1} err={2}" -f $proc.Id, $out, $err)
}

$endAt = if ($ConfiguredEndAt) {
    [datetime]::Parse($ConfiguredEndAt)
} else {
    (Get-Date).AddHours($DurationHours)
}
Write-WatchLog ("tech_watchdog_started interval_seconds={0} stale_minutes={1} end_at={2}" -f $IntervalSeconds, $StaleMinutes, $endAt.ToString("o"))

while ((Get-Date) -lt $endAt) {
    $proc = Get-TechProcess
    $age = Get-StateAgeMinutes
    $monitor = Get-LatestMonitorSnapshot
    $status = "ok"
    $reason = ""
    $processQueryFailed = $proc -and ($proc.PSObject.Properties.Name -contains "QueryFailed") -and $proc.QueryFailed

    if ($processQueryFailed) {
        if ($null -eq $age) {
            $status = "missing_state"
            $reason = "process query failed and state file missing: $($proc.Error)"
        } elseif ($age -gt $StaleMinutes) {
            $status = "stale"
            $reason = "process query failed and state age ${age}m > ${StaleMinutes}m: $($proc.Error)"
            Start-TechLane
        } else {
            $status = "ok"
            $reason = "process query unavailable; state fresh"
        }
    } elseif ($null -eq $proc) {
        $status = "missing"
        $reason = "tech live process not found"
        Start-TechLane
    } elseif ($null -eq $age) {
        $status = "missing_state"
        $reason = "state file missing"
    } elseif ($age -gt $StaleMinutes) {
        $status = "stale"
        $reason = "state age ${age}m > ${StaleMinutes}m"
    }

    if ($status -eq "ok") {
        $navText = if ($monitor) { $monitor.nav } else { "" }
        $openText = if ($monitor) { $monitor.openTradeCount } else { "" }
        $pidText = if ($proc) { $proc.ProcessId } else { "" }
        Write-WatchLog ("ok pid={0} state_age_minutes={1} nav={2} open={3} reason={4}" -f $pidText, $age, $navText, $openText, $reason)
    } else {
        Write-WatchLog ("issue status={0} reason={1} pid={2} state_age_minutes={3}" -f $status, $reason, $(if ($proc) { $proc.ProcessId } else { "" }), $age)
    }

    [ordered]@{
        time = (Get-Date).ToString("o")
        endAt = $endAt.ToString("o")
        script = "oanda_tech_prod_live_account_manager.py"
        pid = if ($proc) { $proc.ProcessId } else { $null }
        stateAgeMinutes = $age
        staleLimitMinutes = $StaleMinutes
        status = $status
        reason = $reason
        monitor = $monitor
    } | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $SummaryJson -Encoding UTF8

    Start-Sleep -Seconds $IntervalSeconds
}

Write-WatchLog "tech_watchdog_finished"
