param(
    [int]$DurationHours = 10,
    [int]$IntervalSeconds = 180,
    [int]$MaxCycles = 0
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSCommandPath
$RuntimeDir = Join-Path $Root 'data\runtime_logs'
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null

$StartedAt = Get-Date
$Stamp = $StartedAt.ToString('yyyyMMdd_HHmmss')
$LogPath = Join-Path $RuntimeDir "gpt_live_10h_watchdog_$Stamp.jsonl"
$LatestPath = Join-Path $RuntimeDir 'gpt_live_10h_watchdog_latest.json'
$ManagerPidPath = Join-Path $RuntimeDir 'gpt_live_manager.pid'
$StdoutPath = Join-Path $RuntimeDir "gpt_live_10h_watchdog_manager_$Stamp.stdout.log"
$StderrPath = Join-Path $RuntimeDir "gpt_live_10h_watchdog_manager_$Stamp.stderr.log"

$PythonCandidates = @(
    (Join-Path $env:APPDATA 'uv\python\cpython-3.12-windows-x86_64-none\python.exe'),
    (Join-Path $Root '..venv\Scripts\python.exe'),
    'python'
)

$Python = $PythonCandidates | Where-Object {
    try { Get-Command $_ -ErrorAction Stop | Out-Null; $true } catch { $false }
} | Select-Object -First 1

if (-not $Python) {
    throw 'No Python executable found for GPT live watchdog.'
}

$Runner = "import runpy,sys; root=r'$Root'; site=r'$Root\..venv\Lib\site-packages'; script=sys.argv[1]; sys.path.insert(0,root); sys.path.append(site); sys.argv=[script]+sys.argv[2:]; runpy.run_path(script,run_name='__main__')"
$ManagerScript = Join-Path $Root 'oanda_gpt_prod_live_account_manager.py'
$StatusScript = Join-Path $Root 'oanda_live_account_readonly_status.py'
$DataDir = Join-Path $Root 'data\forex_gpt_manager\account_gpt_prod_live'
$CallTimesNy = @(
    '00:15', '01:45', '03:15', '04:45',
    '06:15', '07:45', '09:15', '10:45',
    '12:15', '13:45', '15:15', '16:45',
    '18:15', '19:45', '21:15', '22:45'
)
$FridayCallTimesNy = @(
    '00:15', '01:45', '03:15', '04:45',
    '06:15', '07:45', '09:15', '10:45',
    '12:15', '13:45', '14:45', '15:45'
)
$ScheduleWindowMinutes = 10
$ScheduleMissGraceMinutes = 8
$MinMinutesBetweenGptScans = 75

function Get-GptManagerProcess {
    $tracked = @()
    if (Test-Path $ManagerPidPath) {
        try {
            $pidText = [string](Get-Content $ManagerPidPath -Raw)
            $pidValue = [int]($pidText.Trim())
            $process = Get-Process -Id $pidValue -ErrorAction Stop
            $tracked += [pscustomobject]@{
                ProcessId = $process.Id
                Name = $process.ProcessName
            }
        } catch {
            Remove-Item -LiteralPath $ManagerPidPath -Force -ErrorAction SilentlyContinue
        }
    }
    if ($tracked.Count -gt 0) {
        return @($tracked)
    }
    try {
        return @(Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match 'python' -and $_.CommandLine -like '*oanda_gpt_prod_live_account_manager.py*'
        })
    } catch {
        return @()
    }
}

function Start-GptManager {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $Python
    $psi.WorkingDirectory = $Root
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $runnerArg = $Runner.Replace('"', '\"')
    $managerArg = $ManagerScript.Replace('"', '\"')
    $psi.Arguments = "-S -c `"$runnerArg`" `"$managerArg`" --scan-on-launch-live"
    $proc = [System.Diagnostics.Process]::Start($psi)
    Set-Content -Path $ManagerPidPath -Value ([string]$proc.Id)
    Start-Sleep -Seconds 4
    return $proc.Id
}

function Invoke-RepoScriptJson {
    param(
        [string]$ScriptPath,
        [string[]]$ScriptArgs
    )
    $output = & $Python -S -c $Runner $ScriptPath @ScriptArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Script failed with exit code $LASTEXITCODE`: $ScriptPath"
    }
    return ($output | Out-String | ConvertFrom-Json)
}

function Get-LatestCsvRow {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        return $null
    }
    $rows = @(Import-Csv $Path)
    if ($rows.Count -eq 0) {
        return $null
    }
    return $rows[-1]
}

function Convert-TradeSummary {
    param($Trades)
    return ,@($Trades | ForEach-Object {
        [pscustomobject]@{
            id = $_.id
            instrument = $_.instrument
            units = $_.units
            price = $_.price
            unrealizedPL = $_.unrealizedPL
            protection = $_.protection
            missingProtection = $_.missingProtection
        }
    })
}

function Convert-JsonArray {
    param($Value)
    if ($null -eq $Value) {
        return ,@()
    }
    if ($Value -is [System.Array]) {
        return ,@($Value)
    }
    if ($Value -is [pscustomobject] -and @($Value.PSObject.Properties).Count -eq 0) {
        return ,@()
    }
    return ,@($Value)
}

function Get-JsonProperty {
    param(
        $Object,
        [string]$Name
    )
    if ($null -eq $Object) {
        return $null
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Convert-OrderSummary {
    param($Orders)
    return ,@($Orders | ForEach-Object {
        [pscustomobject]@{
            instrument = Get-JsonProperty -Object $_ -Name 'instrument'
            action = Get-JsonProperty -Object $_ -Name 'action'
            direction = Get-JsonProperty -Object $_ -Name 'direction'
            outlook_confidence = Get-JsonProperty -Object $_ -Name 'outlook_confidence'
            risk_pct = Get-JsonProperty -Object $_ -Name 'risk_pct'
            entry_min = Get-JsonProperty -Object $_ -Name 'entry_min'
            entry_max = Get-JsonProperty -Object $_ -Name 'entry_max'
            stop_loss = Get-JsonProperty -Object $_ -Name 'stop_loss'
            take_profit = Get-JsonProperty -Object $_ -Name 'take_profit'
            trailing_stop_pips = Get-JsonProperty -Object $_ -Name 'trailing_stop_pips'
            expected_R = Get-JsonProperty -Object $_ -Name 'expected_R'
            reason = Get-JsonProperty -Object $_ -Name 'reason'
        }
    })
}

function Get-CallTimesForDate {
    param([datetime]$Date)
    if ($Date.DayOfWeek -eq [System.DayOfWeek]::Friday) {
        return ,$FridayCallTimesNy
    }
    return ,$CallTimesNy
}

function Get-ScheduledScanTimesAround {
    param([datetime]$Now)
    $times = New-Object System.Collections.Generic.List[datetime]
    foreach ($offset in -1..1) {
        $date = $Now.Date.AddDays($offset)
        foreach ($hhmm in (Get-CallTimesForDate -Date $date)) {
            $parts = $hhmm.Split(':')
            $times.Add([datetime]::new($date.Year, $date.Month, $date.Day, [int]$parts[0], [int]$parts[1], 0))
        }
    }
    return ,@($times | Sort-Object)
}

function Get-GptScheduleHealth {
    param(
        [datetime]$Now,
        $Decision
    )
    $latestDecisionLocal = $null
    if ($Decision -and $Decision.time_ny) {
        try {
            $latestDecisionLocal = ([datetimeoffset]::Parse([string]$Decision.time_ny)).LocalDateTime
        } catch {
            $latestDecisionLocal = $null
        }
    }

    $scheduleTimes = Get-ScheduledScanTimesAround -Now $Now
    $lastSlot = @($scheduleTimes | Where-Object { $_ -le $Now } | Select-Object -Last 1) | Select-Object -First 1
    $nextSlot = @($scheduleTimes | Where-Object { $_ -gt $Now } | Select-Object -First 1) | Select-Object -First 1

    $lastCovered = $false
    $recentScanSuppressesSlot = $false
    $minutesSinceLatest = $null
    if ($latestDecisionLocal) {
        $minutesSinceLatest = [math]::Round(($Now - $latestDecisionLocal).TotalMinutes, 2)
        if ($lastSlot) {
            $lastCovered = $latestDecisionLocal -ge $lastSlot
            $recentScanSuppressesSlot = (
                $latestDecisionLocal -lt $lastSlot -and
                (($lastSlot - $latestDecisionLocal).TotalMinutes -lt $MinMinutesBetweenGptScans)
            )
        }
    }

    $missThreshold = if ($lastSlot) { $lastSlot.AddMinutes($ScheduleWindowMinutes + $ScheduleMissGraceMinutes) } else { $null }
    $missed = $false
    $reason = 'no_due_slot'
    if ($lastSlot) {
        if ($lastCovered) {
            $reason = 'last_slot_covered'
        } elseif ($recentScanSuppressesSlot) {
            $reason = 'recent_scan_within_min_gap'
        } elseif ($missThreshold -and $Now -ge $missThreshold) {
            $missed = $true
            $reason = 'last_slot_overdue_without_scan'
        } else {
            $reason = 'inside_schedule_window_or_grace'
        }
    }

    return [pscustomobject]@{
        latest_decision_ny = if ($latestDecisionLocal) { $latestDecisionLocal.ToString('o') } else { $null }
        minutes_since_latest_decision = $minutesSinceLatest
        last_scheduled_ny = if ($lastSlot) { $lastSlot.ToString('o') } else { $null }
        next_scheduled_ny = if ($nextSlot) { $nextSlot.ToString('o') } else { $null }
        miss_threshold_ny = if ($missThreshold) { $missThreshold.ToString('o') } else { $null }
        last_slot_covered = $lastCovered
        recent_scan_suppresses_slot = $recentScanSuppressesSlot
        missed = $missed
        reason = $reason
    }
}

function Convert-ToLocalDateTime {
    param([string]$Value)
    if (-not $Value) {
        return $null
    }
    try {
        return ([datetimeoffset]::Parse($Value)).LocalDateTime
    } catch {
        return $null
    }
}

function Convert-ToIntSafe {
    param($Value)
    try {
        return [int]$Value
    } catch {
        return 0
    }
}

$EndAt = $StartedAt.AddHours($DurationHours)
$cycle = 0

while ((Get-Date) -lt $EndAt) {
    $cycle += 1
    $now = Get-Date
    $alerts = New-Object System.Collections.Generic.List[string]
    $restarted = $false
    $restartPid = $null

    try {
        $manager = @(Get-GptManagerProcess)
        if ($manager.Count -eq 0) {
            $alerts.Add('gpt_manager_not_running_restarted')
            $restartPid = Start-GptManager
            $restarted = $true
            $manager = @(Get-GptManagerProcess)
        }

        $statusRows = @(Invoke-RepoScriptJson -ScriptPath $StatusScript -ScriptArgs @('--json', '--role', 'gpt_live', '--required-protection', 'hard'))
        $status = $statusRows | Select-Object -First 1
        if (-not $status -or -not $status.ok) {
            $alerts.Add('gpt_status_not_ok')
        }
        $unprotectedTradeIds = if ($status) { Convert-JsonArray -Value $status.unprotectedTradeIds } else { @() }
        if ($unprotectedTradeIds.Count -gt 0) {
            $alerts.Add('gpt_trade_missing_hard_protection')
        }

        $decision = Get-LatestCsvRow -Path (Join-Path $DataDir 'decisions.csv')
        $action = Get-LatestCsvRow -Path (Join-Path $DataDir 'actions.csv')
        $order = Get-LatestCsvRow -Path (Join-Path $DataDir 'order_result_ledger.csv')
        $scheduleHealth = Get-GptScheduleHealth -Now $now -Decision $decision
        if ($scheduleHealth.missed) {
            $alerts.Add('gpt_scheduled_scan_overdue')
        }
        $rawDecision = $null
        if ($decision -and $decision.raw_path -and (Test-Path $decision.raw_path)) {
            try {
                $rawDecision = Get-Content $decision.raw_path -Raw | ConvertFrom-Json
            } catch {
                $alerts.Add('latest_decision_raw_parse_failed')
            }
        } elseif ($decision -and $decision.raw_path) {
            $alerts.Add('latest_decision_raw_missing')
        }
        $decisionActionCount = if ($decision) { Convert-ToIntSafe -Value $decision.action_count } else { 0 }
        $decisionTimeLocal = if ($decision) { Convert-ToLocalDateTime -Value $decision.time_ny } else { $null }
        $actionTimeLocal = if ($action) { Convert-ToLocalDateTime -Value $action.time_ny } else { $null }
        $latestActionAfterDecision = $false
        if ($decisionTimeLocal -and $actionTimeLocal) {
            $latestActionAfterDecision = $actionTimeLocal -ge $decisionTimeLocal
        }
        if ($decisionActionCount -gt 0 -and -not $latestActionAfterDecision) {
            $alerts.Add('latest_decision_action_missing_after_decision')
        }
        if ($latestActionAfterDecision -and $action.status -and [string]$action.status -notin @('accepted', 'logged', 'skipped')) {
            $alerts.Add("latest_action_status_$($action.status)")
        }
        $qualityReview = Get-JsonProperty -Object $rawDecision -Name 'live_decision_quality_review'
        $missedEntryFallback = Get-JsonProperty -Object $rawDecision -Name 'live_missed_entry_fallback'
        if ($qualityReview) {
            $qualityIssueCount = Convert-ToIntSafe -Value (Get-JsonProperty -Object $qualityReview -Name 'issue_count')
            $promotedOrder = Get-JsonProperty -Object $missedEntryFallback -Name 'promoted_order'
            $fallbackPromoted = [bool]$promotedOrder
            if ($qualityIssueCount -gt 0 -and -not $fallbackPromoted -and $decisionActionCount -eq 0) {
                $alerts.Add('unresolved_live_decision_quality_issue')
            }
        }

        $record = [pscustomobject]@{
            time_local = $now.ToString('o')
            cycle = $cycle
            started_at = $StartedAt.ToString('o')
            end_at = $EndAt.ToString('o')
            manager_pids = @($manager | Select-Object -ExpandProperty ProcessId)
            restarted_manager = $restarted
            restarted_pid = $restartPid
            status_ok = if ($status) { [bool]$status.ok } else { $false }
            nav = if ($status) { $status.NAV } else { $null }
            balance = if ($status) { $status.balance } else { $null }
            margin_used = if ($status) { $status.marginUsed } else { $null }
            margin_closeout_percent = if ($status) { $status.marginCloseoutPercent } else { $null }
            open_trade_count = if ($status) { $status.openTradeCount } else { $null }
            pending_order_count = if ($status) { $status.pendingOrderCount } else { $null }
            unrealized_pl = if ($status) { $status.unrealizedPL } else { $null }
            unprotected_trade_ids = $unprotectedTradeIds
            trades = if ($status) { @(Convert-TradeSummary -Trades $status.trades) } else { @() }
            schedule_health = $scheduleHealth
            latest_decision = if ($decision) {
                [pscustomobject]@{
                    time_ny = $decision.time_ny
                    action_count = $decision.action_count
                    portfolio_bias = $decision.portfolio_bias
                    raw_path = $decision.raw_path
                    market_summary = Get-JsonProperty -Object $rawDecision -Name 'market_summary'
                    underdeployment_reason = Get-JsonProperty -Object $rawDecision -Name 'underdeployment_reason'
                    orders_to_execute = if ($rawDecision) { @(Convert-OrderSummary -Orders (Get-JsonProperty -Object $rawDecision -Name 'orders_to_execute')) } else { @() }
                    live_failed_thesis_consistency = if (Get-JsonProperty -Object $rawDecision -Name 'live_failed_thesis_consistency') {
                        $failedThesis = Get-JsonProperty -Object $rawDecision -Name 'live_failed_thesis_consistency'
                        [pscustomobject]@{
                            local_verdict = Get-JsonProperty -Object $failedThesis -Name 'local_verdict'
                            issue_count = Get-JsonProperty -Object $failedThesis -Name 'issue_count'
                            issues = Convert-JsonArray -Value (Get-JsonProperty -Object $failedThesis -Name 'issues')
                        }
                    } else { $null }
                    live_decision_quality_review = if ($qualityReview) {
                        [pscustomobject]@{
                            local_verdict = Get-JsonProperty -Object $qualityReview -Name 'local_verdict'
                            issue_count = Get-JsonProperty -Object $qualityReview -Name 'issue_count'
                            issues = Convert-JsonArray -Value (Get-JsonProperty -Object $qualityReview -Name 'issues')
                            missed_entry_candidates = Convert-JsonArray -Value (Get-JsonProperty -Object $qualityReview -Name 'missed_entry_candidates')
                        }
                    } else { $null }
                    live_missed_entry_fallback = $missedEntryFallback
                }
            } else { $null }
            latest_action = if ($action) {
                [pscustomobject]@{
                    time_ny = $action.time_ny
                    action_type = $action.action_type
                    status = $action.status
                    instrument = $action.instrument
                    direction = $action.direction
                    units = $action.units
                    fill_price = $action.fill_price
                    stop_loss = $action.stop_loss
                    take_profit = $action.take_profit
                    reject_reason = $action.reject_reason
                }
            } else { $null }
            latest_order = if ($order) {
                [pscustomobject]@{
                    time_ny = $order.time_ny
                    source = $order.source
                    status = $order.status
                    instrument = $order.instrument
                    direction = $order.direction
                    trade_id = $order.trade_id
                    order_id = $order.order_id
                    price = $order.price
                    reason = $order.reason
                }
            } else { $null }
            alerts = @($alerts)
        }
    } catch {
        $record = [pscustomobject]@{
            time_local = $now.ToString('o')
            cycle = $cycle
            started_at = $StartedAt.ToString('o')
            end_at = $EndAt.ToString('o')
            manager_pids = @()
            restarted_manager = $restarted
            restarted_pid = $restartPid
            status_ok = $false
            alerts = @('watchdog_cycle_error')
            error = $_.Exception.Message
        }
    }

    $json = $record | ConvertTo-Json -Depth 12 -Compress
    Add-Content -Path $LogPath -Value $json
    $record | ConvertTo-Json -Depth 12 | Set-Content -Path $LatestPath

    if ($MaxCycles -gt 0 -and $cycle -ge $MaxCycles) {
        break
    }

    $remaining = ($EndAt - (Get-Date)).TotalSeconds
    if ($remaining -le 0) {
        break
    }
    Start-Sleep -Seconds ([Math]::Min($IntervalSeconds, [int][Math]::Ceiling($remaining)))
}
