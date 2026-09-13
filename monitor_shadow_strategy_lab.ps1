param(
    [string]$ApiUrl = "http://127.0.0.1:8765/api/full-state",
    [string]$ExpectedRunLabel = "",
    [string]$StateRoot = "D:\forex\trad\data\oanda_training_manager\state",
    [switch]$UseDashboardApi,
    [double]$DurationHours = 3.0,
    [int]$IntervalSec = 60,
    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"
$started = Get-Date
$stopAt = $started.AddHours($DurationHours)
$sample = 0

while ((Get-Date) -lt $stopAt) {
    $sample++
    try {
        if ($UseDashboardApi) {
            $state = Invoke-RestMethod -Uri $ApiUrl -TimeoutSec 120
            $lab = $state.strategy_lab
            $micro = $state.micro_patterns
        } else {
            $heartbeatPath = Join-Path $StateRoot "strategy_lab_heartbeat_v1.json"
            $microPath = Join-Path $StateRoot "micro_pattern_dashboard_v1.json"
            $accountPath = Join-Path $StateRoot "account_dashboard_v1.json"
            $signalPath = Join-Path $StateRoot "practice_007_signal_snapshot_v1.json"
            $heartbeatItem = Get-Item -LiteralPath $heartbeatPath
            $heartbeat = Get-Content -LiteralPath $heartbeatPath -Raw | ConvertFrom-Json
            $details = $heartbeat.details
            $heartbeatAgeSec = ((Get-Date).ToUniversalTime() - $heartbeatItem.LastWriteTimeUtc).TotalSeconds
            $lab = [pscustomobject]@{
                active = ($heartbeat.status -eq "running" -and $heartbeatAgeSec -lt 30.0)
                run_label = [string]$details.run_label
                last_write_age_sec = $heartbeatAgeSec
                lane_count = [int]$details.lane_count
                evaluation_cycles = [int]$details.evaluation_cycles
                signals = [int]$details.signals
                near_misses = [int]$details.near_misses
                hard_rejects = [int]$details.hard_rejects
                outcomes = [int]$details.outcomes
                lanes = @()
            }
            $microItem = Get-Item -LiteralPath $microPath
            $micro = Get-Content -LiteralPath $microPath -Raw | ConvertFrom-Json
            $micro | Add-Member -NotePropertyName last_write_age_sec -NotePropertyValue (
                ((Get-Date).ToUniversalTime() - $microItem.LastWriteTimeUtc).TotalSeconds
            ) -Force
            $micro | Add-Member -NotePropertyName quote_updates -NotePropertyValue (
                [int64]$micro.changed_quote_updates
            ) -Force
            $accounts = Get-Content -LiteralPath $accountPath -Raw | ConvertFrom-Json
            $signalSnapshot = Get-Content -LiteralPath $signalPath -Raw | ConvertFrom-Json
            $patternPredictions = [int](
                ($micro.models | Measure-Object -Property predictions -Sum).Sum
            )
            $state = [pscustomobject]@{
                total_pattern_predictions = $patternPredictions
                micro_model_count = @($micro.models).Count
                micro_quote_updates = [int64]$micro.changed_quote_updates
                accounts = $accounts
                signal_snapshot = $signalSnapshot
            }
        }
        $labelMatches = (
            [string]::IsNullOrWhiteSpace($ExpectedRunLabel) -or
            $lab.run_label -eq $ExpectedRunLabel
        )
        $topAccepted = @(
            $lab.lanes |
                Where-Object { [int]$_.accepted.n -ge 3 } |
                Sort-Object { [double]$_.accepted.avg } -Descending |
                Select-Object -First 10 |
                ForEach-Object {
                    [ordered]@{
                        lane = $_.lane_id
                        n = [int]$_.accepted.n
                        avg = [double]$_.accepted.avg
                        win_rate = [double]$_.accepted.win_rate
                    }
                }
        )
        $missedUpside = @(
            $lab.lanes |
                Where-Object { [int]$_.missed.n -ge 5 } |
                Sort-Object { [double]$_.missed.avg } -Descending |
                Select-Object -First 10 |
                ForEach-Object {
                    [ordered]@{
                        lane = $_.lane_id
                        n = [int]$_.missed.n
                        avg = [double]$_.missed.avg
                        win_rate = [double]$_.missed.win_rate
                    }
                }
        )
        $payload = [ordered]@{
            time = (Get-Date).ToUniversalTime().ToString("o")
            event = "monitor_sample"
            sample = $sample
            healthy = (
                $lab.active -eq $true -and
                $labelMatches -and
                [double]$lab.last_write_age_sec -lt 30.0
            )
            active = [bool]$lab.active
            run_label = [string]$lab.run_label
            age_sec = [double]$lab.last_write_age_sec
            lane_count = [int]$lab.lane_count
            cycles = [int]$lab.evaluation_cycles
            signals = [int]$lab.signals
            near_misses = [int]$lab.near_misses
            hard_rejects = [int]$lab.hard_rejects
            outcomes = [int]$lab.outcomes
            pattern_predictions = [int]$state.total_pattern_predictions
            micro_active = [bool]$micro.active
            micro_models = [int]$state.micro_model_count
            micro_quotes = [int]$state.micro_quote_updates
            micro_latest_forecasts = [int]@(
                $micro.latest_forecasts | Where-Object { $null -ne $_ }
            ).Count
            micro_model_summaries = @(
                $micro.models |
                    Sort-Object @{Expression = { [double]$_.ready_matured }; Descending = $true } |
                    Select-Object -First 12 |
                    ForEach-Object {
                        [ordered]@{
                            model_id = [string]$_.model_id
                            predictions = [int]$_.predictions
                            ready_matured = [int]$_.ready_matured
                            direction_accuracy = [double]$_.direction_accuracy
                            average_net_pips = [double]$_.average_net_pips
                            movement_coefficient_correlation = $_.movement_coefficient_correlation
                        }
                    }
            )
            accounts = @(
                $state.accounts.accounts |
                    Where-Object { $_.account_id -match "-(002|005|007|013)$" } |
                    ForEach-Object {
                        [ordered]@{
                            account_id = [string]$_.account_id
                            role = [string]$_.role
                            nav = [double]$_.NAV
                            balance = [double]$_.balance
                            realized_pl = [double]$_.pl
                            unrealized_pl = [double]$_.unrealizedPL
                            open_trades = [int]$_.openTradeCount
                            unprotected_trades = @($_.unprotectedTradeIds).Count
                        }
                    }
            )
            signal_feed_candidates = [int]$state.signal_snapshot.feed_candidate_count
            consolidated_signals = [int]$state.signal_snapshot.consolidated_signal_count
            qualified_signals = [int]$state.signal_snapshot.qualified_signal_count
            selected_signal = $state.signal_snapshot.selected
            selected_signal_stage = [string]$state.signal_snapshot.selected_stage
            selected_signal_final_gate_status = [string]$state.signal_snapshot.selected_final_gate_status
            selected_signal_routable_after_direction_conflict_gate = [bool]$state.signal_snapshot.selected_routable_after_direction_conflict_gate
            top_signals = @(
                $state.signal_snapshot.top_signals |
                    Select-Object -First 5 |
                    ForEach-Object {
                        [ordered]@{
                            instrument = [string]$_.instrument
                            direction = [string]$_.direction
                            horizon_sec = [int]$_.preferred_horizon_sec
                            confidence = [double]$_.signal_confidence
                            projected_net_pips = [double]$_.projected_net_pips
                            blocked_by = @($_.signal_blocked_by)
                        }
                    }
            )
            disk_event7_record = [int64](
                Get-WinEvent -FilterHashtable @{ LogName = "System"; Id = 7 } -MaxEvents 1 -ErrorAction SilentlyContinue
            ).RecordId
            d_free_gb = [math]::Round((Get-PSDrive -Name D).Free / 1GB, 2)
            top_accepted = $topAccepted
            missed_upside = $missedUpside
        }
    } catch {
        $payload = [ordered]@{
            time = (Get-Date).ToUniversalTime().ToString("o")
            event = "monitor_error"
            sample = $sample
            healthy = $false
            error = $_.Exception.Message
        }
    }
    $payload | ConvertTo-Json -Compress -Depth 6 | Add-Content -LiteralPath $OutputPath -Encoding utf8

    $remaining = ($stopAt - (Get-Date)).TotalSeconds
    if ($remaining -le 0) {
        break
    }
    Start-Sleep -Seconds ([int][math]::Min($IntervalSec, [math]::Max(1, $remaining)))
}

[ordered]@{
    time = (Get-Date).ToUniversalTime().ToString("o")
    event = "monitor_complete"
    elapsed_hours = [math]::Round(((Get-Date) - $started).TotalHours, 4)
    samples = $sample
} | ConvertTo-Json -Compress | Add-Content -LiteralPath $OutputPath -Encoding utf8
