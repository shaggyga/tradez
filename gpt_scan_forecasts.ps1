param(
  [int]$Top = 25
)

$ErrorActionPreference = "Stop"

$files = @(
  @{
    name = "canary"
    path = ".\trad\data\technical_scout_manager\account_canary_primary_forecast_rotation\latest_forecasts.csv"
  },
  @{
    name = "tech003"
    path = ".\trad\data\technical_scout_manager\account_tech003_primary_forecast_rotation_maxnew16\latest_forecasts.csv"
  }
)

$rows = foreach ($f in $files) {
  Import-Csv $f.path |
    Where-Object { $_.reject_reason -eq "" -and [double]$_.edge_pips -gt 0 } |
    ForEach-Object {
      [pscustomobject]@{
        feed = $f.name
        generated_utc = $_.generated_utc
        instrument = $_.instrument
        direction = $_.direction
        probability = [double]$_.probability
        profit_factor = [double]$_.profit_factor
        edge_pips = [double]$_.edge_pips
        spread_pips = [double]$_.spread_pips
        expected_r = [double]$_.expected_r_multiple
        rank_score = [double]$_.rank_score
        m1_hold = [double]$_.m1_hold_prob
        m1_quick = [double]$_.m1_quick_prob
        m1_status = $_.m1_overlay_status
        bid = $_.bid
        ask = $_.ask
        entry_ref = $_.entry_ref
        stop_loss = $_.stop_loss
        take_profit = $_.take_profit
        stop_pips = $_.stop_pips
        tp_pips = $_.take_profit_pips
        trail_pips = $_.trailing_stop_pips
        model_tf = $_.model_feature_timeframe
        model_time = $_.model_feature_time_utc
        m1_time = $_.m1_overlay_feature_time_utc
      }
    }
}

$rows |
  Sort-Object @{Expression = "expected_r"; Descending = $true}, @{Expression = "edge_pips"; Descending = $true} |
  Select-Object -First $Top |
  ConvertTo-Json -Depth 4
