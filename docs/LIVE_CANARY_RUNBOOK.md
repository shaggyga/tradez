# Live Canary Runbook

This runbook describes the current demo canary setup for the account ending in
004. It is the only demo account intended to keep trading during the current
day-long observation window.

## Current Canary

| Field | Value |
|---|---|
| Config | `config/canary_primary_forecast_rotation_bot.json` |
| Bot ID | `canary_primary_forecast_rotation` |
| Account role | `canary_demo_primary_forecast_rotation_best_wide_new12` |
| Data dir | `data/technical_scout_manager/account_canary_primary_forecast_rotation` |
| Mode | `demo` |
| Source | `oanda` |
| Execution | Enabled by `--execute` and `demo_execution_enabled: true` |

## Selected Trading Setup

The canary runs the full-history M30/H1/H4 continuation-execution stream with an
M1 hold overlay:

```text
fresh_fullhist_m30_h1_h4_continuation_oanda_20260707
M1 overlay: hold_threshold >= 0.50
```

The selected configuration is:

```text
max_new_positions_per_cycle: 12
atr_stop_multiplier: 1.50
take_profit_edge_capture: 0.60
take_profit_min_r_multiple: 0.50
trailing_stop_r_multiple: 1.00
signal_gone_exit_minutes: 120
signal_gone_loser_policy: defer_losers_unless_opposite
```

## Start Command

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe `
  .\oanda_primary_forecast_rotation_bot.py `
  --mode demo `
  --source oanda `
  --config .\config\canary_primary_forecast_rotation_bot.json `
  --execute
```

When starting detached from PowerShell, redirect stdout/stderr into:

```text
data/technical_scout_manager/account_canary_primary_forecast_rotation/logs/
```

## Stop Command

Find the process:

```powershell
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
  Where-Object { $_.CommandLine -like '*canary_primary_forecast_rotation_bot.json*' }
```

Stop it:

```powershell
Stop-Process -Id <PID>
```

Remove `process.lock` only after verifying the PID in the lock file is no
longer alive.

## Runtime Files

| File | Meaning |
|---|---|
| `latest_decision.json` | Most recent cycle decision, actions, top forecasts, and safety blocks. |
| `latest_forecasts.csv` | Current forecast table after model-stream and M1 overlay processing. |
| `state.json` | Persisted managed-position state. |
| `actions.csv` | Append-only action journal. |
| `logs/*.out.log` | Cycle stdout and summaries. |
| `logs/*.err.log` | Exceptions/stderr. Empty is expected during normal operation. |

## What To Watch

The main mismatch to monitor is not just win rate. Track:

- realized PnL
- `mae_r` and `mfe_r` once trades close
- positions that were forecast-right at 120 minutes but closed negative
- replacement losses
- stop/trailing-stop losses
- stale positions held beyond the useful forecast window
- spread-gated periods with no trading

The backtest shows the model can be endpoint-right while the trade still loses
if the position path or exit timing is poor. Do not judge the canary only by
headline trade win rate.
