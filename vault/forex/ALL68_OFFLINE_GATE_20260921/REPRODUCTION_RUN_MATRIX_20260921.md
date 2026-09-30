# Reproduction run matrix

All commands use:

`C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe -I -B C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_calendar_baseline.py`

The runner is offline, reads the checkpointed ZIP only, writes pair-resumable slices beneath Stage C, and has no broker/client dependency.

| Candidate | Run ID | Train start/end | Test end | Status |
|---|---|---|---|---|
| Ridge | `calendar_baseline_v1` | 2024-07-01 / 2024-07-22 | 2024-07-29 | Retired |
| Ridge | `calendar_aug_2024_v1` | 2024-08-01 / 2024-08-22 | 2024-08-29 | Retired |
| Ridge | `calendar_sep_2024_v1` | 2024-09-01 / 2024-09-22 | 2024-09-29 | Retired |
| HGB | `hgb_jul_2024_v1` | 2024-07-01 / 2024-07-22 | 2024-07-29 | Retired |
| HGB | `hgb_aug_2024_v1` | 2024-08-01 / 2024-08-22 | 2024-08-29 | Retired |

For nondefault rows, set these process-local environment variables before invoking the runner:

```powershell
$env:FOREX_CALENDAR_MODEL='ridge' # or hist_gradient_boosting
$env:FOREX_CALENDAR_RUN_ID='example_run_id'
$env:FOREX_CALENDAR_TRAIN_START='2024-10-01T00:00:00+00:00'
$env:FOREX_CALENDAR_TRAIN_END='2024-10-22T00:00:00+00:00'
$env:FOREX_CALENDAR_TEST_END='2024-10-29T00:00:00+00:00'
```

Never reuse a run ID for a changed model, target, feature definition, or date window. The runner’s individual pair slices are restartable; completed `*.npz` slices are reused and temporary `*.tmp.npz` files are ignored.

All retained results are endpoint-only forecast-skill diagnostics. They do not establish an executable historical path, after-cost P&L, a policy result, or trading authorization.

## One-command handoff verification

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -I -B 'C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\verify_stage_c_handoff.py'
```

Expected result: `offline only; models denied; multiday paths incomplete`.

For the complete local check, run:

```powershell
& 'C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\verify_stage_c.ps1'
```

Expected result: the handoff and preflight pass, followed by 17 passing Stage C
contracts. The pytest cache warning is nonfunctional and comes from the shared
workspace denying cache writes.
