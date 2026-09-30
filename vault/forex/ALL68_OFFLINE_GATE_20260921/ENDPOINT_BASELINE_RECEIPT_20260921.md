# Endpoint-only all-68 baseline receipt — 2026-09-21

## Purpose and boundary

This is a bounded, fully offline forecast-skill diagnostic. It uses endpoint-only midpoint returns because the separate strict exact-minute path audit found the 24/48/72-hour paths incomplete. It does **not** model fills, costs, financing, positions, portfolio return, or trade viability.

Raw M1 timestamps are reconstructed as interval starts. A decision using the source candle is therefore timestamped one minute later. Forecasts use hourly origins, a fixed 21-day training block, and a later fixed 7-day held-forward block across all 68 declared instruments.

## Label and coverage decision

The strict-path audit remains authoritative for path and execution claims: its 24/48/72-hour labels were all blocked by intermediate gaps in the sampled window. The endpoint-only target has a different, explicitly limited purpose: signed midpoint movement at the exact elapsed endpoint, even when intermediate bars are absent.

Endpoint coverage, 24-hour origin sample (93,002 origin rows):

| Horizon | Endpoint available | Endpoint missing |
|---:|---:|---:|
| 24h | 89,505 | 3,497 |
| 48h | 89,978 | 3,024 |
| 72h | 89,565 | 3,437 |

Coverage artifact: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_ENDPOINT_TARGET_COVERAGE.json`  
SHA-256: `1a693918dfba1da99f4d5cbb35dd9ccd75e4ec4e5cde990a6be46166eb1b2cf2`

## First held-forward comparison

Features were causal 60-minute, 240-minute, and 1,440-minute returns, quoted spread, and UTC time sine. The comparison is no-change versus a pooled ridge model (lambda 20), trained 2024-07-01 through 2024-07-22 and evaluated 2024-07-22 through 2024-07-29. No-change is an abstention for direction, so it has zero directional-forecast coverage.

| Horizon | Train rows | Test rows | Ridge direction accuracy | Ridge MAE (bps) | No-change MAE (bps) | Result |
|---:|---:|---:|---:|---:|---:|---|
| 24h | 11,872 | 4,033 | 52.9% | 46.89 | 46.91 | Direction diagnostic only; error improvement is negligible. |
| 48h | 7,929 | 2,684 | 39.6% | 74.33 | 70.09 | Negative: worse error than no-change. |
| 72h | 7,900 | 2,668 | 53.7% | 52.60 | 52.65 | Direction diagnostic only; error improvement is negligible. |

This is one held-forward block with overlapping targets. It is not independent episode evidence, not a promotion result, and not an executable result.

## Reproduction

Use the isolated interpreter and do not point this workflow at a broker or credentials:

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -I -B -m pytest -q --noconftest 'C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\test_endpoint_targets.py' 'C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\test_all68_endpoint_baseline.py'
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -I -B 'C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_endpoint_baseline.py'
```

Verified test result: 5 passed. The cache warning is nonfunctional: the shared root blocks pytest cache creation.

Baseline implementation SHA-256: `19ddf6f6531a3c774fa05b5bfc8e7fd77467b6f1e764b12c290981e162ee1e3d`  
Report SHA-256: `e9a3a10fff34b111568595492f3ef3c6bc4f03198d3965399627bba02144a182`  
Forecast tape SHA-256: `7acdd662f3dffff2a40ded3c6c48efb673124a89cf1126d22a4653e8e62e2b6c`

## Next resumable work

1. Add a daily-close and trading-day target calendar; do not relabel 48/72 elapsed hours as trading days.
2. Repeat this exact model comparison on preregistered later blocks with interval-aware purge and episode/block reporting.
3. Add a recovered corrected tree baseline only after its feature and timing lineage is bound.
4. Keep strict-path labels separate until a data-quality policy can support historical execution simulation.
5. Advisor/GPT comparison remains deferred by current user instruction.
