# Calendar baseline later-block receipt — August 2024

This is an independent, separately stored rerun of the initial all-68 calendar-target baseline. It uses the same code, features, target contract, and model comparison, with run ID `calendar_aug_2024_v1` and different fixed windows: training 2024-08-01 through 2024-08-22 UTC; held-forward test 2024-08-22 through 2024-08-29 UTC.

| Target | Train rows | Test rows | Ridge direction accuracy | Ridge MAE (bps) | No-change MAE (bps) |
|---|---:|---:|---:|---:|---:|
| Next UTC daily close | 15,869 | 5,339 | 52% | 24.85 | 24.78 |
| Two trading days | 14,581 | 5,294 | 46% | 41.58 | 39.50 |
| Five trading days | 11,939 | 5,284 | 46% | 60.08 | 58.76 |

## Decision

This later block is negative by MAE at every target and shows sub-50% directional accuracy at two and five trading days. It **invalidates any favorable interpretation of the first July block**. The model has no admission status and must not feed a policy, trade simulation, or live system.

The report is still useful as a reproducible negative control: it confirms that the same causal pipeline and target definition can produce materially different results on a later block.

Report SHA-256: `100fa64b71b78ea6e3b2cbda64177434ca2568ba9f319360a71feabf11d93267`  
Forecast tape SHA-256: `693bf4c687abc7bc4dcf3be02bcd1d2e41d0b854d30fe356ed63c3ca6cc9c133`

Artifacts:

- `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_CALENDAR_BASELINE_calendar_aug_2024_v1_REPORT.json`
- `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_CALENDAR_BASELINE_calendar_aug_2024_v1_FORECAST_TAPE.jsonl.gz`

No advisor/GPT, broker, network, account, or trading action was used.
