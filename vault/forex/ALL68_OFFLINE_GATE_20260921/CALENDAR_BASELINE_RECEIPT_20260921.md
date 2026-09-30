# All-68 calendar-target baseline receipt — 2026-09-21

## Scope

This is an offline, endpoint-only midpoint-return forecast diagnostic. It evaluates no-change against a pooled ridge model across all 68 instruments. It does not simulate paths, orders, fills, costs, financing, P&L, or portfolio policy.

The targets use the separately documented UTC daily-close convention and count Monday-Friday close boundaries. Source timestamps are interval starts; a decision is `raw timestamp + 60 seconds`. Training labels are admitted only when their own target is ready by the fixed training cutoff.

## Fixed held-forward comparison

Training window: 2024-07-01 through 2024-07-22 UTC.  
Test window: 2024-07-22 through 2024-07-29 UTC, hourly decisions.

| Target | Train rows | Test rows | Ridge direction accuracy | Ridge MAE (bps) | No-change MAE (bps) |
|---|---:|---:|---:|---:|---:|
| Next UTC daily close | 15,488 | 5,305 | 57% | 20.25 | 20.44 |
| Two trading days | 14,281 | 5,283 | 56% | 46.34 | 46.64 |
| Five trading days | 10,277 | 5,305 | 64% | 84.74 | 89.04 |

The simple ridge model uses causal 60m/240m/1440m returns, quoted spread, and UTC time sine. The no-change baseline abstains from directional forecasts. Every pair was retained in the extraction ledger; missing endpoints were excluded only from the target-specific matched comparison.

## Interpretation

The initial block alone was a positive-looking diagnostic, but it is not admission evidence. The separately stored August later block is negative by MAE at all three targets and has sub-50% two/five-day direction. See [the later-block receipt](CALENDAR_BASELINE_AUGUST_BLOCK_RECEIPT_20260921.md). The pooled ridge model therefore has **no admission status**. Repeated blocks, interval-aware purge, episode/block uncertainty, and matched native-coverage reporting remain required. Strict M1 path and executable evaluation remain blocked by known gaps.

## Reproduction and fingerprints

Implementation: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_calendar_baseline.py`  
Implementation SHA-256: `4f5ddc48b5d32b974acce7ce4553d52dd92c61aadcadfa6cb86c4cfdce2dabdd`

Report SHA-256: `0e80fd32afff91b8ca59a7a2ed89f0fbf596a2366a6a1e180279f0f224ab7db9`  
Immutable forecast tape SHA-256: `d7db55a3fa2db869ff47ad2e34fdfcc2bc4be97fe8a13a75cdf972d6cb5e881c`

The intermediate `calendar_baseline_parts_v1` directory is pair-resumable. Each extraction slice is atomically stored before pooled fitting, so reruns continue from completed pair slices.

Advisor/GPT comparisons remain deferred. No broker, network, account, or trading actions were used.
