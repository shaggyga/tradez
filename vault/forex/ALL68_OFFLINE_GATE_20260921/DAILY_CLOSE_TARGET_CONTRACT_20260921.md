# Daily-close and trading-day target contract

The source archive records M1 price bars but does not prove the broker's original daily-session close convention. The initial offline research default is therefore **UTC midnight**. This is a deterministic target-calendar decision, not a claim about a broker's historical roll or executable close.

For a source bar stamped `T`, decision availability is reconstructed as `T + 60 seconds`. A daily-close midpoint endpoint at close boundary `C` uses the raw bar `C - 60 seconds` and becomes label-available at `C`. One, two, and five trading-day labels count eligible Monday-Friday UTC close boundaries strictly after the decision; weekends are skipped.

This target family is endpoint-only and has no path, fill, cost, financing, or P&L meaning.

## All-68 coverage result

The coverage audit is complete over 68 pairs using hourly origins from 2024-07-01 through 2024-07-08 and a read-only query through 2024-07-17.

| Target | Endpoint available | Missing target |
|---|---:|---:|
| Next UTC daily close | 7,337 | 298 |
| Two trading-day closes | 7,297 | 338 |
| Five trading-day closes | 7,213 | 422 |

This permits endpoint-only forecast-skill evaluation for each target. Strict path and execution evaluation remain separately blocked by the M1 path audit.

Coverage artifact: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_CALENDAR_TARGET_COVERAGE.json`  
SHA-256: `9d99a0dd62c1456e02c933d5d7cc245a3f9c4d6a3f2849a7f25ceeaa5d5c6892`

## Strict-path audit

The follow-up exact-calendar-minute audit examined every minute from each hourly origin through its calendar endpoint. Of 7,635 next-close origins, 154 had every calendar minute present; 7,183 had missing calendar minutes and 298 were missing their endpoint. For two trading days, all 7,297 endpoint-available origins had missing calendar minutes. For five trading days, all 7,213 endpoint-available origins had missing calendar minutes. The audit does not classify a missing minute as an unexpected data defect versus an expected weekend/session closure.

Therefore the calendar labels remain forecast-skill diagnostics only. They must not be used as fixed-hold execution returns, barrier outcomes, or policy P&L until a session-calendar and execution-path policy is separately bound.

A 16-day all-68 common-missingness audit reinforces this boundary: 5,808 expected minutes were absent from every pair and are closure candidates, but 14,978 expected minutes had only partial pair presence. A session calendar could classify common closures, but it cannot by itself resolve pair-specific missingness. The ranked per-pair view identifies EUR/DKK (39%), TRY/JPY (40%), ZAR/JPY (52%), USD/HKD (54%), and USD/THB (56%) as the lowest presence rates in this window.

Common-missingness artifact: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_GLOBAL_MISSINGNESS_AUDIT.json`  
SHA-256: `bb1d96447825c8a5f698b12b4fc6cb0fc85c152f45558974846d9b7f44b09884`

Strict-path artifact: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\ALL68_CALENDAR_STRICT_PATH_AUDIT.json`  
SHA-256: `9dd328cc932d17c91d5de6090ef6141bc5c887923e2507518eaf01b9fe6c61e6`

Implementation: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\calendar_targets.py`  
Test: `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\test_calendar_targets.py`  
Verification: 15 focused Stage C contracts passed on 2026-09-21.
