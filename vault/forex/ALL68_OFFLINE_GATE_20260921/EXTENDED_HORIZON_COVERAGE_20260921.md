# Extended elapsed-horizon coverage result

The first all-68 extended-horizon audit used 24 hours of origins beginning
2024-06-24 UTC and a four-day read-only query. It tested exact-minute elapsed
targets at 1,440, 2,880, and 4,320 minutes. All 68 instruments remained in the
ledger.

| Horizon | Complete paths | Gap in path | Missing target |
| --- | ---: | ---: | ---: |
| 24 hours | 0 | 89,505 | 3,497 |
| 48 hours | 0 | 89,978 | 3,024 |
| 72 hours | 0 | 89,565 | 3,437 |

This is a data-support result, not a model failure. Exact-minute labels require
an uninterrupted path; no missing bar was filled and no longer-horizon target
was replaced with a shorter one. The chosen raw-history period cannot support
this strict all-68 elapsed-target baseline.

Daily-close and 2/5-trading-day targets remain separate and are not implied by
this elapsed-horizon result. They need a versioned session/calendar contract.
The next baseline slice should use availability-aware pair-specific support,
report matched-common support separately, and either select a supported
partition or retain this as an explicit all-pair blocker.
