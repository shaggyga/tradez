# Cleanup and reproducibility checkpoint — September 15, 2026

The [latest completed specialist study](../ROLLING_SPECIALISTS_20260915/README.md) remains intact. All 156 payload files were reverified before this checkpoint. Earlier rolling comparisons remain separate historical references.

The [new two-period research recipe](source/forex/trad/docs/validation/rolling_period_replication_20260915/PRE_FIT_SCOPE.md), source closure, exact build commands, input manifests and both five-prefix normalizer extracts are preserved here. Those model runs are still in progress at capture: this checkpoint does not claim new prediction results. Full price/feature/quote/forecast row datasets remain local dependencies and are protected from this cleanup.

Old, dated stdout logs may now be losslessly compressed locally using [the archiver](source/forex/trad/tools/archive_forex_cold_logs_v1.py). The exact selection and [review](source/forex/cleanup_audit_20260915/ARCHIVER_REVIEW.json) are retained. No database, model, feature, forecast or current configuration is eligible. An original is removed only after writer exclusion, a full decompression/hash match and a durable restoration receipt. Compressed diagnostic logs remain local because they may contain private operational details.

## Restoration

From the actual project, use its recorded Python runtime and:

```powershell
python -B tools/archive_forex_cold_logs_v1.py --restore-receipt C:\Users\zmoor\Documents\forex\cold_log_archive_20260915\receipts\ITEM.json --restore-copy C:\Users\zmoor\Documents\forex\restored_logs\ORIGINAL.out.log
```

Each receipt gives the exact original name, size, timestamp, hash and local gzip location. Restoration creates a separate copy and refuses overwrites; compare its verified hash before any deliberate return to an original location.

Cache cleanup affects downloadable/rebuildable caches and old crash dumps, not model inputs. Heat and VALORANT relocation is separately in progress and will receive its own verified transfer/restoration receipts. Final cleanup results will be appended as separate files after completion. This local copy is verified; cloud synchronization is not asserted.
