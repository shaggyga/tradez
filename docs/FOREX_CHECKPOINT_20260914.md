# Forex checkpoint — September 14, 2026

Canonical project: C:\Users\zmoor\Documents\forex\trad. Runtime observations span 10:47–11:01 a.m. EDT. [All 37 defined work items and their dispositions](../FOREX_PENDING_IMPROVEMENTS.md) are the sole current queue. [Evidence index](validation/checkpoint_20260914/README.md).

## Verdict

Collection and price-local H1 research are operating. The full combined native prediction and feature-to-future-outcome system is **not fully operational**. No setup has demonstrated dependable profit after executable costs. Several actual historical direction scores exceed 50%; that is different from sufficient, repeatable after-cost decisions.

This is a work checkpoint, not a service shutdown. This pass did not start/stop workers, change live source profiles, alter clock/trading guards, enable/disable the practice runner, place orders, fit a model or start a monitor. No unattended research job was started by this checkpoint.

## Actual runtime evidence

| Component | Observed condition |
|---|---|
| Selected profile | Operational V8; 21 selected/core research services process-running in the 10:47–10:52 sample. Running processes/fresh status files are not completed pipeline health. |
| Quotes/native candles | Quotes connected across 68 instruments, M1 and native M5/H1 updating; real sparse/gap/freshness exclusions remain. |
| Price-local study V3 | 136 H1 forecasts across 68 pairs, zero errors in the retained 10:55 readback. It is not the old full multi-horizon curve. |
| Compact native transport | Repeated cold-bootstrap consumer preparation timeout. Retained last success around 03:07:44 EDT; later fresh statuses cannot make that success current. |
| Native joint study V7 | Zero forecasts, 68 unavailable; 557 errors at 10:55. No current native candidates for the practice consumer. |
| Rich feature observer | Partial and load-dependent: 51 rich pairs at 10:47, 11 at 10:50 with 53 budget exclusions, then 58 at 10:55. The latter snapshot had 63 H1–H4 pairs, but only 33 M30. No stable full-coverage claim. |
| Feature-forward study | Initial inspection found 2,019,936 excluded feature comparisons; the exact 11:01 read-only query found 2,081,345 total, all excluded, zero alerts/controls. 6,528/6,936 pair outcome jobs had settled in the earlier sample, which does not establish successful feature-comparison admission. |
| Whole-project integrity | Degraded; it expects retired study workers outside the current supervisor allowlist and reports legacy feature state. It needs current-profile binding before its full failure count is meaningful. |
| Clock | Trusted after bounded quarantine in the sample. Trigger logs showed actual approximately 2.058s and 2.024s offset jumps; later small deltas do not explain the trigger. Diagnose clock/event-delay behavior. |
| Practice007 successor V2 | Separate runner reports enabled=true, practice environment, flat local status, NAV $40.7708, zero claims, all candidates refused no_published_native_forecast. This is local status, not a fresh independent broker query. |
| Practice expiry | Original cutoff remains 2026-09-16 00:02:33 UTC (September 15, 8:02:33 p.m. EDT). Research can_place_orders=false does not mean this separate runner is disabled. No deadline or daily eight-claim limit changed. |

## Corrected at this checkpoint

1. **Unsafe dedup selection repaired.** The original V1 audit selected every numeric column, including 68 nonregistry fields. Those included future targets and diagnostic outcomes. No model had been fit from that manifest. V2 now requires the ordered input registry, causal flags and matching schema; target/diagnostic fields are explicitly rejected.
2. **Verified 795 registered inputs → 790 unique selected inputs.** Five nominal 15-minute cross-currency fields equal their one-minute counterparts; no constant input columns found. Canonicalized NaN/signed-zero hashes are checked with direct equality. This is exact deduplication only; it does not restore the intended missing window or prove predictive improvement.
3. **Five regression tests passed.** Checks cover future-column exclusion, malicious/malformed registry input, duplicated names, absent registered fields, constant/missing fields, equal-value representations and unchanged source bytes. [Test receipt](validation/checkpoint_20260914/dedup_tests.xml).
4. **Full source matrix preserved.** SHA-256 remains 50fe7f8f19328a4915b5c6963090f6797f52c3a8099e345a15563ce0424de123 across 205,029 rows and 870 total columns. V1 is retained explicitly as invalid evidence, not a selectable model manifest.
5. **Archival output restored.** The earlier invocation of normalize_and_score_archive.py with --help actually reran it. Only archival generation timestamp and dependent binding changed; this pass restored those two tracked files from their historical Git bytes and retained the diff. It was not a new historical model validation.
6. **Current queue reconciled.** All original FXG-001–027 and the 29 INTRA/NEWS/TAG items remain referenced. Older stopped/staged/zero-fit prefixes are historical. The source census, 36 compact/peer fits, richer six-cell comparisons, all-observed inference, ARIMA, MA, conditional-direction work and implemented adapters/gates remain completed within their declared scopes.

The exact-copy refit, repaired-window comparison, train-only redundancy/ablation analysis and later confirmation remain **unperformed**. The active model weights and registered schemas were not replaced.

## Cleaning and storage

At 10:53:52 EDT, C had 82,092,396,544 free bytes (approximately 76.46 GiB); D had 2,124,890,218,496 (approximately 1,978.96 GiB). Running collection means free space changes. C was above the current 50 GiB storage guard floor.

Removed **96 generated bytecode files / 5,004,892 bytes (4.77 MiB)** after checking resolved workspace containment, regular files, corresponding source and no tracked cache members. The receipt lists each removed file. No original source, dataset, model, database/WAL, receipt, failed-run evidence, log or credential was deleted. Inaccessible pytest caches were left intact without ACL changes. This amount is new cleanup only; earlier approximately 10.05 GiB plus 70.3 MB cleanup was already recorded and is not counted again.

Read-only logical-size inventory (lower bounds; inaccessible and some overlength paths omitted):

| Location | Approximate logical bytes | Disposition |
|---|---:|---|
| trad | 227,193,206,962 | Preserve canonical project |
| trad/data | 222,553,301,715 | Main storage workload; assess per-ledger retention |
| revamp_8h_20260912 | 12,662,150,743 | Research and recreation evidence; not disposable tmp |
| vault_quarantine_20260809 | 6,064,564,100 | Recoverable historical model packages |
| AppData ForexResearchData/unified_intrahour_v1 | 8,023,117,580 | Historical data and unique fitted model |
| AppData ForexResearchData/m1_reacquired_20260725 | 1,593,003,002 | Historical M1 data |

The largest database is data/oanda_training_manager/state/strategy_shadow_outcomes_v1.sqlite, 59,806,691,328 bytes. edge_evidence_v1.sqlite (~10.78 GB), source_governance_v1.sqlite (~7.59 GB) and news_technical_watchlist_v1.sqlite (~6.12 GB) also need retention/free-page/writer assessment before any coherent backup and offline compaction. Live SQLite and WAL files must remain a consistent set.

Three concrete dated log candidates total **11,515,970,098 bytes (10.72 GiB)**:

- data/oanda_training_manager/logs/move_first_operational_mapping_alignment_v3_supervised_20260901_121402.out.log — 5,440,358,043 bytes.
- data/oanda_training_manager/logs/move_first_operational_mapping_alignment_v2_supervised_20260902_033714.out.log — 3,338,943,366 bytes.
- data/oanda_training_manager/logs/move_first_operational_mapping_alignment_v2_supervised_20260901_104847.out.log — 2,736,668,689 bytes.

These are **pending cold-storage candidates**, not deleted/reclaimed space. Verify no writer, original references, copy/decompression hashes and relocation/restoration before removing any original. Add bounded rotation for current writers separately. Long-path enumeration failures must not be mistaken for missing evidence; a sampled 276-character path exists through the Windows extended path form.

## Required next sequence

1. Correct current-profile status, feature publication/forward latency and native transport preparation; diagnose recurring clock-offset jumps and per-pair continuity.
2. Verify eligible feature populations actually enter and settle in the forward ledger under normal simultaneous load. Restore current native joint forecast production after true context eligibility.
3. Run the specifically changed price-only reduced/repaired input comparison against the closest existing experiments. Keep discovered archive periods as development evidence.
4. Use later fixed-rule confirmation for a promising result; otherwise record failure and stop that variant.
5. Complete compatible curve, remaining-risk (if used), resumable manager and entry/exit economics comparisons. Reuse installed causal gates and existing all-observed estimates.
6. Resume official-news enrichment/consensus/rate expectations later, as requested; maintain the current news-dependent operational path meanwhile.
7. Execute manifested log/database/research-copy storage work, full private-state restoration and backup verification; keep cosmetic dashboard work deferred.

## Checkpoint and recreation boundary

This Git checkpoint contains corrected audit code/tests, current backlog/README/log pointers, the new checkpoint and compact verification evidence. Earlier archival files and immutable research records retain their original identity. New dedup V2 is a research input-selection artifact only.

A vault copy/source overlay is verified separately by a publication receipt. It is not a full runtime database backup, proof of cloud synchronization, or a recreation of every historical source/artifact. The latest full source baseline and older seals remain separate. No Git remote was configured when checked; private GitHub authentication/creation/push and off-device recovery verification remain pending.
