# Forex operational checkpoint — 2026-09-13T13:43:38.829786+00:00

The reviewed ingestion and native H1 research changes are installed in the actual project at `C:\Users\zmoor\Documents\forex\trad`. Four new configuration files passed their actual source-owner and path checks. **Services and trading remain stopped; this is an offline software acceptance, not a live-performance or profitability claim.**

## What changed

- Installed the exact 45-file source kit: one existing collector replaced with its prior bytes backed up, 17 new files, 27 unchanged. The collector now recovers from parse failures through a full-body retry instead of getting stuck behind conditional responses; malformed feeds remain visible failures.
- Connected source observations, classification revisions, durable publication and consumer observations to the shared history input for 68 pairs. Later transport failures invalidate old handles.
- Preserved the joint model's original completed-minute origin and exact H1 target. Issued forecasts can still receive their native outcomes while current news is unavailable. No current quote is substituted for the model's original target.
- Repaired a worker crash caused by 68 full forecast summaries exceeding 1 MiB. Compact status retains original values, probabilities, targets and evidence hashes; complete fitted vectors and provenance remain in the ledger. The largest tested summary was 673,922 bytes. Publication failures remain visible and subsequent ticks continue; recovery requires verified readback.
- Initialized only the planned previously absent news database: seven empty user tables, 126,976 bytes, no imported history. Its exact device/inode identity is bound into the new configuration without numeric rounding.
- Corrected policy construction to distinguish 190 collector-managed feed definitions from two separately managed adapters: CFTC positioning and FRED/ALFRED vintages. CFTC's missing news-cohort identifier is not invented. Their existing health-state routes remain declared, with no state contents inspected. Disabled/unsupported declarations remain explicit; inclusion in the policy does not prove a feed is currently reachable.
- Completed the configuration path check after creating its missing empty state directory. The first attempt had installed all four exact files before failing that check; its failure journal is preserved. Recovery validated the existing files without reinstalling them or opening a database.

The numerical joint model, its original readiness requirements and old research cohorts were preserved. This work changes software reliability and measurement, not learned forecast quality.

## Evidence and limits

The final integration, with all 45 source files verified before project imports, passed two cases covering actual owned collector, transport, history, worker and native-ledger behavior, including scoring after a news outage. A separate eight-case footprint check with 68 rows and nine publication-lifecycle checks passed. Synthetic forecast fitting and thread execution were explicit fixture substitutions; no actual model fit, original market-data read, broker action or network request was performed.

See [source acceptance](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/joint_revision_operational_001/ROOT_OPERATIONAL_SOURCE_KIT_ACCEPTANCE_002.json>), [installed-file receipt](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/source_kit_installation_001/actual_install_root_002/events/0061_COMPLETE.json>), [empty database receipt](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/joint_revision_operational_001/production_collector_init_004/RECEIPT.json>), [inactive operating profile](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/joint_revision_operational_001/production_profile_005/OPERATING_PROFILE_005.json>), [preserved first configuration failure](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/joint_revision_operational_001/config_installation_001/FAILURE.json>), and [actual installed configuration validation](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/joint_revision_operational_001/config_path_recovery_001/RECEIPT.json>). The profile's earlier staged-installation marker is superseded by that separate validation receipt; it remains disabled.

## Prediction findings

No audited setup has established dependable profit after costs. The recovered July 11 H1 programme includes completed ARIMA/SARIMAX runs: ARIMA achieved 49.95% direction over 47,580 forecast rows and −1.7974 net pips over 3,172 selected trades; SARIMAX achieved 50.02% and −1.8136 net pips. ARIMA also narrowly lost to no-change on RMSE. Its gross positive result disappears after costs. Tiny positive subsets of 22 and 13 trades failed validation.

Some H1 forecasts were above chance: an initial Ridge sample scored 58/93 correct (62.4%), and a matched joint comparison scored 52.07% versus 50.20% for price-only. Both still lost after executable costs. These periods and scoring methods differ; they do not establish a current uniform model ranking. The latest wide-feature baseline is compact gradient boosting, not ARIMA. [Full ARIMA and H1 clarification](<C:/Users/zmoor/Documents/forex/trad/docs/FOREX_ARIMA_AND_H1_READBACK_20260913.md>) records the distinctions and exact retained-report provenance. No scores were recalculated in this pass.

## Remaining work

1. The required clock-monitor startup remains blocked by automatic approval review, whose stated reason was “blocked by policy.” No alternate startup route was attempted. Actual service startup, fresh-ledger activation and process supervision remain separate unperformed steps. [Exact activation blocker](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/market_open_clock_resolution_001/CURRENT_ACTIVATION_BLOCKED_002.md>).
2. Establish real feed/quote health after an authorized successful startup. Fresh news history must meet 48 mature eligible contexts, 12 nonzero contexts and 8 distinct patterns; elapsed time alone is insufficient. Old evidence is not backfilled as earlier knowledge.
3. Keep the CFTC positioning and FRED/ALFRED vintage routes separate from news-article ingestion until source-bound bridges are implemented and tested. The current policy does not claim to ingest every external data format.
4. Migrate older independent price controls to their original native targets under a separate reviewed cohort. Their existing label is `translated_live_quote_h1`; they are not directly comparable to the new native joint cohort. [Current control semantics](<C:/Users/zmoor/Documents/forex/revamp_8h_20260912/runtime/opening_price_target_semantics_001/OPENING_CONTROL_SEMANTICS_001.json>).
5. Continue comparable directional, execution-cost and horizon-curve research without repeating completed ARIMA/boosting experiments or promoting an unprofitable model. A stable position-management benefit remains unestablished.

The original archive and prior checkpoints remain intact. Checkpoint 009 packages current source and recreation records alongside a pinned reference to checkpoint 008's historical source base of 222 files. Its eventual publication receipt establishes local copy completion; it does not prove cloud synchronization. No 48-hour capacity, runtime or forecast-success guarantee is made.
