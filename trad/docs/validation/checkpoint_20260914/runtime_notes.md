# Read-only runtime checkpoint notes

Observed by the runtime review September 14, 10:47–10:52 EDT, then root readback at 10:55 and 11:01. No service/config/broker mutations.

- V8 profile SHA-256: 1df9fb95cbc713c8729268e04e5d34d24bce064d4ad2266cab3161ad1bd221f6.
- 21 selected/core services process-running in the first sample; practice runner separate.
- Native transport status: data/oanda_training_manager/operational_repair_20260913_v4/revision_news_v1/transport_state/status.json. Last successful epoch 1789369664.769207; repeated consumer_owned_preparation_time_bound. Retained 182 successes are not fresh success.
- Native joint: operational_repair_20260913_v4/joint_price_news_study_v7/heartbeat.json; zero forecasts, 68 unavailable. Errors 550 initially, 557 at root readback.
- Price-local V3: operational_repair_20260913_v1/pair_local_forecast_study_v3/heartbeat.json; 136 forecasts/68 pairs, zero errors. At 10:48:51 all latest H1 targets future, issue ages 13.6–735 seconds.
- Feature observer: operational_repair_20260914_features_v2/feature_observations_heartbeat_v2.json. Rich M1 coverage 51 at10:47, 11 at10:50, 58 in the root10:55 read. At10:50 source stage27.203s left build3.390s,53 budget exclusions.
- Feature-forward: operational_repair_20260914_features_v1/feature_forward_v1/feature_forward_v1.sqlite. Initial2,019,936 comparisons all excluded. Root read-only query at11:01 found2,081,345 total,0 alerts,0 controls,2,081,345 excluded. Exact query/path/time in forward_categories_readback.json. No writable/immutable shortcut connection used.
- Earlier retained price studies fully settled:16,004=15,518 outcomes+486 exclusions;36,555=33,676+2,879. Zero unresolved/errors in the sample.
- Official pair/horizon collector:9 input events,1,600 outcomes (1,460 valid/140 invalid). Counts do not establish directional success.
- Practice local status: data/oanda_training_manager/practice007_native_v7_20260913_v2/status.json. enabled=true, practice_trial_enabled, practice environment, local NAV/balance40.7708, flat, zero claims/entries/intents. All68 candidates no_published_native_forecast. No independent broker query was made by this checkpoint.
- Practice end:2026-09-16T00:02:33.213475Z. Recovery horizon2026-09-20 is a different startup-control bound; not an extension of trial time.
- project_integrity_audit_v1.json expected old jointV3/repairV1/priceV1/V2 and failed expected_research_workers_not_in_supervisor_allowlist. Its17 failures require version-aware triage.
- Clock event logs preserved original offset jumps2.058s at10:14:24 and2.024s at10:36:30; guard was trusted after quarantine10:46:30. Do not infer trigger size from the newest offset delta.
- Storage guard at10:45:36:76.595GiB C free,50GiB floor,25GiB critical, no deletion/vacuum. Feature archive had approximately1.744GB today; managed-SQLite growth is not all project growth.

Paths without a drive prefix are relative to C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager unless the line already begins data/. Live files continue changing. runtime_readback.json binds the exact bytes seen by the root read, not every earlier sampled state.
