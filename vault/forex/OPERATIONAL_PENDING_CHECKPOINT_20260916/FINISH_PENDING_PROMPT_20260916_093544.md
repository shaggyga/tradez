Continue the Forex operational pending work in C:\Users\zmoor\Documents\forex\trad using the vault at C:\Users\zmoor\OneDrive\thevault\projects\forex as the audit/checkpoint guide. Keep the runtime no-orders/research-only unless the user explicitly authorizes order placement.

Current checkpoint:
- V18 supervisor profile is active and source-bound: config/operational_runtime_v18_20260916.json, expected hash 820e3853c4b73d45b0f3c17eefccecca889fffbb8990dff00d95f571ebeb8288.
- All-68 derived technical publisher has been repaired and is supervised. It should publish all 68 registered pairs with minimal and movement features. Confirm data/oanda_training_manager/state/all68_derived_technical_features_v1.json coverage and source_errors=0.
- Forward feature workers were repaired so partial broker quote refusals are visible but nonfatal when other usable quotes exist. Fresh ledgers were created and old ledgers were preserved under retired_source_generation_* directories. Confirm primary feature forward is observing and cached M5 forward is progressing from waiting_for_fresh_history toward usable readiness.
- Remaining major blocker is joint_price_news_study_v9. The collector/transport itself is current and successful, but the joint study has hit bounded validation failures: first io_capture_duration_bound, then stream_validation_time_bound during news bootstrap. A bloated capture_archive was rotated to capture_archive.retired_io_capture_bound_20260916_092804 and recreated. Do not blindly relax trading/model gates. Determine whether joint validation cache advances across retries; if it does, wait/read back. If it resets, either isolate joint-news as degraded with evidence or implement a source-bound successor plan with tests and config/profile hash updates.
- Do not repeat prior broad model experiments without checking vault records. The model problem remains unresolved: prediction edge/profitability has not been proven. Operational goal is correct feeds/readability and reproducible evidence, not claiming profitability.

Immediate next checks:
1. Inspect supervisor/watchdog health and managed roles.
2. Inspect joint validation cache tables and heartbeat scheduler events to see if bootstrap advances or resets after stream_validation_time_bound.
3. Confirm all68 derived coverage and forward worker freshness after another cycle.
4. If code/config changes are needed, update hashes in the relevant config and V18 profile, run meaningful tests, restart only affected no-orders roles, and write a checkpoint in project docs and vault.
