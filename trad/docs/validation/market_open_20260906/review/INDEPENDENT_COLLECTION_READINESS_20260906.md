# Independent market-open readiness review

The frozen configuration supports restarting the collection-only setup. This review does not establish that the restarted processes are healthy; that needs a fresh post-launch observation. Practice entry is not ready.

All six source bindings, the semantic contract hash recorded by the study heartbeat, and Python 3.12.10 / NumPy 2.5.1 / scikit-learn 1.9.0 match the registered study. The lab and dashboard diagnostic additions are outside those source bindings and provide no order or promotion capability. The new exact-price offline evaluator remains separate from the registered study.

The research launcher selects a closed allowlist of 12 workers. Its first managed-process gate excludes the fast executor, strategy lab, historical proof producer, lifecycle/evidence workers and legacy outcome writer. Real-money environment flags remain disabled. The legacy recovery watchdog must stay off: it recovers the broader safe-core launcher, not this research-only launcher.

The saved study is waiting for a tradeable quote, with zero errors and zero collected forecasts or outcomes. All 68 saved quotes are nontradeable and contain Friday market times. These are expected pre-opening observations, not current prices. The seven required candle tails currently have only seven common contiguous bars at the final Friday boundary, and the latest common close is over 46 hours old.

After the weekend gap, the registered model requires 335 completed common M1 bars across all seven pairs: about 5 hours 35 minutes of continuous common data. Archive publication, the 15-minute model cadence and model build add delay. The first H1 outcomes require another hour after the original reference. Restarting collection therefore does not imply immediate opening forecasts or trades.

The retained authorization has zero confirmed candidates, no authorized entries and entry_authorized=false. It is over 41 hours old, against the executor's 900-second maximum age. The lifecycle contains 0 confirmed candidates, 43,872 still collecting and 9,485 rejected. Fresh candidate-specific authorization and verifier evidence would still be required even if the executor were separately enabled.

The prior full-project integrity snapshot is degraded, including stale or inactive research checks; it is not an all-clear. Its selected collection systems need fresh verification after launch. The last clock snapshot is mitigated with no active clock discontinuity but no broker clock sample. The last storage snapshot reports 95.271 GiB free, above its 50 GiB minimum. All of these are timestamped saved states, not assertions of present process health.

Source evidence is in INDEPENDENT_COLLECTION_READINESS_20260906.json. It includes exact source hashes, selected state timestamps, contract/dependency checks and bounded candle-tail receipts. The process/task snapshot is a separate read-only observation. No broker calls, credentials, production databases, process/task changes, source/config changes or runtime starts were performed by this review.

Source locations reviewed: start_oanda_research_collection.ps1; oanda_always_on_supervisor.ps1 (allowlist around 104, first process gate around 544, study launch around 3217, executor launch around 3629); oanda_supervisor_watchdog.ps1 (safe-core launcher around 31); oanda_causal_forecast_study.py (source/version check around 62, quote gate around 85); oanda_causal_forecast_inputs.py (contiguous shared suffix around 159).
