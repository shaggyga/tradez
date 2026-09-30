# Historical link map

Observed 2026-09-21T18:11:21.133429+00:00. This document preserves the **81 observed missing local-link occurrences** from a bounded scan of Forex Vault root Markdown documents. Original documents and their links were not rewritten. It is intended for `thevault/projects/forex/DESIGN_ALIGNMENT_20260921`; direct-root links use `../<filename>`.

## Exact scan scope

- Root: `C:\Users\zmoor\OneDrive\thevault\projects\forex`.
- Included 51 top-level Markdown files of at most 100,000 bytes; no recursion into dated packages. The exact filename set appears below.
- Parsed ordinary inline Markdown links matching `[label](target)`. Anchor-only, HTTP(S), and mail links were excluded. URI escaping was decoded and fragment suffixes ignored for path-presence checks. This is a bounded link-pattern scan, not a complete Markdown parser or anchor validator.
- Checked 215 local link occurrences whose normalized target remained under `C:\Users\zmoor\OneDrive\thevault`. 37 targets outside that boundary were not followed. No D-drive access occurred.
- Found 81 missing occurrences across 16 source documents. Repeated occurrences are retained separately, including repeated targets on the same line.
- 6 occurrences have one of the three confirmed mappings below; 75 remain explicitly unresolved. A missing loose-file target may be a source-archive member or external historical path, not proof that the underlying evidence is absent everywhere.
- Huge Markdown payloads, archive payload searches, credentials, and secrets were not inspected. Filesystem presence does not prove content correctness or cloud synchronization.

## Three confirmed mappings

Fresh hashes of the three existing destination files match the SHA-256 values recorded for the corresponding members of the bound source manifest. This pass did not reopen those archive members; confirmation means hash agreement with their manifest records, not a new archive extraction.

Source manifest: [source\forex_worktree_source_9ba4f7d6ff91a501f414d8bf.manifest.json](../source\forex_worktree_source_9ba4f7d6ff91a501f414d8bf.manifest.json). Manifest SHA-256: `c5ef60ed9034e85bec475513b533b925e0bec13ac9f3d233b8d2917546ebddbe`. Source snapshot ID: `9ba4f7d6ff91a501f414d8bfee96a0385cdb2ce0a92bb39b923e48124324a3d4`.

| Historical target text | Existing Vault destination | Recorded source member | SHA-256 |
|---|---|---|---|
| `FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md` | [OPERATIONAL_STATUS_REPAIR_CURRENT.md](../OPERATIONAL_STATUS_REPAIR_CURRENT.md) | `docs/FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md` | `aba30e7ed9eaf8890363dcea2466c2c74e1c98c9c0f57349f44c5c48791d4975` |
| `FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md` | [PAIR_DASHBOARD_CONSISTENCY_CURRENT.md](../PAIR_DASHBOARD_CONSISTENCY_CURRENT.md) | `docs/FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md` | `fd8b517f629d49010bc5f9d20d5206912cd71e517c75f8c0d3a9d576aae63f55` |
| `FOREX_PAIR_FORECAST_COVERAGE_20260907.md` | [PAIR_FORECAST_COVERAGE_CURRENT.md](../PAIR_FORECAST_COVERAGE_CURRENT.md) | `docs/FOREX_PAIR_FORECAST_COVERAGE_20260907.md` | `89b6d19738912474ac203ec6f743b7502d07d2a6ecda2eec0b6b054e13190843` |

## All observed missing occurrences

A mapped destination is a navigation aid. No redirect, alias file, symlink, or replacement was created. Unresolved rows require a separate bounded provenance lookup before any link is changed; do not guess a replacement or copy sensitive evidence into the Vault.

| # | Source document | Source line at scan | Original missing target | Resolution |
|---:|---|---:|---|---|
| 1 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 14 | `FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md` | SHA-matched mapping: [OPERATIONAL_STATUS_REPAIR_CURRENT.md](../OPERATIONAL_STATUS_REPAIR_CURRENT.md) |
| 2 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 14 | `../FOREX_OPERATIONAL_STATUS_REPAIR_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 3 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 25 | `FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md` | SHA-matched mapping: [PAIR_DASHBOARD_CONSISTENCY_CURRENT.md](../PAIR_DASHBOARD_CONSISTENCY_CURRENT.md) |
| 4 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 25 | `../FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 5 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 40 | `FOREX_PAIR_FORECAST_COVERAGE_20260907.md` | SHA-matched mapping: [PAIR_FORECAST_COVERAGE_CURRENT.md](../PAIR_FORECAST_COVERAGE_CURRENT.md) |
| 6 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 40 | `../FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 7 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 54 | `../FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 8 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 72 | `FOREX_SIGNALS_LIVE_OPTIMIZATION_20260907.md` | Unresolved in this bounded scan |
| 9 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 73 | `../FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 10 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 90 | `FOREX_MARKET_OPEN_20260906.md` | Unresolved in this bounded scan |
| 11 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 96 | `FOREX_ENTRY_IMPROVEMENTS_20260906.md` | Unresolved in this bounded scan |
| 12 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 103 | `FOREX_PREDICTION_SANITY_20260906.md` | Unresolved in this bounded scan |
| 13 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 104 | `FOREX_MODEL_INVENTORY_20260906.md` | Unresolved in this bounded scan |
| 14 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 114 | `FOREX_CAUSAL_IO_REPAIR_20260906.md` | Unresolved in this bounded scan |
| 15 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 122 | `FOREX_CAUSAL_TIMING_REPAIR_20260906.md` | Unresolved in this bounded scan |
| 16 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 127 | `FOREX_RESEARCH_RESTART_20260906.md` | Unresolved in this bounded scan |
| 17 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 131 | `FOREX_FIXED_EVALUATION_20260906.md` | Unresolved in this bounded scan |
| 18 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 138 | `FOREX_OPTIMIZATION_REVIEW_20260906.md` | Unresolved in this bounded scan |
| 19 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 139 | `FOREX_PREDICTION_QUALITY_20260906.md` | Unresolved in this bounded scan |
| 20 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 148 | `FOREX_REPAIR_REVIEW_20260905.md` | Unresolved in this bounded scan |
| 21 | [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md) | 149 | `FOREX_PERFORMANCE_AUDIT_20260905.md` | Unresolved in this bounded scan |
| 22 | [CAUSAL_IO_REPAIR_CURRENT.md](../CAUSAL_IO_REPAIR_CURRENT.md) | 4 | `FOREX_CAUSAL_TIMING_REPAIR_20260906.md` | Unresolved in this bounded scan |
| 23 | [ENTRY_IMPROVEMENTS_CURRENT.md](../ENTRY_IMPROVEMENTS_CURRENT.md) | 22 | `FOREX_WEEK_ENTRY_AUDIT_20260906.md` | Unresolved in this bounded scan |
| 24 | [ENTRY_IMPROVEMENTS_CURRENT.md](../ENTRY_IMPROVEMENTS_CURRENT.md) | 40 | `../FOREX_ENTRY_RESEARCH_FOLLOWUP_20260906.json` | Unresolved in this bounded scan |
| 25 | [FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md](../FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md) | 37 | `validation/joint_price_news_20260907/price_v2_first_outcomes/PRICE_V2_FIRST_OUTCOMES_ASSESSMENT_20260907.json` | Unresolved in this bounded scan |
| 26 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 29 | `execution/execution_gate_reproduction.json` | Unresolved in this bounded scan |
| 27 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 29 | `execution/EXECUTION_AUDIT.md` | Unresolved in this bounded scan |
| 28 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 45 | `evidence/evidence_clock_reproduction.json` | Unresolved in this bounded scan |
| 29 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 51 | `known_fastlane_faults.json` | Unresolved in this bounded scan |
| 30 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 59 | `late_commit_reproduction.py` | Unresolved in this bounded scan |
| 31 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 59 | `late_commit_reproduction.json` | Unresolved in this bounded scan |
| 32 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 73 | `recovery/credential_bypass_reproduction.json` | Unresolved in this bounded scan |
| 33 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 79 | `recovery/source_only_register_validation.json` | Unresolved in this bounded scan |
| 34 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 108 | `runtime_observation.json` | Unresolved in this bounded scan |
| 35 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 110 | `evidence/EVIDENCE_AUDIT.md` | Unresolved in this bounded scan |
| 36 | [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md) | 110 | `recovery/RECOVERY_AUDIT.md` | Unresolved in this bounded scan |
| 37 | [JOINT_PRICE_NEWS_CURRENT.md](../JOINT_PRICE_NEWS_CURRENT.md) | 58 | `../FOREX_JOINT_PRICE_NEWS_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 38 | [JOINT_PRICE_NEWS_CURRENT.md](../JOINT_PRICE_NEWS_CURRENT.md) | 58 | `validation/joint_price_news_20260907/CURATED_EVIDENCE_COPY_MANIFEST_20260907.json` | Unresolved in this bounded scan |
| 39 | [NEWS_IDENTITY_CANDIDATE_CURRENT.md](../NEWS_IDENTITY_CANDIDATE_CURRENT.md) | 5 | `FOREX_REVAMP_BASELINE_RECOVERY_20260908.md` | Unresolved in this bounded scan |
| 40 | [NEWS_IDENTITY_CANDIDATE_CURRENT.md](../NEWS_IDENTITY_CANDIDATE_CURRENT.md) | 5 | `../FOREX_PENDING_IMPROVEMENTS.md` | Unresolved in this bounded scan |
| 41 | [NEWS_RESEARCH_DEPLOYMENT_CURRENT.md](../NEWS_RESEARCH_DEPLOYMENT_CURRENT.md) | 52 | `../FOREX_PENDING_IMPROVEMENTS.md` | Unresolved in this bounded scan |
| 42 | [NEWS_RESEARCH_DEPLOYMENT_CURRENT.md](../NEWS_RESEARCH_DEPLOYMENT_CURRENT.md) | 56 | `../FOREX_NEWS_RESEARCH_DEPLOYMENT_VALIDATION_20260908.json` | Unresolved in this bounded scan |
| 43 | [NEWS_RESEARCH_DEPLOYMENT_CURRENT.md](../NEWS_RESEARCH_DEPLOYMENT_CURRENT.md) | 57 | `validation/news_identity_integration_20260908/EVIDENCE_COPY_MANIFEST_20260908.json` | Unresolved in this bounded scan |
| 44 | [NEWS_RESEARCH_DEPLOYMENT_CURRENT.md](../NEWS_RESEARCH_DEPLOYMENT_CURRENT.md) | 58 | `validation/news_research_deployment_20260908/DEPLOYMENT_EVIDENCE_MANIFEST_20260908.json` | Unresolved in this bounded scan |
| 45 | [NEWS_RESEARCH_DEPLOYMENT_CURRENT.md](../NEWS_RESEARCH_DEPLOYMENT_CURRENT.md) | 59 | `FOREX_REVAMP_BASELINE_RECOVERY_20260908.md` | Unresolved in this bounded scan |
| 46 | [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md) | 36 | `../FOREX_OPTIMIZATION_VALIDATION_20260906.json` | Unresolved in this bounded scan |
| 47 | [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md) | 36 | `validation/optimization_20260906/integrity_publication_benchmark.json` | Unresolved in this bounded scan |
| 48 | [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md) | 36 | `validation/optimization_20260906/integrity_consumer_review.md` | Unresolved in this bounded scan |
| 49 | [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md) | 46 | `FOREX_PREDICTION_QUALITY_20260906.md` | Unresolved in this bounded scan |
| 50 | [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md) | 46 | `../FOREX_PREDICTION_QUALITY_20260906.json` | Unresolved in this bounded scan |
| 51 | [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md) | 50 | `../FOREX_OPTIMIZATION_BACKLOG_20260906.json` | Unresolved in this bounded scan |
| 52 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 15 | `validation/overnight_curve_buildout_20260909/joint_v3_reassessment_v1/calibration_v1/actual_calibration_002/JOINT_PROBABILITY_BINS_20260909.json` | Unresolved in this bounded scan |
| 53 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 17 | `validation/overnight_curve_buildout_20260909/prospective_pilot/complete_results_v3_002/COMPLETE_PILOT_HORIZON_RESULTS_20260909.md` | Unresolved in this bounded scan |
| 54 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 31 | `validation/overnight_curve_buildout_20260909/observed_management/OBSERVED_THREE_EPISODE_FINAL_ACCEPTANCE_20260909.json` | Unresolved in this bounded scan |
| 55 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 37 | `validation/overnight_curve_buildout_20260909/management_correctness_audit_v1/ACCOUNT_RECONCILIATION_SOURCE_TRANSITION_V2_20260909.json` | Unresolved in this bounded scan |
| 56 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 39 | `validation/overnight_curve_buildout_20260909/replay/chain_bridge_v1/CURVE_CHAIN_MANAGEMENT_BRIDGE_CONTRACT_20260909.md` | Unresolved in this bounded scan |
| 57 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 41 | `validation/overnight_curve_buildout_20260909/replay/chain_bridge_v1/ACTUAL_RETAINED_H3_BRIDGE_DIAGNOSTIC_20260909.md` | Unresolved in this bounded scan |
| 58 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 45 | `validation/overnight_curve_buildout_20260909/richer_inputs/prospective_risk_v1/complete_results_004/COMPLETE_RETAINED_RISK_RESULTS_20260909.json` | Unresolved in this bounded scan |
| 59 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 57 | `validation/overnight_curve_buildout_20260909/ledger_endpoint_reliability_probe_v1/LEDGER_ENDPOINT_V2_RELIABILITY_ASSESSMENT_20260909.json` | Unresolved in this bounded scan |
| 60 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 61 | `validation/overnight_curve_buildout_20260909/try_pair_publication_audit_v1/TRY_READINESS_REFRESH_001_20260909.json` | Unresolved in this bounded scan |
| 61 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 63 | `validation/overnight_curve_buildout_20260909/operations_v3/OPERATIONS_V3_OBSERVATION_004_20260909.json` | Unresolved in this bounded scan |
| 62 | [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) | 69 | `validation/overnight_curve_buildout_20260909/feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json` | Unresolved in this bounded scan |
| 63 | [PAIR_FAMILY_REPAIR_CURRENT.md](../PAIR_FAMILY_REPAIR_CURRENT.md) | 15 | `FOREX_JOINT_PRICE_NEWS_20260907.md` | Unresolved in this bounded scan |
| 64 | [PAIR_FAMILY_REPAIR_CURRENT.md](../PAIR_FAMILY_REPAIR_CURRENT.md) | 17 | `../FOREX_PAIR_FAMILY_REPAIR_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 65 | [PREDICTION_QUALITY_CURRENT.md](../PREDICTION_QUALITY_CURRENT.md) | 3 | `../FOREX_PREDICTION_QUALITY_20260906.json` | Unresolved in this bounded scan |
| 66 | [PREDICTION_QUALITY_CURRENT.md](../PREDICTION_QUALITY_CURRENT.md) | 3 | `validation/assess_saved_predictions_20260906.py` | Unresolved in this bounded scan |
| 67 | [PREDICTION_SANITY_CURRENT.md](../PREDICTION_SANITY_CURRENT.md) | 106 | `FOREX_MODEL_INVENTORY_20260906.md` | Unresolved in this bounded scan |
| 68 | [PREDICTION_SANITY_CURRENT.md](../PREDICTION_SANITY_CURRENT.md) | 131 | `validation/prediction_sanity_20260906/comovement/comovement_sanity.svg` | Unresolved in this bounded scan |
| 69 | [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md) | 14 | `FOREX_OPERATIONAL_STATUS_REPAIR_20260907.md` | SHA-matched mapping: [OPERATIONAL_STATUS_REPAIR_CURRENT.md](../OPERATIONAL_STATUS_REPAIR_CURRENT.md) |
| 70 | [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md) | 14 | `../FOREX_OPERATIONAL_STATUS_REPAIR_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 71 | [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md) | 25 | `FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md` | SHA-matched mapping: [PAIR_DASHBOARD_CONSISTENCY_CURRENT.md](../PAIR_DASHBOARD_CONSISTENCY_CURRENT.md) |
| 72 | [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md) | 25 | `../FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 73 | [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md) | 40 | `FOREX_PAIR_FORECAST_COVERAGE_20260907.md` | SHA-matched mapping: [PAIR_FORECAST_COVERAGE_CURRENT.md](../PAIR_FORECAST_COVERAGE_CURRENT.md) |
| 74 | [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md) | 40 | `../FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 75 | [REPAIR_REVIEW_CURRENT.md](../REPAIR_REVIEW_CURRENT.md) | 43 | `FOREX_PERFORMANCE_AUDIT_20260905.md` | Unresolved in this bounded scan |
| 76 | [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md) | 3 | `../FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json` | Unresolved in this bounded scan |
| 77 | [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md) | 9 | `../config/causal_forecast_study_gap_v2_20260907.json` | Unresolved in this bounded scan |
| 78 | [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md) | 13 | `../config/causal_forecast_study_eurusd_v1_20260907.json` | Unresolved in this bounded scan |
| 79 | [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md) | 21 | `../oanda_weekend_reopening_baseline.py` | Unresolved in this bounded scan |
| 80 | [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md) | 35 | `FOREX_PREDICTION_SANITY_20260906.md` | Unresolved in this bounded scan |
| 81 | [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md) | 37 | `../FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json` | Unresolved in this bounded scan |

## Exact included Markdown documents

- [ACTIVE_PIPELINE.md](../ACTIVE_PIPELINE.md)
- [AGENTS.md](../AGENTS.md)
- [AUDIT_START_HERE.md](../AUDIT_START_HERE.md)
- [AUDIT_STATE_CURRENT.md](../AUDIT_STATE_CURRENT.md)
- [AUTHORITY_AND_REUSE_MAP_20260921.md](../AUTHORITY_AND_REUSE_MAP_20260921.md)
- [BLURB_DATASET_AUDIT_CURRENT.md](../BLURB_DATASET_AUDIT_CURRENT.md)
- [CAUSAL_IO_REPAIR_CURRENT.md](../CAUSAL_IO_REPAIR_CURRENT.md)
- [CAUSAL_TIMING_REPAIR_CURRENT.md](../CAUSAL_TIMING_REPAIR_CURRENT.md)
- [CURRENT_NEWS_AUDIT_AND_REPAIR.md](../CURRENT_NEWS_AUDIT_AND_REPAIR.md)
- [ENTRY_IMPROVEMENTS_CURRENT.md](../ENTRY_IMPROVEMENTS_CURRENT.md)
- [EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md](../EXISTING_FEATURE_HORIZON_AUDIT_CURRENT.md)
- [FAULT_AUDIT_CURRENT.md](../FAULT_AUDIT_CURRENT.md)
- [FEATURE_GENERATION_WORKED_EXAMPLE.md](../FEATURE_GENERATION_WORKED_EXAMPLE.md)
- [FIXED_EVALUATION_CURRENT.md](../FIXED_EVALUATION_CURRENT.md)
- [FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md](../FOREX_PRICE_V2_FIRST_OUTCOMES_20260907.md)
- [HORIZON_COVERAGE_REVIEW_CURRENT.md](../HORIZON_COVERAGE_REVIEW_CURRENT.md)
- [INDEPENDENT_AUDIT_20260905.md](../INDEPENDENT_AUDIT_20260905.md)
- [JOINT_PRICE_NEWS_CURRENT.md](../JOINT_PRICE_NEWS_CURRENT.md)
- [KNOWLEDGE_INDEX.md](../KNOWLEDGE_INDEX.md)
- [LIVE_WATCH_REPORT_20260907.md](../LIVE_WATCH_REPORT_20260907.md)
- [MARKET_OPEN_CURRENT.md](../MARKET_OPEN_CURRENT.md)
- [MODEL_FEATURE_SPACE_CURRENT.md](../MODEL_FEATURE_SPACE_CURRENT.md)
- [MODEL_INVENTORY_CURRENT.md](../MODEL_INVENTORY_CURRENT.md)
- [NEWS_IDENTITY_CANDIDATE_CURRENT.md](../NEWS_IDENTITY_CANDIDATE_CURRENT.md)
- [NEWS_RESEARCH_DEPLOYMENT_CURRENT.md](../NEWS_RESEARCH_DEPLOYMENT_CURRENT.md)
- [OPERATIONAL_STATUS_REPAIR_CURRENT.md](../OPERATIONAL_STATUS_REPAIR_CURRENT.md)
- [OPTIMIZATION_REVIEW_CURRENT.md](../OPTIMIZATION_REVIEW_CURRENT.md)
- [OVERNIGHT_CURVE_BUILDOUT_CURRENT.md](../OVERNIGHT_CURVE_BUILDOUT_CURRENT.md)
- [PAIR_DASHBOARD_CONSISTENCY_CURRENT.md](../PAIR_DASHBOARD_CONSISTENCY_CURRENT.md)
- [PAIR_FAMILY_REPAIR_CURRENT.md](../PAIR_FAMILY_REPAIR_CURRENT.md)
- [PAIR_FORECAST_COVERAGE_CURRENT.md](../PAIR_FORECAST_COVERAGE_CURRENT.md)
- [PENDING_IMPROVEMENTS_CURRENT.md](../PENDING_IMPROVEMENTS_CURRENT.md)
- [PERFORMANCE_AUDIT_CURRENT.md](../PERFORMANCE_AUDIT_CURRENT.md)
- [PRACTICE_RESUME_20260910.md](../PRACTICE_RESUME_20260910.md)
- [PRACTICE_TRADE_REVIEW_20260909.md](../PRACTICE_TRADE_REVIEW_20260909.md)
- [PRACTICE_TRIAL_20260909.md](../PRACTICE_TRIAL_20260909.md)
- [PREDICTION_QUALITY_CURRENT.md](../PREDICTION_QUALITY_CURRENT.md)
- [PREDICTION_SANITY_CURRENT.md](../PREDICTION_SANITY_CURRENT.md)
- [PROJECT_HISTORY.md](../PROJECT_HISTORY.md)
- [README.md](../README.md)
- [RECREATION.md](../RECREATION.md)
- [RECREATION_HISTORY_THROUGH_20260907.md](../RECREATION_HISTORY_THROUGH_20260907.md)
- [REPAIR_REVIEW_CURRENT.md](../REPAIR_REVIEW_CURRENT.md)
- [RESEARCH_INDEX.md](../RESEARCH_INDEX.md)
- [RESEARCH_RESTART_CURRENT.md](../RESEARCH_RESTART_CURRENT.md)
- [REVAMP_BASELINE_RECOVERY_CURRENT.md](../REVAMP_BASELINE_RECOVERY_CURRENT.md)
- [SIGNALS_LIVE_OPTIMIZATION_CURRENT.md](../SIGNALS_LIVE_OPTIMIZATION_CURRENT.md)
- [SYSTEM_GUIDE.md](../SYSTEM_GUIDE.md)
- [VAULT_AUDIT_SUMMARY_20260921.md](../VAULT_AUDIT_SUMMARY_20260921.md)
- [WEEK_ENTRY_AUDIT_CURRENT.md](../WEEK_ENTRY_AUDIT_CURRENT.md)
- [WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_CURRENT.md](../WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_CURRENT.md)

## Preservation rule

Keep sealed checkpoint contents, historical receipt bytes, manifests, and original links intact unless a separately reviewed change updates their dependent references and preserves prior bytes. This map is a dated companion, not a replacement for those artifacts. The older whole-Vault readability report has a different, broader scope; its historical broken-link count must not be confused with this root-only 81-occurrence scan.

