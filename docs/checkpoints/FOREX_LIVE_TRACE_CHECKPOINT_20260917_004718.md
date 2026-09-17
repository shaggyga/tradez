# Forex live trace checkpoint 20260917_004718

Created: 2026-09-17T00:47:19.5499625-04:00
Project: C:\Users\zmoor\Documents\forex\trad

## Purpose

This checkpoint records the current live trace-mapping run and project state without changing 007 or unlocking 006 execution. It is intended as a reproducible handoff point before any larger cleanup, repo creation, or source consolidation.

## Live monitor

- Monitor script: data\oanda_training_manager\trace_mapping_20260917_morning\trace_mapping_monitor_until_0945_20260917_fixed.ps1
- Full JSONL log: data\oanda_training_manager\trace_mapping_20260917_morning\trace_mapping_monitor_until_0945_20260917_fixed.jsonl
- Compact event log: data\oanda_training_manager\trace_mapping_20260917_morning\trace_mapping_events_until_0945_20260917_fixed.jsonl
- Snapshot helper: data\oanda_training_manager\trace_mapping_20260917_morning\trace_extract_snapshot_20260917.py
- Execution posture: 006 remains blocked by loss lock; 007 unchanged.

## Current compact status

`json
{
  "exists": true,
  "path": "data\\oanda_training_manager\\trace_mapping_20260917_morning\\trace_mapping_monitor_until_0945_20260917_fixed.jsonl",
  "file_mtime": "2026-09-17T00:46:29.794902",
  "rows": 18,
  "first": "2026-09-17T00:29:13",
  "last": "2026-09-17T00:46:28",
  "last_event": null,
  "regimes": {
    "RISK_ON": 1,
    "USD_WEAKNESS": 16,
    "MIXED": 1
  },
  "selected_counts": [
    {
      "instrument": "USD_JPY",
      "side": "sell",
      "count": 7
    },
    {
      "instrument": "EUR_HKD",
      "side": "buy",
      "count": 2
    },
    {
      "instrument": "EUR_JPY",
      "side": "buy",
      "count": 1
    },
    {
      "instrument": "USD_ZAR",
      "side": "sell",
      "count": 1
    }
  ],
  "top_move_presence": [
    {
      "instrument": "NZD_HKD",
      "count": 18
    },
    {
      "instrument": "USD_ZAR",
      "count": 17
    },
    {
      "instrument": "NZD_USD",
      "count": 17
    },
    {
      "instrument": "AUD_HKD",
      "count": 17
    },
    {
      "instrument": "CHF_ZAR",
      "count": 15
    },
    {
      "instrument": "ZAR_JPY",
      "count": 14
    },
    {
      "instrument": "AUD_USD",
      "count": 13
    },
    {
      "instrument": "EUR_ZAR",
      "count": 12
    },
    {
      "instrument": "USD_PLN",
      "count": 8
    },
    {
      "instrument": "NZD_CAD",
      "count": 8
    }
  ],
  "last_regime": "USD_WEAKNESS",
  "last_selected": [
    [
      "USD_ZAR",
      "sell",
      7.005636
    ]
  ],
  "last_top_moves": [
    {
      "instrument": "USD_ZAR",
      "move15_pips": -140.3,
      "move5_pips": -115.1,
      "move60_pips": -299.25,
      "ret15_bps": -8.589714,
      "ret5_bps": -7.047415,
      "ret60_bps": -18.312348,
      "spread_bps": 5.306893,
      "tech_finite": 216,
      "tech_missing": []
    },
    {
      "instrument": "ZAR_JPY",
      "move15_pips": 0.75,
      "move5_pips": 0.6,
      "move60_pips": 1.45,
      "ret15_bps": 7.852376,
      "ret5_bps": 6.281407,
      "ret60_bps": 15.186827,
      "spread_bps": 25.117739,
      "tech_finite": 216,
      "tech_missing": []
    },
    {
      "instrument": "CHF_ZAR",
      "move15_pips": -144.35,
      "move5_pips": -90.95,
      "move60_pips": -256.65,
      "ret15_bps": -7.296526,
      "ret5_bps": -4.597912,
      "ret60_bps": -12.969327,
      "spread_bps": 6.373628,
      "tech_finite": 216,
      "tech_missing": []
    },
    {
      "instrument": "EUR_ZAR",
      "move15_pips": -137.2,
      "move5_pips": -104.85,
      "move60_pips": -237.95,
      "ret15_bps": -7.326898,
      "ret5_bps": -5.599793,
      "ret60_bps": -12.703838,
      "spread_bps": 6.047988,
      "tech_finite": 216,
      "tech_missing": []
    },
    {
      "instrument": "GBP_ZAR",
      "move15_pips": -167.05,
      "move5_pips": -118.95,
      "move60_pips": -268.55,
      "ret15_bps": -7.644743,
      "ret5_bps": -5.444132,
      "ret60_bps": -12.286856,
      "spread_bps": 6.522085,
      "tech_finite": 216,
      "tech_missing": []
    }
  ],
  "last_coverage": {
    "current_pairs": 65,
    "fully_finite_pairs": 65,
    "histories_read": 68,
    "minimal_feature_count": 9,
    "minimal_feature_current_pairs": 65,
    "minimal_feature_finite_pairs": 68,
    "movement_feature_count": 2,
    "movement_feature_current_pairs": 65,
    "movement_feature_finite_pairs": 68,
    "registered_pairs": 68,
    "source_errors": 0,
    "status_counts": {
      "derived_current": 32,
      "observed_current": 33,
      "stale_gap": 3
    }
  },
  "last_account": {
    "pl": "-6.1930",
    "open": 0,
    "unrealizedPL": "0.0000",
    "suffix": "-006",
    "NAV": "43.6023",
    "balance": "43.6023",
    "pending": 0
  },
  "bad_rows": 0
}

`

## Git state

- Branch: main
- Remote: none configured
- Untracked files count: 1540

### Modified / untracked summary

`	ext
 M FOREX_PENDING_IMPROVEMENTS.md
 M FOREX_PROJECT_LOG.md
 M README.md
 M oanda_currency_rank_model.py
 M oanda_feature_forward_worker_v1.py
 M oanda_market_sentiment_ticker.py
 M oanda_practice_all_pairs_opportunity_scalper.py
 M oanda_practice_currency_rank_challenger.py
?? config/derived_technical_features_v1_20260916.json
?? config/feature_forward_operational_v2_20260916.json
?? config/feature_reuse_and_account_role_registry_v1_20260916.json
?? config/joint_price_news_operational_v5_20260916.json
?? config/joint_price_news_operational_v6_20260916.json
?? config/joint_price_news_operational_v6_20260916.json.before_revision_io12_30s_20260916_093900
?? config/operational_runtime_v13_20260916.json
?? config/operational_runtime_v14_20260916.json
?? config/operational_runtime_v15_20260916.json
?? config/operational_runtime_v16_20260916.json
?? config/operational_runtime_v17_20260916.json
?? config/operational_runtime_v18_20260916.json
?? config/operational_runtime_v19_20260916.json
?? config/operational_runtime_v19_20260916.json.bak_bar600_20260916T151445Z
?? config/operational_runtime_v19_20260916.json.bak_quote_age_20260916T151422Z
?? config/revision_consumer_incremental_profile_v2_20260916.json
?? config/revision_news_io_base_operational_v6_20260916.json
?? config/revision_news_io_base_operational_v7_20260916.json
?? config/revision_news_io_operational_v6_20260916.json
?? config/revision_news_io_operational_v7_20260916.json
?? config/revision_transport_operational_v6_20260916.json
?? config/revision_transport_operational_v7_20260916.json
?? config/rolling_technical_dataset_v1_20260915.json
?? config/rolling_technical_operations_v2_20260916.json
?? docs/FOREX_ALL68_STABILITY_20260916.md
?? docs/FOREX_DERIVED_ALL68_FEATURE_FEED_20260916.md
?? docs/FOREX_OPERATIONAL_REPAIR_20260916.md
?? docs/FOREX_ROLLING_MODEL_COMPARISON_20260915.md
?? docs/FOREX_ROLLING_PERIOD_REPLICATION_20260915.md
?? docs/FOREX_ROLLING_SPECIALISTS_20260915.md
?? docs/FOREX_ROLLING_TECHNICAL_DATASET_20260915.md
?? docs/FOREX_STORAGE_CHECKPOINT_20260915.md
?? docs/FOREX_TRAINING_WINDOWS_20260915.md
?? docs/FOREX_V18_DERIVED_SUPERVISOR_CUTOVER_20260916.md
?? docs/validation/all68_stability_20260916/
?? docs/validation/derived_all68_20260916/
?? docs/validation/live_trader_20260916/
?? docs/validation/news_incremental_20260916/
?? docs/validation/operational_pending_20260916/
?? docs/validation/rolling_model_comparison_20260915/
?? docs/validation/rolling_operations_20260916/
?? docs/validation/rolling_period_replication_20260915/
?? docs/validation/rolling_specialists_20260915/
?? docs/validation/rolling_technical_20260915/
?? docs/validation/rolling_training_windows_20260915/
?? docs/validation/v18_derived_supervisor_20260916/
?? oanda_all68_m1_cadence_v3.py
?? oanda_all68_m1_forward_updater_v2.py
?? oanda_all68_technical_availability_v1.py
?? oanda_causal_forecast_ledger_joint_news_v6.py
?? oanda_causal_forecast_ledger_joint_news_v7.py
?? oanda_derived_technical_features_v1.py
?? oanda_derived_technical_publisher_v1.py
?? oanda_feature_forward_reader_v2.py
?? oanda_feature_forward_worker_v2.py
?? oanda_joint_price_news_forecast_study_v8.py
?? oanda_joint_price_news_forecast_study_v9.py
?? oanda_joint_price_news_isolation_status_v1.py
?? oanda_operational_log_relay_v1.py
?? oanda_operational_log_relay_v2.py
?? oanda_operational_recovery_contract_v2.ps1
?? oanda_operational_recovery_contract_v3.ps1
?? oanda_operational_recovery_contract_v4.ps1
?? oanda_operational_recovery_contract_v5.ps1
?? oanda_operational_recovery_contract_v6.ps1
?? oanda_operational_supervisor_v2.ps1
?? oanda_operational_supervisor_v3.ps1
?? oanda_operational_supervisor_v4.ps1
?? oanda_operational_supervisor_v5.ps1
?? oanda_operational_supervisor_v6.ps1
?? oanda_rolling_family_design_v1.py
?? oanda_rolling_model_baselines_v1.py
?? oanda_rolling_model_design_v1.py
?? oanda_rolling_model_scoring_v1.py
?? oanda_rolling_specialist_scoring_v1.py
?? oanda_rolling_specialists_v1.py
?? oanda_rolling_technical_alignment_v2.py
?? oanda_rolling_technical_continuity_v2.py
?? oanda_rolling_technical_dataset_v1.py
?? oanda_rolling_technical_endpoint_labels_v1.py
?? oanda_rolling_technical_features_v1.py
?? oanda_rolling_technical_inputs_v1.py
?? oanda_rolling_technical_labels_v1.py
?? oanda_rolling_technical_panel_batch_v1.py
?? oanda_rolling_technical_panel_v1.py
?? oanda_rolling_technical_ranges_v1.py
?? oanda_rolling_technical_store_v1.py
?? oanda_rolling_technical_worker_v1.py
?? oanda_rolling_technical_worker_v2.py
?? oanda_supervisor_watchdog_v2.ps1
?? oanda_supervisor_watchdog_v3.ps1
?? oanda_supervisor_watchdog_v4.ps1
?? oanda_supervisor_watchdog_v5.ps1
?? oanda_supervisor_watchdog_v6.ps1
?? projection_revision_consumer_v2.py
?? revision_joint_inputs_v4.py
?? revision_joint_inputs_v5.py
?? revision_joint_point_v3.py
?? revision_joint_point_v4.py
?? revision_news_io_v11.py
?? revision_news_io_v12.py
?? revision_transport_v5.py
?? revision_transport_v6.py
?? shared_revision_history_v4.py
?? shared_revision_history_v5.py
?? start_oanda_operational_research_v2.ps1
?? start_oanda_operational_research_v3.ps1
?? start_oanda_operational_research_v4.ps1
?? start_oanda_operational_research_v5.ps1
?? start_oanda_operational_research_v6.ps1
?? start_oanda_supervisor_watchdog_v2.ps1
?? start_oanda_supervisor_watchdog_v3.ps1
?? start_oanda_supervisor_watchdog_v4.ps1
?? start_oanda_supervisor_watchdog_v5.ps1
?? start_oanda_supervisor_watchdog_v6.ps1
?? test_forex_market_brief_v1.py
?? test_oanda_all68_m1_forward_updater_v2.py
?? test_oanda_all68_technical_availability_v1.py
?? test_oanda_derived_technical_features_v1.py
?? test_oanda_derived_technical_publisher_v1.py
?? test_oanda_feature_forward_reader_v2.py
?? test_oanda_joint_price_news_isolation_status_v1.py
?? test_oanda_m1_cadence_v3.py
?? test_oanda_operational_log_relay_v1.py
?? test_oanda_operational_log_relay_v2.py
?? test_oanda_operational_recovery_v2.py
?? test_oanda_operational_recovery_v3.py
?? test_oanda_operational_recovery_v4.py
?? test_oanda_operational_recovery_v5.py
?? test_oanda_operational_recovery_v6.py
?? test_oanda_practice_all_pairs_execution_gate.py
?? test_oanda_rolling_family_design_v1.py
?? test_oanda_rolling_model_baselines_v1.py
?? test_oanda_rolling_model_design_v1.py
?? test_oanda_rolling_model_scoring_v1.py
?? test_oanda_rolling_specialist_scoring_v1.py
?? test_oanda_rolling_specialists_v1.py
?? test_oanda_rolling_technical_alignment_v2.py
?? test_oanda_rolling_technical_continuity_v2.py
?? test_oanda_rolling_technical_dataset_v1.py
?? test_oanda_rolling_technical_endpoint_labels_v1.py
?? test_oanda_rolling_technical_features_v1.py
?? test_oanda_rolling_technical_inputs_v1.py
?? test_oanda_rolling_technical_labels_v1.py
?? test_oanda_rolling_technical_panel_batch_v1.py
?? test_oanda_rolling_technical_panel_v1.py
?? test_oanda_rolling_technical_ranges_v1.py
?? test_oanda_rolling_technical_store_v1.py
?? test_oanda_rolling_technical_worker_v1.py
?? test_oanda_rolling_technical_worker_v2.py
?? test_projection_revision_consumer_v2.py
?? test_publish_operational_checkpoint_v1.py
?? test_revision_news_descendants_v2.py
?? test_revision_news_descendants_v3.py
?? test_revision_news_incremental_v2.py
?? test_revision_news_resumable_v3.py
?? test_revision_transport_v6.py
?? tools/analyze_rolling_specialist_component_baselines_v1.py
?? tools/analyze_rolling_technical_training_v1.py
?? tools/archive_forex_cold_logs_v1.py
?? tools/audit_rolling_family_recovery_v1.py
?? tools/audit_rolling_model_comparison_v1.py
?? tools/audit_rolling_period_replication_v1.py
?? tools/audit_rolling_specialists_v1.py
?? tools/build_rolling_comparison_quotes_v1.py
?? tools/build_rolling_technical_dataset_v1.py
?? tools/build_rolling_technical_endpoints_v1.py
?? tools/build_rolling_technical_history.py
?? tools/check_rolling_alignment_live_v2.py
?? tools/check_rolling_operations_live_v2.py
?? tools/check_rolling_operations_resume_v2.py
?? tools/export_rolling_specialist_normalizers_v1.py
?? tools/forex_market_brief_v1.py
?? tools/prepare_rolling_model_comparison_v1.py
?? tools/prepare_rolling_specialists_v1.py
?? tools/prepare_rolling_specialists_v2.py
?? tools/publish_operational_checkpoint_v1.py
?? tools/publish_rolling_period_checkpoint_v1.py
?? tools/publish_rolling_specialist_checkpoint_v1.py
?? tools/resume_rolling_family_replication_v1.py
?? tools/rolling_empty_binning_compat_v1.py
?? tools/run_rolling_model_comparison_v1.py
?? tools/run_rolling_specialists_v1.py
?? tools/run_rolling_specialists_v2.py
?? tools/summarize_rolling_model_comparison_v1.py
?? tools/summarize_rolling_period_replication_v1.py
?? tools/summarize_rolling_specialists_v1.py
?? tools/test_analyze_rolling_specialist_component_baselines_v1.py
?? tools/test_analyze_rolling_technical_training_v1.py
?? tools/test_archive_forex_cold_logs_v1.py
?? tools/test_audit_rolling_family_recovery_v1.py
?? tools/test_audit_rolling_model_comparison_v1.py
?? tools/test_audit_rolling_period_replication_v1.py
?? tools/test_audit_rolling_specialists_v1.py
?? tools/test_build_rolling_comparison_quotes_v1.py
?? tools/test_build_rolling_technical_endpoints_v1.py
?? tools/test_build_rolling_technical_history.py
?? tools/test_export_rolling_specialist_normalizers_v1.py
?? tools/test_prepare_rolling_model_comparison_v1.py
?? tools/test_prepare_rolling_specialists_v1.py
?? tools/test_prepare_rolling_specialists_v2.py
?? tools/test_publish_rolling_period_checkpoint_v1.py
?? tools/test_resume_rolling_family_replication_v1.py
?? tools/test_rolling_empty_binning_compat_v1.py
?? tools/test_run_rolling_model_comparison_v1.py
?? tools/test_run_rolling_specialists_v1.py
?? tools/test_run_rolling_specialists_v2.py
?? tools/test_summarize_rolling_model_comparison_v1.py
?? tools/test_summarize_rolling_period_replication_v1.py
?? tools/test_summarize_rolling_specialists_v1.py

`

### Diff stat for tracked files

`	ext
 FOREX_PENDING_IMPROVEMENTS.md                   |   28 +-
 FOREX_PROJECT_LOG.md                            |  543 +++++-----
 README.md                                       |  100 +-
 oanda_currency_rank_model.py                    |   34 +-
 oanda_feature_forward_worker_v1.py              |  552 ++++++-----
 oanda_market_sentiment_ticker.py                |   79 ++
 oanda_practice_all_pairs_opportunity_scalper.py | 1197 +++++++++++++----------
 oanda_practice_currency_rank_challenger.py      |   28 +-
 8 files changed, 1498 insertions(+), 1063 deletions(-)

`

## Operational interpretation at checkpoint

The live trace run is producing useful movement traces and feature-coverage diagnostics. The strongest repeated pattern so far is broad USD weakness with NZD/AUD strength, but selected-entry logic has often stepped aside as fresh 5m confirmation decayed. This supports the current trace-mapping objective: identify sustained move traces and distinguish early confirmation from late-entry/chase risk.

This checkpoint intentionally does not mark 006 trades as safe and does not change 007.
