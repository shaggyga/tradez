#!/usr/bin/env python3
"""Build an incremental, credential-free Forex model checkpoint."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

try:
    from forex_vault_import import VaultImportError, audit_archive, import_archives
except ModuleNotFoundError:
    from trad.forex_vault_import import (
        VaultImportError,
        audit_archive,
        import_archives,
    )

try:
    from oanda_integrity_publication import (
        DETAIL_DIRECTORY as INTEGRITY_DETAIL_DIRECTORY,
        verified_detail_artifact_bytes,
    )
except ModuleNotFoundError:
    from trad.oanda_integrity_publication import (
        DETAIL_DIRECTORY as INTEGRITY_DETAIL_DIRECTORY,
        verified_detail_artifact_bytes,
    )


SOURCE_EXTENSIONS = {
    ".py", ".ps1", ".cmd", ".md", ".html", ".css", ".js", ".json",
    ".toml", ".yaml", ".yml",
}
MODEL_EXTENSIONS = {
    ".joblib", ".pkl", ".pickle", ".onnx", ".pt", ".pth", ".cbm",
}
PRUNED_DIRS = {
    ".git", ".idea", ".pytest_cache", ".mypy_cache", "__pycache__", ".venv",
    "..venv", "venv", "node_modules", "archive", "raw_decisions",
}
SECRET_NAMES = {"creds", "creds.txt", "creds.py", ".env"}
MODEL_ARCHIVE_RE = re.compile(
    r"^forex_model_checkpoint_(20[0-9]{6}_[0-9]{6})\.zip$"
)
MODEL_VALIDATION_RECEIPT = "forex_model_checkpoint_current.validation.json"
MODEL_RETENTION_RECEIPT_PREFIX = "forex_model_checkpoint_retention_"
DEFAULT_VAULT_PROJECT = (
    Path.home() / "OneDrive" / "thevault" / "projects" / "forex"
)
CANONICAL_PROJECT_RECORDS = (
    (Path("trad/docs/FOREX_PREDICTION_SANITY_20260906.md"), "PREDICTION_SANITY_CURRENT.md"),
    (Path("trad/FOREX_PREDICTION_SANITY_VALIDATION_20260906.json"), "PREDICTION_SANITY_VALIDATION_CURRENT.json"),
    (Path("trad/docs/FOREX_MODEL_INVENTORY_20260906.md"), "MODEL_INVENTORY_CURRENT.md"),
    (Path("trad/FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json"), "COMOVEMENT_RESEARCH_PLAN_CURRENT.json"),
    (Path("trad/docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md"), "CAUSAL_TIMING_REPAIR_CURRENT.md"),
    (Path("trad/FOREX_CAUSAL_TIMING_REPAIR_VALIDATION_20260906.json"), "CAUSAL_TIMING_REPAIR_VALIDATION_CURRENT.json"),
    (Path("trad/FOREX_CAUSAL_IO_FOLLOWUP_20260906.json"), "CAUSAL_TIMING_FOLLOWUP_CURRENT.json"),
    (Path("trad/config/causal_forecast_study_v1_io_r2_20260906.json"), "CAUSAL_FORECAST_STUDY_PROTOCOL_CURRENT.json"),
    (Path("trad/docs/FOREX_CAUSAL_IO_REPAIR_20260906.md"), "CAUSAL_IO_REPAIR_CURRENT.md"),
    (Path("trad/FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json"), "CAUSAL_IO_REPAIR_VALIDATION_CURRENT.json"),
    (Path("trad/docs/FOREX_RESEARCH_RESTART_20260906.md"), "RESEARCH_RESTART_CURRENT.md"),
    (Path("trad/FOREX_RESEARCH_RESTART_VALIDATION_20260906.json"), "RESEARCH_RESTART_VALIDATION_CURRENT.json"),
    (Path("trad/docs/FOREX_FIXED_EVALUATION_20260906.md"), "FIXED_EVALUATION_CURRENT.md"),
    (Path("trad/FOREX_FIXED_EVALUATION_VALIDATION_20260906.json"), "FIXED_EVALUATION_VALIDATION_CURRENT.json"),
    (Path("trad/FOREX_FIXED_EVALUATION_RESULTS_20260906.json"), "FIXED_EVALUATION_RESULTS_CURRENT.json"),
    (Path("trad/FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json"), "FIXED_EVALUATION_FOLLOWUP_CURRENT.json"),
    (Path("trad/config/fixed_forecast_evaluation_v1_20260906.json"), "FIXED_EVALUATION_PROTOCOL_CURRENT.json"),
    (Path("trad/docs/FOREX_OPTIMIZATION_REVIEW_20260906.md"), "OPTIMIZATION_REVIEW_CURRENT.md"),
    (Path("trad/FOREX_OPTIMIZATION_VALIDATION_20260906.json"), "OPTIMIZATION_VALIDATION_CURRENT.json"),
    (Path("trad/docs/FOREX_PREDICTION_QUALITY_20260906.md"), "PREDICTION_QUALITY_CURRENT.md"),
    (Path("trad/FOREX_PREDICTION_QUALITY_20260906.json"), "PREDICTION_QUALITY_CURRENT.json"),
    (Path("trad/FOREX_OPTIMIZATION_BACKLOG_20260906.json"), "OPTIMIZATION_BACKLOG_CURRENT.json"),
    (Path("trad/docs/FOREX_REPAIR_REVIEW_20260905.md"), "REPAIR_REVIEW_CURRENT.md"),
    (Path("trad/FOREX_REPAIR_VALIDATION_20260905.json"), "REPAIR_VALIDATION_CURRENT.json"),
    (Path("trad/docs/FOREX_PERFORMANCE_AUDIT_20260905.md"), "PERFORMANCE_AUDIT_CURRENT.md"),
    (Path("trad/FOREX_PERFORMANCE_AUDIT_20260905.json"), "PERFORMANCE_AUDIT_CURRENT.json"),
    (Path("trad/docs/FOREX_INDEPENDENT_AUDIT_20260905.md"), "INDEPENDENT_AUDIT_20260905.md"),
    (Path("trad/FOREX_INDEPENDENT_AUDIT_20260905.json"), "INDEPENDENT_AUDIT_20260905.json"),
    (Path("trad/config/source_factor_response_v9.json"), "SOURCE_FACTOR_RESPONSE_V9_INACTIVE.json"),
    (Path("trad/config/source_conditioned_currency_rank_v8.json"), "SOURCE_CONDITIONED_CURRENCY_RANK_V8_INACTIVE.json"),
    (Path("trad/FOREX_AUDIT_START_HERE.md"), "README.md"),
    (Path("trad/FOREX_AUDIT_START_HERE.md"), "AUDIT_START_HERE.md"),
    (Path("trad/FOREX_AUDIT_STATE_CURRENT.json"), "AUDIT_STATE_CURRENT.json"),
    (Path("trad/docs/AUDIT_STATE_CURRENT.md"), "AUDIT_STATE_CURRENT.md"),
    (Path("trad/docs/FOREX_FAULT_AUDIT_CURRENT.md"), "FAULT_AUDIT_CURRENT.md"),
    (Path("trad/FOREX_FAULT_AUDIT_CURRENT.json"), "FAULT_AUDIT_CURRENT.json"),
    (Path("trad/FOREX_VAULT_RESET_VALIDATION_20260905.json"), "VAULT_RESET_VALIDATION_CURRENT.json"),
    (Path("trad/config/move_first_news_case_cohort_v3_20260901.json"), "MOVE_FIRST_NEWS_CASE_COHORT_CURRENT.json"),
    (Path("trad/docs/VAULT_RECREATION_CURRENT.md"), "RECREATION.md"),
    (Path("trad/config/requirements-research-audit-20260905.txt"), "RESEARCH_DEPENDENCIES_CURRENT.txt"),
    (Path("trad/FOREX_PENDING_IMPROVEMENTS.md"), "PENDING_IMPROVEMENTS_CURRENT.md"),
    (Path("trad/FOREX_PROJECT_LOG.md"), "PROJECT_LOG_CURRENT.md"),
    (Path("trad/FOREX_ISSUE_REGISTER_CURRENT.json"), "ISSUE_REGISTER_CURRENT.json"),
    (
        Path("trad/FOREX_CORRECTNESS_REPAIR_VALIDATION_20260901.json"),
        "CORRECTNESS_REPAIR_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_OFFICIAL_GEOPOLITICAL_AND_SCHEDULED_EVENT_CAPTURE_"
            "VALIDATION_20260901.json"
        ),
        "OFFICIAL_GEOPOLITICAL_AND_SCHEDULED_EVENT_CAPTURE_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_VAULT_CANONICAL_SNAPSHOT_VALIDATION_20260901.json"),
        "VAULT_CANONICAL_SNAPSHOT_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_D_"
            "VALIDATION_20260901.json"
        ),
        "EXECUTABLE_MOVE_CENSUS_COHORT_D_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_E_"
            "VALIDATION_20260901.json"
        ),
        "EXECUTABLE_MOVE_CENSUS_COHORT_E_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_F_"
            "VALIDATION_20260901.json"
        ),
        "EXECUTABLE_MOVE_CENSUS_COHORT_F_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_G_"
            "VALIDATION_20260902.json"
        ),
        "EXECUTABLE_MOVE_CENSUS_COHORT_G_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_INTEGRITY_PUBLICATION_FRESHNESS_"
            "VALIDATION_20260901.json"
        ),
        "INTEGRITY_PUBLICATION_FRESHNESS_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_EVIDENCE_SNAPSHOT_VALIDATION_20260901.json"),
        "EVIDENCE_SNAPSHOT_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_PREFINAL_SELECTION_TELEMETRY_VALIDATION_20260901.json"),
        "PREFINAL_SELECTION_TELEMETRY_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_FACTOR_SUPPORT_DIAGNOSTIC_VALIDATION_20260901.json"),
        "FACTOR_SUPPORT_DIAGNOSTIC_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_FACTOR_SUPPORT_METRICS_VALIDATION_20260901.json"),
        "FACTOR_SUPPORT_METRICS_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_POINT_IN_TIME_INTEGRITY_VALIDATION_20260901.json"),
        "POINT_IN_TIME_INTEGRITY_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_SOURCE_RANK_ABSTAIN_INVENTORY_VALIDATION_20260901.json"),
        "SOURCE_RANK_ABSTAIN_INVENTORY_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json"),
        "BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_OUTCOME_PUBLICATION_ATOMICITY_VALIDATION_20260901.json"),
        "OUTCOME_PUBLICATION_ATOMICITY_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_NEWS_GOVERNANCE_FAST_LANE_VALIDATION_20260901.json"),
        "NEWS_GOVERNANCE_FAST_LANE_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_OPERATIONAL_MAPPING_ALIGNMENT_VALIDATION_20260901.json"),
        "OPERATIONAL_MAPPING_ALIGNMENT_V1_VALIDATION_BASELINE.json",
    ),
    (
        Path(
            "trad/FOREX_OPERATIONAL_MAPPING_SYNDICATION_DEDUP_"
            "VALIDATION_20260901.json"
        ),
        "OPERATIONAL_MAPPING_ALIGNMENT_V2_VALIDATION_BASELINE.json",
    ),
    (
        Path("trad/FOREX_NARRATIVE_FAMILY_DECAY_VALIDATION_20260901.json"),
        "OPERATIONAL_MAPPING_ALIGNMENT_V3_INVALIDATED_BASELINE.json",
    ),
    (
        Path(
            "trad/FOREX_OPERATIONAL_MAPPING_SUBSECOND_CAUSALITY_"
            "VALIDATION_20260902.json"
        ),
        "OPERATIONAL_MAPPING_ALIGNMENT_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_QUOTE_TRADEABILITY_STATUS_"
            "VALIDATION_20260901.json"
        ),
        "QUOTE_TRADEABILITY_STATUS_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_AGGREGATE_SIGNAL_LINEAGE_AND_CONFLICT_AUDIT_"
            "VALIDATION_20260901.json"
        ),
        "AGGREGATE_SIGNAL_LINEAGE_AND_CONFLICT_AUDIT_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SECONDARY_CRYPTO_SOURCE_FACTOR_GATE_"
            "VALIDATION_20260901.json"
        ),
        "SECONDARY_CRYPTO_SOURCE_FACTOR_GATE_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_DIRECT_COMMODITY_ACCESS_AUDIT_20260901.json"),
        "DIRECT_COMMODITY_ACCESS_AUDIT_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_ISSUER_BOUND_POLICY_COMMUNICATION_"
            "VALIDATION_20260901.json"
        ),
        "ISSUER_BOUND_POLICY_COMMUNICATION_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_DISCOVERY_SOURCE_LINEAGE_"
            "VALIDATION_20260901.json"
        ),
        "DISCOVERY_SOURCE_LINEAGE_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_MAJOR_MOVE_CENSUS_LIVENESS_"
            "VALIDATION_20260901.json"
        ),
        "MAJOR_MOVE_CENSUS_LIVENESS_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_LIFECYCLE_PUBLICATION_CONSISTENCY_"
            "VALIDATION_20260901.json"
        ),
        "LIFECYCLE_PUBLICATION_CONSISTENCY_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SOURCE_READINESS_AND_INTEGRITY_FRESHNESS_"
            "VALIDATION_20260901.json"
        ),
        "SOURCE_READINESS_AND_INTEGRITY_FRESHNESS_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_INTEGRITY_RUNTIME_CONTRACT_RELOAD_"
            "VALIDATION_20260902.json"
        ),
        "INTEGRITY_RUNTIME_CONTRACT_RELOAD_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SCHEDULED_FACTOR_AND_INTEGRITY_LIVENESS_"
            "VALIDATION_20260902.json"
        ),
        "SCHEDULED_FACTOR_AND_INTEGRITY_LIVENESS_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_LIFECYCLE_VERIFIER_CADENCE_"
            "VALIDATION_20260902.json"
        ),
        "LIFECYCLE_VERIFIER_CADENCE_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_PROJECT_INTEGRITY_HISTORY_ROTATION_"
            "VALIDATION_20260902.json"
        ),
        "PROJECT_INTEGRITY_HISTORY_ROTATION_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_LIFECYCLE_GENEALOGY_LOCK_RETRY_"
            "VALIDATION_20260902.json"
        ),
        "LIFECYCLE_GENEALOGY_LOCK_RETRY_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_LIFECYCLE_SEMANTIC_REPUBLICATION_"
            "VALIDATION_20260902.json"
        ),
        "LIFECYCLE_SEMANTIC_REPUBLICATION_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SIGNAL_TRIAL_BARRIER_SUMMARY_PERFORMANCE_"
            "VALIDATION_20260902.json"
        ),
        "SIGNAL_TRIAL_BARRIER_SUMMARY_PERFORMANCE_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SUPERVISOR_PROCESS_SNAPSHOT_PERFORMANCE_"
            "VALIDATION_20260902.json"
        ),
        "SUPERVISOR_PROCESS_SNAPSHOT_PERFORMANCE_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SIGNAL_SNAPSHOT_QUOTE_COVERAGE_"
            "VALIDATION_20260902.json"
        ),
        "SIGNAL_SNAPSHOT_QUOTE_COVERAGE_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_RESEARCH_STDOUT_GROWTH_VALIDATION_20260902.json"),
        "RESEARCH_STDOUT_GROWTH_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_BOC_PLACEHOLDER_HANDOFF_VALIDATION_20260902.json"),
        "BOC_PLACEHOLDER_HANDOFF_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_OFFICIAL_EVENT_PAIR_QUOTE_CAPTURE_"
            "VALIDATION_20260902.json"
        ),
        "OFFICIAL_EVENT_PAIR_QUOTE_CAPTURE_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_OFFICIAL_EVENT_PAIR_HORIZON_CAPTURE_"
            "VALIDATION_20260902.json"
        ),
        "OFFICIAL_EVENT_PAIR_HORIZON_CAPTURE_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_OFFICIAL_RELEASE_URL_IDENTITY_DEDUP_"
            "VALIDATION_20260902.json"
        ),
        "OFFICIAL_RELEASE_URL_IDENTITY_DEDUP_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_IMMUTABLE_FIRST_SEEN_ACTIVATION_REPLAY_"
            "VALIDATION_20260902.json"
        ),
        "IMMUTABLE_FIRST_SEEN_ACTIVATION_REPLAY_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_JPY_CONTINUATION_CASE_AUDIT_20260902.json"),
        "JPY_CONTINUATION_CASE_AUDIT_CURRENT.json",
    ),
    (
        Path("trad/FOREX_SEVEN_HOUR_LIVE_WATCH_VALIDATION_20260902.json"),
        "SEVEN_HOUR_LIVE_WATCH_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_GDELT_SECURE_TRANSPORT_INCIDENT_20260902.json"),
        "GDELT_SECURE_TRANSPORT_INCIDENT_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SOURCE_RANK_V7_ATOMIC_PUBLICATION_"
            "VALIDATION_20260902.json"
        ),
        "SOURCE_RANK_V7_ATOMIC_PUBLICATION_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_WEEK_RECAP_CUTOFF_CLOCK_VALIDATION_20260904.json"),
        "WEEK_RECAP_CUTOFF_CLOCK_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_OFFICIAL_EVENT_CAPTURE_READ_CLOCK_VALIDATION_20260904.json"),
        "OFFICIAL_EVENT_CAPTURE_READ_CLOCK_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_BLS_EMPLOYMENT_DIRECT_PATH_VALIDATION_20260904.json"),
        "BLS_EMPLOYMENT_DIRECT_PATH_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_STATCAN_LFS_DIRECT_PATH_VALIDATION_20260904.json"),
        "STATCAN_LFS_DIRECT_PATH_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_CONDITIONAL_POLICY_AND_SYNDICATION_GUARD_"
            "VALIDATION_20260904.json"
        ),
        "CONDITIONAL_POLICY_AND_SYNDICATION_GUARD_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_WATCHLIST_POST_INPUT_READ_CLOCK_"
            "VALIDATION_20260904.json"
        ),
        "WATCHLIST_POST_INPUT_READ_CLOCK_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_JAPAN_EXTERNAL_POLICY_PRESSURE_RESEARCH_"
            "VALIDATION_20260904.json"
        ),
        "JAPAN_EXTERNAL_POLICY_PRESSURE_RESEARCH_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_SQLITE_READER_LOCK_HARDENING_"
            "VALIDATION_20260904.json"
        ),
        "SQLITE_READER_LOCK_HARDENING_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_DEESCALATION_PROPOSAL_GUARD_"
            "VALIDATION_20260904.json"
        ),
        "DEESCALATION_PROPOSAL_GUARD_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_MARKET_STATE_ANALYSIS_GUARD_"
            "VALIDATION_20260904.json"
        ),
        "MARKET_STATE_ANALYSIS_GUARD_VALIDATION_CURRENT.json",
    ),
    (
        Path("trad/FOREX_CLASSIFICATION_HOT_RELOAD_INCIDENT_20260904.json"),
        "CLASSIFICATION_HOT_RELOAD_INCIDENT_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_CONDITIONAL_MARKET_RECAP_GUARD_"
            "VALIDATION_20260904.json"
        ),
        "CONDITIONAL_MARKET_RECAP_GUARD_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_OPPOSING_POLICY_CLAIM_GUARD_"
            "VALIDATION_20260904.json"
        ),
        "OPPOSING_POLICY_CLAIM_GUARD_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_CONFLICT_DURATION_RECAP_GUARD_"
            "VALIDATION_20260904.json"
        ),
        "CONFLICT_DURATION_RECAP_GUARD_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/FOREX_MISS_RECAP_AND_MARKET_CLOSE_WATCH_"
            "VALIDATION_20260904.json"
        ),
        "MISS_RECAP_AND_MARKET_CLOSE_WATCH_VALIDATION_CURRENT.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "week_to_date_event_move_audit/"
            "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_20260904T210000Z.json"
        ),
        "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_CURRENT.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "week_to_date_event_move_audit/"
            "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_20260904T210000Z.md"
        ),
        "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_CURRENT.md",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "week_to_date_event_move_audit/"
            "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_20260904T210000Z.sha256"
        ),
        "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP_CURRENT.sha256",
    ),
    (
        Path("trad/config/forex_source_gap_register_v1.json"),
        "SOURCE_GAP_REGISTER_CURRENT.json",
    ),
    (Path("trad/MODEL_FEATURE_SPACE.md"), "MODEL_FEATURE_SPACE_CURRENT.md"),
    (Path("trad/config/model_feature_space.json"), "MODEL_FEATURE_SPACE_CURRENT.json"),
    (Path("trad/config/news_sources_v1.json"), "NEWS_SOURCES_CURRENT.json"),
    (
        Path("trad/config/official_central_bank_source_map_v1.json"),
        "OFFICIAL_CENTRAL_BANK_SOURCE_MAP_CURRENT.json",
    ),
    (
        Path("trad/config/official_currency_source_depth_v1.json"),
        "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json",
    ),
    (
        Path("trad/config/scheduled_event_quote_capture_v2.json"),
        "SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT_CURRENT.json",
    ),
    (
        Path("trad/config/scheduled_event_factor_reaction_v1_20260902b.json"),
        "SCHEDULED_EVENT_FACTOR_REACTION_CONTRACT_CURRENT.json",
    ),
    (
        Path("trad/config/move_first_live_case_capture_v4_20260901.json"),
        "MOVE_FIRST_LIVE_CASE_CAPTURE_CONTRACT_CURRENT.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "move_first_live_case_capture_v4/"
            "MOVE_FIRST_LIVE_CASE_CAPTURE_CURRENT.json"
        ),
        "MOVE_FIRST_LIVE_CASE_CAPTURE_STATUS_CURRENT.json",
    ),
    (
        Path("trad/config/move_first_live_arm_alignment_v1_20260901.json"),
        "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CONTRACT_CURRENT.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "move_first_live_arm_alignment_v1/"
            "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CURRENT.json"
        ),
        "MOVE_FIRST_LIVE_ARM_ALIGNMENT_STATUS_CURRENT.json",
    ),
    (
        Path(
            "trad/config/"
            "move_first_operational_mapping_alignment_v1_20260901.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V1_BASELINE.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "move_first_operational_mapping_alignment_v1/"
            "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V1_STATUS_BASELINE.json",
    ),
    (
        Path(
            "trad/config/"
            "move_first_operational_mapping_alignment_v2_20260901.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V2_BASELINE.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "move_first_operational_mapping_alignment_v2/"
            "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V2_STATUS_BASELINE.json",
    ),
    (
        Path(
            "trad/config/"
            "move_first_operational_mapping_alignment_v3_20260901.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V3_INVALIDATED_BASELINE.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "move_first_operational_mapping_alignment_v3/"
            "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V3_STATUS_INVALIDATED_BASELINE.json",
    ),
    (
        Path(
            "trad/config/"
            "move_first_operational_mapping_alignment_v4_20260902.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CONTRACT_CURRENT.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "move_first_operational_mapping_alignment_v4/"
            "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json"
        ),
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_STATUS_CURRENT.json",
    ),
    (
        Path("trad/config/move_first_calendar_episode_labels_v1_20260901.json"),
        "MOVE_FIRST_CALENDAR_EPISODE_LABELS_CURRENT.json",
    ),
    (
        Path("trad/config/source_conditioned_currency_rank_v6.json"),
        "SOURCE_CONDITIONED_CURRENCY_RANK_V6_BASELINE.json",
    ),
    (
        Path("trad/config/source_conditioned_currency_rank_v7.json"),
        "SOURCE_CONDITIONED_CURRENCY_RANK_CURRENT.json",
    ),
)
SECOND_FORECAST_ARTIFACTS = (
    (
        Path("trad/docs/SECOND_FORECAST_RUNBOOK.md"),
        "SECOND_FORECAST_RUNBOOK.md",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/state/"
            "second_ridge_models_v1.json"
        ),
        "second_ridge_models_v1.json",
    ),
    (
        Path(
            "trad/data/oanda_training_manager/reports/"
            "second_ridge_fit_v1.json"
        ),
        "second_ridge_fit_v1.json",
    ),
)
SIGNAL_INTERACTION_ARTIFACTS = (
    Path("trad/data/oanda_training_manager/state/signal_combination_audit_v1.json"),
    Path("trad/data/oanda_training_manager/state/signal_combination_deep_v1.json"),
    Path("trad/data/oanda_training_manager/state/signal_combination_historical_v1.json"),
    Path("trad/data/oanda_training_manager/reports/exit_policy_s5_walkforward_v1.json"),
)
MODEL_UPGRADE_ARTIFACTS = (
    Path("README.md"),
    Path("docs/MODEL_UPGRADE_RUN_20260717.md"),
    Path("docs/GPT_ACCOUNT_002_WIRING_AUDIT_20260718.md"),
    Path("docs/MODEL_GAP_ROADMAP_20260718.md"),
    Path("docs/MODEL_GAP_ALL_FAMILIES_20260719.md"),
    Path("docs/MODEL_GAP_RUNTIME_MATRIX_20260719.md"),
    Path("docs/ALL_SIGNAL_MATRIX_FINALITY_20260718.md"),
    Path("docs/CHAT_HANDOFF_20260718.md"),
    Path("docs/CHAT_HANDOFF_MODEL_GAP_20260719.md"),
    Path("docs/C_DRIVE_MIGRATION_20260718.md"),
    Path("docs/VAULT_IMPORT_AND_VALIDATION_20260719.md"),
    Path("docs/MODEL_GAP_COMPLETION_20260720.md"),
    Path("docs/MODEL_GAP_MARKET_VALIDATION_20260720.md"),
    Path("docs/CHAT_LOG_RECAP_20260720.md"),
    Path("docs/POST_GAP_EXECUTION_POLICY_20260720.md"),
    Path("docs/PAIR_FAMILY_SIGNAL_MATRIX_20260720.md"),
    Path("trad/docs/MA_FEATURE_GRID.md"),
    Path("trad/docs/CHAT_LOG_RECAP_20260727_MA_FEATURE_GRID.md"),
    Path("trad/docs/PRACTICE_006_MERGED_WIRING_20260721.md"),
    Path(
        "trad/data/oanda_training_manager/reports/"
        "practice_006_merged_wiring_v1.json"
    ),
)
MA_FEATURE_GRID_ARTIFACTS = (
    Path(
        "trad/data/oanda_training_manager/models/ma_feature_grid/"
        "ma_feature_grid_latest.joblib"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_latest.json"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_metrics_latest.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "MA_FEATURE_GRID_LATEST.md"
    ),
)
NEWS_EVENT_TAGGING_ARTIFACTS = (
    Path(
        "trad/data/oanda_training_manager/state/"
        "news_outcome_improvement_audit_v2.sqlite"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "news_outcome_improvement_audit_v2.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "news_improvement_queue_v2.json"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/"
        "news_outcome_improvement/NEWS_OUTCOME_IMPROVEMENT_CURRENT.md"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "ALL_PAIR_NEWS_EVENT_TAGGING_LATEST.md"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "events_latest.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "events_latest.json"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "latest_pair_news_context.json"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "manifest.json"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "market_movement_news_tags.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "news_event_tags.sqlite"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "pair_event_tags_latest.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "pair_event_tags_latest.json"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "run_latest.json"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_links.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_links.parquet"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_tags.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_tags.parquet"
    ),
    Path(
        "trad/data/significant_moves/event_links/"
        "move_event_links.csv"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/official_central_bank_coverage/"
        "OFFICIAL_CENTRAL_BANK_COVERAGE_CURRENT.json"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/official_central_bank_coverage/"
        "OFFICIAL_CENTRAL_BANK_COVERAGE_CURRENT.md"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/official_currency_source_depth/"
        "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json"
    ),
    Path(
        "trad/data/oanda_training_manager/reports/official_currency_source_depth/"
        "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.md"
    ),
)
NEWS_EVENT_TAGGING_SOURCE_FILES = (
    Path("README.md"),
    Path("docs/FUTURE_NOTES.md"),
    Path("trad/README.md"),
    Path("trad/docs/LOCAL_NEWS_SENTIMENT.md"),
    Path("trad/oanda_news_event_tagger.py"),
    Path("trad/oanda_local_news_sentiment.py"),
    Path("trad/oanda_news_classification_contract.py"),
    Path("trad/oanda_news_outcome_improvement_audit.py"),
    Path("trad/oanda_official_release_fast_lane.py"),
    Path("trad/oanda_official_release_fast_lane_contract.py"),
    Path("trad/oanda_official_event_quote_horizon_capture_v1.py"),
    Path("trad/oanda_official_event_quote_horizon_capture_v1_verifier.py"),
    Path("trad/oanda_official_event_pair_quote_capture_v1.py"),
    Path("trad/oanda_official_event_pair_quote_capture_v1_verifier.py"),
    Path("trad/oanda_official_event_pair_quote_capture_v2.py"),
    Path("trad/oanda_official_event_pair_quote_capture_v2_verifier.py"),
    Path("trad/oanda_official_event_pair_quote_capture_v3.py"),
    Path("trad/oanda_official_event_pair_quote_capture_v3_verifier.py"),
    Path("trad/oanda_official_event_pair_horizon_capture_v1.py"),
    Path("trad/oanda_official_event_pair_horizon_capture_v1_verifier.py"),
    Path("trad/oanda_official_event_paired_evaluator_v1.py"),
    Path("trad/oanda_official_event_paired_evaluator_v1_verifier.py"),
    Path("trad/oanda_official_release_fast_mapper.py"),
    Path("trad/oanda_official_release_fast_response_watch.py"),
    Path("trad/oanda_causal_source_factor_response_map_v1.py"),
    Path("trad/oanda_causal_source_factor_response_map_v2.py"),
    Path("trad/oanda_causal_source_factor_response_map_v3.py"),
    Path("trad/oanda_causal_source_factor_response_map_v4.py"),
    Path("trad/oanda_causal_source_factor_response_map_v5.py"),
    Path("trad/oanda_causal_source_factor_response_map_v6.py"),
    Path("trad/oanda_causal_source_factor_response_map_v7.py"),
    Path("trad/oanda_causal_source_factor_response_map_v8.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v1.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v2.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v3.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v4.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v5.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v6.py"),
    Path("trad/oanda_source_conditioned_currency_rank_v7.py"),
    Path("trad/oanda_source_governance.py"),
    Path("trad/oanda_project_integrity_audit.py"),
    Path("trad/oanda_news_source_coverage.py"),
    Path("trad/oanda_official_central_bank_coverage.py"),
    Path("trad/oanda_official_currency_source_depth.py"),
    Path("trad/oanda_news_feed_backtest.py"),
    Path("trad/oanda_signal_news_monitor.py"),
    Path(
        "trad/oanda_forex_gpt_advisor_all_pairs_v5_24_"
        "movement_ledger.py"
    ),
    Path("trad/oanda_gpt_prod_live_account_manager.py"),
    Path("trad/oanda_practice_live_dashboard.py"),
    Path("trad/oanda_main_signal_dashboard.html"),
    Path("trad/oanda_always_on_supervisor.ps1"),
    Path("trad/forex_model_vault_sync.py"),
    Path("trad/config/news_event_seed_v1.json"),
    Path("trad/config/news_sources_v1.json"),
    Path("trad/config/forex_source_gap_register_v1.json"),
    Path("trad/config/official_central_bank_source_map_v1.json"),
    Path("trad/config/official_event_quote_horizon_capture_v1.json"),
    Path("trad/config/official_event_pair_quote_capture_v1.json"),
    Path("trad/config/official_event_pair_quote_capture_v2.json"),
    Path("trad/config/official_event_pair_quote_capture_v3.json"),
    Path("trad/config/official_event_pair_horizon_capture_v1.json"),
    Path("trad/config/official_event_paired_evaluator_v1.json"),
    Path("trad/config/official_currency_source_depth_v1.json"),
    Path("trad/config/linked_currency_policy_drivers_v1.json"),
    Path("trad/config/accounts_registry.json"),
    Path("trad/config/shadow_runtime_retirements_v1.json"),
    Path("trad/config/source_conditioned_currency_rank_v1.json"),
    Path("trad/config/source_conditioned_currency_rank_v2.json"),
    Path("trad/config/source_conditioned_currency_rank_v3.json"),
    Path("trad/config/source_conditioned_currency_rank_v4.json"),
    Path("trad/config/source_conditioned_currency_rank_v5.json"),
    Path("trad/config/source_conditioned_currency_rank_v6.json"),
    Path("trad/config/source_conditioned_currency_rank_v7.json"),
    Path("trad/docs/ALL_PAIR_NEWS_EVENT_TAGGING.md"),
    Path("trad/docs/OFFICIAL_EVENT_QUOTE_HORIZON_CAPTURE_V1.md"),
    Path("trad/docs/OFFICIAL_EVENT_PAIRED_EVALUATOR_V1.md"),
    Path("trad/test_oanda_news_event_tagger.py"),
    Path("trad/test_oanda_official_central_bank_coverage.py"),
    Path("trad/test_oanda_official_currency_source_depth.py"),
    Path("trad/test_oanda_local_news_sentiment.py"),
    Path("trad/test_oanda_news_outcome_improvement_audit.py"),
    Path("trad/test_oanda_official_release_fast_lane.py"),
    Path("trad/test_oanda_official_release_fast_lane_communications.py"),
    Path("trad/test_oanda_official_event_quote_horizon_capture_v1.py"),
    Path("trad/test_oanda_official_event_quote_horizon_capture_v1_verifier.py"),
    Path("trad/test_oanda_official_event_pair_quote_capture_v1.py"),
    Path("trad/test_oanda_official_event_pair_quote_capture_v2.py"),
    Path("trad/test_oanda_official_event_pair_quote_capture_v3.py"),
    Path("trad/test_oanda_official_event_pair_horizon_capture_v1.py"),
    Path("trad/test_oanda_official_event_paired_evaluator_v1.py"),
    Path("trad/test_oanda_official_event_paired_evaluator_v1_verifier.py"),
    Path("trad/test_oanda_source_governance.py"),
    Path("trad/test_oanda_project_integrity_audit.py"),
    Path("trad/test_oanda_official_release_fast_mapper.py"),
    Path("trad/test_oanda_official_release_fast_response_watch.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v1.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v2.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v3.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v4.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v5.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v6.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v7.py"),
    Path("trad/test_oanda_causal_source_factor_response_map_v8.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v1.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v2.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v3.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v4.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v5.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v6.py"),
    Path("trad/test_oanda_source_conditioned_currency_rank_v7.py"),
    Path("trad/test_oanda_shadow_runtime_retirements.py"),
    Path("trad/test_oanda_atomic_publish_resilience.py"),
    Path("trad/test_oanda_progress_heartbeat_wiring.py"),
    Path("trad/test_oanda_news_feed_backtest.py"),
    Path("trad/test_oanda_signal_news_monitor.py"),
    Path("trad/test_gpt_v5_24_account_wiring.py"),
    Path("trad/test_gpt_live_news_watch.py"),
    Path("trad/test_forex_model_vault_sync.py"),
)
VALIDATION_RECREATION_FILES = (
    Path(
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "panel_m1_compact_canary_20260718.parquet"
    ),
    Path(
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "panel_m1_compact_canary_20260718.parquet.manifest.json"
    ),
    Path(
        "trad/data/oanda_training_manager/model_space/"
        "model_gap_shared_panel_model_benchmark_expected.json"
    ),
    Path(
        "trad/data/oanda_training_manager/model_space/"
        "model_gap_shared_panel_neural_benchmark_expected.json"
    ),
    Path(
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "foundation_m1_holdout_v1.npz"
    ),
    Path(
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "foundation_m1_holdout_v1.npz.manifest.json"
    ),
)
RUNTIME_REQUIREMENTS_DIRS = (
    Path("trad/config/runtime_requirements"),
    Path("trad/config/runtime_requirements_d"),
)
VAULT_BOOTSTRAP_FILES = (
    (Path("README.md"), "README.md"),
    (Path("trad/forex_vault_import.py"), "forex_vault_import.py"),
    (Path("trad/forex_vault_bootstrap.ps1"), "forex_vault_bootstrap.ps1"),
    (
        Path("docs/VAULT_IMPORT_AND_VALIDATION_20260719.md"),
        "VAULT_IMPORT_AND_VALIDATION_20260719.md",
    ),
)
CURRENT_STATE_ARTIFACTS = (
    Path("trad/data/oanda_training_manager/state/account_dashboard_v1.json"),
    Path("trad/data/oanda_training_manager/state/account_007_dashboard_v1.json"),
    Path("trad/data/oanda_training_manager/state/practice_007_last_signal_v1.json"),
    Path("trad/data/oanda_training_manager/state/practice_007_signal_snapshot_v1.json"),
    Path("trad/data/oanda_training_manager/state/news_technical_watchlist_v1.json"),
    Path(
        "trad/data/oanda_training_manager/state/"
        "practice_006_news_challenger_heartbeat_v1.json"
    ),
    Path("trad/data/oanda_training_manager/state/project_integrity_audit_v1.json"),
    Path("trad/data/oanda_training_manager/state/lane_promotion_v1.json"),
    Path("trad/data/oanda_training_manager/state/strategy_exit_fit_v1.json"),
    Path(
        "trad/data/oanda_training_manager/state/"
        "pair_family_timeframe_horizon_v1.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "practice_007_execution_policy_v1.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "practice_007_execution_policy_challenger_v1.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "timeframe_matrix_calibration_v1.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "practice_006_merged_relay_heartbeat_v1.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "sma_signal_filter_v1.json"
    ),
    Path(
        "trad/data/oanda_training_manager/state/"
        "sma_signal_filter_v1.joblib"
    ),
    Path(
        "trad/data/oanda_training_manager/model_space/"
        "modern_model_dependency_status_latest.json"
    ),
)
INTEGRITY_SNAPSHOT_ARTIFACTS = (
    Path("trad/data/oanda_training_manager/state/project_integrity_audit_v1.json"),
)
MODEL_LIFECYCLE_FILES = (
    Path("trad/data/oanda_training_manager/model_lifecycle/index.json"),
    Path("trad/data/oanda_training_manager/model_lifecycle/research_queue.jsonl"),
)
MODEL_LIFECYCLE_DIRS = (
    Path("trad/data/oanda_training_manager/model_lifecycle/candidates"),
    Path("trad/data/oanda_training_manager/promotions"),
    Path("trad/data/oanda_training_manager/model_space"),
)
MODERN_MODEL_GAP_DIRS = (
    Path("trad/data/oanda_training_manager/reports/modern_model_gap"),
    Path("trad/data/oanda_training_manager/models/modern_model_gap"),
)
MOVING_AVERAGE_CROSSOVER_REPORT_ROOT = Path(
    "trad/data/oanda_training_manager/reports/moving_average_crossover_sweep"
)
MOVING_AVERAGE_CROSSOVER_REPORT_FILES = {
    "manifest.json",
    "SUMMARY.md",
    "ASSESSMENT.md",
    "aggregate_results.parquet",
    "top_validation_selected_candidates.csv",
    "pair_validation_selected_candidates.csv",
    "data_inventory.csv",
}
MOVING_AVERAGE_ENTRY_QUALITY_REPORT_ROOT = Path(
    "trad/data/oanda_training_manager/reports/"
    "moving_average_crossover_entry_quality"
)
MOVING_AVERAGE_ENTRY_QUALITY_REPORT_FILES = {
    "manifest.json",
    "SUMMARY.md",
    "ASSESSMENT.md",
    "entry_quality_aggregate.parquet",
    "entry_quality_candidates.csv",
    "data_inventory.csv",
}
MULTITIMEFRAME_CROSSOVER_REPORT_ROOT = Path(
    "trad/data/oanda_training_manager/reports/"
    "multitimeframe_crossover_stack"
)
MULTITIMEFRAME_CROSSOVER_REPORT_FILES = {
    "manifest.json",
    "SUMMARY.md",
    "ASSESSMENT.md",
    "stack_entry_quality_aggregate.parquet",
    "stack_candidates.csv",
    "data_inventory.csv",
}
SMA_SIGNAL_FILTER_REPORT_ROOT = Path(
    "trad/data/oanda_training_manager/reports/sma_signal_filter"
)
SMA_SIGNAL_FILTER_REPORT_FILES = {
    "manifest.json",
    "assessment.md",
    "model_comparison.csv",
}
FULL_MATRIX_AUDIT_DIR = Path(
    "trad/data/oanda_training_manager/training_sets/model_gap_full_matrix"
)
SPIKE_BLURB_REPORT_DIR = Path(
    "trad/data/oanda_training_manager/reports/spike_blurb_factor_reconstruction"
)
CLEAN_IMPORT_AUDIT_GLOB = "vault_clean_import_*.json"


def open_binary_read(
    path: Path,
    *,
    attempts: int = 40,
    retry_delay_sec: float = 0.05,
):
    last_error: OSError | None = None
    for attempt in range(max(1, attempts)):
        try:
            return path.open("rb")
        except (FileNotFoundError, PermissionError) as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(max(0.0, retry_delay_sec))
    assert last_error is not None
    raise last_error


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open_binary_read(path) as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json_optional(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def iter_tree(root: Path):
    if not root.exists():
        return
    for current, dirs, files in os.walk(root):
        blocked = PRUNED_DIRS | {"data", "reports"}
        dirs[:] = sorted(d for d in dirs if d.lower() not in blocked)
        current_path = Path(current)
        for name in sorted(files):
            path = current_path / name
            lower_name = name.lower()
            if lower_name in SECRET_NAMES or lower_name.endswith(".key"):
                continue
            if path.suffix.lower() not in SOURCE_EXTENSIONS:
                continue
            yield path


def _capture_integrity_exports(root: Path, files: list[Path]) -> dict[str, bytes]:
    """Pin the exact included integrity snapshots and their verified closure.

    Only this explicit snapshot allowlist can add dependencies. In particular,
    this never walks the detail directory or follows references in other JSON.
    Relative archive paths preserve the original snapshot-directory base.
    """
    included = {path.relative_to(root).as_posix() for path in files}
    captured: dict[str, bytes] = {}
    resolved_root = root.resolve()
    for relative in INTEGRITY_SNAPSHOT_ARTIFACTS:
        name = relative.as_posix()
        if name not in included:
            continue
        path = root / relative
        if not path.resolve().is_relative_to(resolved_root):
            raise ValueError("integrity snapshot escapes checkpoint root")
        encoded = path.read_bytes()
        payload = json.loads(encoded)
        if not isinstance(payload, dict):
            raise ValueError("integrity checkpoint snapshot is not an object")
        artifacts = verified_detail_artifact_bytes(payload, snapshot_dir=path.parent)
        captured[name] = encoded
        for artifact, content in artifacts.items():
            captured[artifact.relative_to(resolved_root).as_posix()] = content
    return captured


def collect_files(root: Path) -> list[Path]:
    trad = root / "trad"
    selected: dict[str, Path] = {}

    # Runtime source and configuration. Raw market data and report payloads are
    # handled separately so they cannot inflate the vault checkpoint.
    for path in iter_tree(trad):
        rel = path.relative_to(root).as_posix()
        parts = {part.lower() for part in path.relative_to(trad).parts}
        if "data" in parts or "reports" in parts:
            continue
        selected[rel] = path
    for path in iter_tree(root / "docs"):
        selected[path.relative_to(root).as_posix()] = path

    # Fitted artifacts used by the currently selected HGB account lane. Future
    # model-cache directories under the same manager tree are picked up without
    # needing another code change.
    model_root = trad / "data" / "technical_scout_manager"
    if model_root.exists():
        for current, dirs, files in os.walk(model_root):
            current_path = Path(current)
            if current_path.name.lower() != "model_cache":
                if current_path == model_root:
                    dirs[:] = sorted(d for d in dirs if d.lower() not in PRUNED_DIRS)
                else:
                    dirs[:] = sorted(d for d in dirs if d.lower() == "model_cache")
                continue
            dirs[:] = []
            for name in sorted(files):
                path = current_path / name
                if path.suffix.lower() in MODEL_EXTENSIONS:
                    selected[path.relative_to(root).as_posix()] = path

    # Compact, credential-free S1 artifacts are required to reproduce the live
    # forecast layer. Raw ledgers and market-data partitions remain excluded.
    for relative_path, _ in SECOND_FORECAST_ARTIFACTS:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for relative_path in SIGNAL_INTERACTION_ARTIFACTS:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for relative_path in MODEL_UPGRADE_ARTIFACTS:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for relative_path in MA_FEATURE_GRID_ARTIFACTS:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for relative_path in NEWS_EVENT_TAGGING_ARTIFACTS:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    ma_model_root = (
        trad / "data" / "oanda_training_manager" / "models"
    )
    if ma_model_root.is_dir():
        for directory in ma_model_root.glob("ma_feature_grid*"):
            if not directory.is_dir():
                continue
            for path in directory.glob("*_latest.joblib"):
                if path.is_file():
                    selected[path.relative_to(root).as_posix()] = path
    ma_report_root = (
        trad / "data" / "oanda_training_manager" / "reports"
    )
    if ma_report_root.is_dir():
        for directory in ma_report_root.glob("ma_feature_grid*"):
            if not directory.is_dir():
                continue
            for path in directory.iterdir():
                if (
                    path.is_file()
                    and "latest" in path.name.lower()
                    and path.suffix.lower() in {".json", ".csv", ".md"}
                ):
                    selected[path.relative_to(root).as_posix()] = path
    for relative_path in VALIDATION_RECREATION_FILES:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for relative_dir in RUNTIME_REQUIREMENTS_DIRS:
        requirements_dir = root / relative_dir
        if requirements_dir.is_dir():
            for path in requirements_dir.glob("requirements-*.txt"):
                if path.is_file():
                    selected[path.relative_to(root).as_posix()] = path
    for relative_path in CURRENT_STATE_ARTIFACTS + MODEL_LIFECYCLE_FILES:
        path = root / relative_path
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for relative_dir in MODEL_LIFECYCLE_DIRS:
        directory = root / relative_dir
        if not directory.is_dir():
            continue
        for path in directory.rglob("*"):
            if path.name.startswith("vault_clean_import_"):
                continue
            if path.is_file() and path.suffix.lower() in {".json", ".jsonl"}:
                selected[path.relative_to(root).as_posix()] = path
    for relative_dir in MODERN_MODEL_GAP_DIRS:
        directory = root / relative_dir
        if not directory.is_dir():
            continue
        for path in directory.rglob("*"):
            if path.is_file() and path.suffix.lower() in ({".json"} | MODEL_EXTENSIONS):
                selected[path.relative_to(root).as_posix()] = path
    # Compact spike/blurb summaries are the reproducible evidence index for the
    # movement-first reconstruction. The 292 MiB SQLite/raw-price payload stays
    # excluded; only bounded JSON/Markdown/CSV reports enter the checkpoint.
    spike_blurb_report_dir = root / SPIKE_BLURB_REPORT_DIR
    if spike_blurb_report_dir.is_dir():
        for path in spike_blurb_report_dir.rglob("*"):
            if (
                path.is_file()
                and path.suffix.lower() in {".json", ".md", ".csv"}
                and path.stat().st_size <= 5 * 1024 * 1024
            ):
                selected[path.relative_to(root).as_posix()] = path
    matrix_root = root / FULL_MATRIX_AUDIT_DIR
    if matrix_root.is_dir():
        for path in matrix_root.rglob("*"):
            if not path.is_file():
                continue
            include = (
                path.name.endswith(".manifest.json")
                or path.name in {
                    "full_matrix_latest.json",
                    "full_matrix_state.json",
                    "coverage_cells.csv",
                }
            )
            if include:
                selected[path.relative_to(root).as_posix()] = path
    upgrade_report_root = (
        root / "trad" / "data" / "oanda_training_manager" / "reports"
    )
    for path in upgrade_report_root.glob("model_upgrade_20260717_*.json"):
        if path.is_file():
            selected[path.relative_to(root).as_posix()] = path
    for directory in upgrade_report_root.glob("model_upgrade_all68_*"):
        if not directory.is_dir():
            continue
        for path in directory.rglob("*.json"):
            if path.is_file():
                selected[path.relative_to(root).as_posix()] = path

    crossover_root = root / MOVING_AVERAGE_CROSSOVER_REPORT_ROOT
    if crossover_root.is_dir():
        completed_runs = [
            path
            for path in crossover_root.glob("all68_*")
            if path.is_dir() and (path / "manifest.json").is_file()
        ]
        if completed_runs:
            run_groups = (
                [path for path in completed_runs if "exact" not in path.name.lower()],
                [path for path in completed_runs if "exact" in path.name.lower()],
            )
            latest_runs = {
                max(
                    group,
                    key=lambda path: (path / "manifest.json").stat().st_mtime_ns,
                )
                for group in run_groups
                if group
            }
            for latest_run in latest_runs:
                for name in MOVING_AVERAGE_CROSSOVER_REPORT_FILES:
                    path = latest_run / name
                    if path.is_file():
                        selected[path.relative_to(root).as_posix()] = path

    entry_quality_root = root / MOVING_AVERAGE_ENTRY_QUALITY_REPORT_ROOT
    if entry_quality_root.is_dir():
        completed_runs = [
            path
            for path in entry_quality_root.glob("all68_*")
            if path.is_dir() and (path / "manifest.json").is_file()
        ]
        if completed_runs:
            run_groups = (
                [path for path in completed_runs if "exact" not in path.name.lower()],
                [path for path in completed_runs if "exact" in path.name.lower()],
            )
            latest_runs = {
                max(
                    group,
                    key=lambda path: (path / "manifest.json").stat().st_mtime_ns,
                )
                for group in run_groups
                if group
            }
            for latest_run in latest_runs:
                for name in MOVING_AVERAGE_ENTRY_QUALITY_REPORT_FILES:
                    path = latest_run / name
                    if path.is_file():
                        selected[path.relative_to(root).as_posix()] = path

    multitimeframe_root = root / MULTITIMEFRAME_CROSSOVER_REPORT_ROOT
    if multitimeframe_root.is_dir():
        completed_runs = [
            path
            for path in multitimeframe_root.glob("all68_*")
            if path.is_dir() and (path / "manifest.json").is_file()
        ]
        if completed_runs:
            latest_run = max(
                completed_runs,
                key=lambda path: (path / "manifest.json").stat().st_mtime_ns,
            )
            for name in MULTITIMEFRAME_CROSSOVER_REPORT_FILES:
                path = latest_run / name
                if path.is_file():
                    selected[path.relative_to(root).as_posix()] = path

    sma_filter_root = root / SMA_SIGNAL_FILTER_REPORT_ROOT
    if sma_filter_root.is_dir():
        completed_runs = [
            path
            for path in sma_filter_root.glob("all68_*")
            if path.is_dir() and (path / "manifest.json").is_file()
        ]
        if completed_runs:
            latest_run = max(
                completed_runs,
                key=lambda path: (path / "manifest.json").stat().st_mtime_ns,
            )
            for name in SMA_SIGNAL_FILTER_REPORT_FILES:
                path = latest_run / name
                if path.is_file():
                    selected[path.relative_to(root).as_posix()] = path

    # Include only the verified transitive closure of explicitly selected
    # integrity snapshots. Missing/corrupt details abort checkpoint collection.
    for relative in _capture_integrity_exports(root, list(selected.values())):
        selected[relative] = root / relative
    return [selected[key] for key in sorted(selected)]


def collect_news_event_checkpoint_files(root: Path) -> list[Path]:
    selected = [
        root / relative_path
        for relative_path in (
            *NEWS_EVENT_TAGGING_SOURCE_FILES,
            *NEWS_EVENT_TAGGING_ARTIFACTS,
        )
        if (root / relative_path).is_file()
    ]
    return sorted(
        set(selected),
        key=lambda path: path.relative_to(root).as_posix(),
    )


def build_manifest_from_rows(
    root: Path,
    rows: list[dict],
    *,
    created_utc: str | None = None,
) -> dict:
    normalized_rows = sorted(rows, key=lambda row: str(row["path"]))
    content_hash = hashlib.sha256(
        json.dumps(
            normalized_rows,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "schema_version": 1,
        "created_utc": created_utc or datetime.now(timezone.utc).isoformat(),
        "source_root": str(root),
        "credential_free": True,
        "raw_market_data_included": False,
        "file_count": len(normalized_rows),
        "total_bytes": sum(int(row["size"]) for row in normalized_rows),
        "content_sha256": content_hash,
        "files": normalized_rows,
    }


def build_manifest(root: Path, files: list[Path]) -> dict:
    rows = []
    for path in files:
        stat = path.stat()
        rows.append({
            "path": path.relative_to(root).as_posix(),
            "size": stat.st_size,
            "sha256": sha256_file(path),
        })
    return build_manifest_from_rows(root, rows)


def write_zip(
    root: Path,
    files: list[Path],
    manifest: dict,
    destination: Path,
) -> dict:
    # A current snapshot can change after collect_files(). Capture its current
    # bytes and exact detail closure before creating/replacing any checkpoint;
    # use these same verified bytes throughout ZIP creation.
    captured = _capture_integrity_exports(root, files)
    selected = {path.relative_to(root).as_posix(): path for path in files}
    for relative in INTEGRITY_SNAPSHOT_ARTIFACTS:
        if relative.as_posix() not in captured:
            continue
        prefix = (relative.parent / INTEGRITY_DETAIL_DIRECTORY).as_posix() + "/"
        selected = {
            name: path for name, path in selected.items()
            if not name.startswith(prefix) or name in captured
        }
    selected.update({name: root / name for name in captured})
    files = [selected[name] for name in sorted(selected)]
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix="forex_model_checkpoint_", suffix=".tmp", dir=destination.parent)
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        rows = []
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for path in files:
                relative = path.relative_to(root).as_posix()
                digest = hashlib.sha256()
                size = 0
                member = zipfile.ZipInfo(
                    relative,
                    date_time=(1980, 1, 1, 0, 0, 0),
                )
                member.compress_type = zipfile.ZIP_DEFLATED
                source_stream = (
                    io.BytesIO(captured[relative]) if relative in captured
                    else open_binary_read(path)
                )
                with source_stream as source, archive.open(
                    member,
                    "w",
                    force_zip64=True,
                ) as target:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                        size += len(chunk)
                        target.write(chunk)
                rows.append(
                    {
                        "path": relative,
                        "size": size,
                        "sha256": digest.hexdigest(),
                    }
                )
            snapshot_manifest = build_manifest_from_rows(
                root,
                rows,
                created_utc=manifest.get("created_utc"),
            )
            manifest_entry = zipfile.ZipInfo(
                "MODEL_CHECKPOINT_MANIFEST.json",
                date_time=(1980, 1, 1, 0, 0, 0),
            )
            manifest_entry.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(
                manifest_entry,
                json.dumps(snapshot_manifest, indent=2),
            )
        temp_path.replace(destination)
        return snapshot_manifest
    finally:
        temp_path.unlink(missing_ok=True)


def copy_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f"{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        shutil.copy2(source, temp_path)
        os.replace(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)


def write_bytes_atomic(destination: Path, payload: bytes) -> None:
    """Publish one already-captured byte snapshot without a second source read."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f"{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)


def validate_checkpoint_archive(
    archive_path: Path,
    expected_manifest: dict,
    *,
    reconstruct: bool,
) -> dict:
    """Fail closed unless an archive is hash-, manifest-, CRC-, and import-safe."""
    archive_path = archive_path.resolve()
    expected_sha256 = sha256_file(archive_path)
    try:
        audit = audit_archive(archive_path, expected_sha256)
        if audit.manifest != expected_manifest:
            raise VaultImportError(
                f"embedded manifest does not match current snapshot: {archive_path}"
            )
        reconstruction_report = None
        if reconstruct:
            with tempfile.TemporaryDirectory(
                prefix="forex_model_checkpoint_reconstruct_"
            ) as temporary:
                reconstruction_report = import_archives(
                    [archive_path],
                    Path(temporary) / "restored",
                    expected_hashes=[expected_sha256],
                    verify_only=False,
                )
                if reconstruction_report.get("status") != "imported_and_verified":
                    raise VaultImportError(
                        "checkpoint reconstruction did not return passed status"
                    )
    except (OSError, zipfile.BadZipFile, VaultImportError) as exc:
        raise RuntimeError(
            f"checkpoint validation failed before retention: {archive_path}"
        ) from exc
    return {
        "schema_version": 1,
        "status": "passed",
        "verified_utc": datetime.now(timezone.utc).isoformat(),
        "archive": archive_path.name,
        "archive_sha256": audit.archive_sha256,
        "archive_bytes": archive_path.stat().st_size,
        "manifest_name": audit.manifest_name,
        "manifest_sha256": hashlib.sha256(
            json.dumps(
                audit.manifest,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "content_sha256": audit.manifest.get("content_sha256"),
        "payload_files_verified": len(audit.rows),
        "represented_bytes": audit.manifest.get("total_bytes"),
        "zip_crc_verified": True,
        "safe_member_paths_verified": True,
        "credential_like_files": 0,
        "reconstructability_verified": bool(reconstruct),
        "reconstruction_verified_utc": (
            datetime.now(timezone.utc).isoformat() if reconstruct else None
        ),
        "reconstructed_files_verified": (
            reconstruction_report.get("merged_files")
            if reconstruction_report is not None
            else None
        ),
        "account_processes_started": 0,
    }


def list_timestamped_model_archives(vault_project: Path) -> list[Path]:
    """Return only exact, direct-child, non-symlink model checkpoint archives."""
    rows = []
    for path in vault_project.iterdir():
        if (
            path.parent == vault_project
            and path.is_file()
            and not path.is_symlink()
            and MODEL_ARCHIVE_RE.fullmatch(path.name)
        ):
            rows.append(path)
    return sorted(rows, key=lambda path: path.name, reverse=True)


def apply_checkpoint_retention(
    vault_project: Path,
    *,
    retention: int,
    validation_receipt_path: Path,
    validation_receipt: dict,
    protected_archive: Path,
    stamp: str,
) -> dict:
    """Record exact deletion targets before deleting direct-child archives."""
    if isinstance(retention, bool) or not isinstance(retention, int) or retention < 1:
        raise ValueError("retention must be an integer of at least one")
    if validation_receipt.get("status") != "passed":
        raise RuntimeError("current checkpoint has no passing validation receipt")
    receipt_archive_sha256 = str(
        validation_receipt.get("archive_sha256") or ""
    ).lower()
    if not receipt_archive_sha256:
        raise RuntimeError("current checkpoint validation receipt is not hash-bound")
    observed_receipt = load_json_optional(validation_receipt_path)
    if observed_receipt != validation_receipt:
        raise RuntimeError("current checkpoint validation receipt is not durable")

    protected_archive = protected_archive.resolve()
    if (
        protected_archive.parent != vault_project.resolve()
        or not protected_archive.is_file()
        or protected_archive.is_symlink()
        or MODEL_ARCHIVE_RE.fullmatch(protected_archive.name) is None
        or sha256_file(protected_archive) != receipt_archive_sha256
    ):
        raise RuntimeError("new timestamped checkpoint is not safely protected")
    archives = list_timestamped_model_archives(vault_project)
    archives = [
        protected_archive,
        *(path for path in archives if path.resolve() != protected_archive),
    ]
    kept = archives[:retention]
    targets = archives[retention:]
    base = {
        "schema_version": 1,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "destination": str(vault_project.resolve()),
        "retention": retention,
        "current_validation_receipt": {
            "path": validation_receipt_path.name,
            "sha256": sha256_file(validation_receipt_path),
            "archive_sha256": receipt_archive_sha256,
        },
        "kept": [
            {
                "name": path.name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in kept
        ],
        "deletion_targets": [
            {
                "name": path.name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in targets
        ],
        "deleted": [],
    }
    if not targets:
        return {**base, "status": "not_required", "tombstone": None}

    tombstone_path = vault_project / (
        f"{MODEL_RETENTION_RECEIPT_PREFIX}{stamp}_{time.time_ns()}_{os.getpid()}.json"
    )
    receipt = {**base, "status": "planned", "tombstone": tombstone_path.name}
    write_json_atomic(tombstone_path, receipt)
    deleted = []
    for path, target in zip(targets, base["deletion_targets"]):
        if (
            path.parent != vault_project
            or path.is_symlink()
            or not path.is_file()
            or MODEL_ARCHIVE_RE.fullmatch(path.name) is None
            or path.stat().st_size != target["bytes"]
            or sha256_file(path) != target["sha256"]
        ):
            raise RuntimeError(
                f"retention target changed after tombstone creation: {path}"
            )
        path.unlink()
        deleted.append(target)
        receipt = {**receipt, "status": "deleting", "deleted": list(deleted)}
        write_json_atomic(tombstone_path, receipt)
    receipt = {
        **receipt,
        "status": "deleted",
        "completed_utc": datetime.now(timezone.utc).isoformat(),
        "deleted": deleted,
    }
    write_json_atomic(tombstone_path, receipt)
    return {
        **receipt,
        "tombstone_sha256": sha256_file(tombstone_path),
    }


def sync_second_forecast_artifacts(root: Path, vault_project: Path) -> dict:
    destination = vault_project / "artifacts" / "second_forecast"
    destination.mkdir(parents=True, exist_ok=True)
    rows = []
    changed = False
    for relative_path, destination_name in SECOND_FORECAST_ARTIFACTS:
        source = root / relative_path
        if not source.is_file():
            continue
        digest = sha256_file(source)
        target = destination / destination_name
        target_matches = (
            target.is_file()
            and target.stat().st_size == source.stat().st_size
            and sha256_file(target) == digest
        )
        if not target_matches:
            temporary = target.with_suffix(f"{target.suffix}.{os.getpid()}.tmp")
            shutil.copy2(source, temporary)
            os.replace(temporary, target)
            changed = True
        rows.append(
            {
                "name": destination_name,
                "source": relative_path.as_posix(),
                "size": source.stat().st_size,
                "sha256": digest,
            }
        )

    fit_report_path = (
        root
        / "trad"
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "second_ridge_fit_v1.json"
    )
    fit_report = {}
    if fit_report_path.is_file():
        try:
            fit_report = json.loads(fit_report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            fit_report = {}
    content_hash = hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    manifest_path = destination / "artifact_manifest.json"
    previous_manifest = {}
    if manifest_path.is_file():
        try:
            previous_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            previous_manifest = {}
    created_utc = (
        previous_manifest.get("created_utc")
        if previous_manifest.get("content_sha256") == content_hash
        else datetime.now(timezone.utc).isoformat()
    )
    manifest = {
        "schema_version": 2,
        "created_utc": created_utc,
        "source_root": str(root),
        "purpose": (
            "Compact one-second forecast model, fit evidence, and operator "
            "runbook"
        ),
        "credential_free": True,
        "raw_market_data_included": False,
        "content_sha256": content_hash,
        "pairs_requested": fit_report.get("pairs_requested"),
        "pairs_fitted": fit_report.get("pairs_fitted"),
        "complete_horizon_pair_count": fit_report.get(
            "complete_horizon_pair_count"
        ),
        "model_count": fit_report.get("model_count"),
        "pair_specific_model_count": fit_report.get(
            "pair_specific_model_count"
        ),
        "proxy_model_count": fit_report.get("proxy_model_count"),
        "horizons_sec": fit_report.get("horizons_sec") or [],
        "historical_gate_passes": fit_report.get("historical_gate_passes"),
        "files": rows,
    }
    if manifest != previous_manifest:
        temporary_manifest = manifest_path.with_suffix(
            f".json.{os.getpid()}.tmp"
        )
        temporary_manifest.write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary_manifest, manifest_path)
        changed = True
    return {
        "destination": str(destination),
        "changed": changed,
        "content_sha256": content_hash,
        "file_count": len(rows),
    }


def sync_bootstrap_files(root: Path, vault_project: Path) -> dict:
    rows = []
    changed = False
    for relative_path, destination_name in VAULT_BOOTSTRAP_FILES:
        source = root / relative_path
        if not source.is_file():
            continue
        digest = sha256_file(source)
        target = vault_project / destination_name
        target_matches = (
            target.is_file()
            and target.stat().st_size == source.stat().st_size
            and sha256_file(target) == digest
        )
        if not target_matches:
            temporary = target.with_suffix(f"{target.suffix}.{os.getpid()}.tmp")
            shutil.copy2(source, temporary)
            os.replace(temporary, target)
            changed = True
        rows.append(
            {
                "name": destination_name,
                "source": relative_path.as_posix(),
                "size": source.stat().st_size,
                "sha256": digest,
            }
        )
    return {"changed": changed, "files": rows}


def sync_canonical_project_records(root: Path, vault_project: Path) -> dict:
    """Preflight every required record before publishing any current bytes."""
    try:
        from tools import vault_worktree_snapshot as source_safety
    except ModuleNotFoundError:
        from trad.tools import vault_worktree_snapshot as source_safety

    root = Path(root).absolute()
    vault_project = Path(vault_project).absolute()
    # Reject name-surrogate redirects, but permit OneDrive hydration tags.
    for directory in (root, vault_project):
        for ancestor in (directory, *directory.parents):
            if ancestor.is_symlink() or getattr(ancestor.lstat(), "st_reparse_tag", 0) & 0x20000000:
                raise RuntimeError("canonical record preflight rejected a redirected root")
        if not directory.is_dir():
            raise RuntimeError("canonical record preflight requires existing directories")
    if not CANONICAL_PROJECT_RECORDS:
        raise RuntimeError("canonical record preflight has no configured records")
    private_values = source_safety.known_private_values([root / "trad" / "creds"])
    prepared = []
    destinations_seen = set()
    for relative_path, destination_name in CANONICAL_PROJECT_RECORDS:
        relative_name = relative_path.as_posix()
        source_safety.safe_name(relative_name)
        source_safety.safe_name(destination_name)
        if "/" in destination_name or destination_name.casefold() in destinations_seen:
            raise RuntimeError("canonical record preflight rejected duplicate/nonlocal destination")
        destinations_seen.add(destination_name.casefold())
        try:
            source = source_safety.regular_file(root, relative_name)
            snapshot = source.read_bytes()
        except (OSError, RuntimeError) as exc:
            raise RuntimeError(
                f"canonical record preflight missing/invalid required source: {relative_name}"
            ) from exc
        source_safety.audit_payload(relative_name.removeprefix("trad/"), snapshot, private_values)
        digest = hashlib.sha256(snapshot).hexdigest()
        snapshot_size = len(snapshot)
        target = vault_project / destination_name
        if target.exists() or target.is_symlink():
            source_safety.regular_file(vault_project, destination_name)
        target_matches = (
            target.is_file()
            and target.stat().st_size == snapshot_size
            and sha256_file(target) == digest
        )
        prepared.append((target, snapshot, target_matches, {
                "name": destination_name,
                "source": relative_name,
                "size": snapshot_size,
                "sha256": digest,
            }))
    manifest_path = vault_project / "SHARED_PROJECT_STATE_CURRENT.json"
    if manifest_path.exists() or manifest_path.is_symlink():
        source_safety.regular_file(vault_project, manifest_path.name)
    # All configured source bytes and output paths are now approved. A failed
    # preflight above leaves both old records and the old manifest untouched.
    rows = []
    changed = False
    for target, snapshot, target_matches, row in prepared:
        if not target_matches:
            write_bytes_atomic(target, snapshot)
            changed = True
        if sha256_file(target) != row["sha256"]:
            raise RuntimeError("canonical record persisted-byte verification failed")
        rows.append(row)
    manifest = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "records": rows,
        "record_count": len(rows),
        "contains_credentials": False,
        "credential_audit": {
            "passed": True,
            "records_scanned": len(rows),
            "known_private_values_checked": bool(private_values),
            "shared_pattern_rules": sorted(source_safety.credentials.PATTERNS),
        },
    }
    previous = load_json_optional(manifest_path)
    comparable_previous = dict(previous)
    comparable_previous.pop("generated_utc", None)
    comparable_manifest = dict(manifest)
    comparable_manifest.pop("generated_utc", None)
    if comparable_previous != comparable_manifest:
        write_json_atomic(manifest_path, manifest)
        changed = True
    return {
        "changed": changed,
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "records": rows,
    }


def sync_clean_import_artifacts(root: Path, vault_project: Path) -> dict:
    source = (
        root / "trad" / "data" / "oanda_training_manager" / "model_space"
    )
    destination = vault_project / "artifacts" / "clean_import"
    destination.mkdir(parents=True, exist_ok=True)
    rows = []
    changed = False
    for path in sorted(source.glob(CLEAN_IMPORT_AUDIT_GLOB)):
        if not path.is_file():
            continue
        digest = sha256_file(path)
        target = destination / path.name
        target_matches = (
            target.is_file()
            and target.stat().st_size == path.stat().st_size
            and sha256_file(target) == digest
        )
        if not target_matches:
            temporary = target.with_suffix(f"{target.suffix}.{os.getpid()}.tmp")
            shutil.copy2(path, temporary)
            os.replace(temporary, target)
            changed = True
        rows.append(
            {
                "name": path.name,
                "source": path.relative_to(root).as_posix(),
                "size": path.stat().st_size,
                "sha256": digest,
            }
        )
    content_sha256 = hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    artifact_manifest = {
        "schema_version": 1,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Exact-checkpoint clean import and recreation evidence",
        "checkpoint_payload_member": False,
        "content_sha256": content_sha256,
        "file_count": len(rows),
        "files": rows,
    }
    manifest_path = destination / "artifact_manifest.json"
    previous_manifest = load_json_optional(manifest_path)
    comparable_previous = dict(previous_manifest)
    comparable_previous.pop("created_utc", None)
    comparable_current = dict(artifact_manifest)
    comparable_current.pop("created_utc", None)
    if comparable_previous != comparable_current:
        write_json_atomic(manifest_path, artifact_manifest)
        changed = True
    elif previous_manifest:
        artifact_manifest = previous_manifest
    return {
        "destination": str(destination),
        "changed": changed,
        "content_sha256": artifact_manifest["content_sha256"],
        "file_count": len(rows),
    }


def build_checkpoint_pointer(
    root: Path,
    manifest: dict,
    current_zip: Path,
    timestamped_archive: Path | None,
    bootstrap_result: dict,
    current_validation_receipt: dict,
    retention_result: dict,
) -> dict:
    model_space = (
        root / "trad" / "data" / "oanda_training_manager" / "model_space"
    )
    completion = load_json_optional(
        model_space / "model_gap_completion_latest.json"
    )
    validation_path = model_space / "vault_validation_recreation_summary_latest.json"
    validation = load_json_optional(validation_path)
    clean_import_path = model_space / "vault_clean_import_validation_latest.json"
    clean_import = load_json_optional(clean_import_path)
    runtime = load_json_optional(
        model_space / "model_gap_runtime_profiles_d_latest.json"
    )
    matrix = load_json_optional(
        root
        / "trad"
        / "data"
        / "oanda_training_manager"
        / "training_sets"
        / "model_gap_full_matrix"
        / "full_matrix_latest.json"
    )
    ma_grid_path = (
        root
        / "trad"
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "ma_feature_grid"
        / "ma_feature_grid_latest.json"
    )
    ma_grid = load_json_optional(ma_grid_path)
    ma_grid_contract = ma_grid.get("contract") or {}
    ma_planned_cells = ma_grid.get("planned_cell_count")
    ma_fitted_cells = ma_grid.get("fitted_cell_count")
    ma_coverage_ratio = None
    if ma_planned_cells:
        ma_coverage_ratio = round(
            float(ma_fitted_cells or 0) / float(ma_planned_cells),
            6,
        )

    compile_result = validation.get("compile") or {}
    pytest_result = validation.get("pytest") or {}
    full_reruns = validation.get("full_reruns") or []
    evidence = validation.get("evidence") or []
    completion_summary = completion.get("summary") or {}
    runtime_summary = runtime.get("summary") or {}
    matrix_coverage = (
        completion.get("matrix_coverage") or matrix.get("coverage") or {}
    )

    bootstrap_aliases = {
        "README.md": "readme",
        "forex_model_vault_import.py": "importer",
        "forex_vault_bootstrap.ps1": "script",
        "VAULT_IMPORT_AND_VALIDATION_20260719.md": "runbook",
    }
    standalone_bootstrap = {}
    for row in bootstrap_result.get("files") or []:
        name = str(row.get("name") or "")
        key = bootstrap_aliases.get(name, Path(name).stem.lower())
        standalone_bootstrap[key] = {
            "path": name,
            "bytes": row.get("size"),
            "sha256": row.get("sha256"),
        }

    disk_guard_record = None
    disk_guard_root = (
        root
        / "trad"
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "disk_guard"
    )
    if disk_guard_root.is_dir():
        guard_files = sorted(
            disk_guard_root.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if guard_files:
            guard = load_json_optional(guard_files[0])
            disk_guard_record = guard.get("latest_event7_record")

    source_validation = {
        "status": validation.get("status"),
        "mode": validation.get("mode"),
        "generated_utc": validation.get("generated_utc"),
        "report": validation_path.relative_to(root).as_posix(),
        "report_sha256": (
            sha256_file(validation_path) if validation_path.is_file() else None
        ),
        "evidence_files": len(evidence),
        "compiled_files": compile_result.get("files"),
        "tests_passed": pytest_result.get("passed"),
        "full_reruns": len(full_reruns),
        "full_reruns_passed": sum(
            row.get("status") == "passed"
            for row in full_reruns
            if isinstance(row, dict)
        ),
        "account_processes_started": validation.get(
            "account_processes_started", 0
        ),
    }
    current_archive_sha256 = sha256_file(current_zip)
    if (
        current_validation_receipt.get("status") != "passed"
        or str(current_validation_receipt.get("archive_sha256") or "").lower()
        != current_archive_sha256
        or current_validation_receipt.get("content_sha256")
        != manifest.get("content_sha256")
        or not current_validation_receipt.get("reconstructability_verified")
    ):
        raise RuntimeError(
            "checkpoint pointer requires a current, hash-bound reconstruction receipt"
        )
    reported_clean_import_sha256 = str(
        clean_import.get("archive_sha256") or ""
    ).lower()
    clean_import_status = clean_import.get("status") or "pending"
    if clean_import_status == "passed" and (
        not reported_clean_import_sha256
        or reported_clean_import_sha256 != current_archive_sha256
    ):
        clean_import_status = "stale_not_current_archive"
    clean_import_validation = {
        "status": clean_import_status,
        "report": (
            clean_import_path.relative_to(root).as_posix()
            if clean_import_path.is_file()
            else None
        ),
        "report_sha256": (
            sha256_file(clean_import_path) if clean_import_path.is_file() else None
        ),
        "archive_sha256": clean_import.get("archive_sha256"),
        "payload_files_verified": clean_import.get("payload_files_verified"),
        "tests_passed": clean_import.get("tests_passed"),
        "full_reruns_passed": clean_import.get("full_reruns_passed"),
        "credential_like_files": clean_import.get("credential_like_files", 0),
        "account_processes_started": clean_import.get(
            "account_processes_started", 0
        ),
        "current_archive_sha256": current_archive_sha256,
        "matches_current_archive": (
            reported_clean_import_sha256 == current_archive_sha256
        ),
    }

    return {
        "schema_version": 5,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "checkpoint_source_root": str(root),
        "runtime_root": os.environ.get(
            "FOREX_MODEL_RUNTIME_ROOT",
            str(root / "runtime" / "model_gap"),
        ),
        "recovery_order": [current_zip.name],
        "base_checkpoint": {
            "archive": current_zip.name,
            "timestamped_archive": (
                timestamped_archive.name if timestamped_archive else None
            ),
            "archive_bytes": current_zip.stat().st_size,
            "archive_sha256": current_archive_sha256,
            "created_utc": manifest.get("created_utc"),
            "payload_file_count": manifest.get("file_count"),
            "represented_bytes": manifest.get("total_bytes"),
            "content_sha256": manifest.get("content_sha256"),
        },
        "standalone_bootstrap": standalone_bootstrap,
        "import_contract": {
            "manifest_verified": True,
            "credential_like_files": 0,
            "raw_market_data_included": False,
            "foundation_model_weights_included": False,
            "account_processes_started": 0,
            "account_wiring_changed": False,
            "production_model_promoted": False,
        },
        "validation": {
            "current_archive_reconstruction": current_validation_receipt,
            "source_full_recreation": source_validation,
            "clean_import_and_full_recreation": clean_import_validation,
        },
        "retention": retention_result,
        "model_gap_summary": {
            "registry_models": completion_summary.get("models"),
            "families": completion_summary.get("families"),
            "package_runtimes_available": runtime_summary.get(
                "models_runtime_available"
            ),
            "runnable_including_pinned_s4": completion_summary.get(
                "runtime_available"
            ),
            "synthetic_or_better": completion_summary.get(
                "synthetic_or_better"
            ),
            "bounded_market_qualified": completion_summary.get(
                "bounded_market_qualified"
            ),
            "market_backtest_required": completion_summary.get(
                "market_backtest_required"
            ),
            "market_backtest_complete": completion_summary.get(
                "market_backtest_complete"
            ),
            "market_performance_passed": completion_summary.get(
                "market_performance_passed"
            ),
            "all_runnable_models_market_backtested": completion_summary.get(
                "all_runnable_models_market_backtested"
            ),
            "explicit_blockers": runtime.get("explicit_blockers") or {},
            "production_eligible": completion_summary.get(
                "production_eligible", 0
            ),
            "account_wired": completion_summary.get("account_wired", 0),
        },
        "prediction_space": {
            "pairs": 68,
            "input_timeframes": 13,
            "outcome_horizons": 12,
            "timeframe_horizon_cells": matrix_coverage.get(
                "observed_timeframe_horizon_cells"
            ),
            "pair_expanded_cells": matrix_coverage.get("observed_pair_cells"),
            "coverage_ratio": matrix_coverage.get("coverage_ratio"),
            "total_events": matrix_coverage.get("total_events"),
            "fully_covered": matrix_coverage.get("fully_covered"),
        },
        "strict_ma_feature_grid": {
            "family": ma_grid.get("family"),
            "report": (
                ma_grid_path.relative_to(root).as_posix()
                if ma_grid_path.is_file()
                else None
            ),
            "report_sha256": (
                sha256_file(ma_grid_path) if ma_grid_path.is_file() else None
            ),
            "artifact": ma_grid.get("artifact"),
            "artifact_sha256": ma_grid.get("artifact_sha256"),
            "pairs": ma_grid.get("instrument_count"),
            "input_timeframes": len(ma_grid.get("timeframes") or []),
            "timeframes": ma_grid.get("timeframes") or [],
            "outcome_horizons": len(ma_grid.get("horizons_sec") or []),
            "horizons_sec": ma_grid.get("horizons_sec") or [],
            "planned_cells": ma_planned_cells,
            "fitted_cells": ma_fitted_cells,
            "unsupported_cells": ma_grid.get("unsupported_cell_count"),
            "coverage_ratio": ma_coverage_ratio,
            "feature_count_by_timeframe": ma_grid_contract.get(
                "feature_count_by_timeframe"
            )
            or {},
            "execution_policy": ma_grid_contract.get("execution_policy"),
            "real_account_authorized": ma_grid.get(
                "real_account_authorized", False
            ),
        },
        "dashboard": {
            "summary": "http://127.0.0.1:8765/",
            "model_viewer": "http://127.0.0.1:8765/full",
        },
        "runtime_note": (
            "Importing this checkpoint does not start trading, collection, "
            "backfill, dashboard, or account workers. The checkpoint changes "
            "no account wiring and promotes no model."
        ),
        "storage_warning": (
            "D: remains physically suspect after prior SMART/read failures and "
            "Event ID 7 bad blocks. Heavy operations are guarded; the latest "
            f"observed Event ID 7 record is {disk_guard_record}. The OneDrive "
            "vault is the intentional C: recovery copy."
        ),
    }


def sync_once(root: Path, destinations: list[Path], retention: int) -> dict:
    if isinstance(retention, bool) or not isinstance(retention, int) or retention < 1:
        raise ValueError("retention must be an integer of at least one")
    if not destinations:
        raise ValueError("at least one checkpoint destination is required")
    resolved_destinations = [path.resolve() for path in destinations]
    if len(set(resolved_destinations)) != len(resolved_destinations):
        raise ValueError("checkpoint destinations must be unique")

    files = collect_files(root)
    manifest = build_manifest(root, files)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    states = []
    for vault_project in resolved_destinations:
        vault_project.mkdir(parents=True, exist_ok=True)
        current_manifest = (
            vault_project / "forex_model_checkpoint_current.manifest.json"
        )
        previous = {}
        if current_manifest.exists():
            try:
                previous = json.loads(
                    current_manifest.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError):
                previous = {}
        current_zip = vault_project / "forex_model_checkpoint_current.zip"
        states.append(
            {
                "vault_project": vault_project,
                "current_manifest": current_manifest,
                "current_zip": current_zip,
                "previous": previous,
                "changed": (
                    previous.get("content_sha256")
                    != manifest["content_sha256"]
                    or not current_zip.exists()
                ),
            }
        )

    rebuild = any(state["changed"] for state in states)
    if not rebuild and len(states) > 1:
        archive_hashes = {
            sha256_file(state["current_zip"]) for state in states
        }
        rebuild = len(archive_hashes) != 1

    if rebuild:
        canonical_zip = states[0]["current_zip"]
        manifest = write_zip(root, files, manifest, canonical_zip)
        for state in states[1:]:
            copy_file_atomic(canonical_zip, state["current_zip"])
    else:
        # Use the exact previously embedded manifest, including its creation time.
        manifest = states[0]["previous"]

    # Phase one: validate every current archive and new timestamped copy, and
    # durably record a current hash-bound receipt. No retention deletion is
    # permitted until every destination completes this phase.
    for state in states:
        vault_project = state["vault_project"]
        current_zip = state["current_zip"]
        receipt_path = vault_project / MODEL_VALIDATION_RECEIPT
        existing_receipt = load_json_optional(receipt_path)
        observed_archive_sha256 = sha256_file(current_zip)
        reuse_reconstruction = (
            not rebuild
            and existing_receipt.get("status") == "passed"
            and existing_receipt.get("archive_sha256")
            == observed_archive_sha256
            and existing_receipt.get("content_sha256")
            == manifest.get("content_sha256")
            and existing_receipt.get("reconstructability_verified") is True
        )
        validation_receipt = validate_checkpoint_archive(
            current_zip,
            manifest,
            reconstruct=not reuse_reconstruction,
        )
        if reuse_reconstruction:
            validation_receipt.update(
                {
                    "reconstructability_verified": True,
                    "reconstructed_files_verified": existing_receipt.get(
                        "reconstructed_files_verified"
                    ),
                    "reconstruction_verified_utc": existing_receipt.get(
                        "reconstruction_verified_utc"
                    )
                    or existing_receipt.get("verified_utc"),
                    "reused_reconstruction_receipt_sha256": sha256_file(
                        receipt_path
                    ),
                }
            )

        timestamped = None
        if rebuild:
            timestamped = vault_project / f"forex_model_checkpoint_{stamp}.zip"
            copy_file_atomic(current_zip, timestamped)
            timestamped_validation = validate_checkpoint_archive(
                timestamped,
                manifest,
                reconstruct=False,
            )
            if (
                timestamped_validation["archive_sha256"]
                != validation_receipt["archive_sha256"]
            ):
                raise RuntimeError(
                    "timestamped checkpoint differs from validated current archive"
                )
            validation_receipt["timestamped_archive"] = {
                "archive": timestamped.name,
                "archive_sha256": timestamped_validation["archive_sha256"],
                "archive_bytes": timestamped_validation["archive_bytes"],
                "zip_crc_verified": timestamped_validation["zip_crc_verified"],
            }
        else:
            archives = list_timestamped_model_archives(vault_project)
            timestamped = archives[0] if archives else None
        write_json_atomic(receipt_path, validation_receipt)
        state.update(
            {
                "timestamped": timestamped,
                "validation_receipt": validation_receipt,
                "validation_receipt_path": receipt_path,
            }
        )

    # Phase two: all destinations are now verified. Record each exact target
    # before applying retention. An unchanged run never prunes history.
    for state in states:
        if rebuild:
            retention_result = apply_checkpoint_retention(
                state["vault_project"],
                retention=retention,
                validation_receipt_path=state["validation_receipt_path"],
                validation_receipt=state["validation_receipt"],
                protected_archive=state["timestamped"],
                stamp=stamp,
            )
        else:
            retention_result = {
                "schema_version": 1,
                "status": "not_evaluated_unchanged",
                "retention": retention,
                "tombstone": None,
                "deleted": [],
            }
        state["retention_result"] = retention_result

    results = []
    for state in states:
        vault_project = state["vault_project"]
        current_manifest = state["current_manifest"]
        current_zip = state["current_zip"]
        timestamped = state["timestamped"]
        write_json_atomic(current_manifest, manifest)
        artifact_result = sync_second_forecast_artifacts(root, vault_project)
        bootstrap_result = sync_bootstrap_files(root, vault_project)
        clean_import_result = sync_clean_import_artifacts(root, vault_project)
        canonical_records_result = sync_canonical_project_records(
            root, vault_project
        )
        pointer_path = vault_project / "CHECKPOINT_LATEST.json"
        pointer = build_checkpoint_pointer(
            root,
            manifest,
            current_zip,
            timestamped,
            bootstrap_result,
            state["validation_receipt"],
            state["retention_result"],
        )
        write_json_atomic(pointer_path, pointer)
        results.append(
            {
                "destination": str(vault_project),
                "changed": bool(rebuild),
                "zip": str(current_zip),
                "second_forecast_artifacts": artifact_result,
                "bootstrap_files": bootstrap_result,
                "clean_import_artifacts": clean_import_result,
                "canonical_project_records": canonical_records_result,
                "current_archive_validation": {
                    "path": str(state["validation_receipt_path"]),
                    "sha256": sha256_file(state["validation_receipt_path"]),
                    "archive_sha256": state["validation_receipt"][
                        "archive_sha256"
                    ],
                    "status": state["validation_receipt"]["status"],
                },
                "retention": state["retention_result"],
                "checkpoint_pointer": {
                    "path": str(pointer_path),
                    "sha256": sha256_file(pointer_path),
                    "archive_sha256": pointer["base_checkpoint"][
                        "archive_sha256"
                    ],
                },
            }
        )
    return {"manifest": manifest, "destinations": results}


def sync_news_event_checkpoint(
    root: Path,
    destinations: list[Path],
) -> dict:
    files = collect_news_event_checkpoint_files(root)
    manifest = build_manifest(root, files)
    results = []
    canonical_zip: Path | None = None
    for destination in destinations:
        package_root = (
            destination
            / "updates"
            / "all_pair_news_event_tagging_20260727"
        )
        package_root.mkdir(parents=True, exist_ok=True)
        archive = package_root / "all_pair_news_event_tagging_current.zip"
        if canonical_zip is None:
            manifest = write_zip(root, files, manifest, archive)
            canonical_zip = archive
        else:
            copy_file_atomic(canonical_zip, archive)
        manifest_path = (
            package_root
            / "all_pair_news_event_tagging_current.manifest.json"
        )
        write_json_atomic(manifest_path, manifest)
        exposed = {}
        for relative_path in (
            Path("docs/FUTURE_NOTES.md"),
            Path(
                "trad/data/oanda_training_manager/news_event_tags/"
                "ALL_PAIR_NEWS_EVENT_TAGGING_LATEST.md"
            ),
            Path(
                "trad/data/oanda_training_manager/news_event_tags/"
                "run_latest.json"
            ),
            Path(
                "trad/data/oanda_training_manager/news_event_tags/"
                "latest_pair_news_context.json"
            ),
        ):
            source = root / relative_path
            if source.is_file():
                target = package_root / source.name
                copy_file_atomic(source, target)
                exposed[source.name] = {
                    "path": str(target),
                    "sha256": sha256_file(target),
                }
        pointer = {
            "schema_version": "forex_news_event_checkpoint_v1",
            "created_utc": manifest["created_utc"],
            "source_root": str(root),
            "archive": str(archive),
            "archive_sha256": sha256_file(archive),
            "manifest": str(manifest_path),
            "content_sha256": manifest["content_sha256"],
            "file_count": manifest["file_count"],
            "total_bytes": manifest["total_bytes"],
            "exposed_files": exposed,
            "raw_market_data_included": False,
            "credentials_included": False,
        }
        pointer_path = package_root / "CHECKPOINT_LATEST.json"
        write_json_atomic(pointer_path, pointer)
        results.append(
            {
                "destination": str(package_root),
                "archive": str(archive),
                "archive_sha256": pointer["archive_sha256"],
                "manifest": str(manifest_path),
                "pointer": str(pointer_path),
            }
        )
    return {"manifest": manifest, "destinations": results}


def parse_args() -> argparse.Namespace:
    default_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=default_root)
    parser.add_argument("--interval-sec", type=int, default=0, help="Repeat interval; zero runs once.")
    parser.add_argument("--retention", type=int, default=12)
    parser.add_argument("--destination", type=Path, action="append")
    parser.add_argument("--news-event-only", action="store_true")
    parser.add_argument(
        "--canonical-only",
        action="store_true",
        help=(
            "Publish only the small credential-free current project records; "
            "do not build, copy, or prune checkpoint archives."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    destinations = args.destination or [DEFAULT_VAULT_PROJECT]
    while True:
        if args.news_event_only and args.canonical_only:
            raise ValueError("--news-event-only and --canonical-only are mutually exclusive")
        if args.canonical_only:
            results = []
            for destination in destinations:
                destination = destination.resolve()
                destination.mkdir(parents=True, exist_ok=True)
                records = sync_canonical_project_records(
                    args.root.resolve(),
                    destination,
                )
                results.append(
                    {
                        "destination": str(destination),
                        "changed": records["changed"],
                        "canonical_project_records": records,
                    }
                )
            result = {
                "manifest": {
                    "content_sha256": hashlib.sha256(
                        json.dumps(
                            [
                                item["canonical_project_records"]["records"]
                                for item in results
                            ],
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest(),
                    "file_count": sum(
                        len(item["canonical_project_records"]["records"])
                        for item in results
                    ),
                    "total_bytes": sum(
                        row["size"]
                        for item in results
                        for row in item["canonical_project_records"]["records"]
                    ),
                },
                "destinations": results,
            }
        elif args.news_event_only:
            result = sync_news_event_checkpoint(
                args.root.resolve(),
                destinations,
            )
        else:
            result = sync_once(
                args.root.resolve(),
                destinations,
                args.retention,
            )
        print(json.dumps({
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "content_sha256": result["manifest"]["content_sha256"],
            "file_count": result["manifest"]["file_count"],
            "total_bytes": result["manifest"]["total_bytes"],
            "destinations": result["destinations"],
        }), flush=True)
        if args.interval_sec <= 0:
            return 0
        time.sleep(max(60, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
