from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest

import forex_model_vault_sync as vault_sync


def _write(root: Path, relative: str, text: str = "{}") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _fixture_records(monkeypatch, root: Path) -> None:
    """These historical fixtures intentionally configure only their own records."""
    monkeypatch.setattr(vault_sync, "CANONICAL_PROJECT_RECORDS", tuple(
        record for record in vault_sync.CANONICAL_PROJECT_RECORDS
        if (root / record[0]).is_file()
    ))


def _runtime_fixture_record(monkeypatch) -> None:
    monkeypatch.setattr(vault_sync, "CANONICAL_PROJECT_RECORDS", (
        (Path("trad/runtime.py"), "RUNTIME_TEST_CURRENT.py"),
    ))


def test_checkpoint_collects_current_audit_and_model_metadata(tmp_path) -> None:
    _write(tmp_path, "trad/runtime.py", "print('runtime')")
    _write(tmp_path, "trad/creds", "must-not-archive")
    _write(
        tmp_path,
        "docs/RUNTIME_AND_SMA_AUDIT_20260726.md",
        "current runtime audit",
    )
    _write(tmp_path, "docs/MODEL_GAP_ROADMAP_20260718.md", "roadmap")
    _write(
        tmp_path,
        "docs/ALL_SIGNAL_MATRIX_FINALITY_20260718.md",
        "finality contract",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_lifecycle/index.json",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_lifecycle/research_queue.jsonl",
        "{\"queued\": true}\n",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_lifecycle/"
        "research_queue_before_prune.jsonl",
        "old backup",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_lifecycle/candidates/a.json",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_space/"
        "modern_model_dependency_status_latest.json",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_space/"
        "vault_clean_import_validation_latest.json",
        "clean import proof",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "panel_m1_compact_canary_20260718.parquet",
        "compact fixture",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "foundation_m1_holdout_v1.npz",
        "foundation fixture",
    )
    _write(
        tmp_path,
        "trad/config/runtime_requirements/"
        "requirements-core_timeseries-exact.txt",
        "example==1.0\n",
    )
    _write(
        tmp_path,
        "trad/config/runtime_requirements_d/"
        "requirements-core_timeseries-exact.txt",
        "example==1.0\n",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/state/account_dashboard_v1.json",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/state/sma_signal_filter_v1.json",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/state/sma_signal_filter_v1.joblib",
        "model",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/models/ma_feature_grid/"
        "ma_feature_grid_latest.joblib",
        "ma model",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_latest.json",
        '{"family": "moving_average_feature_grid"}',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_metrics_latest.csv",
        "timeframe,horizon_sec\n",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "MA_FEATURE_GRID_LATEST.md",
        "ma summary",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/news_event_tags/manifest.json",
        '{"event_count": 37}',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_tags.parquet",
        "compact news result",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/news_event_tags/"
        ".failed_news_tag.tmp",
        "must-not-archive",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/models/"
        "ma_feature_grid_intensive_xgb/ma_feature_grid_latest.joblib",
        "xgb challenger",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "ma_feature_grid_intensive_xgb/ma_feature_grid_latest.json",
        '{"family": "moving_average_feature_grid"}',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "ma_feature_grid_intensive_xgb/ma_feature_grid_20260727.json",
        '{"stamped": true}',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/raw/candles.json",
        "raw data",
    )
    matrix_root = (
        "trad/data/oanda_training_manager/training_sets/"
        "model_gap_full_matrix/full_run"
    )
    _write(tmp_path, f"{matrix_root}/full_matrix_state.json")
    _write(tmp_path, f"{matrix_root}/coverage_cells.csv", "cell,count\n")
    _write(tmp_path, f"{matrix_root}/panel_m1.parquet.manifest.json")
    _write(tmp_path, f"{matrix_root}/panel_m1.parquet", "bulk panel")
    crossover_root = (
        "trad/data/oanda_training_manager/reports/"
        "moving_average_crossover_sweep"
    )
    _write(tmp_path, f"{crossover_root}/smoke_newer/manifest.json")
    _write(tmp_path, f"{crossover_root}/all68_wide/SUMMARY.md", "summary")
    _write(tmp_path, f"{crossover_root}/all68_wide/manifest.json")
    _write(
        tmp_path,
        f"{crossover_root}/all68_wide/aggregate_results.parquet",
        "compact aggregate",
    )
    _write(tmp_path, f"{crossover_root}/all68_recent_exact/manifest.json")
    _write(
        tmp_path,
        f"{crossover_root}/all68_recent_exact/ASSESSMENT.md",
        "exact assessment",
    )
    entry_root = (
        "trad/data/oanda_training_manager/reports/"
        "moving_average_crossover_entry_quality"
    )
    _write(tmp_path, f"{entry_root}/smoke_newer/manifest.json")
    _write(tmp_path, f"{entry_root}/all68_full/manifest.json")
    _write(
        tmp_path,
        f"{entry_root}/all68_full/entry_quality_aggregate.parquet",
        "entry aggregate",
    )
    _write(tmp_path, f"{entry_root}/all68_exact/manifest.json")
    _write(
        tmp_path,
        f"{entry_root}/all68_exact/ASSESSMENT.md",
        "entry assessment",
    )
    stack_root = (
        "trad/data/oanda_training_manager/reports/"
        "multitimeframe_crossover_stack"
    )
    _write(tmp_path, f"{stack_root}/smoke_newer/manifest.json")
    _write(tmp_path, f"{stack_root}/all68_extended/manifest.json")
    _write(
        tmp_path,
        f"{stack_root}/all68_extended/stack_entry_quality_aggregate.parquet",
        "stack aggregate",
    )
    _write(
        tmp_path,
        f"{stack_root}/all68_extended/ASSESSMENT.md",
        "stack assessment",
    )
    sma_filter_root = (
        "trad/data/oanda_training_manager/reports/sma_signal_filter"
    )
    _write(tmp_path, f"{sma_filter_root}/smoke_newer/manifest.json")
    _write(tmp_path, f"{sma_filter_root}/all68_current/manifest.json")
    _write(
        tmp_path,
        f"{sma_filter_root}/all68_current/assessment.md",
        "filter assessment",
    )
    _write(
        tmp_path,
        f"{sma_filter_root}/all68_current/model_comparison.csv",
        "model,net\n",
    )

    paths = {
        path.relative_to(tmp_path).as_posix()
        for path in vault_sync.collect_files(tmp_path)
    }

    assert "trad/runtime.py" in paths
    assert "docs/RUNTIME_AND_SMA_AUDIT_20260726.md" in paths
    assert "docs/MODEL_GAP_ROADMAP_20260718.md" in paths
    assert "docs/ALL_SIGNAL_MATRIX_FINALITY_20260718.md" in paths
    assert (
        "trad/data/oanda_training_manager/model_lifecycle/candidates/a.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/state/account_dashboard_v1.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/state/sma_signal_filter_v1.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/state/sma_signal_filter_v1.joblib"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/models/ma_feature_grid/"
        "ma_feature_grid_latest.joblib"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/models/"
        "ma_feature_grid_intensive_xgb/ma_feature_grid_latest.joblib"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/reports/"
        "ma_feature_grid_intensive_xgb/ma_feature_grid_latest.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/reports/"
        "ma_feature_grid_intensive_xgb/ma_feature_grid_20260727.json"
        not in paths
    )
    assert (
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_latest.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_metrics_latest.csv"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "MA_FEATURE_GRID_LATEST.md"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/news_event_tags/manifest.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_tags.parquet"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/news_event_tags/"
        ".failed_news_tag.tmp"
        not in paths
    )
    assert (
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "panel_m1_compact_canary_20260718.parquet"
        in paths
    )
    assert (
        "trad/config/runtime_requirements/"
        "requirements-core_timeseries-exact.txt"
        in paths
    )
    assert (
        "trad/config/runtime_requirements_d/"
        "requirements-core_timeseries-exact.txt"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/model_space/validation_fixtures/"
        "foundation_m1_holdout_v1.npz"
        in paths
    )
    assert "trad/creds" not in paths
    assert (
        "trad/data/oanda_training_manager/model_lifecycle/"
        "research_queue_before_prune.jsonl"
        not in paths
    )
    assert "trad/data/oanda_training_manager/raw/candles.json" not in paths
    assert f"{matrix_root}/full_matrix_state.json" in paths
    assert f"{matrix_root}/coverage_cells.csv" in paths
    assert f"{matrix_root}/panel_m1.parquet.manifest.json" in paths
    assert f"{matrix_root}/panel_m1.parquet" not in paths
    assert f"{crossover_root}/all68_wide/manifest.json" in paths
    assert f"{crossover_root}/all68_wide/SUMMARY.md" in paths
    assert f"{crossover_root}/all68_wide/aggregate_results.parquet" in paths
    assert f"{crossover_root}/all68_recent_exact/manifest.json" in paths
    assert f"{crossover_root}/all68_recent_exact/ASSESSMENT.md" in paths
    assert f"{crossover_root}/smoke_newer/manifest.json" not in paths
    assert f"{entry_root}/all68_full/manifest.json" in paths
    assert f"{entry_root}/all68_full/entry_quality_aggregate.parquet" in paths
    assert f"{entry_root}/all68_exact/manifest.json" in paths
    assert f"{entry_root}/all68_exact/ASSESSMENT.md" in paths
    assert f"{entry_root}/smoke_newer/manifest.json" not in paths
    assert f"{stack_root}/all68_extended/manifest.json" in paths
    assert (
        f"{stack_root}/all68_extended/stack_entry_quality_aggregate.parquet"
        in paths
    )
    assert f"{stack_root}/all68_extended/ASSESSMENT.md" in paths
    assert f"{stack_root}/smoke_newer/manifest.json" not in paths
    assert f"{sma_filter_root}/all68_current/manifest.json" in paths
    assert f"{sma_filter_root}/all68_current/assessment.md" in paths
    assert f"{sma_filter_root}/all68_current/model_comparison.csv" in paths
    assert f"{sma_filter_root}/smoke_newer/manifest.json" not in paths
    assert (
        "trad/data/oanda_training_manager/model_space/"
        "vault_clean_import_validation_latest.json"
        not in paths
    )


def test_checkpoint_zip_matches_manifest(tmp_path) -> None:
    _write(tmp_path, "trad/runtime.py", "print('runtime')")
    files = vault_sync.collect_files(tmp_path)
    manifest = vault_sync.build_manifest(tmp_path, files)
    destination = tmp_path / "checkpoint.zip"

    written = vault_sync.write_zip(tmp_path, files, manifest, destination)

    with zipfile.ZipFile(destination) as archive:
        embedded = json.loads(
            archive.read("MODEL_CHECKPOINT_MANIFEST.json").decode("utf-8")
        )
        names = set(archive.namelist())
    assert embedded["content_sha256"] == manifest["content_sha256"]
    assert written["content_sha256"] == embedded["content_sha256"]
    assert "trad/runtime.py" in names
    assert embedded["file_count"] == len(names) - 1


def test_targeted_news_checkpoint_excludes_raw_rolling_data(tmp_path) -> None:
    _write(tmp_path, "docs/FUTURE_NOTES.md", "future notes\n")
    _write(tmp_path, "trad/oanda_news_event_tagger.py", "TAGGER = True\n")
    _write(
        tmp_path,
        "trad/oanda_local_news_sentiment.py",
        "LOCAL_NEWS = True\n",
    )
    _write(
        tmp_path,
        "trad/oanda_signal_news_monitor.py",
        "MONITOR = True\n",
    )
    _write(
        tmp_path,
        "trad/oanda_news_outcome_improvement_audit.py",
        "AUTOMATIC_AUDIT = True\n",
    )
    _write(
        tmp_path,
        "trad/config/news_sources_v1.json",
        '{"sources": []}\n',
    )
    _write(
        tmp_path,
        "trad/test_oanda_local_news_sentiment.py",
        "def test_local_news(): pass\n",
    )
    _write(
        tmp_path,
        "trad/test_oanda_signal_news_monitor.py",
        "def test_signal_news_monitor(): pass\n",
    )
    _write(
        tmp_path,
        "trad/test_oanda_news_outcome_improvement_audit.py",
        "def test_automatic_news_audit(): pass\n",
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/state/"
        "news_outcome_improvement_audit_v2.json",
        '{"status": "ok"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/state/"
        "news_improvement_queue_v2.json",
        '{"queue_count": 1}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/news_event_tags/manifest.json",
        '{"event_count": 37}',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_tags.parquet",
        "compact result",
    )
    _write(
        tmp_path,
        "trad/data/market_movement_ledger/market_movements.csv",
        "raw rolling data",
    )

    paths = {
        path.relative_to(tmp_path).as_posix()
        for path in vault_sync.collect_news_event_checkpoint_files(tmp_path)
    }

    assert "trad/oanda_news_event_tagger.py" in paths
    assert "trad/oanda_local_news_sentiment.py" in paths
    assert "trad/oanda_signal_news_monitor.py" in paths
    assert "trad/oanda_news_outcome_improvement_audit.py" in paths
    assert "trad/config/news_sources_v1.json" in paths
    assert "trad/test_oanda_local_news_sentiment.py" in paths
    assert "trad/test_oanda_signal_news_monitor.py" in paths
    assert "trad/test_oanda_news_outcome_improvement_audit.py" in paths
    assert (
        "trad/data/oanda_training_manager/state/"
        "news_outcome_improvement_audit_v2.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/state/news_improvement_queue_v2.json"
        in paths
    )
    assert "docs/FUTURE_NOTES.md" in paths
    assert (
        "trad/data/oanda_training_manager/news_event_tags/manifest.json"
        in paths
    )
    assert (
        "trad/data/oanda_training_manager/news_event_tags/"
        "significant_move_news_tags.parquet"
        in paths
    )
    assert (
        "trad/data/market_movement_ledger/market_movements.csv"
        not in paths
    )


def test_checkpoint_manifest_hashes_exact_archived_bytes(tmp_path) -> None:
    source = _write(tmp_path, "trad/runtime.py", "before\n")
    files = vault_sync.collect_files(tmp_path)
    stale_manifest = vault_sync.build_manifest(tmp_path, files)
    source.write_bytes(b"after\n")
    destination = tmp_path / "checkpoint.zip"

    written = vault_sync.write_zip(
        tmp_path,
        files,
        stale_manifest,
        destination,
    )

    with zipfile.ZipFile(destination) as archive:
        embedded = json.loads(
            archive.read("MODEL_CHECKPOINT_MANIFEST.json").decode("utf-8")
        )
        payload = archive.read("trad/runtime.py")
    expected_hash = hashlib.sha256(payload).hexdigest()
    row = embedded["files"][0]
    assert payload == b"after\n"
    assert row["sha256"] == expected_hash
    assert written["content_sha256"] == embedded["content_sha256"]
    assert embedded["content_sha256"] != stale_manifest["content_sha256"]


def test_binary_snapshot_read_retries_atomic_writer_lock(
    tmp_path,
    monkeypatch,
) -> None:
    source = _write(tmp_path, "state.json", '{"ok": true}\n')
    expected = source.read_bytes()
    original_open = Path.open
    attempts = 0

    def flaky_open(path, *args, **kwargs):
        nonlocal attempts
        if path == source and attempts == 0:
            attempts += 1
            raise PermissionError("atomic writer owns destination")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", flaky_open)

    with vault_sync.open_binary_read(
        source,
        attempts=2,
        retry_delay_sec=0.0,
    ) as handle:
        assert handle.read() == expected
    assert attempts == 1


def test_bootstrap_files_are_synced_with_hashes(tmp_path) -> None:
    _write(tmp_path, "trad/forex_vault_import.py", "print('import')\n")
    _write(tmp_path, "trad/forex_vault_bootstrap.ps1", "Write-Output 'ok'\n")
    _write(
        tmp_path,
        "docs/VAULT_IMPORT_AND_VALIDATION_20260719.md",
        "# Import\n",
    )
    vault = tmp_path / "vault"
    vault.mkdir()

    result = vault_sync.sync_bootstrap_files(tmp_path, vault)

    assert result["changed"] is True
    assert len(result["files"]) == 3
    assert (vault / "forex_vault_import.py").is_file()
    assert (vault / "forex_vault_bootstrap.ps1").is_file()


def test_canonical_project_records_sync_sources_features_and_progress(tmp_path, monkeypatch) -> None:
    _write(tmp_path, "trad/FOREX_PENDING_IMPROVEMENTS.md", "pending-v1\n")
    _write(tmp_path, "trad/FOREX_PROJECT_LOG.md", "log-v1\n")
    _write(
        tmp_path,
        "trad/FOREX_OFFICIAL_GEOPOLITICAL_AND_SCHEDULED_EVENT_CAPTURE_"
        "VALIDATION_20260901.json",
        '{"status": "validated_prospective"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_VAULT_CANONICAL_SNAPSHOT_VALIDATION_20260901.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_D_"
        "VALIDATION_20260901.json",
        '{"cohort_id": "all68_executable_move_census_v2_20260901d"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_E_"
        "VALIDATION_20260901.json",
        '{"cohort_id": "all68_executable_move_census_v2_20260901e"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_F_"
        "VALIDATION_20260901.json",
        '{"cohort_id": "all68_executable_move_census_v3_20260901f"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_G_"
        "VALIDATION_20260902.json",
        '{"cohort_id": "all68_executable_move_census_v3_20260902g"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_OPERATIONAL_MAPPING_SUBSECOND_CAUSALITY_"
        "VALIDATION_20260902.json",
        '{"status": "validated_subsecond_causal"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_INTEGRITY_PUBLICATION_FRESHNESS_"
        "VALIDATION_20260901.json",
        '{"status": "ok"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_MAJOR_MOVE_CENSUS_LIVENESS_"
        "VALIDATION_20260901.json",
        '{"status": "validated_live"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_LIFECYCLE_PUBLICATION_CONSISTENCY_"
        "VALIDATION_20260901.json",
        '{"status": "validated_live"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_SOURCE_READINESS_AND_INTEGRITY_FRESHNESS_"
        "VALIDATION_20260901.json",
        '{"status": "validated_live"}\n',
    )
    _write(
        tmp_path,
        "trad/config/forex_source_gap_register_v1.json",
        '{"repository_controlled_pending": 0}\n',
    )
    _write(tmp_path, "trad/MODEL_FEATURE_SPACE.md", "features-v1\n")
    _write(
        tmp_path,
        "trad/config/official_currency_source_depth_v1.json",
        '{"currency_count": 21}\n',
    )
    _write(
        tmp_path,
        "trad/config/news_sources_v1.json",
        '{"sources": []}\n',
    )
    _write(
        tmp_path,
        "trad/config/scheduled_event_quote_capture_v2.json",
        '{"contract_id": "scheduled-event-v2"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_LIFECYCLE_VERIFIER_CADENCE_VALIDATION_20260902.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_PROJECT_INTEGRITY_HISTORY_ROTATION_VALIDATION_20260902.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_LIFECYCLE_GENEALOGY_LOCK_RETRY_VALIDATION_20260902.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_LIFECYCLE_SEMANTIC_REPUBLICATION_VALIDATION_20260902.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_SIGNAL_TRIAL_BARRIER_SUMMARY_PERFORMANCE_"
        "VALIDATION_20260902.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/FOREX_SUPERVISOR_PROCESS_SNAPSHOT_PERFORMANCE_"
        "VALIDATION_20260902.json",
        '{"status": "validated"}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_calendar_episode_labels_v1_20260901.json",
        '{"research_only": true}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_live_case_capture_v4_20260901.json",
        '{"cohort_id": "v4"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "move_first_live_case_capture_v4/"
        "MOVE_FIRST_LIVE_CASE_CAPTURE_CURRENT.json",
        '{"case_count": 1}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_live_arm_alignment_v1_20260901.json",
        '{"audit_id": "alignment-v1"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "move_first_live_arm_alignment_v1/"
        "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CURRENT.json",
        '{"resolved_factor_episode_count": 1}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_operational_mapping_alignment_v1_20260901.json",
        '{"cohort_id": "operational-mapping-v1"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "move_first_operational_mapping_alignment_v1/"
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json",
        '{"resolved_factor_episode_count": 0}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_operational_mapping_alignment_v2_20260901.json",
        '{"cohort_id": "operational-mapping-v2"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "move_first_operational_mapping_alignment_v2/"
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json",
        '{"resolved_factor_episode_count": 0, "story_deduplication_rule": "v2"}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_operational_mapping_alignment_v3_20260901.json",
        '{"cohort_id": "operational-mapping-v3"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "move_first_operational_mapping_alignment_v3/"
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json",
        '{"resolved_factor_episode_count": 0, "story_deduplication_rule": "v3"}\n',
    )
    _write(
        tmp_path,
        "trad/config/move_first_operational_mapping_alignment_v4_20260902.json",
        '{"cohort_id": "operational-mapping-v4"}\n',
    )
    _write(
        tmp_path,
        "trad/data/oanda_training_manager/reports/"
        "move_first_operational_mapping_alignment_v4/"
        "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CURRENT.json",
        '{"resolved_factor_episode_count": 0, "story_deduplication_rule": "v4"}\n',
    )
    vault = tmp_path / "vault"
    vault.mkdir()

    _fixture_records(monkeypatch, tmp_path)
    first = vault_sync.sync_canonical_project_records(tmp_path, vault)
    assert first["changed"] is True
    assert len(first["records"]) == 37
    assert (vault / "PENDING_IMPROVEMENTS_CURRENT.md").read_text() == "pending-v1\n"
    assert (vault / "PROJECT_LOG_CURRENT.md").read_text() == "log-v1\n"
    assert (
        vault / "EXECUTABLE_MOVE_CENSUS_COHORT_D_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "EXECUTABLE_MOVE_CENSUS_COHORT_E_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "EXECUTABLE_MOVE_CENSUS_COHORT_F_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "EXECUTABLE_MOVE_CENSUS_COHORT_G_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "OPERATIONAL_MAPPING_ALIGNMENT_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "INTEGRITY_PUBLICATION_FRESHNESS_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "MAJOR_MOVE_CENSUS_LIVENESS_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "LIFECYCLE_PUBLICATION_CONSISTENCY_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault
        / "SOURCE_READINESS_AND_INTEGRITY_FRESHNESS_VALIDATION_CURRENT.json"
    ).is_file()
    assert (vault / "SOURCE_GAP_REGISTER_CURRENT.json").is_file()
    assert (vault / "MODEL_FEATURE_SPACE_CURRENT.md").is_file()
    assert (vault / "OFFICIAL_CURRENCY_SOURCE_DEPTH_CURRENT.json").is_file()
    assert (vault / "NEWS_SOURCES_CURRENT.json").is_file()
    assert (
        vault
        / "OFFICIAL_GEOPOLITICAL_AND_SCHEDULED_EVENT_CAPTURE_VALIDATION_CURRENT.json"
    ).is_file()
    assert (vault / "VAULT_CANONICAL_SNAPSHOT_VALIDATION_CURRENT.json").is_file()
    assert (vault / "SCHEDULED_EVENT_QUOTE_CAPTURE_CONTRACT_CURRENT.json").is_file()
    assert (vault / "LIFECYCLE_VERIFIER_CADENCE_VALIDATION_CURRENT.json").is_file()
    assert (
        vault / "PROJECT_INTEGRITY_HISTORY_ROTATION_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "LIFECYCLE_GENEALOGY_LOCK_RETRY_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault / "LIFECYCLE_SEMANTIC_REPUBLICATION_VALIDATION_CURRENT.json"
    ).is_file()
    assert (
        vault
        / "SIGNAL_TRIAL_BARRIER_SUMMARY_PERFORMANCE_VALIDATION_CURRENT.json"
    ).is_file()
    assert (vault / "MOVE_FIRST_CALENDAR_EPISODE_LABELS_CURRENT.json").is_file()
    assert (vault / "MOVE_FIRST_LIVE_CASE_CAPTURE_CONTRACT_CURRENT.json").is_file()
    assert (vault / "MOVE_FIRST_LIVE_CASE_CAPTURE_STATUS_CURRENT.json").is_file()
    assert (vault / "MOVE_FIRST_LIVE_ARM_ALIGNMENT_CONTRACT_CURRENT.json").is_file()
    assert (vault / "MOVE_FIRST_LIVE_ARM_ALIGNMENT_STATUS_CURRENT.json").is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_CONTRACT_CURRENT.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_STATUS_CURRENT.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V1_BASELINE.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V1_STATUS_BASELINE.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V2_BASELINE.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V2_STATUS_BASELINE.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V3_INVALIDATED_BASELINE.json"
    ).is_file()
    assert (
        vault / "MOVE_FIRST_OPERATIONAL_MAPPING_ALIGNMENT_V3_STATUS_INVALIDATED_BASELINE.json"
    ).is_file()
    manifest = json.loads(
        (vault / "SHARED_PROJECT_STATE_CURRENT.json").read_text(encoding="utf-8")
    )
    assert manifest["record_count"] == 37
    assert manifest["contains_credentials"] is False

    second = vault_sync.sync_canonical_project_records(tmp_path, vault)
    assert second["changed"] is False


def test_canonical_record_manifest_hashes_exact_published_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "trad" / "FOREX_PENDING_IMPROVEMENTS.md"
    _write(tmp_path, "trad/FOREX_PENDING_IMPROVEMENTS.md", "snapshot-v1\n")
    vault = tmp_path / "vault"
    vault.mkdir()
    original_write = vault_sync.write_bytes_atomic
    mutated = False

    def mutate_source_then_publish(destination: Path, payload: bytes) -> None:
        nonlocal mutated
        if not mutated:
            source.write_text("snapshot-v2\n", encoding="utf-8")
            mutated = True
        original_write(destination, payload)

    monkeypatch.setattr(vault_sync, "write_bytes_atomic", mutate_source_then_publish)
    _fixture_records(monkeypatch, tmp_path)
    result = vault_sync.sync_canonical_project_records(tmp_path, vault)
    row = result["records"][0]
    published = vault / "PENDING_IMPROVEMENTS_CURRENT.md"

    assert source.read_text(encoding="utf-8") == "snapshot-v2\n"
    assert published.read_text(encoding="utf-8") == "snapshot-v1\n"
    assert row["size"] == published.stat().st_size
    assert row["sha256"] == vault_sync.sha256_file(published)


def test_current_source_rank_vault_record_tracks_v7_and_preserves_v6_baseline() -> None:
    records = dict(vault_sync.CANONICAL_PROJECT_RECORDS)

    assert records[
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_D_"
            "VALIDATION_20260901.json"
        )
    ] == "EXECUTABLE_MOVE_CENSUS_COHORT_D_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_E_"
            "VALIDATION_20260901.json"
        )
    ] == "EXECUTABLE_MOVE_CENSUS_COHORT_E_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_EXECUTABLE_MOVE_CENSUS_COHORT_G_"
            "VALIDATION_20260902.json"
        )
    ] == "EXECUTABLE_MOVE_CENSUS_COHORT_G_VALIDATION_CURRENT.json"

    assert records[Path("trad/config/source_conditioned_currency_rank_v7.json")] == (
        "SOURCE_CONDITIONED_CURRENCY_RANK_CURRENT.json"
    )
    assert records[Path("trad/config/source_conditioned_currency_rank_v6.json")] == (
        "SOURCE_CONDITIONED_CURRENCY_RANK_V6_BASELINE.json"
    )
    assert records[
        Path("trad/FOREX_BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_20260901.json")
    ] == "BOJ_MARKET_STRUCTURE_OVERLAY_VALIDATION_CURRENT.json"
    assert records[
        Path("trad/FOREX_OUTCOME_PUBLICATION_ATOMICITY_VALIDATION_20260901.json")
    ] == "OUTCOME_PUBLICATION_ATOMICITY_VALIDATION_CURRENT.json"
    assert records[
        Path("trad/FOREX_NEWS_GOVERNANCE_FAST_LANE_VALIDATION_20260901.json")
    ] == "NEWS_GOVERNANCE_FAST_LANE_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_INTEGRITY_RUNTIME_CONTRACT_RELOAD_"
            "VALIDATION_20260902.json"
        )
    ] == "INTEGRITY_RUNTIME_CONTRACT_RELOAD_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_SCHEDULED_FACTOR_AND_INTEGRITY_LIVENESS_"
            "VALIDATION_20260902.json"
        )
    ] == "SCHEDULED_FACTOR_AND_INTEGRITY_LIVENESS_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_LIFECYCLE_VERIFIER_CADENCE_"
            "VALIDATION_20260902.json"
        )
    ] == "LIFECYCLE_VERIFIER_CADENCE_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_PROJECT_INTEGRITY_HISTORY_ROTATION_"
            "VALIDATION_20260902.json"
        )
    ] == "PROJECT_INTEGRITY_HISTORY_ROTATION_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_LIFECYCLE_GENEALOGY_LOCK_RETRY_"
            "VALIDATION_20260902.json"
        )
    ] == "LIFECYCLE_GENEALOGY_LOCK_RETRY_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_LIFECYCLE_SEMANTIC_REPUBLICATION_"
            "VALIDATION_20260902.json"
        )
    ] == "LIFECYCLE_SEMANTIC_REPUBLICATION_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_SIGNAL_TRIAL_BARRIER_SUMMARY_PERFORMANCE_"
            "VALIDATION_20260902.json"
        )
    ] == "SIGNAL_TRIAL_BARRIER_SUMMARY_PERFORMANCE_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_SUPERVISOR_PROCESS_SNAPSHOT_PERFORMANCE_"
            "VALIDATION_20260902.json"
        )
    ] == "SUPERVISOR_PROCESS_SNAPSHOT_PERFORMANCE_VALIDATION_CURRENT.json"
    assert records[
        Path("trad/config/scheduled_event_factor_reaction_v1_20260902b.json")
    ] == "SCHEDULED_EVENT_FACTOR_REACTION_CONTRACT_CURRENT.json"
    assert records[
        Path("trad/FOREX_OPERATIONAL_MAPPING_ALIGNMENT_VALIDATION_20260901.json")
    ] == "OPERATIONAL_MAPPING_ALIGNMENT_V1_VALIDATION_BASELINE.json"
    assert records[
        Path(
            "trad/FOREX_OPERATIONAL_MAPPING_SYNDICATION_DEDUP_"
            "VALIDATION_20260901.json"
        )
    ] == "OPERATIONAL_MAPPING_ALIGNMENT_V2_VALIDATION_BASELINE.json"
    assert records[
        Path("trad/FOREX_NARRATIVE_FAMILY_DECAY_VALIDATION_20260901.json")
    ] == "OPERATIONAL_MAPPING_ALIGNMENT_V3_INVALIDATED_BASELINE.json"
    assert records[
        Path(
            "trad/FOREX_OPERATIONAL_MAPPING_SUBSECOND_CAUSALITY_"
            "VALIDATION_20260902.json"
        )
    ] == "OPERATIONAL_MAPPING_ALIGNMENT_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_AGGREGATE_SIGNAL_LINEAGE_AND_CONFLICT_AUDIT_"
            "VALIDATION_20260901.json"
        )
    ] == "AGGREGATE_SIGNAL_LINEAGE_AND_CONFLICT_AUDIT_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_SECONDARY_CRYPTO_SOURCE_FACTOR_GATE_"
            "VALIDATION_20260901.json"
        )
    ] == "SECONDARY_CRYPTO_SOURCE_FACTOR_GATE_VALIDATION_CURRENT.json"
    assert records[
        Path("trad/FOREX_DIRECT_COMMODITY_ACCESS_AUDIT_20260901.json")
    ] == "DIRECT_COMMODITY_ACCESS_AUDIT_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_DISCOVERY_SOURCE_LINEAGE_"
            "VALIDATION_20260901.json"
        )
    ] == "DISCOVERY_SOURCE_LINEAGE_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_MAJOR_MOVE_CENSUS_LIVENESS_"
            "VALIDATION_20260901.json"
        )
    ] == "MAJOR_MOVE_CENSUS_LIVENESS_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_LIFECYCLE_PUBLICATION_CONSISTENCY_"
            "VALIDATION_20260901.json"
        )
    ] == "LIFECYCLE_PUBLICATION_CONSISTENCY_VALIDATION_CURRENT.json"
    assert records[
        Path(
            "trad/FOREX_SOURCE_READINESS_AND_INTEGRITY_FRESHNESS_"
            "VALIDATION_20260901.json"
        )
    ] == "SOURCE_READINESS_AND_INTEGRITY_FRESHNESS_VALIDATION_CURRENT.json"
    assert Path("trad/oanda_causal_source_factor_response_map_v8.py") in (
        vault_sync.NEWS_EVENT_TAGGING_SOURCE_FILES
    )
    assert Path("trad/oanda_source_conditioned_currency_rank_v7.py") in (
        vault_sync.NEWS_EVENT_TAGGING_SOURCE_FILES
    )


def test_sync_once_rebuilds_pointer_from_current_checkpoint(tmp_path, monkeypatch) -> None:
    _runtime_fixture_record(monkeypatch)
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    _write(root, "README.md", "# Current project\n")
    _write(root, "trad/runtime.py", "print('runtime')\n")
    _write(root, "trad/forex_vault_import.py", "print('import')\n")
    _write(root, "trad/forex_vault_bootstrap.ps1", "Write-Output 'ok'\n")
    _write(
        root,
        "trad/data/oanda_training_manager/model_space/"
        "model_gap_completion_latest.json",
        json.dumps(
            {
                "summary": {
                    "models": 30,
                    "families": 7,
                    "runtime_available": 28,
                    "synthetic_or_better": 28,
                    "bounded_market_qualified": 11,
                    "production_eligible": 0,
                    "account_wired": 0,
                },
                "matrix_coverage": {
                    "observed_timeframe_horizon_cells": 156,
                    "observed_pair_cells": 10608,
                    "coverage_ratio": 1.0,
                    "fully_covered": True,
                    "total_events": 4005572,
                },
            }
        ),
    )
    _write(
        root,
        "trad/data/oanda_training_manager/model_space/"
        "model_gap_runtime_profiles_d_latest.json",
        json.dumps(
            {
                "summary": {"models_runtime_available": 27},
                "explicit_blockers": {
                    "mamba": "requires CUDA",
                    "timesfm_icf": "no public runtime",
                },
            }
        ),
    )
    _write(
        root,
        "trad/data/oanda_training_manager/model_space/"
        "vault_validation_recreation_summary_latest.json",
        json.dumps(
            {
                "status": "passed",
                "mode": "full",
                "evidence": [{}] * 17,
                "compile": {"status": "passed", "files": 30},
                "pytest": {"status": "passed", "passed": 105},
                "full_reruns": [{"status": "passed"}] * 3,
                "account_processes_started": 0,
            }
        ),
    )
    _write(
        root,
        "trad/data/oanda_training_manager/training_sets/"
        "model_gap_full_matrix/full_matrix_latest.json",
        json.dumps({"coverage": {"observed_pair_cells": 10608}}),
    )
    _write(
        root,
        "trad/data/oanda_training_manager/model_space/"
        "vault_clean_import_validation_latest.json",
        json.dumps(
            {
                "status": "passed",
                "archive_sha256": "validated-archive",
                "tests_passed": 106,
            }
        ),
    )
    _write(
        root,
        "trad/data/oanda_training_manager/reports/ma_feature_grid/"
        "ma_feature_grid_latest.json",
        json.dumps(
            {
                "artifact": "ma_feature_grid_latest.joblib",
                "artifact_sha256": "ma-artifact-hash",
                "contract": {
                    "execution_policy": "shadow_only",
                    "feature_count_by_timeframe": {"S5": 643, "D1": 293},
                },
                "family": "moving_average_feature_grid",
                "fitted_cell_count": 610,
                "horizons_sec": list(range(26)),
                "instrument_count": 68,
                "planned_cell_count": 702,
                "real_account_authorized": False,
                "timeframes": [f"T{index}" for index in range(27)],
                "unsupported_cell_count": 92,
            }
        ),
    )

    result = vault_sync.sync_once(root, [vault], retention=2)
    pointer_path = vault / "CHECKPOINT_LATEST.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    archive = vault / pointer["base_checkpoint"]["archive"]

    assert pointer["schema_version"] == 5
    assert pointer["checkpoint_source_root"] == str(root)
    assert pointer["base_checkpoint"]["archive_bytes"] == archive.stat().st_size
    assert (
        pointer["base_checkpoint"]["archive_sha256"]
        == vault_sync.sha256_file(archive)
    )
    assert (
        pointer["base_checkpoint"]["content_sha256"]
        == result["manifest"]["content_sha256"]
    )
    assert pointer["validation"]["source_full_recreation"]["tests_passed"] == 105
    assert pointer["prediction_space"]["pair_expanded_cells"] == 10608
    assert pointer["strict_ma_feature_grid"]["input_timeframes"] == 27
    assert pointer["strict_ma_feature_grid"]["outcome_horizons"] == 26
    assert pointer["strict_ma_feature_grid"]["fitted_cells"] == 610
    assert pointer["strict_ma_feature_grid"]["coverage_ratio"] == 0.868946
    assert pointer["strict_ma_feature_grid"]["real_account_authorized"] is False
    assert set(pointer["model_gap_summary"]["explicit_blockers"]) == {
        "mamba",
        "timesfm_icf",
    }
    assert "lag_llama" not in json.dumps(pointer)
    clean_import = pointer["validation"]["clean_import_and_full_recreation"]
    assert clean_import["status"] == "stale_not_current_archive"
    assert clean_import["matches_current_archive"] is False
    current_validation = pointer["validation"]["current_archive_reconstruction"]
    assert current_validation["status"] == "passed"
    assert current_validation["archive_sha256"] == vault_sync.sha256_file(archive)
    assert current_validation["zip_crc_verified"] is True
    assert current_validation["reconstructability_verified"] is True
    assert (
        json.loads(
            (vault / vault_sync.MODEL_VALIDATION_RECEIPT).read_text(
                encoding="utf-8"
            )
        )
        == current_validation
    )
    assert (
        vault
        / "artifacts"
        / "clean_import"
        / "vault_clean_import_validation_latest.json"
    ).is_file()
    with zipfile.ZipFile(archive) as checkpoint:
        assert not any(
            name.endswith("vault_clean_import_validation_latest.json")
            for name in checkpoint.namelist()
        )


def test_sync_once_builds_one_identical_archive_for_all_destinations(
    tmp_path,
    monkeypatch,
) -> None:
    _runtime_fixture_record(monkeypatch)
    root = tmp_path / "source"
    vault_a = tmp_path / "vault_a"
    vault_b = tmp_path / "vault_b"
    _write(root, "README.md", "# Current project\n")
    _write(root, "trad/runtime.py", "print('runtime')\n")
    original_write_zip = vault_sync.write_zip
    write_calls = 0

    def counted_write_zip(*args, **kwargs):
        nonlocal write_calls
        write_calls += 1
        return original_write_zip(*args, **kwargs)

    monkeypatch.setattr(vault_sync, "write_zip", counted_write_zip)
    result = vault_sync.sync_once(root, [vault_a, vault_b], retention=2)

    archive_a = vault_a / "forex_model_checkpoint_current.zip"
    archive_b = vault_b / "forex_model_checkpoint_current.zip"
    hashes = {
        vault_sync.sha256_file(archive_a),
        vault_sync.sha256_file(archive_b),
    }
    result_hashes = {
        row["checkpoint_pointer"]["archive_sha256"]
        for row in result["destinations"]
    }
    assert write_calls == 1
    assert len(hashes) == 1
    assert result_hashes == hashes


def test_main_defaults_to_onedrive_only_destination(tmp_path, monkeypatch) -> None:
    captured = {}

    class Args:
        root = tmp_path
        interval_sec = 0
        retention = 2
        destination = None
        news_event_only = False
        canonical_only = False

    def fake_sync_once(root, destinations, retention):
        captured.update(
            root=root,
            destinations=destinations,
            retention=retention,
        )
        return {
            "manifest": {
                "content_sha256": "content",
                "file_count": 1,
                "total_bytes": 1,
            },
            "destinations": [],
        }

    monkeypatch.setattr(vault_sync, "parse_args", lambda: Args())
    monkeypatch.setattr(vault_sync, "sync_once", fake_sync_once)

    assert vault_sync.main() == 0
    assert captured["destinations"] == [vault_sync.DEFAULT_VAULT_PROJECT]
    assert all(path.drive.upper() != "D:" for path in captured["destinations"])


def test_main_canonical_only_never_builds_checkpoint_archive(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    _write(root, "trad/FOREX_PENDING_IMPROVEMENTS.md", "nothing open\n")
    source_root = root

    class Args:
        root = source_root
        interval_sec = 0
        retention = 12
        destination = [vault]
        news_event_only = False
        canonical_only = True

    def reject_full_sync(*args, **kwargs):
        raise AssertionError("canonical-only mode must not build a checkpoint")

    monkeypatch.setattr(vault_sync, "parse_args", lambda: Args())
    monkeypatch.setattr(vault_sync, "sync_once", reject_full_sync)
    _fixture_records(monkeypatch, root)

    assert vault_sync.main() == 0
    assert (vault / "PENDING_IMPROVEMENTS_CURRENT.md").is_file()
    assert (vault / "SHARED_PROJECT_STATE_CURRENT.json").is_file()
    assert not list(vault.glob("*.zip"))


def test_archive_validation_failure_precedes_retention_deletion(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    _write(root, "trad/runtime.py", "print('runtime')\n")
    vault.mkdir()
    old_a = _write(vault, "forex_model_checkpoint_20200101_000000.zip", "a")
    old_b = _write(vault, "forex_model_checkpoint_20200102_000000.zip", "b")

    def reject_archive(*args, **kwargs):
        raise RuntimeError("synthetic archive validation failure")

    monkeypatch.setattr(vault_sync, "validate_checkpoint_archive", reject_archive)
    try:
        vault_sync.sync_once(root, [vault], retention=1)
    except RuntimeError as exc:
        assert "synthetic archive validation failure" in str(exc)
    else:
        raise AssertionError("sync must fail closed on archive validation failure")

    assert old_a.is_file()
    assert old_b.is_file()
    assert not list(vault.glob(f"{vault_sync.MODEL_RETENTION_RECEIPT_PREFIX}*.json"))


def test_retention_writes_hash_bound_tombstone_before_exact_delete(tmp_path, monkeypatch) -> None:
    _runtime_fixture_record(monkeypatch)
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    _write(root, "trad/runtime.py", "print('runtime')\n")
    vault.mkdir()
    old = _write(
        vault,
        "forex_model_checkpoint_20200101_000000.zip",
        "recoverable old archive bytes",
    )
    old_sha256 = vault_sync.sha256_file(old)
    old_bytes = old.stat().st_size

    result = vault_sync.sync_once(root, [vault], retention=1)

    assert not old.exists()
    tombstones = list(
        vault.glob(f"{vault_sync.MODEL_RETENTION_RECEIPT_PREFIX}*.json")
    )
    assert len(tombstones) == 1
    tombstone = json.loads(tombstones[0].read_text(encoding="utf-8"))
    assert tombstone["status"] == "deleted"
    assert tombstone["deletion_targets"] == tombstone["deleted"]
    assert tombstone["deleted"] == [
        {
            "name": old.name,
            "bytes": old_bytes,
            "sha256": old_sha256,
        }
    ]
    current_receipt = json.loads(
        (vault / vault_sync.MODEL_VALIDATION_RECEIPT).read_text(encoding="utf-8")
    )
    assert (
        tombstone["current_validation_receipt"]["archive_sha256"]
        == current_receipt["archive_sha256"]
    )
    assert result["destinations"][0]["retention"]["status"] == "deleted"


def test_unchanged_sync_reuses_bound_reconstruction_and_never_prunes(tmp_path, monkeypatch) -> None:
    _runtime_fixture_record(monkeypatch)
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    _write(root, "trad/runtime.py", "print('runtime')\n")
    vault.mkdir()

    first = vault_sync.sync_once(root, [vault], retention=2)
    extra = _write(
        vault,
        "forex_model_checkpoint_20200101_000000.zip",
        "older archive added after the first sync",
    )
    second = vault_sync.sync_once(root, [vault], retention=1)

    assert first["destinations"][0]["changed"] is True
    assert second["destinations"][0]["changed"] is False
    assert extra.is_file()
    assert (
        second["destinations"][0]["retention"]["status"]
        == "not_evaluated_unchanged"
    )
    receipt = json.loads(
        (vault / vault_sync.MODEL_VALIDATION_RECEIPT).read_text(encoding="utf-8")
    )
    assert receipt["reconstructability_verified"] is True
    assert receipt["reused_reconstruction_receipt_sha256"]


@pytest.mark.parametrize("failure", ["missing", "pattern_secret", "known_private", "redirect"])
def test_canonical_preflight_failure_preserves_all_old_records_and_manifest(tmp_path, monkeypatch, failure) -> None:
    from tools import vault_worktree_snapshot as safety
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    vault.mkdir()
    _write(root, "trad/first.md", "new first\n")
    second = _write(root, "trad/second.md", "new second\n")
    monkeypatch.setattr(vault_sync, "CANONICAL_PROJECT_RECORDS", (
        (Path("trad/first.md"), "FIRST_CURRENT.md"),
        (Path("trad/second.md"), "SECOND_CURRENT.md"),
    ))
    old = {name: _write(vault, name, "old bytes").read_bytes() for name in (
        "FIRST_CURRENT.md", "SECOND_CURRENT.md", "SHARED_PROJECT_STATE_CURRENT.json")}
    secret_value = ""
    if failure == "missing":
        second.unlink()
    elif failure == "pattern_secret":
        secret_value = "sk-" + "Xy45Ab67" * 4
        second.write_text(secret_value)
    elif failure == "known_private":
        secret_value = "1234567890abcdef" * 2 + "-" + "abcdef1234567890" * 2
        _write(root, "trad/creds", secret_value)
        second.write_text("copied value " + secret_value)
    else:
        original = safety.regular_file
        def redirected(directory, name):
            if name == "trad/second.md":
                raise RuntimeError("symlink/reparse point is prohibited")
            return original(directory, name)
        monkeypatch.setattr(safety, "regular_file", redirected)
    with pytest.raises(RuntimeError) as error:
        vault_sync.sync_canonical_project_records(root, vault)
    assert not secret_value or secret_value not in str(error.value)
    assert all((vault / name).read_bytes() == payload for name, payload in old.items())


def test_canonical_preflight_rejects_redirected_manifest_before_writes(tmp_path, monkeypatch) -> None:
    from tools import vault_worktree_snapshot as safety
    root = tmp_path / "source"
    vault = tmp_path / "vault"
    _write(root, "trad/first.md", "new first\n")
    _write(vault, "FIRST_CURRENT.md", "old first\n")
    _write(vault, "SHARED_PROJECT_STATE_CURRENT.json", "old manifest\n")
    monkeypatch.setattr(vault_sync, "CANONICAL_PROJECT_RECORDS", (
        (Path("trad/first.md"), "FIRST_CURRENT.md"),
    ))
    original = safety.regular_file
    def redirected(directory, name):
        if name == "SHARED_PROJECT_STATE_CURRENT.json":
            raise RuntimeError("symlink/reparse point is prohibited")
        return original(directory, name)
    monkeypatch.setattr(safety, "regular_file", redirected)
    with pytest.raises(RuntimeError, match="symlink"):
        vault_sync.sync_canonical_project_records(root, vault)
    assert (vault / "FIRST_CURRENT.md").read_text() == "old first\n"
    assert (vault / "SHARED_PROJECT_STATE_CURRENT.json").read_text() == "old manifest\n"
