#!/usr/bin/env python3
"""Resumable all-68 M1 expansion and atomic feature refresh worker."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sys
import time

import all68_weekly_missed_move_study as study
import oanda_gpt_training_strategy_manager as manager


def atomic_feature_refresh() -> dict:
    root = manager.PROJECT_ROOT / "data" / "all68_weekly_move_study"
    live = root / "features"
    staging = root / f"features_staging_{int(time.time())}"
    backup = root / f"features_backup_{int(time.time())}"
    staging.mkdir(parents=True, exist_ok=False)
    original_feature_root = study.FEATURE_ROOT
    try:
        study.FEATURE_ROOT = staging
        paths, quality = study.build_all_features()
    finally:
        study.FEATURE_ROOT = original_feature_root
    if len(paths) != 68 or len(list(staging.glob("*.parquet"))) != 68:
        raise RuntimeError(
            f"staging feature build incomplete: paths={len(paths)} "
            f"files={len(list(staging.glob('*.parquet')))}"
        )
    resolved_root = root.resolve()
    if staging.resolve().parent != resolved_root or live.resolve().parent != resolved_root:
        raise RuntimeError("feature swap paths escaped the all68 workspace")
    if live.exists():
        os.replace(live, backup)
    try:
        os.replace(staging, live)
    except Exception:
        if backup.exists() and not live.exists():
            os.replace(backup, live)
        raise
    if backup.exists():
        shutil.rmtree(backup)
    return {
        "feature_files": len(paths),
        "quality_rows": len(quality),
        "feature_root": str(live),
    }


def main() -> int:
    manager.ensure_dirs()
    manager.HISTORY_EXPANSION_LOCK.write_text(
        json.dumps({"pid": os.getpid(), "started_utc": manager.iso_utc()}),
        encoding="utf-8",
    )
    status_path = (
        manager.DIRS["reports"] / "historical_expansion_worker_status.json"
    )
    status = {
        "started_utc": manager.iso_utc(),
        "pid": os.getpid(),
        "target_days": manager.HISTORICAL_EXPANSION_TARGET_DAYS,
        "stage": "starting",
    }
    manager.save_json(status_path, status)
    try:
        training_manager = manager.TrainingStrategyManager()
        status["stage"] = "backfilling_m1"
        manager.save_json(status_path, status)
        backfill = training_manager.deep_backfill_m1(
            days=manager.HISTORICAL_EXPANSION_TARGET_DAYS,
            instruments=training_manager.instruments[: manager.MAX_INSTRUMENTS_SCAN],
        )
        status["backfill"] = backfill
        status["stage"] = "building_features"
        manager.save_json(status_path, status)
        feature_refresh = atomic_feature_refresh()
        status["feature_refresh"] = feature_refresh
        status["stage"] = "complete"
        status["completed_utc"] = manager.iso_utc()
        status["research_dataset_rebuild_pending"] = True
        manager.save_json(status_path, status)
        print(json.dumps(status, indent=2, default=str), flush=True)
        return 0
    except Exception as exc:
        status["stage"] = "error"
        status["error"] = f"{type(exc).__name__}: {exc}"
        status["failed_utc"] = manager.iso_utc()
        manager.save_json(status_path, status)
        raise
    finally:
        try:
            manager.HISTORY_EXPANSION_LOCK.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
