#!/usr/bin/env python3
"""Prospective proof collector for the frozen executable-opportunity model.

The worker is research-only.  It writes immutable forecasts only while the
quote stream is fresh and before the declared outcome exists, then matures the
same forecasts at the exact frozen horizon.  It has no broker or authorization
client and cannot place orders.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping

import joblib
import numpy as np

import oanda_68_pair_opportunity_census as source
import oanda_executable_opportunity_feature_contract as features
import oanda_executable_opportunity_ranking as ranking


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
ARTIFACT = ROOT / "data" / "oanda_training_manager" / "model_space" / "proof_cohorts" / "executable_opportunity_ranking_v2"
CONFIG = ROOT / "config" / "executable_opportunity_ranking_v2.json"
SOURCE_DB = STATE / "practice_007_quote_intensity_shadow_v1.sqlite"
QUOTES = STATE / "practice_007_market_quotes_v1.json"
CLOCK_INTEGRITY = STATE / "clock_integrity_v1.json"
LEDGER = STATE / "executable_opportunity_prospective_v1.sqlite"
OUTPUT = STATE / "executable_opportunity_prospective_v1.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "executable_opportunity_ranking" / "EXECUTABLE_OPPORTUNITY_PROSPECTIVE_CURRENT.md"
OBSERVATION_TIME_CONTRACT_ID = (
    "executable_opportunity_clock_integrity_v3_no_cached_executor_offset_20260817"
)
PENDING_QUEUE_CONTRACT_ID = "executable_opportunity_pending_outcome_queue_v1_20260828"
SUMMARY_CACHE_CONTRACT_ID = "executable_opportunity_exact_summary_cache_v1_20260828"
_SOURCE_INSTRUMENT_COUNT_CACHE: dict[str, int] = {}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_utc(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


def collector_observation_time(
    local_now: dt.datetime,
    *,
    integrity_path: Path = CLOCK_INTEGRITY,
    maximum_state_age_sec: float = 90.0,
) -> tuple[dt.datetime, dict[str, Any]]:
    """Return a causally trusted collection time from a fresh clock audit.

    The prior collector preferred a cached offset in the long-running top
    executor heartbeat.  After Windows Time is repaired that cached value can
    remain materially non-zero even though both the fresh quote-stream clock
    audit and OANDA HTTPS Date cross-check show the host aligned.  Applying it
    would backdate new forecasts.  This contract therefore trusts only the
    independently refreshed clock-integrity state and fails closed when that
    state is unavailable or stale.
    """

    local = (
        local_now.replace(tzinfo=dt.timezone.utc)
        if local_now.tzinfo is None
        else local_now.astimezone(dt.timezone.utc)
    )
    state = read_json(integrity_path)
    generated = _parse_utc(state.get("generated_utc"))
    state_age_sec = (
        max(0.0, (local - generated).total_seconds())
        if generated is not None
        else math.inf
    )
    state_fresh = bool(
        generated is not None
        and state_age_sec <= float(maximum_state_age_sec)
        and str(state.get("status") or "") in {"ok", "mitigated"}
    )
    timestamp_trusted = state.get("timestamp_normalization_trusted") is True
    host_synchronized = bool(
        state_fresh
        and timestamp_trusted
        and state.get("host_clock_synchronized") is True
    )

    external = state.get("external_https_clock")
    external = external if isinstance(external, Mapping) else {}
    try:
        external_offset = float(external.get("offset_sec"))
    except (TypeError, ValueError):
        external_offset = math.nan
    try:
        external_round_trip_ms = float(external.get("round_trip_ms"))
    except (TypeError, ValueError):
        external_round_trip_ms = math.inf
    try:
        external_precision_sec = float(external.get("precision_sec"))
    except (TypeError, ValueError):
        external_precision_sec = math.inf
    external_correction_usable = bool(
        state_fresh
        and external.get("status") == "ok"
        and math.isfinite(external_offset)
        and 2.0 < abs(external_offset) <= 300.0
        and external_round_trip_ms <= 2_000.0
        and external_precision_sec <= 1.0
    )

    try:
        broker_offset = float(state.get("broker_clock_lead_sec"))
    except (TypeError, ValueError):
        broker_offset = math.nan
    try:
        broker_samples = int(state.get("broker_clock_sample_count") or 0)
    except (TypeError, ValueError):
        broker_samples = 0
    broker_correction_usable = bool(
        state_fresh
        and state.get("source_fresh") is True
        and broker_samples >= 32
        and math.isfinite(broker_offset)
        and 2.0 < abs(broker_offset) <= 300.0
    )

    if host_synchronized:
        source_name = "clock_integrity_synchronized_host"
        applied_offset = 0.0
        status = "aligned"
        trusted = True
    elif external_correction_usable:
        source_name = "clock_integrity_oanda_https_date"
        applied_offset = external_offset
        status = "external_offset_applied"
        trusted = True
    elif broker_correction_usable:
        source_name = "clock_integrity_fresh_quote_stream"
        applied_offset = broker_offset
        status = "broker_offset_applied"
        trusted = True
    else:
        source_name = "unavailable"
        applied_offset = 0.0
        status = "clock_integrity_untrusted"
        trusted = False

    corrected = local + dt.timedelta(seconds=applied_offset)
    return corrected, {
        "contract_id": OBSERVATION_TIME_CONTRACT_ID,
        "source": source_name,
        "local_raw_utc": local.isoformat(),
        "normalized_utc": corrected.isoformat(),
        "status": status,
        "integrity_generated_utc": (
            generated.isoformat() if generated is not None else None
        ),
        "integrity_age_sec": (
            round(state_age_sec, 3) if math.isfinite(state_age_sec) else None
        ),
        "integrity_state_fresh": state_fresh,
        "host_clock_synchronized": host_synchronized,
        "broker_clock_lead_sec": (
            round(broker_offset, 3) if math.isfinite(broker_offset) else None
        ),
        "broker_clock_sample_count": broker_samples,
        "external_clock_offset_sec": (
            round(external_offset, 3)
            if math.isfinite(external_offset)
            else None
        ),
        "applied_offset_sec": round(applied_offset, 3),
        "cached_executor_offset_permitted": False,
        "trusted_for_prospective_evidence": trusted,
        "normalized": bool(applied_offset),
    }


def atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temporary.write_text(value, encoding="utf-8")
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(0.5, 0.01 * (2**attempt)))
    finally:
        temporary.unlink(missing_ok=True)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
          forecast_id TEXT PRIMARY KEY,
          cohort_id TEXT NOT NULL,
          issued_at_utc TEXT NOT NULL,
          entry_epoch INTEGER NOT NULL,
          knowledge_time_utc TEXT NOT NULL,
          instrument TEXT NOT NULL,
          horizon_sec INTEGER NOT NULL,
          entry_mid REAL NOT NULL,
          entry_spread_pips REAL NOT NULL,
          modeled_entry_cost_pips REAL NOT NULL,
          predicted_clear_probability REAL NOT NULL,
          predicted_up_probability REAL NOT NULL,
          predicted_direction INTEGER NOT NULL,
          predicted_direction_confidence REAL NOT NULL,
          predicted_magnitude_pips REAL NOT NULL,
          predicted_ev_pips REAL NOT NULL,
          passed_frozen_gate INTEGER NOT NULL,
          feature_json TEXT NOT NULL,
          model_artifact_sha256 TEXT NOT NULL,
          collector_source_sha256 TEXT NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0),
          UNIQUE(cohort_id,entry_epoch,instrument,horizon_sec)
        );
        CREATE TABLE IF NOT EXISTS outcomes (
          forecast_id TEXT PRIMARY KEY REFERENCES forecasts(forecast_id),
          matured_at_utc TEXT NOT NULL,
          outcome_knowledge_time_utc TEXT NOT NULL,
          exit_mid REAL NOT NULL,
          exit_spread_pips REAL NOT NULL,
          signed_move_pips REAL NOT NULL,
          absolute_move_pips REAL NOT NULL,
          modeled_cost_pips REAL NOT NULL,
          movement_cleared_cost INTEGER NOT NULL,
          predicted_side_net_pips REAL NOT NULL,
          direction_correct INTEGER NOT NULL,
          research_only INTEGER NOT NULL CHECK(research_only=1),
          execution_eligible INTEGER NOT NULL CHECK(execution_eligible=0)
        );
        CREATE TABLE IF NOT EXISTS cycles (
          generated_utc TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pending_outcome_queue_v1 (
          forecast_id TEXT PRIMARY KEY REFERENCES forecasts(forecast_id),
          exit_epoch INTEGER NOT NULL,
          instrument TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS pending_outcome_queue_v1_exit_epoch
          ON pending_outcome_queue_v1(exit_epoch,forecast_id);
        CREATE TABLE IF NOT EXISTS prospective_runtime_metadata_v1 (
          metadata_key TEXT PRIMARY KEY,
          metadata_value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prospective_exact_summary_cache_v1 (
          cache_key TEXT PRIMARY KEY,
          payload_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS forecasts_no_update BEFORE UPDATE ON forecasts
          BEGIN SELECT RAISE(ABORT,'immutable forecasts'); END;
        CREATE TRIGGER IF NOT EXISTS forecasts_no_delete BEFORE DELETE ON forecasts
          BEGIN SELECT RAISE(ABORT,'immutable forecasts'); END;
        CREATE TRIGGER IF NOT EXISTS outcomes_no_update BEFORE UPDATE ON outcomes
          BEGIN SELECT RAISE(ABORT,'immutable outcomes'); END;
        CREATE TRIGGER IF NOT EXISTS outcomes_no_delete BEFORE DELETE ON outcomes
          BEGIN SELECT RAISE(ABORT,'immutable outcomes'); END;
        CREATE TRIGGER IF NOT EXISTS forecasts_pending_outcome_queue_v1
          AFTER INSERT ON forecasts
          BEGIN
            INSERT OR IGNORE INTO pending_outcome_queue_v1(
              forecast_id,exit_epoch,instrument
            ) VALUES(
              NEW.forecast_id,NEW.entry_epoch+NEW.horizon_sec,NEW.instrument
            );
            DELETE FROM prospective_exact_summary_cache_v1;
          END;
        CREATE TRIGGER IF NOT EXISTS outcomes_pending_outcome_queue_v1
          AFTER INSERT ON outcomes
          BEGIN
            DELETE FROM pending_outcome_queue_v1
              WHERE forecast_id=NEW.forecast_id;
            DELETE FROM prospective_exact_summary_cache_v1;
          END;
        """
    )
    queue_initialized = db.execute(
        "SELECT metadata_value FROM prospective_runtime_metadata_v1 "
        "WHERE metadata_key=?",
        (PENDING_QUEUE_CONTRACT_ID,),
    ).fetchone()
    if queue_initialized is None:
        # One bounded migration reconstructs the derived queue from immutable
        # evidence.  Forecast/outcome triggers then keep it exact without a
        # full anti-join on every 30-second mature-only cycle.
        db.execute(
            """
            INSERT OR IGNORE INTO pending_outcome_queue_v1(
              forecast_id,exit_epoch,instrument
            )
            SELECT f.forecast_id,f.entry_epoch+f.horizon_sec,f.instrument
            FROM forecasts f
            LEFT JOIN outcomes o ON o.forecast_id=f.forecast_id
            WHERE o.forecast_id IS NULL
            """
        )
        db.execute(
            "INSERT INTO prospective_runtime_metadata_v1 VALUES (?,?)",
            (PENDING_QUEUE_CONTRACT_ID, "initialized"),
        )
    return db


def verify_artifact(manifest: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    checks = (
        (CONFIG, str(manifest.get("config_sha256") or ""), "config"),
        (ROOT / "oanda_executable_opportunity_ranking.py", str(manifest.get("source_code_sha256") or ""), "frozen source"),
    )
    for path, expected, label in checks:
        if not path.exists() or not expected or digest(path) != expected:
            failures.append(f"{label} hash mismatch")
    if not (ARTIFACT / "models.joblib").exists():
        failures.append("model artifact missing")
    if not bool(manifest.get("research_only")) or bool(manifest.get("execution_eligible")):
        failures.append("artifact is not fail-closed")
    return failures


def prospective_contract(
    manifest: Mapping[str, Any], *, collector_sha256: str | None = None
) -> dict[str, Any]:
    """Bind causal collection code separately from the frozen model artifact."""
    model_cohort_id = str(manifest.get("cohort_id") or "")
    collector_sha = collector_sha256 or digest(Path(__file__))
    definition_sha = hashlib.sha256(
        f"{model_cohort_id}|{collector_sha}".encode("utf-8")
    ).hexdigest()
    return {
        "cohort_id": f"{model_cohort_id}.collector.{definition_sha[:16]}",
        "model_cohort_id": model_cohort_id,
        "supersedes_cohort_id": model_cohort_id,
        "collector_sha256": collector_sha,
        "cohort_definition_sha256": definition_sha,
        "material_collector_change_requires_new_cohort": True,
        "research_only": True,
        "execution_eligible": False,
    }


def source_clock(path: Path) -> dict[str, Any]:
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    highwater_row = db.execute(
        "SELECT MAX(minute_epoch) FROM quote_intensity_minutes_v1"
    ).fetchone()
    highwater = int(highwater_row[0] or 0)
    # minute_epoch is the leading primary-key column.  Restricting the live
    # clock to the latest two minute buckets preserves the current high-water
    # semantics while avoiding a complete retained-history scan.  Coverage is
    # measured separately over a bounded hour and the fixed-universe count is
    # established once per process.
    row = db.execute(
        "SELECT MAX(last_event_epoch),MAX(last_broker_time) "
        "FROM quote_intensity_minutes_v1 "
        "WHERE minute_epoch BETWEEN ? AND ?",
        (max(0, highwater - 60), highwater),
    ).fetchone()
    recent_instrument_count = int(
        db.execute(
            "SELECT COUNT(DISTINCT instrument) "
            "FROM quote_intensity_minutes_v1 "
            "WHERE minute_epoch BETWEEN ? AND ?",
            (max(0, highwater - 3600), highwater),
        ).fetchone()[0]
        or 0
    )
    cache_key = str(path.resolve())
    if cache_key not in _SOURCE_INSTRUMENT_COUNT_CACHE:
        # The fixed source universe is historical metadata.  Establish it once
        # per process, then keep it exact when a newly seen instrument appears
        # in the bounded live window; never repeat the retained-history scan.
        _SOURCE_INSTRUMENT_COUNT_CACHE[cache_key] = int(
            db.execute(
                "SELECT COUNT(DISTINCT instrument) "
                "FROM quote_intensity_minutes_v1"
            ).fetchone()[0]
            or 0
        )
    instrument_count = max(
        int(_SOURCE_INSTRUMENT_COUNT_CACHE[cache_key]), recent_instrument_count
    )
    _SOURCE_INSTRUMENT_COUNT_CACHE[cache_key] = instrument_count
    db.close()
    last_event = float(row[0] or 0.0)
    return {
        "source_highwater_epoch": highwater,
        "last_event_epoch": last_event,
        "last_broker_time": row[1],
        "instrument_count": instrument_count,
        "recent_source_instrument_count": recent_instrument_count,
        "latest_completed_epoch": int(math.floor(last_event / 60.0) * 60 - 60) if last_event else 0,
        "clock_scan_contract": "bounded_latest_two_minute_buckets_v1",
        "instrument_coverage_lookback_sec": 3600,
        "instrument_count_full_history_scan_per_cycle": False,
    }


def candidate_epoch(completed_epoch: int, horizon: int) -> int:
    return completed_epoch - completed_epoch % horizon


def gate(row: Mapping[str, float], config: Mapping[str, Any]) -> bool:
    return bool(
        float(row["predicted_clear_probability"]) >= float(config["minimum_clear_probability"])
        and float(row["predicted_direction_confidence"]) >= float(config["minimum_direction_confidence"])
        and float(row["predicted_magnitude_pips"])
        >= float(config["minimum_predicted_magnitude_cost_ratio"]) * float(row["entry_cost_pips"])
        and float(row["predicted_ev_pips"]) > 0
    )


def exact_source_times(path: Path, epochs: set[int]) -> dict[tuple[int, str], tuple[str, float]]:
    if not epochs:
        return {}
    placeholders = ",".join("?" for _ in epochs)
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    rows = db.execute(
        f"SELECT minute_epoch,instrument,last_broker_time,last_event_epoch FROM quote_intensity_minutes_v1 WHERE minute_epoch IN ({placeholders})",
        tuple(sorted(epochs)),
    ).fetchall()
    db.close()
    return {(int(epoch), str(instrument)): (str(broker), float(event)) for epoch, instrument, broker, event in rows}


def required_outcome_epochs(db: sqlite3.Connection, completed_epoch: int) -> set[int]:
    """Return only exact source minutes needed by currently due outcomes."""
    return {
        int(row[0])
        for row in db.execute(
            "SELECT DISTINCT exit_epoch FROM pending_outcome_queue_v1 "
            "WHERE exit_epoch<=?",
            (int(completed_epoch),),
        )
    }


def load_required_minutes(
    path: Path,
    *,
    feature_epochs: set[int],
    outcome_epochs: set[int],
) -> tuple[dict[str, dict[int, dict[str, float]]], list[str], int]:
    """Load the bounded causal feature window plus exact due outcome minutes.

    The previous collector loaded the complete retained minute archive during
    every 30-second cycle.  That was equivalent statistically but could take
    long enough under memory pressure for supervision to restart the worker,
    creating avoidable prospective evidence gaps.  Live features need only
    the declared 30-minute lookback, while outcome maturation needs only the
    exact target minutes already named by immutable forecasts.
    """
    by_pair: dict[str, dict[int, dict[str, float]]] = {}
    instruments: set[str] = set()
    loaded_rows = 0
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)

    def ingest(rows: Any) -> None:
        nonlocal loaded_rows
        for epoch, instrument, mid, spread, updates, imb5, imb30, imb120 in rows:
            name = str(instrument)
            timeline = by_pair.setdefault(name, {})
            key = int(epoch)
            if key not in timeline:
                loaded_rows += 1
            timeline[key] = {
                "mid": float(mid),
                "spread": float(spread),
                "updates": float(updates),
                "imbalance_5s": float(imb5),
                "imbalance_30s": float(imb30),
                "imbalance_120s": float(imb120),
            }
            instruments.add(name)

    columns = (
        "minute_epoch,instrument,last_mid,average_spread_pips,updates,"
        "imbalance_5s,imbalance_30s,imbalance_120s"
    )
    if feature_epochs:
        start = min(feature_epochs) - 1800
        end = max(feature_epochs)
        ingest(
            db.execute(
                f"SELECT {columns} FROM quote_intensity_minutes_v1 "
                "WHERE minute_epoch BETWEEN ? AND ? ORDER BY minute_epoch,instrument",
                (start, end),
            )
        )
    remaining = sorted(outcome_epochs - feature_epochs)
    for offset in range(0, len(remaining), 500):
        chunk = remaining[offset : offset + 500]
        placeholders = ",".join("?" for _ in chunk)
        ingest(
            db.execute(
                f"SELECT {columns} FROM quote_intensity_minutes_v1 "
                f"WHERE minute_epoch IN ({placeholders}) ORDER BY minute_epoch,instrument",
                chunk,
            )
        )
    db.close()
    return by_pair, sorted(instruments), loaded_rows


def issue_forecasts(
    db: sqlite3.Connection,
    by_pair: Mapping[str, Mapping[int, Mapping[str, float]]],
    models: Mapping[str, Any],
    manifest: Mapping[str, Any],
    config: Mapping[str, Any],
    clock: Mapping[str, Any],
    now: dt.datetime,
    maximum_issue_lag_sec: float,
    source_db: Path = SOURCE_DB,
) -> tuple[int, int, list[str]]:
    new_rows = 0
    gate_rows = 0
    skipped: list[str] = []
    completed = int(clock["latest_completed_epoch"])
    prospective_start = int(manifest["prospective_start_epoch"])
    model_hash = digest(ARTIFACT / "models.joblib")
    collector_hash = digest(Path(__file__))
    candidate_epochs = {
        int(horizon): candidate_epoch(completed, int(horizon))
        for horizon in config.get("horizons_sec") or []
    }
    times = exact_source_times(source_db, set(candidate_epochs.values()))
    for horizon, epoch in candidate_epochs.items():
        decision_available = epoch + 60
        if epoch < prospective_start:
            skipped.append(f"{horizon}s_before_prospective_start")
            continue
        if now.timestamp() - decision_available > maximum_issue_lag_sec:
            skipped.append(f"{horizon}s_late_backfill_refused")
            continue
        # A forecast can never be written after its outcome row could exist.
        if completed >= epoch + horizon:
            skipped.append(f"{horizon}s_outcome_already_available")
            continue
        frame = features.feature_frame(
            by_pair, [epoch], float(config["modeled_slippage_pips"]), float(config["maximum_entry_spread_pips"])
        )
        if frame.empty:
            skipped.append(f"{horizon}s_no_complete_feature_rows")
            continue
        bundle = models[str(horizon)]
        x = frame[bundle["features"]]
        frame["predicted_clear_probability"] = ranking.probability(bundle["clear_model"], bundle["clear_calibrator"], x)
        frame["predicted_up_probability"] = ranking.probability(bundle["direction_model"], bundle["direction_calibrator"], x)
        frame["predicted_direction_confidence"] = (2 * frame["predicted_up_probability"] - 1).abs()
        frame["predicted_magnitude_pips"] = np.maximum(0, bundle["magnitude_model"].predict(x))
        frame["predicted_ev_pips"] = (
            frame["predicted_clear_probability"]
            * frame["predicted_magnitude_pips"]
            * frame["predicted_direction_confidence"]
            - frame["entry_cost_pips"]
        )
        for record in frame.to_dict("records"):
            knowledge = times.get((epoch, str(record["instrument"])))
            if knowledge is None or knowledge[1] >= now.timestamp() + 1:
                continue
            passed = gate(record, config)
            forecast_key = f"{manifest['cohort_id']}|{epoch}|{record['instrument']}|{horizon}"
            forecast_id = hashlib.sha256(forecast_key.encode("utf-8")).hexdigest()
            feature_json = json.dumps({key: float(record[key]) for key in ranking.FEATURES}, sort_keys=True, separators=(",", ":"))
            cursor = db.execute(
                "INSERT OR IGNORE INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    forecast_id, manifest["cohort_id"], now.isoformat(), epoch, knowledge[0], str(record["instrument"]), horizon,
                    float(record["entry_mid"]), float(record["spread_pips"]), float(record["entry_cost_pips"]),
                    float(record["predicted_clear_probability"]), float(record["predicted_up_probability"]),
                    1 if float(record["predicted_up_probability"]) >= 0.5 else -1,
                    float(record["predicted_direction_confidence"]), float(record["predicted_magnitude_pips"]),
                    float(record["predicted_ev_pips"]), int(passed), feature_json, model_hash, collector_hash, 1, 0,
                ),
            )
            if cursor.rowcount:
                new_rows += 1
                gate_rows += int(passed)
    return new_rows, gate_rows, skipped


def mature_outcomes(
    db: sqlite3.Connection,
    by_pair: Mapping[str, Mapping[int, Mapping[str, float]]],
    clock: Mapping[str, Any],
    slippage: float,
    now: dt.datetime,
    source_db: Path = SOURCE_DB,
) -> int:
    completed = int(clock["latest_completed_epoch"])
    pending = db.execute(
        "SELECT f.forecast_id,f.entry_epoch,f.instrument,f.horizon_sec,f.entry_mid,f.entry_spread_pips,f.predicted_direction "
        "FROM pending_outcome_queue_v1 q "
        "JOIN forecasts f ON f.forecast_id=q.forecast_id "
        "WHERE q.exit_epoch<=? ORDER BY q.exit_epoch,q.forecast_id",
        (completed,),
    ).fetchall()
    due_epochs = {int(row[1]) + int(row[3]) for row in pending}
    times = exact_source_times(source_db, due_epochs)
    matured = 0
    for forecast_id, entry_epoch, instrument, horizon, entry_mid, entry_spread, side in pending:
        exit_epoch = int(entry_epoch) + int(horizon)
        future = by_pair.get(str(instrument), {}).get(exit_epoch)
        knowledge = times.get((exit_epoch, str(instrument)))
        if future is None or knowledge is None:
            continue
        start = {"mid": float(entry_mid), "spread": float(entry_spread)}
        window = source.modeled_window(str(instrument), start, future, slippage)
        move = float(window["signed_move_pips"])
        cost = float(window["modeled_cost_pips"])
        cursor = db.execute(
            "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                forecast_id, now.isoformat(), knowledge[0], float(future["mid"]), float(future["spread"]),
                move, abs(move), cost, int(bool(window["movement_cleared_cost"])), float(side) * move - cost,
                int((move > 0 and int(side) > 0) or (move < 0 and int(side) < 0)), 1, 0,
            ),
        )
        matured += int(bool(cursor.rowcount))
    return matured


def pending_forecast_count(db: sqlite3.Connection) -> int:
    """Count immutable forecasts that still require a causal outcome row."""
    return int(
        db.execute(
            "SELECT COUNT(*) FROM pending_outcome_queue_v1"
        ).fetchone()[0]
        or 0
    )


def summary(
    db: sqlite3.Connection,
    config: Mapping[str, Any],
    *,
    cohort_id: str | None = None,
) -> dict[str, Any]:
    """Summarize one immutable collector cohort unless explicitly unscoped."""

    if cohort_id is None:
        forecast = db.execute(
            "SELECT COUNT(*),SUM(passed_frozen_gate),COUNT(DISTINCT entry_epoch),"
            "MIN(issued_at_utc),MAX(issued_at_utc) FROM forecasts"
        ).fetchone()
        outcome = db.execute(
            "SELECT COUNT(*),AVG(predicted_side_net_pips),AVG(direction_correct),"
            "AVG(movement_cleared_cost) FROM outcomes"
        ).fetchone()
        magnitude_gate = db.execute(
            """SELECT COUNT(*) FROM forecasts
                WHERE predicted_clear_probability>=?
                  AND predicted_magnitude_pips>=?*modeled_entry_cost_pips""",
            (
                float(config["minimum_clear_probability"]),
                float(config["minimum_predicted_magnitude_cost_ratio"]),
            ),
        ).fetchone()
    else:
        forecast = db.execute(
            "SELECT COUNT(*),SUM(passed_frozen_gate),COUNT(DISTINCT entry_epoch),"
            "MIN(issued_at_utc),MAX(issued_at_utc) FROM forecasts WHERE cohort_id=?",
            (cohort_id,),
        ).fetchone()
        outcome = db.execute(
            "SELECT COUNT(*),AVG(o.predicted_side_net_pips),"
            "AVG(o.direction_correct),AVG(o.movement_cleared_cost) "
            "FROM outcomes o JOIN forecasts f ON f.forecast_id=o.forecast_id "
            "WHERE f.cohort_id=?",
            (cohort_id,),
        ).fetchone()
        magnitude_gate = db.execute(
            """SELECT COUNT(*) FROM forecasts
                WHERE cohort_id=?
                  AND predicted_clear_probability>=?
                  AND predicted_magnitude_pips>=?*modeled_entry_cost_pips""",
            (
                cohort_id,
                float(config["minimum_clear_probability"]),
                float(config["minimum_predicted_magnitude_cost_ratio"]),
            ),
        ).fetchone()
    return {
        "forecasts": int(forecast[0] or 0), "gate_passes": int(forecast[1] or 0),
        "decision_epochs": int(forecast[2] or 0), "first_issued_utc": forecast[3], "last_issued_utc": forecast[4],
        "matured": int(outcome[0] or 0), "average_predicted_side_net_pips": outcome[1],
        "direction_accuracy": outcome[2], "cost_clear_rate": outcome[3],
        "magnitude_gate_passes": int(magnitude_gate[0] or 0),
        "directional_trade_gate_passes": int(forecast[1] or 0),
    }


def _summary_cache_key(
    config: Mapping[str, Any], *, cohort_id: str | None
) -> str:
    definition = {
        "contract_id": SUMMARY_CACHE_CONTRACT_ID,
        "cohort_id": cohort_id,
        "minimum_clear_probability": float(config["minimum_clear_probability"]),
        "minimum_predicted_magnitude_cost_ratio": float(
            config["minimum_predicted_magnitude_cost_ratio"]
        ),
    }
    return hashlib.sha256(
        json.dumps(definition, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def exact_cached_summary(
    db: sqlite3.Connection,
    config: Mapping[str, Any],
    *,
    cohort_id: str | None = None,
) -> dict[str, Any]:
    """Return an exact summary without rescanning unchanged immutable rows.

    The insert triggers delete every cached summary whenever a forecast or
    outcome is appended.  A cache hit is therefore equivalent to recomputing
    :func:`summary`; it is never used across an evidence mutation.
    """

    cache_key = _summary_cache_key(config, cohort_id=cohort_id)
    row = db.execute(
        "SELECT payload_json FROM prospective_exact_summary_cache_v1 "
        "WHERE cache_key=?",
        (cache_key,),
    ).fetchone()
    if row is not None:
        try:
            value = json.loads(str(row[0]))
        except json.JSONDecodeError:
            value = None
        if isinstance(value, dict):
            return value
        db.execute(
            "DELETE FROM prospective_exact_summary_cache_v1 WHERE cache_key=?",
            (cache_key,),
        )
    value = summary(db, config, cohort_id=cohort_id)
    db.execute(
        "INSERT OR REPLACE INTO prospective_exact_summary_cache_v1 VALUES (?,?)",
        (cache_key, json.dumps(value, sort_keys=True, separators=(",", ":"))),
    )
    return value


def exact_cached_ledger_cohort_count(db: sqlite3.Connection) -> int:
    """Cache the exact cohort count until a forecast insert invalidates it."""

    cache_key = f"{SUMMARY_CACHE_CONTRACT_ID}:ledger_cohort_count"
    row = db.execute(
        "SELECT payload_json FROM prospective_exact_summary_cache_v1 "
        "WHERE cache_key=?",
        (cache_key,),
    ).fetchone()
    if row is not None:
        try:
            value = json.loads(str(row[0]))
        except json.JSONDecodeError:
            value = None
        if isinstance(value, dict) and isinstance(value.get("value"), int):
            return int(value["value"])
        db.execute(
            "DELETE FROM prospective_exact_summary_cache_v1 WHERE cache_key=?",
            (cache_key,),
        )
    value = int(
        db.execute("SELECT COUNT(DISTINCT cohort_id) FROM forecasts").fetchone()[0]
        or 0
    )
    db.execute(
        "INSERT OR REPLACE INTO prospective_exact_summary_cache_v1 VALUES (?,?)",
        (cache_key, json.dumps({"value": value}, separators=(",", ":"))),
    )
    return value


def run_once(
    source_db: Path = SOURCE_DB,
    ledger_path: Path = LEDGER,
    output: Path = OUTPUT,
    report: Path = REPORT,
    maximum_source_age_sec: float = 180.0,
    maximum_issue_lag_sec: float = 120.0,
    mature_only: bool = False,
) -> dict[str, Any]:
    config = read_json(CONFIG)
    manifest = read_json(ARTIFACT / "manifest.json")
    collection_contract = prospective_contract(manifest)
    runtime_manifest = {**manifest, "cohort_id": collection_contract["cohort_id"]}
    generated, observation_clock = collector_observation_time(now_utc())
    failures = verify_artifact(manifest)
    clock = source_clock(source_db)
    source_age = generated.timestamp() - float(clock["last_event_epoch"] or 0.0)
    clock_trusted = bool(observation_clock.get("trusted_for_prospective_evidence"))
    status = (
        "blocked_integrity" if failures else
        "ok" if mature_only else
        "market_or_source_stale" if source_age > maximum_source_age_sec else
        "blocked_clock_integrity" if not clock_trusted else "ok"
    )
    db = open_ledger(ledger_path)
    new_forecasts = new_gate_passes = new_outcomes = 0
    skipped: list[str] = []
    if status == "ok":
        source._PIP_SIZES = source.load_pip_sizes(QUOTES)
        feature_epochs = (
            set()
            if mature_only
            else {
                candidate_epoch(int(clock["latest_completed_epoch"]), int(horizon))
                for horizon in config.get("horizons_sec") or []
            }
        )
        outcome_epochs = required_outcome_epochs(db, int(clock["latest_completed_epoch"]))
        by_pair, instruments, loaded_rows = load_required_minutes(
            source_db,
            feature_epochs=feature_epochs,
            outcome_epochs=outcome_epochs,
        )
        new_outcomes = mature_outcomes(
            db, by_pair, clock, float(config["modeled_slippage_pips"]), generated, source_db
        )
        if not mature_only:
            models = joblib.load(ARTIFACT / "models.joblib")
            new_forecasts, new_gate_passes, skipped = issue_forecasts(
                db, by_pair, models, runtime_manifest, config, clock, generated, maximum_issue_lag_sec, source_db
            )
        clock["loaded_instrument_count"] = len(instruments)
        clock["loaded_minute_row_count"] = loaded_rows
        clock["bounded_feature_epoch_count"] = len(feature_epochs)
        clock["exact_outcome_epoch_count"] = len(outcome_epochs)
    totals = exact_cached_summary(
        db, config, cohort_id=collection_contract["cohort_id"]
    )
    all_cohort_diagnostics = exact_cached_summary(db, config)
    ledger_cohort_count = exact_cached_ledger_cohort_count(db)
    pending_forecasts = pending_forecast_count(db)
    if mature_only and not failures:
        status = (
            "drained_retired"
            if pending_forecasts == 0
            else "draining_pending_outcomes"
        )
    payload = {
        "schema_version": 1, "generated_utc": generated.isoformat(), "status": status,
        "cohort_id": collection_contract["cohort_id"],
        "model_cohort_id": collection_contract["model_cohort_id"],
        "collection_cohort": collection_contract,
        "research_only": True, "execution_eligible": False,
        "can_place_orders": False, "can_promote": False, "supported_execution_decision": "no_trade",
        "artifact_failures": failures, "source": {**clock, "age_sec": source_age},
        "observation_clock": observation_clock,
        "cycle": {"new_forecasts": new_forecasts, "new_gate_passes": new_gate_passes, "new_outcomes": new_outcomes, "skipped": skipped},
        "totals": totals,
        "totals_scope": "current_collection_cohort_only",
        "all_cohort_diagnostics": all_cohort_diagnostics,
        "ledger_cohort_count": ledger_cohort_count,
        "pending_forecasts": pending_forecasts,
        "runtime_mode": "mature_only" if mature_only else "produce_and_mature",
        "contract": {
            "forecasts_written_before_outcomes": True, "late_backfill_refused": True,
            "immutable_forecasts_and_outcomes": True, "exact_horizon": True,
            "material_model_change_requires_new_cohort": True,
            "material_collector_change_requires_new_cohort": True,
            "cross_cohort_aggregation_is_diagnostic_not_proof": True,
            "future_forecast_production": not mature_only,
            "pending_forecasts_preserved_to_maturity": True,
            "pending_outcome_queue_contract_id": PENDING_QUEUE_CONTRACT_ID,
            "exact_summary_cache_contract_id": SUMMARY_CACHE_CONTRACT_ID,
            "summary_cache_invalidated_by_forecast_or_outcome_insert": True,
            "mature_only_full_history_scan_per_cycle": False,
        },
    }
    db.execute("INSERT INTO cycles VALUES (?,?,?)", (generated.isoformat(), status, json.dumps(payload, sort_keys=True)))
    db.commit()
    db.close()
    atomic(output, json.dumps(payload, indent=2, sort_keys=True))
    lines = [
        "# Executable Opportunity Prospective Proof", "", f"Generated: `{payload['generated_utc']}`", "",
        f"- Status: **{status}**", f"- Collection cohort: `{collection_contract['cohort_id']}`",
        f"- Frozen model cohort: `{collection_contract['model_cohort_id']}`",
        f"- Forecasts / maturities: **{totals['forecasts']} / {totals['matured']}**",
        "- Evidence totals scope: **current collection cohort only**",
        f"- Historical ledger cohorts retained for diagnostics: **{ledger_cohort_count}**",
        f"- Magnitude-only gate passes: **{totals['magnitude_gate_passes']}**",
        f"- Directional trade-gate passes: **{totals['directional_trade_gate_passes']}**",
        f"- Current cycle: **+{new_forecasts} forecasts, +{new_outcomes} outcomes**",
        f"- Source age: **{source_age:.1f}s**", "- Execution: **research-only; no_trade**", "",
        "Late or stale observations are not backfilled. A closed market therefore produces no forecasts rather than synthetic evidence.", "",
    ]
    atomic(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--maximum-source-age-sec", type=float, default=180.0)
    parser.add_argument("--maximum-issue-lag-sec", type=float, default=120.0)
    parser.add_argument(
        "--mature-only",
        action="store_true",
        help="Stop new forecasts and resolve only immutable pending outcomes.",
    )
    args = parser.parse_args()
    stop = time.monotonic() + args.duration_sec
    while True:
        payload = run_once(
            maximum_source_age_sec=args.maximum_source_age_sec,
            maximum_issue_lag_sec=args.maximum_issue_lag_sec,
            mature_only=args.mature_only,
        )
        if args.once or time.monotonic() >= stop:
            return 0
        sleep_sec = (
            max(args.interval_sec, 240.0)
            if args.mature_only and payload.get("status") == "drained_retired"
            else args.interval_sec
        )
        time.sleep(min(sleep_sec, max(0.0, stop - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
