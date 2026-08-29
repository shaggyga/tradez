#!/usr/bin/env python3
"""Continuously publish, score, and gate fresh model-gap forecasts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import msvcrt
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_model_gap_live_signal_producer import (
        DEFAULT_MODEL_ROOT,
        DEFAULT_PROMOTION_STATE,
        DEFAULT_REPORT,
        ModelGapArtifactProducer,
    )
    from oanda_model_predictor_auto_promotion import ACCOUNT_SCOPE
    from oanda_signal_contribution_feed import SignalContributionFeed
except ModuleNotFoundError:
    from trad.oanda_model_gap_live_signal_producer import (
        DEFAULT_MODEL_ROOT,
        DEFAULT_PROMOTION_STATE,
        DEFAULT_REPORT,
        ModelGapArtifactProducer,
    )
    from trad.oanda_model_predictor_auto_promotion import ACCOUNT_SCOPE
    from trad.oanda_signal_contribution_feed import SignalContributionFeed


ROOT = Path(__file__).resolve().parent
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_FEATURE_SNAPSHOT = STATE_ROOT / "live_model_feature_snapshot_v1.json"
DEFAULT_FEED = STATE_ROOT / "practice_007_signal_feed_v1.sqlite"
DEFAULT_LEDGER = STATE_ROOT / "model_gap_live_forecasts_v1.sqlite"
DEFAULT_PROCESS_LOCK = STATE_ROOT / "model_gap_live_worker_v1.lock"
DEFAULT_STATE = STATE_ROOT / "model_gap_live_worker_v1.json"
DEFAULT_FEATURE_ARCHIVE = (
    ROOT / "data" / "oanda_training_manager" / "prospective_live_model_features"
)
MIN_ALL_PAIR_SNAPSHOT_AGE_SEC = 600.0


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
            temporary.unlink(missing_ok=True)


class WorkerProcessLock:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.handle: Any | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            handle.close()
            return False
        self.handle = handle
        return True

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self.handle.close()
            self.handle = None


def sync_promotion_registry(
    feed: SignalContributionFeed,
    producer: ModelGapArtifactProducer,
) -> dict[str, Any]:
    promoted = producer.promotion.promoted_models(producer.runtimes)
    eligibility = {
        model_id: model_id in promoted for model_id in producer.requested_models
    }
    changed = feed.set_contributor_account_eligibility(
        eligibility,
        account_scope=ACCOUNT_SCOPE,
        policy_status=producer.promotion.status,
    )
    return {
        "policy_status": producer.promotion.status,
        "promoted_models": sorted(promoted),
        "registry_rows_updated": changed,
        "account_scope": ACCOUNT_SCOPE,
        "real_account_authorized": False,
    }


def archive_feature_snapshot(
    snapshot: dict[str, Any],
    root: Path,
) -> Path | None:
    """Persist one deduplicated, compressed row per instrument and snapshot."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    generated = finite(snapshot.get("generated_epoch"))
    instruments = snapshot.get("instruments") or {}
    if generated <= 0.0 or not isinstance(instruments, dict):
        return None
    timestamp = datetime.fromtimestamp(generated, timezone.utc)
    snapshot_id = str(snapshot.get("snapshot_id") or generated)
    digest = hashlib.sha256(snapshot_id.encode("utf-8")).hexdigest()[:20]
    directory = (
        Path(root)
        / f"date={timestamp.strftime('%Y%m%d')}"
        / f"hour={timestamp.strftime('%H')}"
    )
    path = directory / f"features_{digest}.parquet"
    if path.is_file():
        return path
    rows: list[dict[str, Any]] = []
    for instrument, raw in sorted(instruments.items()):
        if not isinstance(raw, dict):
            continue
        quote = raw.get("quote") or {}
        views = raw.get("timeframe_features") or {}
        if not isinstance(views, dict) or not views:
            views = {"UNSPECIFIED": raw.get("features") or {}}
        else:
            views = dict(views)
        unified = raw.get("unified_forecast_features") or {}
        if isinstance(unified, dict) and unified:
            views["UNIFIED_MTF"] = unified
        for timeframe, feature_values in sorted(views.items()):
            if not isinstance(feature_values, dict):
                continue
            row: dict[str, Any] = {
                "snapshot_id": snapshot_id,
                "generated_epoch": generated,
                "generated_utc": str(snapshot.get("generated_utc") or ""),
                "instrument": str(instrument),
                "input_timeframe": str(timeframe),
                "feature_origin_utc": str(
                    feature_values.get("candle_time")
                    or raw.get("feature_origin_utc")
                    or ""
                ),
                "bid": finite(quote.get("bid"), math.nan),
                "ask": finite(quote.get("ask"), math.nan),
                "quote_time": str(quote.get("time") or ""),
                "quote_source": str(quote.get("source") or ""),
            }
            for prefix, values in (
                ("", feature_values),
                ("micro_", raw.get("microstructure") or {}),
            ):
                if not isinstance(values, dict):
                    continue
                for name, value in values.items():
                    if isinstance(value, bool):
                        row[prefix + str(name)] = value
                    elif isinstance(value, (int, float)):
                        numeric = finite(value, math.nan)
                        row[prefix + str(name)] = (
                            numeric if math.isfinite(numeric) else None
                        )
                    elif isinstance(value, str):
                        row[prefix + str(name)] = value
            row["input_timeframe"] = str(timeframe)
            rows.append(row)
    if not rows:
        return None
    ordered_columns = list(rows[0])
    ordered_columns.extend(
        sorted(
            {
                name
                for row in rows[1:]
                for name in row
                if name not in ordered_columns
            }
        )
    )
    normalized_rows = [
        {name: row.get(name) for name in ordered_columns}
        for row in rows
    ]
    directory.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    try:
        pq.write_table(
            pa.Table.from_pylist(normalized_rows),
            temporary,
            compression="zstd",
            use_dictionary=True,
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


class LiveForecastLedger:
    def __init__(self, path: Path, retention_days: int = 30) -> None:
        self.path = Path(path)
        self.retention_days = max(1, int(retention_days))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existing_database = self.path.is_file() and self.path.stat().st_size > 0
        self.connection = sqlite3.connect(self.path, timeout=60.0)
        self.connection.execute("PRAGMA busy_timeout=60000")
        # WAL mode persists in the database header. Reissuing the mode change
        # against an active existing ledger requests an exclusive lock and can
        # stall startup behind long-running readers. Only initialize a new
        # ledger; repair a genuinely non-WAL existing ledger explicitly.
        journal_mode = str(
            self.connection.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        if not existing_database or journal_mode != "wal":
            journal_mode = str(
                self.connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
        if journal_mode != "wal":
            raise RuntimeError(
                f"forecast ledger requires WAL journal mode, found {journal_mode}"
            )
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.last_register_stats = {
            "candidates": 0,
            "points": 0,
            "expired_points_skipped": 0,
            "inserted_points": 0,
        }
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                candidate_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                family TEXT NOT NULL,
                model_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                input_timeframe TEXT NOT NULL,
                generated_epoch REAL NOT NULL,
                target_epoch REAL NOT NULL,
                direction TEXT NOT NULL,
                probability_up REAL NOT NULL,
                predicted_signed_pips REAL NOT NULL,
                predicted_magnitude_pips REAL NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                entry_mid REAL NOT NULL,
                entry_spread_pips REAL NOT NULL,
                pip REAL NOT NULL,
                artifact_sha256 TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                PRIMARY KEY(candidate_id, horizon_sec)
            );
            CREATE INDEX IF NOT EXISTS model_gap_prediction_maturity
                ON predictions(target_epoch, model_id, input_timeframe, horizon_sec);
            CREATE TABLE IF NOT EXISTS outcomes (
                candidate_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                family TEXT NOT NULL,
                model_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                input_timeframe TEXT NOT NULL,
                generated_epoch REAL NOT NULL,
                target_epoch REAL NOT NULL,
                observed_epoch REAL NOT NULL,
                outcome_delay_sec REAL NOT NULL,
                direction TEXT NOT NULL,
                probability_up REAL NOT NULL,
                predicted_signed_pips REAL NOT NULL,
                predicted_magnitude_pips REAL NOT NULL,
                actual_up INTEGER NOT NULL,
                direction_correct INTEGER NOT NULL,
                executable_profitable INTEGER NOT NULL,
                executable_net_pips REAL NOT NULL,
                signed_mid_move_pips REAL NOT NULL,
                signed_pip_error REAL NOT NULL,
                signed_pip_abs_error REAL NOT NULL,
                magnitude_pip_error REAL NOT NULL,
                magnitude_pip_abs_error REAL NOT NULL,
                brier REAL NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                entry_mid REAL NOT NULL,
                entry_spread_pips REAL NOT NULL,
                exit_spread_pips REAL NOT NULL,
                pip REAL NOT NULL,
                artifact_sha256 TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY(candidate_id, horizon_sec)
            );
            CREATE INDEX IF NOT EXISTS model_gap_outcome_scope
                ON outcomes(observed_epoch, model_id, input_timeframe, horizon_sec);
            """
        )
        migrations = {
            "predictions": {
                "family": (
                    "ALTER TABLE predictions ADD COLUMN family TEXT NOT NULL "
                    "DEFAULT ''"
                ),
                "predicted_signed_pips": (
                    "ALTER TABLE predictions ADD COLUMN predicted_signed_pips "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "predicted_magnitude_pips": (
                    "ALTER TABLE predictions ADD COLUMN predicted_magnitude_pips "
                    "REAL NOT NULL DEFAULT 0"
                ),
            },
            "outcomes": {
                "family": (
                    "ALTER TABLE outcomes ADD COLUMN family TEXT NOT NULL DEFAULT ''"
                ),
                "predicted_signed_pips": (
                    "ALTER TABLE outcomes ADD COLUMN predicted_signed_pips "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "predicted_magnitude_pips": (
                    "ALTER TABLE outcomes ADD COLUMN predicted_magnitude_pips "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "signed_pip_error": (
                    "ALTER TABLE outcomes ADD COLUMN signed_pip_error "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "signed_pip_abs_error": (
                    "ALTER TABLE outcomes ADD COLUMN signed_pip_abs_error "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "magnitude_pip_error": (
                    "ALTER TABLE outcomes ADD COLUMN magnitude_pip_error "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "magnitude_pip_abs_error": (
                    "ALTER TABLE outcomes ADD COLUMN magnitude_pip_abs_error "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "entry_bid": (
                    "ALTER TABLE outcomes ADD COLUMN entry_bid "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "entry_ask": (
                    "ALTER TABLE outcomes ADD COLUMN entry_ask "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "entry_mid": (
                    "ALTER TABLE outcomes ADD COLUMN entry_mid "
                    "REAL NOT NULL DEFAULT 0"
                ),
                "pip": (
                    "ALTER TABLE outcomes ADD COLUMN pip "
                    "REAL NOT NULL DEFAULT 0.0001"
                ),
                "artifact_sha256": (
                    "ALTER TABLE outcomes ADD COLUMN artifact_sha256 "
                    "TEXT NOT NULL DEFAULT ''"
                ),
                "snapshot_id": (
                    "ALTER TABLE outcomes ADD COLUMN snapshot_id "
                    "TEXT NOT NULL DEFAULT ''"
                ),
            },
        }
        for table, statements in migrations.items():
            existing = {
                str(row[1])
                for row in self.connection.execute(f"PRAGMA table_info({table})")
            }
            for column, statement in statements.items():
                if column not in existing:
                    self.connection.execute(statement)
        self.connection.commit()
        self.pending_predictions = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM predictions"
            ).fetchone()[0]
        )

    def register(
        self,
        candidates: list[dict[str, Any]],
        snapshot_id: str,
        *,
        minimum_target_epoch: float | None = None,
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        points = 0
        expired_points = 0
        for candidate in candidates:
            generated = finite(candidate.get("forecast_generated_epoch"))
            bid = finite(candidate.get("bid"))
            ask = finite(candidate.get("ask"))
            pip = max(1e-12, finite(candidate.get("pip"), 0.0001))
            if generated <= 0.0 or bid <= 0.0 or ask < bid:
                continue
            metadata = (
                (candidate.get("signal_provenance") or {}).get("producer_metadata")
                or {}
            )
            for raw_horizon, point in (candidate.get("forecast_curve") or {}).items():
                if not isinstance(point, dict):
                    continue
                horizon = int(finite(raw_horizon, finite(point.get("horizon_sec"))))
                if horizon <= 0:
                    continue
                points += 1
                target_epoch = generated + horizon
                if (
                    minimum_target_epoch is not None
                    and target_epoch <= float(minimum_target_epoch)
                ):
                    expired_points += 1
                    continue
                rows.append(
                    (
                        str(candidate.get("id") or ""),
                        horizon,
                        str(candidate.get("family") or ""),
                        str(candidate.get("model_id") or ""),
                        str(candidate.get("instrument") or ""),
                        str(candidate.get("input_timeframe") or ""),
                        generated,
                        target_epoch,
                        str(point.get("direction") or candidate.get("direction") or ""),
                        finite(point.get("probability_up"), 0.5),
                        finite(point.get("predicted_signed_pips")),
                        max(
                            0.0,
                            finite(
                                point.get("predicted_magnitude_pips"),
                                abs(finite(point.get("predicted_signed_pips"))),
                            ),
                        ),
                        bid,
                        ask,
                        0.5 * (bid + ask),
                        max(0.0, (ask - bid) / pip),
                        pip,
                        str(metadata.get("artifact_sha256") or ""),
                        snapshot_id,
                    )
                )
        if not rows:
            self.last_register_stats = {
                "candidates": len(candidates),
                "points": points,
                "expired_points_skipped": expired_points,
                "inserted_points": 0,
            }
            return 0
        before = self.connection.total_changes
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO predictions(
                candidate_id, horizon_sec, family, model_id, instrument,
                input_timeframe, generated_epoch, target_epoch, direction,
                probability_up, predicted_signed_pips, predicted_magnitude_pips,
                entry_bid, entry_ask, entry_mid, entry_spread_pips, pip,
                artifact_sha256, snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.connection.commit()
        inserted = self.connection.total_changes - before
        self.pending_predictions += inserted
        self.last_register_stats = {
            "candidates": len(candidates),
            "points": points,
            "expired_points_skipped": expired_points,
            "inserted_points": inserted,
        }
        return inserted

    def _delete_prediction_rowids(
        self,
        rowids: list[int],
        *,
        chunk_size: int = 1000,
    ) -> None:
        for offset in range(0, len(rowids), max(1, int(chunk_size))):
            chunk = rowids[offset : offset + max(1, int(chunk_size))]
            placeholders = ",".join("?" for _ in chunk)
            self.connection.execute(
                f"DELETE FROM predictions WHERE rowid IN ({placeholders})",
                chunk,
            )

    def mature(
        self,
        snapshot: dict[str, Any],
        max_delay_sec: float,
        batch_size: int = 100000,
        *,
        censor_missing_quotes: bool = True,
    ) -> dict[str, int]:
        observed = finite(snapshot.get("generated_epoch"), time.time())
        instruments = snapshot.get("instruments") or {}
        lower_bound = observed - max(0.0, max_delay_sec)
        due = self.connection.execute(
            """
            SELECT p.rowid, p.* FROM predictions p
            WHERE p.target_epoch > ? AND p.target_epoch <= ?
            ORDER BY p.target_epoch LIMIT ?
            """,
            (lower_bound, observed, max(1, int(batch_size))),
        ).fetchall()
        columns = [row[1] for row in self.connection.execute("PRAGMA table_info(predictions)")]
        matured: list[tuple[Any, ...]] = []
        censored: list[tuple[Any, ...]] = []
        processed_rowids: list[int] = []
        for values in due:
            row = dict(zip(columns, values[1:]))
            quote = ((instruments.get(row["instrument"]) or {}).get("quote") or {})
            bid = finite(quote.get("bid"))
            ask = finite(quote.get("ask"))
            delay = max(0.0, observed - finite(row["target_epoch"]))
            quote_available = bid > 0.0 and ask >= bid
            if not quote_available and not censor_missing_quotes:
                continue
            processed_rowids.append(int(values[0]))
            status = "matured"
            if delay > max_delay_sec or not quote_available:
                status = "censored_late_or_missing_quote"
                bid = finite(row["entry_bid"])
                ask = finite(row["entry_ask"])
            pip = max(1e-12, finite(row["pip"]))
            exit_mid = 0.5 * (bid + ask)
            signed_move = (exit_mid - finite(row["entry_mid"])) / pip
            actual_up = int(signed_move > 0.0)
            direction = str(row["direction"])
            net = (
                (bid - finite(row["entry_ask"])) / pip
                if direction == "buy"
                else (finite(row["entry_bid"]) - ask) / pip
            )
            probability_up = finite(row["probability_up"], 0.5)
            predicted_signed = finite(row.get("predicted_signed_pips"))
            predicted_magnitude = max(
                0.0,
                finite(
                    row.get("predicted_magnitude_pips"),
                    abs(predicted_signed),
                ),
            )
            signed_error = predicted_signed - signed_move
            magnitude_error = predicted_magnitude - abs(signed_move)
            output = (
                row["candidate_id"],
                row["horizon_sec"],
                row["family"],
                row["model_id"],
                row["instrument"],
                row["input_timeframe"],
                row["generated_epoch"],
                row["target_epoch"],
                observed,
                delay,
                direction,
                probability_up,
                predicted_signed,
                predicted_magnitude,
                actual_up,
                int((direction == "buy") == bool(actual_up)),
                int(net > 0.0 and status == "matured"),
                net if status == "matured" else 0.0,
                signed_move if status == "matured" else 0.0,
                signed_error if status == "matured" else 0.0,
                abs(signed_error) if status == "matured" else 0.0,
                magnitude_error if status == "matured" else 0.0,
                abs(magnitude_error) if status == "matured" else 0.0,
                (probability_up - actual_up) ** 2 if status == "matured" else 0.0,
                row["entry_bid"],
                row["entry_ask"],
                row["entry_mid"],
                row["entry_spread_pips"],
                max(0.0, (ask - bid) / pip),
                pip,
                row["artifact_sha256"],
                row["snapshot_id"],
                status,
            )
            (matured if status == "matured" else censored).append(output)
        rows = [*matured, *censored]
        if rows:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO outcomes(
                    candidate_id, horizon_sec, family, model_id, instrument,
                    input_timeframe, generated_epoch, target_epoch,
                    observed_epoch, outcome_delay_sec, direction, probability_up,
                    predicted_signed_pips, predicted_magnitude_pips, actual_up,
                    direction_correct, executable_profitable,
                    executable_net_pips, signed_mid_move_pips, signed_pip_error,
                    signed_pip_abs_error, magnitude_pip_error,
                    magnitude_pip_abs_error, brier, entry_bid, entry_ask,
                    entry_mid, entry_spread_pips, exit_spread_pips, pip,
                    artifact_sha256, snapshot_id, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self._delete_prediction_rowids(processed_rowids)
        remaining_batch = max(0, int(batch_size) - len(rows))
        expired = []
        if remaining_batch:
            expired = self.connection.execute(
                """
                SELECT p.rowid
                FROM predictions p
                WHERE p.target_epoch <= ?
                ORDER BY p.target_epoch
                LIMIT ?
                """,
                (lower_bound, remaining_batch),
            ).fetchall()
        if expired:
            self._delete_prediction_rowids([int(row[0]) for row in expired])
        if rows or expired:
            self.connection.commit()
            self.pending_predictions = max(
                0,
                self.pending_predictions - len(processed_rowids) - len(expired),
            )
        return {
            "matured": len(matured),
            "censored": len(censored) + len(expired),
        }

    def prune(self, now: float) -> int:
        cutoff = now - self.retention_days * 86400.0
        before = self.connection.total_changes
        self.connection.execute("DELETE FROM outcomes WHERE observed_epoch < ?", (cutoff,))
        prediction_cursor = self.connection.execute(
            """
            DELETE FROM predictions
            WHERE generated_epoch < ?
            """,
            (cutoff,),
        )
        self.connection.commit()
        self.pending_predictions = max(
            0,
            self.pending_predictions - max(0, prediction_cursor.rowcount),
        )
        return self.connection.total_changes - before

    def summary(self, window_days: int = 7) -> dict[str, Any]:
        cutoff = time.time() - max(1, int(window_days)) * 86400.0
        rows = self.connection.execute(
            """
            SELECT family, model_id, input_timeframe, horizon_sec, COUNT(*),
                   AVG(direction_correct), AVG(executable_profitable),
                   AVG(executable_net_pips), SUM(executable_net_pips), AVG(brier),
                   AVG(signed_pip_abs_error), AVG(signed_pip_error),
                   AVG(magnitude_pip_abs_error), AVG(magnitude_pip_error),
                   AVG(predicted_signed_pips * signed_mid_move_pips),
                   AVG(predicted_signed_pips), AVG(signed_mid_move_pips),
                   AVG(predicted_signed_pips * predicted_signed_pips),
                   AVG(signed_mid_move_pips * signed_mid_move_pips)
            FROM outcomes
            WHERE observed_epoch >= ? AND status = 'matured'
            GROUP BY family, model_id, input_timeframe, horizon_sec
            ORDER BY family, model_id, input_timeframe, horizon_sec
            """,
            (cutoff,),
        ).fetchall()
        cost_rows = self.connection.execute(
            """
            WITH matured AS (
                SELECT family, model_id, input_timeframe, horizon_sec,
                       direction_correct, executable_profitable,
                       executable_net_pips,
                       0.5 * (entry_spread_pips + exit_spread_pips)
                           AS round_trip_spread_pips
                FROM outcomes
                WHERE observed_epoch >= ? AND status = 'matured'
            )
            SELECT family, model_id, input_timeframe, horizon_sec,
                   CASE
                       WHEN round_trip_spread_pips <= 3.0 THEN 'liquid_le_3pips'
                       WHEN round_trip_spread_pips <= 10.0 THEN 'moderate_3_10pips'
                       WHEN round_trip_spread_pips <= 50.0 THEN 'wide_10_50pips'
                       ELSE 'extreme_gt_50pips'
                   END AS cost_bucket,
                   COUNT(*), AVG(direction_correct), AVG(executable_profitable),
                   AVG(executable_net_pips), SUM(executable_net_pips),
                   AVG(round_trip_spread_pips)
            FROM matured
            GROUP BY family, model_id, input_timeframe, horizon_sec, cost_bucket
            ORDER BY family, model_id, input_timeframe, horizon_sec, cost_bucket
            """,
            (cutoff,),
        ).fetchall()
        def correlation(row: tuple[Any, ...]) -> float | None:
            covariance = finite(row[14]) - finite(row[15]) * finite(row[16])
            left_variance = max(
                0.0,
                finite(row[17]) - finite(row[15]) ** 2,
            )
            right_variance = max(
                0.0,
                finite(row[18]) - finite(row[16]) ** 2,
            )
            denominator = math.sqrt(left_variance * right_variance)
            return None if denominator <= 1e-12 else covariance / denominator

        return {
            "database": str(self.path.resolve()),
            "pending": self.pending_predictions,
            "window_days": max(1, int(window_days)),
            "cells": [
                {
                    "family": row[0],
                    "model_id": row[1],
                    "input_timeframe": row[2],
                    "horizon_sec": int(row[3]),
                    "n": int(row[4]),
                    "direction_accuracy": float(row[5]),
                    "executable_win_rate": float(row[6]),
                    "average_net_pips": float(row[7]),
                    "sum_net_pips": float(row[8]),
                    "brier": float(row[9]),
                    "signed_pip_mae": float(row[10]),
                    "signed_pip_bias": float(row[11]),
                    "magnitude_pip_mae": float(row[12]),
                    "magnitude_pip_bias": float(row[13]),
                    "signed_pip_correlation": correlation(row),
                }
                for row in rows
            ],
            "cost_bucket_basis": (
                "round-trip spread pips = mean of entry and exit quoted spreads"
            ),
            "cost_bucket_cells": [
                {
                    "family": row[0],
                    "model_id": row[1],
                    "input_timeframe": row[2],
                    "horizon_sec": int(row[3]),
                    "cost_bucket": row[4],
                    "n": int(row[5]),
                    "direction_accuracy": float(row[6]),
                    "executable_win_rate": float(row[7]),
                    "average_net_pips": float(row[8]),
                    "sum_net_pips": float(row[9]),
                    "average_round_trip_spread_pips": float(row[10]),
                }
                for row in cost_rows
            ],
        }

    def close(self) -> None:
        self.connection.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-snapshot", type=Path, default=DEFAULT_FEATURE_SNAPSHOT)
    parser.add_argument("--signal-feed", type=Path, default=DEFAULT_FEED)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--process-lock", type=Path, default=DEFAULT_PROCESS_LOCK)
    parser.add_argument("--feature-archive-root", type=Path, default=DEFAULT_FEATURE_ARCHIVE)
    parser.add_argument("--model-root", type=Path, default=DEFAULT_MODEL_ROOT)
    parser.add_argument("--model-report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--promotion-state", type=Path, default=DEFAULT_PROMOTION_STATE)
    parser.add_argument("--promotion-max-age-sec", type=float, default=1800.0)
    parser.add_argument(
        "--models",
        default="logistic_baseline,hist_gradient_boosting,catboost,ngboost",
    )
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--ttl-sec", type=float, default=180.0)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=180.0)
    parser.add_argument("--max-outcome-delay-sec", type=float, default=180.0)
    parser.add_argument("--maturity-batch-size", type=int, default=20000)
    parser.add_argument("--summary-interval-sec", type=float, default=21600.0)
    parser.add_argument("--prune-interval-sec", type=float, default=86400.0)
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument("--reload-sec", type=float, default=900.0)
    parser.add_argument("--promotion-reload-sec", type=float, default=60.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    # A complete 65+ pair snapshot currently takes about six minutes to roll.
    # A shorter freshness ceiling creates deterministic dead zones in which all
    # structural forecasts disappear even though completed-bar inputs remain
    # valid for the one-hour decision horizon.
    args.ttl_sec = max(args.ttl_sec, MIN_ALL_PAIR_SNAPSHOT_AGE_SEC)
    args.max_snapshot_age_sec = max(
        args.max_snapshot_age_sec,
        MIN_ALL_PAIR_SNAPSHOT_AGE_SEC,
    )
    args.max_outcome_delay_sec = max(
        args.max_outcome_delay_sec,
        MIN_ALL_PAIR_SNAPSHOT_AGE_SEC,
    )
    if min(
        args.interval_sec,
        args.duration_sec,
        args.ttl_sec,
        args.max_snapshot_age_sec,
        args.max_outcome_delay_sec,
        args.summary_interval_sec,
        args.prune_interval_sec,
        args.reload_sec,
        args.promotion_reload_sec,
        args.promotion_max_age_sec,
    ) <= 0.0 or args.retention_days <= 0 or args.maturity_batch_size <= 0:
        raise SystemExit("worker timing and retention arguments must be positive")
    return args


def _startup_state(args: argparse.Namespace, stage: str) -> None:
    atomic_json(
        args.state,
        {
            "schema_version": 1,
            "updated_utc": utc_iso(),
            "status": f"starting:{stage}",
            "feature_snapshot": str(args.feature_snapshot.resolve()),
            "process_lock": str(args.process_lock.resolve()),
            "contract": {
                "account_execution_authorized": False,
                "practice_account_execution_authorized": False,
                "real_account_execution_authorized": False,
            },
        },
    )


def _run_locked(args: argparse.Namespace) -> int:
    model_ids = [value.strip() for value in args.models.split(",") if value.strip()]
    _startup_state(args, "loading_models")
    producer = ModelGapArtifactProducer(
        args.model_root,
        args.model_report,
        model_ids,
        promotion_state_path=args.promotion_state,
        promotion_max_age_sec=args.promotion_max_age_sec,
    )
    _startup_state(args, "opening_signal_feed")
    feed = SignalContributionFeed(args.signal_feed)
    _startup_state(args, "syncing_registry")
    promotion_registry = sync_promotion_registry(feed, producer)
    _startup_state(args, "opening_ledger")
    ledger = LiveForecastLedger(args.ledger, args.retention_days)
    stop_at = time.monotonic() + args.duration_sec
    last_reload = time.monotonic()
    last_promotion_reload = time.monotonic()
    previous_state = read_json(args.state)
    last_snapshot_id = str(
        previous_state.get("feature_snapshot_id") or ""
    )
    cycles = int(previous_state.get("cycles") or 0)
    published = int(previous_state.get("published_forecasts") or 0)
    rejected = int(previous_state.get("rejected_forecasts") or 0)
    registered = int(
        previous_state.get("registered_prediction_points") or 0
    )
    matured = int(previous_state.get("matured_prediction_points") or 0)
    censored = int(previous_state.get("censored_prediction_points") or 0)
    errors = int(previous_state.get("errors") or 0)
    previous_archive = previous_state.get("feature_archive") or {}
    archived_snapshots = int(
        previous_archive.get("archived_snapshots") or 0
    )
    last_archive = str(previous_archive.get("last_partition") or "")
    ledger_summary = previous_state.get("ledger") or {}
    feed_summary = previous_state.get("feed") or {}
    current_ledger_path = str(args.ledger.resolve()).casefold()
    previous_ledger_path = str(ledger_summary.get("database") or "").casefold()
    can_reuse_summary = bool(
        ledger_summary
        and feed_summary
        and previous_ledger_path == current_ledger_path
    )
    summary_source = "previous_state" if can_reuse_summary else "none"
    summary_refreshed_utc = str(previous_state.get("updated_utc") or "")
    last_summary = time.monotonic() if summary_source == "previous_state" else -math.inf
    last_prune = time.monotonic()
    try:
        while time.monotonic() < stop_at:
            cycle_started = time.monotonic()
            now = time.time()
            snapshot = read_json(args.feature_snapshot)
            generated = finite(snapshot.get("generated_epoch"))
            snapshot_id = str(snapshot.get("snapshot_id") or generated or "")
            age = math.inf if generated <= 0.0 else max(0.0, now - generated)
            status = "waiting_for_feature_snapshot"
            publish_result: dict[str, Any] = {}
            mature_seconds = 0.0
            forecast_seconds = 0.0
            summary_seconds = 0.0
            prune_seconds = 0.0
            if time.monotonic() - last_reload >= args.reload_sec:
                producer.reload()
                promotion_registry = sync_promotion_registry(feed, producer)
                last_reload = time.monotonic()
                last_promotion_reload = last_reload
            elif time.monotonic() - last_promotion_reload >= args.promotion_reload_sec:
                producer.reload_promotion()
                promotion_registry = sync_promotion_registry(feed, producer)
                last_promotion_reload = time.monotonic()
            if snapshot and age <= args.max_snapshot_age_sec:
                if snapshot_id and snapshot_id != last_snapshot_id:
                    try:
                        forecast_started = time.monotonic()
                        archive_path = archive_feature_snapshot(
                            snapshot,
                            args.feature_archive_root,
                        )
                        if archive_path is not None:
                            archived_snapshots += 1
                            last_archive = str(archive_path.resolve())
                        raw_forecasts = producer.forecast(snapshot)
                        normalized = [
                            feed.normalize_forecast(
                                row,
                                "model_gap_live_artifact",
                                now=now,
                                max_age_sec=args.max_snapshot_age_sec,
                            )
                            for row in raw_forecasts
                        ]
                        publish_result = feed.publish_forecasts(
                            raw_forecasts,
                            "model_gap_live_artifact",
                            args.ttl_sec,
                            max_age_sec=args.max_snapshot_age_sec,
                        )
                        candidate_ids = publish_result.pop(
                            "candidate_ids", []
                        )
                        publish_result["candidate_id_count"] = len(
                            candidate_ids
                        )
                        registered += ledger.register(normalized, snapshot_id)
                        published += int(publish_result.get("accepted") or 0)
                        rejected += int(publish_result.get("rejected") or 0)
                        last_snapshot_id = snapshot_id
                        status = "published"
                        forecast_seconds = time.monotonic() - forecast_started
                    except Exception as exc:
                        errors += 1
                        status = f"publish_error:{type(exc).__name__}:{exc}"
                else:
                    status = "snapshot_already_processed"
                mature_started = time.monotonic()
                outcome = ledger.mature(
                    snapshot,
                    args.max_outcome_delay_sec,
                    batch_size=args.maturity_batch_size,
                )
                mature_seconds = time.monotonic() - mature_started
                matured += outcome["matured"]
                censored += outcome["censored"]
            elif snapshot:
                status = "stale_feature_snapshot"
            if time.monotonic() - last_prune >= args.prune_interval_sec:
                prune_started = time.monotonic()
                ledger.prune(now)
                prune_seconds = time.monotonic() - prune_started
                last_prune = time.monotonic()
            if time.monotonic() - last_summary >= args.summary_interval_sec:
                summary_started = time.monotonic()
                ledger_summary = ledger.summary()
                feed_summary = feed.coverage(fresh_sec=args.ttl_sec)
                summary_seconds = time.monotonic() - summary_started
                last_summary = time.monotonic()
                summary_source = "live_refresh"
                summary_refreshed_utc = utc_iso()
            ledger_summary = {
                **ledger_summary,
                "database": str(args.ledger.resolve()),
                "pending": ledger.pending_predictions,
            }
            cycles += 1
            state = {
                "schema_version": 1,
                "updated_utc": utc_iso(),
                "status": status,
                "feature_snapshot": str(args.feature_snapshot.resolve()),
                "feature_snapshot_id": snapshot_id,
                "feature_snapshot_age_sec": None if not math.isfinite(age) else round(age, 3),
                "cycles": cycles,
                "published_forecasts": published,
                "rejected_forecasts": rejected,
                "registered_prediction_points": registered,
                "matured_prediction_points": matured,
                "censored_prediction_points": censored,
                "errors": errors,
                "feature_archive": {
                    "root": str(args.feature_archive_root.resolve()),
                    "archived_snapshots": archived_snapshots,
                    "last_partition": last_archive,
                    "storage": "one row per instrument/timeframe/snapshot in hourly zstd parquet",
                },
                "last_publish": publish_result,
                "producer": producer.summary(),
                "predictor_auto_promotion": promotion_registry,
                "ledger": ledger_summary,
                "feed": feed_summary,
                "performance": {
                    "maturity_batch_size": args.maturity_batch_size,
                    "mature_seconds": round(mature_seconds, 3),
                    "forecast_seconds": round(forecast_seconds, 3),
                    "summary_seconds": round(summary_seconds, 3),
                    "prune_seconds": round(prune_seconds, 3),
                    "cycle_seconds": round(time.monotonic() - cycle_started, 3),
                    "summary_interval_sec": args.summary_interval_sec,
                    "prune_interval_sec": args.prune_interval_sec,
                    "summary_source": summary_source,
                    "summary_refreshed_utc": summary_refreshed_utc,
                },
                "contract": {
                    "historical_metrics_used_as_live_values": False,
                    "artifact_hash_verification_required": True,
                    "unsupported_cells_emitted": False,
                    "practice_account_execution_authorized": bool(
                        promotion_registry.get("promoted_models")
                    ),
                    "account_execution_authorized": bool(
                        promotion_registry.get("promoted_models")
                    ),
                    "account_scope": ACCOUNT_SCOPE,
                    "real_account_execution_authorized": False,
                    "outcomes_use_executable_bid_ask_costs": True,
                },
            }
            atomic_json(args.state, state)
            if args.once:
                return 0
            time.sleep(min(args.interval_sec, max(0.0, stop_at - time.monotonic())))
    finally:
        ledger.close()
        feed.close()
    return 0


def run(args: argparse.Namespace) -> int:
    process_lock = WorkerProcessLock(args.process_lock)
    if not process_lock.acquire():
        return 0
    try:
        return _run_locked(args)
    finally:
        process_lock.release()


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
