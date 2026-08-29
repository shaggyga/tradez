#!/usr/bin/env python3
"""Batched, typed storage for detailed shadow-trading outcomes."""

from __future__ import annotations

import json
import hashlib
import math
import sqlite3
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def sampled_detail(event_id: str, horizon_sec: int, sample_rate: float) -> bool:
    rate = max(0.0, min(1.0, float(sample_rate)))
    if rate <= 0.0:
        return False
    if rate >= 1.0:
        return True
    checksum = zlib.crc32(f"{event_id}:{int(horizon_sec)}".encode("utf-8"))
    return checksum / 0xFFFFFFFF < rate


class ShadowOutcomeStore:
    """Append-only forecast and outcome evidence with batched compatibility writes.

    ``outcomes`` remains the compact compatibility table used by the existing
    research workers.  ``canonical_forecasts`` is written before an outcome can
    exist and ``canonical_outcomes`` mirrors each matured result.  SQLite
    triggers make the canonical tables immutable so a later code revision
    cannot silently rewrite the evidence it is being judged against.
    """

    COLUMNS = (
        "event_id",
        "horizon_sec",
        "observed_utc",
        "lane_id",
        "family",
        "profile",
        "model_id",
        "input_timeframe",
        "training_timeframe",
        "kind",
        "instrument",
        "direction",
        "blocked_reason",
        "miss_class",
        "theoretical_pips",
        "outcome_delay_sec",
        "entry_time",
        "exit_time",
        "entry_bid",
        "entry_ask",
        "exit_bid",
        "exit_ask",
        "pip",
        "entry_spread_pips",
        "max_favorable_pips",
        "max_adverse_pips",
        "first_positive_sec",
        "path_samples",
        "positive_path_samples",
        "configured_stop_pips",
        "configured_target_r",
        "configured_stop_hit_sec",
        "configured_target_hit_sec",
        "diagnostics_json",
    )

    CORE_KEYS = {
        "id",
        "event_id",
        "horizon_sec",
        "lane_id",
        "family",
        "profile",
        "model_id",
        "input_timeframe",
        "training_timeframe",
        "kind",
        "instrument",
        "direction",
        "blocked_reason",
        "miss_class",
        "theoretical_pips",
        "outcome_delay_sec",
        "entry_time",
        "exit_time",
        "entry_bid",
        "entry_ask",
        "exit_bid",
        "exit_ask",
        "pip",
        "entry_spread_pips",
        "max_favorable_pips",
        "max_adverse_pips",
        "first_positive_sec",
        "path_samples",
        "positive_path_samples",
        "configured_stop_pips",
        "configured_target_r",
        "configured_stop_hit_sec",
        "configured_target_hit_sec",
    }

    OUTCOME_DIAGNOSTIC_KEYS = {
        "favorable_hits",
        "adverse_hits",
        "volatility_regime",
        "timeframe_prediction",
        "maturity_worker",
        "forecast_recorded_utc",
        "quote_snapshot_generated_utc",
        "provider_quote_time",
        "quote_snapshot_age_sec",
        "path_complete",
        "first_worker_observed_utc",
        "recovered_from_ledger",
        "research_only",
        "account_eligible",
        "can_place_orders",
    }

    def __init__(
        self,
        database_path: Path,
        *,
        batch_size: int = 4096,
        flush_sec: float = 1.0,
        busy_timeout_ms: int = 5000,
    ) -> None:
        self.database_path = Path(database_path)
        self.batch_size = max(64, int(batch_size))
        self.flush_sec = max(0.1, float(flush_sec))
        self.busy_timeout_ms = max(100, int(busy_timeout_ms))
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            self.database_path,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        self.connection.execute("PRAGMA temp_store=MEMORY")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS outcomes (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                observed_utc TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                family TEXT NOT NULL,
                profile TEXT NOT NULL,
                model_id TEXT NOT NULL,
                input_timeframe TEXT NOT NULL,
                training_timeframe TEXT NOT NULL,
                kind TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                blocked_reason TEXT NOT NULL,
                miss_class TEXT NOT NULL,
                theoretical_pips REAL NOT NULL,
                outcome_delay_sec REAL NOT NULL,
                entry_time TEXT NOT NULL,
                exit_time TEXT NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                exit_bid REAL NOT NULL,
                exit_ask REAL NOT NULL,
                pip REAL NOT NULL,
                entry_spread_pips REAL NOT NULL,
                max_favorable_pips REAL NOT NULL,
                max_adverse_pips REAL NOT NULL,
                first_positive_sec REAL,
                path_samples INTEGER NOT NULL,
                positive_path_samples INTEGER NOT NULL,
                configured_stop_pips REAL NOT NULL,
                configured_target_r REAL NOT NULL,
                configured_stop_hit_sec REAL,
                configured_target_hit_sec REAL,
                diagnostics_json TEXT NOT NULL,
                UNIQUE(event_id, horizon_sec)
            );
            CREATE INDEX IF NOT EXISTS shadow_outcomes_scope
                ON outcomes(horizon_sec, kind, family, instrument);
            CREATE INDEX IF NOT EXISTS shadow_outcomes_time
                ON outcomes(observed_utc);

            CREATE TABLE IF NOT EXISTS canonical_forecasts (
                event_id TEXT PRIMARY KEY,
                recorded_utc TEXT NOT NULL,
                model_version TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                data_cutoff_utc TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                family TEXT NOT NULL,
                profile TEXT NOT NULL,
                model_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                horizons_json TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                pip REAL NOT NULL,
                entry_spread_pips REAL NOT NULL,
                session_bucket TEXT NOT NULL,
                liquidity_bucket TEXT NOT NULL,
                predicted_magnitude_pips REAL,
                probability_up REAL,
                blocked_reason TEXT NOT NULL,
                miss_class TEXT NOT NULL,
                track_outcome INTEGER NOT NULL,
                forecast_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS canonical_forecasts_scope
                ON canonical_forecasts(family, instrument, entry_time);
            CREATE INDEX IF NOT EXISTS canonical_forecasts_cutoff
                ON canonical_forecasts(data_cutoff_utc);

            -- A small derived restart index uses the timestamp that actually
            -- defines maturity.  Indexing canonical_forecasts(entry_time)
            -- retroactively would rebuild a multi-gigabyte table during live
            -- startup; this queue is populated transactionally for every new
            -- tracked forecast and conservatively seeded from the former
            -- bounded cutoff window for pre-upgrade rows.
            CREATE TABLE IF NOT EXISTS canonical_recovery_index_v2 (
                event_id TEXT PRIMARY KEY,
                entry_time TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS canonical_recovery_index_v2_entry_time
                ON canonical_recovery_index_v2(entry_time);
            CREATE TRIGGER IF NOT EXISTS canonical_recovery_index_v2_no_update
            BEFORE UPDATE ON canonical_recovery_index_v2
            BEGIN
                SELECT RAISE(ABORT, 'canonical recovery index is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS canonical_recovery_index_v2_no_delete
            BEFORE DELETE ON canonical_recovery_index_v2
            BEGIN
                SELECT RAISE(ABORT, 'canonical recovery index is immutable');
            END;
            CREATE TABLE IF NOT EXISTS canonical_recovery_migrations (
                migration_id TEXT PRIMARY KEY,
                completed_utc TEXT NOT NULL,
                entry_cutoff_utc TEXT NOT NULL,
                inserted_rows INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS canonical_outcomes (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                observed_utc TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                family TEXT NOT NULL,
                profile TEXT NOT NULL,
                model_id TEXT NOT NULL,
                input_timeframe TEXT NOT NULL,
                training_timeframe TEXT NOT NULL,
                kind TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                blocked_reason TEXT NOT NULL,
                miss_class TEXT NOT NULL,
                theoretical_pips REAL NOT NULL,
                outcome_delay_sec REAL NOT NULL,
                entry_time TEXT NOT NULL,
                exit_time TEXT NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                exit_bid REAL NOT NULL,
                exit_ask REAL NOT NULL,
                pip REAL NOT NULL,
                entry_spread_pips REAL NOT NULL,
                max_favorable_pips REAL NOT NULL,
                max_adverse_pips REAL NOT NULL,
                first_positive_sec REAL,
                path_samples INTEGER NOT NULL,
                positive_path_samples INTEGER NOT NULL,
                configured_stop_pips REAL NOT NULL,
                configured_target_r REAL NOT NULL,
                configured_stop_hit_sec REAL,
                configured_target_hit_sec REAL,
                diagnostics_json TEXT NOT NULL,
                UNIQUE(event_id, horizon_sec)
            );
            CREATE INDEX IF NOT EXISTS canonical_outcomes_scope
                ON canonical_outcomes(horizon_sec, kind, family, instrument);
            CREATE INDEX IF NOT EXISTS canonical_outcomes_time
                ON canonical_outcomes(observed_utc);

            CREATE TABLE IF NOT EXISTS forecast_integrity_events (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                observed_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                reason TEXT NOT NULL,
                details_json TEXT NOT NULL,
                UNIQUE(event_id, horizon_sec, event_type)
            );
            CREATE INDEX IF NOT EXISTS forecast_integrity_time
                ON forecast_integrity_events(observed_utc);

            CREATE TRIGGER IF NOT EXISTS canonical_forecasts_no_update
            BEFORE UPDATE ON canonical_forecasts
            BEGIN SELECT RAISE(ABORT, 'canonical forecasts are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS canonical_forecasts_no_delete
            BEFORE DELETE ON canonical_forecasts
            BEGIN SELECT RAISE(ABORT, 'canonical forecasts are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS canonical_outcomes_no_update
            BEFORE UPDATE ON canonical_outcomes
            BEGIN SELECT RAISE(ABORT, 'canonical outcomes are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS canonical_outcomes_no_delete
            BEFORE DELETE ON canonical_outcomes
            BEGIN SELECT RAISE(ABORT, 'canonical outcomes are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS forecast_integrity_no_update
            BEFORE UPDATE ON forecast_integrity_events
            BEGIN SELECT RAISE(ABORT, 'forecast integrity events are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS forecast_integrity_no_delete
            BEFORE DELETE ON forecast_integrity_events
            BEGIN SELECT RAISE(ABORT, 'forecast integrity events are immutable'); END;
            """
        )
        existing_columns = {
            str(row[1]) for row in self.connection.execute("PRAGMA table_info(outcomes)")
        }
        migrations = {
            "entry_spread_pips": (
                "ALTER TABLE outcomes ADD COLUMN entry_spread_pips REAL NOT NULL DEFAULT 0"
            ),
            "first_positive_sec": (
                "ALTER TABLE outcomes ADD COLUMN first_positive_sec REAL"
            ),
            "positive_path_samples": (
                "ALTER TABLE outcomes ADD COLUMN positive_path_samples INTEGER NOT NULL DEFAULT 0"
            ),
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                self.connection.execute(statement)
        self.connection.commit()
        self.pending: list[tuple[Any, ...]] = []
        self.forecast_pending: list[tuple[Any, ...]] = []
        self.integrity_pending: list[tuple[Any, ...]] = []
        self.committed_rows = 0
        self.committed_forecasts = 0
        self.committed_canonical_outcomes = 0
        self.committed_integrity_events = 0
        self.lock_retries = 0
        self.last_storage_error = ""
        self.last_flush_monotonic = time.monotonic()
        self.next_flush_attempt_monotonic = 0.0

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)

    @staticmethod
    def _liquidity_bucket(spread_pips: Any) -> str:
        spread = finite(spread_pips, math.inf)
        if spread <= 2.0:
            return "liquid_le_2"
        if spread <= 3.0:
            return "normal_2_to_3"
        if spread <= 5.0:
            return "elevated_3_to_5"
        return "wide_gt_5"

    @staticmethod
    def _session_bucket(value: Any) -> str:
        text = str(value or "").strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return "unknown"
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        hour = parsed.astimezone(timezone.utc).hour
        if hour < 7:
            return "asia"
        if hour < 13:
            return "london"
        if hour < 17:
            return "overlap"
        if hour < 22:
            return "new_york"
        return "rollover"

    def observe_forecast(
        self,
        payload: dict[str, Any],
        *,
        horizons: Any,
        track_outcome: bool,
    ) -> None:
        """Persist one forecast before it is admitted to the maturity queue."""

        payload = dict(payload)
        event_id = str(payload.get("id") or payload.get("event_id") or "")
        if not event_id:
            raise ValueError("canonical forecast requires event_id")
        entry_text = str(payload.get("entry_time") or "")
        cutoff_text = str(payload.get("data_cutoff_utc") or entry_text)
        future_cutoff = False
        try:
            entry_time = datetime.fromisoformat(entry_text.replace("Z", "+00:00"))
            cutoff_time = datetime.fromisoformat(cutoff_text.replace("Z", "+00:00"))
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
            if cutoff_time.tzinfo is None:
                cutoff_time = cutoff_time.replace(tzinfo=timezone.utc)
            future_cutoff = cutoff_time > entry_time
        except ValueError:
            future_cutoff = bool(cutoff_text and entry_text and cutoff_text > entry_text)
        if future_cutoff:
            payload["kind"] = "miss"
            payload["blocked_reason"] = "future_data_cutoff"
            payload["miss_class"] = "timestamp_violation"
            payload["account_eligible"] = False
            payload["can_place_orders"] = False
            track_outcome = False
        safe_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"created_monotonic", "last_path_sample_monotonic"}
        }
        if track_outcome:
            # Preserve every informative entry-time observation while omitting
            # mechanically empty placeholders. The canonical maturity worker
            # treats absent path state as zero and does not consume disabled
            # model stubs, so this is a lossless representation change.
            safe_payload = {
                key: value
                for key, value in safe_payload.items()
                if value is not None and value != {}
            }
            microstructure = safe_payload.get("microstructure_at_entry")
            if isinstance(microstructure, dict):
                depth_is_informative = any(
                    finite(microstructure.get(key)) > 0.0
                    for key in (
                        "order_book_available",
                        "position_book_available",
                        "bid_total_liquidity",
                        "ask_total_liquidity",
                        "depth_total_liquidity",
                    )
                )
                if not depth_is_informative:
                    compact_microstructure = {
                        key: value
                        for key, value in microstructure.items()
                        if key
                        in {
                            "quote_receive_age_sec",
                            "live_spread_pips",
                            "current_volume",
                            "volume_ratio_12",
                            "volume_ratio_30",
                        }
                        and value is not None
                    }
                    if compact_microstructure:
                        safe_payload["microstructure_at_entry"] = compact_microstructure
                    else:
                        safe_payload.pop("microstructure_at_entry", None)
            sma_filter = safe_payload.get("sma_filter")
            if (
                isinstance(sma_filter, dict)
                and not bool(sma_filter.get("ready"))
                and str(sma_filter.get("reason") or "")
                == "sma_filter_model_disabled"
            ):
                safe_payload.pop("sma_filter", None)
            safe_payload["storage_contract"] = "compact_maturing_forecast_v2"
        if not track_outcome:
            # Rejected forecasts are still immutable causal evidence, but they
            # do not need the mutable path/maturity payload used for restart
            # recovery. Keep the complete normalized contract and the actual
            # forecast-time diagnostics while dropping duplicated zero-filled
            # and path-state structures. This prevents hard rejects from
            # dominating ledger growth without losing rejection evidence.
            retained = {
                "id",
                "cohort_id",
                "forecast_contract_version",
                "cost_model_version",
                "training_cutoff_utc",
                "training_dataset_sha256",
                "input_timeframe",
                "training_timeframe",
                "entry_time",
                "expected_signed_pips",
                "expected_after_spread_pips",
                "stop_loss_pips",
                "take_profit_r",
                "volatility_regime",
                "research_only",
                "account_eligible",
                "can_place_orders",
                "provider_quote_time",
                "proof_diagnostics",
            }
            safe_payload = {key: value for key, value in safe_payload.items() if key in retained}
            safe_payload = {key: value for key, value in safe_payload.items() if value is not None}
            diagnostics = payload.get("forecast_diagnostics")
            if isinstance(diagnostics, dict):
                blockers = diagnostics.get("blockers")
                economic = diagnostics.get("economic_gates")
                compact_diagnostics: dict[str, Any] = {}
                if isinstance(blockers, list) and blockers:
                    compact_diagnostics["blockers"] = [str(value) for value in blockers[:8]]
                if isinstance(economic, dict):
                    compact_diagnostics["economic_gates"] = {
                        key: economic[key]
                        for key in (
                            "spread_pips",
                            "signal_to_spread",
                            "reward_to_spread",
                            "atr_pips",
                        )
                        if key in economic
                    }
                if compact_diagnostics:
                    safe_payload["rejection_diagnostics"] = compact_diagnostics
            # All omitted values are retained in typed canonical_forecasts
            # columns. Keeping only the non-column contract fields here cuts
            # high-frequency reject storage without deleting or aggregating a
            # single causal decision. The event id and entry time remain in
            # the JSON so payload hashes retain observation identity.
            safe_payload["storage_contract"] = "compact_nonmaturing_forecast_v4"
        horizon_values = sorted({int(finite(value)) for value in horizons if finite(value) > 0})
        if track_outcome:
            safe_payload["remaining_horizons"] = horizon_values
            safe_payload["track_outcome"] = True
        if future_cutoff:
            for horizon_sec in horizon_values or [0]:
                self.integrity_pending.append(
                    (
                        event_id,
                        int(horizon_sec),
                        utc_now(),
                        "forecast_timestamp_violation",
                        "data cutoff occurs after forecast entry time",
                        self._json(
                            {
                                "entry_time": entry_text,
                                "data_cutoff_utc": cutoff_text,
                                "policy": "fail_closed_not_matured",
                            }
                        ),
                    )
                )
        forecast_json = self._json(safe_payload)
        payload_hash = hashlib.sha256(forecast_json.encode("utf-8")).hexdigest()
        probability_up = payload.get("probability_up")
        predicted_magnitude = payload.get("predicted_magnitude_pips")
        self.forecast_pending.append(
            (
                event_id,
                utc_now(),
                str(payload.get("model_version") or "unknown"),
                str(payload.get("feature_version") or "unknown"),
                str(payload.get("data_cutoff_utc") or payload.get("entry_time") or ""),
                str(payload.get("lane_id") or ""),
                str(payload.get("family") or ""),
                str(payload.get("profile") or ""),
                str(payload.get("model_id") or ""),
                str(payload.get("kind") or ""),
                str(payload.get("instrument") or ""),
                str(payload.get("direction") or ""),
                self._json(horizon_values),
                str(payload.get("entry_time") or ""),
                finite(payload.get("entry_bid")),
                finite(payload.get("entry_ask")),
                finite(payload.get("pip")),
                finite(payload.get("entry_spread_pips")),
                str(payload.get("session_bucket") or self._session_bucket(payload.get("entry_time"))),
                str(
                    payload.get("liquidity_bucket")
                    or self._liquidity_bucket(payload.get("entry_spread_pips"))
                ),
                None if predicted_magnitude is None else finite(predicted_magnitude),
                None if probability_up is None else finite(probability_up),
                str(payload.get("blocked_reason") or ""),
                str(payload.get("miss_class") or ""),
                int(bool(track_outcome)),
                forecast_json,
                payload_hash,
            )
        )
        self.flush()

    def observe_integrity_event(
        self,
        *,
        event_id: str,
        horizon_sec: Any,
        event_type: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.integrity_pending.append(
            (
                str(event_id),
                int(finite(horizon_sec)),
                utc_now(),
                str(event_type),
                str(reason),
                self._json(details or {}),
            )
        )
        self.flush()

    def recover_pending(
        self,
        horizons: Any,
        *,
        horizon_mode: str = "all",
    ) -> list[dict[str, Any]]:
        """Recover still-maturable forecasts after a worker restart."""

        horizon_values = sorted({int(finite(value)) for value in horizons if finite(value) > 0})
        if not horizon_values:
            return []
        now_utc = datetime.now(timezone.utc)
        now_monotonic = time.monotonic()
        maximum_age = max(horizon_values) + 120.0
        cutoff_utc = (now_utc - timedelta(seconds=maximum_age)).isoformat()
        # Perform one exact, entry-time-correct migration for pre-upgrade rows.
        # The existing scope index is larger than a cutoff-window scan but the
        # live 32-GB ledger audit completes it in roughly 38 seconds, safely
        # below the worker's 900-second startup allowance.  Once committed,
        # every new tracked forecast maintains the index transactionally.
        migration_id = "canonical_recovery_entry_time_exact_v2_20260817"
        migration_maximum_age = max(86_400.0, max(horizon_values)) + 120.0
        migration_cutoff_utc = (
            now_utc - timedelta(seconds=migration_maximum_age)
        ).isoformat()
        # Acquire ownership before checking the marker. A concurrent first
        # opener may hold this lock for the one-time exact scan, so retry past
        # the ordinary five-second busy timeout and re-read inside the lock.
        migration_lock_acquired = False
        for attempt in range(12):
            try:
                self.connection.execute("BEGIN IMMEDIATE")
                migration_lock_acquired = True
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 11:
                    raise
                time.sleep(0.25)
        if not migration_lock_acquired:
            raise sqlite3.OperationalError("recovery migration lock unavailable")
        try:
            migrated = self.connection.execute(
                """
                SELECT entry_cutoff_utc, inserted_rows
                FROM canonical_recovery_migrations WHERE migration_id=?
                """,
                (migration_id,),
            ).fetchone()
            coverage_complete = bool(
                migrated is not None
                and str(migrated[0]) <= migration_cutoff_utc
            )
            if not coverage_complete:
                before_migration = self.connection.total_changes
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO canonical_recovery_index_v2 (
                        event_id, entry_time, payload_sha256
                    )
                    SELECT event_id, entry_time, payload_sha256
                    FROM canonical_forecasts
                        INDEXED BY canonical_forecasts_scope
                    WHERE track_outcome=1 AND entry_time >= ?
                    """,
                    (migration_cutoff_utc,),
                )
                inserted_rows = self.connection.total_changes - before_migration
                if migrated is None:
                    self.connection.execute(
                        """
                        INSERT INTO canonical_recovery_migrations (
                            migration_id, completed_utc, entry_cutoff_utc,
                            inserted_rows
                        ) VALUES (?,?,?,?)
                        """,
                        (
                            migration_id, utc_now(), migration_cutoff_utc,
                            inserted_rows,
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE canonical_recovery_migrations
                        SET completed_utc=?, entry_cutoff_utc=?, inserted_rows=?
                        WHERE migration_id=?
                        """,
                        (
                            utc_now(), migration_cutoff_utc,
                            int(migrated[1]) + inserted_rows, migration_id,
                        ),
                    )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

        # Forecast membership and matured outcomes must be read from one
        # explicit WAL snapshot; otherwise outcome commits between 500-ID
        # batches could make recovery depend on batch order.
        self.connection.execute("BEGIN")
        try:
            rows = self.connection.execute(
                """
                SELECT f.event_id, f.entry_time, f.horizons_json,
                       f.forecast_json
                FROM canonical_recovery_index_v2 AS recovery
                    INDEXED BY canonical_recovery_index_v2_entry_time
                JOIN canonical_forecasts AS f
                  ON f.event_id=recovery.event_id
                 AND f.entry_time=recovery.entry_time
                 AND f.payload_sha256=recovery.payload_sha256
                WHERE recovery.entry_time >= ? AND f.track_outcome=1
                ORDER BY recovery.entry_time DESC
                """,
                (cutoff_utc,),
            ).fetchall()
            matured_by_event: dict[str, set[int]] = {}
            event_ids = [str(row[0]) for row in rows]
            # Stay well below SQLite's common 999-variable limit.  Each lookup
            # is served by the immutable (event_id, horizon_sec) unique index.
            for start in range(0, len(event_ids), 500):
                batch = event_ids[start : start + 500]
                placeholders = ",".join("?" for _ in batch)
                for event_id, horizon_sec in self.connection.execute(
                    f"""
                    SELECT event_id, horizon_sec
                    FROM canonical_outcomes
                    WHERE event_id IN ({placeholders})
                    """,
                    batch,
                ):
                    matured_by_event.setdefault(str(event_id), set()).add(
                        int(horizon_sec)
                    )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        recovered: list[dict[str, Any]] = []
        for event_id, entry_time, horizons_json, forecast_json in rows:
            try:
                entry = datetime.fromisoformat(str(entry_time).replace("Z", "+00:00"))
                if entry.tzinfo is None:
                    entry = entry.replace(tzinfo=timezone.utc)
                age = max(0.0, (now_utc - entry.astimezone(timezone.utc)).total_seconds())
            except ValueError:
                continue
            if age > maximum_age:
                continue
            try:
                stored_horizons = [int(value) for value in json.loads(horizons_json)]
                item = json.loads(forecast_json)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if horizon_mode == "declared" and stored_horizons:
                diagnostics = item.get("forecast_diagnostics") or {}
                signal = diagnostics.get("signal") or {}
                reference = int(
                    finite(
                        item.get("signal_reference_horizon_sec"),
                        finite(
                            signal.get("reference_horizon_sec"),
                            60
                            if signal.get("expected_signed_move_pips") is not None
                            else 300,
                        ),
                    )
                )
                stored_horizons = [
                    min(stored_horizons, key=lambda value: abs(int(value) - reference))
                ]
            matured = matured_by_event.get(str(event_id), set())
            remaining: list[int] = []
            for value in stored_horizons:
                if value not in horizon_values or value in matured:
                    continue
                if age <= value + 120.0:
                    remaining.append(value)
                    continue
                self.integrity_pending.append(
                    (
                        str(event_id),
                        int(value),
                        utc_now(),
                        "restart_outcome_window_missed",
                        "worker_restart_exceeded_horizon_grace",
                        self._json(
                            {
                                "entry_time": str(entry_time),
                                "age_sec": round(age, 3),
                                "horizon_sec": int(value),
                            }
                        ),
                    )
                )
            if not remaining:
                continue
            item["remaining_horizons"] = remaining
            item["created_monotonic"] = now_monotonic - age
            item["recovered_from_ledger"] = True
            item.setdefault("max_favorable_pips", 0.0)
            item.setdefault("max_adverse_pips", 0.0)
            item.setdefault("first_positive_sec", None)
            item.setdefault("path_samples", 0)
            item.setdefault("positive_path_samples", 0)
            if str(item.get("kind") or "") == "signal":
                item.setdefault("favorable_hits", {})
                item.setdefault("adverse_hits", {})
            recovered.append(item)
        self.flush()
        return recovered

    def observe(self, payload: dict[str, Any]) -> None:
        diagnostics = {
            key: value
            for key, value in payload.items()
            if key in self.OUTCOME_DIAGNOSTIC_KEYS
        }
        diagnostics["storage_contract"] = "compact_outcome_diagnostics_v1"
        stop_hit = payload.get("configured_stop_hit_sec")
        target_hit = payload.get("configured_target_hit_sec")
        first_positive = payload.get("first_positive_sec")
        self.pending.append(
            (
                str(payload.get("id") or payload.get("event_id") or ""),
                int(finite(payload.get("horizon_sec"))),
                utc_now(),
                str(payload.get("lane_id") or ""),
                str(payload.get("family") or ""),
                str(payload.get("profile") or ""),
                str(payload.get("model_id") or ""),
                str(payload.get("input_timeframe") or ""),
                str(payload.get("training_timeframe") or ""),
                str(payload.get("kind") or ""),
                str(payload.get("instrument") or ""),
                str(payload.get("direction") or ""),
                str(payload.get("blocked_reason") or ""),
                str(payload.get("miss_class") or ""),
                finite(payload.get("theoretical_pips")),
                finite(payload.get("outcome_delay_sec")),
                str(payload.get("entry_time") or ""),
                str(payload.get("exit_time") or ""),
                finite(payload.get("entry_bid")),
                finite(payload.get("entry_ask")),
                finite(payload.get("exit_bid")),
                finite(payload.get("exit_ask")),
                finite(payload.get("pip")),
                finite(payload.get("entry_spread_pips")),
                finite(payload.get("max_favorable_pips")),
                finite(payload.get("max_adverse_pips")),
                None if first_positive is None else finite(first_positive),
                int(finite(payload.get("path_samples"))),
                int(finite(payload.get("positive_path_samples"))),
                finite(payload.get("configured_stop_pips")),
                finite(payload.get("configured_target_r")),
                None if stop_hit is None else finite(stop_hit),
                None if target_hit is None else finite(target_hit),
                self._json(diagnostics),
            )
        )
        self.flush()

    def flush(self, *, force: bool = False) -> int:
        if not self.pending and not self.forecast_pending and not self.integrity_pending:
            return 0
        now = time.monotonic()
        if not force and now < self.next_flush_attempt_monotonic:
            return 0
        elapsed = now - self.last_flush_monotonic
        buffered = len(self.pending) + len(self.forecast_pending) + len(self.integrity_pending)
        if not force and buffered < self.batch_size and elapsed < self.flush_sec:
            return 0
        before_outcomes = self.connection.total_changes
        placeholders = ",".join("?" for _ in self.COLUMNS)
        try:
            if self.pending:
                self.connection.executemany(
                    f"""
                    INSERT OR IGNORE INTO outcomes ({",".join(self.COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    self.pending,
                )
            inserted_outcomes = self.connection.total_changes - before_outcomes
            before_canonical = self.connection.total_changes
            if self.pending:
                self.connection.executemany(
                    f"""
                    INSERT OR IGNORE INTO canonical_outcomes ({",".join(self.COLUMNS)})
                    VALUES ({placeholders})
                    """,
                    self.pending,
                )
            inserted_canonical = self.connection.total_changes - before_canonical
            before_forecasts = self.connection.total_changes
            if self.forecast_pending:
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO canonical_forecasts (
                        event_id, recorded_utc, model_version, feature_version,
                        data_cutoff_utc, lane_id, family, profile, model_id, kind,
                        instrument, direction, horizons_json, entry_time, entry_bid,
                        entry_ask, pip, entry_spread_pips, session_bucket,
                        liquidity_bucket, predicted_magnitude_pips, probability_up,
                        blocked_reason, miss_class, track_outcome, forecast_json,
                        payload_sha256
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    self.forecast_pending,
                )
            inserted_forecasts = self.connection.total_changes - before_forecasts
            if self.forecast_pending:
                event_ids = [str(row[0]) for row in self.forecast_pending]
                for start in range(0, len(event_ids), 500):
                    batch = event_ids[start : start + 500]
                    placeholders = ",".join("?" for _ in batch)
                    self.connection.execute(
                        f"""
                        INSERT OR IGNORE INTO canonical_recovery_index_v2 (
                            event_id, entry_time, payload_sha256
                        )
                        SELECT event_id, entry_time, payload_sha256
                        FROM canonical_forecasts
                        WHERE track_outcome=1
                          AND event_id IN ({placeholders})
                        """,
                        batch,
                    )
            before_integrity = self.connection.total_changes
            if self.integrity_pending:
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO forecast_integrity_events (
                        event_id, horizon_sec, observed_utc, event_type, reason,
                        details_json
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    self.integrity_pending,
                )
            inserted_integrity = self.connection.total_changes - before_integrity
            self.connection.commit()
        except sqlite3.OperationalError as exc:
            self.connection.rollback()
            if "locked" not in str(exc).lower():
                raise
            self.lock_retries += 1
            self.last_storage_error = str(exc)
            self.next_flush_attempt_monotonic = time.monotonic() + max(
                1.0,
                self.busy_timeout_ms / 1000.0,
            )
            if force:
                raise
            return 0
        self.committed_rows += max(0, inserted_outcomes)
        self.committed_canonical_outcomes += max(0, inserted_canonical)
        self.committed_forecasts += max(0, inserted_forecasts)
        self.committed_integrity_events += max(0, inserted_integrity)
        self.pending.clear()
        self.forecast_pending.clear()
        self.integrity_pending.clear()
        self.last_storage_error = ""
        self.next_flush_attempt_monotonic = 0.0
        self.last_flush_monotonic = time.monotonic()
        return inserted_outcomes

    def summary(self) -> dict[str, Any]:
        retry_after_sec = max(
            0.0,
            self.next_flush_attempt_monotonic - time.monotonic(),
        )
        return {
            "database": str(self.database_path.resolve()),
            "buffered_rows": len(self.pending),
            "buffered_forecasts": len(self.forecast_pending),
            "buffered_integrity_events": len(self.integrity_pending),
            "committed_rows": self.committed_rows,
            "committed_forecasts": self.committed_forecasts,
            "committed_canonical_outcomes": self.committed_canonical_outcomes,
            "committed_integrity_events": self.committed_integrity_events,
            "batch_size": self.batch_size,
            "flush_sec": self.flush_sec,
            "busy_timeout_ms": self.busy_timeout_ms,
            "lock_retries": self.lock_retries,
            "last_storage_error": self.last_storage_error,
            "retry_after_sec": round(retry_after_sec, 3),
        }

    def close(self) -> None:
        try:
            self.flush(force=True)
        finally:
            self.connection.close()


__all__ = ["ShadowOutcomeStore", "sampled_detail"]
