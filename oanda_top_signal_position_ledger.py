#!/usr/bin/env python3
"""Persist a no-order what-if position ledger for the top signal at each horizon."""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from oanda_quote_transport import load_quote_snapshot
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_quote_transport import load_quote_snapshot


UTC = timezone.utc
ROOT = Path(__file__).resolve().parent
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_SIGNALS = STATE_ROOT / "practice_007_signal_snapshot_research_v1.json"
DEFAULT_QUOTES = STATE_ROOT / "practice_007_market_quotes_v1.json"
DEFAULT_DATABASE = STATE_ROOT / "top_signal_position_ledger_v1.sqlite"
DEFAULT_STATE = STATE_ROOT / "top_signal_position_ledger_v1.json"
DEFAULT_MAX_QUOTE_AGE_SEC = 90.0
MAX_TARGET_QUOTE_DISTANCE_SEC = 60.0
FACTOR_EPISODE_GAP_SEC = 900.0
MEASUREMENT_VERSION = "raw_all_signal_consensus_v5_strict_horizon_lineage"
PREVIOUS_MEASUREMENT_VERSION = "raw_all_signal_consensus_v4_horizon_lineage"
OLDER_MEASUREMENT_VERSION = "raw_all_signal_consensus_v3_exact_target_quote"
LEGACY_MEASUREMENT_VERSION = "legacy_filtered_direction_v1"
AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID = "all_signal_horizon_lineage_v1"
CANONICAL_HORIZONS = (
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    10800,
    14400,
    21600,
    28800,
    43200,
    86400,
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, allow_nan=False, default=str),
            encoding="utf-8",
        )
        for attempt in range(20):
            try:
                temporary.replace(path)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                # Windows readers can briefly deny destination replacement.
                # Retry the same complete temporary file instead of killing
                # the continuously published position-ledger worker.
                time.sleep(min(0.05 * (attempt + 1), 0.25))
    finally:
        temporary.unlink(missing_ok=True)


def horizon_label(horizon_sec: int) -> str:
    horizon = int(horizon_sec)
    if horizon == 86400:
        return "D1"
    if horizon >= 3600 and horizon % 3600 == 0:
        return f"H{horizon // 3600}"
    if horizon >= 60 and horizon % 60 == 0:
        return f"M{horizon // 60}"
    return f"{horizon}s"


def display_side(direction: str) -> str:
    return "Long" if str(direction).lower() in {"buy", "long"} else "Short"


def liquidity_cost_bucket(spread_pips: float) -> str:
    spread = safe_float(spread_pips, math.inf)
    if spread <= 2.0:
        return "liquid_le_2"
    if spread <= 3.0:
        return "normal_2_to_3"
    if spread <= 5.0:
        return "elevated_3_to_5"
    return "wide_gt_5"


def quote_map(
    payload: dict[str, Any],
    *,
    now_epoch: float | None = None,
    max_age_sec: float = DEFAULT_MAX_QUOTE_AGE_SEC,
) -> dict[str, dict[str, Any]]:
    generated = str(payload.get("generated_utc") or "").strip()
    try:
        generated_epoch = datetime.fromisoformat(
            generated.replace("Z", "+00:00")
        ).timestamp()
    except (TypeError, ValueError):
        return {}
    current_epoch = time.time() if now_epoch is None else now_epoch
    if current_epoch - generated_epoch > max(0.0, max_age_sec):
        return {}
    quotes = payload.get("quotes") or payload.get("market_quotes") or {}
    return quotes if isinstance(quotes, dict) else {}


def candidate_rank(candidate: dict[str, Any]) -> tuple[Any, ...]:
    return (
        bool(candidate.get("signal_eligible")),
        bool(candidate.get("validated")),
        bool(candidate.get("paper_consensus_eligible")),
        not bool(candidate.get("direction_conflict")),
        safe_float(candidate.get("signal_confidence")) - 0.5,
        safe_float(candidate.get("projected_net_pips_per_hour")),
        safe_float(candidate.get("projected_net_pips")),
        int(candidate.get("family_count") or 0),
    )


def candidates_by_horizon(
    snapshot: dict[str, Any],
) -> dict[int, list[dict[str, Any]]]:
    """Return every reconstructed candidate, ranked within its direct horizon.

    Keeping this extraction separate from the one-winner display policy lets
    research ledgers compare top-one and diversified top-three cohorts without
    changing the production signal ranking or execution path.
    """
    by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for signal in snapshot.get("top_signals") or []:
        if not isinstance(signal, dict):
            continue
        instrument = str(signal.get("instrument") or "")
        if not instrument:
            continue
        parent_direction = str(signal.get("direction") or "").lower()
        for point in signal.get("horizon_breakdown") or []:
            if not isinstance(point, dict):
                continue
            horizon = int(safe_float(point.get("horizon_sec")))
            if horizon not in CANONICAL_HORIZONS:
                continue
            validation = (
                point.get("execution_validation")
                or signal.get("execution_validation")
                or {}
            )
            filtered_direction = str(
                point.get("direction") or parent_direction
            ).lower()
            raw_signed = safe_float(point.get("raw_ensemble_signed_net_pips"))
            raw_probability_up = safe_float(
                point.get("raw_probability_up"),
                0.5,
            )
            raw_fields_available = bool(
                "raw_ensemble_signed_net_pips" in point
                or "raw_probability_up" in point
            )
            if abs(raw_signed) >= 0.05:
                raw_direction = "buy" if raw_signed > 0.0 else "sell"
            elif raw_probability_up >= 0.505:
                raw_direction = "buy"
            elif raw_probability_up <= 0.495:
                raw_direction = "sell"
            else:
                raw_direction = "neutral"
            point_eligible = bool(point.get("signal_eligible"))
            direction = (
                filtered_direction
                if point_eligible or not raw_fields_available
                else raw_direction
            )
            if direction not in {"buy", "sell", "long", "short"}:
                continue
            direction = "buy" if direction in {"buy", "long"} else "sell"
            raw_side_confidence = (
                raw_probability_up
                if direction == "buy"
                else 1.0 - raw_probability_up
            )
            raw_side_net_pips = (
                raw_signed if direction == "buy" else -raw_signed
            )
            displayed_confidence = safe_float(
                point.get("signal_confidence"),
                safe_float(signal.get("signal_confidence"), 0.5),
            )
            displayed_projection = safe_float(point.get("projected_net_pips"))
            displayed_projection_per_hour = safe_float(
                point.get("projected_net_pips_per_hour")
            )
            direction_source = "filtered_execution_consensus"
            if not point_eligible and raw_fields_available:
                displayed_confidence = raw_side_confidence
                displayed_projection = raw_side_net_pips
                displayed_projection_per_hour = (
                    raw_side_net_pips * 3600.0 / max(1, horizon)
                )
                direction_source = "raw_all_signal_consensus"
            elif not raw_fields_available:
                direction_source = "legacy_snapshot_fallback"
            blocked = list(point.get("signal_blocked_by") or [])
            if not blocked:
                blocked = list(signal.get("signal_blocked_by") or [])
            source_signal_id = str(signal.get("id") or "")
            aggregate_signal_id = str(
                point.get("aggregate_signal_id")
                or signal.get("aggregate_signal_id")
                or ""
            )
            signal_lineage_contract_id = str(
                point.get("aggregate_signal_lineage_contract_id")
                or signal.get("aggregate_signal_lineage_contract_id")
                or "legacy_source_signal_id"
            )
            strict_lineage_available = bool(
                aggregate_signal_id.startswith("aggregate_signal_")
                and signal_lineage_contract_id
                == AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID
            )
            if not aggregate_signal_id:
                aggregate_signal_id = source_signal_id
            candidate = {
                "signal_id": aggregate_signal_id,
                "source_signal_id": source_signal_id,
                "signal_lineage_contract_id": signal_lineage_contract_id,
                "instrument": instrument,
                "direction": direction,
                "side": display_side(direction),
                "horizon_sec": horizon,
                "horizon_label": horizon_label(horizon),
                "family": str(point.get("best_family") or signal.get("family") or ""),
                "lane_id": str(point.get("best_model_id") or signal.get("lane_id") or ""),
                "input_timeframe": str(point.get("best_input_timeframe") or ""),
                "signal_confidence": displayed_confidence,
                "projected_net_pips": displayed_projection,
                "projected_net_pips_per_hour": displayed_projection_per_hour,
                "gross_to_spread": safe_float(point.get("gross_to_spread")),
                "all_contributor_gross_to_spread": safe_float(
                    point.get("all_contributor_gross_to_spread"),
                    safe_float(point.get("gross_to_spread")),
                ),
                "directional_gross_to_spread": (
                    safe_float(point.get("directional_gross_to_spread"))
                    if "directional_gross_to_spread" in point
                    else None
                ),
                "spread_pips": safe_float(
                    point.get("spread_pips"),
                    safe_float(signal.get("spread_pips")),
                ),
                "liquidity_quality": safe_float(
                    point.get("liquidity_quality"),
                    safe_float(signal.get("liquidity_quality")),
                ),
                "raw_ensemble_aligned_weight_pct": safe_float(
                    point.get("raw_ensemble_aligned_weight_pct")
                ),
                "family_count": int(safe_float(point.get("family_count"))),
                "component_count": int(safe_float(point.get("component_count"))),
                "signal_eligible": point_eligible,
                "paper_consensus_eligible": bool(
                    point.get("paper_consensus_eligible")
                ),
                "validated": bool(validation.get("validated")),
                "direction_conflict": bool(
                    point.get(
                        "direction_conflict",
                        signal.get("direction_conflict"),
                    )
                ),
                "direction_conflict_contract_id": str(
                    point.get("direction_conflict_contract_id")
                    or signal.get("direction_conflict_contract_id")
                    or "legacy_direction_conflict"
                ),
                "direction_conflict_details": {
                    "contributor_count": int(
                        point.get("direction_conflict_contributor_count")
                        or signal.get("direction_conflict_contributor_count")
                        or 0
                    ),
                    "account_eligible_count": int(
                        point.get("direction_conflict_account_eligible_count")
                        or signal.get("direction_conflict_account_eligible_count")
                        or 0
                    ),
                    "execution_component_count": int(
                        point.get(
                            "direction_conflict_execution_component_count"
                        )
                        or signal.get(
                            "direction_conflict_execution_component_count"
                        )
                        or 0
                    ),
                    "shadow_only_count": int(
                        point.get("direction_conflict_shadow_only_count")
                        or signal.get("direction_conflict_shadow_only_count")
                        or 0
                    ),
                    "only_shadow_or_account_ineligible": bool(
                        point.get(
                            "direction_conflict_only_shadow_or_account_ineligible"
                        )
                        or signal.get(
                            "direction_conflict_only_shadow_or_account_ineligible"
                        )
                    ),
                    "contributors": list(
                        point.get("direction_conflict_contributors")
                        or signal.get("direction_conflict_contributors")
                        or []
                    ),
                },
                "blocked_by": blocked,
                "execution_validation": dict(validation)
                if isinstance(validation, dict)
                else {},
                "negative_historical_warmup": bool(
                    validation.get("negative_historical_warmup")
                )
                if isinstance(validation, dict)
                else False,
                "promotion_rejected": bool(validation.get("promotion_rejected"))
                if isinstance(validation, dict)
                else False,
                "measurement_version": (
                    LEGACY_MEASUREMENT_VERSION
                    if direction_source == "legacy_snapshot_fallback"
                    else MEASUREMENT_VERSION
                    if strict_lineage_available
                    else PREVIOUS_MEASUREMENT_VERSION
                ),
                "direction_source": direction_source,
                "strict_lineage_available": strict_lineage_available,
            }
            aggressive_shadow = (
                (
                    direction_source == "raw_all_signal_consensus"
                    or (
                        candidate["signal_eligible"]
                        and candidate["direction_conflict"]
                    )
                )
                and candidate["signal_confidence"] >= 0.52
                and candidate["projected_net_pips"] >= 0.25
                and candidate["raw_ensemble_aligned_weight_pct"] >= 65.0
                and candidate["family_count"] >= 2
            )
            candidate["policy_state"] = (
                "executable"
                if candidate["signal_eligible"]
                and candidate["validated"]
                and not candidate["direction_conflict"]
                and not candidate["blocked_by"]
                else "conflicted_aggressive_shadow"
                if aggressive_shadow and candidate["direction_conflict"]
                else "aggressive_shadow"
                if aggressive_shadow
                else "diagnostic_shadow"
            )
            by_horizon[horizon].append(candidate)
    return {
        horizon: sorted(rows, key=candidate_rank, reverse=True)
        for horizon, rows in by_horizon.items()
        if rows
    }


def top_by_horizon(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    by_horizon = candidates_by_horizon(snapshot)
    return [
        by_horizon[horizon][0]
        for horizon in CANONICAL_HORIZONS
        if by_horizon.get(horizon)
    ]


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS positions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cohort_key TEXT NOT NULL UNIQUE,
            signal_id TEXT NOT NULL,
            source_signal_id TEXT NOT NULL DEFAULT '',
            signal_lineage_contract_id TEXT NOT NULL DEFAULT 'legacy_source_signal_id',
            observed_at TEXT NOT NULL,
            opened_epoch REAL NOT NULL,
            opened_at TEXT NOT NULL,
            target_epoch REAL NOT NULL,
            target_at TEXT NOT NULL,
            closed_epoch REAL,
            closed_at TEXT,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            side TEXT NOT NULL,
            horizon_sec INTEGER NOT NULL,
            horizon_label TEXT NOT NULL,
            family TEXT,
            lane_id TEXT,
            input_timeframe TEXT,
            policy_state TEXT NOT NULL,
            signal_eligible INTEGER NOT NULL,
            validated INTEGER NOT NULL,
            direction_conflict INTEGER NOT NULL,
            direction_conflict_contract_id TEXT NOT NULL DEFAULT 'legacy_direction_conflict',
            direction_conflict_details_json TEXT NOT NULL DEFAULT '{}',
            blocked_by_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            projected_net_pips REAL NOT NULL,
            projected_net_pips_per_hour REAL NOT NULL,
            gross_to_spread REAL NOT NULL,
            measurement_version TEXT NOT NULL DEFAULT 'legacy_filtered_direction_v1',
            direction_source TEXT NOT NULL DEFAULT 'legacy_filtered_direction',
            entry_bid REAL NOT NULL,
            entry_ask REAL NOT NULL,
            entry_mid REAL NOT NULL,
            entry_spread_pips REAL NOT NULL,
            exit_bid REAL,
            exit_ask REAL,
            exit_mid REAL,
            gross_pips REAL,
            net_pips REAL,
            win INTEGER,
            best_net_pips REAL,
            worst_net_pips REAL,
            time_to_positive_sec REAL,
            outcome_quote_epoch REAL,
            outcome_quote_at TEXT,
            target_quote_distance_sec REAL,
            maturity_valid INTEGER,
            maturity_reason TEXT,
            status TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS position_status_horizon
        ON positions(status, horizon_sec);
        CREATE INDEX IF NOT EXISTS position_closed_epoch
        ON positions(closed_epoch DESC);
        CREATE INDEX IF NOT EXISTS position_status_measurement_maturity
        ON positions(status, measurement_version, maturity_valid);
        CREATE INDEX IF NOT EXISTS position_status_maturity
        ON positions(status, maturity_valid);
        CREATE INDEX IF NOT EXISTS position_factor_episode_order
        ON positions(
            status, measurement_version, maturity_valid,
            policy_state, opened_epoch
        );
        CREATE INDEX IF NOT EXISTS position_policy_repair
        ON positions(policy_state, direction_conflict, measurement_version);
        CREATE INDEX IF NOT EXISTS position_recent_status_epoch
        ON positions(
            (CASE status WHEN 'open' THEN 0 ELSE 1 END),
            COALESCE(closed_epoch, target_epoch) DESC
        );
        """
    )
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(positions)")
    }
    if "measurement_version" not in columns:
        connection.execute(
            "ALTER TABLE positions ADD COLUMN measurement_version TEXT NOT NULL "
            f"DEFAULT '{LEGACY_MEASUREMENT_VERSION}'"
        )
    if "direction_source" not in columns:
        connection.execute(
            "ALTER TABLE positions ADD COLUMN direction_source TEXT NOT NULL "
            "DEFAULT 'legacy_filtered_direction'"
        )
    for column, definition in (
        ("source_signal_id", "TEXT NOT NULL DEFAULT ''"),
        (
            "signal_lineage_contract_id",
            "TEXT NOT NULL DEFAULT 'legacy_source_signal_id'",
        ),
        (
            "direction_conflict_contract_id",
            "TEXT NOT NULL DEFAULT 'legacy_direction_conflict'",
        ),
        ("direction_conflict_details_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("outcome_quote_epoch", "REAL"),
        ("outcome_quote_at", "TEXT"),
        ("target_quote_distance_sec", "REAL"),
        ("maturity_valid", "INTEGER"),
        ("maturity_reason", "TEXT"),
    ):
        if column not in columns:
            connection.execute(
                f"ALTER TABLE positions ADD COLUMN {column} {definition}"
            )
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS ledger_revision(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            position_revision INTEGER NOT NULL,
            metadata_revision INTEGER NOT NULL DEFAULT 0,
            mature_revision INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    revision_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(ledger_revision)")
    }
    for column in ("metadata_revision", "mature_revision"):
        if column not in revision_columns:
            connection.execute(
                f"ALTER TABLE ledger_revision ADD COLUMN {column} "
                "INTEGER NOT NULL DEFAULT 0"
            )
    connection.executescript(
        """
        INSERT OR IGNORE INTO ledger_revision(
            singleton, position_revision, metadata_revision, mature_revision
        ) VALUES(1, 0, 0, 0);

        DROP TRIGGER IF EXISTS positions_revision_insert;
        DROP TRIGGER IF EXISTS positions_revision_delete;
        DROP TRIGGER IF EXISTS positions_revision_update;
        DROP TRIGGER IF EXISTS positions_metadata_revision_update;
        DROP TRIGGER IF EXISTS positions_mature_revision_update;

        CREATE TRIGGER positions_revision_insert
        AFTER INSERT ON positions
        BEGIN
            UPDATE ledger_revision
            SET position_revision=position_revision+1,
                metadata_revision=metadata_revision+1,
                mature_revision=mature_revision+
                    CASE WHEN NEW.status='matured' THEN 1 ELSE 0 END
            WHERE singleton=1;
        END;

        CREATE TRIGGER positions_revision_delete
        AFTER DELETE ON positions
        BEGIN
            UPDATE ledger_revision
            SET position_revision=position_revision+1,
                metadata_revision=metadata_revision+1,
                mature_revision=mature_revision+
                    CASE WHEN OLD.status='matured' THEN 1 ELSE 0 END
            WHERE singleton=1;
        END;

        CREATE TRIGGER positions_metadata_revision_update
        AFTER UPDATE OF status, measurement_version, maturity_valid
        ON positions
        WHEN
            OLD.status IS NOT NEW.status
            OR OLD.measurement_version IS NOT NEW.measurement_version
            OR OLD.maturity_valid IS NOT NEW.maturity_valid
        BEGIN
            UPDATE ledger_revision
            SET position_revision=position_revision+1,
                metadata_revision=metadata_revision+1
            WHERE singleton=1;
        END;

        CREATE TRIGGER positions_mature_revision_update
        AFTER UPDATE OF
            status, measurement_version, maturity_valid, policy_state,
            horizon_sec, horizon_label, opened_epoch, instrument, direction,
            win, gross_pips, net_pips, best_net_pips, worst_net_pips,
            entry_spread_pips
        ON positions
        WHEN
            (OLD.status='matured' OR NEW.status='matured')
            AND (
                OLD.status IS NOT NEW.status
                OR OLD.measurement_version IS NOT NEW.measurement_version
                OR OLD.maturity_valid IS NOT NEW.maturity_valid
                OR OLD.policy_state IS NOT NEW.policy_state
                OR OLD.horizon_sec IS NOT NEW.horizon_sec
                OR OLD.horizon_label IS NOT NEW.horizon_label
                OR OLD.opened_epoch IS NOT NEW.opened_epoch
                OR OLD.instrument IS NOT NEW.instrument
                OR OLD.direction IS NOT NEW.direction
                OR OLD.win IS NOT NEW.win
                OR OLD.gross_pips IS NOT NEW.gross_pips
                OR OLD.net_pips IS NOT NEW.net_pips
                OR OLD.best_net_pips IS NOT NEW.best_net_pips
                OR OLD.worst_net_pips IS NOT NEW.worst_net_pips
                OR OLD.entry_spread_pips IS NOT NEW.entry_spread_pips
            )
        BEGIN
            UPDATE ledger_revision
            SET position_revision=position_revision+1,
                mature_revision=mature_revision+1
            WHERE singleton=1;
        END;
        """
    )
    quarantine_legacy_maturities(connection)
    connection.commit()
    return connection


def quote_values(quotes: dict[str, dict[str, Any]], instrument: str) -> tuple[float, float, float] | None:
    quote = quotes.get(instrument) or {}
    bid = safe_float(quote.get("bid"))
    ask = safe_float(quote.get("ask"))
    pip = safe_float(quote.get("pip"), 0.01 if instrument.endswith("_JPY") else 0.0001)
    if bid <= 0.0 or ask <= bid or pip <= 0.0:
        return None
    return bid, ask, pip


def quote_epoch(quote: dict[str, Any]) -> float | None:
    numeric = safe_float(quote.get("quote_epoch"), math.nan)
    if math.isfinite(numeric) and numeric > 0.0:
        return numeric
    value = str(quote.get("time") or quote.get("timestamp") or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def quarantine_legacy_maturities(
    connection: sqlite3.Connection,
    maximum_distance_sec: float = MAX_TARGET_QUOTE_DISTANCE_SEC,
) -> int:
    """Quarantine demonstrably late legacy outcomes without inventing quote time.

    Older rows did not retain the target quote timestamp. Rows whose processing
    time alone proves they were more than the allowed distance from target are
    invalidated. Remaining legacy rows are explicitly timing-unverified and are
    excluded from the new exact-target aggregates.
    """
    late = connection.execute(
        """
        UPDATE positions
        SET status='quarantined_maturity', maturity_valid=0,
            target_quote_distance_sec=ABS(closed_epoch-target_epoch),
            maturity_reason='legacy_processing_delay_gt_limit_quote_time_unrecorded'
        WHERE status='matured' AND outcome_quote_epoch IS NULL
          AND closed_epoch IS NOT NULL
          AND ABS(closed_epoch-target_epoch) > ?
        """,
        (float(maximum_distance_sec),),
    )
    connection.execute(
        """
        UPDATE positions
        SET maturity_reason='legacy_quote_timestamp_unrecorded'
        WHERE status='matured' AND outcome_quote_epoch IS NULL
          AND maturity_reason IS NULL
        """
    )
    return max(0, int(late.rowcount))


def current_pips(row: sqlite3.Row, bid: float, ask: float, pip: float) -> tuple[float, float]:
    entry_mid = safe_float(row["entry_mid"])
    midpoint = (bid + ask) / 2.0
    if row["direction"] == "buy":
        gross = (midpoint - entry_mid) / pip
        net = (bid - safe_float(row["entry_ask"])) / pip
    else:
        gross = (entry_mid - midpoint) / pip
        net = (safe_float(row["entry_bid"]) - ask) / pip
    return gross, net


def mature_positions(connection: sqlite3.Connection, quotes: dict[str, dict[str, Any]], now_epoch: float) -> int:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT * FROM positions WHERE status = 'open' ORDER BY id"
    ).fetchall()
    closed = 0
    for row in rows:
        quote = quotes.get(str(row["instrument"])) or {}
        values = quote_values(quotes, str(row["instrument"]))
        if values is None:
            continue
        observed_quote_epoch = quote_epoch(quote)
        target_epoch = safe_float(row["target_epoch"])
        if observed_quote_epoch is None:
            if now_epoch >= target_epoch:
                connection.execute(
                    """
                    UPDATE positions SET status='quarantined_maturity',
                        maturity_valid=0,
                        maturity_reason='outcome_quote_timestamp_missing'
                    WHERE id=? AND status='open'
                    """,
                    (row["id"],),
                )
            continue
        bid, ask, pip = values
        gross, net = current_pips(row, bid, ask, pip)
        best = max(safe_float(row["best_net_pips"], -math.inf), net)
        worst = min(safe_float(row["worst_net_pips"], math.inf), net)
        first_positive = row["time_to_positive_sec"]
        if first_positive is None and net > 0.0:
            first_positive = max(
                0.0, observed_quote_epoch - safe_float(row["opened_epoch"])
            )
        if observed_quote_epoch < target_epoch:
            connection.execute(
                """
                UPDATE positions SET best_net_pips=?, worst_net_pips=?,
                    time_to_positive_sec=? WHERE id=?
                """,
                (best, worst, first_positive, row["id"]),
            )
            continue
        target_distance = observed_quote_epoch - target_epoch
        quote_at = datetime.fromtimestamp(observed_quote_epoch, UTC).isoformat()
        if target_distance > MAX_TARGET_QUOTE_DISTANCE_SEC:
            connection.execute(
                """
                UPDATE positions SET closed_epoch=?, closed_at=?,
                    outcome_quote_epoch=?, outcome_quote_at=?,
                    target_quote_distance_sec=?, maturity_valid=0,
                    maturity_reason='outcome_quote_after_target_limit',
                    status='quarantined_maturity'
                WHERE id=? AND status='open'
                """,
                (
                    observed_quote_epoch,
                    quote_at,
                    observed_quote_epoch,
                    quote_at,
                    target_distance,
                    row["id"],
                ),
            )
            continue
        connection.execute(
            """
            UPDATE positions SET closed_epoch=?, closed_at=?, exit_bid=?, exit_ask=?,
                exit_mid=?, gross_pips=?, net_pips=?, win=?, best_net_pips=?,
                worst_net_pips=?, time_to_positive_sec=?, outcome_quote_epoch=?,
                outcome_quote_at=?, target_quote_distance_sec=?, maturity_valid=1,
                maturity_reason='exact_target_quote_within_limit', status='matured'
            WHERE id=?
            """,
            (
                observed_quote_epoch,
                quote_at,
                bid,
                ask,
                (bid + ask) / 2.0,
                gross,
                net,
                int(net > 0.0),
                best,
                worst,
                first_positive,
                observed_quote_epoch,
                quote_at,
                target_distance,
                row["id"],
            ),
        )
        closed += 1
    connection.commit()
    return closed


def open_positions(
    connection: sqlite3.Connection,
    candidates: Iterable[dict[str, Any]],
    quotes: dict[str, dict[str, Any]],
    observed_at: str,
    now_epoch: float,
) -> int:
    opened = 0
    open_horizons = {
        (int(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT horizon_sec, measurement_version FROM positions "
            "WHERE status='open'"
        ).fetchall()
    }
    for candidate in candidates:
        horizon = int(candidate["horizon_sec"])
        measurement_version = str(
            candidate.get("measurement_version") or MEASUREMENT_VERSION
        )
        if (horizon, measurement_version) in open_horizons:
            continue
        values = quote_values(quotes, str(candidate["instrument"]))
        if values is None:
            continue
        bid, ask, pip = values
        entry_quote_epoch = quote_epoch(quotes.get(str(candidate["instrument"])) or {})
        opened_epoch = entry_quote_epoch if entry_quote_epoch is not None else now_epoch
        cohort_key = (
            f"{measurement_version}:{horizon}:{int(opened_epoch)}:"
            f"{candidate['instrument']}:{candidate['direction']}"
        )
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO positions(
                cohort_key, signal_id, source_signal_id,
                signal_lineage_contract_id, observed_at, opened_epoch, opened_at,
                target_epoch, target_at, instrument, direction, side,
                horizon_sec, horizon_label, family, lane_id, input_timeframe,
                policy_state, signal_eligible, validated, direction_conflict,
                direction_conflict_contract_id, direction_conflict_details_json,
                blocked_by_json, confidence, projected_net_pips,
                projected_net_pips_per_hour, gross_to_spread, entry_bid,
                measurement_version, direction_source, entry_ask, entry_mid,
                entry_spread_pips, best_net_pips,
                worst_net_pips, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'open')
            """,
            (
                cohort_key,
                candidate["signal_id"],
                candidate.get("source_signal_id", ""),
                candidate.get(
                    "signal_lineage_contract_id",
                    "legacy_source_signal_id",
                ),
                observed_at,
                opened_epoch,
                datetime.fromtimestamp(opened_epoch, UTC).isoformat(),
                opened_epoch + horizon,
                datetime.fromtimestamp(opened_epoch + horizon, UTC).isoformat(),
                candidate["instrument"],
                candidate["direction"],
                candidate["side"],
                horizon,
                candidate["horizon_label"],
                candidate["family"],
                candidate["lane_id"],
                candidate["input_timeframe"],
                candidate["policy_state"],
                int(candidate["signal_eligible"]),
                int(candidate["validated"]),
                int(candidate["direction_conflict"]),
                candidate.get(
                    "direction_conflict_contract_id",
                    "legacy_direction_conflict",
                ),
                json.dumps(
                    candidate.get("direction_conflict_details") or {},
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                json.dumps(candidate["blocked_by"], separators=(",", ":")),
                candidate["signal_confidence"],
                candidate["projected_net_pips"],
                candidate["projected_net_pips_per_hour"],
                candidate["gross_to_spread"],
                bid,
                measurement_version,
                candidate.get("direction_source", "raw_all_signal_consensus"),
                ask,
                (bid + ask) / 2.0,
                (ask - bid) / pip,
                -(ask - bid) / pip,
                -(ask - bid) / pip,
            ),
        )
        if cursor.rowcount == 1:
            opened += 1
            open_horizons.add((horizon, measurement_version))
    connection.commit()
    return opened


def aggregate_rows(
    connection: sqlite3.Connection,
    measurement_version: str = MEASUREMENT_VERSION,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT horizon_sec, horizon_label, policy_state, COUNT(*) AS n,
               SUM(CASE WHEN win=1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN gross_pips>0 THEN 1 ELSE 0 END) AS direction_hits,
               AVG(net_pips) AS avg_net_pips,
               AVG(gross_pips) AS avg_gross_pips,
               AVG(best_net_pips) AS avg_best_net_pips,
               AVG(worst_net_pips) AS avg_worst_net_pips
        FROM positions WHERE status='matured' AND measurement_version=?
          AND maturity_valid=1
        GROUP BY horizon_sec, horizon_label, policy_state
        ORDER BY horizon_sec, policy_state
        """,
        (measurement_version,),
    ).fetchall()
    return [
        {
            "horizon_sec": int(row[0]),
            "horizon_label": row[1],
            "policy_state": row[2],
            "n": int(row[3]),
            "wins": int(row[4]),
            "win_rate_pct": round(100.0 * int(row[4]) / int(row[3]), 3),
            "direction_hits": int(row[5]),
            "direction_accuracy_pct": round(100.0 * int(row[5]) / int(row[3]), 3),
            "avg_net_pips": round(safe_float(row[6]), 4),
            "avg_gross_pips": round(safe_float(row[7]), 4),
            "avg_best_net_pips": round(safe_float(row[8]), 4),
            "avg_worst_net_pips": round(safe_float(row[9]), 4),
        }
        for row in rows
    ]


def repair_policy_labels(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        """
        UPDATE positions SET policy_state='diagnostic_shadow'
        WHERE policy_state='executable'
          AND (signal_eligible=0 OR validated=0 OR direction_conflict=1
               OR blocked_by_json <> '[]')
        """
    )
    reclassified_conflicts = connection.execute(
        """
        UPDATE positions SET policy_state='conflicted_aggressive_shadow'
        WHERE policy_state='aggressive_shadow' AND direction_conflict=1
          AND measurement_version=?
        """,
        (MEASUREMENT_VERSION,),
    )
    connection.commit()
    return max(0, int(cursor.rowcount)) + max(
        0, int(reclassified_conflicts.rowcount)
    )


def aggregate_cost_rows(
    connection: sqlite3.Connection,
    *,
    include_horizon: bool = False,
    measurement_version: str = MEASUREMENT_VERSION,
    bucket_basis: str = "realized_cost_drag",
) -> list[dict[str, Any]]:
    if bucket_basis not in {"realized_cost_drag", "entry_spread"}:
        raise ValueError(f"Unsupported bucket basis: {bucket_basis}")
    bucket_value_sql = (
        "MAX(0.0, gross_pips - net_pips)"
        if bucket_basis == "realized_cost_drag"
        else "entry_spread_pips"
    )
    bucket_sql = f"""
        CASE
            WHEN {bucket_value_sql} <= 2.0 THEN 'liquid_le_2'
            WHEN {bucket_value_sql} <= 3.0 THEN 'normal_2_to_3'
            WHEN {bucket_value_sql} <= 5.0 THEN 'elevated_3_to_5'
            ELSE 'wide_gt_5'
        END
    """
    horizon_columns = "horizon_sec, horizon_label, " if include_horizon else ""
    horizon_group = "horizon_sec, horizon_label, " if include_horizon else ""
    rows = connection.execute(
        f"""
        SELECT {horizon_columns}{bucket_sql} AS cost_bucket,
               policy_state, COUNT(*) AS n,
               SUM(CASE WHEN gross_pips>0 THEN 1 ELSE 0 END) AS direction_hits,
               SUM(CASE WHEN net_pips>0 THEN 1 ELSE 0 END) AS after_cost_wins,
               AVG(entry_spread_pips) AS avg_entry_spread_pips,
               AVG(MAX(0.0, gross_pips - net_pips)) AS avg_realized_cost_drag_pips,
               AVG(gross_pips) AS avg_gross_pips,
               AVG(net_pips) AS avg_net_pips
        FROM positions
        WHERE status='matured' AND measurement_version=?
          AND maturity_valid=1
        GROUP BY {horizon_group}cost_bucket, policy_state
        ORDER BY {horizon_group}cost_bucket, policy_state
        """,
        (measurement_version,),
    ).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        offset = 0
        item: dict[str, Any] = {}
        if include_horizon:
            item["horizon_sec"] = int(row[0])
            item["horizon_label"] = str(row[1])
            offset = 2
        n = int(row[offset + 2])
        direction_hits = int(row[offset + 3])
        after_cost_wins = int(row[offset + 4])
        item.update(
            {
                "cost_bucket": str(row[offset]),
                "bucket_basis": bucket_basis,
                "policy_state": str(row[offset + 1]),
                "n": n,
                "direction_hits": direction_hits,
                "direction_accuracy_pct": round(100.0 * direction_hits / n, 3),
                "after_cost_wins": after_cost_wins,
                "after_cost_win_rate_pct": round(100.0 * after_cost_wins / n, 3),
                "avg_entry_spread_pips": round(safe_float(row[offset + 5]), 4),
                "avg_realized_cost_drag_pips": round(
                    safe_float(row[offset + 6]), 4
                ),
                "avg_gross_pips": round(safe_float(row[offset + 7]), 4),
                "avg_net_pips": round(safe_float(row[offset + 8]), 4),
            }
        )
        output.append(item)
    return output


def currency_factor_ids(instrument: str, direction: str) -> set[str]:
    """Return directional currency factors represented by one pair forecast."""

    parts = str(instrument or "").upper().split("_")
    if len(parts) != 2:
        return {f"instrument:{instrument}:{direction}"}
    base, quote = parts
    base_side = "long" if str(direction).lower() in {"buy", "long"} else "short"
    quote_side = "short" if base_side == "long" else "long"
    return {f"currency:{base}:{base_side}", f"currency:{quote}:{quote_side}"}


def aggregate_factor_episode_rows(
    connection: sqlite3.Connection,
    measurement_version: str = MEASUREMENT_VERSION,
    episode_gap_sec: float = FACTOR_EPISODE_GAP_SEC,
) -> list[dict[str, Any]]:
    """Score correlated signals once per continuous directional factor episode.

    Positions remain untouched in the raw ledger.  Within each policy arm,
    observations no more than ``episode_gap_sec`` apart are joined when they
    share a directional currency factor.  This collapses simultaneous JPY
    propagation (and equivalent cross-pair propagation in other currencies)
    without merging opposite directions.
    """

    rows = connection.execute(
        """
        SELECT policy_state, opened_epoch, instrument, direction,
               gross_pips, net_pips
        FROM positions
        WHERE status='matured' AND measurement_version=?
          AND maturity_valid=1
          AND gross_pips IS NOT NULL AND net_pips IS NOT NULL
        ORDER BY policy_state, opened_epoch
        """,
        (measurement_version,),
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for policy_state, opened_epoch, instrument, forecast_direction, gross, net in rows:
        grouped[str(policy_state)].append(
            {
                "opened_epoch": safe_float(opened_epoch),
                "factors": currency_factor_ids(str(instrument), str(forecast_direction)),
                "gross_pips": safe_float(gross),
                "net_pips": safe_float(net),
            }
        )

    output: list[dict[str, Any]] = []
    for policy_state, items in sorted(grouped.items()):
        parent = list(range(len(items)))

        def find(index: int) -> int:
            while parent[index] != index:
                parent[index] = parent[parent[index]]
                index = parent[index]
            return index

        def union(left: int, right: int) -> None:
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parent[right_root] = left_root

        for right, current in enumerate(items):
            left = right - 1
            while left >= 0:
                age = current["opened_epoch"] - items[left]["opened_epoch"]
                if age > episode_gap_sec:
                    break
                if current["factors"] & items[left]["factors"]:
                    union(left, right)
                left -= 1

        episodes: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for index, item in enumerate(items):
            episodes[find(index)].append(item)
        episode_values = [
            (
                sum(item["gross_pips"] for item in episode) / len(episode),
                sum(item["net_pips"] for item in episode) / len(episode),
                len(episode),
            )
            for episode in episodes.values()
        ]
        count = len(episode_values)
        position_count = len(items)
        output.append(
            {
                "policy_state": policy_state,
                "episode_count": count,
                "position_count": position_count,
                "direction_hits": sum(value[0] > 0.0 for value in episode_values),
                "direction_accuracy_pct": round(
                    100.0 * sum(value[0] > 0.0 for value in episode_values) / count,
                    3,
                ),
                "after_cost_wins": sum(value[1] > 0.0 for value in episode_values),
                "after_cost_win_rate_pct": round(
                    100.0 * sum(value[1] > 0.0 for value in episode_values) / count,
                    3,
                ),
                "avg_episode_gross_pips": round(
                    sum(value[0] for value in episode_values) / count, 4
                ),
                "avg_episode_net_pips": round(
                    sum(value[1] for value in episode_values) / count, 4
                ),
                "avg_positions_per_episode": round(position_count / count, 3),
                "max_positions_per_episode": max(value[2] for value in episode_values),
                "episode_gap_sec": float(episode_gap_sec),
                "weighting": "one equal-weight vote per directional currency-factor episode",
            }
        )
    return output


def recent_rows(connection: sqlite3.Connection, limit: int = 80) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT * FROM positions
        ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END,
                 COALESCE(closed_epoch, target_epoch) DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["signal_eligible"] = bool(item["signal_eligible"])
        item["validated"] = bool(item["validated"])
        item["direction_conflict"] = bool(item["direction_conflict"])
        item["win"] = None if item["win"] is None else bool(item["win"])
        item["direction_hit"] = (
            None
            if item.get("gross_pips") is None
            else safe_float(item.get("gross_pips")) > 0.0
        )
        entry_bucket = liquidity_cost_bucket(
            safe_float(item.get("entry_spread_pips"), math.inf)
        )
        item["entry_liquidity_bucket"] = entry_bucket
        if item.get("gross_pips") is not None and item.get("net_pips") is not None:
            realized_cost_drag = max(
                0.0,
                safe_float(item.get("gross_pips"))
                - safe_float(item.get("net_pips")),
            )
            item["realized_cost_drag_pips"] = realized_cost_drag
            item["realized_cost_bucket"] = liquidity_cost_bucket(
                realized_cost_drag
            )
            item["cost_bucket"] = item["realized_cost_bucket"]
            item["cost_bucket_basis"] = "realized_cost_drag_pips"
        else:
            item["realized_cost_drag_pips"] = None
            item["realized_cost_bucket"] = None
            item["cost_bucket"] = entry_bucket
            item["cost_bucket_basis"] = "entry_spread_pips_pending"
        item["blocked_by"] = json.loads(item.pop("blocked_by_json") or "[]")
        output.append(item)
    return output


def position_revisions(connection: sqlite3.Connection) -> tuple[int, int]:
    """Return trigger-maintained metadata and mature-history generations."""

    row = connection.execute(
        "SELECT metadata_revision, mature_revision "
        "FROM ledger_revision WHERE singleton=1"
    ).fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


def position_metadata_payload(connection: sqlite3.Connection) -> dict[str, Any]:
    """Build count/integrity fields that ignore open mark-to-market changes."""

    counts = dict(
        connection.execute(
            "SELECT status, COUNT(*) FROM positions GROUP BY status"
        ).fetchall()
    )
    measurement_counts = dict(
        connection.execute(
            "SELECT measurement_version, COUNT(*) FROM positions "
            "WHERE status='matured' GROUP BY measurement_version"
        ).fetchall()
    )
    maturity_integrity = {
        str(state): int(count)
        for state, count in connection.execute(
            """
            SELECT CASE
                WHEN status='quarantined_maturity' THEN 'quarantined'
                WHEN status='matured' AND maturity_valid=1 THEN 'exact_valid'
                WHEN status='matured' AND maturity_valid IS NULL THEN 'legacy_timing_unverified'
                ELSE status
            END, COUNT(*) FROM positions GROUP BY 1
            """
        )
    }
    return {
        "counts": {
            "open": int(counts.get("open", 0)),
            "matured": int(counts.get("matured", 0)),
            "total": int(sum(counts.values())),
            "current_matured": int(
                measurement_counts.get(MEASUREMENT_VERSION, 0)
            ),
            "legacy_matured": int(
                measurement_counts.get(LEGACY_MEASUREMENT_VERSION, 0)
            ),
            "previous_v2_matured": int(
                measurement_counts.get(PREVIOUS_MEASUREMENT_VERSION, 0)
            ),
            "older_v3_matured": int(
                measurement_counts.get(OLDER_MEASUREMENT_VERSION, 0)
            ),
        },
        "maturity_integrity": maturity_integrity,
    }


def mature_summary_payload(connection: sqlite3.Connection) -> dict[str, Any]:
    """Build the output fields that depend only on matured position history."""

    return {
        "by_horizon": aggregate_rows(connection),
        "by_cost_bucket": aggregate_cost_rows(connection),
        "by_horizon_cost_bucket": aggregate_cost_rows(
            connection,
            include_horizon=True,
        ),
        "by_entry_liquidity_bucket": aggregate_cost_rows(
            connection,
            bucket_basis="entry_spread",
        ),
        "by_horizon_entry_liquidity_bucket": aggregate_cost_rows(
            connection,
            include_horizon=True,
            bucket_basis="entry_spread",
        ),
        "by_currency_factor_episode": aggregate_factor_episode_rows(connection),
        "legacy_by_horizon": aggregate_rows(
            connection,
            LEGACY_MEASUREMENT_VERSION,
        ),
    }


def cached_publish_payloads(
    connection: sqlite3.Connection,
    cache: dict[str, Any] | None,
    *,
    metadata_dirty: bool,
    mature_dirty: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Cache count metadata and mature summaries on independent revisions.

    Open inserts advance only metadata.  A transition to or from ``matured``
    and changes to fields consumed by mature aggregates advance mature history.
    Open-position MFE/MAE updates advance neither because ``recent_rows`` reads
    them afresh every cycle.
    """

    if cache is None:
        return (
            position_metadata_payload(connection),
            mature_summary_payload(connection),
        )
    metadata_revision, mature_revision = position_revisions(connection)
    metadata = cache.get("metadata")
    mature = cache.get("mature")
    if (
        metadata_dirty
        or not isinstance(metadata, dict)
        or cache.get("metadata_revision") != metadata_revision
    ):
        metadata = position_metadata_payload(connection)
        cache["metadata_revision"] = metadata_revision
        cache["metadata"] = metadata
    if (
        mature_dirty
        or not isinstance(mature, dict)
        or cache.get("mature_revision") != mature_revision
    ):
        mature = mature_summary_payload(connection)
        cache["mature_revision"] = mature_revision
        cache["mature"] = mature
    return metadata, mature


def publish_state(
    connection: sqlite3.Connection,
    path: Path,
    last_cycle: dict[str, Any],
    *,
    summary_cache: dict[str, Any] | None = None,
    metadata_dirty: bool = True,
    mature_dirty: bool = True,
) -> None:
    metadata, mature_summaries = cached_publish_payloads(
        connection,
        summary_cache,
        metadata_dirty=metadata_dirty,
        mature_dirty=mature_dirty,
    )
    write_json_atomic(
        path,
        {
            "schema_version": 2,
            "generated_utc": utc_now(),
            "status": "ok",
            "research_only": True,
            "can_place_orders": False,
            "direction_terms": ["Long", "Short"],
            "horizon_alias_policy": {"H24": "D1", "deduplicated": True},
            "measurement": {
                "current_version": MEASUREMENT_VERSION,
                "direction_policy": (
                    "eligible rows use filtered execution consensus; blocked rows "
                    "use the raw all-signal consensus side"
                ),
                "legacy_rows_excluded_from_current_summary": True,
                "win_semantics": "net_pips_gt_0_after_entry_and_exit_spread",
                "direction_hit_semantics": "gross_midpoint_move_in_forecast_direction_gt_0",
                "cost_bucket_basis": "realized_cost_drag_pips_for_matured_rows",
                "entry_liquidity_bucket_basis": "entry_spread_pips",
                "target_quote_contract": "quote timestamp at or after target and no more than 60 seconds late",
                "historical_rows_without_quote_timestamp_are_noncanonical": True,
                "aggregate_lineage_contract_id": (
                    AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID
                ),
                "strict_lineage_required_for_current_version": True,
                "transitional_version": PREVIOUS_MEASUREMENT_VERSION,
            },
            "counts": metadata["counts"],
            "maturity_integrity": metadata["maturity_integrity"],
            "last_cycle": last_cycle,
            **mature_summaries,
            "positions": recent_rows(connection),
        },
    )


def run_cycle(
    connection: sqlite3.Connection,
    signal_path: Path,
    quote_path: Path,
    state_path: Path,
    *,
    summary_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = load_json(signal_path)
    quote_payload = load_quote_snapshot(quote_path)
    now_epoch = time.time()
    quotes = quote_map(quote_payload, now_epoch=now_epoch)
    candidates = top_by_horizon(snapshot)
    reclassified = repair_policy_labels(connection)
    closed = mature_positions(connection, quotes, now_epoch)
    opened = open_positions(
        connection,
        candidates,
        quotes,
        str(snapshot.get("updated_at") or snapshot.get("generated_utc") or utc_now()),
        now_epoch,
    )
    cycle = {
        "time": utc_now(),
        "signal_snapshot": str(signal_path),
        "signal_updated_at": snapshot.get("updated_at"),
        "quote_generated_utc": quote_payload.get("generated_utc"),
        "quote_count": len(quotes),
        "horizon_candidates": len(candidates),
        "opened": opened,
        "matured": closed,
        "reclassified": reclassified,
    }
    publish_state(
        connection,
        state_path,
        cycle,
        summary_cache=summary_cache,
        metadata_dirty=bool(opened or closed),
        mature_dirty=bool(closed),
    )
    return cycle


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--quotes", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--interval-sec", type=float, default=5.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    connection = open_database(args.database)
    summary_cache: dict[str, Any] = {}
    try:
        stop_at = time.monotonic() + max(0.0, args.duration_sec)
        while True:
            run_cycle(
                connection,
                args.signals,
                args.quotes,
                args.state,
                summary_cache=summary_cache,
            )
            if args.once or time.monotonic() >= stop_at:
                break
            time.sleep(max(0.25, args.interval_sec))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
