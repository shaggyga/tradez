#!/usr/bin/env python3
"""Fit persistent, horizon-aware promotion evidence from executable FX outcomes."""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def agreement_adjusted_signal_thresholds(
    min_confidence: float,
    min_expected_net_pips: float,
    agreeing_families: int,
    opposing_families: int,
) -> tuple[float, float, str]:
    """Relax only the soft entry gate when independent families agree cleanly."""

    confidence = max(0.5, _finite(min_confidence, 0.54))
    expected_net = _finite(min_expected_net_pips, 0.05)
    agreeing = max(0, int(agreeing_families))
    opposing = max(0, int(opposing_families))
    if opposing > 0 or agreeing < 2:
        return confidence, expected_net, "base"
    if agreeing >= 3:
        adjusted_net = expected_net if expected_net < 0.0 else max(0.0, expected_net - 0.05)
        return max(0.5, confidence - 0.015), adjusted_net, "three_plus_family_agreement"
    adjusted_net = expected_net if expected_net < 0.0 else max(0.0, expected_net - 0.025)
    return max(0.5, confidence - 0.008), adjusted_net, "two_family_agreement"


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def session_name(value: Any) -> str:
    parsed = value if isinstance(value, datetime) else _parse_time(value)
    if parsed is None:
        return "unknown"
    hour = parsed.hour
    if hour < 7:
        return "asia"
    if hour < 13:
        return "london"
    if hour < 17:
        return "overlap"
    if hour < 22:
        return "new_york"
    return "rollover"


def canonical_lane_identity(lane_id: Any, family: Any, profile: Any) -> tuple[str, str]:
    lane = str(lane_id or "")
    family_name = str(family or "")
    profile_name = str(profile or "")
    if family_name == "second_ridge_forecast" or lane.startswith("second_ridge_forecast.h"):
        return f"ridge_return.s1.{profile_name}", "ridge_return"
    return lane, family_name


def _metrics(values: Iterable[float]) -> dict[str, Any]:
    sample = [float(value) for value in values if math.isfinite(float(value))]
    if not sample:
        return {
            "n": 0,
            "avg": 0.0,
            "median": 0.0,
            "win_rate": 0.0,
            "stdev": 0.0,
            "lower_confidence": -999.0,
        }
    average = statistics.fmean(sample)
    stdev = statistics.stdev(sample) if len(sample) > 1 else math.inf
    lower = average - 1.645 * stdev / math.sqrt(len(sample)) if math.isfinite(stdev) else -999.0
    return {
        "n": len(sample),
        "avg": round(average, 4),
        "median": round(statistics.median(sample), 4),
        "win_rate": round(100.0 * sum(value > 0.0 for value in sample) / len(sample), 2),
        "stdev": round(stdev, 4) if math.isfinite(stdev) else 999.0,
        "lower_confidence": round(lower, 4),
    }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _promotion_key(row: dict[str, Any]) -> tuple[str, int]:
    return str(row.get("lane_id") or ""), int(row.get("horizon_sec") or 0)


def _promotion_evidence_size(row: dict[str, Any]) -> tuple[int, int, int, int]:
    raw = row.get("raw") or {}
    training = row.get("training") or {}
    holdout = row.get("holdout") or {}
    return (
        int(raw.get("n") or 0),
        int(row.get("independent_blocks") or 0),
        int(holdout.get("n") or 0),
        int(training.get("n") or 0),
    )


def reconcile_promotion_state(
    candidate: dict[str, Any],
    incumbent: dict[str, Any],
    *,
    allow_state_regression: bool = False,
) -> dict[str, Any]:
    """Merge partial live evidence without erasing a richer historical state."""

    candidate_rows = [
        row for row in (candidate.get("signal_evidence") or [])
        if isinstance(row, dict) and all(_promotion_key(row))
    ]
    incumbent_rows = [
        row for row in (incumbent.get("signal_evidence") or [])
        if isinstance(row, dict) and all(_promotion_key(row))
    ]
    candidate_raw = int(candidate.get("raw_rows") or 0)
    incumbent_raw = int(incumbent.get("raw_rows") or 0)
    candidate_horizons = {int(value) for value in candidate.get("horizons_sec") or []}
    incumbent_horizons = {int(value) for value in incumbent.get("horizons_sec") or []}
    complete_rebuild = bool(
        candidate.get("source_complete")
        and incumbent_horizons.issubset(candidate_horizons)
    )
    regression = (
        incumbent_rows
        and (
            candidate_raw < incumbent_raw
            or len(candidate_rows) < len(incumbent_rows)
            or (bool(incumbent.get("source_complete")) and not bool(candidate.get("source_complete")))
        )
    )
    if allow_state_regression or complete_rebuild or not regression:
        result = dict(candidate)
        result["artifact_update"] = {
            "replaced": True,
            "reason": (
                "explicit_state_regression_override"
                if allow_state_regression and regression
                else "complete_compacted_rebuild"
                if complete_rebuild and regression
                else "coverage_non_regression_passed"
            ),
            "candidate_raw_rows": candidate_raw,
            "incumbent_raw_rows": incumbent_raw,
            "candidate_evidence_count": len(candidate_rows),
            "incumbent_evidence_count": len(incumbent_rows),
        }
        return result

    merged = {_promotion_key(row): row for row in incumbent_rows}
    for row in candidate_rows:
        key = _promotion_key(row)
        if key not in merged or _promotion_evidence_size(row) >= _promotion_evidence_size(merged[key]):
            merged[key] = row
    ranked = sorted(
        merged.values(),
        key=lambda row: (
            bool(row.get("eligible")),
            _finite(row.get("score"), -999.0),
            int(row.get("independent_blocks") or 0),
        ),
        reverse=True,
    )
    result = dict(candidate)
    result.update(
        {
            "status": "historical_backfill_preserving_incumbent",
            "source_complete": False,
            "horizons_sec": sorted(
                {
                    int(value)
                    for value in (
                        list(incumbent.get("horizons_sec") or [])
                        + list(candidate.get("horizons_sec") or [])
                    )
                }
            ),
            "raw_rows": max(candidate_raw, incumbent_raw),
            "evidence_count": len(ranked),
            "eligible_count": 0,
            "eligible_lane_count": 0,
            "top_evidence": ranked[:80],
            "signal_evidence": ranked,
            "qualified_evidence": [],
            "artifact_update": {
                "replaced": False,
                "reason": "richer_incumbent_merged_during_partial_backfill",
                "candidate_raw_rows": candidate_raw,
                "incumbent_raw_rows": incumbent_raw,
                "candidate_evidence_count": len(candidate_rows),
                "incumbent_evidence_count": len(incumbent_rows),
                "merged_evidence_count": len(ranked),
            },
        }
    )
    return result


@dataclass(frozen=True)
class PromotionThresholds:
    min_samples: int = 30
    min_average_pips: float = 0.10
    min_median_pips: float = 0.0
    min_win_rate: float = 52.0
    min_lower_confidence_pips: float = 0.0
    min_independent_blocks: int = 12
    min_holdout_blocks: int = 4
    min_pairs: int = 3
    min_sessions: int = 2
    min_segment_samples: int = 10


def _block_rows(records: list[dict[str, Any]], horizon_sec: int) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    fallback = 0
    for row in records:
        parsed = _parse_time(row.get("entry_time"))
        if parsed is None:
            bucket = 10**15 + fallback
            fallback += 1
        else:
            bucket = int(parsed.timestamp()) // max(1, int(horizon_sec))
        grouped[bucket].append(row)
    blocks: list[dict[str, Any]] = []
    for bucket, rows in sorted(grouped.items()):
        blocks.append(
            {
                "bucket": bucket,
                "avg": statistics.fmean(_finite(row.get("endpoint_pips")) for row in rows),
                "rows": rows,
            }
        )
    return blocks


def _segment_metrics(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        if key == "session":
            value = session_name(row.get("entry_time"))
        else:
            value = str(row.get(key) or "unknown")
        grouped[value].append(_finite(row.get("endpoint_pips")))
    return {name: _metrics(values) for name, values in grouped.items()}


def evaluate_promotion_scope(
    records: list[dict[str, Any]],
    horizon_sec: int,
    thresholds: PromotionThresholds,
) -> dict[str, Any]:
    ordered = sorted(
        records,
        key=lambda row: (str(row.get("entry_time") or ""), int(row.get("row_id") or 0)),
    )
    blocks = _block_rows(ordered, horizon_sec)
    split = max(1, min(len(blocks) - 1, int(len(blocks) * 0.70))) if len(blocks) > 1 else len(blocks)
    training_blocks = blocks[:split]
    holdout_blocks = blocks[split:]
    training = _metrics(block["avg"] for block in training_blocks)
    holdout = _metrics(block["avg"] for block in holdout_blocks)
    raw = _metrics(_finite(row.get("endpoint_pips")) for row in ordered)
    holdout_rows = [row for block in holdout_blocks for row in block["rows"]]
    pair_metrics = _segment_metrics(holdout_rows, "instrument")
    session_metrics = _segment_metrics(holdout_rows, "session")

    stability_chunks: list[list[float]] = []
    holdout_values = [float(block["avg"]) for block in holdout_blocks]
    if holdout_values:
        chunk_size = max(1, math.ceil(len(holdout_values) / 3))
        stability_chunks = [
            holdout_values[index : index + chunk_size]
            for index in range(0, len(holdout_values), chunk_size)
        ]
    stability_averages = [statistics.fmean(chunk) for chunk in stability_chunks]
    positive_stability_blocks = sum(value > 0.0 for value in stability_averages)
    sample_fraction = min(
        1.0,
        len(ordered) / max(1.0, float(thresholds.min_samples)),
    )
    block_fraction = min(
        1.0,
        len(blocks) / max(1.0, float(thresholds.min_independent_blocks)),
    )
    holdout_fraction = min(
        1.0,
        len(holdout_blocks) / max(1.0, float(thresholds.min_holdout_blocks)),
    )
    evidence_strength = (
        sample_fraction * block_fraction * holdout_fraction
    ) ** (1.0 / 3.0)
    conservative_prior = min(
        -0.05,
        float(thresholds.min_lower_confidence_pips) - 0.05,
    )
    holdout_lower_for_score = float(holdout["lower_confidence"])
    if holdout_lower_for_score <= -100.0:
        holdout_lower_for_score = conservative_prior
    adjusted_lower_confidence = (
        evidence_strength * holdout_lower_for_score
        + (1.0 - evidence_strength) * conservative_prior
    )

    reasons: list[str] = []
    if len(ordered) < thresholds.min_samples:
        reasons.append("minimum_samples")
    if len(blocks) < thresholds.min_independent_blocks:
        reasons.append("minimum_independent_blocks")
    if len(holdout_blocks) < thresholds.min_holdout_blocks:
        reasons.append("minimum_holdout_blocks")
    if training["avg"] < thresholds.min_average_pips:
        reasons.append("training_net_edge")
    if holdout["avg"] < thresholds.min_average_pips:
        reasons.append("holdout_net_edge")
    if holdout["median"] < thresholds.min_median_pips:
        reasons.append("holdout_median")
    if holdout["win_rate"] < thresholds.min_win_rate:
        reasons.append("holdout_win_rate")
    if holdout["lower_confidence"] < thresholds.min_lower_confidence_pips:
        reasons.append("holdout_confidence")
    if len(pair_metrics) < thresholds.min_pairs:
        reasons.append("pair_breadth")
    if len(session_metrics) < thresholds.min_sessions:
        reasons.append("session_breadth")
    if len(stability_averages) < 3 or positive_stability_blocks < len(stability_averages):
        reasons.append("time_block_stability")

    return {
        "eligible": not reasons,
        "blocked_by": reasons,
        "horizon_sec": int(horizon_sec),
        "sample_count": len(ordered),
        "independent_blocks": len(blocks),
        "holdout_blocks": len(holdout_blocks),
        "split": "oldest 70% independent blocks / newest 30% holdout",
        "raw": raw,
        "training": training,
        "holdout": holdout,
        "pair_count": len(pair_metrics),
        "session_count": len(session_metrics),
        "positive_time_blocks": positive_stability_blocks,
        "time_block_count": len(stability_averages),
        "pair_metrics": pair_metrics,
        "session_metrics": session_metrics,
        "evidence_strength": round(evidence_strength, 6),
        "adjusted_lower_confidence": round(adjusted_lower_confidence, 4),
        "score": round(adjusted_lower_confidence, 4),
    }


class LanePromotionStore:
    """Compact executable-return ledger with incremental path-ledger backfill."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database_path, timeout=120.0)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=120000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS outcomes (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_row_id INTEGER,
                event_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                observed_utc TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                family TEXT NOT NULL,
                profile TEXT NOT NULL,
                kind TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_time TEXT,
                endpoint_pips REAL NOT NULL,
                UNIQUE(event_id, horizon_sec)
            );
            CREATE INDEX IF NOT EXISTS idx_promotion_scope
                ON outcomes(kind, horizon_sec, lane_id, entry_time);
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        migration_key = "canonical_s1_ridge_lane_v1"
        if self._metadata(migration_key, "0") != "1":
            self.connection.execute(
                """
                UPDATE outcomes
                SET lane_id = 'ridge_return.s1.' || profile,
                    family = 'ridge_return'
                WHERE family = 'second_ridge_forecast'
                   OR lane_id LIKE 'second_ridge_forecast.h%'
                """
            )
            self._set_metadata(migration_key, 1)
        self.connection.commit()

    def observe(self, **row: Any) -> None:
        if str(row.get("kind") or "") != "signal":
            return
        lane_id, family = canonical_lane_identity(
            row.get("lane_id"), row.get("family"), row.get("profile")
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO outcomes (
                source_row_id, event_id, horizon_sec, observed_utc, lane_id,
                family, profile, kind, instrument, direction, entry_time,
                endpoint_pips
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'signal', ?, ?, ?, ?)
            """,
            (
                row.get("source_row_id"),
                str(row.get("event_id") or ""),
                int(_finite(row.get("horizon_sec"))),
                utc_now(),
                lane_id,
                family,
                str(row.get("profile") or ""),
                str(row.get("instrument") or ""),
                str(row.get("direction") or ""),
                str(row.get("entry_time") or ""),
                _finite(row.get("endpoint_pips")),
            ),
        )

    def _metadata(self, key: str, default: str = "") -> str:
        row = self.connection.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        return str(row[0]) if row else default

    def _set_metadata(self, key: str, value: Any) -> None:
        self.connection.execute(
            "INSERT INTO metadata(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    def backfill_from(
        self,
        source_database: Path,
        horizons: Iterable[int],
        *,
        chunk_rows: int = 50000,
        max_chunks: int = 2,
    ) -> dict[str, Any]:
        source_path = Path(source_database)
        horizon_set = {int(value) for value in horizons}
        if not source_path.is_file():
            return {"source_complete": False, "cursor": 0, "source_max_row": 0, "inserted": 0}
        cursor_value = int(self._metadata("source_cursor", "0") or 0)
        previous_horizons = {
            int(value)
            for value in self._metadata("source_horizons", "").split(",")
            if value.strip()
        }
        pending_horizons = {
            int(value)
            for value in self._metadata("source_backfill_horizons", "").split(",")
            if value.strip()
        }
        new_horizons = horizon_set - previous_horizons
        horizon_backfill_reset = bool(new_horizons)
        if horizon_backfill_reset:
            cursor_value = 0
            self._set_metadata("source_cursor", 0)
            self._set_metadata("initial_backfill_complete", 0)
            pending_horizons = new_horizons
            self._set_metadata(
                "source_backfill_horizons",
                ",".join(str(value) for value in sorted(pending_horizons)),
            )
            previous_horizons |= horizon_set
            self._set_metadata(
                "source_horizons",
                ",".join(str(value) for value in sorted(previous_horizons)),
            )
        query_horizons = pending_horizons or horizon_set
        self.connection.execute("ATTACH DATABASE ? AS promotion_source", (str(source_path),))
        max_row = int(
            self.connection.execute(
                "SELECT COALESCE(MAX(row_id), 0) FROM promotion_source.outcomes"
            ).fetchone()[0]
        )
        inserted = 0
        scanned = 0
        placeholders = ",".join("?" for _ in query_horizons)
        try:
            for _ in range(max(1, int(max_chunks))):
                if cursor_value >= max_row:
                    break
                upper_row = min(max_row, cursor_value + max(1, int(chunk_rows)))
                before = self.connection.total_changes
                self.connection.execute(
                    f"""
                    INSERT OR IGNORE INTO outcomes (
                        source_row_id, event_id, horizon_sec, observed_utc,
                        lane_id, family, profile, kind, instrument, direction,
                        entry_time, endpoint_pips
                    )
                    SELECT row_id, event_id, horizon_sec, observed_utc,
                           CASE
                               WHEN family = 'second_ridge_forecast'
                                 OR lane_id LIKE 'second_ridge_forecast.h%'
                               THEN 'ridge_return.s1.' || profile
                               ELSE lane_id
                           END,
                           CASE
                               WHEN family = 'second_ridge_forecast'
                                 OR lane_id LIKE 'second_ridge_forecast.h%'
                               THEN 'ridge_return'
                               ELSE family
                           END,
                           profile, kind, instrument, direction,
                           entry_time, endpoint_pips
                    FROM promotion_source.outcomes
                    WHERE row_id > ? AND row_id <= ?
                      AND kind = 'signal'
                      AND horizon_sec IN ({placeholders})
                    """,
                    (cursor_value, upper_row, *sorted(query_horizons)),
                )
                inserted += self.connection.total_changes - before
                scanned += upper_row - cursor_value
                cursor_value = upper_row
                self._set_metadata("source_cursor", cursor_value)
                self.connection.commit()
        finally:
            self.connection.execute("DETACH DATABASE promotion_source")
        source_complete = cursor_value >= max_row
        if source_complete:
            self._set_metadata("initial_backfill_complete", 1)
            self._set_metadata("source_backfill_horizons", "")
            self._set_metadata(
                "source_horizons",
                ",".join(str(value) for value in sorted(previous_horizons | horizon_set)),
            )
        initial_complete = self._metadata("initial_backfill_complete", "0") == "1"
        self.connection.commit()
        return {
            "source_complete": bool(source_complete and initial_complete),
            "cursor": cursor_value,
            "source_max_row": max_row,
            "scanned": scanned,
            "inserted": inserted,
            "horizon_backfill_reset": horizon_backfill_reset,
            "queried_horizons": sorted(query_horizons),
            "compact_rows": int(self.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]),
        }

    def sync_from_compact(
        self,
        source_database: Path,
        horizons: Iterable[int],
        *,
        cursor_key: str = "compact_source_cursor",
        chunk_rows: int = 100000,
        max_chunks: int = 20,
    ) -> dict[str, Any]:
        source_path = Path(source_database)
        horizon_set = {int(value) for value in horizons}
        if not source_path.is_file():
            return {"source_complete": False, "cursor": 0, "source_max_row": 0, "inserted": 0}
        cursor_value = int(self._metadata(cursor_key, "0") or 0)
        self.connection.execute("ATTACH DATABASE ? AS compact_source", (str(source_path),))
        max_row = int(
            self.connection.execute(
                "SELECT COALESCE(MAX(row_id), 0) FROM compact_source.outcomes"
            ).fetchone()[0]
        )
        inserted = 0
        scanned = 0
        placeholders = ",".join("?" for _ in horizon_set)
        try:
            for _ in range(max(1, int(max_chunks))):
                if cursor_value >= max_row:
                    break
                upper_row = min(max_row, cursor_value + max(1, int(chunk_rows)))
                before = self.connection.total_changes
                self.connection.execute(
                    f"""
                    INSERT OR IGNORE INTO outcomes (
                        source_row_id, event_id, horizon_sec, observed_utc,
                        lane_id, family, profile, kind, instrument, direction,
                        entry_time, endpoint_pips
                    )
                    SELECT source_row_id, event_id, horizon_sec, observed_utc,
                           lane_id, family, profile, kind, instrument, direction,
                           entry_time, endpoint_pips
                    FROM compact_source.outcomes
                    WHERE row_id > ? AND row_id <= ?
                      AND kind = 'signal'
                      AND horizon_sec IN ({placeholders})
                    """,
                    (cursor_value, upper_row, *sorted(horizon_set)),
                )
                inserted += self.connection.total_changes - before
                scanned += upper_row - cursor_value
                cursor_value = upper_row
                self._set_metadata(cursor_key, cursor_value)
                self.connection.commit()
        finally:
            self.connection.execute("DETACH DATABASE compact_source")
        self.connection.commit()
        return {
            "source_complete": cursor_value >= max_row,
            "cursor": cursor_value,
            "source_max_row": max_row,
            "scanned": scanned,
            "inserted": inserted,
        }

    def flush(self) -> None:
        self.connection.commit()

    def close(self) -> None:
        self.connection.commit()
        self.connection.close()


class LanePromotionModel:
    """Direct executable-net/no-trade model over persisted lane outcomes."""

    def __init__(
        self,
        database_path: Path,
        state_path: Path,
        horizons: Iterable[int],
        *,
        thresholds: PromotionThresholds | None = None,
        refresh_sec: float = 300.0,
        fit_enabled: bool = True,
        source_complete: bool = True,
        source_metadata: dict[str, Any] | None = None,
        allow_state_regression: bool = False,
    ) -> None:
        self.database_path = Path(database_path)
        self.state_path = Path(state_path)
        self.horizons = tuple(sorted({int(value) for value in horizons if int(value) > 0}))
        self.thresholds = thresholds or PromotionThresholds()
        self.refresh_sec = max(10.0, float(refresh_sec))
        self.fit_enabled = bool(fit_enabled)
        self.source_complete = bool(source_complete)
        self.source_metadata = dict(source_metadata or {})
        self.allow_state_regression = bool(allow_state_regression)
        self.last_refresh_monotonic = -math.inf
        self.state_mtime_ns = 0
        self.evidence: dict[tuple[str, int], dict[str, Any]] = {}
        self.signal_evidence: dict[tuple[str, int], dict[str, Any]] = {}
        self.state: dict[str, Any] = {}

    def _records(self) -> list[dict[str, Any]]:
        if not self.database_path.is_file() or not self.horizons:
            return []
        connection = sqlite3.connect(self.database_path, timeout=30.0)
        connection.execute("PRAGMA query_only=ON")
        placeholders = ",".join("?" for _ in self.horizons)
        cursor = connection.execute(
            f"""
            SELECT row_id, horizon_sec, lane_id, family, profile, instrument,
                   direction, entry_time, endpoint_pips
            FROM outcomes
            WHERE kind = 'signal' AND horizon_sec IN ({placeholders})
            """,
            self.horizons,
        )
        rows = [
            {
                "row_id": values[0],
                "horizon_sec": values[1],
                "lane_id": values[2],
                "family": values[3],
                "profile": values[4],
                "instrument": values[5],
                "direction": values[6],
                "entry_time": values[7],
                "endpoint_pips": values[8],
            }
            for values in cursor.fetchall()
        ]
        connection.close()
        return rows

    @staticmethod
    def _compact(row: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in row.items() if key not in {"pair_metrics", "session_metrics"}}

    def fit(self) -> dict[str, Any]:
        records = self._records()
        grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in records:
            grouped[(str(row["lane_id"]), int(row["horizon_sec"]))].append(row)
        evidence: dict[tuple[str, int], dict[str, Any]] = {}
        for (lane_id, horizon), rows in grouped.items():
            result = evaluate_promotion_scope(rows, horizon, self.thresholds)
            result.update(
                {
                    "lane_id": lane_id,
                    "family": str(rows[0].get("family") or ""),
                    "profile": str(rows[0].get("profile") or ""),
                }
            )
            evidence[(lane_id, horizon)] = result
        ranked = sorted(
            evidence.values(),
            key=lambda row: (
                bool(row.get("eligible")),
                _finite(row.get("score"), -999.0),
                int(row.get("independent_blocks") or 0),
            ),
            reverse=True,
        )
        eligible = [row for row in ranked if row.get("eligible")] if self.source_complete else []
        state = {
            "schema_version": 2,
            "generated_at": utc_now(),
            "status": (
                "historical_backfill"
                if not self.source_complete
                else "qualified_evidence_available"
                if eligible
                else "no_qualified_evidence"
            ),
            "source_complete": self.source_complete,
            "backfill": self.source_metadata,
            "database": str(self.database_path.resolve()),
            "model_type": "direct executable-net return with explicit no-trade outcome",
            "method": "lane/horizon evidence grouped into non-overlapping horizon blocks; oldest 70% fit and newest 30% holdout; lower-bound score shrunk by sample and independent-block coverage",
            "horizons_sec": list(self.horizons),
            "thresholds": asdict(self.thresholds),
            "raw_rows": len(records),
            "evidence_count": len(ranked),
            "eligible_count": len(eligible),
            "eligible_lane_count": len({str(row.get("lane_id") or "") for row in eligible}),
            "top_evidence": [self._compact(row) for row in ranked[:80]],
            # Execution ranks individual signals. Keep every compact scope so
            # historical evidence can calibrate confidence without promoting a
            # whole model lane.
            "signal_evidence": [self._compact(row) for row in ranked],
            "qualified_evidence": eligible,
        }
        try:
            incumbent = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            incumbent = {}
        state = reconcile_promotion_state(
            state,
            incumbent if isinstance(incumbent, dict) else {},
            allow_state_regression=self.allow_state_regression,
        )
        _atomic_json(self.state_path, state)
        qualified_rows = state.get("qualified_evidence") or []
        signal_rows = state.get("signal_evidence") or []
        self.evidence = {
            _promotion_key(row): row
            for row in qualified_rows
            if isinstance(row, dict) and row.get("eligible")
        }
        self.signal_evidence = {
            _promotion_key(row): row
            for row in signal_rows
            if isinstance(row, dict) and all(_promotion_key(row))
        }
        self.state = state
        self.last_refresh_monotonic = time.monotonic()
        self.state_mtime_ns = self.state_path.stat().st_mtime_ns
        return state

    def load_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        self.state = payload if isinstance(payload, dict) else {}
        rows = self.state.get("qualified_evidence") or []
        self.evidence = {
            (str(row.get("lane_id") or ""), int(row.get("horizon_sec") or 0)): row
            for row in rows
            if isinstance(row, dict) and row.get("eligible")
        }
        signal_rows = self.state.get("signal_evidence") or self.state.get("top_evidence") or rows
        self.signal_evidence = {
            (str(row.get("lane_id") or ""), int(row.get("horizon_sec") or 0)): row
            for row in signal_rows
            if isinstance(row, dict) and row.get("lane_id") and int(row.get("horizon_sec") or 0) > 0
        }
        try:
            self.state_mtime_ns = self.state_path.stat().st_mtime_ns
        except OSError:
            self.state_mtime_ns = 0
        self.last_refresh_monotonic = time.monotonic()
        return self.state

    @staticmethod
    def _shrunk_win_rate(metrics: dict[str, Any], prior_n: float = 20.0) -> float:
        n = max(0.0, _finite(metrics.get("n")))
        wins = max(0.0, min(n, _finite(metrics.get("win_rate")) * n / 100.0))
        return (wins + 0.5 * prior_n) / max(1.0, n + prior_n)

    def _score_signal_at_horizon(
        self,
        candidate: dict[str, Any],
        horizon: int,
        *,
        min_confidence: float,
        min_expected_net_pips: float,
    ) -> dict[str, Any]:
        candidate = dict(candidate)
        lane_id = str(candidate.get("lane_id") or "")
        lane_evidence = self.signal_evidence.get((lane_id, int(horizon))) or {}

        spread = max(0.0, _finite(candidate.get("spread_pips")))
        forecast_curve = candidate.get("forecast_curve") or {}
        forecast_point = (
            forecast_curve.get(str(int(horizon)))
            or forecast_curve.get(int(horizon))
            or {}
        )
        if isinstance(forecast_point, dict) and forecast_point:
            candidate.pop("predicted_signed_pips", None)
            candidate.pop("projected_net_pips", None)
            probability_up = max(
                0.01,
                min(0.99, _finite(forecast_point.get("probability_up"), 0.5)),
            )
            predicted_value = forecast_point.get("predicted_signed_pips")
            predicted_signed_pips = (
                _finite(predicted_value) if predicted_value is not None else None
            )
            point_projected_net = forecast_point.get("projected_net_pips")
            direction = "buy" if probability_up >= 0.5 else "sell"
            update = {
                "direction": direction,
                "forecast_horizon_sec": int(horizon),
                "probability_up": probability_up,
                "forecast_point": forecast_point,
                "account_eligible": bool(
                    candidate.get("account_eligible", True)
                    and forecast_point.get("account_eligible", True)
                ),
                "research_only": bool(
                    candidate.get("research_only")
                    or forecast_point.get("research_only")
                    or not forecast_point.get("account_eligible", True)
                ),
            }
            if not update["account_eligible"]:
                update["research_blocked_reason"] = str(
                    forecast_point.get("research_blocked_reason")
                    or "predictor_cell_not_promoted"
                )
            predictor_evidence = forecast_point.get("predictor_promotion_evidence")
            if isinstance(predictor_evidence, dict):
                update["predictor_promotion_evidence"] = predictor_evidence
            if predicted_signed_pips is not None:
                directional_pips = (
                    predicted_signed_pips
                    if direction == "buy"
                    else -predicted_signed_pips
                )
                update["predicted_signed_pips"] = predicted_signed_pips
                update["projected_net_pips"] = directional_pips - spread
            elif point_projected_net is not None:
                update["projected_net_pips"] = _finite(point_projected_net)
            candidate.update(update)
        predictor_evidence = candidate.get("predictor_promotion_evidence")
        evidence = (
            predictor_evidence
            if isinstance(predictor_evidence, dict)
            and predictor_evidence.get("eligible")
            else lane_evidence
        )
        raw = evidence.get("raw") or {}
        training = evidence.get("training") or {}
        holdout = evidence.get("holdout") or {}
        signal_ratio = max(0.0, _finite(candidate.get("signal_to_spread")))
        probability_up = candidate.get("probability_up")
        if probability_up is not None:
            up = max(0.01, min(0.99, _finite(probability_up, 0.5)))
            side_probability = up if str(candidate.get("direction")) == "buy" else 1.0 - up
        else:
            side_probability = 0.5 + min(0.16, max(0.0, signal_ratio - 1.0) * 0.035)
        probability_edge = max(0.0, min(1.0, 2.0 * (side_probability - 0.5)))
        atr = max(0.0, _finite(candidate.get("atr_pips")))
        volatility_cap = (
            atr * math.sqrt(max(1.0, float(horizon)) / 300.0)
            if atr > 0.0
            else math.inf
        )
        reference_horizon = max(
            1.0,
            _finite(
                candidate.get("signal_reference_horizon_sec"),
                60.0 if candidate.get("predicted_signed_pips") is not None else 300.0,
            ),
        )
        gross_movement = 0.0
        projection_method = "ratio_fallback"
        if candidate.get("projected_net_pips") is not None:
            instant_edge = _finite(candidate.get("projected_net_pips"), -999.0)
            gross_movement = max(0.0, instant_edge + spread)
            projection_method = (
                "horizon_forecast_curve" if forecast_point else "explicit_projected_net"
            )
        elif candidate.get("predicted_signed_pips") is not None:
            gross_movement = abs(_finite(candidate.get("predicted_signed_pips"))) * math.sqrt(
                max(1.0, float(horizon)) / reference_horizon
            )
            if math.isfinite(volatility_cap):
                gross_movement = min(gross_movement, volatility_cap)
            instant_edge = gross_movement - spread
            projection_method = "horizon_scaled_equation"
        elif candidate.get("signal_strength_pips") is not None:
            impulse = abs(_finite(candidate.get("signal_strength_pips")))
            horizon_fraction = min(1.0, max(1.0, float(horizon)) / reference_horizon)
            gross_movement = impulse * horizon_fraction
            if math.isfinite(volatility_cap):
                gross_movement = min(gross_movement, volatility_cap)
            instant_edge = gross_movement - spread
            # The observed impulse is the movement estimate. Directional
            # probability is already applied to confidence and ranking below;
            # multiplying the magnitude by 2p-1 here double-discounted every
            # structural setup and made even spread-clearing signals impossible.
            projection_method = "costed_structural_impulse"
        else:
            gross_movement = max(0.0, signal_ratio - 1.0) * max(spread, 0.10)
            instant_edge = gross_movement

        agreement_probability = max(0.0, min(1.0, _finite(candidate.get("agreement_probability"), 0.5)))
        agreeing_families = int(_finite(candidate.get("agreement_family_count")))
        opposing_families = int(_finite(candidate.get("opposing_family_count")))
        effective_min_confidence, effective_min_expected_net, agreement_gate_mode = (
            agreement_adjusted_signal_thresholds(
                min_confidence,
                min_expected_net_pips,
                agreeing_families,
                opposing_families,
            )
        )
        current_confidence = (
            0.75 * side_probability + 0.25 * agreement_probability
            if agreeing_families >= 2
            else side_probability
        )

        raw_n = int(_finite(raw.get("n")))
        independent_blocks = int(_finite(evidence.get("independent_blocks")))
        sample_reliability = min(1.0, math.sqrt(raw_n / 150.0)) if raw_n else 0.0
        block_reliability = min(1.0, math.sqrt(independent_blocks / 12.0)) if independent_blocks else 0.0
        reliability = sample_reliability * block_reliability
        cold_start_applies = int(horizon) >= 7200
        cold_start_samples = 30 if cold_start_applies else 0
        cold_start_blocks = 4 if cold_start_applies else 0
        cold_start_factor = 1.0
        if cold_start_applies:
            sample_fraction = min(1.0, raw_n / max(1.0, float(cold_start_samples)))
            block_fraction = min(
                1.0,
                independent_blocks / max(1.0, float(cold_start_blocks)),
            )
            cold_start_factor = math.sqrt(sample_fraction * block_fraction)
            current_confidence = 0.5 + (current_confidence - 0.5) * cold_start_factor
        cold_start_ready = bool(
            not cold_start_applies
            or (
                raw_n >= cold_start_samples
                and independent_blocks >= cold_start_blocks
            )
        )

        raw_win = self._shrunk_win_rate(raw)
        holdout_win = self._shrunk_win_rate(holdout, prior_n=10.0)
        historical_win = 0.40 * raw_win + 0.60 * holdout_win if holdout.get("n") else raw_win
        training_edge = _finite(training.get("avg"))
        holdout_edge = _finite(holdout.get("avg"))
        raw_edge = _finite(raw.get("avg"))
        historical_edge = (
            0.25 * raw_edge + 0.25 * training_edge + 0.50 * holdout_edge
            if holdout.get("n")
            else raw_edge
        )
        raw_lower = _finite(raw.get("lower_confidence"), raw_edge)
        training_lower = _finite(training.get("lower_confidence"), training_edge)
        holdout_lower = _finite(holdout.get("lower_confidence"), holdout_edge)
        raw_lower = raw_edge if raw_lower <= -100.0 else raw_lower
        training_lower = training_edge if training_lower <= -100.0 else training_lower
        holdout_lower = holdout_edge if holdout_lower <= -100.0 else holdout_lower
        historical_lower = (
            0.25 * raw_lower + 0.25 * training_lower + 0.50 * holdout_lower
            if holdout.get("n")
            else raw_lower
        )
        conservative_historical_edge = (
            0.65 * historical_edge + 0.35 * historical_lower
        )
        if training.get("n") and holdout.get("n") and training_edge * holdout_edge < 0.0:
            reliability *= 0.50

        historical_confidence = 0.5 + (historical_win - 0.5) * reliability
        historical_weight = 0.65 * reliability
        confidence = current_confidence * (1.0 - historical_weight) + historical_confidence * historical_weight
        unpenalized_expected_net = (
            instant_edge * (1.0 - historical_weight)
            + conservative_historical_edge * historical_weight
        )
        turnover_pressure = max(
            0.0,
            math.sqrt(300.0 / max(15.0, float(horizon))) - 1.0,
        )
        short_horizon_cost = spread * (0.10 + 0.20 * turnover_pressure)
        required_net_edge = effective_min_expected_net + short_horizon_cost
        expected_net = unpenalized_expected_net - short_horizon_cost
        gross_to_spread = (
            gross_movement / spread if spread > 0.0 else math.inf
        )
        minimum_gross_to_spread = max(
            1.0,
            _finite(candidate.get("minimum_gross_to_spread"), 1.15),
        )
        negative_history_veto = bool(
            raw_n >= 30
            and reliability >= 0.45
            and raw_edge < 0.0
            and training_edge < 0.0
            and holdout_edge <= 0.0
        )

        blocked_by: list[str] = []
        if confidence < effective_min_confidence:
            blocked_by.append("signal_confidence")
        if expected_net < effective_min_expected_net:
            blocked_by.append("expected_net_edge")
        if instant_edge < -0.05:
            blocked_by.append("current_cost_edge")
        if spread > 0.0 and gross_to_spread < minimum_gross_to_spread:
            blocked_by.append("forecast_below_spread_buffer")
        if negative_history_veto:
            blocked_by.append("persistent_negative_history")
        if not cold_start_ready:
            blocked_by.append("cold_start_evidence")
        if not bool(candidate.get("account_eligible", True)):
            blocked_by.append(
                str(candidate.get("research_blocked_reason") or "account_ineligible")
            )
        for reason in candidate.get("preconsensus_blockers") or []:
            blocker = f"setup_gate:{reason}"
            if blocker not in blocked_by:
                blocked_by.append(blocker)

        confidence_edge = max(0.0, 2.0 * (confidence - 0.5))
        base_score = (
            expected_net * max(0.10, confidence_edge)
            + max(0.0, instant_edge - short_horizon_cost) * 0.20
            + max(0.0, agreement_probability - 0.5) * 0.25
        )
        turnover_multiplier = 1.0 / (1.0 + 0.35 * turnover_pressure)
        score = base_score * turnover_multiplier
        pip_return_on_margin = max(
            0.0,
            _finite(candidate.get("pip_return_on_margin_per_pip_pct")),
        )
        if pip_return_on_margin <= 0.0:
            bid = _finite(candidate.get("bid"))
            ask = _finite(candidate.get("ask"))
            mid = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else max(bid, ask)
            pip = max(0.0, _finite(candidate.get("pip")))
            margin_rate = max(1e-6, _finite(candidate.get("margin_rate"), 0.02))
            if mid > 0.0 and pip > 0.0:
                pip_return_on_margin = 100.0 * pip / (mid * margin_rate)
        expected_margin_return_per_hour = (
            expected_net
            * pip_return_on_margin
            * 3600.0
            / max(60.0, float(horizon))
        )
        liquidity_quality = max(
            0.10,
            min(1.0, _finite(candidate.get("liquidity_quality"), 0.50)),
        )
        cost_quality = (
            atr / max(0.01, atr + 2.0 * spread)
            if atr > 0.0
            else 1.0 / (1.0 + spread)
        )
        confidence_quality = 0.20 + 0.80 * confidence_edge
        confidence_adjusted_margin_return = (
            expected_margin_return_per_hour
            * confidence_quality
            * liquidity_quality
            * cost_quality
            * turnover_multiplier
        )
        legacy_score_tiebreaker = 0.001 * math.tanh(score)
        normalized_rank_score = (
            confidence_adjusted_margin_return + legacy_score_tiebreaker
        )
        return {
            **candidate,
            "execution_horizon_sec": int(horizon),
            "signal_eligible": not blocked_by,
            "signal_blocked_by": blocked_by,
            "signal_confidence": round(confidence, 6),
            "base_min_signal_confidence": round(min_confidence, 6),
            "effective_min_signal_confidence": round(effective_min_confidence, 6),
            "base_min_expected_net_pips": round(min_expected_net_pips, 4),
            "effective_min_expected_net_pips": round(effective_min_expected_net, 4),
            "agreement_gate_mode": agreement_gate_mode,
            "signal_score": round(score, 6),
            "normalized_rank_score": round(normalized_rank_score, 8),
            "legacy_score_tiebreaker": round(legacy_score_tiebreaker, 8),
            "expected_margin_return_per_hour_pct": round(
                expected_margin_return_per_hour,
                8,
            ),
            "confidence_adjusted_margin_return_per_hour_pct": round(
                confidence_adjusted_margin_return,
                8,
            ),
            "pip_return_on_margin_per_pip_pct": round(
                pip_return_on_margin,
                8,
            ),
            "liquidity_quality": round(liquidity_quality, 6),
            "cost_quality": round(cost_quality, 6),
            "base_signal_score": round(base_score, 6),
            "turnover_multiplier": round(turnover_multiplier, 6),
            "instant_projected_net_pips": round(instant_edge, 4),
            "projected_gross_movement_pips": round(gross_movement, 4),
            "projection_method": projection_method,
            "directional_probability_edge": round(probability_edge, 6),
            "signal_reference_horizon_sec": int(round(reference_horizon)),
            "volatility_projection_cap_pips": (
                None if not math.isfinite(volatility_cap) else round(volatility_cap, 4)
            ),
            "unpenalized_projected_net_pips": round(unpenalized_expected_net, 4),
            "short_horizon_cost_pips": round(short_horizon_cost, 4),
            "execution_uncertainty_cost_pips": round(short_horizon_cost, 4),
            "gross_to_spread": (
                None if not math.isfinite(gross_to_spread) else round(gross_to_spread, 6)
            ),
            "minimum_gross_to_spread": round(minimum_gross_to_spread, 6),
            "required_net_edge_pips": round(required_net_edge, 4),
            "projected_net_pips": round(expected_net, 4),
            "projected_net_pips_per_hour": round(expected_net * 3600.0 / max(1, int(horizon)), 4),
            "historical_expected_net_pips": round(historical_edge, 4),
            "historical_lower_bound_pips": round(historical_lower, 4),
            "sample_adjusted_historical_edge_pips": round(
                conservative_historical_edge,
                4,
            ),
            "historical_win_probability": round(historical_win, 6),
            "historical_reliability": round(reliability, 6),
            "cold_start_applies": cold_start_applies,
            "cold_start_ready": cold_start_ready,
            "cold_start_factor": round(cold_start_factor, 6),
            "cold_start_sample_count": raw_n,
            "cold_start_required_samples": cold_start_samples,
            "cold_start_independent_blocks": independent_blocks,
            "cold_start_required_blocks": cold_start_blocks,
            "promotion_score": _finite(evidence.get("score"), -999.0),
            "promotion_evidence": self._compact(evidence) if evidence else {},
        }

    def rank_signal_candidates(
        self,
        candidates: list[dict[str, Any]],
        *,
        min_confidence: float = 0.54,
        min_expected_net_pips: float = 0.05,
    ) -> list[dict[str, Any]]:
        """Rank opportunities; strict lane promotion remains diagnostic only."""
        self.refresh_if_due()
        ranked: list[dict[str, Any]] = []
        for candidate in candidates:
            explicit_horizon = int(_finite(candidate.get("forecast_horizon_sec")))
            forecast_curve = candidate.get("forecast_curve") or {}
            curve_horizons: list[int] = []
            if isinstance(forecast_curve, dict) and forecast_curve:
                for value in forecast_curve:
                    horizon = int(_finite(value))
                    if horizon > 0 and horizon in self.horizons:
                        curve_horizons.append(horizon)
            if explicit_horizon > 0:
                horizons = [explicit_horizon]
            elif curve_horizons:
                horizons = sorted(set(curve_horizons))
            else:
                horizons = list(self.horizons)
            choices = [
                self._score_signal_at_horizon(
                    candidate,
                    horizon,
                    min_confidence=min_confidence,
                    min_expected_net_pips=min_expected_net_pips,
                )
                for horizon in horizons
                if horizon > 0
            ]
            if not choices:
                continue
            choices.sort(
                key=lambda row: (
                    bool(row.get("signal_eligible")),
                    _finite(row.get("normalized_rank_score"), -999.0),
                    _finite(row.get("signal_score"), -999.0),
                    _finite(row.get("signal_confidence")),
                ),
                reverse=True,
            )
            best = dict(choices[0])
            best["signal_horizon_curve"] = [
                {
                    "horizon_sec": int(row.get("execution_horizon_sec") or 0),
                    "direction": str(row.get("direction") or ""),
                    "probability_up": _finite(row.get("probability_up"), 0.5),
                    "predicted_signed_pips": _finite(
                        row.get("predicted_signed_pips")
                    ),
                    "account_eligible": bool(row.get("account_eligible", True)),
                    "research_only": bool(row.get("research_only")),
                    "preconsensus_class": str(
                        row.get("preconsensus_class") or "accepted"
                    ),
                    "matrix_input_weight": _finite(
                        row.get("matrix_input_weight"), 1.0
                    ),
                    "signal_eligible": bool(row.get("signal_eligible")),
                    "signal_blocked_by": list(row.get("signal_blocked_by") or []),
                    "signal_confidence": _finite(row.get("signal_confidence")),
                    "signal_score": _finite(row.get("signal_score")),
                    "normalized_rank_score": _finite(
                        row.get("normalized_rank_score")
                    ),
                    "expected_margin_return_per_hour_pct": _finite(
                        row.get("expected_margin_return_per_hour_pct")
                    ),
                    "confidence_adjusted_margin_return_per_hour_pct": _finite(
                        row.get(
                            "confidence_adjusted_margin_return_per_hour_pct"
                        )
                    ),
                    "liquidity_quality": _finite(
                        row.get("liquidity_quality")
                    ),
                    "cost_quality": _finite(row.get("cost_quality")),
                    "projected_net_pips": _finite(row.get("projected_net_pips")),
                    "instant_projected_net_pips": _finite(
                        row.get("instant_projected_net_pips")
                    ),
                    "projected_gross_movement_pips": _finite(
                        row.get("projected_gross_movement_pips")
                    ),
                    "projection_method": str(row.get("projection_method") or ""),
                    "directional_probability_edge": _finite(
                        row.get("directional_probability_edge")
                    ),
                    "signal_reference_horizon_sec": int(
                        _finite(row.get("signal_reference_horizon_sec"))
                    ),
                    "volatility_projection_cap_pips": row.get(
                        "volatility_projection_cap_pips"
                    ),
                    "projected_net_pips_per_hour": _finite(row.get("projected_net_pips_per_hour")),
                    "historical_expected_net_pips": _finite(row.get("historical_expected_net_pips")),
                    "historical_lower_bound_pips": _finite(
                        row.get("historical_lower_bound_pips")
                    ),
                    "sample_adjusted_historical_edge_pips": _finite(
                        row.get("sample_adjusted_historical_edge_pips")
                    ),
                    "historical_reliability": _finite(row.get("historical_reliability")),
                    "cold_start_applies": bool(row.get("cold_start_applies")),
                    "cold_start_ready": bool(row.get("cold_start_ready")),
                    "cold_start_factor": _finite(row.get("cold_start_factor"), 1.0),
                    "cold_start_sample_count": int(
                        _finite(row.get("cold_start_sample_count"))
                    ),
                    "cold_start_required_samples": int(
                        _finite(row.get("cold_start_required_samples"))
                    ),
                    "cold_start_independent_blocks": int(
                        _finite(row.get("cold_start_independent_blocks"))
                    ),
                    "cold_start_required_blocks": int(
                        _finite(row.get("cold_start_required_blocks"))
                    ),
                    "unpenalized_projected_net_pips": _finite(
                        row.get("unpenalized_projected_net_pips")
                    ),
                    "short_horizon_cost_pips": _finite(
                        row.get("short_horizon_cost_pips")
                    ),
                    "execution_uncertainty_cost_pips": _finite(
                        row.get("execution_uncertainty_cost_pips")
                    ),
                    "gross_to_spread": row.get("gross_to_spread"),
                    "minimum_gross_to_spread": _finite(
                        row.get("minimum_gross_to_spread"),
                        1.15,
                    ),
                    "required_net_edge_pips": _finite(
                        row.get("required_net_edge_pips")
                    ),
                }
                for row in sorted(choices, key=lambda item: int(item.get("execution_horizon_sec") or 0))
            ]
            ranked.append(best)
        ranked.sort(
            key=lambda row: (
                bool(row.get("signal_eligible")),
                _finite(row.get("normalized_rank_score"), -999.0),
                _finite(row.get("signal_score"), -999.0),
                _finite(row.get("signal_confidence")),
                _finite(row.get("instant_projected_net_pips"), -999.0),
                -_finite(row.get("spread_pips")),
            ),
            reverse=True,
        )
        return ranked

    def refresh_if_due(self, *, force: bool = False) -> dict[str, Any]:
        if self.fit_enabled and (force or time.monotonic() - self.last_refresh_monotonic >= self.refresh_sec):
            return self.fit()
        if not self.fit_enabled:
            try:
                modified = self.state_path.stat().st_mtime_ns
            except OSError:
                modified = 0
            if force or modified != self.state_mtime_ns:
                return self.load_state()
        return self.state

    def _segment_projection(self, row: dict[str, Any], instrument: str, session: str) -> dict[str, Any]:
        holdout = row.get("holdout") or {}
        base = _finite(holdout.get("avg"), -999.0)
        numerator = base * 40.0
        denominator = 40.0
        blockers: list[str] = []
        segments: dict[str, Any] = {}
        for label, metrics in (
            ("pair", (row.get("pair_metrics") or {}).get(instrument) or {}),
            ("session", (row.get("session_metrics") or {}).get(session) or {}),
        ):
            n = int(metrics.get("n") or 0)
            average = _finite(metrics.get("avg"))
            weight = min(40.0, float(n))
            if n:
                numerator += average * weight
                denominator += weight
            if n >= self.thresholds.min_segment_samples and average <= 0.0:
                blockers.append(f"negative_{label}_segment")
            segments[label] = {"name": instrument if label == "pair" else session, **metrics}
        expected = numerator / denominator if denominator else -999.0
        if expected < self.thresholds.min_average_pips:
            blockers.append("projected_net_edge")
        return {
            "expected_net_pips": round(expected, 4),
            "segments": segments,
            "blocked_by": blockers,
        }

    def qualify_candidates(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.refresh_if_due()
        if not self.state.get("source_complete"):
            return []
        current_session = session_name(datetime.now(timezone.utc))
        qualified: list[dict[str, Any]] = []
        for candidate in candidates:
            if not bool(candidate.get("account_eligible", True)):
                continue
            best: dict[str, Any] | None = None
            lane_id = str(candidate.get("lane_id") or "")
            instrument = str(candidate.get("instrument") or "")
            for horizon in self.horizons:
                evidence = self.evidence.get((lane_id, horizon))
                if not evidence or not evidence.get("eligible"):
                    continue
                projection = self._segment_projection(evidence, instrument, current_session)
                if projection["blocked_by"]:
                    continue
                scored = {
                    **candidate,
                    "execution_horizon_sec": int(horizon),
                    "promotion_score": _finite(evidence.get("score"), -999.0),
                    "projected_net_pips": projection["expected_net_pips"],
                    "promotion_evidence": self._compact(evidence),
                    "promotion_segments": projection["segments"],
                }
                if best is None or (
                    scored["promotion_score"],
                    scored["projected_net_pips"],
                ) > (
                    best["promotion_score"],
                    best["projected_net_pips"],
                ):
                    best = scored
            if best is not None:
                qualified.append(best)
        qualified.sort(
            key=lambda row: (
                _finite(row.get("promotion_score"), -999.0),
                _finite(row.get("projected_net_pips"), -999.0),
                _finite(row.get("signal_to_spread")),
                -_finite(row.get("spread_pips")),
            ),
            reverse=True,
        )
        return qualified

    def eligible_summary(self, limit: int = 8) -> list[dict[str, Any]]:
        rows = [row for row in self.evidence.values() if row.get("eligible")]
        rows.sort(key=lambda row: (_finite(row.get("score"), -999.0), int(row.get("sample_count") or 0)), reverse=True)
        return [self._compact(row) for row in rows[:limit]]


__all__ = [
    "LanePromotionModel",
    "LanePromotionStore",
    "PromotionThresholds",
    "evaluate_promotion_scope",
    "session_name",
]
